"""Tokenizer 词表版本检查：定时检查远端是否有新版本，只记录状态，绝不自动下载。

为什么不自动下载：词表可达数十 MB，而估算函数（token 预占 / 上下文超限拦截）在请求
热路径上。一次自动下载就可能拖死正在服务的请求。因此这里只发 HEAD 请求比对 ETag，
真正的下载由管理员在后台点「更新」触发（admin.warmup_tokenizer_vocab）。

词表本身存在 PostgreSQL tokenizer_vocabs 表（所有实例共享、容器重建不丢）；版本检查
结果存在进程内 _STATUS（HEAD 比对结果），meta 存在 usage_utils._HF_VOCAB_META（PG 快照）。
更新通过 runtime_sync 在实例间广播。
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from loguru import logger

import config
import usage_utils

# repo → 最近一次检查结果。状态取值：
#   current          本地（PG）已有且与远端 ETag 一致
#   missing          本地没有词表，运行时正在降级
#   update_available 远端 ETag 与本地记录不同，需要管理员手动更新
#   unknown          有词表但没有 etag（历史遗留），无法判断新旧
#   check_failed     检查过程出错（网络/镜像不可用）
_STATUS: dict[str, dict[str, Any]] = {}


def builtin_hf_repos() -> list[str]:
    """内置规则里声明的 HF 词表。规则由后端维护，用户不可编辑，所以这里就是全集。"""
    repos = []
    for rule in usage_utils._BUILTIN_TOKENIZER_RULES:
        if rule.get("enabled") is False or str(rule.get("type") or "") != "huggingface":
            continue
        repo = str(rule.get("repo") or "").strip()
        if repo and repo not in repos:
            repos.append(repo)
    return repos


async def _check_one(session, repo: str, mirror: str) -> dict:
    """检查单个 repo。只发 HEAD，不读 body。"""
    local_meta = usage_utils._HF_VOCAB_META.get(repo, {})
    local_etag = local_meta.get("etag")
    entry: dict[str, Any] = {
        "repo": repo,
        "present": repo in usage_utils._HF_VOCAB_META,  # PG 里有即 present
        "local_etag": local_etag,
        "local_bytes": local_meta.get("bytes") or 0,
        "downloaded_at": local_meta.get("downloaded_at"),
        "checked_at": time.time(),
    }

    url = f"{mirror}/{repo}/resolve/main/tokenizer.json"
    try:
        async with session.head(url, allow_redirects=True) as response:
            if response.status != 200:
                entry["status"] = "check_failed"
                entry["error"] = f"HTTP {response.status}"
                return entry
            remote_etag = (response.headers.get("ETag") or "").strip('"') or None
            entry["remote_etag"] = remote_etag
    except Exception as e:
        entry["status"] = "check_failed"
        entry["error"] = f"{type(e).__name__}: {e}"
        return entry

    if not entry["present"]:
        entry["status"] = "missing"
    elif not local_etag:
        # 有词表但没记录 etag（历史遗留），无法比对。
        entry["status"] = "unknown"
    elif remote_etag and remote_etag != local_etag:
        entry["status"] = "update_available"
    else:
        entry["status"] = "current"
    return entry


async def run_check_once() -> dict[str, dict]:
    """跑一轮检查并更新内存状态。任何失败都不抛异常——这是后台任务，不能影响服务。"""
    repos = builtin_hf_repos()
    if not repos:
        return {}
    mirror = config.Config.tokenizer_vocab_mirror()

    import aiohttp

    results: dict[str, dict] = {}
    try:
        timeout = aiohttp.ClientTimeout(total=60, sock_read=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for repo in repos:
                results[repo] = await _check_one(session, repo, mirror)
    except Exception as e:
        logger.warning(f"[tokenizer-vocab] 检查轮次失败: {type(e).__name__}: {e}")
        return _STATUS

    # 折叠进 _STATUS：只保留展示所需字段（present/bytes 来自 PG 快照，不在这里改）。
    for repo, item in results.items():
        _STATUS[repo] = {
            "repo": repo,
            "status": item.get("status", "check_failed"),
            "remote_etag": item.get("remote_etag"),
            "error": item.get("error"),
            "checked_at": item.get("checked_at"),
        }

    stale = [repo for repo, item in results.items() if item.get("status") in ("missing", "update_available")]
    if stale:
        logger.info(f"[tokenizer-vocab] 需要手动更新的词表: {stale}")
    return results


def current_status() -> list[dict]:
    """给管理端读的状态快照。没查过的 repo 也列出来，否则前端看不到它存在。

    字段命名与前端 TokenizerVocabItem 对齐：present / etag / bytes / downloaded_at /
    status / remote_etag / error。
    """
    out = []
    for repo in builtin_hf_repos():
        meta = usage_utils._HF_VOCAB_META.get(repo, {})
        present = repo in usage_utils._HF_VOCAB_META
        cached = _STATUS.get(repo, {})
        out.append({
            "repo": repo,
            "present": present,
            "etag": meta.get("etag"),
            "bytes": meta.get("bytes") or 0,
            "downloaded_at": meta.get("downloaded_at"),
            # 没跑过 HEAD 检查时，按「PG 有没有」给个直观默认值。
            "status": cached.get("status") or ("current" if present else "missing"),
            "remote_etag": cached.get("remote_etag"),
            "error": cached.get("error"),
        })
    return out


def record_download(repo: str, *, etag: str | None, size: int, mirror: str) -> None:
    """管理员手动更新词表后调用：刷新内存状态，让 current_status() 立即反映新值。

    只改 _STATUS 与 usage_utils._HF_VOCAB_META；PG 写入由 warmup 端点完成。
    """
    now = time.time()
    _STATUS[repo] = {
        "repo": repo,
        "status": "current",
        "remote_etag": etag,
        "error": None,
        "checked_at": now,
    }
    meta = dict(usage_utils._HF_VOCAB_META.get(repo, {}))
    meta.update({
        "etag": etag,
        "bytes": size,
        "mirror": mirror,
        "downloaded_at": now,
        "updated_at": now,
    })
    usage_utils._HF_VOCAB_META[repo] = meta


async def check_loop() -> None:
    """后台检查循环。开关与间隔都读全局配置，改配置后下一轮生效。

    多实例下每个实例各查各的：词表状态只用于「提示管理员去更新」，按实例独立无妨，
    所以不需要分布式锁。HEAD 请求很轻，重复几次可以接受。
    """
    # 启动后先等一会：别和启动期的目录同步、渠道初始化抢网络。
    await asyncio.sleep(30)
    while True:
        try:
            if config.Config.tokenizer_vocab_check_enabled():
                await run_check_once()
            interval = config.Config.tokenizer_vocab_check_interval_hours() * 3600
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[tokenizer-vocab] 检查循环异常: {type(e).__name__}: {e}")
            interval = 3600
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
