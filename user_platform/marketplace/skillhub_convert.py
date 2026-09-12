"""skillhub 技能源同步（公开 API → skills 模块）。

SkillHub（https://skillhub.cn/skills?sortBy=score，腾讯云 dnspod 团队运营的国内
AI Skills 社区，站点数据由 api.skillhub.cn / api.skillhub.tencent.com 双域名提供）的
技能是公开 API：

- ``GET /api/skills?page=N&pageSize=24&sortBy=score`` 分页拉列表
  （code=0 信封：data.skills[] / data.total；item 字段见下）
- ``GET /api/v1/download?slug={slug}`` 下载 zip（302 跳腾讯云 COS，aiohttp 默认
  跟随重定向）；zip 含 SKILL.md（frontmatter name/description）+ 可执行脚本

无需鉴权，服务端可定时同步。逐页流式：拉一页 → 当页下载+解析+落库 → 再拉下一页，
每轮只取 score 头部 Top 120（``MAX_PAGES * PAGE_SIZE``），不抓 14 万+ 全量。

item 关键字段：``slug``（坐标）、``name``（展示名，中文标题）、``description``
（平台侧干净单行简介）、``category``（主分类 key）、``subCategories``
（[{key,name}] 对象数组，取 name 作子分类标签）、``namespace.canonicalName``
（@handle/slug 全局坐标）、``stars`` / ``downloads`` / ``score`` / ``version``。
去重键用 ``repo_full_name = skillhub/{namespace.canonicalName or slug}``，
与 agentscope 的 agentscope/{code} 同位。
"""
from __future__ import annotations

import asyncio
import io
import random
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote_plus

import aiohttp
from loguru import logger

from . import config as mp_config

BASE = "https://api.skillhub.cn"
PAGE_SIZE = 24  # 站点默认页大小，上游按它调优
# 每次同步只拉 score 头部若干页（Top N），不抓全量——社区总量 14 万+，全量一轮要数小时
# 且易触发风控；按 skillhub 首页「精选头部」口径拉 Top 120 足够覆盖质量最高的部分。
MAX_PAGES = 5
DEFAULT_VERSION = "1.0.0"
ALL_EDITORS = ["claude", "codex", "opencode", "cursor", "gemini"]

# 请求节流：skillhub 在腾讯网关（stgw）后面，实测高频请求直接 503（风控）。分页
# 与逐项下载都拉长间隔并叠加随机抖动——固定节奏的机器人特征更易被识别，抖动
# 让节奏贴近人工浏览。
_PAGE_SLEEP = 1.5   # 分页间隔基数（秒）
_PAGE_JITTER = 1.0  # 分页随机抖动上限（秒）
_ITEM_SLEEP = 2.5   # 逐项下载间隔基数（秒）
_ITEM_JITTER = 2.0  # 逐项下载随机抖动上限（秒）

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=120)
_last_result: dict[str, Any] = {"ran_at": "", "ok": None, "detail": "从未同步", "logs": []}
# 实时进度：同步进行中由前端轮询 last-sync 端点读取。
_progress: dict[str, Any] = {"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""}
def last_result() -> dict[str, Any]:
    return {**_last_result, "progress": dict(_progress)}
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
async def _gh_get_json(url: str) -> Any:
    """经 proxy_manager GET JSON（与 leaderboard_probe 同链路）。"""
    from providers.proxy_manager import get_proxy_manager

    manager = get_proxy_manager()
    resp = await manager.request(
        url=url, method="GET", headers={"Accept": "application/json"},
        timeout=_FETCH_TIMEOUT, proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status >= 400:
        raise RuntimeError(f"HTTP {resp.status}")
    return await resp.json()
async def _gh_get_bytes(url: str) -> bytes:
    """经 proxy_manager GET 二进制（zip 下载）。"""
    from providers.proxy_manager import get_proxy_manager

    manager = get_proxy_manager()
    resp = await manager.request(
        url=url, method="GET", headers={"Accept": "application/zip, application/octet-stream"},
        timeout=_FETCH_TIMEOUT, proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status >= 400:
        raise RuntimeError(f"HTTP {resp.status}")
    return await resp.read()
async def _fetch_page(page: int) -> tuple[list[dict], int]:
    """拉一页技能列表（按 score 排序，与 skillhub 首页口径一致）。

    返回 (items, total)：items 为本页条目（空=翻到底），total 为上游总量
    （仅日志展示用，不参与断流判断——真正决定拉多少页的是 MAX_PAGES）。
    """
    data = await _gh_get_json(f"{BASE}/api/skills?page={page}&pageSize={PAGE_SIZE}&sortBy=score")
    payload = (data or {}).get("data") or {}
    items = payload.get("skills") or []
    total = payload.get("total") or 0
    return items, int(total)
def _parse_skill_zip(data: bytes) -> tuple[str, str, str]:
    """从 zip 里提取 SKILL.md 的 frontmatter name + 正文。返回 (name, description, content)。"""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        candidates = sorted(
            (name for name in archive.namelist() if PurePosixPath(name).name == "SKILL.md" and not name.endswith("/")),
            key=lambda name: (len(PurePosixPath(name).parts), len(name)),
        )
        if not candidates:
            raise ValueError("zip 中未找到 SKILL.md")
        content = archive.read(candidates[0]).decode("utf-8", "replace")
    # 极简 frontmatter 解析
    name = ""
    description = ""
    lines = content.replace("\r\n", "\n").split("\n")
    if lines and lines[0].strip() == "---":
        for line in lines[1:]:
            if line.strip() == "---":
                break
            if line.startswith((" ", "\t", "-")):
                continue
            if ":" in line:
                key, _, value = line.partition(":")
                value = value.strip()
                if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                if key.strip() == "name":
                    name = value
                elif key.strip() == "description":
                    description = value
    return name, description, content
async def sync_skillhub(
    *,
    overwrite_draft: bool = False,
    overwrite_published: bool = False,
    run_by: str = "manual",
) -> dict:
    """同步 skillhub 技能源：逐页流式拉 Top N → 每条下载 zip → 落库，一页一处理。

    逐页流式：拉一页 → 当页逐条下载+解析+写库 → 再拉下一页。不先缓冲全量列表，
    内存峰值只有一页；每轮只取 score 头部 ``MAX_PAGES * PAGE_SIZE`` 条（Top 120），
    社区全量 14 万+ 不抓——质量头部已覆盖，且全量轮次太长易触发风控。

    覆盖模式（手动「立即同步」弹框勾选；定时循环不传恒为不覆盖）：overwrite_draft
    / overwrite_published 按行状态命中才重写资源字段，详见
    ``leaderboard_resource_store.upsert_item``。本源 install_spec 每轮从当轮 zip
    确定性派生，恒带 install_spec_fresh=True——覆盖时分类/安装配置一并重写。

    统一源模型：落 ``marketplace_leaderboard_items``，source='skillhub'，与
    agent-leaderboard / agency-agents / agency-agents-zh / agentscope 同一条草稿→发布流水线，来源筛选即 source 列。
    ON CONFLICT 只刷 external_data，重同步不冲掉管理员改过的资源字段。
    """
    global _last_result, _progress
    ran_at = _now_iso()
    logs: list[dict[str, str]] = []

    def _log(phase: str, detail: str) -> None:
        logs.append({"ts": _now_iso(), "phase": phase, "detail": detail})

    # 进入同步态：实时进度可见
    _progress.update({"running": True, "phase": "init", "current": 0, "total": 0, "current_item": ""})

    try:
        overwrite_on = overwrite_draft or overwrite_published
        overwrite_note = (
            "；覆盖：" + "、".join(filter(None, [
                "已发布" if overwrite_published else "",
                "草稿" if overwrite_draft else "",
            ])) if overwrite_on else ""
        )
        _log("fetch_list", f"逐页拉取 {BASE}/api/skills?sortBy=score（Top {MAX_PAGES * PAGE_SIZE}）{overwrite_note}")
        _progress.update({"phase": "fetch_list", "current_item": ""})
        converted = 0
        failed: list[dict] = []
        import marketplace_leaderboard_store as lb_store

        # 逐页流式：拉一页 → 当页下载+解析+落库 → 再拉下一页。不先缓冲全量列表，
        # 内存峰值只有一页；进度 current/total 跨页累计，total 在首页拿到总量后定。
        idx = 0
        total = 0
        for page in range(1, MAX_PAGES + 1):
            items, page_total = await _fetch_page(page)
            if page == 1:
                total = min(page_total or MAX_PAGES * PAGE_SIZE, MAX_PAGES * PAGE_SIZE)
                _log("fetch_list_ok", f"上游总量 {page_total}，本轮按 score 取 Top {total}")
                _progress.update({"phase": "download", "total": total})
            if not items:
                _log("fetch_list_done", f"第 {page} 页无条目，翻到底，停止")
                break
            _log("page", f"第 {page}/{MAX_PAGES} 页，{len(items)} 条")
            for it in items:
                idx += 1
                slug = str(it.get("slug") or "").strip()
                ns = it.get("namespace") or {}
                ns_name = str(ns.get("canonicalName") or "").strip()  # @handle/slug
                code = str(ns_name or slug)
                if not slug:
                    continue
                _progress.update({"current": idx, "current_item": slug})
                try:
                    _log("download", f"[{idx}/{total}] {slug}：下载 zip")
                    zip_data = await _gh_get_bytes(f"{BASE}/api/v1/download?slug={quote_plus(slug)}")
                    name, description, content = _parse_skill_zip(zip_data)
                    # 分类：category 是主分类；subCategories 是 [{key,name}] 对象数组，取 name。
                    category = str(it.get("category") or "")
                    subcats = [
                        str(sc.get("name") or "").strip()
                        for sc in (it.get("subCategories") or [])
                        if isinstance(sc, dict) and str(sc.get("name") or "").strip()
                    ]
                    # 描述优先 API 列表里的 description（平台侧干净单行简介）；SKILL.md 的
                    # frontmatter description 可能是 YAML 多行块标量（description: | / >），
                    # naive 逐行解析会拆出 "|" 这种垃圾，只作兜底。
                    summary = str(it.get("description") or "").strip() or description
                    # 展示名：列表 name 是展示名（中文标题），SKILL.md frontmatter name 作兜底。
                    display = str(it.get("name") or "").strip() or name or slug
                    version = str(it.get("version") or "").strip() or DEFAULT_VERSION
                    stars = int(it.get("stars") or 0)
                    # 项目跳转：站点详情路径 /skills/{namespace.handle}/{slug}（无命名空间
                    # 则 /skills/{slug}），与站点 SPA 路由一致。API 的 homepage 字段是
                    # api.skillhub.cn 域（浏览器打开 405），不能用作跳转链接。
                    ns_handle = str(ns.get("handle") or "").strip()
                    repo_url = (
                        f"https://skillhub.cn/skills/{quote_plus(ns_handle)}/{quote_plus(slug)}"
                        if ns_handle else f"https://skillhub.cn/skills/{quote_plus(slug)}"
                    )
                    _log("upsert", f"[{idx}/{total}] {slug}：写入候选池（分类={category or '未分类'}）")
                    await lb_store.upsert_item({
                        # 本源 install_spec 从当轮 zip 确定性派生，恒为新鲜识别值
                        #（覆盖模式据此一并重写安装配置；见 store.upsert_item）。
                        "install_spec_fresh": True,
                        "source": "skillhub",
                        "board": "skills",
                        "repo_full_name": f"skillhub/{code}",
                        "repo_url": repo_url,
                        "description": summary,
                        "stars": stars,
                        "forks": 0,
                        "language": "",
                        "topics": subcats,
                        "upstream_category": category,
                        "use_cases": [],
                        "name": display,
                        "display_name": display,
                        "publisher": "skillhub",
                        "version": version,
                        "categories": [category] if category else [],
                        "tags": subcats,
                        "target_module": "skill",
                        "target_modules": ["skill"],
                        "install_spec": {
                            "skill": {
                                "install_method": "direct_url",
                                "download_url": f"{BASE}/api/v1/download?slug={quote_plus(slug)}",
                                "entries": [{"name": name or display, "path": "", "entry": "SKILL.md", "editors": list(ALL_EDITORS)}],
                            },
                        },
                        "external_data": {
                            "source": "skillhub",
                            "board": "skills",
                            "repo_full_name": f"skillhub/{code}",
                            "repo_url": repo_url,
                            "description": summary,
                            "raw": it,
                        },
                        "installable": True,
                        "labels": [],
                    }, overwrite_published=overwrite_published, overwrite_draft=overwrite_draft)
                    converted += 1
                except Exception as exc:  # noqa: BLE001
                    failed.append({"id": slug, "code": code, "errors": [str(exc)[:200]]})
                    _log("fail", f"[{idx}/{total}] {slug}：{str(exc)[:150]}")
                await asyncio.sleep(_ITEM_SLEEP + random.uniform(0, _ITEM_JITTER))  # 节流防风控
            # 页间节流（页内每项已有节流，页与页之间再加一道，贴近人工翻页节奏）
            if page < MAX_PAGES and items:
                await asyncio.sleep(_PAGE_SLEEP + random.uniform(0, _PAGE_JITTER))
        ok = not failed
        detail = f"同步 {converted} 条，失败 {len(failed)} 条{overwrite_note}"
        _log("done", detail)
        _last_result.update({"ran_at": ran_at, "ok": ok, "detail": detail, "logs": logs})
        # 执行记录落 DB（重启后面板仍能看到最新一份日志）+ 上次同步时间随配置持久化。
        try:
            import marketplace_sync_run_store
            from .source_config import record_source_sync
            await marketplace_sync_run_store.record_run(
                "skillhub", ok=ok, detail=detail, logs=logs, run_by=run_by,
                counts={"converted": converted, "failed": len(failed)},
            )
            await record_source_sync("skillhub", ran_at)
        except Exception:  # noqa: BLE001 — 记录失败不影响同步结果
            pass
        return {"converted": converted, "failed": failed[:20], "ran_at": ran_at, "detail": detail, "logs": logs}
    except Exception as exc:  # noqa: BLE001
        _log("error", str(exc)[:200])
        _last_result.update({"ran_at": ran_at, "ok": False, "detail": str(exc)[:200], "logs": logs})
        try:
            import marketplace_sync_run_store
            from .source_config import record_source_sync
            await marketplace_sync_run_store.record_run("skillhub", ok=False, detail=str(exc)[:200], logs=logs, run_by=run_by)
            await record_source_sync("skillhub", ran_at)
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        _progress.update({"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""})
async def sync_loop() -> None:
    """后台定时同步。开关关闭时空转（每小时复查配置，便于热开启）。

    与 leaderboard_sync.sync_loop 同款 asyncio 后台循环；已在跑时由
    ``_progress.running`` 挡掉，不并发跑。
    """
    import asyncio

    from .source_config import get_source_config_async

    while True:
        try:
            cfg = await get_source_config_async()
            if bool(cfg.get("skillhub_enabled")):
                hours = int(cfg.get("skillhub_interval_hours") or 24)
                if not _progress.get("running"):
                    await sync_skillhub()
                await asyncio.sleep(max(1, hours) * 3600)
                continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 循环不能因一次失败退出
            logger.warning("[skillhub] sync_loop error: {}", exc)
        # 未启用 / 出错：每小时复查一次配置（热开启），不空跑同步。
        await asyncio.sleep(3600)
