"""外部榜单同步（Agent-Leaderboard）。

**只有配置了市场管理并显式打开 ``leaderboard_sync_enabled`` 的部署才会运行。**
其余部署只消费已配置的市场仓库，不抓外部榜单——所以这里所有入口都先查开关，
关闭时直接返回，不建连接、不写库。

同步做的事只有一件：把上游「仓库元数据」落成候选池草稿。它**从不发布**，
``upsert_item`` 也永不改已发布行的状态。发布始终是管理员的显式点击。

归类口径（关键，别按 board 直接映射模块）：上游 board 是**主题**（skills / mcp /
prompts / frameworks / research），我们的四类是**安装形态**（mcp / skill / prompt /
plugin）。一个 skills 榜的仓库可能是插件包，一个 mcp 榜的第一名可能只是 awesome
目录。所以这里只做保守预归类 + 打标签，最终归类由管理员收口。
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
from datetime import datetime, timezone
from typing import Any

import aiohttp
from loguru import logger

from . import config as mp_config
from .source_config import (
    DEFAULT_LEADERBOARD_BOARDS,
    DEFAULT_LEADERBOARD_REPO,
    get_source_config_async,
)

# board -> 上游数据文件。全同步这五个（frameworks/research 也拉，但多为仅浏览）。
BOARD_FILES = {
    "skills": "data/data.json",
    "mcp": "data/mcp_data.json",
    "prompts": "data/prompts_data.json",
    "frameworks": "data/frameworks_data.json",
    "research": "data/auto_research_data.json",
}

# frameworks / research 的绝大多数是开发库（langchain）或独立研究程序，
# 不是给编辑器装的扩展——四类里没有它们的安装形态，默认仅浏览。
_BROWSE_ONLY_BOARDS = frozenset({"frameworks", "research"})

# awesome 目录：一篇 README 列别人的东西，本身不是可装物。命名与描述都很有辨识度。
_DIRECTORY_RE = re.compile(
    r"\bawesome\b|\bcurated\b|\bcollection\b|\blist of\b|\bdirectory\b|\bresources\b",
    re.IGNORECASE,
)

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=120)
_MAX_BYTES = 32 * 1024 * 1024  # 上游单个 board 文件最大 ~16MB

_lock = asyncio.Lock()
_last_result: dict[str, Any] = {"ran_at": "", "ok": None, "detail": "从未同步"}


def last_result() -> dict[str, Any]:
    return dict(_last_result)


async def _settings() -> dict[str, Any]:
    return await get_source_config_async()


async def is_enabled() -> bool:
    """同步开关。未配置市场仓库时也视为关闭——没有市场就没有候选池的意义。"""
    source = await _settings()
    if not source.get("leaderboard_sync_enabled"):
        return False
    return bool(mp_config.settings.enabled)


def resolve_boards(source: dict[str, Any]) -> list[str]:
    """要同步的 board 列表（过滤掉不认识的名字）。"""
    raw = str(source.get("leaderboard_boards") or DEFAULT_LEADERBOARD_BOARDS)
    boards = [b.strip() for b in raw.split(",") if b.strip()]
    return [b for b in boards if b in BOARD_FILES]


def _repo(source: dict[str, Any]) -> str:
    return str(source.get("leaderboard_repo") or DEFAULT_LEADERBOARD_REPO).strip()


async def _fetch_board(session: aiohttp.ClientSession, repo: str, path: str) -> Any:
    """取一个 board 的 JSON。

    走 GitHub Contents/Blobs API 而不是 raw：raw 在部分网络下不可达，而 API 主机
    通常可达；Contents API 对超过 ~1MB 的文件只回 sha，需要再取 blob（board 文件
    普遍 1–16MB，所以这条分支是常态而非兜底）。出站统一经 proxy_manager，
    代理取资源中心配置的 ``proxy_id``（与市场读写同源）。
    """
    from providers.proxy_manager import get_proxy_manager

    proxy_id = mp_config.settings.proxy_id or None
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ai-lubricant-leaderboard-sync"}
    token = mp_config.settings.github_token
    if token:
        headers["Authorization"] = f"token {token}"

    manager = get_proxy_manager()
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref=main"
    resp = await manager.request(
        url=url, method="GET", headers=headers, timeout=_FETCH_TIMEOUT, proxy_config_id=proxy_id
    )
    if resp.status >= 400:
        raise RuntimeError(f"GET {path} HTTP {resp.status}")
    meta = await resp.json()
    if not isinstance(meta, dict):
        raise RuntimeError(f"{path}: unexpected payload")
    if meta.get("content") and meta.get("encoding") == "base64":
        return json.loads(base64.b64decode(meta["content"]).decode("utf-8", "replace"))
    blob_sha = meta.get("sha")
    if not blob_sha:
        raise RuntimeError(f"{path}: no content and no sha")
    blob_url = f"https://api.github.com/repos/{repo}/git/blobs/{blob_sha}"
    resp = await manager.request(
        url=blob_url, method="GET", headers=headers, timeout=_FETCH_TIMEOUT, proxy_config_id=proxy_id
    )
    if resp.status >= 400:
        raise RuntimeError(f"GET blob {blob_sha[:8]} HTTP {resp.status}")
    blob = await resp.json()
    content = blob.get("content") if isinstance(blob, dict) else None
    if not content:
        raise RuntimeError(f"{path}: blob has no content")
    raw = base64.b64decode(content)
    if len(raw) > _MAX_BYTES:
        raise RuntimeError(f"{path}: payload too large ({len(raw)} bytes)")
    return json.loads(raw.decode("utf-8", "replace"))


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def classify(board: str, repo: dict) -> dict[str, Any]:
    """按上游事实预分类（一级分类，多选）。

    分类是**多选**：上游 category 字符串里命中几个形态关键词就采信几个（一条
    既可以当 skill 装又可以当 mcp 用是真实存在的）。board→模块的映射与发布兜底
    借用 ``store.derive_target_modules`` 同一份：两处各写必然漂移。

    * 命中 awesome 目录特征 → 仅浏览（installable=false）
    * frameworks / research → 仅浏览，开发库/研究程序不是编辑器扩展
    * 推不出任何形态 → 仅浏览，等管理员在资源编辑里勾选
    """
    import marketplace_leaderboard_store as store

    full_name = str(repo.get("full_name") or "")
    text = f"{full_name} {repo.get('description') or ''}"

    if _DIRECTORY_RE.search(text) or board in _BROWSE_ONLY_BOARDS:
        return {"target_module": "", "target_modules": [], "installable": False}

    modules = store.derive_target_modules(board, str(repo.get("category") or ""))
    if not modules:
        # 认不出形态的可安装条目没有意义（用户侧不知道往哪装），落仅浏览等人工勾选。
        return {"target_module": "", "target_modules": [], "installable": False}
    return {
        "target_module": modules[0],
        "target_modules": modules,
        "installable": True,
    }


def default_install_spec(modules: list[str], repo_full_name: str) -> dict:
    """按分类生成默认安装参数：同步即建完整草稿，省得管理员每条手填。

    skill → github_clone + 子路径空 + ref main；plugin → 仓库 main 分支 tarball +
    provider claude；prompt → 空内容（正文需读 README，不能凭空生成）；mcp 走
    launch_spec（agent 识别），不进 install_spec。
    """
    spec: dict[str, Any] = {}
    if "skill" in modules:
        spec["skill"] = {"install_method": "github_clone", "path": "", "ref": "main"}
    if "plugin" in modules:
        spec["plugin"] = {
            # 插件 URL 导入器只接受 zip（package.json + entry），不要用 tar.gz。
            "download_url": f"https://github.com/{repo_full_name}/archive/refs/heads/main.zip",
            "provider": "claude",
        }
    if "prompt" in modules:
        spec["prompt"] = {"content": ""}
    return spec


def _version_from_ts(value: Any) -> str:
    """上游更新时间 → 版本号 YYYY.MM.DD。上游没给时回 latest。"""
    ts = _parse_ts(value)
    return ts.strftime("%Y.%m.%d") if ts else "latest"


def build_external_data(source: str, board: str, repo: dict, rank: int | None) -> dict:
    """组装 external_data：**唯一上游真相**，只读留档。

    同步遇到已存在条目时只更新这一份；资源字段全部从这里派生（首次入池时），
    之后靠编辑弹框里逐项的「同步」按钮主动拉取，不再被同步自动改写。
    """
    full_name = str(repo.get("full_name") or "").strip()
    ts = _parse_ts(repo.get("updated_at"))
    return {
        "source": source,
        "board": board,
        "repo_full_name": full_name,
        "repo_url": str(repo.get("url") or f"https://github.com/{full_name}"),
        "description": str(repo.get("description") or ""),
        "upstream_category": str(repo.get("category") or ""),
        "language": str(repo.get("language") or ""),
        "topics": repo.get("topics") or [],
        "use_cases": repo.get("use_cases") or [],
        "stars": int(repo.get("stars") or 0),
        "forks": int(repo.get("forks") or 0),
        "upstream_rank": int(rank) if rank is not None else None,
        "upstream_updated_at": ts.isoformat() if ts else None,
        "version": _version_from_ts(repo.get("updated_at")),
        "raw": repo,
    }


def derive_resource_fields(external: dict, modules: list[str]) -> dict:
    """从 external_data 派生资源字段（首次入池时写入；之后逐项「同步」按钮复用）。

    映射口径（用户确认）：
    - use_cases → categories（子分类，多选）
    - topics    → tags（标签）
    - 名称=仓库短名；显示名=仓库名；摘要=上游描述；发布者=owner；版本=更新日期
    """
    full_name = str(external.get("repo_full_name") or "")
    owner, _, short = full_name.partition("/")
    return {
        "name": short or full_name,
        "display_name": short or full_name,
        "description": str(external.get("description") or ""),
        "publisher": owner,
        "version": str(external.get("version") or "latest"),
        "categories": [str(c).strip() for c in (external.get("use_cases") or []) if str(c).strip()],
        "tags": [str(t).strip() for t in (external.get("topics") or []) if str(t).strip()],
        "install_spec": default_install_spec(modules, full_name),
    }


def _assemble_item(source: str, board: str, repo: dict, rank: int | None, verdict: dict) -> dict:
    """把仓库记录 + 分类判定落成候选池条目（同步与手动添加共用，避免漂移）。

    顺序即口径：**先组装 external_data，再据它派生资源字段**。
    """
    full_name = str(repo.get("full_name") or "").strip()
    modules = verdict.get("target_modules") or []
    external = build_external_data(source, board, repo, rank)
    # external_data 保留「同步当时的派生默认」供编辑弹框逐项同步按钮使用；
    # 之后重新同步只更新 external_data，不自动改资源字段。
    external["target_modules"] = modules
    external["categories"] = external.get("use_cases") or []
    external["tags"] = external.get("topics") or []
    return {
        "source": source,
        "board": board,
        "repo_full_name": full_name,
        "repo_url": external["repo_url"],
        # 上游列（列表/搜索用的投影缓存，真相在 external_data）
        "description": external["description"],
        "stars": external["stars"],
        "forks": external["forks"],
        "language": external["language"],
        "topics": external["topics"],
        "upstream_category": external["upstream_category"],
        "use_cases": external["use_cases"],
        "upstream_rank": external["upstream_rank"],
        "upstream_updated_at": _parse_ts(repo.get("updated_at")),
        "sort_order": external["upstream_rank"],
        "external_data": external,
        # 派生资源字段
        **derive_resource_fields(external, modules),
        **verdict,
    }


def to_item(board: str, repo: dict, rank: int | None = None) -> dict | None:
    """把上游一条仓库记录落成候选池条目。

    ``rank`` 是条目在上游 board 文件 ``repos[]`` 里的位置（1=榜首），即**上游名次**。
    它只作为我们名次的默认值落库（管理员手动定过名次的行同步不跟随），不改归类。
    """
    full_name = str(repo.get("full_name") or "").strip()
    if not full_name or "/" not in full_name:
        return None
    return _assemble_item("agent-leaderboard", board, repo, rank, classify(board, repo))


async def fetch_repo_metadata(full_name: str) -> dict:
    """手动添加：查单个 GitHub 仓库的元数据。出站同 ``_fetch_board``（经 proxy_manager）。"""
    from providers.proxy_manager import get_proxy_manager

    proxy_id = mp_config.settings.proxy_id or None
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ai-lubricant-leaderboard-sync"}
    token = mp_config.settings.github_token
    if token:
        headers["Authorization"] = f"token {token}"

    manager = get_proxy_manager()
    url = f"https://api.github.com/repos/{full_name}"
    resp = await manager.request(
        url=url, method="GET", headers=headers, timeout=_FETCH_TIMEOUT, proxy_config_id=proxy_id
    )
    if resp.status >= 400:
        raise RuntimeError(f"GET repo {full_name} HTTP {resp.status}")
    meta = await resp.json()
    if not isinstance(meta, dict):
        raise RuntimeError(f"repo {full_name}: unexpected payload")
    return meta


def manual_item(full_name: str, meta: dict) -> dict | None:
    """手动添加的 GitHub 项目 → 与同步条目同形态的草稿（source='manual'）。

    分类默认从仓库 topics/描述里挑 mcp/skill/plugin/prompt 关键词（可能与同步
    一致的推导），没命中则留空等管理员在编辑弹框里勾选。
    """
    if not full_name or "/" not in full_name:
        return None
    repo = {
        "full_name": full_name,
        "url": str(meta.get("html_url") or f"https://github.com/{full_name}"),
        "description": meta.get("description") or "",
        "stars": meta.get("stargazers_count") or 0,
        "forks": meta.get("forks_count") or 0,
        "language": meta.get("language") or "",
        "topics": meta.get("topics") or [],
        "category": " ".join(meta.get("topics") or []),
        "use_cases": [],
        "updated_at": meta.get("updated_at"),
    }
    return _assemble_item("manual", "manual", repo, None, classify("manual", repo))


async def sync_once() -> dict[str, Any]:
    """跑一次全量同步。关闭时直接返回，不建连接、不写库。"""
    global _last_result

    if not await is_enabled():
        result = {"ok": False, "skipped": True, "detail": "外部榜单同步未启用"}
        return result

    import marketplace_leaderboard_store as store

    source = await _settings()
    repo = _repo(source)
    boards = resolve_boards(source)
    if not boards:
        return {"ok": False, "skipped": True, "detail": "未配置要同步的 board"}

    async with _lock:
        total = 0
        per_board: dict[str, int] = {}
        errors: list[str] = []
        async with aiohttp.ClientSession(timeout=_FETCH_TIMEOUT) as session:
            for board in boards:
                path = BOARD_FILES[board]
                try:
                    payload = await _fetch_board(session, repo, path)
                except Exception as exc:  # noqa: BLE001 — 单 board 失败不拖垮其余
                    errors.append(f"{board}: {exc}")
                    logger.warning("[leaderboard] fetch {} failed: {}", board, exc)
                    continue
                repos = payload.get("repos") if isinstance(payload, dict) else None
                if not isinstance(repos, list):
                    errors.append(f"{board}: 上游没有 repos[]")
                    continue
                count = 0
                # board 文件里 repos[] 的顺序就是上游名次（1=榜首）；跳过的坏条目照样
                # 占位——名次反映文件里的真实位置，不因解析失败而前移。
                for position, entry in enumerate(repos, start=1):
                    if not isinstance(entry, dict):
                        continue
                    item = to_item(board, entry, position)
                    if item is None:
                        continue
                    try:
                        # 确定性 repo-shape 探针（.mcp.json/SKILL.md/插件 manifest/README 提示）：
                        # 结果落 external_data.probe（ON CONFLICT 随重同步保鲜），派生 spec 只填
                        # 新条目尚无的键——已有行资源字段仍冻结，重派生走「同步」按钮。探针失败
                        # 不挡同步（attach_probe 内部兜错）。
                        from . import leaderboard_probe
                        item = await leaderboard_probe.attach_probe(item)
                        await store.upsert_item(item)
                        count += 1
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"{board}/{item['repo_full_name']}: {exc}")
                per_board[board] = count
                total += count
                logger.info("[leaderboard] {} synced {} repos", board, count)

        ran_at = datetime.now(timezone.utc).isoformat()
        ok = total > 0 and not errors
        detail = f"同步 {total} 条（{', '.join(f'{k}:{v}' for k, v in per_board.items()) or '无'}）"
        if errors:
            detail += f"；{len(errors)} 项失败"
        _last_result = {"ran_at": ran_at, "ok": ok, "detail": detail}
        return {
            "ok": ok,
            "total": total,
            "per_board": per_board,
            "errors": errors[:20],
            "ran_at": ran_at,
            "detail": detail,
        }


async def sync_loop() -> None:
    """后台定时同步。开关关闭时空转（每小时复查一次配置，便于热开启）。

    每次同步跑完后顺带做一次已发布 launch_spec 巡检（remote 握手 / stdio registry
    HEAD）——remote 是"看起来对其实挂了"的重灾区，握手一次就识破并降级 failed。失败
    不影响下一次同步；未启用时两者都空转。
    """
    while True:
        try:
            if await is_enabled():
                source = await _settings()
                hours = int(source.get("leaderboard_sync_interval_hours") or 24)
                await sync_once()
                try:
                    from . import leaderboard_verify
                    await leaderboard_verify.verify_published_batch()
                except Exception as exc:  # noqa: BLE001 — 巡检失败不拖累同步
                    logger.warning("[leaderboard] verify patrol error: {}", exc)
                await asyncio.sleep(max(1, hours) * 3600)
                continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 循环绝不能因单次异常退出
            logger.warning("[leaderboard] sync loop error: {}", exc)
        await asyncio.sleep(3600)
