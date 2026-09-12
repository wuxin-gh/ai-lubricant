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
import os
import re
from datetime import datetime, timezone
from typing import Any

import aiohttp
from loguru import logger

from . import config as mp_config
from ..git_clients import github_rate_limit_message, is_rate_limit_error
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
# 执行状态（内存即可，重启即失）：重复点击 / 定时循环重复触发时由路由据此返回 409。
# 进度字段供前端 status 端点轮询；finished_at/ok/detailed logs 由 sync_run_store 落 DB。
_progress: dict[str, Any] = {
    "running": False, "phase": "", "current": 0, "total": 0, "current_item": "",
}


def last_result() -> dict[str, Any]:
    return {**_last_result, "progress": dict(_progress)}


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


async def _fetch_board(session: aiohttp.ClientSession, repo: str, path: str, *, proxy_id: str = "") -> Any:
    """取一个 board 的 JSON。**raw 直链优先**（CDN、不占 api.github.com 配额），
    raw 拿不到再走 Contents/Blobs API——raw 在部分网络下不可达，API 主机通常
    可达，兜底是常态而非异常。Contents API 对超过 ~1MB 的文件只回 sha，需要再
    取 blob（board 文件普遍 1–16MB）。出站统一经 proxy_manager，代理取本源专用
    ``leaderboard_proxy_id``（留空回落全局 ``proxy_id``）。
    """
    # raw 优先：路径 + main 分支已知，board JSON 可大（>1MB Contents API 就要走
    # 两跳 blob），raw 一跳直出且不占 5000/h 配额。
    from . import leaderboard_probe

    raw = await leaderboard_probe.fetch_raw_text(
        repo, path, "main", proxy_id=proxy_id, max_bytes=_MAX_BYTES,
        timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT.total or 120),
    )
    if raw is not None:
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise RuntimeError(f"{path}: raw 内容不是合法 JSON: {exc}") from exc

    from providers.proxy_manager import get_proxy_manager

    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ai-lubricant-leaderboard-sync"}
    token = mp_config.settings.github_token
    if token:
        headers["Authorization"] = f"token {token}"

    pid = proxy_id or None
    manager = get_proxy_manager()
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref=main"
    resp = await manager.request(
        url=url, method="GET", headers=headers, timeout=_FETCH_TIMEOUT, proxy_config_id=pid
    )
    if resp.status >= 400:
        text = await resp.text()
        rate_limited = github_rate_limit_message(resp, text)
        if rate_limited:
            raise RuntimeError(rate_limited)
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
        url=blob_url, method="GET", headers=headers, timeout=_FETCH_TIMEOUT, proxy_config_id=pid
    )
    if resp.status >= 400:
        text = await resp.text()
        rate_limited = github_rate_limit_message(resp, text)
        if rate_limited:
            raise RuntimeError(rate_limited)
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


# ── 探针复用与预算护栏 ───────────────────────────────────────────────────────
# 探针（git-trees + contents，每条 1-10 次 API）是配额大头：一轮全量同步轻易打穿
# 认证 5000 次/小时，把上传/发布挤成 403。三道防线：没变化的条目零网络复用上轮
# 探针；探针前查免费端点 /rate_limit，余量不足只同步元数据；真撞限流由熔断停轮。


def _probe_reuse_max_age_days() -> float:
    """探针复用的年龄上限（天）。0 = 永不复用、每轮全量重探（兜底逃生门）。"""
    try:
        return max(0.0, float(os.getenv("LEADERBOARD_PROBE_MAX_AGE_DAYS", "7")))
    except ValueError:
        return 7.0


def _probe_min_budget() -> int:
    """探针前要求的最小 API 余量；低于则本轮不新探（元数据同步照常）。"""
    try:
        return max(0, int(os.getenv("LEADERBOARD_PROBE_MIN_BUDGET", "500")))
    except ValueError:
        return 500


def _probe_reusable(hint: dict | None, external: dict) -> bool:
    """上轮探针能否零网络复用：上游 ``updated_at`` 没变 + 探针健康 + 未超龄。

    探针结果是 repo 状态的纯函数——上游没动就没有信息可刷新；上游真动而榜单
    ``updated_at`` 滞后的，由年龄上限兜底（超龄强制重探）。
    """
    if not isinstance(hint, dict):
        return False
    if hint.get("probe_error"):
        return False  # 上轮探针失败（含限流）的条目永远重试
    if _probe_reuse_max_age_days() <= 0:
        return False
    ts = _parse_ts(hint.get("fetched_at"))
    if ts is None:
        return False
    if (datetime.now(timezone.utc) - ts).total_seconds() > _probe_reuse_max_age_days() * 86400:
        return False
    return str(hint.get("upstream_updated_at") or "") == str(external.get("upstream_updated_at") or "")


async def _probe_budget_remaining(proxy_id: str) -> int | None:
    """查 GitHub core 配额余量。``/rate_limit`` 是免费端点（不计配额）。

    读不到（网络/代理异常）返回 None——视作「不预检」，探针照常（熔断仍兜底）。
    """
    from . import leaderboard_probe

    try:
        data = await leaderboard_probe.get_json("https://api.github.com/rate_limit", proxy_id=proxy_id)
    except Exception:  # noqa: BLE001 — 预检失败不挡同步
        return None
    try:
        return int(((data or {}).get("resources") or {}).get("core", {}).get("remaining"))
    except (TypeError, ValueError, AttributeError):
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
    """手动添加：查单个 GitHub 仓库的元数据。出站同 ``_fetch_board``（经 proxy_manager），
    走本源专属 ``leaderboard_proxy_id``（留空回落全局 ``proxy_id``），与 sync_once 同口径。
    """
    from providers.proxy_manager import get_proxy_manager
    from .source_config import get_source_config_async, effective_proxy_id

    source = await get_source_config_async()
    proxy_id = effective_proxy_id(source, "leaderboard_proxy_id")
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ai-lubricant-leaderboard-sync"}
    token = mp_config.settings.github_token
    if token:
        headers["Authorization"] = f"token {token}"

    manager = get_proxy_manager()
    url = f"https://api.github.com/repos/{full_name}"
    resp = await manager.request(
        url=url, method="GET", headers=headers, timeout=_FETCH_TIMEOUT, proxy_config_id=(proxy_id or None)
    )
    if resp.status >= 400:
        text = await resp.text()
        rate_limited = github_rate_limit_message(resp, text)
        if rate_limited:
            raise RuntimeError(rate_limited)
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


async def sync_once(
    run_by: str = "schedule",
    *,
    overwrite_published: bool = False,
    overwrite_draft: bool = False,
) -> dict[str, Any]:
    """跑一次全量同步。关闭时直接返回，不建连接、不写库。

    ``run_by``：schedule=定时循环触发；manual=管理端「立即同步」。只进执行记录，不影响行为。

    覆盖模式（manual 弹框勾选，默认全不勾=不覆盖；定时循环不传恒为不覆盖）：
    - ``overwrite_draft`` / ``overwrite_published``：已存在行的资源字段（名称/
      描述/版本，带新鲜探针时含分类/安装配置）按上游+探针最新值重写，管理员手改
      让位；详见 ``store.upsert_item`` 的覆盖语义。状态不翻——覆盖是刷新数据，
      不是隐式发布/撤回。
    """
    global _last_result

    if not await is_enabled():
        result = {"ok": False, "skipped": True, "detail": "外部榜单同步未启用"}
        return result

    # 执行状态（内存）：已在跑直接返回，不排队不重复跑。路由层也拦一次，这里是
    # 兜底（定时循环与手动点击共用本函数）。
    if _progress.get("running"):
        return {"ok": False, "skipped": True, "detail": "已有同步在运行中"}

    import marketplace_leaderboard_store as store

    overwrite_on = overwrite_published or overwrite_draft

    source = await _settings()
    repo = _repo(source)
    boards = resolve_boards(source)
    if not boards:
        return {"ok": False, "skipped": True, "detail": "未配置要同步的 board"}
    # 本源专用代理（留空回落全局 proxy_id）；探针出网同源。
    from .source_config import effective_proxy_id
    proxy_id = effective_proxy_id(source, "leaderboard_proxy_id")

    _progress.update({"running": True, "phase": "fetch", "current": 0, "total": 0, "current_item": ""})
    logs: list[dict[str, str]] = []

    def _log(phase: str, text: str) -> None:
        logs.append({"ts": datetime.now(timezone.utc).isoformat(), "phase": phase, "detail": text})

    try:
        async with _lock:
            total = 0
            per_board: dict[str, int] = {}
            errors: list[str] = []
            rate_limited_msg = ""
            reused = probed = budget_skipped = 0
            # 探针预算预检：/rate_limit 免费端点。余量不足时本轮只同步元数据
            # （有旧 probe 由 upsert 保留规则原样带回），探针留给下轮——避免把
            # 配额烧在批量探测上，把额度留给真正需要的上传/发布。
            probe_budget_ok = True
            remaining = await _probe_budget_remaining(proxy_id)
            if remaining is not None and remaining < _probe_min_budget():
                probe_budget_ok = False
                _log(
                    "skip",
                    f"GitHub API 余量 {remaining}（阈值 {_probe_min_budget()}），本轮跳过新探针只同步元数据",
                )
                logger.warning("[leaderboard] probe budget low: remaining {}", remaining)
            # 复用快照：上轮探针的轻量投影，零网络复用判定依据。
            probe_reuse = await store.load_probe_reuse_hints()
            async with aiohttp.ClientSession(timeout=_FETCH_TIMEOUT) as session:
                for board in boards:
                    path = BOARD_FILES[board]
                    _log("fetch", f"拉取 board {board}（{path}）")
                    _progress.update({"phase": "fetch", "current_item": board})
                    try:
                        payload = await _fetch_board(session, repo, path, proxy_id=proxy_id)
                    except Exception as exc:  # noqa: BLE001 — 单 board 失败不拖垮其余
                        errors.append(f"{board}: {exc}")
                        _log("fail", f"{board}: {exc}")
                        logger.warning("[leaderboard] fetch {} failed: {}", board, exc)
                        if is_rate_limit_error(exc):
                            rate_limited_msg = str(exc)
                            break
                        continue
                    repos = payload.get("repos") if isinstance(payload, dict) else None
                    if not isinstance(repos, list):
                        errors.append(f"{board}: 上游没有 repos[]")
                        _log("fail", f"{board}: 上游没有 repos[]")
                        continue
                    count = 0
                    _progress.update({"phase": "upsert", "total": len(repos), "current": 0})
                    # board 文件里 repos[] 的顺序就是上游名次（1=榜首）；跳过的坏条目照样
                    # 占位——名次反映文件里的真实位置，不因解析失败而前移。
                    for position, entry in enumerate(repos, start=1):
                        if not isinstance(entry, dict):
                            continue
                        item = to_item(board, entry, position)
                        if item is None:
                            continue
                        _progress.update({
                            "current": position, "current_item": f"{board}/{item['repo_full_name']}",
                        })
                        external = item["external_data"]
                        hint = probe_reuse.get((item["source"], board, item["repo_full_name"]))
                        prev_stack = hint.get("stack") or {} if isinstance(hint, dict) else {}
                        prev_tags = hint.get("stack_tags") or [] if isinstance(hint, dict) else []
                        hit_rate_limit = ""
                        try:
                            from . import leaderboard_probe
                            if _probe_reusable(hint, external):
                                # 上游没变 + 上轮探针健康且未超龄：零网络复用。probe 本体
                                # 不带进 external_data——upsert ON CONFLICT 保留规则把行里旧
                                # probe 自动移植；stack/tags 顺带带回写列。
                                item["stack"] = prev_stack
                                item["stack_tags"] = prev_tags
                                reused += 1
                            elif not probe_budget_ok:
                                # 预算不足：旧 probe 由 upsert 保留规则带回（宁 stale 勿丢），
                                # stack 带回；新条目本轮不探，下轮自动补。
                                item["stack"] = prev_stack
                                item["stack_tags"] = prev_tags
                                budget_skipped += 1
                            else:
                                item = await leaderboard_probe.attach_probe(item, proxy_id=proxy_id)
                                probed += 1
                                # 探针撞限流：错误标记是瞬态故障，pop 掉再落库——让 upsert
                                # 保留规则留住行里旧 probe，stack 用 hint 里的旧值兜住。
                                new_probe = (item.get("external_data") or {}).get("probe")
                                if isinstance(new_probe, dict) and is_rate_limit_error(str(new_probe.get("error") or "")):
                                    hit_rate_limit = str(new_probe.get("error"))
                                    item["external_data"].pop("probe", None)
                                    item["stack"] = prev_stack
                                    item["stack_tags"] = prev_tags
                            if overwrite_on:
                                await store.upsert_item(
                                    item,
                                    overwrite_published=overwrite_published,
                                    overwrite_draft=overwrite_draft,
                                )
                            else:
                                # 默认路径保持旧调用形态，兼容同步相关的轻量测试/适配器。
                                await store.upsert_item(item)
                            count += 1
                        except Exception as exc:  # noqa: BLE001
                            errors.append(f"{board}/{item['repo_full_name']}: {exc}")
                            _log("fail", f"{board}/{item['repo_full_name']}: {str(exc)[:150]}")
                        # 限流熔断：配额已耗尽——后面的探针每条还要打 1-10 次 API，继续只会
                        # 徒增 403 并可能触发滥用检测。停掉本轮探测：已 upsert 的保留，未探的
                        # 行 external_data.probe 维持上轮结果（下轮自动补）；执行记录给一条带
                        # 恢复时间的直白原因。
                        if hit_rate_limit:
                            rate_limited_msg = hit_rate_limit
                            skipped = len(repos) - position
                            msg = f"{board}: {rate_limited_msg}；跳过剩余 {skipped} 条探针（已有数据保留，下轮自动补）"
                            errors.append(msg)
                            _log("fail", msg)
                            break
                    per_board[board] = count
                    total += count
                    if rate_limited_msg:
                        break
                    _log("board_done", f"{board}：同步 {count} 条")
                    logger.info("[leaderboard] {} synced {} repos", board, count)

            ran_at = datetime.now(timezone.utc).isoformat()
            ok = total > 0 and not errors
            detail = f"同步 {total} 条（{', '.join(f'{k}:{v}' for k, v in per_board.items()) or '无'}）"
            probe_stat = f"探针复用 {reused}、新探 {probed}"
            if budget_skipped:
                probe_stat += f"、配额不足跳过 {budget_skipped}"
            detail += f"；{probe_stat}"
            if overwrite_on:
                mode = "、".join(filter(None, [
                    "已发布" if overwrite_published else "",
                    "草稿" if overwrite_draft else "",
                ]))
                detail += f"；覆盖模式（{mode}）"
            if errors:
                detail += f"；{len(errors)} 项失败"
            if rate_limited_msg:
                detail += f"；因 {rate_limited_msg} 提前结束"
            _log("done", detail)
            _last_result = {"ran_at": ran_at, "ok": ok, "detail": detail}
            # 执行记录落 DB（重启后面板仍能看到最新一份日志）+ 上次同步时间随配置持久化。
            try:
                import marketplace_sync_run_store
                await marketplace_sync_run_store.record_run(
                    "agent-leaderboard", ok=ok, detail=detail, logs=logs,
                    run_by=run_by,
                    counts={"total": total, "per_board": per_board, "errors": len(errors)},
                )
                from .source_config import record_source_sync
                await record_source_sync("agent-leaderboard", ran_at)
            except Exception as exc:  # noqa: BLE001 — 记录失败不影响同步结果
                logger.warning("[leaderboard] record run failed: {}", exc)
            return {
                "ok": ok,
                "total": total,
                "per_board": per_board,
                "errors": errors[:20],
                "ran_at": ran_at,
                "detail": detail,
            }
    finally:
        _progress.update({"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""})


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
