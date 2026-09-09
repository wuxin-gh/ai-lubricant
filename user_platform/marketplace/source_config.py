"""市场仓库源配置（DB 主配置为真相源，环境变量作首次部署兜底）。

市场仓库是 MCP / 插件 / Skill / 项目提示词 / 渠道模板 / 节点程序版本的**同一个**
GitHub 仓库。因此这里的一份配置同时决定：

- 市场展示与管理读哪个仓库（前端直读 raw，服务端写用 token）
- 渠道目录（``channel_catalog``）从哪个仓库同步、是否自动同步、多久同步一次
- 节点程序版本（``node-versions`` 模块）从哪个仓库的 Release 资产下载

**仓库地址与 GitHub token 已改为只在服务端 ``.env`` 配置**（``MARKETPLACE_REPO_URL``
/ ``MARKETPLACE_GITHUB_TOKEN``），本模块不再存取这两项：历史 DB 值读取时被忽略，
下一次保存其它配置字段时被自然清除。分支 / modules / index_name / 代理仍存
DB 主配置 blob 的 ``marketplace`` key，可在管理端「资源中心 → 配置」里改（代理），
改完热更新，不需要重启。

优先级：DB 主配置 > 环境变量 / ``.env`` > 内置默认。
内置默认仓库地址是 ``DEFAULT_REPO_URL``，因此全新部署不配任何东西也能看市场。
token 只保存在服务端，读接口一律不回明文。
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

DEFAULT_REPO_URL = "https://github.com/wuxin-gh/ai-lubricant-marketplace"
DEFAULT_BRANCH = "main"
DEFAULT_INDEX_NAME = "index.json"
DEFAULT_MODULES = "mcp,plugins,skills,channels,prompts,node-versions"
DEFAULT_SYNC_INTERVAL_MINUTES = 10

# ── 外部榜单同步（Agent-Leaderboard 这类外部目录）──
# 默认全关：只有显式配置了市场管理并打开开关的部署才会跑同步。其余部署（绝大多数）
# 只消费已配置的市场仓库，不自己抓外部榜单，也不需要这些配置项存在。
DEFAULT_LEADERBOARD_REPO = "jaychempan/Agent-Leaderboard"
DEFAULT_LEADERBOARD_BOARDS = "skills,mcp,prompts,frameworks,research"
DEFAULT_LEADERBOARD_INTERVAL_HOURS = 24

# agency-agents 提示词源（英文/中文社区）：GitHub tarball，自带 frontmatter + divisions.json。
AGENCY_AGENTS_REPO = "msitarzewski/agency-agents"
AGENCY_AGENTS_ZH_REPO = "jnMetaCode/agency-agents-zh"
AGENCY_AGENTS_DEFAULT_REF = "3c9588880b7cafaec325a104899fd8bbe27e7d72"
AGENCY_AGENTS_INTERVAL_HOURS = 24

# agentscope 技能源：公开 API（/api/v1/skills），无需鉴权，分页拉列表 + zip 下载。
AGENTSCOPE_BASE = "https://platform.agentscope.io"
AGENTSCOPE_INTERVAL_HOURS = 24

_CONFIG_KEY = "marketplace"

# 字符串项：DB 缺失时留空，由 marketplace.config 回落到 env/ini/内置默认。
# 注意 repo_url / github_token 已不在其中——这两项只认 .env，历史 DB 值读取时被
# _normalize 丢弃，下一次保存任何配置时被自然清除。
_STRING_KEYS = (
    "github_branch", "modules", "index_name", "proxy_id",
    # 消费侧（读）坐标与平台：填 Gitee 镜像地址即整条读链路切到 Gitee（生产写侧
    # 恒为 GitHub）。留空回落主仓库坐标与 github 平台。
    "consumer_repo_url", "consumer_branch", "consumer_platform",
    # 外部榜单同步：源仓库与要同步的 board 列表（留空回落内置默认）。
    "leaderboard_repo", "leaderboard_boards",
    # 各内容源出网代理（留空回落上面的 proxy_id 全局代理）：
    # agentscope 是公开国内 API，不单独配代理，恒走全局；其余三个源可各自指定。
    "leaderboard_proxy_id", "agency_agents_proxy_id", "agency_agents_zh_proxy_id",
)


def _normalize(data: dict[str, Any] | None) -> dict[str, Any]:
    raw = data if isinstance(data, dict) else {}
    merged: dict[str, Any] = {key: str(raw.get(key, "") or "").strip() for key in _STRING_KEYS}
    auto_sync = raw.get("auto_sync_enabled", True)
    merged["auto_sync_enabled"] = auto_sync is not False
    try:
        interval = int(raw.get("sync_interval_minutes") or DEFAULT_SYNC_INTERVAL_MINUTES)
    except (TypeError, ValueError):
        interval = DEFAULT_SYNC_INTERVAL_MINUTES
    merged["sync_interval_minutes"] = max(1, min(1440, interval))
    # 外部榜单同步默认关闭（opt-in）：未显式打开的部署不跑抓取，也不建候选池。
    merged["leaderboard_sync_enabled"] = raw.get("leaderboard_sync_enabled") is True
    try:
        hours = int(raw.get("leaderboard_sync_interval_hours") or DEFAULT_LEADERBOARD_INTERVAL_HOURS)
    except (TypeError, ValueError):
        hours = DEFAULT_LEADERBOARD_INTERVAL_HOURS
    merged["leaderboard_sync_interval_hours"] = max(1, min(720, hours))
    # MCP launch_spec 发布门禁（软+开关，默认软=不强制 verified）：开=可安装 MCP 条目
    # 发布前必须 verified（真握手成功过）；关=filled/verified 都放行，verified 只是徽标。
    merged["leaderboard_require_verified"] = raw.get("leaderboard_require_verified") is True
    # ── 内容源同步（agency-agents / agency-agents-zh / agentscope）──
    # 每个源独立开关/间隔/ref；默认全关。
    merged["agency_agents_enabled"] = raw.get("agency_agents_enabled") is True
    try:
        aa_hours = int(raw.get("agency_agents_interval_hours") or AGENCY_AGENTS_INTERVAL_HOURS)
    except (TypeError, ValueError):
        aa_hours = AGENCY_AGENTS_INTERVAL_HOURS
    merged["agency_agents_interval_hours"] = max(1, min(720, aa_hours))
    merged["agency_agents_ref"] = str(raw.get("agency_agents_ref") or "").strip()
    merged["agency_agents_zh_enabled"] = raw.get("agency_agents_zh_enabled") is True
    try:
        aaz_hours = int(raw.get("agency_agents_zh_interval_hours") or AGENCY_AGENTS_INTERVAL_HOURS)
    except (TypeError, ValueError):
        aaz_hours = AGENCY_AGENTS_INTERVAL_HOURS
    merged["agency_agents_zh_interval_hours"] = max(1, min(720, aaz_hours))
    merged["agency_agents_zh_ref"] = str(raw.get("agency_agents_zh_ref") or "").strip()
    merged["agentscope_enabled"] = raw.get("agentscope_enabled") is True
    try:
        as_hours = int(raw.get("agentscope_interval_hours") or AGENTSCOPE_INTERVAL_HOURS)
    except (TypeError, ValueError):
        as_hours = AGENTSCOPE_INTERVAL_HOURS
    merged["agentscope_interval_hours"] = max(1, min(720, as_hours))
    # 最近一次同步时间（同步收尾时由 record_source_sync 写入，跟配置同一 blob）。
    # 内存 _last_result 重启即失，这里持久化让面板重启后仍能显示「上次 X 时间同步过」。
    for _k in (
        "leaderboard_last_sync_at", "agency_agents_last_sync_at",
        "agency_agents_zh_last_sync_at", "agentscope_last_sync_at",
    ):
        merged[_k] = str(raw.get(_k) or "").strip()
    return merged


# source → 本源专用 proxy_id 字段名 / last_sync_at 字段名 / 同步记录名（与 sync_run_store 对齐）。
_SOURCE_KEYS: dict[str, tuple[str, str]] = {
    "agent-leaderboard": ("leaderboard_proxy_id", "leaderboard_last_sync_at"),
    "agency-agents": ("agency_agents_proxy_id", "agency_agents_last_sync_at"),
    "agency-agents-zh": ("agency_agents_zh_proxy_id", "agency_agents_zh_last_sync_at"),
    "agentscope": ("", "agentscope_last_sync_at"),  # agentscope 不单独配代理，恒走全局
}


def effective_proxy_id(source: dict[str, Any], source_key: str) -> str:
    """源专用代理（留空）→ 回落全局 ``proxy_id``。返回 ""=直连。"""
    own = str(source.get(source_key) or "").strip() if source_key else ""
    return own or str(source.get("proxy_id") or "").strip()


async def record_source_sync(source: str, ran_at_iso: str) -> None:
    """同步收尾时把「上次同步时间」写进配置 blob（跟其余配置字段同表）。"""
    if source not in _SOURCE_KEYS:
        return
    last_key = _SOURCE_KEYS[source][1]
    await update_source_config({last_key: ran_at_iso})


def _read_config_blob() -> dict[str, Any]:
    """同步读主配置；配置层不可用时当作空（回落 env/ini）。"""
    try:
        from config import CONFIG_STORE

        data = CONFIG_STORE.read_main()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


async def _read_config_blob_async() -> dict[str, Any]:
    try:
        from config import CONFIG_STORE

        if hasattr(CONFIG_STORE, "read_main_async"):
            data = await CONFIG_STORE.read_main_async()
        else:
            data = CONFIG_STORE.read_main()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def get_source_config() -> dict[str, Any]:
    """DB 里保存的市场源配置（未配的字符串项为空串，由调用方回落）。"""
    return _normalize(_read_config_blob().get(_CONFIG_KEY))


async def get_source_config_async() -> dict[str, Any]:
    blob = await _read_config_blob_async()
    return _normalize(blob.get(_CONFIG_KEY))


async def update_source_config(patch: dict[str, Any]) -> dict[str, Any]:
    """按字段合并保存（部分更新，未传的字段不动）。

    repo_url / github_token 不再被接受：patch 里出现也忽略——这两项只认 .env。
    """
    current = await get_source_config_async()
    merged = deepcopy(current)
    for key in _STRING_KEYS:
        if key not in patch:
            continue
        merged[key] = str(patch.get(key) or "").strip()
    if "auto_sync_enabled" in patch:
        merged["auto_sync_enabled"] = patch.get("auto_sync_enabled") is not False
    if "sync_interval_minutes" in patch:
        try:
            interval = int(patch.get("sync_interval_minutes") or DEFAULT_SYNC_INTERVAL_MINUTES)
        except (TypeError, ValueError):
            interval = DEFAULT_SYNC_INTERVAL_MINUTES
        merged["sync_interval_minutes"] = max(1, min(1440, interval))
    if "leaderboard_sync_enabled" in patch:
        merged["leaderboard_sync_enabled"] = patch.get("leaderboard_sync_enabled") is True
    if "leaderboard_sync_interval_hours" in patch:
        try:
            hours = int(patch.get("leaderboard_sync_interval_hours") or DEFAULT_LEADERBOARD_INTERVAL_HOURS)
        except (TypeError, ValueError):
            hours = DEFAULT_LEADERBOARD_INTERVAL_HOURS
        merged["leaderboard_sync_interval_hours"] = max(1, min(720, hours))
    if "leaderboard_require_verified" in patch:
        merged["leaderboard_require_verified"] = patch.get("leaderboard_require_verified") is True
    # ── 内容源同步（agency-agents / agency-agents-zh / agentscope）──
    if "agency_agents_enabled" in patch:
        merged["agency_agents_enabled"] = patch.get("agency_agents_enabled") is True
    if "agency_agents_interval_hours" in patch:
        try:
            aa_hours = int(patch.get("agency_agents_interval_hours") or AGENCY_AGENTS_INTERVAL_HOURS)
        except (TypeError, ValueError):
            aa_hours = AGENCY_AGENTS_INTERVAL_HOURS
        merged["agency_agents_interval_hours"] = max(1, min(720, aa_hours))
    if "agency_agents_ref" in patch:
        merged["agency_agents_ref"] = str(patch.get("agency_agents_ref") or "").strip()
    if "agency_agents_zh_enabled" in patch:
        merged["agency_agents_zh_enabled"] = patch.get("agency_agents_zh_enabled") is True
    if "agency_agents_zh_interval_hours" in patch:
        try:
            aaz_hours = int(patch.get("agency_agents_zh_interval_hours") or AGENCY_AGENTS_INTERVAL_HOURS)
        except (TypeError, ValueError):
            aaz_hours = AGENCY_AGENTS_INTERVAL_HOURS
        merged["agency_agents_zh_interval_hours"] = max(1, min(720, aaz_hours))
    if "agency_agents_zh_ref" in patch:
        merged["agency_agents_zh_ref"] = str(patch.get("agency_agents_zh_ref") or "").strip()
    if "agentscope_enabled" in patch:
        merged["agentscope_enabled"] = patch.get("agentscope_enabled") is True
    if "agentscope_interval_hours" in patch:
        try:
            as_hours = int(patch.get("agentscope_interval_hours") or AGENTSCOPE_INTERVAL_HOURS)
        except (TypeError, ValueError):
            as_hours = AGENTSCOPE_INTERVAL_HOURS
        merged["agentscope_interval_hours"] = max(1, min(720, as_hours))

    blob = await _read_config_blob_async()
    blob[_CONFIG_KEY] = merged

    from config import CONFIG_STORE

    if hasattr(CONFIG_STORE, "write_main_async"):
        await CONFIG_STORE.write_main_async(blob)
    else:
        CONFIG_STORE.write_main(blob)
    return merged


def public_view(source: dict[str, Any], resolved: dict[str, Any]) -> dict[str, Any]:
    """管理端读接口形态：DB 原值 + 已生效值。仓库地址看 ``effective``（env 解析结果）。"""
    return {
        "github_branch": source.get("github_branch") or "",
        "modules": source.get("modules") or "",
        "index_name": source.get("index_name") or "",
        "proxy_id": source.get("proxy_id") or "",
        "consumer_repo_url": source.get("consumer_repo_url") or "",
        "consumer_branch": source.get("consumer_branch") or "",
        "consumer_platform": source.get("consumer_platform") or "",
        "auto_sync_enabled": bool(source.get("auto_sync_enabled", True)),
        "sync_interval_minutes": int(source.get("sync_interval_minutes") or DEFAULT_SYNC_INTERVAL_MINUTES),
        # 外部榜单同步：默认关闭，回显内置默认供表单占位。
        "leaderboard_sync_enabled": bool(source.get("leaderboard_sync_enabled")),
        "leaderboard_sync_interval_hours": int(
            source.get("leaderboard_sync_interval_hours") or DEFAULT_LEADERBOARD_INTERVAL_HOURS
        ),
        "leaderboard_repo": source.get("leaderboard_repo") or "",
        "leaderboard_boards": source.get("leaderboard_boards") or "",
        # MCP launch_spec 发布门禁（软+开关，默认软）。
        "leaderboard_require_verified": bool(source.get("leaderboard_require_verified")),
        # ── 内容源同步（agency-agents / agency-agents-zh / agentscope）──
        "agency_agents_enabled": bool(source.get("agency_agents_enabled")),
        "agency_agents_interval_hours": int(source.get("agency_agents_interval_hours") or AGENCY_AGENTS_INTERVAL_HOURS),
        "agency_agents_ref": source.get("agency_agents_ref") or "",
        "agency_agents_zh_enabled": bool(source.get("agency_agents_zh_enabled")),
        "agency_agents_zh_interval_hours": int(source.get("agency_agents_zh_interval_hours") or AGENCY_AGENTS_INTERVAL_HOURS),
        "agency_agents_zh_ref": source.get("agency_agents_zh_ref") or "",
        "agentscope_enabled": bool(source.get("agentscope_enabled")),
        "agentscope_interval_hours": int(source.get("agentscope_interval_hours") or AGENTSCOPE_INTERVAL_HOURS),
        # 各源出网代理：留空回落 proxy_id 全局（effective_proxy_id 计算；agentscope 无此字段恒走全局）
        "leaderboard_proxy_id": source.get("leaderboard_proxy_id") or "",
        "agency_agents_proxy_id": source.get("agency_agents_proxy_id") or "",
        "agency_agents_zh_proxy_id": source.get("agency_agents_zh_proxy_id") or "",
        # 各源最近一次同步时间（同步收尾由 record_source_sync 写入，与配置同 blob 持久化）
        "leaderboard_last_sync_at": source.get("leaderboard_last_sync_at") or "",
        "agency_agents_last_sync_at": source.get("agency_agents_last_sync_at") or "",
        "agency_agents_zh_last_sync_at": source.get("agency_agents_zh_last_sync_at") or "",
        "agentscope_last_sync_at": source.get("agentscope_last_sync_at") or "",
        "default_leaderboard_repo": DEFAULT_LEADERBOARD_REPO,
        "default_leaderboard_boards": DEFAULT_LEADERBOARD_BOARDS,
        "effective": resolved,
    }
