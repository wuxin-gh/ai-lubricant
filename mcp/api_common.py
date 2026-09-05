"""mcp 子路由共享的基础 helper 与常量。

各 api_* 子模块自带 ``APIRouter(prefix="/mcp")``，从此处导入跨模块复用的：
- manifest 加载（内置服务从 mcp_builtin 读 manifest.json，自定义套默认 manifest）
- 部署形态推导（derive_deploy_scope）
- 鉴权（_require_admin）、运行时展示地址（_runtime_settings / _runtime_public_base）
- 热重载（_reload_service / _reload_services）
- service_row_to_dict 向后兼容包装
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

import mcp_plugin_store
from fastapi import HTTPException


# ── Manifest 加载：内置 MCP 从代码目录读 manifest.json；自定义服务套默认 manifest ──

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BUILTIN_MANIFEST_DIRS = {
    "cdp-bridge": _REPO_ROOT / "mcp_builtin" / "cdp_bridge",
    "mail": _REPO_ROOT / "mcp_builtin" / "mail",
}


_DEFAULT_MANIFEST_ACTIONS = [
    {"id": "info", "label": "基础信息", "panel": "info"},
    {"id": "env", "label": "环境变量", "panel": "env"},
    {"id": "config", "label": "连接配置", "panel": "config"},
    {"id": "tools", "label": "工具列表", "panel": "tools"},
]


def _default_manifest(service: dict) -> dict:
    """自定义 MCP 没有自带 manifest 时套用的默认 manifest。"""
    return {
        "schema": 1,
        "name": service.get("name"),
        "display_name": service.get("display_name") or service.get("name"),
        "description": service.get("description") or "",
        "icon": None,
        "category": service.get("category") or "custom",
        "builtin_rules": {
            "default_installed": False,
            "removable": True,
            "editable_fields": [
                "display_name", "description", "enabled", "command",
                "args", "transport", "url", "env_template",
            ],
        },
        "home_actions": _DEFAULT_MANIFEST_ACTIONS,
        "panels": {
            "info": {"kind": "info", "read_only": False},
            "env": {"kind": "env", "read_only": False},
            "config": {
                "kind": "config",
                "read_only": True,
                "storage": {
                    "type": "api",
                    "get": "/mcp/runtime/services/{service_id}/client-config",
                },
                "copyable": True,
            },
            "tools": {"kind": "tools", "read_only": True},
        },
    }


def _load_builtin_manifest(name: str) -> dict | None:
    """从代码目录读 manifest.json；缺失返回 None。"""
    base = _BUILTIN_MANIFEST_DIRS.get(name)
    if not base:
        return None
    manifest_path = base / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as e:
        return {"_error": f"manifest.json 解析失败: {e}", "name": name}


def _manifest_for_service(service: dict) -> dict:
    """取服务的 manifest：内置优先读文件，没有则默认；自定义走默认。"""
    name = service.get("name")
    if name and service.get("builtin"):
        m = _load_builtin_manifest(name)
        if m and "_error" not in m:
            return m
        if m and "_error" in m:
            return m
    return _default_manifest(service)


def _service_manifest_dir(name: str) -> Path | None:
    """返回内置 MCP 的 manifest 所在目录（用于附件下载相对路径解析）。"""
    return _BUILTIN_MANIFEST_DIRS.get(name)


def _mask_token(t: str) -> str:
    if not t:
        return ""
    tail = t[-4:] if len(t) > 4 else ""
    return f"****{tail}"


# ── 部署形态推导 ──
#
# MCP 生态里混着两类东西，区分它们是「这个进程在哪里跑」的客观事实，不是策略选择：
#
#   进程型（stdio）：npx -y @playwright/mcp、filesystem、git MCP。价值就在于操作本地
#     环境（读工作区、控本机浏览器、跑本地 git）。放服务端则操作的是服务端文件系统，
#     功能直接失效 —— 所以它天生属于节点，由编辑器 CLI 自己拉起（deploy_scope=session）。
#   服务型（remote http/sse）：无状态、无本地依赖，放哪都一样。因此服务端统一接一次，
#     agent 与所有编辑器共用一份配置/token/审计（deploy_scope=server）。
#
# 因此不给使用者「部署到哪」的选择题：stdio 选服务端会操作错机器，remote 选会话是
# 重复连同一端点，两个错误组合都没有正当用途。想让 stdio 全局可用，走 node_hosted
# （节点上托管 + 经节点隧道代理成 remote），而不是在服务端造 stdio 沙箱。

DEPLOY_SCOPE_SERVER = "server"
DEPLOY_SCOPE_SESSION = "session"
DEPLOY_SCOPE_NODE_HOSTED = "node_hosted"

_REMOTE_TRANSPORTS = {"sse", "streamable-http", "http", "websocket", "ws"}


def derive_deploy_scope(transport: str | None, url: str | None = None) -> str:
    """按 transport 客观推导部署形态。remote 类 → server；stdio 类 → session。

    url 只作兜底：有 url 却没写明 transport 的，按远程处理（历史数据里存在这种行）。
    """
    t = (transport or "").strip().lower()
    if t in _REMOTE_TRANSPORTS:
        return DEPLOY_SCOPE_SERVER
    if t == "stdio":
        return DEPLOY_SCOPE_SESSION
    return DEPLOY_SCOPE_SERVER if (url or "").strip() else DEPLOY_SCOPE_SESSION


def _service_row_to_dict(row) -> dict:
    """保留向后兼容：旧代码可能从外部引用。新代码请直接用 mcp_plugin_store.service_row_to_dict。"""
    return mcp_plugin_store.service_row_to_dict(row)


# ── 共享 principal/param 模型 ──
# 管理端 /mcp/users 与用户侧 /api/v1/users/mcp-principals 的 param 模型一致，统一
# 定义在此，两套路由共用。principal 自带操作参数（cdp_client_id / mail_account_id /
# term_id 等），不再用 grants 树表达子权限。

class PrincipalParamReq(BaseModel):
    param_key: str
    param_value: str


class ReplacePrincipalParamsReq(BaseModel):
    params: list[PrincipalParamReq] = Field(default_factory=list)


class SetPrincipalTokenStatusReq(BaseModel):
    status: str  # active | disabled


async def _resync_builtin_principals() -> None:
    """principal 参数替换后，把新快照推给活着的 CDP/邮箱 driver。

    param 变了要重下发，确保活跃 driver 的客户端/邮箱资源与刚校验过的参数一致。
    管理端 / 用户侧 replace_params 都调它，只写一处。
    """
    from mcp_runtime.configuration import resync_builtin_service
    await resync_builtin_service("cdp-bridge")
    await resync_builtin_service("mail")


# ── 鉴权与运行时设置 ──

async def _require_admin(authorization: str | None) -> None:
    from admin import _require_admin as _ra
    await _ra(authorization)


def _runtime_settings() -> dict:
    """读取 MCP 连接展示设置（对外拼接 SSE / CDP WS 地址用）。

    MCP Runtime 已合并进主程序：SSE 网关与 CDP WS 会话（/mcp/cdp-bridge/session）都
    挂在**主服务端口**上，没有独立进程端口。故默认端口跟随主服务端口（env.ini [server]
    port），不再有独立 runtime 端口可配。admin 可另存 mcp_runtime.port/display_host 覆盖，
    用于外部经反向代理接入时展示与内部监听不同的对外端口 / IP。
    """
    try:
        import config
        main_config = config.CONFIG_STORE.read_main()
    except Exception:
        main_config = {}
    saved = main_config.get("mcp_runtime") or {}
    server_port = int((main_config.get("server") or {}).get("port") or 8001)
    default_host = "127.0.0.1"
    port = int(saved.get("port") or server_port)
    display_host = str(saved.get("display_host") or default_host).strip() or default_host
    return {"port": port, "display_host": display_host}


def _runtime_public_base() -> str:
    """返回第三方客户端连接配置中显示的地址。"""
    settings = _runtime_settings()
    host = settings["display_host"]
    return f"http://{host}:{settings['port']}"


async def _reload_service(service_id: int, authorization: str | None) -> Any:
    """热重载 MCP 服务运行配置（env/auth/manifest），失败不回滚已保存的 DB。

    Runtime 已在进程内，直接调 admin_api.reload_service_core；不再走 HTTP 自代理。
    失败时把结构化原因回传给前端（reload_skipped），而非吞成 503——保存配置不应
    因 Runtime 临时不可达而回滚 DB，但要让前端看到 reload 没成功。
    """
    from mcp_runtime.admin_api import reload_service_core
    try:
        return await reload_service_core(service_id)
    except HTTPException as e:
        return {"ok": False, "status": "reload_skipped", "error": str(e.detail)}
    except Exception as e:
        return {"ok": False, "status": "reload_skipped", "error": str(e)}


async def _reload_services(service_ids, authorization: str | None) -> list[Any]:
    """批量热重载多个服务（set_market_user_services 授权变更后逐个重下发）。

    单个失败不阻断其余：每个服务的 reload 结果独立返回。"""
    results: list[Any] = []
    for sid in service_ids:
        results.append(await _reload_service(int(sid), authorization))
    return results
