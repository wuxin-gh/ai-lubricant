"""MCP Runtime 代理路由：插件版本管理 + 安全审查 + 热加载 + 连接配置。

MCP Runtime 已合并进主程序：以下端点直接调 admin_api 的 *_core 函数，
不再经 HTTP 自代理（原 _runtime_proxy / 独立 8003 进程已退役）。
主服务直接调用进程内 core，复用 admin session 鉴权（router 层 _require_admin）。
"""
from __future__ import annotations

import mcp_plugin_store
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from .api_common import _require_admin, _runtime_public_base

router = APIRouter(prefix="/mcp", tags=["mcp"])


class CreateVersionPayload(BaseModel):
    code: str = ""
    config_json: dict = {}
    author: str = ""
    source: str = "manual"


@router.get("/runtime/services")
async def runtime_list_services(authorization: str | None = Header(None)):
    await _require_admin(authorization)
    return await mcp_plugin_store.list_services()


@router.get("/runtime/services/{service_id}/versions")
async def runtime_list_versions(service_id: int, authorization: str | None = Header(None)):
    await _require_admin(authorization)
    return await mcp_plugin_store.list_versions(service_id)


@router.post("/runtime/services/{service_id}/versions")
async def runtime_create_version(service_id: int, payload: CreateVersionPayload, authorization: str | None = Header(None)):
    await _require_admin(authorization)
    from mcp_runtime.admin_api import create_version_core
    return await create_version_core(service_id, payload)


@router.post("/runtime/versions/{version_id}/security-check")
async def runtime_security_check(version_id: int, authorization: str | None = Header(None)):
    await _require_admin(authorization)
    from mcp_runtime.security_review import run_security_check
    return await run_security_check(version_id)


@router.post("/runtime/versions/{version_id}/activate")
async def runtime_activate(version_id: int, authorization: str | None = Header(None)):
    await _require_admin(authorization)
    from mcp_runtime.admin_api import activate_core
    return await activate_core(version_id)


@router.post("/runtime/services/{service_id}/start")
async def runtime_start_service(service_id: int, authorization: str | None = Header(None)):
    """启动 MCP 服务：builtin 走 adapter，custom 走 active version 热加载。"""
    await _require_admin(authorization)
    from mcp_runtime.admin_api import start_service_core
    result = await start_service_core(service_id)
    if not result["ok"]:
        raise HTTPException(result.get("code", 400), result.get("error", "启动失败"))
    return result


@router.post("/runtime/services/{service_id}/stop")
async def runtime_stop_service(service_id: int, authorization: str | None = Header(None)):
    """停止 MCP 服务：从 runtime 卸载。"""
    await _require_admin(authorization)
    from mcp_runtime.admin_api import stop_service_core
    return await stop_service_core(service_id)


@router.post("/runtime/services/{service_id}/reload")
async def runtime_reload_service(service_id: int, authorization: str | None = Header(None)):
    """热重载 MCP 服务配置（env/auth/manifest），无需停止服务。"""
    await _require_admin(authorization)
    from mcp_runtime.admin_api import reload_service_core
    return await reload_service_core(service_id)


@router.get("/runtime/services/{service_id}/client-config")
async def runtime_client_config(service_id: int, authorization: str | None = Header(None)):
    """返回第三方客户端连接本 MCP 服务的 SSE 配置片段。"""
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    name = service["name"]
    base = _runtime_public_base()
    sse_url = f"{base}/mcp/{name}/sse"
    messages_endpoint = f"{base}/mcp/{name}/messages"
    # cdp-bridge 扩展的 WebSocket 接入地址（内置于 runtime，http(s)→ws(s)）。
    ws_session_url = ""
    if name == "cdp-bridge":
        ws_base = base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        ws_session_url = f"{ws_base}/mcp/{name}/session"
    auth = await mcp_plugin_store.get_service_auth(service_id)
    note_suffix = (
        " 鉴权已开启：连接时必须带 ?token=<MCP用户token> 或 Authorization: Bearer <token>，"
        "未授权 token 会被拒绝。"
        if auth["auth_enabled"]
        else " 该服务未开启鉴权，任何人可连接。"
    )

    def _configs_for(url: str) -> dict:
        # Claude Desktop 风格的 SSE 配置；其它 MCP 客户端字段名略有差异但结构一致。
        return {
            "claude_desktop": {"mcpServers": {name: {"url": url, "transport": "sse"}}},
            "cursor": {"mcpServers": {name: {"url": url, "transport": "sse"}}},
            "generic": {
                "transport": "sse",
                "url": url,
                "messages_endpoint": messages_endpoint,
                "note": "SSE 连接后，endpoint 事件会返回带 session_id 的 messages 地址。" + note_suffix,
            },
        }

    # 鉴权开启时只返回占位模板。管理 API 不得把完整 MCP token 拼入
    # URL/config，也不返回授权 token 集合；users 仅保留非敏感身份元数据。
    token_template = f"{sse_url}?token=<TOKEN>" if auth["auth_enabled"] else sse_url
    user_configs: list[dict] = []
    if auth["auth_enabled"]:
        all_users = await mcp_plugin_store.list_mcp_users(mask_token=True)
        user_configs = [
            {"user_id": u["id"], "name": u["name"]}
            for u in all_users
            if u.get("enabled") and u.get("id") in auth.get("allowed_user_ids", set())
        ]

    return {
        "service_id": service_id,
        "service_name": name,
        "transport": "sse",
        "sse_url": sse_url,
        "ws_session_url": ws_session_url,
        "token_template": token_template,
        "auth_enabled": auth["auth_enabled"],
        "users": user_configs,
        "runtime_status": service.get("runtime_status"),
        "enabled": service.get("enabled"),
        "configs": _configs_for(token_template),
    }
