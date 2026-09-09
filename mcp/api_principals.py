"""MCP principal / grants / service-users / runtime-settings / market-settings。

管理端 principal（owner_user_id IS NULL，平台级）+ 服务↔用户授权 + 连接展示设置。
用户侧 principal 路由在 user_platform/routes_mcp_principals.py（owner_user_id=用户）。
两套共用 mcp_plugin_store 的 principal 函数族，下一步合并。
"""
from __future__ import annotations

import asyncio

import mcp_plugin_store
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field

from .api_common import (
    PrincipalParamReq, ReplacePrincipalParamsReq,
    _reload_service, _reload_services, _require_admin, _resync_builtin_principals,
    _runtime_public_base, _runtime_settings,
)

router = APIRouter(prefix="/mcp", tags=["mcp"])
_RESERVED_SERVICE_NAMES = frozenset({"marketplace-status"})


async def _reject_reserved_grants(service_ids: list[int]) -> None:
    if not service_ids:
        return
    services = await mcp_plugin_store.list_services()
    reserved = {
        int(service["id"]): str(service.get("name") or "")
        for service in services
        if service.get("id") is not None and service.get("name") in _RESERVED_SERVICE_NAMES
    }
    requested = sorted(reserved[service_id] for service_id in service_ids if service_id in reserved)
    if requested:
        raise HTTPException(403, f"系统保留 MCP 只能由专用服务端流程授权: {', '.join(requested)}")


# ── Request models ──

class MCPUserRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    token: str | None = None
    enabled: bool = True
    chat_enabled: bool = False
    description: str = ""
    # usage_type 只允许 agent|external；task 由任务服务内部创建，管理端禁止手动建。
    usage_type: str = "external"


class UpdateMCPUserRequest(BaseModel):
    name: str | None = None
    token: str | None = None
    enabled: bool | None = None
    chat_enabled: bool | None = None
    description: str | None = None


class UpdateMCPUserTokenStatusRequest(BaseModel):
    status: str  # active | disabled


class UpdateMCPServiceUsersRequest(BaseModel):
    user_ids: list[int] = []


class UpdateMCPUserServicesRequest(BaseModel):
    service_ids: list[int] = []


class UpdateMCPServiceAuthRequest(BaseModel):
    auth_enabled: bool = True


class MCPRuntimeSettingsRequest(BaseModel):
    # port 可选：不传则保留已保存的对外端口，回退到主服务端口。不再有独立 runtime
    # 端口（原 8003 独立进程已退役），默认值故作 None 以免把死端口写进配置。
    port: int | None = Field(None, ge=1, le=65535)
    display_host: str = Field("127.0.0.1", min_length=1, max_length=255)


# param 模型已上提到 api_common（管理端与用户侧共用）；保留别名向后兼容。
McpPrincipalParamReq = PrincipalParamReq
ReplaceMcpUserParamsReq = ReplacePrincipalParamsReq


# ── Runtime settings（连接展示端口/IP） ──

@router.get("/runtime-settings")
async def get_runtime_settings(authorization: str | None = Header(None)) -> dict:
    """读取 MCP Runtime 端口与连接配置显示地址。"""
    await _require_admin(authorization)
    settings = _runtime_settings()
    settings["connection_base"] = _runtime_public_base()
    return settings


@router.put("/runtime-settings")
async def update_runtime_settings(payload: MCPRuntimeSettingsRequest, authorization: str | None = Header(None)) -> dict:
    """保存 MCP 对外连接显示设置（端口 + IP）。

    MCP Runtime 已合并进主程序，这两项不再控制内部监听端口/进程，仅用于拼接连接
    配置里展示给外部 MCP 客户端的 SSE / WebSocket 地址（外部经反向代理接入时的对外
    端口与 IP，可与主服务内部监听端口不同）。保存即用于后续生成连接串，无需重启。
    """
    await _require_admin(authorization)
    display_host = payload.display_host.strip().rstrip("/")
    if not display_host:
        raise HTTPException(400, "连接显示 IP 不能为空")
    if "://" in display_host or "/" in display_host or ":" in display_host or any(ch.isspace() for ch in display_host):
        raise HTTPException(400, "连接显示 IP 只填写 IP 或域名，不要包含协议、端口或路径")
    import config
    main_config = await config.CONFIG_STORE.read_main_async()
    updated = dict(main_config)
    saved_runtime = dict(main_config.get("mcp_runtime") or {})
    # port 可选：未传则保留原值（回退到主服务端口）。
    if payload.port is not None:
        saved_runtime["port"] = payload.port
    saved_runtime["display_host"] = display_host
    updated["mcp_runtime"] = saved_runtime
    await config.CONFIG_STORE.write_main_async(updated)
    settings = _runtime_settings()
    return {**settings, "connection_base": _runtime_public_base()}


# ── Market settings：用户 ↔ 服务授权总览 ──

@router.get("/market-settings")
async def get_market_settings(authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    # 管理端口径一致：只列平台级 external principal（agent/task 是会话级身份，
    # 由系统自动管理服务授权，不在管理端授权矩阵里手动配）。
    all_users = await mcp_plugin_store.list_platform_mcp_users(mask_token=True)
    users = [u for u in all_users if u.get("usage_type") not in ("agent", "task")]
    services = [
        service for service in await mcp_plugin_store.list_services()
        if service.get("name") not in _RESERVED_SERVICE_NAMES
    ]
    service_rows = []
    # 并发取每个 service 的 auth 状态 + 每个 user 的授权 service 列表，避免串行
    # 逐条 await 把 Redis/DB 往返乘以 (services+users) 数量。
    services_auth = await asyncio.gather(
        *[mcp_plugin_store.get_service_auth(s["id"]) for s in services]
    )
    user_service_ids = await asyncio.gather(
        *[mcp_plugin_store.list_services_for_mcp_user(u["id"]) for u in users]
    )
    for service, auth in zip(services, services_auth):
        service_rows.append({
            "id": service["id"], "name": service["name"],
            "display_name": service.get("display_name") or service["name"],
            "enabled": bool(service.get("enabled")), "auth_enabled": auth["auth_enabled"],
        })
    for user, sids in zip(users, user_service_ids):
        user["service_ids"] = sids
    return {"users": users, "services": service_rows}


@router.put("/market-settings/users/{user_id}/services")
async def set_market_user_services(
    user_id: int,
    payload: UpdateMCPUserServicesRequest,
    authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    if not await mcp_plugin_store.get_mcp_user(user_id):
        raise HTTPException(404, f"MCP user {user_id} not found")
    await _reject_reserved_grants(payload.service_ids)
    try:
        before, after = await mcp_plugin_store.set_services_for_mcp_user(user_id, payload.service_ids)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    await _reload_services(set(before) | set(after), authorization)
    return {"user_id": user_id, "service_ids": after}


# ── MCP 用户体系：管理端与用户侧共用 mcp_users 表，owner_user_id 区分来源 ──
# 管理端创建的 principal：owner_user_id IS NULL（平台级）；用户侧：owner_user_id=用户。
# 两套 API 能力对齐：CRUD + 权限设置（grants）+ 子权限 + 禁用启用（token_status）。
# token 统一走 token_hash + token_hint，明文只在创建/轮换时返回一次。

@router.get("/users")
async def list_users(authorization: str | None = Header(None)) -> list[dict]:
    await _require_admin(authorization)
    # 管理端只列平台级 principal（owner IS NULL）。agent/task 类型是会话级身份，
    # 归用户侧可见（list_owned_mcp_principals），不在管理端混显。
    users = await mcp_plugin_store.list_platform_mcp_users(mask_token=True)
    return [u for u in users if u.get("usage_type") not in ("agent", "task")]


@router.get("/users/authorization-options")
async def list_user_authorization_options(authorization: str | None = Header(None)) -> dict:
    """平台级可授权资源：平台 MCP 服务 + 平台内置工具实例（CDP/邮箱）及其子项。

    管理端只给平台级 principal（owner IS NULL）授权，故资源也只列平台级
    （service.user_id IS NULL / instance.owner_user_id IS NULL）。用户侧个人
    资源不在管理端授权范围内。
    """
    await _require_admin(authorization)
    import builtin_tool_store

    services = [
        item for item in await mcp_plugin_store.list_services_by_user(None)
        if item.get("name") not in _RESERVED_SERVICE_NAMES
    ]
    resources: list[dict] = [
        {
            "resource_kind": "service", "resource_id": item["id"],
            "resource_type": "service", "name": item.get("display_name") or item.get("name"),
            "url": item.get("url") or "",
            "description": item.get("description") or "",
            "tool_count": item.get("tool_count") or 0,
            "transport": item.get("transport") or "",
            "children": [],
        }
        for item in services if item.get("enabled", True)
    ]
    resources.extend([
        {
            "resource_kind": "builtin_resource",
            "resource_id": resource["id"],
            "resource_type": resource.get("resource_type"),
            "name": resource.get("name") or resource.get("display_name") or str(resource["id"]),
            "children": [],
        }
        for resource in await builtin_tool_store.list_resources(owner_user_id=None)
        if resource.get("enabled", True)
    ])
    return {"resources": resources}


@router.post("/users")
async def create_user(payload: MCPUserRequest, authorization: str | None = Header(None)) -> dict:
    """创建平台级 MCP 用户（owner_user_id=NULL）。

    usage_type 只允许 agent|external；task 由任务服务内部创建，拒绝手动建。
    external 创建返回明文 token 一次；agent 不返回明文。
    """
    await _require_admin(authorization)
    if payload.usage_type not in ("agent", "external"):
        raise HTTPException(400, "usage_type 只允许 agent 或 external")
    try:
        return await mcp_plugin_store.create_platform_mcp_principal(
            payload.name,
            description=payload.description,
            enabled=payload.enabled,
            chat_enabled=payload.chat_enabled,
            usage_type=payload.usage_type,
        )
    except Exception as exc:
        if exc.__class__.__name__ == "UniqueViolationError":
            raise HTTPException(409, "MCP 用户名称已存在") from exc
        if isinstance(exc, ValueError):
            raise HTTPException(400, str(exc)) from exc
        raise


@router.patch("/users/{user_id}")
async def update_user(user_id: int, payload: UpdateMCPUserRequest, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    result = await mcp_plugin_store.update_mcp_user(
        user_id,
        name=payload.name,
        description=payload.description,
        enabled=payload.enabled,
        chat_enabled=payload.chat_enabled,
    )
    if not result:
        raise HTTPException(404, f"MCP user {user_id} not found")
    return result


@router.delete("/users/{user_id}")
async def delete_user(user_id: int, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    if not await mcp_plugin_store.delete_mcp_user(user_id):
        raise HTTPException(404, f"MCP user {user_id} not found")
    return {"ok": True}


@router.post("/users/{user_id}/rotate-token")
async def rotate_user_token(user_id: int, authorization: str | None = Header(None)) -> dict:
    """重新生成 token；明文新 token 仅本次返回。"""
    await _require_admin(authorization)
    result = await mcp_plugin_store.rotate_mcp_user_token(user_id)
    if not result:
        raise HTTPException(404, f"MCP user {user_id} not found")
    return result


@router.put("/users/{user_id}/token-status")
async def set_user_token_status(
    user_id: int, payload: UpdateMCPUserTokenStatusRequest, authorization: str | None = Header(None),
) -> dict:
    """禁用/启用 principal token（token_status: active|disabled）。"""
    await _require_admin(authorization)
    try:
        result = await mcp_plugin_store.set_mcp_user_token_status(user_id, payload.status)
    except ValueError as exc:
        raise HTTPException(400, str(exc) or "请求参数无效") from exc
    if not result:
        raise HTTPException(404, f"MCP user {user_id} not found")
    return result


# ── MCP 用户（principal）的操作参数 ──

@router.get("/users/{user_id}/params")
async def list_user_params(user_id: int, authorization: str | None = Header(None)) -> dict:
    """列出 principal 的全部操作参数（cdp_client_id / mail_account_id 等）。"""
    await _require_admin(authorization)
    if await mcp_plugin_store.get_mcp_user(user_id) is None:
        raise HTTPException(404, f"MCP user {user_id} not found")
    return {"params": await mcp_plugin_store.list_principal_params(user_id)}


@router.put("/users/{user_id}/params")
async def replace_user_params(
    user_id: int, payload: ReplacePrincipalParamsReq, authorization: str | None = Header(None),
) -> dict:
    """全量替换 principal 的操作参数。保存后重下发内置服务快照。"""
    await _require_admin(authorization)
    try:
        params = await mcp_plugin_store.set_principal_params(
            user_id, [p.model_dump() for p in payload.params],
        )
    except LookupError as exc:
        raise HTTPException(404, f"MCP user {user_id} not found") from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc) or "请求参数无效") from exc
    # param 变了要重下发，确保活跃 driver 的客户端/邮箱资源与刚校验过的参数一致。
    await _resync_builtin_principals()
    return {"params": params}


# ── 服务 ↔ 用户 授权关系 ──

@router.get("/services/{service_id}/users")
async def list_service_users(service_id: int, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    if not await mcp_plugin_store.get_service(service_id):
        raise HTTPException(404, f"MCP service {service_id} not found")
    user_ids = await mcp_plugin_store.list_service_users(service_id)
    auth = await mcp_plugin_store.get_service_auth(service_id)
    return {
        "service_id": service_id,
        "user_ids": user_ids,
        "auth_enabled": auth["auth_enabled"],
    }


@router.put("/services/{service_id}/users")
async def set_service_users(service_id: int, payload: UpdateMCPServiceUsersRequest, authorization: str | None = Header(None)) -> dict:
    """全量替换授权用户列表。保存后自动重新加载服务配置（无需重启）。"""
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    if service.get("name") in _RESERVED_SERVICE_NAMES:
        raise HTTPException(403, "系统保留 MCP 只能由专用服务端流程授权")
    user_ids = await mcp_plugin_store.set_service_users(service_id, payload.user_ids)
    await _reload_service(service_id, authorization)
    return {"service_id": service_id, "user_ids": user_ids}


@router.put("/services/{service_id}/auth")
async def set_service_auth(service_id: int, payload: UpdateMCPServiceAuthRequest, authorization: str | None = Header(None)) -> dict:
    """开关访问控制。保存后自动重新加载服务配置（无需重启）。

    鉴权现在一律强制——所有 MCP 服务都必须带 token 才能连，不允许「任何人可连」的
    匿名模式（那是绕过安全边界的后门）。本端点只接受 auth_enabled=True；
    传 False 一律 409。字段与端点保留是为了兼容既有调用方与未来可能的「展示态」。
    """
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    if not payload.auth_enabled:
        raise HTTPException(409, "MCP 服务鉴权不可关闭：所有服务一律要求 token")
    result = await mcp_plugin_store.set_service_auth_enabled(service_id, payload.auth_enabled)
    if not result:
        raise HTTPException(404, f"MCP service {service_id} not found")
    await _reload_service(service_id, authorization)
    return {"service_id": service_id, "auth_enabled": result["auth_enabled"]}
