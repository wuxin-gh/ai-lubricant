"""用户自有 MCP principal 与资源授权路由。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from mcp.api_common import (
    PrincipalParamReq, ReplacePrincipalParamsReq,
    SetPrincipalTokenStatusReq, _resync_builtin_principals,
)

from .deps import get_current_user
from .models import User

router = APIRouter(prefix="/api/v1/users/mcp-principals", tags=["monkeycode-mcp-principals"])


class CreatePrincipalReq(BaseModel):
    name: str
    description: str = ""
    enabled: bool = True
    # 用户侧可建 external / agent；task 由 Agent 入口/任务服务内部建。external 返回
    # 明文 token 一次；agent 不返回明文（masked，仅 token_hint）。
    usage_type: str = "external"


class UpdatePrincipalReq(BaseModel):
    name: str | None = None
    description: str | None = None
    enabled: bool | None = None


# param / token-status 模型已上提到 mcp.api_common（管理端与用户侧共用）。
ParamReq = PrincipalParamReq
SetTokenStatusReq = SetPrincipalTokenStatusReq
ReplaceParamsReq = ReplacePrincipalParamsReq


def _bad_request(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc) or "请求参数无效")


# agent/task principal 绑定的是用户自己的 Agent/任务，其权限本就该由用户在用户侧配置
# （见 mcp_plugin_store.get_manageable_mcp_principal 注释：读已放行，写口径必须一致）。
# 只有平台 external principal 才是纯管理端配置，用户侧只读。
_USER_WRITABLE_MANAGEABLE_TYPES = ("agent", "task")


@router.get("/runtime-address")
async def runtime_address(user: User = Depends(get_current_user)) -> dict:
    """返回当前用户可用的 MCP Runtime 接入地址（只读）。

    复用 mcp.api 的展示地址逻辑；用户侧只读，不能改 port/host。
    """
    from mcp.api import _runtime_public_base, _runtime_settings

    settings = _runtime_settings()
    return {
        "port": settings["port"],
        "display_host": settings["display_host"],
        "connection_base": _runtime_public_base(),
    }


@router.get("/authorization/options")
async def authorization_options(user: User = Depends(get_current_user)) -> dict:
    """返回当前用户可授权的个人 MCP、CDP 浏览器和邮箱账户。"""
    import builtin_tool_store
    import mcp_plugin_store

    owner = str(user.id)
    services = [
        item for item in await mcp_plugin_store.list_services_by_user(owner)
        if item.get("name") != "marketplace-status"
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
    for resource in await builtin_tool_store.list_resources(owner_user_id=owner):
        if resource.get("resource_type") not in ("cdp_client", "mail_account", "device"):
            continue
        if not resource.get("enabled", True):
            continue
        resources.append({
            "resource_kind": "builtin_resource",
            "resource_id": resource["id"],
            "resource_type": resource.get("resource_type"),
            "name": resource.get("name") or resource.get("display_name") or str(resource["id"]),
            "children": [],
        })
    return {"resources": resources}


@router.get("/instance-authorizations")
async def instance_authorizations(user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store as store

    return {"resources": await store.list_owned_resource_principal_params(str(user.id))}


@router.get("")
async def list_principals(user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store as store

    return {"principals": await store.list_owned_mcp_principals(str(user.id))}


@router.post("")
async def create_principal(body: CreatePrincipalReq, user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store as store

    if not body.name.strip():
        raise HTTPException(status_code=400, detail="名称不能为空")
    if body.usage_type not in ("external", "agent"):
        raise HTTPException(status_code=400, detail="用户侧只能创建 external 或 agent 类型 MCP 用户")
    try:
        return await store.create_owned_mcp_principal(
            str(user.id), body.name, description=body.description, enabled=body.enabled,
            usage_type=body.usage_type,
        )
    except Exception as exc:
        if exc.__class__.__name__ == "UniqueViolationError":
            raise HTTPException(status_code=409, detail="MCP 用户名称已存在") from exc
        raise


@router.patch("/{principal_id}")
async def update_principal(
    principal_id: int, body: UpdatePrincipalReq, user: User = Depends(get_current_user),
) -> dict:
    import mcp_plugin_store as store

    if body.name is not None and not body.name.strip():
        raise HTTPException(status_code=400, detail="名称不能为空")
    row = await store.update_owned_mcp_principal(
        principal_id, str(user.id), **body.model_dump(exclude_none=True),
    )
    if row is not None:
        return row
    # agent/task principal（owner=NULL 的平台 Agent、或本人的任务）由用户侧管理；
    # 平台 external principal 才是管理端专属配置，用户侧只读。
    manageable = await store.get_manageable_mcp_principal(principal_id, str(user.id))
    if manageable is not None:
        if manageable.get("usage_type") in _USER_WRITABLE_MANAGEABLE_TYPES:
            updated = await store.update_manageable_mcp_principal(
                principal_id, str(user.id), **body.model_dump(exclude_none=True),
            )
            if updated is not None:
                return updated
        raise HTTPException(status_code=403, detail="平台 MCP 用户只能在管理端修改")
    raise HTTPException(status_code=404, detail="MCP 用户不存在")


@router.delete("/{principal_id}")
async def delete_principal(principal_id: int, user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store as store

    if not await store.delete_owned_mcp_principal(principal_id, str(user.id)):
        raise HTTPException(status_code=404, detail="MCP 用户不存在")
    return {"deleted": True}


@router.post("/{principal_id}/rotate-token")
async def rotate_token(principal_id: int, user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store as store

    row = await store.rotate_owned_mcp_principal_token(principal_id, str(user.id))
    if row is None:
        raise HTTPException(status_code=404, detail="MCP 用户不存在")
    return row


@router.put("/{principal_id}/token-status")
async def set_token_status(
    principal_id: int, body: SetTokenStatusReq, user: User = Depends(get_current_user),
) -> dict:
    import mcp_plugin_store as store

    try:
        row = await store.set_owned_mcp_principal_token_status(
            principal_id, str(user.id), body.status,
        )
    except ValueError as exc:
        raise _bad_request(exc) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="MCP 用户不存在")
    return row


@router.get("/{principal_id}/params")
async def list_params(principal_id: int, user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store as store

    # Reading platform principal params is useful for diagnostics; writes below
    # require strict ownership.
    if await store.get_manageable_mcp_principal(principal_id, str(user.id)) is None:
        raise HTTPException(status_code=404, detail="MCP 用户不存在")
    return {"params": await store.list_principal_params(principal_id)}


@router.put("/{principal_id}/params")
async def replace_params(
    principal_id: int, body: ReplaceParamsReq, user: User = Depends(get_current_user),
) -> dict:
    import mcp_plugin_store as store

    # 写入口径与 list/read 对齐：自有 principal 直接放行；agent/task principal（含
    # owner=NULL 的平台 Agent）由用户在 Agent/任务入口配置权限，也放行。只有平台
    # external principal 是管理端专属配置。
    if await store.get_owned_mcp_principal(principal_id, str(user.id)) is None:
        manageable = await store.get_manageable_mcp_principal(principal_id, str(user.id))
        if manageable is None:
            raise HTTPException(status_code=404, detail="MCP 用户不存在")
        if manageable.get("usage_type") not in _USER_WRITABLE_MANAGEABLE_TYPES:
            raise HTTPException(status_code=403, detail="平台 MCP 用户只能在管理端修改")
    payload: list[dict[str, Any]] = [item.model_dump() for item in body.params]
    try:
        params = await store.set_principal_params(
            principal_id, payload, owner_user_id=str(user.id),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="MCP 用户不存在") from exc
    except ValueError as exc:
        raise _bad_request(exc) from exc
    # param 变了要重下发，确保活跃 driver 的客户端/邮箱资源与刚校验过的参数一致。
    await _resync_builtin_principals()
    return {"params": params}
