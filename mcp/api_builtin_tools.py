"""内置工具管理端聚合视图：CDP/Mail/设备资源 + token 的跨 owner 运维视图。

内置工具不是 MCP 服务；这些端点只为管理端"工具"Tab 提供聚合视图。
secret_data、token_hash、完整 token 永不返回。
"""
from __future__ import annotations

import mcp_plugin_store  # noqa: F401  — 保留 import 以维持模块初始化语义一致性
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from .api_common import _require_admin

router = APIRouter(prefix="/mcp", tags=["mcp"])


class BuiltinTokenStatusPayload(BaseModel):
    status: str  # 'active' | 'disabled'


@router.get("/builtin-tools/resources")
async def list_builtin_tool_resources(authorization: str | None = Header(None)):
    """管理端内置工具总览：一级资源 + 安全配置 + token 摘要 + CDP 实时状态。

    内置工具不是 MCP 服务；本端点只为管理端"工具"Tab提供跨 owner 运维视图。
    secret_data、token_hash、完整 token 永不返回。资源直挂 owner，无实例层。
    """
    await _require_admin(authorization)
    import builtin_tool_store

    resources = await builtin_tool_store.list_resources()
    all_tokens = await builtin_tool_store.list_all_tokens()
    tokens_by_resource: dict[int, list[dict]] = {}
    for token in all_tokens:
        resource_id = token.get("resource_id")
        if resource_id is not None:
            tokens_by_resource.setdefault(int(resource_id), []).append(token)

    # CDP runtime 是 best-effort：driver 未初始化时只返回空 contexts，不让管理页报错。
    # 经 registry 取数（registry.get_builtin_state），不直读 cdp_server.driver 全局单例——
    # 保证读到的会话状态与 SSE 网关实际驱动的插件实例一致。
    try:
        from mcp_runtime.registry import registry
        all_contexts = registry.get_builtin_state("cdp-bridge")
    except Exception:
        all_contexts = []

    rows: list[dict] = []
    for resource in resources:
        resource_id = int(resource["id"])
        safe = dict(resource)
        safe.pop("token_hash", None)
        safe.pop("secret_data", None)

        tokens = tokens_by_resource.get(resource_id, [])
        token_summary = {
            "total": len(tokens),
            "active": sum(t.get("status") == "active" for t in tokens),
            "disabled": sum(t.get("status") == "disabled" for t in tokens),
            "external": sum(t.get("target_type") == "external" for t in tokens),
        }
        row = {
            **safe,
            "tokens": tokens,
            "token_summary": token_summary,
        }

        if resource.get("resource_type") == "cdp_client":
            client_ids = {str(resource_id)}
            contexts = []
            for context in all_contexts or []:
                context_clients = [
                    c for c in (context.get("clients") or [])
                    if str(c.get("client_id")) in client_ids
                ]
                if str(context.get("context_id")) in client_ids or context_clients:
                    contexts.append({**context, "clients": context_clients})
            connected_ids = {
                str(c.get("client_id"))
                for context in contexts
                for c in (context.get("clients") or [])
                if c.get("connected")
            }
            row["cdp"] = {
                "clients_total": 1,
                "clients_enabled": 1 if safe.get("enabled", True) else 0,
                "clients_connected": len(connected_ids),
                "active_pages": sum(
                    int(c.get("active_count") or 0)
                    for ctx in contexts
                    for c in (ctx.get("clients") or [])
                ),
                "contexts": contexts,
            }
        rows.append(row)
    return {"resources": rows}


@router.get("/builtin-tools/tokens")
async def list_builtin_tool_tokens(authorization: str | None = Header(None)):
    """管理端全量 token 视图：用户建的 external + 服务自动建的 agent/node/user。

    用户侧（``/api/v1/users/builtin-tools``）只显示 external；服务自动签发的
    agent/node/user 只在这里可见。只读 + 名称快照，不含明文（明文只在签发时一次性
    返回，只留 hint）。
    """
    await _require_admin(authorization)
    import builtin_tool_store

    tokens = await builtin_tool_store.list_all_tokens()
    return {"tokens": tokens}


@router.put("/builtin-tools/tokens/{token_id}/status")
async def set_builtin_tool_token_status(
    token_id: int, payload: BuiltinTokenStatusPayload, authorization: str | None = Header(None)
):
    """管理端启用 / 禁用任意 token（含服务自动签发的 agent/node/user）。"""
    await _require_admin(authorization)
    if payload.status not in ("active", "disabled"):
        raise HTTPException(400, "不支持的 token 状态")
    import builtin_tool_store

    row = await builtin_tool_store.set_token_status(token_id, payload.status)
    if row is None:
        raise HTTPException(404, "token 不存在")
    return row


@router.delete("/builtin-tools/tokens/{token_id}")
async def delete_builtin_tool_token(token_id: int, authorization: str | None = Header(None)):
    """管理端删除任意 token。"""
    await _require_admin(authorization)
    import builtin_tool_store

    deleted = await builtin_tool_store.delete_token(token_id)
    return {"deleted": deleted}
