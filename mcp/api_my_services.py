"""用户侧个人外部 MCP（my-services）。

个人 MCP 上游：user_id=自己、kind=sse；headers 支持 Bearer token 便捷入口和自定义请求头。
管理端/内置服务对用户只读，不在此处改动。
"""
from __future__ import annotations

from typing import Any

import mcp_plugin_store
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field

from .api_common import _mask_token

router = APIRouter(prefix="/mcp", tags=["mcp"])


# ── Request models ──

class CreateMyMcpServiceRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    display_name: str | None = None
    description: str = ""
    url: str = Field(..., min_length=1)
    token: str = ""  # 明文 token，组装为 Authorization: Bearer 存储
    headers: dict[str, str] | None = None
    enabled: bool = True


class UpdateMyMcpServiceRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    url: str | None = None
    token: str | None = None  # 字段缺省时不改；空串清除 Bearer token
    headers: dict[str, str] | None = None
    enabled: bool | None = None


# ── headers 合并：Bearer 与自定义头 ──

def _bearer_headers(token: str | None) -> dict[str, str]:
    """统一鉴权头组装：token → {"Authorization": "Bearer <token>"}。空 token → 空头。"""
    token = (token or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _is_sensitive_header(name: str) -> bool:
    normalized = name.lower().replace("-", "_")
    return normalized in {"authorization", "cookie", "set_cookie"} or any(
        marker in normalized for marker in ("token", "api_key", "apikey", "secret", "password")
    )


def _merge_service_headers(
    token: str | None,
    custom_headers: dict[str, str] | None,
    *,
    existing_headers: dict[str, str] | None = None,
    token_provided: bool = True,
) -> dict[str, str]:
    """合并 Bearer 与自定义头。

    - 自定义含 ``Authorization`` 时以自定义为准（token 让位）。
    - 敏感头：提交值为空、或等于旧值的脱敏串（``_mask_token(old)``）时保留旧密文，
      避免编辑表单回填的遮蔽串覆盖真实凭据。
    - 编辑时 ``Authorization`` 不在脱敏视图里回显，故未显式覆盖时保留已存储的 bearer。
    """
    existing = {
        str(key).strip(): str(value)
        for key, value in (existing_headers or {}).items()
        if str(key).strip()
    }

    def _find_existing(name: str) -> str | None:
        for key, value in existing.items():
            if key.lower() == name.lower():
                return value
        return None

    def _dedupe(name: str, target: dict) -> None:
        for key in list(target):
            if key.lower() == name.lower():
                del target[key]

    if custom_headers is None:
        merged = dict(existing)
    else:
        merged: dict[str, str] = {}
        # Authorization 不在脱敏视图里回显；编辑其他头时保留已存储的 bearer，
        # 除非本次显式覆盖。
        for key, value in existing.items():
            if key.lower() == "authorization":
                merged[key] = value
        for raw_key, raw_value in custom_headers.items():
            key = str(raw_key).strip()
            if not key:
                continue
            value = str(raw_value)
            old = _find_existing(key)
            _dedupe(key, merged)
            if _is_sensitive_header(key):
                if not value.strip() or (old is not None and value == _mask_token(old)):
                    if old is not None:
                        merged[next(k for k in existing if k.lower() == key.lower())] = old
                else:
                    merged[key] = value
            elif value.strip():
                merged[key] = value

    custom_has_auth = bool(custom_headers) and any(
        str(k).strip().lower() == "authorization" for k in custom_headers
    )
    if token_provided and not custom_has_auth:
        _dedupe("authorization", merged)
        merged.update(_bearer_headers(token))

    return merged


# ── 用户解析与归属校验 ──

async def _async_user_id() -> str | None:
    """复用 /agent 的 C 端用户解析（session cookie）。"""
    from agent.api import _resolve_user_from_session
    return await _resolve_user_from_session()


async def _require_my_service_owner(service_id: int, user_id: str) -> dict:
    """取服务行并校验归属；非 owner 或不存在 → 404/403。"""
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    if service.get("user_id") != user_id:
        # 不泄露他人服务存在性
        raise HTTPException(404, f"MCP service {service_id} not found")
    return service


async def _my_service_to_dict(service: dict, *, mask_token: bool = True) -> dict:
    """对外脱敏视图：headers 拆出 token 字段便于前端展示，可选脱敏。"""
    headers = service.get("headers") or {}
    auth_header = headers.get("Authorization") or headers.get("authorization") or ""
    token = ""
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
    shown = token
    if mask_token and token:
        shown = _mask_token(token)
    visible_headers = {
        key: (_mask_token(value) if _is_sensitive_header(key) else value)
        for key, value in headers.items()
        if key.lower() != "authorization"
    }
    tools_cache = service.get("tools_cache") or []
    return {
        "id": service["id"],
        "name": service.get("name"),
        "display_name": service.get("display_name") or service.get("name"),
        "description": service.get("description") or "",
        "kind": service.get("kind") or "sse",
        "url": service.get("url"),
        "token": shown,
        "has_token": bool(token),
        "headers": visible_headers,
        "enabled": bool(service.get("enabled")),
        "runtime_status": service.get("runtime_status"),
        "runtime_last_error": service.get("runtime_last_error"),
        "tool_count": len(tools_cache) if isinstance(tools_cache, list) else 0,
        "tools": [
            {"name": t.get("name"), "description": t.get("description") or ""}
            for t in tools_cache if isinstance(t, dict)
        ] if isinstance(tools_cache, list) else [],
    }


def _resolve_my_caller(authorization: str | None) -> str:
    """已弃用：保留为同步包装以防外部引用；新代码用 _async_user_id。"""
    raise HTTPException(401, "未登录")


# ── 路由 ──

@router.get("/my-services")
async def list_my_services(authorization: str | None = Header(None)) -> list[dict]:
    """列出当前用户的个人 MCP 服务（kind=sse, user_id=自己）。"""
    user_id = await _async_user_id()
    if not user_id:
        raise HTTPException(401, "未登录")
    services = await mcp_plugin_store.list_services_by_user(user_id)
    return [await _my_service_to_dict(s) for s in services]


@router.post("/my-services")
async def create_my_service(payload: CreateMyMcpServiceRequest, authorization: str | None = Header(None)) -> dict:
    user_id = await _async_user_id()
    if not user_id:
        raise HTTPException(401, "未登录")
    headers = _merge_service_headers(payload.token, payload.headers)
    fields = {
        "name": payload.name,
        "display_name": payload.display_name or payload.name,
        "description": payload.description,
        "category": "custom",
        "transport": "streamable-http",
        "url": payload.url,
        "headers": headers,
        "kind": "sse",
        "scope": "user",
        "user_id": user_id,
        "source": "user",
        "enabled": payload.enabled,
    }
    try:
        row = await mcp_plugin_store.create_service(fields)
    except LookupError:
        raise HTTPException(409, f"MCP 服务名 '{payload.name}' 已存在")
    except RuntimeError:
        raise HTTPException(503, "Database not available")
    service = row
    # 建好后立刻试拉工具缓存，失败不阻断创建（返回里 runtime_status 自会反映）。
    await _sync_my_service_tools(service["id"], user_id)
    fresh = await mcp_plugin_store.get_service(service["id"])
    return await _my_service_to_dict(fresh or service)


@router.patch("/my-services/{service_id}")
async def update_my_service(
    service_id: int,
    payload: UpdateMyMcpServiceRequest,
    authorization: str | None = Header(None),
) -> dict:
    user_id = await _async_user_id()
    if not user_id:
        raise HTTPException(401, "未登录")
    service = await _require_my_service_owner(service_id, user_id)
    updates: dict[str, Any] = {}
    if payload.display_name is not None:
        updates["display_name"] = payload.display_name
    if payload.description is not None:
        updates["description"] = payload.description
    if payload.url is not None:
        updates["url"] = payload.url
    if payload.enabled is not None:
        updates["enabled"] = payload.enabled
    if payload.token is not None or payload.headers is not None:
        new_headers = _merge_service_headers(
            payload.token,
            payload.headers,
            existing_headers=service.get("headers"),
            token_provided=payload.token is not None,
        )
        updates["headers"] = new_headers
    if not updates:
        return await _my_service_to_dict(service)
    row = await mcp_plugin_store.patch_service(service_id, updates)
    if not row:
        raise HTTPException(404, f"MCP service {service_id} not found")
    # 鉴权/URL 变了需要 runtime 重注册；token/url 改动触发一次工具再发现。
    if payload.token is not None or payload.headers is not None or payload.url is not None or payload.enabled is not None:
        await _sync_my_service_tools(service_id, user_id)
    fresh = await mcp_plugin_store.get_service(service_id)
    return await _my_service_to_dict(fresh or row)


@router.delete("/my-services/{service_id}")
async def delete_my_service(service_id: int, authorization: str | None = Header(None)) -> dict:
    user_id = await _async_user_id()
    if not user_id:
        raise HTTPException(401, "未登录")
    service = await _require_my_service_owner(service_id, user_id)
    # 先从 runtime 卸载，避免删表后 registry 里留死引用。
    try:
        from mcp_runtime.registry import registry
        name = service.get("name")
        if name and registry.get(name) is not None:
            await registry.deactivate(name)
    except Exception as exc:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).warning("[mcp] my-service delete deactivate failed: %s", exc)
    deleted = await mcp_plugin_store.delete_service_owned(service_id, user_id)
    if not deleted:
        raise HTTPException(404, f"MCP service {service_id} not found")
    return {"ok": True, "msg": f"MCP service {service_id} deleted"}


@router.post("/my-services/{service_id}/sync")
async def sync_my_service(service_id: int, authorization: str | None = Header(None)) -> dict:
    """触发外部 SSE tools/list 刷新 tools_cache。"""
    user_id = await _async_user_id()
    if not user_id:
        raise HTTPException(401, "未登录")
    # 先按 owner 取出实际服务行用来查 name/放入过滤条件
    service = await mcp_plugin_store.get_service(service_id)
    if not service or service.get("user_id") != user_id:
        raise HTTPException(404, f"MCP service {service_id} not found")
    return await _sync_my_service_tools(service_id, user_id)


async def _sync_my_service_tools(service_id: int, user_id: str) -> dict:
    """通过 SSE 直连客户端拉取外部工具列表并写回 tools_cache/runtime_status。"""
    service = await mcp_plugin_store.get_service(service_id)
    if not service or service.get("user_id") != user_id:
        raise HTTPException(404, f"MCP service {service_id} not found")
    url = service.get("url")
    if not url:
        raise HTTPException(400, "sse 服务缺少 url")
    headers = service.get("headers") or {}
    from mcp_runtime.sse_client import SSEClient, SSEClientError
    client = SSEClient(url, headers)
    try:
        tools = await client.list_tools()
    except SSEClientError as exc:
        await mcp_plugin_store.mark_runtime_error(service_id, str(exc))
        return {"ok": False, "error": str(exc), "tool_count": 0}
    normalized = [
        {
            "name": t.get("name") or "",
            "description": t.get("description") or "",
            "input_schema": t.get("inputSchema") or t.get("input_schema") or {"type": "object", "properties": {}},
            "service_name": service.get("name"),
        }
        for t in tools if isinstance(t, dict) and t.get("name")
    ]
    await mcp_plugin_store.mark_runtime_status(service_id, "loaded", tools_cache=normalized)
    # 服务已在 runtime registry 注册过的话，原地重激活刷新工具集
    try:
        from mcp_runtime.registry import registry
        name = service.get("name")
        if name and registry.get(name) is not None:
            auth = await mcp_plugin_store.get_service_auth(service_id)
            await registry.activate_sse(
                name, url, headers=headers,
                auth_enabled=auth["auth_enabled"], allowed_tokens=auth["allowed_tokens"],
            )
    except Exception as exc:  # noqa: BLE001 — 同步失败不影响缓存写入，前端能看到 tool_count
        import logging
        logging.getLogger(__name__).warning("[mcp] my-service sync re-activate failed: %s", exc)
    return {"ok": True, "tool_count": len(normalized), "tools": [{"name": t["name"], "description": t["description"]} for t in normalized]}
