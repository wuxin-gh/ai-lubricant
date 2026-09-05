"""MCP 服务注册表（管理端）：服务 CRUD + manifest/assets/sop + 配置资源 + actions/views
+ env-vars + 安装编排 + tools + agent-config-schema。

mcp_services 的读写统一经 mcp_plugin_store 收口（不直连 SQL）。
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import mcp_plugin_store
from fastapi import APIRouter, HTTPException, Header, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from agent.mcp_client import MCPManager, MCPServerConfig
from mcp.configuration import (
    ConfigurationConflict, ConfigurationError, ResourceContext, configuration_contract,
    get_resource_adapter, validate_resource_value,
)
from .api_common import (
    DEPLOY_SCOPE_SERVER, DEPLOY_SCOPE_SESSION, _manifest_for_service,
    _mask_token, _reload_service, _require_admin, _service_manifest_dir,
    derive_deploy_scope,
)

router = APIRouter(prefix="/mcp", tags=["mcp"])


# ── Request models ──

class CreateMCPServiceRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    display_name: str | None = None
    description: str = ""
    category: str = "custom"
    transport: str = "stdio"
    command: str | None = None
    args: list[str] = []
    env_template: dict[str, str] = {}
    url: str | None = None
    icon: str | None = None
    version: str | None = None
    author: str | None = None
    docs_url: str | None = None
    install_command: str | None = None
    source: str | None = None
    market_id: str | None = None
    market_version: str = ""
    template: bool = False
    enabled: bool = True


class UpdateMCPServiceRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    category: str | None = None
    transport: str | None = None
    command: str | None = None
    args: list[str] | None = None
    env_template: dict[str, str] | None = None
    url: str | None = None
    icon: str | None = None
    version: str | None = None
    author: str | None = None
    docs_url: str | None = None
    install_command: str | None = None
    enabled: bool | None = None


class MCPEnvVar(BaseModel):
    key: str = Field(..., min_length=1, max_length=100)
    value: str = ""
    secret: bool = False
    description: str = ""


class UpdateMCPEnvVarsRequest(BaseModel):
    items: list[MCPEnvVar] = []


class McpSopFile(BaseModel):
    name: str
    content: str


class McpSopRequest(BaseModel):
    files: list[McpSopFile] = []


class ResourceMutationRequest(BaseModel):
    value: dict[str, Any]
    expected_revision: int | None = None


class ResourceActionRequest(BaseModel):
    expected_revision: int | None = None


class DeployToNodeRequest(BaseModel):
    node_id: str = Field(..., min_length=1, max_length=100)


# ── CRUD endpoints ──

@router.get("/catalog")
async def list_catalog() -> list[dict]:
    return await mcp_plugin_store.list_catalog()


@router.get("/services")
async def list_services(authorization: str | None = Header(None)) -> list[dict]:
    """列出已安装的 MCP 服务（不包含 template=true 的市场目录项）。

    仅管理端：个人服务的 headers 含上游 Bearer 明文 token，未鉴权会泄露全平台凭据。
    响应里 headers 的 Authorization 脱敏（管理员只需知道配了哪些头，不需明文 token）。
    """
    await _require_admin(authorization)
    services = await mcp_plugin_store.list_services()
    return [_mask_service_headers(s) for s in services]


def _mask_service_headers(service: dict) -> dict:
    """脱敏 service 响应里的认证头，保留首尾片段以便认出配的是哪把 key。

    与渠道请求日志同一套判定（security.is_sensitive_header），自定义头名也能覆盖。
    """
    from security import is_sensitive_header, sanitize_header_value

    headers = service.get("headers")
    if not isinstance(headers, dict) or not headers:
        return service
    out = dict(service)
    out["headers"] = {
        k: sanitize_header_value(k, v) if is_sensitive_header(k) else v
        for k, v in headers.items()
    }
    return out


@router.get("/services/manifests")
async def list_service_manifests(authorization: str | None = Header(None)) -> list[dict]:
    """返回所有已安装服务的 manifest + DB runtime 状态。仅管理端。"""
    await _require_admin(authorization)
    services = await mcp_plugin_store.list_services()
    out: list[dict] = []
    for service in services:
        manifest = _manifest_for_service(service)
        # 附件下载 URL 由后端补齐，前端只渲染即可。
        assets = []
        for asset in manifest.get("assets") or []:
            a = dict(asset)
            a["download_url"] = f"/mcp/services/{service['name']}/assets/{asset.get('id')}/download"
            assets.append(a)
        if assets:
            manifest = dict(manifest)
            manifest["assets"] = assets
        out.append({"service": _mask_service_headers(service), "manifest": manifest})
    return out


@router.get("/services/{service_name}/assets")
async def list_service_assets(service_name: str) -> dict:
    service = await mcp_plugin_store.get_service_by_name(service_name)
    if not service:
        raise HTTPException(404, f"MCP service '{service_name}' not found")
    manifest = _manifest_for_service(service)
    assets = []
    for asset in manifest.get("assets") or []:
        a = dict(asset)
        a["download_url"] = f"/mcp/services/{service_name}/assets/{asset.get('id')}/download"
        assets.append(a)
    return {"service_name": service_name, "assets": assets}


def _resolve_asset_path(service_name: str, source: str) -> Path:
    base = _service_manifest_dir(service_name)
    if not base:
        raise HTTPException(404, f"No manifest directory for service '{service_name}'")
    target = (base / source).resolve()
    base_resolved = base.resolve()
    if not str(target).startswith(str(base_resolved)):
        raise HTTPException(400, "Invalid asset path")
    if not target.exists():
        raise HTTPException(404, f"Asset source not found: {source}")
    return target


@router.get("/services/{service_name}/assets/{asset_id}/download")
async def download_service_asset(service_name: str, asset_id: str):
    service = await mcp_plugin_store.get_service_by_name(service_name)
    if not service:
        raise HTTPException(404, f"MCP service '{service_name}' not found")
    manifest = _manifest_for_service(service)
    asset = next((a for a in (manifest.get("assets") or []) if a.get("id") == asset_id), None)
    if not asset:
        raise HTTPException(404, f"Asset '{asset_id}' not found")
    source = asset.get("source") or ""
    target = _resolve_asset_path(service_name, source)
    file_name = asset.get("file_name") or target.name
    mime = asset.get("mime") or "application/octet-stream"
    kind = asset.get("kind") or "file"

    if kind == "directory_zip":
        if not target.is_dir():
            raise HTTPException(400, "Asset source is not a directory")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in target.rglob("*"):
                if path.is_file():
                    zf.write(path, path.relative_to(target.parent).as_posix())
        buf.seek(0)
        headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
        return StreamingResponse(buf, media_type=mime, headers=headers)

    if kind == "file":
        if not target.is_file():
            raise HTTPException(400, "Asset source is not a file")
        data = target.read_bytes()
        headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
        return StreamingResponse(io.BytesIO(data), media_type=mime, headers=headers)

    raise HTTPException(400, f"Unsupported asset kind: {kind}")


# ── SOP：内置服务的 SOP markdown 在线读写（写回源 .md 文件） ──

def _service_sop_dir(service_name: str) -> Path | None:
    """内置服务的 sop 目录（与 manifest 同级）。"""
    base = _service_manifest_dir(service_name)
    if not base:
        return None
    return base / "sop"


@router.get("/services/{service_name}/sop")
async def get_service_sop(service_name: str, authorization: str | None = Header(None)) -> dict:
    """读取内置服务 sop/*.md 文件内容。自定义服务返回空列表。"""
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service_by_name(service_name)
    if not service:
        raise HTTPException(404, f"MCP service '{service_name}' not found")
    sop_dir = _service_sop_dir(service_name)
    files: list[dict] = []
    if sop_dir and sop_dir.is_dir():
        for p in sorted(sop_dir.glob("*.md")):
            files.append({"name": p.name, "content": p.read_text(encoding="utf-8")})
    return {"service_name": service_name, "files": files}


@router.put("/services/{service_name}/sop")
async def update_service_sop(service_name: str, payload: McpSopRequest, authorization: str | None = Header(None)) -> dict:
    """写回 sop/*.md。仅允许覆盖已存在的文件，禁止新建/删除/路径穿越。"""
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service_by_name(service_name)
    if not service:
        raise HTTPException(404, f"MCP service '{service_name}' not found")
    sop_dir = _service_sop_dir(service_name)
    if not sop_dir or not sop_dir.is_dir():
        raise HTTPException(400, f"Service '{service_name}' has no SOP directory")
    updated: list[dict] = []
    for f in payload.files:
        name = (f.name or "").strip()
        # 仅允许文件名：无路径分隔符、无 .. 。
        if not name or "/" in name or "\\" in name or ".." in name:
            raise HTTPException(400, f"Invalid SOP file name: {f.name!r}")
        if not name.endswith(".md"):
            raise HTTPException(400, f"SOP file must be .md: {name}")
        target = (sop_dir / name).resolve()
        sop_dir_resolved = sop_dir.resolve()
        if not str(target).startswith(str(sop_dir_resolved)):
            raise HTTPException(400, "Invalid SOP path")
        if not target.is_file():
            # 不允许新建文件，避免在仓库里凭空生成 .md。
            raise HTTPException(404, f"SOP file not found: {name}")
        target.write_text(f.content, encoding="utf-8")
        updated.append({"name": name, "bytes": len(f.content.encode("utf-8"))})
    return {"service_name": service_name, "updated": updated}


# ── 配置资源（schema v2 contract）：list/read/create/update/delete/replace + rotate/revoke ──

async def _configuration_context(service_id: int, resource_key: str | None = None):
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    manifest = _manifest_for_service(service)
    if int(manifest.get("schema") or 1) < 2:
        raise HTTPException(409, "该服务尚未声明 schema v2 配置契约")
    if resource_key is None:
        return service, manifest
    try:
        definition, adapter = get_resource_adapter(manifest, resource_key)
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    return ResourceContext(service, definition), adapter


async def _apply_resource_change(service_id: int, definition: dict, authorization: str | None) -> dict | None:
    if "runtime-apply" not in (definition.get("capabilities") or []):
        return None
    # 方向翻转：cdp_client / mail_account 实例携带「可操作用户」，保存后按各实例
    # user_ids 并集回写服务级 mcp_service_users，使连接鉴权 allowed_tokens 立即对齐。
    config_type = str((definition.get("storage") or {}).get("configType") or "")
    if config_type in ("cdp_client", "mail_account"):
        try:
            await mcp_plugin_store.recompute_service_users_from_resources(service_id)
        except Exception as exc:  # noqa: BLE001 — 重算失败不应回滚已保存的实例
            return {"ok": False, "state": "failed", "error": f"授权用户重算失败: {exc}"}
    try:
        result = await _reload_service(service_id, authorization)
    except HTTPException as exc:
        return {"ok": False, "state": "failed", "error": str(exc.detail)}
    if result.get("ok"):
        return {**result, "state": "applied"}
    return {**result, "state": "pending"}


def _resource_capabilities(ctx: ResourceContext, adapter: Any) -> set[str]:
    """Capabilities are allowed only when both manifest and adapter declare them."""
    return set(ctx.definition.get("capabilities") or []) & set(adapter.capabilities)


def _require_resource_capability(ctx: ResourceContext, adapter: Any, capability: str) -> None:
    if capability not in _resource_capabilities(ctx, adapter):
        raise HTTPException(405, f"该资源不支持{capability}")


@router.get("/services/{service_id}/configuration")
async def get_service_configuration(service_id: int, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    service, manifest = await _configuration_context(service_id)
    try:
        contract = configuration_contract(manifest)
    except ConfigurationError as exc:
        raise HTTPException(500, str(exc)) from exc
    contract["service_id"] = service_id
    contract["runtime_status"] = service.get("runtime_status")
    # 方向翻转：可操作用户由实例的 user_ids 决定，因此选项给出所有 enabled 用户，
    # 不再按「已授权该服务」预筛（否则会出现先有鸡还是先有蛋）。
    users = await mcp_plugin_store.list_mcp_users(mask_token=True)
    agents = await mcp_plugin_store.list_all_agents_brief()
    contract["reference_options"] = {
        "mcp_users": [
            {"value": user["id"], "label": user["name"]}
            for user in users if user.get("enabled")
        ],
        "agents": [
            {"value": agent["id"], "label": agent.get("display_name") or agent["name"]}
            for agent in agents
        ],
    }
    return contract


@router.get("/services/{service_id}/resources/{resource_key}")
async def list_configuration_resource(
    service_id: int, resource_key: str, parent_id: int | None = None,
    authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    ctx, adapter = await _configuration_context(service_id, resource_key)
    try:
        capabilities = sorted(_resource_capabilities(ctx, adapter))
        if ctx.definition.get("cardinality") == "singleton":
            _require_resource_capability(ctx, adapter, "read")
            return {"data": await adapter.get(ctx), "meta": {"capabilities": capabilities}}
        _require_resource_capability(ctx, adapter, "list")
        data = await adapter.list(ctx, {"parent_id": parent_id})
        return {"data": data, "meta": {"capabilities": capabilities, "count": len(data)}}
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.put("/services/{service_id}/resources/{resource_key}")
async def replace_configuration_resource(
    service_id: int, resource_key: str, payload: ResourceMutationRequest,
    authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    ctx, adapter = await _configuration_context(service_id, resource_key)
    replace = getattr(adapter, "replace", None)
    _require_resource_capability(ctx, adapter, "replace")
    if not callable(replace):
        raise HTTPException(405, "该资源不支持全量替换")
    values = payload.value.get("items")
    if not isinstance(values, list):
        raise HTTPException(400, "items 必须是数组")
    if payload.expected_revision is None:
        raise HTTPException(400, "全量替换必须提供 expected_revision")
    try:
        for value in values:
            validate_resource_value(ctx.definition, value, partial=False)
        data = await replace(ctx, values)
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    apply = await _apply_resource_change(service_id, ctx.definition, authorization)
    return {"data": data, "meta": {"apply": apply}}


@router.post("/services/{service_id}/resources/{resource_key}")
async def create_configuration_resource(
    service_id: int, resource_key: str, payload: ResourceMutationRequest,
    parent_id: int | None = None, authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    ctx, adapter = await _configuration_context(service_id, resource_key)
    _require_resource_capability(ctx, adapter, "create")
    value = dict(payload.value)
    if parent_id is not None:
        value["parent_id"] = parent_id
    try:
        validate_resource_value(ctx.definition, value, partial=False)
        data = await adapter.create(ctx, value)
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    apply = await _apply_resource_change(service_id, ctx.definition, authorization)
    meta = {"apply": apply}
    if ctx.definition.get("storage", {}).get("configType") == "cdp_client":
        meta["token_once"] = True
    return {"data": data, "meta": meta}


@router.patch("/services/{service_id}/resources/{resource_key}/{item_id}")
async def update_configuration_resource(
    service_id: int, resource_key: str, item_id: str, payload: ResourceMutationRequest,
    parent_id: int | None = None, authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    ctx, adapter = await _configuration_context(service_id, resource_key)
    _require_resource_capability(ctx, adapter, "update")
    patch = dict(payload.value)
    if parent_id is not None:
        patch["parent_id"] = parent_id
    normalized_id: Any = int(item_id) if item_id.isdigit() else item_id
    try:
        validate_resource_value(ctx.definition, patch, partial=True)
        patch["expected_revision"] = payload.expected_revision
        data = await adapter.update(ctx, normalized_id, patch)
    except ConfigurationConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    if data is None:
        raise HTTPException(404, "资源记录不存在")
    apply = await _apply_resource_change(service_id, ctx.definition, authorization)
    return {"data": data, "meta": {"apply": apply}}


@router.delete("/services/{service_id}/resources/{resource_key}/{item_id}")
async def delete_configuration_resource(
    service_id: int, resource_key: str, item_id: str, parent_id: int | None = None,
    expected_revision: int | None = None, authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    ctx, adapter = await _configuration_context(service_id, resource_key)
    _require_resource_capability(ctx, adapter, "delete")
    normalized_id: Any = int(item_id) if item_id.isdigit() else item_id
    try:
        delete_with_query = getattr(adapter, "delete_with_query", None)
        deleted = await delete_with_query(ctx, normalized_id, {"parent_id": parent_id}, expected_revision) if callable(delete_with_query) else await adapter.delete(ctx, normalized_id, expected_revision)
    except ConfigurationConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    if not deleted:
        raise HTTPException(404, "资源记录不存在")
    apply = await _apply_resource_change(service_id, ctx.definition, authorization)
    return {"data": {"ok": True}, "meta": {"apply": apply}}


@router.post("/services/{service_id}/resources/{resource_key}/{item_id}/rotate-token")
async def rotate_configuration_resource_token(
    service_id: int, resource_key: str, item_id: str, response: Response,
    payload: ResourceActionRequest = ResourceActionRequest(), authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    ctx, adapter = await _configuration_context(service_id, resource_key)
    rotate = getattr(adapter, "rotate_token", None)
    if not callable(rotate):
        raise HTTPException(405, "该资源不支持 token 轮换")
    normalized_id: Any = int(item_id) if item_id.isdigit() else item_id
    try:
        data = await rotate(ctx, normalized_id, payload.expected_revision)
    except ConfigurationConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    if data is None:
        raise HTTPException(404, "资源记录不存在")
    apply = await _apply_resource_change(service_id, ctx.definition, authorization)
    return {"data": data, "meta": {"apply": apply, "token_once": True}}


@router.post("/services/{service_id}/resources/{resource_key}/{item_id}/revoke")
async def revoke_configuration_resource_token(
    service_id: int, resource_key: str, item_id: str, payload: ResourceActionRequest = ResourceActionRequest(),
    authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    ctx, adapter = await _configuration_context(service_id, resource_key)
    revoke = getattr(adapter, "revoke_token", None)
    if not callable(revoke):
        raise HTTPException(405, "该资源不支持 token 撤销")
    normalized_id: Any = int(item_id) if item_id.isdigit() else item_id
    try:
        data = await revoke(ctx, normalized_id, payload.expected_revision)
    except ConfigurationConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    if data is None:
        raise HTTPException(404, "资源记录不存在")
    apply = await _apply_resource_change(service_id, ctx.definition, authorization)
    return {"data": data, "meta": {"apply": apply}}


@router.post("/services/{service_id}/actions/{action_key}")
async def execute_configuration_action(
    service_id: int, action_key: str, payload: ResourceMutationRequest, response: Response,
    authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    service, manifest = await _configuration_context(service_id)
    action = ((manifest.get("config") or {}).get("actions") or {}).get(action_key)
    if not isinstance(action, dict):
        raise HTTPException(404, f"未知配置动作: {action_key}")
    action_schema = action.get("inputSchema") or {"type": "object", "properties": {}}
    try:
        validate_resource_value({"schema": action_schema}, payload.value, partial=False)
    except ConfigurationError as exc:
        raise HTTPException(400, str(exc)) from exc
    if action.get("resource") and action.get("operation") in {"rotate-token", "revoke"}:
        resource_key = str(action["resource"])
        ctx, adapter = await _configuration_context(service_id, resource_key)
        item_id = payload.value.get("id", payload.value.get("client_id"))
        if item_id in (None, ""):
            raise HTTPException(400, "id 或 client_id 为必填项")
        method_name = "rotate_token" if action["operation"] == "rotate-token" else "revoke_token"
        method = getattr(adapter, method_name, None)
        if not callable(method):
            raise HTTPException(405, "该资源不支持此动作")
        normalized_id: Any = int(item_id) if str(item_id).isdigit() else str(item_id)
        try:
            data = await method(ctx, normalized_id, payload.value.get("expected_revision"))
        except ConfigurationError as exc:
            raise HTTPException(400, str(exc)) from exc
        if data is None:
            raise HTTPException(404, "资源记录不存在")
        apply = await _apply_resource_change(service_id, ctx.definition, authorization)
        return {"data": data, "meta": {"apply": apply, "token_once": action["operation"] == "rotate-token"}}
    binding = str(action.get("binding") or "")
    if not binding:
        raise HTTPException(500, f"配置动作 binding 无效: {action_key}")
    from mcp_runtime.admin_api import _dispatch_service_capability
    data = await _dispatch_service_capability(service_id, "action", action_key, payload.value)
    return {"data": data, "meta": {"realtime": True}}


@router.get("/services/{service_id}/views/{view_key}")
async def query_configuration_view(
    service_id: int, view_key: str, request: Request, authorization: str | None = Header(None),
) -> dict:
    await _require_admin(authorization)
    _service, manifest = await _configuration_context(service_id)
    view = ((manifest.get("config") or {}).get("views") or {}).get(view_key)
    if not isinstance(view, dict) or not str(view.get("binding") or ""):
        raise HTTPException(404, f"未知配置视图: {view_key}")
    arguments = dict(request.query_params)
    schema = view.get("querySchema") or {"type": "object", "properties": {}}
    if schema.get("properties"):
        try:
            arguments = validate_resource_value({"schema": schema}, arguments, partial=False)
        except ConfigurationError as exc:
            raise HTTPException(400, str(exc)) from exc
    return await _query_configuration_view(service_id, view_key, arguments)


async def _query_configuration_view(service_id: int, view_key: str, arguments: dict) -> dict:
    from mcp_runtime.admin_api import _dispatch_service_capability
    data = await _dispatch_service_capability(service_id, "view", view_key, arguments)
    return {"data": data, "meta": {"realtime": True}}


@router.get("/services/{service_id}")
async def get_service(service_id: int, authorization: str | None = Header(None)) -> dict:
    # 仅管理端：service 含 headers 明文 token，未鉴权会泄露他人凭据。
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    return _mask_service_headers(service)


@router.get("/services/{service_id}/env-vars")
async def list_service_env_vars(service_id: int, authorization: str | None = Header(None)) -> list[dict]:
    await _require_admin(authorization)
    if not await mcp_plugin_store.get_service(service_id):
        raise HTTPException(404, f"MCP service {service_id} not found")
    return await mcp_plugin_store.list_service_env_vars(service_id, mask_secret=True)


@router.put("/services/{service_id}/env-vars")
async def update_service_env_vars(service_id: int, request: UpdateMCPEnvVarsRequest, authorization: str | None = Header(None)) -> list[dict]:
    await _require_admin(authorization)
    if not await mcp_plugin_store.get_service(service_id):
        raise HTTPException(404, f"MCP service {service_id} not found")
    saved = await mcp_plugin_store.replace_service_env_vars(
        service_id,
        [item.model_dump() for item in request.items],
    )
    await _reload_service(service_id, authorization)
    return saved


@router.delete("/services/{service_id}/env-vars/{key}")
async def delete_service_env_var(service_id: int, key: str, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    if not await mcp_plugin_store.get_service(service_id):
        raise HTTPException(404, f"MCP service {service_id} not found")
    deleted = await mcp_plugin_store.delete_service_env_var(service_id, key)
    if not deleted:
        raise HTTPException(404, f"MCP env var '{key}' not found")
    return {"ok": True}


@router.post("/services")
async def create_service(request: CreateMCPServiceRequest, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    scope = derive_deploy_scope(request.transport, request.url)
    # session 形态（stdio）没有「安装」动作：它只是让使用者的 MCP 选项里多一个可选项，
    # 进程由编辑器 CLI 在节点上自己拉起。所以直接以 ready 落库，避免前端显示一个永远
    # 不会推进的假进度条。
    session_ready = scope == DEPLOY_SCOPE_SESSION and not request.template
    install_state = "ready" if session_ready else "created"
    install_step = "随会话在节点启动" if session_ready else ""
    fields = {
        "name": request.name,
        "display_name": request.display_name or request.name,
        "description": request.description,
        "category": request.category,
        "transport": request.transport,
        "command": request.command,
        "args": request.args,
        "env_template": request.env_template,
        "url": request.url,
        "icon": request.icon,
        "version": request.version,
        "author": request.author,
        "docs_url": request.docs_url,
        "install_command": request.install_command,
        "source": request.source,
        "market_id": request.market_id,
        "market_version": request.market_version,
        "template": request.template,
        "enabled": request.enabled,
        "deploy_scope": scope,
        "install_state": install_state,
        "install_step": install_step,
    }
    try:
        service = await mcp_plugin_store.create_service(fields)
    except LookupError:
        raise HTTPException(409, f"MCP service name '{request.name}' already exists")
    except RuntimeError:
        raise HTTPException(503, "Database not available")

    # 全局远程形态：后台拉起安装编排（configuring->starting->testing->ready）。
    # 不阻塞 create 请求；前端轮询 install/status。
    if scope == DEPLOY_SCOPE_SERVER and not request.template:
        from mcp_runtime.installer import kick_off_install
        kick_off_install(service["id"])
    return service


@router.patch("/services/{service_id}")
async def update_service(service_id: int, request: UpdateMCPServiceRequest, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)

    # 先取服务行，按 manifest 的 editable_fields 收紧可改字段（内置服务不能改启动命令等）。
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    manifest = _manifest_for_service(service)
    rules = manifest.get("builtin_rules") or {}
    allowed = set(rules.get("editable_fields") or [])

    updates: dict[str, Any] = {}
    for field_name in (
        "display_name", "description", "category", "transport",
        "command", "url", "icon", "version",
        "author", "docs_url", "install_command", "enabled",
    ):
        value = getattr(request, field_name, None)
        if value is not None:
            if allowed and field_name not in allowed:
                raise HTTPException(403, f"字段 '{field_name}' 不允许修改（当前服务 manifest 限制可编辑字段：{sorted(allowed)}）")
            updates[field_name] = value

    if request.args is not None:
        if allowed and "args" not in allowed:
            raise HTTPException(403, "字段 'args' 不允许修改")
        updates["args"] = request.args

    if request.env_template is not None:
        if allowed and "env_template" not in allowed:
            raise HTTPException(403, "字段 'env_template' 不允许修改")
        updates["env_template"] = request.env_template

    if not updates:
        raise HTTPException(400, "No fields to update")

    updated_service = await mcp_plugin_store.patch_service(service_id, updates)
    if not updated_service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    if request.enabled is not None:
        await _reload_service(service_id, authorization)
    return updated_service


@router.delete("/services/{service_id}")
async def delete_service(service_id: int, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    try:
        deleted = await mcp_plugin_store.delete_service(service_id)
    except PermissionError:
        raise HTTPException(400, "Cannot delete builtin MCP service")
    except RuntimeError:
        raise HTTPException(503, "Database not available")
    if not deleted:
        raise HTTPException(404, f"MCP service {service_id} not found")
    return {"ok": True, "msg": f"MCP service {service_id} deleted"}


@router.post("/services/{service_id}/toggle")
async def toggle_service(service_id: int, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    row = await mcp_plugin_store.toggle_service_enabled(service_id)
    if not row:
        raise HTTPException(404, f"MCP service {service_id} not found")
    apply = await _reload_service(service_id, authorization)
    return {"id": row["id"], "enabled": row["enabled"], "apply": {**apply, "state": "applied" if apply.get("ok") else "pending"}}


@router.post("/services/{service_id}/install/retry")
async def retry_install(service_id: int, authorization: str | None = Header(None)) -> dict:
    """重跑安装编排（从当前失败步推进）。session 形态无安装流程，直接返回 ready。"""
    await _require_admin(authorization)
    state = await mcp_plugin_store.get_install_state(service_id)
    if not state:
        raise HTTPException(404, f"MCP service {service_id} not found")
    if (state.get("deploy_scope") or "server") == DEPLOY_SCOPE_SESSION:
        await mcp_plugin_store.mark_install_state(service_id, "ready", step="随会话在节点启动")
        return {"service_id": service_id, "install_state": "ready"}
    from mcp_runtime.installer import kick_off_install
    kick_off_install(service_id)
    return {"service_id": service_id, "install_state": "starting"}


@router.post("/services/{service_id}/deploy-to-node")
async def deploy_service_to_node(
    service_id: int, req: DeployToNodeRequest, authorization: str | None = Header(None)
) -> dict:
    """形态 C：把一个 stdio MCP 部署到执行节点上常驻，并代理成全局可用的 remote。

    部署成功后 deploy_scope 变为 node_hosted，随即走形态 A 的安装状态机
    （starting->testing->ready）——testing 会经隧道真列一次工具，顺带验证隧道通。
    """
    await _require_admin(authorization)
    from mcp_runtime.node_hosted import NodeHostedError, deploy_to_node

    try:
        result = await deploy_to_node(service_id, req.node_id.strip())
    except NodeHostedError as e:
        raise HTTPException(400, str(e))
    from mcp_runtime.installer import kick_off_install
    kick_off_install(service_id)
    # 裸 dict，与同级 install/status、start、stop 等路由一致（这些走 request.get/post
    # 直取 data，不是 monkeycode 的 {code,message,data} 信封链路）
    return result


@router.post("/services/{service_id}/undeploy-from-node")
async def undeploy_service_from_node(
    service_id: int, authorization: str | None = Header(None)
) -> dict:
    """杀掉节点上的托管进程并把服务打回 session 形态。"""
    await _require_admin(authorization)
    from mcp_runtime.node_hosted import stop_on_node

    stopped = await stop_on_node(service_id)
    await mcp_plugin_store.set_deploy_scope(
        service_id, DEPLOY_SCOPE_SESSION,
        install_state="ready", install_step="随会话在节点启动",
    )
    return {"stopped": stopped}


@router.get("/services/{service_id}/install/status")
async def get_install_status(service_id: int, authorization: str | None = Header(None)) -> dict:
    """前端轮询安装进度用。"""
    await _require_admin(authorization)
    state = await mcp_plugin_store.get_install_state(service_id)
    if not state:
        raise HTTPException(404, f"MCP service {service_id} not found")
    return state


@router.post("/services/{service_id}/test")
async def test_service_connection(service_id: int, authorization: str | None = Header(None)) -> dict:
    """Test that an MCP service config is usable."""
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")

    # Build MCPServerConfig from service dict
    config_dict = {
        "name": service["name"],
        "transport": service.get("transport"),
        "command": service.get("command"),
        "args": service.get("args") or [],
        "env": service.get("env_template") or {},
        "url": service.get("url"),
        "builtin": service.get("builtin"),
    }
    config = MCPServerConfig.from_dict(config_dict)
    mgr = MCPManager()
    result = await mgr.test_config(config)
    await mgr.close()
    return result


@router.get("/services/{service_id}/tools")
async def list_service_tools(
    service_id: int, refresh: bool = False, authorization: str | None = Header(None)
) -> list[dict]:
    """Return cached tools, or force a runtime rediscovery with ``refresh=true``.

    仅管理端：refresh=true 会让后端用该 service 的凭据向外发起连接，未鉴权可被
    滥用驱动他人 token 连外。
    """
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")

    cached = service.get("tools_cache")
    if cached and not refresh:
        out = []
        for t in cached:
            if not isinstance(t, dict):
                continue
            out.append({
                "name": t.get("name") or "",
                "description": t.get("description") or "",
                "input_schema": t.get("input_schema") or t.get("inputSchema") or {"type": "object", "properties": {}},
                "service_name": t.get("service_name") or service["name"],
            })
        if out:
            return out

    config = MCPServerConfig.from_dict({
        "name": service["name"], "transport": service.get("transport"),
        "command": service.get("command"), "args": service.get("args") or [],
        "env": service.get("env_template") or {}, "url": service.get("url"),
        "builtin": service.get("builtin"),
    })
    mgr = MCPManager()
    try:
        tools = await mgr.discover_tools(config)
    except Exception as exc:
        if refresh:
            raise HTTPException(503, f"无法从 MCP Runtime 刷新工具: {exc}") from exc
        raise
    finally:
        await mgr.close()

    normalized = [
        {"name": t.name, "description": t.description, "input_schema": t.input_schema, "service_name": t.service_name}
        for t in tools
    ]
    await mcp_plugin_store.set_tools_cache(service_id, normalized)
    return normalized


def _infer_type(value: Any) -> str:
    """按默认值推断字段类型；None/未知一律 string。"""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def _agent_config_schema(service: dict, version: dict | None = None) -> dict:
    """聚合服务的 per-agent 可配置参数 schema。

    来源：
      1. manifest.agent_config.fields（内置/市场声明，优先级最高）
      2. active version config_json.agent_config.fields 或 config_schema 中 scope='agent' 的字段

    注意：env_template 是服务运行配置（端口、token、secret 等），不自动暴露为 Agent 配置；
    需要出现在 Agent 编辑器里的字段必须显式声明为 agent_config。

    同 key 去重：manifest > config_json。返回 {service_name, fields}。
    version 由路由层异步取好后传入；纯函数，便于单测。
    """
    name = service.get("name") or ""
    fields: list[dict] = []
    seen: set[str] = set()

    def add_field(f: dict, source: str) -> None:
        if not isinstance(f, dict):
            return
        key = f.get("key")
        if not key or key in seen:
            return
        seen.add(key)
        fields.append({
            "key": key,
            "label": f.get("label") or key,
            "type": f.get("type") or _infer_type(f.get("default")),
            "default": f.get("default"),
            "description": f.get("description") or "",
            "required": bool(f.get("required")),
            "enum": f.get("enum"),
            "secret": bool(f.get("secret")),
            "source": source,
        })

    # 1) manifest.agent_config.fields —— 优先级最高，先放进去
    manifest = _manifest_for_service(service) or {}
    for f in (manifest.get("agent_config") or {}).get("fields", []) or []:
        add_field(f, "manifest")

    # 2) active version config_json —— 仅接收显式声明为 Agent 侧配置的字段
    if version:
        cfg_json = version.get("config_json") or {}
        if isinstance(cfg_json, dict):
            agent_fields = (cfg_json.get("agent_config") or {}).get("fields")
            if isinstance(agent_fields, list):
                entries = [f for f in agent_fields if isinstance(f, dict)]
            else:
                declared = cfg_json.get("config_schema")
                entries = [
                    f for f in declared
                    if isinstance(f, dict) and f.get("scope") == "agent"
                ] if isinstance(declared, list) else []
            for f in entries:
                add_field(f, "config_json")

    return {"service_name": name, "fields": fields}


@router.get("/services/{service_id}/agent-config-schema")
async def get_service_agent_config_schema(service_id: int, authorization: str | None = Header(None)) -> dict:
    """返回 MCP 服务在 Agent 编辑器里可配置的参数 schema（显式 agent_config 声明）。"""
    await _require_admin(authorization)
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    version = await mcp_plugin_store.get_active_version(service_id)
    return _agent_config_schema(service, version)
