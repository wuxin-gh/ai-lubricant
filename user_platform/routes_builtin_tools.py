"""C-side 一级工具资源路由（CDP 客户端 / 邮箱服务 / 设备控制）。

资源直接归属当前用户；不再存在 builtin_tool_instances 或 instance_id API。
Storage only —— 不触碰模型请求主链路。

路由注册顺序约束：所有静态子路径（/resources/cdp-clients、/resources/mail-services、
/resources/devices、/resources/device-pairing-codes 及其子路径）必须注册在
/resources/{resource_id} 之前——FastAPI 按注册顺序匹配，动态段在前会把
"cdp-clients" 当 resource_id 解析整数失败而 422。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .deps import get_current_user
from .models import User

router = APIRouter(prefix="/api/v1/users/builtin-tools", tags=["user-platform-builtin-tools"])


class CreateCdpClientReq(BaseModel):
    name: str


class CreateMailAccountReq(BaseModel):
    display_name: str
    mailbox_type: str = "TempMail"
    base_url: str | None = None
    username: str | None = None
    password: str | None = None
    secret_key: str | None = None
    mail_suffix: str | None = None
    enabled: bool = True


class CreateMailAddressReq(BaseModel):
    address: str
    source_address: str | None = None


class MailQueryReq(BaseModel):
    address: str | None = None
    keyword: str | None = None
    limit: int = 20
    offset: int = 0


class MintPairingCodeReq(BaseModel):
    label: str = ""


class UpdateResourceReq(BaseModel):
    data: dict[str, Any] = {}
    expected_revision: int | None = None


class IssueTokenReq(BaseModel):
    target_type: str
    target_id: str | None = None
    display_token: bool = False
    expires_at: datetime | None = None


async def _owned_resource(resource_id: int, user: User, *, resource_type: str | None = None) -> dict:
    import builtin_tool_store as store

    resource = await store.get_resource(resource_id, owner_user_id=str(user.id))
    if resource is None or (resource_type and resource.get("resource_type") != resource_type):
        raise HTTPException(status_code=404, detail="资源不存在")
    return resource


async def _resync_builtin(name: str) -> None:
    from mcp_runtime.configuration import resync_builtin_service
    try:
        await resync_builtin_service(name)
    except Exception:
        from loguru import logger
        logger.warning("[builtin-tools] resync '{}' failed (data is persisted)", name)


async def _resync_resource(resource_type: str) -> None:
    await _resync_builtin(
        "cdp-bridge" if resource_type == "cdp_client"
        else "mail" if resource_type in ("mail_account", "mail_address")
        else "device-control"
    )


# ── 一级资源 CRUD ─────────────────────────────────────────────────────────────

@router.get("/resources")
async def list_resources(resource_type: str | None = None, user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    return {"resources": await store.list_resources(resource_type, owner_user_id=str(user.id))}


@router.post("/resources")
async def create_resource(body: UpdateResourceReq, user: User = Depends(get_current_user)) -> dict:
    resource_type = str(body.data.get("resource_type") or body.data.get("detail_type") or "")
    if resource_type not in {"cdp_client", "mail_account", "mail_address", "device"}:
        raise HTTPException(status_code=400, detail="不支持的资源类型")
    import builtin_tool_store as store
    value = {k: v for k, v in body.data.items() if k not in ("resource_type", "detail_type")}
    row = await store.create_resource(str(user.id), resource_type, value)
    await _resync_resource(resource_type)
    return row


# ── CDP 客户端资源 ────────────────────────────────────────────────────────────

@router.get("/resources/cdp-clients")
async def list_cdp_clients_resource(user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    resources = await store.list_resources("cdp_client", owner_user_id=str(user.id))
    try:
        from mcp_builtin.cdp_bridge import server as cdp_server
        contexts = cdp_server.get_driver().snapshot_contexts()
    except Exception:
        contexts = []
    live_by_id: dict[str, dict] = {}
    for context in contexts or []:
        for live in context.get("clients") or []:
            live_by_id[str(live.get("client_id") or "")] = live
    for resource in resources:
        live = live_by_id.get(str(resource.get("id"))) or {}
        resource["connected"] = bool(live.get("connected"))
        resource["pages"] = live.get("pages") or []
    return {"clients": resources}


@router.post("/resources/cdp-clients")
async def create_cdp_client_resource(body: CreateCdpClientReq, user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="客户端名称必填")
    client, token = await store.create_cdp_client(str(user.id), body.name.strip())
    await _resync_builtin("cdp-bridge")
    return {"client": client, "token": token}


@router.post("/resources/cdp-clients/{resource_id}/rotate-token")
async def rotate_cdp_client_resource(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    await _owned_resource(resource_id, user, resource_type="cdp_client")
    import builtin_tool_store as store
    try:
        client, token = await store.rotate_cdp_token(resource_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="客户端不存在") from exc
    await _resync_builtin("cdp-bridge")
    return {"client": client, "token": token}


@router.post("/resources/cdp-clients/{resource_id}/revoke-token")
async def revoke_cdp_client_resource(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    await _owned_resource(resource_id, user, resource_type="cdp_client")
    import builtin_tool_store as store
    client = await store.revoke_cdp_token(resource_id)
    if client is None:
        raise HTTPException(status_code=404, detail="客户端不存在")
    await _resync_builtin("cdp-bridge")
    return {"client": client}


# ── 邮箱服务资源 ──────────────────────────────────────────────────────────────

@router.get("/resources/mail-services")
async def list_mail_services(user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    accounts = await store.list_resources("mail_account", owner_user_id=str(user.id))
    addresses = await store.list_resources("mail_address", owner_user_id=str(user.id))
    for account in accounts:
        account["resource_id"] = int(account["id"])
        account["addresses"] = addresses
    return {"services": accounts}


@router.post("/resources/mail-services")
async def create_mail_service(body: CreateMailAccountReq, user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    if not body.display_name.strip():
        raise HTTPException(status_code=400, detail="邮箱服务名称必填")
    value = body.model_dump(exclude_none=True)
    row = await store.create_resource(str(user.id), "mail_account", value)
    row["resource_id"] = int(row["id"])
    row["addresses"] = []
    await _resync_builtin("mail")
    return row


@router.delete("/resources/mail-services/{resource_id}")
async def delete_mail_service(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    await _owned_resource(resource_id, user, resource_type="mail_account")
    deleted = await store.delete_resource(resource_id, owner_user_id=str(user.id))
    await _resync_builtin("mail")
    return {"deleted": deleted}


@router.post("/resources/mail-services/{resource_id}/addresses")
async def create_mail_service_address(resource_id: int, body: CreateMailAddressReq, user: User = Depends(get_current_user)) -> dict:
    account = await _owned_resource(resource_id, user, resource_type="mail_account")
    import builtin_tool_store as store
    value = body.model_dump(exclude_none=True)
    value["parent_resource_id"] = resource_id
    value["account_resource_id"] = resource_id
    row = await store.create_resource(str(user.id), "mail_address", value)
    await _resync_builtin("mail")
    return row


@router.post("/resources/mail-services/{resource_id}/query")
async def query_mail_service(resource_id: int, body: MailQueryReq, user: User = Depends(get_current_user)) -> dict:
    await _owned_resource(resource_id, user, resource_type="mail_account")
    import builtin_tool_store as store
    try:
        return await store.mail_query_for_resource(
            resource_id, address=body.address or "", keyword=body.keyword or "",
            limit=body.limit, offset=body.offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ── 设备控制资源 ──────────────────────────────────────────────────────────────

@router.get("/resources/devices")
async def list_devices_resource(user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    resources = await store.list_resources("device", owner_user_id=str(user.id))
    try:
        from mcp_builtin.device_control import driver as dc_driver
        live_rows = dc_driver.get_driver().snapshot_devices()
    except Exception:
        live_rows = []
    live_by_id = {str(r.get("device_id") or ""): r for r in live_rows or []}
    from datetime import datetime, timezone
    for resource in resources:
        live = live_by_id.get(str(resource.get("device_id") or "")) or {}
        info = live.get("device_info") or resource.get("device_info") or {}
        resource["device_info"] = info
        resource["platform"] = str(info.get("platform") or ("ios" if info.get("node_id") else "android"))
        resource["online"] = bool(live)
        resource["capabilities"] = live.get("capabilities") or []
        # 在线设备的「最后在线」用内存里心跳刷新的 last_seen（epoch 秒，每帧都 touch），
        # 离线设备没 live，落库的 last_seen_at（断连时刻）兜底——两层拼接出准点的时间。
        live_last_seen = live.get("last_seen")
        if isinstance(live_last_seen, (int, float)):
            resource["last_seen_at"] = datetime.fromtimestamp(
                live_last_seen, timezone.utc,
            ).isoformat(timespec="seconds")
        # last_error / last_error_at 由 _resource_value 把 data JSONB 扁平化到顶层，
        # 已自带，无需在此再拼。node_id 同理。
        node_id = str(info.get("node_id") or "")
        if node_id:
            resource["node_id"] = node_id
    return {"devices": resources}


@router.post("/resources/device-pairing-codes")
async def create_device_pairing_resource(body: MintPairingCodeReq, user: User = Depends(get_current_user)) -> dict:
    # Pairing codes are scoped to the user's device resources in the store; the
    # device-control protocol accepts the code once and creates a direct resource.
    import mcp_builtin.device_control.store as dc_store
    try:
        code, ttl = await dc_store.mint_pairing_code_for_owner(str(user.id), body.label)
    except AttributeError:
        raise HTTPException(status_code=501, detail="设备配对资源迁移尚未完成")
    except dc_store.RedisUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"code": code, "ttl": ttl}


# ── iOS device management (role ios_host) ───────────────────────────────────────
@router.get("/resources/ios-hosts")
async def list_ios_hosts(user: User = Depends(get_current_user)) -> dict:
    """List available iOS host nodes the user can access."""
    from .nodes_service import nodes_service

    my_nodes = (await nodes_service.list_my_nodes(user.id)).get("nodes") or []
    hosts = [n for n in my_nodes if n.get("role") == "ios_host"]
    return {"ios_hosts": hosts}


@router.get("/resources/ios-hosts/{node_id}/devices")
async def list_ios_devices(node_id: str, user: User = Depends(get_current_user)) -> dict:
    """Retrieve the cached device inventory for one ios_host node."""
    from .nodes_service import nodes_service
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

    if not await nodes_service.user_can_use_node(user.id, node_id):
        raise HTTPException(status_code=403, detail="您无权访问此节点")

    client = get_node_client()
    try:
        return await client.get_ios_devices(node_id)
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "permission_denied":
            raise HTTPException(status_code=403, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc


@router.post("/resources/ios-claims")
async def claim_ios_device(
    body: dict,
    user: User = Depends(get_current_user),
) -> dict:
    """Claim an iOS device: mint a pairing code, dispatch claim frame, wait for ack."""
    from .nodes_service import nodes_service
    from .node_client import get_node_client, NodeServerUnavailable, RPCError
    import mcp_builtin.device_control.store as dc_store
    from server import builtin_tool_store

    node_id = (body.get("node_id") or "").strip()
    udid = (body.get("udid") or "").strip()
    label = (body.get("label") or "").strip()
    if not node_id or not udid:
        raise HTTPException(status_code=400, detail="node_id and udid are required")

    if not await nodes_service.user_can_use_node(user.id, node_id):
        raise HTTPException(status_code=403, detail="您无权访问此节点")

    # Conflict check: is this UDID already claimed by anyone?
    existing = await builtin_tool_store.list_resources(
        owner_user_id=None, resource_type="device"
    )
    for res in existing:
        info = (res.get("data") or {}).get("device_info") or {}
        ios_info = (res.get("data") or {}).get("ios") or {}
        if ios_info.get("udid") == udid:
            raise HTTPException(status_code=409, detail=f"UDID {udid} 已被认领")

    # Mint a one-time pairing code
    try:
        code, ttl = await dc_store.mint_pairing_code_for_owner(str(user.id), label)
    except dc_store.RedisUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    # Dispatch the claim frame
    client = get_node_client()
    try:
        result = await client.ios_claim_device(node_id, udid, label, code)
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "permission_denied":
            raise HTTPException(status_code=403, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        if exc.code == "deadline_exceeded":
            raise HTTPException(status_code=504, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

    return result


@router.post("/resources/{resource_id}/release-device")
async def release_ios_device(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    """Release a claimed iOS device: dispatch release frame, stop the node-side goroutine."""
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

    resource = await _owned_resource(resource_id, user, resource_type="device")
    data = resource.get("data") or {}
    ios_info = data.get("ios") or {}
    device_id = data.get("device_id") or ""
    udid = ios_info.get("udid") or ""
    node_id = ios_info.get("node_id") or ""

    if not node_id or not udid:
        raise HTTPException(
            status_code=400, detail="此设备不是 iOS 设备或缺少节点绑定信息"
        )

    client = get_node_client()
    try:
        await client.ios_release_device(node_id, device_id, udid, delete_credential=True)
    except NodeServerUnavailable:
        pass  # node offline: the device is already disconnected
    except RPCError:
        pass  # node-side failure: still revoke the resource

    # Revoke the device resource (same as the existing Android path)
    from server import builtin_tool_store

    await builtin_tool_store.revoke_device(resource_id)
    await _resync_builtin("device-control")
    return {"resource_id": resource_id, "device_id": device_id}


@router.post("/resources/{resource_id}/revoke-device")
async def revoke_device_resource(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    """解除设备资源配对：清 token 并让在线连接收到 close 4003。"""
    await _owned_resource(resource_id, user, resource_type="device")
    import builtin_tool_store as store

    device = await store.revoke_device(resource_id)
    if device is None:
        raise HTTPException(status_code=404, detail="设备不存在")
    await _resync_builtin("device-control")
    return {"device": device}


# ── 通用资源 CRUD（动态 {resource_id}）——必须注册在所有静态子路径之后，
# 否则 FastAPI 会把 "cdp-clients"/"mail-services" 等当作 resource_id 解析而 422。
@router.get("/resources/{resource_id}")
async def get_resource(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    return await _owned_resource(resource_id, user)


@router.patch("/resources/{resource_id}")
@router.put("/resources/{resource_id}")
async def update_resource(resource_id: int, body: UpdateResourceReq, user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    existing = await _owned_resource(resource_id, user)
    try:
        row = await store.update_resource(
            resource_id, body.data, owner_user_id=str(user.id), expected_revision=body.expected_revision
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="资源不存在")
    await _resync_resource(str(existing.get("resource_type") or ""))
    return row


@router.delete("/resources/{resource_id}")
async def delete_resource(resource_id: int, user: User = Depends(get_current_user)) -> dict:
    import builtin_tool_store as store
    existing = await _owned_resource(resource_id, user)
    deleted = await store.delete_resource(resource_id, owner_user_id=str(user.id))
    await _resync_resource(str(existing.get("resource_type") or ""))
    return {"deleted": deleted}


# ── 外部 MCP 接入说明与 CDP connection info ───────────────────────────────────

_BUILTIN_MCP_SERVICE_NAMES = {"cdp": "cdp-bridge", "mail": "mail", "device": "device-control"}


@router.get("/mcp-integration/{tool_kind}")
async def mcp_integration_metadata(tool_kind: str, user: User = Depends(get_current_user)) -> dict:
    import mcp_plugin_store
    from mcp_runtime.configuration import _manifest_for_service
    from mcp_runtime.sse_gateway import _tools_for_service

    service_name = _BUILTIN_MCP_SERVICE_NAMES.get(str(tool_kind).lower())
    if service_name is None:
        raise HTTPException(status_code=400, detail="不支持的内置工具类型")
    service = await mcp_plugin_store.get_service_by_name(service_name)
    if service is None:
        raise HTTPException(status_code=404, detail="内置 MCP 服务不存在")
    from mcp.api import _runtime_public_base
    base = _runtime_public_base()
    manifest = _manifest_for_service(service)
    try:
        tools = _tools_for_service(service_name)
    except Exception:
        tools = []
    return {
        "tool_kind": tool_kind,
        "service_name": service_name,
        "display_name": service.get("display_name") or manifest.get("display_name") or service_name,
        "description": service.get("description") or manifest.get("description") or "",
        "transport": "sse",
        "sse_url": f"{base}/mcp/{service_name}/sse",
        "token_url": f"{base}/mcp/{service_name}/sse?token=<TOKEN>",
        "tools": tools,
        "resources": [], "actions": [], "views": [],
        "runtime_status": service.get("runtime_status"),
        "enabled": bool(service.get("enabled", True)),
    }


@router.get("/cdp/connection-info")
async def cdp_connection_info(user: User = Depends(get_current_user)) -> dict:
    from mcp.api import _runtime_public_base
    base = _runtime_public_base()
    ws_base = base.replace("http://", "ws://", 1).replace("https://", "wss://", 1)
    return {
        "ws_session_url": f"{ws_base}/mcp/cdp-bridge/session",
        "extension_download_url": "/mcp/services/cdp-bridge/assets/chrome-extension/download",
        "extension_file_name": "cdp-bridge-extension.zip",
    }
