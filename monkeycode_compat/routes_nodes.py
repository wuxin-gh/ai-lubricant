"""Node orchestration routes — agent-compose nodes ↔ team groups.

Two surfaces, one service (:mod:`nodes_service`):

* **Admin (global)** ``/api/v1/admin/nodes*`` — drives the in-process NodeService
  control plane (list/onboard/approve/revoke) and owns node↔group bindings.
  Gated to platform admins (or the emergency admin token). These admin routes are
  the *sole* control entrypoint: the node server no longer exposes an
  unauthenticated HTTP control surface.
* **Team self-service** ``/api/v1/teams/{...}/groups/{gid}/nodes*`` — a group's
  members act on the nodes bound to their group. The permission is derived from
  the bound node's role (management ⇒ may launch execution nodes).

If the node server is switched off (the emergency escape hatch), control-plane
calls raise ``NodeServerUnavailable`` which is surfaced as HTTP 503.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .node_client import NodeServerUnavailable, RPCError
from .deps import client_meta, get_current_team_id, get_current_user
from .models import TeamGroup, User
from .nodes_service import (
    NodesServiceError,
    latest_node_version,
    list_node_binaries,
    nodes_service,
    resolve_node_binary,
)
from .team_users_service import team_users_service
from . import shell_approval_service
from .node_client.tools import PRESET_APPROVAL_CATALOG
from .routes_shell_approval import _normalize_shell, _validate_command_key


async def _group_team_id(group_id: str) -> str | None:
    """Resolve a group's owning team id for audit scoping (None on any failure).

    ``mc_audits`` is team-scoped; the node delete surfaces only carry a group
    id, so we look up the group's team. A miss must never block the unbind, so
    every failure degrades to ``None`` and the caller simply skips the audit.
    """
    try:
        import uuid as _uuid

        group = await TeamGroup.get_or_none(id=_uuid.UUID(str(group_id)))
        return str(group.team_id) if group is not None else None
    except Exception:
        return None

# Admin-global node control plane.
admin_router = APIRouter(prefix="/api/v1/admin", tags=["monkeycode-nodes-admin"])
# Team self-service node surface.
team_router = APIRouter(prefix="/api/v1/teams", tags=["monkeycode-nodes-team"])


# ── request bodies ──────────────────────────────────────────────────────────


class OnboardNodeReq(BaseModel):
    role: str = "execution"  # execution | management
    startup_method: str = "standalone"  # standalone | systemd | docker | docker-compose
    node_name: str | None = None
    manager_node_id: str | None = None  # execution role only; empty = synthetic manager
    labels: dict[str, str] | None = None
    proxy_config_id: str = ""  # 该节点绑定的出口代理池条目；空=直连


class BindNodeReq(BaseModel):
    node_id: str


class CreateExecutionNodeReq(BaseModel):
    startup_method: str = "docker"
    node_name: str | None = None
    image: str | None = None


class PutNodeShellApprovalReq(BaseModel):
    shell_flavor: str = Field(..., description="posix / powershell / cmd / unknown")
    note: str | None = None


# ── auth ──────────────────────────────────────────────────────────────────


async def _require_admin(user: User = Depends(get_current_user)) -> User:
    """Gate admin-global node endpoints to platform administrators.

    The emergency admin token resolves to a role=admin user (see deps), so this
    also authorizes the emergency channel — matching the rest of the console.
    """
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


# ── error mapping ───────────────────────────────────────────────────────────

_REASON_STATUS = {
    "group_not_found": 404,
    "node_not_found": 404,
    "node_not_bound": 404,
    "node_id_required": 400,
    "no_manage_rights": 403,
}

# In-process NodeService RPCError code → HTTP status (Connect's canonical map).
_RPC_CODE_STATUS = {
    "canceled": 499,
    "unknown": 500,
    "invalid_argument": 400,
    "deadline_exceeded": 504,
    "not_found": 404,
    "permission_denied": 403,
    "failed_precondition": 412,
    "unavailable": 503,
    "internal": 500,
}


def _raise_domain(exc: NodesServiceError) -> None:
    status = _REASON_STATUS.get(exc.reason, 400)
    raise HTTPException(status_code=status, detail=str(exc))


async def _require_execution_node(node_id: str) -> dict:
    row = await _guard(nodes_service.get_node_if_exists(node_id))
    if row is None:
        raise HTTPException(status_code=404, detail="节点不存在")
    role = str(row.get("role") or "").strip()
    is_passive = bool(row.get("is_passive"))
    if role != "execution" or is_passive:
        raise HTTPException(status_code=422, detail="仅执行节点支持授权配置")
    return row


async def _audit_node_shell_approval(
    request: Request,
    user: User,
    operation: str,
    node_id: str,
    request_payload: dict,
    response_payload: dict | None,
) -> None:
    """配置治理是 best-effort：审计失败不能让一次已完成的配置变更报错。"""
    try:
        from .deps import resolve_team_id

        ip, ua = client_meta(request)
        await team_users_service.record_audit(
            await resolve_team_id(str(user.id)),
            str(user.id),
            operation,
            request={"node_id": node_id, **request_payload},
            response=response_payload,
            source_ip=ip,
            user_agent=ua,
        )
    except Exception:  # noqa: BLE001
        return


async def _guard(coro):
    """Run a service coroutine, mapping domain/orchestration errors to HTTP."""
    try:
        return await coro
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"节点服务未启用：{exc}")
    except RPCError as exc:
        # In-process NodeService domain error (invalid_argument/not_found/…).
        raise HTTPException(status_code=_RPC_CODE_STATUS.get(exc.code, 400), detail=exc.message)
    except NodesServiceError as exc:
        _raise_domain(exc)


# ── admin: daemon control plane ───────────────────────────────────────────


@admin_router.get("/nodes")
async def admin_list_nodes(status: str | None = None, _: User = Depends(_require_admin)) -> dict:
    return await _guard(nodes_service.list_nodes(status))


def _node_server_url(request: Request) -> str:
    """Public origin a remote node agent dials back to.

    Prefers the configured ``node_server_public_url`` (set behind a reverse
    proxy / for a stable console URL); otherwise derives it from the current
    request origin so a single-host deployment needs no extra config.

    Note this is the DIAL-BACK origin, not the control-plane address: the node
    control process is separate (``python -m node_server``), and this data
    service reaches it via ``agent_compose_base_url`` / ``node_control_token``
    (see ``node_client/client.py``); when that is unreachable, node listings
    surface HTTP 503.
    """
    from . import config

    configured = (config.settings.node_server_public_url or "").strip().rstrip("/")
    if configured:
        return configured
    # Derive from the request: scheme://host (host already includes any port).
    return f"{request.url.scheme}://{request.url.netloc}"


@admin_router.post("/nodes/onboard")
async def admin_onboard_node(
    body: OnboardNodeReq, request: Request, _: User = Depends(_require_admin)
) -> dict:
    result = await _guard(
        nodes_service.onboard_node(
            role=body.role,
            startup_method=body.startup_method,
            node_name=body.node_name,
            manager_node_id=body.manager_node_id,
            labels=body.labels,
            proxy_config_id=body.proxy_config_id,
        )
    )
    # A secret only exists for an installable node. Persist it briefly so a new
    # node machine can fetch a self-contained bootstrap script by node_id without
    # ever receiving an administrator session or putting the secret in its URL.
    if result.get("node_id") and result.get("secret"):
        try:
            from .nodes_service import save_node_bootstrap

            await save_node_bootstrap(
                result["node_id"],
                secret=result["secret"],
                server_url=_node_server_url(request),
                role=(result.get("node") or {}).get("role") or body.role,
                startup_method=(result.get("node") or {}).get("startup_method") or body.startup_method,
                proxy_config_id=body.proxy_config_id,
            )
        except Exception:
            # Redis bootstrap is a convenience; the primary daemon onboarding
            # result remains valid even if this short-lived helper is unavailable.
            from loguru import logger

            logger.warning("[nodes] failed to save node bootstrap", exc_info=True)
    return result


@admin_router.post("/nodes/{node_id}/approve")
async def admin_approve_node(node_id: str, _: User = Depends(_require_admin)) -> dict:
    return await _guard(nodes_service.approve_node(node_id))


@admin_router.post("/nodes/{node_id}/revoke")
async def admin_revoke_node(node_id: str, _: User = Depends(_require_admin)) -> dict:
    return await _guard(nodes_service.revoke_node(node_id))


@admin_router.get("/nodes/{node_id}/shell-approval/catalog")
async def admin_node_shell_approval_catalog(
    node_id: str, _: User = Depends(_require_admin)
) -> dict:
    node = await _require_execution_node(node_id)
    grouped = {flavor: [] for flavor in ("posix", "powershell", "cmd", "unknown")}
    for item in PRESET_APPROVAL_CATALOG:
        grouped.setdefault(item["shell"], []).append(item)
    return {
        "node_id": node_id,
        "actual_shell_flavor": shell_approval_service.infer_node_shell_flavor(node),
        "catalog": grouped,
    }


@admin_router.get("/nodes/{node_id}/shell-approval")
async def admin_list_node_shell_approval(
    node_id: str, _: User = Depends(_require_admin)
) -> dict:
    node = await _require_execution_node(node_id)
    return {
        "node_id": node_id,
        "actual_shell_flavor": shell_approval_service.infer_node_shell_flavor(node),
        "policies": await shell_approval_service.list_node_policy(node_id),
    }


@admin_router.put("/nodes/{node_id}/shell-approval/{command_key}")
async def admin_put_node_shell_approval(
    node_id: str,
    command_key: str,
    body: PutNodeShellApprovalReq,
    request: Request,
    user: User = Depends(_require_admin),
) -> dict:
    await _require_execution_node(node_id)
    shell = _normalize_shell(body.shell_flavor)
    if len(body.note or "") > 255:
        raise HTTPException(status_code=422, detail="note 过长")
    note = (body.note or "").strip() or None
    key = _validate_command_key(command_key, shell)
    result = await shell_approval_service.upsert_node_policy(node_id, key, shell, note)
    await _audit_node_shell_approval(
        request, user, "node.shell_approval.upsert", node_id,
        {"command_key": key, "shell_flavor": shell, "note": note}, result,
    )
    return result


@admin_router.delete("/nodes/{node_id}/shell-approval/{command_key}")
async def admin_delete_node_shell_approval(
    node_id: str,
    command_key: str,
    shell_flavor: str,
    request: Request,
    user: User = Depends(_require_admin),
) -> dict:
    await _require_execution_node(node_id)
    shell = _normalize_shell(shell_flavor)
    key = (command_key or "").strip().lower()
    if not key:
        raise HTTPException(status_code=422, detail="command_key 不能为空")
    deleted = await shell_approval_service.delete_node_policy(node_id, key, shell)
    result = {"deleted": deleted}
    await _audit_node_shell_approval(
        request, user, "node.shell_approval.delete", node_id,
        {"command_key": key, "shell_flavor": shell}, result,
    )
    return result


@admin_router.delete("/nodes/{node_id}/shell-approval")
async def admin_clear_node_shell_approval(
    node_id: str,
    request: Request,
    shell_flavor: str | None = None,
    user: User = Depends(_require_admin),
) -> dict:
    await _require_execution_node(node_id)
    shell = _normalize_shell(shell_flavor) if shell_flavor is not None else None
    deleted = await shell_approval_service.clear_node_policy(node_id, shell)
    result = {"deleted": deleted}
    await _audit_node_shell_approval(
        request, user, "node.shell_approval.clear", node_id,
        {"shell_flavor": shell}, result,
    )
    return result


@admin_router.delete("/nodes/{node_id}")
async def admin_delete_node(node_id: str, _: User = Depends(_require_admin)) -> dict:
    """Hard-delete a node record (any status) so it disappears from listings.

    Unlike ``revoke`` (which keeps the record in a REVOKED state for audit),
    this removes the row entirely. Uses ``force`` so an online node can still
    be removed; the management-node-still-owning-execution guard still applies
    (delete or reassign its execution nodes first).
    """
    return await _guard(nodes_service.delete_node(node_id))


@admin_router.delete("/nodes/{node_id}/onboard")
async def admin_revoke_onboard_node(node_id: str, _: User = Depends(_require_admin)) -> dict:
    return await _guard(nodes_service.revoke_onboard_node(node_id))


class PublicIPLookupConfigReq(BaseModel):
    ipv4_urls: list[str] = Field(default_factory=list)
    ipv6_urls: list[str] = Field(default_factory=list)


class NodeProxyConfigReq(BaseModel):
    proxy_config_id: str = ""


@admin_router.get("/nodes/{node_id}/proxy-config")
async def admin_get_node_proxy_config(node_id: str, _: User = Depends(_require_admin)) -> dict:
    """读取节点绑定的出口代理（proxy_config_id + 解析后的三字段）。"""
    return await _guard(nodes_service.get_node_proxy_config(node_id))


@admin_router.put("/nodes/{node_id}/proxy-config")
async def admin_update_node_proxy_config(
    node_id: str, body: NodeProxyConfigReq, _: User = Depends(_require_admin)
) -> dict:
    """更新节点绑定的出口代理并推送给该在线节点（空 id 即直连）。"""
    return await _guard(nodes_service.update_node_proxy_config(node_id, body.proxy_config_id))


@admin_router.get("/nodes/public-ip-lookup-config")
async def admin_get_public_ip_lookup_config(_: User = Depends(_require_admin)) -> dict:
    """读取所有真实节点共享的公网 IP 探测地址列表。"""
    return await _guard(nodes_service.get_public_ip_lookup_config())


@admin_router.put("/nodes/public-ip-lookup-config")
async def admin_update_public_ip_lookup_config(
    body: PublicIPLookupConfigReq, _: User = Depends(_require_admin)
) -> dict:
    """更新全局 IPv4/IPv6 探测地址并实时下发到在线节点。"""
    return await _guard(
        nodes_service.update_public_ip_lookup_config(body.ipv4_urls, body.ipv6_urls)
    )


@admin_router.get("/global-config/node-global")
async def admin_get_node_global_config(_: User = Depends(_require_admin)) -> dict:
    """读取节点全局公网 IP 探测配置。"""
    return await _guard(nodes_service.get_public_ip_lookup_config())


@admin_router.put("/global-config/node-global")
async def admin_update_node_global_config(
    body: PublicIPLookupConfigReq, _: User = Depends(_require_admin)
) -> dict:
    """更新节点全局公网 IP 探测配置并实时下发。"""
    return await _guard(
        nodes_service.update_public_ip_lookup_config(body.ipv4_urls, body.ipv6_urls)
    )


    manager_node_id: str


class NodeCapacityReq(BaseModel):
    max_sessions: int | None = None
    cpu_total: float | None = None
    memory_total: int | None = None


@admin_router.patch("/nodes/{node_id}/capacity")
async def admin_set_node_capacity(
    node_id: str, body: NodeCapacityReq, _: User = Depends(_require_admin)
) -> dict:
    """配置节点最大并发 session 数、可分配 CPU 核数与内存字节数。"""
    capacity = {
        key: value
        for key, value in body.model_dump(exclude_none=True).items()
        if float(value) > 0
    }
    return await _guard(nodes_service.set_node_capacity(node_id, capacity))


@admin_router.post("/nodes/{node_id}/move")
async def admin_move_node(
    node_id: str, body: MoveNodeReq, _: User = Depends(_require_admin)
) -> dict:
    """把执行节点挪到 body.manager_node_id 指定的管理节点下。"""
    return await _guard(nodes_service.move_node(node_id, body.manager_node_id))


@admin_router.post("/nodes/{node_id}/editors/{editor}/install")
async def admin_install_node_editor(
    node_id: str, editor: str, _: User = Depends(_require_admin)
) -> dict:
    """在该节点上安装一个受支持的编辑器 CLI（claude/codex/gemini/opencode）。"""
    return await _guard(nodes_service.manage_editor(node_id, editor, "install"))


@admin_router.post("/nodes/{node_id}/editors/{editor}/upgrade")
async def admin_upgrade_node_editor(
    node_id: str, editor: str, _: User = Depends(_require_admin)
) -> dict:
    """升级该节点上一个已安装的编辑器 CLI 到最新版本。"""
    return await _guard(nodes_service.manage_editor(node_id, editor, "upgrade"))



@admin_router.get("/nodes/latest-version")
async def admin_latest_node_version(_: User = Depends(_require_admin)) -> dict:
    """返回当前服务端托管的节点二进制版本（dist/VERSION）。空串表示未知/未配置。

    详情页据此与节点自报的 client_version 比对，落后时展示「更新」按钮。
    """
    return {"version": latest_node_version()}


@admin_router.get("/nodes/latest-release")
async def admin_latest_node_release(_: User = Depends(_require_admin)) -> dict:
    """返回服务端缓存的 ``node-releases/version.json`` 摘要。

    节点详情据此判断"是否落后"以及"是否可升级"。``stale`` 表示远端拉取失败、
    返回的是上一次成功快照。``coverage`` 按 (role,platform,arch) 给出每个平台
    各自的最新版本与下载链接——version.json 已按平台各自取最新合并，因此不同
    平台可能对应不同版本号，前端按本节点 os/arch/role 取对应行判断。
    """
    import node_release_catalog

    latest = await node_release_catalog.get_latest_release()
    cov = node_release_catalog.coverage(latest)
    return {
        "version": str(latest.get("version") or ""),
        "version_notes": str(latest.get("version_notes") or ""),
        "release_tag": str(latest.get("release_tag") or ""),
        "updated_at": str(latest.get("updated_at") or ""),
        "stale": bool(latest.get("stale")),
        "coverage": cov,
        # 兼容旧前端字段名（assets 即 coverage）
        "assets": cov,
    }


@admin_router.post("/nodes/{node_id}/upgrade")
async def admin_upgrade_node(
    node_id: str, request: Request, _: User = Depends(_require_admin)
) -> dict:
    """统一升级节点：服务端按 version.json 选 runtime + 节点程序资产，经所选
    代理探测可达后，先发 RuntimeUpgrade 帧热切换，再发 SelfUpgrade 帧替换重启。"""
    body = {}
    try:
        parsed = await request.json()
        if isinstance(parsed, dict):
            body = parsed
    except Exception:
        body = {}
    return await _guard(
        nodes_service.upgrade_node(
            node_id,
            proxy_config_id=str(body.get("proxy_config_id") or ""),
        )
    )


@admin_router.post("/nodes/{node_id}/self-upgrade")
async def admin_self_upgrade_node(
    node_id: str, request: Request, _: User = Depends(_require_admin)
) -> dict:
    """下发自升级（保留供精细场景；默认走统一 ``/upgrade``）。

    ``{proxy_config_id}`` 选代理；下载地址来自服务端缓存的 version.json。
    """
    body = {}
    try:
        parsed = await request.json()
        if isinstance(parsed, dict):
            body = parsed
    except Exception:
        body = {}
    return await _guard(
        nodes_service.upgrade_node(
            node_id,
            proxy_config_id=str(body.get("proxy_config_id") or ""),
        )
    )


@admin_router.post("/nodes/{node_id}/runtime-upgrade")
async def admin_runtime_upgrade_node(
    node_id: str, request: Request, _: User = Depends(_require_admin)
) -> dict:
    """下发 runtime 升级（保留供精细场景；默认走统一 ``/upgrade``）。

    与 self-upgrade 一样改为读 version.json，不再走本机镜像。
    """
    body = {}
    try:
        parsed = await request.json()
        if isinstance(parsed, dict):
            body = parsed
    except Exception:
        body = {}
    return await _guard(
        nodes_service.upgrade_node(
            node_id,
            proxy_config_id=str(body.get("proxy_config_id") or ""),
        )
    )


@admin_router.get("/nodes/{node_id}/upgrade-defaults")
async def admin_node_upgrade_defaults(node_id: str, _: User = Depends(_require_admin)) -> dict:
    """返回该节点上次成功升级用的代理 id，供升级弹窗预选（空=直连）。"""
    node = await nodes_service.get_node_if_exists(node_id)
    last_proxy = str(node.get("last_proxy_config_id") or "") if node else ""
    return {"last_proxy_id": last_proxy}


# ── admin: node agent binaries (served by ai-lubricant, not agent-compose) ────
# Per the install guide, the daemon no longer hosts the binary; the management
# console does. These endpoints list + stream the platform builds placed under
# the configured node-bin directory so operators can download them when
# installing a node.


@admin_router.get("/nodes/binaries")
async def admin_list_node_binaries(_: User = Depends(_require_admin)) -> dict:
    """List downloadable node builds (node-execution / agent-compose-node-management) for node operators."""
    return {"binaries": list_node_binaries()}


@admin_router.get("/nodes/binaries/{name}")
async def admin_download_node_binary(name: str, _: User = Depends(_require_admin)):
    """Stream one binary (or the checksums file) by basename."""
    path = resolve_node_binary(name)
    if path is None:
        raise HTTPException(status_code=404, detail="节点运行程序不存在或未配置")
    # Force download rather than inline render (checksums is text; binaries are
    # opaque). media_type is generic — the browser uses the filename.
    return FileResponse(
        path,
        filename=path.name,
        media_type="application/octet-stream",
        content_disposition_type="attachment",
    )


@admin_router.get("/nodes/{node_id}")
async def admin_get_node_detail(node_id: str, _: User = Depends(_require_admin)) -> dict:
    """单节点详情 + runtime/节点程序各自的当前/最新版本与升级判定。

    必须声明在 ``/nodes/latest-*``、``/nodes/binaries`` 等静态子路径之后，避免 FastAPI
    把字面量误当 node_id。详情弹框只调本接口，不再依赖节点列表快照做版本判断。
    """
    return await _guard(nodes_service.get_node_detail(node_id))


# ── admin: node ↔ group bindings ──────────────────────────────────────────


@admin_router.get("/groups/{group_id}/nodes")
async def admin_list_group_nodes(group_id: str, _: User = Depends(_require_admin)) -> dict:
    return await _guard(nodes_service.list_group_nodes(group_id))


@admin_router.post("/groups/{group_id}/nodes")
async def admin_bind_group_node(group_id: str, body: BindNodeReq, _: User = Depends(_require_admin)) -> dict:
    return await _guard(nodes_service.bind_node_to_group(group_id, body.node_id))


@admin_router.delete("/groups/{group_id}/nodes/{node_id}")
async def admin_unbind_group_node(
    group_id: str, node_id: str, request: Request, user: User = Depends(_require_admin)
) -> dict:
    ok = await _guard(nodes_service.unbind_node_from_group(group_id, node_id))
    if not ok:
        raise HTTPException(status_code=404, detail="绑定不存在")
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        await _group_team_id(group_id), str(user.id), "node.unbind",
        request={"group_id": group_id, "node_id": node_id}, response={"deleted": True},
        source_ip=ip, user_agent=ua,
    )
    return {}


# ── team self-service (permission derived from bound node role) ─────────────


@team_router.get("/my-nodes")
async def team_list_my_nodes(user: User = Depends(get_current_user)) -> dict:
    """The nodes bound to every group the caller belongs to (read-only use view).

    This is the C-side console's "my nodes" surface: a user picks one of these
    to run a task on. Aggregated + deduplicated across the user's groups, with
    live daemon state joined on.
    """
    return await _guard(nodes_service.list_my_nodes(str(user.id)))


@team_router.get("/groups/{group_id}/nodes")
async def team_list_group_nodes(
    group_id: str, team_id: str = Depends(get_current_team_id)
) -> dict:
    return await _guard(nodes_service.list_team_group_nodes(team_id, group_id))


@team_router.get("/groups/{group_id}/node-picker")
async def team_group_node_picker(
    group_id: str, team_id: str = Depends(get_current_team_id)
) -> dict:
    return await _guard(nodes_service.list_group_node_picker(team_id, group_id))


@team_router.post("/groups/{group_id}/nodes")
async def team_bind_group_node(
    group_id: str,
    body: BindNodeReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    result = await _guard(nodes_service.bind_team_group_node(team_id, group_id, body.node_id))
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "group.bind_node",
        request={"group_id": group_id, "node_id": body.node_id}, response=result,
        source_ip=ip, user_agent=ua,
    )
    return result


@team_router.delete("/groups/{group_id}/nodes/{node_id}")
async def team_unbind_group_node(
    group_id: str,
    node_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    deleted = await _guard(nodes_service.unbind_team_group_node(team_id, group_id, node_id))
    if not deleted:
        raise HTTPException(status_code=404, detail="绑定不存在")
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "group.unbind_node",
        request={"group_id": group_id, "node_id": node_id}, response={"deleted": True},
        source_ip=ip, user_agent=ua,
    )
    return {"deleted": True}


@team_router.post("/nodes/{node_id}/editors/{editor}/install")
async def team_install_node_editor(
    node_id: str, editor: str, user: User = Depends(get_current_user)
) -> dict:
    if editor not in {"claude", "codex", "gemini", "opencode", "cursor"}:
        raise HTTPException(status_code=400, detail="不支持该编辑器")
    if await nodes_service.user_can_use_node(str(user.id), node_id) is None:
        raise HTTPException(status_code=403, detail="无权管理该节点")
    return await _guard(nodes_service.manage_editor(node_id, editor, "install"))


@team_router.post("/nodes/{node_id}/tools/{tool}/install")
async def team_install_node_tool(
    node_id: str, tool: str, user: User = Depends(get_current_user)
) -> dict:
    if tool != "ocr":
        raise HTTPException(status_code=400, detail="不支持该工具")
    if await nodes_service.user_can_use_node(str(user.id), node_id) is None:
        raise HTTPException(status_code=403, detail="无权管理该节点")
    # npm availability is visible in the node labels; fail with a structured
    # guide before dispatching an npm command that cannot work.
    review_nodes = await nodes_service.list_my_nodes(str(user.id))
    node = next(
        (item for item in review_nodes.get("nodes", []) if item.get("node_id") == node_id),
        None,
    )
    caps = (node or {}).get("capabilities") or {}
    if not str(caps.get("npm_version") or "").strip():
        guide = "docker" if (node or {}).get("startup_method") in {"docker", "docker-compose"} else "standalone"
        raise HTTPException(
            status_code=422,
            detail={"code": "missing_runtime", "missing": "npm", "guide": guide},
        )
    return await _guard(nodes_service.manage_editor(node_id, "ocr", "install"))


@team_router.post("/groups/{group_id}/execution-nodes")
async def team_create_execution_node(
    group_id: str,
    body: CreateExecutionNodeReq,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    return await _guard(
        nodes_service.create_group_execution_node(
            team_id, group_id,
            startup_method=body.startup_method,
            node_name=body.node_name,
            image=body.image,
        )
    )


@team_router.delete("/groups/{group_id}/execution-nodes/{node_id}")
async def team_delete_execution_node(
    group_id: str,
    node_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    result = await _guard(
        nodes_service.delete_group_execution_node(team_id, group_id, node_id)
    )
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "node.delete",
        request={"group_id": group_id, "node_id": node_id}, response=result,
        source_ip=ip, user_agent=ua,
    )
    return result
