"""REST surface for the tunnel manager.

Three entry points, all under ``/api/v1/users`` and gated to the logged-in
user (the platform admin / user who owns the schemes/bindings):

* **Scheme pool** — ``/tunnel-schemes`` CRUD.
* **Project-attached bindings** — ``/projects/{project_id}/tunnels``: list +
  create + delete. Project access (owner/read_write) is enforced.
* **Node-page overview** — ``/nodes/{node_id}/tunnels`` (this node's bindings,
  across all projects + standalone) and ``/tunnels`` (global overview) +
  create/delete **standalone** bindings (``project_id`` null) from the node
  page.

Bindings never attach to a task or a session.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from .deps import audit_user_action, get_current_user
from .models import User
from .project_service import project_service
from . import tunnel_service
from .tunnel_service import TunnelServiceError

router = APIRouter(prefix="/api/v1/users", tags=["monkeycode-tunnels"])


def _to_http(exc: TunnelServiceError) -> HTTPException:
    code = {
        "not_found": 404,
        "invalid_argument": 400,
        "in_use": 409,
        "allocation_failed": 409,
    }.get(exc.code, 400)
    return HTTPException(status_code=code, detail=exc.message)


# ── scheme pool ──────────────────────────────────────────────────────────


class SchemeReq(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    kind: str
    config: dict = Field(default_factory=dict)
    team_id: str | None = None
    enabled: bool = True


class SchemeUpdate(BaseModel):
    name: str | None = None
    config: dict | None = None
    enabled: bool | None = None


@router.get("/tunnel-schemes")
async def list_schemes(user: User = Depends(get_current_user)) -> list[dict]:
    return await tunnel_service.list_schemes(str(user.id))


@router.post("/tunnel-schemes")
async def create_scheme(
    body: SchemeReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        row = await tunnel_service.create_scheme(
            str(user.id),
            name=body.name, kind=body.kind, config=body.config, team_id=body.team_id,
        )
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.scheme.create",
        request_body=body.model_dump(exclude_none=True),
        response={"id": row["id"]},
    )
    return row


@router.put("/tunnel-schemes/{scheme_id}")
async def update_scheme(
    scheme_id: str, body: SchemeUpdate, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        row = await tunnel_service.update_scheme(
            str(user.id), scheme_id,
            name=body.name, config=body.config, enabled=body.enabled,
        )
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.scheme.update",
        request_body=body.model_dump(exclude_none=True), response={"id": row["id"]},
    )
    return row


@router.get("/tunnel-schemes/{scheme_id}/bindings")
async def list_scheme_bindings(
    scheme_id: str, user: User = Depends(get_current_user)
) -> list[dict]:
    """Every proxy binding using one scheme (scheme-page expandable table)."""
    return await tunnel_service.list_bindings(str(user.id), scheme_id=scheme_id)


@router.delete("/tunnel-schemes/{scheme_id}")
async def delete_scheme(
    scheme_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        result = await tunnel_service.delete_scheme(str(user.id), scheme_id)
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.scheme.delete",
        request_body={"scheme_id": scheme_id}, response={"deleted": True},
    )
    return result


# ── bindings: project-attached ───────────────────────────────────────────


class BindingReq(BaseModel):
    scheme_id: str
    # 目标:节点 id、``__main__``(主服务本机),或留空由项目运行中任务解析。
    node_id: str | None = None
    local_port: int = Field(..., ge=1, le=65535)
    local_host: str = "127.0.0.1"
    # cloudflared managed 模式必填:相对方案根域名的子域名(如 app)。
    subdomain: str | None = None
    # 用户备注,列表显示。可空。
    description: str | None = None


async def _require_project_write(user: User, project_id: str) -> None:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    if project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=403, detail="无项目写权限")


@router.get("/projects/{project_id}/tunnels")
async def list_project_tunnels(
    project_id: str, user: User = Depends(get_current_user)
) -> list[dict]:
    await _require_project_write(user, project_id)
    return await tunnel_service.list_bindings(str(user.id), project_id=project_id)


@router.post("/projects/{project_id}/tunnels")
async def create_project_tunnel(
    project_id: str, body: BindingReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    await _require_project_write(user, project_id)
    try:
        row = await tunnel_service.create_binding(
            str(user.id), scheme_id=body.scheme_id, node_id=body.node_id,
            local_port=body.local_port, local_host=body.local_host,
            subdomain=body.subdomain, description=body.description,
            project_id=project_id,
        )
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.create",
        request_body={"project_id": project_id, **body.model_dump(exclude_none=True)},
        response={"id": row["id"]},
    )
    return row


@router.delete("/projects/{project_id}/tunnels/{binding_id}")
async def delete_project_tunnel(
    project_id: str, binding_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    await _require_project_write(user, project_id)
    try:
        result = await tunnel_service.delete_binding(str(user.id), binding_id)
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.delete",
        request_body={"project_id": project_id, "binding_id": binding_id},
        response=result,
    )
    return result


# ── bindings: node-page overview + standalone ────────────────────────────


@router.get("/tunnels")
async def list_all_tunnels(user: User = Depends(get_current_user)) -> list[dict]:
    """Global overview (every binding owned by the caller, all projects + standalone)."""
    return await tunnel_service.list_bindings(str(user.id))


@router.post("/tunnels")
async def create_tunnel(
    body: BindingReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Create a standalone binding from the scheme page.

    ``node_id`` names the target: an execution node id or ``__main__`` for the
    main service's own machine.
    """
    try:
        row = await tunnel_service.create_binding(
            str(user.id), scheme_id=body.scheme_id, node_id=body.node_id,
            local_port=body.local_port, local_host=body.local_host,
            subdomain=body.subdomain, description=body.description,
            project_id=None,
        )
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.create_standalone",
        request_body=body.model_dump(exclude_none=True), response={"id": row["id"]},
    )
    return row


@router.put("/tunnels/{binding_id}")
async def update_tunnel(
    binding_id: str, body: BindingReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Edit a binding's mutable fields (node / local host+port / subdomain).

    ``scheme_id`` is immutable and must match the stored value; everything else
    reconfigures the runtime asynchronously.
    """
    try:
        row = await tunnel_service.update_binding(
            str(user.id), binding_id,
            scheme_id=body.scheme_id, node_id=body.node_id,
            local_port=body.local_port, local_host=body.local_host,
            subdomain=body.subdomain, description=body.description,
        )
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.update",
        request_body=body.model_dump(exclude_none=True),
        response={"id": row["id"], "client_status": row["client_status"]},
    )
    return row


@router.post("/tunnels/{binding_id}/start")
async def start_tunnel(
    binding_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Start/restart a binding's client on its target (node or main service)."""
    try:
        row = await tunnel_service.start_binding(str(user.id), binding_id)
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.start",
        request_body={"binding_id": binding_id},
        response={"id": row["id"], "client_status": row["client_status"]},
    )
    return row


@router.post("/tunnels/{binding_id}/stop")
async def stop_tunnel(
    binding_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Stop a binding's client, keeping the record and provider resources."""
    try:
        row = await tunnel_service.stop_binding_service(str(user.id), binding_id)
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.stop",
        request_body={"binding_id": binding_id},
        response={"id": row["id"], "client_status": row["client_status"]},
    )
    return row


@router.delete("/tunnels/{binding_id}")
async def delete_tunnel(
    binding_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Delete any binding the caller owns (scheme-page table action)."""
    try:
        result = await tunnel_service.delete_binding(str(user.id), binding_id)
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.delete",
        request_body={"binding_id": binding_id}, response=result,
    )
    return result


@router.get("/nodes/{node_id}/tunnels")
async def list_node_tunnels(
    node_id: str, user: User = Depends(get_current_user)
) -> list[dict]:
    """All bindings landed on one node (cross-project + standalone)."""
    return await tunnel_service.list_bindings(str(user.id), node_id=node_id)


@router.post("/nodes/{node_id}/tunnels")
async def create_node_tunnel(
    node_id: str, body: BindingReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Create a standalone binding (project_id null) from the node page."""
    if node_id != body.node_id:
        raise HTTPException(status_code=400, detail="node_id mismatch")
    try:
        row = await tunnel_service.create_binding(
            str(user.id), scheme_id=body.scheme_id, node_id=body.node_id,
            local_port=body.local_port, local_host=body.local_host,
            subdomain=body.subdomain, description=body.description,
            project_id=None,
        )
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.create_standalone",
        request_body=body.model_dump(exclude_none=True), response={"id": row["id"]},
    )
    return row


@router.delete("/nodes/{node_id}/tunnels/{binding_id}")
async def delete_node_tunnel(
    node_id: str, binding_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        result = await tunnel_service.delete_binding(str(user.id), binding_id)
    except TunnelServiceError as exc:
        raise _to_http(exc) from exc
    await audit_user_action(
        request, user, "tunnel.binding.delete_standalone",
        request_body={"node_id": node_id, "binding_id": binding_id},
        response=result,
    )
    return result
