"""Admin host-terminal websocket: browser xterm ↔ a node's host shell.

One route, ``/api/v1/admin/nodes/{node_id}/terminal``, bridges a browser
terminal to an interactive shell running on the node's own host machine. The
node spawns the shell in a PTY (see ``nodes/common/agent/terminal.go``); the
frames travel over that node's existing NodeConnect stream, multiplexed by
``terminal_id``. The data service proxies the WebSocket through
:mod:`.node_client.terminal`; the standalone control process multiplexes it
onto the node's live NodeConnect stream.

Wire protocol (client ↔ this route) is deliberately the SAME JSON envelope the
existing xterm component already speaks, so the frontend reuses it unchanged:

* client → server: ``{"type":"data","data":<base64>}`` (stdin),
  ``{"type":"resize","data":"{\\"row\\":R,\\"col\\":C}"}``, ``{"type":"ping"}``
* server → client: ``{"type":"connected","data":<json>}`` once the shell is
  requested, ``{"type":"data","data":<base64>}`` (PTY output),
  ``{"type":"error","data":<text>}`` on failure/exit.

SECURITY: a terminal here is equivalent to shell access on the node host, so
the route is gated to platform administrators only (the same authorization
``routes_nodes`` uses) and every open is audited. Nodes without a client
process (``passive_management`` grouping containers) and offline nodes are
refused.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, WebSocket
from loguru import logger

from .deps import get_current_user
from .models import User
from .session import USER_SESSION_COOKIE, session_store

router = APIRouter(tags=["user-platform-nodes-admin"])

# WebSocket close codes (4xxx = application-defined).
_CLOSE_UNAUTHORIZED = 4401
_CLOSE_UNAVAILABLE = 4503


async def _require_admin_http(user: User = Depends(get_current_user)) -> User:
    """HTTP admin gate for the terminal-management endpoints.

    Mirrors routes_nodes._require_admin but lives here next to the routes that
    need it, so the shell-grade capability stays co-located with its audit.
    """
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


def _control_endpoint_for(_user: User) -> tuple[str, str]:
    """Resolve control-plane base url + token (caller has already authenticated).

    Kept as a small wrapper so the routes read symmetrically; the user param is
    accepted for a future per-user control-plane scope without touching callers.
    """
    from . import config

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        raise HTTPException(status_code=503, detail="控制面未配置")
    return base, token


async def _resolve_admin(websocket: WebSocket) -> User | None:
    """Resolve the caller as a platform admin, or None.

    Mirrors ``deps.get_current_user_id``'s primary channel: the C-side session
    cookie. A websocket handshake carries cookies, so no token needs to be put
    in the URL (where it would land in logs/history). The emergency admin Bearer
    channel is intentionally NOT accepted here — a browser cannot attach an
    Authorization header to a WebSocket, so supporting it would mean a
    query-string token, which we refuse for a shell-grade capability.
    """
    cookie = websocket.cookies.get(USER_SESSION_COOKIE)
    if not cookie:
        return None
    try:
        data = await session_store.get(USER_SESSION_COOKIE, cookie)
    except Exception as exc:  # noqa: BLE001 - redis down must not 500 the ws
        logger.warning("[nodes] terminal ws session lookup failed: {}", exc)
        return None
    if data is None:
        return None
    user = await User.get_or_none(id=data.uid)
    if user is None or user.is_deleted or user.is_blocked or user.status != "active":
        return None
    if user.role != "admin":
        return None
    return user


# The admin route remains separate because it grants unrestricted platform shell access.
# This route is user-scoped: the caller must have a GroupNode grant for the node.
@router.websocket("/api/v1/teams/my-nodes/{node_id}/terminal")
async def user_node_terminal(websocket: WebSocket, node_id: str) -> None:
    """Interactive shell for a node the signed-in user is authorized to use."""
    cookie = websocket.cookies.get(USER_SESSION_COOKIE)
    if not cookie:
        await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="需要登录")
        return
    try:
        data = await session_store.get(USER_SESSION_COOKIE, cookie)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[nodes] user terminal session lookup failed: {}", exc)
        data = None
    if data is None:
        await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="需要登录")
        return
    user = await User.get_or_none(id=data.uid)
    if user is None or user.is_deleted or user.is_blocked or user.status != "active":
        await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="账号不可用")
        return

    from .nodes_service import NodesService

    nodes_service = NodesService()
    if not await nodes_service.user_can_use_node(str(user.id), node_id):
        await websocket.close(code=4403, reason="无权访问该节点")
        return
    live = (await nodes_service._live_node_map()).get(node_id)
    if (
        live is None
        or live.get("is_passive")
        or live.get("status") != "approved"
        or not (live.get("online") if "online" in live else live.get("connected"))
    ):
        await websocket.close(code=_CLOSE_UNAVAILABLE, reason="节点未审批、离线或不支持终端")
        return
    from . import config
    from .node_client.terminal import proxy_terminal

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        await websocket.close(code=_CLOSE_UNAVAILABLE, reason="控制面未配置")
        return
    await proxy_terminal(
        websocket,
        base,
        token,
        f"/internal/node-terminal/{node_id}",
        owner=f"user:{user.id}",
    )


@router.websocket("/api/v1/admin/nodes/{node_id}/terminal")
async def admin_node_terminal(websocket: WebSocket, node_id: str) -> None:
    """Interactive shell on ``node_id``'s host machine (admin only)."""
    user = await _resolve_admin(websocket)
    if user is None:
        await websocket.close(code=_CLOSE_UNAUTHORIZED, reason="需要管理员权限")
        return

    from . import config
    from .node_client.terminal import proxy_terminal

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        await websocket.close(code=_CLOSE_UNAVAILABLE, reason="控制面未配置")
        return
    # Admin authentication + audit stay here; node approval/capability/offline
    # checks and the live PTY are owned by the control service.
    #
    # The audit is recorded on *intent*, before the control service mints the
    # terminal id: shell access on a node host is the highest-privilege node
    # action, so the attempt must be recorded even when the control service then
    # refuses (unapproved / offline / no terminal support).
    await _audit_open(websocket, user, node_id, terminal_id="")
    await proxy_terminal(
        websocket,
        base,
        token,
        f"/internal/node-terminal/{node_id}",
        owner=f"admin:{user.id}",
    )


# ── terminal management (list / interrupt / close) ─────────────────────────
#
# These HTTP endpoints are the management console's view onto a node's open
# host terminals: what each is running, who owns it, and the actions to stop a
# command or close a terminal. The data service only authorizes and forwards —
# the node is the source of truth (it owns the PTYs), and the control service
# layers on owner attribution and agent-command state the node cannot see.
#
# They share the WebSocket terminal's admin gate (shell-grade capability) and
# audit, because interrupt/close alter what is running on the host just as an
# open would.


@router.get("/api/v1/admin/nodes/{node_id}/terminals")
async def admin_list_node_terminals(
    node_id: str, user: User = Depends(_require_admin_http)
) -> dict:
    from .node_client.terminal import request_terminals

    base, token = _control_endpoint_for(user)
    return await request_terminals(base, token, f"/internal/node-terminals/{node_id}")


@router.post("/api/v1/admin/nodes/{node_id}/terminals/{terminal_id}/interrupt")
async def admin_interrupt_node_terminal(
    node_id: str, terminal_id: str, user: User = Depends(_require_admin_http)
) -> dict:
    from .node_client.terminal import request_terminals

    base, token = _control_endpoint_for(user)
    await _audit_management(user, node_id, terminal_id, "node.terminal_interrupt")
    return await request_terminals(
        base, token, f"/internal/node-terminals/{node_id}/{terminal_id}/interrupt", method="POST"
    )


@router.delete("/api/v1/admin/nodes/{node_id}/terminals/{terminal_id}")
async def admin_close_node_terminal(
    node_id: str, terminal_id: str, user: User = Depends(_require_admin_http)
) -> dict:
    from .node_client.terminal import request_terminals

    base, token = _control_endpoint_for(user)
    await _audit_management(user, node_id, terminal_id, "node.terminal_close")
    return await request_terminals(
        base, token, f"/internal/node-terminals/{node_id}/{terminal_id}", method="DELETE"
    )


async def _audit_management(user: User, node_id: str, terminal_id: str, action: str) -> None:
    """Record interrupt/close intent (best-effort, never blocks the action)."""
    try:
        from .deps import resolve_team_id
        from .team_users_service import team_users_service

        await team_users_service.record_audit(
            await resolve_team_id(str(user.id)),
            str(user.id),
            action,
            request={"node_id": node_id, "terminal_id": terminal_id},
            response={"requested": True},
            user_agent=None,
        )
    except Exception:  # noqa: BLE001 - audit must never break the terminal action
        pass


async def _audit_open(websocket: WebSocket, user: User, node_id: str, terminal_id: str) -> None:
    """Record the shell open in the shared audit stream (best-effort).

    Shell access on a node host is the highest-privilege node action, so it is
    always audited. Like the rest of the audit surface, a failure here never
    blocks the action.
    """
    try:
        from .deps import resolve_team_id
        from .team_users_service import team_users_service

        client = websocket.client
        forwarded = (websocket.headers.get("x-forwarded-for") or "").split(",")[0].strip()
        await team_users_service.record_audit(
            await resolve_team_id(str(user.id)),
            str(user.id),
            "node.terminal_open",
            request={"node_id": node_id, "terminal_id": terminal_id},
            response={"opened": True},
            source_ip=forwarded or (client.host if client else None),
            user_agent=websocket.headers.get("user-agent"),
        )
    except Exception:  # noqa: BLE001 - audit must never break the terminal
        pass
