"""User-authorized Task workspace facade: files, terminal, ports.

Mirrors :mod:`.routes_editors_workspace` but keyed on the canonical Task. A
Task owns a persistent, isolated workspace directory on its execution node; the
files/terminal tunnels address that workspace through the Task's current
``node_session_id``.

Unlike the editor facade, a Task does **not** provision a separate
``editor-workspace-*`` session: the provider runtime session itself owns the
workspace. Therefore files/terminal are only available while the Task has a
live runtime (``node_session_id``). Stopping the Task preserves the workspace
on disk, but the tunnel returns 409 until the Task is restarted — at which
point the same persistent directory is reused.

The browser only ever sends the Task id; the ``node_session_id`` and node-local
path are resolved server-side and never accepted from the client.
"""
from __future__ import annotations

import base64
import contextlib
import json
import pathlib
import urllib.parse
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket
from loguru import logger

from .deps import get_current_user
from .models import User
from .session import USER_SESSION_COOKIE, session_store
from .node_client.errors import NodeServerUnavailable
from .node_client.terminal import proxy_terminal, request_terminals
from .node_client.tunnel import TunnelResponse, request_session_tunnel
from .task_service import task_service

router = APIRouter(prefix="/api/v1/users/tasks", tags=["monkeycode-task-workspace"])

# WebSocket close codes (4xxx = application-defined), mirroring routes_nodes_terminal.
_CLOSE_UNAUTHORIZED = 4401
_CLOSE_NOT_FOUND = 4404
_CLOSE_UNAVAILABLE = 4503
_CLOSE_CONFLICT = 4409

# The xterm component pings every 5s; a client silent far longer is half-open.
_IDLE_TIMEOUT = 120.0

# Directory listings / file reads are bounded so a single request cannot pull an
# unbounded blob back through the tunnel into browser memory.
_FILE_READ_LIMIT = 4 << 20  # 4 MiB
_ATTACHMENT_LIMIT = 10 << 20  # 10 MiB, matches the historical mobile limit


def _map_task_value_error(exc: ValueError) -> HTTPException:
    reason = str(exc)
    if reason == "task_not_found":
        return HTTPException(status_code=404, detail="任务不存在")
    if reason == "runtime_not_bound":
        return HTTPException(status_code=409, detail="任务当前没有可用运行时，发送一条消息即可自动恢复")
    return HTTPException(status_code=400, detail=reason or "任务工作区不可用")


def _file_service_unavailable(response: TunnelResponse) -> HTTPException | None:
    """Map a dead node-side file service onto an explicit, actionable error.

    节点把「session 服务连不上」回报为 503（listener 已死）或 502（其它网关错）。
    这里不再把原始 dial 错误透传给浏览器，而是给出原因与下一步动作；按约定
    不自动重启文件服务/运行时——恢复动作是发送下一条消息。
    """
    if response.status < 400:
        return None
    text = response.body.decode("utf-8", "replace") or ""
    if response.status == 503 or "not reachable" in text or "is not running" in text:
        return HTTPException(
            status_code=503,
            detail="任务工作区文件服务当前不可用（任务运行环境已退出或尚未就绪）。发送一条消息恢复任务后即可重新查看目录。",
        )
    detail = text or "节点文件服务错误"
    try:
        parsed = json.loads(text or "{}")
        if isinstance(parsed, dict):
            detail = str(parsed.get("error") or parsed.get("detail") or detail)
    except (ValueError, TypeError):
        pass
    return HTTPException(status_code=response.status, detail=detail)


async def _resolve_task_session(task_id: str, user_id: str, role: str | None) -> str:
    try:
        return await task_service.task_workspace_node_session_id(
            user_id, task_id, role=role
        )
    except ValueError as exc:
        raise _map_task_value_error(exc) from exc


@router.get("/{task_id}/files")
async def list_task_files(
    task_id: str,
    path: str = Query("/", description="工作区内的相对路径"),
    user: User = Depends(get_current_user),
) -> dict:
    """List a directory (or read a small text file) in the Task workspace.

    Read-only. Proxies the node ``files`` service through the control-plane
    tunnel; Task ownership is checked here and the control URL/token never reach
    the browser.
    """
    node_session_id = await _resolve_task_session(task_id, str(user.id), user.role)
    clean = "/" + (path or "/").lstrip("/")
    try:
        response = await request_session_tunnel(
            session_id=node_session_id,
            service="files",
            method="GET",
            path=clean,
            body_limit=_FILE_READ_LIMIT,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    status_code, payload = response.status, response.body
    content_type = next(
        (value for key, value in response.headers.items() if key.lower() == "content-type"),
        "",
    )
    mapped = _file_service_unavailable(response)
    if mapped is not None:
        raise mapped
    if "application/json" in content_type:
        try:
            entries = json.loads(payload.decode("utf-8", "replace") or "[]")
        except (ValueError, TypeError):
            entries = []
        entries = entries if isinstance(entries, list) else []
        return {"path": clean, "is_dir": True, "entries": entries, "count": len(entries)}
    # A file: return bounded text; binary is returned base64 with a flag.
    truncated = len(payload) > _FILE_READ_LIMIT
    payload = payload[:_FILE_READ_LIMIT]
    try:
        text = payload.decode("utf-8")
        return {"path": clean, "is_dir": False, "content": text, "encoding": "utf-8", "truncated": truncated}
    except UnicodeDecodeError:
        return {
            "path": clean,
            "is_dir": False,
            "content": base64.b64encode(payload).decode("ascii"),
            "encoding": "base64",
            "truncated": truncated,
        }


async def _task_files_json(
    task_id: str,
    user: User,
    *,
    path: str,
) -> dict:
    node_session_id = await _resolve_task_session(task_id, str(user.id), user.role)
    try:
        response = await request_session_tunnel(
            session_id=node_session_id,
            service="files",
            method="GET",
            path=path,
            body_limit=_FILE_READ_LIMIT,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    mapped = _file_service_unavailable(response)
    if mapped is not None:
        raise mapped
    payload = response.body.decode("utf-8", "replace")
    try:
        body = json.loads(payload or "{}")
    except (ValueError, TypeError):
        body = {"success": False, "error": payload or "节点文件服务返回无效 JSON"}
    return body if isinstance(body, dict) else {"success": False, "error": "节点文件服务返回无效数据"}


@router.put("/{task_id}/attachments")
async def upload_task_attachment(
    task_id: str,
    request: Request,
    filename: str = Query(..., min_length=1, max_length=255),
    user: User = Depends(get_current_user),
) -> dict:
    """Upload one attachment directly into the canonical Task workspace.

    The browser never chooses a filesystem path: the server strips directories,
    prefixes a UUID, and confines every upload under
    ``.task-attachments``. The returned ``workspace://`` URL is safe to
    include in ``TaskMessageReq.attachments``; the service expands it to a path
    the agent can read from its workspace.
    """
    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="附件内容为空")
    if len(raw) > _ATTACHMENT_LIMIT:
        raise HTTPException(status_code=413, detail="附件不能超过 10 MiB")
    safe_name = pathlib.PurePath(filename.replace("\\", "/")).name
    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in safe_name)
    if safe_name in ("", ".", ".."):
        safe_name = "attachment.bin"
    relative = f".task-attachments/{uuid.uuid4().hex}-{safe_name}"
    node_session_id = await _resolve_task_session(task_id, str(user.id), user.role)
    try:
        response = await request_session_tunnel(
            session_id=node_session_id,
            service="files",
            method="PUT",
            path=relative,
            headers={"content-type": request.headers.get("content-type", "application/octet-stream")},
            body=raw,
            body_limit=1024,
            timeout_seconds=60,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    mapped = _file_service_unavailable(response)
    if mapped is not None:
        raise mapped
    if response.status >= 400:
        raise HTTPException(
            status_code=response.status,
            detail=response.body.decode("utf-8", "replace") or "附件上传失败",
        )
    return {
        "url": f"workspace://{relative}",
        "filename": safe_name,
        "path": relative,
        "size": len(raw),
    }


@router.get("/{task_id}/files/changes")
async def list_task_file_changes(
    task_id: str,
    user: User = Depends(get_current_user),
) -> dict:
    """Historical ``repo_file_changes`` contract over canonical Task REST."""
    return await _task_files_json(task_id, user, path="__git_changes__")


@router.get("/{task_id}/files/diff")
async def get_task_file_diff(
    task_id: str,
    path: str = Query(..., min_length=1),
    context_lines: int = Query(20, ge=0, le=100),
    user: User = Depends(get_current_user),
) -> dict:
    """Historical ``repo_file_diff`` response keyed by canonical Task id."""
    query = urllib.parse.urlencode({"path": path, "context_lines": context_lines})
    return await _task_files_json(task_id, user, path=f"__git_diff__?{query}")


@router.get("/{task_id}/ports")
async def list_task_ports(
    task_id: str,
    user: User = Depends(get_current_user),
) -> dict:
    """Port preview for a Task workspace.

    Task node sessions do not currently expose a port-forwarding surface on the
    node. Rather than fabricate VM-shaped data, return an empty set with
    ``supported=true`` so the UI renders the normal "no ports" empty state.
    When node-side Task port discovery lands, this is where it surfaces.
    """
    # Ownership + runtime resolution: still 409 when the runtime is offline so
    # the UI does not pretend a workspace is browsable while the Task is stopped.
    await _resolve_task_session(task_id, str(user.id), user.role)
    return {"ports": [], "supported": True}


async def _resolve_ws_user(websocket: WebSocket) -> User | None:
    """Resolve the C-side user from the session cookie (mirror admin ws)."""
    cookie = websocket.cookies.get(USER_SESSION_COOKIE)
    if not cookie:
        return None
    try:
        data = await session_store.get(USER_SESSION_COOKIE, cookie)
    except Exception as exc:  # noqa: BLE001 - redis down must not 500 the ws
        logger.warning("[task-ws] terminal session lookup failed: {}", exc)
        return None
    if data is None:
        return None
    user = await User.get_or_none(id=data.uid)
    if user is None or user.is_deleted or user.is_blocked or user.status != "active":
        return None
    return user


async def _send(websocket: WebSocket, kind: str, data: str = "") -> None:
    await websocket.send_text(json.dumps({"type": kind, "data": data}, ensure_ascii=False))


async def _reject_ws(websocket: WebSocket, code: int, reason: str) -> None:
    """Accept then surface a readable error frame before closing.

    Starlette hides pre-accept close codes/reasons from the browser (the WS
    never opens, so the client only sees ``onerror``). Accepting first lets us
    send an ``error`` frame the xterm client toasts verbatim, then close.
    """
    try:
        await websocket.accept()
    except Exception:  # noqa: BLE001 - already accepted / disconnected
        return
    try:
        await _send(websocket, "error", reason)
    except Exception:  # noqa: BLE001
        pass
    with contextlib.suppress(Exception):
        await websocket.close(code=code, reason=reason)


@router.get("/{task_id}/terminals")
async def list_task_terminals(
    task_id: str, user: User = Depends(get_current_user)
) -> dict:
    node_session_id = await _resolve_task_session(task_id, str(user.id), user.role)
    from . import config

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        raise HTTPException(status_code=503, detail="控制面未配置")
    return await request_terminals(
        base, token, f"/internal/session-terminals/{node_session_id}"
    )


@router.delete("/{task_id}/terminals/{terminal_id}")
async def delete_task_terminal(
    task_id: str,
    terminal_id: str,
    user: User = Depends(get_current_user),
) -> dict:
    node_session_id = await _resolve_task_session(task_id, str(user.id), user.role)
    from . import config

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        raise HTTPException(status_code=503, detail="控制面未配置")
    return await request_terminals(
        base,
        token,
        f"/internal/session-terminals/{node_session_id}/{terminal_id}",
        method="DELETE",
    )


@router.websocket("/{task_id}/terminals/connect")
async def connect_task_terminal(websocket: WebSocket, task_id: str) -> None:
    await task_terminal(websocket, task_id)


@router.websocket("/{task_id}/terminal")
async def task_terminal(websocket: WebSocket, task_id: str) -> None:
    """Interactive shell in the Task's workspace directory (owner only).

    Task-level: addresses the Task's own runtime session. The node resolves the
    workspace path from the session's task-owned workspace key (the server never
    dictates a node-local path). Gated to the Task owner.
    """
    user = await _resolve_ws_user(websocket)
    if user is None:
        await _reject_ws(websocket, _CLOSE_UNAUTHORIZED, "需要登录")
        return

    try:
        node_session_id = await task_service.task_workspace_node_session_id(
            str(user.id), task_id, role=user.role
        )
    except ValueError as exc:
        reason = str(exc)
        if reason == "task_not_found":
            await _reject_ws(websocket, _CLOSE_NOT_FOUND, "任务不存在")
        else:
            await _reject_ws(
                websocket, _CLOSE_CONFLICT, "任务当前没有可用运行时，请重启任务后重试"
            )
        return

    from . import config

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        await _reject_ws(websocket, _CLOSE_UNAVAILABLE, "控制面未配置")
        return

    terminal_id = (websocket.query_params.get("terminal_id") or "").strip()
    if not terminal_id or len(terminal_id) > 128:
        await _reject_ws(websocket, 4400, "终端 ID 无效")
        return

    # The data service authorized the Task owner above; the control process owns
    # the live Registry/PTY. Bridge the browser frames through the internal
    # session-terminal endpoint (bearer-gated). node_session_id / node id never
    # reach the browser.
    await proxy_terminal(websocket, base, token, f"/internal/session-terminal/{node_session_id}")
