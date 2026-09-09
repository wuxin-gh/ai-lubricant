"""User-authorized editor workspace facade: files, terminal, ports.

The node exposes a per-session file service and can open a host-shell terminal
in a session's workspace, but those primitives live behind the raw node tunnel
(:mod:`.node_client.tunnel`) and the admin-only host terminal, neither of which
authenticates the C-side user or checks editor ownership.

This module is the browser-facing surface. The workspace (files / terminal /
ports) is **editor-level**, not per-task: every task dispatched under an editor
tags its node session with the same ``editor_workdir``, so the node resolves the
same shared working directory regardless of which task's node session we address.
We pick any active task's ``node_session_id`` as the handle into the node and
the workspace follows. The browser only ever sends the editor id; the
``node_session_id`` and node-local path are resolved server-side and never
accepted from the client.

Every route:

* resolves the logged-in user from the C-side session cookie,
* verifies the editor belongs to that user (``get_editor_for_user``),
* picks an active task's ``node_session_id`` as the node handle (409 if the
  editor has no running task yet),
* only then proxies to the node's ``files`` service / opens a terminal scoped to
  the editor workspace.
"""
from __future__ import annotations

import base64
import contextlib
import json

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket
from loguru import logger

from db import PostgresClient
from .deps import get_current_user
from .models import User
from .session import USER_SESSION_COOKIE, session_store
from .node_client import get_editor_workspace_session_node_id
from .node_client.client import get_local_node_client
from .node_client.errors import Code, NodeServerUnavailable, RPCError
from .node_client.tunnel import request_session_tunnel

router = APIRouter(prefix="/api/v1/users/editors", tags=["user-platform-editor-workspace"])

# WebSocket close codes (4xxx = application-defined), mirroring routes_nodes_terminal.
_CLOSE_UNAUTHORIZED = 4401
_CLOSE_FORBIDDEN = 4403
_CLOSE_NOT_FOUND = 4404
_CLOSE_UNAVAILABLE = 4503

# The xterm component pings every 5s; a client silent far longer is half-open.
_IDLE_TIMEOUT = 120.0

# Directory listings / file reads are bounded so a single request cannot pull an
# unbounded blob back through the tunnel into browser memory.
_FILE_READ_LIMIT = 4 << 20  # 4 MiB


def editor_workspace_session_id(editor_id: str) -> str:
    """Stable node session id for the editor-owned workspace runtime."""
    return f"editor-workspace-{editor_id}"


async def _ensure_editor_workspace(editor: dict) -> str:
    """Provision the editor workspace session without starting a provider runtime.

    Tasks still get their own provider sessions. This lightweight node session
    only owns the shared editor_workdir and its files service, so terminal/files
    work even when the editor has zero tasks.
    """
    editor_id = str(editor["id"])
    session_id = editor_workspace_session_id(editor_id)
    # dispatch_session on the control side is NOT idempotent (it re-sends
    # CreateSession), so first check whether a binding already exists by
    # reading the shared PG ledger directly (the data side may read static
    # placement but must not operate on live nodes).
    if await get_editor_workspace_session_node_id(session_id) is not None:
        return session_id

    try:
        result = await get_local_node_client().dispatch_session(
            (editor.get("node_id") or "").strip() or None,
            {
                "sessionId": session_id,
                "editorId": editor_id,
                "projectId": editor.get("project_id") or "",
                "provider": editor.get("provider") or "claude",
                "model": "",
                "interactive": False,
                "deferStart": True,
                "tags": {"editor_workdir": editor.get("workdir") or f"editors/{editor_id}"},
                "git": {"branch": editor.get("branch") or ""},
            },
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail="控制面当前不可用，请稍后重试") from exc
    except RPCError as exc:
        if exc.code == Code.NOT_FOUND:
            raise HTTPException(status_code=409, detail="编辑器绑定的节点不存在或已离线，请重新绑定可用节点") from exc
        if exc.code in (Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED):
            raise HTTPException(status_code=503, detail="编辑器绑定的节点当前不可用，请稍后重试") from exc
        raise HTTPException(status_code=503, detail=f"编辑器工作区创建失败：{exc.message}") from exc
    if result.get("accepted") is False:
        raise HTTPException(status_code=503, detail=result.get("error") or "编辑器工作区创建失败")
    return str(result.get("sessionId") or result.get("session_id") or session_id)


_PROMPT_START = '<!-- ai-lubricant:project-prompt:start -->'
_PROMPT_END = '<!-- ai-lubricant:project-prompt:end -->'
_LEGACY_PROMPT_START = '<!-- model-api:project-prompt:start -->'
_LEGACY_PROMPT_END = '<!-- model-api:project-prompt:end -->'


def _managed_prompt_markers(existing: str) -> tuple[str, str, int] | None:
    """Return the marker pair and start offset for either marker generation."""
    for start_marker, end_marker in (
        (_PROMPT_START, _PROMPT_END),
        (_LEGACY_PROMPT_START, _LEGACY_PROMPT_END),
    ):
        start = existing.find(start_marker)
        if start < 0:
            continue
        if existing.find(end_marker, start + len(start_marker)) >= 0:
            return start_marker, end_marker, start
    return None


def merge_managed_prompt(existing: str, prompt_content: str) -> str:
    """Replace only our managed block (either generation) and preserve user text."""
    block = f"{_PROMPT_START}\n{prompt_content.rstrip()}\n{_PROMPT_END}"
    markers = _managed_prompt_markers(existing)
    if markers:
        old_start, old_end, start = markers
        end = existing.find(old_end, start + len(old_start)) + len(old_end)
        return existing[:start] + block + existing[end:]
    prefix = existing.rstrip()
    return f"{prefix}\n\n{block}\n" if prefix else block + "\n"


async def _read_workspace_text(node_session_id: str, filename: str) -> str:
    response = await request_session_tunnel(
        session_id=node_session_id,
        service="files",
        method="GET",
        path=f"/{filename}",
        body_limit=_FILE_READ_LIMIT,
    )
    if response.status == 404:
        return ""
    if response.status >= 400:
        raise HTTPException(response.status, response.body.decode("utf-8", "replace") or "提示词文件读取失败")
    return response.body.decode("utf-8", "replace")


async def write_editor_prompt_file(editor: dict, prompt_content: str) -> str:
    """Write the selected project prompt into this editor's workspace.

    Claude reads CLAUDE.md; Codex/OpenCode read AGENTS.md. The node files service
    enforces workspace traversal boundaries and creates parent directories.
    """
    node_session_id = await _ensure_editor_workspace(editor)
    filename = "CLAUDE.md" if editor.get("provider") == "claude" else "AGENTS.md"
    existing = await _read_workspace_text(node_session_id, filename)
    body = merge_managed_prompt(existing, prompt_content).encode("utf-8")
    try:
        response = await request_session_tunnel(
            session_id=node_session_id,
            service="files",
            method="PUT",
            path=f"/{filename}",
            headers={"content-type": "text/markdown; charset=utf-8"},
            body=body,
            body_limit=_FILE_READ_LIMIT,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    status_code, payload = response.status, response.body
    if status_code >= 400:
        raise HTTPException(status_code=status_code, detail=payload.decode("utf-8", "replace") or "提示词文件写入失败")
    return filename


async def remove_editor_prompt_file(editor: dict) -> str:
    """Remove only the managed prompt block, preserving user-authored content."""
    node_session_id = await _ensure_editor_workspace(editor)
    filename = "CLAUDE.md" if editor.get("provider") == "claude" else "AGENTS.md"
    existing = await _read_workspace_text(node_session_id, filename)
    markers = _managed_prompt_markers(existing)
    if markers is None:
        return filename
    old_start, old_end, start = markers
    end = existing.find(old_end, start + len(old_start)) + len(old_end)
    next_content = (existing[:start] + existing[end:]).strip()
    response = await request_session_tunnel(
        session_id=node_session_id,
        service="files",
        method="PUT",
        path=f"/{filename}",
        headers={"content-type": "text/markdown; charset=utf-8"},
        body=((next_content + "\n") if next_content else "").encode("utf-8"),
        body_limit=_FILE_READ_LIMIT,
    )
    if response.status >= 400:
        raise HTTPException(response.status, response.body.decode("utf-8", "replace") or "提示词文件清理失败")
    return filename


async def _resolve_editor_workspace(editor_id: str, user_id: str) -> tuple[dict, str]:
    """Return (editor, editor-owned node_session_id) after ownership checks."""
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user_id))
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    return editor, await _ensure_editor_workspace(editor)


@router.get("/{editor_id}/files")
async def list_editor_files(
    editor_id: str,
    path: str = Query("/", description="工作区内的相对路径"),
    user: User = Depends(get_current_user),
) -> dict:
    """List a directory (or read a small text file) in the editor workspace.

    Read-only. Proxies the node ``files`` service through the control-plane
    tunnel; ownership is checked here and the control URL/token never reach the
    browser.
    """
    _, node_session_id = await _resolve_editor_workspace(editor_id, str(user.id))
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
    if status_code >= 400:
        raise HTTPException(status_code=status_code, detail=payload.decode("utf-8", "replace") or "文件服务错误")
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


@router.get("/{editor_id}/ports")
async def list_editor_ports(
    editor_id: str,
    user: User = Depends(get_current_user),
) -> dict:
    """Port preview for an editor workspace.

    Editor node sessions do not currently expose a port-forwarding surface on
    the node (unlike task VMs). Rather than fabricate VM-shaped data, return an
    empty set with ``supported=true`` so the UI renders the normal "no ports"
    empty state (button stays enabled, dialog opens). When node-side editor
    port discovery lands, this is where it surfaces.
    """
    await _resolve_editor_workspace(editor_id, str(user.id))
    return {"ports": [], "supported": True}


async def _resolve_ws_user(websocket: WebSocket) -> User | None:
    """Resolve the C-side user from the session cookie (mirror admin ws)."""
    cookie = websocket.cookies.get(USER_SESSION_COOKIE)
    if not cookie:
        return None
    try:
        data = await session_store.get(USER_SESSION_COOKIE, cookie)
    except Exception as exc:  # noqa: BLE001 - redis down must not 500 the ws
        logger.warning("[editor-ws] terminal session lookup failed: {}", exc)
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


def _parse_size(raw: str) -> tuple[int, int]:
    try:
        payload = json.loads(raw or "{}")
        return int(payload.get("row") or 0), int(payload.get("col") or 0)
    except (ValueError, TypeError):
        return 0, 0


async def _resolve_editor_workspace_ws(editor_id: str, user: User) -> tuple[dict, str] | None:
    """WebSocket variant: authenticate editor and provision its workspace."""
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    if not editor:
        return None
    try:
        return editor, await _ensure_editor_workspace(editor)
    except HTTPException:
        return None

@router.get("/{editor_id}/terminals")
async def list_editor_terminals(
    editor_id: str, user: User = Depends(get_current_user)
) -> dict:
    _, node_session_id = await _resolve_editor_workspace(editor_id, str(user.id))
    from . import config
    from .node_client.terminal import request_terminals

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        raise HTTPException(status_code=503, detail="控制面未配置")
    return await request_terminals(
        base, token, f"/internal/session-terminals/{node_session_id}"
    )


@router.delete("/{editor_id}/terminals/{terminal_id}")
async def delete_editor_terminal(
    editor_id: str,
    terminal_id: str,
    user: User = Depends(get_current_user),
) -> dict:
    _, node_session_id = await _resolve_editor_workspace(editor_id, str(user.id))
    from . import config
    from .node_client.terminal import request_terminals

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


@router.websocket("/{editor_id}/terminals/connect")
async def connect_editor_terminal(websocket: WebSocket, editor_id: str) -> None:
    await editor_terminal(websocket, editor_id)


@router.websocket("/{editor_id}/terminal")
async def editor_terminal(websocket: WebSocket, editor_id: str) -> None:
    """Interactive shell in the editor's workspace directory (owner only).

    Editor-level: uses any active task's node session as the handle; the node
    resolves the workspace path from the session's ``editor_workdir`` tag (the
    server never dictates a node-local path). Gated to the editor owner.
    """
    user = await _resolve_ws_user(websocket)
    if user is None:
        await _reject_ws(websocket, _CLOSE_UNAUTHORIZED, "需要登录")
        return

    binding = await _resolve_editor_workspace_ws(editor_id, user)
    if binding is None:
        await _reject_ws(websocket, _CLOSE_NOT_FOUND, "编辑器不存在或工作区创建失败，请确认节点在线并已绑定")
        return
    editor, node_session_id = binding

    from . import config

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        await _reject_ws(websocket, _CLOSE_UNAVAILABLE, "控制面未配置")
        return
    from .node_client.terminal import proxy_terminal

    terminal_id = (websocket.query_params.get("terminal_id") or "").strip()
    if not terminal_id or len(terminal_id) > 128:
        await _reject_ws(websocket, 4400, "终端 ID 无效")
        return

    # The data service authorized the editor owner above; the control process
    # owns the live Registry/PTY. Bridge the browser frames through the internal
    # session-terminal endpoint (bearer-gated). node_session_id / node id never
    # reach the browser.
    await proxy_terminal(websocket, base, token, f"/internal/session-terminal/{node_session_id}")
