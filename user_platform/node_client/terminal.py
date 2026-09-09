"""Data-side terminal websocket proxy copied from the control gateway.

It carries browser JSON frames to the internal control websocket. Browser
identity/ownership authorization remains in the data route; the control token
never reaches the browser.
"""
from __future__ import annotations

import asyncio
import contextlib
from urllib.parse import urlencode

from fastapi import HTTPException, WebSocket, WebSocketDisconnect


async def request_terminals(
    control_url: str,
    token: str,
    path: str,
    *,
    method: str = "GET",
    json_body: dict | None = None,
    timeout: float = 30,
) -> dict:
    """Call one bearer-gated control-plane terminal registry endpoint."""
    import aiohttp

    url = f"{control_url.rstrip('/')}{path}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as client:
            async with client.request(
                method,
                url,
                headers={"Authorization": f"Bearer {token}"},
                json=json_body,
            ) as response:
                raw = await response.text()
                try:
                    payload = __import__("json").loads(raw or "{}")
                except (TypeError, ValueError):
                    payload = {}
                if response.status >= 400:
                    detail = payload.get("detail") if isinstance(payload, dict) else None
                    raise HTTPException(response.status, str(detail or raw or "终端服务错误"))
                return payload if isinstance(payload, dict) else {}
    except HTTPException:
        raise
    except aiohttp.ClientError as exc:
        raise HTTPException(503, f"控制面终端服务不可用: {exc}") from exc


async def run_terminal_command(
    control_url: str,
    token: str,
    node_id: str,
    terminal_id: str,
    command: str,
    *,
    cwd: str = "",
    timeout_ms: int = 0,
    max_output_bytes: int = 0,
) -> dict:
    """Run one agent command inside the browser-attached PTY for this terminal.

    The control service writes the command into that PTY (so the operator sees
    it) and returns the bounded result. The command's own timeout bounds the
    PTY wait; the HTTP call is given a little more so a completing command is
    never cut off by the transport before its result comes back.
    """
    http_timeout = (min(max(int(timeout_ms or 0), 0), 600_000) / 1000) + 30 if timeout_ms else 180
    return await request_terminals(
        control_url,
        token,
        f"/internal/node-terminal/{node_id}/{terminal_id}/commands",
        method="POST",
        json_body={
            "command": command,
            "cwd": cwd,
            "timeout_ms": timeout_ms,
            "max_output_bytes": max_output_bytes,
        },
        timeout=http_timeout,
    )


async def proxy_terminal(
    websocket: WebSocket,
    control_url: str,
    token: str,
    path: str,
    *,
    owner: str = "",
) -> None:
    import aiohttp

    # The browser's own query params flow through, but `owner` is authoritative:
    # it is set by the authenticated data route (admin/user id) and must win over
    # any browser-supplied value so the control-plane attribution cannot be
    # spoofed. Drop any client `owner` before appending the trusted one.
    params = [(k, v) for k, v in websocket.query_params.multi_items() if k != "owner"]
    if owner:
        params.append(("owner", owner))
    query = urlencode(params)
    url = f"{control_url.rstrip('/')}{path}"
    if query:
        url += "?" + query
    await websocket.accept()
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as client:
            async with client.ws_connect(url, headers={"Authorization": f"Bearer {token}"}) as upstream:
                async def browser_to_control() -> None:
                    while True:
                        await upstream.send_str(await websocket.receive_text())

                async def control_to_browser() -> None:
                    async for message in upstream:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            await websocket.send_text(message.data)
                        elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            return

                await asyncio.gather(browser_to_control(), control_to_browser())
    except (WebSocketDisconnect, asyncio.CancelledError, aiohttp.ClientError):
        pass
    finally:
        with contextlib.suppress(Exception):
            await websocket.close()
