"""Data-side Connect server-stream passthrough for FollowToolRun.

Preserves the Connect streaming framing (5-byte prefix + protobuf) by relaying
the HTTP body verbatim; the control process owns the live tool-run event queues.
"""
from __future__ import annotations

import asyncio

from loguru import logger

_SERVICE = "agentcompose.v2.NodeService"
_PATH = f"/{_SERVICE}/FollowToolRun"


async def proxy_follow_toolrun_asgi(scope: dict, receive, send, *, base_url: str, token: str) -> None:
    """Pass the Connect server-stream through the data service unchanged."""
    if not (base_url or "").strip() or not (token or "").strip():
        await send({"type": "http.response.start", "status": 503, "headers": []})
        await send({"type": "http.response.body", "body": b"control plane unavailable", "more_body": False})
        return
    if scope.get("type") != "http":
        return
    if scope.get("method") != "POST":
        await send({"type": "http.response.start", "status": 405, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        return

    body = bytearray()
    while True:
        event = await receive()
        if event.get("type") == "http.disconnect":
            return
        if event.get("type") != "http.request":
            continue
        body.extend(event.get("body") or b"")
        if not event.get("more_body", False):
            break

    import aiohttp

    url = f"{base_url.rstrip('/')}{_PATH}"
    headers = {
        "Content-Type": "application/connect+proto",
        "Connect-Protocol-Version": "1",
        "Authorization": f"Bearer {token}",
    }
    timeout = aiohttp.ClientTimeout(total=None)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.post(url, data=bytes(body), headers=headers) as response:
                response_headers = []
                for key, value in response.headers.items():
                    if key.lower() not in {"content-length", "transfer-encoding", "connection"}:
                        response_headers.append((key.lower().encode(), value.encode()))
                await send({"type": "http.response.start", "status": response.status, "headers": response_headers})
                async for chunk in response.content.iter_chunked(64 * 1024):
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
                await send({"type": "http.response.body", "body": b"", "more_body": False})
    except (aiohttp.ClientError, asyncio.CancelledError):
        logger.debug("[node-client] follow-toolrun proxy ended")
