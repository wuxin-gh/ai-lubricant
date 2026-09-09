"""Remote node forward-proxy client (data service side).

The control process owns the live NodeConnect stream and the forward-proxy
endpoint. The data service's :class:`RemoteNodeConnectManager` mirrors the
``request()`` surface and streams the node's HTTP response back over HTTP so
``ProxyManager._request_via_node`` is unchanged.
"""
from __future__ import annotations

import base64
import json
from typing import AsyncIterator, Optional

_STATUS_HEADER = "x-node-proxy-status"
_HEADERS_HEADER = "x-node-proxy-headers"


class RemoteNodeProxyResponse:
    """Data-side view over the control node-proxy HTTP stream."""

    def __init__(self, session, response, status: int, headers: dict):
        self._session = session
        self._response = response
        self.status = status
        self.headers = headers
        self._closed = False

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._response.content.iter_chunked(64 * 1024):
                yield chunk
        finally:
            await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._response.release()
        except Exception:  # noqa: BLE001
            pass
        try:
            await self._session.close()
        except Exception:  # noqa: BLE001
            pass


class RemoteNodeConnectManager:
    """Data-service stand-in for the control-side NodeConnectManager."""

    def __init__(self, *, base_url: str, token: str, timeout: float = 120.0):
        self._base_url = (base_url or "").rstrip("/")
        self._token = (token or "").strip()
        self._timeout = timeout

    async def request(
        self,
        node_id: str,
        *,
        method: str,
        url: str,
        headers: Optional[dict] = None,
        body: bytes = b"",
    ) -> RemoteNodeProxyResponse:
        if not self._base_url or not self._token:
            raise ConnectionError("control plane node proxy is not configured")
        import aiohttp

        descriptor = {
            "method": method,
            "url": url,
            "headers": headers or {},
            "body": base64.b64encode(body or b"").decode("ascii"),
        }
        endpoint = f"{self._base_url}/internal/node-proxy/{node_id}"
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_read=self._timeout))
        try:
            response = await session.post(
                endpoint,
                json=descriptor,
                headers={"Authorization": f"Bearer {self._token}"},
            )
        except Exception:
            await session.close()
            raise ConnectionError("control plane node proxy unreachable")
        if response.status >= 400:
            message = (await response.text())[:500]
            response.release()
            await session.close()
            raise ConnectionError(f"node proxy failed: HTTP {response.status} {message}")
        try:
            status = int(response.headers.get(_STATUS_HEADER) or 200)
            headers = json.loads(response.headers.get(_HEADERS_HEADER) or "{}")
        except (TypeError, ValueError):
            status, headers = 200, {}
        return RemoteNodeProxyResponse(session, response, status, headers)
