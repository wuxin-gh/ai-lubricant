"""Data-side HTTP tunnel client for editor workspace files.

Calls the control-plane session tunnel directly (token-gated). The browser
only ever sees editor_id; node_session_id and the control URL stay internal.
"""
from __future__ import annotations

import urllib.parse
from dataclasses import dataclass

from .errors import NodeServerUnavailable


@dataclass
class TunnelResponse:
    status: int
    headers: dict[str, str]
    body: bytes


async def request_session_tunnel(
    *,
    session_id: str,
    service: str,
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    body: bytes = b"",
    body_limit: int = 4 << 20,
    timeout_seconds: int = 30,
) -> TunnelResponse:
    """Forward one request to the control-plane session tunnel."""
    from .. import config

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        raise NodeServerUnavailable("未配置控制面 tunnel 地址/token")
    import aiohttp

    url = (
        f"{base}/api/nodes/sessions/"
        f"{urllib.parse.quote(session_id, safe='')}/"
        f"{urllib.parse.quote(service, safe='')}/"
        f"{(path or '/').lstrip('/')}"
    )
    outbound = dict(headers or {})
    outbound["Authorization"] = f"Bearer {token}"
    outbound.pop("host", None)
    timeout = aiohttp.ClientTimeout(total=max(1, timeout_seconds))
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.request(method, url, headers=outbound, data=body) as response:
                payload = await response.content.read(body_limit + 1)
                return TunnelResponse(
                    status=response.status,
                    headers={k.lower(): v for k, v in response.headers.items()},
                    body=payload,
                )
    except aiohttp.ClientError as exc:
        raise NodeServerUnavailable(f"控制面 tunnel 不可用: {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - timeout / connection reset
        raise NodeServerUnavailable(f"控制面 tunnel 请求失败: {exc}") from exc
