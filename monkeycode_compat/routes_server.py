"""Public server/runtime metadata required by the MonkeyCode user portal.

The vendored frontend requests this endpoint before authentication to determine
SaaS/private edition behavior. It uses the original generated Go client, so the
response intentionally keeps MonkeyCode's ``{code, message, data}`` envelope
rather than the plain REST responses used by the endpoint-map adapter.
"""
from __future__ import annotations

import os

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api/v1/server", tags=["monkeycode-server"])


def _version() -> str:
    return os.getenv("APP_VERSION", "dev").strip() or "dev"


@router.get("/config")
async def server_config() -> dict:
    """Return deployment metadata consumed during frontend bootstrap."""
    version = _version()
    return {
        "code": 0,
        "message": "",
        "data": {
            "current_version": version,
            "latest_version": version,
            "edition": "private",
            "region": (os.getenv("AI_LUBRICANT_REGION") or os.getenv("MONKEYCODE_REGION") or "cn").strip() or "cn",
        },
    }


@router.get("/client-ip")
async def server_client_ip(request: Request) -> dict:
    """Echo the caller's public IP.

    Replaces the vendored frontend's old call to the upstream
    ``monkeycode-ai.online/get-my-ip`` service: opening a VM port forward needs
    the browser's own IP for the whitelist, and the backend sees it on every
    request anyway (X-Forwarded-For first hop, else the direct peer).
    """
    from .deps import client_meta

    ip, _user_agent = client_meta(request)
    return {"code": 0, "message": "", "data": {"ip": ip or ""}}
