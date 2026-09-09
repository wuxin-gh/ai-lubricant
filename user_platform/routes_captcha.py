"""Self-hosted cap.js PoW CAPTCHA routes (``/api/v1/public/captcha``).

The vendored ``@cap.js/widget`` talks to a self-hosted backend via two POST
endpoints under its ``apiEndpoint`` (``/api/v1/public/captcha/``):

* ``POST challenge`` -> ``{"challenge": {c, s, d}, "token", "expires"}``
* ``POST redeem`` with ``{"token", "solutions": [...]}`` -> ``{"success",
  "token", "expires"}``

These responses are the widget's raw contract — plain JSON, NOT the Go
``web.Resp`` ``{code, message, data}`` envelope the rest of the compat surface
uses. The widget reads ``challenge``/``token``/``success``/``expires`` directly,
so wrapping them would break solving. This surface is public (pre-auth) by
design: it gates login/register. It never touches ``/admin/*`` or ``/v1/*``.
"""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from .captcha_service import captcha_service

router = APIRouter(prefix="/api/v1/public/captcha", tags=["user-platform-captcha"])


class RedeemReq(BaseModel):
    token: str
    solutions: list = []


@router.post("/challenge")
async def challenge() -> dict:
    """Issue a fresh PoW challenge. Raw cap.js shape (no web.Resp envelope)."""
    result = await captcha_service.create_challenge()
    return {
        "challenge": {
            "c": result.count,
            "s": result.salt_length,
            "d": result.difficulty,
        },
        "token": result.token,
        "expires": result.expires_ms,
    }


@router.post("/redeem")
async def redeem(body: RedeemReq) -> dict:
    """Verify PoW solutions; issue a short-lived verification token on success."""
    ok, verification, expires_ms = await captcha_service.redeem(body.token, body.solutions)
    if not ok:
        return {"success": False}
    return {"success": True, "token": verification, "expires": expires_ms}
