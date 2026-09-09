"""Expo push delivery for the ``mobile_push`` channel kind.

One small HTTP client: POST to Expo Push Service (``/--/api/v2/push/send``)
with the device's Expo push token. The response is classified so the caller
(``notify_core._consume_one``) can record success/failure and *retire* devices
whose token Expo reports as gone (``DeviceNotRegistered``) — a retired device
is disabled in ``mc_notify_devices`` so it stops matching future events, while
its channel row stays (re-registration re-enables it).

Design constraints (shared with :mod:`notify_dispatch`):
* best-effort — a push failure must never fail the task lifecycle that
  triggered the notification;
* raw SQL over ``db.PostgresClient.pool`` (the same dispatcher serves the
  data-service and node-server processes, which register different Tortoise
  models);
* the push token is sensitive: never logged in plaintext, never returned by
  the listing API.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from loguru import logger

# Endpoint is overridable for self-hosted Expo push gateways / proxies.
EXPO_PUSH_ENDPOINT = "https://exp.host/--/api/v2/push/send"
_HTTP_TIMEOUT = 10

# Android notification channel registered by the app (mobile/src/notifications/push.ts).
DEFAULT_ANDROID_CHANNEL = "task-events"


@dataclass(slots=True)
class PushResult:
    """Outcome of one Expo push send, in the vocabulary of the outbox loop."""

    ok: bool
    error: str = ""
    # Token is gone (uninstalled app, re-installed Expo app id, expired
    # credential). The caller should disable the device row.
    device_gone: bool = False


# Expo error details that mean "stop sending to this token forever".
_DEVICE_GONE_ERRORS = {
    "DeviceNotRegistered",
    "InvalidCredentials",
    "InvalidProviderToken",
    "MismatchSenderId",
    "TopicDisallowed",
}


def build_push_payload(
    push_token: str,
    *,
    title: str,
    body: str,
    data: Mapping[str, Any] | None = None,
    severity: str = "info",
) -> dict[str, Any]:
    """Construct one Expo push message. Pure — no I/O — so it is unit-testable.

    ``data`` carries the deep-link payload (route + ids) the app reads when the
    notification is tapped; it must stay JSON-serializable and small.
    """
    severity = (severity or "info").lower()
    return {
        "to": push_token,
        "title": (title or "").strip()[:120] or "通知",
        "body": (body or "").strip()[:240],
        "sound": "default",
        "channelId": DEFAULT_ANDROID_CHANNEL,
        # Android: color the notification per severity; legacy FCM payload kept
        # for pre-channel-API Androids.
        "priority": "high" if severity in ("error", "critical", "high") else "default",
        "data": {str(k): _scalar(v) for k, v in (data or {}).items()},
    }


def _scalar(value: Any) -> Any:
    """Expo ``data`` values must be scalars; stringify everything else."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def classify_expo_response(status: int, body: bytes | str) -> PushResult:
    """Map an Expo Push API response onto :class:`PushResult`.

    * 2xx with ``status == "ok"`` → success.
    * 2xx with a per-ticket ``error`` → inspect ``details.error``: the
      device-gone family retires the token; anything else is a transient or
      message-level failure.
    * 4xx with the same device-gone detail retires too; other 4xx are
      configuration errors (bad payload/API) — reported, not retried blindly.
    * 5xx / 429 → retryable failure.
    """
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else (body or "")
    try:
        parsed = json.loads(text) if text else {}
    except ValueError:
        parsed = {}
    # Expo returns {"data": [{"status":"ok", ...}]} when the request body is
    # an array (we send a one-item array). Accept the direct ticket form too so
    # a self-hosted compatible gateway can return {status,...}.
    ticket = parsed
    if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
        ticket = parsed["data"][0] if parsed["data"] else {}
    if 200 <= status < 300:
        if isinstance(ticket, dict) and str(ticket.get("status") or "") == "ok":
            return PushResult(ok=True)
        err = ticket if isinstance(ticket, dict) else {}
        detail = (err.get("details") or {}).get("error") if isinstance(err.get("details"), dict) else None
        message = str(err.get("message") or "") or "invalid Expo push ticket"
        return PushResult(
            ok=False,
            error=f"{detail or 'PushError'}: {message}",
            device_gone=detail in _DEVICE_GONE_ERRORS,
        )
    # Expo error envelope: {"errors": [{"code": ..., "message": ...}]}
    if isinstance(parsed.get("errors"), list) and parsed["errors"]:
        first = parsed["errors"][0]
        return PushResult(
            ok=False,
            error=f"HTTP {status}: {first.get('message', '')}".strip(),
        )
    return PushResult(ok=False, error=f"HTTP {status}")


async def send_expo_push(payload: Mapping[str, Any], *, endpoint: str = EXPO_PUSH_ENDPOINT) -> PushResult:
    """POST one message to Expo Push Service. Never raises."""
    import aiohttp

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
        ) as session:
            async with session.post(
                endpoint,
                json=[dict(payload)],
                headers={"Accept": "application/json"},
            ) as resp:
                return classify_expo_response(resp.status, await resp.read())
    except Exception as exc:  # noqa: BLE001 — push is best-effort
        logger.info("[notify] expo push request failed: {}", exc)
        return PushResult(ok=False, error=str(exc) or exc.__class__.__name__)


async def load_device_by_channel(channel_target_id: str | None) -> dict[str, Any] | None:
    """Fetch the enabled device row bound to one ``mobile_push`` channel.

    Raw SQL (see module docstring). Returns ``None`` when the channel is not
    device-bound or the device is disabled/gone — the caller treats that as a
    skipped delivery, not an error.
    """
    from db import PostgresClient

    if PostgresClient.pool is None or not channel_target_id:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, owner_id, push_token, enabled, platform "
            "FROM mc_notify_devices WHERE id = $1",
            _maybe_uuid(channel_target_id),
        )
    if row is None or not row["enabled"] or not (row["push_token"] or "").strip():
        return None
    return dict(row)


def _maybe_uuid(value: Any):
    import uuid as _uuid

    try:
        return _uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


async def disable_device(device_id: Any, reason: str) -> None:
    """Retire a device whose token Expo rejected; idempotent, never raises.

    The channel row (``mc_notify_channels``) is disabled as well so no future
    event matches it until the app re-registers (register re-enables both).
    """
    from db import PostgresClient

    did = _maybe_uuid(device_id)
    if PostgresClient.pool is None or did is None:
        return
    try:
        async with PostgresClient.pool.acquire() as conn:
            await conn.execute(
                "UPDATE mc_notify_devices SET enabled = false, last_error = $1, "
                "updated_at = now() WHERE id = $2",
                (reason or "")[:500],
                did,
            )
            await conn.execute(
                "UPDATE mc_notify_channels c SET enabled = false, updated_at = now() "
                "WHERE c.kind = 'mobile_push' AND c.target_id = $1",
                str(did),
            )
    except Exception:  # noqa: BLE001
        logger.exception("[notify] disable device {} failed", device_id)
