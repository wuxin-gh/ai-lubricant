"""Outbound notify dispatch: one-way HTTP POST per subscribed channel.

The notify domain stores channels/subscriptions (``notify_service``); this module
is the missing delivery half. All four channel kinds are the *same* transport — a
single stateless ``POST`` to a bot webhook URL — and differ only in the JSON body
shape and how the shared secret is folded in:

* ``dingtalk`` — ``timestamp`` + ``sign`` as **query** params (HMAC over
  ``"{timestamp}\\n{secret}"``, base64, then urlencoded).
* ``feishu``   — ``timestamp`` + ``sign`` inside the **body** (HMAC keyed by
  ``"{timestamp}\\n{secret}"`` over an *empty* message).
* ``wecom``    — no signing at all; the secret is already inside the URL's key.
* ``webhook``  — our own event JSON, optionally signed with an
  ``X-Notify-Signature: sha256=<hex>`` header over the exact bytes sent.

Deliberately *not* an IM integration: no OAuth, no access-token refresh, no
callback endpoint, no member-id mapping. Every event in the catalogue is
"tell the user once", so nothing here needs to receive a reply.

Storage access is raw SQL over ``db.PostgresClient.pool`` rather than Tortoise.
That is the one design constraint worth stating: ``task.ended`` must fire from
:mod:`node_server`, a separate process that only registers its own ``mc_ac_*``
Tortoise models and so cannot query ``mc_notify_*`` through the ORM. Both
processes do initialize ``PostgresClient``, so raw SQL is what lets one
dispatcher serve both — same reasoning as ``node_server.task_finalize``.

Dispatch is always best-effort: a notification must never fail the task
lifecycle that triggered it. Every send attempt is recorded in
``mc_notify_send_logs`` so ``list_send_logs`` finally has rows to show.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import uuid
from typing import Any, Iterable, Mapping

from loguru import logger

_HTTP_TIMEOUT = 10
# A channel is a chat webhook, not a data sink: a wall of text is unreadable in
# a group and some providers reject oversized bodies outright.
_MAX_TEXT = 2000
# Cap the fan-out so a user who wired up dozens of channels cannot turn one task
# transition into an unbounded burst of outbound requests.
_MAX_CHANNELS_PER_EVENT = 20

# Human-readable titles for the catalogue in ``notify_service.NOTIFY_EVENT_TYPES``.
_EVENT_TITLES = {
    "task.created": "任务已创建",
    "task.ended": "任务已结束",
    "task.paused": "任务暂停",
    "vm.expiring_soon": "虚拟机即将到期",
    "quota.refreshed": "配额已刷新",
    "quota.basic_exhausted": "基础配额已耗尽",
    "quota.pro_exhausted": "Pro 配额已耗尽",
    "quota.ultra_exhausted": "Ultra 配额已耗尽",
}


def _clip(text: str) -> str:
    text = (text or "").strip()
    return text if len(text) <= _MAX_TEXT else text[: _MAX_TEXT - 1] + "…"


def render_text(event_type: str, fields: Mapping[str, Any]) -> str:
    """Render an event as the plain-text body IM bots display.

    ``fields`` is a small ordered label→value map supplied by the trigger site;
    empty values are dropped so a partially-known event still reads cleanly.
    """
    title = _EVENT_TITLES.get(event_type, event_type)
    lines = [title]
    for label, value in fields.items():
        text = str(value or "").strip()
        if text:
            lines.append(f"{label}：{text}")
    return _clip("\n".join(lines))


def _sign_dingtalk(url: str, secret: str) -> str:
    """Append DingTalk's ``timestamp``/``sign`` query params to the webhook URL."""
    timestamp = str(round(time.time() * 1000))
    digest = hmac.new(
        secret.encode("utf-8"), f"{timestamp}\n{secret}".encode("utf-8"), hashlib.sha256
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(digest).decode("utf-8"))
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}timestamp={timestamp}&sign={sign}"


def _sign_feishu(secret: str) -> dict[str, Any]:
    """Feishu signs an *empty* message with ``"{timestamp}\\n{secret}"`` as the key."""
    timestamp = str(int(time.time()))
    digest = hmac.new(
        f"{timestamp}\n{secret}".encode("utf-8"), b"", hashlib.sha256
    ).digest()
    return {"timestamp": timestamp, "sign": base64.b64encode(digest).decode("utf-8")}


def build_request(
    kind: str,
    webhook_url: str,
    secret: str,
    *,
    event_type: str,
    text: str,
    payload: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any], dict[str, str]]:
    """Return ``(url, json_body, extra_headers)`` for one channel kind.

    This is the only place the four kinds differ, and it is pure — no I/O — so
    the body/signature shape is unit-testable without a webhook server.
    """
    kind = (kind or "").strip().lower()
    url = webhook_url.strip()
    headers: dict[str, str] = {}

    if kind == "dingtalk":
        if secret:
            url = _sign_dingtalk(url, secret)
        return url, {"msgtype": "text", "text": {"content": text}}, headers

    if kind == "feishu":
        body: dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
        if secret:
            body.update(_sign_feishu(secret))
        return url, body, headers

    if kind == "wecom":
        # The bot key is already part of the URL; WeCom has no body signature.
        return url, {"msgtype": "text", "text": {"content": text}}, headers

    # Generic webhook: send the structured event, not a chat string. Signed over
    # the exact serialized bytes by the caller (which owns serialization).
    return url, {"event_type": event_type, "text": text, "data": dict(payload or {})}, headers


async def _post(
    url: str, body: dict[str, Any], headers: dict[str, str], secret: str, sign_body: bool
) -> None:
    """POST one notification, raising on transport or provider-level failure."""
    import aiohttp

    raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    send_headers = {"Content-Type": "application/json; charset=utf-8", **headers}
    if sign_body and secret:
        digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
        send_headers["X-Notify-Signature"] = f"sha256={digest}"

    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, data=raw, headers=send_headers) as response:
            text = (await response.text())[:500]
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}: {text}")
            # DingTalk/Feishu/WeCom all answer 200 with a non-zero ``errcode`` /
            # ``code`` when the robot rejects the message (bad sign, keyword not
            # matched, robot disabled). Treating that as success is why a broken
            # channel would otherwise look healthy in the send log.
            try:
                parsed = json.loads(text) if text else {}
            except ValueError:
                return
            if isinstance(parsed, dict):
                for key in ("errcode", "code", "StatusCode"):
                    if key in parsed and parsed[key] not in (0, None, "0"):
                        message = (
                            parsed.get("errmsg")
                            or parsed.get("msg")
                            or parsed.get("message")
                            or text
                        )
                        raise RuntimeError(f"provider rejected ({key}={parsed[key]}): {message}")


def _as_json(value: Any) -> Any:
    """asyncpg hands JSONB back as ``str`` unless a codec is installed."""
    if isinstance(value, (str, bytes)):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


async def _log_send(
    conn: Any,
    *,
    subscription_id: uuid.UUID | None,
    channel_id: uuid.UUID,
    event_type: str,
    event_ref_id: str,
    status: str,
    error: str,
) -> None:
    await conn.execute(
        "INSERT INTO mc_notify_send_logs "
        "(id, subscription_id, channel_id, event_type, event_ref_id, status, error, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
        uuid.uuid4(),
        subscription_id or channel_id,
        channel_id,
        event_type[:64],
        (event_ref_id or "")[:255],
        status[:32],
        (error or "")[:2000],
    )


async def _deliver_one(
    row: Mapping[str, Any], *, event_type: str, text: str, event_ref_id: str, payload: Mapping[str, Any]
) -> None:
    """Send to one channel and record the outcome. Never raises."""
    from db import PostgresClient

    kind = str(row["kind"] or "")
    secret = str(row["secret"] or "")
    webhook_url = str(row["webhook_url"] or "")
    channel_id = row["id"]
    subscription_id = row.get("subscription_id")
    status, error = "success", ""

    if not webhook_url:
        status, error = "failed", "channel has no webhook_url"
    else:
        try:
            url, body, headers = build_request(
                kind, webhook_url, secret, event_type=event_type, text=text, payload=payload
            )
            extra = _as_json(row.get("headers")) or {}
            if isinstance(extra, dict):
                headers.update({str(k): str(v) for k, v in extra.items()})
            await _post(url, body, headers, secret, sign_body=kind not in _SIGNED_IN_REQUEST)
        except Exception as exc:  # noqa: BLE001
            status, error = "failed", str(exc) or exc.__class__.__name__
            logger.info(
                "[notify] channel {} ({}) delivery failed: {}", channel_id, kind, error
            )

    try:
        if PostgresClient.pool is None:
            return
        async with PostgresClient.pool.acquire() as conn:
            await _log_send(
                conn,
                subscription_id=subscription_id,
                channel_id=channel_id,
                event_type=event_type,
                event_ref_id=event_ref_id,
                status=status,
                error=error,
            )
    except Exception:  # noqa: BLE001
        logger.exception("[notify] writing send log for channel {} failed", channel_id)


# Kinds that carry their signature inside the request itself, so the generic
# ``X-Notify-Signature`` header would be redundant.
_SIGNED_IN_REQUEST = frozenset({"dingtalk", "feishu", "wecom"})


async def _subscribed_channels(user_id: str, event_type: str) -> list[dict[str, Any]]:
    """Resolve every channel that should receive this user's event.

    Two owner scopes fan out together:

    * ``owner_type='user'`` — the acting user's own channels.
    * ``owner_type='team'`` — channels owned by any team the user belongs to, so
      one shared group robot covers every member without each of them wiring
      their own. Matching a team channel by ``owner_id = user_id`` would never
      hit (the owner is a *team* id), which is why the membership join is here
      rather than a second query at the call site.

    A channel carrying several matching subscription rows is de-duplicated, since
    the fan-out is per channel, not per subscription.
    """
    from db import PostgresClient

    if PostgresClient.pool is None:
        return []
    try:
        owner = uuid.UUID(str(user_id))
    except (ValueError, TypeError):
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT ON (c.id) "
            "       c.id, c.kind, c.webhook_url, c.secret, c.headers, "
            "       s.id AS subscription_id, c.created_at "
            "FROM mc_notify_channels c "
            "JOIN mc_notify_subscriptions s ON s.channel_id = c.id "
            "WHERE c.enabled AND s.enabled "
            "  AND s.event_types::jsonb @> $2::jsonb "
            "  AND ("
            "        (COALESCE(c.owner_type, 'user') = 'user' AND c.owner_id = $1)"
            "     OR (c.owner_type = 'team' AND c.owner_id IN ("
            "            SELECT team_id FROM mc_team_members WHERE user_id = $1"
            "        ))"
            "  ) "
            "ORDER BY c.id, c.created_at "
            "LIMIT $3",
            owner,
            json.dumps([event_type]),
            _MAX_CHANNELS_PER_EVENT,
        )
    return [dict(r) for r in rows]


async def dispatch_event(
    user_id: str,
    event_type: str,
    *,
    fields: Mapping[str, Any] | None = None,
    event_ref_id: str = "",
    payload: Mapping[str, Any] | None = None,
) -> int:
    """Fan an event out to every channel the user subscribed to it.

    Returns the number of channels attempted. Never raises — notification is
    strictly a side effect of the lifecycle transition that triggered it, so a
    dead webhook or an unreachable database must not surface to the caller.
    """
    try:
        rows = await _subscribed_channels(user_id, event_type)
        if not rows:
            return 0
        body = dict(payload or fields or {})
        text = render_text(event_type, fields or {})
        await asyncio.gather(
            *(
                _deliver_one(
                    row,
                    event_type=event_type,
                    text=text,
                    event_ref_id=event_ref_id,
                    payload=body,
                )
                for row in rows
            )
        )
        return len(rows)
    except Exception:  # noqa: BLE001
        logger.exception("[notify] dispatching {} for user {} failed", event_type, user_id)
        return 0


def dispatch_event_background(
    user_id: str,
    event_type: str,
    *,
    fields: Mapping[str, Any] | None = None,
    event_ref_id: str = "",
    payload: Mapping[str, Any] | None = None,
) -> None:
    """Fire-and-forget :func:`dispatch_event`.

    Used at lifecycle trigger sites (task created / task ended) so a slow or
    hanging webhook never adds latency to the request or frame loop that caused
    it. The task reference is dropped intentionally; ``dispatch_event`` already
    swallows and logs everything, so there is no lost exception to retrieve.
    """
    try:
        task = asyncio.ensure_future(
            dispatch_event(
                user_id,
                event_type,
                fields=fields,
                event_ref_id=event_ref_id,
                payload=payload,
            )
        )
        # Keep a strong reference until completion: bare ensure_future results
        # can otherwise be garbage-collected mid-flight.
        _PENDING.add(task)
        task.add_done_callback(_PENDING.discard)
    except RuntimeError:
        # No running loop (sync context / shutdown). Nothing to notify with.
        logger.debug("[notify] no running loop; skipped {} for {}", event_type, user_id)


_PENDING: set[asyncio.Task] = set()


async def send_test(channel: Mapping[str, Any], secret: str) -> tuple[bool, str]:
    """Deliver a test message to one channel, bypassing subscriptions.

    ``channel`` is a raw channel record; ``secret`` is the *unmasked* stored
    secret, which is why this takes it separately from the service-layer DTO.
    Returns ``(ok, error)`` and logs the attempt like any real send.
    """
    kind = str(channel.get("kind") or "")
    webhook_url = str(channel.get("webhook_url") or "")
    if not webhook_url:
        return False, "渠道未配置 webhook 地址"
    text = _clip("通知渠道测试\n这是一条来自平台的测试消息，收到即表示配置正确。")
    ok, error = True, ""
    try:
        url, body, headers = build_request(
            kind, webhook_url, secret, event_type="test", text=text, payload={}
        )
        extra = _as_json(channel.get("headers")) or {}
        if isinstance(extra, dict):
            headers.update({str(k): str(v) for k, v in extra.items()})
        await _post(url, body, headers, secret, sign_body=kind not in _SIGNED_IN_REQUEST)
    except Exception as exc:  # noqa: BLE001
        ok, error = False, str(exc) or exc.__class__.__name__

    try:
        from db import PostgresClient

        if PostgresClient.pool is not None:
            async with PostgresClient.pool.acquire() as conn:
                await _log_send(
                    conn,
                    subscription_id=None,
                    channel_id=channel["id"],
                    event_type="test",
                    event_ref_id="",
                    status="success" if ok else "failed",
                    error=error,
                )
    except Exception:  # noqa: BLE001
        logger.exception("[notify] writing test send log failed")
    return ok, error
