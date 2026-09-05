"""Unified notification core: emit → outbox → worker.

``emit_notification`` is the single entry every hook site calls. It writes one
``mc_notify_outbox`` row (a pending push) carrying the event params and an
*envelope* (title/message/severity/… for the in-app bell). It no longer writes
a ``notifications`` row unconditionally — the notification center is now just
another channel kind (``notify_center``): a row appears in the bell only when a
configured event is bound to a ``notify_center`` channel. This is the spec's
"不是默认所有日志都进通知中心" — the in-app center is opt-in per event, exactly
like the webhook/IM channels.

The worker drains the outbox: match ``mc_notify_events`` by event type, owner,
status, effective date/time window, event parameters and trigger conditions;
then follow ``mc_notify_event_channels`` bindings to deliver to each channel.
For a ``notify_center`` channel, "delivery" writes the ``notifications`` row
(the bell + the surface the console and mobile app both read). For IM/webhook
channels it POSTs the bot webhook. Delivery attempts are recorded in
``mc_notify_send_logs``.

Startup gate (spec): the worker re-scans ``status='pending'`` outbox rows from
the DB *before* flipping ``_initialized`` (an ``asyncio.Event``). Until then
``emit_notification`` only writes rows — they are picked up by the startup
scan (or the 30s top-up sweep), never lost. A CAS ``pending → pushing`` update
on consume makes a row safe to enqueue more than once (restart re-scan, top-up
sweep, in-memory queue) — only one consumer pushes.

Storage is raw SQL over ``db.PostgresClient.pool`` (not Tortoise): this process
and the node-server process both init ``PostgresClient`` but register different
Tortoise models, so raw SQL is what lets one dispatcher serve both — same
reasoning as :mod:`notify_dispatch`.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Mapping

from loguru import logger

from .notify_dispatch import (
    _SIGNED_IN_REQUEST,
    _as_json,
    _log_send,
    _post,
    build_request,
)

# Event type → human title. Drives both the in-app notification title and the
# IM push body header. Kept here (not in notify_dispatch._EVENT_TITLES) so the
# new typed catalogue is the single source for the core domain.
_EVENT_TITLES = {
    # account
    "account.frozen": "账号已冻结",
    "account.unfrozen": "账号已解冻",
    "account.created": "账号新增",
    "account.init_failed": "账号初始化失败",
    "account.logged_out": "账号已退登",
    # channel
    "channel.frozen": "渠道已冻结",
    "channel.created": "渠道已创建",
    # api key
    "api_key.created": "密钥已创建",
    "api_key.modified": "密钥已修改",
    "api_key.usage_threshold": "密钥用量已达阈值",
    # model / node / security
    "model.new": "发现新模型",
    "node.new_version": "节点有新版本",
    "node.online": "节点已上线",
    "security.warning": "安全告警",
    # system
    "log.cleanup_done": "日志清理完成",
    "log.cleanup_failed": "日志清理失败",
    # task (user-side)
    "task.created": "任务已创建",
    "task.ended": "任务已结束",
    "task.idle": "任务停留",
    "task.deleted": "任务已删除",
}

_MAX_TEXT = 2000
_TOPUP_INTERVAL = 30  # seconds between pending re-scans (drain catch-up + crash recovery)

# The in-app notification center is a channel kind, not a default sink: a
# ``notifications`` row (bell + app push) is written only when a configured
# event binds a channel of this kind. Everything else is an outbound HTTP POST.
NOTIFY_CENTER_KIND = "notify_center"

_PENDING_BG: set[asyncio.Task] = set()

_initialized: asyncio.Event = asyncio.Event()
_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_topup_task: asyncio.Task | None = None
_stop = False


def _clip(text: str) -> str:
    text = (text or "").strip()
    return text if len(text) <= _MAX_TEXT else text[: _MAX_TEXT - 1] + "…"


# Keys carried in params for matching/conditions rather than for the reader.
# ``severity`` is injected by ``emit_notification`` so a configured
# ``trigger_condition.type=severity`` has a value to compare against; rendering
# it would add a bare "severity：error" line to every push body.
_RENDER_SKIP_KEYS = frozenset({"severity"})


def _render(event_type: str, params: Mapping[str, Any]) -> str:
    """Render an event's params as the IM push body."""
    title = _EVENT_TITLES.get(event_type, event_type)
    lines = [title]
    for key, value in (params or {}).items():
        if key in _RENDER_SKIP_KEYS:
            continue
        text = str(value or "").strip()
        if text:
            lines.append(f"{key}：{text}")
    return _clip("\n".join(lines))


def _filter_matches(filters: Mapping[str, Any] | None, params: Mapping[str, Any]) -> bool:
    """A rule's ``filters`` against the event's ``params``.

    Empty filters = match all. Otherwise every dimension must hit: the filter
    value (a list) must intersect the event's value for that key (singular form
    accepted too — ``provider_names`` filter vs ``provider_name`` param).
    """
    if not filters:
        return True
    for key, want in filters.items():
        if not want:
            continue  # empty list for this dimension = no constraint
        want_list = want if isinstance(want, list) else [want]
        singular = key[:-1] if (key.endswith("s") and len(key) > 1) else key
        got = (params or {}).get(key)
        if got is None:
            got = (params or {}).get(singular)
        if got is None:
            return False  # event carries no value for this dimension
        got_list = got if isinstance(got, list) else [str(got)]
        if not {str(x) for x in got_list} & {str(x) for x in want_list}:
            return False
    return True


def _maybe_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


async def _match_channels(
    event_type: str, owner_type: str, owner_id: Any, params: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Resolve channels through configured event definitions and bindings.

    The event definition is the source of truth: status/time window, configured
    event parameters, and trigger conditions are all evaluated before the
    event→channel binding is used. The old subscription-rule table is not read
    here; it remains only as a compatibility table for already deployed data.
    """
    from datetime import datetime, timezone

    from db import PostgresClient

    if PostgresClient.pool is None:
        return []
    oid = _maybe_uuid(owner_id)
    if owner_type == "platform":
        oid = oid or uuid.UUID("00000000-0000-0000-0000-0000000009f1")
        owner_sql = "e.owner_type = 'platform' AND e.owner_id = $2"
        args: tuple[Any, ...] = (oid,)
    elif owner_type == "user" and oid is not None:
        owner_sql = (
            "(e.owner_type = 'user' AND e.owner_id = $2) OR "
            "(e.owner_type = 'team' AND e.owner_id IN "
            "(SELECT team_id FROM mc_team_members WHERE user_id = $2))"
        )
        args = (oid,)
    elif owner_type == "team" and oid is not None:
        owner_sql = "e.owner_type = 'team' AND e.owner_id = $2"
        args = (oid,)
    else:
        return []

    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT e.id AS event_id, e.effective_from, e.effective_to, "
            "e.daily_start, e.daily_end, e.trigger_condition, e.event_params, "
            "c.id, c.kind, c.webhook_url, c.secret, c.headers "
            "FROM mc_notify_events e "
            "JOIN mc_notify_event_channels ec ON ec.event_id = e.id "
            "JOIN mc_notify_channels c ON c.id = ec.channel_id "
            f"WHERE e.event_type = $1 AND e.status = 'active' "
            f"AND ec.enabled AND c.enabled AND ({owner_sql})",
            event_type,
            *args,
        )
        # Group by event first. The JOIN yields one row per (event, channel), but
        # status/window/params/trigger-condition are properties of the *event* —
        # evaluating them per row would advance the "N consecutive times" counter
        # once per bound channel, so an event bound to 3 channels would satisfy
        # "notify on the 3rd occurrence" on its very first occurrence.
        by_event: dict[Any, list[Mapping[str, Any]]] = {}
        for row in rows:
            by_event.setdefault(row["event_id"], []).append(row)

        matched: list[dict[str, Any]] = []
        seen_channels: set[Any] = set()
        now = datetime.now(timezone.utc)
        fingerprint = _event_fingerprint(params)
        for event_id, event_rows in by_event.items():
            head = event_rows[0]
            if not _event_time_active(head, now):
                continue
            event_params = _as_json(head.get("event_params")) or {}
            if not _filter_matches(event_params, params):
                continue
            condition = _as_json(head.get("trigger_condition")) or {}
            if not await _trigger_condition_allows(
                conn, event_id, fingerprint, condition, params, now
            ):
                continue
            for row in event_rows:
                channel_id = row.get("id")
                # One event may bind a channel that another event also binds;
                # dedupe so the same webhook is not hit twice for one occurrence.
                if channel_id in seen_channels:
                    continue
                seen_channels.add(channel_id)
                matched.append(dict(row))
        return matched


def _coerce_time(value: Any) -> Any:
    """Normalize DB/API daily-window values to ``datetime.time``."""
    from datetime import time as _time

    if value is None or hasattr(value, "hour"):
        return value
    text = str(value)
    try:
        parts = [int(p) for p in text.split(":")[:3]]
        while len(parts) < 3:
            parts.append(0)
        return _time(*parts)
    except (TypeError, ValueError):
        return None


def _event_time_active(row: Mapping[str, Any], now: Any) -> bool:
    """Check absolute effective range and optional daily time window."""
    start, end = row.get("effective_from"), row.get("effective_to")
    if start is not None and now < start:
        return False
    if end is not None and now > end:
        return False
    daily_start, daily_end = _coerce_time(row.get("daily_start")), _coerce_time(row.get("daily_end"))
    if daily_start is None or daily_end is None:
        return True
    current = now.timetz().replace(tzinfo=None)
    start_time = daily_start.replace(tzinfo=None) if hasattr(daily_start, "replace") else daily_start
    end_time = daily_end.replace(tzinfo=None) if hasattr(daily_end, "replace") else daily_end
    # A window crossing midnight is valid from start→24:00 OR 00:00→end.
    if start_time <= end_time:
        return start_time <= current <= end_time
    return current >= start_time or current <= end_time


def _event_fingerprint(params: Mapping[str, Any]) -> str:
    """Stable state key; event parameters can contain arbitrary JSON values."""
    return json.dumps(dict(params), ensure_ascii=False, sort_keys=True, default=str)[:255]


def _compare(actual: Any, op: str, expected: Any) -> bool:
    try:
        left, right = float(actual), float(expected)
    except (TypeError, ValueError):
        left, right = str(actual), str(expected)
    return {
        ">": left > right, ">=": left >= right, "<": left < right,
        "<=": left <= right, "==": left == right, "!=": left != right,
    }.get(op, False)


async def _trigger_condition_allows(conn: Any, event_id: Any, fingerprint: str,
                                    condition: Mapping[str, Any], params: Mapping[str, Any], now: Any) -> bool:
    """Evaluate severity, threshold, count and silence conditions.

    ``conditions`` is treated as AND. A single object is also accepted for
    compact configs. Runtime counters live in ``mc_notify_event_states``.
    """
    if not condition:
        return True
    conditions = condition.get("conditions")
    if not isinstance(conditions, list):
        conditions = [condition]
    state = await conn.fetchrow(
        "SELECT id, window_started_at, count, last_triggered_at "
        "FROM mc_notify_event_states WHERE event_id=$1 AND fingerprint=$2",
        event_id, fingerprint,
    )
    state_id = state["id"] if state else uuid.uuid4()
    count = int(state["count"] if state else 0)
    window_started = state["window_started_at"] if state else None
    last_triggered = state["last_triggered_at"] if state else None
    for item in conditions:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "").lower()
        if kind == "severity":
            levels = {"info": 0, "warn": 1, "warning": 1, "error": 2, "critical": 3, "high": 3}
            actual = levels.get(str(params.get("severity") or "info").lower(), 0)
            minimum = levels.get(str(item.get("min") or "info").lower(), 0)
            if actual < minimum:
                return False
        elif kind == "threshold":
            field = str(item.get("field") or "")
            if field not in params or not _compare(params.get(field), str(item.get("op") or ">="), item.get("value")):
                return False
        elif kind == "silence":
            window = max(0, int(item.get("window_seconds") or 0))
            if window and last_triggered is not None and (now - last_triggered).total_seconds() < window:
                return False
        elif kind == "count":
            window = max(0, int(item.get("window_seconds") or 0))
            if window and window_started is not None and (now - window_started).total_seconds() >= window:
                count, window_started = 0, now
            count += 1
            if count < max(1, int(item.get("times") or 1)):
                await conn.execute(
                    "INSERT INTO mc_notify_event_states "
                    "(id,event_id,fingerprint,window_started_at,count,last_triggered_at) "
                    "VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_id,fingerprint) DO UPDATE SET "
                    "window_started_at=$4,count=$5,last_triggered_at=$6,updated_at=now()",
                    state_id, event_id, fingerprint, window_started or now, count, last_triggered,
                )
                return False
            count, window_started = 0, now
    await conn.execute(
        "INSERT INTO mc_notify_event_states "
        "(id,event_id,fingerprint,window_started_at,count,last_triggered_at) "
        "VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_id,fingerprint) DO UPDATE SET "
        "window_started_at=$4,count=$5,last_triggered_at=$6,updated_at=now()",
        state_id, event_id, fingerprint, window_started or now, count, now,
    )
    return True


async def _deliver_notify_center(
    event_type: str,
    envelope: Mapping[str, Any],
    params: Mapping[str, Any],
    owner_type: str,
    owner_id: Any,
) -> None:
    """Write the in-app ``notifications`` row for a ``notify_center`` channel.

    This is the delivery half of the notification center: the bell, the console
    list and the mobile app all read this table. ``upsert_notification`` is
    reused so ``dedupe_key``/``occurrence_count`` collapse repeats instead of
    flooding the center. Raises on failure so the caller records a failed send
    log, same as a rejected webhook.
    """
    from db import PostgresClient

    row = await PostgresClient.upsert_notification(
        {
            "severity": envelope.get("severity") or params.get("severity") or "info",
            "kind": envelope.get("kind") or event_type.split(".")[0],
            "source": envelope.get("source") or event_type,
            "title": envelope.get("title") or _EVENT_TITLES.get(event_type, event_type),
            "message": envelope.get("message") or "",
            "detail": envelope.get("detail") or "",
            "metadata": dict(params),
            "event_type": event_type,
            "owner_type": owner_type,
            "user_id": owner_id,
            "dedupe_key": envelope.get("dedupe_key"),
            "dedupe_window_seconds": envelope.get("dedupe_window_seconds") or 300,
            "request_log_id": envelope.get("request_log_id"),
            "provider_name": params.get("provider_name"),
            "account_username": params.get("account_username"),
            "model": params.get("model_id"),
        }
    )
    if row is None:
        raise RuntimeError("notification center write returned no row")


async def _consume_one(outbox_id: Any) -> None:
    """Push one outbox row to every matched channel, then finalize it.

    The ``pending → pushing`` UPDATE is a CAS: only the consumer that flips it
    proceeds, so a row enqueued more than once (re-scan, top-up) is pushed once.
    """
    from db import PostgresClient

    if PostgresClient.pool is None:
        return
    oid = _maybe_uuid(outbox_id)
    if oid is None:
        return
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE mc_notify_outbox SET status='pushing', attempt_count=attempt_count+1, "
            "started_at=now() "
            "WHERE id=$1 AND status='pending' "
            "RETURNING id, event_type, params, envelope, owner_type, owner_id",
            oid,
        )
    if row is None:
        return  # already being pushed / done by another consumer

    params = _as_json(row.get("params")) or {}
    envelope = _as_json(row.get("envelope")) or {}
    channels = await _match_channels(row["event_type"], row["owner_type"], row["owner_id"], params)
    text = _render(row["event_type"], params)
    status, error = "success", ""
    logs: list[tuple[Any, str, str]] = []  # (channel_id, status, error)

    for ch in channels:
        channel_id = ch.get("id")
        kind = str(ch.get("kind") or "")
        webhook_url = str(ch.get("webhook_url") or "")
        secret = str(ch.get("secret") or "")
        # The notification center is a channel, so it goes through the same
        # match → deliver → log path; only the transport differs (DB write vs
        # HTTP POST), and it has no webhook_url to validate.
        if kind == NOTIFY_CENTER_KIND:
            try:
                await _deliver_notify_center(
                    row["event_type"], envelope, params, row["owner_type"], row["owner_id"]
                )
                logs.append((channel_id, "success", ""))
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                error = str(exc) or exc.__class__.__name__
                logs.append((channel_id, "failed", error))
                logger.info("[notify] outbox {} notify-center write failed: {}", outbox_id, error)
            continue
        if not webhook_url:
            logs.append((channel_id, "failed", "channel has no webhook_url"))
            continue
        try:
            req_url, body, headers = build_request(
                kind, webhook_url, secret,
                event_type=row["event_type"], text=text, payload=params,
            )
            extra = _as_json(ch.get("headers")) or {}
            if isinstance(extra, dict):
                headers.update({str(k): str(v) for k, v in extra.items()})
            await _post(req_url, body, headers, secret, sign_body=kind not in _SIGNED_IN_REQUEST)
            logs.append((channel_id, "success", ""))
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            error = str(exc) or exc.__class__.__name__
            logs.append((channel_id, "failed", error))
            logger.info("[notify] outbox {} channel {} delivery failed: {}", outbox_id, channel_id, error)

    try:
        async with PostgresClient.pool.acquire() as conn:
            for channel_id, st, er in logs:
                await _log_send(
                    conn,
                    subscription_id=None,
                    channel_id=channel_id,
                    event_type=row["event_type"],
                    event_ref_id=str(outbox_id),
                    status=st,
                    error=er,
                )
            await conn.execute(
                "UPDATE mc_notify_outbox SET status=$1, last_error=$2, pushed_at=now() WHERE id=$3",
                status, (error or "")[:2000], oid,
            )
    except Exception:  # noqa: BLE001
        logger.exception("[notify] finalize outbox {} failed", outbox_id)


async def _drain_pending() -> None:
    """Enqueue every pending outbox row. CAS on consume makes re-enqueue safe."""
    from db import PostgresClient

    if PostgresClient.pool is None or _queue is None:
        return
    try:
        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM mc_notify_outbox WHERE status='pending' ORDER BY created_at"
            )
        for r in rows:
            await _queue.put(str(r["id"]))
    except Exception:  # noqa: BLE001
        logger.exception("[notify] drain pending outbox failed")


async def _worker_loop() -> None:
    while not _stop:
        try:
            outbox_id = await _queue.get()
            await _consume_one(outbox_id)
        except asyncio.CancelledError:
            break
        except Exception:  # noqa: BLE001
            logger.exception("[notify] worker loop error")


async def _topup_loop() -> None:
    """Re-scan pending rows on a cadence: catches anything written during the
    startup drain window (before _initialized was set) and recovers from a
    consumer that died mid-push leaving rows stuck in 'pushing'."""
    while not _stop:
        try:
            await asyncio.sleep(_TOPUP_INTERVAL)
            # Stale 'pushing' rows (consumer crashed) → back to 'pending'.
            from db import PostgresClient
            if PostgresClient.pool is not None:
                async with PostgresClient.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE mc_notify_outbox SET status='pending', started_at=NULL "
                        "WHERE status='pushing' "
                        "AND started_at < now() - interval '5 minutes'"
                    )
            await _drain_pending()
        except asyncio.CancelledError:
            break
        except Exception:  # noqa: BLE001
            logger.exception("[notify] topup loop error")


async def emit_notification(
    event_type: str,
    *,
    params: Mapping[str, Any] | None = None,
    owner_type: str = "platform",
    owner_id: Any = None,
    severity: str = "info",
    kind: str | None = None,
    source: str | None = None,
    title: str | None = None,
    message: str = "",
    detail: str = "",
    dedupe_key: str | None = None,
    dedupe_window_seconds: int = 300,
    request_log_id: int | None = None,
) -> str | None:
    """Record one event occurrence as a pending outbox row.

    No ``notifications`` row is written here: the in-app center is a channel
    (``notify_center``), so the bell only fills when a configured event binds
    such a channel — the worker does that write during delivery. The bell-facing
    fields (title/message/severity/…) are stashed in the outbox ``envelope`` so
    the worker can reconstruct the row without re-deriving them.

    ``owner_type`` ∈ {platform, user, team}; ``owner_id`` is the user/team id
    (None for platform). ``params`` carries the event specifics and doubles as
    the push template variables. Returns the outbox id (or None if the DB is
    unreachable). Never raises.
    """
    from db import PostgresClient

    params = dict(params or {})
    # Severity is part of the event envelope as well as the in-app row. Keeping
    # it in outbox params lets a configured ``trigger_condition.type=severity``
    # compare the actual occurrence against its minimum.
    params.setdefault("severity", severity)
    # Everything the notify-center channel needs to build a bell row later.
    envelope = {
        "severity": severity,
        "kind": kind or event_type.split(".")[0],
        "source": source or event_type,
        "title": title or _EVENT_TITLES.get(event_type, event_type),
        "message": message,
        "detail": detail,
        "dedupe_key": dedupe_key,
        "dedupe_window_seconds": dedupe_window_seconds,
        "request_log_id": request_log_id,
    }

    oid = uuid.uuid4()
    try:
        if PostgresClient.pool is not None:
            async with PostgresClient.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO mc_notify_outbox "
                    "(id, notification_id, event_type, params, envelope, owner_type, owner_id, status) "
                    "VALUES ($1, NULL, $2, $3::jsonb, $4::jsonb, $5, $6, 'pending')",
                    oid,
                    event_type[:64],
                    json.dumps(params, ensure_ascii=False),
                    json.dumps(envelope, ensure_ascii=False),
                    owner_type[:16],
                    _maybe_uuid(owner_id),
                )
            if _initialized.is_set() and _queue is not None:
                await _queue.put(str(oid))
    except Exception:  # noqa: BLE001
        logger.exception("[notify] insert outbox failed for {}", event_type)
    return str(oid)


def emit_notification_background(*args: Any, **kwargs: Any) -> None:
    """Fire-and-forget :func:`emit_notification` for hook sites that must not block."""
    try:
        task = asyncio.ensure_future(emit_notification(*args, **kwargs))
        _PENDING_BG.add(task)
        task.add_done_callback(_PENDING_BG.discard)
    except RuntimeError:
        logger.debug("[notify] no running loop; skipped")


async def start() -> None:
    """Start the worker: drain pending, flip _initialized, run consume + top-up loops."""
    global _queue, _worker_task, _topup_task
    if _queue is not None:
        return  # already started
    _queue = asyncio.Queue()
    await _drain_pending()  # backlog first…
    _initialized.set()  # …then accept live enqueues from emit_notification
    _worker_task = asyncio.create_task(_worker_loop(), name="notify-outbox-worker")
    _topup_task = asyncio.create_task(_topup_loop(), name="notify-outbox-topup")
    logger.info("[notify] outbox worker started, backlog drained")


async def stop() -> None:
    """Stop the worker and clear state so a later start() reinitializes cleanly."""
    global _stop, _worker_task, _topup_task, _queue
    _stop = True
    for task in (_worker_task, _topup_task):
        if task is not None:
            task.cancel()
    for task in (_worker_task, _topup_task):
        if task is not None:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
    _worker_task = _topup_task = None
    _queue = None
    _initialized.clear()
    _stop = False
