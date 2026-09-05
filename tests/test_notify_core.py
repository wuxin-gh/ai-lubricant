"""notify_core: emit → outbox → worker → push (P0 core).

Stubbed asyncpg pool + fake _post; no Postgres, no outbound HTTP. Mirrors the
``_FakeConn``/``_FakePool`` shape in ``test_notify_dispatch``.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, time, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio

from monkeycode_compat import notify_core
from monkeycode_compat.notify_core import _filter_matches, emit_notification


class _FakeConn:
    """Records every SQL call and returns scripted results per method."""

    def __init__(self):
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchval_returns: dict[str, Any] = {}  # sql-substring → value
        self.fetchrow_returns: list[dict | None] = []  # FIFO queue
        self.fetch_returns: list[list[dict]] = []  # FIFO queue

    async def fetchval(self, sql: str, *params: Any):
        self.fetches.append((sql, params))
        for sub, val in self.fetchval_returns.items():
            if sub in sql:
                return val
        return None

    async def fetchrow(self, sql: str, *params: Any):
        self.fetches.append((sql, params))
        return self.fetchrow_returns.pop(0) if self.fetchrow_returns else None

    async def fetch(self, sql: str, *params: Any):
        self.fetches.append((sql, params))
        return self.fetch_returns.pop(0) if self.fetch_returns else []

    async def execute(self, sql: str, *params: Any):
        self.executes.append((sql, params))
        return "INSERT 0 1"


class _FakePool:
    def __init__(self, conn: _FakeConn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(_self):
                return conn

            async def __aexit__(_self, *exc):
                return False

        return _Ctx()


def _install(monkeypatch, conn: _FakeConn) -> _FakeConn:
    import db

    monkeypatch.setattr(db.PostgresClient, "pool", _FakePool(conn))
    return conn


@pytest_asyncio.fixture(autouse=True)
async def _reset_worker():
    await notify_core.stop()
    yield
    await notify_core.stop()


# ── pure helpers ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_filter_matches_provider_list():
    """A rule scoped to provider_names only hits events for those providers."""
    assert _filter_matches({"provider_names": ["a", "b"]}, {"provider_name": "a"}) is True
    assert _filter_matches({"provider_names": ["a", "b"]}, {"provider_name": "c"}) is False
    assert _filter_matches({}, {"provider_name": "a"}) is True, "empty filters = match all"
    assert _filter_matches({"provider_names": ["a"]}, {}) is False, "event lacks the dimension"


@pytest.mark.asyncio
async def test_render_uses_event_title_and_drops_empty():
    text = notify_core._render("account.frozen", {"provider_name": "p", "reason": "403", "extra": ""})
    assert text.startswith("账号已冻结")
    assert "provider_name：p" in text
    assert "reason：403" in text
    assert "extra" not in text, "empty values must not render"


# ── emit ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_emit_writes_outbox_only_not_the_bell(monkeypatch):
    """Emit writes the outbox row and nothing else; the bell is a channel now.

    The notification center is opt-in per event (``notify_center`` channel), so
    ``emit_notification`` must NOT touch ``notifications`` — otherwise every
    event would land in the center regardless of configuration.
    """
    conn = _install(monkeypatch, _FakeConn())

    oid = await emit_notification(
        "account.frozen",
        params={"provider_name": "p", "account_username": "u", "reason": "403"},
        owner_type="platform",
    )
    assert oid is not None
    outbox_inserts = [s for s, _ in conn.executes if "INSERT INTO mc_notify_outbox" in s]
    assert len(outbox_inserts) == 1, "emit must write exactly one outbox row"
    assert not any("notifications" in s for s, _ in conn.executes), "no unconditional bell row"
    assert "envelope" in outbox_inserts[0], "bell fields are carried in the envelope"
    # Gate not set → no queue → nothing enqueued.
    assert notify_core._queue is None
    assert notify_core._initialized.is_set() is False


@pytest.mark.asyncio
async def test_emit_enqueues_after_start(monkeypatch):
    """After start() flips _initialized, emit enqueues the outbox id."""
    conn = _install(monkeypatch, _FakeConn())
    conn.fetch_returns = [[]]  # _drain_pending finds nothing
    await notify_core.start()
    assert notify_core._initialized.is_set() is True

    consumed: list[str] = []

    async def fake_consume(outbox_id):
        consumed.append(outbox_id)

    monkeypatch.setattr(notify_core, "_consume_one", fake_consume)
    await emit_notification("channel.created", params={"provider_name": "p"}, owner_type="platform")
    await asyncio.sleep(0.05)
    assert len(consumed) == 1, "post-start emit must reach the consumer"


# ── worker consume ─────────────────────────────────────────────────────────


def _event_row(
    *,
    event_id: Any = None,
    channel_id: Any = None,
    url: str = "https://x.test/h",
    event_params: dict | None = None,
    trigger_condition: dict | None = None,
    effective_from: Any = None,
    effective_to: Any = None,
    daily_start: Any = None,
    daily_end: Any = None,
) -> dict:
    """One row of the (event ⋈ event_channels ⋈ channels) join.

    Mirrors the SELECT in ``_match_channels``: event-level config (window,
    params, trigger condition) plus the bound channel's delivery fields.
    """
    return {
        "event_id": event_id or uuid.uuid4(),
        "effective_from": effective_from,
        "effective_to": effective_to,
        "daily_start": daily_start,
        "daily_end": daily_end,
        "trigger_condition": trigger_condition or {},
        "event_params": event_params or {},
        "id": channel_id or uuid.uuid4(),
        "kind": "webhook",
        "webhook_url": url,
        "secret": "",
        "headers": None,
    }


def _outbox_row(outbox_id: Any, event_type: str = "account.frozen", params: dict | None = None) -> dict:
    return {
        "id": outbox_id,
        "event_type": event_type,
        "params": params if params is not None else {"provider_name": "p"},
        "envelope": {},
        "owner_type": "platform",
        "owner_id": None,
    }


def _capture_post(monkeypatch) -> list[str]:
    sent: list[str] = []

    async def fake_post(url, body, headers, secret, sign_body):
        sent.append(url)

    monkeypatch.setattr(notify_core, "_post", fake_post)
    return sent


@pytest.mark.asyncio
async def test_worker_matches_event_and_pushes(monkeypatch):
    """An active event bound to a channel → one _post, send_log, outbox finalized."""
    conn = _install(monkeypatch, _FakeConn())
    channel_id, outbox_id = uuid.uuid4(), uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id)]
    conn.fetch_returns = [[_event_row(channel_id=channel_id)]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == ["https://x.test/h"]
    assert any("INSERT INTO mc_notify_send_logs" in s for s, _ in conn.executes), "send log written"
    assert any(
        "UPDATE mc_notify_outbox SET status=$1" in s for s, _ in conn.executes
    ), "outbox finalized"


@pytest.mark.asyncio
async def test_selection_reads_event_tables_not_the_legacy_rule_table(monkeypatch):
    """Guard the source of truth: selection must come from the event entity.

    The legacy ``mc_notify_subscription_rules`` table is kept for already
    deployed rows but must not drive delivery any more, otherwise an event's
    status/window/trigger config would be silently bypassed.
    """
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id)]
    conn.fetch_returns = [[]]
    _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))

    selection = next(s for s, _ in conn.fetches if "mc_notify_events" in s)
    assert "mc_notify_event_channels" in selection, "bindings resolve the channels"
    assert "e.status = 'active'" in selection, "disabled events must not deliver"
    assert "mc_notify_subscription_rules" not in selection


@pytest.mark.asyncio
async def test_notify_center_channel_writes_the_bell_row(monkeypatch):
    """A bound ``notify_center`` channel is what puts the event in the center."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    envelope = {"title": "账号已冻结", "message": "m", "severity": "error"}
    row = _outbox_row(outbox_id)
    row["envelope"] = envelope
    conn.fetchrow_returns = [row]
    center = _event_row()
    center["kind"] = notify_core.NOTIFY_CENTER_KIND
    center["webhook_url"] = ""  # no endpoint: delivery is a DB write
    conn.fetch_returns = [[center]]
    sent = _capture_post(monkeypatch)

    written: list[dict] = []

    async def fake_upsert(data):
        written.append(data)
        return {"id": 7}

    import db

    monkeypatch.setattr(db.PostgresClient, "upsert_notification", fake_upsert)
    await notify_core._consume_one(str(outbox_id))

    assert sent == [], "notify_center must not POST anywhere"
    assert len(written) == 1, "the bell row is written by the channel, not by emit"
    assert written[0]["title"] == "账号已冻结"
    assert written[0]["severity"] == "error"
    assert written[0]["event_type"] == "account.frozen"
    logs = [p for s, p in conn.executes if "INSERT INTO mc_notify_send_logs" in s]
    assert logs and logs[0][5] == "success", "delivery is logged like any channel"


@pytest.mark.asyncio
async def test_no_notify_center_binding_keeps_the_center_empty(monkeypatch):
    """Without a notify_center binding, a webhook-only event writes no bell row."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id)]
    conn.fetch_returns = [[_event_row()]]  # webhook kind only
    _capture_post(monkeypatch)

    written: list[dict] = []

    async def fake_upsert(data):
        written.append(data)
        return {"id": 1}

    import db

    monkeypatch.setattr(db.PostgresClient, "upsert_notification", fake_upsert)
    await notify_core._consume_one(str(outbox_id))

    assert written == [], "the center is opt-in; an unbound event must not appear"


@pytest.mark.asyncio
async def test_cas_prevents_double_push(monkeypatch):
    """Enqueuing the same id twice pushes once: the second CAS returns no row."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    # First CAS hits; second CAS (same id, already 'pushing') returns None.
    conn.fetchrow_returns = [_outbox_row(outbox_id, event_type="x", params={}), None]
    conn.fetch_returns = [[_event_row()], []]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    await notify_core._consume_one(str(outbox_id))
    assert len(sent) == 1, "second CAS must not push again"


@pytest.mark.asyncio
async def test_event_params_scope_push_to_matching_provider(monkeypatch):
    """An event configured for provider 'a' does not fire for provider 'b'."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id, params={"provider_name": "b"})]
    conn.fetch_returns = [[_event_row(event_params={"provider_names": ["a"]})]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "event scoped to another provider must not push"


# ── effective window ───────────────────────────────────────────────────────


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 24, hour, minute, tzinfo=timezone.utc)


def test_date_range_bounds_are_inclusive_of_the_middle_only():
    row = {
        "effective_from": _at(0), "effective_to": _at(23, 59),
        "daily_start": None, "daily_end": None,
    }
    assert notify_core._event_time_active(row, _at(12)) is True
    assert notify_core._event_time_active(row, _at(12) - timedelta(days=1)) is False
    assert notify_core._event_time_active(row, _at(12) + timedelta(days=1)) is False


def test_absent_window_always_active():
    row = {"effective_from": None, "effective_to": None, "daily_start": None, "daily_end": None}
    assert notify_core._event_time_active(row, _at(3)) is True


def test_daily_window_gates_by_time_of_day():
    row = {
        "effective_from": None, "effective_to": None,
        "daily_start": time(9, 0), "daily_end": time(18, 0),
    }
    assert notify_core._event_time_active(row, _at(12)) is True
    assert notify_core._event_time_active(row, _at(8, 59)) is False
    assert notify_core._event_time_active(row, _at(18, 1)) is False


def test_daily_window_crossing_midnight_covers_both_sides():
    """A 22:00–06:00 window is the night shift, not an empty set."""
    row = {
        "effective_from": None, "effective_to": None,
        "daily_start": time(22, 0), "daily_end": time(6, 0),
    }
    assert notify_core._event_time_active(row, _at(23)) is True
    assert notify_core._event_time_active(row, _at(2)) is True
    assert notify_core._event_time_active(row, _at(12)) is False


@pytest.mark.asyncio
async def test_event_outside_its_window_does_not_push(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id)]
    conn.fetch_returns = [[
        _event_row(effective_from=_at(0) - timedelta(days=30), effective_to=_at(0) - timedelta(days=1))
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "an expired event must not deliver"


# ── trigger conditions ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_severity_condition_filters_below_minimum(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [
        _outbox_row(outbox_id, params={"provider_name": "p", "severity": "info"}),
        None,  # no prior trigger state
    ]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "severity", "min": "error"})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "info is below the configured minimum of error"


@pytest.mark.asyncio
async def test_severity_condition_passes_at_or_above_minimum(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [
        _outbox_row(outbox_id, params={"provider_name": "p", "severity": "critical"}),
        None,
    ]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "severity", "min": "error"})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == ["https://x.test/h"]


@pytest.mark.asyncio
async def test_threshold_condition_compares_a_numeric_field(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id, params={"used_percent": 42}), None]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "threshold", "field": "used_percent", "op": ">=", "value": 80})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "42 does not clear a >=80 threshold"


@pytest.mark.asyncio
async def test_threshold_condition_missing_field_does_not_fire(monkeypatch):
    """A condition on a field the event never carries must fail closed."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id, params={"provider_name": "p"}), None]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "threshold", "field": "used_percent", "op": ">=", "value": 1})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == []


@pytest.mark.asyncio
async def test_silence_window_suppresses_a_recent_repeat(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    recent = datetime.now(timezone.utc) - timedelta(seconds=30)
    conn.fetchrow_returns = [
        _outbox_row(outbox_id),
        {"id": uuid.uuid4(), "window_started_at": None, "count": 0, "last_triggered_at": recent},
    ]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "silence", "window_seconds": 300})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "a repeat inside the silence window is suppressed"


@pytest.mark.asyncio
async def test_silence_window_allows_once_it_has_elapsed(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    stale = datetime.now(timezone.utc) - timedelta(seconds=600)
    conn.fetchrow_returns = [
        _outbox_row(outbox_id),
        {"id": uuid.uuid4(), "window_started_at": None, "count": 0, "last_triggered_at": stale},
    ]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "silence", "window_seconds": 300})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == ["https://x.test/h"]


@pytest.mark.asyncio
async def test_count_condition_holds_until_the_nth_occurrence(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id), None]  # first ever occurrence
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "count", "times": 3, "window_seconds": 600})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "1st of 3 must not deliver yet"
    states = [(s, p) for s, p in conn.executes if "mc_notify_event_states" in s]
    assert len(states) == 1, "the pending count is persisted once"
    assert states[0][1][4] == 1, "counter advanced to exactly 1"


@pytest.mark.asyncio
async def test_count_condition_fires_on_the_nth_and_resets(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    conn.fetchrow_returns = [
        _outbox_row(outbox_id),
        {"id": uuid.uuid4(), "window_started_at": now - timedelta(seconds=10), "count": 2, "last_triggered_at": None},
    ]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"type": "count", "times": 3, "window_seconds": 600})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == ["https://x.test/h"], "3rd occurrence delivers"
    states = [(s, p) for s, p in conn.executes if "mc_notify_event_states" in s]
    assert states[-1][1][4] == 0, "counter resets after firing"


@pytest.mark.asyncio
async def test_count_advances_once_per_occurrence_not_once_per_bound_channel(monkeypatch):
    """The counter belongs to the event, not to each (event, channel) pair.

    The selection query returns one row per bound channel. Evaluating the
    trigger per row would advance a "3 consecutive times" counter three times
    for an event bound to three channels, firing it on the first occurrence.
    """
    conn = _install(monkeypatch, _FakeConn())
    outbox_id, event_id = uuid.uuid4(), uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id), None]
    condition = {"type": "count", "times": 3, "window_seconds": 600}
    conn.fetch_returns = [[
        _event_row(event_id=event_id, trigger_condition=condition, url="https://a.test/h"),
        _event_row(event_id=event_id, trigger_condition=condition, url="https://b.test/h"),
        _event_row(event_id=event_id, trigger_condition=condition, url="https://c.test/h"),
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == [], "one occurrence must not satisfy a 3-times condition"
    states = [(s, p) for s, p in conn.executes if "mc_notify_event_states" in s]
    assert len(states) == 1, "trigger state evaluated once per event, not per channel"
    assert states[0][1][4] == 1


@pytest.mark.asyncio
async def test_conditions_are_conjunctive(monkeypatch):
    """A list of conditions is AND: a passing severity cannot rescue a failing threshold."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id = uuid.uuid4()
    conn.fetchrow_returns = [
        _outbox_row(outbox_id, params={"severity": "critical", "used_percent": 10}),
        None,
    ]
    conn.fetch_returns = [[
        _event_row(trigger_condition={"conditions": [
            {"type": "severity", "min": "warn"},
            {"type": "threshold", "field": "used_percent", "op": ">=", "value": 80},
        ]})
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == []


@pytest.mark.asyncio
async def test_one_event_bound_to_several_channels_pushes_each_once(monkeypatch):
    conn = _install(monkeypatch, _FakeConn())
    outbox_id, event_id = uuid.uuid4(), uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id)]
    conn.fetch_returns = [[
        _event_row(event_id=event_id, url="https://a.test/h"),
        _event_row(event_id=event_id, url="https://b.test/h"),
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sorted(sent) == ["https://a.test/h", "https://b.test/h"]


@pytest.mark.asyncio
async def test_a_channel_bound_by_two_events_is_not_double_pushed(monkeypatch):
    """One occurrence must not hit the same webhook twice."""
    conn = _install(monkeypatch, _FakeConn())
    outbox_id, shared_channel = uuid.uuid4(), uuid.uuid4()
    conn.fetchrow_returns = [_outbox_row(outbox_id), None, None]
    conn.fetch_returns = [[
        _event_row(channel_id=shared_channel),
        _event_row(channel_id=shared_channel),
    ]]
    sent = _capture_post(monkeypatch)

    await notify_core._consume_one(str(outbox_id))
    assert sent == ["https://x.test/h"]


# ── startup drain ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_start_drains_pending_and_pushes(monkeypatch):
    """start() re-scans pending outbox rows and feeds them to the consumer."""
    conn = _install(monkeypatch, _FakeConn())
    pending_id = uuid.uuid4()
    conn.fetch_returns = [[{"id": pending_id}]]  # _drain_pending finds one row
    consumed: list[str] = []

    async def fake_consume(outbox_id):
        consumed.append(outbox_id)

    monkeypatch.setattr(notify_core, "_consume_one", fake_consume)

    await notify_core.start()
    assert notify_core._initialized.is_set() is True
    await asyncio.sleep(0.05)
    assert consumed == [str(pending_id)], "startup scan must enqueue the pending row"
