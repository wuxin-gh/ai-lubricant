"""Conversation content lives in ClickHouse; PostgreSQL keeps state and order.

``mc_task_events`` used to hold the whole conversation in a ``payload`` jsonb
column, which forced a read-merge-update per runtime frame and left no record of
what the runtime actually reported. Content now goes to ClickHouse
``task_messages`` (append-only, one row per frame) and the PostgreSQL row keeps
only what PostgreSQL is needed for: the delivery state machine, the ordering
``seq``, and the ``logical_event_id`` that joins the two.

These tests pin the seam between the two stores — what gets written where, and
how a page is put back together — with both stores stubbed.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from types import SimpleNamespace

import pytest

from monkeycode_compat import task_message_store
from monkeycode_compat import task_service as task_service_module
from monkeycode_compat.task_service import TaskService

TASK_ID = uuid.uuid4()


class _FakeCH:
    """Stand-in for the borrowed ``ClickHousePayloadClient``.

    Only the two methods the task path calls are implemented. ``fail`` makes
    both raise, which is the degraded state the fallbacks exist for.
    """

    def __init__(self, rows: list[dict] | None = None, *, fail: bool = False):
        self.rows = rows or []
        self.fail = fail
        self.inserted: list[list] = []
        self.queries: list[tuple[str, dict]] = []

    async def insert_task_message_row(self, row: list) -> None:
        if self.fail:
            raise RuntimeError("clickhouse down")
        self.inserted.append(row)

    async def query(self, sql: str, params: dict | None = None) -> list[dict]:
        self.queries.append((sql, params or {}))
        if self.fail:
            raise RuntimeError("clickhouse down")
        return self.rows


def _ch_frame(logical_id: str, seq: int, item: dict, **envelope) -> dict:
    """One ``task_messages`` row as ``query`` returns it."""
    body = {"item": item, "agent_id": envelope.get("agent_id", ""), "runtime_seq": seq}
    body.update({k: v for k, v in envelope.items() if v})
    return {
        "logical_event_id": logical_id,
        "seq": seq,
        "item_type": item.get("type", ""),
        "item_json": json.dumps(body, ensure_ascii=False),
        "agent_id": envelope.get("agent_id", ""),
        "subagent_id": envelope.get("subagent_id", ""),
        "event_kind": envelope.get("event_kind", ""),
        "tool_name": envelope.get("tool_name", ""),
        "phase": envelope.get("phase", ""),
        "status": envelope.get("status", ""),
        "created_at": datetime(2026, 9, 4, 12, 0, 0),
    }


def _pg_row(
    seq: int,
    *,
    kind: str = "item_ref",
    event_type: str = "agent_message",
    logical_event_id: str | None = None,
    payload: dict | None = None,
    client_message_id: str | None = None,
    delivery_status: str | None = None,
):
    return SimpleNamespace(
        id=uuid.uuid4(),
        seq=seq,
        kind=kind,
        event_type=event_type,
        logical_event_id=logical_event_id,
        payload=payload,
        client_message_id=client_message_id,
        delivery_status=delivery_status,
        delivery_attempt=1,
        failure_reason=None,
        created_at=datetime(2026, 9, 4, 12, 0, 0),
    )


@pytest.fixture(autouse=True)
def _detach_client():
    """Every test attaches what it needs; none may leak into the next."""
    yield
    task_message_store.attach_client(None)


# ── build_row / merge_frames ────────────────────────────────────────────────


def test_build_row_matches_the_shared_column_order():
    """The gateway and node_server insert positionally into the same table, so a
    row assembled here must line up with the canonical column list."""
    from integrations.clickhouse import TASK_MESSAGES_COLUMNS

    row = task_message_store.build_row(
        task_id=str(TASK_ID),
        logical_event_id="msg-1",
        seq=42,
        item_type="user_input",
        item={"id": "msg-1", "type": "user_input", "text": "hi"},
    )
    assert len(row) == len(TASK_MESSAGES_COLUMNS)
    by_name = dict(zip(TASK_MESSAGES_COLUMNS, row))
    assert by_name["task_id"] == str(TASK_ID)
    assert by_name["logical_event_id"] == "msg-1"
    assert by_name["seq"] == 42
    assert by_name["item_type"] == "user_input"
    # version = seq is what makes a re-insert of one frame collapse instead of
    # duplicating under ReplacingMergeTree.
    assert by_name["version"] == 42
    assert json.loads(by_name["item_json"])["item"]["text"] == "hi"


def test_merge_frames_folds_a_sparse_completion_onto_its_opening():
    """A tool call opens with title+input and closes with only output+status.

    Later frames win field by field; an absent or empty field never overwrites a
    populated one. Getting this backwards is what made a replayed transcript show
    an untitled card labelled with the raw protocol type.
    """
    merged = task_message_store.merge_frames([
        _ch_frame("t1", 10, {
            "id": "t1", "type": "tool_call",
            "title": "mcp__github__search_code", "input": {"q": "x"}, "status": "running",
        }, tool_name="mcp__github__search_code", phase="start", status="running"),
        _ch_frame("t1", 11, {
            "id": "t1", "type": "tool_call", "output": "found", "status": "done",
        }, status="done"),
    ])

    assert merged["item"]["title"] == "mcp__github__search_code"
    assert merged["item"]["input"] == {"q": "x"}
    assert merged["item"]["output"] == "found"
    assert merged["item"]["status"] == "done"
    # Envelope fields merge the same way: the opening frame's tool_name survives
    # a completion frame that omitted it.
    assert merged["tool_name"] == "mcp__github__search_code"
    assert merged["status"] == "done"


def test_merge_frames_ignores_unparseable_and_empty_input():
    assert task_message_store.merge_frames([]) is None
    assert task_message_store.merge_frames([{"item_json": "not json"}]) is None


# ── write path ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_user_input_content_goes_to_clickhouse_and_pg_keeps_state(monkeypatch):
    service = TaskService()
    ch = _FakeCH()
    task_message_store.attach_client(ch)
    created: dict = {}

    async def next_event_seq(_task_id):
        return 7

    async def create(**kw):
        created.update(kw)
        return SimpleNamespace(**kw)

    monkeypatch.setattr(service, "_next_event_seq", next_event_seq)
    monkeypatch.setattr(
        task_service_module.TaskEvent, "create", classmethod(lambda cls, **kw: create(**kw))
    )

    await service._persist_user_input_item(
        TASK_ID, "第一条消息", client_message_id="msg-1", delivery_status="pending",
    )

    # Content in ClickHouse, keyed by the client's own idempotency key.
    row = ch.inserted[0]
    assert row[1] == str(TASK_ID)
    assert row[2] == "msg-1"
    assert row[3] == 7
    assert json.loads(row[5])["item"]["text"] == "第一条消息"

    # PostgreSQL keeps state + index and no content at all.
    assert created["kind"] == "item_ref"
    assert created["payload"] is None
    assert created["logical_event_id"] == "msg-1"
    assert created["client_message_id"] == "msg-1"
    assert created["delivery_status"] == "pending"
    assert created["seq"] == 7


@pytest.mark.asyncio
async def test_user_input_falls_back_to_pg_payload_when_clickhouse_is_down(monkeypatch):
    """A send must not fail, and its content must not vanish, because the content
    store is unreachable. The row then looks exactly like a pre-split one, so the
    read path needs no special case for a degraded install."""
    service = TaskService()
    task_message_store.attach_client(_FakeCH(fail=True))
    created: dict = {}

    async def next_event_seq(_task_id):
        return 8

    monkeypatch.setattr(service, "_next_event_seq", next_event_seq)
    monkeypatch.setattr(
        task_service_module.TaskEvent,
        "create",
        classmethod(lambda cls, **kw: _async(created.update(kw) or SimpleNamespace(**kw))),
    )

    await service._persist_user_input_item(
        TASK_ID, "hello", client_message_id="msg-2", delivery_status="pending",
    )

    assert created["kind"] == "item"
    assert created["payload"] == {
        "item": {"id": "msg-2", "type": "user_input", "text": "hello"},
        "agent_id": "",
    }
    # The join key is written either way, so a later backfill can find the row.
    assert created["logical_event_id"] == "msg-2"


@pytest.mark.asyncio
async def test_user_input_without_clickhouse_configured_stays_in_pg(monkeypatch):
    """No client attached at all (ClickHouse not deployed) is the same path as a
    failed write — ``insert_row`` returns False rather than raising."""
    service = TaskService()
    created: dict = {}

    async def next_event_seq(_task_id):
        return 3

    monkeypatch.setattr(service, "_next_event_seq", next_event_seq)
    monkeypatch.setattr(
        task_service_module.TaskEvent,
        "create",
        classmethod(lambda cls, **kw: _async(created.update(kw) or SimpleNamespace(**kw))),
    )

    await service._persist_user_input_item(TASK_ID, "hi", client_message_id="msg-3")

    assert created["kind"] == "item"
    assert created["payload"]["item"]["text"] == "hi"


def _async(value):
    async def _run():
        return value

    return _run()


# ── read path: page assembly ───────────────────────────────────────────────


def _install_page(monkeypatch, service, rows: list, ch: _FakeCH | None):
    """Point ``list_task_events`` at a fixed page of PostgreSQL index rows."""
    if ch is not None:
        task_message_store.attach_client(ch)

    async def owned(_user_id, _task_id, role=None):
        return SimpleNamespace(id=TASK_ID)

    monkeypatch.setattr(service, "_owned_task", owned)

    class _Query:
        def __init__(self, rows):
            self._rows = rows

        def filter(self, **_kw):
            return self

        def order_by(self, *_a):
            return self

        def limit(self, n):
            return _async(self._rows[:n])

    monkeypatch.setattr(
        task_service_module.TaskEvent, "filter", classmethod(lambda cls, **kw: _Query(rows))
    )


@pytest.mark.asyncio
async def test_page_joins_clickhouse_content_onto_index_rows(monkeypatch):
    service = TaskService()
    ch = _FakeCH(rows=[
        _ch_frame("t1", 10, {"id": "t1", "type": "tool_call", "title": "Read", "status": "running"}),
        _ch_frame("t1", 11, {"id": "t1", "type": "tool_call", "output": "ok", "status": "done"}),
    ])
    # Newest-first, as the query returns it; the method reverses for render.
    _install_page(monkeypatch, service, [_pg_row(2, logical_event_id="t1", event_type="tool_call")], ch)

    out = await service.list_task_events("u", str(TASK_ID))

    assert len(out["rows"]) == 1
    item = out["rows"][0]["payload"]["item"]
    assert item["title"] == "Read"  # from the opening frame
    assert item["output"] == "ok"   # from the closing frame
    # The content query is scoped to the page's ids, not the whole task.
    _, params = ch.queries[0]
    assert params["ids"] == ["t1"]
    assert params["task_id"] == str(TASK_ID)


@pytest.mark.asyncio
async def test_repeated_index_rows_for_one_item_collapse_to_one_entry(monkeypatch):
    """Append-only means one index row per *frame*, so a tool call reported three
    times has three rows pointing at the same logical item. Emitting them all
    would repeat the bubble once per frame; the earliest row wins so the item
    stays where it started in the conversation."""
    service = TaskService()
    ch = _FakeCH(rows=[
        _ch_frame("t1", 10, {"id": "t1", "type": "tool_call", "title": "Read", "status": "running"}),
        _ch_frame("t1", 12, {"id": "t1", "type": "tool_call", "output": "ok", "status": "done"}),
    ])
    _install_page(monkeypatch, service, [
        _pg_row(5, logical_event_id="t1", event_type="tool_call"),
        _pg_row(4, logical_event_id="t1", event_type="tool_call"),
        _pg_row(3, logical_event_id="t1", event_type="tool_call"),
    ], ch)

    out = await service.list_task_events("u", str(TASK_ID))

    assert len(out["rows"]) == 1
    assert out["rows"][0]["seq"] == 3  # the item's first appearance
    assert out["rows"][0]["payload"]["item"]["output"] == "ok"


@pytest.mark.asyncio
async def test_transport_rows_are_kept_and_never_looked_up_in_clickhouse(monkeypatch):
    """Stage/error/message rows exist only in PostgreSQL and carry their payload
    inline. Paging the content store instead of the index would have dropped them
    from history entirely."""
    service = TaskService()
    ch = _FakeCH()
    _install_page(monkeypatch, service, [
        _pg_row(9, kind="error", event_type="", payload={"message": "provider timeout"}),
        _pg_row(8, kind="stage", event_type="git_clone", payload={"ok": True}),
    ], ch)

    out = await service.list_task_events("u", str(TASK_ID))

    assert [r["kind"] for r in out["rows"]] == ["stage", "error"]
    assert out["rows"][1]["payload"] == {"message": "provider timeout"}
    assert ch.queries == []  # nothing to join


@pytest.mark.asyncio
async def test_inline_payload_rows_are_not_refetched(monkeypatch):
    """A non-NULL payload means the content is already here — written before the
    split, or while ClickHouse was down. Re-fetching it would be wasteful and
    would overwrite the only copy that exists."""
    service = TaskService()
    ch = _FakeCH()
    _install_page(monkeypatch, service, [
        _pg_row(
            4, kind="item", event_type="user_input", logical_event_id="msg-1",
            payload={"item": {"id": "msg-1", "type": "user_input", "text": "legacy"}, "agent_id": ""},
            client_message_id="msg-1", delivery_status="completed",
        ),
    ], ch)

    out = await service.list_task_events("u", str(TASK_ID))

    assert out["rows"][0]["payload"]["item"]["text"] == "legacy"
    assert out["rows"][0]["delivery_status"] == "completed"
    assert ch.queries == []


@pytest.mark.asyncio
async def test_missing_content_renders_as_unavailable_not_as_a_gap(monkeypatch):
    """When ClickHouse is unreachable the turn is marked, not dropped.

    An empty gap in a transcript reads as data loss and sends the user looking
    for a bug; a marked one reads as a degraded store. The delivery state next to
    it is still accurate, because that never left PostgreSQL.
    """
    service = TaskService()
    _install_page(monkeypatch, service, [
        _pg_row(
            6, event_type="user_input", logical_event_id="msg-9",
            client_message_id="msg-9", delivery_status="running",
        ),
    ], _FakeCH(fail=True))

    out = await service.list_task_events("u", str(TASK_ID))

    row = out["rows"][0]
    assert row["payload"]["item"]["content_unavailable"] is True
    assert row["payload"]["item"]["type"] == "user_input"
    assert row["delivery_status"] == "running"


@pytest.mark.asyncio
async def test_cursor_counts_index_rows_not_collapsed_entries(monkeypatch):
    """The cursor must advance over what the page consumed, not what it emitted.

    Three index rows collapsing to one entry still consumed three rows of the
    index; a cursor derived from the emitted list would hand back the same page
    forever.
    """
    service = TaskService()
    ch = _FakeCH(rows=[_ch_frame("t1", 10, {"id": "t1", "type": "tool_call"})])
    _install_page(monkeypatch, service, [
        _pg_row(7, logical_event_id="t1", event_type="tool_call"),
        _pg_row(6, logical_event_id="t1", event_type="tool_call"),
    ], ch)

    out = await service.list_task_events("u", str(TASK_ID), limit=2)

    assert len(out["rows"]) == 1
    assert out["next_before"] == 6  # oldest raw row on the page


@pytest.mark.asyncio
async def test_short_page_ends_pagination(monkeypatch):
    service = TaskService()
    _install_page(monkeypatch, service, [_pg_row(2, kind="stage", payload={"ok": True})], _FakeCH())

    out = await service.list_task_events("u", str(TASK_ID), limit=50)

    assert out["next_before"] is None

