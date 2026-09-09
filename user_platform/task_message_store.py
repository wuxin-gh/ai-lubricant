"""Gateway-side access to the ClickHouse ``task_messages`` content store.

The task surface splits its storage by what the data *is*: PostgreSQL
``mc_task_events`` holds state and index (delivery status machine, ordering
``seq``, the ``logical_event_id`` join key), and ClickHouse ``task_messages``
holds the conversation content itself, appended one row per runtime frame. See
:class:`user_platform.models_task.TaskEvent` for why the split exists.

Two processes write that table, and they deliberately share no code: the node
control plane has its own facade (``node_server/task_message_store.py`` — it
cannot import the data service, see ``node_server/shared_store.py``), and this
module is the gateway's. This one does not own a client; it borrows the already
connected :class:`~integrations.clickhouse.ClickHousePayloadClient` that
``main.py`` builds for the agent conversation store, since the rows live in the
same database.

Every function degrades instead of raising: when ClickHouse is not configured
the gateway keeps writing the content into the PostgreSQL ``payload`` column,
which is exactly how rows written before the split read, so the read path needs
no special case for a degraded install.
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any

from loguru import logger

_client = None  # ClickHousePayloadClient, attached by main.py's lifespan


def attach_client(client) -> None:
    """Borrow the connected client ``main.py`` already built. ``None`` detaches."""
    global _client
    _client = client


def is_ready() -> bool:
    return _client is not None


def now_ch() -> str:
    """ClickHouse ``DateTime64(3)`` wants ``'YYYY-MM-DD HH:MM:SS.mmm'`` in UTC."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + f".{int((time.time() % 1) * 1000):03d}"


def build_row(
    *,
    task_id: str,
    logical_event_id: str,
    seq: int,
    item_type: str,
    item: dict,
    row_id: str = "",
    agent_id: str = "",
    subagent_id: str = "",
    event_kind: str = "",
    tool_name: str = "",
    phase: str = "",
    status: str = "",
    created_at: str = "",
) -> list:
    """Assemble one ``task_messages`` row in ``TASK_MESSAGES_COLUMNS`` order.

    ``item_json`` carries the canonical envelope — the item plus the routing
    fields the read path needs — byte-compatible with what node_server writes
    (``node_server.task_message_store.build_row``) and with the legacy
    PostgreSQL ``payload`` shape, so a page can mix rows from either writer and
    from either era without the reader branching on origin.
    """
    envelope: dict[str, Any] = {
        "item": item,
        "agent_id": agent_id or "",
        "runtime_seq": int(seq or 0),
    }
    if logical_event_id:
        envelope["logical_event_id"] = logical_event_id
    if event_kind:
        envelope["event_kind"] = event_kind
    if tool_name:
        envelope["tool_name"] = tool_name
    if subagent_id:
        envelope["subagent_id"] = subagent_id
    if phase:
        envelope["phase"] = phase
    if status:
        envelope["status"] = status
    return [
        str(row_id or uuid.uuid4()),
        str(task_id),
        str(logical_event_id or ""),
        int(seq or 0),
        str(item_type or "")[:64],
        json.dumps(envelope, ensure_ascii=False, default=str),
        str(agent_id or ""),
        str(subagent_id or ""),
        str(event_kind or ""),
        str(tool_name or ""),
        str(phase or ""),
        str(status or ""),
        str(created_at or now_ch()),
        int(seq or 0),  # version = seq; a re-insert of the same frame collapses
    ]


async def insert_row(row: list) -> bool:
    """Append one frame. ``False`` means "not stored" — caller keeps the PG payload.

    Never raises: a ClickHouse hiccup must not fail the user's send. The caller
    treats ``False`` as "content stays in PostgreSQL", which is a supported
    (degraded) state, not an error.
    """
    if _client is None:
        return False
    try:
        await _client.insert_task_message_row(row)
        return True
    except Exception:  # noqa: BLE001
        logger.warning("[task-messages] clickhouse insert failed; content stays in postgres")
        return False


async def fetch_by_logical_ids(task_id: str, logical_ids: list[str]) -> dict[str, list[dict]]:
    """Load every frame of the given logical items, grouped by ``logical_event_id``.

    The read path pages PostgreSQL (the ordered index) and then asks for the
    content of the ids on that page. Fetching *all* frames of an id — not just
    the ones whose ``seq`` fell inside the page — is what keeps a page boundary
    from splitting a tool call: the opening frame carries the title and input,
    the closing one the output, and a page that showed only the latter would
    render an untitled card. Returned frames are oldest-first per id, which is
    the order the field-level last-wins merge expects.
    """
    ids = [str(x) for x in dict.fromkeys(logical_ids) if x]
    if _client is None or not ids:
        return {}
    try:
        rows = await _client.query(
            "SELECT logical_event_id, seq, item_type, item_json, agent_id, subagent_id, "
            "event_kind, tool_name, phase, status, created_at "
            "FROM task_messages FINAL "
            "WHERE task_id = {task_id:String} "
            "AND logical_event_id IN {ids:Array(String)} "
            "ORDER BY seq ASC",
            {"task_id": str(task_id), "ids": ids},
        )
    except Exception:  # noqa: BLE001
        logger.warning("[task-messages] clickhouse read failed; falling back to postgres payload")
        return {}
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("logical_event_id") or ""), []).append(dict(row))
    return grouped


def merge_frames(frames: list[dict]) -> dict | None:
    """Fold one logical item's frames into the envelope the API returns.

    The runtime reports an item as it advances: a tool call opens with
    ``{id, type, title, input, status:'running'}`` and closes with a sparse
    ``{id, type, output, status:'done'}``. Later frames win field by field, and
    an absent or empty field never overwrites a populated one — the same
    last-wins rule the client applies in ``mergeNormalizedItem``, so live and
    replayed transcripts converge on the same item.

    This used to happen on the *write* side, which cost a read per frame and
    erased the trail. Doing it here keeps the stored frames honest about what
    the runtime actually said and still hands the client one item per call.
    """
    if not frames:
        return None
    merged_item: dict[str, Any] = {}
    envelope: dict[str, Any] = {}
    for frame in frames:
        try:
            parsed = json.loads(frame.get("item_json") or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(parsed, dict):
            continue
        for key, value in parsed.items():
            if key == "item":
                continue
            if value is None or (isinstance(value, str) and not value):
                continue
            envelope[key] = value
        item = parsed.get("item")
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            if value is None or (isinstance(value, str) and not value):
                continue
            merged_item[key] = value
    if not merged_item and not envelope:
        return None
    envelope["item"] = merged_item
    envelope.setdefault("agent_id", "")
    return envelope
