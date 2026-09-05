"""The two physical copies of the ``task_messages`` schema must stay identical.

``task_messages`` is written by two processes that deliberately share no code:
the gateway/data service (``integrations.clickhouse``) and the node control plane
(``node_server.task_message_store``, which cannot import the data service — see
``node_server/shared_store.py``). Each therefore carries its own copy of the DDL
and the row's column order.

That is the same arrangement as the delivery-status transition table, and it
fails the same way: one side gets edited, the other keeps inserting rows in the
old column order, and the mismatch shows up as silently shifted content — a
``tool_name`` landing in ``phase`` — not as an error. This test is the seam that
makes that a CI failure instead.
"""
from __future__ import annotations

import re

from integrations.clickhouse import TASK_MESSAGES_COLUMNS, TASK_MESSAGES_DDL
from node_server.task_message_store import COLUMN_NAMES as NODE_COLUMNS
from node_server.task_message_store import _SCHEMA_DDL as NODE_DDL


def _normalize(ddl: str) -> str:
    """Collapse whitespace so indentation differences are not failures."""
    return re.sub(r"\s+", " ", ddl).strip()


def test_column_order_is_identical():
    # Order, not just membership: clickhouse-connect inserts positionally, so a
    # reordering silently writes each value into the neighbouring column.
    assert TASK_MESSAGES_COLUMNS == NODE_COLUMNS


def test_ddl_is_identical():
    assert _normalize(TASK_MESSAGES_DDL) == _normalize(NODE_DDL)


def test_ddl_declares_every_column_in_the_insert_list():
    """A column in the insert list that the DDL never creates fails at runtime
    only once real traffic hits it."""
    body = _normalize(TASK_MESSAGES_DDL)
    for column in TASK_MESSAGES_COLUMNS:
        assert re.search(rf"[ (]{re.escape(column)} ", body), column


def test_engine_and_sort_key_are_what_the_read_path_assumes():
    """``query_task_messages`` pages on ``seq DESC`` and reads ``FINAL``; both
    only behave as intended with this engine and sort key."""
    body = _normalize(TASK_MESSAGES_DDL)
    assert "ENGINE = ReplacingMergeTree(version)" in body
    assert "ORDER BY (task_id, seq)" in body
