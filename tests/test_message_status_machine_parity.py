"""Parity guard: gateway and node_server delivery-status transition tables must match.

``monkeycode_compat/message_status_machine.py`` is the authoritative table; the
gateway imports it directly. ``node_server/task_message_status.py`` keeps a
physical copy because node_server is a self-contained package that does not
import gateway code (see ``node_server/shared_store.py``). That separation
means the two can drift — and they already did (node_server lacked
``failed → pending`` and the whole ``cancelled`` key, and allowed
``pending → received/running`` the gateway rejects).

This test imports both copies and asserts they are identical, so a future
one-sided edit fails CI instead of shipping a silent state-machine drift.
"""
from __future__ import annotations

import os
import sys

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from monkeycode_compat.message_status_machine import (  # noqa: E402
    MESSAGE_STATUS_TRANSITIONS as GATEWAY_TRANSITIONS,
)
from node_server.task_message_status import _TRANSITIONS as NODE_TRANSITIONS  # noqa: E402


def _normalize(table: dict) -> dict[str, frozenset[str]]:
    """Compare as dict[str, frozenset] regardless of tuple/frozenset source."""
    return {str(k): frozenset(v) for k, v in table.items()}


def test_gateway_and_node_transition_tables_are_identical() -> None:
    gateway = _normalize(GATEWAY_TRANSITIONS)
    node = _normalize(NODE_TRANSITIONS)
    # Key drift (one side adds/removes a from-state) shows up here first with a
    # readable diff; a value-only drift is caught by the equality below.
    assert set(gateway) == set(node), (
        f"from-state keys diverge: gateway={sorted(gateway)} node={sorted(node)}"
    )
    assert gateway == node, (
        f"transitions diverge: gateway={gateway} node={node}"
    )
