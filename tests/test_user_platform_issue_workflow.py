"""Issue requirement/bug state machine + normalizer safety.

These cover the pure logic added for the requirement/bug tracker without needing
a database: the two independent transition flows, rejection of illegal moves,
recovery of unknown legacy states, issue-type normalization, and pending-item
coercion. The DB-backed create/update paths are exercised by the service tests
that run against a live compat connection.
"""
from __future__ import annotations

import pytest

from user_platform.project_service import (
    ISSUE_TRANSITIONS,
    IssueTransitionError,
    _ASSIGN_PLANS,
    _initial_status,
    _normalize_issue_type,
    _normalize_pending_items,
    _validate_transition,
)


# ---- issue type normalization --------------------------------------------
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("requirement", "requirement"),
        ("bug", "bug"),
        ("BUG", "bug"),
        ("  bug  ", "bug"),
        ("", "requirement"),
        (None, "requirement"),
        ("nonsense", "requirement"),
    ],
)
def test_normalize_issue_type(raw, expected):
    assert _normalize_issue_type(raw) == expected


def test_initial_status_is_always_unassigned():
    assert _initial_status("requirement") == "unassigned"
    assert _initial_status("bug") == "unassigned"


def test_assign_task_intent_mappings():
    assert _ASSIGN_PLANS["requirement"]["analysis"] == {
        "task_role": "design",
        "task_type": "design",
        "sub_type": "generate_design",
        "status": "designing",
    }
    assert _ASSIGN_PLANS["requirement"]["fix"]["task_role"] == "develop"
    assert _ASSIGN_PLANS["requirement"]["fix"]["sub_type"] == "execute_task"
    assert _ASSIGN_PLANS["bug"]["analysis"]["task_role"] == "diagnose"
    assert _ASSIGN_PLANS["bug"]["analysis"]["sub_type"] == "diagnose_bug"
    assert _ASSIGN_PLANS["bug"]["fix"]["task_role"] == "fix"
    assert _ASSIGN_PLANS["bug"]["fix"]["sub_type"] == "fix_bug"


def test_unassigned_issue_can_start_analysis_or_fix():
    _validate_transition("requirement", "unassigned", "designing")
    _validate_transition("requirement", "unassigned", "developing")
    _validate_transition("bug", "unassigned", "diagnosing")
    _validate_transition("bug", "unassigned", "fixing")


# ---- requirement flow -----------------------------------------------------
def test_requirement_happy_path_transitions():
    flow = [
        "unassigned",
        "designing",
        "design_pending_confirmation",
        "design_confirmed",
        "developing",
        "completed",
        "closed",
    ]
    for current, target in zip(flow, flow[1:]):
        _validate_transition("requirement", current, target)  # no raise


def test_requirement_rejects_skipping_states():
    with pytest.raises(IssueTransitionError):
        _validate_transition("requirement", "unassigned", "completed")
    with pytest.raises(IssueTransitionError):
        _validate_transition("requirement", "designing", "developing")


def test_requirement_can_be_sent_back_for_redesign():
    # pending_confirmation may bounce back to designing (rework loop)
    _validate_transition(
        "requirement", "design_pending_confirmation", "designing"
    )


# ---- bug flow -------------------------------------------------------------
def test_bug_happy_path_transitions():
    flow = [
        "unassigned",
        "diagnosing",
        "reason_pending_confirmation",
        "reason_confirmed",
        "fixing",
        "fixed",
        "closed",
    ]
    for current, target in zip(flow, flow[1:]):
        _validate_transition("bug", current, target)  # no raise


def test_bug_and_requirement_flows_are_disjoint():
    # a bug state is not reachable in the requirement flow and vice versa
    with pytest.raises(IssueTransitionError):
        _validate_transition("requirement", "designing", "fixing")
    with pytest.raises(IssueTransitionError):
        _validate_transition("bug", "diagnosing", "developing")


def test_any_unfinished_state_can_close():
    for issue_type, states in ISSUE_TRANSITIONS.items():
        for state, allowed in states.items():
            if state == "closed":
                assert allowed == set()
            else:
                assert "closed" in allowed, f"{issue_type}:{state} cannot close"


def test_same_state_is_a_noop():
    _validate_transition("requirement", "designing", "designing")  # no raise


# ---- legacy-state recovery ------------------------------------------------
def test_unknown_current_state_recovers_to_known_target():
    # a row written before the state machine (e.g. "completed" as a legacy
    # free value on a bug) can still be moved into a declared state
    _validate_transition("bug", "legacy_value", "diagnosing")  # no raise


def test_unknown_current_state_rejects_unknown_target():
    with pytest.raises(IssueTransitionError):
        _validate_transition("requirement", "legacy_value", "also_bogus")


# ---- pending items --------------------------------------------------------
def test_pending_items_from_strings():
    items = _normalize_pending_items(["confirm migration", "  ", "check auth"])
    assert [i["content"] for i in items] == ["confirm migration", "check auth"]
    assert all(i["status"] == "pending" for i in items)
    assert all(i["id"] for i in items)


def test_pending_items_from_dicts_preserve_id_and_status():
    items = _normalize_pending_items(
        [
            {"id": "pi-1", "content": "resolved one", "status": "resolved"},
            {"content": "no id", "status": "weird"},
            {"content": "   "},  # dropped: empty content
        ]
    )
    assert items[0] == {"id": "pi-1", "content": "resolved one", "status": "resolved"}
    assert items[1]["status"] == "pending"  # unknown status coerced
    assert len(items) == 2


def test_pending_items_bad_payload_is_empty_not_error():
    assert _normalize_pending_items(None) == []
    assert _normalize_pending_items("not a list") == []
    assert _normalize_pending_items([123, None]) == []
