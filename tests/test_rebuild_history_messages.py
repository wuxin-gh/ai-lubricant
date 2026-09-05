"""History-rebuild normalizer + gateway malformed-tool_call visibility tests.

These cover the bug where agent conversation history was replayed with the stored
*display* shape ``{name, args, status}`` for ``tool_calls`` and a non-standard
sibling ``tool_results`` field, producing orphaned tool_calls (no paired
role=tool) that upstreams either 400 on (Anthropic/Responses/strict OpenAI) or
silently drop (tools-as-prompt channels → blank assistant turn → model
hallucinates the missing tool results).

The fix has two halves:
- ``agent.context_manager.rebuild_history_messages`` normalizes stored rows into
  OpenAI wire shape + paired role=tool, dropping incomplete calls;
- the write side persists the ``id`` so replay has a real correlation key;
- the gateway text-flattening paths now WARN (and ``combine_messages`` no longer
  raises AttributeError on a non-object entry) instead of degrading silently.
"""
import json

import pytest

from agent.context_manager import rebuild_history_messages, serialize_tool_result, attach_tool_result
from message_utils import combine_messages


# ── rebuild_history_messages ──────────────────────────────────────────────

def _assistant_row(msg_id, *, content="", calls=None, results=None):
    return {
        "id": msg_id,
        "role": "assistant",
        "content": content,
        "tool_calls": calls or [],
        "tool_results": results or [],
    }


def _user_row(msg_id, content):
    return {"id": msg_id, "role": "user", "content": content}


# ── attach_tool_result (live tool_result → collected tool_calls/results) ──
# The bug this guards: agent_loop emits ``index`` as *this turn's* nth call and
# resets it each turn, but collected_tool_calls accumulates across turns. Indexing
# by ``index`` mis-pairs a later turn's result onto an earlier turn's call (which
# was already done) and leaves the actually-running call stuck on ``running`` —
# the permanent spinner in history replay.

def _calls(*ids):
    return [{"id": i, "name": f"t{i}", "args": {}, "status": "running"} for i in ids]


def test_attach_tool_result_matches_by_id_across_turns():
    calls, results = _calls("A"), []
    attach_tool_result(calls, results, call_id="A", index=0, result="rA")
    # Turn 2: index resets to 0, but id B is new — must not touch A.
    calls.append({"id": "B", "name": "tB", "args": {}, "status": "running"})
    attach_tool_result(calls, results, call_id="B", index=0, result="rB")

    assert [c["status"] for c in calls] == ["done", "done"]
    assert results == ["rA", "rB"]  # parallel to calls by position


def test_attach_tool_result_falls_back_to_last_running_when_id_missing():
    calls = [{"id": "A", "status": "done"}, {"id": "B", "status": "running"}]
    results = ["rA"]
    attach_tool_result(calls, results, call_id=None, index=0, result="rB")
    # No id → last running call (B) is the target, not the done one (A).
    assert calls[1]["status"] == "done"
    assert results[1] == "rB"
    assert calls[0]["status"] == "done"  # untouched


def test_attach_tool_result_appends_when_no_call_matches():
    """A stray result with no matching call is kept for display, not dropped."""
    calls = [{"id": "A", "status": "done"}]
    results = ["rA"]
    attach_tool_result(calls, results, call_id="ghost", index=0, result="stray")
    assert calls[0]["status"] == "done"
    assert results == ["rA", "stray"]  # appended in order, no paired call


def test_rebuild_normalizes_display_shape_to_wire_and_pairs_role_tool():
    """A completed call with a result becomes wire tool_calls + a role=tool."""
    history = [
        _user_row(1, "list files"),
        _assistant_row(
            2,
            content="",
            calls=[{"id": "call_1", "name": "file_read", "args": {"path": "a.py"}, "status": "done"}],
            results=[{"content": "print('hi')"}],
        ),
    ]
    out = rebuild_history_messages(history, system_prompt="sys")
    assert out[0] == {"role": "system", "content": "sys"}
    assert out[1] == {"role": "user", "content": "list files"}
    assistant = out[2]
    assert assistant["role"] == "assistant"
    assert assistant["tool_calls"] == [{
        "id": "call_1",
        "type": "function",
        "function": {"name": "file_read", "arguments": json.dumps({"path": "a.py"}, ensure_ascii=False)},
    }]
    tool_msg = out[3]
    assert tool_msg == {
        "role": "tool",
        "tool_call_id": "call_1",
        "name": "file_read",
        "content": serialize_tool_result({"content": "print('hi')"}),
    }
    # The non-standard tool_results sibling field must NOT be sent upstream.
    assert "tool_results" not in assistant


def test_rebuild_skips_running_calls_without_a_result():
    """status=running (mid-batch persist after a crash) must not be replayed —
    an orphaned tool_call without a role=tool reply 400s on some upstreams."""
    history = [
        _user_row(1, "go"),
        _assistant_row(
            2,
            content="",
            calls=[
                {"id": "c1", "name": "file_read", "args": {}, "status": "done"},
                {"id": "c2", "name": "code_run", "args": {}, "status": "running"},
            ],
            results=[{"ok": True}],  # only one result; c2 never finished
        ),
    ]
    out = rebuild_history_messages(history)
    assistant = [m for m in out if m.get("role") == "assistant"][-1]
    assert [tc["id"] for tc in assistant["tool_calls"]] == ["c1"]
    # Only one role=tool, paired with the completed call.
    tool_msgs = [m for m in out if m.get("role") == "tool"]
    assert len(tool_msgs) == 1
    assert tool_msgs[0]["tool_call_id"] == "c1"


def test_rebuild_drops_call_with_no_result_even_if_done():
    history = [
        _user_row(1, "go"),
        _assistant_row(
            2,
            content="",
            calls=[{"id": "c1", "name": "file_read", "args": {}, "status": "done"}],
            results=[],  # no result recorded
        ),
    ]
    out = rebuild_history_messages(history)
    assistant = out[-1]
    assert "tool_calls" not in assistant
    assert [m for m in out if m.get("role") == "tool"] == []


def test_rebuild_drops_blank_assistant_turn_with_no_replayed_calls():
    """An assistant entry whose content is empty and whose calls were all
    dropped must not produce a blank upstream turn."""
    history = [
        _user_row(1, "go"),
        _assistant_row(2, content="", calls=[{"id": "c1", "name": "x", "args": {}, "status": "running"}], results=[]),
    ]
    out = rebuild_history_messages(history)
    assert all(m["role"] != "assistant" or m.get("content") or m.get("tool_calls") for m in out)


def test_rebuild_keeps_assistant_with_content_even_if_calls_dropped():
    history = [
        _user_row(1, "go"),
        _assistant_row(2, content="I will read the file", calls=[{"id": "c1", "name": "x", "args": {}, "status": "running"}], results=[]),
    ]
    out = rebuild_history_messages(history)
    assert out[-1] == {"role": "assistant", "content": "I will read the file"}


def test_rebuild_skip_msg_id_excludes_target_row():
    history = [
        _user_row(1, "go"),
        _assistant_row(2, content="mid", calls=[{"id": "c1", "name": "x", "args": {}, "status": "done"}], results=[{"r": 1}]),
    ]
    out = rebuild_history_messages(history, skip_msg_id=2)
    assert all(m.get("role") != "assistant" for m in out if m.get("content") == "mid")
    assert [m for m in out if m.get("role") == "tool"] == []


def test_rebuild_synthesizes_id_when_missing_so_pairing_stays_intact():
    """Legacy rows (pre-fix) have no id on stored tool_calls — replay must still
    produce a usable, stable id so role=tool correlates with tool_calls."""
    history = [
        _user_row(1, "go"),
        _assistant_row(
            2,
            content="",
            calls=[{"name": "file_read", "args": {}, "status": "done"}],  # no id, legacy
            results=[{"ok": True}],
        ),
    ]
    out = rebuild_history_messages(history)
    assistant = out[-2]
    tool_msg = out[-1]
    assert assistant["tool_calls"][0]["id"] == tool_msg["tool_call_id"]


def test_rebuild_preserves_non_assistant_messages_verbatim():
    history = [
        {"id": 1, "role": "system", "content": "preset"},
        _user_row(2, "hi"),
    ]
    out = rebuild_history_messages(history)
    # No system_prompt arg → only the rows; a stored system row passes through.
    assert out == [{"role": "system", "content": "preset"}, {"role": "user", "content": "hi"}]


# ── serialize_tool_result determinism ─────────────────────────────────────

def test_serialize_tool_result_string_passthrough_and_none_empty():
    assert serialize_tool_result("hello") == "hello"
    assert serialize_tool_result(None) == ""
    assert serialize_tool_result({"a": 1, "b": 2}) == json.dumps({"b": 2, "a": 1}, sort_keys=True, ensure_ascii=False)


# ── combine_messages robustness (gateway defense) ─────────────────────────

def test_combine_messages_does_not_raise_on_non_object_tool_call_entry():
    """A bare-string tool_calls entry used to raise AttributeError (not a
    ValueError) and escape the 400 mapping as a 500. It must now be skipped."""
    messages = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "", "tool_calls": ["not-an-object"]},  # type: ignore[list-item]
    ]
    # Must not raise.
    combine_messages(messages)


def test_combine_messages_renders_stub_for_display_shape_tool_call():
    """A display-shape entry (no function.name) renders an empty stub instead
    of crashing — the WARN is the visibility hook, not a raise."""
    messages = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "", "tool_calls": [{"name": "file_read", "args": {}, "status": "done"}]},
    ]
    out = combine_messages(messages)
    assert "<assistant_tool_calls>" in out
    assert "tool" in out  # name defaulted to "tool"
