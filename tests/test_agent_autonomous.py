import time
from pathlib import Path

import pytest

from agent import autonomous_worker as aw
from agent.autonomous_worker import AutonomousWorker, parse_todo_line


def test_parse_todo_line_priority_and_plain():
    assert parse_todo_line("[P1] review logs") is not None
    item = parse_todo_line("[P1] review logs")
    assert item.priority == 1
    assert item.text == "review logs"
    plain = parse_todo_line("just do it")
    assert plain.priority == 99
    assert plain.text == "just do it"
    assert parse_todo_line("# comment") is None
    assert parse_todo_line("") is None


@pytest.mark.asyncio
async def test_autonomous_worker_no_queue_returns_none(monkeypatch, tmp_path):
    root = tmp_path / "agents" / "1"
    monkeypatch.setattr(aw, "agent_root", lambda aid: root)
    worker = AutonomousWorker(1, lambda p: _noop(p), idle_interval=1)
    assert await worker.claim_if_idle() is None


@pytest.mark.asyncio
async def test_autonomous_worker_not_idle_does_not_claim(monkeypatch, tmp_path):
    root = tmp_path / "agents" / "1"
    monkeypatch.setattr(aw, "agent_root", lambda aid: root)
    todo = root / "temp" / "TODO.txt"
    todo.parent.mkdir(parents=True, exist_ok=True)
    todo.write_text("[P1] clean logs\n", encoding="utf-8")
    dispatched: list[str] = []

    async def dispatch(prompt: str) -> None:
        dispatched.append(prompt)

    worker = AutonomousWorker(1, dispatch, idle_interval=3600)
    # Recent activity → not idle.
    monkeypatch.setattr(worker, "_last_activity_fn", _recent_activity)
    assert await worker.claim_if_idle() is None
    assert dispatched == []
    # Queue untouched.
    assert "[P1] clean logs" in todo.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_autonomous_worker_claims_when_idle_and_removes_item(monkeypatch, tmp_path):
    root = tmp_path / "agents" / "1"
    monkeypatch.setattr(aw, "agent_root", lambda aid: root)
    todo = root / "temp" / "TODO.txt"
    todo.parent.mkdir(parents=True, exist_ok=True)
    todo.write_text("[P2] second\n[P1] first\n", encoding="utf-8")
    dispatched: list[str] = []

    async def dispatch(prompt: str) -> None:
        dispatched.append(prompt)

    worker = AutonomousWorker(1, dispatch, idle_interval=1)
    monkeypatch.setattr(worker, "_last_activity_fn", _old_activity)
    claimed = await worker.claim_if_idle()
    assert claimed == "first"
    remaining = todo.read_text(encoding="utf-8")
    assert "first" not in remaining
    assert "second" in remaining


@pytest.mark.asyncio
async def test_autonomous_worker_requeues_on_dispatch_failure(monkeypatch, tmp_path):
    root = tmp_path / "agents" / "1"
    monkeypatch.setattr(aw, "agent_root", lambda aid: root)
    todo = root / "temp" / "TODO.txt"
    todo.parent.mkdir(parents=True, exist_ok=True)
    todo.write_text("[P1] risky task\n", encoding="utf-8")

    async def dispatch(prompt: str) -> None:
        raise RuntimeError("boom")

    worker = AutonomousWorker(1, dispatch, idle_interval=1)
    monkeypatch.setattr(worker, "_last_activity_fn", _old_activity)
    # tick catches the dispatch error and requeues with a note.
    result = await worker.tick()
    assert result == "risky task"
    body = todo.read_text(encoding="utf-8")
    assert "risky task" in body
    assert "failed" in body


async def _noop(_prompt: str) -> None:
    return None


async def _recent_activity(_agent_id: int) -> float:
    return time.time()  # just now → not idle


async def _old_activity(_agent_id: int) -> float:
    return time.time() - 7200  # 2h ago → idle