from pathlib import Path

import pytest

import agent.api as agent_api


def test_send_message_request_accepts_execution_modes():
    interact = agent_api.SendMessageRequest(content="hi")
    assert interact.mode == "interact"
    assert interact.goal_config is None

    goal = agent_api.SendMessageRequest(
        content="work",
        mode="goal",
        goal_config={"objective": "improve docs", "budget_seconds": 600, "max_turns": 8},
    )
    assert goal.mode == "goal"
    assert goal.goal_config.objective == "improve docs"
    assert goal.goal_config.budget_seconds == 600

    with pytest.raises(Exception):
        agent_api.SendMessageRequest(content="x", mode="invalid")


def test_attachment_default_uses_real_agent_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Legacy/empty DB workspace resolves to data/agents/<id>, not shared agent/workspace."""
    from agent import file_memory as fm

    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "agents")
    relative, target = agent_api._attachment_target(str(fm.agent_root(7)), "conv-1", "note.txt")

    assert relative.startswith(".chat-attachments/conv-1/")
    assert target.is_relative_to((tmp_path / "agents" / "7").resolve())
