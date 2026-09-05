from pathlib import Path

import pytest

from agent import plan_mode as pm


@pytest.fixture()
def mgr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> pm.PlanModeManager:
    monkeypatch.setattr(pm, "agent_workspace_root", lambda aid: tmp_path / "ws")
    return pm.PlanModeManager(1)


def test_create_writes_scaffold_and_is_idempotent(mgr: pm.PlanModeManager) -> None:
    first = mgr.create("conv-abc", "ship docs")
    assert first.plan_path.exists()
    text = first.plan_path.read_text(encoding="utf-8")
    assert "ship docs" in text
    assert "- Objective: ship docs" in text
    # Second create must not clobber the existing plan.
    second = mgr.create("conv-abc", "ship docs")
    assert second is first
    assert second.plan_path.read_text(encoding="utf-8") == text


def test_set_steps_renders_markers(mgr: pm.PlanModeManager) -> None:
    session = mgr.create("conv-1", "refactor")
    mgr.set_steps(session, [
        {"text": "read code", "state": "[ ]"},
        {"text": "patch", "state": "[P]"},
        {"text": "verify", "state": "[D]"},
    ])
    body = session.plan_path.read_text(encoding="utf-8")
    assert "- [ ] read code" in body
    assert "- [P] patch" in body
    assert "- [D] verify" in body


def test_mark_step_updates_state(mgr: pm.PlanModeManager) -> None:
    session = mgr.create("conv-2", "fix bug")
    mgr.set_steps(session, [{"text": "a"}, {"text": "b"}])
    mgr.mark_step(session, 0, "[✓]")
    body = session.plan_path.read_text(encoding="utf-8")
    assert "- [✓] a" in body
    assert "- [ ] b" in body


def test_plan_path_is_under_workspace_and_slug_safe(mgr: pm.PlanModeManager) -> None:
    session = mgr.create("conv-../evil", "x")
    # Slug must collapse traversal chars; path stays inside workspace/.
    assert ".." not in session.plan_path.parts[-2]
    assert session.plan_path.parent.parent.name == "ws"
