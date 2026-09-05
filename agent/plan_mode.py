"""Plan mode runtime support.

The plan mode keeps a durable plan.md in the Agent workspace and provides a
simple state container for the five GA-inspired phases:
explore → plan → user_confirm → execute → verify.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent.file_memory import agent_workspace_root

# Slug chars kept deliberately narrow: the task name comes from user content and
# is used as a directory name, so anything outside this set (including "." and
# path separators) is collapsed to "-" to prevent traversal.
_SLUG_UNSAFE = re.compile(r"[^a-z0-9_-]+")


@dataclass
class PlanStep:
    text: str
    state: str = "[ ]"
    detail: str = ""


@dataclass
class PlanSession:
    agent_id: int
    task_name: str
    objective: str
    plan_path: Path
    phase: str = "explore"
    steps: list[PlanStep] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    verified: bool = False

    def render(self) -> str:
        lines = [
            f"# Plan: {self.task_name}",
            "",
            f"- Objective: {self.objective}",
            f"- Phase: {self.phase}",
            "",
            "## Steps",
        ]
        if not self.steps:
            lines.append("- [ ] TODO: add steps")
        else:
            for step in self.steps:
                suffix = f" — {step.detail}" if step.detail else ""
                lines.append(f"- {step.state} {step.text}{suffix}")
        if self.notes:
            lines.extend(["", "## Notes"])
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines).rstrip() + "\n"


class PlanModeManager:
    """In-process plan state; plan.md is the durable artifact.

    One manager per Agent. Sessions are keyed by a slugified task name and
    persisted to ``workspace/plan_<task>/plan.md``.
    """

    def __init__(self, agent_id: int) -> None:
        self.agent_id = int(agent_id)
        self._sessions: dict[str, PlanSession] = {}

    def _session_key(self, task_name: str) -> str:
        slug = _SLUG_UNSAFE.sub("-", task_name.strip().lower()).strip("-")
        return slug or "plan"

    def _plan_path(self, task_name: str) -> Path:
        # Plan artifacts live under the Agent's real workspace/ subtree so the
        # GA write boundary (file_write/file_patch may touch workspace/ but not
        # memory/) applies uniformly and the model can re-read its own plan.
        return agent_workspace_root(self.agent_id) / f"plan_{self._session_key(task_name)}" / "plan.md"

    def create(self, task_name: str, objective: str) -> PlanSession:
        """Create a new plan session, or return the existing one if plan.md exists.

        Plan mode spans multiple conversation turns; the second turn must not
        clobber the plan.md the first turn produced. We key by the slugified
        task name (callers pass the conversation id for a stable per-convo plan)
        and only write the scaffold when the file is absent.
        """
        plan_path = self._plan_path(task_name)
        session = PlanSession(
            agent_id=self.agent_id,
            task_name=task_name,
            objective=objective,
            plan_path=plan_path,
        )
        key = self._session_key(task_name)
        existing = self._sessions.get(key)
        if existing is not None:
            return existing
        if plan_path.exists():
            # Preserve an earlier turn's plan; model continues editing it.
            session.phase = "execute"
            self._sessions[key] = session
            return session
        self._sessions[key] = session
        self.save(session)
        return session

    def get(self, task_name: str) -> PlanSession | None:
        return self._sessions.get(self._session_key(task_name))

    def save(self, session: PlanSession) -> Path:
        session.plan_path.parent.mkdir(parents=True, exist_ok=True)
        session.plan_path.write_text(session.render(), encoding="utf-8")
        return session.plan_path

    def update_phase(self, session: PlanSession, phase: str) -> Path:
        session.phase = phase
        return self.save(session)

    def set_steps(self, session: PlanSession, steps: list[dict[str, Any]]) -> Path:
        session.steps = [
            PlanStep(
                text=str(step.get("text") or step.get("title") or f"step-{idx + 1}"),
                state=str(step.get("state") or "[ ]"),
                detail=str(step.get("detail") or ""),
            )
            for idx, step in enumerate(steps)
        ]
        return self.save(session)

    def mark_step(self, session: PlanSession, index: int, state: str) -> Path:
        if 0 <= index < len(session.steps):
            session.steps[index].state = state
        return self.save(session)

    def add_note(self, session: PlanSession, note: str) -> Path:
        if note:
            session.notes.append(note)
        return self.save(session)


__all__ = ["PlanModeManager", "PlanSession", "PlanStep"]
