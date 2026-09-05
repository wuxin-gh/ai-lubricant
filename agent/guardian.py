"""Guardian / Daemon modes for Agent.

Implements two GenericAgent-inspired long-running modes:

1. Goal Mode:
   - User gives an objective + time budget.
   - Agent is repeatedly woken with alternating create/inspect/improve prompts.
   - Budget exhaustion triggers wrap-up.

2. Autonomous Mode:
   - Agent can run background TODO-driven work when enabled.
   - This module stores state; actual idle detection/triggering is handled by scheduler/API layer.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from db import PostgresClient


GOAL_CONTINUATION_PROMPT = """
[GOAL MODE]
Objective: {objective}
Elapsed turns: {turns_used}/{max_turns}
Remaining budget: approximately {remaining_seconds} seconds

Current phase: {phase}

Rules:
- Creation phase: produce or modify real deliverables toward the objective.
- Inspection phase: review from tester/reader/maintainer perspectives; find concrete issues.
- Improvement phase: act on inspection findings with substantive changes.
- Do not merely report progress; execute meaningful work.
- If blocked, ask the user with ask_user instead of guessing.
""".strip()

GOAL_WRAP_UP_PROMPT = """
[GOAL MODE WRAP-UP]
The time/turn budget is exhausted.
Summarize:
1. What was completed
2. What remains unfinished
3. Any files/artifacts produced
4. Risks or decisions needing user input
Then stop.
""".strip()


class Guardian:
    """Guardian mode state manager."""

    def __init__(self, agent_id: int):
        self.agent_id = agent_id

    async def start_goal_mode(
        self,
        objective: str,
        budget_seconds: int,
        max_turns: int = 50,
    ) -> dict[str, Any]:
        """Start a goal-mode run for this agent."""
        if not PostgresClient.pool:
            raise RuntimeError("Database not available")
        async with PostgresClient.pool.acquire() as conn:
            # Stop existing running goal modes for this agent
            await conn.execute(
                "UPDATE agent_goal_states SET status='stopped', updated_at=now() "
                "WHERE agent_id=$1 AND mode='goal' AND status='running'",
                self.agent_id,
            )
            row = await conn.fetchrow(
                """
                INSERT INTO agent_goal_states(
                    agent_id, mode, objective, budget_seconds, start_time,
                    turns_used, max_turns, status, state
                ) VALUES($1, 'goal', $2, $3, now(), 0, $4, 'running', '{}'::jsonb)
                RETURNING *
                """,
                self.agent_id, objective, budget_seconds, max_turns,
            )
        return _state_row_to_dict(row)

    async def stop_goal_mode(self) -> bool:
        """Stop active goal mode for this agent."""
        if not PostgresClient.pool:
            return False
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE agent_goal_states SET status='stopped', updated_at=now() "
                "WHERE agent_id=$1 AND mode='goal' AND status='running'",
                self.agent_id,
            )
        return result.endswith("1")

    async def get_goal_status(self) -> dict[str, Any] | None:
        """Get latest goal-mode state."""
        if not PostgresClient.pool:
            return None
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_goal_states WHERE agent_id=$1 AND mode='goal' "
                "ORDER BY created_at DESC LIMIT 1",
                self.agent_id,
            )
        return _state_row_to_dict(row) if row else None

    async def next_goal_prompt(self) -> str | None:
        """Advance goal-mode state and return the next continuation/wrap-up prompt.

        ``wrapping_up`` is intentionally readable exactly once: when the budget is
        exhausted we atomically switch to ``wrapping_up`` and return the wrap-up
        prompt. The next call sees no ``running`` row and returns None.
        """
        if not PostgresClient.pool:
            return None
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_goal_states WHERE agent_id=$1 AND mode='goal' AND status='running' "
                "ORDER BY created_at DESC LIMIT 1",
                self.agent_id,
            )
            if not row:
                return None

            state = dict(row)
            turns_used = int(state.get("turns_used") or 0)
            max_turns = int(state.get("max_turns") or 50)
            budget_seconds = int(state.get("budget_seconds") or 0)
            start_time = state.get("start_time")
            objective = state.get("objective") or ""

            elapsed = 0
            if start_time:
                now = datetime.now(timezone.utc)
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                elapsed = int((now - start_time).total_seconds())
            remaining = max(0, budget_seconds - elapsed)

            if turns_used >= max_turns or remaining <= 0:
                await conn.execute(
                    "UPDATE agent_goal_states SET status='wrapping_up', updated_at=now() WHERE id=$1",
                    state["id"],
                )
                return GOAL_WRAP_UP_PROMPT

            phase = _goal_phase(turns_used)
            await conn.execute(
                "UPDATE agent_goal_states SET turns_used=turns_used+1, updated_at=now() WHERE id=$1",
                state["id"],
            )
            return GOAL_CONTINUATION_PROMPT.format(
                objective=objective,
                turns_used=turns_used + 1,
                max_turns=max_turns,
                remaining_seconds=remaining,
                phase=phase,
            )

    async def mark_goal_done(self, *, budget_exhausted: bool = False) -> None:
        """Mark wrapping/running goal mode as done (or done_budget)."""
        if not PostgresClient.pool:
            return
        status = "done_budget" if budget_exhausted else "done"
        async with PostgresClient.pool.acquire() as conn:
            await conn.execute(
                "UPDATE agent_goal_states SET status=$2, updated_at=now() "
                "WHERE agent_id=$1 AND mode='goal' AND status IN ('running','wrapping_up')",
                self.agent_id,
                status,
            )

    async def set_autonomous_enabled(self, enabled: bool) -> dict[str, Any]:
        """Enable/disable autonomous mode flag on the agent row."""
        if not PostgresClient.pool:
            raise RuntimeError("Database not available")
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE agents SET autonomous_enabled=$2, updated_at=now() WHERE id=$1 RETURNING id, autonomous_enabled",
                self.agent_id, enabled,
            )
        if not row:
            raise RuntimeError(f"Agent {self.agent_id} not found")
        return {"agent_id": row["id"], "autonomous_enabled": row["autonomous_enabled"]}

    async def get_autonomous_status(self) -> dict[str, Any]:
        if not PostgresClient.pool:
            return {"agent_id": self.agent_id, "autonomous_enabled": False}
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, autonomous_enabled, guardian_enabled, guardian_interval FROM agents WHERE id=$1",
                self.agent_id,
            )
        if not row:
            return {"agent_id": self.agent_id, "autonomous_enabled": False}
        return {
            "agent_id": row["id"],
            "autonomous_enabled": row["autonomous_enabled"],
            "guardian_enabled": row["guardian_enabled"],
            "guardian_interval": row["guardian_interval"],
        }


def _goal_phase(turns_used: int) -> str:
    """Return the next phase name based on turn count."""
    if turns_used == 0:
        return "creation"
    phases = ["inspection", "improvement"]
    return phases[(turns_used - 1) % 2]


def _state_row_to_dict(row) -> dict[str, Any]:
    d = dict(row)
    for key in ("start_time", "created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    return d
