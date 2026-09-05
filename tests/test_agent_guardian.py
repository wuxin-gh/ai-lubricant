from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.guardian import GOAL_CONTINUATION_PROMPT, GOAL_WRAP_UP_PROMPT, Guardian
from db import PostgresClient


@pytest.fixture
def reset_pool():
    original = PostgresClient.pool
    PostgresClient.pool = None
    yield
    PostgresClient.pool = original


def _pool_with_conn(conn):
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.mark.asyncio
async def test_goal_prompt_advances_creation_phase(reset_pool):
    conn = AsyncMock()
    conn.fetchrow.return_value = {
        "id": 9,
        "objective": "ship docs",
        "turns_used": 0,
        "max_turns": 5,
        "budget_seconds": 600,
        "start_time": datetime.now(timezone.utc),
    }
    PostgresClient.pool = _pool_with_conn(conn)

    prompt = await Guardian(3).next_goal_prompt()

    assert "Current phase: creation" in prompt
    assert "ship docs" in prompt
    conn.execute.assert_awaited_once()
    assert "turns_used=turns_used+1" in conn.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_goal_budget_returns_single_wrap_up_then_none(reset_pool):
    conn = AsyncMock()
    expired = {
        "id": 9,
        "objective": "ship docs",
        "turns_used": 1,
        "max_turns": 5,
        "budget_seconds": 60,
        "start_time": datetime.now(timezone.utc) - timedelta(seconds=120),
    }
    conn.fetchrow.side_effect = [expired, None]
    PostgresClient.pool = _pool_with_conn(conn)
    guardian = Guardian(3)

    assert await guardian.next_goal_prompt() == GOAL_WRAP_UP_PROMPT
    assert await guardian.next_goal_prompt() is None
    assert "status='wrapping_up'" in conn.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_mark_goal_done_budget(reset_pool):
    conn = AsyncMock()
    PostgresClient.pool = _pool_with_conn(conn)

    await Guardian(3).mark_goal_done(budget_exhausted=True)

    sql, agent_id, status = conn.execute.await_args.args
    assert "status=$2" in sql
    assert agent_id == 3
    assert status == "done_budget"
