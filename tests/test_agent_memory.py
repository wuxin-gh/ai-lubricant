import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from db import PostgresClient
from agent.memory import MemorySystem


def run(coro):
    return asyncio.run(coro)


class FakeConn:
    def __init__(self, fetch_rows=None):
        self.execute = AsyncMock()
        self.fetch = AsyncMock(side_effect=list(fetch_rows or []))


@pytest.fixture
def reset_pool():
    original = PostgresClient.pool
    PostgresClient.pool = None
    yield
    PostgresClient.pool = original


@pytest.fixture
def fake_db(reset_pool):
    conn = FakeConn()
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    PostgresClient.pool = pool
    return conn


def test_upsert_insight_and_get_l1_insights(fake_db):
    rows = [
        {"key": "k1", "value": "v1", "category": "general", "priority": 5},
        {"key": "k2", "value": "v2", "category": "general", "priority": 1},
    ]
    fake_db.fetch.side_effect = [rows]
    memory = MemorySystem()

    run(memory.upsert_l1_insight("k1", "v1", category="general", priority=5))
    result = run(memory.get_l1_insights(limit=2))

    sql, *params = fake_db.execute.await_args.args
    assert "INSERT INTO agent_insight_index" in sql
    assert "ON CONFLICT(key) DO UPDATE SET" in sql
    assert params == ["k1", "v1", "general", 5, None]

    fetch_sql, *fetch_params = fake_db.fetch.await_args.args
    assert "FROM agent_insight_index" in fetch_sql
    assert "ORDER BY priority DESC, category ASC, updated_at DESC" in fetch_sql
    assert "LIMIT $1" in fetch_sql
    assert fetch_params == [2]
    assert result == rows


def test_upsert_fact_and_get_l2_facts(fake_db):
    rows = [
        {"fact_key": "prefs", "fact_value": {"theme": "dark"}, "source": "agent"},
    ]
    fake_db.fetch.side_effect = [rows]
    memory = MemorySystem()

    payload = {"theme": "dark", "alerts": True}
    run(memory.upsert_l2_fact("prefs", payload, source="agent", confidence=0.7, verified=True))
    result = run(memory.get_l2_facts(limit=3))

    sql, *params = fake_db.execute.await_args.args
    assert "INSERT INTO agent_global_facts" in sql
    assert "$2::jsonb" in sql
    assert params[0] == "prefs"
    assert params[1] == '{"theme": "dark", "alerts": true}'
    assert params[2:] == ["agent", 0.7, True]

    fetch_sql, *fetch_params = fake_db.fetch.await_args.args
    assert "FROM agent_global_facts" in fetch_sql
    assert "ORDER BY updated_at DESC, created_at DESC" in fetch_sql
    assert fetch_params == [3]
    assert result == rows


def test_legacy_learn_skill_upserts_inside_transaction(fake_db):
    """Legacy L3 stays readable during migration; writes still use an atomic upsert."""
    class _Tx:
        async def __aenter__(self):
            return None
        async def __aexit__(self, *_args):
            return False

    fake_db.transaction = lambda: _Tx()
    fake_db.fetchval = AsyncMock(return_value=None)
    memory = MemorySystem()

    run(memory.learn_skill(name="triage", content="steps", trigger_patterns=["error", "fail"]))

    assert "INSERT INTO agent_skills" in fake_db.execute.await_args.args[0]


def test_archive_session_and_get_l4_archive(fake_db):
    rows = [
        {"session_id": "s2", "summary": "newest"},
        {"session_id": "s1", "summary": "older"},
    ]
    fake_db.fetch.side_effect = [rows]
    memory = MemorySystem()

    run(memory.archive_session("session-1"))
    result = run(memory.get_l4_archive(limit=2))

    sql, *params = fake_db.execute.await_args.args
    assert "INSERT INTO agent_session_archives" in sql
    assert "$10::jsonb" in sql
    assert params == ["session-1", None, "", "", [], [], 0, 0, None, "{}"]

    fetch_sql, *fetch_params = fake_db.fetch.await_args.args
    assert "FROM agent_session_archives" in fetch_sql
    assert "ORDER BY archived_at DESC" in fetch_sql
    assert fetch_params == [2]
    assert result == rows


def test_optional_filters_change_query_paths(fake_db):
    memory = MemorySystem()
    fake_db.fetch.side_effect = [[{"key": "k"}], [{"fact_key": "f"}], [{"name": "skill"}]]

    run(memory.get_l1_insights(category="ops", limit=4))
    insight_sql, *insight_params = fake_db.fetch.await_args_list[0].args
    assert "WHERE category=$1" in insight_sql
    assert insight_params == ["ops", 4]

    run(memory.get_l2_facts(source="system", limit=5))
    fact_sql, *fact_params = fake_db.fetch.await_args_list[1].args
    assert "WHERE source=$1" in fact_sql
    assert fact_params == ["system", 5]

    run(memory.list_skills(category="ops", limit=6))
    skill_sql, *skill_params = fake_db.fetch.await_args_list[2].args
    assert "FROM agent_skills" in skill_sql
    assert "WHERE category=$1" in skill_sql
    assert "ORDER BY updated_at DESC, created_at DESC" in skill_sql
    assert skill_params == ["ops", 6]
