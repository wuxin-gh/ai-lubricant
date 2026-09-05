"""TDD tests for GenericAgent database migration in db.py."""
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import PostgresClient


AGENT_TABLES = (
    "agent_insight_index",
    "agent_global_facts",
    "agent_skills",
    "agent_session_archives",
    "agent_tasks",
    "agent_scheduled_tasks",
    "agent_config",
)

AGENT_INDEXES = (
    "idx_agent_tasks_status",
    "idx_agent_tasks_parent",
    "idx_agent_skills_parent",
    "idx_agent_skills_category",
    "idx_agent_tasks_created",
)


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def reset_pool():
    """Reset the class-level pool around every test."""
    original = PostgresClient.pool
    PostgresClient.pool = None
    yield
    PostgresClient.pool = original


def _fake_pool():
    conn = AsyncMock()
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


def test_create_agent_tables_sql_contains_all_tables():
    """The DDL string/body contains all table names and key columns."""
    source = Path(PROJECT_ROOT / "db.py").read_text(encoding="utf-8")
    method_start = source.index("async def create_agent_tables")
    method_end = source.index("\n    @classmethod", method_start + 1)
    method_body = source[method_start:method_end]

    for table in AGENT_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in method_body, f"missing table {table}"

    for index in AGENT_INDEXES:
        assert f"CREATE INDEX IF NOT EXISTS {index}" in method_body, f"missing index {index}"

    key_columns = (
        "id SERIAL PRIMARY KEY",
        "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
        "key TEXT UNIQUE NOT NULL",
        "key TEXT PRIMARY KEY",
        "fact_key TEXT UNIQUE NOT NULL",
        "name TEXT NOT NULL",
        "cron_expression TEXT NOT NULL",
        "task_prompt TEXT NOT NULL",
        "prompt TEXT NOT NULL",
        "parent_skill_id INT REFERENCES agent_skills(id)",
        "skill_id INT REFERENCES agent_skills(id)",
        "parent_task_id UUID REFERENCES agent_tasks(id)",
        "request_log_id INT REFERENCES request_logs(id)",
        "id BIGSERIAL PRIMARY KEY",
    )
    for column in key_columns:
        assert column in method_body, f"missing key column/constraint: {column}"


def test_create_agent_tables_mocks_asyncpg(reset_pool):
    """Mock PostgresClient.pool and verify conn.execute is called for each table/index."""
    pool, conn = _fake_pool()
    PostgresClient.pool = pool

    run(PostgresClient.create_agent_tables())

    calls = [call.args[0].strip() for call in conn.execute.await_args_list]
    create_table_calls = [c for c in calls if c.upper().startswith("CREATE TABLE")]
    create_index_calls = [c for c in calls if c.upper().startswith("CREATE INDEX")]

    assert len(create_table_calls) >= len(AGENT_TABLES)
    assert len(create_index_calls) >= len(AGENT_INDEXES)

    for table in AGENT_TABLES:
        assert any(table in c for c in create_table_calls), f"no CREATE TABLE for {table}"

    for index in AGENT_INDEXES:
        assert any(index in c for c in create_index_calls), f"no CREATE INDEX for {index}"


def test_fk_ordering_skills_before_scheduled_tasks(reset_pool):
    """agent_skills must be created before agent_scheduled_tasks because of the FK."""
    pool, conn = _fake_pool()
    PostgresClient.pool = pool

    run(PostgresClient.create_agent_tables())

    calls = [call.args[0].strip() for call in conn.execute.await_args_list]
    table_order = [c for c in calls if c.upper().startswith("CREATE TABLE")]

    skills_idx = next(
        i for i, c in enumerate(table_order) if "agent_skills" in c
    )
    scheduled_idx = next(
        i for i, c in enumerate(table_order) if "agent_scheduled_tasks" in c
    )
    assert skills_idx < scheduled_idx, "agent_skills must be created before agent_scheduled_tasks"

    # Also assert FK in the scheduled tasks definition points to the right table
    scheduled_ddl = table_order[scheduled_idx]
    assert "REFERENCES agent_skills(id)" in scheduled_ddl
