import asyncio
import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from db import PostgresClient
from agent.scheduler import AgentScheduler


def run(coro):
    return asyncio.run(coro)


def squash_sql(sql: str) -> str:
    return " ".join(sql.split())


class FakeConn:
    def __init__(self):
        self.execute = AsyncMock()
        self.fetch = AsyncMock()
        self.fetchrow = AsyncMock()


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


@pytest.fixture
def scheduler(reset_pool):
    """Create AgentScheduler with mocked APScheduler (no real scheduling)."""
    with patch("agent.scheduler.AsyncIOScheduler"):
        sched = AgentScheduler()
    return sched


# ---------------------------------------------------------------------------
# add_job tests
# ---------------------------------------------------------------------------


def test_add_job_inserts_and_registers(fake_db, scheduler):
    fake_db.fetchrow.return_value = {"id": 42}

    result = run(
        scheduler.add_job(
            name="daily triage",
            cron_expression="0 9 * * *",
            task_prompt="Summarize overnight incidents",
            skill_id=7,
            enabled=True,
        )
    )

    sql, *params = fake_db.fetchrow.await_args.args
    # agent_id 早已在列里；user_id 是多用户隔离归属列；task_kind 起新增脚本型任务列。
    flat = squash_sql(sql)
    assert flat.startswith("INSERT INTO agent_scheduled_tasks(name, cron_expression, task_prompt")
    assert "task_kind" in flat and "script_code" in flat and "approved_hash" in flat
    assert flat.endswith("RETURNING id")
    # prompt 型任务前 7 个入参与历史一致；脚本列取默认值。
    assert params[:7] == ["daily triage", "0 9 * * *", "Summarize overnight incidents", 7, True, None, None]
    assert params[7] == "prompt"  # task_kind 默认
    assert result == 42
    # APScheduler.add_job was called (enabled=True)
    scheduler._scheduler.add_job.assert_called_once()


def test_add_job_disabled_skips_apscheduler(fake_db, scheduler):
    fake_db.fetchrow.return_value = {"id": 10}

    result = run(
        scheduler.add_job(
            name="draft",
            cron_expression="0 * * * *",
            task_prompt="test",
            enabled=False,
        )
    )

    assert result == 10
    # APScheduler.add_job NOT called (disabled)
    scheduler._scheduler.add_job.assert_not_called()


def test_add_job_returns_zero_without_pool(reset_pool, scheduler):
    result = run(scheduler.add_job("n", "* * * * *", "task"))
    assert result == 0


def test_add_job_backfills_user_id_from_agent_owner(fake_db, scheduler):
    """AI 经 capability_call 建任务无 C 端 caller：add_job 按 agent_id 查 agents.user_id
    补归属，否则 user_id 落 NULL 会被用户态列表过滤掉（用户自己看不到 AI 建的任务）。
    """
    # fetchrow 被调两次（先查 agents.user_id，再 INSERT RETURNING id），
    # 同一个 return_value 两边都覆盖：owner 查询读 user_id，INSERT 读 id。
    fake_db.fetchrow.return_value = {"id": 42, "user_id": "u-owner-9"}

    run(scheduler.add_job(
        name="ai task",
        cron_expression="0 9 * * *",
        task_prompt="x",
        agent_id=5,
        # user_id 不传（模拟 capability_call 路径）
    ))

    # 先查 agents.user_id，再 INSERT：INSERT 的 user_id 参数应为解析出的 owner。
    insert_call = fake_db.fetchrow.await_args_list[-1]
    _sql, *params = insert_call.args
    assert params[6] == "u-owner-9"  # user_id 位置（name,cron,prompt,skill,enabled,agent_id,user_id,...）
    assert params[5] == 5  # agent_id


# ---------------------------------------------------------------------------
# cancel_job tests
# ---------------------------------------------------------------------------


def test_cancel_job_disables_and_removes_from_apscheduler(fake_db, scheduler):
    fake_db.fetchrow.return_value = {"id": 9}

    result = run(scheduler.cancel_job(9))

    sql, *params = fake_db.fetchrow.await_args.args
    assert squash_sql(sql) == (
        "UPDATE agent_scheduled_tasks SET enabled=false "
        "WHERE id=$1 AND enabled=true RETURNING id"
    )
    assert params == [9]
    assert result is True
    scheduler._scheduler.remove_job.assert_called_once_with("9")


def test_cancel_job_returns_false_when_not_found(fake_db, scheduler):
    fake_db.fetchrow.return_value = None

    assert run(scheduler.cancel_job(9)) is False


def test_cancel_job_noop_when_pool_is_none(reset_pool, scheduler):
    assert run(scheduler.cancel_job(1)) is False


# ---------------------------------------------------------------------------
# get_jobs tests
# ---------------------------------------------------------------------------


def test_get_jobs_fetches_from_db(fake_db, scheduler):
    rows = [
        {"id": 1, "name": "a", "enabled": True, "next_run_at": None, "last_run_at": None, "created_at": datetime.now(timezone.utc)},
    ]
    fake_db.fetch.return_value = rows

    result = run(scheduler.get_jobs())

    sql, *params = fake_db.fetch.await_args.args
    flat_sql = squash_sql(sql)
    assert "FROM agent_scheduled_tasks" in flat_sql
    assert "WHERE" not in flat_sql
    assert params == []
    assert len(result) == 1
    # datetime fields should be serialized to strings
    assert isinstance(result[0]["created_at"], str)


def test_get_jobs_with_enabled_filter(fake_db, scheduler):
    fake_db.fetch.return_value = [{"id": 3, "enabled": True, "created_at": None, "last_run_at": None, "next_run_at": None}]

    result = run(scheduler.get_jobs(enabled=True))

    sql, *params = fake_db.fetch.await_args.args
    flat_sql = squash_sql(sql)
    assert "WHERE enabled=$1" in flat_sql
    assert params == [True]


def test_get_jobs_returns_empty_without_pool(reset_pool, scheduler):
    assert run(scheduler.get_jobs()) == []


# ---------------------------------------------------------------------------
# run_now tests
# ---------------------------------------------------------------------------


def test_run_now_returns_false_when_not_found(fake_db, scheduler):
    fake_db.fetchrow.return_value = None

    result = run(scheduler.run_now(99))
    assert result is False


def test_run_now_noop_when_pool_is_none(reset_pool, scheduler):
    assert run(scheduler.run_now(1)) is False


# ---------------------------------------------------------------------------
# _finalize_job tests (结果落库现在内联在执行链路，经 _finalize_job 收尾)
# ---------------------------------------------------------------------------


def test_finalize_job_writes_result_and_health(fake_db, scheduler):
    from agent import scheduler as sched_mod

    run(sched_mod._finalize_job(
        11, "0 9 * * *", "ok",
        last_exit_code=0, last_stderr="", consecutive_failures=0,
    ))

    sql, *params = fake_db.execute.await_args.args
    assert "UPDATE agent_scheduled_tasks" in sql
    assert "last_result=$2" in sql
    assert "last_exit_code=$4" in sql
    # job_id, result_text, next_run, exit_code, stderr, failures, heal_history
    assert params[0] == 11
    assert params[1] == "ok"
    assert params[3] == 0


def test_finalize_job_noop_when_pool_is_none(reset_pool, scheduler):
    from agent import scheduler as sched_mod

    run(sched_mod._finalize_job(1, "0 9 * * *", "ok"))


# ---------------------------------------------------------------------------
# _reload_jobs_from_db tests
# ---------------------------------------------------------------------------


def test_reload_jobs_from_db_loads_enabled_jobs(fake_db, scheduler):
    fake_db.fetch.return_value = [
        {"id": 1, "cron_expression": "0 9 * * *"},
        {"id": 2, "cron_expression": "0 * * * *"},
    ]

    run(scheduler._reload_jobs_from_db())

    assert scheduler._scheduler.add_job.call_count == 2


def test_reload_jobs_skips_on_missing_pool(reset_pool, scheduler):
    # should not raise
    run(scheduler._reload_jobs_from_db())
