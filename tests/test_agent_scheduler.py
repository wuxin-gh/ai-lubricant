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


# ---------------------------------------------------------------------------
# 执行记录（agent_scheduled_task_runs）tests
# ---------------------------------------------------------------------------


def test_start_run_inserts_running_row(fake_db, scheduler):
    from agent import scheduler as sched_mod

    fake_db.fetchrow.return_value = {"id": 77}
    result = run(sched_mod._start_run(11, task_kind="script", triggered_by="manual"))

    sql, *params = fake_db.fetchrow.await_args.args
    flat = squash_sql(sql)
    assert flat.startswith("INSERT INTO agent_scheduled_task_runs")
    assert "status" not in params  # status 走字面量 'running'
    assert params == [11, "script", "manual"]
    assert result == 77


def test_start_run_returns_none_without_pool(reset_pool, scheduler):
    from agent import scheduler as sched_mod

    assert run(sched_mod._start_run(1, task_kind="prompt", triggered_by="scheduler")) is None


def test_start_run_swallows_db_error(fake_db, scheduler):
    """_start_run 失败不阻断执行——run 记录是旁路，任务本体照跑。"""
    from agent import scheduler as sched_mod

    fake_db.fetchrow.side_effect = RuntimeError("db down")
    assert run(sched_mod._start_run(1, task_kind="prompt", triggered_by="scheduler")) is None


def test_finish_run_updates_only_provided_columns(fake_db, scheduler):
    from agent import scheduler as sched_mod

    run(sched_mod._finish_run(
        77, status="failed", exit_code=1, stderr="boom", result_text="x" * 5000,
    ))

    sql, *params = fake_db.execute.await_args.args
    flat = squash_sql(sql)
    assert flat.startswith("UPDATE agent_scheduled_task_runs SET")
    assert flat.endswith("WHERE id=$1")
    # 只写传入的列 + status；没传的（stdout/conversation_id 等）不该出现。
    assert "exit_code=$" in flat and "stderr=$" in flat
    assert "stdout" not in flat and "conversation_id" not in flat
    # params = [run_id, status, exit_code, stderr, result_text]（列序按 dict 插入序）；
    # result_text 截断到 2000。
    assert params[0] == 77
    assert params[1] == "failed"
    assert params[2] == 1
    assert params[3] == "boom"
    assert len(params[4]) == 2000


def test_finish_run_noop_when_run_id_none(fake_db, scheduler):
    from agent import scheduler as sched_mod

    run(sched_mod._finish_run(None, status="completed"))
    fake_db.execute.assert_not_awaited()


def test_finish_run_caps_stdout(fake_db, scheduler):
    from agent import scheduler as sched_mod

    run(sched_mod._finish_run(1, status="completed", stdout="y" * 30000))

    _sql, *params = fake_db.execute.await_args.args
    stdout_val = [p for p in params if isinstance(p, str) and p.startswith("y")][0]
    assert len(stdout_val) == 16000


def test_list_runs_orders_desc_and_pages(fake_db, scheduler):
    fake_db.fetch.return_value = [{"id": 5, "run_at": datetime.now(timezone.utc), "status": "completed"}]

    result = run(scheduler.list_runs(11, limit=50))

    sql, *params = fake_db.fetch.await_args.args
    flat = squash_sql(sql)
    assert "FROM agent_scheduled_task_runs WHERE job_id=$1" in flat
    assert "ORDER BY run_at DESC" in flat
    assert "result_snippet" in flat
    assert params == [11, 50]
    assert isinstance(result[0]["run_at"], str)

    # cursor 翻页：加 AND id < $n
    run(scheduler.list_runs(11, limit=50, cursor=100))
    sql, *params = fake_db.fetch.await_args.args
    flat = squash_sql(sql)
    assert "AND id < $3" in flat
    assert params == [11, 50, 100]


def test_list_runs_returns_empty_without_pool(reset_pool, scheduler):
    assert run(scheduler.list_runs(1)) == []


def test_get_run_returns_row_with_str_dates(fake_db, scheduler):
    fake_db.fetchrow.return_value = {"id": 5, "job_id": 11, "run_at": datetime.now(timezone.utc), "stdout": "out"}

    result = run(scheduler.get_run(5))

    sql, *params = fake_db.fetchrow.await_args.args
    assert squash_sql(sql) == "SELECT * FROM agent_scheduled_task_runs WHERE id=$1"
    assert params == [5]
    assert isinstance(result["run_at"], str)
    assert result["stdout"] == "out"


def test_get_run_none_when_missing(fake_db, scheduler):
    fake_db.fetchrow.return_value = None
    assert run(scheduler.get_run(404)) is None


def test_prompt_run_status_mapping():
    from agent import scheduler as sched_mod

    assert sched_mod._prompt_run_status([{"result": "CURRENT_TASK_DONE", "data": "ok"}]) == "completed"
    assert sched_mod._prompt_run_status([{"result": "ERROR", "data": "bad"}]) == "failed"
    assert sched_mod._prompt_run_status([{"result": "MAX_TURNS_EXCEEDED", "data": ""}]) == "failed"
    assert sched_mod._prompt_run_status([{"result": "EXITED", "data": ""}]) == "aborted"
    assert sched_mod._prompt_run_status([]) == "completed"
    assert sched_mod._prompt_run_status([{"turn": 3}]) == "completed"


def test_cleanup_old_runs_window_delete(fake_db, scheduler):
    fake_db.execute.return_value = None
    run(scheduler._cleanup_old_runs())

    sql, *_ = fake_db.execute.await_args.args
    flat = squash_sql(sql)
    assert flat.startswith("DELETE FROM agent_scheduled_task_runs")
    assert "row_number() OVER (PARTITION BY job_id ORDER BY run_at DESC)" in flat
    assert "rn > 200" in flat


def test_cleanup_old_runs_swallows_error(fake_db, scheduler):
    fake_db.execute.side_effect = RuntimeError("nope")
    # should not raise
    run(scheduler._cleanup_old_runs())


def test_execute_prompt_job_records_run_and_finalizes(fake_db, scheduler):
    """_execute_prompt_job 应开 run 行、结尾写 status，并保持 _finalize_job 兼容。"""
    from agent import scheduler as sched_mod

    row = {
        "id": 11, "cron_expression": "0 9 * * *", "task_prompt": "summarize",
        "task_kind": "prompt", "agent_id": None, "user_id": "u1",
        "name": "t", "consecutive_failures": 0,
    }
    fake_db.fetchrow.return_value = {"id": 77}  # _start_run 的 RETURNING id

    done = [{"result": "CURRENT_TASK_DONE", "data": "all good"}]
    with patch.object(sched_mod, "_scheduled_run", AsyncMock(return_value=done)), \
         patch.object(sched_mod, "_finalize_job", AsyncMock()) as fin:
        run(sched_mod._execute_prompt_job(row, triggered_by="manual"))

        # run 收尾：status=completed + result_text + duration_ms
        finish_calls = [c for c in fake_db.execute.await_args_list
                        if "agent_scheduled_task_runs" in c.args[0]]
        assert finish_calls, "应至少有一次 _finish_run 的 UPDATE agent_scheduled_task_runs"
        finish_sql, *finish_params = finish_calls[-1].args
        assert finish_params[1] == "completed"
        assert finish_params[2] == '{"result": "CURRENT_TASK_DONE", "data": "all good"}'
        # _finalize_job 仍被调用（列表 last_* 语义不变）。
        fin.assert_awaited_once()


def test_scheduled_run_persists_conversation(fake_db, scheduler):
    """_scheduled_run 应建 kind=scheduled 会话、写 user/assistant 消息、
    on_event 渐进落库，结尾 assistant 转回 done。"""
    from agent import scheduler as sched_mod
    from agent import conversation_store

    row = {"id": 11, "name": "t", "user_id": "u1", "agent_id": None, "api_key_id": None, "model": ""}

    conv_created = {"id": "conv-abc"}
    user_msg = {"id": 101}
    asst_msg = {"id": 102}
    create_conv = AsyncMock(return_value=conv_created)
    add_msg = AsyncMock(side_effect=lambda *args, **kw: user_msg if args[1] == "user" else asst_msg)
    update_msg = AsyncMock()
    run_task = AsyncMock(return_value=[{"result": "CURRENT_TASK_DONE", "data": "done"}])

    class FakeAgent:
        _agent_system_prompt = "persona"

        def __init__(self, *args, **kwargs):
            pass

        async def _ensure_config(self): return None
        async def resolve_scheduled_llm(self, api_key_id=None, model=""): return None
        async def run_task(self, prompt, system_prompt="", max_turns=None, llm=None, on_event=None):
            if on_event:
                await on_event({"type": "content", "text": "hello"})
                await on_event({"type": "done", "usage": {"total_tokens": 5}})
            return [{"result": "CURRENT_TASK_DONE", "data": "done"}]

    with patch.object(sched_mod, "_scheduled_scene", lambda r: None), \
         patch("agent.agent_main.GenericAgent", FakeAgent), \
         patch.object(conversation_store, "create_conversation", create_conv), \
         patch.object(conversation_store, "add_message", add_msg), \
         patch.object(conversation_store, "update_message", update_msg), \
         patch("agent.scene_context.append_prompt", lambda a, b: "sys"), \
         patch("agent.scene_context.persist", lambda s: {"scheduled_job_id": 11}):
        result = run(sched_mod._scheduled_run(row, "do the thing", max_turns=5, run_id=77))

    assert result[0]["result"] == "CURRENT_TASK_DONE"
    # 会话归属 = 任务 owner，kind=scheduled（不进普通会话列表，但按 id 可读）。
    create_conv.assert_awaited_once()
    kwargs = create_conv.await_args.kwargs
    assert kwargs["kind"] == "scheduled"
    assert kwargs["user_id"] == "u1"
    assert kwargs["agent_id"] is None
    # user 消息先建（分页锚点），assistant 占位后建。
    roles = [c.args[1] for c in add_msg.await_args_list]
    assert roles == ["user", "assistant"]
    # 结尾 assistant 转回 done。
    final_update = update_msg.await_args_list[-1]
    assert final_update.args[0] == 102
    assert final_update.kwargs.get("status") == "done"
    assert final_update.kwargs.get("content") == "hello"


def test_scheduled_run_degrades_without_clickhouse(fake_db, scheduler):
    """CH 不可用 → 对话不落库，但任务本体照跑，run 行 conversation_id 留空。"""
    from agent import scheduler as sched_mod
    from agent import conversation_store

    row = {"id": 11, "name": "t", "user_id": "u1", "agent_id": None, "api_key_id": None, "model": ""}

    class FakeAgent:
        _agent_system_prompt = "persona"

        def __init__(self, *args, **kwargs):
            pass

        async def _ensure_config(self): return None
        async def resolve_scheduled_llm(self, api_key_id=None, model=""): return None
        async def run_task(self, prompt, system_prompt="", max_turns=None, llm=None, on_event=None):
            assert on_event is None  # 没建成会话就不该有事件出口
            return [{"result": "CURRENT_TASK_DONE", "data": "ok"}]

    finish_calls = []

    async def spy_finish(run_id, *, status, **fields):
        finish_calls.append((status, fields))

    with patch.object(sched_mod, "_scheduled_scene", lambda r: None), \
         patch("agent.agent_main.GenericAgent", FakeAgent), \
         patch.object(conversation_store, "create_conversation",
                      AsyncMock(side_effect=RuntimeError("ch down"))), \
         patch.object(sched_mod, "_finish_run", spy_finish), \
         patch("agent.scene_context.append_prompt", lambda a, b: "sys"):
        result = run(sched_mod._scheduled_run(row, "go", max_turns=5, run_id=88))

    assert result[0]["result"] == "CURRENT_TASK_DONE"
    # 没有中途 conversation_id 写回。
    assert all("conversation_id" not in fields for _s, fields in finish_calls)
