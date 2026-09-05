import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.scheduled_script import (
    ScheduledScriptApproval,
    canonical_script,
    is_script_approved,
    row_script_hash,
    script_hash,
)
from agent.code_run_approval import CodeRunApprovalBatch


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# hash / canonical parity with CodeRunApprovalBatch
# ---------------------------------------------------------------------------


def test_canonical_matches_code_run_approval_batch():
    """脚本哈希口径必须与交互态 code_run 审批完全一致，否则同段脚本两处哈希会漂移。"""
    args = {"code": "print('hi')", "type": "python", "timeout": 60}
    assert canonical_script("print('hi')", "python", 60) == CodeRunApprovalBatch._canonical(args)


def test_script_hash_stable_and_defaulted():
    assert script_hash("x=1") == script_hash("x=1", "python", 60)
    assert script_hash("x=1") != script_hash("x=2")


# ---------------------------------------------------------------------------
# is_script_approved
# ---------------------------------------------------------------------------


def test_is_script_approved_matches_hash():
    row = {"script_code": "print(1)", "script_type": "python", "script_timeout": 60}
    row["approved_hash"] = row_script_hash(row)
    assert is_script_approved(row) is True


def test_is_script_approved_false_when_no_hash():
    assert is_script_approved({"script_code": "print(1)"}) is False


def test_is_script_approved_false_when_tampered():
    row = {"script_code": "print(1)", "script_type": "python", "script_timeout": 60}
    row["approved_hash"] = row_script_hash(row)
    row["script_code"] = "print(2)"  # 改动后哈希失配
    assert is_script_approved(row) is False


# ---------------------------------------------------------------------------
# ScheduledScriptApproval.decision_for
# ---------------------------------------------------------------------------


def test_approval_allows_matching_hash():
    h = script_hash("print(1)", "python", 60)
    approval = ScheduledScriptApproval(approved_hash=h, job_id=5)
    outcome, cid = approval.decision_for({"code": "print(1)", "type": "python", "timeout": 60})
    assert outcome == "allow"
    assert cid and cid.startswith("scheduled-task:5:")


def test_approval_denies_mismatch():
    h = script_hash("print(1)", "python", 60)
    approval = ScheduledScriptApproval(approved_hash=h, job_id=5)
    outcome, cid = approval.decision_for({"code": "print(2)", "type": "python", "timeout": 60})
    assert outcome == "deny"
    assert cid is None


def test_approval_denies_when_no_approved_hash():
    approval = ScheduledScriptApproval(approved_hash=None, job_id=5)
    outcome, cid = approval.decision_for({"code": "print(1)", "type": "python"})
    assert outcome == "deny"


def test_approval_conversation_id_is_job_scoped():
    approval = ScheduledScriptApproval(approved_hash="x", job_id=7)
    assert approval.conversation_id == "scheduled-task:7"


# ---------------------------------------------------------------------------
# _execute_script_job: hash-lock blocks unapproved scripts
# ---------------------------------------------------------------------------


class _FakeConn:
    def __init__(self):
        self.execute = AsyncMock()


def _fake_pool():
    conn = _FakeConn()
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


def test_unapproved_script_is_blocked_and_disabled():
    """未授权脚本不执行：写 blocked 结果并 enabled=false，不触发 run_scheduled_script。"""
    from agent import scheduler as sched_mod
    from db import PostgresClient

    pool, conn = _fake_pool()
    original = PostgresClient.pool
    PostgresClient.pool = pool
    try:
        row = {
            "id": 1, "name": "t", "cron_expression": "0 9 * * *",
            "task_kind": "script", "script_code": "import sys; sys.exit(1)",
            "script_type": "python", "script_timeout": 60,
            "approved_hash": None, "on_error": "diagnose", "consecutive_failures": 0,
        }
        with patch.object(sched_mod, "_finalize_job", new=AsyncMock()) as fin, \
             patch("agent.scheduled_script.run_scheduled_script", new=AsyncMock()) as runner:
            run(sched_mod._execute_script_job(row))
        runner.assert_not_awaited()
        fin.assert_not_awaited()
        # blocked + disable 走内联 UPDATE
        sql = conn.execute.await_args.args[0]
        assert "enabled=false" in sql
    finally:
        PostgresClient.pool = original


def test_approved_script_success_resets_failures():
    """exit_code=0 → consecutive_failures 归零，不进自愈。"""
    from agent import scheduler as sched_mod
    from db import PostgresClient

    pool, _ = _fake_pool()
    original = PostgresClient.pool
    PostgresClient.pool = pool
    try:
        code = "print('ok')"
        row = {
            "id": 2, "name": "t", "cron_expression": "0 9 * * *",
            "task_kind": "script", "script_code": code,
            "script_type": "python", "script_timeout": 60,
            "on_error": "diagnose", "consecutive_failures": 3,
        }
        row["approved_hash"] = row_script_hash(row)
        ok_result = {"status": "ok", "exit_code": 0, "stdout": "ok", "stderr": ""}
        with patch.object(sched_mod, "_finalize_job", new=AsyncMock()) as fin, \
             patch("agent.scheduled_script.run_scheduled_script", new=AsyncMock(return_value=ok_result)), \
             patch.object(sched_mod, "_heal_script", new=AsyncMock()) as heal:
            run(sched_mod._execute_script_job(row))
        heal.assert_not_awaited()
        assert fin.await_args.kwargs["consecutive_failures"] == 0
        assert fin.await_args.kwargs["last_exit_code"] == 0
    finally:
        PostgresClient.pool = original


def test_approved_script_failure_triggers_heal():
    """exit_code!=0 且 on_error!=none → 进 _heal_script。"""
    from agent import scheduler as sched_mod
    from db import PostgresClient

    pool, _ = _fake_pool()
    original = PostgresClient.pool
    PostgresClient.pool = pool
    try:
        code = "import sys; sys.exit(3)"
        row = {
            "id": 3, "name": "t", "cron_expression": "0 9 * * *",
            "task_kind": "script", "script_code": code,
            "script_type": "python", "script_timeout": 60,
            "on_error": "diagnose", "consecutive_failures": 0,
        }
        row["approved_hash"] = row_script_hash(row)
        fail_result = {"status": "ok", "exit_code": 3, "stdout": "", "stderr": "boom"}
        with patch("agent.scheduled_script.run_scheduled_script", new=AsyncMock(return_value=fail_result)), \
             patch.object(sched_mod, "_heal_script", new=AsyncMock()) as heal:
            run(sched_mod._execute_script_job(row))
        heal.assert_awaited_once()
    finally:
        PostgresClient.pool = original


def test_failure_with_on_error_none_skips_heal():
    from agent import scheduler as sched_mod
    from db import PostgresClient

    pool, _ = _fake_pool()
    original = PostgresClient.pool
    PostgresClient.pool = pool
    try:
        code = "import sys; sys.exit(3)"
        row = {
            "id": 4, "name": "t", "cron_expression": "0 9 * * *",
            "task_kind": "script", "script_code": code,
            "script_type": "python", "script_timeout": 60,
            "on_error": "none", "consecutive_failures": 1,
        }
        row["approved_hash"] = row_script_hash(row)
        fail_result = {"status": "ok", "exit_code": 3, "stdout": "", "stderr": "boom"}
        with patch.object(sched_mod, "_finalize_job", new=AsyncMock()) as fin, \
             patch("agent.scheduled_script.run_scheduled_script", new=AsyncMock(return_value=fail_result)), \
             patch.object(sched_mod, "_heal_script", new=AsyncMock()) as heal:
            run(sched_mod._execute_script_job(row))
        heal.assert_not_awaited()
        assert fin.await_args.kwargs["consecutive_failures"] == 2
    finally:
        PostgresClient.pool = original


# ---------------------------------------------------------------------------
# propose_script_fix: allow_ai_script_fix gate
# ---------------------------------------------------------------------------


def test_propose_fix_without_permission_only_parks_pending():
    """allow_ai_script_fix=false → 只落 pending，不改 script_code/approved_hash，且 disable。"""
    from agent.scheduler import AgentScheduler
    from db import PostgresClient

    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value={
        "id": 9, "task_kind": "script", "script_code": "old",
        "script_type": "python", "script_timeout": 60,
        "allow_ai_script_fix": False, "cron_expression": "0 9 * * *",
    })
    conn.execute = AsyncMock()
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    original = PostgresClient.pool
    PostgresClient.pool = pool
    try:
        with patch("agent.scheduler.AsyncIOScheduler"):
            sched = AgentScheduler()
        result = run(sched.propose_script_fix(9, "new code", "fix"))
        assert result["applied"] is False
        sql = conn.execute.await_args.args[0]
        assert "pending_script_code" in sql and "enabled=false" in sql
    finally:
        PostgresClient.pool = original


def test_propose_fix_with_permission_applies_immediately():
    """allow_ai_script_fix=true → 写 script_code + approved_hash，清 pending，applied=True。"""
    from agent.scheduler import AgentScheduler
    from agent.scheduled_script import script_hash
    from db import PostgresClient

    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value={
        "id": 10, "task_kind": "script", "script_code": "old",
        "script_type": "python", "script_timeout": 60,
        "allow_ai_script_fix": True, "cron_expression": "0 9 * * *",
    })
    conn.execute = AsyncMock()
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    original = PostgresClient.pool
    PostgresClient.pool = pool
    try:
        with patch("agent.scheduler.AsyncIOScheduler"):
            sched = AgentScheduler()
        result = run(sched.propose_script_fix(10, "new code", "fix"))
        assert result["applied"] is True
        sql, *params = conn.execute.await_args.args
        assert "script_code=$2" in sql and "approved_hash=$3" in sql
        assert params[2] == script_hash("new code", "python", 60)
    finally:
        PostgresClient.pool = original
