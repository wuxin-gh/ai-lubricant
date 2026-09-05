"""marketplace_store（PG 编辑真相源 + 发布 outbox）的行为回归。

FakePool 照 tests/test_marketplace_leaderboard.py 的模式直接替换
``PostgresClient.pool``；SQL 不真正执行，靠 fake conn 记录语句与参数断言行为
（pending coalesce / claim CAS / 退避 / stale 回队的关键谓词都在 SQL 文本里）。
"""
from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, "server")

import marketplace_store as store  # noqa: E402


class _FakeConn:
    """按语句关键字路由的假连接：capture SQL 文本 + 参数。"""

    def __init__(self, state: dict):
        self._state = state

    async def execute(self, sql, *args):
        self._state.setdefault("execute", []).append((sql, args))

    async def fetchrow(self, sql, *args):
        self._state.setdefault("fetchrow", []).append((sql, args))
        return self._state.get("row_result")

    async def fetch(self, sql, *args):
        self._state.setdefault("fetch", []).append((sql, args))
        return self._state.get("fetch_result", [])

    async def fetchval(self, sql, *args):
        self._state.setdefault("fetchval", []).append((sql, args))
        return self._state.get("val_result")

    def transaction(self):
        # 最小事务桩：async with 上下文。
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _Ctx(self._conn)

    async def release(self, conn):
        pass


class _Ctx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


@pytest.fixture()
def fake_pool(monkeypatch):
    def install(state: dict):
        import db

        conn = _FakeConn(state)
        pool = _FakePool(conn)
        monkeypatch.setattr(db.PostgresClient, "pool", pool)
        monkeypatch.setattr(store, "_populated_cache", None)
        return conn

    return install


def _manifest(item_id="local.agnes", module="channels") -> dict:
    return {
        "schema": "ai-lubricant.channel-template/v1",
        "id": item_id, "name": item_id, "display_name": "Agnes",
        "version": "1.0.0", "summary": "s", "kind": "channel_template", "module": module,
        "resource": {
            "type": "channel_template",
            "channel": {
                "name": "Agnes", "base_url": "https://api.agnes.example.com/v1",
                "chat_protocols": [{"enabled": True, "protocol": "openai", "path": "/v1/chat/completions"}],
            },
            "freeze_policy": {"enabled": True, "rules": []},
        },
    }


def test_upsert_item_writes_manifest_summary_and_enqueues_upsert(fake_pool):
    state: dict = {"row_result": {"id": 1, "module": "channels", "item_id": "local.agnes",
                                  "manifest": {}, "summary": {}, "status": "published", "revision": 1,
                                  "updated_at": None}}
    fake_pool(state)
    row = asyncio.run(store.upsert_item("channels", _manifest()))
    assert row["item_id"] == "local.agnes"
    sqls = [sql for sql, _ in state["execute"]] + [sql for sql, _ in state["fetchrow"]]
    # 同一事务里：items upsert（fetchrow RETURNING）+ job 入队（execute）。
    insert_item = [s for s in sqls if "INSERT INTO marketplace_items" in s]
    insert_job = [s for s in sqls if "INSERT INTO marketplace_publish_jobs" in s]
    assert insert_item and insert_job
    # pending 同键 coalesce 用 ON CONFLICT DO UPDATE（部分唯一索引）。
    assert "ON CONFLICT (module, item_id) WHERE status='pending' DO UPDATE" in insert_job[0]


def test_is_populated_caches_and_degrades_without_pool(fake_pool):
    state: dict = {"val_result": True}
    fake_pool(state)
    assert asyncio.run(store.is_populated()) is True
    assert store._populated_cache is True  # 缓存生效
    import db

    monkey_pool = db.PostgresClient.pool
    db.PostgresClient.pool = None
    store._populated_cache = None
    try:
        assert asyncio.run(store.is_populated()) is False
    finally:
        db.PostgresClient.pool = monkey_pool


def test_claim_pending_uses_cas_with_skip_locked(fake_pool):
    state: dict = {"fetch_result": [ {"id": 7, "module": "channels", "item_id": "x",
                                     "action": "upsert", "status": "pushing",
                                     "attempts": 1, "payload": {}} ]}
    fake_pool(state)
    jobs = asyncio.run(store.claim_pending(10))
    assert len(jobs) == 1
    sql, args = state["fetch"][0]
    # CAS 认领 + SKIP LOCKED（多实例安全）双谓词。
    assert "SET status='pushing'" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "available_at <= now()" in sql
    assert args == (10,)


def test_mark_failed_backs_off_then_fails(fake_pool, monkeypatch):
    state: dict = {}
    fake_pool(state)
    # 第 4 次失败：回 pending + 指数退避。
    state["row_result"] = {"attempts": 4}
    asyncio.run(store.mark_failed([9], "network hiccup"))
    retry_sql = [sql for sql, _ in state["execute"] if "SET status='pending'" in sql]
    assert retry_sql, "attempt<MAX 应退回 pending"
    backoff_args = [args for sql, args in state["execute"] if "make_interval(secs => $3)" in sql]
    assert backoff_args and backoff_args[0][2] > 0
    # 达上限：置 failed。
    state["row_result"] = {"attempts": store.MAX_ATTEMPTS}
    asyncio.run(store.mark_failed([9], "dead"))
    fail_sql = [sql for sql, _ in state["execute"] if "status='failed'" in sql]
    assert fail_sql


def test_requeue_stale_flips_pushing_back_to_pending(fake_pool):
    state: dict = {"fetch_result": [{"id": 1}, {"id": 2}]}
    fake_pool(state)
    assert asyncio.run(store.requeue_stale()) == 2
    sql, args = state["fetch"][0][0], state["fetch"][0][1]
    assert "status='pushing'" in sql and "status='pending'" in sql
    assert args[0] == store._STALE_PUSHING_SECONDS


def test_delete_item_soft_routes_to_set_status(fake_pool):
    state: dict = {"row_result": {"id": 1, "module": "channels", "item_id": "local.agnes",
                                  "manifest": {}, "summary": {}, "status": "hidden", "revision": 2,
                                  "updated_at": None}}
    fake_pool(state)
    asyncio.run(store.delete_item("channels", "local.agnes", hard=False))
    sqls = [sql for sql, _ in state["execute"]] + [sql for sql, _ in state["fetchrow"]]
    # soft = hidden 状态（manifest/summary/行三处同步）+ upsert 入队（item 文件也要镜像）。
    assert any("jsonb_set(manifest, '{status}'" in s for s in sqls)
    assert any("jsonb_set(summary, '{status}'" in s for s in sqls), "summary 行的 status 也要同步，否则消费列表仍显示 published"
    all_args = [args for _, args in state["execute"]]
    assert any("upsert" in a for a in all_args)


def test_bootstrap_items_no_overwrite_uses_do_nothing(fake_pool):
    state: dict = {}
    fake_pool(state)
    asyncio.run(store.bootstrap_items_no_overwrite([("channels", _manifest())]))
    sqls = [sql for sql, _ in state["execute"]]
    insert = [s for s in sqls if "INSERT INTO marketplace_items" in s]
    assert insert and "ON CONFLICT (module, item_id) DO NOTHING" in insert[0]
    assert not any("marketplace_publish_jobs" in s for s in sqls), "bootstrap 不入队——仓库已是该状态"


def test_bootstrap_items_overwrite_uses_do_update(fake_pool):
    """resync 显式采纳：仓库值覆盖 store 行。"""
    state: dict = {}
    fake_pool(state)
    asyncio.run(store.bootstrap_items([("channels", _manifest())]))
    sqls = [sql for sql, _ in state["execute"]]
    insert = [s for s in sqls if "INSERT INTO marketplace_items" in s]
    assert insert and "DO UPDATE" in insert[0]


def test_is_populated_counts_bootstrap_flag(fake_pool):
    """空 store + 已 bootstrap（全部条目硬删后的场景）仍是 store 模式，不得回落仓库旧快照。"""
    state: dict = {"val_result": False}
    fake_pool(state)
    # FakeConn 返回固定值；真实谓词里 flag 与行存在做 OR——这里验证 SQL 文本含 flag 探测。
    # is_populated 直接返回 False（假值），仅断言查询发出。
    asyncio.run(store.is_populated())
    sql = state["fetchval"][0][0]
    assert "marketplace_store_bootstrapped" in sql
