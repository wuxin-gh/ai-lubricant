"""Regression tests for the official redis-py async connection pool."""
import asyncio

import pytest
from redis.asyncio import Redis
from redis.asyncio.connection import BlockingConnectionPool

from rd import JdbcClient, RedisJdbc


def _redis(max_connections=2, pool_timeout=0.1):
    return RedisJdbc(
        host="127.0.0.1", port=6379, max_connections=max_connections,
        prefix_key="test", connect_timeout=1, stream_timeout=1,
        pool_timeout=pool_timeout,
    )


def test_uses_official_bounded_pool():
    redis = _redis(max_connections=3)
    try:
        pool = redis.connection_pool
        assert isinstance(pool, BlockingConnectionPool)
        assert pool.max_connections == 3
        assert pool.timeout == 0.1
    finally:
        asyncio.run(redis.aclose())


@pytest.mark.asyncio
async def test_waiter_cancel_does_not_block_future_acquisition(monkeypatch):
    redis = _redis(max_connections=1, pool_timeout=1)
    pool = redis.connection_pool
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fake_connect(connection):
        entered.set()
        await release.wait()

    monkeypatch.setattr(pool, "ensure_connection", fake_connect)
    first = asyncio.create_task(pool.get_connection("GET"))
    await entered.wait()

    waiter = asyncio.create_task(pool.get_connection("GET"))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    release.set()
    connection = await first
    await pool.release(connection)
    await redis.aclose()


@pytest.mark.asyncio
async def test_acquire_releases_connection_on_cancel(monkeypatch):
    redis = _redis(max_connections=1)
    pool = redis.connection_pool
    connection = object()
    released = []

    async def fake_get_connection(*_args, **_kwargs):
        return connection

    async def fake_release(value):
        released.append(value)

    monkeypatch.setattr(pool, "get_connection", fake_get_connection)
    monkeypatch.setattr(pool, "release", fake_release)

    async def use_connection():
        connection = await pool.get_connection("GET")
        try:
            await asyncio.sleep(60)
        finally:
            await pool.release(connection)

    task = asyncio.create_task(use_connection())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert released == [connection]
    await redis.aclose()


def test_pool_snapshot_does_not_depend_on_coredis_internals():
    redis = _redis(max_connections=4)
    JdbcClient.redis = redis
    try:
        snapshot = JdbcClient.pool_snapshot()
        assert snapshot["max"] == 4
        assert snapshot["constructing"] == 0
        assert snapshot["deficit"] == 0
    finally:
        JdbcClient.redis = None
        asyncio.run(redis.aclose())


@pytest.mark.asyncio
async def test_eval_adapter_translates_keys_and_args(monkeypatch):
    redis = _redis()
    captured = {}

    async def fake_eval(self, script, numkeys, *keys_and_args):
        captured["value"] = (script, numkeys, keys_and_args)
        return [1, "ok"]

    monkeypatch.setattr(Redis, "eval", fake_eval)
    result = await redis.eval("return 1", keys=["k1", "k2"], args=["a1"])
    assert result == [1, "ok"]
    assert captured["value"] == ("return 1", 2, ("k1", "k2", "a1"))
    await redis.aclose()
