"""账号删除时清理冻结关系的单测。"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import admin
from rate_limiter import ModelClientPool


def test_remove_account_from_pool_clears_all_redis_freezes(monkeypatch):
    """删除账号须清账号级 + 所有模型级 Redis 冻结键，避免同名账号重建后幽灵回灌。"""
    removed_client = SimpleNamespace(username="gone")
    kept_client = SimpleNamespace(username="keep")
    pool = SimpleNamespace(clients=[removed_client, kept_client])
    pools = ModelClientPool._provider_pools
    ModelClientPool._provider_pools = {"p1": pool}

    calls: list[tuple[str, str]] = []

    async def fake_clear(provider: str, username: str):
        calls.append((provider, username))

    async def fake_refresh():
        return None

    monkeypatch.setattr(admin.RedisLimitBackend, "clear_account_cooldown", classmethod(lambda cls, p, u: fake_clear(p, u)))
    monkeypatch.setattr(ModelClientPool, "refresh_models", classmethod(lambda cls: fake_refresh()))
    try:
        async def run():
            admin._remove_account_from_pool("p1", "gone")
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(run())
    finally:
        ModelClientPool._provider_pools = pools

    assert [client.username for client in pool.clients] == ["keep"]
    assert calls == [("p1", "gone")]
