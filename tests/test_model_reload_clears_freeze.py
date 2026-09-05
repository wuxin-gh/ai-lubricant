"""模型表变更清理冻结关系的单测（用户明确要求：删模型/更新模型列表都要清冻结）。

覆盖 ModelClientPool._prune_account_freezes_for_channel / reload_channel_models：
- 模型从渠道模型表删除后，账号对该模型的冻结条目被清、Redis 清理被调度。
- 仍在表内的模型冻结保留。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import rate_limiter
from rate_limiter import AccountClient, ModelClientPool


def _make_client(username: str = "u1") -> AccountClient:
    provider = SimpleNamespace(PROVIDER_NAME="p1", username=username, quotas=None)
    return AccountClient(provider, rpm_limit=0)


class _FakeChannel:
    def __init__(self):
        self.models: list[dict] = []

    def register_models(self, rows):
        self.models = [dict(r) for r in rows]


def test_reload_channel_models_clears_freeze_for_deleted_model(monkeypatch):
    client = _make_client()
    client.freeze(kind="account_model", model_id="keep", seconds=3600, reason="429")
    client.freeze(kind="account_model", model_id="gone", seconds=3600, reason="429")

    channel = _FakeChannel()
    pool = SimpleNamespace(clients=[client], channel=channel)

    cleared: list[tuple] = []

    async def _fake_clear(provider, username, model):
        cleared.append((provider, username, model))

    monkeypatch.setattr(rate_limiter.RedisLimitBackend, "clear_account_model_cooldown", classmethod(lambda cls, p, u, m: _fake_clear(p, u, m)))

    pools = ModelClientPool._provider_pools
    ModelClientPool._provider_pools = {"p1": pool}
    try:
        async def _run():
            # 新模型表只保留 keep，gone 被删。
            ModelClientPool.reload_channel_models("p1", [{"model_id": "keep", "upstream_model_id": "keep"}])
            # 让被调度的 Redis 清理任务跑完。
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(_run())
    finally:
        ModelClientPool._provider_pools = pools

    # gone 的冻结被清，keep 保留。
    assert client.frozen_model_ids() == ["keep"]
    assert ("p1", "u1", "gone") in cleared
    assert ("p1", "u1", "keep") not in cleared


def test_prune_account_freezes_noop_when_all_models_present(monkeypatch):
    client = _make_client()
    client.freeze(kind="account_model", model_id="m1", seconds=3600, reason="429")
    channel = _FakeChannel()
    channel.register_models([{"model_id": "m1", "upstream_model_id": "m1"}])
    pool = SimpleNamespace(clients=[client], channel=channel)

    cleared: list = []

    async def _fake_clear(provider, username, model):
        cleared.append((provider, username, model))

    monkeypatch.setattr(rate_limiter.RedisLimitBackend, "clear_account_model_cooldown", classmethod(lambda cls, p, u, m: _fake_clear(p, u, m)))

    async def _run():
        ModelClientPool._prune_account_freezes_for_channel("p1", pool, channel)
        await asyncio.sleep(0)

    asyncio.run(_run())
    assert client.frozen_model_ids() == ["m1"]
    assert cleared == []
