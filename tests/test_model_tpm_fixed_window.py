import asyncio
import time
from types import SimpleNamespace

import pytest

import config
from limits.backend import RedisLimitBackend
from limits.manager import LimitManager
from rate_limiter import ModelClientPool
import runtime_handlers
import runtime_sync


@pytest.fixture(autouse=True)
def _reset_model_tpm_state():
    ModelClientPool._model_tpm_cooldowns.clear()
    yield
    ModelClientPool._model_tpm_cooldowns.clear()


@pytest.mark.asyncio
async def test_model_tpm_selection_is_memory_only(monkeypatch):
    async def _redis_must_not_run(*args, **kwargs):
        raise AssertionError("selection must not query Redis")

    monkeypatch.setattr(RedisLimitBackend, "get_window", classmethod(_redis_must_not_run))
    monkeypatch.setattr(RedisLimitBackend, "sum_events", classmethod(_redis_must_not_run), raising=False)

    assert await ModelClientPool._model_tpm_available("m1") is True
    ModelClientPool._model_tpm_cooldowns["m1"] = time.time() + 30
    assert await ModelClientPool._model_tpm_available("m1") is False


@pytest.mark.asyncio
async def test_model_tpm_record_uses_fixed_window_backend(monkeypatch):
    calls = []

    async def _record(cls, model_id, tokens, ttl_seconds=60):
        calls.append((model_id, tokens, ttl_seconds))
        return 120, 41

    monkeypatch.setattr(RedisLimitBackend, "record_model_tpm_window", classmethod(_record))
    result = await LimitManager.record_model_tokens("m1", 25)

    assert result == (120, 41)
    assert calls == [("m1", 25, 60)]


@pytest.mark.asyncio
async def test_record_token_usage_freezes_model_and_broadcasts(monkeypatch):
    pool = SimpleNamespace(clients=[])
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_limit", classmethod(lambda cls, model: 100))

    async def _record(cls, model_id, tokens):
        return 125, 37

    published = []

    async def _publish(event_type, name=None, version=None, extra=None):
        published.append((event_type, name, extra))

    monkeypatch.setattr(LimitManager, "record_model_tokens", classmethod(_record))
    monkeypatch.setattr(runtime_sync, "publish", _publish)

    await ModelClientPool.record_token_usage("m1", "p1", "u1", 25)
    await asyncio.sleep(0)

    assert ModelClientPool._model_tpm_cooldowns["m1"] > time.time() + 30
    assert published[0][0] == runtime_sync.EVENT_COOLDOWN
    assert published[0][1] == "model-tpm:m1"
    assert published[0][2]["scope"] == "model_tpm"
    assert published[0][2]["used"] == 125


@pytest.mark.asyncio
async def test_model_tpm_pubsub_event_updates_memory():
    until = time.time() + 45
    await runtime_handlers._on_cooldown_event(
        "model-tpm:m1",
        {"extra": {"scope": "model_tpm", "model_id": "m1", "until": until}},
    )
    assert ModelClientPool._model_tpm_cooldowns["m1"] == until


@pytest.mark.asyncio
async def test_restore_model_tpm_cooldown_uses_runtime_limit(monkeypatch):
    async def _windows(cls):
        return [("limited", 120, 25), ("below", 80, 25), ("unlimited", 999, 25)]

    monkeypatch.setattr(RedisLimitBackend, "list_model_tpm_windows", classmethod(_windows))
    monkeypatch.setattr(
        ModelClientPool,
        "_model_tpm_limit",
        classmethod(lambda cls, model: {"limited": 100, "below": 100, "unlimited": 0}[model]),
    )

    await ModelClientPool.restore_model_tpm_cooldowns()

    assert ModelClientPool._is_model_tpm_cooling("limited")
    assert not ModelClientPool._is_model_tpm_cooling("below")
    assert not ModelClientPool._is_model_tpm_cooling("unlimited")



@pytest.mark.asyncio
async def test_model_tpm_lua_window_is_atomic_under_concurrency(monkeypatch):
    class FakeRedis:
        prefix_key = "test"

        def __init__(self):
            self.value = 0
            self.ttl = None
            self.expire_calls = 0
            self.lock = asyncio.Lock()

        async def eval(self, script, keys=None, args=None):
            assert "INCRBY" in script
            key = keys[0]
            amount, ttl_seconds = args
            async with self.lock:
                await asyncio.sleep(0)
                self.value += int(amount)
                if self.value == int(amount):
                    self.ttl = int(ttl_seconds)
                    self.expire_calls += 1
                return [self.value, self.ttl]

    redis = FakeRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: redis))

    results = await asyncio.gather(*(
        RedisLimitBackend.record_model_tpm_window("m1", 7, 60)
        for _ in range(100)
    ))

    assert redis.value == 700
    assert redis.expire_calls == 1
    assert sorted(value for value, _ in results) == list(range(7, 701, 7))
    assert {ttl for _, ttl in results} == {60}


def test_get_providers_snapshot_reads_underlying_store_cache(monkeypatch):
    snapshot = {"p1": {"rate_limit": {}}}
    monkeypatch.setattr(config.CONFIG_STORE.store, "_providers_cache", snapshot)
    assert config.Config.get_providers_snapshot() is snapshot


@pytest.mark.asyncio
async def test_get_providers_uses_memory_cache_without_database(monkeypatch):
    snapshot = {"p1": {"rate_limit": {}}}
    monkeypatch.setattr(config.CONFIG_STORE.store, "_providers_cache", snapshot)

    async def _must_not_reload():
        raise AssertionError("warm provider cache must not reload PostgreSQL")

    monkeypatch.setattr(config.CONFIG_STORE, "list_providers_async", _must_not_reload)
    assert await config.Config.get_providers() is snapshot


def test_model_tpm_limit_reads_memory_snapshots(monkeypatch):
    monkeypatch.setattr(ModelClientPool, "get_model_route_providers", classmethod(lambda cls, model: ["p1", "p2"]))
    monkeypatch.setattr(config.CONFIG_STORE.store, "_providers_cache", {
        "p1": {"rate_limit": {"tpm_per_model": 100}},
        "p2": {"rate_limit": {"tpm_per_model": 200}},
    })
    monkeypatch.setattr(
        "rate_limiter.get_effective_provider_policy_sync",
        lambda provider: {"model_tpm": 150 if provider == "p1" else 0},
    )

    assert ModelClientPool._model_tpm_limit("m1") == 200
