from __future__ import annotations

import asyncio

import pytest

import config
from providers.custom import CustomProvider
from rate_limiter import ModelClientPool


_TASK_ATTRS = (
    "_account_init_task",
    "_refresh_task",
    "_delete_task",
    "_clean_response_task",
    "_check_task",
    "_scheduled_test_task",
    "_hourly_stats_task",
)


@pytest.fixture(autouse=True)
def _restore_model_client_pool_state():
    pools = ModelClientPool._provider_pools
    task_values = {name: getattr(ModelClientPool, name) for name in _TASK_ATTRS}
    ModelClientPool._provider_pools = {}
    for name in _TASK_ATTRS:
        setattr(ModelClientPool, name, None)
    yield
    ModelClientPool._provider_pools = pools
    for name, value in task_values.items():
        setattr(ModelClientPool, name, value)


def test_initialize_orders_custom_before_builtin_without_blocking(monkeypatch):
    events: list[str] = []
    builtin_started = asyncio.Event()
    release_builtin = asyncio.Event()

    class BuiltinProvider:
        pass

    class FakePool:
        def __init__(self, name, client_class):
            self.name = name
            self.client_class = client_class
            self.clients = []

        async def restore_cooldowns(self):
            events.append(f"restore:{self.name}")

        async def init_all(self, *, use_invitation_interval=True):
            events.append(f"start:{self.name}:{use_invitation_interval}")
            if self.client_class is BuiltinProvider:
                builtin_started.set()
                await release_builtin.wait()
            events.append(f"done:{self.name}")

    async def blocked_loop():
        await asyncio.Event().wait()

    async def run():
        custom = FakePool("custom", CustomProvider)
        builtin = FakePool("builtin", BuiltinProvider)
        ModelClientPool._provider_pools = {"builtin": builtin, "custom": custom}

        monkeypatch.setattr(ModelClientPool, "restore_model_tpm_cooldowns", blocked_noop)
        monkeypatch.setattr(ModelClientPool, "clean_message", blocked_noop)
        hydrate_calls = []

        async def hydrate_models(*, sync_upstream=True):
            hydrate_calls.append(sync_upstream)

        monkeypatch.setattr(ModelClientPool, "refresh_models", hydrate_models)
        monkeypatch.setattr(ModelClientPool, "refresh_models_loop", blocked_loop)
        monkeypatch.setattr(ModelClientPool, "delete_message_loop", blocked_loop)
        monkeypatch.setattr(ModelClientPool, "clean_response_loop", blocked_loop)
        monkeypatch.setattr(ModelClientPool, "check_account_loop", blocked_loop)
        monkeypatch.setattr(ModelClientPool, "_hourly_stats_loop", blocked_loop)
        monkeypatch.setattr(config.Config, "message_delete_enabled", staticmethod(lambda: False))

        await asyncio.wait_for(ModelClientPool.initialize(), timeout=0.1)
        await asyncio.wait_for(builtin_started.wait(), timeout=0.1)

        assert events.index("done:custom") < events.index("start:builtin:True")
        assert "start:custom:False" in events
        assert hydrate_calls == [False]
        assert ModelClientPool._account_init_task is not None
        assert not ModelClientPool._account_init_task.done()
        assert ModelClientPool._refresh_task is not None
        assert ModelClientPool._check_task is not None

        release_builtin.set()
        await ModelClientPool.stop()

    async def blocked_noop():
        return None

    asyncio.run(run())


def test_refresh_models_loop_runs_immediately_then_sleeps(monkeypatch):
    refresh_called = asyncio.Event()
    sleep_started = asyncio.Event()
    calls = 0

    async def refresh_models():
        nonlocal calls
        calls += 1
        refresh_called.set()

    async def controlled_sleep(delay):
        assert delay == 17 * 60
        sleep_started.set()
        await asyncio.Event().wait()

    async def run():
        monkeypatch.setattr(ModelClientPool, "refresh_models", refresh_models)
        monkeypatch.setattr(config.Config, "model_refresh_interval", staticmethod(lambda: 17))
        monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

        task = asyncio.create_task(ModelClientPool.refresh_models_loop())
        await asyncio.wait_for(refresh_called.wait(), timeout=0.1)
        await asyncio.wait_for(sleep_started.wait(), timeout=0.1)
        assert calls == 1

        task.cancel()
        await task
        assert calls == 1

    asyncio.run(run())


def test_refresh_models_loop_sleeps_after_failed_refresh(monkeypatch):
    sleep_started = asyncio.Event()
    calls = 0

    async def refresh_models():
        nonlocal calls
        calls += 1
        raise RuntimeError("upstream unavailable")

    async def controlled_sleep(delay):
        sleep_started.set()
        await asyncio.Event().wait()

    async def run():
        monkeypatch.setattr(ModelClientPool, "refresh_models", refresh_models)
        monkeypatch.setattr(config.Config, "model_refresh_interval", staticmethod(lambda: 1))
        monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

        task = asyncio.create_task(ModelClientPool.refresh_models_loop())
        await asyncio.wait_for(sleep_started.wait(), timeout=0.1)
        assert calls == 1

        task.cancel()
        await task
        assert calls == 1

    asyncio.run(run())
