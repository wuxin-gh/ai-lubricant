from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import admin


async def _allow_admin(*_args, **_kwargs):
    return "admin"


def test_check_provider_accounts_awaits_forced_check(monkeypatch):
    calls: list[bool] = []

    class Pool:
        async def check_account(self, *, force: bool = False):
            await asyncio.sleep(0)
            calls.append(force)

    monkeypatch.setattr(admin, "_require_admin", _allow_admin)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda _name: Pool())

    result = asyncio.run(admin.check_provider_accounts("demo", "Bearer token"))

    assert result == {"ok": True, "message": "账号状态检查已完成"}
    assert calls == [True]


def test_check_provider_accounts_preserves_missing_channel_response(monkeypatch):
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda _name: None)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(admin.check_provider_accounts("missing", "Bearer token"))

    assert exc_info.value.status_code == 404


@pytest.mark.parametrize(
    ("old_enabled", "new_enabled", "expected_tasks"),
    [(False, True, 1), (True, False, 0), (True, True, 0)],
)
def test_toggle_provider_rechecks_only_on_reenable(
    monkeypatch, old_enabled: bool, new_enabled: bool, expected_tasks: int
):
    state = {"enabled": old_enabled}
    scheduled_states: list[bool] = []

    class Channel:
        enabled = old_enabled

        def update(self, cfg):
            self.enabled = cfg.get("enabled", True) is not False

    pool = SimpleNamespace(channel=Channel())

    async def read_config(_name):
        return dict(state)

    async def write_config(_name, cfg):
        state.update(cfg)

    async def log_operation(*_args, **_kwargs):
        return None

    def create_task(coro):
        scheduled_states.append(pool.channel.enabled)
        coro.close()
        return SimpleNamespace()

    monkeypatch.setattr(admin, "_require_admin", _allow_admin)
    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_write_provider_base", write_config)
    monkeypatch.setattr(admin, "_provider_extra", lambda _name, cfg: cfg)
    monkeypatch.setattr(admin, "_log_operation", log_operation)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda _name: pool)
    monkeypatch.setattr(admin.asyncio, "create_task", create_task)

    result = asyncio.run(admin.toggle_provider("demo", new_enabled, "Bearer token"))

    assert result == {"ok": True}
    assert state["enabled"] is new_enabled
    assert pool.channel.enabled is new_enabled
    assert len(scheduled_states) == expected_tasks
    assert all(scheduled_states)
