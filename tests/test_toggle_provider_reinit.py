"""toggle_provider 重新启用触发 init_all + check_account 单测。

覆盖修复：渠道从禁用改启用后，账号不应停在「待检查」（auth_ok=None）。
后台任务必须先 init_all 补 init_auth，再 check_account 体检。
"""
from __future__ import annotations

import asyncio

import pytest

import admin


class _FakePool:
    def __init__(self):
        self.calls: list[str] = []
        self.channel = None

    async def init_all(self, *, use_invitation_interval: bool = True):
        self.calls.append(f"init_all(use_invitation_interval={use_invitation_interval})")

    async def check_account(self, *, force: bool = False):
        self.calls.append(f"check_account(force={force})")


@pytest.fixture
def _patch_env(monkeypatch):
    pool = _FakePool()

    async def _require_admin(token):
        return None

    async def _read_provider_config(name):
        return {"enabled": False}  # was disabled

    async def _write_provider_base(name, cfg):
        return None

    async def _log_operation(*args, **kwargs):
        return None

    def _provider_extra(name, cfg):
        return {"provider_name": name, **cfg}

    monkeypatch.setattr(admin, "_require_admin", _require_admin)
    monkeypatch.setattr(admin, "_read_provider_config", _read_provider_config)
    monkeypatch.setattr(admin, "_write_provider_base", _write_provider_base)
    monkeypatch.setattr(admin, "_log_operation", _log_operation)
    monkeypatch.setattr(admin, "_provider_extra", _provider_extra)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    created_tasks: list = []
    real_create_task = asyncio.create_task

    def _tracking_create_task(coro):
        t = real_create_task(coro)
        created_tasks.append(t)
        return t

    monkeypatch.setattr(admin.asyncio, "create_task", _tracking_create_task)
    yield pool, created_tasks


def test_reenable_runs_init_all_then_check_account(_patch_env):
    pool, created = _patch_env
    asyncio.run(admin.toggle_provider("p1", enabled=True))

    async def _drain():
        for t in created:
            await t

    asyncio.run(_drain())
    # 必须先 init_all（补 init_auth）再 check_account（体检），顺序固定。
    assert len(pool.calls) == 2
    assert pool.calls[0].startswith("init_all(")
    assert "use_invitation_interval=False" in pool.calls[0]
    assert pool.calls[1].startswith("check_account(")
    assert "force=True" in pool.calls[1]


def test_reenable_to_disabled_does_not_spawn_task(_patch_env):
    pool, created = _patch_env
    # was disabled -> enabled=False：不满足 not was_enabled and enabled，不应起后台任务。
    asyncio.run(admin.toggle_provider("p1", enabled=False))
    assert created == []
    assert pool.calls == []
