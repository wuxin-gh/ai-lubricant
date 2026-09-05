"""MCP runtime startup lifecycle regression tests."""
import asyncio

import mcp_runtime.startup as startup


class _StartupRegistry:
    def __init__(self):
        self.internal_calls = 0
        self.configurable_snapshots = None

    async def register_internal_builtins(self):
        self.internal_calls += 1
        return {"issue-workflow": object()}

    @staticmethod
    def is_internal_builtin(name):
        return name == "issue-workflow"

    async def register_builtins(self, snapshots, *, configured_only=False):
        self.configurable_snapshots = snapshots
        assert configured_only is True
        return {}


async def _noop(*_args, **_kwargs):
    return None


def _install_store(monkeypatch, services):
    async def list_services():
        return list(services)

    monkeypatch.setattr(startup.mcp_plugin_store, "list_services", list_services)
    monkeypatch.setattr(startup.mcp_plugin_store, "mark_runtime_error", _noop)
    monkeypatch.setattr(startup.mcp_plugin_store, "mark_runtime_status", _noop)


def test_startup_registers_internal_builtin_without_persisted_services(monkeypatch):
    fake_registry = _StartupRegistry()
    _install_store(monkeypatch, [])
    monkeypatch.setattr(startup, "registry", fake_registry)

    asyncio.run(startup.restore_active_plugins())

    assert fake_registry.internal_calls == 1
    assert fake_registry.configurable_snapshots == {}


def test_startup_ignores_legacy_internal_builtin_row(monkeypatch):
    fake_registry = _StartupRegistry()
    legacy_row = {
        "id": 17,
        "name": "issue-workflow",
        "kind": "builtin",
        "builtin": True,
        "enabled": False,
    }
    _install_store(monkeypatch, [legacy_row])
    monkeypatch.setattr(startup, "registry", fake_registry)

    async def unexpected_snapshot(_service):
        raise AssertionError("internal builtin must not build a persisted snapshot")

    monkeypatch.setattr(startup, "build_builtin_snapshot", unexpected_snapshot)

    asyncio.run(startup.restore_active_plugins())

    assert fake_registry.internal_calls == 1
    assert fake_registry.configurable_snapshots == {}
