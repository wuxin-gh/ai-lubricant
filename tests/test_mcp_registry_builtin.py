"""内置 MCP 常驻加载 + 配置下发契约测试。

覆盖本次「内置一体化」重构的关键契约：
- register_builtins()：boot 时把内置 adapter 注册进注册表（常驻，不看 enabled）。
- update_builtin_config()：原地更新 PluginContext 鉴权快照并调 adapter.apply_config，
  **不重建 LoadedPlugin / 工具引用**（同一实例，现有连接不受影响）。
- 未注册时 update_builtin_config 退化为首次注册。

用假 adapter 隔离 cdp-bridge 的可选浏览器依赖（simple_websocket_server / bottle / bs4）。
"""
import asyncio
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import mcp_runtime.registry as registry_mod
from mcp_runtime.registry import MCPRegistry


class _FakeAdapter:
    """最小内置 adapter：register 登记一个工具，apply_config 记录收到的 ctx。"""

    def __init__(self):
        self.applied = []

    def register(self, reg):
        @reg.tool(name="fake_tool", description="fake", params={"type": "object", "properties": {}})
        async def _fake(args, ctx):  # noqa: ANN001
            return {"ok": True}

    def apply_config(self, ctx):
        self.applied.append({
            "auth_enabled": ctx.auth_enabled,
            "allowed_tokens": set(ctx.allowed_tokens),
            "resources": dict(ctx.resources),
        })


@pytest.fixture
def fake_registry(monkeypatch):
    adapter = _FakeAdapter()
    monkeypatch.setattr(registry_mod, "_BUILTIN_ADAPTERS", {"fake-builtin": adapter})
    monkeypatch.setattr(registry_mod, "_INTERNAL_BUILTIN_ADAPTERS", {})
    return MCPRegistry(), adapter


def test_register_builtins_makes_plugin_resident(fake_registry):
    reg, _adapter = fake_registry
    out = asyncio.run(reg.register_builtins())
    assert "fake-builtin" in out
    plugin = reg.get("fake-builtin")
    assert plugin is not None
    assert plugin.tool_names() == ["fake_tool"]


def test_update_builtin_config_in_place_same_instance(fake_registry):
    reg, adapter = fake_registry

    async def _run():
        await reg.register_builtins()
        before = reg.get("fake-builtin")
        before_tool = before.tools[0]
        updated = await reg.update_builtin_config(
            "fake-builtin",
            auth_enabled=True,
            allowed_tokens={"token-a"},
            resources={"clients": [{"instance_key": "client-1", "user_id": 1}]},
        )
        return before, before_tool, updated

    before, before_tool, updated = asyncio.run(_run())
    # 同一 LoadedPlugin 实例、同一工具引用 —— 不重建、现有连接不受影响
    assert updated is before
    assert updated.tools[0] is before_tool
    # ctx 鉴权快照原地生效
    assert updated.ctx.auth_enabled is True
    assert updated.ctx.allowed_tokens == {"token-a"}
    assert updated.ctx.resources == {"clients": [{"instance_key": "client-1", "user_id": 1}]}
    # adapter.apply_config 收到最新配置
    assert adapter.applied and adapter.applied[-1]["auth_enabled"] is True
    assert adapter.applied[-1]["allowed_tokens"] == {"token-a"}


def test_update_builtin_config_falls_back_to_register(fake_registry):
    """未注册时 update_builtin_config 退化为首次注册。"""
    reg, _adapter = fake_registry
    plugin = asyncio.run(reg.update_builtin_config("fake-builtin", auth_enabled=True, allowed_tokens={"t"}))
    assert plugin is not None
    assert reg.get("fake-builtin") is plugin
    assert plugin.ctx.auth_enabled is True


def test_configured_only_skips_builtin_without_snapshot(fake_registry):
    reg, _adapter = fake_registry
    assert asyncio.run(reg.register_builtins(configured_only=True)) == {}
    assert reg.get("fake-builtin") is None


def test_issue_workflow_is_internal_and_requires_no_snapshot():
    reg = MCPRegistry()

    out = asyncio.run(reg.register_internal_builtins())

    plugin = out["issue-workflow"]
    assert plugin.tool_names() == [
        "get_issue_context",
        "update_issue_content",
        "update_issue_status",
        "add_issue_note",
    ]
    assert plugin.ctx.enabled is True
    assert plugin.ctx.auth_enabled is True


def test_register_internal_builtin_without_snapshot(monkeypatch):
    adapter = _FakeAdapter()
    monkeypatch.setattr(registry_mod, "_BUILTIN_ADAPTERS", {})
    monkeypatch.setattr(registry_mod, "_INTERNAL_BUILTIN_ADAPTERS", {"internal": adapter})
    reg = MCPRegistry()

    out = asyncio.run(reg.register_internal_builtins())

    plugin = out["internal"]
    assert reg.get("internal") is plugin
    assert plugin.ctx.enabled is True
    assert plugin.ctx.auth_enabled is True
    assert plugin.ctx.allowed_tokens == set()
    assert reg.is_internal_builtin("internal") is True


def test_internal_builtin_is_not_configurable(monkeypatch):
    adapter = _FakeAdapter()
    monkeypatch.setattr(registry_mod, "_BUILTIN_ADAPTERS", {})
    monkeypatch.setattr(registry_mod, "_INTERNAL_BUILTIN_ADAPTERS", {"internal": adapter})
    reg = MCPRegistry()
    asyncio.run(reg.register_internal_builtins())

    with pytest.raises(registry_mod.PluginLoadError, match="has no configurable snapshot"):
        asyncio.run(reg.update_builtin_config("internal", enabled=False, auth_enabled=False))

    plugin = reg.get("internal")
    assert plugin.ctx.enabled is True
    assert plugin.ctx.auth_enabled is True
