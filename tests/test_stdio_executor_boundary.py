"""stdio MCP 执行器转发边界契约测试（本期只有协议边界，无执行器实现）。

覆盖点：
- StdioSpec 序列化（to_wire）与从 DB service 行构造（from_service，含 args 字符串解码）。
- 未配置执行器时 is_configured() 为假、_post/list_tools/call_tool 抛 ExecutorUnavailable，
  且**绝不**在本进程内拉起子进程（纯降级）。
- registry.activate_stdio 在执行器不可用时抛 PluginLoadError（调用方据此标 runtime error）。
- 执行器可达（打桩）时，activate_stdio 建出转发工具，call 经执行器往返。

纯函数 + 打桩，不连库、不起子进程、不发真实网络。
"""
import asyncio
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import mcp_runtime.stdio_executor as se
from mcp_runtime.registry import MCPRegistry


# ── StdioSpec ───────────────────────────────────────────────────────────────


def test_stdio_spec_to_wire_roundtrip():
    spec = se.StdioSpec(name="fs", command="npx", args=["-y", "server-fs"], env={"ROOT": "/tmp"})
    wire = spec.to_wire()
    assert wire == {
        "name": "fs",
        "command": "npx",
        "args": ["-y", "server-fs"],
        "env": {"ROOT": "/tmp"},
    }


def test_stdio_spec_from_service_decodes_string_args():
    service = {"name": "fs", "command": "npx", "args": '["-y","server-fs"]', "env_template": {"A": "1"}}
    spec = se.StdioSpec.from_service(service)
    assert spec.command == "npx"
    assert spec.args == ["-y", "server-fs"]
    assert spec.env == {"A": "1"}


def test_stdio_spec_from_service_env_override():
    service = {"name": "fs", "command": "npx", "args": [], "env_template": {"A": "1"}}
    spec = se.StdioSpec.from_service(service, env={"B": "2"})
    assert spec.env == {"B": "2"}


# ── 未配置执行器：纯降级，不拉起子进程 ─────────────────────────────────────────


def test_not_configured_when_env_missing(monkeypatch):
    monkeypatch.delenv("MCP_STDIO_EXECUTOR_URL", raising=False)
    monkeypatch.delenv("MCP_STDIO_EXECUTOR_TOKEN", raising=False)
    # config 兜底也读不到时视为未部署（config 读失败被吞）。
    monkeypatch.setattr(se, "_executor_settings", lambda: ("", ""))
    assert se.is_configured() is False


def test_list_tools_unavailable_when_not_configured(monkeypatch):
    monkeypatch.setattr(se, "_executor_settings", lambda: ("", ""))
    spec = se.StdioSpec(name="fs", command="npx")
    with pytest.raises(se.ExecutorUnavailable):
        asyncio.run(se.list_tools(spec))


def test_call_tool_unavailable_when_not_configured(monkeypatch):
    monkeypatch.setattr(se, "_executor_settings", lambda: ("", ""))
    spec = se.StdioSpec(name="fs", command="npx")
    with pytest.raises(se.ExecutorUnavailable):
        asyncio.run(se.call_tool(spec, "read", {"path": "/x"}))


# ── registry.activate_stdio ───────────────────────────────────────────────────


def test_activate_stdio_raises_plugin_load_error_when_executor_down(monkeypatch):
    from mcp_runtime.registry import PluginLoadError

    async def _boom(spec):
        raise se.ExecutorUnavailable("not deployed")

    monkeypatch.setattr(se, "list_tools", _boom)
    reg = MCPRegistry()
    spec = se.StdioSpec(name="fs", command="npx")
    with pytest.raises(PluginLoadError):
        asyncio.run(reg.activate_stdio("fs", spec))
    # 未部署时绝不落地任何插件
    assert reg.get("fs") is None


def test_activate_stdio_forwards_tools_when_executor_up(monkeypatch):
    calls = {}

    async def _list(spec):
        return [{"name": "read", "description": "read file", "input_schema": {"type": "object"}}]

    async def _call(spec, tool, arguments):
        calls["last"] = (spec.name, tool, arguments)
        return [{"type": "text", "text": "ok"}]

    monkeypatch.setattr(se, "list_tools", _list)
    monkeypatch.setattr(se, "call_tool", _call)

    reg = MCPRegistry()
    spec = se.StdioSpec(name="fs", command="npx")
    plugin = asyncio.run(reg.activate_stdio("fs", spec, auth_enabled=True, allowed_tokens={"t"}))
    assert plugin.tool_names() == ["read"]
    assert plugin.ctx.auth_enabled is True
    assert plugin.ctx.allowed_tokens == {"t"}

    # 工具调用经执行器转发（不在本进程执行）。
    out = asyncio.run(reg.call_tool("fs", "read", {"path": "/x"}))
    assert out == [{"type": "text", "text": "ok"}]
    assert calls["last"] == ("fs", "read", {"path": "/x"})
