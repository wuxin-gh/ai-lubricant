"""外部 SSE MCP 服务运行时单测：activate_sse 工具发现 + 直连调用 + 失败降级。

验证合并后的数据层（kind=sse 走 registry.activate_sse → SSEClient 直连远端）：
  - 工具发现成功 → LoadedPlugin 注册 + tools_cache 写回。
  - tools/call 经进程内 handler 直调 SSEClient，返回 content 列表。
  - 远端不可达/鉴权失败 → PluginLoadError，上层降级为工具报错不阻断。
  - SSEClient 的 endpoint 握手 / initialize / notifications/initialized 流程正确。
"""
import asyncio
import json
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from mcp_runtime.sse_client import SSEClient, SSEClientError  # noqa: E402
from mcp_runtime.registry import registry, PluginLoadError  # noqa: E402


# ── SSEClient：用 fake stream 验证握手 + RPC 流程 ───────────────────────────

class _FakeStream:
    """模拟 aiohttp SSE 响应：按预置事件列表逐行吐出。"""

    def __init__(self, events: list[tuple[str, str]]):
        # events: [(event_name, data_json_str), ...]
        lines: list[bytes] = []
        for name, data in events:
            lines.append(f"event: {name}\n".encode())
            lines.append(f"data: {data}\n".encode())
            lines.append(b"\n")  # 空行分隔
        self._lines = lines
        self.url = "http://fake/sse"
        self.status = 200

    @property
    def content(self):
        async def gen():
            for line in self._lines:
                yield line
        return gen()

    async def text(self):
        return ""

    async def release(self):
        return None

    def close(self):
        return None


class _FakePostResponse:
    status = 200

    async def text(self):
        return ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None


class _FakeSession:
    """模拟 aiohttp.ClientSession：get 返回 _FakeStream，post 返回 async-cm 响应。"""

    def __init__(self, stream: _FakeStream):
        self._stream = stream
        self.posts: list[tuple[str, dict]] = []
        self.gets: list[tuple[str, dict | None]] = []
        self._post_resp = _FakePostResponse()

    async def get(self, url, headers=None):
        self.gets.append((url, headers))
        return self._stream

    def post(self, url, headers=None, json=None):
        self.posts.append((url, json or {}))
        return self._post_resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None


def _build_stream_events(messages_url: str = "http://fake/messages"):
    """构造一次完整 SSE 握手 + tools/list 响应的事件序列。"""
    return [
        ("endpoint", messages_url),
        ("message", json.dumps({
            "jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2024-11-05"},
        })),
        ("message", json.dumps({
            "jsonrpc": "2.0", "id": 2,
            "result": {"tools": [
                {"name": "search", "description": "search the web",
                 "inputSchema": {"type": "object", "properties": {"q": {"type": "string"}}}},
                {"name": "fetch", "description": "fetch a url", "inputSchema": {"type": "object"}},
            ]},
        })),
    ]


@pytest.mark.asyncio
async def test_sse_client_list_tools_runs_full_handshake(monkeypatch):
    """list_tools 完成 endpoint→initialize→notifications/initialized→tools/list 全流程。"""
    stream = _FakeStream(_build_stream_events())
    session = _FakeSession(stream)

    import aiohttp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **kw: session)

    client = SSEClient("http://fake/sse", {"Authorization": "Bearer tok"})
    tools = await client.list_tools()

    assert len(tools) == 2
    assert tools[0]["name"] == "search"
    # 三次 POST：initialize、notifications/initialized、tools/list
    methods = [p[1].get("method") for p in session.posts]
    assert methods == ["initialize", "notifications/initialized", "tools/list"]
    # 鉴权头透传到 SSE GET 与每次 POST
    assert client.headers == {"Authorization": "Bearer tok"}


@pytest.mark.asyncio
async def test_sse_client_call_tool_returns_content_list(monkeypatch):
    """call_tool 返回 result.content 列表（与进程内网关 ToolResult 形状一致）。"""
    # 一次 _rpc 只用一个 stream：endpoint → init(id=1) → tools/call(id=2 content)。
    events = [
        ("endpoint", "http://fake/messages"),
        ("message", json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2024-11-05"}})),
        ("message", json.dumps({
            "jsonrpc": "2.0", "id": 2,
            "result": {"content": [{"type": "text", "text": "result text"}]},
        })),
    ]
    stream = _FakeStream(events)
    session = _FakeSession(stream)
    import aiohttp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **kw: session)

    client = SSEClient("http://fake/sse")
    content = await client.call_tool("search", {"q": "x"})
    assert content == [{"type": "text", "text": "result text"}]
    # 最后一次 POST 应为 tools/call
    assert session.posts[-1][1]["method"] == "tools/call"
    assert session.posts[-1][1]["params"] == {"name": "search", "arguments": {"q": "x"}}


@pytest.mark.asyncio
async def test_sse_client_raises_on_missing_endpoint(monkeypatch):
    """SSE 流未给出 endpoint event → SSEClientError（上层降级为 PluginLoadError）。"""
    stream = _FakeStream([
        ("message", json.dumps({"jsonrpc": "2.0", "id": 1, "result": {}})),
    ])
    session = _FakeSession(stream)
    import aiohttp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **kw: session)

    client = SSEClient("http://fake/sse")
    with pytest.raises(SSEClientError, match="endpoint"):
        await client.list_tools()


@pytest.mark.asyncio
async def test_sse_client_raises_on_jsonrpc_error(monkeypatch):
    """远端返回 JSON-RPC error → SSEClientError。"""
    events = [
        ("endpoint", "http://fake/messages"),
        ("message", json.dumps({"jsonrpc": "2.0", "id": 1, "result": {}})),
        ("message", json.dumps({
            "jsonrpc": "2.0", "id": 2,
            "error": {"code": -32000, "message": "unauthorized"},
        })),
    ]
    stream = _FakeStream(events)
    session = _FakeSession(stream)
    import aiohttp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **kw: session)

    client = SSEClient("http://fake/sse")
    with pytest.raises(SSEClientError, match="unauthorized"):
        await client.list_tools()


# ── registry.activate_sse：发现失败 → PluginLoadError ──────────────────────

@pytest.mark.asyncio
async def test_activate_sse_raises_plugin_load_error_on_discovery_failure(monkeypatch):
    """SSEClient.list_tools 抛 SSEClientError → activate_sse 转 PluginLoadError，不注册。"""
    async def boom(self):
        raise SSEClientError("connection refused")
    monkeypatch.setattr(SSEClient, "list_tools", boom)

    # 清理同名残留注册，避免污染断言
    try:
        await registry.deactivate("sse-fail")
    except Exception:
        pass

    with pytest.raises(PluginLoadError, match="sse 'sse-fail'"):
        await registry.activate_sse("sse-fail", "http://unreachable/sse")

    assert "sse-fail" not in registry._plugins  # noqa: SLF001 — 直查内部状态断言未注册


@pytest.mark.asyncio
async def test_activate_sse_registers_tools_and_routes_calls(monkeypatch):
    """发现成功 → LoadedPlugin 注册；call_tool 经 handler 直调 SSEClient。"""
    discovered = [
        {"name": "ping", "description": "ping tool", "inputSchema": {"type": "object"}},
    ]

    async def fake_list_tools(self):
        return discovered
    monkeypatch.setattr(SSEClient, "list_tools", fake_list_tools)

    async def fake_call_tool(self, name, args):
        assert name == "ping"
        assert args == {"host": "1.1.1.1"}
        return [{"type": "text", "text": "pong"}]
    monkeypatch.setattr(SSEClient, "call_tool", fake_call_tool)

    try:
        plugin = await registry.activate_sse(
            "sse-ok", "http://fake/sse",
            headers={"Authorization": "Bearer t"},
        )
        assert plugin.tool_names() == ["ping"]

        result = await registry.call_tool("sse-ok", "ping", {"host": "1.1.1.1"})
        assert result == [{"type": "text", "text": "pong"}]
    finally:
        await registry.deactivate("sse-ok")


# ── 用户侧 MCP：自定义 headers / 请求参数 / 脱敏 ─────────────────────────────


@pytest.mark.asyncio
async def test_sse_client_passes_custom_headers_and_query_url(monkeypatch):
    """自定义 headers 与 URL query 应透传到 SSE GET。"""
    stream = _FakeStream(_build_stream_events())
    session = _FakeSession(stream)
    import aiohttp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **kw: session)

    client = SSEClient(
        "http://fake/sse?foo=bar",
        {"Authorization": "Bearer tok", "X-Api-Key": "secret-key"},
    )
    await client.list_tools()

    assert session.gets, "SSE GET 应被调用"
    url, headers = session.gets[0]
    assert url == "http://fake/sse?foo=bar"
    assert headers is not None
    assert headers.get("Authorization") == "Bearer tok"
    assert headers.get("X-Api-Key") == "secret-key"


def test_merge_service_headers_create_merges_custom_and_bearer():
    from mcp.api import _merge_service_headers
    merged = _merge_service_headers(
        "plain-token",
        {"X-Api-Key": "abc", "X-Trace": "keep"},
    )
    assert merged == {
        "Authorization": "Bearer plain-token",
        "X-Api-Key": "abc",
        "X-Trace": "keep",
    }


def test_merge_service_headers_custom_authorization_overrides_token():
    from mcp.api import _merge_service_headers
    merged = _merge_service_headers(
        "plain-token",
        {"Authorization": "Custom xyz"},
    )
    # 显式 Authorization 优先，token 让位。
    assert merged == {"Authorization": "Custom xyz"}


def test_merge_service_headers_update_preserves_masked_sensitive_value():
    from mcp.api import _merge_service_headers
    existing = {"Authorization": "Bearer old-token", "X-Api-Key": "real-secret"}
    # 编辑表单回填脱敏串（****cret）→ 保留旧密文。
    merged = _merge_service_headers(
        None,
        {"X-Api-Key": "****cret"},
        existing_headers=existing,
        token_provided=False,
    )
    assert merged["X-Api-Key"] == "real-secret"
    # 未显式覆盖 Authorization → 保留已存储 bearer。
    assert merged["Authorization"] == "Bearer old-token"


def test_merge_service_headers_update_empty_sensitive_preserves_existing():
    from mcp.api import _merge_service_headers
    existing = {"Authorization": "Bearer old-token", "X-Api-Key": "real-secret"}
    merged = _merge_service_headers(
        None,
        {"X-Api-Key": ""},
        existing_headers=existing,
        token_provided=False,
    )
    assert merged["X-Api-Key"] == "real-secret"
    assert merged["Authorization"] == "Bearer old-token"


def test_merge_service_headers_update_replaces_non_sensitive():
    from mcp.api import _merge_service_headers
    existing = {"Authorization": "Bearer old-token", "X-Trace": "old"}
    merged = _merge_service_headers(
        None,
        {"X-Trace": "new"},
        existing_headers=existing,
        token_provided=False,
    )
    assert merged["X-Trace"] == "new"
    assert merged["Authorization"] == "Bearer old-token"


def test_merge_service_headers_token_provided_replaces_authorization():
    from mcp.api import _merge_service_headers
    existing = {"Authorization": "Bearer old-token", "X-Trace": "keep"}
    merged = _merge_service_headers(
        "new-token",
        {"X-Trace": "keep"},
        existing_headers=existing,
        token_provided=True,
    )
    assert merged["Authorization"] == "Bearer new-token"
    assert merged["X-Trace"] == "keep"


def test_my_service_to_dict_masks_sensitive_headers_and_excludes_authorization():
    from mcp.api import _my_service_to_dict
    import asyncio
    service = {
        "id": 1,
        "name": "svc",
        "display_name": "Svc",
        "description": "",
        "kind": "sse",
        "url": "http://x/sse",
        "headers": {
            "Authorization": "Bearer sk-plain",
            "X-Api-Key": "secret-value",
            "X-Trace": "visible",
        },
        "enabled": True,
        "runtime_status": "loaded",
        "runtime_last_error": None,
        "tools_cache": [{"name": "t", "description": "d"}],
    }
    out = asyncio.run(_my_service_to_dict(service))
    assert out["token"] == "****lain"
    assert out["has_token"] is True
    assert "Authorization" not in out["headers"]
    assert "authorization" not in out["headers"]
    # 敏感头脱敏、非敏感头保留。
    assert out["headers"]["X-Api-Key"] == "****alue"
    assert out["headers"]["X-Trace"] == "visible"


@pytest.mark.asyncio
async def test_activate_sse_empty_tools_raises(monkeypatch):
    """远端返回空工具列表 → PluginLoadError（registry 不注册空服务）。"""
    async def empty(self):
        return []
    monkeypatch.setattr(SSEClient, "list_tools", empty)

    with pytest.raises(PluginLoadError, match="未返回任何工具"):
        await registry.activate_sse("sse-empty", "http://fake/sse")
