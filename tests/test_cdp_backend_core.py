"""Focused contracts for CDP client auth protocol 2 and user/client/page ownership."""
import asyncio
import hashlib
import json
import time
from pathlib import Path

import pytest

from mcp.configuration import RESOURCE_ADAPTERS, ResourceContext
from mcp_builtin.cdp_bridge.TMWebDriver import TMWebDriver, TabBusyError, TabLeaseManager
from mcp_builtin.cdp_bridge import server as cdp_server
from mcp_runtime.builtin_plugins import cdp_bridge_plugin
from mcp_runtime.plugin_loader import PluginContext, current_request_token
from mcp_runtime.sse_gateway import (
    _authenticate_ext_frame,
    _chat_identity,
    _handle_chat_frame,
    _owned_chat_page,
    _route_ext_frame,
)
import mcp_runtime.sse_gateway as sse_gateway
import mcp_plugin_store
from fastapi import HTTPException

from agent import cdp_chat_service, scene_context


class FakeClient:
    def __init__(self):
        self._user_id = self._client_id = self._token = None
        self.closed = []
        self.sent = []

    def send_message(self, payload): self.sent.append(json.loads(payload))
    def close_auth(self, code, reason): self.closed.append((code, reason))


class FakeWebSocket:
    def __init__(self): self.frames = []
    async def send_text(self, raw): self.frames.append(json.loads(raw))


def client_row(token="secret", *, row_id=7, user_id=3, enabled=True):
    # user_ids 是「可操作用户」列表（方向翻转后）；driver 只用 token_hash + instance_key。
    return {"id": row_id, "instance_key": str(row_id), "name": "Chrome", "user_ids": [user_id],
            "enabled": enabled, "token_hash": hashlib.sha256(token.encode()).hexdigest(), "token_hint": "sec...ret"}


def test_driver_authenticates_hash_and_builds_client_pages():
    driver = TMWebDriver(clients=[client_row()], external_ws=True)
    client = FakeClient()
    config = driver.authenticate_client("secret", "7")
    assert config and config["client_id"] == "7"
    driver.bind_client(client, config)
    driver.ingest_message(json.dumps({"type": "tabs_update", "tabs": [{"id": 12, "url": "https://example.test", "title": "x"}]}), client)
    # 会话池按 client_id 隔离：context key = client_id。
    ctx = driver.get_context("7")
    assert set(ctx.clients) == {"7"}
    assert set(ctx.clients["7"].pages) == {"7:12"}
    assert driver.get_all_sessions("7")[0]["id"] == "7:12"


def test_authenticated_ext_ready_is_accepted_as_compatibility_alias():
    driver = TMWebDriver(clients=[client_row()], external_ws=True)
    client = FakeClient(); driver.bind_client(client, driver.authenticate_client("secret"))
    driver.ingest_message(json.dumps({"type": "ext_ready", "client_id": "evil", "tabs": [
        {"id": 12, "url": "https://example.test"},
    ]}), client)
    assert set(driver.get_context("7").sessions) == {"7:12"}


def test_composite_session_resolution_routes_commands_to_owning_client():
    rows = [client_row("secret-a", row_id=7), client_row("secret-b", row_id=8)]
    driver = TMWebDriver(clients=rows, external_ws=True)
    clients = {}
    for token, client_id in (("secret-a", "7"), ("secret-b", "8")):
        client = FakeClient()
        driver.bind_client(client, driver.authenticate_client(token))
        driver.ingest_message(json.dumps({"type": "tabs_update", "tabs": [
            {"id": 12, "url": f"https://{client_id}.example.test"},
        ]}), client)
        clients[client_id] = client

    # 会话池按 client_id 隔离，token= 语义现在是 client_id。
    page, raw_tab_id = driver.raw_tab_id("8:12", token="8")
    assert page.id == "8:12"
    assert page.ws_client is clients["8"]
    assert raw_tab_id == 12


def test_all_tab_targeted_tools_use_canonical_resolver(monkeypatch):
    calls = []

    class Driver:
        def raw_tab_id(self, session_id, token=None):
            calls.append((session_id, token))
            return type("Page", (), {"id": "client-a:12"})(), 12

        def get_context(self, token):
            return type("Context", (), {"default_session_id": None})()

    page, raw_tab_id = cdp_server._resolve_tab(Driver(), "client-a:12", 3)
    assert page.id == "client-a:12"
    assert raw_tab_id == 12
    assert calls == [("client-a:12", 3)]

    source = Path(cdp_server.__file__).read_text(encoding="utf-8")
    for tool in ("browser_focus_tab", "browser_batch", "browser_network_start", "browser_network_get", "browser_network_stop", "browser_screenshot"):
        body = source.split(f"async def {tool}", 1)[1].split("\n@mcp.tool()", 1)[0]
        assert "_extension_command(" in body or "_resolve_tab(" in body


def test_same_token_rejects_second_active_connection():
    driver = TMWebDriver(clients=[client_row()], external_ws=True)
    config = driver.authenticate_client("secret")
    driver.bind_client(FakeClient(), config)
    with pytest.raises(ValueError, match="already connected"):
        driver.bind_client(FakeClient(), config)


def test_reload_revocation_disconnects_live_client():
    driver = TMWebDriver(clients=[client_row()], external_ws=True)
    client = FakeClient(); driver.bind_client(client, driver.authenticate_client("secret"))
    driver.apply_clients([])
    assert client.closed == [(4403, "authorization revoked")]
    assert "7" not in driver.get_context("7").clients


def test_reload_rotation_disconnects_live_client():
    driver = TMWebDriver(clients=[client_row()], external_ws=True)
    client = FakeClient(); driver.bind_client(client, driver.authenticate_client("secret"))
    rotated = client_row("new-secret")
    driver.apply_clients([rotated])
    assert client.closed == [(4403, "authorization revoked")]
    assert driver.authenticate_client("secret") is None
    assert driver.authenticate_client("new-secret")["client_id"] == "7"


def test_auth_protocol_2_and_legacy_first_frame_rejection():
    async def scenario():
        driver = TMWebDriver(clients=[client_row()], external_ws=True)
        ws, adapter = FakeWebSocket(), FakeClient()
        assert await _authenticate_ext_frame(json.dumps({"type": "auth", "protocol": 2, "token": "secret", "client_id": "7"}), ws, driver, adapter)
        assert ws.frames[-1]["type"] == "auth_ok"
        ws2, adapter2 = FakeWebSocket(), FakeClient()
        assert not await _authenticate_ext_frame(json.dumps({"type": "ext_ready", "token": "secret"}), ws2, driver, adapter2)
        assert ws2.frames[-1]["type"] == "auth_error"
    asyncio.run(scenario())


def test_cdp_client_create_returns_plaintext_once_and_persists_hash(monkeypatch):
    adapter = RESOURCE_ADAPTERS["shared"]
    ctx = ResourceContext({"id": 2}, {"cardinality": "collection", "storage": {"configType": "cdp_client", "handler": "cdp-client"}})
    saved = {}
    async def create(service_id, config_type, value, **kwargs):
        saved.update(service_id=service_id, config_type=config_type, value=value, **kwargs)
        return {"id": 9, "service_id": service_id, "config_type": config_type, "instance_key": "9", "revision": 1,
                "token_hash": kwargs["token_hash"], "token_hint": kwargs["token_hint"], **value}
    monkeypatch.setattr(mcp_plugin_store, "generate_runtime_token", lambda: "cdp_once")
    monkeypatch.setattr(mcp_plugin_store, "create_runtime_config", create)
    monkeypatch.setattr(mcp_plugin_store, "get_mcp_user", lambda user_id: _coro({"id": user_id, "enabled": True}))
    monkeypatch.setattr(mcp_plugin_store, "list_agent_ids", lambda agent_ids: _coro(list(agent_ids)))
    monkeypatch.setattr(mcp_plugin_store, "recompute_service_users_from_resources", lambda service_id: _coro([4]))
    result = asyncio.run(adapter.create(ctx, {"name": "Chrome", "user_ids": [4], "agent_ids": [], "enabled": True}))
    assert result["token"] == "cdp_once"
    assert "token_hash" not in result
    assert result["client_id"] == "9"
    assert saved["token_hash"] == hashlib.sha256(b"cdp_once").hexdigest()
    assert saved["token_hint"] == "cdp_once...once"
    assert saved["value"].get("token") is None


def test_plugin_driver_config_keeps_enabled_clients_with_token_hash():
    cfg = cdp_bridge_plugin._driver_config({"clients": [
        client_row(user_id="4"),
        {**client_row("other", row_id=8), "enabled": False},
    ]}, True, set())
    assert len(cfg["clients"]) == 1
    assert cfg["clients"][0]["instance_key"] == "7"


def test_tool_wrapper_routes_token_to_client_id(monkeypatch):
    # _wrap 现在用 driver 自带的 authenticate_client（纯内存 hash 查找）把请求 token
    # 解析成 client_id，与浏览器扩展 WS 握手走同一条路径；不再用预计算的明文映射表。
    async def tool():
        return json.dumps({"ok": True, "client_id": cdp_server.current_token.get()})

    driver = TMWebDriver(clients=[client_row("mcp-token")], external_ws=True)
    monkeypatch.setattr(cdp_server, "driver", driver)

    context = PluginContext("cdp-bridge")
    request_token = current_request_token.set("mcp-token")
    try:
        result = asyncio.run(cdp_bridge_plugin._wrap(tool)({}, context))
    finally:
        current_request_token.reset(request_token)
    # client_row 默认 instance_key="7" → 解析出的 client_id 为 "7"。
    assert result == {"ok": True, "client_id": "7"}


def test_extension_contract_authenticates_before_tabs_and_removes_legacy_token():
    background = Path(cdp_server.__file__).with_name("tmwd_cdp_bridge").joinpath("background.js").read_text(encoding="utf-8")
    auth_send = "socket.send(JSON.stringify({ type: 'auth', protocol: 2, token: bridgeConfig.clientToken }))"
    assert auth_send in background
    assert background.index(auth_send) < background.index("await sendReadyFrames(socket)")
    assert "data.type === 'auth_ok' && data.protocol === 2" in background
    assert "type: 'tabs_update'" in background
    assert "chrome.storage.local.remove('bridgeToken')" in background
    assert "clientToken: usableClientToken(stored.bridgeToken)" not in background
    assert "reprovisionRequired = !nextConfig.clientToken" in background
    assert "const CONNECTION_TIMEOUT_MS = 10000" in background
    assert "const AUTH_RESPONSE_TIMEOUT_MS = 10000" in background
    assert "code: 'connection_timeout'" in background
    assert "code: 'auth_timeout'" in background
    assert "clearConnectionTimer();" in background
    assert "clearAuthTimer();" in background


def test_ext_frame_router_replies_pong_and_routes_driver_frames():
    async def scenario():
        driver = TMWebDriver(clients=[client_row()], external_ws=True)
        ws, adapter = FakeWebSocket(), FakeClient()
        driver.bind_client(adapter, driver.authenticate_client("secret"))
        await _route_ext_frame(json.dumps({"type": "ping"}), ws, adapter, driver)
        assert ws.frames[-1] == {"type": "pong", "protocol": 2}
        await _route_ext_frame(json.dumps({
            "type": "tabs_update",
            "tabs": [{"id": 12, "url": "https://example.test"}],
        }), ws, adapter, driver)
        assert set(driver.get_context("7").sessions) == {"7:12"}
        assert len(ws.frames) == 1

    asyncio.run(scenario())


def test_ext_frame_router_forwards_chat_frames(monkeypatch):
    async def scenario():
        ws, adapter = FakeWebSocket(), FakeClient()
        adapter._user_id, adapter._client_id, adapter._client_alias = 3, "7", "Chrome"
        monkeypatch.setattr(mcp_plugin_store, "list_agents_for_cdp_client", lambda cid: _coro([{"id": 5}]))
        await _route_ext_frame(json.dumps({"type": "chat_list_agents", "reqId": "r1"}), ws, adapter, None)
        assert ws.frames[-1] == {"type": "chat_agents", "reqId": "r1", "agents": [{"id": 5}]}

    asyncio.run(scenario())


def test_extension_detects_half_open_socket_and_refreshes_chat_panel():
    bridge_dir = Path(cdp_server.__file__).with_name("tmwd_cdp_bridge")
    background = bridge_dir.joinpath("background.js").read_text(encoding="utf-8")
    assert "const PONG_TIMEOUT_MS = 60000" in background
    assert "lastFrameAt = Date.now()" in background
    assert "Date.now() - lastFrameAt > PONG_TIMEOUT_MS" in background
    assert "const dead = markDisconnected();" in background
    assert "function markDisconnected" in background
    assert "failPendingChatRequests(" in background
    assert "桥接连接已断开，请稍后重试" in background

    panel = bridge_dir.joinpath("content.js").read_text(encoding="utf-8")
    assert 'id="refreshChat"' in panel
    assert "onRefreshChat" in panel
    assert "data.type === 'error'" in panel

    gateway_src = Path(sse_gateway.__file__).read_text(encoding="utf-8")
    assert "def _is_ping_frame" in gateway_src
    assert '"pong"' in gateway_src



    adapter = FakeClient()
    adapter._user_id, adapter._client_id, adapter._client_alias = 3, "7", "Work Chrome"
    identity = _chat_identity(adapter, {
        "tabId": 12,
        "token": "attacker-token",
        "user_id": 99,
        "client_id": "evil",
        "client_name": "Evil",
        "session_key": "evil:1",
    })
    assert identity == {
        "user_id": 3,
        "client_id": "7",
        "client_alias": "Work Chrome",
        "session_key": "7:12",
    }


def test_chat_frame_lists_agents_by_connected_client(monkeypatch):
    async def scenario():
        ws, adapter = FakeWebSocket(), FakeClient()
        adapter._user_id, adapter._client_id, adapter._client_alias = 3, "7", "Chrome"
        # 网页对话可选 agent 现按连接的 CDP 客户端 agent_ids 决定，不再按 mcp_user 反推。
        monkeypatch.setattr(mcp_plugin_store, "list_agents_for_cdp_client", lambda cid: _coro([{"id": 5}]))
        await _handle_chat_frame(json.dumps({
            "type": "chat_list_agents", "reqId": "r1", "token": "untrusted",
        }), ws, adapter)
        assert ws.frames[-1] == {"type": "chat_agents", "reqId": "r1", "agents": [{"id": 5}]}

    asyncio.run(scenario())


def test_owned_chat_page_requires_same_authenticated_client():
    driver = TMWebDriver(clients=[client_row()], external_ws=True)
    owner = FakeClient()
    driver.bind_client(owner, driver.authenticate_client("secret"))
    driver.ingest_message(json.dumps({"type": "tabs_update", "tabs": [{"id": 12, "url": "https://example.test"}]}), owner)
    assert _owned_chat_page(driver, owner, "7:12") is not None

    impostor = FakeClient()
    impostor._user_id, impostor._client_id = 3, "7"
    assert _owned_chat_page(driver, impostor, "7:12") is None
    assert _owned_chat_page(driver, owner, "7:99") is None


# ── 会话归属：按 CDP 客户端，不按标签页 ────────────────────────────────

def _cdp_conv(*, client_id="7", tab_id="12", base_prompt="你是助手。", **extra_settings):
    """构造一条 CDP 会话行，system_prompt 里已写入建会话时的场景段。"""
    scene = scene_context.normalize(cdp={
        "client_id": client_id, "session_key": f"{client_id}:{tab_id}",
    })
    settings = {**scene_context.persist(scene), **extra_settings}
    return {
        "id": "conv-1",
        "agent_id": 5,
        "system_prompt": scene_context.append_prompt(base_prompt, scene),
        "chat_settings": settings,
    }


def test_conversation_stays_owned_after_its_tab_closes():
    """归属按 client_id：换标签页后仍能读回自己的历史（tabId 已失效）。"""
    conv = _cdp_conv(tab_id="12")
    # 同一浏览器、另一个标签页 —— 旧实现在这里 403。
    assert cdp_chat_service._require_conv_owned_by_client(conv, "7") is conv["chat_settings"]


def test_other_client_still_cannot_read_the_conversation():
    """收窄到 client_id 不能打破客户端之间的隔离。"""
    conv = _cdp_conv(client_id="7")
    with pytest.raises(HTTPException) as excinfo:
        cdp_chat_service._require_conv_owned_by_client(conv, "8")
    assert excinfo.value.status_code == 403


def test_conversation_without_cdp_owner_is_rejected():
    with pytest.raises(HTTPException) as excinfo:
        cdp_chat_service._require_conv_owned_by_client({"chat_settings": {}}, "7")
    assert excinfo.value.status_code == 403


def test_continuing_from_another_tab_rebinds_scene_to_the_live_tab():
    """续聊时场景段必须指向本轮所在标签页，否则模型操作一个已关闭的 tab。"""
    conv = _cdp_conv(tab_id="12", reasoning_effort="high")
    update_fields = {}

    ownership = cdp_chat_service._rebind_conv_scene(
        conv, conv["chat_settings"],
        client_id="7", session_key="7:99", client_alias="Chrome",
        update_fields=update_fields,
    )

    # 提示词里的 tab_id 换成新的，旧的不再残留，基础提示词保留。
    assert "tab_id: 99" in update_fields["system_prompt"]
    assert "tab_id: 12" not in update_fields["system_prompt"]
    assert update_fields["system_prompt"].startswith("你是助手。")
    # conv 内存态同步更新：本轮 agent 直接读 conv["system_prompt"]。
    assert conv["system_prompt"] == update_fields["system_prompt"]
    # 归属键跟着换，其余 chat_settings 原样保留。
    assert ownership["cdp_session_key"] == "7:99"
    assert ownership["reasoning_effort"] == "high"
    assert update_fields["chat_settings"] is ownership


def test_continuing_in_the_same_tab_writes_nothing():
    """同标签页续聊不该产生一次多余的会话写库。"""
    conv = _cdp_conv(tab_id="12")
    before = dict(conv)
    update_fields = {}

    ownership = cdp_chat_service._rebind_conv_scene(
        conv, conv["chat_settings"],
        client_id="7", session_key="7:12", client_alias="7",
        update_fields=update_fields,
    )

    assert update_fields == {}
    assert ownership is conv["chat_settings"]
    assert conv["system_prompt"] == before["system_prompt"]


def test_rebind_preserves_a_pending_chat_settings_update():
    """场景重绑与本轮思考等级更新共用一次写库，不能互相覆盖。"""
    conv = _cdp_conv(tab_id="12")
    # 调用方已把本轮 reasoning_effort 放进 update_fields（send_message 的既有逻辑）。
    pending = {**conv["chat_settings"], "reasoning_effort": "low"}
    update_fields = {"chat_settings": pending}

    ownership = cdp_chat_service._rebind_conv_scene(
        conv, conv["chat_settings"],
        client_id="7", session_key="7:99", client_alias="7",
        update_fields=update_fields,
    )

    assert ownership["reasoning_effort"] == "low"
    assert ownership["cdp_session_key"] == "7:99"


async def _coro(value):
    return value


# ── TabLeaseManager：单 tab 占用、busy、续期、过期、批量释放 ─────────────


def test_lease_acquired_then_released_lets_another_holder_take_it():
    mgr = TabLeaseManager(default_ttl=300)
    lease = mgr.try_acquire("7:12", "chat:conv1")
    assert isinstance(lease, object)
    assert mgr.get("7:12").holder == "chat:conv1"
    assert mgr.release("7:12", "chat:conv1") is True
    # same tab now free for a different holder
    mgr.try_acquire("7:12", "agent:abc")
    assert mgr.get("7:12").holder == "agent:abc"


def test_lease_blocks_other_holder_with_busy():
    mgr = TabLeaseManager(default_ttl=300)
    mgr.try_acquire("7:12", "chat:conv1")
    with pytest.raises(TabBusyError) as excinfo:
        mgr.try_acquire("7:12", "agent:abc")
    assert excinfo.value.lease.holder == "chat:conv1"
    assert excinfo.value.lease.session_key == "7:12"


def test_lease_renews_for_same_holder_and_switches_tab():
    mgr = TabLeaseManager(default_ttl=300)
    first = mgr.try_acquire("7:12", "agent:abc")
    second = mgr.try_acquire("7:34", "agent:abc")
    # one holder only owns one tab at a time: the first lease is dropped.
    assert mgr.get("7:12") is None
    assert mgr.get("7:34").holder == "agent:abc"
    assert first.holder == second.holder == "agent:abc"


def test_lease_expires_after_ttl_and_frees_the_tab():
    mgr = TabLeaseManager(default_ttl=0.05)
    mgr.try_acquire("7:12", "chat:conv1")
    time.sleep(0.08)
    # expired lease is lazily evicted on read
    assert mgr.get("7:12") is None
    # a different holder can now acquire
    mgr.try_acquire("7:12", "agent:abc")


def test_release_by_holder_drops_all_tabs_for_a_conversation():
    mgr = TabLeaseManager(default_ttl=300)
    mgr.try_acquire("7:12", "chat:conv1")
    # holder switched tabs once; release_by_holder still clears any it currently owns
    assert mgr.release_by_holder("chat:conv1") >= 1
    assert mgr.get("7:12") is None


def test_release_only_succeeds_for_the_owning_holder():
    mgr = TabLeaseManager(default_ttl=300)
    mgr.try_acquire("7:12", "agent:abc")
    assert mgr.release("7:12", "chat:other") is False
    assert mgr.get("7:12").holder == "agent:abc"


def test_session_for_holder_returns_currently_leased_tab():
    mgr = TabLeaseManager(default_ttl=300)
    mgr.try_acquire("7:34", "agent:abc")
    assert mgr.session_for_holder("agent:abc") == "7:34"
    assert mgr.session_for_holder("agent:missing") is None
