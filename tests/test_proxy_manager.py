"""ProxyManager 单测：验证实例创建/复用/清理/closing 状态机 + 真实 HTTP 三种模式。"""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
import pytest_asyncio

from providers.proxy_manager import (
    IDLE_TIMEOUT,
    OutboundResponse,
    ProxyManager,
    RequestInstance,
)
from providers.base import BaseProvider


def _make_proxy_config(mode="network", **overrides):
    cfg = {
        "id": "proxy-1",
        "mode": mode,
        "url": "http://user:pass@proxy-host:7890",
        "proxy_url": "http://user:pass@proxy-host:7890",
        "url_prefix": "",
        "version": 1,
    }
    cfg.update(overrides)
    return cfg


@pytest.fixture
def manager():
    m = ProxyManager()
    m.update_proxy_config(_make_proxy_config())
    return m


def _fake_response():
    """构造一个假的 OutboundResponse（不走真实 HTTP）。"""
    async def iter_bytes():
        yield b"data: hello\n\n"

    return OutboundResponse(status=200, headers={}, body_iter=iter_bytes())


# ─── 状态机单测 ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_instance_reuse(manager):
    """相同 (proxy_id, host) 复用同一个实例。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        resp1 = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await resp1.__aexit__(None, None, None)
        resp2 = await manager.request(url="https://api.example.com/v1/models", proxy_config_id="proxy-1")
        await resp2.__aexit__(None, None, None)

        assert mock_req.call_count == 2
        assert len(manager._instances) == 1


@pytest.mark.asyncio
async def test_different_host_different_instance(manager):
    """不同 target_host 创建不同实例。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r1 = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r1.__aexit__(None, None, None)
        r2 = await manager.request(url="https://api.other.com/v1/chat", proxy_config_id="proxy-1")
        await r2.__aexit__(None, None, None)

        assert len(manager._instances) == 2


@pytest.mark.asyncio
async def test_account_id_isolates_instances(manager):
    """需登录渠道：相同 (proxy, host) 但不同 account_id -> 各自独立实例
    （避免共用 session 被上游当同源检测）；account_id 为空则共用（默认渠道）。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        # 两个不同账号，相同代理 + host -> 两个实例
        ra = await manager.request(
            url="https://api.example.com/v1/chat", proxy_config_id="proxy-1", account_id="acc-a")
        await ra.__aexit__(None, None, None)
        rb = await manager.request(
            url="https://api.example.com/v1/chat", proxy_config_id="proxy-1", account_id="acc-b")
        await rb.__aexit__(None, None, None)
        assert len(manager._instances) == 2

        # 同账号复用
        ra2 = await manager.request(
            url="https://api.example.com/v1/chat", proxy_config_id="proxy-1", account_id="acc-a")
        await ra2.__aexit__(None, None, None)
        assert len(manager._instances) == 2

        # account_id 为空的两次请求共用一个实例（默认渠道语义不变）
        r0a = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r0a.__aexit__(None, None, None)
        r0b = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r0b.__aexit__(None, None, None)
        assert len(manager._instances) == 3  # 只多了空账号那一个


@pytest.mark.asyncio
async def test_config_change_recreates_instance(manager):
    """配置 version 变更 -> 旧实例关闭，新请求创建新实例。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r.__aexit__(None, None, None)
        old_instance = list(manager._instances.values())[0]

        # 配置变更（version 递增）
        manager.update_proxy_config(_make_proxy_config(version=2))
        r2 = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r2.__aexit__(None, None, None)

        new_instance = list(manager._instances.values())[0]
        assert old_instance is not new_instance
        assert old_instance._closing is True


@pytest.mark.asyncio
async def test_cleanup_expired_instance(manager):
    """空闲超时的实例被清理。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r.__aexit__(None, None, None)
        instance = list(manager._instances.values())[0]

        # 模拟超时
        instance._last_used = time.monotonic() - IDLE_TIMEOUT - 1

        await manager.cleanup()

        assert len(manager._instances) == 0
        assert instance._closing is True


@pytest.mark.asyncio
async def test_cleanup_skip_in_flight(manager):
    """有请求进行中的实例不清理。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        # 不关 response -> in_flight 保持 > 0
        instance = list(manager._instances.values())[0]
        instance._last_used = time.monotonic() - IDLE_TIMEOUT - 1

        await manager.cleanup()

        assert len(manager._instances) == 1  # 不清理
        await r.__aexit__(None, None, None)  # 清理


@pytest.mark.asyncio
async def test_closing_waits_for_in_flight(manager):
    """closing 后等 in_flight 归零才关 session。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        instance = list(manager._instances.values())[0]
        # in_flight 已经是 1（request 期间）
        instance._closing = True

        # 还没归零，不关 session
        await instance._on_request_done()
        # session 还是 None（_request_via_session 被 mock，没真正创建）
        # 验证 _close_resources 不抛异常
        assert instance._in_flight == 0


@pytest.mark.asyncio
async def test_proxy_config_changed_callback(manager):
    """on_proxy_config_changed 删除该代理所有实例。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r1 = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r1.__aexit__(None, None, None)
        r2 = await manager.request(url="https://api.other.com/v1/chat", proxy_config_id="proxy-1")
        await r2.__aexit__(None, None, None)
        assert len(manager._instances) == 2

        await manager.on_proxy_config_changed("proxy-1")

        assert len(manager._instances) == 0


@pytest.mark.asyncio
async def test_unknown_proxy_id_falls_back_to_direct(manager):
    """proxy_config_id 查不到（被删/未同步）-> 降级直连，绝不报错阻断出站。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()
        r = await manager.request(url="https://api.example.com/", proxy_config_id="not-exist")
        await r.__aexit__(None, None, None)

    inst = list(manager._instances.values())[0]
    assert inst.mode == "direct"


@pytest.mark.asyncio
async def test_empty_proxy_id_is_implicit_direct(manager):
    """proxy_config_id 为空（无代理账号，占绝大多数）-> 隐式直连。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()
        r = await manager.request(url="https://api.example.com/", proxy_config_id="")
        await r.__aexit__(None, None, None)
        r2 = await manager.request(url="https://api.example.com/", proxy_config_id=None)
        await r2.__aexit__(None, None, None)

    # 空 id 与 None 归一到同一直连实例
    assert len(manager._instances) == 1
    assert list(manager._instances.values())[0].mode == "direct"


# ─── 三大热更新场景（改渠道 url / 改账号代理 / 改代理配置） ──────────────────────

@pytest.mark.asyncio
async def test_url_prefix_exposed_on_instance(manager):
    """BUG 回归：url_prefix 模式下，RequestInstance 必须暴露 url_prefix 属性，
    否则 install_url_prefix_interceptor 读 owner.url_prefix 恒得 None、前缀改写失效。"""
    manager.update_proxy_config(_make_proxy_config(
        mode="url_prefix", id="proxy-prefix", url_prefix="https://reverse.example.com",
    ))
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()
        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-prefix")
        await r.__aexit__(None, None, None)

    inst = list(manager._instances.values())[0]
    # 拦截器就是靠这个属性拿前缀；RequestInstance 必须能读到 _proxy_config["url_prefix"]。
    assert inst.url_prefix == "https://reverse.example.com"


@pytest.mark.asyncio
async def test_change_channel_url_orphans_old_host_and_cleans_up(manager):
    """场景1 改渠道 url：host 变 -> 新 host 建新实例；旧 host 实例空闲后，
    下一次请求收尾的 cleanup 会把它回收（不依赖外部定时器）。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r1 = await manager.request(url="https://old-host.example.com/v1/chat", proxy_config_id="proxy-1")
        await r1.__aexit__(None, None, None)
        old_inst = manager._instances[("", "proxy-1", "old-host.example.com")]

        # 渠道 url 改了 -> 旧 host 实例即刻置为过期（模拟空闲超时）
        old_inst._last_used = time.monotonic() - IDLE_TIMEOUT - 1

        # 打到新 host：建新实例，且本次请求收尾的 cleanup 顺手回收旧 host 孤儿
        r2 = await manager.request(url="https://new-host.example.com/v1/chat", proxy_config_id="proxy-1")
        await r2.__aexit__(None, None, None)

        assert ("", "proxy-1", "new-host.example.com") in manager._instances
        assert ("", "proxy-1", "old-host.example.com") not in manager._instances
        assert old_inst._closing is True


@pytest.mark.asyncio
async def test_change_proxy_content_bumps_version_and_recreates(manager):
    """场景3 改代理配置本身：调用方只调 update_proxy_config、没显式递增 version，
    但内容（url/密码）变了 -> 自动递增 version -> 旧实例失效、下次请求重建。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r.__aexit__(None, None, None)
        old_inst = manager._instances[("", "proxy-1", "api.example.com")]

        # 改代理 url，但不碰 version（模拟接线层只同步了配置内容）
        manager.update_proxy_config(_make_proxy_config(
            id="proxy-1", url="http://user:pass@new-proxy:7890",
            proxy_url="http://user:pass@new-proxy:7890",
        ))
        # 兜底应已把 version 从 1 递增到 2
        assert manager._current_config("proxy-1")["version"] == 2

        r2 = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r2.__aexit__(None, None, None)
        new_inst = manager._instances[("", "proxy-1", "api.example.com")]

        assert new_inst is not old_inst
        assert old_inst._closing is True


@pytest.mark.asyncio
async def test_update_proxy_config_no_content_change_keeps_version(manager):
    """重复下发相同配置不应递增 version（否则每次同步都误判变更、白重建实例）。"""
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r.__aexit__(None, None, None)
        inst = manager._instances[("", "proxy-1", "api.example.com")]

        # 原样再下发一遍（内容没变）
        manager.update_proxy_config(_make_proxy_config())
        assert manager._current_config("proxy-1")["version"] == 1

        r2 = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r2.__aexit__(None, None, None)
        # 实例应被复用，不重建
        assert manager._instances[("", "proxy-1", "api.example.com")] is inst


# ─── node 模式（mock node manager） ─────────────────────────────────────────────

class _FakeProxyResponse:
    """模拟 NodeProxyResponse：记录请求描述，回放预设响应 chunk。"""

    def __init__(self, response_chunks):
        self._response_chunks = list(response_chunks)
        self.method = None
        self.url = None
        self.headers = None
        self.body = None
        self.status = 200
        self.headers_out = {"Content-Type": "application/json"}
        self.closed = False

    async def iter_chunks(self):
        for c in self._response_chunks:
            yield c

    async def close(self):
        self.closed = True


class _FakeNodeManager:
    """模拟 NodeConnectManager：把收到的请求描述喂给一个 _FakeProxyResponse。"""

    def __init__(self, response_chunks):
        self._response_chunks = response_chunks
        self.responses = []

    async def request(self, node_id, *, method, url, headers=None, body=b""):
        r = _FakeProxyResponse(self._response_chunks)
        r.method = method
        r.url = url
        r.headers = headers
        r.body = body
        self.responses.append(r)
        return r


@pytest.mark.asyncio
async def test_node_mode_basic():
    """node 模式：请求描述下发给节点，响应流式回收。"""
    from providers.proxy_manager import ProxyManager

    chunks = [b'{"choices": [{"delta": {"content": "hi"}}]}']
    node_mgr = _FakeNodeManager(chunks)
    mgr = ProxyManager(node_manager=node_mgr)
    mgr.update_proxy_config({
        "id": "proxy-node-1", "mode": "node", "node_id": "node-123", "version": 1,
    })

    r = await mgr.request(
        url="https://api.example.com/v1/chat/completions",
        proxy_config_id="proxy-node-1",
        method="POST",
        headers={"Authorization": "Bearer xxx"},
        json={"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]},
    )
    async with r as resp:
        body = await resp.read()
        assert b"choices" in body

    # 验证请求描述确实下发了
    assert len(node_mgr.responses) == 1
    req = node_mgr.responses[0]
    assert req.method == "POST"
    assert req.url == "https://api.example.com/v1/chat/completions"
    assert req.headers["Authorization"] == "Bearer xxx"
    assert b"gpt-4" in req.body

    await mgr.close_all()


@pytest.mark.asyncio
async def test_node_mode_request_idempotent():
    """node 模式：两次请求各自独立（请求级，不复用 tunnel）。"""
    from providers.proxy_manager import ProxyManager

    node_mgr = _FakeNodeManager([b"ok"])
    mgr = ProxyManager(node_manager=node_mgr)
    mgr.update_proxy_config({
        "id": "proxy-node-1", "mode": "node", "node_id": "node-123", "version": 1,
    })

    r1 = await mgr.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-node-1")
    async with r1:
        await r1.read()
    r2 = await mgr.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-node-1")
    async with r2:
        await r2.read()

    # 两次请求 = 两个 response 对象（请求级）
    assert len(node_mgr.responses) == 2
    await mgr.close_all()


# ─── 真实 HTTP 测试（direct 模式，本地服务器） ──────────────────────────────

@pytest_asyncio.fixture
async def http_server():
    """起一个本地 HTTP 服务器，避免依赖外部 httpbin。"""
    from aiohttp import web

    async def handle_get(request):
        return web.Response(text='{"hello": "world"}', content_type="application/json")

    async def handle_stream(request):
        n = int(request.match_info["n"])
        resp = web.StreamResponse(headers={"Content-Type": "application/json"})
        await resp.prepare(request)
        for i in range(n):
            await resp.write(f'{{"i": {i}}}\n'.encode())
        await resp.write_eof()
        return resp

    async def handle_multicookie(request):
        # 两个独立 Set-Cookie 头：模拟 eaichat/qwen 登录响应。
        resp = web.Response(text="ok")
        resp.headers.add("Set-Cookie", "YL-Token=tok123; Path=/")
        resp.headers.add("Set-Cookie", "YL-Ssid=ssid456; Path=/")
        return resp

    app = web.Application()
    app.router.add_get("/get", handle_get)
    app.router.add_get("/stream/{n}", handle_stream)
    app.router.add_get("/multicookie", handle_multicookie)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    yield base
    await runner.cleanup()


@pytest.mark.asyncio
async def test_real_http_direct_mode(http_server):
    """direct 模式真实发请求：验证 session 复用 + 响应流 + 关闭。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))

    r1 = await mgr.request(url=f"{http_server}/get", proxy_config_id="proxy-direct", method="GET")
    async with r1 as resp:
        assert resp.status == 200
        body = await resp.read()
        assert b"hello" in body

    # 同 host 复用实例
    r2 = await mgr.request(url=f"{http_server}/get", proxy_config_id="proxy-direct", method="GET")
    async with r2 as resp:
        assert resp.status == 200
    assert len(mgr._instances) == 1

    await mgr.close_all()


@pytest.mark.asyncio
async def test_real_http_streaming(http_server):
    """流式响应：验证 iter_any 能拿到多个 chunk。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))

    r = await mgr.request(url=f"{http_server}/stream/5", proxy_config_id="proxy-direct", method="GET")
    async with r as resp:
        assert resp.status == 200
        chunks = []
        async for chunk in resp.iter_any():
            chunks.append(chunk)
        assert len(chunks) >= 1
        body = b"".join(chunks)
        assert len(body) > 0

    await mgr.close_all()


# ─── facade（ProxyManagerSession）：复刻 aiohttp session 的双语义 ──────────────

@pytest.mark.asyncio
async def test_facade_async_with_semantics(http_server):
    """facade.get() 支持 `async with ... as resp`（aiohttp 主流写法）。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))
    session = mgr.session(proxy_config_id="proxy-direct")

    async with session.get(f"{http_server}/get") as resp:
        assert resp.status == 200
        body = await resp.read()
        assert b"hello" in body

    await mgr.close_all()


@pytest.mark.asyncio
async def test_facade_await_semantics(http_server):
    """facade.get() 也支持 `resp = await ...`（aiohttp 另一种写法）。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))
    session = mgr.session(proxy_config_id="proxy-direct")

    resp = await session.get(f"{http_server}/get")
    assert resp.status == 200
    body = await resp.read()
    assert b"hello" in body

    await mgr.close_all()


@pytest.mark.asyncio
async def test_facade_swallows_proxy_kwarg(http_server):
    """facade 吞掉历史遗留的 proxy= 参数（路由由配置决定），调用点无需改。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))
    session = mgr.session(proxy_config_id="proxy-direct")

    # 传入 proxy= 不应报错、不应真的走那个不存在的代理
    async with session.get(f"{http_server}/get", proxy="http://nonexistent:9999") as resp:
        assert resp.status == 200
        body = await resp.read()
        assert b"hello" in body

    await mgr.close_all()


@pytest.mark.asyncio
async def test_facade_close_is_noop_for_pool(http_server):
    """facade.close() 不能关掉池里的复用 session（facade 无自有连接）。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))
    session = mgr.session(proxy_config_id="proxy-direct")

    async with session.get(f"{http_server}/get") as resp:
        await resp.read()
    # 关 facade
    await session.close()
    assert session.closed is True
    # 池里的实例仍在、仍可用（facade.close 不波及池）
    r2 = await mgr.session(proxy_config_id="proxy-direct").get(f"{http_server}/get")
    assert r2.status == 200
    await r2.read()

    await mgr.close_all()


@pytest.mark.asyncio
async def test_multi_set_cookie_preserved(http_server):
    """回归：登录响应的多条同名 Set-Cookie 必须全部保留（eaichat 取 2 个 cookie、
    qwen 遍历全部 cookie）。dict(resp.headers) 会把多条塌成一条，故 .cookies 必须
    透传 aiohttp 已正确解析的 cookies，而不是从塌陷 headers 重解析。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))

    r = await mgr.request(url=f"{http_server}/multicookie", proxy_config_id="proxy-direct", method="GET")
    async with r as resp:
        # 两条独立 Set-Cookie 都要在
        assert resp.cookies.get("YL-Token") is not None, "第一个 cookie 丢失"
        assert resp.cookies.get("YL-Ssid") is not None, "第二个 cookie 丢失（多值 Set-Cookie 塌陷 bug）"
        assert resp.cookies.get("YL-Token").value == "tok123"
        assert resp.cookies.get("YL-Ssid").value == "ssid456"

    await mgr.close_all()


# ─── 实际代理决策记录（请求日志据此展示"是否走代理/走错代理"） ──────────────────

def test_mask_proxy_url_hides_password():
    from providers.proxy_manager import _mask_proxy_url

    assert _mask_proxy_url("http://user:secret@10.0.0.5:8080") == "http://user:***@10.0.0.5:8080"
    # 无密码原样返回
    assert _mask_proxy_url("http://10.0.0.5:8080") == "http://10.0.0.5:8080"
    # 有用户名无密码
    assert _mask_proxy_url("http://user@10.0.0.5:8080") == "http://user@10.0.0.5:8080"
    # 空值
    assert _mask_proxy_url("") == ""
    assert _mask_proxy_url(None) == ""


def test_describe_and_record_four_modes():
    from providers.proxy_manager import (
        describe_and_record,
        reset_outbound_proxy,
        take_outbound_proxy,
    )

    # network：脱敏 proxy_url
    reset_outbound_proxy()
    describe_and_record(
        {"id": "px-1", "mode": "network", "proxy_url": "http://u:p@10.0.0.5:8080"},
        "api.anthropic.com",
    )
    info = take_outbound_proxy()
    assert info == {
        "mode": "network",
        "proxy_config_id": "px-1",
        "target_host": "api.anthropic.com",
        "proxy_url": "http://u:***@10.0.0.5:8080",
    }

    # direct：无代理目标字段
    describe_and_record({"id": "", "mode": "direct"}, "api.openai.com")
    info = take_outbound_proxy()
    assert info == {"mode": "direct", "proxy_config_id": "", "target_host": "api.openai.com"}

    # url_prefix
    describe_and_record(
        {"id": "px-2", "mode": "url_prefix", "url_prefix": "https://fwd.example.com"},
        "api.x.com",
    )
    assert take_outbound_proxy()["url_prefix"] == "https://fwd.example.com"

    # node
    describe_and_record({"id": "px-3", "mode": "node", "node_id": "node-9"}, "api.y.com")
    assert take_outbound_proxy()["node_id"] == "node-9"


def test_reset_outbound_proxy_clears():
    from providers.proxy_manager import (
        describe_and_record,
        reset_outbound_proxy,
        take_outbound_proxy,
    )

    describe_and_record({"id": "px", "mode": "direct"}, "h")
    assert take_outbound_proxy() is not None
    reset_outbound_proxy()
    assert take_outbound_proxy() is None


@pytest.mark.asyncio
async def test_request_records_proxy_decision(manager):
    """出站唯一入口 request() 应把实际代理决策写入 ContextVar（在请求方法内写入）。"""
    from providers.proxy_manager import reset_outbound_proxy, take_outbound_proxy

    reset_outbound_proxy()
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()
        r = await manager.request(url="https://api.example.com/v1/chat", proxy_config_id="proxy-1")
        await r.__aexit__(None, None, None)

    info = take_outbound_proxy()
    assert info is not None
    assert info["mode"] == "network"
    assert info["proxy_config_id"] == "proxy-1"
    assert info["target_host"] == "api.example.com"
    # 密码脱敏（_make_proxy_config 的 proxy_url 含 user:pass）
    assert info["proxy_url"] == "http://user:***@proxy-host:7890"


# ─── facade 动态 proxy_config_id（账号换绑代理立即生效） ────────────────────────
# 回归：facade 曾在 __init__ 里把 proxy_config_id 冻结成字符串快照，而 provider
# 长期缓存 facade（BaseProvider._get_session）。账号从直连换绑代理后，热更新只改了
# provider.proxy_config_id，facade 仍用旧空串 -> ProxyManager 隐式直连 ->
# 「改了代理不生效，请求日志显示直连」。现改为 callable 每次出站实时求值。

def _dual_proxy_manager():
    """一个同时装了 direct 与 url_prefix 两条配置的 manager。"""
    mgr = ProxyManager()
    mgr.update_proxy_config(_make_proxy_config(mode="direct", id="proxy-direct"))
    mgr.update_proxy_config(_make_proxy_config(
        mode="url_prefix", id="proxy-prefix", url_prefix="https://prefix.example.com",
    ))
    return mgr


@pytest.mark.asyncio
async def test_facade_callable_proxy_config_id_reflects_rebinding():
    """同一个 facade 实例：callable 返回值从 "" 改成代理 id 后，下一次请求即按新 id 路由。

    这条直接锁住本次 bug——旧实现（字符串快照）会恒为 direct。
    """
    from providers.proxy_manager import reset_outbound_proxy, take_outbound_proxy

    mgr = _dual_proxy_manager()
    bound = {"id": ""}  # 模拟 provider.proxy_config_id，初始为直连
    session = mgr.session(account_id="acc-1", proxy_config_id=lambda: bound["id"])

    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        reset_outbound_proxy()
        r1 = await session.get("https://api.example.com/v1/chat")
        await r1.__aexit__(None, None, None)
        assert take_outbound_proxy()["mode"] == "direct"

        # 账号换绑前缀代理（等价于 _apply_account_update_in_place 改 provider.proxy_config_id）
        bound["id"] = "proxy-prefix"

        mock_req.return_value = _fake_response()
        reset_outbound_proxy()
        r2 = await session.get("https://api.example.com/v1/chat")
        await r2.__aexit__(None, None, None)
        info = take_outbound_proxy()
        assert info["mode"] == "url_prefix"
        assert info["proxy_config_id"] == "proxy-prefix"
        assert info["url_prefix"] == "https://prefix.example.com"

    await mgr.close_all()


@pytest.mark.asyncio
async def test_facade_callable_unbinding_falls_back_to_direct():
    """反向：从代理 id 改回 "" （解绑代理）后，下一次请求回到直连。"""
    from providers.proxy_manager import reset_outbound_proxy, take_outbound_proxy

    mgr = _dual_proxy_manager()
    bound = {"id": "proxy-prefix"}
    session = mgr.session(account_id="acc-1", proxy_config_id=lambda: bound["id"])

    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        reset_outbound_proxy()
        r1 = await session.get("https://api.example.com/v1/chat")
        await r1.__aexit__(None, None, None)
        assert take_outbound_proxy()["mode"] == "url_prefix"

        bound["id"] = ""  # 解绑

        mock_req.return_value = _fake_response()
        reset_outbound_proxy()
        r2 = await session.get("https://api.example.com/v1/chat")
        await r2.__aexit__(None, None, None)
        info = take_outbound_proxy()
        assert info["mode"] == "direct"
        assert info["proxy_config_id"] == ""

    await mgr.close_all()


@pytest.mark.asyncio
async def test_facade_callable_raising_falls_back_to_direct():
    """解析器抛异常时回退隐式直连，绝不因路由解析失败阻断出站。"""
    from providers.proxy_manager import reset_outbound_proxy, take_outbound_proxy

    mgr = _dual_proxy_manager()

    def _boom() -> str:
        raise RuntimeError("resolver exploded")

    session = mgr.session(account_id="acc-1", proxy_config_id=_boom)

    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()
        reset_outbound_proxy()
        r = await session.get("https://api.example.com/v1/chat")
        await r.__aexit__(None, None, None)

    info = take_outbound_proxy()
    assert info["mode"] == "direct"
    assert info["proxy_config_id"] == ""

    await mgr.close_all()


@pytest.mark.asyncio
async def test_base_provider_session_follows_rebinding(monkeypatch):
    """provider 层端到端：改 provider.proxy_config_id 后，缓存的 _session 也按新值路由
    （不需要任何入口手动清 provider._session）。"""
    import providers.proxy_manager as pm
    from providers.proxy_manager import reset_outbound_proxy, take_outbound_proxy

    mgr = _dual_proxy_manager()
    monkeypatch.setattr(pm, "_shared_manager", mgr)

    class _FakeProvider:
        PROVIDER_NAME = "fake"
        proxy_account_key = "fake:u1"
        _make_session = BaseProvider._make_session
        _get_session = BaseProvider._get_session
        _stream_client_timeout = BaseProvider._stream_client_timeout
        _stream_request_timeout = BaseProvider._stream_request_timeout

        def __init__(self):
            self.proxy_config_id = ""
            self.timeout_seconds = 0
            self._session = None

    provider = _FakeProvider()

    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()

        session = provider._get_session()
        reset_outbound_proxy()
        r1 = await session.get("https://api.example.com/v1/chat")
        await r1.__aexit__(None, None, None)
        assert take_outbound_proxy()["mode"] == "direct"

        # 账号换绑：只改 provider 字段，不碰 provider._session
        provider.proxy_config_id = "proxy-prefix"
        assert provider._get_session() is session  # 仍是同一个缓存 facade

        mock_req.return_value = _fake_response()
        reset_outbound_proxy()
        r2 = await session.get("https://api.example.com/v1/chat")
        await r2.__aexit__(None, None, None)
        assert take_outbound_proxy()["proxy_config_id"] == "proxy-prefix"

    await mgr.close_all()


# ─── sync_proxy_manager_configs：全量收敛 + 剪掉已删条目 ───────────────────────

@pytest.mark.asyncio
async def test_sync_proxy_manager_configs_prunes_deleted(monkeypatch):
    """全量列表里没有的旧 id 要从 _proxy_configs 剪掉，并关掉其残留实例，
    否则「删了代理但出站仍走旧代理」。"""
    import providers.proxy_manager as pm
    from proxy_utils import sync_proxy_manager_configs

    mgr = ProxyManager()
    monkeypatch.setattr(pm, "_shared_manager", mgr)

    proxies = [
        {"id": "px-keep", "name": "keep", "mode": "direct", "url": ""},
        {"id": "px-drop", "name": "drop", "mode": "url_prefix", "url": "https://drop.example.com"},
    ]
    await sync_proxy_manager_configs(proxies)
    assert set(mgr._proxy_configs) == {"px-keep", "px-drop"}
    assert mgr._proxy_configs["px-drop"]["url_prefix"] == "https://drop.example.com"

    # 让 px-drop 上有一个活着的实例，验证剪掉时会被标记关闭
    with patch.object(RequestInstance, "_request_via_session", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = _fake_response()
        r = await mgr.request(url="https://api.example.com/v1/chat", proxy_config_id="px-drop")
        await r.__aexit__(None, None, None)
    dropped_instance = next(inst for key, inst in mgr._instances.items() if key[1] == "px-drop")

    # 全量 PUT 删掉 px-drop
    await sync_proxy_manager_configs([proxies[0]])
    assert set(mgr._proxy_configs) == {"px-keep"}
    assert dropped_instance._closing is True
    assert not [key for key in mgr._instances if key[1] == "px-drop"]

    await mgr.close_all()

