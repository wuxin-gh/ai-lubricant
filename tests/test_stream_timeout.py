import asyncio
from types import SimpleNamespace

import aiohttp

import providers.proxy_manager as pm
from channel import Channel
from providers.base import BaseProvider
from providers.custom import CustomProvider


class DummyProvider(BaseProvider):
    PROVIDER_NAME = "dummy"
    BASE_URL = "https://example.com"

    async def init_auth(self, is_check: bool = False) -> bool:
        return True

    async def check_auth(self) -> bool:
        return True

    async def fetch_upstream_model_list(self, retry=0) -> list[dict]:
        return []

    async def _do_stream_chat(self, model_id: str, messages: list[dict], **kwargs):
        if False:
            yield {}

    async def _do_non_stream_chat(self, model_id: str, messages: list[dict], **kwargs) -> dict:
        return {}


class FakeReqCtx:
    """模拟 aiohttp session.request(...) 返回的请求上下文（async with 语义）。"""

    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeResponse:
    def __init__(self):
        self.status = 200
        self.headers = {}
        self.charset = "utf-8"
        self.cookies = {}  # 收口后 _AiohttpOutboundResponse 会透传 resp.cookies
        self.content = SimpleNamespace(iter_any=self._iter_any)

    async def _iter_any(self):
        if False:
            yield b""


class FakeSession:
    """模拟真实 aiohttp.ClientSession：记录构造时的 timeout 与每次 request 的 timeout。"""

    def __init__(self, **kwargs):
        self.closed = False
        self.init_timeout = kwargs.get("timeout")
        self.request_calls = []

    def request(self, method, url, **kwargs):
        self.request_calls.append((method, url, kwargs))
        return FakeReqCtx(FakeResponse())

    async def close(self):
        self.closed = True


class FakeClientSessionFactory:
    """替换 proxy_manager 里真正建 session 的 aiohttp.ClientSession。"""

    def __init__(self):
        self.sessions: list[FakeSession] = []

    def __call__(self, *args, **kwargs):
        session = FakeSession(**kwargs)
        self.sessions.append(session)
        return session


def _install_fake_pm(monkeypatch) -> FakeClientSessionFactory:
    """收口后出站真实 session 建在 providers.proxy_manager。patch 该 seam + 用全新
    ProxyManager 单例，隔离测试间状态。返回工厂以便断言。"""
    factory = FakeClientSessionFactory()
    # 真实 aiohttp session 在 proxy_manager._get_session 里用 aiohttp.ClientSession 构造。
    monkeypatch.setattr(pm.aiohttp, "ClientSession", factory)
    # connector 不需要真的建（工厂忽略 connector kwarg，但仍会调用它）。
    monkeypatch.setattr(pm, "make_insecure_connector", lambda *a, **k: None)
    # 用全新单例，避免此前测试在共享池里留下的实例/session 干扰。
    monkeypatch.setattr(pm, "_shared_manager", None)
    return factory


async def _collect_events(provider: BaseProvider):
    events = []
    async for event in provider.send_sse_request(
        "POST",
        "https://example.com/stream",
        {"accept": "text/event-stream"},
        data="{}",
    ):
        events.append(event)
    return events


def test_channel_request_timeout_defaults_to_120_seconds():
    channel = Channel("custom", {})
    provider = CustomProvider("u", "p", provider_name="custom")
    provider.attach_channel(channel)

    assert channel.timeout_seconds == 120
    assert provider.timeout_seconds == 120
    timeout = provider._stream_request_timeout()
    assert timeout is not None
    assert timeout.total is None
    assert timeout.sock_read == 120


def test_send_sse_request_applies_stream_timeout(monkeypatch):
    """provider 配了 timeout=45：该值必须作为 per-request timeout 传到真实
    session.request（aiohttp 以 per-request timeout 优先，故实际读超时=45s）。"""
    provider = DummyProvider("u", "p", timeout=45)
    factory = _install_fake_pm(monkeypatch)

    asyncio.run(_collect_events(provider))

    assert factory.sessions, "expected a real client session to be created in proxy_manager"
    session = factory.sessions[0]
    assert session.request_calls, "expected session.request to be called"
    method, url, request_kwargs = session.request_calls[0]
    assert method == "POST"
    assert url == "https://example.com/stream"
    # per-request timeout 承载 provider 的 45s 读超时，且不设 total 上限。
    req_timeout = request_kwargs["timeout"]
    assert req_timeout is not None
    assert req_timeout.total is None
    assert req_timeout.sock_read == 45


def test_send_sse_request_without_timeout(monkeypatch):
    """provider 未配 timeout：per-request timeout 由 facade 兜底成 ClientTimeout(total=None)
    —— 无 total 上限、无 sock_read，与旧行为（None）语义等价（都不限时）。"""
    provider = DummyProvider("u", "p")
    factory = _install_fake_pm(monkeypatch)

    asyncio.run(_collect_events(provider))

    assert factory.sessions, "expected a real client session to be created in proxy_manager"
    session = factory.sessions[0]
    _, _, request_kwargs = session.request_calls[0]
    req_timeout = request_kwargs["timeout"]
    # 收口后 facade 把 None 兜底成 ClientTimeout(total=None)：无 total、无 sock_read。
    assert req_timeout is not None
    assert req_timeout.total is None
    assert req_timeout.sock_read is None
