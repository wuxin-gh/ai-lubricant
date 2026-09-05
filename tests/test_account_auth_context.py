"""账号授权：浏览器 origin → 框架回调地址 → auth_context → 按 mode 分发。

锁的是这次重构的四条契约：
1. 回调地址的服务端部分来自**浏览器 origin**（前端上报 / Origin 头），不是 request.base_url
   —— 反代或容器后按当次请求推导会拼出内网地址；
2. body 里的 origin 是不可信输入：Origin 头存在时头胜，非法值一律丢弃回落；
3. 授权模式按 account_schema()["auth_start"]["mode"] 分发，不再靠捕获 501 猜能力；
4. device_code 的落库在扫描器内完成，不依赖前端继续轮询 /auth/status。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import admin


async def _allow_admin(*_args, **_kwargs):
    return "admin-token-abcdef0123456789"


def _request(*, base_url="http://127.0.0.1:8001/", headers=None):
    """最小 Request 替身：只用到 headers / base_url / url_for。"""
    return SimpleNamespace(
        headers=headers or {},
        base_url=base_url,
        url_for=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no router")),
    )


# ==================== origin 归一化 ====================

@pytest.mark.parametrize("raw,expected", [
    ("https://example.com", "https://example.com"),
    ("http://127.0.0.1:8001", "http://127.0.0.1:8001"),
    ("https://example.com/", "https://example.com"),          # 末尾斜杠归一
    ("HTTPS://EXAMPLE.COM:8443", "https://example.com:8443"),  # 大小写归一，端口保留
    ("http://[::1]:8001", "http://[::1]:8001"),                # IPv6 字面量补方括号
])
def test_normalize_auth_origin_accepts_valid(raw, expected):
    assert admin._normalize_auth_origin(raw) == expected


@pytest.mark.parametrize("raw", [
    "javascript:alert(1)",                 # 非 http(s) scheme
    "ftp://example.com",
    "https://example.com/path",            # 带路径
    "https://example.com/?q=1",            # 带 query
    "https://example.com/#frag",           # 带 fragment
    "https://user:pw@example.com",         # 带 userinfo
    "https://",                            # 无 host
    "not-a-url",
    "",
    "   ",
    None,
    12345,
])
def test_normalize_auth_origin_rejects_invalid(raw):
    assert admin._normalize_auth_origin(raw) is None


def test_origin_header_wins_over_body():
    """Origin 头由浏览器设置、页面 JS 改不了；body 值是不可信入参，冲突时头胜。"""
    request = _request(headers={"origin": "https://real.example.com"})
    resolved = admin._auth_origin_from_request(request, {"origin": "https://evil.example.com"})
    assert resolved == "https://real.example.com"


def test_referer_used_when_origin_header_absent():
    request = _request(headers={"referer": "https://panel.example.com/manager/channels?tab=accounts"})
    assert admin._auth_origin_from_request(request, {}) == "https://panel.example.com"


def test_body_origin_used_when_no_headers():
    request = _request()
    assert admin._auth_origin_from_request(request, {"origin": "https://panel.example.com"}) == "https://panel.example.com"


def test_invalid_body_origin_falls_back_to_base_url():
    request = _request(base_url="http://127.0.0.1:8001/")
    assert admin._auth_origin_from_request(request, {"origin": "javascript:alert(1)"}) == "http://127.0.0.1:8001"


def test_no_origin_anywhere_falls_back_to_base_url():
    request = _request(base_url="https://svc.internal:9000/")
    assert admin._auth_origin_from_request(request, {}) == "https://svc.internal:9000"


# ==================== auth_context ====================

def test_auth_context_callback_url_uses_browser_origin_not_base_url():
    """核心断言：回调地址跟浏览器走，不跟服务端当次请求走。"""
    request = _request(base_url="http://app-container:8001/",
                       headers={"origin": "https://panel.example.com"})
    ctx = admin._build_auth_context(request, {}, "my-channel", "state-xyz")
    assert ctx["callback_url"] == "https://panel.example.com/admin/providers/my-channel/accounts/auth/callback"
    assert "app-container" not in ctx["callback_url"]
    assert ctx["origin"] == "https://panel.example.com"
    assert ctx["scheme"] == "https"
    assert ctx["host"] == "panel.example.com"
    assert ctx["port"] == 443           # 默认端口显式补齐
    assert ctx["state"] == "state-xyz"
    assert ctx["provider"] == "my-channel"
    assert ctx["loopback_callback_path"] == "/oauth/callback"


def test_auth_context_keeps_explicit_port_and_quotes_provider_name():
    request = _request(headers={"origin": "http://127.0.0.1:8001"})
    ctx = admin._build_auth_context(request, {}, "chan/with space", "s")
    assert ctx["port"] == 8001
    assert ctx["callback_path"] == "/admin/providers/chan%2Fwith%20space/accounts/auth/callback"
    assert ctx["callback_url"] == f"http://127.0.0.1:8001{ctx['callback_path']}"


# ==================== 按 mode 分发 ====================

class _Recorder:
    """记录哪条授权路径被调用，以及钩子收到的 auth_context。"""

    def __init__(self):
        self.calls: list[str] = []
        self.contexts: list[dict | None] = []


def _install_common(monkeypatch, states: dict):
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)

    async def read_config(_name):
        return {}

    async def set_state(state, data):
        states[state] = dict(data)

    async def del_state(state):
        states.pop(state, None)

    async def log_op(*_a, **_k):
        return None

    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)
    monkeypatch.setattr(admin, "_del_account_auth_state", del_state)
    monkeypatch.setattr(admin, "_log_operation", log_op)
    monkeypatch.setattr(admin, "_merge_saved_account_for_auth", lambda acc, _cfg: dict(acc))
    monkeypatch.setattr(admin, "_resolve_account_proxy", lambda *_a, **_k: None)
    monkeypatch.setattr(admin, "_resolve_account_url_prefix", lambda *_a, **_k: None)
    monkeypatch.setattr(admin, "_provider_extra", lambda *_a, **_k: {})


def _device_provider(rec: _Recorder, *, accepts_context: bool = True):
    class Provider:
        def __init__(self, **_kw):
            pass

        @classmethod
        def account_schema(cls):
            return {"auth_start": {"enabled": True, "mode": "device_code"}}

        @classmethod
        async def build_account_auth_start(cls, *_a, **_k):
            rec.calls.append("build_account_auth_start")
            raise HTTPException(status_code=501, detail="not implemented")

        if accepts_context:
            async def begin_device_flow(self, auth_context=None):
                rec.calls.append("begin_device_flow")
                rec.contexts.append(auth_context)
                return {"task_type": "device_code", "auth_url": "https://up.example/auth",
                        "interval": 3, "expires_in": 300, "poll_params": {"t": "1"}}
        else:
            async def begin_device_flow(self):
                rec.calls.append("begin_device_flow")
                rec.contexts.append(None)
                return {"task_type": "device_code", "auth_url": "https://up.example/auth",
                        "interval": 3, "expires_in": 300, "poll_params": {"t": "1"}}

    return Provider


def test_device_code_mode_skips_callback_probe(monkeypatch):
    """声明 mode=device_code 就直接走设备码，不再先撞一次 callback 的 501。"""
    states: dict = {}
    rec = _Recorder()
    _install_common(monkeypatch, states)
    provider_cls = _device_provider(rec)
    monkeypatch.setattr(admin, "_get_provider_class", lambda *_a, **_k: provider_cls)
    monkeypatch.setattr(admin, "_provider_account_schema",
                        lambda *_a, **_k: {"auth_start": {"enabled": True, "mode": "device_code"}})

    request = _request(headers={"origin": "https://panel.example.com"})
    result = asyncio.run(admin.start_provider_account_auth(
        "demo", {"account": {"username": "u"}, "origin": "https://panel.example.com"},
        request, "Bearer t"))

    assert rec.calls == ["begin_device_flow"]      # callback 钩子一次都没调
    assert result["mode"] == "device_code"
    assert result["poll_status"] is True
    assert "redirect_uri" not in result           # device_code 不回传回调地址
    stored = states[result["state"]]
    assert stored["task_type"] == "device_code"
    assert stored["poll_params"] == {"t": "1"}
    assert stored["auth_context"]["origin"] == "https://panel.example.com"


def test_device_flow_receives_auth_context(monkeypatch):
    states: dict = {}
    rec = _Recorder()
    _install_common(monkeypatch, states)
    monkeypatch.setattr(admin, "_get_provider_class", lambda *_a, **_k: _device_provider(rec))
    monkeypatch.setattr(admin, "_provider_account_schema",
                        lambda *_a, **_k: {"auth_start": {"mode": "device_code"}})

    request = _request(headers={"origin": "http://127.0.0.1:8001"})
    asyncio.run(admin.start_provider_account_auth("demo", {"account": {}}, request, "Bearer t"))

    ctx = rec.contexts[0]
    assert ctx is not None
    assert ctx["port"] == 8001
    assert ctx["callback_url"].startswith("http://127.0.0.1:8001/admin/providers/demo/")


def test_legacy_single_arg_device_hook_still_called(monkeypatch):
    """老 spec 写 begin_device_flow(self) —— 框架按签名探测，不能因多传参数而炸。"""
    states: dict = {}
    rec = _Recorder()
    _install_common(monkeypatch, states)
    monkeypatch.setattr(admin, "_get_provider_class",
                        lambda *_a, **_k: _device_provider(rec, accepts_context=False))
    monkeypatch.setattr(admin, "_provider_account_schema",
                        lambda *_a, **_k: {"auth_start": {"mode": "device_code"}})

    request = _request(headers={"origin": "http://127.0.0.1:8001"})
    result = asyncio.run(admin.start_provider_account_auth("demo", {"account": {}}, request, "Bearer t"))

    assert rec.calls == ["begin_device_flow"]
    assert result["mode"] == "device_code"


def test_callback_mode_skips_device_flow_and_returns_redirect_uri(monkeypatch):
    states: dict = {}
    rec = _Recorder()
    _install_common(monkeypatch, states)

    class Provider:
        def __init__(self, **_kw):
            pass

        @classmethod
        async def build_account_auth_start(cls, name, account, redirect_uri, state, cfg=None, auth_context=None):
            rec.calls.append("build_account_auth_start")
            rec.contexts.append(auth_context)
            return {"auth_url": f"https://login.example/authorize?redirect_uri={redirect_uri}&state={state}"}

        async def begin_device_flow(self, auth_context=None):
            rec.calls.append("begin_device_flow")
            return {}

    monkeypatch.setattr(admin, "_get_provider_class", lambda *_a, **_k: Provider)
    monkeypatch.setattr(admin, "_provider_account_schema",
                        lambda *_a, **_k: {"auth_start": {"enabled": True, "mode": "oauth_callback"}})

    request = _request(base_url="http://app-container:8001/",
                       headers={"origin": "https://panel.example.com"})
    result = asyncio.run(admin.start_provider_account_auth("demo", {"account": {}}, request, "Bearer t"))

    assert rec.calls == ["build_account_auth_start"]   # 不碰设备码
    expected = "https://panel.example.com/admin/providers/demo/accounts/auth/callback"
    assert result["redirect_uri"] == expected
    assert expected in result["auth_url"]              # 渠道拿到的就是框架拼的那份
    assert rec.contexts[0]["callback_url"] == expected


def test_missing_mode_keeps_legacy_501_probe(monkeypatch):
    """没声明 mode 的老渠道：先试 callback、501 才退设备码（行为保持）。"""
    states: dict = {}
    rec = _Recorder()
    _install_common(monkeypatch, states)
    monkeypatch.setattr(admin, "_get_provider_class", lambda *_a, **_k: _device_provider(rec))
    monkeypatch.setattr(admin, "_provider_account_schema", lambda *_a, **_k: {"auth_start": {}})

    request = _request(headers={"origin": "http://127.0.0.1:8001"})
    result = asyncio.run(admin.start_provider_account_auth("demo", {"account": {}}, request, "Bearer t"))

    assert rec.calls == ["build_account_auth_start", "begin_device_flow"]
    assert result["mode"] == "device_code"


def test_non_501_callback_error_propagates(monkeypatch):
    """渠道自己抛的非 501 错误不能被当成「不支持 callback」而静默回退。"""
    states: dict = {}
    _install_common(monkeypatch, states)

    class Provider:
        def __init__(self, **_kw):
            pass

        @classmethod
        async def build_account_auth_start(cls, *_a, **_k):
            raise HTTPException(status_code=502, detail="上游授权服务不可用")

        async def begin_device_flow(self, auth_context=None):
            raise AssertionError("不应回退到设备码")

    monkeypatch.setattr(admin, "_get_provider_class", lambda *_a, **_k: Provider)
    monkeypatch.setattr(admin, "_provider_account_schema", lambda *_a, **_k: {"auth_start": {}})

    request = _request(headers={"origin": "http://127.0.0.1:8001"})
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.start_provider_account_auth("demo", {"account": {}}, request, "Bearer t"))
    assert exc.value.status_code == 502
    assert states == {}   # 失败清理掉 state 行


# ==================== loopback 根路由（/oauth/callback）====================

def _loopback_request(params: dict):
    return SimpleNamespace(query_params=params)

def _install_loopback(monkeypatch, tasks, provider=None, provider_cls=None):
    async def list_tasks():
        return tasks

    async def read_config(_name):
        return {}

    monkeypatch.setattr(admin, "_list_pending_device_code_tasks", list_tasks)
    monkeypatch.setattr(admin, "_read_provider_config", read_config)

    class ProviderCls:
        pass

    cls = provider_cls or ProviderCls
    monkeypatch.setattr(admin, "_get_provider_class", lambda *_a, **_k: cls)
    if provider is not None:
        monkeypatch.setattr(admin, "_build_scanner_instance", lambda *_a, **_k: provider)


# ==================== 手工补投回调地址（auth/replay）====================

def test_parse_callback_url_accepts_three_shapes():
    """完整 URL / ?query / 裸 query 三种形态都能取出参数。"""
    full = admin._parse_callback_url_params(
        "http://127.0.0.1:0/oauth/callback?code=abc&secret=s1")
    qmark = admin._parse_callback_url_params("?code=abc&secret=s1")
    bare = admin._parse_callback_url_params("code=abc&secret=s1")
    assert full == {"code": "abc", "secret": "s1"}
    assert qmark == {"code": "abc", "secret": "s1"}
    assert bare == {"code": "abc", "secret": "s1"}
    assert admin._parse_callback_url_params("") == {}
    assert admin._parse_callback_url_params("not a url") == {}
    assert admin._parse_callback_url_params(None) == {}


def test_replay_authorized_completes_account(monkeypatch):
    """跨机场景：管理员把地址栏里带 code 的回调地址粘回管理端 → 补投即完成落库。"""
    persisted = _stub_finalize_deps(monkeypatch)

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            if params.get("code"):
                return {"status": "authorized",
                        "account_data": {"user_name": "neo"}}
            return None

    record = {"provider": "demo", "status": "pending", "task_type": "device_code",
              "poll_params": {"portal_secret": "s1", "code_verifier": "v", "port": 0},
              "account": {}}
    _install_loopback(monkeypatch, [("s1", record)], provider=Provider(), provider_cls=Provider)

    logged: list = []

    async def log_op(*_a, **_k):
        logged.append(True)

    monkeypatch.setattr(admin, "_log_operation", log_op)

    async def set_state(_state, _data):
        pass

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)

    monkeypatch.setattr(admin, "_require_admin", _allow_admin)
    result = asyncio.run(admin.replay_provider_account_auth(
        "demo", {"callback_url": "http://127.0.0.1:0/oauth/callback?code=auth-code&secret=s1"},
        "Bearer t"))

    assert result["ok"] is True
    assert result["status"] == "completed"
    assert result["username"] == "neo"
    assert persisted and persisted[0][1]["username"] == "neo"


def test_replay_first_stage_returns_pending(monkeypatch):
    """补投第一阶段地址：secret 已回填进轮询参数，等扫描器拿凭证（pending）。"""
    states: dict = {}

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            if params.get("secret") and params.get("redirect"):
                return {"status": "claimed",
                        "poll_params": {"secret": params["secret"]},
                        "redirect": params["redirect"]}
            return None

    record = {"provider": "demo", "status": "pending", "task_type": "device_code",
              "poll_params": {"ticket_id": "t1", "secret": "local"}, "account": {}}
    _install_loopback(monkeypatch, [("s1", record)], provider=Provider(), provider_cls=Provider)

    async def set_state(state, data):
        states[state] = dict(data)

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)

    result = asyncio.run(admin.replay_provider_account_auth(
        "demo",
        {"callback_url": "http://127.0.0.1:8001/oauth/callback?secret=portal-s&redirect=https://portal.example/next?ticket_id=t1"},
        "Bearer t"))

    assert result["ok"] is True
    assert result["status"] == "pending"
    assert states["s1"]["poll_params"]["secret"] == "portal-s"   # 轮询下一轮用新 secret


def test_replay_without_params_raises_400(monkeypatch):
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.replay_provider_account_auth("demo", {"callback_url": "http://x/y"}, "Bearer t"))
    assert exc.value.status_code == 400


def test_replay_unclaimed_raises_400(monkeypatch):
    """没人认领（会话过期/参数不完整）→ 明确 400，而不是静默成功。"""
    _install_loopback(monkeypatch, [])
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.replay_provider_account_auth(
            "demo", {"callback_url": "?code=x&secret=y"}, "Bearer t"))
    assert exc.value.status_code == 400


def test_replay_only_claims_own_provider_sessions(monkeypatch):
    """auth/replay 只处理本渠道的会话，不会替别的渠道认领。"""

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            return {"status": "authorized", "account_data": {"user_name": "neo"}}

    other = {"provider": "other-channel", "status": "pending", "task_type": "device_code",
             "poll_params": {}, "account": {}}
    _install_loopback(monkeypatch, [("s1", other)], provider=Provider(), provider_cls=Provider)
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.replay_provider_account_auth("demo", {"callback_url": "?code=x"}, "Bearer t"))
    assert exc.value.status_code == 400


def test_replay_passes_full_callback_url_to_hook(monkeypatch):
    """手工补投时整条 URL 透传给渠道钩子（需要 fragment/非常规参数的 spec 自己解析）。"""
    received: dict = {}

    class Provider:
        async def handle_loopback_callback(self, params, poll_params, callback_url=""):
            received["params"] = params
            received["url"] = callback_url
            return {"status": "authorized", "account_data": {"user_name": "neo"}}

    record = {"provider": "demo", "status": "pending", "task_type": "device_code",
              "poll_params": {}, "account": {}}
    _install_loopback(monkeypatch, [("s1", record)], provider=Provider(), provider_cls=Provider)
    _stub_finalize_deps(monkeypatch)

    async def set_state(_s, _d):
        pass

    async def read_config(_n):
        return {}

    async def log_op(*_a, **_k):
        pass

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)
    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_log_operation", log_op)
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)

    full_url = "http://127.0.0.1:8001/oauth/callback?code=abc&secret=s1#token=tok"
    asyncio.run(admin.replay_provider_account_auth("demo", {"callback_url": full_url}, "Bearer t"))

    assert received["params"]["code"] == "abc"           # query 参数已解析
    assert received["url"] == full_url                    # 整条 URL 原样透传（含 fragment）


def test_replay_legacy_hook_without_url_param_still_works(monkeypatch):
    """老 spec 只声明 (p, params, poll_params) 两个参数——不传 URL 也能正常工作。"""
    persisted = _stub_finalize_deps(monkeypatch)

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            if params.get("code"):
                return {"status": "authorized", "account_data": {"user_name": "neo"}}
            return None

    record = {"provider": "demo", "status": "pending", "task_type": "device_code",
              "poll_params": {}, "account": {}}
    _install_loopback(monkeypatch, [("s1", record)], provider=Provider(), provider_cls=Provider)

    async def set_state(_s, _d):
        pass

    async def read_config(_n):
        return {}

    async def log_op(*_a, **_k):
        pass

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)
    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_log_operation", log_op)
    monkeypatch.setattr(admin, "_require_admin", _allow_admin)

    result = asyncio.run(admin.replay_provider_account_auth(
        "demo", {"callback_url": "http://x/oauth/callback?code=c"}, "Bearer t"))
    assert result["status"] == "completed"
    assert persisted and persisted[0][1]["username"] == "neo"


def test_loopback_no_pending_sessions_returns_neutral_page(monkeypatch):
    _install_loopback(monkeypatch, [])
    resp = asyncio.run(admin.oauth_loopback_callback(_loopback_request({})))
    assert resp.status_code == 400
    assert admin._LOOPBACK_NEUTRAL_MESSAGE in resp.body.decode()


def test_loopback_unclaimed_returns_same_neutral_page(monkeypatch):
    """有 pending 会话但渠道不认领（None）——与无会话同文案，不泄漏授权是否在进行。"""
    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            return None

    _install_loopback(monkeypatch, [("s1", {"provider": "demo", "poll_params": {}})],
                      provider=Provider(), provider_cls=Provider)
    resp = asyncio.run(admin.oauth_loopback_callback(_loopback_request({"secret": "x"})))
    assert resp.status_code == 400
    assert admin._LOOPBACK_NEUTRAL_MESSAGE in resp.body.decode()


def test_loopback_claimed_merges_poll_params_and_redirects(monkeypatch):
    """第一阶段：渠道认领 → portal 下发的 secret merge 进 poll_params → 307 到渠道给的地址。"""
    states: dict = {}

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            if params.get("secret") and params.get("redirect") and params.get("secret") != poll_params.get("secret"):
                return {"status": "claimed",
                        "poll_params": {"secret": params["secret"]},
                        "redirect": params["redirect"]}
            return None

    record = {"provider": "demo", "status": "pending", "task_type": "device_code",
              "poll_params": {"ticket_id": "t1", "secret": "local-secret"}}
    _install_loopback(monkeypatch, [("s1", record)], provider=Provider(), provider_cls=Provider)

    async def set_state(state, data):
        states[state] = dict(data)

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)

    resp = asyncio.run(admin.oauth_loopback_callback(_loopback_request({
        "secret": "portal-secret", "redirect": "https://portal.example.com/next?ticket_id=t1"})))

    assert resp.status_code == 307
    assert resp.headers["location"] == "https://portal.example.com/next?ticket_id=t1"
    assert states["s1"]["poll_params"]["secret"] == "portal-secret"   # 扫描器下一轮用新 secret
    assert states["s1"]["poll_params"]["ticket_id"] == "t1"           # 原参数不被覆盖


def test_loopback_claimed_without_valid_redirect_gets_neutral_page(monkeypatch):
    """开放重定向防护：redirect 不是 http(s) 绝对 URL 就不给跳。"""
    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            return {"status": "claimed", "poll_params": {}, "redirect": "javascript:alert(1)"}

    _install_loopback(monkeypatch, [("s1", {"provider": "demo", "poll_params": {}})],
                      provider=Provider(), provider_cls=Provider)
    resp = asyncio.run(admin.oauth_loopback_callback(_loopback_request({"secret": "s", "redirect": "x"})))
    assert resp.status_code == 400
    assert admin._LOOPBACK_NEUTRAL_MESSAGE in resp.body.decode()


def test_loopback_state_hint_short_circuits_enumeration(monkeypatch):
    """回调带 state（spec 塞进 auth_callback_url）→ 只调那个会话，不遍历别的 pending。

    否则多会话并发时回调会挨个问每个会话「这是你的吗」——那就是用户不想要的中间层拦截。
    这里放两个 pending 会话 s1 / s2，回调带 state=s2，s1 的钩子根本不该被调到。
    """
    calls: list = []

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            calls.append(poll_params.get("ticket_id"))
            if params.get("code") and poll_params.get("ticket_id") == "t2":
                return {"status": "authorized", "account_data": {"user_name": "neo"}}
            return None

    rec1 = {"provider": "demo", "status": "pending", "task_type": "device_code", "poll_params": {"ticket_id": "t1"}}
    rec2 = {"provider": "demo", "status": "pending", "task_type": "device_code", "poll_params": {"ticket_id": "t2"}}
    _install_loopback(monkeypatch, [("s1", rec1), ("s2", rec2)],
                      provider=Provider(), provider_cls=Provider)
    _stub_finalize_deps(monkeypatch)

    asyncio.run(admin.oauth_loopback_callback(_loopback_request({"state": "s2", "code": "c"})))

    assert calls == ["t2"]   # state 命中 s2，s1 没被调


def _stub_finalize_deps(monkeypatch) -> list:
    """_finalize_authorized_auth 里真正碰 DB/配置的依赖，测试里全部打桩。

    返回 persisted 列表供断言（(name, account) 逐次记录）。
    """
    persisted: list = []

    async def persist(name, account):
        persisted.append((name, dict(account)))

    async def log_op(*_a, **_k):
        return None

    monkeypatch.setattr(admin, "_persist_account", persist)
    monkeypatch.setattr(admin, "_reload_account_into_pool", lambda *_a, **_k: None)
    monkeypatch.setattr(admin, "_log_operation", log_op)
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])
    monkeypatch.setattr(admin, "_normalize_account_proxy_ref", lambda acc, _proxies: acc)
    monkeypatch.setattr(admin, "_provider_rpm", lambda *_a, **_k: 0)
    monkeypatch.setattr(admin, "_provider_extra", lambda *_a, **_k: {})
    return persisted


def test_loopback_authorized_finalizes_account(monkeypatch):
    """第二阶段：渠道换到凭证 → authorized → 直接落库 + completed。"""
    states: dict = {}

    class Provider:
        async def handle_loopback_callback(self, params, poll_params):
            if params.get("code"):
                return {"status": "authorized",
                        "account_data": {"user_name": "neo", "access_key_id": "AK"}}
            return None

    record = {"provider": "demo", "status": "pending", "task_type": "device_code",
              "poll_params": {}, "account": {}}
    _install_loopback(monkeypatch, [("s1", record)], provider=Provider(), provider_cls=Provider)
    persisted = _stub_finalize_deps(monkeypatch)

    async def set_state(state, data):
        states[state] = dict(data)

    async def read_config(_n):
        return {}

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)
    monkeypatch.setattr(admin, "_read_provider_config", read_config)

    resp = asyncio.run(admin.oauth_loopback_callback(_loopback_request({"code": "abc", "secret": "portal-secret"})))

    assert resp.status_code == 200
    body = resp.body.decode()
    assert "授权成功" in body
    assert persisted and persisted[0][1]["username"] == "neo"
    assert states["s1"]["status"] == "completed"
    assert states["s1"]["completed"] is True
    assert states["s1"]["account"]["username"] == "neo"


def test_finalize_authorized_auth_is_idempotent(monkeypatch):
    """已 completed 的会话再 finalize 直接返回已存账号（loopback 与扫描器抢跑护栏）。"""
    persisted = _stub_finalize_deps(monkeypatch)

    async def read_config(_n):
        return {}

    monkeypatch.setattr(admin, "_read_provider_config", read_config)

    record = {"completed": True, "account": {"username": "neo"}, "account_data": {"user_name": "someone-else"}}
    account = asyncio.run(admin._finalize_authorized_auth("demo", "s1", record))
    assert account == {"username": "neo"}
    assert persisted == []          # 幂等：不重复落库


def test_scanner_authorized_finalizes_without_status_poll(monkeypatch):
    """扫描器拿到 authorized 就在原地完成落库——不再依赖前端继续查 /auth/status。"""

    class Provider:
        async def poll_device_flow(self, poll_params, auth_context=None):
            return {"status": "authorized", "account_data": {"user_name": "neo"}}

    provider = Provider()

    async def read_config(_name):
        return {}

    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_build_scanner_instance", lambda *_a, **_k: provider)
    persisted = _stub_finalize_deps(monkeypatch)

    async def set_state(state, data):
        pass

    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)

    record = {"provider": "demo", "task_type": "device_code", "status": "pending",
              "interval": 3, "next_poll_at": 0, "expires_at": 9999999999,
              "poll_params": {}, "account": {}}
    asyncio.run(admin._poll_one_auth_task("s1", record))

    assert record["status"] == "completed"
    assert record["completed"] is True
    assert record["account"]["username"] == "neo"
    assert persisted and persisted[0][1]["username"] == "neo"


# ==================== 回调落地页（成功页倒计时 / 返回首页 / 注入防护）====================

def test_callback_page_success_shows_countdown_and_back_button():
    """成功页：展示渠道+账号，给「返回首页」按钮和 60 秒倒计时，不再 900ms 闪关。

    弹窗一闪而过时用户看不到任何结果；跨机手工补投更是只有这一页可看。
    """
    html = admin._account_auth_callback_html(
        True, "账号授权成功，已写入账号列表", "codearts-agent", "hid_42z")

    assert "授权成功" in html
    assert f"const countdown = {admin._AUTH_CALLBACK_AUTO_CLOSE_SECONDS};" in html
    assert admin._AUTH_CALLBACK_AUTO_CLOSE_SECONDS == 60
    assert 'id="back" type="button">返回首页<' in html
    assert '"username": "hid_42z"' in html            # 账号名进 payload，由脚本渲染
    assert "ai-lubricant-account-auth" in html        # 父页通知不能丢
    assert '"notify": true' in html                   # 真实判决才通知父页
    # 按钮回首页；倒计时到点先尝试关窗，关不掉（非脚本开出的窗口）退回首页
    assert "location.replace('/')" in html and "window.close()" in html
    assert "setTimeout(() => window.close(), 900)" not in html


def test_callback_page_failure_does_not_auto_close():
    """失败页不倒计时也不自动关：错误信息一闪即失就没法排障了。"""
    html = admin._account_auth_callback_html(False, "ticket expired", "codearts-agent")

    assert "授权失败" in html
    assert "const countdown = 0;" in html
    assert "请手动关闭本页" in html
    assert '"notify": true' in html                   # 失败是对本次授权的判决，要通知父页


def test_neutral_page_does_not_notify_parent_or_claim_failure():
    """中性页（无人认领 / 没有进行中的授权）不是判决：不 postMessage，不写「失败」。

    回归：父页收到 ok:false 会 clearInterval 停掉状态轮询（Channels.tsx 的 message
    handler），中性页若也发就把一个还能靠 ticket 轮询救回来的会话打死了。
    """
    resp = admin._loopback_neutral_page()
    html = resp.body.decode()

    assert resp.status_code == 400
    assert "授权失败" not in html
    assert "没有进行中的授权" in html
    assert '"notify": false' in html
    # 中性页没有具体渠道，别渲染「渠道 oauth」这种占位行
    assert '"provider": ""' in html
    assert "payload.notify && window.opener" in html


def test_callback_page_escapes_script_close_in_message():
    """message 带上游原文：``</script>`` 必须转义，否则提前闭合脚本块可执行注入。"""
    html = admin._account_auth_callback_html(
        True, "</script><img src=x onerror=alert(1)>", "demo", "u")

    assert "</script><img" not in html
    assert "\\u003c/script>" in html
    assert html.count("</script>") == 1   # 只有模板自己那一个闭合标签
