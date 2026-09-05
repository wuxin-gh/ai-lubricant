"""CodeArts spec 的授权契约（本机回调 + ticket 轮询双通道）。

specs/ 在 .gitignore 内、由使用者自持，文件不存在时整个模块跳过。
锁的是这次框架重构后 spec 侧的四条行为：
1. begin_device_flow 的回调端口来自框架 auth_context，不再写死 0；
2. user_code 不再塞说明文字（前端会渲染成「验证码：」）；
3. handle_loopback_callback 两阶段认领：ticket_id 对不上不认领、secret 对不上不换码；
4. poll_device_flow 接受可选 auth_context 尾参。
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from providers.code_loader import load_code_provider_class, invalidate_cache

_SPEC_PATH = Path(__file__).resolve().parent.parent / "specs" / "codearts_code_channel.py"

if not _SPEC_PATH.exists():
    pytest.skip("specs/codearts_code_channel.py 不存在（使用者自持）", allow_module_level=True)

_SOURCE = _SPEC_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def channel_cls():
    cls = load_code_provider_class("codearts-test", _SOURCE)
    yield cls
    invalidate_cache("codearts-test")


def _run(coro):
    return asyncio.run(coro)


def test_declares_expected_auth_surface(channel_cls):
    hooks = channel_cls._spec_hooks
    assert {"init_auth", "check_auth", "fetch_models", "stream_chat", "refresh_auth",
            "account_schema", "begin_device_flow", "poll_device_flow",
            "handle_loopback_callback"}.issubset(hooks)
    # presence-triggered 方法都注入到适配器（admin 靠 getattr / vars 探测）
    for name in ("begin_device_flow", "poll_device_flow", "handle_loopback_callback",
                 "refresh_account_auth"):
        assert name in vars(channel_cls), name
    assert channel_cls.SUPPORTS_TOKEN_AUTO_REFRESH is True
    assert channel_cls.SCHEDULED_REFRESH is True
    assert channel_cls.TOOLS_AS_PROMPT is True
    assert channel_cls.REQUIRES_BASE_URL is False


def test_account_schema_declares_device_code_mode(channel_cls):
    schema = channel_cls.account_schema()
    assert schema["provider_name"] == "codearts-test"
    assert schema["auth_start"]["enabled"] is True
    assert schema["auth_start"]["mode"] == "device_code"
    # loopback 完成方式：上游只回调 127.0.0.1:{port}，前端据此显示「补投回调」输入框
    assert schema["auth_start"]["completion"] == "loopback"
    keys = {f["key"] for f in schema["fields"]}
    # 刷新凭证必需的两个字段必须在表单里（否则手动建号无法续期）
    assert {"refresh_token", "code_verifier"}.issubset(keys)


def test_begin_device_flow_uses_port_from_auth_context(channel_cls):
    """回调端口跟框架给的浏览器 origin 走，不再是渠道写死的 0。默认 vscode-codebot 身份。"""
    from urllib.parse import parse_qs, urlsplit

    p = channel_cls(username="u", password="")
    ctx = {"origin": "http://127.0.0.1:8001", "port": 8001, "state": "s1",
           "callback_url": "http://127.0.0.1:8001/admin/providers/codearts-test/accounts/auth/callback"}
    result = _run(p.begin_device_flow(ctx))

    assert result["task_type"] == "device_code"
    query = parse_qs(urlsplit(result["auth_url"]).query)
    assert query["port"] == ["8001"]
    # 默认身份 = vscode-codebot：SHA-256 / UUID ticket / 显式 auth_callback_url
    assert query["code_challenge_method"] == ["SHA-256"]
    assert query["uri_scheme"] == ["vscode-codebot"]
    assert query["client_id"] == ["vscode-codebot"]
    assert query["plugin-name"] == ["snap_vscode"]
    assert query["theme"] == ["2"]
    # auth_callback_url 里塞了 state：portal 若原样带回来，后端直接按 state 命中会话
    assert query["auth_callback_url"] == ["http://127.0.0.1:8001/oauth/callback?state=s1"]
    assert len(query["ticket_id"][0]) == 36   # UUID4 含 dash
    # 第二阶段换码要用同一个 port 拼 redirect_uri
    assert result["poll_params"]["port"] == 8001
    assert len(result["poll_params"]["code_verifier"]) >= 43
    # user_code 不塞说明文字：前端会把它渲染成「验证码：xxx」
    assert result["user_code"] == ""
    assert result["message"]


def test_begin_device_flow_without_context_degrades_to_port_zero(channel_cls):
    """没有 auth_context（老调用方）时不炸，退回 port=0 纯轮询形态。"""
    from urllib.parse import parse_qs, urlsplit

    p = channel_cls(username="u", password="")
    result = _run(p.begin_device_flow(None))
    query = parse_qs(urlsplit(result["auth_url"]).query)
    assert query["port"] == ["0"]
    # port=0 时不带显式回调地址（and port 短路，避免拼成 127.0.0.1:0）
    assert "auth_callback_url" not in query


def test_begin_device_flow_codearts_profile_uses_legacy_shape(channel_cls):
    """auth_client=codearts 走旧身份：S256 / 32-hex ticket / 不带 auth_callback_url。"""
    from urllib.parse import parse_qs, urlsplit

    p = channel_cls(username="u", password="", auth_client="codearts")
    result = _run(p.begin_device_flow({"port": 8001}))
    query = parse_qs(urlsplit(result["auth_url"]).query)
    assert query["code_challenge_method"] == ["S256"]
    assert query["uri_scheme"] == ["codearts"]
    assert query["client_id"] == ["codearts"]
    assert query["plugin-name"] == ["snap_AIIDE"]
    assert query["theme"] == ["dark"]
    assert "auth_callback_url" not in query
    assert len(query["ticket_id"][0]) == 32   # 32 hex


def test_account_schema_declares_auth_client_default_vscode(channel_cls):
    """account_schema 声明 auth_client 下拉，默认 vscode-codebot，保留 codearts。"""
    schema = channel_cls.account_schema()
    field = next(f for f in schema["fields"] if f["key"] == "auth_client")
    assert field["type"] == "select"
    assert field["default_value"] == "vscode-codebot"
    assert {opt["value"] for opt in field["options"]} == {"vscode-codebot", "codearts"}
    # 字段要能挂成实例属性并随授权落库，否则刷新时读不到身份
    assert "auth_client" in channel_cls.ACCOUNT_FIELDS


def test_loopback_first_stage_claims_by_ticket_id(channel_cls):
    p = channel_cls(username="u", password="")
    poll_params = {"ticket_id": "t" * 32, "secret": "local", "code_verifier": "v", "port": 8001}

    claimed = _run(p.handle_loopback_callback(
        {"secret": "portal-secret",
         "redirect": f"https://codearts.huaweicloud.com/portal/next?ticket_id={'t' * 32}"},
        poll_params))
    assert claimed["status"] == "claimed"
    # portal 下发的真 secret 回填进轮询参数（ticket 轮询必须用它）
    assert claimed["poll_params"]["secret"] == "portal-secret"
    assert claimed["poll_params"]["portal_secret"] == "portal-secret"
    assert claimed["redirect"].endswith("t" * 32)


def test_loopback_first_stage_ignores_other_sessions_ticket(channel_cls):
    """redirect 里的 ticket_id 不是本会话的就不认领（多会话并发时不能抢别人的回调）。"""
    p = channel_cls(username="u", password="")
    outcome = _run(p.handle_loopback_callback(
        {"secret": "portal-secret", "redirect": "https://portal.example/next?ticket_id=someone-else"},
        {"ticket_id": "t" * 32, "secret": "local", "port": 8001}))
    assert outcome is None


def test_loopback_second_stage_requires_matching_portal_secret(channel_cls):
    """两阶段形态：经过 stage-1（portal_secret_saved 非空）后，code 回调的 secret 必须
    与回填值相等才换码——防别的会话抢回调。直回调形态（无 portal_secret_saved）在
    下一条用例里单独覆盖。"""
    p = channel_cls(username="u", password="")
    # 经历过 stage-1：portal_secret_saved 已回填，回调 secret 对不上 → 不换
    outcome = _run(p.handle_loopback_callback(
        {"code": "auth-code", "secret": "wrong-secret"},
        {"ticket_id": "t" * 32, "portal_secret": "portal-secret", "code_verifier": "v", "port": 8001}))
    assert outcome is None


def test_loopback_direct_callback_exchanges_code_without_secret_handshake(channel_cls, monkeypatch):
    """vscode-codebot 直回调形态：portal 登录完直接带 code 回来，没有 secret 握手。
    portal_secret_saved 为空时只要 code 在就换码——这是「回调来了就处理」的简单模型，
    单会话场景；不再强求 portal_secret 握手。"""
    captured: dict = {}

    class FakeResponse:
        status = 200

        async def text(self):
            return ('{"user_id":"uid","user_name":"neo","domain_id":"did",'
                    '"refresh_token":"rt","credentials":{"access_key_id":"AK",'
                    '"secret_access_key":"SK","security_token":"ST",'
                    '"expiration":"2030-01-01T00:00:00Z"}}')

        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    class FakeSession:
        def post(self, url, headers=None, data=None, **_kw):
            captured["url"] = url
            captured["form"] = dict(data or {})
            return FakeResponse()

        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    p = channel_cls(username="u", password="")   # 默认 vscode-codebot 身份
    monkeypatch.setattr(type(p), "_make_session", lambda self, *a, **k: FakeSession())

    # 没有 portal_secret_saved（直回调），回调只带 code → 直接换码
    outcome = _run(p.handle_loopback_callback(
        {"code": "auth-code"},
        {"ticket_id": "t" * 32, "code_verifier": "verifier-value", "port": 8001}))

    assert outcome["status"] == "authorized"
    assert outcome["account_data"]["user_name"] == "neo"
    assert outcome["account_data"]["auth_client"] == "vscode-codebot"
    assert captured["form"]["code"] == "auth-code"
    assert captured["form"]["client_id"] == "vscode-codebot"


def test_loopback_second_stage_exchanges_code(channel_cls, monkeypatch):
    """secret 对得上就用 authorization_code 换凭证，redirect_uri 用登录时那个 port。"""
    captured: dict = {}

    class FakeResponse:
        status = 200

        async def text(self):
            return (
                '{"user_id":"uid","user_name":"neo","domain_id":"did",'
                '"refresh_token":"rt","credentials":{"access_key_id":"AK",'
                '"secret_access_key":"SK","security_token":"ST",'
                '"expiration":"2030-01-01T00:00:00Z"}}'
            )

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_a):
            return False

    class FakeSession:
        def post(self, url, headers=None, data=None, **_kw):
            captured["url"] = url
            captured["form"] = dict(data or {})
            captured["headers"] = dict(headers or {})
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_a):
            return False

    p = channel_cls(username="u", password="")
    monkeypatch.setattr(type(p), "_make_session", lambda self, *a, **k: FakeSession())

    outcome = _run(p.handle_loopback_callback(
        {"code": "auth-code", "secret": "portal-secret"},
        {"ticket_id": "t" * 32, "portal_secret": "portal-secret",
         "code_verifier": "verifier-value", "port": 8001}))

    assert outcome["status"] == "authorized"
    data = outcome["account_data"]
    assert data["user_name"] == "neo"
    assert data["access_key_id"] == "AK"
    assert data["refresh_token"] == "rt"
    assert data["code_verifier"] == "verifier-value"   # 刷新凭证要用登录时那份
    assert data["auth_client"] == "vscode-codebot"     # 身份随号落库，刷新时同源
    assert captured["form"]["grant_type"] == "authorization_code"
    assert captured["form"]["code"] == "auth-code"
    assert captured["form"]["client_id"] == "vscode-codebot"   # 与 authorize 必须一致
    assert captured["form"]["redirect_uri"] == "http://127.0.0.1:8001/oauth/callback"
    assert captured["headers"]["DPoP"]                  # RFC 9449 证明头必带
    assert "/v1/oauth2/tokens" in captured["url"]


def test_poll_device_flow_accepts_optional_auth_context(channel_cls):
    """扫描器按签名传 auth_context；spec 收下但不依赖它（形态兼容即可）。"""
    import inspect

    hook = channel_cls._spec_hooks["poll_device_flow"]
    params = list(inspect.signature(hook).parameters)
    assert params[:2] == ["p", "poll_params"]
    assert "auth_context" in params


def _fake_sts_token(user_name: str = "hid_42z_q_f4hqn5gzr") -> str:
    """造一个形态贴合华为云 STS token 的串：base64(头部字节 + JSON 载荷 + 签名)。"""
    import base64 as _b64
    import json as _json

    payload = _json.dumps({
        "access": "HST3WMZYH7Q8S1Q473B8",
        "issued_at": 1788338040512,
        "methods": ["token"],
        "user": {
            "domain": {"id": "129423727a434ed9b9be5024793a9e5a", "name": user_name},
            "id": "004740bbe5ff44f99d605cd23e91c122",
            "name": user_name,
            "user_type": 17,
        },
    }, separators=(",", ":")).encode("utf-8")
    raw = b"\x82\ncn-north-4P\xda" + payload + b"\x8aU(\xca\x89\xd5F\x90 e\xe8^\x8e\xbc_"
    return _b64.urlsafe_b64encode(raw).decode("ascii")


def test_identity_recovered_from_sts_token_when_exchange_omits_user_name(channel_cls, monkeypatch):
    """直回调换码响应不带 user_name → 从 STS token 载荷解出账号名，不再落占位名。

    回归：admin 的 _finalize_authorized_auth 缺 user_name 时会用「{渠道名}-user」
    建号（实测落成 codearts-agent-user），账号列表里根本认不出是哪个华为云账号。
    """
    import json as _json

    token = _fake_sts_token()
    captured: dict = {}

    class FakeResponse:
        status = 200

        async def text(self):
            # 上游只回凭证，不回 user_id / user_name / domain_id
            return _json.dumps({
                "refresh_token": "rt",
                "credentials": {
                    "access_key_id": "AK", "secret_access_key": "SK",
                    "security_token": token, "expiration": "2030-01-01T00:00:00Z",
                },
            })

        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    class FakeSession:
        def post(self, url, headers=None, data=None, **_kw):
            captured["form"] = dict(data or {})
            return FakeResponse()

        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    p = channel_cls(username="u", password="")
    monkeypatch.setattr(type(p), "_make_session", lambda self, *a, **k: FakeSession())

    outcome = _run(p.handle_loopback_callback(
        {"code": "auth-code"},
        {"ticket_id": "t" * 32, "code_verifier": "verifier-value", "port": 8001}))

    data = outcome["account_data"]
    assert data["user_name"] == "hid_42z_q_f4hqn5gzr"      # 从 token 补出来的
    assert data["user_id"] == "004740bbe5ff44f99d605cd23e91c122"
    assert data["domain_id"] == "129423727a434ed9b9be5024793a9e5a"
    assert data["security_token"] == token


def test_upstream_user_name_wins_over_token_payload(channel_cls, monkeypatch):
    """ticket 通道回了 user_name 就用它，不被 token 载荷覆盖。"""
    import json as _json

    token = _fake_sts_token("from-token")

    class FakeResponse:
        status = 200

        async def text(self):
            return _json.dumps({
                "user_name": "from-upstream", "user_id": "uid-upstream",
                "refresh_token": "rt",
                "credentials": {"access_key_id": "AK", "secret_access_key": "SK",
                                "security_token": token, "expiration": "2030-01-01T00:00:00Z"},
            })

        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    class FakeSession:
        def post(self, *_a, **_kw): return FakeResponse()
        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    p = channel_cls(username="u", password="")
    monkeypatch.setattr(type(p), "_make_session", lambda self, *a, **k: FakeSession())

    outcome = _run(p.handle_loopback_callback(
        {"code": "c"}, {"code_verifier": "v", "port": 8001}))
    assert outcome["account_data"]["user_name"] == "from-upstream"
    assert outcome["account_data"]["user_id"] == "uid-upstream"


def test_malformed_security_token_does_not_break_authorization(channel_cls, monkeypatch):
    """token 不是预期格式时只是补不出账号名，不能抛异常阻断落库。"""
    import json as _json

    class FakeResponse:
        status = 200

        async def text(self):
            return _json.dumps({
                "credentials": {"access_key_id": "AK", "secret_access_key": "SK",
                                "security_token": "not-a-valid-sts-token",
                                "expiration": "2030-01-01T00:00:00Z"},
            })

        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    class FakeSession:
        def post(self, *_a, **_kw): return FakeResponse()
        async def __aenter__(self): return self
        async def __aexit__(self, *_a): return False

    p = channel_cls(username="u", password="")
    monkeypatch.setattr(type(p), "_make_session", lambda self, *a, **k: FakeSession())

    outcome = _run(p.handle_loopback_callback(
        {"code": "c"}, {"code_verifier": "v", "port": 8001}))
    assert outcome["status"] == "authorized"
    assert "user_name" not in outcome["account_data"]


def test_source_keeps_protocol_landmarks():
    """协议关键串在位（防重构时把上游端点/签名算法改跑偏）。"""
    for token in ("/api/v2/chat/completions", "snap-manager/v1/login/ticket",
                  "/v1/oauth2/tokens", "SDK-HMAC-SHA256", "dpop+jwt",
                  "agent-center/agents/useragents", "/oauth/callback"):
        assert token in _SOURCE, token
    # port 不再写死 0
    assert '"port": 0' not in _SOURCE
    assert 'ctx.get("port")' in _SOURCE
    # 两种身份都在，且默认是 vscode-codebot（对齐官方 VSCode 插件 authorize 形态）
    for token in ("snap_vscode", "26.8.203", "snap_AIIDE", "auth_callback_url"):
        assert token in _SOURCE, token
    assert '_DEFAULT_AUTH_PROFILE = "vscode-codebot"' in _SOURCE
