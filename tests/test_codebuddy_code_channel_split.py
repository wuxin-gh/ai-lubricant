"""CodeBuddy 代码渠道国内/国际版拆分契约（specs/codebuddy_code_channel_cn.py / _intl.py）。

拆分自原 CN/Intl 双路由单文件 spec（2026-09-12）。本测试锁住拆分后的行为契约：

1. 两份 spec 都能被 code_loader 加载（普通类 + 可识别钩子）；
2. 接入域名**不写死在代码里**——全部出站路径从渠道配置 p.base_url 拼派，
   X-Domain 头默认取渠道地址 host，账号 domain 字段可覆盖（文档站
   code-channel-ai.md 硬性规则 4：域名写死=骗用户，填了不生效）；
3. 设备码流返回形态符合框架扫描器契约：begin 必带 task_type="device_code"
   （admin._invoke_begin_device_flow 缺失直接 500），poll 只认
   {"status": "pending"/"authorized"/"expired"} + account_data；
4. 11128 风控脱敏核心行为（workbuddy2api 逆向对齐，2026-09-12 实测修复）：
   SDK 变体身份句（逗号续接）按核心片段改写、键值整段剥离、developer→system、
   tool_choice any→required、幂等、不污染原对象。

specs/ 在 .gitignore 内、由使用者自持，文件不存在时整个模块跳过。
"""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from providers.code_loader import load_code_provider_class, invalidate_cache

_SPECS_DIR = Path(__file__).resolve().parent.parent / "specs"
_VARIANTS = {
    "cn": {
        "path": _SPECS_DIR / "codebuddy_code_channel_cn.py",
        "channel": "codebuddy-cn-test",
        "cls_name": "CodeBuddyCNChannel",
        "display_name": "腾讯 CodeBuddy（国内版）",
        # 写死常量期望（workbuddy2api 同款分工）：
        "chat_base": "https://copilot.tencent.com",      # 聊天/授权/刷新/模型
        "billing_base": "https://www.codebuddy.cn",      # 签到/余额/Origin
        "x_domain": "www.codebuddy.cn",
    },
    "intl": {
        "path": _SPECS_DIR / "codebuddy_code_channel_intl.py",
        "channel": "codebuddy-intl-test",
        "cls_name": "CodeBuddyIntlChannel",
        "display_name": "CodeBuddy / WorkBuddy（国际版）",
        # 写死常量期望（workbuddy.ai 全家桶单域名）：
        "chat_base": "https://www.workbuddy.ai",
        "billing_base": "https://www.workbuddy.ai",
        "x_domain": "www.workbuddy.ai",
    },
}

_missing = [v["channel"] for v in _VARIANTS.values() if not v["path"].exists()]
if _missing:
    pytest.skip(f"specs/codebuddy_code_channel_{{cn,intl}}.py 不存在（使用者自持）: {_missing}", allow_module_level=True)

for _v in _VARIANTS.values():
    _v["source"] = _v["path"].read_text(encoding="utf-8")


@pytest.fixture(params=list(_VARIANTS), ids=list(_VARIANTS), scope="module")
def variant(request):
    """参数化夹具：逐版本跑同一套契约断言（variant 与 channel_cls 用同一实例，避免笛卡尔积）。"""
    return _VARIANTS[request.param]


@pytest.fixture(scope="module")
def channel_cls(variant):
    """当前 variant 的适配器类（同一 variant 配对，不独立参数化）。"""
    cls = load_code_provider_class(variant["channel"], variant["source"])
    yield cls
    invalidate_cache(variant["channel"])


def _module(v):
    """加载 spec 模块本身（拿模块级 helper；与 code_loader 的 exec 命名空间同一套定义）。"""
    spec = importlib.util.spec_from_file_location(v["path"].stem + "_module", v["path"])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _stub(v, **extra):
    """池外实例 stub：模块级 helper 与静态钩子首参 p 都是鸭子类型。extra 覆盖默认值。

    base_url 默认塞一个**配错的地址**：接入点写死设计下它不该参与寻址。"""
    values = dict(
        base_url="https://misconfigured.example/v2", api_key="tok", username="tester",
        user_id="uid-1", enterprise_id="", domain="", proxy=None)
    values.update(extra)
    return SimpleNamespace(**values)


class _FakeResp:
    """最小响应 stub：async with 兼容（begin/poll 里 `async with session.get/post(...)`）。"""
    status = 200

    def __init__(self, env):
        import json as _json
        self._env = _json.dumps(env)

    async def text(self):
        return self._env

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSessionCtx:
    """p._make_session() 的替身：post/get 按 URL 分发返回预置 env（对齐 workbuddy2api
    真实流程的三端点：auth/state、auth/token、login/account）。routes=GET 路由，
    routes_post=POST 路由；未命中的路径返回 404 信封。"""

    def __init__(self, routes: dict | None = None, routes_post: dict | None = None):
        self._routes = routes or {}
        self._routes_post = routes_post or {}

    async def __aenter__(self):
        routes, routes_post = self._routes, self._routes_post

        class _S:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            def get(self, url, **kw):
                for frag, env in routes.items():
                    if frag in url:
                        return _FakeResp(env)
                return _FakeResp({"code": 404, "msg": "no route"})

            def post(self, url, **kw):
                for frag, env in routes_post.items():
                    if frag in url:
                        return _FakeResp(env)
                return _FakeResp({"code": 404, "msg": "no route"})

        return _S()

    async def __aexit__(self, *exc):
        return False


# ==================== 加载与声明 ====================

def test_spec_loads_with_expected_hooks(channel_cls):
    assert {
        "account_schema", "headers", "payload", "init_auth",
        "begin_device_flow", "poll_device_flow", "refresh_auth", "fetch_models",
        "stream_chat",
    }.issubset(channel_cls._spec_hooks)
    # 接入点写死：base_url（框架全部出站路径）+ build_url（聊天 URL）覆盖钩子必须在。
    assert "base_url" in channel_cls._spec_hooks
    assert "build_url" in channel_cls._spec_hooks


def test_class_flags(channel_cls):
    assert channel_cls.SUPPORTS_MULTI_MESSAGES is True
    # token 自愈 + 签到挂 init_auth 检测链路 + 每日 10:00 刷新兜底。
    assert channel_cls.SUPPORTS_TOKEN_AUTO_REFRESH is True
    assert channel_cls.SCHEDULED_REFRESH is True
    # 上游按自家 CLI 校验请求头，必须关掉框架伪装头。
    assert channel_cls.APPLY_CLIENT_PRESET is False
    # 接入点写死在常量里：渠道地址表单可留空（豁免必填校验）。
    assert channel_cls.REQUIRES_BASE_URL is False
    # 区域不再进账号字段——选国内/国际由「贴哪个 spec 的渠道」决定。
    assert "region" not in channel_cls.ACCOUNT_FIELDS


def test_account_schema_no_region(variant, channel_cls):
    schema = channel_cls.account_schema()
    keys = {f["key"] for f in schema["fields"]}
    assert keys == {"password", "user_id", "enterprise_id", "domain", "refresh_token"}
    assert schema["display_name"] == variant["display_name"]
    assert schema["auth_start"]["mode"] == "device_code"
    assert "last_checkin" in schema["metadata_badges"]


# ==================== 接入点写死（无视渠道配置，运营决策 2026-09-12）====================

def test_base_hardcoded_ignores_channel_config(variant):
    """接入点写死在常量里：渠道配置的 base_url 无论填什么都参与不了寻址。"""
    mod = _module(variant)
    p_bad = _stub(variant, base_url="https://garbage.example/v2/")  # 配错也不会引起 404
    assert mod._base(p_bad) == variant["chat_base"]
    assert mod._base(p_bad, "billing") == variant["billing_base"]


def test_x_domain_defaults_to_spec_constant(variant):
    mod = _module(variant)
    p = _stub(variant, base_url="https://garbage.example/v2/")
    # X-Domain 默认 = spec 常量（不是渠道地址 host）；Origin = 产品官网（billing 域）。
    assert mod._billing_headers(p)["X-Domain"] == variant["x_domain"]
    h = getattr(mod, variant["cls_name"]).headers(p, {}, None)
    assert h["X-Domain"] == variant["x_domain"]
    assert h["Origin"] == variant["billing_base"]
    assert h["User-Agent"] == "CLI/2.63.2 CodeBuddy/2.63.2"
    # 账号 domain 字段仍可覆盖
    p.domain = "my.proxy.example"
    assert mod._billing_headers(p)["X-Domain"] == "my.proxy.example"


def test_addressing_hooks_hardcoded(variant):
    """base_url/build_url 覆盖钩子返回写死常量——协议行 path 与渠道地址全不参与。"""
    mod = _module(variant)
    cls = getattr(mod, variant["cls_name"])
    p = _stub(variant, base_url="https://garbage.example/v2/")
    assert cls.base_url(p) == variant["chat_base"]
    assert cls.build_url(p, {}) == variant["chat_base"] + "/v2/chat/completions"


# ==================== 强制流式（上游只认流式，11101 防护）====================

def test_stream_chat_forces_upstream_stream(variant):
    """出站前把渠道态 upstream_stream 钉成 True：协议行配 auto/固定关也不发非流式
    请求（非流式 400 code=11101「Non-stream chat request is currently not supported」）。"""
    mod = _module(variant)

    async def fake_stream(p, model_id, messages, **kw):
        yield {"content": "ok", "thinking": "", "tool_calls": []}

    mod.CustomProvider = SimpleNamespace(_do_stream_chat=fake_stream)

    # 渠道态上游流式 = auto → 出站后必须被钉成 True
    state = SimpleNamespace(upstream_stream="auto")
    p = _stub(variant)
    p._channel = SimpleNamespace(_state=state)

    async def collect():
        frames = []
        async for f in getattr(mod, variant["cls_name"]).stream_chat(p, "m1", []):
            frames.append(f)
        return frames

    frames = asyncio.run(collect())
    assert frames and frames[0]["content"] == "ok"
    assert state.upstream_stream is True

    # 渠道态 = False（固定关）→ 同样被钉住
    state2 = SimpleNamespace(upstream_stream=False)
    p2 = _stub(variant)
    p2._channel = SimpleNamespace(_state=state2)

    async def collect2():
        async for f in getattr(mod, variant["cls_name"]).stream_chat(p2, "m1", []):
            pass

    asyncio.run(collect2())
    assert state2.upstream_stream is True

    # 池外实例（无渠道挂载）不炸、照常走流式
    p3 = _stub(variant)
    p3._channel = None

    async def collect3():
        frames = []
        async for f in getattr(mod, variant["cls_name"]).stream_chat(p3, "m1", []):
            frames.append(f)
        return frames

    assert asyncio.run(collect3())[0]["content"] == "ok"


# ==================== 设备码流形态（对齐 workbuddy2api cmd/login + admin 扫描器契约）====================

def test_begin_device_flow_shape(variant):
    """POST auth/state → 上游签发 {state, authUrl}，直接打开上游登录页（不自己拼 URL）。"""
    mod = _module(variant)
    upstream_url = ("https://www.workbuddy.cn/login/?platform=workbuddy"
                    "&state=s-uuid&version=5.3.14&loginSessionId=l-uuid")
    p = _stub(variant)
    p._make_session = lambda **kw: _FakeSessionCtx(
        routes_post={"/v2/plugin/auth/state": {"code": 0, "msg": "",
                                               "data": {"state": "s-uuid", "authUrl": upstream_url}}})
    result = asyncio.run(getattr(mod, variant["cls_name"]).begin_device_flow(p))
    # admin._invoke_begin_device_flow 强校验 task_type，缺失直接 500。
    assert result["task_type"] == "device_code"
    # authUrl 原样透传上游签发的登录页（workbuddy.cn 域，不重写成 base_url）。
    assert result["auth_url"] == upstream_url
    assert result["verification_uri"] == upstream_url
    assert result["poll_params"].get("state") == "s-uuid"
    assert int(result["expires_in"]) > 0 and int(result["interval"]) > 0


def test_begin_device_flow_requires_state_and_authurl(variant):
    mod = _module(variant)
    from fastapi import HTTPException
    for bad in ({"code": 0, "data": {"state": "s"}},                # 缺 authUrl
                {"code": 0, "data": {"authUrl": "https://x"}},      # 缺 state
                {"code": 11217, "msg": "no"}):                      # 业务码非 0
        p = _stub(variant)

        def _mk_session(bad=bad, **kw):
            return _FakeSessionCtx(routes_post={"/v2/plugin/auth/state": bad})

        p._make_session = _mk_session
        with pytest.raises(HTTPException):
            asyncio.run(getattr(mod, variant["cls_name"]).begin_device_flow(p))


def test_poll_device_flow_shapes(variant):
    """GET auth/token → 登录中=业务码≠0 / HTTP 非 200；完成=code0+token，再取 login/account。"""
    mod = _module(variant)

    def _p(routes):
        p = _stub(variant)
        p._make_session = lambda **kw: _FakeSessionCtx(routes)
        return p

    # 登录中：HTTP 200 + 业务码≠0（"login ing..."）→ pending
    r = asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(
        _p({"/v2/plugin/auth/token": {"code": 11217, "msg": "login ing..."}}), {"state": "s1"}))
    assert r == {"status": "pending"}

    # HTTP 非 200（4xx/5xx）→ 抛出（扫描器当 pending 下轮重试）
    from fastapi import HTTPException
    p_503 = _stub(variant)

    class _BadResp:
        status = 503
        async def text(self):
            return "unavailable"
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False

    class _BadSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def get(self, url, **kw):
            return _BadResp()

    p_503._make_session = lambda **kw: _BadSession()
    with pytest.raises(HTTPException):
        asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(p_503, {"state": "s2"}))

    # 完成：code0 + accessToken/refreshToken/domain → authorized；
    # login/account 补 uid/enterpriseId（补不齐时 plugin/accounts 兜底）；
    # account_data key ⊆ schema ∪ {原生字段 username, access_token} ∪ ACCOUNT_FIELDS
    r = asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(
        _p({"/v2/plugin/auth/token": {"code": 0, "msg": "",
                                      "data": {"accessToken": "tok-x", "refreshToken": "rt-x",
                                               "domain": "copilot.tencent.com"}},
            "/v2/plugin/login/account": {"code": 0, "msg": "",
                                         "data": {"uid": "u-x", "enterpriseId": "ent-x"}}}),
        {"state": "s3"}))
    assert r["status"] == "authorized"
    ad = r["account_data"]
    assert ad["password"] == "tok-x" and ad["access_token"] == "tok-x"
    assert ad["refresh_token"] == "rt-x" and ad["user_id"] == "u-x" and ad["enterprise_id"] == "ent-x"
    assert ad["domain"] == "copilot.tencent.com"
    # username 有真实值（此处 login/account 只给 uid，按链回退到 uid）——不再是服务端占位名
    assert ad["username"] == "u-x"
    cls = getattr(mod, variant["cls_name"])
    schema_keys = {f["key"] for f in cls.account_schema()["fields"]}
    assert set(ad) <= schema_keys | {"access_token", "username"} | set(cls.ACCOUNT_FIELDS)

    # login/account + plugin/accounts 双路：login/account 给 uid，accounts 按 uid 命中补 username
    r = asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(
        _p({"/v2/plugin/auth/token": {"code": 0, "msg": "",
                                      "data": {"accessToken": "tok-z", "refreshToken": "rt-z"}},
            "/v2/plugin/login/account": {"code": 0, "msg": "", "data": {"uid": "u-z"}},
            "/v2/plugin/accounts": {"code": 0, "msg": "",
                                    "data": {"accounts": [{"uid": "u-z", "username": "buddy@qq.com",
                                                           "nickname": "Buddy", "enterpriseId": "ent-z"}]}}}),
        {"state": "s5"}))
    ad = r["account_data"]
    assert ad["username"] == "buddy@qq.com" and ad["user_id"] == "u-z"
    assert ad["enterprise_id"] == "ent-z"

    # accounts 多条且 uid 无法命中 → **不盲取第一条**（串号源头：落库按 username 合并，
    # 拿错的 username 会把凭据覆盖进另一行已存在账号——2026-09-12 用户实测踩到）
    r = asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(
        _p({"/v2/plugin/auth/token": {"code": 0, "msg": "",
                                      "data": {"accessToken": "tok-w", "refreshToken": "rt-w"}},
            "/v2/plugin/login/account": {"code": 0, "msg": "", "data": {"nickname": "Partial"}},
            "/v2/plugin/accounts": {"code": 0, "msg": "",
                                    "data": {"accounts": [{"uid": "u-a", "username": "a@qq.com"},
                                                           {"uid": "u-b", "username": "b@qq.com"}]}}}),
        {"state": "s6"}))
    ad = r["account_data"]
    assert "user_id" not in ad          # uid 无参照 + 多条 → 宁缺勿错
    assert ad["username"] == "Partial"  # login/account 自己给的信息保留

    # accounts 恰好唯一条目（无 uid 参照）→ 采纳
    r = asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(
        _p({"/v2/plugin/auth/token": {"code": 0, "msg": "",
                                      "data": {"accessToken": "tok-v", "refreshToken": "rt-v"}},
            "/v2/plugin/login/account": {"code": 404, "msg": "", "data": {}},
            "/v2/plugin/accounts": {"code": 0, "msg": "",
                                    "data": {"accounts": [{"uid": "u-solo", "username": "solo@qq.com"}]}}}),
        {"state": "s7"}))
    ad = r["account_data"]
    assert ad["username"] == "solo@qq.com" and ad["user_id"] == "u-solo"

    # login/account 挂了且 accounts 也拿不到：token 照常落号，不写空字段
    r = asyncio.run(getattr(mod, variant["cls_name"]).poll_device_flow(
        _p({"/v2/plugin/auth/token": {"code": 0, "msg": "",
                                      "data": {"accessToken": "tok-y", "refreshToken": "rt-y"}}}),
        {"state": "s4"}))
    assert r["status"] == "authorized" and "user_id" not in r["account_data"] and "username" not in r["account_data"]


# ==================== 11128 风控脱敏（2026-09-12 实测修复核心）====================

def test_user_info_extraction(variant):
    """用户信息提取：单/双层 data、account/user 子对象、int uid 多形态并集，先到先得。"""
    mod = _module(variant)
    env = {"code": 0, "data": {"account": {"uid": 12345, "username": "buddy@qq.com",
                                           "nickname": "Nick", "enterpriseId": "e1"},
                               "user": {"email": "buddy@qq.com"}}}
    info = mod._unwrap_acct_info(env)
    assert mod._pick_field(info, "uid", "id") == "12345"
    assert mod._pick_field(info, "username", "login", "nickname") == "buddy@qq.com"
    assert mod._pick_field(info, "email", "mail") == "buddy@qq.com"
    assert mod._pick_field(info, "enterpriseId", "eid") == "e1"
    # 双层 data + 顶层扁平形态
    info2 = mod._unwrap_acct_info({"code": 0, "data": {"data": {"uid": "u9", "login": "u9name"}}})
    assert mod._pick_field(info2, "uid", "id") == "u9"
    assert mod._pick_field(info2, "username", "login", "nickname") == "u9name"
    # accounts 列表双形态
    assert mod._accounts_entries([{"uid": "a"}]) == [{"uid": "a"}]
    assert mod._accounts_entries({"accounts": [{"uid": "b"}]}) == [{"uid": "b"}]


def test_sanitize_rewrites_sdk_variant_identity(variant):
    """身份句改写按核心片段（不带句读）——CLI 句号版与 Agent SDK 逗号续接版都要命中。"""
    mod = _module(variant)
    sdk = "You are Claude Code, Anthropic's official CLI for Claude, running within the Claude Agent SDK."
    out = mod._sanitize_outbound_body({"system": sdk})
    assert "official CLI tool for Claude" in out["system"]
    assert "You are Claude Code" in out["system"]  # 只改黑名单核心片段，身份语义保留
    cli = "You are Claude Code, Anthropic's official CLI for Claude."
    assert "official CLI tool for Claude" in mod._sanitize_text(cli)


def test_sanitize_rewrites_are_idempotent(variant):
    """改写新串不得包含旧串（重试/重入滚雪球回归，2026-09-12 对照 workbuddy2api 抓到）。"""
    mod = _module(variant)
    for old, new in mod._SANITIZE_REWRITES:
        assert old not in new, (old, new)
    # Codex 句任意变体（CLI 句号版 / 逗号续接版）都改写、且二次过钩子不再变化
    for sent in ("You are a coding agent running in the Codex CLI, a terminal-based coding assistant.",
                 "You are a coding agent running in the Codex CLI, a terminal-based coding assistant. "
                 "Codex CLI is an open source project led by OpenAI."):
        once = mod._sanitize_text(sent)
        assert once != sent and "inside the Codex CLI" in once, sent
        twice = mod._sanitize_text(once)
        assert twice == once, (once, twice)
    # 三连过钩子也不长毛刺
    body = {"messages": [{"role": "user", "content": "You are a coding agent running in the Codex CLI, a terminal-based coding assistant."}]}
    b1 = mod._sanitize_outbound_body(body)
    b2 = mod._sanitize_outbound_body(b1)
    b3 = mod._sanitize_outbound_body(b2)
    assert b3["messages"][0]["content"] == b2["messages"][0]["content"] == b1["messages"][0]["content"]
    assert "tool tool" not in b3["messages"][0]["content"] and "inside inside" not in b3["messages"][0]["content"]


def test_sanitize_strips_kv_and_normalizes(variant):
    mod = _module(variant)
    body = {
        "model": "opus",
        "messages": [
            {"role": "developer", "content": "Main branch (you will usually use this for PRs)"},
            {"role": "user", "content": "hello x-anthropic-billing-header: fake; cc_version=1.2;"},
        ],
        "tool_choice": {"type": "any"},
    }
    snapshot = {"system": None, "m0": dict(body["messages"][0])}
    out = mod._sanitize_outbound_body(body)
    assert out["messages"][0]["role"] == "system"
    assert "Default branch" in out["messages"][0]["content"]
    assert "x-anthropic-billing-header" not in out["messages"][1]["content"]
    assert "cc_version=" not in out["messages"][1]["content"]
    assert out["tool_choice"] == "required"
    # 幂等：重试二次过钩子不再变化
    out2 = mod._sanitize_outbound_body(out)
    assert out2["messages"] == out["messages"]
    # 不污染原对象（与请求日志/客户端共享的 dict 不动）
    assert body["messages"][0] == snapshot["m0"]
    # tool_choice 指定函数名 → 函数名 string
    out3 = mod._sanitize_outbound_body({"tool_choice": {"type": "function", "function": {"name": "grep"}}})
    assert out3["tool_choice"] == "grep"
    # none + tools → 整组删除（上游对 none+tools 组合报错）
    out4 = mod._sanitize_outbound_body({"tool_choice": "none", "tools": [{"x": 1}]})
    assert "tool_choice" not in out4 and "tools" not in out4
    # 无指纹原对象直返（零拷贝）
    clean = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "tool_choice": "auto"}
    assert mod._sanitize_outbound_body(clean) is clean
