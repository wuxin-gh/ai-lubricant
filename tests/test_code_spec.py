"""代码渠道 spec 适配器单测。

验证 build_adapter_class 把一个 spec 类包成 CustomProvider 子类后的行为：
- @staticmethod 与普通 def 两种写法都能调
- 只写 stream_chat / 只写 non_stream_chat 的聚合规则
- headers 后处理、account_schema classmethod 透出、设备码钩子、类级开关复制
- 零钩子 / 多候选 / 误继承基类 的报错
"""
import asyncio
import json

import pytest

from providers.code_spec import (
    CodeSpecError,
    HOOK_ALIASES,
    build_adapter_class,
)
from providers.custom import CustomProvider


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


# ---- 调用约定 ----

def test_staticmethod_and_plain_def_both_work():
    """@staticmethod init_auth(p,...) 与 def init_auth(self,...) 两种写法语义一致。"""
    static_src = """
class S:
    @staticmethod
    async def init_auth(p, is_check=False):
        return True
    @staticmethod
    async def stream_chat(p, m, msgs, **k):
        yield {"content": "s", "thinking": "", "tool_calls": []}
"""
    plain_src = """
class P:
    async def init_auth(p, is_check=False):
        return True
    async def stream_chat(p, m, msgs, **k):
        yield {"content": "p", "thinking": "", "tool_calls": []}
"""
    ns_s: dict = {}
    exec(static_src, ns_s)
    ns_p: dict = {}
    exec(plain_src, ns_p)
    cls_s = build_adapter_class("s", ns_s["S"])
    cls_p = build_adapter_class("p", ns_p["P"])
    assert _run(cls_s(username="u", password="").init_auth()) is True
    assert _run(cls_p(username="u", password="").init_auth()) is True


# ---- 聚合规则 ----

def test_only_stream_chat_aggregates_without_network():
    """只写 stream_chat，非流式走聚合、不对上游发第二次请求。"""
    src = """
class OnlyStream:
    @staticmethod
    async def stream_chat(p, model_id, messages, **kw):
        yield {"content": "hel", "thinking": "", "tool_calls": []}
        yield {"content": "lo", "thinking": "", "tool_calls": []}
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("only-stream", ns["OnlyStream"])
    p = cls(username="u", password="")
    # base_url 不可达；如果回落配置驱动会发网络，这里会抛连接异常。聚合应纯本地。
    result = _run(p._do_non_stream_chat("m", [{"role": "user", "content": "x"}]))
    assert result["choices"][0]["message"]["content"] == "hello"


def test_stream_only_dict_tool_call_aggregates_to_json_string():
    """只写 stream_chat 且 yield dict 形态 arguments（eaichat 形态）：非流式聚合不崩，
    arguments 被框架序列化成 JSON 字符串，content 一并保留。

    这是 eaichat 样例删掉 non_stream_chat + _normalize_tool_calls 的前提：框架
    _merge_tool_calls 现在容 dict 参数（旧实现 `str += dict` 直接 TypeError）。
    """
    src = """
class DictTool:
    @staticmethod
    async def stream_chat(p, model_id, messages, **kw):
        yield {"content": "查一下。", "thinking": "", "tool_calls": []}
        yield {"content": "", "thinking": "", "tool_calls": [
            {"index": 0, "id": "c1", "type": "function",
             "function": {"name": "get_weather", "arguments": {"city": "北京"}}}]}
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("dict-tool", ns["DictTool"])
    p = cls(username="u", password="")
    result = _run(p._do_non_stream_chat("m", [{"role": "user", "content": "x"}]))
    message = result["choices"][0]["message"]
    assert message["content"] == "查一下。"
    tcs = message.get("tool_calls") or []
    assert tcs and tcs[0]["function"]["name"] == "get_weather"
    args = tcs[0]["function"]["arguments"]
    assert isinstance(args, str)  # 协议要求字符串，不能是 dict
    assert json.loads(args) == {"city": "北京"}


def test_only_non_stream_chat_yields_single_frame():
    """只写 non_stream_chat，流式把结果拍成单帧。"""
    src = """
class OnlyNonStream:
    @staticmethod
    async def non_stream_chat(p, model_id, messages, **kw):
        return {"choices": [{"message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]}
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("only-nostream", ns["OnlyNonStream"])
    p = cls(username="u", password="")

    async def collect():
        out = []
        async for chunk in p._do_stream_chat("m", [{"role": "user", "content": "x"}]):
            out.append(chunk)
        return out

    frames = _run(collect())
    assert len(frames) == 1
    assert frames[0]["content"] == "ok"


# ---- 后处理钩子 ----

def test_headers_post_process_gets_super_result():
    """headers 钩子拿到框架算好的头并改写；返回 falsy 保留原值。"""
    src = """
class WithHeaders:
    @staticmethod
    def headers(p, base, kwargs):
        base["X-Custom"] = "added"
        return base
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("with-headers", ns["WithHeaders"])
    p = cls(username="u", password="")
    h = p._headers("openai", kwargs={})
    assert h["X-Custom"] == "added"


def test_headers_returning_falsy_keeps_super_result():
    src = """
class WithHeadersNone:
    @staticmethod
    def headers(p, base, kwargs):
        return None
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("with-headers-none", ns["WithHeadersNone"])
    p = cls(username="u", password="")
    h = p._headers("openai", kwargs={})
    assert isinstance(h, dict) and h  # 保留 super 结果


def test_payload_hook_receives_base_payload():
    """payload 钩子拿到框架构好的 body（kwargs['_base_payload']），改一个字段返回。"""
    src = """
class WithPayload:
    @staticmethod
    def payload(p, endpoint, model_id, messages, stream, kwargs):
        body = dict(kwargs["_base_payload"])
        body["vendor_mode"] = "fast"
        return body
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("with-payload", ns["WithPayload"])
    p = cls(username="u", password="", base_url="https://x.example")
    body = p._build_protocol_payload("openai", "m", [{"role": "user", "content": "hi"}], True)
    assert body["vendor_mode"] == "fast"
    # 框架标准字段仍在（证明是后处理而非从零构造）
    assert body.get("model") == "m"


# ---- account_schema classmethod ----

def test_account_schema_hook_surfaces_on_adapter():
    """spec 的 account_schema() 经适配器 classmethod 透出，provider_name 对齐渠道 id。"""
    src = """
class WithSchema:
    @staticmethod
    def account_schema():
        return {
            "display_name": "Demo",
            "fields": [{"key": "token", "type": "password", "secret": True}],
            "auth_start": {"enabled": True, "mode": "device_code"},
        }
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("demo", ns["WithSchema"])
    schema = cls.account_schema()
    assert schema["display_name"] == "Demo"
    assert schema["provider_name"] == "demo"
    assert schema["auth_start"]["enabled"] is True


def test_no_account_schema_falls_back_to_generic():
    src = """
class NoSchema:
    @staticmethod
    async def init_auth(p): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("no-schema", ns["NoSchema"])
    schema = cls.account_schema()
    # 回落 BaseProvider 通用 schema（password 字段）
    assert any(f.get("key") == "password" for f in schema.get("fields", []))


# ---- 设备码授权 ----

def test_device_flow_hooks_surface_as_instance_methods():
    """声明 begin_device_flow + poll_device_flow -> 适配器暴露实例方法，形态符合扫描器契约。"""
    src = """
class DeviceFlow:
    @staticmethod
    async def init_auth(p, is_check=False): return True
    @staticmethod
    async def begin_device_flow(p):
        return {
            "task_type": "device_code",
            "auth_url": "https://example.com/device",
            "user_code": "ABC",
            "verification_uri": "https://example.com/device",
            "expires_in": 900,
            "interval": 7,
            "poll_params": {"device_code": "dc", "interval": 7},
        }
    @staticmethod
    async def poll_device_flow(p, poll_params):
        return {"status": "authorized", "account_data": {"token": "t"}}
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("device", ns["DeviceFlow"])
    assert hasattr(cls, "begin_device_flow") and hasattr(cls, "poll_device_flow")
    p = cls(username="u", password="")
    started = _run(p.begin_device_flow())
    assert started["task_type"] == "device_code"
    assert started["user_code"] == "ABC"
    polled = _run(p.poll_device_flow({"device_code": "dc"}))
    assert polled["status"] == "authorized"


def test_no_device_flow_hook_not_present_on_adapter():
    """未声明 begin_device_flow -> 适配器不暴露该方法（admin getattr 探测走 501 回落）。"""
    src = """
class NoDevice:
    @staticmethod
    async def init_auth(p): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("no-device", ns["NoDevice"])
    assert "begin_device_flow" not in vars(cls)
    assert not hasattr(cls, "begin_device_flow") or "begin_device_flow" not in vars(cls)


def test_device_flow_hooks_receive_auth_context_when_signature_allows():
    """begin_device_flow / poll_device_flow 声明了 auth_context 尾参就收到框架上下文。

    老 spec（单参）不受影响——test_device_flow_hooks_surface_as_instance_methods 锁的那份
    源码就是单参写法，能跑通即向后兼容；这里锁新写法拿到的是同一份 dict。
    """
    seen: dict = {}

    src = """
class ContextAwareDevice:
    @staticmethod
    async def init_auth(p, is_check=False): return True
    @staticmethod
    async def begin_device_flow(p, auth_context=None):
        return {
            "task_type": "device_code",
            "auth_url": "https://example.com/device",
            "user_code": "",
            "verification_uri": "https://example.com/device",
            "expires_in": 900,
            "interval": 7,
            "poll_params": {"device_code": "dc"},
            "seen_origin": (auth_context or {}).get("origin", ""),
        }
    @staticmethod
    async def poll_device_flow(p, poll_params, auth_context=None):
        return {"status": "pending", "seen_origin": (auth_context or {}).get("origin", "")}
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("ctx-device", ns["ContextAwareDevice"])
    p = cls(username="u", password="")
    ctx = {"origin": "https://panel.example.com", "state": "s", "port": 8001}
    started = _run(p.begin_device_flow(ctx))
    assert started["seen_origin"] == "https://panel.example.com"
    polled = _run(p.poll_device_flow({"device_code": "dc"}, ctx))
    assert polled["seen_origin"] == "https://panel.example.com"
    seen["ok"] = True
    assert seen["ok"]


def test_loopback_callback_hook_presence_triggered():
    """声明 handle_loopback_callback 才注入适配器；未声明 admin 跳过该会话。"""
    src_with = """
class WithLoopback:
    @staticmethod
    async def init_auth(p, is_check=False): return True
    @staticmethod
    async def handle_loopback_callback(p, params, poll_params):
        if params.get("secret") == poll_params.get("secret"):
            return {"status": "claimed", "poll_params": {"secret": "portal"}, "redirect": "https://portal.example/next"}
        return None
"""
    ns: dict = {}
    exec(src_with, ns)
    cls = build_adapter_class("with-loopback", ns["WithLoopback"])
    assert "handle_loopback_callback" in vars(cls)
    p = cls(username="u", password="")
    claimed = _run(p.handle_loopback_callback(
        {"secret": "portal"}, {"secret": "portal"}))
    assert claimed["status"] == "claimed"
    assert _run(p.handle_loopback_callback({"secret": "x"}, {"secret": "portal"})) is None

    src_without = """
class WithoutLoopback:
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns2: dict = {}
    exec(src_without, ns2)
    cls2 = build_adapter_class("without-loopback", ns2["WithoutLoopback"])
    assert "handle_loopback_callback" not in vars(cls2)
    assert not hasattr(cls2, "handle_loopback_callback") or "handle_loopback_callback" not in vars(cls2)


# ---- 类级开关 ----

def test_class_flags_copied_to_adapter():
    src = """
class WithFlags:
    SUPPORTS_TOKEN_AUTO_REFRESH = True
    SCHEDULED_REFRESH = True
    @staticmethod
    async def init_auth(p): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("flags", ns["WithFlags"])
    assert cls.SUPPORTS_TOKEN_AUTO_REFRESH is True
    assert cls.SCHEDULED_REFRESH is True


def test_requires_base_url_defaults_true_and_can_be_waived():
    """渠道地址默认必填（继承 CustomProvider.REQUIRES_BASE_URL=True）；spec 可显式豁免。"""
    default_src = """
class NeedsUrl:
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    waived_src = """
class NoUrl:
    REQUIRES_BASE_URL = False
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns_a: dict = {}
    exec(default_src, ns_a)
    ns_b: dict = {}
    exec(waived_src, ns_b)
    assert build_adapter_class("needs-url", ns_a["NeedsUrl"]).REQUIRES_BASE_URL is True
    assert build_adapter_class("no-url", ns_b["NoUrl"]).REQUIRES_BASE_URL is False


# ---- 报错 ----

def test_zero_hooks_raises_with_hint():
    src = """
class Empty:
    @staticmethod
    async def mystream(p):  # 拼错的钩子名
        return True
"""
    ns: dict = {}
    exec(src, ns)
    with pytest.raises(CodeSpecError) as exc:
        build_adapter_class("empty", ns["Empty"])
    msg = str(exc.value)
    assert "未定义" in msg or "可识别钩子" in msg
    assert "mystream" in msg


def test_inherited_base_provider_raises():
    from providers.base import BaseProvider

    class Legacy(BaseProvider):
        async def init_auth(self, is_check=False): return True

    with pytest.raises(CodeSpecError) as exc:
        build_adapter_class("legacy", Legacy)
    assert "不应继承" in str(exc.value)


def test_adapter_is_custom_provider_subclass():
    src = """
class C:
    @staticmethod
    async def init_auth(p): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("c", ns["C"])
    assert issubclass(cls, CustomProvider)
    assert cls.spec is ns["C"]


def test_hook_aliases_documented():
    """HOOK_ALIASES 覆盖关键能力面，文档与样例口径锚定。"""
    for must in (
        "init_auth", "stream_chat", "non_stream_chat", "fetch_models",
        "headers", "build_url", "base_url", "payload", "parse_chunk",
        "account_schema", "build_auth_start", "handle_auth_callback",
        "begin_device_flow", "poll_device_flow", "handle_loopback_callback",
        "refresh_auth",
        "on_response_headers", "update_quota", "check_message",
        "on_channel_attached", "clear_conversations", "close",
    ):
        assert must in HOOK_ALIASES, must


# ---- 账号字段：OAuth 型渠道的前提 ----

# 这一组锚定「代码渠道能表达 OAuth 型渠道」。CustomProvider.__init__ 只取 _api_key，
# 其余账号字段（access_token / enterprise_id / …）原本会丢掉，spec 钩子读不到；
# admin 热更新又用 hasattr 判断要不要写回运行态实例，属性不存在就整段跳过。

def _account_spec_src() -> str:
    return """
class OAuthChannel:
    # 表单里没有、但运行时要读写的派生字段
    ACCOUNT_FIELDS = ("access_token_expires_at_ms",)

    @staticmethod
    def account_schema():
        return {
            "fields": [
                {"key": "access_token", "label": "Access Token", "type": "password"},
                {"key": "refresh_token", "label": "Refresh Token", "type": "password"},
                {"key": "enterprise_id", "label": "企业 ID", "type": "text"},
                {"key": "password", "label": "不该覆盖", "type": "password"},
            ],
        }

    @staticmethod
    async def init_auth(p, is_check=False):
        return bool(p.access_token)
"""


def test_account_fields_become_instance_attrs():
    """schema 声明的字段 ∪ ACCOUNT_FIELDS 都挂成实例属性，spec 钩子可 p.xxx 直读。"""
    ns: dict = {}
    exec(_account_spec_src(), ns)
    cls = build_adapter_class("oauth", ns["OAuthChannel"])
    inst = cls(username="u", password="pw", access_token="tok",
               enterprise_id="ent-1", access_token_expires_at_ms=123)

    assert inst.access_token == "tok"
    assert inst.enterprise_id == "ent-1"
    assert inst.access_token_expires_at_ms == 123
    # 钩子能读到字段
    assert _run(inst.init_auth()) is True


def test_account_fields_placeholder_when_absent():
    """缺值也要挂空串占位——admin 的 hasattr 门禁靠属性存在与否决定是否同步新 token。"""
    ns: dict = {}
    exec(_account_spec_src(), ns)
    cls = build_adapter_class("oauth-empty", ns["OAuthChannel"])
    inst = cls(username="u", password="pw")

    for key in ("access_token", "refresh_token", "enterprise_id", "access_token_expires_at_ms"):
        assert hasattr(inst, key), key
        assert getattr(inst, key) == ""


def test_account_fields_never_shadow_username_password():
    """username/password 归 BaseProvider 持有，schema 里声明了也不能被二次覆盖成空串。"""
    ns: dict = {}
    exec(_account_spec_src(), ns)
    cls = build_adapter_class("oauth-pw", ns["OAuthChannel"])
    assert "password" not in cls.ACCOUNT_FIELDS
    assert cls(username="u", password="pw").password == "pw"


def test_no_account_fields_keeps_default_init():
    """没有字段要挂的 spec 不注入 __init__，行为与改动前一致。"""
    src = """
class Plain:
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("plain", ns["Plain"])
    assert "__init__" not in vars(cls)
    assert not hasattr(cls, "ACCOUNT_FIELDS")


def test_account_fields_available_to_on_channel_attached():
    """池外临时实例（设备码扫描器 / 手动刷新）只在 __init__ 里 attach 一次 Channel，
    on_channel_attached 必须已经能看到账号字段。"""
    src = """
class AttachChannel:
    ACCOUNT_FIELDS = ("access_token",)

    @staticmethod
    def on_channel_attached(p):
        p.seen_token = p.access_token

    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("attach", ns["AttachChannel"])
    inst = cls(username="u", password="pw", access_token="tok")
    assert inst.seen_token == "tok"


def test_broken_account_schema_does_not_block_loading():
    """作者的 account_schema 抛异常时字段集退化成 ACCOUNT_FIELDS，渠道仍能加载。"""
    src = """
class BadSchema:
    ACCOUNT_FIELDS = ("access_token",)

    @staticmethod
    def account_schema():
        raise RuntimeError("boom")

    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("bad-schema", ns["BadSchema"])
    assert cls.ACCOUNT_FIELDS == ("access_token",)
    assert cls(username="u", password="pw").access_token == ""


# ---- base_url 钩子：动态上游地址 ----

def test_base_url_hook_overrides_all_outbound_paths():
    """base_url 钩子比 build_url 更底层：_url / 模型列表 / 图片视频语音全部跟着变。"""
    src = """
class DynamicBase:
    @staticmethod
    def base_url(p):
        return "https://dynamic.example.com"

    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("dyn-base", ns["DynamicBase"])
    inst = cls(username="u", password="pw", base_url="https://configured.example.com")

    assert inst.base_url == "https://dynamic.example.com"
    assert inst._url("/models") == "https://dynamic.example.com/models"
    assert inst._chat_url().startswith("https://dynamic.example.com")


def test_base_url_hook_falsy_falls_back_to_channel_config():
    """钩子返回空（还没拿到动态地址）时回落渠道配置里用户填的地址。"""
    src = """
class LazyBase:
    @staticmethod
    def base_url(p):
        return getattr(p, "_discovered", "")

    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("lazy-base", ns["LazyBase"])
    inst = cls(username="u", password="pw", base_url="https://configured.example.com")

    assert inst.base_url == "https://configured.example.com"
    inst._discovered = "https://found.example.com"
    assert inst.base_url == "https://found.example.com"


def test_no_base_url_hook_keeps_custom_provider_property():
    src = """
class NoBase:
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("no-base", ns["NoBase"])
    assert "base_url" not in vars(cls)
    assert cls(username="u", password="pw",
               base_url="https://configured.example.com").base_url == "https://configured.example.com"


# ---- APPLY_CLIENT_PRESET：关掉伪装头 ----

def test_apply_client_preset_false_strips_disguise_headers():
    """上游按自己的 CLI 校验请求头时，spec 写 APPLY_CLIENT_PRESET = False 关掉伪装头。

    关键：client_preset="none" 关不掉——_effective_client_preset 会兜到协议默认 preset
    （openai -> opencode），只有这个开关能真正关。
    """
    src = """
class BareHeaders:
    APPLY_CLIENT_PRESET = False

    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("bare", ns["BareHeaders"])
    inst = cls(username="u", password="pw", base_url="https://x.example",
               api_key="sk-1", client_preset="none")
    headers = inst._headers(kwargs={})

    assert headers.get("Authorization") == "Bearer sk-1"
    # opencode preset 的特征头一个都不该出现
    for leaked in ("x-stainless-lang", "x-stainless-runtime", "originator"):
        assert leaked not in {k.lower() for k in headers}, leaked


def test_client_preset_applied_by_default():
    """不声明 APPLY_CLIENT_PRESET 的 spec 保持原行为（套 preset）。"""
    src = """
class DefaultHeaders:
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("default-headers", ns["DefaultHeaders"])
    inst = cls(username="u", password="pw", base_url="https://x.example",
               api_key="sk-1", client_preset="none")
    headers = {k.lower() for k in inst._headers(kwargs={})}
    # openai + client_preset="none" 被 _effective_client_preset 兜到 opencode preset，
    # 其标志头是 x-session-affinity —— 正好证明「配置层关不掉伪装头」。
    assert "x-session-affinity" in headers


def test_apply_client_preset_false_still_runs_headers_hook():
    """关了 preset 之后，spec 自己的 headers 钩子照常后处理。"""
    src = """
class BarePlusHook:
    APPLY_CLIENT_PRESET = False

    @staticmethod
    def headers(p, base, kwargs):
        base["X-Custom"] = "1"
        return base

    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    ns: dict = {}
    exec(src, ns)
    cls = build_adapter_class("bare-hook", ns["BarePlusHook"])
    headers = cls(username="u", password="pw", base_url="https://x.example",
                  api_key="sk-1")._headers(kwargs={})
    assert headers["X-Custom"] == "1"
