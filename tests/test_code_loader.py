"""代码渠道加载器单测（v2：spec 类模型）。

验证: exec 贴的源码 → 取 spec 类（带可识别钩子的普通类）→ 包成 CustomProvider 适配器 →
PROVIDER_NAME 对齐 channel id → 缓存 → 各类错误清晰抛 CodeChannelError。
"""
import pytest

from providers.code_loader import (
    CodeChannelError,
    invalidate_cache,
    load_code_provider_class,
)
from providers.custom import CustomProvider


ECHO_SRC = """
class EchoChannel:
    @staticmethod
    async def init_auth(p, is_check=False):
        return True

    @staticmethod
    async def fetch_models(p):
        return []

    @staticmethod
    async def stream_chat(p, model_id, messages, **kw):
        msg = messages[-1].get("content", "") if messages else ""
        yield {"content": msg, "thinking": "", "tool_calls": []}
"""


@pytest.fixture(autouse=True)
def _clean_cache():
    invalidate_cache()
    yield
    invalidate_cache()


def test_loads_adapter_and_overrides_provider_name():
    cls = load_code_provider_class("my-channel", ECHO_SRC)
    # 适配器类名以 CodeChannel_ 前缀 + 渠道 id；spec 类通过 cls.spec 暴露
    assert cls.__name__ == "CodeChannel_my-channel"
    assert cls.spec.__name__ == "EchoChannel"
    assert issubclass(cls, CustomProvider)
    # loader 强制 PROVIDER_NAME = 渠道 id，redis_prefix / 日志对齐
    assert cls.PROVIDER_NAME == "my-channel"
    inst = cls(username="u", password="p")
    assert inst.redis_prefix == "ai-lubricant:my-channel:u"


def test_spec_class_does_not_need_provider_name():
    """spec 类无需（也不应）写 PROVIDER_NAME：loader 强制覆盖为渠道 id。"""
    src = """
class EchoChannel:
    @staticmethod
    async def init_auth(p, is_check=False): return True
    @staticmethod
    async def stream_chat(p, m, msgs, **k):
        yield {"content": "hi", "thinking": "", "tool_calls": []}
"""
    cls = load_code_provider_class("channel-id", src)
    assert cls.PROVIDER_NAME == "channel-id"
    inst = cls(username="u", password="p")
    assert inst.redis_prefix == "ai-lubricant:channel-id:u"


def test_cache_returns_same_class_for_unchanged_source():
    a = load_code_provider_class("ch", ECHO_SRC)
    b = load_code_provider_class("ch", ECHO_SRC)
    assert a is b  # 同源码不重 exec


def test_cache_invalidates_on_source_change():
    a = load_code_provider_class("ch", ECHO_SRC)
    b_src = ECHO_SRC.replace("EchoChannel", "Echo2")
    b = load_code_provider_class("ch", b_src)
    assert b is not a
    assert b.spec.__name__ == "Echo2"
    assert b.PROVIDER_NAME == "ch"  # 仍覆盖为 channel id


def test_syntax_error_raises_code_channel_error_with_lineno():
    bad = "class EchoChannel:\n    @staticmethod\n    async def init_auth(p):\n        return True\n     bad-indent"
    with pytest.raises(CodeChannelError) as exc:
        load_code_provider_class("bad-syntax", bad)
    msg = str(exc.value)
    assert "语法错误" in msg or "syntax" in msg.lower()
    assert "行" in msg or "lineno" in msg.lower()


def test_no_spec_class_raises():
    with pytest.raises(CodeChannelError) as exc:
        load_code_provider_class("no-spec", "x = 1\ny = 2\n")
    assert "未定义" in str(exc.value)


def test_multiple_spec_classes_raises():
    src = ECHO_SRC + "\n\nclass OtherChannel:\n    @staticmethod\n    async def init_auth(p): return True\n"
    with pytest.raises(CodeChannelError) as exc:
        load_code_provider_class("multi", src)
    assert "一个" in str(exc.value)


def test_class_with_no_recognizable_hooks_raises_with_hint():
    """有 spec 类但没写任何可识别钩子 -> 报错并提示未识别方法名（防拼错静默不生效）。"""
    src = """
class BadChannel:
    @staticmethod
    async def inot_auth(p):  # 拼错的钩子名
        return True
    @staticmethod
    async def stream_cht(p, m, msgs, **k):  # 拼错
        yield {}
"""
    with pytest.raises(CodeChannelError) as exc:
        load_code_provider_class("no-hooks", src)
    msg = str(exc.value)
    assert "未定义任何可识别钩子" in msg or "未定义" in msg
    assert "inot_auth" in msg  # 未识别方法名进报文


def test_base_provider_subclass_is_rejected_with_migration_hint():
    """误继承框架基类的类 -> 明确拒绝并给迁移指引，避免两套写法并存。"""
    src = """
class Legacy(BaseProvider):
    PROVIDER_NAME = "legacy"
    async def init_auth(self, is_check=False): return True
    async def _do_stream_chat(self, m, msgs, **k): yield {}
    async def _do_non_stream_chat(self, m, msgs, **k): return {}
"""
    with pytest.raises(CodeChannelError) as exc:
        load_code_provider_class("legacy", src)
    msg = str(exc.value)
    assert "不再继承" in msg or "不应继承" in msg
    assert "Legacy" in msg


def test_empty_source_raises():
    with pytest.raises(CodeChannelError):
        load_code_provider_class("empty", "")
    with pytest.raises(CodeChannelError):
        load_code_provider_class("empty2", "   \n  ")


def test_injected_helpers_available():
    """贴的代码能用注入的 helper（logger / json / Channel / AccountClient / ModelClientPool …）少写 import。"""
    src = """
class PChannel:
    uses = {
        "logger": type(logger).__name__,
        "json": json.dumps({"a": 1}),
        "channel_cls": Channel.__name__,
        "account_client_cls": AccountClient.__name__,
        "pool_cls": ModelClientPool.__name__,
        "limit_state_cls": ProviderLimitState.__name__,
        "proxy_mgr": callable(get_proxy_manager),
        "completion_id_prefix": generate_completion_id().split("-")[0],
        # 文本转工具调用 helper（prompt 注入型渠道必备）
        "parse_fn": callable(parse_tool_calls_from_content),
        "prompt_fn": callable(generate_tools_prompt),
        "render_json_fn": callable(render_tool_call_json),
        "render_json_result_fn": callable(render_tool_result_json),
        "render_xml_fn": callable(render_tool_call_xml),
        "render_xml_result_fn": callable(render_tool_result_xml),
    }
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
    cls = load_code_provider_class("helpers", src)
    inst = cls(username="u", password="p")
    assert inst.spec.uses["logger"] == "Logger"
    assert inst.spec.uses["json"] == '{"a": 1}'
    assert inst.spec.uses["channel_cls"] == "Channel"
    assert inst.spec.uses["account_client_cls"] == "AccountClient"
    assert inst.spec.uses["pool_cls"] == "ModelClientPool"
    assert inst.spec.uses["limit_state_cls"] == "ProviderLimitState"
    assert inst.spec.uses["proxy_mgr"] is True
    assert inst.spec.uses["completion_id_prefix"] == "chatcmpl"
    for key in ("parse_fn", "prompt_fn", "render_json_fn",
                "render_json_result_fn", "render_xml_fn", "render_xml_result_fn"):
        assert inst.spec.uses[key] is True, key


def test_usage_helpers_not_injected():
    """usage 交给框架自动估算，不再注入 estimate_usage / normalize_usage / merge_usage / aiohttp。

    作者要自定义 usage 只需 yield {"usage": {...}}（流式）或在响应体放 usage（非流式），
    不该再手拼；发请求也统一走 p.send_sse_request / p._make_session，不注入裸 aiohttp。
    """
    for missing in ("estimate_usage", "normalize_usage", "merge_usage", "aiohttp"):
        src = f"""
class NeedHelper:
    x = {missing}
    @staticmethod
    async def init_auth(p, is_check=False): return True
"""
        with pytest.raises(CodeChannelError) as exc:
            load_code_provider_class(f"need-{missing}", src)
        assert missing in str(exc.value)  # NameError 冒泡成可读的执行失败报文


def test_catalog_sample_code_loads_and_runs():
    """目录预设里的文档化样例源码必须能 exec、能实例化、能跑通 echo。"""
    from user_platform.marketplace.channel_catalog import _code_entry

    sample = _code_entry()["preset"]["code"]
    assert sample and sample.strip()
    cls = load_code_provider_class("catalog-sample", sample)
    assert cls.spec.__name__ == "EchoChannel"
    # 样例没写 PROVIDER_NAME，loader 仍覆盖为渠道 id
    assert cls.PROVIDER_NAME == "catalog-sample"
    inst = cls(username="u", password="p")
    assert inst.redis_prefix == "ai-lubricant:catalog-sample:u"


def test_prompt_injection_helpers_reachable_and_roundtrip():
    """场景 K：上游不认 function calling 时，spec 用 p.build_tools_prompt(json) +
    p.prepare_provider_messages 注入、p.parse_tool_calls 兜底解析、render_tool_call_json
    反向序列化历史轮——四个能力都必须经实例 / 注入命名空间可达，且文本→工具调用闭环成立。

    锁的是「给代码模板提供文本转工具方法」这条：p.build_tools_prompt 现在带 format_type
    参数（旧版硬编码 xml，拿不到 json 形态），parse/render 已注入。
    """
    import asyncio

    src = """
class TextToolChannel:
    @staticmethod
    async def init_auth(p, is_check=False):
        return True

    @staticmethod
    async def non_stream_chat(p, model_id, messages, **kwargs):
        tools = kwargs.get("tools") or []
        tools_prompt = p.build_tools_prompt(tools, format_type="tool_function") if tools else ""
        msgs = p.prepare_provider_messages(messages, tools_prompt)
        # 模拟上游把工具调用当正文吐出来（不守 function calling）
        full_content = '好的我来查。```tool_function\\n{"name":"get_weather","arguments":{"city":"北京"}}\\n```'
        full_tool_calls = []
        cleaned, parsed = p.parse_tool_calls(full_content)
        if parsed:
            full_tool_calls.extend(parsed)
        # 历史轮反向序列化也必须可用（注入的模块函数）
        _ = render_tool_call_json(parsed[0]) if parsed else ""
        return p.build_openai_response(
            generate_completion_id(), model_id, cleaned,
            tool_calls=full_tool_calls or None)

    @staticmethod
    async def stream_chat(p, model_id, messages, **kwargs):
        # 流式路径框架自动从 content 切 tool_calls；spec 只需正常 yield
        yield {"content": "ok", "thinking": "", "tool_calls": []}
"""
    cls = load_code_provider_class("text-tool", src)
    inst = cls(username="u", password="p", base_url="https://x.example")

    tools = [{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "天气",
            "parameters": {"type": "object",
                           "properties": {"city": {"type": "string"}}},
        },
    }]
    # 1) build_tools_prompt(tool_function) 产出 tool_function 围栏（统一形态）
    prompt = inst.build_tools_prompt(tools, format_type="tool_function")
    assert "```tool_function" in prompt
    assert '"name"' in prompt

    # 2) prepare_provider_messages 把 tools_prompt 拼成首条 system
    msgs = inst.prepare_provider_messages([{"role": "user", "content": "hi"}], prompt)
    assert msgs[0]["role"] == "system" and msgs[0]["content"] == prompt

    # 3) 文本→工具调用闭环：模型吐的 json 块被解析成 OpenAI tool_calls
    result = asyncio.run(inst._do_non_stream_chat(
        "m", [{"role": "user", "content": "hi"}], tools=tools))
    tcs = (result.get("choices", [{}])[0].get("message", {}) or {}).get("tool_calls") or []
    assert tcs and tcs[0]["function"]["name"] == "get_weather"
    import json
    assert json.loads(tcs[0]["function"]["arguments"]) == {"city": "北京"}
    # 工具块从正文里剥干净，只留前置文本
    assert (result.get("choices", [{}])[0].get("message", {}) or {}).get("content") == "好的我来查。"

    invalidate_cache("text-tool")


def test_non_stream_collect_parses_tool_function_fence():
    """非流式聚合路径必须把 tool_function 围栏解析成 tool_calls。

    eaichat 这类只写 stream_chat 的 TOOLS_AS_PROMPT 渠道，客户端发 stream=false 时
    走 _collect_stream_chat_result 把流式帧聚合成非流式响应。模型把工具调用当正文
    吐（```tool_function 围栏）时，聚合路径必须同口径地解析围栏成 tool_calls、把
    围栏从 content 里剥干净——否则围栏原样漏进 content、客户端拿不到 tool_calls
    （流式路径 base.py 缓冲切块已解析，非流式聚合这条路不能漏）。
    """
    import asyncio

    src = """
class FenceNonStreamChannel:
    TOOLS_AS_PROMPT = True
    REQUIRES_BASE_URL = False
    @staticmethod
    async def init_auth(p, is_check=False): return True
    @staticmethod
    async def stream_chat(p, model_id, messages, **kwargs):
        import json as _json
        # 模型把工具调用当正文逐帧吐（不守 function calling），围栏混在 content 里
        payload = _json.dumps({"name": "get_weather", "arguments": {"city": "北京"}})
        text = chr(96) * 3 + "tool_function" + chr(10) + payload + chr(10) + chr(96) * 3
        for ch in text:
            yield {"content": ch, "thinking": "", "tool_calls": []}
"""
    cls = load_code_provider_class("fence-non-stream", src)
    inst = cls(username="u", password="p")
    tools = [{"type": "function", "function": {
        "name": "get_weather", "description": "天气",
        "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}}]

    async def run():
        result = await inst._do_non_stream_chat(
            "m", [{"role": "user", "content": "hi"}], tools=tools)
        await inst.close()
        return result

    result = asyncio.run(run())
    message = (result.get("choices") or [{}])[0].get("message", {}) or {}
    tcs = message.get("tool_calls") or []
    assert tcs and tcs[0]["function"]["name"] == "get_weather"
    import json
    assert json.loads(tcs[0]["function"]["arguments"]) == {"city": "北京"}
    # 围栏必须从 content 里剥干净，客户端不能看到 ```tool_function 文本
    assert "tool_function" not in (message.get("content") or "")
    assert message.get("content") == ""

    invalidate_cache("fence-non-stream")


def test_chat_end_to_end_through_loaded_class():
    """贴的 EchoChannel 经 chat() 出标准 OpenAI SSE，PROVIDER_NAME 对齐 channel id。"""
    import asyncio
    import json

    src = """
class EchoChannel:
    @staticmethod
    async def init_auth(p, is_check=False): return True
    @staticmethod
    async def fetch_models(p): return []
    @staticmethod
    async def stream_chat(p, model_id, messages, **kw):
        msg = messages[-1].get("content", "") if messages else ""
        yield {"content": msg, "thinking": "", "tool_calls": []}
"""
    cls = load_code_provider_class("echo-e2e", src)
    p = cls(username="u", password="p")

    async def run():
        out = []
        async for piece in p.chat("echo", [{"role": "user", "content": "ping"}], stream=True):
            if isinstance(piece, str):
                out.append(piece)
        await p.close()
        return "".join(out)

    sse = asyncio.run(run())
    contents = []
    for line in sse.splitlines():
        line = line.strip()
        if line.startswith("data:") and line[5:].strip() not in ("", "[DONE]"):
            try:
                obj = json.loads(line[5:].strip())
                for ch in obj.get("choices") or []:
                    d = ch.get("delta") or {}
                    if d.get("content"):
                        contents.append(d["content"])
            except json.JSONDecodeError:
                pass
    assert "".join(contents) == "ping"
    assert "[DONE]" in sse
