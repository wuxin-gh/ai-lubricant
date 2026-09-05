import asyncio
import json

import pytest
from fastapi import HTTPException
from message_utils import make_openai_response
from providers.custom import CustomProvider
from channel import normalize_chat_protocols


class StreamCollectProvider(CustomProvider):
    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield {"content": "hel", "thinking": "", "tool_calls": []}
        yield {"content": "lo", "thinking": "", "tool_calls": []}
        yield {"content": "", "thinking": "", "tool_calls": [], "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}}


class SSEErrorProvider(CustomProvider):
    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield 'data: {"error":{"message":"Function id \'3b9748d8-1d85-40e8-8573-0eeaa63a4b63\': DEGRADED function cannot be invoked"}}\n\n'


class NonStreamFallbackProvider(CustomProvider):
    async def _do_non_stream_chat(self, model_id, messages, **kwargs):
        return make_openai_response("chatcmpl_test", model_id, "hello")


def test_stream_openai_error_payload_normalizes_degraded_function():
    import pytest
    from fastapi import HTTPException

    provider = SSEErrorProvider(
        "user",
        "key",
        provider_name="test-provider",
        base_url="https://example.test",
    )

    async def collect():
        chunks = []
        async for chunk in provider.chat("test-model", [{"role": "user", "content": "hi"}], stream=True):
            chunks.append(chunk)
        return chunks

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(collect())

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == {
        "error": {
            "message": "上游函数临时不可用，已按上游异常处理",
            "type": "server_error",
            "code": "upstream_function_degraded",
        }
    }


def test_openai_sse_error_detects_bare_json_after_heartbeat():
    upstream_error = {
        "error": {
            "message": (
                "This model's maximum context length is 202752 tokens. "
                "However, your messages resulted in 364164 tokens."
            ),
            "type": "bad_response_status_code",
            "param": "",
            "code": "bad_response_status_code",
        }
    }

    with pytest.raises(HTTPException) as exc_info:
        CustomProvider._check_openai_sse_error(
            ": PING\n\n" + json.dumps(upstream_error)
        )

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == upstream_error


def test_openai_sse_error_ignores_non_error_bare_json():
    CustomProvider._check_openai_sse_error(': PING\n\n{"status":"ok"}')


class CloudflareErrorsProvider(CustomProvider):
    """占位，仅用于通过 ``_do_non_stream_chat`` 触发真实响应。"""
    pass


def _cloudflare_errors_body():
    return {
        "success": False,
        "errors": [{
            "message": "Function id '3b9748d8-1d85-40e8-8573-0eeaa63a4b63': DEGRADED function cannot be invoked",
            "code": "function_degraded",
        }],
    }


def test_non_stream_cloudflare_errors_array_is_raised_and_normalized():
    import pytest
    from fastapi import HTTPException

    provider = CloudflareErrorsProvider(
        "user",
        "key",
        provider_name="test-provider",
        base_url="https://example.test",
        upstream_stream=False,
    )

    # 直接测试 error body 提取器 + 抛异常入口，避免 mock 整条 aiohttp 链路。
    with pytest.raises(HTTPException) as exc_info:
        provider._raise_upstream_error_from_body(_cloudflare_errors_body())

    assert exc_info.value.status_code == 503
    message = exc_info.value.detail
    assert "上游函数临时不可用，已按上游异常处理" in message
    assert "Function id" not in message
    assert "DEGRADED function cannot be invoked" not in message


def test_extract_upstream_error_matches_cloudflare_errors_shape():
    from providers.custom import CustomProvider

    message, code = CustomProvider._extract_upstream_error(_cloudflare_errors_body())
    assert "Function id" in message
    assert code == "function_degraded"


def test_non_stream_client_collects_forced_upstream_stream_response():
    provider = StreamCollectProvider(
        "user",
        "key",
        provider_name="test-provider",
        base_url="https://example.test",
        upstream_stream=True,
    )

    result = asyncio.run(provider._do_non_stream_chat("test-model", [{"role": "user", "content": "hi"}]))

    assert result["choices"][0]["message"]["content"] == "hello"
    assert result["usage"] == {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}


def test_stream_client_simulates_stream_when_upstream_stream_disabled():
    provider = NonStreamFallbackProvider(
        "user",
        "key",
        provider_name="test-provider",
        base_url="https://example.test",
        chat_protocols=[{"protocol": "openai", "path": "/v1/chat/completions", "upstream_stream": False}],
    )

    async def collect():
        chunks = []
        async for chunk in CustomProvider._do_stream_chat(provider, "test-model", [{"role": "user", "content": "hi"}]):
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect())

    assert chunks == [{"content": "hello", "thinking": "", "tool_calls": [], "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}]


# ---------------------------------------------------------------------------
# 上游有真实内容但 usage 缺失 / completion_tokens=0 时，绝不能判失败：
# 流式结束兜底应按内容估算 usage，非流式应删除零 usage 交给 _usage 估算。
# 只有「无内容 + 零 completion」才视为上游空响应。
# ---------------------------------------------------------------------------
from main import (  # noqa: E402
    _chunk_has_stream_content,
    _response_has_content,
    _validate_stream_completion,
    _validate_upstream_usage_payload,
    EmptyNonStreamResponseError,
    IncompleteStreamError,
)


def test_stream_content_detection_accepts_reasoning_dict_chunks():
    # 部分 provider 会先把上游 SSE 解析成 dict，再交给主链路。
    # content 为空但 thinking/reasoning_content 非空，仍属于真实模型输出。
    assert _chunk_has_stream_content({
        "content": "",
        "thinking": "These",
        "tool_calls": [],
        "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
    })
    assert _chunk_has_stream_content({
        "choices": [{"delta": {"content": "", "reasoning_content": " two"}}],
        "usage": None,
    })

    # 已见 reasoning 输出后，结束帧的零 completion usage 不应再判为空流。
    _validate_stream_completion(
        {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
        had_content=True,
    )

    # delta.reasoning（OpenRouter/gateway 形态）也属于真实模型输出。
    assert _chunk_has_stream_content({
        "choices": [{"delta": {"content": "", "reasoning": " thinking"}}],
        "usage": None,
    })
    assert _response_has_content({"choices": [{"message": {"reasoning": " thinking"}}]})


def test_stream_content_detection_keeps_empty_dict_chunk_invalid():
    assert not _chunk_has_stream_content({
        "content": "",
        "thinking": "",
        "tool_calls": [],
        "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
    })
    with pytest.raises(IncompleteStreamError):
        _validate_stream_completion(
            {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
            had_content=False,
        )


def test_validate_upstream_usage_skips_zero_completion_when_content_present():
    # usage 与 content 同 payload：有内容 → 删 usage、不抛
    payload_with_content_and_zero_usage = {
        "choices": [{"message": {"content": "我把 CodeBuddy 的 UA/版本头补齐。"}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 0, "total_tokens": 1},
    }
    _validate_upstream_usage_payload(payload_with_content_and_zero_usage, stream=False)
    assert "usage" not in payload_with_content_and_zero_usage

    # usage 与 content 分属不同 chunk：同 attempt 累计已见过内容 → 不抛
    _validate_upstream_usage_payload(
        {"usage": {"prompt_tokens": 1, "completion_tokens": 0, "total_tokens": 1}},
        stream=False,
        had_content=True,
    )

    # Anthropic 直通形态：content 为非空 list、usage output_tokens=0 → 不抛、删 usage
    anthropic_payload = {
        "id": "msg_x",
        "type": "message",
        "content": [{"type": "text", "text": "hello"}],
        "usage": {"input_tokens": 5, "output_tokens": 0},
    }
    _validate_upstream_usage_payload(anthropic_payload, stream=False)
    assert "usage" not in anthropic_payload


def test_validate_upstream_usage_fails_on_empty_response_with_zero_completion():
    import pytest

    with pytest.raises(EmptyNonStreamResponseError):
        _validate_upstream_usage_payload(
            {"usage": {"prompt_tokens": 1, "completion_tokens": 0, "total_tokens": 1}},
            stream=False,
            had_content=False,
        )


# ---------------------------------------------------------------------------
# 部分上游在结束帧会回非法 "delta": []（应为 dict）。所有 OpenAI delta 解析路径
# 必须兜底成空 dict，保留 finish_reason，不能抛 'list' object has no attribute 'get'。
# ---------------------------------------------------------------------------
from message_utils import convert_stream_to_anthropic as _csa  # noqa: E402


def _bailu_chunks():
    return [
        'data: {"id":"x","object":"chat.completion.chunk","model":"bailu-2.7-free","choices":[{"index":0,"delta":{"reasoning_content":"嗯","role":"assistant"},"finish_reason":null}]}\n\n',
        'data: {"id":"x","object":"chat.completion.chunk","model":"bailu-2.7-free","choices":[{"index":0,"delta":{"content":"你的"},"finish_reason":null}]}\n\n',
        'data: {"id":"x","object":"chat.completion.chunk","model":"bailu-2.7-free","choices":[{"index":0,"delta":[],"finish_reason":null}]}\n\n',
        'data: {"id":"x","object":"chat.completion.chunk","model":"bailu-2.7-free","choices":[{"index":0,"delta":[],"finish_reason":"stop"}]}\n\n',
        'data: [DONE]\n\n',
    ]


def test_convert_stream_to_anthropic_tolerates_list_delta():
    async def gen():
        for c in _bailu_chunks():
            yield c

    events: list[str] = []

    async def collect():
        async for ev in _csa(gen(), "bailu-2.7-free"):
            events.append(ev)

    asyncio.run(collect())

    types = [l for ev in events for l in ev.splitlines() if l.startswith("event:")]
    assert types[0] == "event: message_start"
    assert types[-1] == "event: message_stop"
    assert any("thinking_delta" in ev for ev in events)
    assert any("text_delta" in ev for ev in events)
    assert any('"stop_reason": "end_turn"' in ev for ev in events)


def test_convert_stream_to_anthropic_fallback_estimates_input_and_output_usage():
    """上游不返回 usage 时，message_start / message_delta 必须按请求和输出内容兜底估算。"""
    import json as _json

    async def gen():
        # 无任何 usage chunk，纯内容 + 结束
        yield 'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"你的吗？"},"finish_reason":null}]}\n\n'
        yield 'data: {"id":"x","choices":[{"index":0,"delta":[],"finish_reason":"stop"}]}\n\n'
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in _csa(
            gen(),
            "m",
            request_messages=[{"role": "user", "content": "你在吗"}],
        ):
            events.append(ev)

    asyncio.run(collect())

    message_start_line = next(
        line for ev in events for line in ev.splitlines()
        if line.startswith("data:") and '"message_start"' in line
    )
    message_delta_line = next(
        line for ev in events for line in ev.splitlines()
        if line.startswith("data:") and '"message_delta"' in line
    )

    start_usage = _json.loads(message_start_line[5:].strip())["message"]["usage"]
    delta_usage = _json.loads(message_delta_line[5:].strip())["usage"]

    assert start_usage["input_tokens"] > 0, start_usage  # 由请求估算
    assert delta_usage["output_tokens"] > 0, delta_usage  # 由输出内容估算
    assert delta_usage["input_tokens"] >= start_usage["input_tokens"], delta_usage


def test_convert_stream_to_anthropic_reasoning_text_tool_calls_preserves_signature_and_usage():
    """reasoning -> text -> tool_calls + choices:[] usage 必须保持可执行和真实 token。"""
    from message_utils import PROXY_SYNTHETIC_THINKING_SIGNATURE

    def _chunk(delta=None, finish_reason=None, usage=None):
        payload = {
            "id": "chatcmpl_regression",
            "object": "chat.completion.chunk",
            "model": "glm-5.2",
            "choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish_reason}],
        }
        if usage is not None:
            payload["choices"] = []
            payload["usage"] = usage
        return json.dumps(payload)

    async def gen():
        yield f"data: {_chunk({'role': 'assistant'})}\n\n"
        yield f"data: {_chunk({'reasoning_content': '先分析'})}\n\n"
        yield f"data: {_chunk({'content': '调用工具'})}\n\n"
        yield f"data: {_chunk({'tool_calls': [{'index': 0, 'id': 'call_regression', 'type': 'function', 'function': {'name': 'lookup', 'arguments': '{'}}]})}\n\n"
        yield f"data: {_chunk({'tool_calls': [{'index': 0, 'type': 'function', 'function': {'arguments': '\"q\":\"hello\"}'}}]})}\n\n"
        yield f"data: {_chunk({}, finish_reason='tool_calls')}\n\n"
        yield f"data: {_chunk(usage={'prompt_tokens': 25627, 'completion_tokens': 169, 'total_tokens': 25796})}\n\n"
        yield "data: [DONE]\n\n"

    events = []

    async def collect():
        async for event in _csa(
            gen(),
            "glm-5.2",
            request_messages=[{"role": "user", "content": "hi"}],
        ):
            events.append(event)

    asyncio.run(collect())

    payloads = []
    for event in events:
        for line in event.splitlines():
            if line.startswith("data:"):
                body = line[5:].strip()
                if body not in ("", "[DONE]"):
                    payloads.append(json.loads(body))

    assert payloads[0]["type"] == "message_start"
    assert payloads[-1]["type"] == "message_stop"
    message_start = next(item for item in payloads if item["type"] == "message_start")
    message_delta = next(item for item in payloads if item["type"] == "message_delta")
    assert message_start["message"]["usage"]["input_tokens"] > 0
    assert message_delta["usage"] == {"input_tokens": 25627, "output_tokens": 169}
    assert message_delta["delta"]["stop_reason"] == "tool_use"

    thinking_start_index = next(
        index for index, item in enumerate(payloads)
        if item["type"] == "content_block_start"
        and item["content_block"]["type"] == "thinking"
    )
    thinking_start = payloads[thinking_start_index]
    assert thinking_start["content_block"]["signature"] == PROXY_SYNTHETIC_THINKING_SIGNATURE
    thinking_stop_index = next(
        index for index in range(thinking_start_index + 1, len(payloads))
        if payloads[index]["type"] == "content_block_stop"
        and payloads[index]["index"] == thinking_start["index"]
    )
    assert payloads[thinking_stop_index - 1]["delta"]["type"] == "signature_delta"
    assert any(
        item["type"] == "content_block_delta"
        and item["delta"]["type"] == "thinking_delta"
        for item in payloads
    )
    assert any(
        item["type"] == "content_block_delta"
        and item["delta"]["type"] == "text_delta"
        for item in payloads
    )
    assert any(
        item["type"] == "content_block_delta"
        and item["delta"]["type"] == "input_json_delta"
        for item in payloads
    )
    tool_start = next(
        item for item in payloads
        if item["type"] == "content_block_start"
        and item["content_block"]["type"] == "tool_use"
    )
    assert tool_start["content_block"]["id"] == "call_regression"
    assert tool_start["content_block"]["name"] == "lookup"

    # 闭环完整性：每个 content_block_start 都必须有且仅有一个同 index 的 content_block_stop，
    # 且 stop 必须出现在 start 之后、下一个不同 index 的 start 之前（block 不可嵌套）。
    starts = [item for item in payloads if item["type"] == "content_block_start"]
    stops = [item for item in payloads if item["type"] == "content_block_stop"]
    assert len(starts) == len(stops), (starts, stops)
    started_indices: dict[int, int] = {}
    open_at: dict[int, int] = {}
    for pos, item in enumerate(payloads):
        if item["type"] == "content_block_start":
            idx = item["index"]
            assert idx not in open_at, f"index {idx} 嵌套未闭环"
            open_at[idx] = pos
        elif item["type"] == "content_block_stop":
            idx = item["index"]
            assert idx in open_at, f"index {idx} 无 start 却 stop"
            started_indices[idx] = open_at.pop(idx)
    assert not open_at, f"未闭环的 block: {open_at}"


def test_convert_stream_to_anthropic_reasoning_to_tool_calls_closes_thinking():
    """reasoning 直跳 tool_calls（无中间 text）：thinking 块必须在收到 tool_calls 时立即闭环，
    不能拖到流尾兜底——否则 tool_use 与未关闭的 thinking 形成嵌套/乱序。"""
    def _chunk(delta=None, finish_reason=None, usage=None):
        payload = {
            "id": "chatcmpl_rt",
            "object": "chat.completion.chunk",
            "model": "glm-5.2",
            "choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish_reason}],
        }
        if usage is not None:
            payload["choices"] = []
            payload["usage"] = usage
        return json.dumps(payload)

    async def gen():
        yield f"data: {_chunk({'reasoning_content': '想想'})}\n\n"
        yield f"data: {_chunk({'tool_calls': [{'index': 0, 'id': 'call_rt', 'type': 'function', 'function': {'name': 'do', 'arguments': '{'}}]})}\n\n"
        yield f"data: {_chunk({'tool_calls': [{'index': 0, 'type': 'function', 'function': {'arguments': '"a":1}'}}]})}\n\n"
        yield f"data: {_chunk({}, finish_reason='tool_calls')}\n\n"
        yield f"data: {_chunk(usage={'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15})}\n\n"
        yield "data: [DONE]\n\n"

    events = []

    async def collect():
        async for event in _csa(gen(), "glm-5.2", request_messages=[{"role": "user", "content": "hi"}]):
            events.append(event)

    asyncio.run(collect())

    payloads = []
    for event in events:
        for line in event.splitlines():
            if line.startswith("data:"):
                body = line[5:].strip()
                if body not in ("", "[DONE]"):
                    payloads.append(json.loads(body))

    # thinking(0) 完全闭环后，才出现 tool_use(1) 的 start
    t_start = next(i for i, item in enumerate(payloads)
                   if item["type"] == "content_block_start" and item["content_block"]["type"] == "thinking")
    t_stop = next(i for i in range(t_start + 1, len(payloads))
                  if payloads[i]["type"] == "content_block_stop" and payloads[i]["index"] == payloads[t_start]["index"])
    tu_start = next(i for i, item in enumerate(payloads)
                    if item["type"] == "content_block_start" and item["content_block"]["type"] == "tool_use")
    assert t_stop < tu_start, "thinking 未在 tool_use 开始前闭环"
    # thinking stop 前紧贴 signature_delta
    assert payloads[t_stop - 1]["delta"]["type"] == "signature_delta"
    # tool_use 块的 start index 与 thinking 不同（新 block）
    assert payloads[tu_start]["index"] != payloads[t_start]["index"]


def test_parse_sse_data_tolerates_list_delta():
    from providers.base import BaseProvider

    content, thinking, tool_calls = BaseProvider.parse_sse_data(
        'data: {"choices":[{"index":0,"delta":[],"finish_reason":"stop"}]}\n\n'
    )
    assert content == "" and thinking == "" and tool_calls == []

    content, thinking, tool_calls = BaseProvider.parse_sse_data(
        'data: {"choices":[{"index":0,"delta":{"content":"hi","reasoning_content":"想"},"finish_reason":null}]}\n\n'
    )
    assert content == "hi" and thinking == "想" and tool_calls == []




# ---------------------------------------------------------------------------
# 端到端：发送给上游的请求体 stream 字段必须跟随渠道 upstream_stream 配置，
# 而不是客户端请求的 stream。捕获实际发往上游的 payload 验证。
# ---------------------------------------------------------------------------


class _FakeResp:
    def __init__(self):
        self.status = 200
        self.headers = {}

    async def text(self):
        return '{"choices":[{"message":{"content":"hi"}}]}'

    async def read(self):
        return b'{"choices":[{"message":{"content":"hi"}}]}'

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, capture):
        self._capture = capture

    def post(self, url, headers=None, json=None, data=None, proxy=None):
        self._capture.append(json if isinstance(json, dict) else {})
        return _FakeResp()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class UpstreamCaptureProvider(CustomProvider):
    """记录所有发往上游的请求体，用于断言 stream 字段。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.captured_bodies = []

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        body = kwargs.get("json")
        self.captured_bodies.append(body if isinstance(body, dict) else {})
        yield 'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
        yield 'data: [DONE]\n\n'

    def _make_session(self):
        return _FakeSession(self.captured_bodies)


def _run_chat(client_stream: bool, provider_stream: bool):
    provider = UpstreamCaptureProvider(
        "user",
        "key",
        provider_name="test-provider",
        base_url="https://example.test",
        protocol="openai",
        upstream_stream=provider_stream,
    )

    async def go():
        async for _ in provider.chat("test-model", [{"role": "user", "content": "hi"}], stream=client_stream):
            pass

    asyncio.run(go())
    return provider.captured_bodies


def test_client_nonstream_with_channel_stream_sends_upstream_stream_true():
    """客户端非流式 + 渠道配置流式：发给上游的 stream 必须是 true。"""
    bodies = _run_chat(client_stream=False, provider_stream=True)
    assert bodies, "no upstream body captured"
    assert all(b.get("stream") is True for b in bodies), bodies


def test_client_stream_with_channel_nonstream_sends_upstream_stream_false():
    """客户端流式 + 渠道配置非流式：发给上游的 stream 必须是 false。"""
    bodies = _run_chat(client_stream=True, provider_stream=False)
    assert bodies, "no upstream body captured"
    assert all(b.get("stream") is False for b in bodies), bodies


def test_client_stream_matches_channel_stream():
    """客户端与渠道一致时：上游 stream 跟随渠道配置。"""
    bodies_stream = _run_chat(client_stream=True, provider_stream=True)
    assert all(b.get("stream") is True for b in bodies_stream), bodies_stream

    bodies_nonstream = _run_chat(client_stream=False, provider_stream=False)
    assert all(b.get("stream") is False for b in bodies_nonstream), bodies_nonstream


def _run_chat_with_protocols(client_stream: bool, upstream_stream):
    """用显式 chat_protocols 行（含三态 upstream_stream）驱动 provider。"""
    provider = UpstreamCaptureProvider(
        "user",
        "key",
        provider_name="test-provider",
        base_url="https://example.test",
        protocol="openai",
        chat_protocols=[{
            "id": "openai-chat-0",
            "enabled": True,
            "protocol": "openai",
            "path": "/v1/chat/completions",
            "upstream_stream": upstream_stream,
            "client_preset": "none",
            "header_template": "",
            "models": [],
        }],
    )

    async def go():
        async for _ in provider.chat("test-model", [{"role": "user", "content": "hi"}], stream=client_stream):
            pass

    asyncio.run(go())
    return provider.captured_bodies


def test_auto_follows_client_stream_true():
    """auto（随客户端）：客户端流式 → 上游 stream 必须为 true。"""
    bodies = _run_chat_with_protocols(client_stream=True, upstream_stream="auto")
    assert bodies, "no upstream body captured"
    assert all(b.get("stream") is True for b in bodies), bodies


def test_auto_follows_client_stream_false():
    """auto（随客户端）：客户端非流式 → 上游 stream 必须为 false。"""
    bodies = _run_chat_with_protocols(client_stream=False, upstream_stream="auto")
    assert bodies, "no upstream body captured"
    assert all(b.get("stream") is False for b in bodies), bodies


def test_explicit_true_via_protocols_overrides_client():
    """协议行固定开：客户端非流式也强制上游 stream=true。"""
    bodies = _run_chat_with_protocols(client_stream=False, upstream_stream=True)
    assert bodies, "no upstream body captured"
    assert all(b.get("stream") is True for b in bodies), bodies


def test_explicit_false_via_protocols_overrides_client():
    """协议行固定关：客户端流式也强制上游 stream=false。"""
    bodies = _run_chat_with_protocols(client_stream=True, upstream_stream=False)
    assert bodies, "no upstream body captured"
    assert all(b.get("stream") is False for b in bodies), bodies


# ---------------------------------------------------------------------------
# 回归：上游错误帧务必立即转异常、不能原样透传/continue 吞掉。
# custom Anthropic 直通流/转写流、eaichat 流式/非流式
# 现在统一把检测到的错误转 HTTPException → 由 _chat_with_retry 切换账号重试。
# ---------------------------------------------------------------------------

class _ErrorSSEFeedProvider(CustomProvider):
    """可注入 send_sse_request 产出的 provider 基类，用于模拟上游 SSE 错误帧。"""
    def __init__(self, events: list[str] | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._events = events or []

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        for ev in self._events:
            yield ev


def test_anthropic_passthrough_stream_raises_on_openai_style_error_frame():
    """Anthropic 直通流收到 OpenAI 风格 {"error":...} 帧（无 type 字段）必须抛异常。

    修复前：parse_sse_type 返回 event_type="" → 绕过 ``if event_type=="error"``
    → 原样 ``yield event`` 透传给客户端。
    修复后：兜底 ``if isinstance(payload, dict) and payload.get("error")`` 拦截。
    """
    import pytest

    provider = _ErrorSSEFeedProvider(
        events=[
            'data: {"error":{"message":"Console API returned 429","type":"upstream_error","code":"upstream_error"}}\\n\\n',
            'data: {"type":"message","usage":{"input_tokens":100,"output_tokens":0}}\\n\\n',
            'data: [DONE]\\n\\n',
        ],
        username="user", password="key",
        provider_name="test-anthropic", base_url="https://example.test",
        protocol="anthropic",
    )

    async def collect():
        async for _ in provider._anthropic_passthrough_stream(
            "claude-sonnet-4-5", {"messages": [{"role":"user","content":"hi"}]},
        ):
            pass

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(collect())

    assert exc_info.value.status_code == 502
    detail = exc_info.value.detail
    assert "Console API returned 429" in detail


def test_anthropic_passthrough_stream_raises_on_bare_json_error_body():
    """Anthropic 直通流收到「HTTP 200 + 不带 data: 前缀的裸 JSON 错误体」必须抛异常。

    这类上游先发 SSE 心跳注释，最后直接吐一个 JSON 错误对象。parse_sse_type 对这种
    形态解析出的 payload 恒为空 dict，基于 payload 的两道 error 拦截全部漏判，错误
    原文被当正常内容透传给客户端，也不会进入重试/超限归一。
    """
    import pytest

    provider = _ErrorSSEFeedProvider(
        events=[
            ': PING\n\n',
            '{"error":{"code":"server_error","message":"thinking.budget_tokens must be less than max_tokens"}}',
        ],
        username="user", password="key",
        provider_name="test-anthropic", base_url="https://example.test",
        protocol="anthropic",
    )

    async def collect():
        async for _ in provider._anthropic_passthrough_stream(
            "claude-sonnet-4-5", {"messages": [{"role": "user", "content": "hi"}]},
        ):
            pass

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(collect())

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == {
        "error": {
            "code": "server_error",
            "message": "thinking.budget_tokens must be less than max_tokens",
        }
    }


def test_bare_json_thinking_budget_error_reaches_context_overflow_canonicalizer():
    """裸 JSON 错误体转成的 HTTPException 必须能被超限归一器识别。

    这是本次漏判的下游后果：漏抛异常时错误原文原样透传，客户端拿到的是上游的
    code=server_error 且没有 type 字段；抛出后归一器把它统一成 400 + 固定文案。
    """
    import retry_policy

    with pytest.raises(HTTPException) as exc_info:
        CustomProvider._check_bare_json_error(
            '{"error":{"code":"server_error","message":"thinking.budget_tokens must be less than max_tokens"}}'
        )

    canonical = retry_policy.canonicalize_upstream_error(
        exc_info.value.status_code, exc_info.value.detail
    )
    assert canonical is not None
    assert canonical.category == "thinking_budget_invalid"
    assert canonical.status_code == 400
    assert canonical.type == "invalid_request_error"
    assert canonical.message == "thinking.budget_tokens must be less than max_tokens"


def test_responses_passthrough_stream_raises_on_bare_json_error_body():
    """Responses 同协议直通流对裸 JSON 错误体的拦截，与 anthropic 直通对齐。"""
    import pytest

    provider = _ErrorSSEFeedProvider(
        events=[
            ': PING\n\n',
            '{"error":{"code":"server_error","message":"thinking.budget_tokens must be less than max_tokens"}}',
        ],
        username="user", password="key",
        provider_name="test-responses", base_url="https://example.test",
    )

    async def collect():
        async for _ in provider._responses_passthrough_stream("gpt-5", {"input": "hi"}):
            pass

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(collect())

    assert exc_info.value.status_code == 502


def test_bare_json_check_ignores_normal_sse_and_non_error_json():
    """不能扩大解析范围：普通 SSE 帧、非错误裸 JSON、多行 JSON 都不许抛。"""
    CustomProvider._check_bare_json_error('data: {"error":{"message":"x"}}\n\n')
    CustomProvider._check_bare_json_error(': PING\n\n{"status":"ok"}')
    CustomProvider._check_bare_json_error('event: error\ndata: {"error":{"message":"x"}}\n\n')
    CustomProvider._check_bare_json_error('{"error":{"message":"a"}}\n{"error":{"message":"b"}}')
    CustomProvider._check_bare_json_error('not json at all')


def test_anthropic_stream_chat_raises_on_openai_style_error_frame():
    """Anthropic 转写流（生成 OpenAI chunk 的 _anthropic_stream_chat）收到 OpenAI 风格
    error 帧也必须抛异常（与 passthrough 一致的兜底逻辑）。
    """
    import pytest

    provider = _ErrorSSEFeedProvider(
        events=[
            'data: {"error":{"message":"Rate limit exceeded","type":"rate_limit_error","code":"rate_limit_exceeded"}}\\n\\n',
        ],
        username="user", password="key",
        provider_name="test-anthropic", base_url="https://example.test",
        protocol="anthropic",
    )

    async def collect():
        async for _ in provider._anthropic_stream_chat(
            "claude-sonnet-4-5", {"messages": [{"role":"user","content":"hi"}]},
        ):
            pass

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(collect())

    assert exc_info.value.status_code == 429
    assert "Rate limit exceeded" in exc_info.value.detail



def test_parse_sse_usage_reads_comment_line_tokens():
    from providers.base import BaseProvider

    event = (
        'data: {"choices":[{"index":0,"delta":{"content":"<","role":"assistant"},'
        '"finish_reason":null}],"usage":null}\n'
        ': {"input_tokens":18930,"output_tokens":3,"chunk_tokens":1,"cached_tokens":null,'
        '"prefill_worker_id":7587896061590269241,"tokenize_latency":{"secs":0,"nanos":41098861}}\n\n'
    )
    usage = BaseProvider.parse_sse_usage(event)
    assert usage is not None
    assert usage["input_tokens"] == 18930
    assert usage["output_tokens"] == 3


def test_parse_sse_usage_ignores_non_token_comment_lines():
    """普通 keep-alive / ping 注释不含 token 字段，不能被误判成 usage。"""
    from providers.base import BaseProvider

    assert BaseProvider.parse_sse_usage(': keep-alive\n\n') is None
    assert BaseProvider.parse_sse_usage(': {"prefill_worker_id":123,"nanos":456}\n\n') is None
    # data: 层已有 usage 时优先用 data: 层，不看注释
    event = (
        'data: {"choices":[{"delta":{"content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}\n'
        ': {"input_tokens":999,"output_tokens":999}\n\n'
    )
    usage = BaseProvider.parse_sse_usage(event)
    assert usage == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}


class MiniMaxCommentUsageProvider(CustomProvider):
    """复刻 MiniMax 流：每个事件由 data: chunk + ": {token 统计}" 注释行组成，
    data: chunk 的 usage 恒为 null，真实 token 累计放在注释行。"""

    _CONTENT = ["<", "block", ">", "yes", "</", "block"]

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        input_tokens = 18930
        for i, piece in enumerate(self._CONTENT):
            output_tokens = i + 1
            data = {
                "id": "chatcmpl-mmx",
                "choices": [{"index": 0, "delta": {"content": piece, "role": "assistant"}, "finish_reason": None}],
                "model": "minimaxai/minimax-m3",
                "object": "chat.completion.chunk",
                "usage": None,
            }
            comment = {"input_tokens": input_tokens, "output_tokens": output_tokens, "chunk_tokens": 1, "cached_tokens": None}
            yield f"data: {json.dumps(data)}\n: {json.dumps(comment)}\n\n"
        # 结束帧：finish_reason=stop，仍带累计 token 注释
        final_data = {
            "id": "chatcmpl-mmx",
            "choices": [{"index": 0, "delta": {"content": "", "role": "assistant"}, "finish_reason": "stop"}],
            "model": "minimaxai/minimax-m3",
            "object": "chat.completion.chunk",
            "usage": None,
        }
        final_comment = {"input_tokens": input_tokens, "output_tokens": 7, "chunk_tokens": 1, "cached_tokens": None}
        yield f"data: {json.dumps(final_data)}\n: {json.dumps(final_comment)}\n\n"
        yield "data: [DONE]\n\n"


def test_minimax_comment_usage_flows_to_final_usage_chunk():
    """端到端：MiniMax 注释行 token 统计必须落到最终 usage chunk，而非内容估算。"""
    provider = MiniMaxCommentUsageProvider(
        "user",
        "key",
        provider_name="minimax",
        base_url="https://example.test",
        protocol="openai",
        upstream_stream=True,
    )

    async def collect():
        chunks = []
        async for chunk in provider.chat("minimax-m3", [{"role": "user", "content": "hi"}], stream=True):
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect())

    usage = None
    for chunk in chunks:
        if isinstance(chunk, str) and '"usage"' in chunk:
            for line in chunk.splitlines():
                line = line.strip()
                if not line.startswith("data:"):
                    continue
                body = line[5:].strip()
                if body in ("[DONE]", ""):
                    continue
                obj = json.loads(body)
                if isinstance(obj.get("usage"), dict):
                    usage = obj["usage"]
    assert usage is not None, chunks
    # input_tokens=18930 → prompt_tokens；output_tokens 累计到 7 → completion_tokens
    assert usage["prompt_tokens"] == 18930, usage
    assert usage["completion_tokens"] == 7, usage
    assert usage["total_tokens"] == 18937, usage


# ---------------------------------------------------------------------------
# 回归：convert_stream_to_anthropic 遇到 data:null 帧不能崩溃或中断内容输出。
# json.loads("null") 返回 None，原来代码直接 None.get("choices") 抛 AttributeError，
# 导致流中断、内容丢失、被误判为 zero completion 空响应。
# ---------------------------------------------------------------------------

def test_convert_stream_to_anthropic_tolerates_data_null_frames():
    """data: null 与真实内容帧混合时，转换不崩溃、内容正常输出、output_tokens > 0。"""
    import asyncio as _asyncio
    from message_utils import convert_stream_to_anthropic as _csa

    def _chunk(delta=None, finish_reason=None):
        return json.dumps({
            "id": "msg_test",
            "object": "chat.completion.chunk",
            "created": 1784701667,
            "model": "test-model",
            "choices": [{"delta": delta or {}, "finish_reason": finish_reason, "index": 0}],
            "usage": None,
        })

    async def gen():
        yield f"data: {_chunk({'content': 'Hello', 'role': 'assistant'})}\n\n"
        yield "data: null\n\n"
        yield f"data: {_chunk({'tool_calls': [{'index': 0, 'id': 'tc1', 'type': 'function', 'function': {'name': 'my_tool', 'arguments': ''}}]})}\n\n"
        yield f"data: {_chunk({'tool_calls': [{'index': 0, 'type': 'function', 'function': {'arguments': '{"key": "val"}'}}]})}\n\n"
        yield "data: null\n\n"
        yield f"data: {_chunk({}, finish_reason='tool_calls')}\n\n"
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in _csa(gen(), "test-model", request_messages=[{"role": "user", "content": "hi"}]):
            events.append(ev)

    _asyncio.run(collect())

    assert any("text_delta" in ev for ev in events), "文本内容应该输出到 Anthropic 事件"
    assert any("input_json_delta" in ev for ev in events), "tool_calls 应该转为 Anthropic input_json_delta"
    assert any('"type": "message_stop"' in ev for ev in events), "流应正常结束"

    message_delta_line = next(
        line for ev in events for line in ev.splitlines()
        if line.startswith("data:") and '"message_delta"' in line
    )
    delta_obj = json.loads(message_delta_line[5:].strip())
    assert delta_obj["usage"]["output_tokens"] > 0, "有内容时 output_tokens 应 > 0（估算兜底）"


def test_chunk_has_stream_content_on_user_reported_trace_frames():
    """用户 trace 的 content/tool_calls 帧应识别为有内容；data:null 和 [DONE] 不算内容。"""

    def _frame(delta=None, finish_reason=None):
        return f"data: {json.dumps({'id': 'x', 'object': 'chat.completion.chunk', 'choices': [{'delta': delta or {}, 'finish_reason': finish_reason, 'index': 0}], 'usage': None})}\n\n"

    assert _chunk_has_stream_content(_frame({"content": "This"}))
    assert _chunk_has_stream_content(_frame({
        "tool_calls": [{"index": 0, "id": "tc1", "type": "function", "function": {"name": "my_func", "arguments": ""}}]
    }))
    assert _chunk_has_stream_content(_frame({
        "tool_calls": [{"index": 0, "type": "function", "function": {"arguments": '{"key": "val"}'}}]
    }))
    # 空 delta + role 帧不算内容
    assert not _chunk_has_stream_content(_frame({"content": "", "role": "assistant"}))
    # data: null 不算内容
    assert not _chunk_has_stream_content("data: null\n\n")
    # [DONE] 不算内容
    assert not _chunk_has_stream_content("data: [DONE]\n\n")


# ---------------------------------------------------------------------------
# normalize_chat_protocols 三态：auto / true / false / 缺省
# ---------------------------------------------------------------------------

def test_normalize_upstream_stream_three_state():
    rows = normalize_chat_protocols([
        {"protocol": "openai", "path": "/a", "upstream_stream": "auto"},
        {"protocol": "openai", "path": "/b", "upstream_stream": False},
        {"protocol": "openai", "path": "/c", "upstream_stream": True},
        {"protocol": "openai", "path": "/d"},
    ])
    assert rows[0]["upstream_stream"] == "auto"
    assert rows[1]["upstream_stream"] is False
    assert rows[2]["upstream_stream"] is True
    # 缺省沿用既有默认 = 固定开
    assert rows[3]["upstream_stream"] is True


def test_normalize_upstream_stream_auto_case_insensitive():
    rows = normalize_chat_protocols([
        {"protocol": "openai", "path": "/a", "upstream_stream": "AUTO"},
    ])
    assert rows[0]["upstream_stream"] == "auto"


def test_upstream_stream_enum_constants():
    """三态枚举的存储形态锁定：JSON 原生布尔 + 字面量 "auto"。

    存量数据是 JSON 布尔，故 ON/OFF 必须保持 True/False —— 换成 int 会要求
    迁移所有 provider_configs.config 里的 chat_protocols 行。
    """
    from channel import (
        UPSTREAM_STREAM_ON,
        UPSTREAM_STREAM_OFF,
        UPSTREAM_STREAM_AUTO,
        UPSTREAM_STREAM_CHOICES,
    )

    assert UPSTREAM_STREAM_ON is True
    assert UPSTREAM_STREAM_OFF is False
    assert UPSTREAM_STREAM_AUTO == "auto"
    assert set(UPSTREAM_STREAM_CHOICES) == {True, False, "auto"}


def test_resolve_upstream_stream_only_auto_follows_client():
    """只有 auto 跟随客户端；固定开/关与缺省都无视客户端。"""
    from channel import resolve_upstream_stream

    # auto：完全跟随
    assert resolve_upstream_stream("auto", True) is True
    assert resolve_upstream_stream("auto", False) is False

    # 固定开/关：无视客户端
    assert resolve_upstream_stream(True, False) is True
    assert resolve_upstream_stream(False, True) is False

    # 缺省沿用固定开
    assert resolve_upstream_stream(None, False) is True

    # 非法字符串不得被当成 auto（否则会静默变成跟随客户端）
    assert resolve_upstream_stream("nonsense", False) is True


def test_normalize_upstream_stream_rejects_non_auto_strings():
    """只有 "auto" 是特殊值；其他字符串按普通真值 → 固定开。"""
    from channel import normalize_upstream_stream

    assert normalize_upstream_stream("auto") == "auto"
    assert normalize_upstream_stream("  Auto ") == "auto"
    assert normalize_upstream_stream("true") is True
    assert normalize_upstream_stream("stream") is True
    assert normalize_upstream_stream("") is True
