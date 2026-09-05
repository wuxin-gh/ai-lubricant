"""跨协议回帧误判空响应的回归测试。

## 历史 bug

出站协议由渠道配置/客户端模板决定，上游实际回什么协议不受我们控制。二者不一致时
（「选择的协议与模拟客户端的协议不一致」），解析器按出站协议的字段名去读，一个字
都取不到：

    出站 openai → 上游回 anthropic 帧
      parse_sse_data 只认 choices[].delta，content_block_delta 取不到内容
      openai_sse_completed 只认 [DONE]/finish_reason，不认 message_stop
      → has_output=False & completed=False
      → IncompleteStreamError("upstream stream ended before [DONE] or finish_reason")

而 usage 因为 _extract_usage_payload 读顶层 usage 反倒提取成功 —— 于是出现
「usage 有 352 output_tokens 却判定无输出」的自相矛盾状态。更严重的是内容整路
丢失，客户端即使不报错也收不到任何东西。

所以修复是「识别并正确解析」（providers/stream_protocol_sniff.py），不是「放宽
判定别抛异常」：测试同时断言不抛异常 **且** 内容/usage 正确落地。

覆盖四个出站协议 × 上游回其它协议的组合，以及同协议不受影响（防兜底反向误伤）。
"""

import asyncio
import json

import pytest
from fastapi import HTTPException

from providers.base import IncompleteStreamError
from providers.custom import CustomProvider
from providers.stream_protocol_sniff import (
    sniff_event_protocol,
    sniff_response_protocol,
    extract_protocol_error,
)


# ==================== 真实报文 ====================

# 用户实际遇到的那条流：Kiro 自我介绍。上游回 anthropic 帧，出站却是 openai。
# usage 明确 output_tokens=352，内容明确非空，却被判空响应。
REAL_KIRO_ANTHROPIC_EVENTS = [
    'event: message_start\n'
    'data: {"message":{"content":[],"id":"msg_5e2d1086-765b-455b-8d1f-0ca846f5697b",'
    '"model":"claude-opus-5","role":"assistant","stop_reason":null,"stop_sequence":null,'
    '"type":"message","usage":{"input_tokens":13,"output_tokens":0}},"type":"message_start"}\n\n',

    'event: content_block_start\n'
    'data: {"content_block":{"text":"","type":"text"},"index":0,"type":"content_block_start"}\n\n',

    'event: content_block_delta\n'
    'data: {"delta":{"text":"我是 Kiro，一个可以直接在你本地环境里干活的 AI 编程助手。",'
    '"type":"text_delta"},"index":0,"type":"content_block_delta"}\n\n',

    'event: content_block_delta\n'
    'data: {"delta":{"text":"你现在是通过 `kiro-cli chat` 命令在跟我对话。",'
    '"type":"text_delta"},"index":0,"type":"content_block_delta"}\n\n',

    'event: content_block_delta\n'
    'data: {"delta":{"text":"有什么具体任务，直接说就行。","type":"text_delta"},'
    '"index":0,"type":"content_block_delta"}\n\n',

    'event: content_block_stop\n'
    'data: {"index":0,"type":"content_block_stop"}\n\n',

    'event: message_delta\n'
    'data: {"delta":{"stop_reason":"end_turn"},"type":"message_delta",'
    '"usage":{"input_tokens":7297,"output_tokens":352}}\n\n',

    'event: message_stop\n'
    'data: {"type":"message_stop"}\n\n',
]

_EXPECTED_KIRO_TEXT = (
    "我是 Kiro，一个可以直接在你本地环境里干活的 AI 编程助手。"
    "你现在是通过 `kiro-cli chat` 命令在跟我对话。"
    "有什么具体任务，直接说就行。"
)


# ==================== provider 桩 ====================

def _make_provider(cls, protocol):
    return cls(
        "user", "key",
        provider_name="ch-test", base_url="https://example.test",
        protocol=protocol, upstream_stream=True,
    )


class _EventListProvider(CustomProvider):
    """按预置 event 列表回放上游 SSE。"""

    EVENTS: list = []

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        for event in self.EVENTS:
            yield event


class AnthropicFramesProvider(_EventListProvider):
    EVENTS = REAL_KIRO_ANTHROPIC_EVENTS


class OpenAIFramesProvider(_EventListProvider):
    EVENTS = [
        'data: {"object":"chat.completion.chunk","choices":[{"index":0,'
        '"delta":{"content":"你好"},"finish_reason":null}]}\n\n',
        'data: {"object":"chat.completion.chunk","choices":[{"index":0,'
        '"delta":{"content":"世界"},"finish_reason":null}]}\n\n',
        'data: {"object":"chat.completion.chunk","choices":[{"index":0,'
        '"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,'
        '"completion_tokens":7,"total_tokens":18}}\n\n',
        'data: [DONE]\n\n',
    ]


class ResponsesFramesProvider(_EventListProvider):
    EVENTS = [
        'event: response.output_text.delta\n'
        'data: {"type":"response.output_text.delta","delta":"你好"}\n\n',
        'event: response.output_text.delta\n'
        'data: {"type":"response.output_text.delta","delta":"世界"}\n\n',
        'event: response.completed\n'
        'data: {"type":"response.completed","response":{"usage":'
        '{"input_tokens":11,"output_tokens":7,"total_tokens":18}}}\n\n',
    ]


class GeminiFramesProvider(_EventListProvider):
    EVENTS = [
        'data: {"candidates":[{"content":{"parts":[{"text":"你好"}],"role":"model"}}]}\n\n',
        'data: {"candidates":[{"content":{"parts":[{"text":"世界"}],"role":"model"},'
        '"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,'
        '"candidatesTokenCount":7,"totalTokenCount":18}}\n\n',
    ]


# rate_limit_exceeded 流：真实上游在 ping 之后只发一个 response.failed，
# 错误体嵌在 response.error 内嵌，顶层无 error 键。
# 修复前这条会被当成正常空完成（responses/anthropic/gemini 路径）或误判成
# 空响应（openai 路径），真实原因「并发超限」一个字都不显示。
RATE_LIMIT_FAILED_EVENTS = [
    'data: {"type": "ping"}\n\n',
    'data: {"type": "ping"}\n\n',
    'data: {"type": "ping"}\n\n',
    'event: response.failed\n'
    'data: {"type":"response.failed","response":{"id":"resp_e7f3c604c338461e8718a1ee5a76c1b8",'
    '"object":"response","model":"claude-opus-5","status":"failed","output":[],'
    '"error":{"code":"rate_limit_exceeded",'
    '"message":"Concurrency limit exceeded for account, please retry later"}}}\n\n',
]


class RateLimitFailedProvider(_EventListProvider):
    EVENTS = RATE_LIMIT_FAILED_EVENTS


class ResponsesTopLevelErrorProvider(_EventListProvider):
    """顶层带 error 的 responses 帧（既有路径覆盖的形态，回归保护）。"""
    EVENTS = [
        'event: error\n'
        'data: {"type":"error","error":{"code":"rate_limit_exceeded",'
        '"message":"Concurrency limit exceeded for account, please retry later"}}\n\n',
    ]


class AnthropicToolUseProvider(_EventListProvider):
    """上游回 anthropic 工具调用帧：验证 tool index 重映射不串号。

    上游块索引在 text/tool_use 之间共享（text=0, tool_use=1,2），
    OpenAI tool_calls.index 只对工具计数（应为 0,1）。
    """
    EVENTS = [
        'event: message_start\n'
        'data: {"type":"message_start","message":{"content":[],"role":"assistant",'
        '"usage":{"input_tokens":50,"output_tokens":0}}}\n\n',
        'event: content_block_start\n'
        'data: {"type":"content_block_start","index":0,'
        '"content_block":{"type":"text","text":""}}\n\n',
        'event: content_block_delta\n'
        'data: {"type":"content_block_delta","index":0,'
        '"delta":{"type":"text_delta","text":"查一下"}}\n\n',
        'event: content_block_start\n'
        'data: {"type":"content_block_start","index":1,'
        '"content_block":{"type":"tool_use","id":"toolu_a","name":"read_file"}}\n\n',
        'event: content_block_delta\n'
        'data: {"type":"content_block_delta","index":1,'
        '"delta":{"type":"input_json_delta","partial_json":"{\\"p\\":1}"}}\n\n',
        'event: content_block_start\n'
        'data: {"type":"content_block_start","index":2,'
        '"content_block":{"type":"tool_use","id":"toolu_b","name":"grep"}}\n\n',
        'event: content_block_delta\n'
        'data: {"type":"content_block_delta","index":2,'
        '"delta":{"type":"input_json_delta","partial_json":"{\\"q\\":2}"}}\n\n',
        'event: message_delta\n'
        'data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},'
        '"usage":{"input_tokens":50,"output_tokens":25}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ]


class EmptyAnthropicFramesProvider(_EventListProvider):
    """上游回 anthropic 结构帧但**真的没有内容**：必须仍然判空，兜底不能吞掉真空流。"""
    EVENTS = [
        'event: message_start\n'
        'data: {"type":"message_start","message":{"content":[],"role":"assistant",'
        '"usage":{"input_tokens":13,"output_tokens":0}}}\n\n',
        'event: content_block_start\n'
        'data: {"type":"content_block_start","index":0,'
        '"content_block":{"type":"text","text":""}}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
        'event: message_delta\n'
        'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},'
        '"usage":{"input_tokens":13,"output_tokens":0}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ]


# ==================== 收集辅助 ====================

def _drain(provider, model="claude-opus-5"):
    """跑完整 provider.chat 流，返回 (chunks, 拼接内容, 最终 usage, tool_calls)。"""
    async def run():
        out = []
        async for chunk in provider.chat(model, [{"role": "user", "content": "hi"}], stream=True):
            out.append(chunk)
        return out

    chunks = asyncio.run(run())
    content = ""
    usage = None
    tool_calls: list = []
    for chunk in chunks:
        if not isinstance(chunk, str):
            continue
        for line in chunk.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            body = line[5:].strip()
            if body in ("[DONE]", ""):
                continue
            try:
                obj = json.loads(body)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            if isinstance(obj.get("usage"), dict):
                usage = obj["usage"]
            for choice in obj.get("choices") or []:
                delta = choice.get("delta") or {}
                if isinstance(delta, dict):
                    content += delta.get("content") or ""
                    for tc in delta.get("tool_calls") or []:
                        tool_calls.append(tc)
    return chunks, content, usage, tool_calls


# ==================== 嗅探单元测试 ====================

@pytest.mark.parametrize("event,expected", [
    (REAL_KIRO_ANTHROPIC_EVENTS[0], "anthropic"),
    (REAL_KIRO_ANTHROPIC_EVENTS[2], "anthropic"),
    (REAL_KIRO_ANTHROPIC_EVENTS[7], "anthropic"),
    (OpenAIFramesProvider.EVENTS[0], "openai"),
    (ResponsesFramesProvider.EVENTS[0], "responses"),
    (GeminiFramesProvider.EVENTS[0], "gemini"),
])
def test_sniff_identifies_protocol_from_frame(event, expected):
    assert sniff_event_protocol(event) == expected


def test_sniff_returns_none_for_unrecognizable_frame():
    """认不出必须返回 None，绝不猜 —— 猜错会把同协议流推进转换路径造成新的内容丢失。"""
    assert sniff_event_protocol('data: {"foo":"bar"}\n\n') is None
    assert sniff_event_protocol('data: [DONE]\n\n') is None
    assert sniff_event_protocol("") is None
    assert sniff_event_protocol(None) is None


# ==================== 核心回归：用户这条真实流 ====================

def test_real_kiro_anthropic_stream_over_openai_endpoint_not_empty():
    """用户实际报错的那条流：出站 openai、上游回 anthropic，不得判空且内容必须完整送达。"""
    provider = _make_provider(AnthropicFramesProvider, "openai")
    chunks, content, usage, _ = _drain(provider)

    # 修复前这里抛 IncompleteStreamError("upstream stream ended before [DONE] or finish_reason")
    assert not any("upstream stream ended before" in str(c) for c in chunks), chunks
    assert not any("empty output" in str(c) for c in chunks), chunks

    # 兜底的意义在于内容真的送到客户端，而不只是「不报错」
    assert content == _EXPECTED_KIRO_TEXT, content

    # usage 必须按 Anthropic→OpenAI 口径落地（total = 输入 + 输出）
    assert usage is not None, chunks
    assert usage["prompt_tokens"] == 7297, usage
    assert usage["completion_tokens"] == 352, usage
    assert usage["total_tokens"] == 7649, usage


def test_real_kiro_stream_has_no_contradictory_zero_output_state():
    """回归那个自相矛盾状态：usage 有 output_tokens 却判定无输出。"""
    provider = _make_provider(AnthropicFramesProvider, "openai")
    _, content, usage, _ = _drain(provider)
    assert usage["completion_tokens"] > 0
    assert content, "usage 报告了输出 token，内容不能为空"


# ==================== 四协议交叉矩阵 ====================

@pytest.mark.parametrize("outbound", ["openai", "anthropic", "responses", "gemini"])
def test_anthropic_upstream_frames_survive_any_outbound_protocol(outbound):
    provider = _make_provider(AnthropicFramesProvider, outbound)
    chunks, content, usage, _ = _drain(provider)
    assert content == _EXPECTED_KIRO_TEXT, (outbound, content, chunks)
    assert usage is not None and usage["completion_tokens"] == 352, (outbound, usage)


@pytest.mark.parametrize("outbound", ["anthropic", "responses", "gemini"])
def test_openai_upstream_frames_survive_cross_protocol_outbound(outbound):
    provider = _make_provider(OpenAIFramesProvider, outbound)
    chunks, content, usage, _ = _drain(provider)
    assert content == "你好世界", (outbound, content, chunks)
    assert usage is not None and usage["completion_tokens"] == 7, (outbound, usage)


@pytest.mark.parametrize("outbound", ["openai", "anthropic", "gemini"])
def test_responses_upstream_frames_survive_cross_protocol_outbound(outbound):
    provider = _make_provider(ResponsesFramesProvider, outbound)
    chunks, content, usage, _ = _drain(provider)
    assert content == "你好世界", (outbound, content, chunks)
    assert usage is not None and usage["completion_tokens"] == 7, (outbound, usage)


@pytest.mark.parametrize("outbound", ["openai", "anthropic", "responses"])
def test_gemini_upstream_frames_survive_cross_protocol_outbound(outbound):
    provider = _make_provider(GeminiFramesProvider, outbound)
    chunks, content, usage, _ = _drain(provider)
    assert content == "你好世界", (outbound, content, chunks)
    assert usage is not None and usage["completion_tokens"] == 7, (outbound, usage)


# ==================== 工具调用 index 重映射 ====================

def test_cross_protocol_tool_index_remapped_not_shared_with_text_blocks():
    """上游块索引 text=0/tool=1,2 → OpenAI tool_calls.index 必须是 0,1。

    不重映射会让工具参数拼到错误的调用上（index 1 的 arguments 落到不存在的 tool 1）。
    """
    provider = _make_provider(AnthropicToolUseProvider, "openai")
    chunks, content, usage, tool_calls = _drain(provider)

    assert content == "查一下", chunks
    assert tool_calls, chunks
    starts = [tc for tc in tool_calls if (tc.get("function") or {}).get("name")]
    assert [tc["index"] for tc in starts] == [0, 1], starts
    assert [tc["function"]["name"] for tc in starts] == ["read_file", "grep"], starts
    assert usage is not None and usage["completion_tokens"] == 25, usage


# ==================== response.failed 错误必须显式报出 ====================

@pytest.mark.parametrize("outbound", ["responses", "openai", "anthropic", "gemini"])
def test_rate_limit_response_failed_surfaces_real_error(outbound):
    """上游回 responses response.failed(error 嵌在 response.error)。

    修复前：responses/anthropic/gemini 路径当成正常空完成吞掉；openai 路径误判
    空响应。四条路径都未暴露「并发超限」。修复后必须抛 HTTPException，429 +
    原始 message，让上层换账号重试/冻结正确触发。
    """
    provider = _make_provider(RateLimitFailedProvider, outbound)
    with pytest.raises(HTTPException) as exc_info:
        async def run():
            async for _ in provider.chat(
                "claude-opus-5", [{"role": "user", "content": "hi"}], stream=True
            ):
                pass
        asyncio.run(run())

    assert exc_info.value.status_code == 429, (outbound, exc_info.value.status_code)
    detail = str(exc_info.value.detail)
    assert "Concurrency limit exceeded" in detail, (outbound, detail)
    assert "rate_limit_exceeded" in detail or "Concurrency limit" in detail, (outbound, detail)


def test_responses_top_level_error_still_raises():
    """顶层带 error 的 responses 帧（既有路径覆盖形态）：回归保护，不能因拆分
    response.failed 分支而漏掉顶层 error 帧。"""
    provider = _make_provider(ResponsesTopLevelErrorProvider, "responses")
    with pytest.raises(HTTPException) as exc_info:
        async def run():
            async for _ in provider.chat(
                "claude-opus-5", [{"role": "user", "content": "hi"}], stream=True
            ):
                pass
        asyncio.run(run())

    assert exc_info.value.status_code == 429, exc_info.value.status_code
    assert "Concurrency limit exceeded" in str(exc_info.value.detail)


def test_extract_protocol_error_finds_nested_response_error():
    """extract_protocol_error 必须穿透 response.error 内嵌查到错误。"""
    payload = {
        "type": "response.failed",
        "response": {
            "status": "failed",
            "error": {"code": "rate_limit_exceeded", "message": "limit hit"},
        },
    }
    err = extract_protocol_error(payload)
    assert err == {"code": "rate_limit_exceeded", "message": "limit hit"}


def test_extract_protocol_error_returns_none_for_normal_frame():
    assert extract_protocol_error({"type": "ping"}) is None
    assert extract_protocol_error({"type": "response.output_text.delta", "delta": "x"}) is None
    assert extract_protocol_error({}) is None


# ==================== 反向保护：真空流仍要判空 ====================

def test_truly_empty_anthropic_stream_still_raises():
    """兜底只负责「认出协议正确解析」，不能把真正的空流吞成成功。"""
    provider = _make_provider(EmptyAnthropicFramesProvider, "openai")

    with pytest.raises(IncompleteStreamError):
        async def run():
            async for _ in provider.chat(
                "claude-opus-5", [{"role": "user", "content": "hi"}], stream=True
            ):
                pass
        asyncio.run(run())


def test_same_protocol_stream_unaffected_by_fallback():
    """同协议路径不得进入兜底（零影响回归）。"""
    provider = _make_provider(OpenAIFramesProvider, "openai")
    chunks, content, usage, _ = _drain(provider)
    assert content == "你好世界", chunks
    assert usage["completion_tokens"] == 7, usage


# ==================== 非流式 ====================

def test_sniff_response_protocol_matrix():
    assert sniff_response_protocol({
        "choices": [{"message": {"content": "hi"}}]
    }) == "openai"
    assert sniff_response_protocol({
        "type": "message", "role": "assistant",
        "content": [{"type": "text", "text": "hi"}],
        "usage": {"input_tokens": 3, "output_tokens": 2},
    }) == "anthropic"
    assert sniff_response_protocol({"object": "response", "output": [], "status": "completed"}) == "responses"
    assert sniff_response_protocol({
        "candidates": [{"content": {"parts": [{"text": "hi"}]}}]
    }) == "gemini"
    assert sniff_response_protocol({"foo": "bar"}) is None


def test_non_stream_anthropic_body_normalized_not_lost():
    """出站 openai、上游回 anthropic JSON：内容与 usage 必须归一到 OpenAI 形态。"""
    from providers.stream_protocol_sniff import normalize_response_to_openai

    body = {
        "type": "message",
        "role": "assistant",
        "id": "msg_x",
        "content": [{"type": "text", "text": "我是 Kiro"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 7297, "output_tokens": 352},
    }
    converted = normalize_response_to_openai(body, "anthropic", "claude-opus-5")

    assert converted is not None
    assert converted["choices"][0]["message"]["content"] == "我是 Kiro"
    assert converted["choices"][0]["finish_reason"] == "stop"
    assert converted["usage"]["prompt_tokens"] == 7297
    assert converted["usage"]["completion_tokens"] == 352
    assert converted["usage"]["total_tokens"] == 7649
