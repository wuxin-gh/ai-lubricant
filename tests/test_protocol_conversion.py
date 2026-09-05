import asyncio

import pytest
from fastapi import HTTPException

from message_utils import (
    anthropic_to_openai_messages,
    combine_messages,
    iter_sse_payloads,
    normalize_responses_input,
    openai_messages_to_responses_payload,
    parse_sse_data_value,
    responses_to_openai_messages,
    sanitize_anthropic_request_body,
    sanitize_anthropic_tool_id,
    sanitize_anthropic_tool_ids_in_body,
    sanitize_anthropic_tool_name,
    coerce_anthropic_system,
)
from providers.base import BaseProvider, IncompleteStreamError
from providers.custom import CustomProvider


def _provider(protocol="anthropic"):
    return CustomProvider(
        "user",
        "password",
        base_url="https://example.com",
        protocol=protocol,
        api_key="test-key",
    )


def test_openai_text_parts_convert_to_anthropic_text_blocks():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "stream": False,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "text", "text": "world"},
            ],
        }],
    })

    assert payload["messages"] == [{
        "role": "user",
        "content": [
            {"type": "text", "text": "hello"},
            {"type": "text", "text": "world"},
        ],
    }]


def test_openai_image_url_converts_to_anthropic_url_source():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "describe"},
                {"type": "image_url", "image_url": {"url": "https://example.com/a.png", "detail": "high"}},
            ],
        }],
    })

    content = payload["messages"][0]["content"]
    assert content == [
        {"type": "text", "text": "describe"},
        {"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}},
    ]
    assert "image_url" not in str(payload)


def test_openai_data_url_converts_to_anthropic_base64_source():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "messages": [{
            "role": "user",
            "content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}],
        }],
    })

    assert payload["messages"][0]["content"] == [{
        "type": "image",
        "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"},
    }]


def test_openai_assistant_reasoning_and_list_content_convert_to_blocks():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "messages": [{
            "role": "assistant",
            "reasoning_content": "thinking",
            "content": [{"type": "text", "text": "answer"}],
        }],
    })

    assert payload["messages"][0]["content"] == [
        {"type": "thinking", "thinking": "thinking", "signature": ""},
        {"type": "text", "text": "answer"},
    ]


def test_openai_tool_calls_convert_to_anthropic_tool_use_blocks():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "messages": [{
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{\"q\": \"abc\"}"},
            }],
        }],
    })

    assert payload["messages"][0]["content"] == [{
        "type": "tool_use",
        "id": "call_1",
        "name": "lookup",
        "input": {"q": "abc"},
    }]


def test_openai_tool_message_converts_to_anthropic_tool_result():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "messages": [{"role": "tool", "tool_call_id": "call_1", "content": "done"}],
    })

    assert payload["messages"] == [{
        "role": "user",
        "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "done"}],
    }]


def test_anthropic_style_image_block_in_openai_request_returns_400():
    try:
        _provider()._openai_request_to_anthropic({
            "model": "claude-test",
            "messages": [{
                "role": "user",
                "content": [{"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}}],
            }],
        })
    except HTTPException as exc:
        assert exc.status_code == 400
        assert exc.detail["error"]["code"] == "invalid_message_content"
        assert "Unsupported OpenAI content part type" in exc.detail["error"]["message"]
    else:
        raise AssertionError("expected HTTPException")


def test_openai_custom_payload_preserves_content_parts():
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": "describe"},
            {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
        ],
    }]

    payload = _provider("openai")._build_openai_payload("gpt-test", messages, False)

    assert payload["messages"] is messages
    assert payload["messages"][0]["content"][1]["type"] == "image_url"


def test_raw_anthropic_body_passthrough_preserves_messages_and_overrides_model_stream():
    raw = {
        "model": "client-model",
        "stream": True,
        "system": [{"type": "text", "text": "sys", "cache_control": {"type": "ephemeral"}}],
        "messages": [{
            "role": "user",
            "content": [{"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}}],
        }],
        "tools": [{"name": "lookup", "input_schema": {"type": "object"}}],
    }

    payload = _provider("anthropic")._build_anthropic_payload(
        "routed-model",
        [{"role": "user", "content": "ignored"}],
        False,
        _raw_anthropic_body=raw,
    )

    assert payload["model"] == "routed-model"
    assert payload["stream"] is False
    assert payload["messages"] is raw["messages"]
    assert "You are Claude Code" in payload["system"]
    assert str(raw["system"]) in payload["system"]
    assert payload["tools"] is raw["tools"]


def test_openai_to_responses_payload_preserves_text_and_image_parts_without_metadata():
    payload = openai_messages_to_responses_payload(
        "resp-test",
        [{
            "role": "user",
            "content": [
                {"type": "text", "text": "describe"},
                {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
            ],
        }],
        False,
        {"metadata": {"trace_id": "abc"}, "max_tokens": 8},
    )

    assert payload["input"] == [{
        "role": "user",
        "content": [
            {"type": "input_text", "text": "describe"},
            {"type": "input_image", "image_url": "https://example.com/a.png"},
        ],
    }]
    assert payload["metadata"] == {"trace_id": "abc"}


def test_text_fallback_does_not_emit_python_list_repr():
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": "hello"},
            {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
        ],
    }]

    combined = combine_messages(messages)
    latest = _provider("openai").get_latest_user_message(messages)

    assert "[{'type'" not in combined
    assert "[{'type'" not in latest
    assert "hello" in combined
    assert "[image_url: https://example.com/a.png]" in combined
    assert latest == "hello\n[image_url: https://example.com/a.png]"


def test_iter_sse_payloads_mixed_done_events():
    json_then_done = (
        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
        "data: done\n\n"
    )
    results = list(iter_sse_payloads(json_then_done))
    assert results == [
        {"choices": [{"delta": {"content": "hi"}}]},
        "[DONE]",
    ]

    assert list(iter_sse_payloads("data: done\n\ndata: [DONE]\n\n")) == [
        "[DONE]",
        "[DONE]",
    ]

    assert list(iter_sse_payloads("data: done\n\n")) == ["[DONE]"]

    assert list(iter_sse_payloads("data: [DONE]\n\n")) == ["[DONE]"]

    assert list(iter_sse_payloads("data: not-json-content\n\n")) == []


def test_parse_sse_data_value_done_markers():
    assert parse_sse_data_value("done") == "[DONE]"
    assert parse_sse_data_value("DONE") == "[DONE]"
    assert parse_sse_data_value("Done") == "[DONE]"
    assert parse_sse_data_value("[DONE]") == "[DONE]"
    assert parse_sse_data_value("") is None
    assert parse_sse_data_value('{"key": "value"}') == {"key": "value"}


def test_openai_reasoning_xhigh_maps_to_anthropic_output_config_max():
    payload = _provider()._openai_request_to_anthropic({
        "model": "claude-test",
        "messages": [{"role": "user", "content": "hi"}],
        "reasoning_effort": "xhigh",
    })

    assert payload["thinking"] == {"type": "enabled", "budget_tokens": 1024}
    assert payload["output_config"] == {"effort": "max"}


def test_openai_reasoning_effort_maps_to_responses_reasoning():
    payload = openai_messages_to_responses_payload(
        "resp-test",
        [{"role": "user", "content": "hi"}],
        False,
        {"reasoning_effort": "xhigh"},
    )

    assert payload["reasoning"] == {"effort": "xhigh"}


def test_openai_reasoning_disabled_omits_responses_reasoning():
    payload = openai_messages_to_responses_payload(
        "resp-test",
        [{"role": "user", "content": "hi"}],
        False,
        {"reasoning_effort": "none"},
    )

    assert "reasoning" not in payload


# ===================== BaseProvider 不完整流检测 (Task 4) =====================


class _FakeStreamProvider(BaseProvider):
    """最小可实例化的 BaseProvider 子类，_do_stream_chat 直接 yield 预置 chunk。"""

    PROVIDER_NAME = "fake"

    def __init__(self, chunks):
        super().__init__("user", "password")
        self._chunks = chunks

    async def init_auth(self, is_check: bool = False) -> bool:  # pragma: no cover - 未使用
        return True

    async def check_auth(self) -> bool:  # pragma: no cover - 未使用
        return True

    async def fetch_upstream_model_list(self):  # pragma: no cover - 未使用
        return []

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        # 先 yield 空 dict 表示连接成功（触发 role chunk），再 yield 预置 chunk。
        yield {}
        for chunk in self._chunks:
            yield chunk

    async def _do_non_stream_chat(self, model_id, messages, **kwargs):  # pragma: no cover
        return {}


async def _drain_chat(provider, **kwargs):
    out = []
    async for piece in provider.chat("fake-model", [{"role": "user", "content": "hi"}], stream=True, **kwargs):
        out.append(piece)
    return out


def test_base_chat_raises_on_done_only_stream():
    """场景1：上游只发 data: [DONE]，无任何内容 -> 抛 IncompleteStreamError。"""
    provider = _FakeStreamProvider(["data: [DONE]\n\n"])
    with pytest.raises(IncompleteStreamError):
        asyncio.run(_drain_chat(provider))


def test_base_chat_raises_on_bare_done_only_stream():
    """场景2：上游只发 data: done（裸 done）-> 抛 IncompleteStreamError。"""
    provider = _FakeStreamProvider(["data: done\n\n"])
    with pytest.raises(IncompleteStreamError):
        asyncio.run(_drain_chat(provider))


def test_base_chat_normal_content_stream_completes():
    """场景3：上游有正常内容 + [DONE] -> 不抛异常，内容完整送达客户端。"""
    provider = _FakeStreamProvider([
        'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',
        'data: {"choices":[{"delta":{"content":" world"}}]}\n\n',
        "data: [DONE]\n\n",
    ])
    out = asyncio.run(_drain_chat(provider))
    joined = "".join(p for p in out if isinstance(p, str))
    assert "Hello" in joined
    assert "world" in joined
    assert "data: [DONE]" in joined


def test_base_chat_respects_disabled_flag():
    """场景4：stream_incomplete_error_enabled=False + 空流 -> 不抛异常。"""
    provider = _FakeStreamProvider(["data: [DONE]\n\n"])
    out = asyncio.run(_drain_chat(provider, stream_incomplete_error_enabled=False))
    assert any(isinstance(p, str) and "data: [DONE]" in p for p in out)


def test_base_chat_reasoning_only_stream_completes():
    """场景5：上游仅返回 reasoning_content（思考）+ [DONE] -> 视为真实输出，不抛异常。

    base.py 中 has_real_output 的判定包含 delta.reasoning_content（line 394），
    因此 thinking-only 流应被视为有真实生成，不能判定为不完整流。
    """
    provider = _FakeStreamProvider([
        'data: {"choices":[{"delta":{"reasoning_content":"thinking..."}}]}\n\n',
        "data: [DONE]\n\n",
    ])
    out = asyncio.run(_drain_chat(provider))
    assert any(isinstance(p, str) and "data: [DONE]" in p for p in out)


def test_base_chat_usage_tokens_only_stream_completes():
    """场景6：上游无 content/tool_calls，但 usage.completion_tokens>0 + [DONE] -> 不抛异常。

    usage 中存在 completion tokens 证明上游确实产生了生成，
    _completion_tokens(full_usage) > 0 使不完整流条件不成立。
    """
    provider = _FakeStreamProvider([
        'data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":7}}\n\n',
        "data: [DONE]\n\n",
    ])
    out = asyncio.run(_drain_chat(provider))
    usage_payloads = []
    for piece in out:
        if not isinstance(piece, str):
            continue
        for payload in iter_sse_payloads(piece):
            if isinstance(payload, dict) and payload.get("usage"):
                usage_payloads.append(payload["usage"])
    assert len(usage_payloads) == 1
    assert usage_payloads[0]["prompt_tokens"] == 5
    assert usage_payloads[0]["completion_tokens"] == 7
    assert usage_payloads[0]["total_tokens"] == 12
    assert any(isinstance(p, str) and "data: [DONE]" in p for p in out)


def test_base_chat_estimates_usage_when_upstream_omits_usage():
    provider = _FakeStreamProvider([
        'data: {"choices":[{"delta":{"reasoning_content":"thinking..."}}]}\n\n',
        'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{}"}}]}}]}\n\n',
        'data: [DONE]\n\n',
    ])
    out = asyncio.run(_drain_chat(provider))
    usage_payloads = []
    for piece in out:
        if not isinstance(piece, str):
            continue
        for payload in iter_sse_payloads(piece):
            if isinstance(payload, dict) and payload.get("usage"):
                usage_payloads.append(payload["usage"])
    assert usage_payloads, out
    usage = usage_payloads[-1]
    assert usage["prompt_tokens"] > 0
    assert usage["completion_tokens"] > 0
    assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"]



def test_base_chat_tool_calls_only_stream_completes():
    """场景7：上游仅返回 tool_calls + [DONE] -> 视为真实输出，不抛异常。

    has_real_output 的判定包含 delta.tool_calls（line 395），tool-call-only 是合法生成。
    """
    provider = _FakeStreamProvider([
        'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1",'
        '"type":"function","function":{"name":"get_weather","arguments":"{}"}}]}}]}\n\n',
        "data: [DONE]\n\n",
    ])
    out = asyncio.run(_drain_chat(provider))
    assert any(isinstance(p, str) and "data: [DONE]" in p for p in out)


def test_base_chat_raises_on_finish_reason_without_done():
    """场景8：上游只发 finish_reason=stop（无 content、无 [DONE]）-> 抛 IncompleteStreamError。

    terminator_only 同时覆盖 seen_finish_output / upstream_finish_reason，
    即使没有 [DONE]，仅终止信号无真实输出也应判定为不完整流。
    """
    provider = _FakeStreamProvider([
        'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n',
    ])
    with pytest.raises(IncompleteStreamError):
        asyncio.run(_drain_chat(provider))


# ============ role_chunk 推迟到首个真实内容 (Task 8) ============


def _is_role_chunk(piece):
    """判断某个 yield 出来的 chunk 是否为 role-only 启动块（delta.role==assistant 且无 content）。"""
    if not isinstance(piece, str):
        return False
    for payload in iter_sse_payloads(piece):
        if not isinstance(payload, dict):
            continue
        for choice in payload.get("choices", []) or []:
            delta = choice.get("delta") or {}
            if isinstance(delta, dict) and delta.get("role") == "assistant" and not delta.get("content"):
                return True
    return False


class _ConnThenChunksProvider(_FakeStreamProvider):
    """模拟 qwen.py 模式：先 yield {} 连接标记，再 yield 预置 chunk。"""

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield {}
        for chunk in self._chunks:
            yield chunk


class _NoConnProvider(_FakeStreamProvider):
    """模拟 custom.py 模式：不发连接 {}，直到有真实内容才 yield。"""

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        for chunk in self._chunks:
            yield chunk


class _QwenEmptyStreamProvider(_FakeStreamProvider):
    """模拟 yield {} on connection provider 的空流：先 yield {} 连接标记，再 yield 终止信号。"""

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield {}
        yield "data: [DONE]\n\n"


def test_task8_role_chunk_deferred_to_first_real_content():
    """场景1：provider 先 yield {} 再发正常内容 + [DONE]。

    断言 chat() 消费出的 SSE 块以 role-only 启动块开头（delta.role==assistant 且无 content），
    随后才是 content 块。即 SSE 顺序保持不变：role 块仍在首个内容块之前。
    """
    provider = _ConnThenChunksProvider([
        'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',
        'data: {"choices":[{"delta":{"content":" world"}}]}\n\n',
        "data: [DONE]\n\n",
    ])
    out = asyncio.run(_drain_chat(provider))

    str_chunks = [p for p in out if isinstance(p, str)]
    assert str_chunks, "expected at least one SSE chunk"
    # 首个 SSE 块必须是 role-only 启动块
    assert _is_role_chunk(str_chunks[0]), f"first chunk is not a role chunk: {str_chunks[0]!r}"
    # role 块只出现一次
    assert sum(1 for c in str_chunks if _is_role_chunk(c)) == 1
    # role 块之后才是 content
    first_content_idx = next(i for i, c in enumerate(str_chunks) if '"content":"Hello"' in c)
    role_idx = next(i for i, c in enumerate(str_chunks) if _is_role_chunk(c))
    assert role_idx < first_content_idx, "role chunk must precede first content chunk"
    joined = "".join(str_chunks)
    assert "Hello" in joined and "world" in joined and "data: [DONE]" in joined


def test_task8_no_role_chunk_leaks_before_empty_stream_raise():
    """场景2：provider 先 yield {} 连接标记，随后只有终止信号、无内容。

    断言 chat(stream=True) 抛 IncompleteStreamError，且在 raise 之前没有任何 role_chunk 被 yield，
    且根本没有任何 SSE 字节泄漏到客户端（IncompleteStreamError 在 priming 阶段透明重试）。
    """
    provider = _QwenEmptyStreamProvider([])

    collected = []

    async def _collect():
        async for piece in provider.chat(
            "fake-model", [{"role": "user", "content": "hi"}], stream=True
        ):
            collected.append(piece)

    with pytest.raises(IncompleteStreamError):
        asyncio.run(_collect())

    assert not any(_is_role_chunk(p) for p in collected), (
        f"role chunk leaked before empty-stream raise: {collected!r}"
    )
    # 真实 qwen 空流不应有任何 SSE 字符串块泄漏到客户端
    assert not any(isinstance(p, str) for p in collected), (
        f"no client-visible SSE bytes should leak before empty-stream raise: {collected!r}"
    )


def test_task8_done_string_empty_stream_no_role_leak():
    """场景2b：provider 先 yield {}，再 yield 裸 [DONE] 字符串（无内容）。

    这覆盖会透传 [DONE] 字符串的渠道。断言 chat(stream=True) 抛 IncompleteStreamError，
    且在 raise 之前没有任何 role_chunk 被 yield（plan QA 场景2 的核心约束：
    重试后不会出现重复 role）。[DONE] 终止符按历史行为透传，不构成 role 重复。
    """
    provider = _ConnThenChunksProvider(["data: [DONE]\n\n"])

    collected = []

    async def _collect():
        async for piece in provider.chat(
            "fake-model", [{"role": "user", "content": "hi"}], stream=True
        ):
            collected.append(piece)

    with pytest.raises(IncompleteStreamError):
        asyncio.run(_collect())

    assert not any(_is_role_chunk(p) for p in collected), (
        f"role chunk leaked before empty-stream raise: {collected!r}"
    )


def test_task8_no_conn_marker_role_chunk_still_fires_on_first_content():
    """场景3：custom.py 模式（不发连接 {}，直到内容才 yield）。

    断言 role_chunk 仍在首个真实内容块上触发（行为不变）。
    """
    provider = _NoConnProvider([
        'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n',
        "data: [DONE]\n\n",
    ])
    out = asyncio.run(_drain_chat(provider))

    str_chunks = [p for p in out if isinstance(p, str)]
    assert str_chunks
    assert _is_role_chunk(str_chunks[0]), f"first chunk is not a role chunk: {str_chunks[0]!r}"
    assert sum(1 for c in str_chunks if _is_role_chunk(c)) == 1
    joined = "".join(str_chunks)
    assert "Hi" in joined and "data: [DONE]" in joined


def test_task8_no_conn_marker_empty_stream_still_raises():
    """场景3b：不发连接 {} 且空流（仅 [DONE]）-> 仍抛 IncompleteStreamError，无 role_chunk 泄漏。"""
    provider = _NoConnProvider(["data: [DONE]\n\n"])

    collected = []

    async def _collect():
        async for piece in provider.chat(
            "fake-model", [{"role": "user", "content": "hi"}], stream=True
        ):
            collected.append(piece)

    with pytest.raises(IncompleteStreamError):
        asyncio.run(_collect())

    assert not any(_is_role_chunk(p) for p in collected)


# ==================== Anthropic tool_use.id sanitizer ====================


def test_sanitize_anthropic_tool_id_replaces_illegal_chars():
    """函数式测试：含非法字符的 ID 被正确替换为 '_'。"""
    assert sanitize_anthropic_tool_id("functions.todowrite:3") == "functions_todowrite_3"


def test_sanitize_anthropic_tool_id_valid_unchanged():
    """函数式测试：已合规的 ID 原样返回。"""
    assert sanitize_anthropic_tool_id("call_1") == "call_1"


def test_sanitize_anthropic_tool_id_empty_fallback():
    """函数式测试：空 ID / None 兜底生成非空合规 ID。"""
    result = sanitize_anthropic_tool_id("")
    assert result != ""
    import re
    assert re.match(r"^[a-zA-Z0-9_-]+$", result), f"id={result!r} does not match Anthropic pattern"
    result_none = sanitize_anthropic_tool_id(None)
    assert result_none != ""
    assert re.match(r"^[a-zA-Z0-9_-]+$", result_none)


def test_sanitize_anthropic_tool_ids_in_body_preserves_pairing():
    """集成测试：body 中 tool_use.id 与 tool_result.tool_use_id 经清洗后仍然匹配。"""
    raw_body = {
        "model": "claude-opus-4-8",
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me check"},
                    {"type": "tool_use", "id": "functions.todowrite:3", "name": "todowrite",
                     "input": {"content": "test"}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "functions.todowrite:3", "content": "done"},
                ],
            },
        ],
    }

    sanitized = sanitize_anthropic_tool_ids_in_body(raw_body)

    assert sanitized["messages"][0]["content"][1]["id"] == "functions_todowrite_3"
    assert sanitized["messages"][1]["content"][0]["tool_use_id"] == "functions_todowrite_3"
    assert sanitized["messages"] is not raw_body["messages"]
    # 原始 body 未被突变
    assert raw_body["messages"][0]["content"][1]["id"] == "functions.todowrite:3"


def test_anthropic_to_openai_messages_sanitizes_illegal_tool_ids():
    """跨协议转换：Anthropic tool_use.id 含非法字符（冒号）时，转成 OpenAI
    tool_calls[].id / tool_call_id 前被清洗，且 tool_use 与 tool_result 配对不断裂。

    复现线上报错：客户端 /v1/messages 传入 id="WebSearch:0"（含 ':'），
    经 openai 协议渠道出口透传给上游 Anthropic 兼容网关时被拒。
    """
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "let me search"},
                    {"type": "tool_use", "id": "WebSearch:0", "name": "WebSearch",
                     "input": {"query": "hello"}},
                    {"type": "tool_use", "id": "mcp__codegraph__codegraph_explore:1",
                     "name": "mcp__codegraph__codegraph_explore", "input": {"query": "x"}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "WebSearch:0", "content": "r0"},
                    {"type": "tool_result",
                     "tool_use_id": "mcp__codegraph__codegraph_explore:1", "content": "r1"},
                ],
            },
        ],
    }

    messages, _kwargs = anthropic_to_openai_messages(body)

    import re
    pattern = re.compile(r"^[a-zA-Z0-9_-]+$")

    assistant_msg = next(m for m in messages if m.get("role") == "assistant")
    tool_call_ids = [tc["id"] for tc in assistant_msg["tool_calls"]]
    assert tool_call_ids == ["WebSearch_0", "mcp__codegraph__codegraph_explore_1"]
    for tid in tool_call_ids:
        assert pattern.match(tid), f"tool_call id {tid!r} 仍含非法字符"

    tool_msgs = [m for m in messages if m.get("role") == "tool"]
    tool_call_id_refs = [m["tool_call_id"] for m in tool_msgs]
    # 配对不断裂：tool 消息引用的 id 与 assistant tool_calls 的 id 一致
    assert tool_call_id_refs == tool_call_ids
    for tid in tool_call_id_refs:
        assert pattern.match(tid), f"tool_call_id {tid!r} 仍含非法字符"

    # tool 消息回带原始工具名，且名字绝不等于（清洗后的）id
    tool_names = [m.get("name") for m in tool_msgs]
    assert tool_names == ["WebSearch", "mcp__codegraph__codegraph_explore"]
    for name, tid in zip(tool_names, tool_call_id_refs):
        assert name != tid, f"工具名 {name!r} 不应等于 tool_call_id"


def test_anthropic_to_openai_messages_tool_result_carries_tool_name():
    """Anthropic tool_result 不带 name，转成 OpenAI role=tool 时应从前面的
    tool_use 回带原始工具名，避免下游 provider 用 tool_call_id 当函数名。

    复现线上报错：`工具 call_a6ce550dae0949f48df56953 返回：...`
    """
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_a", "name": "search",
                     "input": {"q": "hi"}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "call_a", "content": "ok"},
                ],
            },
        ],
    }

    messages, _ = anthropic_to_openai_messages(body)
    tool_msg = next(m for m in messages if m.get("role") == "tool")
    assert tool_msg["name"] == "search"
    assert tool_msg["tool_call_id"] == "call_a"
    assert tool_msg["name"] != tool_msg["tool_call_id"]


def test_anthropic_to_openai_messages_empty_tool_use_name_falls_back_to_tool():
    """tool_use 缺 name 时，OpenAI function.name 兜底为 "tool"，不留空串。"""
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_a", "input": {"q": "hi"}},
                ],
            },
        ],
    }

    messages, _ = anthropic_to_openai_messages(body)
    assistant_msg = next(m for m in messages if m.get("role") == "assistant")
    assert assistant_msg["tool_calls"][0]["function"]["name"] == "tool"


def test_anthropic_to_openai_messages_orphan_tool_result_no_id_as_name():
    """无匹配 tool_use 的 orphan tool_result：不发明名字，也绝不把 id 当 name。"""
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "call_orphan", "content": "ok"},
                ],
            },
        ],
    }

    messages, _ = anthropic_to_openai_messages(body)
    tool_msg = next(m for m in messages if m.get("role") == "tool")
    assert tool_msg.get("name") != tool_msg["tool_call_id"]
    assert "name" not in tool_msg


def test_responses_to_openai_messages_tool_result_carries_tool_name():
    """Responses function_call_output 不带 name，转成 OpenAI role=tool 时应从
    前面的 function_call 回带原始工具名。"""
    messages, _ = responses_to_openai_messages({
        "input": [
            {"type": "function_call", "call_id": "c1", "name": "lookup", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "c1", "output": "ok"},
        ]
    })

    tool_msg = next(m for m in messages if m.get("role") == "tool")
    assert tool_msg["name"] == "lookup"
    assert tool_msg["tool_call_id"] == "c1"
    assert tool_msg["name"] != tool_msg["tool_call_id"]


# ==================== tool name sanitization (兼容缺口) ====================
# 复现线上报错：
#   ① Responses 渠道："Invalid 'input[5].name': string too long. Expected a
#      string with maximum length 128, but got a string with length 24505 instead."
#   ② Anthropic 原生壳/OpenAI 渠道："Invalid tool use format."
# 根因：Anthropic→OpenAI 转换链路对 tool_use.name 原样透传，超长 / 含非法字符
# 的 name 穿到上游被拒。出站前统一 sanitize_anthropic_tool_name 归一化。


def test_sanitize_anthropic_tool_name_truncates_and_cleans():
    """name 满足 ^[a-zA-Z0-9_-]{1,64}$:超长截断、非法字符→_、空→tool。"""
    assert sanitize_anthropic_tool_name("get_weather") == "get_weather"
    assert sanitize_anthropic_tool_name("") == "tool"
    assert sanitize_anthropic_tool_name(None) == "tool"
    assert sanitize_anthropic_tool_name("   ") == "tool"
    # 超长全合法字符:截到 64
    assert sanitize_anthropic_tool_name("a" * 300) == "a" * 64
    assert len(sanitize_anthropic_tool_name("a" * 65)) == 64
    assert len(sanitize_anthropic_tool_name("a" * 64)) == 64
    # 非法字符(冒号/点)→_
    assert sanitize_anthropic_tool_name("foo.bar:baz") == "foo_bar_baz"
    # 超长 + 非法:先清洗再截断
    big = "a.b" * 100  # 300 字符
    out = sanitize_anthropic_tool_name(big)
    assert len(out) == 64 and out == ("a_b" * 22)[:64]


def test_anthropic_to_openai_messages_sanitizes_oversized_tool_name():
    """客户端历史里带超长/非法 tool_use.name 时,转 OpenAI 时归一化,不再原样穿到上游。

    复现 ① / ②:24505 字符的 name 原样透传会被 Anthropic 原生壳 / Responses 上游拒。
    """
    huge_name = "x" * 24505  # 模拟畸形 name
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_a", "name": huge_name, "input": {"q": "hi"}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "call_a", "content": "ok"},
                ],
            },
        ],
    }
    messages, kwargs = anthropic_to_openai_messages(body)

    assistant_msg = next(m for m in messages if m.get("role") == "assistant")
    fn_name = assistant_msg["tool_calls"][0]["function"]["name"]
    assert len(fn_name) == 64, f"function.name 应被截到 64, 实际 {len(fn_name)}"
    assert fn_name == "x" * 64

    # role=tool.name 从 tool_use 回带,与 function.name 同源同值(配对一致)
    tool_msg = next(m for m in messages if m.get("role") == "tool")
    assert tool_msg["name"] == fn_name


def test_anthropic_to_openai_messages_sanitizes_illegal_tool_name():
    """tool_use.name 含非法字符(:. 等)时转 OpenAI 清洗为 _。"""
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_a", "name": "mcp__foo.bar:read", "input": {}},
                ],
            },
        ],
    }
    messages, _ = anthropic_to_openai_messages(body)
    assistant_msg = next(m for m in messages if m.get("role") == "assistant")
    assert assistant_msg["tool_calls"][0]["function"]["name"] == "mcp__foo_bar_read"


def test_openai_messages_to_responses_payload_sanitizes_function_call_name():
    """Responses 出站 input[].name = function.name,必须归一化(① 的直接现场)。

    24505 字符的 name 走 OpenAI→Responses 转换后必须被截到 64,否则 Responses
    上游回 "input[N].name too long"。
    """
    huge_name = "y" * 24505
    messages = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": "call_a",
                "type": "function",
                "function": {"name": huge_name, "arguments": "{}"},
            }],
        },
        {"role": "user", "content": "ok"},
    ]
    payload = openai_messages_to_responses_payload("m", messages, False, {})
    fc_items = [i for i in payload["input"] if i.get("type") == "function_call"]
    assert fc_items and len(fc_items[0]["name"]) == 64
    assert fc_items[0]["name"] == "y" * 64


def test_sanitize_anthropic_tool_ids_in_body_now_cleans_tool_use_name():
    """Anthropic 直通场景(passthrough / _build_anthropic_payload)走 body 清洗,
    tool_use.name 也要归一,否则 Anthropic 原生上游回 "Invalid tool use format"。
    """
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "toolu_a", "name": "z" * 300, "input": {}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_a", "content": "ok"},
                ],
            },
        ],
    }
    sanitized = sanitize_anthropic_tool_ids_in_body(body)
    block = sanitized["messages"][0]["content"][0]
    assert len(block["name"]) == 64
    assert block["name"] == "z" * 64
    # id 清洗仍生效,pairing 不断裂
    assert sanitized["messages"][1]["content"][0]["tool_use_id"] == "toolu_a"


def test_sanitize_anthropic_request_body_cleans_tool_use_name():
    """sanitize_anthropic_request_body (所有 anthropic 出站路径入口) 也清洗 name。"""
    body = {
        "model": "claude-opus-4-8",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "toolu_a", "name": "bad.name:here", "input": {}},
                ],
            },
        ],
    }
    sanitized = sanitize_anthropic_request_body(body)
    assert sanitized["messages"][0]["content"][0]["name"] == "bad_name_here"


def test_combine_messages_tool_no_name_uses_tool_not_id():
    """combine_messages 对仅含 tool_call_id 的 role=tool 用 "tool" 兜底，
    不再把 id 截断当名字（旧行为 tool_<id前缀>）。"""
    combined = combine_messages([
        {"role": "tool", "tool_call_id": "call_a6ce550dae0949f48df56953", "content": "ok"},
    ])
    assert 'name="tool"' in combined
    assert "call_a6ce" not in combined


# ==================== Anthropic thinking block sanitizer ====================


def test_sanitize_anthropic_request_body_strips_empty_signature_thinking():
    """空 signature 的 thinking block 无法被上游 Anthropic 校验通过，必须剔除。

    复现线上报错：客户端回传历史 assistant 消息，thinking block 的 signature 为空字符串，
    上游报 `data did not match any variant of untagged enum MessageContent`。
    我们做 Claude Code 伪装，出口为 Anthropic Messages，必须把这类无效块清掉。
    """
    body = {
        "model": "claude-opus-4-8",
        "messages": [
            {"role": "user", "content": "hi"},
            {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "some reasoning", "signature": ""},
                    {"type": "tool_use", "id": "toolu_bdrk_01ABC",
                     "name": "mcp__codegraph__codegraph_explore", "input": {"query": "q"}},
                ],
            },
        ],
    }

    sanitized = sanitize_anthropic_request_body(body)

    content = sanitized["messages"][1]["content"]
    # 空 signature 的 thinking block 被剔除
    assert not any(b.get("type") == "thinking" for b in content), "空 signature thinking 未被剔除"
    # tool_use block 保留
    assert any(b.get("type") == "tool_use" for b in content), "tool_use 被误删"
    # 原始 body 未被突变
    assert body["messages"][1]["content"][0]["signature"] == ""


def test_sanitize_anthropic_request_body_keeps_valid_signature_thinking():
    """合法 signature 的 thinking block 必须原样保留。"""
    body = {
        "model": "claude-opus-4-8",
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "real reasoning",
                     "signature": "EuYBCk...validSig..."},
                    {"type": "text", "text": "answer"},
                ],
            },
        ],
    }

    sanitized = sanitize_anthropic_request_body(body)
    content = sanitized["messages"][0]["content"]
    thinking_blocks = [b for b in content if b.get("type") == "thinking"]
    assert len(thinking_blocks) == 1
    assert thinking_blocks[0]["signature"] == "EuYBCk...validSig..."


def test_sanitize_anthropic_request_body_strips_missing_signature_thinking():
    """signature 字段缺失的 thinking block 也要剔除。"""
    body = {
        "messages": [
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "no sig field"},
                {"type": "text", "text": "ok"},
            ]},
        ],
    }

    sanitized = sanitize_anthropic_request_body(body)
    content = sanitized["messages"][0]["content"]
    assert not any(b.get("type") == "thinking" for b in content)
    assert any(b.get("type") == "text" for b in content)


def test_sanitize_anthropic_request_body_strips_synthetic_signature_thinking():
    """跨协议转换补的合成 signature 不是上游签名，回传给真 Anthropic 会被拒，必须剔除。

    OpenAI 上游只给 reasoning_content，没有签名；convert_stream_to_anthropic 为了让
    带 interleaved-thinking 的客户端认这个 thinking 块，补了
    PROXY_SYNTHETIC_THINKING_SIGNATURE。会话中途切到 anthropic 协议渠道时这个块会
    直通触达真实 Anthropic，语义同「空 signature」——无法验证的 thinking 块一律不发。
    """
    from message_utils import PROXY_SYNTHETIC_THINKING_SIGNATURE

    body = {
        "model": "claude-opus-4-8",
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "proxy reasoning",
                     "signature": PROXY_SYNTHETIC_THINKING_SIGNATURE},
                    {"type": "tool_use", "id": "toolu_01ABC", "name": "my_tool", "input": {}},
                ],
            },
        ],
    }

    sanitized = sanitize_anthropic_request_body(body)
    content = sanitized["messages"][0]["content"]
    assert not any(b.get("type") == "thinking" for b in content), "合成 signature thinking 未被剔除"
    assert any(b.get("type") == "tool_use" for b in content), "tool_use 被误删"


# ==================== coerce_anthropic_system ====================


def test_coerce_system_auto_and_empty_keep_original():
    """auto / 空类型 / 空值均原样返回，不做任何转化。"""
    arr = [{"type": "text", "text": "a"}]
    assert coerce_anthropic_system("s", "auto") == "s"
    assert coerce_anthropic_system(arr, "auto") is arr
    assert coerce_anthropic_system("s", "") == "s"
    assert coerce_anthropic_system(None, "str") is None
    assert coerce_anthropic_system("", "array") == ""


def test_coerce_system_to_str_merges_list_text_blocks():
    system = [
        {"type": "text", "text": "part1"},
        {"type": "text", "text": "part2", "cache_control": {"type": "ephemeral"}},
    ]
    assert coerce_anthropic_system(system, "str") == "part1\n\npart2"
    # 已是字符串则原样
    assert coerce_anthropic_system("plain", "str") == "plain"


def test_coerce_system_to_array_wraps_string_and_keeps_list():
    assert coerce_anthropic_system("hello", "array") == [{"type": "text", "text": "hello"}]
    # 已是列表原样保留 cache_control
    system = [{"type": "text", "text": "x", "cache_control": {"type": "ephemeral"}}]
    assert coerce_anthropic_system(system, "array") is system


def _anthropic_provider_with_system_type(system_type: str):
    return CustomProvider(
        "u",
        "sk-test",
        protocol="anthropic",
        base_url="https://example.invalid",
        chat_protocols=[
            {"id": "anthropic-chat", "protocol": "anthropic", "path": "/v1/messages",
             "upstream_stream": True, "system_type": system_type},
        ],
    )


def test_apply_system_type_str_merges_client_list_system():
    """system_type=str：list system 合并为字符串（直通与跨协议共用同一出口逻辑）。"""
    provider = _anthropic_provider_with_system_type("str")
    endpoint = provider.get_chat_protocol_candidates("m", "anthropic")[0]
    data = {"system": [{"type": "text", "text": "you are"}, {"type": "text", "text": "helpful"}]}
    result = provider._apply_system_type(data, {"_endpoint_config": endpoint})
    assert result["system"] == "you are\n\nhelpful"


def test_apply_system_type_array_wraps_string_system():
    """system_type=array：字符串 system 包成列表。"""
    provider = _anthropic_provider_with_system_type("array")
    endpoint = provider.get_chat_protocol_candidates("m", "anthropic")[0]
    data = {"system": "be nice"}
    result = provider._apply_system_type(data, {"_endpoint_config": endpoint})
    assert result["system"] == [{"type": "text", "text": "be nice"}]


def test_apply_system_type_auto_keeps_system_untouched():
    """auto：保持原始 system 形态不变。"""
    provider = _anthropic_provider_with_system_type("auto")
    endpoint = provider.get_chat_protocol_candidates("m", "anthropic")[0]
    data = {"system": [{"type": "text", "text": "sys"}]}
    result = provider._apply_system_type(data, {"_endpoint_config": endpoint})
    assert result["system"] == [{"type": "text", "text": "sys"}]


def test_apply_system_type_end_to_end_str_via_build_payload():
    """端到端：str 类型在跨协议构造出口把注入后的 system 收敛为字符串。"""
    provider = _anthropic_provider_with_system_type("str")
    endpoint = provider.get_chat_protocol_candidates("m", "anthropic")[0]
    payload = provider._build_anthropic_payload(
        "m",
        [{"role": "system", "content": "be nice"}, {"role": "user", "content": "hi"}],
        False,
        _from_openai=True,
        _endpoint_config=endpoint,
    )
    # 无论 preset 是否注入 marker，最终 system 必为字符串形态
    assert isinstance(payload["system"], str)
    assert "be nice" in payload["system"]


# ==================== openai 协议行 send_reasoning_content ====================


def _openai_provider_with_send_reasoning(send_reasoning_content):
    return CustomProvider(
        "u",
        "sk-test",
        protocol="openai",
        base_url="https://example.invalid",
        chat_protocols=[
            {"id": "openai-chat", "protocol": "openai", "path": "/v1/chat/completions",
             "upstream_stream": True, "send_reasoning_content": send_reasoning_content},
        ],
    )


def test_openai_send_reasoning_content_default_passes_through():
    """默认（true/缺省）：assistant.reasoning_content 原样透传给上游。"""
    from message_utils import strip_assistant_reasoning_content
    provider = _openai_provider_with_send_reasoning(True)
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    messages = [
        {"role": "assistant", "content": "ok", "reasoning_content": "think"},
        {"role": "user", "content": "hi"},
    ]
    payload = provider._build_openai_payload(
        "m", messages, False, _endpoint_config=endpoint,
    )
    assert payload["messages"][0].get("reasoning_content") == "think"
    # 助手函数：无 assistant.reasoning_content 可剥时原样返回同一列表
    clean_msgs = [{"role": "user", "content": "hi"}]
    assert strip_assistant_reasoning_content(clean_msgs) is clean_msgs


def test_openai_send_reasoning_content_false_strips_assistant_field():
    """send_reasoning_content=false：出口剥离 assistant.reasoning_content，且不改原列表。"""
    from message_utils import strip_assistant_reasoning_content
    provider = _openai_provider_with_send_reasoning(False)
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    messages = [
        {"role": "assistant", "content": "ok", "reasoning_content": "secret-thought"},
        {"role": "user", "content": "hi"},
    ]
    snapshot = [dict(m) for m in messages]
    payload = provider._build_openai_payload(
        "m", messages, False, _endpoint_config=endpoint,
    )
    out = payload["messages"]
    assert "reasoning_content" not in out[0]
    assert out[0].get("content") == "ok"
    # 非 assistant 消息不受影响
    assert out[1].get("content") == "hi"
    # 原列表与消息对象未被原地修改（跨重试复用安全）
    assert messages == snapshot
    assert messages[0].get("reasoning_content") == "secret-thought"
    # 助手函数直接验证：返回新列表，原列表不变
    stripped = strip_assistant_reasoning_content(messages)
    assert stripped is not messages
    assert "reasoning_content" not in stripped[0]
    assert messages[0].get("reasoning_content") == "secret-thought"


# ==================== Responses call_id / tool fallback ====================


def test_openai_messages_to_responses_payload_fills_tool_fields():
    messages = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"id": "", "type": "function", "function": {"arguments": ""}}],
        },
        {"role": "tool", "content": "ok"},
    ]

    payload = openai_messages_to_responses_payload("gpt-5", messages, False, {})
    function_call = next(item for item in payload["input"] if item.get("type") == "function_call")
    output = next(item for item in payload["input"] if item.get("type") == "function_call_output")

    assert function_call["call_id"].startswith("call_")
    assert function_call["name"] == "tool"
    assert function_call["arguments"] == "{}"
    assert output["call_id"] == function_call["call_id"]


def test_normalize_responses_input_fills_missing_call_id_name_arguments():
    normalized = normalize_responses_input([
        {"type": "function_call", "id": "fc_1", "name": "", "arguments": ""},
        {"type": "function_call_output", "output": None},
    ])

    assert normalized[0]["call_id"] == "fc_1"
    assert normalized[0]["name"] == "tool"
    assert normalized[0]["arguments"] == "{}"
    assert normalized[1]["call_id"].startswith("call_")
    assert normalized[1]["output"] == ""


def test_responses_to_openai_messages_pairs_missing_output_call_id():
    messages, _ = responses_to_openai_messages({
        "input": [
            {"type": "function_call", "name": "", "arguments": ""},
            {"type": "function_call_output", "output": "ok"},
        ]
    })

    assistant_msg = next(m for m in messages if m.get("role") == "assistant")
    tool_msg = next(m for m in messages if m.get("role") == "tool")
    assert assistant_msg["tool_calls"][0]["id"].startswith("call_")
    assert assistant_msg["tool_calls"][0]["function"]["name"] == "tool"
    assert assistant_msg["tool_calls"][0]["function"]["arguments"] == "{}"
    assert tool_msg["tool_call_id"] == assistant_msg["tool_calls"][0]["id"]
