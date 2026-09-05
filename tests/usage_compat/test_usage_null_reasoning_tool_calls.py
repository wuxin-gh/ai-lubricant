"""usage=null + reasoning/tool_calls 流式报文兼容回归测试。"""

import asyncio
import json

import admin
import main
from providers.base import BaseProvider
from providers.base import IncompleteStreamError


STREAM_ID = "msg_ROGWVSx6sEneC29oTMOot2Ra"
MODEL = "claude-opus-4-8"
CREATED = 1784512982


def _sse(payload) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _chunk(delta=None, finish_reason=None):
    return {
        "id": STREAM_ID,
        "object": "chat.completion.chunk",
        "created": CREATED,
        "model": MODEL,
        "system_fingerprint": None,
        "choices": [{
            "delta": delta or {},
            "logprobs": None,
            "finish_reason": finish_reason,
            "index": 0,
        }],
        "usage": None,
    }


class UsageNullReasoningToolProvider(BaseProvider):
    PROVIDER_NAME = "usage-null-reasoning-tool-test"

    def __init__(self, frames: list[str]):
        super().__init__("user", "password")
        self.frames = frames

    async def init_auth(self, is_check: bool = False) -> bool:
        return True

    async def check_auth(self) -> bool:
        return True

    async def fetch_upstream_model_list(self):
        return []

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield {}
        for frame in self.frames:
            yield frame

    async def _do_non_stream_chat(self, model_id, messages, **kwargs):
        return {}


def _frames() -> list[str]:
    return [
        _sse(_chunk({"content": "", "role": "assistant"})),
        _sse(_chunk()),
        _sse(_chunk({"reasoning_content": "Looking"})),
        _sse(_chunk({"reasoning_content": " for"})),
        _sse(_chunk({"reasoning_content": " the `_headers` method in"})),
        _sse(_chunk({"reasoning_content": " custom.py."})),
        "data: null\n\n",
        _sse(_chunk({
            "tool_calls": [{
                "index": 0,
                "id": "toolu_bdrk_01RatiQjG4RWnRoZm31GkZ8K",
                "type": "function",
                "function": {"name": "Grep", "arguments": ""},
            }],
        })),
        _sse(_chunk({
            "tool_calls": [{
                "index": 0,
                "type": "function",
                "function": {
                    "arguments": '{"output_mode": "content", "pattern": "def _headers", '
                    '"path": "d:\\\\code\\\\ai-lubricant\\\\providers\\\\custom.py", "-n": true}',
                },
            }],
        })),
        "data: null\n\n",
        _sse(_chunk({}, finish_reason="tool_calls")),
        "data: [DONE]\n\n",
    ]


def _collect(provider):
    async def run():
        return [
            chunk
            async for chunk in provider.chat(
                MODEL,
                [{"role": "user", "content": "inspect the headers method"}],
                stream=True,
            )
        ]

    return asyncio.run(run())


def _usage_chunks(chunks):
    usages = []
    for chunk in chunks:
        if not isinstance(chunk, str):
            continue
        for line in chunk.splitlines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            payload = json.loads(data)
            if isinstance(payload, dict) and isinstance(payload.get("usage"), dict):
                usages.append(payload["usage"])
    return usages


def test_usage_null_reasoning_and_tool_calls_estimate_without_zero_error():
    raw_frames = _frames()
    provider = UsageNullReasoningToolProvider(raw_frames)
    chunks = _collect(provider)

    usages = _usage_chunks(chunks)
    assert usages, "usage=null 上游应在流结束时补发估算 usage"
    assert usages[-1]["completion_tokens"] > 0
    assert usages[-1]["prompt_tokens"] > 0
    assert not any("upstream response usage reports zero completion tokens" in str(chunk) for chunk in chunks)


def test_usage_null_payloads_do_not_hide_reasoning_or_tool_output():
    reasoning_chunk = _sse(_chunk({"reasoning_content": "Looking"}))
    tool_chunk = _sse(_chunk({"tool_calls": [{"index": 0, "function": {"name": "Grep", "arguments": ""}}]}))

    assert main._chunk_has_stream_content(reasoning_chunk)
    assert main._chunk_has_stream_content(tool_chunk)
    assert admin._has_first_token_content(reasoning_chunk)
    assert admin._has_first_token_content(tool_chunk)


def _reasoning_alias_frames() -> list[str]:
    def frame(delta=None, finish_reason=None, usage=None):
        payload = {
            "id": "gen_01KZ1FRKYR9E5EXHPWA88YS6HM",
            "object": "chat.completion.chunk",
            "created": 1785682749,
            "model": "zai/glm-5.2",
            "choices": [{
                "index": 0,
                "delta": delta or {},
                "logprobs": None,
                "finish_reason": finish_reason,
            }],
        }
        if usage is not None:
            payload["usage"] = usage
        return _sse(payload)

    return [
        frame({"role": "assistant"}),
        frame({
            "reasoning": "The",
            "reasoning_details": [{"type": "reasoning.text", "text": "The", "format": "unknown", "index": 0}],
        }),
        frame({
            "reasoning": " user is asking",
            "reasoning_details": [{"type": "reasoning.text", "text": " user is asking", "format": "unknown", "index": 0}],
        }),
        frame(
            {"provider_metadata": {"gateway": {"generationId": "gen_01KZ1FRKYR9E5EXHPWA88YS6HM"}}},
            finish_reason="stop",
            usage={
                "prompt_tokens": 312088,
                "completion_tokens": 1527,
                "total_tokens": 313615,
                "completion_tokens_details": {"reasoning_tokens": 1526},
            },
        ),
        "data: [DONE]\n\n",
    ]


def test_reasoning_alias_stream_is_real_output_and_keeps_reported_usage():
    """线上 delta.reasoning 形态不能被 BaseProvider 或主链路误判为空流。"""
    raw_frames = _reasoning_alias_frames()
    chunks = _collect(UsageNullReasoningToolProvider(raw_frames))

    assert any('"reasoning": "The"' in chunk for chunk in chunks if isinstance(chunk, str))
    assert any('"reasoning": " user is asking"' in chunk for chunk in chunks if isinstance(chunk, str))
    assert not any("upstream response usage reports zero completion tokens" in str(chunk) for chunk in chunks)

    accumulated_usage = {}
    had_content = False
    for chunk in chunks:
        main._accumulate_stream_usage(accumulated_usage, chunk)
        had_content = had_content or main._chunk_has_stream_content(chunk)

    assert had_content
    assert accumulated_usage["completion_tokens"] == 1527
    main._validate_stream_completion(accumulated_usage, had_content)


def test_response_content_detection_accepts_reasoning_alias():
    assert main._response_has_content({"choices": [{"delta": {"reasoning": "thinking"}}]})
    assert main._response_has_content({"choices": [{"message": {"reasoning": "thinking"}}]})


def _user_reported_frames() -> list[str]:
    """Claude Code 现场 trace：真实文本/tool_calls，所有 chunk usage=null，夹杂 data:null。"""
    def frame(delta=None, finish_reason=None):
        return _sse({
            "id": "msg_011CdGd279JDTydheEs3VvJ2",
            "object": "chat.completion.chunk",
            "created": 1784701667,
            "model": MODEL,
            "system_fingerprint": None,
            "choices": [{
                "delta": delta or {},
                "logprobs": None,
                "finish_reason": finish_reason,
                "index": 0,
            }],
            "usage": None,
        })

    return [
        frame({"content": "", "role": "assistant"}),
        frame(),
        frame({"content": "This"}),
        frame({"content": " is an upstream context window error that isn't being caught/classified as non"}),
        frame({"content": "-retryable. Let me look at how it's currently detected."}),
        "data: null\n\n",
        frame({"tool_calls": [{
            "index": 0,
            "id": "toolu_01JaDjigMWsJCZffekJqzUtW",
            "type": "function",
            "function": {"name": "mcp__codegraph__codegraph_explore", "arguments": ""},
        }]}),
        frame({"tool_calls": [{
            "index": 0,
            "type": "function",
            "function": {"arguments": '{"projectPath": "d:\\\\code\\\\ai-lubricant",'},
        }]}),
        "data: null\n\n",
        frame({}, finish_reason="tool_calls"),
        "data: [DONE]\n\n",
    ]


class UserReportedCrossProtocolProvider(UsageNullReasoningToolProvider):
    async def _do_stream_chat(self, model_id, messages, **kwargs):
        for frame in _user_reported_frames():
            yield frame


def test_user_reported_content_and_tool_calls_survive_anthropic_conversion():
    """有内容的 usage=null trace 不能被跨协议链路误判为 zero completion 空响应。"""
    provider = UserReportedCrossProtocolProvider([])

    async def collect():
        return [
            chunk async for chunk in provider.chat_anthropic(
                MODEL,
                [{"role": "user", "content": "inspect the headers method"}],
                stream=True,
            )
        ]

    chunks = asyncio.run(collect())
    assert any('"type": "content_block_delta"' in chunk and 'text_delta' in chunk for chunk in chunks if isinstance(chunk, str))
    assert any('"type": "input_json_delta"' in chunk for chunk in chunks if isinstance(chunk, str))
    assert any('"type": "message_stop"' in chunk for chunk in chunks if isinstance(chunk, str))
    assert not any("upstream response usage reports zero completion tokens" in str(chunk) for chunk in chunks)

    message_delta = next(
        json.loads(line[5:].strip())
        for chunk in chunks if isinstance(chunk, str)
        for line in chunk.splitlines()
        if line.startswith("data:") and '"type": "message_delta"' in line
    )
    assert message_delta["usage"]["output_tokens"] > 0



def _glm_usage_null_frames() -> list[str]:
    def frame(delta=None, finish_reason=None, usage_marker=True, created=1784769371):
        payload = {
            "id": "292f702f134a48a1a77647d809fcf784",
            "object": "chat.completion.chunk",
            "created": created,
            "model": "glm-5.2",
            "choices": [{
                "index": 0,
                "delta": delta or {},
                "logprobs": None,
                "finish_reason": finish_reason,
                "matched_stop": 154829 if finish_reason else None,
            }],
        }
        if usage_marker:
            payload["usage"] = None
        return _sse(payload)

    return [
        frame({"reasoning_content": None, "role": "assistant", "content": ""}, usage_marker=False),
        frame({"role": None, "content": "My", "reasoning_content": None, "tool_calls": None}),
        frame({"role": None, "content": " BFS", "reasoning_content": None, "tool_calls": None}),
        frame({"role": None, "content": " missed", "reasoning_content": None, "tool_calls": None}),
        frame({"role": None, "content": " UserMenu/Sidebar/MainConfig", "reasoning_content": None, "tool_calls": None}),
        frame({"role": None, "content": ". Let me re-run a corrected BFS.", "reasoning_content": None, "tool_calls": None}),
        frame({
            "role": None,
            "content": None,
            "reasoning_content": None,
            "tool_calls": [{
                "id": "call_e606bcb97cf840158e710684",
                "index": 0,
                "type": "function",
                "function": {
                    "name": "Bash",
                    "arguments": '{"command": "python resolve.py", "description": "Resolve transitive admin closure with index/css"}',
                },
            }],
        }, created=1784769374),
        frame({"reasoning_content": None}, finish_reason="tool_calls", usage_marker=False, created=1784769374),
        "data: [DONE]\n\n",
    ]


class GLMUsageNullProvider(UsageNullReasoningToolProvider):
    async def _do_stream_chat(self, model_id, messages, **kwargs):
        for frame in _glm_usage_null_frames():
            yield frame


def test_user_reported_openai_tool_argument_fragments_still_estimate_usage():
    """真实线上形态：data:null、usage:null、tool arguments 分片仍应产生正 usage。"""
    def frame(delta=None, finish_reason=None):
        return _sse({
            "id": "msg_01ROyVin1Lwwv7i0JFmNGbsQ",
            "object": "chat.completion.chunk",
            "created": 1785599836,
            "model": "claude-opus-5",
            "choices": [{"delta": delta or {}, "finish_reason": finish_reason, "index": 0}],
            "usage": None,
        })

    frames = [
        frame({"content": "", "role": "assistant"}),
        frame(),
        frame({"reasoning_content": "\\n"}),
        "data: null\n\n",
        frame({"tool_calls": [{"index": 1, "type": "function", "function": {"arguments": "点凭据。验"}}]}),
        frame({"tool_calls": [{"index": 1, "type": "function", "function": {"arguments": "证期内保留该开关，双进程"}}]}),
        frame({"tool_calls": [{"index": 1, "type": "function", "function": {"arguments": "模式稳定后生产"}}]}),
        frame({}, finish_reason="tool_calls"),
        "data: [DONE]\n\n",
    ]

    chunks = _collect(UsageNullReasoningToolProvider(frames))
    usages = _usage_chunks(chunks)
    assert usages and usages[-1]["completion_tokens"] > 0
    tool_chunks = [chunk for chunk in chunks if isinstance(chunk, str) and "tool_calls" in chunk]
    assert len(tool_chunks) >= 3
    assert any("点凭据。验" in chunk for chunk in tool_chunks)
    assert any("模式稳定后生产" in chunk for chunk in tool_chunks)


def test_nonstream_passthrough_anthropic_wrapper_body_counts_as_content():
    """GLM /v1/messages 非流式 → 渠道流式：封装 body 里有内容，不能因顶层零 usage 判空。"""
    wrapper = {
        "_passthrough_anthropic": True,
        "body": {
            "id": "msg_292f702f",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "My BFS missed UserMenu/Sidebar..."}],
            "stop_reason": "tool_use",
        },
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    assert main._response_has_content(wrapper)
    main._validate_upstream_usage_payload(wrapper, stream=False)  # 不抛即通过
    # body 仍真空的封装要继续判空（保留原语义）
    empty_wrapper = {
        "_passthrough_anthropic": True,
        "body": {"content": []},
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    assert not main._response_has_content(empty_wrapper)
    import pytest as _pytest
    with _pytest.raises(main.EmptyNonStreamResponseError):
        main._validate_upstream_usage_payload(empty_wrapper, stream=False)


def test_glm_usage_null_trace_survives_anthropic_conversion():
    provider = GLMUsageNullProvider([])

    async def collect():
        return [
            chunk async for chunk in provider.chat_anthropic(
                "glm-5.2",
                [{"role": "user", "content": "inspect imports"}],
                stream=True,
            )
        ]

    chunks = asyncio.run(collect())
    assert any('"type": "content_block_delta"' in chunk and "text_delta" in chunk for chunk in chunks if isinstance(chunk, str))
    assert any('"type": "input_json_delta"' in chunk for chunk in chunks if isinstance(chunk, str))
    assert any('"type": "message_stop"' in chunk for chunk in chunks if isinstance(chunk, str))
    assert not any("upstream response usage reports zero completion tokens" in str(chunk) for chunk in chunks)

    message_delta = next(
        json.loads(line[5:].strip())
        for chunk in chunks if isinstance(chunk, str)
        for line in chunk.splitlines()
        if line.startswith("data:") and '"type": "message_delta"' in line
    )
    assert message_delta["usage"]["output_tokens"] > 0


def test_truly_empty_cross_protocol_stream_still_raises():
    class EmptyProvider(UsageNullReasoningToolProvider):
        async def _do_stream_chat(self, model_id, messages, **kwargs):
            yield _sse(_chunk({}, finish_reason="stop"))
            yield "data: [DONE]\n\n"

    provider = EmptyProvider([])

    async def collect():
        return [
            chunk async for chunk in provider.chat_anthropic(
                MODEL,
                [{"role": "user", "content": "empty"}],
                stream=True,
            )
        ]

    with __import__("pytest").raises(IncompleteStreamError):
        asyncio.run(collect())
