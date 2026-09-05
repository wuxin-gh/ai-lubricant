import asyncio
import json

from fastapi import HTTPException

import main
from db import PostgresClient
from message_utils import openai_messages_to_responses_payload, parse_sse_event, responses_usage_to_openai_usage, convert_stream_to_responses, iter_sse_payloads
from providers.base import BaseProvider
from providers.custom import CustomProvider
from usage_utils import normalize_usage


def test_normalize_usage_keeps_cache_tokens_as_input_details():
    usage = normalize_usage({
        "input_tokens": 100,
        "output_tokens": 30,
        "input_tokens_details": {
            "cached_tokens": 40,
            "cache_creation_input_tokens": 10,
        },
    })

    assert usage == {
        "prompt_tokens": 100,
        "completion_tokens": 30,
        "total_tokens": 130,
        "cached_tokens": 40,
        "cache_creation_tokens": 10,
        "reasoning_tokens": 0,
    }


def test_normalize_usage_extracts_cache_write_aliases_and_breakdowns():
    assert normalize_usage({
        "input_tokens": 100,
        "output_tokens": 30,
        "cache_write_tokens": 12,
    })["cache_creation_tokens"] == 12
    assert normalize_usage({
        "input_tokens": 100,
        "output_tokens": 30,
        "prompt_cache_miss_tokens": 13,
    })["cache_creation_tokens"] == 13
    assert normalize_usage({
        "input_tokens": 100,
        "output_tokens": 30,
        "cache_creation": {
            "ephemeral_5m_input_tokens": 10,
            "ephemeral_1h_input_tokens": 20,
        },
    })["cache_creation_tokens"] == 30
    assert normalize_usage({
        "input_tokens": 100,
        "output_tokens": 30,
        "input_tokens_details": {
            "cache_creation": {
                "ephemeral_5m_input_tokens": 7,
                "ephemeral_1h_input_tokens": 8,
            },
        },
    })["cache_creation_tokens"] == 15


def test_normalize_usage_adds_cache_when_total_input_excludes_it():
    # OpenAI 风格（prompt_tokens 本应是总输入）但渠道把缓存从 input 扣掉了：
    # input(50) <= 缓存(命中40 + 写入20 = 60)，需把缓存加回 -> 50 + 60 = 110。
    usage = normalize_usage({
        "prompt_tokens": 50,
        "completion_tokens": 30,
        "prompt_tokens_details": {
            "cached_tokens": 40,
            "cache_creation_input_tokens": 20,
        },
    })
    assert usage["prompt_tokens"] == 110
    assert usage["cached_tokens"] == 40
    assert usage["cache_creation_tokens"] == 20
    assert usage["total_tokens"] == 140


def test_normalize_usage_keeps_total_input_when_it_already_includes_cache():
    # 正常 OpenAI：prompt_tokens 已含缓存明细，input(100) > 缓存(40)，不重复加。
    usage = normalize_usage({
        "prompt_tokens": 100,
        "completion_tokens": 30,
        "prompt_tokens_details": {
            "cached_tokens": 40,
        },
    })
    assert usage["prompt_tokens"] == 100
    assert usage["total_tokens"] == 130


def test_normalize_usage_recomputes_total_from_prompt_plus_completion():
    usage = normalize_usage({
        "prompt_tokens": 100,
        "completion_tokens": 30,
        "total_tokens": 150,  # upstream total may omit/cache-differ; normalized total is authoritative
    })
    assert usage["total_tokens"] == 130, "total should be normalized as prompt + completion"


def test_normalize_usage_ignores_upstream_total_when_prompt_completion_missing():
    usage = normalize_usage({
        "total_tokens": 200,
    })
    assert usage["total_tokens"] == 0, "upstream-only total is not enough to derive normalized prompt + completion"


def test_normalize_usage_total_zero_when_all_missing():
    usage = normalize_usage({})
    assert usage["total_tokens"] == 0


def test_usage_normalization_keeps_cache_tokens_as_input_details():
    assert main._usage({
        "usage": {
            "input_tokens": 100,
            "output_tokens": 30,
            "input_tokens_details": {
                "cached_tokens": 40,
                "cache_creation_input_tokens": 10,
            },
        }
    }) == {
        "prompt_tokens": 100,
        "completion_tokens": 30,
        "total_tokens": 130,
        "cached_tokens": 40,
        "cache_creation_tokens": 10,
        "reasoning_tokens": 0,
    }
    assert normalize_usage({"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 4, "cache_creation_input_tokens": 3}) == {
        "prompt_tokens": 17,
        "completion_tokens": 5,
        "total_tokens": 22,
        "cached_tokens": 4,
        "cache_creation_tokens": 3,
        "reasoning_tokens": 0,
    }


def test_normalize_usage_extracts_openai_cache_and_reasoning_tokens():
    usage = normalize_usage({
        "prompt_tokens": 9679,
        "completion_tokens": 106,
        "total_tokens": 9785,
        "prompt_tokens_details": {"cached_tokens": 7936},
        "completion_tokens_details": {"reasoning_tokens": 93},
    })

    assert usage == {
        "prompt_tokens": 9679,
        "completion_tokens": 106,
        "total_tokens": 9785,
        "cached_tokens": 7936,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 93,
    }


def test_normalize_usage_extracts_native_usage():
    usage = normalize_usage({
        "promptTokens": 3098,
        "completionTokens": 175,
        "totalTokens": 3273,
        "nativeUsage": {
            "completion_tokens": 175,
            "prompt_tokens": 3098,
            "total_tokens": 3273,
            "prompt_tokens_details": {"cached_tokens": 3072},
            "completion_tokens_details": {"reasoning_tokens": 96},
        },
    })

    assert usage == {
        "prompt_tokens": 3098,
        "completion_tokens": 175,
        "total_tokens": 3273,
        "cached_tokens": 3072,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 96,
    }


def test_custom_provider_usage_is_not_rewritten_when_unknown():
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="openai",
        base_url="https://example.invalid",
    )
    usage = {"vendorTokens": 12}

    assert provider.format_openai_usage(usage) == usage


class StreamOrderProvider(BaseProvider):
    PROVIDER_NAME = "stream-order-test"
    is_custom_providers = True

    async def init_auth(self, is_check: bool = False) -> bool:
        return True

    async def check_auth(self) -> bool:
        return True

    async def fetch_upstream_model_list(self) -> list[dict]:
        return []

    async def _do_non_stream_chat(self, model_id: str, messages: list[dict], **kwargs) -> dict:
        return {}

    async def _do_stream_chat(self, model_id: str, messages: list[dict], **kwargs):
        yield {
            "content": "",
            "thinking": "",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "Bash", "arguments": '{"command":"pwd"}'},
            }],
        }
        yield {
            "content": "",
            "thinking": "",
            "tool_calls": [],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            "finish_reason": "tool_calls",
        }


def _stream_json_events(chunks):
    events = []
    for chunk in chunks:
        if isinstance(chunk, str) and chunk.startswith("data: {"):
            events.append(json.loads(chunk[len("data: "):].strip()))
    return events


def test_request_stream_enabled_normalizes_string_false_values():
    false_values = [False, None, 0, 0.0, "", "false", "False", "  false  ", "0", "no", "off", "null", "none"]
    for value in false_values:
        assert main._request_stream_enabled({"stream": value}) is False

    true_values = [True, 1, 1.0, "true", "True", "  true  ", "1", "yes", "on", "unexpected-non-empty"]
    for value in true_values:
        assert main._request_stream_enabled({"stream": value}) is True


def test_stream_finish_reason_and_usage_order_follows_internal_chunk_order():
    provider = StreamOrderProvider("u", "p")

    async def collect():
        return [chunk async for chunk in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True)]

    events = _stream_json_events(asyncio.run(collect()))
    finish_index = next(
        i for i, event in enumerate(events)
        if event.get("choices") and event["choices"][0].get("finish_reason") == "tool_calls"
    )
    usage_index = next(i for i, event in enumerate(events) if event.get("usage"))

    assert finish_index == usage_index
    assert events[usage_index]["choices"][0]["finish_reason"] == "tool_calls"


def test_stream_usage_before_finish_order_is_preserved():
    class UsageFirstProvider(StreamOrderProvider):
        async def _do_stream_chat(self, model_id: str, messages: list[dict], **kwargs):
            yield {
                "content": "",
                "thinking": "",
                "tool_calls": [],
                "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            }
            yield {"content": "", "thinking": "", "tool_calls": [], "finish_reason": "stop"}

    provider = UsageFirstProvider("u", "p")

    async def collect():
        return [chunk async for chunk in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True)]

    events = _stream_json_events(asyncio.run(collect()))
    usage_index = next(i for i, event in enumerate(events) if event.get("usage"))
    finish_index = next(
        i for i, event in enumerate(events)
        if event.get("choices") and event["choices"][0].get("finish_reason") == "stop"
    )

    assert usage_index < finish_index
    assert len([event for event in events if event.get("usage")]) == 1


def test_stream_partial_tool_call_arguments_are_yielded_immediately():
    class PartialToolProvider(StreamOrderProvider):
        async def _do_stream_chat(self, model_id: str, messages: list[dict], **kwargs):
            yield {"content": "", "thinking": "", "tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "Bash", "arguments": '{"command"'}}]}
            yield {"content": "", "thinking": "", "tool_calls": [{"index": 0, "function": {"arguments": ':"pwd"'}}]}
            yield {"content": "", "thinking": "", "tool_calls": [{"index": 0, "function": {"arguments": "}"}}]}
            yield {"content": "", "thinking": "", "tool_calls": [], "finish_reason": "tool_calls"}

    provider = PartialToolProvider("u", "p")

    async def collect():
        return [chunk async for chunk in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True)]

    events = _stream_json_events(asyncio.run(collect()))
    tool_events = [event for event in events if event.get("choices") and event["choices"][0].get("delta", {}).get("tool_calls")]

    assert [event["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"] for event in tool_events] == [
        '{"command"',
        ':"pwd"',
        "}",
    ]


def test_stream_content_with_tools_is_not_buffered_after_done():
    class ContentWithToolsProvider(StreamOrderProvider):
        async def _do_stream_chat(self, model_id: str, messages: list[dict], **kwargs):
            yield {"content": "你好！我在。", "thinking": "", "tool_calls": []}
            yield {"content": "", "thinking": "", "tool_calls": [], "finish_reason": "stop"}
            yield {"content": "", "thinking": "", "tool_calls": [], "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}}
            yield {"content": "", "thinking": "", "tool_calls": [], "done": True}

    provider = ContentWithToolsProvider("u", "p")

    async def collect():
        return [chunk async for chunk in provider.chat(
            "m",
            [{"role": "user", "content": "hi"}],
            stream=True,
            tools=[{"type": "function", "function": {"name": "noop", "parameters": {"type": "object"}}}],
        )]

    chunks = asyncio.run(collect())
    events = _stream_json_events(chunks)
    content_index = next(
        i for i, event in enumerate(events)
        if event.get("choices") and event["choices"][0].get("delta", {}).get("content") == "你好！我在。"
    )
    finish_index = next(
        i for i, event in enumerate(events)
        if event.get("choices") and event["choices"][0].get("finish_reason") == "stop"
    )
    done_index = chunks.index("data: [DONE]\n\n")
    content_chunk_index = next(i for i, chunk in enumerate(chunks) if isinstance(chunk, str) and "你好！我在。" in chunk)

    assert content_index < finish_index
    assert content_chunk_index < done_index


def test_stream_done_is_only_synthesized_when_missing():
    provider = StreamOrderProvider("u", "p")

    async def collect():
        return [chunk async for chunk in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True)]

    chunks = asyncio.run(collect())

    assert chunks.count("data: [DONE]\n\n") == 1


def test_stream_preserves_upstream_finish_reason_from_internal_chunk():
    class LengthProvider(StreamOrderProvider):
        async def _do_stream_chat(self, model_id: str, messages: list[dict], **kwargs):
            yield {"content": "partial", "thinking": "", "tool_calls": []}
            yield {
                "content": "",
                "thinking": "",
                "tool_calls": [],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                "finish_reason": "length",
            }

    provider = LengthProvider("u", "p")

    async def collect():
        return [chunk async for chunk in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True)]

    events = _stream_json_events(asyncio.run(collect()))
    finish_events = [event for event in events if event.get("choices") and event["choices"][0].get("finish_reason")]

    assert finish_events[-1]["choices"][0]["finish_reason"] == "length"


def test_custom_provider_channel_protocol_uses_configured_protocol():
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="responses",
        base_url="https://example.invalid",
    )

    assert provider.get_channel_protocol() == "responses"
    assert provider.same_channel_protocol("responses", {"input": []}, "input") is True
    assert provider.same_channel_protocol("anthropic", {"messages": []}, "messages") is False


def test_normalize_usage_keeps_openai_cached_tokens_inside_prompt_total():
    usage = normalize_usage({
        "prompt_tokens": 81425,
        "completion_tokens": 127,
        "total_tokens": 81552,
        "prompt_tokens_details": {"cached_tokens": 80896},
        "completion_tokens_details": {"reasoning_tokens": 0},
    })

    assert usage == {
        "prompt_tokens": 81425,
        "completion_tokens": 127,
        "total_tokens": 81552,
        "cached_tokens": 80896,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }

def test_parse_sse_event_supports_event_and_data_on_same_line():
    event_type, payload = parse_sse_event('event: response.completed data: {"type":"response.completed","response":{"usage":{"input_tokens":7,"output_tokens":3}}}')

    assert event_type == "response.completed"
    assert payload["response"]["usage"] == {"input_tokens": 7, "output_tokens": 3}
    assert BaseProvider.parse_sse_usage('event: response.completed data: {"type":"response.completed","response":{"usage":{"input_tokens":7,"output_tokens":3}}}') == {"input_tokens": 7, "output_tokens": 3}


def test_index_prefixed_sse_done_and_finish_reason_are_detected():
    chunk = '''data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":9905,"completion_tokens":11,"total_tokens":9916}}
8: "data: [DONE] "'''

    payloads = list(iter_sse_payloads(chunk))
    assert payloads[0]["choices"][0]["finish_reason"] == "stop"
    assert payloads[1] == "[DONE]"
    assert BaseProvider.openai_sse_completed(chunk) is True
    assert main._usage_from_stream_chunk(chunk) == {"prompt_tokens": 9905, "completion_tokens": 11, "total_tokens": 9916}


def test_responses_usage_to_openai_usage_keeps_cache_and_reasoning_out_of_total():
    assert responses_usage_to_openai_usage({
        "input_tokens": 20097,
        "input_tokens_details": {"cached_tokens": 3584},
        "output_tokens": 52,
        "output_tokens_details": {"reasoning_tokens": 9},
        "total_tokens": 20149,
    }) == {
        "prompt_tokens": 20097,
        "completion_tokens": 52,
        "total_tokens": 20149,
        "cached_tokens": 3584,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 9,
        "prompt_tokens_details": {"cached_tokens": 3584, "cache_creation_input_tokens": 0},
        "completion_tokens_details": {"reasoning_tokens": 9},
    }


def test_stream_payloads_extracts_usage_with_text_before_json():
    chunk = 'event: response.completed data: prefix-before-json {"type":"response.completed","response":{"usage":{"input_tokens":11,"output_tokens":2,"input_tokens_details":{"cached_tokens":5}}}} trailing'

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 11,
        "completion_tokens": 2,
        "total_tokens": 13,
        "cached_tokens": 5,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }


def test_stream_payloads_prefers_latest_usage_when_large_response_contains_tool_usage():
    chunk = 'event: response.completed data: {"type":"response.completed","response":{"tool_usage":{"image_gen":{"input_tokens":0,"output_tokens":0,"total_tokens":0}},"usage":{"input_tokens":20268,"input_tokens_details":{"cached_tokens":0},"output_tokens":91,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":20359}},"sequence_number":9}'

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 20268,
        "completion_tokens": 91,
        "total_tokens": 20359,
        "cached_tokens": 0,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }


def test_stream_payloads_extracts_single_quoted_usage_chunk_with_done_suffix():
    chunk = '''"data: {"id":"resp_0c715927f5cf5552016a229bcbaaf88199ad5f15b235c53e84","object":"chat.completion.chunk","created":1780653003,"model":"gpt-5.5","choices":[],"usage":{"prompt_tokens":20072,"completion_tokens":112,"total_tokens":20184,"prompt_tokens_details":{"cached_tokens":3584},"completion_tokens_details":{"reasoning_tokens":35}}} "
59: "data: [DONE]'''

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 20072,
        "completion_tokens": 112,
        "total_tokens": 20184,
        "cached_tokens": 3584,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 35,
    }


def test_stream_usage_merges_missing_fields_from_multiple_usage_shapes():
    chunk = '''data: {"usage":{"completion_tokens":14,"total_tokens":14}}
data: {"usage":{"prompt_tokens":78,"prompt_tokens_details":{"cached_tokens":10}}}
data: {"usage":{"completion_tokens_details":{"reasoning_tokens":3}}}'''

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 78,
        "completion_tokens": 14,
        "total_tokens": 92,
        "cached_tokens": 10,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 3,
    }


def test_stream_payloads_extracts_quoted_sse_usage_chunk():
    chunk = '"data: {"id":"resp_1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":19585,"completion_tokens":207,"total_tokens":19792,"prompt_tokens_details":{"cached_tokens":3584}}} "'

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 19585,
        "completion_tokens": 207,
        "total_tokens": 19792,
        "cached_tokens": 3584,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }

def test_stream_payloads_extracts_index_prefixed_quoted_usage_chunk():
    chunk = '13: "data: {"id":"resp_1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":189865,"completion_tokens":47,"total_tokens":189912,"prompt_tokens_details":{"cached_tokens":162304},"completion_tokens_details":{"reasoning_tokens":22}}} "'

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 189865,
        "completion_tokens": 47,
        "total_tokens": 189912,
        "cached_tokens": 162304,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 22,
    }


def test_stream_payloads_extracts_usage_from_indexed_event_list_text():
    chunk = '''0: "data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]} "
1: "data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"0"},"finish_reason":null}]} "
16: "data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]} "
17: "data: {"id":"resp_1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":19895,"completion_tokens":19,"total_tokens":19914,"prompt_tokens_details":{"cached_tokens":3584}}} "
18: "data: [DONE] "'''

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 19895,
        "completion_tokens": 19,
        "total_tokens": 19914,
        "cached_tokens": 3584,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }


def test_stream_payloads_extracts_usage_from_raw_indexed_event_list_without_data_split():
    chunk = '''data: {"id":"resp_2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]} "
1: "data: {"id":"resp_2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"0"},"finish_reason":null}]} "
19: "data: {"id":"resp_2","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":19928,"completion_tokens":21,"total_tokens":19949,"prompt_tokens_details":{"cached_tokens":3584}}} "
20: "data: [DONE] "'''

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 19928,
        "completion_tokens": 21,
        "total_tokens": 19949,
        "cached_tokens": 3584,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }


def test_stream_payloads_extracts_responses_event_usage():
    chunk = 'event: response.created data: {"type":"response.created","response":{"usage":{"input_tokens":20097,"input_tokens_details":{"cached_tokens":0},"output_tokens":52,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":20149}},"sequence_number":55}'

    assert main._usage({"usage": main._usage_from_stream_chunk(chunk)}, estimate=False) == {
        "prompt_tokens": 20097,
        "completion_tokens": 52,
        "total_tokens": 20149,
        "cached_tokens": 0,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
    }


def test_responses_stream_extracts_usage_from_completed_response_event():
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="responses",
        base_url="https://example.invalid",
    )

    payload = {
        "type": "response.completed",
        "response": {
            "id": "resp_1",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 20,
                "input_tokens_details": {"cached_tokens": 30, "cache_creation_tokens": 4},
            },
        },
    }

    assert provider._extract_usage(payload) == payload["response"]["usage"]


def test_responses_stream_accepts_chat_completion_chunks_from_codex_simulation(monkeypatch):
    """Codex 模拟走 Responses 出站，但部分上游实际返回 chat.completion.chunk。"""
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="responses",
        base_url="https://example.invalid",
    )
    upstream_events = [
        'data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}\n\n',
        'data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"你好"},"finish_reason":null}]}\n\n',
        'data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"，我是 Codex"},"finish_reason":null}]}\n\n',
        'data: {"id":"resp_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}\n\n',
        'data: {"id":"resp_1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":59,"completion_tokens":425,"total_tokens":484}}\n\n',
        'data: [DONE]\n\n',
    ]

    async def fake_send_sse_request(*args, **kwargs):
        for event in upstream_events:
            yield event

    monkeypatch.setattr(provider, "send_sse_request", fake_send_sse_request)

    async def collect():
        return [chunk async for chunk in provider._responses_stream_chat("gpt-5.6-sol", [{"role": "user", "content": "hi"}])]

    chunks = asyncio.run(collect())

    assert chunks[0] == {}
    assert any(chunk.get("content") == "你好" for chunk in chunks if isinstance(chunk, dict))
    assert any(chunk.get("content") == "，我是 Codex" for chunk in chunks if isinstance(chunk, dict))
    assert chunks[-1]["usage"] == {"prompt_tokens": 59, "completion_tokens": 425, "total_tokens": 484}


def test_converted_responses_stream_ends_with_done_after_completed():
    async def source():
        yield 'data: {"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}\n\n'
        yield 'data: {"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
        yield 'data: {"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}\n\n'
        yield 'data: [DONE]\n\n'

    async def collect():
        return [chunk async for chunk in convert_stream_to_responses(source(), "m")]

    chunks = asyncio.run(collect())
    string_chunks = [chunk for chunk in chunks if isinstance(chunk, str)]

    assert string_chunks[-1] == "data: [DONE]\n\n"
    assert string_chunks[-2].startswith("event: response.completed\n")
    assert sum(1 for chunk in string_chunks if chunk == "data: [DONE]\n\n") == 1

    event_type, completed = parse_sse_event(string_chunks[-2])
    assert event_type == "response.completed"
    assert completed["type"] == "response.completed"
    response = completed["response"]
    assert response["status"] == "completed"
    assert response["output_text"] == "hi"
    assert response["usage"]["input_tokens"] == 7
    assert response["usage"]["output_tokens"] == 2
    assert response["usage"]["total_tokens"] == 9


def test_converted_responses_created_and_completed_wrap_response_payloads():
    async def source():
        yield 'data: {"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}\n\n'

    async def collect():
        return [chunk async for chunk in convert_stream_to_responses(source(), "m")]

    chunks = [chunk for chunk in asyncio.run(collect()) if isinstance(chunk, str)]
    created_type, created = parse_sse_event(chunks[0])
    completed_type, completed = parse_sse_event(chunks[-2])

    assert created_type == "response.created"
    assert created["type"] == "response.created"
    assert created["response"]["object"] == "response"
    assert completed_type == "response.completed"
    assert completed["type"] == "response.completed"
    assert completed["response"]["object"] == "response"


def test_responses_passthrough_stream_does_not_append_done(monkeypatch):
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="responses",
        base_url="https://example.invalid",
    )
    upstream_events = [
        'event: response.created\ndata: {"id":"resp_1","type":"response.created"}\n\n',
        'event: response.completed\ndata: {"id":"resp_1","type":"response.completed","usage":{"input_tokens":3,"output_tokens":1}}\n\n',
    ]

    async def fake_send_sse_request(*args, **kwargs):
        for event in upstream_events:
            yield event

    monkeypatch.setattr(provider, "send_sse_request", fake_send_sse_request)

    async def collect():
        return [chunk async for chunk in provider._responses_passthrough_stream("m", {"input": "hi"})]

    chunks = asyncio.run(collect())
    string_chunks = [chunk for chunk in chunks if isinstance(chunk, str)]

    assert string_chunks == upstream_events
    assert "data: [DONE]\n\n" not in string_chunks


def test_stream_attempt_log_response_body_is_client_stream_not_router_response():
    source = open("main.py", encoding="utf-8").read()
    body = source[source.index("async def _chat_with_retry"):source.index("@app.post(\"/v1/responses\")", source.index("async def _chat_with_retry"))]

    assert "attempt_client_response_body = _new_stream_log_body() if stream else None" in body
    assert "_append_stream_log_body(attempt_client_response_body, chunk)" in body
    assert "response_body=attempt_client_response_body or attempt_response_body or {}" in body
    assert "response_body=attempt_response_body or route_info.get(\"router_response_body\") or {}" not in body


def test_stream_attempt_keeps_provider_usage_chunk_over_empty_summary():
    source = open("main.py", encoding="utf-8").read()
    body = source[source.index("async def _chat_with_retry"):source.index("@app.post(\"/v1/responses\")", source.index("async def _chat_with_retry"))]

    assert 'finalize_usage_body = attempt_usage_body or (_stream_summary_to_openai_response(model, attempt_stream_summary) if attempt_stream_summary is not None else None)' in body
    assert 'summary_usage_body = _stream_summary_to_openai_response(model, attempt_stream_summary)' not in body
    assert 'usage=_usage(\n                    finalize_usage_body,' in body


def test_finalize_failed_attempt_does_not_store_response_body(monkeypatch):
    """失败 attempt 不保存合成/占位响应体：错误由 error 字段表达，
    避免 partial summary 合成的 chat.completion 把内部 scheme 模型名写进日志。"""
    captured = {}

    async def _fake_finalize_channel_attempt_log(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(main, "_finalize_channel_attempt_log", _fake_finalize_channel_attempt_log)

    route_info = {"provider": "p1", "account": "a1", "response_model": "opus"}
    error = main.IncompleteStreamError(main.UPSTREAM_ZERO_COMPLETION_MESSAGE, "upstream_incomplete_usage")
    # 有 partial summary（旧逻辑会据此合成 response_body 且模型名 = 内部派生名）
    attempt_stream_summary = {
        "content": "half",
        "reasoning_content": "",
        "tool_calls": [],
        "finish_reason": None,
        "usage": {},
    }

    asyncio.run(main._finalize_failed_attempt(
        error=error,
        route_info=route_info,
        provider_name="p1",
        username="a1",
        model="opus#scheme:4",
        retry_count=0,
        request_id="req_1",
        messages=[{"role": "user", "content": "hi"}],
        stream=True,
        api_key=None,
        api_key_name="",
        request_headers={},
        client_request_path="/v1/chat/completions",
        client_request_body=None,
        attempt_start=0.0,
        attempt_stream_summary=attempt_stream_summary,
        status="error",
    ))

    assert captured["response_body"] is None
    assert captured["success"] is False
    assert captured["error"] == main.UPSTREAM_ZERO_COMPLETION_MESSAGE
    # 内部派生 scheme 模型名不得出现在失败日志的任何持久化字段里
    assert "opus#scheme:4" not in json.dumps(captured.get("response_body"))


def test_channel_attempt_log_endpoint_uses_client_request_path():
    log_data = main._build_channel_attempt_log(
        request_id="req_1",
        route_info={"provider": "p1", "account": "a1", "routed_model": "m1", "attempt_key": "k1", "attempt_no": 1},
        model="m1",
        messages=[{"role": "user", "content": "hi"}],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        start_time=100.0,
        duration_ms=1,
        success=True,
        status="ok",
        response_body={},
        error="",
        client_request_path="/v1/chat/completions",
    )

    assert log_data["endpoint"] == "/v1/chat/completions"
    assert log_data["request_body"]["path"] == "/v1/chat/completions"


def test_query_request_logs_lists_all_rows_not_filtered_by_parent_log_id():
    source = open("db.py", encoding="utf-8").read()
    body = source[source.index("async def query_request_logs"):source.index("    @classmethod", source.index("async def query_request_logs") + 1)]

    assert "parent_log_id IS NULL" not in body
    assert "parent_request_id" not in body
    assert "retry_path" not in body
    assert "jsonb_array_length" not in body


def test_request_logs_schema_uses_attempt_identity_not_parent_log_id():
    source = open("db.py", encoding="utf-8").read()
    create_start = source.index("CREATE TABLE IF NOT EXISTS request_logs")
    create_end = source.index(")\n            \"\"\"", create_start)
    create_body = source[create_start:create_end]

    assert "attempt_key TEXT NOT NULL" in create_body
    assert "attempt_no INTEGER NOT NULL" in create_body
    assert "parent_log_id" not in create_body
    assert "parent_request_id" not in create_body
    assert "retry_path" not in create_body

    # 旧库升级：加 attempt_key/attempt_no 列、回填、建唯一索引，再删除 parent_log_id。
    assert "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS attempt_key TEXT" in source
    assert "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS attempt_no INTEGER" in source
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_request_logs_attempt_key ON request_logs(attempt_key)" in source
    assert "ALTER TABLE request_logs DROP COLUMN IF EXISTS parent_log_id" in source
    assert "ALTER TABLE request_logs DROP COLUMN IF EXISTS parent_request_id" in source
    assert "ALTER TABLE request_logs DROP COLUMN IF EXISTS retry_path" in source


def test_insert_request_log_writes_attempt_identity_columns():
    source = open("db.py", encoding="utf-8").read()
    body = source[source.index("async def insert_request_log"):source.index("    @classmethod", source.index("async def insert_request_log") + 1)]

    assert "attempt_key" in body
    assert "attempt_no" in body
    assert "upstream_returned_model" in body
    assert "ON CONFLICT (attempt_key) DO NOTHING" in body
    assert "parent_log_id" not in body
    assert "parent_request_id" not in body
    assert "retry_path" not in body


def test_request_log_schema_and_writer_include_router_request_headers():
    source = open("db.py", encoding="utf-8").read()
    insert_body = source[source.index("async def insert_request_log"):source.index("    @classmethod", source.index("async def insert_request_log") + 1)]
    update_body = source[source.index("async def update_request_log"):source.index("    @classmethod", source.index("async def update_request_log") + 1)]
    detail_body = source[source.index("async def get_request_log_detail"):source.index("    @classmethod\n    async def dashboard_stats", source.index("async def get_request_log_detail"))]

    assert "router_request_headers JSONB" in source
    assert "upstream_returned_model TEXT" in source
    assert "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS router_request_headers JSONB" in source
    assert "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_returned_model TEXT" in source
    assert "router_request_headers" in insert_body
    assert "router_request_headers" in update_body
    assert 'data["router_request_headers"] = cls._loads_json(data.get("router_request_headers"), {})' in detail_body
    assert 'item["router_request_headers"] = cls._loads_json(item.get("router_request_headers"), {})' in detail_body


def test_insert_request_log_serializes_structured_error(monkeypatch):
    import asyncio
    import json

    captured = {}

    class FakeAcquire:
        async def __aenter__(self):
            class FakeConn:
                async def fetchrow(self, query, *args):
                    captured["args"] = args
                    return {"id": 1}

            return FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(PostgresClient, "pool", FakePool())

    asyncio.run(PostgresClient.insert_request_log({
        "request_id": "req_structured_error",
        "error": {"message": "upstream returned HTML", "type": "invalid_response"},
        "upstream_status": {"status": 500},
    }))

    assert json.loads(captured["args"][-1]) == {"message": "upstream returned HTML", "type": "invalid_response"}
    assert json.loads(captured["args"][-3]) == {"status": 500}


def test_get_request_log_detail_loads_attempts_via_request_id():
    source = open("db.py", encoding="utf-8").read()
    body = source[source.index("async def get_request_log_detail"):source.index("    @classmethod\n    async def dashboard_stats", source.index("async def get_request_log_detail"))]

    assert "request_id=$1 AND id <> $2" in body
    assert "parent_log_id = $1" not in body
    assert "parent_request_id" not in body
    assert "WHERE request_id = ANY" not in body
    assert "retry_path" not in body
    assert "attempts" in body


def test_startup_hook_repairs_legacy_prompt_and_total_tokens():
    db_source = open("db.py", encoding="utf-8").read()
    start = db_source.index("async def _normalize_legacy_usage_tokens")
    end = db_source.index("    @classmethod", start + 1)
    body = db_source[start:end]
    assert "UPDATE request_logs" in body
    assert "prompt_tokens = prompt_tokens + cached_tokens + cache_creation_tokens" in body
    assert "total_tokens = prompt_tokens + cached_tokens + cache_creation_tokens + completion_tokens" in body
    assert "prompt_tokens <= cached_tokens + cache_creation_tokens" in body


def test_dashboard_stats_filters_to_finalized_channel_attempts():
    db_source = open("db.py", encoding="utf-8").read()
    start = db_source.index("async def dashboard_stats")
    end = db_source.index("    @classmethod", start + 1)
    body = db_source[start:end]
    assert "status IS NOT NULL AND status <> 'requesting'" in body
    assert "total_tokens > 0" not in body
    assert "parent_log_id IS NULL" not in body
    assert "parent_request_id" not in body
    assert "retry_path" not in body


def test_admin_probes_use_canonical_requesting_status():
    admin_source = open("admin.py", encoding="utf-8").read()
    fallback_start = admin_source.index("async def _run_fallback_probe")
    probe_start = admin_source.index("async def detect_provider_model")
    fallback_body = admin_source[fallback_start:probe_start]
    probe_body = admin_source[probe_start:]
    assert '"status": "requesting"' in fallback_body
    assert '"status": "running"' not in fallback_body
    assert '"status": "requesting"' in probe_body
    assert '"status": "running"' not in probe_body


def test_usage_endpoints_use_successful_positive_token_usage():
    db_source = open("db.py", encoding="utf-8").read()
    for method in ("billing_usage", "recent_logs", "total_successful_request_count", "token_stats", "account_daily_usage"):
        start = db_source.index(f"async def {method}")
        end = db_source.index("    @classmethod", start + 1)
        body = db_source[start:end]
        assert "total_tokens > 0" in body, f"{method} should filter billable usage by tokens"
        assert "success=true" in body, f"{method} should require successful usage rows"
        assert "parent_log_id" not in body, f"{method} should not reference parent_log_id"
        assert "parent_request_id" not in body
        assert "retry_path" not in body


def test_main_does_not_insert_separate_main_row_and_attempts_use_writer_queue():
    main_source = open("main.py", encoding="utf-8").read()

    assert "_channel_attempt_request_id" not in main_source
    assert "parent_log_id" not in main_source
    assert "parent_request_id" not in main_source
    assert "retry_path" not in main_source
    assert "_insert_main_request_log" not in main_source
    assert "main_log_id" not in main_source
    assert "first_attempt_log_id" not in main_source
    assert "attempt_parent_log_id" not in main_source
    assert "request_log_writer.enqueue_started" in main_source
    assert "request_log_writer.enqueue_finalized" in main_source


def test_admin_test_request_writes_parent_log_id_null():
    admin_source = open("admin.py", encoding="utf-8").read()
    test_body = admin_source[admin_source.index("async def test_provider_accounts"):admin_source.index("@router.", admin_source.index("async def test_provider_accounts"))]

    assert "parent_request_id" not in test_body
    assert "retry_path" not in test_body


def test_api_key_lookup_supports_non_ascii_input(monkeypatch):
    import config

    async def fake_list_api_keys():
        return [{"key": "测试使用", "disabled": False}]

    monkeypatch.setattr(config.PostgresClient, "list_api_keys", fake_list_api_keys)

    result = asyncio.run(config.Config.get_api_key_config("测试使用"))

    assert result["key"] == "测试使用"


def test_admin_test_account_error_detail_is_display_string():
    import admin

    assert admin._format_admin_error({
        "error": {
            "message": "Expecting value: line 1 column 1 (char 0)",
            "type": "invalid_response",
        }
    }) == "上游响应无法解析为 JSON：响应体为空或 SSE data 后没有有效 JSON（原始错误：Expecting value: line 1 column 1 (char 0)）"


def test_admin_test_failure_diagnostics_redact_sensitive_values():
    import admin

    diagnostics = admin._test_failure_diagnostics(
        {"body": {"messages": [{"content": "hello"}]}, "headers": {"Authorization": "Bearer client-secret"}},
        {
            "router_request_path": "/api/free-trial/chat",
            "router_request_body": {"messages": [], "token": "upstream-secret"},
            "router_response_body": ["data: error"],
            "upstream_status": "500",
        },
        "upstream error",
    )

    assert diagnostics["request_headers"]["Authorization"] == "Bearer clie...cret"
    assert diagnostics["router_request"]["token"] == "***"
    assert diagnostics["router_response"] == ["data: error"]
    assert diagnostics["router_request_path"] == "/api/free-trial/chat"


def test_admin_test_account_ui_formats_structured_error():
    source = open("static/admin.html", encoding="utf-8").read()
    body = source[source.index("async function runTestAndShowResult"):source.index("async function addCustomModel")]

    assert "formatLogError(result.error)" in body
    assert "statusCode + error" in body


def test_admin_test_account_overwrites_header_status_with_exception_status(monkeypatch):
    import asyncio
    import admin

    logs = []

    async def fake_require_admin(token):
        return None

    async def fake_insert_request_log(data):
        logs.append(data)
        return 1

    class FakeProvider:
        async def chat(self, model, messages, stream=False, **kwargs):
            kwargs["router_request_headers_callback"]({"Authorization": "Bearer sk-test", "x-app": "cli"})
            kwargs["response_headers_callback"]({":status": "200"})
            raise HTTPException(status_code=502, detail={"error": {"message": "upstream returned non JSON"}})
            yield

    account = type("Account", (), {"username": "u1", "provider": FakeProvider()})()
    pool = type("Pool", (), {"clients": [account]})()

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)
    monkeypatch.setattr(admin.PostgresClient, "insert_request_log", fake_insert_request_log)

    result = asyncio.run(admin.test_provider_accounts("zero_bug", {"username": "u1", "model": "m1", "test_type": "chat"}, token="t"))

    assert result["results"][0]["status_code"] == 502
    assert result["results"][0]["error"] == "upstream returned non JSON"
    assert logs[0]["response_headers"][":status"] == "200"
    assert logs[0]["router_request_headers"] == {"Authorization": "Bearer *******", "x-app": "cli"}
    assert logs[0]["upstream_status"] == "502"


def test_build_channel_attempt_log_uses_attempt_identity():
    route_info = {"provider": "p1", "account": "a1", "retry": 0, "attempt_key": "k_1", "attempt_no": 2}

    log = main._build_channel_attempt_log(
        request_id="req_parent",
        route_info=route_info,
        model="m1",
        messages=[],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        start_time=100.0,
        duration_ms=0,
        success=False,
        status="requesting",
        response_body=None,
        error="",
    )

    assert log["attempt_key"] == "k_1"
    assert log["attempt_no"] == 2
    assert log["request_id"] == "req_parent"
    assert log["status"] == "requesting"
    assert log["success"] is False
    assert log["duration_ms"] == 0
    assert "parent_log_id" not in log
    assert "retry_path" not in log
    assert "parent_request_id" not in log


def test_build_channel_attempt_log_carries_estimated_prompt_tokens():
    # 预测输入 token 由主链路算一次并塞进 route_info，日志构造须原样带出，跨候选/重试复用同一值。
    route_info = {"provider": "p1", "account": "a1", "attempt_key": "k_1", "attempt_no": 1, "estimated_prompt_tokens": 4321}

    log = main._build_channel_attempt_log(
        request_id="req_parent",
        route_info=route_info,
        model="m1",
        messages=[],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        start_time=100.0,
        duration_ms=0,
        success=False,
        status="requesting",
        response_body=None,
        error="",
    )

    assert log["estimated_prompt_tokens"] == 4321


def test_request_log_sql_and_params_include_estimated_prompt_tokens():
    # 源码切片断言：新增列必须同时出现在建表、INSERT 列/占位符、finalize UPDATE 与详情查询里。
    source = open("db.py", encoding="utf-8").read()

    create_start = source.index("CREATE TABLE IF NOT EXISTS request_logs")
    create_body = source[create_start:source.index(")\n            \"\"\"", create_start)]
    assert "estimated_prompt_tokens INTEGER NOT NULL DEFAULT 0" in create_body
    assert "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS estimated_prompt_tokens INTEGER NOT NULL DEFAULT 0" in source

    insert_start = source.index("_REQUEST_LOG_INSERT_SQL")
    insert_body = source[insert_start:source.index("ON CONFLICT (attempt_key) DO NOTHING", insert_start)]
    assert "estimated_prompt_tokens" in insert_body
    # 占位符数量与列数一致：44 列 → $44。
    assert "$44::jsonb" in insert_body

    finalize_start = source.index("async def finalize_request_log_batch")
    finalize_body = source[finalize_start:source.index("    @classmethod", finalize_start + 1)]
    assert "estimated_prompt_tokens=$27" in finalize_body

    detail_start = source.index("async def get_request_log_detail")
    detail_body = source[detail_start:source.index("    @classmethod", detail_start + 1)]
    assert "estimated_prompt_tokens" in detail_body


def test_build_channel_attempt_log_includes_router_request_headers():
    route_info = {"provider": "p1", "account": "a1", "router_request_headers": {"Authorization": "Bearer *******", "x-app": "cli"}, "attempt_key": "k_1", "attempt_no": 1}

    log = main._build_channel_attempt_log(
        request_id="req_parent",
        route_info=route_info,
        model="m1",
        messages=[],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        start_time=100.0,
        duration_ms=0,
        success=False,
        status="requesting",
        response_body=None,
        error="",
    )

    assert log["router_request_headers"] == {"Authorization": "Bearer *******", "x-app": "cli"}


def test_finalize_channel_attempt_log_enqueues_finalized_event(monkeypatch):
    import asyncio

    finalized = []

    def fake_enqueue_finalized(*, request_id, attempt_key, attempt_no, payload):
        finalized.append({"request_id": request_id, "attempt_key": attempt_key, "attempt_no": attempt_no, "payload": payload})

    monkeypatch.setattr(main.request_log_writer, "enqueue_finalized", fake_enqueue_finalized)

    route_info = {
        "provider": "p1",
        "account": "a1",
        "requested_model": "m1",
        "routed_model": "m1",
        "retry": 0,
        "attempt_key": "k_final",
        "attempt_no": 1,
    }

    asyncio.run(main._finalize_channel_attempt_log(
        request_id="req_parent",
        route_info=route_info,
        model="m1",
        messages=[{"role": "user", "content": "hi"}],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        attempt_start=100.0,
        success=True,
        status="ok",
        response_body={"id": "resp"},
        error="",
        usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3, "cached_tokens": 0, "cache_creation_tokens": 0},
    ))

    assert len(finalized) == 1
    event = finalized[0]
    assert event["request_id"] == "req_parent"
    assert event["attempt_key"] == "k_final"
    assert event["attempt_no"] == 1
    assert event["payload"]["status"] == "ok"
    assert event["payload"]["success"] is True
    assert event["payload"]["total_tokens"] == 3


def test_finalize_channel_attempt_log_carries_attempt_identity(monkeypatch):
    import asyncio

    finalized = []
    monkeypatch.setattr(
        main.request_log_writer,
        "enqueue_finalized",
        lambda *, request_id, attempt_key, attempt_no, payload: finalized.append(payload),
    )

    route_info = {
        "provider": "p1",
        "account": "a1",
        "requested_model": "m1",
        "routed_model": "m1",
        "retry": 1,
        "attempt_key": "k_err",
        "attempt_no": 2,
    }

    asyncio.run(main._finalize_channel_attempt_log(
        request_id="req_parent",
        route_info=route_info,
        model="m1",
        messages=[{"role": "user", "content": "hi"}],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        attempt_start=100.0,
        success=False,
        status="error",
        response_body={"error": main.UPSTREAM_ZERO_COMPLETION_MESSAGE},
        error=main.UPSTREAM_ZERO_COMPLETION_MESSAGE,
    ))

    assert len(finalized) == 1
    payload = finalized[0]
    assert payload["attempt_key"] == "k_err"
    assert payload["attempt_no"] == 2
    assert payload["status"] == "error"
    assert payload["error"] == main.UPSTREAM_ZERO_COMPLETION_MESSAGE


def test_finalize_channel_attempt_log_skips_without_provider(monkeypatch):
    import asyncio

    finalized = []
    monkeypatch.setattr(
        main.request_log_writer,
        "enqueue_finalized",
        lambda *, request_id, attempt_key, attempt_no, payload: finalized.append(payload),
    )

    asyncio.run(main._finalize_channel_attempt_log(
        request_id="req_parent",
        route_info={"retry": 0, "status": "error"},
        model="m1",
        messages=[],
        stream=False,
        api_key=None,
        api_key_name="",
        request_headers={},
        attempt_start=100.0,
        success=False,
        status="error",
        response_body={"error": "no provider"},
        error="no provider",
    ))

    assert finalized == []


def test_stream_error_chunk_normalizes_degraded_function_message():
    chunk = main._openai_stream_error_chunk(
        "Function id '3b9748d8-1d85-40e8-8573-0eeaa63a4b63': DEGRADED function cannot be invoked"
    )

    payload = json.loads(chunk.split("data:", 1)[1].strip())
    assert payload["error"]["message"] == "上游函数临时不可用，已按上游异常处理"
    assert "Function id" not in payload["error"]["message"]


def test_http_exception_handler_normalizes_openai_error_shape():
    class FakeURL:
        path = "/v1/chat/completions"

    class FakeRequest:
        url = FakeURL()

    exc = HTTPException(
        status_code=503,
        detail={
            "error": {
                "message": "Function id '3b9748d8-1d85-40e8-8573-0eeaa63a4b63': DEGRADED function cannot be invoked",
                "type": "server_error",
                "code": "upstream_error",
            }
        },
    )

    response = asyncio.run(main.openai_http_exception_handler(FakeRequest(), exc))
    payload = json.loads(response.body.decode("utf-8"))

    assert payload["error"]["message"] == "上游函数临时不可用，已按上游异常处理"
    assert "Function id" not in payload["error"]["message"]


def test_invalid_request_error_is_retryable_unless_configured():
    error = HTTPException(
        status_code=400,
        detail='{"error":{"message":"Unsupported parameter: metadata","type":"invalid_request_error","param":"metadata"}}',
    )

    assert not main._is_non_retryable_upstream_error(error)


def test_request_error_wrapping_invalid_request_is_retryable_unless_configured():
    error = HTTPException(
        status_code=400,
        detail={
            "error": {
                "message": "all 10 attempts failed: HTTP 400: {\"error\":{\"message\":\"Unsupported parameter: metadata\",\"type\":\"invalid_request_error\",\"param\":\"\",\"code\":null}}",
                "type": "request_error",
            }
        },
    )

    assert not main._is_non_retryable_upstream_error(error)


def test_configured_invalid_request_error_is_not_retryable(monkeypatch):
    monkeypatch.setattr(
        main.config.Config,
        "get_non_retryable_parameter_errors",
        classmethod(lambda cls: {"status_codes": [], "types": ["invalid_request_error"], "codes": [], "params": [], "markers": ["unsupported parameter"]}),
    )
    error = HTTPException(
        status_code=400,
        detail='{"error":{"message":"Unsupported parameter: metadata","type":"invalid_request_error","param":"metadata"}}',
    )

    assert main._is_non_retryable_upstream_error(error)


def test_server_error_wrapping_invalid_request_is_retryable():
    error = HTTPException(
        status_code=400,
        detail={
            "error": {
                "message": "auth_unavailable: no auth available (providers=1111111,codex, model=11111/gpt-5.5(medium))",
                "type": "server_error",
                "code": "internal_server_error",
            }
        },
    )

    assert not main._is_non_retryable_upstream_error(error)


def test_plain_unsupported_model_request_is_retryable():
    error = HTTPException(
        status_code=400,
        detail="The '11111/gpt-5.5' model is not supported when using Codex with a ChatGPT account.",
    )

    assert not main._is_non_retryable_upstream_error(error)


def test_context_too_large_is_not_retryable():
    error = HTTPException(
        status_code=400,
        detail={
            "error": {
                "message": "Your input exceeds the context window of this model. Please adjust your input and try again.",
                "type": "invalid_request_error",
                "code": "context_too_large",
            }
        },
    )

    assert main._is_non_retryable_upstream_error(error)


def test_vllm_invalid_value_for_function_is_not_retryable():
    error = HTTPException(
        status_code=400,
        detail={
            "error": {
                "message": "Invalid value for `function`: `None` in `tool_choice`! Correct usage: `{\"type\": \"function\", \"function\": {\"name\": \"my_function\"}}`",
                "type": "value_error",
            }
        },
    )
    assert main._is_non_retryable_upstream_error(error)


def test_upstream_auth_permission_and_not_found_are_retryable_unless_configured():
    for status_code in (401, 403, 404):
        error = HTTPException(status_code=status_code, detail="upstream deterministic error")

        assert not main._is_non_retryable_upstream_error(error)


def test_configured_parameter_status_code_is_not_retryable(monkeypatch):
    monkeypatch.setattr(
        main.config.Config,
        "get_non_retryable_parameter_errors",
        classmethod(lambda cls: {"status_codes": [400], "types": [], "codes": [], "params": [], "markers": []}),
    )
    error = HTTPException(
        status_code=400,
        detail={"error": {"message": "Bad value for metadata", "type": "invalid_request_error", "param": "metadata"}},
    )

    assert main._is_non_retryable_upstream_error(error)


def test_upstream_server_errors_are_retryable():
    for status_code in (500, 502, 503):
        error = HTTPException(status_code=status_code, detail="temporary upstream failure")

        assert not main._is_non_retryable_upstream_error(error)


def test_request_4xx_only_cools_when_freeze_policy_matches_but_rate_limit_and_server_errors_do(monkeypatch):
    class Client:
        username = "acct"

        def __init__(self, status_code):
            self.status_code = status_code

        async def chat(self, *args, **kwargs):
            if self.status_code is None:
                yield {"choices": [{"message": {"content": "ok"}}]}
                return
            raise HTTPException(status_code=self.status_code, detail="upstream rejected request")
            yield

        chat_anthropic = chat

    class Account:
        def __init__(self):
            self.last_route_info = {
                "provider": "p",
                "account": "acct",
                "routed_model": "m",
            }

        async def release(self):
            return None

    class Pool:
        def __init__(self):
            self.cooldown_until = 0
            self.calls = []
            self.policy_calls = []
            self.freeze_status_codes = set()

        def apply_freeze_policy(self, username, status_code=None, headers=None, model_id=None, reason=None, exception=False, error_code=None, body=None):
            self.policy_calls.append((username, status_code, reason, model_id))
            if status_code in self.freeze_status_codes:
                self.cooldown_until = 1
                return "freeze"
            return ""

        def mark_account_exception(self, username, cooldown_seconds=None, reason="", error_code=None, model_id=None, body=None):
            self.policy_calls.append((username, None, reason, model_id))
            return ""

        def handle_response_error(self, username, status_code, detail, model_id=None):
            self.calls.append((username, status_code, detail))
            self.cooldown_until = 1
            return "cooldown"

    current_status = 400
    pool = Pool()

    async def acquire(**kwargs):
        return (Client(current_status), "p", Account()), {}

    async def false_group(cls, model, snapshot=None):
        return False

    async def rate_codes(provider):
        return []

    async def finalize(**kwargs):
        return None

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)
    monkeypatch.setattr(main, "_finalize_channel_attempt_log", finalize)
    monkeypatch.setattr(main, "_is_non_retryable_upstream_error", lambda error: False)
    monkeypatch.setattr(main.config.Config, "system_debug_enabled", classmethod(lambda cls: False))
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kwargs: None)
    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(false_group))
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: 0)
    monkeypatch.setattr(main.config.Config, "get_global_retry_count", lambda: 0)
    monkeypatch.setattr(main.config.Config, "get_provider_extra_retry_status_codes", lambda provider: set())
    monkeypatch.setattr(main.config.Config, "stream_incomplete_error_enabled", lambda: False)
    monkeypatch.setattr(main.ModelClientPool, "get_model_providers", lambda model: ["p"])
    monkeypatch.setattr(main.ModelClientPool, "get_provider_pool", lambda provider: pool)
    monkeypatch.setattr(main.ModelClientPool, "resolve_upstream_id", lambda provider, model: model)
    monkeypatch.setattr(main.ModelClientPool, "record_channel_failure", lambda *args, **kwargs: None)
    account_failures = []
    monkeypatch.setattr(
        main.ModelClientPool,
        "record_account_failure",
        lambda *args, **kwargs: account_failures.append((args, kwargs)),
    )

    async def invoke(status_code):
        nonlocal current_status
        current_status = status_code
        try:
            async for _ in main._chat_with_retry_for_model("m", [], False):
                pass
        except HTTPException:
            pass

    asyncio.run(invoke(400))
    assert pool.calls == []
    assert pool.cooldown_until == 0
    assert account_failures == []
    assert [call[1] for call in pool.policy_calls] == [400]

    # 显式 freeze_policy 命中的 403 虽然不可重试，也必须冻结（这正是“403 当天不可用”要的）。
    pool.freeze_status_codes = {403}
    asyncio.run(invoke(403))
    assert pool.calls == []
    assert pool.cooldown_until == 1
    assert [call[1] for call in pool.policy_calls] == [400, 403]

    # A new Anthropic request can immediately select and use the same primary account.
    current_status = None

    async def collect_anthropic():
        return [chunk async for chunk in main._chat_with_retry_for_model(
            "m", [], False, chat_method="chat_anthropic"
        )]

    chunks = asyncio.run(collect_anthropic())
    assert chunks[0]["choices"][0]["message"]["content"] == "ok"
    assert pool.calls == []

    asyncio.run(invoke(429))
    asyncio.run(invoke(503))
    assert [call[1] for call in pool.calls] == [429, 503]
    assert len(account_failures) == 2
    assert pool.cooldown_until == 1


def test_responses_route_error_uses_openai_not_found_shape_for_unknown_model(monkeypatch):
    async def false_async(*args, **kwargs):
        return False

    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(false_async))
    monkeypatch.setattr(main.ModelClientPool, "get_model_routes", lambda model: [])
    monkeypatch.setattr(main.ModelClientPool, "get_model_info", lambda model: None)
    monkeypatch.setattr(main.model_metadata, "has_explicit_metadata", false_async)
    monkeypatch.setattr(main.config.Config, "system_debug_enabled", classmethod(lambda cls: False))

    try:
        asyncio.run(main._raise_responses_route_error("gpt-missing", "sk", "codex", "api_key_not_allowed_for_model"))
    except HTTPException as exc:
        assert exc.status_code == 404
        assert exc.detail == {
            "error": {
                "message": "The model 'gpt-missing' does not exist.",
                "type": "not_found_error",
                "code": "model_not_found",
                "param": "model",
            }
        }
    else:
        raise AssertionError("expected HTTPException")


def test_responses_route_error_uses_openai_permission_shape_for_denied_model(monkeypatch):
    async def false_async(*args, **kwargs):
        return False

    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(false_async))
    monkeypatch.setattr(main.ModelClientPool, "get_model_routes", lambda model: [{"provider": "chat"}])
    monkeypatch.setattr(main.ModelClientPool, "get_model_info", lambda model: None)
    monkeypatch.setattr(main.model_metadata, "has_explicit_metadata", false_async)
    monkeypatch.setattr(main.config.Config, "system_debug_enabled", classmethod(lambda cls: False))

    try:
        asyncio.run(main._raise_responses_route_error("gpt-denied", "sk", "codex", "api_key_not_allowed_for_model"))
    except HTTPException as exc:
        assert exc.status_code == 403
        assert exc.detail["error"]["message"] == "You do not have access to model 'gpt-denied'."
        assert exc.detail["error"]["type"] == "permission_error"
        assert exc.detail["error"]["code"] == "model_access_denied"
        assert exc.detail["error"]["param"] == "model"
    else:
        raise AssertionError("expected HTTPException")


def test_responses_route_error_treats_metadata_only_model_as_access_denied(monkeypatch):
    async def false_async(*args, **kwargs):
        return False

    async def true_async(*args, **kwargs):
        return True

    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(false_async))
    monkeypatch.setattr(main.ModelClientPool, "get_model_routes", lambda model: [])
    monkeypatch.setattr(main.ModelClientPool, "get_model_info", lambda model: None)
    monkeypatch.setattr(main.model_metadata, "has_explicit_metadata", true_async)
    monkeypatch.setattr(main.config.Config, "system_debug_enabled", classmethod(lambda cls: False))

    try:
        asyncio.run(main._raise_responses_route_error("gpt-metadata-only", "sk", "codex", "api_key_not_allowed_for_model"))
    except HTTPException as exc:
        assert exc.status_code == 403
        assert exc.detail == {
            "error": {
                "message": "You do not have access to model 'gpt-metadata-only'.",
                "type": "permission_error",
                "code": "model_access_denied",
                "param": "model",
            }
        }
    else:
        raise AssertionError("expected HTTPException")


def test_http_exception_handler_maps_status_to_protocol_error_type():
    import types

    async def check(path, status, expected_type):
        req = types.SimpleNamespace(url=types.SimpleNamespace(path=path))
        resp = await main.openai_http_exception_handler(req, HTTPException(status_code=status, detail="boom"))
        body = json.loads(resp.body.decode("utf-8"))
        return body["error"]["type"]

    assert asyncio.run(check("/v1/responses", 404, "not_found_error")) == "not_found_error"
    assert asyncio.run(check("/v1/responses", 403, "permission_error")) == "permission_error"


def test_openai_to_responses_conversion_preserves_metadata():
    payload = openai_messages_to_responses_payload(
        "test-model",
        [{"role": "user", "content": "hi"}],
        False,
        {"metadata": {"trace_id": "abc"}, "max_tokens": 8},
    )

    assert payload["metadata"] == {"trace_id": "abc"}


def test_provider_retry_count_uses_explicit_channel_value(monkeypatch):
    monkeypatch.setattr(
        main.config.Config,
        "get_providers_snapshot",
        classmethod(lambda cls: {"p1": {"retry_count": 9}}),
    )
    monkeypatch.setattr(
        main.config.Config,
        "_load",
        classmethod(lambda cls: {"retry": {"max_retries": 2}}),
    )

    assert main.config.Config.get_provider_retry_count("p1") == 9


def test_provider_retry_count_defaults_to_zero_when_unset(monkeypatch):
    monkeypatch.setattr(
        main.config.Config,
        "get_providers_snapshot",
        classmethod(lambda cls: {"p1": {}}),
    )
    monkeypatch.setattr(
        main.config.Config,
        "_load",
        classmethod(lambda cls: {"retry": {"max_retries": 2}}),
    )

    assert main.config.Config.get_provider_retry_count("p1") == 0
    assert main.config.Config.get_global_retry_count() == 2


def test_global_retry_count_defaults_to_three_when_missing(monkeypatch):
    monkeypatch.setattr(
        main.config.Config,
        "get_providers_snapshot",
        classmethod(lambda cls: {"p1": {}}),
    )
    monkeypatch.setattr(main.config.Config, "_load", classmethod(lambda cls: {}))

    assert main.config.Config.get_provider_retry_count("p1") == 0
    assert main.config.Config.get_global_retry_count() == 3


def test_get_request_log_detail_returns_attempts_list(monkeypatch):
    import asyncio

    class FakeAcquire:
        async def __aenter__(self):
            class FakeConn:
                async def fetchrow(self, *args):
                    return {
                        "id": 1,
                        "request_id": "req_group",
                        "created_at": 0,
                        "time": 0,
                        "attempt_key": "k_1",
                        "attempt_no": 1,
                        "model": "m1",
                        "duration_ms": 120,
                    }

                async def fetch(self, query, *args):
                    # 安全事件查询按 log_id 传入，attempt 查询按 request_id + log_id 传入。
                    if "security_events" in query:
                        return []
                    # 流程聚合按 request_id 关联，不再依赖 parent_log_id。
                    assert "request_id=$1 AND id <> $2" in query
                    assert "parent_log_id" not in query
                    assert args[0] == "req_group"
                    assert args[1] == 1
                    return [
                        {"id": 9, "request_id": "req_group", "attempt_key": "k_2",
                         "attempt_no": 2, "model": "m1", "time": 1.0, "duration_ms": 80,
                         "source": "live"},
                    ]

            return FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(PostgresClient, "pool", FakePool())

    detail = asyncio.run(PostgresClient.get_request_log_detail(1))

    assert len(detail["attempts"]) == 1
    attempt = detail["attempts"][0]
    assert attempt["id"] == 9
    assert attempt["request_id"] == "req_group"
    assert attempt["attempt_no"] == 2
    assert attempt["model"] == "m1"
    assert attempt["time"] == 1.0
    assert detail["requested_model"] == "m1"
    assert detail["actual_model"] == "m1"
    assert detail["total_duration_ms"] == 200
    assert detail["actual_attempt_count"] == 2
