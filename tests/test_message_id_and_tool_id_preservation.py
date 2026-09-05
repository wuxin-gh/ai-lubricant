"""单测：跨协议转换保留上游 message_id / response_id，且不丢失 tool 调用 id。

覆盖三处改动：
- convert_stream_to_anthropic：惰性发出 message_start，取上游首帧 id
- openai_to_anthropic_response：保留上游 id + sanitize tool_use id
- convert_stream_to_responses：保留上游 id + 流式补全 function_call 项（之前完全丢失）
"""
import asyncio
import json

from message_utils import (
    convert_stream_to_anthropic,
    convert_stream_to_responses,
    openai_to_anthropic_response,
)


def _sse(obj: dict) -> str:
    return "data: " + json.dumps(obj, ensure_ascii=False) + "\n\n"


def _parse_events(events: list[str]) -> list[dict]:
    """从 SSE 文本列表里抽出每个事件的 data JSON。"""
    out = []
    for ev in events:
        for line in ev.splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
                if not raw or raw == "[DONE]":
                    continue
                try:
                    out.append(json.loads(raw))
                except json.JSONDecodeError:
                    pass
    return out


# ---------------------------------------------------------------------------
# convert_stream_to_anthropic: 保留上游 id
# ---------------------------------------------------------------------------

def test_convert_stream_to_anthropic_preserves_upstream_message_id():
    async def gen():
        yield _sse({"id": "chatcmpl-abc123", "object": "chat.completion.chunk", "model": "m",
                    "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}]})
        yield _sse({"id": "chatcmpl-abc123", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in convert_stream_to_anthropic(gen(), "m"):
            events.append(ev)

    asyncio.run(collect())

    types = [line for ev in events for line in ev.splitlines() if line.startswith("event:")]
    assert types[0] == "event: message_start", types[:3]

    start = next(d for d in _parse_events(events) if d.get("type") == "message_start")
    assert start["message"]["id"] == "chatcmpl-abc123", start["message"]["id"]


def test_convert_stream_to_anthropic_falls_back_when_no_upstream_id():
    async def gen():
        # 没有 id 字段
        yield _sse({"choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}]})
        yield _sse({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in convert_stream_to_anthropic(gen(), "m"):
            events.append(ev)

    asyncio.run(collect())

    start = next(d for d in _parse_events(events) if d.get("type") == "message_start")
    assert start["message"]["id"].startswith("msg_"), start["message"]["id"]


def test_convert_stream_to_anthropic_preserves_tool_use_id():
    async def gen():
        yield _sse({"id": "chatcmpl-t1", "choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 0, "id": "toolu_abc", "type": "function",
                            "function": {"name": "get_weather", "arguments": "{\"city\":\"x\"}"}}]
        }, "finish_reason": None}]})
        yield _sse({"id": "chatcmpl-t1", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in convert_stream_to_anthropic(gen(), "m"):
            events.append(ev)

    asyncio.run(collect())

    block_starts = [d for d in _parse_events(events) if d.get("type") == "content_block_start"]
    tool_starts = [d for d in block_starts if d.get("content_block", {}).get("type") == "tool_use"]
    assert tool_starts, block_starts
    assert tool_starts[0]["content_block"]["id"] == "toolu_abc"


def test_merge_tool_calls_repeated_name_not_concatenated():
    """部分上游（glm-5.2 等）在每个 arguments 分片里都重复带 function.name。

    历史上 _merge_tool_calls 对 name 也用 += 累加，会把 "Grep" 拼成 "GrepGrepGrep..."。
    name 与 id 一样是完整值，应覆盖；只有 arguments 是分片增量，才累加。
    """
    from message_utils import _merge_tool_calls

    fragments = [
        {"index": 0, "id": "chatcmpl-tool-x", "type": "function",
         "function": {"name": "Grep", "arguments": '{"-n": '}},
        {"index": 0, "id": "chatcmpl-tool-x", "type": "function",
         "function": {"name": "Grep", "arguments": 'true, "output_mode": "'}},
        {"index": 0, "id": "chatcmpl-tool-x", "type": "function",
         "function": {"name": "Grep", "arguments": 'content'}},
        {"index": 0, "id": "chatcmpl-tool-x", "type": "function",
         "function": {"name": "Grep", "arguments": '", "pattern": "q'}},
        {"index": 0, "id": "chatcmpl-tool-x", "type": "function",
         "function": {"name": "Grep", "arguments": '|t|'}},
        {"index": 0, "id": "chatcmpl-tool-x", "type": "function",
         "function": {"name": "Grep", "arguments": 't_id"}'}},
    ]

    merged = _merge_tool_calls(fragments)
    assert len(merged) == 1
    fn = merged[0]["function"]
    # name 只覆盖不累加
    assert fn["name"] == "Grep", fn["name"]
    # arguments 分片正确拼接成合法 JSON
    assert fn["arguments"] == '{"-n": true, "output_mode": "content", "pattern": "q|t|t_id"}', fn["arguments"]
    assert merged[0]["id"] == "chatcmpl-tool-x"


def test_convert_stream_to_anthropic_repeated_tool_name_stays_single():
    """端到端：glm-5.2 风格的 tool_calls 多分片（每片重复带 name）流式转换后，
    tool_use 块的 name 必须是单个 "Grep"，不能拼接。"""
    async def gen():
        yield _sse({"id": "chatcmpl-t2", "choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 0, "id": "call_1", "type": "function",
                            "function": {"name": "Grep", "arguments": '{"-n": '}}]
        }, "finish_reason": None}]})
        yield _sse({"id": "chatcmpl-t2", "choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 0, "type": "function",
                            "function": {"name": "Grep", "arguments": 'true}'}}]
        }, "finish_reason": None}]})
        yield _sse({"id": "chatcmpl-t2", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []
    async def collect():
        async for ev in convert_stream_to_anthropic(gen(), "m"):
            events.append(ev)
    asyncio.run(collect())

    tool_starts = [d for d in _parse_events(events)
                   if d.get("type") == "content_block_start"
                   and d.get("content_block", {}).get("type") == "tool_use"]
    assert tool_starts, events
    assert tool_starts[0]["content_block"]["name"] == "Grep", tool_starts[0]["content_block"]


# ---------------------------------------------------------------------------
# openai_to_anthropic_response: 保留上游 id + sanitize tool_use id
# ---------------------------------------------------------------------------

def test_openai_to_anthropic_response_preserves_upstream_id():
    resp = openai_to_anthropic_response(
        {"id": "chatcmpl-XYZ", "choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}]},
        "m",
    )
    assert resp["id"] == "chatcmpl-XYZ", resp["id"]


def test_openai_to_anthropic_response_falls_back_when_no_upstream_id():
    resp = openai_to_anthropic_response(
        {"choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}]}, "m"
    )
    assert resp["id"].startswith("msg_"), resp["id"]


def test_openai_to_anthropic_response_sanitizes_tool_use_id():
    """含非法字符的 tool_call id 必须被 sanitize，不能原样透传给 Anthropic 客户端。"""
    resp = openai_to_anthropic_response(
        {"id": "chatcmpl-T", "choices": [{"message": {
            "content": "",
            "tool_calls": [{"id": "functions.todowrite:3", "type": "function",
                            "function": {"name": "todo_write", "arguments": "{}"}}],
        }, "finish_reason": "tool_calls"}]},
        "m",
    )
    tool_use = next(b for b in resp["content"] if b.get("type") == "tool_use")
    sid = tool_use["id"]
    # 不允许 . : 等非法字符
    assert "." not in sid and ":" not in sid, sid
    # 合法字符集
    import re
    assert re.match(r"^[a-zA-Z0-9_-]+$", sid), sid
    # 配对性：sanitize 是确定性的，functions.todowrite:3 -> functions_todowrite_3
    assert sid == "functions_todowrite_3", sid


# ---------------------------------------------------------------------------
# convert_stream_to_responses: 保留上游 id + 补全 function_call
# ---------------------------------------------------------------------------

def test_convert_stream_to_responses_preserves_upstream_response_id():
    async def gen():
        yield _sse({"id": "resp_upstream_1", "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}]})
        yield _sse({"id": "resp_upstream_1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in convert_stream_to_responses(gen(), "m"):
            events.append(ev)

    asyncio.run(collect())

    created = next(d for d in _parse_events(events) if d.get("type") == "response.created")
    assert created["response"]["id"] == "resp_upstream_1", created["response"]["id"]
    completed = next(d for d in _parse_events(events) if d.get("type") == "response.completed")
    assert completed["response"]["id"] == "resp_upstream_1"


def test_convert_stream_to_responses_emits_function_call_items():
    """之前流式跨协议完全丢失 tool_calls，这里断言 function_call 项被补全下发。"""
    async def gen():
        yield _sse({"id": "chatcmpl-fc", "choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 0, "id": "call_42", "type": "function",
                            "function": {"name": "search", "arguments": "{\"q\":\"x\"}"}}]
        }, "finish_reason": None}]})
        yield _sse({"id": "chatcmpl-fc", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in convert_stream_to_responses(gen(), "m"):
            events.append(ev)

    asyncio.run(collect())

    types = [d.get("type") for d in _parse_events(events)]
    assert "response.function_call_arguments.delta" in types, types
    assert "response.function_call_arguments.done" in types, types

    completed = next(d for d in _parse_events(events) if d.get("type") == "response.completed")
    output = completed["response"]["output"]
    fc_items = [it for it in output if it.get("type") == "function_call"]
    assert len(fc_items) == 1, output
    fc = fc_items[0]
    assert fc["call_id"] == "call_42", fc
    assert fc["name"] == "search", fc
    assert fc["arguments"] == '{"q":"x"}', fc
    assert fc["status"] == "completed", fc


def test_convert_stream_to_responses_falls_back_when_no_upstream_id():
    async def gen():
        yield _sse({"choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}]})
        yield _sse({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
        yield "data: [DONE]\n\n"

    events: list[str] = []

    async def collect():
        async for ev in convert_stream_to_responses(gen(), "m"):
            events.append(ev)

    asyncio.run(collect())

    created = next(d for d in _parse_events(events) if d.get("type") == "response.created")
    assert created["response"]["id"].startswith("resp_"), created["response"]["id"]
