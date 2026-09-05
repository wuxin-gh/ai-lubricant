import os
import re
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.agent_loop import BaseHandler, StepOutcome, agent_runner_loop


def _response(content="", tool_calls=None, usage=None, finish_reason=None, reasoning=""):
    return SimpleNamespace(
        content=content,
        tool_calls=tool_calls or [],
        usage=usage,
        finish_reason=finish_reason,
        reasoning=reasoning,
    )


def _stream_client(*responses):
    """构造一个以 stream_chat 逐个 yield response 的假 client（agent_loop 现走流式）。"""
    responses = list(responses)
    index = 0

    class _Client:
        pass

    client = _Client()
    # 便于旧断言读取调用参数：记录最后一次 messages/tools
    client.calls = []

    async def _stream(messages, tools=None):
        nonlocal index
        client.calls.append({"messages": messages, "tools": tools})
        if index >= len(responses):
            return
        r = responses[index]
        index += 1
        yield r

    client.stream_chat = _stream
    return client


def _tool_call(name="lookup", arguments='{"q":"x"}'):
    return {
        "id": "call_1",
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


async def _collect_loop(*args, **kwargs):
    return [item async for item in agent_runner_loop(*args, **kwargs)]


class ScriptedHandler(BaseHandler):
    def __init__(self, outcomes):
        super().__init__(tools_registry=AsyncMock())
        self.outcomes = list(outcomes)
        self.calls = []

    async def dispatch(self, tool_name, args, response, index=0, tool_num=1):
        self.calls.append((tool_name, args, response, index, tool_num))
        return self.outcomes.pop(0)


@pytest.mark.asyncio
async def test_agent_loop_no_tool_terminates():
    client = _stream_client(_response("done"))
    handler = ScriptedHandler([])

    outputs = await _collect_loop(
        client,
        "system",
        "do work",
        handler,
        tools_schema=[],
        verbose=False,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "done"}
    assert client.calls[0]["messages"] == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "do work"},
    ]
    assert client.calls[0]["tools"] == []
    assert handler.calls == []


@pytest.mark.asyncio
async def test_agent_loop_single_tool_then_done():
    client = _stream_client(
        _response("calling tool", [_tool_call("lookup", '{"q":"x"}')]),
        _response("final answer"),
    )
    handler = ScriptedHandler([
        StepOutcome(data={"answer": 42}, next_prompt="continue after lookup"),
    ])

    outputs = await _collect_loop(
        client,
        "system",
        "question",
        handler,
        tools_schema=[{"type": "function", "function": {"name": "lookup"}}],
        verbose=False,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "final answer"}
    assert handler.calls[0][0] == "lookup"
    assert handler.calls[0][1] == {"q": "x"}
    assert handler.calls[0][3:] == (0, 1)
    # 工具结果走标准 role=tool 消息，不再拼进 user content。
    second_messages = client.calls[1]["messages"]
    assistant_msg = second_messages[-3]
    tool_msg = second_messages[-2]
    user_msg = second_messages[-1]
    assert assistant_msg["role"] == "assistant"
    # The id is minted by the agent, not taken from the upstream ("call_1" here),
    # so history stays protocol-neutral across per-turn channel routing. What has
    # to hold is that the role=tool reply carries the id of the call it answers.
    minted_id = assistant_msg["tool_calls"][0]["id"]
    assert re.fullmatch(r"call_agent_[0-9a-f]{8}_\d+_0", minted_id)
    assert tool_msg["role"] == "tool"
    assert tool_msg["tool_call_id"] == minted_id
    assert tool_msg["name"] == "lookup"
    assert '"answer": 42' in tool_msg["content"]
    assert user_msg["role"] == "user"
    assert user_msg["content"].startswith("continue after lookup")
    assert "Tool results:" not in user_msg["content"]
    assert "answer" not in user_msg["content"]


@pytest.mark.asyncio
async def test_agent_loop_tool_without_next_prompt_continues_with_result():
    client = _stream_client(
        _response("calling tool", [_tool_call("lookup", '{"q":"x"}')]),
        _response("final answer"),
    )
    handler = ScriptedHandler([
        StepOutcome(data={"answer": 42}),
    ])

    outputs = await _collect_loop(
        client,
        "system",
        "question",
        handler,
        tools_schema=[{"type": "function", "function": {"name": "lookup"}}],
        verbose=False,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "final answer"}
    assert len(client.calls) == 2
    second_messages = client.calls[1]["messages"]
    tool_msg = second_messages[-2]
    user_msg = second_messages[-1]
    assert tool_msg["role"] == "tool"
    assert '"answer": 42' in tool_msg["content"]
    assert user_msg["role"] == "user"
    assert "如果任务已经完成，请直接给出最终回答" in user_msg["content"]
    assert "answer" not in user_msg["content"]


@pytest.mark.asyncio
async def test_agent_loop_generates_and_correlates_missing_tool_call_ids():
    first = _tool_call("first")
    second = _tool_call("second")
    first.pop("id")
    second.pop("id")
    client = _stream_client(
        _response("", [first, second]),
        _response("done"),
    )
    handler = ScriptedHandler([
        StepOutcome(data={"value": 1}),
        StepOutcome(data={"value": 2}),
    ])

    outputs = await _collect_loop(
        client,
        "system",
        "question",
        handler,
        tools_schema=[],
        verbose=False,
    )

    assert outputs[-1]["data"] == "done"
    second_messages = client.calls[1]["messages"]
    assistant_msg = second_messages[-4]
    tool_messages = second_messages[-3:-1]
    call_ids = [call["id"] for call in assistant_msg["tool_calls"]]
    # Ids are minted by the agent and scoped to the run, so the run tag is not
    # predictable — what must hold is that they are ours, distinct per call, and
    # that each role=tool reply carries the id of the call it answers.
    assert all(re.fullmatch(r"call_agent_[0-9a-f]{8}_1_\d+", cid) for cid in call_ids)
    assert len(set(call_ids)) == len(call_ids)
    assert [msg["tool_call_id"] for msg in tool_messages] == call_ids
    assert [msg["name"] for msg in tool_messages] == ["first", "second"]
    assert all(msg["role"] == "tool" for msg in tool_messages)
    assert second_messages[-1]["role"] == "user"
    assert "value" not in second_messages[-1]["content"]


@pytest.mark.asyncio
async def test_agent_loop_max_turns_exceeded():
    client = _stream_client(
        _response("tool", [_tool_call()]),
        _response("tool", [_tool_call()]),
    )
    handler = ScriptedHandler([
        StepOutcome(data={"n": 1}, next_prompt="again"),
        StepOutcome(data={"n": 2}, next_prompt="again"),
    ])

    outputs = await _collect_loop(
        client,
        "system",
        "start",
        handler,
        tools_schema=[],
        max_turns=2,
        verbose=False,
    )

    assert outputs[-1] == {"result": "MAX_TURNS_EXCEEDED", "data": {"n": 2}}
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_agent_loop_exits_on_should_exit():
    client = _stream_client(_response("tool", [_tool_call()]))
    handler = ScriptedHandler([
        StepOutcome(data={"status": "stopped"}, next_prompt="ignored", should_exit=True),
    ])

    outputs = await _collect_loop(
        client,
        "system",
        "start",
        handler,
        tools_schema=[],
        max_turns=5,
        verbose=False,
    )

    assert outputs[-1] == {"result": "EXITED", "data": {"status": "stopped"}}
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_agent_loop_yields_turn_markers():
    client = _stream_client(
        _response("tool", [_tool_call()]),
        _response("done"),
    )
    handler = ScriptedHandler([
        StepOutcome(data="tool data", next_prompt="next"),
    ])

    outputs = await _collect_loop(
        client,
        "system",
        "start",
        handler,
        tools_schema=[],
        yield_info=True,
        verbose=False,
    )

    assert outputs[0] == {"turn": 1}
    assert outputs[1] == {"turn": 2}
    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "done"}


@pytest.mark.asyncio
async def test_agent_loop_done_event_carries_last_turn_usage():
    """流式：done 事件只带最后一轮的 usage，不跨轮累加。

    prompt_tokens 是本轮重发的整个上下文大小（存量），不是增量流量；累加它会让
    徽章在 N 轮后显示约 N 倍真实占用，所以 done 只回最后一轮的快照。
    """
    client = _stream_client(
        _response("first", [_tool_call("lookup", '{"q":"x"}')], usage={"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}),
        _response("final answer", usage={"prompt_tokens": 20, "completion_tokens": 8, "total_tokens": 28}),
    )
    handler = ScriptedHandler([StepOutcome(data={"answer": 42})])
    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    outputs = await _collect_loop(
        client,
        "system",
        "question",
        handler,
        tools_schema=[{"type": "function", "function": {"name": "lookup"}}],
        verbose=False,
        on_event=on_event,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "final answer"}
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(done_events) == 1
    usage = done_events[0].get("usage") or {}
    assert usage.get("prompt_tokens") == 20  # 最后一轮，不是 10+20
    assert usage.get("completion_tokens") == 8  # 最后一轮，不是 5+8
    assert usage.get("total_tokens") == 28  # 最后一轮，不是 15+28
    # done 不再回传正文
    assert "data" not in done_events[0]


@pytest.mark.asyncio
async def test_agent_loop_content_event_streams_deltas():
    """流式：content 事件逐 delta 推送，不再一次性带全文。"""
    chunks = [
        _response("Hel"),
        _response("lo"),
        _response(" world"),
    ]

    class _MultiChunkClient:
        async def stream_chat(self, messages, tools=None):
            for c in chunks:
                yield c

    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    await _collect_loop(
        _MultiChunkClient(),
        "system",
        "hi",
        ScriptedHandler([]),
        tools_schema=[],
        verbose=False,
        on_event=on_event,
    )

    content_events = [e for e in events if e.get("type") == "content"]
    assert "".join(e["text"] for e in content_events) == "Hello world"


@pytest.mark.asyncio
async def test_agent_loop_streams_reasoning_separately_from_content():
    """思考增量逐段发 reasoning 事件，且不混进 content / assistant content。"""
    chunks = [
        _response(reasoning="思考开头"),
        _response(reasoning="思考尾"),
        _response(content="最终回答"),
    ]

    class _Client:
        async def stream_chat(self, messages, tools=None):
            for c in chunks:
                yield c

    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    outputs = await _collect_loop(
        _Client(),
        "system",
        "hi",
        ScriptedHandler([]),
        tools_schema=[],
        verbose=False,
        on_event=on_event,
    )
    reasoning = "".join(e["text"] for e in events if e.get("type") == "reasoning")
    content = "".join(e["text"] for e in events if e.get("type") == "content")
    assert reasoning == "思考开头思考尾"
    assert content == "最终回答"
    # 思考不回灌：assistant 正文里不应出现思考文字
    assert "思考" not in "".join(
        str(m.get("content") or "") for m in outputs if isinstance(m, dict)
    )


@pytest.mark.asyncio
async def test_agent_loop_stream_error_falls_back_to_chat():
    """stream_chat 报错（且未产出内容）→ 退回 chat()，事件格式仍统一。"""
    non_stream_resp = _response(
        "fallback answer",
        usage={"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10},
    )

    class _Client:
        def __init__(self):
            self.calls = []

        async def stream_chat(self, messages, tools=None):
            self.calls.append({"stream": True, "messages": messages, "tools": tools})
            raise RuntimeError("upstream does not support stream")
            yield  # pragma: no cover - 让函数成为 async generator

        async def chat(self, messages, tools=None):
            self.calls.append({"stream": False, "messages": messages, "tools": tools})
            return non_stream_resp

    client = _Client()
    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    outputs = await _collect_loop(
        client,
        "system",
        "hi",
        ScriptedHandler([]),
        tools_schema=[],
        verbose=False,
        on_event=on_event,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "fallback answer"}
    content_events = [e for e in events if e.get("type") == "content"]
    assert "".join(e["text"] for e in content_events) == "fallback answer"
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(done_events) == 1
    assert done_events[0].get("usage", {}).get("total_tokens") == 10
    # done 不带正文
    assert "data" not in done_events[0]
    # 调用过 stream_chat 和 chat 两个路径
    assert any(c["stream"] for c in client.calls)
    assert any(not c["stream"] for c in client.calls)


@pytest.mark.asyncio
async def test_agent_loop_stream_empty_chunks_falls_back_to_chat():
    """stream_chat 连上但没解析出任何 chunk（非 SSE 整段响应）→ 退回 chat()。"""
    non_stream_resp = _response("from chat", usage={"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7})

    class _Client:
        def __init__(self):
            self.calls = []

        async def stream_chat(self, messages, tools=None):
            self.calls.append({"stream": True, "messages": messages, "tools": tools})
            if False:
                yield  # pragma: no cover

        async def chat(self, messages, tools=None):
            self.calls.append({"stream": False, "messages": messages, "tools": tools})
            return non_stream_resp

    client = _Client()
    outputs = await _collect_loop(
        client,
        "system",
        "hi",
        ScriptedHandler([]),
        tools_schema=[],
        verbose=False,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "from chat"}
    assert any(not c["stream"] for c in client.calls)


@pytest.mark.asyncio
async def test_agent_loop_stream_partial_error_does_not_fallback():
    """已吐过部分内容才失败 → 不重试，直接抛给上层（避免重复回答）。"""
    class _Client:
        async def stream_chat(self, messages, tools=None):
            yield _response("partial")
            raise RuntimeError("stream broke mid-way")

    with pytest.raises(RuntimeError, match="stream broke mid-way"):
        await _collect_loop(
            _Client(),
            "system",
            "hi",
            ScriptedHandler([]),
            tools_schema=[],
            verbose=False,
        )


@pytest.mark.asyncio
async def test_agent_loop_retries_other_exception_then_succeeds(monkeypatch):
    """非网络异常（如 RuntimeError）在未吐内容、fallback 也失败时重试，并发出 retry 事件。"""
    import agent.agent_loop as al
    monkeypatch.setattr(al.asyncio, "sleep", AsyncMock(return_value=None))

    non_stream_resp = _response("recovered", usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2})

    class _Client:
        max_retries = 1

        def __init__(self):
            self.chat_calls = 0

        async def stream_chat(self, messages, tools=None):
            raise RuntimeError("upstream hiccup")
            yield  # pragma: no cover - async generator

        async def chat(self, messages, tools=None):
            self.chat_calls += 1
            if self.chat_calls < 2:
                raise ValueError("chat also broken on first try")
            return non_stream_resp

    client = _Client()
    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    outputs = await _collect_loop(
        client,
        "system",
        "hi",
        ScriptedHandler([]),
        tools_schema=[],
        verbose=False,
        on_event=on_event,
    )

    assert outputs[-1] == {"result": "CURRENT_TASK_DONE", "data": "recovered"}
    retry_events = [e for e in events if e.get("type") == "retry"]
    assert len(retry_events) == 1
    assert retry_events[0]["attempt"] == 1
    assert retry_events[0]["max"] == 1
    assert retry_events[0]["scope"] == "stream"
    assert "ValueError" in retry_events[0]["reason"]


@pytest.mark.asyncio
async def test_agent_loop_network_exception_not_retried_at_loop(monkeypatch):
    """网络异常由 LLMBridge 负责；loop 层收到即代表已耗尽，不再重试直接抛。"""
    import asyncio as _asyncio
    import agent.agent_loop as al
    monkeypatch.setattr(al.asyncio, "sleep", AsyncMock(return_value=None))

    class _Client:
        max_retries = 3  # loop 层会读这个值，但网络异常不走 loop 重试

        async def stream_chat(self, messages, tools=None):
            raise _asyncio.TimeoutError()
            yield  # pragma: no cover

        async def chat(self, messages, tools=None):
            raise _asyncio.TimeoutError()

    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    with pytest.raises(_asyncio.TimeoutError):
        await _collect_loop(
            _Client(),
            "system",
            "hi",
            ScriptedHandler([]),
            tools_schema=[],
            verbose=False,
            on_event=on_event,
        )
    assert not [e for e in events if e.get("type") == "retry"]


@pytest.mark.asyncio
async def test_agent_loop_max_retries_zero_just_falls_back(monkeypatch):
    """max_retries=0 → 不重试，单次 stream 失败直接走 fallback chat；chat 也失败则直抛。"""
    import agent.agent_loop as al
    monkeypatch.setattr(al.asyncio, "sleep", AsyncMock(return_value=None))

    class _Client:
        max_retries = 0

        async def stream_chat(self, messages, tools=None):
            raise RuntimeError("no stream")
            yield  # pragma: no cover

        async def chat(self, messages, tools=None):
            raise ValueError("chat also broken")

    events: list[dict] = []

    async def on_event(ev: dict):
        events.append(ev)

    with pytest.raises(ValueError):
        await _collect_loop(
            _Client(),
            "system",
            "hi",
            ScriptedHandler([]),
            tools_schema=[],
            verbose=False,
            on_event=on_event,
        )
    assert not [e for e in events if e.get("type") == "retry"]
