import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure the worktree root is on sys.path so that
# ``from agent.llm_bridge import LLMBridge`` resolves.
_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.llm_bridge import LLMBridge, LLMResponse, _LLMHttpError  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _llm_config(**overrides) -> dict:
    cfg = {
        "id": 1,
        "name": "test-llm",
        "base_url": "https://example.com/v1",
        "chat_path": "/chat/completions",
        "api_key": "sk-test",
        "protocol": "openai",
        "enabled": True,
    }
    cfg.update(overrides)
    return cfg


def _make_response(status: int, body: dict | str | None = None):
    """构造一个伪 aiohttp 响应对象。"""
    resp = MagicMock()
    resp.status = status
    if isinstance(body, (dict, list)):
        resp.json = AsyncMock(return_value=body)
        resp.text = AsyncMock(return_value=json.dumps(body))
    elif isinstance(body, str):
        resp.json = AsyncMock(side_effect=ValueError("not json"))
        resp.text = AsyncMock(return_value=body)
    else:
        resp.json = AsyncMock(return_value={})
        resp.text = AsyncMock(return_value="")
    return resp


def _patch_aiohttp_post(monkeypatch, response):
    """拦截 LLMBridge._make_session，返回受控的 session/post。"""
    post_ctx = MagicMock()
    post_ctx.__aenter__ = AsyncMock(return_value=response)
    post_ctx.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.post = MagicMock(return_value=post_ctx)
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))
    return session, post_ctx


def test_fork_inherits_config_and_overrides_model():
    bridge = LLMBridge(_llm_config(), "model-a")
    inherited = bridge.fork()
    overridden = bridge.fork(model="model-b")

    assert inherited is not bridge
    assert inherited.model == "model-a"
    assert inherited.base_url == "https://example.com/v1"
    assert inherited.api_key == "sk-test"
    assert overridden.model == "model-b"
    # fork 拷贝 llm_config（而非共享同一 dict）：thinking_enabled/reasoning_effort
    # 覆盖只作用于派生桥，绝不回写父桥的配置。
    assert overridden.llm_config == bridge.llm_config
    assert overridden.llm_config is not bridge.llm_config
    bridge.fork(thinking_enabled=True, reasoning_effort="high")
    assert "thinking_enabled" not in bridge.llm_config or bridge.llm_config["thinking_enabled"] is not True


def test_endpoint_joins_base_url_and_path():
    bridge = LLMBridge(_llm_config(base_url="https://x.com/v1/", chat_path="/chat/completions"), "m")
    assert bridge._endpoint() == "https://x.com/v1/chat/completions"

    bridge2 = LLMBridge(_llm_config(base_url="https://x.com/v1", chat_path="chat/completions"), "m")
    assert bridge2._endpoint() == "https://x.com/v1/chat/completions"


def test_chat_returns_content_and_tool_calls(monkeypatch):
    tool_calls = [{"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": "{}"}}]
    payload = {
        "choices": [{"message": {"role": "assistant", "content": "hi", "tool_calls": tool_calls}}],
        "usage": {"prompt_tokens": 3, "completion_tokens": 4, "total_tokens": 7},
    }
    _patch_aiohttp_post(monkeypatch, _make_response(200, payload))

    bridge = LLMBridge(_llm_config(), "public-model")
    result = _run(bridge.chat([{"role": "user", "content": "hi"}], tools=[{"type": "function", "function": {"name": "lookup"}}]))

    assert isinstance(result, LLMResponse)
    assert result.content == "hi"
    assert result.tool_calls == tool_calls
    assert result.usage == payload["usage"]
    assert result.raw == payload


def test_chat_propagates_upstream_error(monkeypatch):
    err_body = {"error": {"message": "Token locked due to a potential leak"}}
    _patch_aiohttp_post(monkeypatch, _make_response(403, err_body))

    bridge = LLMBridge(_llm_config(), "m")
    with pytest.raises(_LLMHttpError) as exc_info:
        _run(bridge.chat([{"role": "user", "content": "x"}]))

    assert exc_info.value.status_code == 403
    assert "Token locked" in exc_info.value.message


def test_chat_handles_tool_calls_with_empty_content(monkeypatch):
    tool_calls = [{"id": "c2", "type": "function", "function": {"name": "g", "arguments": "{}"}}]
    payload = {"choices": [{"message": {"content": None, "tool_calls": tool_calls}}], "usage": None}
    _patch_aiohttp_post(monkeypatch, _make_response(200, payload))

    result = _run(LLMBridge(_llm_config(), "m").chat([{"role": "user", "content": "x"}]))
    assert result.content == ""
    assert result.tool_calls == tool_calls
    assert result.usage is None


def test_reads_timeout_and_retry_config():
    bridge = LLMBridge(_llm_config(timeout_seconds=30, max_retries=4), "m")
    assert bridge.timeout_seconds == 30
    assert bridge.max_retries == 4


def test_defaults_timeout_and_retry_when_unset():
    bridge = LLMBridge(_llm_config(), "m")
    assert bridge.timeout_seconds == 600
    assert bridge.max_retries == 2  # 列默认值


def _make_session_closing_on_first_call_then_success(monkeypatch, responses):
    """前 N 次 post 抛网络异常，之后返回最终响应。responses=[exc1, exc2, ..., final_resp]。"""
    post_index = {"i": 0}

    post_ctx_ok = MagicMock()
    post_ctx_ok.__aenter__ = AsyncMock(return_value=responses[-1])
    post_ctx_ok.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    def _post(*a, **kw):
        i = post_index["i"]
        post_index["i"] += 1
        if i < len(responses) - 1:
            exc = responses[i]
            ctx = MagicMock()
            # __aenter__ 抛异常，模拟建连/请求阶段网络错误
            ctx.__aenter__ = AsyncMock(side_effect=exc)
            ctx.__aexit__ = AsyncMock(return_value=None)
            return ctx
        return post_ctx_ok

    session.post = MagicMock(side_effect=_post)
    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))
    return session


def test_chat_retries_network_exception_then_succeeds(monkeypatch):
    import asyncio as _asyncio
    post_index = {"i": 0}
    final = _make_response(200, {"choices": [{"message": {"content": "ok"}}], "usage": None})
    post_ctx_ok = MagicMock()
    post_ctx_ok.__aenter__ = AsyncMock(return_value=final)
    post_ctx_ok.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    def _post(*a, **kw):
        i = post_index["i"]
        post_index["i"] += 1
        if i < 2:
            return MagicMock(__aenter__=AsyncMock(side_effect=_asyncio.TimeoutError()),
                             __aexit__=AsyncMock(return_value=None))
        return post_ctx_ok

    session.post = MagicMock(side_effect=_post)
    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))

    retried: list[dict] = []

    async def on_retry(info: dict):
        retried.append(info)

    # 退避 sleep 太慢，patch 成即时
    monkeypatch.setattr(llm_bridge.asyncio, "sleep", AsyncMock(return_value=None))

    bridge = LLMBridge(_llm_config(max_retries=2), "m")
    result = _run(bridge.chat([{"role": "user", "content": "x"}], on_retry=on_retry))

    assert result.content == "ok"
    assert len(retried) == 2
    assert retried[0]["attempt"] == 1
    assert retried[0]["max"] == 2
    assert retried[0]["scope"] == "network"
    assert "TimeoutError" in retried[0]["reason"]


def test_chat_exhausted_retries_raises(monkeypatch):
    import asyncio as _asyncio
    post_index = {"i": 0}
    session = MagicMock()
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    def _post(*a, **kw):
        post_index["i"] += 1
        return MagicMock(__aenter__=AsyncMock(side_effect=_asyncio.TimeoutError()),
                         __aexit__=AsyncMock(return_value=None))

    session.post = MagicMock(side_effect=_post)
    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))
    monkeypatch.setattr(llm_bridge.asyncio, "sleep", AsyncMock(return_value=None))

    retried: list[dict] = []

    async def on_retry(info: dict):
        retried.append(info)

    bridge = LLMBridge(_llm_config(max_retries=1), "m")
    with pytest.raises(_asyncio.TimeoutError):
        _run(bridge.chat([{"role": "user", "content": "x"}], on_retry=on_retry))
    assert len(retried) == 1  # 失败一次后发出一次 retry 事件，之后到上限再失败直接抛


def test_chat_max_retries_zero_no_retry(monkeypatch):
    import asyncio as _asyncio
    calls = {"n": 0}
    session = MagicMock()
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    def _post(*a, **kw):
        calls["n"] += 1
        return MagicMock(__aenter__=AsyncMock(side_effect=_asyncio.TimeoutError()),
                         __aexit__=AsyncMock(return_value=None))

    session.post = MagicMock(side_effect=_post)
    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))

    bridge = LLMBridge(_llm_config(max_retries=0), "m")
    with pytest.raises(_asyncio.TimeoutError):
        _run(bridge.chat([{"role": "user", "content": "x"}]))
    assert calls["n"] == 1  # 只调用一次，不重试


def test_chat_non_200_not_retried_at_bridge(monkeypatch):
    """_LLMHttpError 属「其他异常」，bridge 层不重试，直接抛交由 loop 处理。"""
    calls = {"n": 0}
    resp = _make_response(500, {"error": {"message": "boom"}})
    post_ctx = MagicMock()
    post_ctx.__aenter__ = AsyncMock(return_value=resp)
    post_ctx.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    def _post(*a, **kw):
        calls["n"] += 1
        return post_ctx

    session.post = MagicMock(side_effect=_post)
    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))

    bridge = LLMBridge(_llm_config(max_retries=3), "m")
    with pytest.raises(_LLMHttpError):
        _run(bridge.chat([{"role": "user", "content": "x"}]))
    assert calls["n"] == 1  # HTTP 错误不重试


@pytest.mark.asyncio
async def test_stream_chat_does_not_retry_after_first_yield(monkeypatch):
    """首 chunk 已 yield 后再断流 → 不重试，直接抛（避免重复内容）。"""
    import asyncio as _asyncio
    from message_utils import iter_sse_payloads  # noqa: F401

    payloads = [
        {"choices": [{"delta": {"content": "partial"}, "finish_reason": None}]},
    ]
    lines = [f"data: {__import__('json').dumps(p)}\n\n".encode() for p in payloads]

    class _Content:
        def __init__(self, lines):
            self._lines = lines
            self._i = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._i < len(self._lines):
                v = self._lines[self._i]
                self._i += 1
                return v
            # 模拟读完首行后断连
            raise _asyncio.TimeoutError()

    resp = MagicMock()
    resp.status = 200
    resp.text = AsyncMock(return_value="")
    resp.content = _Content(lines)
    post_ctx = MagicMock()
    post_ctx.__aenter__ = AsyncMock(return_value=resp)
    post_ctx.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.close = AsyncMock(return_value=None)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=post_ctx)
    import agent.llm_bridge as llm_bridge
    monkeypatch.setattr(llm_bridge.LLMBridge, "_make_session", MagicMock(return_value=session))
    monkeypatch.setattr(llm_bridge.asyncio, "sleep", AsyncMock(return_value=None))

    retried: list[dict] = []

    async def on_retry(info: dict):
        retried.append(info)

    bridge = LLMBridge(_llm_config(max_retries=3), "m")
    chunks = []
    with pytest.raises(_asyncio.TimeoutError):
        async for c in bridge.stream_chat([{"role": "user", "content": "x"}], on_retry=on_retry):
            chunks.append(c)
    assert len(chunks) == 1
    assert chunks[0].content == "partial"
    assert retried == []  # 已吐内容，不重试


# ── 思考(reasoning)增量解析 ────────────────────────────────────────────────
# 上游把思考放在 delta.reasoning_content / reasoning / thinking（各家不统一）。
# 桥必须把它解析进 LLMResponse.reasoning，且不混进 content —— 否则 agent_loop
# 拿不到思考，前端就只能看到最终答案。


def _stream_bridge_chunks(monkeypatch, payloads: list[dict]) -> list[LLMResponse]:
    """让 LLMBridge.stream_chat 读一组预设 SSE payload，返回它 yield 的 chunk。"""
    lines = [f"data: {json.dumps(p)}\n\n".encode() for p in payloads]

    class _Content:
        def __aiter__(self):
            return self

        def __init__(self):
            self._i = 0

        async def __anext__(self):
            if self._i >= len(lines):
                raise StopAsyncIteration
            value = lines[self._i]
            self._i += 1
            return value

    resp = MagicMock()
    resp.status = 200
    resp.text = AsyncMock(return_value="")
    resp.content = _Content()
    _patch_aiohttp_post(monkeypatch, resp)

    bridge = LLMBridge(_llm_config(), "m")

    async def run():
        out = []
        async for chunk in bridge.stream_chat([{"role": "user", "content": "x"}]):
            out.append(chunk)
        return out

    return _run(run())


@pytest.mark.parametrize("field", ["reasoning_content", "reasoning", "thinking"])
def test_stream_chat_parses_reasoning_delta(monkeypatch, field):
    chunks = _stream_bridge_chunks(monkeypatch, [
        {"choices": [{"delta": {field: "думаю"}}]},
        {"choices": [{"delta": {"content": "answer"}}]},
    ])
    assert "".join(c.reasoning for c in chunks) == "думаю"
    assert "".join(c.content for c in chunks) == "answer"
    # 思考绝不能混进正文
    assert "думаю" not in "".join(c.content for c in chunks)


def test_stream_chat_yields_reasoning_only_chunk(monkeypatch):
    """只有思考、没有正文的 chunk 也必须 yield，否则思考会被整段吞掉。"""
    chunks = _stream_bridge_chunks(monkeypatch, [
        {"choices": [{"delta": {"reasoning_content": "step 1"}}]},
        {"choices": [{"delta": {"reasoning_content": " step 2"}}]},
    ])
    assert "".join(c.reasoning for c in chunks) == "step 1 step 2"
    assert "".join(c.content for c in chunks) == ""


def test_chat_non_stream_parses_reasoning(monkeypatch):
    payload = {
        "choices": [{"message": {"role": "assistant", "content": "hi", "reasoning_content": "because"}}],
    }
    _patch_aiohttp_post(monkeypatch, _make_response(200, payload))
    result = _run(LLMBridge(_llm_config(), "m").chat([{"role": "user", "content": "hi"}]))
    assert result.content == "hi"
    assert result.reasoning == "because"


def test_gateway_bridge_stream_parses_reasoning(monkeypatch):
    from agent.llm_bridge import GatewayLLMBridge

    async def gen():
        yield f"data: {json.dumps({'choices': [{'delta': {'reasoning_content': 'why'}}]})}\n\n"
        yield f"data: {json.dumps({'choices': [{'delta': {'content': 'so'}}]})}\n\n"

    _install_fake_main(monkeypatch, generator=gen, stream=True)
    bridge = GatewayLLMBridge(api_key="sk-gw", api_key_name="k", model="gpt-x")

    async def run():
        out = []
        async for chunk in bridge.stream_chat([{"role": "user", "content": "hi"}]):
            out.append(chunk)
        return out

    chunks = _run(run())
    assert "".join(c.reasoning for c in chunks) == "why"
    assert "".join(c.content for c in chunks) == "so"


# ── 思考模式（agent 级 reasoning_effort 注入）──────────────────────────────

def test_build_body_no_reasoning_when_thinking_disabled():
    bridge = LLMBridge(_llm_config(), "m")
    body = bridge._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert "reasoning_effort" not in body
    assert "reasoning" not in body


def test_build_body_openai_injects_reasoning_effort():
    bridge = LLMBridge(
        _llm_config(protocol="openai", thinking_enabled=True, reasoning_effort="medium"),
        "m",
    )
    body = bridge._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert body["reasoning_effort"] == "medium"


def test_build_body_chat_protocol_injects_reasoning_effort():
    bridge = LLMBridge(
        _llm_config(protocol="chat", thinking_enabled=True, reasoning_effort="high"),
        "m",
    )
    body = bridge._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert body["reasoning_effort"] == "high"


def test_build_body_responses_protocol_injects_reasoning_object():
    bridge = LLMBridge(
        _llm_config(protocol="responses", thinking_enabled=True, reasoning_effort="low"),
        "m",
    )
    body = bridge._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert body["reasoning"] == {"effort": "low"}
    assert "reasoning_effort" not in body


def test_build_body_thinking_enabled_but_empty_effort_does_not_inject():
    # 开了开关但 effort 为空 → 不注入，避免发送空值
    bridge = LLMBridge(
        _llm_config(protocol="openai", thinking_enabled=True, reasoning_effort=""),
        "m",
    )
    body = bridge._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert "reasoning_effort" not in body
    assert "reasoning" not in body


def test_build_body_anthropic_protocol_skips_injection():
    bridge = LLMBridge(
        _llm_config(protocol="anthropic", thinking_enabled=True, reasoning_effort="medium"),
        "m",
    )
    body = bridge._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert "reasoning_effort" not in body
    assert "reasoning" not in body


def test_fork_inherits_thinking_config():
    bridge = LLMBridge(
        _llm_config(protocol="openai", thinking_enabled=True, reasoning_effort="xhigh"),
        "m",
    )
    child = bridge.fork(model="child")
    assert child.thinking_enabled is True
    assert child.reasoning_effort == "xhigh"
    body = child._build_body([{"role": "user", "content": "x"}], None, stream=False)
    assert body["reasoning_effort"] == "xhigh"


# ==================== GatewayLLMBridge ====================
# 走网关（main.dispatch_entry）的桥：验证请求体组装、SSE 解析、非流式收集。


def _install_fake_main(monkeypatch, *, generator, stream: bool, non_stream_result=None):
    """安装一个假的 main 模块，拦截 dispatch_entry / _collect_non_stream_result。

    dispatch_entry 记录调用参数到返回的 spy dict；generator 为异步生成器工厂。
    """
    import sys as _sys

    spy: dict = {}

    async def fake_dispatch_entry(**kwargs):
        spy["dispatch_kwargs"] = kwargs
        return {"kind": "chat", "stream": stream, "generator": generator(), "model": kwargs.get("body", {}).get("model")}

    async def fake_collect(gen):
        # 消费 generator（跳过 _last_route_info），返回预设非流式结果。
        async for _ in gen:
            pass
        return non_stream_result

    fake_main = MagicMock()
    fake_main.dispatch_entry = fake_dispatch_entry
    fake_main._collect_non_stream_result = fake_collect
    monkeypatch.setitem(_sys.modules, "main", fake_main)
    return spy


def test_gateway_bridge_stream_parses_sse_content_and_tool_calls(monkeypatch):
    from agent.llm_bridge import GatewayLLMBridge

    async def gen():
        # 路由信息 dict 应被跳过；SSE 字符串按 OpenAI 增量解析。
        yield {"_last_route_info": {"provider": "p"}}
        yield f"data: {json.dumps({'choices': [{'delta': {'content': 'Hello'}}]})}\n\n"
        yield f"data: {json.dumps({'choices': [{'delta': {'tool_calls': [{'index': 0, 'id': 'c1', 'function': {'name': 'lookup', 'arguments': '{}'}}]}}]})}\n\n"
        yield f"data: {json.dumps({'usage': {'prompt_tokens': 3, 'completion_tokens': 2, 'total_tokens': 5}})}\n\n"

    spy = _install_fake_main(monkeypatch, generator=gen, stream=True)
    bridge = GatewayLLMBridge(api_key="sk-gw", api_key_name="k", model="gpt-x")

    async def run():
        out = []
        async for chunk in bridge.stream_chat([{"role": "user", "content": "hi"}], tools=[{"type": "function"}]):
            out.append(chunk)
        return out

    chunks = _run(run())
    contents = "".join(c.content for c in chunks if c.content)
    assert contents == "Hello"
    tool_calls = [tc for c in chunks for tc in (c.tool_calls or [])]
    assert any(tc["function"]["name"] == "lookup" for tc in tool_calls)
    usage = next((c.usage for c in chunks if c.usage), None)
    assert usage and usage["total_tokens"] == 5
    # dispatch_entry 被以网关 key + /v1/chat/completions 调用，且透传 tools。
    kwargs = spy["dispatch_kwargs"]
    assert kwargs["api_key"] == "sk-gw"
    assert kwargs["endpoint"] == "/v1/chat/completions"
    assert kwargs["body"]["tools"] == [{"type": "function"}]
    assert kwargs["body"]["stream"] is True


def test_gateway_bridge_chat_non_stream(monkeypatch):
    from agent.llm_bridge import GatewayLLMBridge

    async def gen():
        yield {"_last_route_info": {"provider": "p"}}

    result = {
        "choices": [{"message": {"content": "done", "tool_calls": []}}],
        "usage": {"total_tokens": 7},
    }
    _install_fake_main(monkeypatch, generator=gen, stream=False, non_stream_result=result)
    bridge = GatewayLLMBridge(api_key="sk-gw", model="gpt-x")

    resp = _run(bridge.chat([{"role": "user", "content": "hi"}]))
    assert resp.content == "done"
    assert resp.usage == {"total_tokens": 7}


def test_gateway_bridge_fork_keeps_key_changes_model(monkeypatch):
    from agent.llm_bridge import GatewayLLMBridge

    bridge = GatewayLLMBridge(
        api_key="sk-gw", api_key_name="k", model="main-model",
        provider_whitelist={"openai"},
    )
    child = bridge.fork(model="sub-model")
    assert child.api_key == "sk-gw"
    assert child.model == "sub-model"
    assert child.provider_whitelist == {"openai"}
    # 网关自带重试，本层不叠加。
    assert bridge.max_retries == 0
