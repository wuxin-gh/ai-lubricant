"""请求主链路重构接线测试（全 fake，无 PG/Redis/provider）。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import aiohttp
import pytest
from fastapi import HTTPException

import main
from rate_limiter import ModelClientPool


class _Account:
    def __init__(self, route_info):
        self.last_route_info = route_info
        self.release_calls = 0

    async def release(self):
        self.release_calls += 1


async def _false_group(cls, model, snapshot=None):
    return False


async def _empty_defaults(model, *, snapshot=None):
    return {}


def _patch_common(monkeypatch, *, retries=1):
    snapshot = SimpleNamespace(generation=1)
    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(_false_group))
    monkeypatch.setattr(main.config.Config, "get_global_retry_count", lambda: retries)
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: retries)
    monkeypatch.setattr(main.config.Config, "get_provider_extra_retry_status_codes", lambda provider: set())
    monkeypatch.setattr(main.config.Config, "stream_incomplete_error_enabled", lambda: False)
    monkeypatch.setattr(main.config.Config, "get_rate_limit_status_codes", lambda provider: asyncio.sleep(0, result=[]))
    monkeypatch.setattr(main.config.Config, "get_non_retryable_parameter_errors", lambda: {})
    monkeypatch.setattr(main, "_request_defaults_from_metadata", _empty_defaults)
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["p"])
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider, model: model)
    monkeypatch.setattr(ModelClientPool, "record_channel_failure", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "record_account_failure", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "record_account_success", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "record_channel_ttft", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", lambda provider: None)
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kwargs: None)
    monkeypatch.setattr(main, "_finalize_channel_attempt_log", lambda **kwargs: asyncio.sleep(0))


def test_dispatch_builds_request_context_without_applying_model_defaults(monkeypatch):
    defaults_called = []

    async def validate(body, api_key=None):
        return body["model"], body["messages"]

    async def defaults(model, kwargs, *, snapshot=None):
        defaults_called.append(model)
        return kwargs

    monkeypatch.setattr(main, "_validate_chat_request", validate)
    monkeypatch.setattr(main, "_apply_global_request_defaults", defaults)

    body = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": False}
    dispatch = asyncio.run(main.dispatch_entry(
        endpoint="/v1/chat/completions",
        body=body,
        headers={},
        api_key=None,
        api_key_name=None,
        request_protocol="openai",
        chat_method="chat",
    ))
    context = dispatch["context"]
    assert isinstance(context, main.RequestContext)
    assert context.original_model == "m"
    assert context.original_body == body
    assert defaults_called == []  # 入口不再固定模型默认参数；选中候选后才应用
    asyncio.run(dispatch["generator"].aclose())


def test_attempt_defaults_use_selected_routed_model_and_fresh_snapshot(monkeypatch):
    _patch_common(monkeypatch, retries=1)
    seen_defaults = []
    acquired = []

    async def defaults(model, *, snapshot=None):
        seen_defaults.append((model, snapshot.generation))
        return {"temperature": 0.7}

    monkeypatch.setattr(main, "_request_defaults_from_metadata", defaults)

    class Client:
        username = "acct"
        async def chat(self, model, messages, **kwargs):
            acquired.append((model, kwargs.get("temperature")))
            if len(acquired) == 1:
                raise aiohttp.ClientConnectionError("retry")
            yield {"choices": [{"message": {"content": "ok"}}]}

    accounts = []
    async def acquire(**kwargs):
        account = _Account({"provider": "p", "account": "acct", "routed_model": "routed"})
        accounts.append(account)
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [x async for x in main._chat_with_retry_for_model("requested", [], False)]

    asyncio.run(collect())
    assert seen_defaults == [("routed", 1)]
    assert acquired == [("routed", 0.7), ("routed", 0.7)]
    # 网络异常先由渠道内层用同一个账号重试，不重新选路。
    assert [a.release_calls for a in accounts] == [1]


def test_attempt_preserves_explicit_endpoint_path_over_routed_protocol_row(monkeypatch):
    _patch_common(monkeypatch, retries=0)
    explicit_endpoint = {
        "id": "selected-by-test",
        "protocol": "anthropic",
        "path": "/fixed/messages",
    }
    routed_endpoint = {
        "id": "selected-by-request-protocol",
        "protocol": "responses",
        "path": "/v1/responses",
    }
    captured_kwargs = {}

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            captured_kwargs.update(kwargs)
            yield {"choices": [{"message": {"content": "ok"}}]}

    account = _Account({
        "provider": "p",
        "account": "acct",
        "routed_model": "m",
        "endpoint_config": routed_endpoint,
    })

    async def acquire(**kwargs):
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [chunk async for chunk in main._chat_with_retry_for_model(
            "m",
            [],
            False,
            request_protocol="responses",
            _endpoint_config=explicit_endpoint,
        )]

    asyncio.run(collect())

    assert captured_kwargs["_endpoint_config"] == explicit_endpoint
    assert captured_kwargs["_endpoint_config"]["path"] == "/fixed/messages"


def test_empty_non_stream_response_retries_same_account(monkeypatch):
    _patch_common(monkeypatch, retries=2)
    acquire_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal acquire_calls
            acquire_calls += 1
            raise main.EmptyNonStreamResponseError("empty")
            yield

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})

    async def acquire(**kwargs):
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [chunk async for chunk in main._chat_with_retry_for_model("m", [], False)]

    try:
        asyncio.run(collect())
    except main.HTTPException as exc:
        assert exc.status_code == 429
    # 渠道内层 2 次重试均复用同一账号，不重新进入 acquire。
    assert acquire_calls == 3


def test_response_model_reloads_with_attempt_snapshot(monkeypatch):
    _patch_common(monkeypatch, retries=1)
    snapshots = [SimpleNamespace(generation=0), SimpleNamespace(generation=1), SimpleNamespace(generation=2)]
    snapshot_index = 0
    rewritten = []
    calls = 0

    def current_snapshot():
        nonlocal snapshot_index
        value = snapshots[min(snapshot_index, len(snapshots) - 1)]
        snapshot_index += 1
        return value

    async def is_group(cls, model, snapshot=None):
        return True

    async def resolve_group(cls, model, snapshot=None):
        return {"name": "group", "response_model": f"public-{snapshot.generation}"}

    async def response_model(cls, model, snapshot=None):
        return f"public-{snapshot.generation}"

    monkeypatch.setattr(main.model_catalog, "current_snapshot", current_snapshot)
    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(is_group))
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve_group))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(response_model))
    monkeypatch.setattr(main.config.Config, "get_model_group_models", classmethod(lambda cls, model, snapshot=None: asyncio.sleep(0, result=["m"])))

    class Client:
        username = "acct"
        async def chat(self, model, messages, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise aiohttp.ClientConnectionError("retry")
            yield {"choices": [{"message": {"content": "ok"}}]}

    async def acquire(**kwargs):
        account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})
        return (Client(), "p", account), {}

    def rewrite(chunk, model, rewrite_nested=False):
        rewritten.append((model, rewrite_nested))
        return chunk

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)
    monkeypatch.setattr(main, "_stream_chunk_with_public_model", rewrite)

    async def collect():
        return [x async for x in main._chat_with_retry_for_model(
            "alias", [], False,
            _response_model_source="alias",
            _group_request_identity=True,
        )]

    asyncio.run(collect())
    # 网络异常由渠道内层在同一个已选路上下文中重试，因此响应模型保持首次选路快照。
    assert rewritten == [("public-1", True)]


def test_stream_error_after_downstream_started_does_not_retry(monkeypatch):
    _patch_common(monkeypatch, retries=2)
    acquire_calls = 0

    class Client:
        username = "acct"
        async def chat(self, model, messages, **kwargs):
            yield "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
            raise RuntimeError("broken after output")

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})
    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        chunks = []
        try:
            async for chunk in main._chat_with_retry_for_model("m", [], True):
                chunks.append(chunk)
        except main.StreamStartedChannelError as exc:
            return chunks, exc
        raise AssertionError("expected StreamStartedChannelError")

    chunks, exc = asyncio.run(collect())
    assert acquire_calls == 1
    assert any(isinstance(c, str) and "hi" in c for c in chunks)
    # 流已开始输出后上游报错：无法重试只能收尾，但绝不透传上游原始错误文案，
    # 与非流式最终 429 口径一致——统一 rate_limit + 标准 RETRYABLE_CLIENT_MESSAGE。
    assert "broken after output" not in str(exc)
    assert str(exc) == main.RETRYABLE_CLIENT_MESSAGE
    assert exc.status_code == 429
    assert exc.error_type == "rate_limit_error"
    assert account.release_calls == 1


def test_incomplete_stream_before_output_retries_next_candidate(monkeypatch):
    """空流/零 completion 在未向客户端输出前应切下一个候选重试，而不是直接 429。"""
    _patch_common(monkeypatch, retries=2)
    acquire_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal acquire_calls
            if acquire_calls == 1:
                raise main.IncompleteStreamError(
                    main.UPSTREAM_ZERO_COMPLETION_MESSAGE, "upstream_incomplete_usage"
                )
                yield
            else:
                yield 'data: {"choices":[{"delta":{"content":"ok"}}]}\n\n'

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [chunk async for chunk in main._chat_with_retry_for_model("m", [], True)]

    chunks = asyncio.run(collect())

    assert acquire_calls == 2, "不完整流应换下一个候选重试"
    assert any(isinstance(c, str) and "ok" in c for c in chunks)


def test_incomplete_stream_after_downstream_started_does_not_retry(monkeypatch):
    """已经向客户端发过字节后再判不完整：只能收尾，绝不能换渠道拼接输出。"""
    _patch_common(monkeypatch, retries=2)
    acquire_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            yield 'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
            raise main.IncompleteStreamError(
                main.UPSTREAM_ZERO_COMPLETION_MESSAGE, "upstream_incomplete_usage"
            )

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        chunks = []
        try:
            async for chunk in main._chat_with_retry_for_model("m", [], True):
                chunks.append(chunk)
        except main.StreamStartedChannelError as exc:
            return chunks, exc
        raise AssertionError("expected StreamStartedChannelError")

    chunks, exc = asyncio.run(collect())
    assert acquire_calls == 1
    assert any(isinstance(c, str) and "hi" in c for c in chunks)
    assert exc.status_code == 429


def test_non_retryable_4xx_passthrough_without_retry(monkeypatch):
    _patch_common(monkeypatch, retries=2)
    monkeypatch.setattr(
        main.config.Config,
        "get_non_retryable_parameter_errors",
        lambda: {"params": ["metadata"]},
    )
    acquire_calls = 0

    class Client:
        username = "acct"
        async def chat(self, model, messages, **kwargs):
            raise HTTPException(
                status_code=400,
                detail={"error": {"type": "invalid_request_error", "param": "metadata", "message": "bad"}},
            )
            yield

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})
    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        with_status = None
        try:
            async for _ in main._chat_with_retry_for_model("m", [], False):
                pass
        except HTTPException as exc:
            with_status = exc.status_code
        return with_status

    assert asyncio.run(collect()) == 400
    assert acquire_calls == 1
    assert account.release_calls == 1


def test_unknown_404_switches_outer_candidate_without_inner_retry(monkeypatch):
    """未配置的 404 不在同账号原地重试，但必须交给全局外层重新选路。"""
    _patch_common(monkeypatch, retries=1)
    acquire_calls = 0
    chat_calls = 0
    accounts = []

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal chat_calls
            chat_calls += 1
            if chat_calls == 1:
                raise HTTPException(status_code=404, detail="upstream route not found")
            yield {"choices": [{"message": {"content": "ok"}}]}

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        account = _Account({"provider": "p", "account": f"acct-{acquire_calls}", "routed_model": "m"})
        accounts.append(account)
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [x async for x in main._chat_with_retry_for_model("m", [], False)]

    chunks = asyncio.run(collect())
    assert acquire_calls == 2, "404 应由外层重新选路"
    assert chat_calls == 2, "404 不应触发同账号内层重复请求"
    assert any(isinstance(c, dict) and c.get("choices") for c in chunks)
    assert [account.release_calls for account in accounts] == [1, 1]


def test_repeated_404_uses_exact_global_attempt_budget_then_returns_429(monkeypatch):
    _patch_common(monkeypatch, retries=0)
    monkeypatch.setattr(main.config.Config, "get_global_retry_count", lambda: 2)
    acquire_calls = 0
    chat_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal chat_calls
            chat_calls += 1
            raise HTTPException(status_code=404, detail="upstream route not found")
            yield

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        account = _Account({"provider": "p", "account": f"acct-{acquire_calls}", "routed_model": "m"})
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        try:
            async for _ in main._chat_with_retry_for_model("m", [], False):
                pass
        except HTTPException as exc:
            return exc
        return None

    exc = asyncio.run(collect())
    assert exc is not None
    assert exc.status_code == 429
    assert exc.detail["upstream_status"] == 404
    assert exc.detail["retries"] == 2
    assert acquire_calls == 3
    assert chat_calls == 3


def test_channel_retry_count_four_retries_same_account_before_outer(monkeypatch):
    """retry_count=4：首次失败后同账号再请求 4 次，全部失败后才进入外层选路。"""
    _patch_common(monkeypatch, retries=4)
    # 只给一个外层候选预算，便于精确断言一个候选内：首次 + 4 次重试 = 5 次请求。
    monkeypatch.setattr(main.config.Config, "get_global_retry_count", lambda: 0)
    chat_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal chat_calls
            chat_calls += 1
            raise HTTPException(status_code=502, detail=f"failed-{chat_calls}")
            yield

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})
    acquire_calls = 0

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [x async for x in main._chat_with_retry_for_model("m", [], False)]

    with pytest.raises(HTTPException) as exc:
        asyncio.run(collect())
    assert exc.value.status_code == 429
    assert acquire_calls == 1, "渠道内层 4 次重试期间不得重新选路"
    assert chat_calls == 5, "首次请求失败后应在同账号原地再请求 4 次"
    assert account.release_calls == 1, "同一候选的并发预占只释放一次"


def test_configured_extra_status_code_retries_same_account(monkeypatch):
    """配置为额外重试状态码的 408：内层同账号原地重试，不重新选路。"""
    _patch_common(monkeypatch, retries=2)
    monkeypatch.setattr(
        main.config.Config,
        "get_provider_extra_retry_status_codes",
        lambda provider: {408},
    )
    chat_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal chat_calls
            chat_calls += 1
            if chat_calls == 1:
                raise HTTPException(status_code=408, detail="timeout")
            yield {"choices": [{"message": {"content": "ok"}}]}

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})
    acquire_calls = 0

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        return [x async for x in main._chat_with_retry_for_model("m", [], False)]

    chunks = asyncio.run(collect())
    assert acquire_calls == 1, "408 内层同账号重试，不重新选路"
    assert chat_calls == 2, "首次 408 后同账号再发一次成功"
    assert any(isinstance(c, dict) and c.get("choices") for c in chunks)
    assert account.release_calls == 1


def test_global_retry_count_drives_outer_loop_not_channel(monkeypatch):
    """外层选路次数由全局重试次数决定，不被渠道内层次数覆盖。"""
    _patch_common(monkeypatch, retries=0)
    monkeypatch.setattr(main.config.Config, "get_global_retry_count", lambda: 2)
    # 渠道内层 0 次：网络异常不内层重试，直接交外层换路。

    chat_calls = 0

    class Client:
        username = "acct"

        async def chat(self, model, messages, **kwargs):
            nonlocal chat_calls
            chat_calls += 1
            raise aiohttp.ClientConnectionError("network")
            yield

    account = _Account({"provider": "p", "account": "acct", "routed_model": "m"})
    acquire_calls = 0

    async def acquire(**kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return (Client(), "p", account), {}

    monkeypatch.setattr(main, "_acquire_client_with_routing_timing", acquire)

    async def collect():
        try:
            async for _ in main._chat_with_retry_for_model("m", [], False):
                pass
        except main.HTTPException as exc:
            return exc.status_code
        return None

    assert asyncio.run(collect()) == 429
    # 全局 2 → 外层 3 次选路；渠道内层 0 → 每次外层只发 1 个请求。
    # 若渠道次数覆盖全局，total_attempts 会是 1，acquire/chat 只各 1 次。
    assert acquire_calls == 3
    assert chat_calls == 3
