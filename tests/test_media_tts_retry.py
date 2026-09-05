"""媒体/语音生成重试链路与聊天主链路统一行为测试。

直接测 _media_generation_with_retry / _tts_generation_with_retry，绕开 endpoint 层的
config store / DB 依赖。聚焦三点：
- 上游 5xx 重试耗尽 → 统一 429（不裸透传上游状态码）；
- 可识别的上下文/输入超限 → 经 _canonical_context_overflow_exception 归一成统一 code/message；
- NoAvailableAccountError 原样冒泡，交外层组编排。
"""

import asyncio
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main
import retry_policy as rp
from rate_limiter import ModelClientPool, NoAvailableAccountError


class _FakeAccountClient:
    def __init__(self, provider):
        self.provider = provider
        self.username = "u1"
        self.released = False
        self.last_route_info = {
            "provider": "fake",
            "account": "u1",
            "requested_model": "m",
            "routed_model": "m",
            "public_model_id": "m",
            "upstream_model_id": "upstream-m",
        }

    async def release(self):
        self.released = True


def _make_sequence_provider(outcomes):
    class _P:
        PROVIDER_NAME = "fake"
        username = "u1"
        supports_image_generation = True
        supports_video_generation = True
        supports_tts = True

        def __init__(self):
            self.calls = 0

        async def _next(self):
            self.calls += 1
            outcome = outcomes[min(self.calls - 1, len(outcomes) - 1)]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        async def generate_image(self, model_id, prompt, **kwargs):
            return await self._next()

        async def generate_video(self, model_id, prompt, **kwargs):
            return await self._next()

        async def generate_speech(self, model_id, text, **kwargs):
            return await self._next()

    return _P()


def _make_provider(exc):
    class _P:
        PROVIDER_NAME = "fake"
        username = "u1"
        supports_image_generation = True
        supports_video_generation = True
        supports_tts = True

        async def generate_image(self, model_id, prompt, **kwargs):
            raise exc

        async def generate_video(self, model_id, prompt, **kwargs):
            raise exc

        async def generate_speech(self, model_id, text, **kwargs):
            raise exc

    return _P()


def _patch_common(monkeypatch, provider, *, retry_count=1):
    account = _FakeAccountClient(provider)

    async def acquire(*args, **kwargs):
        return provider, "fake", account

    monkeypatch.setattr(main.config.Config, "get_global_retry_count", lambda: retry_count)
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda p: retry_count)
    monkeypatch.setattr(main.config.Config, "get_provider_extra_retry_status_codes", lambda p: set())
    monkeypatch.setattr(main.config.Config, "context_overflow_not_retryable_enabled", lambda: True)
    monkeypatch.setattr(main.config.Config, "get_non_retryable_parameter_errors", lambda: {})
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["fake"])
    monkeypatch.setattr(ModelClientPool, "get_model_info", lambda model: {"output_modalities": ["image", "video"]})
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider_name, model: "upstream-m")
    monkeypatch.setattr(main, "_finalize_channel_attempt_log", _async_none)
    monkeypatch.setattr(main, "_build_channel_attempt_log", lambda **kw: {"attempt_key": "k", "attempt_no": 1})
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kw: None)
    monkeypatch.setattr(ModelClientPool, "record_channel_failure", lambda *a, **kw: None)
    monkeypatch.setattr(ModelClientPool, "record_account_failure", lambda *a, **kw: None)
    monkeypatch.setattr(ModelClientPool, "record_account_success", lambda *a, **kw: None)
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", lambda name: None)
    return account


async def _async_none(**kw):
    return None


def _run(coro):
    return asyncio.run(coro)


def test_media_5xx_exhausts_to_unified_429(monkeypatch):
    provider = _make_provider(HTTPException(status_code=502, detail="upstream boom"))
    account = _patch_common(monkeypatch, provider, retry_count=1)

    with pytest.raises(HTTPException) as exc:
        _run(main._media_generation_with_retry(
            kind="image", model="m", prompt="p", body={"model": "m", "prompt": "p"},
            api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/images/generations",
            client_type="openai",
        ))

    assert exc.value.status_code == 429
    assert exc.value.detail["kind"] == "upstream_exception"
    assert exc.value.detail["upstream_status"] == 502
    assert account.released is True


def test_media_inner_retry_reuses_same_account_before_outer_route(monkeypatch):
    provider = _make_sequence_provider([
        HTTPException(status_code=502, detail="first failed"),
        {"created": 1, "data": [{"url": "ok"}]},
    ])
    _patch_common(monkeypatch, provider, retry_count=1)
    acquire_calls = 0

    async def acquire(*args, **kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return provider, "fake", _FakeAccountClient(provider)

    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)

    result = _run(main._media_generation_with_retry(
        kind="image", model="m", prompt="p", body={"model": "m", "prompt": "p"},
        api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/images/generations",
        client_type="openai",
    ))

    assert result["data"][0]["url"] == "ok"
    assert provider.calls == 2
    assert acquire_calls == 1


def test_media_extra_retry_status_retries_same_account(monkeypatch):
    provider = _make_sequence_provider([
        HTTPException(status_code=408, detail="timeout response"),
        {"created": 1, "data": [{"url": "ok"}]},
    ])
    _patch_common(monkeypatch, provider, retry_count=1)
    monkeypatch.setattr(main.config.Config, "get_provider_extra_retry_status_codes", lambda p: {408})

    result = _run(main._media_generation_with_retry(
        kind="image", model="m", prompt="p", body={"model": "m", "prompt": "p"},
        api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/images/generations",
        client_type="openai",
    ))

    assert result["data"][0]["url"] == "ok"
    assert provider.calls == 2


def test_media_unconfigured_404_switches_outer_candidate(monkeypatch):
    """普通 404 不做同账号内层重试，但外层会重新 acquire 候选。"""
    provider = _make_sequence_provider([
        HTTPException(status_code=404, detail="upstream route not found"),
        {"created": 1, "data": [{"url": "ok"}]},
    ])
    _patch_common(monkeypatch, provider, retry_count=1)
    acquire_calls = 0

    async def acquire(*args, **kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return provider, "fake", _FakeAccountClient(provider)

    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)

    result = _run(main._media_generation_with_retry(
        kind="image", model="m", prompt="p", body={"model": "m", "prompt": "p"},
        api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/images/generations",
        client_type="openai",
    ))

    assert result["data"][0]["url"] == "ok"
    assert acquire_calls == 2
    assert provider.calls == 2
    decision = rp.classify_failure(rp.FailureInput(
        is_http_exception=True, status_code=404, detail="upstream route not found",
        downstream_started=False, stream=False, cancelled=False,
        candidates_remaining=True, upstream_started=True,
        non_retryable_override=False, transient_exception=False,
    ))
    assert decision.action == rp.FailureAction.RETRY


def test_media_context_overflow_canonicalized(monkeypatch):
    """上游 413 超限经媒体出口归一成统一 code/message/status（不透传上游原文）。"""
    detail = '{"error":{"message":"输入太长（估算 327107，上限 256000），请删减后重试。","type":"invalid_request_error","code":"input_token_limit_exceeded"}}'
    provider = _make_provider(HTTPException(status_code=413, detail=detail))
    _patch_common(monkeypatch, provider, retry_count=0)

    with pytest.raises(HTTPException) as exc:
        _run(main._media_generation_with_retry(
            kind="image", model="m", prompt="p", body={"model": "m", "prompt": "p"},
            api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/images/generations",
            client_type="openai",
        ))

    # canonical 命中 input_too_long：客户端拿到统一 400 + 固定 message，不含上游原文/前缀。
    assert exc.value.status_code == 400
    err = exc.value.detail["error"]
    assert err["code"] == "input_too_long"
    assert err["message"] == "Input tokens exceed the maximum allowed limit"
    assert "输入太长" not in err["message"]


def test_tts_5xx_exhausts_to_unified_429(monkeypatch):
    provider = _make_provider(HTTPException(status_code=503, detail="upstream boom"))
    account = _patch_common(monkeypatch, provider, retry_count=1)

    with pytest.raises(HTTPException) as exc:
        _run(main._tts_generation_with_retry(
            model="m", text="hi", body={"model": "m", "input": "hi"},
            api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/audio/speech",
            client_type="openai",
        ))

    assert exc.value.status_code == 429
    assert exc.value.detail["kind"] == "upstream_exception"
    assert account.released is True


def test_tts_unconfigured_404_switches_outer_candidate(monkeypatch):
    provider = _make_sequence_provider([
        HTTPException(status_code=404, detail="voice route not found"),
        b"audio",
    ])
    _patch_common(monkeypatch, provider, retry_count=1)
    acquire_calls = 0

    async def acquire(*args, **kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return provider, "fake", _FakeAccountClient(provider)

    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)

    result = _run(main._tts_generation_with_retry(
        model="m", text="hi", body={"model": "m", "input": "hi"},
        api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/audio/speech",
        client_type="openai",
    ))

    assert result == b"audio"
    assert acquire_calls == 2
    assert provider.calls == 2


def test_no_available_account_bubbles_without_retry(monkeypatch):
    """acquire 阶段就无账号：NoAvailableAccountError 原样冒泡，不写日志、不重试。"""

    async def acquire(*args, **kwargs):
        raise NoAvailableAccountError(status_code=429, detail="no account")

    provider = _make_provider(HTTPException(status_code=500, detail="x"))
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda p: 1)
    monkeypatch.setattr(main.config.Config, "context_overflow_not_retryable_enabled", lambda: True)
    monkeypatch.setattr(main.config.Config, "get_non_retryable_parameter_errors", lambda: {})
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["fake"])
    monkeypatch.setattr(ModelClientPool, "get_model_info", lambda model: {})
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(main, "_finalize_channel_attempt_log", _async_none)
    monkeypatch.setattr(main, "_build_channel_attempt_log", lambda **kw: {"attempt_key": "k", "attempt_no": 1})
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kw: None)
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", lambda name: None)

    with pytest.raises(NoAvailableAccountError):
        _run(main._media_generation_with_retry(
            kind="image", model="m", prompt="p", body={"model": "m", "prompt": "p"},
            api_key=None, api_key_name=None, request_headers={}, client_request_path="/v1/images/generations",
            client_type="openai",
        ))
