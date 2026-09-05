import asyncio
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main
from rate_limiter import ModelClientPool


class _FakeAccountClient:
    def __init__(self, provider):
        self.provider = provider
        self.username = "u1"
        self.released = False
        self.last_route_info = {
            "provider": "fake",
            "account": "u1",
            "requested_model": "tts-model",
            "routed_model": "tts-model",
            "public_model_id": "tts-model",
            "upstream_model_id": "upstream-tts-model",
        }

    async def release(self):
        self.released = True


class _FakeProvider:
    PROVIDER_NAME = "fake"
    username = "u1"
    supports_tts = True

    def __init__(self):
        self.calls = []

    async def generate_speech(self, model_id, text, **kwargs):
        self.calls.append(("speech", model_id, text, kwargs))
        return {"audio": "fake-b64", "content_type": "audio/mpeg"}


class _FakeRequest:
    def __init__(self, body, path="/v1/audio/speech"):
        self._body = body
        self.headers = {}
        self.url = type("URL", (), {"path": path})()

    async def json(self):
        return self._body


def _async_return(value):
    async def _coro():
        return value
    return _coro()


def test_tts_endpoint_requires_model(monkeypatch):
    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._tts_endpoint(_FakeRequest({"input": "hello"}), "Bearer k"))
    assert exc.value.status_code == 400
    assert "model" in exc.value.detail


def test_tts_endpoint_requires_input(monkeypatch):
    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._tts_endpoint(_FakeRequest({"model": "m"}), "Bearer k"))
    assert exc.value.status_code == 400
    assert "input" in exc.value.detail


def test_tts_generation_forwards_parameters_and_releases(monkeypatch):
    provider = _FakeProvider()
    account = _FakeAccountClient(provider)

    async def acquire(*args, **kwargs):
        assert kwargs["operation"] == "tts_generation"
        return provider, "fake", account

    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    monkeypatch.setattr(main, "_acquire_api_key_limit", lambda request, api_key: _async_return(None))
    monkeypatch.setattr(main, "_release_api_key_limit", lambda request: _async_return(None))
    monkeypatch.setattr(main, "_api_key_name", lambda api_key: _async_return("key"))
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: 0)
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["fake"])
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider_name, model: "upstream-tts-model")
    monkeypatch.setattr(main, "_schedule_log_request", lambda data: None)
    monkeypatch.setattr(main, "_schedule_finalize_request_log", lambda route_info, data: None)

    response = asyncio.run(main._tts_endpoint(
        _FakeRequest({
            "model": "tts-model",
            "input": "hello",
            "voice": "alloy",
            "response_format": "mp3",
            "speed": 1.0,
        }), "Bearer k"))

    assert response.status_code == 200
    assert account.released is True
    kind, model_id, text, kwargs = provider.calls[0]
    assert kind == "speech"
    assert model_id == "upstream-tts-model"
    assert text == "hello"
    assert kwargs["voice"] == "alloy"
    assert kwargs["response_format"] == "mp3"
    assert kwargs["speed"] == 1.0


def test_tts_result_response_bytes(monkeypatch):
    result = b"\x00\x01\x02"
    response = main._tts_result_response(result, "mp3")
    assert response.status_code == 200
    assert response.body == result
    assert response.media_type == "audio/mpeg"


def test_tts_result_response_dict(monkeypatch):
    result = {"audio": "fake-b64", "content_type": "audio/mpeg"}
    response = main._tts_result_response(result, "mp3")
    assert response.status_code == 200
    assert response.body == b'{"audio":"fake-b64","content_type":"audio/mpeg"}'


def test_tts_result_response_base64_audio(monkeypatch):
    import base64
    audio_bytes = b"\x00\x01\x02\x03"
    result = {"audio": base64.b64encode(audio_bytes).decode("ascii"), "content_type": "audio/wav"}
    response = main._tts_result_response(result, "wav")
    assert response.status_code == 200
    assert response.body == audio_bytes
    assert response.media_type == "audio/wav"
