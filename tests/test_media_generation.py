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
            "requested_model": "img-model",
            "routed_model": "img-model",
            "public_model_id": "img-model",
            "upstream_model_id": "upstream-img-model",
        }

    async def release(self):
        self.released = True


class _FakeProvider:
    PROVIDER_NAME = "fake"
    username = "u1"
    supports_image_generation = True
    supports_video_generation = True

    def __init__(self):
        self.calls = []

    async def generate_image(self, model_id, prompt, **kwargs):
        self.calls.append(("image", model_id, prompt, kwargs))
        return {"created": 1, "data": [{"url": "https://example.test/image.png"}]}

    async def generate_video(self, model_id, prompt, **kwargs):
        self.calls.append(("video", model_id, prompt, kwargs))
        return {"created": 1, "data": [{"url": "https://example.test/video.mp4"}]}


class _FakeRequest:
    def __init__(self, body, path="/v1/images/generations"):
        self._body = body
        self.headers = {}
        self.url = type("URL", (), {"path": path})()

    async def json(self):
        return self._body


def test_media_endpoint_requires_model(monkeypatch):
    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._media_generation_endpoint("image", _FakeRequest({"prompt": "p"}), "Bearer k"))
    assert exc.value.status_code == 400
    assert "model" in exc.value.detail


def test_media_endpoint_requires_prompt(monkeypatch):
    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._media_generation_endpoint("video", _FakeRequest({"model": "m"}, "/v1/videos/generations"), "Bearer k"))
    assert exc.value.status_code == 400
    assert "prompt" in exc.value.detail


def test_image_generation_forwards_parameters_and_releases(monkeypatch):
    provider = _FakeProvider()
    account = _FakeAccountClient(provider)

    async def acquire(*args, **kwargs):
        assert kwargs["operation"] == "image_generation"
        return provider, "fake", account

    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    monkeypatch.setattr(main, "_acquire_api_key_limit", lambda request, api_key: _async_return(None))
    monkeypatch.setattr(main, "_release_api_key_limit", lambda request: _async_return(None))
    monkeypatch.setattr(main, "_api_key_name", lambda api_key: _async_return("key"))
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: 0)
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["fake"])
    monkeypatch.setattr(ModelClientPool, "get_model_info", lambda model: {"output_modalities": ["image", "video"]})
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider_name, model: "upstream-img-model")
    monkeypatch.setattr(main, "_schedule_log_request", lambda data: None)
    monkeypatch.setattr(main, "_schedule_finalize_request_log", lambda route_info, data: None)

    response = asyncio.run(main._media_generation_endpoint("image", _FakeRequest({"model": "img-model", "prompt": "draw", "size": "512x512", "quality": "hd", "n": 2}), "Bearer k"))

    assert response.status_code == 200
    assert account.released is True
    kind, model_id, prompt, kwargs = provider.calls[0]
    assert kind == "image"
    assert model_id == "upstream-img-model"
    assert prompt == "draw"
    assert kwargs["size"] == "512x512"
    assert kwargs["quality"] == "hd"
    assert kwargs["n"] == 2


def test_video_generation_forwards_parameters_and_releases(monkeypatch):
    provider = _FakeProvider()
    account = _FakeAccountClient(provider)

    async def acquire(*args, **kwargs):
        assert kwargs["operation"] == "video_generation"
        return provider, "fake", account

    monkeypatch.setattr(main, "_extract_api_key", lambda *args, **kwargs: _async_return("k"))
    monkeypatch.setattr(main, "_prepare_client_request_body", lambda body, api_key, headers, path: _async_return((body, "openai")))
    monkeypatch.setattr(main, "_acquire_api_key_limit", lambda request, api_key: _async_return(None))
    monkeypatch.setattr(main, "_release_api_key_limit", lambda request: _async_return(None))
    monkeypatch.setattr(main, "_api_key_name", lambda api_key: _async_return("key"))
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: 0)
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["fake"])
    monkeypatch.setattr(ModelClientPool, "get_model_info", lambda model: {"output_modalities": ["image", "video"]})
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider_name, model: "upstream-img-model")
    monkeypatch.setattr(main, "_schedule_log_request", lambda data: None)
    monkeypatch.setattr(main, "_schedule_finalize_request_log", lambda route_info, data: None)

    response = asyncio.run(main._media_generation_endpoint("video", _FakeRequest({"model": "img-model", "prompt": "animate", "seconds": 6, "size": "720x1280"}, "/v1/videos/generations"), "Bearer k"))

    assert response.status_code == 200
    assert account.released is True
    kind, model_id, prompt, kwargs = provider.calls[0]
    assert kind == "video"
    assert model_id == "upstream-img-model"
    assert prompt == "animate"
    assert kwargs["seconds"] == 6
    assert kwargs["size"] == "720x1280"


def _async_return(value):
    async def _coro():
        return value
    return _coro()
