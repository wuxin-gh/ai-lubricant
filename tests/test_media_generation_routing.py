import asyncio

import rate_limiter
from rate_limiter import ModelClientPool


def test_media_metadata_uses_output_modalities_over_capabilities():
    metadata = {
        "output_modalities": ["image"],
        "capabilities": {"image_generation": False},
    }

    assert ModelClientPool._metadata_allows_operation(metadata, "image_generation") is True
    assert ModelClientPool._metadata_allows_operation({"output_modalities": ["text"]}, "image_generation") is False


def test_route_supports_operation_uses_route_and_model_output_modalities(monkeypatch):
    async def fake_metadata(model):
        return {"output_modalities": ["image"]}, False

    monkeypatch.setattr(rate_limiter, "get_model_metadata", fake_metadata)

    assert asyncio.run(ModelClientPool._route_supports_operation(
        {"provider": "p1", "extra_config": {"output_modalities": ["text"]}},
        "gpt-image-2",
        "image_generation",
    )) is False
    assert asyncio.run(ModelClientPool._route_supports_operation(
        {"provider": "p1", "extra_config": {"output_modalities": ["image"]}},
        "gpt-image-2",
        "image_generation",
    )) is True


def test_provider_supports_operation_checks_method_not_support_flag():
    class ImageProvider:
        supports_image_generation = False

        async def generate_image(self, model_id, prompt, **kwargs):
            return {}

    class FlagOnlyProvider:
        supports_image_generation = True

    assert ModelClientPool._provider_supports_operation(ImageProvider(), "image", "image_generation") is True
    assert ModelClientPool._provider_supports_operation(FlagOnlyProvider(), "flag", "image_generation") is False


def test_pick_operation_compatible_route_skips_default_chat_route(monkeypatch):
    async def fake_metadata(model):
        return {"output_modalities": ["image"]}, False

    monkeypatch.setattr(rate_limiter, "get_model_metadata", fake_metadata)

    class ChatProvider:
        pass

    class ImageProvider:
        async def generate_image(self, model_id, prompt, **kwargs):
            return {}

    chat_pool = type("Pool", (), {"clients": [type("Client", (), {"provider": ChatProvider()})()]})()
    image_pool = type("Pool", (), {"clients": [type("Client", (), {"provider": ImageProvider()})()]})()
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"chat": chat_pool, "image": image_pool})
    # 渠道=内存对象后 pick_operation_compatible_route 读 get_model_routes（内存扫描）。
    _routes = {
        "gpt-image-2": [
            {"provider": "chat", "extra_config": {"output_modalities": ["text"]}},
            {"provider": "image", "extra_config": {"output_modalities": ["image"]}},
        ]
    }
    monkeypatch.setattr(ModelClientPool, "get_model_routes", classmethod(lambda cls, m: [dict(r) for r in _routes.get(m, [])]))

    routes = [
        {"id": "default", "model": "gpt-image-2", "provider": "chat"},
        {"id": "image", "model": "gpt-image-2", "provider": "image"},
    ]

    route, diagnostics = asyncio.run(ModelClientPool.pick_operation_compatible_route("gpt-image-2", routes, "image_generation"))

    assert route["id"] == "image"
    assert diagnostics
    assert "default" in diagnostics[0]
