from providers.cloudflare import CloudflareProvider
from rate_limiter import ModelClientPool


def test_cloudflare_builds_account_scoped_urls_from_account_id():
    provider = CloudflareProvider(
        "user@example.com",
        "cf-token",
        account_id="acc123",
        base_url="",
        models_path="",
    )

    assert provider.is_init() is True
    assert provider.base_url == "https://api.cloudflare.com/client/v4/accounts/acc123/ai/v1"
    assert provider._chat_url() == "https://api.cloudflare.com/client/v4/accounts/acc123/ai/v1/chat/completions"
    assert provider.models_path == "https://api.cloudflare.com/client/v4/accounts/acc123/ai/models/search"
    assert provider._url(provider.models_path) == "https://api.cloudflare.com/client/v4/accounts/acc123/ai/models/search"


def test_cloudflare_extracts_nested_models_and_prefers_cf_model_ids():
    raw = CloudflareProvider._extract_raw_models({
        "success": True,
        "result": {
            "models": [
                {"id": "internal-id", "name": "@cf/meta/llama-3.1-8b-instruct"},
                {"id": "@cf/moonshotai/kimi-k2.6", "name": "Kimi K2"},
            ]
        },
    })
    provider = CloudflareProvider("user@example.com", "cf-token", account_id="acc123")

    models = provider._normalize_models(raw)

    assert [m["id"] for m in models] == [
        "@cf/meta/llama-3.1-8b-instruct",
        "@cf/moonshotai/kimi-k2.6",
    ]
    assert models[0]["cloudflare_id"] == "internal-id"


def _raw_paid_model(model_id: str) -> dict:
    return {
        "id": model_id,
        "name": model_id,
        "properties": [
            {"property_id": "require_workers_paid", "value": "true"},
            {"property_id": "context_window", "value": "262144"},
        ],
    }


def test_cloudflare_marks_paid_only_models_but_keeps_them_in_normalize():
    """normalize 阶段只给付费模型打标，不剔除——剔除由 fetch 时的账号权限探测决定。"""
    raw = [
        {"id": "@cf/openai/gpt-oss-120b", "name": "@cf/openai/gpt-oss-120b"},
        _raw_paid_model("@cf/moonshotai/kimi-k2.7-code"),
    ]
    provider = CloudflareProvider("user@example.com", "cf-token", account_id="acc123")

    models = provider._normalize_models(raw)

    assert [m["id"] for m in models] == [
        "@cf/openai/gpt-oss-120b",
        "@cf/moonshotai/kimi-k2.7-code",
    ]
    assert models[0].get("require_workers_paid") is None
    assert models[1].get("require_workers_paid") is True


def test_cloudflare_filter_drops_models_the_account_has_no_access_to(monkeypatch):
    """探测返回 403/code 5035 的付费模型被剔除；有权访问的保留；非套餐门禁的失败保留。"""
    import asyncio
    import providers.cloudflare as cf_mod

    raw = [
        {"id": "@cf/openai/gpt-oss-120b", "name": "@cf/openai/gpt-oss-120b"},
        _raw_paid_model("@cf/moonshotai/kimi-k2.7-code"),  # 账号无权 -> 剔除
        _raw_paid_model("@cf/moonshotai/kimi-k2.6"),        # 账号有权 -> 保留
        _raw_paid_model("@cf/deepseek/deepseek-r1"),        # 5xx 不确定 -> 保留
    ]
    provider = CloudflareProvider("user@example.com", "cf-token", account_id="acc123")
    normalized = provider._normalize_models(raw)

    probe_results = {
        "@cf/moonshotai/kimi-k2.7-code": (False, "Workers Free plan 不可用 (403 / code 5035)"),
        "@cf/moonshotai/kimi-k2.6": (True, ""),
        "@cf/deepseek/deepseek-r1": (True, "HTTP 500，非套餐门禁，保留"),
    }

    async def fake_probe(self, session, upstream_model_id):
        return probe_results[upstream_model_id]

    monkeypatch.setattr(cf_mod.CloudflareProvider, "_probe_model_access", fake_probe)

    # _make_session 是基类方法；这里不需要真实网络，探测已被 mock，直接给个最小替身。
    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(provider, "_make_session", lambda: _FakeSession())

    kept = asyncio.run(provider._filter_models_by_account_access(normalized))

    assert [m["id"] for m in kept] == [
        "@cf/openai/gpt-oss-120b",
        "@cf/moonshotai/kimi-k2.6",
        "@cf/deepseek/deepseek-r1",
    ]




def test_cloudflare_provider_maps_public_model_to_upstream_for_direct_account_tests(monkeypatch):
    # 渠道=内存对象后不再有全局 _model_upstream_map；resolve_upstream_id 现读 Channel.models。
    # 直接 patch 访问器等价覆盖单模型映射。
    monkeypatch.setattr(
        ModelClientPool,
        "resolve_upstream_id",
        classmethod(
            lambda cls, provider, model_id: "@cf/moonshotai/kimi-k2.6"
            if (provider, model_id) == ("cloudflare", "kimi-k2.6")
            else model_id
        ),
    )
    provider = CloudflareProvider("user@example.com", "cf-token", account_id="acc123")

    payload = provider._build_openai_payload("kimi-k2.6", [{"role": "user", "content": "hi"}], False)

    assert payload["model"] == "@cf/moonshotai/kimi-k2.6"
