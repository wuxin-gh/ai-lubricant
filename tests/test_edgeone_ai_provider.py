from providers.edgeone_ai import EdgeOneAIProvider
from rate_limiter import ModelClientPool


def test_edgeone_sends_upstream_model_mapped_from_account_model(monkeypatch):
    provider = EdgeOneAIProvider("edge-demo", model_name="deepseek-v4-flash")
    # 渠道=内存对象后不再有全局 _model_upstream_map；patch 访问器等价覆盖单模型映射。
    monkeypatch.setattr(
        ModelClientPool,
        "resolve_upstream_id",
        classmethod(lambda cls, provider, model_id: "@tx/deepseek-ai/deepseek-v4"
                    if (provider, model_id) == ("edgeone-ai", "deepseek-v4-flash") else model_id),
    )

    payload = provider._build_openai_payload("wrong-request-model", [{"role": "user", "content": "hi"}], False)

    assert payload["model"] == "@tx/deepseek-ai/deepseek-v4"


def test_edgeone_account_model_check_uses_system_model_name():
    provider = EdgeOneAIProvider("edge-demo", model_name="deepseek-v4-flash")

    assert provider.supports_system_model("deepseek-v4-flash") is True
    assert provider.supports_system_model("@tx/deepseek-ai/deepseek-v4") is False
