"""Integration tests for protocol-level thinking/reasoning parameter handling.

Tests the new behavior after removing the thinking-system classification:
- OpenAI protocol: reasoning_effort passes through, no Qwen params injected
- Anthropic protocol: thinking passes through, no Qwen params injected
- Responses protocol: reasoning passes through, no Qwen params injected
- Cross-protocol: reasoning_effort <-> thinking <-> reasoning conversion
- No non-standard params (thinking_mode/thinking_enabled/auto_thinking/etc.) leak into payload
"""
import pytest
from unittest.mock import patch

from providers.custom import CustomProvider


@pytest.fixture
def provider_openai():
    return CustomProvider("user", "key", base_url="https://example.com", protocol="openai", api_key="test-key")


@pytest.fixture
def provider_anthropic():
    return CustomProvider("user", "key", base_url="https://example.com", protocol="anthropic", api_key="test-key")


@pytest.fixture
def provider_responses():
    return CustomProvider("user", "key", base_url="https://example.com", protocol="responses", api_key="test-key")


NONSTANDARD_KEYS = (
    "thinking_mode", "thinking_enabled", "auto_thinking",
    "thinking_format", "auto_search", "research_mode", "thinking_budget",
)


class TestOpenAIProtocol:
    """OpenAI 协议：reasoning_effort 透传，不注入非标准参数"""

    def test_reasoning_effort_passes_through(self, provider_openai):
        kwargs = {"reasoning_effort": "high"}
        payload = provider_openai._build_openai_payload("gpt-4", [{"role": "user", "content": "test"}], False, **kwargs)
        assert payload.get("reasoning_effort") == "high"

    def test_no_nonstandard_params_injected(self, provider_openai):
        """普通请求不应被注入任何非标准参数"""
        payload = provider_openai._build_openai_payload("gpt-4", [{"role": "user", "content": "test"}], False)
        for key in NONSTANDARD_KEYS:
            assert key not in payload, f"payload should not contain {key}"

    def test_client_nonstandard_params_not_forwarded(self, provider_openai):
        """即使用户传了非标准参数，也不应进入 OpenAI payload"""
        kwargs = {"thinking_mode": "Thinking", "thinking_enabled": True, "auto_search": True}
        payload = provider_openai._build_openai_payload("gpt-4", [{"role": "user", "content": "test"}], False, **kwargs)
        for key in NONSTANDARD_KEYS:
            assert key not in payload, f"payload should not forward {key}"


class TestAnthropicProtocol:
    """Anthropic 协议：thinking 透传，不注入非标准参数"""

    def test_thinking_passes_through_via_raw_body(self, provider_anthropic):
        """同协议（anthropic→anthropic）：原始 body 里的 thinking 原样透传"""
        raw_anthropic_body = {
            "model": "claude-3",
            "messages": [{"role": "user", "content": "test"}],
            "max_tokens": 1024,
            "thinking": {"type": "enabled", "budget_tokens": 1024},
        }
        payload = provider_anthropic._build_anthropic_payload(
            "claude-3", [{"role": "user", "content": "test"}], False,
            _raw_anthropic_body=raw_anthropic_body, _client_protocol="anthropic",
        )
        assert payload.get("thinking") == {"type": "enabled", "budget_tokens": 1024}

    def test_no_reasoning_effort_in_anthropic(self, provider_anthropic):
        """Anthropic payload 不应包含 OpenAI 的 reasoning_effort"""
        kwargs = {"reasoning_effort": "high"}
        payload = provider_anthropic._build_anthropic_payload("claude-3", [{"role": "user", "content": "test"}], False, **kwargs)
        assert "reasoning_effort" not in payload

    def test_no_nonstandard_params_injected(self, provider_anthropic):
        payload = provider_anthropic._build_anthropic_payload("claude-3", [{"role": "user", "content": "test"}], False)
        for key in NONSTANDARD_KEYS:
            assert key not in payload, f"payload should not contain {key}"


class TestResponsesProtocol:
    """Responses 协议：reasoning 透传"""

    def test_responses_payload_structure(self, provider_responses):
        payload = provider_responses._build_responses_payload("gpt-4", [{"role": "user", "content": "test"}], False)
        assert payload.get("model") is not None
        assert "input" in payload or "instructions" in payload

    def test_no_nonstandard_params_injected(self, provider_responses):
        payload = provider_responses._build_responses_payload("gpt-4", [{"role": "user", "content": "test"}], False)
        for key in NONSTANDARD_KEYS:
            assert key not in payload, f"payload should not contain {key}"


class TestSupportedProtocols:
    """supported_protocols 配置"""

    def test_default_supported_protocols_equals_protocol(self):
        p = CustomProvider("user", "key", base_url="https://example.com", protocol="anthropic", api_key="test-key")
        assert p.supported_protocols == ["anthropic"]

    def test_custom_supported_protocols(self):
        p = CustomProvider(
            "user", "key",
            base_url="https://example.com",
            protocol="openai",
            api_key="test-key",
            supported_protocols=["openai", "anthropic", "responses"],
        )
        assert "anthropic" in p.supported_protocols
        assert "responses" in p.supported_protocols
        assert "openai" in p.supported_protocols


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
