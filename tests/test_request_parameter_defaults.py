import asyncio

import main
from message_utils import openai_messages_to_responses_payload
from providers.custom import CustomProvider


def test_global_model_metadata_supplies_max_tokens_and_tools(monkeypatch):
    default_tool = {
        "type": "function",
        "function": {
            "name": "lookup",
            "description": "lookup data",
            "parameters": {"type": "object", "properties": {}},
        },
    }

    async def fake_metadata(model):
        return {
            "max_tokens": 123,
            "parameters": {"tools": [default_tool], "temperature": 0.2},
        }, False

    monkeypatch.setattr(main.model_metadata, "get_model_metadata", fake_metadata)

    kwargs = asyncio.run(main._apply_global_request_defaults("demo", {"temperature": 0.8}))

    assert kwargs["max_tokens"] == 123
    assert kwargs["tools"] == [default_tool]
    # 客户端已显式给出的参数不被默认值覆盖
    assert kwargs["temperature"] == 0.8


def test_global_defaults_do_not_override_present_values(monkeypatch):
    async def fake_metadata(model):
        return {"max_tokens": 500, "parameters": {"max_tokens": 999, "top_p": 0.5}}, False

    monkeypatch.setattr(main.model_metadata, "get_model_metadata", fake_metadata)

    kwargs = asyncio.run(main._apply_global_request_defaults("demo", {"max_tokens": 42}))

    assert kwargs["max_tokens"] == 42
    assert kwargs["top_p"] == 0.5


def test_custom_provider_openai_payload_includes_defaulted_parameters():
    provider = CustomProvider("user", "key", provider_name="custom-a", base_url="https://example.test")
    kwargs = {"max_tokens": 9, "tools": [{"type": "function", "function": {"name": "tool_a"}}], "top_p": 0.7}

    payload = provider._build_openai_payload("upstream-model", [{"role": "user", "content": "hi"}], False, **kwargs)

    assert payload["max_tokens"] == 9
    assert payload["tools"] == kwargs["tools"]
    assert payload["top_p"] == 0.7


def test_anthropic_conversion_uses_defaulted_max_tokens_and_tools():
    provider = CustomProvider("user", "key", provider_name="custom-a", base_url="https://example.test", protocol="anthropic")
    tool = {
        "type": "function",
        "function": {"name": "search", "description": "", "parameters": {"type": "object", "properties": {}}},
    }
    payload = provider._build_openai_payload("claude-upstream", [{"role": "user", "content": "hi"}], False, max_tokens=321, tools=[tool])

    anthropic = provider._openai_request_to_anthropic(payload)

    assert anthropic["max_tokens"] == 321
    assert anthropic["tools"] == [{"name": "search", "description": "", "input_schema": {"type": "object", "properties": {}}}]


def test_responses_conversion_uses_defaulted_max_tokens_and_tools():
    tool = {
        "type": "function",
        "function": {"name": "search", "description": "", "parameters": {"type": "object", "properties": {}}},
    }

    payload = openai_messages_to_responses_payload(
        "gpt-upstream",
        [{"role": "user", "content": "hi"}],
        False,
        {"max_tokens": 77, "tools": [tool]},
    )

    assert payload["max_output_tokens"] == 77
    assert payload["tools"][0]["name"] == "search"
