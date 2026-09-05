"""Tests for providers/gemini_proto.py (pure Gemini protocol helpers)."""
import json
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from providers.gemini_proto import (
    openai_messages_to_gemini,
    build_gemini_generation_config,
    build_gemini_tools,
    build_gemini_payload,
    parse_gemini_chunk,
    gemini_response_to_openai,
    gemini_usage_to_openai,
    normalize_gemini_models,
    gemini_stream_url,
    gemini_nonstream_url,
    gemini_chat_url,
    gemini_models_url,
    gemini_model_name,
    _tool_message_to_part,
)


def test_model_name_normalization():
    assert gemini_model_name("gemini-2.5-pro") == "models/gemini-2.5-pro"
    assert gemini_model_name("models/gemini-2.5-pro") == "models/gemini-2.5-pro"
    assert gemini_model_name("") == ""


def test_url_builders():
    base = "https://generativelanguage.googleapis.com"
    assert gemini_models_url(base) == f"{base}/v1beta/models"
    assert gemini_stream_url(base, "gemini-2.5-pro") == f"{base}/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"
    assert gemini_nonstream_url(base, "gemini-2.5-pro") == f"{base}/v1beta/models/gemini-2.5-pro:generateContent"
    # models/ prefix preserved
    assert gemini_stream_url(base, "models/gemini-2.5-pro") == f"{base}/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"
    # absolute models_path passthrough
    assert gemini_models_url(base, "https://other.example/models").startswith("https://other.example/models")


def test_chat_url_falls_back_to_official_form_when_path_empty():
    """path 留空（含仅空白）时行为与官方形态完全一致，老渠道不受影响。"""
    base = "https://generativelanguage.googleapis.com"
    for empty in (None, "", "   "):
        assert gemini_chat_url(base, "gemini-2.5-pro", True, "v1beta", empty) == gemini_stream_url(base, "gemini-2.5-pro")
        assert gemini_chat_url(base, "gemini-2.5-pro", False, "v1beta", empty) == gemini_nonstream_url(base, "gemini-2.5-pro")


def test_chat_url_template_placeholders():
    """通用/自建上游：{model} 与 {method} 占位符按流式与否替换。"""
    base = "https://relay.example.com"
    tpl = "/gemini/v1beta/models/{model}:{method}"
    assert gemini_chat_url(base, "gemini-2.5-pro", False, "v1beta", tpl) == \
        f"{base}/gemini/v1beta/models/gemini-2.5-pro:generateContent"
    assert gemini_chat_url(base, "gemini-2.5-pro", True, "v1beta", tpl) == \
        f"{base}/gemini/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"
    # 模板自带 models/ 前缀时，入参的 models/ 前缀不重复拼接
    assert gemini_chat_url(base, "models/gemini-2.5-pro", False, "v1beta", tpl) == \
        f"{base}/gemini/v1beta/models/gemini-2.5-pro:generateContent"
    # 缺省前导斜杠也能拼
    assert gemini_chat_url(base, "gemini-2.5-pro", False, "v1beta", "gemini/v1/models/{model}:{method}") == \
        f"{base}/gemini/v1/models/gemini-2.5-pro:generateContent"


def test_chat_url_template_without_placeholders_appends_model_and_method():
    """只写到模型前缀（无占位符）时补 /{model}:{method}。"""
    base = "https://relay.example.com"
    assert gemini_chat_url(base, "gemini-2.5-pro", False, "v1beta", "/proxy/v1beta/models") == \
        f"{base}/proxy/v1beta/models/gemini-2.5-pro:generateContent"
    assert gemini_chat_url(base, "gemini-2.5-pro", True, "v1beta", "/proxy/v1beta/models/") == \
        f"{base}/proxy/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"


def test_chat_url_absolute_template_passthrough():
    """模板写绝对 URL 时忽略 base_url，直连该地址。"""
    assert gemini_chat_url("https://ignored.example", "gemini-2.5-pro", False, "v1beta",
                           "https://other.example/v1/models/{model}:{method}") == \
        "https://other.example/v1/models/gemini-2.5-pro:generateContent"


def test_messages_system_instruction():
    messages = [
        {"role": "system", "content": "Be helpful."},
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hi there"},
    ]
    contents, system_instruction = openai_messages_to_gemini(messages)
    assert system_instruction == {"parts": [{"text": "Be helpful."}]}
    assert contents == [
        {"role": "user", "parts": [{"text": "Hello"}]},
        {"role": "model", "parts": [{"text": "Hi there"}]},
    ]


def test_messages_tool_calls_and_tool_role():
    messages = [
        {"role": "user", "content": "What's the weather?"},
        {"role": "assistant", "content": "", "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "weather", "arguments": json.dumps({"city": "sf"})}},
        ]},
        {"role": "tool", "name": "weather", "content": "sunny"},
    ]
    contents, _ = openai_messages_to_gemini(messages)
    assert contents[0]["role"] == "user"
    assert contents[1]["role"] == "model"
    assert contents[1]["parts"][0]["functionCall"]["name"] == "weather"
    assert contents[1]["parts"][0]["functionCall"]["args"] == {"city": "sf"}
    assert contents[2]["role"] == "function"
    assert contents[2]["parts"][0]["functionResponse"]["name"] == "weather"


def test_messages_image_url():
    messages = [{"role": "user", "content": [
        {"type": "text", "text": "describe"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,ABC"}},
    ]}]
    contents, _ = openai_messages_to_gemini(messages)
    parts = contents[0]["parts"]
    assert parts[0] == {"text": "describe"}
    assert parts[1]["inlineData"] == {"mimeType": "image/png", "data": "ABC"}


def test_generation_config_mapping():
    cfg = build_gemini_generation_config({
        "temperature": 0.7,
        "top_p": 0.9,
        "top_k": 40,
        "max_tokens": 1024,
        "stop": ["END"],
        "thinking_enabled": True,
        "thinking_budget": 2048,
    })
    # thinking_enabled/thinking_budget are non-standard params, no longer mapped to thinkingConfig
    assert cfg == {
        "temperature": 0.7,
        "topP": 0.9,
        "topK": 40,
        "maxOutputTokens": 1024,
        "stopSequences": ["END"],
    }


def test_build_gemini_tools():
    tools = [
        {"type": "function", "function": {"name": "get_weather", "description": "Get weather", "parameters": {"type": "object"}}},
        {"type": "not_function"},
    ]
    out = build_gemini_tools(tools)
    assert out == [{"functionDeclarations": [
        {"name": "get_weather", "description": "Get weather", "parameters": {"type": "object"}},
    ]}]
    assert build_gemini_tools(None) == []


def test_build_gemini_payload_assembly():
    payload = build_gemini_payload(
        "gemini-2.5-pro",
        [{"role": "user", "content": "hi"}],
        temperature=0.5,
        max_tokens=10,
    )
    assert payload["contents"] == [{"role": "user", "parts": [{"text": "hi"}]}]
    assert payload["generationConfig"] == {"temperature": 0.5, "maxOutputTokens": 10}
    assert "systemInstruction" not in payload


def test_parse_chunk_text_and_finish():
    chunk = {"candidates": [{"content": {"parts": [{"text": "Hello"}]}, "finishReason": "STOP"}], "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 2, "totalTokenCount": 7}}
    parsed = parse_gemini_chunk(chunk)
    assert parsed["content"] == "Hello"
    assert parsed["finish_reason"] == "stop"
    assert parsed["usage"] == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}


def test_parse_chunk_thinking_and_tool_call():
    chunk = {"candidates": [{"content": {"parts": [
        {"thoughtText": "thinking..."},
        {"functionCall": {"name": "do_x", "args": {"a": 1}}},
    ]}, "finishReason": "STOP"}]}
    parsed = parse_gemini_chunk(chunk)
    assert parsed["thinking"] == "thinking..."
    assert parsed["tool_calls"][0]["function"]["name"] == "do_x"
    assert json.loads(parsed["tool_calls"][0]["function"]["arguments"]) == {"a": 1}


def test_gemini_response_to_openai_assembly():
    obj = {"candidates": [{"content": {"parts": [{"text": "final answer"}]}, "finishReason": "STOP"}], "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 4, "totalTokenCount": 7}}
    resp = gemini_response_to_openai(obj, "gemini-2.5-pro", "chatcmpl-test")
    assert resp["id"] == "chatcmpl-test"
    assert resp["choices"][0]["message"]["content"] == "final answer"
    assert resp["choices"][0]["finish_reason"] == "stop"
    assert resp["usage"]["total_tokens"] == 7


def test_usage_with_cached_and_reasoning():
    usage = gemini_usage_to_openai({"promptTokenCount": 10, "candidatesTokenCount": 5, "cachedContentTokenCount": 3, "thoughtsTokenCount": 2})
    assert usage["prompt_tokens"] == 10
    assert usage["completion_tokens"] == 5
    assert usage["cached_tokens"] == 3
    assert usage["reasoning_tokens"] == 2
    assert usage["prompt_tokens_details"]["cached_tokens"] == 3
    assert usage["completion_tokens_details"]["reasoning_tokens"] == 2


def test_normalize_gemini_models():
    data = {"models": [
        {"name": "models/gemini-2.5-pro", "displayName": "Gemini 2.5 Pro"},
        {"name": "models/gemini-2.5-flash"},
    ]}
    models = normalize_gemini_models(data, owner="gemini")
    assert len(models) == 2
    assert models[0]["id"] == "gemini-2.5-pro"
    assert models[0]["name"] == "Gemini 2.5 Pro"
    assert models[0]["owned_by"] == "gemini"
    assert models[1]["id"] == "gemini-2.5-flash"


def test_tool_message_to_part_uses_name_not_id():
    """role=tool 缺 name 时，functionResponse.name 应兜底为 "tool"，
    绝不能把 tool_call_id 当函数名上报（会与 functionCall.name 错配）。"""
    part = _tool_message_to_part({"tool_call_id": "call_x", "content": "ok"})
    assert part["functionResponse"]["name"] == "tool"
    assert part["functionResponse"]["name"] != "call_x"


def test_tool_message_to_part_prefers_explicit_name():
    """带 name 时优先用真实工具名。"""
    part = _tool_message_to_part({"tool_call_id": "call_x", "name": "search", "content": "ok"})
    assert part["functionResponse"]["name"] == "search"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
