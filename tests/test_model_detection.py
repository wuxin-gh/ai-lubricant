"""渠道实际模型探测（context-length probe）相关测试。

只覆盖纯函数部分：从上游错误文本中提取实际最大 token 数。
端到端的 HTTP 行为需要真实渠道，留给手动验证。
"""
import pytest

import admin


# ── _extract_context_limit ────────────────────────────────────────────────────

def test_extract_openai_style_context_limit():
    """典型 OpenAI/vLLM 风格报错：maximum context length of N tokens"""
    err = (
        "This model's maximum context length is 8192 tokens. "
        "However, your messages resulted in 9001 tokens."
    )
    assert admin._extract_context_limit(err) == 8192


def test_extract_value_error_with_completion_split():
    """报错里同时出现 input 与 completion 两段数字，应抽出上下文上限本身"""
    err = (
        "ValueError: Requested token count exceeds the model's maximum context "
        "length of 202752 tokens. You requested a total of 203584 tokens: 139584 "
        "tokens from the input messages and 64000 tokens for the completion."
    )
    assert admin._extract_context_limit(err) == 202752


def test_extract_anthropic_style():
    err = "request exceeded the maximum context length of 200000 tokens"
    assert admin._extract_context_limit(err) == 200000


def test_extract_context_window_phrase():
    err = "input exceeds the context window of 128000 tokens"
    assert admin._extract_context_limit(err) == 128000


def test_extract_chinese_phrase():
    err = "请求超过模型的最大上下文长度 200000 tokens"
    assert admin._extract_context_limit(err) == 200000


def test_extract_sse_payload_string():
    """SSE data 行里嵌套的 error.message 也能抽出"""
    err = 'data: {"error":{"message":"Requested token count exceeds the model\'s maximum context length of 65536 tokens.","type":"internal_server_error","code":500}}'
    assert admin._extract_context_limit(err) == 65536


def test_extract_returns_none_when_no_limit():
    err = "rate limit exceeded, please retry later"
    assert admin._extract_context_limit(err) is None


def test_extract_ignores_small_numbers():
    """像 500（HTTP code）这种小数字不应被当成上下文上限"""
    err = "internal error, status 500, retry in 30 seconds"
    assert admin._extract_context_limit(err) is None


def test_extract_handles_dict_error():
    """传入 dict 形式的 error（HTTPException.detail 可能是 dict）也能抽出"""
    err = {"error": {"message": "context length of 131072 tokens exceeded", "code": 400}}
    assert admin._extract_context_limit(err) == 131072


def test_extract_handles_empty():
    assert admin._extract_context_limit("") is None
    assert admin._extract_context_limit(None) is None
