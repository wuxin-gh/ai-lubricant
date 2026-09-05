"""管理端测试面板的零 completion 兜底回归测试。

历史 bug：管理端 /admin 渠道测试面板只用 OpenAI `delta.content`（final_text）判定
"是否有输出"，且非流式校验只看当前 chunk，导致 Anthropic / Responses / Gemini /
reasoning / tool_calls 等协议在上游 usage.completion_tokens=0 时被误判为空响应，
抛 502 `upstream response usage reports zero completion tokens`。

修复后：判定统一走协议无关的 `_has_first_token_content`，有真实输出就删零 usage、
交给后续统计估算兜底，不判失败；只有真正无内容 + 零 completion 才抛 502。
"""
import pytest
from fastapi import HTTPException

from admin import _admin_validate_upstream_usage_payload


# ---------------------------------------------------------------------------
# 有真实内容 + usage.completion_tokens=0 → 删 usage、不抛（各协议）
# ---------------------------------------------------------------------------

def test_openai_content_with_zero_completion_drops_usage():
    payload = {
        "choices": [{"message": {"content": "hi"}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
    }
    _admin_validate_upstream_usage_payload(payload)
    assert "usage" not in payload


def test_openai_reasoning_only_with_zero_completion_drops_usage():
    # 仅 reasoning_content（无 content）也算真实输出。
    payload = {
        "choices": [{"delta": {"reasoning_content": "思考中"}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
    }
    _admin_validate_upstream_usage_payload(payload)
    assert "usage" not in payload


def test_openai_tool_calls_only_with_zero_completion_drops_usage():
    payload = {
        "choices": [{"delta": {"tool_calls": [{"id": "1", "function": {"name": "f"}}]}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
    }
    _admin_validate_upstream_usage_payload(payload)
    assert "usage" not in payload


def test_anthropic_content_block_with_zero_output_tokens_drops_usage():
    # Anthropic 直通：content 走 content_block_delta.text，usage 用 output_tokens。
    payload = {
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "hello"},
        "usage": {"input_tokens": 5, "output_tokens": 0},
    }
    _admin_validate_upstream_usage_payload(payload)
    assert "usage" not in payload


def test_responses_output_item_with_zero_completion_drops_usage():
    # Responses API：内容走 response.output_item.added。
    payload = {
        "type": "response.output_item.added",
        "item": {"type": "message"},
        "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5},
    }
    _admin_validate_upstream_usage_payload(payload)
    assert "usage" not in payload


def test_content_and_usage_in_separate_chunks_uses_prior_had_content():
    # usage 帧与 content 帧分离：本 attempt 之前已见过内容 → had_content=True → 不抛。
    _admin_validate_upstream_usage_payload(
        {"usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5}},
        had_content=True,
    )


# ---------------------------------------------------------------------------
# 真正无内容 + 零 completion → 仍抛 502
# ---------------------------------------------------------------------------

def test_empty_response_with_zero_completion_raises():
    with pytest.raises(HTTPException) as exc:
        _admin_validate_upstream_usage_payload(
            {"choices": [{"message": {}}],
             "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5}},
        )
    assert exc.value.status_code == 502


def test_no_usage_payload_never_raises():
    # 没有 usage 字段时不判定（交给流末尾/估算兜底）。
    _admin_validate_upstream_usage_payload({"choices": [{"message": {}}]})


def test_nonzero_completion_never_raises():
    _admin_validate_upstream_usage_payload(
        {"choices": [{"message": {}}],
         "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}},
    )
