import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main


# ==================== 顶层 model（OpenAI）====================

def test_openai_dict_top_level_model_always_rewritten():
    payload = {"id": "x", "model": "member-real", "choices": []}
    out = main._response_with_public_model(payload, "my-group")
    assert out["model"] == "my-group"


def test_openai_sse_string_top_level_model_rewritten():
    chunk = 'data: {"id":"x","model":"member-real","choices":[]}\n\n'
    out = main._stream_chunk_with_public_model(chunk, "my-group")
    assert '"model": "my-group"' in out
    assert "member-real" not in out


# ==================== 嵌套 model 仅在 rewrite_nested 时改写 ====================

def test_anthropic_message_start_nested_model_rewritten_when_nested_enabled():
    chunk = (
        "event: message_start\n"
        'data: {"type":"message_start","message":{"id":"m1","model":"member-real","role":"assistant"}}\n\n'
    )
    out = main._stream_chunk_with_public_model(chunk, "my-group", rewrite_nested=True)
    payload = json.loads(out.split("data:", 1)[1].strip())
    assert payload["message"]["model"] == "my-group"
    # event 行保持不变
    assert out.startswith("event: message_start\n")


def test_anthropic_message_start_nested_model_untouched_when_disabled():
    chunk = (
        "event: message_start\n"
        'data: {"type":"message_start","message":{"id":"m1","model":"member-real","role":"assistant"}}\n\n'
    )
    out = main._stream_chunk_with_public_model(chunk, "my-group", rewrite_nested=False)
    assert "member-real" in out
    assert "my-group" not in out


def test_responses_created_nested_model_rewritten_when_nested_enabled():
    chunk = 'data: {"type":"response.created","response":{"id":"r1","model":"member-real"}}\n\n'
    out = main._stream_chunk_with_public_model(chunk, "my-group", rewrite_nested=True)
    payload = json.loads(out.split("data:", 1)[1].strip())
    assert payload["response"]["model"] == "my-group"


def test_passthrough_body_nested_model_rewritten_when_nested_enabled():
    payload = {"_passthrough_anthropic": True, "body": {"id": "m1", "model": "member-real"}}
    out = main._response_with_public_model(payload, "my-group", rewrite_nested=True)
    assert out["body"]["model"] == "my-group"
    # 包裹字段保持
    assert out["_passthrough_anthropic"] is True


def test_passthrough_body_nested_model_untouched_when_disabled():
    payload = {"_passthrough_responses": True, "body": {"id": "r1", "model": "member-real"}}
    out = main._response_with_public_model(payload, "my-group", rewrite_nested=False)
    assert out["body"]["model"] == "member-real"


# ==================== 不改写时返回同一对象，避免无谓重排 ====================

def test_returns_same_object_when_nothing_changes():
    payload = {"type": "content_block_delta", "delta": {"text": "hi"}}
    out = main._response_with_public_model(payload, "my-group", rewrite_nested=True)
    assert out is payload
