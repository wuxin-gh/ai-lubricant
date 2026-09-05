"""同协议 Anthropic 直通流的 usage 兜底与空流回归测试。"""

import asyncio
import json

import pytest

from providers.base import IncompleteStreamError
from providers.custom import CustomProvider


class AnthropicPassthroughProvider(CustomProvider):
    def __init__(self, events):
        super().__init__(
            "user",
            "key",
            provider_name="anthropic-test",
            base_url="https://example.test",
            protocol="anthropic",
            upstream_stream=True,
        )
        self.events = events

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        for event in self.events:
            yield event


def _event(event_type, payload):
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _collect(provider):
    async def run():
        return [
            chunk async for chunk in provider.chat_anthropic(
                "claude-opus-5",
                [{"role": "user", "content": "inspect the config"}],
                stream=True,
                _raw_anthropic_body={
                    "model": "claude-opus-5",
                    "messages": [{"role": "user", "content": "inspect the config"}],
                    "stream": True,
                },
            )
        ]

    return asyncio.run(run())


def test_passthrough_zero_usage_estimates_from_text_and_tool_input():
    events = [
        _event("message_start", {
            "type": "message",
            "message": {
                "id": "msg_test",
                "role": "assistant",
                "content": [],
                "usage": {"input_tokens": 0, "output_tokens": 0},
            },
        }),
        _event("content_block_start", {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": "检查完成"},
        }),
        _event("content_block_delta", {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "，可以继续。"},
        }),
        _event("content_block_start", {
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "tool_use", "id": "toolu_test", "name": "Bash", "input": {}},
        }),
        _event("content_block_delta", {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '{"command":"pwd"}'},
        }),
        _event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 0},
        }),
        _event("message_stop", {"type": "message_stop"}),
    ]

    chunks = _collect(AnthropicPassthroughProvider(events))
    done = next(chunk for chunk in chunks if isinstance(chunk, dict) and chunk.get("_passthrough_done"))

    assert done["message_id"] == "msg_test"
    assert done["usage"]["prompt_tokens"] > 0
    assert done["usage"]["completion_tokens"] > 0
    assert any(isinstance(chunk, str) and "partial_json" in chunk for chunk in chunks)


def test_passthrough_empty_zero_usage_still_raises_incomplete_stream():
    events = [
        _event("message_start", {
            "type": "message",
            "message": {
                "id": "msg_empty",
                "role": "assistant",
                "content": [],
                "usage": {"input_tokens": 0, "output_tokens": 0},
            },
        }),
        _event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 0},
        }),
        _event("message_stop", {"type": "message_stop"}),
    ]

    with pytest.raises(IncompleteStreamError):
        _collect(AnthropicPassthroughProvider(events))
