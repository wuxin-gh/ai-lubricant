"""choices[].usage 上游报文回归测试。"""

import asyncio
import json

import admin
import main
from providers.base import BaseProvider
from providers.custom import CustomProvider


UPSTREAM_USAGE = {
    "prompt_tokens": 145356,
    "completion_tokens": 90,
    "total_tokens": 145446,
    "cached_tokens": 1792,
    "completion_tokens_details": {"reasoning_tokens": 1},
    "prompt_tokens_details": {"cached_tokens": 1792},
}


def make_payload(*, usage=None) -> dict:
    choice = {
        "finish_reason": "tool_calls",
        "delta": {},
        "index": 0,
    }
    if usage is not None:
        choice["usage"] = usage
    return {
        "id": "e8b3d123232a4a8bb12c8b0048d81003",
        "model": "moonshotai/kimi-k3-free",
        "choices": [choice],
        "object": "chat.completion.chunk",
        "created": 1784507719,
    }


def make_sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


class RawSseProvider(BaseProvider):
    PROVIDER_NAME = "choice-usage-test"

    def __init__(self, frames: list[str]):
        super().__init__("user", "password")
        self.frames = frames

    async def init_auth(self, is_check: bool = False) -> bool:
        return True

    async def check_auth(self) -> bool:
        return True

    async def fetch_upstream_model_list(self):
        return []

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield {}
        for frame in self.frames:
            yield frame

    async def _do_non_stream_chat(self, model_id, messages, **kwargs):
        return {}


def collect(provider: BaseProvider) -> list:
    async def run():
        return [
            chunk
            async for chunk in provider.chat(
                "moonshotai/kimi-k3-free",
                [{"role": "user", "content": "hi"}],
                stream=True,
            )
        ]

    return asyncio.run(run())


def emitted_usages(chunks: list) -> list[dict]:
    usages = []
    for chunk in chunks:
        if not isinstance(chunk, str):
            continue
        for line in chunk.splitlines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            payload = json.loads(data)
            if isinstance(payload.get("usage"), dict):
                usages.append(payload["usage"])
    return usages


def test_base_stream_uses_choice_level_usage_instead_of_estimate():
    frames = [
        make_sse({
            "id": "e8b3d123232a4a8bb12c8b0048d81003",
            "model": "moonshotai/kimi-k3-free",
            "choices": [{
                "finish_reason": None,
                "delta": {"tool_calls": [{
                    "index": 0,
                    "id": "Read_72",
                    "type": "function",
                    "function": {"name": "Read", "arguments": "{\"file_path\":\"x\"}"},
                }]},
                "index": 0,
            }],
            "object": "chat.completion.chunk",
        }),
        make_sse(make_payload(usage=UPSTREAM_USAGE)),
        "data: [DONE]\n\n",
    ]

    usages = emitted_usages(collect(RawSseProvider(frames)))
    assert usages
    assert usages[-1]["prompt_tokens"] == 145356
    assert usages[-1]["completion_tokens"] == 90
    assert usages[-1]["total_tokens"] == 145446
    assert usages[-1]["cached_tokens"] == 1792
    assert usages[-1]["reasoning_tokens"] == 1


def test_all_main_extractors_read_choice_level_usage():
    payload = make_payload(usage=UPSTREAM_USAGE)

    assert BaseProvider.parse_sse_usage(make_sse(payload)) == UPSTREAM_USAGE
    assert CustomProvider._extract_usage(payload) == UPSTREAM_USAGE
    assert main._extract_usage_payload(payload) == UPSTREAM_USAGE
    assert main._usage_from_stream_chunk(make_sse(payload))["completion_tokens"] == 90
    assert admin._admin_extract_usage_payload(payload) == UPSTREAM_USAGE


def test_stream_summary_reads_choice_level_usage():
    summary = main._new_stream_summary()
    main._append_stream_summary(summary, make_sse(make_payload(usage=UPSTREAM_USAGE)))

    assert summary["usage"]["prompt_tokens"] == 145356
    assert summary["usage"]["completion_tokens"] == 90
