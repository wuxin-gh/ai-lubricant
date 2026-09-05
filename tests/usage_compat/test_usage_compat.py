"""兼容性回归：choices[].usage 修复不能破坏已有 usage 提取路径。

覆盖所有改动点仍在原有上游形态上保持行为不变：
- 顶层 usage（标准 OpenAI）
- Responses 风格 response.usage
- Anthropic 风格 message.usage
- SSE 注释行兜底（MiniMax）
- 累计型上游（每帧全量 usage）
- 零 completion 内容估算兜底
- 不重复计费（choice.usage 与顶层 usage 同帧只取顶层）
"""

import asyncio
import json

import admin
import main
from providers.base import BaseProvider
from providers.custom import CustomProvider


# ── 直接提取器 ──────────────────────────────────────────────

def test_top_level_usage_unchanged():
    payload = {"choices": [{"delta": {}}], "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}}

    assert BaseProvider.parse_sse_usage(f"data: {json.dumps(payload)}\n\n") == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
    assert CustomProvider._extract_usage(payload) == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
    assert main._extract_usage_payload(payload) == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
    assert admin._admin_extract_usage_payload(payload) == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}


def test_responses_nested_usage_unchanged():
    payload = {"type": "response.completed", "response": {"usage": {"input_tokens": 7, "output_tokens": 3}}}

    assert BaseProvider.parse_sse_usage(f"event: response.completed\ndata: {json.dumps(payload)}\n\n") == {"input_tokens": 7, "output_tokens": 3}
    assert CustomProvider._extract_usage(payload) == {"input_tokens": 7, "output_tokens": 3}
    assert main._extract_usage_payload(payload) == {"input_tokens": 7, "output_tokens": 3}
    assert admin._admin_extract_usage_payload(payload) == {"input_tokens": 7, "output_tokens": 3}


def test_message_nested_usage_unchanged():
    payload = {"choices": [{"message": {"content": "hi", "usage": {"prompt_tokens": 11, "completion_tokens": 2}}}]}

    assert main._extract_usage_payload(payload) == {"prompt_tokens": 11, "completion_tokens": 2}
    assert admin._admin_extract_usage_payload(payload) == {"prompt_tokens": 11, "completion_tokens": 2}


def test_comment_line_usage_fallback_unchanged():
    # data: chunk 的 usage 恒为 null，真实 token 在注释行；data 层无 usage 时仍走注释行兜底。
    event = (
        'data: {"choices":[{"delta":{"content":"hi"}}],"usage":null}\n'
        ': {"input_tokens":18930,"output_tokens":3}\n\n'
    )
    fallback = BaseProvider.parse_sse_usage(event)

    assert fallback == {"input_tokens": 18930, "output_tokens": 3}


def test_data_layer_usage_takes_priority_over_comment():
    event = (
        'data: {"choices":[{"delta":{"content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}\n'
        ': {"input_tokens":999,"output_tokens":999}\n\n'
    )

    assert BaseProvider.parse_sse_usage(event) == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}


def test_no_usage_returns_none_unchanged():
    payload = {"choices": [{"delta": {"content": "hi"}}]}

    assert BaseProvider.parse_sse_usage(f"data: {json.dumps(payload)}\n\n") is None
    assert CustomProvider._extract_usage(payload) is None
    assert main._extract_usage_payload(payload) is None
    assert admin._admin_extract_usage_payload(payload) is None


# ── 不重复计费：choice.usage 与顶层 usage 同帧时只取顶层 ────

def test_top_level_takes_priority_over_choice_level_in_same_frame():
    # 防止修复把 choice.usage 和顶层 usage 同时累计导致计数翻倍。
    payload = {
        "choices": [{"delta": {}, "finish_reason": "stop", "usage": {"prompt_tokens": 999, "completion_tokens": 999}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
    }

    # 写入流式 summary，merge_usage 以最后非零值为准，不应把 choice 的 999 叠加上来。
    summary = main._new_stream_summary()
    main._append_stream_summary(summary, f"data: {json.dumps(payload)}\n\n")

    assert summary["usage"]["prompt_tokens"] == 100
    assert summary["usage"]["completion_tokens"] == 50


# ── dict 累计型上游：最终下发累计 completion，不被首帧 0 锁死 ─

class KimiCumulativeProvider(BaseProvider):
    PROVIDER_NAME = "kimi-cumulative"

    def __init__(self, frames: list[dict]):
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


def _parse_usages(chunks: list) -> list[dict]:
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


def _collect(provider: BaseProvider) -> list:
    async def run():
        return [
            chunk
            async for chunk in provider.chat(
                "kimi-k2.6",
                [{"role": "user", "content": "hi"}],
                stream=True,
            )
        ]

    return asyncio.run(run())


def test_cumulative_top_level_usage_emits_final_value():
    # 顶层 usage 累计型（Kimi/vLLM），首帧 completion=0，最终 231。
    frames = [
        {"content": "", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209781, "completion_tokens": 0}},
        {"reasoning_content": " Now", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209782, "completion_tokens": 1}},
        {"content": " 现在添加", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209942, "completion_tokens": 161}},
        {"finish_reason": "stop", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 210012, "completion_tokens": 231}},
    ]

    usages = _parse_usages(_collect(KimiCumulativeProvider(frames)))

    assert usages
    assert usages[-1]["completion_tokens"] == 231
    assert usages[-1]["prompt_tokens"] == 209781


def test_zero_completion_estimates_by_content_unchanged():
    frames = [
        {"content": "Hello world", "tool_calls": [],
         "usage": {"prompt_tokens": 100, "total_tokens": 100, "completion_tokens": 0}},
        {"finish_reason": "stop", "tool_calls": [],
         "usage": {"prompt_tokens": 100, "total_tokens": 100, "completion_tokens": 0}},
    ]

    usages = _parse_usages(_collect(KimiCumulativeProvider(frames)))

    assert usages
    assert usages[-1]["completion_tokens"] > 0  # 走内容估算兜底，非 0
