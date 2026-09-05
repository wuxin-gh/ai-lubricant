"""流式累计 usage 兜底回归测试。

历史 bug：累计型上游（如 Kimi vLLM 每帧带全量 usage、completion_tokens 逐帧递增），
走 BaseProvider dict 路径时，中间帧的 usage 被提前下发并把 seen_usage_output 锁死，
导致最终累计的 completion（如 231）丢失，日志/DB 记成 0 或早期小值。

修复后：dict 路径只累计 usage，流结束后统一补发最终累计值（completion 仍为 0 时再按内容估算）。
"""
import asyncio
import json

from providers.base import BaseProvider


class _KimiStreamProvider(BaseProvider):
    """模拟 Kimi/vLLM：每帧带累计 usage、reasoning/content delta，末尾带纯 usage 帧。"""

    PROVIDER_NAME = "kimi-fake"

    def __init__(self, frames: list[dict]):
        super().__init__("user", "password")
        self._frames = frames

    async def init_auth(self, is_check: bool = False) -> bool:
        return True

    async def check_auth(self) -> bool:
        return True

    async def fetch_upstream_model_list(self):
        return []

    async def _do_stream_chat(self, model_id, messages, **kwargs):
        yield {}
        for frame in self._frames:
            yield frame

    async def _do_non_stream_chat(self, model_id, messages, **kwargs):  # pragma: no cover
        return {}


def _kimi_frames():
    """还原线上报文：role 帧(zero completion) -> reasoning 累计 -> content -> finish+usage -> 纯 usage。"""
    frames = [
        {"content": "", "thinking": "", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209781, "completion_tokens": 0}},
        {"reasoning_content": " Now", "content": "", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209782, "completion_tokens": 1}},
        {"reasoning_content": " I need to add the `_reload_service` helper", "content": "", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209792, "completion_tokens": 11}},
        {"content": " 现在添加 `_reload_service`", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209942, "completion_tokens": 161}},
        {"content": "` 辅助函数，以及一个公共 reload 端点。 ", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 209962, "completion_tokens": 181}},
        {"finish_reason": "stop", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 210012, "completion_tokens": 231}},
        {"content": "", "tool_calls": [],
         "usage": {"prompt_tokens": 209781, "total_tokens": 210012, "completion_tokens": 231}},
    ]
    return frames


def _collect_usage_chunks(provider):
    out = []
    async def run():
        async for piece in provider.chat("kimi-k2.6", [{"role": "user", "content": "hi"}], stream=True):
            out.append(piece)
    asyncio.run(run())
    return out


def _parse_usage_from_chunks(pieces):
    usages = []
    for p in pieces:
        if not isinstance(p, str):
            continue
        for line in p.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            if isinstance(obj.get("usage"), dict):
                usages.append(obj["usage"])
    return usages


def test_cumulative_usage_emits_final_completion_not_zero_or_early_value():
    """累计型 usage：最终下发 completion 应为累计值 231，不能被首帧 0 或早期 1 锁死。"""
    provider = _KimiStreamProvider(_kimi_frames())
    usages = _parse_usage_from_chunks(_collect_usage_chunks(provider))
    assert usages, "应至少下发一个 usage chunk"
    last = usages[-1]
    assert last.get("completion_tokens") == 231, f"expected final 231, got {last}"
    assert last.get("prompt_tokens") == 209781


def test_cumulative_usage_only_one_usage_chunk_emitted():
    """dict 路径不应中途下发 usage，只在流末尾补一次，避免客户端/日志收到中间值。"""
    provider = _KimiStreamProvider(_kimi_frames())
    usages = _parse_usage_from_chunks(_collect_usage_chunks(provider))
    assert len(usages) == 1, f"dict 路径应只下发一次最终 usage，实际 {len(usages)}: {usages}"


# ---------------------------------------------------------------------------
# 零 completion 兜底：上游始终只发 {prompt:X, completion:0} → 按内容估算
# ---------------------------------------------------------------------------
def test_zero_completion_cumulative_estimates_by_content():
    frames = [
        {"content": "Hello world", "tool_calls": [],
         "usage": {"prompt_tokens": 100, "total_tokens": 100, "completion_tokens": 0}},
        {"finish_reason": "stop", "tool_calls": [],
         "usage": {"prompt_tokens": 100, "total_tokens": 100, "completion_tokens": 0}},
    ]
    provider = _KimiStreamProvider(frames)
    usages = _parse_usage_from_chunks(_collect_usage_chunks(provider))
    assert usages
    last = usages[-1]
    # completion 仍为 0 时估算兜底：completion_tokens 应为按内容估算的正值。
    assert last.get("completion_tokens", 0) > 0, f"expected estimated completion > 0, got {last}"
