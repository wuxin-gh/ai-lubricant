"""上游把 content/thinking 显式置为 null 时的流式累加回归测试。

历史 bug：`BaseProvider.chat` 的 dict 路径用 `chunk.get("thinking", "")` 取值，
上游发 `{"content": null, "reasoning_content": null}`（常见于仅带 usage 的收尾帧、
未开思考的 reasoning 字段）时拿到的是 None，`full_thinking += thinking` 抛
`TypeError: can only concatenate str (not "NoneType") to str`。

该异常从 provider 内部冒到 _chat_with_retry，被当成渠道异常：账号按 freeze_policy
冻结 60 秒、请求切下一个候选重试——但上游其实返回正常。修复后 None 归一成空串。
"""
import asyncio
import json

from providers.base import BaseProvider


class _NullDeltaProvider(BaseProvider):
    PROVIDER_NAME = "null-delta-fake"

    def __init__(self, frames):
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


def _collect(provider):
    out = []

    async def run():
        async for piece in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True):
            out.append(piece)

    asyncio.run(run())
    return out


def _content_from_chunks(pieces):
    text = ""
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
            for choice in obj.get("choices") or []:
                text += (choice.get("delta") or {}).get("content") or ""
    return text


def test_null_content_and_thinking_frames_do_not_raise():
    """null content/thinking 的收尾帧不能让整条流报 TypeError。"""
    frames = [
        {"content": "hello", "thinking": None, "tool_calls": None},
        {"content": None, "thinking": None, "tool_calls": None,
         "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13}},
        {"content": None, "thinking": None, "finish_reason": "stop"},
    ]
    pieces = _collect(_NullDeltaProvider(frames))
    assert _content_from_chunks(pieces) == "hello"


def test_parse_sse_data_tolerates_null_delta_fields():
    """SSE 字符串路径同样不能因 delta.content/reasoning_content 为 null 而抛错。"""
    event = "data: " + json.dumps({
        "choices": [{"delta": {"content": None, "reasoning_content": None}}],
        "reasoning_content": None,
    }) + "\n\n"
    assert BaseProvider.parse_sse_data(event) == ("", "", [])


def test_parse_sse_data_tolerates_null_qwen_data_fields():
    event = "data: " + json.dumps({"data": {"text": None, "thinking": None}}) + "\n\n"
    assert BaseProvider.parse_sse_data(event) == ("", "", [])
