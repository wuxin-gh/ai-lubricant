"""流式路径 tool_function 围栏工具调用解析回归测试。

意图驱动后，工具调用的唯一边界是 tool_function 围栏。流式出口仅当请求带 tools
时启用 extract_complete_tool_calls 缓冲解析，把围栏块切成标准 OpenAI tool_calls
delta 下发、围栏外文本原样下发；半截围栏留 buffer 等补全；流结束 flush 残留。

围栏外的 XML / Hermes / 裸 JSON / 普通 json 代码块在流式路径不当工具调用，
原样透传成可见文本（它们是正文，不是意图信号）。
"""
import asyncio
import json

from providers.base import BaseProvider


class _ToolStreamProvider(BaseProvider):
    PROVIDER_NAME = "tool-stream-fake"

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


def _collect(provider, tools=None):
    out = []

    async def run():
        kwargs = {}
        if tools is not None:
            kwargs["tools"] = tools
        async for piece in provider.chat("m", [{"role": "user", "content": "hi"}], stream=True, **kwargs):
            out.append(piece)

    asyncio.run(run())
    return out


def _deltas(pieces):
    """从 SSE chunk 字符串里抽出所有 delta dict。"""
    deltas = []
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
                delta = choice.get("delta")
                if isinstance(delta, dict):
                    deltas.append(delta)
    return deltas


def _one_tool():
    return [{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "天气",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
        },
    }]


_FENCE_OPEN = "```tool_function"
_FENCE_CLOSE = "```"


def _fence_stream_frames(payload: str):
    """把一个 tool_function 围栏块拆成逐字符 delta 帧。"""
    text = _FENCE_OPEN + "\n" + payload + "\n" + _FENCE_CLOSE
    return [{"content": ch} for ch in text]


def test_stream_fence_tool_call_becomes_tool_calls_delta():
    """带 tools 时，围栏内的 JSON 必须被解析成 tool_calls delta，围栏文本不泄漏。"""
    payload = '{"name": "get_weather", "arguments": {"city": "北京"}}'
    frames = [
        {"content": "好的，我帮你查。"},
        *_fence_stream_frames(payload),
        {"finish_reason": "tool_calls"},
    ]
    pieces = _collect(_ToolStreamProvider(frames), tools=_one_tool())
    deltas = _deltas(pieces)

    contents = "".join(d.get("content") or "" for d in deltas)
    flat = [tc for d in deltas if d.get("tool_calls") for tc in d["tool_calls"]]

    assert "好的，我帮你查。" in contents
    assert "tool_function" not in contents
    assert "get_weather" not in contents
    assert '"name"' not in contents
    assert flat, "no tool_calls delta emitted"
    assert flat[0]["function"]["name"] == "get_weather"
    assert json.loads(flat[0]["function"]["arguments"]) == {"city": "北京"}


def test_stream_fence_split_across_many_chunks():
    """围栏被逐字符切帧仍要解析成完整 tool_calls。"""
    payload = '{"name": "get_weather", "arguments": {"city": "上海"}}'
    frames = _fence_stream_frames(payload) + [{"finish_reason": "tool_calls"}]
    pieces = _collect(_ToolStreamProvider(frames), tools=_one_tool())
    deltas = _deltas(pieces)
    contents = "".join(d.get("content") or "" for d in deltas)
    assert "tool_function" not in contents
    flat = [tc for d in deltas if d.get("tool_calls") for tc in d["tool_calls"]]
    assert flat
    assert flat[0]["function"]["name"] == "get_weather"


def test_stream_text_only_passes_through_when_tools_disabled():
    """无 tools 时不启用缓冲，普通文本原样透传（含围栏也不解析）。"""
    text = _FENCE_OPEN + '\n{"name": "foo"}\n' + _FENCE_CLOSE + " 是普通文本"
    frames = [{"content": text}, {"finish_reason": "stop"}]
    pieces = _collect(_ToolStreamProvider(frames), tools=None)
    deltas = _deltas(pieces)
    contents = "".join(d.get("content") or "" for d in deltas)
    assert contents == text
    assert not any(d.get("tool_calls") for d in deltas)


def test_stream_trailing_text_after_fence_flushed():
    """围栏闭合后若还有普通文本，要正常下发，不能被吞。"""
    payload = '{"name": "get_weather", "arguments": {"city": "深圳"}}'
    frames = [
        {"content": _FENCE_OPEN + "\n" + payload + "\n" + _FENCE_CLOSE},
        {"content": "查到了。"},
        {"finish_reason": "tool_calls"},
    ]
    pieces = _collect(_ToolStreamProvider(frames), tools=_one_tool())
    deltas = _deltas(pieces)
    contents = "".join(d.get("content") or "" for d in deltas)
    assert "查到了。" in contents
    assert "tool_function" not in contents
    assert "get_weather" not in contents


def test_stream_incomplete_fence_held_no_premature_leak():
    """未闭合的围栏留 buffer，前置文本可即时下发，半截围栏/JSON 不泄漏。"""
    frames = [
        {"content": "前置文本。" + _FENCE_OPEN + "\n"},
        {"content": '{"name": "get_weather", "arguments": {"city":'},  # 未闭合
        {"finish_reason": "tool_calls"},
    ]
    pieces = _collect(_ToolStreamProvider(frames), tools=_one_tool())
    deltas = _deltas(pieces)
    contents = "".join(d.get("content") or "" for d in deltas)
    assert "前置文本。" in contents
    assert "tool_function" not in contents
    assert "get_weather" not in contents


def test_stream_xml_outside_fence_passes_as_text():
    """围栏外的 XML function 标签在流式路径不当工具调用，原样下发成可见文本。"""
    text = " <function=get_weather><parameter=city>北京</parameter></function>"
    frames = [{"content": ch} for ch in text] + [{"finish_reason": "stop"}]
    pieces = _collect(_ToolStreamProvider(frames), tools=_one_tool())
    deltas = _deltas(pieces)
    contents = "".join(d.get("content") or "" for d in deltas)
    flat = [tc for d in deltas if d.get("tool_calls") for tc in d["tool_calls"]]
    assert not flat, "围栏外 XML 被误判成工具调用"
    assert "function=get_weather" in contents  # XML 作为正文下发


def test_stream_dict_arguments_normalized_to_json_string():
    """上游 yield dict 形态 tool_calls arguments（eaichat 形态）时，流式出口必须
    序列化成 JSON 字符串再下发——原样下发 dict 是协议违规，客户端 SDK 解析会炸。
    """
    frames = [
        {"content": "", "thinking": "", "tool_calls": [
            {"index": 0, "id": "c1", "type": "function",
             "function": {"name": "get_weather", "arguments": {"city": "北京"}}}]},
        {"finish_reason": "tool_calls"},
    ]
    pieces = _collect(_ToolStreamProvider(frames), tools=_one_tool())
    deltas = _deltas(pieces)
    flat = [tc for d in deltas if d.get("tool_calls") for tc in d["tool_calls"]]
    assert flat
    args = flat[0]["function"]["arguments"]
    assert isinstance(args, str), "dict arguments leaked to client unserialized"
    assert json.loads(args) == {"city": "北京"}
