"""custom 渠道 usage 为空 / 独立注释事件 token 回归测试。

历史 bug：custom 流式 `_do_stream_chat` 对纯注释事件（is_sse_comment_only）直接
`continue`，使 `parse_sse_usage` 的注释兜底永远拿不到把 token 放在**独立注释事件**里
的上游（如 MiniMax 类）。结果 `pending_usage` 恒为空 → 误判 zero completion 空响应
（IncompleteStreamError）或漏记 token。

现有 `tests/test_upstream_stream.py::test_minimax_comment_usage_flows_to_final_usage_chunk`
只覆盖了 data: + 注释同帧的形态（is_sse_comment_only 返回 False），没覆盖注释行
作为独立事件单独下发的形态。这里补齐。
"""

import asyncio
import json

from providers.custom import CustomProvider


class IndependentCommentUsageProvider(CustomProvider):
    """token 统计放在独立的纯注释事件里（与 data: 事件不同帧）。"""

    _CONTENT = ["你", "好", "，", "在"]

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        input_tokens = 18930
        for i, piece in enumerate(self._CONTENT):
            data = {
                "id": "chatcmpl-x",
                "choices": [{"index": 0, "delta": {"content": piece, "role": "assistant"}, "finish_reason": None}],
                "model": "deepseek",
                "object": "chat.completion.chunk",
                "usage": None,
            }
            # data: 事件
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            # 独立的纯注释事件（整帧只有以 ':' 开头的行 → is_sse_comment_only=True）
            comment = {"input_tokens": input_tokens, "output_tokens": i + 1}
            yield f": {json.dumps(comment)}\n\n"
        # 结束帧
        final_data = {
            "id": "chatcmpl-x",
            "choices": [{"index": 0, "delta": {"content": "", "role": "assistant"}, "finish_reason": "stop"}],
            "model": "deepseek",
            "object": "chat.completion.chunk",
            "usage": None,
        }
        yield f"data: {json.dumps(final_data)}\n\n"
        yield f": {json.dumps({'input_tokens': input_tokens, 'output_tokens': 4})}\n\n"
        yield "data: [DONE]\n\n"


class EmptyCommentOnlyProvider(CustomProvider):
    """纯注释事件形态下，上游有真实内容但从不在 data 帧带 usage。"""

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        yield 'data: {"choices":[{"index":0,"delta":{"content":"hi","role":"assistant"},"finish_reason":null}],"usage":null}\n\n'
        yield ': {"input_tokens":100,"output_tokens":2}\n\n'
        yield 'data: {"choices":[{"index":0,"delta":{"content":"","role":"assistant"},"finish_reason":"stop"}],"usage":null}\n\n'
        yield "data: [DONE]\n\n"


class ContentNoTerminatorProvider(CustomProvider):
    """上游吐了完整内容，但流结束时既无 finish_reason 也无 [DONE]（连接自然收尾）。

    还原线上 gpt-5.6-sol 报文：满屏 delta.content、无 usage、末尾缺终止帧。
    修复前 custom 的 `if not completed:` 无 has_output 守卫，会误判不完整流抛
    IncompleteStreamError；base 层同类判定本就有 not has_output 守卫，此处对齐。
    """

    _CONTENT = ["你好", "，我", "是", "助手", "。"]

    async def send_sse_request(self, method, url, headers, on_headers=None, **kwargs):
        for piece in self._CONTENT:
            data = {
                "id": "resp_x",
                "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}],
                "model": "gpt-5.6-sol",
                "object": "chat.completion.chunk",
            }
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
        # 上游没有发 finish_reason，也没有 [DONE]，流直接结束


def _collect_final_usage(provider, model="deepseek"):
    async def run():
        chunks = []
        async for chunk in provider.chat(model, [{"role": "user", "content": "hi"}], stream=True):
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(run())
    usage = None
    for chunk in chunks:
        if isinstance(chunk, str) and '"usage"' in chunk:
            for line in chunk.splitlines():
                line = line.strip()
                if not line.startswith("data:"):
                    continue
                body = line[5:].strip()
                if body in ("[DONE]", ""):
                    continue
                obj = json.loads(body)
                if isinstance(obj.get("usage"), dict):
                    usage = obj["usage"]
    return chunks, usage


def test_independent_comment_event_token_flows_to_final_usage():
    provider = IndependentCommentUsageProvider(
        "user", "key",
        provider_name="deepseek", base_url="https://example.test",
        protocol="openai", upstream_stream=True,
    )
    chunks, usage = _collect_final_usage(provider)

    # 独立注释事件里的 token 必须被采集，而非退回内容估算或误判空响应
    assert usage is not None, chunks
    assert usage["prompt_tokens"] == 18930, usage
    assert usage["completion_tokens"] == 4, usage
    assert not any(
        "upstream response usage reports zero completion tokens" in str(c) for c in chunks
    ), chunks


def test_comment_only_usage_does_not_raise_zero_completion():
    provider = EmptyCommentOnlyProvider(
        "user", "key",
        provider_name="deepseek", base_url="https://example.test",
        protocol="openai", upstream_stream=True,
    )
    chunks, usage = _collect_final_usage(provider)

    # 有内容 + 注释行 token → 不能误判 zero completion 空响应
    assert not any(
        "upstream response usage reports zero completion tokens" in str(c) for c in chunks
    ), chunks
    assert usage is not None, chunks
    assert usage["prompt_tokens"] == 100, usage
    assert usage["completion_tokens"] == 2, usage


def test_content_without_terminator_does_not_raise_incomplete_stream():
    """上游吐了完整内容但缺终止帧（无 finish_reason / [DONE]）时，不能判不完整流。

    还原线上 gpt-5.6-sol：满屏 content、末尾无终止信号。有真实输出就应正常收尾，
    usage 由 base 层按内容估算兜底。对齐 base 层 has_output 守卫；修复前 custom 的
    `if not completed` 无条件抛 IncompleteStreamError，导致满屏内容被误判成空响应异常。
    """
    provider = ContentNoTerminatorProvider(
        "user", "key",
        provider_name="sol", base_url="https://example.test",
        protocol="openai", upstream_stream=True,
    )
    chunks, usage = _collect_final_usage(provider, model="gpt-5.6-sol")

    # 有内容缺终止帧 → 不能抛不完整流 / 空响应
    assert not any(
        "upstream stream ended before" in str(c)
        or "upstream stream completed with empty output" in str(c)
        or "upstream response usage reports zero completion tokens" in str(c)
        for c in chunks
    ), chunks
    # 内容照常输出
    assert any(isinstance(c, str) and "你好" in c for c in chunks), chunks
    # usage 由内容估算兜底（completion 非零）
    assert usage is not None, chunks
    assert usage["completion_tokens"] > 0, usage
