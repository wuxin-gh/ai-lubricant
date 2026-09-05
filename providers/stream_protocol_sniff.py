"""跨协议 SSE 兜底：按上游实际回帧识别协议，并归一成 OpenAI delta 形态。

## 为什么需要

出站协议由渠道配置/客户端模板决定（`_resolve_build_endpoint`），但上游实际回什么
协议不受我们控制。二者不一致时，解析器按出站协议的字段名去读，一个字都取不到：

    出站 openai → 上游回 anthropic 帧
      parse_sse_data 只认 choices[].delta，content_block_delta 取不到内容
      openai_sse_completed 只认 [DONE]/finish_reason，不认 message_stop
      → has_output=False & completed=False → IncompleteStreamError（误判空流）

而 usage 因为 `_extract_usage_payload` 读顶层 `usage`，反倒能提取成功 ——
于是出现「usage 有 352 output_tokens 却判定无输出」的自相矛盾状态。
更严重的是内容整路丢失，客户端即使不报错也收不到任何东西。

所以兜底必须是「识别并正确解析」，而不是「放宽判定别抛异常」。

## 设计

- **只用强特征**，认不出返回 None，绝不猜。宁可不兜底，也不能把同协议流误判成
  跨协议再走一遍转换（那会引入新的内容丢失）。
- **粘性**：同一条流不会中途换协议。首帧认出后整条流沿用，避免因某帧特征弱而摇摆。
  语义对齐 `_responses_stream_chat` 里既有的 `saw_chat_completion_chunk`。
- **只在原路径什么都没解析到时才启用**：同协议路径完全不进这里，零影响。
- 归一输出复用 `_do_stream_chat` 内部已在用的 dict 形态
  `{content, thinking, tool_calls, usage, finish_reason, done}`，不新增中间表示。
"""

from __future__ import annotations

import json
import uuid

from message_utils import iter_sse_payloads, parse_sse_event

# 协议标识。与 `_resolve_build_endpoint` 的 endpoint 取值保持同名，便于日志比对。
PROTO_OPENAI = "openai"
PROTO_ANTHROPIC = "anthropic"
PROTO_RESPONSES = "responses"
PROTO_GEMINI = "gemini"

# Anthropic 流事件名全集（强特征：其它协议不会出现这些 type）
_ANTHROPIC_EVENT_TYPES = frozenset({
    "message_start",
    "message_delta",
    "message_stop",
    "content_block_start",
    "content_block_delta",
    "content_block_stop",
})

# Anthropic content_block_delta 的 delta.type 全集
_ANTHROPIC_DELTA_TYPES = frozenset({
    "text_delta",
    "thinking_delta",
    "input_json_delta",
    "signature_delta",
})


def sniff_payload_protocol(payload) -> str | None:
    """按单个 data payload 判协议。只用强特征，认不出返回 None。"""
    if not isinstance(payload, dict):
        return None

    # ---- Anthropic：type ∈ 事件名全集 ----
    payload_type = payload.get("type")
    if isinstance(payload_type, str):
        if payload_type in _ANTHROPIC_EVENT_TYPES:
            return PROTO_ANTHROPIC
        # ---- Responses：type 以 response. 开头 ----
        if payload_type.startswith("response."):
            return PROTO_RESPONSES

    # ---- OpenAI Chat Completions：object=chat.completion* 或 choices[].delta ----
    obj = payload.get("object")
    if isinstance(obj, str) and obj.startswith("chat.completion"):
        return PROTO_OPENAI
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict) and isinstance(first.get("delta"), (dict, list)):
            return PROTO_OPENAI

    # ---- Gemini：candidates[].content.parts / usageMetadata ----
    candidates = payload.get("candidates")
    if isinstance(candidates, list) and candidates:
        first = candidates[0]
        if isinstance(first, dict) and isinstance(first.get("content"), dict):
            return PROTO_GEMINI
    if isinstance(payload.get("usageMetadata"), dict):
        return PROTO_GEMINI

    # ---- Responses 弱形态兜底：response 对象内嵌 output 列表 ----
    # 仅在明确带 output/status 时认，避免把任意含 response 键的帧误判。
    response_obj = payload.get("response")
    if isinstance(response_obj, dict) and (
        isinstance(response_obj.get("output"), list) or response_obj.get("status")
    ):
        return PROTO_RESPONSES

    return None


def sniff_event_protocol(event_str: str) -> str | None:
    """按一个完整 SSE event（可能含多个 data 行）判协议。

    先看 data payload 的强特征；都认不出时再退到 `event:` 行名 —— 部分上游
    只在 event 行给 anthropic 事件名，data 里是精简对象。
    """
    if not isinstance(event_str, str):
        return None

    for payload in iter_sse_payloads(event_str):
        if payload == "[DONE]":
            # [DONE] 是 OpenAI/Responses 共用终止符，本身不足以定协议，继续看其它帧。
            continue
        proto = sniff_payload_protocol(payload)
        if proto:
            return proto

    # 退到 event 行名。parse_sse_event 返回 (event_type, payload)。
    try:
        event_type, _ = parse_sse_event(event_str)
    except (AttributeError, json.JSONDecodeError, ValueError):
        return None
    if isinstance(event_type, str):
        if event_type in _ANTHROPIC_EVENT_TYPES:
            return PROTO_ANTHROPIC
        if event_type.startswith("response."):
            return PROTO_RESPONSES
    return None


def extract_protocol_error(payload) -> dict | None:
    """协议无关地抽出上游错误体。非错误帧返回 None。

    覆盖各协议的错误摆放位置：
      - OpenAI / Gemini / Anthropic error 事件：顶层 ``error``
      - Responses ``response.failed``：嵌套在 ``response.error`` —— 顶层没有 error 键，
        只查顶层会整帧漏掉，然后 ``response.failed`` 被当成正常完成，真实原因
        （如 rate_limit_exceeded / 并发超限）一个字都不显示。
      - Cloudflare 风格：``errors`` 列表

    只返回错误体本身，不做 status 映射 —— 映射由调用方的 `_raise_upstream_error`
    统一负责（rate_limit → 429 等），避免两处各判一套。
    """
    if not isinstance(payload, dict):
        return None

    err = payload.get("error")
    if isinstance(err, dict) and (err.get("message") or err.get("code")):
        return err
    if isinstance(err, str) and err.strip():
        return {"message": err}

    # Responses：response.failed / response.incomplete 把错误嵌在 response 对象里
    response_obj = payload.get("response")
    if isinstance(response_obj, dict):
        nested = response_obj.get("error")
        if isinstance(nested, dict) and (nested.get("message") or nested.get("code")):
            return nested
        if isinstance(nested, str) and nested.strip():
            return {"message": nested}

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict) and (first.get("message") or first.get("code")):
            return first
        if isinstance(first, str) and first.strip():
            return {"message": first}
    elif isinstance(errors, dict) and (errors.get("message") or errors.get("code")):
        return errors

    return None


def extract_event_error(event_str: str) -> dict | None:
    """从一个完整 SSE event 中抽出上游错误体。非错误事件返回 None。"""
    if not isinstance(event_str, str):
        return None
    for payload in iter_sse_payloads(event_str):
        err = extract_protocol_error(payload)
        if err is not None:
            return err
    return None


def _empty_delta() -> dict:
    return {"content": "", "thinking": "", "tool_calls": [], "usage": None,
            "finish_reason": None, "done": False, "error": None}


def _has_signal(delta: dict) -> bool:
    """归一结果是否携带任何有效信号（内容/工具/usage/终止/错误）。"""
    return bool(
        delta.get("content")
        or delta.get("thinking")
        or delta.get("tool_calls")
        or delta.get("usage")
        or delta.get("finish_reason")
        or delta.get("done")
        or delta.get("error")
    )


class CrossProtocolStreamNormalizer:
    """把非出站协议的上游 SSE 帧归一成 OpenAI delta 形态。

    有状态：需要跨帧维护 tool_calls 的 index 重映射与累计 usage，因此按流实例化一次。

    上游块索引在 thinking/text/tool_use 之间共享，而 OpenAI `tool_calls.index`
    只对工具计数 —— 必须重映射，否则工具参数会拼到错误的调用上。这套映射语义
    与 `custom.py::_anthropic_stream_chat` 一致。
    """

    def __init__(self, protocol: str):
        self.protocol = protocol
        self._tool_index_map: dict = {}
        self._cumulative_usage: dict = {}
        self._saw_output = False

    @property
    def saw_output(self) -> bool:
        """本归一器是否见过真实输出（content/thinking/tool_calls）。"""
        return self._saw_output

    @property
    def cumulative_usage(self) -> dict:
        return dict(self._cumulative_usage)

    def _tool_index(self, upstream_idx) -> int:
        idx = self._tool_index_map.get(upstream_idx)
        if idx is None:
            idx = len(self._tool_index_map)
            self._tool_index_map[upstream_idx] = idx
        return idx

    def feed(self, event_str: str) -> list[dict]:
        """喂一个 SSE event，返回 0..n 个 OpenAI delta dict。"""
        if self.protocol == PROTO_ANTHROPIC:
            return self._feed_anthropic(event_str)
        if self.protocol == PROTO_RESPONSES:
            return self._feed_responses(event_str)
        if self.protocol == PROTO_GEMINI:
            return self._feed_gemini(event_str)
        return self._feed_openai(event_str)

    # ---------------- Anthropic ----------------

    def _feed_anthropic(self, event_str: str) -> list[dict]:
        out: list[dict] = []
        for payload in iter_sse_payloads(event_str):
            if not isinstance(payload, dict):
                continue
            event_type = payload.get("type")
            delta_out = _empty_delta()

            if event_type == "message_start":
                msg = payload.get("message") or {}
                usage = msg.get("usage") if isinstance(msg, dict) else None
                if isinstance(usage, dict):
                    self._merge_anthropic_usage(usage)
                continue

            if event_type == "content_block_start":
                block = payload.get("content_block") or {}
                if isinstance(block, dict):
                    if block.get("type") == "tool_use":
                        tool_idx = self._tool_index(payload.get("index"))
                        delta_out["tool_calls"] = [{
                            "index": tool_idx,
                            "id": block.get("id") or "",
                            "type": "function",
                            "function": {"name": block.get("name") or "", "arguments": ""},
                        }]
                        self._saw_output = True
                    elif block.get("type") == "text" and block.get("text"):
                        delta_out["content"] = block.get("text") or ""
                        self._saw_output = True
                    elif block.get("type") == "thinking" and block.get("thinking"):
                        delta_out["thinking"] = block.get("thinking") or ""
                        self._saw_output = True

            elif event_type == "content_block_delta":
                delta = payload.get("delta") or {}
                if isinstance(delta, dict):
                    delta_type = delta.get("type")
                    if delta_type == "input_json_delta":
                        partial = delta.get("partial_json") or ""
                        if partial:
                            tool_idx = self._tool_index(payload.get("index"))
                            delta_out["tool_calls"] = [{
                                "index": tool_idx,
                                "function": {"arguments": partial},
                            }]
                            self._saw_output = True
                    elif delta_type == "signature_delta":
                        # 签名不是可见输出，也不计入内容判定，直接忽略。
                        continue
                    else:
                        text = delta.get("text") or ""
                        thinking = delta.get("thinking") or delta.get("thinking_delta") or ""
                        if text:
                            delta_out["content"] = text
                            self._saw_output = True
                        if thinking:
                            delta_out["thinking"] = thinking
                            self._saw_output = True

            elif event_type == "message_delta":
                usage = payload.get("usage")
                if isinstance(usage, dict):
                    self._merge_anthropic_usage(usage)
                stop_reason = (payload.get("delta") or {}).get("stop_reason") \
                    if isinstance(payload.get("delta"), dict) else None
                if stop_reason:
                    delta_out["finish_reason"] = _anthropic_stop_to_openai(stop_reason)

            elif event_type == "message_stop":
                # Anthropic 的流终止符。OpenAI 侧对应 [DONE]，必须映射，
                # 否则 openai_sse_completed 永远认不到完成，判「未收到终止信号」。
                delta_out["done"] = True
                if self._cumulative_usage:
                    delta_out["usage"] = self._openai_usage()

            if _has_signal(delta_out):
                out.append(delta_out)
        return out

    def _merge_anthropic_usage(self, usage: dict) -> None:
        for key, value in usage.items():
            if value is not None:
                self._cumulative_usage[key] = value

    def _openai_usage(self) -> dict:
        """Anthropic usage → OpenAI usage。

        Token 计费口径：total = 输入 + 输出；缓存读/写属输入明细，不额外计入 total。
        """
        usage = self._cumulative_usage
        prompt = _int_or_zero(usage.get("input_tokens"))
        completion = _int_or_zero(usage.get("output_tokens"))
        cached = _int_or_zero(usage.get("cache_read_input_tokens"))
        cache_write = _int_or_zero(usage.get("cache_creation_input_tokens"))
        result = {
            "prompt_tokens": prompt + cached + cache_write,
            "completion_tokens": completion,
            "total_tokens": prompt + cached + cache_write + completion,
        }
        if cached or cache_write:
            result["prompt_tokens_details"] = {"cached_tokens": cached}
            if cache_write:
                result["cache_creation_input_tokens"] = cache_write
        return result

    # ---------------- Responses ----------------

    def _feed_responses(self, event_str: str) -> list[dict]:
        out: list[dict] = []
        for payload in iter_sse_payloads(event_str):
            if payload == "[DONE]":
                out.append({**_empty_delta(), "done": True})
                continue
            if not isinstance(payload, dict):
                continue
            event_type = payload.get("type") or ""
            delta_out = _empty_delta()

            # 错误优先：response.failed 的 error 嵌在 response.error 里，顶层没有 error 键。
            # 必须在下面的终止分支之前拦住，否则会被当成正常完成、真实原因被吞掉。
            protocol_error = extract_protocol_error(payload)
            if protocol_error is not None:
                delta_out["error"] = protocol_error
                out.append(delta_out)
                continue

            if event_type == "response.output_text.delta":
                text = payload.get("delta") or ""
                if text:
                    delta_out["content"] = text
                    self._saw_output = True
            elif event_type in (
                "response.reasoning_text.delta",
                "response.reasoning_summary_text.delta",
            ):
                text = payload.get("delta") or ""
                if text:
                    delta_out["thinking"] = text
                    self._saw_output = True
            elif event_type == "response.output_item.added":
                item = payload.get("item") or {}
                if isinstance(item, dict) and item.get("type") == "function_call":
                    tool_idx = self._tool_index(payload.get("output_index"))
                    delta_out["tool_calls"] = [{
                        "index": tool_idx,
                        "id": item.get("call_id") or item.get("id") or "",
                        "type": "function",
                        "function": {"name": item.get("name") or "", "arguments": ""},
                    }]
                    self._saw_output = True
            elif event_type == "response.function_call_arguments.delta":
                args = payload.get("delta") or ""
                if args:
                    tool_idx = self._tool_index(payload.get("output_index"))
                    delta_out["tool_calls"] = [{
                        "index": tool_idx,
                        "function": {"arguments": args},
                    }]
                    self._saw_output = True
            elif event_type == "response.completed":
                response_obj = payload.get("response") or {}
                usage = response_obj.get("usage") if isinstance(response_obj, dict) else None
                if not isinstance(usage, dict):
                    usage = payload.get("usage")
                if isinstance(usage, dict):
                    from message_utils import responses_usage_to_openai_usage
                    self._cumulative_usage = dict(usage)
                    delta_out["usage"] = responses_usage_to_openai_usage(usage)
                delta_out["done"] = True
            elif event_type == "response.incomplete":
                response_obj = payload.get("response") or {}
                usage = response_obj.get("usage") if isinstance(response_obj, dict) else None
                if not isinstance(usage, dict):
                    usage = payload.get("usage")
                if isinstance(usage, dict):
                    from message_utils import responses_usage_to_openai_usage
                    self._cumulative_usage = dict(usage)
                    delta_out["usage"] = responses_usage_to_openai_usage(usage)
                delta_out["done"] = True

            if _has_signal(delta_out):
                out.append(delta_out)
        return out

    # ---------------- Gemini ----------------

    def _feed_gemini(self, event_str: str) -> list[dict]:
        from providers.gemini_proto import parse_gemini_chunk

        out: list[dict] = []
        for payload in iter_sse_payloads(event_str):
            if not isinstance(payload, dict):
                continue
            parsed = parse_gemini_chunk(payload)
            delta_out = _empty_delta()
            delta_out["content"] = parsed.get("content") or ""
            delta_out["thinking"] = parsed.get("thinking") or ""
            delta_out["tool_calls"] = parsed.get("tool_calls") or []
            delta_out["usage"] = parsed.get("usage")
            delta_out["finish_reason"] = parsed.get("finish_reason")
            if delta_out["content"] or delta_out["thinking"] or delta_out["tool_calls"]:
                self._saw_output = True
            if delta_out["usage"]:
                self._cumulative_usage = dict(delta_out["usage"])
            # Gemini 流没有独立终止事件，finishReason 即终止信号。
            if delta_out["finish_reason"]:
                delta_out["done"] = True
            if _has_signal(delta_out):
                out.append(delta_out)
        return out

    # ---------------- OpenAI ----------------

    def _feed_openai(self, event_str: str) -> list[dict]:
        from providers.base import BaseProvider

        content, thinking, tool_calls = BaseProvider.parse_sse_data(event_str)
        usage = BaseProvider.parse_sse_usage(event_str)
        done = any(p == "[DONE]" for p in iter_sse_payloads(event_str))
        finish_reason = None
        for payload in iter_sse_payloads(event_str):
            if not isinstance(payload, dict):
                continue
            for choice in payload.get("choices") or []:
                if isinstance(choice, dict) and choice.get("finish_reason"):
                    finish_reason = str(choice["finish_reason"])
        if content or thinking or tool_calls:
            self._saw_output = True
        if isinstance(usage, dict):
            self._cumulative_usage = dict(usage)
        delta_out = {
            "content": content, "thinking": thinking, "tool_calls": tool_calls,
            "usage": usage, "finish_reason": finish_reason, "done": done,
        }
        return [delta_out] if _has_signal(delta_out) else []


def _anthropic_stop_to_openai(stop_reason) -> str | None:
    mapping = {
        "end_turn": "stop",
        "stop_sequence": "stop",
        "max_tokens": "length",
        "tool_use": "tool_calls",
        "pause_turn": "stop",
        "refusal": "content_filter",
    }
    if not isinstance(stop_reason, str):
        return None
    return mapping.get(stop_reason, "stop")


def _int_or_zero(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


# ==================== 非流式 ====================

def sniff_response_protocol(body) -> str | None:
    """按非流式 JSON body 判协议。只用强特征，认不出返回 None。"""
    if not isinstance(body, dict):
        return None

    # OpenAI：choices[].message
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict) and isinstance(first.get("message"), dict):
            return PROTO_OPENAI

    # Anthropic：type=message + content 数组（+ role=assistant）
    if body.get("type") == "message" and isinstance(body.get("content"), list):
        return PROTO_ANTHROPIC
    if isinstance(body.get("content"), list) and body.get("role") == "assistant" \
            and isinstance(body.get("usage"), dict):
        return PROTO_ANTHROPIC

    # Responses：object=response 或 output 列表 + status
    if body.get("object") == "response":
        return PROTO_RESPONSES
    if isinstance(body.get("output"), list) and body.get("status"):
        return PROTO_RESPONSES

    # Gemini：candidates[].content.parts
    candidates = body.get("candidates")
    if isinstance(candidates, list) and candidates:
        first = candidates[0]
        if isinstance(first, dict) and isinstance(first.get("content"), dict):
            return PROTO_GEMINI

    return None


def normalize_response_to_openai(body: dict, protocol: str, model_id: str) -> dict | None:
    """把非 OpenAI 形态的非流式响应体转成 OpenAI 响应。认不出/转不了返回 None。"""
    if not isinstance(body, dict):
        return None
    if protocol == PROTO_ANTHROPIC:
        return _anthropic_response_to_openai(body, model_id)
    if protocol == PROTO_RESPONSES:
        from message_utils import responses_to_openai_response
        try:
            return responses_to_openai_response(body, model_id)
        except Exception:
            return None
    if protocol == PROTO_GEMINI:
        from providers.gemini_proto import gemini_response_to_openai
        try:
            return gemini_response_to_openai(body, model_id)
        except Exception:
            return None
    return None


def _anthropic_response_to_openai(body: dict, model_id: str) -> dict:
    """Anthropic messages 响应 → OpenAI chat.completion。

    语义对齐 `custom.py::_anthropic_non_stream_chat` 的解析分支。
    """
    from message_utils import make_openai_response, generate_completion_id

    content_parts: list[str] = []
    thinking_parts: list[str] = []
    tool_calls: list[dict] = []
    for block in body.get("content") or []:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text":
            content_parts.append(block.get("text") or "")
        elif block_type == "thinking":
            thinking_parts.append(block.get("thinking") or "")
        elif block_type == "tool_use":
            tool_calls.append({
                "id": block.get("id") or f"call_{uuid.uuid4().hex[:24]}",
                "type": "function",
                "function": {
                    "name": block.get("name") or "",
                    "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False),
                },
            })

    response = make_openai_response(
        generate_completion_id(),
        model_id,
        "".join(content_parts),
        thinking="".join(thinking_parts),
        tool_calls=tool_calls or None,
    )
    usage = body.get("usage")
    if isinstance(usage, dict):
        normalizer = CrossProtocolStreamNormalizer(PROTO_ANTHROPIC)
        normalizer._merge_anthropic_usage(usage)
        response["usage"] = normalizer._openai_usage()
    stop_reason = body.get("stop_reason")
    if stop_reason and response.get("choices"):
        mapped = _anthropic_stop_to_openai(stop_reason)
        if mapped:
            response["choices"][0]["finish_reason"] = mapped
    return response
