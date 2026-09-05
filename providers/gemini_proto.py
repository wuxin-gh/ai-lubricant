"""Gemini-compatible upstream protocol helpers.

Shared by the ``gemini`` protocol
branch of :class:`providers.custom.CustomProvider`. All functions are pure so
they can be unit-tested without a live upstream.
"""
from __future__ import annotations

import json
import time
import uuid
from urllib.parse import quote

from loguru import logger


def gemini_model_name(model_id: str) -> str:
    """Normalize a model id to the ``models/<name>`` form used in URL paths."""
    model = str(model_id or "").strip()
    if not model:
        return model
    if model.startswith("models/"):
        return model
    return f"models/{model}"


def _base_url(base_url: str) -> str:
    return str(base_url or "").rstrip("/")


def gemini_models_url(base_url: str, models_path: str | None = None) -> str:
    path = models_path or "/v1beta/models"
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{_base_url(base_url)}/{path.lstrip('/')}"


def gemini_stream_url(base_url: str, model_id: str, api_version: str = "v1beta") -> str:
    model = quote(gemini_model_name(model_id), safe="/")
    return f"{_base_url(base_url)}/{api_version}/{model}:streamGenerateContent?alt=sse"


def gemini_nonstream_url(base_url: str, model_id: str, api_version: str = "v1beta") -> str:
    model = quote(gemini_model_name(model_id), safe="/")
    return f"{_base_url(base_url)}/{api_version}/{model}:generateContent"


# 通用 Gemini 上游（自建代理 / 中转）路径模板占位符：
#   {model}  → 当前请求模型 id（原样，可含 models/ 前缀）
#   {method} → 非流式 generateContent / 流式 streamGenerateContent?alt=sse
_GEMINI_METHOD_STREAM = "streamGenerateContent?alt=sse"
_GEMINI_METHOD_NONSTREAM = "generateContent"


def gemini_chat_url(
    base_url: str,
    model_id: str,
    stream: bool,
    api_version: str = "v1beta",
    chat_path: str | None = None,
) -> str:
    """构造 Gemini 聊天地址。

    chat_path 为空 → 回退官方形态 {api_version}/models/{model}:{method}。
    chat_path 提供时按模板拼接：支持 {model}/{method} 占位符；无占位符时把 :method
    追加到末尾（兼容只写到 models/{model} 前缀的写法）。绝对 URL(http/https) 原样透传。
    """
    template = str(chat_path or "").strip()
    if not template:
        return (
            gemini_stream_url(base_url, model_id, api_version)
            if stream
            else gemini_nonstream_url(base_url, model_id, api_version)
        )
    method = _GEMINI_METHOD_STREAM if stream else _GEMINI_METHOD_NONSTREAM
    # 模板自己写 models/ 前缀，故 {model} 只填裸模型名，避免 models/models/xxx。
    raw_model = str(model_id or "").strip()
    if raw_model.startswith("models/"):
        raw_model = raw_model[len("models/"):]
    model = quote(raw_model, safe="/")
    if "{model}" in template or "{method}" in template:
        path = template.replace("{model}", model).replace("{method}", method)
    else:
        # 只给了到模型的前缀（如 /v1beta/models）：补 /{model}:{method}
        path = f"{template.rstrip('/')}/{model}:{method}"
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{_base_url(base_url)}/{path.lstrip('/')}"


def openai_messages_to_gemini(messages: list[dict]) -> tuple[list[dict], dict | None]:
    """Convert OpenAI ``messages`` to Gemini ``contents`` + ``systemInstruction``."""
    contents: list[dict] = []
    system_parts: list[dict] = []
    for msg in messages or []:
        role = msg.get("role") or "user"
        content = msg.get("content", "")
        if role == "system":
            system_parts.extend(_content_to_parts(content))
            continue
        parts: list[dict] = []
        if role == "assistant" and msg.get("tool_calls"):
            parts.extend(_tool_calls_to_parts(msg.get("tool_calls") or []))
        if role == "tool":
            parts = [_tool_message_to_part(msg)]
            gemini_role = "function"
        elif role == "assistant":
            gemini_role = "model"
        else:
            gemini_role = "user"
        parts.extend(_content_to_parts(content))
        if parts:
            contents.append({"role": gemini_role, "parts": parts})
    system_instruction = {"parts": system_parts} if system_parts else None
    return contents, system_instruction


def _content_to_parts(content) -> list[dict]:
    if content is None:
        return []
    if isinstance(content, str):
        return [{"text": content}] if content else []
    if isinstance(content, list):
        parts = []
        for item in content:
            if not isinstance(item, dict):
                if item is not None:
                    parts.append({"text": str(item)})
                continue
            kind = item.get("type")
            if kind == "text":
                text = item.get("text", "")
                if text:
                    parts.append({"text": text})
            elif kind == "image_url":
                part = _image_url_part(item.get("image_url"))
                if part:
                    parts.append(part)
            elif kind in ("input_text", "output_text"):
                text = item.get("text", "")
                if text:
                    parts.append({"text": text})
        return parts
    return [{"text": json.dumps(content, ensure_ascii=False)}]


def _image_url_part(image_url) -> dict | None:
    url = image_url.get("url") if isinstance(image_url, dict) else image_url
    if not isinstance(url, str) or not url:
        return None
    if not url.startswith("data:"):
        return {"fileData": {"fileUri": url}}
    header, _, data = url.partition(",")
    if not data:
        return None
    mime = "image/png"
    if header.startswith("data:"):
        mime = header[5:].split(";", 1)[0] or mime
    return {"inlineData": {"mimeType": mime, "data": data}}


def _tool_calls_to_parts(tool_calls: list[dict]) -> list[dict]:
    parts = []
    for call in tool_calls:
        if not isinstance(call, dict):
            continue
        fn = call.get("function") or {}
        name = fn.get("name") or call.get("name")
        if not name:
            continue
        # fn.name missing → fell back to the non-standard call["name"]. This
        # recovers the tool name but call["args"] (display shape) is not probed,
        # so arguments are lost. Surface it: the caller should send wire shape.
        if not fn.get("name") and call.get("name"):
            logger.warning(
                "_tool_calls_to_parts: tool_call has no function.name — recovered "
                "name from non-standard call.name but arguments may be lost. "
                "Caller must send wire shape {id, type, function: {name, arguments}}."
            )
        args = fn.get("arguments") or call.get("arguments") or {}
        if isinstance(args, str):
            try:
                args = json.loads(args) if args else {}
            except json.JSONDecodeError:
                args = {"value": args}
        parts.append({
            "functionCall": {
                "name": name,
                "args": args if isinstance(args, dict) else {"value": args},
            }
        })
    return parts


def _tool_message_to_part(msg: dict) -> dict:
    name = msg.get("name") or "tool"
    content = msg.get("content", "")
    if isinstance(content, str):
        response = {"result": content}
    elif isinstance(content, dict):
        response = content
    else:
        response = {"result": content}
    return {"functionResponse": {"name": name, "response": response}}


def build_gemini_generation_config(kwargs: dict | None) -> dict:
    kwargs = kwargs or {}
    config: dict = {}
    mapping = {
        "temperature": "temperature",
        "top_p": "topP",
        "top_k": "topK",
        "max_tokens": "maxOutputTokens",
    }
    for src, dst in mapping.items():
        value = kwargs.get(src)
        if value is not None:
            config[dst] = value
    stop = kwargs.get("stop") or kwargs.get("stop_sequences")
    if isinstance(stop, str) and stop:
        config["stopSequences"] = [stop]
    elif isinstance(stop, list) and stop:
        config["stopSequences"] = stop
    return config


def build_gemini_tools(tools: list[dict] | None) -> list[dict]:
    declarations = []
    for tool in tools or []:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        fn = tool.get("function") or {}
        name = fn.get("name")
        if not name:
            continue
        item = {"name": name}
        if fn.get("description"):
            item["description"] = fn.get("description")
        if isinstance(fn.get("parameters"), dict):
            item["parameters"] = fn.get("parameters")
        declarations.append(item)
    return [{"functionDeclarations": declarations}] if declarations else []


def build_gemini_payload(model_id: str, messages: list[dict], **kwargs) -> dict:
    """Build a Gemini ``generateContent`` request body from OpenAI inputs."""
    contents, system_instruction = openai_messages_to_gemini(messages)
    payload: dict = {"contents": contents}
    if system_instruction:
        payload["systemInstruction"] = system_instruction
    generation_config = build_gemini_generation_config(kwargs)
    if generation_config:
        payload["generationConfig"] = generation_config
    tools = build_gemini_tools(kwargs.get("tools"))
    if tools:
        payload["tools"] = tools
    return payload


def gemini_usage_to_openai(usage: dict | None) -> dict | None:
    if not isinstance(usage, dict):
        return None
    prompt = int(usage.get("promptTokenCount") or usage.get("prompt_tokens") or 0)
    completion = int(usage.get("candidatesTokenCount") or usage.get("completion_tokens") or 0)
    total = int(usage.get("totalTokenCount") or (prompt + completion))
    cached = int(usage.get("cachedContentTokenCount") or 0)
    reasoning = int(usage.get("thoughtsTokenCount") or 0)
    result: dict = {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total}
    if cached:
        result["prompt_tokens_details"] = {"cached_tokens": cached}
        result["cached_tokens"] = cached
    if reasoning:
        result["completion_tokens_details"] = {"reasoning_tokens": reasoning}
        result["reasoning_tokens"] = reasoning
    return result


def parse_gemini_chunk(obj: dict) -> dict:
    """Extract OpenAI-shaped deltas from one Gemini stream/JSON chunk."""
    result = {
        "content": "",
        "thinking": "",
        "tool_calls": [],
        "usage": gemini_usage_to_openai(obj.get("usageMetadata")) if isinstance(obj, dict) else None,
        "finish_reason": None,
    }
    candidates = obj.get("candidates") if isinstance(obj, dict) else None
    if not candidates:
        return result
    candidate = candidates[0] or {}
    result["finish_reason"] = _finish_reason(candidate.get("finishReason"))
    parts = ((candidate.get("content") or {}).get("parts") or [])
    for part in parts:
        if not isinstance(part, dict):
            continue
        if part.get("text"):
            result["content"] += part.get("text") or ""
        if part.get("thoughtText"):
            result["thinking"] += part.get("thoughtText") or ""
        fn = part.get("functionCall")
        if isinstance(fn, dict):
            name = fn.get("name") or "function"
            args = fn.get("args") or {}
            result["tool_calls"].append({
                "id": f"call_{uuid.uuid4().hex[:24]}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)},
            })
    return result


def gemini_response_to_openai(obj: dict, model_id: str, completion_id: str | None = None) -> dict:
    """Assemble a non-stream OpenAI response from a Gemini ``generateContent`` body."""
    parsed = parse_gemini_chunk(obj or {})
    completion_id = completion_id or f"chatcmpl-{uuid.uuid4().hex}"
    message: dict = {"role": "assistant", "content": parsed.get("content") or ""}
    if parsed.get("thinking"):
        message["reasoning_content"] = parsed["thinking"]
    if parsed.get("tool_calls"):
        message["tool_calls"] = parsed["tool_calls"]
    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": parsed.get("finish_reason") or ("tool_calls" if parsed.get("tool_calls") else "stop"),
        }],
        "usage": parsed.get("usage") or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def normalize_gemini_models(data: dict, owner: str = "gemini") -> list[dict]:
    """Normalize a Gemini ``/models`` response into the project model-list shape."""
    models = []
    for item in (data or {}).get("models", []) or []:
        if not isinstance(item, dict):
            continue
        raw_name = item.get("name") or item.get("id")
        if not raw_name:
            continue
        model_id = str(raw_name).removeprefix("models/")
        models.append({
            "id": model_id,
            "name": item.get("displayName") or item.get("display_name") or model_id,
            "owned_by": owner,
            "created": int(time.time()),
            "object": "model",
            "raw": item,
        })
    return models


def _finish_reason(reason: str | None) -> str | None:
    if not reason:
        return None
    mapping = {
        "STOP": "stop",
        "MAX_TOKENS": "length",
        "SAFETY": "content_filter",
        "RECITATION": "content_filter",
        "MALFORMED_FUNCTION_CALL": "tool_calls",
    }
    return mapping.get(str(reason).upper(), str(reason).lower())
