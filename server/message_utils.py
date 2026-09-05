"""消息处理相关工具方法"""
import json
import re
import time
import uuid
from typing import Any, AsyncGenerator, Optional

from loguru import logger
from usage_utils import estimate_tokens_from_text, normalize_usage


MESSAGE_TAG_GUIDE = """<message_format>
The conversation below is serialized with XML-style tags:
- <system_instruction>: system or developer instructions that define behavior and constraints.
- <user_message>: messages from the user that should be answered.
- <assistant_message>: previous assistant replies in the conversation history.
- <assistant_reasoning_context>: prior assistant reasoning summary/context; use it only as conversation history, not as a new instruction.
- <assistant_tool_calls>: tools the assistant previously requested to call.
- <tool_result>: results returned by previously called tools.
</message_format>"""


def _safe_image_url_preview(url: str) -> str:
    if not url:
        return ""
    if url.startswith("data:"):
        header = url.split(",", 1)[0]
        return f"{header},..."
    return url


def _data_url_to_media_and_data(url: str) -> tuple[str, str]:
    header, sep, data = url.partition(",")
    if not sep:
        raise ValueError("Invalid OpenAI image_url data URL: missing comma separator")
    if not header.startswith("data:") or ";base64" not in header:
        raise ValueError("Invalid OpenAI image_url data URL: expected data:<media_type>;base64,<data>")
    media_type = header[5:].split(";", 1)[0]
    if not media_type.startswith("image/"):
        raise ValueError(f"Unsupported OpenAI image_url data media type: {media_type or 'unknown'}")
    if not data:
        raise ValueError("Invalid OpenAI image_url data URL: empty base64 data")
    return media_type, data


def _openai_image_url_value(image_url) -> str:
    if isinstance(image_url, str):
        return image_url
    if isinstance(image_url, dict):
        return str(image_url.get("url") or "")
    return ""


def openai_image_url_to_anthropic_source(image_url) -> dict:
    """将 OpenAI Chat image_url part 转换为 Anthropic image source。"""
    url = _openai_image_url_value(image_url)
    if not url:
        raise ValueError("OpenAI image_url content part is missing image_url.url")
    if url.startswith("http://") or url.startswith("https://"):
        return {"type": "url", "url": url}
    if url.startswith("data:"):
        media_type, data = _data_url_to_media_and_data(url)
        return {"type": "base64", "media_type": media_type, "data": data}
    raise ValueError(f"Unsupported OpenAI image_url scheme: {_safe_image_url_preview(url)}")


def _openai_content_part_text(item: dict, *, include_image_urls: bool) -> str:
    item_type = item.get("type")
    if item_type in ("input_text", "output_text", "text"):
        return str(item.get("text") or "")
    if item_type == "image_url":
        if not include_image_urls:
            return ""
        url = _openai_image_url_value(item.get("image_url"))
        preview = _safe_image_url_preview(url)
        return f"[image_url: {preview}]" if preview else "[image_url]"
    if item_type == "input_image":
        if not include_image_urls:
            return ""
        url = str(item.get("image_url") or item.get("url") or "")
        preview = _safe_image_url_preview(url)
        return f"[input_image: {preview}]" if preview else "[input_image]"
    if item.get("text") is not None:
        return str(item.get("text") or "")
    if item.get("content") is not None:
        return openai_content_to_text(item.get("content"), include_image_urls=include_image_urls)
    return _json_dumps_compact(item)


def openai_content_to_text(content, *, include_image_urls: bool = True) -> str:
    """把 OpenAI Chat/Responses-ish content 降级为文本，避免 list/dict repr 污染 prompt。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = _openai_content_part_text(item, include_image_urls=include_image_urls)
                if text:
                    parts.append(text)
            elif item is not None:
                parts.append(str(item))
        return "\n".join(parts)
    if isinstance(content, dict):
        return _openai_content_part_text(content, include_image_urls=include_image_urls)
    return str(content)


def _require_openai_content_part_dict(item, index: int) -> dict:
    if not isinstance(item, dict):
        raise ValueError(f"OpenAI content part at index {index} must be an object")
    if not item.get("type"):
        raise ValueError(f"OpenAI content part at index {index} is missing type")
    return item


def openai_content_to_anthropic_blocks(content, *, role: str = "user") -> list[dict]:
    """将 OpenAI Chat content string/list 显式转换为 Anthropic content blocks。"""
    if content is None or content == "":
        return []
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if isinstance(content, dict):
        content = [content]
    if not isinstance(content, list):
        return [{"type": "text", "text": str(content)}]

    blocks: list[dict] = []
    for idx, raw_item in enumerate(content):
        if isinstance(raw_item, str):
            blocks.append({"type": "text", "text": raw_item})
            continue
        item = _require_openai_content_part_dict(raw_item, idx)
        item_type = item.get("type")
        if item_type in ("text", "input_text", "output_text"):
            blocks.append({"type": "text", "text": str(item.get("text") or "")})
        elif item_type == "image_url":
            if role != "user":
                raise ValueError("OpenAI image_url content parts can only be converted for user messages")
            blocks.append({"type": "image", "source": openai_image_url_to_anthropic_source(item.get("image_url"))})
        else:
            raise ValueError(f"Unsupported OpenAI content part type for Anthropic conversion: {item_type}")
    return blocks


def _parse_openai_tool_arguments(arguments, tool_name: str):
    if arguments is None or arguments == "":
        return {}
    if isinstance(arguments, dict):
        return dict(arguments)
    if not isinstance(arguments, str):
        logger.warning(
            f"OpenAI tool call arguments for {tool_name or 'unknown'} must be a JSON object; falling back to {{}}"
        )
        return {}
    try:
        parsed = json.loads(arguments)
    except json.JSONDecodeError:
        logger.warning(
            f"OpenAI tool call arguments for {tool_name or 'unknown'} are not valid JSON; falling back to {{}}"
        )
        return {}
    if not isinstance(parsed, dict):
        logger.warning(
            f"OpenAI tool call arguments for {tool_name or 'unknown'} must decode to a JSON object; falling back to {{}}"
        )
        return {}
    return parsed


_ANTHROPIC_TOOL_ID_ALLOWED = re.compile(r"^[a-zA-Z0-9_-]+$")
_ANTHROPIC_TOOL_ID_ILLEGAL = re.compile(r"[^a-zA-Z0-9_-]")

# Anthropic Messages API 要求 tool_use.name 满足 ^[a-zA-Z0-9_-]{1,64}$。
# OpenAI chat 不校验 function.name，跨协议（Anthropic 客户端 → OpenAI 渠道 →
# Anthropic 原生壳，或 Responses 渠道）时超长 / 含非法字符的 name 会原样穿到上游
# 被拒（"Invalid tool use format" / "input[N].name too long"）。出站前统一归一化。
_ANTHROPIC_TOOL_NAME_MAX = 64

# 跨协议（OpenAI 上游 → Anthropic 客户端）转换时，reasoning_content 转成的 thinking
# 块没有上游真实 opaque 签名。带 interleaved-thinking 的客户端（Claude Code）要求
# thinking 块有非空 signature 才把它当合法块处理并保留回传——空 signature 会导致同一
# 轮里后续 tool_use 块不被识别为可执行调用，整段降级成文字。这里用一个非空、可识别、
# 不冒充真实签名的合成值补上；请求侧 sanitize_anthropic_request_body 会把它剔除，保证
# 它永不触达真实 Anthropic 上游（真 Anthropic 会拒绝伪造签名）。
PROXY_SYNTHETIC_THINKING_SIGNATURE = "cHJveHktc3ludGhldGljLXNpZ25hdHVyZQ"


def sanitize_anthropic_tool_ids_in_body(body: dict) -> dict:
    """对 Anthropic Messages API 请求体中的 tool_use.id / tool_result.tool_use_id / tool_use.name 做深度清洗。

    遍历 ``body["messages"]`` 中每条消息的 ``content`` 列表，找到 ``tool_use`` 块
    则对其 ``id`` 与 ``name`` 字段调用 :func:`sanitize_anthropic_tool_id` /
    :func:`sanitize_anthropic_tool_name`；找到 ``tool_result`` 块
    则对其 ``tool_use_id`` 字段调用同一函数。

    保证：
      - 同一 pair（assistant tool_use + user tool_result）共享同一个原始 id，
        经同一函数处理后仍然匹配。
      - 不触碰其他字段（system、tools、stop_sequences、thinking 等）。
      - 无 tool 块时返回浅拷贝，零开销。

    Args:
        body: 原始 Anthropic Messages API 请求体（dict）。

    Returns:
        一份已清洗的新 dict，结构完全一致。
    """
    import copy

    if not isinstance(body, dict):
        return body

    messages = body.get("messages")
    if not isinstance(messages, list):
        return copy.copy(body)

    # 快速检测：是否至少有一条消息包含含 tool 块的 content
    needs_rewrite = False
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        content = msg.get("content")
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") in ("tool_use", "tool_result"):
                    needs_rewrite = True
                    break
        if needs_rewrite:
            break

    if not needs_rewrite:
        return copy.copy(body)

    # 深度复制后逐个块清洗
    sanitized: dict = copy.deepcopy(body)
    for msg in sanitized.get("messages", []):
        if not isinstance(msg, dict):
            continue
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "tool_use":
                block["id"] = sanitize_anthropic_tool_id(block.get("id"))
                # name 同口径清洗:Anthropic 原生上游对 tool_use.name 校验 ^[a-zA-Z0-9_-]{1,64}$,
                # 超长/非法会以 "Invalid tool use format" 拒;直通场景下客户端历史里
                # 可能带上轮上游吐出的畸形 name,这里一并归一。
                if "name" in block:
                    block["name"] = sanitize_anthropic_tool_name(block.get("name"))
            elif block_type == "tool_result":
                block["tool_use_id"] = sanitize_anthropic_tool_id(block.get("tool_use_id"))
    return sanitized


def coerce_anthropic_system(system, system_type: str):
    """按渠道协议行配置的 system_type 强制 Anthropic ``system`` 字段形态。

    Anthropic ``system`` 允许字符串或文本块列表两种形态,不同上游接受度不一。

    - system_type 非 ``str``/``array``(含 ``auto``/空)→ 原样返回,保持"同协议直通
      原样、跨协议默认字符串"的现状,不做任何转化。
    - system 为空(None/"")→ 原样返回。
    - 目标 ``str``:列表则提取各 text 块的 ``text`` 用 "\\n\\n" 合并
      (字符串形态无法承载 cache_control 等,合并时丢弃);已是字符串原样返回。
    - 目标 ``array``:字符串包成 ``[{"type": "text", "text": s}]``;
      已是列表原样返回(保留 cache_control 等块级属性)。
    """
    target = str(system_type or "").lower()
    if target not in ("str", "array"):
        return system
    if system is None or system == "":
        return system
    if target == "str":
        if isinstance(system, str):
            return system
        if isinstance(system, list):
            parts = []
            for block in system:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text")
                    if text:
                        parts.append(str(text))
                elif isinstance(block, str) and block:
                    parts.append(block)
            return "\n\n".join(parts)
        return system
    # target == "array"
    if isinstance(system, str):
        return [{"type": "text", "text": system}]
    return system


def strip_assistant_reasoning_content(messages: list[dict]) -> list[dict]:
    """剥离 messages 中 assistant 角色的 reasoning_content 字段。

    用于 openai 协议行 ``send_reasoning_content=false`` 配置：不把思考内容
    透传给上游。返回新列表，仅对含 reasoning_content 的 assistant 消息做浅拷贝，
    其余消息原样保留引用（调用方传入的列表与消息对象不会被原地修改，避免跨重试复用）。
    无需剥离时原样返回同一列表。
    """
    if not isinstance(messages, list):
        return messages
    if not any(
        isinstance(m, dict) and m.get("role") == "assistant" and "reasoning_content" in m
        for m in messages
    ):
        return messages
    result: list[dict] = []
    for msg in messages:
        if (
            isinstance(msg, dict)
            and msg.get("role") == "assistant"
            and "reasoning_content" in msg
        ):
            new_msg = dict(msg)
            new_msg.pop("reasoning_content", None)
            result.append(new_msg)
        else:
            result.append(msg)
    return result


def sanitize_anthropic_request_body(body: dict) -> dict:
    """清洗发往 Anthropic Messages API 的请求体。

    - tool_use.id / tool_result.tool_use_id 规范化为 Anthropic 允许的字符集。
    - 删除无效的 thinking block：Anthropic 上游要求 thinking.signature 是上游返回的
      非空 opaque 签名；空 signature 无法伪造，继续透传会导致请求体反序列化/校验失败。
      我们跨协议转换时补的 PROXY_SYNTHETIC_THINKING_SIGNATURE 同理要剔除。
    """
    sanitized = sanitize_anthropic_tool_ids_in_body(body)
    if not isinstance(sanitized, dict):
        return sanitized

    messages = sanitized.get("messages")
    if not isinstance(messages, list):
        return sanitized

    import copy

    next_body = sanitized
    copied = False
    for msg_index, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        next_content = []
        changed = False
        for block in content:
            if isinstance(block, dict) and block.get("type") == "thinking":
                signature = block.get("signature")
                # 空 signature 无法被真实 Anthropic 校验通过；等于我们的合成签名的块也不能
                # 触达真实 Anthropic（会话中途切到 anthropic 协议渠道时会经直通被拒）。两者一律剔除。
                if (
                    not isinstance(signature, str)
                    or not signature.strip()
                    or signature == PROXY_SYNTHETIC_THINKING_SIGNATURE
                ):
                    changed = True
                    continue
            next_content.append(block)
        if not changed:
            continue
        if not copied:
            next_body = copy.deepcopy(sanitized)
            messages = next_body.get("messages")
            copied = True
        messages[msg_index]["content"] = next_content or ""
    return next_body


def sanitize_anthropic_tool_id(tool_id) -> str:
    """将任意来源的 tool 调用 ID 规范化为 Anthropic 允许的格式。

    Anthropic Messages API 要求 tool_use.id / tool_result.tool_use_id 仅由
    ``[a-zA-Z0-9_-]`` 组成（正则 ``^[a-zA-Z0-9_-]+$``）。但客户端（尤其是
    OpenAI / Responses 风格的工具命名）可能传入诸如 ``functions.todowrite:3``
    这类含 ``.`` / ``:`` 等非法字符的 ID，直接透传会被 Anthropic 拒绝。

    本函数对非法字符做确定性替换（非法字符 → ``_``），保证：
      - 同一个输入 ID 始终映射到同一个输出 ID（tool_use 与对应 tool_result 配对不断裂）。
      - 空值兜底生成新的合规 ID。

    Args:
        tool_id: 原始 tool 调用 ID（可能为 None / 空 / 含非法字符）。

    Returns:
        合规的 tool 调用 ID。
    """
    text = str(tool_id or "").strip()
    if not text:
        return f"toolu_{uuid.uuid4().hex[:24]}"
    if _ANTHROPIC_TOOL_ID_ALLOWED.match(text):
        return text
    sanitized = _ANTHROPIC_TOOL_ID_ILLEGAL.sub("_", text)
    # 替换后若仍为空（极端情况，如全部为非法字符且被压成空），兜底生成
    return sanitized or f"toolu_{uuid.uuid4().hex[:24]}"


def sanitize_anthropic_tool_name(name: str) -> str:
    """与 `sanitize_anthropic_tool_id` 同口径:非法字符→`_`、截断到 64、空→`tool`。
    保证同一个工具在所有转换链路（Anthropic 客户端→OpenAI 渠道→Anthropic 原生壳）
    输出的 name 一致,避免配对断裂和上游 "Invalid tool use format"。

    Args:
        name: 原始工具名（可能超长或含非法字符）

    Returns:
        合规的 tool_use.name / function.name
    """
    text = str(name or "").strip()
    if not text:
        return "tool"
    if _ANTHROPIC_TOOL_ID_ALLOWED.match(text) and len(text) <= _ANTHROPIC_TOOL_NAME_MAX:
        return text
    sanitized = _ANTHROPIC_TOOL_ID_ILLEGAL.sub("_", text)
    return sanitized[:_ANTHROPIC_TOOL_NAME_MAX] or "tool"


def openai_tool_calls_to_anthropic_blocks(tool_calls: list | None) -> list[dict]:
    """将 OpenAI assistant tool_calls 转换为 Anthropic tool_use blocks。"""
    blocks: list[dict] = []
    for tool_call in tool_calls or []:
        if not isinstance(tool_call, dict):
            logger.warning("OpenAI tool_call entry is not an object; skipping")
            continue
        function = tool_call.get("function") or {}
        if not isinstance(function, dict):
            function = {}
        if not function.get("name"):
            # No function.name → the block below fabricates name="tool" and a
            # synthetic toolu_ id with zero input, producing a plausible-but-wrong
            # orphan tool_use that Anthropic then rejects. Surface it so a
            # misbehaving caller (agent replay path that forgot to normalize to
            # wire shape) is discoverable instead of silent.
            logger.warning(
                "openai_tool_calls_to_anthropic_blocks: tool_call has no "
                "function.name — fabricating name='tool' + synthetic id. Caller "
                "must send wire shape {id, type, function: {name, arguments}}."
            )
        name = sanitize_anthropic_tool_name(function.get("name"))
        blocks.append({
            "type": "tool_use",
            "id": sanitize_anthropic_tool_id(tool_call.get("id")),
            "name": name,
            "input": _parse_openai_tool_arguments(function.get("arguments"), name),
        })
    return blocks


def openai_tool_message_to_anthropic_block(message: dict) -> dict:
    """将 OpenAI role=tool 消息转换为 Anthropic tool_result block。"""
    tool_call_id = message.get("tool_call_id") or message.get("id") or f"toolu_{uuid.uuid4().hex[:24]}"
    tool_call_id = sanitize_anthropic_tool_id(tool_call_id)
    return {
        "type": "tool_result",
        "tool_use_id": tool_call_id,
        "content": openai_content_to_text(message.get("content", "")),
    }


def _openai_system_content_to_text(content) -> str:
    if content is None or content == "":
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        content = [content]
    if isinstance(content, list):
        parts = []
        for idx, raw_item in enumerate(content):
            if isinstance(raw_item, str):
                parts.append(raw_item)
                continue
            item = _require_openai_content_part_dict(raw_item, idx)
            item_type = item.get("type")
            if item_type in ("text", "input_text", "output_text"):
                parts.append(str(item.get("text") or ""))
            else:
                raise ValueError(f"Unsupported OpenAI system content part type for Anthropic conversion: {item_type}")
        return "\n".join([part for part in parts if part])
    return str(content)


def openai_messages_to_anthropic_messages(messages: list[dict]) -> tuple[list[str], list[dict]]:
    """将 OpenAI Chat messages 显式转换为 Anthropic system parts 和 messages。"""
    system_parts: list[str] = []
    anthropic_messages: list[dict] = []
    for msg in messages or []:
        if not isinstance(msg, dict):
            raise ValueError("OpenAI messages entries must be objects")
        role = msg.get("role") or "user"
        content = msg.get("content", "")
        if role in ("system", "developer"):
            text = _openai_system_content_to_text(content)
            if text:
                system_parts.append(text)
            continue
        if role == "tool":
            anthropic_messages.append({"role": "user", "content": [openai_tool_message_to_anthropic_block(msg)]})
            continue
        if role not in ("user", "assistant"):
            raise ValueError(f"Unsupported OpenAI message role for Anthropic conversion: {role}")

        blocks: list[dict] = []
        if role == "assistant" and msg.get("reasoning_content"):
            blocks.append({"type": "thinking", "thinking": str(msg.get("reasoning_content") or ""), "signature": ""})
        blocks.extend(openai_content_to_anthropic_blocks(content, role=role))
        if role == "assistant" and msg.get("tool_calls"):
            blocks.extend(openai_tool_calls_to_anthropic_blocks(msg.get("tool_calls") or []))
        anthropic_messages.append({"role": role, "content": blocks if blocks else ""})
    return system_parts, anthropic_messages


def combine_messages(messages: list[dict], tools_prompt: str = "") -> str:
    """将多轮对话消息拼接成一条完整的对话上下文。"""
    if not messages:
        return tools_prompt

    if not tools_prompt and len(messages) == 1 and messages[0].get("role") == "user":
        content = openai_content_to_text(messages[0].get("content", ""))
        if content:
            return content

    parts = [MESSAGE_TAG_GUIDE]

    if tools_prompt:
        parts.append(f"<system_instruction>\n{tools_prompt}\n</system_instruction>")

    role_tags = {
        "system": "system_instruction",
        "user": "user_message",
        "assistant": "assistant_message",
    }

    for msg in messages:
        role = msg.get("role", "user")
        content = openai_content_to_text(msg.get("content", ""))
        reasoning_content = ""
        if role == "assistant":
            reasoning_content = msg.get("reasoning_content") or msg.get("thinking") or ""

        if reasoning_content:
            parts.append(f"<assistant_reasoning_context>\n{reasoning_content}\n</assistant_reasoning_context>")

        if role == "assistant" and msg.get("tool_calls"):
            tool_calls_str = []
            for tc in msg["tool_calls"]:
                # Guard: a non-object entry (e.g. a bare string) would raise
                # AttributeError here and escape the ValueError→400 mapping at
                # the provider boundary as a 500. Skip it visibly instead.
                if not isinstance(tc, dict):
                    logger.warning(
                        "combine_messages: non-object tool_call entry skipped "
                        "(would raise AttributeError). Caller must send wire shape."
                    )
                    continue
                func = tc.get("function")
                if not isinstance(func, dict) or not func.get("name"):
                    logger.warning(
                        "combine_messages: assistant tool_call has no function.name — "
                        "rendering an empty stub. Caller must send wire shape "
                        "{id, type, function: {name, arguments}} plus paired role=tool."
                    )
                    func = func if isinstance(func, dict) else {}
                name = sanitize_anthropic_tool_name(func.get("name"))
                args_preview = func.get("arguments", "")
                tool_calls_str.append(f"<tool_call name=\"{name}\">{args_preview}</tool_call>")
            tool_calls_content = "\n".join(tool_calls_str)
            parts.append(f"<assistant_tool_calls>\n{tool_calls_content}\n</assistant_tool_calls>")

        if not content:
            continue

        if role == "tool":
            # 仅用消息自带的 name；缺失时统一 "tool"，绝不把 tool_call_id 截断当名字
            tool_name = sanitize_anthropic_tool_name(msg.get("name"))
            parts.append(f"<tool_result name=\"{tool_name}\">\n{content}\n</tool_result>")
            continue

        tag = role_tags.get(role, "message")
        parts.append(f"<{tag}>\n{content}\n</{tag}>")

    result = "\n\n".join(parts)

    # 记录拼接结果长度（用于调试上下文过长问题）
    if len(result) > 100000:
        logger.warning(f"[combine_messages] result_len={len(result)}, parts_count={len(parts)}, "
                       f"tools_prompt_len={len(tools_prompt)}, may_cause_xiaomi_empty_response")

    return result


def make_openai_chunk(completion_id: str, model: str, delta: dict, finish_reason: str = None) -> str:
    """生成 OpenAI 格式的 SSE chunk"""
    chunk = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason
        }]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"


def make_openai_response(completion_id: str, model: str, content: str,
                         thinking: str = "", tool_calls: list = None) -> dict:
    """生成 OpenAI 格式的完整响应。"""
    message = {
        "role": "assistant",
        "content": content
    }

    if thinking:
        message["reasoning_content"] = thinking

    if tool_calls:
        message["tool_calls"] = tool_calls

    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": "stop" if not tool_calls else "tool_calls"
        }],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
    }


def iter_sse_data_values(event_str: str):
    if not isinstance(event_str, str):
        return
    for line in event_str.strip().split('\n'):
        stripped = line.strip()
        marker = "data:"
        idx = stripped.find(marker)
        if idx < 0:
            continue
        yield stripped[idx + len(marker):].strip()


def parse_sse_data_value(data_text: str):
    text = str(data_text or "").strip()
    if not text:
        return None
    if text.startswith("[DONE]"):
        return "[DONE]"
    if text.strip().lower() == "done":
        return "[DONE]"
    json_start = -1
    for char in ("{", "["):
        pos = text.find(char)
        if pos >= 0 and (json_start < 0 or pos < json_start):
            json_start = pos
    if json_start < 0:
        return None
    try:
        value, _ = json.JSONDecoder().raw_decode(text[json_start:])
        return value
    except json.JSONDecodeError:
        return None


def iter_sse_payloads(event_str: str):
    for data_text in iter_sse_data_values(event_str):
        value = parse_sse_data_value(data_text)
        if value == "[DONE]" or isinstance(value, dict):
            yield value


def parse_sse_event(event_str: str) -> tuple[str, dict]:
    """解析 SSE 事件，返回 (event_type, data)"""
    event_type = ""
    data = {}

    for line in event_str.strip().split('\n'):
        stripped = line.strip()
        if stripped.startswith("event:"):
            event_value = stripped[6:].strip()
            data_marker = " data:"
            data_idx = event_value.find(data_marker)
            if data_idx >= 0:
                event_type = event_value[:data_idx].strip()
                payload = parse_sse_data_value(event_value[data_idx + 1:])
                if isinstance(payload, dict):
                    data = payload
            else:
                event_type = event_value
        elif "data:" in stripped:
            payload = parse_sse_data_value(stripped[stripped.find("data:"):])
            if isinstance(payload, dict):
                data = payload

    if not event_type and isinstance(data, dict):
        event_type = str(data.get("type") or "")
    return event_type, data


def generate_completion_id() -> str:
    """生成 OpenAI 格式的 completion ID。"""
    return f"chatcmpl-{uuid.uuid4().hex[:29]}"


# ==================== Anthropic Messages API 格式 ====================

def generate_anthropic_message_id() -> str:
    """生成 Anthropic 格式的 message ID"""
    return f"msg_{uuid.uuid4().hex[:24]}"


def make_anthropic_sse_event(event_type: str, data: dict) -> str:
    """生成 Anthropic 格式的 SSE 事件"""
    return f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def normalize_anthropic_sse_event(event_str: str) -> str:
    """把上游非标准的 SSE 事件文本规范化为 ``event: X\ndata: {...}\n\n``。

    兼容上游返回中常见的两类异常：
    1) 每行带前导空格（``" event: ..."`` / ``" data: ..."``）—— 严格 SSE 客户端会忽略这些行；
    2) ``\\r\\n`` 与 ``\\n`` 混合换行，或尾部多余空白。

    若 event/data 无法解析则原样返回，避免影响已可工作的通路。
    """
    if not isinstance(event_str, str) or not event_str:
        return event_str
    text = event_str.replace("\r\n", "\n").replace("\r", "\n")
    event_type = ""
    data_lines: list[str] = []
    for raw_line in text.split("\n"):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith(":"):
            continue
        if stripped.startswith("event:"):
            event_type = stripped[len("event:"):].strip()
        elif stripped.startswith("data:"):
            data_lines.append(stripped[len("data:"):].strip())
        else:
            data_lines.append(stripped)
    data_text = "\n".join(data_lines).strip()
    if not event_type and not data_text:
        return event_str
    payload = parse_sse_data_value(data_text) if data_text else None
    if payload == "[DONE]":
        return "data: [DONE]\n\n"
    if not isinstance(payload, dict):
        return event_str
    if not event_type:
        event_type = str(payload.get("type") or "").strip()
    if not event_type:
        return event_str
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def anthropic_to_openai_messages(body: dict) -> tuple[list[dict], dict]:
    """将 Anthropic Messages API 请求转换为 OpenAI 格式。

    Returns: (messages, kwargs) 元组
    """
    messages = []
    input_summaries = []
    # tool_use.id → tool_use.name 映射：Anthropic tool_result 不带 name，
    # 转成 OpenAI role=tool 时需从前面的 tool_use 回带，避免下游用 tool_call_id 兜底当名字。
    tool_id_to_name: dict[str, str] = {}

    # 处理 system prompt
    system = body.get("system")
    if system:
        system_text = ""
        if isinstance(system, str):
            system_text = system
        elif isinstance(system, list):
            for block in system:
                if block.get("type") == "text":
                    system_text += block.get("text", "")
        if system_text:
            messages.append({"role": "system", "content": system_text})

    # 转换 messages
    for msg in body.get("messages", []):
        role = msg.get("role")
        content = msg.get("content")
        reasoning_content = msg.get("reasoning_content", "")

        if isinstance(content, list):
            input_summaries.append({
                "role": role,
                "blocks": [
                    {
                        "type": block.get("type"),
                        "keys": sorted(block.keys()),
                        "text_len": len(block.get("text", "")),
                        "thinking_len": len(block.get("thinking", "")),
                        "data_len": len(block.get("data", "")),
                        "has_signature": bool(block.get("signature")),
                    }
                    for block in content
                ],
                "top_reasoning_len": len(reasoning_content),
            })
        else:
            input_summaries.append({
                "role": role,
                "content_type": type(content).__name__,
                "content_len": len(content) if isinstance(content, str) else 0,
                "top_reasoning_len": len(reasoning_content),
            })

        if isinstance(content, str):
            msg_dict = {"role": role, "content": content}
            if role == "assistant" and reasoning_content:
                msg_dict["reasoning_content"] = reasoning_content
            messages.append(msg_dict)
        elif isinstance(content, list):
            text_parts = []
            thinking_parts = []
            redacted_thinking_parts = []
            tool_calls = []
            tool_results = []

            for block in content:
                block_type = block.get("type")
                if block_type == "text":
                    text_parts.append(block.get("text", ""))
                elif block_type == "thinking":
                    thinking = block.get("thinking", "")
                    if thinking:
                        thinking_parts.append(thinking)
                elif block_type == "redacted_thinking":
                    redacted = block.get("data", "")
                    if redacted:
                        redacted_thinking_parts.append(redacted)
                elif block_type == "tool_use":
                    tool_use_id = sanitize_anthropic_tool_id(block.get("id"))
                    tool_use_name = sanitize_anthropic_tool_name(block.get("name"))
                    if block.get("name"):
                        # 回带给后续 tool_result 的 role=tool.name;存清洗后的值,
                        # 与 tool_calls[].function.name 同源,保证配对一致。
                        tool_id_to_name[tool_use_id] = tool_use_name
                    tool_calls.append({
                        "id": tool_use_id,
                        "type": "function",
                        "function": {
                            "name": tool_use_name,
                            "arguments": json.dumps(block.get("input", {}), ensure_ascii=False)
                        }
                    })
                elif block_type == "tool_result":
                    result_content = block.get("content", "")
                    if isinstance(result_content, list):
                        texts = [rb.get("text", "") for rb in result_content if rb.get("type") == "text"]
                        result_content = "\n".join(texts)
                    tool_results.append({
                        "tool_use_id": sanitize_anthropic_tool_id(block.get("tool_use_id")),
                        "content": str(result_content)
                    })

            if role == "assistant":
                msg_dict = {"role": "assistant", "content": "\n".join(text_parts) if text_parts else ""}
                thinking = reasoning_content or "\n".join(thinking_parts)
                if thinking:
                    msg_dict["reasoning_content"] = thinking
                if tool_calls:
                    msg_dict["tool_calls"] = tool_calls
                messages.append(msg_dict)
            elif role == "user":
                # Anthropic 中 tool_result 在 user 消息内，OpenAI 用 tool 角色
                for tr in tool_results:
                    tool_msg = {
                        "role": "tool",
                        "content": tr["content"],
                        "tool_call_id": tr["tool_use_id"]
                    }
                    # 从前面的 tool_use 回带原始工具名；orphan result 不发明名字，留给下游 "tool" 兜底
                    recovered_name = tool_id_to_name.get(tr["tool_use_id"])
                    if recovered_name:
                        tool_msg["name"] = recovered_name
                    messages.append(tool_msg)
                if text_parts:
                    messages.append({"role": "user", "content": "\n".join(text_parts)})

    # 构建 kwargs
    kwargs = {
        "max_tokens": body.get("max_tokens"),
        "tools": None,
    }
    if body.get("temperature") is not None:
        kwargs["temperature"] = body.get("temperature")

    # 转换 tools
    anthropic_tools = body.get("tools")
    if anthropic_tools:
        openai_tools = []
        for tool in anthropic_tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": sanitize_anthropic_tool_name(tool.get("name")),
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {})
                }
            })
        kwargs["tools"] = openai_tools

    tool_choice = body.get("tool_choice")
    if isinstance(tool_choice, dict):
        if tool_choice.get("type") == "tool":
            kwargs["tool_choice"] = {"type": "function", "function": {"name": sanitize_anthropic_tool_name(tool_choice.get("name"))}}
        elif tool_choice.get("type") in ("auto", "any", "none"):
            kwargs["tool_choice"] = "required" if tool_choice.get("type") == "any" else tool_choice.get("type")
    elif tool_choice is not None:
        kwargs["tool_choice"] = tool_choice

    if body.get("stop_sequences") is not None:
        kwargs["stop"] = body.get("stop_sequences")
    for key in ("top_p", "top_k", "metadata", "service_tier", "container", "context_management", "mcp_servers", "stream_options", "parallel_tool_calls", "user"):
        if body.get(key) is not None:
            kwargs[key] = body.get(key)

    # 处理 thinking
    thinking = body.get("thinking")
    if isinstance(thinking, dict) and thinking.get("type") == "enabled":
        kwargs["thinking_enabled"] = True
        kwargs["thinking_mode"] = "Thinking"
        kwargs["auto_thinking"] = False
        budget = thinking.get("budget_tokens")
        if budget is not None:
            kwargs["thinking_budget"] = budget

    assistant_reasoning_count = sum(
        1 for msg in messages
        if msg.get("role") == "assistant" and msg.get("reasoning_content")
    )
    input_summary_json = json.dumps(input_summaries, ensure_ascii=False)
    if len(input_summary_json) > 500:
        input_summary_json = input_summary_json[:500] + "...[truncated]"
    logger.debug(f"anthropic_to_openai_messages - tools_count={len(kwargs.get('tools') or [])}, "
                 f"thinking_enabled={kwargs.get('thinking_enabled')}, "
                 f"thinking_mode={kwargs.get('thinking_mode')}, "
                 f"messages_count={len(messages)}, assistant_reasoning_count={assistant_reasoning_count}, "
                 f"input_summaries={input_summary_json}")
    return messages, kwargs


# ──────────────────────────────────────────────
# Qwen thinking 辅助函数
# ──────────────────────────────────────────────

QWEN_THINKING_MODES = ("Fast", "Thinking", "Auto")
"""Qwen 支持的 thinking_mode 枚举值。

参考开源项目:
- https://github.com/songquanpeng/one-api/blob/main/relay/adaptor/qwen/main.go
"""


def map_reasoning_effort_to_qwen(effort: Optional[str]) -> dict:
    """Map OpenAI reasoning_effort to Qwen upstream kwargs.
    
    Mapping table (exact per spec):
        None / missing / "" -> {"thinking_enabled": False, "thinking_mode": "Fast", "auto_thinking": False}
        "low"               -> {"thinking_enabled": True, "thinking_mode": "Auto", "auto_thinking": True}
        "medium"            -> {"thinking_enabled": True, "thinking_mode": "Thinking", "auto_thinking": False}
        "high"              -> {"thinking_enabled": True, "thinking_mode": "Thinking", "auto_thinking": False}
        "auto"              -> {"thinking_enabled": True, "thinking_mode": "Auto", "auto_thinking": True}
        unknown string      -> {"thinking_enabled": False, "thinking_mode": "Fast", "auto_thinking": False}
    """
    if not effort:
        return {"thinking_enabled": False, "thinking_mode": "Fast", "auto_thinking": False}
    
    normalized = effort.lower()
    if normalized == "low":
        return {"thinking_enabled": True, "thinking_mode": "Auto", "auto_thinking": True}
    elif normalized in ("medium", "high"):
        return {"thinking_enabled": True, "thinking_mode": "Thinking", "auto_thinking": False}
    elif normalized == "auto":
        return {"thinking_enabled": True, "thinking_mode": "Auto", "auto_thinking": True}
    else:
        return {"thinking_enabled": False, "thinking_mode": "Fast", "auto_thinking": False}


def normalize_legacy_thinking_mode(value: str) -> str:
    """将旧版 thinking_mode 值规范化为 Qwen 标准值。

    映射表:
        "Disabled" / "disabled" -> "Fast"
        "Enabled" / "enabled"   -> "Thinking"
        "Fast" | "Thinking" | "Auto" -> 保持原值
        其他值                  -> "Fast"（安全兜底）

    参考开源项目:
    - https://github.com/songquanpeng/one-api/blob/main/relay/adaptor/qwen/main.go

    Args:
        value: 旧版 thinking_mode 值（不区分大小写）。

    Returns:
        规范化的 Qwen thinking_mode 值。
    """
    if not value:
        return "Fast"

    lower = value.lower()
    normalization = {
        "disabled": "Fast",
        "enabled": "Thinking",
        "fast": "Fast",
        "thinking": "Thinking",
        "auto": "Auto",
    }
    return normalization.get(lower, "Fast")


def derive_auto_thinking_from_mode(mode: str) -> bool:
    """根据 thinking_mode 推断是否启用 auto_thinking。

    仅当 mode 为 "Auto"（不区分大小写）时返回 True。

    Args:
        mode: Qwen thinking_mode 值。

    Returns:
        是否启用 auto_thinking。
    """
    if not mode:
        return False
    return mode.lower() == "auto"


# ──────────────────────────────────────────────
# 思考体系分类（thinking vs reasoning）
# ──────────────────────────────────────────────

def classify_thinking_system(owned_by: Optional[str], reasoning_owned_by: list[str]) -> str:
    """将模型按 owned_by 归类为 "reasoning" 或 "thinking" 体系。

    使用 EXACT（精确）+ 大小写不敏感匹配，而非子串匹配。
    例如 owned_by="openai" 命中 "openai"，但 owned_by="openai-proxy" 不命中 "openai"。

    Args:
        owned_by: 模型归属方标识（可能为 None / 空字符串）。
        reasoning_owned_by: 归入 reasoning 体系的 owned_by 列表
            （由 Config.get_reasoning_owned_by() 提供，默认 ["openai", "deepseek", "google"]）。

    Returns:
        "reasoning" 若 owned_by 精确（大小写不敏感）命中列表，否则 "thinking"。
        owned_by 为 None / 空 -> "thinking"。
    """
    if not owned_by:
        return "thinking"
    normalized = owned_by.strip().lower()
    if not normalized:
        return "thinking"
    candidates = {
        str(item).strip().lower()
        for item in (reasoning_owned_by or [])
        if str(item).strip()
    }
    return "reasoning" if normalized in candidates else "thinking"


def openai_to_anthropic_response(openai_resp: dict, model: str) -> dict:
    """将 OpenAI 格式响应转换为 Anthropic Messages API 格式"""
    choice_list = openai_resp.get("choices") or [{}]
    choice = choice_list[0]
    message = choice.get("message", {}) or {}
    content_str = message.get("content", "") or ""
    thinking_str = message.get("reasoning_content", "") or ""
    tool_calls = message.get("tool_calls", []) or []
    finish_reason = choice.get("finish_reason", "stop") or "stop"

    content_blocks = []

    if thinking_str:
        content_blocks.append({
            "type": "thinking",
            "thinking": thinking_str,
            "signature": PROXY_SYNTHETIC_THINKING_SIGNATURE
        })

    if content_str:
        content_blocks.append({"type": "text", "text": content_str})

    for tc in tool_calls:
        func = tc.get("function", {})
        raw_args = func.get("arguments", "{}")
        if isinstance(raw_args, dict):
            input_dict = raw_args
        else:
            try:
                input_dict = json.loads(raw_args)
            except json.JSONDecodeError:
                input_dict = {}
        content_blocks.append({
            "type": "tool_use",
            "id": sanitize_anthropic_tool_id(tc.get("id")),
            "name": sanitize_anthropic_tool_name(func.get("name")),
            "input": input_dict
        })

    if not content_blocks:
        content_blocks.append({"type": "text", "text": ""})

    stop_reason_map = {"stop": "end_turn", "tool_calls": "tool_use", "length": "max_tokens"}

    return {
        "id": openai_resp.get("id") or generate_anthropic_message_id(),
        "type": "message",
        "role": "assistant",
        "content": content_blocks,
        "model": model,
        "stop_reason": stop_reason_map.get(finish_reason, "end_turn"),
        "stop_sequence": None,
        "usage": {
            "input_tokens": openai_resp.get("usage", {}).get("prompt_tokens", 0),
            "output_tokens": openai_resp.get("usage", {}).get("completion_tokens", 0)
        }
    }


def _json_dumps_compact(value) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _responses_text_from_content(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                item_type = item.get("type")
                if item_type in ("input_text", "output_text", "text"):
                    parts.append(str(item.get("text") or ""))
                elif item.get("text") is not None:
                    parts.append(str(item.get("text") or ""))
                elif item.get("content") is not None:
                    parts.append(_responses_text_from_content(item.get("content")))
        return "\n".join([p for p in parts if p])
    return _json_dumps_compact(content)


def _responses_tool_to_openai(tool: dict) -> dict | None:
    if not isinstance(tool, dict):
        return None
    if tool.get("type") != "function":
        return None
    if isinstance(tool.get("function"), dict):
        # 已是 OpenAI function 形态:原样透传(保留额外字段)。name 清洗留给
        # 出站 _build_openai_payload / sanitize_anthropic_request_body 统一处理。
        return tool
    name = sanitize_anthropic_tool_name(tool.get("name"))
    if name == "tool" and not str(tool.get("name") or "").strip():
        # 原始 name 为空才丢弃;清洗后落到 "tool" 兜底的仍保留(避免丢工具)。
        return None
    function = {"name": name}
    if tool.get("description") is not None:
        function["description"] = tool.get("description")
    if tool.get("parameters") is not None:
        function["parameters"] = tool.get("parameters")
    return {"type": "function", "function": function}


def _openai_tool_to_responses(tool: dict) -> dict:
    if not isinstance(tool, dict):
        return tool
    if tool.get("type") == "function":
        function = tool.get("function") or {}
        if not isinstance(function, dict):
            return tool
        result = {"type": "function", "name": sanitize_anthropic_tool_name(function.get("name"))}
        if function.get("description") is not None:
            result["description"] = function.get("description")
        if function.get("parameters") is not None:
            result["parameters"] = function.get("parameters")
        return result
    return tool


def responses_usage_to_openai_usage(usage: dict) -> dict:
    if not isinstance(usage, dict):
        return {}
    details = usage.get("input_tokens_details") if isinstance(usage.get("input_tokens_details"), dict) else {}
    prompt = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
    completion = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
    cached = int(usage.get("cached_tokens") or details.get("cached_tokens") or usage.get("cache_read_input_tokens") or 0)
    cache_creation = int(
        usage.get("cache_creation_tokens")
        or usage.get("cache_creation_input_tokens")
        or details.get("cache_creation_tokens")
        or details.get("cache_creation_input_tokens")
        or 0
    )
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
        "cached_tokens": cached,
        "cache_creation_tokens": cache_creation,
        "reasoning_tokens": int((usage.get("output_tokens_details") or {}).get("reasoning_tokens") or usage.get("reasoning_tokens") or 0),
        "prompt_tokens_details": {"cached_tokens": cached, "cache_creation_input_tokens": cache_creation},
        "completion_tokens_details": {"reasoning_tokens": int((usage.get("output_tokens_details") or {}).get("reasoning_tokens") or usage.get("reasoning_tokens") or 0)},
    }


def openai_usage_to_responses_usage(usage: dict | None) -> dict:
    normalized = responses_usage_to_openai_usage(usage or {})
    return {
        "input_tokens": normalized.get("prompt_tokens", 0),
        "output_tokens": normalized.get("completion_tokens", 0),
        "total_tokens": normalized.get("total_tokens", 0),
        "input_tokens_details": {
            "cached_tokens": normalized.get("cached_tokens", 0),
            "cache_creation_tokens": normalized.get("cache_creation_tokens", 0),
        },
    }


def normalize_responses_input(input_items) -> list:
    """清洗 Responses input 数组中的 function_call / function_call_output。"""
    normalized = []
    if not isinstance(input_items, list):
        return normalized
    for item in input_items:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "function_call":
            call_id = item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:24]}"
            normalized.append({
                **item,
                "type": "function_call",
                "call_id": call_id,
                "name": sanitize_anthropic_tool_name(item.get("name")),
                "arguments": item.get("arguments") or "{}",
            })
            continue
        if item_type == "function_call_output":
            normalized.append({
                **item,
                "type": "function_call_output",
                "call_id": item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:24]}",
                "output": item.get("output") or "",
            })
            continue
        normalized.append(item)
    return normalized


def normalize_responses_tools(tools) -> list:
    normalized = []
    if not isinstance(tools, list):
        return normalized
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        tool_type = tool.get("type")
        if tool_type == "function":
            converted = _openai_tool_to_responses(tool) if isinstance(tool.get("function"), dict) else dict(tool)
            if str(converted.get("name") or "").strip():
                normalized.append(converted)
            continue
        # Responses 原生工具类型（web_search / file_search / computer_use 等）原样保留；
        # 未知 type 但带 name 的也保留（部分上游接受自定义 type），无 name 的丢弃。
        if str(tool.get("name") or "").strip():
            normalized.append(tool)
    return normalized


def responses_to_openai_messages(body: dict) -> tuple[list[dict], dict]:
    messages: list[dict] = []
    instructions = body.get("instructions")
    if isinstance(instructions, str) and instructions:
        messages.append({"role": "system", "content": instructions})

    input_value = body.get("input")
    if isinstance(input_value, str):
        messages.append({"role": "user", "content": input_value})
    elif isinstance(input_value, list):
        pending_call_ids: list[str] = []
        # call_id → function name：function_call_output 不带 name，
        # 转成 OpenAI role=tool 时从前面的 function_call 回带，避免下游用 call_id 兜底当名字。
        call_id_to_name: dict[str, str] = {}
        for item in input_value:
            if isinstance(item, str):
                messages.append({"role": "user", "content": item})
                continue
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            if item_type == "function_call_output":
                call_id = item.get("call_id") or item.get("id") or (pending_call_ids.pop(0) if pending_call_ids else None) or f"call_{uuid.uuid4().hex[:24]}"
                tool_msg = {
                    "role": "tool",
                    "content": _responses_text_from_content(item.get("output")),
                    "tool_call_id": call_id,
                }
                recovered_name = call_id_to_name.get(call_id)
                if recovered_name:
                    tool_msg["name"] = recovered_name
                messages.append(tool_msg)
                continue
            if item_type == "function_call":
                call_id = item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:24]}"
                pending_call_ids.append(call_id)
                if item.get("name"):
                    call_id_to_name[call_id] = item.get("name")
                messages.append({
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": sanitize_anthropic_tool_name(item.get("name")),
                            "arguments": _json_dumps_compact(item.get("arguments")) or "{}",
                        },
                    }],
                })
                continue
            role = item.get("role") or ("assistant" if item_type == "message" and item.get("role") == "assistant" else "user")
            if role == "developer":
                role = "system"
            content = _responses_text_from_content(item.get("content"))
            if content or role in ("system", "user", "assistant"):
                messages.append({"role": role, "content": content})

    kwargs: dict = {}
    if body.get("max_output_tokens") is not None:
        kwargs["max_tokens"] = body.get("max_output_tokens")
    if body.get("text") is not None:
        kwargs["text"] = body.get("text")
    for key in (
        "temperature", "top_p", "tool_choice", "parallel_tool_calls", "store", "include",
        "truncation", "previous_response_id", "reasoning", "service_tier", "user",
        "prompt_cache_key", "client_metadata", "metadata", "stream_options",
    ):
        if body.get(key) is not None:
            kwargs[key] = body.get(key)
    if body.get("tools") is not None:
        tools = [_responses_tool_to_openai(t) for t in (body.get("tools") or [])]
        kwargs["tools"] = [t for t in tools if t]
    return messages, kwargs


def _openai_content_to_responses_parts(content, role: str) -> list[dict]:
    text_type = "input_text" if role == "user" else "output_text"
    if content is None or content == "":
        return []
    if isinstance(content, str):
        return [{"type": text_type, "text": content}]
    if isinstance(content, dict):
        content = [content]
    if not isinstance(content, list):
        return [{"type": text_type, "text": str(content)}]

    parts: list[dict] = []
    for item in content:
        if isinstance(item, str):
            parts.append({"type": text_type, "text": item})
            continue
        if not isinstance(item, dict):
            if item is not None:
                parts.append({"type": text_type, "text": str(item)})
            continue
        item_type = item.get("type")
        if item_type in ("text", "input_text", "output_text"):
            parts.append({"type": text_type, "text": str(item.get("text") or "")})
        elif item_type == "image_url":
            if role != "user":
                parts.append({"type": text_type, "text": openai_content_to_text([item])})
                continue
            url = _openai_image_url_value(item.get("image_url"))
            if url:
                parts.append({"type": "input_image", "image_url": url})
        elif item_type == "input_image":
            if role != "user":
                parts.append({"type": text_type, "text": openai_content_to_text([item])})
                continue
            url = item.get("image_url") or item.get("url")
            part = {"type": "input_image"}
            if url is not None:
                part["image_url"] = url
            if item.get("file_id") is not None:
                part["file_id"] = item.get("file_id")
            parts.append(part)
        elif item.get("text") is not None or item.get("content") is not None:
            parts.append({"type": text_type, "text": openai_content_to_text([item])})
        else:
            parts.append({"type": text_type, "text": _json_dumps_compact(item)})
    return parts


def openai_messages_to_responses_payload(model: str, messages: list[dict], stream: bool, kwargs: dict) -> dict:
    payload = {"model": model, "input": [], "stream": stream}
    instruction_parts = []
    pending_call_ids: list[str] = []
    for message in messages or []:
        role = message.get("role", "user")
        content = message.get("content", "")
        if role in ("system", "developer"):
            if content:
                instruction_parts.append(_responses_text_from_content(content))
            continue
        if role == "tool":
            call_id = message.get("tool_call_id") or (pending_call_ids.pop(0) if pending_call_ids else None) or f"call_{uuid.uuid4().hex[:24]}"
            payload["input"].append({
                "type": "function_call_output",
                "call_id": call_id,
                "output": _responses_text_from_content(content),
            })
            continue
        if role == "assistant" and message.get("tool_calls"):
            for tool_call in message.get("tool_calls") or []:
                function = tool_call.get("function") or {}
                if not isinstance(function, dict):
                    function = {}
                if not function.get("name"):
                    logger.warning(
                        "openai_messages_to_responses_payload: tool_call has no "
                        "function.name — fabricating name='tool' + synthetic call_id. "
                        "Caller must send wire shape {id, type, function: {name, arguments}}."
                    )
                call_id = tool_call.get("id") or f"call_{uuid.uuid4().hex[:24]}"
                pending_call_ids.append(call_id)
                payload["input"].append({
                    "type": "function_call",
                    "call_id": call_id,
                    "name": sanitize_anthropic_tool_name(function.get("name")),
                    "arguments": function.get("arguments") or "{}",
                })
        if content or role in ("user", "assistant"):
            content_parts = _openai_content_to_responses_parts(content, role)
            if not content_parts and role in ("user", "assistant"):
                content_parts = [{"type": "input_text" if role == "user" else "output_text", "text": ""}]
            payload["input"].append({
                "role": role,
                "content": content_parts,
            })
    if instruction_parts:
        payload["instructions"] = "\n".join(instruction_parts)
    if kwargs.get("max_tokens") is not None:
        payload["max_output_tokens"] = kwargs.get("max_tokens")
    if kwargs.get("text") is not None:
        payload["text"] = kwargs.get("text")
    if kwargs.get("tools") is not None:
        payload["tools"] = normalize_responses_tools(kwargs.get("tools"))
    for key in (
        "temperature", "top_p", "tool_choice", "parallel_tool_calls", "store", "include",
        "truncation", "previous_response_id", "reasoning", "service_tier", "user",
        "prompt_cache_key", "client_metadata", "metadata", "stream_options",
    ):
        if kwargs.get(key) is not None:
            payload[key] = kwargs.get(key)
    # 跨协议 thinking 映射：OpenAI reasoning_effort → Responses reasoning.effort。
    # 关闭思考（none/minimal）省略 reasoning，不显式下发；已带 reasoning 对象时不覆盖。
    if payload.get("reasoning") is None:
        effort = kwargs.get("reasoning_effort")
        if effort and effort not in ("minimal", "none"):
            payload["reasoning"] = {"effort": effort}
    return payload


def openai_to_responses_response(openai_resp: dict, model: str) -> dict:
    choice = ((openai_resp or {}).get("choices") or [{}])[0] or {}
    message = choice.get("message") or {}
    response_id = (openai_resp or {}).get("id") or f"resp_{uuid.uuid4().hex[:24]}"
    output = []
    content = message.get("content") or ""
    if content or not message.get("tool_calls"):
        output.append({
            "type": "message",
            "id": f"msg_{uuid.uuid4().hex[:24]}",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": content, "annotations": []}],
        })
    for tool_call in message.get("tool_calls") or []:
        function = tool_call.get("function") or {}
        if not isinstance(function, dict):
            function = {}
        call_id = tool_call.get("id") or f"call_{uuid.uuid4().hex[:24]}"
        output.append({
            "type": "function_call",
            "id": tool_call.get("id") or f"fc_{call_id[5:] if str(call_id).startswith('call_') else call_id}",
            "call_id": call_id,
            "name": sanitize_anthropic_tool_name(function.get("name")),
            "arguments": function.get("arguments") or "{}",
            "status": "completed",
        })
    return {
        "id": response_id,
        "object": "response",
        "created_at": (openai_resp or {}).get("created") or int(time.time()),
        "status": "completed",
        "model": model,
        "output": output,
        "output_text": content,
        "usage": openai_usage_to_responses_usage((openai_resp or {}).get("usage") or {}),
    }


def responses_to_openai_response(resp: dict, model: str) -> dict:
    content_parts = []
    reasoning_parts = []
    tool_calls = []
    for item in (resp or {}).get("output") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            content_parts.append(_responses_text_from_content(item.get("content")))
        elif item_type == "function_call":
            tool_calls.append({
                "id": item.get("call_id") or item.get("id") or f"call_{uuid.uuid4().hex[:24]}",
                "type": "function",
                "function": {"name": sanitize_anthropic_tool_name(item.get("name")), "arguments": item.get("arguments") or "{}"},
            })
        elif item_type == "reasoning":
            reasoning_parts.append(_responses_text_from_content(item.get("content") or item.get("summary")))
    if not content_parts and (resp or {}).get("output_text"):
        content_parts.append(str(resp.get("output_text") or ""))
    message = {"role": "assistant", "content": "".join(content_parts)}
    if reasoning_parts:
        message["reasoning_content"] = "\n".join(reasoning_parts)
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {
        "id": (resp or {}).get("id") or generate_completion_id(),
        "object": "chat.completion",
        "created": (resp or {}).get("created_at") or int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if tool_calls else "stop"}],
        "usage": responses_usage_to_openai_usage((resp or {}).get("usage") or {}),
    }


def make_responses_sse_event(event_type: str, data: dict) -> str:
    return f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def convert_stream_to_responses(openai_stream, model: str) -> AsyncGenerator[str, None]:
    message_id = f"msg_{uuid.uuid4().hex[:24]}"
    created_at = int(time.time())
    # response_id 取上游首帧 id（跨协议直通链路是上游真实 id，dict 重建链路是统一 completion_id），
    # 上游没给才回退 resp_<uuid>。为此 response.created/in_progress 惰性发出——先读到首帧拿 id 再发。
    response_id: str | None = None
    response_started = False

    def _response_created_events() -> list[str]:
        nonlocal response_started, response_id
        if response_started:
            return []
        if not response_id:
            response_id = f"resp_{uuid.uuid4().hex[:24]}"
        response_started = True
        base = {
            "id": response_id, "object": "response",
            "created_at": created_at, "status": "in_progress", "model": model, "output": [],
        }
        return [
            make_responses_sse_event("response.created", {"type": "response.created", "response": {**base, "status": "in_progress"}}),
            make_responses_sse_event("response.in_progress", {"type": "response.in_progress", "response": base}),
        ]

    item_started = False
    part_started = False
    output_text = ""
    usage: dict = {}
    finish_reason = None
    buffered_tool_calls: list = []

    async for chunk_str in openai_stream:
        if isinstance(chunk_str, dict):
            continue
        if not isinstance(chunk_str, str):
            continue
        for line in chunk_str.splitlines():
            if not line.startswith("data:"):
                continue
            line_data = line[5:].strip()
            if not line_data or line_data == "[DONE]":
                continue
            try:
                chunk = json.loads(line_data)
            except json.JSONDecodeError:
                continue
            if not isinstance(chunk, dict):
                continue
            if not response_id and chunk.get("id"):
                response_id = chunk["id"]
            if chunk.get("usage"):
                usage = chunk.get("usage") or usage
            for choice in chunk.get("choices") or []:
                delta = choice.get("delta") or {}
                if choice.get("finish_reason"):
                    finish_reason = choice.get("finish_reason")
                if delta.get("tool_calls"):
                    buffered_tool_calls.extend(delta["tool_calls"])
                text_delta = delta.get("content") or ""
                if text_delta:
                    for ev in _response_created_events():
                        yield ev
                    if not item_started:
                        item_started = True
                        yield make_responses_sse_event("response.output_item.added", {
                            "type": "response.output_item.added", "output_index": 0,
                            "item": {"type": "message", "id": message_id, "status": "in_progress", "role": "assistant", "content": []},
                        })
                    if not part_started:
                        part_started = True
                        yield make_responses_sse_event("response.content_part.added", {
                            "type": "response.content_part.added", "item_id": message_id, "output_index": 0, "content_index": 0,
                            "part": {"type": "output_text", "text": "", "annotations": []},
                        })
                    output_text += text_delta
                    yield make_responses_sse_event("response.output_text.delta", {
                        "type": "response.output_text.delta", "item_id": message_id, "output_index": 0, "content_index": 0, "delta": text_delta,
                    })

    merged_tc = _merge_tool_calls(buffered_tool_calls)

    # 无文本且无工具时补一个空 message item，保留原有行为；有工具且无文本时不发空 message。
    if not item_started and not merged_tc:
        for ev in _response_created_events():
            yield ev
        item_started = True
        part_started = True
        yield make_responses_sse_event("response.output_item.added", {
            "type": "response.output_item.added", "output_index": 0,
            "item": {"type": "message", "id": message_id, "status": "in_progress", "role": "assistant", "content": []},
        })
        yield make_responses_sse_event("response.content_part.added", {
            "type": "response.content_part.added", "item_id": message_id, "output_index": 0, "content_index": 0,
            "part": {"type": "output_text", "text": "", "annotations": []},
        })

    output_items: list = []
    # 收尾 message item（仅当有文本或空 message 占位）
    if item_started:
        yield make_responses_sse_event("response.output_text.done", {
            "type": "response.output_text.done", "item_id": message_id, "output_index": 0, "content_index": 0, "text": output_text,
        })
        yield make_responses_sse_event("response.content_part.done", {
            "type": "response.content_part.done", "item_id": message_id, "output_index": 0, "content_index": 0,
            "part": {"type": "output_text", "text": output_text, "annotations": []},
        })
        output_item = {
            "type": "message", "id": message_id, "status": "completed", "role": "assistant",
            "content": [{"type": "output_text", "text": output_text, "annotations": []}],
        }
        yield make_responses_sse_event("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": output_item})
        output_items.append(output_item)

    # 发送 function_call 项（之前流式跨协议完全丢失工具调用，这里补全，口径对齐非流式
    # openai_to_responses_response 的 output 结构：type/id/call_id/name/arguments/status）
    fc_index = 0
    for tc in merged_tc:
        func = tc.get("function") or {}
        if not isinstance(func, dict):
            func = {}
        raw_id = tc.get("id")
        call_id = raw_id or f"call_{uuid.uuid4().hex[:24]}"
        if raw_id:
            fc_item_id = raw_id
        else:
            fc_item_id = f"fc_{call_id[5:] if str(call_id).startswith('call_') else call_id}"
        fc_name = sanitize_anthropic_tool_name(func.get("name"))
        fc_args = func.get("arguments") or "{}"
        output_index = (1 if item_started else 0) + fc_index
        for ev in _response_created_events():
            yield ev
        yield make_responses_sse_event("response.output_item.added", {
            "type": "response.output_item.added", "output_index": output_index,
            "item": {"type": "function_call", "id": fc_item_id, "call_id": call_id, "name": fc_name, "arguments": "", "status": "in_progress"},
        })
        yield make_responses_sse_event("response.function_call_arguments.delta", {
            "type": "response.function_call_arguments.delta", "item_id": fc_item_id, "output_index": output_index, "delta": fc_args,
        })
        yield make_responses_sse_event("response.function_call_arguments.done", {
            "type": "response.function_call_arguments.done", "item_id": fc_item_id, "output_index": output_index, "arguments": fc_args,
        })
        fc_item = {
            "type": "function_call", "id": fc_item_id, "call_id": call_id, "name": fc_name,
            "arguments": fc_args, "status": "completed",
        }
        yield make_responses_sse_event("response.output_item.done", {
            "type": "response.output_item.done", "output_index": output_index, "item": fc_item,
        })
        output_items.append(fc_item)
        fc_index += 1

    for ev in _response_created_events():
        yield ev
    completed = {
        "id": response_id, "object": "response",
        "created_at": created_at, "status": "completed", "model": model,
        "output": output_items, "output_text": output_text,
        "usage": openai_usage_to_responses_usage(usage),
    }
    if finish_reason:
        completed["finish_reason"] = finish_reason
    yield make_responses_sse_event("response.completed", {"type": "response.completed", "response": completed})
    yield "data: [DONE]\n\n"


def _extract_sub_frames(delta: dict) -> list[tuple[str, str | list]]:
    """从一帧 OpenAI delta 提取有序单类型子帧。

    归一层已把任意上游协议统一成 OpenAI delta 形态（见 stream_protocol_sniff 输出约定与
    parse_sse_data 的多字段口径），这里只做「当前帧实际有哪几类输出、按什么顺序」的提取：
    一帧同时带 reasoning_content + content + tool_calls 时拆成多子帧，调用方据此做
    通用的 prev/curr 类型比较与闭环，不再每分支各自写死判断。
    提取不到返回空列表，调用方不管。
    """
    frames: list[tuple[str, str | list]] = []
    thinking = delta.get("reasoning_content") or delta.get("reasoning") or delta.get("thinking")
    if thinking:
        frames.append(("thinking", thinking))
    content = delta.get("content")
    if content:
        frames.append(("text", content))
    tool_calls = delta.get("tool_calls")
    if tool_calls:
        frames.append(("tool_use", tool_calls))
    return frames


def normalize_tool_call_arguments(tool_calls: list) -> list:
    """把 tool_calls 里 dict 形态的 ``function.arguments`` 序列化成 JSON 字符串。

    OpenAI 协议要求 ``arguments`` 是字符串；部分上游（eaichat 等）直接给 dict。
    流式出口若原样下发 dict，客户端拿到的是协议违规的 delta（agent SDK 解析会炸）。
    非流式聚合侧由 ``_merge_tool_calls`` 归一，流式侧用本函数，两条路径同口径，
    渠道实现不必各自再写一份 ``_normalize_tool_calls``。

    就地改写传入的 dict（与上游帧同一对象），并返回同一列表，便于链式使用。
    """
    for tc in tool_calls or []:
        if not isinstance(tc, dict):
            continue
        func = tc.get("function")
        if isinstance(func, dict) and isinstance(func.get("arguments"), dict):
            func["arguments"] = json.dumps(func["arguments"], ensure_ascii=False)
    return tool_calls


def _merge_tool_calls(partial_tool_calls: list) -> list:
    """合并流式中的部分 tool calls"""
    merged = {}
    for tc in partial_tool_calls:
        if not isinstance(tc, dict):
            continue
        idx = tc.get("index", len(merged))
        if idx not in merged:
            merged[idx] = {"id": "", "type": "function", "function": {"name": "", "arguments": ""}}
        if tc.get("id"):
            merged[idx]["id"] = tc["id"]
        func = tc.get("function") or {}
        if not isinstance(func, dict):
            func = {}
        if func.get("name"):
            # name 与 id 一样只覆盖不累加：部分上游每个 arguments 分片都重复带 name，
            # 累加会把 "Grep" 拼成 "GrepGrepGrep..."。只有 arguments 是分片增量，才用 +=。
            merged[idx]["function"]["name"] = func["name"]
        if func.get("arguments"):
            if isinstance(func["arguments"], dict):
                # 上游一次性给全 dict 形态 arguments（eaichat 等）：序列化成 OpenAI 协议
                # 要求的 JSON 字符串并整体替换。dict 不是流式分片（分片按约定是字符串），
                # 不能也不需要与已累积内容拼接；不归一会在聚合层直接 TypeError。
                merged[idx]["function"]["arguments"] = json.dumps(func["arguments"], ensure_ascii=False)
            else:
                merged[idx]["function"]["arguments"] += func["arguments"]

    result = list(merged.values())
    for tc in result:
        tc["id"] = sanitize_anthropic_tool_id(tc.get("id"))
        fn = tc.setdefault("function", {})
        if not isinstance(fn, dict):
            tc["function"] = fn = {}
        fn["name"] = sanitize_anthropic_tool_name(fn.get("name"))
        fn["arguments"] = fn.get("arguments") or "{}"
    return result


async def convert_stream_to_anthropic(
    openai_stream,
    model: str,
    request_messages: list[dict] | None = None,
) -> AsyncGenerator[str, None]:
    """将 OpenAI 流式响应转换为 Anthropic SSE 事件流

    request_messages（OpenAI 格式）仅用于上游不返回 usage 时兜底估算 input_tokens；
    若上游返回了 usage，则优先用上游值。
    """
    # message_id 取上游首帧的 id（跨协议直通时是上游真实 chatcmpl-*，dict 重建链路是统一
    # completion_id），保证客户端看到的 id 与上游/日志一致；上游没给才回退生成。
    # 为此 message_start 必须惰性发出——先读到首帧拿 id，再发；Anthropic 要求它恒为首事件，
    # 所以任何 content_block / message_delta 之前都要先 _ensure_message_start()。
    message_id: str | None = None
    message_started = False

    block_index = 0
    thinking_block_open = False
    text_block_open = False
    buffered_tool_calls = []
    finish_reason = None
    total_input_tokens = 0
    total_output_tokens = 0
    output_text = ""
    reasoning_text = ""
    # 当前正在写的 content block 类型：thinking / text / None。
    # 类型切换时（如 reasoning→content、reasoning→tool_calls、content→tool_calls）
    # 必须先关闭当前 block 再开新块，避免出现跨类型嵌套或忘记闭环。
    # 保留“后一个开始前关闭前一个”的流式设计，只把触发点从“仅 content 关 thinking”
    # 补全为“任意类型切换都关上一个”，流结束仅做兜底。
    current_block_type: str | None = None

    # 上游通常不在 SSE 里返回 input_tokens；用请求 messages 估算兜底，
    # 避免 Claude Code 因 message_start.usage.input_tokens=0 报上下文/计费错误。
    estimated_input_tokens = 0
    if request_messages:
        prompt_text = ""
        for msg in request_messages:
            if isinstance(msg, dict):
                prompt_text += "\n" + json.dumps(msg.get("content", ""), ensure_ascii=False)
                if msg.get("tool_calls"):
                    prompt_text += "\n" + json.dumps(msg["tool_calls"], ensure_ascii=False)
        estimated_input_tokens = estimate_tokens_from_text(prompt_text)
        total_input_tokens = estimated_input_tokens

    def _message_start_events() -> list[str]:
        """生成 message_start + ping；已发过则返回空列表。"""
        nonlocal message_started
        if message_started:
            return []
        message_started = True
        return [
            make_anthropic_sse_event("message_start", {
                "type": "message_start",
                "message": {
                    "id": message_id or generate_anthropic_message_id(),
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": model,
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": total_input_tokens, "output_tokens": 0}
                }
            }),
            make_anthropic_sse_event("ping", {"type": "ping"}),
        ]

    def _close_open_block() -> list[str]:
        """关闭当前正在写的 content block（类型切换 / 流结束兜底时调用）。

        thinking 块关闭前必须发 signature_delta（合成签名），否则带 interleaved-thinking
        的客户端不认这个块、后续 tool_use 会被降级成文字。text 块直接 stop。
        关闭后递增 block_index 并清状态，调用方负责把 current_block_type 置 None。
        """
        nonlocal block_index, thinking_block_open, text_block_open
        events: list[str] = []
        if thinking_block_open:
            thinking_block_open = False
            events.append(make_anthropic_sse_event("content_block_delta", {
                "type": "content_block_delta",
                "index": block_index,
                "delta": {"type": "signature_delta", "signature": PROXY_SYNTHETIC_THINKING_SIGNATURE}
            }))
            events.append(make_anthropic_sse_event("content_block_stop", {
                "type": "content_block_stop",
                "index": block_index
            }))
            block_index += 1
        elif text_block_open:
            text_block_open = False
            events.append(make_anthropic_sse_event("content_block_stop", {
                "type": "content_block_stop",
                "index": block_index
            }))
            block_index += 1
        return events

    async for chunk_str in openai_stream:
        if isinstance(chunk_str, dict) and "_retry_path" in chunk_str:
            continue
        if not chunk_str or not chunk_str.strip():
            continue

        # logger.debug(f"Anthropic stream received chunk: {chunk_str[:200]}")

        for line in chunk_str.strip().split('\n'):
            if not line.startswith("data:"):
                continue
            line_data = line[5:].strip()
            if line_data == "[DONE]" or not line_data:
                continue

            try:
                chunk_data = json.loads(line_data)
            except json.JSONDecodeError:
                continue
            if not isinstance(chunk_data, dict):
                # data: null / 数组等非对象帧（json.loads 成功但不是 dict），跳过，
                # 避免后续 .get("choices") 对 None 抛 AttributeError 中断整条流。
                continue

            choices = chunk_data.get("choices", [])

            # id 与 usage 的收集必须在 `if not choices: continue` 之前：
            # 累计型上游常在流末尾发一帧 {"choices": [], "usage": {...真实 token...}}，
            # 若在 skip 之后收集，这帧的真实 usage 永远收不到，message_delta 退回文本估算。
            if not message_id and chunk_data.get("id"):
                message_id = chunk_data["id"]

            usage = chunk_data.get("usage")
            if not isinstance(usage, dict):
                for usage_choice in choices or []:
                    if isinstance(usage_choice, dict) and isinstance(usage_choice.get("usage"), dict):
                        usage = usage_choice["usage"]
                        break
            if usage:
                usage = normalize_usage(usage)
                total_input_tokens = max(total_input_tokens, usage.get("prompt_tokens", 0) or 0)
                total_output_tokens = max(total_output_tokens, usage.get("completion_tokens", 0) or 0)

            if not choices:
                continue

            delta = choices[0].get("delta")
            if not isinstance(delta, dict):
                # 部分上游在结束帧会回 "delta": [] 之类的非法形态，兜底成空 dict，
                # 只保留 finish_reason 信息，避免后续 .get 触发 'list' object has no attribute 'get'。
                delta = {}
            fr = choices[0].get("finish_reason")

            if fr:
                finish_reason = fr

            # 按当前帧实际输出提取有序子帧；同帧多输出（reasoning+content+tool_calls）
            # 拆成多子帧逐个走通用的 prev/curr 比较，不再每分支写死类型字符串判断。
            for curr_type, value in _extract_sub_frames(delta):
                for start_event in _message_start_events():
                    yield start_event
                if curr_type != current_block_type:
                    for close_event in _close_open_block():
                        yield close_event
                    current_block_type = curr_type
                if curr_type == "thinking":
                    reasoning_text += value
                    if not thinking_block_open:
                        thinking_block_open = True
                        yield make_anthropic_sse_event("content_block_start", {
                            "type": "content_block_start",
                            "index": block_index,
                            "content_block": {"type": "thinking", "thinking": "", "signature": PROXY_SYNTHETIC_THINKING_SIGNATURE}
                        })
                    yield make_anthropic_sse_event("content_block_delta", {
                        "type": "content_block_delta",
                        "index": block_index,
                        "delta": {"type": "thinking_delta", "thinking": value}
                    })
                elif curr_type == "text":
                    output_text += value
                    if not text_block_open:
                        text_block_open = True
                        yield make_anthropic_sse_event("content_block_start", {
                            "type": "content_block_start",
                            "index": block_index,
                            "content_block": {"type": "text", "text": ""}
                        })
                    yield make_anthropic_sse_event("content_block_delta", {
                        "type": "content_block_delta",
                        "index": block_index,
                        "delta": {"type": "text_delta", "text": value}
                    })
                elif curr_type == "tool_use":
                    # tool_use 仍缓冲到流尾合并发送；类型切换处已先闭合 thinking/text。
                    buffered_tool_calls.extend(value)

            # finish_reason 是本轮 done 信号：本帧 delta 已处理完，若有 content block
            # 仍开着，在这里立刻闭环，不拖到流末尾兜底。同帧带最后一片 content/tool_calls
            # 的上游也能先发完该片再关。
            if fr and current_block_type is not None:
                for close_event in _close_open_block():
                    yield close_event
                current_block_type = None

    # 循环结束：无论有无内容都要保证 message_start 已发（tool-only / 空流 / 纯 usage 场景）。
    for start_event in _message_start_events():
        yield start_event

    # 关闭未关闭的 block（流尾兜底：正常路径已在类型切换处关闭，这里只兜未闭合的最后一个）
    for close_event in _close_open_block():
        yield close_event

    # 发送 tool_use blocks
    merged_tc = _merge_tool_calls(buffered_tool_calls)
    logger.info(f"[convert_stream_to_anthropic] buffered_tool_calls={len(buffered_tool_calls)}, "
                f"merged_tool_calls={len(merged_tc)}, finish_reason={finish_reason}")

    for tc in merged_tc:
        func = tc.get("function") or {}
        if not isinstance(func, dict):
            func = {}
        tc_id = sanitize_anthropic_tool_id(tc.get("id"))
        tc_name = sanitize_anthropic_tool_name(func.get("name"))
        tc_input = func.get("arguments") or "{}"

        yield make_anthropic_sse_event("content_block_start", {
            "type": "content_block_start",
            "index": block_index,
            "content_block": {"type": "tool_use", "id": tc_id, "name": tc_name, "input": {}}
        })
        yield make_anthropic_sse_event("content_block_delta", {
            "type": "content_block_delta",
            "index": block_index,
            "delta": {"type": "input_json_delta", "partial_json": tc_input}
        })
        yield make_anthropic_sse_event("content_block_stop", {
            "type": "content_block_stop",
            "index": block_index
        })
        block_index += 1

    # 协议允许空 content_block_start；但不要再补空 text_delta，避免客户端把它当成空首字/首 token。
    if block_index == 0:
        yield make_anthropic_sse_event("content_block_start", {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        })
        yield make_anthropic_sse_event("content_block_stop", {
            "type": "content_block_stop",
            "index": 0
        })
        block_index += 1

    # message_delta + message_stop
    stop_reason_map = {"stop": "end_turn", "tool_calls": "tool_use", "length": "max_tokens"}
    stop_reason = stop_reason_map.get(finish_reason, "end_turn")

    logger.debug(f"convert_stream_to_anthropic - finish_reason={finish_reason}, stop_reason={stop_reason}, block_count={block_index}")

    # 与 usage_utils.estimate_usage 口径保持一致：fallback 估算时把 reasoning、
    # 文本内容、工具调用参数 JSON 一并计入，避免纯工具调用/纯思考场景 fallback=0 低估。
    fallback_text = (reasoning_text or "") + (output_text or "")
    if merged_tc:
        fallback_text += "\n" + json.dumps(merged_tc, ensure_ascii=False)

    yield make_anthropic_sse_event("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": stop_reason, "stop_sequence": None},
        "usage": {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens if total_output_tokens > 0 else estimate_tokens_from_text(fallback_text),
        }
    })
    yield make_anthropic_sse_event("message_stop", {"type": "message_stop"})