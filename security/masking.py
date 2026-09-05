"""凭据遮蔽/还原

参考 agentfw 的 core/masking.ts：
- 遍历请求体 messages 中的文本内容，将识别到的凭据替换为固定假值
- 维护 per-request fake→real 映射
- 在响应中将假值还原为真实值

对 OpenAI 格式请求，处理 messages[].content（字符串或 content parts 数组）。
"""

from __future__ import annotations

import copy
import logging
import re
from typing import Any

from security.sensitive_rules import get_masking_rules
from security.types import MaskingRule, MaskResult

logger = logging.getLogger("security.masking")


# 凭据类 header：与 Authorization 同样保留首尾几位，排查时才能认出用的是哪把 key。
_CREDENTIAL_HEADER_PARTS = ("token", "api-key", "apikey", "secret", "credential", "password")

# 整体遮蔽的 header：cookie 常含多条凭据串在一起，保留片段收益低、泄露面大。
_OPAQUE_HEADER_PARTS = ("cookie",)

_SENSITIVE_HEADER_PARTS = ("authorization",) + _OPAQUE_HEADER_PARTS + _CREDENTIAL_HEADER_PARTS


def mask_secret_value(value: Any) -> str:
    """部分隐藏敏感值，保留首尾各 4 位，中间用 ... 占位。

    过短的值仍全部以星号遮蔽，避免暴露完整凭据。
    """
    text = str(value or "")
    if not text:
        return "***"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def mask_authorization_header(value: Any) -> str:
    """保留 Authorization 认证方案（Bearer/Basic 等），仅部分隐藏凭据部分。

    例如 ``Bearer sk-II2KB0...Hemfn`` —— 方案 + token 首尾用于排查用的是哪把 key，
    中间凭据不暴露。
    """
    text = str(value or "")
    if not text:
        return "***"
    scheme, separator, credential = text.partition(" ")
    if separator and credential:
        return f"{scheme} {mask_secret_value(credential)}"
    return mask_secret_value(text)


def is_sensitive_header(key: Any) -> bool:
    """该 header 名是否属于需要脱敏的凭据/敏感头。"""
    lower_key = str(key).lower()
    return any(part in lower_key for part in _SENSITIVE_HEADER_PARTS)


def sanitize_header_value(key: Any, value: Any) -> str:
    """按 header 名脱敏：凭据头保留首尾用于排查，cookie 等整体 ***。"""
    lower_key = str(key).lower()
    if "authorization" in lower_key:
        return mask_authorization_header(value)
    if any(part in lower_key for part in _OPAQUE_HEADER_PARTS):
        return "***"
    if any(part in lower_key for part in _CREDENTIAL_HEADER_PARTS):
        return mask_secret_value(value)
    return str(value)


def _make_unique_fake(base_fake: str, used: dict[str, str]) -> str:
    """如果 base_fake 已被占用，添加 _N 后缀保证唯一"""
    if base_fake not in used:
        return base_fake
    counter = 2
    while f"{base_fake}_{counter}" in used:
        counter += 1
    return f"{base_fake}_{counter}"


def _mask_text(text: str, rules: list[MaskingRule], restore_map: dict[str, str]) -> str:
    """对单段文本执行遮蔽，返回遮蔽后文本，同时更新 restore_map"""
    result = text
    for rule in rules:
        def _replace(match: re.Match[str], _rule: MaskingRule = rule) -> str:
            if _rule.group is not None:
                # 只遮蔽指定捕获组，保留上下文（如 "Bearer " 前缀）
                real = match.group(_rule.group)
                if not real:
                    return match.group(0)
                # 检查是否已为这个 real 值分配过 fake
                for f, r in restore_map.items():
                    if r == real:
                        return match.string[match.start():match.start(_rule.group)] + f + match.string[match.end(_rule.group):match.end()]
                fake = _make_unique_fake(_rule.fake, restore_map)
                restore_map[fake] = real
                return match.string[match.start():match.start(_rule.group)] + fake + match.string[match.end(_rule.group):match.end()]
            else:
                real = match.group(0)
                for f, r in restore_map.items():
                    if r == real:
                        return f
                fake = _make_unique_fake(_rule.fake, restore_map)
                restore_map[fake] = real
                return fake

        result = rule.pattern.sub(_replace, result)
    return result


def _mask_content(content: Any, rules: list[MaskingRule], restore_map: dict[str, str]) -> Any:
    """遮蔽 content 字段（可能是字符串或 content parts 数组）"""
    if isinstance(content, str):
        return _mask_text(content, rules, restore_map)
    if isinstance(content, list):
        masked_parts = []
        for part in content:
            if isinstance(part, dict):
                masked_part = dict(part)
                text = masked_part.get("text")
                if isinstance(text, str):
                    masked_part["text"] = _mask_text(text, rules, restore_map)
                masked_parts.append(masked_part)
            else:
                masked_parts.append(part)
        return masked_parts
    return content


def mask_credentials(body: dict) -> MaskResult:
    """遍历请求体 messages，将识别到的凭据替换为固定假值

    返回:
        MaskResult: 包含遮蔽后的 body 深拷贝和 fake→real 映射
    """
    restore_map: dict[str, str] = {}
    masked_body = copy.deepcopy(body)
    rules = get_masking_rules()

    messages = masked_body.get("messages")
    if not isinstance(messages, list):
        return MaskResult(masked_body=masked_body, restore_map=restore_map)

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        content = msg.get("content")
        if content is not None:
            msg["content"] = _mask_content(content, rules, restore_map)

    return MaskResult(masked_body=masked_body, restore_map=restore_map)


def restore_credentials(text: str, restore_map: dict[str, str]) -> str:
    """在响应文本中将假值还原为真实值"""
    if not restore_map:
        return text
    result = text
    for fake, real in restore_map.items():
        result = result.replace(fake, real)
    return result


def restore_response_body(body: dict, restore_map: dict[str, str]) -> dict:
    """还原响应体中的凭据（深拷贝后还原）"""
    if not restore_map:
        return body
    restored = copy.deepcopy(body)

    # 还原 choices[].message.content 和 choices[].delta.content
    choices = restored.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            for key in ("message", "delta"):
                msg = choice.get(key)
                if not isinstance(msg, dict):
                    continue
                content = msg.get("content")
                if isinstance(content, str):
                    msg["content"] = restore_credentials(content, restore_map)
                elif isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict):
                            text = part.get("text")
                            if isinstance(text, str):
                                part["text"] = restore_credentials(text, restore_map)

    return restored
