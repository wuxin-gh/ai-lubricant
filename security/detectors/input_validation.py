"""输入校验检测器

校验请求结构和大小：空消息、消息体积过大、消息数过多、无效 role。
阈值从 config.security_input_limits() 读取，支持热更新。
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from security.types import RiskTag, ScanRequest

# 合法的 OpenAI 消息 role
_VALID_ROLES = frozenset({"system", "user", "assistant", "tool", "function"})


def _get_limits() -> dict:
    """读取输入校验阈值"""
    try:
        from config import Config
        return Config.security_input_limits()
    except Exception:
        return {"max_message_size_bytes": 512000, "max_messages": 500}


def _estimate_message_size(msg: dict) -> int:
    """估算单条消息的字节大小"""
    content = msg.get("content")
    if isinstance(content, str):
        return len(content.encode("utf-8"))
    if isinstance(content, list):
        total = 0
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str):
                    total += len(text.encode("utf-8"))
        return total
    return 0


def detect(req: ScanRequest) -> list[RiskTag]:
    """校验请求输入"""
    from security.types import RiskTag

    tags: list[RiskTag] = []
    limits = _get_limits()
    max_size = limits.get("max_message_size_bytes", 512000)
    max_messages = limits.get("max_messages", 500)

    # 空消息检查
    if not req.messages:
        tags.append(RiskTag(
            tag="input:empty-messages",
            severity="high",
            detail={"reason": "messages 为空"},
        ))
        return tags  # 空消息无需继续检查

    # 消息数过多
    msg_count = len(req.messages)
    if msg_count > max_messages:
        tags.append(RiskTag(
            tag="input:too-many-messages",
            severity="warn",
            detail={"count": msg_count, "limit": max_messages},
        ))

    # 逐条消息检查
    for i, msg in enumerate(req.messages):
        # role 合法性
        role = msg.get("role", "")
        if role and role not in _VALID_ROLES:
            tags.append(RiskTag(
                tag="input:invalid-role",
                severity="high",
                detail={"index": i, "role": role},
            ))

        # 单条消息大小
        size = _estimate_message_size(msg)
        if size > max_size:
            tags.append(RiskTag(
                tag="input:message-too-large",
                severity="warn",
                detail={"index": i, "size_bytes": size, "limit_bytes": max_size},
            ))

    return tags
