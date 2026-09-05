"""凭据泄露检测器

扫描请求消息中的凭据模式（API Key、Bearer Token 等）。
规则来自可配置的敏感数据规则（security.sensitive_rules）。

检测器签名: (ScanRequest) -> list[RiskTag]
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from security.types import RiskTag, ScanRequest


def _collect_message_text(messages: list[dict]) -> str:
    """提取消息列表中的所有文本内容"""
    parts: list[str] = []
    for msg in messages:
        content = msg.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    text = part.get("text")
                    if isinstance(text, str):
                        parts.append(text)
    return "\n".join(parts)


def detect(req: ScanRequest) -> list[RiskTag]:
    """检测请求中的凭据泄露"""
    from security.sensitive_rules import get_detection_rules
    from security.types import RiskTag

    # 合并所有文本：消息内容 + 额外文本（system prompt 等）
    text = _collect_message_text(req.messages)
    if req.extra_text:
        text = text + "\n" + req.extra_text

    if not text:
        return []

    tags: list[RiskTag] = []
    seen: set[str] = set()

    for rule in get_detection_rules():
        match = rule.pattern.search(text)
        if match and rule.id not in seen:
            seen.add(rule.id)
            # 只记录前 6 字符，不泄露完整密钥
            tags.append(RiskTag(
                tag=f"secret:{rule.id}",
                severity=rule.severity,
                detail={"prefix": match.group(0)[:6]},
            ))

    return tags
