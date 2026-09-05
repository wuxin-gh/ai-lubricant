"""提示注入检测器

扫描用户/工具消息中的注入尝试（指令覆盖、角色注入、数据外泄指令等）。
参考 agentfw 的 risk/prompt-injection.ts。

仅扫描 role == "user" 和 role == "tool" 的消息，跳过 assistant 消息。
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from security.types import RiskTag, ScanRequest

# ── 数据外泄正则的组合块 ─────────────────────────────────
# 外泄目标：URL / 邮箱 / 明确的外部关键词 / 裸域名。
# 要求动作后必须跟一个明确的外部目标，避免命中 "send secret to the server"
# 这类可能是正常后端调用的句子。
_EXFIL_TARGET = (
    r"(?:"
    r"https?://\S+"                                                  # 完整 URL
    r"|[\w.+-]+@[\w.-]+\.\w{2,}"                                     # 邮箱
    r"|(?:an?\s+)?(?:external|remote|attacker(?:'s)?|evil|third[\s-]*party|my\s+own)\b"  # 外部关键词(英)
    r"|外部|远程|攻击者|第三方|我的服务器|我的邮箱"                    # 外部关键词(中)
    r"|[\w-]+\.[a-z]{2,}(?:/\S*)?"                                   # 裸域名 evil.com[/path]
    r")"
)
# 凭据类关键词（中英）
_CRED_KW = r"(?:api[\s_-]*keys?|secret\s*keys?|access\s*tokens?|secrets?|credentials?|passwords?|密钥|访问令牌|access\s*token|凭据|凭证|密码|token)"
# 外泄动作（中）
_EXFIL_ACT_CN = r"(?:发送|发给|上传|外传|泄露|回传|上报|提交|传输|传送)"

# 英文：send/upload/... <凭据> to/into/via <外部目标>
_EXFIL_EN = (
    r"(?:send|upload|post|leak|exfiltrate|forward|transmit)\s+"
    r"(?:the\s+|your\s+|my\s+|all\s+|these\s+|our\s+)*"
    + _CRED_KW +
    r"s?\s+(?:to|into|via)\s+" + _EXFIL_TARGET
)
# 中文：兼容"发送密钥到 X"（动作在前）与"把密钥发送到 X"（关键词在前）
_EXFIL_CN = (
    r"(?:"
    + _EXFIL_ACT_CN + r"[^。\n]{0,8}" + _CRED_KW            # 动作在前
    + r"|"
    + r"(?:把|将)?[^。\n]{0,8}" + _CRED_KW + r"[^。\n]{0,8}" + _EXFIL_ACT_CN  # 关键词在前
    + r")"
    + r"[^。\n]{0,4}(?:到|给|至)\s*" + _EXFIL_TARGET
)

# 注入模式定义
_PATTERNS: list[tuple[str, re.Pattern[str], str]] = [
    # 指令覆盖（英文 + 中文）
    (
        "injection:instruction-override",
        re.compile(
            r"ignore\s+(?:all\s+)?previous\s+instructions"
            r"|disregard\s+(?:all\s+)?(?:prior|previous)\s+instructions"
            r"|forget\s+(?:all\s+)?(?:prior|previous)\s+instructions"
            r"|override\s+(?:system|previous)\s+(?:prompt|instructions)"
            r"|you\s+are\s+now\s+"
            r"|new\s+instructions?\s*:"
            r"|忽略(?:之前|上面|以上)的(?:所有)?(?:指令|指示|规则)"
            r"|无视(?:之前|上面|以上)的(?:所有)?(?:指令|指示|规则)"
            r"|忘记(?:之前|上面|以上)的(?:所有)?(?:指令|指示)"
            r"|新指令\s*[:：]",
            re.IGNORECASE,
        ),
        "high",
    ),
    # 角色注入
    # 只保留 chat 模板专用控制标记（这些几乎不会出现在正常对话里）。
    # 移除 "Human:" / "Assistant:" / "# system"：贴对话日志、问 prompt 写法、
    # 写普通注释都会命中这些，误判率过高。
    (
        "injection:role-override",
        re.compile(
            r"<\|?im_start\|?>"
            r"|<\|?im_end\|?>"
            r"|<\|(?:system|user|assistant|end)\|>"
            r"|<\|eot_id\|>"
            r"|<\|start_header_id\|>",
            re.IGNORECASE | re.MULTILINE,
        ),
        "high",
    ),
    # 数据外泄指令
    # 只匹配"把凭据发/传到某个外部目标"的祈使句，避免命中普通技术讨论。
    # 外泄动作后必须跟明确的外部目标（URL / 邮箱 / 外部关键词 / 裸域名），
    # 单独出现 "exfiltrate" 或裸 "key" 不再触发。
    (
        "injection:exfiltration",
        re.compile(_EXFIL_EN + r"|" + _EXFIL_CN, re.IGNORECASE),
        "high",
    ),
    # 隐藏字符（零宽字符、BIDI 控制字符）
    (
        "injection:hidden-chars",
        re.compile(
            r"[​‌‍‎‏‪‫‬‭‮⁠﻿]"
        ),
        "warn",
    ),
]


def _get_content_text(content) -> str:
    """提取消息 content 中的文本"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return ""


def detect(req: ScanRequest) -> list[RiskTag]:
    """检测请求中的提示注入"""
    from security.types import RiskTag

    tags: list[RiskTag] = []
    seen: set[str] = set()

    # 只扫描 user 和 tool 消息
    for msg in req.messages:
        role = msg.get("role", "")
        if role not in ("user", "tool"):
            continue

        text = _get_content_text(msg.get("content"))
        if not text:
            continue

        for tag_name, pattern, severity in _PATTERNS:
            match = pattern.search(text)
            if match and tag_name not in seen:
                seen.add(tag_name)
                tags.append(RiskTag(
                    tag=tag_name,
                    severity=severity,
                    detail={"role": role, "match_preview": match.group(0)[:50]},
                ))

    return tags
