"""内置凭据遮蔽规则

参考 agentfw 的 core/masking.ts。
fake 值用 base64 存储，加载时 decode —— 避免源码中出现真实密钥格式触发扫描。
规则顺序重要：更具体的模式在前，避免被通用模式吞掉。
"""

from __future__ import annotations

import re
from base64 import b64decode

from security.types import MaskingRule


def _dec(b64: str) -> str:
    """base64 解码"""
    return b64decode(b64).decode("utf-8")


# 内置遮蔽规则 —— 顺序重要
BUILTIN_RULES: list[MaskingRule] = [
    # 1. Anthropic API Key (sk-ant-...)
    MaskingRule(
        id="anthropic-key",
        label="Anthropic API Key",
        pattern=re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}"),
        fake=_dec("c2stYW50LWFwaTAzLXg3S2Q5TG0yUXA4UnY1VHgxWmI2TmM0SGcwSnNfYUJjRGVGZ0hpSmtMbU5vUHFSc1R1VndYeVoxMjM0NTY3ODkwYWJjZEVmR2hJaktsTW5PcFFyQUE="),
    ),
    # 2. OpenAI API Key (sk-... / sk-proj-...)，排除 sk-ant- 和 sk_live_
    MaskingRule(
        id="openai-key",
        label="OpenAI API Key",
        pattern=re.compile(r"\bsk-(?!ant-)(?:proj-)?[A-Za-z0-9_-]{20,}\b"),
        fake=_dec("c2stcHJvai1UM0JsYmtGSmFIMmtQcTdSajR0WGNWOG5CMW1Zd0U2c1owZExmRzV1SW9BM2VLN0R3U2hRMk52R3hSbQ=="),
    ),
    # 3. Stripe Secret Key (sk_live_... / rk_live_...)
    MaskingRule(
        id="stripe-key",
        label="Stripe Secret Key",
        pattern=re.compile(r"\b[sr]k_live_[A-Za-z0-9]{20,}\b"),
        fake=_dec("c2tfbGl2ZV81MU1aOHhRMmVadktZbG8yQzlhQmNEZUZnSGlKa0xtTm9QcVJzVHVWd1h5WjAxMjM0NTY3ODlhYmNk"),
    ),
    # 4. GitHub PAT (ghp_ / gho_ / ghs_ / github_pat_)
    MaskingRule(
        id="github-pat",
        label="GitHub Token",
        pattern=re.compile(r"\b(?:ghp|gho|ghs|ghr|github_pat)_[A-Za-z0-9_]{20,}\b"),
        fake=_dec("Z2hwX0ExYjJDM2Q0RTVmNkc3aDhJOWowSzFMMm0zTjRvNVA2cTdSOA=="),
    ),
    # 5. AWS Access Key ID (AKIA...)
    MaskingRule(
        id="aws-akid",
        label="AWS Access Key ID",
        pattern=re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
        fake=_dec("QUtJQVhFRU5OTERPQUZTT0lBSUtB"),
    ),
    # 6. AWS Secret Access Key
    MaskingRule(
        id="aws-secret",
        label="AWS Secret Access Key",
        pattern=re.compile(r"(?:aws_secret_access_key|AWS_SECRET_ACCESS_KEY)\s*[=:]\s*[A-Za-z0-9/+=]{30,}"),
        fake="aws_secret_access_key=AAAAAABBBBBBCCCCCCDDDDDEEEEEFFFFFGGGGHHHH==",
    ),
    # 7. Google API Key (AIza...)
    MaskingRule(
        id="google-key",
        label="Google API Key",
        pattern=re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
        fake=_dec("QUl6YVN5RExPcmx0R09FaFpPSE5WcUdWYVJqTWtCakxqRnpPQT09"),
    ),
    # 8. Slack Token (oxb- / oxp- / ...)
    MaskingRule(
        id="slack-token",
        label="Slack Token",
        pattern=re.compile(r"\bxox[baprs]-[a-zA-Z0-9-]{10,}\b"),
        fake=_dec("eG94Yi0wMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVoxMjM0"),
    ),
    # 9. Ethereum Private Key (0x + 64 hex chars)
    MaskingRule(
        id="eth-private-key",
        label="Ethereum Private Key",
        pattern=re.compile(r"\b0x[0-9a-fA-F]{64}\b"),
        fake=_dec("MHhhYmNkZWYwMTIzNDU2Nzg5YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWYw"),
    ),
    # 10. Bearer Token (Authorization header) — 遮蔽整个 Bearer token，保留 "Bearer " 前缀
    MaskingRule(
        id="bearer-token",
        label="Bearer Token",
        pattern=re.compile(r"Bearer\s+([A-Za-z0-9._\-=]{20,})"),
        group=1,
        fake="Bearer_REDACTED_TOKEN_xxxxxxxxxxxxxxxxxxxx",
    ),
]
