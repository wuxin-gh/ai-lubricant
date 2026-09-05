"""安全模块数据结构"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(slots=True)
class RiskTag:
    """单条风险标签"""
    tag: str            # 如 "secret:openai-key", "injection:role-override"
    severity: str       # "info" | "warn" | "high"
    detail: dict = field(default_factory=dict)


@dataclass(slots=True)
class ScanRequest:
    """传给检测器的扫描请求"""
    messages: list[dict]       # OpenAI 格式消息列表
    model: str = ""
    api_key: str | None = None
    request_id: str = ""
    stream: bool = False
    extra_text: str = ""       # system prompt 等附加文本

    @classmethod
    def from_body(cls, body: dict, *, api_key: str | None = None,
                  request_id: str = "", extra_text: str = "") -> ScanRequest:
        """从请求 body 构建 ScanRequest"""
        messages = body.get("messages") or []
        return cls(
            messages=messages,
            model=body.get("model", ""),
            api_key=api_key,
            request_id=request_id,
            stream=bool(body.get("stream")),
            extra_text=extra_text,
        )


@dataclass(slots=True)
class ScanResult:
    """检测管线返回结果"""
    tags: list[RiskTag] = field(default_factory=list)
    blocked: bool = False
    block_reason: str = ""

    @property
    def has_tags(self) -> bool:
        return len(self.tags) > 0

    @property
    def high_tags(self) -> list[RiskTag]:
        return [t for t in self.tags if t.severity == "high"]

    @property
    def warn_tags(self) -> list[RiskTag]:
        return [t for t in self.tags if t.severity == "warn"]


@dataclass(slots=True)
class MaskingRule:
    """单条凭据遮蔽规则"""
    id: str
    label: str
    pattern: re.Pattern[str]
    fake: str
    group: int | None = None   # 捕获组索引，None = 整个匹配


@dataclass(slots=True)
class MaskResult:
    """遮蔽结果"""
    masked_body: dict                          # 遮蔽后的请求体（深拷贝）
    restore_map: dict[str, str] = field(default_factory=dict)  # fake -> real
