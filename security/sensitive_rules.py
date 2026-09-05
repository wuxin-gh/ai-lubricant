"""可配置敏感数据规则加载器

统一驱动凭据泄露检测和凭据遮蔽。
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from security.masking_rules import BUILTIN_RULES

logger = logging.getLogger("security.sensitive_rules")

SEVERITIES = {"info", "warn", "high"}
_DETECT_IDS = {
    "anthropic-key",
    "openai-key",
    "stripe-key",
    "github-pat",
    "aws-akid",
    "google-key",
    "slack-token",
    "bearer-token",
    "aws-secret",
}


@dataclass(slots=True)
class SensitiveRule:
    """单条敏感数据规则，同时用于检测和遮蔽"""

    id: str
    label: str
    pattern: re.Pattern[str]
    pattern_src: str
    ignorecase: bool = False
    severity: str = "high"
    detect_enabled: bool = True
    mask_enabled: bool = True
    fake: str = ""
    group: int | None = None
    enabled: bool = True
    builtin: bool = False


def seed_sensitive_rules() -> list[dict[str, Any]]:
    """从内置遮蔽规则生成可配置规则字典"""
    rules: list[dict[str, Any]] = []
    for rule in BUILTIN_RULES:
        rules.append({
            "id": rule.id,
            "label": rule.label,
            "pattern": rule.pattern.pattern,
            "ignorecase": bool(rule.pattern.flags & re.IGNORECASE),
            "severity": "high",
            "detect_enabled": rule.id in _DETECT_IDS,
            "mask_enabled": True,
            "fake": rule.fake,
            "group": rule.group,
            "enabled": True,
            "builtin": True,
        })
    return rules


def normalize_rule(raw: dict[str, Any]) -> dict[str, Any]:
    """归一化规则字典，便于 API 返回和保存"""
    group = raw.get("group")
    if group in ("", None):
        group = None
    else:
        group = int(group)

    rule_id = str(raw.get("id") or "").strip()
    label = str(raw.get("label") or rule_id).strip() or rule_id
    severity = str(raw.get("severity") or "high")
    if severity not in SEVERITIES:
        severity = "high"

    return {
        "id": rule_id,
        "label": label,
        "pattern": str(raw.get("pattern") or ""),
        "ignorecase": bool(raw.get("ignorecase", False)),
        "severity": severity,
        "detect_enabled": bool(raw.get("detect_enabled", True)),
        "mask_enabled": bool(raw.get("mask_enabled", True)),
        "fake": str(raw.get("fake") or ""),
        "group": group,
        "enabled": bool(raw.get("enabled", True)),
        "builtin": bool(raw.get("builtin", False)),
    }


def _compile_one(raw: dict[str, Any]) -> SensitiveRule:
    normalized = normalize_rule(raw)
    flags = re.IGNORECASE if normalized["ignorecase"] else 0
    compiled = re.compile(normalized["pattern"], flags)
    return SensitiveRule(
        id=normalized["id"],
        label=normalized["label"],
        pattern=compiled,
        pattern_src=normalized["pattern"],
        ignorecase=normalized["ignorecase"],
        severity=normalized["severity"],
        detect_enabled=normalized["detect_enabled"],
        mask_enabled=normalized["mask_enabled"],
        fake=normalized["fake"],
        group=normalized["group"],
        enabled=normalized["enabled"],
        builtin=normalized["builtin"],
    )


def compile_rules(raw_rules: list[dict[str, Any]]) -> list[SensitiveRule]:
    """编译规则；单条坏正则只跳过，不影响安全管线"""
    compiled: list[SensitiveRule] = []
    for raw in raw_rules:
        try:
            compiled.append(_compile_one(raw))
        except Exception as e:
            logger.warning("敏感数据规则编译失败，已跳过: %s: %s", raw.get("id"), e)
    return compiled


def _load_raw_rules() -> list[dict[str, Any]]:
    try:
        from config import Config
        security = Config._load().get("security", {})
        rules = security.get("sensitive_rules") if isinstance(security, dict) else None
        if isinstance(rules, list) and rules:
            return [r for r in rules if isinstance(r, dict)]
    except Exception as e:
        logger.warning("读取敏感数据规则失败，使用内置规则: %s", e)
    return seed_sensitive_rules()


_cache_sig: str | None = None
_cache_rules: list[SensitiveRule] | None = None


def _signature(raw_rules: list[dict[str, Any]]) -> str:
    return json.dumps(raw_rules, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def refresh_cache() -> None:
    """清空本进程规则缓存"""
    global _cache_sig, _cache_rules
    _cache_sig = None
    _cache_rules = None


def get_sensitive_rules() -> list[SensitiveRule]:
    """读取并编译敏感数据规则"""
    global _cache_sig, _cache_rules
    raw_rules = _load_raw_rules()
    sig = _signature(raw_rules)
    if _cache_sig == sig and _cache_rules is not None:
        return _cache_rules
    _cache_rules = compile_rules(raw_rules)
    _cache_sig = sig
    return _cache_rules


def get_detection_rules() -> list[SensitiveRule]:
    """启用的凭据泄露检测规则"""
    return [r for r in get_sensitive_rules() if r.enabled and r.detect_enabled]


def get_masking_rules() -> list[SensitiveRule]:
    """启用的凭据遮蔽规则，保持配置顺序"""
    return [r for r in get_sensitive_rules() if r.enabled and r.mask_enabled]


def validate_rule(raw: dict[str, Any]) -> str | None:
    """校验规则；返回错误信息或 None"""
    try:
        rule = normalize_rule(raw)
    except Exception as e:
        return f"规则格式无效: {e}"

    if not rule["id"]:
        return "id 不能为空"
    if not rule["label"]:
        return "label 不能为空"
    if not rule["pattern"].strip():
        return "pattern 不能为空"
    if rule["severity"] not in SEVERITIES:
        return "severity 非法"

    try:
        compiled = re.compile(rule["pattern"], re.IGNORECASE if rule["ignorecase"] else 0)
    except re.error as e:
        return f"正则无效: {e}"

    group = rule.get("group")
    if group is not None:
        if group < 0 or group > compiled.groups:
            return f"group 超出范围（该正则有 {compiled.groups} 个捕获组）"

    return None


def normalize_rules_for_storage(raw_rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """保存前统一规则格式"""
    return [normalize_rule(r) for r in raw_rules]
