"""将本地 provider 基础配置导出为不含账号/密钥的渠道市场模板。"""
from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from channel import normalize_model_id_rewrite_rules

SCHEMA = "ai-lubricant.channel-template/v1"
_SAFE_RE = re.compile(r"[^a-z0-9._-]+")


def _safe_base_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlsplit(text)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    host = parsed.hostname
    if parsed.port:
        host += f":{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path.rstrip("/"), "", ""))


def _slug(value: str) -> str:
    normalized = _SAFE_RE.sub("-", value.lower()).strip("-._")
    return (normalized or hashlib.sha256(value.encode()).hexdigest()[:16])[:120]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))


def _template_summary(cfg: dict) -> str:
    """模板说明：渠道自带 description（兼容 summary 字段）原样带出，没有就留空。

    不再生成「由当前渠道导出…」占位文案——那只会让所有模板的说明变成同一句话。
    """
    for key in ("description", "summary"):
        text = str(cfg.get(key) or "").strip()
        if text:
            return text
    return ""


def resolve_requested_providers(requested: Any, available: Any) -> tuple[list[str], list[str]]:
    """把「从当前平台导入」弹框勾选的渠道收敛成最终要导出的清单。

    返回 ``(selected, unknown)``：``requested`` 为空/非列表时导出全部可用渠道
    （脚本与旧调用方沿用此行为）；否则按勾选项去重排序，并把不在 ``available``
    里的挑出来交给调用方报错。纯函数，便于单测。
    """
    avail = {str(name) for name in (available or [])}
    if not isinstance(requested, list) or not requested:
        return sorted(avail), []
    picked = list(dict.fromkeys(str(name).strip() for name in requested if str(name).strip()))
    selected = sorted(set(picked) & avail)
    unknown = [name for name in picked if name not in avail]
    return selected, unknown


def _protocol_rows(cfg: dict) -> list[dict]:
    raw = cfg.get("chat_protocols") or cfg.get("endpoint_configs") or []
    rows = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, dict):
            continue
        rows.append({
            "enabled": item.get("enabled") is not False,
            "protocol": str(item.get("protocol") or "openai").strip().lower(),
            "path": str(item.get("path") or "").strip(),
            "upstream_stream": item.get("upstream_stream", True),
            "client_preset": str(item.get("client_preset") or "none"),
            "system_type": str(item.get("system_type") or "auto"),
            "send_reasoning_content": item.get("send_reasoning_content") is not False,
            "models": _string_list(item.get("models")),
        })
    return rows


_FREEZE_MODE_MAP = {
    "no_freeze": ("account", "none", 0),
    "account_daily": ("account", "today", 0),
    "account_model_daily": ("account_model", "today", 0),
    "account_permanent": ("account", "disabled", 0),
    "fixed_duration": ("account", "seconds", None),
    "account_model_fixed": ("account_model", "seconds", None),
    "account_model_weekly": ("account_model", "week", 0),
    "account_model_monthly": ("account_model", "month", 0),
    "channel_fixed": ("channel", "seconds", None),
    "channel_model_fixed": ("channel_model", "seconds", None),
}


def _freeze_rule(item: dict) -> dict:
    """归一化单条冻结规则到 object/period/value 三字段口径。

    优先用新字段（freeze_object/freeze_period/freeze_value）；只有旧 freeze_mode/
    freeze_seconds 时按映射转换，保证导出的模板与平台冻结规则口径一致。
    """
    base = {
        "condition": str(item.get("condition") or "status_code"),
        "key": str(item.get("key") or ""),
        "operator": str(item.get("operator") or "=="),
        "value": str(item.get("value") or ""),
        # 规则开关随模板走；缺省为开，导入端才不会把老模板的规则当停用。
        "enabled": item.get("enabled", True) is not False,
    }
    if item.get("freeze_object") or item.get("freeze_period"):
        base["freeze_object"] = str(item.get("freeze_object") or "account")
        base["freeze_period"] = str(item.get("freeze_period") or "seconds")
        base["freeze_value"] = max(0, int(item.get("freeze_value") or 0))
        return base
    mode = str(item.get("freeze_mode") or "fixed_duration").strip().lower()
    obj, period, fixed = _FREEZE_MODE_MAP.get(mode, ("account", "seconds", None))
    try:
        value = int(item.get("freeze_seconds") or 0) if fixed is None else fixed
    except (TypeError, ValueError):
        value = 0
    base["freeze_object"] = obj
    base["freeze_period"] = period
    base["freeze_value"] = max(0, value)
    return base


def _freeze_policy(policy: dict) -> dict:
    raw = policy.get("freeze_policy") if isinstance(policy, dict) else {}
    raw = raw if isinstance(raw, dict) else {}
    rules = [_freeze_rule(item) for item in (raw.get("rules") or []) if isinstance(item, dict)]
    return {"enabled": raw.get("enabled") is not False, "rules": rules}


def build_manifest(name: str, cfg: dict, policy: dict) -> dict:
    display_name = str(cfg.get("remark") or name).strip() or name
    # marketplace 不入库 base64 data URL（体积大且不可移植）：导出时清空，
    # 保留内置符号键与 HTTP/HTTPS 图片 URL。
    raw_icon = str(cfg.get("icon") or "").strip()
    manifest_icon = "" if raw_icon.lower().startswith("data:") else raw_icon
    channel: dict[str, Any] = {
        "name": display_name,
        "enabled": cfg.get("enabled") is not False,
        "base_url": _safe_base_url(cfg.get("base_url") or ""),
        "billing_mode": "request" if cfg.get("billing_mode") == "request" else "token",
        "timeout": max(1, min(3600, int(cfg.get("timeout") or 120))),
        "retry_count": cfg.get("retry_count"),
        "extra_retry_status_codes": [int(code) for code in (cfg.get("extra_retry_status_codes") or []) if isinstance(code, int) and 100 <= code <= 599],
        "chat_protocols": _protocol_rows(cfg),
        "models_path": str(cfg.get("models_path") or ""),
        "image_path": str(cfg.get("image_path") or ""),
        "video_path": str(cfg.get("video_path") or ""),
        "speech_path": str(cfg.get("speech_path") or ""),
        "website_url": str(cfg.get("website_url") or ""),
        "rate_limit": dict(cfg.get("rate_limit") or {}),
        "auto_update_models": bool(cfg.get("auto_update_models", False)),
        "model_id_rewrite_rules": normalize_model_id_rewrite_rules(cfg.get("model_id_rewrite_rules")),
    }
    if channel["retry_count"] is not None:
        channel["retry_count"] = max(0, min(10, int(channel["retry_count"])))
    item_id = f"local.{_slug(name)}"
    return {
        "schema": SCHEMA, "id": item_id, "kind": "channel_template", "name": _slug(name),
        "display_name": display_name,
        "summary": _template_summary(cfg),
        "publisher": "local-ai-lubricant", "category": str(cfg.get("builtin_type") or cfg.get("type") or "custom"),
        "tags": _string_list(cfg.get("tags")),
        "icon": manifest_icon,
        "version": "1.0.0", "status": "published",
        "resource": {"type": "channel_template", "channel": channel, "freeze_policy": _freeze_policy(policy)},
    }
