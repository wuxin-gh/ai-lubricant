"""渠道市场模板的安全解析与基础配置合并。

本模块不访问账号表，也不写数据库。路由层负责鉴权、持久化、运行时广播和审计；
这里仅将已验证的市场 manifest 收敛为 provider_configs 可安全接收的基础配置补丁。
"""
from __future__ import annotations

import copy
import datetime as dt
from typing import Any

from monkeycode_compat.marketplace.validator import validate_manifest

# 完整渠道模板可覆盖的、与账号无关的基础配置字段。不要在这里增加 accounts、
# header_template 或任何凭据字段；市场 validator 也会在上游再次拒绝它们。
_TEMPLATE_CHANNEL_FIELDS = {
    "name",
    "enabled",
    "base_url",
    "billing_mode",
    "timeout",
    "retry_count",
    "extra_retry_status_codes",
    "chat_protocols",
    "models_path",
    "image_path",
    "video_path",
    "speech_path",
    "rate_limit",
    "account_priority",
    "account_weight",
    "auto_update_models",
    "model_id_rewrite_rules",
    "website_url",
}


def apply_channel_template(existing: dict[str, Any], manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """合并一个渠道模板，返回 ``(base_config, freeze_policy)``。

    仅覆盖模板显式声明的基础字段；任何未声明的本地配置保持不变。账户凭据不属于
    ``existing`` 的基础配置读取范围，调用方用 ``write_provider_base_async`` 持久化时
    也不会触及 ``provider_accounts``。
    """
    errors = validate_manifest("channels", manifest)
    if errors:
        raise ValueError("; ".join(errors))

    resource = manifest.get("resource")
    if not isinstance(resource, dict):  # 已由 validator 保证，此处留作边界保护。
        raise ValueError("渠道模板缺少 resource")
    channel = resource.get("channel")
    if not isinstance(channel, dict):
        raise ValueError("渠道模板缺少 resource.channel")
    freeze_policy = resource.get("freeze_policy")
    if not isinstance(freeze_policy, dict):
        raise ValueError("渠道模板缺少 resource.freeze_policy")

    merged = copy.deepcopy(existing)
    for key in _TEMPLATE_CHANNEL_FIELDS:
        if key in channel:
            merged[key] = copy.deepcopy(channel[key])
    # icon 是 manifest 顶层元数据；应用模板时同步到 provider 基础配置。
    if "icon" in manifest:
        merged["icon"] = copy.deepcopy(manifest.get("icon") or "")

    # 来源字段属于基础配置元数据，展示/聚合时可读；不影响渠道运行配置。
    merged["channel_template_id"] = str(manifest["id"])
    merged["channel_template_version"] = str(manifest.get("version") or "")
    merged["channel_template_applied_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    merged["channel_config_revision"] = int(merged.get("channel_config_revision") or 0) + 1
    return merged, copy.deepcopy(freeze_policy)
