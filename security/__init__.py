"""安全模块 —— 风险检测、凭据遮蔽、安全事件日志

参考 agentfw 的安全架构：
- 纯函数检测器插件管线（pipeline.py）
- 凭据遮蔽/还原（masking.py）
- 安全事件审计（event_log.py）
"""

from security.event_log import log_security_event, log_security_events
from security.masking import (
    is_sensitive_header,
    mask_authorization_header,
    mask_credentials,
    mask_secret_value,
    restore_credentials,
    restore_response_body,
    sanitize_header_value,
)
from security.pipeline import scan_request, scan_request_sync
from security.types import MaskResult, MaskingRule, RiskTag, ScanRequest, ScanResult

__all__ = [
    "MaskResult",
    "MaskingRule",
    "RiskTag",
    "ScanRequest",
    "ScanResult",
    "log_security_event",
    "is_sensitive_header",
    "log_security_events",
    "mask_authorization_header",
    "mask_credentials",
    "mask_secret_value",
    "restore_credentials",
    "restore_response_body",
    "sanitize_header_value",
    "scan_request",
    "scan_request_sync",
]
