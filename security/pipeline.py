"""安全检测管线编排器

参考 agentfw 的 risk/pipeline.ts：逐个运行已注册的检测器，
每个检测器的异常被隔离，不影响其他检测器。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from security.detectors import DETECTORS, init_detectors

if TYPE_CHECKING:
    from security.types import RiskTag, ScanRequest, ScanResult

logger = logging.getLogger("security.pipeline")


def scan_request_sync(req: ScanRequest) -> ScanResult:
    """同步版本：运行所有检测器，返回 ScanResult"""
    from security.types import ScanResult

    init_detectors()

    tags: list[RiskTag] = []
    for detector in DETECTORS:
        try:
            result = detector(req)
            if result:
                tags.extend(result)
        except Exception as e:
            logger.warning(f"安全检测器异常: {getattr(detector, '__name__', detector)}: {e}")

    # 根据配置决定是否阻断
    block_on_high = _get_block_on_high()
    blocked = block_on_high and any(t.severity == "high" for t in tags)
    block_reason = ""
    if blocked:
        from security.event_log import TAG_TITLE_MAP

        high_tags = [t for t in tags if t.severity == "high"]
        reasons = [TAG_TITLE_MAP.get(t.tag, f"安全事件: {t.tag}") for t in high_tags]
        block_reason = "安全风险检测: " + "、".join(reasons)

    return ScanResult(tags=tags, blocked=blocked, block_reason=block_reason)


async def scan_request(req: ScanRequest) -> ScanResult:
    """异步版本（当前为同步调用，未来可扩展为异步检测器）"""
    return scan_request_sync(req)


def _get_block_on_high() -> bool:
    """读取配置：high 级别是否阻断请求"""
    try:
        from config import Config
        return Config.security_block_on_high()
    except Exception:
        return True  # 默认阻断
