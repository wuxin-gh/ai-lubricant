"""安全检测器注册表

新增检测器只需：
1. 在 detectors/ 下新建模块，实现 `def detect(req: ScanRequest) -> list[RiskTag]`
2. 在此文件 import 并 append 到 DETECTORS
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from security.types import RiskTag, ScanRequest

# 检测器函数签名: (ScanRequest) -> list[RiskTag]
Detector = "Callable[[ScanRequest], list[RiskTag]]"

# 延迟导入避免循环引用
def _build_detectors() -> list:
    from security.detectors.credential_leak import detect as credential_leak_detect
    from security.detectors.prompt_injection import detect as prompt_injection_detect
    from security.detectors.input_validation import detect as input_validation_detect
    return [input_validation_detect, credential_leak_detect, prompt_injection_detect]


DETECTORS: list = []


def init_detectors() -> None:
    """初始化检测器列表（模块首次使用时调用）"""
    if not DETECTORS:
        DETECTORS.extend(_build_detectors())
