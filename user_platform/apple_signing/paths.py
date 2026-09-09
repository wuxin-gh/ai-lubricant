"""机器级缓存路径（移植自 iPASide paths.py，裁剪到只剩服务端需要的部分）。

只有 anisette provisioning 状态是机器级磁盘缓存——它是 Apple 眼里的「这台设备」
身份，所有账号共享，且**必须保持稳定**（反复重 provision 会触发 Apple 反滥用）。
其余一切可变状态（session token / cert / profile）都在 PG secret_data 里，
由调用方持有，本包不落盘。

目录默认在用户配置目录下的 ai-lubricant/，可用 ``APPLE_SIGNING_CACHE_DIR``
覆盖（测试 / 多实例隔离）。
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

_APP_NAME = "ai-lubricant"


def cache_dir() -> Path:
    """机器级缓存根目录（不存在则创建）。"""
    override = os.environ.get("APPLE_SIGNING_CACHE_DIR")
    if override:
        path = Path(override)
    elif os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~/AppData/Local")
        path = Path(base) / _APP_NAME
    else:
        base = os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache")
        path = Path(base) / _APP_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def anisette_state_file() -> Path:
    """anisette 便携库 + provisioning 状态的合并缓存文件。"""
    return cache_dir() / "anisette.bin"


def account_slug(email: str) -> str:
    """email 的稳定、文件系统安全的短标识（保留自 iPASide：日志/缓存里不泄露地址）。"""
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()[:16]
