"""Apple 端点的 TLS 信任（移植自 iPASide tls.py，缓存位置改走本包 paths）。

gsa.apple.com 呈现的证书链到 Apple 自家 Root CA，不在公共信任库（certifi）也不常在
OS 库里。这里钉 Apple CA 链，验证合并 bundle（certifi 公共根 + Apple 根/中间证书）。
绝不禁用证书校验——认证流承载密码。bundle 构建一次后缓存于机器级缓存目录。
"""

from __future__ import annotations

import threading
from pathlib import Path

import certifi

from . import paths

_APPLE_CA = Path(__file__).parent / "certs" / "apple_gsa_ca.pem"

_bundle_lock = threading.Lock()
_bundle_path: str | None = None


def ca_bundle() -> str:
    """返回「公共根 + Apple CA」合并 bundle 的路径（每进程构建一次）。"""
    global _bundle_path
    if _bundle_path:
        return _bundle_path
    with _bundle_lock:
        if _bundle_path:
            return _bundle_path
        combined = paths.cache_dir() / "apple_ca_bundle.pem"
        public = Path(certifi.where()).read_bytes()
        apple = _APPLE_CA.read_bytes()
        combined.write_bytes(public + b"\n" + apple)
        _bundle_path = str(combined)
        return _bundle_path
