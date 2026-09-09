"""LAN discovery responder — 让同网段的手机 App 自动找到登录地址。

手机端（mobile/src/native/lanDiscover.ts + plugins/withLanDiscovery.js 注入的原生
模块）向 ``255.255.255.255:LAN_DISCOVERY_PORT`` 发一个 UDP 广播，payload 为魔法串
``AILUBRICANT_DISCOVER_V1``；本模块在 main.py 的 lifespan 里监听同一端口，收到
匹配的质询后把 ``{app, name, version, http_port}`` 单播回源地址。手机用「响应
数据报的源 IP + http_port」拼出登录地址，因此服务器无需自知本机 IP——docker 端口
映射下响应源地址也会被 NAT 正确改写为宿主 IP（见 docs/lan-discovery.md 的部署
限制）。

设计约束：
- **绝不阻断启动**：bind 失败 / 端口被占 / 协议异常一律只 ``logger.warning``。
- **loopback 抑制是硬闸门**：桌面版默认 ``DESKTOP_MAIN_HOST=127.0.0.1`` 时响应了
  手机也连不上，此时默认不响应并打提示；只有显式 ``LAN_DISCOVERY_ALLOW_LOOPBACK``
  =true 才绕过（绕过也只是为了本机联调，无实际可用性）。
- 按源 IP 限速（默认 500ms/次）：响应是 ~120B 的小单播，限速只为防同一地址高频
  质询被滥用，放大系数本来就只有 ~6x。
- 纯逻辑（build_response / should_respond / 限速）拆成独立函数便于零依赖单测
  （tests/test_lan_discovery.py）。
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import time

from loguru import logger

DISCOVERY_MAGIC = "AILUBRICANT_DISCOVER_V1"
DEFAULT_DISCOVERY_PORT = 58160
DEFAULT_HTTP_PORT = 8001
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")
# 单 IP 两次响应之间的最小间隔（秒）。
_RATE_LIMIT_SECONDS = 0.5

_transport: asyncio.DatagramTransport | None = None


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def advertised_http_port() -> int:
    """响应里通告的 HTTP 端口。

    优先级：显式 ``LAN_DISCOVERY_HTTP_PORT``（docker compose 里设为宿主映射端口）
    → ``DESKTOP_MAIN_PORT``（桌面版 uvicorn 实际监听端口）→ 硬编码 8001。
    注意不读 ``SERVER_PORT``——它在 .env.example 里存在但没有任何代码消费。
    """
    for name in ("LAN_DISCOVERY_HTTP_PORT", "DESKTOP_MAIN_PORT"):
        raw = os.environ.get(name)
        if raw:
            try:
                return int(raw)
            except ValueError:
                logger.warning("[lan-discovery] {}={!r} 不是数字，忽略", name, raw)
    return DEFAULT_HTTP_PORT


def build_response() -> bytes:
    """构造质询响应 JSON（一次构造，进程内复用也无所谓，保持简单每次现造）。"""
    return json.dumps(
        {
            "app": "ai-lubricant",
            "name": (os.environ.get("LAN_DISCOVERY_SERVER_NAME") or "Ai Lubricant").strip() or "Ai Lubricant",
            "version": os.getenv("APP_VERSION", "dev").strip() or "dev",
            "http_port": advertised_http_port(),
        },
        separators=(",", ":"),
    ).encode()


def should_respond() -> bool:
    """HTTP bind 为 loopback 时抑制响应（响应了手机也连不上）。

    判定依据 ``DESKTOP_MAIN_HOST``：未设置（裸跑 python main.py / docker，绑定
    0.0.0.0 或 ::）视为对外可用；显式 loopback 才抑制。``LAN_DISCOVERY_ALLOW_LOOPBACK``
    =true 可绕过，仅供本机联调。
    """
    host = os.environ.get("DESKTOP_MAIN_HOST")
    if host is not None and host.strip().lower() in _LOOPBACK_HOSTS:
        return _env_flag("LAN_DISCOVERY_ALLOW_LOOPBACK", default=False)
    return True


class _RateLimiter:
    """per-IP 最小响应间隔；仅跟踪最近活跃的少量地址，防内存膨胀。"""

    _MAX_TRACKED = 256

    def __init__(self) -> None:
        self._last: dict[str, float] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        last = self._last.get(key)
        if last is not None and now - last < _RATE_LIMIT_SECONDS:
            return False
        if last is None and len(self._last) >= self._MAX_TRACKED:
            # 简单淘汰：清空重来。质询频率极低（人手点扫描），损失可忽略。
            self._last.clear()
        self._last[key] = now
        return True


class LanDiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self._limiter = _RateLimiter()
        self._response = build_response()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:  # type: ignore[override]
        self.transport = transport  # type: ignore[attr-defined]  # DatagramProtocol 约定属性

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        try:
            text = data.decode("utf-8", errors="ignore").strip()
            if text != DISCOVERY_MAGIC:
                return
            if not self._limiter.allow(str(addr[0])):
                return
            transport = getattr(self, "transport", None)
            if transport is None:
                return
            transport.sendto(self._response, addr)
        except Exception as exc:  # noqa: BLE001 - 响应绝不能炸掉事件循环
            logger.warning("[lan-discovery] respond to {} failed: {}", addr, exc)


async def start_lan_discovery() -> bool:
    """在当前事件循环上启动 UDP 监听。失败只 warning，返回 False 不抛。

    由 main.py lifespan 调用；三处部署形态（python main.py / docker main:app /
    desktop shell_asgi 包装）都走这里，无需各自集成。
    """
    global _transport
    if not _env_flag("LAN_DISCOVERY_ENABLED", default=True):
        logger.info("[lan-discovery] disabled via LAN_DISCOVERY_ENABLED")
        return False
    if not should_respond():
        logger.warning(
            "[lan-discovery] HTTP 仅监听 loopback（DESKTOP_MAIN_HOST={}），局域网发现已抑制；"
            "设 DESKTOP_MAIN_HOST=0.0.0.0 并放行防火墙后可用，或 LAN_DISCOVERY_ALLOW_LOOPBACK=true 强制响应（仅本机联调）",
            os.environ.get("DESKTOP_MAIN_HOST"),
        )
        return False
    try:
        port = int(os.environ.get("LAN_DISCOVERY_PORT") or DEFAULT_DISCOVERY_PORT)
    except ValueError:
        port = DEFAULT_DISCOVERY_PORT
    try:
        loop = asyncio.get_running_loop()
        # family 必须显式 AF_INET：不指定时部分平台会建 IPv6 socket，收不到
        # IPv4 limited broadcast（255.255.255.255）。
        transport, _protocol = await loop.create_datagram_endpoint(
            LanDiscoveryProtocol,
            local_addr=("0.0.0.0", port),
            family=socket.AF_INET,
        )
        _transport = transport  # type: ignore[assignment]
        logger.info(
            "[lan-discovery] listening on 0.0.0.0:{}, advertising http_port={}",
            port, advertised_http_port(),
        )
        return True
    except OSError as exc:
        logger.warning("[lan-discovery] bind 0.0.0.0:{} failed ({}); LAN discovery off", port, exc)
        return False
    except Exception:
        logger.exception("[lan-discovery] unexpected startup failure; LAN discovery off")
        return False


async def stop_lan_discovery() -> None:
    """关掉 UDP 监听（transport.close() 同步且幂等，重复调用安全）。"""
    global _transport
    transport, _transport = _transport, None
    if transport is not None:
        try:
            transport.close()
        except Exception:  # noqa: BLE001
            logger.exception("[lan-discovery] close failed")
