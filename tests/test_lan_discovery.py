"""LAN 发现应答器（lan_discovery.py）的零依赖单测。

不碰 PG / Redis / 真实网络：datagram_received 是同步回调，用 FakeTransport 捕获
sendto 即可验证协议行为。覆盖：魔法串匹配响应、非魔法串忽略、per-IP 限速、
loopback 抑制判定（should_respond）、build_response 字段。
"""
import json
import sys
import time
from pathlib import Path

_PROJ = Path(__file__).resolve().parents[1]
if str(_PROJ) not in sys.path:
    sys.path.insert(0, str(_PROJ))

import lan_discovery


class FakeTransport:
    """捕获 sendto 的假 transport；datagram_received 同步回调可直接驱动。"""

    def __init__(self):
        self.sent: list[tuple[bytes, tuple]] = []

    def sendto(self, data, addr):
        self.sent.append((data, addr))


def _make_protocol():
    proto = lan_discovery.LanDiscoveryProtocol()
    transport = FakeTransport()
    proto.connection_made(transport)
    return proto, transport


def test_magic_probe_gets_json_response():
    proto, transport = _make_protocol()
    addr = ("192.168.1.50", 54321)
    proto.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), addr)
    assert len(transport.sent) == 1
    data, to = transport.sent[0]
    assert to == addr
    payload = json.loads(data)
    assert payload["app"] == "ai-lubricant"
    assert "name" in payload and "version" in payload and "http_port" in payload


def test_non_magic_datagram_ignored():
    proto, transport = _make_protocol()
    for junk in (b"HELLO", b"", b'{"json":1}', lan_discovery.DISCOVERY_MAGIC.encode() + b"X"):
        proto.datagram_received(junk, ("10.0.0.2", 1000))
    assert transport.sent == []


def test_rate_limit_suppresses_second_probe_within_window(monkeypatch):
    proto, transport = _make_protocol()
    addr = ("192.168.1.51", 1000)
    proto.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), addr)
    # 窗口内第二包被限速吞掉
    proto.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), addr)
    assert len(transport.sent) == 1

    # 把限速窗口调没（往前拨时间）后第三包恢复响应
    monkeypatch.setattr(lan_discovery, "_RATE_LIMIT_SECONDS", 0.0)
    proto2, transport2 = _make_protocol()
    proto2.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), addr)
    proto2.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), addr)
    assert len(transport2.sent) == 2


def test_rate_limit_keys_by_ip():
    proto, transport = _make_protocol()
    proto.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), ("192.168.1.52", 1000))
    proto.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), ("192.168.1.53", 1000))
    # 不同 IP 不互相限速
    assert len(transport.sent) == 2


def test_respond_error_never_raises():
    # transport 故意不带 sendto（模拟异常路径），回调不能抛
    class BrokenTransport:
        def sendto(self, data, addr):
            raise RuntimeError("boom")

    proto = lan_discovery.LanDiscoveryProtocol()
    proto.connection_made(BrokenTransport())
    proto.datagram_received(lan_discovery.DISCOVERY_MAGIC.encode(), ("192.168.1.54", 1))


def test_should_respond_loopback_matrix(monkeypatch):
    # 未设置（裸跑 / docker）→ 响应
    monkeypatch.delenv("DESKTOP_MAIN_HOST", raising=False)
    monkeypatch.delenv("LAN_DISCOVERY_ALLOW_LOOPBACK", raising=False)
    assert lan_discovery.should_respond() is True

    # 显式 loopback → 默认抑制
    monkeypatch.setenv("DESKTOP_MAIN_HOST", "127.0.0.1")
    assert lan_discovery.should_respond() is False
    monkeypatch.setenv("DESKTOP_MAIN_HOST", "localhost")
    assert lan_discovery.should_respond() is False

    # loopback + ALLOW_LOOPBACK=true → 放行（本机联调）
    monkeypatch.setenv("LAN_DISCOVERY_ALLOW_LOOPBACK", "true")
    assert lan_discovery.should_respond() is True

    # 非 loopback 的显式绑定（如 0.0.0.0）→ 响应
    monkeypatch.setenv("DESKTOP_MAIN_HOST", "0.0.0.0")
    monkeypatch.delenv("LAN_DISCOVERY_ALLOW_LOOPBACK", raising=False)
    assert lan_discovery.should_respond() is True


def test_advertised_http_port_priority(monkeypatch):
    monkeypatch.delenv("LAN_DISCOVERY_HTTP_PORT", raising=False)
    monkeypatch.delenv("DESKTOP_MAIN_PORT", raising=False)
    assert lan_discovery.advertised_http_port() == 8001

    monkeypatch.setenv("DESKTOP_MAIN_PORT", "8101")
    assert lan_discovery.advertised_http_port() == 8101

    # 显式覆盖优先于桌面端口
    monkeypatch.setenv("LAN_DISCOVERY_HTTP_PORT", "3006")
    assert lan_discovery.advertised_http_port() == 3006


def test_build_response_fields(monkeypatch):
    monkeypatch.setenv("APP_VERSION", "260907")
    monkeypatch.setenv("LAN_DISCOVERY_SERVER_NAME", "客厅主机")
    monkeypatch.setenv("LAN_DISCOVERY_HTTP_PORT", "3006")
    payload = json.loads(lan_discovery.build_response())
    assert payload == {
        "app": "ai-lubricant",
        "name": "客厅主机",
        "version": "260907",
        "http_port": 3006,
    }


def test_rate_limiter_max_tracked_eviction():
    limiter = lan_discovery._RateLimiter()
    base_ip = "10.1."
    for i in range(lan_discovery._RateLimiter._MAX_TRACKED + 10):
        assert limiter.allow(f"{base_ip}{i}.1") is True
    # 淘汰是清空重来：表容量有界即可，不校验具体保留哪些 key
    assert len(limiter._last) <= lan_discovery._RateLimiter._MAX_TRACKED
