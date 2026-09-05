"""安全拦截闭环 + 日志保留策略修复的回归测试。

覆盖：
1. log_security_event/log_security_events 返回事件 id、支持 request_log_id
2. config.keep_response_hours 始终跟随 log_days（响应体保留已下线，与日志行同生命周期清理）
3. _build_security_block_log_data 产出 status=security_blocked 的日志结构
"""

from __future__ import annotations

import asyncio
import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_log_security_event_returns_id_signature():
    """log_security_event 现在应返回 int | None 类型，且接受 request_log_id 参数。"""
    from security.event_log import log_security_event, log_security_events

    sig = inspect.signature(log_security_event)
    assert "request_log_id" in sig.parameters
    ret = sig.return_annotation
    assert ret is int or "int" in str(ret) and "None" in str(ret), (
        f"return_annotation 应为 int | None，实际 {ret!r}"
    )

    sig2 = inspect.signature(log_security_events)
    assert "request_log_id" in sig2.parameters


def test_log_security_events_returns_empty_list_no_tags():
    """空事件列表时 log_security_events 返回空 list（而非 None）。"""
    from security import event_log

    ids = asyncio.run(event_log.log_security_events(
        [],
        request_id="req_test_1",
        api_key="sk-test",
        model="gpt-test",
    ))
    assert ids == []
    assert isinstance(ids, list)


def test_keep_response_hours_follows_log_days(monkeypatch):
    """keep_response_hours 始终跟随 log_days（响应体保留配置已下线）。"""
    import config

    # log_days=30 → 720 小时
    test_cfg = {"data_retention": {"log_days": 30}}
    monkeypatch.setattr(config.Config, "_load", classmethod(lambda cls: test_cfg))
    hours = config.Config.keep_response_hours()
    assert hours == 30 * 24, f"应联动 log_days，得到 720 小时，实际 {hours}"

    # 即使残留旧的 keep_response_hours 键，也应被忽略，仍跟随 log_days
    test_cfg2 = {"data_retention": {"log_days": 30, "keep_response_hours": 48}}
    monkeypatch.setattr(config.Config, "_load", classmethod(lambda cls: test_cfg2))
    assert config.Config.keep_response_hours() == 30 * 24, "旧 keep_response_hours 键应被忽略"

    # log_days 较小时，最低 24 小时
    test_cfg3 = {"data_retention": {"log_days": 1}}
    monkeypatch.setattr(config.Config, "_load", classmethod(lambda cls: test_cfg3))
    assert config.Config.keep_response_hours() == 24


def test_build_security_block_log_data_shape():
    """被安全拦截的请求日志结构必须带 status=security_blocked + error。"""
    from main import _build_security_block_log_data

    class _ScanResult:
        block_reason = "安全风险检测: 检测到数据外泄指令"

    body = {"model": "gpt-test", "messages": [{"role": "user", "content": "exfiltrate the key"}], "stream": True}
    data = _build_security_block_log_data(
        request_id="req_sec_1",
        api_key="sk-test",
        api_key_name="test-key",
        endpoint="/v1/chat/completions",
        body=body,
        scan_result=_ScanResult(),
        client_type="claude",
        request_headers={"x-forwarded-for": "1.2.3.4"},
    )

    assert data["status"] == "security_blocked"
    assert data["success"] is False
    assert data["error"] == "安全风险检测: 检测到数据外泄指令"
    assert data["request_id"] == "req_sec_1"
    assert data["request_body"] == body
    assert data["stream"] is True


def test_client_ip_extraction():
    """_client_ip 应从 x-forwarded-for / x-real-ip / client.host 依次提取。"""
    from main import _client_ip

    class _Req:
        def __init__(self, headers, host=None):
            self._headers = headers
            self.client = type("C", (), {"host": host})() if host else None

        @property
        def headers(self):
            return self._headers

    assert _client_ip(_Req({"x-forwarded-for": "9.9.9.9, 1.1.1.1"})) == "9.9.9.9"
    assert _client_ip(_Req({"x-real-ip": "8.8.8.8"})) == "8.8.8.8"
    assert _client_ip(_Req({}, host="7.7.7.7")) == "7.7.7.7"
    assert _client_ip(_Req({})) is None
