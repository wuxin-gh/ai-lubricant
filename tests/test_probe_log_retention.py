"""定时检测日志保留策略测试（全有或全无语义）。

开关「保留检测日志」关闭（route_info["_is_save_log"]=False）时，定时检测的成功与
失败 attempt 都不写请求日志：_enqueue_started_log 不入队 started，
_finalize_channel_attempt_log 不入队 finalized。手动测试/正常请求缺省视为 True，
始终写日志。
"""
from __future__ import annotations

import asyncio

import main


def _pending_log(attempt_key: str = "probe-attempt") -> dict:
    return {
        "request_id": "probe-request",
        "attempt_key": attempt_key,
        "attempt_no": 1,
        "success": False,
        "status": "requesting",
    }


async def _finalize(route_info: dict, *, success: bool) -> None:
    await main._finalize_channel_attempt_log(
        request_id="probe-request",
        route_info=route_info,
        model="m1",
        messages=[{"role": "user", "content": "hi"}],
        stream=False,
        api_key="admin-check",
        api_key_name="admin-check",
        request_headers={},
        client_request_path="/admin/providers/demo/accounts/test",
        client_request_body={"model": "m1"},
        attempt_start=1.0,
        success=success,
        status="ok" if success else "error",
        response_body={"ok": True} if success else None,
        error="" if success else "upstream failed",
    )


def test_probe_save_log_off_writes_nothing_and_skips_proxy(monkeypatch):
    """关闭保留日志：_finalize_channel_attempt_log 在读 is_save_log 处直接短路，
    不调 enqueue_started/enqueue_finalized，也不读出站代理。"""
    events: list[tuple[str, dict]] = []
    proxy_reads: list[bool] = []
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kwargs: events.append(("started", kwargs)))
    monkeypatch.setattr(main.request_log_writer, "enqueue_finalized", lambda **kwargs: events.append(("finalized", kwargs)))

    def fake_take_proxy():
        proxy_reads.append(True)
        return {"mode": "direct"}

    monkeypatch.setattr(main, "take_outbound_proxy", fake_take_proxy)
    route_info = {
        "provider": "demo",
        "account": "acct1",
        "attempt_key": "probe-attempt",
        "attempt_no": 1,
        "_is_save_log": False,
    }

    asyncio.run(_finalize(route_info, success=False))

    assert events == []
    assert proxy_reads == []


def test_probe_save_log_off_skips_success_too(monkeypatch):
    """关闭保留日志：成功 attempt 同样不写——成功失败一视同仁。"""
    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kwargs: events.append(("started", kwargs)))
    monkeypatch.setattr(main.request_log_writer, "enqueue_finalized", lambda **kwargs: events.append(("finalized", kwargs)))
    monkeypatch.setattr(main, "take_outbound_proxy", lambda: None)
    route_info = {
        "provider": "demo",
        "account": "acct1",
        "attempt_key": "probe-attempt",
        "attempt_no": 1,
        "_is_save_log": False,
    }

    asyncio.run(_finalize(route_info, success=True))

    assert events == []


def test_probe_save_log_on_writes_finalized(monkeypatch):
    """默认（保留日志）：_finalize_channel_attempt_log 写 finalized 一条。"""
    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kwargs: events.append(("started", kwargs)))
    monkeypatch.setattr(main.request_log_writer, "enqueue_finalized", lambda **kwargs: events.append(("finalized", kwargs)))
    monkeypatch.setattr(main, "take_outbound_proxy", lambda: None)
    route_info = {
        "provider": "demo",
        "account": "acct1",
        "attempt_key": "probe-attempt",
        "attempt_no": 1,
        "_is_save_log": True,
    }

    asyncio.run(_finalize(route_info, success=True))

    assert [kind for kind, _ in events] == ["finalized"]
    assert events[0][1]["attempt_key"] == "probe-attempt"
    assert events[0][1]["payload"]["success"] is True


def test_regular_request_without_flag_still_finalizes(monkeypatch):
    """正常请求 route_info 不带 _is_save_log → 缺省 True，照常写 finalized。"""
    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(main.request_log_writer, "enqueue_finalized", lambda **kwargs: events.append(("finalized", kwargs)))
    monkeypatch.setattr(main, "take_outbound_proxy", lambda: None)
    route_info = {
        "provider": "demo",
        "account": "acct1",
        "attempt_key": "normal-attempt",
        "attempt_no": 1,
    }

    asyncio.run(_finalize(route_info, success=False))

    assert [kind for kind, _ in events] == ["finalized"]
    assert events[0][1]["attempt_key"] == "normal-attempt"


def test_enqueue_started_respects_is_save_log(monkeypatch):
    """_enqueue_started_log：_is_save_log=False 不入队，True 入队 1 条。"""
    events: list[dict] = []
    monkeypatch.setattr(main.request_log_writer, "enqueue_started", lambda **kwargs: events.append(kwargs))
    pending = _pending_log()

    route_info = {"_is_save_log": False}
    main._enqueue_started_log(request_id="probe-request", route_info=route_info, pending_log=pending)
    assert events == []

    route_info = {"_is_save_log": True}
    main._enqueue_started_log(request_id="probe-request", route_info=route_info, pending_log=pending)
    assert len(events) == 1
    assert events[0]["attempt_key"] == "probe-attempt"
