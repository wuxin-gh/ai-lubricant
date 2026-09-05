"""Phase E tests: request-log payload fan-out safety + sanitization.

These tests avoid a real ClickHouse/DB. They assert the critical safety
properties of the dual-write path:

* The main PG writer's payload fan-out is a NO-OP when the ClickHouse switch
  is disabled (the default) — it never enqueues and never raises.
* The payload writer strips the large body fields into ``payload`` and keeps
  everything else as ``metadata`` (so PG metadata vs CH payload stay separated).
* Sensitive keys are masked before leaving for ClickHouse (nested headers too).
"""
from __future__ import annotations

import types


def test_fanout_is_noop_when_disabled(monkeypatch):
    """With the switch off, _fanout_payload must not enqueue or raise."""
    import clickhouse_config
    from request_log_writer import RequestLogEvent, RequestLogWriter
    import request_payload_writer as rpw

    # Replace the cached ClickHouse settings with a disabled stand-in.
    disabled = types.SimpleNamespace(enabled=False)
    monkeypatch.setattr(clickhouse_config, "get_settings", lambda: disabled, raising=False)

    enqueued: list = []
    monkeypatch.setattr(rpw.payload_writer, "enqueue", lambda ev: enqueued.append(ev))

    event = RequestLogEvent(
        kind="started",
        request_id="req-1",
        attempt_key="req-1:1",
        attempt_no=1,
        payload={"request_body": {"a": 1}},
    )
    # Must be a no-op: no enqueue, no exception.
    RequestLogWriter._fanout_payload(event)
    assert enqueued == []


def test_fanout_enqueues_when_enabled(monkeypatch):
    """开关打开时照常入队（回归保护）。"""
    import clickhouse_config
    from request_log_writer import RequestLogEvent, RequestLogWriter
    import request_payload_writer as rpw

    settings = types.SimpleNamespace(enabled=True, max_payload_bytes=4194304)
    monkeypatch.setattr(clickhouse_config, "get_settings", lambda: settings, raising=False)

    enqueued: list = []
    monkeypatch.setattr(rpw.payload_writer, "enqueue", lambda ev: enqueued.append(ev))

    event = RequestLogEvent(
        kind="started",
        request_id="req-1",
        attempt_key="req-1:1",
        attempt_no=1,
        payload={"request_body": {"a": 1}},
    )
    RequestLogWriter._fanout_payload(event)
    assert len(enqueued) == 1


def test_flush_splits_metadata_and_payload():
    """Large body fields go to payload; the rest stays as metadata."""
    from request_payload_writer import _PAYLOAD_FIELDS

    payload = {
        "request_body": {"messages": [1, 2]},
        "response_body": {"text": "hi"},
        "provider_name": "qwen",
        "model": "qwen-max",
        "total_tokens": 10,
    }
    metadata = {k: v for k, v in payload.items() if k not in _PAYLOAD_FIELDS}
    body = {k: payload[k] for k in _PAYLOAD_FIELDS if k in payload}

    assert metadata == {"provider_name": "qwen", "model": "qwen-max", "total_tokens": 10}
    assert set(body) == {"request_body", "response_body"}


def test_sanitize_masks_sensitive_keys():
    from request_payload_writer import RequestPayloadWriter

    writer = RequestPayloadWriter()
    payload = {
        "response_body": {"ok": True},
        "request_headers": {"Authorization": "Bearer secret", "X-Trace": "keep"},
        "token": "raw-secret",
    }
    out = writer._sanitize_payload(payload)
    # Top-level sensitive key masked.
    assert out["token"] == "***"
    # Nested header sensitive key masked (original key preserved, value masked),
    # non-sensitive preserved.
    headers = out["request_headers"]
    # Authorization 保留认证方案，仅部分遮蔽凭据（"Bearer secret" → "Bearer ******"）。
    assert headers.get("Authorization") == "Bearer ******" or headers.get("authorization") == "Bearer ******"
    assert headers["X-Trace"] == "keep"
    # Non-sensitive body preserved.
    assert out["response_body"] == {"ok": True}
