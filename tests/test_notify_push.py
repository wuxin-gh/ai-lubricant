"""Mobile push channel: payload construction + Expo response classification.

Pure-function unit tests (no DB, no HTTP) for notify_push. The worker's
mobile_push delivery branch is exercised through a fake device row in
test_notify_core; this module covers the small surface that is pure logic.

* ``build_push_payload`` — title/body clipped, deep-link data scalarized,
  severity drives priority.
* ``classify_expo_response`` — success / per-ticket error / device-gone
  retirement / 5xx retryable / malformed body. Expo wraps a batch result as
  ``{"data": [ticket]}``; a self-hosted gateway returning a bare ticket is
  accepted too.
"""
from __future__ import annotations

import json

from user_platform.notify_push import build_push_payload, classify_expo_response


def _envelope(ticket: dict) -> str:
    """Expo batch shape: an array under ``data``."""
    return json.dumps({"data": [ticket]})


def test_build_payload_clips_and_scalarizes():
    payload = build_push_payload(
        "ExponentPushToken[abc]",
        title="任务「超长标题" + "x" * 200 + "」",
        body="内容" + "y" * 300,
        data={"task_id": "123", "route": "/task/123", "nested": {"a": 1}, "count": 3},
        severity="error",
    )
    assert payload["to"] == "ExponentPushToken[abc]"
    assert len(payload["title"]) <= 120
    assert len(payload["body"]) <= 240
    # data values must be JSON scalars (Expo rejects nested objects).
    assert payload["data"]["task_id"] == "123"
    assert payload["data"]["count"] == 3
    assert payload["data"]["nested"] == "{'a': 1}"  # str(...) — scalarized
    assert payload["priority"] == "high"
    assert payload["channelId"] == "task-events"
    assert payload["sound"] == "default"


def test_build_payload_info_severity_default_priority():
    payload = build_push_payload("t", title="", body="", severity="info")
    assert payload["priority"] == "default"
    assert payload["title"] == "通知"  # empty title → placeholder
    assert payload["body"] == ""


def test_classify_success():
    r = classify_expo_response(200, _envelope({"status": "ok", "id": "abc"}))
    assert r.ok is True
    assert r.device_gone is False


def test_classify_success_bare_ticket_form():
    # A self-hosted gateway returning a single ticket (no data[] wrapper).
    r = classify_expo_response(200, json.dumps({"status": "ok"}))
    assert r.ok is True


def test_classify_device_not_registered_retires_token():
    body = _envelope({
        "status": "error",
        "message": "The recipient device is no longer registered.",
        "details": {"error": "DeviceNotRegistered"},
    })
    r = classify_expo_response(200, body)
    assert r.ok is False
    assert r.device_gone is True
    assert "DeviceNotRegistered" in r.error


def test_classify_message_error_not_device_gone():
    body = _envelope({
        "status": "error",
        "message": "Message too big",
        "details": {"error": "MessageTooBig"},
    })
    r = classify_expo_response(200, body)
    assert r.ok is False
    assert r.device_gone is False


def test_classify_4xx_config_error_no_retire():
    r = classify_expo_response(400, json.dumps({"errors": [{"code": "INVALID", "message": "bad request"}]}))
    assert r.ok is False
    assert r.device_gone is False
    assert "HTTP 400" in r.error


def test_classify_5xx_retryable():
    r = classify_expo_response(503, "Service Unavailable")
    assert r.ok is False
    assert r.device_gone is False
    assert "HTTP 503" in r.error


def test_classify_429_retryable():
    r = classify_expo_response(429, "")
    assert r.ok is False
    assert r.device_gone is False


def test_classify_malformed_body_treated_as_failure():
    # A 200 with a non-JSON body has no per-ticket status → treated as failure
    # (missing/expired ticket is suspicious; safer to surface it than to claim
    # success and skip the device). device_gone stays False (not a retire).
    r = classify_expo_response(200, "not json")
    assert r.ok is False
    assert r.device_gone is False
