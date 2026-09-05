import asyncio
import re

from db import PostgresClient
from request_log_writer import AttemptWriteState, RequestLogWriter


def test_request_log_insert_sql_matches_parameter_count():
    sql = PostgresClient._REQUEST_LOG_INSERT_SQL
    columns_text = re.search(r"request_logs\s*\((.*?)\)\s*VALUES", sql, re.DOTALL).group(1)
    columns = [column.strip() for column in columns_text.split(",")]
    placeholders = [int(value) for value in re.findall(r"\$(\d+)", sql)]
    params = PostgresClient._request_log_insert_params(
        {"request_id": "req-1", "attempt_key": "attempt-1"}
    )

    assert len(columns) == len(params)
    assert placeholders == list(range(1, len(params) + 1))


def test_request_log_writer_calls_finalized_hook_after_db_update(monkeypatch):
    writer = RequestLogWriter(flush_size=10)
    seen = []

    async def _insert(cls, rows):
        return [row["attempt_key"] for row in rows]

    async def _finalize(cls, rows):
        return [row["attempt_key"] for row in rows]

    async def _on_finalized(payload):
        seen.append(dict(payload))

    monkeypatch.setattr(PostgresClient, "insert_request_log_batch", classmethod(_insert))
    monkeypatch.setattr(PostgresClient, "finalize_request_log_batch", classmethod(_finalize))
    writer.set_on_finalized(_on_finalized)
    writer._states["attempt-1"] = AttemptWriteState(
        request_id="req-1",
        attempt_key="attempt-1",
        attempt_no=1,
        started_payload={"attempt_key": "attempt-1"},
        final_payload={
            "attempt_key": "attempt-1", "success": False,
            "status": "cancelled", "completion_tokens": 7,
        },
    )

    asyncio.run(writer._flush())

    assert seen == [{
        "attempt_key": "attempt-1", "success": False,
        "status": "cancelled", "completion_tokens": 7,
    }]
    assert "attempt-1" not in writer._states
    assert writer.status()["finals_updated"] == 1


def test_request_log_writer_hook_failure_does_not_requeue_finalized_row(monkeypatch):
    writer = RequestLogWriter(flush_size=10)

    async def _insert(cls, rows):
        return [row["attempt_key"] for row in rows]

    async def _finalize(cls, rows):
        return [row["attempt_key"] for row in rows]

    async def _on_finalized(_payload):
        raise RuntimeError("billing unavailable")

    monkeypatch.setattr(PostgresClient, "insert_request_log_batch", classmethod(_insert))
    monkeypatch.setattr(PostgresClient, "finalize_request_log_batch", classmethod(_finalize))
    writer.set_on_finalized(_on_finalized)
    writer._states["attempt-1"] = AttemptWriteState(
        request_id="req-1",
        attempt_key="attempt-1",
        attempt_no=1,
        started_payload={"attempt_key": "attempt-1"},
        final_payload={"attempt_key": "attempt-1", "status": "cancelled"},
    )

    asyncio.run(writer._flush())

    assert "attempt-1" not in writer._states
    assert writer.status()["finals_updated"] == 1


def test_payload_writer_masks_protocol_auth_headers_but_keeps_body_token_counts():
    """header 字段按子串判定覆盖各协议认证头；body 字段仍精确匹配，不误伤用量。"""
    from request_payload_writer import RequestPayloadWriter

    sanitized = RequestPayloadWriter()._sanitize_payload(
        {
            "router_request_headers": {
                "x-goog-api-key": "AIzaSyD-1234567890abcdefghijklmn",
                "x-api-key": "sk-ant-api03-QWERTYuiop1234567890asdfgh",
                "Authorization": "Bearer sk-proj-QWERTYuiop1234567890asdfgh",
                "cookie": "session=abcdefghijklmnop",
                "x-stainless-os": "Windows",
            },
            "router_request_body": {"model": "gpt-5", "max_tokens": 64000, "prompt_tokens": 1234},
        }
    )

    headers = sanitized["router_request_headers"]
    # gemini 的认证头名字不在任何穷举列表里，靠子串判定才能命中。
    assert headers["x-goog-api-key"] == "AIza...klmn"
    assert headers["x-api-key"] == "sk-a...dfgh"
    assert headers["Authorization"] == "Bearer sk-p...dfgh"
    assert headers["cookie"] == "***"
    assert headers["x-stainless-os"] == "Windows"

    # body 里含 "token" 的字段是用量数字，不能被当成凭据遮掉。
    assert sanitized["router_request_body"]["max_tokens"] == 64000
    assert sanitized["router_request_body"]["prompt_tokens"] == 1234


def test_payload_writer_keeps_oversized_payload_untruncated():
    """payload 不做体积截断，超限也原样写库（调试场景需要完整请求/响应体）。"""
    import asyncio
    from request_payload_writer import PayloadEvent, RequestPayloadWriter

    writer = RequestPayloadWriter()
    inserted = []

    class _Client:
        async def insert_events(self, events):
            inserted.extend(events)

    writer._client = _Client()
    asyncio.run(writer._flush_batch([
        PayloadEvent(
            kind="finalized", request_id="req-big", attempt_key="attempt-big", attempt_no=1,
            payload={"request_body": {"messages": ["x" * 5000]}, "response_body": {"text": "ok"}},
        ),
    ]))

    assert len(inserted) == 1
    assert inserted[0].payload["request_body"] == {"messages": ["x" * 5000]}
    assert inserted[0].payload["response_body"] == {"text": "ok"}


def test_payload_writer_coerces_bytes_instead_of_dropping_batch():
    """payload 里混入 bytes 时整批仍能写出，坏值只退化成字符串。

    回归：签名类渠道（codearts/qoderwork）出站 body 是 bytes，曾让 _flush_batch
    序列化（json.dumps）抛 TypeError，同批最多 flush_size 条无关请求的 payload 一起被丢。
    """
    import asyncio
    from request_payload_writer import PayloadEvent, RequestPayloadWriter

    writer = RequestPayloadWriter()
    inserted = []

    class _Client:
        async def insert_events(self, events):
            inserted.extend(events)

    writer._client = _Client()
    asyncio.run(writer._flush_batch([
        PayloadEvent(
            kind="finalized", request_id="req-bytes", attempt_key="attempt-bytes", attempt_no=1,
            payload={"router_request_body": {"data": b'{"model":"GLM-5.2"}'}},
        ),
        PayloadEvent(
            kind="finalized", request_id="req-clean", attempt_key="attempt-clean", attempt_no=1,
            payload={"request_body": {"model": "gpt-5"}},
        ),
    ]))

    # 同批的无关请求不再被连带丢弃。
    assert [event.attempt_key for event in inserted] == ["attempt-bytes", "attempt-clean"]
    assert inserted[0].payload["router_request_body"]["data"] == '{"model":"GLM-5.2"}'
    assert inserted[1].payload["request_body"] == {"model": "gpt-5"}


def test_payload_writer_coerce_json_safe_handles_nested_and_exotic_values():
    """归一覆盖嵌套容器；无法原生序列化的对象退化成 repr，不抛异常。"""
    import json
    from datetime import datetime

    from request_payload_writer import RequestPayloadWriter

    coerced = RequestPayloadWriter._coerce_json_safe({
        "nested": {"raw": bytearray(b"ok"), "list": [b"a", ("b", 1), None, True, 1.5]},
        "when": datetime(2026, 9, 3, 10, 39, 16),
        "kept": "text",
    })

    assert coerced["nested"]["raw"] == "ok"
    assert coerced["nested"]["list"] == ["a", ["b", 1], None, True, 1.5]
    assert coerced["kept"] == "text"
    assert isinstance(coerced["when"], str)
    # 归一后的树必须能无损序列化 —— 这是入库写库共同的前提。
    json.dumps(coerced, ensure_ascii=False, separators=(",", ":"))


def test_payload_writer_coerce_json_safe_replaces_invalid_utf8():
    """非 UTF-8 字节不抛 UnicodeDecodeError，按替换字符降级。"""
    from request_payload_writer import RequestPayloadWriter

    assert RequestPayloadWriter._coerce_json_safe(b"\xff\xfe") == "��"
