r"""PostgresClient._dumps 序列化回归测试。

复现线上请求日志写入失败：
`invalid input syntax for type json DETAIL: Escape sequence "\ " is invalid.`
这些测试不连接 PostgreSQL，只验证 _dumps 始终产出可被 json.loads 解析、
且 PostgreSQL jsonb 可接受的 JSON 文本。
"""
import json
from datetime import datetime

from db import PostgresClient


def _valid_json(text: str):
    """断言 _dumps 返回的是合法 JSON，且不含 PostgreSQL 拒绝的 NUL。"""
    assert isinstance(text, str)
    assert "\x00" not in text
    return json.loads(text)


def test_dumps_normal_dict():
    data = {"model": "opus", "stream": True, "n": 3}
    parsed = _valid_json(PostgresClient._dumps(data))
    assert parsed == data


def test_dumps_upstream_429_error_object():
    data = {
        "error": {
            "message": "daily usage limit exceeded",
            "type": "usage_limit_exceeded",
        }
    }
    parsed = _valid_json(PostgresClient._dumps(data))
    assert parsed == data


def test_dumps_backslash_space_in_string():
    r"""复现 PostgreSQL 报错中的非法 escape：'\ '（反斜杠+空格）。

    Python json.dumps 会正确转义反斜杠，所以作为字符串值写入 JSONB 是合法的。
    """
    data = {"error": "path C:\\ Program Files\\ something"}
    parsed = _valid_json(PostgresClient._dumps(data))
    assert parsed["error"] == "path C:\\ Program Files\\ something"


def test_dumps_strips_real_nul_in_string():
    data = {"msg": "hello\x00world"}
    parsed = _valid_json(PostgresClient._dumps(data))
    assert parsed["msg"] == "helloworld"
    assert "\x00" not in PostgresClient._dumps(data)


def test_dumps_strips_real_nul_nested():
    data = {
        "outer": ["a\x00b", {"inner": "x\x00y"}, ("t\x00",)]
    }
    parsed = _valid_json(PostgresClient._dumps(data))
    assert parsed == {"outer": ["ab", {"inner": "xy"}, ["t"]]}


def test_dumps_router_response_body_mixed_list():
    data = [
        {"id": "chunk-1", "delta": "C:\\ temp"},
        "raw upstream text with \\ trailing",
        "...[truncated]",
    ]
    parsed = _valid_json(PostgresClient._dumps(data))
    assert len(parsed) == 3
    assert parsed[0]["delta"] == "C:\\ temp"


def test_dumps_non_serializable_object_falls_back_to_str():
    data = {"ts": datetime(2026, 7, 1, 15, 21, 43)}
    parsed = _valid_json(PostgresClient._dumps(data))
    assert "2026-07-01" in parsed["ts"]


def test_dumps_raw_string_value():
    text = "daily usage limit exceeded"
    parsed = _valid_json(PostgresClient._dumps(text))
    assert parsed == text


def test_dumps_preserves_unicode():
    data = {"error": "配额超限：daily usage limit exceeded"}
    parsed = _valid_json(PostgresClient._dumps(data))
    assert parsed["error"] == "配额超限：daily usage limit exceeded"


def test_dumps_rejects_nonstandard_nan():
    """PostgreSQL jsonb 不接受 NaN/Infinity；_dumps 必须降级为合法 JSON。"""
    data = {"ratio": float("nan")}
    result = PostgresClient._dumps(data)
    # 不应出现 PostgreSQL 拒绝的非标准裸 token；降级成字符串是可接受的。
    parsed = _valid_json(result)
    assert parsed == str(data)


def test_dumps_fallback_when_value_unserializable():
    """包含无法序列化对象时，降级为字符串而非抛错，保证日志不丢。"""
    class Unserializable:
        def __str__(self):
            return "C:\\ broken\x00 object"

    data = {"upstream": Unserializable()}
    parsed = _valid_json(PostgresClient._dumps(data))
    assert "broken" in parsed["upstream"]
    assert "\x00" not in parsed["upstream"]
