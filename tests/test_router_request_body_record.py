"""``record_router_request_body`` 的出站 body 归一回归测试。

签名类渠道（codearts / qoderwork）为了让签名算在精确字节上，把出站 body 定稿成
``bytes`` 再交给 aiohttp（``send_sse_request(..., data=<bytes>)``）。
``send_sse_request`` 会把 kwargs 里的 ``data`` 摘出来送进请求记录，因此 bytes 必须
在这里解回文本 —— 否则它会一路留在 ``router_request_body`` 里，直到
ClickHouse writer 的 ``json.dumps`` 抛
``TypeError: Object of type bytes is not JSON serializable``，
同批（最多 flush_size 条）无关请求的 payload 一起被丢弃。
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from providers.custom import CustomProvider


def _provider() -> CustomProvider:
    return CustomProvider(
        "user",
        "key",
        provider_name="test",
        base_url="https://example.test",
    )


def _record(body):
    """跑一遍 record_router_request_body，返回落进请求记录的值。"""
    seen = []
    asyncio.run(
        _provider().record_router_request_body(
            body, {"router_request_body_callback": seen.append}
        )
    )
    assert len(seen) == 1
    return seen[0]


def test_record_decodes_json_bytes_body():
    """codearts 形态：出站 bytes 是明文 JSON，解成 dict 供日志详情页展示。"""
    payload = {"model": "GLM-5.2", "stream": True, "messages": [{"role": "user", "content": "hi"}]}
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    assert _record({"data": raw}) == payload


def test_record_keeps_non_json_bytes_body_as_text():
    """qoderwork 形态：出站是自定义 base64 密文，落字符串（出站真就是这串）。"""
    recorded = _record({"data": b"vGc$xQ2lm9"})

    assert recorded == "vGc$xQ2lm9"


def test_record_bytes_body_is_json_serializable():
    """归一后的值必须能进 json.dumps —— ClickHouse writer 的硬前提。"""
    raw = json.dumps({"model": "GLM-5.2"}, separators=(",", ":")).encode("utf-8")

    for body in ({"data": raw}, {"data": b"\xff\xfe not json"}):
        json.dumps(_record(body), ensure_ascii=False, separators=(",", ":"))


def test_record_handles_bytearray_and_invalid_utf8():
    """bytearray 同样解包；非 UTF-8 字节按替换字符降级，不抛 UnicodeDecodeError。"""
    assert _record({"data": bytearray(b'{"model":"GLM-5.2"}')}) == {"model": "GLM-5.2"}
    assert _record({"data": b"\xff\xfe"}) == "��"


def test_record_str_body_behaviour_unchanged():
    """既有 str 出站体（eaichat 系列）行为不变：明文解 dict，非 JSON 留字符串。"""
    assert _record({"data": '{"model":"gpt-5"}'}) == {"model": "gpt-5"}
    assert _record({"data": "not-json"}) == "not-json"


def test_record_dict_body_passthrough():
    """常规渠道传 json=<dict>，摘出来是 {"json": {...}}，不该被当成 data 解包。"""
    body = {"json": {"model": "gpt-5"}}

    assert _record(body) == body


def test_record_multi_key_body_not_unwrapped():
    """同时带 data 与 params 时不拆包，但 bytes 仍要解码 —— 否则照样炸 json.dumps。"""
    recorded = _record({"data": b'{"a":1}', "params": {"beta": "true"}})

    assert recorded == {"data": '{"a":1}', "params": {"beta": "true"}}
    json.dumps(recorded, ensure_ascii=False, separators=(",", ":"))


def test_record_without_callback_is_noop():
    """没有回调时直接返回，不做任何解码工作。"""
    asyncio.run(_provider().record_router_request_body({"data": b"\xff"}, {}))
    asyncio.run(_provider().record_router_request_body({"data": b"\xff"}, None))
