"""QoderWork spec 本地契约与协议 smoke tests。

specs/ 是可复制的本地源料目录，在干净部署中可能不存在；这种情况下跳过，
避免源料未粘贴时影响其它项目测试。
"""

import asyncio
import base64
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest


SPEC_PATH = Path(__file__).resolve().parents[1] / "specs" / "qoderwork.py"
if not SPEC_PATH.exists():
    pytest.skip("local QoderWork spec source is not present", allow_module_level=True)

_spec = importlib.util.spec_from_file_location("qoderwork_spec_under_test", SPEC_PATH)
assert _spec and _spec.loader
qoderwork = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(qoderwork)


def test_qoder_encoding_round_trips_upstream_vectors():
    cases = [
        b"",
        b"a",
        b"hi",
        b"hello world",
        b'{"model":"qmodel_preview","stream":true}',
        bytes(range(16)),
        "中文 UTF-8 内容 ✓".encode(),
    ]
    for raw in cases:
        encoded = qoderwork._qoder_encode(raw)
        assert qoderwork._qoder_decode(encoded) == raw
        assert "=" not in encoded
        assert all(char in qoderwork.QODER_B64_ALPHABET or char == "$" for char in encoded)


def test_qoder_encoding_known_vectors():
    assert qoderwork._qoder_encode(b"hi") == "$HzP"
    assert qoderwork._qoder_encode(bytes(range(16))) == "R_wlRf$$d&NBopnG__To_fpg"


def test_begin_device_flow_builds_qoder_pkce_url_without_network():
    result = asyncio.run(qoderwork.QoderWorkChannel.begin_device_flow(SimpleNamespace()))
    assert result["task_type"] == "device_code"
    assert result["interval"] >= 1
    assert result["poll_params"]["nonce"]
    assert len(result["poll_params"]["verifier"]) == 64

    query = parse_qs(urlsplit(result["auth_url"]).query)
    assert query["challenge_method"] == ["S256"]
    assert query["client_id"] == [qoderwork.QODER_CLIENT_ID]
    assert query["redirect_uri"] == [qoderwork.QODER_REDIRECT_URI]
    assert len(query["challenge"][0]) == 43
    assert query["nonce"] == [result["poll_params"]["nonce"]]


class _CosyProvider:
    access_token = "dt-test"
    refresh_token = "drt-test"
    password = "drt-test"
    uid = "uid-test"
    nickname = "Qoder Tester"
    machine_id = "machine-test"
    machine_token = "machine-token-test"
    machine_type = "machine-type-test"


def test_cosy_headers_have_expected_signature_shape_and_fields():
    provider = _CosyProvider()
    url = "https://gateway.qoder.com.cn/algo/api/v2/model/list?Encode=1"
    headers = qoderwork._cosy_headers(provider, body="", url=url, sse=False)

    assert headers["authorization"].startswith("Bearer COSY.")
    payload_b64, signature = headers["authorization"][len("Bearer COSY."):].rsplit(".", 1)
    payload = json.loads(base64.b64decode(payload_b64))
    assert payload["cosyVersion"] == qoderwork.COSY_VERSION
    assert len(signature) == 32
    assert headers["cosy-user"] == provider.uid
    assert headers["accept-encoding"] == "identity"
    assert "cache-control" not in headers

    chat_headers = qoderwork._cosy_headers(
        provider,
        body="encoded-body",
        url="https://gateway.qoder.com.cn/algo/chat",
        sse=True,
        model_key="qmodel_preview",
    )
    assert chat_headers["cache-control"] == "no-cache"
    assert chat_headers["x-model-key"] == "qmodel_preview"
    assert chat_headers["x-model-source"] == "system"


def test_agent_body_preserves_messages_tools_and_latest_prompt():
    messages = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "最后一条问题"},
    ]
    tools = [{"type": "function", "function": {"name": "lookup"}}]
    body = qoderwork._build_agent_body("qmodel_preview", messages, tools)
    assert body["messages"] == messages
    assert body["tools"] == tools
    assert body["chat_context"]["text"]["text"] == "最后一条问题"
    assert body["model_config"] == {"key": "qmodel_preview", "is_reasoning": False}
    assert body["business"]["begin_at"] > 0


def test_nested_sse_emits_reasoning_tools_usage_finish_and_done():
    inner = {
        "choices": [{
            "delta": {
                "content": "hello",
                "reasoning_content": "thinking",
                "tool_calls": [{"index": 0, "id": "call-1", "type": "function"}],
            },
            "finish_reason": "tool_calls",
        }],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2},
    }
    event = "data:" + json.dumps({"body": json.dumps(inner)}) + "\n\n"
    frames = qoderwork._iter_nested_chunk_frames(event)
    assert {"content": "hello", "thinking": "thinking", "tool_calls": inner["choices"][0]["delta"]["tool_calls"]} in frames
    assert {"usage": inner["usage"]} in frames
    assert {"finish_reason": "tool_calls"} in frames

    done = 'data:' + json.dumps({"body": "[DONE]"}) + "\n\n"
    assert {"done": True} in qoderwork._iter_nested_chunk_frames(done)


def test_model_name_normalization_and_static_mapping():
    assert qoderwork._normalize_model_name(" Qwen3.8_Max--Preview ") == "qwen3.8-max-preview"
    assert qoderwork._resolve_model_key("qwen3.7-max") == "qmodel_latest"
    qoderwork._DYNAMIC_MODEL_MAP["custom-model"] = "upstream-custom"
    assert qoderwork._resolve_model_key("CUSTOM-MODEL") == "upstream-custom"


def test_qoder_channel_declares_login_refresh_and_readonly_status():
    channel = qoderwork.QoderWorkChannel
    schema = channel.account_schema()
    assert channel.SUPPORTS_TOKEN_AUTO_REFRESH is True
    assert channel.SCHEDULED_REFRESH is True
    assert channel.APPLY_CLIENT_PRESET is False
    assert schema["auth_start"] == {
        "enabled": True,
        "label": "网页登录授权",
        "mode": "device_code",
        "description": "打开 Qoder 授权页完成登录，服务端自动轮询并回填 dt-/drt-。",
    }
    fields = {field["key"]: field for field in schema["fields"]}
    assert fields["quota_remaining"]["readonly"] is True
    assert fields["last_checkin"]["readonly"] is True
    assert "machine_token" in channel.ACCOUNT_FIELDS
