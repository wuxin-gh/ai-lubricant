"""Codex 伪装出站请求的端到端断言：headers 与 body 逐字段与真实抓包对齐。

用真实抓包样本（codex-tui/0.146.0）作为入站请求，检查两条路径：
1. 伪装（入站非 codex）：headers/body 每请求现造身份，两处同源；
2. 直通（入站就是 codex）：沿用客户端自己的 session/turn，不另起会话。
"""
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from providers.custom import CustomProvider


# 用户提供的真实 codex-tui 抓包（headers 与 body 的关键字段）。
CAPTURED_SESSION = "01a0627a-3ef1-72f2-8900-38c31be2f087"
CAPTURED_TURN = "01a0627a-572b-7a13-ae99-dade6120a22c"
CAPTURED_INSTALLATION = "4de163e5-08f4-442c-9698-fc57a64ffbd7"
CAPTURED_TURN_METADATA = json.dumps({
    "installation_id": CAPTURED_INSTALLATION,
    "session_id": CAPTURED_SESSION,
    "thread_id": CAPTURED_SESSION,
    "turn_id": CAPTURED_TURN,
    "window_id": f"{CAPTURED_SESSION}:0",
    "request_kind": "turn",
    "thread_source": "user",
    "sandbox": "windows_elevated",
    "turn_started_at_unix_ms": 1788358580101,
}, ensure_ascii=False, separators=(",", ":"))


def _provider(preset: str = "codex-tui", protocol: str = "responses") -> CustomProvider:
    return CustomProvider(
        "u",
        "sk-test",
        protocol=protocol,
        base_url="https://example.invalid",
        client_preset=preset,
    )


@pytest.fixture(autouse=True)
def _no_simulated_defaults(monkeypatch):
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})


def test_spoofed_outbound_matches_captured_header_shape():
    """伪装一个非 codex 入站请求，出站 header 字段集与真实抓包一致。"""
    provider = _provider()
    kwargs = {"request_id": "req-shape"}

    headers = provider._headers("responses", kwargs=kwargs)

    # 抓包里 codex-tui 的这些头必须齐（值可变，形状不可缺）。
    for key in ("originator", "User-Agent", "x-codex-beta-features", "x-codex-window-id",
                "x-codex-turn-metadata", "x-client-request-id", "session-id", "thread-id",
                "accept", "accept-encoding"):
        assert headers.get(key), key

    assert headers["originator"] == "codex-tui"
    assert headers["x-codex-beta-features"] == "remote_compaction_v2"
    assert headers["accept"] == "text/event-stream"
    assert headers["accept-encoding"] == "identity"
    assert headers["Authorization"] == "Bearer sk-test"
    # 伪装绝不能残留 Desktop 的会话常量（旧实现全站共用同一 session/turn）。
    assert "019ea213-658d-78a1-97bb-cf61a69729d8" not in json.dumps(headers)
    assert "019ea2bd-a57f-7f11-9f4b-9001bfc8cc9c" not in json.dumps(headers)


def test_spoofed_turn_metadata_field_set_matches_capture():
    provider = _provider()

    metadata = json.loads(provider._headers("responses", kwargs={"request_id": "req-meta"})["x-codex-turn-metadata"])

    assert set(metadata) == set(json.loads(CAPTURED_TURN_METADATA))
    assert metadata["installation_id"] == CAPTURED_INSTALLATION
    assert metadata["request_kind"] == "turn"
    assert metadata["thread_source"] == "user"
    assert isinstance(metadata["turn_started_at_unix_ms"], int)
    # session/thread/turn 全是 uuid7（抓包同形），turn 与 session 不同值。
    assert uuid.UUID(metadata["session_id"]).version == 7
    assert uuid.UUID(metadata["turn_id"]).version == 7
    assert metadata["turn_id"] != metadata["session_id"]
    assert metadata["window_id"] == f"{metadata['session_id']}:0"


def test_spoofed_body_client_metadata_field_set_matches_capture():
    """出站 body 的 client_metadata 键集与真实抓包一致（含那串同值 turn-metadata）。"""
    provider = _provider()
    kwargs = {"request_id": "req-body"}

    payload = provider._build_protocol_payload(
        "responses", "gpt-5.5", [{"role": "user", "content": "hi"}], True, **kwargs,
    )
    headers = provider._headers("responses", kwargs=kwargs)

    assert set(payload["client_metadata"]) == {
        "session_id", "thread_id", "turn_id",
        "x-codex-installation-id", "x-codex-turn-metadata", "x-codex-window-id",
    }
    assert payload["client_metadata"]["x-codex-turn-metadata"] == headers["x-codex-turn-metadata"]
    assert payload["prompt_cache_key"] == headers["thread-id"]
    # Responses 形态的结构性字段（抓包同形）。
    assert payload["store"] is False
    assert payload["include"] == ["reasoning.encrypted_content"]
    assert payload["text"] == {"verbosity": "low"}
    assert payload["parallel_tool_calls"] is True
    assert payload["instructions"].startswith("You are Codex, a coding agent based on GPT-5.")


def test_two_requests_never_share_session_or_turn():
    """并发/连续请求各自独立会话，不再出现「全站同一 session_id」的可判别指纹。"""
    provider = _provider()

    ids = set()
    turns = set()
    for i in range(5):
        headers = provider._headers("responses", kwargs={"request_id": f"req-{i}"})
        ids.add(headers["session-id"])
        turns.add(json.loads(headers["x-codex-turn-metadata"])["turn_id"])

    assert len(ids) == 5
    assert len(turns) == 5


def test_inbound_codex_passthrough_keeps_client_identity():
    """真实 codex 入站（同协议直通）：出站沿用客户端 session/turn 与那串 turn-metadata。"""
    provider = _provider()
    raw_body = {
        "model": "gpt-5.5",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": True,
        "instructions": CustomProvider.CODEX_DEFAULT_INSTRUCTIONS,
        "client_metadata": {
            "session_id": CAPTURED_SESSION,
            "thread_id": CAPTURED_SESSION,
            "turn_id": CAPTURED_TURN,
            "x-codex-installation-id": CAPTURED_INSTALLATION,
            "x-codex-turn-metadata": CAPTURED_TURN_METADATA,
            "x-codex-window-id": f"{CAPTURED_SESSION}:0",
        },
    }
    kwargs = {"request_id": "req-passthrough-e2e", "_raw_responses_body": raw_body, "client_type": "codex-tui"}

    payload = provider._build_protocol_payload("responses", "gpt-5.5", [], True, **kwargs)
    headers = provider._headers("responses", kwargs=kwargs)

    assert headers["session-id"] == CAPTURED_SESSION
    assert headers["thread-id"] == CAPTURED_SESSION
    assert headers["x-codex-window-id"] == f"{CAPTURED_SESSION}:0"
    assert headers["x-codex-turn-metadata"] == CAPTURED_TURN_METADATA
    assert payload["client_metadata"] == raw_body["client_metadata"]
    # 直通不改客户端 instructions。
    assert payload["instructions"] == CustomProvider.CODEX_DEFAULT_INSTRUCTIONS


def test_inbound_codex_client_body_is_not_double_augmented():
    """入站是 codex 的另一构建（Desktop↔TUI）时视为同族直通，不再叠加一遍伪装默认值。"""
    provider = _provider()
    raw_body = {"model": "gpt-5.5", "input": "hi", "stream": True, "instructions": "custom client prompt"}

    payload = provider._build_protocol_payload(
        "responses", "gpt-5.5", [], True,
        _raw_responses_body=raw_body,
        client_type="codex-cli",   # 探测出 Desktop，渠道配 tui —— 同一客户端的两个构建
        request_id="req-same-family",
    )

    assert payload["instructions"] == "custom client prompt"
    assert "client_metadata" not in payload


def test_inbound_codex_openai_variant_is_not_treated_as_responses_family():
    """codex-openai 是另一种线格式，不与 responses 变体互相直通，仍按自身目标协议补齐。"""
    assert CustomProvider._client_matches_preset("codex-openai", "codex-tui") is False
    assert CustomProvider._client_matches_preset("codex-tui", "codex-openai") is False
    assert CustomProvider._client_matches_preset("codex-cli", "codex-tui") is True
    assert CustomProvider._client_matches_preset("codex-openai", "codex-openai") is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
