import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main


def test_update_token_stats_uses_total_tokens_when_provided():
    main._token_stats.clear()
    main.update_token_stats("key1", prompt_tokens=100, completion_tokens=30, total_tokens=150)
    assert main._token_stats["key1"]["total_tokens"] == 150


def test_update_token_stats_falls_back_to_prompt_plus_completion_when_total_is_zero():
    main._token_stats.clear()
    main.update_token_stats("key2", prompt_tokens=100, completion_tokens=30, total_tokens=0)
    assert main._token_stats["key2"]["total_tokens"] == 130


def test_update_token_stats_falls_back_to_prompt_plus_completion_when_total_is_none():
    main._token_stats.clear()
    main.update_token_stats("key3", prompt_tokens=100, completion_tokens=30, total_tokens=None)
    assert main._token_stats["key3"]["total_tokens"] == 130


def test_add_recent_log_uses_total_tokens_when_provided():
    main._recent_logs.clear()
    main._add_recent_log("key1", "m1", "/v1/chat", "ok", prompt_tokens=100, completion_tokens=30, total_tokens=150)
    assert main._recent_logs[-1]["total_tokens"] == 150


def test_add_recent_log_falls_back_to_prompt_plus_completion_when_total_is_zero():
    main._recent_logs.clear()
    main._add_recent_log("key2", "m1", "/v1/chat", "ok", prompt_tokens=100, completion_tokens=30, total_tokens=0)
    assert main._recent_logs[-1]["total_tokens"] == 130


def test_add_recent_log_falls_back_to_prompt_plus_completion_when_total_is_none():
    main._recent_logs.clear()
    main._add_recent_log("key3", "m1", "/v1/chat", "ok", prompt_tokens=100, completion_tokens=30, total_tokens=None)
    assert main._recent_logs[-1]["total_tokens"] == 130


def test_extract_client_session_context_passthroughs_explicit_headers_only():
    context = main._extract_client_session_context({
        "x-claude-code-session-id": " session_123 ",
        "x-client-request-id": " request_123 ",
    })
    assert context == {
        "client_session_id": "session_123",
        "client_session_source": "x-claude-code-session-id",
        "client_request_id": "request_123",
    }


def test_extract_client_session_context_does_not_generate_client_ids():
    assert main._extract_client_session_context({}) == {}


def test_extract_client_session_context_accepts_alternate_client_headers():
    assert main._extract_client_session_context({"session-id": "codex-session"}) == {
        "client_session_id": "codex-session",
        "client_session_source": "session-id",
    }
    assert main._extract_client_session_context({"x-session-affinity": "open-session", "x-request-id": "req-1"}) == {
        "client_session_id": "open-session",
        "client_session_source": "x-session-affinity",
        "client_request_id": "req-1",
    }


def test_model_output_modalities_prefers_runtime_model_info(monkeypatch):
    monkeypatch.setattr(main.ModelClientPool, "get_model_info", lambda model: {"output_modalities": ["text", "image"]})
    assert asyncio.run(main._model_output_modalities("m1")) == ["text", "image"]


def test_model_output_modalities_defaults_to_text(monkeypatch):
    monkeypatch.setattr(main.ModelClientPool, "get_model_info", lambda model: {})

    async def _empty_metadata(model):
        return ({}, True)

    monkeypatch.setattr(main.model_metadata, "get_model_metadata", _empty_metadata)
    assert asyncio.run(main._model_output_modalities("m1")) == ["text"]
