"""Key-per-session reversal: gateway resolves session key → editor, and a
session key may only touch its own session (no horizontal access to sibling
threads under the same editor)."""
from __future__ import annotations

import pytest
from fastapi import HTTPException

import main


EDITOR = {"id": "ed_claude", "provider": "claude", "status": "active", "api_key_id": None}
SESSION_KEY = {
    "id": 88,
    "name": "session:es_1",
    "scope": "editor",
    "disabled": False,
    "expires_at": None,
    "version": 1,
    "parent_id": 5,
}


def claude_headers(thread_id: str = "thread-x") -> dict:
    return {"x-claude-code-session-id": thread_id}


def _install(monkeypatch, *, session, api_key=SESSION_KEY):
    async def get_api_key_config(_key, include_disabled=False):
        return api_key

    async def get_editor_by_session_api_key_id(api_key_id):
        assert api_key_id == api_key["id"]
        return {**EDITOR, "session_id": session["id"]} if session else None

    async def get_editor_by_api_key_id(api_key_id):
        return None

    async def get_editor_session(editor_id, session_id):
        assert editor_id == EDITOR["id"]
        return session if session and session["id"] == session_id else None

    async def get_editor_session_by_thread(editor_id, thread_id):
        # Session-key path must never fall through to a thread lookup.
        raise AssertionError("session key must resolve via its own session")

    async def get_api_key_by_id(api_key_id):
        assert api_key_id == 5
        return {"id": 5, "disabled": False, "expires_at": None, "usage_limit": {}}

    monkeypatch.setattr(main.config.Config, "get_api_key_config", get_api_key_config)
    monkeypatch.setattr(main.PostgresClient, "get_editor_by_session_api_key_id", get_editor_by_session_api_key_id)
    monkeypatch.setattr(main.PostgresClient, "get_editor_by_api_key_id", get_editor_by_api_key_id)
    monkeypatch.setattr(main.PostgresClient, "get_editor_session", get_editor_session)
    monkeypatch.setattr(main.PostgresClient, "get_editor_session_by_thread", get_editor_session_by_thread)
    # Parent-key lookup resolves from the in-memory snapshot, not the DB: it sits
    # on every request path, so Config.get_api_key_by_id is the real call site.
    monkeypatch.setattr(main.config.Config, "get_api_key_by_id", get_api_key_by_id)


@pytest.mark.asyncio
async def test_active_session_key_resolves_to_its_session(monkeypatch):
    active = {
        "id": "es_1",
        "editor_id": EDITOR["id"],
        "status": "active",
        "provider_thread_id": "thread-x",
    }
    _install(monkeypatch, session=active)

    ctx = await main._validate_editor_request_context("sk-sess", claude_headers(), {"messages": []})

    assert ctx["first_request"] is False
    assert ctx["session"]["id"] == "es_1"
    assert ctx["api_key_id"] == 88
    assert ctx["api_key_parent_id"] == 5


@pytest.mark.asyncio
async def test_pending_session_key_binds_on_first_request_for_non_codex(monkeypatch):
    pending = {
        "id": "es_1",
        "editor_id": EDITOR["id"],
        "status": "pending_first_request",
        "provider_thread_id": None,
    }
    _install(monkeypatch, session=pending)

    ctx = await main._validate_editor_request_context("sk-sess", claude_headers(), {"messages": []})

    assert ctx["first_request"] is True
    assert ctx["session"]["id"] == "es_1"


@pytest.mark.asyncio
async def test_disabled_session_key_rejected(monkeypatch):
    disabled = {**SESSION_KEY, "disabled": True}
    _install(monkeypatch, session=None, api_key=disabled)

    with pytest.raises(HTTPException, match="Editor API Key 已停用") as exc:
        await main._validate_editor_request_context("sk-sess", claude_headers(), {"messages": []})

    assert exc.value.status_code == 403
