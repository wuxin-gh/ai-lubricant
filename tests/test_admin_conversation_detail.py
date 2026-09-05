"""Manager-side single-conversation detail endpoints (/admin/{agent,chat}/conversations/{id}).

The manager conversations page lists agent/chat conversations through the
admin endpoints (admin token), but the *detail* previously only existed on the
user-side ``/agent/conversations/{id}`` route, which resolves the caller from
the C-side session cookie and filters by ``user_id`` — a manager logged in via
session would 404 on other users' conversations. These admin endpoints read
across users.

The handlers are called directly with the admin guard and the ClickHouse-backed
conversation store patched out, so the test needs neither HTTP nor ClickHouse.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def patched_admin(monkeypatch):
    """Bypass the admin token check for the handler under test."""
    import admin as admin_module

    async def _ok(_token=None):
        return True

    monkeypatch.setattr(admin_module, "_require_admin", _ok)
    return admin_module


def _patch_store(monkeypatch, conv, messages):
    from agent import conversation_store

    monkeypatch.setattr(conversation_store, "is_ready", lambda: True)

    async def _get_conversation(conv_id):
        return conv

    async def _get_messages(conv_id):
        return messages

    monkeypatch.setattr(conversation_store, "get_conversation", _get_conversation)
    monkeypatch.setattr(conversation_store, "get_messages", _get_messages)


@pytest.mark.asyncio
async def test_admin_agent_conversation_detail_reads_across_users(patched_admin, monkeypatch):
    """Another user's conversation is returned: admin mode does not filter by user_id."""
    conv = {"id": "c1", "title": "t", "status": "active", "kind": "agent", "user_id": "someone-else"}
    _patch_store(monkeypatch, conv, [{"id": 1, "role": "user", "content": "hi"}])

    result = await patched_admin.admin_get_agent_conversation("c1", token="Bearer x")

    assert result["conversation"]["id"] == "c1"
    assert result["conversation"]["user_id"] == "someone-else"
    assert len(result["messages"]) == 1


@pytest.mark.asyncio
async def test_admin_chat_conversation_detail_reads_across_users(patched_admin, monkeypatch):
    conv = {"id": "c2", "title": "t", "status": "active", "kind": "chat", "user_id": "someone-else"}
    _patch_store(monkeypatch, conv, [{"id": 1, "role": "assistant", "content": "yo"}])

    result = await patched_admin.admin_get_chat_conversation("c2", token="Bearer x")

    assert result["conversation"]["id"] == "c2"
    assert len(result["messages"]) == 1


@pytest.mark.asyncio
async def test_admin_conversation_detail_missing_returns_404(patched_admin, monkeypatch):
    from fastapi import HTTPException

    _patch_store(monkeypatch, None, [])

    with pytest.raises(HTTPException) as exc:
        await patched_admin.admin_get_agent_conversation("missing", token="Bearer x")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_admin_conversation_detail_soft_deleted_returns_404(patched_admin, monkeypatch):
    """Soft-deleted rows (status='deleted') must not surface in the manager UI."""
    from fastapi import HTTPException

    _patch_store(monkeypatch, {"id": "c3", "status": "deleted"}, [])

    with pytest.raises(HTTPException) as exc:
        await patched_admin.admin_get_chat_conversation("c3", token="Bearer x")
    assert exc.value.status_code == 404
