"""Agent/chat message pagination by user turns."""
from __future__ import annotations

import pytest

from agent import conversation_store


class _Client:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    async def query(self, sql: str, params: dict):
        self.calls.append((sql, params))
        if "SELECT id\n        FROM agent_messages" in sql:
            return [{"id": 30}, {"id": 20}, {"id": 10}]
        return [
            {"id": 20, "conversation_id": "c1", "role": "user", "content": "older", "tool_calls": "", "tool_results": "", "turn_number": 0, "status": "done", "error": None, "media": "", "model": "", "created_at": "2026-08-16 00:00:00.000"},
            {"id": 21, "conversation_id": "c1", "role": "assistant", "content": "reply", "tool_calls": "", "tool_results": "", "turn_number": 0, "status": "done", "error": None, "media": "", "model": "m", "created_at": "2026-08-16 00:00:01.000"},
            {"id": 30, "conversation_id": "c1", "role": "user", "content": "newer", "tool_calls": "", "tool_results": "", "turn_number": 0, "status": "done", "error": None, "media": "", "model": "", "created_at": "2026-08-16 00:00:02.000"},
        ]


@pytest.mark.asyncio
async def test_message_page_uses_user_turn_cursor(monkeypatch):
    client = _Client()
    monkeypatch.setattr(conversation_store, "_client", client)
    monkeypatch.setattr(conversation_store, "_ready", True)

    result = await conversation_store.get_messages_page_by_user_turns("c1", limit=2)

    assert [message["id"] for message in result["messages"]] == [20, 21, 30]
    assert result["page"] == {"next_cursor": 20, "has_more": True}
    assert "role = 'user'" in client.calls[0][0]
    assert "ORDER BY id DESC" in client.calls[0][0]


@pytest.mark.asyncio
async def test_message_page_applies_cursor_to_both_queries(monkeypatch):
    client = _Client()
    monkeypatch.setattr(conversation_store, "_client", client)
    monkeypatch.setattr(conversation_store, "_ready", True)

    await conversation_store.get_messages_page_by_user_turns("c1", limit=2, cursor=40)

    assert client.calls[0][1]["cursor"] == 40
    assert client.calls[1][1]["cursor"] == 40
    assert "id < %(cursor)s" in client.calls[0][0]
    assert "id < %(cursor)s" in client.calls[1][0]
