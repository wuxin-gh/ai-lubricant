"""会话历史游标分页：归属隔离、agent/node 过滤、稳定游标边界。"""
from __future__ import annotations

import base64
import json

import pytest

from agent import conversation_store


class _FakeClient:
    """记录最后一次查询的 SQL/params，并回放预置行。"""

    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.sql = ""
        self.params: dict = {}

    async def query(self, sql: str, params: dict | None = None):
        self.sql = sql
        self.params = params or {}
        limit = int(self.params.get("limit", len(self._rows)))
        return self._rows[:limit]


def _row(conv_id: str, updated_at: str) -> dict:
    return {
        "id": conv_id,
        "title": f"conv {conv_id}",
        "system_prompt": "",
        "model": "gpt-test",
        "llm_model_id": None,
        "status": "active",
        "kind": "agent",
        "agent_id": 7,
        "user_id": "u-1",
        "cdp_client_id": "",
        "chat_settings": "{}",
        "created_at": updated_at,
        "updated_at": updated_at,
    }


@pytest.fixture()
def fake_store(monkeypatch):
    def _install(rows: list[dict]) -> _FakeClient:
        client = _FakeClient(rows)
        monkeypatch.setattr(conversation_store, "_client", client, raising=False)
        monkeypatch.setattr(conversation_store, "_ready", True, raising=False)

        async def _noop_require() -> None:
            return None

        monkeypatch.setattr(conversation_store, "_require", _noop_require, raising=False)
        return client

    return _install


@pytest.mark.asyncio
async def test_paged_filters_by_owner_agent_and_node(fake_store):
    client = fake_store([_row("c1", "2026-08-16 10:00:00.000")])

    result = await conversation_store.list_conversations_for_caller_paged(
        "u-1", limit=10, kind="agent", agent_id=7, node_id="node-9",
    )

    assert client.params["uid"] == "u-1"
    assert client.params["aid"] == 7
    assert client.params["nid"] == "node-9"
    assert "user_id = %(uid)s" in client.sql
    assert "agent_id = %(aid)s" in client.sql
    assert "JSONExtractString(chat_settings, 'node_id') = %(nid)s" in client.sql
    assert "ORDER BY updated_at DESC, id DESC" in client.sql
    assert [item["id"] for item in result["items"]] == ["c1"]
    assert result["page"] == {"next_cursor": None, "has_next_page": False}


@pytest.mark.asyncio
async def test_admin_caller_is_not_owner_filtered(fake_store):
    client = fake_store([_row("c1", "2026-08-16 10:00:00.000")])

    await conversation_store.list_conversations_for_caller_paged(None, limit=5, kind="chat")

    assert "user_id" not in client.params
    assert client.params["kind"] == "chat"


@pytest.mark.asyncio
async def test_next_cursor_encodes_updated_at_and_id(fake_store):
    rows = [
        _row("c1", "2026-08-16 10:00:00.000"),
        _row("c2", "2026-08-16 09:00:00.000"),
        _row("c3", "2026-08-16 08:00:00.000"),
    ]
    fake_store(rows)

    result = await conversation_store.list_conversations_for_caller_paged("u-1", limit=2, kind="agent")

    assert [item["id"] for item in result["items"]] == ["c1", "c2"]
    assert result["page"]["has_next_page"] is True
    cursor = result["page"]["next_cursor"]
    raw = base64.urlsafe_b64decode(cursor.encode("ascii") + b"=" * (-len(cursor) % 4))
    decoded_time, decoded_id = json.loads(raw.decode("utf-8"))
    assert decoded_id == "c2"
    assert decoded_time.startswith("2026-08-16 09:00:00")


@pytest.mark.asyncio
async def test_cursor_applies_stable_tiebreaker(fake_store):
    client = fake_store([_row("c9", "2026-08-16 07:00:00.000")])
    cursor = base64.urlsafe_b64encode(
        json.dumps(["2026-08-16 09:00:00.000", "c2"], separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    await conversation_store.list_conversations_for_caller_paged("u-1", limit=2, kind="agent", cursor=cursor)

    assert client.params["cur_time"] == "2026-08-16 09:00:00.000"
    assert client.params["cur_id"] == "c2"
    assert "updated_at = parseDateTime64BestEffort(%(cur_time)s, 3) AND id < %(cur_id)s" in client.sql


@pytest.mark.asyncio
async def test_invalid_cursor_raises_value_error(fake_store):
    fake_store([])

    with pytest.raises(ValueError):
        await conversation_store.list_conversations_for_caller_paged("u-1", cursor="not-a-cursor")
