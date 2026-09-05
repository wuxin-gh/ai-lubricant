from __future__ import annotations

import base64
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import agent.api as agent_api
from agent import conversation_store as real_store


class _Store:
    conversations: dict[str, dict] = {}

    @classmethod
    async def get_conversation_owned(cls, caller, conv_id):
        conv = cls.conversations.get(conv_id)
        if not conv:
            return None
        if caller is None or str(conv.get("user_id")) == str(caller):
            return conv
        return None


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(agent_api.router)
    app.dependency_overrides[agent_api.get_agent_caller] = lambda: None
    return TestClient(app)


def _ready(monkeypatch):
    monkeypatch.setattr(real_store, "is_ready", lambda: True)
    monkeypatch.setattr(agent_api, "_require_ch", lambda: None)


def test_chat_attachment_writes_temp_and_returns_image_data_url(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _ready(monkeypatch)
    monkeypatch.setattr(real_store, "get_conversation_owned", _Store.get_conversation_owned)
    _Store.conversations = {
        "chat-1": {
            "id": "chat-1",
            "user_id": "u1",
            "agent_id": None,
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    png = b"\x89PNG\r\n\x1a\n"

    with _client() as client:
        response = client.post(
            "/agent/chat/conversations/chat-1/attachments?filename=../pic.png",
            content=png,
            headers={"content-type": "image/png"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["filename"].endswith("-pic.png")
    assert body["path"].startswith(".chat-attachments/chat-1/")
    assert body["data_url"] == "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    target = tmp_path / "agent" / "temp" / body["path"]
    assert target.read_bytes() == png
    assert target.resolve().is_relative_to((tmp_path / "agent" / "temp").resolve())


def test_agent_attachment_writes_configured_workspace(tmp_path, monkeypatch):
    import db

    monkeypatch.chdir(tmp_path)
    _ready(monkeypatch)
    monkeypatch.setattr(real_store, "get_conversation_owned", _Store.get_conversation_owned)
    workspace = tmp_path / "workspace"
    _Store.conversations = {
        "agent-1": {
            "id": "agent-1",
            "user_id": "u1",
            "agent_id": 7,
            "status": "active",
        },
    }

    async def get_agent(cls, agent_id):
        assert agent_id == 7
        return {"id": 7, "workspace_root": str(workspace)}

    monkeypatch.setattr(db.PostgresClient, "get_agent", classmethod(get_agent))

    with _client() as client:
        response = client.post(
            "/agent/conversations/agent-1/attachments?filename=notes.txt",
            content=b"hello",
            headers={"content-type": "text/plain"},
        )

    assert response.status_code == 200
    body = response.json()
    target = workspace / body["path"]
    assert target.read_text(encoding="utf-8") == "hello"
    assert target.resolve().is_relative_to(workspace.resolve())
