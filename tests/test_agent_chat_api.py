import os
import sys
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import agent.api as agent_api
import db
import model_catalog
from agent import conversation_store as _real_conversation_store


def make_client():
    app = FastAPI()
    app.include_router(agent_api.router)
    # 覆盖鉴权依赖为管理员模式（caller=None，不做 user_id 过滤），
    # 使这些持久化 CRUD 测试无需构造 session cookie / admin token。
    app.dependency_overrides[agent_api.get_agent_caller] = lambda: None
    return TestClient(app)


class FakeConversationStore:
    """内存版对话存储，镜像 conversation_store 的对话方法签名。"""
    conversations: dict[str, dict] = {}
    messages: dict[str, list[dict]] = {}
    counter: int = 0
    msg_counter: int = 0
    touched: list[str] = []

    @classmethod
    def reset(cls):
        cls.conversations = {}
        cls.messages = {}
        cls.counter = 0
        cls.msg_counter = 0
        cls.touched = []

    @classmethod
    def _now(cls):
        return datetime.now(timezone.utc).isoformat()

    @classmethod
    def is_ready(cls):
        return True

    @classmethod
    async def create_conversation(
        cls,
        title='新对话',
        system_prompt='',
        model='',
        agent_id=None,
        llm_model_id=None,
        kind='agent',
        chat_settings=None,
        user_id=None,
    ):
        cls.counter += 1
        conv_id = f"{kind}-{cls.counter}"
        now = cls._now()
        row = {
            "id": conv_id,
            "title": title,
            "system_prompt": system_prompt,
            "model": model,
            "agent_id": agent_id,
            "llm_model_id": llm_model_id,
            "status": "active",
            "kind": kind,
            "chat_settings": chat_settings,
            "user_id": user_id,
            "created_at": now,
            "updated_at": now,
        }
        cls.conversations[conv_id] = row
        cls.messages[conv_id] = []
        return dict(row)

    @classmethod
    async def list_conversations(cls, limit=50, kind='agent'):
        rows = [c for c in cls.conversations.values() if c.get("status") == "active" and c.get("kind") == kind]
        return [dict(r) for r in rows[:limit]]

    @classmethod
    async def list_conversations_for_caller(cls, user_id, limit=50, kind='agent'):
        # 测试用管理员模式（user_id=None）：等价 list_conversations，不做归属过滤。
        if user_id is None:
            return await cls.list_conversations(limit=limit, kind=kind)
        rows = [
            c for c in cls.conversations.values()
            if c.get("status") == "active" and c.get("kind") == kind
            and str(c.get("user_id") or "") == str(user_id)
        ]
        return [dict(r) for r in rows[:limit]]

    @classmethod
    async def list_conversations_admin(cls, *, kind, limit=20, cursor=None):
        rows = [
            c for c in cls.conversations.values()
            if c.get("status") == "active" and c.get("kind") == kind
        ]
        return {"conversations": [dict(r) for r in rows[:limit]], "page": {"cursor": None, "has_next_page": False}}

    @classmethod
    async def get_conversation(cls, conv_id):
        row = cls.conversations.get(conv_id)
        return dict(row) if row else None

    @classmethod
    async def get_conversation_owned(cls, user_id, conv_id):
        conv = await cls.get_conversation(conv_id)
        if conv is None:
            return None
        if user_id is None:
            return conv
        if str(conv.get("user_id") or "") != str(user_id):
            return None
        return conv

    @classmethod
    async def delete_conversation(cls, conv_id):
        existed = conv_id in cls.conversations
        cls.conversations.pop(conv_id, None)
        cls.messages.pop(conv_id, None)
        return existed

    @classmethod
    async def update_conversation(cls, conv_id, **fields):
        row = cls.conversations.get(conv_id)
        if not row:
            return {}
        row.update(fields)
        row["updated_at"] = cls._now()
        return dict(row)

    @classmethod
    async def add_message(cls, conversation_id, role, content='', tool_calls=None, tool_results=None, turn_number=0, status='done', error=None, media=None, model='', usage=None, reasoning=''):
        cls.msg_counter += 1
        msg = {
            "id": cls.msg_counter,
            "conversation_id": conversation_id,
            "role": role,
            "content": content,
            "tool_calls": tool_calls,
            "tool_results": tool_results,
            "turn_number": turn_number,
            "status": status,
            "error": error,
            "media": media,
            "model": model,
            "usage": usage,
            "reasoning": reasoning,
            "created_at": cls._now(),
        }
        cls.messages.setdefault(conversation_id, []).append(msg)
        return dict(msg)

    @classmethod
    async def get_messages(cls, conversation_id):
        return [dict(m) for m in cls.messages.get(conversation_id, [])]

    @classmethod
    async def update_message(cls, message_id, **fields):
        for msgs in cls.messages.values():
            for m in msgs:
                if m.get("id") == message_id:
                    m.update(fields)
                    return

    @classmethod
    async def touch_conversation(cls, conv_id):
        cls.touched.append(conv_id)
        if conv_id in cls.conversations:
            cls.conversations[conv_id]["updated_at"] = cls._now()


def install_fake(monkeypatch):
    FakeConversationStore.reset()
    # agent.api 通过 `from agent import conversation_store` 引用模块；替换模块级函数指向 fake 类方法。
    for _fn in (
        "create_conversation", "list_conversations", "list_conversations_for_caller",
        "list_conversations_admin", "get_conversation", "get_conversation_owned",
        "delete_conversation", "update_conversation", "add_message", "get_messages",
        "update_message", "touch_conversation", "is_ready",
    ):
        monkeypatch.setattr(_real_conversation_store, _fn, getattr(FakeConversationStore, _fn))


def test_chat_models_excludes_internal_scheme_nodes_and_keeps_key_filtering(monkeypatch):
    from rate_limiter import ModelClientPool
    import config

    derived_id = f"opus{model_catalog.SCHEME_GROUP_SEP}1"

    async def resolve_key(api_key_id, caller):
        assert api_key_id == 7
        assert caller is None
        return {"key": "sk-chat"}

    async def ensure_models_fresh(cls):
        return None

    async def allows_model(cls, api_key, model):
        assert api_key == "sk-chat"
        return model != "blocked-model"

    monkeypatch.setattr(agent_api, "_resolve_caller_api_key", resolve_key)
    monkeypatch.setattr(ModelClientPool, "_ensure_models_fresh", classmethod(ensure_models_fresh))
    monkeypatch.setattr(ModelClientPool, "_models", [
        {"id": "real-model", "object": "model", "max_context_tokens": 1000},
        {"id": "public-group", "object": "model", "type": "model_group", "models": ["real-model"]},
        {"id": "blocked-model", "object": "model"},
        {"id": derived_id, "object": "model", "type": "model_group", "models": ["real-model"]},
    ])
    # 目录过滤已收口到 get_models_response：除 Key 名单外还要过「当前可供给」，
    # 所以可供给集合必须显式给出，否则一条都不返回。
    monkeypatch.setattr(
        ModelClientPool, "available_model_ids",
        classmethod(lambda cls: {"real-model", "blocked-model"}),
    )
    monkeypatch.setattr(config.Config, "api_key_allows_model", classmethod(allows_model))

    with make_client() as client:
        response = client.get("/agent/chat/models?api_key_id=7")

    assert response.status_code == 200
    body = response.json()
    assert body["object"] == "list"
    by_id = {item["id"]: item for item in body["data"]}
    assert set(by_id) == {"real-model", "public-group"}
    assert by_id["real-model"]["description"] == "1K 上下文"
    assert by_id["public-group"]["description"] == "自定义模型组"
    assert all(model_catalog.SCHEME_GROUP_SEP not in model_id for model_id in by_id)


def test_chat_conversation_crud_and_message_append(monkeypatch):
    install_fake(monkeypatch)
    with make_client() as client:
        create = client.post("/agent/chat/conversations", json={
            "system_prompt": "be concise",
            "model": "test-model",
            "chat_settings": {
                "model": "test-model",
                "apiKey": "sk-test",
                "protocol": "openai",
                "temperature": 0.7,
                "maxTokens": 1024,
                "stream": True,
                "systemPrompt": "be concise",
            },
        })
        assert create.status_code == 200
        conv = create.json()
        assert conv["kind"] == "chat"
        assert conv["chat_settings"]["protocol"] == "openai"

        user = client.post(f"/agent/chat/conversations/{conv['id']}/messages", json={
            "role": "user",
            "content": "你好，这是第一条消息，应该成为标题",
            "status": "done",
        })
        assert user.status_code == 200
        assistant = client.post(f"/agent/chat/conversations/{conv['id']}/messages", json={
            "role": "assistant",
            "content": "你好",
            "status": "done",
        })
        assert assistant.status_code == 200
        assert FakeConversationStore.touched == [conv["id"], conv["id"]]

        detail = client.get(f"/agent/chat/conversations/{conv['id']}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["conversation"]["title"].startswith("你好，这是第一条消息")
        assert [m["role"] for m in body["messages"]] == ["user", "assistant"]


def test_chat_list_isolated_from_agent_conversations(monkeypatch):
    install_fake(monkeypatch)
    with make_client() as client:
        chat = client.post("/agent/chat/conversations", json={"model": "chat-model"}).json()
        agent = client.post("/agent/conversations", json={"model": "agent-model"}).json()

        chat_list = client.get("/agent/chat/conversations").json()
        agent_list = client.get("/agent/conversations").json()

        assert [c["id"] for c in chat_list] == [chat["id"]]
        assert [c["id"] for c in agent_list] == [agent["id"]]

        assert client.get(f"/agent/chat/conversations/{agent['id']}").status_code == 404


def test_chat_patch_settings_and_delete(monkeypatch):
    install_fake(monkeypatch)
    with make_client() as client:
        conv = client.post("/agent/chat/conversations", json={"model": "old"}).json()

        patch = client.patch(f"/agent/chat/conversations/{conv['id']}", json={
            "model": "new",
            "chat_settings": {
                "model": "new",
                "apiKey": "",
                "protocol": "responses",
                "temperature": 1,
                "maxTokens": 2048,
                "stream": False,
                "systemPrompt": "updated",
            },
        })
        assert patch.status_code == 200
        assert patch.json()["model"] == "new"
        assert patch.json()["chat_settings"]["protocol"] == "responses"

        delete = client.delete(f"/agent/chat/conversations/{conv['id']}")
        assert delete.status_code == 200
        assert delete.json() == {"deleted": True}
        assert client.get(f"/agent/chat/conversations/{conv['id']}").status_code == 404


def test_chat_append_message_persists_media(monkeypatch):
    install_fake(monkeypatch)
    with make_client() as client:
        conv = client.post("/agent/chat/conversations", json={"model": "img-model"}).json()
        media = [{"type": "image", "url": "https://example.com/a.png", "mimeType": "image/png"}]
        msg = client.post(f"/agent/chat/conversations/{conv['id']}/messages", json={
            "role": "assistant",
            "content": "已生成 1 张图片",
            "status": "done",
            "media": media,
        }).json()
        assert msg["media"] == media

        detail = client.get(f"/agent/chat/conversations/{conv['id']}").json()
        assert detail["messages"][-1]["media"] == media


def test_chat_append_message_persists_model(monkeypatch):
    install_fake(monkeypatch)
    with make_client() as client:
        conv = client.post("/agent/chat/conversations", json={"model": "img-model"}).json()
        msg = client.post(f"/agent/chat/conversations/{conv['id']}/messages", json={
            "role": "assistant",
            "content": "回复",
            "status": "done",
            "model": "gpt-4o-mini",
        }).json()
        assert msg["model"] == "gpt-4o-mini"

        detail = client.get(f"/agent/chat/conversations/{conv['id']}").json()
        assert detail["messages"][-1]["model"] == "gpt-4o-mini"

