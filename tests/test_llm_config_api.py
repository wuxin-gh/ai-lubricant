import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from fastapi import FastAPI
from fastapi.testclient import TestClient

import agent.llm_config_api as lca


def _make_app(monkeypatch) -> FastAPI:
    app = FastAPI()
    app.include_router(lca.router)
    # 鉴权放行
    monkeypatch.setattr(lca, "require_admin", AsyncMock(return_value=None))
    # 模拟 DB pool 存在
    monkeypatch.setattr(lca.PostgresClient, "pool", MagicMock())
    return app


def _row(**overrides) -> dict:
    r = {
        "id": 1,
        "name": "my-llm",
        "base_url": "https://example.com/v1",
        "chat_path": "/chat/completions",
        "api_key": "sk-secret",
        "models": ["gpt-4o"],
        "protocol": "openai",
        "enabled": True,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    r.update(overrides)
    return r


def test_list_llm_configs_masks_api_key(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(lca.PostgresClient, "list_agent_llm_configs", AsyncMock(return_value=[_row()]))
    client = TestClient(app)

    resp = client.get("/agent/llm-configs")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    item = data[0]
    assert item["name"] == "my-llm"
    assert item["api_key_masked"] == "sk-sec***"
    assert item["has_api_key"] is True
    assert "api_key" not in item  # 明文不回传


def test_create_llm_config_calls_db_and_masks(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(
        lca.PostgresClient,
        "create_agent_llm_config",
        AsyncMock(return_value=_row(name="new", api_key="sk-1234567890")),
    )
    monkeypatch.setattr(lca, "_sync_config_models", AsyncMock(return_value=None))
    client = TestClient(app)

    resp = client.post(
        "/agent/llm-configs",
        json={"name": "new", "base_url": "https://x.com/v1", "api_key": "sk-1234567890", "models": ["m1"]},
    )
    assert resp.status_code == 200
    item = resp.json()
    assert item["name"] == "new"
    assert item["has_api_key"] is True
    assert "api_key" not in item
    lca.PostgresClient.create_agent_llm_config.assert_awaited_once()
    call_kwargs = lca.PostgresClient.create_agent_llm_config.await_args.kwargs
    assert call_kwargs["name"] == "new"
    assert call_kwargs["models"] == ["m1"]
    assert call_kwargs["timeout_seconds"] == 600
    assert call_kwargs["max_retries"] == 2


def test_create_llm_config_duplicate_name_returns_409(monkeypatch):
    app = _make_app(monkeypatch)

    async def raise_dup(**kwargs):
        raise Exception("unique constraint violation")

    monkeypatch.setattr(lca.PostgresClient, "create_agent_llm_config", raise_dup)
    client = TestClient(app)

    resp = client.post(
        "/agent/llm-configs",
        json={"name": "dup", "base_url": "https://x.com/v1", "api_key": "sk-x"},
    )
    assert resp.status_code == 409


def test_toggle_llm_config_flips_enabled(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(lca.PostgresClient, "get_agent_llm_config", AsyncMock(return_value=_row(enabled=True)))
    monkeypatch.setattr(
        lca.PostgresClient,
        "update_agent_llm_config",
        AsyncMock(return_value=_row(enabled=False)),
    )
    client = TestClient(app)

    resp = client.post("/agent/llm-configs/1/toggle")
    assert resp.status_code == 200
    assert resp.json() == {"id": 1, "enabled": False}
    lca.PostgresClient.update_agent_llm_config.assert_awaited_once_with(1, enabled=False)


def test_delete_llm_config(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(lca.PostgresClient, "delete_agent_llm_config", AsyncMock(return_value=True))
    client = TestClient(app)

    resp = client.delete("/agent/llm-configs/1")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_get_llm_config_404(monkeypatch):
    app = _make_app(monkeypatch)
    monkeypatch.setattr(lca.PostgresClient, "get_agent_llm_config", AsyncMock(return_value=None))
    client = TestClient(app)

    resp = client.get("/agent/llm-configs/99")
    assert resp.status_code == 404
