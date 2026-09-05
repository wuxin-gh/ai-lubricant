from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import main


# ── helpers ──────────────────────────────────────────────────────────────────


def make_api_key(scope: str = "task", **over) -> dict:
    base = {
        "id": 71,
        "name": "task-key",
        "scope": scope,
        "disabled": False,
        "expires_at": None,
        "version": 1,
        "parent_id": 7,
    }
    base.update(over)
    return base


def make_task(**over) -> dict:
    base = {
        "id": "00000000-0000-0000-0000-00000000000a",
        "user_id": "00000000-0000-0000-0000-000000000001",
        "provider": "codex",
        "api_key_id": 71,
        "parent_api_key_id": 7,
        "provider_thread_id": None,
        "expected_client_id": "install-1",
        "bootstrap_content_hash": hashlib.sha256(b"hello").hexdigest(),
        "first_request_seen": False,
        "bootstrap_consumed": False,
        "status": "pending",
        "deleted_at": None,
    }
    base.update(over)
    return base


def install_task_key_stubs(monkeypatch, *, api_key=make_api_key(), task=make_task(), parent=None):
    async def get_api_key_config(_key, include_disabled=False):
        return api_key

    async def get_api_key_by_id(key_id):
        assert key_id == api_key["parent_id"]
        return parent or {"id": api_key["parent_id"], "disabled": False, "expires_at": None}

    async def get_task_by_api_key_id(key_id):
        assert key_id == api_key["id"]
        return task

    async def usage_totals(_key_id, include_children=False):
        return {"requests": 0, "total_tokens": 0}

    monkeypatch.setattr(main.config.Config, "get_api_key_config", get_api_key_config)
    # Parent-key lookup resolves from the in-memory snapshot, not the DB: it sits
    # on every request path, so Config.get_api_key_by_id is the real call site.
    monkeypatch.setattr(main.config.Config, "get_api_key_by_id", get_api_key_by_id)
    monkeypatch.setattr(main.PostgresClient, "get_task_by_api_key_id", get_task_by_api_key_id)
    monkeypatch.setattr(main.PostgresClient, "api_key_usage_totals", usage_totals)


def codex_headers(*, thread_id="thread-1", installation_id="install-1") -> dict:
    return {
        "thread-id": thread_id,
        "session-id": thread_id,
        "x-codex-turn-metadata": json.dumps(
            {"thread_id": thread_id, "session_id": thread_id, "installation_id": installation_id}
        ),
    }


# ── validator dispatch ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_request_context_uses_task_path_for_task_scope(monkeypatch):
    install_task_key_stubs(monkeypatch)
    ctx = await main._resolve_request_context("sk-task", codex_headers(), {"input": [{"role": "user", "content": "hello"}]})
    assert ctx["task_id"] == "00000000-0000-0000-0000-00000000000a"
    assert ctx["first_request"] is True
    assert ctx["provider_thread_id"] == "thread-1"


@pytest.mark.asyncio
async def test_resolve_request_context_returns_empty_for_general_scope(monkeypatch):
    # A general-scope key must not be handled by the task or editor validators.
    async def get_api_key_config(_key, include_disabled=False):
        return make_api_key(scope="general", id=42, parent_id=None)

    monkeypatch.setattr(main.config.Config, "get_api_key_config", get_api_key_config)
    ctx = await main._resolve_request_context("sk-general", codex_headers(), {"input": []})
    assert ctx == {}


# ── task validator ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_task_validator_rejects_wrong_provider(monkeypatch):
    install_task_key_stubs(monkeypatch, task=make_task(provider="claude"))
    with pytest.raises(HTTPException, match="请求 provider 与任务不匹配") as exc:
        await main._validate_task_request_context(
            "sk-task", codex_headers(), {"input": [{"role": "user", "content": "hello"}]}
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_task_validator_rejects_codex_wrong_installation(monkeypatch):
    install_task_key_stubs(monkeypatch)
    with pytest.raises(HTTPException, match="Codex 客户端实例不匹配") as exc:
        await main._validate_task_request_context(
            "sk-task",
            codex_headers(installation_id="install-other"),
            {"input": [{"role": "user", "content": "hello"}]},
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_task_validator_rejects_codex_wrong_content(monkeypatch):
    install_task_key_stubs(monkeypatch)
    with pytest.raises(HTTPException, match="Codex 首条消息与预注册任务不匹配") as exc:
        await main._validate_task_request_context(
            "sk-task", codex_headers(), {"input": [{"role": "user", "content": "different"}]}
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_task_validator_bound_thread_must_match(monkeypatch):
    install_task_key_stubs(monkeypatch, task=make_task(provider_thread_id="thread-bound", first_request_seen=True, bootstrap_consumed=True))
    with pytest.raises(HTTPException, match="provider 会话不匹配") as exc:
        await main._validate_task_request_context(
            "sk-task", codex_headers(thread_id="thread-other"), {"input": []}
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_task_validator_bound_thread_accepts_match(monkeypatch):
    install_task_key_stubs(monkeypatch, task=make_task(provider_thread_id="thread-bound", first_request_seen=True, bootstrap_consumed=True))
    ctx = await main._validate_task_request_context(
        "sk-task", codex_headers(thread_id="thread-bound"), {"input": []}
    )
    assert ctx["first_request"] is False
    assert ctx["provider_thread_id"] == "thread-bound"


@pytest.mark.asyncio
async def test_task_validator_rejects_disabled_key(monkeypatch):
    install_task_key_stubs(monkeypatch, api_key=make_api_key(disabled=True))
    with pytest.raises(HTTPException, match="已停用") as exc:
        await main._validate_task_request_context(
            "sk-task", codex_headers(), {"input": [{"role": "user", "content": "hello"}]}
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_task_validator_rejects_when_parent_disabled(monkeypatch):
    install_task_key_stubs(monkeypatch, parent={"id": 7, "disabled": True, "expires_at": None})
    with pytest.raises(HTTPException, match="父 API Key 不可用") as exc:
        await main._validate_task_request_context(
            "sk-task", codex_headers(), {"input": [{"role": "user", "content": "hello"}]}
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_task_validator_returns_empty_for_general_scope(monkeypatch):
    async def get_api_key_config(_key, include_disabled=False):
        return make_api_key(scope="general")

    monkeypatch.setattr(main.config.Config, "get_api_key_config", get_api_key_config)
    ctx = await main._validate_task_request_context("sk-general", codex_headers(), {"input": []})
    assert ctx == {}


# ── child key writer scope ───────────────────────────────────────────────────


def test_create_task_child_api_key_signature_accepts_scope():
    # The writer must accept an explicit scope so task/editor share the clone
    # logic without editor callers changing behavior.
    import inspect

    sig = inspect.signature(main.PostgresClient._create_child_api_key)
    assert "scope" in sig.parameters
    assert sig.parameters["scope"].default == "editor"


# ── thread bind concurrency ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_bind_task_provider_thread_loses_race_returns_none(monkeypatch):
    bound = {"id": "00000000-0000-0000-0000-00000000000a", "provider_thread_id": "thread-1"}

    async def fetchrow(query, *args, **_kw):
        # The conditional UPDATE matches no row (race lost) → returns None.
        return None

    class _Conn:
        async def execute(self, *_args, **_kw):
            return "OK"

        async def fetchrow(self, query, *args, **kw):
            return await fetchrow(query, *args, **kw)

        def transaction(self):
            return _Tx()

    class _Tx:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_exc):
            return False

    class _Pool:
        def acquire(self):
            class _A:
                async def __aenter__(self):
                    return _Conn()

                async def __aexit__(self, *_exc):
                    return False

            return _A()

    monkeypatch.setattr(main.PostgresClient, "pool", _Pool())
    result = await main.PostgresClient.bind_task_provider_thread(
        "00000000-0000-0000-0000-00000000000a", "thread-1", "req-1"
    )
    assert result is None
