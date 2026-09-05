import asyncio

import admin
import pytest
from fastapi import HTTPException


def _allow_admin(monkeypatch):
    async def require_admin(_token):
        return None

    monkeypatch.setattr(admin, "_require_admin", require_admin)


def test_batch_add_accounts_rejects_duplicate_before_writing(monkeypatch):
    _allow_admin(monkeypatch)

    async def read_provider(_name):
        return {"accounts": [{"username": "existing"}], "rate_limit": {}}

    writes = []

    async def persist(_name, account):
        writes.append(account)

    monkeypatch.setattr(admin, "_read_provider_config", read_provider)
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])
    monkeypatch.setattr(admin, "_persist_account", persist)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.batch_add_accounts(
            "demo",
            {"accounts": [{"username": "new", "password": "a"}, {"username": "existing", "password": "b"}]},
            "token",
        ))

    assert exc.value.status_code == 400
    assert writes == []


def test_batch_add_accounts_persists_and_reloads_runtime(monkeypatch):
    _allow_admin(monkeypatch)
    stored = []
    loaded = []
    events = []

    async def read_provider(_name):
        return {"accounts": [], "rate_limit": {}}

    async def persist(_name, account):
        stored.append(dict(account))

    async def log_operation(*_args):
        return None

    async def publish(name):
        events.append(name)

    monkeypatch.setattr(admin, "_read_provider_config", read_provider)
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])
    monkeypatch.setattr(admin, "_persist_account", persist)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda _name: object())
    monkeypatch.setattr(admin, "_reload_account_into_pool", lambda _name, account, _rpm, _extra, _proxies=None: loaded.append(account["username"]))
    monkeypatch.setattr(admin, "_provider_extra", lambda _name, _cfg: {})
    monkeypatch.setattr(admin, "_log_operation", log_operation)
    monkeypatch.setattr(admin, "_publish_channel_event", publish)

    result = asyncio.run(admin.batch_add_accounts(
        "demo",
        {"accounts": [{"username": "a", "password": "ka"}, {"username": "b", "password": "kb"}]},
        "token",
    ))

    assert result == {"ok": True, "added": ["a", "b"], "count": 2}
    assert [item["username"] for item in stored] == ["a", "b"]
    assert loaded == ["a", "b"]
    assert events == ["demo"]


def test_batch_delete_accounts_returns_partial_results(monkeypatch):
    _allow_admin(monkeypatch)
    removed = []
    backups = []

    async def read_provider(_name):
        return {"accounts": [{"username": "a"}, {"username": "b"}]}

    async def remove(_name, username):
        if username == "b":
            return False
        removed.append(username)
        return True

    async def cleanup(*_args):
        return False

    async def log_operation(*_args):
        return None

    async def publish(*_args):
        return None

    monkeypatch.setattr(admin, "_read_provider_config", read_provider)
    monkeypatch.setattr(admin, "_remove_account_row", remove)
    monkeypatch.setattr(admin, "_remove_account_from_pool", lambda *_args: None)
    monkeypatch.setattr(admin, "_cleanup_account_routes", cleanup)
    monkeypatch.setattr(admin, "_write_delete_backup", lambda _kind, name, _payload: backups.append(name))
    monkeypatch.setattr(admin, "_log_operation", log_operation)
    monkeypatch.setattr(admin, "_publish_channel_event", publish)

    result = asyncio.run(admin.batch_delete_accounts("demo", {"usernames": ["a", "missing", "b"]}, "token"))

    assert result["deleted"] == ["a"]
    assert [item["username"] for item in result["failed"]] == ["missing", "b"]
    assert removed == ["a"]
    assert backups == ["demo-a", "demo-b"]


def test_batch_update_proxy_validates_all_and_clears_proxy(monkeypatch):
    _allow_admin(monkeypatch)
    persisted = []
    reloaded = []
    cfg = {
        "accounts": [
            {"username": "a", "password": "secret-a", "proxy_id": "p1"},
            {"username": "b", "password": "secret-b", "proxy_id": "p1"},
        ],
        "rate_limit": {},
    }

    async def read_provider(_name):
        return cfg

    async def persist(_name, account):
        persisted.append(dict(account))

    async def log_operation(*_args):
        return None

    async def publish(*_args):
        return None

    monkeypatch.setattr(admin, "_read_provider_config", read_provider)
    monkeypatch.setattr(admin, "_read_proxies", lambda: [{"id": "p1", "url": "http://proxy"}])
    monkeypatch.setattr(admin, "_persist_account", persist)
    monkeypatch.setattr(admin, "_reload_account_into_pool", lambda _name, account, _rpm, _extra, _proxies=None: reloaded.append(dict(account)))
    monkeypatch.setattr(admin, "_provider_extra", lambda _name, _cfg: {})
    monkeypatch.setattr(admin, "_log_operation", log_operation)
    monkeypatch.setattr(admin, "_publish_channel_event", publish)

    result = asyncio.run(admin.batch_update_account_proxy("demo", {"usernames": ["a", "b"], "proxy": None}, "token"))

    assert result == {"ok": True, "updated": ["a", "b"], "count": 2}
    assert all("proxy_id" not in account and "proxy" not in account for account in persisted)
    assert [account["password"] for account in persisted] == ["secret-a", "secret-b"]
    assert len(reloaded) == 2


def test_batch_update_proxy_replaces_existing_proxy_id(monkeypatch):
    _allow_admin(monkeypatch)
    persisted = []
    cfg = {
        "accounts": [{"username": "a", "password": "secret", "proxy_id": "old"}],
        "rate_limit": {},
    }

    async def read_provider(_name):
        return cfg

    async def persist(_name, account):
        persisted.append(dict(account))

    async def noop(*_args):
        return None

    monkeypatch.setattr(admin, "_read_provider_config", read_provider)
    monkeypatch.setattr(admin, "_read_proxies", lambda: [
        {"id": "old", "url": "http://old.example"},
        {"id": "new", "url": "http://new.example"},
    ])
    monkeypatch.setattr(admin, "_persist_account", persist)
    monkeypatch.setattr(admin, "_reload_account_into_pool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(admin, "_provider_extra", lambda _name, _cfg: {})
    monkeypatch.setattr(admin, "_log_operation", noop)
    monkeypatch.setattr(admin, "_publish_channel_event", noop)

    result = asyncio.run(admin.batch_update_account_proxy(
        "demo", {"usernames": ["a"], "proxy": "new"}, "token"
    ))

    assert result == {"ok": True, "updated": ["a"], "count": 1}
    assert persisted == [{"username": "a", "password": "secret", "proxy_id": "new"}]



    _allow_admin(monkeypatch)
    writes = []

    async def read_provider(_name):
        return {"accounts": [{"username": "a"}], "rate_limit": {}}

    async def persist(_name, account):
        writes.append(account)

    monkeypatch.setattr(admin, "_read_provider_config", read_provider)
    monkeypatch.setattr(admin, "_persist_account", persist)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.batch_update_account_proxy("demo", {"usernames": ["a", "missing"], "proxy": None}, "token"))

    assert exc.value.status_code == 400
    assert writes == []
