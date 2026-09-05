import asyncio

import pytest
from fastapi import HTTPException

import config
import admin
import main
import model_catalog
from rate_limiter import NoAvailableAccountError
from db import PostgresClient


def test_backup_resolves_fresh_one_hop_and_uses_canonical_visited(monkeypatch):
    first = type("Snapshot", (), {"generation": 1})()
    second = type("Snapshot", (), {"generation": 2})()
    snapshots = [first, first, second, second]
    attempted = []

    def current_snapshot():
        return snapshots.pop(0) if snapshots else second

    groups_by_generation = {
        1: {
            "alias": {"name": "primary", "backup_group": "old-backup"},
            "primary": {"name": "primary", "backup_group": "old-backup"},
            "old-backup": {"name": "old-backup"},
        },
        2: {
            "primary": {"name": "primary", "backup_group": "fresh-backup"},
            "fresh-backup": {"name": "fresh-backup", "backup_group": "alias"},
            "alias": {"name": "primary", "backup_group": "fresh-backup"},
        },
    }

    async def resolve(cls, model, snapshot=None):
        return groups_by_generation[snapshot.generation].get(model)

    async def response_model(cls, model, snapshot=None):
        return "stable-public"

    async def exhausted(model, *args, **kwargs):
        attempted.append((model, kwargs.get("_stable_response_model")))
        raise NoAvailableAccountError(status_code=429, detail="exhausted")
        yield

    monkeypatch.setattr(main.model_catalog, "current_snapshot", current_snapshot)
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(response_model))
    monkeypatch.setattr(main, "_chat_with_retry_for_model", exhausted)

    async def collect():
        with pytest.raises(NoAvailableAccountError):
            async for _ in main._chat_with_retry("alias", [], False):
                pass

    asyncio.run(collect())
    assert attempted == [("alias", "stable-public"), ("fresh-backup", "stable-public")]


def test_backup_logs_primary_rejection_detail(monkeypatch):
    snapshot = type("Snapshot", (), {"generation": 7})()
    warnings = []

    async def resolve(cls, model, snapshot=None):
        return {"name": model, "backup_group": ""}

    async def exhausted(*args, **kwargs):
        raise NoAvailableAccountError(status_code=429, detail="reservation_denied x2")
        yield

    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(lambda cls, model, snapshot=None: _async_return("")))
    monkeypatch.setattr(main, "_chat_with_retry_for_model", exhausted)
    monkeypatch.setattr(main.logger, "warning", warnings.append)

    async def collect():
        with pytest.raises(NoAvailableAccountError):
            async for _ in main._chat_with_retry("opus", [], False):
                pass

    asyncio.run(collect())
    assert any("reservation_denied x2" in message and "group='opus'" in message for message in warnings)


def test_retry_exhaustion_advances_to_backup_group(monkeypatch):
    snapshot = type("Snapshot", (), {"generation": 9})()
    attempted = []

    async def resolve(cls, model, snapshot=None):
        groups = {
            "primary": {"name": "primary", "backup_group": "backup"},
            "backup": {"name": "backup", "backup_group": ""},
        }
        return groups.get(model)

    async def response_model(cls, model, snapshot=None):
        return "stable-public"

    async def run_group(model, *args, **kwargs):
        attempted.append(model)
        if model == "primary":
            raise main.ModelGroupExhaustedError(
                status_code=429,
                detail={"message": "服务端异常，请重试", "upstream_status": 404},
            )
        yield {"choices": [{"message": {"content": "backup ok"}}]}

    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(response_model))
    monkeypatch.setattr(main, "_chat_with_retry_for_model", run_group)

    async def collect():
        return [chunk async for chunk in main._chat_with_retry("primary", [], False)]

    chunks = asyncio.run(collect())
    assert attempted == ["primary", "backup"]
    assert chunks == [{"choices": [{"message": {"content": "backup ok"}}]}]


def test_explicit_client_error_does_not_advance_to_backup_group(monkeypatch):
    snapshot = type("Snapshot", (), {"generation": 10})()
    attempted = []

    async def resolve(cls, model, snapshot=None):
        groups = {
            "primary": {"name": "primary", "backup_group": "backup"},
            "backup": {"name": "backup", "backup_group": ""},
        }
        return groups.get(model)

    async def run_group(model, *args, **kwargs):
        attempted.append(model)
        raise HTTPException(status_code=400, detail="bad client parameter")
        yield

    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(lambda cls, model, snapshot=None: _async_return("")))
    monkeypatch.setattr(main, "_chat_with_retry_for_model", run_group)

    async def collect():
        with pytest.raises(HTTPException) as exc:
            async for _ in main._chat_with_retry("primary", [], False):
                pass
        return exc.value

    exc = asyncio.run(collect())
    assert exc.status_code == 400
    assert attempted == ["primary"]


def _async_return(value):
    async def _coro():
        return value
    return _coro()


def _patch_validate(monkeypatch):
    monkeypatch.setattr(admin.config.Config, "get_providers", classmethod(lambda cls: _async_return({})))
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_names", lambda: [])


def test_normalize_model_group_defaults_empty_backup_group():
    out = PostgresClient.normalize_model_group("g1", {"models": ["m1"]})
    assert out["backup_model_group"] == ""


def test_normalize_model_group_keeps_backup_group():
    out = PostgresClient.normalize_model_group("g1", {"models": ["m1"], "backup_group": " backup "})
    assert out["backup_model_group"] == "backup"


def test_get_model_group_backup_returns_enabled_existing_group(monkeypatch):
    monkeypatch.setattr(
        config.Config,
        "get_model_group_index",
        classmethod(lambda cls: _async_return({"g1": {"name": "g1", "backup_group": "g2"}, "g2": {"name": "g2", "models": ["m2"]}})),
    )
    assert asyncio.run(config.Config.get_model_group_backup("g1")) == "g2"


def test_get_model_group_backup_ignores_missing_or_self(monkeypatch):
    monkeypatch.setattr(
        config.Config,
        "get_model_group_index",
        classmethod(lambda cls: _async_return({"g1": {"name": "g1", "backup_group": "g1"}})),
    )
    assert asyncio.run(config.Config.get_model_group_backup("g1")) == ""


def test_list_model_group_backup_chain_stops_before_cycle(monkeypatch):
    monkeypatch.setattr(
        config.Config,
        "get_model_group_index",
        classmethod(lambda cls: _async_return({"g1": {"name": "g1", "backup_group": "g2"}, "g2": {"name": "g2", "backup_group": "g1"}})),
    )
    assert asyncio.run(config.Config.list_model_group_backup_chain("g1")) == ["g1", "g2"]


def test_validate_model_groups_accepts_backup_group(monkeypatch):
    _patch_validate(monkeypatch)
    groups = {
        "primary": {"name": "primary", "models": ["m1"], "backup_group": "backup"},
        "backup": {"name": "backup", "models": ["m2"]},
    }
    asyncio.run(admin._validate_model_groups(groups))
    assert groups["primary"]["backup_model_group"] == "backup"


def test_validate_model_groups_rejects_missing_backup_group(monkeypatch):
    _patch_validate(monkeypatch)
    groups = {"primary": {"name": "primary", "models": ["m1"], "backup_group": "missing"}}
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin._validate_model_groups(groups))
    assert exc.value.status_code == 400
    assert "backup_model_group" in exc.value.detail


def test_validate_model_groups_rejects_backup_group_cycle(monkeypatch):
    _patch_validate(monkeypatch)
    groups = {
        "g1": {"name": "g1", "models": ["m1"], "backup_group": "g2"},
        "g2": {"name": "g2", "models": ["m2"], "backup_group": "g1"},
    }
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin._validate_model_groups(groups))
    assert exc.value.status_code == 400
    assert "backup_model_group" in exc.value.detail
