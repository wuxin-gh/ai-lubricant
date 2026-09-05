import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import MappingProxyType

import pytest

import model_catalog
from db import PostgresClient


def _source(*, model="m1", max_tokens=1024):
    return {
        "groups": {
            "primary": {
                "name": "primary",
                "enabled": True,
                "models": [model, "m2"],
                "aliases": ["chat"],
                "metadata_model": "",
            },
            "disabled": {
                "name": "disabled",
                "enabled": False,
                "models": [model],
                "aliases": ["hidden"],
            },
        },
        "metadata": [
            {"model_id": model, "max_tokens": max_tokens, "capabilities": {"vision": True}},
            {"model_id": "m2", "max_tokens": 2048},
        ],
        "default": {"max_tokens": 4096, "input_modalities": ["text"]},
    }


def _reset_catalog(monkeypatch):
    empty = model_catalog._build_snapshot(
        {"groups": {}, "metadata": {}, "default": {}}, generation=0
    )
    monkeypatch.setattr(model_catalog, "_snapshot", empty)
    monkeypatch.setattr(model_catalog, "_reload_lock", asyncio.Lock())


class SourceDB:
    source = _source()
    calls = 0

    @classmethod
    async def load_model_catalog_source(cls):
        cls.calls += 1
        return cls.source


@pytest.mark.asyncio
async def test_reload_publishes_deeply_immutable_atomic_snapshot(monkeypatch):
    _reset_catalog(monkeypatch)
    SourceDB.source = _source()
    SourceDB.calls = 0

    old = model_catalog.current_snapshot()
    changed = await model_catalog.reload_from_db(db_client=SourceDB)
    snapshot = model_catalog.current_snapshot()

    assert changed is True
    assert old.generation == 0
    assert old.groups == {}
    assert snapshot is model_catalog.current_snapshot()
    assert snapshot.generation == 1
    assert snapshot.groups["primary"]["models"] == ("m1", "m2")
    assert snapshot.group_index["primary"] is snapshot.groups["primary"]
    assert snapshot.group_index["chat"] is snapshot.groups["primary"]
    assert "hidden" not in snapshot.group_index
    assert snapshot.metadata["m1"]["capabilities"]["vision"] is True
    assert snapshot.group_metadata["chat"]["max_tokens"] == 1024
    assert isinstance(snapshot.groups, MappingProxyType)
    with pytest.raises(TypeError):
        snapshot.default["max_tokens"] = 1
    with pytest.raises(TypeError):
        snapshot.metadata["m1"]["capabilities"]["vision"] = False


@pytest.mark.asyncio
async def test_same_content_keeps_generation_fingerprint_and_identity(monkeypatch):
    _reset_catalog(monkeypatch)
    SourceDB.source = _source()

    first_changed = await model_catalog.reload_from_db(db_client=SourceDB)
    first = model_catalog.current_snapshot()
    same = _source()
    SourceDB.source = {
        "default": dict(reversed(list(same["default"].items()))),
        "metadata": list(reversed(same["metadata"])),
        "groups": same["groups"],
    }
    second_changed = await model_catalog.reload_from_db(db_client=SourceDB)
    second = model_catalog.current_snapshot()

    assert first_changed is True
    assert second_changed is False
    assert second is first
    assert second.generation == 1
    assert second.fingerprint == first.fingerprint


@pytest.mark.asyncio
async def test_changed_content_bumps_generation(monkeypatch):
    _reset_catalog(monkeypatch)
    SourceDB.source = _source(max_tokens=1024)
    await model_catalog.reload_from_db(db_client=SourceDB)
    first = model_catalog.current_snapshot()
    SourceDB.source = _source(max_tokens=512)

    changed = await model_catalog.reload_from_db(db_client=SourceDB)
    second = model_catalog.current_snapshot()

    assert changed is True
    assert second.generation == first.generation + 1
    assert second.fingerprint != first.fingerprint
    assert second.metadata["m1"]["max_tokens"] == 512


@pytest.mark.asyncio
async def test_failed_reload_preserves_previous_snapshot(monkeypatch):
    _reset_catalog(monkeypatch)
    SourceDB.source = _source()
    await model_catalog.reload_from_db(db_client=SourceDB)
    current = model_catalog.current_snapshot()

    class FailingDB:
        @classmethod
        async def load_model_catalog_source(cls):
            raise RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        await model_catalog.reload_from_db(db_client=FailingDB)

    assert model_catalog.current_snapshot() is current


def test_current_snapshot_performs_no_database_work(monkeypatch):
    _reset_catalog(monkeypatch)

    class ExplodingDB:
        @classmethod
        async def load_model_catalog_source(cls):
            raise AssertionError("must not be called")

    monkeypatch.setattr(PostgresClient, "load_model_catalog_source", ExplodingDB.load_model_catalog_source)
    assert model_catalog.current_snapshot().generation == 0


@pytest.mark.asyncio
async def test_reload_lock_serializes_source_loads(monkeypatch):
    _reset_catalog(monkeypatch)

    class BlockingDB:
        active = 0
        max_active = 0

        @classmethod
        async def load_model_catalog_source(cls):
            cls.active += 1
            cls.max_active = max(cls.max_active, cls.active)
            await asyncio.sleep(0.01)
            cls.active -= 1
            return _source()

    await asyncio.gather(
        model_catalog.reload_from_db(db_client=BlockingDB),
        model_catalog.reload_from_db(db_client=BlockingDB),
    )
    assert BlockingDB.max_active == 1


class FakeTransaction:
    def __init__(self, connection, options):
        self.connection = connection
        self.options = options

    async def __aenter__(self):
        self.connection.events.append(("transaction_enter", self.options))
        self.connection.in_transaction = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.connection.events.append(("transaction_exit", exc_type))
        self.connection.in_transaction = False
        return False


class FakeConnection:
    def __init__(self):
        self.events = []
        self.in_transaction = False
        self.group_rows = [
            {
                "name": "first",
                "kind": "custom",
                "enabled": True,
                "remark": "",
                "models": json.dumps(["m1"]),
                "aliases": json.dumps(["chat"]),
                "provider_whitelist": json.dumps([]),
                "provider_blacklist": json.dumps([]),
                "selection_strategy": "intelligent",
                "backup_group": "",
                "response_model": "",
                "metadata_model": "",
                "metadata": None,
                "schemes": None,
                "active_scheme": "",
                "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
            {
                "name": "m1",
                "kind": "real",
                "enabled": True,
                "remark": "",
                "models": json.dumps([]),
                "aliases": json.dumps([]),
                "provider_whitelist": json.dumps([]),
                "provider_blacklist": json.dumps([]),
                "selection_strategy": "intelligent",
                "backup_group": "",
                "response_model": "",
                "metadata_model": "",
                "metadata": json.dumps({"max_tokens": 100}),
                "schemes": None,
                "active_scheme": "",
                "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        ]

    def transaction(self, **options):
        self.events.append(("transaction", options))
        return FakeTransaction(self, options)

    async def fetch(self, query, *args):
        assert self.in_transaction
        self.events.append(("fetch", query, args))
        if "FROM model_groups" in query:
            return self.group_rows
        raise AssertionError(query)

    async def fetchrow(self, query, *args):
        assert self.in_transaction
        self.events.append(("fetchrow", query, args))
        if "FROM app_config" in query:
            return {"data": json.dumps({"max_tokens": 4096})}
        if "INSERT INTO model_groups" in query:
            name = args[0]
            return {**self.group_rows[0], "name": name, "models": args[4], "aliases": args[5]}
        raise AssertionError(query)

    async def execute(self, query, *args):
        assert self.in_transaction
        self.events.append(("execute", query, args))
        return "DELETE 1"


class FakePool:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


@pytest.mark.asyncio
async def test_db_source_loader_uses_one_readonly_repeatable_read_transaction(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setattr(PostgresClient, "pool", FakePool(connection))

    source = await PostgresClient.load_model_catalog_source()

    assert connection.events[0] == (
        "transaction",
        {"isolation": "repeatable_read", "readonly": True},
    )
    assert sum(event[0] == "transaction" for event in connection.events) == 1
    assert "ORDER BY created_at ASC, name" in next(
        event[1] for event in connection.events if event[0] == "fetch" and "model_groups" in event[1]
    )
    assert list(source["groups"]) == ["first", "m1"]
    assert source["groups"]["m1"]["kind"] == "real"
    assert source["groups"]["first"]["kind"] == "custom"
    assert source["metadata"] == [{"model_id": "m1", "max_tokens": 100}]
    assert source["default"] == {"max_tokens": 4096}


@pytest.mark.asyncio
async def test_replace_model_groups_is_one_transaction_and_refreshes_once(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setattr(PostgresClient, "pool", FakePool(connection))
    refreshes = 0

    async def refresh(cls):
        nonlocal refreshes
        refreshes += 1

    monkeypatch.setattr(PostgresClient, "_refresh_model_routing_cache", classmethod(refresh))

    result = await PostgresClient.replace_model_groups(
        {
            "one": {"models": ["m1"]},
            "two": {"models": ["m2"], "aliases": ["chat"]},
        }
    )

    assert list(result) == ["one", "two"]
    assert sum(event[0] == "transaction" for event in connection.events) == 1
    statements = [event for event in connection.events if event[0] in ("execute", "fetchrow")]
    assert statements[0][0:2] == ("execute", "DELETE FROM model_groups WHERE kind <> 'real'")
    assert sum(event[0] == "fetchrow" and "INSERT INTO model_groups" in event[1] for event in statements) == 2
    assert refreshes == 1


@pytest.mark.asyncio
async def test_rename_upsert_and_delete_are_inside_same_transaction(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setattr(PostgresClient, "pool", FakePool(connection))

    async def refresh(cls):
        return None

    monkeypatch.setattr(PostgresClient, "_refresh_model_routing_cache", classmethod(refresh))

    result = await PostgresClient.upsert_model_group(
        "old", {"name": "new", "models": ["m1"]}
    )

    assert result["name"] == "new"
    assert sum(event[0] == "transaction" for event in connection.events) == 1
    insert_index = next(i for i, event in enumerate(connection.events) if event[0] == "fetchrow")
    delete_index = next(
        i
        for i, event in enumerate(connection.events)
        if event[0] == "execute" and "WHERE name=$1" in event[1]
    )
    exit_index = next(i for i, event in enumerate(connection.events) if event[0] == "transaction_exit")
    assert insert_index < delete_index < exit_index
