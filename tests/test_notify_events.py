"""Configured notification events: entity fields + channel bindings + isolation.

The event is an entity of its own. It stores type, effective range, daily
window, status, trigger conditions and heterogeneous event parameters. The
many-to-many binding table answers only "which channels receive it".

In-memory Tortoise for mc_notify_*; no Postgres, no outbound HTTP.
"""
from __future__ import annotations

import uuid
from datetime import time

import pytest
import pytest_asyncio
from tortoise import Tortoise

from monkeycode_compat.models_notify import NotifyEventChannel, NotifyEventState
from monkeycode_compat.notify_service import notify_service

_PLATFORM = "00000000-0000-0000-0000-0000000009f1"


@pytest_asyncio.fixture
async def tortoise_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"monkeycode_compat": ["monkeycode_compat.models_notify"]},
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def _channel(owner: str, owner_type: str = "platform", name: str = "bot") -> dict:
    return await notify_service.create_channel(
        owner,
        {"name": name, "kind": "webhook", "webhook_url": "https://x.test/h"},
        owner_type=owner_type,
    )


def _payload(channel_ids: list[str], **over) -> dict:
    data = {
        "name": "指定渠道账号冻结",
        "event_type": "account.frozen",
        "status": "active",
        "effective_from": "2026-08-01T00:00:00Z",
        "effective_to": "2026-12-31T23:59:59Z",
        "daily_start": "09:00",
        "daily_end": "18:00",
        "trigger_condition": {
            "conditions": [
                {"type": "severity", "min": "warn"},
                {"type": "count", "times": 3, "window_seconds": 600},
                {"type": "silence", "window_seconds": 3600},
                {"type": "threshold", "field": "failure_rate", "op": ">=", "value": 80},
            ]
        },
        "event_params": {
            "provider_names": ["openai", "anthropic"],
            "account_usernames": ["account-a"],
        },
        "channel_ids": channel_ids,
    }
    data.update(over)
    return data


@pytest.mark.asyncio
async def test_event_round_trip_contains_all_configured_fields(tortoise_db):
    ch = await _channel(_PLATFORM)
    event = await notify_service.create_event(
        _PLATFORM, _payload([ch["id"]]), owner_type="platform"
    )
    assert event is not None
    assert event["name"] == "指定渠道账号冻结"
    assert event["event_type"] == "account.frozen"
    assert event["category"] == "account", "category defaults from the event catalogue"
    assert event["status"] == "active"
    assert event["effective_from"].startswith("2026-08-01T00:00:00")
    assert event["effective_to"].startswith("2026-12-31T23:59:59")
    assert event["daily_start"].startswith("09:00")
    assert event["daily_end"].startswith("18:00")
    assert event["trigger_condition"]["conditions"][1]["times"] == 3
    assert event["event_params"]["provider_names"] == ["openai", "anthropic"]
    assert event["event_params"]["account_usernames"] == ["account-a"]
    assert event["channel_ids"] == [ch["id"]]

    rows = await notify_service.list_events(_PLATFORM, owner_type="platform")
    assert [r["id"] for r in rows] == [event["id"]]
    assert rows[0]["channel_ids"] == [ch["id"]]


@pytest.mark.asyncio
async def test_one_event_binds_many_channels(tortoise_db):
    a = await _channel(_PLATFORM, name="a")
    b = await _channel(_PLATFORM, name="b")
    event = await notify_service.create_event(
        _PLATFORM, _payload([a["id"], b["id"]]), owner_type="platform"
    )
    assert set(event["channel_ids"]) == {a["id"], b["id"]}
    assert await NotifyEventChannel.filter(event_id=uuid.UUID(event["id"])).count() == 2


@pytest.mark.asyncio
async def test_event_drops_foreign_channel_binding(tortoise_db):
    own = await _channel(_PLATFORM)
    foreign_owner = str(uuid.uuid4())
    foreign = await _channel(foreign_owner, owner_type="user", name="foreign")
    event = await notify_service.create_event(
        _PLATFORM, _payload([own["id"], foreign["id"]]), owner_type="platform"
    )
    assert event["channel_ids"] == [own["id"]]


@pytest.mark.asyncio
async def test_platform_and_user_events_are_isolated(tortoise_db):
    user = str(uuid.uuid4())
    pch = await _channel(_PLATFORM)
    uch = await _channel(user, owner_type="user")
    platform_event = await notify_service.create_event(
        _PLATFORM, _payload([pch["id"]]), owner_type="platform"
    )
    user_event = await notify_service.create_event(
        user,
        {
            "event_type": "task.ended",
            "name": "任务结束提醒",
            "channel_ids": [uch["id"]],
            "status": "active",
        },
        owner_type="user",
    )
    assert [e["id"] for e in await notify_service.list_events(_PLATFORM, owner_type="platform")] == [platform_event["id"]]
    assert [e["id"] for e in await notify_service.list_events(user, owner_type="user")] == [user_event["id"]]


@pytest.mark.asyncio
async def test_user_cannot_create_platform_event(tortoise_db):
    user = str(uuid.uuid4())
    ch = await _channel(user, owner_type="user")
    assert await notify_service.create_event(
        user, _payload([ch["id"]]), owner_type="user"
    ) is None, "account.frozen is platform-only"


@pytest.mark.asyncio
async def test_event_update_replaces_bindings_and_config(tortoise_db):
    a = await _channel(_PLATFORM, name="a")
    b = await _channel(_PLATFORM, name="b")
    event = await notify_service.create_event(
        _PLATFORM, _payload([a["id"]]), owner_type="platform"
    )
    ok = await notify_service.update_event(
        _PLATFORM,
        event["id"],
        {
            "name": "renamed",
            "status": "disabled",
            "effective_from": None,
            "effective_to": None,
            "daily_start": "22:00",
            "daily_end": "06:00",
            "trigger_condition": {"conditions": [{"type": "silence", "window_seconds": 60}]},
            "event_params": {"provider_names": ["p2"]},
            "channel_ids": [b["id"]],
        },
        owner_type="platform",
    )
    assert ok is True
    row = (await notify_service.list_events(_PLATFORM, owner_type="platform"))[0]
    assert row["name"] == "renamed"
    assert row["status"] == "disabled"
    assert row["effective_from"] is None and row["effective_to"] is None
    assert row["daily_start"].startswith("22:00")
    assert row["daily_end"].startswith("06:00")
    assert row["trigger_condition"]["conditions"][0]["window_seconds"] == 60
    assert row["event_params"] == {"provider_names": ["p2"]}
    assert row["channel_ids"] == [b["id"]]


@pytest.mark.asyncio
async def test_another_owner_cannot_update_or_delete_event(tortoise_db):
    owner, other = str(uuid.uuid4()), str(uuid.uuid4())
    ch = await _channel(owner, owner_type="user")
    event = await notify_service.create_event(
        owner, {"event_type": "task.ended", "channel_ids": [ch["id"]]}, owner_type="user"
    )
    assert await notify_service.update_event(other, event["id"], {"status": "disabled"}, owner_type="user") is False
    assert await notify_service.delete_event(other, event["id"], owner_type="user") is False
    assert (await notify_service.list_events(owner, owner_type="user"))[0]["status"] == "active"


@pytest.mark.asyncio
async def test_delete_cascades_bindings_and_trigger_state(tortoise_db):
    ch = await _channel(_PLATFORM)
    event = await notify_service.create_event(
        _PLATFORM, _payload([ch["id"]]), owner_type="platform"
    )
    event_id = uuid.UUID(event["id"])
    await NotifyEventState.create(
        id=uuid.uuid4(), event_id=event_id, fingerprint="x", count=2
    )
    assert await NotifyEventChannel.filter(event_id=event_id).count() == 1
    assert await NotifyEventState.filter(event_id=event_id).count() == 1

    assert await notify_service.delete_event(_PLATFORM, event["id"], owner_type="platform") is True
    assert await NotifyEventChannel.filter(event_id=event_id).count() == 0
    assert await NotifyEventState.filter(event_id=event_id).count() == 0
    assert await notify_service.list_events(_PLATFORM, owner_type="platform") == []


def test_time_parser_accepts_minute_precision_and_cross_midnight_values():
    assert notify_service._parse_time("09:30") == time(9, 30)
    assert notify_service._parse_time("22:00") == time(22, 0)
    assert notify_service._parse_time("06:00:59") == time(6, 0, 59)


def test_time_parser_rejects_invalid_values():
    assert notify_service._parse_time("25:00") is None
    assert notify_service._parse_time("bad") is None
    assert notify_service._parse_time(None) is None


def test_datetime_parser_makes_naive_input_utc():
    value = notify_service._parse_dt("2026-08-24T12:00")
    assert value is not None and value.tzinfo is not None
