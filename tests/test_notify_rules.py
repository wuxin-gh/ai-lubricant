"""Subscription rules: owner isolation + event-scope enforcement.

The rules table is what decides *who gets pushed what*, so two things must hold:
a user cannot attach a rule to someone else's channel, and a user cannot
subscribe to an admin-only platform event (freeze/security/api_key). Both are
silent failures if broken — the wrong person simply starts receiving events.

In-memory Tortoise for ``mc_notify_*``; no Postgres, no outbound HTTP.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from tortoise import Tortoise

from user_platform.notify_service import notify_service

_PLATFORM = "00000000-0000-0000-0000-0000000009f1"


@pytest_asyncio.fixture
async def tortoise_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"user_platform": ["user_platform.models_notify"]},
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def _channel(owner: str, owner_type: str = "user") -> dict:
    return await notify_service.create_channel(
        owner,
        {"name": "bot", "kind": "webhook", "webhook_url": "https://x.test/h"},
        owner_type=owner_type,
    )


# ── event catalogue scoping ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_user_catalogue_hides_platform_events():
    """The console must not even list admin-only events."""
    user_events = {e["type"] for e in notify_service.list_event_types(owner_scope="user")}
    all_events = {e["type"] for e in notify_service.list_event_types()}
    assert "task.ended" in user_events
    assert "node.online" in user_events
    assert "account.frozen" not in user_events, "platform event leaked to user scope"
    assert "security.warning" not in user_events
    assert "account.frozen" in all_events, "admin catalogue must still carry it"


@pytest.mark.asyncio
async def test_catalogue_entries_carry_category_and_scope():
    for entry in notify_service.list_event_types():
        assert entry.get("category"), f"{entry['type']} missing category"
        assert entry.get("owner_scope") in {"user", "platform"}, entry


@pytest.mark.asyncio
async def test_legacy_events_are_preserved():
    """Prior upstream events must not be dropped (existing subscriptions)."""
    types = {e["type"] for e in notify_service.list_event_types()}
    for legacy in (
        "task.created", "task.ended", "vm.expiring_soon", "quota.refreshed",
        "quota.basic_exhausted", "quota.pro_exhausted", "quota.ultra_exhausted",
    ):
        assert legacy in types, f"legacy event {legacy} was dropped"


# ── rule CRUD + scoping ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_user_rule_round_trip(tortoise_db):
    user = str(uuid.uuid4())
    ch = await _channel(user)
    rule = await notify_service.create_rule(
        user,
        {"channel_id": ch["id"], "event_type": "task.ended", "filters": {}},
        owner_type="user",
    )
    assert rule is not None and rule["event_type"] == "task.ended"

    rows = await notify_service.list_rules(user, owner_type="user")
    assert [r["id"] for r in rows] == [rule["id"]]

    assert await notify_service.update_rule(
        user, rule["id"], {"enabled": False}, owner_type="user"
    ) is True
    rows = await notify_service.list_rules(user, owner_type="user")
    assert rows[0]["enabled"] is False

    assert await notify_service.delete_rule(user, rule["id"], owner_type="user") is True
    assert await notify_service.list_rules(user, owner_type="user") == []


@pytest.mark.asyncio
async def test_user_cannot_subscribe_a_platform_event(tortoise_db):
    """``account.frozen`` is admin-only: a user-scope rule must be refused."""
    user = str(uuid.uuid4())
    ch = await _channel(user)
    assert await notify_service.create_rule(
        user, {"channel_id": ch["id"], "event_type": "account.frozen"}, owner_type="user"
    ) is None
    assert await notify_service.list_rules(user, owner_type="user") == []


@pytest.mark.asyncio
async def test_platform_may_subscribe_any_event(tortoise_db):
    ch = await _channel(_PLATFORM, "platform")
    rule = await notify_service.create_rule(
        _PLATFORM,
        {"channel_id": ch["id"], "event_type": "account.frozen",
         "filters": {"provider_names": ["p1"]}},
        owner_type="platform",
    )
    assert rule is not None
    assert rule["filters"] == {"provider_names": ["p1"]}


@pytest.mark.asyncio
async def test_rule_cannot_target_another_owners_channel(tortoise_db):
    """Attaching a rule to a channel you don't own would push to their webhook."""
    owner, attacker = str(uuid.uuid4()), str(uuid.uuid4())
    ch = await _channel(owner)
    assert await notify_service.create_rule(
        attacker, {"channel_id": ch["id"], "event_type": "task.ended"}, owner_type="user"
    ) is None


@pytest.mark.asyncio
async def test_one_user_cannot_touch_anothers_rule(tortoise_db):
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    ch = await _channel(a)
    rule = await notify_service.create_rule(
        a, {"channel_id": ch["id"], "event_type": "task.ended"}, owner_type="user"
    )
    assert await notify_service.update_rule(
        b, rule["id"], {"enabled": False}, owner_type="user"
    ) is False
    assert await notify_service.delete_rule(b, rule["id"], owner_type="user") is False
    assert await notify_service.list_rules(b, owner_type="user") == []
    # Untouched for the real owner.
    assert (await notify_service.list_rules(a, owner_type="user"))[0]["enabled"] is True


@pytest.mark.asyncio
async def test_user_scope_and_platform_scope_do_not_bleed(tortoise_db):
    """Same owner id in both scopes must still yield disjoint rule lists."""
    shared = _PLATFORM  # deliberately reuse the platform id as a "user" id
    user_ch = await _channel(shared, "user")
    plat_ch = await _channel(shared, "platform")
    user_rule = await notify_service.create_rule(
        shared, {"channel_id": user_ch["id"], "event_type": "task.ended"}, owner_type="user"
    )
    plat_rule = await notify_service.create_rule(
        shared, {"channel_id": plat_ch["id"], "event_type": "account.frozen"}, owner_type="platform"
    )
    assert [r["id"] for r in await notify_service.list_rules(shared, owner_type="user")] == [user_rule["id"]]
    assert [r["id"] for r in await notify_service.list_rules(shared, owner_type="platform")] == [plat_rule["id"]]


@pytest.mark.asyncio
async def test_invalid_payloads_are_refused(tortoise_db):
    user = str(uuid.uuid4())
    ch = await _channel(user)
    assert await notify_service.create_rule(user, {"channel_id": ch["id"]}, owner_type="user") is None
    assert await notify_service.create_rule(user, {"event_type": "task.ended"}, owner_type="user") is None
    assert await notify_service.create_rule(
        user, {"channel_id": "not-a-uuid", "event_type": "task.ended"}, owner_type="user"
    ) is None
    assert await notify_service.create_rule(
        user, {"channel_id": ch["id"], "event_type": "nope.unknown"}, owner_type="user"
    ) is None
