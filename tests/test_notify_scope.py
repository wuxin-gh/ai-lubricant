"""Notify channel owner-scope isolation (personal vs team).

``notify_service`` serves two surfaces from one table: ``/api/v1/users/notify``
(``owner_type="user"``) and ``/api/v1/teams/notify`` (``owner_type="team"``,
where the owner id is a *team* id). Because both are addressed by the same
channel-id URLs, the scope check is the only thing preventing one surface from
reading or mutating the other's rows — and a webhook URL plus its secret is
exactly what must not leak across a tenant boundary.

A regression here is silent: the wrong channel simply becomes reachable, with no
error and nothing in a log. Hence these are locked down explicitly.

In-memory Tortoise for ``mc_notify_*``; no Postgres and no outbound HTTP.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from tortoise import Tortoise

from monkeycode_compat.models_notify import NotifyChannel, NotifySubscription
from monkeycode_compat.notify_service import notify_service


def _configured(rows: list[dict]) -> list[dict]:
    """Drop the auto-provisioned notify-center channel from a listing.

    ``list_channels`` provisions the in-app center on demand, so every scope now
    contains it. These tests assert the *configured* (user-created) channels, so
    the built-in one is filtered out here.
    """
    return [r for r in rows if not r.get("builtin")]


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


async def _team_channel(team_id: str, **over) -> dict:
    payload = {
        "name": "group bot",
        "kind": "feishu",
        "webhook_url": "https://open.feishu.cn/hook/team",
        "secret": "team-secret",
        "event_types": ["task.ended"],
    }
    payload.update(over)
    return await notify_service.create_channel(team_id, payload, owner_type="team")


async def _user_channel(user_id: str, **over) -> dict:
    payload = {
        "name": "my bot",
        "kind": "dingtalk",
        "webhook_url": "https://oapi.dingtalk.com/hook/me",
        "event_types": ["task.created"],
    }
    payload.update(over)
    return await notify_service.create_channel(user_id, payload, owner_type="user")


@pytest.mark.asyncio
async def test_scope_comes_from_the_route_not_the_request_body(tortoise_db):
    """``owner_type`` in the body must be ignored.

    The routes pass the scope as a keyword; if the body could set it, any user
    could mint a team-shared channel through the personal endpoint.
    """
    user = str(uuid.uuid4())
    created = await notify_service.create_channel(
        user,
        {
            "name": "forged",
            "kind": "webhook",
            "webhook_url": "https://example.test/hook",
            "owner_type": "team",  # attacker-supplied
        },
        owner_type="user",
    )
    row = await NotifyChannel.get(id=uuid.UUID(created["id"]))
    assert row.owner_type == "user"
    assert str(row.owner_id) == user


@pytest.mark.asyncio
async def test_team_and_personal_lists_do_not_bleed(tortoise_db):
    """Same owner id in both scopes must still yield disjoint lists.

    A user id and a team id are both UUIDs drawn from different namespaces, so
    filtering on ``owner_id`` alone would be enough to collide if they ever
    matched. The scope column is what keeps them apart.
    """
    shared_id = str(uuid.uuid4())  # deliberately the SAME id in both scopes
    team_ch = await _team_channel(shared_id)
    user_ch = await _user_channel(shared_id)

    team_rows = _configured(await notify_service.list_channels(shared_id, owner_type="team"))
    user_rows = _configured(await notify_service.list_channels(shared_id, owner_type="user"))

    assert [r["id"] for r in team_rows] == [team_ch["id"]]
    assert [r["id"] for r in user_rows] == [user_ch["id"]]


@pytest.mark.asyncio
async def test_personal_caller_cannot_reach_a_team_channel_by_id(tortoise_db):
    """Every read/write entry point must refuse a cross-scope channel id.

    ``role="admin"`` is passed on purpose: privilege widens the *personal*
    surface, and must not become a way around the scope boundary.
    """
    team = str(uuid.uuid4())
    user = str(uuid.uuid4())
    team_ch = await _team_channel(team)
    cid = team_ch["id"]

    assert await notify_service.update_channel(
        user, cid, {"name": "hijacked"}, role="admin"
    ) is False
    assert await notify_service.delete_channel(user, cid, role="admin") is False
    assert await notify_service.test_channel(user, cid, role="admin") is None
    assert await notify_service.list_send_logs(user, cid, role="admin") is None
    assert await notify_service.list_subscriptions(user, cid, role="admin") is None

    # The row itself is untouched.
    row = await NotifyChannel.get(id=uuid.UUID(cid))
    assert row.name == "group bot"


@pytest.mark.asyncio
async def test_team_caller_cannot_reach_a_personal_channel_by_id(tortoise_db):
    team = str(uuid.uuid4())
    user = str(uuid.uuid4())
    user_ch = await _user_channel(user)

    assert await notify_service.update_channel(
        team, user_ch["id"], {"name": "hijacked"}, owner_type="team"
    ) is False
    assert await notify_service.delete_channel(
        team, user_ch["id"], owner_type="team"
    ) is False
    assert await notify_service.test_channel(
        team, user_ch["id"], owner_type="team"
    ) is None


@pytest.mark.asyncio
async def test_one_team_cannot_reach_another_teams_channel(tortoise_db):
    """Tenant isolation inside the team scope itself."""
    team_a, team_b = str(uuid.uuid4()), str(uuid.uuid4())
    ch = await _team_channel(team_a)

    assert _configured(await notify_service.list_channels(team_b, owner_type="team")) == []
    assert await notify_service.update_channel(
        team_b, ch["id"], {"name": "x"}, owner_type="team"
    ) is False
    assert await notify_service.delete_channel(
        team_b, ch["id"], owner_type="team"
    ) is False


@pytest.mark.asyncio
async def test_privileged_role_does_not_widen_into_team_rows(tortoise_db):
    """``admin`` sees all *personal* channels, never team ones.

    Widening the team scope for privileged roles would put every tenant's
    webhook URLs on one page, so the team query stays strictly owner-scoped.
    """
    team = str(uuid.uuid4())
    user_a, user_b = str(uuid.uuid4()), str(uuid.uuid4())
    team_ch = await _team_channel(team)
    a = await _user_channel(user_a)
    b = await _user_channel(user_b, name="other user bot")

    rows = await notify_service.list_channels(user_a, role="admin")
    ids = {r["id"] for r in rows}
    assert {a["id"], b["id"]} <= ids, "privileged role should see all personal rows"
    assert team_ch["id"] not in ids, "team channel leaked into the personal list"


@pytest.mark.asyncio
async def test_privileged_team_listing_is_still_scoped_to_its_own_team(tortoise_db):
    team_a, team_b = str(uuid.uuid4()), str(uuid.uuid4())
    a = await _team_channel(team_a)
    await _team_channel(team_b, name="other team bot")

    rows = _configured(
        await notify_service.list_channels(team_a, role="admin", owner_type="team")
    )
    assert [r["id"] for r in rows] == [a["id"]]


@pytest.mark.asyncio
async def test_team_round_trip_within_its_own_scope(tortoise_db):
    """The happy path still works, and the secret never crosses the DTO."""
    team = str(uuid.uuid4())
    ch = await _team_channel(team)

    assert ch["event_types"] == ["task.ended"]
    assert ch["has_secret"] is True
    assert "secret" not in ch, "raw secret must never leave the service"

    ok = await notify_service.update_channel(
        team,
        ch["id"],
        {"name": "renamed", "event_types": ["task.created", "task.ended"]},
        owner_type="team",
    )
    assert ok is True

    rows = _configured(await notify_service.list_channels(team, owner_type="team"))
    assert rows[0]["name"] == "renamed"
    assert sorted(rows[0]["event_types"]) == ["task.created", "task.ended"]
    # An update that only carries events must not wipe the stored secret.
    assert rows[0]["has_secret"] is True

    assert await notify_service.delete_channel(
        team, ch["id"], owner_type="team"
    ) is True
    assert _configured(await notify_service.list_channels(team, owner_type="team")) == []


@pytest.mark.asyncio
async def test_delete_cascades_subscription_rows(tortoise_db):
    team = str(uuid.uuid4())
    ch = await _team_channel(team)
    cid = uuid.UUID(ch["id"])
    assert await NotifySubscription.filter(channel_id=cid).count() == 1

    await notify_service.delete_channel(team, ch["id"], owner_type="team")
    assert await NotifySubscription.filter(channel_id=cid).count() == 0
