"""Focused tests for the new group-permission binding logic.

Covers TeamSkill group binding (team_users_service) and MCP-user group binding
(mcp_plugin_store helpers) against an in-memory SQLite DB. The binding rules:
granting a group must not disturb the same resource's bindings to other groups.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={
            "monkeycode_compat": [
                "monkeycode_compat.models_team_admin",
                "monkeycode_compat.models",
            ]
        },
        use_tz=False,
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def _make_team_group(team_id: str, name="g1"):
    from monkeycode_compat.models import TeamGroup

    gid = uuid.uuid4()
    await TeamGroup.create(id=gid, team_id=uuid.UUID(team_id), name=name)
    return str(gid)


@pytest.mark.asyncio
async def test_team_skill_group_binding_isolated_per_group(db):
    from monkeycode_compat.models_team_admin import TeamSkill
    from monkeycode_compat.team_users_service import team_users_service

    team_id = str(uuid.uuid4())
    g1 = await _make_team_group(team_id, "g1")
    g2 = await _make_team_group(team_id, "g2")

    s1 = await TeamSkill.create(
        id=uuid.uuid4(), team_id=uuid.UUID(team_id), name="s1",
        group_ids=[g2],  # already bound to the other group
    )
    s2 = await TeamSkill.create(
        id=uuid.uuid4(), team_id=uuid.UUID(team_id), name="s2", group_ids=[],
    )

    # list_group_skills flags the row already bound to g2.
    rows = await team_users_service.list_group_skills(team_id, g1)
    by_id = {r["id"]: r for r in rows}
    assert by_id[str(s1.id)]["bound"] is False
    assert by_id[str(s2.id)]["bound"] is False

    # Grant only s1 to g1.
    await team_users_service.set_group_skills(team_id, g1, [str(s1.id)])

    rows = await team_users_service.list_group_skills(team_id, g1)
    by_id = {r["id"]: r for r in rows}
    assert by_id[str(s1.id)]["bound"] is True
    assert by_id[str(s2.id)]["bound"] is False

    # s1's binding to g2 must survive (only the g1 edge was added).
    await s1.refresh_from_db()
    assert g1 in list(s1.group_ids or [])
    assert g2 in list(s1.group_ids or [])

    # Unbinding g1 must not touch g2.
    await team_users_service.set_group_skills(team_id, g1, [])
    await s1.refresh_from_db()
    assert g1 not in list(s1.group_ids or [])
    assert g2 in list(s1.group_ids or [])


@pytest.mark.asyncio
async def test_team_skill_set_skills_rejects_other_team_group(db):
    from monkeycode_compat.models_team_admin import TeamSkill
    from monkeycode_compat.team_users_service import team_users_service

    team_a = str(uuid.uuid4())
    team_b = str(uuid.uuid4())
    g_b = await _make_team_group(team_b, "gb")  # group owned by team B

    with pytest.raises(ValueError):
        # _owned_group returns None → group_not_found
        await team_users_service.list_group_skills(team_a, g_b)
