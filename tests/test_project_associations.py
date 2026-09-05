"""Tests for the project-to-project association feature.

Covers the backend contract: owner-only add/remove, self-association rejection,
capability probing (can_read/can_write) using the *source* project's git identity
token against the target repo, and delete cleanup.
"""
from __future__ import annotations

import os
import sys
import uuid

import pytest
import pytest_asyncio

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)


@pytest_asyncio.fixture
async def tortoise_db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite::memory:",
        modules={
            "monkeycode_compat": [
                "monkeycode_compat.models_git",
                "monkeycode_compat.models_project",
            ]
        },
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.fixture(autouse=True)
def neutralize_prefetch(monkeypatch):
    from monkeycode_compat import git_service

    monkeypatch.setattr(
        git_service.git_service, "_prefetch_repositories", lambda *_a, **_k: None
    )


async def _make_identity(user_id, platform="github", token="ghp_tok", **kw):
    from monkeycode_compat.models_git import GitIdentity

    return await GitIdentity.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        platform=platform,
        access_token=token,
        **kw,
    )


async def _make_project(user_id, identity_id, name="proj", repo_url="https://github.com/owner/repo", **kw):
    from monkeycode_compat.models_project import Project

    return await Project.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        name=name,
        platform="github",
        repo_url=repo_url,
        git_identity_id=uuid.UUID(identity_id) if identity_id else None,
        **kw,
    )


# ── add / remove / owner-only ───────────────────────────────────────────────
@pytest.mark.asyncio
async def test_add_then_list_association(tortoise_db, monkeypatch):
    from monkeycode_compat import git_clients as gc, project_service

    owner = str(uuid.uuid4())
    ident_a = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident_a.id), name="A")
    proj_b = await _make_project(owner, str(ident_a.id), name="B",
                                  repo_url="https://github.com/owner/other")

    async def fake_cap(platform, full_name, opts):
        return (True, True)

    monkeypatch.setattr(gc, "fetch_repo_capability", fake_cap)

    assoc = await project_service.project_service.add_association(
        owner, str(proj_a.id), str(proj_b.id)
    )
    assert assoc["source_project_id"] == str(proj_a.id)
    assert assoc["target_project_id"] == str(proj_b.id)

    rows = await project_service.project_service.list_associations(owner, str(proj_a.id))
    assert rows and rows[0]["target_project"]["name"] == "B"
    assert rows[0]["can_read"] is True
    assert rows[0]["can_write"] is True
    # can_read True → repo_url exposed.
    assert rows[0]["target_project"]["repo_url"].endswith("/other")


@pytest.mark.asyncio
async def test_add_association_owner_only(tortoise_db):
    from monkeycode_compat import project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident.id))
    proj_b = await _make_project(owner, str(ident.id), name="B")
    other = str(uuid.uuid4())  # a stranger
    result = await project_service.project_service.add_association(
        other, str(proj_a.id), str(proj_b.id)
    )
    assert result is None


@pytest.mark.asyncio
async def test_self_association_rejected(tortoise_db):
    from monkeycode_compat import project_service

    import pytest as _pytest

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj = await _make_project(owner, str(ident.id))
    with _pytest.raises(ValueError, match="self"):
        await project_service.project_service.add_association(
            owner, str(proj.id), str(proj.id)
        )


@pytest.mark.asyncio
async def test_remove_association(tortoise_db):
    from monkeycode_compat import project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident.id))
    proj_b = await _make_project(owner, str(ident.id), name="B")
    await project_service.project_service.add_association(owner, str(proj_a.id), str(proj_b.id))
    ok = await project_service.project_service.remove_association(
        owner, str(proj_a.id), str(proj_b.id)
    )
    assert ok is True
    rows = await project_service.project_service.list_associations(owner, str(proj_a.id))
    assert rows == []


@pytest.mark.asyncio
async def test_remove_association_owner_only(tortoise_db):
    from monkeycode_compat import project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident.id))
    proj_b = await _make_project(owner, str(ident.id), name="B")
    await project_service.project_service.add_association(owner, str(proj_a.id), str(proj_b.id))
    other = str(uuid.uuid4())
    ok = await project_service.project_service.remove_association(
        other, str(proj_a.id), str(proj_b.id)
    )
    assert ok is False


# ── capability probing ─────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_inaccessible_target_hides_repo_url(tortoise_db, monkeypatch):
    from monkeycode_compat import git_clients as gc, project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident.id))
    proj_b = await _make_project(owner, str(ident.id), name="B",
                                  repo_url="https://github.com/owner/secret")
    await project_service.project_service.add_association(owner, str(proj_a.id), str(proj_b.id))

    async def fake_cap(platform, full_name, opts):
        return (False, False)  # token cannot read B

    monkeypatch.setattr(gc, "fetch_repo_capability", fake_cap)
    rows = await project_service.project_service.list_associations(owner, str(proj_a.id))
    assert rows[0]["can_read"] is False
    assert rows[0]["can_write"] is False
    # can_read False → repo_url must not be exposed.
    assert "repo_url" not in rows[0]["target_project"]


@pytest.mark.asyncio
async def test_read_only_target_marks_no_write(tortoise_db, monkeypatch):
    from monkeycode_compat import git_clients as gc, project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident.id))
    proj_b = await _make_project(owner, str(ident.id), name="B",
                                  repo_url="https://github.com/owner/ro")
    await project_service.project_service.add_association(owner, str(proj_a.id), str(proj_b.id))

    async def fake_cap(platform, full_name, opts):
        return (True, False)  # read but no push

    monkeypatch.setattr(gc, "fetch_repo_capability", fake_cap)
    rows = await project_service.project_service.list_associations(owner, str(proj_a.id))
    assert rows[0]["can_read"] is True
    assert rows[0]["can_write"] is False


@pytest.mark.asyncio
async def test_source_without_identity_marks_inaccessible(tortoise_db, monkeypatch):
    from monkeycode_compat import git_clients as gc, project_service

    owner = str(uuid.uuid4())
    proj_a = await _make_project(owner, None)  # no git identity on source
    proj_b = await _make_project(owner, None, name="B")
    await project_service.project_service.add_association(owner, str(proj_a.id), str(proj_b.id))

    called = {}

    async def fake_cap(platform, full_name, opts):
        called["hit"] = True
        return (True, True)

    monkeypatch.setattr(gc, "fetch_repo_capability", fake_cap)
    rows = await project_service.project_service.list_associations(owner, str(proj_a.id))
    assert rows[0]["can_read"] is False
    assert "hit" not in called  # never probed without an identity


# ── delete cleanup ──────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_delete_project_cleans_associations(tortoise_db):
    from monkeycode_compat import project_service
    from monkeycode_compat.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj_a = await _make_project(owner, str(ident.id))
    proj_b = await _make_project(owner, str(ident.id), name="B")
    await project_service.project_service.add_association(owner, str(proj_a.id), str(proj_b.id))
    # proj_b also has an association pointing INTO it from A; deleting B must
    # remove that back-reference too.
    assert await ProjectAssociation.filter(target_project_id=proj_b.id).count() == 1
    await project_service.project_service.delete_project(owner, str(proj_b.id))
    assert await ProjectAssociation.filter(target_project_id=proj_b.id).count() == 0
    assert await ProjectAssociation.filter(source_project_id=proj_a.id).count() == 0
