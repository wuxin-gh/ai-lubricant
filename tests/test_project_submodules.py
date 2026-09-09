"""Tests for the read-only git-submodule surface (``.gitmodules`` derivation).

Covers the pure ``parse_gitmodules`` / ``_resolve_submodule_url`` helpers and
``project_service.list_submodules``: parse the superproject's ``.gitmodules``,
resolve each relative url against the parent repo, and match it to an existing
platform project the caller can see. Nothing is written to the database.
"""
from __future__ import annotations

import base64
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
            "user_platform": [
                "user_platform.models_git",
                "user_platform.models_project",
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
    from user_platform import git_service

    monkeypatch.setattr(
        git_service.git_service, "_prefetch_repositories", lambda *_a, **_k: None
    )


async def _make_identity(user_id, platform="github", token="ghp_tok", **kw):
    from user_platform.models_git import GitIdentity

    return await GitIdentity.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        platform=platform,
        access_token=token,
        **kw,
    )


async def _make_project(user_id, identity_id, name="proj", repo_url="https://github.com/owner/repo", **kw):
    from user_platform.models_project import Project

    return await Project.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        name=name,
        platform="github",
        repo_url=repo_url,
        git_identity_id=uuid.UUID(identity_id) if identity_id else None,
        **kw,
    )


# The real shape used by this very repository (5 sections, Tab-indented lines,
# one ``branch`` key on the last section).
_REAL_GITMODULES = """[submodule "node_server"]
\tpath = node_server
\turl = ../ai-lubricant-node-server.git
[submodule "nodes"]
\tpath = nodes
\turl = ../ai-lubricant-nodes.git
[submodule "user-frontend"]
\tpath = user-frontend
\turl = ../ai-lubricant-user-frontend.git
[submodule "mobile"]
\tpath = mobile
\turl = ../ai-lubricant-mobile.git
[submodule "device-control"]
\tpath = device-control
\turl = ../ai-lubricant-device-control.git
\tbranch = master
"""


# ── parse_gitmodules ───────────────────────────────────────────────────────
def test_parse_gitmodules_real_shape():
    from user_platform.project_service import parse_gitmodules

    rows = parse_gitmodules(_REAL_GITMODULES)
    assert len(rows) == 5
    assert rows[0] == {
        "name": "node_server",
        "path": "node_server",
        "url": "../ai-lubricant-node-server.git",
        "branch": "",
    }
    assert rows[4]["path"] == "device-control"
    assert rows[4]["branch"] == "master"


def test_parse_gitmodules_drops_incomplete_and_ignores_non_submodule_sections():
    from user_platform.project_service import parse_gitmodules

    text = (
        '[submodule "a"]\n\tpath = a\n\turl = ../a.git\n'
        '[submodule "missing-url"]\n\tpath = b\n'
        '[core]\n\tbare = false\n'
    )
    rows = parse_gitmodules(text)
    assert [r["path"] for r in rows] == ["a"]
    assert parse_gitmodules("") == []


# ── _resolve_submodule_url ─────────────────────────────────────────────────
def test_resolve_submodule_url_relative_and_absolute():
    from user_platform.project_service import _resolve_submodule_url

    parent = "https://gitee.com/xiongrun/ai-lubricant.git"
    assert _resolve_submodule_url(parent, "../ai-lubricant-nodes.git") == (
        "https://gitee.com/xiongrun/ai-lubricant-nodes.git"
    )
    # git treats the superproject url as a directory: ``./`` appends to it,
    # ``../`` pops one component off it.
    assert _resolve_submodule_url(parent, "./sub.git") == (
        "https://gitee.com/xiongrun/ai-lubricant.git/sub.git"
    )
    assert _resolve_submodule_url(parent, "../../a/b.git") == "https://gitee.com/a/b.git"
    absolute = "https://github.com/org/repo.git"
    assert _resolve_submodule_url(parent, absolute) == absolute


# ── list_submodules ────────────────────────────────────────────────────────
def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


async def _stub_blob(monkeypatch, module, text: str):
    """Stub ``git_clients.fetch_blob`` so _resolve_submodule_map sees the
    .gitmodules content without network access.

    _resolve_submodule_map must NOT go through get_blob（那会因 path 非空
    再触发子模块识别，死递归），所以这里的桩打在 git_clients 层。
    """

    async def fake_fetch_blob(platform, full_name, opts, *, path, ref=""):
        from user_platform.git_clients import Blob

        if path == ".gitmodules":
            return Blob(content=_b64(text), is_binary=False, sha="x", size=len(text))
        return None

    monkeypatch.setattr(module.git_clients, "fetch_blob", fake_fetch_blob)


@pytest.mark.asyncio
async def test_list_submodules_resolves_and_matches_projects(tortoise_db, monkeypatch):
    from user_platform import project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    parent = await _make_project(
        owner, str(ident.id), name="ai-lubricant",
        repo_url="https://gitee.com/xiongrun/ai-lubricant.git",
    )
    nodes_proj = await _make_project(
        owner, str(ident.id), name="nodes",
        repo_url="https://gitee.com/xiongrun/ai-lubricant-nodes.git",
    )

    await _stub_blob(monkeypatch, project_service, _REAL_GITMODULES)

    rows = await project_service.project_service.list_submodules(owner, str(parent.id))
    assert rows is not None and len(rows) == 5
    by_path = {r["path"]: r for r in rows}
    # Resolved relative url against the parent's host/owner.
    assert by_path["nodes"]["url"] == "https://gitee.com/xiongrun/ai-lubricant-nodes.git"
    assert by_path["device-control"]["branch"] == "master"
    # Only the project registered in the platform is linked.
    assert by_path["nodes"]["project_id"] == str(nodes_proj.id)
    assert by_path["nodes"]["project_name"] == "nodes"
    assert "project_id" not in by_path["mobile"]


@pytest.mark.asyncio
async def test_list_submodules_empty_when_no_gitmodules(tortoise_db, monkeypatch):
    from user_platform import project_service

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj = await _make_project(owner, str(ident.id))

    await _stub_blob(monkeypatch, project_service, "")
    rows = await project_service.project_service.list_submodules(owner, str(proj.id))
    assert rows == []


@pytest.mark.asyncio
async def test_list_submodules_inaccessible_returns_none(tortoise_db):
    from user_platform import project_service

    owner = str(uuid.uuid4())
    other = str(uuid.uuid4())
    ident = await _make_identity(owner)
    proj = await _make_project(owner, str(ident.id))

    rows = await project_service.project_service.list_submodules(other, str(proj.id))
    assert rows is None
