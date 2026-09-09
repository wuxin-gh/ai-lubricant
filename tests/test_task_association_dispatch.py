"""Gateway-side association dispatch: env-carried dep clones + credential note.

Covers ``TaskService._build_association_env`` (the bridge from
``mc_project_associations`` to ``NodeCreateSession.env``), the scoped/mode-bearing
proxy token, the workspace-subdir sanitizer, and the per-turn git-credential note.
No node or control plane is involved — these are pure gateway units.
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from user_platform import task_service as task_service_module
from user_platform.task_service import TaskService, _safe_subdir


ORIGIN = "http://127.0.0.1:8003"
CONTROL_TOKEN = "control-secret"


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
def proxy_configured(monkeypatch):
    monkeypatch.setattr(task_service_module, "_git_proxy_origin", lambda: ORIGIN)
    monkeypatch.setattr(task_service_module, "_git_proxy_control_token", lambda: CONTROL_TOKEN)


@pytest.fixture(autouse=True)
def neutralize_prefetch(monkeypatch):
    from user_platform import git_service

    monkeypatch.setattr(
        git_service.git_service, "_prefetch_repositories", lambda *_a, **_k: None
    )


async def _make_identity(user_id):
    from user_platform.models_git import GitIdentity

    return await GitIdentity.create(
        id=uuid.uuid4(), user_id=uuid.UUID(user_id), platform="github", access_token="ghp_tok"
    )


async def _make_project(user_id, identity_id, name, repo_url, branch=None):
    from user_platform.models_project import Project

    return await Project.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        name=name,
        platform="github",
        repo_url=repo_url,
        branch=branch,
        git_identity_id=identity_id,
    )


def _task(task_id):
    return SimpleNamespace(id=task_id)


# ── _safe_subdir ────────────────────────────────────────────────────────────
def test_safe_subdir_blocks_traversal_and_separators():
    assert _safe_subdir("..") == "dep"
    assert _safe_subdir("../../etc") == "etc"          # separators dropped, no traversal left
    assert "/" not in _safe_subdir("a/b")
    assert "\\" not in _safe_subdir("a\\b")
    assert _safe_subdir("") == "dep"
    assert _safe_subdir(None) == "dep"


def test_safe_subdir_keeps_plain_names():
    assert _safe_subdir("Ai-Lubricant_v2.0") == "Ai-Lubricant_v2.0"
    assert _safe_subdir("my project") == "my-project"


# ── proxy token shape ───────────────────────────────────────────────────────
def test_proxy_token_carries_scope_and_mode():
    tid = str(uuid.uuid4())
    token = task_service_module._git_proxy_token(tid, "main", "rw")
    parts = token.split(".")
    assert len(parts) == 4
    assert parts[0] == tid
    assert parts[1:3] == ["main", "rw"]


def test_proxy_token_signature_depends_on_scope_and_mode():
    tid = str(uuid.uuid4())
    other = str(uuid.uuid4())
    ro = task_service_module._git_proxy_token(tid, "main", "ro").split(".")[-1]
    rw = task_service_module._git_proxy_token(tid, "main", "rw").split(".")[-1]
    scoped = task_service_module._git_proxy_token(tid, other, "ro").split(".")[-1]
    # Distinct mode and distinct scope both change the signature, so neither can
    # be edited in the clear-text URL segment without breaking verification.
    assert ro != rw
    assert ro != scoped


def test_proxy_token_rejects_unknown_mode():
    tid = str(uuid.uuid4())
    # An unrecognized mode degrades to read-only rather than silently granting rw.
    assert task_service_module._git_proxy_token(tid, "main", "admin") == \
        task_service_module._git_proxy_token(tid, "main", "ro")


# ── _build_association_env ──────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_readable_association_becomes_dep_git_env(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, project_service
    from user_platform.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    src = await _make_project(owner, ident.id, "A", "https://github.com/o/a")
    dep = await _make_project(owner, ident.id, "Dep Lib", "https://github.com/o/dep", branch="develop")
    await ProjectAssociation.create(
        id=uuid.uuid4(), source_project_id=src.id, target_project_id=dep.id, relation="depends_on"
    )

    async def readable_writable(platform, full_name, opts):
        return (True, True)

    monkeypatch.setattr(gc, "fetch_repo_capability", readable_writable)

    task_id = uuid.uuid4()
    env = await TaskService()._build_association_env(
        {"extra": {"project_id": str(src.id)}}, _task(task_id)
    )
    assert len(env) == 1
    entry = env[0]
    assert entry["name"] == "AI_LUBRICANT_DEP_GIT_0"
    assert entry["secret"] is True
    spec = json.loads(entry["value"])
    assert spec["subdir"] == "Dep-Lib"
    assert spec["branch"] == "develop"
    assert spec["mode"] == "rw"
    # The URL is the proxy, scoped to the TARGET project id — never the real repo.
    assert "github.com" not in spec["url"]
    assert spec["url"].startswith("http://")
    userinfo = spec["url"].split("://", 1)[1].split("@", 1)[0]
    assert userinfo == task_service_module._git_proxy_token(str(task_id), str(dep.id), "rw")


@pytest.mark.asyncio
async def test_read_only_association_gets_ro_mode(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc
    from user_platform.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    src = await _make_project(owner, ident.id, "A", "https://github.com/o/a")
    dep = await _make_project(owner, ident.id, "B", "https://github.com/o/b")
    await ProjectAssociation.create(
        id=uuid.uuid4(), source_project_id=src.id, target_project_id=dep.id
    )

    async def read_only(platform, full_name, opts):
        return (True, False)

    monkeypatch.setattr(gc, "fetch_repo_capability", read_only)

    env = await TaskService()._build_association_env(
        {"extra": {"project_id": str(src.id)}}, _task(uuid.uuid4())
    )
    spec = json.loads(env[0]["value"])
    assert spec["mode"] == "ro"


@pytest.mark.asyncio
async def test_unreadable_association_becomes_placeholder(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc
    from user_platform.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    src = await _make_project(owner, ident.id, "A", "https://github.com/o/a")
    dep = await _make_project(owner, ident.id, "Secret Repo", "https://github.com/o/secret")
    await ProjectAssociation.create(
        id=uuid.uuid4(), source_project_id=src.id, target_project_id=dep.id
    )

    async def unreadable(platform, full_name, opts):
        return (False, False)

    monkeypatch.setattr(gc, "fetch_repo_capability", unreadable)

    env = await TaskService()._build_association_env(
        {"extra": {"project_id": str(src.id)}}, _task(uuid.uuid4())
    )
    assert len(env) == 1
    entry = env[0]
    assert entry["name"] == "AI_LUBRICANT_DEP_PLACEHOLDER_0"
    assert entry["secret"] is False
    # Name + subdir only — no repo URL and no token for a repo we cannot read.
    assert entry["value"] == "Secret Repo|Secret-Repo"
    assert "http" not in entry["value"]


@pytest.mark.asyncio
async def test_probe_failure_degrades_to_placeholder(tortoise_db, monkeypatch):
    """A capability probe that raises must not abort dispatch."""
    from user_platform import git_clients as gc
    from user_platform.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    src = await _make_project(owner, ident.id, "A", "https://github.com/o/a")
    dep = await _make_project(owner, ident.id, "B", "https://github.com/o/b")
    await ProjectAssociation.create(
        id=uuid.uuid4(), source_project_id=src.id, target_project_id=dep.id
    )

    async def boom(platform, full_name, opts):
        raise RuntimeError("upstream exploded")

    monkeypatch.setattr(gc, "fetch_repo_capability", boom)

    env = await TaskService()._build_association_env(
        {"extra": {"project_id": str(src.id)}}, _task(uuid.uuid4())
    )
    assert env[0]["name"] == "AI_LUBRICANT_DEP_PLACEHOLDER_0"


@pytest.mark.asyncio
async def test_explicit_subdir_is_used_and_sanitized(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc
    from user_platform.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    src = await _make_project(owner, ident.id, "A", "https://github.com/o/a")
    dep = await _make_project(owner, ident.id, "B", "https://github.com/o/b")
    await ProjectAssociation.create(
        id=uuid.uuid4(), source_project_id=src.id, target_project_id=dep.id,
        target_subdir="../escape",
    )

    async def readable(platform, full_name, opts):
        return (True, False)

    monkeypatch.setattr(gc, "fetch_repo_capability", readable)

    env = await TaskService()._build_association_env(
        {"extra": {"project_id": str(src.id)}}, _task(uuid.uuid4())
    )
    spec = json.loads(env[0]["value"])
    assert ".." not in spec["subdir"]
    assert "/" not in spec["subdir"]


@pytest.mark.asyncio
async def test_no_project_or_no_associations_yields_empty(tortoise_db):
    service = TaskService()
    assert await service._build_association_env({}, _task(uuid.uuid4())) == []
    assert await service._build_association_env(
        {"extra": {"project_id": str(uuid.uuid4())}}, _task(uuid.uuid4())
    ) == []


@pytest.mark.asyncio
async def test_unconfigured_proxy_yields_empty(tortoise_db, monkeypatch):
    """Without a proxy origin/control token there is no way to mint dep URLs."""
    from user_platform.models_project import ProjectAssociation

    owner = str(uuid.uuid4())
    ident = await _make_identity(owner)
    src = await _make_project(owner, ident.id, "A", "https://github.com/o/a")
    dep = await _make_project(owner, ident.id, "B", "https://github.com/o/b")
    await ProjectAssociation.create(
        id=uuid.uuid4(), source_project_id=src.id, target_project_id=dep.id
    )
    monkeypatch.setattr(task_service_module, "_git_proxy_control_token", lambda: "")
    env = await TaskService()._build_association_env(
        {"extra": {"project_id": str(src.id)}}, _task(uuid.uuid4())
    )
    assert env == []


# ── credential note ─────────────────────────────────────────────────────────
def test_credential_note_prefixes_content():
    out = task_service_module._with_git_credential_note("fix the bug")
    assert out.endswith("fix the bug")
    assert out.startswith(task_service_module._GIT_CREDENTIAL_NOTE)
    # The note tells the agent not to scrape .git/config for a credential.
    assert ".git/config" in out


def test_credential_note_on_empty_content():
    assert task_service_module._with_git_credential_note("") == task_service_module._GIT_CREDENTIAL_NOTE
