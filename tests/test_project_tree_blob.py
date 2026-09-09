"""Tests for the project detail page's repository read surface.

Backs the README + file-tree + file-preview feature. Three layers:

* ``git_clients`` — the new per-platform branches/tree/blob clients. Upstream
  HTTP is mocked at ``_get_json`` so no network is touched; we assert auth
  headers, URL shapes, FileMode mapping, and base64 content cleaning.
* ``project_service._derive_full_name`` — the ``repo_url`` → ``owner/repo``
  heuristic the frontend relies on to resolve the default branch.
* ``git_service.list_branches`` / ``project_service.get_tree`` / ``get_blob`` +
  the ``routes_git`` branches route — ownership/access enforcement, graceful
  degradation, and the double-encoding unquote on the branches path segment.
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

from user_platform import git_clients
from user_platform.project_service import _derive_full_name


# ── _derive_full_name ─────────────────────────────────────────────────────────
def test_derive_full_name_basic():
    assert _derive_full_name("https://github.com/owner/repo") == "owner/repo"


def test_derive_full_name_strips_git_suffix():
    assert _derive_full_name("https://github.com/owner/repo.git") == "owner/repo"


def test_derive_full_name_preserves_multilevel_namespace():
    assert (
        _derive_full_name("https://gitlab.com/group/sub/repo")
        == "group/sub/repo"
    )


def test_derive_full_name_http_and_trailing_slash():
    assert _derive_full_name("http://gitea.example/o/r/") == "o/r"


def test_derive_full_name_bare_path():
    assert _derive_full_name("owner/repo") == "owner/repo"


def test_derive_full_name_empty():
    assert _derive_full_name(None) == ""
    assert _derive_full_name("") == ""


# ── git_clients: mocking harness ──────────────────────────────────────────────
class _MockGet:
    """Records calls to ``_get_json`` and replays queued ``(body, headers)``."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[dict] = []

    async def __call__(self, url, *, headers, params=None):
        self.calls.append({"url": url, "headers": headers, "params": params or {}})
        if self._responses:
            return self._responses.pop(0)
        return [], {}


@pytest.fixture
def patch_get(monkeypatch):
    def _install(responses):
        mock = _MockGet(responses)
        monkeypatch.setattr(git_clients, "_get_json", mock)
        return mock

    return _install


# ── branches ──────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_github_branches_auth_and_mapping(patch_get):
    mock = patch_get([([{"name": "main"}, {"name": "dev"}], {})])
    branches = await git_clients.fetch_branches(
        "github", "a/b", git_clients.RepositoryOptions(token="ghp_secret")
    )
    assert [b.name for b in branches] == ["main", "dev"]
    assert mock.calls[0]["headers"]["Authorization"] == "token ghp_secret"
    assert "api.github.com/repos/a/b/branches" in mock.calls[0]["url"]


@pytest.mark.asyncio
async def test_gitlab_branches_uses_encoded_project_id(patch_get):
    mock = patch_get([([{"name": "main"}], {})])
    await git_clients.fetch_branches(
        "gitlab", "group/sub/repo", git_clients.RepositoryOptions(token="glpat")
    )
    # Full path is URL-encoded into the project id segment.
    assert "group%2Fsub%2Frepo/repository/branches" in mock.calls[0]["url"]
    assert mock.calls[0]["headers"]["PRIVATE-TOKEN"] == "glpat"


@pytest.mark.asyncio
async def test_fetch_branches_unsupported_platform_empty():
    branches = await git_clients.fetch_branches(
        "svn", "a/b", git_clients.RepositoryOptions(token="t")
    )
    assert branches == []


@pytest.mark.asyncio
async def test_fetch_branches_empty_full_name():
    branches = await git_clients.fetch_branches(
        "github", "", git_clients.RepositoryOptions(token="t")
    )
    assert branches == []


# ── tree ────────────────────────────────────────────────────────────────────--
@pytest.mark.asyncio
async def test_github_tree_maps_dir_and_file_modes(patch_get):
    mock = patch_get([
        (
            [
                {"name": "src", "path": "src", "type": "dir", "sha": "s1"},
                {"name": "README.md", "path": "README.md", "type": "file", "sha": "s2", "size": 42},
            ],
            {},
        ),
    ])
    entries = await git_clients.fetch_tree(
        "github", "a/b", git_clients.RepositoryOptions(token="t"), ref="main", path=""
    )
    by_name = {e.name: e for e in entries}
    assert by_name["src"].mode == git_clients._MODE_DIRECTORY
    assert by_name["README.md"].mode == git_clients._MODE_REGULAR
    assert by_name["README.md"].size == 42
    assert mock.calls[0]["params"]["ref"] == "main"


@pytest.mark.asyncio
async def test_gitlab_tree_maps_type_and_mode(patch_get):
    patch_get([
        (
            [
                {"name": "dir", "path": "dir", "type": "tree", "id": "i1", "mode": "040000"},
                {"name": "f", "path": "f", "type": "blob", "id": "i2", "mode": "100644"},
                {"name": "link", "path": "link", "type": "blob", "id": "i3", "mode": "120000"},
            ],
            {},
        ),
    ])
    entries = await git_clients.fetch_tree(
        "gitlab", "g/p", git_clients.RepositoryOptions(token="t")
    )
    by_name = {e.name: e for e in entries}
    assert by_name["dir"].mode == git_clients._MODE_DIRECTORY
    assert by_name["f"].mode == git_clients._MODE_REGULAR
    assert by_name["link"].mode == git_clients._MODE_SYMLINK


# ── blob ────────────────────────────────────────────────────────────────────--
@pytest.mark.asyncio
async def test_github_blob_cleans_wrapped_base64(patch_get):
    # GitHub wraps base64 at 60 cols with newlines; frontend atob rejects those.
    patch_get([
        (
            {"type": "file", "encoding": "base64", "content": "aGVsbG8=\n", "sha": "s", "size": 5},
            {},
        ),
    ])
    blob = await git_clients.fetch_blob(
        "github", "a/b", git_clients.RepositoryOptions(token="t"), path="README.md"
    )
    assert blob is not None
    assert "\n" not in blob.content
    assert blob.content == "aGVsbG8="


@pytest.mark.asyncio
async def test_github_blob_directory_target_returns_none(patch_get):
    patch_get([([{"type": "dir"}], {})])
    blob = await git_clients.fetch_blob(
        "github", "a/b", git_clients.RepositoryOptions(token="t"), path="src"
    )
    assert blob is None


@pytest.mark.asyncio
async def test_fetch_blob_empty_path_returns_none():
    blob = await git_clients.fetch_blob(
        "github", "a/b", git_clients.RepositoryOptions(token="t"), path=""
    )
    assert blob is None


# ── DB-backed service + route contract ────────────────────────────────────────
@pytest_asyncio.fixture
async def tortoise_db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
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


async def _make_project(user_id, identity_id, **kw):
    from user_platform.models_project import Project

    return await Project.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        name="proj",
        platform="github",
        repo_url="https://github.com/owner/repo",
        git_identity_id=uuid.UUID(identity_id),
        **kw,
    )


@pytest.mark.asyncio
async def test_list_branches_ownership_denied(tortoise_db):
    from user_platform import git_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    other = str(uuid.uuid4())
    result = await git_service.git_service.list_branches(
        other, str(identity.id), "owner/repo"
    )
    assert result is None


@pytest.mark.asyncio
async def test_list_branches_missing_token_returns_empty(tortoise_db, monkeypatch):
    from user_platform import git_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner, token=None)
    result = await git_service.git_service.list_branches(
        owner, str(identity.id), "owner/repo"
    )
    assert result == []


@pytest.mark.asyncio
async def test_list_branches_forwards_and_maps(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)

    async def fake(platform, full_name, opts):
        assert platform == "github"
        assert full_name == "owner/repo"
        assert opts.token == "ghp_tok"
        return [gc.Branch(name="main")]

    monkeypatch.setattr(gc, "fetch_branches", fake)
    result = await git_service.git_service.list_branches(
        owner, str(identity.id), "owner/repo"
    )
    assert result == [{"name": "main"}]


@pytest.mark.asyncio
async def test_get_tree_ownership_denied(tortoise_db):
    from user_platform import project_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id))
    other = str(uuid.uuid4())
    result = await project_service.project_service.get_tree(other, str(project.id))
    assert result is None


@pytest.mark.asyncio
async def test_get_tree_forwards_full_name_and_creds(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, project_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id))

    async def fake(platform, full_name, opts, *, ref, path, recursive):
        assert full_name == "owner/repo"  # derived from repo_url
        assert opts.token == "ghp_tok"
        return [gc.TreeEntry(name="README.md", path="README.md", mode=gc._MODE_REGULAR, sha="s")]

    monkeypatch.setattr(gc, "fetch_tree", fake)
    result = await project_service.project_service.get_tree(
        owner, str(project.id), ref="main"
    )
    assert result == [
        {"name": "README.md", "path": "README.md", "mode": 1, "sha": "s"}
    ]


@pytest.mark.asyncio
async def test_get_tree_no_identity_returns_empty(tortoise_db):
    from user_platform import project_service
    from user_platform.models_project import Project

    owner = str(uuid.uuid4())
    project = await Project.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(owner),
        name="p",
        platform="github",
        repo_url="https://github.com/o/r",
        git_identity_id=None,
    )
    result = await project_service.project_service.get_tree(owner, str(project.id))
    assert result == []


@pytest.mark.asyncio
async def test_get_blob_forwards_and_returns_dict(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, project_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id))

    async def fake(platform, full_name, opts, *, path, ref):
        assert path == "README.md"
        return gc.Blob(content="aGk=", is_binary=False, sha="s", size=2)

    monkeypatch.setattr(gc, "fetch_blob", fake)
    result = await project_service.project_service.get_blob(
        owner, str(project.id), path="README.md"
    )
    assert result["content"] == "aGk="
    assert result["sha"] == "s"


@pytest.mark.asyncio
async def test_get_blob_ownership_denied(tortoise_db):
    from user_platform import project_service

    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id))
    other = str(uuid.uuid4())
    result = await project_service.project_service.get_blob(
        other, str(project.id), path="README.md"
    )
    assert result is None


def test_route_raw_blob_returns_image_bytes(monkeypatch):
    import base64

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import user_platform.routes_project as routes_project
    from user_platform.deps import get_current_user

    png = b"\x89PNG\r\n\x1a\nfixture"
    captured: dict = {}

    async def fake_get_blob(user_id, project_id, *, role=None, path, ref=""):
        captured.update(user_id=user_id, project_id=project_id, role=role, path=path, ref=ref)
        return {"content": base64.b64encode(png).decode(), "is_binary": True}

    monkeypatch.setattr(routes_project.project_service, "get_blob", fake_get_blob)

    class _User:
        id = uuid.uuid4()
        role = "user"

    app = FastAPI()
    app.include_router(routes_project.router)
    app.dependency_overrides[get_current_user] = lambda: _User()
    response = TestClient(app).get(
        "/api/v1/users/projects/project-1/tree/blob/raw",
        params={"path": "frontend/public/logo-dark.png", "ref": "main"},
    )

    assert response.status_code == 200
    assert response.content == png
    assert response.headers["content-type"] == "image/png"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert captured["path"] == "frontend/public/logo-dark.png"
    assert captured["ref"] == "main"


def test_route_raw_blob_rejects_active_content(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import user_platform.routes_project as routes_project
    from user_platform.deps import get_current_user

    class _User:
        id = uuid.uuid4()
        role = "user"

    app = FastAPI()
    app.include_router(routes_project.router)
    app.dependency_overrides[get_current_user] = lambda: _User()
    response = TestClient(app).get(
        "/api/v1/users/projects/project-1/tree/blob/raw",
        params={"path": "logo.svg", "ref": "main"},
    )

    assert response.status_code == 415


def test_route_branches_unquotes_double_encoded_repo(monkeypatch):
    """Route layer: the frontend encodes ``owner/repo`` once and the request
    layer encodes again, so the path segment arrives double-encoded. The route
    must ``unquote`` once to recover ``owner/repo`` before hitting the service.

    Kept a sync test with the service stubbed (same rationale as the git-identity
    route test): a live ``:memory:`` DB is bound to one loop while TestClient
    drives its own. The DB path is covered by the service tests above."""
    from urllib.parse import quote

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import user_platform.routes_git as routes_git
    from user_platform.deps import get_current_user

    identity_id = str(uuid.uuid4())
    captured: dict = {}

    async def fake_list_branches(user_id, iid, repo_full_name, *, role=None):
        captured.update(iid=iid, repo_full_name=repo_full_name, role=role)
        return [{"name": "main"}]

    monkeypatch.setattr(routes_git.git_service, "list_branches", fake_list_branches)

    class _User:
        id = uuid.uuid4()
        role = "user"

    app = FastAPI()
    app.include_router(routes_git.identity_router)
    app.dependency_overrides[get_current_user] = lambda: _User()

    client = TestClient(app)
    # Frontend does encodeURIComponent("owner/repo") -> "owner%2Frepo"; the request
    # layer encodes once more -> "owner%252Frepo" in the actual URL.
    once = quote("owner/repo", safe="")  # owner%2Frepo
    twice = quote(once, safe="")  # owner%252Frepo
    resp = client.get(
        f"/api/v1/users/git-identities/{identity_id}/{twice}/branches"
    )
    assert resp.status_code == 200
    assert resp.json() == [{"name": "main"}]
    # Route unquoted exactly once: recovers owner%2Frepo → owner/repo.
    assert captured["repo_full_name"] == "owner/repo"
    assert captured["iid"] == identity_id
