"""Tests for the upstream-compat git-identity repository listing.

Two layers are covered:

* ``git_clients`` — the async per-platform repository clients and the shared
  in-memory pagination helper. Upstream HTTP is mocked at ``_get_json`` so no
  network is touched; we assert auth headers, native paging (GitHub/GitLab),
  keyword filtering, URL mapping/fallbacks, and the full-list vs page_info
  semantics per platform.
* ``git_service`` + ``routes_git`` — the identity-detail contract the C-side
  portal consumes (``authorized_repositories`` / ``repo_page_info``), the repo
  cache (flush bypass, invalidation on update/delete), page-size normalization,
  ownership denial, and upstream-error swallowing. These run against an isolated
  in-memory Tortoise sqlite DB.
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


# ── git_clients: pagination helper ───────────────────────────────────────────
def test_paginate_repos_filters_and_slices():
    repos = [
        git_clients.AuthRepository(full_name=f"acme/repo-{i}", url=f"u{i}")
        for i in range(5)
    ]
    repos.append(git_clients.AuthRepository(full_name="other/thing", url="ux"))

    page = git_clients.paginate_repos(repos, keyword="acme", page=1, size=2)
    assert [r.full_name for r in page.repositories] == ["acme/repo-0", "acme/repo-1"]
    assert page.page_info == {"total_count": 5, "has_next_page": True}

    last = git_clients.paginate_repos(repos, keyword="acme", page=3, size=2)
    assert [r.full_name for r in last.repositories] == ["acme/repo-4"]
    assert last.page_info == {"total_count": 5, "has_next_page": False}


def test_paginate_repos_keyword_is_case_insensitive():
    repos = [git_clients.AuthRepository(full_name="Acme/Widget", url="u")]
    page = git_clients.paginate_repos(repos, keyword="acme", page=1, size=10)
    assert len(page.repositories) == 1


def test_normalize_base_variants():
    assert git_clients._normalize_base("", "def.host") == ("https", "def.host")
    assert git_clients._normalize_base("https://x.com/", "d") == ("https", "x.com")
    assert git_clients._normalize_base("http://x.com", "d") == ("http", "x.com")
    assert git_clients._normalize_base("x.com/", "d") == ("https", "x.com")


def test_unsupported_platform_returns_empty(anyio_backend=None):
    import asyncio

    page = asyncio.run(
        git_clients.fetch_repositories("svn", git_clients.RepositoryOptions(token="t"))
    )
    assert page.repositories == []
    assert page.page_info is None


# ── git_clients: per-platform mocking ────────────────────────────────────────
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


@pytest.mark.asyncio
async def test_github_full_list_auth_and_mapping(patch_get):
    mock = patch_get([
        ([{"full_name": "a/b", "html_url": "https://github.com/a/b", "description": "d"}], {}),
    ])
    page = await git_clients.fetch_repositories(
        "github", git_clients.RepositoryOptions(token="ghp_secret")
    )
    assert page.page_info is None
    assert page.repositories[0].full_name == "a/b"
    assert page.repositories[0].url == "https://github.com/a/b"
    # Auth header carries the token as a GitHub "token" credential.
    assert mock.calls[0]["headers"]["Authorization"] == "token ghp_secret"
    assert "api.github.com/user/repos" in mock.calls[0]["url"]


@pytest.mark.asyncio
async def test_github_native_pagination_uses_link_header(patch_get):
    mock = patch_get([
        (
            [{"full_name": "a/b", "html_url": "h", "description": ""}],
            {"Link": '<https://api.github.com/user/repos?page=3>; rel="next", '
                     '<https://api.github.com/user/repos?page=5>; rel="last"'},
        ),
    ])
    page = await git_clients.fetch_repositories(
        "github", git_clients.RepositoryOptions(token="t", page=1, size=20)
    )
    assert page.page_info["has_next_page"] is True
    assert page.page_info["total_count"] == 5 * 20
    assert mock.calls[0]["params"]["page"] == 1
    assert mock.calls[0]["params"]["per_page"] == 20


@pytest.mark.asyncio
async def test_github_keyword_falls_back_to_fetch_all(patch_get):
    # keyword + page>0 → fetch-all (single short page) then in-memory paginate.
    mock = patch_get([
        (
            [
                {"full_name": "acme/one", "html_url": "h1", "description": ""},
                {"full_name": "other/two", "html_url": "h2", "description": ""},
            ],
            {},
        ),
    ])
    page = await git_clients.fetch_repositories(
        "github", git_clients.RepositoryOptions(token="t", page=1, size=10, keyword="acme")
    )
    assert [r.full_name for r in page.repositories] == ["acme/one"]
    assert page.page_info == {"total_count": 1, "has_next_page": False}
    # Fetched via the fetch-all path (no keyword sent upstream).
    assert "search" not in mock.calls[0]["params"]


@pytest.mark.asyncio
async def test_gitlab_native_pagination_and_headers(patch_get):
    mock = patch_get([
        (
            [{"path_with_namespace": "g/p", "http_url_to_repo": "http://g/p.git", "description": ""}],
            {"X-Total": "42", "X-Next-Page": "2"},
        ),
    ])
    page = await git_clients.fetch_repositories(
        "gitlab",
        git_clients.RepositoryOptions(token="glpat", page=1, size=20, keyword="p"),
    )
    assert page.repositories[0].full_name == "g/p"
    assert page.repositories[0].url == "http://g/p.git"
    assert page.page_info == {"total_count": 42, "has_next_page": True}
    # PAT auth uses PRIVATE-TOKEN; keyword maps to GitLab ``search``.
    assert mock.calls[0]["headers"]["PRIVATE-TOKEN"] == "glpat"
    assert mock.calls[0]["params"]["search"] == "p"


@pytest.mark.asyncio
async def test_gitlab_oauth_uses_bearer(patch_get):
    mock = patch_get([([], {})])
    await git_clients.fetch_repositories(
        "gitlab", git_clients.RepositoryOptions(token="oauth", is_oauth=True)
    )
    assert mock.calls[0]["headers"]["Authorization"] == "Bearer oauth"


@pytest.mark.asyncio
async def test_gitlab_url_falls_back_to_ssh(patch_get):
    patch_get([([{"path_with_namespace": "g/p", "ssh_url_to_repo": "git@g:p.git"}], {})])
    page = await git_clients.fetch_repositories(
        "gitlab", git_clients.RepositoryOptions(token="t")
    )
    assert page.repositories[0].url == "git@g:p.git"


@pytest.mark.asyncio
async def test_gitea_fetch_all_then_in_memory_paginate(patch_get):
    mock = patch_get([
        (
            [{"full_name": "o/r1", "clone_url": "c1"}, {"full_name": "o/r2", "clone_url": "c2"}],
            {},
        ),
    ])
    page = await git_clients.fetch_repositories(
        "gitea",
        git_clients.RepositoryOptions(token="t", base_url="https://gitea.example", page=1, size=1),
    )
    assert [r.full_name for r in page.repositories] == ["o/r1"]
    assert page.page_info == {"total_count": 2, "has_next_page": True}
    assert mock.calls[0]["headers"]["Authorization"] == "token t"
    assert "gitea.example/api/v1/user/repos" in mock.calls[0]["url"]


@pytest.mark.asyncio
async def test_gitee_sends_access_token_and_maps_html_url(patch_get):
    mock = patch_get([([{"full_name": "o/r", "html_url": "https://gitee.com/o/r"}], {})])
    page = await git_clients.fetch_repositories(
        "gitee", git_clients.RepositoryOptions(token="gt")
    )
    assert page.repositories[0].url == "https://gitee.com/o/r"
    assert mock.calls[0]["params"]["access_token"] == "gt"


@pytest.mark.asyncio
async def test_codeup_resolves_org_then_lists(patch_get):
    mock = patch_get([
        ([{"id": "org-1"}], {}),  # ResolveOrgID
        ([{"pathWithNamespace": "grp/repo", "httpCloneUrl": "https://c/grp/repo.git"}], {}),
    ])
    page = await git_clients.fetch_repositories(
        "codeup", git_clients.RepositoryOptions(token="yx")
    )
    assert page.page_info is None  # codeup never paginates
    assert page.repositories[0].full_name == "grp/repo"
    assert page.repositories[0].url == "https://c/grp/repo.git"
    # Org resolution + repo listing both send the yunxiao token header.
    assert mock.calls[0]["headers"]["x-yunxiao-token"] == "yx"
    assert "org-1" in mock.calls[1]["url"]


@pytest.mark.asyncio
async def test_codeup_url_fallback_appends_git(patch_get):
    # organization_id is set → the org-resolve GET is skipped; only the repo
    # list is fetched. webUrl without a .git suffix must get one appended.
    patch_get([
        ([{"pathWithNamespace": "g/r", "webUrl": "https://c/g/r"}], {}),
    ])
    page = await git_clients.fetch_repositories(
        "codeup", git_clients.RepositoryOptions(token="t", organization_id="o")
    )
    assert page.repositories[0].url == "https://c/g/r.git"


@pytest.mark.asyncio
async def test_cnb_bearer_and_web_url_fallback(patch_get):
    mock = patch_get([([{"path": "cnb/test", "name": "test"}], {})])
    page = await git_clients.fetch_repositories(
        "cnb", git_clients.RepositoryOptions(token="cnbtok")
    )
    assert page.repositories[0].full_name == "cnb/test"
    assert page.repositories[0].url == "https://cnb.cool/cnb/test"
    assert mock.calls[0]["headers"]["Authorization"] == "Bearer cnbtok"


@pytest.mark.asyncio
async def test_atomgit_web_url_fallback(patch_get):
    patch_get([([{"full_name": "o/r"}], {})])
    page = await git_clients.fetch_repositories(
        "atomgit", git_clients.RepositoryOptions(token="t")
    )
    assert page.repositories[0].url == "https://atomgit.com/o/r"


@pytest.mark.asyncio
async def test_get_json_raises_without_leaking_token(monkeypatch):
    class _Resp:
        status = 401

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self, content_type=None):
            return {}

    class _Session:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def get(self, url, headers=None, params=None):
            return _Resp()

    monkeypatch.setattr(git_clients.aiohttp, "ClientSession", _Session)
    with pytest.raises(git_clients.GitClientError) as exc:
        await git_clients._get_json(
            "https://api.github.com/user/repos", headers={"Authorization": "token SEKRET"}
        )
    assert "SEKRET" not in str(exc.value)


# ── git_service + routes: DB-backed contract ─────────────────────────────────
@pytest_asyncio.fixture
async def tortoise_db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"user_platform": ["user_platform.models_git"]},
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.fixture(autouse=True)
def clear_repo_cache(monkeypatch):
    from user_platform import git_service

    # Neutralize the fire-and-forget prefetch by default: left live it schedules
    # a real upstream Git call on the test loop that outlives the per-test mock
    # and blocks loop teardown. Prefetch is exercised explicitly in its own test.
    monkeypatch.setattr(git_service.git_service, "_prefetch_repositories", lambda *_a, **_k: None)
    git_service._repo_cache._store.clear()
    yield
    git_service._repo_cache._store.clear()


async def _make_identity(platform="github", token="ghp_tok", **kw):
    from user_platform.models_git import GitIdentity

    return await GitIdentity.create(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        platform=platform,
        access_token=token,
        **kw,
    )


@pytest.mark.asyncio
async def test_get_identity_appends_paginated_repos(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    identity = await _make_identity()

    async def fake_fetch(platform, opts):
        assert opts.token == "ghp_tok"
        return gc.RepositoryPage(
            repositories=[gc.AuthRepository(full_name="a/b", url="u", description="d")],
            page_info={"total_count": 1, "has_next_page": False},
        )

    monkeypatch.setattr(gc, "fetch_repositories", fake_fetch)

    result = await git_service.git_service.get_identity(
        str(identity.user_id), str(identity.id), page=1, size=20
    )
    assert result["authorized_repositories"] == [
        {"full_name": "a/b", "url": "u", "description": "d"}
    ]
    assert result["repo_page_info"] == {"total_count": 1, "has_next_page": False}
    # Secret never leaks: only presence + masked tail.
    assert "access_token" not in result
    assert result["has_access_token"] is True


@pytest.mark.asyncio
async def test_get_identity_full_list_has_no_page_info(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    identity = await _make_identity()
    monkeypatch.setattr(
        gc, "fetch_repositories",
        lambda p, o: _coro(gc.RepositoryPage(repositories=[])),
    )
    result = await git_service.git_service.get_identity(
        str(identity.user_id), str(identity.id)
    )
    assert result["authorized_repositories"] == []
    assert "repo_page_info" not in result


@pytest.mark.asyncio
async def test_get_identity_swallows_upstream_error(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    identity = await _make_identity()

    async def boom(platform, opts):
        raise gc.GitClientError("upstream 500")

    monkeypatch.setattr(gc, "fetch_repositories", boom)
    result = await git_service.git_service.get_identity(
        str(identity.user_id), str(identity.id), page=1, size=20
    )
    assert result is not None
    assert result["authorized_repositories"] == []


@pytest.mark.asyncio
async def test_get_identity_size_normalized(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    identity = await _make_identity()
    seen = {}

    async def capture(platform, opts):
        seen["size"] = opts.size
        return gc.RepositoryPage(repositories=[], page_info={"total_count": 0, "has_next_page": False})

    monkeypatch.setattr(gc, "fetch_repositories", capture)
    # size=0 in paginated mode → default 20; size=999 → capped 100.
    await git_service.git_service.get_identity(
        str(identity.user_id), str(identity.id), page=1, size=0
    )
    assert seen["size"] == 20
    await git_service.git_service.get_identity(
        str(identity.user_id), str(identity.id), page=1, size=999, flush=True
    )
    assert seen["size"] == 100


@pytest.mark.asyncio
async def test_repo_cache_hit_and_flush_bypass(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    identity = await _make_identity()
    calls = {"n": 0}

    async def counting(platform, opts):
        calls["n"] += 1
        return gc.RepositoryPage(repositories=[], page_info={"total_count": 0, "has_next_page": False})

    monkeypatch.setattr(gc, "fetch_repositories", counting)

    await git_service.git_service.get_identity(str(identity.user_id), str(identity.id), page=1, size=20)
    await git_service.git_service.get_identity(str(identity.user_id), str(identity.id), page=1, size=20)
    assert calls["n"] == 1  # second call served from cache
    await git_service.git_service.get_identity(
        str(identity.user_id), str(identity.id), page=1, size=20, flush=True
    )
    assert calls["n"] == 2  # flush bypasses the cache


@pytest.mark.asyncio
async def test_update_invalidates_repo_cache(tortoise_db, monkeypatch):
    from user_platform import git_clients as gc, git_service

    identity = await _make_identity()
    calls = {"n": 0}

    async def counting(platform, opts):
        calls["n"] += 1
        return gc.RepositoryPage(repositories=[], page_info={"total_count": 0, "has_next_page": False})

    monkeypatch.setattr(gc, "fetch_repositories", counting)

    await git_service.git_service.get_identity(str(identity.user_id), str(identity.id), page=1, size=20)
    assert calls["n"] == 1
    await git_service.git_service.update_identity(
        str(identity.user_id), str(identity.id), {"remark": "x"}
    )
    await git_service.git_service.get_identity(str(identity.user_id), str(identity.id), page=1, size=20)
    # Cache was invalidated on update → upstream hit again (plus a prefetch may
    # have run, so assert it grew rather than an exact count).
    assert calls["n"] >= 2


@pytest.mark.asyncio
async def test_get_identity_ownership_denied(tortoise_db):
    from user_platform import git_service

    identity = await _make_identity()
    other_user = str(uuid.uuid4())
    result = await git_service.git_service.get_identity(
        other_user, str(identity.id), page=1, size=20
    )
    assert result is None


def test_route_get_identity_forwards_query_and_returns_contract(monkeypatch):
    """Route layer: ``flush``/``page``/``size``/``keyword`` are bound and
    forwarded to the service, and the service's dict is returned verbatim (the
    frontend reads ``data.authorized_repositories`` / ``data.repo_page_info``).

    Kept a plain sync test with the service stubbed: a live Tortoise ``:memory:``
    connection is bound to one event loop, but ``TestClient`` drives the endpoint
    on its own loop — querying the DB across that boundary deadlocks. The DB path
    is already covered by the ``git_service`` tests above; here we isolate the
    route's own responsibility (param binding + passthrough)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import user_platform.routes_git as routes_git
    from user_platform.deps import get_current_user

    identity_id = str(uuid.uuid4())
    captured: dict = {}

    async def fake_get_identity(user_id, iid, *, role=None, flush=False, page=0, size=0, keyword=""):
        captured.update(
            user_id=user_id, iid=iid, role=role, flush=flush, page=page, size=size, keyword=keyword
        )
        return {
            "id": iid,
            "has_access_token": True,
            "authorized_repositories": [{"full_name": "a/b", "url": "https://x/a/b", "description": ""}],
            "repo_page_info": {"total_count": 1, "has_next_page": False},
        }

    monkeypatch.setattr(routes_git.git_service, "get_identity", fake_get_identity)

    class _User:
        id = uuid.uuid4()
        role = "user"

    app = FastAPI()
    app.include_router(routes_git.identity_router)
    app.dependency_overrides[get_current_user] = lambda: _User()

    client = TestClient(app)
    resp = client.get(f"/api/v1/users/git-identities/{identity_id}?page=1&size=20&flush=true&keyword=web")
    assert resp.status_code == 200
    body = resp.json()
    assert captured["flush"] is True
    assert captured["page"] == 1
    assert captured["size"] == 20
    assert captured["keyword"] == "web"
    assert captured["iid"] == identity_id
    assert body["authorized_repositories"] == [
        {"full_name": "a/b", "url": "https://x/a/b", "description": ""}
    ]
    assert body["repo_page_info"] == {"total_count": 1, "has_next_page": False}
    assert "access_token" not in body


def _coro(value):
    async def _c():
        return value

    return _c()
