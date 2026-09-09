"""Webhook platform-client contract tests.

Validates that the four supported platforms (github/gitlab/gitea/gitee) build
the right URL, headers, and JSON body for list/create/update/delete — without
touching the network. Tokens must never leak into raised messages.
"""
from __future__ import annotations

import uuid

import pytest

from user_platform import git_clients


def _opts(token="ghp_SEKRET", base_url="", **kw):
    return git_clients.RepositoryOptions(token=token, base_url=base_url, **kw)


class _Resp:
    """Minimal aiohttp response shim recording the request for assertions."""

    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status
        self.captured = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        # _post_json/_patch_json read text() first and only fall through to
        # json() when it's truthy, so return a non-empty JSON string.
        import json as _json
        return _json.dumps(self._payload)


def _install_session(monkeypatch, handler):
    """Install a fake aiohttp ClientSession that routes to ``handler(method, url, headers, params, json_body)``."""

    class _Session:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def _req(self, method):
            def go(url, headers=None, params=None, json=None):
                resp = _Resp(handler(method, url, headers or {}, params, json))
                resp.captured = (method, url, headers, params, json)
                return resp
            return go

        get = lambda self, *a, **k: self._req("GET")(*a, **k)
        post = lambda self, *a, **k: self._req("POST")(*a, **k)
        patch = lambda self, *a, **k: self._req("PATCH")(*a, **k)
        delete = lambda self, *a, **k: self._req("DELETE")(*a, **k)

    monkeypatch.setattr(git_clients.aiohttp, "ClientSession", _Session)


# ── GitHub ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_github_create_webhook_payload(monkeypatch):
    captured = {}

    def handler(method, url, headers, params, json):
        captured.update(method=method, url=url, headers=headers, body=json)
        return {"id": 42, "active": True, "events": ["push"], "config": {"url": "http://cb"}}

    _install_session(monkeypatch, handler)
    wh = await git_clients.create_webhook(
        "github", "acme/widget", _opts(),
        url="http://host/cb", secret="s", events=["push", "pull_request"], active=True,
    )
    assert wh.hook_id == "42"
    assert captured["url"].endswith("/repos/acme/widget/hooks")
    assert captured["body"]["name"] == "web"
    assert captured["body"]["config"]["url"] == "http://host/cb"
    assert captured["body"]["config"]["secret"] == "s"
    assert captured["body"]["events"] == ["push", "pull_request"]
    assert captured["headers"]["Authorization"] == "token ghp_SEKRET"


@pytest.mark.asyncio
async def test_github_delete_webhook_uses_hook_id(monkeypatch):
    captured = {}

    def handler(method, url, headers, params, json):
        captured.update(method=method, url=url)
        return {"_status": 204}

    _install_session(monkeypatch, handler)
    status = await git_clients.delete_webhook(
        "github", "acme/widget", _opts(), hook_id="42"
    )
    assert captured["method"] == "DELETE"
    assert captured["url"].endswith("/repos/acme/widget/hooks/42")
    assert status == 200


# ── GitLab ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_gitlab_create_webhook_encodes_project(monkeypatch):
    captured = {}

    def handler(method, url, headers, params, json):
        captured.update(method=method, url=url, headers=headers, body=json)
        return {"id": 7, "url": "http://cb", "active": True}

    _install_session(monkeypatch, handler)
    wh = await git_clients.create_webhook(
        "gitlab", "group/sub/proj", _opts(is_oauth=False),
        url="http://host/cb", secret="s", events=["push", "merge_request"], active=True,
    )
    assert wh.hook_id == "7"
    # full_name is URL-encoded into the project path.
    assert "/api/v4/projects/group%2Fsub%2Fproj/hooks" in captured["url"]
    assert captured["headers"]["PRIVATE-TOKEN"] == "ghp_SEKRET"
    assert captured["body"]["push_events"] is True
    assert captured["body"]["merge_requests_events"] is True
    assert captured["body"]["token"] == "s"


@pytest.mark.asyncio
async def test_gitlab_oauth_uses_bearer(monkeypatch):
    captured = {}

    def handler(method, url, headers, params, json):
        captured.update(headers=headers)
        return {"id": 1, "url": "http://cb", "active": True}

    _install_session(monkeypatch, handler)
    await git_clients.create_webhook(
        "gitlab", "o/r", _opts(token="oatok", is_oauth=True),
        url="http://cb", secret="s", events=["push"],
    )
    assert captured["headers"]["Authorization"] == "Bearer oatok"


# ── Gitea ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_gitea_update_webhook_payload(monkeypatch):
    captured = {}

    def handler(method, url, headers, params, json):
        captured.update(method=method, url=url, headers=headers, body=json)
        return {"id": 9, "active": False, "events": ["push"], "config": {"url": "http://cb"}}

    _install_session(monkeypatch, handler)
    wh = await git_clients.update_webhook(
        "gitea", "owner/repo", _opts(base_url="https://gitea.local"),
        hook_id="9", url="http://cb", secret=None, events=["push"], active=False,
    )
    assert wh.active is False
    assert captured["method"] == "PATCH"
    assert captured["url"] == "https://gitea.local/api/v1/repos/owner/repo/hooks/9"
    assert captured["headers"]["Authorization"] == "token ghp_SEKRET"
    assert "secret" not in captured["body"]["config"]  # no secret rotation


# ── Gitee ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_gitee_create_webhook_token_in_body(monkeypatch):
    captured = {}

    def handler(method, url, headers, params, json):
        captured.update(method=method, url=url, body=json)
        return {"id": 3, "url": "http://cb", "active": True, "push_events": True}

    _install_session(monkeypatch, handler)
    wh = await git_clients.create_webhook(
        "gitee", "o/r", _opts(),
        url="http://cb", secret="s", events=["push"], active=True,
    )
    assert wh.hook_id == "3"
    # Gitee carries access_token in the body, not the Authorization header.
    assert captured["body"]["access_token"] == "ghp_SEKRET"
    assert captured["body"]["password"] == "s"
    assert captured["url"].endswith("/api/v5/repos/o/r/hooks")


# ── Unsupported platforms + token safety ────────────────────────────────────
@pytest.mark.asyncio
async def test_unsupported_platform_raises(monkeypatch):
    with pytest.raises(git_clients.GitClientError):
        await git_clients.create_webhook(
            "codeup", "o/r", _opts(),
            url="http://cb", secret="s", events=["push"],
        )


@pytest.mark.asyncio
async def test_error_message_never_leaks_token(monkeypatch):
    class _Resp:
        status = 500

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def text(self):
            return "boom"

        async def json(self, content_type=None):
            return {}

    class _Session:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def post(self, url, headers=None, params=None, json=None):
            return _Resp()

    monkeypatch.setattr(git_clients.aiohttp, "ClientSession", _Session)
    with pytest.raises(git_clients.GitClientError) as exc:
        await git_clients.create_webhook(
            "github", "o/r", _opts(token="SUPER_SECRET_TOKEN"),
            url="http://cb", secret="s", events=["push"],
        )
    assert "SUPER_SECRET_TOKEN" not in str(exc.value)


@pytest.mark.asyncio
async def test_delete_404_is_success(monkeypatch):
    class _Resp:
        status = 404

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def text(self):
            return "gone"

    class _Session:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def delete(self, url, headers=None, params=None):
            return _Resp()

    monkeypatch.setattr(git_clients.aiohttp, "ClientSession", _Session)
    status = await git_clients.delete_webhook(
        "github", "o/r", _opts(), hook_id="1"
    )
    assert status == 404
