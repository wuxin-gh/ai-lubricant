"""Webhook service orchestration tests.

Exercises the upsert idempotency, 404-on-delete tolerance, access control and
secret masking in :mod:`webhook_service`, with the Git platform calls and the
project/identity loaders mocked — no network, no real DB beyond an in-memory
Tortoise for the ``ProjectWebhook`` table.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from tortoise import Tortoise

from monkeycode_compat import git_clients, webhook_service as webhook_module
from monkeycode_compat.webhook_service import webhook_service
from monkeycode_compat.models_webhook import ProjectWebhook


@pytest_asyncio.fixture
async def tortoise_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"monkeycode_compat": ["monkeycode_compat.models_webhook"]},
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


class _FakeProject:
    """Stand-in for a Project row with the attributes the service reads."""

    def __init__(self, *, git_identity_id="id-1", platform="github", repo_url="https://github.com/acme/widget"):
        self.id = uuid.uuid4()
        self.user_id = uuid.uuid4()
        self.git_identity_id = git_identity_id
        self.platform = platform
        self.repo_url = repo_url


class _FakeIdentity:
    def __init__(self, token="ghp_tok", platform="github"):
        self.access_token = token
        self.platform = platform


def _set_public_origin(monkeypatch, origin: str) -> None:
    """Override the configured webhook public origin for callback URL assertions."""
    import dataclasses

    from monkeycode_compat import config

    monkeypatch.setattr(
        config, "settings", dataclasses.replace(config.settings, webhook_public_origin=origin)
    )


def _patch_resolver(monkeypatch, *, project, access="owner", identity=None, opts=None):
    """Make webhook_service use a fake project/identity instead of the DB."""
    async def fake_resolve(self, user_id, project_id, role):
        return project, access

    async def fake_load(self, project):
        if identity is None:
            return None
        return identity, "acme/widget", (identity.platform or "github"), (opts or git_clients.RepositoryOptions(token=identity.access_token))

    monkeypatch.setattr(webhook_module.WebhookService, "_resolve", fake_resolve)
    monkeypatch.setattr(webhook_module.WebhookService, "_load_identity_and_opts", fake_load)


# ── enable: create when absent ───────────────────────────────────────────────
@pytest.mark.asyncio
async def test_enable_creates_when_no_existing_hook(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())

    async def fake_list(platform, full_name, opts):
        return []

    created = {}

    async def fake_create(platform, full_name, opts, *, url, secret, events, active):
        created.update(url=url, secret=secret, events=events, active=active)
        return git_clients.Webhook(hook_id="100", url=url, events=events, active=active)

    async def fake_update(*a, **k):
        raise AssertionError("update should not be called when no existing hook")

    monkeypatch.setattr(git_clients, "list_webhooks", fake_list)
    monkeypatch.setattr(git_clients, "create_webhook", fake_create)
    monkeypatch.setattr(git_clients, "update_webhook", fake_update)

    result = await webhook_service.enable_webhook(
        str(project.user_id), str(project.id), role="user", events=["push"],
        review_enabled=False, request=None,
    )
    assert result["hook_id"] == "100"
    assert result["active"] is True
    assert result["events"] == ["push"]
    # Secret must be masked, not plaintext.
    assert result["has_secret"] is True
    assert result["secret_masked"].startswith("***")
    assert "ghp_tok" not in result["secret_masked"]
    # Callback URL is built from settings (empty here → relative form).
    assert result["callback_url"].endswith(f"/api/v1/webhooks/projects/{project.id}")


# ── enable: update when platform already has our callback URL ────────────────
@pytest.mark.asyncio
async def test_enable_updates_existing_hook(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())
    # Configured public origin is used verbatim in the callback URL.
    _set_public_origin(monkeypatch, "http://host")
    callback = f"http://host/api/v1/webhooks/projects/{project.id}"

    async def fake_list(platform, full_name, opts):
        # Platform already has a hook pointing at our callback URL.
        return [git_clients.Webhook(hook_id="200", url=callback, events=["push"], active=False)]

    calls = {"create": 0, "update": 0}

    async def fake_create(*a, **k):
        calls["create"] += 1
        raise AssertionError("should not create when existing matches")

    async def fake_update(platform, full_name, opts, *, hook_id, url, secret, events, active):
        calls["update"] += 1
        assert hook_id == "200"
        assert url == callback
        assert active is True
        return git_clients.Webhook(hook_id="200", url=url, events=events, active=active)

    monkeypatch.setattr(git_clients, "list_webhooks", fake_list)
    monkeypatch.setattr(git_clients, "create_webhook", fake_create)
    monkeypatch.setattr(git_clients, "update_webhook", fake_update)

    result = await webhook_service.enable_webhook(
        str(project.user_id), str(project.id), events=["push"],
        review_enabled=False, request=None,
    )
    assert calls == {"create": 0, "update": 1}
    assert result["hook_id"] == "200"


# ── enable: second call reuses the local row (no duplicate) ──────────────────
@pytest.mark.asyncio
async def test_enable_idempotent_across_calls(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())

    async def fake_list(platform, full_name, opts):
        # After the first enable, list returns our hook (matched by callback URL).
        from monkeycode_compat import config
        base = (config.settings.webhook_public_origin or "").rstrip("/")
        url = f"{base}/api/v1/webhooks/projects/{project.id}"
        return [git_clients.Webhook(hook_id="300", url=url, events=["push"], active=True)]

    async def fake_update(platform, full_name, opts, *, hook_id, url, secret, events, active):
        return git_clients.Webhook(hook_id="300", url=url, events=events, active=active)

    async def fake_create(*a, **k):
        raise AssertionError("should not create on second call")

    monkeypatch.setattr(git_clients, "list_webhooks", fake_list)
    monkeypatch.setattr(git_clients, "update_webhook", fake_update)
    monkeypatch.setattr(git_clients, "create_webhook", fake_create)

    await webhook_service.enable_webhook(
        str(project.user_id), str(project.id), events=["push"],
        review_enabled=False, request=None,
    )
    result = await webhook_service.enable_webhook(
        str(project.user_id), str(project.id), events=["push"],
        review_enabled=False, request=None,
    )
    # Exactly one row in the table.
    assert await ProjectWebhook.filter(project_id=project.id).count() == 1
    assert result["hook_id"] == "300"


# ── delete: platform 404 is tolerated ────────────────────────────────────────
@pytest.mark.asyncio
async def test_delete_tolerates_platform_404(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())

    # Seed a row.
    await ProjectWebhook.create(
        id=uuid.uuid4(), project_id=project.id, user_id=project.user_id,
        platform="github", full_name="acme/widget", hook_id="555",
        callback_url="http://cb", secret="s", events=["push"], active=True,
    )

    async def fake_delete(platform, full_name, opts, *, hook_id):
        raise git_clients.GitClientError("DELETE ... returned HTTP 404: gone")

    monkeypatch.setattr(git_clients, "delete_webhook", fake_delete)

    deleted = await webhook_service.disable_webhook(
        str(project.user_id), str(project.id),
    )
    assert deleted is True
    assert await ProjectWebhook.filter(project_id=project.id).count() == 0


# ── delete: no row → False ───────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_delete_when_not_configured(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())
    deleted = await webhook_service.disable_webhook(
        str(project.user_id), str(project.id),
    )
    assert deleted is False


# ── callback resync: recompute origin + update upstream/local ─────────────────
@pytest.mark.asyncio
async def test_resync_callback_updates_upstream_and_local(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())
    row = await ProjectWebhook.create(
        id=uuid.uuid4(), project_id=project.id, user_id=project.user_id,
        platform="github", full_name="acme/widget", hook_id="777",
        callback_url="http://old/api/v1/webhooks/projects/old", secret="keep-secret",
        events=["push", "pull_request"], active=True,
    )
    _set_public_origin(monkeypatch, "https://new.example")
    captured = {}

    async def fake_update(platform, full_name, opts, *, hook_id, url, secret, events, active):
        captured.update(
            platform=platform, hook_id=hook_id, url=url, secret=secret,
            events=events, active=active,
        )
        return git_clients.Webhook(hook_id=hook_id, url=url, events=events, active=active)

    monkeypatch.setattr(git_clients, "update_webhook", fake_update)
    result = await webhook_service.resync_callback(
        str(project.user_id), str(project.id), request=None,
    )
    expected = f"https://new.example/api/v1/webhooks/projects/{project.id}"
    assert captured == {
        "platform": "github", "hook_id": "777", "url": expected,
        "secret": None, "events": ["push", "pull_request"], "active": True,
    }
    assert result["callback_url"] == expected
    saved = await ProjectWebhook.get(id=row.id)
    assert saved.callback_url == expected
    assert saved.secret == "keep-secret"


# ── access control: read_only cannot enable ──────────────────────────────────
@pytest.mark.asyncio
async def test_read_only_collaborator_cannot_enable(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, access="read_only", identity=_FakeIdentity())
    with pytest.raises(ValueError, match="forbidden"):
        await webhook_service.enable_webhook(
            str(project.user_id), str(project.id), events=["push"], request=None,
        )


# ── get masks the secret ─────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_get_returns_masked_secret(tortoise_db, monkeypatch):
    project = _FakeProject()
    _patch_resolver(monkeypatch, project=project, identity=_FakeIdentity())
    await ProjectWebhook.create(
        id=uuid.uuid4(), project_id=project.id, user_id=project.user_id,
        platform="github", full_name="acme/widget", hook_id="9",
        callback_url="http://cb", secret="super-secret-value-123", events=["push"], active=True,
    )
    result = await webhook_service.get_webhook(str(project.user_id), str(project.id))
    assert result is not None
    assert result["has_secret"] is True
    assert result["secret_masked"].startswith("***")
    assert "super-secret-value-123" not in result["secret_masked"]


# ── canonical review config validation ───────────────────────────────────────
@pytest.mark.asyncio
async def test_validate_review_config_requires_provider_when_enabled(tortoise_db):
    project = _FakeProject()
    with pytest.raises(ValueError, match="review_provider_required"):
        await webhook_module._validate_review_config(
            str(project.user_id), project,
            review_provider=None, review_node_ids=["node-1"], review_auto=False,
            review_framework="open_code_review_delegate", review_enabled=True,
            review_api_key_id=1, review_skill_config=[], review_mcp_config=[],
            review_plugin_config=[],
        )


@pytest.mark.asyncio
async def test_validate_review_config_rejects_codex_without_bootstrap(tortoise_db):
    project = _FakeProject()
    with pytest.raises(ValueError, match="review_provider_bootstrap_unsupported"):
        await webhook_module._validate_review_config(
            str(project.user_id), project,
            review_provider="codex", review_node_ids=["node-1"], review_auto=False,
            review_framework="open_code_review_delegate", review_enabled=True,
            review_api_key_id=1, review_skill_config=[], review_mcp_config=[],
            review_plugin_config=[],
        )


@pytest.mark.asyncio
async def test_validate_review_config_requires_parent_key(tortoise_db, monkeypatch):
    project = _FakeProject()

    async def validate_nodes(*_args, **_kwargs):
        return []

    from monkeycode_compat.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "validate_candidate_nodes", validate_nodes)
    with pytest.raises(ValueError, match="review_api_key_required"):
        await webhook_module._validate_review_config(
            str(project.user_id), project,
            review_provider="claude", review_node_ids=["node-1"], review_auto=False,
            review_framework="open_code_review_delegate", review_enabled=True,
            review_api_key_id=None, review_skill_config=[], review_mcp_config=[],
            review_plugin_config=[],
        )


@pytest.mark.asyncio
async def test_validate_review_config_checks_parent_key_ownership(tortoise_db, monkeypatch):
    project = _FakeProject()

    async def validate_nodes(*_args, **_kwargs):
        return []

    async def missing_parent(_key_id, _user_id):
        return None

    from monkeycode_compat.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "validate_candidate_nodes", validate_nodes)
    from db import PostgresClient
    monkeypatch.setattr(PostgresClient, "get_api_key_by_id_for_user", missing_parent)
    with pytest.raises(ValueError, match="review_api_key_unavailable"):
        await webhook_module._validate_review_config(
            str(project.user_id), project,
            review_provider="claude", review_node_ids=["node-1"], review_auto=False,
            review_framework="open_code_review_delegate", review_enabled=True,
            review_api_key_id=99, review_skill_config=[], review_mcp_config=[],
            review_plugin_config=[],
        )


def test_session_config_rejects_non_object_entries():
    with pytest.raises(ValueError, match="review_session_config_invalid"):
        webhook_module._validate_session_config("skills", [{"name": "ok"}, "bad"])

