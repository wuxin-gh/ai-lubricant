from __future__ import annotations

import hashlib
import hmac
import json
import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from tortoise import Tortoise

from monkeycode_compat.models_webhook import ProjectWebhook
from monkeycode_compat.models_webhook_event import ProjectWebhookEvent
from monkeycode_compat.routes_webhook_receive import router


@pytest_asyncio.fixture
async def app_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"monkeycode_compat": [
            "monkeycode_compat.models_webhook",
            "monkeycode_compat.models_webhook_event",
        ]},
    )
    await Tortoise.generate_schemas()
    app = FastAPI()
    app.include_router(router)
    try:
        yield app
    finally:
        await Tortoise.close_connections()


async def _hook(platform: str, secret: str = "hook-secret"):
    return await ProjectWebhook.create(
        id=uuid.uuid4(), project_id=uuid.uuid4(), user_id=uuid.uuid4(),
        platform=platform, full_name="acme/repo", hook_id="1",
        callback_url="http://test/cb", secret=secret, events=["push"], active=True,
        review_enabled=True,
    )


async def _post(app, hook, payload, headers):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(
            f"/api/v1/webhooks/projects/{hook.project_id}",
            content=json.dumps(payload).encode(), headers=headers,
        )


@pytest.mark.asyncio
async def test_github_signature_and_duplicate_delivery(app_db):
    hook = await _hook("github")
    payload = {"ref": "refs/heads/main", "before": "a" * 40, "after": "b" * 40}
    body = json.dumps(payload).encode()
    sig = "sha256=" + hmac.new(b"hook-secret", body, hashlib.sha256).hexdigest()
    headers = {"X-GitHub-Event": "push", "X-GitHub-Delivery": "d-1", "X-Hub-Signature-256": sig}
    response = await _post(app_db, hook, payload, headers)
    duplicate = await _post(app_db, hook, payload, headers)
    assert response.status_code == 202
    assert duplicate.status_code == 202
    assert response.json()["event_id"] == duplicate.json()["event_id"]
    assert await ProjectWebhookEvent.all().count() == 1


@pytest.mark.asyncio
async def test_invalid_signature_does_not_leak_secret(app_db):
    hook = await _hook("github")
    response = await _post(
        app_db, hook, {"after": "b" * 40},
        {"X-GitHub-Event": "push", "X-Hub-Signature-256": "sha256=bad"},
    )
    assert response.status_code == 401
    assert "hook-secret" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("platform", "headers"),
    [
        ("gitlab", {"X-Gitlab-Token": "hook-secret", "X-Gitlab-Event": "Push Hook"}),
        ("gitee", {"X-Gitee-Token": "hook-secret", "X-Gitee-Event": "push_hooks"}),
    ],
)
async def test_token_platforms_accept_valid_delivery(app_db, platform, headers):
    hook = await _hook(platform)
    response = await _post(
        app_db, hook, {"ref": "refs/heads/main", "after": "b" * 40}, headers,
    )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_gitea_hmac_accepts_valid_delivery(app_db):
    hook = await _hook("gitea")
    payload = {"ref": "refs/heads/main", "after": "b" * 40}
    body = json.dumps(payload).encode()
    sig = hmac.new(b"hook-secret", body, hashlib.sha256).hexdigest()
    response = await _post(
        app_db, hook, payload,
        {"X-Gitea-Event": "push", "X-Gitea-Delivery": "g1", "X-Gitea-Signature": sig},
    )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_unknown_event_is_accepted_and_ignored(app_db):
    hook = await _hook("gitlab")
    response = await _post(
        app_db, hook, {"object_kind": "pipeline"},
        {"X-Gitlab-Token": "hook-secret", "X-Gitlab-Event": "Pipeline Hook"},
    )
    assert response.status_code == 202
    assert response.json()["status"] == "ignored"


@pytest.mark.asyncio
async def test_body_limit_and_disabled_hook(app_db):
    hook = await _hook("github")
    async with AsyncClient(transport=ASGITransport(app=app_db), base_url="http://test") as client:
        oversized = await client.post(
            f"/api/v1/webhooks/projects/{hook.project_id}",
            content=b"{}", headers={"Content-Length": str(2 * 1024 * 1024 + 1)},
        )
        hook.active = False
        await hook.save(update_fields=["active"])
        disabled = await client.post(
            f"/api/v1/webhooks/projects/{hook.project_id}", content=b"{}",
        )
    assert oversized.status_code == 413
    assert disabled.status_code == 404
