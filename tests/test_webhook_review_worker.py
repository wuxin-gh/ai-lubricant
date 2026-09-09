from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from tortoise import Tortoise

from user_platform.models_project import Project
from user_platform.models_webhook import ProjectWebhook
from user_platform.models_webhook_event import ProjectWebhookEvent
from user_platform import webhook_review_worker as worker_module


@pytest_asyncio.fixture
async def db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"user_platform": [
            "user_platform.models_project",
            "user_platform.models_webhook",
            "user_platform.models_webhook_event",
            "user_platform.models_review",
        ]},
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def _rows(event_type="push", *, provider="claude", node_ids=None,
                skill_config=None, mcp_config=None, plugin_config=None,
                api_key_id=None, auto=False, legacy_editor_ids=None,
                model_limits=None, **event_fields):
    project = await Project.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="p", platform="github",
        repo_url="https://github.com/acme/repo.git", branch="main",
        git_identity_id=uuid.uuid4(),
    )
    hook = await ProjectWebhook.create(
        id=uuid.uuid4(), project_id=project.id, user_id=project.user_id,
        platform="github", full_name="acme/repo", hook_id="1",
        callback_url="http://cb", secret="s", events=["push"], active=True,
        review_executor="claude_delegate", review_provider=provider,
        review_node_ids=node_ids if node_ids is not None else ["node-1"],
        review_skill_config=skill_config or [],
        review_mcp_config=mcp_config or [],
        review_plugin_config=plugin_config or [],
        review_editor_ids=legacy_editor_ids or [],
        review_auto=auto, review_enabled=True,
        review_api_key_id=api_key_id, review_model_limits=model_limits or {},
    )
    event = await ProjectWebhookEvent.create(
        id=uuid.uuid4(), project_id=project.id, webhook_id=hook.id,
        platform="github", delivery_id=str(uuid.uuid4()), event_type=event_type,
        payload={}, status="pending", **event_fields,
    )
    return project, hook, event


def test_review_prompt_modes():
    push = type("E", (), {
        "event_type": "push", "commit_sha": "abc", "before_sha": "def",
        "source_branch": "main", "base_ref": None, "target_branch": None,
        "head_ref": None, "pr_number": None,
    })()
    pr = type("E", (), {
        "event_type": "pull_request", "commit_sha": "head", "before_sha": None,
        "source_branch": "feature", "target_branch": "main",
        "base_ref": "base", "head_ref": "head", "pr_number": "7",
    })()
    assert "--commit abc" in worker_module._review_prompt(push)
    assert "--from base --to head" in worker_module._review_prompt(pr)


@pytest.mark.asyncio
async def test_worker_dispatches_registered_executor_once(db, monkeypatch):
    _project, hook, event = await _rows(commit_sha="abc", before_sha="def")
    called = []

    async def execute(e, h):
        called.append((str(e.id), str(h.id)))
        return str(uuid.uuid4())

    monkeypatch.setitem(worker_module._EXECUTORS, "claude_delegate", execute)
    worker = worker_module.WebhookReviewWorker()
    assert await worker.process_one() is True
    saved = await ProjectWebhookEvent.get(id=event.id)
    assert saved.status == "completed"
    assert saved.task_id is not None
    assert len(called) == 1
    assert await worker.process_one() is False


@pytest.mark.asyncio
async def test_worker_retries_failed_dispatch(db, monkeypatch):
    _project, _hook, event = await _rows(commit_sha="abc")

    async def fail(_event, _hook):
        raise RuntimeError("node unavailable")

    monkeypatch.setitem(worker_module._EXECUTORS, "claude_delegate", fail)
    worker = worker_module.WebhookReviewWorker()
    await worker.process_one()
    saved = await ProjectWebhookEvent.get(id=event.id)
    assert saved.status == "failed"
    assert saved.attempts == 1
    assert saved.next_attempt_at is not None
    assert "node unavailable" in saved.last_error


@pytest.mark.asyncio
async def test_worker_start_recovers_processing_rows(db, monkeypatch):
    _project, _hook, event = await _rows(commit_sha="abc")
    await ProjectWebhookEvent.filter(id=event.id).update(status="processing")
    worker = worker_module.WebhookReviewWorker()
    await worker.start()
    try:
        recovered = await ProjectWebhookEvent.get(id=event.id)
        assert recovered.status == "pending"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_claude_delegate_passes_review_capabilities(db, monkeypatch):
    project, hook, event = await _rows(
        event_type="pull_request", base_ref="base", head_ref="head",
        source_branch="feature", target_branch="main", pr_number="3",
        api_key_id=42, skill_config=[{"name": "existing", "url": "https://x/repo.git"}],
    )
    captured = {}
    acquire_calls = []

    async def fake_mcp(_event):
        return {"name": "review-result", "type": "sse", "url": "https://gw/mcp"}

    async def fake_identity(_identity_id):
        return type("Identity", (), {"access_token": "git-token", "username": "git-user"})()

    async def fake_create(user_id, req):
        captured.update(user_id=user_id, req=req)
        return {"id": str(uuid.uuid4()), "status": "processing"}

    async def fake_acquire(_user_id, node_ids, _framework, _provider, *, event_id, project_id=None):
        acquire_calls.append(list(node_ids))
        return {"node": {"node_id": "node-1"}, "lease_id": str(uuid.uuid4()), "slot": 1}

    async def fake_check(_user_id, node_id, _framework, _provider):
        return {"ok": True, "node_id": node_id}

    async def fake_bind(_lease_id, _task_id):
        return None

    monkeypatch.setattr(worker_module, "_review_mcp_entry", fake_mcp)
    from user_platform.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "acquire_review_slot", fake_acquire)
    monkeypatch.setattr(review_node_service, "check_node_for_review", fake_check)
    monkeypatch.setattr(review_node_service, "bind_lease_task", fake_bind)
    from user_platform.git_service import git_service
    monkeypatch.setattr(git_service, "load_identity_for_read", fake_identity)
    monkeypatch.setattr(worker_module.task_service, "create_task", fake_create)
    task_id = await worker_module.execute_claude_delegate(event, hook)
    assert uuid.UUID(task_id)
    req = captured["req"]
    assert req["node_id"] == "node-1"
    assert req["cli_name"] == "claude"
    assert req["provider"] == "claude"
    assert req["mode"] == "default"
    assert req["parent_api_key_id"] == 42
    assert req["repo"]["token"] == "git-token"
    assert req["repo"]["commit"] == "head"
    assert any(s["name"] == "open-code-review-delegate" for s in req["_session_skills"])
    assert any(s["name"] == "existing" for s in req["_session_skills"])
    assert any(m["name"] == "review-result" for m in req["_session_mcps"])
    assert "--from base --to head" in req["content"]
    # No editor template was consulted; the node pool comes from the webhook.
    assert acquire_calls == [["node-1"]]
    # No editor-derived child key is planted; task_service owns the LLM config.
    assert "_session_llm" not in req
    # A review holds its own slot lease, so it must NOT take the exclusive
    # TaskNodeBinding — otherwise unique(node_id) caps a node at one review.
    assert req["_review_lease_id"]


@pytest.mark.asyncio
async def test_auto_mode_passes_all_execution_nodes(db, monkeypatch):
    project, hook, event = await _rows(commit_sha="abc", auto=True, api_key_id=42)
    captured_nodes = {}

    async def fake_list_my_nodes(user_id):
        return {"nodes": [
            {"node_id": "n-exec-1", "node_role": "execution", "display_only": False},
            {"node_id": "n-exec-2", "role": "execution"},
            {"node_id": "n-mgmt", "node_role": "management", "display_only": False},
            {"node_id": "n-display", "node_role": "execution", "display_only": True},
        ]}

    async def fake_acquire(_user_id, node_ids, _framework, _provider, *, event_id, project_id=None):
        captured_nodes.update(nodes=list(node_ids))
        return {"node": {"node_id": "n-exec-1"}, "lease_id": str(uuid.uuid4()), "slot": 1}

    async def fake_check(_user_id, node_id, _framework, _provider):
        return {"ok": True, "node_id": node_id}

    async def fake_bind(_lease_id, _task_id):
        return None

    async def fake_identity(_identity_id):
        return type("Identity", (), {"access_token": "t", "username": "u"})()

    async def fake_create(_user_id, req):
        return {"id": str(uuid.uuid4()), "status": "processing"}

    monkeypatch.setattr(worker_module, "_review_mcp_entry", lambda _e: _none())
    from user_platform import nodes_service
    monkeypatch.setattr(nodes_service.nodes_service, "list_my_nodes", fake_list_my_nodes)
    from user_platform.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "acquire_review_slot", fake_acquire)
    monkeypatch.setattr(review_node_service, "check_node_for_review", fake_check)
    monkeypatch.setattr(review_node_service, "bind_lease_task", fake_bind)
    from user_platform.git_service import git_service
    monkeypatch.setattr(git_service, "load_identity_for_read", fake_identity)
    monkeypatch.setattr(worker_module.task_service, "create_task", fake_create)
    await worker_module.execute_claude_delegate(event, hook)
    assert captured_nodes["nodes"] == ["n-exec-1", "n-exec-2"]


@pytest.mark.asyncio
async def test_no_provider_when_enabled_raises(db, monkeypatch):
    project, hook, event = await _rows(commit_sha="abc", provider=None,
                                       node_ids=[], api_key_id=42)
    # A row with no provider and no legacy editors cannot be dispatched.
    with pytest.raises(RuntimeError, match="review_provider_unsupported"):
        await worker_module.execute_claude_delegate(event, hook)


@pytest.mark.asyncio
async def test_codex_review_is_rejected_without_bootstrap_identity(db, monkeypatch):
    project, hook, event = await _rows(
        commit_sha="abc", provider="codex", node_ids=["node-1"], api_key_id=42,
    )
    with pytest.raises(RuntimeError, match="review_provider_bootstrap_unsupported"):
        await worker_module.execute_claude_delegate(event, hook)


@pytest.mark.asyncio
async def test_review_without_parent_key_fails_before_leasing(db, monkeypatch):
    project, hook, event = await _rows(
        commit_sha="abc", provider="claude", node_ids=["node-1"], api_key_id=None,
    )
    with pytest.raises(RuntimeError, match="review_api_key_required"):
        await worker_module.execute_claude_delegate(event, hook)


@pytest.mark.asyncio
async def test_no_candidate_nodes_raises(db, monkeypatch):
    project, hook, event = await _rows(commit_sha="abc", provider="claude",
                                       node_ids=[], api_key_id=42)
    with pytest.raises(RuntimeError, match="no_review_node_available"):
        await worker_module.execute_claude_delegate(event, hook)


@pytest.mark.asyncio
async def test_legacy_adapter_drains_legacy_row(db, monkeypatch):
    project, hook, event = await _rows(
        commit_sha="abc", provider=None, node_ids=[], api_key_id=None,
        legacy_editor_ids=["ed-1"],
    )
    captured = {}

    async def fake_list_editors(project_id, user_id, *, include_deleted=False):
        return [{
            "id": "ed-1", "project_id": project_id, "owner_user_id": user_id,
            "provider": "claude", "node_id": "node-1", "api_key_id": 7,
            "skill_config": [{"name": "legacy-skill"}],
            "mcp_config": [], "plugin_config": [],
        }]

    async def fake_acquire(_user_id, node_ids, _framework, _provider, *, event_id, project_id=None):
        captured["nodes"] = list(node_ids)
        return {"node": {"node_id": "node-1"}, "lease_id": str(uuid.uuid4()), "slot": 1}

    async def fake_check(_user_id, node_id, _framework, _provider):
        return {"ok": True, "node_id": node_id}

    async def fake_bind(_lease_id, _task_id):
        return None

    async def fake_identity(_identity_id):
        return type("Identity", (), {"access_token": "t", "username": "u"})()

    async def fake_create(_user_id, req):
        captured["req"] = req
        return {"id": str(uuid.uuid4()), "status": "processing"}

    from db import PostgresClient
    monkeypatch.setattr(PostgresClient, "list_project_editors_for_user", fake_list_editors)
    monkeypatch.setattr(worker_module, "_review_mcp_entry", lambda _e: _none())
    from user_platform import nodes_service

    async def fake_list_my_nodes(_user_id):
        return {"nodes": [
            {"node_id": "node-1", "node_role": "execution", "status": "approved",
             "online": True, "connected": True},
            {"node_id": "node-2", "node_role": "execution", "status": "approved",
             "online": True, "connected": True},
        ]}

    monkeypatch.setattr(nodes_service.nodes_service, "list_my_nodes", fake_list_my_nodes)
    from user_platform.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "acquire_review_slot", fake_acquire)
    monkeypatch.setattr(review_node_service, "check_node_for_review", fake_check)
    monkeypatch.setattr(review_node_service, "bind_lease_task", fake_bind)
    from user_platform.git_service import git_service
    monkeypatch.setattr(git_service, "load_identity_for_read", fake_identity)
    monkeypatch.setattr(worker_module.task_service, "create_task", fake_create)
    await worker_module.execute_claude_delegate(event, hook)
    # The row is rewritten onto the canonical fields and the editor list cleared.
    saved = await ProjectWebhook.get(id=hook.id)
    assert saved.review_provider == "claude"
    assert saved.review_node_ids == ["node-1"]
    assert saved.review_editor_ids == []
    assert saved.review_api_key_id == 7
    assert any(s["name"] == "legacy-skill" for s in captured["req"]["_session_skills"])
    assert captured["nodes"] == ["node-1"]


@pytest.mark.asyncio
async def test_legacy_adapter_merges_same_provider_only(db, monkeypatch):
    project, hook, event = await _rows(
        commit_sha="abc", provider=None, node_ids=[], api_key_id=None,
        legacy_editor_ids=["ed-1", "ed-2"],
    )
    captured = {}

    async def fake_list_editors(project_id, user_id, *, include_deleted=False):
        return [
            {"id": "ed-1", "provider": "claude", "node_id": "node-1", "api_key_id": 7,
             "skill_config": [{"name": "a"}], "mcp_config": [], "plugin_config": []},
            {"id": "ed-2", "provider": "codex", "node_id": "node-2",
             "skill_config": [{"name": "b"}], "mcp_config": [], "plugin_config": []},
        ]

    async def fake_acquire(_user_id, node_ids, _framework, _provider, *, event_id, project_id=None):
        captured["nodes"] = list(node_ids)
        return {"node": {"node_id": "node-1"}, "lease_id": str(uuid.uuid4()), "slot": 1}

    async def fake_check(_user_id, node_id, _framework, _provider):
        return {"ok": True, "node_id": node_id}

    async def fake_bind(_lease_id, _task_id):
        return None

    async def fake_identity(_identity_id):
        return type("Identity", (), {"access_token": "t", "username": "u"})()

    async def fake_create(_user_id, req):
        captured["req"] = req
        return {"id": str(uuid.uuid4()), "status": "processing"}

    from db import PostgresClient
    monkeypatch.setattr(PostgresClient, "list_project_editors_for_user", fake_list_editors)
    monkeypatch.setattr(worker_module, "_review_mcp_entry", lambda _e: _none())
    from user_platform import nodes_service

    async def fake_list_my_nodes(_user_id):
        return {"nodes": [
            {"node_id": "node-1", "node_role": "execution", "status": "approved",
             "online": True, "connected": True},
            {"node_id": "node-2", "node_role": "execution", "status": "approved",
             "online": True, "connected": True},
        ]}

    monkeypatch.setattr(nodes_service.nodes_service, "list_my_nodes", fake_list_my_nodes)
    from user_platform.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "acquire_review_slot", fake_acquire)
    monkeypatch.setattr(review_node_service, "check_node_for_review", fake_check)
    monkeypatch.setattr(review_node_service, "bind_lease_task", fake_bind)
    from user_platform.git_service import git_service
    monkeypatch.setattr(git_service, "load_identity_for_read", fake_identity)
    monkeypatch.setattr(worker_module.task_service, "create_task", fake_create)
    await worker_module.execute_claude_delegate(event, hook)
    saved = await ProjectWebhook.get(id=hook.id)
    # First editor's provider wins; the codex editor's node/skills are dropped.
    assert saved.review_provider == "claude"
    assert saved.review_node_ids == ["node-1"]
    skill_names = {s["name"] for s in saved.review_skill_config}
    assert skill_names == {"a"}


@pytest.mark.asyncio
async def test_review_passes_model_limits_as_usage_limit(db, monkeypatch):
    project, hook, event = await _rows(
        commit_sha="abc", api_key_id=42,
        model_limits={"max_requests": 50, "max_total_tokens": 1000},
    )
    captured = {}

    async def fake_identity(_identity_id):
        return type("Identity", (), {"access_token": "t", "username": "u"})()

    async def fake_create(_user_id, req):
        captured["req"] = req
        return {"id": str(uuid.uuid4()), "status": "processing"}

    async def fake_acquire(_u, _n, _f, _p, *, event_id, project_id=None):
        return {"node": {"node_id": "node-1"}, "lease_id": str(uuid.uuid4()), "slot": 1}

    async def fake_check(_u, n, _f, _p):
        return {"ok": True, "node_id": n}

    async def fake_bind(_l, _t):
        return None

    monkeypatch.setattr(worker_module, "_review_mcp_entry", lambda _e: _none())
    from user_platform.review_node_service import review_node_service
    monkeypatch.setattr(review_node_service, "acquire_review_slot", fake_acquire)
    monkeypatch.setattr(review_node_service, "check_node_for_review", fake_check)
    monkeypatch.setattr(review_node_service, "bind_lease_task", fake_bind)
    from user_platform.git_service import git_service
    monkeypatch.setattr(git_service, "load_identity_for_read", fake_identity)
    monkeypatch.setattr(worker_module.task_service, "create_task", fake_create)
    await worker_module.execute_claude_delegate(event, hook)
    assert captured["req"]["parent_api_key_id"] == 42
    assert captured["req"]["usage_limit"] == {"max_requests": 50, "max_total_tokens": 1000}


async def _none():
    return None
