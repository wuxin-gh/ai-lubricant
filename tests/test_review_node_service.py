from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from tortoise import Tortoise

from user_platform import review_node_service as module
from user_platform.models_review import ReviewNodeLease
from user_platform.review_node_service import ReviewNodeService

_FRAMEWORK = "open_code_review_delegate"


@pytest_asyncio.fixture
async def db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"user_platform": ["user_platform.models_review"]},
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


def _node(node_id: str, *, labels=None, active=0, online=True, capacity=2):
    return {
        "node_id": node_id,
        "node_name": node_id,
        "node_role": "execution",
        "status": "approved",
        "online": online,
        "connected": online,
        "active_sessions": active,
        "capacity": {"max_sessions": capacity},
        "capabilities": labels or {},
    }


_FULL = {
    "git_version": "2.47.0", "runtime_version": "1.0.0",
    "node_version": "22.0.0", "npm_version": "10.0.0",
    "editor_version_claude": "2.0.0", "ocr_version": "1.0.0",
}
# Same host tooling but the codex CLI instead of claude.
_FULL_CODEX = {**{k: v for k, v in _FULL.items() if k != "editor_version_claude"},
               "editor_version_codex": "1.2.3"}


@pytest.mark.asyncio
async def test_list_review_nodes_exposes_capability_matrix(db, monkeypatch):
    async def list_nodes(_user_id):
        return {"nodes": [_node("ready", labels=_FULL), _node("missing", labels={"git_version": "2"})]}

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    result = await ReviewNodeService().list_review_nodes("user", _FRAMEWORK, "claude")
    assert result["framework"]["name"] == _FRAMEWORK
    assert result["nodes"][0]["review_ready"] is True
    assert result["nodes"][1]["review_ready"] is False
    assert "ocr" in result["nodes"][1]["review_missing"]
    assert "editor:claude" in result["nodes"][1]["review_missing"]


@pytest.mark.asyncio
async def test_codex_is_not_offered_until_webhook_can_register_installation_id(db, monkeypatch):
    """Background webhook Tasks cannot currently satisfy Codex bootstrap auth."""
    async def list_nodes(_user_id):
        return {"nodes": [_node("codex-node", labels=_FULL_CODEX)]}

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    service = ReviewNodeService()
    with pytest.raises(ValueError, match="unsupported_review_editor"):
        await service.list_review_nodes("user", _FRAMEWORK, "codex")
    for_claude = await service.list_review_nodes("user", _FRAMEWORK, "claude")
    assert for_claude["nodes"][0]["review_ready"] is False
    assert "editor:claude" in for_claude["nodes"][0]["review_missing"]


@pytest.mark.asyncio
async def test_unsupported_editor_for_framework_is_rejected(db, monkeypatch):
    async def list_nodes(_user_id):
        return {"nodes": []}

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    with pytest.raises(ValueError, match="unsupported_review_editor"):
        # gemini is not in this framework's supported_editors.
        await ReviewNodeService().list_review_nodes("user", _FRAMEWORK, "gemini")


@pytest.mark.asyncio
async def test_acquire_slot_prefers_least_loaded_and_authorized(db, monkeypatch):
    async def list_nodes(_user_id):
        return {"nodes": [_node("busy", labels=_FULL, active=1), _node("free", labels=_FULL)]}

    async def allowed(_user_id, _node_id):
        return SimpleNamespace(group_id="g")

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    monkeypatch.setattr(module.nodes_service, "user_can_use_node", allowed)
    acquired = await ReviewNodeService().acquire_review_slot(
        "user", ["busy", "free"], _FRAMEWORK, "claude",
        event_id=str(uuid.uuid4()), project_id=str(uuid.uuid4()),
    )
    assert acquired["node"]["node_id"] == "free"
    assert acquired["slot"] == 1


@pytest.mark.asyncio
async def test_slots_bound_concurrency_per_node(db, monkeypatch):
    """A capacity-2 node hosts two reviews, then refuses the third."""
    async def list_nodes(_user_id):
        return {"nodes": [_node("solo", labels=_FULL, capacity=2)]}

    async def allowed(_user_id, _node_id):
        return SimpleNamespace(group_id="g")

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    monkeypatch.setattr(module.nodes_service, "user_can_use_node", allowed)
    service = ReviewNodeService()
    project_id = str(uuid.uuid4())
    slots = []
    for _ in range(3):
        acquired = await service.acquire_review_slot(
            "user", ["solo"], _FRAMEWORK, "claude",
            event_id=str(uuid.uuid4()), project_id=project_id,
        )
        slots.append(acquired["slot"] if acquired else None)
    assert sorted(slots[:2]) == [1, 2]
    assert slots[2] is None
    assert await service.project_inflight(project_id) == 2


@pytest.mark.asyncio
async def test_release_frees_the_slot_for_the_next_review(db, monkeypatch):
    async def list_nodes(_user_id):
        return {"nodes": [_node("solo", labels=_FULL, capacity=1)]}

    async def allowed(_user_id, _node_id):
        return SimpleNamespace(group_id="g")

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    monkeypatch.setattr(module.nodes_service, "user_can_use_node", allowed)
    service = ReviewNodeService()
    first_event = str(uuid.uuid4())
    first = await service.acquire_review_slot(
        "user", ["solo"], _FRAMEWORK, "claude", event_id=first_event,
    )
    assert first is not None
    # Node is saturated while the first review holds its slot.
    assert await service.acquire_review_slot(
        "user", ["solo"], _FRAMEWORK, "claude", event_id=str(uuid.uuid4()),
    ) is None
    assert await service.release_review_slot(event_id=first_event) is True
    assert await service.acquire_review_slot(
        "user", ["solo"], _FRAMEWORK, "claude", event_id=str(uuid.uuid4()),
    ) is not None


@pytest.mark.asyncio
async def test_lease_binds_task_id(db, monkeypatch):
    async def list_nodes(_user_id):
        return {"nodes": [_node("solo", labels=_FULL)]}

    async def allowed(_user_id, _node_id):
        return SimpleNamespace(group_id="g")

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    monkeypatch.setattr(module.nodes_service, "user_can_use_node", allowed)
    service = ReviewNodeService()
    acquired = await service.acquire_review_slot(
        "user", ["solo"], _FRAMEWORK, "claude", event_id=str(uuid.uuid4()),
    )
    task_id = str(uuid.uuid4())
    await service.bind_lease_task(acquired["lease_id"], task_id)
    lease = await ReviewNodeLease.get(id=acquired["lease_id"])
    assert str(lease.task_id) == task_id


@pytest.mark.asyncio
async def test_unauthorized_node_is_never_leased(db, monkeypatch):
    """A node the user's groups were not granted must not be scheduled."""
    async def list_nodes(_user_id):
        return {"nodes": [_node("foreign", labels=_FULL)]}

    async def denied(_user_id, _node_id):
        return None

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    monkeypatch.setattr(module.nodes_service, "user_can_use_node", denied)
    acquired = await ReviewNodeService().acquire_review_slot(
        "user", ["foreign"], _FRAMEWORK, "claude", event_id=str(uuid.uuid4()),
    )
    assert acquired is None
    assert await ReviewNodeLease.all().count() == 0


@pytest.mark.asyncio
async def test_enabled_validation_returns_missing_nodes(db, monkeypatch):
    async def list_nodes(_user_id):
        return {"nodes": [_node("bad", labels={"git_version": "2"})]}

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    failures = await ReviewNodeService().validate_candidate_nodes(
        "user", ["bad"], _FRAMEWORK, "claude", require_ready=True
    )
    assert failures[0]["node_id"] == "bad"
    assert "ocr" in failures[0]["missing"]


@pytest.mark.asyncio
async def test_draft_config_allows_unready_nodes(db, monkeypatch):
    """review_enabled=False saves as a draft even with unready nodes."""
    async def list_nodes(_user_id):
        return {"nodes": [_node("bad", labels={"git_version": "2"})]}

    monkeypatch.setattr(module.nodes_service, "list_my_nodes", list_nodes)
    failures = await ReviewNodeService().validate_candidate_nodes(
        "user", ["bad"], _FRAMEWORK, "claude", require_ready=False
    )
    assert failures == []
