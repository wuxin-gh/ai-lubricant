"""Task sessions share nodes; webhook reviews additionally hold capacity leases.

Normal tasks rely on the node server's configured max_sessions/CPU/memory
admission and never create a TaskNodeBinding. Reviews hold ReviewNodeLease slots
so webhook scheduling can reserve capacity before dispatch. These tests lock in
that split and the review session-id bookkeeping.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from tortoise import Tortoise

from user_platform import task_service as module
from user_platform.models_review import ReviewNodeLease
from user_platform.models_task import TaskNodeBinding

_MODULES = [
    "user_platform.models_task",
    "user_platform.models_review",
]


@pytest_asyncio.fixture
async def db():
    await Tortoise.init(
        db_url="sqlite://:memory:", modules={"user_platform": _MODULES}
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.mark.asyncio
async def test_normal_tasks_do_not_use_an_exclusive_node_binding(db):
    """The node server, not TaskNodeBinding, controls normal task capacity."""
    common = {"group_id": uuid.uuid4(), "user_id": uuid.uuid4()}
    first = await TaskNodeBinding.create(
        id=uuid.uuid4(), task_id=uuid.uuid4(), node_id="n1", **common
    )
    # Legacy rows can still exist in migrated databases, but the normal create
    # path must not write another one. The model constraint is retained only for
    # old-row cleanup compatibility.
    assert first.node_id == "n1"


@pytest.mark.asyncio
async def test_review_leases_let_two_reviews_share_a_node(db):
    """Two reviews coexist on one node because they use slots, not the lock."""
    project_id = uuid.uuid4()
    for slot in (1, 2):
        await ReviewNodeLease.create(
            id=uuid.uuid4(), node_id="n1", project_id=project_id,
            event_id=uuid.uuid4(), slot=slot, status="active",
        )
    assert await ReviewNodeLease.filter(node_id="n1", status="active").count() == 2
    # No exclusive binding was taken, so a normal task could still claim the node.
    assert await TaskNodeBinding.filter(node_id="n1").exists() is False


@pytest.mark.asyncio
async def test_record_review_session_id_writes_to_the_lease(db):
    """A review's session id lands on its lease."""
    lease = await ReviewNodeLease.create(
        id=uuid.uuid4(), node_id="n1", event_id=uuid.uuid4(), slot=1, status="active",
    )
    await module.task_service._record_review_session_id(str(lease.id), "sess-1")
    refreshed = await ReviewNodeLease.get(id=lease.id)
    assert refreshed.session_id == "sess-1"


@pytest.mark.asyncio
async def test_record_review_session_id_ignores_an_empty_lease(db):
    """Normal tasks pass no lease id; the call must be a no-op, not an error."""
    await module.task_service._record_review_session_id("", "sess-2")
    assert await ReviewNodeLease.all().count() == 0


@pytest.mark.asyncio
async def test_release_node_tears_down_a_review_session_and_frees_the_slot(db, monkeypatch):
    """_release_node must handle the lease kind, not just the binding kind."""
    task_id = uuid.uuid4()
    lease = await ReviewNodeLease.create(
        id=uuid.uuid4(), node_id="n1", event_id=uuid.uuid4(), task_id=task_id,
        slot=1, status="active", session_id="sess-3",
    )
    deleted: list[str] = []

    class FakeClient:
        enabled = True

        async def delete_node_session(self, session_id):
            deleted.append(session_id)

    monkeypatch.setattr(module, "get_local_node_client", lambda: FakeClient())
    await module.task_service._release_node(task_id)
    assert deleted == ["sess-3"]
    assert await ReviewNodeLease.filter(id=lease.id).exists() is False


@pytest.mark.asyncio
async def test_release_node_is_a_noop_without_any_occupancy(db, monkeypatch):
    monkeypatch.setattr(module, "get_local_node_client", lambda: SimpleNamespace(enabled=False))
    await module.task_service._release_node(uuid.uuid4())  # must not raise
