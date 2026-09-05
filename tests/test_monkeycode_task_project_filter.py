"""Project-filtered task listing for the manager project detail page.

``TaskService.list_tasks`` grew an optional ``project_id`` filter so the
manager project-detail task tab reads only that project's tasks (the prior
behavior returned every task the caller could see, ignoring project_id).
These tests pin the filter against an in-memory SQLite DB:

* normal users stay user-scoped even with a project_id (no cross-user access);
* admins cross users but still limited to the requested project;
* no project_id keeps the legacy behavior;
* an invalid project_id yields an empty result without raising.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={
            "monkeycode_compat": [
                "monkeycode_compat.models_task",
            ]
        },
        use_tz=False,
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def _seed(user_id: uuid.UUID, project_id: uuid.UUID, other_project_id: uuid.UUID):
    """Create one task in project, one in the other project, one unlinked."""
    from monkeycode_compat.models_task import ProjectTask, Task

    in_project = await Task.create(
        id=uuid.uuid4(), user_id=user_id, kind="develop", content="in project",
        status="pending",
    )
    in_other = await Task.create(
        id=uuid.uuid4(), user_id=user_id, kind="develop", content="other project",
        status="finished",
    )
    unlinked = await Task.create(
        id=uuid.uuid4(), user_id=user_id, kind="develop", content="unlinked",
        status="pending",
    )
    await ProjectTask.create(
        id=uuid.uuid4(), task_id=in_project.id, model_id=uuid.UUID(int=0),
        image_id=uuid.UUID(int=0), project_id=project_id,
    )
    await ProjectTask.create(
        id=uuid.uuid4(), task_id=in_other.id, model_id=uuid.UUID(int=0),
        image_id=uuid.UUID(int=0), project_id=other_project_id,
    )
    return in_project, in_other, unlinked


@pytest.mark.asyncio
async def test_list_tasks_filtered_by_project_for_normal_user(db):
    user_id = uuid.uuid4()
    project_id = uuid.uuid4()
    other_id = uuid.uuid4()
    in_project, in_other, unlinked = await _seed(user_id, project_id, other_id)

    from monkeycode_compat.task_service import task_service

    result = await task_service.list_tasks(
        str(user_id), role="individual", page=1, page_size=50, project_id=str(project_id)
    )
    ids = {row["id"] for row in result.rows}
    assert ids == {str(in_project.id)}
    assert result.total == 1
    # Other project's task and the unlinked task are excluded.
    assert str(in_other.id) not in ids
    assert str(unlinked.id) not in ids


@pytest.mark.asyncio
async def test_list_tasks_admin_cross_user_but_project_filtered(db):
    owner = uuid.uuid4()
    project_id = uuid.uuid4()
    other_id = uuid.uuid4()
    in_project, in_other, unlinked = await _seed(owner, project_id, other_id)

    from monkeycode_compat.task_service import task_service

    # Admin sees across users, but the project filter still applies.
    result = await task_service.list_tasks(
        str(uuid.uuid4()), role="admin", page=1, page_size=50, project_id=str(project_id)
    )
    ids = {row["id"] for row in result.rows}
    assert ids == {str(in_project.id)}
    assert result.total == 1


@pytest.mark.asyncio
async def test_list_tasks_without_project_keeps_legacy_behavior(db):
    user_id = uuid.uuid4()
    project_id = uuid.uuid4()
    other_id = uuid.uuid4()
    await _seed(user_id, project_id, other_id)

    from monkeycode_compat.task_service import task_service

    # No project_id → every visible task returned (legacy behavior).
    result = await task_service.list_tasks(str(user_id), role="admin", page=1, page_size=50)
    assert result.total == 3


@pytest.mark.asyncio
async def test_list_tasks_invalid_project_id_returns_empty(db):
    user_id = uuid.uuid4()

    from monkeycode_compat.task_service import task_service

    result = await task_service.list_tasks(
        str(user_id), role="admin", page=1, page_size=50, project_id="not-a-uuid"
    )
    assert result.total == 0
    assert result.rows == []


@pytest.mark.asyncio
async def test_list_tasks_filters_statuses_and_paginates(db):
    user_id = uuid.uuid4()
    project_id = uuid.uuid4()
    other_id = uuid.uuid4()
    await _seed(user_id, project_id, other_id)

    from monkeycode_compat.task_service import task_service

    result = await task_service.list_tasks(
        str(user_id), role="individual", page=1, page_size=50,
        project_id=str(project_id), statuses=["finished"],
    )
    assert result.total == 0
    assert result.rows == []

    result = await task_service.list_tasks(
        str(user_id), role="individual", page=1, page_size=50,
        project_id=str(project_id), statuses=["pending"],
    )
    assert result.total == 1
    assert [row["status"] for row in result.rows] == ["pending"]
