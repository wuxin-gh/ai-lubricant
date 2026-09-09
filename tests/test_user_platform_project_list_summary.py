"""Manager project-list summary enrichment (creator / requirement count / editor count).

The manager project list page reads ``creator``, ``issue_count`` (requirement-only
in this surface), ``task_count`` and ``editor_count``. The backend previously
returned only base project fields + the ``editors`` array, so creator fell back
to ``-`` and counts were always 0. These tests pin the enrichment against an
in-memory SQLite DB:

* creator resolves to the owning user's DomainUser shape (name/email/id);
* requirement count excludes bugs (the manager "需求" column must not sum bugs);
* task count reflects ProjectTask bindings for the project only;
* zero values are returned explicitly, not dropped;
* pagination totals/rows are unchanged;
* the route derives ``editor_count`` from the existing bulk editor summaries
  helper in a single call, regardless of how many editors each project has.
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
            "user_platform": [
                "user_platform.models",
                "user_platform.models_project",
                "user_platform.models_task",
            ]
        },
        use_tz=False,
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def _make_user(name: str, email: str) -> uuid.UUID:
    from user_platform.models import User

    user = await User.create(id=uuid.uuid4(), name=name, email=email)
    return user.id


async def _make_project(user_id: uuid.UUID, name: str) -> uuid.UUID:
    from user_platform.models_project import Project

    project = await Project.create(id=uuid.uuid4(), user_id=user_id, name=name)
    return project.id


@pytest.mark.asyncio
async def test_list_projects_enriches_creator_requirement_and_task_counts(db):
    from user_platform.models_project import ProjectIssue
    from user_platform.models_task import ProjectTask, Task
    from user_platform.project_service import project_service

    owner_id = await _make_user("张三", "zhangsan@example.com")
    other_id = await _make_user("李四", "lisi@example.com")
    project_a = await _make_project(owner_id, "项目A")
    project_b = await _make_project(other_id, "项目B")

    # project_a: 2 requirements + 1 bug; project_b: 1 requirement.
    await ProjectIssue.create(
        id=uuid.uuid4(), user_id=owner_id, project_id=project_a,
        issue_type="requirement", status="unassigned", title="需求1",
    )
    await ProjectIssue.create(
        id=uuid.uuid4(), user_id=owner_id, project_id=project_a,
        issue_type="requirement", status="unassigned", title="需求2",
    )
    await ProjectIssue.create(
        id=uuid.uuid4(), user_id=owner_id, project_id=project_a,
        issue_type="bug", status="unassigned", title="bug1",
    )
    await ProjectIssue.create(
        id=uuid.uuid4(), user_id=other_id, project_id=project_b,
        issue_type="requirement", status="unassigned", title="需求B",
    )

    # project_a: 2 tasks; project_b: 0.
    t1 = await Task.create(
        id=uuid.uuid4(), user_id=owner_id, kind="develop", content="t1", status="pending",
    )
    t2 = await Task.create(
        id=uuid.uuid4(), user_id=owner_id, kind="develop", content="t2", status="pending",
    )
    await ProjectTask.create(
        id=uuid.uuid4(), task_id=t1.id, model_id=uuid.UUID(int=0),
        image_id=uuid.UUID(int=0), project_id=project_a,
    )
    await ProjectTask.create(
        id=uuid.uuid4(), task_id=t2.id, model_id=uuid.UUID(int=0),
        image_id=uuid.UUID(int=0), project_id=project_a,
    )

    result = await project_service.list_projects(
        str(uuid.uuid4()), role="admin", page=1, page_size=50,
    )
    rows = {row["id"]: row for row in result.rows}

    a = rows[str(project_a)]
    b = rows[str(project_b)]
    assert a["creator"]["id"] == str(owner_id)
    assert a["creator"]["name"] == "张三"
    assert a["creator"]["email"] == "zhangsan@example.com"
    assert a["issue_count"] == 2  # requirements only, bug excluded
    assert a["task_count"] == 2
    assert b["creator"]["id"] == str(other_id)
    assert b["issue_count"] == 1
    assert b["task_count"] == 0
    assert result.total == 2


@pytest.mark.asyncio
async def test_list_projects_creator_none_when_owner_soft_deleted(db):
    from user_platform.models import User
    from user_platform.models_project import Project
    from user_platform.project_service import project_service

    owner = await User.create(id=uuid.uuid4(), name="软删", email="gone@example.com")
    await Project.create(id=uuid.uuid4(), user_id=owner.id, name="孤儿项目")
    owner.is_deleted = True
    await owner.save(update_fields=["is_deleted"])

    result = await project_service.list_projects(
        str(uuid.uuid4()), role="admin", page=1, page_size=50,
    )
    assert len(result.rows) == 1
    assert result.rows[0]["creator"] is None
    assert result.rows[0]["issue_count"] == 0
    assert result.rows[0]["task_count"] == 0


@pytest.mark.asyncio
async def test_list_projects_empty_page_skips_enrichment(db):
    from user_platform.project_service import project_service

    # No projects at all → empty page returned without enrichment queries.
    result = await project_service.list_projects(
        str(uuid.uuid4()), role="admin", page=1, page_size=50,
    )
    assert result.total == 0
    assert result.rows == []


@pytest.mark.asyncio
async def test_list_projects_pagination_preserved(db):
    from user_platform.project_service import project_service

    owner_id = await _make_user("分页", "page@example.com")
    for i in range(3):
        await _make_project(owner_id, f"项目{i}")

    page1 = await project_service.list_projects(
        str(uuid.uuid4()), role="admin", page=1, page_size=2,
    )
    page2 = await project_service.list_projects(
        str(uuid.uuid4()), role="admin", page=2, page_size=2,
    )
    assert page1.total == 3 and page1.page == 1 and page1.page_size == 2
    assert len(page1.rows) == 2
    assert len(page2.rows) == 1
    page1_ids = {r["id"] for r in page1.rows}
    assert page2.rows[0]["id"] not in page1_ids
    # Enrichment still applies on each page.
    assert page1.rows[0]["creator"] is not None
    assert page1.rows[0]["task_count"] == 0


@pytest.mark.asyncio
async def test_list_projects_normal_user_scoped_to_owned_and_collab(db):
    from user_platform.models_project import ProjectCollaborator
    from user_platform.project_service import project_service

    owner_id = await _make_user("所有者", "owner@example.com")
    viewer_id = await _make_user("访客", "viewer@example.com")
    stranger_id = await _make_user("陌生人", "stranger@example.com")
    mine = await _make_project(owner_id, "我的")
    shared = await _make_project(viewer_id, "我拥有的")
    private = await _make_project(stranger_id, "别人私有")

    await ProjectCollaborator.create(
        id=uuid.uuid4(), project_id=mine, user_id=viewer_id, role="read_only",
    )

    result = await project_service.list_projects(
        str(viewer_id), role="individual", page=1, page_size=50,
    )
    ids = {r["id"] for r in result.rows}
    assert ids == {str(mine), str(shared)}
    assert str(private) not in ids


@pytest.mark.asyncio
async def test_route_adds_editor_count_from_bulk_summaries(db, monkeypatch):
    """The route must derive editor_count from the existing bulk helper in one call."""
    from user_platform import routes_project
    from user_platform.models import User
    from user_platform.models_project import Project

    owner = await User.create(id=uuid.uuid4(), name="路由", email="route@example.com")
    project1 = await Project.create(id=uuid.uuid4(), user_id=owner.id, name="带编辑器")
    project2 = await Project.create(id=uuid.uuid4(), user_id=owner.id, name="无编辑器")
    pid1, pid2 = str(project1.id), str(project2.id)

    captured: dict[str, list[str]] = {"calls": [], "ids": []}

    async def fake_summaries(project_ids):
        captured["calls"].append(list(project_ids))
        captured["ids"] = list(project_ids)
        return {
            pid1: [
                {"id": "e1", "project_id": pid1, "name": "ed1",
                 "provider": "p", "status": "active", "session_count": 0, "sessions": []},
                {"id": "e2", "project_id": pid1, "name": "ed2",
                 "provider": "p", "status": "active", "session_count": 0, "sessions": []},
            ],
            pid2: [],
        }

    monkeypatch.setattr(
        routes_project.PostgresClient, "list_editor_summaries_for_projects",
        classmethod(lambda cls, ids: fake_summaries(ids)),
    )

    # The route imports get_current_user from .deps; pass a lightweight user stand-in.
    class _StubUser:
        id = owner.id
        role = "admin"

    result = await routes_project.list_projects(user=_StubUser(), page=1, page_size=50)

    assert result["total"] == 2
    rows = {r["id"]: r for r in result["rows"]}
    assert rows[pid1]["editor_count"] == 2
    assert len(rows[pid1]["editors"]) == 2
    assert rows[pid2]["editor_count"] == 0
    assert rows[pid2]["editors"] == []
    # Single bulk call covering the current-page project ids.
    assert len(captured["calls"]) == 1
    assert set(captured["ids"]) == {pid1, pid2}
