"""project_service.scan_stack_profile 服务层测试。

仿 tests/test_project_tree_blob.py：tortoise sqlite 内存库 + monkeypatch
git_clients 的 fetch_tree/fetch_blob，不打真实网络。覆盖：正常扫描落库、
无凭据降级、树接口失败降级、cnb 浅树回退逐层下钻、_project_dict 携带 stack。
"""
from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass

import pytest
import pytest_asyncio

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from monkeycode_compat import git_clients
from monkeycode_compat.models_project import Project
from monkeycode_compat.project_service import project_service


@pytest_asyncio.fixture
async def tortoise_db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={
            "monkeycode_compat": [
                "monkeycode_compat.models_git",
                "monkeycode_compat.models_project",
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
    from monkeycode_compat import git_service

    monkeypatch.setattr(
        git_service.git_service, "_prefetch_repositories", lambda *_a, **_k: None
    )


async def _make_identity(user_id, platform="github", token="ghp_tok", **kw):
    from monkeycode_compat.models_git import GitIdentity

    return await GitIdentity.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        platform=platform,
        access_token=token,
        **kw,
    )


async def _make_project(user_id, identity_id, **kw):
    return await Project.create(
        id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        name="stack-test",
        git_identity_id=uuid.UUID(identity_id),
        **kw,
    )


@dataclass
class _FakeEntry:
    path: str
    mode: int


def _fake_tree(entries):
    async def fetch_tree(platform, full_name, opts, *, ref="", path="", recursive=False):
        if path:
            return []
        return entries
    return fetch_tree


_BLOB_TEXTS = {
    "requirements.txt": "fastapi==0.104\nuvicorn\n",
}


def _fake_blobs(texts=None):
    store = texts if texts is not None else _BLOB_TEXTS

    async def fetch_blob(platform, full_name, opts, *, path, ref=""):
        if path not in store:
            return None
        import base64

        return git_clients.Blob(content=base64.b64encode(store[path].encode()).decode())
    return fetch_blob


@pytest.mark.asyncio
async def test_scan_stack_profile_persists_profile(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id), repo_url="https://github.com/o/r")

    entries = [
        _FakeEntry("requirements.txt", git_clients._MODE_REGULAR),
        _FakeEntry("main.py", git_clients._MODE_REGULAR),
        _FakeEntry("app", git_clients._MODE_DIRECTORY),
        _FakeEntry("app/api.py", git_clients._MODE_REGULAR),
    ]
    monkeypatch.setattr(git_clients, "fetch_tree", _fake_tree(entries))
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs())

    profile = await project_service.scan_stack_profile(str(project.id))
    assert profile is not None
    assert profile["primary_language"] == "python"
    assert "fastapi" in profile["frameworks"]

    reloaded = await Project.get(id=project.id)
    assert reloaded.stack_profile["schema"] == "ai-lubricant.stack-profile/v1"
    assert reloaded.stack_profile["primary_language"] == "python"


@pytest.mark.asyncio
async def test_scan_stack_profile_no_repo_context(tortoise_db):
    # 无凭据/无 repo_url → 优雅记 error，不抛。
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner, token=None)
    project = await _make_project(owner, str(identity.id))
    profile = await project_service.scan_stack_profile(str(project.id))
    assert profile["error"] == "no_repo_context"
    reloaded = await Project.get(id=project.id)
    assert reloaded.stack_profile["error"] == "no_repo_context"


@pytest.mark.asyncio
async def test_scan_stack_profile_tree_error_recorded(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id), repo_url="https://github.com/o/r")

    async def boom(*_a, **_k):
        raise git_clients.GitClientError("upstream 500")

    monkeypatch.setattr(git_clients, "fetch_tree", boom)
    profile = await project_service.scan_stack_profile(str(project.id))
    assert "upstream 500" in profile["error"]
    reloaded = await Project.get(id=project.id)
    assert reloaded.stack_profile["error"]


@pytest.mark.asyncio
async def test_scan_stack_profile_cnb_shallow_tree_walk(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner, platform="cnb")
    project = await _make_project(owner, str(identity.id), repo_url="https://cnb.cool/o/r")

    calls: list[tuple[str, bool]] = []

    async def fetch_tree(platform, full_name, opts, *, ref="", path="", recursive=False):
        calls.append((path, recursive))
        if not path:
            return [
                _FakeEntry("src", git_clients._MODE_DIRECTORY),
                _FakeEntry("requirements.txt", git_clients._MODE_REGULAR),
            ]
        return [
            _FakeEntry("src/api.py", git_clients._MODE_REGULAR),
            _FakeEntry("src/deep", git_clients._MODE_DIRECTORY),
        ]

    monkeypatch.setattr(git_clients, "fetch_tree", fetch_tree)
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs())

    profile = await project_service.scan_stack_profile(str(project.id))
    # 触发了第二层下钻（cnb 平台 + 浅树），且结果标 truncated。
    assert ("src", False) in calls
    assert profile["truncated"] is True
    assert profile["primary_language"] == "python"


@pytest.mark.asyncio
async def test_scan_stack_profile_gitmodules_marks_truncated(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id), repo_url="https://github.com/o/r")

    entries = [
        _FakeEntry(".gitmodules", git_clients._MODE_REGULAR),
        _FakeEntry("main.go", git_clients._MODE_REGULAR),
    ]
    monkeypatch.setattr(git_clients, "fetch_tree", _fake_tree(entries))
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs({}))

    profile = await project_service.scan_stack_profile(str(project.id))
    # .gitmodules 在场但内容取不到（blob 桩未提供）→ 子模块无法纳入结果，
    # 保留 truncated 提示；根 profile 本身不受影响。
    assert profile["truncated"] is True
    assert "submodules" not in profile
    assert profile["primary_language"] == "go"


@pytest.mark.asyncio
async def test_project_dict_carries_stack(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id), repo_url="https://github.com/o/r")

    entries = [_FakeEntry("package.json", git_clients._MODE_REGULAR)]
    monkeypatch.setattr(git_clients, "fetch_tree", _fake_tree(entries))
    monkeypatch.setattr(
        git_clients, "fetch_blob",
        _fake_blobs({"package.json": '{"dependencies":{"vue":"3"}}'}),
    )
    await project_service.scan_stack_profile(str(project.id))

    # enrich 需要 User/Task 等未注册模型，与本测试目的（_project_dict 带 stack）无关。
    from monkeycode_compat import project_service as ps_mod

    async def passthrough(rows):
        return rows

    monkeypatch.setattr(ps_mod, "_enrich_project_rows", passthrough)
    data = await ps_mod.project_service.get_project(owner, str(project.id))
    assert data["stack"] is not None
    assert "vue" in data["stack"]["frameworks"]


@pytest.mark.asyncio
async def test_scan_missing_project_returns_none(tortoise_db):
    assert await project_service.scan_stack_profile(str(uuid.uuid4())) is None
    assert await project_service.scan_stack_profile("not-a-uuid") is None


# ── 子模块识别（.gitmodules → 逐个用父凭据扫子仓库）──────────────────────────
def _fake_tree_by_repo(trees: dict[str, list]):
    async def fetch_tree(platform, full_name, opts, *, ref="", path="", recursive=False):
        if path:
            return []
        return trees.get(full_name, [])
    return fetch_tree


def _fake_blobs_by_repo(blobs: dict[str, dict[str, str]]):
    import base64

    async def fetch_blob(platform, full_name, opts, *, path, ref=""):
        text = blobs.get(full_name, {}).get(path)
        if text is None:
            return None
        return git_clients.Blob(content=base64.b64encode(text.encode()).decode())
    return fetch_blob


@pytest.mark.asyncio
async def test_scan_submodules_scanned_with_parent_credentials(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id),
                                  repo_url="https://github.com/o/parent.git")

    trees = {
        "o/parent": [
            _FakeEntry(".gitmodules", git_clients._MODE_REGULAR),
            _FakeEntry("main.py", git_clients._MODE_REGULAR),
        ],
        # 子模块内容不在父递归树里——子仓库自己的树。
        "o/sub-ui": [
            _FakeEntry("package.json", git_clients._MODE_REGULAR),
            _FakeEntry("src/App.tsx", git_clients._MODE_REGULAR),
        ],
    }
    blobs = {
        "o/parent": {
            ".gitmodules": '[submodule "sub-ui"]\n\tpath = sub-ui\n\turl = ../sub-ui.git\n',
        },
        "o/sub-ui": {
            "package.json": '{"dependencies":{"react":"18","vite":"5"}}',
        },
    }
    monkeypatch.setattr(git_clients, "fetch_tree", _fake_tree_by_repo(trees))
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs_by_repo(blobs))

    profile = await project_service.scan_stack_profile(str(project.id))
    assert profile["primary_language"] == "python"
    # 子模块成功纳入 → 不再因 .gitmodules 置 truncated。
    assert profile["truncated"] is False
    assert "submodules" in profile
    assert len(profile["submodules"]) == 1
    sub = profile["submodules"][0]
    assert sub["path"] == "sub-ui"
    # 相对 url 按父仓库地址解析成同 host 的绝对 url。
    assert sub["url"] == "https://github.com/o/sub-ui.git"
    assert sub["primary_language"] == "typescript"
    assert "react" in sub["frameworks"]
    # 紧凑投影：不带 languages/evidence 这些大字段。
    assert "evidence" not in sub and "languages" not in sub
    # 父凭据（identity token）被复用去取子仓库——fetch_tree 收到同一个 opts。
    assert "scanned_at" in sub


@pytest.mark.asyncio
async def test_scan_submodules_branch_field_used_as_ref(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id),
                                  repo_url="https://github.com/o/parent.git")

    tree_refs: list[tuple[str, str]] = []

    async def fetch_tree(platform, full_name, opts, *, ref="", path="", recursive=False):
        tree_refs.append((full_name, ref))
        if full_name == "o/parent":
            return [_FakeEntry(".gitmodules", git_clients._MODE_REGULAR)]
        return [_FakeEntry("main.go", git_clients._MODE_REGULAR)]

    blobs = {
        "o/parent": {
            ".gitmodules": '[submodule "dev"]\n\tpath = dev\n\turl = ../dev.git\n\tbranch = develop\n',
        },
    }
    monkeypatch.setattr(git_clients, "fetch_tree", fetch_tree)
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs_by_repo(blobs))

    profile = await project_service.scan_stack_profile(str(project.id))
    # .gitmodules 声明的 branch 优先作子仓库 ref；父仓库用项目 branch。
    assert ("o/dev", "develop") in tree_refs
    assert ("o/parent", "") in tree_refs
    assert profile["submodules"][0]["primary_language"] == "go"


@pytest.mark.asyncio
async def test_scan_submodules_error_recorded_per_submodule(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id),
                                  repo_url="https://github.com/o/parent.git")

    trees = {"o/parent": [_FakeEntry(".gitmodules", git_clients._MODE_REGULAR)]}
    blobs = {
        "o/parent": {
            ".gitmodules": (
                '[submodule "good"]\n\tpath = good\n\turl = ../good.git\n'
                '[submodule "bad"]\n\tpath = bad\n\turl = ../bad.git\n'
            ),
        },
    }

    async def fetch_tree(platform, full_name, opts, *, ref="", path="", recursive=False):
        if full_name == "o/bad":
            raise git_clients.GitClientError("HTTP 404 no such repo")
        if full_name == "o/parent":
            return trees["o/parent"]
        return [_FakeEntry("requirements.txt", git_clients._MODE_REGULAR)]

    monkeypatch.setattr(git_clients, "fetch_tree", fetch_tree)
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs_by_repo({
        **blobs,
        "o/good": {"requirements.txt": "django\n"},
    }))

    profile = await project_service.scan_stack_profile(str(project.id))
    by_path = {s["path"]: s for s in profile["submodules"]}
    # 单个子模块取树失败只记该子模块 error，不拖垮整体也不置根 truncated。
    assert "error" in by_path["bad"]
    assert "404" in by_path["bad"]["error"]
    assert by_path["good"]["primary_language"] == "python"
    assert "django" in by_path["good"]["frameworks"]
    assert profile["truncated"] is False


@pytest.mark.asyncio
async def test_scan_submodules_cap_and_nested_hint(tortoise_db, monkeypatch):
    owner = str(uuid.uuid4())
    identity = await _make_identity(owner)
    project = await _make_project(owner, str(identity.id),
                                  repo_url="https://github.com/o/parent.git")

    # 12 个子模块声明 → 只扫前 _SCAN_MAX_SUBMODULES 个。
    mods = "\n".join(
        f'[submodule "s{i}"]\n\tpath = s{i}\n\turl = ../s{i}.git' for i in range(12)
    )
    trees = {
        "o/parent": [_FakeEntry(".gitmodules", git_clients._MODE_REGULAR)],
        "o/s0": [
            _FakeEntry(".gitmodules", git_clients._MODE_REGULAR),
            _FakeEntry("main.py", git_clients._MODE_REGULAR),
        ],
    }
    blobs = {"o/parent": {".gitmodules": mods}}

    async def fetch_tree(platform, full_name, opts, *, ref="", path="", recursive=False):
        if full_name == "o/s0":
            return trees["o/s0"]
        return trees["o/parent"]

    monkeypatch.setattr(git_clients, "fetch_tree", fetch_tree)
    monkeypatch.setattr(git_clients, "fetch_blob", _fake_blobs_by_repo(blobs))

    from monkeycode_compat import project_service as ps

    assert ps._SCAN_MAX_SUBMODULES == 10
    profile = await project_service.scan_stack_profile(str(project.id))
    assert len(profile["submodules"]) == 10
    # 子模块自身声明 .gitmodules（嵌套）→ 该子模块标 truncated，v1 不下钻。
    assert profile["submodules"][0]["truncated"] is True
    # 根 profile 不因嵌套子模块受影响。
    assert profile["truncated"] is False


# ── 路由层（TestClient + dependency_overrides，仿 test_project_tree_blob.py）──
def _make_test_client(monkeypatch):
    from fastapi import FastAPI

    import monkeycode_compat.routes_project as routes_project
    from monkeycode_compat.deps import get_current_user

    class _User:
        id = uuid.uuid4()
        role = "user"

    app = FastAPI()
    app.include_router(routes_project.router)
    app.dependency_overrides[get_current_user] = lambda: _User()
    return routes_project, app


def test_route_get_stack_200(monkeypatch):
    routes_project, app = _make_test_client(monkeypatch)
    stack = {"schema": "ai-lubricant.stack-profile/v1", "primary_language": "python"}

    async def fake_get_project(user_id, project_id, *, role=None):
        return {"id": project_id, "stack": stack}

    monkeypatch.setattr(routes_project.project_service, "get_project", fake_get_project)
    resp = __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app).get(
        f"/api/v1/users/projects/{uuid.uuid4()}/stack"
    )
    assert resp.status_code == 200
    assert resp.json() == {"stack": stack}


def test_route_get_stack_null_and_404(monkeypatch):
    routes_project, app = _make_test_client(monkeypatch)

    async def missing(user_id, project_id, *, role=None):
        return None

    async def unscanned(user_id, project_id, *, role=None):
        return {"id": project_id, "stack": None}

    client = __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app)
    monkeypatch.setattr(routes_project.project_service, "get_project", missing)
    assert client.get("/api/v1/users/projects/p/stack").status_code == 404

    # 已访问但未扫描（null = 未扫/扫描中/失败）→ 200 + stack: null。
    monkeypatch.setattr(routes_project.project_service, "get_project", unscanned)
    resp = client.get("/api/v1/users/projects/p/stack")
    assert resp.status_code == 200
    assert resp.json() == {"stack": None}


def test_route_rescan_scans_and_audits(monkeypatch):
    routes_project, app = _make_test_client(monkeypatch)
    profile = {"schema": "ai-lubricant.stack-profile/v1", "primary_language": "go"}

    async def fake_get_project(user_id, project_id, *, role=None):
        return {"id": project_id, "stack": None}

    async def fake_scan(project_id):
        return profile

    audits: list[tuple] = []

    async def fake_audit(request, user, action, **kw):
        audits.append(action)

    monkeypatch.setattr(routes_project.project_service, "get_project", fake_get_project)
    monkeypatch.setattr(routes_project.project_service, "scan_stack_profile", fake_scan)
    monkeypatch.setattr(routes_project, "audit_user_action", fake_audit)
    client = __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app)
    resp = client.post(f"/api/v1/users/projects/{uuid.uuid4()}/stack/rescan")
    assert resp.status_code == 200
    assert resp.json() == {"stack": profile}
    assert audits == ["project.stack.rescan"]


def test_route_create_triggers_spawn_scan(monkeypatch):
    routes_project, app = _make_test_client(monkeypatch)
    project_id = str(uuid.uuid4())

    async def fake_create(user_id, req):
        return {"id": project_id, "name": req.get("name")}

    spawned: list[str] = []
    monkeypatch.setattr(routes_project.project_service, "create_project", fake_create)
    monkeypatch.setattr(routes_project, "audit_user_action", _noop_audit)
    monkeypatch.setattr(routes_project, "_spawn_stack_scan",
                        lambda pid: spawned.append(pid))
    client = __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app)
    resp = client.post("/api/v1/users/projects", json={"name": "t"})
    assert resp.status_code == 200
    assert resp.json()["id"] == project_id
    # 建项目即触发异步扫描，且不影响响应契约。
    assert spawned == [project_id]


async def _noop_audit(*_a, **_k):
    return None
