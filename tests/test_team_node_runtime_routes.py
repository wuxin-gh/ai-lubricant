"""用户侧执行节点运行时管理路由的守卫回归测试。

2026-09 新增的 ``/api/v1/teams/nodes/*`` 通道（删除/统一升级/编辑器升级/升级代理/
详情/upgrade-defaults/latest-release）把管理端能力下沉到用户侧。权限口径：
``user_can_use_node`` 派生管理权（直接绑定或经所属已绑定管理节点派生）+ 一道
「仅执行节点」硬闸——管理节点在用户侧恒只读，删除/升级一律 422。

这里用 mock 替身测守卫分支（403 无权 / 422 管理节点 / 404 不存在 / 200 路由挂载），
不打真实控制面与 DB。
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from user_platform import routes_nodes
from user_platform.deps import get_current_user
from user_platform.models import User

EXEC_NODE = {
    "node_id": "n-1",
    "role": "execution",
    "is_passive": False,
    "status": "approved",
    "last_proxy_config_id": "px-9",
}
MGMT_NODE = {
    "node_id": "m-1",
    "role": "management",
    "is_passive": False,
    "status": "approved",
}


@pytest.fixture()
def client():
    app = FastAPI()
    app.include_router(routes_nodes.team_router)
    # 伪造登录用户：只测守卫分支，不落库。
    app.dependency_overrides[get_current_user] = lambda: User(
        id=uuid.uuid4(), email="t@example.com", password_hash="x", role="user"
    )
    return TestClient(app)


def _guard_case(client, method: str, path: str, *, can_use, node_row, expect_status: int):
    from contextlib import ExitStack

    async def fake_can_use(user_id, node_id):
        return can_use if can_use else None

    async def fake_get(node_id):
        return node_row

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(
                routes_nodes.nodes_service,
                "user_can_use_node",
                new=AsyncMock(side_effect=fake_can_use),
            )
        )
        stack.enter_context(
            patch.object(
                routes_nodes.nodes_service,
                "get_node_if_exists",
                new=AsyncMock(side_effect=fake_get),
            )
        )
        call = getattr(client, method)
        r = call(path, json={}) if method in ("post", "put") else call(path)
    assert r.status_code == expect_status, (method, path, r.status_code, r.text[:200])


# ── 路由挂载：全部可达（401/422/403 任一非 404 都证明已注册）────────────────

def test_team_runtime_routes_mounted(client):
    """路由全部可达（非 404）。user_can_use_node 打桩避免落进 Tortoise 查询——
    mounted 检查只关心路由注册，守卫行为由下面三个用例覆盖。"""
    paths = [
        ("get", "/api/v1/teams/nodes/latest-release"),
        ("get", "/api/v1/teams/nodes/n-1"),
        ("get", "/api/v1/teams/nodes/n-1/upgrade-defaults"),
        ("post", "/api/v1/teams/nodes/n-1/upgrade"),
        ("post", "/api/v1/teams/nodes/n-1/editors/claude/upgrade"),
        ("get", "/api/v1/teams/nodes/n-1/proxy-config"),
        ("put", "/api/v1/teams/nodes/n-1/proxy-config"),
        ("delete", "/api/v1/teams/nodes/n-1"),
    ]

    async def fake_can_use(user_id, node_id):
        return None

    async def fake_get(node_id):
        return EXEC_NODE

    from contextlib import ExitStack

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(
                routes_nodes.nodes_service,
                "user_can_use_node",
                new=AsyncMock(side_effect=fake_can_use),
            )
        )
        stack.enter_context(
            patch.object(
                routes_nodes.nodes_service,
                "get_node_if_exists",
                new=AsyncMock(side_effect=fake_get),
            )
        )
        for method, path in paths:
            call = getattr(client, method)
            r = call(path, json={}) if method in ("post", "put") else call(path)
            assert r.status_code != 404, f"{method} {path} not mounted: {r.status_code}"


# ── execution_only 硬闸：管理节点一律 422，无权 403，节点不在台账 404 ────────

@pytest.mark.parametrize(
    "method,path",
    [
        ("delete", "/api/v1/teams/nodes/m-1"),
        ("post", "/api/v1/teams/nodes/m-1/upgrade"),
        ("get", "/api/v1/teams/nodes/m-1/upgrade-defaults"),
        ("put", "/api/v1/teams/nodes/m-1/proxy-config"),
        ("post", "/api/v1/teams/nodes/m-1/editors/claude/upgrade"),
    ],
)
def test_management_node_rejected(client, method, path):
    _guard_case(client, method, path, can_use="bind-ok", node_row=MGMT_NODE, expect_status=422)


@pytest.mark.parametrize(
    "method,path",
    [
        ("delete", "/api/v1/teams/nodes/n-1"),
        ("post", "/api/v1/teams/nodes/n-1/upgrade"),
        ("get", "/api/v1/teams/nodes/n-1"),
    ],
)
def test_no_grant_rejected(client, method, path):
    _guard_case(client, method, path, can_use=None, node_row=EXEC_NODE, expect_status=403)


@pytest.mark.parametrize(
    "method,path",
    [
        ("delete", "/api/v1/teams/nodes/n-1"),
        ("post", "/api/v1/teams/nodes/n-1/upgrade"),
    ],
)
def test_granted_but_node_missing(client, method, path):
    _guard_case(client, method, path, can_use="bind-ok", node_row=None, expect_status=404)


def test_upgrade_editor_whitelist(client):
    """非白名单编辑器 400（与管理端 install 路由同口径）。"""
    _guard_case(
        client, "post", "/api/v1/teams/nodes/n-1/editors/vim/upgrade",
        can_use="bind-ok", node_row=EXEC_NODE, expect_status=400,
    )
