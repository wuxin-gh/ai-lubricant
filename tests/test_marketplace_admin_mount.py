"""市场管理接口挂载行为的 HTTP 层回归测试。

历史 bug：``admin_router`` 只在进程启动时按当时的 ``settings.writable`` 决定是否
挂载，而「资源中心 → 配置」保存 github_token 是热更新（reload_settings）。启动后
才配 token 的部署里 ``/status`` 立即报 writable=true（导航与市场管理页出现），但
运行中的进程里 ``/admin/*`` 从未挂载——点任何 tab 都 404，重启才能恢复。

修复后两组路由常驻挂载，「是否可写」由 ``_require_market_writable`` 在请求时判定。
这里的用例把 router 挂一次（模拟启动），之后只切换配置——同一条请求从 404 变 200，
证明热配置无需重启即可生效。
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from user_platform.marketplace import config as mp_config
from user_platform.marketplace import routes
from user_platform.marketplace.config import MarketplaceSettings

READONLY_SETTINGS = MarketplaceSettings(
    repo_url="https://github.com/acme/market",
    github_owner="acme",
    github_repo="market",
    github_branch="main",
    github_token="",
    modules=("mcp", "plugins", "skills", "channels", "prompts", "node-versions", "mobile-versions"),
    index_name="index.json",
    proxy_id="",
)

WRITABLE_SETTINGS = MarketplaceSettings(
    repo_url="https://github.com/acme/market",
    github_owner="acme",
    github_repo="market",
    github_branch="main",
    github_token="tok",
    modules=("mcp", "plugins", "skills", "channels", "prompts", "node-versions", "mobile-versions"),
    index_name="index.json",
    proxy_id="",
)


class _FakeAdminUser:
    role = "admin"


class _FakeGitHubClient:
    """只够 _read_index 用的假客户端：索引存在但为空。"""

    async def read_json_or_none(self, path, use_cache=True):
        return ({"schema": "x", "items": []}, "sha")


@pytest.fixture()
def client(monkeypatch):
    # 挂载只发生这一次（模拟进程启动）；后续用例不再动路由，只切配置。
    app = FastAPI()
    app.include_router(routes.router)
    app.include_router(routes.admin_router)
    # _require_admin 只检查 user.role，假用户足够。
    app.dependency_overrides[routes.get_current_user] = lambda: _FakeAdminUser()
    # 默认落到可写状态之外的读侧基线；个别用例再覆盖。
    monkeypatch.setattr(mp_config, "settings", READONLY_SETTINGS)
    return TestClient(app)


def test_admin_catalog_404_when_token_missing(client):
    """未配 github_token：/admin/* 仍要保持「不存在」的 404 语义。"""
    resp = client.get("/api/v1/marketplace/admin/catalog", params={"module": "mcp"})
    assert resp.status_code == 404


def test_admin_catalog_works_after_hot_config_without_restart(client, monkeypatch):
    """核心回归：启动后（挂载后）才配 token，同一请求无需重启即从 404 变 200。"""
    assert client.get("/api/v1/marketplace/admin/catalog", params={"module": "mcp"}).status_code == 404

    # 「资源中心 → 配置」保存后的效果：reload_settings 重绑 mp_config.settings。
    monkeypatch.setattr(mp_config, "settings", WRITABLE_SETTINGS)
    monkeypatch.setattr(routes, "_client", lambda: _FakeGitHubClient())

    resp = client.get("/api/v1/marketplace/admin/catalog", params={"module": "mcp"})
    assert resp.status_code == 200
    assert resp.json()["items"] == []


def test_status_reports_writable_without_memoization(client, monkeypatch):
    """/status 是导航显隐的探测口，必须永远可答且跟随当前配置。"""
    assert client.get("/api/v1/marketplace/status").json()["writable"] is False
    monkeypatch.setattr(mp_config, "settings", WRITABLE_SETTINGS)
    assert client.get("/api/v1/marketplace/status").json()["writable"] is True


def test_admin_rejects_non_admin_even_when_writable(client, monkeypatch):
    """可写状态只放行配置，不放松鉴权：普通用户仍要被 _require_admin 拒掉。"""
    monkeypatch.setattr(mp_config, "settings", WRITABLE_SETTINGS)
    client.app.dependency_overrides[routes.get_current_user] = lambda: type("U", (), {"role": "user"})()
    resp = client.get("/api/v1/marketplace/admin/catalog", params={"module": "mcp"})
    assert resp.status_code == 403


def test_admin_catalog_is_store_only_never_hits_github(client, monkeypatch):
    """目录读取只打本地 store，绝不回退 GitHub：未填充时返回空目录，不调 _client。"""
    monkeypatch.setattr(mp_config, "settings", WRITABLE_SETTINGS)

    def _boom(*args, **kwargs):
        raise AssertionError("catalog must not call the GitHub client in a request")

    monkeypatch.setattr(routes, "_client", _boom)
    # 没有 PG pool → store.list_summaries 返回 []；catalog 应直接回空目录。
    resp = client.get("/api/v1/marketplace/admin/catalog", params={"module": "mcp"})
    assert resp.status_code == 200
    assert resp.json()["items"] == []


def test_admin_export_is_store_only_never_hits_github(client, monkeypatch):
    """导出同样只读本地 store；未填充时各模块为空，不现拉 GitHub。"""
    monkeypatch.setattr(mp_config, "settings", WRITABLE_SETTINGS)

    def _boom(*args, **kwargs):
        raise AssertionError("export must not call the GitHub client in a request")

    monkeypatch.setattr(routes, "_client", _boom)
    resp = client.get("/api/v1/marketplace/admin/export")
    assert resp.status_code == 200
    body = resp.json()
    # 各模块都在导出结构里但内容为空（本地 store 未填充），不现拉 GitHub。
    assert set(body["modules"]) == set(WRITABLE_SETTINGS.modules)
    for mod in body["modules"].values():
        assert mod["index"]["items"] == []
        assert mod["manifests"] == {}


def test_consumer_index_returns_empty_without_network(client, monkeypatch):
    """只读消费侧：store 未填充且缓存未预热时返回空索引，不在请求线程出网。"""
    from user_platform.marketplace import consumer_cache

    consumer_cache._cache.clear()

    def _boom(*args, **kwargs):
        raise AssertionError("consumer/index must not fetch in the request thread")

    monkeypatch.setattr(consumer_cache, "get_raw", _boom)
    monkeypatch.setattr(consumer_cache, "_fetch_raw", _boom)
    resp = client.get("/api/v1/marketplace/consumer/index/mcp")
    assert resp.status_code == 200
    assert resp.json()["items"] == []


def test_consumer_status_uses_peek_not_get_raw(client, monkeypatch):
    """/consumer/status 只读后台预热的 marker 快照，不现拉。"""
    from user_platform.marketplace import consumer_cache

    consumer_cache._cache.clear()

    def _boom(*args, **kwargs):
        raise AssertionError("consumer/status must not fetch in the request thread")

    monkeypatch.setattr(consumer_cache, "get_raw", _boom)
    resp = client.get("/api/v1/marketplace/consumer/status")
    assert resp.status_code == 200
    # 缓存未预热 → 未验证（verified=False），但请求必须秒回，不抛错。
    assert resp.json()["verified"] is False
