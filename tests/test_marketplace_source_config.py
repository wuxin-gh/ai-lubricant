"""env-only 市场源配置：repo_url / github_token 只认 .env，DB 旧值被忽略与清除。

仓库地址与写入 token 不再存 DB、也不再从 DB 读；改只能在服务端 .env（MARKETPLACE_REPO_URL /
MARKETPLACE_GITHUB_TOKEN）。这几个用例锁住该边界，并确认 _valid_module 无条件放行发行模块
（modules 白名单漏列 mobile-versions 时历史版本等索引浏览也不 400）。
"""
from __future__ import annotations

import asyncio
import sys
import types

import pytest

from user_platform.marketplace import config as mp_config
from user_platform.marketplace.source_config import (
    DEFAULT_REPO_URL,
    _normalize,
    public_view,
    update_source_config,
)


# ── 假 ``config`` 模块：避免 import 真实 server/config.py（会构造 Redis/PG store）────

class _FakeStore:
    def __init__(self, blob: dict | None = None):
        self._blob = blob or {}
        self.writes: list[dict] = []

    def read_main(self) -> dict:
        return self._blob

    async def read_main_async(self) -> dict:
        return self._blob

    def write_main(self, blob: dict) -> None:
        self.writes.append(blob)

    async def write_main_async(self, blob: dict) -> None:
        self.writes.append(blob)


@pytest.fixture
def fake_config(monkeypatch):
    store = _FakeStore()
    module = types.ModuleType("config")
    module.CONFIG_STORE = store
    monkeypatch.setitem(sys.modules, "config", module)
    return store


def _clean_env(monkeypatch):
    for name in (
        "MARKETPLACE_REPO_URL", "MARKETPLACE_GITHUB_REPO_URL",
        "MARKETPLACE_GITHUB_OWNER", "MARKETPLACE_GITHUB_REPO",
        "MARKETPLACE_GITHUB_BRANCH", "MARKETPLACE_GITHUB_TOKEN",
        "MARKETPLACE_MODULES", "MARKETPLACE_INDEX_NAME", "MARKETPLACE_PROXY_ID",
    ):
        monkeypatch.delenv(name, raising=False)


# ── _normalize / public_view ──────────────────────────────────────────────────

def test_normalize_drops_repo_and_token():
    cfg = _normalize({"repo_url": "https://github.com/db/legacy", "github_token": "secret"})
    assert "repo_url" not in cfg
    assert "github_token" not in cfg


def test_public_view_no_repo_or_token_fields():
    view = public_view(
        _normalize({}),
        {
            "repo_url": "https://github.com/x/y", "owner": "x", "repo": "y",
            "branch": "main", "modules": [], "index_name": "index.json",
            "enabled": True, "writable": False,
        },
    )
    assert "repo_url" not in view
    assert "github_token_set" not in view
    assert "default_repo_url" not in view
    # 已生效值仍在（仓库地址唯一可信来源）。
    assert view["effective"]["repo_url"] == "https://github.com/x/y"


# ── update_source_config：patch 带 repo_url / github_token 被忽略 ─────────────

def test_update_source_config_ignores_repo_and_token(fake_config, monkeypatch):
    _clean_env(monkeypatch)
    monkeypatch.setenv("MARKETPLACE_REPO_URL", "https://github.com/env/repo")
    fake_config._blob = {}  # DB 里啥都没有

    merged = asyncio.run(update_source_config({
        "repo_url": "https://github.com/patched/repo",
        "github_token": "should-be-ignored",
        "proxy_id": "p1",
    }))

    assert merged["proxy_id"] == "p1"
    assert "repo_url" not in merged
    assert "github_token" not in merged
    # 落库的 blob 里也不该再带这两项。
    written = fake_config.writes[-1]["marketplace"]
    assert "repo_url" not in written
    assert "github_token" not in written
    assert written["proxy_id"] == "p1"


# ── load_settings：env 优先于 DB 旧值 ─────────────────────────────────────────

def test_load_settings_repo_env_overrides_legacy_db(fake_config, monkeypatch):
    _clean_env(monkeypatch)
    monkeypatch.setenv("MARKETPLACE_REPO_URL", "https://github.com/env/repo")
    # DB 里残留旧值——读取必须忽略它。
    fake_config._blob = {"marketplace": {
        "repo_url": "https://github.com/db/legacy",
        "github_token": "db-token",
    }}

    settings = mp_config.load_settings()

    assert settings.github_owner == "env"
    assert settings.github_repo == "repo"
    assert settings.github_token == ""  # DB 里的 db-token 被忽略


def test_load_settings_falls_back_to_default_when_no_env(fake_config, monkeypatch):
    _clean_env(monkeypatch)
    fake_config._blob = {"marketplace": {"repo_url": "https://github.com/db/legacy"}}

    settings = mp_config.load_settings()

    assert settings.repo_url == DEFAULT_REPO_URL
    assert settings.github_token == ""


# ── _valid_module：发行模块无条件放行 ──────────────────────────────────────────

def test_valid_module_release_modules_bypass(monkeypatch):
    from user_platform.marketplace import routes

    settings = mp_config.MarketplaceSettings(
        repo_url="https://github.com/o/r", github_owner="o", github_repo="r",
        github_branch="main", github_token="", modules=("mcp",),
        index_name="index.json", proxy_id="",
    )
    monkeypatch.setattr(mp_config, "settings", settings)

    assert routes._valid_module("mcp") is True            # 在白名单里
    assert routes._valid_module("node-versions") is True   # 发行模块，放行
    assert routes._valid_module("mobile-versions") is True  # 发行模块，放行（modules 没列）
    assert routes._valid_module("channels") is False       # 不在白名单、非发行模块
    assert routes._valid_module("bogus") is False
