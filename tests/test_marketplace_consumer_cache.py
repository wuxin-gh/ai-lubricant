"""consumer_cache 行为回归：read-through、TTL、404 缓存、写失效、失败回旧快照。

用例锁住与 github.py 读缓存同源的几个正确性边界：缓存命中不打网络、404 也缓存、
管理端写路径按模块失效、远端失败时保留最后一次成功快照（消费侧降级而非空白）。
"""
from __future__ import annotations

import asyncio

import pytest

from user_platform.git_clients import GitClientError
from user_platform.marketplace import consumer_cache
from user_platform.marketplace.config import (
    MarketplaceConsumerSettings,
    MarketplaceSettings,
)


@pytest.fixture(autouse=True)
def _clean_cache():
    consumer_cache._cache.clear()
    yield
    consumer_cache._cache.clear()


class _FetchRecorder:
    """假出网：记录调用并可按 path 返回数据 / 抛 404 / 抛 500。"""

    def __init__(self):
        self.calls: list[str] = []
        self.files: dict[str, dict] = {}
        self.missing: set[str] = set()
        self.broken: set[str] = set()

    async def __call__(self, path: str):
        self.calls.append(path)
        if path in self.broken:
            raise GitClientError(f"GET {path} returned HTTP 502")
        if path in self.missing or path not in self.files:
            raise GitClientError(f"GET {path} returned HTTP 404")
        return self.files[path]


@pytest.fixture
def fetch(monkeypatch):
    recorder = _FetchRecorder()
    monkeypatch.setattr(consumer_cache, "_fetch_raw", recorder)
    return recorder


def _settings(modules=("mcp", "skills")):
    return MarketplaceSettings(
        repo_url="https://github.com/o/r", github_owner="o", github_repo="r",
        github_branch="main", github_token="", modules=modules,
        index_name="index.json", proxy_id="",
    )


# ── read-through 与命中 ───────────────────────────────────────────────────────

def test_get_raw_fetches_once_then_hits(fetch):
    fetch.files["modules/mcp/index.json"] = {"items": [{"id": "a"}]}

    data1, stale1 = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))
    data2, stale2 = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))

    assert fetch.calls.count("modules/mcp/index.json") == 1
    assert data1 == data2 == {"items": [{"id": "a"}]}
    assert stale1 is False and stale2 is False


def test_get_raw_returns_deep_copies(fetch):
    fetch.files["modules/mcp/index.json"] = {"items": [{"id": "a"}]}

    data, _ = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))
    data["items"].append({"id": "mutated"})

    again, _ = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))
    assert again == {"items": [{"id": "a"}]}  # 调用方改写不污染缓存本体


def test_get_raw_fresh_bypasses_cache(fetch):
    fetch.files["modules/mcp/index.json"] = {"v": 1}

    asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))
    fetch.files["modules/mcp/index.json"] = {"v": 2}
    data, _ = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json", fresh=True))

    assert fetch.calls.count("modules/mcp/index.json") == 2
    assert data == {"v": 2}


# ── TTL 过期重新现拉 ─────────────────────────────────────────────────────────

def test_entry_expires_after_ttl(fetch):
    fetch.files["modules/mcp/index.json"] = {"v": 1}

    asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))
    # 把条目人为拨到 TTL 之外，模拟时间流逝。
    consumer_cache._cache["modules/mcp/index.json"]["fetched_at"] -= (
        consumer_cache._item_ttl_seconds() + 1
    )
    data, _ = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))

    assert fetch.calls.count("modules/mcp/index.json") == 2


# ── 404 缓存 ─────────────────────────────────────────────────────────────────

def test_404_is_cached_as_none(fetch):
    async def _first():
        try:
            await consumer_cache.get_raw("modules/mcp/items/nope.json")
            return "no-raise"
        except GitClientError:
            return "raised"

    assert asyncio.run(_first()) == "raised"
    # 第二次读：命中缓存的 None，不再打网络。
    data, stale = asyncio.run(consumer_cache.get_raw("modules/mcp/items/nope.json"))
    assert data is None
    assert stale is False
    assert fetch.calls.count("modules/mcp/items/nope.json") == 1


# ── 写失效 ───────────────────────────────────────────────────────────────────

def test_invalidate_scopes_to_module(fetch):
    fetch.files["modules/mcp/index.json"] = {"m": "mcp"}
    fetch.files["modules/mcp/items/a.json"] = {"id": "a"}
    fetch.files["modules/skills/index.json"] = {"m": "skills"}
    fetch.files["marketplace.json"] = {"schema": "x"}
    for path in list(fetch.files):
        asyncio.run(consumer_cache.get_raw(path))

    consumer_cache.invalidate("mcp")

    assert "modules/mcp/index.json" not in consumer_cache._cache
    assert "modules/mcp/items/a.json" not in consumer_cache._cache
    assert "modules/skills/index.json" in consumer_cache._cache
    assert "marketplace.json" in consumer_cache._cache


def test_invalidate_all_clears_everything(fetch):
    fetch.files["modules/mcp/index.json"] = {}
    fetch.files["marketplace.json"] = {}
    for path in list(fetch.files):
        asyncio.run(consumer_cache.get_raw(path))

    consumer_cache.invalidate()

    assert consumer_cache._cache == {}


def test_invalidate_marker_only_touches_marker(fetch):
    fetch.files["marketplace.json"] = {"schema": "x"}
    fetch.files["modules/mcp/index.json"] = {}
    for path in list(fetch.files):
        asyncio.run(consumer_cache.get_raw(path))

    consumer_cache.invalidate_marker()

    assert "marketplace.json" not in consumer_cache._cache
    assert "modules/mcp/index.json" in consumer_cache._cache


# ── 失败回旧快照 ─────────────────────────────────────────────────────────────

def test_failure_serves_last_snapshot_as_stale(fetch):
    fetch.files["modules/mcp/index.json"] = {"v": 1}
    asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))

    # 过期后远端 502：应回旧快照并标 stale，而不是抛掉已有数据。
    fetch.broken.add("modules/mcp/index.json")
    consumer_cache._cache["modules/mcp/index.json"]["fetched_at"] -= (
        consumer_cache._item_ttl_seconds() + 1
    )
    data, stale = asyncio.run(consumer_cache.get_raw("modules/mcp/index.json"))

    assert data == {"v": 1}
    assert stale is True


def test_failure_without_snapshot_raises(fetch):
    fetch.broken.add("modules/mcp/index.json")

    async def _run():
        try:
            await consumer_cache.get_raw("modules/mcp/index.json")
            return "no-raise"
        except GitClientError:
            return "raised"

    assert asyncio.run(_run()) == "raised"


# ── refresh_all：模块索引 + marker 预热，逐文件容错 ──────────────────────────

def test_refresh_all_warms_indexes_and_marker(fetch, monkeypatch):
    settings = _settings(modules=("mcp", "skills"))
    consumer = MarketplaceConsumerSettings(
        repo_url=settings.repo_url, github_owner="o", github_repo="r",
        github_branch="main", modules=("mcp", "skills"), index_name="index.json",
        platform="github",
    )
    monkeypatch.setattr(consumer_cache.mp_config, "settings", settings)
    monkeypatch.setattr(consumer_cache.mp_config, "consumer_settings", consumer)

    fetch.files["modules/mcp/index.json"] = {"items": []}
    fetch.files["modules/skills/index.json"] = {"items": []}
    # 发行模块即使没配进 modules 白名单也要刷（_valid_module 同口径）。
    fetch.files["modules/node-versions/index.json"] = {"items": []}
    fetch.files["modules/mobile-versions/index.json"] = {"items": []}
    fetch.files["marketplace.json"] = {"schema": "ai-lubricant.market.v1"}
    fetch.broken.add("modules/skills/index.json")  # 单文件失败不拖垮整轮

    result = asyncio.run(consumer_cache.refresh_all())

    assert result["ok"] is True
    # 5 个路径（mcp / skills / node-versions / mobile-versions / marker），skills 失败。
    assert result["refreshed"] == 4 and result["failed"] == 1
    assert "modules/mcp/index.json" in consumer_cache._cache
    assert "modules/mobile-versions/index.json" in consumer_cache._cache
    assert "marketplace.json" in consumer_cache._cache
