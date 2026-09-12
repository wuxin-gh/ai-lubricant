"""节点公网 IP 探测备份（``node_public_ip`` key）：形状归一 + 默认回落 + 部分更新。

锁住的边界：
- DB 无 key → 内置默认列表（全新部署也有一份可用备份）；
- key 存在但列表为空 → 保留空（管理员显式清空，不被默认值覆盖）；
- URL 列表归一：strip、丢空、保序去重；
- update 只动 patch 里出现的地址族，另一族保持原值；
- public_view 只回备份列表 + 默认列表，无内部字段。
"""
from __future__ import annotations

import asyncio
import sys
import types

import pytest

from user_platform.marketplace.node_ip_config import (
    DEFAULT_IPV4_URLS,
    DEFAULT_IPV6_URLS,
    _normalize,
    public_view,
    update_node_ip_config,
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


# ── _normalize ────────────────────────────────────────────────────────────────

def test_normalize_none_falls_back_to_defaults():
    cfg = _normalize(None)
    assert cfg == {"ipv4_urls": list(DEFAULT_IPV4_URLS), "ipv6_urls": list(DEFAULT_IPV6_URLS)}
    # 默认列表就是产品要求的四条。
    assert DEFAULT_IPV4_URLS == ["https://ip4.me/", "http://httpbin.org/ip"]
    assert DEFAULT_IPV6_URLS == ["https://ip6only.me/", "http://v6.ipv6-test.com/api/myip.php"]


def test_normalize_empty_dict_keeps_empty_not_defaults():
    """key 存在但没填列表 = 管理员显式清空，保留空（不回退默认）。"""
    cfg = _normalize({})
    assert cfg == {"ipv4_urls": [], "ipv6_urls": []}


def test_normalize_strips_dedups_drops_empty():
    cfg = _normalize({
        "ipv4_urls": ["  https://a.com/ip ", "", "https://a.com/ip", "https://b.com/ip"],
        "ipv6_urls": ["https://c.com/ip"],
    })
    # strip + 丢空 + 保序去重。
    assert cfg["ipv4_urls"] == ["https://a.com/ip", "https://b.com/ip"]
    assert cfg["ipv6_urls"] == ["https://c.com/ip"]


def test_normalize_malformed_shapes_fall_back_to_empty():
    cfg = _normalize({"ipv4_urls": "not-a-list", "ipv6_urls": 42})
    assert cfg == {"ipv4_urls": [], "ipv6_urls": []}


# ── public_view ───────────────────────────────────────────────────────────────

def test_public_view_shape_only_backup_and_defaults():
    view = public_view({"ipv4_urls": ["https://a.com"], "ipv6_urls": [], "secret": "x"})
    assert set(view.keys()) == {"ipv4_urls", "ipv6_urls", "default_ipv4_urls", "default_ipv6_urls"}
    assert view["ipv4_urls"] == ["https://a.com"]
    assert view["ipv6_urls"] == []
    assert view["default_ipv4_urls"] == list(DEFAULT_IPV4_URLS)
    assert view["default_ipv6_urls"] == list(DEFAULT_IPV6_URLS)
    assert "secret" not in view


# ── update_node_ip_config：部分更新 + 未传族不动 ──────────────────────────────

def test_update_replaces_ipv4_keeps_ipv6(fake_config):
    fake_config._blob = {"node_public_ip": {
        "ipv4_urls": ["https://old.com/ip"],
        "ipv6_urls": ["https://v6.old.com/ip"],
    }}

    merged = asyncio.run(update_node_ip_config({
        "ipv4_urls": ["https://new.com/ip"],
    }))

    assert merged["ipv4_urls"] == ["https://new.com/ip"]
    # ipv6 未传 → 保持原值。
    assert merged["ipv6_urls"] == ["https://v6.old.com/ip"]
    # 落库 blob 里 ipv6 未被改动。
    written = fake_config.writes[-1]["node_public_ip"]
    assert written["ipv4_urls"] == ["https://new.com/ip"]
    assert written["ipv6_urls"] == ["https://v6.old.com/ip"]


def test_update_both_families_replace_full(fake_config):
    fake_config._blob = {"node_public_ip": {
        "ipv4_urls": ["https://old.com/ip"],
        "ipv6_urls": ["https://v6.old.com/ip"],
    }}

    merged = asyncio.run(update_node_ip_config({
        "ipv4_urls": ["https://a.com/ip", "https://a.com/ip"],
        "ipv6_urls": [],
    }))

    # 全量替换 + 去重；ipv6 显式清空。
    assert merged["ipv4_urls"] == ["https://a.com/ip"]
    assert merged["ipv6_urls"] == []


def test_update_on_missing_key_merges_with_defaults(fake_config):
    """blob 无 key：current 回落默认，故只传 ipv4 时 ipv6 取默认（备份总有 sensible 兜底）。"""
    fake_config._blob = {}

    merged = asyncio.run(update_node_ip_config({"ipv4_urls": ["https://custom.com/ip"]}))

    assert merged["ipv4_urls"] == ["https://custom.com/ip"]
    assert merged["ipv6_urls"] == list(DEFAULT_IPV6_URLS)
