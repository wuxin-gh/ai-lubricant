"""社区运营配置（技术交流群 + 社区通知）：独立 ``community`` key 的形状归一与整段替换。

锁住的边界：
- 群类型白名单、data URL 前缀校验、无二维码的群整条丢弃；
- notice 开了但没有内容视为关闭；
- update 只动 patch 里出现的段（groups / notice），其余保持原值；
- public_view 只回 groups + notice，无内部字段。
"""
from __future__ import annotations

import asyncio
import sys
import types

import pytest

from user_platform.marketplace.community_config import (
    _normalize,
    public_view,
    update_community_config,
)

QR = "data:image/webp;base64,AAAA"
IMG = "data:image/png;base64,BBBB"


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

def test_normalize_passes_groups_and_notice():
    cfg = _normalize({
        "groups": [
            {"id": "g1", "type": "wechat", "label": "微信群①", "qr_image": QR},
            {"id": "g2", "type": "bogus", "label": "未知类型", "qr_image": QR},
            {"id": "g3", "type": "feishu", "label": "没二维码", "qr_image": ""},
        ],
        "notice": {
            "enabled": True,
            "entries": [
                {"id": "n1", "kind": "text", "text": "欢迎加入"},
                {"id": "n2", "kind": "image", "image": IMG},
                {"id": "n3", "kind": "image", "image": ""},
            ],
        },
    })
    # g2 类型兜底为 other；g3 没有二维码被整条丢弃。
    assert [g["id"] for g in cfg["groups"]] == ["g1", "g2"]
    assert cfg["groups"][1]["type"] == "other"
    # n3 没有图片被丢弃。
    assert [e["id"] for e in cfg["notice"]["entries"]] == ["n1", "n2"]
    assert cfg["notice"]["enabled"] is True


def test_normalize_notice_enabled_requires_entries():
    cfg = _normalize({"notice": {"enabled": True, "entries": []}})
    assert cfg["notice"]["enabled"] is False
    assert cfg["notice"]["entries"] == []


def test_normalize_malformed_shapes_fall_back_to_empty():
    cfg = _normalize({"groups": "not-a-list", "notice": "not-a-dict"})
    assert cfg == {"groups": [], "notice": {"enabled": False, "entries": []}}


def test_normalize_bad_data_url_prefix_dropped():
    cfg = _normalize({
        "groups": [{"id": "g1", "type": "qq", "qr_image": "data:text/html;base64,XXXX"}],
    })
    assert cfg["groups"] == []


def test_normalize_caps_counts():
    groups = [{"id": f"g{i}", "type": "wechat", "qr_image": QR} for i in range(20)]
    entries = [{"id": f"n{i}", "kind": "text", "text": "x"} for i in range(20)]
    cfg = _normalize({"groups": groups, "notice": {"enabled": True, "entries": entries}})
    assert len(cfg["groups"]) == 12
    assert len(cfg["notice"]["entries"]) == 12


def test_normalize_auto_generates_ids():
    cfg = _normalize({"groups": [{"type": "wechat", "qr_image": QR}]})
    assert cfg["groups"][0]["id"] == "g1"


# ── public_view ───────────────────────────────────────────────────────────────

def test_public_view_only_groups_and_notice():
    view = public_view({"groups": [], "notice": {"enabled": False, "entries": []}, "secret": "x"})
    assert set(view.keys()) == {"groups", "notice"}
    assert "secret" not in view


# ── update_community_config：整段替换 + 未传段不动 ─────────────────────────────

def test_update_replaces_groups_keeps_notice(fake_config):
    fake_config._blob = {"community": {
        "groups": [{"id": "old", "type": "qq", "qr_image": QR}],
        "notice": {"enabled": True, "entries": [{"id": "n1", "kind": "text", "text": "保留"}]},
    }}

    merged = asyncio.run(update_community_config({
        "groups": [{"id": "new", "type": "wechat", "label": "微信群", "qr_image": QR}],
    }))

    assert [g["id"] for g in merged["groups"]] == ["new"]
    assert merged["notice"]["entries"][0]["text"] == "保留"
    # 落库 blob 里 notice 未被改动。
    written = fake_config.writes[-1]["community"]
    assert [g["id"] for g in written["groups"]] == ["new"]
    assert written["notice"]["entries"][0]["text"] == "保留"


def test_update_notice_toggle_via_shorthand(fake_config):
    fake_config._blob = {"community": {
        "groups": [],
        "notice": {"enabled": False, "entries": [{"id": "n1", "kind": "text", "text": "内容"}]},
    }}

    merged = asyncio.run(update_community_config({"notice_enabled": True}))
    assert merged["notice"]["enabled"] is True
    assert merged["notice"]["entries"][0]["text"] == "内容"

    # 没有内容时开关翻不开。
    merged = asyncio.run(update_community_config({
        "notice": {"enabled": True, "entries": []},
    }))
    assert merged["notice"]["enabled"] is False
