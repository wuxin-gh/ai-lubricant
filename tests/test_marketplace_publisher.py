"""render.py（仓库发布文件纯渲染）与 publisher（outbox worker）的行为回归。

render 是纯函数直接测；publisher 用假 store + 假 GitHub client 驱动
``_publish_module_locked``，验证「渲染永远读 store 当前状态」的核心语义。
"""
from __future__ import annotations

import asyncio
import sys

import pytest

sys.path.insert(0, "server")

from monkeycode_compat.marketplace import publisher  # noqa: E402
from monkeycode_compat.marketplace.render import (  # noqa: E402
    render_index,
    render_marker,
    render_mobile_release,
    render_node_release,
)
from monkeycode_compat.marketplace.validator import index_summary  # noqa: E402


# ── render_index / render_marker ─────────────────────────────────────────────


def _mcp(id="a.b", display="A") -> dict:
    return {
        "schema": "ai-lubricant.mcp/v1", "id": id, "name": id,
        "display_name": display, "summary": "s", "version": "1.0.0", "kind": "mcp",
        "resource": {"type": "remote_mcp", "url": "https://mcp.example.com/sse", "transport": "sse"},
    }


def test_render_index_sorts_and_stamps_schema():
    summaries = [index_summary("mcp", _mcp("z.z", "Z")), index_summary("mcp", _mcp("a.a", "A"))]
    index = render_index("mcp", summaries)
    assert index["schema"] == "ai-lubricant.market.index.v1"
    assert index["module"] == "mcp"
    assert [it["id"] for it in index["items"]] == ["a.a", "z.z"]
    assert index["updated_at"]


def test_render_marker_dedupes_and_sorts_modules():
    marker = render_marker(["channels", "mcp", "channels", "", "plugins"])
    assert marker["schema"] == "ai-lubricant.market.v1"
    assert marker["modules"] == ["channels", "mcp", "plugins"]


# ── render_node_release / render_mobile_release ──────────────────────────────


def _node_asset(filename="node-execution-linux-amd64", **over) -> dict:
    base = {
        "filename": filename, "component": "node", "role": "execution",
        "platform": "linux", "arch": "amd64", "format": "executable",
        "repo_path": f"node-releases/v/files/{filename}",
        "digest": "sha256:" + "a" * 64, "size_bytes": 1,
    }
    base.update(over)
    return base


def test_render_node_release_excludes_draft_and_test():
    manifests = [
        {"status": "published", "test_version": False, "version": "1.0.0",
         "version_notes": "n", "release_tag": "t",
         "assets": [_node_asset()]},
        {"status": "draft", "test_version": False, "version": "2.0.0",
         "version_notes": "d", "release_tag": "t2",
         "assets": [_node_asset(filename="node-management-linux-amd64", role="management")]},
        {"status": "published", "test_version": True, "version": "3.0.0",
         "version_notes": "t", "release_tag": "t3",
         "assets": [_node_asset(filename="node-execution-linux-arm64", arch="arm64")]},
    ]
    payload, errors = render_node_release(manifests)
    assert errors == []
    assert payload["version"] == "1.0.0"
    roles = {(a["role"], a["arch"]) for a in payload["assets"]}
    assert ("execution", "amd64") in roles
    assert ("management", "amd64") not in roles
    assert ("execution", "arm64") not in roles


def test_render_node_release_empty_when_no_published():
    payload, errors = render_node_release([])
    assert errors == []
    assert payload["assets"] == []
    assert payload["version"] == ""


def test_render_mobile_release_picks_latest_published_and_ios_block():
    manifests = [
        {"status": "published", "test_version": False, "version": "260601",
         "version_notes": "old", "release_tag": "m1", "ios_store_url": "https://apps.apple.com/x",
         "assets": [{"filename": "ai-lubricant-260601-android.apk", "platform": "android",
                     "format": "apk", "repo_path": "mobile-releases/260601/files/a.apk",
                     "digest": "sha256:" + "a" * 64, "size_bytes": 1}]},
        {"status": "published", "test_version": False, "version": "260608",
         "version_notes": "new", "release_tag": "m2", "ios_store_url": "https://apps.apple.com/y",
         "assets": [{"filename": "ai-lubricant-260608-android.apk", "platform": "android",
                     "format": "apk", "repo_path": "mobile-releases/260608/files/a.apk",
                     "digest": "sha256:" + "b" * 64, "size_bytes": 2}]},
    ]
    payload, errors = render_mobile_release(manifests)
    assert errors == []
    assert payload["version"] == "260608"
    assert payload["assets"][0]["version"] == "260608"
    assert payload["ios"] == {"version": "260608", "store_url": "https://apps.apple.com/y"}


# ── publisher._publish_module_locked ────────────────────────────────────────


class _FakeGitClient:
    def __init__(self):
        self.json_files: dict[str, dict] = {}
        self.blobs: dict[str, bytes] = {}
        self.deletes: list[str] = []

    async def read_json_or_none(self, path, use_cache: bool = True):
        if path in self.json_files:
            return (self.json_files[path], "sha")
        return None

    async def write_json(self, path, payload, message):
        self.json_files[path] = payload

    async def write_bytes(self, path, data, message):
        self.blobs[path] = data

    async def delete_file(self, path, message):
        existed = path in self.json_files or path in self.blobs
        self.deletes.append(path)
        self.json_files.pop(path, None)
        self.blobs.pop(path, None)
        return existed


class _FakeStore:
    """最小 store 面：publisher 渲染所需的三类查询 + 可变当前状态。"""

    def __init__(self, items: dict[tuple[str, str], dict]):
        # (module, item_id) -> {"manifest":..., "summary":..., "status":...}
        self.items = items

    async def list_item_ids(self, module):
        return {mid for (m, mid) in self.items if m == module}

    async def get_item(self, module, item_id):
        return self.items.get((module, item_id))

    async def list_summaries(self, module):
        return [dict(v["summary"]) for (m, _), v in self.items.items() if m == module]

    async def list_manifests(self, module, *, include_hidden=True):
        return [v["manifest"] for (m, _), v in sorted(self.items.items())
                if m == module and (include_hidden or v["status"] == "published")]

    async def populated_modules(self):
        return sorted({m for (m, _) in self.items})


@pytest.fixture()
def invalidate_spy(monkeypatch):
    calls: list[str] = []

    class _CC:
        @staticmethod
        def invalidate(module=None):
            calls.append(f"mod:{module}")

        @staticmethod
        def invalidate_marker():
            calls.append("marker")

    import monkeycode_compat.marketplace.consumer_cache as cc

    monkeypatch.setattr(cc, "invalidate", _CC.invalidate)
    monkeypatch.setattr(cc, "invalidate_marker", _CC.invalidate_marker)
    return calls


def _channel_item(item_id="local.a", display="A", status="published") -> dict:
    manifest = {
        "schema": "ai-lubricant.channel-template/v1", "id": item_id,
        "name": item_id, "display_name": display, "version": "1.0.0",
        "summary": "s", "kind": "channel_template", "status": status,
        "resource": {
            "type": "channel_template",
            "channel": {"name": display, "base_url": "https://api.example.com/v1",
                        "chat_protocols": [{"enabled": True, "protocol": "openai", "path": "/v1/chat/completions"}]},
            "freeze_policy": {"enabled": True, "rules": []},
        },
    }
    summary = index_summary("channels", manifest)
    return {"manifest": manifest, "summary": summary, "status": status}


def test_publish_module_writes_item_index_and_marker(invalidate_spy, monkeypatch):
    store = _FakeStore({("channels", "local.a"): _channel_item()})
    client = _FakeGitClient()
    monkeypatch.setattr(publisher, "_client", lambda: client)

    job = {"id": 1, "module": "channels", "item_id": "local.a", "action": "upsert", "payload": {}}
    asyncio.run(publisher._publish_module_locked(store, client, "channels", [job]))

    item_path = "modules/channels/items/local.a.json"
    assert item_path in client.json_files
    assert client.json_files[item_path]["display_name"] == "A"
    assert "modules/channels/index.json" in client.json_files
    assert client.json_files["modules/channels/index.json"]["items"][0]["id"] == "local.a"
    # 集合从空集 → {channels}：marker 要写。
    assert client.json_files["marketplace.json"]["modules"] == ["channels"]
    assert "mod:channels" in invalidate_spy and "marker" in invalidate_spy


def test_publish_module_skips_marker_when_unchanged(invalidate_spy, monkeypatch):
    store = _FakeStore({("channels", "local.a"): _channel_item()})
    client = _FakeGitClient()
    client.json_files["marketplace.json"] = {"schema": "ai-lubricant.market.v1",
                                             "modules": ["channels"], "updated_at": "x"}
    monkeypatch.setattr(publisher, "_client", lambda: client)

    job = {"id": 1, "module": "channels", "item_id": "local.a", "action": "upsert", "payload": {}}
    asyncio.run(publisher._publish_module_locked(store, client, "channels", [job]))

    # marker 已含同一集合：不得重写（updated_at 保持原值）。
    assert client.json_files["marketplace.json"]["updated_at"] == "x"


def test_publish_module_delete_then_reupsert_renders_current_state(invalidate_spy, monkeypatch):
    """晚到的 delete job 渲染的是 store 当前状态：行在就写新文件，不复活旧数据。"""
    store = _FakeStore({("channels", "local.a"): _channel_item(display="New")})
    client = _FakeGitClient()
    monkeypatch.setattr(publisher, "_client", lambda: client)

    delete_job = {"id": 1, "module": "channels", "item_id": "local.a", "action": "delete",
                  "payload": {"manifest": _channel_item(display="Old")["manifest"]}}
    asyncio.run(publisher._publish_module_locked(store, client, "channels", [delete_job]))

    item_path = "modules/channels/items/local.a.json"
    # 行在（re-upsert 过）→ delete 渲染成写新内容，不删文件。
    assert client.json_files[item_path]["display_name"] == "New"
    assert item_path not in client.deletes


def test_publish_module_refresh_only_renders_index(invalidate_spy, monkeypatch):
    store = _FakeStore({("channels", "local.a"): _channel_item()})
    client = _FakeGitClient()
    monkeypatch.setattr(publisher, "_client", lambda: client)

    refresh_job = {"id": 1, "module": "channels", "item_id": "*", "action": "refresh", "payload": {}}
    asyncio.run(publisher._publish_module_locked(store, client, "channels", [refresh_job]))

    assert "modules/channels/index.json" in client.json_files
    assert "modules/channels/items/local.a.json" not in client.json_files, "refresh 不得写条目文件"
    assert not client.deletes


def test_publish_module_node_versions_renders_version_json(invalidate_spy, monkeypatch):
    manifest = {
        "schema": "ai-lubricant.node-version/v1", "id": "node-suite-1.0.0",
        "kind": "node_program_version", "name": "node-suite", "display_name": "n",
        "version": "1.0.0", "version_notes": "n", "status": "published", "test_version": False,
        "assets": [_node_asset()], "release_tag": "node-suite-1.0.0",
        "category": "node-suite", "tags": [],
    }
    store = _FakeStore({("node-versions", "node-suite-1.0.0"): {
        "manifest": manifest, "summary": index_summary("node-versions", manifest), "status": "published"}})
    client = _FakeGitClient()
    monkeypatch.setattr(publisher, "_client", lambda: client)

    job = {"id": 1, "module": "node-versions", "item_id": "node-suite-1.0.0", "action": "upsert", "payload": {}}
    asyncio.run(publisher._publish_module_locked(store, client, "node-versions", [job]))

    version_json = client.json_files["node-releases/version.json"]
    assert version_json["version"] == "1.0.0"
    assert all("repo_path" in a for a in version_json["assets"])
