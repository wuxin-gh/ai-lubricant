"""render.py（仓库发布文件纯渲染）与 publisher（outbox worker）的行为回归。

render 是纯函数直接测；publisher 用假 store + 假 GitHub client 驱动
``_publish_module_locked``，验证「渲染永远读 store 当前状态」的核心语义。
"""
from __future__ import annotations

import asyncio
import sys

import pytest

sys.path.insert(0, "server")

from user_platform.marketplace import publisher  # noqa: E402
from user_platform.marketplace.render import (  # noqa: E402
    render_index,
    render_marker,
    render_mobile_release,
    render_node_release,
)
from user_platform.marketplace.validator import index_summary  # noqa: E402


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


def _node_manifest(version, *, status="published", test=False, role="execution",
                   filename=None, arch="amd64") -> dict:
    fn = filename or f"node-execution-linux-{arch}"
    return {
        "schema": "ai-lubricant.node-version/v1", "id": f"node-suite-{version}",
        "kind": "node_program_version", "name": "node-suite", "display_name": "n",
        "version": version, "version_notes": "n", "status": status, "test_version": test,
        "assets": [_node_asset(filename=fn, role=role, arch=arch)],
        "release_tag": f"node-suite-{version}", "category": "node-suite", "tags": [],
    }


def _node_row(manifest: dict) -> dict:
    return {"manifest": manifest, "summary": index_summary("node-versions", manifest),
            "status": manifest["status"]}


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

    import user_platform.marketplace.consumer_cache as cc

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


def test_render_node_release_includes_test_only_when_include_test():
    """仓库那份（include_test=False）排除测试版；writable 本地快照那份纳入。"""
    manifests = [
        {"status": "published", "test_version": False, "version": "1.0.0",
         "version_notes": "n", "release_tag": "t",
         "assets": [_node_asset()]},
        {"status": "published", "test_version": True, "version": "3.0.0",
         "version_notes": "t", "release_tag": "t3",
         "assets": [_node_asset(filename="node-execution-linux-arm64", arch="arm64")]},
    ]
    payload, errors = render_node_release(manifests)
    assert errors == []
    assert payload["version"] == "1.0.0"
    assert all(a["arch"] != "arm64" for a in payload["assets"])

    payload_t, errors_t = render_node_release(manifests, include_test=True)
    assert errors_t == []
    assert payload_t["version"] == "3.0.0"
    assert "arm64" in {a["arch"] for a in payload_t["assets"]}


def test_publish_node_versions_skips_mirror_for_draft_and_test(invalidate_spy, monkeypatch):
    """草稿/测试版不进公开仓库：item 文件、per-version manifest、索引都不含；
    只有「已发布且非测试版」才镜像。只读部署读仓库 raw 因此感知不到测试版。"""
    pub = _node_manifest("1.0.0")
    draft = _node_manifest("2.0.0", status="draft")
    test = _node_manifest("3.0.0", test=True, arch="arm64")
    store = _FakeStore({
        ("node-versions", "node-suite-1.0.0"): _node_row(pub),
        ("node-versions", "node-suite-2.0.0"): _node_row(draft),
        ("node-versions", "node-suite-3.0.0"): _node_row(test),
    })
    client = _FakeGitClient()
    monkeypatch.setattr(publisher, "_client", lambda: client)
    jobs = [
        {"id": 1, "module": "node-versions", "item_id": "node-suite-1.0.0", "action": "upsert", "payload": {}},
        {"id": 2, "module": "node-versions", "item_id": "node-suite-2.0.0", "action": "upsert", "payload": {}},
        {"id": 3, "module": "node-versions", "item_id": "node-suite-3.0.0", "action": "upsert", "payload": {}},
    ]
    asyncio.run(publisher._publish_module_locked(store, client, "node-versions", jobs))

    # 已发布：item 文件 + per-version manifest 都写。
    assert "modules/node-versions/items/node-suite-1.0.0.json" in client.json_files
    assert "node-releases/1.0.0/manifest.json" in client.json_files
    # 草稿/测试：不镜像 item 文件，不写 per-version manifest。
    assert "modules/node-versions/items/node-suite-2.0.0.json" not in client.json_files
    assert "modules/node-versions/items/node-suite-3.0.0.json" not in client.json_files
    assert "node-releases/2.0.0/manifest.json" not in client.json_files
    assert "node-releases/3.0.0/manifest.json" not in client.json_files
    # 仓库索引只含已发布（草稿/测试被过滤）。
    index = client.json_files["modules/node-versions/index.json"]
    assert [it["id"] for it in index["items"]] == ["node-suite-1.0.0"]
    # 仓库 version.json 只含已发布版本。
    assert client.json_files["node-releases/version.json"]["version"] == "1.0.0"


def test_publish_node_versions_local_apply_includes_test_when_writable(invalidate_spy, monkeypatch):
    """writable 部署：publisher 给本进程快照 apply 的那份把测试版当正常版纳入
    （include_test=True），而写仓库的那份仍 exclude test。"""
    import user_platform.marketplace.config as config
    import node_release_catalog as nrc

    monkeypatch.setattr(config.MarketplaceSettings, "writable", property(lambda self: True))
    pub = _node_manifest("1.0.0")
    test = _node_manifest("3.0.0", test=True, arch="arm64")
    store = _FakeStore({
        ("node-versions", "node-suite-1.0.0"): _node_row(pub),
        ("node-versions", "node-suite-3.0.0"): _node_row(test),
    })
    client = _FakeGitClient()
    monkeypatch.setattr(publisher, "_client", lambda: client)
    applied: list[dict] = []

    async def _capture(release, **kwargs):
        applied.append(release)

    monkeypatch.setattr(nrc, "apply_release", _capture)
    job = {"id": 1, "module": "node-versions", "item_id": "node-suite-3.0.0", "action": "upsert", "payload": {}}
    asyncio.run(publisher._publish_module_locked(store, client, "node-versions", [job]))

    # 仓库那份 exclude test：version=1.0.0。
    assert client.json_files["node-releases/version.json"]["version"] == "1.0.0"
    # 本地 apply 那份 include test：3.0.0（测试版）当正常版纳入，version=3.0.0。
    assert applied and applied[0]["version"] == "3.0.0"


# ── 榜单推送（module=leaderboard 走 outbox，worker 不碰 GitHub） ───────────────


def _install_leaderboard(monkeypatch, results: dict[int, dict]):
    """publish_items 桩：item_id -> {'bucket': published|skipped|failed}。"""
    import marketplace_leaderboard_store as lb
    import user_platform.marketplace.source_config as sc

    calls: list[tuple] = []

    async def fake_publish_items(item_ids, *, operator, require_verified=False):
        calls.append((tuple(item_ids), operator, require_verified))
        out: dict[str, list] = {"published": [], "skipped": [], "failed": []}
        for i in item_ids:
            r = results.get(i)
            if r is None:
                continue
            out[r["bucket"]].append(r.get("payload", i))
        return out

    async def fake_source():
        return {"leaderboard_require_verified": True}

    monkeypatch.setattr(lb, "publish_items", fake_publish_items)
    monkeypatch.setattr(sc, "get_source_config_async", fake_source)
    return calls


def test_publish_leaderboard_splits_done_and_failed(monkeypatch):
    """逐 job 收口：published/skipped 归 done（幂等），failed 带 job_id 与首个原因；
    require_verified 从榜单 source 配置读取并透传。"""
    results = {
        1: {"bucket": "published"},
        2: {"bucket": "skipped"},
        3: {"bucket": "failed", "payload": {"id": 3, "error": "上游未给出安装形态"}},
    }
    calls = _install_leaderboard(monkeypatch, results)
    jobs = [
        {"id": 11, "item_id": "1", "payload": {"operator": "admin"}},
        {"id": 12, "item_id": "2", "payload": {}},
        {"id": 13, "item_id": "3", "payload": {}},
    ]
    status, error, done, failed = asyncio.run(publisher.publish_leaderboard(jobs))
    assert status == "ok"
    assert sorted(done) == [11, 12]
    assert failed == [13]
    assert "上游未给出安装形态" in error
    # 有 failed 也有 done：整体不算 failed（单条失败不影响其余条目），下轮只重试 failed。
    assert calls and calls[0][2] is True  # require_verified 透传


def test_publish_leaderboard_all_failed(monkeypatch):
    results = {5: {"bucket": "failed", "payload": {"id": 5, "error": "门禁不过"}}}
    _install_leaderboard(monkeypatch, results)
    status, error, done, failed = asyncio.run(
        publisher.publish_leaderboard([{"id": 15, "item_id": "5", "payload": {}}])
    )
    assert status == "failed"
    assert done == []
    assert failed == [15]
    assert "门禁不过" in error


def test_publish_once_routes_leaderboard_module(monkeypatch):
    """module=leaderboard 的 job 走 publish_leaderboard 分支，不进 GitHub 渲染路径。"""
    import marketplace_store as ms

    jobs = [{"id": 11, "module": "leaderboard", "item_id": "1", "payload": {}}]
    done_ids: list[int] = []
    failed_calls: list[tuple] = []

    async def fake_claim(limit):
        return jobs

    async def fake_mark_done(ids):
        done_ids.extend(ids)

    async def fake_mark_failed(ids, error):
        failed_calls.append((ids, error))

    async def fake_publish_leaderboard(module_jobs):
        return ("ok", "", [j["id"] for j in module_jobs], [])

    monkeypatch.setattr(ms, "claim_pending", fake_claim)
    monkeypatch.setattr(ms, "mark_done", fake_mark_done)
    monkeypatch.setattr(ms, "mark_failed", fake_mark_failed)
    monkeypatch.setattr(publisher, "publish_leaderboard", fake_publish_leaderboard)

    summary = asyncio.run(publisher.publish_once())
    assert done_ids == [11]
    assert failed_calls == []
    assert summary["modules"]["leaderboard"]["status"] == "ok"
