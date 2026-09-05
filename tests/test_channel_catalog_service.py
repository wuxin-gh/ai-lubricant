from __future__ import annotations

import asyncio
from types import SimpleNamespace

from monkeycode_compat.marketplace import channel_catalog


def test_catalog_always_merges_custom_system_and_remote(monkeypatch):
    channel_catalog.configure_builtin_loader(lambda: [{
        "id": "demo", "name": "Demo", "description": "D", "tags": [],
        "builtin_type": "demo", "preset": {"remark": "old", "billing_mode": "token"},
    }])
    monkeypatch.setattr(channel_catalog, "_snapshot", {
        "remote_items": [{
            "id": "demo-channel", "name": "Demo 渠道", "description": "new", "tags": ["推荐"],
            "builtin_type": "demo", "preset": {"remark": "new"},
        }, {
            "id": "vendor", "name": "Vendor", "description": "V", "tags": [],
            "builtin_type": "", "preset": {"billing_mode": "request"},
        }],
        "updated_at": "now", "stale": False,
    })

    async def headers():
        return []

    monkeypatch.setattr(channel_catalog.PostgresClient, "get_header_templates", headers)
    result = asyncio.run(channel_catalog.get_catalog())
    by_id = {item["id"]: item for item in result["items"]}

    assert set(by_id) == {"custom", "code", "demo", "vendor"}
    assert by_id["demo"]["name"] == "Demo 渠道"
    assert by_id["demo"]["builtin_type"] == "demo"
    assert by_id["demo"]["preset"]["billing_mode"] == "token"
    assert by_id["demo"]["preset"]["remark"] == "new"


def test_catalog_resolves_header_name_to_local_id(monkeypatch):
    channel_catalog.configure_builtin_loader(lambda: [])
    monkeypatch.setattr(channel_catalog, "_snapshot", {
        "remote_items": [{
            "id": "vendor", "name": "Vendor", "description": "", "tags": [], "builtin_type": "",
            "preset": {"chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions", "header_template_name": "Claude"}]},
        }],
        "updated_at": "now", "stale": False,
    })

    async def headers():
        return [{"id": "header-1", "name": "Claude", "headers": {}}]

    monkeypatch.setattr(channel_catalog.PostgresClient, "get_header_templates", headers)
    result = asyncio.run(channel_catalog.get_catalog())
    vendor = next(item for item in result["items"] if item["id"] == "vendor")
    assert vendor["preset"]["chat_protocols"][0]["header_template"] == "header-1"
    assert "header_template_name" not in vendor["preset"]["chat_protocols"][0]


def test_failed_refresh_keeps_last_good_snapshot(monkeypatch):
    original = {"remote_items": [{"id": "old", "name": "Old", "description": "", "tags": [], "builtin_type": "", "preset": {}}], "updated_at": "old", "stale": False}
    monkeypatch.setattr(channel_catalog, "_snapshot", original.copy())
    monkeypatch.setattr(channel_catalog.mp_config, "settings", SimpleNamespace(
        enabled=True, modules=("channels",), github_owner="owner", github_repo="repo",
        github_branch="main", index_name="index.json",
    ))

    async def headers():
        return []

    monkeypatch.setattr(channel_catalog.PostgresClient, "get_header_templates", headers)
    channel_catalog.configure_builtin_loader(lambda: [])

    class BrokenSession:
        async def __aenter__(self):
            raise RuntimeError("offline")
        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(channel_catalog.aiohttp, "ClientSession", lambda **_kwargs: BrokenSession())
    result = asyncio.run(channel_catalog.refresh(publish=False))

    assert result["ok"] is False
    assert any(item["id"] == "old" for item in result["items"])


def _authoritative_channel_manifest(item_id: str = "local.new", summary: str = "") -> dict:
    return {
        "schema": "ai-lubricant.channel-template/v1",
        "id": item_id,
        "name": item_id.removeprefix("local."),
        "display_name": "New Channel",
        "version": "1.0.0",
        "summary": summary,
        "kind": "channel_template",
        "resource": {
            "type": "channel_template",
            "channel": {
                "name": "New Channel",
                "base_url": "https://api.example.com/v1",
                "billing_mode": "token",
                "chat_protocols": [{"enabled": True, "protocol": "openai", "path": "/v1/chat/completions"}],
            },
            "freeze_policy": {"enabled": True, "rules": []},
        },
    }


def test_apply_authoritative_manifests_merges_without_raw_fetch(monkeypatch):
    channel_catalog.configure_builtin_loader(lambda: [])
    monkeypatch.setattr(channel_catalog, "_snapshot", {
        "remote_items": [
            {"id": "local.old", "name": "Old", "description": "old", "tags": [], "builtin_type": "", "preset": {}},
            {"id": "local.new", "name": "Old New", "description": "old new", "tags": [], "builtin_type": "", "preset": {}},
        ],
        "updated_at": "old",
        "stale": True,
    })

    persisted = []
    published = []

    async def set_config(_cls, key, value):
        persisted.append((key, value))

    async def headers():
        return []

    async def publish(event, scope):
        published.append((event, scope))

    monkeypatch.setattr(channel_catalog.PostgresClient, "set_config", classmethod(set_config))
    monkeypatch.setattr(channel_catalog.PostgresClient, "get_header_templates", headers)
    monkeypatch.setattr(channel_catalog, "_fetch_json", lambda *_args: (_ for _ in ()).throw(AssertionError("raw fetch is not allowed")))

    import runtime_sync
    monkeypatch.setattr(runtime_sync, "publish", publish)
    result = asyncio.run(channel_catalog.apply_authoritative_manifests([
        _authoritative_channel_manifest("local.new", "fresh"),
    ]))

    assert result["ok"] is True
    assert result["stale"] is False
    by_id = {item["id"]: item for item in result["items"] if item["id"].startswith("local.")}
    assert by_id["local.old"]["name"] == "Old"
    assert by_id["local.new"]["description"] == "fresh"
    assert persisted[0][0] == channel_catalog.SNAPSHOT_KEY
    assert persisted[0][1]["stale"] is False
    assert published == [(runtime_sync.EVENT_CHANNEL_CATALOG, "__all__")]


def test_apply_authoritative_manifests_rejects_invalid_without_mutation(monkeypatch):
    original = {
        "remote_items": [{"id": "local.old", "name": "Old", "description": "", "tags": [], "builtin_type": "", "preset": {}}],
        "updated_at": "old",
        "stale": False,
    }
    monkeypatch.setattr(channel_catalog, "_snapshot", original.copy())
    called = []

    async def set_config(_cls, *_args):
        called.append(True)

    monkeypatch.setattr(channel_catalog.PostgresClient, "set_config", classmethod(set_config))
    invalid = _authoritative_channel_manifest("local.invalid")
    invalid["resource"]["channel"]["base_url"] = ""

    try:
        asyncio.run(channel_catalog.apply_authoritative_manifests([invalid]))
    except ValueError as exc:
        assert "base_url" in str(exc)
    else:
        raise AssertionError("invalid manifest must be rejected")

    assert channel_catalog._snapshot == original
    assert called == []
