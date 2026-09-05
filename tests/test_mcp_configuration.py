"""Unified schema-v2 MCP configuration contracts."""
import asyncio
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from mcp.configuration import (
    ConfigurationError,
    RESOURCE_ADAPTERS,
    ResourceContext,
    configuration_contract,
    validate_resource_value,
)
from mcp_runtime.configuration import build_builtin_snapshot
import mcp_runtime.configuration as runtime_config
from mcp_runtime.builtin_plugins.cdp_bridge_plugin import _driver_config
import db as db_module


def test_builtin_resource_bindings_share_one_registry():
    assert set(RESOURCE_ADAPTERS) == {"shared"}


def test_configuration_contract_rejects_unknown_binding():
    manifest = {
        "schema": 2,
        "name": "bad",
        "config": {"resources": {"x": {
            "binding": "unknown.binding", "capabilities": [], "schema": {"type": "object", "properties": {}}
        }}},
    }
    with pytest.raises(ConfigurationError, match="未注册"):
        configuration_contract(manifest)


def test_resource_validation_required_types_readonly_and_constraints():
    definition = {"schema": {
        "type": "object",
        "properties": {
            "id": {"type": "integer", "readOnly": True},
            "name": {"type": "string"},
            "enabled": {"type": "boolean"},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "email": {"type": "string", "format": "email"},
            "url": {"type": "string", "format": "uri"},
        },
        "required": ["name"],
    }}
    assert validate_resource_value(definition, {"name": "x", "enabled": True})["name"] == "x"
    with pytest.raises(ConfigurationError, match="必填"):
        validate_resource_value(definition, {"enabled": True})
    with pytest.raises(ConfigurationError, match="只读"):
        validate_resource_value(definition, {"id": 1, "name": "x"})
    with pytest.raises(ConfigurationError, match="类型"):
        validate_resource_value(definition, {"name": "x", "enabled": "yes"})
    with pytest.raises(ConfigurationError, match="不能小于"):
        validate_resource_value(definition, {"name": "x", "port": 0})
    with pytest.raises(ConfigurationError, match="不能大于"):
        validate_resource_value(definition, {"name": "x", "port": 65536})
    with pytest.raises(ConfigurationError, match="邮箱"):
        validate_resource_value(definition, {"name": "x", "email": "invalid"})
    with pytest.raises(ConfigurationError, match="HTTP"):
        validate_resource_value(definition, {"name": "x", "url": "file:///tmp/x"})


def test_runtime_snapshot_materializes_shared_resources(monkeypatch):
    """快照 materialize：driver_settings 仍走 cdp_driver singleton（mcp_runtime_configs）；
    CDP clients / 邮箱 upstream_accounts 的实例数据现在源自 builtin_tool_* 新表，经
    builtin_tool_store 的投影函数产出连接层既有形状。
    """
    import builtin_tool_store
    async def service_env(_sid): return {"BASE": "1"}
    async def service_auth(_sid): return {"auth_enabled": True, "allowed_tokens": {"tok"}, "allowed_user_ids": {1}}
    async def configs(_sid, config_type, *, include_secrets=False):
        return []
    async def by_key(_sid, config_type, key, *, include_secrets=False):
        return {"id": 9, "instance_key": key, "external_ws": True} if config_type == "cdp_driver" else None
    # 内置工具实例数据源投影：CDP client 行（每个 active token 一行，client_id=实例 id）、
    # 邮箱账户 + 地址列表。都来自 builtin_tool_* 新表，明文 token 不落库、只搬 token_hash。
    async def cdp_clients():
        return [{"id": "2", "instance_key": "2", "name": "Chrome", "owner_user_id": "u1",
                 "enabled": True, "token_hash": "abc", "token_hint": "cdp...bc"}]
    async def mail_accounts():
        return [{"id": 1, "instance_key": "1", "enabled": True, "username": "a@example.com",
                 "addresses": [{"id": 8, "address": "x@example.com"}]}]
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_service_env", service_env)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_service_auth", service_auth)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "list_runtime_configs", configs)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_runtime_config_by_key", by_key)
    monkeypatch.setattr(builtin_tool_store, "cdp_driver_clients", cdp_clients)
    monkeypatch.setattr(builtin_tool_store, "mail_upstream_accounts", mail_accounts)
    mail = asyncio.run(build_builtin_snapshot({"id": 1, "name": "mail", "env_template": {}}))
    assert mail.resources["upstream_accounts"][0]["addresses"][0]["id"] == 8
    cdp = asyncio.run(build_builtin_snapshot({"id": 2, "name": "cdp-bridge", "env_template": {}}))
    assert cdp.resources["driver_settings"]["external_ws"] is True
    assert cdp.resources["clients"][0]["token_hash"] == "abc"
    # token → client_id 不再预计算：请求时由 driver.authenticate_client 现场 hash 解析。
    assert "_cdp_client_ids_by_token" not in cdp.resources
    # cdp-bridge 的连接鉴权走「每客户端连接 token」，WS 握手硬要求 ctx.auth_enabled=True
    # 才放行——快照必须强制 True，否则扩展在校验 token 前就被 4403 拒（连不上第二成因）。
    assert cdp.auth_enabled is True


def test_cdp_driver_uses_shared_clients_not_service_auth_tokens():
    cfg = _driver_config({"clients": [{"instance_key": "c1", "enabled": True, "token_hash": "abc"}]}, True, {"tok"})
    assert cfg["clients"][0]["token_hash"] == "abc"
    assert set(cfg) == {"clients"}


def test_shared_runtime_table_and_idempotent_legacy_migration_are_declared():
    source = open(db_module.__file__, encoding="utf-8").read()
    for column in ("config_type", "instance_key", "data JSONB", "secret_data JSONB", "token_hash", "token_hint", "revision"):
        assert column in source
    assert "UNIQUE(service_id, config_type, instance_key)" in source
    assert "runtime_configs_envelope_v1" in source
    assert "runtime_configs_envelope_v2" in source
    for legacy in ("mcp_service_env_vars", "mcp_mail_configs", "mcp_mail_addresses"):
        assert f"FROM {legacy}" in source


def test_shared_adapter_uses_allowlisted_config_type(monkeypatch):
    adapter = RESOURCE_ADAPTERS["shared"]
    ctx = ResourceContext(service={"id": 3}, definition={"storage": {"configType": "cdp_driver", "handler": "object"}, "cardinality": "singleton"})
    saved = {}
    async def upsert(service_id, config_type, patch, *, defaults=None, expected_revision=None):
        saved.update(service_id=service_id, config_type=config_type, patch=patch)
        return {"id": 1, "service_id": service_id, "config_type": config_type, "instance_key": "singleton", **patch}
    monkeypatch.setattr("mcp.configuration.mcp_plugin_store.upsert_runtime_singleton", upsert)
    result = asyncio.run(adapter.update(ctx, None, {"external_ws": True}))
    assert saved["config_type"] == "cdp_driver"
    assert result["external_ws"] is True


def test_resync_builtin_service_pushes_snapshot_to_live_plugin(monkeypatch):
    """用户改 CDP/邮箱数据后，resync_builtin_service 应重建快照并原地下发给活着的插件。

    这是修「新建 CDP 客户端后扩展连不上」的核心：只写 DB 不够，必须把新 token_hash
    推进运行中的 driver（clients_by_hash）。这里断言 update_builtin_config 收到最新
    snapshot 的 resources。
    """
    import builtin_tool_store

    async def get_service_by_name(name):
        return {"id": 2, "name": name, "env_template": {}, "enabled": True}
    async def service_env(_sid): return {}
    async def service_auth(_sid): return {"auth_enabled": False, "allowed_tokens": set(), "allowed_user_ids": set()}
    async def configs(_sid, config_type, *, include_secrets=False): return []
    async def by_key(_sid, config_type, key, *, include_secrets=False):
        return {"id": 9, "instance_key": key, "external_ws": True} if config_type == "cdp_driver" else None
    async def cdp_clients():
        return [{"id": "5", "instance_key": "5", "name": "C", "enabled": True, "token_hash": "newhash", "token_hint": "cdp...sh"}]
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_service_by_name", get_service_by_name)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_service_env", service_env)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_service_auth", service_auth)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "list_runtime_configs", configs)
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_runtime_config_by_key", by_key)
    monkeypatch.setattr(builtin_tool_store, "cdp_driver_clients", cdp_clients)

    pushed = {}

    class _FakeRegistry:
        def get(self, name):
            return object()  # 已注册（driver 活着）

        async def update_builtin_config(self, name, **kwargs):
            pushed.update(name=name, **kwargs)
            return object()

    monkeypatch.setattr("mcp_runtime.registry.registry", _FakeRegistry())
    ok = asyncio.run(runtime_config.resync_builtin_service("cdp-bridge"))
    assert ok is True
    assert pushed["name"] == "cdp-bridge"
    assert pushed["resources"]["clients"][0]["token_hash"] == "newhash"
    # cdp-bridge 强制 auth_enabled=True：WS 握手入口硬要求它才放行，连接鉴权已收口
    # 到每客户端连接 token，与 mcp_services.auth_enabled 无关。即使 DB 开关是关的
    # （service_auth 返回 False），下发给 driver 的也必须是 True。
    assert pushed["auth_enabled"] is True


def test_resync_builtin_service_degrades_when_not_registered(monkeypatch):
    """driver 尚未起（未注册）时 resync 返回 False，不抛——下次 start 会带上最新配置。"""
    async def get_service_by_name(name):
        return {"id": 2, "name": name, "env_template": {}, "enabled": True}
    monkeypatch.setattr(runtime_config.mcp_plugin_store, "get_service_by_name", get_service_by_name)

    class _EmptyRegistry:
        def get(self, name):
            return None

    monkeypatch.setattr("mcp_runtime.registry.registry", _EmptyRegistry())
    assert asyncio.run(runtime_config.resync_builtin_service("cdp-bridge")) is False

