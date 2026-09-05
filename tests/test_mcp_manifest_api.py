"""MCP manifest / 连接配置端点测试。

覆盖本次「用户管理统一化 + CDP 面板重构」的关键契约：
- 自定义服务默认 manifest：home_actions 用 users 取代 access，含 tools 面板，无 access。
- CDP 内置 manifest：panels 为 {users, tools, clients, config}，无 access、无 agent_config，
  clients 面板带 save_grants 存储。
- runtime_client_config：鉴权开启时只返回 token_template(<TOKEN>) 与非敏感授权用户元数据。
- UpdateMCPServiceAuthRequest 默认 auth_enabled=True。
"""
import os
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import mcp.api as mcp_api


CDP = "cdp-bridge"
MAIL = "mail"


def test_default_manifest_uses_config_and_tools_without_service_users():
    m = mcp_api._default_manifest({"name": "custom-x", "display_name": "Custom X"})
    action_ids = [a["id"] for a in m["home_actions"]]
    assert action_ids == ["info", "env", "config", "tools"]
    assert "users" not in m["panels"]
    assert m["panels"]["tools"]["kind"] == "tools"
    assert m["panels"]["config"]["storage"]["get"] == "/mcp/runtime/services/{service_id}/client-config"


def test_cdp_manifest_uses_uniform_resources_and_runtime_view():
    m = mcp_api._load_builtin_manifest(CDP)
    assert m and "_error" not in m
    assert m["schema"] == 2
    # 「驱动配置」tab 已并入「客户端列表」tab（clients panel 现为 resource 面板）。
    assert set(m["panels"].keys()) == {"clients", "tools", "config"}
    assert m["panels"]["clients"]["kind"] == "resource"
    resources = m["config"]["resources"]
    assert resources["driver_settings"]["cardinality"] == "singleton"
    assert "legacy_ws_port" not in resources["driver_settings"]["schema"]["properties"]
    assert "multi_user" not in resources["driver_settings"]["schema"]["properties"]
    assert resources["driver_settings"]["binding"] == "shared"
    assert resources["driver_settings"]["storage"]["configType"] == "cdp_driver"
    assert resources["clients"]["binding"] == "shared"
    assert resources["clients"]["ui"]["renderer"] == "cdp_clients"
    assert resources["clients"]["schema"]["properties"]["client_id"]["readOnly"] is True
    assert resources["clients"]["storage"]["configType"] == "cdp_client"
    assert resources["clients"]["storage"]["handler"] == "cdp-client"
    # 方向翻转：可操作用户（多选）+ 可操作 agent 列表。
    assert resources["clients"]["schema"]["properties"]["user_ids"]["type"] == "array"
    assert resources["clients"]["schema"]["properties"]["user_ids"]["x-reference"] == "mcp_users"
    assert resources["clients"]["schema"]["properties"]["agent_ids"]["type"] == "array"
    assert resources["clients"]["schema"]["properties"]["agent_ids"]["x-reference"] == "agents"
    assert "session_grants" not in resources
    assert m["config"]["actions"]["rotate_client_token"]["operation"] == "rotate-token"
    assert m["config"]["actions"]["revoke_client"]["operation"] == "revoke"
    assert m["config"]["views"]["sessions"]["binding"] == "cdp.sessions"


def test_mail_manifest_uses_uniform_resources_and_query_action():
    m = mcp_api._load_builtin_manifest(MAIL)
    assert m and "_error" not in m
    assert m["schema"] == 2
    assert m["panels"]["configuration"]["kind"] == "resource"
    assert m["panels"]["messages"]["kind"] == "action"
    resources = m["config"]["resources"]
    assert resources["upstream_accounts"]["binding"] == "shared"
    assert resources["upstream_accounts"]["storage"]["handler"] == "mail-account"
    assert resources["upstream_accounts"]["storage"]["configType"] == "mail_account"
    suffix_schema = resources["upstream_accounts"]["schema"]["properties"]["mail_suffix"]
    assert suffix_schema["type"] == "string"
    assert "mail_suffix" in resources["upstream_accounts"]["ui"]["order"]
    assert resources["address_mappings"]["binding"] == "shared"
    assert resources["address_mappings"]["relation"]["parentResource"] == "upstream_accounts"
    assert m["config"]["actions"]["query_messages"]["binding"] == "mail.query_messages"
    assert m["config"]["actions"]["query_messages"]["inputSchema"]["properties"]["offset"]["default"] == 0
    assert m["config"]["actions"]["query_messages"]["ui"]["resources"] == {
        "accounts": "upstream_accounts", "addresses": "address_mappings",
    }


def test_auth_request_defaults_enabled():
    req = mcp_api.UpdateMCPServiceAuthRequest()
    assert req.auth_enabled is True


@pytest.fixture
def client(monkeypatch):
    async def _noop_admin(authorization):
        return None

    monkeypatch.setattr(mcp_api, "_require_admin", _noop_admin)
    monkeypatch.setattr(mcp_api, "_runtime_public_base", lambda: "http://host:8003")

    async def _fake_get_service(service_id):
        return {"id": service_id, "name": CDP, "runtime_status": "loaded", "enabled": True}

    async def _fake_get_service_auth(service_id):
        return {"auth_enabled": True, "allowed_tokens": {"secret-a", "secret-b"}, "allowed_user_ids": {1, 2}}

    async def _fake_list_users(mask_token=True):
        assert mask_token is True
        return [
            {"id": 1, "name": "alice", "token": "********", "enabled": True},
            {"id": 2, "name": "bob", "token": "********", "enabled": True},
            {"id": 3, "name": "carol", "token": "********", "enabled": True},
        ]

    monkeypatch.setattr(mcp_api.mcp_plugin_store, "get_service", _fake_get_service)
    monkeypatch.setattr(mcp_api.mcp_plugin_store, "get_service_auth", _fake_get_service_auth)
    monkeypatch.setattr(mcp_api.mcp_plugin_store, "list_mcp_users", _fake_list_users)

    app = FastAPI()
    app.include_router(mcp_api.router)
    return TestClient(app)


def test_client_config_tokenized(client):
    r = client.get("/mcp/services/7/client-config")
    assert r.status_code == 200
    body = r.json()
    assert body["auth_enabled"] is True
    # 顶层 sse_url 为裸 URL，token_template 用 <TOKEN> 占位
    assert body["sse_url"] == "http://host:8003/mcp/cdp-bridge/sse"
    assert body["token_template"] == "http://host:8003/mcp/cdp-bridge/sse?token=<TOKEN>"
    # 顶层配置片段用模板占位
    cd_url = body["configs"]["claude_desktop"]["mcpServers"]["cdp-bridge"]["url"]
    assert cd_url.endswith("?token=<TOKEN>")
    # users 只含已授权用户（carol 未授权应被排除），且没有凭据或即用 URL。
    assert body["users"] == [
        {"user_id": 1, "name": "alice"},
        {"user_id": 2, "name": "bob"},
    ]
    assert "authorized_tokens" not in body
    serialized = str(body)
    assert "secret-a" not in serialized
    assert "secret-b" not in serialized
    # cdp-bridge 返回内置 WebSocket 接入地址（http→ws）
    assert body["ws_session_url"] == "ws://host:8003/mcp/cdp-bridge/session"


def test_list_tools_uses_cache(monkeypatch):
    """tools 端点：命中 tools_cache 直接返回，env_template 为 JSON 字符串也不 500。"""
    async def _fake_get_service(service_id):
        # tools_cache 已由 service_row_to_dict 解码成 list；env_template 保持字符串
        # 以复现旧 bug 场景（此路径命中缓存不会走 from_dict，故只要不 500 即可）。
        return {
            "id": service_id,
            "name": CDP,
            "builtin": True,
            "transport": "stdio",
            "env_template": '{"FOO": "bar"}',
            "tools_cache": [
                {"name": "browser_scan", "description": "scan", "input_schema": {"type": "object"}, "service_name": CDP},
            ],
        }

    monkeypatch.setattr(mcp_api.mcp_plugin_store, "get_service", _fake_get_service)

    app = FastAPI()
    app.include_router(mcp_api.router)
    c = TestClient(app)
    r = c.get("/mcp/services/1/tools")
    assert r.status_code == 200
    tools = r.json()
    assert len(tools) == 1
    assert tools[0]["name"] == "browser_scan"
    assert tools[0]["service_name"] == CDP


def test_from_dict_decodes_jsonb_string_env():
    """MCPServerConfig.from_dict：env/args 为 JSONB 字符串时不再抛 ValueError。"""
    from agent.mcp_client import MCPServerConfig

    cfg = MCPServerConfig.from_dict({
        "name": "x",
        "transport": "stdio",
        "env": '{"A": "1"}',
        "args": '["--flag"]',
    })
    assert cfg.env == {"A": "1"}
    assert cfg.args == ["--flag"]
