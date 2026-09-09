"""MCP 客户端桥接单测：resolve_effective_services / _ensure_tools 注册 / 进程内网关调用。

MCP Runtime 已合并进主程序：agent 直调 sse_gateway._handle_rpc（同进程、同 registry
单例），不再绕 127.0.0.1:8003 回环。list_services、get_service_auth、_handle_rpc 都用
monkeypatch 替身。
"""
import json
import os
import sys

from unittest.mock import AsyncMock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.config import AgentConfig
from agent.mcp_client import MCPManager, resolve_effective_services  # noqa: E402
from agent.tools import ToolContext, ToolRegistry  # noqa: E402
import mcp_plugin_store as mps  # noqa: E402


# ── resolve_effective_services ────────────────────────────────────────────────

def _svc(name: str, *, builtin: bool = False, enabled: bool = True, kind: str | None = None,
         auth_enabled: bool = False, tools_cache=None, svc_id: int | None = None) -> dict:
    return {
        "id": svc_id if svc_id is not None else hash(name) & 0xFFFF,
        "name": name,
        "builtin": builtin,
        "kind": kind,
        "enabled": enabled,
        "auth_enabled": auth_enabled,
        "tools_cache": tools_cache if tools_cache is not None else [
            {"name": f"{name}_tool", "description": f"{name} tool", "input_schema": {}, "service_name": name},
        ],
    }


@pytest.mark.asyncio
async def test_resolve_none_principal_returns_empty(monkeypatch):
    """agent 未绑 principal（principal_id=None）→ 无 MCP 工具。"""
    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("cdp-bridge", builtin=True, svc_id=1),
        _svc("custom-a", builtin=False, svc_id=2),
    ]))
    assert await resolve_effective_services(None) == []


@pytest.mark.asyncio
async def test_resolve_cdp_needs_service_grant(monkeypatch):
    """cdp-bridge 走 service grant 判权（与网关同源）；param 只收窄实例范围，
    不在列表过滤层参与。无 registry 时退化不过滤。"""
    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("cdp-bridge", builtin=True, svc_id=1),
        _svc("mail", builtin=True, svc_id=2),
    ]))

    # 授权 cdp-bridge(1) 但不授权 mail(2) → 只挂 cdp-bridge（param 有无不参与判权）。
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([1]))
    monkeypatch.setattr(mps, "list_principal_params",
                        lambda pid: _coro([{"param_key": "cdp_client_id", "param_value": "7"}]))
    services = await resolve_effective_services(10)
    assert [s["name"] for s in services] == ["cdp-bridge"]

    # 无任何 service 授权 → 两个内置都不挂（即便有 param）。
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([]))
    assert await resolve_effective_services(10) == []


@pytest.mark.asyncio
async def test_resolve_mail_needs_service_grant(monkeypatch):
    """mail 走 service grant 判权。"""
    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("cdp-bridge", builtin=True, svc_id=1),
        _svc("mail", builtin=True, svc_id=2),
    ]))
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([2]))
    monkeypatch.setattr(mps, "list_principal_params",
                        lambda pid: _coro([{"param_key": "mail_account_id", "param_value": "3"}]))
    services = await resolve_effective_services(10)
    assert [s["name"] for s in services] == ["mail"]


@pytest.mark.asyncio
async def test_resolve_ordinary_service_needs_grant(monkeypatch):
    """普通服务（含内置 marketplace-status）看 service 授权集合（mcp_grants service 行）。"""
    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("marketplace-status", builtin=True, svc_id=1),
        _svc("custom-a", builtin=False, svc_id=2),
        _svc("custom-b", builtin=False, svc_id=3),
    ]))
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    # 只授权 marketplace-status(1) 与 custom-a(2)；custom-b(3) 未授权。
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([1, 2]))
    services = await resolve_effective_services(10)
    assert sorted(s["name"] for s in services) == ["custom-a", "marketplace-status"]


@pytest.mark.asyncio
async def test_resolve_disabled_service_filtered(monkeypatch):
    """服务本身 disabled → 即使被授权也不挂。"""
    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("custom-a", builtin=False, svc_id=2, enabled=False),
    ]))
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([2]))
    assert await resolve_effective_services(10) == []


@pytest.mark.asyncio
async def test_resolve_registry_unloaded_filtered(monkeypatch):
    """就绪门改为 registry-loaded：非 session 形态未加载进 registry → 过滤（避免
    agent 拿到 plugin not loaded 的服务）。session 形态不经 registry，跳过此门。
    registry 不可用（无 runtime）→ 退化为不滤（放过）。"""
    # mock registry：custom-a 已加载、custom-b 未加载（_plugins 非空=已初始化）。
    import mcp_runtime.registry as regmod
    fake_registry = type("R", (), {})()
    fake_registry._plugins = {"custom-a": object()}  # 非空 ⇒ 视为已初始化，触发 registry 门
    fake_registry.get = lambda name: {"custom-a": object()}.get(name)
    monkeypatch.setattr(regmod, "registry", fake_registry)

    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("custom-a", builtin=False, svc_id=2, kind="sse"),
        _svc("custom-b", builtin=False, svc_id=3, kind="sse"),
    ]))
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([2, 3]))
    services = await resolve_effective_services(10)
    assert [s["name"] for s in services] == ["custom-a"]


@pytest.mark.asyncio
async def test_resolve_node_hosted_dead_host_filtered(monkeypatch):
    """node_hosted 服务宿主 host_status='dead' → 过滤。"""
    def _svc_node(name, sid, host_status):
        s = _svc(name, builtin=False, svc_id=sid)
        s["deploy_scope"] = "node_hosted"
        s["install_state"] = "ready"
        s["host_status"] = host_status
        return s
    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc_node("live-svc", 2, "alive"),
        _svc_node("dead-svc", 3, "dead"),
    ]))
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([2, 3]))
    services = await resolve_effective_services(10)
    assert [s["name"] for s in services] == ["live-svc"]


@pytest.mark.asyncio
async def test_marketplace_mcp_exposes_management_tools_only(monkeypatch):
    from mcp_runtime.builtin_plugins import marketplace_plugin
    from mcp_runtime.plugin_loader import PluginRegistrar

    registrar = PluginRegistrar("marketplace-status")
    marketplace_plugin.register(registrar)
    names = {tool.name for tool in registrar.tools}
    assert "marketplace_inspect_repository" not in names
    assert names == {
        "marketplace_get_status", "marketplace_list_items", "marketplace_get_item",
        "marketplace_validate_manifest", "marketplace_upsert_item",
        "marketplace_import_items", "marketplace_hide_item", "marketplace_unhide_item",
        "marketplace_delete_item", "marketplace_export",
        "marketplace_list_exportable_channels", "marketplace_import_current_channels",
        "marketplace_start_channel_import", "marketplace_get_channel_import",
        "marketplace_batch_delete_channels",
        # 外部榜单草稿管理（市场管理场景保留能力）
        "marketplace_leaderboard_list", "marketplace_leaderboard_get",
        "marketplace_leaderboard_update", "marketplace_leaderboard_publish",
        "marketplace_leaderboard_unpublish", "marketplace_leaderboard_delete",
        "marketplace_leaderboard_set_sort", "marketplace_leaderboard_sync_field", "marketplace_leaderboard_verify",
        "marketplace_leaderboard_add_github",
    }


@pytest.mark.asyncio
async def test_resolve_list_services_failure_returns_empty(monkeypatch):
    """DB 不可用时退化为空集，不抛异常。"""
    async def boom():
        raise RuntimeError("db down")
    monkeypatch.setattr(mps, "list_services", boom)
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([]))
    assert await resolve_effective_services(10) == []


@pytest.mark.asyncio
async def test_available_mcp_hides_scene_reserved_services(monkeypatch):
    """场景保留 MCP（marketplace-status 等）不出现在 Agent 编辑器的 MCP 选择器。

    它们只由对应场景的对话临时获权（见 _collect_service_tokens 的 scene 门禁）；
    普通 Agent 能勾上就是越权入口，所以 available-mcp 直接不返回。
    """
    from agent.api import list_available_mcp

    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("marketplace-status", builtin=True, svc_id=1),
        _svc("issue-workflow", builtin=True, svc_id=2),
        _svc("review-result", builtin=True, svc_id=3),
        _svc("cdp-bridge", builtin=True, svc_id=4),
        _svc("custom-svc", svc_id=5),
    ]))

    out = await list_available_mcp(None)
    all_names = [s["name"] for s in out["builtin"] + out["admin"] + out["upstream"]]
    assert "marketplace-status" not in all_names
    assert "issue-workflow" not in all_names
    assert "review-result" not in all_names
    assert "cdp-bridge" in all_names
    assert "custom-svc" in all_names


# ── _ensure_tools 索引 MCP 能力（不注册为一级工具）────────────────────────────

@pytest.mark.asyncio
async def test_ensure_tools_indexes_mcp_capabilities_not_first_class_tools(monkeypatch, tmp_path):
    """GA 对齐：MCP 方法进能力索引 + capability_call 路由表，不进一级工具 schema。

    工具集固定为 10 个原子工具，MCP 方法通过 capability_call("<service>.<method>")
    调用——否则每绑一个服务就往每轮提示词里加一份 schema，正是 GA 要避免的膨胀。
    """
    from agent.agent_main import GenericAgent

    monkeypatch.setattr(mps, "list_services", lambda: _coro([
        _svc("cdp-bridge", builtin=True, svc_id=1, tools_cache=[
            {"name": "browser_get_tabs", "description": "list tabs", "input_schema": {"type": "object"}, "service_name": "cdp-bridge"},
        ]),
    ]))
    monkeypatch.setattr(mps, "get_service_auth",
                       lambda sid: _coro({"auth_enabled": False, "allowed_tokens": set()}))
    # principal 绑了 cdp_client_id param（实例范围，driver 读）+ cdp-bridge 服务授权
    # → cdp-bridge 生效（网关/列表只看 service grant，param 不在判权层）。
    monkeypatch.setattr(mps, "list_principal_params",
                        lambda pid: _coro([{"param_key": "cdp_client_id", "param_value": "5"}]))
    monkeypatch.setattr(mps, "list_services_for_mcp_user", lambda pid: _coro([1]))
    monkeypatch.setattr(mps, "get_mcp_user",
                        lambda uid, mask_token=False: _coro({"id": uid, "enabled": True, "token": ""}))

    config = AgentConfig(
        workspace_root=str(tmp_path / "ws"),
        allowed_roots=[str(tmp_path / "ws"), str(tmp_path / "tmp")],
        mcp_user_id=10,
    )
    agent = GenericAgent(config)
    tools = await agent._ensure_tools()
    schema = tools.get_schema()
    names = {entry["function"]["name"] for entry in schema}
    # GA fixed tool set — MCP methods are NOT first-class tools.
    assert "capability_call" in names
    assert "cdp-bridge__browser_get_tabs" not in names
    # The service is indexed for capability_call routing instead.
    cap_index = tools._capability_index
    cdp_entry = next((e for e in cap_index if e.get("service") == "cdp-bridge"), None)
    assert cdp_entry is not None
    assert "browser_get_tabs" in cdp_entry.get("method_names", [])
    # Method metadata carries the input_schema so the seeded SOP can document params.
    assert cdp_entry["methods"][0]["input_schema"] == {"type": "object"}
    # MCP 挂载幂等：重复 _ensure_tools 不应改变 schema 或能力索引。
    await agent._ensure_tools()
    assert len(tools.get_schema()) == len(schema)
    assert len(tools._capability_index) == len(cap_index)


@pytest.mark.asyncio
async def test_agent_config_schema_aggregates_manifest_and_agent_scoped_config_json(monkeypatch):
    """_agent_config_schema 只聚合显式 Agent 侧字段，且按 manifest > config_json 去重。"""
    import mcp.api_catalog as mcp_catalog

    service = {
        "name": "cdp-bridge",
        "env_template": {
            "shared": "from_env",
            "env_only": 1,
        },
    }
    version = {
        "config_json": {
            "config_schema": [
                {"key": "shared", "label": "shared from config", "default": "from_cfg", "description": "cfg desc", "scope": "agent"},
                {"key": "cfg_only", "default": True, "scope": "agent"},
                {"key": "runtime_only", "default": "secret", "scope": "runtime"},
            ]
        }
    }
    monkeypatch.setattr(mcp_catalog, "_manifest_for_service", lambda _svc: {
        "agent_config": {
            "fields": [
                {"key": "shared", "label": "shared from manifest", "type": "string", "default": "from_manifest", "description": "manifest desc", "required": True, "enum": ["a", "b"], "secret": False},
                {"key": "manifest_only", "default": "x", "description": "manifest only", "secret": True},
            ]
        }
    })
    schema = mcp_catalog._agent_config_schema(service, version)
    assert schema["service_name"] == "cdp-bridge"
    fields = {item["key"]: item for item in schema["fields"]}
    assert list(fields) == ["shared", "manifest_only", "cfg_only"]
    assert fields["shared"]["source"] == "manifest"
    assert fields["shared"]["label"] == "shared from manifest"
    assert fields["shared"]["default"] == "from_manifest"
    assert fields["shared"]["required"] is True
    assert fields["manifest_only"]["source"] == "manifest"
    assert fields["manifest_only"]["secret"] is True
    assert fields["cfg_only"]["source"] == "config_json"
    assert fields["cfg_only"]["type"] == "boolean"
    assert "env_only" not in fields
    assert "runtime_only" not in fields


# ── MCPManager.call_tool 返回 ToolResult 形状 ──────────────────────────────────

@pytest.mark.asyncio
async def test_call_tool_returns_ok_toolresult_on_success(monkeypatch):
    """call_tool 成功时返回 {"status":"ok","content":[...]}；失败转 error ToolResult。"""
    mgr = MCPManager()

    async def fake_sse_call(self, service, tool, args):
        return [{"type": "text", "text": "executed"}]
    monkeypatch.setattr(MCPManager, "_sse_call", fake_sse_call)

    result = await mgr.call_tool("cdp-bridge__browser_get_tabs", {})
    assert result["status"] == "ok"
    assert result["service"] == "cdp-bridge"
    assert result["tool"] == "browser_get_tabs"
    assert result["content"] == [{"type": "text", "text": "executed"}]


@pytest.mark.asyncio
async def test_call_tool_missing_service_prefix_returns_error_toolresult():
    mgr = MCPManager()
    result = await mgr.call_tool("no_prefix_tool", {})
    assert result["status"] == "error"
    assert "缺少服务前缀" in result["msg"]


@pytest.mark.asyncio
async def test_call_tool_sse_failure_returns_error_toolresult(monkeypatch):
    mgr = MCPManager()
    from agent.mcp_client import MCPConnectionError

    async def boom(self, service, tool, args):
        raise MCPConnectionError("gateway 503")
    monkeypatch.setattr(MCPManager, "_sse_call", boom)

    result = await mgr.call_tool("cdp-bridge__oops", {})
    assert result["status"] == "error"
    assert "gateway 503" in result["msg"]


# ── 进程内网关路由：不再绕 8003，直调 _handle_rpc ──────────────────────────────

@pytest.mark.asyncio
async def test_call_tool_routes_through_in_process_gateway(monkeypatch):
    """call_tool 走进程内 _handle_rpc，透传 service/method/params/token，整形出 content。"""
    import mcp_runtime.sse_gateway as gw

    seen = {}

    async def fake_handle_rpc(service_name, payload, token):
        seen.update(service=service_name, payload=payload, token=token)
        return {"jsonrpc": "2.0", "id": payload["id"],
                "result": {"content": [{"type": "text", "text": "ran"}]}}
    monkeypatch.setattr(gw, "_handle_rpc", fake_handle_rpc)

    mgr = MCPManager(service_tokens={"cdp-bridge": "tok-abc"})
    result = await mgr.call_tool("cdp-bridge__browser_get_tabs", {"url": "x"})

    assert result["status"] == "ok"
    assert result["content"] == [{"type": "text", "text": "ran"}]
    assert seen["service"] == "cdp-bridge"
    assert seen["token"] == "tok-abc"  # 服务 token 透传给网关做鉴权 + ContextVar 注入
    assert seen["payload"]["method"] == "tools/call"
    assert seen["payload"]["params"] == {"name": "browser_get_tabs", "arguments": {"url": "x"}}


@pytest.mark.asyncio
async def test_gateway_rpc_maps_jsonrpc_error_to_connection_error(monkeypatch):
    """网关返回 JSON-RPC error（如鉴权失败）→ _gateway_rpc 抛 MCPConnectionError。"""
    import mcp_runtime.sse_gateway as gw
    from agent.mcp_client import MCPConnectionError

    async def fake_handle_rpc(service_name, payload, token):
        return {"jsonrpc": "2.0", "id": payload["id"],
                "error": {"code": -32000, "message": "token 未被授权访问该 MCP 服务"}}
    monkeypatch.setattr(gw, "_handle_rpc", fake_handle_rpc)

    mgr = MCPManager()
    with pytest.raises(MCPConnectionError, match="未被授权"):
        await mgr._gateway_rpc("cdp-bridge", "tools/list", {}, "")


@pytest.mark.asyncio
async def test_collect_service_tokens_uses_bound_mcp_user_for_all_services(monkeypatch, tmp_path):
    """绑定 mcp_user_id 的 agent 对**所有**开启鉴权的服务固定用该用户 token（前提是已获授权）。"""
    from agent.agent_main import GenericAgent

    monkeypatch.setattr(mps, "get_mcp_user", lambda uid, mask_token=True: _coro({
        "id": uid,
        "enabled": True,
        "token": "mcpu_bound",
    }))
    # 绑定用户 token 已在两个服务的 allowed_tokens 内 → 两个服务都固定用它。
    monkeypatch.setattr(mps, "get_service_auth", lambda sid: _coro({
        "auth_enabled": True,
        "allowed_tokens": {"mcpu_bound", "mcpu_other"},
    }))

    agent = GenericAgent(AgentConfig(
        workspace_root=str(tmp_path / "ws"),
        allowed_roots=[str(tmp_path / "ws"), str(tmp_path / "tmp")],
        mcp_user_id=7,
    ))
    tokens = await agent._collect_service_tokens([
        _svc("cdp-bridge", builtin=True, auth_enabled=True, svc_id=1),
        _svc("custom", auth_enabled=True, svc_id=2),
    ])

    assert tokens["cdp-bridge"] == "mcpu_bound"
    assert tokens["custom"] == "mcpu_bound"


@pytest.mark.asyncio
async def test_collect_service_tokens_skips_service_when_bound_user_unauthorized(monkeypatch, tmp_path):
    """绑定用户 token 不在服务 allowed_tokens 内 → 不任取，跳过该服务（避免跨用户串台）。"""
    from agent.agent_main import GenericAgent

    monkeypatch.setattr(mps, "get_mcp_user", lambda uid, mask_token=True: _coro({
        "id": uid,
        "enabled": True,
        "token": "mcpu_bound",
    }))
    monkeypatch.setattr(mps, "get_service_auth", lambda sid: _coro({
        "auth_enabled": True,
        "allowed_tokens": {"mcpu_other"},
    }))

    agent = GenericAgent(AgentConfig(
        workspace_root=str(tmp_path / "ws"),
        allowed_roots=[str(tmp_path / "ws"), str(tmp_path / "tmp")],
        mcp_user_id=7,
    ))
    tokens = await agent._collect_service_tokens([
        _svc("custom", auth_enabled=True, svc_id=2),
    ])

    assert "custom" not in tokens


@pytest.mark.asyncio
async def test_marketplace_token_only_issued_in_marketplace_scene(monkeypatch, tmp_path):
    """marketplace-status 的身份 token 只在市场管理场景签发。

    普通 Agent 即使服务清单里还挂着 marketplace-status（历史配置遗留），也拿不到
    身份 token——adapter 拿不到可核验的管理员身份，调用必然失败。
    """
    from agent.agent_main import GenericAgent
    from agent.scene_context import MARKETPLACE_ADMIN, SceneSpec
    import builtin_tool_store

    issued: list[tuple] = []

    async def fake_issue_token(kind, target_id, display_token=False, expires_at=None):
        issued.append((kind, str(target_id)))
        return object(), "mp_token"

    monkeypatch.setattr(builtin_tool_store, "issue_token", fake_issue_token)

    base = {
        "workspace_root": str(tmp_path / "ws"),
        "allowed_roots": [str(tmp_path / "ws"), str(tmp_path / "tmp")],
    }

    # 市场管理场景 → 签发 user 身份 token（管理员上下文 target="__admin__"）。
    scene = SceneSpec(type=MARKETPLACE_ADMIN, services=("marketplace-status",))
    agent = GenericAgent(AgentConfig(**base), scene=scene)
    tokens = await agent._collect_service_tokens([
        _svc("marketplace-status", builtin=True, svc_id=9),
    ])
    assert tokens.get("marketplace-status") == "mp_token"
    assert issued == [("user", "__admin__")]

    # 普通对话（无场景）→ 不签发，服务也不进后面的授权循环。
    issued.clear()
    plain = GenericAgent(AgentConfig(**base))
    tokens = await plain._collect_service_tokens([
        _svc("marketplace-status", builtin=True, svc_id=9),
    ])
    assert "marketplace-status" not in tokens
    assert issued == []


@pytest.mark.asyncio
async def test_collect_service_tokens_never_falls_back_for_unbound_cdp(monkeypatch, tmp_path):
    """未绑定 MCP 用户时 cdp-bridge 禁止任取授权 token，避免跨用户串台。"""
    from agent.agent_main import GenericAgent

    monkeypatch.setattr(mps, "get_service_auth", lambda sid: _coro({
        "auth_enabled": True,
        "allowed_tokens": {"mcpu_allowed"},
    }))

    agent = GenericAgent(AgentConfig(
        workspace_root=str(tmp_path / "ws"),
        allowed_roots=[str(tmp_path / "ws"), str(tmp_path / "tmp")],
    ))
    tokens = await agent._collect_service_tokens([
        _svc("cdp-bridge", builtin=True, auth_enabled=True, svc_id=1),
    ])

    assert "cdp-bridge" not in tokens


@pytest.mark.asyncio
async def test_collect_service_tokens_ignores_auth_enabled_flag(monkeypatch, tmp_path):
    """鉴权一律强制：identity token 签得出时，DB auth_enabled=False 的服务也挂 token。

    曾有的 bug：agent 侧按 DB auth_enabled=False 跳过 token 收集，网关对
    device-control 却强制要求 token（configuration.py 硬编码 True）→ 401
    「请提供 token」。修后 agent 不再看该开关，一律挂 identity token；授权
    与否由网关按 principal param / mcp_service_users 判。
    """
    from agent.agent_main import GenericAgent
    import builtin_tool_store

    async def fake_issue_token(kind, target_id, display_token=False, expires_at=None):
        return object(), "ident_token"

    monkeypatch.setattr(builtin_tool_store, "issue_token", fake_issue_token)

    # 不 mock get_service_auth：identity_token 命中时根本不该走到 legacy 分支
    # （走到说明逻辑漏了）。service dict 的 auth_enabled 全 False 也不影响。
    async def no_legacy(sid):
        raise AssertionError("identity_token 路径不应回落 legacy get_service_auth")

    monkeypatch.setattr(mps, "get_service_auth", no_legacy)

    # agent_id 非 None → issue identity token；mcp_user_id 非 None → 绑定 principal。
    agent = GenericAgent(
        AgentConfig(
            workspace_root=str(tmp_path / "ws"),
            allowed_roots=[str(tmp_path / "ws"), str(tmp_path / "tmp")],
            mcp_user_id=7,
        ),
        agent_id=42,
    )
    tokens = await agent._collect_service_tokens([
        _svc("device-control", builtin=True, auth_enabled=False, svc_id=1),
        _svc("custom-a", auth_enabled=False, svc_id=2),
        _svc("mail", builtin=True, auth_enabled=False, svc_id=3),
    ])

    assert tokens == {
        "device-control": "ident_token",
        "custom-a": "ident_token",
        "mail": "ident_token",
    }


# ── helpers ────────────────────────────────────────────────────────────────────

async def _coro(value):
    return value


# ── 网关 _check_service_auth：鉴权一律强制（无匿名放行）────────────────────────

def _fake_plugin_ctx(enabled=True, auth_enabled=False):
    """造一个最小 plugin，带 ctx（enabled / auth_enabled）。"""
    from mcp_runtime.plugin_loader import PluginContext

    class _P:
        pass

    p = _P()
    p.ctx = PluginContext("fake", enabled=enabled, auth_enabled=auth_enabled)
    return p


@pytest.mark.asyncio
async def test_gateway_requires_token_even_when_service_auth_disabled(monkeypatch):
    """后门已关：服务的 ctx.auth_enabled=False（旧「匿名」形态）也必须带 token → 401。"""
    import mcp_runtime.sse_gateway as gw
    from fastapi import HTTPException

    plugin = _fake_plugin_ctx(enabled=True, auth_enabled=False)
    monkeypatch.setattr(gw.registry, "get", lambda name: plugin)

    with pytest.raises(HTTPException) as ei:
        await gw._check_service_auth("custom-a", "")
    assert ei.value.status_code == 401
    assert "请提供 token" in ei.value.detail


@pytest.mark.asyncio
async def test_gateway_identity_token_custom_service_needs_grant(monkeypatch):
    """agent identity token 调 custom 服务：principal 被授权（mcp_service_users）→ 放行。"""
    import mcp_runtime.sse_gateway as gw
    import builtin_tool_store
    from fastapi import HTTPException

    plugin = _fake_plugin_ctx(enabled=True, auth_enabled=False)
    monkeypatch.setattr(gw.registry, "get", lambda name: plugin)

    async def fake_resolve(tok):
        return {"kind": "identity", "target": None,
                "token": {"target_type": "agent", "target_id": "42"}}
    monkeypatch.setattr(builtin_tool_store, "resolve_token", fake_resolve)
    monkeypatch.setattr(mps, "get_agent_mcp_principal_id", lambda aid: _coro(7))
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "get_service_by_name", lambda name: _coro({"id": 3, "name": name}))
    monkeypatch.setattr(mps, "mcp_user_granted_service", lambda uid, sid: _coro(True))

    # 被授权 → 放行（不抛即通过）。
    await gw._check_service_auth("custom-a", "ident-token")

    # 未授权 → 403。
    monkeypatch.setattr(mps, "mcp_user_granted_service", lambda uid, sid: _coro(False))
    with pytest.raises(HTTPException) as ei:
        await gw._check_service_auth("custom-a", "ident-token")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_gateway_identity_token_builtin_param_still_wins(monkeypatch):
    """agent identity token 调 device-control：principal 有该服务授权（mcp_grants 的
    service 行）→ 放行。param 只收窄实例范围（driver 侧校验），网关不再做「有没有
    param」的前置拦截——校验下沉工具侧（本测试锁的就是这个语义）。"""
    import mcp_runtime.sse_gateway as gw
    import builtin_tool_store
    from fastapi import HTTPException

    plugin = _fake_plugin_ctx(enabled=True, auth_enabled=True)
    monkeypatch.setattr(gw.registry, "get", lambda name: plugin)

    async def fake_resolve(tok):
        return {"kind": "identity", "target": None,
                "token": {"target_type": "agent", "target_id": "42"}}
    monkeypatch.setattr(builtin_tool_store, "resolve_token", fake_resolve)
    monkeypatch.setattr(mps, "get_agent_mcp_principal_id", lambda aid: _coro(7))
    monkeypatch.setattr(mps, "get_service_by_name", lambda name: _coro({"id": 1, "name": name}))
    monkeypatch.setattr(mps, "mcp_user_granted_service", lambda uid, sid: _coro(True))

    # 有服务授权 → 放行（不抛即通过）。param 是否存在不参与网关判权。
    await gw._check_service_auth("device-control", "ident-token")

    # 无服务授权 → 403（实例收窄是插件 driver 的事，网关只判服务级）。
    monkeypatch.setattr(mps, "mcp_user_granted_service", lambda uid, sid: _coro(False))
    with pytest.raises(HTTPException) as ei:
        await gw._check_service_auth("device-control", "ident-token")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_gateway_principal_token_custom_service_needs_grant(monkeypatch):
    """principal 明文 token 直连 custom 服务：在 mcp_service_users 里 → 放行，否则 403。"""
    import mcp_runtime.sse_gateway as gw
    import builtin_tool_store
    from fastapi import HTTPException

    plugin = _fake_plugin_ctx(enabled=True, auth_enabled=False)
    monkeypatch.setattr(gw.registry, "get", lambda name: plugin)

    async def fake_resolve(tok):
        return {"kind": "principal", "target": {"id": 7, "name": "u"}, "token": None}
    monkeypatch.setattr(builtin_tool_store, "resolve_token", fake_resolve)
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "get_service_by_name", lambda name: _coro({"id": 3, "name": name}))
    monkeypatch.setattr(mps, "mcp_user_granted_service", lambda uid, sid: _coro(True))

    await gw._check_service_auth("custom-a", "principal-token")  # 放行

    monkeypatch.setattr(mps, "mcp_user_granted_service", lambda uid, sid: _coro(False))
    with pytest.raises(HTTPException) as ei:
        await gw._check_service_auth("custom-a", "principal-token")
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_gateway_review_result_identity_token_session_scoped(monkeypatch):
    """review-result：编辑器/webhook 链路签的 identity token（target_id=webhook event id，
    不是 agents 行 id）→ 网关只验「已认证身份」，对象级授权由 plugin._scope 收口。"""
    import mcp_runtime.sse_gateway as gw
    import builtin_tool_store
    from fastapi import HTTPException

    plugin = _fake_plugin_ctx(enabled=True, auth_enabled=True)
    monkeypatch.setattr(gw.registry, "get", lambda name: plugin)

    async def fake_resolve(tok):
        return {"kind": "identity", "target": None,
                "token": {"target_type": "agent", "target_id": "98765"}}
    monkeypatch.setattr(builtin_tool_store, "resolve_token", fake_resolve)
    # target_id 是 webhook event id，不是 agents 行 id → principal 反查返回 None，
    # 然后落 session-scoped 专支放行（review-result 对象级授权由 plugin._scope 收口）。
    monkeypatch.setattr(mps, "get_agent_mcp_principal_id", lambda aid: _coro(None))

    await gw._check_service_auth("review-result", "ident-token")  # 放行

    # 非 identity token（如 principal token）→ 403。
    async def fake_resolve_principal(tok):
        return {"kind": "principal", "target": {"id": 7}, "token": None}
    monkeypatch.setattr(builtin_tool_store, "resolve_token", fake_resolve_principal)
    monkeypatch.setattr(mps, "list_principal_params", lambda pid: _coro([]))
    monkeypatch.setattr(mps, "get_service_by_name", lambda name: _coro(None))
    with pytest.raises(HTTPException) as ei:
        await gw._check_service_auth("review-result", "principal-token")
    assert ei.value.status_code == 403
