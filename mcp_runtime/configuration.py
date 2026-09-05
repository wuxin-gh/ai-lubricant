"""Materialize manifest-declared shared MCP configuration into runtime snapshots."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import mcp_plugin_store

_BUILTIN_ROOT = Path(__file__).resolve().parent.parent / "mcp_builtin"


def _manifest_for_service(service: dict) -> dict:
    path = _BUILTIN_ROOT / service["name"].replace("-", "_") / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {"schema": 1, "name": service["name"]}


@dataclass(frozen=True)
class BuiltinConfigSnapshot:
    service_id: int
    service_name: str
    enabled: bool
    env: dict[str, Any]
    auth_enabled: bool
    allowed_tokens: set[str]
    resources: dict[str, Any]


async def _materialize_shared_resource(service_id: int, definition: dict) -> Any:
    storage = definition.get("storage") or {}
    config_type = str(storage.get("configType") or "")
    if config_type not in mcp_plugin_store.ALLOWED_RUNTIME_CONFIG_TYPES:
        raise ValueError(f"unsupported runtime config type: {config_type}")
    if definition.get("cardinality") == "singleton":
        return await mcp_plugin_store.get_runtime_config_by_key(service_id, config_type, "singleton", include_secrets=True) or dict(storage.get("defaults") or {})
    if definition.get("relation"):
        return None
    rows = await mcp_plugin_store.list_runtime_configs(service_id, config_type, include_secrets=True)
    nested = storage.get("embed") or {}
    if nested:
        child_type = str(nested.get("configType") or "")
        field = str(nested.get("field") or child_type)
        children = await mcp_plugin_store.list_runtime_configs(service_id, child_type, include_secrets=True)
        for row in rows:
            row[field] = [child for child in children if child.get("parent_instance_key") == row["instance_key"]]
    return rows


async def build_builtin_snapshot(service: dict) -> BuiltinConfigSnapshot:
    service_id = int(service["id"])
    env = await mcp_plugin_store.get_service_env(service_id) or dict(service.get("env_template") or {})
    auth = await mcp_plugin_store.get_service_auth(service_id)
    resources: dict[str, Any] = {"service_id": service_id}
    definitions = ((_manifest_for_service(service).get("config") or {}).get("resources") or {})
    for key, definition in definitions.items():
        if "runtime-apply" not in (definition.get("capabilities") or []):
            continue
        if definition.get("binding") != "shared":
            raise ValueError(f"unsupported runtime resource binding: {definition.get('binding')}")
        value = await _materialize_shared_resource(service_id, definition)
        if value is not None:
            resources[key] = value
    # 内置工具（CDP/邮箱）的实例数据现在源自 builtin_tool_* 新表（用户拥有实例 +
    # 面向使用方的 token），不再走 mcp_runtime_configs。数据源在此收口投影成连接层
    # 既有的输入形状，连接逻辑（driver / mail 插件）一行不动。
    import builtin_tool_store
    auth_enabled = bool(auth["auth_enabled"])
    if service.get("name") == "cdp-bridge":
        # client 行喂给 driver 的 clients_by_hash（token_hash → client 行）。会话池按
        # client_id=实例 id 隔离；一个 CDP 实例 = 一个浏览器，多 token 共享同一 client_id。
        # token → client_id 的解析不再预计算（新表只存 hash，明文不落库，映射建不出来）：
        # 请求时由 driver.authenticate_client(明文 token) 现场 hash 查表解析，与浏览器
        # 扩展 WS 握手走同一条路径。故这里不再产出 _cdp_client_ids_by_token。
        resources["clients"] = await builtin_tool_store.cdp_driver_clients()
        # cdp-bridge 的连接鉴权已收口到「每客户端一个连接 token」（cdp_client 明细行，
        # 由 driver.clients_by_hash 现场校验），与 mcp_services.auth_enabled + mcp_service_users
        # 那套 MCP 用户 token 无关。但 WS 握手入口（sse_gateway.ext_session）硬要求
        # ctx.auth_enabled=True 才放行，否则连 token 都不校验就 4403。CDP 本就没有
        # 「任何人可连」模式，故这里强制 auth_enabled=True——否则管理员没手动开该 DB
        # 开关时，所有扩展连接会在校验 token 前就被拒（正是「连不上」的第二个成因）。
        auth_enabled = True
    elif service.get("name") == "mail":
        resources["upstream_accounts"] = await builtin_tool_store.mail_upstream_accounts()
    elif service.get("name") == "device-control":
        # device-control 的连接凭据存 device 明细行（token_hash + device_id）。这里把所有
        # enabled 实例下 enabled 且有 token_hash 的设备收成 hash 集合，喂给 driver 的
        # authorized_hashes 快照。WS 握手每帧复查这个集合：解除配对/轮换后 hash 消失，
        # 活连接当场 close 4003，不等心跳超时（与 CP 的 clients_by_hash 复查同思路）。
        # 与 cdp-bridge 同理：device-control 没有「匿名设备」模式，强制 MCP 工具鉴权开启，
        # 否则任何拿到 SSE 地址的客户端都能驱动任意设备。
        resources["authorized_hashes"] = await builtin_tool_store.device_authorized_hashes()
        auth_enabled = True
    return BuiltinConfigSnapshot(service_id, service["name"], bool(service.get("enabled", True)), env, auth_enabled, set(auth["allowed_tokens"]), resources)


async def resync_builtin_service(name: str) -> bool:
    """把某内置服务（cdp-bridge / mail）的最新配置重下发给运行中的 driver。

    用户侧改了内置工具实例数据（新建 CDP 客户端、配邮箱账户等）后，DB 是变了，但
    运行中的 driver 内存态（``clients_by_hash`` / mail ctx.resources）不会自己更新——
    admin 侧靠 reload/start 触发这条链，用户侧没有。本 helper 就是那条链的内部入口：
    同进程直接调 registry 单例（MCP Runtime 已合并进主程序），无需 admin 鉴权，无网络。

    - 服务不存在 / 未注册：返回 False（driver 尚未起，下次 start 会带上最新配置）。
    - 成功下发：返回 True。

    绝不抛：重下发失败不应让用户的保存操作回滚（DB 已落，下次 reload 兜底）。
    """
    from .registry import registry

    try:
        service = await mcp_plugin_store.get_service_by_name(name)
        if not service:
            return False
        if registry.get(name) is None:
            return False
        snapshot = await build_builtin_snapshot(service)
        await registry.update_builtin_config(
            name,
            env=snapshot.env,
            resources=snapshot.resources,
            enabled=snapshot.enabled,
            auth_enabled=snapshot.auth_enabled,
            allowed_tokens=snapshot.allowed_tokens,
        )
        return True
    except Exception:
        return False
