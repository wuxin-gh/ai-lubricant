"""MCP client integration layer.

This module intentionally keeps the MCP integration thin and defensive:
- It stores/normalizes MCP server configuration.
- It can test whether a server command/URL is plausibly reachable.
- It exposes a stable MCPManager interface for later tool registration.

Full protocol-level calls are isolated here so agent/tools.py does not need
transport-specific logic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal


logger = logging.getLogger(__name__)

MCPTransport = Literal["stdio", "streamable-http"]


@dataclass
class MCPServerConfig:
    name: str
    transport: MCPTransport = "stdio"
    command: str | None = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    builtin: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MCPServerConfig":
        raw_args = data.get("args") or []
        if isinstance(raw_args, str):
            try:
                raw_args = json.loads(raw_args)
            except Exception:
                raw_args = [raw_args]
        # env 可能是 asyncpg 返回的 JSONB 字符串，先解码再转 dict（否则 dict("...") 抛 ValueError）。
        raw_env = data.get("env") or {}
        if isinstance(raw_env, str):
            try:
                raw_env = json.loads(raw_env)
            except Exception:
                raw_env = {}
        if not isinstance(raw_env, dict):
            raw_env = {}
        raw_headers = data.get("headers") or {}
        if isinstance(raw_headers, str):
            try:
                raw_headers = json.loads(raw_headers)
            except Exception:
                raw_headers = {}
        if not isinstance(raw_headers, dict):
            raw_headers = {}
        return cls(
            name=data["name"],
            transport=data.get("transport") or "stdio",
            command=data.get("command"),
            args=list(raw_args),
            env=dict(raw_env),
            url=data.get("url"),
            headers={str(k): str(v) for k, v in raw_headers.items() if v is not None},
            builtin=bool(data.get("builtin", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "transport": self.transport,
            "command": self.command,
            "args": self.args,
            "env": self.env,
            "url": self.url,
            "headers": self.headers,
            "builtin": self.builtin,
        }


@dataclass
class MCPTool:
    name: str
    description: str = ""
    input_schema: dict[str, Any] = field(default_factory=dict)
    service_name: str = ""

    def to_openai_schema(self) -> dict[str, Any]:
        """Convert MCP tool schema to OpenAI function-calling schema."""
        function_name = f"{self.service_name}__{self.name}" if self.service_name else self.name
        return {
            "type": "function",
            "function": {
                "name": function_name,
                "description": self.description or f"MCP tool {self.name}",
                "parameters": self.input_schema or {"type": "object", "properties": {}},
            },
        }


class MCPConnectionError(RuntimeError):
    pass


def _is_builtin(service: dict) -> bool:
    return bool(service.get("builtin")) or service.get("kind") == "builtin"


# 就绪门的黑名单：这些状态说明服务正在装或装失败了，不该给 agent。
# 'created' 不在其中——存量服务的 DB 默认值是 created，挡它会让现有可用服务消失。
_INSTALL_NOT_READY_STATES = frozenset({"configuring", "starting", "testing", "error"})


async def resolve_effective_services(principal_id: int | None) -> list[dict]:
    """解析 agent 的有效 MCP 服务集——完全由绑定 principal 的授权推导。

    agent 不再自带 MCP 服务清单（原 agents.mcp_servers 已删）：它只关联一个 MCP
    principal（agents.mcp_user_id），挂哪些服务由该 principal 被授权了什么决定。
    与网关判权（sse_gateway._check_service_auth）同源，避免「列表里有工具但调用被 403」。

    规则：
      - principal_id 为 None（未绑定）→ 无 MCP 工具。
      - 内置工具服务按 param 判权：cdp-bridge 需 principal 有 cdp_client_id param，
        mail 需有 mail_account_id param（param 值即它能操作的那个客户端/账户）。
      - 其余服务（含内置 marketplace-status 与普通 custom/sse/stdio）按服务级授权
        判权：principal 在 mcp_service_users 里被授权该服务才挂。
      - 服务本身必须 enabled，且过就绪门（安装中/失败、node_hosted 宿主已死均排除）。

    返回项直接复用 mcp_plugin_store 的 service dict（含 tools_cache/id/auth/headers 等）；
    kind 由 DB 行自带（builtin/custom/sse/stdio），MCPManager 不再分派，全部过 _gateway_rpc。
    """
    import mcp_plugin_store
    # 判权 param 映射的规范来源在网关；函数内惰性导入，避免模块加载期循环依赖。
    from mcp_runtime.sse_gateway import _BUILTIN_SERVICE_PARAM_KEY

    if principal_id is None:
        return []

    try:
        # 三个独立维度各自 try/except 退化为「该维度无授权」，但彼此并发：agent
        # 启动每次展开都走这里，串行会把三段 Redis/DB 往返叠成 sum。
        services_result, params_result, granted_result = await asyncio.gather(
            mcp_plugin_store.list_services(),
            mcp_plugin_store.list_principal_params(int(principal_id)),
            mcp_plugin_store.list_services_for_mcp_user(int(principal_id)),
            return_exceptions=True,
        )
    except Exception as exc:  # noqa: BLE001 — gather 内异常已 return_exceptions，这里只兜未覆盖的
        logger.warning("[mcp] effective services prefetch failed principal=%s: %s", principal_id, exc)
        return []

    if isinstance(services_result, Exception):
        logger.warning("[mcp] list_services failed, no MCP tools attached: %s", services_result)
        return []
    all_services = services_result
    if isinstance(params_result, Exception):
        logger.warning("[mcp] list_principal_params failed principal=%s: %s", principal_id, params_result)
        params = []
    else:
        params = params_result
    if isinstance(granted_result, Exception):
        logger.warning("[mcp] list_services_for_mcp_user failed principal=%s: %s", principal_id, granted_result)
        granted_ids = set()
    else:
        granted_ids = set(granted_result)

    param_keys = {str(p.get("param_key") or "") for p in params if p.get("param_key")}

    effective: list[dict] = []
    for svc in all_services:
        if not svc.get("enabled"):
            continue
        name = svc.get("name") or ""
        required_param = _BUILTIN_SERVICE_PARAM_KEY.get(name)
        if required_param is not None:
            # 内置工具服务：有对应 param 才代表这个 principal 能操作某个具体资源。
            if required_param not in param_keys:
                continue
            effective.append(svc)
            continue

        sid = svc.get("id")
        if sid is None or sid not in granted_ids:
            continue
        # 就绪门：挡住「明确处于安装中或安装失败」的服务，避免 agent 拿到装了一半的
        # MCP 在 registry.call_tool 抛 KeyError("plugin not loaded")。
        #
        # 注意是黑名单而非白名单：存量服务（本功能上线前建的）没有 install_state，
        # DB 默认 'created'，若要求必须等于 ready 会让现有可用服务突然对 agent 消失。
        # session 形态不经服务端 registry（编辑器 CLI 自己在节点拉起），不受此门约束。
        scope = svc.get("deploy_scope") or "server"
        state = svc.get("install_state")
        if scope != "session" and state in _INSTALL_NOT_READY_STATES:
            logger.info(
                "[mcp] service %s install_state=%s, not ready for agent",
                svc.get("name"), state,
            )
            continue
        # node_hosted（形态 C）多一道门：进程在节点上，节点掉线后 install_state
        # 可能还停在 ready，但隧道已经不通。host_status 是那个真相源。
        if scope == "node_hosted" and svc.get("host_status") == "dead":
            logger.info(
                "[mcp] node-hosted service %s host is dead, not ready for agent",
                svc.get("name"),
            )
            continue
        effective.append(svc)
    return effective


class MCPManager:
    """Manage MCP server connections for one Agent instance.

    Current implementation provides lifecycle/test scaffolding and a stable API.
    Protocol-specific tool discovery/calls can be extended here without changing
    agent_loop or tools registry code.
    """

    def __init__(
        self,
        server_configs: list[dict[str, Any]] | None = None,
        service_tokens: dict[str, str] | None = None,
        cdp_session_id: str = "",
    ):
        self.configs = [MCPServerConfig.from_dict(c) for c in (server_configs or [])]
        self.tools: dict[str, MCPTool] = {}
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        # service_name -> token（服务开启鉴权时，SSE/RPC 用它带 ?token= / Bearer）。
        self._service_tokens: dict[str, str] = dict(service_tokens or {})
        # One logical holder per Agent runtime. It survives multiple MCP tool calls
        # so scan -> execute -> wait owns the same CDP tab until this manager closes.
        self.lease_id = f"agent:{uuid.uuid4().hex[:16]}"
        self.cdp_session_id = str(cdp_session_id or "")

    async def test_config(self, config: MCPServerConfig) -> dict[str, Any]:
        """Validate that the MCP service config is usable enough to attempt launch/connect."""
        if config.name == "cdp-bridge" or config.builtin:
            return {"ok": True, "message": "Built-in MCP service is loaded by mcp_runtime"}

        if config.transport == "stdio":
            if not config.command:
                return {"ok": False, "error": "stdio transport requires command"}
            executable = config.command.split()[0]
            if not shutil.which(executable):
                return {"ok": False, "error": f"Command not found: {executable}"}
            return {"ok": True, "message": "Command is available"}

        if config.transport == "streamable-http":
            if not config.url:
                return {"ok": False, "error": "streamable-http transport requires url"}
            # Avoid adding hard dependency on httpx/aiohttp; use stdlib in thread.
            import urllib.request
            try:
                def _probe():
                    req = urllib.request.Request(config.url, method="GET")
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        return resp.status
                status = await asyncio.to_thread(_probe)
                return {"ok": True, "message": f"HTTP endpoint reachable: {status}"}
            except Exception as exc:
                return {"ok": False, "error": f"HTTP endpoint not reachable: {exc}"}

        return {"ok": False, "error": f"Unsupported transport: {config.transport}"}

    async def test_all(self) -> list[dict[str, Any]]:
        # 并发探测每个 MCP server：test_config 是独立的网络 IO，串行会把耗时
        # 叠加成 sum；gather 让总耗时≈最慢一个。单条失败不拖垮其余。
        results = await asyncio.gather(
            *[self.test_config(cfg) for cfg in self.configs]
        )
        for cfg, result in zip(self.configs, results):
            result["name"] = cfg.name
        return list(results)

    async def discover_tools(self, config: MCPServerConfig) -> list[MCPTool]:
        """Discover tools via MCP-over-SSE tools/list.

        This is used as a fallback when DB tools_cache is missing/stale. Runtime
        failures are reported as an empty list so agent startup can continue.
        """
        try:
            tools_payload = await self._sse_list_tools(config.name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[mcp] tools/list failed service=%s: %s", config.name, exc)
            return []
        tools: list[MCPTool] = []
        for item in tools_payload:
            if not isinstance(item, dict):
                continue
            name = item.get("name") or ""
            if not name:
                continue
            tools.append(
                MCPTool(
                    name=name,
                    description=item.get("description") or "",
                    input_schema=item.get("inputSchema") or item.get("input_schema") or {"type": "object", "properties": {}},
                    service_name=config.name,
                )
            )
        return tools

    async def discover_all_tools(self) -> list[MCPTool]:
        # 并发发现：每个 server 的 tools/list 是独立网络 IO，串行会把启动
        # 延迟叠成 sum。失败已在 discover_tools 内兜底成空数组，gather 安全。
        per_service = await asyncio.gather(
            *[self.discover_tools(cfg) for cfg in self.configs]
        )
        all_tools: list[MCPTool] = []
        for cfg, tools in zip(self.configs, per_service):
            for tool in tools:
                self.tools[f"{cfg.name}__{tool.name}"] = tool
            all_tools.extend(tools)
        return all_tools

    def _split_namespaced(self, namespaced_name: str) -> tuple[str, str]:
        """拆 service__tool；无分隔符时 service 归空、整名作工具名。"""
        if "__" in namespaced_name:
            service, tool = namespaced_name.split("__", 1)
            return service, tool
        return "", namespaced_name

    async def call_tool(self, namespaced_name: str, arguments: dict[str, Any]) -> Any:
        """通过 MCP-over-SSE 网关调用 runtime 里已加载的工具。

        协议对齐 mcp_runtime/sse_gateway.py：
          GET /mcp/{service}/sse            → 读首个 event: endpoint 拿 messages URL
          POST {messages}                   → initialize（响应从 SSE 流按 id 回读）
          POST {messages}                   → tools/call（同上）
        返回 loop 期望的 ToolResult dict（{"status": "ok"/"error", ...}）。
        """
        service_name, tool_name = self._split_namespaced(namespaced_name)
        if not service_name:
            return {"status": "error", "msg": f"MCP 工具名缺少服务前缀: {namespaced_name}"}
        try:
            content = await self._sse_call(service_name, tool_name, arguments or {})
        except MCPConnectionError as exc:
            return {"status": "error", "msg": str(exc)}
        except Exception as exc:  # noqa: BLE001 — 工具调用失败必须变成 ToolResult，不能冒泡打断 loop
            logger.warning("[mcp] call_tool %s failed: %s", namespaced_name, exc)
            return {"status": "error", "msg": f"MCP 调用失败: {exc}"}
        return {"status": "ok", "content": content, "service": service_name, "tool": tool_name}

    @staticmethod
    async def _gateway_rpc(service_name: str, method: str, params: dict, token: str) -> dict:
        """进程内直调 MCP 网关的 JSON-RPC 处理器。

        MCP Runtime 已合并进主程序：网关（sse_gateway._handle_rpc）与本 agent 同进程、
        同 registry 单例。原先绕 127.0.0.1:8003 的 SSE 握手/回环纯属自打回环——runtime
        退役后那个端口根本没人监听。这里直接调 _handle_rpc，鉴权（_check_service_auth）、
        请求 token 注入 ContextVar（cdp-bridge 按 client_id 隔离要用）、content 整形都在
        网关侧原样发生，与外部 MCP 客户端走 SSE 完全同一条路径。
        """
        from mcp_runtime.sse_gateway import _handle_rpc

        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            result = await _handle_rpc(service_name, payload, token)
        except Exception as exc:  # noqa: BLE001 — 网关鉴权/未加载等抛的异常一律归为连接错误
            raise MCPConnectionError(f"MCP 网关调用失败 service={service_name} method={method}: {exc}") from exc
        if not isinstance(result, dict):
            raise MCPConnectionError(f"MCP 网关 {method} 无响应 service={service_name}")
        if "error" in result:
            err = result["error"] or {}
            raise MCPConnectionError(err.get("message") or f"MCP error: {err}")
        return result.get("result") or {}

    async def _sse_call(self, service_name: str, tool_name: str, arguments: dict[str, Any]) -> list[dict]:
        """经进程内网关执行 tools/call，返回 result.content 列表。"""
        token = self._service_tokens.get(service_name, "")
        from mcp_runtime.plugin_loader import current_cdp_holder, current_cdp_session_id
        holder_token = current_cdp_holder.set(self.lease_id)
        session_token = current_cdp_session_id.set(self.cdp_session_id)
        try:
            payload = await self._gateway_rpc(
                service_name, "tools/call", {"name": tool_name, "arguments": arguments}, token
            )
        finally:
            current_cdp_session_id.reset(session_token)
            current_cdp_holder.reset(holder_token)
        content = payload.get("content")
        if isinstance(content, list):
            return content
        return [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}]

    async def _sse_list_tools(self, service_name: str) -> list[dict]:
        """经进程内网关 tools/list，返回 tool 描述列表。tools_cache 回退时用。"""
        token = self._service_tokens.get(service_name, "")
        payload = await self._gateway_rpc(service_name, "tools/list", {}, token)
        tools = payload.get("tools")
        return tools if isinstance(tools, list) else []

    async def close(self) -> None:
        """Stop any subprocesses launched by the manager and release CDP leases."""
        # Release any CDP tab leases this Agent runtime acquired under its holder.
        # In-process driver lives in mcp_builtin.cdp_bridge.server; missing driver
        # or import failure must never block agent teardown.
        try:
            from mcp_builtin.cdp_bridge.server import release_holder
            release_holder(self.lease_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[mcp] release_holder %s failed: %s", self.lease_id, exc)
        for proc in list(self._processes.values()):
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
        self._processes.clear()
