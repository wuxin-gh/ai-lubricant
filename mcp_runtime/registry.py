"""Registry — Runtime 内存里的活动能力注册表，热注册核心。

register/unregister 全走内存操作，Runtime 不重启即生效。Runtime 启动时从 DB
恢复所有 enabled 且有 active_version 的 service。
"""
from __future__ import annotations

import asyncio
from importlib import import_module
from typing import Any

from loguru import logger

from mcp_builtin.catalog import (
    INTERNAL_BUILTIN_SERVICE_SPECS,
    PERSISTED_BUILTIN_SERVICE_SPECS,
)

from .plugin_loader import (
    ActionDef, PluginContext, ToolDef, ViewDef, load_plugin_source,
    PluginLoadError, PluginRegistrar,
)

# Built-in plugin adapters keyed by service name. Each adapter exposes a
# ``register(reg)`` function compatible with PluginRegistrar and is imported
# normally (no exec). These are capabilities vendored into this repo and are
# part of the runtime itself — always-on, registered once at boot, no
# enable/start/stop gating (that lifecycle belongs to stdio/custom plugins).
# The adapter module's top level only depends on plugin_loader; heavy/optional
# deps are imported lazily inside register(), so importing it here is safe.
#
# 服务名单的唯一声明源是 mcp_builtin.catalog（DB seed 也消费它），这里只按 spec
# 解析出 adapter 模块对象，不再各处手写服务名。
_BUILTIN_ADAPTERS: dict[str, Any] = {
    spec.name: import_module(spec.adapter_module) for spec in PERSISTED_BUILTIN_SERVICE_SPECS
}

# Internal session services are runtime infrastructure rather than persisted,
# administrator-configurable MCP services. Their authorization context comes
# from each request, so startup must not depend on an mcp_services row.
_INTERNAL_BUILTIN_ADAPTERS: dict[str, Any] = {
    spec.name: import_module(spec.adapter_module) for spec in INTERNAL_BUILTIN_SERVICE_SPECS
}


def _builtin_adapter(name: str) -> Any | None:
    return _BUILTIN_ADAPTERS.get(name) or _INTERNAL_BUILTIN_ADAPTERS.get(name)


class LoadedPlugin:
    """一个已加载的 custom 插件：工具表 + 命名空间（持有函数引用防 GC）。"""
    __slots__ = ("name", "tools", "actions", "views", "namespace", "ctx")

    def __init__(self, name: str, tools: list[ToolDef], namespace: dict, ctx: PluginContext, *, actions: list[ActionDef] | None = None, views: list[ViewDef] | None = None):
        self.name = name
        self.tools = tools
        self.actions = actions or []
        self.views = views or []
        self.namespace = namespace
        self.ctx = ctx

    def tool_names(self) -> list[str]:
        return [t.name for t in self.tools]

    def find(self, tool_name: str) -> ToolDef | None:
        for t in self.tools:
            if t.name == tool_name:
                return t
        return None


class MCPRegistry:
    """单例注册表。所有 activate/unregister 都经过这里。"""

    def __init__(self) -> None:
        self._plugins: dict[str, LoadedPlugin] = {}
        self._lock = asyncio.Lock()
        # SSE Gateway 订阅工具列表变化（用于推送 tools/list_changed 事件）
        self._subscribers: list = []

    def get(self, name: str) -> LoadedPlugin | None:
        return self._plugins.get(name)

    def get_builtin_state(self, name: str) -> list[dict]:
        """取内置服务的运行态快照（管理端聚合视图用，不直读插件内部全局）。

        adapter 可选暴露 ``builtin_state()`` 运行态函数。未注册 / 未暴露 /
        取数抛错都返回空列表——管理页按 best-effort 显示，不让运维页因 runtime 细节报错。
        这条路径保证读到的状态与 SSE 网关实际驱动的插件实例一致（都经 registry 取）。

        历史上只 cdp-bridge 暴露过这个能力，方法名叫 ``cdp_builtin_state``（漏抽象）；
        新增内置服务（device-control 等）用通用名 ``builtin_state``。两者都认，避免改老适配器。
        """
        adapter = _builtin_adapter(name)
        if adapter is None:
            return []
        fn = getattr(adapter, "builtin_state", None) or getattr(adapter, "cdp_builtin_state", None)
        if not callable(fn):
            return []
        try:
            return fn() or []
        except Exception as e:  # noqa: BLE001 — 管理端取运行态，失败降级为空
            logger.warning(f"[mcp-registry] get_builtin_state('{name}') failed: {e}")
            return []

    def list_plugins(self) -> list[LoadedPlugin]:
        return list(self._plugins.values())

    def subscribe(self, cb) -> None:
        if cb not in self._subscribers:
            self._subscribers.append(cb)

    def _notify(self, plugin_name: str, action: str) -> None:
        for cb in list(self._subscribers):
            try:
                cb(plugin_name, action)
            except Exception as e:  # 订阅者异常不能影响注册流程
                logger.warning(f"[mcp-registry] subscriber error: {e}")

    async def activate_custom(self, name: str, code: str, env: dict | None = None, *, resources: dict[str, Any] | None = None, auth_enabled: bool = False, allowed_tokens: set[str] | None = None) -> LoadedPlugin:
        """加载 custom 插件源码并原子替换旧版本。失败抛 PluginLoadError，旧版本保留。"""
        async with self._lock:
            registrar = load_plugin_source(code, name, env=env)
            ctx = PluginContext(
                plugin_name=name,
                env=env or {},
                resources=resources or {},
                auth_enabled=auth_enabled,
                allowed_tokens=set(allowed_tokens or set()),
            )
            # 用 registrar 收集到的 ToolDef 构造 LoadedPlugin；namespace 保留以防 GC
            new_plugin = LoadedPlugin(
                name=name,
                tools=list(registrar.tools),
                actions=list(registrar.actions),
                views=list(registrar.views),
                namespace={},
                ctx=ctx,
            )
            self._plugins[name] = new_plugin
            self._notify(name, "activated")
            logger.info(f"[mcp-registry] activated custom plugin '{name}' tools={new_plugin.tool_names()}")
            return new_plugin

    async def activate_stdio(self, name: str, spec: Any, *, auth_enabled: bool = False, allowed_tokens: set[str] | None = None) -> LoadedPlugin:
        """加载 stdio 插件——不在本进程拉起子进程，转发给独立执行器。

        本进程只做转发点：向执行器发现工具列表，为每个工具建一个 handler，调用时
        经 ``stdio_executor.call_tool`` 转发。stdio 子进程在执行器的隔离沙箱内运行，
        污染面（文件系统/越权）被隔离在执行器边界内（执行器本身是下一阶段实现）。

        执行器未部署/不可达时抛 PluginLoadError，调用方标记 runtime_status=error，
        绝不退化为本进程内拉起子进程。
        """
        from . import stdio_executor

        try:
            discovered = await stdio_executor.list_tools(spec)
        except stdio_executor.ExecutorUnavailable as exc:
            raise PluginLoadError(f"stdio '{name}' 执行器不可用: {exc}") from exc

        ctx = PluginContext(
            plugin_name=name,
            auth_enabled=auth_enabled,
            allowed_tokens=set(allowed_tokens or set()),
        )

        def _make_handler(tool_name: str):
            async def handler(args: dict, _ctx: PluginContext) -> Any:
                return await stdio_executor.call_tool(spec, tool_name, args or {})
            return handler

        tools: list[ToolDef] = []
        for item in discovered:
            if not isinstance(item, dict):
                continue
            tname = item.get("name") or ""
            if not tname:
                continue
            tools.append(ToolDef(
                name=tname,
                description=item.get("description") or "",
                params=item.get("input_schema") or item.get("inputSchema") or {"type": "object", "properties": {}},
                handler=_make_handler(tname),
            ))
        if not tools:
            raise PluginLoadError(f"stdio '{name}' 执行器未返回任何工具")

        async with self._lock:
            new_plugin = LoadedPlugin(name=name, tools=tools, namespace={"__stdio_spec__": spec}, ctx=ctx)
            self._plugins[name] = new_plugin
            self._notify(name, "activated")
            logger.info(f"[mcp-registry] activated stdio plugin '{name}' via executor tools={new_plugin.tool_names()}")
            return new_plugin

    async def activate_sse(
        self,
        name: str,
        url: str,
        headers: dict[str, str] | None = None,
        *,
        auth_enabled: bool = False,
        allowed_tokens: set[str] | None = None,
    ) -> LoadedPlugin:
        """Register a remote SSE MCP service in the local registry.

        The remote endpoint remains the source of execution; the registry only
        exposes its discovered tools through the same gateway contract as local
        plugins.
        """
        from .sse_client import SSEClient, SSEClientError

        client = SSEClient(url, headers)
        try:
            discovered = await client.list_tools()
        except SSEClientError as exc:
            raise PluginLoadError(f"sse '{name}' 工具发现失败: {exc}") from exc

        ctx = PluginContext(
            plugin_name=name,
            auth_enabled=auth_enabled,
            allowed_tokens=set(allowed_tokens or set()),
        )

        def _make_handler(tool_name: str):
            async def handler(args: dict, _ctx: PluginContext) -> Any:
                try:
                    return await client.call_tool(tool_name, args or {})
                except SSEClientError as exc:
                    raise RuntimeError(str(exc)) from exc
            return handler

        tools: list[ToolDef] = []
        for item in discovered:
            if not isinstance(item, dict):
                continue
            tool_name = str(item.get("name") or "").strip()
            if not tool_name:
                continue
            tools.append(ToolDef(
                name=tool_name,
                description=item.get("description") or "",
                params=item.get("inputSchema") or item.get("input_schema") or {"type": "object", "properties": {}},
                handler=_make_handler(tool_name),
            ))
        if not tools:
            raise PluginLoadError(f"sse '{name}' 未返回任何工具")

        async with self._lock:
            new_plugin = LoadedPlugin(
                name,
                tools,
                namespace={"__sse_client__": client},
                ctx=ctx,
            )
            self._plugins[name] = new_plugin
            self._notify(name, "activated")
            logger.info(f"[mcp-registry] activated sse plugin '{name}' tools={new_plugin.tool_names()}")
            return new_plugin

    async def activate_builtin(self, name: str, env: dict | None = None, *, resources: dict[str, Any] | None = None, enabled: bool = True, auth_enabled: bool = False, allowed_tokens: set[str] | None = None) -> LoadedPlugin:
        """加载内置插件（vendored in-tree）。失败抛 PluginLoadError，旧版本保留。
        与 activate_custom 不同：不走 exec，直接 import adapter 的 register(reg)。
        env / 鉴权快照通过 PluginRegistrar 透传给 adapter.register；CDP
        客户端身份仅由 runtime materialized client hashes 配置。
        内置服务由 register_builtins() 或 register_internal_builtins() 在 boot 时注册一次；
        配置变更走 update_builtin_config()（原地下发，不重建）。本方法仅作首次注册的实现体。
        """
        adapter = _builtin_adapter(name)
        if adapter is None:
            raise PluginLoadError(f"no built-in adapter registered for service '{name}'")
        async with self._lock:
            registrar = PluginRegistrar(
                name,
                env=env or {},
                resources=resources or {},
                auth_enabled=auth_enabled,
                allowed_tokens=set(allowed_tokens or set()),
            )
            try:
                adapter.register(registrar)
            except Exception as e:
                raise PluginLoadError(f"builtin '{name}' register() failed: {type(e).__name__}: {e}") from e
            if not registrar.tools and not registrar.actions and not registrar.views:
                raise PluginLoadError(f"builtin '{name}' did not register any capability")
            ctx = PluginContext(
                plugin_name=name,
                env=env or {},
                resources=resources or {},
                enabled=enabled,
                auth_enabled=auth_enabled,
                allowed_tokens=set(allowed_tokens or set()),
            )
            new_plugin = LoadedPlugin(
                name=name,
                tools=list(registrar.tools),
                actions=list(registrar.actions),
                views=list(registrar.views),
                namespace={"__adapter__": adapter},
                ctx=ctx,
            )
            self._plugins[name] = new_plugin
            self._notify(name, "activated")
            logger.info(f"[mcp-registry] activated builtin plugin '{name}' tools={new_plugin.tool_names()}")
            return new_plugin

    async def register_internal_builtins(self) -> dict[str, LoadedPlugin]:
        """Register runtime-owned session services without persisted config."""
        out: dict[str, LoadedPlugin] = {}
        for name in _INTERNAL_BUILTIN_ADAPTERS:
            try:
                out[name] = await self.activate_builtin(
                    name,
                    enabled=True,
                    auth_enabled=True,
                )
            except Exception as e:
                logger.error(f"[mcp-registry] register internal builtin '{name}' failed: {e}")
        return out

    @staticmethod
    def is_internal_builtin(name: str) -> bool:
        return name in _INTERNAL_BUILTIN_ADAPTERS

    async def register_builtins(
        self, config_by_name: dict[str, dict] | None = None, *, configured_only: bool = False,
    ) -> dict[str, LoadedPlugin]:
        """在 runtime 启动时把所有内置服务注册进注册表（一体、常驻）。

        内置是 runtime 的组成部分，天生就在——不看 DB enabled，不走启停。这里对每个
        内置 adapter 调一次 activate_builtin 建立常驻 LoadedPlugin。config_by_name 可
        为每个服务传入首次 env/auth/resources 配置；缺省用空配置注册。

        单个内置注册失败只记日志、不阻断其它内置与整个 runtime 启动。
        """
        config_by_name = config_by_name or {}
        out: dict[str, LoadedPlugin] = {}
        for name in _BUILTIN_ADAPTERS:
            if configured_only and name not in config_by_name:
                logger.error(f"[mcp-registry] skip builtin '{name}': no persisted configuration snapshot")
                continue
            cfg = config_by_name.get(name) or {}
            try:
                out[name] = await self.activate_builtin(
                    name,
                    env=cfg.get("env"),
                    resources=cfg.get("resources"),
                    enabled=bool(cfg.get("enabled", True)),
                    auth_enabled=bool(cfg.get("auth_enabled", False)),
                    allowed_tokens=cfg.get("allowed_tokens"),
                )
            except Exception as e:
                logger.error(f"[mcp-registry] register builtin '{name}' failed: {e}")
        return out

    async def update_builtin_config(
        self, name: str, *,
        env: dict | None = None,
        resources: dict[str, Any] | None = None,
        enabled: bool = True,
        auth_enabled: bool = False,
        allowed_tokens: set[str] | None = None,
    ) -> LoadedPlugin:
        """给常驻的内置服务下发新配置（鉴权 token 与统一 resources）。

        原地更新 PluginContext（SSE gateway 每请求读它做鉴权），并让 adapter 把
        materialized 配置推给活着的 driver。不重建工具、不替换 LoadedPlugin，

        若该内置尚未注册（异常路径），退化为首次注册。

        内部会话服务（issue-workflow）没有可配置面：授权按请求 identity token 判定，
        不接受管理端/DB 下发的 enabled / token 快照，这里直接拒绝。
        """
        if name in _INTERNAL_BUILTIN_ADAPTERS:
            raise PluginLoadError(f"internal builtin '{name}' has no configurable snapshot")
        plugin = self._plugins.get(name)
        if plugin is None:
            return await self.activate_builtin(
                name, env=env, resources=resources, enabled=enabled,
                auth_enabled=auth_enabled, allowed_tokens=allowed_tokens,
            )
        adapter = _BUILTIN_ADAPTERS.get(name)
        if adapter is None:
            raise PluginLoadError(f"no built-in adapter registered for service '{name}'")
        async with self._lock:
            if resources is not None:
                plugin.ctx.resources = dict(resources)
            plugin.ctx.enabled = enabled
            plugin.ctx.auth_enabled = auth_enabled
            plugin.ctx.allowed_tokens = set(allowed_tokens or set())
            # 让 adapter 把新配置推给活着的 driver（configure_driver 走热更新分支）
            apply_cfg = getattr(adapter, "apply_config", None)
            if callable(apply_cfg):
                try:
                    apply_cfg(plugin.ctx)
                except Exception as e:
                    raise PluginLoadError(f"builtin '{name}' apply_config failed: {type(e).__name__}: {e}") from e
            logger.info(f"[mcp-registry] updated builtin config '{name}' auth={auth_enabled} tokens={len(plugin.ctx.allowed_tokens)}")
            return plugin

    async def deactivate(self, name: str) -> bool:
        async with self._lock:
            removed = self._plugins.pop(name, None)
            if removed:
                self._notify(name, "deactivated")
                logger.info(f"[mcp-registry] deactivated plugin '{name}'")
            return removed is not None

    async def call_tool(self, plugin_name: str, tool_name: str, arguments: dict) -> Any:
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            raise KeyError(f"plugin not loaded: {plugin_name}")
        if not plugin.ctx.enabled:
            raise ValueError(f"plugin disabled: {plugin_name}")
        tool = plugin.find(tool_name)
        if not tool:
            raise KeyError(f"unknown tool: {plugin_name}/{tool_name}")
        return await tool.handler(arguments or {}, plugin.ctx)

    async def call_action(self, plugin_name: str, action_name: str, arguments: dict) -> Any:
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            raise KeyError(f"plugin not loaded: {plugin_name}")
        if not plugin.ctx.enabled:
            raise ValueError(f"plugin disabled: {plugin_name}")
        action = next((item for item in plugin.actions if item.name == action_name), None)
        if not action:
            raise KeyError(f"unknown action: {plugin_name}/{action_name}")
        return await action.handler(arguments or {}, plugin.ctx)

    async def query_view(self, plugin_name: str, view_name: str, arguments: dict | None = None) -> Any:
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            raise KeyError(f"plugin not loaded: {plugin_name}")
        if not plugin.ctx.enabled:
            raise ValueError(f"plugin disabled: {plugin_name}")
        view = next((item for item in plugin.views if item.name == view_name), None)
        if not view:
            raise KeyError(f"unknown view: {plugin_name}/{view_name}")
        return await view.handler(arguments or {}, plugin.ctx)
    def all_tool_schemas(self) -> list[dict]:
        """聚合所有已加载插件的工具 schema（MCP tools/list 返回）。"""
        schemas: list[dict] = []
        for plugin in self._plugins.values():
            for t in plugin.tools:
                schemas.append({
                    "name": t.name,
                    "description": t.description or f"MCP tool {t.name}",
                    "inputSchema": t.params or {"type": "object", "properties": {}},
                })
        return schemas


# 进程内单例
registry = MCPRegistry()


__all__ = ["MCPRegistry", "LoadedPlugin", "registry", "PluginLoadError"]
