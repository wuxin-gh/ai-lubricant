"""Plugin loader — 在 Runtime 进程内加载 custom 类插件的 Python 源码。

插件契约：源码必须暴露 register(reg) 函数，reg.tool(...) 装饰器登记工具。

    def register(reg):
        @reg.tool(name="echo", description="回声",
                  params={"type":"object","properties":{"text":{"type":"string"}},"required":["text"]})
        async def echo(args, ctx):
            return {"text": args["text"]}

工具函数签名统一 async def fn(args: dict, ctx: PluginContext) -> Any。
ctx 注入：env（环境变量）、log（loguru logger）、http（共享 aiohttp session 工厂）。

加载在独立命名空间内 exec，加载失败抛 PluginLoadError，调用方负责回滚。
这是执行任意代码——安全靠「版本必须通过 AI 安全审查才能 activate」这层闸门保证，
Runtime 进程内不做静态沙箱（Python 无法真正沙箱）。
"""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from loguru import logger


class PluginLoadError(Exception):
    """插件加载失败（编译/契约/执行）。"""


# SSE Gateway 每请求注入的 token，供插件 handler 读取。
# 对 cdp-bridge，cdp_bridge_plugin 的 _wrap 会把它桥接到 cdp 的 current_token。
current_request_token: ContextVar[str] = ContextVar("current_request_token", default="")
# Optional logical holder/session for long-lived CDP tab leases. Empty holder
# means a per-call lease; empty session falls back to the holder's selected tab.
current_cdp_holder: ContextVar[str] = ContextVar("current_cdp_holder", default="")
current_cdp_session_id: ContextVar[str] = ContextVar("current_cdp_session_id", default="")


@dataclass
class ToolDef:
    name: str
    description: str
    params: dict
    handler: Callable[[dict, "PluginContext"], Awaitable[Any]]


@dataclass
class ActionDef:
    name: str
    description: str
    params: dict
    handler: Callable[[dict, "PluginContext"], Awaitable[Any]]


@dataclass
class ViewDef:
    name: str
    description: str
    params: dict
    handler: Callable[[dict, "PluginContext"], Awaitable[Any]]


@dataclass
class PluginContext:
    """注入给工具函数的运行时上下文。"""
    plugin_name: str
    env: dict = field(default_factory=dict)
    # Schema-v2 resource bindings supplied by the runtime.
    resources: dict[str, Any] = field(default_factory=dict)
    log = logger  # loguru logger，类属性即可
    # Logical availability is separate from registry residency for built-ins.
    enabled: bool = True
    auth_enabled: bool = False
    allowed_tokens: set[str] = field(default_factory=set)

    def http_session(self):
        """共享的 aiohttp session 工厂，避免每个插件各自管理连接池。"""
        import aiohttp
        from providers.base import make_insecure_connector
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120),
            connector=make_insecure_connector(),
        )


class PluginRegistrar:
    """register(reg) 执行期间收集工具定义。"""

    def __init__(self, plugin_name: str, env: dict | None = None, *, resources: dict[str, Any] | None = None, auth_enabled: bool = False, allowed_tokens: set[str] | None = None):
        self.plugin_name = plugin_name
        self.env = env or {}
        self.resources = resources or {}
        self.auth_enabled = auth_enabled
        self.allowed_tokens = set(allowed_tokens or set())
        self.tools: list[ToolDef] = []
        self.actions: list[ActionDef] = []
        self.views: list[ViewDef] = []
        # Names are unique within a capability kind; the same public name may
        # intentionally be exposed as a tool, action, and/or view.
        self._names: dict[str, set[str]] = {"tool": set(), "action": set(), "view": set()}

    def _register(self, kind: str, definition_type, name: str, description: str, params: dict | None):
        if not name or not isinstance(name, str):
            raise PluginLoadError(f"invalid {kind} name: {name!r}")
        names = self._names[kind]
        if name in names:
            raise PluginLoadError(f"duplicate {kind} name in plugin: {name}")
        names.add(name)
        if params is None:
            params = {"type": "object", "properties": {}}
        if not isinstance(params, dict):
            raise PluginLoadError(f"{kind} {name} params must be a JSON Schema dict")

        def deco(fn: Callable[[dict, PluginContext], Awaitable[Any]]):
            if not callable(fn):
                raise PluginLoadError(f"{kind} {name} handler is not callable")
            getattr(self, f"{kind}s").append(
                definition_type(name=name, description=description, params=params, handler=fn)
            )
            return fn

        return deco

    def tool(self, name: str, *, description: str = "", params: dict | None = None):
        return self._register("tool", ToolDef, name, description, params)

    def action(self, name: str, *, description: str = "", params: dict | None = None):
        return self._register("action", ActionDef, name, description, params)

    def view(self, name: str, *, description: str = "", params: dict | None = None):
        return self._register("view", ViewDef, name, description, params)


def load_plugin_source(code: str, plugin_name: str, env: dict | None = None) -> PluginRegistrar:
    """编译并执行插件源码，返回收集到的工具定义。

    失败时抛 PluginLoadError，调用方据此回滚（不替换已加载的旧版本）。
    """
    if not code or not code.strip():
        raise PluginLoadError("empty plugin source")

    try:
        compiled = compile(code, f"<mcp_plugin:{plugin_name}>", "exec")
    except SyntaxError as e:
        raise PluginLoadError(f"compile failed: {e}") from e

    ns: dict = {"__name__": f"mcp_plugin_{plugin_name}"}
    try:
        exec(compiled, ns)  # noqa: S102 — 受信任来源（AI 审查通过 + 管理员激活）
    except Exception as e:
        raise PluginLoadError(f"exec failed: {type(e).__name__}: {e}") from e

    register_fn = ns.get("register")
    if not callable(register_fn):
        raise PluginLoadError("plugin source must define register(reg)")

    registrar = PluginRegistrar(plugin_name, env=env)
    try:
        register_fn(registrar)
    except Exception as e:
        raise PluginLoadError(f"register() failed: {type(e).__name__}: {e}") from e

    if not registrar.tools and not registrar.actions and not registrar.views:
        raise PluginLoadError("register() did not register any capability")

    return registrar
