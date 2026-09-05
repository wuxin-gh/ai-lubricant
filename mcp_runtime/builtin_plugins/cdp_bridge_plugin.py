"""CDP Bridge built-in adapter.

Bridges the vendored ``mcp_builtin.cdp_bridge`` capability into the MCP
runtime registry. The upstream package is a FastMCP server that talks to a
real Chrome session through a companion extension over WebSocket. Instead of
spawning it as a stdio subprocess, we import its tool functions directly and
re-register them with the runtime's ``PluginRegistrar`` so they are callable
through the SSE gateway like any other loaded plugin.

The Chrome-extension WebSocket endpoint is owned by ``mcp_runtime`` and uses
mandatory auth protocol 2. Activating this adapter only creates the in-process
driver state; it never starts standalone HTTP/WebSocket listeners.
"""
from __future__ import annotations

import json
import os
import uuid
from typing import Any, Awaitable, Callable

from mcp_runtime.plugin_loader import PluginContext, PluginLoadError, PluginRegistrar

# 缓存一次 vendored cdp-bridge 的函数表；可选依赖缺失时置错误原因，register/apply 时抛出。
_CDP_TOOLS: dict[str, Callable] | None = None
_CDP_IMPORT_ERROR: str | None = None


def _load_cdp_bridge_tools() -> dict[str, Callable]:
    """Import vendored cdp-bridge once and cache it.

    The registry imports this adapter at module load even if optional browser
    bridge dependencies are missing (heavy deps only surface here). On missing
    deps we raise PluginLoadError so the UI shows a clear reason.
    """
    global _CDP_TOOLS, _CDP_IMPORT_ERROR
    if _CDP_TOOLS is not None:
        return _CDP_TOOLS
    if _CDP_IMPORT_ERROR is not None:
        raise PluginLoadError(_CDP_IMPORT_ERROR)
    try:
        from mcp_builtin.cdp_bridge.server import (
            apply_config,
            browser_batch,
            browser_execute_js,
            browser_focus_tab,
            browser_get_tabs,
            browser_navigate,
            browser_network_get,
            browser_network_start,
            browser_network_stop,
            browser_open_tab,
            browser_scan,
            browser_screenshot,
            browser_switch_tab,
            browser_wait,
            init_driver,
        )
    except ModuleNotFoundError as exc:
        missing = exc.name or str(exc)
        _CDP_IMPORT_ERROR = (
            f"cdp-bridge 运行依赖缺失: {missing}. "
            "请安装 simple-websocket-server、bottle、beautifulsoup4 后再启动。"
        )
        raise PluginLoadError(_CDP_IMPORT_ERROR) from exc
    _CDP_TOOLS = {
        "browser_get_tabs": browser_get_tabs,
        "browser_scan": browser_scan,
        "browser_execute_js": browser_execute_js,
        "browser_switch_tab": browser_switch_tab,
        "browser_focus_tab": browser_focus_tab,
        "browser_open_tab": browser_open_tab,
        "browser_batch": browser_batch,
        "browser_wait": browser_wait,
        "browser_navigate": browser_navigate,
        "browser_screenshot": browser_screenshot,
        "browser_network_start": browser_network_start,
        "browser_network_get": browser_network_get,
        "browser_network_stop": browser_network_stop,
        "init_driver": init_driver,
        "apply_config": apply_config,
    }
    return _CDP_TOOLS


def _driver_config(resources: dict | None, auth_enabled: bool, allowed_tokens) -> dict:
    """Translate materialized CDP client hashes into live driver configuration.

    会话池按 client_id（instance_key）隔离，认证/绑定只需 token_hash + client_id；
    「可操作用户」(user_ids) 不再决定会话归属，仅用于 token→client 路由与服务级
    连接鉴权，因此这里不要求 user_id。保留 user_ids 供配置检视。
    """
    resources = resources or {}
    clients = []
    for item in resources.get("clients") or []:
        if not item.get("enabled", True) or not item.get("token_hash"):
            continue
        if not (item.get("instance_key") or item.get("id")):
            continue
        clients.append(dict(item))
    return {"clients": clients}


async def _principal_cdp_client_id(token: str) -> str | None:
    """把 token 解析到其 principal 绑定的 CDP 客户端 id（读 principal 的 cdp_client_id param）。

    principal kind 直取；identity kind（agent）经 agent_id → principal 再取。
    没绑 cdp_client_id param 返回 None（交给后续外部 token 路径）。
    """
    import builtin_tool_store
    import mcp_plugin_store

    resolved = await builtin_tool_store.resolve_token(token)
    if not resolved:
        return None
    principal_id: int | None = None
    if resolved.get("kind") == "principal":
        principal_id = int((resolved.get("target") or {}).get("id"))
    elif resolved.get("kind") == "identity":
        meta = resolved.get("token") or {}
        target_id = meta.get("target_id")
        if meta.get("target_type") == "agent" and target_id and str(target_id).isdigit():
            principal_id = await mcp_plugin_store.get_agent_mcp_principal_id(int(target_id))
    if principal_id is None:
        return None
    value = await mcp_plugin_store.get_principal_param(principal_id, "cdp_client_id")
    return str(value) if value is not None else None


async def _resolve_cdp_client_id(token: str, driver, requested_client_id: Any = None) -> str:
    """把每请求 token 解析成要驱动的 CDP 客户端 client_id（会话池隔离键）。

    支持两类 token（见 [[project-external-mcp-token-instance-binding]]）：
    - CDP 客户端连接 token：DB 只存 token_hash，driver.authenticate_client(明文)
      现场 hash 查表得到 client_id（与扩展 WS 握手同一路径）。
    - 外部 MCP 访问 token（builtin_tool_tokens 行，hash 不在 driver 索引里）：
      principal 带 cdp_client_id param 就直接用它；否则经
      builtin_tool_store.resolve_external_cdp_client 复核 external+cdp+已启用客户端。

    两条都拿不到就报错，区分「外部 token 未绑客户端」与「连接 token 无效」。
    """
    config = driver.authenticate_client(token) if driver is not None else None
    client_id = config.get("client_id") if config else None
    if client_id is not None:
        return str(client_id)
    # 不是 CDP 连接 token：尝试当外部 MCP 访问 token 解析其绑定的客户端。
    try:
        bound = await _principal_cdp_client_id(token)
        if bound is not None:
            # principal 直接绑了 cdp_client_id param，用它定位资源。
            requested = str(requested_client_id or "").strip()
            if requested and requested != bound:
                raise ValueError(f"CDP client {requested} is not authorized for this MCP principal")
            return bound
        client_id = await builtin_tool_store.resolve_external_cdp_client(token)
    except ValueError:
        raise
    except Exception:
        client_id = None
    if client_id is None:
        raise ValueError("MCP token is not authorized to operate any CDP client")
    return str(client_id)


def _wrap(fn: Callable[..., Awaitable[str]]) -> Callable[[dict, PluginContext], Awaitable[Any]]:
    """Adapt a cdp-bridge tool function to the runtime's (args, ctx) signature.

    cdp-bridge tool functions are ``async def fn(**kwargs) -> str`` returning a
    JSON string. The runtime calls handlers as ``fn(args: dict, ctx)``. We
    forward matching kwargs and parse the JSON string into a dict so the SSE
    gateway can shape it as MCP content.

    此外把 runtime 通用 current_request_token 桥接到 cdp 的 current_token：
    用 driver 自带的 authenticate_client 把请求 token 解析成它对应的 CDP 客户端
    client_id（纯内存 hash 查找，与浏览器扩展 WS 握手走同一条解析路径），再以
    client_id 作为浏览器会话池的隔离键（一个 CDP 实例=一个浏览器，会话按 client 隔离）。
    token 明文永不落库，鉴权只比对 token_hash。

    holder 透传：网页对话/Agent MCP 调用会把占用者标识写进
    ``current_cdp_holder``（进程内 ContextVar，同 task 自然透传）。本桥把它设到
    cdp 的 ``current_holder``，``execute_js`` 据此做 tab 租约；租约冲突抛
    ``TabBusyError``，这里转成结构化 busy 结果返回给上层，不让工具调用异常打断
    agent loop。无 holder（外部 MCP 直连）时，``execute_js`` 自建单次 rpc 租约。
    """
    from mcp_builtin.cdp_bridge import server as cdp_server
    from mcp_builtin.cdp_bridge.server import current_token as cdp_current_token
    from mcp_builtin.cdp_bridge.TMWebDriver import current_holder as cdp_current_holder
    from mcp_builtin.cdp_bridge.TMWebDriver import current_session_id as cdp_current_session_id
    from mcp_builtin.cdp_bridge.TMWebDriver import TabBusyError
    from mcp_runtime.plugin_loader import current_request_token, current_cdp_holder, current_cdp_session_id

    async def handler(args: dict, ctx: PluginContext) -> Any:
        token = current_request_token.get("")
        driver = cdp_server.driver
        client_id = await _resolve_cdp_client_id(token, driver, (args or {}).get("client_id"))
        requested_session = str(
            (args or {}).get("session_id")
            or (args or {}).get("switch_tab_id")
            or (args or {}).get("tab_id")
            or current_cdp_session_id.get("")
            or ""
        )
        if requested_session and ":" not in requested_session:
            requested_session = f"{client_id}:{requested_session}"
        tok = cdp_current_token.set(str(client_id))
        holder_tok = cdp_current_holder.set(current_cdp_holder.get(""))
        sess_tok = cdp_current_session_id.set(requested_session)
        try:
            kwargs = {k: v for k, v in (args or {}).items() if k in fn.__code__.co_varnames}
            result = await fn(**kwargs)
        except TabBusyError as exc:
            return _busy_payload(exc)
        finally:
            cdp_current_session_id.reset(sess_tok)
            cdp_current_holder.reset(holder_tok)
            cdp_current_token.reset(tok)
        if isinstance(result, str):
            try:
                return json.loads(result)
            except Exception:
                return {"text": result}
        return result

    return handler


def _busy_payload(exc) -> dict:
    lease = exc.lease
    return {
        "status": "busy",
        "session_key": lease.session_key,
        "holder": lease.holder,
        "expires_at": lease.expires_at,
        "msg": "Tab is occupied by another conversation or task; use another tab or retry later.",
    }


# Tool name -> (function key, description, JSON Schema params). Function objects are
# resolved lazily in register() so importing this adapter does not require the
# optional browser bridge dependencies.
_TOOLS: list[tuple[str, str, str, dict]] = [
    (
        "browser_get_tabs",
        "browser_get_tabs",
        "List all connected browser tabs with their IDs, URLs, and titles.",
        {"type": "object", "properties": {}},
    ),
    (
        "browser_scan",
        "browser_scan",
        "Return optimized HTML/text of the active browser tab plus the tab list.",
        {
            "type": "object",
            "properties": {
                "tabs_only": {"type": "boolean", "default": False},
                "switch_tab_id": {"type": "string", "default": ""},
                "text_only": {"type": "boolean", "default": False},
            },
        },
    ),
    (
        "browser_execute_js",
        "browser_execute_js",
        "Execute JavaScript in the browser and capture results plus DOM changes.",
        {
            "type": "object",
            "properties": {
                "script": {"type": "string"},
                "switch_tab_id": {"type": "string", "default": ""},
                "no_monitor": {"type": "boolean", "default": False},
            },
            "required": ["script"],
        },
    ),
    (
        "browser_switch_tab",
        "browser_switch_tab",
        "Switch the active MCP browser tab without changing the visible Chrome tab.",
        {"type": "object", "properties": {"tab_id": {"type": "string"}}, "required": ["tab_id"]},
    ),
    (
        "browser_focus_tab",
        "browser_focus_tab",
        "Bring a Chrome tab to the foreground: activate the tab and focus its window.",
        {"type": "object", "properties": {"tab_id": {"type": "string"}}, "required": ["tab_id"]},
    ),
    (
        "browser_open_tab",
        "browser_open_tab",
        "Open a new tab, or a new browser window, at a URL. The new tab needs a moment "
        "to register itself, so its ID may not appear in browser_get_tabs immediately.",
        {
            "type": "object",
            "properties": {
                "url": {"type": "string", "default": ""},
                "new_window": {"type": "boolean", "default": False},
                "active": {"type": "boolean", "default": True},
            },
        },
    ),
    (
        "browser_batch",
        "browser_batch",
        "Run multiple extension/CDP commands in one request.",
        {
            "type": "object",
            "properties": {
                "commands": {"type": "array", "items": {"type": "object"}},
                "tab_id": {"type": "string", "default": ""},
                "timeout": {"type": "number", "default": 20},
            },
            "required": ["commands"],
        },
    ),
    (
        "browser_wait",
        "browser_wait",
        "Wait until a JavaScript condition returns a truthy value.",
        {
            "type": "object",
            "properties": {
                "condition_js": {"type": "string"},
                "timeout": {"type": "number", "default": 10},
                "interval": {"type": "number", "default": 0.5},
                "switch_tab_id": {"type": "string", "default": ""},
            },
            "required": ["condition_js"],
        },
    ),
    (
        "browser_navigate",
        "browser_navigate",
        "Navigate the active tab to a URL.",
        {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]},
    ),
    (
        "browser_screenshot",
        "browser_screenshot",
        "Take a screenshot of the active tab. The platform Agent runtime intercepts "
        "the result and spills the PNG bytes to a workspace file before the model "
        "ever sees them, so the returned `path` is the only way to reference the "
        "image — never read or inline `base64`. Pass `path` to `ask_user` "
        "attachments to show the user.",
        {
            "type": "object",
            "properties": {
                "tab_id": {"type": "string", "default": ""},
                "path": {
                    "type": "string",
                    "description": (
                        "Destination workspace path for the PNG; defaults to "
                        "workspace/screenshots/shot_<timestamp>.png. The returned "
                        "result carries this path — the bytes are never inlined."
                    ),
                },
            },
        },
    ),
    (
        "browser_network_start",
        "browser_network_start",
        "Start capturing network requests on a tab. Attaches the debugger and keeps "
        "it attached, buffering every request (URL, method, headers, status, timing, "
        "response body). A 'capturing' banner is shown at the top of the page until "
        "stopped. Use browser_network_get to read captured requests and "
        "browser_network_stop to finish.",
        {
            "type": "object",
            "properties": {
                "tab_id": {"type": "string", "default": ""},
                "url_pattern": {"type": "string", "default": ""},
            },
        },
    ),
    (
        "browser_network_get",
        "browser_network_get",
        "Return the network requests captured so far on a tab without stopping the "
        "capture. Each request includes URL, method, request/response headers, status, "
        "MIME type, timing, and response body (truncated to 1MB).",
        {"type": "object", "properties": {"tab_id": {"type": "string", "default": ""}}},
    ),
    (
        "browser_network_stop",
        "browser_network_stop",
        "Stop capturing network requests on a tab, detach the debugger, hide the "
        "banner, and return all captured requests.",
        {"type": "object", "properties": {"tab_id": {"type": "string", "default": ""}}},
    ),
]


async def _sessions_view(_args: dict, ctx: PluginContext) -> dict:
    from mcp_builtin.cdp_bridge import server as cdp_server
    driver = cdp_server.driver
    service_id = ctx.resources.get("service_id")
    if driver is None:
        return {"service_id": service_id, "contexts": [], "note": "driver 未初始化"}

    contexts = driver.snapshot_contexts()
    return {"service_id": service_id, "contexts": contexts}


def cdp_builtin_state() -> list[dict]:
    """返回 CDP 实时会话快照（供管理端经 registry 取数，不直读 driver 单例）。

    driver 未初始化或缺依赖时返回空列表。这是 cdp-bridge 对外暴露的**唯一**运行态
    读取入口——管理端 /api_builtin_tools 不再 ``from mcp_builtin.cdp_bridge import server``
    直抓全局 driver，改走 registry.get_builtin_state("cdp-bridge")，保证读到的永远是
    registry 当前持有的那个插件实例的状态（与 SSE 网关实际驱动的 driver 一致）。
    """
    try:
        from mcp_builtin.cdp_bridge import server as cdp_server
    except ModuleNotFoundError:
        return []
    driver = cdp_server.driver
    if driver is None:
        return []
    return driver.snapshot_contexts()


def register(reg: PluginRegistrar) -> None:
    """Register all cdp-bridge tools with the runtime registrar.

    Builtin cdp-bridge is part of the runtime: register() creates the process
    driver once and registers static tool definitions. Later auth/grant changes
    use apply_config(ctx) and do not rebuild tools or restart connections.
    """
    tools = _load_cdp_bridge_tools()
    cfg = _driver_config(reg.resources, reg.auth_enabled, reg.allowed_tokens)
    tools["init_driver"](
        clients=cfg["clients"],
        external_ws=True,
    )

    for name, fn_key, description, params in _TOOLS:
        props = params.setdefault("properties", {})
        props["client_id"] = {
            "type": "string",
            "description": "Browser client ID. Required when this MCP principal can access multiple clients.",
        }
        reg.tool(name=name, description=description, params=params)(_wrap(tools[fn_key]))
    reg.view(name="sessions", description="Runtime browser session snapshot")(_sessions_view)


def apply_config(ctx: PluginContext) -> None:
    """Push updated auth/session config into the live cdp-bridge driver."""
    tools = _load_cdp_bridge_tools()
    cfg = _driver_config(ctx.resources, ctx.auth_enabled, ctx.allowed_tokens) if ctx.enabled else {"clients": []}
    tools["apply_config"](clients=cfg["clients"])


__all__ = ["register", "apply_config", "cdp_builtin_state"]
