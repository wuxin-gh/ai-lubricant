"""device-control built-in adapter.

把 vendored ``mcp_builtin.device_control`` 能力桥进 MCP runtime registry。设备
（Android App）经配对码接入、用长期 token 拨 ``/mcp/device-control/ws/device``；
本 adapter 不起独立监听器——WS 端点由 ``mcp_runtime/sse_gateway.device_ws`` 托管。
这里只建进程内 driver 单例 + 注册 16 个 MCP 工具（spec §8 命令表）。

双 token 与 cdp-bridge 同构（见 [[project-builtin-tools-two-token-kinds]]）：
- 设备连接 token：存 device 明细行（token_hash），WS 握手时 authenticate_device 校验。
- MCP 访问 token：principal 带 ``device_id`` param 指向某台设备，或 external token 绑
  具体 device 明细。adapter 的 ``_resolve_device_id`` 把请求 token 解析成协议 device_id，
  交 driver 路由。两种 token 不能混。
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable

from mcp_runtime.plugin_loader import PluginContext, PluginLoadError, PluginRegistrar

# 缓存一次 device_control driver 的导入；可选依赖缺失时置错误原因，register/apply 时抛。
_DC_DRIVER: Any | None = None
_DC_IMPORT_ERROR: str | None = None


def _load_dc() -> Any:
    """Import vendored device_control driver once and cache it.

    registry 在模块加载时就急切 import 本 adapter（见 registry._BUILTIN_ADAPTERS），
    故顶层不能 import 重依赖——device_control 无重依赖，但仍照 cdp-bridge 的形态把真实
    import 推迟进这里，失败原因缓存成 PluginLoadError，UI 能给出清晰提示。
    """
    global _DC_DRIVER, _DC_IMPORT_ERROR
    if _DC_DRIVER is not None:
        return _DC_DRIVER
    if _DC_IMPORT_ERROR is not None:
        raise PluginLoadError(_DC_IMPORT_ERROR)
    try:
        from mcp_builtin.device_control import driver as dc_driver
    except ModuleNotFoundError as exc:
        _DC_IMPORT_ERROR = f"device-control 运行依赖缺失: {exc.name or exc}"
        raise PluginLoadError(_DC_IMPORT_ERROR) from exc
    dc_driver.init_driver()
    _DC_DRIVER = dc_driver
    return dc_driver


async def _principal_device_id(token: str) -> str | None:
    """把 token 解析到其 principal 绑定的 device 明细 id（读 principal 的 device_id param）。

    principal kind 直取；identity kind（agent）经 agent_id → principal 再取。
    没绑 device_id param 返回 None（交给后续外部 token 路径）。
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
    value = await mcp_plugin_store.get_principal_param(principal_id, "device_id")
    return str(value) if value is not None else None


async def _resolve_device_id(token: str, requested_device_id: Any = None) -> str:
    """把每请求 token 解析成要驱动的协议 device_id。

    与 cdp-bridge 的 ``_resolve_cdp_client_id`` 同构，两路：
    - 外部 MCP 访问 token（builtin_tool_tokens 行，绑 device 实例 + 具体 device 明细）：
      ``resolve_external_device_id`` 复核该设备属于本实例、已启用、有 token，返回其
      device_id。
    - principal（或 agent identity 经 principal）带 ``device_id`` param：param 值是
      device 明细 id（grants 物化的口径），这里再查 detail 取出协议 device_id。
      requested_device_id（工具调用方显式给的）必须与此一致，否则拒。

    两路都拿不到就报错，区分「未绑设备」与「连接 token 无效」。
    """
    import builtin_tool_store

    # principal 路径：param 值是 device 资源 id。
    bound_resource_id = await _principal_device_id(token)
    if bound_resource_id is not None:
        requested = str(requested_device_id or "").strip()
        if requested and requested != bound_resource_id:
            raise ValueError(f"device {requested} is not authorized for this MCP principal")
        resource = await builtin_tool_store.get_resource(int(bound_resource_id))
        if resource is None or resource.get("resource_type") != "device":
            raise ValueError("authorized device resource not found")
        if not resource.get("enabled", True) or not resource.get("token_hash"):
            raise ValueError("authorized device is disabled or unpaired")
        device_id = str(resource.get("device_id") or "")
        if not device_id:
            raise ValueError("authorized device has no device_id")
        return device_id

    # 外部 MCP token 路径：绑 device 明细 id，resolve_external_device_id 直返 device_id。
    # requested（工具调用方显式给的）必须与 token 绑定的设备一致，否则拒——
    # 一个 external token 只授权一台设备，不能借它驱动别的。
    requested = str(requested_device_id or "").strip()
    try:
        device_id = await builtin_tool_store.resolve_external_device_id(token)
    except ValueError:
        raise
    except Exception:
        device_id = None
    if not device_id:
        raise ValueError("MCP token is not authorized to operate any device")
    if requested and requested != device_id:
        raise ValueError(f"device {requested} is not authorized for this token")
    return str(device_id)


def _wrap(cmd: str) -> Callable[[dict, PluginContext], Awaitable[Any]]:
    """把一条 device-control 命令包成 runtime 工具 handler。

    解析请求 token → device_id（双 token 见上），调 driver.call 下发并等 call-response。
    返回的 data 原样透传（get_screen_state 的 tree/screenshot、list_apps 的 apps 等）。
    设备端报错（DeviceError）原样上抛，runtime 网关回成 JSON-RPC error。
    """
    from mcp_runtime.plugin_loader import current_request_token
    from mcp_builtin.device_control import driver as dc_driver

    async def handler(args: dict, ctx: PluginContext) -> Any:
        token = current_request_token.get("")
        driver = dc_driver.get_driver()
        if driver is None:
            raise RuntimeError("device-control driver not loaded")
        args = args or {}
        # device_id 由 token 解析；调用方不必也不能自己挑设备（防越权）。
        device_id = await _resolve_device_id(token, args.get("device_id"))
        timeout_ms = args.get("timeout_ms")
        # 剥掉路由用的元字段，剩下的作为命令 args 下发设备。
        cmd_args = {k: v for k, v in args.items() if k not in ("device_id", "timeout_ms")}
        result = await driver.call(device_id, cmd, cmd_args, timeout_ms)
        return result

    return handler


# 工具定义：spec §8 命令表逐条转成 (name, description, JSON Schema)。
# 描述里说明 args 形态；device_id / timeout_ms 由 adapter 统一注入，不进设备 args。
def _tools() -> list[tuple[str, str, dict]]:
    from mcp_builtin.device_control import protocol as proto
    press_keys = list(proto.PRESS_KEYS)
    # node_id 与坐标二选一的命令共用这个 schema 片段（spec §8.4：恰好一个）。
    target_props = {
        "node_id": {
            "type": "string",
            "description": "无障碍节点 id（与 x/y 二选一；优先于坐标，spec §8.4）。",
        },
        "x": {"type": "integer", "description": "物理像素横坐标（与 node_id 二选一）."},
        "y": {"type": "integer", "description": "物理像素纵坐标（与 node_id 二选一）."},
    }
    return [
        (
            "get_screen_state",
            "读取当前屏幕：UI 树（TSV）、屏幕元信息、截图（可选）。返回 {screen, tree, screenshot?, next_cursor?}。",
            {
                "type": "object",
                "properties": {
                    "include_screenshot": {"type": "boolean", "default": True},
                    "max_nodes": {"type": "integer"},
                    "cursor": {"type": "string"},
                },
            },
        ),
        (
            "tap",
            "点击屏幕上一点（坐标或 node_id）。",
            {"type": "object", "properties": dict(target_props)},
        ),
        (
            "long_press",
            "长按屏幕上一点（坐标或 node_id），默认 500ms。",
            {
                "type": "object",
                "properties": {
                    **target_props,
                    "duration_ms": {"type": "integer", "default": 500},
                },
            },
        ),
        (
            "double_tap",
            "双击屏幕上一点（坐标或 node_id）。",
            {"type": "object", "properties": dict(target_props)},
        ),
        (
            "swipe",
            "从 (x1,y1) 滑到 (x2,y2)，默认 300ms。",
            {
                "type": "object",
                "properties": {
                    "x1": {"type": "integer"},
                    "y1": {"type": "integer"},
                    "x2": {"type": "integer"},
                    "y2": {"type": "integer"},
                    "duration_ms": {"type": "integer", "default": 300},
                },
                "required": ["x1", "y1", "x2", "y2"],
            },
        ),
        (
            "scroll",
            "滚动：direction ∈ up/down/left/right；可指定 node_id 限定滚动容器。",
            {
                "type": "object",
                "properties": {
                    "direction": {"type": "string", "enum": ["up", "down", "left", "right"]},
                    "node_id": {"type": "string"},
                },
                "required": ["direction"],
            },
        ),
        (
            "scroll_to_node",
            "滚动让指定 node 出现在屏内（spec §8.5，最多 5 次、300ms 间隔）。",
            {"type": "object", "properties": {"node_id": {"type": "string"}}, "required": ["node_id"]},
        ),
        (
            "type_text",
            "向当前聚焦的可输入框输入文本。",
            {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
        ),
        (
            "set_text",
            "替换指定 node 的文本内容（a11y ACTION_SET_TEXT）。",
            {
                "type": "object",
                "properties": {
                    "node_id": {"type": "string"},
                    "text": {"type": "string"},
                },
                "required": ["node_id", "text"],
            },
        ),
        (
            "press_key",
            f"按下功能键。key ∈ {press_keys}。",
            {
                "type": "object",
                "properties": {"key": {"type": "string", "enum": press_keys}},
                "required": ["key"],
            },
        ),
        (
            "dismiss_keyboard",
            "收起软键盘（已收起时仍返回 ok）。",
            {"type": "object", "properties": {}},
        ),
        (
            "press_back",
            "按下返回键。",
            {"type": "object", "properties": {}},
        ),
        (
            "press_home",
            "按下 Home 键。",
            {"type": "object", "properties": {}},
        ),
        (
            "press_recents",
            "按下最近任务键。",
            {"type": "object", "properties": {}},
        ),
        (
            "open_app",
            "按包名打开应用。",
            {"type": "object", "properties": {"package": {"type": "string"}}, "required": ["package"]},
        ),
        (
            "list_apps",
            "列出已安装应用。默认排除系统应用；include_system=true 含系统应用。返回 {apps:[{package,name}]}。",
            {
                "type": "object",
                "properties": {"include_system": {"type": "boolean", "default": False}},
            },
        ),
    ]


def builtin_state() -> list[dict]:
    """返回设备实时在线快照（供管理端经 registry.get_builtin_state 取数）。

    driver 未初始化或缺依赖时返回空列表。与 cdp-bridge 的 cdp_builtin_state 同形，
    走 registry.get_builtin_state("device-control")，保证读到的是 registry 当前持有的
    那个插件实例的状态（与 SSE 网关实际驱动的 driver 一致）。
    """
    try:
        from mcp_builtin.device_control import driver as dc_driver
    except ModuleNotFoundError:
        return []
    driver = dc_driver.get_driver()
    if driver is None:
        return []
    return driver.snapshot_devices()


def register(reg: PluginRegistrar) -> None:
    """注册 device-control 的 16 个工具并初始化 driver 单例。

    builtin device-control 是 runtime 组成部分：register() 建一次进程内 driver，
    注册静态工具定义。后续配对/解除配对经 apply_config 把 authorized_hashes 推给 driver，
    不重建工具、不重连。
    """
    _load_dc()  # 初始化 driver 单例
    for name, description, params in _tools():
        props = params.setdefault("properties", {})
        # device_id：principal 可访问多设备时由调用方显式指定（解析后会与 token 绑定值核对）。
        props["device_id"] = {
            "type": "string",
            "description": "目标设备 device_id（protocol 标识）。token 只绑一台时可省略；多台时必填。",
        }
        props["timeout_ms"] = {
            "type": "integer",
            "description": "设备侧响应预算（ms），默认 15000，上限 60000。",
        }
        reg.tool(name=name, description=description, params=params)(_wrap(name))


def apply_config(ctx: PluginContext) -> None:
    """把新的 authorized_hashes 快照推给活着的 driver。

    resources 由 build_builtin_snapshot 物化（device_authorized_hashes）。
    服务停用（enabled=False）时下发空集：driver.is_authorized 全 False，活连接逐帧
    被 close 4003——与 cdp 的 apply_clients 吊销 diff 同效果。
    """
    from mcp_builtin.device_control import driver as dc_driver
    driver = dc_driver.get_driver()
    if driver is None:
        return
    hashes = ctx.resources.get("authorized_hashes") if ctx.enabled else set()
    driver.apply_config(set(hashes) if hashes else set())


__all__ = ["register", "apply_config", "builtin_state"]
