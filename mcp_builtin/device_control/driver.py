"""device-control 设备连接表与命令下发。

对应参考实现的 ``internal/hub/hub.go``：维护「哪些设备连着」，把 MCP 工具调用
翻译成一条 ``call`` 帧发给设备，再等它的 ``call-response``。

与 cdp-bridge driver 的形态一致（``init_driver`` / ``apply_config`` / ``get_driver``
单例），这样 mcp_runtime 的现有接线能直接复用；但内部实现有三点故意不同：

1. **用 ``asyncio.Future`` 按 request_id 配对**，不像 cdp-bridge 那样轮询等结果。
   cdp driver 是同步的，才需要 ``asyncio.to_thread`` +
   ``run_coroutine_threadsafe`` 那一层；这里全程 async，省掉整层复杂度。
2. **只有两级结构** ``device_id -> DeviceContext``。cdp 是
   ``client_id -> UserContext -> ClientContext -> pages``，因为一个浏览器扇出成
   很多标签页；一台手机就是一个端点，没有第三层。
3. **新连接踢旧连接**（spec §4.4 last-writer-wins，旧连接 close 4009），不用
   cdp 的 ``already_connected`` 拒绝式。安卓半开连接远比 Chrome 频繁——doze、
   蜂窝/WiFi 切换、进程被后台回收都不发 TCP FIN——拒绝式会把设备锁在
   「服务端以为还连着、设备怎么都连不上」的状态里。空闲超时仍然保留，但它只是
   兜底清理，不再是设备能否重连的关键路径。

并发模型：每条连接一个读循环（在 sse_gateway 里），写入由 ``_send`` 串行化。
driver 自身的状态改动都在事件循环单线程内完成，故不需要锁。
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

from mcp_builtin.device_control import protocol as proto


class DeviceOffline(Exception):
    """设备当前没有活的连接。"""


class DeviceContext:
    """一台已注册设备的活连接。

    ``pending`` 是 request_id → Future 的映射，也是这个类存在的理由：下发和收结果
    发生在不同的协程里（HTTP 请求 vs WS 读循环），Future 是它们之间的汇合点。
    """

    __slots__ = (
        "device_id",
        "resource_id",
        "session_id",
        "capabilities",
        "device_info",
        "connected_at",
        "last_seen",
        "token_hash",
        "_send",
        "_close",
        "_pending",
        "_closed",
        "_mutating_lock",
    )

    def __init__(
        self,
        device_id: str,
        session_id: str,
        capabilities: list[str],
        device_info: dict | None,
        token_hash: str,
        send: Callable[[dict], Awaitable[None]],
        close: Callable[[int, str], Awaitable[None]],
        resource_id: int | None = None,
    ) -> None:
        self.device_id = device_id
        # 对应 builtin_tool_resources.id：断连/device_status 事件要把运行态落库时用它定位行。
        self.resource_id = resource_id
        self.session_id = session_id
        self.capabilities = list(capabilities or [])
        self.device_info = device_info or {}
        self.token_hash = token_hash
        self.connected_at = time.time()
        self.last_seen = time.time()
        self._send = send
        self._close = close
        self._pending: dict[str, asyncio.Future] = {}
        self._closed = False
        # spec §5：设备必须按收到顺序串行执行会改 UI 的命令。设备端自己也做了
        # 串行化，但服务端同样不并发下发，避免「两个 tap 交错」这种正确性问题在
        # 网络层就已经产生。只读命令（get_screen_state / list_apps）不占这把锁。
        self._mutating_lock = asyncio.Lock()

    # ── 基础状态 ────────────────────────────────────────────────────────────
    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_flight(self) -> int:
        return len(self._pending)

    def touch(self) -> None:
        """记录「收到了一帧」。spec §7：任意帧都刷新活性，不限于 heartbeat。"""
        self.last_seen = time.time()

    def supports(self, cmd: str) -> bool:
        """设备是否声明了这个能力（spec §8）。"""
        return cmd in self.capabilities

    def set_capabilities(self, capabilities: list[str]) -> None:
        """处理 capabilities-changed 事件（spec §9）。"""
        self.capabilities = list(capabilities or [])

    def snapshot(self) -> dict:
        """给管理端/用户端看的运行态快照。"""
        return {
            "device_id": self.device_id,
            "session_id": self.session_id,
            "capabilities": list(self.capabilities),
            "device_info": dict(self.device_info),
            "connected_at": self.connected_at,
            "last_seen": self.last_seen,
            "in_flight": self.in_flight,
            "idle_seconds": round(time.time() - self.last_seen, 1),
        }

    # ── 命令下发 ────────────────────────────────────────────────────────────
    async def call(self, cmd: str, args: dict | None, timeout_ms: int | None) -> Any:
        """下发一条命令并等它的 call-response。

        抛 :class:`proto.DeviceError` 表示「设备说不」——这是协议层面的正常答复；
        抛 :class:`DeviceOffline` 表示连接没了。调用方要区分这两种。
        """
        if not self.supports(cmd):
            # spec §8：服务端根本不该下发未声明的 cmd，在这里就拦住。
            raise proto.DeviceError(
                proto.ERR_UNSUPPORTED,
                f"device did not declare capability {cmd}",
            )
        if self._closed:
            raise DeviceOffline(f"device {self.device_id} is not connected")
        if len(self._pending) >= proto.MAX_IN_FLIGHT:
            # spec §5：在途上限 8。设备收到第 9 条会回 overloaded，我们提前挡掉。
            raise proto.DeviceError(
                proto.ERR_OVERLOADED,
                "too many in-flight calls",
                retryable=True,
            )

        if cmd in proto.READ_ONLY_COMMANDS:
            return await self._dispatch(cmd, args, timeout_ms)
        async with self._mutating_lock:
            return await self._dispatch(cmd, args, timeout_ms)

    async def _dispatch(self, cmd: str, args: dict | None, timeout_ms: int | None) -> Any:
        budget_ms = proto.clamp_call_timeout(timeout_ms)
        request_id = proto.new_id("req_")
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[request_id] = future

        try:
            await self._send(proto.call_frame(request_id, cmd, args, budget_ms))
        except Exception as exc:
            self._pending.pop(request_id, None)
            raise DeviceOffline(f"could not send to device {self.device_id}: {exc}") from exc

        # 服务端预算 = 设备预算 + 宽限（spec §5）。设备到点会自己回 timeout 错误；
        # 这个更长的预算只防「设备连 timeout 都没回」。
        server_budget = (budget_ms + proto.SERVER_GRACE_MS) / 1000
        try:
            return await asyncio.wait_for(future, timeout=server_budget)
        except asyncio.TimeoutError:
            # 必须 pop：spec §5 说超时后放弃这个 request_id 并忽略迟到的响应。
            # 不 pop 会让 _pending 无限增长，还会把在途计数永久占满。
            self._pending.pop(request_id, None)
            await self._cancel(request_id)
            raise proto.DeviceError(
                proto.ERR_TIMEOUT,
                "no call-response within server budget",
                retryable=True,
            ) from None
        finally:
            self._pending.pop(request_id, None)

    async def _cancel(self, request_id: str) -> None:
        """发一条建议性 call-cancel（spec §5）。

        尽力而为：调用方已经放弃了，这里失败也改变不了什么。
        """
        try:
            await self._send(proto.call_cancel_frame(request_id))
        except Exception:
            pass

    # ── 收结果 ──────────────────────────────────────────────────────────────
    def deliver(self, request_id: str, ok: bool, data: Any, error: dict | None) -> None:
        """把一条 call-response 交给它的等待者。

        request_id 不认识就直接丢弃——那是「响应输给了超时竞速」的正常形态
        （spec §5 明确要求忽略迟到响应），不是错误。
        """
        future = self._pending.pop(request_id, None)
        if future is None or future.done():
            return
        if ok:
            # spec §3.1：ok=true 时 data 必填，但允许是 {}。
            future.set_result(data if data is not None else {})
        else:
            future.set_exception(proto.DeviceError.from_frame(error))

    # ── 收尾 ────────────────────────────────────────────────────────────────
    async def close(self, code: int, reason: str) -> None:
        """关连接，并让所有在途调用立刻失败。

        「立刻失败」是关键：不做的话每个等待者都要干等满自己的预算才醒
        （cdp driver 就缺这一步）。参考 node_server 的 ``Connection.close()``。
        """
        if self._closed:
            return
        self._closed = True
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(
                    DeviceOffline(f"device {self.device_id} disconnected: {reason}")
                )
        try:
            await self._close(code, reason)
        except Exception:
            pass


class DeviceControlDriver:
    """所有已连接设备的注册表，兼命令路由。

    对应 ``hub.Hub``。不持有凭据——那是 store 的事；这里只管活连接。
    """

    def __init__(self) -> None:
        # device_id -> 活连接。一台设备最多一条（spec §4.4）。
        self._devices: dict[str, DeviceContext] = {}
        # 已授权设备的 token_hash 快照，由 apply_config 下发。吊销后要能立刻
        # 断掉活连接，而不是等心跳超时。
        #
        # None 表示「还没下发过配置」，与「下发了空快照」是两种状态：前者要放行
        # （启动瞬间不能把正常连接全断掉），后者意味着凭据全被删了、必须全拒。
        # 用空 set 兼表两种，会让「删光所有设备」变成「全部放行」。
        self._authorized_hashes: set[str] | None = None

    # ── 连接生命周期 ────────────────────────────────────────────────────────
    async def register(self, ctx: DeviceContext) -> None:
        """登记一条连接，并踢掉同 device_id 的旧连接（spec §4.4）。

        踢旧连接放到独立 task 里：旧连接可能已经卡死（半开），不能让它的收尾
        阻塞新连接的握手。
        """
        existing = self._devices.get(ctx.device_id)
        self._devices[ctx.device_id] = ctx
        if existing is not None and existing is not ctx:
            asyncio.create_task(
                existing.close(proto.CLOSE_REPLACED, "replaced by newer connection")
            )

    def unregister(self, ctx: DeviceContext) -> None:
        """摘掉一条连接，但仅当它仍是当前登记的那条。

        这个判断是必要的：被踢掉的旧连接稍后也会走清理流程，如果不判断就会把
        取代它的新连接误删（与 ``hub.Remove`` 同构）。
        """
        if self._devices.get(ctx.device_id) is ctx:
            del self._devices[ctx.device_id]

    def get(self, device_id: str) -> DeviceContext | None:
        return self._devices.get(device_id)

    def is_online(self, device_id: str) -> bool:
        ctx = self._devices.get(device_id)
        return ctx is not None and not ctx.closed

    def online_ids(self) -> list[str]:
        return [device_id for device_id, ctx in self._devices.items() if not ctx.closed]

    def snapshot_devices(self) -> list[dict]:
        """所有活连接的运行态快照（供管理端/用户端展示）。"""
        return [ctx.snapshot() for ctx in self._devices.values()]

    # ── 命令路由 ────────────────────────────────────────────────────────────
    async def call(
        self,
        device_id: str,
        cmd: str,
        args: dict | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """按 device_id 路由一条命令。"""
        ctx = self._devices.get(device_id)
        if ctx is None:
            raise DeviceOffline(f"device {device_id} is not connected")
        return await ctx.call(cmd, args, timeout_ms)

    # ── 配置热更新 ──────────────────────────────────────────────────────────
    def apply_config(self, authorized_hashes: set[str] | None) -> None:
        """更新已授权 token 快照，并断开不再授权的活连接。

        对应 cdp driver 的 ``apply_clients`` 吊销 diff：用户在前端删掉/轮换了
        设备凭据后，活连接必须当场断，不能等到下次心跳超时。
        """
        allowed: set[str] = set(authorized_hashes or ())
        self._authorized_hashes = allowed
        for ctx in list(self._devices.values()):
            if ctx.token_hash and ctx.token_hash not in allowed:
                asyncio.create_task(
                    ctx.close(proto.CLOSE_AUTH_FAILED, "credential revoked")
                )

    def is_authorized(self, token_hash: str) -> bool:
        """token hash 是否还在授权快照里。

        WS 读循环每帧复查这个，所以配置 reload 后失效的连接会在下一帧被断掉
        （与 cdp 的 ``clients_by_hash`` 复查同一思路）。
        """
        if self._authorized_hashes is None:
            # 还没下发过配置：不做二次判断，交给握手时的 store 认证结果，
            # 避免启动瞬间把正常连接全断掉。
            #
            # 注意与「下发了空快照」的区别——那说明凭据全被删了，下面会全部拒绝。
            return True
        return token_hash in self._authorized_hashes

    async def disconnect(self, device_id: str, code: int, reason: str) -> None:
        """强制断开某台设备（吊销/删除时用，让操作立刻生效）。"""
        ctx = self._devices.get(device_id)
        if ctx is not None:
            await ctx.close(code, reason)

    # ── 空闲收割 ────────────────────────────────────────────────────────────
    async def reap_stale(self, timeout_s: int = proto.HEARTBEAT_TIMEOUT_S) -> list[str]:
        """断开超过 timeout_s 没有任何帧的连接（spec §7，close 4010）。

        因为已经改成「新连接踢旧连接」，这里只是兜底清理：它让服务端的在线状态
        不会长期显示一台其实已经消失的设备，但设备重连不再依赖它先跑完。
        """
        now = time.time()
        reaped: list[str] = []
        for device_id, ctx in list(self._devices.items()):
            if now - ctx.last_seen > timeout_s:
                reaped.append(device_id)
                await ctx.close(proto.CLOSE_STALE, "heartbeat timeout")
        return reaped


# ── 单例接线（与 cdp-bridge 同形态，便于 registry 复用）─────────────────────
driver: DeviceControlDriver | None = None


def init_driver() -> DeviceControlDriver:
    """创建进程内 driver 单例（幂等）。"""
    global driver
    if driver is None:
        driver = DeviceControlDriver()
    return driver


def get_driver() -> DeviceControlDriver | None:
    return driver


def apply_config(authorized_hashes: set[str] | None) -> None:
    """把新的授权快照推给活着的 driver。"""
    if driver is not None:
        driver.apply_config(authorized_hashes)


__all__ = [
    "DeviceContext",
    "DeviceControlDriver",
    "DeviceOffline",
    "driver",
    "init_driver",
    "get_driver",
    "apply_config",
]
