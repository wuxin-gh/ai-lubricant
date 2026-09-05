"""device-control driver 的连接与命令路由契约测试。

这里钉住的是「照搬 Go 参考实现时最容易漏掉」的几条，每条都对应一个真实
故障模式（不是为覆盖率而写）：

- 超时后必须 pop 掉 pending：不 pop 会让在途计数被永久占满，8 次超时之后
  这台设备就再也发不出命令了（表现为莫名的 overloaded）。
- 断连时必须把所有 pending future 置异常：不做的话每个等待者都要干等满
  自己的预算才醒（cdp driver 缺的正是这一步）。
- 新连接踢旧连接：安卓半开连接很常见（doze / 网络切换 / 后台），若改成
  「已连接就拒绝」，设备会被锁死在无法重连的状态。
- 迟到的 call-response 必须被忽略而不是炸掉（spec §5）。
"""
import asyncio
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from mcp_builtin.device_control import protocol as proto
from mcp_builtin.device_control.driver import (
    DeviceContext,
    DeviceControlDriver,
    DeviceOffline,
)


class FakeSocket:
    """记录下发帧的假 WS；不模拟网络，只让 driver 的发送路径可观测。"""

    def __init__(self, fail_send: bool = False) -> None:
        self.sent: list[dict] = []
        self.closed_with: tuple[int, str] | None = None
        self.fail_send = fail_send

    async def send(self, frame: dict) -> None:
        if self.fail_send:
            raise ConnectionResetError("socket is gone")
        self.sent.append(frame)

    async def close(self, code: int, reason: str) -> None:
        self.closed_with = (code, reason)

    def frames_of_type(self, frame_type: str) -> list[dict]:
        return [f for f in self.sent if f.get("type") == frame_type]


def make_ctx(
    sock: FakeSocket | None = None,
    device_id: str = "dev_test",
    capabilities: list[str] | None = None,
) -> tuple[DeviceContext, FakeSocket]:
    sock = sock or FakeSocket()
    ctx = DeviceContext(
        device_id=device_id,
        session_id="ses_test",
        token_hash="hash_test",
        capabilities=capabilities if capabilities is not None else list(proto.COMMANDS),
        device_info={"platform": "android", "model": "Pixel 7"},
        send=sock.send,
        close=sock.close,
    )
    return ctx, sock


# ── capability 门禁（spec §8）───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_undeclared_command_is_refused_without_sending():
    """未声明的 cmd 根本不该下发到设备（spec §8）。"""
    ctx, sock = make_ctx(capabilities=["get_screen_state", "tap"])

    with pytest.raises(proto.DeviceError) as excinfo:
        await ctx.call("type_text", {"text": "hi"}, None)

    assert excinfo.value.code == proto.ERR_UNSUPPORTED
    assert sock.sent == [], "未声明的命令不能发给设备"


@pytest.mark.asyncio
async def test_capabilities_changed_event_opens_new_command():
    """capabilities-changed 之后，新声明的命令应当可用（spec §9）。"""
    ctx, sock = make_ctx(capabilities=["tap"])
    assert ctx.supports("type_text") is False

    ctx.set_capabilities(["tap", "type_text"])
    assert ctx.supports("type_text") is True


# ── request_id ↔ Future 配对 ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_call_resolves_with_deliver_by_request_id():
    """一次正常往返：下发 call，按 request_id 交付结果。"""
    ctx, sock = make_ctx()

    task = asyncio.create_task(ctx.call("get_screen_state", {}, None))
    await asyncio.sleep(0)  # 让 _dispatch 把帧发出去

    calls = sock.frames_of_type(proto.TYPE_CALL)
    assert len(calls) == 1
    request_id = calls[0]["request_id"]
    assert calls[0]["cmd"] == "get_screen_state"

    ctx.deliver(request_id, ok=True, data={"tree": "node_a\t..."}, error=None)
    assert await task == {"tree": "node_a\t..."}
    assert ctx.in_flight == 0


@pytest.mark.asyncio
async def test_device_error_response_raises_device_error():
    """ok=false 要变成 DeviceError，且错误码原样透出（spec §12）。"""
    ctx, sock = make_ctx()

    task = asyncio.create_task(ctx.call("tap", {"node_id": "node_gone"}, None))
    await asyncio.sleep(0)
    request_id = sock.frames_of_type(proto.TYPE_CALL)[0]["request_id"]

    ctx.deliver(
        request_id,
        ok=False,
        data=None,
        error={"code": proto.ERR_STALE_NODE, "message": "node not in latest tree"},
    )

    with pytest.raises(proto.DeviceError) as excinfo:
        await task
    assert excinfo.value.code == proto.ERR_STALE_NODE
    assert ctx.in_flight == 0


@pytest.mark.asyncio
async def test_ok_false_without_error_object_still_fails_cleanly():
    """ok=false 却没带 error 违反 spec §3.1，必须兜底成 device_error 而不是挂住。"""
    ctx, sock = make_ctx()

    task = asyncio.create_task(ctx.call("tap", {"x": 1, "y": 2}, None))
    await asyncio.sleep(0)
    request_id = sock.frames_of_type(proto.TYPE_CALL)[0]["request_id"]

    ctx.deliver(request_id, ok=False, data=None, error=None)

    with pytest.raises(proto.DeviceError) as excinfo:
        await task
    assert excinfo.value.code == proto.ERR_DEVICE_ERROR


@pytest.mark.asyncio
async def test_ok_true_with_null_data_becomes_empty_dict():
    """spec §3.1：ok=true 时 data 必填，但允许为空对象。"""
    ctx, sock = make_ctx()

    task = asyncio.create_task(ctx.call("press_back", {}, None))
    await asyncio.sleep(0)
    request_id = sock.frames_of_type(proto.TYPE_CALL)[0]["request_id"]

    ctx.deliver(request_id, ok=True, data=None, error=None)
    assert await task == {}


@pytest.mark.asyncio
async def test_late_response_for_unknown_request_id_is_ignored():
    """迟到/未知 request_id 的响应必须被丢弃，不能抛异常（spec §5）。"""
    ctx, _sock = make_ctx()

    # 不该抛：这是响应输给超时竞速后的正常形态。
    ctx.deliver("req_nonexistent", ok=True, data={"stale": True}, error=None)
    assert ctx.in_flight == 0


# ── 超时（spec §5）──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_timeout_pops_pending_and_sends_cancel(monkeypatch):
    """超时后必须 pop 掉 pending，并发一条建议性 call-cancel。

    这是最关键的一条回归点：不 pop 的话在途计数只增不减，8 次超时之后设备
    就永久 overloaded 了。
    """
    # 把预算压到极小，避免测试真的等 20 秒。
    monkeypatch.setattr(proto, "SERVER_GRACE_MS", 10)
    ctx, sock = make_ctx()

    with pytest.raises(proto.DeviceError) as excinfo:
        await ctx.call("tap", {"x": 1, "y": 2}, timeout_ms=1)

    assert excinfo.value.code == proto.ERR_TIMEOUT
    assert excinfo.value.retryable is True
    assert ctx.in_flight == 0, "超时必须释放在途槽位，否则设备会被永久占满"
    assert len(sock.frames_of_type(proto.TYPE_CALL_CANCEL)) == 1


@pytest.mark.asyncio
async def test_repeated_timeouts_do_not_exhaust_in_flight_slots(monkeypatch):
    """连续超时 MAX_IN_FLIGHT+1 次后仍能继续下发（承接上一条的真实后果）。"""
    monkeypatch.setattr(proto, "SERVER_GRACE_MS", 5)
    ctx, sock = make_ctx()

    for _ in range(proto.MAX_IN_FLIGHT + 1):
        with pytest.raises(proto.DeviceError) as excinfo:
            await ctx.call("tap", {"x": 1, "y": 1}, timeout_ms=1)
        assert excinfo.value.code == proto.ERR_TIMEOUT

    assert ctx.in_flight == 0
    # 还能正常发第 10 条并拿到结果，证明槽位没被泄漏占满。
    task = asyncio.create_task(ctx.call("get_screen_state", {}, None))
    await asyncio.sleep(0)
    request_id = sock.frames_of_type(proto.TYPE_CALL)[-1]["request_id"]
    ctx.deliver(request_id, ok=True, data={"ok": 1}, error=None)
    assert await task == {"ok": 1}


# ── 在途上限（spec §5）──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_in_flight_cap_returns_overloaded():
    """第 9 条并发只读命令要被挡成 overloaded（spec §5）。

    用只读命令是因为改 UI 的命令会被串行锁排队，攒不出并发在途。
    """
    ctx, sock = make_ctx()

    tasks = [
        asyncio.create_task(ctx.call("get_screen_state", {}, None))
        for _ in range(proto.MAX_IN_FLIGHT)
    ]
    await asyncio.sleep(0)
    assert ctx.in_flight == proto.MAX_IN_FLIGHT

    with pytest.raises(proto.DeviceError) as excinfo:
        await ctx.call("list_apps", {}, None)
    assert excinfo.value.code == proto.ERR_OVERLOADED
    assert excinfo.value.retryable is True

    # 收尾：让已挂起的调用都失败，避免测试留下未完成任务。
    await ctx.close(proto.CLOSE_STALE, "test teardown")
    for task in tasks:
        with pytest.raises(DeviceOffline):
            await task


@pytest.mark.asyncio
async def test_mutating_commands_are_serialized():
    """改 UI 的命令必须按收到顺序串行（spec §5：交错两次点击是正确性问题）。"""
    ctx, sock = make_ctx()

    first = asyncio.create_task(ctx.call("tap", {"x": 1, "y": 1}, None))
    second = asyncio.create_task(ctx.call("tap", {"x": 2, "y": 2}, None))
    await asyncio.sleep(0)

    # 第二条必须还没下发——它在等第一条完成。
    assert len(sock.frames_of_type(proto.TYPE_CALL)) == 1

    ctx.deliver(sock.frames_of_type(proto.TYPE_CALL)[0]["request_id"], True, {}, None)
    await first
    await asyncio.sleep(0)

    calls = sock.frames_of_type(proto.TYPE_CALL)
    assert len(calls) == 2, "第一条完成后第二条才下发"
    ctx.deliver(calls[1]["request_id"], True, {}, None)
    await second


# ── 断连（spec §7）──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_close_fails_all_pending_immediately():
    """断连要让所有在途调用立刻失败，而不是各自干等满预算。"""
    ctx, sock = make_ctx()

    tasks = [
        asyncio.create_task(ctx.call("get_screen_state", {}, None))
        for _ in range(3)
    ]
    await asyncio.sleep(0)
    assert ctx.in_flight == 3

    await ctx.close(proto.CLOSE_STALE, "heartbeat timeout")

    for task in tasks:
        with pytest.raises(DeviceOffline):
            await task
    assert ctx.in_flight == 0
    assert ctx.closed is True
    assert sock.closed_with == (proto.CLOSE_STALE, "heartbeat timeout")


@pytest.mark.asyncio
async def test_call_on_closed_context_raises_offline():
    ctx, _sock = make_ctx()
    await ctx.close(proto.CLOSE_AUTH_FAILED, "revoked")

    with pytest.raises(DeviceOffline):
        await ctx.call("get_screen_state", {}, None)


@pytest.mark.asyncio
async def test_close_is_idempotent():
    """重复 close 不能重复关 socket 或再次置异常。"""
    ctx, sock = make_ctx()
    await ctx.close(proto.CLOSE_REPLACED, "first")
    await ctx.close(proto.CLOSE_STALE, "second")

    assert sock.closed_with == (proto.CLOSE_REPLACED, "first"), "首次 close 的原因应保留"


@pytest.mark.asyncio
async def test_send_failure_surfaces_as_offline_and_releases_slot():
    """发送失败要变成 DeviceOffline，并且不能泄漏在途槽位。"""
    sock = FakeSocket(fail_send=True)
    ctx, _ = make_ctx(sock=sock)

    with pytest.raises(DeviceOffline):
        await ctx.call("get_screen_state", {}, None)
    assert ctx.in_flight == 0


# ── 注册表：新连接踢旧连接（spec §4.4）─────────────────────────────────────


@pytest.mark.asyncio
async def test_new_connection_evicts_old_one_for_same_device():
    """同一 device_id 再注册要踢掉旧连接并 close 4009（last-writer-wins）。

    这是刻意不用「已连接就拒绝」的：安卓半开连接频繁，拒绝式会把设备锁死。
    """
    driver = DeviceControlDriver()
    old_ctx, old_sock = make_ctx(device_id="dev_same")
    new_ctx, new_sock = make_ctx(device_id="dev_same")

    await driver.register(old_ctx)
    await driver.register(new_ctx)
    # 踢旧连接是后台 task（旧连接可能半开，不能阻塞新连接握手），
    # 所以要让出一轮事件循环才能观察到它关闭。
    await asyncio.sleep(0)

    assert old_ctx.closed is True
    assert old_sock.closed_with is not None
    assert old_sock.closed_with[0] == proto.CLOSE_REPLACED
    assert new_ctx.closed is False
    assert driver.get("dev_same") is new_ctx


@pytest.mark.asyncio
async def test_stale_connection_teardown_does_not_remove_its_replacement():
    """旧连接的延迟清理不能把顶替它的新连接摘掉。

    对应 Go 版 hub.Remove / node_server 的 unregister 同名保护。
    """
    driver = DeviceControlDriver()
    old_ctx, _ = make_ctx(device_id="dev_same")
    new_ctx, _ = make_ctx(device_id="dev_same")

    await driver.register(old_ctx)
    await driver.register(new_ctx)

    # 旧连接的 read loop 这时才发现自己断了，来做清理。
    driver.unregister(old_ctx)

    assert driver.get("dev_same") is new_ctx, "新连接必须还在注册表里"
    assert driver.is_online("dev_same") is True


@pytest.mark.asyncio
async def test_unregister_removes_the_live_connection():
    driver = DeviceControlDriver()
    ctx, _ = make_ctx(device_id="dev_one")

    await driver.register(ctx)
    assert driver.online_ids() == ["dev_one"]

    driver.unregister(ctx)
    assert driver.online_ids() == []
    assert driver.is_online("dev_one") is False


@pytest.mark.asyncio
async def test_call_on_offline_device_raises_offline():
    driver = DeviceControlDriver()

    with pytest.raises(DeviceOffline):
        await driver.call("dev_missing", "get_screen_state", {}, None)


# ── 吊销与收割 ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_apply_config_disconnects_revoked_device():
    """吊销后活连接要立刻断（close 4003），不能等心跳超时。"""
    driver = DeviceControlDriver()
    ctx, sock = make_ctx(device_id="dev_revoke")
    await driver.register(ctx)
    driver.apply_config({"hash_test"})
    assert driver.is_online("dev_revoke") is True

    # 该设备的 token 被吊销：快照里不再有它的 hash。
    driver.apply_config(set())
    await asyncio.sleep(0)

    assert ctx.closed is True
    assert sock.closed_with is not None
    assert sock.closed_with[0] == proto.CLOSE_AUTH_FAILED


@pytest.mark.asyncio
async def test_reap_stale_closes_silent_connections(monkeypatch):
    """超过心跳超时没收到任何帧的连接要被收割（spec §7）。"""
    driver = DeviceControlDriver()
    fresh_ctx, fresh_sock = make_ctx(device_id="dev_fresh")
    stale_ctx, stale_sock = make_ctx(device_id="dev_stale")
    await driver.register(fresh_ctx)
    await driver.register(stale_ctx)

    # 把 stale 的 last_seen 推回到超时之前。
    stale_ctx.last_seen -= proto.HEARTBEAT_TIMEOUT_S + 5

    reaped = await driver.reap_stale()

    assert reaped == ["dev_stale"]
    assert stale_ctx.closed is True
    assert stale_sock.closed_with[0] == proto.CLOSE_STALE
    assert fresh_ctx.closed is False
    assert driver.is_online("dev_fresh") is True


@pytest.mark.asyncio
async def test_touch_resets_liveness_so_device_is_not_reaped():
    """任意帧都刷新活性，不只是心跳帧（spec §7）。"""
    driver = DeviceControlDriver()
    ctx, _ = make_ctx(device_id="dev_active")
    await driver.register(ctx)

    ctx.last_seen -= proto.HEARTBEAT_TIMEOUT_S + 5
    ctx.touch()  # 例如收到一条 call-response

    assert await driver.reap_stale() == []
    assert ctx.closed is False


def test_is_authorized_passes_before_any_config_is_applied():
    """还没下发过配置时放行：启动瞬间不能把已握手的正常连接全断掉。

    握手时已经过 store 认证，这里的复查只针对「配置 reload 后凭据没了」。
    """
    driver = DeviceControlDriver()
    assert driver.is_authorized("hash_a") is True


def test_is_authorized_reflects_applied_snapshot():
    driver = DeviceControlDriver()
    driver.apply_config({"hash_a", "hash_b"})

    assert driver.is_authorized("hash_a") is True
    assert driver.is_authorized("hash_c") is False


def test_empty_snapshot_rejects_everything():
    """下发空快照 = 凭据全被删了，必须全拒——不能和「未下发」混为一谈。

    用空 set 兼表两种状态时，「删光所有设备」会退化成「全部放行」。
    """
    driver = DeviceControlDriver()
    driver.apply_config(set())

    assert driver.is_authorized("hash_a") is False
    assert driver.is_authorized("") is False


@pytest.mark.asyncio
async def test_snapshot_devices_reports_runtime_state():
    """管理端/用户端要能看到在线态与设备信息。"""
    driver = DeviceControlDriver()
    ctx, _ = make_ctx(device_id="dev_snap", capabilities=["tap", "get_screen_state"])
    await driver.register(ctx)

    rows = driver.snapshot_devices()
    assert len(rows) == 1
    row = rows[0]
    assert row["device_id"] == "dev_snap"
    assert row["capabilities"] == ["tap", "get_screen_state"]
    assert row["device_info"]["model"] == "Pixel 7"
    assert row["in_flight"] == 0
