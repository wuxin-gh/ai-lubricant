"""device-control WS 端点与配对 HTTP 端点的契约测试。

钉住「照搬 spec v0 时最容易漏」的几条握手/吊销不变量，全部用 fake WebSocket
+ monkeypatch 的 store/driver，不依赖真 DB / Redis / 网络。与 driver 单测互补：
driver 测配对与命令路由的内部状态，这里测 sse_gateway 把帧翻译成 driver 调用
+ DB 凭据校验的接线是否正确。
"""
import asyncio
import json
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from mcp_builtin.device_control import protocol as proto
from mcp_builtin.device_control import driver as dc_driver_mod


class FakeWebSocket:
    """记录下发帧的假 WS；可被驱动成 push 首帧、读超时、断连。"""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed_with: tuple[int, str] | None = None
        self._inbox: asyncio.Queue = asyncio.Queue()
        self._accept_calls = 0

    def __aiter__(self):
        return self

    async def accept(self) -> None:
        self._accept_calls += 1

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed_with is None:
            self.closed_with = (code, reason)

    async def receive_text(self) -> str:
        # 队列空时挂住直到被喂帧或被取消；测试用 wait_for 控制超时分支。
        return await self._inbox.get()

    async def push(self, frame: dict | str) -> None:
        await self._inbox.put(json.dumps(frame) if isinstance(frame, dict) else frame)

    def frames(self) -> list[dict]:
        return [json.loads(f) for f in self.sent]

    def frames_of_type(self, ftype: str) -> list[dict]:
        return [f for f in self.frames() if f.get("type") == ftype]


def _register_frame(device_id="dev_x", token="tok_x", capabilities=None, pv=0):
    return {
        "type": proto.TYPE_REGISTER,
        "protocol_version": pv,
        "device_id": device_id,
        "auth": {"scheme": proto.SCHEME_TOKEN, "token": token},
        "capabilities": capabilities if capabilities is not None else ["tap", "get_screen_state"],
        "device_info": {"platform": "android", "model": "Pixel 8"},
    }


@pytest.fixture
def fresh_driver(monkeypatch):
    """每个测试给一个干净的 driver 单例 + stub 掉 registry 门禁。

    WS 端点的 registry.get("device-control") 门禁与 cdp-bridge /session 同形（服务
    未注册或停用就拒），这里 stub 成一个 enabled 插件，好让测试专注验证握手/帧路由，
    不必真去 activate_builtin 拉起整套插件注册。
    """
    import builtin_tool_store  # noqa: F401  确保模块已加载，monkeypatch 才能改它的属性
    from mcp_runtime import sse_gateway as gw

    monkeypatch.setattr(dc_driver_mod, "driver", None)
    driver = dc_driver_mod.init_driver()
    # 抢在 WS 握手前下发一次非空授权集，否则 is_authorized(None 态) 放行会让吊销断言失真。
    driver.apply_config({"hash_x"})

    class _Ctx:
        enabled = True

    class _Plugin:
        def __init__(self):
            self.ctx = _Ctx()

    _stub = _Plugin()

    def fake_get(name):
        return _stub if name == "device-control" else None

    monkeypatch.setattr(gw.registry, "get", fake_get)
    return driver


# authenticate_device 是协程，测试里必须用 async 假函数 patch，否则 `await <dict>`
# 会炸成 "object dict can't be used in 'await' expression"。
async def _auth_ok(d, t):
    return {"device_id": d, "token_hash": "hash_x", "enabled": True}


async def _auth_none(*_a, **_k):
    return None


async def _run_ws(ws, service_name="device-control"):
    from mcp_runtime import sse_gateway as gw
    await gw.device_ws(ws, service_name)


# ── 握手：spec §4 ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_register_handshake_sends_registered_and_registers(fresh_driver, monkeypatch):
    """正常 register：回 registered 帧，ctx 进 driver 注册表。"""
    called = {}

    async def fake_auth(device_id, token):
        called["args"] = (device_id, token)
        return {"device_id": device_id, "token_hash": "hash_x", "enabled": True}

    monkeypatch.setattr("builtin_tool_store.authenticate_device", fake_auth)

    ws = FakeWebSocket()
    task = asyncio.create_task(_run_ws(ws))
    await ws.push(_register_frame(device_id="dev_x", token="tok_x"))
    await asyncio.sleep(0.02)  # 让握手跑完

    registered = ws.frames_of_type(proto.TYPE_REGISTERED)
    assert len(registered) == 1
    assert registered[0]["protocol_version"] == proto.VERSION
    assert registered[0]["device_id"] == "dev_x"
    assert "session_id" in registered[0]
    assert registered[0]["accepted_capabilities"] == ["tap", "get_screen_state"]
    assert called["args"] == ("dev_x", "tok_x")
    assert fresh_driver.is_online("dev_x") is True

    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_wrong_protocol_version_closes_4004(fresh_driver, monkeypatch):
    """版本不匹配必须 close 4004，绝假定兼容（spec §14）。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_none)
    ws = FakeWebSocket()
    await _run_ws_with_first(ws, _register_frame(pv=99))
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_VERSION_UNSUPPORTED


@pytest.mark.asyncio
async def test_bad_auth_scheme_closes_4003(fresh_driver, monkeypatch):
    """未知 scheme 是鉴权失败（4003），不是版本失败（spec §11）。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_none)
    frame = _register_frame()
    frame["auth"]["scheme"] = "totp"  # v0 只有 token
    ws = FakeWebSocket()
    await _run_ws_with_first(ws, frame)
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_AUTH_FAILED


@pytest.mark.asyncio
async def test_invalid_credentials_closes_4003(fresh_driver, monkeypatch):
    """authenticate 返回 None → 4003，且 driver 里没有该设备。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_none)
    ws = FakeWebSocket()
    await _run_ws_with_first(ws, _register_frame())
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_AUTH_FAILED
    assert fresh_driver.online_ids() == []


@pytest.mark.asyncio
async def test_no_register_within_deadline_closes_4008(fresh_driver, monkeypatch):
    """10s 内没发 register → close 4008（spec §4.1）。"""
    monkeypatch.setattr(proto, "REGISTER_TIMEOUT_S", 0.05)
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_none)
    ws = FakeWebSocket()
    await _run_ws(ws)
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_REGISTER_TIMEOUT


# ── 帧路由：call-response / event / 重复 register ──────────────────────────


@pytest.mark.asyncio
async def test_call_response_delivered_to_pending(fresh_driver, monkeypatch):
    """设备回 call-response，对应等待的 ctx.call 要能拿到结果。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_ok)
    ws = FakeWebSocket()
    task = asyncio.create_task(_run_ws(ws))
    await ws.push(_register_frame())
    await asyncio.sleep(0.02)

    ctx = fresh_driver.get("dev_x")
    call_task = asyncio.create_task(ctx.call("get_screen_state", {}, None))
    await asyncio.sleep(0.02)
    call_frame = ws.frames_of_type(proto.TYPE_CALL)[0]
    await ws.push({
        "type": proto.TYPE_CALL_RESPONSE,
        "request_id": call_frame["request_id"],
        "ok": True,
        "data": {"tree": "node_a\t..."},
    })
    assert await asyncio.wait_for(call_task, 2) == {"tree": "node_a\t..."}

    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_capabilities_changed_event_updates_ctx(fresh_driver, monkeypatch):
    """capabilities-changed 事件后，新能力可用（spec §9）。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_ok)
    ws = FakeWebSocket()
    task = asyncio.create_task(_run_ws(ws))
    await ws.push(_register_frame(capabilities=["tap"]))
    await asyncio.sleep(0.02)

    ctx = fresh_driver.get("dev_x")
    assert ctx.supports("type_text") is False

    await ws.push({"type": proto.TYPE_EVENT, "kind": proto.EVENT_CAPABILITIES_CHANGED,
                   "capabilities": ["tap", "type_text"]})
    await asyncio.sleep(0.02)
    assert ctx.supports("type_text") is True

    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_duplicate_register_is_fatal(fresh_driver, monkeypatch):
    """一条连接上的第二次 register close 4007（spec §4.3）。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_ok)
    ws = FakeWebSocket()
    task = asyncio.create_task(_run_ws(ws))
    await ws.push(_register_frame())
    await asyncio.sleep(0.02)
    await ws.push(_register_frame())  # 第二次
    await asyncio.sleep(0.02)
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_DUPLICATE_REGISTER
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_revoked_token_disconnects_live_connection(fresh_driver, monkeypatch):
    """配置 reload 后 token 失效：活连接收到下一帧时 close 4003（spec 吊销）。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_ok)
    ws = FakeWebSocket()
    task = asyncio.create_task(_run_ws(ws))
    await ws.push(_register_frame())
    await asyncio.sleep(0.02)

    # 吊销：把授权集清空（用户在前端解除配对）。
    fresh_driver.apply_config(set())
    # 推一帧心跳触发每帧复查 → 发现 hash 不在授权集 → close 4003。
    await ws.push({"type": proto.TYPE_HEARTBEAT, "seq": 1})
    await asyncio.sleep(0.02)
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_AUTH_FAILED
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_oversized_frame_closes_4002(fresh_driver, monkeypatch):
    """超 4MiB 的首帧 close 4002（spec §2）。"""
    monkeypatch.setattr("builtin_tool_store.authenticate_device", _auth_none)
    monkeypatch.setattr(proto, "MAX_FRAME_BYTES", 16)
    ws = FakeWebSocket()
    await _run_ws_with_first(ws, "x" * 100)  # 超过压小的上限
    assert ws.closed_with is not None
    assert ws.closed_with[0] == proto.CLOSE_FRAME_TOO_LARGE


# ── pair 端点：spec §11 ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pair_redeems_code_and_returns_credentials(monkeypatch):
    """配对码兑换成功 → 返回 {device_id, token, protocol_version}，且推 driver 新 hash。"""
    from mcp_runtime import sse_gateway as gw

    redeemed = {}

    async def fake_redeem(code):
        redeemed["code"] = code
        return 42, "我的手机"

    async def fake_create(instance_id, label, device_info=None):
        return {"device_id": "dev_new", "token_hint": "dev_…"}, "dev_new", "tok_plain"

    async def fake_hashes():
        return {"hash_new"}

    monkeypatch.setattr("mcp_builtin.device_control.store.redeem_pairing_code", fake_redeem)
    monkeypatch.setattr("builtin_tool_store.create_device", fake_create)
    monkeypatch.setattr("builtin_tool_store.device_authorized_hashes", fake_hashes)
    # driver 单例取法
    monkeypatch.setattr(dc_driver_mod, "driver", None)
    driver = dc_driver_mod.init_driver()
    monkeypatch.setattr(gw, "_device_driver", lambda: driver)

    class FakeReq:
        async def json(self):
            return {"code": "ABCD-EFGH"}

    resp = await gw.device_pair(FakeReq())
    assert resp["device_id"] == "dev_new"
    assert resp["token"] == "tok_plain"
    assert resp["protocol_version"] == proto.VERSION
    assert redeemed["code"] == "ABCD-EFGH"
    assert driver.is_authorized("hash_new") is True


@pytest.mark.asyncio
async def test_pair_unknown_code_returns_403(monkeypatch):
    """未知/过期/已用码一律 403，不做区分（spec §11）。"""
    from mcp_runtime import sse_gateway as gw
    from mcp_builtin.device_control.store import UnknownCodeError

    async def fake_redeem(code):
        raise UnknownCodeError("nope")

    monkeypatch.setattr("mcp_builtin.device_control.store.redeem_pairing_code", fake_redeem)

    class FakeReq:
        async def json(self):
            return {"code": "ZZZZ"}

    with pytest.raises(gw.HTTPException) as exc:
        await gw.device_pair(FakeReq())
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_pair_empty_code_returns_400(monkeypatch):
    from mcp_runtime import sse_gateway as gw

    class FakeReq:
        async def json(self):
            return {"code": "  "}

    with pytest.raises(gw.HTTPException) as exc:
        await gw.device_pair(FakeReq())
    assert exc.value.status_code == 400


# ── 路由分发：未知服务名/未加载 ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unknown_service_name_rejected(fresh_driver):
    """非 device-control 的服务名走 ws/device 端点要被拒。"""
    ws = FakeWebSocket()
    await _run_ws(ws, service_name="cdp-bridge")
    assert ws._accept_calls == 0  # 没 accept 就直接 close
    assert ws.closed_with is not None


# ── 辅助 ──────────────────────────────────────────────────────────────────────


async def _run_ws_with_first(ws, first):
    """喂一帧首帧后跑 device_ws，等其自然结束（close 或读阻塞被取消）。"""
    task = asyncio.create_task(_run_ws(ws))
    if isinstance(first, str):
        await ws.push(first)
    else:
        await ws.push(first)
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
