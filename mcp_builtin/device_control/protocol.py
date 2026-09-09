"""device-control 协议 v0 的常量与帧定义。

这是 device-control `spec/protocol-v0.md` 的直接转写（对应参考实现的
``internal/protocol/protocol.go``）。这里只放常量与纯函数，不含任何 IO 或状态：
任何改动如果不是同时改协议 spec，就是 bug。

设备端 App 已按本 spec 实现并真机验证过，所以字段名、close code、错误码
都不能自行调整——改了就连不上。
"""
from __future__ import annotations

import base64
import os

# 本实现所讲的 protocol_version（spec §14）。握手双方都带这个字段，
# 服务端权威：不匹配直接 close 4004，绝不"假定兼容"。
VERSION = 0

# ── 帧类型（spec §3）────────────────────────────────────────────────────────
# 设备 → 服务端
TYPE_REGISTER = "register"
TYPE_HEARTBEAT = "heartbeat"
TYPE_CALL_RESPONSE = "call-response"
TYPE_EVENT = "event"
# 服务端 → 设备
TYPE_REGISTERED = "registered"
TYPE_CALL = "call"
TYPE_CALL_CANCEL = "call-cancel"

# ── WebSocket close code（spec §13，应用区间 4000-4999）──────────────────────
# 设备端把 4003 当终态（擦凭据、停止重连），其余一律退避重连。
CLOSE_FRAME_TOO_LARGE = 4002
CLOSE_AUTH_FAILED = 4003
CLOSE_VERSION_UNSUPPORTED = 4004
CLOSE_DUPLICATE_REGISTER = 4007
CLOSE_REGISTER_TIMEOUT = 4008
CLOSE_REPLACED = 4009
CLOSE_STALE = 4010

# ── 设备上报的错误码（spec §12）──────────────────────────────────────────────
# 服务端只消费这些码，绝不代替设备"编造"一个。
ERR_UNSUPPORTED = "unsupported"
ERR_BAD_ARGS = "bad_args"
ERR_STALE_NODE = "stale_node"
ERR_NOT_FOUND = "not_found"
ERR_TIMEOUT = "timeout"
ERR_CANCELLED = "cancelled"
ERR_DUPLICATE_REQUEST = "duplicate_request"
ERR_OVERLOADED = "overloaded"
ERR_PERMISSION_DENIED = "permission_denied"
ERR_NOT_READY = "not_ready"
ERR_DEVICE_ERROR = "device_error"

# ── 协议常量与默认值（spec §2 §4 §5 §7）─────────────────────────────────────
MAX_FRAME_BYTES = 4 << 20      # §2：4 MiB
REGISTER_TIMEOUT_S = 10        # §4.1：WS 打开后必须在 10s 内发 register
HEARTBEAT_INTERVAL_S = 15      # §7：设备心跳间隔
HEARTBEAT_TIMEOUT_S = 60       # §7：任意帧都刷新，超时收割
MAX_IN_FLIGHT = 8              # §5：单设备最大在途 call
DEFAULT_CALL_TIMEOUT_MS = 15000  # §5
MAX_CALL_TIMEOUT_MS = 60000    # §5：设备侧可 clamp 到这个上限
SERVER_GRACE_MS = 5000         # §5：服务端预算 = timeout_ms + 5s

# v0 只定义 token 一种 auth scheme（spec §11）。scheme 是扩展点，
# 将来加 TOTP/mTLS 不用改信封；收到未知 scheme 一律 close 4003。
SCHEME_TOKEN = "token"

# ── 事件类型（spec §9）──────────────────────────────────────────────────────
EVENT_CONTROL_REVOKED = "control-revoked"
EVENT_CAPABILITIES_CHANGED = "capabilities-changed"
# app 在会话内上报设备态（无障碍开关变化等），服务端 merge 进 device_info 落库，
# 网页不必等下次 register 即可看到实时开关状态。v0 专用，未在 spec §9 列出但走
# 同一 event 前向兼容铰链（未知 kind 忽略），旧服务端收到不会报错。
EVENT_DEVICE_STATUS = "device_status"

# ── 命令词汇表（spec §8）────────────────────────────────────────────────────
# 服务端不得下发设备未在 capabilities 里声明的 cmd；真下发了设备会回
# unsupported。这个元组是"协议认识哪些命令"的单一来源，工具定义从它派生。
COMMANDS: tuple[str, ...] = (
    "get_screen_state",
    "tap",
    "long_press",
    "double_tap",
    "swipe",
    "scroll",
    "scroll_to_node",
    "type_text",
    "set_text",
    "press_key",
    "dismiss_keyboard",
    "press_back",
    "press_home",
    "press_recents",
    "open_app",
    "list_apps",
)

# press_key 的取值（spec §8.6）。音量/电源键在 v0 故意排除。
PRESS_KEYS: tuple[str, ...] = (
    "enter",
    "tab",
    "delete",
    "backspace",
    "escape",
    "space",
    "dpad_up",
    "dpad_down",
    "dpad_left",
    "dpad_right",
    "dpad_center",
)

# 只读命令：spec §5 要求设备串行化所有会改 UI 状态的命令，这两个例外可并发。
READ_ONLY_COMMANDS: frozenset[str] = frozenset({"get_screen_state", "list_apps"})


def clamp_call_timeout(ms: int | None) -> int:
    """套用 spec §5 的默认值与上限。

    非正数（含 None）视为"没填"，取默认 15s；超过 60s 截到 60s。
    """
    try:
        value = int(ms or 0)
    except (TypeError, ValueError):
        return DEFAULT_CALL_TIMEOUT_MS
    if value <= 0:
        return DEFAULT_CALL_TIMEOUT_MS
    return min(value, MAX_CALL_TIMEOUT_MS)


def new_id(prefix: str) -> str:
    """生成 prefix + 128 位 base64url 随机串。

    用于 device_id / request_id / session_id（spec §3.3 要求不透明且 ≤64 字节）。
    去掉 base64 填充，因此形如 ``req_`` + 22 字符。
    """
    return prefix + base64.urlsafe_b64encode(os.urandom(16)).decode("ascii").rstrip("=")


def registered_frame(
    device_id: str,
    session_id: str,
    server_time: str,
    accepted_capabilities: list[str],
) -> dict:
    """构造 register 的 ack（spec §3.2）。

    协商参数走这一帧下发；设备端会采纳 heartbeat_interval_s。
    """
    return {
        "type": TYPE_REGISTERED,
        "protocol_version": VERSION,
        "device_id": device_id,
        "server_time": server_time,
        "session_id": session_id,
        "heartbeat_interval_s": HEARTBEAT_INTERVAL_S,
        "heartbeat_timeout_s": HEARTBEAT_TIMEOUT_S,
        "accepted_capabilities": list(accepted_capabilities),
    }


def call_frame(request_id: str, cmd: str, args: dict | None, timeout_ms: int) -> dict:
    """构造一条 call（spec §3.2）。"""
    return {
        "type": TYPE_CALL,
        "request_id": request_id,
        "cmd": cmd,
        "args": args or {},
        "timeout_ms": timeout_ms,
    }


def call_cancel_frame(request_id: str) -> dict:
    """构造建议性取消帧（spec §5）。设备仍必须回一条 call-response。"""
    return {"type": TYPE_CALL_CANCEL, "request_id": request_id}


class DeviceError(Exception):
    """设备回的结构化错误（spec §12）。

    这是"设备说不"，属于协议层面的正常答复，不是服务端故障——所以调用方要
    把它和传输失败区分开（参考实现里 hub.Call 用两个返回值做这件事）。
    """

    __slots__ = ("code", "message", "retryable", "details")

    def __init__(
        self,
        code: str,
        message: str = "",
        retryable: bool = False,
        details: dict | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details

    def to_dict(self) -> dict:
        out: dict = {"code": self.code}
        if self.message:
            out["message"] = self.message
        if self.retryable:
            out["retryable"] = True
        if self.details is not None:
            out["details"] = self.details
        return out

    @classmethod
    def from_frame(cls, error: dict | None) -> "DeviceError":
        """从 call-response.error 还原。

        ok=false 但没带 error 违反 spec §3.1；这里合成一个 device_error，
        避免等待方永远挂着（参考实现 wsdevice.go 同样这么兜底）。
        """
        if not isinstance(error, dict):
            return cls(ERR_DEVICE_ERROR, "device reported failure without error object")
        return cls(
            code=str(error.get("code") or ERR_DEVICE_ERROR),
            message=str(error.get("message") or ""),
            retryable=bool(error.get("retryable")),
            details=error.get("details") if isinstance(error.get("details"), dict) else None,
        )


__all__ = [
    "VERSION",
    "TYPE_REGISTER",
    "TYPE_HEARTBEAT",
    "TYPE_CALL_RESPONSE",
    "TYPE_EVENT",
    "TYPE_REGISTERED",
    "TYPE_CALL",
    "TYPE_CALL_CANCEL",
    "CLOSE_FRAME_TOO_LARGE",
    "CLOSE_AUTH_FAILED",
    "CLOSE_VERSION_UNSUPPORTED",
    "CLOSE_DUPLICATE_REGISTER",
    "CLOSE_REGISTER_TIMEOUT",
    "CLOSE_REPLACED",
    "CLOSE_STALE",
    "ERR_UNSUPPORTED",
    "ERR_BAD_ARGS",
    "ERR_STALE_NODE",
    "ERR_NOT_FOUND",
    "ERR_TIMEOUT",
    "ERR_CANCELLED",
    "ERR_DUPLICATE_REQUEST",
    "ERR_OVERLOADED",
    "ERR_PERMISSION_DENIED",
    "ERR_NOT_READY",
    "ERR_DEVICE_ERROR",
    "MAX_FRAME_BYTES",
    "REGISTER_TIMEOUT_S",
    "HEARTBEAT_INTERVAL_S",
    "HEARTBEAT_TIMEOUT_S",
    "MAX_IN_FLIGHT",
    "DEFAULT_CALL_TIMEOUT_MS",
    "MAX_CALL_TIMEOUT_MS",
    "SERVER_GRACE_MS",
    "SCHEME_TOKEN",
    "EVENT_CONTROL_REVOKED",
    "EVENT_CAPABILITIES_CHANGED",
    "COMMANDS",
    "PRESS_KEYS",
    "READ_ONLY_COMMANDS",
    "clamp_call_timeout",
    "new_id",
    "registered_frame",
    "call_frame",
    "call_cancel_frame",
    "DeviceError",
]
