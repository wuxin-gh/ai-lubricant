"""device-control 协议层与配对码存储的契约测试。

这两层是纯函数 + Redis，没有 WebSocket 依赖，所以可以独立跑。它们锁住的是
「与已上线的 Android App 对齐」这件事：App 已按 spec v0 真机验证过，任何一处
常量、字母表或归一化规则漂移，表现都是「配对不上 / 连不上」而不是报错，
排查成本极高。
"""
import asyncio
import os
import sys

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import pytest

from mcp_builtin.device_control import protocol, store


# ── 协议常量：改动等于改协议，必须与 spec/App 同步 ──────────────────────────

def test_protocol_version_is_zero():
    """spec §14：v0 就是整数 0，服务端权威。App 收到不匹配会 close 4004 并停。"""
    assert protocol.VERSION == 0


def test_close_codes_match_spec():
    """App 的 classifyClose 按这些码分流，4003 是唯一「擦凭据且不重连」的终态。"""
    assert protocol.CLOSE_FRAME_TOO_LARGE == 4002
    assert protocol.CLOSE_AUTH_FAILED == 4003
    assert protocol.CLOSE_VERSION_UNSUPPORTED == 4004
    assert protocol.CLOSE_DUPLICATE_REGISTER == 4007
    assert protocol.CLOSE_REGISTER_TIMEOUT == 4008
    assert protocol.CLOSE_REPLACED == 4009
    assert protocol.CLOSE_STALE == 4010


def test_protocol_limits_match_spec():
    assert protocol.MAX_FRAME_BYTES == 4 * 1024 * 1024
    assert protocol.REGISTER_TIMEOUT_S == 10
    assert protocol.HEARTBEAT_INTERVAL_S == 15
    assert protocol.HEARTBEAT_TIMEOUT_S == 60
    assert protocol.MAX_IN_FLIGHT == 8
    assert protocol.DEFAULT_CALL_TIMEOUT_MS == 15000
    assert protocol.MAX_CALL_TIMEOUT_MS == 60000
    assert protocol.SERVER_GRACE_MS == 5000


def test_command_vocabulary_is_the_16_spec_commands():
    """spec §8 的命令表。工具定义从这里派生，所以少一个就等于少一个 MCP 工具。"""
    assert len(protocol.COMMANDS) == 16
    assert "get_screen_state" in protocol.COMMANDS
    assert "tap" in protocol.COMMANDS
    assert "list_apps" in protocol.COMMANDS
    # press_back/home/recents 是独立命令而非 press_key 取值，以便在只读平台上
    # 单独做能力门禁（spec §8）。
    assert {"press_back", "press_home", "press_recents"} <= set(protocol.COMMANDS)
    assert "press_back" not in protocol.PRESS_KEYS


def test_read_only_commands_are_the_two_concurrency_safe_ones():
    """spec §5：除这两个以外的命令都会改 UI 状态，设备必须串行执行。"""
    assert protocol.READ_ONLY_COMMANDS == frozenset({"get_screen_state", "list_apps"})


def test_clamp_call_timeout_applies_default_and_cap():
    assert protocol.clamp_call_timeout(None) == protocol.DEFAULT_CALL_TIMEOUT_MS
    assert protocol.clamp_call_timeout(0) == protocol.DEFAULT_CALL_TIMEOUT_MS
    assert protocol.clamp_call_timeout(-5) == protocol.DEFAULT_CALL_TIMEOUT_MS
    assert protocol.clamp_call_timeout("bad") == protocol.DEFAULT_CALL_TIMEOUT_MS
    assert protocol.clamp_call_timeout(3000) == 3000
    assert protocol.clamp_call_timeout(999999) == protocol.MAX_CALL_TIMEOUT_MS


def test_new_id_is_opaque_and_within_64_bytes():
    """spec §3.3：不透明、≤64 字节。"""
    value = protocol.new_id("req_")
    assert value.startswith("req_")
    assert len(value) <= 64
    assert "=" not in value  # 去掉 base64 填充
    assert protocol.new_id("req_") != value  # 每次不同


def test_registered_frame_shape():
    frame = protocol.registered_frame(
        device_id="dev_x", session_id="ses_y", server_time="2026-08-25T00:00:00Z",
        accepted_capabilities=["tap"],
    )
    assert frame["type"] == "registered"
    assert frame["protocol_version"] == 0
    assert frame["device_id"] == "dev_x"
    assert frame["session_id"] == "ses_y"
    assert frame["heartbeat_interval_s"] == protocol.HEARTBEAT_INTERVAL_S
    assert frame["heartbeat_timeout_s"] == protocol.HEARTBEAT_TIMEOUT_S
    assert frame["accepted_capabilities"] == ["tap"]


def test_call_frame_defaults_args_to_empty_object():
    """spec §3.2：args 省略时设备侧按空对象处理，别发 null。"""
    frame = protocol.call_frame("req_1", "press_home", None, 15000)
    assert frame == {
        "type": "call", "request_id": "req_1", "cmd": "press_home",
        "args": {}, "timeout_ms": 15000,
    }


# ── 设备错误：spec §12 的结构化错误 ─────────────────────────────────────────

def test_device_error_from_frame_synthesises_missing_error_object():
    """ok=false 但没带 error 违反 spec §3.1；必须兜底成 device_error，
    否则等待方永远挂着（参考实现 wsdevice.go 同样兜底）。"""
    err = protocol.DeviceError.from_frame(None)
    assert err.code == protocol.ERR_DEVICE_ERROR
    assert "without error object" in err.message


def test_device_error_round_trips_structured_fields():
    err = protocol.DeviceError.from_frame({
        "code": "stale_node", "message": "gone", "retryable": False,
        "details": {"node_id": "node_1"},
    })
    assert err.code == "stale_node"
    assert err.retryable is False
    assert err.details == {"node_id": "node_1"}
    assert err.to_dict()["details"] == {"node_id": "node_1"}


def test_device_error_to_dict_omits_empty_optionals():
    """只带 code 的错误不该序列化出一堆 null 字段。"""
    assert protocol.DeviceError("timeout").to_dict() == {"code": "timeout"}


# ── 配对码：字母表与归一化必须与 Go 版/App 逐字一致 ─────────────────────────

def test_code_alphabet_excludes_confusable_characters():
    """store.go:31 故意排除 I/O/0/1，这样念出来或手抄都不会歧义。
    App 端不校验字母表，靠服务端 403 兜底，所以这里是唯一的真相源。"""
    assert protocol is not None
    for ch in "IO01":
        assert ch not in store.CODE_ALPHABET
    assert len(store.CODE_ALPHABET) == 32


def test_normalize_code_matches_go_normalizecode():
    """store.go:148-156：大写化 + 只留 [A-Z0-9]。App 端 normalizeCode 同规则，
    两边不一致会把一个正确的码变成 403。"""
    assert store.normalize_code("abcd-efgh") == "ABCDEFGH"
    assert store.normalize_code("ABCD EFGH") == "ABCDEFGH"
    assert store.normalize_code("a1b2-c3d4") == "A1B2C3D4"
    assert store.normalize_code("  k7qm-3xpd  ") == "K7QM3XPD"
    assert store.normalize_code("") == ""
    # 非 ASCII 字符被丢掉，不能让 upper() 的 Unicode 折叠混进来
    assert store.normalize_code("ABCＤ") == "ABC"


def test_generate_code_has_dash_in_the_middle_and_valid_alphabet():
    code = store.generate_code()
    assert len(code) == store.CODE_LEN + 1
    assert code[store.CODE_LEN // 2] == "-"
    assert all(ch in store.CODE_ALPHABET for ch in code.replace("-", ""))
    # 归一化后长度回到 CODE_LEN，与 hash 输入一致
    assert len(store.normalize_code(code)) == store.CODE_LEN


def test_hash_secret_is_stable_and_secrets_equal_is_constant_time():
    a = store.hash_secret("K7QM3XPD")
    assert a == store.hash_secret("K7QM3XPD")
    assert len(a) == 64
    assert store.secrets_equal(a, a) is True
    assert store.secrets_equal(a, store.hash_secret("OTHER")) is False


def test_generated_token_is_high_entropy():
    token = store.generate_token()
    assert len(token) >= 40  # 32 字节 urlsafe base64
    assert token != store.generate_token()


def test_new_device_id_uses_dev_prefix():
    device_id = store.new_device_id()
    assert device_id.startswith("dev_")
    assert len(device_id) <= 64


# ── 配对码 Redis 语义 ───────────────────────────────────────────────────────

class _FakeRedis:
    """最小 Redis 替身：只覆盖本模块用到的语义。

    刻意区分「覆写方法自动加前缀」与「原生方法不加前缀」——RedisJdbc 的这个
    不对称是真实存在的（rd.py 只覆写了一部分方法），store 必须两边对称，
    否则写进去的 key 读不出来。
    """

    def __init__(self, prefix_key: str | None = "mapi"):
        self.prefix_key = prefix_key
        self.data: dict[str, str] = {}

    def get_key(self, key: str) -> str:
        return f"{self.prefix_key}:{key}" if self.prefix_key is not None else key

    async def set(self, key, value, nx=False, ex=None):
        # 覆写方法：自动加前缀
        full = self.get_key(key)
        if nx and full in self.data:
            return None
        self.data[full] = value
        return True

    async def getdel(self, full_key):
        # 原生方法：调用方必须已经加好前缀
        return self.data.pop(full_key, None)


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


@pytest.fixture
def fake_redis(monkeypatch):
    redis = _FakeRedis()
    monkeypatch.setattr(store, "_redis", lambda: redis)
    return redis


def test_mint_then_redeem_round_trip(fake_redis):
    """写用裸 key（覆写 set 自动加前缀）、读用完整 key（原生 getdel），
    两边必须指向同一物理 key。这个测试就是那个对称性的回归点。"""
    code, ttl = _run(store.mint_pairing_code(instance_id=42, label="我的手机"))
    assert ttl == store.PAIRING_CODE_TTL_SECONDS
    owner_payload, label = _run(store.redeem_pairing_code(code))
    assert owner_payload == "owner:42"
    assert label == "我的手机"


def test_pairing_code_is_single_use(fake_redis):
    """GETDEL 而非 GET+DELETE：两台设备抢同一个码，必须恰好一个成功。"""
    code, _ = _run(store.mint_pairing_code(instance_id=7))
    _run(store.redeem_pairing_code(code))
    with pytest.raises(store.UnknownCodeError):
        _run(store.redeem_pairing_code(code))


def test_redeem_accepts_user_typed_variants(fake_redis):
    """用户可能漏了横线或用小写；归一化后应命中同一个码。"""
    code, _ = _run(store.mint_pairing_code(instance_id=9))
    typed = code.replace("-", "").lower()
    owner_payload, _ = _run(store.redeem_pairing_code(typed))
    assert owner_payload == "owner:9"


def test_redeem_unknown_code_raises_unknown(fake_redis):
    with pytest.raises(store.UnknownCodeError):
        _run(store.redeem_pairing_code("ZZZZ-ZZZZ"))


def test_redeem_empty_code_raises_unknown_without_touching_redis(fake_redis):
    """空码不该产生一次 Redis 往返。"""
    with pytest.raises(store.UnknownCodeError):
        _run(store.redeem_pairing_code("---"))
    assert fake_redis.data == {}


def test_plaintext_code_is_never_stored(fake_redis):
    """只存 hash：泄露 Redis 不该拿到可用的配对码。"""
    code, _ = _run(store.mint_pairing_code(instance_id=1))
    normalized = store.normalize_code(code)
    for key, value in fake_redis.data.items():
        assert normalized not in key
        assert normalized not in value
    assert store.hash_secret(normalized) in next(iter(fake_redis.data))


def test_pairing_requires_redis(monkeypatch):
    """Redis 不可用时必须显式失败，不能退化到进程内存——多 worker 下
    「A worker 发码、B worker 兑换失败」看起来就是随机故障。"""
    monkeypatch.setattr(store, "_redis", lambda: None)
    with pytest.raises(store.RedisUnavailableError):
        _run(store.mint_pairing_code(instance_id=1))
    with pytest.raises(store.RedisUnavailableError):
        _run(store.redeem_pairing_code("ABCD-EFGH"))


def test_works_without_key_prefix(monkeypatch):
    """prefix_key 为 None（未配置前缀）时读写仍要对称。"""
    redis = _FakeRedis(prefix_key=None)
    monkeypatch.setattr(store, "_redis", lambda: redis)
    code, _ = _run(store.mint_pairing_code(instance_id=3))
    assert _run(store.redeem_pairing_code(code))[0] == "owner:3"
