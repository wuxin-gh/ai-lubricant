import asyncio
import contextvars
import time
from types import SimpleNamespace

import pytest

from limits.backend import RedisLimitBackend
from limits.manager import LimitManager, begin_routing_redis_scope, end_routing_redis_scope
from limits.rules import LimitLease, LimitSubject
from rate_limiter import AccountClient


class _Provider:
    PROVIDER_NAME = "p1"
    username = "u1"
    quotas = None

    def __init__(self):
        self.limits = SimpleNamespace()

    async def reserve_limits_with_decision(self, model_id, messages, context=None):
        return await LimitManager.acquire_account(context["account_client"], model_id, messages)

    async def reserve_limits(self, model_id, messages, context=None):
        result = await self.reserve_limits_with_decision(model_id, messages, context)
        return result.lease

    async def record_message(self, model_id, messages):
        return None

    async def check_message(self, model_id, messages):
        return True

    async def check_limits(self, model_id=None, messages=None, context=None):
        return SimpleNamespace(allowed=True)

    def is_init(self):
        return True


@pytest.mark.asyncio
async def test_account_lock_does_not_wrap_redis_wait(monkeypatch):
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def _slow_reserve(cls, *args, **kwargs):
        entered.set()
        await finish.wait()
        return {"allowed": True, "reason": "", "ttl": 0, "rpm": 0, "rpd": 0}

    monkeypatch.setattr(RedisLimitBackend, "reserve_account_limits", classmethod(_slow_reserve))
    account = AccountClient(_Provider(), rpm_limit=1)

    reserve_task = asyncio.create_task(account.reserve("m1", []))
    await entered.wait()
    async with asyncio.timeout(0.05):
        async with account._lock:
            assert account._in_flight == 0
    finish.set()
    assert await reserve_task is True
    await account.release()


@pytest.mark.asyncio
async def test_local_fallback_registration_is_atomic(monkeypatch):
    async def _redis_unavailable(cls, *args, **kwargs):
        return None

    monkeypatch.setattr(RedisLimitBackend, "reserve_account_limits", classmethod(_redis_unavailable))
    account = AccountClient(_Provider(), rpm_limit=1, concurrent_limit=0)

    results = await asyncio.gather(*(account.reserve("m1", []) for _ in range(20)))
    assert all(results)
    assert account._in_flight == 20


@pytest.mark.asyncio
async def test_release_uses_context_lease_id_not_completion_order(monkeypatch):
    seq = 0
    released = []

    async def _reserve(cls, account_client, model_id, messages=None, now=None, *, is_test=False):
        nonlocal seq
        seq += 1
        lease = LimitLease(LimitSubject(provider="p1", account="u1", model=model_id), f"lease-{seq}", concurrency_acquired=True)
        return SimpleNamespace(lease=lease, decision=SimpleNamespace(allowed=True), allowed=True)

    async def _release(cls, lease):
        released.append(lease.lease_id)

    monkeypatch.setattr(LimitManager, "acquire_account", classmethod(_reserve))
    monkeypatch.setattr(LimitManager, "release_account_lease", classmethod(_release))
    account = AccountClient(_Provider(), rpm_limit=0, concurrent_limit=10)
    first_ready = asyncio.Event()
    second_done = asyncio.Event()

    async def first():
        assert await account.reserve("m1", [])
        first_ready.set()
        await second_done.wait()
        await account.release()

    async def second():
        await first_ready.wait()
        assert await account.reserve("m1", [])
        await account.release()
        second_done.set()

    await asyncio.gather(first(), second())
    assert released == ["lease-2", "lease-1"]
    assert account._limit_leases == {}
    assert account._in_flight == 0


@pytest.mark.asyncio
async def test_reservation_rechecks_configured_frozen_state_after_candidate_collection():
    account = AccountClient(_Provider(), rpm_limit=0, concurrent_limit=0, is_frozen=False)
    # 模拟候选收集完成后、真正 reserve 前管理员冻结账号。
    account.freeze(kind="account", seconds=None)

    result = await account.reserve_with_decision("m1", [])

    assert result.allowed is False
    assert result.decision.reason == "account_frozen"
    assert account._in_flight == 0


@pytest.mark.asyncio
async def test_concurrency_redis_uncertainty_fails_closed(monkeypatch):
    async def _uncertain(cls, *args, **kwargs):
        return None

    async def _release(cls, *args, **kwargs):
        return False

    monkeypatch.setattr(RedisLimitBackend, "reserve_account_limits", classmethod(_uncertain))
    monkeypatch.setattr(RedisLimitBackend, "release_account_reservation", classmethod(_release))
    account = AccountClient(_Provider(), rpm_limit=0, concurrent_limit=1)
    assert await account.reserve("m1", []) is False
    assert account._in_flight == 0
    await LimitManager.shutdown()


@pytest.mark.asyncio
async def test_reservation_uncertainty_skips_later_concurrency_waits(monkeypatch):
    calls = 0

    async def _uncertain(cls, *args, **kwargs):
        nonlocal calls
        calls += 1
        return None

    async def _release(cls, *args, **kwargs):
        return True

    monkeypatch.setattr(RedisLimitBackend, "reserve_account_limits", classmethod(_uncertain))
    monkeypatch.setattr(RedisLimitBackend, "release_account_reservation", classmethod(_release))
    account = AccountClient(_Provider(), rpm_limit=0, concurrent_limit=1)
    token = begin_routing_redis_scope()
    try:
        first = await account.reserve_with_decision("m1", [])
        second = await account.reserve_with_decision("m1", [])
        assert first.decision.reason == "redis_uncertain"
        assert second.decision.reason == "redis_uncertain"
        assert second.decision.details["skipped"] is True
        assert calls == 1
    finally:
        end_routing_redis_scope(token)
        await LimitManager.shutdown()


@pytest.mark.asyncio
async def test_confirmed_concurrency_lease_is_renewed_and_released(monkeypatch):
    renewed = asyncio.Event()
    released = []

    async def _reserve(cls, *args, **kwargs):
        return {
            "allowed": True,
            "reason": "",
            "ttl": 0,
            "rpm": 0,
            "rpd": 0,
            "concurrent_used": 0,
            "lease_expires_at": time.time() + 1,
        }

    async def _renew(cls, account_key, lease_id, ttl_seconds):
        renewed.set()
        return time.time() + ttl_seconds

    async def _release(cls, account_key, lease_id):
        released.append(lease_id)
        return True

    monkeypatch.setattr(RedisLimitBackend, "reserve_account_limits", classmethod(_reserve))
    monkeypatch.setattr(RedisLimitBackend, "renew_account_reservation", classmethod(_renew))
    monkeypatch.setattr(RedisLimitBackend, "release_account_reservation", classmethod(_release))
    monkeypatch.setattr(LimitManager, "CONCURRENCY_LEASE_SECONDS", 0.03)

    account = AccountClient(_Provider(), rpm_limit=0, concurrent_limit=1)
    assert await account.reserve("m1", []) is True
    await asyncio.wait_for(renewed.wait(), timeout=0.2)
    await account.release()

    assert len(released) == 1
    assert LimitManager._active_leases == {}
    await LimitManager.shutdown()


def test_atomic_account_lua_checks_freeze_before_increment(monkeypatch):
    captured = {}

    class FakeRedis:
        prefix_key = "test"

        async def eval(self, script, keys=None, args=None):
            captured["script"] = script
            captured["key_count"] = len(keys or [])
            captured["keys"] = keys
            captured["args"] = args
            return [0, "frozen", 32, 0, 0]

    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: FakeRedis()))
    result = asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "lease-1",
        freeze_key="limit:cooldown:p1:u1",
        model_freeze_key="limit:model-tpm-freeze:m1",
        account_model_freeze_key="limit:cooldown:p1:u1:model:m1",
        concurrent_limit=2, rpm_limit=3, rpd_limit=10,
    ))

    script = captured["script"]
    # 三个冻结 TTL 检查必须早于任何计量写入（HSET）与并发占用（ZADD）。
    first_write = min(script.index("HSET"), script.index("ZADD"))
    assert script.index("TTL', KEYS[4]") < first_write
    assert script.index("TTL', KEYS[5]") < first_write
    assert script.index("TTL', KEYS[6]") < first_write
    # 计量走 per-reservation ledger（HSET member=reservation_id），不再裸 INCR。
    assert "INCR'" not in script
    # KEYS: concurrency + rpm/rpd + 3 个 freeze + rph + tpm/tph/tpd + model_tpm
    assert captured["key_count"] == 13
    assert result == {
        "allowed": False,
        "reason": "frozen",
        "ttl": 32,
        "retry_after": 0,
        "rpm": 0,
        "rpd": 0,
        "rph": 0,
        "concurrent_used": 0,
        "lease_expires_at": 0,
        "tpm": 0,
        "tph": 0,
        "tpd": 0,
        "model_tpm": 0,
        "model_tpm_freeze_ttl": 0,
        "window_ids": result["window_ids"],
    }


def test_reserve_ledger_keys_are_window_scoped_and_reservation_scoped(monkeypatch):
    """ledger key 必须带固定窗口 id：窗口切换天然隔离，回滚不会扣到新窗口。"""
    captured = {}

    class FakeRedis:
        prefix_key = None

        async def eval(self, script, keys=None, args=None):
            captured["keys"] = keys
            captured["args"] = args
            return [1, "", 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]

    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: FakeRedis()))
    result = asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "lease-abc",
        freeze_key="limit:cooldown:p1:u1",
        model_freeze_key="limit:model-tpm-freeze:m1",
        account_model_freeze_key="limit:cooldown:p1:u1:model:m1",
        rpm_limit=5, model_id="m1", token_amount=100, tpm_limit=1000,
    ))
    wids = result["window_ids"]
    keys = captured["keys"]
    assert keys[1] == f"limit:reservation:v1:account:account:p1:u1:rpm:{wids['rpm']}"
    assert keys[7] == f"limit:reservation:v1:account:account:p1:u1:tpm:{wids['tpm']}"
    # 模型 TPM 是跨账号共享窗口，key 里不含账号。
    assert keys[10] == f"limit:reservation:v1:model:model:m1:tpm:{wids['model_tpm']}"
    # reservation_id 作为 HASH member 传入，回滚/补差按它精确匹配。
    assert captured["args"][3] == "lease-abc"


def test_reserve_denies_token_overflow_when_flag_disabled(monkeypatch):
    """关闭「允许 token 预占触线放行」后，projected 触线必须在写入任何 ledger 前拒绝。"""
    captured = {}

    class FakeRedis:
        prefix_key = None

        async def eval(self, script, keys=None, args=None):
            captured["script"] = script
            captured["args"] = args
            return [0, "token_limit", 42, 0, 0, 0, 0, 0]

    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: FakeRedis()))
    result = asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "lease-1",
        freeze_key="limit:cooldown:p1:u1",
        model_freeze_key="limit:model-tpm-freeze:m1",
        account_model_freeze_key="limit:cooldown:p1:u1:model:m1",
        model_id="m1", token_amount=500, tpm_limit=1000,
        allow_token_overflow=False,
    ))
    script = captured["script"]
    # allow_exceed 标志按 0/1 下传
    assert captured["args"][24] == 0
    # 触线拒绝分支（return token_limit）必须出现在请求数写入之前
    assert script.index("'token_limit'") < script.index("rpm = projected")
    assert result["allowed"] is False
    assert result["reason"] == "token_limit"
    # token_limit 是瞬时拒绝（换候选即可），不当作冻结时长
    assert result["ttl"] == 0
    assert result["retry_after"] == 42


def test_reserve_allows_token_overflow_by_default(monkeypatch):
    """默认放行：触线仍写 ledger 并放行本次，靠 freeze 挡后续（与请求数口径一致）。"""
    captured = {}

    class FakeRedis:
        prefix_key = None

        async def eval(self, script, keys=None, args=None):
            captured["args"] = args
            return [1, "tpm", 30, 0, 0, 0, 0, 0, 1200, 0, 0, 0]

    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: FakeRedis()))
    result = asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "lease-1",
        freeze_key="limit:cooldown:p1:u1",
        model_freeze_key="limit:model-tpm-freeze:m1",
        account_model_freeze_key="limit:cooldown:p1:u1:model:m1",
        model_id="m1", token_amount=500, tpm_limit=1000,
    ))
    assert captured["args"][24] == 1
    assert result["allowed"] is True
    assert result["tpm"] == 1200
    # 触线放行时返回 freeze 原因 + 剩余窗口，供 Manager 冻结后续请求
    assert result["reason"] == "tpm"
    assert result["ttl"] == 30


# ==================== per-reservation ledger 语义（可精确回滚）====================

class _LedgerRedis:
    """模拟 coredis eval + zset/hset 的最小 ledger，覆盖 reserve/rollback/reconcile 的
    语义：按 reservation_id 精确增删，回滚不影响并发请求、窗口切换天然隔离。"""

    def __init__(self):
        self.prefix_key = None
        self.store: dict[str, dict] = {}
        self.concurrency: dict[str, dict[str, float]] = {}
        self.frozen: dict[str, tuple] = {}

    async def eval(self, script, keys=None, args=None):
        keys = keys or []
        # 用脚本首命令粗略分派到对应实现。
        head = script.strip()
        if head.startswith("local freeze_ttl"):
            return self._reserve(keys, args)
        if head.startswith("local removed"):
            return self._rollback(keys, args)
        if head.startswith("local rid"):
            return self._reconcile(keys, args)
        return None

    def _hset(self, key, mapping):
        h = self.store.setdefault(key, {})
        h.update(mapping)
        return h

    def _hget(self, key, field):
        return self.store.get(key, {}).get(field)

    def _hdel(self, key, *fields):
        h = self.store.get(key, {})
        removed = 0
        for f in fields:
            if f in h:
                del h[f]
                removed += 1
        return removed

    def _reserve(self, keys, args):
        # KEYS[4..6] 三个 freeze 先检查；这里没置冻结，直接放行。
        conc_key = keys[0]
        now = float(args[0])
        self.concurrency.setdefault(conc_key, {})
        # 冻结键在 fake 里恒不存在，跳过。
        rid = args[3]
        token_amount = int(args[13])
        allow_overflow = int(args[24]) == 1
        grace = int(args[25])

        def projected(key, amount):
            old = self._hget(key, rid)
            old_n = int(old) if old is not None else 0
            total = int(self._hget(key, "__total") or 0)
            return max(0, total - old_n + amount), old_n

        token_specs = [
            (keys[7], int(args[14]), int(args[15])),
            (keys[8], int(args[16]), int(args[17])),
            (keys[9], int(args[18]), int(args[19])),
            (keys[10], int(args[20]), int(args[21])),
        ]
        if token_amount > 0 and not allow_overflow:
            for key, limit, _ttl in token_specs:
                if limit > 0:
                    proj, _ = projected(key, token_amount)
                    if proj >= limit:
                        return [0, "token_limit", 30, 0, 0, 0, 0, 0]

        def write(key, amount, ttl):
            proj, _old = projected(key, amount)
            self._hset(key, {rid: str(amount), f"{rid}:state": "reserved", "__total": str(proj)})
            if key not in self.store.get("__ttl_seen", {key}):
                pass

        rpm = rpd = rph = 0
        if int(args[4]) > 0:
            write(keys[1], 1, int(args[5]))
            rpm = int(self._hget(keys[1], "__total"))
        if int(args[6]) > 0:
            write(keys[2], 1, int(args[7]))
            rpd = int(self._hget(keys[2], "__total"))
        if int(args[8]) > 0:
            write(keys[6], 1, int(args[9]))
            rph = int(self._hget(keys[6], "__total"))
        token_used = [0, 0, 0, 0]
        if token_amount > 0:
            for i, (key, limit, _ttl) in enumerate(token_specs):
                if limit > 0:
                    write(key, token_amount, 0)
                    token_used[i] = int(self._hget(key, "__total"))
        self.concurrency[conc_key][rid] = now + float(args[2])
        return [1, "", 0, rpm, rpd, 0, now + float(args[2]), rph,
                token_used[0], token_used[1], token_used[2], token_used[3]]

    def _rollback(self, keys, args):
        rid = args[0]
        removed = 0
        for key in keys:
            old = self._hget(key, rid)
            if old is None:
                continue
            total = int(self._hget(key, "__total") or 0)
            self._hset(key, {"__total": str(max(0, total - int(old)))})
            self._hdel(key, rid, f"{rid}:state")
            removed += 1
        # 真实 Redis 对 Lua 的单个数字返回是整数（不是数组），与生产解析 int(result) 对齐。
        return removed

    def _reconcile(self, keys, args):
        rid = args[0]
        actual = int(args[1])
        metric_count = int(args[2])
        account_freeze_enabled = int(args[3]) == 1
        model_freeze_enabled = int(args[4]) == 1
        limits = [int(x) for x in args[5:]]
        out = []
        account_freeze_ttl = 0
        for i in range(metric_count):
            key = keys[i]
            if key not in self.store:
                out.append(-1)
                continue
            old = self._hget(key, rid)
            old_n = int(old) if old is not None else 0
            total = int(self._hget(key, "__total") or 0)
            new_total = max(0, total - old_n + actual)
            self._hset(key, {rid: str(actual), f"{rid}:state": "committed", "__total": str(new_total)})
            out.append(new_total)
            limit = limits[i] if i < len(limits) else 0
            if limit > 0 and new_total >= limit:
                # fake 里 TTL 恒定为 30，够验证冻结分支。
                if i == 3:
                    if model_freeze_enabled:
                        self.frozen[keys[metric_count + 1]] = ("reservation:model_tpm", 30)
                elif 30 > account_freeze_ttl:
                    account_freeze_ttl = 30
        if account_freeze_ttl > 0 and account_freeze_enabled:
            self.frozen[keys[metric_count]] = ("reservation:tpm", account_freeze_ttl)
        model_ttl = 30 if (model_freeze_enabled and keys[metric_count + 1] in self.frozen) else 0
        # 生产 Lua 在末两位返回 account/model freeze TTL。
        return [*out, account_freeze_ttl, model_ttl]

    async def zrem(self, key, member):
        return 1 if self.concurrency.get(key, {}).pop(member, None) is not None else 0


def _make_reservation(redis, allow_overflow=True, token_amount=100):
    return asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "lease-x",
        freeze_key="limit:cooldown:p1:u1",
        model_freeze_key="limit:model-tpm-freeze:m1",
        account_model_freeze_key="limit:cooldown:p1:u1:model:m1",
        model_id="m1", token_amount=token_amount,
        tpm_limit=1000, tph_limit=0, tpd_limit=0, model_tpm_limit=5000,
        allow_token_overflow=allow_overflow,
    ))


def test_rollback_removes_request_and_token_together(monkeypatch):
    fake = _LedgerRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: fake))
    wids = RedisLimitBackend._reservation_window_ids(time.time())
    asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "lease-x",
        freeze_key="f", model_freeze_key="mf", account_model_freeze_key="amf",
        rpm_limit=10, model_id="m1", token_amount=100, tpm_limit=1000,
        allow_token_overflow=True,
    ))
    acc = RedisLimitBackend._account_ledger_keys("account:p1:u1", wids)
    assert int(fake._hget(acc["rpm"], "__total")) == 1
    assert int(fake._hget(acc["tpm"], "__total")) == 100

    rollback = asyncio.run(RedisLimitBackend.rollback_account_reservation(
        "account:p1:u1", "m1", "lease-x", window_ids=wids,
    ))
    # 请求数与 token 一并退回，且不为负。
    assert rollback["removed"] >= 2
    assert int(fake._hget(acc["rpm"], "__total")) == 0
    assert int(fake._hget(acc["tpm"], "__total")) == 0


def test_rollback_isolates_by_reservation_id(monkeypatch):
    fake = _LedgerRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: fake))
    # 两次预占用不同 reservation_id，回退 A 不影响 B 的 token 总量。
    asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "A",
        freeze_key="f", model_freeze_key="mf", account_model_freeze_key="amf",
        model_id="m1", token_amount=100, tpm_limit=1000, allow_token_overflow=True,
    ))
    asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "B",
        freeze_key="f", model_freeze_key="mf", account_model_freeze_key="amf",
        model_id="m1", token_amount=100, tpm_limit=1000, allow_token_overflow=True,
    ))
    wids = RedisLimitBackend._reservation_window_ids(time.time())
    tpm_key = RedisLimitBackend._account_ledger_keys("account:p1:u1", wids)["tpm"]
    assert int(fake._hget(tpm_key, "__total")) == 200

    asyncio.run(RedisLimitBackend.rollback_account_reservation(
        "account:p1:u1", "m1", "A", window_ids=wids,
    ))
    # A 的 100 退掉，B 的 100 留下
    assert int(fake._hget(tpm_key, "__total")) == 100
    assert fake._hget(tpm_key, "A") is None
    assert fake._hget(tpm_key, "B") == "100"


def test_rollback_idempotent_and_missing_is_noop(monkeypatch):
    fake = _LedgerRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: fake))
    wids = RedisLimitBackend._reservation_window_ids(time.time())
    # 从未预占：回退 no-op，不产生负数。
    r = asyncio.run(RedisLimitBackend.rollback_account_reservation(
        "account:p1:u1", "m1", "never", window_ids=wids,
    ))
    assert r["removed"] == 0
    tpm_key = RedisLimitBackend._account_ledger_keys("account:p1:u1", wids)["tpm"]
    assert fake.store.get(tpm_key) is None


def test_reconcile_replaces_reserved_with_actual(monkeypatch):
    fake = _LedgerRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: fake))
    wids = RedisLimitBackend._reservation_window_ids(time.time())
    asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "R",
        freeze_key="f", model_freeze_key="mf", account_model_freeze_key="amf",
        model_id="m1", token_amount=100, tpm_limit=1000, model_tpm_limit=5000,
        allow_token_overflow=True,
    ))
    # 实际只用了 30：补差退掉 70
    asyncio.run(RedisLimitBackend.reconcile_account_reservation(
        "account:p1:u1", "m1", "R", 30,
        account_tpm_limit=1000, model_tpm_limit=5000,
        account_freeze_key="f", model_freeze_key="mf", window_ids=wids,
    ))
    tpm_key = RedisLimitBackend._account_ledger_keys("account:p1:u1", wids)["tpm"]
    model_key = RedisLimitBackend._model_ledger_key("m1", wids["model_tpm"])
    assert int(fake._hget(tpm_key, "__total")) == 30
    assert int(fake._hget(model_key, "__total")) == 30
    assert fake._hget(tpm_key, "R") == "30"
    assert fake._hget(tpm_key, "R:state") == "committed"

    # 重复 reconcile 相同 actual：delta=0，幂等
    asyncio.run(RedisLimitBackend.reconcile_account_reservation(
        "account:p1:u1", "m1", "R", 30,
        account_tpm_limit=1000, model_tpm_limit=5000,
        account_freeze_key="f", model_freeze_key="mf", window_ids=wids,
    ))
    assert int(fake._hget(tpm_key, "__total")) == 30


def test_reconcile_actual_above_reserved_top_up(monkeypatch):
    fake = _LedgerRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: fake))
    wids = RedisLimitBackend._reservation_window_ids(time.time())
    asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "R",
        freeze_key="f", model_freeze_key="mf", account_model_freeze_key="amf",
        model_id="m1", token_amount=100, tpm_limit=10000, allow_token_overflow=True,
    ))
    # 实际用了 250：补差加 150
    asyncio.run(RedisLimitBackend.reconcile_account_reservation(
        "account:p1:u1", "m1", "R", 250 , window_ids=wids,
    ))
    tpm_key = RedisLimitBackend._account_ledger_keys("account:p1:u1", wids)["tpm"]
    assert int(fake._hget(tpm_key, "__total")) == 250


def test_reconcile_freezes_when_actual_pushes_over_limit(monkeypatch):
    """补差后超限不能拒绝（已服务），但必须写 reservation 来源的冻结挡住后续请求。"""
    fake = _LedgerRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: fake))
    wids = RedisLimitBackend._reservation_window_ids(time.time())
    asyncio.run(RedisLimitBackend.reserve_account_limits(
        "account:p1:u1", "R",
        freeze_key="f", model_freeze_key="mf", account_model_freeze_key="amf",
        model_id="m1", token_amount=100, tpm_limit=1000, model_tpm_limit=5000,
        allow_token_overflow=True,
    ))
    # 真实用量 1200 > tpm_limit 1000：补差写入，同时冻结账号后续请求。
    result = asyncio.run(RedisLimitBackend.reconcile_account_reservation(
        "account:p1:u1", "m1", "R", 1200,
        account_tpm_limit=1000, model_tpm_limit=5000,
        account_freeze_key="f", model_freeze_key="mf", window_ids=wids,
    ))
    assert result["used"]["account_tpm"] == 1200
    # 账号冻结来源标记为 reservation，便于与上游 freeze_policy 区分。
    assert fake.frozen["f"][0] == "reservation:tpm"
    # 模型 TPM 未触线（1200 < 5000），不冻结模型
    assert "mf" not in fake.frozen


def test_tpm_lua_writes_freeze_before_return(monkeypatch):
    captured = {}

    class FakeRedis:
        prefix_key = "test"

        async def eval(self, script, keys=None, args=None):
            captured["script"] = script
            return [100, 40, 200, 40]

    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: FakeRedis()))
    result = asyncio.run(RedisLimitBackend.record_tpm_and_freeze(
        account_key="account:p1:u1", model_id="m1", tokens=50, ttl_seconds=60,
        account_limit=100, model_limit=200,
        account_freeze_key="limit:cooldown:p1:u1",
        model_freeze_key="limit:model-tpm-freeze:m1",
    ))

    assert "redis.call('SET', KEYS[3], 'tpm', 'EX', account_ttl)" in captured["script"]
    assert "redis.call('SET', KEYS[4], 'tpm', 'EX', model_ttl)" in captured["script"]
    assert result["account_used"] == 100
    assert result["model_used"] == 200
