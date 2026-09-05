"""Unified limit manager used by account routing and provider quotas."""

from __future__ import annotations

import asyncio
import contextvars
import time
import uuid
from datetime import datetime, timedelta

from loguru import logger

from .backend import RedisLimitBackend
from .rules import LimitDecision, LimitLease, LimitReservation, LimitSubject


ROUTING_AUX_REDIS_TIMEOUT_SECONDS = 0.25
ROUTING_RESERVE_REDIS_TIMEOUT_SECONDS = 1.0
ACCOUNT_CONCURRENCY_LEASE_SECONDS = 900
UNCERTAIN_RESERVATION_CLEANUP_DELAYS = (0.25, 1.0, 3.0)
# 可降级辅助 Redis 操作（候选可靠性 / API-key 流量 / 后台扫描）的并发上限。
# 达到上限立即走中性值降级，不进共享池排队——避免辅助读把连接占满、饿死账号原子
# 预占与租约释放这类一致性操作。一致性路径绝不走这个 bulkhead。
ROUTING_AUX_REDIS_MAX_CONCURRENCY = 32
_aux_redis_semaphore: asyncio.Semaphore | None = None
_aux_redis_rejected = 0


def aux_redis_semaphore() -> asyncio.Semaphore:
    """惰性创建辅助 Redis 并发闸（绑定当前事件循环）。"""
    global _aux_redis_semaphore
    if _aux_redis_semaphore is None:
        _aux_redis_semaphore = asyncio.Semaphore(ROUTING_AUX_REDIS_MAX_CONCURRENCY)
    return _aux_redis_semaphore


def aux_redis_rejected_count() -> int:
    return _aux_redis_rejected


async def aux_redis_call(awaitable, *, stage: str, timeout: float = ROUTING_AUX_REDIS_TIMEOUT_SECONDS, default=None):
    """辅助 Redis 调用：先过并发闸，再走既有硬截止。

    闸满即刻降级（返回 ``default``），并把传入 awaitable 正确关闭，避免
    "coroutine was never awaited" 告警与悬挂任务。
    """
    global _aux_redis_rejected
    sem = aux_redis_semaphore()
    # locked() 为真即当前无可用许可。检查与下面的 async with 之间无 await，
    # 单线程事件循环里许可数不会被其它协程改动，故这里判定是可靠的即时拒绝。
    if sem.locked():
        _aux_redis_rejected += 1
        if hasattr(awaitable, "cancel"):
            awaitable.cancel()
            if hasattr(awaitable, "add_done_callback"):
                awaitable.add_done_callback(_consume_cancelled_task)
        elif asyncio.iscoroutine(awaitable):
            awaitable.close()
        _mark_routing_redis_degraded(stage, 0)
        return default
    async with sem:
        return await routing_redis_call(
            awaitable, stage=stage, timeout=timeout, default=default
        )

_PROCESS_BOOT_ID = uuid.uuid4().hex
_routing_redis_state: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "routing_redis_state", default=None
)


def begin_routing_redis_scope(timing: dict | None = None, *, token_estimate: int = 0):
    """建立一次请求选路的 Redis 降级上下文；递归调用自动继承。

    ``token_estimate`` 是本次请求的 token 预占量（请求级常量，选路前由主链路算好），
    ``_try_reserve`` 从这里读取并传给账号原子预占；不进函数签名，避免 4 个选路分支
    逐个穿透。
    """
    state = {
        "degraded": False,
        "timeout_stage": "",
        "reserve_uncertain": False,
        "timing": timing,
        # 仅在本次请求选路作用域内复用可降级辅助读，避免模型组/重试重复访问 Redis。
        # 不跨请求共享，因而不会把 RPM/可靠性值长期缓存为陈旧数据。
        "cache": {},
        "token_estimate": int(token_estimate or 0),
    }
    return _routing_redis_state.set(state)


def end_routing_redis_scope(token) -> None:
    _routing_redis_state.reset(token)


def routing_redis_state() -> dict | None:
    return _routing_redis_state.get()


def _mark_routing_redis_degraded(stage: str, elapsed_ms: int) -> None:
    state = _routing_redis_state.get()
    if state is None:
        return
    state["degraded"] = True
    if not state["timeout_stage"]:
        state["timeout_stage"] = stage
    timing = state.get("timing")
    if isinstance(timing, dict):
        timing["routing_redis_degraded"] = True
        timing.setdefault("redis_timeout_stage", stage)
        timing.setdefault("redis_timeout_ms", elapsed_ms)


def _consume_cancelled_task(task: asyncio.Future) -> None:
    """回收硬截止后后台结束的任务异常，避免未获取异常告警。"""
    try:
        task.exception()
    except (asyncio.CancelledError, Exception):
        pass


# 诊断告警限频：故障期间同一 stage 每秒最多打一条，避免日志 I/O 反过来加重事件循环压力。
_REDIS_WARN_INTERVAL_SECONDS = 1.0
_redis_warn_last: dict[str, float] = {}
_redis_warn_suppressed: dict[str, int] = {}


async def _measure_loop_lag() -> float:
    """测量事件循环调度延迟（毫秒）：sleep(0) 的实际耗时即当前 loop 排队滞后。

    用于区分「Redis 真慢」与「整个 asyncio loop 被阻塞」——后者会让所有基于
    asyncio.wait 的业务截止一起虚高（250ms 截止却记录到 1-2s 就是这种情况）。
    """
    started = time.monotonic()
    await asyncio.sleep(0)
    return (time.monotonic() - started) * 1000


def _classify_redis_failure(exc: BaseException | None) -> str:
    """把 Redis 失败归类，便于定位是等连接、等回包还是连接本身出问题。"""
    if exc is None:
        return "deadline"
    name = type(exc).__name__
    if isinstance(exc, asyncio.TimeoutError) or name in ("TimeoutError", "CommandSyntaxError"):
        return "command_timeout"
    if name in ("ConnectionError", "ConnectionAbortedError", "ConnectionResetError", "RedisConnectionError"):
        return "connection_error"
    if "Pool" in name or "pool" in str(exc).lower():
        return "pool_timeout"
    return name


def _log_redis_degraded(stage: str, elapsed_ms: int, kind: str, loop_lag_ms: float, exc: BaseException | None = None) -> None:
    """限频输出带池状态/循环延迟/失败分类的诊断日志。"""
    now = time.monotonic()
    last = _redis_warn_last.get(stage, 0.0)
    if now - last < _REDIS_WARN_INTERVAL_SECONDS:
        _redis_warn_suppressed[stage] = _redis_warn_suppressed.get(stage, 0) + 1
        return
    _redis_warn_last[stage] = now
    suppressed = _redis_warn_suppressed.pop(stage, 0)
    try:
        from rd import JdbcClient
        pool = JdbcClient.pool_snapshot()
    except Exception:
        pool = {}
    suffix = f" suppressed={suppressed}" if suppressed else ""
    error_part = f" error={exc}" if exc is not None else ""
    # available/waiting 是判读的关键：in_use 只统计 lease 出去的连接，测不出建连接失败
    # 丢掉的槽。available=0 而 in_use 低 → 容量被泄漏；available>0 却仍超时 → 看 loop_lag。
    logger.warning(
        "[routing] Redis {} stage={} elapsed={}ms loop_lag={:.1f}ms "
        "pool_in_use={}/{} available={} waiting={} active={}{}{}",
        kind, stage, elapsed_ms, loop_lag_ms,
        pool.get("in_use"), pool.get("max"),
        pool.get("available"), pool.get("waiting"), pool.get("active"),
        error_part, suffix,
    )


async def routing_redis_call(
    awaitable,
    *,
    stage: str,
    timeout: float = ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
    default=None,
    skip_when_degraded: bool = True,
):
    """给选路 Redis 操作加硬业务截止；到点立即失败开放，不等待驱动完成取消。"""
    state = _routing_redis_state.get()
    if skip_when_degraded and state and state.get("degraded"):
        if hasattr(awaitable, "cancel"):
            awaitable.cancel()
            if hasattr(awaitable, "add_done_callback"):
                awaitable.add_done_callback(_consume_cancelled_task)
        elif asyncio.iscoroutine(awaitable):
            awaitable.close()
        return default

    started = time.monotonic()
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            elapsed_ms = max(1, int((time.monotonic() - started) * 1000))
            _mark_routing_redis_degraded(stage, elapsed_ms)
            task.cancel()
            task.add_done_callback(_consume_cancelled_task)
            loop_lag_ms = await _measure_loop_lag()
            _log_redis_degraded(stage, elapsed_ms, "deadline", loop_lag_ms)
            return default
        return task.result()
    except asyncio.CancelledError:
        task.cancel()
        task.add_done_callback(_consume_cancelled_task)
        raise
    except Exception as exc:
        elapsed_ms = max(1, int((time.monotonic() - started) * 1000))
        _mark_routing_redis_degraded(stage, elapsed_ms)
        loop_lag_ms = await _measure_loop_lag()
        _log_redis_degraded(stage, elapsed_ms, _classify_redis_failure(exc), loop_lag_ms, exc)
        return default


def _seconds_until_day_end(now: float | None = None) -> int:
    """到本地自然日结束的秒数（rpd 固定窗口的 TTL）。"""
    dt = datetime.fromtimestamp(now or time.time())
    tomorrow = (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((tomorrow - dt).total_seconds()))


def _freeze_account_local_and_broadcast(account_client, seconds: int, reason: str) -> None:
    """Lua 已写 Redis 冻结 Key；这里只更新本机镜像并广播其它实例。"""
    if seconds <= 0:
        return
    provider = account_client.provider
    provider_name = getattr(provider, "PROVIDER_NAME", "")
    username = getattr(provider, "username", "")
    account_client.freeze(kind="account", seconds=seconds, reason=reason)
    try:
        import runtime_sync
        asyncio.create_task(runtime_sync.publish(
            runtime_sync.EVENT_COOLDOWN,
            f"{provider_name}:{username}",
            extra={"provider": provider_name, "username": username,
                   "until": time.time() + seconds, "reason": reason},
        ))
    except Exception:
        pass
    logger.warning(f"[{provider_name}] 账号 {username} 触线冻结 {seconds}s: {reason}")


def _apply_account_freeze(account_client, seconds: int, reason: str) -> None:
    """触线冻结：写内存桶 + 异步写 Redis 兜底 + pubsub 广播。选账号只看内存布尔。"""
    if seconds <= 0:
        return
    provider = account_client.provider
    provider_name = getattr(provider, "PROVIDER_NAME", "")
    username = getattr(provider, "username", "")
    account_client.freeze(kind="account", seconds=seconds, reason=reason)
    try:
        asyncio.create_task(RedisLimitBackend.set_account_cooldown(provider_name, username, seconds, reason))
    except RuntimeError:
        pass
    try:
        import runtime_sync
        asyncio.create_task(runtime_sync.publish(
            runtime_sync.EVENT_COOLDOWN,
            f"{provider_name}:{username}",
            extra={"provider": provider_name, "username": username,
                   "until": time.time() + seconds, "reason": reason},
        ))
    except Exception:
        pass
    logger.warning(f"[{provider_name}] 账号 {username} 触线冻结 {seconds}s: {reason}")


class LimitManager:
    """Central runtime limit checks.

    触线即冻结语义：选账号/预占阶段不读 rpm/rpd/tpm 计数——计数在请求发生时（rpm/rpd）
    与 token 回写时（tpm）累加进 Redis 固定窗口，越线即给账号打 cooldown（时长=窗口剩余 TTL）。
    并发是唯一的预占期硬校验（Redis 原子占用）。
    """

    REQUEST_WINDOW_SECONDS = 60
    TOKEN_WINDOW_SECONDS = 60
    # 到量冻结维度：每小时次数 / 每小时 tokens / 每天 tokens（触线即冻结账号到窗口末）
    REQUEST_HOUR_SECONDS = 3600
    TOKEN_HOUR_SECONDS = 3600
    TOKEN_DAY_SECONDS = 86400
    CONCURRENCY_LEASE_SECONDS = ACCOUNT_CONCURRENCY_LEASE_SECONDS
    _active_leases: dict[str, LimitLease] = {}
    _cleanup_tasks: set[asyncio.Task] = set()
    _renewal_tasks: set[asyncio.Task] = set()
    _heartbeat_task: asyncio.Task | None = None
    HEARTBEAT_INTERVAL_SECONDS = 20
    HEARTBEAT_TTL_SECONDS = 60

    @classmethod
    async def start_heartbeat(cls) -> None:
        """先写一次心跳，再启动续期；Redis 不可用时安全降级，不阻断主服务。"""
        await RedisLimitBackend.set_process_heartbeat(
            _PROCESS_BOOT_ID, ttl_seconds=cls.HEARTBEAT_TTL_SECONDS,
        )
        if cls._heartbeat_task and not cls._heartbeat_task.done():
            return
        cls._heartbeat_task = asyncio.create_task(
            cls._heartbeat_loop(), name="limit-process-heartbeat",
        )

    @classmethod
    async def _heartbeat_loop(cls) -> None:
        while True:
            try:
                await asyncio.sleep(cls.HEARTBEAT_INTERVAL_SECONDS)
                await RedisLimitBackend.set_process_heartbeat(
                    _PROCESS_BOOT_ID, ttl_seconds=cls.HEARTBEAT_TTL_SECONDS,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[limit-heartbeat] refresh failed")

    @classmethod
    async def reap_orphan_reservations(cls) -> int:
        """回滚心跳失效进程留下的预占；当前进程 boot_id 永远视为存活。"""
        reaped = await RedisLimitBackend.reap_orphan_reservations(
            current_boot_id=_PROCESS_BOOT_ID,
        )
        if reaped:
            logger.warning("[limit-reservation] reaped {} orphan reservations", reaped)
        return reaped

    @classmethod
    def _record_reservation_denial(cls, reason: str) -> None:
        state = routing_redis_state()
        timing = state.get("timing") if state else None
        if not isinstance(timing, dict):
            return
        denials = timing.setdefault("reservation_denials", {})
        denials[reason] = int(denials.get(reason) or 0) + 1

    @classmethod
    def account_subject(cls, account_client, model_id: str) -> LimitSubject:
        provider = account_client.provider
        return LimitSubject(
            provider=getattr(provider, "PROVIDER_NAME", ""),
            account=getattr(provider, "username", ""),
            model=model_id,
            routed_model=model_id,
        )

    @classmethod
    async def account_available_locked(
        cls,
        account_client,
        model_id: str | None = None,
        messages: list[dict] | None = None,
        now: float | None = None,
        *,
        is_test: bool = False,
    ) -> LimitDecision:
        now = now or time.time()
        provider = account_client.provider

        # 测试（is_test）请求：跳过账号/渠道的硬禁用与冷却，令其可探测可达性。
        if not is_test:
            if account_client.disabled:
                return LimitDecision(False, "account_disabled", details={"message": account_client.disable_reason or "account disabled"})
            if account_client.is_frozen:
                # 账号级冻结：临时冷却带 retry_after，永久冻结不带。
                remaining = account_client.cooldown_remaining(now)
                if remaining > 0:
                    return LimitDecision(False, "account_cooldown", remaining)
                return LimitDecision(False, "account_frozen")
            # 渠道禁用兜底：候选收集后状态可能变化，正式发送 reserve 阶段再确认一次渠道 enabled。
            channel = getattr(account_client, "channel", None)
            if channel is not None and not getattr(channel, "enabled", True):
                return LimitDecision(False, "channel_disabled")
        if not provider.is_init():
            return LimitDecision(False, "provider_not_initialized")
        if not is_test:
            if model_id and account_client.is_model_frozen(model_id):
                return LimitDecision(False, "account_model_cooldown")

            if hasattr(account_client, "sync_provider_quota"):
                account_client.sync_provider_quota()
            if getattr(account_client, "_provider_quota_disabled_until", 0) > now:
                return LimitDecision(False, "provider_quota_cooldown")
            daily_remaining = getattr(account_client, "_provider_daily_remaining", None)
            if daily_remaining is not None and daily_remaining <= 0:
                return LimitDecision(False, "provider_daily_quota_exhausted")

        # 本地窗口只作评分信号维护（不参与限流判定）
        account_client._requests = [t for t in account_client._requests if now - t < cls.REQUEST_WINDOW_SECONDS]
        account_client._token_usages = [
            (t, n) for t, n in account_client._token_usages if now - t < cls.TOKEN_WINDOW_SECONDS
        ]

        quota = getattr(provider, "quotas", None)
        if quota is not None and hasattr(quota, "dimensions"):
            for dim in quota.dimensions():
                if dim.is_exhausted():
                    return LimitDecision(False, "provider_quota_exhausted", details={"dimension": dim.name})

        if model_id and not await provider.check_message(model_id, messages):
            return LimitDecision(False, "provider_message_quota_exceeded")

        return LimitDecision(True)

    @classmethod
    async def acquire_account(
        cls,
        account_client,
        model_id: str,
        messages: list[dict] | None = None,
        now: float | None = None,
        *,
        is_test: bool = False,
        token_estimate: int = 0,
    ) -> LimitReservation:
        now = now or time.time()
        decision = await cls.account_available_locked(account_client, model_id, messages, now, is_test=is_test)
        if not decision.allowed:
            return LimitReservation(None, decision)

        subject = cls.account_subject(account_client, model_id)
        lease = LimitLease(
            subject=subject,
            lease_id=f"lease:{_PROCESS_BOOT_ID}:{uuid.uuid4().hex}",
            expires_at=now + cls.CONCURRENCY_LEASE_SECONDS,
        )
        lease.reservation_id = lease.lease_id
        rpm_limit = int(getattr(account_client, "rpm_limit", 0) or 0)
        rpd_limit = int(getattr(account_client, "rpd_limit", 0) or 0)
        rph_limit = int(getattr(account_client, "rph_limit", 0) or 0)
        tpm_limit = int(getattr(account_client, "tpm_limit", 0) or 0)
        tph_limit = int(getattr(account_client, "tph_limit", 0) or 0)
        tpd_limit = int(getattr(account_client, "tpd_limit", 0) or 0)
        model_tpm_limit = cls._model_tpm_limit(model_id)
        token_amount = max(0, int(token_estimate or 0))
        has_token_limits = tpm_limit > 0 or tph_limit > 0 or tpd_limit > 0 or model_tpm_limit > 0
        has_remote_limits = (
            account_client.concurrent_limit > 0
            or rpm_limit > 0 or rpd_limit > 0 or rph_limit > 0
            or (token_amount > 0 and has_token_limits)
        )

        state = routing_redis_state()
        if account_client.concurrent_limit > 0 and state and state.get("reserve_uncertain"):
            cls._record_reservation_denial("redis_uncertain")
            return LimitReservation(None, LimitDecision(
                False,
                "redis_uncertain",
                retry_after=1,
                details={"stage": "account_atomic_reserve", "skipped": True},
            ))

        if has_remote_limits:
            window_ids = RedisLimitBackend._reservation_window_ids(now)
            allow_token_overflow = cls._allow_token_overflow()
            reserve_result = await routing_redis_call(
                RedisLimitBackend.reserve_account_limits(
                    subject.account_key,
                    lease.lease_id,
                    freeze_key=f"limit:cooldown:{subject.provider}:{subject.account}",
                    model_freeze_key=f"limit:model-tpm-freeze:{model_id}",
                    account_model_freeze_key=f"limit:cooldown:{subject.provider}:{subject.account}:model:{model_id}",
                    concurrent_limit=account_client.concurrent_limit,
                    concurrency_ttl=cls.CONCURRENCY_LEASE_SECONDS,
                    rpm_limit=rpm_limit,
                    rpm_ttl=cls.REQUEST_WINDOW_SECONDS,
                    rpd_limit=rpd_limit,
                    rpd_ttl=_seconds_until_day_end(now),
                    rph_limit=rph_limit,
                    rph_ttl=cls.REQUEST_HOUR_SECONDS,
                    model_id=model_id,
                    token_amount=token_amount,
                    tpm_limit=tpm_limit,
                    tpm_ttl=cls.TOKEN_WINDOW_SECONDS,
                    tph_limit=tph_limit,
                    tph_ttl=cls.TOKEN_HOUR_SECONDS,
                    tpd_limit=tpd_limit,
                    tpd_ttl=_seconds_until_day_end(now),
                    model_tpm_limit=model_tpm_limit,
                    model_tpm_ttl=cls.TOKEN_WINDOW_SECONDS,
                    allow_token_overflow=allow_token_overflow,
                    window_ids=window_ids,
                ),
                stage="account_atomic_reserve",
                timeout=ROUTING_RESERVE_REDIS_TIMEOUT_SECONDS,
                default=None,
                skip_when_degraded=False,
            )
            if reserve_result is not None:
                if not reserve_result.get("allowed"):
                    backend_reason = reserve_result.get("reason") or "limited"
                    reason = {
                        "frozen": "account_frozen",
                        "model_frozen": "model_frozen",
                        "account_model_frozen": "account_model_frozen",
                        "concurrent": "concurrent_limit",
                        "token_limit": "token_limit",
                    }.get(backend_reason, backend_reason)
                    ttl = int(reserve_result.get("ttl") or 0)
                    if backend_reason == "frozen" and ttl > 0:
                        account_client.freeze(kind="account", seconds=ttl, reason="account limit cooling down")
                    elif backend_reason == "model_frozen" and ttl > 0:
                        try:
                            from rate_limiter import ModelClientPool
                            ModelClientPool.apply_model_tpm_cooldown_event(model_id, time.time() + ttl)
                        except ImportError:
                            pass
                    elif backend_reason == "account_model_frozen" and ttl > 0:
                        account_client.freeze(
                            kind="account_model",
                            model_id=model_id,
                            seconds=ttl,
                            reason="account limit cooling down",
                        )
                    cls._record_reservation_denial(reason)
                    return LimitReservation(None, LimitDecision(
                        False,
                        reason,
                        retry_after=ttl or int(reserve_result.get("retry_after") or 0) or None,
                        details={
                            "current": int(reserve_result.get("concurrent_used") or 0),
                            "limit": account_client.concurrent_limit,
                            "scope": "account_model" if backend_reason == "account_model_frozen" else "account",
                        },
                    ))
                lease.concurrency_acquired = account_client.concurrent_limit > 0
                lease.window_ids = dict(reserve_result.get("window_ids") or window_ids)
                lease.reserved_amounts = {
                    "rpm": 1 if rpm_limit > 0 else 0,
                    "rph": 1 if rph_limit > 0 else 0,
                    "rpd": 1 if rpd_limit > 0 else 0,
                    "tokens": token_amount if has_token_limits else 0,
                    "tpm_limit": tpm_limit,
                    "tph_limit": tph_limit,
                    "tpd_limit": tpd_limit,
                    "model_tpm_limit": model_tpm_limit,
                }
                confirmed_expiry = float(reserve_result.get("lease_expires_at") or 0)
                if confirmed_expiry > 0:
                    lease.expires_at = confirmed_expiry
                if lease.concurrency_acquired:
                    cls._active_leases[lease.lease_id] = lease
                    cls._start_lease_renewal(lease)
                freeze_reason = reserve_result.get("reason") or ""
                freeze_ttl = int(reserve_result.get("ttl") or 0)
                if freeze_reason and freeze_ttl > 0:
                    _freeze_account_local_and_broadcast(account_client, freeze_ttl, f"{freeze_reason} 触线")
                # 模型 TPM 是跨账号共享窗口：Lua 已写 Redis 冻结，这里同步本机镜像 + 广播，
                # 否则本进程选账号只读内存镜像会漏判，反复选中已触线的模型。
                model_freeze_ttl = int(reserve_result.get("model_tpm_freeze_ttl") or 0)
                if model_freeze_ttl > 0:
                    cls._apply_model_tpm_freeze(model_id, model_freeze_ttl,
                                                int(reserve_result.get("model_tpm") or 0))
            else:
                if account_client.concurrent_limit > 0:
                    if state is not None:
                        state["reserve_uncertain"] = True
                        timing = state.get("timing")
                        if isinstance(timing, dict):
                            timing["account_reservation_uncertain"] = True
                    cls._schedule_failed_reservation_cleanup(
                        subject.account_key, lease.lease_id, model_id, window_ids
                    )
                    cls._record_reservation_denial("redis_uncertain")
                    return LimitReservation(None, LimitDecision(
                        False,
                        "redis_uncertain",
                        retry_after=1,
                        details={"stage": "account_atomic_reserve", "cleanup_scheduled": True},
                    ))
                lease.local_fallback = True

        return LimitReservation(lease, LimitDecision(True))

    @classmethod
    def _apply_model_tpm_freeze(cls, model_id: str, ttl_seconds: int, used: int = 0) -> None:
        """模型 TPM 触线：更新本机冷却镜像并广播（Redis 冻结键已由 Lua 写入）。

        与 ModelClientPool.record_token_usage 的模型冻结口径一致：选账号只读内存镜像，
        Redis 是跨重启/跨实例的真相源。
        """
        if not model_id or ttl_seconds <= 0:
            return
        until = time.time() + ttl_seconds
        try:
            from rate_limiter import ModelClientPool
            ModelClientPool.apply_model_tpm_cooldown_event(model_id, until)
        except ImportError:
            return
        try:
            import runtime_sync
            asyncio.create_task(runtime_sync.publish(
                runtime_sync.EVENT_COOLDOWN,
                f"model-tpm:{model_id}",
                extra={
                    "scope": "model_tpm",
                    "model_id": model_id,
                    "until": until,
                    "used": used,
                },
            ))
        except Exception:
            pass

    @classmethod
    def _allow_token_overflow(cls) -> bool:
        """读全局「允许 token 预占触线放行」开关（内存缓存，无 IO）；读不到按放行。"""
        try:
            import config
            return config.Config.allow_token_reservation_overflow()
        except Exception:
            return True

    @classmethod
    def _model_tpm_limit(cls, model_id: str) -> int:
        """模型级 TPM 限额（与选路侧同源）；取不到按 0（不限）。"""
        if not model_id:
            return 0
        try:
            from rate_limiter import ModelClientPool
            return int(ModelClientPool._model_tpm_limit(model_id) or 0)
        except Exception:
            return 0

    @classmethod
    async def reconcile_account_reservation(cls, lease: LimitLease | None, actual_tokens: int) -> dict | None:
        """成功结算：把 token 预占量替换成真实用量（多退少补），并按结算后用量补冻结。

        请求数计数保持 1 不变（成功请求就该占 1 次）。幂等：同一 lease 重复结算 delta=0。
        """
        if lease is None or lease.settled or lease.rolled_back:
            return None
        reservation_id = lease.reservation_id or lease.lease_id
        if not reservation_id or lease.local_fallback:
            lease.settled = True
            return None
        if not lease.reserved_amounts.get("tokens") and actual_tokens <= 0:
            await RedisLimitBackend.finalize_reservation(reservation_id)
            lease.settled = True
            return None
        subject = lease.subject
        amounts = lease.reserved_amounts or {}
        model_for_reconcile = subject.routed_model or subject.model
        result = await routing_redis_call(
            RedisLimitBackend.reconcile_account_reservation(
                subject.account_key,
                model_for_reconcile,
                reservation_id,
                max(0, int(actual_tokens or 0)),
                account_tpm_limit=int(amounts.get("tpm_limit") or 0),
                account_tph_limit=int(amounts.get("tph_limit") or 0),
                account_tpd_limit=int(amounts.get("tpd_limit") or 0),
                model_tpm_limit=int(amounts.get("model_tpm_limit") or 0),
                tpm_ttl=cls.TOKEN_WINDOW_SECONDS,
                tph_ttl=cls.TOKEN_HOUR_SECONDS,
                tpd_ttl=_seconds_until_day_end(),
                model_tpm_ttl=cls.TOKEN_WINDOW_SECONDS,
                window_ids=lease.window_ids or None,
                account_freeze_key=f"limit:cooldown:{subject.provider}:{subject.account}",
                model_freeze_key=f"limit:model-tpm-freeze:{model_for_reconcile}" if model_for_reconcile else None,
            ),
            stage="account_reservation_reconcile",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
            skip_when_degraded=False,
        )
        if result is not None:
            await RedisLimitBackend.finalize_reservation(reservation_id)
        lease.settled = True
        # 补差后若把模型 TPM 顶过线，同步本机模型冷却镜像 + 广播（Redis 冻结已由 Lua 写入）。
        if isinstance(result, dict) and int(result.get("model_freeze_ttl") or 0) > 0 and model_for_reconcile:
            model_used = 0
            used_map = result.get("used") or {}
            if isinstance(used_map, dict):
                model_used = int(used_map.get("model_tpm") or 0)
            cls._apply_model_tpm_freeze(model_for_reconcile, int(result["model_freeze_ttl"]), model_used)
        return result

    @classmethod
    async def rollback_account_reservation(cls, lease: LimitLease | None) -> dict | None:
        """失败回退：按 reservation_id 精确删除本次请求数与 token 预占。幂等。

        回退是"失败不占额度"的兜底：Redis 超时/降级时立即返回 None，这里再排一个后台重试，
        避免一次瞬时 Redis 抖动就让失败请求把配额占到窗口结束。回退按 reservation_id
        精确匹配、天然幂等，重试不会误删别人的预占。
        """
        if lease is None or lease.rolled_back or lease.settled:
            return None
        reservation_id = lease.reservation_id or lease.lease_id
        lease.rolled_back = True
        if not reservation_id or lease.local_fallback:
            return None
        subject = lease.subject
        model_id = subject.routed_model or subject.model
        window_ids = dict(lease.window_ids or {}) or None
        result = await routing_redis_call(
            RedisLimitBackend.rollback_account_reservation(
                subject.account_key,
                model_id,
                reservation_id,
                window_ids=window_ids,
            ),
            stage="account_reservation_rollback",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
            skip_when_degraded=False,
        )
        if result is None:
            cls._schedule_ledger_rollback_retry(
                subject.account_key, model_id, reservation_id, window_ids
            )
        return result

    @classmethod
    def _schedule_ledger_rollback_retry(
        cls,
        account_key: str,
        model_id: str | None,
        reservation_id: str,
        window_ids: dict[str, str] | None,
    ) -> None:
        try:
            task = asyncio.create_task(
                cls._retry_ledger_rollback(account_key, model_id, reservation_id, window_ids)
            )
        except RuntimeError:
            return
        cls._cleanup_tasks.add(task)
        task.add_done_callback(cls._cleanup_tasks.discard)
        task.add_done_callback(_consume_cancelled_task)

    @classmethod
    async def _retry_ledger_rollback(
        cls,
        account_key: str,
        model_id: str | None,
        reservation_id: str,
        window_ids: dict[str, str] | None,
    ) -> None:
        """Redis 抖动后重试回退；窗口过期后 Lua 自然 no-op，不会扣到新窗口。"""
        for delay in UNCERTAIN_RESERVATION_CLEANUP_DELAYS:
            await asyncio.sleep(delay)
            result = await routing_redis_call(
                RedisLimitBackend.rollback_account_reservation(
                    account_key, model_id, reservation_id, window_ids=window_ids
                ),
                stage="account_reservation_rollback_retry",
                timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
                default=None,
                skip_when_degraded=False,
            )
            if result is not None:
                return

    @classmethod
    async def acquire_account_locked(
        cls,
        account_client,
        model_id: str,
        messages: list[dict] | None = None,
        now: float | None = None,
        *,
        is_test: bool = False,
    ) -> LimitLease | None:
        result = await cls.acquire_account(account_client, model_id, messages, now, is_test=is_test)
        return result.lease

    @classmethod
    def _start_lease_renewal(cls, lease: LimitLease) -> None:
        try:
            task = asyncio.create_task(cls._renew_account_lease(lease))
        except RuntimeError:
            return
        lease.renewal_task = task
        cls._renewal_tasks.add(task)
        task.add_done_callback(cls._renewal_tasks.discard)
        task.add_done_callback(_consume_cancelled_task)

    @classmethod
    async def _renew_account_lease(cls, lease: LimitLease) -> None:
        interval = max(0.01, cls.CONCURRENCY_LEASE_SECONDS / 3)
        while lease.lease_id in cls._active_leases:
            await asyncio.sleep(interval)
            if lease.lease_id not in cls._active_leases:
                return
            expires_at = await routing_redis_call(
                RedisLimitBackend.renew_account_reservation(
                    lease.subject.account_key,
                    lease.lease_id,
                    cls.CONCURRENCY_LEASE_SECONDS,
                ),
                stage="account_concurrency_renew",
                timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
                default=None,
                skip_when_degraded=False,
            )
            if expires_at:
                lease.expires_at = float(expires_at)

    @classmethod
    def _schedule_failed_reservation_cleanup(
        cls,
        account_key: str,
        lease_id: str,
        model_id: str | None = None,
        window_ids: dict[str, str] | None = None,
    ) -> None:
        try:
            task = asyncio.create_task(
                cls._cleanup_failed_reservation(account_key, lease_id, model_id, window_ids)
            )
        except RuntimeError:
            return
        cls._cleanup_tasks.add(task)
        task.add_done_callback(cls._cleanup_tasks.discard)
        task.add_done_callback(_consume_cancelled_task)

    @classmethod
    async def _cleanup_failed_reservation(
        cls,
        account_key: str,
        lease_id: str,
        model_id: str | None = None,
        window_ids: dict[str, str] | None = None,
    ) -> None:
        # Redis 结果不确定：预占可能已经落库。先按 reservation_id 回退计量 ledger（幂等、
        # 只删本 id 的 member），再清并发 lease，避免这次不确定预占永久占着账号配额。
        await routing_redis_call(
            RedisLimitBackend.rollback_account_reservation(
                account_key, model_id, lease_id, window_ids=window_ids
            ),
            stage="account_failed_reservation_ledger_rollback",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
            skip_when_degraded=False,
        )
        for delay in UNCERTAIN_RESERVATION_CLEANUP_DELAYS:
            await asyncio.sleep(delay)
            removed = await routing_redis_call(
                RedisLimitBackend.release_account_reservation(account_key, lease_id),
                stage="account_failed_reservation_cleanup",
                timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
                default=None,
                skip_when_degraded=False,
            )
            if removed:
                return

    @classmethod
    def detach_account_lease_locked(cls, account_client, lease_id: str | None = None) -> LimitLease | None:
        """按 lease_id 精确弹出；调用方必须持有 AccountClient._lock。"""
        lease = account_client._limit_leases.pop(lease_id, None) if lease_id else None
        if lease is not None and account_client._in_flight > 0:
            account_client._in_flight -= 1
        return lease

    @classmethod
    async def release_account_lease(cls, lease: LimitLease | None) -> None:
        """在账号锁外释放 Redis lease，慢 Redis 不得阻塞同账号后续预占。"""
        if not lease or not lease.concurrency_acquired:
            return
        cls._active_leases.pop(lease.lease_id, None)
        renewal_task = lease.renewal_task
        lease.renewal_task = None
        if isinstance(renewal_task, asyncio.Task) and renewal_task is not asyncio.current_task():
            renewal_task.cancel()
            await asyncio.gather(renewal_task, return_exceptions=True)
        await routing_redis_call(
            RedisLimitBackend.release_account_reservation(lease.subject.account_key, lease.lease_id),
            stage="account_concurrency_release",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
            skip_when_degraded=False,
        )

    @classmethod
    async def shutdown(cls) -> None:
        heartbeat_task = cls._heartbeat_task
        cls._heartbeat_task = None
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
        leases = list(cls._active_leases.values())
        if leases:
            await asyncio.gather(*(cls.release_account_lease(lease) for lease in leases), return_exceptions=True)
        cleanup_tasks = list(cls._cleanup_tasks)
        cls._cleanup_tasks.clear()
        for task in cleanup_tasks:
            task.cancel()
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        renewal_tasks = list(cls._renewal_tasks)
        cls._renewal_tasks.clear()
        for task in renewal_tasks:
            task.cancel()
        if renewal_tasks:
            await asyncio.gather(*renewal_tasks, return_exceptions=True)

    @classmethod
    async def release_account_locked(cls, account_client) -> None:
        # 兼容旧调用；正式请求路径由 AccountClient.release 传当前任务 owner。
        lease_ids = account_client._lease_context.get()
        lease_id = lease_ids[-1] if lease_ids else None
        if lease_id:
            account_client._lease_context.set(lease_ids[:-1])
        lease = cls.detach_account_lease_locked(account_client, lease_id)
        await cls.release_account_lease(lease)

    @classmethod
    async def _record_hour_day_tokens_and_freeze(cls, account_key: str, account_client, tokens: int) -> None:
        """tph/tpd：响应后固定窗口累加，触线→冻结账号（与 tpm 同构，接受瞬时超发）。

        每小时 tokens = 3600s 滑窗；每天 tokens = 86400s 滑窗。命中即给账号打
        cooldown 到窗口末，下次选路被过滤。account_key 形如 ``account:{provider}:{user}``。
        """
        for field, limit_attr, window in (
            ("tph", "tph_limit", cls.TOKEN_HOUR_SECONDS),
            ("tpd", "tpd_limit", cls.TOKEN_DAY_SECONDS),
        ):
            limit = int(getattr(account_client, limit_attr, 0) or 0)
            if limit <= 0:
                continue
            result = await RedisLimitBackend.incrby_window(f"{account_key}:{field}", tokens, window)
            if result is None:
                continue
            used, ttl = result
            if used >= limit:
                _apply_account_freeze(account_client, ttl, f"{field} 触线 {used}/{limit}")

    @classmethod
    async def record_account_tokens(cls, account_client, tokens: int) -> None:
        if tokens <= 0:
            return
        subject = cls.account_subject(account_client, "")
        # tpm：响应后累加（token 数事后才知），触线→冻结到窗口末（接受瞬时超发）
        tpm_limit = int(getattr(account_client, "tpm_limit", 0) or 0)
        result = await RedisLimitBackend.incrby_window(
            f"{subject.account_key}:tpm", tokens, cls.TOKEN_WINDOW_SECONDS
        )
        if result is not None and tpm_limit > 0:
            used, ttl = result
            if used >= tpm_limit:
                _apply_account_freeze(account_client, ttl, f"tpm 触线 {used}/{tpm_limit}")
        # tph/tpd：每小时 / 每天 tokens 触线冻结
        await cls._record_hour_day_tokens_and_freeze(subject.account_key, account_client, tokens)
        now = time.time()
        account_client._token_usages = [
            (t, n) for t, n in account_client._token_usages if now - t < cls.TOKEN_WINDOW_SECONDS
        ]
        account_client._token_usages.append((now, tokens))  # 本地评分信号

    @classmethod
    async def record_tpm_and_maybe_freeze(
        cls,
        account_client,
        model_id: str,
        tokens: int,
        model_limit: int,
    ) -> dict | None:
        if tokens <= 0:
            return None
        subject = cls.account_subject(account_client, model_id)
        # tph/tpd 不依赖 tpm/model_limit，先于 tpm 早期返回处理，保证即使未配 tpm 仍生效
        await cls._record_hour_day_tokens_and_freeze(subject.account_key, account_client, tokens)
        account_limit = int(getattr(account_client, "tpm_limit", 0) or 0)
        if account_limit <= 0 and model_limit <= 0:
            return None
        return await routing_redis_call(
            RedisLimitBackend.record_tpm_and_freeze(
                account_key=subject.account_key,
                model_id=model_id,
                tokens=tokens,
                ttl_seconds=cls.TOKEN_WINDOW_SECONDS,
                account_limit=account_limit,
                model_limit=model_limit,
                account_freeze_key=f"limit:cooldown:{subject.provider}:{subject.account}",
                model_freeze_key=f"limit:model-tpm-freeze:{model_id}",
            ),
            stage="tpm_record_and_freeze",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
            skip_when_degraded=False,
        )

    @classmethod
    async def record_model_tokens(cls, model_id: str, tokens: int) -> tuple[int, int] | None:
        """响应后写模型 TPM 固定窗口；选路阶段不读取这个 Redis key。"""
        if tokens <= 0:
            return None
        return await routing_redis_call(
            RedisLimitBackend.record_model_tpm_window(model_id, tokens, cls.TOKEN_WINDOW_SECONDS),
            stage="model_tpm_record",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
            skip_when_degraded=False,
        )
