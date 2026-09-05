"""速率限制和账号池管理"""
import asyncio
import contextvars
import hashlib
import json
import random
import time
import traceback
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Mapping, Optional

from fastapi import HTTPException
from loguru import logger

import config
import model_catalog
from db import PostgresClient
from channel import Channel, strip_model_owner_prefix
from limit_policy_store import get_effective_provider_policy, get_effective_provider_policy_sync, match_freeze_rules, normalize_freeze_policy
from limits import LimitManager
from limits.manager import (
    ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
    _freeze_account_local_and_broadcast,
    aux_redis_call,
    begin_routing_redis_scope,
    end_routing_redis_scope,
    routing_redis_call,
    routing_redis_state,
)
from limits.backend import RedisLimitBackend
from model_metadata import FALLBACK_DEFAULT_MODEL_METADATA, apply_model_metadata, get_model_metadata, has_explicit_metadata, _mutable
from providers.base import BaseProvider
from providers.custom import CustomProvider
from usage_utils import estimate_request_part_tokens


def _scheduled_test_interval_seconds(cfg: dict) -> int:
    """把 scheduled_test 频率配置换算成「到下次可执行」的最小间隔秒数。

    - daily：固定按 24h 近似（避免引入本地时钟对齐，循环本身 60s tick 会自然到点）。
    - minutes/hours：value * 单位。最小兜底 60s，防止配置成 0/负数时高频打上游。
    """
    unit = (cfg.get("frequency_unit") or "minutes").strip().lower()
    try:
        value = int(cfg.get("frequency_value") or 0)
    except (TypeError, ValueError):
        value = 0
    if unit == "daily":
        return 24 * 3600
    if unit == "hours":
        seconds = value * 3600
    else:  # minutes / 兜底
        seconds = value * 60
    return max(60, seconds)


def _unsupported_snapshot_keyword(exc: TypeError) -> bool:
    message = str(exc)
    return "unexpected keyword argument" in message and "snapshot" in message


async def _catalog_call(callable_, *args, snapshot):
    """Call snapshot-aware APIs while preserving legacy monkeypatched tests."""
    try:
        return await callable_(*args, snapshot=snapshot)
    except TypeError as exc:
        if not _unsupported_snapshot_keyword(exc):
            raise
        return await callable_(*args)


async def _catalog_method_call(callable_, *args, snapshot, **kwargs):
    """Snapshot-aware call helper for overridable internal selection methods."""
    try:
        return await callable_(*args, snapshot=snapshot, **kwargs)
    except TypeError as exc:
        if not _unsupported_snapshot_keyword(exc):
            raise
        return await callable_(*args, **kwargs)


_MEDIA_OPERATION_CONFIG = {
    "image_generation": {"method": "generate_image", "modality": "image"},
    "video_generation": {"method": "generate_video", "modality": "video"},
    "tts_generation": {"method": "generate_speech", "modality": "audio"},
}

# 冻结/冷却原因在下游（Redis 冷却记录、内存 cooldown_reason、日志、管理端展示）展示时的最大长度。
# 数据库请求日志仍记录完整错误，这里只截取关键字段，避免整段上游响应体到处透传。
_FREEZE_REASON_MAX_LENGTH = 200

_ROUTING_TIMING: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "routing_timing", default=None
)


def begin_routing_timing(timing: dict | None = None):
    timing = timing if isinstance(timing, dict) else {}
    return _ROUTING_TIMING.set(timing)


def end_routing_timing(token) -> None:
    _ROUTING_TIMING.reset(token)


def _routing_stage_started() -> float:
    return time.monotonic()


def _record_routing_stage(name: str, started: float) -> None:
    timing = _ROUTING_TIMING.get()
    if timing is None:
        return
    elapsed_ms = max(0, int((time.monotonic() - started) * 1000))
    timing[name] = int(timing.get(name) or 0) + elapsed_ms


def _record_routing_stage_once(name: str, started: float) -> None:
    timing = _ROUTING_TIMING.get()
    if timing is None or name in timing:
        return
    timing[name] = max(0, int((time.monotonic() - started) * 1000))


async def _timed_routing_call(name: str, awaitable):
    started = _routing_stage_started()
    try:
        return await awaitable
    finally:
        _record_routing_stage(name, started)


def _pick_reason_message(value) -> str:
    """从 JSON 解析后的错误体里递归抽取 message/detail 文本。"""
    if isinstance(value, dict):
        for key in ("message", "detail", "error"):
            inner = value.get(key)
            if isinstance(inner, str) and inner.strip():
                return inner.strip()
            if isinstance(inner, (dict, list)):
                picked = _pick_reason_message(inner)
                if picked:
                    return picked
        return ""
    if isinstance(value, list):
        for item in value:
            picked = _pick_reason_message(item)
            if picked:
                return picked
    return ""


def summarize_freeze_reason(reason) -> str:
    """从上游错误文本中提取简短原因，供冻结/冷却记录与下游展示使用。

    数据库请求日志仍记录完整错误；本函数只抽取 message/detail 等关键字段并截断，
    避免 Redis 冷却记录、日志打印和管理端页面携带整段上游响应体。
    """
    if reason is None:
        return ""
    if isinstance(reason, (dict, list)):
        try:
            reason = json.dumps(reason, ensure_ascii=False)
        except Exception:
            reason = str(reason)
    text = str(reason).strip()
    if not text:
        return ""
    # 上游错误体常以 JSON 形式出现，尝试抽出其中的 message/detail 字段
    if text[0] in "{[":
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if parsed is not None:
            extracted = _pick_reason_message(parsed)
            if extracted:
                text = extracted
    text = " ".join(text.split())
    if len(text) > _FREEZE_REASON_MAX_LENGTH:
        text = text[:_FREEZE_REASON_MAX_LENGTH].rstrip() + "…"
    return text


class NoAvailableAccountError(HTTPException):
    """acquisition 阶段就失败：没有任何账号可用 / TPM 超限 / 专用线路不可用。
    未发起任何上游请求，重试无意义，且不写请求日志。

    选路失败立即交给外层 _run_group_fallback_pipeline 走备份链（主→主备→备→备备），
    不消耗 max_retries 名额。code/reasons 供终端 429 归一映射业务码与 Retry-After。
    """

    def __init__(self, status_code: int = 429, detail="", *, code: str = "", reasons: dict | None = None):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code or "no_available_account"
        self.reasons = reasons or {}


# reason code → 业务码映射（按 _collect_candidates / _select_and_reserve_candidates
# 产生的 reason 取主因）。selection 级失败统一归 no_available_account；瞬态并发/限频单列。
_REASON_TO_BUSINESS_CODE = {
    "account_frozen": "no_available_account",
    "account_cooldown": "no_available_account",
    "account_model_cooldown": "no_available_account",
    "account_model_frozen": "no_available_account",
    "account_disabled": "no_available_account",
    "model_frozen": "no_available_account",
    "concurrent_limit": "concurrent_limit_exceeded",
    "model_tpm_limited": "rate_limit_exceeded",
    "provider_quota_exhausted": "quota_exhausted",
    "provider_daily_quota_exhausted": "quota_exhausted",
    "provider_quota_cooldown": "quota_exhausted",
    "provider_message_quota_exceeded": "quota_exhausted",
    "redis_uncertain": "service_busy",
    "provider_not_initialized": "service_busy",
    "selection_deadline": "no_available_account",
    "selection_exhausted": "no_available_account",
    "selection_returned_none": "no_available_account",
}


def _dominant_reason_code(reasons: dict | None) -> tuple[str, dict]:
    """按计数最大的 reason 映射业务码；返回 (code, reasons_snapshot)。"""
    if not reasons:
        return "no_available_account", {}
    items = [(k, v) for k, v in reasons.items() if v]
    if not items:
        return "no_available_account", dict(reasons)
    top = max(items, key=lambda kv: kv[1])[0]
    return _REASON_TO_BUSINESS_CODE.get(top, "no_available_account"), dict(reasons)


class LegacyRateLimiter:
    """API Key 速率限制器"""

    _minute_requests: dict[str, list[float]] = defaultdict(list)
    _day_requests: dict[str, list[float]] = defaultdict(list)
    _lock = asyncio.Lock()

    @classmethod
    async def check(cls, api_key: str) -> bool:
        """检查速率限制"""
        rate_config = await config.Config.get_api_key_rate_limit(api_key)
        rpm = rate_config.get("requests_per_minute", 60)
        rpd = rate_config.get("requests_per_day", 1000)

        redis = RedisRateTracker._redis()
        if redis is not None:
            return await RedisRateTracker.check_and_record_rpm(f"api-key:{api_key}", rpm) and await RedisRateTracker.check_and_record_rpm(f"api-key-day:{api_key}", rpd, 86400)

        async with cls._lock:
            now = time.time()

            cls._minute_requests[api_key] = [
                t for t in cls._minute_requests[api_key] if now - t < 60
            ]
            cls._day_requests[api_key] = [
                t for t in cls._day_requests[api_key] if now - t < 86400
            ]

            if rpm > 0 and len(cls._minute_requests[api_key]) >= rpm:
                return False

            if rpd > 0 and len(cls._day_requests[api_key]) >= rpd:
                return False

            if rpm > 0:
                cls._minute_requests[api_key].append(now)
            if rpd > 0:
                cls._day_requests[api_key].append(now)
            return True

    @classmethod
    async def acquire(cls, api_key: str):
        """获取许可，超过报 429"""
        if not await cls.check(api_key):
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

    @classmethod
    async def get_usage(cls, api_key: str) -> dict:
        """获取当前使用情况（不记录请求）"""
        rate_config = await config.Config.get_api_key_rate_limit(api_key)
        rpm = rate_config.get("requests_per_minute", 60)
        rpd = rate_config.get("requests_per_day", 1000)
        now = time.time()
        minute_used = await RedisRateTracker.get_rpm_usage(f"api-key:{api_key}")
        day_used = await RedisRateTracker.get_rpm_usage(f"api-key-day:{api_key}", 86400)
        if minute_used is None:
            minute_used = len([t for t in cls._minute_requests.get(api_key, []) if now - t < 60])
        if day_used is None:
            day_used = len([t for t in cls._day_requests.get(api_key, []) if now - t < 86400])
        return {"rpm_limit": rpm, "rpm_used": minute_used, "rpd_limit": rpd, "rpd_used": day_used}


class RedisRateTracker:
    """Redis-backed RPM/TPM tracking - survives restarts"""

    @staticmethod
    def _redis():
        from rd import JdbcClient
        return JdbcClient.redis

    @staticmethod
    def _member_token(member) -> int:
        if isinstance(member, bytes):
            member = member.decode("utf-8")
        parts = str(member).split(":")
        if len(parts) < 2:
            return 0
        try:
            return int(parts[1])
        except ValueError:
            return 0

    @classmethod
    async def check_rpm(cls, key: str, rpm_limit: int, window_seconds: int = 60) -> bool:
        if rpm_limit <= 0:
            return True
        redis = cls._redis()
        if redis is None:
            return True
        redis_key = f"ratelimit:rpm:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - window_seconds)
            count = await redis.zcard(redis_key) or 0
            return count < rpm_limit
        except Exception:
            return True

    @classmethod
    async def record_rpm(cls, key: str, window_seconds: int = 60) -> None:
        redis = cls._redis()
        if redis is None:
            return
        redis_key = f"ratelimit:rpm:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - window_seconds)
            await redis.zadd(redis_key, {f"{now}:{random.random()}": now})
            await redis.expire(redis_key, window_seconds + 60)
        except Exception:
            pass

    @classmethod
    async def check_and_record_rpm(cls, key: str, rpm_limit: int, window_seconds: int = 60) -> bool:
        if not await cls.check_rpm(key, rpm_limit, window_seconds):
            return False
        await cls.record_rpm(key, window_seconds)
        return True

    @classmethod
    async def get_rpm_usage(cls, key: str, window_seconds: int = 60) -> int | None:
        redis = cls._redis()
        if redis is None:
            return None
        redis_key = f"ratelimit:rpm:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - window_seconds)
            return int(await redis.zcard(redis_key) or 0)
        except Exception:
            return None

    @classmethod
    async def check_tpm(cls, key: str, tpm_limit: int) -> bool:
        if tpm_limit <= 0:
            return True
        redis = cls._redis()
        if redis is None:
            return True
        redis_key = f"ratelimit:tpm:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - 60)
            members = await redis.zrange(redis_key, 0, -1, withscores=True)
            current = sum(cls._member_token(member) for member, _ in (members or []))
            return current < tpm_limit
        except Exception:
            return True

    @classmethod
    async def record_tpm(cls, key: str, tokens: int):
        if tokens <= 0:
            return
        redis = cls._redis()
        if redis is None:
            return
        redis_key = f"ratelimit:tpm:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - 60)
            await redis.zadd(redis_key, {f"{now}:{tokens}:{random.random()}": now})
            await redis.expire(redis_key, 120)
        except Exception:
            pass

    @classmethod
    async def check_and_record_tpm(cls, account_key: str, tpm_limit: int, tokens: int) -> bool:
        if not await cls.check_tpm(account_key, tpm_limit):
            return False
        await cls.record_tpm(account_key, tokens)
        return True


class RateLimiter:
    """API Key request, token, and concurrency limiter（触线即拒绝 + 固定窗口计数）。"""

    _lock = asyncio.Lock()
    REQUEST_WINDOWS = {
        "requests_per_minute": ("rpm", 60),
        "requests_per_5h": ("rp5h", 5 * 3600),
        "requests_per_day": ("rpd", 86400),
        "requests_per_week": ("rpw", 7 * 86400),
    }
    TOKEN_WINDOWS = {
        "tokens_per_minute": ("tpm", 60),
        "tokens_per_day": ("tpd", 86400),
        "tokens_per_week": ("tpw", 7 * 86400),
    }

    @classmethod
    def _subject(cls, api_key: str) -> str:
        return hashlib.sha256((api_key or "").encode("utf-8")).hexdigest()[:32]

    @classmethod
    async def _limits(cls, api_key: str) -> dict:
        rate_config = await config.Config.get_api_key_rate_limit(api_key)
        return {
            "requests_per_minute": int(rate_config.get("requests_per_minute", rate_config.get("rpm", 0)) or 0),
            "requests_per_5h": int(rate_config.get("requests_per_5h", rate_config.get("requests_per_5_hours", 0)) or 0),
            "requests_per_day": int(rate_config.get("requests_per_day", rate_config.get("rpd", 0)) or 0),
            "requests_per_week": int(rate_config.get("requests_per_week", rate_config.get("rpw", 0)) or 0),
            "tokens_per_minute": int(rate_config.get("tokens_per_minute", rate_config.get("tpm", 0)) or 0),
            "tokens_per_day": int(rate_config.get("tokens_per_day", rate_config.get("tpd", 0)) or 0),
            "tokens_per_week": int(rate_config.get("tokens_per_week", rate_config.get("tpw", 0)) or 0),
            "concurrent_requests": int(rate_config.get("concurrent_requests", rate_config.get("concurrent", 0)) or 0),
            "max_ips": int(rate_config.get("max_ips", 0) or 0),
            "ip_window_seconds": int(rate_config.get("ip_window_seconds", 0) or 0),
        }

    # 触线即拒绝语义（与账号侧"触线即冻结"同构，只是 API Key 越线直接 429 而非冻结账号）：
    # 入口只判内存 blocked_until 布尔（pubsub 同步）；计数走 Redis 固定窗口，越线设 blocked + 广播。
    # 并发保留 Redis 原子占用。token 瞬时超发可接受。
    _blocked_until: dict[str, dict[str, float]] = defaultdict(dict)  # subject -> {field: 到期时间戳}

    @classmethod
    def _window_ttl(cls, window_seconds: int, now: float) -> int:
        """把滑窗秒数映射成固定窗口 TTL：日/周对齐到自然边界，其余用原秒数。"""
        if window_seconds >= 7 * 86400:
            dt = datetime.fromtimestamp(now)
            days_ahead = 7 - dt.weekday()
            nxt = (dt + timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
            return max(1, int((nxt - dt).total_seconds()))
        if window_seconds >= 86400:
            dt = datetime.fromtimestamp(now)
            nxt = (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return max(1, int((nxt - dt).total_seconds()))
        return window_seconds

    @classmethod
    def _is_blocked(cls, subject: str, now: float) -> bool:
        blocks = cls._blocked_until.get(subject)
        if not blocks:
            return False
        for field in list(blocks.keys()):
            if blocks[field] <= now:
                del blocks[field]
        return bool(blocks)

    @classmethod
    def _set_blocked(cls, subject: str, field: str, ttl: int, now: float) -> None:
        cls._blocked_until[subject][field] = now + ttl
        try:
            import runtime_sync
            asyncio.create_task(runtime_sync.publish(
                runtime_sync.EVENT_APIKEY, subject,
                extra={"blocked_field": field, "until": now + ttl},
            ))
        except Exception:
            pass

    @classmethod
    def apply_block_event(cls, subject: str, field: str, until: float) -> None:
        """pubsub apikey-block 事件落到本实例内存。"""
        cls._blocked_until[subject][field] = until

    @classmethod
    def clear_block_event(cls, subject: str, field: str) -> None:
        """pubsub apikey-block 回源发现 Redis 已无该 block 时，清本实例内存。"""
        blocks = cls._blocked_until.get(subject)
        if blocks is None:
            return
        blocks.pop(field, None)
        if not blocks:
            cls._blocked_until.pop(subject, None)

    @classmethod
    async def acquire(cls, api_key: str, client_ip: str | None = None) -> dict:
        """预占：入口只判内存 blocked 布尔 + 并发原子占用；请求计数固定窗口累加，越线设 blocked。"""
        subject = cls._subject(api_key)
        now = time.time()
        if cls._is_blocked(subject, now):
            raise HTTPException(
                status_code=429,
                detail={"message": "API key rate limit exceeded; please retry later.", "code": "api_key_rate_limit_exceeded", "kind": "api_key_rate_limit_exceeded", "retry_after": 1},
            )

        limits = await cls._limits(api_key)
        max_ips = limits["max_ips"]
        if max_ips > 0 and client_ip:
            window = limits["ip_window_seconds"] or 3600
            verdict = await RedisLimitBackend.check_and_add_ip(
                f"api-key:{subject}",
                client_ip,
                max_ips,
                window,
            )
            if verdict is not None and verdict[0] is False:
                raise HTTPException(
                    status_code=429,
                    detail={"message": "API key IP limit exceeded.", "code": "api_key_ip_limit_exceeded", "kind": "api_key_ip_limit_exceeded"},
                )

        lease = {"subject": subject, "lease_id": f"api-key-lease:{uuid.uuid4().hex}", "concurrency_acquired": False}
        concurrent_limit = limits["concurrent_requests"]
        if concurrent_limit > 0:
            acquired = await RedisLimitBackend.acquire_concurrency(f"api-key:{subject}", concurrent_limit, lease["lease_id"])
            if acquired is False:
                raise HTTPException(
                    status_code=429,
                    detail={"message": "API key concurrency limit exceeded; please retry.", "code": "api_key_concurrent_limit_exceeded", "kind": "api_key_concurrent_limit_exceeded", "retry_after": 1},
                )
            lease["concurrency_acquired"] = acquired is True

        # 请求数固定窗口累加（发送即计），越线设 blocked（本次仍放行，接受瞬时超发）
        for field, (_, window) in cls.REQUEST_WINDOWS.items():
            limit = limits[field]
            if limit <= 0:
                continue
            ttl = cls._window_ttl(window, now)
            result = await RedisLimitBackend.incr_window(f"api-key:{subject}:{field}", ttl)
            if result is not None:
                count, real_ttl = result
                if count >= limit:
                    cls._set_blocked(subject, field, real_ttl, now)
        return lease

    @classmethod
    async def release(cls, lease: dict | None) -> None:
        if not lease:
            return
        subject = lease.get("subject")
        if lease.get("concurrency_acquired"):
            await RedisLimitBackend.release_concurrency(f"api-key:{subject}", lease.get("lease_id", ""))

    @classmethod
    async def record_usage(cls, api_key: str, total_tokens: int = 0) -> None:
        tokens = int(total_tokens or 0)
        if not api_key or tokens <= 0:
            return
        limits = await cls._limits(api_key)
        subject = cls._subject(api_key)
        now = time.time()
        for field, (_, window) in cls.TOKEN_WINDOWS.items():
            limit = limits[field]
            if limit <= 0:
                continue
            ttl = cls._window_ttl(window, now)
            result = await RedisLimitBackend.incrby_window(f"api-key:{subject}:{field}", tokens, ttl)
            if result is not None:
                used, real_ttl = result
                if used >= limit:
                    cls._set_blocked(subject, field, real_ttl, now)

    @classmethod
    async def get_usage(cls, api_key: str) -> dict:
        """Return current API Key usage without recording a request（展示用，读固定窗口）。"""
        limits = await cls._limits(api_key)
        subject = cls._subject(api_key)
        result = {}
        for field, (prefix, _window) in cls.REQUEST_WINDOWS.items():
            used, _ttl = await RedisLimitBackend.get_window(f"api-key:{subject}:{field}")
            result[f"{prefix}_limit"] = limits[field]
            result[f"{prefix}_used"] = used
        for field, (prefix, _window) in cls.TOKEN_WINDOWS.items():
            used, _ttl = await RedisLimitBackend.get_window(f"api-key:{subject}:{field}")
            result[f"{prefix}_limit"] = limits[field]
            result[f"{prefix}_used"] = used
        concurrent_used = await RedisLimitBackend.concurrency_used(f"api-key:{subject}")
        if concurrent_used is None:
            concurrent_used = 0
        result["concurrent_limit"] = limits["concurrent_requests"]
        result["concurrent_used"] = concurrent_used
        ip_used = 0
        if limits["max_ips"] > 0:
            ip_used = await RedisLimitBackend.set_cardinality(f"api-key:{subject}") or 0
        result["ip_limit"] = limits["max_ips"]
        result["ip_used"] = ip_used
        return result


class AccountState(str, Enum):
    """账号此刻的规范状态——「账号处于什么状态」的唯一判据。

    取代散落各处的布尔组合（is_frozen / cooldown / disabled / auth_ok 各自拼），
    由 :meth:`AccountClient.state` 一次算出，前端/移动端只读它做展示与筛选。

    优先级（高→低，与 LimitManager.account_available_locked 的判定顺序一致）：
      disabled > frozen > cooling > model_frozen > auth_failed > checking > available

    互斥：任一时刻账号只落一个状态，由 :meth:`AccountClient.state` 的 if-elif 链保证。
    实时性：每次调用都拿 now 重算 until，账号级 TTL 到期后同一方法会立刻改判——
    例如账号级冷却先到期、模型级仍在冻，则由 cooling 自动翻成 model_frozen，无需外部刷新。

    维度边界：
    - model_frozen 表示「账号级未冻，但有部分模型在冻」——承载 per-model 冻结维度，
      具体是哪些模型见结构化 freeze_items（scope=account_model）/ frozen_model_ids()。
    - requesting（_in_flight>0）是瞬时活动标志，与静态状态正交，不并入本枚举。
    """

    DISABLED = "disabled"          # 账号开关关闭 / 未加载（是禁用，不是冻结）
    FROZEN = "frozen"              # 永久冻结（_freeze_state until=None）
    COOLING = "cooling"            # 临时冷却（until>now，有剩余秒数）
    MODEL_FROZEN = "model_frozen"  # 账号级未冻，但部分模型在冻（见 frozen_model_ids）
    AUTH_FAILED = "auth_failed"    # 认证失败（auth_ok is False）
    CHECKING = "checking"          # 尚未体检（auth_ok is None）
    AVAILABLE = "available"        # 可用


class FailureWindow:
    """账号滑动窗口统计件。纯内存、单实例本地统计。

    只记录被选中、成功、失败和连续成功/失败，统计结果供评分使用；
    不负责冻结，也不维护独立的“熔断”状态。
    """

    def __init__(self, *, window_seconds: float = 300):
        self._picks: list[float] = []
        self._success: list[float] = []
        self._failure: list[float] = []
        self._consecutive_failures: int = 0
        self._consecutive_successes: int = 0
        self._window = window_seconds

    def record_pick(self, now: float) -> None:
        """记一次被选中（reserve 成功）。"""
        self._picks.append(now)

    def pick_factor(self, now: float, soft_cap: int, window_seconds: float) -> float:
        """账号级被选中频率因子（线性）：<= soft_cap 为 1.0，超出按 0.15/次线性衰减到 0.3 封底。
        顺手清理过期时间戳。"""
        recent = [t for t in self._picks if now - t < window_seconds]
        self._picks = recent
        n = len(recent)
        if n <= soft_cap:
            return 1.0
        return max(0.3, 1.0 - (n - soft_cap) * 0.15)

    def record_success(self, now: float) -> None:
        """记一次成功：append 成功时间戳，清连续失败，连续成功 +1。"""
        self._success.append(now)
        self._consecutive_failures = 0
        self._consecutive_successes += 1

    def record_failure(self, now: float, account_level: bool, threshold: int | None = None,
                       base: int | None = None, step: int | None = None,
                       max_seconds: int | None = None) -> None:
        """记录一次失败；账号级失败增加连续失败计数，供评分降权使用。"""
        self._failure.append(now)
        self._consecutive_successes = 0
        if account_level:
            self._consecutive_failures += 1

    def error_rate(self, now: float, window: float) -> float:
        """账号级错误率：window 窗口内 failure / (success + failure)。顺手清理过期。"""
        s = [t for t in self._success if now - t < window]
        f = [t for t in self._failure if now - t < window]
        self._success = s
        self._failure = f
        total = len(s) + len(f)
        return len(f) / total if total > 0 else 0.0

    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def consecutive_successes(self) -> int:
        return self._consecutive_successes

    def circuit_failure_count(self, now: float) -> int:
        """兼容统计查询：返回当前窗口内账号级失败次数，供诊断使用，不代表冻结。"""
        failures = [t for t in self._failure if now - t < self._window]
        self._failure = failures
        return len(failures)

    def clear_circuit(self) -> None:
        """兼容重置入口：清除窗口统计，不改变冻结状态。"""
        self._failure = []

    def reset_failures(self) -> None:
        """清连续失败计数（成功解冻时）；连续成功计数保留——那是让权信号，由 record_success 维护。"""
        self._consecutive_failures = 0

    def reset_streaks(self) -> None:
        """清连续成功/失败计数（重置账号评分状态时）。"""
        self._consecutive_failures = 0
        self._consecutive_successes = 0

    def clear_all(self) -> None:
        """清全部统计（账号彻底重置时）。"""
        self._picks = []
        self._success = []
        self._failure = []
        self._consecutive_failures = 0
        self._consecutive_successes = 0


class AccountClient:
    """单个账号客户端"""

    def __init__(self, provider: BaseProvider, rpm_limit: int, priority: int = 0, weight: int = 0, tpm_limit: int = 0, concurrent_limit: int = 0, balance: float | None = None, balance_threshold: float = 0, is_frozen: bool = False, disabled: bool = False, disable_reason: str = "", rpd_limit: int = 0, rph_limit: int = 0, tph_limit: int = 0, tpd_limit: int = 0, metadata: dict | None = None):
        self.provider = provider
        self.rpm_limit = rpm_limit
        self.rpd_limit = rpd_limit  # 每账号每日请求上限（触线即冻结到当天结束）
        # 到量冻结维度：每小时次数 / 每小时 tokens / 每天 tokens（触线即冻结账号到窗口末）
        self.rph_limit = rph_limit
        self.tph_limit = tph_limit
        self.tpd_limit = tpd_limit
        self.tpm_limit = tpm_limit
        self.concurrent_limit = concurrent_limit
        self._in_flight = 0
        # 仅用于池外/测试 stub：正式运行账号会 attach Channel，priority/weight 从渠道读取。
        self._legacy_priority = int(priority or 0)
        self._legacy_weight = max(1, int(weight or 1))
        self.disabled = bool(disabled)
        self.disable_reason = disable_reason
        self._requests: list[float] = []
        self._token_usages: list[tuple[float, int]] = []
        self._limit_leases: dict[str, object] = {}
        self._lease_context: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
            f"account_limit_leases:{id(self)}", default=()
        )
        self._lock = asyncio.Lock()
        # auth 状态缓存
        self.auth_ok: bool | None = None       # True=认证通过, False=认证失败, None=未检查
        self.auth_checked_at: float = 0        # 上次检查时间戳
        self.auth_error: str = ""              # 最近一次检查的错误信息
        self.last_route_info: dict = {}
        # Provider-reported daily quota state (e.g. from upstream headers)
        self._provider_daily_remaining: int | None = None
        self._provider_daily_limit: int | None = None
        self._provider_quota_disabled_until: float = 0  # timestamp; >0 means quota-exhaustion cooldown
        # Balance / frozen state for intelligent routing
        self.balance = balance
        self.balance_threshold = float(balance_threshold or 0)
        # 账号元数据：用户自定义键值对，apply diff 更新（热更新），供 headers 模板 {{account.metadata.*}} 取用。
        self.metadata: dict = dict(metadata if isinstance(metadata, dict) else {})
        # 冻结态桶：账号级 + 账号×模型级冻结，内存为准、Redis 兜底（重启/跨实例回灌）。
        # 内部结构对外不暴露，只通过 freeze/unfreeze/is_frozen 访问。
        #   until: None=永久冻结, float>0=临时到期点, 0=没冻
        # 构造期兼容 is_frozen=True 的旧调用路径：按永久冻结初始化进桶。
        self._freeze_state: dict = {"account": {"until": 0, "reason": ""}, "models": {}}
        if is_frozen:
            self._freeze_state["account"]["until"] = None
        # 滑动窗口统计件：被选中频率 / 成功失败计数 / 连续计数。纯统计供评分用。
        # 纯统计不触发冻结，调用方读结果再调 freeze。
        self._stats = FailureWindow()

    async def _generic_available_locked(self, now: float) -> bool:
        decision = await self.provider.check_limits(context={"account_client": self})
        return decision.allowed

    async def can_accept(self, model_id: str, messages: list[dict] | None = None) -> bool:
        """检查账号是否可接收新请求，不记录请求"""
        async with self._lock:
            decision = await self.provider.check_limits(
                model_id,
                messages,
                context=self._limit_context(),
            )
            return decision.allowed

    async def reserve_with_decision(self, model_id: str, messages: list[dict] | None = None, *, token_estimate: int = 0):
        """Redis 原子预占在账号锁外执行；返回结构化拒绝原因。

        ``token_estimate`` 是本次请求的 token 预占量（prompt 估算 + 有界预期输出），
        由主链路在选路前算好传入；成功后用真实 usage 补差，失败时按 reservation 精确回滚。
        """
        reserve_with_decision = getattr(self.provider, "reserve_limits_with_decision", None)
        if reserve_with_decision is not None:
            result = await reserve_with_decision(
                model_id,
                messages,
                context=self._limit_context(token_estimate=token_estimate),
            )
        else:
            from limits.rules import LimitDecision, LimitReservation
            lease = await self.provider.reserve_limits(
                model_id,
                messages,
                context=self._limit_context(token_estimate=token_estimate),
            )
            result = LimitReservation(lease, LimitDecision(lease is not None, "" if lease is not None else "reservation_denied"))
        lease = result.lease
        if lease is None:
            return result
        async with self._lock:
            if lease.local_fallback and self.concurrent_limit > 0 and self._in_flight >= self.concurrent_limit:
                from limits.rules import LimitDecision, LimitReservation
                return LimitReservation(None, LimitDecision(False, "concurrent_limit"))
            self._limit_leases[lease.lease_id] = lease
            self._lease_context.set((*self._lease_context.get(), lease.lease_id))
            self._requests.append(time.time())
            self._in_flight += 1
        try:
            await self.provider.record_message(model_id, messages)
        except BaseException:
            async with self._lock:
                rollback = LimitManager.detach_account_lease_locked(self, lease.lease_id)
            await LimitManager.release_account_lease(rollback)
            lease_ids = self._lease_context.get()
            self._lease_context.set(tuple(x for x in lease_ids if x != lease.lease_id))
            raise
        return result

    def current_limit_lease(self):
        """当前任务最近一次预占的 lease（供 CandidateReservation 结算/回滚用）。

        与 :meth:`release` 同源：都读 ``_lease_context``（每个请求任务独立的 contextvar），
        因此并发请求各自拿到自己的 lease，不会互相错认。
        """
        lease_ids = self._lease_context.get()
        if not lease_ids:
            return None
        return self._limit_leases.get(lease_ids[-1])

    async def reserve(self, model_id: str, messages: list[dict] | None = None) -> bool:
        result = await self.reserve_with_decision(model_id, messages)
        return result.allowed

    def _limit_context(self, token_estimate: int = 0) -> dict:
        """构造下传给 LimitManager 的 context；is_test 时透传标记，
        使 account_available_locked 跳过 disabled/cooldown 等硬状态。"""
        ctx = {"account_client": self}
        if getattr(self, "_is_test", False):
            ctx["is_test"] = True
        if token_estimate:
            ctx["token_estimate"] = int(token_estimate)
        return ctx

    async def release(self):
        """释放账号并发占用；本地状态在锁内更新，Redis lease 在锁外释放。"""
        lease_ids = self._lease_context.get()
        lease_id = lease_ids[-1] if lease_ids else None
        if lease_id:
            self._lease_context.set(lease_ids[:-1])
        async with self._lock:
            lease = LimitManager.detach_account_lease_locked(self, lease_id)
        await LimitManager.release_account_lease(lease)

    async def check_available(
        self,
        record_request: bool = True,
        model_id: str | None = None,
        messages: list[dict] | None = None,
    ) -> bool:
        """执行异步限额检查；无 model_id 时走 provider 通用检查。"""
        if model_id is None:
            now = time.time()
            async with self._lock:
                return await self._generic_available_locked(now)
        if record_request:
            return await self.reserve(model_id, messages)
        return await self.can_accept(model_id, messages)

    # ── 冻结态：唯一入口 freeze/unfreeze，内部 _freeze_state 桶不对外暴露 ──
    def freeze(self, *, kind: str = "account", model_id: str | None = None,
               seconds: int | None = 0, reason: str = "", refresh: bool = True) -> bool:
        """冻结账号（或账号×模型）。内存为准，Redis 兜底由调用方/广播负责落 key。

        kind ∈ {"account", "account_model"}：
          - account：冻结整账号。model_id 被忽略。seconds=None 永久，>0 临时到 now+seconds。
          - account_model：冻结该账号的该模型，账号其他模型不受影响。model_id 必填。
        seconds=0 等价于「不冻结」（无操作）。

        refresh=False（渠道「失败刷新冻结周期」关）时，若该对象已处于生效冻结中则不改动，
        保留原到期时间。返回是否真的写了冻结态——调用方据此决定要不要落 Redis TTL 与广播，
        因为 Redis 的 SET EX 会无条件重设 TTL，光靠内存不变拦不住刷新。

        本方法只改内存桶；Redis 兜底 key（临时带 TTL / 永久无 TTL）与跨实例 pubsub 广播
        由调用方在 ProviderPool 层完成（freeze 需要渠道上下文与 Redis 句柄，故下沉到 pool）。
        """
        until = None if seconds is None else (time.time() + seconds if seconds and seconds > 0 else 0)
        if until == 0:
            return False  # 0 = 不冻结
        if not refresh:
            already = self.is_model_frozen(model_id) if (kind == "account_model" and model_id) else self.is_frozen
            if already:
                return False
        entry = {"until": until, "reason": reason or ""}
        if kind == "account_model" and model_id:
            self._freeze_state["models"][model_id] = entry
        else:
            self._freeze_state["account"] = entry
        return True

    def unfreeze(self, *, kind: str = "account", model_id: str | None = None) -> bool:
        """解冻。返回是否确实清除了冻结态（未冻结返 False）。
        kind=account：清整账号冻结（**含其全部模型级冻结**）——账号已证明可达。
        kind=account_model：只清该模型冻结，其余保留。Redis 兜底由调用方负责。"""
        now = time.time()
        changed = False
        if kind == "account_model" and model_id:
            if model_id in self._freeze_state["models"]:
                del self._freeze_state["models"][model_id]
                changed = True
            return changed
        # account：清账号级 + 全部模型级
        acc = self._freeze_state["account"]
        if acc.get("until") is not None or (acc.get("until", 0) and acc["until"] > now) or self._freeze_state["models"]:
            changed = True
        self._freeze_state["account"] = {"until": 0, "reason": ""}
        self._freeze_state["models"] = {}
        return changed

    def set_freeze_state_raw(self, *, account_until=None, model_untils: dict | None = None,
                             reason: str = "") -> None:
        """内部/回灌用：直接写桶（从 Redis 兜底回灌时调用，绕过时间戳计算）。
        account_until: None=永久, float=到期点, 0=没冻。model_untils: {model_id: float|None}。"""
        if account_until is not None and account_until != 0:
            self._freeze_state["account"] = {"until": account_until, "reason": reason}
        if model_untils:
            for mid, u in model_untils.items():
                self._freeze_state["models"][mid] = {"until": u, "reason": reason}

    @property
    def is_frozen(self) -> bool:
        """账号级是否**处于冻结中**（永久(None)与未到期临时都算 True）。

        注意命名陷阱：True 不等于「永久冻结」。要区分永久/临时用 state()：
        永久 → AccountState.FROZEN；临时 → AccountState.COOLING（cooldown_remaining>0）。
        模型级用 is_model_frozen(model_id)。
        """
        now = time.time()
        u = self._freeze_state["account"].get("until", 0)
        return u is None or (u > now)

    def is_model_frozen(self, model_id: str) -> bool:
        """该账号的指定模型是否冻结。"""
        now = time.time()
        m = self._freeze_state["models"].get(model_id)
        if m is None:
            return False
        u = m.get("until", 0)
        return u is None or (u > now)

    def cooldown_remaining(self, now: float | None = None) -> int:
        """账号级临时冻结剩余秒数；永久冻结(None)与未冻结返回 0（永久≠冷却）。"""
        now = now or time.time()
        u = self._freeze_state["account"].get("until", 0)
        if u is None or u <= now:
            return 0
        return int(u - now)

    def freeze_reason(self) -> str:
        """账号级冻结原因（未冻结返回空串）。"""
        return self._freeze_state["account"].get("reason", "")

    def last_request_at(self) -> float:
        """最近一次真实请求（预占成功）的时间戳；从未请求过返回 0。

        _requests 在预占成功时 append，是唯一的「账号被真实流量打过」信号。
        定时检测据此实现「最近请求过就跳过本轮」，省一次无意义的上游探测。
        """
        return self._requests[-1] if self._requests else 0.0

    def is_available(self, model_id: str | None = None) -> bool:
        """账号（对该模型）是否可选：未禁用、未冻结。选路用。"""
        if self.disabled:
            return False
        if self.is_frozen:
            return False
        if model_id and self.is_model_frozen(model_id):
            return False
        return True

    def frozen_model_ids(self, now: float | None = None) -> list[str]:
        """当前仍在冻结中的模型 id 列表（顺带惰性剔除已到期条目）。

        `is_model_frozen` 只能问单个模型，状态判定与「按被冻模型探测」都需要枚举。
        过期条目只有 unfreeze 才会删，会一直堆在桶里，这里顺手清掉：桶里剩下的都是
        真在冻的，`state()` 判 model_frozen 才不会被僵尸条目误导。
        返回顺序按剩余时间升序（最快到期的在前），探测封顶时优先测最快恢复的那些。
        """
        now = now or time.time()
        alive: list[tuple[float, str]] = []
        expired: list[str] = []
        for mid, entry in self._freeze_state["models"].items():
            u = entry.get("until", 0)
            if u is None:
                alive.append((float("inf"), mid))  # 永久冻结排最后
            elif u > now:
                alive.append((u, mid))
            else:
                expired.append(mid)
        for mid in expired:
            self._freeze_state["models"].pop(mid, None)
        alive.sort(key=lambda item: item[0])
        return [mid for _, mid in alive]

    def prune_freeze_for_models(self, valid_model_ids: set[str]) -> list[str]:
        """删除不在 valid_model_ids 内的模型冻结条目，返回被清掉的 model_id。

        模型从渠道模型表里被删/改名后，它的冻结条目就成了幽灵：既让账号永远显示
        model_frozen，探测也无从下手（选路里没有这个模型）。模型表每次灌入后调用本方法，
        调用方拿返回值同步清 Redis 兜底键。
        """
        removed = [mid for mid in self._freeze_state["models"] if mid not in valid_model_ids]
        for mid in removed:
            self._freeze_state["models"].pop(mid, None)
        return removed

    def state(self) -> AccountState:
        """账号此刻的规范状态（唯一判据，见 :class:`AccountState`）。

        优先级与 LimitManager.account_available_locked 对齐：禁用先于冻结——
        账号开关关掉后即便还留着冻结态，它也不参与路由，展示也应说「已禁用」。
        永久冻结 vs 临时冷却靠 cooldown_remaining 区分（永久 until=None → 剩余 0）。
        账号级冻结优先于模型级：账号级到期后同一次调用就会翻成 model_frozen。
        """
        if self.disabled:
            return AccountState.DISABLED
        if self.is_frozen:
            return AccountState.COOLING if self.cooldown_remaining() > 0 else AccountState.FROZEN
        if self.frozen_model_ids():
            return AccountState.MODEL_FROZEN
        if self.auth_ok is False:
            return AccountState.AUTH_FAILED
        if self.auth_ok is None:
            return AccountState.CHECKING
        return AccountState.AVAILABLE

    # cooldown_until / cooldown_reason 旧读写点（admin 显示、pubsub、clear_runtime_freeze）
    # 在桶化后仍被多处直接读写，这里做兼容 shim 转发，阶段三清理完外部调用点后删除。
    @property
    def _cooldown_until(self) -> float:
        acc = self._freeze_state["account"]
        u = acc.get("until", 0)
        return 0 if u is None else (u or 0)

    @_cooldown_until.setter
    def _cooldown_until(self, value: float) -> None:
        # 旧代码 set_cooldown/restore/clear 直接写 _cooldown_until；转发进桶。
        # value>0=临时到期点，0=没冻。永久(None)不通过此 setter 设置（走 freeze）。
        self._freeze_state["account"]["until"] = value or 0

    @property
    def cooldown_reason(self) -> str:
        return self._freeze_state["account"].get("reason", "")

    @cooldown_reason.setter
    def cooldown_reason(self, value: str) -> None:
        self._freeze_state["account"]["reason"] = value or ""

    def set_cooldown(self, seconds: int = 60, reason: str = ""):
        """设置冷却时间（429 后暂时不可用）。兼容旧入口，转发到 freeze。"""
        self.freeze(kind="account", seconds=seconds, reason=reason)

    def _has_provider_quota(self) -> bool:
        """是否启用 provider-reported 每日限额检查"""
        return self._provider_daily_limit is not None

    def update_provider_quota(self, remaining: int | None, limit: int | None) -> None:
        """从上游响应头更新 provider-reported 每日限额。"""
        if remaining is not None:
            self._provider_daily_remaining = remaining
        if limit is not None:
            self._provider_daily_limit = limit
        if remaining is not None and remaining <= 0:
            # 限额耗尽，禁用至次日（近似 24h，上游重置时会通过 headers 恢复）
            self._provider_quota_disabled_until = time.time() + 86400
        elif remaining is not None and remaining > 0:
            self._provider_quota_disabled_until = 0

    def sync_provider_quota(self) -> None:
        """从 provider.quotas (WindowQuota/CountQuota) 同步到 AccountClient 字段。"""
        quota = getattr(self.provider, "quotas", None)
        if quota is None:
            return
        # 兼容旧 CountQuota
        daily_remaining = getattr(quota, "daily_remaining", None)
        daily_limit = getattr(quota, "daily_limit", None)
        if daily_remaining is not None or daily_limit is not None:
            self.update_provider_quota(daily_remaining, daily_limit)
        # 同步 WindowQuota 维度到 AccountClient 快照字段
        if hasattr(quota, "dimensions"):
            for dim in quota.dimensions():
                if dim.name == "requests_daily":
                    self.update_provider_quota(dim.remaining, dim.limit)

    def is_model_available(self, model_id: str) -> bool:
        """兼容旧调用，真实配额由 provider.check_message 判断"""
        return True

    @property
    def daily_requests_limit(self) -> int | None:
        quota = getattr(self.provider, "quotas", None)
        if quota and hasattr(quota, "dimension"):
            dim = quota.dimension("requests_daily")
            if dim:
                return dim.limit
        return getattr(quota, "daily_limit", None) if quota else None

    @property
    def daily_requests_remaining(self) -> int | None:
        quota = getattr(self.provider, "quotas", None)
        if quota and hasattr(quota, "dimension"):
            dim = quota.dimension("requests_daily")
            if dim:
                return dim.remaining
        return getattr(quota, "daily_remaining", None) if quota else None

    @property
    def model_daily_remaining(self) -> dict[str, int]:
        quota = getattr(self.provider, "quotas", None)
        if quota and hasattr(quota, "dimensions"):
            model_remaining: dict[str, int] = {}
            for dim in quota.dimensions():
                if isinstance(dim.name, str) and dim.name.startswith("requests_daily_model:"):
                    model_remaining[dim.name.split(":", 1)[1]] = dim.remaining or 0
            if model_remaining:
                return model_remaining
        if quota and hasattr(quota, "model_remaining"):
            return quota.model_remaining
        return {}

    @property
    def concurrent_used(self) -> int:
        return self._in_flight

    @property
    def username(self) -> str:
        return self.provider.username

    @property
    def channel(self):
        """所属渠道领域对象（由 ProviderPool 注入的共享 Channel）。"""
        return getattr(self.provider, "_channel", None)

    @property
    def priority(self) -> int:
        ch = self.channel
        return getattr(ch, "priority", self._legacy_priority) if ch else self._legacy_priority

    @property
    def weight(self) -> int:
        ch = self.channel
        return max(1, getattr(ch, "weight", self._legacy_weight)) if ch else self._legacy_weight


class ProviderPool:
    """单个提供商的账号池"""

    def __init__(self, provider_name: str, client_class: type[BaseProvider]):
        self.provider_name = provider_name
        self.client_class = client_class
        self.clients: list[AccountClient] = []
        self._current_index = 0
        self._lock = asyncio.Lock()
        # 渠道领域对象：承载渠道级配置与行为，所有账号共享同一实例。
        self.channel: Channel = Channel(provider_name)

    @property
    def enabled(self) -> bool:
        """渠道是否启用（正式发送/定时任务读这个；手动测试/拉模型列表 bypass）。"""
        return getattr(self.channel, "enabled", True)

    def freeze_refresh_on_failure(self) -> bool:
        """渠道级「失败刷新冻结周期」：关时已冻结对象保留现有到期时间，不被后续失败刷新。

        只看渠道配置，与请求模式无关。池外无渠道时兜底默认开（宁刷新不静默留住过长冻结）。
        """
        channel = getattr(self, "channel", None)
        if channel is None or not hasattr(channel, "freeze_refresh_on_failure"):
            return True
        try:
            return bool(channel.freeze_refresh_on_failure())
        except Exception:
            return True

    def add_accounts(self, accounts: list[dict], rpm_limit: int, provider_extra: dict = None):
        """添加账号"""
        provider_extra = dict(provider_extra or {})
        # 先建/更新共享 Channel：把渠道级配置（含 enabled/priority/weight/billing_mode/协议/路径/能力）原子快照到渠道对象。
        self.channel.apply(provider_extra)
        rate_limit = provider_extra.get("rate_limit", {})
        policy = provider_extra.get("limit_policy") or {}
        policy_enabled = policy.get("enabled", True) is not False
        if policy and not policy_enabled:
            tpm_limit = 0
            concurrent_limit = 0
            rpm_limit = 0
            rpd_limit = 0
            rph_limit = 0
            tph_limit = 0
            tpd_limit = 0
        else:
            tpm_limit = int(policy.get("account_tpm", rate_limit.get("tpm_per_account", 0)) or 0)
            concurrent_limit = int(policy.get("account_concurrent", rate_limit.get("concurrent_per_account", 0)) or 0)
            rpm_limit = int(policy.get("account_rpm", rpm_limit) or 0)
            rpd_limit = int(policy.get("account_rpd", rate_limit.get("rpd_per_account", 0)) or 0)
            rph_limit = int(policy.get("account_rph") or 0)
            tph_limit = int(policy.get("account_tph") or 0)
            tpd_limit = int(policy.get("account_tpd") or 0)
        for acc in accounts:
            # 账号禁用（switch=False）只软跳过发送消息路由；账号依旧加载，保证可测试/拉模型列表/认证。
            is_disabled = acc.get("switch") is False

            # 账号级配置只保留凭证/代理/账号覆盖；渠道级字段由 self.channel 提供。
            account_only_keys = ("username", "password", "proxy", "url_prefix", "switch", "balance", "balance_threshold",
                                 "tpm_limit", "tpm_per_account", "concurrent_limit", "concurrent_per_account",
                                 "is_frozen", "priority", "weight", "account_priority", "account_weight",
                                 "enabled", "rate_limit", "limit_policy")
            extra = {k: v for k, v in provider_extra.items() if k not in account_only_keys}
            extra.update({k: v for k, v in acc.items() if k not in account_only_keys})

            provider = self.client_class(
                username=acc["username"],
                password=acc.get("password", ""),
                proxy=acc.get("proxy"),
                url_prefix=acc.get("url_prefix"),
                **extra
            )
            # 同渠道账号共享同一 Channel 实例；attach 后 provider 的渠道级 property 直接读它。
            provider.attach_channel(self.channel)
            account_rpm_limit = rpm_limit if getattr(provider, "USES_ACCOUNT_RPM", True) else 0
            self.clients.append(AccountClient(
                provider,
                account_rpm_limit,
                tpm_limit=int(acc.get("tpm_limit", acc.get("tpm_per_account", tpm_limit)) or 0),
                concurrent_limit=int(acc.get("concurrent_limit", acc.get("concurrent_per_account", concurrent_limit)) or 0),
                rpd_limit=rpd_limit,
                rph_limit=int(acc.get("rph_limit", rph_limit) or 0),
                tph_limit=int(acc.get("tph_limit", tph_limit) or 0),
                tpd_limit=int(acc.get("tpd_limit", tpd_limit) or 0),
                balance=float(acc.get("balance") or 0) if acc.get("balance") is not None else None,
                balance_threshold=float(acc.get("balance_threshold") or 0),
                is_frozen=bool(acc.get("is_frozen") or False),
                disabled=bool(is_disabled),
                disable_reason="switch_off" if is_disabled else "",
                metadata=dict(acc.get("metadata") or {}),
            ))

    async def restore_cooldowns(self):
        """从 Redis 兜底回灌账号冻结态到内存桶（重启后调用）。

        账号级 / 模型级冻结都写进对应 AccountClient 的 _freeze_state（内存为准）。
        永久冻结（Redis 无 TTL key）回灌为 until=None；临时冻结回灌为到期点。
        模型级若不在启动时回灌，重启后 Redis 里到月底的冻结 key 仍在、却会被选号
        重新选中——冻结名存实亡。
        """
        now = time.time()
        # 账号级回灌
        for client in self.clients:
            try:
                info = await RedisLimitBackend.get_account_cooldown_info(self.provider_name, client.username)
                if info is None:
                    continue
                reason = str(info.get("reason") or "")
                if info.get("permanent"):
                    client.freeze(kind="account", seconds=None, reason=reason)
                else:
                    until = float(info.get("until") or 0)
                    if until > now:
                        client.set_freeze_state_raw(account_until=until, reason=reason)
            except Exception:
                pass
        # 模型级回灌：Redis 是跨重启真相源，内存桶必须从冻结索引重建。
        try:
            model_map = await RedisLimitBackend.batch_get_provider_model_cooldowns(self.provider_name)
            for username, entries in (model_map or {}).items():
                client = ModelClientPool._find_account_client(self.provider_name, username)
                if client is None:
                    continue
                for e in entries:
                    model = e.get("model") or ""
                    if not model:
                        continue
                    reason = str(e.get("reason") or "")
                    if e.get("permanent"):
                        client.freeze(kind="account_model", model_id=model, seconds=None, reason=reason)
                    else:
                        until = float(e.get("until") or 0)
                        if until > now:
                            client.set_freeze_state_raw(model_untils={model: until}, reason=reason)
        except Exception:
            logger.exception(f"[{self.provider_name}] 模型级冷却回灌失败")

    async def get_available_client(self, exclude_accounts: list[str] = None) -> BaseProvider | None:
        """轮询获取可用客户端，可排除指定账号"""
        if not self.clients:
            return None

        exclude_accounts = exclude_accounts or []

        async with self._lock:
            if not self.clients:
                return None
            # 账号被增删后 _current_index 可能越界，访问前先归一
            if self._current_index >= len(self.clients) or self._current_index < 0:
                self._current_index = 0
            # 遍历所有账号
            for _ in range(len(self.clients)):
                client = self.clients[self._current_index]
                self._current_index = (self._current_index + 1) % len(self.clients)

                # 排除指定账号
                if client.username in exclude_accounts:
                    continue

                if await client.check_available():
                    return client.provider

            # 所有账号都不可用
            return None

    async def get_initialized_provider(self) -> BaseProvider | None:
        """拉上游模型列表专用：只看账号凭据是否已初始化，不走请求级限流（冷却/RPM/TPM/并发）。

        但**排除 disabled 账号**：禁用是手动开关（"别用这个账号"），不是限流状态，
        与冷却/冻结两码事。与本函数下游 fetch_provider_upstream_models 的兜底
        初始化循环同源——那里也跳过 disabled。
        """
        if not self.clients:
            return None
        async with self._lock:
            if not self.clients:
                return None
            if self._current_index >= len(self.clients) or self._current_index < 0:
                self._current_index = 0
            for _ in range(len(self.clients)):
                client = self.clients[self._current_index]
                self._current_index = (self._current_index + 1) % len(self.clients)
                if client.disabled:
                    continue
                if client.provider.is_init():
                    return client.provider
            return None

    def mark_account_cooldown(self, username: str, seconds: int, reason: str = ""):
        seconds = max(0, int(seconds or 0))
        if seconds <= 0:
            return
        reason = summarize_freeze_reason(reason)
        refresh = self.freeze_refresh_on_failure()
        for client in self.clients:
            if client.username == username:
                changed = client.freeze(kind="account", seconds=seconds, reason=reason, refresh=refresh)
                if not changed:
                    logger.debug(f"[{self.provider_name}] 账号 {username} 已在冷却中，保留现有到期时间")
                    return
                asyncio.create_task(RedisLimitBackend.set_account_cooldown(self.provider_name, username, seconds, reason))
                suffix = f"，原因: {reason}" if reason else ""
                logger.warning(f"[{self.provider_name}] 账号 {username} 已设置 {seconds} 秒冷却{suffix}")
                break

    def mark_account_429(self, username: str, cooldown_seconds: int | None = None, reason: str = "") -> str:
        """标记账号遇到限频状态码，按 freeze_policy 设置冻结。"""
        return self.apply_freeze_policy(username, status_code=429, reason=reason or "rate_limit")

    def mark_account_exception(self, username: str, cooldown_seconds: int | None = None, reason: str = "", error_code: str | None = None, model_id: str | None = None) -> str:
        """标记账号遇到异常，按 freeze_policy 设置冻结；未命中时按全局配置兜底。"""
        result = self.apply_freeze_policy(username, exception=True, reason=reason or "exception", error_code=error_code, model_id=model_id)
        if not result:
            try:
                raw = config.Config._load().get("rate_limit", {}).get("exception_cooldown_seconds")
                seconds = int(raw) if raw is not None else 300
            except (TypeError, ValueError):
                seconds = cooldown_seconds if cooldown_seconds and cooldown_seconds > 0 else 60
            if seconds > 0:
                self._apply_fallback_cooldown(username, seconds, reason or "exception", model_id)
                return "freeze"
        return result

    def clear_account_cooldown(self, username: str):
        """清除账号冷却（解冻），同时清除 Redis 中的冷却记录"""
        for client in self.clients:
            if client.username == username:
                client.unfreeze(kind="account")
                break
        asyncio.create_task(RedisLimitBackend.clear_account_cooldown(self.provider_name, username))

    def clear_all_cooldowns(self) -> int:
        """批量清除本渠道下所有账号的冷却（内存 + Redis）。

        账号级 + 模型级冷却一并清除。返回 clearable 账号数（即使全部为 0 也会扫描 Redis）。
        Redis 端使用 SCAN+DEL，避免阻塞 Redis。
        """
        cleared = 0
        for client in self.clients:
            client.unfreeze(kind="account")
            cleared += 1
        asyncio.create_task(RedisLimitBackend.clear_all_provider_cooldowns(self.provider_name))
        return cleared

    def clear_runtime_freeze_on_success(self, username: str, model_id: str | None = None) -> bool:
        """请求/测试成功后，自动解除该账号的冻结，让账号恢复正常。

        清理账号永久冻结状态 ``is_frozen``、账号级冷却、连续失败熔断，以及成功模型的
        模型级冷却；不触碰 ``disabled``（账号 switch 关闭），禁用不是冻结，也不会进入
        定时检测候选。

        成功即证明账号可达：账号级冷却或永久冻结存在时清整账号（含其所有模型级冷却）；
        仅模型级冷却时只清成功的这个模型，其余模型的冷却保留。

        返回是否确实清除了任何冻结（未冻结时直接返回 False，避免无谓广播）。
        """
        if not username:
            return False
        client = None
        for c in self.clients:
            if c.username == username:
                client = c
                break
        if client is None:
            return False
        now = time.time()
        account_frozen = bool(client.is_frozen)  # 账号级冻结（永久 + 临时）
        model_cooling = bool(model_id) and client.is_model_frozen(model_id)
        if not (account_frozen or model_cooling):
            return False
        # 清账号级冻结桶。连续失败/成功计数（评分信号）由 record_success 维护，不在此清。
        if account_frozen:
            client.unfreeze(kind="account")
        # Redis 清除是 I/O，仅在有事件循环时调度（同步单测无 loop 时纯内存清除即可）。
        try:
            asyncio.get_running_loop()
            has_loop = True
        except RuntimeError:
            has_loop = False
        if account_frozen:
            # 整账号冻结过：账号已证明可达，Redis 侧账号级 + 全部模型级冻结键一并清除，并账号级广播。
            if has_loop:
                asyncio.create_task(RedisLimitBackend.clear_account_cooldown(self.provider_name, username))
                self._broadcast_cooldown_cleared(username)
        elif model_cooling:
            # 只有成功的这个模型被冻结：只清它，别的模型冻结保留。
            client.unfreeze(kind="account_model", model_id=model_id)
            if has_loop:
                asyncio.create_task(RedisLimitBackend.clear_account_model_cooldown(self.provider_name, username, model_id))
                self._broadcast_cooldown_cleared(username, model_id=model_id)
        suffix = f" 模型 {model_id}" if (model_cooling and not account_frozen) else ""
        logger.info(f"[{self.provider_name}] 账号 {username}{suffix} 请求成功，自动解除冻结")
        return True

    def _broadcast_cooldown_cleared(self, username: str, model_id: str | None = None) -> None:
        """广播解冻事件，让其它实例同步清内存冷却（与 _freeze_account 的冻结广播对称）。"""
        try:
            import runtime_sync
            extra = {"provider": self.provider_name, "username": username, "cleared": True}
            if model_id:
                extra["model_id"] = model_id
            asyncio.create_task(runtime_sync.publish(
                runtime_sync.EVENT_COOLDOWN,
                f"{self.provider_name}:{username}",
                extra=extra,
            ))
        except Exception:
            pass

    def apply_freeze_policy(self, username: str, status_code: int | None = None, headers: dict | None = None, model_id: str | None = None, reason: str | None = None, exception: bool = False, error_code: str | None = None) -> str:
        """根据 freeze_policy 规则评估是否冻结该账号。

        返回值:
        - "freeze"    : 命中规则，已执行临时/永久冻结或禁用动作
        - "no_freeze" : 命中不冻结规则，跳过冻结
        - ""          : 未命中、规则未启用或动作非法
        """
        if not username:
            return ""
        # 冻结决策交给渠道领域对象（规则本就是渠道级）；池外/无渠道时兜底旧路径。
        channel = getattr(self, "channel", None)
        if channel is not None:
            matched = channel.match_freeze(status_code=status_code, headers=headers, exception=exception, error_code=error_code)
        else:
            try:
                policy = get_effective_provider_policy_sync(self.provider_name)
            except Exception:
                return ""
            matched = match_freeze_rules(policy.get("freeze_policy"), status_code=status_code, headers=headers, exception=exception, error_code=error_code)
        if not matched:
            return ""
        if matched["freeze_mode"] == "no_freeze":
            return "no_freeze"
        scope = matched["scope"]
        seconds = matched["seconds"]
        freeze_mode = matched["freeze_mode"]
        permanent = bool(matched.get("permanent"))
        disable = bool(matched.get("disable"))
        freeze_reason = summarize_freeze_reason(reason) or f"freeze_policy:{freeze_mode}"
        # 若是 model scope 但 model_id 缺失，降级到更宽的 scope（account_model→account，channel_model→channel）
        if scope == "channel_model" and not model_id:
            scope = "channel"
        if scope == "account_model" and not model_id:
            scope = "account"

        # 禁用（disabled）：账号 switch off / 渠道 enabled off。是禁用不是冻结，
        # 不写 TTL、不写 is_frozen，不被定时检测解冻（禁用账号永不进入检测候选）。
        if disable:
            if scope not in ("account", "channel"):
                logger.error(f"[{self.provider_name}] 禁用仅支持账号或渠道，收到 scope={scope}")
                return ""
            self._apply_disable(scope, username, freeze_reason)
            self._notify_freeze(scope, username, model_id, seconds, freeze_reason, permanent=False, disabled=True)
            return "freeze"

        # 永久冻结（permanent）：仅账号，置 is_frozen=True。是冻结，可被定时检测成功解冻
        # （clear_runtime_freeze_on_success 会清 is_frozen）。available_map 把 is_frozen 归入异常供探测。
        if permanent:
            if scope != "account":
                logger.error(f"[{self.provider_name}] 永久冻结仅支持账号，收到 scope={scope}")
                return ""
            self._apply_permanent_freeze(scope, username, freeze_reason)
            self._notify_freeze(scope, username, model_id, seconds, freeze_reason, permanent=True)
            return "freeze"

        if scope == "channel_model":
            # 渠道-模型固定时长：冻结本渠道所有账号的该模型（不冻结账号本身）
            for client in self.clients:
                self._freeze_account_model(client.username, model_id, seconds, freeze_reason)
            logger.warning(f"[{self.provider_name}] 渠道所有账号模型 {model_id} 已冻结 {seconds} 秒，原因: {freeze_reason}")
        elif scope == "channel":
            # 渠道固定时长：冻结本渠道所有账号
            for client in self.clients:
                self._freeze_account(client, seconds, freeze_reason)
            logger.warning(f"[{self.provider_name}] 渠道所有账号已冻结 {seconds} 秒，原因: {freeze_reason}")
        elif scope == "account_model":
            self._freeze_account_model(username, model_id, seconds, freeze_reason)
            logger.warning(f"[{self.provider_name}] 账号 {username} 模型 {model_id} 已冻结 {seconds} 秒，原因: {freeze_reason}")
        else:
            for client in self.clients:
                if client.username == username:
                    self._freeze_account(client, seconds, freeze_reason)
                    break
            logger.warning(f"[{self.provider_name}] 账号 {username} 已冻结 {seconds} 秒，原因: {freeze_reason}")
        # 仅渠道级冻结需人工介入，落通知中心；单账号级冻结不发，避免刷屏。
        self._notify_freeze(scope, username, model_id, seconds, freeze_reason, permanent)
        return "freeze"

    def _apply_disable(self, scope: str, username: str | None, reason: str) -> None:
        """禁用账号或渠道并持久化；禁用不是冻结，不写 TTL / is_frozen。"""
        if scope == "channel":
            channel = getattr(self, "channel", None)
            if channel is not None:
                channel.enabled = False
            logger.warning(f"[{self.provider_name}] 渠道已禁用（enabled=False），原因: {reason}")
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return

            async def _persist_channel_disabled():
                try:
                    from admin import _read_provider_config, _write_provider_base
                    cfg = await _read_provider_config(self.provider_name)
                    cfg["enabled"] = False
                    await _write_provider_base(self.provider_name, cfg)
                except Exception:
                    logger.exception(f"[{self.provider_name}] 禁用渠道持久化失败")

            asyncio.create_task(_persist_channel_disabled())
            return

        if scope != "account":
            logger.error(f"[{self.provider_name}] 不支持对 {scope} 执行禁用")
            return
        client = next((c for c in self.clients if c.username == username), None)
        if client is None:
            return
        client.disabled = True
        client.disable_reason = "switch_off"
        logger.warning(f"[{self.provider_name}] 账号 {username} 已禁用（switch=False），原因: {reason}")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _persist_account_disabled():
            try:
                from admin import _persist_account, _read_provider_config
                cfg = await _read_provider_config(self.provider_name)
                acc = next((a for a in cfg.get("accounts", []) if a.get("username") == username), None)
                if acc is None:
                    logger.warning(f"[{self.provider_name}] 禁用账号 {username} 持久化失败：配置中找不到账号")
                    return
                acc["switch"] = False
                await _persist_account(self.provider_name, acc)
            except Exception:
                logger.exception(f"[{self.provider_name}] 禁用账号 {username} 持久化失败")

        asyncio.create_task(_persist_account_disabled())

    def _apply_permanent_freeze(self, scope: str, username: str | None, reason: str) -> None:
        """永久冻结账号：内存桶 until=None + Redis 无 TTL key 兜底 + 广播。不写 DB is_frozen。"""
        if scope != "account":
            logger.error(f"[{self.provider_name}] 永久冻结仅支持账号，收到 scope={scope}")
            return
        client = next((c for c in self.clients if c.username == username), None)
        if client is None:
            return
        # 永久冻结是冻结对象升级（临时→永久），不是周期刷新，不受「失败刷新冻结周期」限制。
        self._freeze_account(client, None, reason, refresh=True)
        logger.warning(f"[{self.provider_name}] 账号 {username} 已永久冻结，原因: {reason}")

    def _notify_freeze(self, scope: str, username: str, model_id: str | None,
                       seconds: int, reason: str, permanent: bool, disabled: bool = False) -> None:
        """冻结/禁用 → 通知中心 + 出站 outbox（best-effort，绝不影响主流程）。

        channel 级（整渠道/整渠道模型）= ``channel.frozen``；account 级 =
        ``account.frozen``。原先只通知渠道级以避刷屏，现改为统一通知，刷屏由
        订阅规则 + dedupe_key 控制（管理员只订阅关心的渠道/事件即可）。
        emit_notification_background 内部已 upsert_notification + 写 outbox，
        这里不再单独 upsert。
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if scope.startswith("channel"):
            event_type = "channel.frozen"
            severity = "critical" if disabled else "error"
            if disabled:
                title = f"渠道 {self.provider_name} 已被禁用"
                message = f"渠道已停用，需人工启用，原因: {reason}"
            elif model_id:
                title = f"渠道 {self.provider_name} 模型 {model_id} 已被冻结"
                message = f"渠道所有账号模型 {model_id} 已冻结 {seconds} 秒，原因: {reason}"
            else:
                title = f"渠道 {self.provider_name} 已被冻结"
                message = f"渠道所有账号已冻结 {seconds} 秒，原因: {reason}"
        else:
            event_type = "account.frozen"
            severity = "warn"
            if model_id:
                title = f"账号 {username} 模型 {model_id} 已被冻结"
                message = f"账号 {username} 的模型 {model_id} 已冻结 {seconds} 秒，原因: {reason}"
            else:
                title = f"账号 {username} 已被冻结"
                message = f"账号 {username} 已冻结 {seconds} 秒，原因: {reason}"
        params = {
            "provider_name": self.provider_name,
            # 渠道级冻结影响本渠道全部账号，不能归因到恰好触发它的那一个账号，
            # 否则通知列表/订阅过滤会把整渠道停摆误读成单账号故障。
            "account_username": None if scope.startswith("channel") else username,
            "model_id": model_id or None,
            "scope": scope,
            "reason": reason,
            "seconds": seconds,
            "permanent": permanent,
            "disabled": disabled,
        }
        # dedupe 主体与归因保持同一口径：渠道级不含账号（否则同一次整渠道停摆会因
        # 触发它的账号不同而生成不同 key，去重失效、通知刷屏）；账号级才带账号名。
        subject = "" if scope.startswith("channel") else (username or "")
        dedupe_key = f"freeze:{self.provider_name}:{scope}:{subject}:{model_id or ''}"
        from monkeycode_compat.notify_core import emit_notification_background
        emit_notification_background(
            event_type,
            params=params,
            owner_type="platform",
            severity=severity,
            kind="account",
            source="freeze_policy",
            title=title,
            message=message,
            dedupe_key=dedupe_key,
            dedupe_window_seconds=3600 if (permanent or disabled) else 300,
        )

    def _freeze_account(self, client, seconds: int | None, reason: str, *, refresh: bool | None = None) -> bool:
        """冻结单账号：内存桶 + Redis 兜底 + pubsub 广播。seconds=None 永久冻结。

        refresh 缺省读取渠道「失败刷新冻结周期」；关时已冻结账号保留原 TTL，且不再写 Redis/广播。
        永久冻结等升级动作可显式传 refresh=True。
        """
        should_refresh = self.freeze_refresh_on_failure() if refresh is None else bool(refresh)
        changed = client.freeze(kind="account", seconds=seconds, reason=reason, refresh=should_refresh)
        if not changed:
            return False
        asyncio.create_task(RedisLimitBackend.set_account_cooldown(
            self.provider_name, client.username, seconds, reason))
        self._broadcast_cooldown(client.username, model_id=None, seconds=seconds, reason=reason)
        return True

    def _freeze_account_model(self, username: str, model_id: str, seconds: int | None, reason: str, *, refresh: bool | None = None) -> bool:
        """冻结账号+模型：内存桶 + Redis 兜底 + pubsub 广播。seconds=None 永久冻结。

        Redis SET 必须先于广播完成：接收端（含本实例自收自发）靠读 Redis 落桶，
        若广播先到、SET 未落地，会读到 None 而误清，导致月底冻结被重新选中。
        """
        client = next((c for c in self.clients if c.username == username), None)
        should_refresh = self.freeze_refresh_on_failure() if refresh is None else bool(refresh)
        if client is not None:
            changed = client.freeze(
                kind="account_model", model_id=model_id, seconds=seconds, reason=reason, refresh=should_refresh,
            )
            if not changed:
                return False
        asyncio.create_task(self._freeze_account_model_async(username, model_id, seconds, reason))
        return True

    async def _freeze_account_model_async(self, username: str, model_id: str, seconds: int | None, reason: str) -> None:
        """先写 Redis（权威），再广播；保证事件到达时 Redis 已有 key。"""
        try:
            await RedisLimitBackend.set_account_model_cooldown(
                self.provider_name, username, model_id, seconds, reason)
        except Exception:
            pass
        self._broadcast_cooldown(username, model_id=model_id, seconds=seconds, reason=reason)

    def _apply_fallback_cooldown(self, username: str, seconds: int, reason: str, model_id: str | None) -> None:
        """兜底冷却：有 model_id 时只冻结账号+模型，否则退回整账号冷却。"""
        seconds = max(0, int(seconds or 0))
        if seconds <= 0:
            return
        if model_id:
            freeze_reason = summarize_freeze_reason(reason)
            self._freeze_account_model(username, model_id, seconds, freeze_reason)
            suffix = f"，原因: {freeze_reason}" if freeze_reason else ""
            logger.warning(f"[{self.provider_name}] 账号 {username} 模型 {model_id} 兜底冻结 {seconds} 秒{suffix}")
        else:
            self.mark_account_cooldown(username, seconds, reason)

    def _broadcast_cooldown(self, username: str, model_id: str | None, seconds: int | None, reason: str) -> None:
        try:
            import runtime_sync
            # 接收端以 Redis 为权威、忽略此 until 字段；permanent(seconds=None) 时置 None，仅作信号。
            until = None if seconds is None else (time.time() + seconds)
            asyncio.create_task(runtime_sync.publish(
                runtime_sync.EVENT_COOLDOWN,
                f"{self.provider_name}:{username}",
                extra={"provider": self.provider_name, "username": username,
                       "model_id": model_id, "until": until, "reason": reason},
            ))
        except Exception:
            pass

    def handle_response_error(self, username: str | None, status_code: int, reason: str = "", model_id: str | None = None) -> str:
        """请求异常的统一入口，按 freeze_policy 冻结该账号；未命中时按全局配置兜底。

        返回值：
        - ``"freeze"``: 已写入 Redis 冷却
        - ``""``: 未命中或未启用
        """
        if not username:
            return ""
        try:
            code = int(status_code)
        except (TypeError, ValueError):
            return ""
        result = self.apply_freeze_policy(username, status_code=code, reason=reason, model_id=model_id)
        if not result:
            try:
                raw = config.Config._load().get("rate_limit", {}).get("cooldown_seconds")
                seconds = int(raw) if raw is not None else 60
            except (TypeError, ValueError):
                seconds = 60
            if seconds > 0:
                self._apply_fallback_cooldown(username, seconds, reason or f"http_{code}", model_id)
                return "freeze"
        return result

    async def init_all(self, *, use_invitation_interval: bool = True):
        """初始化所有账号"""
        if not self.clients:
            return
        # 定时/后台初始化路径（Path B）：渠道禁用时整体跳过 init_auth。
        if self.channel is not None and not self.channel.enabled:
            return
        for c in self.clients:
            # 账号 switch off（Path B）：跳过定时初始化，账号仍留在池中供手动测试/拉模型。
            if c.disabled:
                continue
            try:
                ok = await asyncio.wait_for(c.provider.init_auth(True), timeout=30)
                c.auth_ok = bool(ok)
                c.auth_checked_at = time.time()
                if ok:
                    c.auth_error = ""
                elif getattr(c.provider, "SUPPORTS_TOKEN_AUTO_REFRESH", False):
                    c.auth_error = "认证已过期，等待自动刷新"
                else:
                    c.auth_error = "认证已失效，需人工更新凭据"
            except asyncio.TimeoutError:
                c.auth_ok = False
                c.auth_checked_at = time.time()
                c.auth_error = "初始化超时"
                logger.error(f"{self.provider_name} 账号 {c.username} 初始化超时")
            except Exception:
                c.auth_ok = False
                c.auth_checked_at = time.time()
                c.auth_error = "初始化失败"
                # 通知走统一事件域：是否进通知中心/webhook 由订阅配置决定，
                # 这里只负责产生事件，绝不直接写 notifications 表。
                from monkeycode_compat.notify_core import emit_notification_background
                emit_notification_background(
                    "account.init_failed",
                    params={
                        "provider_name": self.provider_name,
                        "account_username": c.username,
                        "upstream": c.provider.PROVIDER_NAME,
                    },
                    owner_type="platform",
                    severity="error",
                    kind="account",
                    source="account_init",
                    title=f"{c.provider.PROVIDER_NAME}渠道 {self.provider_name} 账号初始化失败",
                    message=f"账号 {c.username} 初始化失败",
                    detail=traceback.format_exc(),
                    dedupe_key=f"account_init:{self.provider_name}:{c.username}",
                    dedupe_window_seconds=300,
                )
                logger.error(f"{self.provider_name} 账号 {c.username} 初始化失败: {traceback.format_exc()}")
            if use_invitation_interval:
                await asyncio.sleep(c.provider.invitation_interval)

    async def clean_message(self, max_age_hours):
        for c in self.clients:
            try:
                if c.disabled or c.auth_ok is not True:
                    continue
                if c.provider.is_init():
                    await c.provider.clear_conversations(max_age_hours)
            except:
                logger.error(f"{self.provider_name} 账号 {c.username} 清除历史消息失败: {traceback.format_exc()}")
            await asyncio.sleep(c.provider.invitation_interval)

    async def check_account(self, *, force: bool = False):
        # 定时/后台校验路径（Path B）：渠道禁用整体跳过；账号 switch off 单个跳过。
        # 手动检查可绕过渠道级门禁，但仍尊重账号自身的 switch。
        if not force and self.channel is not None and not self.channel.enabled:
            logger.info(f"{self.provider_name} 渠道已禁用，跳过账号状态检查")
            return
        health_cfg = getattr(self.channel, "health_check", {}) if self.channel is not None else {}
        if not force and isinstance(health_cfg, dict) and health_cfg.get("enabled") is False:
            return
        is_not_auth_list = {}
        for c in self.clients:
            if c.disabled:
                continue
            try:
                supports_auto_refresh = bool(getattr(c.provider, "SUPPORTS_TOKEN_AUTO_REFRESH", False))
                if supports_auto_refresh:
                    # 可自愈渠道定时体检时先走 init_auth(True)：它会在 access_token
                    # 过期/缺失时用 refresh_token、账号密码或设备指纹主动刷新。
                    # 只跑 health_check 会让 Atomcode/CodeBuddy/Codex 等永远停在过期态。
                    is_auth = bool(await c.provider.init_auth(True))
                elif c.provider.is_init():
                    # jiekou/xiaomi/puter/cloudflare 等人工凭据渠道只验证，
                    # 不执行任何隐藏的重新登录或 cookie 获取动作。
                    is_auth = bool(await c.provider.health_check())
                else:
                    is_auth = False
                c.auth_ok = is_auth
                c.auth_checked_at = time.time()
                if is_auth:
                    c.auth_error = ""
                elif supports_auto_refresh:
                    c.auth_error = "认证已过期，等待自动刷新"
                else:
                    c.auth_error = "认证已失效，需人工更新凭据"
                if is_auth is False:
                    is_not_auth_list.setdefault(c.provider.PROVIDER_NAME, []).append(c.provider.username)
            except Exception:
                c.auth_ok = False
                c.auth_checked_at = time.time()
                c.auth_error = traceback.format_exc()
                logger.error(f"{self.provider_name} 账号 {c.username} 校验过期报错: {traceback.format_exc()}")
            await asyncio.sleep(random.uniform(2, 5))
        for provider_name, not_auth_user_list in is_not_auth_list.items():
            if len(not_auth_user_list) > 0:
                message = f"渠道： {provider_name}, 这些账号({','.join(not_auth_user_list)})已经退登，请检查原因"
                from monkeycode_compat.notify_core import emit_notification
                await emit_notification(
                    "account.logged_out",
                    params={
                        "provider_name": provider_name,
                        "account_usernames": list(not_auth_user_list),
                        "usernames": list(not_auth_user_list),
                    },
                    owner_type="platform",
                    severity="warn",
                    kind="account",
                    source="account_check",
                    title="模型平台账号检查异常",
                    message=message,
                    dedupe_key=f"account_check:{provider_name}",
                    dedupe_window_seconds=300,
                )


class ModelClientPool:
    """模型客户端池 - 一个模型可有多个提供商"""

    _provider_pools: dict[str, ProviderPool] = {}
    _models: list[dict] = []
    _models_by_id: dict[str, dict] = {}
    # 模型路由已下沉为各渠道 Channel.models 属性（内存真相源，请求分发时扫描）；
    # 全局 _model_routes/_model_providers/_model_upstream_map 已删除。
    _models_dirty: bool = False  # /v1/models 缓存脏标记：模型/元数据/分组变更时置脏，GET 时惰性重建
    _models_requested_generation: int = 0
    _models_built_generation: int = -1
    _models_rebuild_lock = asyncio.Lock()
    # 模型 TPM 固定窗口触线镜像：model -> 到期时间戳。选路只读内存，Redis 仅在 token 回写时累加。
    _model_tpm_cooldowns: dict[str, float] = {}
    _model_rr_index: dict[str, int] = {}  # model_id -> 轮询索引
    _channel_scores: dict[str, dict] = {}
    _provider_billing_mode: dict[str, str] = {}  # provider_name -> "token" | "request"
    # Header 模板库缓存：id -> headers(dict)；由 admin 写入后失效，provider 运行时读
    _header_templates: dict[str, dict] = {}
    _header_templates_loaded: bool = False
    # Per-model per-provider daily quota remaining (from upstream response headers)
    _model_provider_quota: dict[str, dict[str, int]] = {}  # model_id -> {provider_name: remaining}
    _provider_recent_picks: dict[str, list[float]] = {}  # provider_name -> reserve 成功时间戳（渠道级频率惩罚）
    _provider_pick_history: list[tuple[float, str]] = []  # 最近成功 reserve 的 (时间戳, provider_name) 序列，用于连续同渠道惩罚
    # Intelligent routing tuning constants
    PICK_WINDOW_SECONDS = 180  # 账号级频率惩罚滑窗长度（3 分钟）
    PICK_SOFT_CAP = 5          # 账号级线性惩罚软上限（窗口内命中 ≤ 此值不降权）
    ERROR_WINDOW_SECONDS = 60  # 账号级错误率滑窗长度（低错误期基线；高错误期由 ERROR_WINDOW_TIERS 动态缩短）
    # 全局错误水位 → 账号级错误率窗口阶梯缩短：失败是系统性的而非账号特异性时，加快账号恢复，避免高错误期饿死所有账号。
    # 阈值取 _global_recent_failure_count()（聚合 _channel_scores 各渠道 recent_failures 计数）。
    ERROR_WINDOW_TIERS = [(240, 15.0), (120, 25.0), (60, 40.0)]  # (全局近期失败阈值, 窗口秒) 由高到低
    # API Key 流量越高，对近期错误的渠道/账号越保守：error_rate × volume_factor 抑制
    VOLUME_RPM_TIERS = [(30, 0.0), (120, 0.3), (300, 0.6)]      # (RPM 用量阈值, volume_factor) 阶梯递增
    VOLUME_RPM_MAX = 0.8                                          # 封顶抑制强度
    VOLUME_ERROR_FLOOR = 0.2                                      # volume_error_factor 封底，避免完全饿死
    # Channel-level frequency penalty (exponential): 3 分钟内该渠道命中越多, factor 越低
    PROVIDER_PICK_THRESHOLD = 5     # 达到此值开始降权（第 5 次 0.55、第 6 次 0.30、第 8 次触底）
    PROVIDER_PICK_DECAY_BASE = 0.55 # 指数降权底数：(0.55)^(n-5+1)
    PROVIDER_PICK_FLOOR = 0.10      # 渠道级因子封底，避免完全饿死
    # 单次 attempt 内选择-预占循环的总时限与退避：防高并发下对 Redis 的热循环打点。
    # deadline 到点即停止本轮（返回 None，交由上层切下一 attempt/组）；backoff 只在 reserve
    # 失败后、下一次 select 前生效，指数增长带上限，测试模式（is_test）跳过以免拖慢。
    SELECTION_DEADLINE_MS = 3000    # 本轮选择-预占总时限（毫秒）
    SELECTION_BACKOFF_BASE_MS = 5   # reserve 失败首次退避基数（毫秒）
    SELECTION_BACKOFF_CAP_MS = 50   # 退避上限（毫秒）
    # Recent provider usage bucket penalty: 最近 10 分钟内的最近 20 次选择，用多的 provider 降到概率档
    PROVIDER_USAGE_HISTORY_WINDOW_SECONDS = 600  # 只看最近 10 分钟内的成功选择
    PROVIDER_USAGE_HISTORY_MAX_ROUNDS = 20       # 最多保留最近 20 轮成功选择
    PROVIDER_USAGE_5_COUNT = 3                   # 最近 5 轮命中 >=3：权重打折
    PROVIDER_USAGE_5_FACTOR = 0.65
    PROVIDER_USAGE_10_COUNT = 5                  # 最近 10 轮命中 >=5：进入低概率池
    PROVIDER_USAGE_10_FACTOR = 0.45
    PROVIDER_USAGE_10_MEDIAN_CAP = 0.35
    PROVIDER_USAGE_20_COUNT = 8                  # 最近 20 轮命中 >=8：极小概率池
    PROVIDER_USAGE_20_FACTOR = 0.25
    PROVIDER_USAGE_20_MEDIAN_CAP = 0.12
    # Consecutive-failure decay（仅用于 error_score 指数降权；一次成功即清零）
    CONSECUTIVE_DECAY_INTELLIGENT = 0.8   # intelligent: error_score *= 0.8^n
    CONSECUTIVE_DECAY_FAST = 0.7          # fast_intelligent: 对失败更不宽容
    # Daily reliability factor（Redis 日级成功/失败计数，跨重启）
    RELIABILITY_MIN_SAMPLES = 5           # 当天样本不足此值返回中性 1.0，不压新账号
    RELIABILITY_FLOOR = 0.2               # 可靠性下限，避免单候选被完全饿死
    # Exploration：带权随机中给次优候选留极小概率，避免一味死磕最快那一条
    EXPLORE_EPSILON = 0.05
    SPEED_NO_DATA_BASELINE = 0.7
    # fast_intelligent 间歇性探索轮：每轮以此概率忽略速度权重、在所有已进池的健康渠道间
    # 均匀随机撒一次流量，打破"top1/top2 两个渠道来回锁死、其余渠道拿不到机会"的粘性。
    # 只对 fast_intelligent 生效；不做冷渠道/坏渠道主动探测——池内候选已过健康过滤。
    FAST_EXPLORATION_RATE = 0.20
    # Session affinity / anti-affinity
    SESSION_AFFINITY_TTL = 600   # session 成功亲和 10 分钟内有效
    SESSION_AFFINITY_FACTOR = 1.3   # session 成功复用升权（与 upstream 亲和同量级，二者取最大值，不再连乘）
    SESSION_FAILURE_TTL = 300    # 全局失败计数 5 分钟内有效
    # 全平台单一失败计数器：所有请求（不分渠道/账号/客户端 api key/session）共用一个计数。
    # 任一请求报错 +1，任一请求成功清零。达 GLOBAL_FAILURE_TRIGGER 即进入"避险"模式：
    # 评分改为按各渠道近期成功率加权，把流量导向成功率高的渠道。
    GLOBAL_FAILURE_TRIGGER = 2   # 全局连续报错达此值即避险
    GLOBAL_STRESS_FLOOR = 0.05   # 避险时低成功率渠道的降权地板，避免完全饿死
    GLOBAL_STRESS_NO_DATA_SUCCESS = 0.7  # 无渠道样本不当成 100% 成功，给中性基线
    _GLOBAL_FAILURE_BUCKET = "__global__"
    # 连续成功让权：成功 streak 超过 grace 后指数衰减让权，失败即清零。
    # 与连续失败衰减对称，避免流量长期粘在同一个持续成功的账号上。
    CONSECUTIVE_SUCCESS_GRACE = 3    # 连续成功 ≤ 此值不让权
    CONSECUTIVE_SUCCESS_DECAY = 0.8  # 每超 1 次 ×0.8
    CONSECUTIVE_SUCCESS_FLOOR = 0.3  # 让权地板，避免完全饿死（与频率地板一致）
    # 同上游模型亲和：全局（跨 session）按 upstream_model_id 记录最近成功命中的账号，
    # 复用同一上游账号+同模型才有 prompt cache 命中收益，故同 account+upstream 时升权。
    UPSTREAM_MODEL_AFFINITY_TTL = 600   # 10 分钟内成功过的同模型同账号亲和升权
    UPSTREAM_MODEL_AFFINITY_FACTOR = 1.3
    # 短期成功率/响应均衡窗口：4 分钟内最近 10 次请求。
    # 报错后短期内偏向高成功率账号；某次响应远超自身历史均值则短期降权，均衡到响应短的账号。
    SHORT_WINDOW_SECONDS = 240
    SHORT_WINDOW_MAX_SAMPLES = 10
    SHORT_TERM_SUCCESS_BOOST = 1.2   # 窗口内全成功（且有失败记录）时的最高升权
    SHORT_TERM_SUCCESS_FLOOR = 0.6   # 窗口内全失败时的最低降权
    SHORT_TERM_LATENCY_OVERSHOOT = 1.8  # 最近一次耗时 > 中位×此值视为远超均值
    SHORT_TERM_LATENCY_PENALTY = 0.6    # 远超均值时短期降权因子
    # 绝对慢兜底：渠道 TTFT EWMA 或 账号近期成功中位耗时超过此值即重罚。
    # 30s 阈值兜底极端慢（公益站排队/卡死），避免误伤 opus 等正常慢响应（2-5s）。
    ABSOLUTE_SLOW_THRESHOLD_MS = 30000
    ABSOLUTE_SLOW_PENALTY = 0.5
    # 亲和封顶（仅在 score 函数内封顶 reuse_score，不改动底层 _session_factor /
    # _upstream_model_affinity_factor）。fast 更倾向快速切换，intelligent 保留一定复用稳定性。
    FAST_AFFINITY_CAP = 1.1
    INTELLIGENT_AFFINITY_CAP = 1.2
    # session_id -> {account_key, last_success_ts, model}
    _session_affinity: dict[str, dict] = {}
    # session_id -> {account_key -> {count, last_ts}}
    # 全平台单一失败计数器：key 永远是 _GLOBAL_FAILURE_BUCKET（不区分渠道/账号/客户端/session）
    _session_failures: dict[str, dict[str, dict]] = {}
    # upstream_model_id -> {account_key, last_success_ts}（全局同模型亲和，跨 session）
    _upstream_model_affinity: dict[str, dict] = {}
    # account_key -> list[{ts, success, duration_ms}]（短期窗口成功/失败/耗时）
    _short_term_outcomes: dict[str, list[dict]] = {}
    _lock = asyncio.Lock()
    _rr_lock = asyncio.Lock()
    _account_init_task: Optional[asyncio.Task] = None
    # 启动期账号加载完成信号：restore_cooldowns（冻结回灌）+ init_all（体检 auth_ok）
    # 跑完后置位。定时检测/认证体检两个 loop 首轮前等它，避免读到 auth_ok 仍为 None、
    # 冻结态尚未回灌的半成品状态（会把「尚未体检」误判成异常账号去探测）。
    # 无论初始化成功还是失败都会置位（见 _initialize_builtin_pools 的 finally），
    # 所以不会把 loop 永久挂死；启动流程本身不等它，不影响服务可用时间。
    # 默认置位：只有 initialize() 会 clear（那才是「初始化确实在进行中」的唯一时刻）。
    # 这样不经 initialize() 直接驱动 loop 的调用方（单测）不会被门禁挂住。
    _account_init_done: asyncio.Event = asyncio.Event()
    _account_init_done.set()
    _refresh_task: Optional[asyncio.Task] = None
    _delete_task: Optional[asyncio.Task] = None
    _check_task: Optional[asyncio.Task] = None
    _scheduled_test_task: Optional[asyncio.Task] = None
    # 各渠道「上一次定时检测触发」的时间戳（Unix 秒），与 scheduled_test_loop 的 last_run 同步更新；
    # 只读内存快照，供管理端渠道详情展示，服务重启即清零（与 last_run 同生命周期，非持久化审计数据）。
    _last_scheduled_test_triggered_at: dict[str, float] = {}
    # scheduled_test_loop 的检测队列与在途集合：提升为类属性，供 trigger_scheduled_test_now
    # 手动入队复用同一消费者池；仅在 loop 启动时实例化，未就绪时手动触发返回 (-1, None)。
    _scheduled_test_queue: Optional[asyncio.Queue] = None
    _scheduled_test_pending: Optional[set] = None
    # 单账号单轮定时检测最多探测的被冻模型数（按最快到期升序取前 N）。
    PROBE_MAX_FROZEN_MODELS: int = 5
    _clean_response_task: Optional[asyncio.Task] = None
    _last_response_clean_date: Optional[str] = None
    _hourly_stats_task: Optional[asyncio.Task] = None
    # 可靠性计数是可降级辅助数据：有界合并，避免每个请求结果都创建 Redis task。
    RELIABILITY_QUEUE_MAX = 4096
    RELIABILITY_FLUSH_INTERVAL = 0.25
    _reliability_pending: dict[str, int] = {}
    _reliability_dropped: int = 0
    _reliability_flush_task: Optional[asyncio.Task] = None

    @classmethod
    def register_provider(cls, provider_name: str, client_class: type[BaseProvider]):
        """注册提供商"""
        cls._provider_pools[provider_name] = ProviderPool(provider_name, client_class)

    @classmethod
    async def add_accounts(cls, provider_name: str, accounts: list[dict]):
        """添加账号"""
        pool = cls._provider_pools.get(provider_name)
        if pool:
            rpm = await config.Config.get_provider_rpm_limit(provider_name)
            provider_config = (await config.Config.get_providers()).get(provider_name, {})
            cls._provider_billing_mode[provider_name] = provider_config.get("billing_mode", "token")
            policy = await get_effective_provider_policy(provider_name)
            provider_extra = {k: v for k, v in provider_config.items() if k not in ("accounts", "rate_limit")}
            provider_extra.update({
                "rate_limit": provider_config.get("rate_limit", {}),
                "limit_policy": policy,
                "provider_name": provider_name
            })
            pool.add_accounts(accounts, rpm, provider_extra)

    @classmethod
    def resolve_upstream_id(cls, provider_name: str, model_id: str) -> str:
        """根据 (provider, model_id) 查上游真实 id；找不到返回 model_id 本身。"""
        pool = cls._provider_pools.get(provider_name)
        channel = getattr(pool, "channel", None) if pool else None
        if channel is not None:
            return channel.resolve_upstream_id(model_id)
        return model_id

    @staticmethod
    def _provider_model_mapping(model: dict) -> tuple[str, str]:
        upstream_id = (model.get("upstream_model_id") or model.get("id") or "").strip()
        model_id = (model.get("model_id") or model.get("id") or upstream_id).strip()
        return upstream_id, model_id or upstream_id

    @classmethod
    def _public_model_id(cls, provider_name: str, model_id: str) -> str:
        """上游模型 ID → 对外 model_id：切 "/" 前缀，再套渠道的 model_id 改写规则。

        纯函数式的「改写后应该叫什么」，不关心库里现状——落库与否由
        _resolve_target_model_id 判定。渠道对象拿不到时退化为只切前缀，
        保持无配置时的既有行为。
        """
        pool = cls.get_provider_pool(provider_name)
        channel = getattr(pool, "channel", None) if pool else None
        if channel is not None:
            return channel.public_model_id(model_id)
        return strip_model_owner_prefix(model_id)

    @classmethod
    def _channel_has_rewrite_rules(cls, provider_name: str) -> bool:
        """本渠道是否配了生效中的改写规则；拿不到渠道对象时按「没配」处理。"""
        pool = cls.get_provider_pool(provider_name)
        channel = getattr(pool, "channel", None) if pool else None
        return bool(channel is not None and channel.has_model_id_rewrite_rules())

    @classmethod
    def _is_regex_hit(cls, provider_name: str, raw_model_id: str) -> bool:
        """规则是否改了这个模型的名字——命中即 is_regex=True，导入时被过滤丢弃。

        只切 owner/ 前缀不算命中：那是无配置时的默认形态，不是规则。前端「模拟过滤
        及改写规则」开关只是预览这个标记；数据层过滤（手动导入、自动更新）都用它，
        两处不能各写各的判定。
        """
        return cls._public_model_id(provider_name, raw_model_id) != strip_model_owner_prefix(raw_model_id)

    @classmethod
    def _resolve_target_model_id(
        cls,
        provider_name: str,
        upstream_id: str,
        raw_model_id: str,
        tracked_map: dict,
    ) -> str:
        """决定这个上游模型对外应该叫什么——改写与落库的唯一判定入口。

        流程（与改写规则的语义同源）：拉到上游列表 → 过一遍改写规则得出目标名
        → 看目标名在本渠道是否已被占用 → 占了就不动，没占就用改写结果。

        存在性判断落在**改写后**的名字上，而不是拿上游原始 ID 去查 tracked_map。
        老写法只要这个 upstream_id 在库里整行就跳过改写，规则于是只对全新模型
        生效——改一版规则想让已入库的 xxx:free 变成真实名字做不到。

        但重算只在渠道**配了规则**时才发生：切 owner/ 前缀是无配置时的默认形态，
        不是规则。若不加这道闸，没配规则的渠道每轮定时同步都会把管理端里的手工
        改名（如 acme/kept → custom-kept）冲回默认形态 kept。

        撞名不让位：渠道的 model_id 只有普通索引、本来允许多个同名，deepseek-v4-flash-free
        改名后和原版 deepseek-v4-flash 撞成同名是正常的——两行 upstream_model_id 不同，
        各自一房。早期实现为了「避免撞名」让后到的行退回只切前缀，反而让规则改不生效，
        与「规则搜到就改名保留」的意图相悖，已删。
        """
        current = tracked_map.get(upstream_id)
        if current is not None and not cls._channel_has_rewrite_rules(provider_name):
            # 已跟踪 + 无规则：库里的值就是真相，手工改名不被默认形态冲掉。
            return current
        rewritten = cls._public_model_id(provider_name, raw_model_id)
        if rewritten == current:
            return rewritten
        return rewritten

    @classmethod
    def _build_upstream_model_rows(
        cls,
        provider_name: str,
        models: list[dict],
        tracked_map: dict,
    ) -> list[dict]:
        """把上游模型列表整形成带规则判定的行；管理端拉取与定时同步共用。

        每行的 is_regex / regex_model_id 就是规则判定的唯一结果：管理端「预览过滤
        结果」读它，定时同步也读它。两处共用一份计算，预览看到的过滤与改名就是真正
        落库的结果，不会各写一套判定再漂移。
        """
        rows: list[dict] = []
        for model in models:
            upstream_id, public_model_id = cls._provider_model_mapping(model)
            if not upstream_id:
                continue
            # 纯规则结果（切前缀 + 套规则），供前端判「这行有没有被规则命中」。
            regex_model_id = cls._public_model_id(provider_name, public_model_id)
            # 命中规则 = 规则改了名字；只切前缀不算（那是无配置时的默认形态）。
            is_regex = cls._is_regex_hit(provider_name, public_model_id)
            # 落库用的目标名：规则搜到就改名，撞名无所谓（渠道允许多个同名）。
            rewritten_model_id = cls._resolve_target_model_id(
                provider_name, upstream_id, public_model_id, tracked_map)
            rows.append({
                "upstream_model_id": upstream_id,
                "model_id": rewritten_model_id,
                "raw_model_id": public_model_id,
                "regex_model_id": regex_model_id,
                "is_regex": is_regex,
                "name": model.get("name") or public_model_id or upstream_id,
                "tracked": upstream_id in tracked_map,
                "current_model_id": rewritten_model_id,
                # Provider implementations may expose the original upstream
                # payload under raw; the base/custom provider path keeps it at
                # the model root, so preserve that metadata instead of
                # returning None and losing all importable fields.
                "raw": model.get("raw") if isinstance(model.get("raw"), dict) else model,
            })
        return rows

    @classmethod
    async def _initialize_builtin_pools(cls, pools: list[ProviderPool]):
        try:
            restore_results = await asyncio.gather(
                *(pool.restore_cooldowns() for pool in cls._provider_pools.values()),
                return_exceptions=True,
            )
            for pool, result in zip(cls._provider_pools.values(), restore_results):
                if isinstance(result, Exception):
                    logger.error(f"恢复账号冷却失败 provider={getattr(pool, 'provider_name', '?')}: {result}")

            init_results = await asyncio.gather(
                *(pool.init_all() for pool in pools),
                return_exceptions=True,
            )
            for pool, result in zip(pools, init_results):
                if isinstance(result, Exception):
                    logger.error(f"后台账号初始化失败 provider={getattr(pool, 'provider_name', '?')}: {result}")
            await cls.restore_model_tpm_cooldowns()
            await cls.clean_message()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error(f"后台账号初始化失败: {traceback.format_exc()}")
        finally:
            # 无论成功/失败/被取消都置位：账号体检失败不该把定时检测永久挂死，
            # 后续 tick 按当时真实 state 决定探测谁。shutdown 取消时置位同样无害
            # ——那时两个 loop 自己也在被取消。
            cls._account_init_done.set()

    @classmethod
    async def initialize(cls):
        """初始化账号，并启动模型管理定时任务。"""
        custom_pools = [
            pool for pool in cls._provider_pools.values()
            if pool.client_class is CustomProvider
        ]
        builtin_pools = [
            pool for pool in cls._provider_pools.values()
            if pool.client_class is not CustomProvider
        ]

        if custom_pools:
            await asyncio.gather(*(
                pool.init_all(use_invitation_interval=False)
                for pool in custom_pools
            ))

        # Route rows are local persisted state and must exist before inference routing starts.
        await cls.refresh_models(sync_upstream=False)

        cls._account_init_done.clear()
        cls._account_init_task = asyncio.create_task(
            cls._initialize_builtin_pools(builtin_pools)
        )
        cls._refresh_task = asyncio.create_task(cls.refresh_models_loop())

        if config.Config.message_delete_enabled():
            cls._delete_task = asyncio.create_task(cls.delete_message_loop())

        cls._clean_response_task = asyncio.create_task(cls.clean_response_loop())
        cls._check_task = asyncio.create_task(cls.check_account_loop())
        cls._scheduled_test_task = asyncio.create_task(cls.scheduled_test_loop())
        cls._hourly_stats_task = asyncio.create_task(cls._hourly_stats_loop())

    @classmethod
    async def check_account_loop(cls):
        # 首轮前等启动期账号加载完成（冻结回灌 + 体检），避免和 init_all 并发对同一批
        # 账号打重复认证请求、以及把半成品状态当真。启动流程不等这个 event。
        await cls._account_init_done.wait()
        last_check: dict[str, float] = {}
        while True:
            await asyncio.sleep(60)
            try:
                now = time.time()
                for provider_name in list(cls._provider_pools.keys()):
                    interval_minutes = await config.Config.get_provider_check_interval(provider_name)
                    if now - last_check.get(provider_name, 0) < interval_minutes * 60:
                        continue
                    last_check[provider_name] = now
                    pool = cls._provider_pools.get(provider_name)
                    if not pool:
                        continue
                    try:
                        # logger.info(f"定时刷新账号状态: {provider_name} (interval={interval_minutes}min)")
                        await pool.check_account()
                    except Exception as e:
                        logger.error(f"刷新 {provider_name} 失败: {e}")
                # 顺手清理过期的 session 亲和/反亲和记录，避免内存泄漏
                cls.cleanup_session_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"刷新失败: {e}")

    @classmethod
    def last_scheduled_test_triggered_at(cls, provider_name: str) -> float | None:
        """该渠道上一次定时检测到点触发的时间戳（Unix 秒）；从未触发过返回 None。"""
        return cls._last_scheduled_test_triggered_at.get(provider_name)

    @classmethod
    async def trigger_scheduled_test_now(cls, provider_name: str, overrides: dict | None = None) -> tuple[int, float | None]:
        """手动触发一次定时检测：用 overrides（前端当前表单）覆盖已保存配置的对应字段，
        按 _scheduled_test_collect_tasks 算出候选并入队，由 scheduled_test_loop 的 worker 异步消费，
        不阻塞调用方，立即返回 (入队账号数, 触发时间戳)。队列未就绪（loop 尚未启动）返回 (-1, None)。
        """
        queue = cls._scheduled_test_queue
        pending = cls._scheduled_test_pending
        if queue is None or pending is None:
            return -1, None
        pool = cls._provider_pools.get(provider_name)
        if not pool:
            return 0, None
        saved: dict = {}
        try:
            saved = await config.Config.get_provider_scheduled_test(provider_name)
        except Exception:
            saved = {}
        cfg = {**(saved or {}), **(overrides or {})}
        try:
            tasks = cls._scheduled_test_collect_tasks(provider_name, pool, cfg)
        except Exception as e:
            logger.error(f"[scheduled_test] {provider_name} 手动触发收集任务失败: {e}")
            return 0, None
        now = time.time()
        enqueued = 0
        for task in tasks:
            key = (provider_name, task["username"])
            if key in pending:
                continue
            task["key"] = key
            pending.add(key)
            queue.put_nowait(task)
            enqueued += 1
        cls._last_scheduled_test_triggered_at[provider_name] = now
        return enqueued, now

    @classmethod
    def _scheduled_test_channel_model_ids(cls, provider_name: str) -> list[str]:
        return [
            r.get("model_id") for r in cls.get_provider_routes(provider_name)
            if r.get("model_id")
        ]

    @classmethod
    def _scheduled_test_models_for_client(cls, cfg: dict, client, channel_model_ids: list[str]) -> list:
        """决定该账号本轮探测哪些模型。

        优先级：
          1) 用户显式配了 test_models/test_model → 尊重配置，原样用。
          2) 未配 + 账号 model_frozen → 测被冻模型（与渠道模型表求交集，封顶 N，
             最快到期的先测）；交集为空说明模型已删/改名，跳过该账号模型探测。
          3) 未配 + 其他状态 → 测该渠道自己的首个模型，绝不兜底全局目录第 0 个。
        """
        configured = cfg.get("test_models")
        if isinstance(configured, list) and configured:
            return list(configured)
        if cfg.get("test_model"):
            return [cfg.get("test_model")]
        if client.state() is AccountState.MODEL_FROZEN:
            valid = set(channel_model_ids)
            frozen = [m for m in client.frozen_model_ids() if m in valid]
            if not frozen:
                logger.info(
                    f"[scheduled_test] 账号 {client.username} 被冻模型已不在渠道模型表，"
                    f"跳过其模型探测（冻结关系应已被模型表变更清理）"
                )
                return []
            return frozen[:cls.PROBE_MAX_FROZEN_MODELS]
        return [channel_model_ids[0]] if channel_model_ids else [None]

    @classmethod
    def _scheduled_test_is_unavailable(cls, state: "AccountState | None") -> bool:
        """filter=unavailable 的「异常」判据；生产者与消费者共用，避免两处口径漂移。

        available 显然不算异常；checking 也不算——auth_ok=None 只表示「尚未体检出结果」，
        不是故障。把它算进异常会在启动后（体检未跑完）和新增账号后误发探测。
        """
        return state is not None and state not in (AccountState.AVAILABLE, AccountState.CHECKING)

    @classmethod
    def _scheduled_test_collect_tasks(cls, provider_name: str, pool, cfg: dict) -> list[dict]:
        """生产者纯逻辑：算出本轮该渠道要入队的**账号任务**列表。

        队列故意不固化 model：账号可能排队几十秒，期间账号级 TTL 会到期、部分模型冻结会
        变化、模型表会更新；消费者在真正发探测那一刻重新 state() + 重新选模型，才能满足
        「实时状态」语义，也避免探测排队期间已删除的账号/模型。

        候选筛选按规范 state()：available 算「可用」；异常见 _scheduled_test_is_unavailable
        （model_frozen 算异常，checking 不算——它只是还没体检出结果）。
        """
        account_filter = cfg.get("account_filter") or "unavailable"
        # 渠道无模型且未显式配置时，消费者也无从探测；生产时先挡一层，避免空任务入队。
        channel_model_ids = cls._scheduled_test_channel_model_ids(provider_name)
        if not channel_model_ids and not (cfg.get("test_models") or cfg.get("test_model")):
            logger.warning(
                f"[scheduled_test] {provider_name} 无任何模型且未配 test_models，跳过本轮"
            )
            return []
        clients_by_name: dict[str, "AccountClient"] = {}
        state_map: dict[str, AccountState] = {}
        for client in pool.clients:
            if client.disabled:
                continue
            clients_by_name[client.username] = client
            state_map[client.username] = client.state()
        configured = cfg.get("accounts")
        if isinstance(configured, list) and configured:
            pool_all = list(state_map.keys())
            candidate = [u for u in configured if u in pool_all]
        else:
            candidate = list(state_map.keys())
        if account_filter == "available":
            candidate = [u for u in candidate if state_map.get(u) is AccountState.AVAILABLE]
        elif account_filter == "unavailable":
            candidate = [u for u in candidate if cls._scheduled_test_is_unavailable(state_map.get(u))]
        if not candidate:
            logger.info(
                f"[scheduled_test] {provider_name} 到点但无候选账号 "
                f"(filter={account_filter}, enabled账号数={len(state_map)})"
            )
            return []
        tasks = [
            {"provider_name": provider_name, "pool": pool, "username": username, "cfg": cfg}
            for username in candidate if username in clients_by_name
        ]
        logger.info(
            f"[scheduled_test] {provider_name} 入队检测: 账号={candidate}, "
            f"任务数={len(tasks)}, filter={account_filter}"
        )
        return tasks

    @classmethod
    async def _scheduled_test_probe_one(cls, task: dict) -> None:
        """消费者：真正发探测那一刻重找账号、重判状态、重选模型，再逐个探测。

        队列只保存 provider + username，不保存 AccountClient/model 快照：
        - 账号排队期间被删除 → 当前 pool.clients 找不到，直接跳过，绝不会检测已删账号；
        - TTL 到期 / 状态变化 → 重新 state() 并重套 account_filter；
        - cooling 到期后变 model_frozen → 此刻重新 frozen_model_ids()，测被冻模型；
        - 模型表变化 → 此刻重读渠道模型表，已删模型不会被探测。
        """
        from admin import _run_provider_test
        provider_name = task["provider_name"]
        pool = task["pool"]
        username = task["username"]
        cfg = task["cfg"]
        # 渠道在排队期间被禁用/移除：不再发探测。
        current_pool = cls._provider_pools.get(provider_name)
        if current_pool is not pool or not getattr(pool, "enabled", True):
            logger.info(f"[scheduled_test] {provider_name} 排队期间已禁用或移除，跳过 {username}")
            return
        client = next((c for c in pool.clients if c.username == username), None)
        if client is None:
            logger.info(f"[scheduled_test] {provider_name} 账号 {username} 排队期间已删除，跳过本轮")
            return
        # AccountClient.state() 也会判 disabled；这里显式再挡一次，既强调调度门禁，
        # 也让替身/兼容 client 不会因 state 实现不完整而误发探测。
        if getattr(client, "disabled", False):
            return
        state = client.state()
        if state is AccountState.DISABLED:
            return
        account_filter = cfg.get("account_filter") or "unavailable"
        if account_filter == "available" and state is not AccountState.AVAILABLE:
            logger.info(
                f"[scheduled_test] {provider_name} 账号 {username} 状态已变为 {state.value}，"
                f"不再匹配 available，跳过"
            )
            return
        if account_filter == "unavailable" and not cls._scheduled_test_is_unavailable(state):
            logger.info(
                f"[scheduled_test] {provider_name} 账号 {username} 排队期间状态变为 "
                f"{state.value}，不再匹配 unavailable，跳过本轮"
            )
            return
        skip_within = config.Config.scheduled_test_skip_if_requested_within()
        if skip_within > 0:
            elapsed = time.time() - client.last_request_at()
            if elapsed < skip_within:
                logger.info(
                    f"[scheduled_test] {provider_name} 账号 {client.username} "
                    f"{int(elapsed)}s 前刚请求过，跳过本轮"
                )
                return
        channel_model_ids = cls._scheduled_test_channel_model_ids(provider_name)
        models = cls._scheduled_test_models_for_client(cfg, client, channel_model_ids)
        if not models:
            return
        retain_logs = cfg.get("retain_failed_logs") is not False
        logger.info(
            f"[scheduled_test] {provider_name} 开始检测: 账号={client.username}, "
            f"状态={state.value}, 模型={models}, retain_logs={retain_logs}"
        )
        for model in models:
            try:
                await _run_provider_test(provider_name, pool, {
                    "username": [client.username],
                    "model": model,
                    "test_type": cfg.get("test_type") or "chat",
                    "client_type": cfg.get("client_type") or "none",
                    "protocol": cfg.get("protocol") or "",
                    "retain_failed_logs": retain_logs,
                }, is_probe=True)
            except asyncio.CancelledError:
                # 单次探测内部的 timeout/stream cleanup 可能抛出 CancelledError；
                # 只有 worker 自身被 cancel（shutdown）才向外传播。
                current_task = asyncio.current_task()
                if current_task is not None and current_task.cancelling():
                    raise
                logger.warning(
                    f"[scheduled_test] {provider_name} 账号 {client.username} 模型 {model} "
                    f"探测被内部取消，保留冻结状态，等待下一周期重试"
                )
            except Exception as e:
                logger.error(
                    f"[scheduled_test] {provider_name} 账号 {client.username} 模型 {model} 测试失败: {e}"
                )

    @classmethod
    async def scheduled_test_loop(cls):
        """定时渠道检测：按各渠道 scheduled_test 配置到点跑一次 /accounts/test 同款流程。

        走 is_probe 模式：与手动测试一样绕过选路/预占（冻结键不拦探测），但失败时按正常
        请求语义记健康度 + 冻结。是否按新周期刷新已冻结对象的 TTL 由渠道级冻结策略
        freeze_policy.refresh_freeze_on_failure 在冻结写入点判定，与请求模式无关，本循环不再透传。
        成功会解除临时或永久冻结（clear_runtime_freeze_on_success），
        但绝不改账号 switch / 渠道 enabled；禁用对象不会进入定时检测。与 check_account_loop
        （认证探针）相互独立。

        架构：本 loop 是**生产者**——每 60s tick 扫所有到点渠道，调
        _scheduled_test_collect_tasks 算出「要检测哪些账号」塞进队列；一组 worker 是
        **消费者**，按全局 scheduled_test.concurrency 并发消费（_scheduled_test_probe_one），
        在发探测那一刻重找账号、重判 state()、重选模型，避免用陈旧快照白测。
        并发度支持热更新（管理端保存后下一个 tick 生效，不必重启）。
        pending 去重保证「上一轮还在途的账号」不会被下一轮重复入队，这才是真正会堆积的形态。
        """
        # 不阻塞服务启动；只阻塞本 loop 的首轮，确保账号列表/冻结态/认证态已经收敛。
        await cls._account_init_done.wait()
        last_run: dict[str, float] = {}
        # 队列与在途集合提升为类属性：trigger_scheduled_test_now 手动入队复用同一消费者池，
        # 避免手动触发另起一套 worker。未就绪（loop 尚未启动）时手动触发直接返回 -1。
        cls._scheduled_test_queue = asyncio.Queue()
        cls._scheduled_test_pending = set()
        queue = cls._scheduled_test_queue
        # 在途/排队中的账号键，防止上一轮还没消费完、下一轮又把同一个账号塞一遍。
        pending = cls._scheduled_test_pending
        workers: list[asyncio.Task] = []

        async def _worker():
            while True:
                task = await queue.get()
                try:
                    if task is None:
                        return
                    await cls._scheduled_test_probe_one(task)
                finally:
                    pending.discard(task.get("key") if isinstance(task, dict) else None)
                    queue.task_done()

        def _start_workers(count: int) -> None:
            workers.extend(asyncio.create_task(_worker()) for _ in range(count))

        try:
            concurrency = config.Config.scheduled_test_concurrency()
            _start_workers(concurrency)
            while True:
                await asyncio.sleep(60)
                try:
                    # 全局配置热更新：Config.reload_async() 在管理端保存后已刷新缓存。
                    # 增加并发立即补 worker；降低并发在已有排队任务消费完后投递退出哨兵，
                    # 不中断正在跑的探测，也不需要重启服务。
                    workers[:] = [worker for worker in workers if not worker.done()]
                    wanted_concurrency = config.Config.scheduled_test_concurrency()
                    if wanted_concurrency > len(workers):
                        _start_workers(wanted_concurrency - len(workers))
                    elif wanted_concurrency < len(workers):
                        for _ in range(len(workers) - wanted_concurrency):
                            queue.put_nowait(None)
                    concurrency = wanted_concurrency
                    now = time.time()
                    for provider_name in list(cls._provider_pools.keys()):
                        try:
                            cfg = await config.Config.get_provider_scheduled_test(provider_name)
                        except Exception:
                            cfg = {}
                        if not cfg.get("enabled"):
                            continue
                        interval = _scheduled_test_interval_seconds(cfg)
                        if interval <= 0:
                            continue
                        if now - last_run.get(provider_name, 0) < interval:
                            continue
                        last_run[provider_name] = now
                        cls._last_scheduled_test_triggered_at[provider_name] = now
                        pool = cls._provider_pools.get(provider_name)
                        if not pool:
                            continue
                        # 渠道禁用时不运行定时测试；手动测试仍可 bypass 渠道门禁。
                        if not getattr(pool, "enabled", True):
                            continue
                        try:
                            for task in cls._scheduled_test_collect_tasks(provider_name, pool, cfg):
                                key = (provider_name, task["username"])
                                if key in pending:
                                    logger.info(
                                        f"[scheduled_test] {provider_name} {key[1]} 上一轮仍在途，跳过入队"
                                    )
                                    continue
                                task["key"] = key
                                pending.add(key)
                                queue.put_nowait(task)
                        except Exception as e:
                            logger.error(f"[scheduled_test] {provider_name} 入队失败: {e}")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"[scheduled_test] 循环异常: {e}")
        finally:
            for w in workers:
                w.cancel()

    @classmethod
    async def refresh_models_loop(cls):
        interval = config.Config.model_refresh_interval()
        try:
            while True:
                try:
                    await cls.refresh_models()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"模型列表刷新失败: {e}")
                await asyncio.sleep(interval * 60)
        except asyncio.CancelledError:
            return

    @classmethod
    async def delete_message_loop(cls):
        interval = config.Config.message_delete_interval()
        while True:
            await asyncio.sleep(interval * 60)
            try:
                logger.info("删除历史消息信息...")
                await cls.clean_message()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"刷新失败: {e}")

    @classmethod
    async def _cleanup_payload_store(cls, keep_days: int, max_entries: int = 0) -> str:
        """按同一保留口径（天数 + 条数）删 ClickHouse 里的请求/响应体。

        双写没开就直接跳过。这里用独立的短生命周期客户端而不是复用 payload_writer
        的那个：写入侧客户端带 session 且不允许并发查询，清理是分钟级的 mutation，
        借用会把写入阻塞住。

        与 Postgres 主表清理同口径：超过保留天数或超出条数上限（保留最新）任一命中
        即删，两个条件取更严格的边界。删除走分区删除 + 边界月 mutation：整月早于边界
        的分区 DROP 立即回收磁盘，只有跨边界的当月走 ALTER DELETE mutation。所以磁盘
        基本是立刻降的，不像全量 mutation 那样要等重写完。

        条数口径以 Postgres 主表为锚（request_log_entry_boundary = 第 max_entries 条
        现存日志的时间）：主删从随，主表删掉的行对应的 payload 必被带走，不会留下
        日志查不到、请求体却继续占盘的孤儿。读边界失败不阻断清理——降级传 None，
        由 ClickHouse 侧按本表事件数兜底（见 delete_payloads_older_than）。
        """
        try:
            from clickhouse_config import get_settings

            ch = get_settings()
            if not ch.enabled:
                return ""
        except Exception as e:
            logger.warning(f"读取 ClickHouse 配置失败，跳过 payload 清理: {e}")
            return ""
        entries_boundary = None
        if max_entries and max_entries > 0:
            try:
                from db import PostgresClient

                entries_boundary = await PostgresClient.request_log_entry_boundary(max_entries)
            except Exception as e:
                logger.warning(f"读取请求日志条数边界失败，payload 清理退回事件数兜底: {e}")
        client = None
        try:
            from integrations.clickhouse import ClickHousePayloadClient

            client = ClickHousePayloadClient(
                addr=ch.addr,
                database=ch.database,
                username=ch.username,
                password=ch.password,
            )
            await client.connect()
            result = await client.delete_payloads_older_than(
                keep_days, max_entries=max_entries, entries_boundary=entries_boundary
            )
            dropped = result.get("dropped_partitions") or []
            mut = bool(result.get("mutation_submitted"))
            inactive_n = int(result.get("inactive_parts") or 0)
            inactive_bytes = int(result.get("inactive_bytes") or 0)
            logger.info(
                f"ClickHouse payload 清理: keep_days={keep_days}, max_entries={max_entries}, "
                f"dropped={len(dropped)} {dropped}, mutation_submitted={mut}, "
                f"inactive_parts={inactive_n} ({inactive_bytes} bytes)"
            )
            if not (dropped or mut or inactive_n):
                return ""
            parts = f"删 {len(dropped)} 个过期分区" if dropped else "无整月过期分区"
            mut_note = "，边界月 mutation 已提交" if mut else ""
            if inactive_n:
                # 旧 part 的物理删除靠 ClickHouse 后台（cleanup_parts_interval 默认 10 分钟），
                # 客户端没法强制删。detached 目录的要手动 rm。这里只如实报告积压量。
                gb = inactive_bytes / 1024 / 1024 / 1024
                size_note = f"{gb:.1f}GB" if gb >= 1 else f"{inactive_bytes / 1024 / 1024:.0f}MB"
                gc_note = f"，另有 {inactive_n} 个旧 part（~{size_note}）等后台自动回收"
            else:
                gc_note = ""
            return f"；请求体已清理（{parts}{mut_note}{gc_note}，分区删除立即回收磁盘）"
        except Exception as e:
            # payload 清理失败不能拖垮 Postgres 侧的清理结果，只降级记录。
            logger.warning(f"ClickHouse payload 清理失败: {e}")
            return f"；请求体清理失败：{e}"
        finally:
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass

    @classmethod
    async def clean_response_data(cls, trigger: str = "daily"):
        """执行日志清理：按「日志保留天数」直接删除主表 request_logs 旧行。

        归档表与归档逻辑已下线，日志统一按天数清理主表（可叠加条数上限）。
        删除行数写 loguru，并产生 ``log.cleanup_done`` / ``log.cleanup_failed``
        事件；是否落通知中心或推 webhook 由订阅配置决定，未配置则只留日志。

        trigger 区分触发来源（"daily" 定时 / "manual" 管理端手动）。手动触发用独立
        dedupe_key，否则连点两次会被定时任务的 1 小时去重窗口吞掉，管理员看不到第二次
        的结果；且手动清理失败必须产生事件（点了按钮的人在等回执），定时失败只写日志。
        """
        from db import PostgresClient
        from monkeycode_compat.notify_core import emit_notification
        manual = trigger == "manual"
        keep_hours = config.Config.keep_response_hours()
        keep_days = config.Config.get_log_retention_days()
        max_entries = config.Config.get_log_retention_max_entries()
        logger.info(f"执行日志清理({trigger}): log_retention_days={keep_days}, keep_hours={keep_hours}, max_entries={max_entries}")
        try:
            # 先聚合（aggregate-then-delete）：尽量让聚合水位追平当前时刻，减少
            # 清理被水位护栏扣留的行。聚合失败不阻塞清理——cleanup_request_logs
            # 的水位护栏是硬保证，扣留的行留到下一轮清理再删。
            try:
                await cls._aggregate_hourly_stats()
            except Exception as agg_exc:
                logger.error(f"清理前小时聚合失败（继续清理，水位护栏兜底）: {agg_exc}")
            archived = await PostgresClient.cleanup_request_logs(keep_hours, keep_days)
            # 请求体存在 ClickHouse，Postgres 删行不会带走它们。同口径（天数 + 条数）
            # 一起删，否则日志行没了但 payload 继续占盘，成为查不到的孤儿 payload。
            payload_note = await cls._cleanup_payload_store(keep_days, max_entries)
            limit_note = f" / 最多 {max_entries} 条" if max_entries and max_entries > 0 else ""
            await emit_notification(
                "log.cleanup_done",
                params={"keep_days": keep_days, "max_entries": max_entries, "deleted": archived or 0, "trigger": trigger},
                owner_type="platform",
                severity="info",
                kind="system",
                source="log_cleanup",
                title="手动日志清理完成" if manual else "定时日志清理完成",
                message=f"按 {keep_days} 天保留{limit_note}清理请求日志，本次删除 {archived or 0} 行{payload_note}",
                dedupe_key=f"log_cleanup:manual:{int(time.time())}" if manual else "log_cleanup:daily",
                dedupe_window_seconds=0 if manual else 3600,
            )
            return archived or 0
        except Exception as e:
            logger.error(f"日志清理失败: {e}")
            if manual:
                await emit_notification(
                    "log.cleanup_failed",
                    params={"keep_days": keep_days, "trigger": trigger, "error": str(e)},
                    owner_type="platform",
                    severity="error",
                    kind="system",
                    source="log_cleanup",
                    title="手动日志清理失败",
                    message=f"清理请求日志失败：{e}",
                    dedupe_key=f"log_cleanup:manual:{int(time.time())}",
                    dedupe_window_seconds=0,
                )
            return 0

    @classmethod
    async def clean_response_loop(cls):
        """定时清理过期的 response_body 数据（每日在 data_retention.cleanup_hour 整点执行，默认 2:00 AM）"""
        logger.info("响应数据定时清理启动，每日按配置整点执行（默认 2:00）")
        while True:
            await asyncio.sleep(3600)
            try:
                from datetime import datetime
                today = datetime.now().strftime("%Y-%m-%d")
                cleanup_hour = config.Config.get_cleanup_hour()
                if datetime.now().hour == cleanup_hour and cls._last_response_clean_date != today:
                    cls._last_response_clean_date = today
                    logger.info(f"执行定时数据清理（{cleanup_hour}:00）...")
                    await cls.clean_response_data()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"定时清理失败: {e}")

    # 单轮聚合分批的小时数：每批一个区间（先删后插），批间失败即停——
    # 聚合表 max(hour) 不越过未聚合数据，清理的聚合水位才始终可信。
    HOURLY_AGGREGATE_BATCH_HOURS = 24

    @classmethod
    async def _aggregate_hourly_stats(cls):
        """把所有已完整结束、但还没进预聚合表的小时补齐（纯增量，绝不重建）。

        仪表盘以 current_hour 为界：current_hour 之后实时查 request_logs，之前只读
        hourly_dashboard_stats。所以凡是 < current_hour 又没聚合的小时，在仪表盘上
        就是缺数的，必须全部补上——只算“上一小时”会漏两种情况：

        1. 半截小时：聚合落笔时 status 尚为 requesting 的行，收尾后要重算该桶；
        2. 定时唤醒相对整点漂移时，某个小时会被整个跳过且永不回看。

        aggregate_hourly_logs 对区间先 DELETE 再 INSERT，重算任意小时都是幂等的，
        因此这里可以放心重算边界小时。区间按 HOURLY_AGGREGATE_BATCH_HOURS 分批
        推进、失败即停：某批异常时不再继续后续批次，聚合表 max(hour) 停在失败批
        之前——它同时是清理的聚合水位，必须永不越过未聚合数据；下一整点自动
        从失败批重试。启动不做全量重建（会连带抹掉超出 request_logs 保留期的
        历史），口径变更用 script/rebuild_hourly_stats.py 显式执行。

        聚合表为空时从 request_logs 最早小时起补，受日志保留期约束量级有限。
        """
        from datetime import datetime, timezone, timedelta
        try:
            now_utc = datetime.now(timezone.utc)
            current_hour = now_utc.replace(minute=0, second=0, microsecond=0)

            last_hour = await PostgresClient.last_aggregated_dashboard_hour()
            if last_hour is None:
                # 空表（新装/聚合表被清）：从原始日志最早小时起补齐。不设总量
                # 上限——超长停机后一次性补完，否则落下的空洞在聚合表里永久缺数。
                earliest = await PostgresClient.earliest_request_log_hour()
                if earliest is None:
                    return
                if earliest.tzinfo is None:
                    earliest = earliest.replace(tzinfo=timezone.utc)
                start_hour = earliest.replace(minute=0, second=0, microsecond=0)
            else:
                if last_hour.tzinfo is None:
                    last_hour = last_hour.replace(tzinfo=timezone.utc)
                # 从最后一个已聚合小时本身重算：它可能是被写成半截数据的桶。
                start_hour = last_hour.replace(minute=0, second=0, microsecond=0)

            if start_hour >= current_hour:
                return

            batch = timedelta(hours=max(1, cls.HOURLY_AGGREGATE_BATCH_HOURS))
            logger.info(f"聚合小时日志: {start_hour.isoformat()} -> {current_hour.isoformat()}")
            batch_start = start_hour
            while batch_start < current_hour:
                # 不吞批内异常：失败即停，水位不推进，下一整点从失败批重试。
                batch_end = min(batch_start + batch, current_hour)
                await PostgresClient.aggregate_hourly_logs(batch_start, batch_end)
                batch_start = batch_end
            logger.info("小时日志聚合完成")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"小时日志聚合失败: {e}")

    @classmethod
    async def _hourly_stats_loop(cls):
        """后台循环：整点后立刻把已结束小时聚合进 hourly_dashboard_stats。

        必须对齐整点唤醒，不能裸 sleep(3600)：后者把触发时刻锁在进程启动的分钟偏移
        上，于是每小时开头那段时间里，刚结束的小时已被仪表盘划进预聚合段却还没聚合，
        表现为“最近一个小时数据不及时”。裸 sleep 还会因聚合耗时累积右移而跳过整个小时。
        """
        from datetime import datetime, timezone, timedelta

        # 启动即跑一次：把停机/半截小时的空档补齐（分批推进，见方法 docstring）。
        try:
            await cls._aggregate_hourly_stats()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"_hourly_stats_loop 首轮异常: {e}")

        logger.info("小时统计聚合定时任务启动，每整点后执行一次")
        while True:
            try:
                now = datetime.now(timezone.utc)
                next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                # 缓冲：等末尾请求落库（status 从 requesting 收尾）后再聚合。
                delay = (next_hour - now).total_seconds() + 30
                await asyncio.sleep(delay)
                await cls._aggregate_hourly_stats()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"_hourly_stats_loop 异常: {e}")
                await asyncio.sleep(60)

    @classmethod
    async def stop(cls):
        """停止定时刷新，并关闭各 provider 复用的长连接 aiohttp session。

        每个 BaseProvider 在流式转发时复用 self._session（仅当 closed 才重建，
        见 providers/base.py），进程退出若不主动关闭，解释器回收时会由 aiohttp
        打印 "Unclosed client session" 警告。这里在 shutdown 阶段统一收尾。
        """
        tasks = []
        for task_attr in (
            "_account_init_task", "_refresh_task", "_delete_task",
            "_clean_response_task", "_check_task", "_scheduled_test_task", "_hourly_stats_task",
        ):
            task = getattr(cls, task_attr, None)
            setattr(cls, task_attr, None)
            if task:
                task.cancel()
                tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        flush_task = cls._reliability_flush_task
        cls._reliability_flush_task = None
        if flush_task is not None:
            flush_task.cancel()
            await asyncio.gather(flush_task, return_exceptions=True)
        try:
            await asyncio.wait_for(cls._flush_reliability_counts(), timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning("[reliability] shutdown flush 超时，剩余计数丢弃")

        await LimitManager.shutdown()

        # 关闭各 provider 实例复用的长连接 session
        for pool in cls._provider_pools.values():
            for account_client in pool.clients:
                provider = getattr(account_client, "provider", None)
                if provider is None:
                    continue
                try:
                    close = getattr(provider, "close", None)
                    if close is not None:
                        result = close()
                        if asyncio.iscoroutine(result):
                            await result
                except Exception as e:
                    logger.warning(f"关闭 provider session 失败 provider={getattr(provider, 'PROVIDER_NAME', '?')}: {e}")

    @classmethod
    def _metadata_for_model(cls, model_id: str, by_id: dict, groups_by_id: dict, default: dict) -> tuple[dict, bool]:
        metadata = {**FALLBACK_DEFAULT_MODEL_METADATA, **(default or {})}
        record = by_id.get(model_id) or groups_by_id.get(model_id)
        if not record:
            return metadata, True
        metadata.update({k: v for k, v in record.items() if k != "model_id"})
        return metadata, False

    @classmethod
    def _apply_metadata_from_indices(cls, item: dict, by_id: dict, groups_by_id: dict, default: dict) -> bool:
        metadata, is_default_only = cls._metadata_for_model(item.get("id"), by_id, groups_by_id, default)
        for key, value in metadata.items():
            if item.get(key) is None:
                item[key] = value
        if "multimodal" not in item and "input_modalities" in item:
            item["multimodal"] = item["input_modalities"]
        return not is_default_only

    @classmethod
    def _groups_index_from_loaded_data(cls, by_id: dict, default: dict, enabled_groups: dict) -> dict:
        try:
            default_max = int(default.get("max_tokens") or 0)
        except (TypeError, ValueError):
            default_max = 0
        groups_by_id: dict = {}
        for name, group in (enabled_groups or {}).items():
            metadata_model = str(group.get("metadata_model") or "").strip()
            best_record = by_id.get(metadata_model) if metadata_model else None
            best_max = None
            members = [m for m in group.get("models", []) if isinstance(m, str) and m]
            if best_record is None:
                for member in members:
                    record = by_id.get(member)
                    if not record:
                        continue
                    try:
                        eff = int(record.get("max_tokens") or default_max or 0)
                    except (TypeError, ValueError):
                        eff = default_max
                    if best_record is None or eff < best_max:
                        best_record = record
                        best_max = eff
            if best_record is not None:
                groups_by_id[name] = {k: v for k, v in best_record.items() if k != "model_id"}
            # 别名继承同组 same best_record
            for alias in group.get("aliases", []) or []:
                if isinstance(alias, str) and alias and alias != name and alias not in groups_by_id:
                    if best_record is not None:
                        groups_by_id[alias] = {k: v for k, v in best_record.items() if k != "model_id"}
        return groups_by_id

    @classmethod
    def _apply_group_member_min_tokens_from_indices(cls, item: dict, members: list[str], by_id: dict, groups_by_id: dict, default: dict) -> None:
        for field in ("max_tokens", "max_context_tokens"):
            values = []
            for member in members:
                metadata, _ = cls._metadata_for_model(member, by_id, groups_by_id, default)
                try:
                    value = int(metadata.get(field) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    values.append(value)
            if values:
                item[field] = min(values)

    @classmethod
    async def _apply_group_member_min_tokens(cls, item: dict, members: list[str]) -> None:
        for field in ("max_tokens", "max_context_tokens"):
            values = []
            for member in members:
                metadata, _ = await get_model_metadata(member)
                try:
                    value = int(metadata.get(field) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    values.append(value)
            if values:
                item[field] = min(values)

    @classmethod
    async def _build_models_from_routes(cls, snapshot=None) -> list[dict]:
        snapshot = snapshot or model_catalog.current_snapshot()
        model_list: list[dict] = []
        # snapshot 的嵌套结构被 model_catalog._freeze 冻成 MappingProxyType/tuple。
        # dict() 只做浅拷贝，嵌套字段（如 capabilities）仍是 mappingproxy，
        # 会随 item 一路带进 _models，最终 pydantic 序列化时抛
        # "Unable to serialize unknown type: <class 'mappingproxy'>"。
        # 用 _mutable 递归还原为可变 dict/list，从源头杜绝。
        default = _mutable(snapshot.default)
        by_id = {model_id: _mutable(item) for model_id, item in snapshot.metadata.items()}
        groups_by_id = {model_id: _mutable(item) for model_id, item in snapshot.group_metadata.items()}

        emitted_ids = set()
        for model_id in cls.all_model_ids():
            if model_id not in by_id:
                continue
            item = {
                "id": model_id,
                "object": "model",
                "created": int(time.time()),
                "owned_by": "ai-lubricant",
            }
            cls._apply_metadata_from_indices(item, by_id, groups_by_id, default)
            model_list.append(item)
            emitted_ids.add(model_id)

        # 自定义模型（组主名 + 别名）条目：解析索引按创建时间建，重名时先创建者胜。
        group_index = snapshot.group_index
        custom_emitted = set()
        for display_id, group in group_index.items():
            if display_id in custom_emitted:
                continue
            members = [m for m in group.get("models", []) if isinstance(m, str) and m and m in by_id and cls.has_model_route(m)]
            if not members:
                continue
            custom_emitted.add(display_id)
            # 如有同名真实模型条目，用自定义条目替换（组优先）。
            if display_id in emitted_ids:
                model_list[:] = [m for m in model_list if m.get("id") != display_id]
                emitted_ids.discard(display_id)
            item = {
                "id": display_id,
                "object": "model",
                "created": int(time.time()),
                "owned_by": "model-group",
                "type": "model_group",
                "models": members,
            }
            remark = str(group.get("remark") or group.get("display_name") or "").strip()
            if remark:
                item["remark"] = remark
                item["name"] = remark
            cls._apply_metadata_from_indices(item, by_id, groups_by_id, default)
            # 仅当未显式指定「模型元数据」来源时，才按成员逐字段取最小值；
            # 显式指定时直接采用该模型的原值，不做最小值折算。
            if not str(group.get("metadata_model") or "").strip():
                cls._apply_group_member_min_tokens_from_indices(item, members, by_id, groups_by_id, default)
            model_list.append(item)
        return model_list

    @classmethod
    async def rebuild_model_response_cache(cls, snapshot=None) -> None:
        snapshot = snapshot or model_catalog.current_snapshot()
        # Explicit rebuild callers (startup/route refresh) request a new build too.
        if not cls._models_dirty and cls._models_built_generation >= cls._models_requested_generation:
            cls._models_requested_generation += 1
        next_snapshot = snapshot
        async with cls._models_rebuild_lock:
            while cls._models_built_generation < cls._models_requested_generation:
                target_generation = cls._models_requested_generation
                build_snapshot = next_snapshot or model_catalog.current_snapshot()
                next_snapshot = None
                models = await cls._build_models_from_routes(snapshot=build_snapshot)
                cls._models = models
                cls._models_by_id = {item.get("id"): item for item in models if item.get("id")}
                cls._models_built_generation = target_generation
            cls._models_dirty = cls._models_built_generation < cls._models_requested_generation

    @classmethod
    def _mark_models_dirty(cls) -> None:
        """Advance the requested build generation so concurrent dirties are not lost."""
        cls._models_requested_generation += 1
        cls._models_dirty = True

    @classmethod
    async def _ensure_models_fresh(cls) -> None:
        if cls._models_dirty or cls._models_built_generation < cls._models_requested_generation:
            await cls.rebuild_model_response_cache(snapshot=model_catalog.current_snapshot())

    @classmethod
    async def _ensure_header_templates_loaded(cls) -> None:
        if cls._header_templates_loaded:
            return
        cls._header_templates_loaded = True
        cls._header_templates = {}
        try:
            templates = await PostgresClient.get_header_templates()
        except Exception:
            return
        for t in templates:
            tid = str(t.get("id") or "")
            headers = t.get("headers")
            if tid and isinstance(headers, dict):
                cls._header_templates[tid] = headers

    @classmethod
    async def get_header_template(cls, template_id: str | None) -> dict:
        """按模板 id 取一组 header 覆盖。不存在或空 id 返回 {}。"""
        if not template_id:
            return {}
        await cls._ensure_header_templates_loaded()
        return dict(cls._header_templates.get(template_id) or {})

    @classmethod
    def set_header_template_cache(cls, templates: list[dict]) -> None:
        """用已规范化的 header 模板列表替换运行态同步视图。"""
        loaded: dict[str, dict] = {}
        for template in templates:
            if not isinstance(template, dict):
                continue
            template_id = str(template.get("id") or "")
            headers = template.get("headers")
            if template_id and isinstance(headers, dict):
                loaded[template_id] = dict(headers)
        cls._header_templates = loaded
        cls._header_templates_loaded = True

    @classmethod
    async def invalidate_header_template_cache(cls) -> None:
        """admin 写入 header 模板后调用，下次读取时重新加载。"""
        cls._header_templates_loaded = False
        cls._header_templates = {}

    @classmethod
    async def get_models_response(cls, api_key: str | None = None) -> dict:
        """获取 OpenAI 格式公开模型列表；运行时方案降级节点不对客户端暴露。

        禁用过滤在此处按读取时状态现算，不烘进 _models —— 该缓存与管理端
        get_admin_models_response 共用，管理端要看全量（含禁用）。

        传 api_key 时额外套该 Key 的模型白/黑名单。名单过滤在此收口而非各调用点
        自行实现：自定义模型的 models 成员数组要按同样口径裁剪，散在调用点写只会
        漏成员——组过了名单、成员没过，被拉黑或落在已禁用渠道上的模型仍从成员
        数组漏给客户端。api_key=None 表示不做 Key 级过滤（内部/管理端调用）。
        """
        await cls._ensure_models_fresh()
        available = cls.available_model_ids()

        async def _visible(model_id) -> bool:
            """单个真实模型名是否对外可见：非内部 id + 可供给 + 过 Key 名单。"""
            if model_catalog.is_internal_model_id(model_id):
                return False
            if model_id not in available:
                return False
            if api_key is None:
                return True
            return await config.Config.api_key_allows_model(api_key, model_id)

        data = []
        for m in cls._models:
            model_id = m.get("id")
            if model_catalog.is_internal_model_id(model_id):
                continue
            if api_key is not None and not await config.Config.api_key_allows_model(api_key, model_id):
                continue
            if m.get("type") == "model_group":
                # 成员逐个过同一道门并重写 models[]，不能原样透出：组自身过了名单
                # 不代表成员都过，否则被拉黑/落在已禁用渠道上的成员会从数组漏出。
                members = [member for member in (m.get("models") or []) if await _visible(member)]
                if not members:
                    continue
                item = dict(m)
                item["models"] = members
                data.append(item)
                continue
            if model_id not in available:
                continue
            data.append(dict(m))
        return {"object": "list", "data": data}

    @classmethod
    async def get_admin_models_response(cls) -> dict:
        # 管理端模型广场只展示真实模型；自定义模型（组主名/别名）不进列表。
        await cls._ensure_models_fresh()
        return {
            "object": "list",
            "data": [m for m in cls._models if m.get("type") != "model_group"],
        }

    @classmethod
    async def get_admin_model_route_options(cls, enabled_groups: dict | None = None) -> list[dict]:
        """轻量管理端模型选项：只用于路由/分组配置，不重建 /v1/models 元数据缓存。"""
        options: list[dict] = []
        route_model_ids = cls.all_model_ids()
        for model_id in sorted(route_model_ids):
            if not model_id:
                continue
            options.append({
                "id": model_id,
                "object": "model",
                "owned_by": "ai-lubricant",
            })

        groups = enabled_groups if enabled_groups is not None else await config.Config.get_enabled_model_groups()
        for group_name, group in sorted((groups or {}).items()):
            if not group_name or group_name in route_model_ids:
                continue
            if not isinstance(group, dict):
                continue
            members = [m for m in group.get("models", []) if isinstance(m, str) and m in route_model_ids]
            if not members:
                continue
            item = {
                "id": group_name,
                "object": "model",
                "owned_by": "model-group",
                "type": "model_group",
                "models": members,
            }
            remark = str(group.get("remark") or group.get("display_name") or "").strip()
            if remark:
                item["remark"] = remark
                item["name"] = remark
            options.append(item)
        return options

    @classmethod
    def get_model_routes(cls, model_id: str) -> list[dict]:
        """扫各渠道 Channel.models，聚合出该模型的路由列表（等价旧 _model_routes[model_id]）。"""
        return [dict(row) for _prov, _pool, row in cls.iter_model_candidates(model_id)]

    @classmethod
    def get_runtime_routes_snapshot(cls) -> dict[str, list[dict]]:
        snapshot: dict[str, list[dict]] = defaultdict(list)
        for provider_name, pool in cls._provider_pools.items():
            channel = getattr(pool, "channel", None)
            if channel is None:
                continue
            for row in channel.models:
                model_id = row.get("model_id")
                if model_id:
                    snapshot[model_id].append(dict(row))
        return dict(snapshot)

    @classmethod
    def get_model_route_providers(cls, model_id: str) -> list[str]:
        providers = []
        seen = set()
        for provider_name, _pool, _row in cls.iter_model_candidates(model_id):
            if provider_name and provider_name not in seen:
                seen.add(provider_name)
                providers.append(provider_name)
        return providers

    @classmethod
    def get_provider_routes(cls, provider_name: str) -> list[dict]:
        pool = cls._provider_pools.get(provider_name)
        channel = getattr(pool, "channel", None) if pool else None
        if channel is None:
            return []
        return [dict(row) for row in channel.models]

    @classmethod
    def all_model_ids(cls) -> set[str]:
        """所有渠道当前具备的公开模型名并集（替代旧 set(_model_routes)）。"""
        ids: set[str] = set()
        for pool in cls._provider_pools.values():
            channel = getattr(pool, "channel", None)
            if channel is not None:
                ids.update(row.get("model_id") for row in channel.models if row.get("model_id"))
        return ids

    @classmethod
    def _pool_can_serve(cls, pool) -> bool:
        """渠道是否有资格对外供给：渠道 enabled 且至少一个账号未禁用。

        与 _select_and_reserve_candidates 的 provider_disabled / account_disabled
        两道门同源（见该函数内注释），只是那里逐候选判定、这里整池判定。
        冻结/冷却不看：那是短时状态，模型本身仍然可用。
        """
        if pool is None:
            return False
        channel = getattr(pool, "channel", None)
        if channel is not None and not channel.enabled:
            return False
        clients = getattr(pool, "clients", None)
        if clients is None:
            return True
        return any(not client.disabled for client in clients)

    @classmethod
    def available_model_ids(cls) -> set[str]:
        """可对外供给的模型名并集：只取未禁用渠道 + 未禁用账号能跑的行。

        不进构建缓存、每次读取现算——渠道 enabled 开关与账号 switch off 都不会
        触发 _mark_models_dirty，烘进 _models 会一直返回旧结果。纯内存扫描。
        """
        ids: set[str] = set()
        for pool in cls._provider_pools.values():
            channel = getattr(pool, "channel", None)
            if channel is None or not cls._pool_can_serve(pool):
                continue
            ids.update(row.get("model_id") for row in channel.models if row.get("model_id"))
        return ids

    @classmethod
    def has_model_route(cls, model_id: str, available_only: bool = False) -> bool:
        """任一渠道是否具备该模型（替代旧 _model_routes.get(m) 真值门）。

        available_only=True 时只认未禁用渠道的未禁用账号，供对外模型列表使用。
        """
        for pool in cls._provider_pools.values():
            channel = getattr(pool, "channel", None)
            if channel is None:
                continue
            if available_only and not cls._pool_can_serve(pool):
                continue
            if any(row.get("model_id") == model_id for row in channel.models):
                return True
        return False

    @classmethod
    async def fetch_provider_upstream_models(cls, provider_name: str) -> dict:
        """打上游拉全量模型列表，与 provider_models 表中该渠道已有行做 join，供管理端选择导入。

        每行同时返回：
          model_id = 套用改写规则后的短名（去 owner/ 前缀 + 规则替换）
          raw_model_id = 原始上游模型名（未经任何处理）
        前端可通过开关在两者间切换显示，无需重新拉取上游。
        """
        pool = cls.get_provider_pool(provider_name)
        if not pool:
            raise HTTPException(status_code=404, detail=f"渠道 '{provider_name}' 不存在")
        if not pool.clients:
            raise HTTPException(status_code=400, detail=f"渠道 '{provider_name}' 未配置任何账号")

        client = await pool.get_initialized_provider()
        if client is None:
            # 定时 init 失败/被跳过（渠道禁用、账号 switch off、登录一次性失败）后池里可能没有
            # 已初始化账号；拉模型列表属 Path A（bypass 渠道/账号开关），先按需兜底初始化，
            # 避免"init 失败过一次就永远拉不到模型列表"的死锁。
            last_init_error = ""
            for c in pool.clients:
                if c.disabled:
                    continue
                try:
                    await asyncio.wait_for(c.provider.init_auth(False), timeout=30)
                    if c.provider.is_init():
                        c.auth_ok = True
                        c.auth_checked_at = time.time()
                        c.auth_error = ""
                        client = c.provider
                        break
                except asyncio.TimeoutError:
                    last_init_error = "初始化超时"
                    logger.warning(f"[{provider_name}] 兜底初始化账号 {c.username} 超时")
                except Exception as e:
                    last_init_error = str(e)
                    logger.warning(f"[{provider_name}] 兜底初始化账号 {c.username} 失败: {e}")
        logger.debug(f"获取 {provider_name} 上游模型列表, {client}...")
        if client is None:
            # 全部账号被禁用时不能报"均未完成初始化"——真实原因是手动关了开关。
            if all(c.disabled for c in pool.clients):
                raise HTTPException(
                    status_code=400,
                    detail=f"渠道 '{provider_name}' 的账号已全部禁用，请先启用任一账号",
                )
            detail = f"渠道 '{provider_name}' 没有可用账号（均未完成初始化）"
            if last_init_error:
                detail += f"，最后错误: {last_init_error}"
            raise HTTPException(status_code=503, detail=detail)

        models = await client.fetch_upstream_model_list()
        if not models:
            fetch_err = getattr(client, "_last_fetch_models_error", "") or ""
            if fetch_err:
                # 上游真的请求失败（网络/HTTP 错误）：502
                logger.warning(f"[{provider_name}] 上游模型列表获取失败: {fetch_err}")
                raise HTTPException(status_code=502, detail=f"上游模型列表获取失败: {fetch_err}")
            # 上游返回 200 但无可导入模型（例如全部被策略过滤、或上游列表为空）：
            # 返回空列表给前端，不报错，由前端提示“无可用模型”。
            logger.info(f"[{provider_name}] 上游返回 200 但模型列表为空，返回空列表给前端")
            return {"provider": provider_name, "upstream_models": []}

        existing_rows = await PostgresClient.list_provider_models(provider_name)
        # provider_models 行以 upstream_model_id 为键
        tracked_map = {r["upstream_model_id"]: r["model_id"] for r in existing_rows}

        return {
            "provider": provider_name,
            "upstream_models": cls._build_upstream_model_rows(provider_name, models, tracked_map),
        }

    @classmethod
    async def check_account(cls):
        """刷新所有模型列表（全量）"""
        for provider_name in ModelClientPool.get_provider_names():
            pool = ModelClientPool.get_provider_pool(provider_name)
            if not pool:
                continue
            try:
                await pool.check_account()
            except:
                logger.error(f"获取 {provider_name} 模型列表失败: {traceback.format_exc()}")

    @classmethod
    async def refresh_models(cls, sync_upstream: bool = True):
        """刷新所有模型列表。

        语义：
        1. sync_upstream=True 时，每个 auto_update_models=True 的渠道：拉上游，信任渠道模型列表，将新 upstream 自动加入 provider_models。
        2. 从 provider_models 全表重建运行时 _model_routes。
        3. /v1/models 响应只从 routes 中返回有显式 model_metadata 的模型。
        """
        if sync_upstream:
            for provider_name in cls.get_provider_names():
                pool = cls.get_provider_pool(provider_name)
                if not pool or not pool.clients:
                    continue

                any_provider = pool.clients[0].provider
                auto_update_models = getattr(any_provider, "auto_update_models", True)
                if not auto_update_models:
                    # logger.debug(f"{provider_name} auto_update_models=False，跳过上游拉取")
                    continue

                try:
                    client = await pool.get_initialized_provider()
                    if client is None:
                        continue
                    upstream = await client.fetch_upstream_model_list()
                except Exception:
                    logger.error(f"获取 {provider_name} 上游模型列表失败: {traceback.format_exc()}")
                    continue

                existing = await PostgresClient.list_provider_models(provider_name)
                existing_map = {r["upstream_model_id"]: r["model_id"] for r in existing}
                existing_ids = set(existing_map)
                # 与管理端「获取上游模型」同一份整形结果（_build_upstream_model_rows）：
                # is_regex 是规则命中标记，regex_model_id 是替换后的名字。同步就是消费
                # 这两个字段，不再自己重算一遍判定——预览看到的过滤与改名就是真正落库的。
                rows = cls._build_upstream_model_rows(provider_name, upstream or [], existing_map)
                if not rows:
                    # 统一规则：只有真正解析出至少一个模型才允许做对账。
                    # 请求失败、空响应、空列表、响应结构异常、列表项均无有效 ID，
                    # 一律视为本次没有正常返回，整轮不增、不改、不删。
                    fetch_err = getattr(client, "_last_fetch_models_error", "") or ""
                    logger.warning(
                        f"[{provider_name}] 上游未正常返回模型列表，跳过本轮同步（不增/不改/不删）"
                        + (f": {fetch_err}" if fetch_err else "")
                    )
                    continue
                upstream_ids = {row["upstream_model_id"] for row in rows}
                # 规则就是过滤条件：is_regex=False 的行丢掉，留下的取 regex_model_id
                # （替换后的名字）落库——deepseek-v4-flash-free 被规则搜到才留下并改名，
                # 原版 deepseek-v4-flash / big-pickle 没被搜到就不进库。没配规则时不过滤，
                # 全量保留并沿用 model_id（已跟踪行保留手工改名），否则没规则的渠道会被清空。
                if cls._channel_has_rewrite_rules(provider_name):
                    targets = {
                        row["upstream_model_id"]: row["regex_model_id"]
                        for row in rows if row["is_regex"]
                    }
                else:
                    targets = {row["upstream_model_id"]: row["model_id"] for row in rows}
                kept_ids = set(targets)
                to_add = kept_ids - existing_ids
                # to_remove 含两类：上游已下线的，以及已入库但不再满足规则的（被规则丢掉的）。
                to_remove = existing_ids - kept_ids
                # 已跟踪且仍保留的行重新过一遍规则：改一版规则就让库里名字跟着变。
                # 目标名与库内一致的行落不进 to_update，无变更的轮次不写库。
                to_update = {
                    uid for uid in (kept_ids & existing_ids)
                    if targets[uid] != existing_map[uid]
                }

                for uid in to_add:
                    await PostgresClient.upsert_provider_model(provider_name, uid, targets[uid])
                for uid in to_update:
                    await PostgresClient.upsert_provider_model(provider_name, uid, targets[uid])
                for uid in to_remove:
                    await PostgresClient.delete_provider_model(provider_name, uid)

                if to_add or to_update or to_remove:
                    logger.debug(
                        f"{provider_name} 同步：上游 {len(upstream_ids)} 个，"
                        f"新增 {len(to_add)} 个，更新映射 {len(to_update)} 个，"
                        f"删除 {len(to_remove)} 个（上游下线），表中原有 {len(existing_ids)} 个"
                    )
                    # 定时刷新拉到实际模型变更时落通知中心（best-effort，无变更不发，避免刷屏）。
                    try:
                        added = sorted(targets[uid] for uid in to_add)
                        # 改名通知要给出「从什么变成了什么」，只报新名字看不出发生了啥。
                        updated = sorted(f"{existing_map[uid]} → {targets[uid]}" for uid in to_update)
                        removed = sorted(existing_map[uid] for uid in to_remove)
                        parts = []
                        if added:
                            parts.append(f"新增 {len(added)} 个")
                        if updated:
                            parts.append(f"更新映射 {len(updated)} 个")
                        if removed:
                            parts.append(f"下线 {len(removed)} 个")
                        from monkeycode_compat.notify_core import emit_notification
                        await emit_notification(
                            "model.new",
                            params={
                                "provider_name": provider_name,
                                "added": added,
                                "updated": updated,
                                "removed": removed,
                            },
                            owner_type="platform",
                            severity="info",
                            kind="model",
                            source="model_catalog",
                            title=f"{provider_name} 模型列表已更新",
                            message=f"渠道 {provider_name} 模型列表变更：{('，'.join(parts)) or '无'}",
                            dedupe_key=f"model_catalog:{provider_name}",
                            dedupe_window_seconds=3600,
                        )
                    except Exception as e:
                        logger.warning(f"模型列表更新通知写入失败: {e}")

        rows = await PostgresClient.list_provider_models(None)
        # 按渠道分组，灌入各 Channel.models（每渠道模型表 = 内存真相源）。
        # 不再维护全局 _model_routes/_model_providers/_model_upstream_map；请求分发扫 Channel.models。
        per_channel: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            per_channel[r["provider"]].append(cls._route_row_from_db(r))
        for provider_name in cls.get_provider_names():
            pool = cls._provider_pools.get(provider_name)
            channel = getattr(pool, "channel", None) if pool else None
            if channel is not None:
                channel.register_models(per_channel.get(provider_name, []))
                cls._prune_account_freezes_for_channel(provider_name, pool, channel)
        await cls.rebuild_model_response_cache()

    @staticmethod
    def _route_row_from_db(r: dict) -> dict:
        """把 provider_models 单行 DB 记录转成 Channel.models 的运行时 row。"""
        extra = r.get("extra_config")
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except (TypeError, ValueError):
                extra = {}
        if not isinstance(extra, dict):
            extra = {}
        return {
            "provider": r["provider"],
            "model_id": r["model_id"],
            "upstream_model_id": r["upstream_model_id"],
            "extra_config": extra,
            "client_preset": extra.get("client_preset"),
            "enable_1m_context": bool(extra.get("enable_1m_context")),
        }

    @classmethod
    def reload_channel_models(cls, provider_name: str, rows: list[dict]) -> None:
        """只重灌单个渠道的 Channel.models（保存模型列表后调用，数据来自 payload，不查库）。"""
        pool = cls._provider_pools.get(provider_name)
        channel = getattr(pool, "channel", None) if pool else None
        if channel is None:
            return
        models: list[dict] = []
        for r in rows:
            model_id = r.get("model_id") or r.get("upstream_model_id")
            if not model_id:
                continue
            row = dict(r)
            row.setdefault("model_id", model_id)
            row.setdefault("provider", provider_name)
            models.append(cls._route_row_from_db(row))
        channel.register_models(models)
        cls._prune_account_freezes_for_channel(provider_name, pool, channel)
        cls._mark_models_dirty()

    @classmethod
    def _prune_account_freezes_for_channel(cls, provider_name: str, pool, channel) -> None:
        """模型表灌入后，清掉该渠道各账号里指向「已不在模型表」的模型冻结条目。

        模型被删/改名后其冻结条目会成为幽灵：账号一直显示 model_frozen，探测也无从下手
        （选路里没这个模型）。这里按最新模型表求差集清内存桶，并同步清 Redis 兜底键。
        用户明确要求：更新模型列表 / 删除模型都要清掉对应冻结关系。
        """
        if pool is None or channel is None:
            return
        valid = {row.get("model_id") for row in getattr(channel, "models", []) if row.get("model_id")}
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        for client in getattr(pool, "clients", []):
            # pool.clients 常规是 AccountClient；对不实现 prune 的测试替身/异类对象宽容跳过。
            prune = getattr(client, "prune_freeze_for_models", None)
            if not callable(prune):
                continue
            removed = prune(valid)
            if not removed:
                continue
            logger.info(
                f"[{provider_name}] 账号 {client.username} 因模型下线清除冻结: {removed}"
            )
            if loop is not None:
                for mid in removed:
                    asyncio.create_task(
                        RedisLimitBackend.clear_account_model_cooldown(provider_name, client.username, mid)
                    )

    @classmethod
    def iter_model_candidates(cls, model_id: str):
        """遍历所有渠道，产出能跑该 model_id 的 (provider_name, pool, row)。纯内存扫描，零查库。"""
        for provider_name, pool in cls._provider_pools.items():
            channel = getattr(pool, "channel", None)
            if channel is None:
                continue
            for row in channel.models:
                if row.get("model_id") == model_id:
                    yield provider_name, pool, row

    @classmethod
    def apply_cooldown_event(cls, provider: str, username: str, model_id: str | None, until: float | None, reason: str = "") -> None:
        """pubsub 冻结事件落到对应 AccountClient 的内存状态桶。

        until=None 表示永久冻结（Redis 无 TTL key）；float>0 表示临时到期点。
        """
        client = cls._find_account_client(provider, username)
        if client is None:
            return
        if until is None:
            # 永久冻结
            client.freeze(kind="account_model" if model_id else "account",
                          model_id=model_id, seconds=None, reason=reason)
            return
        if until <= time.time():
            return
        if model_id:
            client.set_freeze_state_raw(model_untils={model_id: until}, reason=reason)
        else:
            client.set_freeze_state_raw(account_until=until, reason=reason)

    @classmethod
    def clear_cooldown_event(cls, provider: str, username: str, model_id: str | None = None) -> None:
        """pubsub 解冻事件：模型级只清该模型，账号级清整账号及全部模型。"""
        client = cls._find_account_client(provider, username)
        if client is None:
            return
        if model_id is not None:
            client.unfreeze(kind="account_model", model_id=model_id)
        else:
            client.unfreeze(kind="account")

    @classmethod
    async def clean_message(cls):
        providers_config = await config.Config.get_providers()
        clean_task = []
        for provider_name, provider_config in providers_config.items():
            if not provider_config.get("enabled", True):
                continue

            clear_config = await config.Config.get_provider_clear_conversation_config(provider_name)
            if not clear_config.get("enabled", False):
                continue

            pool = ModelClientPool.get_provider_pool(provider_name)
            max_age_hours = clear_config.get("max_age_hours", 2)
            clean_task.append(pool.clean_message(max_age_hours))
        if clean_task:
            await asyncio.gather(*clean_task)

    @classmethod
    def _is_model_tpm_cooling(cls, model_id: str, now: float | None = None) -> bool:
        now = now or time.time()
        until = cls._model_tpm_cooldowns.get(model_id)
        if not until:
            return False
        if until <= now:
            cls._model_tpm_cooldowns.pop(model_id, None)
            return False
        return True

    @classmethod
    def apply_model_tpm_cooldown_event(cls, model_id: str, until: float) -> None:
        if not model_id:
            return
        if until > time.time():
            cls._model_tpm_cooldowns[model_id] = until
        else:
            cls._model_tpm_cooldowns.pop(model_id, None)

    @classmethod
    def clear_model_tpm_cooldown_event(cls, model_id: str) -> None:
        """pubsub model_tpm 回源发现 Redis 已无该窗口时，清本实例内存冷却。"""
        if not model_id:
            return
        cls._model_tpm_cooldowns.pop(model_id, None)

    @classmethod
    async def restore_model_tpm_cooldowns(cls) -> None:
        """启动时按仍存活的 Redis 固定窗口恢复已触线模型；不进入请求热路径。"""
        windows = await RedisLimitBackend.list_model_tpm_windows()
        now = time.time()
        for model_id, used, ttl in windows:
            limit = cls._model_tpm_limit(model_id)
            if limit > 0 and used >= limit and ttl > 0:
                cls._model_tpm_cooldowns[model_id] = now + ttl

    @classmethod
    def _model_tpm_limit(cls, model_id: str) -> int:
        """从运行时内存策略/渠道快照计算模型 TPM；零数据库、零 Redis。"""
        limits = []
        providers_config = config.Config.get_providers_snapshot()
        for provider_name in cls.get_model_route_providers(model_id):
            policy = get_effective_provider_policy_sync(provider_name)
            limit = int(policy.get("model_tpm") or 0)
            if limit <= 0:
                cfg = providers_config.get(provider_name, {})
                limit = int(cfg.get("rate_limit", {}).get("tpm_per_model", 0) or 0)
            if limit > 0:
                limits.append(limit)
        return max(limits) if limits else 0

    @classmethod
    async def _model_tpm_available(cls, model_id: str) -> bool:
        """选路热路径仅看本进程冷却镜像，不查询 Redis。"""
        return not cls._is_model_tpm_cooling(model_id)

    @classmethod
    def _has_model_provider_quota(cls, provider_name: str) -> bool:
        """是否对指定提供商启用模型级 provider 限额检查。"""
        return False  # 模型级 provider 限额功能已禁用

    @classmethod
    def _model_provider_quota_available(cls, model_id: str, provider_name: str) -> bool:
        """检查模型在指定提供商下的每日限额是否可用。"""
        if not cls._has_model_provider_quota(provider_name):
            return True
        per_provider = cls._model_provider_quota.get(model_id)
        if per_provider is None:
            return True  # 无限额数据时默认放行
        remaining = per_provider.get(provider_name)
        if remaining is None:
            return True
        return remaining > 0

    @classmethod
    def update_account_provider_quota(cls, provider_name: str, username: str, remaining: int | None, limit: int | None = None) -> None:
        pool = cls._provider_pools.get(provider_name)
        if not pool:
            return
        for client in pool.clients:
            if client.username == username:
                client.update_provider_quota(remaining, limit)
                return

    @classmethod
    def update_model_provider_quota(cls, model_id: str, provider_name: str, remaining: int) -> None:
        """更新模型在指定提供商下的每日限额余量。"""
        if model_id not in cls._model_provider_quota:
            cls._model_provider_quota[model_id] = {}
        cls._model_provider_quota[model_id][provider_name] = remaining

    @classmethod
    async def record_token_usage(cls, model_id: str, provider_name: str, username: str, tokens: int):
        if tokens <= 0:
            return
        pool = cls._provider_pools.get(provider_name)
        account_client = next((client for client in (getattr(pool, "clients", None) or []) if client.username == username), None)
        model_limit = cls._model_tpm_limit(model_id)
        if account_client is not None:
            tpm_result = await LimitManager.record_tpm_and_maybe_freeze(
                account_client, model_id, tokens, model_limit
            )
        else:
            model_window = await LimitManager.record_model_tokens(model_id, tokens)
            tpm_result = None if model_window is None else {
                "account_used": 0,
                "account_ttl": 0,
                "model_used": model_window[0],
                "model_ttl": model_window[1],
            }
        if tpm_result is not None:
            account_used = int(tpm_result.get("account_used") or 0)
            account_ttl = int(tpm_result.get("account_ttl") or 0)
            account_limit = int(getattr(account_client, "tpm_limit", 0) or 0) if account_client else 0
            if account_limit > 0 and account_used >= account_limit and account_ttl > 0:
                _freeze_account_local_and_broadcast(
                    account_client, account_ttl, f"tpm 触线 {account_used}/{account_limit}"
                )
            model_used = int(tpm_result.get("model_used") or 0)
            model_ttl = int(tpm_result.get("model_ttl") or 0)
            if model_limit > 0 and model_used >= model_limit and model_ttl > 0:
                until = time.time() + model_ttl
                cls._model_tpm_cooldowns[model_id] = until
                try:
                    import runtime_sync
                    asyncio.create_task(runtime_sync.publish(
                        runtime_sync.EVENT_COOLDOWN,
                        f"model-tpm:{model_id}",
                        extra={
                            "scope": "model_tpm",
                            "model_id": model_id,
                            "until": until,
                            "used": model_used,
                            "limit": model_limit,
                        },
                    ))
                except Exception:
                    pass
        if account_client is not None:
            now = time.time()
            account_client._token_usages = [
                (t, n) for t, n in account_client._token_usages if now - t < 60
            ]
            account_client._token_usages.append((now, tokens))

    @classmethod
    def _score_enabled(cls, provider_name: str) -> bool:
        return bool(cls._provider_pools.get(provider_name))

    @classmethod
    def get_channel_score(cls, provider_name: str) -> float:
        return float(cls._channel_scores.get(provider_name, {}).get("score", 1.0))

    @classmethod
    def record_channel_ttft(cls, provider_name: str, username: str | None, model_id: str, ttft_ms: int) -> None:
        if ttft_ms <= 0 or not cls._score_enabled(provider_name):
            return
        current = cls._channel_scores.get(provider_name, {})
        old_ewma = current.get("ttft_ewma_ms")
        ewma = float(ttft_ms) if old_ewma is None else float(old_ewma) * 0.8 + float(ttft_ms) * 0.2
        score = min(10.0, max(0.1, 1000 / max(ewma, 50)))
        # Track error rate: success count
        recent_requests = current.get("recent_requests", 0) + 1
        recent_failures = current.get("recent_failures", 0)
        if recent_requests > 100:
            recent_requests = 50
            recent_failures = int(recent_failures * 0.5)
        error_rate = recent_failures / recent_requests if recent_requests > 0 else 0.0
        cls._channel_scores[provider_name] = {
            "provider": provider_name,
            "last_account": username,
            "last_model": model_id,
            "ttft_ewma_ms": ewma,
            "score": score,
            "recent_requests": recent_requests,
            "recent_failures": recent_failures,
            "error_rate": error_rate,
            "updated_at": time.time(),
        }
        # 同步渠道领域对象的 TTFT/评分（渠道级统计归位）。
        pool = cls._provider_pools.get(provider_name)
        if pool and pool.channel is not None:
            pool.channel.record_ttft(ewma)

    @classmethod
    def record_channel_failure(cls, provider_name: str | None, username: str | None, reason: str = "") -> None:
        if not provider_name or not cls._score_enabled(provider_name):
            return
        reason = summarize_freeze_reason(reason)
        current = cls._channel_scores.get(provider_name, {})
        score = max(0.1, float(current.get("score", 1.0)) * 0.9)
        # Track error rate: failure count
        recent_requests = current.get("recent_requests", 0) + 1
        recent_failures = current.get("recent_failures", 0) + 1
        if recent_requests > 100:
            recent_requests = 50
            recent_failures = int(recent_failures * 0.5)
        error_rate = recent_failures / recent_requests if recent_requests > 0 else 0.0
        cls._channel_scores[provider_name] = {
            **current,
            "provider": provider_name,
            "last_account": username,
            "score": score,
            "last_failure": reason,
            "recent_requests": recent_requests,
            "recent_failures": recent_failures,
            "error_rate": error_rate,
            "updated_at": time.time(),
        }
        # 正常请求的失败唯一在渠道入口进入一次：全平台单一计数 +1。
        # record_account_failure 随后只更新账号统计，不能在两层重复加。
        cls.record_session_failure(None, provider_name, username)
        # 同步渠道领域对象的失败降权。
        pool = cls._provider_pools.get(provider_name)
        if pool and pool.channel is not None:
            pool.channel.record_failure()

    @classmethod
    def _find_account_client(cls, provider_name: str | None, username: str | None) -> AccountClient | None:
        """按 provider_name -> pool -> username 定位 AccountClient。"""
        if not provider_name or not username:
            return None
        pool = cls._provider_pools.get(provider_name)
        if not pool:
            return None
        for client in pool.clients:
            if client.username == username:
                return client
        return None

    @classmethod
    def _is_account_level_failure(cls, reason: str) -> bool:
        """判断失败是否归因于账号本身（应计入连续失败惩罚）。

        参数错误 / 请求体非法等 4xx body 不是账号问题，不计入；
        429/超时/5xx/认证/余额/限流类才累加 _consecutive_failures。
        """
        if not reason:
            return True  # 未知原因默认计入，保守降权
        text = summarize_freeze_reason(reason).lower()
        # 明确非账号问题：4xx 客户端参数类
        non_account_markers = (
            "invalid_request", "bad_request", "parameter", "param",
            "context_length", "too long", "model_not_found", "not found",
            "unsupported", "invalid_api", "invalid model",
        )
        if any(m in text for m in non_account_markers):
            return False
        return True

    @classmethod
    def record_account_success(cls, provider_name: str | None, username: str | None, model_id: str | None = None, session_id: str | None = None, upstream_model_id: str | None = None, duration_ms: int = 0) -> None:
        """记录一次账号级成功（流式/非流式均计），用于账号级错误率分母。
        与 record_channel_ttft 职责分离：TTFT 仍只在首 token 记，本方法记全量成功。
        成功清零连续失败计数，并记录 session 亲和与全局同上游模型亲和。"""
        if cls._score_enabled(provider_name):
            client = cls._find_account_client(provider_name, username)
            if client is not None:
                client._stats.record_success(time.time())
        # 成功即证明账号可达：自动解除临时/永久冻结及熔断状态；disabled（账号开关关闭）不受影响。
        pool = cls._provider_pools.get(provider_name)
        if pool is not None and hasattr(pool, "clear_runtime_freeze_on_success"):
            pool.clear_runtime_freeze_on_success(username, model_id)
        cls._incr_daily_outcome(provider_name, username, success=True)
        cls.record_session_success(session_id, provider_name, username, model_id)
        cls.record_upstream_model_affinity(upstream_model_id, provider_name, username)
        cls.record_short_term_outcome(provider_name, username, success=True, duration_ms=duration_ms)

    @classmethod
    def record_account_failure(cls, provider_name: str | None, username: str | None, reason: str = "", session_id: str | None = None) -> None:
        """记录一次账号级失败，用于账号级错误率分子。渠道级冻结逻辑仍由 record_channel_failure 处理。
        账号级失败累加连续失败计数，**只用于评分降权**（选号时不选），不触发冻结；
        全平台单一失败计数由 record_channel_failure 唯一累加，避免同一错误在渠道/账号两层重复计数；
        短期窗口记失败。"""
        if cls._score_enabled(provider_name):
            client = cls._find_account_client(provider_name, username)
            if client is not None:
                client._stats.record_failure(
                    time.time(),
                    account_level=cls._is_account_level_failure(reason),
                )
        cls._incr_daily_outcome(provider_name, username, success=False)
        cls.record_short_term_outcome(provider_name, username, success=False, duration_ms=0)

    @classmethod
    def _incr_daily_outcome(cls, provider_name: str | None, username: str | None, success: bool) -> None:
        """合并累加 Redis 当天成功/失败计数（每渠道×账号一 key），用于长期可靠性因子。

        不再每个结果起一个 task 直写 Redis：重试风暴下那会同时产生大量后台 task 和
        ``2 × 事件数`` 条命令。这里只在进程内有界缓冲里合并增量，由 :meth:`_reliability_flush_loop`
        定期用一次 pipeline 批量落盘。缓冲满则丢弃新增量（可靠性统计是可降级数据，
        绝不能反压消息主链路）。无事件循环（同步单测）时直接返回，保持原有降级语义。
        """
        if not provider_name or not username:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        kind = "success" if success else "failure"
        key = f"routing:daily:{provider_name}:{username}:{kind}"
        pending = cls._reliability_pending
        if key not in pending and len(pending) >= cls.RELIABILITY_QUEUE_MAX:
            cls._reliability_dropped += 1
            return
        pending[key] = pending.get(key, 0) + 1
        cls._ensure_reliability_flush_task()

    @classmethod
    def _ensure_reliability_flush_task(cls) -> None:
        """惰性启动可靠性计数 flush 任务（首个事件到达时启动）。"""
        task = cls._reliability_flush_task
        if task is not None and not task.done():
            return
        try:
            cls._reliability_flush_task = asyncio.create_task(cls._reliability_flush_loop())
        except RuntimeError:
            cls._reliability_flush_task = None

    @classmethod
    async def _flush_reliability_counts(cls) -> None:
        """把缓冲里的增量一次性写 Redis（单次 pipeline）。失败即丢弃，不重排回缓冲。"""
        if not cls._reliability_pending:
            return
        batch = cls._reliability_pending
        cls._reliability_pending = {}
        dropped = cls._reliability_dropped
        cls._reliability_dropped = 0
        if dropped:
            logger.warning(
                f"[reliability] 可靠性计数缓冲溢出，丢弃 {dropped} 次增量"
                f"（上限 {cls.RELIABILITY_QUEUE_MAX}）"
            )
        try:
            await RedisLimitBackend.batch_incr_calendar_counts(batch, "day")
        except Exception as exc:
            logger.warning(f"[reliability] 批量写日级计数失败，丢弃本批 {len(batch)} 个 key: {exc}")

    @classmethod
    async def _reliability_flush_loop(cls) -> None:
        while True:
            try:
                await asyncio.sleep(cls.RELIABILITY_FLUSH_INTERVAL)
                await cls._flush_reliability_counts()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"[reliability] flush 循环异常: {exc}")

    @classmethod
    async def _account_daily_reliability(cls, provider_name: str | None, username: str | None) -> float:
        """从 Redis 当天计数读长期可靠性（success / max(success+failure, min_samples)）。
        样本不足返回中性 1.0；Redis 不可用返回 1.0。钳到 [RELIABILITY_FLOOR, 1.0]。"""
        if not provider_name or not username:
            return 1.0
        succ, fail = await asyncio.gather(
            RedisLimitBackend.get_calendar_count(f"routing:daily:{provider_name}:{username}:success", "day"),
            RedisLimitBackend.get_calendar_count(f"routing:daily:{provider_name}:{username}:failure", "day"),
        )
        succ = int(succ or 0)
        fail = int(fail or 0)
        total = succ + fail
        if total < cls.RELIABILITY_MIN_SAMPLES:
            return 1.0
        reliability = succ / total
        return max(cls.RELIABILITY_FLOOR, min(1.0, reliability))

    @classmethod
    async def _candidate_reliability(cls, candidates: list[dict]) -> dict[tuple[str, str], float]:
        """批量取候选当天可靠性；Redis 慢时 250ms 内退化为中性值。

        单次 pipeline 读取所有候选的 success/failure 当天计数（``2 × 候选数`` 个 key
        合并到一次往返），替代原先对每个候选并发调用 ``_account_daily_reliability``
        （各自 2 次 GET）——大并发 + 多轮重试会把逐候选扇出乘法放大。可靠性钳制口径
        与 :meth:`_account_daily_reliability` 保持一致。
        """
        keys: dict[tuple[str, str], None] = {}
        for c in candidates:
            keys.setdefault((c["provider_name"], c["account_client"].username), None)
        if not keys:
            return {}
        neutral = {key: 1.0 for key in keys}
        # 每个候选两个日历计数 key：success / failure。用列表保持与候选一一对应的顺序。
        count_keys: list[str] = []
        for provider_name, username in keys:
            count_keys.append(f"routing:daily:{provider_name}:{username}:success")
            count_keys.append(f"routing:daily:{provider_name}:{username}:failure")
        counts = await aux_redis_call(
            RedisLimitBackend.batch_get_calendar_counts(count_keys, "day"),
            stage="candidate_reliability",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=None,
        )
        if not counts:
            return neutral
        out: dict[tuple[str, str], float] = {}
        for provider_name, username in keys:
            succ = int(counts.get(f"routing:daily:{provider_name}:{username}:success", 0) or 0)
            fail = int(counts.get(f"routing:daily:{provider_name}:{username}:failure", 0) or 0)
            total = succ + fail
            if total < cls.RELIABILITY_MIN_SAMPLES:
                out[(provider_name, username)] = 1.0
            else:
                out[(provider_name, username)] = max(
                    cls.RELIABILITY_FLOOR, min(1.0, succ / total)
                )
        return out

    @classmethod
    def _candidate_ttft_min(cls, candidates: list[dict]) -> float | None:
        """候选集中最快 ttft_ewma；用于相对速度排名。无任何 ttft 数据返回 None。"""
        ttft_values = []
        for c in candidates:
            ewma = cls._channel_scores.get(c["provider_name"], {}).get("ttft_ewma_ms")
            if ewma and ewma > 0:
                ttft_values.append(float(ewma))
        return min(ttft_values) if ttft_values else None

    @classmethod
    def _account_key(cls, provider_name: str | None, username: str | None) -> str:
        return f"{provider_name or ''}:{username or ''}"

    @classmethod
    def record_session_success(cls, session_id: str | None, provider_name: str | None, username: str | None, model_id: str | None = None) -> None:
        """记录 session → 账号亲和（成功后该 session 倾向复用同一账号），并清零全平台失败计数。

        亲和仍按 session 隔离（无 session 头不记亲和）；全平台失败计数是单一计数器，
        任一请求成功即清零（与账号级 _consecutive_failures「一次成功即清零」对称）。"""
        cls._session_failures.pop(cls._GLOBAL_FAILURE_BUCKET, None)
        if not session_id:
            return
        cls._session_affinity[session_id] = {
            "account_key": cls._account_key(provider_name, username),
            "last_success_ts": time.time(),
            "model": model_id or "",
        }

    @classmethod
    def record_session_failure(cls, session_id: str | None, provider_name: str | None, username: str | None) -> None:
        """全平台失败计数 +1。单一计数器，不分渠道/账号/客户端 api key/session；
        达 GLOBAL_FAILURE_TRIGGER 后评分按各渠道成功率导向流量。"""
        bucket = cls._session_failures.setdefault(cls._GLOBAL_FAILURE_BUCKET, {})
        now = time.time()
        rec = bucket.get(cls._GLOBAL_FAILURE_BUCKET, {"count": 0, "last_ts": 0})
        if now - rec["last_ts"] >= cls.SESSION_FAILURE_TTL:
            rec = {"count": 0, "last_ts": 0}
        rec["count"] += 1
        rec["last_ts"] = now
        bucket[cls._GLOBAL_FAILURE_BUCKET] = rec

    @classmethod
    def _session_anti_affinity_factor(cls, session_id: str | None, provider_name: str, account_client: AccountClient) -> float:
        """全平台避险：全局失败计数 < GLOBAL_FAILURE_TRIGGER 时中性 1.0；
        达阈值后改为按该渠道近期成功率加权——成功率越高分数越高，把流量导向高成功率渠道。
        这是"导向高成功率渠道"的唯一抓手：一个对所有人一视同仁的计数器本身无法分流，
        必须借助现有每渠道成功率数据（_channel_error_rate）来区分候选。"""
        bucket = cls._session_failures.get(cls._GLOBAL_FAILURE_BUCKET, {})
        rec = bucket.get(cls._GLOBAL_FAILURE_BUCKET)
        now = time.time()
        if not rec or now - rec["last_ts"] >= cls.SESSION_FAILURE_TTL:
            return 1.0
        if rec["count"] < cls.GLOBAL_FAILURE_TRIGGER:
            return 1.0
        # 避险模式：按渠道成功率加权。error_rate 越低（成功率越高）→ 越接近 1.0；
        # error_rate 越高 → 指数级压低，直到地板。失败次数越多避险越狠。
        # 无样本的渠道不能当 100% 成功（_channel_error_rate 无数据返回 0.0），否则冷渠道
        # 会凭"零错误"碾压有真实成绩的渠道；给中性基线。
        stats = cls._channel_scores.get(provider_name) or {}
        if not stats.get("recent_requests"):
            channel_success = cls.GLOBAL_STRESS_NO_DATA_SUCCESS
        else:
            channel_success = max(0.0, 1.0 - cls._channel_error_rate(provider_name))
        power = 1.0 + (rec["count"] - cls.GLOBAL_FAILURE_TRIGGER + 1)
        return max(cls.GLOBAL_STRESS_FLOOR, channel_success ** power)

    @classmethod
    def _session_affinity_factor(cls, session_id: str | None, provider_name: str, account_client: AccountClient) -> float:
        """session 亲和：成功复用升权（1.3）。无 session 或未命中返回 1.0。"""
        if not session_id:
            return 1.0
        now = time.time()
        account_key = cls._account_key(provider_name, account_client.username)
        aff = cls._session_affinity.get(session_id)
        if aff and now - aff["last_success_ts"] < cls.SESSION_AFFINITY_TTL and aff["account_key"] == account_key:
            return cls.SESSION_AFFINITY_FACTOR
        return 1.0

    @classmethod
    def _session_factor(cls, session_id: str | None, provider_name: str, account_client: AccountClient) -> float:
        """session 维度系数：亲和（成功复用升权，按 session）。
        保留组合语义供历史调用/测试；评分主链路改用 _session_anti_affinity_factor × _reuse_affinity_factor
        （亲和与 upstream 亲和取最大值，不再连乘）。"""
        return cls._session_anti_affinity_factor(session_id, provider_name, account_client) * cls._session_affinity_factor(session_id, provider_name, account_client)

    @classmethod
    def _reuse_affinity_factor(cls, session_id: str | None, provider_name: str, account_client: AccountClient, upstream_model_id: str | None = None) -> float:
        """复用升权因子：session 亲和与 upstream 模型亲和取最大值（不再连乘）。
        二者目标高度重叠（都是"最近成功的继续用"），取最大保留复用收益但不放大概率达 >1.6 的叠加粘性。"""
        account_key = cls._account_key(provider_name, account_client.username)
        session_aff = cls._session_affinity_factor(session_id, provider_name, account_client)
        upstream_aff = cls._upstream_model_affinity_factor(upstream_model_id, provider_name, account_key)
        return max(session_aff, upstream_aff)

    @classmethod
    def _success_rotation_factor(cls, account_client: AccountClient) -> float:
        """连续成功让权：成功 streak 超过 grace 后指数衰减让权，把流量让给其它健康渠道。
        ≤ grace → 1.0；超 1 次 → ×0.8，依此衰减到 CONSECUTIVE_SUCCESS_FLOOR 封底。失败清零回到 1.0。"""
        n = account_client._stats.consecutive_successes()
        if n <= cls.CONSECUTIVE_SUCCESS_GRACE:
            return 1.0
        excess = n - cls.CONSECUTIVE_SUCCESS_GRACE
        return max(cls.CONSECUTIVE_SUCCESS_FLOOR, cls.CONSECUTIVE_SUCCESS_DECAY ** excess)

    @classmethod
    def cleanup_session_state(cls) -> None:
        """周期性清理过期的 session 亲和/反亲和记录，避免内存泄漏。"""
        now = time.time()
        # 亲和：TTL 外删除
        expired_aff = [sid for sid, rec in cls._session_affinity.items() if now - rec.get("last_success_ts", 0) > cls.SESSION_AFFINITY_TTL]
        for sid in expired_aff:
            cls._session_affinity.pop(sid, None)
        # 全局失败计数：最后一次失败超过 TTL 则清掉唯一计数器
        for sid in list(cls._session_failures.keys()):
            recs = cls._session_failures[sid]
            for key in list(recs.keys()):
                if now - recs[key].get("last_ts", 0) > cls.SESSION_FAILURE_TTL:
                    recs.pop(key, None)
            if not recs:
                cls._session_failures.pop(sid, None)
        # 同上游模型亲和：TTL 外删除
        expired_model_aff = [
            umid for umid, rec in cls._upstream_model_affinity.items()
            if now - rec.get("last_success_ts", 0) > cls.UPSTREAM_MODEL_AFFINITY_TTL
        ]
        for umid in expired_model_aff:
            cls._upstream_model_affinity.pop(umid, None)
        # 短期窗口：裁剪每个账号窗口外样本，空列表回收
        for key in list(cls._short_term_outcomes.keys()):
            recs = cls._short_term_outcomes[key]
            recs[:] = [r for r in recs if now - r.get("ts", 0) < cls.SHORT_WINDOW_SECONDS]
            if not recs:
                cls._short_term_outcomes.pop(key, None)

    @classmethod
    def record_upstream_model_affinity(cls, upstream_model_id: str | None, provider_name: str | None, username: str | None) -> None:
        """记录全局同上游模型亲和：该 upstream_model_id 最近被哪个账号成功命中。
        选路时候选若同 upstream_model_id 且同 account，获亲和升权（prompt cache 复用前提）。"""
        if not upstream_model_id:
            return
        cls._upstream_model_affinity[upstream_model_id] = {
            "account_key": cls._account_key(provider_name, username),
            "last_success_ts": time.time(),
        }

    @classmethod
    def _upstream_model_affinity_factor(cls, upstream_model_id: str | None, provider_name: str, account_key: str) -> float:
        """同上游模型亲和因子：候选的 upstream_model_id 最近被本账号成功命中过且在 TTL 内 → UPSTREAM_MODEL_AFFINITY_FACTOR；
        upstream_model_id 未命中或 account 不同 → 1.0（cache 命中要同账号同模型才有效，account 不同不升权）。"""
        if not upstream_model_id:
            return 1.0
        rec = cls._upstream_model_affinity.get(upstream_model_id)
        if not rec:
            return 1.0
        if time.time() - rec.get("last_success_ts", 0) >= cls.UPSTREAM_MODEL_AFFINITY_TTL:
            return 1.0
        if rec.get("account_key") == account_key:
            return cls.UPSTREAM_MODEL_AFFINITY_FACTOR
        return 1.0

    @classmethod
    def record_short_term_outcome(cls, provider_name: str | None, username: str | None, success: bool, duration_ms: int = 0) -> None:
        """记录短期窗口（4分钟/10次）内的成功/失败/耗时样本，供短期成功率偏好与响应均衡因子读取。"""
        if not provider_name or not username:
            return
        key = cls._account_key(provider_name, username)
        now = time.time()
        recs = cls._short_term_outcomes.setdefault(key, [])
        recs[:] = [r for r in recs if now - r.get("ts", 0) < cls.SHORT_WINDOW_SECONDS]
        recs.append({"ts": now, "success": bool(success), "duration_ms": int(duration_ms or 0)})
        # 只保留最近 SHORT_WINDOW_MAX_SAMPLES 条
        if len(recs) > cls.SHORT_WINDOW_MAX_SAMPLES:
            del recs[: len(recs) - cls.SHORT_WINDOW_MAX_SAMPLES]

    @classmethod
    def _short_term_samples(cls, account_key: str) -> list[dict]:
        """读取并就地裁剪账号短期窗口样本到窗口内，返回列表（不截断到 10 条，读取侧按需取最近 N 条）。"""
        recs = cls._short_term_outcomes.get(account_key)
        if not recs:
            return []
        now = time.time()
        recs[:] = [r for r in recs if now - r.get("ts", 0) < cls.SHORT_WINDOW_SECONDS]
        if not recs:
            cls._short_term_outcomes.pop(account_key, None)
            return []
        return recs

    @classmethod
    def _short_term_success_factor(cls, account_key: str) -> float:
        """短期成功率偏好因子（4分钟/10次窗口）。
        窗口内有失败记录时（报错触发），成功率越高因子越高（[FLOOR, BOOST]）；
        窗口内无失败记录时返回 1.0（正常基线，不额外扰动）。"""
        recs = cls._short_term_samples(account_key)
        if not recs:
            return 1.0
        recent = recs[-cls.SHORT_WINDOW_MAX_SAMPLES:]
        total = len(recent)
        failures = sum(1 for r in recent if not r.get("success"))
        if failures == 0:
            return 1.0
        success_rate = (total - failures) / total
        return cls.SHORT_TERM_SUCCESS_FLOOR + (cls.SHORT_TERM_SUCCESS_BOOST - cls.SHORT_TERM_SUCCESS_FLOOR) * success_rate

    @classmethod
    def _short_term_latency_factor(cls, account_key: str) -> float:
        """短期响应均衡因子：取该账号窗口内最近 10 次成功的中位耗时，
        最近一次成功耗时若 > 中位×OVERSHOOT（远超均值）则短期降权；否则 1.0。
        样本不足（<2 条成功）返回 1.0。"""
        recs = cls._short_term_samples(account_key)
        if not recs:
            return 1.0
        recent = recs[-cls.SHORT_WINDOW_MAX_SAMPLES:]
        success_durations = [int(r.get("duration_ms") or 0) for r in recent if r.get("success") and r.get("duration_ms")]
        if len(success_durations) < 2:
            return 1.0
        last_duration = success_durations[-1]
        sorted_d = sorted(success_durations[:-1])  # 历史样本（不含本次）做中位基准
        if not sorted_d:
            return 1.0
        mid = len(sorted_d) // 2
        median = float(sorted_d[mid])
        if median <= 0:
            return 1.0
        # 绝对慢分支：最近一次成功耗时本身超阈值即罚，堵「一直慢->中位也慢->相对分支永不触发」漏洞。
        if last_duration > cls.ABSOLUTE_SLOW_THRESHOLD_MS:
            return cls.SHORT_TERM_LATENCY_PENALTY
        if last_duration > median * cls.SHORT_TERM_LATENCY_OVERSHOOT:
            return cls.SHORT_TERM_LATENCY_PENALTY
        return 1.0

    @classmethod
    def _absolute_slow_factor(cls, provider_name: str, account_key: str) -> float:
        """绝对慢兜底：渠道 TTFT EWMA 或 账号近期成功样本中位耗时超阈值 -> 重罚。
        相对速度排名只看池内相对快慢；本因子补「绝对慢」维度，避免一直慢的渠道
        因相对最快而持续被选中。任一信号超阈值即罚。"""
        # 信号1：渠道级 TTFT EWMA（首 token 慢）
        ttft_ewma = cls._channel_scores.get(provider_name, {}).get("ttft_ewma_ms")
        if ttft_ewma and float(ttft_ewma) > cls.ABSOLUTE_SLOW_THRESHOLD_MS:
            return cls.ABSOLUTE_SLOW_PENALTY
        # 信号2：账号级近期成功样本中位全量耗时（整体慢）
        recs = cls._short_term_samples(account_key)
        if recs:
            durations = [int(r.get("duration_ms") or 0) for r in recs if r.get("success") and r.get("duration_ms")]
            if len(durations) >= 2:
                durations.sort()
                mid = len(durations) // 2
                if float(durations[mid]) > cls.ABSOLUTE_SLOW_THRESHOLD_MS:
                    return cls.ABSOLUTE_SLOW_PENALTY
        return 1.0

    @classmethod
    def _quota_factor(cls, account_client: AccountClient, model_id: str, provider_name: str | None = None) -> float:
        factors = []
        daily_remaining = account_client.daily_requests_remaining
        daily_limit = account_client.daily_requests_limit
        if daily_remaining is not None:
            if daily_remaining <= 0:
                return 0.0
            if daily_limit and daily_limit > 0:
                factors.append(0.5 + min(1.0, daily_remaining / daily_limit))
            else:
                factors.append(1.0 + min(1.0, daily_remaining / 1000))

        model_remaining = account_client.model_daily_remaining.get(model_id)
        if model_remaining is not None:
            if model_remaining <= 0:
                return 0.0
            factors.append(1.0 + min(1.0, model_remaining / 1000))

        result = sum(factors) / len(factors) if factors else 1.0

        # 余量联动保护：按次计费账号日额度快耗尽（<20%）时强制让出，避免被大请求打爆。
        if provider_name and daily_limit and daily_limit > 0 and daily_remaining is not None:
            channel = getattr(account_client, "channel", None)
            billing_mode = getattr(channel, "billing_mode", cls._provider_billing_mode.get(provider_name, "token"))
            if billing_mode == "request" and daily_remaining / daily_limit < 0.2:
                result *= 0.3

        return result

    @classmethod
    def _headroom_factor(cls, account_client: AccountClient) -> float:
        now = time.time()
        factors = []
        if account_client.rpm_limit > 0:
            used = len([t for t in account_client._requests if now - t < 60])
            remaining = account_client.rpm_limit - used
            if remaining <= 0:
                return 0.0
            factors.append(0.5 + min(1.0, remaining / account_client.rpm_limit))
        if account_client.tpm_limit > 0:
            used = sum(n for t, n in account_client._token_usages if now - t < 60)
            remaining = account_client.tpm_limit - used
            if remaining <= 0:
                return 0.0
            factors.append(0.5 + min(1.0, remaining / account_client.tpm_limit))
        if account_client.concurrent_limit > 0:
            remaining = account_client.concurrent_limit - account_client.concurrent_used
            if remaining <= 0:
                return 0.0
            factors.append(0.5 + min(1.0, remaining / account_client.concurrent_limit))
        return sum(factors) / len(factors) if factors else 1.0

    @classmethod
    def _pick_window_factor(cls, picks: list[float], now: float) -> tuple[float, list[float]]:
        """账号级滑窗计数 → 频率因子（线性）。返回 (factor, 清理后的近期时间戳)。
        0~SOFT_CAP 次为 1.0（不惩罚）；之后线性衰减到 0.3 封底，避免饿死。"""
        recent = [t for t in picks if now - t < cls.PICK_WINDOW_SECONDS]
        n = len(recent)
        if n <= cls.PICK_SOFT_CAP:
            return 1.0, recent
        excess = n - cls.PICK_SOFT_CAP
        return max(0.3, 1.0 - excess * 0.15), recent

    @classmethod
    def _provider_pick_window_factor(cls, picks: list[float], now: float) -> tuple[float, list[float]]:
        """渠道级滑窗计数 → 频率因子（指数）。返回 (factor, 清理后的近期时间戳)。
        命中 < PROVIDER_PICK_THRESHOLD 不降权；达到阈值起按指数衰减到 PROVIDER_PICK_FLOOR 封底。
        指数衰减比线性更激进，避免高分渠道在被高频命中后仍持续垄断。"""
        recent = [t for t in picks if now - cls.PICK_WINDOW_SECONDS < t <= now]
        n = len(recent)
        if n < cls.PROVIDER_PICK_THRESHOLD:
            return 1.0, recent
        exponent = n - cls.PROVIDER_PICK_THRESHOLD + 1
        factor = cls.PROVIDER_PICK_DECAY_BASE ** exponent
        return max(cls.PROVIDER_PICK_FLOOR, factor), recent

    @classmethod
    def _clean_provider_pick_history(cls, now: float) -> list[tuple[float, str]]:
        """清理 provider pick history：只保留最近 10 分钟内的最近 20 轮成功选择。"""
        cutoff = now - cls.PROVIDER_USAGE_HISTORY_WINDOW_SECONDS
        recent = [(t, p) for (t, p) in cls._provider_pick_history if t >= cutoff]
        if len(recent) > cls.PROVIDER_USAGE_HISTORY_MAX_ROUNDS:
            recent = recent[-cls.PROVIDER_USAGE_HISTORY_MAX_ROUNDS:]
        cls._provider_pick_history = recent
        return recent

    @classmethod
    def _last_used_provider(cls, now: float | None = None) -> str | None:
        """最近一次成功 reserve 的渠道名；用于非智能策略相邻两轮避让同渠道。"""
        history = cls._clean_provider_pick_history(now if now is not None else time.time())
        return history[-1][1] if history else None

    @classmethod
    def _avoid_last_provider(cls, candidates: list[dict]) -> list[dict]:
        """排除上一次命中的渠道；若排除后为空则回退原候选，避免单渠道被饿死。"""
        if not candidates:
            return candidates
        last_provider = cls._last_used_provider()
        if not last_provider:
            return candidates
        filtered = [c for c in candidates if c.get("provider_name") != last_provider]
        return filtered or candidates

    @classmethod
    def _provider_recent_usage_counts(cls, provider_name: str, history: list[tuple[float, str]] | None = None) -> dict[int, int]:
        """统计 provider 在最近 3/5/10/20 轮成功选择中的出现次数。"""
        if history is None:
            history = cls._provider_pick_history
        providers = [p for _t, p in history]
        return {
            3: sum(1 for p in providers[-3:] if p == provider_name),
            5: sum(1 for p in providers[-5:] if p == provider_name),
            10: sum(1 for p in providers[-10:] if p == provider_name),
            20: sum(1 for p in providers[-20:] if p == provider_name),
        }

    @classmethod
    def _provider_recent_usage_score(cls, provider_name: str, base_score: float, median_score: float, now: float | None = None) -> float:
        """按最近轮次使用频率把 provider 权重降到概率档。

        最近 3 轮保持基本正常；最近 5/10/20 轮命中过多时逐级降档。
        高频档用 min(base_score * factor, median_score * cap) 限制高分渠道继续碾压。"""
        if base_score <= 0 or not provider_name:
            return max(0.0, base_score)
        history = cls._clean_provider_pick_history(now if now is not None else time.time())
        counts = cls._provider_recent_usage_counts(provider_name, history)
        cap_base = median_score if median_score > 0 else base_score

        if counts[20] >= cls.PROVIDER_USAGE_20_COUNT:
            return min(base_score * cls.PROVIDER_USAGE_20_FACTOR, cap_base * cls.PROVIDER_USAGE_20_MEDIAN_CAP)
        if counts[10] >= cls.PROVIDER_USAGE_10_COUNT:
            return min(base_score * cls.PROVIDER_USAGE_10_FACTOR, cap_base * cls.PROVIDER_USAGE_10_MEDIAN_CAP)
        if counts[5] >= cls.PROVIDER_USAGE_5_COUNT:
            return base_score * cls.PROVIDER_USAGE_5_FACTOR
        return base_score

    @classmethod
    def _frequency_factor(cls, account_client: AccountClient, provider_name: str | None = None) -> float:
        """短期滑窗频率惩罚：账号级（线性） + 渠道级（指数），任一维度高频都会降权。

        账号级：该账号窗口内被选中次数过多 → 降权（打破单账号粘性）。
        渠道级：该 provider 窗口内被选中次数过多 → 指数降权（打破同渠道多账号轮转绕过账号级惩罚的粘性）。
        取两者更严一方，避免相乘跌破封底饿死。"""
        now = time.time()

        account_factor = account_client._stats.pick_factor(now, cls.PICK_SOFT_CAP, cls.PICK_WINDOW_SECONDS)

        if not provider_name:
            return account_factor

        provider_picks = cls._provider_recent_picks.get(provider_name, [])
        provider_factor, provider_picks = cls._provider_pick_window_factor(provider_picks, now)
        if provider_picks:
            cls._provider_recent_picks[provider_name] = provider_picks  # 顺手清理过期时间戳
        else:
            cls._provider_recent_picks.pop(provider_name, None)  # 清空则移除 key，避免长期积累空列表

        return min(account_factor, provider_factor)

    @classmethod
    def _account_error_rate(cls, account_client: AccountClient) -> float:
        """账号级错误率：当前有效错误率滑窗内 failure / (success + failure)。

        窗口长度由全局错误水位动态伸缩（_effective_error_window）：高错误期缩短，
        加快账号恢复，避免系统性失败时把所有账号饿死。"""
        now = time.time()
        window = cls._effective_error_window()
        return account_client._stats.error_rate(now, window)

    @classmethod
    def _global_recent_failure_count(cls) -> int:
        """全局近期失败计数：聚合各渠道 _channel_scores 的 recent_failures 累计计数。
        用于错误率窗口阶梯伸缩的输入。无数据返回 0。"""
        total = 0
        for provider_name, scores in cls._channel_scores.items():
            try:
                total += int(scores.get("recent_failures", 0) or 0)
            except (TypeError, ValueError):
                continue
        return total

    @classmethod
    def _effective_error_window(cls) -> float:
        """账号级错误率滑窗长度：全局错误水位越高窗口越短（阶梯），低错误期返回 ERROR_WINDOW_SECONDS 基线。

        ⚠️ 只影响账号级错误率窗口，绝不触碰熔断窗口 FAILURE_WINDOW_SECONDS（两者必须解耦）。
        """
        global_fail = cls._global_recent_failure_count()
        for threshold, window in cls.ERROR_WINDOW_TIERS:
            if global_fail >= threshold:
                return float(window)
        return float(cls.ERROR_WINDOW_SECONDS)

    @classmethod
    def _channel_error_rate(cls, provider_name: str | None) -> float:
        """渠道级近期错误率：取 _channel_scores 维护的 error_rate（由 record_channel_ttft/failure 更新）。
        补全用户强调缺失的"渠道级"维度。无数据返回 0.0（不惩罚）。"""
        if not provider_name:
            return 0.0
        try:
            return float(cls._channel_scores.get(provider_name, {}).get("error_rate", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    async def _api_key_volume_factor(cls, api_key: str | None) -> float:
        """读当前请求所用 API Key 的 RPM 窗口用量 → 阶梯映射出 volume_factor（0.0~MAX）。

        流量越高 volume_factor 越大 → 评分时对近期错误渠道/账号抑制越狠。
        api_key 为空 / Redis 不可用 / 读取出错时返回 0.0（退化为旧行为，不额外抑制）。
        只查单窗口（requests_per_minute），不调完整的 get_usage，轻量。"""
        if not api_key:
            return 0.0
        subject = RateLimiter._subject(api_key)
        # 请求级缓存：同一请求因模型组/重试会多次算 volume，但同一 API key 的 RPM 窗口
        # 一次请求内读一次即可。缓存挂在 begin_routing_redis_scope 建立的 state 上，
        # 随请求结束自然失效，绝不跨请求共享陈旧值。
        scope = routing_redis_state()
        cache = scope.get("cache") if isinstance(scope, dict) else None
        cache_key = f"api_key_volume:{subject}"
        if isinstance(cache, dict) and cache_key in cache:
            return cache[cache_key]
        result = await aux_redis_call(
            RedisLimitBackend.get_window(f"api-key:{subject}:requests_per_minute"),
            stage="api_key_volume",
            timeout=ROUTING_AUX_REDIS_TIMEOUT_SECONDS,
            default=(0, 0),
        )
        used, _ttl = result or (0, 0)
        rpm = int(used or 0)
        if rpm <= 0:
            factor = 0.0
        else:
            factor = 0.0
            for threshold, tier_factor in cls.VOLUME_RPM_TIERS:
                if rpm >= threshold:
                    factor = tier_factor
            # 超过最高阶梯阈值按封顶计；负值兜底
            if rpm > cls.VOLUME_RPM_TIERS[-1][0]:
                factor = cls.VOLUME_RPM_MAX
            factor = max(0.0, factor)
        if isinstance(cache, dict):
            cache[cache_key] = factor
        return factor

    @classmethod
    def _volume_error_factor(cls, volume_factor: float, account_error_rate: float, channel_error_rate: float) -> float:
        """流量驱动的近期错误抑制因子（渠道级 + 账号级，取更严一方）。

        1 - volume_factor * max(acct_err, chan_err)：流量越高且错误越高 → 抑制越狠。
        volume_factor=0（低流量或无 RPM 数据）→ 1.0 不抑制（旧行为）。封底 VOLUME_ERROR_FLOOR。"""
        if volume_factor <= 0:
            return 1.0
        combined = max(account_error_rate, channel_error_rate)
        if combined <= 0:
            return 1.0
        return max(cls.VOLUME_ERROR_FLOOR, 1.0 - volume_factor * combined)

    @classmethod
    def _candidate_weight(cls, provider_name: str, account_client: AccountClient, model_id: str) -> dict:
        channel_score = cls.get_channel_score(provider_name)
        priority_factor = 1 / (1 + max(0, account_client.priority))
        quota_factor = cls._quota_factor(account_client, model_id, provider_name)
        headroom_factor = cls._headroom_factor(account_client)
        score = account_client.weight * priority_factor * channel_score * quota_factor * headroom_factor
        return {
            "score": score,
            "channel_score": channel_score,
            "priority_factor": priority_factor,
            "quota_factor": quota_factor,
            "headroom_factor": headroom_factor,
            "weight": account_client.weight,
        }

    @classmethod
    def _weighted_pick(cls, candidates: list[dict]) -> dict:
        total = sum(max(0.0, c["route_info"]["score"]) for c in candidates)
        if total <= 0:
            return random.choice(candidates)
        target = random.uniform(0, total)
        cursor = 0.0
        for candidate in candidates:
            cursor += max(0.0, candidate["route_info"]["score"])
            if cursor >= target:
                return candidate
        return candidates[-1]

    @classmethod
    def _provider_first_weighted_pick(cls, candidates: list[dict], *, explore: bool = False) -> dict | None:
        """provider-first 带权随机：先按 provider 聚合选渠道，再在该渠道内选账号。

        - provider 聚合用该渠道内候选的最大分（max），不按账号数线性放大渠道中奖率。
        - 最近 10 分钟内的最近 20 轮里使用频繁的 provider 会进入概率降档，不再完全靠高分碾压。
        - 用降档后的 provider 权重带权随机选 provider；选中后在其候选内再用 `_weighted_pick` 选账号。
        - 只有一个 provider 时仍会选中它，避免误伤可用性。
        - explore=True 时忽略速度权重与使用降档，在所有已进池的 provider 间均匀随机选一个
          （intermittent exploration），给非 top 渠道间歇性打一次流量，避免流量锁死在少数渠道上。
        """
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]

        if explore:
            # 间歇性探索轮：不按分加权，池内 provider 均匀随机。账号仍在选中 provider 内带权选。
            groups: dict[str, list[dict]] = {}
            for c in candidates:
                groups.setdefault(c["provider_name"], []).append(c)
            chosen_provider_name = random.choice(list(groups.keys()))
            return cls._weighted_pick(groups[chosen_provider_name])

        now = time.time()
        # 1) 按 provider 分组
        groups: dict[str, list[dict]] = {}
        for c in candidates:
            groups.setdefault(c["provider_name"], []).append(c)

        # 2) 每个 provider 聚合分数 = max(候选 score)，避免同 provider 多账号/多模型候选放大中奖率
        base_scores: dict[str, float] = {}
        for provider_name, members in groups.items():
            base_scores[provider_name] = max(max(0.0, m["route_info"]["score"]) for m in members)
        sorted_scores = sorted(base_scores.values())
        mid = len(sorted_scores) // 2
        median_score = sorted_scores[mid] if len(sorted_scores) % 2 else (sorted_scores[mid - 1] + sorted_scores[mid]) / 2

        # 3) 最近轮次使用过多的 provider 降到概率档
        summaries: list[dict] = []
        for provider_name, members in groups.items():
            base_score = base_scores[provider_name]
            adjusted_score = cls._provider_recent_usage_score(provider_name, base_score, median_score, now)
            summaries.append({"provider_name": provider_name, "weight": adjusted_score, "members": members})

        # 3.5) Exploration：给每个 provider 权重一个相对全局最高分的 epsilon 下限，
        # 让最快那条大概率命中，但偶尔（约 EXPLORE_EPSILON）轮转给次优候选，避免一味死磕。
        top_weight = max((s["weight"] for s in summaries), default=0.0)
        if top_weight > 0:
            floor = top_weight * cls.EXPLORE_EPSILON
            for s in summaries:
                if s["weight"] < floor:
                    s["weight"] = floor

        # 4) 带权随机选 provider
        total = sum(s["weight"] for s in summaries)
        if total <= 0:
            chosen_provider = random.choice(summaries)
        else:
            target = random.uniform(0, total)
            cursor = 0.0
            chosen_provider = summaries[-1]
            for s in summaries:
                cursor += s["weight"]
                if cursor >= target:
                    chosen_provider = s
                    break

        # 5) 在选中 provider 内带权随机选账号
        return cls._weighted_pick(chosen_provider["members"])

    @classmethod
    def _speed_score(cls, provider_name: str, ttft_min: float | None, no_data_baseline: float = 0.5) -> float:
        """速度分：给了候选集 ttft_min 时按相对排名（最快=1.0，慢的按比例衰减，下限 0.1）；
        否则退化为旧的绝对值曲线（向后兼容同步测试）。无 ttft 数据给 no_data_baseline
        （默认 0.5 向后兼容；score 主链路传 SPEED_NO_DATA_BASELINE=0.7 以缩小冷渠道差距）。"""
        ttft_ewma = cls._channel_scores.get(provider_name, {}).get("ttft_ewma_ms")
        if not ttft_ewma or ttft_ewma <= 0:
            return no_data_baseline
        if ttft_min and ttft_min > 0:
            return min(1.0, max(0.1, ttft_min / ttft_ewma))
        return min(1.0, max(0.0, 1000 / ttft_ewma / 10))

    @classmethod
    def _intelligent_score(cls, provider_name: str, account_client: AccountClient, model_id: str, session_id: str | None = None, reliability_factor: float = 1.0, ttft_min: float | None = None, upstream_model_id: str | None = None, volume_factor: float = 0.0) -> dict:
        """智能评分：多维度加权（速度、错误率、余额、优先级、权重）× 长期可靠性。

        reliability_factor / ttft_min 由异步选择器传入（读 Redis 日级成功率 + 候选集最快 ttft）；
        缺省 1.0 / None 时退化为旧行为，供同步单测直接调用。"""
        # Check frozen
        if account_client.is_frozen:
            return {"score": 0, "frozen": True}
        # Check balance
        if account_client.balance is not None and account_client.balance_threshold > 0:
            if account_client.balance < account_client.balance_threshold:
                return {"score": 0, "frozen": True, "balance_low": True}
        # Speed score (0-1) — 相对排名（有 ttft_min 时）
        speed_score = cls._speed_score(provider_name, ttft_min, cls.SPEED_NO_DATA_BASELINE)
        # Error score (0-1) — 账号级即时错误率（动态窗口，瞬时波动信号）
        error_rate = cls._account_error_rate(account_client)
        # 连续失败指数降权：连续报错比间隔报错惩罚更重
        consecutive_factor = cls.CONSECUTIVE_DECAY_INTELLIGENT ** account_client._stats.consecutive_failures()
        error_score = max(0.0, 1.0 - error_rate) * consecutive_factor
        # 流量驱动的近期错误抑制（渠道级 + 账号级）：高 QPS 的 key 对最近报错的渠道/账号更保守
        volume_error_factor = cls._volume_error_factor(volume_factor, error_rate, cls._channel_error_rate(provider_name))
        # Balance score (0-1)
        if account_client.balance is not None and account_client.balance_threshold > 0:
            balance_score = min(1.0, max(0.0, account_client.balance / max(account_client.balance_threshold, 1)))
        else:
            balance_score = 1.0
        # Priority score (0-1), higher priority = higher score
        priority_score = 1.0 / (1 + max(0, account_client.priority))
        # Weight score (0-1)
        weight_score = min(1.0, account_client.weight / 100)
        # Quota and headroom
        quota_score = cls._quota_factor(account_client, model_id, provider_name)
        headroom_score = cls._headroom_factor(account_client)
        # Frequency penalty — 用多了降权（短期滑窗），打破粘性（账号级 + 渠道级）
        frequency_score = cls._frequency_factor(account_client, provider_name)
        # 全平台避险开关：唯一全局失败计数达 2 后，按各渠道成功率拉开候选分数
        global_stress_score = cls._session_anti_affinity_factor(session_id, provider_name, account_client)
        account_key = cls._account_key(provider_name, account_client.username)
        # 复用升权：session 亲和 × upstream 亲和 → 取最大值，不再连乘（避免 1.3×1.3 叠加粘性）
        reuse_score = min(cls._reuse_affinity_factor(session_id, provider_name, account_client, upstream_model_id), cls.INTELLIGENT_AFFINITY_CAP)
        upstream_model_score = cls._upstream_model_affinity_factor(upstream_model_id, provider_name, account_key)
        short_term_success_score = cls._short_term_success_factor(account_key)
        short_term_latency_score = cls._short_term_latency_factor(account_key)
        # 绝对慢兜底：渠道 TTFT / 账号近期成功中位超阈值即重罚，补相对速度排名缺的「绝对慢」维度
        absolute_slow_score = cls._absolute_slow_factor(provider_name, account_key)
        # 连续成功让权：成功 streak 超过 grace 后指数衰减，主动把流量让给其它健康渠道
        success_rotation_score = cls._success_rotation_factor(account_client)
        # Total score — priority 提到 25%，用多了/接近限流的账号被乘性因子压低；
        # reliability_factor 把"当天长期失败率"作为乘性因子叠加（解冻 ≠ 回满血）
        total_score = (
            speed_score * 0.20 +
            error_score * 0.25 +
            balance_score * 0.15 +
            priority_score * 0.25 +
            weight_score * 0.15
        ) * quota_score * headroom_score * frequency_score * global_stress_score * reuse_score * reliability_factor * short_term_success_score * short_term_latency_score * success_rotation_score * volume_error_factor * absolute_slow_score
        return {
            "score": total_score,
            "speed_score": speed_score,
            "error_score": error_score,
            "balance_score": balance_score,
            "priority_score": priority_score,
            "weight_score": weight_score,
            "quota_score": quota_score,
            "headroom_score": headroom_score,
            "volume_error_factor": volume_error_factor,
            "frequency_score": frequency_score,
            "consecutive_failures": account_client._stats.consecutive_failures(),
            "consecutive_factor": consecutive_factor,
            "session_score": cls._session_factor(session_id, provider_name, account_client),
            "session_anti_affinity_score": global_stress_score,
            "global_stress_score": global_stress_score,
            "reuse_score": reuse_score,
            "reliability_factor": reliability_factor,
            "upstream_model_score": upstream_model_score,
            "short_term_success_score": short_term_success_score,
            "short_term_latency_score": short_term_latency_score,
            "absolute_slow_score": absolute_slow_score,
            "success_rotation_score": success_rotation_score,
            "frozen": False,
        }

    @classmethod
    def _fast_intelligent_score(cls, provider_name: str, account_client: AccountClient, model_id: str, session_id: str | None = None, reliability_factor: float = 1.0, ttft_min: float | None = None, upstream_model_id: str | None = None, volume_factor: float = 0.0) -> dict:
        """快速智能评分：速度 × 可靠性 乘性主导（快但 flaky 会被可靠性直接压下去），辅以少量加性微调。

        reliability_factor / ttft_min 由异步选择器传入；缺省退化为旧行为供同步单测直接调用。"""
        # Check frozen
        if account_client.is_frozen:
            return {"score": 0, "frozen": True}
        # Check balance
        if account_client.balance is not None and account_client.balance_threshold > 0:
            if account_client.balance < account_client.balance_threshold:
                return {"score": 0, "frozen": True, "balance_low": True}
        # Speed score (0-1) — 相对排名（有 ttft_min 时）
        speed_score = cls._speed_score(provider_name, ttft_min, cls.SPEED_NO_DATA_BASELINE)
        # Error score (0-1) — 账号级即时错误率（动态窗口）
        error_rate = cls._account_error_rate(account_client)
        # 连续失败指数降权：速度优先策略对失败更不宽容，衰减更激进
        consecutive_factor = cls.CONSECUTIVE_DECAY_FAST ** account_client._stats.consecutive_failures()
        error_score = max(0.0, 1.0 - error_rate) * consecutive_factor
        # 流量驱动的近期错误抑制（渠道级 + 账号级）
        volume_error_factor = cls._volume_error_factor(volume_factor, error_rate, cls._channel_error_rate(provider_name))
        # Balance score (0-1)
        if account_client.balance is not None and account_client.balance_threshold > 0:
            balance_score = min(1.0, max(0.0, account_client.balance / max(account_client.balance_threshold, 1)))
        else:
            balance_score = 1.0
        # Priority score (0-1)
        priority_score = 1.0 / (1 + max(0, account_client.priority))
        # Weight score (0-1)
        weight_score = min(1.0, account_client.weight / 100)
        # Quota and headroom
        quota_score = cls._quota_factor(account_client, model_id, provider_name)
        headroom_score = cls._headroom_factor(account_client)
        # Frequency penalty — 用多了降权（短期滑窗），打破粘性（账号级 + 渠道级）
        frequency_score = cls._frequency_factor(account_client, provider_name)
        # 全平台避险开关：唯一全局失败计数达 2 后，按各渠道成功率拉开候选分数
        global_stress_score = cls._session_anti_affinity_factor(session_id, provider_name, account_client)
        account_key = cls._account_key(provider_name, account_client.username)
        reuse_score = min(cls._reuse_affinity_factor(session_id, provider_name, account_client, upstream_model_id), cls.FAST_AFFINITY_CAP)
        upstream_model_score = cls._upstream_model_affinity_factor(upstream_model_id, provider_name, account_key)
        short_term_success_score = cls._short_term_success_factor(account_key)
        short_term_latency_score = cls._short_term_latency_factor(account_key)
        # 绝对慢兜底：渠道 TTFT / 账号近期成功中位超阈值即重罚，补相对速度排名缺的「绝对慢」维度
        absolute_slow_score = cls._absolute_slow_factor(provider_name, account_key)
        # 连续成功让权：成功 streak 超过 grace 后指数衰减，主动把流量让给其它健康渠道
        success_rotation_score = cls._success_rotation_factor(account_client)
        # Total score：速度 × 可靠性 乘性主导（快但 flaky 不再碾压稳定渠道）；
        # 其余维度降为小权重加性微调，避免完全忽略 priority/balance。
        base = (
            error_score * 0.10 +
            balance_score * 0.10 +
            priority_score * 0.08 +
            weight_score * 0.05
        )
        total_score = (speed_score * 0.67 + base) * reliability_factor * quota_score * headroom_score * frequency_score * global_stress_score * reuse_score * short_term_success_score * short_term_latency_score * success_rotation_score * volume_error_factor * absolute_slow_score
        return {
            "score": total_score,
            "speed_score": speed_score,
            "error_score": error_score,
            "balance_score": balance_score,
            "priority_score": priority_score,
            "weight_score": weight_score,
            "quota_score": quota_score,
            "headroom_score": headroom_score,
            "volume_error_factor": volume_error_factor,
            "frequency_score": frequency_score,
            "consecutive_failures": account_client._stats.consecutive_failures(),
            "consecutive_factor": consecutive_factor,
            "session_score": cls._session_factor(session_id, provider_name, account_client),
            "session_anti_affinity_score": global_stress_score,
            "global_stress_score": global_stress_score,
            "reuse_score": reuse_score,
            "reliability_factor": reliability_factor,
            "upstream_model_score": upstream_model_score,
            "short_term_success_score": short_term_success_score,
            "short_term_latency_score": short_term_latency_score,
            "absolute_slow_score": absolute_slow_score,
            "success_rotation_score": success_rotation_score,
            "frozen": False,
        }

    @classmethod
    def _close_score_random_pick(cls, candidates: list[dict]) -> dict:
        """评分相近时（差距 5% 内）随机选择。"""
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        # Sort by score descending
        sorted_candidates = sorted(candidates, key=lambda c: c["route_info"]["score"], reverse=True)
        top_score = sorted_candidates[0]["route_info"]["score"]
        # Find candidates within 5% of top score
        threshold = top_score * 0.95
        close_candidates = [c for c in sorted_candidates if c["route_info"]["score"] >= threshold]
        if len(close_candidates) > 1:
            return random.choice(close_candidates)
        return sorted_candidates[0]

    @classmethod
    async def _candidate_models(
        cls,
        model_id: str,
        route: dict | None = None,
        *,
        is_group: bool | None = None,
        snapshot=None,
    ) -> list[str]:
        snapshot = snapshot or model_catalog.current_snapshot()
        if is_group is None:
            is_group = await _timed_routing_call(
                "model_group_check_ms", _catalog_call(config.Config.is_model_group, model_id, snapshot=snapshot)
            )
        if is_group:
            group_models = await _timed_routing_call(
                "candidate_group_members_ms", _catalog_call(config.Config.get_model_group_models, model_id, snapshot=snapshot)
            )
            candidates = []
            for member_model in group_models:
                has_metadata = await _timed_routing_call(
                    "candidate_metadata_ms", _catalog_call(has_explicit_metadata, member_model, snapshot=snapshot)
                )
                if has_metadata and cls.has_model_route(member_model):
                    candidates.append(member_model)
            return candidates
        return [model_id]

    @classmethod
    def _pick_start_index(cls, strategy: str, member_count: int) -> int:
        if member_count <= 0:
            return 0
        if strategy == "random_member":
            return random.randrange(member_count)
        return 0

    @classmethod
    def _route_entries(cls, route: dict | None) -> list[dict]:
        if not route:
            return []
        entries = route.get("entries")
        if isinstance(entries, list) and entries:
            return [
                {
                    "provider": (entry.get("provider") or "").strip(),
                    "accounts": cls._entry_accounts(entry),
                }
                for entry in entries
                if isinstance(entry, dict) and (entry.get("provider") or "").strip()
            ]
        provider = (route.get("provider") or "").strip()
        if not provider:
            return []
        return [{"provider": provider, "accounts": cls._entry_accounts(route)}]

    @staticmethod
    def _entry_accounts(entry: dict) -> list[str]:
        """解析专用线路 entry 允许的账号：兼容老式单数 account/username 和新式 accounts 数组。

        accounts 为空表示该 provider 下所有账号都允许（兼容旧配置）。
        """
        accounts = entry.get("accounts")
        if isinstance(accounts, str):
            accounts = [accounts]
        if not isinstance(accounts, (list, tuple)):
            accounts = []
        names: list[str] = []
        for name in accounts:
            name = (str(name) or "").strip()
            if name and name not in names:
                names.append(name)
        single = (entry.get("account") or entry.get("username") or "").strip()
        if single and single not in names:
            names.append(single)
        return names

    @classmethod
    def _operation_spec(cls, operation: str | None) -> dict | None:
        if not operation or operation == "chat":
            return None
        return _MEDIA_OPERATION_CONFIG.get(operation)

    @classmethod
    def _provider_supports_operation(cls, provider: BaseProvider, provider_name: str, operation: str | None) -> bool:
        spec = cls._operation_spec(operation)
        if not spec:
            return True
        method_name = spec["method"]
        provider_method = getattr(type(provider), method_name, None)
        base_method = getattr(BaseProvider, method_name, None)
        return provider_method is not None and provider_method is not base_method

    @staticmethod
    def _metadata_allows_operation(metadata: dict | None, operation: str | None) -> bool:
        if not operation or operation == "chat":
            return True
        spec = _MEDIA_OPERATION_CONFIG.get(operation)
        if not spec:
            return True
        metadata = metadata or {}
        output_modalities = metadata.get("output_modalities")
        if isinstance(output_modalities, list) and output_modalities:
            return spec["modality"] in output_modalities
        return True

    @classmethod
    async def _route_supports_operation(cls, model_route: dict, routed_model: str, operation: str | None, snapshot=None) -> bool:
        spec = cls._operation_spec(operation)
        if not spec:
            return True
        extra = model_route.get("extra_config") if isinstance(model_route.get("extra_config"), dict) else {}
        modalities = extra.get("output_modalities") or extra.get("modalities")
        if isinstance(modalities, list) and modalities and spec["modality"] not in modalities:
            return False
        metadata, is_default_only = await _catalog_call(
            get_model_metadata, routed_model, snapshot=snapshot or model_catalog.current_snapshot()
        )
        return True if is_default_only else cls._metadata_allows_operation(metadata, operation)

    @classmethod
    def _format_skip_reasons(cls, reasons: dict[str, int], limit: int = 6) -> str:
        labels = {
            "no_model_route": "模型未配置 provider_models 路由",
            "entry_provider_missing": "专用线路 provider 不在模型路由中",
            "model_tpm_limited": "模型达到 TPM 限制",
            "operation_mismatch": "output_modalities 不匹配当前媒体操作",
            "group_provider_filtered": "模型组 provider 白/黑名单过滤",
            "api_key_provider_filtered": "API Key provider 白/黑名单过滤",
            "provider_quota_limited": "provider 模型配额不足",
            "provider_disabled": "provider 已禁用（仅跳过发送消息）",
            "provider_method_missing": "provider 未实现媒体生成方法",
            "system_model_unsupported": "provider 不支持该模型",
            "entry_account_missing": "专用线路账号不匹配",
            "account_whitelist_missing": "账号不在白名单",
            "excluded_account": "账号已在重试中排除",
            "account_disabled": "账号已禁用（仅跳过发送消息）",
            "account_frozen": "账号被持久冻结",
            "account_cooldown": "账号处于冷却期",
            "account_model_cooldown": "账号的当前模型处于冷却期",
            "channel_disabled": "渠道已禁用",
            "provider_not_initialized": "账号尚未完成初始化",
            "provider_quota_cooldown": "上游配额处于冷却期",
            "provider_daily_quota_exhausted": "上游每日配额耗尽",
            "provider_quota_exhausted": "上游配额耗尽",
            "provider_message_quota_exceeded": "消息级上游配额不足",
            "concurrent_limit": "账号并发已满",
            "model_frozen": "模型达到 TPM 冻结",
            "account_model_frozen": "账号的当前模型被冻结",
            "redis_uncertain": "Redis 预占结果不确定",
            "reservation_denied": "账号预占失败",
            "selection_returned_none": "选择策略未返回候选",
            "selection_deadline": "选择超时",
            "selection_exhausted": "选择耗尽",
            "model_missing": "模型缺失",
        }
        items = [(key, count) for key, count in reasons.items() if count > 0]
        if not items:
            return ""
        items.sort(key=lambda item: item[1], reverse=True)
        return "；".join(f"{labels.get(key, key)} x{count}" for key, count in items[:limit])

    @classmethod
    def _media_unavailable_detail(cls, model_id: str, operation: str | None, reasons: dict[str, int], prefix: str | None = None) -> str:
        if not operation or operation == "chat":
            base = prefix or f"模型 '{model_id}' 所有账号不可用"
            reason_text = cls._format_skip_reasons(reasons)
            return f"{base}：{reason_text}" if reason_text else base
        spec = cls._operation_spec(operation) or {}
        modality = spec.get("modality") or operation
        reason_text = cls._format_skip_reasons(reasons)
        base = prefix or f"模型 '{model_id}' 没有可用于 {operation} 的账号"
        detail = f"{base}：已按 output_modalities={modality} 筛选"
        if reason_text:
            detail += f"；{reason_text}"
        return detail

    @classmethod
    def _raise_no_available(cls, model_id: str, operation: str | None, reasons: dict[str, int] | None, *, prefix: str | None = None, code_override: str | None = None, detail_override: str | None = None) -> None:
        """统一构造 NoAvailableAccountError：按主因映射业务 code（供终端 429 归一 + Retry-After）。

        - 统一 429（用户要求不做 503）；redis_uncertain / provider_not_initialized 经
          _dominant_reason_code 映射为 service_busy code 以区分。
        - TPM 全部受限的特殊场景用 code_override="rate_limit_exceeded" + detail_override 覆盖。
        """
        code, reasons_snapshot = _dominant_reason_code(reasons)
        if code_override:
            code = code_override
        detail = detail_override if detail_override is not None else cls._media_unavailable_detail(model_id, operation, reasons_snapshot, prefix)
        raise NoAvailableAccountError(status_code=429, detail=detail, code=code, reasons=reasons_snapshot)


    @classmethod
    async def pick_operation_compatible_route(cls, model_id: str, routes: list[dict], operation: str | None) -> tuple[dict | None, list[str]]:
        if not routes or not cls._operation_spec(operation):
            return (routes[0] if routes else None), []
        diagnostics = []
        for route in routes:
            entries = cls._route_entries(route)
            if not entries:
                diagnostics.append(f"route {route.get('id') or '*'} 无有效 provider entry")
                continue
            route_reasons = []
            for entry in entries:
                provider = entry.get("provider")
                matched_model_routes = [r for r in cls.get_model_routes(model_id) if r.get("provider") == provider]
                if not matched_model_routes:
                    route_reasons.append(f"{provider}: 模型未配置到该 provider")
                    continue
                operation_routes = [r for r in matched_model_routes if await cls._route_supports_operation(r, model_id, operation)]
                if not operation_routes:
                    route_reasons.append(f"{provider}: output_modalities 不匹配 {operation}")
                    continue
                pool = cls._provider_pools.get(provider)
                if not pool or not pool.clients:
                    route_reasons.append(f"{provider}: provider 未启用或无账号")
                    continue
                if not any(cls._provider_supports_operation(c.provider, provider, operation) for c in pool.clients):
                    method = (cls._operation_spec(operation) or {}).get("method") or operation
                    route_reasons.append(f"{provider}: provider 未实现 {method}")
                    continue
                return route, diagnostics
            diagnostics.append(f"route {route.get('id') or '*'} 跳过：" + "；".join(route_reasons))
        return None, diagnostics

    # ── 候选收集（提取为独立方法，所有策略共用）──────────────────────

    @classmethod
    async def _collect_candidates(
        cls,
        model_id: str,
        candidate_models: list[str],
        exclude_accounts: list,
        messages: list[dict] | None,
        operation: str | None,
        group_whitelist: set,
        group_blacklist: set,
        key_whitelist: set,
        key_blacklist: set,
        route: dict | None = None,
        restrict_to_entries: list[dict] | None = None,
        request_protocol: str | None = None,
        account_whitelist: set[str] | None = None,
        is_test: bool = False,
        is_probe: bool = False,
        snapshot=None,
        input_token_estimate: int = 0,
    ) -> tuple[list[dict], list[str], dict[str, int]]:
        """收集所有可用候选（provider, account）。

        Args:
            restrict_to_entries: 限制只收集这些 provider/account 的候选（用于专用线路和模型组+专用线路交互）
            account_whitelist: 账号白名单（测试/探测专用），非空时只收集这些账号的候选。
            is_test: 手动测试模式，为真时跳过渠道 enabled / 账号 disabled / 冻结 / 冷却 / RPM / TPM 状态过滤，
                     使冷却中 / 禁用的账号也能被选中做测试。
            is_probe: 定时检测模式，与 is_test 同样绕过冻结/冷却过滤（探测的目标正是异常账号），
                     但**尊重**账号 disabled / 渠道 enabled——禁用状态不进入检测周期。
                     二者在失败收口处区分：is_test 不冻结，is_probe 走正常冻结逻辑。
            input_token_estimate: 本次请求预估输入 token（不含输出）。>0 时按渠道模型
                     ``extra_config.max_context_tokens`` 过滤：预估输入超过该渠道模型声明的
                     输入窗口上限则跳过该候选（该渠道装不下，发过去必然 400）。
        """
        now = time.time()
        route_type = "route" if route else "smart"
        candidates = []
        tpm_limited_models = []
        skip_reasons: dict[str, int] = defaultdict(int)

        # 专用线路 provider/account 集合（用于过滤）。账号集合为空表示该 provider 下所有账号可用。
        route_provider_set = None
        route_account_map: dict[str, set[str]] = {}
        if restrict_to_entries:
            route_provider_set = set()
            for entry in restrict_to_entries:
                provider = (entry.get("provider") or "").strip()
                if not provider:
                    continue
                route_provider_set.add(provider)
                accounts = entry.get("accounts")
                if isinstance(accounts, str):
                    accounts = [accounts]
                if not isinstance(accounts, (list, tuple, set)):
                    accounts = []
                account_set = route_account_map.setdefault(provider, set())
                for account in accounts:
                    account = (str(account) or "").strip()
                    if account:
                        account_set.add(account)
                account = (entry.get("account") or entry.get("username") or "").strip()
                if account:
                    account_set.add(account)

        for routed_model in candidate_models:
            # 扫各渠道 Channel.models 现找（内存，零查库）；替代旧全局 _model_routes 索引。
            model_candidates = list(cls.iter_model_candidates(routed_model))
            if not model_candidates:
                skip_reasons["no_model_route"] += 1
                continue
            if not await _timed_routing_call(
                "candidate_tpm_check_ms", cls._model_tpm_available(routed_model)
            ):
                tpm_limited_models.append(routed_model)
                skip_reasons["model_tpm_limited"] += 1
                continue

            for provider_name, pool, model_route in model_candidates:
                if not await _timed_routing_call(
                    "candidate_operation_check_ms",
                    cls._route_supports_operation(model_route, routed_model, operation, snapshot=snapshot),
                ):
                    skip_reasons["operation_mismatch"] += 1
                    continue
                upstream_model_id = model_route.get("upstream_model_id") or routed_model

                # 输入窗口过滤：渠道模型在 extra_config 里声明的 max_context_tokens 是该
                # 渠道模型的输入窗口上限。预估输入 token 超过它则跳过该候选（发过去必然
                # context_too_large）。两值任一 <=0 表示「不限制/未知」，不过滤保持原行为。
                if input_token_estimate > 0:
                    extra = model_route.get("extra_config") or {}
                    try:
                        ctx_limit = int(extra.get("max_context_tokens") or 0)
                    except (TypeError, ValueError):
                        ctx_limit = 0
                    if ctx_limit > 0 and input_token_estimate > ctx_limit:
                        skip_reasons["context_window_exceeded"] += 1
                        continue

                # 专用线路过滤：只取线路内的 provider
                if route_provider_set and provider_name not in route_provider_set:
                    skip_reasons["entry_provider_missing"] += 1
                    continue
                # 专用线路账号白名单：accounts 非空表示只允许这些账号；为空表示整 provider 全量账号（兼容旧配置）
                route_allowed_accounts = route_account_map.get(provider_name) if route_provider_set is not None else None
                channel = getattr(pool, "channel", None)
                provider_tags = getattr(channel, "tags", frozenset()) if channel is not None else frozenset()
                # 模型组渠道标签白名单/黑名单；渠道测试/探测跳过组筛选，只保留下面的精确渠道定向。
                if not (is_test or is_probe):
                    if group_whitelist and not provider_tags.intersection(group_whitelist):
                        skip_reasons["group_provider_filtered"] += 1
                        continue
                    if group_blacklist and provider_tags.intersection(group_blacklist):
                        skip_reasons["group_provider_filtered"] += 1
                        continue
                # 正常请求按 API Key 渠道标签过滤；测试/探测入口传入的是精确渠道名。
                if is_test or is_probe:
                    if key_whitelist and provider_name not in key_whitelist:
                        skip_reasons["api_key_provider_filtered"] += 1
                        continue
                    if key_blacklist and provider_name in key_blacklist:
                        skip_reasons["api_key_provider_filtered"] += 1
                        continue
                else:
                    if key_whitelist and not provider_tags.intersection(key_whitelist):
                        skip_reasons["api_key_provider_filtered"] += 1
                        continue
                    if key_blacklist and provider_tags.intersection(key_blacklist):
                        skip_reasons["api_key_provider_filtered"] += 1
                        continue
                if not cls._model_provider_quota_available(routed_model, provider_name):
                    skip_reasons["provider_quota_limited"] += 1
                    continue
                # pool 已由 iter_model_candidates 给出
                # 渠道启用开关软跳过：禁用渠道不参与发送消息，但账号仍可被测试/拉模型列表。
                # 探测（is_probe）不旁路：禁用渠道不进入检测周期（scheduled_test_loop 已前置过滤，此处双保险）。
                channel = getattr(pool, "channel", None)
                if not is_test and channel is not None and not channel.enabled:
                    skip_reasons["provider_disabled"] += 1
                    continue

                # 协议候选按渠道取一次，池内账号复用（媒体操作不走对话协议行）。
                operation_kind = {
                    "image_generation": "image",
                    "video_generation": "video",
                    "tts_generation": "speech",
                }.get(operation or "", "chat")
                channel_protocol_candidates = None
                if operation_kind == "chat" and channel is not None:
                    channel_protocol_candidates = channel.get_chat_protocol_candidates(routed_model, request_protocol)
                if not channel_protocol_candidates:
                    channel_protocol_candidates = [None]

                for account_client in pool.clients:
                    if not cls._provider_supports_operation(account_client.provider, provider_name, operation):
                        skip_reasons["provider_method_missing"] += 1
                        continue
                    supports_system_model = getattr(account_client.provider, "supports_system_model", None)
                    if supports_system_model and not supports_system_model(routed_model):
                        skip_reasons["system_model_unsupported"] += 1
                        continue
                    # 专用线路账号过滤：accounts 白名单非空时只允许指定账号，空集合表示全量（兼容旧配置）
                    if route_allowed_accounts is not None and account_client.username not in route_allowed_accounts:
                        skip_reasons["entry_account_missing"] += 1
                        continue
                    # 测试账号白名单：非空时只允许指定账号（测试专用）
                    if account_whitelist and account_client.username not in account_whitelist:
                        skip_reasons["account_whitelist_missing"] += 1
                        continue
                    if (provider_name, account_client.username) in exclude_accounts:
                        skip_reasons["excluded_account"] += 1
                        continue
                    # 选择阶段只看内存布尔：禁用 + 冻结 + 冷却。rpm/tpm 改"触线即冻结"——
                    # 计数在请求收尾写 Redis 并判触线，越线则给账号打 cooldown，冻结即被此处过滤。
                    # 选账号阶段不再逐账号读 rpm/tpm 计数（零 Redis 查询）；并发在 reserve 阶段原子占用。
                    # 禁用：仅手动测试（is_test）可选中禁用账号；探测与正常请求都跳过
                    # （探测侧 scheduled_test_loop 组装白名单时已排除禁用账号，此处双保险）。
                    if not is_test:
                        if account_client.disabled:
                            skip_reasons["account_disabled"] += 1
                            continue
                    # 冻结/冷却：测试与探测都绕过——探测的目标正是异常（冻结/冷却中）账号；
                    # 正常请求跳过。is_frozen（含永久冻结状态位）也不拦探测：探测成功会清除
                    # is_frozen，使永久冻结账号恢复；disabled 不走该路径。
                    if not is_test and not is_probe:
                        if account_client.is_frozen:
                            # 账号级冻结：临时冷却 vs 永久冻结用不同 skip_reason。
                            if account_client.cooldown_remaining(now) > 0:
                                skip_reasons["account_cooldown"] += 1
                            else:
                                skip_reasons["account_frozen"] += 1
                            continue
                        # 模型级冻结：该账号该模型是否被冻结
                        if account_client.is_model_frozen(routed_model):
                            skip_reasons["account_model_cooldown"] += 1
                            continue

                    for endpoint_config in channel_protocol_candidates:
                        endpoint_protocol = None
                        if isinstance(endpoint_config, dict):
                            endpoint_protocol = endpoint_config.get("protocol")
                        endpoint_protocol = endpoint_protocol or getattr(account_client.provider, "protocol", None) or getattr(account_client.provider, "channel_protocol", None) or "openai"
                        endpoint_models = endpoint_config.get("models") or [] if isinstance(endpoint_config, dict) else []
                        endpoint_model_bound = bool(endpoint_models and routed_model in endpoint_models)
                        route_info = {
                            "provider": provider_name,
                            "account": account_client.username,
                            "requested_model": model_id,
                            "routed_model": routed_model,
                            "public_model_id": routed_model,
                            "upstream_model_id": upstream_model_id,
                            "route_type": route_type,
                            "operation": operation or "chat",
                            "catalog_generation": (snapshot or model_catalog.current_snapshot()).generation,
                            "protocol": str(endpoint_protocol).lower(),
                            "same_protocol": bool(request_protocol and str(endpoint_protocol).lower() == str(request_protocol).lower()),
                            "endpoint_model_bound": endpoint_model_bound,
                            "score": 1.0,
                        }
                        if isinstance(endpoint_config, dict):
                            route_info["endpoint_config"] = endpoint_config
                            route_info["endpoint_config_id"] = endpoint_config.get("id")
                            route_info["endpoint_path"] = endpoint_config.get("path")
                            if endpoint_config.get("client_preset"):
                                route_info["client_preset"] = endpoint_config.get("client_preset")
                        if route:
                            route_info["route_id"] = route.get("id")
                        if model_route.get("client_preset"):
                            route_info["client_preset"] = model_route.get("client_preset")
                        if model_route.get("enable_1m_context"):
                            route_info["enable_1m_context"] = True
                        if model_route.get("extra_config"):
                            route_info["extra_config"] = model_route.get("extra_config")
                        candidates.append({
                            "provider_name": provider_name,
                            "account_client": account_client,
                            "routed_model": routed_model,
                            "upstream_model_id": upstream_model_id,
                            "route_info": route_info,
                        })
        return candidates, tpm_limited_models, dict(skip_reasons)

    @staticmethod
    def _prefer_same_protocol_candidates(candidates: list[dict], request_protocol: str | None) -> list[dict]:
        """协议行偏好：显式勾选模型的行优先，其次同协议行。

        勾选模型代表该模型必须走这条链路（管理员显式绑定），优先级高于协议直通；
        协议只在"是否绑定模型"相同的档内做次级排序。任一档为空则回退到全集。
        """
        def rank(c: dict) -> int:
            info = c.get("route_info") or {}
            bound = bool(info.get("endpoint_model_bound"))
            same = bool(info.get("same_protocol"))
            if bound:
                return 0 if same else 1
            return 2 if same else 3

        if not request_protocol and not any((c.get("route_info") or {}).get("endpoint_model_bound") for c in candidates):
            return candidates
        best = min((rank(c) for c in candidates), default=3)
        preferred = [c for c in candidates if rank(c) == best]
        return preferred or candidates

    @classmethod
    def _prefer_same_upstream_model_candidates(cls, candidates: list[dict], strategy: str | None = None) -> list[dict]:
        """全策略同上游模型亲和。
        非评分策略（sequential/random_*）不读 score，命中亲和时收缩候选集提升 prompt cache 命中率；
        intelligent/fast_intelligent 已通过 reuse_score 软升权表达同模型优先，不能硬收缩，否则连续成功让权无处切换。"""
        if strategy in {"intelligent", "fast_intelligent"}:
            return candidates
        if not candidates:
            return candidates
        now = time.time()
        best_ts = 0.0
        preferred: list[dict] = []
        for c in candidates:
            upstream_model_id = c.get("upstream_model_id") or (c.get("route_info") or {}).get("upstream_model_id")
            rec = cls._upstream_model_affinity.get(upstream_model_id)
            if not rec:
                continue
            last_ts = float(rec.get("last_success_ts") or 0)
            if now - last_ts >= cls.UPSTREAM_MODEL_AFFINITY_TTL:
                continue
            account_client = c.get("account_client")
            account_key = cls._account_key(c.get("provider_name"), getattr(account_client, "username", None))
            if rec.get("account_key") != account_key:
                continue
            if last_ts > best_ts:
                best_ts = last_ts
                preferred = [c]
            elif last_ts == best_ts:
                preferred.append(c)
        return preferred or candidates

    # ── 6 种选择策略（每种一个独立方法）──────────────────────

    @classmethod
    async def _select_sequential(cls, candidates: list[dict], messages: list[dict] | None, session_id: str | None = None) -> dict | None:
        """顺序：先避让上一轮命中的渠道（相邻两轮尽量交叉），再按优先级从小到大、同优先级按权重从大到小取第一个。"""
        if not candidates:
            return None
        candidates = cls._avoid_last_provider(candidates)
        sorted_candidates = sorted(
            candidates,
            key=lambda c: (c["account_client"].priority, -c["account_client"].weight)
        )
        return sorted_candidates[0]

    @classmethod
    async def _select_channel_random(cls, candidates: list[dict], messages: list[dict] | None, session_id: str | None = None) -> dict | None:
        """渠道随机：先避让上一轮命中的渠道，再随机一个渠道（provider），取该渠道第一个候选。"""
        if not candidates:
            return None
        candidates = cls._avoid_last_provider(candidates)
        by_provider: dict[str, list[dict]] = defaultdict(list)
        for c in candidates:
            by_provider[c["provider_name"]].append(c)
        provider_names = list(by_provider.keys())
        random.shuffle(provider_names)
        for pname in provider_names:
            group = by_provider[pname]
            group.sort(key=lambda c: (c["account_client"].priority, -c["account_client"].weight))
            return group[0]
        return None

    @classmethod
    async def _select_model_random(cls, candidates: list[dict], messages: list[dict] | None, session_id: str | None = None) -> dict | None:
        """模型随机：先随机一个模型，在该模型内走渠道随机（渠道交叉由 _select_channel_random 负责，避免双重过滤）。"""
        if not candidates:
            return None
        by_model: dict[str, list[dict]] = defaultdict(list)
        for c in candidates:
            by_model[c["routed_model"]].append(c)
        model_names = list(by_model.keys())
        random.shuffle(model_names)
        for mname in model_names:
            result = await cls._select_channel_random(by_model[mname], messages, session_id)
            if result:
                return result
        return None

    @classmethod
    async def _select_all_random(cls, candidates: list[dict], messages: list[dict] | None, session_id: str | None = None) -> dict | None:
        """全部随机：先避让上一轮命中的渠道，再随机选一个。"""
        if not candidates:
            return None
        candidates = cls._avoid_last_provider(candidates)
        return random.choice(candidates)

    @classmethod
    def _estimate_prompt_tokens(cls, messages: list[dict] | None, model_id: str = "") -> int:
        """估算 prompt token：复用主流程 tokenizer（按模型规则选 tiktoken），短请求退化为字符粗估避免开销。"""
        if not messages:
            return 0
        total_chars = sum(len(str(m.get("content", ""))) for m in messages if isinstance(m, dict))
        if total_chars < 500:
            # 短请求走快速路径，字符粗估即可
            return total_chars // 3
        total = 0
        for m in messages:
            if isinstance(m, dict):
                total += estimate_request_part_tokens(model_id, m.get("content"))
        return total

    @classmethod
    def _billing_preference(cls, provider_name: str, messages: list[dict] | None, model_id: str = "", estimated_tokens: int | None = None) -> float:
        """计费偏好系数：仅“按次（request）计费”渠道需要按请求大小调偏好。

        request 计费账号：请求越大越划算 → 偏好随 token 上升而上升（0.8 → 2.0）。
        token 计费账号（绝大多数、默认）：请求大小不改变计费性价比 → 中性 1.0，
        直接短路，不触碰 tiktoken 估算（选路热路径 O(N) 候选 × 多轮重试会放大成本）。

        estimated_tokens：调用方在请求级已算好的 prompt token（复用，避免每候选每轮重算）；
        未传时才按需惰性估算，且仅在 request 计费分支才会真正计算。
        """
        pool = cls._provider_pools.get(provider_name)
        channel = getattr(pool, "channel", None) if pool else None
        billing_mode = getattr(channel, "billing_mode", cls._provider_billing_mode.get(provider_name, "token"))
        # token 计费（默认）不依赖请求大小，直接中性，跳过 token 估算。
        if billing_mode == "token":
            return 1.0
        if not messages:
            return 1.0
        tokens = estimated_tokens if estimated_tokens is not None else cls._estimate_prompt_tokens(messages, model_id)
        # 2000 token 为交叉点；ratio 钳制在 [0, 3]
        ratio = max(0.0, min(3.0, tokens / 2000.0))
        return 0.8 + 0.4 * ratio              # 0.8 → 2.0（request 计费）

    @classmethod
    def _prompt_tokens_once(cls, candidates: list[dict], messages: list[dict] | None) -> int:
        """整轮选路只估算一次 prompt token（评分用的计费偏好惰性复用）。

        缓存在候选集共享的 route_info 之外——挂到本次 _select_* 调用的局部；
        因评分只在首轮跑（见 _scored 缓存），实际每请求只计算一次。tokenizer 规则用首个候选
        的 routed_model（billing 偏好只需量级，规则差异可忽略）。"""
        if not messages:
            return 0
        model_id = candidates[0].get("routed_model", "") if candidates else ""
        return cls._estimate_prompt_tokens(messages, model_id)

    @classmethod
    async def _select_intelligent(cls, candidates: list[dict], messages: list[dict] | None, session_id: str | None = None) -> dict | None:
        """智能选择：多维度评分（速度 25% + 错误率 25% + 余额 15% + 优先级 25% + 权重 15%）× 长期可靠性。

        评分只对“尚未评分”的候选执行一次并缓存到候选 dict；reserve 失败后 remaining 剔除该账号、
        再次进入本方法时，其余候选直接复用缓存分数（不重跑 Redis 可靠性查询 / token 估算），
        彻底消除“异常重试导致每轮 O(N) 重评分”的 O(N²) 开销。加权随机挑选仍逐轮在当前有效集上进行。"""
        if not candidates:
            return None
        unscored = [c for c in candidates if not c.get("_scored")]
        if unscored:
            reliability = await cls._candidate_reliability(unscored)
            ttft_min = cls._candidate_ttft_min(candidates)
            est_tokens = cls._prompt_tokens_once(unscored, messages)
            for c in unscored:
                rel = reliability.get((c["provider_name"], c["account_client"].username), 1.0)
                score_info = cls._intelligent_score(c["provider_name"], c["account_client"], c["routed_model"], session_id, rel, ttft_min, c.get("upstream_model_id"), c.get("volume_factor", 0.0))
                billing_factor = cls._billing_preference(c["provider_name"], messages, c["routed_model"], estimated_tokens=est_tokens)
                c["route_info"]["score"] = score_info.get("score", 0) * billing_factor
                c["_scored"] = True
        # 评分全 0（全部冻结 / 熔断冷却 / 余额不足）时不再做"恢复性探测"：选路无时间成本，
        # LRU 探测没有意义，反而会反复重试已知在报错的渠道。直接返回 None，交由组回退 / 429。
        valid = [c for c in candidates if c["route_info"]["score"] > 0]
        if not valid:
            return None
        return cls._provider_first_weighted_pick(valid)

    @classmethod
    async def _select_fast_intelligent(cls, candidates: list[dict], messages: list[dict] | None, session_id: str | None = None) -> dict | None:
        """智能快速选择：速度 × 可靠性 乘性主导（最快 + 低失败率），无速度数据给中位分。

        与 _select_intelligent 相同的“评分一次 + 缓存复用”策略：见其 docstring。"""
        if not candidates:
            return None
        unscored = [c for c in candidates if not c.get("_scored")]
        if unscored:
            reliability = await cls._candidate_reliability(unscored)
            ttft_min = cls._candidate_ttft_min(candidates)
            est_tokens = cls._prompt_tokens_once(unscored, messages)
            for c in unscored:
                rel = reliability.get((c["provider_name"], c["account_client"].username), 1.0)
                score_info = cls._fast_intelligent_score(c["provider_name"], c["account_client"], c["routed_model"], session_id, rel, ttft_min, c.get("upstream_model_id"), c.get("volume_factor", 0.0))
                billing_factor = cls._billing_preference(c["provider_name"], messages, c["routed_model"], estimated_tokens=est_tokens)
                c["route_info"]["score"] = score_info.get("score", 0) * billing_factor
                c["_scored"] = True
        # 评分全 0（全部冻结 / 熔断冷却 / 余额不足）时不再做"恢复性探测"：选路无时间成本，
        # LRU 探测没有意义，反而会反复重试已知在报错的渠道。直接返回 None，交由组回退 / 429。
        valid = [c for c in candidates if c["route_info"]["score"] > 0]
        if not valid:
            return None
        # 间歇性探索：小概率忽略速度权重，在池内所有健康渠道间均匀随机撒一次流量，
        # 防止流量锁死在 top1/top2 两个渠道上、其余已进池渠道永远拿不到机会更新自己的健康度数据。
        # （无 ttft 数据的渠道本就给中位 0.5 分、不会被饿死；这里只解决"进了池但权重太小抽不中"。）
        if random.random() < cls.FAST_EXPLORATION_RATE:
            # explore 轮不避让，保持池内均匀随机撒流量
            return cls._provider_first_weighted_pick(valid, explore=True)
        # 正常轮：避开上一轮命中的渠道，打破连续命中同一渠道的粘性。
        # _avoid_last_provider 在仅剩单渠道时回退原候选（filtered or candidates），不会饿死。
        avoided = cls._avoid_last_provider(valid)
        return cls._provider_first_weighted_pick(avoided)

    @classmethod
    async def _try_reserve(cls, candidate: dict, messages: list[dict] | None, *, is_test: bool = False, is_probe: bool = False, token_estimate: int = 0):
        """尝试占位，返回 ``(成功元组, LimitDecision)``。

        手动测试（is_test）/ 定时检测（is_probe）**跳过预占阶段**：不调
        ``reserve_with_decision`` / ``acquire_account``，因此不进入 Lua 预占脚本，
        冻结键 ``limit:cooldown:{provider}:{account}`` 不再被检查——从结构上消灭
        「测试反而把被测账号冻结」的 bug。候选直接绑定目标账号执行；失败后的冻结/
        健康度差异在执行阶段失败收口处区分（is_test 不冻结，is_probe 走正常冻结）。
        """
        account_client = candidate["account_client"]
        account_client._is_test = is_test or is_probe
        reserve_started = _routing_stage_started()
        # 测试/探测：跳过预占，直接绑定候选。不写并发/RPM/TPM 账本（配额暂不处理），
        # CandidateReservation 在无 lease 时 commit/rollback/release 均优雅降级。
        if is_test or is_probe:
            provider_name = candidate["provider_name"]
            account_client.last_route_info = candidate["route_info"]
            try:
                return (account_client.provider, provider_name, account_client), None
            finally:
                _record_routing_stage("account_reserve_ms", reserve_started)
                account_client._is_test = False
        try:
            reserve_with_decision = getattr(account_client, "reserve_with_decision", None)
            if reserve_with_decision is not None:
                result = await reserve_with_decision(candidate["routed_model"], messages)
            else:
                from limits.rules import LimitDecision, LimitLease, LimitReservation, LimitSubject
                allowed = await account_client.reserve(candidate["routed_model"], messages)
                compatibility_lease = LimitLease(LimitSubject(model=candidate["routed_model"]), "compat") if allowed else None
                result = LimitReservation(compatibility_lease, LimitDecision(bool(allowed), "" if allowed else "reservation_denied"))
            if result.allowed:
                now = time.time()
                provider_name = candidate["provider_name"]
                account_client.last_route_info = candidate["route_info"]
                account_client._stats.record_pick(now)
                cls._provider_recent_picks.setdefault(provider_name, []).append(now)
                cls._provider_pick_history.append((now, provider_name))
                cls._clean_provider_pick_history(now)
                return (account_client.provider, provider_name, account_client), result.decision
            # 预占拒绝不再逐条打日志：拒绝原因已在 [模型组不可用] 汇总行按 reason 计数聚合，
            # 逐候选细节会刷屏且对排查无增益。需要时看汇总行的 reason 分布即可。
            return None, result.decision
        finally:
            _record_routing_stage("account_reserve_ms", reserve_started)
            account_client._is_test = False

    @classmethod
    async def _select_and_reserve_candidates(
        cls,
        candidates: list[dict],
        select_fn,
        messages: list[dict] | None,
        session_id: str | None,
        reasons: dict[str, int],
        *,
        is_test: bool = False,
        is_probe: bool = False,
        token_estimate: int = 0,
    ):
        remaining = list(candidates)
        # 正常情况下每次 reserve 拒绝都会缩减 remaining；显式上限用于防御自定义策略
        # 返回集合外候选或未来改动漏删，避免高并发下形成无界 while/Redis 热循环。
        max_selection_attempts = len(remaining)
        selection_attempts = 0
        # 请求级选择时限：到点即停，避免单个 attempt 在 reserve 循环里久等拖垮整体 deadline。
        # 测试/探测模式不设时限（用例常构造大量候选、又要确定性遍历完）。
        bypass_selection = is_test or is_probe
        deadline = None if bypass_selection else time.monotonic() + cls.SELECTION_DEADLINE_MS / 1000.0
        reserve_failures = 0
        while remaining and selection_attempts < max_selection_attempts:
            if deadline is not None and time.monotonic() >= deadline:
                reasons["selection_deadline"] = reasons.get("selection_deadline", 0) + len(remaining)
                logger.warning(
                    "[路由选择超时] attempts={} remaining={} elapsed_ms>={} reasons={}",
                    selection_attempts, len(remaining), cls.SELECTION_DEADLINE_MS, dict(reasons),
                )
                return None
            selection_attempts += 1
            select_started = _routing_stage_started()
            selected = await select_fn(remaining, messages, session_id)
            _record_routing_stage("strategy_select_ms", select_started)
            if not selected:
                # 测试/探测模式：只验证账号/模型连通性，不看评分门槛。策略因冻结/冷却/
                # 余额不足把候选全打 0 分而返回 None 时，直接取当前有效集第一个强制绑定，
                # 让请求真的打到目标账号（is_test 不冻结；is_probe 走正常冻结）。
                if bypass_selection:
                    selected = remaining[0]
                else:
                    reasons["selection_returned_none"] = reasons.get("selection_returned_none", 0) + 1
                    return None
            reserved, decision = await cls._try_reserve(selected, messages, is_test=is_test, is_probe=is_probe, token_estimate=token_estimate)
            if reserved:
                return reserved
            reason = (decision.reason if decision is not None else None) or "reservation_denied"
            reasons[reason] = reasons.get(reason, 0) + 1
            # reserve 失败后、下一次 select 前轻量退避，削减高并发下对 Redis 的热循环压力。
            # 指数增长带上限；测试/探测模式跳过以免拖慢用例。
            if not bypass_selection:
                backoff_ms = min(
                    cls.SELECTION_BACKOFF_BASE_MS * (2 ** reserve_failures),
                    cls.SELECTION_BACKOFF_CAP_MS,
                )
                reserve_failures += 1
                await asyncio.sleep(backoff_ms / 1000.0)
            provider_name = selected.get("provider_name")
            username = getattr(selected.get("account_client"), "username", None)
            routed_model = selected.get("routed_model")
            if reason in {"account_model_cooldown", "account_model_frozen"}:
                remaining = [
                    c for c in remaining
                    if not (
                        c.get("provider_name") == provider_name
                        and getattr(c.get("account_client"), "username", None) == username
                        and c.get("routed_model") == routed_model
                    )
                ]
            else:
                remaining = [
                    c for c in remaining
                    if not (
                        c.get("provider_name") == provider_name
                        and getattr(c.get("account_client"), "username", None) == username
                    )
                ]
        if remaining:
            reasons["selection_exhausted"] = reasons.get("selection_exhausted", 0) + len(remaining)
            logger.warning(
                "[路由选择耗尽] attempts={} remaining={} reasons={}",
                selection_attempts, len(remaining), dict(reasons),
            )
        return None

    @classmethod
    async def _prepare_and_reserve(
        cls,
        candidates: list[dict],
        *,
        select_fn,
        request_protocol: str | None,
        strategy: str | None,
        volume_factor: float,
        messages: list[dict] | None,
        session_id: str | None,
        reasons: dict[str, int],
        candidate_started,
        is_test: bool = False,
        is_probe: bool = False,
    ):
        """候选公共尾部：协议偏好 → 上游模型偏好 → 打 volume_factor → 记阶段 → 选择预占。

        原 _get_available_client_with_provider 的 4 个分支（专用线路/model_random/
        模型组/普通模型）尾部逻辑完全一致，此处收敛成一处，杜绝复制粘贴漂移。
        命中返回成功元组，否则返回 None（各分支据此决定继续或抛无可用）。
        """
        if not candidates:
            return None
        candidates = cls._prefer_same_protocol_candidates(candidates, request_protocol)
        candidates = cls._prefer_same_upstream_model_candidates(candidates, strategy)
        for c in candidates:
            c["volume_factor"] = volume_factor
        _record_routing_stage_once("candidate_collect_ms", candidate_started)
        return await cls._select_and_reserve_candidates(
            candidates, select_fn, messages, session_id, reasons, is_test=is_test, is_probe=is_probe
        )

    # ── 主选择方法（重构后）──────────────────────

    @classmethod
    async def _real_model_filter_nodes(
        cls, model_id: str, is_group: bool, is_test: bool, is_probe: bool, snapshot=None,
    ) -> list[tuple[set, set]] | None:
        """真实模型多方案过滤节点：[(渠道白, 渠道黑), ...] 按降级顺序。

        激活方案（顶层投影）打头，随后是标记 is_backup 的方案按数组顺序。返回 None 表示
        不走方案链（模型组请求 / 测试探测 / 非 real 行 / 无多套方案），调用方退回单节点
        ——与原单次过滤行为完全一致。方案身份是 id；models 不参与（真实模型的候选模型
        恒为自身，方案只提供渠道档位）。
        """
        if is_group or is_test or is_probe:
            return None
        snap = snapshot or model_catalog.current_snapshot()
        groups = getattr(snap, "groups", None)
        if not isinstance(groups, Mapping):
            return None
        row = groups.get(model_id)
        if not isinstance(row, Mapping) or str(row.get("kind") or "custom") != "real":
            return None
        schemes = row.get("schemes")
        if not isinstance(schemes, (list, tuple)) or len(schemes) <= 1:
            return None

        def _filters(scheme: Mapping) -> tuple[set, set]:
            whitelist = {p for p in (scheme.get("provider_whitelist") or []) if isinstance(p, str) and p}
            blacklist = {p for p in (scheme.get("provider_blacklist") or []) if isinstance(p, str) and p}
            return whitelist, blacklist

        active_id = str(row.get("active_scheme") or "").strip()
        active = next((s for s in schemes if isinstance(s, Mapping) and str(s.get("id") or "").strip() == active_id), None)
        if active is None:
            active = next((s for s in schemes if isinstance(s, Mapping)), None)
        if active is None:
            return None
        nodes = [_filters(active)]
        for scheme in schemes:
            if scheme is active or not isinstance(scheme, Mapping):
                continue
            if not scheme.get("is_backup"):
                continue
            nodes.append(_filters(scheme))
        return nodes if len(nodes) > 1 else None

    @classmethod
    async def _get_available_client_with_provider(
        cls,
        model_id: str,
        exclude_accounts: list[tuple[str, str]] = None,
        messages: list[dict] | None = None,
        route: dict | None = None,
        api_key: str | None = None,
        operation: str | None = None,
        session_id: str | None = None,
        request_protocol: str | None = None,
        provider_whitelist: set[str] | None = None,
        provider_blacklist: set[str] | None = None,
        account_whitelist: set[str] | None = None,
        is_test: bool = False,
        is_probe: bool = False,
        snapshot=None,
        input_token_estimate: int = 0,
    ) -> tuple[BaseProvider, str, AccountClient]:
        """获取模型的可用客户端及提供商名称。

        渠道白/黑名单由调用方显式传入（``provider_whitelist`` / ``provider_blacklist``），
        选账号层不再从 api_key 内部推导。``account_whitelist`` / ``is_test`` / ``is_probe``
        为测试/探测专用。
        """
        snapshot = snapshot or model_catalog.current_snapshot()
        exclude_accounts = exclude_accounts or []
        provider_whitelist = provider_whitelist or set()
        provider_blacklist = provider_blacklist or set()
        candidate_started = _routing_stage_started()
        is_group = await _timed_routing_call(
            "model_group_check_ms", _catalog_call(config.Config.is_model_group, model_id, snapshot=snapshot)
        )

        # 1. 选择策略已收敛到 API Key（不再挂在组/专线上）。
        strategy = await _timed_routing_call(
            "strategy_config_ms", config.Config.get_api_key_strategy(api_key)
        )

        # 2. 解析候选模型
        candidate_models = await _catalog_method_call(
            cls._candidate_models, model_id, route, is_group=is_group, snapshot=snapshot
        )
        if not candidate_models:
            raise HTTPException(status_code=400, detail=f"模型 `{model_id}` 不存在或模型组为空")

        # 3. 按请求模型的渠道标签白/黑名单。自定义模型读组配置；直连真实模型读
        # kind='real' 行自己的过滤。两者共享候选过滤管线，但目录索引彼此隔离。
        group_whitelist, group_blacklist = (set(), set())
        filter_getter = (
            config.Config.get_model_group_provider_filter
            if is_group
            else config.Config.get_real_model_provider_filter
        )
        group_whitelist, group_blacklist = await _timed_routing_call(
            "provider_filter_ms", _catalog_call(filter_getter, model_id, snapshot=snapshot)
        )
        # 真实模型多方案：激活方案打头，is_backup 方案按数组顺序降级（渠道档位兜底）。
        # 单方案/无方案/模型组/测试探测 → None，退回单节点，行为与原实现完全一致。
        filter_nodes = await cls._real_model_filter_nodes(model_id, is_group, is_test, is_probe, snapshot)
        if filter_nodes is None:
            filter_nodes = [(group_whitelist, group_blacklist)]
        # 渠道白/黑名单由调用方显式传入（无内部回退推导）。
        key_whitelist, key_blacklist = provider_whitelist, provider_blacklist

        # 策略方法映射
        strategy_methods = {
            "sequential": cls._select_sequential,
            "random_member": cls._select_channel_random,
            "model_random": cls._select_model_random,
            "random_all": cls._select_all_random,
            "intelligent": cls._select_intelligent,
            "fast_intelligent": cls._select_fast_intelligent,
        }
        select_fn = strategy_methods.get(strategy, cls._select_sequential)
        # 仅智能策略读取当前 API Key 的 RPM 用量；其他策略不增加 Redis 查询。
        volume_factor = await _timed_routing_call(
            "candidate_volume_factor_ms", cls._api_key_volume_factor(api_key)
        ) if strategy in {"intelligent", "fast_intelligent"} else 0.0

        # 4. 根据模型类型收集候选并选择
        route_entries = cls._route_entries(route)

        if route:
            # ── 专用线路 ──
            merged_reasons: dict[str, int] = defaultdict(int)
            tpm_limited_all = []
            for entry in route_entries:
                candidates, tpm_limited, reasons = await _timed_routing_call(
                    "candidate_scan_ms",
                    cls._collect_candidates(
                        model_id, candidate_models, exclude_accounts, messages, operation,
                        group_whitelist, group_blacklist, key_whitelist, key_blacklist,
                        route=route, restrict_to_entries=[entry], request_protocol=request_protocol,
                        account_whitelist=account_whitelist, is_test=is_test, is_probe=is_probe, snapshot=snapshot,
                        input_token_estimate=input_token_estimate,
                    ),
                )
                for k, v in reasons.items():
                    merged_reasons[k] += v
                tpm_limited_all.extend(tpm_limited)
                if not candidates:
                    continue
                reserved = await cls._prepare_and_reserve(
                    candidates, select_fn=select_fn, request_protocol=request_protocol,
                    strategy=strategy, volume_factor=volume_factor, messages=messages,
                    session_id=session_id, reasons=merged_reasons,
                    candidate_started=candidate_started, is_test=is_test, is_probe=is_probe,
                )
                if reserved:
                    return reserved
            def _entry_label(e: dict) -> str:
                accounts = e.get("accounts") or []
                if isinstance(accounts, str):
                    accounts = [accounts]
                accounts = [a for a in (accounts or []) if str(a).strip()]
                acct = ",".join(accounts) if accounts else "*"
                return f"{e.get('provider')}/{acct}"
            entries_desc = ", ".join(_entry_label(e) for e in route_entries) or "*"
            prefix = f"模型 '{model_id}' 专用线路不可用: {entries_desc}"
            cls._raise_no_available(model_id, operation, dict(merged_reasons), prefix=prefix)

        if is_group and strategy == "model_random":
            # ── 模型随机：先随机模型，再在该模型内走渠道随机 ──
            shuffled_models = list(candidate_models)
            random.shuffle(shuffled_models)
            merged_reasons: dict[str, int] = defaultdict(int)
            for member_model in shuffled_models:
                candidates, _, reasons = await _timed_routing_call(
                    "candidate_scan_ms",
                    cls._collect_candidates(
                        model_id, [member_model], exclude_accounts, messages, operation,
                        group_whitelist, group_blacklist, key_whitelist, key_blacklist,
                        request_protocol=request_protocol,
                        account_whitelist=account_whitelist, is_test=is_test, is_probe=is_probe, snapshot=snapshot,
                        input_token_estimate=input_token_estimate,
                    ),
                )
                for k, v in reasons.items():
                    merged_reasons[k] += v
                if not candidates:
                    continue
                reserved = await cls._prepare_and_reserve(
                    candidates, select_fn=cls._select_channel_random, request_protocol=request_protocol,
                    strategy=strategy, volume_factor=volume_factor, messages=messages,
                    session_id=session_id, reasons=merged_reasons,
                    candidate_started=candidate_started, is_test=is_test, is_probe=is_probe,
                )
                if reserved:
                    return reserved
            cls._raise_no_available(model_id, operation, dict(merged_reasons))

        if is_group:
            # ── 模型组（其他策略）──
            all_candidates = []
            tpm_limited_all = []
            merged_reasons: dict[str, int] = defaultdict(int)
            for member_model in candidate_models:
                candidates, tpm_limited, reasons = await _timed_routing_call(
                    "candidate_scan_ms",
                    cls._collect_candidates(
                        model_id, [member_model], exclude_accounts, messages, operation,
                        group_whitelist, group_blacklist, key_whitelist, key_blacklist,
                        request_protocol=request_protocol,
                        account_whitelist=account_whitelist, is_test=is_test, is_probe=is_probe, snapshot=snapshot,
                        input_token_estimate=input_token_estimate,
                    ),
                )
                all_candidates.extend(candidates)
                tpm_limited_all.extend(tpm_limited)
                for k, v in reasons.items():
                    merged_reasons[k] += v

            if not all_candidates:
                if tpm_limited_all and len(tpm_limited_all) == len(candidate_models):
                    cls._raise_no_available(
                        model_id, operation, dict(merged_reasons),
                        code_override="rate_limit_exceeded",
                        detail_override=f"模型 '{model_id}' 达到 TPM 限制",
                    )
                cls._raise_no_available(model_id, operation, dict(merged_reasons))

            reserved = await cls._prepare_and_reserve(
                all_candidates, select_fn=select_fn, request_protocol=request_protocol,
                strategy=strategy, volume_factor=volume_factor, messages=messages,
                session_id=session_id, reasons=merged_reasons,
                candidate_started=candidate_started, is_test=is_test, is_probe=is_probe,
            )
            if reserved:
                return reserved
            cls._raise_no_available(model_id, operation, dict(merged_reasons))

        # ── 普通模型 ──
        # 单节点时与原实现逐行等价（收集 → 预占 → 耗尽报错）；真实模型多方案时按
        # 方案链逐档降级：激活方案无候选/预占耗尽 → 下一套 is_backup 方案，全部耗尽
        # 才收口报错（原因按节点累加）。TPM 限制是模型级检查，与过滤节点无关。
        merged_reasons: dict[str, int] = defaultdict(int)
        last_tpm_limited: list[str] = []
        saw_candidates = False
        for node_whitelist, node_blacklist in filter_nodes:
            candidates, tpm_limited, reasons = await _timed_routing_call(
                "candidate_scan_ms",
                cls._collect_candidates(
                    model_id, candidate_models, exclude_accounts, messages, operation,
                    node_whitelist, node_blacklist, key_whitelist, key_blacklist,
                    request_protocol=request_protocol,
                    account_whitelist=account_whitelist, is_test=is_test, is_probe=is_probe, snapshot=snapshot,
                    input_token_estimate=input_token_estimate,
                ),
            )
            last_tpm_limited = tpm_limited
            for k, v in reasons.items():
                merged_reasons[k] = merged_reasons.get(k, 0) + v
            if not candidates:
                continue
            saw_candidates = True
            reserved = await cls._prepare_and_reserve(
                candidates, select_fn=select_fn, request_protocol=request_protocol,
                strategy=strategy, volume_factor=volume_factor, messages=messages,
                session_id=session_id, reasons=merged_reasons,
                candidate_started=candidate_started, is_test=is_test, is_probe=is_probe,
            )
            if reserved:
                return reserved
        reasons = dict(merged_reasons)
        # 全节点无候选：TPM 限制用专用码收口，否则普通 no_available_account（与原实现一致）。
        if not saw_candidates:
            if last_tpm_limited and len(last_tpm_limited) == len(candidate_models):
                cls._raise_no_available(
                    model_id, operation, reasons,
                    code_override="rate_limit_exceeded",
                    detail_override=f"模型 '{model_id}' 达到 TPM 限制",
                )
            cls._raise_no_available(model_id, operation, reasons)
        # 有候选但预占全部失败；按用户要求统一 429（不做 503）；
        # redis_uncertain / provider_not_initialized 用 service_busy code 区分。
        reason_code = "service_busy" if reasons.get("redis_uncertain") or reasons.get("provider_not_initialized") else "no_available_account"
        cls._raise_no_available(model_id, operation, reasons, code_override=reason_code)

    @classmethod
    async def get_client(cls, model_id: str) -> BaseProvider | None:
        """轮询获取模型的可用客户端"""
        client, _, account_client = await cls._get_available_client_with_provider(model_id)
        await account_client.release()
        return client

    @classmethod
    async def get_client_with_provider(cls, model_id: str, exclude_accounts: list[tuple[str, str]] = None) -> tuple[BaseProvider, str]:
        """轮询获取模型的可用客户端及提供商名称，支持排除账号"""
        client, provider_name, account_client = await cls._get_available_client_with_provider(model_id, exclude_accounts)
        await account_client.release()
        return client, provider_name

    @classmethod
    async def acquire_client_with_provider(
        cls,
        model_id: str,
        exclude_accounts: list[tuple[str, str]] = None,
        messages: list[dict] | None = None,
        route: dict | None = None,
        api_key: str | None = None,
        operation: str | None = None,
        session_id: str | None = None,
        request_protocol: str | None = None,
        provider_whitelist: set[str] | None = None,
        provider_blacklist: set[str] | None = None,
        account_whitelist: set[str] | None = None,
        is_test: bool = False,
        is_probe: bool = False,
        snapshot=None,
        input_token_estimate: int = 0,
    ) -> tuple[BaseProvider, str, AccountClient]:
        """获取模型客户端并保留账号并发占用，调用方需 release。

        渠道白/黑名单由调用方显式传入；``account_whitelist`` / ``is_test`` / ``is_probe`` 为
        测试/探测专用。``is_probe``（定时检测）与 ``is_test`` 同样跳过选路预占，但失败时走与
        正常请求完全一致的冻结逻辑。

        ``input_token_estimate``：本次请求的预估输入 token（不含输出），用于按渠道模型
        ``extra_config.max_context_tokens`` 过滤候选。0 表示不做窗口过滤。
        """
        return await cls._get_available_client_with_provider(
            model_id, exclude_accounts, messages, route, api_key, operation, session_id, request_protocol,
            provider_whitelist=provider_whitelist, provider_blacklist=provider_blacklist,
            account_whitelist=account_whitelist, is_test=is_test, is_probe=is_probe, snapshot=snapshot,
            input_token_estimate=input_token_estimate,
        )

    @classmethod
    async def get_model_group_available_members(cls, model_id: str, snapshot=None) -> list[str]:
        snapshot = snapshot or model_catalog.current_snapshot()
        if not await _catalog_call(config.Config.is_model_group, model_id, snapshot=snapshot):
            return []
        group_models = await _catalog_call(config.Config.get_model_group_models, model_id, snapshot=snapshot)
        return [
            m for m in group_models
            if await _catalog_call(has_explicit_metadata, m, snapshot=snapshot) and cls.has_model_route(m)
        ]

    @classmethod
    def get_model_providers(cls, model_id: str) -> list[str]:
        """获取模型对应的提供商列表"""
        return cls.get_model_route_providers(model_id)

    @classmethod
    def get_provider_names(cls) -> list[str]:
        """获取所有提供商名称"""
        return list(cls._provider_pools.keys())

    @classmethod
    def get_provider_pool(cls, provider_name: str) -> ProviderPool:
        """获取提供商账号池"""
        return cls._provider_pools.get(provider_name)

    @classmethod
    def get_model_info(cls, model_id: str) -> dict | None:
        """Return current catalog-backed model metadata without DB or lazy cache I/O."""
        snapshot = model_catalog.current_snapshot()
        record = snapshot.metadata.get(model_id) or snapshot.group_metadata.get(model_id)
        if record is not None:
            # snapshot 被 _freeze 深度冻结（list→tuple、dict→MappingProxyType）。
            # 用 _mutable 还原成普通 list/dict，与 get_model_metadata / _build_models_from_routes
            # 同源；否则调用方拿到 tuple/MappingProxyType 后 isinstance(..., list) 误判，曾导致
            # _model_output_modalities 把 ('image',) 当非法值回退成 ['text']，图片/视频模型一律被
            # 误判为不支持生成。
            return _mutable({
                **FALLBACK_DEFAULT_MODEL_METADATA,
                **dict(snapshot.default),
                **{key: value for key, value in record.items() if key != "model_id"},
            })
        return cls._models_by_id.get(model_id)

    @classmethod
    async def update_account_quota_from_headers(cls, provider_name: str, username: str, headers: dict, model_id: str | None = None):
        """通过 provider + username 更新账号配额（大小写不敏感）"""
        pool = cls._provider_pools.get(provider_name)
        if not pool:
            return
        for client in pool.clients:
            if client.username == username:
                await client.provider.update_quota_from_headers(headers, model_id)
                # 同步 AccountClient provider-reported 字段
                client.sync_provider_quota()
                return
