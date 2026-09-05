"""限额追踪抽象层 - 支持多维度配额（RPM / TPM / 时间窗口额度）"""
import asyncio
import time
from abc import ABC, abstractmethod
from typing import Callable


class QuotaDimension:
    """单个配额维度（如 RPM、TPM、日额度、5小时额度等）"""

    def __init__(
        self,
        name: str,
        window_seconds: int,
        limit: int = 0,
        used: int = 0,
        reset_at: float | None = None,
        source: str = "memory",
    ):
        self.name = name
        self.window_seconds = window_seconds
        self.limit = limit
        self.used = used
        self.remaining = max(0, limit - used) if limit > 0 else None
        self.reset_at = reset_at
        self.source = source
        self.updated_at = int(time.time())

    def to_snapshot(self) -> dict:
        return {
            "name": self.name,
            "limit": self.limit,
            "used": self.used,
            "remaining": self.remaining,
            "window_seconds": self.window_seconds,
            "reset_at": self.reset_at,
            "source": self.source,
            "updated_at": self.updated_at,
        }

    def is_exhausted(self) -> bool:
        if self.limit <= 0:
            return False
        return (self.remaining or 0) <= 0

    def is_expired(self, now: float | None = None, stale_seconds: int = 600) -> bool:
        now = now or time.time()
        return (now - self.updated_at) > stale_seconds


class QuotaTracker(ABC):
    @abstractmethod
    async def check(self, model_id: str = None, tokens: int = 0) -> bool: ...
    @abstractmethod
    async def record(self, model_id: str = None, tokens: int = 0) -> None: ...
    @abstractmethod
    def snapshot(self) -> dict: ...


class WindowQuota(QuotaTracker):
    """
    通用时间窗口配额管理器。
    支持任意配额维度：RPM(60s)、TPM(60s)、日额度(86400s)、小时额度(3600s)、5小时额度(18000s)等。
    """

    def __init__(
        self,
        provider_name: str = "default",
        username: str = "",
        redis_getter: Callable | None = None,
    ):
        self.provider_name = provider_name
        self.username = username
        self.redis_getter = redis_getter
        self._lock = asyncio.Lock()
        self._dimensions: dict[str, QuotaDimension] = {}

    def _redis(self):
        if not self.redis_getter:
            return None
        try:
            return self.redis_getter()
        except Exception:
            return None

    def register_dimension(
        self,
        name: str,
        window_seconds: int,
        limit: int = 0,
        source: str = "memory",
    ) -> QuotaDimension:
        """注册一个配额维度。若已存在则更新 limit。"""
        dim = self._dimensions.get(name)
        if dim is None:
            dim = QuotaDimension(name=name, window_seconds=window_seconds, limit=limit, source=source)
            self._dimensions[name] = dim
        elif limit > 0:
            dim.limit = limit
            dim.remaining = max(0, limit - dim.used) if limit > 0 else None
        return dim

    def dimension(self, name: str) -> QuotaDimension | None:
        return self._dimensions.get(name)

    def dimensions(self) -> list[QuotaDimension]:
        return list(self._dimensions.values())

    def snapshot(self) -> dict:
        dims = [d.to_snapshot() for d in self.dimensions()]
        redis_avail = self._redis() is not None
        return {
            "dimensions": dims,
            "quota_redis_available": redis_avail,
            "quota_updated_at": max((d.updated_at for d in self.dimensions()), default=0),
        }

    async def check(self, model_id: str = None, tokens: int = 0) -> bool:
        for dim in self.dimensions():
            if dim.is_exhausted():
                return False
        return True

    async def record(self, model_id: str = None, tokens: int = 0) -> None:
        for dim in self.dimensions():
            if dim.limit <= 0:
                continue
            if dim.name == "tpm":
                dim.used += tokens
            else:
                dim.used += 1
            dim.remaining = max(0, dim.limit - dim.used)
            dim.updated_at = int(time.time())

    async def update_from_headers(self, headers: dict, model_id: str | None = None) -> None:
        headers_lower = {k.lower(): v for k, v in (headers or {}).items()}

        # 通用 x-ratelimit 头（OpenAI 格式）
        x_req_remaining = self._to_int(headers_lower.get("x-ratelimit-remaining-requests"))
        x_req_limit = self._to_int(headers_lower.get("x-ratelimit-limit-requests"))
        if x_req_limit is not None or x_req_remaining is not None:
            dim = self.register_dimension("requests_window", window_seconds=0, limit=x_req_limit or 0)
            if x_req_remaining is not None:
                dim.remaining = x_req_remaining
                dim.used = max(0, dim.limit - x_req_remaining) if dim.limit > 0 else 0
            dim.source = "headers"
            dim.updated_at = now

        x_tok_remaining = self._to_int(headers_lower.get("x-ratelimit-remaining-tokens"))
        x_tok_limit = self._to_int(headers_lower.get("x-ratelimit-limit-tokens"))
        if x_tok_limit is not None or x_tok_remaining is not None:
            dim = self.register_dimension("tokens_window", window_seconds=0, limit=x_tok_limit or 0)
            if x_tok_remaining is not None:
                dim.remaining = x_tok_remaining
                dim.used = max(0, dim.limit - x_tok_remaining) if x_tok_limit > 0 else 0
            dim.source = "headers"
            dim.updated_at = now

    @staticmethod
    def _to_int(value, default: int | None = None) -> int | None:
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default


class CountQuota(QuotaTracker):
    """兼容层：保留旧 CountQuota 接口，内部代理到 WindowQuota。"""

    def __init__(
        self,
        daily_limit=2000,
        per_model_limit=500,
        provider_name: str = "default",
        username: str = "",
        redis_getter: Callable | None = None,
    ):
        self.provider_name = provider_name
        self.username = username
        self.redis_getter = redis_getter
        self._inner = WindowQuota(provider_name, username, redis_getter)
        self.daily_limit = daily_limit
        self.daily_remaining = daily_limit
        self.per_model_limit = per_model_limit
        self.model_remaining: dict[str, int] = {}
        self.quota_source = "memory"
        self.quota_updated_at = 0
        self.redis_available = False
        self._inner.register_dimension("requests_daily", window_seconds=0, limit=daily_limit)

    def _redis(self):
        return self._inner._redis()

    async def check(self, model_id=None, tokens=0) -> bool:
        return await self._inner.check(model_id, tokens)

    async def record(self, model_id=None, tokens=0) -> None:
        await self._inner.record(model_id, tokens)
        daily_dim = self._inner.dimension("requests_daily")
        if daily_dim:
            self.daily_limit = daily_dim.limit
            self.daily_remaining = daily_dim.remaining
            self.quota_source = daily_dim.source
            self.quota_updated_at = daily_dim.updated_at

    async def update_from_headers(self, headers: dict, model_id: str | None = None) -> None:
        await self._inner.update_from_headers(headers, model_id)
        daily_dim = self._inner.dimension("requests_daily")
        if daily_dim:
            self.daily_limit = daily_dim.limit
            self.daily_remaining = daily_dim.remaining
            self.quota_source = daily_dim.source
            self.quota_updated_at = daily_dim.updated_at
        self.model_remaining = {}
        for d in self._inner.dimensions():
            if d.name.startswith("requests_daily_model:"):
                m = d.name.split(":", 1)[1]
                self.model_remaining[m] = d.remaining or 0

    async def snapshot(self) -> dict:
        return {
            "daily_requests_limit": self.daily_limit,
            "daily_requests_remaining": self.daily_remaining,
            "model_daily_remaining": dict(self.model_remaining),
            "quota_source": self.quota_source,
            "quota_updated_at": self.quota_updated_at,
            "quota_redis_available": self._redis() is not None,
            "_dimensions": [d.to_snapshot() for d in self._inner.dimensions()],
        }
