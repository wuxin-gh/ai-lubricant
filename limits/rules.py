"""Shared data structures for runtime limits."""

from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class WindowSpec:
    type: str = "rolling"
    seconds: int | None = None
    calendar: str | None = None
    timezone: str = "Asia/Shanghai"


@dataclass(slots=True)
class LimitRule:
    name: str
    scope: str
    metric: str
    limit: int
    window_seconds: int | None = None
    window_type: str = "rolling"
    window: WindowSpec | None = None
    hard: bool = True
    action: str = "reject"
    source: str = "local"


@dataclass(slots=True)
class LimitSubject:
    provider: str = ""
    account: str = ""
    model: str = ""
    routed_model: str = ""
    api_key: str | None = None
    route_id: str | None = None

    @property
    def account_key(self) -> str:
        return f"account:{self.provider}:{self.account}"

    @property
    def model_key(self) -> str:
        return f"model:{self.routed_model or self.model}"

    @property
    def provider_model_key(self) -> str:
        return f"provider-model:{self.provider}:{self.routed_model or self.model}"


@dataclass(slots=True)
class LimitBlock:
    rule: str
    reason: str
    retry_after: int | None = None
    current: int | None = None
    limit: int | None = None


@dataclass(slots=True)
class LimitDecision:
    allowed: bool
    reason: str = ""
    retry_after: int | None = None
    details: dict = field(default_factory=dict)
    blocked_by: list[LimitBlock] = field(default_factory=list)
    headroom: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class LimitLease:
    subject: LimitSubject
    lease_id: str
    reservation_id: str = ""
    acquired_at: float = field(default_factory=time.time)
    expires_at: float | None = None
    concurrency_acquired: bool = False
    local_fallback: bool = False
    acquired_keys: list[str] = field(default_factory=list)
    reserved_amounts: dict[str, int] = field(default_factory=dict)
    window_ids: dict[str, str] = field(default_factory=dict)
    settled: bool = False
    rolled_back: bool = False
    renewal_task: object | None = None


@dataclass(slots=True)
class LimitReservation:
    lease: LimitLease | None
    decision: LimitDecision

    @property
    def allowed(self) -> bool:
        return self.lease is not None and self.decision.allowed


@dataclass(slots=True)
class UpstreamQuotaSnapshot:
    rule: str
    provider: str
    metric: str
    scope: str
    subject: str
    limit: int | None = None
    remaining: int | None = None
    used: int | None = None
    reset_at: float | None = None
    source: str = "upstream"
    updated_at: float = field(default_factory=time.time)
