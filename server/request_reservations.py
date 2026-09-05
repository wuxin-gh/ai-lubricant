"""请求主链路的预占封装（幂等 commit / rollback / release）。

把两类预占收敛成统一、幂等的句柄，杜绝原链路里散落在 success / except / finally /
stream generator finally 的重复释放：

- :class:`ApiKeyReservation` —— 请求级，整个客户端请求只占/释一次；内部重试再多次也
  不重复占用（对齐「API Key 按请求只占 1 份」）。包裹现有 ``RateLimiter.acquire`` /
  ``RateLimiter.release``（当前 release 实质 no-op，这里保留调用点以便未来演进）。
- :class:`CandidateReservation` —— 候选级，选中 ``(provider, account, model)`` 后预占，
  包裹现有 ``AccountClient`` 的 lease（``reserve_with_decision`` 得到、``release`` 释放）。
  成功 commit / 失败 rollback / 并发始终 release —— 全部幂等，重复调用无副作用。

设计约束（见重构 plan）：
- 并发无论成功失败都释放；成功保留的是 RPM/RPD/TPM 等计量结果，不是并发槽位。
- 所有 commit/rollback/release 幂等，主编排器只在一个 finalizer 里调用一次即可，
  多调用也安全。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ApiKeyReservation:
    """API Key 请求级预占句柄。

    现状：``RateLimiter.acquire`` 在超限时抛 429，并在 ``check`` 内记 RPM/RPD 计数；
    ``release`` 目前是 no-op。封装成句柄是为了让主链路有一个明确、幂等的释放点，
    且语义上「一个客户端请求只占一份」——内部重试不再触碰它。
    """

    api_key: str | None
    lease: Any = None
    released: bool = False
    committed: bool = False

    @classmethod
    async def acquire(cls, api_key: str | None, *, enabled: bool, client_ip: str | None = None) -> "ApiKeyReservation":
        """请求入口调用一次。``enabled`` 由调用方传入（api_keys_enabled）。

        超限时 ``RateLimiter.acquire`` 抛 HTTPException(429)，向上传播即可。
        ``client_ip`` 用于 IP 个数限制（未配置时由 RateLimiter 自行跳过）。
        """
        if not api_key or not enabled:
            return cls(api_key=None)
        from rate_limiter import RateLimiter
        lease = await RateLimiter.acquire(api_key, client_ip=client_ip)
        return cls(api_key=api_key, lease=lease)

    async def commit(self) -> None:
        """请求成功收口：标记已确认（RPM/RPD 计数已在 acquire 阶段记入）。幂等。"""
        self.committed = True

    async def release(self) -> None:
        """释放请求级占用。幂等：重复调用无副作用。"""
        if self.released:
            return
        self.released = True
        lease = self.lease
        self.lease = None
        if lease is None:
            return
        from rate_limiter import RateLimiter
        await RateLimiter.release(lease)


@dataclass(slots=True)
class CandidateReservation:
    """候选级预占句柄，包裹 AccountClient 的 lease。

    成功元组由 ``_try_reserve`` 产生：``(provider, provider_name, account_client)``。
    这里额外持有 account_client 以便统一释放，并保证 release 幂等（原链路在多个
    finally/except 里各释一次，极易重复或漏释）。

    计量结算按候选生命周期收口：
    - ``commit(usage)`` —— 请求成功：用真实 token usage 对预占多退少补（reconcile），再由
      finally 释放并发。
    - ``keep_served_usage(usage)`` —— 流式已向客户端产出后中断（已服务）：同 commit，保留
      请求计数、按已产出 usage 结算，绝不回滚。
    - ``rollback()`` —— 失败（4xx/5xx/网络异常/取消未产出）：按 reservation_id 精确回退本次
      请求数与 token 预占，再释放并发。
    全部幂等；``settled``（已结算）与 ``rolled_back`` 互斥，先到者赢。
    """

    provider_name: str
    account_client: Any
    provider: Any = None
    committed: bool = False
    rolled_back: bool = False
    released: bool = False
    settled: bool = False
    _lease: Any = None

    def _capture_lease(self) -> Any:
        """抓取本候选当前任务的 lease（成功元组产生后立即调用一次）。"""
        if self._lease is not None:
            return self._lease
        account_client = self.account_client
        getter = getattr(account_client, "current_limit_lease", None) if account_client else None
        if getter is not None:
            try:
                self._lease = getter()
            except Exception:
                self._lease = None
        return self._lease

    async def commit(self, usage: Any = None) -> None:
        """attempt 成功：用真实 usage 结算 token 预占（多退少补）。幂等。

        并发不在此释放——由主编排 finally 统一 release，保证一个候选恰好释放一次。
        """
        self.committed = True
        await self._settle(usage)

    async def keep_served_usage(self, usage: Any = None) -> None:
        """已服务中断（STREAM_ERROR_AND_STOP，rollback_reservation=False）：保留计数并按已产出
        usage 结算，语义等同成功结算，绝不回滚。幂等。"""
        self.committed = True
        await self._settle(usage)

    async def _settle(self, usage: Any) -> None:
        if self.settled or self.rolled_back:
            return
        lease = self._capture_lease()
        actual_tokens = _usage_total_tokens(usage)
        if lease is not None:
            from limits.manager import LimitManager
            try:
                await LimitManager.reconcile_account_reservation(lease, actual_tokens)
            except Exception:
                pass
        self.settled = True

    async def rollback(self) -> None:
        """attempt 失败：按 reservation 精确回退请求数与 token 预占，再释放并发。幂等。"""
        if self.rolled_back or self.settled:
            # 已结算（成功/已服务）不回滚；已回滚不重复。
            if not self.released:
                await self._release_concurrency()
            return
        self.rolled_back = True
        lease = self._capture_lease()
        if lease is not None:
            from limits.manager import LimitManager
            try:
                await LimitManager.rollback_account_reservation(lease)
            except Exception:
                pass
        await self._release_concurrency()

    async def release(self) -> None:
        """释放并发 lease。无论成功失败都应调用。幂等。"""
        await self._release_concurrency()

    async def _release_concurrency(self) -> None:
        if self.released:
            return
        self.released = True
        account_client = self.account_client
        self.account_client = None
        if account_client is None:
            return
        release = getattr(account_client, "release", None)
        if release is None:
            return
        await release()


def _usage_total_tokens(usage: Any) -> int:
    """从 usage dict 提取 total_tokens；非 dict / 缺失按 0。"""
    if not isinstance(usage, dict):
        return 0
    for key in ("total_tokens", "tokens", "total"):
        value = usage.get(key)
        if value:
            try:
                return max(0, int(value))
            except (TypeError, ValueError):
                continue
    return 0
