"""Provider-owned limit lifecycle wrapper."""

from __future__ import annotations

import asyncio
import inspect
import time

from .manager import LimitManager
from .backend import RedisLimitBackend
from .rules import LimitDecision, UpstreamQuotaSnapshot


def _broadcast_account_cooldown(provider_name: str, username: str | None, until: float, reason: str, clients) -> None:
    """响应头触发账号级冻结时同步广播，使本机之外的实例对齐 _cooldown_until。"""
    try:
        import runtime_sync
        targets = [username] if username else [getattr(c, "username", "") for c in (clients or [])]
        for uname in targets:
            if not uname:
                continue
            asyncio.create_task(runtime_sync.publish(
                runtime_sync.EVENT_COOLDOWN,
                f"{provider_name}:{uname}",
                extra={"provider": provider_name, "username": uname, "until": until, "reason": reason},
            ))
    except Exception:
        pass


def _broadcast_model_cooldown(provider_name: str, username: str | None, model_id: str, until: float, reason: str, clients) -> None:
    """响应头触发模型级冻结时同步广播，使本机之外的实例对齐 account_model 镜像。"""
    try:
        import runtime_sync
        targets = [username] if username else [getattr(c, "username", "") for c in (clients or [])]
        for uname in targets:
            if not uname:
                continue
            asyncio.create_task(runtime_sync.publish(
                runtime_sync.EVENT_COOLDOWN,
                f"{provider_name}:{uname}",
                extra={"provider": provider_name, "username": uname, "model_id": model_id, "until": until, "reason": reason},
            ))
    except Exception:
        pass


class ProviderLimitState:
    """Limit facade attached to each BaseProvider instance.

    The provider owns this object, while the low-level storage and counters stay
    in the shared limit layer. AccountClient passes itself through context so
    legacy runtime fields remain compatible during the migration.
    """

    def __init__(self, provider, config: dict | None = None, account_config: dict | None = None):
        self.provider = provider
        self.config = config or {}
        self.account_config = account_config or {}

    def _account_client(self, context: dict | None):
        return (context or {}).get("account_client")

    async def check(self, model_id: str | None = None, messages: list[dict] | None = None, context: dict | None = None):
        account_client = self._account_client(context)
        if account_client is None:
            if not model_id:
                return LimitDecision(True)
            return LimitDecision(await self.provider.check_message(model_id, messages))
        return await LimitManager.account_available_locked(account_client, model_id, messages, is_test=bool((context or {}).get("is_test")))

    async def reserve_with_decision(self, model_id: str | None = None, messages: list[dict] | None = None, context: dict | None = None):
        is_test = bool((context or {}).get("is_test"))
        if not model_id:
            from .rules import LimitReservation
            return LimitReservation(None, LimitDecision(False, "model_missing"))
        account_client = self._account_client(context)
        if account_client is None:
            from .rules import LimitLease, LimitReservation, LimitSubject
            if not await self.provider.check_message(model_id, messages):
                return LimitReservation(None, LimitDecision(False, "provider_message_quota_exceeded"))
            await self.provider.record_message(model_id, messages)
            return LimitReservation(LimitLease(LimitSubject(model=model_id), "provider-only"), LimitDecision(True))
        return await LimitManager.acquire_account(
            account_client, model_id, messages, is_test=is_test,
            token_estimate=int((context or {}).get("token_estimate") or 0),
        )


    async def reserve(self, model_id: str | None = None, messages: list[dict] | None = None, context: dict | None = None):
        result = await self.reserve_with_decision(model_id, messages, context)
        return result.lease

    async def release(self, lease=None, context: dict | None = None) -> None:
        account_client = self._account_client(context)
        if account_client is not None:
            await LimitManager.release_account_locked(account_client)

    async def record_usage(self, model_id: str, usage: dict | None = None, context: dict | None = None) -> None:
        account_client = self._account_client(context)
        if account_client is None:
            return
        usage = usage or {}
        tokens = int(usage.get("total_tokens") or usage.get("tokens") or 0)
        await LimitManager.record_account_tokens(account_client, tokens)

    async def update_from_headers(self, headers: dict, model_id: str | None = None, context: dict | None = None) -> None:
        await self.provider.update_quota_from_headers(headers, model_id)
        # 评估 headers 条件冻结规则
        account_client = self._account_client(context)
        provider_name = getattr(self.provider, "PROVIDER_NAME", "")
        username = getattr(self.provider, "username", "")
        if not provider_name or not username:
            return
        try:
            channel = getattr(self.provider, "_channel", None)
            if channel is not None and hasattr(channel, "match_freeze"):
                matched = channel.match_freeze(status_code=None, headers=headers)
            else:
                from limit_policy_store import get_effective_provider_policy_sync, match_freeze_rules
                policy = get_effective_provider_policy_sync(provider_name)
                matched = match_freeze_rules(policy.get("freeze_policy"), status_code=None, headers=headers)
            if not matched or matched["freeze_mode"] == "no_freeze":
                return
        except Exception:
            return
        scope = matched["scope"]
        seconds = matched["seconds"]
        freeze_reason = f"freeze_policy:{matched['freeze_mode']}"
        # 若是 model scope 但 model_id 缺失，降级到更宽的 scope（account_model→account，channel_model→channel）
        if scope == "channel_model" and not model_id:
            scope = "channel"
        if scope == "account_model" and not model_id:
            scope = "account"

        # 冻结写入统一走 ProviderPool：与主失败收口共用同一套「失败刷新冻结周期」判定，
        # 关时已冻结对象保留现有 TTL、不再落 Redis/广播。取不到 pool 的池外场景才退回直写。
        pool = None
        try:
            from rate_limiter import ModelClientPool
            pool = ModelClientPool.get_provider_pool(provider_name)
        except Exception:
            pool = None
        if pool is not None:
            if scope == "channel_model":
                for client in getattr(pool, "clients", None) or []:
                    pool._freeze_account_model(client.username, model_id, seconds, freeze_reason)
            elif scope == "channel":
                for client in getattr(pool, "clients", None) or []:
                    pool._freeze_account(client, seconds, freeze_reason)
            elif scope == "account_model":
                pool._freeze_account_model(username, model_id, seconds, freeze_reason)
            else:
                client = next((c for c in pool.clients if c.username == username), None) or account_client
                if client is not None:
                    pool._freeze_account(client, seconds, freeze_reason)
            return

        # 池外兜底：仍尊重渠道「失败刷新冻结周期」——已冻结对象不覆盖 TTL。
        refresh = True
        try:
            if channel is not None and hasattr(channel, "freeze_refresh_on_failure"):
                refresh = bool(channel.freeze_refresh_on_failure())
        except Exception:
            refresh = True
        # 渠道级 scope 需要冻结整个渠道的所有账号，取账号池遍历。
        if scope in ("channel", "channel_model"):
            until = time.time() + seconds
            if scope == "channel_model":
                # 与 apply_freeze_policy 的 channel_model 分支对称：写本机模型级镜像 + pubsub，
                # 否则本进程选账号走 _is_account_model_cooling（内存镜像）会漏判、反复选中已冻结的账号-模型。
                if account_client is not None:
                    account_client.freeze(kind="account_model", model_id=model_id, seconds=seconds, reason=freeze_reason, refresh=refresh)
                    await RedisLimitBackend.set_account_model_cooldown(provider_name, username, model_id, seconds, freeze_reason)
                    _broadcast_model_cooldown(provider_name, username, model_id, until, freeze_reason, None)
            elif account_client is not None:
                account_client.freeze(kind="account", seconds=seconds, reason=freeze_reason, refresh=refresh)
                await RedisLimitBackend.set_account_cooldown(provider_name, username, seconds, freeze_reason)
                _broadcast_account_cooldown(provider_name, username, until, freeze_reason, None)
            return
        if scope == "account_model":
            until = time.time() + seconds
            if account_client is not None:
                if account_client.freeze(kind="account_model", model_id=model_id, seconds=seconds, reason=freeze_reason, refresh=refresh):
                    await RedisLimitBackend.set_account_model_cooldown(provider_name, username, model_id, seconds, freeze_reason)
                    _broadcast_model_cooldown(provider_name, username, model_id, until, freeze_reason, None)
        else:
            if account_client is not None:
                if account_client.freeze(kind="account", seconds=seconds, reason=freeze_reason, refresh=refresh):
                    await RedisLimitBackend.set_account_cooldown(provider_name, username, seconds, freeze_reason)
                    _broadcast_account_cooldown(provider_name, username, time.time() + seconds, freeze_reason, None)

    async def update_snapshot(self, snapshot: UpstreamQuotaSnapshot, ttl_seconds: int = 86400) -> None:
        payload = {
            "rule": snapshot.rule,
            "provider": snapshot.provider,
            "metric": snapshot.metric,
            "scope": snapshot.scope,
            "subject": snapshot.subject,
            "limit": snapshot.limit,
            "remaining": snapshot.remaining,
            "used": snapshot.used,
            "reset_at": snapshot.reset_at,
            "source": snapshot.source,
            "updated_at": snapshot.updated_at,
        }
        key = f"{snapshot.metric}:{snapshot.scope}:{snapshot.subject}"
        await RedisLimitBackend.set_snapshot(key, payload, ttl_seconds)

    async def snapshot(self) -> dict:
        get_quota_snapshot = getattr(self.provider, "get_quota_snapshot", None)
        if not get_quota_snapshot:
            return {}
        result = get_quota_snapshot()
        return await result if inspect.isawaitable(result) else result
