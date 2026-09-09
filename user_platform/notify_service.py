"""Notify domain service (channels / subscriptions / send logs).

Storage + access control for the notification surface. Multi-user isolation is
enforced by ``owner_id``. The channel ``secret`` is masked before leaving this
layer; the full secret is only ever used server-side when actually dispatching.

This layer is storage only — it never touches the model request pipeline.
Actual dispatch (webhook/IM/wechat_mp delivery) depends on outbound integrations
and is layered on later without blocking channel/subscription management.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from .masking import has_secret, mask_secret
from .models_notify import (
    NotifyChannel,
    NotifyDevice,
    NotifyEvent,
    NotifyEventChannel,
    NotifyEventState,
    NotifySendLog,
    NotifySubscription,
    NotifySubscriptionRule,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

_PRIVILEGED_ROLES = {"admin"}

# The in-app notification center is modelled as a channel kind, not as the
# default sink for every event: a ``notifications`` row (bell + app push) is
# written only when a configured event binds a channel of this kind. Exactly one
# such channel exists per owner scope, auto-provisioned on first list, and it is
# not created/deleted by hand — it has no webhook_url to configure.
NOTIFY_CENTER_KIND = "notify_center"
NOTIFY_CENTER_NAME = "通知中心"
MOBILE_PUSH_KIND = "mobile_push"

# Static notify event catalogue. Each entry carries a one-level ``category`` and
# an ``owner_scope`` ("platform" = admin-only, "user" = configurable on the
# user-side console). Legacy upstream types (task.*/vm.*/quota.*) are kept so
# existing subscriptions and history do not break; new typed events are added
# alongside them (spec: preserve prior events, don't discard).
NOTIFY_EVENT_TYPES = (
    # 账号
    {"type": "account.frozen", "name": "账号已冻结", "category": "account", "owner_scope": "platform"},
    {"type": "account.unfrozen", "name": "账号已解冻", "category": "account", "owner_scope": "platform"},
    {"type": "account.created", "name": "账号新增", "category": "account", "owner_scope": "platform"},
    {"type": "account.init_failed", "name": "账号初始化失败", "category": "account", "owner_scope": "platform"},
    {"type": "account.logged_out", "name": "账号已退登", "category": "account", "owner_scope": "platform"},
    # 渠道
    {"type": "channel.frozen", "name": "渠道已冻结", "category": "channel", "owner_scope": "platform"},
    {"type": "channel.created", "name": "渠道已创建", "category": "channel", "owner_scope": "platform"},
    # 密钥
    {"type": "api_key.created", "name": "密钥已创建", "category": "api_key", "owner_scope": "platform"},
    {"type": "api_key.modified", "name": "密钥已修改", "category": "api_key", "owner_scope": "platform"},
    {"type": "api_key.usage_threshold", "name": "密钥用量达阈值", "category": "api_key", "owner_scope": "platform"},
    # 模型 / 节点 / 安全
    {"type": "model.new", "name": "发现新模型", "category": "model", "owner_scope": "platform"},
    {"type": "node.new_version", "name": "节点有新版本", "category": "node", "owner_scope": "platform"},
    {"type": "node.online", "name": "节点已上线", "category": "node", "owner_scope": "user"},
    {"type": "security.warning", "name": "安全告警", "category": "security", "owner_scope": "platform"},
    # 系统
    {"type": "log.cleanup_done", "name": "日志清理完成", "category": "system", "owner_scope": "platform"},
    {"type": "log.cleanup_failed", "name": "日志清理失败", "category": "system", "owner_scope": "platform"},
    # 任务（用户侧）
    {"type": "task.created", "name": "任务创建", "category": "task", "owner_scope": "user"},
    {"type": "task.ended", "name": "任务结束", "category": "task", "owner_scope": "user"},
    {"type": "task.failed", "name": "任务失败", "category": "task", "owner_scope": "user"},
    {"type": "task.paused", "name": "任务暂停", "category": "task", "owner_scope": "user"},
    {"type": "task.idle", "name": "任务停留", "category": "task", "owner_scope": "user"},
    {"type": "task.deleted", "name": "任务已删除", "category": "task", "owner_scope": "user"},
    # 旧事件保留（历史订阅/记录不失效）
    {"type": "vm.expiring_soon", "name": "虚拟机即将到期", "category": "task", "owner_scope": "user"},
    {"type": "quota.refreshed", "name": "配额刷新", "category": "task", "owner_scope": "user"},
    {"type": "quota.basic_exhausted", "name": "基础配额耗尽", "category": "task", "owner_scope": "user"},
    {"type": "quota.pro_exhausted", "name": "Pro 配额耗尽", "category": "task", "owner_scope": "user"},
    {"type": "quota.ultra_exhausted", "name": "Ultra 配额耗尽", "category": "task", "owner_scope": "user"},
)


def _channel_dict(c: NotifyChannel, event_types: list[str] | None = None) -> dict:
    return {
        "id": str(c.id),
        "owner_id": str(c.owner_id),
        "owner_type": c.owner_type,
        "name": c.name,
        "kind": c.kind,
        "webhook_url": c.webhook_url,
        "headers": c.headers,
        "metadata": c.metadata,
        "target_id": c.target_id,
        "enabled": c.enabled,
        # The notify center is provisioned by the service, has no webhook_url and
        # no secret, so the UI must not offer edit/delete/test on it.
        "builtin": c.kind == NOTIFY_CENTER_KIND,
        # The UI models subscribed events as a property *of the channel* (there
        # is no subscription screen), so the backing NotifySubscription row is
        # flattened into the channel DTO here. Without this the receiver list
        # renders every channel's events as "无".
        "event_types": event_types or [],
        # secret is sensitive: expose only presence + masked tail.
        "secret_masked": mask_secret(c.secret),
        "has_secret": has_secret(c.secret),
        "created_at": c.created_at.isoformat() if c.created_at else None,
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    }


def _subscription_dict(s: NotifySubscription) -> dict:
    return {
        "id": str(s.id),
        "channel_id": str(s.channel_id),
        "scope": s.scope,
        "event_types": s.event_types or [],
        "enabled": s.enabled,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


def _send_log_dict(log: NotifySendLog) -> dict:
    return {
        "id": str(log.id),
        "subscription_id": str(log.subscription_id),
        "channel_id": str(log.channel_id),
        "event_type": log.event_type,
        "event_ref_id": log.event_ref_id,
        "status": log.status,
        "error": log.error,
        "created_at": log.created_at.isoformat() if log.created_at else None,
    }


def _event_dict(e: NotifyEvent, channel_ids: list[str] | None = None) -> dict:
    def _time_text(value: Any) -> str | None:
        if value is None:
            return None
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    return {
        "id": str(e.id),
        "owner_type": e.owner_type,
        "owner_id": str(e.owner_id) if e.owner_id else None,
        "name": e.name,
        "event_type": e.event_type,
        "category": e.category,
        "status": e.status,
        "effective_from": e.effective_from.isoformat() if e.effective_from else None,
        "effective_to": e.effective_to.isoformat() if e.effective_to else None,
        "daily_start": _time_text(e.daily_start),
        "daily_end": _time_text(e.daily_end),
        "trigger_condition": e.trigger_condition or {},
        "event_params": e.event_params or {},
        "channel_ids": channel_ids or [],
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "updated_at": e.updated_at.isoformat() if e.updated_at else None,
    }


def _rule_dict(r: NotifySubscriptionRule) -> dict:
    return {
        "id": str(r.id),
        "owner_type": r.owner_type,
        "owner_id": str(r.owner_id),
        "channel_id": str(r.channel_id),
        "event_type": r.event_type,
        "filters": r.filters or {},
        "enabled": r.enabled,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _maybe_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


class NotifyService:
    """Notification channels/subscriptions/logs with per-user isolation."""

    def _is_privileged(self, role: str | None) -> bool:
        return (role or "") in _PRIVILEGED_ROLES

    # ---- Channels ---------------------------------------------------------
    @staticmethod
    async def ensure_notify_center_channel(
        owner_id: str, owner_type: str
    ) -> NotifyChannel | None:
        """Return this owner's notify-center channel, creating it on first use.

        Auto-provisioned rather than hand-created: it carries no webhook_url or
        secret, so there is nothing for a user to fill in — the only decision is
        whether an event binds it, which happens in the event editor.
        """
        oid = _maybe_uuid(owner_id)
        if oid is None:
            return None
        existing = await NotifyChannel.filter(
            owner_id=oid, owner_type=owner_type, kind=NOTIFY_CENTER_KIND
        ).first()
        if existing is not None:
            return existing
        return await NotifyChannel.create(
            id=uuid.uuid4(),
            owner_id=oid,
            owner_type=owner_type,
            name=NOTIFY_CENTER_NAME,
            kind=NOTIFY_CENTER_KIND,
            webhook_url="",
            secret="",
            enabled=True,
        )

    @staticmethod
    async def _event_types_by_channel(
        channel_ids: list[uuid.UUID],
    ) -> dict[str, list[str]]:
        """Collect each channel's subscribed events in one query (no N+1).

        A channel may hold several subscription rows; the union is what the UI
        shows, since it presents events as a flat property of the channel.
        """
        if not channel_ids:
            return {}
        merged: dict[str, list[str]] = {}
        rows = await NotifySubscription.filter(
            channel_id__in=channel_ids, enabled=True
        ).values("channel_id", "event_types")
        for row in rows:
            key = str(row["channel_id"])
            bucket = merged.setdefault(key, [])
            for event in row["event_types"] or []:
                if event not in bucket:
                    bucket.append(str(event))
        return merged

    async def list_channels(
        self, user_id: str, *, role: str | None = None, owner_type: str = "user"
    ) -> list[dict]:
        """List channels for one owner scope.

        ``owner_type="team"`` addresses the team-shared surface, where ``user_id``
        is the *team* id. Team scope is NOT widened for privileged roles: a team
        admin manages their own team's channels, and returning every team's rows
        would leak other teams' webhook URLs into the page.

        The owner's notify-center channel is provisioned here so the event editor
        always has it to bind, without a "create the notification center" step.
        """
        # Only for owner-exact scopes: the privileged cross-user listing below is
        # a read-only view over other people's channels, not an owner surface.
        if owner_type in ("team", "platform") or not self._is_privileged(role):
            await self.ensure_notify_center_channel(user_id, owner_type)
        if owner_type == "team":
            query = NotifyChannel.filter(
                owner_id=uuid.UUID(user_id), owner_type="team"
            )
        elif owner_type == "platform":
            # Platform/admin notification channels use a fixed sentinel owner id;
            # never widen into personal rows even when the caller is privileged.
            query = NotifyChannel.filter(
                owner_id=uuid.UUID(user_id), owner_type="platform"
            )
        elif self._is_privileged(role):
            query = NotifyChannel.filter(owner_type="user")
        else:
            query = NotifyChannel.filter(
                owner_id=uuid.UUID(user_id), owner_type="user"
            )
        rows = await query.order_by("-created_at")
        events = await self._event_types_by_channel([r.id for r in rows])
        return [_channel_dict(r, events.get(str(r.id))) for r in rows]

    async def _owned_channel(
        self,
        user_id: str,
        channel_id: str,
        role: str | None,
        owner_type: str = "user",
    ) -> NotifyChannel | None:
        cid = _maybe_uuid(channel_id)
        if cid is None:
            return None
        channel = await NotifyChannel.get_or_none(id=cid)
        if channel is None:
            return None
        # A channel is only reachable through the scope that owns it, so a user-side
        # request can never mutate a team channel (or vice versa) by id alone.
        if (channel.owner_type or "user") != owner_type:
            return None
        if owner_type == "team":
            return channel if str(channel.owner_id) == str(user_id) else None
        if owner_type == "platform":
            # Platform scope is owner-exact too: privileged widening applies only
            # to the personal surface, never to platform/admin channels.
            return channel if str(channel.owner_id) == str(user_id) else None
        if not self._is_privileged(role) and str(channel.owner_id) != str(user_id):
            return None
        return channel

    async def create_channel(
        self, user_id: str, req: dict, *, owner_type: str = "user"
    ) -> dict:
        """Create a channel owned by ``user_id`` in the given scope.

        ``owner_type`` is supplied by the route (never by the request body), so a
        user-side caller cannot create a team-shared channel by passing a field.
        """
        name = (req.get("name") or "").strip()
        kind = (req.get("kind") or "").strip()
        if not name:
            raise ValueError("name_required")
        if not kind:
            raise ValueError("kind_required")
        channel = await NotifyChannel.create(
            id=uuid.uuid4(),
            owner_id=uuid.UUID(user_id),
            owner_type=owner_type,
            name=name,
            kind=kind,
            webhook_url=req.get("webhook_url") or "",
            secret=req.get("secret") or "",
            headers=req.get("headers") or None,
            metadata=req.get("metadata") or None,
            target_id=req.get("target_id") or "",
            enabled=req.get("enabled", True),
        )
        # The UI sends the subscribed events on the channel; persist them as the
        # channel's canonical subscription row so dispatch can actually match.
        event_types = [str(e) for e in (req.get("event_types") or [])]
        await self._sync_subscription(channel.id, event_types)
        return _channel_dict(channel, event_types)

    @staticmethod
    async def _sync_subscription(
        channel_id: uuid.UUID, event_types: list[str]
    ) -> None:
        """Make the channel's single subscription row match ``event_types``.

        The UI models events as a property *of the channel* (there is no
        subscription screen), while storage keeps the upstream two-table shape.
        Collapsing to one row per channel keeps both true without the UI ever
        having to know about subscriptions.
        """
        existing = await NotifySubscription.filter(channel_id=channel_id).order_by(
            "created_at"
        )
        if existing:
            first, *extra = existing
            first.event_types = event_types
            first.enabled = bool(event_types)
            await first.save(update_fields=["event_types", "enabled", "updated_at"])
            for row in extra:  # collapse any legacy duplicates
                await row.delete()
            return
        await NotifySubscription.create(
            id=uuid.uuid4(),
            channel_id=channel_id,
            scope="self",
            event_types=event_types,
            enabled=bool(event_types),
        )

    async def update_channel(
        self,
        user_id: str,
        channel_id: str,
        req: dict,
        *,
        role: str | None = None,
        owner_type: str = "user",
    ) -> bool:
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return False
        # The notify center is provisioned/managed by the service — its name and
        # (empty) webhook are fixed, so it exposes no editable fields.
        if channel.kind == NOTIFY_CENTER_KIND:
            return False
        changed: list[str] = []
        for field in ("name", "webhook_url", "target_id"):
            if field in req:
                setattr(channel, field, req[field])
                changed.append(field)
        for field in ("headers", "metadata"):
            if field in req:
                setattr(channel, field, req[field])
                changed.append(field)
        if "enabled" in req:
            channel.enabled = bool(req["enabled"])
            changed.append("enabled")
        # secret only updated when a non-empty value is explicitly provided,
        # so a masked round-trip from the client never wipes the stored secret.
        if req.get("secret"):
            channel.secret = req["secret"]
            changed.append("secret")
        # Events live on the subscription row, so they are synced separately and
        # must not short-circuit on an otherwise-unchanged channel.
        if "event_types" in req:
            await self._sync_subscription(
                channel.id, [str(e) for e in (req.get("event_types") or [])]
            )
        if not changed:
            return True
        changed.append("updated_at")
        await channel.save(update_fields=changed)
        return True

    async def delete_channel(
        self,
        user_id: str,
        channel_id: str,
        *,
        role: str | None = None,
        owner_type: str = "user",
    ) -> bool:
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return False
        # Deleting the notify center would silently orphan every event bound to
        # it; it is re-provisioned on the next list anyway.
        if channel.kind == NOTIFY_CENTER_KIND:
            return False
        subs = await NotifySubscription.filter(channel_id=channel.id).values_list("id", flat=True)
        if subs:
            await NotifySendLog.filter(subscription_id__in=list(subs)).delete()
        await NotifySubscription.filter(channel_id=channel.id).delete()
        await channel.delete()
        return True

    # ---- Subscriptions ----------------------------------------------------
    async def list_subscriptions(
        self,
        user_id: str,
        channel_id: str,
        *,
        role: str | None = None,
        owner_type: str = "user",
    ) -> list[dict] | None:
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return None
        rows = await NotifySubscription.filter(channel_id=channel.id).order_by("created_at")
        return [_subscription_dict(r) for r in rows]

    async def create_subscription(
        self,
        user_id: str,
        channel_id: str,
        req: dict,
        *,
        role: str | None = None,
        owner_type: str = "user",
    ) -> dict | None:
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return None
        sub = await NotifySubscription.create(
            id=uuid.uuid4(),
            channel_id=channel.id,
            scope=req.get("scope") or "self",
            event_types=req.get("event_types") or [],
            enabled=req.get("enabled", True),
        )
        return _subscription_dict(sub)

    async def delete_subscription(
        self,
        user_id: str,
        channel_id: str,
        sub_id: str,
        *,
        role: str | None = None,
        owner_type: str = "user",
    ) -> bool:
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return False
        sid = _maybe_uuid(sub_id)
        if sid is None:
            return False
        deleted = await NotifySubscription.filter(id=sid, channel_id=channel.id).delete()
        return deleted > 0

    # ---- Send logs --------------------------------------------------------
    async def list_send_logs(
        self,
        user_id: str,
        channel_id: str,
        *,
        role: str | None = None,
        limit: int = 100,
        owner_type: str = "user",
    ) -> list[dict] | None:
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return None
        rows = (
            await NotifySendLog.filter(channel_id=channel.id)
            .order_by("-created_at")
            .limit(max(1, min(limit, 500)))
        )
        return [_send_log_dict(r) for r in rows]

    # ---- Test delivery ----------------------------------------------------
    async def test_channel(
        self,
        user_id: str,
        channel_id: str,
        *,
        role: str | None = None,
        owner_type: str = "user",
    ) -> tuple[bool, str] | None:
        """Send a test message to one owned channel.

        Returns ``None`` when the channel is missing or not visible to the
        caller, otherwise ``(ok, error)``. The unmasked ``secret`` is read here
        and handed straight to dispatch — it never crosses the DTO boundary.
        """
        channel = await self._owned_channel(user_id, channel_id, role, owner_type)
        if channel is None:
            return None
        # A "test push" for the notify center would just insert a throwaway bell
        # row; there is no external endpoint to probe, so treat it as a no-op OK.
        if channel.kind == NOTIFY_CENTER_KIND:
            return True, ""
        if not channel.enabled:
            return False, "渠道已禁用"
        from .notify_dispatch import send_test

        return await send_test(
            {
                "id": channel.id,
                "kind": channel.kind,
                "webhook_url": channel.webhook_url,
                "headers": channel.headers,
            },
            channel.secret or "",
        )

    # ---- Event catalogue --------------------------------------------------
    def list_event_types(self, *, owner_scope: str | None = None) -> list[dict]:
        """Static list of supported notify event types (no DB access).

        ``owner_scope="user"`` returns only user-configurable events (task/node
        online), so the console cannot subscribe to admin-only platform events.
        """
        items = [dict(item) for item in NOTIFY_EVENT_TYPES]
        if owner_scope:
            items = [it for it in items if it.get("owner_scope") == owner_scope]
        return items

    # ---- Configured events (primary path: event entity + channel bindings) ----
    #
    # An event row carries WHAT to watch (event_type), WHEN it is live
    # (status + effective range + daily window), WHICH occurrences qualify
    # (event_params) and HOW OFTEN to fire (trigger_condition). Channels are a
    # separate binding table, so one event can fan out to several channels and
    # one channel can serve several events.

    @staticmethod
    def _parse_dt(value: Any) -> Any:
        """Parse an ISO datetime from JSON; ``None``/invalid means unbounded."""
        if not value:
            return None
        from datetime import datetime, timezone

        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        # Naive input is read as UTC: the comparison in the matcher is tz-aware,
        # and mixing naive with aware raises instead of just comparing wrong.
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    @staticmethod
    def _parse_time(value: Any) -> Any:
        """Parse ``HH:MM``/``HH:MM:SS`` for the daily window; None = all day."""
        if not value:
            return None
        from datetime import time as _time

        text = str(value).strip()
        try:
            parts = [int(p) for p in text.split(":")[:3]]
        except (TypeError, ValueError):
            return None
        while len(parts) < 3:
            parts.append(0)
        hour, minute, second = parts
        if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
            return None
        return _time(hour, minute, second)

    async def _owned_event(
        self, owner_id: str, event_id: str, owner_type: str
    ) -> NotifyEvent | None:
        eid = _maybe_uuid(event_id)
        if eid is None:
            return None
        event = await NotifyEvent.get_or_none(id=eid)
        if event is None or (event.owner_type or "platform") != owner_type:
            return None
        # Scope is the only thing separating a platform event from a tenant's,
        # so the owner id must match exactly — no privileged widening.
        if str(event.owner_id or "") != str(owner_id):
            return None
        return event

    async def _bind_channels(
        self, event: NotifyEvent, channel_ids: list, owner_id: str, owner_type: str
    ) -> list[str]:
        """Replace an event's channel bindings, keeping only owned channels."""
        bound: list[str] = []
        await NotifyEventChannel.filter(event_id=event.id).delete()
        for raw in channel_ids or []:
            cid = _maybe_uuid(raw)
            if cid is None:
                continue
            if await self._owned_channel(owner_id, str(cid), None, owner_type) is None:
                continue  # silently drop foreign channels rather than 500
            await NotifyEventChannel.create(event_id=event.id, channel_id=cid)
            bound.append(str(cid))
        return bound

    @staticmethod
    async def _channel_ids_for(event_ids: list) -> dict[str, list[str]]:
        """Bindings for many events in one query (no N+1 in the list view)."""
        if not event_ids:
            return {}
        rows = await NotifyEventChannel.filter(
            event_id__in=event_ids, enabled=True
        ).values("event_id", "channel_id")
        out: dict[str, list[str]] = {}
        for row in rows:
            out.setdefault(str(row["event_id"]), []).append(str(row["channel_id"]))
        return out

    async def list_events(self, owner_id: str, *, owner_type: str = "platform") -> list[dict]:
        query = NotifyEvent.filter(owner_type=owner_type)
        oid = _maybe_uuid(owner_id)
        query = query.filter(owner_id=oid) if oid else query.filter(owner_id__isnull=True)
        rows = await query.order_by("-created_at")
        bindings = await self._channel_ids_for([r.id for r in rows])
        return [_event_dict(r, bindings.get(str(r.id), [])) for r in rows]

    async def create_event(
        self, owner_id: str, payload: dict, *, owner_type: str = "platform"
    ) -> dict | None:
        event_type = str(payload.get("event_type") or "").strip()
        if not event_type or not self._event_allowed(event_type, owner_type):
            return None
        entry = next((e for e in NOTIFY_EVENT_TYPES if e["type"] == event_type), None)
        event = await NotifyEvent.create(
            owner_type=owner_type,
            owner_id=_maybe_uuid(owner_id),
            name=(str(payload.get("name") or "").strip() or (entry or {}).get("name") or event_type)[:128],
            event_type=event_type[:64],
            category=str(payload.get("category") or (entry or {}).get("category") or "general")[:32],
            status=("disabled" if str(payload.get("status") or "active") == "disabled" else "active"),
            effective_from=self._parse_dt(payload.get("effective_from")),
            effective_to=self._parse_dt(payload.get("effective_to")),
            daily_start=(self._parse_time(payload.get("daily_start")).isoformat() if self._parse_time(payload.get("daily_start")) else None),
            daily_end=(self._parse_time(payload.get("daily_end")).isoformat() if self._parse_time(payload.get("daily_end")) else None),
            trigger_condition=payload.get("trigger_condition") if isinstance(payload.get("trigger_condition"), dict) else {},
            event_params=payload.get("event_params") if isinstance(payload.get("event_params"), dict) else {},
        )
        bound = await self._bind_channels(event, payload.get("channel_ids") or [], owner_id, owner_type)
        return _event_dict(event, bound)

    async def update_event(
        self, owner_id: str, event_id: str, payload: dict, *, owner_type: str = "platform"
    ) -> bool:
        event = await self._owned_event(owner_id, event_id, owner_type)
        if event is None:
            return False
        if "event_type" in payload:
            et = str(payload["event_type"] or "").strip()
            if not et or not self._event_allowed(et, owner_type):
                return False
            event.event_type = et[:64]
        if "name" in payload:
            name = str(payload["name"] or "").strip()
            if name:
                event.name = name[:128]
        if "category" in payload:
            event.category = str(payload["category"] or "general")[:32]
        if "status" in payload:
            event.status = "disabled" if str(payload["status"]) == "disabled" else "active"
        for field in ("effective_from", "effective_to"):
            if field in payload:
                setattr(event, field, self._parse_dt(payload[field]))
        for field in ("daily_start", "daily_end"):
            if field in payload:
                parsed_time = self._parse_time(payload[field])
                setattr(event, field, parsed_time.isoformat() if parsed_time else None)
        for field in ("trigger_condition", "event_params"):
            if field in payload and isinstance(payload[field], dict):
                setattr(event, field, payload[field])
        await event.save()
        if "channel_ids" in payload:
            await self._bind_channels(event, payload.get("channel_ids") or [], owner_id, owner_type)
        return True

    async def delete_event(
        self, owner_id: str, event_id: str, *, owner_type: str = "platform"
    ) -> bool:
        event = await self._owned_event(owner_id, event_id, owner_type)
        if event is None:
            return False
        # Bindings and counter state are meaningless without the event; drop both
        # so a recreated event with the same params starts from a clean window.
        await NotifyEventChannel.filter(event_id=event.id).delete()
        await NotifyEventState.filter(event_id=event.id).delete()
        await event.delete()
        return True

    # ---- Subscription rules (legacy: superseded by configured events above) ----
    async def list_rules(self, owner_id: str, *, owner_type: str = "user") -> list[dict]:
        rows = await NotifySubscriptionRule.filter(
            owner_type=owner_type, owner_id=uuid.UUID(owner_id)
        ).order_by("-created_at")
        return [_rule_dict(r) for r in rows]

    async def create_rule(
        self, owner_id: str, payload: dict, *, owner_type: str = "user"
    ) -> dict | None:
        """Create one subscription rule after validating the channel belongs to
        this owner and the event type is allowed for the scope."""
        channel_id = _maybe_uuid(payload.get("channel_id"))
        event_type = str(payload.get("event_type") or "").strip()
        if channel_id is None or not event_type:
            return None
        if not self._event_allowed(event_type, owner_type):
            return None
        channel = await self._owned_channel(owner_id, str(channel_id), None, owner_type)
        if channel is None:
            return None
        rule = await NotifySubscriptionRule.create(
            owner_type=owner_type,
            owner_id=uuid.UUID(owner_id),
            channel_id=channel_id,
            event_type=event_type[:64],
            filters=payload.get("filters") if isinstance(payload.get("filters"), dict) else {},
            enabled=payload.get("enabled", True),
        )
        return _rule_dict(rule)

    async def update_rule(
        self, owner_id: str, rule_id: str, payload: dict, *, owner_type: str = "user"
    ) -> bool:
        rid = _maybe_uuid(rule_id)
        if rid is None:
            return False
        rule = await NotifySubscriptionRule.get_or_none(id=rid)
        if rule is None or rule.owner_type != owner_type or str(rule.owner_id) != str(owner_id):
            return False
        if "event_type" in payload:
            et = str(payload["event_type"] or "").strip()
            if not et or not self._event_allowed(et, owner_type):
                return False
            rule.event_type = et[:64]
        if "channel_id" in payload:
            cid = _maybe_uuid(payload.get("channel_id"))
            if cid is None or await self._owned_channel(owner_id, str(cid), None, owner_type) is None:
                return False
            rule.channel_id = cid
        if "filters" in payload and isinstance(payload["filters"], dict):
            rule.filters = payload["filters"]
        if "enabled" in payload:
            rule.enabled = bool(payload["enabled"])
        await rule.save()
        return True

    async def delete_rule(self, owner_id: str, rule_id: str, *, owner_type: str = "user") -> bool:
        rid = _maybe_uuid(rule_id)
        if rid is None:
            return False
        deleted = await NotifySubscriptionRule.filter(
            id=rid, owner_type=owner_type, owner_id=uuid.UUID(owner_id)
        ).delete()
        return bool(deleted)

    @staticmethod
    def _event_allowed(event_type: str, owner_type: str) -> bool:
        """Platform owners may subscribe to any event; user/team owners only to
        events flagged ``owner_scope='user'`` (task/node online)."""
        entry = next((e for e in NOTIFY_EVENT_TYPES if e["type"] == event_type), None)
        if entry is None:
            return False
        if owner_type == "platform":
            return True
        return entry.get("owner_scope") == "user"

    # ---- Mobile push devices (mobile_push channel kind) --------------------

    # Events enabled by default for a user's first registered phone. Deliberately
    # the terminal/attention-worthy ones — not task.created (you just tapped the
    # button, a push for it is noise).
    DEFAULT_PUSH_EVENT_TYPES = ("task.ended", "task.paused", "node.online")
    # Same set, bound to the in-app notify_center channel. This is the "simple
    # mode" default: even with no push device registered, these events still
    # land in the notifications table and the app polls them when open.
    DEFAULT_APP_EVENT_TYPES = ("task.ended", "task.paused", "node.online")

    async def ensure_default_app_notifications(self, user_id: str) -> None:
        """One-time provisioning of the user's in-app (notify_center) defaults.

        On the first call (no user-scope events of these types exist) this
        creates one ``NotifyEvent`` per default type and binds it to the user's
        auto-provisioned notify_center channel — so task terminal/pause and
        node-online events land in ``notifications`` even with no phone
        registered. Idempotent: once created, it never re-touches bindings, so
        a user who turned an event off in the settings screen stays off.
        """
        oid = _maybe_uuid(user_id)
        if oid is None:
            return
        channel = await self.ensure_notify_center_channel(user_id, "user")
        if channel is None:
            return
        for event_type in self.DEFAULT_APP_EVENT_TYPES:
            existing = await NotifyEvent.filter(
                owner_type="user", owner_id=oid, event_type=event_type
            ).first()
            if existing is not None:
                continue  # never re-subscribe what the user chose to manage
            event = await NotifyEvent.create(
                id=uuid.uuid4(),
                owner_type="user",
                owner_id=oid,
                name=next(
                    (e["name"] for e in NOTIFY_EVENT_TYPES if e["type"] == event_type),
                    event_type,
                ),
                event_type=event_type,
                category="task" if event_type.startswith("task") else "node",
                status="active",
            )
            await NotifyEventChannel.get_or_create(
                event_id=event.id, channel_id=channel.id, defaults={"enabled": True}
            )

    @staticmethod
    def _device_dict(d: NotifyDevice) -> dict:
        """Device DTO. The push token is sensitive — only a masked hint leaves
        this layer, mirroring the channel ``secret`` masking rule."""
        from .masking import mask_secret

        return {
            "id": str(d.id),
            "device_key": d.device_key,
            "name": d.name,
            "platform": d.platform,
            "push_provider": d.push_provider,
            "token_masked": mask_secret(d.push_token),
            "app_version": d.app_version,
            "last_seen_at": d.last_seen_at.isoformat() if d.last_seen_at else None,
            "enabled": d.enabled,
            "last_error": (d.last_error or "")[:200],
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }

    async def ensure_device_channel(
        self, owner_id: str, device: NotifyDevice
    ) -> NotifyChannel:
        """Return (creating on first register) the ``mobile_push`` channel bound
        to one device. The channel is what event bindings attach to; the device
        row holds the token/liveness state."""
        existing = await NotifyChannel.filter(
            owner_id=device.owner_id, owner_type="user",
            kind=MOBILE_PUSH_KIND, target_id=str(device.id),
        ).first()
        if existing is not None:
            return existing
        return await NotifyChannel.create(
            id=uuid.uuid4(),
            owner_id=device.owner_id,
            owner_type="user",
            name=(device.name or "我的手机")[:64],
            kind=MOBILE_PUSH_KIND,
            webhook_url="",
            secret="",
            target_id=str(device.id),
            enabled=True,
        )

    async def register_device(self, user_id: str, payload: dict) -> dict:
        """Idempotent register-or-update of the calling phone.

        ``device_key`` is the stable install identity the app keeps locally; the
        same key re-registering (token rotation, app upgrade) updates in place.
        A push token that moved to another account is re-bound to this one and
        the previous binding disabled (Expo delivers a token to exactly one
        device, so keeping two owners would leak this user's notifications).
        On a user's *first* device the default push rules are provisioned;
        later devices start with an empty binding set the user picks in the
        notification settings screen.
        """
        oid = _maybe_uuid(user_id)
        device_key = str(payload.get("device_key") or "").strip()[:128]
        push_token = str(payload.get("push_token") or "").strip()
        if oid is None or not device_key or not push_token:
            raise ValueError("device_key 和 push_token 必填")

        # Token exclusivity across accounts.
        previous = await NotifyDevice.filter(push_token=push_token).exclude(
            owner_id=oid
        )
        for stale in previous:
            stale.enabled = False
            stale.last_error = "token re-registered by another account"
            await stale.save()
            await NotifyChannel.filter(
                kind=MOBILE_PUSH_KIND, target_id=str(stale.id)
            ).update(enabled=False)

        was_first = not await NotifyDevice.filter(owner_id=oid).exists()
        device = await NotifyDevice.get_or_none(owner_id=oid, device_key=device_key)
        if device is None:
            device = await NotifyDevice.create(
                id=uuid.uuid4(),
                owner_id=oid,
                device_key=device_key,
                push_token=push_token,
            )
        else:
            device.push_token = push_token
            device.enabled = True
            device.last_error = ""
        device.name = str(payload.get("name") or payload.get("default_name") or "我的手机")[:128]
        device.platform = str(payload.get("platform") or "")[:16]
        device.push_provider = str(payload.get("push_provider") or "expo")[:16]
        device.app_version = str(payload.get("app_version") or "")[:32]
        device.last_seen_at = _utcnow()
        await device.save()

        channel = await self.ensure_device_channel(user_id, device)
        if channel.name != device.name:
            channel.name = device.name
            await channel.save()
        if was_first:
            await self._ensure_default_push_rules(user_id, channel)
        return {
            "device": self._device_dict(device),
            "channel_id": str(channel.id),
            "defaults_applied": was_first,
        }

    async def _ensure_default_push_rules(self, user_id: str, channel: NotifyChannel) -> None:
        """Provision default event→channel bindings for a user's first phone.

        One ``NotifyEvent`` per default event type (owner-scoped to the user),
        each bound to the device's channel. Idempotent: an event of this type
        already owned by the user is reused (its bindings are NOT touched, so a
        user who deleted the default binding isn't re-subscribed on next login).
        """
        oid = _maybe_uuid(user_id)
        if oid is None:
            return
        for event_type in self.DEFAULT_PUSH_EVENT_TYPES:
            event = await NotifyEvent.filter(
                owner_type="user", owner_id=oid, event_type=event_type
            ).first()
            if event is None:
                event = await NotifyEvent.create(
                    id=uuid.uuid4(),
                    owner_type="user",
                    owner_id=oid,
                    name=next(
                        (e["name"] for e in NOTIFY_EVENT_TYPES if e["type"] == event_type),
                        event_type,
                    ),
                    event_type=event_type,
                    category="task" if event_type.startswith("task") else "node",
                    status="active",
                )
            await NotifyEventChannel.get_or_create(
                event_id=event.id, channel_id=channel.id, defaults={"enabled": True}
            )

    async def list_devices(self, user_id: str) -> list[dict]:
        oid = _maybe_uuid(user_id)
        if oid is None:
            return []
        rows = await NotifyDevice.filter(owner_id=oid).order_by("-last_seen_at")
        return [self._device_dict(d) for d in rows]

    async def _owned_device(self, user_id: str, device_id: str) -> NotifyDevice | None:
        did = _maybe_uuid(device_id)
        if did is None:
            return None
        return await NotifyDevice.get_or_none(id=did, owner_id=_maybe_uuid(user_id))

    async def get_device_token(self, user_id: str, device_id: str) -> str:
        """Raw push token for the test-push endpoint — owner-scoped, never
        returned to the client by anything else."""
        device = await self._owned_device(user_id, device_id)
        if device is None:
            return ""
        return (device.push_token or "").strip()

    async def heartbeat_device(
        self, user_id: str, device_id: str, payload: dict
    ) -> dict | None:
        device = await self._owned_device(user_id, device_id)
        if device is None:
            return None
        token = str(payload.get("push_token") or "").strip()
        if token and token != device.push_token:
            device.push_token = token
            device.enabled = True
            device.last_error = ""
        device.app_version = str(payload.get("app_version") or device.app_version or "")[:32]
        device.last_seen_at = _utcnow()
        await device.save()
        return self._device_dict(device)

    async def update_device(self, user_id: str, device_id: str, payload: dict) -> dict | None:
        device = await self._owned_device(user_id, device_id)
        if device is None:
            return None
        if "name" in payload:
            device.name = str(payload.get("name") or "我的手机")[:128]
        if "enabled" in payload:
            device.enabled = bool(payload.get("enabled"))
        device.last_seen_at = _utcnow()
        await device.save()
        # Keep the bound channel's name/enabled in lockstep so the event editor
        # (which lists channels) never shows a device that cannot deliver.
        await NotifyChannel.filter(
            kind=MOBILE_PUSH_KIND, target_id=str(device.id)
        ).update(enabled=device.enabled, name=device.name, updated_at=_utcnow())
        return self._device_dict(device)

    async def delete_device(self, user_id: str, device_id: str) -> bool:
        """Revoke one phone: device + channel + event bindings all go away."""
        device = await self._owned_device(user_id, device_id)
        if device is None:
            return False
        channels = await NotifyChannel.filter(
            owner_id=device.owner_id, kind=MOBILE_PUSH_KIND, target_id=str(device.id)
        )
        for channel in channels:
            await NotifyEventChannel.filter(channel_id=channel.id).delete()
            await channel.delete()
        await device.delete()
        return True

    async def list_push_rules(self, user_id: str) -> dict:
        """Event catalogue + each event's in-app state and mobile-device bindings.

        ``app_enabled`` is the notify_center binding (on = the event lands in
        the app's notification history; the simple-mode default).
        ``devices`` is the per-device mobile_push binding set (empty when remote
        push is off or no device is targeted). Only ``owner_scope='user'`` events
        are offered (platform events never reach a phone or the user history).
        """
        await self.ensure_default_app_notifications(user_id)
        oid = _maybe_uuid(user_id)
        if oid is None:
            return {"events": []}
        center = await self.ensure_notify_center_channel(user_id, "user")

        # All user-scope channels: notify_center (in-app) + mobile_push (devices).
        channels = await NotifyChannel.filter(
            owner_id=oid, owner_type="user"
        ).all()
        center_id = center.id if center else None
        device_channels = [c for c in channels if c.kind == MOBILE_PUSH_KIND]
        device_by_channel = {str(c.id): c for c in device_channels}

        bindings = await NotifyEventChannel.filter(
            channel_id__in=[c.id for c in channels], enabled=True
        ).all()
        event_ids = {b.event_id for b in bindings}
        events = await NotifyEvent.filter(id__in=list(event_ids)).all() if event_ids else []
        event_type_by_id = {e.id: e.event_type for e in events}

        app_on: set[str] = set()
        device_bindings: dict[str, list[dict]] = {}
        for b in bindings:
            event_type = event_type_by_id.get(b.event_id)
            if not event_type:
                continue
            if center_id is not None and b.channel_id == center_id:
                app_on.add(event_type)
            else:
                ch = device_by_channel.get(str(b.channel_id))
                if ch is None:
                    continue
                device_bindings.setdefault(event_type, []).append({
                    "id": ch.target_id or "",
                    "name": ch.name,
                    "channel_id": str(ch.id),
                })

        catalogue = [
            {"type": e["type"], "name": e["name"], "category": e["category"]}
            for e in NOTIFY_EVENT_TYPES
            if e.get("owner_scope") == "user"
        ]
        return {
            "events": [
                {**entry, "app_enabled": entry["type"] in app_on,
                 "devices": device_bindings.get(entry["type"], [])}
                for entry in catalogue
            ],
            "devices": [
                {"id": c.target_id or "", "name": c.name, "channel_id": str(c.id),
                 "enabled": c.enabled}
                for c in device_channels
            ],
        }

    async def replace_push_rules(self, user_id: str, payload: dict) -> dict:
        """Replace the user's event bindings: in-app (notify_center) toggle +
        per-device mobile_push targeting.

        ``payload`` shape::

            {"app": {event_type: bool}, "bindings": {event_type: [device_id]}}

        Both maps are optional; only the ones present are rewritten. An event
        turned on in ``app`` lands in the in-app history; turned off stops
        recording new rows. ``bindings`` controls mobile_push targeting and is
        only meaningful once a phone is registered (remote push). Device ids
        are validated against the caller's own devices; event types against the
        user-scope catalogue.
        """
        oid = _maybe_uuid(user_id)
        if oid is None:
            raise ValueError("user id 无效")
        app_map = payload.get("app") if isinstance(payload, dict) else None
        bindings = payload.get("bindings") if isinstance(payload, dict) else None

        channels = await NotifyChannel.filter(
            owner_id=oid, owner_type="user"
        ).all()
        center = await self.ensure_notify_center_channel(user_id, "user")
        center_id = center.id if center else None
        device_channels = [c for c in channels if c.kind == MOBILE_PUSH_KIND]
        push_channel_ids = {c.id for c in device_channels}
        devices = await NotifyDevice.filter(owner_id=oid).all()
        device_ids = {d.id for d in devices}
        channel_by_target = {c.target_id: c for c in device_channels if c.target_id}

        # Collect the full set of event types touched by either map.
        wanted_types: set[str] = set()
        if isinstance(app_map, dict):
            wanted_types.update(k for k in app_map if self._event_allowed(str(k), "user"))
        if isinstance(bindings, dict):
            wanted_types.update(k for k in bindings if self._event_allowed(str(k), "user"))

        # Lazily create the missing event rows (the worker matches on the event
        # definition row — a binding without an event row never fires).
        existing = await NotifyEvent.filter(
            owner_type="user", owner_id=oid, event_type__in=list(wanted_types),
        ).all() if wanted_types else []
        event_by_type = {e.event_type: e for e in existing}
        for event_type in wanted_types:
            if event_type in event_by_type:
                continue
            event_by_type[event_type] = await NotifyEvent.create(
                id=uuid.uuid4(),
                owner_type="user",
                owner_id=oid,
                name=next(
                    (e["name"] for e in NOTIFY_EVENT_TYPES if e["type"] == event_type),
                    event_type,
                ),
                event_type=event_type,
                category="task" if event_type.startswith("task") else "node",
                status="active",
            )

        # Rewrite the app (notify_center) bindings for events in the app map.
        if isinstance(app_map, dict) and center_id is not None:
            existing_app = await NotifyEventChannel.filter(
                channel_id=center_id,
                event_id__in=[e.id for e in event_by_type.values()],
            ).all() if event_by_type else []
            have_by_event = {b.event_id: b for b in existing_app}
            for event_type, raw_flag in app_map.items():
                if not self._event_allowed(str(event_type), "user"):
                    continue
                event = event_by_type[event_type]
                want = bool(raw_flag)
                existing_binding = have_by_event.get(event.id)
                if want and existing_binding is None:
                    await NotifyEventChannel.create(
                        event_id=event.id, channel_id=center_id, enabled=True,
                    )
                elif not want and existing_binding is not None:
                    await existing_binding.delete()

        # Rewrite mobile_push device bindings for events in the bindings map.
        if isinstance(bindings, dict):
            # Drop all existing mobile-push bindings, then write the wanted set.
            old_push = await NotifyEventChannel.filter(
                channel_id__in=list(push_channel_ids)
            ).all() if push_channel_ids else []
            for b in old_push:
                await b.delete()
            for event_type, raw_ids in bindings.items():
                if not self._event_allowed(str(event_type), "user"):
                    continue
                event = event_by_type[event_type]
                for raw_id in raw_ids or []:
                    did = _maybe_uuid(raw_id)
                    if did is None or did not in device_ids:
                        continue
                    ch = channel_by_target.get(str(did))
                    if ch is None:
                        continue
                    await NotifyEventChannel.get_or_create(
                        event_id=event.id, channel_id=ch.id, defaults={"enabled": True}
                    )

        return await self.list_push_rules(user_id)


notify_service = NotifyService()
