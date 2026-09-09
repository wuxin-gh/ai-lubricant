"""Tortoise models for the notify domain (``mc_*`` tables only).

Mirror the upstream ent schema for notification channels, subscriptions and
send logs. Storage only; multi-user isolation is enforced by ``owner_id`` in
the service layer. The channel ``secret`` is masked before leaving the service.

Enums (from upstream consts):
* NotifyChannelKind: dingtalk | feishu | wecom | webhook | wechat_mp
* NotifyOwnerType:   user | team | platform
* NotifyEventType:   task.created | task.ended | vm.expiring_soon |
                     quota.refreshed | quota.basic_exhausted |
                     quota.pro_exhausted | quota.ultra_exhausted
* NotifySendStatus:  (success | failed | ... — stored as free text)
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class NotifyChannel(Model):
    """A notification channel (webhook/IM/wechat_mp) owned by a user or team.

    ``secret`` is sensitive and must be masked before leaving the service.
    """

    id = fields.UUIDField(pk=True)
    owner_id = fields.UUIDField()
    owner_type = fields.CharField(max_length=16, default="user")  # NotifyOwnerType
    name = fields.CharField(max_length=64)
    kind = fields.CharField(max_length=32)  # NotifyChannelKind
    webhook_url = fields.TextField(default="")
    secret = fields.TextField(default="")
    headers = fields.JSONField(null=True)
    metadata = fields.JSONField(null=True)
    target_id = fields.CharField(max_length=255, default="")
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_notify_channels"
        indexes = (("owner_id", "owner_type"),)


class NotifySubscription(Model):
    """A subscription binding a channel to a set of event types."""

    id = fields.UUIDField(pk=True)
    channel_id = fields.UUIDField()
    scope = fields.CharField(max_length=32, default="self")
    event_types = fields.JSONField(default=list)
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_notify_subscriptions"
        indexes = (("channel_id",),)


class NotifySendLog(Model):
    """A record of a notification send attempt."""

    id = fields.UUIDField(pk=True)
    subscription_id = fields.UUIDField()
    channel_id = fields.UUIDField()
    event_type = fields.CharField(max_length=64)  # NotifyEventType
    event_ref_id = fields.CharField(max_length=255, default="")
    status = fields.CharField(max_length=32)  # NotifySendStatus
    error = fields.TextField(default="")
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_notify_send_logs"
        indexes = (("subscription_id",), ("status", "created_at"))


class NotifySubscriptionRule(Model):
    """A parameterized notification subscription owned by a user or team."""

    id = fields.UUIDField(pk=True)
    owner_type = fields.CharField(max_length=16, default="user")
    owner_id = fields.UUIDField()
    channel_id = fields.UUIDField()
    event_type = fields.CharField(max_length=64)
    filters = fields.JSONField(default=dict)
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_notify_subscription_rules"
        indexes = (("owner_type", "owner_id"), ("event_type",))


class NotifyOutbox(Model):
    """An outbound notification awaiting delivery.

    Written by ``emit_notification`` at every hook site, drained by the notify
    worker. Persisted so a process restart does not lose pending pushes: the
    worker re-scans ``status='pending'`` rows on startup before flipping
    ``_initialized`` and accepting new items into the in-memory queue.
    """

    id = fields.UUIDField(pk=True)
    notification_id = fields.BigIntField(null=True)  # FK notifications.id (not a Tortoise model)
    event_type = fields.CharField(max_length=64)
    params = fields.JSONField(default=dict)
    owner_type = fields.CharField(max_length=16, default="platform")
    owner_id = fields.UUIDField(null=True)
    status = fields.CharField(max_length=16, default="pending")  # pending/pushing/success/failed
    attempt_count = fields.IntField(default=0)
    last_error = fields.TextField(default="")
    created_at = fields.DatetimeField(auto_now_add=True)
    started_at = fields.DatetimeField(null=True)
    pushed_at = fields.DatetimeField(null=True)

    class Meta:
        table = "mc_notify_outbox"
        indexes = (("status", "created_at"),)


class NotifyEvent(Model):
    """A configured event definition, independent from outbound channels."""

    id = fields.UUIDField(pk=True)
    owner_type = fields.CharField(max_length=16, default="platform")
    owner_id = fields.UUIDField(null=True)
    name = fields.CharField(max_length=128)
    event_type = fields.CharField(max_length=64)
    category = fields.CharField(max_length=32, default="general")
    status = fields.CharField(max_length=16, default="active")  # active/disabled
    effective_from = fields.DatetimeField(null=True)
    effective_to = fields.DatetimeField(null=True)
    daily_start = fields.CharField(max_length=8, null=True)  # HH:MM:SS
    daily_end = fields.CharField(max_length=8, null=True)  # HH:MM:SS
    trigger_condition = fields.JSONField(default=dict)
    event_params = fields.JSONField(default=dict)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_notify_events"
        indexes = (("owner_type", "owner_id"), ("event_type", "status"))


class NotifyEventChannel(Model):
    """Many-to-many binding between an event definition and a channel."""

    id = fields.UUIDField(pk=True)
    event_id = fields.UUIDField()
    channel_id = fields.UUIDField()
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_notify_event_channels"
        unique_together = (("event_id", "channel_id"),)
        indexes = (("event_id",), ("channel_id",))


class NotifyEventState(Model):
    """Runtime counter/window state for count and silence conditions."""

    id = fields.UUIDField(pk=True)
    event_id = fields.UUIDField()
    fingerprint = fields.CharField(max_length=255)
    window_started_at = fields.DatetimeField(null=True)
    count = fields.IntField(default=0)
    last_triggered_at = fields.DatetimeField(null=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_notify_event_states"
        unique_together = (("event_id", "fingerprint"),)
        indexes = (("event_id",),)


class NotifyDevice(Model):
    """A registered mobile device that can receive push notifications.

    One user may bind multiple phones; each phone registers with a stable
    ``device_key`` (generated once per install, kept in the app's local storage).
    ``push_token`` is the Expo push token — sensitive: it is never returned by
    the listing API (only a hint is) and never written to logs in plaintext.

    The notify channel row (``NotifyChannel.kind='mobile_push'``,
    ``target_id`` = device id) is what events bind to; this table is the
    device-side state (token, platform, liveness, last delivery error).
    """

    id = fields.UUIDField(pk=True)
    owner_id = fields.UUIDField()
    device_key = fields.CharField(max_length=128)
    name = fields.CharField(max_length=128, default="")
    platform = fields.CharField(max_length=16, default="")  # android | ios
    push_provider = fields.CharField(max_length=16, default="expo")
    push_token = fields.TextField(default="")
    app_version = fields.CharField(max_length=32, default="")
    last_seen_at = fields.DatetimeField(null=True)
    enabled = fields.BooleanField(default=True)
    last_error = fields.TextField(default="")
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_notify_devices"
        unique_together = (("owner_id", "device_key"),)
        indexes = (("owner_id",), ("enabled", "last_seen_at"))
