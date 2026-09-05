"""Tortoise models for the compatibility namespace (``mc_*`` tables only).

These tables are intentionally separate from the main service schema:

* They never shadow ``users``, ``request_logs``, ``api_keys``, ``provider_*``
  or ``model_*`` from the main service.
* They only store compatibility/audit metadata required by the optional
  MonkeyCode platform surface.
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model

from .config import settings


class SystemUser(Model):
    """Single deterministic placeholder user for legacy resources.

    Used only as an ownership anchor for requests/resources that have no real
    platform session. It is never a real API key owner and never participates
    in the model request pipeline.
    """

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    email = fields.CharField(max_length=255, null=True)
    role = fields.CharField(max_length=32, default="system")
    status = fields.CharField(max_length=32, default="active")
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_system_users"

    @classmethod
    def fixed_id(cls) -> str:
        return settings.system_user_id


# ---------------------------------------------------------------------------
# Phase A: multi-user authentication domain (C-side platform users).
#
# These tables back the MonkeyCode user-facing platform. They are fully
# independent from the main service's single admin token and from the main
# ``api_keys``/``users`` tables. Field shapes intentionally mirror the
# MonkeyCode ent schema so ported handlers behave identically.
#
# Roles (consts.UserRole): individual | subaccount | admin | gittask
# Status (consts.UserStatus): active | inactive | banded
# Platform (consts.UserPlatform): baizhi | google | github | gitlab | gitea | gitee | oidc
# ---------------------------------------------------------------------------


class User(Model):
    """Platform (C-side) end user. Independent of the admin token."""

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    email = fields.CharField(max_length=255, null=True)
    avatar_url = fields.CharField(max_length=512, null=True)
    # bcrypt hash (passlib), byte-compatible with MonkeyCode's crypto.HashPassword.
    password = fields.CharField(max_length=255, null=True)
    role = fields.CharField(max_length=32, default="individual")
    status = fields.CharField(max_length=32, default="active")
    is_blocked = fields.BooleanField(default=False)
    default_configs = fields.JSONField(null=True)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_users"
        indexes = (("email",), ("status",))


class UserIdentity(Model):
    """Third-party identity binding (OAuth/OIDC) for a platform user."""

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    platform = fields.CharField(max_length=32)
    identity_id = fields.CharField(max_length=255)
    username = fields.CharField(max_length=255)
    email = fields.CharField(max_length=255, null=True)
    avatar_url = fields.CharField(max_length=512, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True, null=True)

    class Meta:
        table = "mc_user_identities"
        unique_together = (("platform", "identity_id"),)
        indexes = (("user_id",),)


class Team(Model):
    """Team/tenant grouping for platform users."""

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    member_limit = fields.IntField(default=0)
    task_concurrency_limit = fields.IntField(default=3)
    task_vm_sleep_enabled = fields.BooleanField(default=True)
    task_vm_sleep_seconds = fields.IntField(default=0)
    task_vm_recycle_enabled = fields.BooleanField(default=True)
    task_vm_recycle_seconds = fields.IntField(default=0)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_teams"


class TeamMember(Model):
    """User membership + role within a team. Role: admin | user."""

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    user_id = fields.UUIDField()
    role = fields.CharField(max_length=32, default="user")
    last_active_at = fields.DatetimeField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_team_members"
        unique_together = (("team_id", "user_id"),)
        indexes = (("user_id",), ("team_id",))


class TeamGroup(Model):
    """Sub-group inside a team."""

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    name = fields.CharField(max_length=255)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_team_groups"
        indexes = (("team_id",),)


class TeamGroupMember(Model):
    """User membership inside a team group."""

    id = fields.UUIDField(pk=True)
    group_id = fields.UUIDField()
    user_id = fields.UUIDField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_team_group_members"
        unique_together = (("group_id", "user_id"),)
        indexes = (("user_id",), ("group_id",))
