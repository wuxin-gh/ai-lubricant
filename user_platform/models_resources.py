"""Tortoise models for durable team resource references and grants."""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class ResourceReference(Model):
    """A market resource explicitly referenced by a team."""

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    resource_type = fields.CharField(max_length=32)
    market_module = fields.CharField(max_length=32)
    market_id = fields.CharField(max_length=200)
    name = fields.CharField(max_length=255)
    display_name = fields.CharField(max_length=255, null=True)
    version = fields.CharField(max_length=64, default="")
    manifest = fields.JSONField(default=dict)
    owned_entity_type = fields.CharField(max_length=32, null=True)
    owned_entity_id = fields.CharField(max_length=255, null=True)
    status = fields.CharField(max_length=16, default="active")
    created_by = fields.UUIDField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_resource_references"
        unique_together = (("team_id", "resource_type", "market_id"),)
        indexes = (("team_id", "resource_type"), ("market_module", "market_id"))


class ResourceGrant(Model):
    """Grant one referenced resource to a team group."""

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    group_id = fields.UUIDField()
    resource_id = fields.UUIDField()
    created_by = fields.UUIDField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_resource_grants"
        unique_together = (("resource_id", "group_id"),)
        indexes = (("team_id", "group_id"), ("resource_id",))
