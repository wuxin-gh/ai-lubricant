"""Persisted webhook deliveries and review results (``mc_*`` tables only)."""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class ProjectWebhookEvent(Model):
    """One normalized, idempotent delivery from a project webhook."""

    id = fields.UUIDField(pk=True)
    project_id = fields.UUIDField()
    webhook_id = fields.UUIDField()
    platform = fields.CharField(max_length=32)
    delivery_id = fields.CharField(max_length=255)
    event_type = fields.CharField(max_length=64)
    payload = fields.JSONField(default=dict)
    commit_sha = fields.CharField(max_length=255, null=True)
    before_sha = fields.CharField(max_length=255, null=True)
    base_ref = fields.CharField(max_length=255, null=True)
    head_ref = fields.CharField(max_length=255, null=True)
    pr_number = fields.CharField(max_length=64, null=True)
    source_branch = fields.CharField(max_length=255, null=True)
    target_branch = fields.CharField(max_length=255, null=True)
    status = fields.CharField(max_length=32, default="pending")
    task_id = fields.UUIDField(null=True)
    attempts = fields.IntField(default=0)
    last_error = fields.TextField(null=True)
    next_attempt_at = fields.DatetimeField(null=True)
    findings = fields.JSONField(default=list)
    review_summary = fields.TextField(null=True)
    review_completed_at = fields.DatetimeField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_project_webhook_events"
        unique_together = (("webhook_id", "delivery_id"),)
        indexes = (
            ("project_id", "created_at"),
            ("status", "next_attempt_at"),
            ("task_id",),
        )
