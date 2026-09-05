"""Tortoise models for the project webhook domain (``mc_*`` tables only).

A ``ProjectWebhook`` stores the platform-side webhook we created on a project's
repository so the platform can later call us back (e.g. for automatic review).
It is storage only: multi-user isolation and access control are enforced in
``webhook_service`` via ``project_service``. The ``secret`` field is a
verification secret and must be masked before leaving the service layer.
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class ProjectWebhook(Model):
    """A webhook registered on a project's repository for a Git platform.

    One row per (project, platform). ``hook_id`` is the id the platform
    returned when the webhook was created (string — GitLab uses numeric ids,
    Gitea/Gitee too, but keeping it textual avoids any cross-platform surprises).
    ``secret`` is the shared secret used to verify inbound deliveries; it is
    never returned in plaintext.
    """

    id = fields.UUIDField(pk=True)
    project_id = fields.UUIDField()
    user_id = fields.UUIDField()
    platform = fields.CharField(max_length=32)  # GitPlatform: github/gitlab/gitea/gitee
    full_name = fields.CharField(max_length=512)  # owner/repo
    hook_id = fields.CharField(max_length=255, null=True)  # platform-side webhook id
    callback_url = fields.TextField()
    secret = fields.CharField(max_length=255, null=True)  # masked in responses
    events = fields.JSONField(default=list)  # ["push", "pull_request", ...]
    active = fields.BooleanField(default=True)
    # Review execution is pluggable. First phase implements only
    # ``claude_delegate``; the canonical config below selects provider + nodes.
    review_executor = fields.CharField(max_length=64, default="claude_delegate")
    # Review methodology (see review_executors.REVIEW_FRAMEWORKS), independent
    # from the provider CLI that performs the reasoning.
    review_framework = fields.CharField(max_length=64, default="open_code_review_delegate")
    # Legacy editor-template selection. Kept only so deployed rows/clients can
    # be lazily drained into review_provider/review_node_ids during the rollout.
    review_editor_ids = fields.JSONField(default=list)
    review_auto = fields.BooleanField(default=False)
    # Canonical review execution config. New rows select a provider and execution
    # nodes directly; ``review_editor_ids`` remains only as a lazy-drain source for
    # rows created before Task became the sole work unit.
    review_provider = fields.CharField(max_length=32, null=True)
    review_node_ids = fields.JSONField(default=list)
    review_skill_config = fields.JSONField(default=list)
    review_mcp_config = fields.JSONField(default=list)
    review_plugin_config = fields.JSONField(default=list)
    # Review-specific model + limits. Empty model lets the provider use its default.
    review_model_id = fields.CharField(max_length=128, null=True)
    # Per-review runtime prompt. Resolved to its content at dispatch time and
    # prepended to the review task content so the provider CLI actually sees it
    # (review sessions have no editor workspace CLAUDE.md/AGENTS.md write step).
    review_prompt_id = fields.CharField(max_length=255, null=True)
    # Per-review rate-limit overlay (rpm/tpm/concurrent/max_ips …) applied to
    # the task-scoped child key. Independent from usage_limit (quota); not
    # narrowed against the parent — rate_limit is per-key throttling.
    review_rate_limit = fields.JSONField(default=dict)
    # Per-review child-key expiry (epoch seconds). Null = inherit parent's.
    review_expires_at = fields.BigIntField(null=True)
    # Parent API key from which the canonical Review Task mints its task-scoped key.
    review_api_key_id = fields.IntField(null=True)
    review_model_limits = fields.JSONField(default=dict)
    # When true, superseding queued events for the same PR keeps only the latest.
    review_latest_only = fields.BooleanField(default=False)
    review_enabled = fields.BooleanField(default=True)
    last_error = fields.TextField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_project_webhooks"
        unique_together = (("project_id", "platform"),)
        indexes = (("project_id",), ("user_id",))
