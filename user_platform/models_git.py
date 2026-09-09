"""Tortoise models for the git domain (``mc_*`` tables only).

Mirror the upstream ent schema for git identities, bots and git tasks.
Storage only; multi-user isolation is enforced by ``user_id`` in the service
layer. Secret fields (access_token / oauth_refresh_token / secret_token) are
NEVER returned in API responses — the service layer masks them.

Enums (from upstream consts):
* GitPlatform: github | gitlab | gitea | gitee | codeup | cnb | atomgit
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class GitIdentity(Model):
    """A git credential/identity owned by a platform user.

    ``access_token`` / ``oauth_refresh_token`` are secrets and must be masked
    before leaving the service layer.
    """

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    platform = fields.CharField(max_length=32)  # GitPlatform
    base_url = fields.CharField(max_length=512, null=True)
    access_token = fields.CharField(max_length=1024, null=True)
    username = fields.CharField(max_length=255, null=True)
    email = fields.CharField(max_length=255, null=True)
    installation_id = fields.BigIntField(null=True)
    organization_id = fields.CharField(max_length=255, null=True)
    remark = fields.CharField(max_length=255, null=True)
    oauth_refresh_token = fields.CharField(max_length=1024, null=True)
    oauth_expires_at = fields.DatetimeField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_git_identities"
        indexes = (("user_id",), ("platform",))


class GitBot(Model):
    """A git bot bound to a host, owned by a platform user.

    ``token`` / ``secret_token`` are secrets and must be masked before leaving
    the service layer.
    """

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    name = fields.CharField(max_length=255, null=True)
    host_id = fields.CharField(max_length=64)
    token = fields.CharField(max_length=1024, null=True)
    secret_token = fields.CharField(max_length=1024, null=True)
    platform = fields.CharField(max_length=32)  # GitPlatform
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_git_bots"
        indexes = (("user_id",), ("host_id",))


class GitBotUser(Model):
    """Sharing binding: a git bot shared with an additional user."""

    id = fields.UUIDField(pk=True)
    git_bot_id = fields.UUIDField()
    user_id = fields.UUIDField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_git_bot_users"
        unique_together = (("git_bot_id", "user_id"),)
        indexes = (("git_bot_id",), ("user_id",))


class GitTask(Model):
    """A git-triggered task binding (issue/PR subject -> task)."""

    id = fields.UUIDField(pk=True)
    task_id = fields.UUIDField()
    repo_id = fields.UUIDField(null=True)
    subject_type = fields.CharField(max_length=64)
    subject_id = fields.CharField(max_length=255, null=True)
    subject_number = fields.IntField(null=True)
    subject_url = fields.CharField(max_length=1024, null=True)
    subject_title = fields.CharField(max_length=1024, null=True)
    prompt_id = fields.CharField(max_length=255, null=True)
    show_url = fields.TextField(null=True)
    github_installation_id = fields.BigIntField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_git_tasks"
        indexes = (("task_id",),)


class ProjectGitBot(Model):
    """Binding of a git bot to a project."""

    id = fields.UUIDField(pk=True)
    project_id = fields.UUIDField()
    git_bot_id = fields.UUIDField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_project_git_bots"
        unique_together = (("project_id", "git_bot_id"),)
        indexes = (("project_id",), ("git_bot_id",))
