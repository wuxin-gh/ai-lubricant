"""Tortoise models for the project domain (``mc_*`` tables only).

Mirror the MonkeyCode ent schema for projects, issues, collaborators and issue
comments. Storage only; multi-user isolation is enforced by ``user_id`` in the
service layer.

Enums (from MonkeyCode consts):
* ProjectIssueType:             requirement | bug
* ProjectIssueStatus:           requirement and bug use separate state flows
* ProjectIssuePriority:         1 | 2 | 3   (default 2)
* ProjectCollaboratorRole:       read_only | read_write
* ProjectAssociation.relation:   related | depends_on | ...
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class Project(Model):
    """A code-repository project owned by a platform user."""

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    platform = fields.CharField(max_length=32, null=True)  # GitPlatform
    repo_url = fields.CharField(max_length=512, null=True)
    branch = fields.CharField(max_length=255, null=True)
    git_identity_id = fields.UUIDField(null=True)
    env_variables = fields.JSONField(null=True)
    # 技术栈识别结果（规则驱动，无 LLM；stack_detector.detect_stack 的输出）。
    # null = 尚未扫描/扫描失败；扫描由建项目后异步触发，亦可手动重扫。
    stack_profile = fields.JSONField(null=True)
    # 团队共享：true 时同 team 成员可访问（read_write）；false 仅 owner + 显式协作者。
    is_team_shared = fields.BooleanField(default=False)
    team_id = fields.UUIDField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_projects"
        indexes = (("user_id",), ("team_id",))


class ProjectIssue(Model):
    """An issue inside a project (requirement/design/task tracking)."""

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    project_id = fields.UUIDField()
    issue_type = fields.CharField(max_length=16, default="requirement", source_field="type")
    status = fields.CharField(max_length=32, default="unassigned")
    title = fields.TextField()
    requirement_document = fields.TextField(null=True)
    design_document = fields.TextField(null=True)
    bug_reason = fields.TextField(null=True)
    pending_items = fields.JSONField(default=list)
    resolution_note = fields.TextField(null=True)
    summary = fields.TextField(null=True)
    assignee_id = fields.UUIDField(null=True)
    priority = fields.IntField(default=2)  # ProjectIssuePriority
    # 自由字符串标签列表（对标 TeamSkill.tags 的 free-form 形态，不做独立标签表）
    tags = fields.JSONField(default=list)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)
    closed_at = fields.DatetimeField(null=True)

    class Meta:
        table = "mc_project_issues"
        indexes = (("project_id",), ("user_id",), ("assignee_id",))


class ProjectCollaborator(Model):
    """A collaborator binding between a user and a project."""

    id = fields.UUIDField(pk=True)
    project_id = fields.UUIDField()
    user_id = fields.UUIDField()
    role = fields.CharField(max_length=32, default="read_only")  # ProjectCollaboratorRole
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_project_collaborators"
        unique_together = (("project_id", "user_id"),)
        indexes = (("project_id",), ("user_id",))


class ProjectAssociation(Model):
    """A directed association between two projects (A references B).

    Storage only. The association itself grants no access — repo-level
    permission is decided by whether the source project's git identity token
    can actually reach the target repository (see project_service).
    """

    id = fields.UUIDField(pk=True)
    source_project_id = fields.UUIDField()
    target_project_id = fields.UUIDField()
    relation = fields.CharField(max_length=32, default="related")  # related | depends_on | ...
    # Optional workspace subdirectory for the node-side clone; null falls back
    # to the target project's name.
    target_subdir = fields.CharField(max_length=128, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_project_associations"
        unique_together = (("source_project_id", "target_project_id"),)
        indexes = (("source_project_id",), ("target_project_id",))


class ProjectIssueComment(Model):
    """A comment (with optional threading) on a project issue."""

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    issue_id = fields.UUIDField()
    parent_id = fields.UUIDField(null=True)
    comment = fields.TextField()
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_project_issue_comments"
        indexes = (("issue_id",), ("user_id",))
