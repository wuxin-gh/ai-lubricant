"""Tortoise models for the agent skill/plugin/rule domain (``mc_*`` tables only).

Mirror the upstream ent schema for agent resources (skills, plugins, rules)
with their version + repo entities. These resources are synced from git repos,
activated by version, and delivered by scope (global/team/user).

Storage only; scope isolation (global/team/user) is enforced in the service
layer. The actual git-sync worker + object-storage packaging live outside this
repo (agent-compose / agentresource) and are not implemented here.

Enums (from upstream consts / ent):
* scope_type:  global | team | user
* source_type: github | upload | bare | npm (plugin repos also allow npm)
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


# ---- Skills ---------------------------------------------------------------
class AgentSkillRepo(Model):
    """A source repo that skills are synced from."""

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    scope_type = fields.CharField(max_length=16, default="global")
    scope_id = fields.CharField(max_length=64, default="global")
    created_by = fields.UUIDField()
    source_type = fields.CharField(max_length=16)  # github | upload | bare
    github_url = fields.CharField(max_length=512, null=True)
    ref_type = fields.CharField(max_length=16, null=True)  # branch | tag | commit
    ref_value = fields.CharField(max_length=255, null=True)
    last_upload_filename = fields.CharField(max_length=512, null=True)
    last_upload_at = fields.DatetimeField(null=True)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_skill_repos"
        indexes = (("scope_type", "scope_id"),)


class AgentSkill(Model):
    """An agent skill (from a repo), scoped + version-activated."""

    id = fields.UUIDField(pk=True)
    repo_id = fields.UUIDField()
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    # Current delivery metadata lives on the Skill catalog row. Legacy repo and
    # version rows remain readable during migration but are not the discovery
    # authority for GenericAgent.
    source_type = fields.CharField(max_length=16, default="upload")
    source_url = fields.CharField(max_length=2048, null=True)
    source_filename = fields.CharField(max_length=512, null=True)
    version = fields.CharField(max_length=64, default="")
    download_url = fields.CharField(max_length=2048, null=True)
    digest = fields.CharField(max_length=64, null=True)
    min_agent_version = fields.CharField(max_length=64, null=True)
    max_agent_version = fields.CharField(max_length=64, null=True)
    scope_type = fields.CharField(max_length=16, default="global")
    scope_id = fields.CharField(max_length=64, default="global")
    created_by = fields.UUIDField()
    active_version_id = fields.UUIDField(null=True)
    is_force_delivery = fields.BooleanField(default=False)
    is_orphan = fields.BooleanField(default=False)
    is_deleted = fields.BooleanField(default=False)
    enabled = fields.BooleanField(default=True)
    extension_package_id = fields.CharField(max_length=255, null=True)
    admin_description = fields.TextField(null=True)
    admin_tags = fields.JSONField(null=True)
    script_manifest = fields.JSONField(null=True)
    script_risk = fields.CharField(max_length=16, default="none")
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_skills"
        indexes = (("repo_id",), ("scope_type", "scope_id"), ("enabled",))


class AgentSkillVersion(Model):
    """A concrete version of a skill (points at an object-storage key)."""

    id = fields.UUIDField(pk=True)
    resource_id = fields.UUIDField()
    version = fields.CharField(max_length=64)
    s3_key = fields.CharField(max_length=512)
    parsed_meta = fields.JSONField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_agent_skill_versions"
        indexes = (("resource_id",),)


# The Agent↔Skill binding table (``mc_agent_skill_bindings``) was removed:
# GenericAgent resource delivery goes through AgentSop and never installs
# Skills. The Skill catalog above stays: it powers the resource center.


class AgentSop(Model):
    """Catalog metadata for an independent SOP Markdown file.

    ``file_ref`` points into the controlled SOP source directory. The Markdown
    body stays on disk and is never embedded into a Skill package/version.
    """

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    file_ref = fields.CharField(max_length=512, unique=True)
    scope_type = fields.CharField(max_length=16, default="global")
    scope_id = fields.CharField(max_length=64, default="global")
    created_by = fields.UUIDField(null=True)
    enabled = fields.BooleanField(default=True)
    is_builtin = fields.BooleanField(default=False)
    revision = fields.IntField(default=1)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_sops"
        indexes = (("scope_type", "scope_id"), ("enabled",))


class AgentSopBinding(Model):
    """Agent selection of one independently managed SOP."""

    id = fields.UUIDField(pk=True)
    agent_id = fields.IntField()
    sop_id = fields.UUIDField()
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_sop_bindings"
        unique_together = (("agent_id", "sop_id"),)
        indexes = (("agent_id", "enabled"), ("sop_id",))


# ---- Plugins --------------------------------------------------------------
class AgentPluginRepo(Model):
    """A source repo that plugins are synced from (github/upload/npm/bare)."""

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    scope_type = fields.CharField(max_length=16, default="global")
    scope_id = fields.CharField(max_length=64, default="global")
    created_by = fields.UUIDField()
    source_type = fields.CharField(max_length=16)  # github | upload | npm | bare
    github_url = fields.CharField(max_length=512, null=True)
    ref_type = fields.CharField(max_length=16, null=True)
    ref_value = fields.CharField(max_length=255, null=True)
    last_upload_filename = fields.CharField(max_length=512, null=True)
    last_upload_at = fields.DatetimeField(null=True)
    plugin_discovery_auto_package_json = fields.BooleanField(default=True)
    plugin_manual_entries = fields.JSONField(null=True)
    npm_package_name = fields.CharField(max_length=255, null=True)
    npm_version_spec = fields.CharField(max_length=64, null=True)
    npm_registry_url = fields.CharField(max_length=512, null=True)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_plugin_repos"
        indexes = (("scope_type", "scope_id"),)


class AgentPlugin(Model):
    """An agent plugin (from a repo), scoped + version-activated."""

    id = fields.UUIDField(pk=True)
    repo_id = fields.UUIDField()
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    scope_type = fields.CharField(max_length=16, default="global")
    scope_id = fields.CharField(max_length=64, default="global")
    created_by = fields.UUIDField()
    active_version_id = fields.UUIDField(null=True)
    is_force_delivery = fields.BooleanField(default=False)
    is_orphan = fields.BooleanField(default=False)
    is_deleted = fields.BooleanField(default=False)
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_plugins"
        indexes = (("repo_id",), ("scope_type", "scope_id"), ("enabled",))


class AgentPluginVersion(Model):
    """A concrete version of a plugin."""

    id = fields.UUIDField(pk=True)
    resource_id = fields.UUIDField()
    version = fields.CharField(max_length=64)
    s3_key = fields.CharField(max_length=512)
    parsed_meta = fields.JSONField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_agent_plugin_versions"
        indexes = (("resource_id",),)


# ---- Rules ----------------------------------------------------------------
class AgentRule(Model):
    """A global agent rule, version-activated."""

    id = fields.UUIDField(pk=True)
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    scope_type = fields.CharField(max_length=16, default="global")
    scope_id = fields.CharField(max_length=64, default="global")
    created_by = fields.UUIDField()
    active_version_id = fields.UUIDField(null=True)
    extension_package_id = fields.CharField(max_length=255, null=True)
    extension_rule_id = fields.CharField(max_length=255, null=True)
    extension_version = fields.CharField(max_length=64, null=True)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_agent_rules"


class AgentRuleVersion(Model):
    """A concrete version of a rule (inline content)."""

    id = fields.UUIDField(pk=True)
    rule_id = fields.UUIDField()
    version = fields.CharField(max_length=14)
    content = fields.TextField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_agent_rule_versions"
        indexes = (("rule_id",),)
