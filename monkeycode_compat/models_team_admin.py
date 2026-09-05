"""Tortoise models for the MonkeyCode team-administration domain (``mc_*`` only).

These back the team management surface (``/api/v1/teams/*``): login methods
(OIDC/OAuth), audit log, skills, and the group link tables nodes use to scope
resources to team groups. All additive ``mc_*`` tables — they never shadow the
main service's ``provider_*`` / ``model_*`` / ``users`` tables.
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class TeamModel(Model):
    """A team-scoped LLM channel config (independent of main provider_*/model_*).

    ``api_key`` is a secret and must be masked before leaving the service.
    ``last_check_*`` are written by the health-check endpoints. Backs the
    mobile models screen via ``/api/v1/users/models``.
    """

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    provider = fields.CharField(max_length=64)
    model = fields.CharField(max_length=255)
    base_url = fields.CharField(max_length=512)
    api_key = fields.CharField(max_length=1024, null=True)
    interface_type = fields.CharField(max_length=32, default="openai_chat")
    temperature = fields.FloatField(null=True)
    support_image = fields.BooleanField(default=False)
    remark = fields.CharField(max_length=255, null=True)
    is_hidden = fields.BooleanField(default=False)
    last_check_at = fields.DatetimeField(null=True)
    last_check_success = fields.BooleanField(null=True)
    last_check_error = fields.CharField(max_length=1024, null=True)
    is_deleted = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_team_models"
        indexes = (("team_id",),)


class TeamOIDCConfig(Model):
    """A team login method (OIDC or OAuth provider). ``client_secret`` is a secret.

    ``type`` discriminates the protocol: ``oidc`` (verifies an ID Token against
    JWKS) or ``oauth_github`` / ``oauth_google`` (OAuth2 code flow + userinfo).
    One team may have several enabled methods. The legacy single-row-per-team
    assumption is gone; every query addresses rows by id.
    """

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    type = fields.CharField(max_length=32, default="oidc")
    name = fields.CharField(max_length=255, null=True)
    issuer = fields.CharField(max_length=512, null=True)
    client_id = fields.CharField(max_length=255, null=True)
    client_secret = fields.CharField(max_length=1024, null=True)
    display_name = fields.CharField(max_length=255, null=True)
    scopes = fields.CharField(max_length=512, null=True)
    email_domain = fields.CharField(max_length=255, null=True)
    enabled = fields.BooleanField(default=False)
    auto_create_member = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_team_oidc_configs"
        indexes = (("team_id",),)


class Audit(Model):
    """Team audit log entry (cursor-paginated)."""

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    user_id = fields.UUIDField(null=True)
    operation = fields.CharField(max_length=255, null=True)
    request = fields.TextField(null=True)
    response = fields.TextField(null=True)
    source_ip = fields.CharField(max_length=64, null=True)
    user_agent = fields.CharField(max_length=512, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_audits"
        indexes = (("team_id",), ("created_at",))


class TeamSkill(Model):
    """Team skill metadata used by the group-permission binding surface."""

    id = fields.UUIDField(pk=True)
    team_id = fields.UUIDField()
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    tags = fields.JSONField(default=list)
    content = fields.TextField(default="")
    source_type = fields.CharField(max_length=16, default="text")
    source_label = fields.CharField(max_length=255, null=True)
    skill_md_path = fields.CharField(max_length=512, null=True)
    group_ids = fields.JSONField(default=list)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_team_skills"
        indexes = (("team_id",),)


class TeamModelGroup(Model):
    model_id = fields.UUIDField()
    group_id = fields.UUIDField()

    class Meta:
        table = "mc_team_model_groups"
        unique_together = (("model_id", "group_id"),)
        indexes = (("model_id",), ("group_id",))


class GroupNode(Model):
    """Bind an agent-compose node to a team group.

    ``node_role`` is a snapshot of the role returned by agent-compose at bind
    time (``execution`` or ``management``). The daemon remains authoritative
    for live node status and ownership; this local row only grants the group's
    members access to the selected node.
    """

    id = fields.UUIDField(pk=True)
    group_id = fields.UUIDField()
    node_id = fields.CharField(max_length=128)
    node_role = fields.CharField(max_length=16)
    node_name = fields.CharField(max_length=255, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_group_nodes"
        unique_together = (("group_id", "node_id"),)
        indexes = (("group_id",), ("node_id",))


class NodeShellApprovalPolicy(Model):
    """Administrator-managed auto-allow list scoped to one execution node.

    The command classifier still rejects dangerous shell structures regardless
    of these rows. ``node_id`` deliberately is not a database foreign key: the
    node ledger belongs to the control plane and hard deletion cleans policies
    explicitly.
    """

    id = fields.UUIDField(pk=True)
    node_id = fields.CharField(max_length=128)
    command_key = fields.CharField(max_length=64)
    shell_flavor = fields.CharField(max_length=16)
    note = fields.CharField(max_length=255, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_node_shell_approval_policies"
        unique_together = (("node_id", "command_key", "shell_flavor"),)
        indexes = (("node_id",), ("node_id", "shell_flavor"))


class UserShellApprovalPolicy(Model):
    """Legacy per-user auto-allow list for ``node_shell_exec`` command approval.

    Legacy compatibility routes still manage these rows for one release, but
    the Agent runtime no longer reads them. New persistent authorization is
    stored in :class:`NodeShellApprovalPolicy` and shared by node.
    """

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    command_key = fields.CharField(max_length=64)
    shell_flavor = fields.CharField(max_length=16, default="unknown")
    note = fields.CharField(max_length=255, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_user_shell_approval_policies"
        unique_together = (("user_id", "command_key", "shell_flavor"),)
        indexes = (("user_id",),)
