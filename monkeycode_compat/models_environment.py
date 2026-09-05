"""Tortoise models for user-owned task execution environments.

An *environment* is what a user picks before creating a task. It decides which
HOME the editor runs against, and (for the shared tier) carries a reusable set
of skill/MCP/plugin references so a user does not re-pick them per task.

Three tiers exist, but only ONE of them needs a database row:

* ``system``   — the editor runs against the node operator's real HOME, using
  whatever the operator already installed there. No environment row: there is
  nothing to *configure*. What the node observes there IS recorded, per node, in
  :class:`NodeSystemEnvEntry` — otherwise a system-env task's real capability set
  would stay invisible to both the server and the user.
* ``isolated`` — a throwaway per-session HOME. This is the *initial* environment
  (a clean slate every run), so there is nothing to manage or sync.
* ``shared``   — a named, persistent HOME reused across tasks. This is the only
  tier with a configured resource set here: it has an id, a user-facing name, and
  a resource list the user maintains over time.

Two truth sources, on purpose
-----------------------------
:class:`TaskEnvironment` + :class:`TaskEnvironmentResource` are the *desired*
state (what the user configured, shown and edited server-side).
:class:`TaskEnvironmentInstalled` mirrors what a node actually reported on disk
for that environment. Neither can be derived from the other — disk carries no
``resource_id``/manifest version, and the ledger cannot know what someone
installed by hand from a maintenance shell — so the environment page diffs them
to show 已安装 / 待安装 / 多余.

Credentials are deliberately absent
-----------------------------------
MCP entries store only a *reference*, never a token. Skills and plugins are
files that live in the environment HOME; MCP is config, so it is never
materialized into a shared HOME at all. Each task mints its own MCP token from
its own principal at dispatch time and writes it to that task's private
``stateRoot`` — so two tasks sharing an environment never share or overwrite
each other's credentials.
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model

# Environment tiers. Only ``shared`` has rows in these tables; the other two are
# resolved directly from the tier name at dispatch time.
ENV_TIER_SYSTEM = "system"
ENV_TIER_ISOLATED = "isolated"
ENV_TIER_SHARED = "shared"
ENV_TIERS = (ENV_TIER_SYSTEM, ENV_TIER_ISOLATED, ENV_TIER_SHARED)

# Resource kinds an environment can carry. Skills/plugins are files synced into
# the environment HOME; MCP is config resolved per task (never written there).
ENV_RESOURCE_SKILL = "skill"
ENV_RESOURCE_MCP = "mcp"
ENV_RESOURCE_PLUGIN = "plugin"
ENV_RESOURCE_KINDS = (ENV_RESOURCE_SKILL, ENV_RESOURCE_MCP, ENV_RESOURCE_PLUGIN)


class TaskEnvironment(Model):
    """One user-owned shared environment.

    ``id`` is database-generated and is what the task carries as its ``env_id``;
    the node derives its own local directory from it, so renaming ``name`` never
    moves anything on disk. ``node_id`` is the node whose disk holds this
    environment's HOME: the resource set is portable data, but an installed
    toolchain is not, so an environment is pinned to the node it was created on.
    """

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()
    node_id = fields.CharField(max_length=128)
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    # Bumped on every resource-set edit. The node reports back the revision it
    # last synced so the page can say "配置已改，待同步".
    revision = fields.BigIntField(default=0)
    synced_revision = fields.BigIntField(default=0)
    last_synced_at = fields.DatetimeField(null=True)
    # Last sync error text (redacted), surfaced on the environment page so a
    # failed install is visible instead of silently missing.
    last_sync_error = fields.TextField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_task_environments"
        unique_together = (("user_id", "node_id", "name"),)
        indexes = (("user_id",), ("node_id",))


class TaskEnvironmentResource(Model):
    """A skill/MCP/plugin the user added to an environment (desired state).

    Stores a *reference* (``resource_id`` into ``mc_resource_references``), not a
    resolved wire spec — the same rule tasks already follow. Resolution to a full
    spec (and, for MCP, minting a token) happens per dispatch, so no credential
    is ever persisted here.
    """

    id = fields.UUIDField(pk=True)
    env_id = fields.UUIDField()
    kind = fields.CharField(max_length=16)
    resource_id = fields.UUIDField()
    # Display snapshot so the page can render without re-resolving every row.
    name = fields.CharField(max_length=255, default="")
    version = fields.CharField(max_length=64, default="")
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_task_environment_resources"
        unique_together = (("env_id", "kind", "resource_id"),)
        indexes = (("env_id", "kind"),)


class TaskEnvironmentInstalled(Model):
    """What a node last reported as physically present in an environment HOME.

    This is the *observed* side of the diff. It is replaced wholesale by each
    inventory report, so a row here means "the node saw this on disk at
    ``reported_at``" — including things installed by hand, which show up as
    extras with no matching :class:`TaskEnvironmentResource`.

    MCP never appears here: it is not a file, so there is nothing on disk to
    inventory.
    """

    id = fields.UUIDField(pk=True)
    env_id = fields.UUIDField()
    kind = fields.CharField(max_length=16)
    # Directory name on the node, which is what the runtime activates by name.
    name = fields.CharField(max_length=255)
    version = fields.CharField(max_length=64, default="")
    reported_at = fields.DatetimeField()

    class Meta:
        table = "mc_task_environment_installed"
        unique_together = (("env_id", "kind", "name"),)
        indexes = (("env_id",),)


class NodeSystemEnvEntry(Model):
    """One resource a node reported present in its operator's real HOME.

    The system tier (``env_mode="system"``) has no environment row — it *is* the
    operator's HOME — so this table is keyed by ``node_id`` rather than
    ``env_id``: one node, one operator HOME. It exists because the providers
    already discover whatever the operator installed there (claude reads
    ``~/.claude/skills``, others read ``~/.agents/{skills,plugins}``, gemini
    ``~/.gemini/extensions``, MCP ``~/.mcp.json``), yet none of it was visible to
    the server — a system-env task silently got capabilities nobody could see.

    Replaced wholesale by each inventory report, like
    :class:`TaskEnvironmentInstalled`: a row means "the node saw this at
    ``reported_at``".

    ``platform_managed`` separates what we installed from what the operator did.
    It is the authorization boundary for removal: the node refuses to delete
    anything absent from its own manifest, so an operator's hand-installed
    resource can be shown but never removed through the API.

    Unlike the shared-tier table this DOES carry MCP rows — display only, because
    writing the operator's MCP config would leak per-task credentials into a
    directory other things read.
    """

    id = fields.UUIDField(pk=True)
    node_id = fields.CharField(max_length=128)
    kind = fields.CharField(max_length=16)  # skill | plugin | mcp
    name = fields.CharField(max_length=255)
    version = fields.CharField(max_length=64, default="")
    # Provider whose discovery path this was found under; empty for the
    # provider-neutral .agents tree and for MCP.
    provider = fields.CharField(max_length=32, default="")
    # HOME-relative path, so the console can explain where an entry came from.
    path = fields.CharField(max_length=512, default="")
    platform_managed = fields.BooleanField(default=False)
    # Set once a local resource has been archived into the platform library, so
    # the UI can show 已入库 and other nodes/environments can reuse it.
    archived_reference_id = fields.UUIDField(null=True)
    reported_at = fields.DatetimeField()

    class Meta:
        table = "mc_node_system_env_entries"
        unique_together = (("node_id", "kind", "name"),)
        indexes = (("node_id",),)
