"""User-owned task execution environments.

An *environment* is what a user picks before creating a task. It decides which
HOME the editor runs against, and — for the shared tier — carries a reusable set
of skill/MCP/plugin references the user maintains over time.

Three tiers, one table
----------------------
``system`` (the node operator's real HOME) and ``isolated`` (a throwaway
per-session HOME) need no row: they are resolved from the tier name alone.
``isolated`` in particular is the *initial* environment — a clean slate every
run — so there is nothing to manage or sync. Only ``shared`` has rows here.

Why two truth sources
---------------------
The configured resource set (this module's tables) is the desired state; what a
node reports on disk is the observed state. Neither derives from the other:
disk carries no ``resource_id``/manifest version, and the ledger cannot know
what an operator installed by hand from a maintenance shell. :func:`environment_detail`
diffs them so the page can label every entry 已安装 / 待安装 / 多余.

Install vs. activate
--------------------
Editing an environment's resources is a real install/uninstall on the node
(:func:`sync_environment`). A *task* removing a resource is only a per-run
activation change — it never touches the environment's files, so one task
dropping a skill cannot uninstall it for other tasks on the same environment.

No credentials, ever
--------------------
MCP entries hold only a reference. A task mints its own MCP token from its own
principal at dispatch and the node writes it to that task's private state root,
so environments never store, and shared HOMEs never see, a credential.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from .models_environment import (
    ENV_RESOURCE_KINDS,
    ENV_RESOURCE_MCP,
    ENV_RESOURCE_PLUGIN,
    ENV_RESOURCE_SKILL,
    ENV_TIER_ISOLATED,
    ENV_TIER_SHARED,
    ENV_TIER_SYSTEM,
    ENV_TIERS,
    TaskEnvironment,
    TaskEnvironmentInstalled,
    TaskEnvironmentResource,
)


class EnvironmentError(Exception):
    """Domain failure with a stable code the routes map onto an HTTP status."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _uuid(value: str, *, field: str = "id") -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        raise EnvironmentError("invalid_argument", f"{field} 不是合法 UUID")


def normalize_tier(value: str | None) -> str:
    """Normalize a tier name, defaulting to ``isolated``.

    Empty means isolated on purpose: a caller that never picked a tier gets the
    clean-slate behaviour, which is also what every pre-environment task did.
    """
    tier = str(value or "").strip().lower() or ENV_TIER_ISOLATED
    if tier not in ENV_TIERS:
        raise EnvironmentError("invalid_argument", f"不支持的环境类型 {tier}")
    return tier


def _env_dict(row: TaskEnvironment) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "name": row.name,
        "description": row.description or None,
        "node_id": row.node_id,
        "revision": int(row.revision or 0),
        "synced_revision": int(row.synced_revision or 0),
        # A configured-but-unsynced environment is the normal state right after an
        # edit (and the whole state while its node is offline), so the page needs
        # to distinguish it from a synced one rather than showing a silent lie.
        "needs_sync": int(row.revision or 0) > int(row.synced_revision or 0),
        "last_synced_at": row.last_synced_at.isoformat() if row.last_synced_at else None,
        "last_sync_error": row.last_sync_error or None,
    }


def _resource_dict(row: TaskEnvironmentResource) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "kind": row.kind,
        "resource_id": str(row.resource_id),
        "name": row.name or "",
        "version": row.version or "",
    }


# ── environment CRUD ─────────────────────────────────────────────────────────


async def list_environments(user_id: str, *, node_id: str = "") -> list[dict]:
    """List the caller's shared environments, optionally for one node only.

    The task-creation flow filters by node: an environment's HOME lives on the
    node it was created on, so an environment from another node cannot host this
    task.
    """
    query = TaskEnvironment.filter(user_id=_uuid(user_id, field="user_id"))
    if str(node_id or "").strip():
        query = query.filter(node_id=str(node_id).strip())
    rows = await query.order_by("name")
    return [_env_dict(row) for row in rows]


async def get_environment(user_id: str, env_id: str) -> TaskEnvironment:
    row = await TaskEnvironment.get_or_none(
        id=_uuid(env_id, field="env_id"), user_id=_uuid(user_id, field="user_id")
    )
    if row is None:
        raise EnvironmentError("not_found", "环境不存在")
    return row


async def create_environment(
    user_id: str, *, node_id: str, name: str, description: str | None = None
) -> dict:
    """Create a shared environment. The id is database-generated.

    The id — not the name — is what tasks carry and what the node derives its
    local directory from, so renaming later never moves anything on disk.
    """
    node = str(node_id or "").strip()
    label = str(name or "").strip()
    if not node:
        raise EnvironmentError("invalid_argument", "node_id 不能为空")
    if not label:
        raise EnvironmentError("invalid_argument", "环境名不能为空")
    user_uuid = _uuid(user_id, field="user_id")
    if await TaskEnvironment.filter(user_id=user_uuid, node_id=node, name=label).exists():
        raise EnvironmentError("conflict", f"该节点上已有同名环境「{label}」")
    row = await TaskEnvironment.create(
        id=uuid.uuid4(),
        user_id=user_uuid,
        node_id=node,
        name=label,
        description=(description or "").strip() or None,
    )
    # Provision the directory eagerly when the node is reachable. An offline node
    # is not an error: the row exists, and the directory is created on first use
    # (resolveHome mkdirs it) or on the next sync.
    try:
        from .nodes_service import nodes_service

        await nodes_service.manage_environment(node, str(row.id), "create")
    except Exception as exc:  # noqa: BLE001
        logger.info("[env] provision deferred for {} on {}: {}", row.id, node, exc)
    return _env_dict(row)


async def update_environment(
    user_id: str, env_id: str, *, name: str | None = None, description: str | None = None
) -> dict:
    """Rename / re-describe an environment. Does not touch the node."""
    row = await get_environment(user_id, env_id)
    if name is not None:
        label = str(name).strip()
        if not label:
            raise EnvironmentError("invalid_argument", "环境名不能为空")
        if label != row.name and await TaskEnvironment.filter(
            user_id=row.user_id, node_id=row.node_id, name=label
        ).exists():
            raise EnvironmentError("conflict", f"该节点上已有同名环境「{label}」")
        row.name = label
    if description is not None:
        row.description = str(description).strip() or None
    await row.save()
    return _env_dict(row)


async def delete_environment(user_id: str, env_id: str) -> dict:
    """Delete an environment: remove its directory on the node, then its rows.

    The node call comes first so a failure leaves the row (and its id) intact —
    dropping the row while the directory survives would orphan real disk with no
    way to reach it from the UI.
    """
    row = await get_environment(user_id, env_id)
    from .nodes_service import nodes_service

    try:
        await nodes_service.manage_environment(row.node_id, str(row.id), "remove")
    except Exception as exc:  # noqa: BLE001
        raise EnvironmentError(
            "failed_precondition", f"节点未能删除环境目录：{exc}"
        ) from exc
    await TaskEnvironmentResource.filter(env_id=row.id).delete()
    await TaskEnvironmentInstalled.filter(env_id=row.id).delete()
    await row.delete()
    return {"deleted": True, "id": str(row.id)}


# ── resource set (the user's configured skill/MCP/plugin list) ────────────────


def _normalize_kind(value: str) -> str:
    kind = str(value or "").strip().lower()
    if kind not in ENV_RESOURCE_KINDS:
        raise EnvironmentError("invalid_argument", f"不支持的资源类型 {kind}")
    return kind


async def list_environment_resources(user_id: str, env_id: str) -> list[dict]:
    row = await get_environment(user_id, env_id)
    rows = await TaskEnvironmentResource.filter(env_id=row.id).order_by("kind", "name")
    return [_resource_dict(item) for item in rows]


async def add_environment_resource(
    user_id: str, env_id: str, *, kind: str, resource_id: str, team_id: str
) -> dict:
    """Add one granted resource to an environment's configured set.

    The reference must be visible to this user (same rule tasks follow), so an
    environment cannot smuggle in a resource the user was never granted. Adding
    bumps ``revision``, which is what makes the page show 待同步.
    """
    row = await get_environment(user_id, env_id)
    kind = _normalize_kind(kind)
    rid = _uuid(resource_id, field="resource_id")

    from .resource_reference_service import visible_references

    allowed = {item["id"]: item for item in await visible_references(user_id, team_id, kind)}
    reference = allowed.get(str(rid))
    if reference is None:
        raise EnvironmentError("permission_denied", "该资源未授权给你，无法加入环境")
    if await TaskEnvironmentResource.filter(env_id=row.id, kind=kind, resource_id=rid).exists():
        raise EnvironmentError("conflict", "该资源已在环境中")

    created = await TaskEnvironmentResource.create(
        id=uuid.uuid4(),
        env_id=row.id,
        kind=kind,
        resource_id=rid,
        name=str(reference.get("display_name") or reference.get("name") or ""),
        version=str(reference.get("version") or ""),
    )
    row.revision = int(row.revision or 0) + 1
    await row.save()
    return _resource_dict(created)


async def remove_environment_resource(user_id: str, env_id: str, entry_id: str) -> dict:
    """Remove one resource from the environment's configured set.

    This is a real uninstall intent (the next sync prunes it from the node), as
    opposed to a task deactivating a resource for one run.
    """
    row = await get_environment(user_id, env_id)
    entry = await TaskEnvironmentResource.get_or_none(
        id=_uuid(entry_id, field="entry_id"), env_id=row.id
    )
    if entry is None:
        raise EnvironmentError("not_found", "该资源不在环境中")
    await entry.delete()
    row.revision = int(row.revision or 0) + 1
    await row.save()
    return {"deleted": True, "id": str(entry.id)}


# ── sync + inventory (desired state → node, node → observed state) ────────────


async def _resolved_specs(
    user_id: str, team_id: str, env_row: TaskEnvironment, *, request_base_url: str
) -> tuple[list[dict], list[dict]]:
    """Resolve the environment's skill/plugin references into node wire specs.

    MCP is deliberately excluded: it is config, not a file, and materializing it
    into a shared HOME would write one task's credentials where other tasks read.
    """
    from .resource_reference_service import resolve_reference_specs

    entries = await TaskEnvironmentResource.filter(env_id=env_row.id)
    by_kind: dict[str, list[dict]] = {ENV_RESOURCE_SKILL: [], ENV_RESOURCE_PLUGIN: []}
    for entry in entries:
        if entry.kind in by_kind:
            by_kind[entry.kind].append({"resource_id": str(entry.resource_id)})

    skills = await resolve_reference_specs(
        user_id, team_id, ENV_RESOURCE_SKILL, by_kind[ENV_RESOURCE_SKILL],
        request_base_url=request_base_url,
    ) if by_kind[ENV_RESOURCE_SKILL] else []
    plugins = await resolve_reference_specs(
        user_id, team_id, ENV_RESOURCE_PLUGIN, by_kind[ENV_RESOURCE_PLUGIN],
        request_base_url=request_base_url,
    ) if by_kind[ENV_RESOURCE_PLUGIN] else []
    return skills, plugins


async def sync_environment(
    user_id: str, env_id: str, *, team_id: str, request_base_url: str = ""
) -> dict:
    """Install the configured skill/plugin set onto the node (real install).

    On success the environment records the revision it synced, so the page stops
    showing 待同步. A failure is persisted as ``last_sync_error`` instead of only
    being raised: an install that silently did nothing is exactly the failure
    mode that leaves a user staring at a task without the capability they picked.
    """
    row = await get_environment(user_id, env_id)
    skills, plugins = await _resolved_specs(
        user_id, team_id, row, request_base_url=request_base_url
    )
    from .nodes_service import nodes_service

    target_revision = int(row.revision or 0)
    try:
        await nodes_service.sync_environment(row.node_id, str(row.id), skills, plugins)
    except Exception as exc:  # noqa: BLE001
        row.last_sync_error = str(exc)[:1000]
        await row.save()
        raise EnvironmentError("failed_precondition", f"同步失败：{exc}") from exc
    row.synced_revision = target_revision
    row.last_synced_at = datetime.now(timezone.utc).replace(tzinfo=None)
    row.last_sync_error = None
    await row.save()
    # Refresh the observed side immediately so the page reflects reality without
    # a second manual step.
    try:
        await refresh_environment_inventory(user_id, env_id)
    except Exception as exc:  # noqa: BLE001
        logger.info("[env] inventory refresh after sync failed for {}: {}", env_id, exc)
    return _env_dict(row)


async def refresh_environment_inventory(user_id: str, env_id: str) -> list[dict]:
    """Ask the node what is on disk and replace the stored inventory with it.

    Wholesale replacement is the point: the inventory is a snapshot of observed
    state, so a resource removed on the node must disappear here too rather than
    linger as a stale row.
    """
    row = await get_environment(user_id, env_id)
    from .nodes_service import nodes_service

    result = await nodes_service.inspect_environment(row.node_id, str(row.id))
    observed = [item for item in (result.get("installed") or []) if isinstance(item, dict)]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    await TaskEnvironmentInstalled.filter(env_id=row.id).delete()
    for item in observed:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        await TaskEnvironmentInstalled.create(
            id=uuid.uuid4(),
            env_id=row.id,
            kind=str(item.get("kind") or "").strip() or ENV_RESOURCE_SKILL,
            name=name,
            version=str(item.get("version") or ""),
            reported_at=now,
        )
    return observed


async def environment_detail(user_id: str, env_id: str) -> dict:
    """Environment + its configured set + what the node reported, already diffed.

    Each configured skill/plugin is labelled ``installed`` or ``pending``; entries
    the node reported with no configured counterpart are ``extra`` (installed by
    hand from a maintenance shell). MCP rows are always ``config`` — they are
    never on disk, so "installed" does not apply to them.
    """
    row = await get_environment(user_id, env_id)
    configured = await TaskEnvironmentResource.filter(env_id=row.id).order_by("kind", "name")
    installed = await TaskEnvironmentInstalled.filter(env_id=row.id)

    observed: dict[tuple[str, str], TaskEnvironmentInstalled] = {
        (item.kind, item.name): item for item in installed
    }
    resources: list[dict] = []
    matched: set[tuple[str, str]] = set()
    for entry in configured:
        item = _resource_dict(entry)
        if entry.kind == ENV_RESOURCE_MCP:
            # MCP is resolved per task (with that task's own token), never a file.
            item["state"] = "config"
            resources.append(item)
            continue
        key = (entry.kind, entry.name)
        if key in observed:
            matched.add(key)
            item["state"] = "installed"
            item["installed_version"] = observed[key].version or ""
        else:
            item["state"] = "pending"
        resources.append(item)

    extras = [
        {
            "kind": key[0],
            "name": key[1],
            "version": item.version or "",
            "state": "extra",
        }
        for key, item in observed.items()
        if key not in matched
    ]
    detail = _env_dict(row)
    detail["resources"] = resources
    detail["extras"] = extras
    return detail

