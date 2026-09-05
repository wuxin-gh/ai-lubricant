"""Durable market-resource references and team-group grants.

A market listing is only discoverable metadata.  It becomes assignable after an
administrator creates a team-owned reference here.  Editors and user catalogs
resolve references through this service instead of trusting client-supplied
manifest summaries.
"""
from __future__ import annotations

import uuid
from typing import Any

from .models import TeamGroup, TeamGroupMember
from .models_resources import ResourceGrant, ResourceReference

RESOURCE_TYPES = frozenset({"skill", "plugin", "mcp", "project_prompt"})
MODULE_TO_TYPE = {
    "skills": "skill",
    "plugins": "plugin",
    "mcp": "mcp",
    "prompts": "project_prompt",
}
TYPE_TO_MODULE = {value: key for key, value in MODULE_TO_TYPE.items()}


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _reference_dict(row: ResourceReference, group_ids: list[str] | None = None) -> dict:
    manifest = row.manifest if isinstance(row.manifest, dict) else {}
    return {
        "id": str(row.id),
        "team_id": str(row.team_id),
        "resource_type": row.resource_type,
        "market_module": row.market_module,
        "market_id": row.market_id,
        "name": row.name,
        "display_name": row.display_name or row.name,
        "version": row.version or "",
        "manifest": manifest,
        "owned_entity_type": row.owned_entity_type,
        "owned_entity_id": row.owned_entity_id,
        "status": row.status,
        "group_ids": group_ids or [],
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


async def list_references(team_id: str, resource_type: str | None = None) -> list[dict]:
    query = ResourceReference.filter(team_id=uuid.UUID(team_id))
    if resource_type:
        query = query.filter(resource_type=resource_type)
    rows = await query.order_by("resource_type", "name")
    if not rows:
        return []
    grants = await ResourceGrant.filter(resource_id__in=[row.id for row in rows])
    groups_by_resource: dict[str, list[str]] = {}
    for grant in grants:
        groups_by_resource.setdefault(str(grant.resource_id), []).append(str(grant.group_id))
    return [_reference_dict(row, groups_by_resource.get(str(row.id), [])) for row in rows]


async def get_reference(team_id: str, resource_id: str) -> ResourceReference | None:
    rid = _uuid(resource_id)
    if rid is None:
        return None
    return await ResourceReference.get_or_none(id=rid, team_id=uuid.UUID(team_id))


async def upsert_reference(
    team_id: str,
    manifest: dict,
    *,
    market_module: str,
    created_by: str | None,
    owned_entity_type: str | None = None,
    owned_entity_id: str | None = None,
) -> dict:
    resource_type = MODULE_TO_TYPE.get(market_module)
    if resource_type is None:
        raise ValueError("unsupported_module")
    market_id = str(manifest.get("id") or "").strip()
    name = str(manifest.get("name") or market_id).strip()
    if not market_id or not name:
        raise ValueError("invalid_manifest")
    defaults = {
        "name": name,
        "display_name": str(manifest.get("display_name") or name),
        "version": str(manifest.get("version") or ""),
        "manifest": manifest,
        "owned_entity_type": owned_entity_type,
        "owned_entity_id": owned_entity_id,
        "status": "active",
        "created_by": _uuid(created_by),
    }
    row, _created = await ResourceReference.update_or_create(
        team_id=uuid.UUID(team_id),
        resource_type=resource_type,
        market_id=market_id,
        defaults={"market_module": market_module, **defaults},
    )
    return _reference_dict(row)


async def delete_reference(team_id: str, resource_id: str) -> bool:
    row = await get_reference(team_id, resource_id)
    if row is None:
        return False
    if await ResourceGrant.filter(resource_id=row.id).exists():
        raise ValueError("resource_has_grants")
    await row.delete()
    return True


async def set_group_resources(
    team_id: str,
    group_id: str,
    resource_type: str,
    resource_ids: list[str],
    *,
    created_by: str | None,
) -> list[dict]:
    if resource_type not in RESOURCE_TYPES:
        raise ValueError("unsupported_resource_type")
    team_uuid = uuid.UUID(team_id)
    group_uuid = _uuid(group_id)
    if group_uuid is None or await TeamGroup.get_or_none(
        id=group_uuid, team_id=team_uuid, is_deleted=False
    ) is None:
        raise ValueError("group_not_found")

    requested = {_uuid(value) for value in resource_ids}
    requested.discard(None)
    rows = await ResourceReference.filter(
        id__in=list(requested), team_id=team_uuid, resource_type=resource_type, status="active"
    ) if requested else []
    found = {row.id for row in rows}
    if found != requested:
        raise ValueError("resource_not_referenced")

    type_ids = await _type_ids(team_uuid, resource_type)
    if type_ids:
        await ResourceGrant.filter(
            team_id=team_uuid, group_id=group_uuid, resource_id__in=type_ids
        ).delete()
    actor = _uuid(created_by)
    for row in rows:
        await ResourceGrant.create(
            id=uuid.uuid4(), team_id=team_uuid, group_id=group_uuid,
            resource_id=row.id, created_by=actor,
        )
    return await list_group_resources(team_id, group_id, resource_type)


async def _type_ids(team_id: uuid.UUID, resource_type: str) -> list[uuid.UUID]:
    rows = await ResourceReference.filter(team_id=team_id, resource_type=resource_type).only("id")
    return [row.id for row in rows]


async def list_group_resources(team_id: str, group_id: str, resource_type: str | None = None) -> list[dict]:
    team_uuid = uuid.UUID(team_id)
    group_uuid = _uuid(group_id)
    if group_uuid is None or await TeamGroup.get_or_none(
        id=group_uuid, team_id=team_uuid, is_deleted=False
    ) is None:
        raise ValueError("group_not_found")
    grants = await ResourceGrant.filter(team_id=team_uuid, group_id=group_uuid)
    ids = [grant.resource_id for grant in grants]
    query = ResourceReference.filter(id__in=ids, team_id=team_uuid, status="active")
    if resource_type:
        query = query.filter(resource_type=resource_type)
    return [_reference_dict(row, [str(group_uuid)]) for row in await query.order_by("name")]


async def visible_references(user_id: str, team_id: str, resource_type: str) -> list[dict]:
    """Return references granted to any active group containing the user.

    Team administrators may use every referenced resource in their team.  This
    avoids locking an administrator out when a newly created team has no groups,
    while normal users remain grant-bound.
    """
    from .models import TeamMember

    team_uuid = uuid.UUID(team_id)
    user_uuid = uuid.UUID(user_id)
    member = await TeamMember.get_or_none(team_id=team_uuid, user_id=user_uuid)
    if member and member.role == "admin":
        return await list_references(team_id, resource_type)
    memberships = await TeamGroupMember.filter(user_id=user_uuid)
    group_ids = [row.group_id for row in memberships]
    if not group_ids:
        return []
    grants = await ResourceGrant.filter(team_id=team_uuid, group_id__in=group_ids)
    ids = [grant.resource_id for grant in grants]
    rows = await ResourceReference.filter(
        id__in=ids, team_id=team_uuid, resource_type=resource_type, status="active"
    ).distinct().order_by("name")
    return [_reference_dict(row) for row in rows]


def should_skip_mirror(manifest: dict) -> bool:
    """install_method=github_clone 时强制节点直连 GitHub，即使有 ready 镜像也不用。

    其余取值（server_mirror / 缺省）按现状自动：有镜像走服务端 archive，否则兜底
    github clone。抽成纯函数一方面让 skill 分支的意图可读，另一方面让这条产品约束
    ——「用户选了直连就一定要直连」——可被单测锁定。
    """
    return str((manifest or {}).get("install_method") or "") == "github_clone"


async def resolve_reference_specs(
    user_id: str,
    team_id: str,
    resource_type: str,
    bindings: list[dict] | None,
    *,
    request_base_url: str = "",
) -> list[dict]:
    """Resolve editor binding ids into complete, server-trusted wire specs.

    Three binding shapes are accepted:

    - ``{resource_id}`` — a team resource reference (the canonical path; the
      spec is rebuilt from the stored manifest, never from client fields).
    - ``{service_id}`` — a direct ``mcp_services`` binding (builtin / admin /
      personal SSE MCP picked by id in the create-task dialog). The caller must
      be authorized for that service (``mcp_plugin_store.can_use_service``);
      an unauthorized or disabled service raises instead of silently dropping.
    - legacy full wire specs without ``resource_id``/``service_id`` — pass
      through for compatibility during migration; a reference-looking UUID is
      rejected.
    """
    import mcp_plugin_store
    from db import PostgresClient

    allowed = {row["id"]: row for row in await visible_references(user_id, team_id, resource_type)}
    resolved: list[dict] = []
    for binding in bindings or []:
        # Direct mcp_services binding (builtin / admin / personal).
        service_id_raw = binding.get("service_id")
        if service_id_raw is not None and str(service_id_raw).strip():
            if resource_type != "mcp":
                raise ValueError(f"service_id_binding_not_allowed_for:{resource_type}")
            try:
                service_id = int(service_id_raw)
            except (TypeError, ValueError):
                raise ValueError(f"mcp_service_not_found:{service_id_raw}")
            if not await mcp_plugin_store.can_use_service(user_id=str(user_id), service_id=service_id):
                raise ValueError(f"mcp_not_authorized:{service_id}")
            async with PostgresClient.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM mcp_services WHERE id=$1 AND enabled=true", service_id)
            if not row:
                raise ValueError(f"mcp_not_ready:{service_id}")
            resolved.append(_mcp_service_wire_spec(dict(row)))
            continue

        resource_id = str(binding.get("resource_id") or binding.get("id") or "")
        reference = allowed.get(resource_id)
        if reference is None:
            # Existing full wire specs have no local reference id and remain
            # readable during migration.  A reference-looking UUID is rejected.
            if binding.get("resource_id"):
                raise ValueError(f"resource_not_granted:{resource_id}")
            resolved.append(dict(binding))
            continue
        manifest = reference.get("manifest") if isinstance(reference.get("manifest"), dict) else {}
        resource = manifest.get("resource") if isinstance(manifest.get("resource"), dict) else {}
        name = str(manifest.get("name") or reference.get("name") or reference["market_id"])
        if resource_type == "skill":
            spec = {
                "name": name,
                "source": resource.get("source") or "github",
                "url": manifest.get("source_url") or resource.get("url") or manifest.get("download_url") or "",
                "path": resource.get("path") or "",
                "ref": resource.get("ref") or "",
            }
            # 下载方式优先看 manifest 显式声明：github_clone 强制节点直连（即使有镜像也不
            # 走服务端，比如仓库公开且节点可直连外网）；server_mirror 或缺省则按现状——
            # 有 ready 镜像走服务端 archive，否则兜底 github clone。
            if not should_skip_mirror(manifest):
                try:
                    import resource_mirror_store
                    mirror = await resource_mirror_store.find_ready_mirror("skills", reference["market_id"], reference.get("version") or "")
                    if mirror:
                        token = await resource_mirror_store.get_mirror_secret(int(mirror["id"]))
                        base = request_base_url.rstrip("/")
                        url = f"{base}/api/v1/resources/skills/fetch/{reference['market_id']}"
                        if reference.get("version"):
                            url += f"?version={reference['version']}"
                        spec = {"name": name, "source": "archive", "url": url, "path": "", "ref": "", "token": token}
                except Exception:
                    pass
            resolved.append(spec)
        elif resource_type == "plugin":
            resolved.append({
                "name": name,
                "url": manifest.get("download_url") or resource.get("url") or manifest.get("source_url") or "",
                "version": manifest.get("version") or reference.get("version") or "",
            })
        elif resource_type == "mcp":
            owned_id = reference.get("owned_entity_id")
            from db import PostgresClient
            async with PostgresClient.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM mcp_services WHERE id=$1 AND enabled=true", int(owned_id)) if owned_id else None
            if not row:
                raise ValueError(f"mcp_not_ready:{resource_id}")
            service = dict(row)
            resolved.append(_mcp_service_wire_spec(service, name=name))
    return resolved


def _mcp_service_wire_spec(service: dict, *, name: str = "") -> dict:
    """Build the node-side MCP wire spec from an ``mcp_services`` row.

    One constructor for both binding shapes (team reference and direct
    ``service_id``) so a node always receives the same fields — transport plus
    the command/url it actually needs. The dialog used to submit the browser's
    own summary of an MCP (id/display_name/tool_count and nothing else), which
    the server persisted verbatim: a spec with no transport and no command,
    useless on the node.
    """
    return {
        "name": service.get("name") or name,
        "type": "stdio" if service.get("transport") == "stdio" else "remote",
        "transport": service.get("transport") or "stdio",
        "command": service.get("command") or "",
        "args": list(service.get("args") or []),
        "env": dict(service.get("env_template") or {}),
        "url": service.get("url") or "",
        "headers": dict(service.get("headers") or {}),
    }


async def resolve_prompt_for_user(user_id: str, team_id: str, prompt_id: str, provider: str) -> dict | None:
    """Resolve a prompt only when it is owned by the user or team-granted."""
    from . import project_prompt_store

    prompt = await project_prompt_store.get_prompt(prompt_id)
    if not prompt or not prompt.get("enabled"):
        return None
    providers = prompt.get("providers") or []
    if providers and provider not in providers:
        return None
    if prompt.get("owner_user_id") == str(user_id):
        return prompt
    for reference in await visible_references(user_id, team_id, "project_prompt"):
        if str(reference.get("owned_entity_id") or "") == str(prompt_id):
            return prompt
    return None
