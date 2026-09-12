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


async def _team_is_admin(user_id: str, team_id: str) -> bool:
    """团队管理员判定（新表 resolve 的可见性入口用）。"""
    from .models import TeamMember

    member = await TeamMember.get_or_none(team_id=_uuid(team_id), user_id=_uuid(user_id))
    return bool(member and member.role == "admin")


async def resolve_reference_specs(
    user_id: str,
    team_id: str,
    resource_type: str,
    bindings: list[dict] | None,
    *,
    request_base_url: str = "",
    identity_token: str | None = None,
    gateway_base_url: str = "",
) -> list[dict]:
    """Resolve editor binding ids into complete, server-trusted wire specs.

    绑定按 key 分流（双轨过渡）：

    - ``{reference_id}`` — 新表引用（统一资源池 resources/resource_references），
      经 ``resource_store`` 解析；skills 集合可带 ``entries`` 按子技能过滤。
    - ``{resource_id}`` — 旧表引用（mc_resource_references），走下方旧逻辑，
      清数据退役后此分支随之删除。
    - ``{service_id}`` — a direct ``mcp_services`` binding (builtin / admin /
      personal SSE MCP picked by id in the create-task dialog). The caller must
      be authorized for that service (``mcp_plugin_store.can_use_service``);
      an unauthorized or disabled service raises instead of silently dropping.
    - legacy full wire specs without ``resource_id``/``service_id`` — pass
      through for compatibility during migration; a reference-looking UUID is
      rejected.

    ``identity_token``/``gateway_base_url`` switch MCP spec production into the
    kind-aware gateway mode: non-stdio services are proxied through the local
    SSE gateway (``{gateway_base_url}/mcp/{name}/sse?token={identity_token}``)
    and upstream credentials never leave the server. Without an identity token
    the resolver keeps emitting the legacy full spec (``url`` + ``headers`` from
    the ``mcp_services`` row) so the not-yet-migrated callers (editor session
    path) keep working — that shape is produced by ``_legacy_full_wire_spec``
    and is scheduled to disappear with the editor migration.
    """
    import mcp_plugin_store
    from db import PostgresClient

    # 绑定分流：reference_id → 新表；其余（resource_id / service_id / legacy）→ 旧链路。
    new_bindings: list[dict] = []
    old_bindings: list[dict] = []
    for binding in bindings or []:
        if isinstance(binding, dict) and binding.get("reference_id"):
            new_bindings.append(binding)
        else:
            old_bindings.append(binding)

    resolved: list[dict] = []
    v2_visible: list[dict] | None = None

    async def _load_v2_visible() -> list[dict]:
        """新表（统一资源池）可见引用；skill 收 skill+skills+plugin 容器并集。懒加载一次。"""
        nonlocal v2_visible
        if v2_visible is None:
            import resource_store

            is_admin = await _team_is_admin(user_id, team_id)
            rows: list[dict] = []
            # skill 通道混排：单 skill + 存量 skills 集合 + plugin 容器（带 entries）。
            types = ("skill", "skills", "plugin") if resource_type == "skill" else (resource_type,)
            for t in types:
                rows.extend(await resource_store.visible_references(
                    user_id, team_id, resource_type=t, is_admin=is_admin,
                ))
            v2_visible = rows
        return v2_visible

    if new_bindings:
        import resource_store

        resolved.extend(await resource_store.resolve_specs(await _load_v2_visible(), new_bindings))

    # 旧表未命中 resource_id 时的双轨兜底：装进 shared 环境/节点本机的 v2 引用
    # 存的是裸 UUID（TaskEnvironmentResource.resource_id），调用方无法区分新旧
    # 表——这里按可见性翻译回 {reference_id} 走新链路解析，仍不可见才报未授权。
    late_new_bindings: list[dict] = []
    allowed = {row["id"]: row for row in await visible_references(user_id, team_id, resource_type)}
    for binding in old_bindings:
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
            resolved.append(_mcp_service_wire_spec(
                dict(row),
                identity_token=identity_token,
                gateway_base_url=gateway_base_url,
            ))
            continue

        resource_id = str(binding.get("resource_id") or binding.get("id") or "")
        reference = allowed.get(resource_id)
        if reference is None:
            # 双轨兜底：旧表未见 → 试新表（统一资源池引用）。命中则按
            # {reference_id}（可带 entries 子集）走新链路解析；未命中才拒绝。
            if resource_id and _uuid(resource_id) is not None:
                v2_hit = next(
                    (row for row in await _load_v2_visible() if str(row["id"]) == resource_id),
                    None,
                )
                if v2_hit is not None:
                    late = {"reference_id": resource_id}
                    entries = [str(e).strip() for e in (binding.get("entries") or []) if str(e).strip()]
                    if entries:
                        late["entries"] = entries
                    late_new_bindings.append(late)
                    continue
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
            # 插件容器（manifest.entries 在场，含存量 skills 集合引用）：一条引用
            # 展开成 N 个子技能 spec。子技能名 ``repo_full_name/entry_name`` 跨仓库
            # 不冲突，也是任务期 activeSkills 勾选的稳定标识。绑定可带 ``entries``
            # （任务期勾选）按名过滤；环境同步/编辑器等不带 → 全量。
            collection_entries = manifest.get("entries")
            if isinstance(collection_entries, list) and collection_entries:
                wanted = {str(e) for e in binding.get("entries") or [] if str(e).strip()}
                base_url = manifest.get("source_url") or resource.get("url") or ""
                base_ref = resource.get("ref") or ""
                repo_url = str(base_url).split(".git", 1)[0].rstrip("/")
                repo_short = repo_url.rsplit("/", 2)[-2:]
                repo_label = "/".join(repo_short) if len(repo_short) == 2 else str(reference.get("name") or "")
                for entry in collection_entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_name = str(entry.get("name") or "")
                    if wanted and entry_name not in wanted:
                        continue
                    resolved.append({
                        "name": f"{repo_label}/{entry_name}" if repo_label else entry_name,
                        "source": resource.get("source") or "github",
                        "url": base_url,
                        "path": str(entry.get("path") or ""),
                        "ref": base_ref,
                    })
                continue
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
            # 容器插件（manifest.entries 在场）：与技能集合同一展开逻辑——技能勾选
            # 按子技能 git clone；无 entries 的纯 zip 插件走 download_url 整包。
            collection_entries = manifest.get("entries")
            if isinstance(collection_entries, list) and collection_entries:
                wanted = {str(e) for e in binding.get("entries") or [] if str(e).strip()}
                base_url = manifest.get("source_url") or resource.get("url") or ""
                base_ref = resource.get("ref") or ""
                repo_url = str(base_url).split(".git", 1)[0].rstrip("/")
                repo_short = repo_url.rsplit("/", 2)[-2:]
                repo_label = "/".join(repo_short) if len(repo_short) == 2 else str(reference.get("name") or "")
                for entry in collection_entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_name = str(entry.get("name") or "")
                    if wanted and entry_name not in wanted:
                        continue
                    resolved.append({
                        "name": f"{repo_label}/{entry_name}" if repo_label else entry_name,
                        "source": resource.get("source") or "github",
                        "url": base_url,
                        "path": str(entry.get("path") or ""),
                        "ref": base_ref,
                    })
                continue
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
            resolved.append(_mcp_service_wire_spec(
                service,
                name=name,
                identity_token=identity_token,
                gateway_base_url=gateway_base_url,
            ))
    if late_new_bindings:
        import resource_store

        resolved.extend(await resource_store.resolve_specs(await _load_v2_visible(), late_new_bindings))
    return resolved


def mcp_service_is_stdio(service: dict) -> bool:
    """Classify an ``mcp_services`` row: does the node spawn it locally?

    stdio 形态（kind='stdio' 或 transport='stdio'）的进程由节点在本地拉起，
    spec 必须带 command/args/env_template；其余形态（builtin/custom/sse/
    node_hosted 非 stdio）一律经服务端 SSE 网关代理，节点只拿网关地址。
    两个判据都查：旧行只填了 transport，新行才有 kind。
    """
    kind = str(service.get("kind") or "").strip().lower()
    transport = str(service.get("transport") or "").strip().lower()
    return kind == "stdio" or transport == "stdio"


def _env_map_to_list(mapping: dict | None) -> list[dict]:
    """Coerce an env/headers map into the proto wire shape ``[{name, value}]``.

    ``MCPServerSpec.env`` / ``.headers`` are ``repeated EnvVarSpec`` (a JSON
    list of ``{name, value}`` objects), but the historic row shape stored
    env/headers as a plain ``{KEY: value}`` map. A dict value reaches the
    control-plane JSON parser and is rejected ("repeated field env must be in
    a list which is {}"), so every wire spec must emit a list. The list form
    also matches what the node's ``runtimeEnvMap`` consumes
    (see nodes/execution/editorconfig.go).
    """
    if not isinstance(mapping, dict) or not mapping:
        return []
    out: list[dict] = []
    for key, value in mapping.items():
        name = str(key).strip()
        if not name:
            continue
        out.append({"name": name, "value": "" if value is None else str(value)})
    return out


def _mcp_service_wire_spec(
    service: dict,
    *,
    name: str = "",
    identity_token: str | None = None,
    gateway_base_url: str = "",
) -> dict:
    """Build the node-side MCP wire spec from an ``mcp_services`` row.

    One constructor for both binding shapes (team reference and direct
    ``service_id``) so a node always receives the same fields — transport plus
    the command/url it actually needs.

    kind-aware（Phase 3）：上游密钥绝不下发给节点。

    - stdio 行：节点本地 spawn，保留 command/args/env_template 产出；
      ``headers`` 恒为空（stdio 没有 HTTP 头，行里的 headers 只会白白泄露）。
    - 非 stdio 行 + ``identity_token``：产出网关代理 spec——
      ``type='remote'``/``transport='sse'``。url 形态二选一：
      ``gateway_base_url`` 为空（节点自拼线）时发**相对路径**
      ``/mcp/{name}/sse?token={identity_token}``，由节点拿服务端 hello 通告的
      网关 origin 自己拼绝对地址（服务端零配置）；非空时按旧形态拼绝对
      URL（显式配置了网关基址的部署）。headers/command/args/env 一律清空，
      上游 url/Bearer 留在服务端。
    - 非 stdio 行 + 无 ``identity_token``：迁移期兜底走
      ``_legacy_full_wire_spec``（编辑器线尚未迁移，调用方还没法传 token）。
    """
    svc_name = str(service.get("name") or name or "")
    if mcp_service_is_stdio(service):
        return {
            "name": svc_name,
            "type": "stdio",
            "transport": str(service.get("transport") or "stdio"),
            "command": service.get("command") or "",
            "args": list(service.get("args") or []),
            "env": _env_map_to_list(service.get("env_template")),
            "url": service.get("url") or "",
            "headers": [],
        }
    if identity_token:
        base = str(gateway_base_url or "").strip().rstrip("/")
        if base:
            url = f"{base}/mcp/{svc_name}/sse?token={identity_token}"
        else:
            # 节点自拼形态：相对路径 + 节点侧 hello 通告的网关 origin。
            url = f"/mcp/{svc_name}/sse?token={identity_token}"
        return {
            "name": svc_name,
            "type": "remote",
            "transport": "sse",
            "command": "",
            "args": [],
            "env": [],
            "url": url,
            "headers": [],
        }
    # 迁移期兼容：未迁移的调用方（编辑器会话线）拿到的仍是旧全量 spec。
    return _legacy_full_wire_spec(service, name=name)


async def normalize_mcp_bindings(
    user_id: str,
    team_id: str,
    bindings: list[dict] | None,
) -> list[dict]:
    """把 MCP 绑定列表规范化成可安全持久化的绑定形态（service_id / resource_id），

    编辑器会话无 principal，服务选择直接落 editor 行的 mcp_config 列；落 spec（含
    网关 token 或上游密钥）等于把凭证写进 DB，不安全。本函数只做授权校验 + 归一：

    - ``{service_id}`` 直绑：校验 can_use_service，原样保留。
    - ``{resource_id}`` 团队引用：校验可见，原样保留。
    - legacy 全量 spec（带 name 但无 service_id/resource_id）：按 name 反查 mcp_services
      行，转成 ``{service_id}``；查不到或未授权 → 报错（拒绝静默落垃圾 spec）。
    - 纯无名 / 既非绑定又非 spec → 报错。

    不产 wire spec、不签 token——那两步在派发/hot-resync 时现做（见
    resolve_reference_specs + identity_token）。
    """
    import mcp_plugin_store

    allowed = {row["id"]: row for row in await visible_references(user_id, team_id, "mcp")}
    out: list[dict] = []
    for binding in bindings or []:
        if not isinstance(binding, dict):
            continue
        service_id_raw = binding.get("service_id")
        if service_id_raw is not None and str(service_id_raw).strip():
            try:
                service_id = int(service_id_raw)
            except (TypeError, ValueError):
                raise ValueError(f"mcp_service_not_found:{service_id_raw}")
            if not await mcp_plugin_store.can_use_service(user_id=str(user_id), service_id=service_id):
                raise ValueError(f"mcp_not_authorized:{service_id}")
            out.append({
                "service_id": service_id,
                # 工具类选择的实例绑定（创建弹框「选工具后勾选实例」）：param_key=
                # required_param（cdp_client_id 等），param_values=资源 id 字符串列表。
                # 透传给 task_service 的 grants 写入；空/缺省 = 不收窄（默认全量）。
                **({"param_key": str(binding["param_key"])} if binding.get("param_key") else {}),
                **({"param_values": [str(v) for v in binding["param_values"]]}
                   if isinstance(binding.get("param_values"), list) and binding["param_values"] else {}),
            })
            continue
        resource_id = str(binding.get("resource_id") or binding.get("id") or "")
        if resource_id and resource_id in allowed:
            reference = allowed[resource_id]
            # 引用型 MCP 最终也必须落同一条 service grant：环境/团队/用户
            # 引用只持有 reference id，派发阶段要按 owned_entity_id 找服务并现签
            # task token；保留 resource_id 会在 grants 同步阶段丢失。
            owned_id = reference.get("owned_entity_id")
            try:
                service_id = int(owned_id)
            except (TypeError, ValueError):
                raise ValueError(f"mcp_not_ready:{resource_id}")
            if not await mcp_plugin_store.can_use_service(user_id=str(user_id), service_id=service_id):
                raise ValueError(f"mcp_not_authorized:{service_id}")
            out.append({"service_id": service_id})
            continue
        # legacy 全量 spec：按 name 反查成 service_id 绑定（迁移旧数据用）。
        name = str(binding.get("name") or "").strip()
        if name:
            service = await mcp_plugin_store.get_service_by_name(name)
            if not service or service.get("id") is None:
                raise ValueError(f"mcp_service_not_found_by_name:{name}")
            sid = int(service["id"])
            if not await mcp_plugin_store.can_use_service(user_id=str(user_id), service_id=sid):
                raise ValueError(f"mcp_not_authorized:{sid}")
            out.append({"service_id": sid})
            continue
        if resource_id:
            # resource_id 形态但不在可见集合 → 显式拒绝，不静默落垃圾。
            raise ValueError(f"resource_not_granted:{resource_id}")
        raise ValueError("mcp_binding_invalid:missing service_id/resource_id/name")
    # 去重（同 service_id 多次 = 一条）。
    seen: set[str] = set()
    deduped: list[dict] = []
    for b in out:
        key = f"{b.get('service_id') or b.get('resource_id')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(b)
    return deduped


def _legacy_full_wire_spec(service: dict, *, name: str = "") -> dict:
    """Deprecated: copy the row's own url/headers into the node spec.

    密钥泄露形态——非 stdio 服务的 ``mcp_services.url``/``headers`` 常含上游
    Bearer，直接下发给节点等于把上游凭证交出去。只保留给迁移期诊断/兼容：
    ``resolve_reference_specs`` 在调用方未传 ``identity_token`` 时兜底用它
    （编辑器线迁移完成后删除），新代码一律走
    ``_mcp_service_wire_spec(identity_token=...)`` 的网关代理形态。
    """
    return {
        "name": service.get("name") or name,
        "type": "stdio" if service.get("transport") == "stdio" else "remote",
        "transport": service.get("transport") or "stdio",
        "command": service.get("command") or "",
        "args": list(service.get("args") or []),
        "env": _env_map_to_list(service.get("env_template")),
        "url": service.get("url") or "",
        "headers": _env_map_to_list(service.get("headers")),
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
