"""Team-owned resource references and group grants."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Body, Depends, HTTPException, Request

from .deps import get_current_team_id, get_current_user
from . import github_recognize
from .marketplace import config as marketplace_config
from .marketplace.github import MarketplaceGitHub
from .marketplace.validator import item_path, safe_item_id, validate_manifest
from .models import User
from . import resource_reference_service as references
from . import project_prompt_store

router = APIRouter(prefix="/api/v1/resources", tags=["resource-references"])


def _envelope(data, message: str = "ok") -> dict:
    return {"code": 0, "message": message, "data": data}


async def _market_manifest(module: str, market_id: str) -> dict:
    if module not in marketplace_config.settings.modules:
        raise HTTPException(400, f"unsupported module: {module}")
    safe = safe_item_id(str(market_id).replace("/", "."))
    if not safe:
        raise HTTPException(400, "invalid market id")
    try:
        data, _sha = await MarketplaceGitHub(marketplace_config.settings).read_json(
            item_path(module, safe)
        )
    except Exception as exc:
        raise HTTPException(502, f"读取市场 manifest 失败: {exc}") from exc
    errors = validate_manifest(module, data)
    if errors:
        raise HTTPException(422, {"error": "manifest 校验失败", "errors": errors})
    return data


async def _materialize_prompt(manifest: dict) -> tuple[str, str]:
    from .project_prompt_store import create_prompt, list_admin_prompts, update_prompt

    resource = manifest.get("resource") if isinstance(manifest.get("resource"), dict) else {}
    name = str(manifest.get("display_name") or manifest.get("name") or manifest.get("id"))
    content = str(resource.get("content") or "")
    providers = resource.get("providers") or (manifest.get("compatibility") or {}).get("providers") or []
    market_id = str(manifest.get("id") or "")
    for prompt in await list_admin_prompts():
        if prompt.get("market_id") == market_id:
            updated = await update_prompt(prompt["id"], {
                "name": name, "content": content, "providers": providers, "enabled": True,
                "market_id": market_id, "market_version": str(manifest.get("version") or ""),
            })
            return "project_prompt", str((updated or prompt)["id"])
    prompt = await create_prompt(
        name=name, content=content, providers=list(providers), enabled=True,
        market_id=market_id, market_version=str(manifest.get("version") or ""),
    )
    return "project_prompt", str(prompt["id"])


async def _materialize_mcp(manifest: dict) -> tuple[str | None, str | None]:
    """Reuse an existing MCP install when it came from this market item.

    Creating a new MCP service still needs its configuration dialog (credentials,
    env and deployment choices), so the resource page first installs through the
    existing MCP flow and then references the resulting service.  Bare market ids
    are never considered installed.
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    market_id = str(manifest.get("id") or "")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM mcp_services WHERE market_id=$1 ORDER BY id DESC LIMIT 1", market_id
        )
    if not row:
        raise HTTPException(409, "请先在 MCP 资源页完成安装配置，再引用给团队")
    return "mcp_service", str(row["id"])


@router.get("/references")
async def list_resource_references(
    resource_type: str | None = None,
    _: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    if resource_type and resource_type not in references.RESOURCE_TYPES:
        raise HTTPException(400, "unsupported resource type")
    return _envelope(await references.list_references(team_id, resource_type))


@router.post("/references")
async def create_resource_reference(
    body: dict = Body(...),
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    module = str(body.get("module") or "")
    market_id = str(body.get("market_id") or "")
    manifest = await _market_manifest(module, market_id)
    owned_type: str | None = None
    owned_id: str | None = None
    if module == "prompts":
        owned_type, owned_id = await _materialize_prompt(manifest)
    elif module == "mcp":
        owned_type, owned_id = await _materialize_mcp(manifest)
    else:
        # Skills and plugins are reference-backed market resources.  Their
        # complete manifest is retained and resolved only for authorized users.
        owned_type, owned_id = "market_resource", market_id
    try:
        row = await references.upsert_reference(
            team_id, manifest, market_module=module, created_by=str(user.id),
            owned_entity_type=owned_type, owned_entity_id=owned_id,
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return _envelope(row, "已引用")


@router.delete("/references/{resource_id}")
async def delete_resource_reference(
    resource_id: str,
    _: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        deleted = await references.delete_reference(team_id, resource_id)
    except ValueError as exc:
        if str(exc) == "resource_has_grants":
            raise HTTPException(409, "资源仍分配给团队分组，请先撤销分配") from exc
        raise
    if not deleted:
        raise HTTPException(404, "resource reference not found")
    return _envelope({"deleted": True})


@router.post("/references/from-github")
async def create_reference_from_github(
    body: dict = Body(...),
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """引用一个 GitHub 仓库为团队资源引用（服务器零拷贝）。

    与 ``POST /references`` 不同，这里不要求仓库已进市场——直接用探针产
    出的坐标建引用。坐标服务端再探一次（不信任客户端字段，与
    resolve_reference_specs 同一纪律）。节点交付时按 ``install_method``：
    skill → ``git clone`` GitHub；plugin → 取 archive zip。服务器不留字节。

    Body: ``{"repo": "owner/repo 或 URL", "ref": "分支|commit",
    "kind": "skill"|"plugin", "entry_index": 0}``。``ref`` 为 commit sha 即
    钉死；分支则每次 setup 拉最新。
    """
    repo_input = str(body.get("repo") or "").strip()
    if not repo_input:
        raise HTTPException(400, "请输入 GitHub 仓库地址")
    kind = str(body.get("kind") or body.get("type") or "").strip().lower()
    if kind not in ("skills", "skill", "plugin"):
        raise HTTPException(400, "kind 只能是 skills、skill 或 plugin")
    ref = str(body.get("ref") or "").strip()
    force_type = kind
    recognized = await github_recognize.recognize_repo(repo_input, ref=ref, force_type=force_type)
    if recognized.get("error"):
        raise HTTPException(422, recognized["error"])
    full_name = recognized["repo_full_name"]
    use_ref = recognized["ref"]
    version = recognized.get("head_sha") or use_ref
    install_spec = recognized.get("install_spec") or {}
    name_override = str(body.get("name") or "").strip()
    display_name_override = str(body.get("display_name") or "").strip()
    description_override = str(body.get("description") or "").strip()
    if kind == "skills":
        entries = recognized.get("skill_entries") or (install_spec.get("skill") or {}).get("entries") or []
        if len({str(e.get("path") or "") for e in entries}) < 2:
            raise HTTPException(422, "未识别到可作为技能集合的多个技能条目")
        manifest = github_recognize.build_skills_collection_manifest(
            full_name, use_ref, entries, version=version,
            name=name_override, display_name=display_name_override, description=description_override,
            pin_commit=str(body.get("pin_commit") or "") if body.get("pin_commit") else "",
        )
        module = "skills"
    elif kind == "skill":
        entries = recognized.get("skill_entries") or (install_spec.get("skill") or {}).get("entries") or []
        entry = github_recognize.pick_skill_entry(entries, body.get("entry_index"))
        if entry is None:
            raise HTTPException(422, "未在该仓库识别到 SKILL.md / AGENTS.md / .cursor 规则")
        manifest = github_recognize.build_skill_manifest(
            full_name, use_ref, entry, version=version,
            name=name_override, display_name=display_name_override, description=description_override,
        )
        module = "skills"
    else:
        plugin_spec = install_spec.get("plugin") or {}
        if not plugin_spec:
            raise HTTPException(422, "未在该仓库识别到 .claude-plugin/marketplace.json")
        manifest = github_recognize.build_plugin_manifest(
            full_name, use_ref, plugin_spec, version=version,
            name=name_override, display_name=display_name_override, description=description_override,
        )
        module = "plugins"
    try:
        row = await references.upsert_reference(
            team_id, manifest, market_module=module, created_by=str(user.id),
            owned_entity_type="market_resource", owned_entity_id=manifest["id"],
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return _envelope(row, "已引用（GitHub 直连，服务器不存字节）")


@router.post("/references/from-github-v2")
async def create_reference_from_github_v2(
    body: dict = Body(...),
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Reference a GitHub repo (new schema: resources + resource_references).

    Recognition first lands in the resources table (source_type=github_recognize,
    upsert keyed by repo_full_name — re-recognition updates the same row), then the
    reference FKs to it. This is the "先落池再引用" design: the pool row carries
    type/data/association/editors; the reference carries params + version pin.

    Body: ``{"repo", "ref", "kind": "skills"|"skill"|"plugin"|"mcp", "name",
    "display_name", "description", "entry_index", "pin_commit", "params"}``.
    ``params`` is team-private MCP connection configuration.
    """
    import resource_store

    repo_input = str(body.get("repo") or "").strip()
    if not repo_input:
        raise HTTPException(400, "请输入 GitHub 仓库地址")
    kind = str(body.get("kind") or body.get("type") or "").strip().lower()
    if kind not in ("skills", "skill", "plugin", "mcp"):
        raise HTTPException(400, "kind 只能是 skills、skill、plugin 或 mcp")
    ref = str(body.get("ref") or "").strip()
    recognized = await github_recognize.recognize_repo(repo_input, ref=ref, force_type=kind)
    if recognized.get("error"):
        raise HTTPException(422, recognized["error"])

    full_name = recognized["repo_full_name"]
    use_ref = recognized["ref"]
    head_sha = recognized.get("head_sha") or ""
    install_spec = recognized.get("install_spec") or {}
    name_override = str(body.get("name") or "").strip()
    display_name_override = str(body.get("display_name") or "").strip()
    description_override = str(body.get("description") or "").strip()
    pin_commit = str(body.get("pin_commit") or "").strip()
    clone_url = f"https://github.com/{full_name}.git"

    if kind == "skills":
        entries = recognized.get("skill_entries") or (install_spec.get("skill") or {}).get("entries") or []
        if len({str(e.get("path") or "") for e in entries}) < 2:
            raise HTTPException(422, "未识别到可作为技能集合的多个技能条目")
        resource_type = "skills"
        resource_data = {
            "install_method": "github_clone",
            "source": "github",
            "ref": use_ref,
            "clone_url": clone_url,
            "entries": [
                {
                    "name": str(e.get("name") or ""),
                    "path": str(e.get("path") or "").strip("/"),
                    "entry": str(e.get("entry") or "SKILL.md"),
                }
                for e in entries
            ],
        }
        association = [str(e.get("name") or "") for e in entries if e.get("name")]
        editors: list[str] = []
        for e in entries:
            for ed in (e.get("editors") or []):
                if ed and ed not in editors:
                    editors.append(ed)
        name = name_override or full_name
    elif kind == "skill":
        entries = recognized.get("skill_entries") or (install_spec.get("skill") or {}).get("entries") or []
        entry = github_recognize.pick_skill_entry(entries, body.get("entry_index"))
        if entry is None:
            raise HTTPException(422, "未在该仓库识别到 SKILL.md / AGENTS.md / .cursor 规则")
        resource_type = "skill"
        resource_data = {
            "install_method": "github_clone",
            "source": "github",
            "ref": use_ref,
            "clone_url": clone_url,
            "path": str(entry.get("path") or "").strip("/"),
            "entry": str(entry.get("entry") or "SKILL.md"),
        }
        association = []
        editors = [str(ed) for ed in (entry.get("editors") or [])]
        name = name_override or str(entry.get("name") or full_name.split("/")[-1])
    elif kind == "plugin":
        plugin_spec = install_spec.get("plugin") or {}
        if not plugin_spec:
            raise HTTPException(422, "未在该仓库识别到 .claude-plugin/marketplace.json")
        resource_type = "plugin"
        resource_data = {
            "download_url": str(
                plugin_spec.get("download_url")
                or f"https://github.com/{full_name}/archive/refs/heads/{use_ref}.zip"
            ),
            "provider": str(plugin_spec.get("provider") or "claude"),
            "entry": str(plugin_spec.get("entry") or ".claude-plugin/marketplace.json"),
        }
        association = []
        provider = str(plugin_spec.get("provider") or "claude")
        editors = [provider] if provider in ("claude", "codex", "opencode", "cursor", "gemini") else []
        name = name_override or str(plugin_spec.get("provider") or full_name.split("/")[-1])
    else:  # mcp
        launch = recognized.get("launch_spec") or {}
        mcp_kind = str(launch.get("kind") or "")
        if mcp_kind not in ("stdio", "remote"):
            raise HTTPException(422, "未在该仓库识别到 MCP 启动证据（.mcp.json / README npx·uvx）")
        resource_type = "mcp"
        # probe 的 launch_spec.env 是变量名列表（.mcp.json env 的键），展开成
        # {name: ""} 占位字典——实值由团队引用 params 提供（_mcp_spec_from_params
        # 的 params.env 优先覆盖）。remote 形态 env 恒为 {}，仅 stdio 有意义。
        resource_data = github_recognize.build_mcp_resource_data(launch)
        if not resource_data:
            raise HTTPException(422, "未在该仓库识别到 MCP 启动证据（.mcp.json / README npx·uvx 提示）")
        association = []
        editors = []
        name = name_override or full_name.split("/")[-1]

    # 先落池：upsert resources 行（github_recognize 来源，按 repo_full_name 去重）。
    # 不覆盖 status——识别永远不能替策展发布（与榜单 store 同一不变量）。
    resource = await resource_store.upsert_resource_by_source(
        "github_recognize", "repo_full_name", full_name,
        {
            "resource_type": resource_type,
            "resource_data": resource_data,
            "association": association,
            "editors": editors,
            "source_data": {
                "repo_full_name": full_name,
                "ref": use_ref,
                "head_sha": head_sha,
                "recognized_type": kind,
            },
            "name": name,
            "display_name": display_name_override or name,
            "description": description_override,
            "version": head_sha or use_ref,
        },
    )

    # 再引用：resource_references FK 池行；version 钉 commit（空=跟分支浮动）。
    # params 仅 mcp 用（团队私有 token/headers/env 连接凭证）；其余类型恒空。
    raw_params = body.get("params") if isinstance(body.get("params"), dict) else {}
    try:
        reference = await resource_store.create_reference(
            team_id=team_id,
            resource_id=resource["id"],
            params=raw_params if kind == "mcp" else {},
            display_name=display_name_override or name,
            description=description_override,
            version=pin_commit or "",
            created_by=str(user.id),
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return _envelope(reference, "已引用（新 schema：先落池再引用）")


@router.get("/v2/references")
async def list_references_v2(
    resource_type: str | None = None,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """列团队引用（新表：resource_references JOIN resources，含本体投影）。

    ``resource_type`` 兼容旧枚举：skill → 同时返回 skill 与 skills（集合）；
    project_prompt → prompt。管理端语义：管理员全量、成员按分组授权——
    走与 resolve 相同的可见性口径（TeamMember 角色 + resource_grants）。
    """
    import resource_store
    from .resource_reference_service import _team_is_admin

    requested = str(resource_type or "").strip()
    types: list[str] = []
    if requested:
        base = {"project_prompt": "prompt"}.get(requested, requested)
        types.append(base)
        if base == "skill":
            types.append("skills")
    is_admin = await _team_is_admin(str(user.id), team_id)
    rows: list[dict] = []
    seen: set[str] = set()
    for t in (types or [None]):
        for row in await resource_store.visible_references(
            str(user.id), team_id, resource_type=t, is_admin=is_admin,
        ):
            if row["id"] not in seen:
                seen.add(row["id"])
                rows.append(row)
    return _envelope(rows)


@router.delete("/v2/references/{reference_id}")
async def delete_reference_v2(
    reference_id: str,
    _: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """取消团队引用（新表）。有分组授权时 409（先撤授权再删）。"""
    import resource_store

    try:
        deleted = await resource_store.delete_reference(team_id, reference_id)
    except ValueError as exc:
        if str(exc) == "reference_has_grants":
            raise HTTPException(409, "资源仍分配给团队分组，请先撤销分配") from exc
        raise
    if not deleted:
        raise HTTPException(404, "reference not found")
    return _envelope({"deleted": True})


@router.get("/v2/groups/{group_id}/references")
async def list_group_references_v2(
    group_id: str,
    resource_type: str | None = None,
    _: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """列分组被授权的 v2 引用（新表 resource_grants）。

    ``resource_type`` 兼容旧枚举：skill → 同时返回 skill 与 skills（集合）；
    project_prompt → prompt。管理端语义：管理员全量、成员按分组授权——
    走与 resolve 相同的可见性口径（TeamMember 角色 + resource_grants）。
    """
    import resource_store
    from .models import TeamGroup

    # 验证分组是否存在于团队
    try:
        group_uuid = uuid.UUID(group_id)
        team_uuid = uuid.UUID(team_id)
    except Exception as exc:
        raise HTTPException(404, "分组不存在") from exc
    if await TeamGroup.get_or_none(id=group_uuid, team_id=team_uuid, is_deleted=False) is None:
        raise HTTPException(404, "分组不存在")

    # 获取分组授权的 v2 引用；如果 resource_type 为空或 skill，需合并 skills 集合
    requested = str(resource_type or "").strip()
    types: list[str] = []
    if requested:
        base = {"project_prompt": "prompt"}.get(requested, requested)
        types.append(base)
        if base == "skill":
            types.append("skills")
    rows: list[dict] = []
    seen: set[str] = set()
    for t in types:
        for row in await resource_store.list_group_references(team_id, group_id, resource_type=t):
            if row["id"] not in seen:
                seen.add(row["id"])
                rows.append(row)
    return _envelope(rows)


@router.put("/v2/groups/{group_id}/references")
async def set_group_references_v2(
    group_id: str,
    request: Request,
    body: dict = Body(...),
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """按类型整组替换分组的 v2 引用授权（新表 resource_grants）。

    Body: ``{"reference_ids": ["<uuid>", ...], "resource_type": "skill|plugin|..."}``。
    ``resource_type`` 非空时只替换该类型（skill 同时覆盖 skills 集合），不会误清
    其他类型的授权；缺省为全量替换。引用必须属于本团队，否则 409。
    """
    import resource_store

    reference_ids = body.get("reference_ids") if isinstance(body.get("reference_ids"), list) else []
    # 前端旧枚举 → 新表枚举（project_prompt→prompt；skill 覆盖 skill+skills）。
    requested = str(body.get("resource_type") or "").strip()
    store_type = {"project_prompt": "prompt"}.get(requested, requested) or None
    rows = await resource_store.set_group_grants(
        team_id=team_id,
        group_id=group_id,
        reference_ids=[str(v) for v in reference_ids],
        created_by=str(user.id),
        resource_type=store_type,
    )
    return _envelope(rows)


@router.get("/groups/{group_id}/{resource_type}")
async def list_group_resource_grants(
    group_id: str,
    resource_type: str,
    _: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        rows = await references.list_group_resources(team_id, group_id, resource_type)
    except ValueError as exc:
        raise HTTPException(404, "分组不存在") from exc
    return _envelope(rows)


@router.put("/groups/{group_id}/{resource_type}")
async def set_group_resource_grants(
    group_id: str,
    resource_type: str,
    request: Request,
    body: dict = Body(...),
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    resource_ids = body.get("resource_ids") if isinstance(body.get("resource_ids"), list) else []
    try:
        rows = await references.set_group_resources(
            team_id, group_id, resource_type, [str(value) for value in resource_ids],
            created_by=str(user.id),
        )
    except ValueError as exc:
        message = str(exc)
        if message == "group_not_found":
            raise HTTPException(404, "分组不存在") from exc
        if message == "resource_not_referenced":
            raise HTTPException(409, "只能分配本团队已引用的资源") from exc
        raise HTTPException(400, message) from exc
    return _envelope(rows)


@router.get("/effective/{resource_type}")
async def list_effective_resources(
    resource_type: str,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    if resource_type not in references.RESOURCE_TYPES:
        raise HTTPException(400, "unsupported resource type")
    rows = await references.visible_references(str(user.id), team_id, resource_type)
    return _envelope(rows)
