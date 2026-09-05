"""Team-owned resource references and group grants."""
from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Request

from .deps import get_current_team_id, get_current_user
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
