"""C-side git domain routes (``/api/v1/users/git-identities`` + ``/git-bots``).

Mirrors the MonkeyCode git handler contract for identities and bots. Access
control + secret masking live in ``git_service``. Storage only — never touches
the model request pipeline.

Webhook ingestion (github/gitlab/gitea/gitee/codeup) depends on per-platform
signature verification + the agent-compose runtime and is layered on later
without blocking this CRUD core.
"""
from __future__ import annotations

from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from .deps import audit_user_action, get_current_user
from .git_service import git_service
from .git_clients import GitClientError
from .models import User

identity_router = APIRouter(
    prefix="/api/v1/users/git-identities", tags=["monkeycode-git-identities"]
)
bot_router = APIRouter(prefix="/api/v1/users/git-bots", tags=["monkeycode-git-bots"])


class AddIdentityReq(BaseModel):
    platform: str
    base_url: str | None = None
    access_token: str | None = None
    username: str | None = None
    email: str | None = None
    installation_id: int | None = None
    organization_id: str | None = None
    remark: str | None = None
    oauth_refresh_token: str | None = None


class UpdateIdentityReq(BaseModel):
    base_url: str | None = None
    username: str | None = None
    email: str | None = None
    organization_id: str | None = None
    remark: str | None = None
    access_token: str | None = None
    oauth_refresh_token: str | None = None


class CreateRepositoryReq(BaseModel):
    name: str
    description: str = ""
    private: bool = True
    # Optional org/namespace to create under; empty → authenticated user.
    owner: str = ""


class CreateBotReq(BaseModel):
    platform: str
    host_id: str
    name: str | None = None
    token: str | None = None
    secret_token: str | None = None


class UpdateBotReq(BaseModel):
    name: str | None = None
    host_id: str | None = None
    token: str | None = None
    secret_token: str | None = None


# ---- Git identities -------------------------------------------------------
@identity_router.get("")
async def list_identities(user: User = Depends(get_current_user)) -> list[dict]:
    return await git_service.list_identities(str(user.id), role=user.role)


@identity_router.get("/{identity_id}")
async def get_identity(
    identity_id: str,
    user: User = Depends(get_current_user),
    flush: bool = Query(False, description="是否刷新缓存"),
    page: int = Query(0, ge=0, description="页码（>0 时启用分页，目前 GitHub/GitLab 支持）"),
    size: int = Query(0, ge=0, le=100, description="每页数量（默认 20，上限 100）"),
    keyword: str = Query("", description="按仓库名关键字过滤"),
) -> dict:
    identity = await git_service.get_identity(
        str(user.id),
        identity_id,
        role=user.role,
        flush=flush,
        page=page,
        size=size,
        keyword=keyword,
    )
    if identity is None:
        raise HTTPException(status_code=404, detail="Git 身份不存在或无权访问")
    return identity


@identity_router.get("/{identity_id}/{escaped_repo_full_name:path}/branches")
async def list_repo_branches(
    identity_id: str,
    escaped_repo_full_name: str,
    user: User = Depends(get_current_user),
) -> list[dict]:
    """List branches for a repository accessible via the given identity.

    The frontend URL-encodes the repo full name once (``owner%2Frepo``) and the
    request layer encodes it again, so Starlette hands us a still-encoded value —
    unquote once to recover ``owner/repo``. The ``:path`` converter keeps any
    decoded slashes from truncating the segment.
    """
    repo_full_name = unquote(escaped_repo_full_name)
    branches = await git_service.list_branches(
        str(user.id), identity_id, repo_full_name, role=user.role
    )
    if branches is None:
        raise HTTPException(status_code=404, detail="Git 身份不存在或无权访问")
    return branches


@identity_router.post("/{identity_id}/repositories")
async def create_repository(
    identity_id: str,
    body: CreateRepositoryReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """Create a real remote repository, then return its canonical clone URL.

    This endpoint owns the credentials: the frontend sends only identity id and
    repository metadata. Upstream tokens never leave the server. The platform is
    inferred from the stored identity so callers cannot route a token to a
    different Git host.
    """
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="仓库名称不能为空")
    try:
        result = await git_service.create_repository(
            str(user.id),
            identity_id,
            name=name,
            description=body.description,
            private=body.private,
            owner=body.owner,
            role=user.role,
        )
    except ValueError as exc:
        reason = str(exc)
        if reason == "identity_not_found":
            raise HTTPException(status_code=404, detail="Git 身份不存在或无权访问") from exc
        if reason == "identity_missing_token":
            raise HTTPException(status_code=400, detail="Git 身份缺少 Access Token") from exc
        raise HTTPException(status_code=400, detail="创建远程仓库失败") from exc
    except GitClientError as exc:
        # GitClientError never contains the token; preserve the upstream status /
        # conflict detail so the user can fix a duplicate name or permission issue.
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    await audit_user_action(
        request,
        user,
        "git_repository.create",
        request_body={"identity_id": identity_id, **body.model_dump()},
        response=result,
    )
    return result


@identity_router.post("")
async def add_identity(
    body: AddIdentityReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        result = await git_service.add_identity(str(user.id), body.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="缺少 Git 平台类型") from exc
    await audit_user_action(
        request, user, "git_identity.create",
        request_body=body.model_dump(exclude_none=True), response=result,
    )
    return result


@identity_router.put("/{identity_id}")
async def update_identity(
    identity_id: str, body: UpdateIdentityReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await git_service.update_identity(
        str(user.id), identity_id, body.model_dump(exclude_none=True), role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Git 身份不存在或无权访问")
    await audit_user_action(
        request, user, "git_identity.update",
        request_body={"identity_id": identity_id, **body.model_dump(exclude_none=True)},
        response={"ok": True},
    )
    return {"ok": True}


@identity_router.delete("/{identity_id}")
async def delete_identity(
    identity_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await git_service.delete_identity(str(user.id), identity_id, role=user.role)
    if not ok:
        raise HTTPException(status_code=404, detail="Git 身份不存在或无权访问")
    await audit_user_action(
        request, user, "git_identity.delete",
        request_body={"identity_id": identity_id}, response={"deleted": True},
    )
    return {"deleted": True}


# ---- Git bots -------------------------------------------------------------
@bot_router.get("")
async def list_bots(user: User = Depends(get_current_user)) -> list[dict]:
    return await git_service.list_bots(str(user.id), role=user.role)


@bot_router.post("")
async def create_bot(
    body: CreateBotReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        result = await git_service.create_bot(str(user.id), body.model_dump(exclude_none=True))
    except ValueError as exc:
        reason = str(exc)
        if reason == "host_id_required":
            raise HTTPException(status_code=400, detail="缺少宿主机 ID") from exc
        raise HTTPException(status_code=400, detail="缺少 Git 平台类型") from exc
    await audit_user_action(
        request, user, "git_bot.create",
        request_body=body.model_dump(exclude_none=True), response=result,
    )
    return result


@bot_router.put("/{bot_id}")
async def update_bot(
    bot_id: str, body: UpdateBotReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await git_service.update_bot(
        str(user.id), bot_id, body.model_dump(exclude_none=True), role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Git Bot 不存在或无权访问")
    await audit_user_action(
        request, user, "git_bot.update",
        request_body={"bot_id": bot_id, **body.model_dump(exclude_none=True)},
        response={"ok": True},
    )
    return {"ok": True}


@bot_router.delete("/{bot_id}")
async def delete_bot(
    bot_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await git_service.delete_bot(str(user.id), bot_id, role=user.role)
    if not ok:
        raise HTTPException(status_code=404, detail="Git Bot 不存在或无权访问")
    await audit_user_action(
        request, user, "git_bot.delete",
        request_body={"bot_id": bot_id}, response={"deleted": True},
    )
    return {"deleted": True}
