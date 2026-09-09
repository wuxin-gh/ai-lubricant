"""项目提示词：管理端 CRUD（系统提示词） + 用户端 CRUD（私有提示词）与只读列表。

系统提示词 ``owner_user_id IS NULL``，由管理端维护；用户私有提示词带 ``owner_user_id``，
用户在编辑器配置弹框内自行增删改。编辑器绑定 prompt_id 后，写入工作目录的
CLAUDE.md / AGENTS.md（见 routes_editors / routes_project）。
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .deps import get_current_user
from .models import User
from . import project_prompt_store

admin_router = APIRouter(prefix="/api/v1/admin/project-prompts", tags=["project-prompts-admin"])
user_router = APIRouter(prefix="/api/v1/users/project-prompts", tags=["project-prompts-user"])

_ALLOWED_PROVIDERS = {"claude", "codex", "opencode", "cursor"}


async def _require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


class CreatePromptReq(BaseModel):
    name: str
    content: str = ""
    providers: list[str] = Field(default_factory=list)
    enabled: bool = True


class UpdatePromptReq(BaseModel):
    name: str | None = None
    content: str | None = None
    providers: list[str] | None = None
    enabled: bool | None = None


def _clean_providers(values: list[str]) -> list[str]:
    cleaned = list(dict.fromkeys(str(value).strip().lower() for value in values if str(value).strip()))
    unsupported = [value for value in cleaned if value not in _ALLOWED_PROVIDERS]
    if unsupported:
        raise HTTPException(status_code=422, detail=f"不支持的编辑器类型: {', '.join(unsupported)}")
    return cleaned


# ── 管理端：系统提示词（owner_user_id=NULL） ────────────────────────────────────

@admin_router.get("")
async def admin_list_prompts(_: User = Depends(_require_admin)) -> list[dict]:
    return await project_prompt_store.list_admin_prompts()


@admin_router.post("")
async def admin_create_prompt(body: CreatePromptReq, _: User = Depends(_require_admin)) -> dict:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="提示词名称不能为空")
    return await project_prompt_store.create_prompt(
        name=name, content=body.content,
        providers=_clean_providers(body.providers), enabled=body.enabled,
    )


@admin_router.patch("/{prompt_id}")
async def admin_update_prompt(prompt_id: str, body: UpdatePromptReq, _: User = Depends(_require_admin)) -> dict:
    patch = body.model_dump(exclude_unset=True)
    if "name" in patch:
        patch["name"] = str(patch["name"] or "").strip()
        if not patch["name"]:
            raise HTTPException(status_code=422, detail="提示词名称不能为空")
    if "providers" in patch:
        patch["providers"] = _clean_providers(patch["providers"] or [])
    updated = await project_prompt_store.update_prompt(prompt_id, patch)
    if not updated:
        raise HTTPException(status_code=404, detail="项目提示词不存在")
    return updated


@admin_router.delete("/{prompt_id}")
async def admin_delete_prompt(prompt_id: str, _: User = Depends(_require_admin)) -> dict:
    if not await project_prompt_store.delete_prompt(prompt_id):
        raise HTTPException(status_code=404, detail="项目提示词不存在")
    return {"deleted": True}


# ── 用户端：只读系统启用项 + 自己的私有项 CRUD ─────────────────────────────────

@user_router.get("")
async def list_available_prompts(user: User = Depends(get_current_user)) -> list[dict]:
    return await project_prompt_store.list_prompts(owner_user_id=str(user.id))


@user_router.post("")
async def create_my_prompt(body: CreatePromptReq, user: User = Depends(get_current_user)) -> dict:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="提示词名称不能为空")
    return await project_prompt_store.create_prompt(
        name=name, content=body.content,
        providers=_clean_providers(body.providers), enabled=body.enabled,
        owner_user_id=str(user.id),
    )


@user_router.patch("/{prompt_id}")
async def update_my_prompt(prompt_id: str, body: UpdatePromptReq, user: User = Depends(get_current_user)) -> dict:
    patch = body.model_dump(exclude_unset=True)
    if "name" in patch:
        patch["name"] = str(patch["name"] or "").strip()
        if not patch["name"]:
            raise HTTPException(status_code=422, detail="提示词名称不能为空")
    if "providers" in patch:
        patch["providers"] = _clean_providers(patch["providers"] or [])
    updated = await project_prompt_store.update_prompt(prompt_id, patch, owner_user_id=str(user.id))
    if not updated:
        raise HTTPException(status_code=404, detail="项目提示词不存在或不属于你")
    return updated


@user_router.delete("/{prompt_id}")
async def delete_my_prompt(prompt_id: str, user: User = Depends(get_current_user)) -> dict:
    if not await project_prompt_store.delete_prompt(prompt_id, owner_user_id=str(user.id)):
        raise HTTPException(status_code=404, detail="项目提示词不存在或不属于你")
    return {"deleted": True}
