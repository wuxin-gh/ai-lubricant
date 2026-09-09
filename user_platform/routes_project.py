"""C-side project domain routes (``/api/v1/users/projects``).

Mirrors the upstream project handler contract: projects + nested issues,
collaborators and issue comments. Access control (owner / collaborator /
privileged) lives in ``project_service``. Storage only — never touches the
model request pipeline.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import mimetypes

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from .deps import audit_user_action, get_current_user
from .models import User
from .project_service import IssueTransitionError, project_service
from .routes_editors import (
    CreateEditorReq,
    CreateSessionReq,
    _create_editor_session_core,
    _redact_editor,
    _redact_sessions,
    _validated_editor_create_payload,
)
from db import PostgresClient

router = APIRouter(prefix="/api/v1/users/projects", tags=["user-platform-projects"])

# 技术栈扫描的后台任务引用表（仿 channel_import_jobs._tasks）：asyncio 只持弱
# 引用，fire-and-forget 不留引用可能被 GC 中途丢弃；done_callback 自清理。
# 同项目重扫先 cancel 旧任务，天然去重。
_scan_tasks: dict[str, asyncio.Task] = {}


def _spawn_stack_scan(project_id: str) -> None:
    """建项目后即异步扫描技术栈。绝不抛、绝不阻塞响应；无运行 loop（脚本/
    测试环境）时静默跳过——手动重扫接口是兜底路径。"""
    prev = _scan_tasks.get(project_id)
    if prev is not None and not prev.done():
        prev.cancel()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = loop.create_task(
        project_service.scan_stack_profile(project_id),
        name=f"stack-scan-{project_id}",
    )
    _scan_tasks[project_id] = task
    task.add_done_callback(lambda _t, pid=project_id: _scan_tasks.pop(pid, None))


class CreateProjectReq(BaseModel):
    name: str
    description: str | None = None
    platform: str | None = None
    repo_url: str | None = None
    branch: str | None = None
    git_identity_id: str | None = None
    env_variables: dict | None = None
    is_team_shared: bool = False


class UpdateProjectReq(BaseModel):
    name: str | None = None
    description: str | None = None
    platform: str | None = None
    repo_url: str | None = None
    branch: str | None = None
    env_variables: dict | None = None
    is_team_shared: bool | None = None


class AddCollaboratorReq(BaseModel):
    user_id: str
    role: str | None = "read_only"


class AddAssociationReq(BaseModel):
    target_project_id: str
    relation: str | None = "related"
    target_subdir: str | None = None


class CreateIssueReq(BaseModel):
    title: str
    # requirement | bug —— 服务端归一化，未知值落回 requirement
    type: str | None = None
    requirement_document: str | None = None
    design_document: str | None = None
    bug_reason: str | None = None
    pending_items: list | None = None
    resolution_note: str | None = None
    summary: str | None = None
    assignee_id: str
    priority: int | None = None
    tags: list[str] | None = None


class UpdateIssueReq(BaseModel):
    title: str | None = None
    status: str | None = None
    requirement_document: str | None = None
    design_document: str | None = None
    bug_reason: str | None = None
    pending_items: list | None = None
    resolution_note: str | None = None
    summary: str | None = None
    assignee_id: str | None = None
    priority: int | None = None
    tags: list[str] | None = None


class AddCommentReq(BaseModel):
    comment: str
    parent_id: str | None = None


class AssignIssueReq(BaseModel):
    # Runtime selections are the same fields accepted by CreateTaskReq. Mode is
    # provider/editor-defined and therefore deliberately not an enum.
    node_id: str | None = None
    model_id: str | None = None
    cli_name: str | None = None
    mode: str | None = None
    mode_label: str | None = None
    mode_capability_snapshot: dict | None = None
    git_identity_id: str | None = None
    assignee_id: str | None = None
    # 创建任务弹框里的可编辑内容和任务类型；服务端仍按 issue 类型校验角色映射。
    content: str | None = None
    task_role: str | None = None
    sub_type: str | None = None
    task_type: str | None = None
    branch: str | None = None
    skill_ids: list[str] | None = None
    parent_api_key_id: int | None = None
    usage_limit: dict | None = None
    expires_at: float | None = None
    expected_client_id: str | None = None
    bootstrap_content: str | None = None
    skill_config: list[dict] | None = None
    mcp_config: list[dict] | None = None
    plugin_config: list[dict] | None = None


class ConfirmIssueReq(BaseModel):
    approve: bool = True
    note: str | None = None
    start_task: bool = False
    node_id: str | None = None
    model_id: str | None = None
    cli_name: str | None = None
    mode: str | None = None
    mode_label: str | None = None
    mode_capability_snapshot: dict | None = None
    git_identity_id: str | None = None
    parent_api_key_id: int | None = None
    usage_limit: dict | None = None
    expires_at: float | None = None
    expected_client_id: str | None = None
    bootstrap_content: str | None = None


# ---- Projects -------------------------------------------------------------
@router.get("")
async def list_projects(
    user: User = Depends(get_current_user),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=200),
) -> dict:
    result = await project_service.list_projects(
        str(user.id), role=user.role, page=page, page_size=page_size
    )
    rows = list(result.rows)
    # 编辑器明细走一次批量查询覆盖当前页，避免每行一次 N+1。
    project_ids = [str(row["id"]) for row in rows if row.get("id")]
    if project_ids:
        summaries = await PostgresClient.list_editor_summaries_for_projects(project_ids)
        for row in rows:
            editors = summaries.get(str(row.get("id"))) or []
            row["editors"] = editors
            row["editor_count"] = len(editors)
    return {
        "total": result.total,
        "page": result.page,
        "page_size": result.page_size,
        "rows": rows,
    }


@router.post("")
async def create_project(
    body: CreateProjectReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        result = await project_service.create_project(
            str(user.id), body.model_dump(exclude_none=True)
        )
    except ValueError as exc:
        if str(exc) == "name_required":
            raise HTTPException(status_code=400, detail="缺少项目名称") from exc
        raise HTTPException(status_code=400, detail="创建项目失败") from exc
    await audit_user_action(
        request, user, "project.create",
        request_body=body.model_dump(exclude_none=True), response=result,
    )
    # 技术栈自动识别：fire-and-forget，结果落库后由项目页/管理列表直接读。
    _spawn_stack_scan(result["id"])
    return result


@router.get("/{project_id}")
async def get_project(project_id: str, user: User = Depends(get_current_user)) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return project


@router.get("/{project_id}/stack")
async def get_project_stack(project_id: str, user: User = Depends(get_current_user)) -> dict:
    """返回缓存的技术栈 profile。null = 未扫/扫描中/扫描失败。

    鉴权复用 get_project（任意访问级别可读，识别是只读缓存）。
    """
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return {"stack": project.get("stack")}


@router.post("/{project_id}/stack/rescan")
async def rescan_project_stack(
    project_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """同步重扫技术栈并返回新 profile。任意访问级别可触发——只刷新缓存，
    不写敏感数据。"""
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    profile = await project_service.scan_stack_profile(project_id)
    await audit_user_action(
        request, user, "project.stack.rescan", response={"project_id": project_id}
    )
    return {"stack": profile}


@router.put("/{project_id}")
async def update_project(
    project_id: str, body: UpdateProjectReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await project_service.update_project(
        str(user.id), project_id, body.model_dump(exclude_none=True), role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    await audit_user_action(
        request, user, "project.update",
        request_body={"project_id": project_id, **body.model_dump(exclude_none=True)},
        response={"ok": True},
    )
    return {"ok": True}


@router.delete("/{project_id}")
async def delete_project(
    project_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await project_service.delete_project(str(user.id), project_id, role=user.role)
    if not ok:
        raise HTTPException(status_code=404, detail="项目不存在或非所有者")
    await audit_user_action(
        request, user, "project.delete",
        request_body={"project_id": project_id}, response={"deleted": True},
    )
    return {"deleted": True}


@router.get("/{project_id}/editors")
async def list_project_editors(project_id: str, user: User = Depends(get_current_user)) -> list[dict]:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    rows = await PostgresClient.list_project_editors(project_id)
    for row in rows:
        row["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(row["id"]))
    return [_redact_editor(row) for row in rows]


@router.post("/{project_id}/editors")
async def create_project_editor_plural(
    project_id: str, body: CreateEditorReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    return await create_project_editor(project_id, body, request, user)


@router.get("/{project_id}/editors/{editor_id}")
async def get_project_editor_detail(project_id: str, editor_id: str, user: User = Depends(get_current_user)) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    editor = await PostgresClient.get_project_editor_by_id(editor_id, project_id) if project else None
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在或无权访问")
    editor["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(editor_id))
    return _redact_editor(editor)


@router.post("/{project_id}/editors/{editor_id}/duplicate")
async def duplicate_project_editor(project_id: str, editor_id: str, request: Request, user: User = Depends(get_current_user)) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None or project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    try:
        result = await PostgresClient.duplicate_editor_for_user(editor_id, project_id, str(user.id))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not result:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    await audit_user_action(request, user, "project.editor.duplicate", request_body={"project_id": project_id, "editor_id": editor_id}, response={"id": result["id"]})
    return _redact_editor(result)


@router.delete("/{project_id}/editors/{editor_id}")
async def delete_project_editor(project_id: str, editor_id: str, request: Request, user: User = Depends(get_current_user)) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None or project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    ok = await PostgresClient.close_editor(editor_id, str(user.id))
    if not ok:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    await audit_user_action(request, user, "project.editor.delete", request_body={"project_id": project_id, "editor_id": editor_id}, response={"deleted": True})
    return {"deleted": True, "editor_id": editor_id}


@router.post("/{project_id}/editors/{editor_id}/sessions")
async def create_project_editor_session_plural(
    project_id: str,
    editor_id: str,
    body: CreateSessionReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """Create a session under a concrete project editor.

    Validates project access and that the editor belongs to the project, then
    reuses the standard editor-session dispatch flow.
    """
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None or project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    editor = await PostgresClient.get_project_editor_by_id(editor_id, project_id)
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在或不属于当前项目")
    return await _create_editor_session_core(editor_id, body, request, user, editor)


@router.get("/{project_id}/editor")
async def get_project_editor(project_id: str, user: User = Depends(get_current_user)) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    editor = await PostgresClient.get_project_editor(project_id)
    if not editor:
        return {"project_id": project_id, "editor": None}
    editor["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(editor["id"]))
    return {"project_id": project_id, "editor": _redact_editor(editor), "access_role": project.get("access_role")}


@router.post("/{project_id}/editor")
async def create_project_editor(
    project_id: str,
    body: CreateEditorReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None or project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    if body.project_id and body.project_id != project_id:
        raise HTTPException(status_code=400, detail="编辑器项目归属不一致")
    try:
        payload = _validated_editor_create_payload(body)
        result = await PostgresClient.create_editor_only(
            {"owner_user_id": str(user.id), **payload, "project_id": project_id}
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # 创建时若选了提示词，尽力写入工作目录的 CLAUDE.md / AGENTS.md；workspace 可能尚未就绪，
    # 写失败不阻断创建（保存配置时仍会按 resync 流程写并上报）。
    prompt_id = (payload.get("prompt_id") or "").strip()
    if prompt_id:
        try:
            from . import project_prompt_store
            from .routes_editors_workspace import write_editor_prompt_file

            prompt = await project_prompt_store.get_prompt(prompt_id)
            if prompt:
                await write_editor_prompt_file(result, prompt.get("content") or "")
        except Exception:  # noqa: BLE001 - 创建期写文件失败不阻断
            pass
    await audit_user_action(
        request, user, "project.editor.create",
        request_body={"project_id": project_id, "provider": body.provider},
        response={"id": result["id"], "provider": result["provider"]},
    )
    return _redact_editor(result)


@router.get("/{project_id}/editor/sessions")
async def list_project_editor_sessions(project_id: str, user: User = Depends(get_current_user)) -> list[dict]:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    editor = await PostgresClient.get_project_editor(project_id)
    if not editor:
        return []
    rows = await PostgresClient.list_editor_sessions(editor["id"])
    for row in rows:
        row["provider"] = editor["provider"]
    return _redact_sessions(rows)


@router.patch("/{project_id}/editor")
async def update_project_editor(
    project_id: str,
    body: UpdateEditorReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None or project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    editor = await PostgresClient.get_project_editor(project_id)
    if not editor:
        raise HTTPException(status_code=404, detail="项目尚未配置编辑器")
    updated = await PostgresClient.update_editor_for_user(editor["id"], str(user.id), body.model_dump(exclude_none=True))
    if not updated:
        raise HTTPException(status_code=409, detail="编辑器当前不可修改")

    # 项目提示词写入工作目录的 CLAUDE.md / AGENTS.md（先持久化 prompt_id 再写文件）。
    resync: dict[str, list[str]] = {"applied": [], "failed": []}
    patch = body.model_dump(exclude_none=True)
    if "prompt_id" in patch:
        prompt_id = (patch.get("prompt_id") or "").strip()
        if prompt_id:
            try:
                from . import project_prompt_store
                from .routes_editors_workspace import write_editor_prompt_file

                prompt = await project_prompt_store.get_prompt(prompt_id)
                if not prompt:
                    resync["failed"].append(f"prompt {prompt_id}: 提示词不存在")
                else:
                    filename = await write_editor_prompt_file(updated, prompt.get("content") or "")
                    resync["applied"].append(f"prompt:{filename}")
            except Exception as exc:  # noqa: BLE001 - 写文件失败要如实上报
                resync["failed"].append(f"prompt {prompt_id}: {exc}")

    await audit_user_action(request, user, "project.editor.update", request_body={"project_id": project_id}, response={"id": editor["id"]})
    updated["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(editor["id"]))
    result = _redact_editor(updated)
    result["config_resync"] = resync
    return result


async def create_project_editor_session(
    project_id: str,
    body: CreateSessionReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None or project.get("access_role") not in ("owner", "read_write"):
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    editor = await PostgresClient.get_project_editor(project_id)
    if not editor:
        raise HTTPException(status_code=404, detail="项目尚未配置编辑器")
    return await _create_editor_session_core(editor["id"], body, request, user, editor)


@router.get("/{project_id}/tree/blob/raw")
async def get_tree_blob_raw(
    project_id: str,
    path: str = Query(..., description="图片文件路径"),
    ref: str = Query("", description="分支/引用"),
    user: User = Depends(get_current_user),
) -> Response:
    """Return a repository image as bytes for README-relative ``img`` sources."""
    media_type = (mimetypes.guess_type(path)[0] or "").lower()
    allowed_types = {
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/webp",
        "image/avif",
        "image/x-icon",
        "image/vnd.microsoft.icon",
    }
    if media_type not in allowed_types:
        raise HTTPException(status_code=415, detail="仅支持安全的栅格图片格式")
    blob = await project_service.get_blob(
        str(user.id), project_id, role=user.role, path=path, ref=ref
    )
    content = str((blob or {}).get("content") or "")
    if not content:
        raise HTTPException(status_code=404, detail="图片不存在或无权访问")
    try:
        raw = base64.b64decode(content, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(status_code=502, detail="仓库图片内容无效") from exc
    return Response(
        content=raw,
        media_type=media_type,
        headers={
            "Content-Disposition": "inline",
            "Cache-Control": "private, max-age=300",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/{project_id}/tree/blob")
async def get_tree_blob(
    project_id: str,
    path: str = Query(..., description="文件路径"),
    ref: str = Query("", description="分支/引用"),
    user: User = Depends(get_current_user),
) -> dict:
    blob = await project_service.get_blob(
        str(user.id), project_id, role=user.role, path=path, ref=ref
    )
    if blob is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return blob


@router.get("/{project_id}/tree")
async def get_tree(
    project_id: str,
    recursive: bool = Query(False, description="是否递归"),
    ref: str = Query("", description="分支/引用"),
    path: str = Query("", description="路径"),
    user: User = Depends(get_current_user),
) -> list[dict]:
    entries = await project_service.get_tree(
        str(user.id), project_id, role=user.role, ref=ref, path=path, recursive=recursive
    )
    if entries is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return entries


@router.get("/{project_id}/submodules")
async def list_submodules(
    project_id: str,
    ref: str = Query("", description="分支/引用"),
    user: User = Depends(get_current_user),
) -> list[dict]:
    """Git 子模块（读 .gitmodules 派生，不落库）。"""
    rows = await project_service.list_submodules(
        str(user.id), project_id, role=user.role, ref=ref
    )
    if rows is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return rows


# ---- Collaborators --------------------------------------------------------
@router.get("/{project_id}/collaborators")
async def list_collaborators(project_id: str, user: User = Depends(get_current_user)) -> list[dict]:
    rows = await project_service.list_collaborators(str(user.id), project_id, role=user.role)
    if rows is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return rows


@router.post("/{project_id}/collaborators")
async def add_collaborator(
    project_id: str, body: AddCollaboratorReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        collab = await project_service.add_collaborator(
            str(user.id), project_id, body.user_id, body.role or "read_only", role=user.role
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="无效的用户 ID") from exc
    if collab is None:
        raise HTTPException(status_code=404, detail="项目不存在或非所有者")
    await audit_user_action(
        request, user, "project.add_collaborator",
        request_body={"project_id": project_id, **body.model_dump(exclude_none=True)},
        response=collab,
    )
    return collab


@router.delete("/{project_id}/collaborators/{target_user_id}")
async def remove_collaborator(
    project_id: str, target_user_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await project_service.remove_collaborator(
        str(user.id), project_id, target_user_id, role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="协作者不存在或非所有者")
    await audit_user_action(
        request, user, "project.remove_collaborator",
        request_body={"project_id": project_id, "user_id": target_user_id},
        response={"deleted": True},
    )
    return {"deleted": True}


# ---- Associations ---------------------------------------------------------
@router.get("/{project_id}/associations")
async def list_associations(project_id: str, user: User = Depends(get_current_user)) -> list[dict]:
    rows = await project_service.list_associations(str(user.id), project_id, role=user.role)
    if rows is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return rows


@router.post("/{project_id}/associations")
async def add_association(
    project_id: str, body: AddAssociationReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        assoc = await project_service.add_association(
            str(user.id),
            project_id,
            body.target_project_id,
            body.relation or "related",
            body.target_subdir,
            role=user.role,
        )
    except ValueError as exc:
        detail = {
            "invalid_target_project_id": "无效的关联项目 ID",
            "self_association_not_allowed": "不能关联项目自身",
            "target_project_not_found": "关联项目不存在",
        }.get(str(exc), "无效的关联参数")
        raise HTTPException(status_code=400, detail=detail) from exc
    if assoc is None:
        raise HTTPException(status_code=404, detail="项目不存在或非所有者")
    await audit_user_action(
        request, user, "project.add_association",
        request_body={"project_id": project_id, **body.model_dump(exclude_none=True)},
        response=assoc,
    )
    return assoc


@router.delete("/{project_id}/associations/{target_project_id}")
async def remove_association(
    project_id: str, target_project_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await project_service.remove_association(
        str(user.id), project_id, target_project_id, role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="关联不存在或非所有者")
    await audit_user_action(
        request, user, "project.remove_association",
        request_body={"project_id": project_id, "target_project_id": target_project_id},
        response={"deleted": True},
    )
    return {"deleted": True}


# ---- Issues ---------------------------------------------------------------
@router.get("/{project_id}/issues")
async def list_issues(
    project_id: str,
    user: User = Depends(get_current_user),
    type: str | None = Query(None),
    status: str | None = Query(None),
    priority: int | None = Query(None),
    assigned_to_me: bool = Query(False),
) -> list[dict]:
    try:
        rows = await project_service.list_issues(
            str(user.id), project_id, role=user.role,
            issue_type=type, status=status, priority=priority,
            only_assigned_to_me=assigned_to_me,
        )
    except ValueError as exc:
        detail = {
            "invalid_issue_type": "类型必须是 requirement 或 bug",
            "invalid_priority": "优先级必须是 1、2 或 3",
        }.get(str(exc), "筛选参数不合法")
        raise HTTPException(status_code=400, detail=detail) from exc
    if rows is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return rows


@router.post("/{project_id}/issues")
async def create_issue(
    project_id: str, body: CreateIssueReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        issue = await project_service.create_issue(
            str(user.id), project_id, body.model_dump(exclude_none=True), role=user.role
        )
    except ValueError as exc:
        detail = {
            "title_required": "缺少 Issue 标题",
            "assignee_required": "请选择分配者",
        }.get(str(exc), str(exc))
        raise HTTPException(status_code=400, detail=detail) from exc
    if issue is None:
        raise HTTPException(status_code=404, detail="项目不存在或无写权限")
    await audit_user_action(
        request, user, "issue.create",
        request_body={"project_id": project_id, **body.model_dump(exclude_none=True)},
        response=issue,
    )
    return issue


@router.put("/{project_id}/issues/{issue_id}")
async def update_issue(
    project_id: str, issue_id: str, body: UpdateIssueReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        ok = await project_service.update_issue(
            str(user.id), project_id, issue_id, body.model_dump(exclude_none=True), role=user.role
        )
    except IssueTransitionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not ok:
        raise HTTPException(status_code=404, detail="Issue 不存在或无写权限")
    await audit_user_action(
        request, user, "issue.update",
        request_body={"project_id": project_id, "issue_id": issue_id, **body.model_dump(exclude_none=True)},
        response={"ok": True},
    )
    return {"ok": True}


@router.post("/{project_id}/issues/{issue_id}/assign")
async def assign_issue(
    project_id: str,
    issue_id: str,
    body: AssignIssueReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await project_service.assign_issue(
            str(user.id), project_id, issue_id,
            body.model_dump(exclude_none=True), role=user.role,
        )
    except IssueTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        reason = str(exc)
        detail = {
            "node_required": "请选择一个执行节点",
            "node_forbidden": "无权使用该节点",
            "node_occupied": "该节点已被占用",
        }.get(reason, "分配失败")
        raise HTTPException(status_code=409, detail=detail) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Issue 不存在或无写权限")
    await audit_user_action(
        request, user, "issue.assign",
        request_body={"project_id": project_id, "issue_id": issue_id,
                      **body.model_dump(exclude_none=True)},
        response=result,
    )
    return result


class ReassignIssueReq(BaseModel):
    assignee_id: str


@router.post("/{project_id}/issues/{issue_id}/reassign")
async def reassign_issue(
    project_id: str,
    issue_id: str,
    body: ReassignIssueReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """改派需求：只换 assignee，不 spawn task、不推状态（区别于 assign）。"""
    try:
        result = await project_service.reassign_issue(
            str(user.id), project_id, issue_id, body.assignee_id, role=user.role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="目标用户不合法") from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Issue 不存在或无写权限")
    await audit_user_action(
        request, user, "issue.reassign",
        request_body={"project_id": project_id, "issue_id": issue_id,
                      "assignee_id": body.assignee_id},
        response=result,
    )
    return result


@router.post("/{project_id}/issues/{issue_id}/confirm")
async def confirm_issue(
    project_id: str,
    issue_id: str,
    body: ConfirmIssueReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await project_service.confirm_issue(
            str(user.id), project_id, issue_id,
            body.model_dump(exclude_none=True), role=user.role,
        )
    except IssueTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        reason = str(exc)
        detail = {
            "node_required": "请选择一个执行节点",
            "node_forbidden": "无权使用该节点",
            "node_occupied": "该节点已被占用",
        }.get(reason, "确认失败")
        raise HTTPException(status_code=409, detail=detail) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Issue 不存在或无写权限")
    await audit_user_action(
        request, user, "issue.confirm",
        request_body={"project_id": project_id, "issue_id": issue_id,
                      **body.model_dump(exclude_none=True)},
        response=result,
    )
    return result


# ---- Issue comments -------------------------------------------------------
@router.get("/{project_id}/issues/{issue_id}/comments")
async def list_comments(
    project_id: str, issue_id: str, user: User = Depends(get_current_user)
) -> list[dict]:
    rows = await project_service.list_comments(str(user.id), project_id, issue_id, role=user.role)
    if rows is None:
        raise HTTPException(status_code=404, detail="项目/Issue 不存在或无权访问")
    return rows


@router.post("/{project_id}/issues/{issue_id}/comments")
async def add_comment(
    project_id: str, issue_id: str, body: AddCommentReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        comment = await project_service.add_comment(
            str(user.id), project_id, issue_id, body.model_dump(exclude_none=True), role=user.role
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="评论内容不能为空") from exc
    if comment is None:
        raise HTTPException(status_code=404, detail="项目/Issue 不存在或无写权限")
    await audit_user_action(
        request, user, "issue.add_comment",
        request_body={"project_id": project_id, "issue_id": issue_id, **body.model_dump(exclude_none=True)},
        response=comment,
    )
    return comment
