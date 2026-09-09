"""Editor-instance and multi-session control-plane routes.

An editor is a concrete provider instance. Sessions are pre-registered under
that editor and are not interchangeable with task/node session bindings.
"""
from __future__ import annotations

import hashlib
import uuid
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from pydantic import BaseModel, Field

from db import PostgresClient
from .deps import audit_user_action, get_current_user, resolve_team_id
from .models import User
from .node_client import get_local_node_client

router = APIRouter(prefix="/api/v1/users/editors", tags=["user-platform-editors"])
admin_router = APIRouter(prefix="/api/v1/teams/editors", tags=["user-platform-editor-admin"])


def _merge_mcp_config(base: list[dict] | None, overlay: list[dict] | None) -> list[dict]:
    """Merge editor base MCP config with session overlay by name.

    Session overlay wins and is always preserved on hot resync. This prevents an
    editor-level MCP update from wiping per-task servers such as issue-workflow.
    """
    merged: dict[str, dict] = {}
    unnamed: list[dict] = []
    for item in list(base or []) + list(overlay or []):
        if not isinstance(item, dict):
            continue
        copy = dict(item)
        name = str(copy.get("name") or "").strip()
        if name:
            merged[name] = copy
        else:
            unnamed.append(copy)
    return unnamed + list(merged.values())


def _issue_workflow_mcp_entry(token: str) -> dict:
    from .config import gateway_base_url_for_nodes, settings

    base = gateway_base_url_for_nodes(settings)
    if not base:
        raise RuntimeError("未配置 issue-workflow MCP 服务地址（gateway_public_url）")
    return {
        "name": "issue-workflow",
        "type": "sse",
        "url": f"{base}/mcp/issue-workflow/sse?token={token}",
    }


async def _resolve_editor_mcps(editor: dict, request: Request, mcp_overlay: list[dict]) -> list[dict]:
    """把编辑器行存的 MCP 绑定列表解析成节点可用的安全 wire spec（现签 user
    identity token），并 merge per-session overlay（issue-workflow 等）。

    编辑器无 principal：mcp_config 列存的是服务绑定（service_id/resource_id），
    不是 spec。派发时现造 spec + 现签 token（不落库，token 明文只此一次下发节点）。
    overlay 已是 spec 形态（issue-workflow 自带 token），同名录出。空绑定短路
    （不签 token 不查库——编辑器没勾 MCP 是常态，别为它签 token）。
    """
    mcp_overlay = mcp_overlay or []
    if not (editor.get("mcp_config") or []) and not mcp_overlay:
        return []
    if not (editor.get("mcp_config") or []):
        return list(mcp_overlay)

    from .config import settings
    from .config import gateway_base_url_for_nodes
    from .resource_reference_service import resolve_reference_specs

    # 节点可达的网关基址：显式配置 > 节点服务器 host 回退 > 请求 origin 兜底。
    # gateway_base_url_for_nodes 处理「默认回环 + 远程节点」的组合；都拿不到
    # （未配置且无请求可兜）再退 _editor_gateway_endpoint。
    base = gateway_base_url_for_nodes(settings) or _editor_gateway_endpoint(request).removesuffix("/v1")
    base = base.rstrip("/")
    # user identity token：editor 会话用 user 型（网关 identity 分支 sse 走 can_use_service、
    # 内置走插件 owner 全量）。现签不落库，session 生命周期内有效。
    import builtin_tool_store
    _row, identity_token = await builtin_tool_store.issue_token(
        "user", str(editor.get("owner_user_id") or ""), display_token=False,
    )
    team_id = await resolve_team_id(str(editor.get("owner_user_id") or ""))
    specs = await resolve_reference_specs(
        str(editor.get("owner_user_id") or ""), team_id, "mcp",
        editor.get("mcp_config") or [],
        identity_token=identity_token, gateway_base_url=base,
    )
    return _merge_mcp_config(specs, mcp_overlay)


async def _require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user

_SESSION_SECRET_FIELDS = (
    "expected_client_id",
    "bootstrap_content_hash",
    "bootstrap_token_hash",
    "bootstrap_consumed",
)


def _redact_session(session: dict | None) -> dict | None:
    if not session:
        return session
    result = {k: v for k, v in session.items() if k not in _SESSION_SECRET_FIELDS}
    if "models" not in result:
        raw_models = result.pop("models_json", None)
        if isinstance(raw_models, str):
            import json

            try:
                raw_models = json.loads(raw_models)
            except (TypeError, ValueError):
                raw_models = []
        result["models"] = raw_models if isinstance(raw_models, list) else []
    return result


def _redact_sessions(sessions: list[dict]) -> list[dict]:
    return [_redact_session(s) for s in sessions]


EditorBranchMode = Literal["default", "existing", "auto"]
_EDITOR_BRANCH_MODES = {"default", "existing", "auto"}


class CreateEditorReq(BaseModel):
    provider: str
    project_id: str | None = None
    branch: str | None = None
    # default=跟随仓库默认分支(existing 主分支语义); existing=checkout 已有分支(branch 必填);
    # auto=首次初始化时从默认分支确定性创建 editor/<editor_id>。老数据按 branch 非空回填为 existing。
    branch_mode: EditorBranchMode = "default"
    workdir: str | None = None
    node_id: str | None = None
    name: str | None = None
    prompt_id: str | None = None
    # Key 归属反转后编辑器不再持有父 Key（父 Key 在建 session 时选）。
    # 老客户端可能仍传该字段：接受并忽略一版，避免 422。
    parent_api_key_id: int | None = None
    mcp_config: list[dict] = Field(default_factory=list)
    skill_config: list[dict] = Field(default_factory=list)
    plugin_config: list[dict] = Field(default_factory=list)


def _validated_editor_create_payload(body: CreateEditorReq) -> dict:
    payload = body.model_dump(exclude_none=True)
    payload.pop("parent_api_key_id", None)
    mode = payload.get("branch_mode") or "default"
    branch = str(payload.get("branch") or "").strip()
    if mode not in _EDITOR_BRANCH_MODES:
        raise ValueError("unsupported editor branch mode")
    if mode == "existing" and not branch:
        raise ValueError("existing branch mode requires branch")
    if mode != "existing":
        payload.pop("branch", None)
    else:
        payload["branch"] = branch
    return payload


def _editor_git_payload(editor: dict) -> dict:
    """Build the NodeGitSpec git dict from the editor's branch mode.

    default/existing pass an explicit branch (empty → repo HEAD); auto asks the
    node to create ``editor/<editor_id>`` off the default branch on first init.
    """
    mode = (editor.get("branch_mode") or "default").strip()
    if mode == "auto":
        # editor id is server-generated (ed_<uuid>); sanitize to a git-ref-safe
        # name defensively even though the format is already safe.
        editor_id = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in (editor.get("id") or "")).strip("-_")
        if not editor_id:
            editor_id = "editor"
        return {"branch": "", "createBranch": True, "newBranch": f"editor/{editor_id}"}
    branch = (editor.get("branch") or "").strip()
    return {"branch": branch}



def _redact_editor(editor: dict | None) -> dict | None:
    if not editor:
        return editor
    editor = dict(editor)
    editor["sessions"] = _redact_sessions(editor.get("sessions") or [])
    key_copy = editor.get("api_key_copy")
    if key_copy:
        key_copy = dict(key_copy)
        key_copy.pop("key", None)
        editor["api_key_copy"] = key_copy
    return editor


class UpdateEditorReq(BaseModel):
    name: str | None = None
    project_id: str | None = None
    branch: str | None = None
    workdir: str | None = None
    node_id: str | None = None
    prompt_id: str | None = None
    mcp_config: list[dict] | None = None
    skill_config: list[dict] | None = None
    plugin_config: list[dict] | None = None


class CreateSessionReq(BaseModel):
    model: str | None = None
    models: list[str] = Field(default_factory=list)
    parent_api_key_id: int | None = None
    usage_limit: dict | None = None
    expires_at: float | None = None
    expected_client_id: str | None = None
    first_content: str | None = None
    first_content_hash: str | None = None
    task_name: str | None = None
    # 任务意图沿用 issue 工作流角色，不扩展 TaskType 枚举。
    task_type: str | None = None
    task_role: str | None = None
    sub_type: str | None = None
    # 仅「分配任务」携带；服务端校验 issue 属于 editor 的项目，并自动注入 MCP。
    issue_id: str | None = None
    # provider 原生权限/审批模式（codex read-only/…，claude default/plan/…）。
    mode: str | None = None


class SendSessionMessageReq(BaseModel):
    content: str


class UpdateSessionReq(BaseModel):
    model: str | None = None
    models: list[str] | None = None
    task_name: str | None = None
    usage_limit: dict | None = None
    expires_at: float | None = None
    # 切换 provider 权限/审批模式；只落盘，下一轮 human_message 生效，不重启。
    mode: str | None = None

def _editor_gateway_endpoint(request: Request) -> str:
    """Return the gateway origin used by an editor runtime.

    This must be the **data service** (the model gateway that serves
    ``/v1/messages``), NOT ``node_server_public_url`` — that one is the node
    control plane, which has no ``/v1`` surface at all. Pointing a runtime there
    made every upstream call 404, so the agent produced nothing and the page
    span forever. ``gateway_public_url`` is the configured origin; empty falls
    back to the request's own origin, which is correct for same-host dev and
    behind a reverse proxy.
    """
    from . import config

    origin = (config.settings.gateway_public_url or "").strip().rstrip("/")
    if not origin:
        origin = f"{request.url.scheme}://{request.url.netloc}"
    return origin + "/v1"


async def resolve_allowed_parent_key_ids(user_id: str) -> set[int]:
    """Parent keys a user may clone a session key from."""
    allowed = {
        int(row.get("id") or 0)
        for row in await PostgresClient.list_api_keys_by_user(str(user_id))
    }
    from .routes import resolve_system_api_key_for_user

    allowed.update(
        int(row.get("id") or 0)
        for row in await resolve_system_api_key_for_user(str(user_id))
    )
    return allowed


async def _editor_llm_config(
    editor: dict, request: Request, model: str | None, session: dict | None = None
) -> dict:
    """Build the runtime LLM config for a session.

    Key ownership is per session; the editor's own key is only a fallback for
    sessions created before the reversal (they have no key of their own).

    这里是创建 session 与切换 session 模型的公共汇合点，也是唯一同时持有
    Key 行与模型名的位置——模型校验收口在此，避免非法模型被静默下发给节点、
    直到网关推理阶段才失败。
    """
    key_id = (session or {}).get("api_key_id") or editor.get("api_key_id")
    if not key_id:
        raise HTTPException(status_code=503, detail="会话 API Key 不可用")
    key = await PostgresClient.get_api_key_by_id(int(key_id))
    if not key or key.get("disabled") or not str(key.get("key") or "").strip():
        raise HTTPException(status_code=503, detail="会话 API Key 已失效")
    model_name = str(model or "").strip()
    if model_name:
        from model_catalog import is_internal_model_id
        from rate_limiter import ModelClientPool
        import config as gateway_config

        if is_internal_model_id(model_name):
            raise HTTPException(status_code=400, detail=f"模型 {model_name} 不可直接使用")
        if model_name not in ModelClientPool.available_model_ids():
            raise HTTPException(status_code=400, detail=f"模型 {model_name} 当前不可用")
        if not await gateway_config.Config.api_key_allows_model(str(key.get("key") or ""), model_name):
            raise HTTPException(status_code=403, detail=f"该 API Key 无权使用模型 {model_name}")
    return {
        "endpoint": _editor_gateway_endpoint(request),
        "api_key": str(key.get("key") or ""),
        "model": model_name,
        "protocol": "responses" if editor.get("provider") == "codex" else "chat",
    }


@admin_router.get("")
async def admin_list_editors(
    project_id: str | None = None,
    provider: str | None = None,
    status: str | None = None,
    node_id: str | None = None,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: User = Depends(_require_admin),
) -> dict:
    rows = await PostgresClient.list_all_editors_for_admin(
        project_id=project_id, provider=provider, status=status, node_id=node_id, limit=limit, offset=offset
    )
    return rows


@admin_router.get("/{editor_id}")
async def admin_get_editor(editor_id: str, _: User = Depends(_require_admin)) -> dict:
    editor = await PostgresClient.get_editor_for_admin(editor_id)
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    editor["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(editor_id))
    return _redact_editor(editor)


@admin_router.post("/{editor_id}/sessions/{session_id}/close")
async def admin_close_editor_session(
    editor_id: str, session_id: str, request: Request, user: User = Depends(_require_admin)
) -> dict:
    session = await PostgresClient.get_editor_session(editor_id, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    result = await PostgresClient.set_editor_session_status(editor_id, session_id, "closed")
    node_session_id = (session.get("node_session_id") or "").strip()
    if node_session_id:
        try:
            await get_local_node_client().delete_node_session(node_session_id)
        except Exception:
            pass
    await audit_user_action(
        request,
        user,
        "admin.editor.session.close",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"closed": True},
    )
    return {"closed": bool(result), "session_id": session_id}


@admin_router.get("/{editor_id}/logs")
async def admin_editor_logs(
    editor_id: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: User = Depends(_require_admin),
) -> dict:
    return await PostgresClient.query_editor_request_logs_admin(editor_id, limit=limit, offset=offset)

@router.get("")
async def list_editors(user: User = Depends(get_current_user)) -> list[dict]:
    rows = await PostgresClient.list_editors_for_user(str(user.id))
    return [_redact_editor(row) for row in rows]


@router.post("")
async def create_editor(
    body: CreateEditorReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        payload = _validated_editor_create_payload(body)
        # 编辑器行落绑定（service_id/resource_id），不落 wire spec / token。
        if payload.get("mcp_config"):
            team_id = await resolve_team_id(str(user.id))
            from .resource_reference_service import normalize_mcp_bindings
            payload["mcp_config"] = await normalize_mcp_bindings(
                str(user.id), team_id, payload["mcp_config"],
            )
        result = await PostgresClient.create_editor_only(
            {"owner_user_id": str(user.id), **payload}
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await audit_user_action(
        request,
        user,
        "editor.create",
        request_body={k: v for k, v in body.model_dump(exclude_none=True).items() if k != "parent_api_key_id"},
        response={"id": result["id"], "provider": result["provider"]},
    )
    return _redact_editor(result)


@router.get("/sessions")
async def list_user_editor_sessions(
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
) -> dict:
    # 注意：必须声明在 `/{editor_id}` 之前，否则 `/sessions` 会被当作 editor_id 命中 404。
    result = await PostgresClient.list_editor_sessions_for_user(str(user.id), limit=limit, offset=offset)
    return {"total": result["total"], "rows": _redact_sessions(result["rows"]), "limit": limit, "offset": offset}


@router.get("/{editor_id}")
async def get_editor(editor_id: str, user: User = Depends(get_current_user)) -> dict:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    editor["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(editor_id))
    return _redact_editor(editor)


@router.patch("/{editor_id}")
async def update_editor(
    editor_id: str,
    body: UpdateEditorReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    existing = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    if not existing:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    patch = body.model_dump(exclude_none=True)
    if existing.get("provider") and "provider" in patch:
        # provider 属于编辑器身份，切换 provider 必须新建编辑器，不能就地改。
        raise HTTPException(status_code=400, detail="provider 不可修改，请新建编辑器")
    # 通过服务端 resolver 校验并解析新引用；持久化前先拒绝未授权的 resource_id。
    team_id = await resolve_team_id(str(user.id))
    try:
        from .resource_reference_service import (
            normalize_mcp_bindings,
            resolve_prompt_for_user,
            resolve_reference_specs,
        )

        base_url = str(request.base_url).rstrip("/")
        if "skill_config" in patch:
            patch["skill_config"] = await resolve_reference_specs(
                str(user.id), team_id, "skill", patch["skill_config"], request_base_url=base_url,
            )
        if "plugin_config" in patch:
            patch["plugin_config"] = await resolve_reference_specs(
                str(user.id), team_id, "plugin", patch["plugin_config"], request_base_url=base_url,
            )
        if "mcp_config" in patch:
            # 编辑器行落绑定（service_id/resource_id），不落 wire spec / token。
            patch["mcp_config"] = await normalize_mcp_bindings(
                str(user.id), team_id, patch["mcp_config"],
            )
        if "prompt_id" in patch and (patch.get("prompt_id") or "").strip():
            prompt = await resolve_prompt_for_user(
                str(user.id), team_id, str(patch["prompt_id"]), existing.get("provider") or "",
            )
            if prompt is None:
                raise ValueError("prompt_not_granted")
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=f"资源未授权或配置无效: {exc}") from exc
    updated = await PostgresClient.update_editor_for_user(editor_id, str(user.id), patch)
    if not updated:
        raise HTTPException(status_code=409, detail="编辑器当前不可修改")

    # 配置改动必须重新下发给活着的 session runtime，否则运行态仍用旧配置。
    resync: dict[str, list[str]] = {"applied": [], "failed": []}

    # 项目提示词：写入编辑器工作目录的 CLAUDE.md / AGENTS.md（先持久化 prompt_id 再写文件）。
    if "prompt_id" in patch:
        prompt_id = (patch.get("prompt_id") or "").strip()
        if prompt_id:
            try:
                from . import project_prompt_store
                from .routes_editors_workspace import remove_editor_prompt_file, write_editor_prompt_file

                prompt = await project_prompt_store.get_prompt(prompt_id)
                if not prompt:
                    resync["failed"].append(f"prompt {prompt_id}: 提示词不存在")
                else:
                    filename = await write_editor_prompt_file(updated, prompt.get("content") or "")
                    resync["applied"].append(f"prompt:{filename}")
            except Exception as exc:  # noqa: BLE001 - 写文件失败要如实上报，不遮掩
                resync["failed"].append(f"prompt {prompt_id}: {exc}")
        else:
            try:
                from .routes_editors_workspace import remove_editor_prompt_file
                resync["applied"].append(f"prompt:{await remove_editor_prompt_file(updated)}")
            except Exception as exc:
                resync["failed"].append(f"prompt: {exc}")

    config_changed = any(k in patch for k in ("mcp_config", "skill_config", "plugin_config"))
    if config_changed:
        client = get_local_node_client()
        sessions = await PostgresClient.list_editor_sessions(editor_id)
        for sess in sessions:
            if sess.get("status") not in ("active", "pending_first_request"):
                continue
            node_session_id = sess.get("node_session_id")
            if not node_session_id:
                continue
            try:
                if "mcp_config" in patch:
                    # 绑定列表 → 安全 spec（现签 user token）+ overlay merge。
                    await client.apply_node_session_mcps(
                        node_session_id,
                        await _resolve_editor_mcps(updated, request, sess.get("mcp_overlay_json") or []),
                    )
                if "skill_config" in patch:
                    await client.apply_node_session_skills(node_session_id, updated.get("skill_config") or [])
                if "plugin_config" in patch:
                    await client.apply_node_session_plugins(node_session_id, updated.get("plugin_config") or [])
                # No restart: these are pure disk writes. The next human_message
                # carries the session's current config snapshot, so the runtime
                # re-prepares the provider with the new MCP/skill/plugin set on
                # the next turn.
                resync["applied"].append(sess["id"])
            except Exception as exc:  # noqa: BLE001 - 下发失败要如实上报，不遮掩
                resync["failed"].append(f"{sess['id']}: {exc}")

    audit_patch = {
        key: value
        for key, value in patch.items()
        if key not in ("mcp_config", "skill_config", "plugin_config")
    }
    for key in ("mcp_config", "skill_config", "plugin_config"):
        if key in patch:
            audit_patch[f"{key}_count"] = len(patch[key] or [])
    await audit_user_action(
        request,
        user,
        "editor.update",
        request_body=audit_patch,
        response={"id": editor_id, "resync": resync},
    )
    updated["sessions"] = _redact_sessions(await PostgresClient.list_editor_sessions(editor_id))
    result = _redact_editor(updated)
    result["config_resync"] = resync
    return result


@router.post("/{editor_id}/sessions")
async def create_editor_session(
    editor_id: str,
    body: CreateSessionReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    return await _create_editor_session_core(editor_id, body, request, user, editor)


async def _create_editor_session_core(
    editor_id: str,
    body: CreateSessionReq,
    request: Request,
    user: User,
    editor: dict,
) -> dict:
    issue = None
    target_status: str | None = None
    if body.issue_id:
        from .models_project import ProjectIssue
        from .project_service import ISSUE_TRANSITIONS, _STATUS_LABELS

        try:
            issue_uuid = uuid.UUID(str(body.issue_id))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="需求/bug ID 无效") from exc
        issue = await ProjectIssue.get_or_none(id=issue_uuid, project_id=editor.get("project_id"))
        if issue is None:
            raise HTTPException(status_code=404, detail="需求/bug 不存在或不属于当前项目")
        allowed_roles = {"requirement": {"design", "develop"}, "bug": {"diagnose", "fix"}}
        if body.task_role not in allowed_roles.get(issue.issue_type, set()):
            raise HTTPException(status_code=422, detail="任务类型与需求/bug 类型不匹配")
        target_status = {
            "design": "designing",
            "develop": "developing",
            "diagnose": "diagnosing",
            "fix": "fixing",
        }[body.task_role]
        if issue.status != "unassigned" or target_status not in ISSUE_TRANSITIONS[issue.issue_type][issue.status]:
            current_label = _STATUS_LABELS.get(issue.status, issue.status)
            raise HTTPException(status_code=409, detail=f"当前状态「{current_label}」不可分配")
        duplicate = await PostgresClient.get_active_editor_session_for_issue(
            str(issue.id), body.task_role
        )
        if duplicate:
            raise HTTPException(status_code=409, detail="该需求/bug 已有同类型任务正在运行")
    if editor["provider"] == "codex" and (not body.expected_client_id or not (body.first_content or body.first_content_hash)):
        raise HTTPException(status_code=422, detail="Codex 会话必须预注册 installation_id 和首条消息内容")
    if not body.parent_api_key_id:
        raise HTTPException(status_code=422, detail="创建会话必须选择父 API Key")
    if body.parent_api_key_id not in await resolve_allowed_parent_key_ids(str(user.id)):
        raise HTTPException(status_code=403, detail="无权使用该父 API Key")
    # 显式节点必须校验调用者权限，防止横向占用他人节点。
    node_id = (editor.get("node_id") or "").strip()
    if node_id:
        from .nodes_service import nodes_service

        if not await nodes_service.user_can_use_node(str(user.id), node_id):
            raise HTTPException(status_code=403, detail="无权使用该运行节点")
    models = [m for m in (body.models or []) if str(m).strip()]
    active_model = body.model or (models[0] if models else None)
    # 写入边界校验：候选模型集合与激活模型都必须落在父 Key 的可执行范围内
    # （非内部 id、当前可供给、过白/黑名单）。未指定模型时不校验——会话沿用
    # 编辑器默认模型，交由 _editor_llm_config 在下发前把关。
    if models or active_model:
        from rate_limiter import ModelClientPool
        from model_catalog import is_internal_model_id
        import config as gateway_config

        key_row = await PostgresClient.get_api_key_by_id(int(body.parent_api_key_id))
        if not key_row or key_row.get("disabled") or not str(key_row.get("key") or "").strip():
            raise HTTPException(status_code=400, detail="父 API Key 已失效")
        key_value = str(key_row["key"])
        available_model_ids = ModelClientPool.available_model_ids()

        async def _executable(name: str) -> bool:
            return (
                not is_internal_model_id(name)
                and name in available_model_ids
                and await gateway_config.Config.api_key_allows_model(key_value, name)
            )

        legal: list[str] = []
        seen: set[str] = set()
        for raw in models:
            name = str(raw).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            if await _executable(name):
                legal.append(name)
        if models and not legal:
            raise HTTPException(
                status_code=400, detail="父 API Key 无权使用请求的全部模型，请选择可用模型或换 Key"
            )
        if legal:
            # 保留仍合法的选中值，否则回退首项；models 只落合法模型。
            models = legal
            active_model = active_model if active_model in legal else legal[0]
        elif active_model and not await _executable(str(active_model)):
            raise HTTPException(
                status_code=400, detail=f"父 API Key 无权使用模型 {active_model}"
            )
    task_name = (body.task_name or "").strip()
    if not task_name:
        task_name = " ".join((body.first_content or "").split())[:40] or "未命名任务"
    try:
        session = await PostgresClient.create_editor_session(
            editor_id,
            active_model,
            body.expected_client_id,
            body.first_content_hash or (
                hashlib.sha256(body.first_content.encode("utf-8")).hexdigest()
                if body.first_content else None
            ),
            parent_api_key_id=body.parent_api_key_id,
            usage_limit=body.usage_limit,
            expires_at=body.expires_at,
            models=models,
            task_name=task_name,
            task_type=body.task_type,
            task_role=body.task_role,
            sub_type=body.sub_type,
            issue_id=body.issue_id,
            mode=(body.mode or "").strip() or None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not session:
        raise HTTPException(status_code=409, detail="编辑器当前不可创建会话")
    mcp_overlay: list[dict] = []
    if issue is not None:
        try:
            import builtin_tool_store

            _token_row, issue_workflow_token = await builtin_tool_store.issue_token(
                "agent", session["id"], display_token=False
            )
            mcp_overlay = [_issue_workflow_mcp_entry(issue_workflow_token)]
            await PostgresClient.set_editor_session_mcp_overlay(editor_id, session["id"], mcp_overlay)
        except Exception as exc:
            await PostgresClient.set_editor_session_status(editor_id, session["id"], "error")
            raise HTTPException(status_code=503, detail=f"需求/bug MCP 权限签发失败: {exc}") from exc
    try:
        llm_config = await _editor_llm_config(editor, request, active_model, session)
        mcps = await _resolve_editor_mcps(editor, request, mcp_overlay)
        dispatch = await get_local_node_client().dispatch_session(
            editor.get("node_id"),
            {
                "sessionId": session["id"],
                "editorId": editor_id,
                "editorSessionId": session["id"],
                "projectId": editor.get("project_id") or "",
                "provider": editor["provider"],
                "model": active_model or "",
                "mode": (body.mode or "").strip(),
                "llm": llm_config,
                "interactive": True,
                "deferStart": True,
                "tags": {"editor_workdir": editor.get("workdir") or f"editors/{editor_id}"},
                "git": _editor_git_payload(editor),
                "mcps": mcps,
                "skills": editor.get("skill_config") or [],
                "plugins": editor.get("plugin_config") or [],
            },
        )
        if dispatch.get("accepted") is False:
            raise RuntimeError(dispatch.get("error") or "node rejected editor session")
        node_session_id = str(dispatch.get("sessionId") or dispatch.get("session_id") or session["id"])
        bound = await PostgresClient.bind_editor_session_node(
            editor_id,
            session["id"],
            node_session_id,
            body.expected_client_id if editor["provider"] == "codex" else None,
        )
        if bound:
            # 保留 create 返回的一次性 api_key_copy，绑定行不含该明文。
            key_copy = session.get("api_key_copy")
            session = bound
            if key_copy:
                session["api_key_copy"] = key_copy
        await get_local_node_client().start_node_session_runtime(node_session_id)
        if issue is not None and target_status:
            issue.status = target_status
            await issue.save(update_fields=["status", "updated_at"])
    except Exception as exc:
        await PostgresClient.set_editor_session_status(editor_id, session["id"], "error")
        raise HTTPException(status_code=503, detail=f"节点会话创建失败: {exc}") from exc
    await audit_user_action(
        request,
        user,
        "editor.session.create",
        request_body={
            "editor_id": editor_id,
            "model": active_model,
            "models": models,
            "task_name": task_name,
            "task_type": body.task_type,
            "task_role": body.task_role,
            "sub_type": body.sub_type,
            "issue_id": body.issue_id,
            "parent_api_key_id": body.parent_api_key_id,
            "expected_client_id_present": bool(body.expected_client_id),
            "first_content_present": bool(body.first_content or body.first_content_hash),
        },
        response={"session_id": session["id"], "provider": editor["provider"]},
    )
    return {
        **_redact_session(session),
        "provider": editor["provider"],
    }


@router.get("/{editor_id}/sessions")
async def list_editor_sessions(editor_id: str, user: User = Depends(get_current_user)) -> list[dict]:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    if not editor:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    rows = await PostgresClient.list_editor_sessions(editor_id)
    for row in rows:
        row["provider"] = editor["provider"]
    return _redact_sessions(rows)


@router.post("/{editor_id}/sessions/{session_id}/api-key/disable")
async def disable_editor_session_api_key(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    result = await PostgresClient.disable_editor_session_api_key(
        editor_id, session_id, str(user.id)
    )
    if not result:
        raise HTTPException(status_code=404, detail="会话不存在或 API Key 不可用")
    from config import Config

    await Config.refresh_api_keys_cache()
    await audit_user_action(
        request,
        user,
        "editor.session.api_key.disable",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"key_id": result["id"], "disabled": True},
    )
    return {"disabled": True, "key_id": result["id"], "version": result.get("version")}


@router.post("/{editor_id}/sessions/{session_id}/api-key/rotate")
async def rotate_editor_session_api_key(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    result = await PostgresClient.rotate_editor_session_api_key(
        editor_id, session_id, str(user.id)
    )
    if not result:
        raise HTTPException(status_code=404, detail="会话不存在或 API Key 不可用")
    from config import Config

    await Config.refresh_api_keys_cache()
    await audit_user_action(
        request,
        user,
        "editor.session.api_key.rotate",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"key_id": result["id"], "version": result.get("version")},
    )
    key_value = result.get("key", "")
    return {
        "key_id": result["id"],
        "key": key_value,
        "version": result.get("version"),
        "key_masked": key_value[:7] + "..." + key_value[-4:] if len(key_value) > 12 else key_value,
    }


@router.post("/{editor_id}/api-key/disable")
async def disable_editor_api_key(
    editor_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    result = await PostgresClient.disable_editor_api_key(editor_id, str(user.id))
    if not result:
        raise HTTPException(status_code=404, detail="编辑器不存在或 API Key 不可用")
    from config import Config
    await Config.refresh_api_keys_cache()
    await audit_user_action(
        request,
        user,
        "editor.api_key.disable",
        request_body={"editor_id": editor_id},
        response={"key_id": result["id"], "disabled": True},
    )
    return {"disabled": True, "key_id": result["id"], "version": result.get("version")}


@router.post("/{editor_id}/api-key/rotate")
async def rotate_editor_api_key(
    editor_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    result = await PostgresClient.rotate_editor_api_key(editor_id, str(user.id))
    if not result:
        raise HTTPException(status_code=404, detail="编辑器不存在或 API Key 不可用")
    from config import Config
    await Config.refresh_api_keys_cache()
    await audit_user_action(
        request,
        user,
        "editor.api_key.rotate",
        request_body={"editor_id": editor_id},
        response={"key_id": result["id"], "version": result.get("version")},
    )
    return {
        "key_id": result["id"],
        "key": result.get("key"),
        "version": result.get("version"),
        "key_masked": result.get("key", "")[:7] + "..." + result.get("key", "")[-4:],
    }


@router.get("/{editor_id}/logs")
async def list_editor_request_logs(
    editor_id: str,
    editor_session_id: str | None = None,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
) -> dict:
    if not await PostgresClient.editor_belongs_to_user(editor_id, str(user.id)):
        raise HTTPException(status_code=404, detail="编辑器不存在")
    return await PostgresClient.query_editor_request_logs(
        editor_id,
        str(user.id),
        editor_session_id=editor_session_id,
        limit=limit,
        offset=offset,
    )


@router.get("/{editor_id}/logs/{log_id}")
async def get_editor_request_log(
    editor_id: str,
    log_id: int,
    user: User = Depends(get_current_user),
) -> dict:
    log = await PostgresClient.get_editor_request_log_detail(log_id, editor_id, str(user.id))
    if not log:
        raise HTTPException(status_code=404, detail="日志不存在")
    return log


async def _live_editor_session_runtime(editor_id: str, session_id: str, user: User) -> str:
    """Resolve the node runtime handle for a session the user owns.

    Raises 404 when the session is missing and 409 when it has no live runtime
    bound (stopped/closed sessions have no node session to drive).
    """
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    if session.get("status") not in ("active", "pending_first_request"):
        raise HTTPException(status_code=409, detail="会话当前不可操作")
    node_session_id = (session.get("node_session_id") or "").strip()
    if not node_session_id:
        raise HTTPException(status_code=409, detail="会话尚未绑定运行时")
    return node_session_id


@router.post("/{editor_id}/sessions/{session_id}/messages")
async def send_editor_session_message(
    editor_id: str,
    session_id: str,
    body: SendSessionMessageReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    content = (body.content or "").strip()
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    if not content:
        raise HTTPException(status_code=422, detail="消息不能为空")
    if session.get("status") not in ("active", "pending_first_request"):
        raise HTTPException(status_code=409, detail="会话当前不可发送消息")
    node_session_id = (session.get("node_session_id") or "").strip()
    if not node_session_id:
        raise HTTPException(status_code=409, detail="会话尚未绑定运行时")
    result = await get_local_node_client().send_session_input(
        node_session_id,
        "human_message",
        content,
        model=session.get("model") or "",
        mode=session.get("mode") or "",
    )
    await audit_user_action(
        request,
        user,
        "editor.session.message",
        request_body={"editor_id": editor_id, "session_id": session_id, "content_length": len(content)},
        response={"accepted": result.get("accepted", True)},
    )
    return {"accepted": result.get("accepted", True), "session_id": session_id}


@router.post("/{editor_id}/sessions/{session_id}/cancel")
async def cancel_editor_session_turn(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """Interrupt the turn currently running in the session's runtime.

    Maps onto the same NodeSessionInput channel the message endpoint uses, with
    kind ``cancel`` — the node writes a ``cancel`` frame to the runtime's stdin.
    The session stays alive and accepts the next message.
    """
    node_session_id = await _live_editor_session_runtime(editor_id, session_id, user)
    result = await get_local_node_client().send_session_input(node_session_id, "cancel")
    await audit_user_action(
        request,
        user,
        "editor.session.cancel",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"accepted": result.get("accepted", True)},
    )
    return {"accepted": result.get("accepted", True), "session_id": session_id}


@router.post("/{editor_id}/sessions/{session_id}/restart")
async def restart_editor_session_runtime(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """Restart the session's runtime process (clears in-process context).

    The session row, its API key and its request history are untouched; only the
    editor runtime on the node is recycled.
    """
    node_session_id = await _live_editor_session_runtime(editor_id, session_id, user)
    result = await get_local_node_client().restart_node_session_runtime(node_session_id)
    await audit_user_action(
        request,
        user,
        "editor.session.restart",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"restarted": True},
    )
    return {"restarted": True, "session_id": session_id, "detail": result}


@router.get("/{editor_id}/sessions/{session_id}/events")
async def stream_editor_session_events(
    editor_id: str,
    session_id: str,
    user: User = Depends(get_current_user),
):
    """Server-Sent Events stream of a session's live runtime events.

    Bridges the control plane's ``FollowNodeSession`` Connect server-stream
    (protobuf) into browser-friendly JSON SSE. The browser never sees protobuf:
    each ``NodeSessionEvent`` becomes one SSE ``event:``/``data:`` pair
    (``output`` / ``agent_event`` / ``result``). A ``result`` event ends the
    stream.
    """
    import json as _json

    from fastapi.responses import StreamingResponse

    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    node_session_id = (session.get("node_session_id") or "").strip()
    if not node_session_id:
        raise HTTPException(status_code=409, detail="会话尚未绑定运行时")

    client = get_local_node_client()

    async def event_generator():
        # A leading comment line opens the stream promptly so the browser's
        # EventSource flips to "connected" without waiting for the first event.
        yield ": ok\n\n"
        try:
            async for event in client.follow_session_events(node_session_id):
                kind = event.get("kind")
                sse_event = "agent_event" if kind == "structured" else kind or "message"
                yield f"event: {sse_event}\ndata: {_json.dumps(event, ensure_ascii=False)}\n\n"
                if kind == "result":
                    break
        except Exception as exc:  # noqa: BLE001 - surface the failure to the client, don't 500 mid-stream
            yield f"event: error\ndata: {_json.dumps({'message': str(exc)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers=_stream_headers(),
    )


@router.patch("/{editor_id}/sessions/{session_id}")
async def switch_editor_session_model(
    editor_id: str,
    session_id: str,
    body: UpdateSessionReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """Switch a session's model / mode without recreating the editor.

    Both knobs are pure disk writes on the node: the change takes effect on the
    next turn because the node stamps the session's current model/mode/llm onto
    the next human_message frame and the runtime re-prepares the provider from
    that snapshot. No restart, no live-switch protocol.

    The DB is written last so a runtime failure does not leave a phantom value
    the runtime never actually loaded.
    """
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")

    model = (body.model or "").strip()
    mode = (body.mode or "").strip() if body.mode is not None else ""
    mode_requested = body.mode is not None

    # Metadata-only updates are allowed for stopped sessions. Model/mode changes
    # use the runtime-aware paths below and require a live session.
    if not model and not mode_requested:
        if not any(value is not None for value in (body.task_name, body.models, body.usage_limit, body.expires_at)):
            raise HTTPException(status_code=422, detail="至少提供一个可修改字段")
        updated = await PostgresClient.update_editor_session_metadata(
            editor_id,
            session_id,
            task_name=body.task_name,
            models=body.models,
            usage_limit=body.usage_limit,
            expires_at=body.expires_at,
        )
        return {**_redact_session(updated), "provider": editor["provider"]}

    if session.get("status") not in ("active", "pending_first_request"):
        raise HTTPException(status_code=409, detail="会话当前不可切换模型或模式")
    node_session_id = (session.get("node_session_id") or "").strip()
    if not node_session_id:
        raise HTTPException(status_code=409, detail="会话尚未绑定运行时")
    client = get_local_node_client()
    updated = None

    if mode_requested:
        # Push the mode to the node so its session mirror updates; the node then
        # stamps it onto the next human_message snapshot (pure disk write on the
        # node, effective next turn — no restart).
        try:
            await client.configure_node_session_mode(node_session_id, mode)
        except Exception as exc:  # noqa: BLE001 - 下发失败要如实上报，不写库
            raise HTTPException(status_code=503, detail=f"模式切换下发失败: {exc}") from exc
        updated = await PostgresClient.set_editor_session_mode(editor_id, session_id, mode)
        if not updated:
            raise HTTPException(status_code=409, detail="会话当前不可切换模式")
        await audit_user_action(
            request,
            user,
            "editor.session.switch_mode",
            request_body={"editor_id": editor_id, "session_id": session_id, "mode": mode},
            response={"session_id": session_id, "mode": mode},
        )

    if model:
        llm_config = await _editor_llm_config(editor, request, model, session)
        try:
            await client.configure_node_session_llm(node_session_id, llm_config)
        except Exception as exc:  # noqa: BLE001 - 下发失败要如实上报，不写库
            raise HTTPException(status_code=503, detail=f"模型切换下发失败: {exc}") from exc
        updated = await PostgresClient.set_editor_session_model(editor_id, session_id, model)
        if not updated:
            raise HTTPException(status_code=409, detail="会话当前不可切换模型")
        await audit_user_action(
            request,
            user,
            "editor.session.switch_model",
            request_body={"editor_id": editor_id, "session_id": session_id, "model": model},
            response={"session_id": session_id, "model": model},
        )

    return {
        **_redact_session(updated),
        "provider": editor["provider"],
    }


async def _stop_editor_session_runtime(editor_id: str, session: dict) -> None:
    node_session_id = (session.get("node_session_id") or "").strip()
    async with PostgresClient.pool.acquire() as conn:
        await conn.execute(
            "UPDATE editor_sessions SET status='closed', closed_at=COALESCE(closed_at, now()), updated_at=now() WHERE editor_id=$1 AND id=$2",
            editor_id,
            session["id"],
        )
    if node_session_id:
        try:
            await get_local_node_client().delete_node_session(node_session_id)
        except Exception:
            pass


@router.post("/{editor_id}/sessions/{session_id}/stop")
async def stop_editor_session(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    await _stop_editor_session_runtime(editor_id, session)
    await audit_user_action(
        request,
        user,
        "editor.session.stop",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"stopped": True},
    )
    return {"stopped": True, "session_id": session_id}


@router.delete("/{editor_id}/sessions/{session_id}/data")
async def delete_editor_session_data(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    if session.get("status") != "closed":
        await _stop_editor_session_runtime(editor_id, session)
    if not await PostgresClient.delete_editor_session(editor_id, session_id):
        raise HTTPException(status_code=404, detail="会话不存在")
    await audit_user_action(
        request,
        user,
        "editor.session.delete",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"deleted": True, "request_logs_retained": True},
    )
    return {"deleted": True, "session_id": session_id, "request_logs_retained": True}


@router.delete("/{editor_id}/sessions/{session_id}")
async def close_editor_session(
    editor_id: str,
    session_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    editor = await PostgresClient.get_editor_for_user(editor_id, str(user.id))
    session = await PostgresClient.get_editor_session(editor_id, session_id) if editor else None
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    # Legacy DELETE keeps its historical stop-and-retain semantics.
    await _stop_editor_session_runtime(editor_id, session)
    await audit_user_action(
        request,
        user,
        "editor.session.close",
        request_body={"editor_id": editor_id, "session_id": session_id},
        response={"closed": True},
    )
    return {"closed": True, "session_id": session_id}


@router.delete("/{editor_id}")
async def delete_editor(
    editor_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    sessions = await PostgresClient.list_editor_sessions(editor_id)
    ok = await PostgresClient.close_editor(editor_id, str(user.id))
    if not ok:
        raise HTTPException(status_code=404, detail="编辑器不存在")
    node_client = get_local_node_client()
    for session in sessions:
        node_session_id = (session.get("node_session_id") or "").strip()
        if not node_session_id:
            continue
        try:
            await node_client.delete_node_session(node_session_id)
        except Exception:
            # Editor history is already closed in the ledger. Runtime cleanup is
            # retried by the node/session reaper when a node is available again.
            pass
    await audit_user_action(
        request,
        user,
        "editor.delete",
        request_body={"editor_id": editor_id},
        response={"deleted": True},
    )
    return {"deleted": True, "editor_id": editor_id}
