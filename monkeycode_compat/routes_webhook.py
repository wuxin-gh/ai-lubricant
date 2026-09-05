"""Project webhook management routes (``/api/v1/users/projects/{id}/webhook``).

Lets a project owner (or read_write collaborator) register/list/update/delete a
repository webhook on the project's Git platform using the linked Git identity.
The platform-side ``hook_id`` and verification secret are persisted; the secret
is never returned in plaintext. Review execution is a separate stage.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from .deps import audit_user_action, get_current_user
from .models import User
from .webhook_service import webhook_service

router = APIRouter(prefix="/api/v1/users/projects", tags=["monkeycode-project-webhooks"])


class EnableWebhookReq(BaseModel):
    events: list[str] | None = None
    active: bool | None = None
    review_framework: str = "open_code_review_delegate"
    review_provider: str | None = None
    review_node_ids: list[str] = Field(default_factory=list)
    review_skill_config: list[dict] | None = None
    review_mcp_config: list[dict] | None = None
    review_plugin_config: list[dict] | None = None
    review_editor_ids: list[str] = Field(default_factory=list)
    review_auto: bool = False
    review_enabled: bool = True
    review_model_id: str | None = None
    review_prompt_id: str | None = None
    review_api_key_id: int | None = None
    review_model_limits: dict | None = None
    review_rate_limit: dict | None = None
    review_expires_at: float | None = None
    review_latest_only: bool = False


class UpdateWebhookReq(BaseModel):
    events: list[str] | None = None
    active: bool | None = None
    regenerate_secret: bool = False
    review_framework: str | None = None
    review_provider: str | None = None
    review_node_ids: list[str] | None = None
    review_skill_config: list[dict] | None = None
    review_mcp_config: list[dict] | None = None
    review_plugin_config: list[dict] | None = None
    review_editor_ids: list[str] | None = None
    review_auto: bool | None = None
    review_enabled: bool | None = None
    review_model_id: str | None = None
    review_prompt_id: str | None = None
    review_api_key_id: int | None = None
    review_model_limits: dict | None = None
    review_rate_limit: dict | None = None
    review_expires_at: float | None = None
    review_latest_only: bool | None = None


_ERROR_DETAIL = {
    "project_not_found": "项目不存在或无权访问",
    "forbidden": "无权管理该项目 Webhook",
    "repo_unavailable": "项目未绑定可用的 Git 身份或仓库",
    "webhook_not_configured": "项目尚未配置 Webhook",
    "unsupported_review_framework": "暂不支持该 Review 框架",
    "unsupported_review_editor": "该 Review 框架不支持所选编辑器类型",
    "review_provider_required": "启用 Review 前必须选择执行客户端",
    "review_provider_unsupported": "该 Review 框架不支持所选执行客户端",
    "review_provider_bootstrap_unsupported": "Codex 自动 Review 暂不可用（缺少可预注册的 installation_id）",
    "review_nodes_required": "启用 Review 前必须选择执行节点或开启自动模式",
    "review_api_key_required": "启用 Review 前必须选择父 API Key",
    "review_api_key_unavailable": "父 API Key 不可用或已停用",
    "review_session_config_invalid": "Review 的 Skill/MCP/Plugin 配置格式不正确",
    "review_editor_unavailable": "请选择该项目下可用的编辑器",
    "review_editor_unsupported": "该 Review 框架不支持所选编辑器类型",
    "review_editor_required": "启用 Review 前必须选择编辑器或开启自动模式",
    "review_rate_limit_invalid": "Review 的速率限制配置格式不正确",
    "review_expires_at_invalid": "Review 子 Key 有效期格式不正确",
    "review_prompt_unavailable": "所选提示词不存在或无权使用",
}


def _raise(exc: ValueError) -> None:
    reason = str(exc)
    detail = _ERROR_DETAIL.get(reason)
    if detail is None and reason.startswith("review_nodes_unavailable:"):
        import json

        try:
            nodes = json.loads(reason.split(":", 1)[1])
        except Exception:
            nodes = []
        raise HTTPException(
            status_code=422,
            detail={"code": "review_nodes_unavailable", "nodes": nodes},
        ) from exc
    if detail is None and reason.startswith("review_editors_unavailable:"):
        import json

        try:
            editors = json.loads(reason.split(":", 1)[1])
        except Exception:
            editors = []
        raise HTTPException(
            status_code=422,
            detail={"code": "review_editors_unavailable", "editors": editors},
        ) from exc
    if detail is None and reason.startswith("webhook_upstream_failed"):
        raise HTTPException(status_code=502, detail="Git 平台调用失败，请检查身份权限或稍后重试") from exc
    if detail is None:
        raise HTTPException(status_code=400, detail=reason) from exc
    status = 404 if reason in ("project_not_found", "webhook_not_configured") else 403
    raise HTTPException(status_code=status, detail=detail) from exc


@router.get("/{project_id}/webhook")
async def get_project_webhook(
    project_id: str, user: User = Depends(get_current_user)
) -> dict | None:
    row = await webhook_service.get_webhook(str(user.id), project_id, role=user.role)
    if row is None:
        return None
    return row


@router.get("/{project_id}/webhook/review-nodes")
async def list_project_review_nodes(
    project_id: str,
    framework: str = "open_code_review_delegate",
    editor_provider: str = "",
    user: User = Depends(get_current_user),
) -> dict:
    project, _access = await webhook_service._resolve(
        str(user.id), project_id, user.role
    )
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    from .review_node_service import review_node_service

    try:
        return await review_node_service.list_review_nodes(
            str(user.id), framework, editor_provider
        )
    except ValueError as exc:
        _raise(exc)


@router.get("/{project_id}/webhook/frameworks")
async def list_review_frameworks(
    project_id: str, user: User = Depends(get_current_user)
) -> dict:
    """Review frameworks the console may offer, with their editor support."""
    project, _access = await webhook_service._resolve(
        str(user.id), project_id, user.role
    )
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    from .review_executors import REVIEW_FRAMEWORKS

    return {
        "frameworks": [
            {
                "name": framework.name,
                "label": framework.label,
                "description": framework.description,
                "base_tools": list(framework.base_tools),
                "supported_editors": list(framework.supported_editors),
            }
            for framework in REVIEW_FRAMEWORKS.values()
        ]
    }


@router.get("/{project_id}/webhook/status")
async def get_project_webhook_status(
    project_id: str,
    limit: int = 20,
    user: User = Depends(get_current_user),
) -> dict:
    """Webhook + review runtime state for the management panel."""
    project, _access = await webhook_service._resolve(str(user.id), project_id, user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    row = await webhook_service.get_webhook(str(user.id), project_id, role=user.role)
    from . import config

    max_concurrency = int(config.settings.review_project_max_concurrency)
    if row is None:
        return {"webhook": None, "inflight": 0, "max_concurrency": max_concurrency, "events": []}
    from .models_review import ReviewNodeLease
    from .models_webhook_event import ProjectWebhookEvent

    capped = max(1, min(int(limit or 20), 100))
    events = await ProjectWebhookEvent.filter(
        webhook_id=row["id"]
    ).order_by("-created_at").limit(capped)
    active_leases = await ReviewNodeLease.filter(
        project_id=row["project_id"], status="active"
    ).count()
    return {
        "webhook": row,
        "inflight": active_leases,
        "max_concurrency": max_concurrency,
        "events": [
            {
                "id": str(event.id),
                "event_type": event.event_type,
                "delivery_id": event.delivery_id,
                "status": event.status,
                "pr_number": event.pr_number,
                "commit_sha": event.commit_sha,
                "source_branch": event.source_branch,
                "target_branch": event.target_branch,
                "task_id": str(event.task_id) if event.task_id else None,
                "attempts": event.attempts,
                "last_error": event.last_error,
                "finding_count": len(event.findings or []),
                "review_summary": event.review_summary,
                "created_at": int(event.created_at.timestamp()) if event.created_at else None,
                "review_completed_at": (
                    int(event.review_completed_at.timestamp())
                    if event.review_completed_at else None
                ),
            }
            for event in events
        ],
    }


@router.post("/{project_id}/webhook")
async def enable_project_webhook(
    project_id: str,
    body: EnableWebhookReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await webhook_service.enable_webhook(
            str(user.id), project_id,
            role=user.role,
            events=body.events,
            review_framework=body.review_framework,
            review_provider=body.review_provider,
            review_node_ids=body.review_node_ids,
            review_skill_config=body.review_skill_config,
            review_mcp_config=body.review_mcp_config,
            review_plugin_config=body.review_plugin_config,
            review_editor_ids=body.review_editor_ids,
            review_auto=body.review_auto,
            review_enabled=body.review_enabled,
            review_model_id=body.review_model_id,
            review_prompt_id=body.review_prompt_id,
            review_api_key_id=body.review_api_key_id,
            review_model_limits=body.review_model_limits,
            review_rate_limit=body.review_rate_limit,
            review_expires_at=body.review_expires_at,
            review_latest_only=body.review_latest_only,
            request=request,
        )
    except ValueError as exc:
        _raise(exc)
    await audit_user_action(
        request, user, "project_webhook.enable",
        request_body=body.model_dump(exclude_none=True),
        response={"project_id": project_id, "events": result.get("events")},
    )
    return result


@router.patch("/{project_id}/webhook")
async def update_project_webhook(
    project_id: str,
    body: UpdateWebhookReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await webhook_service.update_webhook(
            str(user.id), project_id,
            role=user.role,
            events=body.events,
            active=body.active,
            regenerate_secret=body.regenerate_secret,
            review_framework=body.review_framework,
            review_provider=body.review_provider,
            review_node_ids=body.review_node_ids,
            review_skill_config=body.review_skill_config,
            review_mcp_config=body.review_mcp_config,
            review_plugin_config=body.review_plugin_config,
            review_editor_ids=body.review_editor_ids,
            review_auto=body.review_auto,
            review_enabled=body.review_enabled,
            review_model_id=body.review_model_id,
            review_prompt_id=body.review_prompt_id,
            review_api_key_id=body.review_api_key_id,
            review_model_limits=body.review_model_limits,
            review_rate_limit=body.review_rate_limit,
            review_expires_at=body.review_expires_at,
            review_latest_only=body.review_latest_only,
            request=request,
        )
    except ValueError as exc:
        _raise(exc)
    await audit_user_action(
        request, user, "project_webhook.update",
        request_body=body.model_dump(exclude_none=True),
        response={"project_id": project_id, "active": result.get("active")},
    )
    return result


@router.post("/{project_id}/webhook/resync-callback")
async def resync_project_webhook_callback(
    project_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    """Recompute and sync the callback URL without rotating the secret."""
    try:
        result = await webhook_service.resync_callback(
            str(user.id), project_id, role=user.role, request=request
        )
    except ValueError as exc:
        _raise(exc)
    await audit_user_action(
        request, user, "project_webhook.resync_callback",
        request_body={"project_id": project_id},
        response={"project_id": project_id, "callback_url": result.get("callback_url")},
    )
    return result


@router.delete("/{project_id}/webhook")
async def delete_project_webhook(
    project_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        deleted = await webhook_service.disable_webhook(
            str(user.id), project_id, role=user.role
        )
    except ValueError as exc:
        _raise(exc)
    await audit_user_action(
        request, user, "project_webhook.delete",
        request_body={"project_id": project_id},
        response={"deleted": deleted},
    )
    return {"deleted": deleted}
