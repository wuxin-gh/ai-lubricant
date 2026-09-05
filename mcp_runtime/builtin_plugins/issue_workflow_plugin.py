"""Session-scoped requirement/bug workflow MCP built-in.

The caller never supplies project_id/issue_id. The session identity token is
resolved to either a legacy task or an editor session; the server-side binding
is the only authority. This prevents an agent from changing another issue by
editing tool arguments.
"""
from __future__ import annotations

import uuid
from typing import Any

import builtin_tool_store
from mcp_runtime.plugin_loader import PluginContext, PluginRegistrar, current_request_token
from monkeycode_compat.models_project import ProjectIssue, ProjectIssueComment
from monkeycode_compat.models_task import ProjectTask, Task
from monkeycode_compat.project_service import (
    IssueTransitionError,
    _issue_dict,
    _normalize_pending_items,
    _validate_transition,
)

_AGENT_STATUS_TARGETS = {
    "design": {"design_pending_confirmation"},
    "diagnose": {"reason_pending_confirmation"},
    "develop": {"completed"},
    "fix": {"fixed"},
}
_ROLE_FIELDS = {
    "design": {"design_document", "pending_items"},
    "diagnose": {"bug_reason", "pending_items"},
    "develop": {"resolution_note"},
    "fix": {"bug_reason", "resolution_note"},
}


async def _scope() -> tuple[Any, Any, ProjectIssue, str]:
    token = current_request_token.get().strip()
    resolved = await builtin_tool_store.resolve_token(token)
    if not resolved or resolved.get("kind") != "identity":
        raise PermissionError("issue-workflow requires a session identity token")
    token_row = resolved.get("token") or {}
    target_id = str(token_row.get("target_id") or "").strip()
    if not target_id:
        raise PermissionError("identity token is not bound to a task or editor session")

    # Current task flow: the identity token targets a Task UUID and the issue
    # binding lives in ProjectTask.
    try:
        tid = uuid.UUID(target_id)
    except (ValueError, TypeError):
        tid = None
    if tid is not None:
        task = await Task.get_or_none(id=tid)
        binding = await ProjectTask.get_or_none(task_id=tid)
        if task is not None and binding is not None and binding.issue_id is not None:
            issue = await ProjectIssue.get_or_none(id=binding.issue_id)
            if issue is not None:
                return task, binding, issue, (binding.task_role or "manual")

    # Unified editor-session flow: only sessions created from「分配任务」carry
    # issue_id and receive this token/MCP. The session row is authoritative.
    from db import PostgresClient

    session = await PostgresClient.get_editor_session_by_id(target_id)
    if not session or not session.get("issue_id"):
        raise PermissionError("identity token target is not linked to an issue")
    issue = await ProjectIssue.get_or_none(id=session["issue_id"])
    if issue is None:
        raise PermissionError("linked issue no longer exists")
    actor = type("EditorSessionActor", (), {
        "id": target_id,
        "user_id": uuid.UUID(str(session["owner_user_id"])),
        "status": session.get("status"),
    })()
    return actor, session, issue, str(session.get("task_role") or "manual")


async def _context(_args: dict, _ctx: PluginContext) -> dict:
    task, binding, issue, role = await _scope()
    comments = await ProjectIssueComment.filter(issue_id=issue.id).order_by("created_at")
    result = _issue_dict(issue)
    result.update({
        "current_task_id": str(task.id),
        "current_task_role": role,
        "comments": [
            {"id": str(c.id), "user_id": str(c.user_id), "comment": c.comment,
             "created_at": c.created_at.isoformat() if c.created_at else None}
            for c in comments
        ],
        "related_tasks": [],
    })
    return result


async def _update_content(args: dict, _ctx: PluginContext) -> dict:
    task, _binding, issue, role = await _scope()
    allowed = _ROLE_FIELDS.get(role, set())
    changed: list[str] = []
    for field in ("design_document", "bug_reason", "resolution_note"):
        if field in args:
            if field not in allowed:
                raise PermissionError(f"task role {role} cannot update {field}")
            setattr(issue, field, str(args[field] or "").strip() or None)
            changed.append(field)
    if "pending_items" in args:
        if "pending_items" not in allowed:
            raise PermissionError(f"task role {role} cannot update pending_items")
        issue.pending_items = _normalize_pending_items(args["pending_items"])
        changed.append("pending_items")
    if changed:
        changed.append("updated_at")
        await issue.save(update_fields=changed)
    return {"ok": True, "task_id": str(task.id), "issue": _issue_dict(issue)}


async def _update_status(args: dict, _ctx: PluginContext) -> dict:
    task, _binding, issue, role = await _scope()
    target = str(args.get("status") or "").strip()
    if target not in _AGENT_STATUS_TARGETS.get(role, set()):
        raise PermissionError(f"task role {role} cannot advance issue to {target}")
    _validate_transition(issue.issue_type, issue.status, target)
    issue.status = target
    await issue.save(update_fields=["status", "updated_at"])
    return {"ok": True, "task_id": str(task.id), "issue": _issue_dict(issue)}


async def _add_note(args: dict, _ctx: PluginContext) -> dict:
    task, _binding, issue, _role = await _scope()
    comment = str(args.get("comment") or "").strip()
    if not comment:
        raise ValueError("comment is required")
    row = await ProjectIssueComment.create(
        id=uuid.uuid4(), user_id=task.user_id, issue_id=issue.id, comment=comment,
    )
    return {"ok": True, "comment_id": str(row.id), "task_id": str(task.id)}


def register(reg: PluginRegistrar) -> None:
    reg.tool(
        name="get_issue_context",
        description="Read the requirement/bug linked to the current task, including documents, pending items, notes and related tasks.",
        params={"type": "object", "properties": {}},
    )(_context)
    reg.tool(
        name="update_issue_content",
        description="Update workflow content allowed for this task role (design document, bug reason, pending items, or resolution note).",
        params={
            "type": "object",
            "properties": {
                "design_document": {"type": "string"},
                "bug_reason": {"type": "string"},
                "resolution_note": {"type": "string"},
                "pending_items": {"type": "array", "items": {}},
            },
        },
    )(_update_content)
    reg.tool(
        name="update_issue_status",
        description="Advance the linked issue to the single status permitted for the current task role. Confirmation statuses remain human-only.",
        params={
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
        },
    )(_update_status)
    reg.tool(
        name="add_issue_note",
        description="Append a note to the requirement/bug linked to the current task.",
        params={
            "type": "object",
            "properties": {"comment": {"type": "string"}},
            "required": ["comment"],
        },
    )(_add_note)


def apply_config(_ctx: PluginContext) -> None:
    """No mutable config: authorization is derived per request from its token."""


__all__ = ["register", "apply_config"]
