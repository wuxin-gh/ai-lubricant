"""Task-scoped structured result sink for webhook-triggered reviews."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loguru import logger

import builtin_tool_store
from mcp_runtime.plugin_loader import PluginContext, PluginRegistrar, current_request_token
from user_platform.models_webhook_event import ProjectWebhookEvent

_ALLOWED_SEVERITIES = {"critical", "high", "medium", "low"}
_MAX_FINDINGS = 500


async def _scope() -> ProjectWebhookEvent:
    token = current_request_token.get().strip()
    resolved = await builtin_tool_store.resolve_token(token)
    if not resolved or resolved.get("kind") != "identity":
        raise PermissionError("review-result requires a session identity token")
    target_id = str((resolved.get("token") or {}).get("target_id") or "").strip()
    event = await ProjectWebhookEvent.get_or_none(id=target_id)
    if event is None:
        raise PermissionError("identity token is not bound to a review event")
    return event


def _normalize_finding(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("each finding must be an object")
    path = str(value.get("path") or "").strip().replace("\\", "/")
    if not path or path.startswith("/") or ".." in path.split("/"):
        raise ValueError("finding path must be repository-relative")
    content = str(value.get("content") or "").strip()
    if not content:
        raise ValueError("finding content is required")
    severity = str(value.get("severity") or "medium").strip().lower()
    if severity not in _ALLOWED_SEVERITIES:
        raise ValueError("unsupported finding severity")
    start = max(0, int(value.get("start_line") or 0))
    end = max(start, int(value.get("end_line") or start))
    return {
        "path": path,
        "start_line": start,
        "end_line": end,
        "severity": severity,
        "content": content[:8000],
        "suggestion": str(value.get("suggestion") or "")[:16000] or None,
    }


async def _submit_findings(args: dict, _ctx: PluginContext) -> dict:
    event = await _scope()
    incoming = list(args.get("findings") or [])
    current = list(event.findings or [])
    if len(current) + len(incoming) > _MAX_FINDINGS:
        raise ValueError("too many review findings")
    current.extend(_normalize_finding(value) for value in incoming)
    event.findings = current
    await event.save(update_fields=["findings", "updated_at"])
    return {"ok": True, "event_id": str(event.id), "finding_count": len(current)}


async def _complete_review(args: dict, _ctx: PluginContext) -> dict:
    event = await _scope()
    event.review_summary = str(args.get("summary") or "").strip()[:16000] or None
    event.review_completed_at = datetime.now(UTC)
    await event.save(
        update_fields=["review_summary", "review_completed_at", "updated_at"]
    )
    # Release the review node slot the moment the review reports completion so
    # the node can host the next queued review immediately.
    try:
        from user_platform.review_node_service import review_node_service

        await review_node_service.release_review_slot(event_id=str(event.id))
    except Exception:
        logger.exception("[review-result] lease release on completion failed")
    return {
        "ok": True,
        "event_id": str(event.id),
        "finding_count": len(event.findings or []),
    }


def register(reg: PluginRegistrar) -> None:
    reg.tool(
        name="submit_findings",
        description="Submit structured, concrete code review findings for the webhook event bound to this session.",
        params={
            "type": "object",
            "properties": {
                "findings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "start_line": {"type": "integer"},
                            "end_line": {"type": "integer"},
                            "severity": {"type": "string"},
                            "content": {"type": "string"},
                            "suggestion": {"type": "string"},
                        },
                        "required": ["path", "severity", "content"],
                    },
                }
            },
            "required": ["findings"],
        },
    )(_submit_findings)
    reg.tool(
        name="complete_review",
        description="Mark structured finding submission complete and store the final review summary.",
        params={
            "type": "object",
            "properties": {"summary": {"type": "string"}},
        },
    )(_complete_review)


def apply_config(_ctx: PluginContext) -> None:
    """Authorization is derived from the request token."""


__all__ = ["register", "apply_config"]
