"""Public webhook receiver for project review deliveries."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Request, Response, status
from tortoise.exceptions import IntegrityError

from .models_webhook import ProjectWebhook
from .models_webhook_event import ProjectWebhookEvent
from .webhook_events import (
    normalize_webhook_event,
    parse_json_body,
    verify_webhook_signature,
)

router = APIRouter(prefix="/api/v1/webhooks", tags=["project-webhook-receive"])
_MAX_BODY_BYTES = 2 * 1024 * 1024


async def _bounded_body(request: Request) -> bytes:
    declared = request.headers.get("content-length")
    if declared:
        try:
            if int(declared) > _MAX_BODY_BYTES:
                raise HTTPException(status_code=413, detail="webhook body too large")
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid content length")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > _MAX_BODY_BYTES:
            raise HTTPException(status_code=413, detail="webhook body too large")
        chunks.append(chunk)
    body = b"".join(chunks)
    if not body:
        raise HTTPException(status_code=400, detail="empty webhook body")
    return body


@router.post("/projects/{project_id}", status_code=status.HTTP_202_ACCEPTED)
async def receive_project_webhook(project_id: str, request: Request, response: Response) -> dict:
    try:
        pid = uuid.UUID(project_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="webhook not found")
    hook = await ProjectWebhook.get_or_none(project_id=pid, active=True)
    if hook is None:
        raise HTTPException(status_code=404, detail="webhook not found")
    body = await _bounded_body(request)
    if not verify_webhook_signature(hook.platform, request.headers, body, hook.secret or ""):
        raise HTTPException(status_code=401, detail="invalid webhook signature")
    try:
        payload = parse_json_body(body)
    except (UnicodeDecodeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid webhook payload")
    event = normalize_webhook_event(hook.platform, request.headers, body, payload)
    event_id = uuid.uuid4()
    if event.ignored_reason:
        event_status = "ignored"
        event_error = event.ignored_reason
    elif not hook.review_enabled:
        event_status = "ignored"
        event_error = "review_disabled"
    else:
        event_status = "pending"
        event_error = None
    try:
        row = await ProjectWebhookEvent.create(
            id=event_id,
            project_id=pid,
            webhook_id=hook.id,
            platform=hook.platform,
            delivery_id=event.delivery_id,
            event_type=event.event_type,
            payload=event.payload,
            commit_sha=event.commit_sha,
            before_sha=event.before_sha,
            base_ref=event.base_ref,
            head_ref=event.head_ref,
            pr_number=event.pr_number,
            source_branch=event.source_branch,
            target_branch=event.target_branch,
            status=event_status,
            last_error=event_error,
        )
    except IntegrityError:
        row = await ProjectWebhookEvent.get(
            webhook_id=hook.id, delivery_id=event.delivery_id
        )
    response.headers["Cache-Control"] = "no-store"
    return {"accepted": True, "event_id": str(row.id), "status": row.status}
