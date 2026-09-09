"""User-side in-app notification center.

The management center uses ``/admin/notifications`` and platform events. This
parallel surface is deliberately owner-scoped to the authenticated C-side user
and exposes only user events through the subscription catalogue. It shares the
same ``notifications`` table and ``notify_core`` outbox, but never widens a
user query into platform/admin rows.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from .deps import get_current_user
from .models import User
from .notify_service import notify_service

router = APIRouter(prefix="/api/v1/users/notifications", tags=["user-platform-user-notifications"])


class UserNotificationRuleReq(BaseModel):
    channel_id: str
    event_type: str
    filters: dict | None = None
    enabled: bool | None = True


class UserNotificationRulePatchReq(BaseModel):
    channel_id: str | None = None
    event_type: str | None = None
    filters: dict | None = None
    enabled: bool | None = None


def _ok(data: Any = None, message: str = "") -> dict:
    return {"code": 0, "message": message, "data": data}


@router.get("")
async def list_user_notifications(
    page: int = 1,
    page_size: int = 20,
    status: str = "",
    severity: str = "",
    event_type: str = "",
    q: str = "",
    user: User = Depends(get_current_user),
) -> dict:
    from db import PostgresClient

    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 20), 100))
    data = await PostgresClient.query_notifications(
        {
            "status": status,
            "severity": severity,
            "event_type": event_type,
            "owner_type": "user",
            "user_id": str(user.id),
            "q": q,
        },
        page_size,
        (page - 1) * page_size,
    )
    return _ok({
        "items": data.get("rows") or [],
        "total": data.get("total") or 0,
        "page": page,
        "page_size": page_size,
        "unread_count": data.get("unread_count") or 0,
    })


@router.get("/unread-count")
async def user_unread_notification_count(user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    data = await PostgresClient.query_notifications(
        {"owner_type": "user", "user_id": str(user.id)}, 1, 0
    )
    return _ok({"unread_count": data.get("unread_count") or 0})


@router.get("/event-types")
async def user_notification_event_types(user: User = Depends(get_current_user)) -> dict:
    return _ok(notify_service.list_event_types(owner_scope="user"))


@router.get("/subscription-rules")
async def list_user_notification_rules(user: User = Depends(get_current_user)) -> dict:
    return _ok(await notify_service.list_rules(str(user.id), owner_type="user"))


@router.post("/subscription-rules")
async def create_user_notification_rule(
    body: UserNotificationRuleReq,
    user: User = Depends(get_current_user),
) -> dict:
    row = await notify_service.create_rule(
        str(user.id), body.model_dump(exclude_none=True), owner_type="user"
    )
    if row is None:
        raise HTTPException(status_code=400, detail="用户侧事件或通知渠道无效")
    return _ok(row, "已添加")


@router.put("/subscription-rules/{rule_id}")
async def update_user_notification_rule(
    rule_id: str,
    body: UserNotificationRulePatchReq,
    user: User = Depends(get_current_user),
) -> dict:
    ok = await notify_service.update_rule(
        str(user.id), rule_id, body.model_dump(exclude_none=True), owner_type="user"
    )
    if not ok:
        raise HTTPException(status_code=404, detail="订阅规则不存在或无效")
    return _ok({"ok": True}, "已保存")


@router.delete("/subscription-rules/{rule_id}")
async def delete_user_notification_rule(rule_id: str, user: User = Depends(get_current_user)) -> dict:
    ok = await notify_service.delete_rule(str(user.id), rule_id, owner_type="user")
    if not ok:
        raise HTTPException(status_code=404, detail="订阅规则不存在")
    return _ok({"deleted": True}, "已删除")


# ---- Configured events (primary path) -------------------------------------
# Same three-layer shape as the admin surface, but scoped to this user and
# restricted to user-scope event types by ``notify_service._event_allowed``.


@router.get("/events")
async def list_user_notification_events(user: User = Depends(get_current_user)) -> dict:
    return _ok(await notify_service.list_events(str(user.id), owner_type="user"))


@router.post("/events")
async def create_user_notification_event(
    body: dict, user: User = Depends(get_current_user)
) -> dict:
    row = await notify_service.create_event(str(user.id), body, owner_type="user")
    if row is None:
        raise HTTPException(status_code=400, detail="事件类型不可用或参数无效")
    return _ok(row, "已添加")


@router.put("/events/{event_id}")
async def update_user_notification_event(
    event_id: str, body: dict, user: User = Depends(get_current_user)
) -> dict:
    ok = await notify_service.update_event(str(user.id), event_id, body, owner_type="user")
    if not ok:
        raise HTTPException(status_code=404, detail="事件不存在或参数无效")
    return _ok({"ok": True}, "已保存")


@router.delete("/events/{event_id}")
async def delete_user_notification_event(
    event_id: str, user: User = Depends(get_current_user)
) -> dict:
    ok = await notify_service.delete_event(str(user.id), event_id, owner_type="user")
    if not ok:
        raise HTTPException(status_code=404, detail="事件不存在")
    return _ok({"deleted": True}, "已删除")


@router.put("/read-all")
async def mark_all_user_notifications_read(user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    updated = await PostgresClient.mark_all_notifications_read_for_owner("user", str(user.id))
    return _ok({"updated": updated})


@router.post("/batch-delete")
async def batch_delete_user_notifications(
    body: dict, user: User = Depends(get_current_user)
) -> dict:
    from db import PostgresClient

    raw_ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="ids 必须为非空数组")
    if len(raw_ids) > 500:
        raise HTTPException(status_code=400, detail="一次最多删除 500 条通知")
    try:
        ids = [int(i) for i in raw_ids]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="ids 必须为整数数组")
    deleted = await PostgresClient.delete_notifications_by_ids_for_owner(
        ids, "user", str(user.id)
    )
    return _ok({"deleted": deleted})


@router.post("/clear-read")
async def clear_read_user_notifications(user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    deleted = await PostgresClient.delete_notifications_by_filter_for_owner(
        "user", str(user.id), status="read"
    )
    return _ok({"deleted": deleted})


@router.post("/clear-all")
async def clear_all_user_notifications(user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    deleted = await PostgresClient.delete_notifications_by_filter_for_owner(
        "user", str(user.id), status=None
    )
    return _ok({"deleted": deleted})


@router.get("/{notification_id}")
async def get_user_notification(notification_id: int, user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    row = await PostgresClient.get_notification_detail_for_owner(
        notification_id, "user", str(user.id)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="通知不存在")
    return _ok(row)


@router.put("/{notification_id}/read")
async def mark_user_notification_read(notification_id: int, user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    ok = await PostgresClient.mark_notification_read_for_owner(
        notification_id, "user", str(user.id)
    )
    if not ok:
        raise HTTPException(status_code=404, detail="通知不存在")
    return _ok({"ok": True})


@router.delete("/{notification_id}")
async def delete_user_notification(notification_id: int, user: User = Depends(get_current_user)) -> dict:
    from db import PostgresClient

    ok = await PostgresClient.delete_notification_for_owner(
        notification_id, "user", str(user.id)
    )
    if not ok:
        raise HTTPException(status_code=404, detail="通知不存在")
    return _ok({"deleted": True})
