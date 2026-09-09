"""Team-shared notify routes (``/api/v1/teams/notify``).

The team-admin twin of :mod:`routes_notify`. A team channel is owned by the
*team* rather than a user, so every member's events can fan out to one shared
group robot instead of each person wiring their own.

Storage and access control are the same ``notify_service`` methods, called with
``owner_type="team"``; only the owner id differs (team id, from
``get_current_team_id``). ``owner_type`` is supplied by the route and never read
from the request body, so a user-side caller cannot create or reach a team
channel by passing a field — and ``_owned_channel`` refuses a channel whose
scope does not match, so a team id cannot address a personal channel either.

Response shape matches ``routes_notify``: the ``web.Resp``
``{code, message, data}`` envelope, because ``team-notifications.tsx`` uses the
generated ``Api.ts`` client directly and reads ``res.data.code === 0``.

Auditing goes to ``mc_audits`` via ``team_users_service.record_audit`` (the team
surface's convention), not ``audit_user_action`` (the user surface's).
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from .deps import client_meta, get_current_team_id, get_current_user
from .models import User
from .notify_service import notify_service
from .team_users_service import team_users_service

router = APIRouter(prefix="/api/v1/teams/notify", tags=["user-platform-team-notify"])

_NOT_FOUND = "渠道不存在或无权访问"


def _ok(data: Any = None, message: str = "") -> dict:
    """Wrap a payload in the ``web.Resp`` envelope the generated client expects."""
    return {"code": 0, "message": message, "data": data}


class CreateChannelReq(BaseModel):
    name: str
    kind: str
    webhook_url: str | None = None
    secret: str | None = None
    headers: dict | None = None
    metadata: dict | None = None
    target_id: str | None = None
    enabled: bool | None = True
    # The UI carries subscribed events on the channel itself (no subscription
    # screen), so accept them here; the service keeps the backing
    # NotifySubscription row in sync.
    event_types: list[str] | None = None


class UpdateChannelReq(BaseModel):
    name: str | None = None
    webhook_url: str | None = None
    secret: str | None = None
    headers: dict | None = None
    metadata: dict | None = None
    target_id: str | None = None
    enabled: bool | None = None
    event_types: list[str] | None = None


async def _audit(
    request: Request,
    team_id: str,
    user: User,
    operation: str,
    *,
    body: Any = None,
    response: Any = None,
) -> None:
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), operation,
        request=body, response=response, source_ip=ip, user_agent=ua,
    )


# ---- Event types ----------------------------------------------------------
@router.get("/event-types")
async def list_event_types(user: User = Depends(get_current_user)) -> dict:
    # Static catalogue, no DB access — ``list_event_types`` is a plain sync
    # method and must not be awaited.
    return _ok(notify_service.list_event_types())


# ---- Channels -------------------------------------------------------------
@router.get("/channels")
async def list_channels(team_id: str = Depends(get_current_team_id)) -> dict:
    return _ok(await notify_service.list_channels(team_id, owner_type="team"))


@router.post("/channels")
async def create_channel(
    body: CreateChannelReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    payload = body.model_dump(exclude_none=True)
    try:
        result = await notify_service.create_channel(team_id, payload, owner_type="team")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="创建通知渠道失败") from exc
    await _audit(request, team_id, user, "notify_channel.create", body=payload, response=result)
    return _ok(result, "已添加")


@router.put("/channels/{channel_id}")
async def update_channel(
    channel_id: str,
    body: UpdateChannelReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    payload = body.model_dump(exclude_none=True)
    ok = await notify_service.update_channel(
        team_id, channel_id, payload, owner_type="team"
    )
    if not ok:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _audit(
        request, team_id, user, "notify_channel.update",
        body={"channel_id": channel_id, **payload}, response={"ok": True},
    )
    return _ok({"ok": True}, "已保存")


@router.delete("/channels/{channel_id}")
async def delete_channel(
    channel_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    ok = await notify_service.delete_channel(team_id, channel_id, owner_type="team")
    if not ok:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _audit(
        request, team_id, user, "notify_channel.delete",
        body={"channel_id": channel_id}, response={"deleted": True},
    )
    return _ok({"deleted": True}, "已删除")


# ---- Test delivery --------------------------------------------------------
@router.post("/channels/{channel_id}/test")
async def test_channel(
    channel_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Send a test message to one team channel.

    A delivery failure is a non-zero envelope code rather than a 5xx: the
    request itself succeeded and the *webhook* is what is misconfigured, so the
    UI shows ``message`` instead of a generic error.
    """
    result = await notify_service.test_channel(team_id, channel_id, owner_type="team")
    if result is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    ok, error = result
    await _audit(
        request, team_id, user, "notify_channel.test",
        body={"channel_id": channel_id}, response={"ok": ok},
    )
    if not ok:
        return {"code": -1, "message": f"测试消息发送失败：{error}", "data": None}
    return _ok({"ok": True}, "测试消息已发送")


# ---- Send logs ------------------------------------------------------------
@router.get("/channels/{channel_id}/send-logs")
async def list_send_logs(
    channel_id: str, team_id: str = Depends(get_current_team_id)
) -> dict:
    rows = await notify_service.list_send_logs(team_id, channel_id, owner_type="team")
    if rows is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return _ok(rows)
