"""C-side notify domain routes (``/api/v1/users/notify``).

Mirrors the MonkeyCode notify handler contract: notification channels,
subscriptions, the static event-type catalogue, and test delivery. Access
control (owner / privileged) lives in ``notify_service``. The channel ``secret``
is masked in responses; outbound delivery lives in ``notify_dispatch``.

Response shape: these routes are consumed by the **generated** ``Api.ts`` client
(``notifications.tsx`` builds a raw ``new Api()``, so it bypasses the
``endpointMap``/``requestUtils`` adapter that wraps plain REST responses). That
client reads ``res.data.code === 0``, so every route here returns the Go
``web.Resp`` ``{code, message, data}`` envelope itself — same as
``routes_server``. Returning a bare list/dict makes the UI read ``code`` as
``undefined`` and report failure on a successful call.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from .deps import audit_user_action, get_current_user
from .models import User
from .notify_service import notify_service

router = APIRouter(prefix="/api/v1/users/notify", tags=["monkeycode-notify"])


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
    # The UI carries the subscribed events on the channel itself (there is no
    # subscription screen), so accept them here and let the service keep the
    # backing NotifySubscription row in sync.
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


class CreateSubscriptionReq(BaseModel):
    scope: str | None = "self"
    event_types: list[str] | None = None
    enabled: bool | None = True


class SubscriptionRuleReq(BaseModel):
    channel_id: str
    event_type: str
    filters: dict | None = None
    enabled: bool | None = True


class SubscriptionRulePatchReq(BaseModel):
    channel_id: str | None = None
    event_type: str | None = None
    filters: dict | None = None
    enabled: bool | None = None


# ---- Event types / parameterized subscription rules -----------------------
@router.get("/subscription-rules")
async def list_subscription_rules(user: User = Depends(get_current_user)) -> dict:
    return _ok(await notify_service.list_rules(str(user.id), owner_type="user"))


@router.post("/subscription-rules")
async def create_subscription_rule(
    body: SubscriptionRuleReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    result = await notify_service.create_rule(
        str(user.id), body.model_dump(exclude_none=True), owner_type="user"
    )
    if result is None:
        raise HTTPException(status_code=400, detail="订阅事件或通知渠道无效")
    await audit_user_action(request, user, "notify_subscription_rule.create", request_body=body.model_dump(), response=result)
    return _ok(result, "已添加")


@router.put("/subscription-rules/{rule_id}")
async def update_subscription_rule(
    rule_id: str,
    body: SubscriptionRulePatchReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    ok = await notify_service.update_rule(
        str(user.id), rule_id, body.model_dump(exclude_none=True), owner_type="user"
    )
    if not ok:
        raise HTTPException(status_code=404, detail="订阅规则不存在或无效")
    await audit_user_action(request, user, "notify_subscription_rule.update", request_body={"rule_id": rule_id, **body.model_dump(exclude_none=True)})
    return _ok({"ok": True}, "已保存")


@router.delete("/subscription-rules/{rule_id}")
async def delete_subscription_rule(
    rule_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    ok = await notify_service.delete_rule(str(user.id), rule_id, owner_type="user")
    if not ok:
        raise HTTPException(status_code=404, detail="订阅规则不存在")
    await audit_user_action(request, user, "notify_subscription_rule.delete", request_body={"rule_id": rule_id})
    return _ok({"deleted": True}, "已删除")


@router.get("/event-types")
async def list_event_types(user: User = Depends(get_current_user)) -> dict:
    # User-side console may only configure user-scoped events (task/node online);
    # admin-only platform events (freeze/security/api_key…) are filtered out.
    # Sync method (static catalogue, no DB) — must not be awaited.
    return _ok(notify_service.list_event_types(owner_scope="user"))


# ---- Channels -------------------------------------------------------------
@router.get("/channels")
async def list_channels(user: User = Depends(get_current_user)) -> dict:
    return _ok(await notify_service.list_channels(str(user.id), role=user.role))


@router.post("/channels")
async def create_channel(
    body: CreateChannelReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    try:
        result = await notify_service.create_channel(
            str(user.id), body.model_dump(exclude_none=True)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="创建通知渠道失败") from exc
    await audit_user_action(
        request, user, "notify_channel.create",
        request_body=body.model_dump(exclude_none=True), response=result,
    )
    return _ok(result, "已添加")


@router.put("/channels/{channel_id}")
async def update_channel(
    channel_id: str, body: UpdateChannelReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await notify_service.update_channel(
        str(user.id), channel_id, body.model_dump(exclude_none=True), role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="渠道不存在或无权访问")
    await audit_user_action(
        request, user, "notify_channel.update",
        request_body={"channel_id": channel_id, **body.model_dump(exclude_none=True)},
        response={"ok": True},
    )
    return _ok({"ok": True}, "已保存")


@router.delete("/channels/{channel_id}")
async def delete_channel(
    channel_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await notify_service.delete_channel(str(user.id), channel_id, role=user.role)
    if not ok:
        raise HTTPException(status_code=404, detail="渠道不存在或无权访问")
    await audit_user_action(
        request, user, "notify_channel.delete",
        request_body={"channel_id": channel_id}, response={"deleted": True},
    )
    return _ok({"deleted": True}, "已删除")


# ---- Test delivery --------------------------------------------------------
@router.post("/channels/{channel_id}/test")
async def test_channel(
    channel_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    """Send a test message to one channel.

    The UI has always had a "测试" button calling this path, but the route did
    not exist — it 404'd. A delivery failure is reported as a non-zero envelope
    code (not a 5xx): the request itself succeeded, the *webhook* is what is
    misconfigured, and the UI shows ``message`` to the user.
    """
    result = await notify_service.test_channel(str(user.id), channel_id, role=user.role)
    if result is None:
        raise HTTPException(status_code=404, detail="渠道不存在或无权访问")
    ok, error = result
    await audit_user_action(
        request, user, "notify_channel.test",
        request_body={"channel_id": channel_id}, response={"ok": ok},
    )
    if not ok:
        return {"code": -1, "message": f"测试消息发送失败：{error}", "data": None}
    return _ok({"ok": True}, "测试消息已发送")


# ---- Subscriptions --------------------------------------------------------
@router.get("/channels/{channel_id}/subscriptions")
async def list_subscriptions(channel_id: str, user: User = Depends(get_current_user)) -> dict:
    rows = await notify_service.list_subscriptions(str(user.id), channel_id, role=user.role)
    if rows is None:
        raise HTTPException(status_code=404, detail="渠道不存在或无权访问")
    return _ok(rows)


@router.post("/channels/{channel_id}/subscriptions")
async def create_subscription(
    channel_id: str, body: CreateSubscriptionReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    sub = await notify_service.create_subscription(
        str(user.id), channel_id, body.model_dump(exclude_none=True), role=user.role
    )
    if sub is None:
        raise HTTPException(status_code=404, detail="渠道不存在或无权访问")
    await audit_user_action(
        request, user, "notify_subscription.create",
        request_body={"channel_id": channel_id, **body.model_dump(exclude_none=True)},
        response=sub,
    )
    return _ok(sub, "已添加")


@router.delete("/channels/{channel_id}/subscriptions/{sub_id}")
async def delete_subscription(
    channel_id: str, sub_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await notify_service.delete_subscription(
        str(user.id), channel_id, sub_id, role=user.role
    )
    if not ok:
        raise HTTPException(status_code=404, detail="订阅不存在或无权访问")
    await audit_user_action(
        request, user, "notify_subscription.delete",
        request_body={"channel_id": channel_id, "sub_id": sub_id}, response={"deleted": True},
    )
    return _ok({"deleted": True}, "已删除")


# ---- Send logs ------------------------------------------------------------
@router.get("/channels/{channel_id}/send-logs")
async def list_send_logs(channel_id: str, user: User = Depends(get_current_user)) -> dict:
    rows = await notify_service.list_send_logs(str(user.id), channel_id, role=user.role)
    if rows is None:
        raise HTTPException(status_code=404, detail="渠道不存在或无权访问")
    return _ok(rows)
