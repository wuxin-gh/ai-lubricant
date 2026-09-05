"""C-side user platform routes (``/api/v1/users``).

These endpoints mirror the MonkeyCode user auth contract so the ported
frontend behaves identically. They are mounted only when the compatibility
layer is enabled, live under their own prefix, and never touch ``/admin/*``
or the ``/v1/*`` model pipeline.

Scope of this first slice: the core auth loop (password login / status /
logout / member list). OAuth / OIDC / email recovery depend on external
identity sources + SMTP and are layered on later without blocking this core.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

import uuid

from .auth_service import AuthedUser, auth_service
from .captcha_service import captcha_service
from .config import settings as compat_settings
from .deps import audit_user_action, get_current_user
from .masking import mask_key as _mask_key
from .models import Team, User
from .models_team_admin import TeamOIDCConfig
from .oidc_service import OIDCError, oidc_service
from .session import USER_SESSION_COOKIE, session_store

router = APIRouter(prefix="/api/v1/users", tags=["monkeycode-users"])


async def _require_captcha(token: str | None) -> None:
    """Gate a pre-auth action on a valid cap.js verification token.

    No-op when ``captcha_required`` is off (escape hatch for Redis outages).
    Otherwise the token must be a live, single-use verification token issued by
    ``/api/v1/public/captcha/redeem``; it is consumed on success. Any missing or
    stale token raises 400 so the client re-solves.
    """
    if not compat_settings.captcha_required:
        return
    if not token or not await captcha_service.validate_token(token):
        raise HTTPException(status_code=400, detail="人机验证失败，请重试")


class PasswordLoginReq(BaseModel):
    email: str
    password: str
    # cap.js PoW verification token (redeemed via /api/v1/public/captcha/*).
    captcha_token: str | None = None


class RegisterReq(BaseModel):
    email: str
    password: str
    name: str | None = None
    captcha_token: str | None = None


class UserResp(BaseModel):
    id: str
    name: str
    email: str | None = None
    avatar_url: str | None = None
    role: str
    status: str


def _user_resp(user: User) -> UserResp:
    return UserResp(
        id=str(user.id),
        name=user.name,
        email=user.email,
        avatar_url=user.avatar_url,
        role=user.role,
        status=user.status,
    )


@router.post("/password-login", response_model=UserResp)
async def password_login(body: PasswordLoginReq, response: Response) -> UserResp:
    await _require_captcha(body.captcha_token)
    try:
        authed, cookie = await auth_service.password_login(body.email, body.password)
    except ValueError as exc:
        reason = str(exc)
        if reason == "user_disabled":
            raise HTTPException(status_code=403, detail="账号已停用") from exc
        raise HTTPException(status_code=401, detail="邮箱或密码错误") from exc
    response.set_cookie(
        key=USER_SESSION_COOKIE,
        value=cookie,
        max_age=session_store.expire_seconds,
        httponly=True,
        samesite=compat_settings.session_cookie_samesite,
        secure=compat_settings.session_cookie_secure,
        path="/",
    )
    return UserResp(
        id=authed.id,
        name=authed.name,
        email=authed.email,
        role=authed.role,
        status=authed.status,
    )


@router.post("/register", response_model=UserResp)
async def register(body: RegisterReq, response: Response) -> UserResp:
    """Create a C-side platform user and start a session immediately.

    Independent of ``/admin/*``. Never touches the model request pipeline.
    """
    await _require_captcha(body.captcha_token)
    try:
        authed = await auth_service.create_user(
            email=body.email, password=body.password, name=body.name
        )
    except ValueError as exc:
        reason = str(exc)
        detail = {
            "invalid_email": "邮箱格式不正确",
            "weak_password": "密码至少 6 位",
            "email_taken": "邮箱已被注册",
        }.get(reason, "注册失败")
        status_code = 409 if reason == "email_taken" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    # Issue a session so the client is logged in right after registering.
    _, cookie = await auth_service.password_login(body.email, body.password)
    response.set_cookie(
        key=USER_SESSION_COOKIE,
        value=cookie,
        max_age=session_store.expire_seconds,
        httponly=True,
        samesite=compat_settings.session_cookie_samesite,
        secure=compat_settings.session_cookie_secure,
        path="/",
    )
    return UserResp(
        id=authed.id,
        name=authed.name,
        email=authed.email,
        role=authed.role,
        status=authed.status,
    )


@router.get("/status")
async def status(request: Request) -> dict:
    """Soft auth: returns login state without forcing a 401.

    The generated MonkeyCode client expects the Go ``web.Resp`` envelope
    ``{code, message, data}`` with ``code === 0`` on success, and reads
    ``data.user`` (DomainTeamUserInfo shape). We match that so the portal's
    runtime provider resolves the session correctly.
    """
    cookie = request.cookies.get(USER_SESSION_COOKIE)
    user_data: dict | None = None
    if cookie:
        authed: AuthedUser | None = await auth_service.resolve_session(cookie)
        if authed is not None:
            user_data = {
                "id": authed.id,
                "name": authed.name,
                "email": authed.email,
                "role": authed.role,
                "status": authed.status,
                "avatar_url": None,
                "has_password": True,
                "is_blocked": False,
                "team": None,
                "identities": [],
            }
    return {
        "code": 0,
        "message": "",
        "data": {"user": user_data, "teams": None} if user_data else None,
    }


@router.get("/oidc/default-team")
async def oidc_default_team() -> dict:
    """Default team login-methods probe (offline edition login page).

    Returns every enabled login method of the first team that has one. The
    login page renders one button per method. ``methods`` is empty when no team
    has SSO enabled — the page falls back to password login.
    """
    first = await TeamOIDCConfig.filter(enabled=True).first()
    if first is None:
        return _web_resp({"enabled": False, "team_id": "", "team_name": "", "methods": []})
    team = await Team.get_or_none(id=first.team_id, is_deleted=False)
    methods = await _public_methods(first.team_id)
    return _web_resp(
        {
            "enabled": True,
            "team_id": str(first.team_id),
            "team_name": team.name if team else "",
            "methods": methods,
        }
    )


@router.get("/oidc/teams/{team_id}")
async def oidc_team_config(team_id: str) -> dict:
    """Public login methods for one team (team-scoped login page)."""
    methods = await _public_methods(team_id)
    return _web_resp({"enabled": bool(methods), "methods": methods})


async def _public_methods(team_id: str) -> list[dict]:
    """One entry per enabled SSO method, for the login page button list."""
    rows = await TeamOIDCConfig.filter(team_id=team_id, enabled=True).order_by("created_at")
    return [
        {
            "method_id": str(cfg.id),
            "type": cfg.type or "oidc",
            "display_name": cfg.display_name or cfg.name or "",
            "login_url": f"/api/v1/users/oidc/login?method_id={cfg.id}",
        }
        for cfg in rows
    ]


def _oidc_redirect_uri(request: Request) -> str:
    return f"{str(request.base_url).rstrip('/')}/api/v1/users/oidc/callback"


@router.get("/oidc/login")
async def oidc_login(request: Request, method_id: str) -> RedirectResponse:
    """Begin a login-method flow: issue state, then redirect to the IdP."""
    cfg = await TeamOIDCConfig.get_or_none(id=method_id, enabled=True)
    if cfg is None:
        raise HTTPException(status_code=400, detail="该登录方式不存在或已关闭")
    try:
        state, nonce = await oidc_service.issue_state(str(cfg.id))
        redirect_uri = _oidc_redirect_uri(request)
        authorize_url = await oidc_service.build_authorize_url(cfg, redirect_uri, state, nonce)
    except OIDCError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url=authorize_url, status_code=302)


@router.get("/oidc/callback")
async def oidc_callback(request: Request, code: str, state: str) -> RedirectResponse:
    """Handle the IdP redirect, exchange code, verify claims, start a session."""
    pending = await oidc_service.consume_state(state)
    if pending is None:
        raise HTTPException(status_code=400, detail="登录状态无效或已过期，请重新登录")
    method_id = pending["method_id"]
    nonce = pending["nonce"]
    cfg = await TeamOIDCConfig.get_or_none(id=method_id, enabled=True)
    if cfg is None:
        return _oidc_error_redirect(str(cfg.team_id) if cfg else "", "该登录方式已关闭")
    redirect_uri = _oidc_redirect_uri(request)
    try:
        token_resp = await oidc_service.exchange_token(cfg, code, redirect_uri)
        claims = await oidc_service.verify_claims(cfg, token_resp, nonce)
        user = await oidc_service.resolve_or_create_user(cfg, claims, str(cfg.team_id))
        cookie = await auth_service.issue_session(user)
    except OIDCError as exc:
        return _oidc_error_redirect(str(cfg.team_id), str(exc))
    redirect = RedirectResponse(url="/console/", status_code=302)
    redirect.set_cookie(
        key=USER_SESSION_COOKIE,
        value=cookie,
        max_age=session_store.expire_seconds,
        httponly=True,
        samesite=compat_settings.session_cookie_samesite,
        secure=compat_settings.session_cookie_secure,
        path="/",
    )
    try:
        await audit_user_action(request, user, "oidc.login", response={"ok": True})
    except Exception:  # noqa: BLE001 - audit never blocks login
        pass
    return redirect


def _oidc_error_redirect(team_id: str, message: str) -> RedirectResponse:
    from urllib.parse import urlencode

    target = f"/team-login/{team_id}" if team_id else "/"
    query = urlencode({"error": message})
    return RedirectResponse(url=f"{target}?{query}", status_code=302)


def _web_resp(data: dict) -> dict:
    return {"code": 0, "message": "", "data": data}


@router.post("/logout")
async def logout(request: Request, response: Response, user: User = Depends(get_current_user)) -> dict:
    cookie = request.cookies.get(USER_SESSION_COOKIE) or ""
    await auth_service.logout(str(user.id), cookie)
    response.delete_cookie(key=USER_SESSION_COOKIE, path="/")
    await audit_user_action(request, user, "user.logout", response={"ok": True})
    return {"ok": True}


@router.get("/members", response_model=list[UserResp])
async def member_list(user: User = Depends(get_current_user)) -> list[UserResp]:
    """同团队成员列表（不含 email，避免跨租户 PII 泄露）。

    历史返回全平台用户含 email；改为按当前用户团队收窄、并隐藏 email 字段。
    """
    from .deps import resolve_team_id
    from .models import TeamMember
    try:
        team_id = await resolve_team_id(str(user.id))
        member_uids = await TeamMember.filter(team_id=uuid.UUID(team_id)).values_list("user_id", flat=True)
    except Exception:
        member_uids = []
    if not member_uids:
        return []
    rows = await User.filter(id__in=list(member_uids), is_deleted=False).order_by("created_at").limit(500)
    # email 不回传（UserResp.email 默认 None 时省略）
    return [UserResp(
        id=str(r.id), name=r.name, email=None, avatar_url=r.avatar_url, role=r.role, status=r.status,
    ) for r in rows]


# ---------------------------------------------------------------------------
# Runtime keys (our replacement for MonkeyCode's llmproxy ModelApiKey).
#
# A runtime key is an ordinary row in the main ``api_keys`` table, additionally
# tagged with ``user_id`` (+ optional ``vm_id``). The model pipeline treats it
# as a normal key — routing/limits/retry/billing are unchanged; the tag only
# flows through ``get_api_key_config`` for ownership/attribution.
#
# This lets a VM call our ``/v1/chat/completions`` directly instead of running
# a separate llmproxy: our platform is a superset of that reverse proxy.
# ---------------------------------------------------------------------------


class IssueRuntimeKeyReq(BaseModel):
    vm_id: str | None = None
    name: str | None = None


class RuntimeKeyResp(BaseModel):
    id: int
    key: str
    name: str
    vm_id: str | None = None
    user_id: str | None = None
    disabled: bool = False


class RuntimeKeyListItem(BaseModel):
    id: int
    key_masked: str
    name: str
    vm_id: str | None = None
    disabled: bool = False
    created_at: float | None = None


@router.post("/model-gateway/runtime-keys", response_model=RuntimeKeyResp)
async def create_runtime_key(
    body: IssueRuntimeKeyReq, request: Request, user: User = Depends(get_current_user)
) -> RuntimeKeyResp:
    from db import PostgresClient

    row = await PostgresClient.issue_runtime_api_key(
        {
            "user_id": str(user.id),
            "vm_id": body.vm_id or None,
            "name": body.name or "runtime-key",
        }
    )
    # response omits the plaintext key (audit rows must not store issued secrets).
    await audit_user_action(
        request, user, "runtime_key.create",
        request_body=body.model_dump(exclude_none=True),
        response={"id": row["id"], "name": row.get("name") or ""},
    )
    return RuntimeKeyResp(
        id=row["id"],
        key=row["key"],
        name=row.get("name") or "",
        vm_id=row.get("vm_id"),
        user_id=row.get("user_id"),
        disabled=bool(row.get("disabled")),
    )


@router.get("/model-gateway/runtime-keys", response_model=list[RuntimeKeyListItem])
async def list_runtime_keys(
    user: User = Depends(get_current_user),
) -> list[RuntimeKeyListItem]:
    from db import PostgresClient

    rows = await PostgresClient.list_api_keys_by_user(str(user.id))
    return [
        RuntimeKeyListItem(
            id=row["id"],
            key_masked=_mask_key(row.get("key") or ""),
            name=row.get("name") or "",
            vm_id=row.get("vm_id"),
            disabled=bool(row.get("disabled")),
            created_at=row.get("created_at"),
        )
        for row in rows
    ]


@router.delete("/model-gateway/runtime-keys/{key_id}")
async def delete_runtime_key(
    key_id: int, request: Request, user: User = Depends(get_current_user)
) -> dict:
    from db import PostgresClient

    deleted = await PostgresClient.delete_runtime_api_key_for_user(key_id, str(user.id))
    if not deleted:
        raise HTTPException(status_code=404, detail="runtime key not found")
    await audit_user_action(
        request, user, "runtime_key.delete",
        request_body={"key_id": key_id}, response={"deleted": True},
    )
    return {"deleted": True}


# ---------------------------------------------------------------------------
# System keys (group-bound). A platform user need not configure their own key:
# an admin binds a system ``api_keys`` row to one or more team groups, and every
# member of those groups is authorized to use it directly. A user's effective
# system keys are the union across all their groups (permission is a group-set
# union — how many groups they are in does not matter).
#
# ``resolve_system_api_key_for_user`` is the shared resolver the request path
# uses; the endpoint below surfaces the same set (key masked) for the console so
# the UI can show "已由平台配置" instead of a self-config prompt.
# ---------------------------------------------------------------------------


async def resolve_system_api_key_for_user(user_id: str) -> list[dict]:
    """The system keys a user may use via their group memberships (union).

    Returns full ``api_keys`` rows (incl. plaintext ``key``) so the request path
    can pick one and enter the main pipeline. Empty when the user is in no group
    or none of their groups has a bound key — the caller then falls back to the
    existing behavior (self-key / global default) rather than hard-failing.
    """
    from .models import TeamGroupMember

    memberships = await TeamGroupMember.filter(user_id=user_id)
    group_ids = [str(m.group_id) for m in memberships]
    if not group_ids:
        return []
    from db import PostgresClient

    return await PostgresClient.list_api_keys_for_groups(group_ids)


class SystemKeyItem(BaseModel):
    id: int
    key_masked: str
    name: str
    disabled: bool = False


class ParentKeyItem(SystemKeyItem):
    source: str
    editor_provider_whitelist: list[str] = []
    editor_provider_blacklist: list[str] = []


@router.get("/model-gateway/system-keys", response_model=list[SystemKeyItem])
async def list_system_keys(user: User = Depends(get_current_user)) -> list[SystemKeyItem]:
    """The system keys available to the caller via their group bindings.

    Key values are masked — the console only needs to know a key exists (and its
    name) to switch from "configure your own key" to "provided by the platform".
    """
    rows = await resolve_system_api_key_for_user(str(user.id))
    return [
        SystemKeyItem(
            id=row["id"],
            key_masked=_mask_key(row.get("key") or ""),
            name=row.get("name") or "",
            disabled=bool(row.get("disabled")),
        )
        for row in rows
    ]


@router.get("/model-gateway/parent-keys", response_model=list[ParentKeyItem])
async def list_parent_keys(user: User = Depends(get_current_user)) -> list[ParentKeyItem]:
    """Keys the caller may use as the parent of an editor-scoped key copy.

    Unions the user's own runtime keys with the system keys reachable through
    group bindings. Key values are masked; the editor create dialog only needs
    the id + a displayable name + source to populate the parent selector.
    """
    from db import PostgresClient

    seen: set[int] = set()
    out: list[ParentKeyItem] = []
    runtime_rows = await PostgresClient.list_api_keys_by_user(str(user.id))
    for row in runtime_rows:
        kid = int(row.get("id") or 0)
        if not kid or kid in seen:
            continue
        seen.add(kid)
        out.append(ParentKeyItem(
            id=kid,
            key_masked=_mask_key(row.get("key") or ""),
            name=row.get("name") or "",
            source="self",
            disabled=bool(row.get("disabled")),
            editor_provider_whitelist=list(row.get("editor_provider_whitelist") or []),
            editor_provider_blacklist=list(row.get("editor_provider_blacklist") or []),
        ))
    system_rows = await resolve_system_api_key_for_user(str(user.id))
    for row in system_rows:
        kid = int(row.get("id") or 0)
        if not kid or kid in seen:
            continue
        seen.add(kid)
        out.append(ParentKeyItem(
            id=kid,
            key_masked=_mask_key(row.get("key") or ""),
            name=row.get("name") or "",
            source="platform",
            disabled=bool(row.get("disabled")),
            editor_provider_whitelist=list(row.get("editor_provider_whitelist") or []),
            editor_provider_blacklist=list(row.get("editor_provider_blacklist") or []),
        ))
    return out
