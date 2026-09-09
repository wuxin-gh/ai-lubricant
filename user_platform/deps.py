"""FastAPI dependencies for C-side user authentication.

These dependencies resolve the current platform user for the team-admin surface
(``/api/v1/teams/*``). Two auth channels are accepted so
the management console works under both entry points:

* Primary — the independent Redis-backed C-side session cookie
  (``ai_lubricant_session``); the caller is whoever the session says.
* Emergency — the admin Bearer token issued by ``/admin/login`` (single
  password). When no C-side session is present, a valid admin token authorizes
  the request as an administrator. This mirrors the guiding rule: an endpoint
  gates on *whether the caller is authorized*, not on *who they are*, so the
  same management pages work whether the operator arrived via an admin C-side
  user or the emergency password.

Neither channel touches the model request pipeline.
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from fastapi import Depends, HTTPException, Request

from .models import User
from .session import USER_SESSION_COOKIE, session_store

# Deterministic identity for the emergency admin (admin-token channel with no
# real admin user yet). A fixed UUID keeps its team membership stable across
# requests instead of spawning a new principal each call.
_EMERGENCY_ADMIN_ID = uuid.UUID("00000000-0000-0000-0000-00000000adad")


def client_meta(request: Request) -> tuple[str | None, str | None]:
    """(source_ip, user_agent) for an audit row.

    ``source_ip`` prefers the first ``X-Forwarded-For`` hop (real client behind
    a proxy), falling back to the direct peer. Both may be ``None``.
    """
    forwarded = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    ip = forwarded or (request.client.host if request.client else None)
    return ip, request.headers.get("user-agent")


async def _valid_admin_token(request: Request) -> bool:
    """True when the request carries a valid emergency admin Bearer token.

    Reuses ``admin._get_admin_session`` so the emergency password channel and
    the ``/admin/*`` surface share one token store. Any import/lookup failure
    is treated as "no token" — never raises.
    """
    authz = request.headers.get("authorization") or ""
    if not authz.startswith("Bearer "):
        return False
    token = authz[7:].strip()
    if not token:
        return False
    try:
        from admin import _get_admin_session

        session = await _get_admin_session(token)
    except Exception:
        return False
    return bool(session) and time.time() <= float(session.get("expires_at") or 0)


async def _ensure_emergency_admin() -> User:
    """Return (creating if needed) the deterministic emergency-admin user.

    Prefer an existing real admin so the emergency token maps onto the operator
    that C-side login would also resolve. Only when no admin exists at all do we
    materialize the fixed-id placeholder, so the "用户与权限" page always has at
    least one manageable administrator row.
    """
    existing = await User.filter(role="admin", is_deleted=False).first()
    if existing is not None:
        return existing
    user = await User.get_or_none(id=_EMERGENCY_ADMIN_ID)
    if user is None:
        user = await User.create(
            id=_EMERGENCY_ADMIN_ID,
            name="管理员",
            email=None,
            role="admin",
            status="active",
        )
    return user


async def get_current_user_id(request: Request) -> str:
    """Resolve the current user id: C-side session, else emergency admin token."""
    cookie = request.cookies.get(USER_SESSION_COOKIE)
    data = await session_store.get(USER_SESSION_COOKIE, cookie or "")
    if data is not None:
        return data.uid
    if await _valid_admin_token(request):
        user = await _ensure_emergency_admin()
        return str(user.id)
    raise HTTPException(status_code=401, detail="未登录")


async def get_optional_user_id(request: Request) -> str | None:
    """Resolve the current user id if present, otherwise None (no 401)."""
    cookie = request.cookies.get(USER_SESSION_COOKIE)
    if cookie:
        data = await session_store.get(USER_SESSION_COOKIE, cookie)
        if data is not None:
            return data.uid
    if await _valid_admin_token(request):
        user = await _ensure_emergency_admin()
        return str(user.id)
    return None


async def get_current_user(user_id: str = Depends(get_current_user_id)) -> User:
    """Load the current user row, 401 if the user no longer exists/active."""
    user = await User.get_or_none(id=user_id)
    if user is None or user.is_deleted:
        raise HTTPException(status_code=401, detail="用户不存在")
    if user.is_blocked or user.status != "active":
        raise HTTPException(status_code=403, detail="账号已停用")
    return user


async def resolve_team_id(user_id: str) -> str:
    """Resolve the team the user belongs to, creating a default one if none.

    The team-admin surface (``/api/v1/teams/*``) is always scoped to a single
    team. A user with no membership yet gets a default team lazily so the
    console never 500s on first use. Returns the team id as a string.
    """
    from .models import Team, TeamMember

    member = await TeamMember.filter(user_id=user_id).first()
    if member is not None:
        return str(member.team_id)
    # No membership yet: create a default team and enroll the user as its admin.
    team = await Team.create(name="默认团队", member_limit=0)
    await TeamMember.create(team_id=team.id, user_id=user_id, role="admin")
    return str(team.id)


async def get_current_team_id(user_id: str = Depends(get_current_user_id)) -> str:
    """FastAPI dependency: current user's team id (lazily created if missing)."""
    return await resolve_team_id(user_id)


async def audit_user_action(
    request: Request,
    user: User,
    operation: str,
    *,
    request_body: Any = None,
    response: Any = None,
) -> None:
    """Record one C-side user write action into the shared audit stream.

    Thin wrapper over ``team_users_service.record_audit``: resolves the caller's
    team (``mc_audits`` is team-scoped) and client IP/UA, then writes one row.
    The whole thing is best-effort — an audit failure (team unresolvable, DB
    down, …) must never break the business action it records, so every error is
    swallowed. Secret fields in ``request_body``/``response`` are masked by
    ``record_audit`` itself.
    """
    try:
        team_id = await resolve_team_id(str(user.id))
        ip, ua = client_meta(request)
        from .team_users_service import team_users_service

        await team_users_service.record_audit(
            team_id, str(user.id), operation,
            request=request_body, response=response,
            source_ip=ip, user_agent=ua,
        )
    except Exception:  # noqa: BLE001 - audit must never break the business action
        pass
