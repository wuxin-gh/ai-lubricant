"""C-side user authentication service.

Password login + session issuance/revocation for the multi-user platform
surface. Fully independent from the admin-token auth used by ``/admin/*``.

The service is intentionally thin: it validates credentials against the
``mc_users`` table (bcrypt) and delegates session storage to the Redis-backed
``session_store``. It never touches the model request pipeline.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from loguru import logger

from .crypto import hash_password, verify_password
from .models import User, UserIdentity
from .session import SessionStore, USER_SESSION_COOKIE, session_store
from .validation import validate_new_credentials


@dataclass(frozen=True)
class AuthedUser:
    id: str
    name: str
    email: str | None
    role: str
    status: str


def _to_authed(user: User) -> AuthedUser:
    return AuthedUser(
        id=str(user.id),
        name=user.name,
        email=user.email,
        role=user.role,
        status=user.status,
    )


class AuthService:
    """Password login + session lifecycle for C-side users."""

    def __init__(self, store: SessionStore | None = None) -> None:
        self._store = store or session_store

    async def create_user(
        self,
        *,
        email: str,
        password: str,
        name: str | None = None,
        role: str = "individual",
        status: str = "active",
    ) -> AuthedUser:
        """Create a C-side platform user with a bcrypt-hashed password.

        Raises ValueError with a stable reason code on invalid input or when the
        email is already taken. Never touches the model request pipeline.
        """
        email = validate_new_credentials(email, password)
        existing = await User.filter(email=email, is_deleted=False).first()
        if existing is not None:
            raise ValueError("email_taken")
        user = await User.create(
            id=uuid.uuid4(),
            name=(name or email.split("@", 1)[0]).strip() or email,
            email=email,
            password=hash_password(password),
            role=role,
            status=status,
        )
        logger.info("[monkeycode-compat] user created id={} email={}", user.id, email)
        return _to_authed(user)

    async def password_login(self, email: str, password: str) -> tuple[AuthedUser, str]:
        """Validate credentials; return (user, session_cookie) on success."""
        user = await User.filter(email=email, is_deleted=False).first()
        if user is None or not user.password:
            raise ValueError("invalid_credentials")
        if user.is_blocked or user.status != "active":
            raise ValueError("user_disabled")
        if not verify_password(password, user.password):
            raise ValueError("invalid_credentials")
        cookie = await self.issue_session(user)
        return _to_authed(user), cookie

    async def issue_session(self, user: User) -> str:
        """Create a session for a user, returning the generated cookie value.

        Shared by password login and OIDC callback so session attributes stay
        consistent. Never touches the model request pipeline.

        The payload snapshots the fields ``resolve_session`` needs to authorize
        without re-querying PG on every request — role/status/blocked/deleted
        are the authoritative gate at request time. They are invalidated by
        ``delete_user_sessions`` when an admin changes them, so a stale payload
        cannot survive a role/block/delete change past the next request.
        """
        return await self._store.save(
            USER_SESSION_COOKIE,
            str(user.id),
            {
                "uid": str(user.id),
                "name": user.name,
                "email": user.email,
                "role": user.role,
                "status": user.status,
                "is_blocked": bool(user.is_blocked),
                "is_deleted": bool(user.is_deleted),
            },
        )

    async def resolve_session(self, cookie: str) -> AuthedUser | None:
        """Resolve the current user from a session cookie, or None.

        Authoritative source is the Redis session payload (snapshot at login),
        not a per-request PG lookup. The payload carries role/status/blocked/
        deleted; an admin mutating any of those calls ``delete_user_sessions``
        so the old cookie stops authorizing immediately — the user re-logs in
        to mint a payload with the new state. No PG trip on the hot path.
        """
        data = await self._store.get(USER_SESSION_COOKIE, cookie)
        if data is None:
            return None
        payload = data.payload or {}
        if not payload:
            return None
        if payload.get("is_deleted") or payload.get("is_blocked") or payload.get("status") != "active":
            return None
        try:
            return AuthedUser(
                id=str(data.uid),
                name=payload.get("name") or "",
                email=payload.get("email"),
                role=str(payload.get("role") or "individual"),
                status=str(payload.get("status") or "active"),
            )
        except (TypeError, ValueError):
            return None

    async def logout(self, uid: str, cookie: str) -> None:
        await self._store.delete(USER_SESSION_COOKIE, uid, cookie)


auth_service = AuthService()
