"""Team users / groups / audit domain service (pure CRUD).

Backs the MonkeyCode team-admin surface (``/api/v1/teams/*``) for platform
users, team groups and the audit log. Everything here is storage only — it
never touches the model request pipeline or any VM/orchestration runtime.

All read views return plain Python dict/list; the frontend endpoint map wraps
them in the Go ``web.Resp`` ``{code, message, data}`` envelope, so this layer
never adds an envelope itself. Timestamps are emitted as unix seconds (int).
"""
from __future__ import annotations

import copy
import json
import logging
import secrets
import string
import uuid
from typing import Any

from .crypto import hash_password, verify_password
from .models import (
    Team,
    TeamGroup,
    TeamGroupMember,
    TeamMember,
    User,
)
from .models_team_admin import Audit, TeamSkill
from .session import USER_SESSION_COOKIE, session_store
from .validation import normalize_email

logger = logging.getLogger("monkeycode_compat.audit")

# Password rules for generated + user-chosen passwords.
_GENERATED_PASSWORD_LEN = 12
_MIN_CHANGE_PASSWORD_LEN = 8
_MAX_CHANGE_PASSWORD_LEN = 32
_PASSWORD_ALPHABET = string.ascii_letters + string.digits

# Plaintext-credential keys whose *values* must never land in the audit log.
# The user asked to persist everything for recoverability, but these are
# secrets — storing them in a queryable audit table is a credential leak and
# does nothing for "undo a mistaken delete". Beyond passwords, C-side write
# operations carry Git tokens, MCP/channel secrets and API keys in their
# request bodies; redact all of them regardless of nesting depth.
_SECRET_KEYS = frozenset({
    "password", "new_password", "current_password",
    "access_token", "token", "secret", "secret_token",
    "oauth_refresh_token", "api_key",
    # ``headers`` (git identity / MCP upstream) carry auth under arbitrary key
    # names (``Authorization`` etc.), so redact the whole value, not by key.
    "headers",
})


def _mask_secrets(value: Any) -> Any:
    """Deep-copy ``value``, replacing any plaintext-credential value with ``***``.

    Recurses through dicts/lists. A key in :data:`_SECRET_KEYS` has its value
    redacted regardless of nesting; every other field is preserved verbatim.
    Pure function — never mutates the input.
    """
    if isinstance(value, dict):
        return {
            k: ("***" if k in _SECRET_KEYS else _mask_secrets(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_mask_secrets(item) for item in value]
    return copy.deepcopy(value)


# Keys, in priority order, that best identify *what* an action acted on. Used to
# derive the audit "target" (具体值) shown in the table without opening details:
# a human-friendly name/email wins over an opaque id.
_TARGET_KEYS = (
    "email", "name", "username", "model_id", "group_id",
    "upstream_id", "node_id", "user_id",
)


def _extract_target(request_obj: Any) -> str | None:
    """Pull a representative object identifier out of an audit request dict.

    Returns the first present, non-empty value among :data:`_TARGET_KEYS`
    (e.g. the email of a created user, the name of a deleted provider). ``None``
    when nothing recognizable is present — the caller then leaves target blank.
    """
    if not isinstance(request_obj, dict):
        return None
    for key in _TARGET_KEYS:
        val = request_obj.get(key)
        if val:
            return str(val)
    return None


def _loads_or_none(text: Any) -> Any:
    """Parse JSON text to an object, tolerating already-parsed / bad / empty input."""
    if text is None or isinstance(text, (dict, list)):
        return text
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return None


def _oplog_target(row: dict) -> str | None:
    """Best "具体值" for an admin ``operation_logs`` row.

    Channel/account operations should read as the *channel name*: provider ops
    already store it in ``target_name``, but account ops store the account's
    username there and keep the channel name under a ``provider`` key inside
    ``old_data``/``new_data``. So prefer that ``provider`` name; when the row's
    own ``target_name`` differs (an account username), append it as
    ``渠道 / 账号`` so both are visible. Falls back to ``target_name``.
    """
    target_name = row.get("target_name") or None
    provider: str | None = None
    for field in ("old_data", "new_data"):
        obj = _loads_or_none(row.get(field))
        if isinstance(obj, dict):
            candidate = obj.get("provider")
            if candidate:
                provider = str(candidate)
                break
    if provider:
        if target_name and target_name != provider:
            return f"{provider} / {target_name}"
        return provider
    return target_name


# ── serialization helpers ────────────────────────────────────────────────────


def _unix(dt: Any) -> int | None:
    """Convert a datetime to unix seconds (int), or None when absent."""
    if dt is None:
        return None
    return int(dt.timestamp())


def _domain_user(user: User, *, hide_email: bool = False) -> dict:
    """DomainUser shape shared across the team surface.

    hide_email=True 时 email 置空，避免跨租户 PII 泄露。
    """
    return {
        "id": str(user.id),
        "name": user.name,
        "email": None if hide_email else user.email,
        "avatar_url": user.avatar_url,
        "role": user.role,
        "status": user.status,
        "has_password": bool(user.password),
        "is_blocked": bool(user.is_blocked),
        "team": None,
        "identities": [],
    }


def _generate_password() -> str:
    """Generate a 12-char random password (letters + digits)."""
    return "".join(secrets.choice(_PASSWORD_ALPHABET) for _ in range(_GENERATED_PASSWORD_LEN))


def _maybe_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


class TeamUsersService:
    """Team-scoped users / groups / audit CRUD."""

    # ── users ────────────────────────────────────────────────────────────────

    async def list_users(self, team_id: str, *, role: str | None = None) -> dict:
        """Members of a team (optionally filtered by member role)."""
        query = TeamMember.filter(team_id=uuid.UUID(team_id))
        if role:
            query = query.filter(role=role)
        members = await query.order_by("created_at")

        user_ids = [m.user_id for m in members]
        users_by_id: dict[str, User] = {}
        if user_ids:
            rows = await User.filter(id__in=user_ids, is_deleted=False)
            users_by_id = {str(r.id): r for r in rows}

        team = await Team.get_or_none(id=uuid.UUID(team_id))
        member_limit = int(team.member_limit) if team else 0

        result_members = []
        for m in members:
            user = users_by_id.get(str(m.user_id))
            if user is None:
                continue
            result_members.append(
                {
                    "created_at": _unix(m.created_at),
                    "last_active_at": _unix(m.last_active_at),
                    "role": m.role,
                    "user": _domain_user(user),
                }
            )
        return {"member_limit": member_limit, "members": result_members}

    async def status(self, user: User, team_id: str) -> dict:
        """Current user + their team summary."""
        team = await Team.get_or_none(id=uuid.UUID(team_id))
        team_view = {"id": str(team.id), "name": team.name} if team else None
        return {"team": team_view, "user": _domain_user(user)}

    async def create_users_with_password(
        self, team_id: str, emails: list[str], group_id: str | None = None
    ) -> dict:
        """Create users with generated passwords and enroll them into the team.

        Existing (non-deleted) users are reused rather than recreated; a fresh
        password is only generated for newly created rows. Members are added to
        the team (idempotently) and, when ``group_id`` is given, to that group.
        """
        team_uuid = uuid.UUID(team_id)
        group_uuid = _maybe_uuid(group_id)

        created_or_reused: list[User] = []
        passwords: list[dict] = []
        for raw_email in emails:
            email = normalize_email(raw_email)
            if not email:
                continue
            existing = await User.filter(email=email, is_deleted=False).first()
            if existing is not None:
                user = existing
            else:
                plain = _generate_password()
                user = await User.create(
                    id=uuid.uuid4(),
                    name=email.split("@", 1)[0] or email,
                    email=email,
                    password=hash_password(plain),
                    role="user",
                    status="active",
                )
                passwords.append({"email": email, "password": plain})

            await TeamMember.get_or_create(
                team_id=team_uuid,
                user_id=user.id,
                defaults={"role": "user"},
            )
            if group_uuid is not None:
                await TeamGroupMember.get_or_create(
                    group_id=group_uuid,
                    user_id=user.id,
                )
            created_or_reused.append(user)

        team = await Team.get_or_none(id=team_uuid)
        team_view = {"id": str(team.id), "name": team.name} if team else None
        users = [{"team": team_view, "user": _domain_user(u)} for u in created_or_reused]
        return {"users": users, "passwords": passwords}

    async def _first_admin_id(self) -> str | None:
        """The oldest (by creation) active admin — the one that must never be
        demoted or deleted, so the console can't lock itself out. Returns the
        user id string, or None when there are no admins yet."""
        first = (
            await User.filter(role="admin", is_deleted=False)
            .order_by("created_at")
            .first()
        )
        return str(first.id) if first is not None else None

    async def list_all_users(self, team_id: str) -> dict:
        """本团队成员列表（供「用户与权限」页），跨团队不泄露。

        历史查全平台 User.filter(is_deleted=False) → 跨租户 PII 泄露；改为只取该 team
        的成员（TeamMember），并隐藏 email。
        """
        team_uuid = uuid.UUID(team_id)
        members = await TeamMember.filter(team_id=team_uuid)
        member_by_uid = {str(m.user_id): m for m in members}

        rows = (
            await User.filter(id__in=list(member_by_uid.keys()), is_deleted=False)
            .order_by("created_at")
        ) if member_by_uid else []
        first_admin_id = await self._first_admin_id()

        team = await Team.get_or_none(id=team_uuid)
        member_limit = int(team.member_limit) if team else 0

        users = []
        for u in rows:
            m = member_by_uid.get(str(u.id))
            users.append(
                {
                    "created_at": _unix(m.created_at) if m else _unix(u.created_at),
                    "last_active_at": _unix(m.last_active_at) if m else None,
                    "is_admin": u.role == "admin",
                    "is_first_admin": str(u.id) == first_admin_id,
                    "user": _domain_user(u, hide_email=True),
                }
            )
        return {"member_limit": member_limit, "users": users}

    async def set_admin(self, team_id: str, user_id: str, is_admin: bool) -> dict:
        """Flip a user's platform role between ``admin`` and ``user``.

        Guard: the first admin can never be demoted, so the management console
        always retains at least one administrator (can't lock itself out).
        Raises ValueError('user_not_found' | 'protected_first_admin').
        """
        uid = _maybe_uuid(user_id)
        user = await User.get_or_none(id=uid) if uid is not None else None
        if user is None or user.is_deleted:
            raise ValueError("user_not_found")
        if not is_admin and str(user.id) == await self._first_admin_id():
            raise ValueError("protected_first_admin")
        user.role = "admin" if is_admin else "user"
        await user.save(update_fields=["role", "updated_at"])
        # Keep team-membership role loosely aligned so team-scoped views agree.
        await TeamMember.filter(team_id=uuid.UUID(team_id), user_id=user.id).update(
            role="admin" if is_admin else "user"
        )
        # role lives in the session payload as a login-time snapshot; drop all
        # of this user's sessions so the new role takes effect on next login.
        await session_store.delete_user_sessions(USER_SESSION_COOKIE, str(user.id))
        return {"is_admin": user.role == "admin"}

    async def create_user(
        self, team_id: str, email: str, name: str | None, is_admin: bool
    ) -> dict:
        """Create one platform user (admin or normal) with a generated password.

        Reuses an existing non-deleted user with the same email rather than
        failing. Returns the domain user plus the one-time generated password
        (only present for freshly created rows).
        """
        team_uuid = uuid.UUID(team_id)
        email = normalize_email(email)
        if not email:
            raise ValueError("invalid_email")
        role = "admin" if is_admin else "user"

        existing = await User.filter(email=email, is_deleted=False).first()
        password: str | None = None
        if existing is not None:
            user = existing
            if user.role != role:
                user.role = role
                await user.save(update_fields=["role", "updated_at"])
        else:
            plain = _generate_password()
            user = await User.create(
                id=uuid.uuid4(),
                name=(name or email.split("@", 1)[0]).strip() or email,
                email=email,
                password=hash_password(plain),
                role=role,
                status="active",
            )
            password = plain

        await TeamMember.get_or_create(
            team_id=team_uuid,
            user_id=user.id,
            defaults={"role": role},
        )
        team = await Team.get_or_none(id=team_uuid)
        team_view = {"id": str(team.id), "name": team.name} if team else None
        return {
            "user": {"team": team_view, "user": _domain_user(user)},
            "password": password,
        }

    async def update_user(
        self, user_id: str, *, name: str | None = None, is_blocked: bool | None = None
    ) -> dict:
        """Edit basic user fields (display name, blocked flag)."""
        uid = _maybe_uuid(user_id)
        user = await User.get_or_none(id=uid) if uid is not None else None
        if user is None or user.is_deleted:
            raise ValueError("user_not_found")
        fields: list[str] = []
        if name is not None and name.strip():
            user.name = name.strip()
            fields.append("name")
        if is_blocked is not None:
            user.is_blocked = bool(is_blocked)
            fields.append("is_blocked")
        if fields:
            fields.append("updated_at")
            await user.save(update_fields=fields)
        # is_blocked gates auth; a stale payload must not survive the change.
        if is_blocked is not None:
            await session_store.delete_user_sessions(USER_SESSION_COOKIE, str(user.id))
        return {"user": _domain_user(user)}

    async def delete_user(self, user_id: str) -> dict:
        """Soft-delete a platform user (team relationships are left intact).

        Guard: the first admin can never be deleted (lock-out protection).
        """
        uid = _maybe_uuid(user_id)
        if uid is not None:
            if str(uid) == await self._first_admin_id():
                raise ValueError("protected_first_admin")
            await User.filter(id=uid).update(is_deleted=True)
            # Soft-deleted users must stop authenticating immediately; the
            # payload's is_deleted snapshot is only refreshed on re-login,
            # which a deleted user cannot perform.
            await session_store.delete_user_sessions(USER_SESSION_COOKIE, str(uid))
        return {}

    async def change_password(
        self, user: User, current_password: str | None, new_password: str
    ) -> dict:
        """Change the current user's password after verifying the old one."""
        if not new_password or not (
            _MIN_CHANGE_PASSWORD_LEN <= len(new_password) <= _MAX_CHANGE_PASSWORD_LEN
        ):
            raise ValueError("weak_password")
        # Verify current password when the user already has one set.
        if user.password:
            if not current_password or not verify_password(current_password, user.password):
                raise ValueError("invalid_current_password")
        user.password = hash_password(new_password)
        await user.save(update_fields=["password", "updated_at"])
        return {}

    async def reset_password(self, user_id: str) -> dict:
        """Admin reset: assign a new generated password to the target user.

        不回传明文密码（避免响应/日志/审计留存 secret）；管理员如需告知用户，走
        一次性下发通道或让用户自行用「忘记密码」。仅返回被重置的用户标识。
        """
        uid = _maybe_uuid(user_id)
        user = await User.get_or_none(id=uid) if uid is not None else None
        if user is None or user.is_deleted:
            raise ValueError("user_not_found")
        plain = _generate_password()
        user.password = hash_password(plain)
        await user.save(update_fields=["password", "updated_at"])
        # 强制下线旧会话，新密码生效后用户需重新登录。
        await session_store.delete_user_sessions(USER_SESSION_COOKIE, str(user.id))
        return {"user_id": str(user.id), "email": user.email, "reset": True}

    # ── groups ────────────────────────────────────────────────────────────────

    async def _group_users(self, group_id: uuid.UUID) -> list[dict]:
        links = await TeamGroupMember.filter(group_id=group_id)
        user_ids = [link.user_id for link in links]
        if not user_ids:
            return []
        rows = await User.filter(id__in=user_ids, is_deleted=False).order_by("created_at")
        return [_domain_user(r) for r in rows]

    async def _domain_group(self, group: TeamGroup) -> dict:
        return {
            "id": str(group.id),
            "name": group.name,
            "created_at": _unix(group.created_at),
            "updated_at": _unix(group.updated_at),
            "users": await self._group_users(group.id),
        }

    async def list_groups(self, team_id: str) -> dict:
        groups = await TeamGroup.filter(
            team_id=uuid.UUID(team_id), is_deleted=False
        ).order_by("created_at")
        return {"groups": [await self._domain_group(g) for g in groups]}

    async def create_group(self, team_id: str, name: str) -> dict:
        name = (name or "").strip()
        if not name:
            raise ValueError("name_required")
        group = await TeamGroup.create(
            id=uuid.uuid4(),
            team_id=uuid.UUID(team_id),
            name=name,
        )
        return await self._domain_group(group)

    async def _owned_group(self, team_id: str, group_id: str) -> TeamGroup | None:
        gid = _maybe_uuid(group_id)
        if gid is None:
            return None
        group = await TeamGroup.get_or_none(
            id=gid, team_id=uuid.UUID(team_id), is_deleted=False
        )
        return group

    async def rename_group(self, team_id: str, group_id: str, name: str) -> dict:
        name = (name or "").strip()
        if not name:
            raise ValueError("name_required")
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise ValueError("group_not_found")
        group.name = name
        await group.save(update_fields=["name", "updated_at"])
        return await self._domain_group(group)

    async def delete_group(self, team_id: str, group_id: str) -> dict:
        group = await self._owned_group(team_id, group_id)
        if group is not None:
            group.is_deleted = True
            await group.save(update_fields=["is_deleted", "updated_at"])
            # 分组是软删（is_deleted），而 api_key_groups 在另一个库连接里、没有真
            # 外键可级联，所以授权行必须显式清掉。否则 resolve_system_api_key_for_user
            # 只按 group_id 匹配、不回查分组是否已删，已删分组的成员仍能拿到 Key。
            from db import PostgresClient

            await PostgresClient.delete_api_key_group_bindings(str(group.id))
        return {}

    async def set_group_users(self, team_id: str, group_id: str, user_ids: list[str]) -> dict:
        """Overwrite the group's membership with ``user_ids`` (delete + rebuild)."""
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise ValueError("group_not_found")
        await TeamGroupMember.filter(group_id=group.id).delete()
        seen: set[str] = set()
        for raw in user_ids or []:
            uid = _maybe_uuid(raw)
            if uid is None or str(uid) in seen:
                continue
            seen.add(str(uid))
            await TeamGroupMember.get_or_create(group_id=group.id, user_id=uid)
        users = await self._group_users(group.id)
        return {"users": users}

    # ── team skills / group bindings ─────────────────────────────────────────

    async def list_team_skills(self, team_id: str) -> list[dict]:
        rows = await TeamSkill.filter(team_id=uuid.UUID(team_id)).order_by("name")
        groups = await TeamGroup.filter(
            team_id=uuid.UUID(team_id), is_deleted=False
        )
        groups_by_id = {str(group.id): {"id": str(group.id), "name": group.name} for group in groups}
        return [
            {
                "id": str(skill.id),
                "name": skill.name,
                "description": skill.description or "",
                "tags": list(skill.tags or []),
                "content": skill.content or "",
                "source_type": skill.source_type,
                "source_label": skill.source_label or "",
                "skill_md_path": skill.skill_md_path,
                "groups": [
                    groups_by_id[group_id]
                    for group_id in [str(value) for value in (skill.group_ids or [])]
                    if group_id in groups_by_id
                ],
                "created_at": _unix(skill.created_at),
                "updated_at": _unix(skill.updated_at),
            }
            for skill in rows
        ]

    async def list_group_skills(self, team_id: str, group_id: str) -> list[dict]:
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise ValueError("group_not_found")
        rows = await TeamSkill.filter(team_id=uuid.UUID(team_id)).order_by("name")
        gid = str(group.id)
        return [
            {
                "id": str(skill.id),
                "name": skill.name,
                "description": skill.description or "",
                "tags": list(skill.tags or []),
                "source_type": skill.source_type,
                "source_label": skill.source_label or "",
                "skill_md_path": skill.skill_md_path,
                "bound": gid in [str(value) for value in (skill.group_ids or [])],
            }
            for skill in rows
        ]

    async def set_group_skills(self, team_id: str, group_id: str, skill_ids: list[str]) -> dict:
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise ValueError("group_not_found")
        selected = {str(value) for value in (skill_ids or [])}
        rows = await TeamSkill.filter(team_id=uuid.UUID(team_id))
        for skill in rows:
            groups = [str(value) for value in (skill.group_ids or [])]
            if str(skill.id) in selected:
                if str(group.id) not in groups:
                    groups.append(str(group.id))
            else:
                groups = [value for value in groups if value != str(group.id)]
            skill.group_ids = groups
            await skill.save(update_fields=["group_ids", "updated_at"])
        return {"ok": True}

    # ── audits ────────────────────────────────────────────────────────────────

    async def record_audit(
        self,
        team_id: str | None,
        user_id: str | None,
        operation: str,
        *,
        request: Any = None,
        response: Any = None,
        source_ip: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        """Write one ``mc_audits`` row for a state-changing team action.

        ``request``/``response`` are masked (:func:`_mask_secrets`) then stored
        as JSON text. The whole body is wrapped in try/except and swallows any
        error: an audit failure must never break the business action it records
        (mirrors ``admin._log_operation``).
        """
        try:
            tid = _maybe_uuid(team_id)
            if tid is None:
                return
            uid = _maybe_uuid(user_id)
            request_text = None
            if request is not None:
                request_text = json.dumps(
                    _mask_secrets(request), ensure_ascii=False, default=str
                )
            response_text = None
            if response is not None:
                response_text = json.dumps(
                    _mask_secrets(response), ensure_ascii=False, default=str
                )
            await Audit.create(
                id=uuid.uuid4(),
                team_id=tid,
                user_id=uid,
                operation=operation,
                request=request_text,
                response=response_text,
                source_ip=(source_ip or None),
                user_agent=(user_agent[:512] if user_agent else None),
            )
        except Exception:
            logger.exception("[monkeycode-compat] audit write failed: %s", operation)

    async def list_audits(
        self,
        team_id: str,
        *,
        cursor: str | None = None,
        limit: int = 20,
        operation: str | None = None,
        user_id: str | None = None,
    ) -> dict:
        """Cursor-paginated audit log (newest first).

        The cursor is the ``created_at`` unix-seconds boundary of the last item
        on the previous page; rows strictly older than it start the next page.
        ``has_next_page`` is decided by over-fetching one row.
        """
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 20
        limit = max(1, min(limit, 100))

        before_ts: int | None = None
        if cursor:
            try:
                before_ts = int(cursor)
            except (ValueError, TypeError):
                before_ts = None

        # ── team-scoped audit rows (mc_audits, Tortoise) ──────────────────────
        query = Audit.filter(team_id=uuid.UUID(team_id))
        if operation:
            query = query.filter(operation=operation)
        target_user = _maybe_uuid(user_id)
        if target_user is not None:
            query = query.filter(user_id=target_user)
        if before_ts is not None:
            try:
                from datetime import datetime, timezone

                boundary = datetime.fromtimestamp(before_ts, tz=timezone.utc)
                query = query.filter(created_at__lt=boundary)
            except (ValueError, TypeError, OSError):
                pass

        rows = await query.order_by("-created_at").limit(limit + 1)

        # Resolve the users referenced by these rows in one query.
        uids = [r.user_id for r in rows if r.user_id is not None]
        users_by_id: dict[str, User] = {}
        if uids:
            users = await User.filter(id__in=uids)
            users_by_id = {str(u.id): u for u in users}

        merged: list[dict] = []
        for r in rows:
            user = users_by_id.get(str(r.user_id)) if r.user_id is not None else None
            # ``target`` (具体值) is the object acted on, derived from the request
            # so the table is readable without opening details; request/response
            # keep the full JSON for the recovery details view.
            target = _extract_target(_loads_or_none(r.request))
            merged.append(
                {
                    "id": str(r.id),
                    "operation": r.operation,
                    "target": target,
                    "request": r.request,
                    "response": r.response,
                    "created_at": _unix(r.created_at),
                    "user": _domain_user(user) if user is not None else None,
                }
            )

        # ── admin-side operation_logs (asyncpg) merged into the same stream ───
        # api_key / provider / account / proxy / model changes already write here
        # (admin._log_operation) but had no read surface. Map each row onto the
        # DomainAudit shape and merge by time so one page shows both. The whole
        # block is best-effort: any failure leaves the mc_audits result intact.
        # ``user_id`` filtering is team-audit only (operation_logs has no user),
        # so skip admin rows when that filter is active.
        if target_user is None:
            try:
                from db import PostgresClient

                op_rows = await PostgresClient.list_operation_logs(
                    before_ts=before_ts, limit=limit + 1, action=operation
                )
                for r in op_rows:
                    merged.append(
                        {
                            # Prefix so the id never collides with mc_audits UUIDs
                            # (frontend uses it only as a React row key).
                            "id": f"oplog:{r.get('id')}",
                            # Stable action code; the frontend maps it to a
                            # localized label. ``target_name`` is the object.
                            "operation": r.get("action") or "",
                            "target": _oplog_target(r),
                            # old_data = before, new_data = after: the full diff,
                            # kept for the recovery details view.
                            "request": r.get("old_data"),
                            "response": r.get("new_data"),
                            "created_at": r.get("created_at"),
                            # Single-password admin channel: no per-user identity,
                            # so operator is shown as "管理员" (not the token).
                            "user": {"id": None, "name": "管理员", "email": None},
                        }
                    )
            except Exception:
                logger.exception("[monkeycode-compat] operation_logs merge failed")

        # ── merge + paginate the combined stream (newest first) ───────────────
        merged.sort(key=lambda a: (a.get("created_at") or 0), reverse=True)
        has_next = len(merged) > limit
        audits = merged[:limit]

        next_cursor = None
        if has_next and audits:
            next_cursor = str(audits[-1]["created_at"])
        return {
            "audits": audits,
            "page": {"cursor": next_cursor, "has_next_page": has_next},
        }


team_users_service = TeamUsersService()
