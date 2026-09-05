"""Git domain service (identities / bots).

Storage + access control for the git surface. Multi-user isolation is enforced
by ``user_id``. Secret fields (``access_token`` / ``oauth_refresh_token`` /
``token`` / ``secret_token``) are ALWAYS masked before leaving this layer — the
full secret is only ever used server-side (e.g. by the agent-compose adapter).

This layer is storage only — it never touches the model request pipeline.
Webhook ingestion (github/gitlab/gitea/gitee/codeup) depends on per-platform
signature verification + the agent-compose runtime and is layered on later.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any

from loguru import logger

from . import git_clients
from .masking import has_secret as _has_secret, mask_secret as _mask_secret
from .models_git import GitBot, GitIdentity

_PRIVILEGED_ROLES = {"admin"}

# Repository-listing pagination bounds (parity with MonkeyCode).
_DEFAULT_REPO_PAGE_SIZE = 20
_MAX_REPO_PAGE_SIZE = 100
# Repository result cache TTL: 7 days (matches MonkeyCode's go-cache expiry).
_REPO_CACHE_TTL = 7 * 24 * 3600
# Bounded prefetch timeout so CRUD never waits on a slow upstream Git API.
_PREFETCH_TIMEOUT = 60


def _normalize_repo_page_size(size: int) -> int:
    """Clamp requested repo page size: default 20, cap 100 (paginated mode)."""
    if size <= 0:
        return _DEFAULT_REPO_PAGE_SIZE
    return min(size, _MAX_REPO_PAGE_SIZE)


class _RepoCache:
    """Process-local repository result cache with a fixed TTL.

    Mirrors MonkeyCode's in-process go-cache: full lists are keyed by
    ``user:identity``; paginated results add ``:page:<keyword>:<page>:<size>``.
    ``flush`` bypasses lookup and overwrites the entry. Entries expire after
    :data:`_REPO_CACHE_TTL` seconds. Not shared across processes — a best-effort
    warm cache, never a source of truth.
    """

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, git_clients.RepositoryPage]] = {}

    @staticmethod
    def _base_key(user_id: str, identity_id: str) -> str:
        return f"{user_id}:{identity_id}"

    def key(self, user_id: str, identity_id: str, page: int, size: int, keyword: str) -> str:
        base = self._base_key(user_id, identity_id)
        if page > 0:
            return f"{base}:page:{keyword}:{page}:{size}"
        return base

    def get(self, key: str) -> git_clients.RepositoryPage | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, page = entry
        if time.time() > expires_at:
            self._store.pop(key, None)
            return None
        return page

    def set(self, key: str, page: git_clients.RepositoryPage) -> None:
        self._store[key] = (time.time() + _REPO_CACHE_TTL, page)

    def invalidate(self, user_id: str, identity_id: str) -> None:
        """Drop every cached entry (full + paginated) for an identity."""
        base = self._base_key(user_id, identity_id)
        for k in [k for k in self._store if k == base or k.startswith(base + ":")]:
            self._store.pop(k, None)


_repo_cache = _RepoCache()


def _identity_dict(i: GitIdentity) -> dict:
    """Public identity view: secrets are masked, never returned in full."""
    return {
        "id": str(i.id),
        "user_id": str(i.user_id),
        "platform": i.platform,
        "base_url": i.base_url,
        "username": i.username,
        "email": i.email,
        "installation_id": i.installation_id,
        "organization_id": i.organization_id,
        "remark": i.remark,
        # Secrets: expose only presence + masked tail.
        "access_token_masked": _mask_secret(i.access_token),
        "has_access_token": _has_secret(i.access_token),
        "has_oauth_refresh_token": _has_secret(i.oauth_refresh_token),
        "oauth_expires_at": i.oauth_expires_at.isoformat() if i.oauth_expires_at else None,
        "created_at": i.created_at.isoformat() if i.created_at else None,
        "updated_at": i.updated_at.isoformat() if i.updated_at else None,
    }


def _bot_dict(b: GitBot) -> dict:
    """Public bot view: token/secret_token masked."""
    return {
        "id": str(b.id),
        "user_id": str(b.user_id),
        "name": b.name,
        "host_id": b.host_id,
        "platform": b.platform,
        "token_masked": _mask_secret(b.token),
        "has_token": _has_secret(b.token),
        "has_secret_token": _has_secret(b.secret_token),
        "created_at": b.created_at.isoformat() if b.created_at else None,
    }


def _maybe_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


class GitService:
    """Git identities + bots with per-user isolation and secret masking."""

    def _is_privileged(self, role: str | None) -> bool:
        return (role or "") in _PRIVILEGED_ROLES

    # ---- Git identities ---------------------------------------------------
    async def list_identities(self, user_id: str, *, role: str | None = None) -> list[dict]:
        query = (
            GitIdentity.all()
            if self._is_privileged(role)
            else GitIdentity.filter(user_id=uuid.UUID(user_id))
        )
        rows = await query.order_by("-created_at")
        return [_identity_dict(r) for r in rows]

    async def _owned_identity(
        self, user_id: str, identity_id: str, role: str | None
    ) -> GitIdentity | None:
        iid = _maybe_uuid(identity_id)
        if iid is None:
            return None
        identity = await GitIdentity.get_or_none(id=iid)
        if identity is None:
            return None
        if not self._is_privileged(role) and str(identity.user_id) != str(user_id):
            return None
        return identity

    async def get_identity(
        self,
        user_id: str,
        identity_id: str,
        *,
        role: str | None = None,
        flush: bool = False,
        page: int = 0,
        size: int = 0,
        keyword: str = "",
    ) -> dict | None:
        """Load one identity (masked), appending its authorized repositories.

        When ``page > 0`` the repository list is server-side paginated and the
        response carries ``repo_page_info`` ({total_count, has_next_page}); when
        ``page == 0`` the full list is returned with no ``repo_page_info`` (parity
        with MonkeyCode). Upstream failures are logged (without secrets) and the
        masked identity is returned with an empty repository list rather than
        failing the whole request. Returns ``None`` only when the identity is
        absent or inaccessible (→ 404 at the route).
        """
        identity = await self._owned_identity(user_id, identity_id, role)
        if identity is None:
            return None
        result = _identity_dict(identity)
        if page > 0:
            size = _normalize_repo_page_size(size)
        repo_page = await self._fetch_repositories(
            identity, flush=flush, page=page, size=size, keyword=keyword
        )
        result["authorized_repositories"] = [r.to_dict() for r in repo_page.repositories]
        if repo_page.page_info is not None:
            result["repo_page_info"] = repo_page.page_info
        return result

    async def _fetch_repositories(
        self,
        identity: GitIdentity,
        *,
        flush: bool,
        page: int,
        size: int,
        keyword: str,
    ) -> git_clients.RepositoryPage:
        """Fetch (and cache) repositories for an identity, swallowing failures.

        Returns an empty page on unsupported platform, missing token, or any
        upstream error — the caller still returns the masked identity. The token
        is used only to authorize the upstream call and is never logged.
        """
        platform = (identity.platform or "").lower()
        if not git_clients.supports_platform(platform):
            return git_clients.RepositoryPage(repositories=[])
        token = identity.access_token or ""
        if not token:
            return git_clients.RepositoryPage(repositories=[])

        cache_key = _repo_cache.key(str(identity.user_id), str(identity.id), page, size, keyword)
        if not flush:
            cached = _repo_cache.get(cache_key)
            if cached is not None:
                return cached

        opts = git_clients.RepositoryOptions(
            token=token,
            base_url=identity.base_url or "",
            organization_id=identity.organization_id or "",
            installation_id=int(identity.installation_id or 0),
            is_oauth=bool(identity.oauth_refresh_token),
            page=page,
            size=size,
            keyword=keyword,
        )
        try:
            repo_page = await git_clients.fetch_repositories(platform, opts)
        except git_clients.GitClientError as exc:
            # Never include the token; identify by platform + identity id only.
            logger.warning(
                "[monkeycode-compat] list repositories failed: platform={} identity_id={} error={}",
                platform,
                identity.id,
                exc,
            )
            return git_clients.RepositoryPage(repositories=[])
        _repo_cache.set(cache_key, repo_page)
        return repo_page

    def _prefetch_repositories(self, identity: GitIdentity) -> None:
        """Warm the full-list cache in the background (bounded, best-effort).

        Mirrors MonkeyCode's post-add/update prefetch: never blocks CRUD, never
        raises into the caller. A missing event loop (sync call site) is a no-op.
        """
        async def _run() -> None:
            try:
                await asyncio.wait_for(
                    self._fetch_repositories(identity, flush=True, page=0, size=0, keyword=""),
                    timeout=_PREFETCH_TIMEOUT,
                )
            except Exception:  # noqa: BLE001 — prefetch is strictly best-effort
                logger.debug(
                    "[monkeycode-compat] repo prefetch skipped: identity_id={}", identity.id
                )

        try:
            asyncio.get_running_loop().create_task(_run())
        except RuntimeError:
            pass

    # ---- Repository creation ----------------------------------------------
    async def create_repository(
        self,
        user_id: str,
        identity_id: str,
        *,
        name: str,
        description: str = "",
        private: bool = True,
        owner: str = "",
        role: str | None = None,
    ) -> dict:
        """Create a remote repository using the given identity's credentials.

        The identity must be owned by (or, for privileged roles, visible to) the
        caller. On success the repo-list cache for that identity is invalidated so
        the freshly created repo shows up on the next list call. The access token
        is never returned; the response mirrors ``AuthRepository.to_dict`` plus a
        ``web_url``. Raises ``ValueError`` for ownership/platform issues and lets
        :class:`git_clients.GitClientError` propagate for upstream failures.
        """
        identity = await self._owned_identity(user_id, identity_id, role)
        if identity is None:
            raise ValueError("identity_not_found")
        platform = (identity.platform or "").lower()
        token = identity.access_token or ""
        if not token:
            raise ValueError("identity_missing_token")
        opts = git_clients.CreateRepoOptions(
            token=token,
            base_url=identity.base_url or "",
            is_oauth=bool(identity.oauth_refresh_token),
            name=(name or "").strip(),
            description=description or "",
            private=private,
            owner=(owner or "").strip(),
        )
        created = await git_clients.create_repository(platform, opts)
        # The new repo must appear in subsequent list calls; drop the cached page.
        _repo_cache.invalidate(str(identity.user_id), str(identity.id))
        return created.to_dict()

    # ---- Read operations (branches / tree / blob) -------------------------
    @staticmethod
    def _build_repo_options(identity: GitIdentity) -> git_clients.RepositoryOptions:
        """Build upstream request options from a stored identity's credentials."""
        return git_clients.RepositoryOptions(
            token=identity.access_token or "",
            base_url=identity.base_url or "",
            organization_id=identity.organization_id or "",
            installation_id=int(identity.installation_id or 0),
            is_oauth=bool(identity.oauth_refresh_token),
        )

    async def load_identity_for_read(self, identity_id: str) -> GitIdentity | None:
        """Load an identity by id for a server-side read (branches/tree/blob).

        No ownership check here: the caller (e.g. ``project_service``) has already
        authorized access via the project, and the identity belongs to the
        project owner rather than the requesting collaborator. Returns ``None``
        when the id is malformed or absent.
        """
        iid = _maybe_uuid(identity_id)
        if iid is None:
            return None
        return await GitIdentity.get_or_none(id=iid)

    async def list_branches(
        self, user_id: str, identity_id: str, repo_full_name: str, *, role: str | None = None
    ) -> list[dict] | None:
        """List branches for ``repo_full_name`` under an owned identity.

        Ownership is enforced via :meth:`_owned_identity`. Missing credentials,
        unsupported platform, or any upstream error degrade to an empty list
        (never raises, never logs the token). Returns ``None`` only when the
        identity is absent or inaccessible (→ 404 at the route).
        """
        identity = await self._owned_identity(user_id, identity_id, role)
        if identity is None:
            return None
        platform = (identity.platform or "").lower()
        if not identity.access_token or not repo_full_name:
            return []
        opts = self._build_repo_options(identity)
        try:
            branches = await git_clients.fetch_branches(platform, repo_full_name, opts)
        except git_clients.GitClientError as exc:
            logger.warning(
                "[monkeycode-compat] list branches failed: platform={} identity_id={} error={}",
                platform,
                identity.id,
                exc,
            )
            return []
        return [b.to_dict() for b in branches]

    async def add_identity(self, user_id: str, req: dict) -> dict:
        platform = (req.get("platform") or "").strip()
        if not platform:
            raise ValueError("platform_required")
        identity = await GitIdentity.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            platform=platform,
            base_url=req.get("base_url") or None,
            access_token=req.get("access_token") or None,
            username=req.get("username") or None,
            email=req.get("email") or None,
            installation_id=req.get("installation_id") or None,
            organization_id=req.get("organization_id") or None,
            remark=req.get("remark") or None,
            oauth_refresh_token=req.get("oauth_refresh_token") or None,
        )
        self._prefetch_repositories(identity)
        return _identity_dict(identity)

    async def update_identity(
        self, user_id: str, identity_id: str, req: dict, *, role: str | None = None
    ) -> bool:
        identity = await self._owned_identity(user_id, identity_id, role)
        if identity is None:
            return False
        changed: list[str] = []
        for field in ("base_url", "username", "email", "organization_id", "remark"):
            if field in req:
                setattr(identity, field, req[field])
                changed.append(field)
        # Secrets are only updated when a non-empty value is explicitly provided,
        # so a masked round-trip from the client never wipes the stored secret.
        if req.get("access_token"):
            identity.access_token = req["access_token"]
            changed.append("access_token")
        if req.get("oauth_refresh_token"):
            identity.oauth_refresh_token = req["oauth_refresh_token"]
            changed.append("oauth_refresh_token")
        if not changed:
            return True
        changed.append("updated_at")
        await identity.save(update_fields=changed)
        # Credentials/base_url/org may have changed → drop stale cached repos and
        # re-warm the cache in the background (matches MonkeyCode's Update).
        _repo_cache.invalidate(str(identity.user_id), str(identity.id))
        self._prefetch_repositories(identity)
        return True

    async def delete_identity(self, user_id: str, identity_id: str, *, role: str | None = None) -> bool:
        identity = await self._owned_identity(user_id, identity_id, role)
        if identity is None:
            return False
        uid, iid = str(identity.user_id), str(identity.id)
        await identity.delete()
        _repo_cache.invalidate(uid, iid)
        return True

    # ---- Git bots ---------------------------------------------------------
    async def list_bots(self, user_id: str, *, role: str | None = None) -> list[dict]:
        query = (
            GitBot.all()
            if self._is_privileged(role)
            else GitBot.filter(user_id=uuid.UUID(user_id))
        )
        rows = await query.order_by("-created_at")
        return [_bot_dict(r) for r in rows]

    async def _owned_bot(self, user_id: str, bot_id: str, role: str | None) -> GitBot | None:
        bid = _maybe_uuid(bot_id)
        if bid is None:
            return None
        bot = await GitBot.get_or_none(id=bid)
        if bot is None:
            return None
        if not self._is_privileged(role) and str(bot.user_id) != str(user_id):
            return None
        return bot

    async def create_bot(self, user_id: str, req: dict) -> dict:
        platform = (req.get("platform") or "").strip()
        host_id = (req.get("host_id") or "").strip()
        if not platform:
            raise ValueError("platform_required")
        if not host_id:
            raise ValueError("host_id_required")
        bot = await GitBot.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            name=req.get("name") or None,
            host_id=host_id,
            token=req.get("token") or None,
            secret_token=req.get("secret_token") or None,
            platform=platform,
        )
        return _bot_dict(bot)

    async def update_bot(
        self, user_id: str, bot_id: str, req: dict, *, role: str | None = None
    ) -> bool:
        bot = await self._owned_bot(user_id, bot_id, role)
        if bot is None:
            return False
        changed: list[str] = []
        for field in ("name", "host_id"):
            if field in req:
                setattr(bot, field, req[field])
                changed.append(field)
        if req.get("token"):
            bot.token = req["token"]
            changed.append("token")
        if req.get("secret_token"):
            bot.secret_token = req["secret_token"]
            changed.append("secret_token")
        if not changed:
            return True
        await bot.save(update_fields=changed)
        return True

    async def delete_bot(self, user_id: str, bot_id: str, *, role: str | None = None) -> bool:
        bot = await self._owned_bot(user_id, bot_id, role)
        if bot is None:
            return False
        await bot.delete()
        return True


git_service = GitService()
