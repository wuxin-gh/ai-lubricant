"""Project-scoped webhook management service.

Registers/list/updates/deletes a repository webhook on a project's Git
platform using the project's linked :class:`GitIdentity` for credentials, and
persists the platform-side ``hook_id`` + verification secret in
:class:`ProjectWebhook`.

Access control reuses :meth:`ProjectService._get_accessible_project` (owner or
read_write may write; read_only may only read). Secrets are masked in every
returned dict via :mod:`masking`. The callback URL base is taken from the
``webhook_public_origin`` setting, falling back to the request origin.
"""
from __future__ import annotations

import secrets
import uuid
from typing import Any

from fastapi import Request
from loguru import logger

from . import config, git_clients
from .git_service import git_service
from .masking import has_secret as _has_secret, mask_secret as _mask_secret
from .models_webhook import ProjectWebhook
from .project_service import _derive_full_name, project_service

# Events the UI may pick from. Per-platform mapping happens in git_clients
# (GitHub uses literal event names; GitLab/Gitee map to boolean flags).
_DEFAULT_EVENTS = ["push"]


def _webhook_dict(row: ProjectWebhook) -> dict:
    return {
        "id": str(row.id),
        "project_id": str(row.project_id),
        "user_id": str(row.user_id),
        "platform": row.platform,
        "full_name": row.full_name,
        "hook_id": row.hook_id,
        "callback_url": row.callback_url,
        "secret_masked": _mask_secret(row.secret),
        "has_secret": _has_secret(row.secret),
        "events": list(row.events or []),
        "active": bool(row.active),
        "review_executor": row.review_executor or "claude_delegate",
        "review_framework": row.review_framework or "open_code_review_delegate",
        "review_provider": row.review_provider or None,
        "review_node_ids": list(row.review_node_ids or []),
        "review_skill_config": list(row.review_skill_config or []),
        "review_mcp_config": list(row.review_mcp_config or []),
        "review_plugin_config": list(row.review_plugin_config or []),
        "review_editor_ids": list(row.review_editor_ids or []),
        "review_auto": bool(row.review_auto),
        "review_enabled": bool(row.review_enabled),
        "review_model_id": row.review_model_id,
        "review_prompt_id": row.review_prompt_id or None,
        "review_api_key_id": row.review_api_key_id,
        "review_model_limits": dict(row.review_model_limits or {}),
        "review_rate_limit": dict(row.review_rate_limit or {}),
        "review_expires_at": int(row.review_expires_at) if row.review_expires_at else None,
        "review_max_concurrency": int(config.settings.review_project_max_concurrency),
        "review_latest_only": bool(row.review_latest_only),
        "last_error": row.last_error,
        "created_at": _unix(row.created_at),
        "updated_at": _unix(row.updated_at),
    }


def _unix(dt: Any) -> int | None:
    if not dt:
        return None
    try:
        return int(dt.timestamp())
    except Exception:
        return None


def _callback_base(request: Request | None) -> str:
    """Public origin for the webhook callback URL.

    Prefers the configured ``webhook_public_origin``; otherwise derives from the
    request origin so a single-host deploy needs no extra config.
    """
    configured = (config.settings.webhook_public_origin or "").strip().rstrip("/")
    if configured:
        return configured
    if request is not None:
        return f"{request.url.scheme}://{request.url.netloc}"
    return ""


def _callback_url(project_id: str, request: Request | None) -> str:
    base = _callback_base(request)
    return f"{base}/api/v1/webhooks/projects/{project_id}"


def _generate_secret() -> str:
    return secrets.token_urlsafe(32)


def _validate_session_config(name: str, value: Any) -> list[dict]:
    """Coerce a session overlay (skills/mcps/plugins) to a list of dicts.

    Empty values are normalized to an empty list so the worker can always
    iterate. Each entry must be a dict; ``name`` is optional but the worker
    dedupes by it, so non-dict entries are rejected up front.
    """
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("review_session_config_invalid")
    items: list[dict] = []
    for entry in value:
        if not isinstance(entry, dict):
            raise ValueError("review_session_config_invalid")
        items.append(dict(entry))
    return items


# Rate-limit keys the gateway enforces per API key. Mirrors the admin API-key
# form so a review's child key can be throttled exactly like a hand-made key.
_RATE_LIMIT_KEYS = (
    "requests_per_minute", "requests_per_5h", "requests_per_day", "requests_per_week",
    "tokens_per_minute", "tokens_per_day", "tokens_per_week",
    "concurrent_requests", "max_ips", "ip_window_seconds",
)


def _normalize_rate_limit(value: Any) -> dict:
    """Keep only known positive-integer rate-limit entries; drop the rest.

    Same shape as ``api_keys.rate_limit``: <=0/absent means "no limit for that
    metric", so those keys are simply omitted instead of stored as zeros.
    """
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("review_rate_limit_invalid")
    result: dict[str, int] = {}
    for key in _RATE_LIMIT_KEYS:
        raw = value.get(key)
        if raw in (None, ""):
            continue
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            raise ValueError("review_rate_limit_invalid") from None
        if parsed > 0:
            result[key] = parsed
    return result


def _normalize_expires_at(value: Any) -> int | None:
    """Epoch-seconds expiry for the review child key; None = inherit parent."""
    if value in (None, "", 0):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        raise ValueError("review_expires_at_invalid") from None
    return parsed if parsed > 0 else None


async def _validate_review_prompt(
    user_id: str, prompt_id: str | None, provider: str | None
) -> str | None:
    """Resolve the review prompt id, rejecting one the user may not use.

    Returns the cleaned id (or None). The content itself is read at dispatch
    time so prompt edits take effect without re-saving the webhook.
    """
    cleaned = (prompt_id or "").strip()
    if not cleaned:
        return None
    from .deps import resolve_team_id
    from .resource_reference_service import resolve_prompt_for_user

    team_id = await resolve_team_id(user_id)
    prompt = await resolve_prompt_for_user(
        user_id, team_id, cleaned, (provider or "").strip().lower()
    )
    if prompt is None:
        raise ValueError("review_prompt_unavailable")
    return cleaned


async def _validate_review_config(
    user_id: str,
    project: Any,
    *,
    review_provider: str | None,
    review_node_ids: list[str],
    review_auto: bool,
    review_framework: str,
    review_enabled: bool,
    review_api_key_id: int | None,
    review_skill_config: list[dict],
    review_mcp_config: list[dict],
    review_plugin_config: list[dict],
) -> None:
    """Validate the canonical review config (provider + nodes + parent key).

    Review is never required to be fully configured at save time — an empty
    provider + node list is a valid draft when ``review_enabled`` is false.
    When enabled, the webhook must carry a provider supported by the chosen
    framework, at least one execution node (or auto mode), and a usable
    parent API key; the canonical Review Task mints its own task-scoped
    child key from that parent.
    """
    import json

    from .review_executors import get_review_framework
    from .review_node_service import review_node_service

    framework = get_review_framework(review_framework)
    if framework is None:
        raise ValueError("unsupported_review_framework")
    if review_auto and review_node_ids:
        # Auto mode picks any ready execution node at dispatch time; manual
        # picks are meaningless and the UI clears them, but enforce it here.
        review_node_ids = []
    provider = (review_provider or "").strip().lower()
    if review_enabled:
        if not provider:
            # Legacy rows still carrying editor ids are validated by their
            # one-shot dispatch adapter; canonical callers must provide this.
            raise ValueError("review_provider_required")
        if provider not in framework.supported_editors:
            if provider == "codex":
                raise ValueError("review_provider_bootstrap_unsupported")
            raise ValueError("review_provider_unsupported")
        if not review_auto and not review_node_ids:
            raise ValueError("review_nodes_required")
        if review_api_key_id is None:
            raise ValueError("review_api_key_required")
    elif provider and provider not in framework.supported_editors:
        # A draft may carry no provider, but a bogus one is still rejected.
        if provider == "codex":
            raise ValueError("review_provider_bootstrap_unsupported")
        raise ValueError("review_provider_unsupported")
    # Node ownership + base availability. When review is disabled we only
    # enforce ownership (draft may save an unready node); when enabled the
    # node must currently be ready to keep dispatch from failing late.
    failures = await review_node_service.validate_candidate_nodes(
        user_id, review_node_ids, review_framework, provider,
        require_ready=bool(review_enabled),
    )
    if failures:
        raise ValueError(
            "review_nodes_unavailable:" + json.dumps(failures, ensure_ascii=False)
        )
    if review_enabled and review_api_key_id is not None:
        from db import PostgresClient

        parent = await PostgresClient.get_api_key_by_id_for_user(
            int(review_api_key_id), user_id
        )
        if parent is None or parent.get("disabled"):
            raise ValueError("review_api_key_unavailable")
    # Skill/MCP/plugin shapes are validated by _validate_session_config at the
    # call site; this function only receives already-coerced lists.


class WebhookService:
    """Project webhook orchestration: platform API + persistence + access control."""

    async def _resolve(
        self, user_id: str, project_id: str, role: str | None
    ) -> tuple[Any, str | None]:
        """Return ``(project, access)`` after project access control.

        ``access`` is ``"owner" | "read_write" | "read_only" | None``.
        """
        project, access = await project_service._get_accessible_project(
            project_id, user_id, role
        )
        return project, access

    async def _load_identity_and_opts(
        self, project: Any
    ) -> tuple[Any, str, str, git_clients.RepositoryOptions] | None:
        """Resolve ``(identity, full_name, platform, opts)`` for a project.

        Returns ``None`` when the project has no usable Git identity/credentials
        or the platform is not webhook-capable.
        """
        if not project.git_identity_id:
            return None
        identity = await git_service.load_identity_for_read(str(project.git_identity_id))
        if identity is None or not identity.access_token:
            return None
        platform = (identity.platform or project.platform or "").lower()
        if platform not in git_clients.WEBHOOK_CAPABLE_PLATFORMS:
            return None
        full_name = _derive_full_name(project.repo_url, project.platform)
        if not full_name:
            return None
        opts = git_service._build_repo_options(identity)
        return identity, full_name, platform, opts

    async def get_webhook(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
    ) -> dict | None:
        project, access = await self._resolve(user_id, project_id, role)
        if project is None:
            return None
        row = await ProjectWebhook.get_or_none(project_id=project.id)
        if row is None:
            return None
        return _webhook_dict(row)

    async def enable_webhook(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        events: list[str] | None = None,
        review_framework: str = "open_code_review_delegate",
        review_provider: str | None = None,
        review_node_ids: list[str] | None = None,
        review_skill_config: list[dict] | None = None,
        review_mcp_config: list[dict] | None = None,
        review_plugin_config: list[dict] | None = None,
        review_editor_ids: list[str] | None = None,
        review_auto: bool = False,
        review_enabled: bool = True,
        review_model_id: str | None = None,
        review_prompt_id: str | None = None,
        review_api_key_id: int | None = None,
        review_model_limits: dict | None = None,
        review_rate_limit: dict | None = None,
        review_expires_at: Any = None,
        review_latest_only: bool = False,
        request: Request | None = None,
    ) -> dict:
        """Upsert a webhook: create on the platform if absent, else update.

        Idempotent: lists existing platform webhooks and reuses the one whose
        callback URL matches ours, so repeated calls never create duplicates.
        """
        project, access = await self._resolve(user_id, project_id, role)
        if project is None:
            raise ValueError("project_not_found")
        if access == "read_only":
            raise ValueError("forbidden")
        review_framework = (review_framework or "open_code_review_delegate").strip()
        node_ids = list(dict.fromkeys(
            str(v).strip() for v in (review_node_ids or []) if str(v).strip()
        ))
        skills = _validate_session_config("skills", review_skill_config)
        mcps = _validate_session_config("mcps", review_mcp_config)
        plugins = _validate_session_config("plugins", review_plugin_config)
        rate_limit = _normalize_rate_limit(review_rate_limit)
        expires_at = _normalize_expires_at(review_expires_at)
        prompt_id = await _validate_review_prompt(
            user_id, review_prompt_id, review_provider
        )
        # New clients select provider + nodes directly; legacy clients may still
        # send review_editor_ids, which is drained lazily at dispatch time.
        legacy_editor_ids = list(dict.fromkeys(
            str(v).strip() for v in (review_editor_ids or []) if str(v).strip()
        ))
        if review_auto:
            node_ids = []
            legacy_editor_ids = []
        # A request that supplies the new provider switches the row to the
        # canonical config and clears the legacy editor list immediately.
        new_config = review_provider is not None or bool(node_ids) or review_auto
        if new_config and not review_provider:
            # Node/auto selections under the new path still need a provider.
            if review_enabled:
                raise ValueError("review_provider_required")
        await _validate_review_config(
            user_id, project,
            review_provider=review_provider,
            review_node_ids=node_ids,
            review_auto=review_auto,
            review_framework=review_framework,
            review_enabled=review_enabled,
            review_api_key_id=review_api_key_id,
            review_skill_config=skills,
            review_mcp_config=mcps,
            review_plugin_config=plugins,
        )
        resolved = await self._load_identity_and_opts(project)
        if resolved is None:
            raise ValueError("repo_unavailable")
        _identity, full_name, platform, opts = resolved
        callback_url = _callback_url(str(project.id), request)
        wanted_events = list(events or _DEFAULT_EVENTS)
        secret = _generate_secret()

        row = await ProjectWebhook.get_or_none(project_id=project.id)
        try:
            existing = await git_clients.list_webhooks(platform, full_name, opts)
        except git_clients.GitClientError as exc:
            await self._record_error(row, project.id, str(exc))
            raise ValueError(f"webhook_upstream_failed: {exc}") from exc
        match = next((h for h in existing if h.url == callback_url), None)

        try:
            if match is not None:
                updated = await git_clients.update_webhook(
                    platform, full_name, opts,
                    hook_id=match.hook_id, url=callback_url, secret=secret,
                    events=wanted_events, active=True,
                )
                hook_id = updated.hook_id or match.hook_id
            elif row is not None and row.hook_id:
                # Stale local row: the platform hook may still exist under a
                # different URL; try updating it in place before creating.
                try:
                    updated = await git_clients.update_webhook(
                        platform, full_name, opts,
                        hook_id=row.hook_id, url=callback_url, secret=secret,
                        events=wanted_events, active=True,
                    )
                    hook_id = updated.hook_id or row.hook_id
                except git_clients.GitClientError as exc:
                    if "HTTP 404" in str(exc):
                        created = await git_clients.create_webhook(
                            platform, full_name, opts,
                            url=callback_url, secret=secret,
                            events=wanted_events, active=True,
                        )
                        hook_id = created.hook_id
                    else:
                        raise
            else:
                created = await git_clients.create_webhook(
                    platform, full_name, opts,
                    url=callback_url, secret=secret,
                    events=wanted_events, active=True,
                )
                hook_id = created.hook_id
        except git_clients.GitClientError as exc:
            await self._record_error(row, project.id, str(exc))
            raise ValueError(f"webhook_upstream_failed: {exc}") from exc

        if row is None:
            row = await ProjectWebhook.create(
                id=uuid.uuid4(),
                project_id=project.id,
                user_id=uuid.UUID(user_id),
                platform=platform,
                full_name=full_name,
                hook_id=str(hook_id),
                callback_url=callback_url,
                secret=secret,
                events=wanted_events,
                active=True,
                review_executor="claude_delegate",
                review_framework=review_framework,
                review_provider=(review_provider or "").strip().lower() or None,
                review_node_ids=node_ids,
                review_skill_config=skills,
                review_mcp_config=mcps,
                review_plugin_config=plugins,
                review_editor_ids=[] if new_config else legacy_editor_ids,
                review_auto=bool(review_auto),
                review_enabled=review_enabled,
                review_model_id=review_model_id,
                review_prompt_id=prompt_id,
                review_api_key_id=review_api_key_id,
                review_model_limits=dict(review_model_limits or {}),
                review_rate_limit=rate_limit,
                review_expires_at=expires_at,
                review_latest_only=bool(review_latest_only),
                last_error=None,
            )
        else:
            row.platform = platform
            row.full_name = full_name
            row.hook_id = str(hook_id)
            row.callback_url = callback_url
            row.secret = secret
            row.events = wanted_events
            row.active = True
            row.review_framework = review_framework
            row.review_provider = (review_provider or "").strip().lower() or None
            row.review_node_ids = node_ids
            row.review_skill_config = skills
            row.review_mcp_config = mcps
            row.review_plugin_config = plugins
            if new_config:
                row.review_editor_ids = []
            else:
                row.review_editor_ids = legacy_editor_ids
            row.review_auto = bool(review_auto)
            row.review_enabled = review_enabled
            row.review_model_id = review_model_id
            row.review_prompt_id = prompt_id
            row.review_api_key_id = review_api_key_id
            row.review_model_limits = dict(review_model_limits or {})
            row.review_rate_limit = rate_limit
            row.review_expires_at = expires_at
            row.review_latest_only = bool(review_latest_only)
            row.last_error = None
            await row.save(
                update_fields=[
                    "platform", "full_name", "hook_id", "callback_url",
                    "secret", "events", "active", "review_framework",
                    "review_provider", "review_node_ids", "review_skill_config",
                    "review_mcp_config", "review_plugin_config",
                    "review_editor_ids", "review_auto", "review_enabled",
                    "review_model_id", "review_prompt_id", "review_api_key_id",
                    "review_model_limits", "review_rate_limit", "review_expires_at",
                    "review_latest_only", "last_error", "updated_at",
                ]
            )
        logger.info(
            "[monkeycode-compat] webhook enabled: project={} platform={} hook_id={}",
            project.id, platform, hook_id,
        )
        return _webhook_dict(row)

    async def update_webhook(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        events: list[str] | None = None,
        active: bool | None = None,
        regenerate_secret: bool = False,
        review_framework: str | None = None,
        review_provider: str | None = None,
        review_node_ids: list[str] | None = None,
        review_skill_config: list[dict] | None = None,
        review_mcp_config: list[dict] | None = None,
        review_plugin_config: list[dict] | None = None,
        review_editor_ids: list[str] | None = None,
        review_auto: bool | None = None,
        review_enabled: bool | None = None,
        review_model_id: str | None = None,
        review_prompt_id: str | None = None,
        review_api_key_id: int | None = None,
        review_model_limits: dict | None = None,
        review_rate_limit: dict | None = None,
        review_expires_at: Any = None,
        review_latest_only: bool | None = None,
        request: Request | None = None,
    ) -> dict:
        project, access = await self._resolve(user_id, project_id, role)
        if project is None:
            raise ValueError("project_not_found")
        if access == "read_only":
            raise ValueError("forbidden")
        from .review_executors import get_review_framework

        if review_framework is not None:
            if get_review_framework(review_framework) is None:
                raise ValueError("unsupported_review_framework")
        row = await ProjectWebhook.get_or_none(project_id=project.id)
        if row is None:
            raise ValueError("webhook_not_configured")
        effective_framework = review_framework or row.review_framework or "open_code_review_delegate"
        # Provider: explicit None in the request clears it; omitted keeps the row.
        if review_provider is not None:
            effective_provider = (review_provider or "").strip().lower() or None
        else:
            effective_provider = (row.review_provider or "").strip().lower() or None
        effective_node_ids = (
            list(dict.fromkeys(str(v).strip() for v in review_node_ids if str(v).strip()))
            if review_node_ids is not None else list(row.review_node_ids or [])
        )
        effective_skills = (
            _validate_session_config("skills", review_skill_config)
            if review_skill_config is not None else list(row.review_skill_config or [])
        )
        effective_mcps = (
            _validate_session_config("mcps", review_mcp_config)
            if review_mcp_config is not None else list(row.review_mcp_config or [])
        )
        effective_plugins = (
            _validate_session_config("plugins", review_plugin_config)
            if review_plugin_config is not None else list(row.review_plugin_config or [])
        )
        effective_editor_ids = (
            list(dict.fromkeys(str(v).strip() for v in review_editor_ids if str(v).strip()))
            if review_editor_ids is not None else list(row.review_editor_ids or [])
        )
        effective_auto = bool(review_auto) if review_auto is not None else bool(row.review_auto)
        new_config = (
            review_provider is not None
            or review_node_ids is not None
            or review_skill_config is not None
            or review_mcp_config is not None
            or review_plugin_config is not None
        )
        if effective_auto:
            effective_node_ids = []
            if new_config:
                effective_editor_ids = []
        effective_enabled = review_enabled if review_enabled is not None else bool(row.review_enabled)
        effective_model_id = review_model_id if review_model_id is not None else row.review_model_id
        effective_api_key_id = (
            review_api_key_id if review_api_key_id is not None else row.review_api_key_id
        )
        effective_model_limits = (
            dict(review_model_limits) if review_model_limits is not None
            else dict(row.review_model_limits or {})
        )
        effective_rate_limit = (
            _normalize_rate_limit(review_rate_limit) if review_rate_limit is not None
            else dict(row.review_rate_limit or {})
        )
        effective_expires_at = (
            _normalize_expires_at(review_expires_at) if review_expires_at is not None
            else (int(row.review_expires_at) if row.review_expires_at else None)
        )
        # An explicit empty string clears the prompt; omitted keeps the row's.
        if review_prompt_id is not None:
            effective_prompt_id = await _validate_review_prompt(
                user_id, review_prompt_id, effective_provider
            )
        else:
            effective_prompt_id = row.review_prompt_id or None
        effective_latest_only = (
            bool(review_latest_only) if review_latest_only is not None
            else bool(row.review_latest_only)
        )
        # Supplying canonical fields switches the row away from the legacy
        # editor-template config. Changing review_auto by itself remains legacy-
        # compatible for old clients whose request model still sends that field.
        if new_config and effective_editor_ids:
            effective_editor_ids = []
        await _validate_review_config(
            user_id, project,
            review_provider=effective_provider,
            review_node_ids=effective_node_ids,
            review_auto=effective_auto,
            review_framework=effective_framework,
            review_enabled=effective_enabled,
            review_api_key_id=effective_api_key_id,
            review_skill_config=effective_skills,
            review_mcp_config=effective_mcps,
            review_plugin_config=effective_plugins,
        )
        resolved = await self._load_identity_and_opts(project)
        if resolved is None:
            raise ValueError("repo_unavailable")
        _identity, full_name, platform, opts = resolved

        new_events = list(events) if events is not None else list(row.events or [])
        new_active = active if active is not None else bool(row.active)
        new_secret = _generate_secret() if regenerate_secret else None

        try:
            await git_clients.update_webhook(
                platform, full_name, opts,
                hook_id=row.hook_id,
                url=row.callback_url,
                secret=new_secret,
                events=new_events,
                active=new_active,
            )
        except git_clients.GitClientError as exc:
            await self._record_error(row, project.id, str(exc))
            raise ValueError(f"webhook_upstream_failed: {exc}") from exc

        if new_secret is not None:
            row.secret = new_secret
        row.events = new_events
        row.active = new_active
        row.review_framework = effective_framework
        row.review_provider = effective_provider
        row.review_node_ids = effective_node_ids
        row.review_skill_config = effective_skills
        row.review_mcp_config = effective_mcps
        row.review_plugin_config = effective_plugins
        row.review_editor_ids = effective_editor_ids
        row.review_auto = effective_auto
        row.review_enabled = effective_enabled
        row.review_model_id = effective_model_id
        row.review_prompt_id = effective_prompt_id
        row.review_api_key_id = effective_api_key_id
        row.review_model_limits = effective_model_limits
        row.review_rate_limit = effective_rate_limit
        row.review_expires_at = effective_expires_at
        row.review_latest_only = effective_latest_only
        row.last_error = None
        await row.save(
            update_fields=[
                "secret", "events", "active", "review_framework",
                "review_provider", "review_node_ids", "review_skill_config",
                "review_mcp_config", "review_plugin_config",
                "review_editor_ids", "review_auto", "review_enabled",
                "review_model_id", "review_prompt_id", "review_api_key_id",
                "review_model_limits", "review_rate_limit", "review_expires_at",
                "review_latest_only", "last_error", "updated_at",
            ]
        )
        return _webhook_dict(row)

    async def resync_callback(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        request: Request | None = None,
    ) -> dict:
        """Recompute the callback URL and sync it to the Git platform.

        Used when the deployment origin changes (e.g. ``webhook_public_origin``
        was edited): the stored ``callback_url`` would otherwise drift from the
        real reachable address, so deliveries stop arriving. We re-PATCH the
        upstream hook with the new URL while preserving its secret/events/active
        state, and persist the new URL locally.
        """
        project, access = await self._resolve(user_id, project_id, role)
        if project is None:
            raise ValueError("project_not_found")
        if access == "read_only":
            raise ValueError("forbidden")
        row = await ProjectWebhook.get_or_none(project_id=project.id)
        if row is None:
            raise ValueError("webhook_not_configured")
        resolved = await self._load_identity_and_opts(project)
        if resolved is None:
            raise ValueError("repo_unavailable")
        _identity, full_name, platform, opts = resolved
        new_url = _callback_url(str(project.id), request)
        # No secret regeneration here: resync is an address repair, not a
        # credential rotation. The stored secret stays so deliveries verify.
        try:
            await git_clients.update_webhook(
                platform, full_name, opts,
                hook_id=row.hook_id,
                url=new_url,
                secret=None,
                events=list(row.events or []),
                active=bool(row.active),
            )
        except git_clients.GitClientError as exc:
            await self._record_error(row, project.id, str(exc))
            raise ValueError(f"webhook_upstream_failed: {exc}") from exc
        row.callback_url = new_url
        row.last_error = None
        await row.save(update_fields=["callback_url", "last_error", "updated_at"])
        logger.info(
            "[monkeycode-compat] webhook callback resynced: project={} url={}",
            project.id, new_url,
        )
        return _webhook_dict(row)

    async def disable_webhook(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
    ) -> bool:
        """Delete the platform webhook and the local row.

        Returns ``True`` if a row was removed, ``False`` if none existed. A
        platform-side 404 is treated as success.
        """
        project, access = await self._resolve(user_id, project_id, role)
        if project is None:
            raise ValueError("project_not_found")
        if access == "read_only":
            raise ValueError("forbidden")
        row = await ProjectWebhook.get_or_none(project_id=project.id)
        if row is None:
            return False
        resolved = await self._load_identity_and_opts(project)
        if resolved is not None:
            _identity, full_name, platform, opts = resolved
            try:
                await git_clients.delete_webhook(
                    platform, full_name, opts, hook_id=row.hook_id
                )
            except git_clients.GitClientError as exc:
                # 404 → already deleted (success). Other failures: record and
                # still drop the local row so the console stays usable; the
                # orphaned platform hook can be cleaned up manually.
                if "HTTP 404" not in str(exc):
                    logger.warning(
                        "[monkeycode-compat] webhook delete failed: project={} error={}",
                        project.id, exc,
                    )
        await row.delete()
        return True

    async def _record_error(
        self, row: ProjectWebhook | None, project_id: uuid.UUID, message: str
    ) -> None:
        if row is None:
            return
        row.last_error = message[:1000]
        await row.save(update_fields=["last_error", "updated_at"])


webhook_service = WebhookService()
