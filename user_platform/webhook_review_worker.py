"""Reliable webhook review dispatcher with a slot-lease queue.

The queue is bounded by two things, whichever is tighter:

1. **Per-node review slots** — each node offers a ``review_capacity`` number of
   concurrent review slots. A review claims a slot via :class:`ReviewNodeLease`
   and releases it when the review finishes, so a node can host several reviews
   up to its capacity. ``acquire_review_slot`` is the atomic guard.
2. **Project concurrency cap** — ``review_project_max_concurrency`` (env/ini)
   bounds how many reviews for *this project* run at once, regardless of how
   many editors are in the candidate pool, so one chatty repo can't hog the
   fleet.

``review_latest_only`` makes a newer event for the same PR/branch supersede
still-queued older ones, so a rapid push stream doesn't pile up stale reviews.

The single executor delegates review reasoning to an editor (Claude Code /
Codex / OpenCode) session. OpenCodeReview supplies deterministic preview/rule
scaffolding through its delegate skill; OCR does not receive a separate LLM
credential.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Awaitable, Callable

from loguru import logger
from tortoise.expressions import Q

from .models_project import Project
from .models_webhook import ProjectWebhook
from .models_webhook_event import ProjectWebhookEvent
from .task_service import task_service

_POLL_SECONDS = 2.0
_MAX_ATTEMPTS = 3
_RETRY_SECONDS = 30
_OCR_SKILL = {
    "name": "open-code-review-delegate",
    "source": "github",
    "url": "https://github.com/alibaba/open-code-review.git",
    "ref": "main",
    "path": "skills/open-code-review-delegate",
}

Executor = Callable[[ProjectWebhookEvent, ProjectWebhook], Awaitable[str]]


def _review_prompt(event: ProjectWebhookEvent, prompt_prefix: str = "") -> str:
    if event.event_type == "pull_request":
        target = (
            f"pull request / merge request #{event.pr_number or '?'}; "
            f"base={event.base_ref or event.target_branch or '?'}; "
            f"head={event.head_ref or event.source_branch or event.commit_sha or '?'}"
        )
        command_hint = (
            f"ocr delegate preview --from {event.base_ref or event.target_branch or '<base>'} "
            f"--to {event.head_ref or event.source_branch or event.commit_sha or '<head>'}"
        )
    else:
        target = (
            f"push commit={event.commit_sha or '?'}; before={event.before_sha or '?'}; "
            f"branch={event.source_branch or '?'}"
        )
        command_hint = f"ocr delegate preview --commit {event.commit_sha or '<commit>'}"
    body = (
        "Perform a read-only code review for this webhook delivery.\n"
        f"Target: {target}\n"
        "Use the open-code-review-delegate skill. Start with its deterministic "
        f"preview/rule workflow (suggested target: `{command_hint}`), review every "
        "included file, and use the review-result MCP tools to submit only concrete "
        "findings and then complete the review. Do not modify files, push commits, "
        "or expose credentials."
    )
    # The per-review project prompt (no editor workspace to write CLAUDE.md into,
    # so it is prepended to the task content instead) leads the instructions.
    prefix = (prompt_prefix or "").strip()
    if prefix:
        return f"{prefix}\n\n{body}"
    return body


async def _resolve_review_prompt_content(hook: ProjectWebhook, project: Project, provider: str) -> str:
    """Content of the webhook's configured review prompt, or '' if none/ungranted."""
    prompt_id = (hook.review_prompt_id or "").strip()
    if not prompt_id:
        return ""
    try:
        from .deps import resolve_team_id
        from .resource_reference_service import resolve_prompt_for_user

        team_id = await resolve_team_id(str(project.user_id))
        prompt = await resolve_prompt_for_user(
            str(project.user_id), team_id, prompt_id, (provider or "").strip().lower()
        )
    except Exception:
        logger.exception("[webhook-review] review prompt resolution failed")
        return ""
    return str((prompt or {}).get("content") or "")


def _merge_named(base: list[dict] | None, overlay: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    unnamed: list[dict] = []
    for item in list(base or []) + overlay:
        if not isinstance(item, dict):
            continue
        copy = dict(item)
        name = str(copy.get("name") or "").strip()
        if name:
            merged[name] = copy
        else:
            unnamed.append(copy)
    return unnamed + list(merged.values())


async def _review_mcp_entry(event: ProjectWebhookEvent) -> dict | None:
    try:
        import builtin_tool_store
        from user_platform import config as compat_config

        _row, token = await builtin_tool_store.issue_token(
            "agent", str(event.id), display_token=False
        )
        base = str(
            getattr(compat_config.settings, "webhook_public_origin", "")
            or getattr(compat_config.settings, "node_server_public_url", "")
            or ""
        ).rstrip("/")
        if not base:
            return None
        return {
            "name": "review-result",
            "type": "sse",
            "url": f"{base}/mcp/review-result/sse?token={token}",
        }
    except Exception:
        logger.exception("[webhook-review] review-result MCP token issuance failed")
        return None


def _editor_cli_name(provider: str) -> str:
    """Map an editor provider to the task ``cli_name`` the node expects."""
    return {
        "claude": "claude", "codex": "codex", "opencode": "opencode", "gemini": "gemini", "cursor": "cursor",
    }.get((provider or "").strip().lower(), "claude")


async def _resolve_legacy_review_config(hook: ProjectWebhook, project: Project) -> None:
    """One-shot drain of a legacy editor-template webhook into canonical config.

    Runs only for rows that still carry ``review_editor_ids``/``review_auto``
    but no ``review_provider``. Picks the first usable editor template (manual
    mode honors the stored order; auto mode considers all of the project's
    editors), copies its provider/node/skill/mcp/plugin/parent-key onto the
    webhook's canonical fields, and clears ``review_editor_ids``. Subsequent
    dispatches take the canonical path, so each legacy row reads the editors
    table at most once.
    """
    from db import PostgresClient
    from .review_node_service import review_node_service
    from .nodes_service import nodes_service

    editors = await PostgresClient.list_project_editors_for_user(
        str(project.id), str(project.user_id)
    )
    if not editors:
        raise RuntimeError("review_legacy_no_editor")
    # Match the old ready-editor semantics: only templates whose bound node is
    # currently usable may seed the migration. This prevents an auto webhook
    # from deterministically choosing a stale first row and then failing forever.
    visible = await nodes_service.list_my_nodes(str(project.user_id))
    node_by_id = {str(n.get("node_id")): n for n in (visible.get("nodes") or [])}
    ready_editors: list[dict] = []
    for editor in editors:
        node_id = str(editor.get("node_id") or "").strip()
        node = node_by_id.get(node_id)
        if node is None:
            continue
        ok, _reason = review_node_service.base_availability(node)
        if ok:
            ready_editors.append(editor)
    editors = ready_editors
    if not editors:
        raise RuntimeError("review_legacy_no_editor")
    manual_ids = [str(v) for v in (hook.review_editor_ids or [])]
    if manual_ids:
        order = {eid: i for i, eid in enumerate(manual_ids)}
        editors = [e for e in editors if str(e.get("id")) in order]
        editors.sort(key=lambda e: order.get(str(e.get("id")), len(order)))
    chosen_provider: str | None = None
    node_ids: list[str] = []
    skills: list[dict] = []
    mcps: list[dict] = []
    plugins: list[dict] = []
    fallback_key_id = hook.review_api_key_id
    for editor in editors:
        provider = (editor.get("provider") or "").strip().lower()
        if not provider:
            continue
        if chosen_provider is None:
            chosen_provider = provider
        elif chosen_provider != provider:
            # Never merge configs from different CLIs into one Task.
            continue
        node_id = (editor.get("node_id") or "").strip()
        if node_id and node_id not in node_ids:
            node_ids.append(node_id)
        skills = _merge_named(skills, list(editor.get("skill_config") or []))
        # editor 行的 mcp_config 现在存服务绑定（service_id/resource_id），不是
        # wire spec。按绑定归一（normalize 校验授权），落 hook 的也是绑定形态——
        # 派发时经 create_task 的 mcp_config → grants 正常链路。
        if editor.get("mcp_config"):
            try:
                from .resource_reference_service import normalize_mcp_bindings
                from .deps import resolve_team_id as _resolve_team_id

                mcps = _merge_named(
                    mcps,
                    await normalize_mcp_bindings(
                        str(project.user_id),
                        await _resolve_team_id(str(project.user_id)),
                        list(editor.get("mcp_config") or []),
                    ),
                )
            except ValueError:
                # 单个 editor 的绑定失效（服务删了/授权没了）不阻断整个迁移。
                pass
        plugins = _merge_named(plugins, list(editor.get("plugin_config") or []))
        if fallback_key_id is None and editor.get("api_key_id"):
            fallback_key_id = int(editor["api_key_id"])
    if chosen_provider is None:
        raise RuntimeError("review_legacy_no_editor")
    hook.review_provider = chosen_provider
    # Auto mode keeps the empty list so dispatch enumerates execution nodes.
    if not bool(hook.review_auto):
        hook.review_node_ids = node_ids
    hook.review_skill_config = skills
    hook.review_mcp_config = mcps
    hook.review_plugin_config = plugins
    if hook.review_api_key_id is None and fallback_key_id is not None:
        hook.review_api_key_id = fallback_key_id
    hook.review_editor_ids = []
    await hook.save(update_fields=[
        "review_provider", "review_node_ids", "review_skill_config",
        "review_mcp_config", "review_plugin_config", "review_api_key_id",
        "review_editor_ids", "updated_at",
    ])


async def execute_claude_delegate(
    event: ProjectWebhookEvent, hook: ProjectWebhook
) -> str:
    """Dispatch one review as a canonical Task on a leased execution node.

    The webhook carries the provider, node pool, parent API key and optional
    skill/mcp/plugin overlays directly; no editor template is consulted on the
    new path. ``task_service.create_task`` mints the Task-owned ``scope='task'``
    child key and injects the node LLM config, so this worker never handles the
    plaintext key. A legacy webhook still configured with ``review_editor_ids``
    is drained into the canonical fields once before dispatch.
    """
    project = await Project.get_or_none(id=event.project_id)
    if project is None:
        raise RuntimeError("project no longer exists")
    from .review_executors import get_review_framework
    from .review_node_service import review_node_service
    from .nodes_service import nodes_service

    framework_name = hook.review_framework or "open_code_review_delegate"
    framework = get_review_framework(framework_name)
    if framework is None:
        raise RuntimeError("unsupported_review_framework")

    # Lazy drain: a row configured under the editor-template model has legacy
    # editor picks but no canonical provider. Resolve once + write back.
    legacy = (not hook.review_provider) and bool(
        hook.review_editor_ids or hook.review_auto
    )
    if legacy:
        await _resolve_legacy_review_config(hook, project)

    provider = (hook.review_provider or "").strip().lower()
    if provider not in framework.supported_editors:
        if provider == "codex":
            raise RuntimeError("review_provider_bootstrap_unsupported")
        raise RuntimeError("review_provider_unsupported")
    if not hook.review_api_key_id:
        raise RuntimeError("review_api_key_required")

    if hook.review_auto:
        nodes = await nodes_service.list_my_nodes(str(project.user_id))
        candidate_node_ids = [
            str(n.get("node_id"))
            for n in (nodes.get("nodes") or [])
            if (n.get("node_role") or n.get("role") or "") == "execution"
            and not n.get("display_only")
        ]
    else:
        candidate_node_ids = [
            str(v).strip() for v in (hook.review_node_ids or []) if str(v).strip()
        ]
    if not candidate_node_ids:
        raise RuntimeError("no_review_node_available")

    acquired = await review_node_service.acquire_review_slot(
        str(project.user_id),
        candidate_node_ids,
        framework_name,
        provider,
        event_id=str(event.id),
        project_id=str(project.id),
    )
    if acquired is None:
        raise RuntimeError("no_review_node_available")
    node_id = str((acquired["node"] or {}).get("node_id") or "").strip()
    lease_id = str(acquired.get("lease_id") or "")

    # Re-verify capability right before dispatch in case the node environment
    # was downgraded after the selection. This is a last-mile safety check, not
    # a configuration prerequisite — review is still delivered as a skill.
    check = await review_node_service.check_node_for_review(
        str(project.user_id), node_id, framework_name, provider
    )
    if not check["ok"]:
        await review_node_service.release_review_slot(lease_id=lease_id)
        raise RuntimeError("review_node_capability_changed")

    mcp_overlay: list[dict] = []
    review_mcp = await _review_mcp_entry(event)
    if review_mcp:
        mcp_overlay.append(review_mcp)
    repo: dict[str, str] = {
        "repo_url": project.repo_url or "",
        "branch": event.source_branch or event.target_branch or project.branch or "",
    }
    checkout_commit = event.commit_sha or event.head_ref
    if checkout_commit:
        repo["commit"] = checkout_commit
    if project.git_identity_id:
        from .git_service import git_service

        identity = await git_service.load_identity_for_read(str(project.git_identity_id))
        if identity is None or not identity.access_token:
            await review_node_service.release_review_slot(lease_id=lease_id)
            raise RuntimeError("project Git identity is unavailable")
        repo["token"] = identity.access_token
        if identity.username:
            repo["username"] = identity.username
    model_id = hook.review_model_id or None
    prompt_prefix = await _resolve_review_prompt_content(hook, project, provider)
    # The canonical Review Task mints its own task-scoped child key from this
    # parent; task_service owns the plaintext and the node LLM config. Limits
    # are enforced server-side as the child key's usage cap.
    req: dict = {
        "content": _review_prompt(event, prompt_prefix),
        "node_id": node_id,
        "provider": provider,
        "cli_name": _editor_cli_name(provider),
        "model_id": model_id,
        "parent_api_key_id": hook.review_api_key_id,
        "usage_limit": dict(hook.review_model_limits or {}) or None,
        "rate_limit": dict(hook.review_rate_limit or {}) or None,
        "expires_at": int(hook.review_expires_at) if hook.review_expires_at else None,
        "git_identity_id": str(project.git_identity_id) if project.git_identity_id else None,
        "repo": repo,
        "task_type": "review",
        "sub_type": "pr_review",
        "mode": "default",
        "task_role": "manual",
        # The review already holds a ReviewNodeLease slot, so it must not take the
        # exclusive TaskNodeBinding (that would cap a node at one review).
        "_review_lease_id": lease_id,
        "extra": {
            "project_id": str(project.id),
            "review_event_id": str(event.id),
        },
        "_session_skills": _merge_named(hook.review_skill_config, [_OCR_SKILL]),
        # MCP 绑定交给 task_service 的正常 grants 链路（mcp_config → service grants，
        # 派发时 _principal_mcp_specs 现造安全 spec）；只有 review-specific overlay
        # 是已经带身份 token 的 session spec，留在 _session_mcps。
        "mcp_config": list(hook.review_mcp_config or []),
        "_session_mcps": list(mcp_overlay),
        "_session_plugins": list(hook.review_plugin_config or []),
    }
    try:
        result = await task_service.create_task(str(project.user_id), req)
    except Exception:
        await review_node_service.release_review_slot(lease_id=lease_id)
        raise
    task_id = str(result.get("id") or "")
    if not task_id or result.get("status") == "error":
        await review_node_service.release_review_slot(lease_id=lease_id)
        raise RuntimeError("review task was not created")
    # Bind the lease to the created task so the completion hook can release the
    # exact slot this review occupied, even across worker restarts.
    await review_node_service.bind_lease_task(lease_id, task_id)
    return task_id


_EXECUTORS: dict[str, Executor] = {"claude_delegate": execute_claude_delegate}


def _supersede_key(event: ProjectWebhookEvent, hook: ProjectWebhook) -> str | None:
    """Identity of the PR/branch a queued event would review.

    Two events with the same key review the same target, so under
    ``review_latest_only`` the older queued one is marked superseded and dropped.
    Returns None for events we can't key (no PR/branch) so they always run.
    """
    if event.event_type == "pull_request":
        return f"pr:{event.pr_number or ''}"
    branch = event.source_branch or event.target_branch or ""
    return f"branch:{branch}" if branch else None


class WebhookReviewWorker:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        # A process may die after claiming an event. With a single compat worker,
        # resetting processing rows at boot is enough to make the DB queue durable.
        await ProjectWebhookEvent.filter(status="processing").update(status="pending")
        # Stale active leases from a crashed process are deleted at boot so the
        # node slots are reclaimable without manual intervention. Leases are a
        # concurrency guard, not history (the event row keeps the audit trail).
        from .models_review import ReviewNodeLease

        await ReviewNodeLease.filter(status="active").delete()
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="webhook-review-worker")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                worked = await self.process_one()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[webhook-review] worker iteration failed")
                worked = False
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=_POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass

    async def _project_inflight(self, project_id: uuid.UUID) -> int:
        """Count reviews currently holding a slot for this project.

        The global per-project cap comes from
        ``AI_LUBRICANT_REVIEW_PROJECT_MAX_CONCURRENCY``; active leases (held from
        dispatch to completion) are the accurate in-flight signal.
        """
        from .review_node_service import review_node_service

        return await review_node_service.project_inflight(str(project_id))

    async def process_one(self) -> bool:
        now = datetime.now(UTC)
        candidate = await ProjectWebhookEvent.filter(
            Q(status="pending") | (Q(status="failed") & Q(next_attempt_at__lte=now))
        ).order_by("created_at").first()
        if candidate is None:
            return False
        # Latest-only: supersede older still-queued events for the same target
        # so a push stream doesn't queue stale reviews.
        hook = await ProjectWebhook.get_or_none(id=candidate.webhook_id, active=True)
        if hook is None:
            candidate.status = "ignored"
            candidate.last_error = "webhook disabled or removed"
            await candidate.save(update_fields=["status", "last_error", "updated_at"])
            return True
        if not hook.review_enabled:
            candidate.status = "ignored"
            candidate.last_error = "review_disabled"
            await candidate.save(update_fields=["status", "last_error", "updated_at"])
            return True
        if hook.review_latest_only:
            await self._supersede_older(candidate, hook)
        # Project concurrency cap is deployment-wide (env/ini), not a webhook
        # form field. Each project has its own lease count and queue.
        from . import config as compat_config

        cap = max(1, int(compat_config.settings.review_project_max_concurrency))
        if await self._project_inflight(candidate.project_id) >= cap:
            return False
        claimed = await ProjectWebhookEvent.filter(
            id=candidate.id, status=candidate.status
        ).update(status="processing", attempts=candidate.attempts + 1, last_error=None)
        if claimed != 1:
            return True
        event = await ProjectWebhookEvent.get(id=candidate.id)
        executor_name = hook.review_executor or "claude_delegate"
        executor = _EXECUTORS.get(executor_name)
        if executor is None:
            await self._fail(event, f"unsupported review executor: {executor_name}")
            return True
        # Dispatch inline (awaited) so an event reaches a terminal dispatch state
        # deterministically; concurrency across candidate nodes comes from the
        # lease gate letting successive iterations claim different nodes' slots.
        await self._dispatch(event, hook, executor)
        return True

    async def _supersede_older(
        self, event: ProjectWebhookEvent, hook: ProjectWebhook
    ) -> None:
        key = _supersede_key(event, hook)
        if not key:
            return
        # Mark older pending events for the same target as superseded. They are
        # not retried; a newer event already covers their diff.
        older = await ProjectWebhookEvent.filter(
            webhook_id=hook.id,
            status="pending",
            created_at__lt=event.created_at,
        )
        for other in older:
            if _supersede_key(other, hook) == key:
                other.status = "superseded"
                other.last_error = "superseded_by_newer_event"
                await other.save(update_fields=["status", "last_error", "updated_at"])

    async def _dispatch(
        self,
        event: ProjectWebhookEvent,
        hook: ProjectWebhook,
        executor: Executor,
    ) -> None:
        try:
            task_id = await executor(event, hook)
            event.task_id = uuid.UUID(task_id)
            event.status = "completed"
            event.last_error = None
            event.next_attempt_at = None
            await event.save(
                update_fields=["task_id", "status", "last_error", "next_attempt_at", "updated_at"]
            )
        except Exception as exc:
            await self._fail(event, str(exc))

    async def _fail(self, event: ProjectWebhookEvent, message: str) -> None:
        event.last_error = (message or "review dispatch failed")[:1000]
        # Release any lease this event may still hold so the node slot is freed
        # for the retry / other events.
        try:
            from .review_node_service import review_node_service

            await review_node_service.release_review_slot(event_id=str(event.id))
        except Exception:
            logger.exception("[webhook-review] lease release on failure failed")
        if event.attempts >= _MAX_ATTEMPTS:
            event.status = "failed"
            event.next_attempt_at = None
        else:
            event.status = "failed"
            event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=_RETRY_SECONDS)
        await event.save(
            update_fields=["status", "last_error", "next_attempt_at", "updated_at"]
        )


webhook_review_worker = WebhookReviewWorker()
