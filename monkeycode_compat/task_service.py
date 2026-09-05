"""Task domain service (coding-agent 'conversation' unit).

Storage + orchestration weaving for the task surface. The standalone node
control process owns live NodeService state; this data service persists task
rows and dispatches sessions through :mod:`.node_client`. If the control
address/token is missing or the process is unavailable, dispatch raises
``NodeServerUnavailable`` and the task degrades to storage-only cleanly.

Multi-user isolation: every query is scoped by ``user_id``. Privileged callers
(admin) may read across users via ``is_privileged``.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, urlparse

from loguru import logger
from tortoise.exceptions import IntegrityError

from .models_task import (
    ProjectTask,
    Task,
    TaskEvent,
    TaskNodeBinding,
    TaskStatusHistory,
    TaskUsageStat,
    TaskVirtualMachine,
)
from .node_client import NodeServerUnavailable, get_local_node_client
from .node_client.errors import Code, RPCError
from .nodes_service import NodesServiceError, nodes_service
from . import task_message_store

# cli_name → agent-compose session provider. The dispatch session REQUIRES a
# provider (the node rejects an empty one); map the C-side CLI choice onto the
# provider names the node understands, defaulting to claude.
_CLI_TO_PROVIDER = {
    "claude": "claude",
    "codex": "codex",
    "gemini": "gemini",
    "opencode": "opencode",
    "cursor": "cursor",
}
_DEFAULT_PROVIDER = "claude"

# Keep fire-and-forget create dispatches strongly referenced until they finish.
# asyncio only holds weak references to tasks, so a long plugin/skill download
# could otherwise be collected right after the HTTP request returned.
_PENDING_CREATE_DISPATCHES: set[asyncio.Task] = set()

_PRIVILEGED_ROLES = {"admin"}

# Server-side git proxy: the node clones a private repo via a signed proxy URL
# instead of ever holding the identity's real access token. Keep the token format
# in sync with ``node_server.git_proxy._sign`` (same domain + shared
# ``node_control_token`` HMAC key).
#
# Token carries scope + mode so a single task can address multiple repositories
# (main + associated) and distinguish read-only from push-enabled access. The
# HMAC signature covers scope + mode, so a client cannot forge ``rw`` by
# editing the clear-text segment.
_GIT_PROXY_DOMAIN = "git-proxy"
_GIT_PROXY_PATH = "/api/v1/public/nodes/git"
_GIT_PROXY_SCOPE_MAIN = "main"
_GIT_PROXY_MODE_RO = "ro"
_GIT_PROXY_MODE_RW = "rw"


def _git_proxy_origin() -> str:
    from . import config

    return (config.settings.node_server_public_url or "").strip().rstrip("/")


def _git_proxy_control_token() -> str:
    """Shared HMAC secret; empty means the proxy is unusable (fail closed)."""
    from . import config

    return (config.settings.node_control_token or "").strip()


def _git_proxy_token(task_id: str, scope: str = _GIT_PROXY_SCOPE_MAIN, mode: str = _GIT_PROXY_MODE_RO) -> str:
    """Stateless signed token the node embeds as the clone URL's userinfo.

    Format: ``<task_id>.<scope>.<mode>.<sig>`` where ``sig`` is the first 32 hex
    chars of ``HMAC-SHA256(node_control_token, "git-proxy:<task_id>:<scope>:<mode>")``.
    The proxy verifies ``sig`` against the same formula (see
    ``node_server.git_proxy._sign``). The 4-segment shape supersedes the old
    2-segment ``<task_id>.<sig>``; old tokens fail validation after deploy, which
    is fine — a re-dispatch re-mints with the new shape.
    """
    import hashlib
    import hmac

    key = _git_proxy_control_token().encode("utf-8")
    scope = scope or _GIT_PROXY_SCOPE_MAIN
    mode = mode if mode in (_GIT_PROXY_MODE_RO, _GIT_PROXY_MODE_RW) else _GIT_PROXY_MODE_RO
    digest = hmac.new(
        key,
        f"{_GIT_PROXY_DOMAIN}:{task_id}:{scope}:{mode}".encode("utf-8"),
        hashlib.sha256,
    )
    return f"{task_id}.{scope}.{mode}.{digest.hexdigest()[:32]}"


def _git_proxy_url(
    task_id: str,
    scope: str = _GIT_PROXY_SCOPE_MAIN,
    mode: str = _GIT_PROXY_MODE_RO,
    *,
    repo_path: str = "",
) -> str:
    """Clone URL the node uses instead of the real repo URL.

    NOTE: the token is ALSO delivered to the node via ``NodeCreateSession.env``
    (see Part 4 of the plan) so it does NOT live in ``.git/config``. The
    userinfo here is kept only as a fallback so plain ``git`` still authenticates
    when the env-driven askpass/extraHeader path is not wired (e.g. legacy nodes);
    the primary credential channel is the env var.

    ``repo_path`` is the host-relative path of the *real* repo (e.g.
    ``owner/repo.git``). Embedding it in the proxy URL makes the main clone land
    at ``/git/r/<repo_path>/``; git then resolves a relative ``.gitmodules`` url
    like ``../sibling.git`` against that path, producing
    ``/git/r/<sibling>/...`` which the proxy recognises as a submodule fetch
    (see :func:`project_service.parse_gitmodules` + the proxy's ``r/`` route).
    """
    origin = _git_proxy_origin()
    token = _git_proxy_token(task_id, scope, mode)
    suffix = _GIT_PROXY_PATH
    if repo_path:
        suffix = f"{suffix}/r/{repo_path}"
    for scheme in ("https://", "http://"):
        if origin.startswith(scheme):
            return f"{scheme}{token}@{origin[len(scheme):]}{suffix}/"
    return f"http://{token}@{origin}{suffix}/"


def _host_relative_path(repo_url: str) -> str:
    """Host-relative path (``owner/repo.git``) of an http(s) repo URL."""
    if not repo_url:
        return ""
    parsed = urlparse(repo_url.strip())
    path = (parsed.path or "").strip("/")
    return path


@dataclass
class TaskListResult:
    total: int
    page: int
    page_size: int
    rows: list[dict]


def _masked_key(value: str) -> str:
    value = str(value or "")
    return f"{value[:7]}...{value[-4:]}" if len(value) > 12 else value


# 自动命名截断长度，与编辑器会话（routes_editors.py 的 task_name）一致。
_TASK_TITLE_MAX_LEN = 40


def _utcnow() -> datetime:
    """Naive UTC now — mc_* tables store naive UTC timestamps (use_tz=False)."""
    return datetime.now(UTC).replace(tzinfo=None)


def _derive_task_title(content: str) -> str:
    """Derive a task title candidate from the first user message.

    Whitespace is collapsed and the result is capped at the same 40-char
    convention editor sessions use. Returns ``""`` when nothing usable is
    left (blank text, attachment-only input, or internal slash commands
    like ``/compact`` — an operational command is not a conversation name).
    """
    text = " ".join((content or "").split())
    if text.startswith("/"):
        return ""
    return text[:_TASK_TITLE_MAX_LEN]


# Wire-spec keys that never leave the server through task DTOs. The persisted
# columns carry the full specs for dispatch/resync; the API surface renders
# only the display shape (name/type/source) so upstream MCP credentials, mirror
# tokens, and the issue-workflow token are not echoed to clients.
_RESOURCE_SECRET_KEYS = (
    "token", "headers", "env", "apiKey", "api_key", "password", "secret",
)


def _redact_resource_config(items: Any) -> list[dict]:
    """Strip secret-bearing keys from a resolved resource config list.

    The persisted columns keep the full wire specs (tokens, env, headers) for
    dispatch/resync; the DTO only carries what the picker needs to render and
    diff: name/type/source/url-host. The ``token`` query param is stripped from
    URLs so an issue-workflow entry exposes its host, not its bearer.
    """
    out: list[dict] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        redacted: dict = {}
        for key, value in item.items():
            if key in _RESOURCE_SECRET_KEYS:
                continue
            if key == "url" and isinstance(value, str):
                parts = urlsplit(value)
                query = urlencode([
                    (pk, pv) for pk, pv in parse_qsl(parts.query)
                    if pk.lower() != "token"
                ])
                value = urlunsplit(parts._replace(query=query))
            redacted[key] = value
        out.append(redacted)
    return out


async def _validate_models_for_key(
    api_key_id: int | None,
    models: list[str] | None,
    *,
    active_model: str | None = None,
) -> tuple[list[str], str | None]:
    """校验一批模型名是否可被指定 API Key 执行。

    收口任务/编辑器写入边界：模型必须非内部 id、当前可供给、且落在该 Key 的
    白/黑名单内。任一不合法直接抛 ValueError，避免任务先落库、节点启动后才
    在网关推理阶段失败。返回过滤后的合法模型列表与激活模型（激活模型若非法
    回退到首个合法模型，全空时为 None）。
    """
    from rate_limiter import ModelClientPool
    from model_catalog import is_internal_model_id
    # 根 config 模块，不是 monkeycode_compat.config（后者是 settings）。
    import config as gateway_config

    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in models or []:
        name = str(raw or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        cleaned.append(name)

    if api_key_id is None:
        # 无执行 Key 的草稿不做 Key 级过滤，但仍拒绝内部 id 与不可供给模型，
        # 避免将来派发时才暴露问题。
        available = ModelClientPool.available_model_ids()
        legal = [m for m in cleaned if not is_internal_model_id(m) and m in available]
    else:
        from db import PostgresClient

        key_row = await PostgresClient.get_api_key_by_id(int(api_key_id))
        if not key_row or key_row.get("disabled") or not str(key_row.get("key") or "").strip():
            raise ValueError("parent_api_key_invalid")
        api_key = str(key_row["key"])
        legal = [
            m for m in cleaned
            if not is_internal_model_id(m)
            and m in ModelClientPool.available_model_ids()
            and await gateway_config.Config.api_key_allows_model(api_key, m)
        ]

    if not cleaned:
        return [], active_model or None
    if not legal:
        raise ValueError("model_not_available_for_key")
    # 激活模型必须落在合法集合内，否则回退到首项。
    resolved_active = active_model if active_model in legal else legal[0]
    return legal, resolved_active


def _merge_mcp_config(base: list[dict] | None, overlay: list[dict] | None) -> list[dict]:
    """Merge task base MCP config with its per-task overlay by name.

    Overlay wins and is always preserved on hot resync, so a base update does
    not wipe per-task servers such as issue-workflow. Mirrors the editor path.
    """
    merged: dict[str, dict] = {}
    unnamed: list[dict] = []
    for item in list(base or []) + list(overlay or []):
        if not isinstance(item, dict):
            continue
        copy = dict(item)
        name = str(copy.get("name") or "").strip()
        if name:
            merged[name] = copy
        else:
            unnamed.append(copy)
    return unnamed + list(merged.values())



def _runtime_failure_reason(raw_error: str, exit_code: int) -> str:
    """Turn a node's raw runtime failure into a cause + a next action.

    The node reports engine-level strings ("agent-compose-runtime not found on
    node", a Node.js ``ERR_MODULE_NOT_FOUND`` stack, …). Shown verbatim they
    tell the person reading the task page nothing about what to *do*, so the
    known shapes are mapped to a cause plus the fix. Anything unrecognized is
    passed through (truncated) rather than swallowed — an unknown real error
    beats a vague generic one.
    """
    text = (raw_error or "").strip()
    lowered = text.lower()
    # Runtime absent, or present but unusable (missing deps / no Node.js). All
    # three are the same user-facing situation: the node's agent runtime is not
    # ready, and only an operator installing/upgrading it can fix that.
    runtime_markers = (
        "agent-compose-runtime not found",
        "agent runtime is not usable",
        "node agent runtime",
        "err_module_not_found",
        "cannot find package",
        "cannot find module",
        "node.js is missing",
    )
    if any(marker in lowered for marker in runtime_markers):
        return "节点上的 Agent 运行环境不可用（未安装或依赖缺失），需要管理员在节点管理页升级该节点的运行环境后重新运行任务。"
    if not text:
        return f"运行环境启动后立即退出（退出码 {exit_code}），未返回错误信息。请联系管理员检查该节点的运行环境。"
    return text[:800]


def _task_reasoning_effort(task: Task) -> str:
    snapshot = task.config_snapshot if isinstance(task.config_snapshot, dict) else {}
    return str(snapshot["reasoning_effort"] or "") if "reasoning_effort" in snapshot else "medium"


# Bring-up steps in execution order, with the words the UI shows. Keyed by the
# short form stored in ``mc_tasks.runtime_stage`` (see node_server.task_stage
# ``stage_key``). ``index``/``total`` let the page render "2/4" without the
# frontend hardcoding the sequence — the order lives here, next to the labels.
_RUNTIME_STAGES: tuple[tuple[str, str], ...] = (
    ("workspace_prepare", "准备工作目录"),
    ("git_clone", "拉取代码"),
    # Skills/plugins/MCP are downloaded and unpacked into the session before the
    # runtime starts. This can take a while, and it used to be an invisible gap
    # between "拉取代码" and "检查运行环境" — the page showed nothing while the
    # node was busy, which reads as a hang. It is also the step where a bad
    # skill/plugin surfaces, so it needs its own name to fail under.
    ("resource_sync", "同步技能与插件"),
    ("runtime_preflight", "检查运行环境"),
    ("runtime_start", "启动运行环境"),
    ("running", "就绪"),
)
_RUNTIME_STAGE_LABELS = dict(_RUNTIME_STAGES)
_RUNTIME_STAGE_ORDER = {key: i + 1 for i, (key, _label) in enumerate(_RUNTIME_STAGES)}
# The last entry is "ready", not a preparation step, so progress counts the ones
# before it: a task at runtime_start is on step 4 of 4, and reaching "running"
# means preparation is done rather than 5/5.
_RUNTIME_STAGE_TOTAL = len(_RUNTIME_STAGES) - 1

# The task is done preparing once the node reports this stage.
RUNTIME_STAGE_READY = "running"


def _task_runtime_stage(task: Task) -> dict | None:
    """Project the node-reported bring-up step for the task DTO.

    Returns ``None`` for a task that has never dispatched (nothing to show).
    Reads the dedicated columns rather than ``config_snapshot`` so the value is
    the same one queries filter on, and carries ``preparing`` so the frontend
    does not have to re-derive "still preparing" from a stage name it would then
    need to keep in sync with the proto.
    """
    key = (getattr(task, "runtime_stage", "") or "").strip()
    if not key:
        # Deferred-create gap: the row is claimed for background dispatch but
        # the node has not reported its first stage frame yet (the ack can take
        # up to the 30s timeout). Without this virtual step the page falls back
        # to 「等待派发」 during exactly the window where the user just clicked
        # 创建 — showing「准备中 · 正在连接执行节点」closes it. index=0 keeps
        # the "n/N" counter omitted until a real stage gives it a position.
        if (getattr(task, "workspace_state", "") or "") == "dispatching":
            return {
                "stage": "dispatching",
                "label": "正在连接执行节点",
                "ok": True,
                "detail": None,
                "index": 0,
                "total": _RUNTIME_STAGE_TOTAL,
                "preparing": True,
            }
        return None
    ok = bool(getattr(task, "runtime_stage_ok", True))
    return {
        "stage": key,
        "label": _RUNTIME_STAGE_LABELS.get(key, key),
        "ok": ok,
        "detail": (getattr(task, "runtime_stage_detail", "") or "") or None,
        "index": _RUNTIME_STAGE_ORDER.get(key, 0),
        "total": _RUNTIME_STAGE_TOTAL,
        # Preparing = a step is in flight and none has failed. A failed step is
        # not "preparing" (it is stuck), and the ready stage is not either.
        "preparing": ok and key != RUNTIME_STAGE_READY,
    }


def _task_summary(task: Task) -> dict:
    config_snapshot = task.config_snapshot if isinstance(task.config_snapshot, dict) else {}
    return {
        "id": str(task.id),
        "user_id": str(task.user_id),
        "kind": task.kind,
        "sub_type": task.sub_type,
        "mode": task.mode,
        "mode_label": task.mode_label,
        "reasoning_effort": _task_reasoning_effort(task),
        "title": task.title,
        "content": task.content,
        "summary": task.summary,
        "status": task.status,
        "provider": task.provider,
        "node_id": task.node_id,
        "node_session_id": task.node_session_id,
        "env_mode": str(config_snapshot.get("env_mode") or "") or None,
        "env_id": str(config_snapshot.get("env_id") or "") or None,
        "env_name": str(config_snapshot.get("env_name") or "") or None,
        "workspace_key": task.workspace_key,
        "workspace_state": task.workspace_state,
        "workspace_source_task_id": task.workspace_source_task_id,
        "dispatch_error": str(config_snapshot.get("dispatch_error") or "") or None,
        # Which bring-up step the node last reported (prepare workspace / clone /
        # runtime pre-flight / start), so the page can say where a task is — or
        # where it stopped — instead of only that it failed. Built from the
        # dedicated columns; the config_snapshot copy is a fallback for rows
        # written before those columns existed.
        "runtime_stage": _task_runtime_stage(task),
        "source_editor_id": task.source_editor_id,
        "source_editor_session_id": task.source_editor_session_id,
        "models": list(task.models_snapshot or []) if isinstance(task.models_snapshot, list) else [],
        "api_key_id": task.api_key_id,
        "parent_api_key_id": task.parent_api_key_id,
        "usage_limit": dict(task.usage_limit or {}) if isinstance(task.usage_limit, dict) else {},
        "expires_at": task.expires_at.isoformat() if task.expires_at else None,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "last_active_at": task.last_active_at.isoformat() if task.last_active_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
    }


async def _enrich_task_summary(
    task: Task,
    item: dict,
    binding: ProjectTask | None = None,
    *,
    api_key_by_id: dict[int, dict] | None = None,
) -> dict:
    """Attach stable ProjectTask + masked key metadata to one Task DTO.

    ``api_key_by_id`` is a pre-batched map (id→row) for the list path so each
    row does not hit the DB. ``None`` falls back to a single lookup (detail path,
    one row — no N+1 there).
    """
    if binding is None:
        binding = await ProjectTask.filter(task_id=task.id).order_by("created_at").first()
    item.update({
        "project_id": str(binding.project_id) if binding and binding.project_id else None,
        "model_id": str(binding.model_id) if binding and binding.model_id and binding.model_id.int else None,
        "git_identity_id": str(binding.git_identity_id) if binding and binding.git_identity_id else None,
        "repo_url": binding.repo_url if binding else None,
        "branch": binding.branch if binding else None,
        "cli_name": binding.cli_name if binding else (task.provider or ""),
        "task_role": binding.task_role if binding else None,
    })
    if task.api_key_id:
        key = None
        if api_key_by_id is not None:
            key = api_key_by_id.get(int(task.api_key_id))
        else:
            from db import PostgresClient

            key = await PostgresClient.get_api_key_by_id(int(task.api_key_id))
        if key is not None:
            item["api_key"] = {
                "key_id": int(key["id"]),
                "name": key.get("name") or "",
                "key_masked": _masked_key(key.get("key") or ""),
                "version": key.get("version"),
                "disabled": bool(key.get("disabled")),
                "expires_at": key.get("expires_at"),
                "usage_limit": dict(key.get("usage_limit") or {}),
            }
    return item


async def _task_detail(task: Task) -> dict:
    detail = await _enrich_task_summary(task, _task_summary(task))
    detail["content"] = task.content
    detail["log_store"] = task.log_store
    detail["mcp_config"] = _redact_resource_config(task.mcp_config)
    detail["skill_config"] = _redact_resource_config(task.skill_config)
    detail["plugin_config"] = _redact_resource_config(task.plugin_config)
    detail["mcp_overlay"] = _redact_resource_config(task.mcp_overlay_json)
    detail["mcp_principal"] = await _task_principal_view(task)
    return detail


async def _task_principal_view(task: Task) -> dict | None:
    """Read-only view of the task's MCP principal.

    The principal is a normal ``usage_type='task'`` mcp_user. The task surface
    only shows its id/enabled flag and grants — never the token, not even a
    hint. Management (enable/disable/grant edit) happens through the task
    lifecycle, not through this DTO.
    """
    if not task.mcp_user_id:
        return None
    import mcp_plugin_store

    owner = str(task.user_id)
    principal = await mcp_plugin_store.get_owned_mcp_principal(task.mcp_user_id, owner)
    if not principal or principal.get("usage_type") != "task":
        # Stale binding or principal removed out-of-band: surface nothing.
        return None
    try:
        params = await mcp_plugin_store.list_principal_params(task.mcp_user_id)
    except Exception:
        logger.exception("[monkeycode-compat] task {} principal view failed", task.id)
        params = []
    return {
        "principal_id": int(task.mcp_user_id),
        "usage_type": "task",
        "enabled": bool(principal.get("enabled")),
        "params": [
            {
                "param_key": p.get("param_key"),
                "param_value": p.get("param_value"),
            }
            for p in params
        ],
    }


# Standard note prefixed to every human turn delivered to a runtime (Part 4.3):
# the agent must not expect a real credential in .git/config and must not
# hand-craft credential-bearing remotes — the workspace is provisioned with
# clean URLs and the credential is supplied out of band, so plain `git` works.
_GIT_CREDENTIAL_NOTE = (
    "仓库凭据由任务范围的签名 token 与环境变量管理，git 远程地址不含凭据；"
    "请勿尝试从 .git/config / git remote 读取或拼接带凭据的 URL，"
    "直接使用 git 命令完成拉取/提交/推送即可。"
)


def _with_git_credential_note(content: str) -> str:
    """Prepend the git-credential note to one human turn.

    Prefixed per turn rather than once per session: it is stateless (no
    "was it already sent for this handle" bookkeeping across re-dispatch and
    runtime revival), and it survives the provider CLI compacting its context —
    which is exactly when an agent would otherwise start guessing at credentials.
    The note is short, so the per-turn cost is a constant handful of tokens.
    """
    body = (content or "").strip()
    return f"{_GIT_CREDENTIAL_NOTE}\n\n{body}".strip() if body else _GIT_CREDENTIAL_NOTE


class TaskService:
    """Task lifecycle: create/list/info/stop/cancel/delete/update."""

    def __init__(self) -> None:
        # A task may receive a first message while the user also clicks Start.
        # Serialize dispatch per task so one storage-state row cannot create two
        # node sessions. The conditional DB update below remains the final guard
        # for requests handled by different API processes.
        self._runtime_dispatch_locks: dict[str, asyncio.Lock] = {}
        # 同任务消息发送串行化（幂等关键区）。与派发锁分开：发送锁 → 派发锁
        # 单向嵌套，派发路径绝不反取发送锁，避免不可重入死锁。
        self._message_send_locks: dict[str, asyncio.Lock] = {}

    def _runtime_dispatch_lock(self, task_id: uuid.UUID | str) -> asyncio.Lock:
        return self._runtime_dispatch_locks.setdefault(str(task_id), asyncio.Lock())

    def _message_send_lock(self, task_id: uuid.UUID | str) -> asyncio.Lock:
        return self._message_send_locks.setdefault(str(task_id), asyncio.Lock())

    def _is_privileged(self, role: str | None) -> bool:
        return (role or "") in _PRIVILEGED_ROLES

    async def list_tasks(
        self,
        user_id: str,
        *,
        role: str | None = None,
        page: int = 1,
        page_size: int = 24,
        project_id: str | None = None,
        statuses: list[str] | None = None,
    ) -> TaskListResult:
        page = max(1, page)
        page_size = max(1, min(page_size, 200))
        query = Task.all() if self._is_privileged(role) else Task.filter(user_id=uuid.UUID(user_id))
        if statuses:
            query = query.filter(status__in=statuses)
        if project_id:
            try:
                pid = uuid.UUID(project_id)
            except (ValueError, TypeError):
                return TaskListResult(total=0, page=page, page_size=page_size, rows=[])
            # ProjectTask is the source of truth for project membership. Collapse
            # duplicate bindings before filtering so total and pagination cannot
            # count the same task more than once.
            task_ids = set(await ProjectTask.filter(project_id=pid).values_list("task_id", flat=True))
            if not task_ids:
                return TaskListResult(total=0, page=page, page_size=page_size, rows=[])
            query = query.filter(id__in=list(task_ids))
        total = await query.count()
        rows = (
            await query.order_by("-created_at")
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        # Enrich the list in one query. The manager conversations page reuses
        # this task list as its project-conversation source and needs project_id
        # to open project detail. A task may have multiple historical bindings;
        # keep the first non-null project id deterministically.
        binding_by_task: dict[uuid.UUID, ProjectTask] = {}
        if rows:
            bindings = await ProjectTask.filter(task_id__in=[row.id for row in rows]).order_by("created_at")
            for binding in bindings:
                binding_by_task.setdefault(binding.task_id, binding)
        # 批量取回本页任务的 api_key 元信息，避免下面逐行 enrich 时 N+1 DB 往返。
        api_key_by_id: dict[int, dict] = {}
        key_ids = [int(row.api_key_id) for row in rows if row.api_key_id]
        if key_ids:
            from db import PostgresClient

            api_key_by_id = await PostgresClient.get_api_keys_by_ids(key_ids)
        summaries = []
        for row in rows:
            item = await _enrich_task_summary(
                row, _task_summary(row), binding_by_task.get(row.id),
                api_key_by_id=api_key_by_id,
            )
            summaries.append(item)
        return TaskListResult(
            total=total,
            page=page,
            page_size=page_size,
            rows=summaries,
        )

    async def get_task(self, user_id: str, task_id: str, *, role: str | None = None) -> dict | None:
        try:
            tid = uuid.UUID(task_id)
        except (ValueError, TypeError):
            return None
        task = await Task.get_or_none(id=tid)
        if task is None:
            return None
        if not self._is_privileged(role) and str(task.user_id) != str(user_id):
            return None
        return await _task_detail(task)

    async def create_task(self, user_id: str, req: dict, *, defer_dispatch: bool = False) -> dict:
        """Persist a task; mint its child key; start the runtime when enabled.

        The row is created first so the task is always visible even if the
        external orchestrator is unavailable. A runnable task must carry a
        parent API Key; a ``scope='task'`` child is minted from it and bound to
        the Task before dispatch. Codex tasks additionally require a pre-registered
        installation id + first-content hash so the first gateway request can be
        bound to this Task only.

        ``defer_dispatch=True`` moves the node dispatch (which waits for the
        node to clone the repo, download skills/plugins/MCP, and start the
        runtime — up to the 30s ack timeout) into a background task so the
        create request returns immediately. The task row lands in
        ``workspace_state='dispatching'`` first, so the UI can render「准备中」
        from the virtual runtime stage until the node's own stage frames take
        over. Internal callers that depend on the created row being live (the
        webhook review lease flow, the issue workflow) keep the default
        synchronous path.
        """
        content = (req.get("content") or "").strip()
        # 内容可为空：有节点时建一个等待用户输入的交互式任务（派发但不推首条
        # 消息），无节点时建一个存储态草稿。用户在详情页发第一条消息后进入对话。

        provider = (req.get("provider") or "").strip().lower() or _CLI_TO_PROVIDER.get(
            (req.get("cli_name") or "").strip().lower(), _DEFAULT_PROVIDER
        )
        parent_api_key_id = req.get("parent_api_key_id")
        node_id = (req.get("node_id") or "").strip()
        wants_dispatch = bool(node_id)

        # A runnable Task must carry a parent key so it can mint its own child
        # key. No parent key ⇒ storage/draft only; node dispatch is refused.
        if wants_dispatch and not parent_api_key_id:
            raise ValueError("parent_api_key_required")

        # 写入边界校验：父 Key 存在时，models 与激活 model_id 必须落在该 Key
        # 的可执行集合内（非内部 id、当前可供给、过白/黑名单）。草稿（无父 Key）
        # 也拒绝内部/不可供给模型，避免派发时才在网关阶段失败。激活模型非法
        # 时回退到首个合法模型，保证落库的 models_snapshot 头部始终合法。
        raw_models = req.get("models")
        models_list = [str(m) for m in raw_models if str(m).strip()] if isinstance(raw_models, list) else []
        validated_models, validated_model = await _validate_models_for_key(
            parent_api_key_id,
            models_list,
            active_model=(req.get("model_id") or (models_list[0] if models_list else None)),
        )
        req = dict(req)
        req["models"] = validated_models or None
        if validated_model is not None:
            req["model_id"] = validated_model

        expected_client_id = (req.get("expected_client_id") or "").strip() or None
        bootstrap_content = (req.get("bootstrap_content") or "").strip()
        bootstrap_content_hash = None
        if bootstrap_content:
            import hashlib

            bootstrap_content_hash = hashlib.sha256(
                bootstrap_content.encode("utf-8")
            ).hexdigest()
        # Codex tasks must pre-register installation id + first content so the
        # first provider request can be matched against this Task only.
        if provider == "codex" and wants_dispatch:
            if not expected_client_id or not bootstrap_content_hash:
                raise ValueError("codex_bootstrap_required")

        # Public callers may select reviewed resources explicitly. Keep the
        # internal underscore keys authoritative for trusted worker overlays,
        # while mapping the public fields into the same node-session shape.
        req = dict(req)
        for public_key, internal_key in (
            ("skill_config", "_session_skills"),
            ("mcp_config", "_session_mcps"),
            ("plugin_config", "_session_plugins"),
        ):
            items = req.get(public_key)
            if internal_key not in req and isinstance(items, list):
                req[internal_key] = [dict(item) for item in items if isinstance(item, dict)]

        task = await Task.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            kind=req.get("task_type") or "develop",
            sub_type=req.get("sub_type") or None,
            mode=(req.get("mode") or None),
            mode_label=(req.get("mode_label") or None),
            mode_capability_snapshot=req.get("mode_capability_snapshot") or None,
            # 创建时没让用户起名：content 非空即首条对话，直接由它补齐任务名；
            # content 为空则留给详情页首条消息补齐（send_task_message）。
            title=(_derive_task_title(content) or None),
            content=content,
            status="pending",
            provider=provider,
            node_id=node_id or None,
            workspace_key=f"task_{uuid.uuid4().hex}",
            workspace_state="pending",
            models_snapshot=req.get("models") or None,
            config_snapshot={
                "system_prompt": req.get("system_prompt"),
                "resource": req.get("resource"),
                "reasoning_effort": str(req.get("reasoning_effort") or "medium"),
                "env_mode": (req.get("env_mode") or "").strip().lower() or None,
                "env_id": (req.get("env_id") or "").strip() or None,
                "env_name": (req.get("env_name") or "").strip() or None,
            },
            mcp_config=list(req.get("mcp_config") or []) if isinstance(req.get("mcp_config"), list) else [],
            skill_config=list(req.get("skill_config") or []) if isinstance(req.get("skill_config"), list) else [],
            plugin_config=list(req.get("plugin_config") or []) if isinstance(req.get("plugin_config"), list) else [],
            environment_snapshot=req.get("environment") or None,
            source_editor_id=(req.get("extra") or {}).get("editor_id"),
            source_editor_session_id=req.get("editor_session_id"),
            expected_client_id=expected_client_id,
            bootstrap_content_hash=bootstrap_content_hash,
            log_store=req.get("log_store") or None,
        )
        # Seed the replay trail with the task's birth state, so a later
        # ``pending → …`` transition has something to hang off and a trail read
        # never has to infer "it must have started at pending".
        await self._record_status_transition(task.id, "status", "pending", reason="create_task")
        # ProjectTask binding (model/git/branch/cli). Stored regardless of
        # whether the session is actually dispatched. Image binding was removed:
        # the runtime is now a chosen node, not a server-defined image, so
        # ``image_id`` is written as the zero UUID for schema compatibility only.
        await ProjectTask.create(
            id=uuid.uuid4(),
            task_id=task.id,
            model_id=_maybe_uuid(req.get("model_id")) or uuid.UUID(int=0),
            image_id=uuid.UUID(int=0),
            git_identity_id=_maybe_uuid(req.get("git_identity_id")),
            project_id=_maybe_uuid((req.get("extra") or {}).get("project_id")),
            issue_id=_maybe_uuid((req.get("extra") or {}).get("issue_id")),
            task_role=(req.get("task_role") or None),
            repo_url=(req.get("repo") or {}).get("repo_url") or None,
            branch=(req.get("repo") or {}).get("branch") or None,
            cli_name=req.get("cli_name") or "",
        )

        # Mint the task child key before any dispatch. The key row is written in
        # its own asyncpg transaction; the Task row is updated through the ORM.
        # On ORM failure the freshly-minted key is disabled so no orphan
        # credential survives.
        api_key_copy = None
        if parent_api_key_id:
            from db import PostgresClient

            try:
                api_key_copy = await PostgresClient.create_task_child_api_key(
                    int(parent_api_key_id),
                    task_id=str(task.id),
                    usage_limit=req.get("usage_limit"),
                    expires_at=req.get("expires_at"),
                    provider=provider,
                    rate_limit=req.get("rate_limit"),
                    # 可用模型同源既写 models_snapshot(下面 Task.create)又收窄子 Key
                    # 的 model_whitelist；可用编辑器(前端单选映射)收窄 editor 白名单；
                    # selection_strategy 覆盖选择策略。省略=继承父级。
                    model_whitelist=validated_models or None,
                    editor_provider_whitelist=req.get("editor_provider_whitelist"),
                    selection_strategy=req.get("selection_strategy"),
                )
            except ValueError as exc:
                task.status = "error"
                await task.save(update_fields=["status", "updated_at"])
                await self._record_status_transition(
                    task.id, "status", "error", frm="pending", reason=str(exc)
                )
                reason = str(exc)
                # 子 Key 白名单越权（动态消息）与不支持的策略码需带具体原因透出，
                # 不能被兜底成 parent_api_key_invalid 丢掉语义。
                if reason.startswith((
                    "子 Key 的模型白名单不能超出父 Key 范围",
                    "子 Key 的编辑器客户端白名单不能超出父 Key 范围",
                )) or reason == "不支持的选择策略":
                    raise
                raise ValueError("parent_api_key_invalid") from exc
            task.api_key_id = api_key_copy["id"]
            task.parent_api_key_id = int(parent_api_key_id)
            task.usage_limit = api_key_copy.get("usage_limit") or None
            try:
                await task.save(
                    update_fields=["api_key_id", "parent_api_key_id", "usage_limit", "updated_at"]
                )
            except Exception:
                await PostgresClient.disable_api_key_by_id(api_key_copy["id"])
                task.api_key_id = None
                task.status = "error"
                await task.save(update_fields=["api_key_id", "status", "updated_at"])
                await self._record_status_transition(
                    task.id, "status", "error", frm="pending", reason="api_key creation failed"
                )
                await self._refresh_api_key_snapshot()
                raise
            # Must happen before dispatch: the node starts issuing /v1 requests
            # as soon as the session is up, and auth resolves the key from the
            # in-memory snapshot.
            await self._refresh_api_key_snapshot()

        client = get_local_node_client()
        if not client.enabled:
            logger.info(
                "[monkeycode-compat] task {} created (storage only; node server disabled)",
                task.id,
            )
            result = await _task_detail(task)
            if api_key_copy:
                result["api_key"] = self._api_key_response(api_key_copy)
            return result

        if not wants_dispatch:
            # Parent key chosen but no node yet: keep the task pending with its
            # child key. The user can dispatch later once a node is selected.
            result = await _task_detail(task)
            if api_key_copy:
                result["api_key"] = self._api_key_response(api_key_copy)
            return result

        # A node is required to dispatch. The node must be one the user's groups
        # were granted (anti-horizontal-access). Beyond that we do NOT lock the
        # node: agent-compose itself admits sessions up to the node's configured
        # capacity (max_sessions/cpu/memory), so several tasks may share one node.
        if not node_id:
            raise ValueError("node_required")
        # The permission check stays in the request even for deferred dispatch:
        # a 403 for a node outside the user's grants must reach the caller, not
        # surface minutes later as a dispatch_failed task row.
        binding_link = await nodes_service.user_can_use_node(user_id, node_id)
        if binding_link is None:
            raise ValueError("node_forbidden")
        await self._require_system_env_allowed(node_id, req.get("env_mode"))

        if defer_dispatch:
            return await self._schedule_created_dispatch(task, req, api_key_copy)
        return await self._dispatch_created_task(task, req, api_key_copy)

    async def _require_system_env_allowed(self, node_id: str, env_mode: object) -> None:
        """system 档派发前校验：目标节点在线且已上报能力但未开启系统环境 → 拒绝。

        与 node_forbidden 同级的派发前 ValueError（routes_task 的 detail_map 负责映射
        成友好文案）。只在节点**在线**时拦截：_live_node_map 对离线节点也会返回条目
        （online=False 且能力快照可能过期），离线节点的真实开关无从判断，交由节点端
        resolveHome 拒绝并落 dispatch_error，避免拿过期快照误杀。
        """
        if str(env_mode or "").strip().lower() != "system":
            return
        live = await nodes_service.node_live_info(node_id)
        if not live or not live.get("online"):
            return
        caps = live.get("capabilities") or {}
        if caps and caps.get("system_env") != "true":
            raise ValueError("node_system_env_disabled")

    async def _schedule_created_dispatch(
        self, task: Task, req: dict, api_key_copy: dict | None
    ) -> dict:
        """Hand the dispatch phase to the background so create returns fast.

        「准备中」的入口：节点连不上、技能/插件/MCP 下载多久，都不阻塞创建响应。
        行先抢占为 ``workspace_state='dispatching'``（DTO 据此投影虚拟 stage，见
        ``_task_runtime_stage``），首条消息先落库（进对话时间线，进程崩溃也不
        丢），再由后台任务做真正的派发与投递。
        """
        # 条件更新抢占派发权（pending → dispatching）。抢不到说明恢复 worker 已
        # 接管该行：后台派发照常发起——节点按 session_id 幂等采纳重复派发
        # （node_server 的 "already exists" 分支），不会产生两个 runtime。
        claimed = await Task.filter(id=task.id, workspace_state="pending").update(
            workspace_state="dispatching", updated_at=_utcnow()
        )
        task.workspace_state = "dispatching"
        if claimed:
            await self._record_status_transition(
                task.id, "workspace_state", "dispatching", frm="pending",
                reason="create dispatch claimed",
            )
        # 首条消息提前落库（delivery_status='pending'）。_dispatch_created_task
        # 随后用同一个幂等键再 persist（IntegrityError → 返回已有行）并 claim，
        # 与用户手动发送/恢复 worker 的补偿投递收敛为一次真正投递。键要写回
        # req：后台任务自己生成的话会换一个 id，persist 出第二行。
        content = (task.content or "").strip()
        if content:
            first_message_id = (req.get("client_message_id") or "").strip() or str(uuid.uuid4())
            req["client_message_id"] = first_message_id
            try:
                await self._persist_user_input_item(
                    task.id, content,
                    client_message_id=first_message_id, delivery_status="pending",
                )
            except Exception:
                logger.exception(
                    "[monkeycode-compat] task {} initial input persist failed", task.id
                )
        # Fire-and-forget, strongly referenced (see _PENDING_CREATE_DISPATCHES):
        # the caller's request loop ends here, nothing else holds this task.
        bg = asyncio.ensure_future(
            self._dispatch_created_task(task, req, api_key_copy, notify_created=False)
        )
        _PENDING_CREATE_DISPATCHES.add(bg)

        def _finish(future: asyncio.Task) -> None:
            _PENDING_CREATE_DISPATCHES.discard(future)
            try:
                future.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 — background boundary
                logger.opt(exception=exc).error(
                    "[monkeycode-compat] task {} deferred dispatch crashed", task.id
                )

        bg.add_done_callback(_finish)
        result = await _task_detail(task)
        if api_key_copy:
            result["api_key"] = self._api_key_response(api_key_copy)
        self._emit_task_created(task)
        return result

    async def _dispatch_created_task(
        self,
        task: Task,
        req: dict,
        api_key_copy: dict | None,
        *,
        notify_created: bool = True,
    ) -> dict:
        """Dispatch a freshly created task and push its first turn.

        Extracted from create_task verbatim so the synchronous and deferred
        paths share one body. ``api_key_copy`` is the one-time plaintext minted
        in create_task (None for internal callers that pack ``_session_llm``).
        """
        client = get_local_node_client()
        node_id = (task.node_id or "").strip()
        # Reviews hold a ReviewNodeLease (their own per-node slot); normal tasks
        # track their session on the Task row itself (task.node_session_id). There
        # is no exclusive occupation row either way.
        review_lease_id = (req.get("_review_lease_id") or "").strip()

        # Public tasks (no internal _session_llm) get a gateway LLM config built
        # from the freshly-minted task child key. The plaintext lives only in
        # the in-memory request flow + the node payload, never in snapshots.
        if api_key_copy and not req.get("_session_llm"):
            req = {
                **req,
                "_session_llm": await self._task_llm_config(req, task, api_key_copy["key"]),
            }

        try:
            await self._rewrite_repo_for_git_proxy(req, task)
            session = self._build_session(req, task)
            assoc_env = await self._build_association_env(req, task)
            if assoc_env:
                # ``NodeCreateSession.env`` is the only cross-process channel that
                # reaches the node agent. The git proxy token rides here (secret)
                # instead of living in .git/config.
                session["env"] = assoc_env
            await self._attach_issue_workflow_mcp(task, req, session)
            # 任务侧创建一个普通 task principal，并把任务选中的 MCP service 授权给它。
            # mcp_users 不反向存 task 关系；task.mcp_user_id 是使用方持有的绑定。
            await self._ensure_task_mcp_principal(task)
            resp = await client.dispatch_session(node_id, session)
            if resp.get("accepted") is not True:
                raise RuntimeError(resp.get("error") or "node rejected session")
            session_id = str(resp.get("sessionId") or resp.get("session_id") or "").strip()
            if not session_id:
                raise RuntimeError("node did not return a session id")
            task.node_session_id = session_id
            if review_lease_id:
                await self._record_review_session_id(review_lease_id, session_id)
            prev_workspace_state = task.workspace_state
            prev_status = task.status
            task.workspace_state = "ready"
            # DispatchSession has no prompt slot. Non-empty content is the
            # first turn; empty content intentionally leaves the interactive
            # runtime waiting for the user to type in the task detail page.
            pushed_first_turn = False
            if content := (task.content or "").strip():
                # 与 send_task_message 同一条投递协议：幂等键先落 mc_task_events，
                # 再经 _deliver_turn 推给 runtime（状态推进由 ACK 事件驱动）。失败
                # 只影响这一轮，任务保持可输入，不把创建请求整体报错。
                first_message_id = (req.get("client_message_id") or "").strip() or str(uuid.uuid4())
                try:
                    first_row = await self._persist_user_input_item(
                        task.id, content,
                        client_message_id=first_message_id, delivery_status="pending",
                    )
                    claim, attempt = await self._claim_message_for_dispatch(task, first_row)
                    if claim == "pending":
                        await self._deliver_turn(
                            task, session_id, content, first_message_id, attempt,
                            _derive_task_title(content),
                        )
                        pushed_first_turn = True
                    else:
                        # create 请求被同 ID 重放，已有投递在途/完成；不执行第二遍。
                        pushed_first_turn = claim in ("received", "running", "completed")
                except Exception:
                    logger.exception(
                        "[monkeycode-compat] task {} initial input push failed", task.id
                    )
            # ``processing`` means "the agent is working on a turn", which is what
            # the UI renders as 运行中. A dispatched-but-idle session is NOT that:
            # with no first turn the runtime is merely up and waiting for input, so
            # calling it 运行中 the instant dispatch returns was simply wrong — and
            # it hid the phase that actually takes time (clone / runtime start),
            # which the node reports separately via runtime_stage. Stay ``pending``
            # until a turn is really in flight; ``pending`` + a session handle is
            # already the "ready for input" state the detail page keys on.
            task.status = "processing" if pushed_first_turn else "pending"
            # 条件更新：node_session_id 仍为空才写。与恢复 worker 的 stale 接管
            # （见 task_runtime_worker）竞争时只允许一个赢家写句柄；输家以 DB 为
            # 准重读，节点侧 "already exists" 幂等让两句柄同源收敛。
            updated = await Task.filter(id=task.id, node_session_id__isnull=True).update(
                node_session_id=task.node_session_id,
                status=task.status,
                workspace_state="ready",
                updated_at=_utcnow(),
            )
            if updated:
                await self._record_status_transition(
                    task.id, "workspace_state", "ready",
                    frm=prev_workspace_state, reason="session dispatched",
                )
                await self._record_status_transition(
                    task.id, "status", task.status,
                    frm=prev_status, reason="dispatch ready" if pushed_first_turn else "ready idle",
                )
            else:
                # 输了写句柄竞争：以 DB 为准重读，不额外记录历史（赢家已记）。
                fresh = await Task.get_or_none(id=task.id)
                if fresh is not None:
                    task.node_session_id = fresh.node_session_id
                    task.status = fresh.status
                    task.workspace_state = fresh.workspace_state
        except NodeServerUnavailable as exc:
            # Control plane unreachable at the transport layer: retryable. Make
            # it visible while keeping the task pending so manual start / first
            # message / the recovery worker can dispatch once it returns.
            logger.info("[monkeycode-compat] task {} dispatch deferred: {}", task.id, exc)
            await self._mark_dispatch_failed(task, str(exc) or "节点控制面暂不可用")
            await self._disable_task_mcp_principal(task)
        except RPCError as exc:
            # The control plane answered with a structured error. Node offline /
            # heartbeat stale / transport-down surfaces as Code.UNAVAILABLE and
            # is retryable (the node comes back). Anything else — node deleted,
            # not approved, cannot run the session — is a permanent rejection.
            retryable = exc.code == Code.UNAVAILABLE
            logger.info(
                "[monkeycode-compat] task {} dispatch {} (code={}): {}",
                task.id, "deferred" if retryable else "rejected", exc.code, exc,
            )
            await self._disable_task_mcp_principal(task)
            await self._mark_dispatch_failed(
                task,
                self._dispatch_rpc_error_message(exc),
                terminal=not retryable,
            )
        except Exception as exc:
            logger.exception("[monkeycode-compat] task {} orchestration failed", task.id)
            await self._disable_task_mcp_principal(task)
            await self._mark_dispatch_failed(task, str(exc) or "派发异常", terminal=True)
        if (getattr(task, "workspace_state", "") or "") == "dispatch_failed" and (
            getattr(task, "status", "") or "") == "error":
            # Terminal dispatch failure: the pre-persisted first turn (deferred
            # path) would linger as ``pending`` forever — mark it so replay
            # shows an undelivered message instead of a phantom sending one.
            await self._cancel_pending_first_turn(task)
        result = await _task_detail(task)
        if api_key_copy:
            result["api_key"] = self._api_key_response(api_key_copy)
        # notify_created=False for the deferred path: the create response has
        # already emitted task.created, emitting twice would duplicate the
        # in-app row + push. The synchronous path (webhook review, issue flow)
        # still emits here as before.
        if notify_created:
            self._emit_task_created(task)
        return result

    @staticmethod
    async def _cancel_pending_first_turn(task: Task) -> None:
        """Mark the pre-persisted first user_input row as cancelled.

        Only the deferred-create path leaves a durable ``pending`` row (it is
        persisted before dispatch so a crash cannot lose the message). On a
        terminal dispatch failure nothing will ever deliver it; replay must not
        render it as an in-flight send. ``cancelled`` keeps the same-ID resend
        retryable through the normal claim state machine.
        """
        try:
            await TaskEvent.filter(
                task_id=task.id,
                event_type="user_input",
                delivery_status="pending",
                client_message_id__not_isnull=True,
            ).update(delivery_status="cancelled", failure_reason="dispatch_failed")
        except Exception:  # noqa: BLE001 — diagnostics must not mask the failure path
            logger.exception("[monkeycode-compat] task {} cancel pending first turn failed", task.id)

    @staticmethod
    def _emit_task_created(task: Task) -> None:
        """Fire the task.created notification without blocking the caller.

        Backgrounded, not awaited: a slow or hanging webhook must not add
        latency to task creation. task.ended is fired from node_server's
        finalizer instead, since that is where the terminal state is decided.
        Goes through the unified notify domain: emit writes the in-app row and
        an outbox row, and the worker pushes it per subscription rule.
        """
        from .notify_core import emit_notification_background

        emit_notification_background(
            "task.created",
            params={
                "task_id": str(task.id),
                "task": task.title or (task.content or "")[:80],
                "status": task.status,
                "provider": task.provider,
            },
            owner_type="user",
            owner_id=str(task.user_id),
            severity="info",
            kind="task",
            source="task_service",
            message=f"任务「{task.title or (task.content or '')[:40]}」已创建",
        )

    @staticmethod
    async def _refresh_api_key_snapshot() -> None:
        """Reload the gateway's API Key snapshot after minting/disabling a task key.

        Auth reads the in-memory snapshot, never the DB (see
        ``Config.refresh_api_keys_cache``). A freshly minted task child key is
        therefore invisible to ``/v1/*`` until some write path reloads it —
        without this call the only thing that did was the 60s reconcile, so the
        node's first request raced it and 401'd, leaving the task with no output
        and no error. Editor child keys already refresh at their route layer; the
        task paths are the same contract. Best-effort: a failed reload must not
        fail task creation (the reconcile still converges), and
        ``refresh_api_keys_cache`` itself keeps the old snapshot on DB error
        rather than blanking it.
        """
        try:
            from config import Config

            await Config.refresh_api_keys_cache()
        except Exception as exc:
            logger.warning("[monkeycode-compat] API Key 快照刷新失败: {}", exc)

    @staticmethod
    def _api_key_response(key_copy: dict) -> dict:
        """One-time plaintext response shape for create/rotate."""
        key_value = str(key_copy.get("key") or "")
        return {
            "key_id": key_copy.get("id"),
            "key": key_value,
            "key_masked": f"{key_value[:7]}...{key_value[-4:]}" if len(key_value) > 12 else key_value,
            "version": key_copy.get("version"),
            "expires_at": key_copy.get("expires_at"),
            "usage_limit": key_copy.get("usage_limit") or {},
        }

    async def _task_llm_config(self, req: dict, task: Task, plaintext_key: str) -> dict:
        """Build the gateway LLM config for a public task runtime."""
        from . import config

        origin = (config.settings.gateway_public_url or "").strip().rstrip("/")
        endpoint = origin if task.provider == "claude" else (f"{origin}/v1" if origin else "")
        # 快照为空（不限制）时由 Key 允许集合解析默认模型；不依赖 provider CLI
        # 自己的默认值（它可能不是网关可路由模型）。
        active_model = await self._resolve_task_active_model(task, req)
        effort = _task_reasoning_effort(task)
        extra = {"REASONING_EFFORT": effort} if effort else {}
        return {
            "endpoint": endpoint,
            "apiKey": plaintext_key,
            "model": active_model,
            "protocol": "responses" if task.provider == "codex" else "chat",
            "extra": extra,
        }


    async def _record_review_session_id(self, review_lease_id: str, session_id: str) -> None:
        """Backfill the dispatch session id onto a webhook review's lease.

        Normal tasks store it on ``task.node_session_id``; reviews hold a slot
        on ``ReviewNodeLease`` instead, so the session id lives there.
        """
        if not review_lease_id:
            return
        from .models_review import ReviewNodeLease

        await ReviewNodeLease.filter(id=review_lease_id).update(session_id=session_id)

    @staticmethod
    async def _effective_git_identity_id(task_id: Any) -> uuid.UUID | None:
        """The git identity authorizing a task's repo (task binding, else project)."""
        from .models_project import Project

        binding = await ProjectTask.filter(task_id=task_id).order_by("created_at").first()
        if binding is None:
            return None
        if binding.git_identity_id:
            return binding.git_identity_id
        if binding.project_id:
            project = await Project.filter(id=binding.project_id).first()
            if project is not None and project.git_identity_id:
                return project.git_identity_id
        return None

    async def _rewrite_repo_for_git_proxy(self, req: dict, task: Task) -> None:
        """Rewrite a private-repo task's clone URL to the server-side git proxy.

        Only applies when the task carries a git identity (directly or via its
        project) and the control-plane public URL is configured — a public repo
        or a missing origin keeps the existing direct-clone behavior. The real
        username/password/token are stripped from the repo spec so the node never
        receives a usable git credential.

        The main repo is minted ``rw`` (the agent is expected to push its work
        back); associated repos get their own per-scope tokens in
        :meth:`_build_association_env`.

        The clone URL embeds the real repo's host-relative path after ``/git/r/``
        so ``git clone --recurse-submodules`` resolves a relative ``.gitmodules``
        url back into the proxy's ``r/`` namespace; the submodule allowlist
        derived from ``.gitmodules`` is persisted to ``config_snapshot`` where
        the proxy's ``_resolve`` reads it (nothing node-supplied is trusted).
        """
        repo = req.get("repo")
        if not isinstance(repo, dict):
            return
        repo_url = (repo.get("repo_url") or "").strip()
        if not repo_url:
            return
        # Only http(s) repos go through the smart-HTTP proxy. An ssh remote
        # authenticates with keys, not a URL credential, so leave it untouched.
        if not repo_url.startswith(("https://", "http://")):
            return
        if not _git_proxy_origin():
            return
        # No shared control token ⇒ the proxy keys its HMAC on an empty secret
        # and refuses (503). Keep the existing direct-clone behavior instead of
        # shipping a URL the node can't use.
        if not _git_proxy_control_token():
            return
        if await self._effective_git_identity_id(task.id) is None:
            return
        rewritten = dict(repo)
        rewritten["repo_url"] = _git_proxy_url(
            str(task.id), _GIT_PROXY_SCOPE_MAIN, _GIT_PROXY_MODE_RW,
            repo_path=_host_relative_path(repo_url),
        )
        for credential_field in ("username", "password", "token"):
            rewritten.pop(credential_field, None)
        req["repo"] = rewritten
        # Best-effort: a .gitmodules fetch failure must not stop the main clone
        # from being rewritten — the proxy then just rejects non-parent r/ paths.
        try:
            await self._persist_submodule_allowlist(task, repo_url, (repo.get("branch") or "").strip())
        except Exception:  # noqa: BLE001
            logger.exception("[monkeycode-compat] task {} submodule allowlist build failed", task.id)

    async def _persist_submodule_allowlist(self, task: Task, repo_url: str, branch: str) -> None:
        """Derive the ``.gitmodules`` submodule allowlist and store it for the proxy.

        The proxy must decide — without trusting the node — whether a
        ``/git/r/<path>`` request is a submodule fetch this task's repo actually
        declared. The only source of truth is the repo's own ``.gitmodules``, so
        the gateway reads it once at dispatch with the task's git identity and
        persists the resolved same-host paths into ``config_snapshot`` (key
        ``submodule_paths``), where the proxy's ``_resolve`` picks them up.

        Best-effort: a fetch/parse failure leaves the key unset, and the proxy
        then rejects every non-parent ``r/`` path (fail closed) while the main
        clone keeps working (it matches the task's own repo path).
        """
        import base64 as _base64

        from . import git_clients
        from .git_service import git_service
        from .project_service import (
            _derive_full_name,
            _resolve_submodule_url,
            parse_gitmodules,
        )

        identity_id = await self._effective_git_identity_id(task.id)
        if identity_id is None:
            return
        identity = await git_service.load_identity_for_read(str(identity_id))
        if identity is None or not identity.access_token:
            return
        platform = (identity.platform or "").lower()
        if not git_clients.supports_platform(platform):
            return
        full_name = _derive_full_name(repo_url)
        if not full_name:
            return
        opts = git_service._build_repo_options(identity)
        try:
            blob = await git_clients.fetch_blob(
                platform, full_name, opts, path=".gitmodules", ref=branch
            )
        except git_clients.GitClientError as exc:
            logger.info(
                "[monkeycode-compat] task {} .gitmodules fetch failed (submodule proxy disabled): {}",
                task.id, exc,
            )
            return
        if blob is None or not blob.content:
            return
        try:
            text = _base64.b64decode(blob.content, validate=True).decode("utf-8", errors="replace")
        except (ValueError, UnicodeDecodeError):
            return
        sections = parse_gitmodules(text)
        if not sections:
            return

        parent = urlparse(repo_url.strip())
        parent_authority = f"{parent.scheme.lower()}://{parent.netloc.lower()}"
        paths: list[str] = []
        for section in sections:
            sub_url = _resolve_submodule_url(repo_url, section["url"])
            parsed = urlparse(sub_url)
            if f"{parsed.scheme.lower()}://{parsed.netloc.lower()}" != parent_authority:
                # A different host cannot be served by the proxy (it would have to
                # forward to an arbitrary upstream) — left to git's direct fetch.
                continue
            sub_path = (parsed.path or "").strip("/")
            if sub_path:
                paths.append(sub_path)
        if not paths:
            return

        snapshot = dict(task.config_snapshot or {}) if isinstance(task.config_snapshot, dict) else {}
        snapshot["submodule_paths"] = sorted(set(paths))
        task.config_snapshot = snapshot
        try:
            await Task.filter(id=task.id).update(config_snapshot=snapshot)
        except Exception:  # noqa: BLE001 - the main clone must not depend on this
            logger.exception(
                "[monkeycode-compat] task {} submodule allowlist persist failed", task.id
            )

    async def _build_association_env(self, req: dict, task: Task) -> list[dict]:
        """Build ``NodeCreateSession.env`` entries for the task project's associations.

        For each association A→B of the task's project:
          * B reachable with A's git identity → an ``AI_LUBRICANT_DEP_GIT_<i>`` JSON
            entry carrying the proxy clone URL (scope=B's project id, mode from
            B's write capability) plus the workspace subdirectory to clone into.
          * B unreachable → an ``AI_LUBRICANT_DEP_PLACEHOLDER_<i>`` entry so the
            node materializes a named placeholder directory instead of cloning.

        Returns ``[]`` when the task has no project, no associations, or the proxy
        is not configured. Secrets are flagged so the node/control plane can redact
        them in logs.
        """
        project_id = (req.get("extra") or {}).get("project_id")
        if not project_id:
            return []
        if not _git_proxy_origin() or not _git_proxy_control_token():
            return []

        from .models_project import Project, ProjectAssociation

        source = await Project.filter(id=_maybe_uuid(project_id)).first()
        if source is None:
            return []
        rows = await ProjectAssociation.filter(source_project_id=source.id).order_by("created_at")
        if not rows:
            return []

        # Imported lazily: project_service imports task_service at module load,
        # so a top-level import here would be circular.
        from .project_service import project_service

        env: list[dict] = []
        dep_index = 0
        placeholder_index = 0
        for assoc in rows:
            target = await Project.filter(id=assoc.target_project_id).first()
            if target is None:
                continue
            # Sanitize the stored subdir too, not just the derived one: the row is
            # user-supplied, and a separator/traversal value must never reach the
            # node's workspace join (the node re-checks, but do not emit it).
            subdir = _safe_subdir(assoc.target_subdir) if (assoc.target_subdir or "").strip() else _safe_subdir(target.name)
            try:
                can_read, can_write = await project_service._probe_target_capability(source, target)
            except Exception:  # noqa: BLE001 - a probe failure must not block dispatch
                can_read, can_write = False, False
            if not can_read:
                env.append({
                    "name": f"AI_LUBRICANT_DEP_PLACEHOLDER_{placeholder_index}",
                    "value": f"{target.name}|{subdir}",
                    "secret": False,
                })
                placeholder_index += 1
                continue
            mode = _GIT_PROXY_MODE_RW if can_write else _GIT_PROXY_MODE_RO
            spec = {
                "url": _git_proxy_url(str(task.id), str(target.id), mode),
                "branch": target.branch or "",
                "subdir": subdir,
                "mode": mode,
            }
            env.append({
                "name": f"AI_LUBRICANT_DEP_GIT_{dep_index}",
                "value": json.dumps(spec, ensure_ascii=False),
                "secret": True,
            })
            dep_index += 1
        return env

    def _build_session(self, req: dict, task: Task) -> dict:
        """Build the agent-compose CreateSession payload ("下发参数包").

        ``provider`` is required by the node (it rejects an empty one); it is
        derived from the C-side ``cli_name``. The session is ``interactive`` so
        the initial task content can be pushed as a ``human_message`` turn — the
        dispatch message itself has no prompt field. Repo url/branch map onto the
        node git spec; the node self-provisions the workspace (no files pushed).
        """
        provider = (req.get("provider") or "").strip().lower() or _CLI_TO_PROVIDER.get(
            (req.get("cli_name") or "").strip().lower(), _DEFAULT_PROVIDER
        )
        session: dict[str, Any] = {
            "sessionId": str(task.id),
            "taskId": str(task.id),
            "projectId": str((req.get("extra") or {}).get("project_id") or ""),
            "provider": provider,
            "interactive": True,
            "tags": {},
        }
        # Execution-environment isolation tier. Stored on the task's
        # config_snapshot at create time. The node refuses env_mode=system
        # unless it opted in; shared requires env_id and the node resolves it to
        # a local dir. Empty/isolated is the historic default (per-session home).
        cfg = task.config_snapshot if isinstance(task.config_snapshot, dict) else {}
        env_mode = str(cfg.get("env_mode") or "").strip().lower()
        if env_mode and env_mode not in {"isolated", "shared", "system"}:
            raise ValueError(f"unsupported env_mode {env_mode!r}")
        if env_mode and env_mode != "isolated":
            env_id = str(cfg.get("env_id") or "").strip()
            if env_mode == "shared" and not env_id:
                raise ValueError("env_mode=shared requires env_id")
            session["envMode"] = env_mode
            if env_id:
                session["envId"] = env_id
            # Shared tier only: the task activates a SUBSET of the environment's
            # installed resources. An empty/absent list means "everything the
            # environment has" (the node enumerates it). These names never
            # install or remove anything — that is the environment's job — they
            # just pick what this run turns on, so a task deactivating a skill
            # cannot uninstall it for other tasks on the same environment.
            if env_mode == "shared":
                active_skills = cfg.get("active_skills")
                active_plugins = cfg.get("active_plugins")
                if isinstance(active_skills, list) and active_skills:
                    session["activeSkills"] = [str(s) for s in active_skills if str(s).strip()]
                if isinstance(active_plugins, list) and active_plugins:
                    session["activePlugins"] = [str(s) for s in active_plugins if str(s).strip()]
        # Internal callers (webhook review, issue workflow) may attach a reviewed
        # capability bundle. Public CreateTaskReq does not expose these underscore
        # keys, so an API client cannot inject arbitrary skill/MCP endpoints.
        for key, target in (
            ("_session_skills", "skills"),
            ("_session_mcps", "mcps"),
            ("_session_plugins", "plugins"),
        ):
            items = req.get(key)
            if isinstance(items, list):
                session[target] = [dict(item) for item in items if isinstance(item, dict)]
        mode = (req.get("mode") or "").strip()
        if mode:
            session["mode"] = mode
        model_id = req.get("model_id")
        if model_id:
            session["model"] = str(model_id)
        # Internal callers may pin the LLM config (endpoint/apiKey/model) for this
        # run — webhook review uses it to plant its own limited child key. The
        # node maps it onto the CLI's environment.
        llm = req.get("_session_llm")
        if isinstance(llm, dict) and llm:
            session["llm"] = dict(llm)
        repo = req.get("repo") or {}
        repo_url = repo.get("repo_url")
        if repo_url:
            branch_mode = (repo.get("branch_mode") or "default").strip()
            if branch_mode == "auto":
                # 从默认分支确定性创建 task/<task_id>，与编辑器 auto 对齐。
                git: dict[str, Any] = {
                    "url": repo_url,
                    "branch": "",
                    "createBranch": True,
                    "newBranch": f"task/{str(task.id)}",
                }
            else:
                git = {"url": repo_url, "branch": repo.get("branch") or ""}
            if repo.get("commit"):
                git["commit"] = repo["commit"]
            for credential_field in ("username", "password", "token"):
                value = repo.get(credential_field)
                if value:
                    git[credential_field] = value
            session["git"] = git
        return session

    @staticmethod
    def _dispatch_rpc_error_message(exc: RPCError) -> str:
        """Translate structured dispatch failures without hiding their cause."""
        message = (exc.message or str(exc) or "").strip()
        lower = message.lower()
        if exc.code == Code.UNAVAILABLE:
            if "offline" in lower:
                return f"执行节点离线（{message}）"
            if "heartbeat stale" in lower:
                return f"执行节点心跳超时（{message}）"
            if "disconnected" in lower:
                return f"执行节点连接中断（{message}）"
            return f"节点运行时暂不可用（{message or 'unavailable'}）"
        if exc.code == Code.NOT_FOUND:
            return f"执行节点或会话不存在（{message or 'not found'}）"
        if exc.code in (Code.PERMISSION_DENIED, Code.FAILED_PRECONDITION):
            return f"执行节点拒绝派发（{message or exc.code}）"
        return f"任务派发失败（{message or exc.code}）"

    async def _mark_dispatch_failed(
        self, task: Task, message: str, *, terminal: bool = False
    ) -> None:
        """Persist a dispatch failure so its reason is visible in the UI.

        The failure is recorded as ``workspace_state='dispatch_failed'`` plus the
        ``dispatch_error`` text the detail page renders. ``terminal`` selects the
        task status: a retryable failure (node offline / control plane down)
        stays ``pending`` so the recovery worker and the retry button can
        re-dispatch once the node returns; a permanent rejection (node deleted /
        not approved / cannot run the session) goes ``error``. ``workspace_state``
        is set either way so the reason is never hidden behind a plain status.

        A terminal failure also parks the task child key (error is a terminal
        state for the key lifecycle, same as task_events finishing it).
        """
        snapshot = dict(task.config_snapshot or {}) if isinstance(task.config_snapshot, dict) else {}
        snapshot["dispatch_error"] = (message or "节点控制面暂不可用")[:1000]
        task.config_snapshot = snapshot
        prev_workspace_state = task.workspace_state
        prev_status = task.status
        task.workspace_state = "dispatch_failed"
        task.status = "error" if terminal else "pending"
        await task.save(
            update_fields=["config_snapshot", "workspace_state", "status", "updated_at"]
        )
        # Both machines moved in one save; record them separately so a trail read
        # can ask "when did workspace_state fail?" independently of the status the
        # failure was classified as (retryable → pending, permanent → error).
        await self._record_status_transition(
            task.id, "workspace_state", "dispatch_failed",
            frm=prev_workspace_state, reason=snapshot["dispatch_error"],
        )
        await self._record_status_transition(
            task.id, "status", task.status, frm=prev_status,
            reason=("terminal: " if terminal else "retryable: ") + snapshot["dispatch_error"],
        )
        if terminal:
            await self._park_task_api_key(task)

    @staticmethod
    async def _record_runtime_error(task: Task, reason: str) -> None:
        """Persist why a session died so the task page can state the cause.

        Reuses the ``dispatch_error`` snapshot slot the detail page already
        renders: from the user's point of view "派发失败" and "运行环境启动失败"
        are the same question ("why can't my task run, and what do I do?"), so
        they share one display path instead of adding a parallel field.
        ``workspace_state`` is left alone — the runtime *was* dispatched; it is
        the run that failed, and overwriting it would lose that distinction.
        """
        reason = (reason or "").strip()
        if not reason:
            return
        snapshot = dict(task.config_snapshot or {}) if isinstance(task.config_snapshot, dict) else {}
        snapshot["dispatch_error"] = reason[:1000]
        task.config_snapshot = snapshot
        try:
            await Task.filter(id=task.id).update(config_snapshot=snapshot)
        except Exception:  # noqa: BLE001
            logger.exception(
                "[monkeycode-compat] task {} runtime error persist failed", task.id
            )

    @staticmethod
    async def _clear_dispatch_failed(task: Task) -> dict:
        """Return config snapshot with transient dispatch metadata removed."""
        snapshot = dict(task.config_snapshot or {}) if isinstance(task.config_snapshot, dict) else {}
        snapshot.pop("dispatch_error", None)
        snapshot.pop("dispatch_retry_at", None)
        task.config_snapshot = snapshot
        return snapshot

    @staticmethod
    async def _clear_runtime_stage(task: Task) -> None:
        """Reset the reported bring-up step before a fresh run begins.

        Stage values describe *one* attempt. Without clearing them, a retry keeps
        rendering the previous failure ("卡在「检查运行环境」") until the node's
        first new stage frame lands — so the button appears to do nothing, or
        worse, to have failed again instantly. Cleared here rather than in the
        node path because only the gateway knows a new attempt is starting.
        """
        task.runtime_stage = ""
        task.runtime_stage_detail = None
        task.runtime_stage_ok = True
        try:
            await Task.filter(id=task.id).update(
                runtime_stage="", runtime_stage_detail=None, runtime_stage_ok=True
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "[monkeycode-compat] task {} runtime stage clear failed", task.id
            )

    async def _dispatch_persisted_task_runtime(self, task: Task) -> str:
        """Dispatch a storage-state task without replaying its stored content.

        Creation intentionally persists the task before talking to the node. If
        the control plane is unavailable at that moment, the same row can be
        started later by this helper. Session ids are task ids, and dispatch is
        serialized per task; the conditional update also prevents an older
        request from overwriting a session persisted by another API process.
        """
        lock = self._runtime_dispatch_lock(task.id)
        async with lock:
            current = await Task.get_or_none(id=task.id)
            if current is None:
                raise ValueError("task_not_found")
            existing = (current.node_session_id or "").strip()
            if existing:
                # The DB handle outlives the control plane's session binding: a
                # node restart tears the binding down, but the Task row still
                # points at the dead handle. Trusting it makes start / first
                # message return a phantom session that 404s on the first send.
                # Probe the binding before short-circuiting:
                # - NOT_FOUND: the binding row is gone (node deleted / cascade).
                # - ok=False ack: the binding row still exists but the node
                #   cannot place the session (node restarted and lost its
                #   in-memory session map). The control plane answers HTTP 200
                #   with a not-ok ack rather than raising, so the absence must be
                #   detected from the ack body, not from an exception.
                # Both mean the handle is stale: clear it and re-dispatch.
                # UNAVAILABLE means the node is offline (handle still valid, 503).
                client = get_local_node_client()
                if not client.enabled:
                    raise ValueError("node_server_unavailable")
                stale_handle = False
                try:
                    probe_ack = await client.start_node_session_runtime(existing)
                except NodeServerUnavailable as exc:
                    # Control plane unreachable while probing an existing handle:
                    # transport raises NodeServerUnavailable, not RPCError, so the
                    # ``except RPCError`` arm below would let it escape — surfacing
                    # as a 500 with no reason. Map to the same retryable state.
                    raise ValueError("node_server_unavailable") from exc
                except RPCError as exc:
                    if exc.code == Code.UNAVAILABLE:
                        raise ValueError("node_server_unavailable") from exc
                    if exc.code != Code.NOT_FOUND:
                        raise
                    stale_handle = True
                    logger.info(
                        "[monkeycode-compat] task {} handle {} stale (binding gone); re-dispatching",
                        current.id, existing,
                    )
                else:
                    if isinstance(probe_ack, dict) and probe_ack.get("ok") is False:
                        stale_handle = True
                        logger.info(
                            "[monkeycode-compat] task {} handle {} stale (node ack not ok: {}); re-dispatching",
                            current.id, existing,
                            (probe_ack.get("error") or "").strip() or "node reported failure",
                        )
                if stale_handle:
                    await Task.filter(
                        id=current.id, node_session_id=existing
                    ).update(node_session_id=None)
                    current.node_session_id = None
                    task.node_session_id = None
                else:
                    # StartSessionRuntime is idempotent for an already-running
                    # session and restarts a stopped one. A previous result/stop
                    # disabled the task principal, so re-enable it before the
                    # next message enters the runtime.
                    await self._enable_task_mcp_principal(current)
                    # Same closed loop for the task child key: finished/error
                    # parked it; resume before _deliver_turn re-reads the row
                    # and pushes a fresh LLM config, or the next turn
                    # authenticates with a disabled key.
                    await self._resume_task_api_key(current)
                    return existing
            # 生命周期边界只有「删除」：任务行存在即允许（重新）派发 runtime。
            # 过去这里白名单 pending/processing/error——finished（上一段 runtime
            # 正常退出/取消后进程退出的误标）/stopped（用户手动停止）的任务被
            # 永久锁死，与「发送消息即恢复」的产品语义冲突。结果记录
            # （completed_at/错误原因）保留在行上，下一条消息成功投递后状态自然
            # 回到 processing。软删行（deleted_at 非空的历史遗留）不可恢复。
            if getattr(current, "deleted_at", None) is not None:
                raise ValueError("task_not_startable")
            is_retry = current.status == "error" and current.workspace_state == "dispatch_failed"

            node_id = (current.node_id or "").strip()
            if not node_id:
                raise ValueError("node_required")
            if await nodes_service.user_can_use_node(str(current.user_id), node_id) is None:
                raise ValueError("node_forbidden")
            config_snapshot = current.config_snapshot if isinstance(current.config_snapshot, dict) else {}
            await self._require_system_env_allowed(node_id, config_snapshot.get("env_mode"))

            client = get_local_node_client()
            if not client.enabled:
                raise ValueError("node_server_unavailable")
            # Resume before the availability check below rejects a parked key:
            # finished/error disabled it, and a retried error task reaches here
            # through start / first-message / the recovery worker.
            await self._resume_task_api_key(current)
            if not current.api_key_id:
                raise ValueError("task_api_key_unavailable")

            from db import PostgresClient

            key_row = await PostgresClient.get_api_key_by_id(int(current.api_key_id))
            if not key_row or key_row.get("disabled") or not key_row.get("key"):
                raise ValueError("task_api_key_unavailable")

            binding = await ProjectTask.filter(task_id=current.id).order_by("created_at").first()
            model_id = ""
            if binding and binding.model_id and binding.model_id.int:
                model_id = str(binding.model_id)
            req: dict[str, Any] = {
                "provider": current.provider,
                "cli_name": (binding.cli_name if binding else "") or current.provider,
                "mode": current.mode or "",
                "model_id": model_id,
                "models": list(current.models_snapshot or []),
                "extra": {
                    **({"project_id": str(binding.project_id)} if binding and binding.project_id else {}),
                    **({"issue_id": str(binding.issue_id)} if binding and binding.issue_id else {}),
                },
                "repo": {
                    "repo_url": binding.repo_url if binding else "",
                    "branch_mode": "existing" if binding and binding.branch else "default",
                    "branch": binding.branch if binding else "",
                },
                "_session_skills": list(current.skill_config or []),
                "_session_mcps": list(current.mcp_config or []),
                "_session_plugins": list(current.plugin_config or []),
            }
            req["_session_llm"] = await self._task_llm_config(
                req, current, str(key_row["key"])
            )

            try:
                await self._rewrite_repo_for_git_proxy(req, current)
                session = self._build_session(req, current)
                assoc_env = await self._build_association_env(req, current)
                if assoc_env:
                    session["env"] = assoc_env
                await self._attach_issue_workflow_mcp(current, req, session)
                await self._ensure_task_mcp_principal(current)
                # 恢复派发会重启 runtime：finalize/stop 曾禁用过 principal，这里
                # 重新启用，否则恢复后的任务 MCP 授权全部失效。
                await self._enable_task_mcp_principal(current)
                response = await client.dispatch_session(node_id, session)
                if response.get("accepted") is not True:
                    raise RuntimeError(response.get("error") or "node rejected session")
                session_id = str(
                    response.get("sessionId") or response.get("session_id") or ""
                ).strip()
                if not session_id:
                    raise RuntimeError("node did not return a session id")
            except NodeServerUnavailable as exc:
                await self._disable_task_mcp_principal(current)
                await self._mark_dispatch_failed(current, str(exc) or "节点控制面暂不可用")
                raise ValueError("node_server_unavailable") from exc
            except RPCError as exc:
                # Mirror create_task: node offline / transport-down is retryable
                # (pending); a permanent RPC rejection terminalizes so the
                # recovery worker doesn't loop on it forever.
                retryable = exc.code == Code.UNAVAILABLE
                logger.info(
                    "[monkeycode-compat] task {} delayed dispatch {} (code={}): {}",
                    current.id, "deferred" if retryable else "rejected", exc.code, exc,
                )
                await self._disable_task_mcp_principal(current)
                await self._mark_dispatch_failed(
                    current,
                    self._dispatch_rpc_error_message(exc),
                    terminal=not retryable,
                )
                raise ValueError("node_server_unavailable" if retryable else "runtime_dispatch_failed") from exc
            except Exception as exc:
                logger.exception(
                    "[monkeycode-compat] task {} delayed dispatch failed", current.id
                )
                await self._disable_task_mcp_principal(current)
                await self._mark_dispatch_failed(current, str(exc) or "派发异常", terminal=True)
                raise ValueError("runtime_dispatch_failed") from exc

            cleared = await self._clear_dispatch_failed(current)
            updated = await Task.filter(
                id=current.id, node_session_id__isnull=True
            ).update(
                node_session_id=session_id,
                workspace_state="ready",
                # This path pushes no first turn — it re-dispatches a stored task
                # so the runtime comes back up and waits for input. Writing
                # ``processing`` here claimed the agent was working when nothing
                # had been asked of it yet; keep it ``pending`` and let the first
                # message move it (see create_task's pushed_first_turn).
                status="pending",
                config_snapshot=cleared,
                # A previous attempt may have left a failed step on the row. Reset
                # it, or the page would keep reporting "卡在检查运行环境" for a
                # session that is being brought up again right now; the node
                # re-reports its stages as it goes.
                runtime_stage="",
                runtime_stage_detail=None,
                runtime_stage_ok=True,
            )
            if not updated:
                winner = await Task.get_or_none(id=current.id)
                winner_session_id = ((winner.node_session_id if winner else None) or "").strip()
                if winner_session_id:
                    return winner_session_id
                raise ValueError("runtime_dispatch_conflict")

            task.node_session_id = session_id
            task.workspace_state = "ready"
            task.status = "pending"
            task.runtime_stage = ""
            task.runtime_stage_detail = None
            task.runtime_stage_ok = True
            return session_id

    async def start_task_runtime(
        self, user_id: str, task_id: str, *, role: str | None = None
    ) -> dict:
        """Idempotently start a pending task that has no node session yet."""
        task = await self._owned_task_or_raise(user_id, task_id, role)
        session_id = await self._dispatch_persisted_task_runtime(task)
        return {
            "started": True,
            "task_id": str(task.id),
            "node_session_id": session_id,
        }

    async def _attach_issue_workflow_mcp(self, task: Task, req: dict, session: dict) -> None:
        """Attach the session-scoped issue workflow MCP to issue-backed tasks.

        The identity token is bound to exactly this task id. The MCP plugin
        derives ProjectTask.issue_id server-side; no issue id is supplied by the
        agent, preventing cross-issue writes.
        """
        issue_id = _maybe_uuid((req.get("extra") or {}).get("issue_id"))
        if issue_id is None:
            return
        try:
            import builtin_tool_store
            from monkeycode_compat.config import settings

            _row, token = await builtin_tool_store.issue_token(
                "agent", str(task.id), display_token=False
            )
        except Exception:
            logger.exception("[monkeycode-compat] issue-workflow token issuance failed")
            return
        base = (getattr(settings, "server_url", "") or "").rstrip("/")
        if not base:
            return
        entry = {
            "name": "issue-workflow",
            "type": "sse",
            "url": f"{base}/mcp/issue-workflow/sse?token={token}",
        }
        # Keep per-task servers outside the user-managed base config. A later
        # resource PATCH merges this overlay back before resync, so it cannot
        # accidentally remove the issue-scoped capability.
        task.mcp_overlay_json = _merge_mcp_config(task.mcp_overlay_json, [entry])
        await task.save(update_fields=["mcp_overlay_json", "updated_at"])
        session["mcps"] = _merge_mcp_config(session.get("mcps"), task.mcp_overlay_json)

    async def _owned_task(self, user_id: str, task_id: str, role: str | None) -> Task | None:
        try:
            tid = uuid.UUID(task_id)
        except (ValueError, TypeError):
            return None
        task = await Task.get_or_none(id=tid)
        if task is None:
            return None
        if not self._is_privileged(role) and str(task.user_id) != str(user_id):
            return None
        return task

    async def _ensure_task_mcp_principal(self, task: Task) -> None:
        """Create the task-scoped MCP principal and grant it the task's MCPs.

        The principal is an ordinary ``usage_type='task'`` mcp_user: same token
        hash + grant auth path as agent/external, only invisible + lifecycle-
        managed. ``mcp_users`` holds no reverse task pointer; the binding lives
        on ``Task.mcp_user_id``. The token plaintext is never returned to any
        task DTO/frontend. Best-effort: a failure here must not abort dispatch —
        the task still runs; the principal is simply absent.

        ``extra.builtin_tool_params`` (``[{param_key, param_value}]`` naming a
        builtin resource row id — cdp_client_id / mail_account_id / device_id)
        binds the task principal to the user's specific builtin tool instances;
        ``set_principal_params`` re-validates ownership server-side, so a stale
        or foreign id fails the binding without granting anything.
        """
        import mcp_plugin_store

        owner = str(task.user_id)
        service_ids = await self._task_mcp_service_ids(task)
        builtin_params = self._task_builtin_tool_params(task)
        existing = getattr(task, "mcp_user_id", None)
        if existing:
            usage = await mcp_plugin_store.get_mcp_principal_usage_type(existing)
            if usage != "task":
                # Stale id pointing at a non-task row: drop the binding and
                # mint a fresh task principal below.
                task.mcp_user_id = None
            elif service_ids or builtin_params:
                if service_ids:
                    try:
                        await mcp_plugin_store.set_services_for_mcp_user(existing, service_ids)
                    except Exception:
                        logger.exception(
                            "[monkeycode-compat] task {} principal service refresh failed", task.id
                        )
                if builtin_params:
                    await self._apply_task_builtin_tool_params(existing, builtin_params, owner)
                await mcp_plugin_store.set_principal_enabled(existing, True)
                return
            else:
                await mcp_plugin_store.set_principal_enabled(existing, True)
                return
        try:
            principal_id = await mcp_plugin_store.create_task_mcp_principal(
                owner, f"task-{str(task.id)[:8]}",
                description=f"task principal for {task.id}",
            )
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} principal create failed", task.id
            )
            return
        if service_ids:
            try:
                await mcp_plugin_store.set_services_for_mcp_user(principal_id, service_ids)
            except Exception:
                logger.exception(
                    "[monkeycode-compat] task {} principal service assign failed", task.id
                )
        if builtin_params:
            await self._apply_task_builtin_tool_params(principal_id, builtin_params, owner)
        task.mcp_user_id = principal_id
        try:
            await task.save(update_fields=["mcp_user_id", "updated_at"])
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} mcp_user_id persist failed", task.id
            )

    @staticmethod
    def _task_builtin_tool_params(task: Task) -> list[dict]:
        """Read the task's builtin tool instance bindings off ``extra``."""
        extra = getattr(task, "extra", None)
        raw = extra.get("builtin_tool_params") if isinstance(extra, dict) else None
        if not isinstance(raw, list):
            return []
        out: list[dict] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("param_key") or "").strip()
            value = str(entry.get("param_value") or "").strip()
            if key and value:
                out.append({"param_key": key, "param_value": value})
        return out

    @staticmethod
    async def _apply_task_builtin_tool_params(principal_id: int, params: list[dict], owner: str) -> None:
        """Bind builtin tool instances to the task principal (best-effort).

        ``set_principal_params`` verifies each value names an enabled
        ``builtin_tool_resources`` row owned by ``owner`` — an invalid or
        foreign id raises and only that binding is absent, never the dispatch.
        """
        import mcp_plugin_store

        try:
            await mcp_plugin_store.set_principal_params(
                int(principal_id), params, owner_user_id=owner,
            )
        except Exception:
            logger.exception(
                "[monkeycode-compat] task principal {} builtin tool params failed", principal_id
            )

    async def _task_mcp_service_ids(self, task: Task) -> list[int]:
        """Resolve the task's MCP config into authorized service ids.

        The persisted ``mcp_config`` carries server-trusted wire specs (already
        filtered by ``resolve_reference_specs`` against the user's authorization),
        so every entry is one the user is authorized to use. Specs carrying an
        explicit ``service_id`` (the direct mcp_services binding the create-task
        dialog sends for builtin/admin/personal MCPs) map straight through;
        reference-resolved entries map by the spec ``name``. Issue-workflow
        overlay entries are local session servers and are skipped (they are not
        ``mcp_services`` rows).

        服务级授权走 mcp_service_users（principal ↔ service）；principal 的内置工具
        资源绑定（CDP 客户端 / 邮箱账户）走 mcp_user_params，与此无关。
        """
        import mcp_plugin_store

        service_ids: list[int] = []
        seen: set[int] = set()

        def _collect(service_id_raw: object) -> None:
            try:
                service_id = int(service_id_raw)
            except (TypeError, ValueError):
                return
            if service_id and service_id not in seen:
                seen.add(service_id)
                service_ids.append(service_id)

        mcp_config = getattr(task, "mcp_config", None) or []
        for spec in list(mcp_config):
            if not isinstance(spec, dict):
                continue
            direct_id = spec.get("service_id")
            if direct_id is not None and str(direct_id).strip():
                _collect(direct_id)
                continue
            name = str(spec.get("name") or "").strip()
            if not name:
                continue
            service = await mcp_plugin_store.get_service_by_name(name)
            if not service:
                # issue-workflow and other per-task overlays are not catalog
                # services; they ride the session directly, no authorization needed.
                continue
            _collect(service["id"])
        return service_ids

    async def _disable_task_mcp_principal(self, task: Task) -> None:
        """Close the security loop on task pause/finish/error: disable principal.

        Idempotent — safe to call repeatedly across stop/finish/error. The row
        is retained (disabled) so the same principal can be re-enabled on
        restart without re-minting grants.
        """
        import mcp_plugin_store

        principal_id = getattr(task, "mcp_user_id", None)
        if not principal_id:
            return
        try:
            await mcp_plugin_store.set_principal_enabled(principal_id, False)
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} principal disable failed", task.id
            )

    async def _enable_task_mcp_principal(self, task: Task) -> None:
        """Re-enable the task principal before the runtime resumes."""
        import mcp_plugin_store

        principal_id = getattr(task, "mcp_user_id", None)
        if not principal_id:
            await self._ensure_task_mcp_principal(task)
            return
        try:
            usage = await mcp_plugin_store.get_mcp_principal_usage_type(principal_id)
            if usage != "task":
                await self._ensure_task_mcp_principal(task)
                return
            await mcp_plugin_store.set_principal_enabled(principal_id, True)
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} principal enable failed", task.id
            )

    async def _delete_task_mcp_principal(self, task: Task) -> None:
        """Delete the task principal before dropping the Task row."""
        import mcp_plugin_store

        principal_id = getattr(task, "mcp_user_id", None)
        if not principal_id:
            return
        try:
            await mcp_plugin_store.delete_mcp_principal(principal_id)
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} principal delete failed", task.id
            )
        task.mcp_user_id = None

    async def _park_task_api_key(self, task: Task) -> None:
        """Disable the bound task child key on a terminal state (finished/error).

        Unlike the explicit revoke (``disable_task_api_key``), the
        ``api_key_id`` binding is kept so a later resume re-enables the same
        key. Idempotent and best-effort like the principal helpers: a failure
        here must not break the status transition itself.
        """
        key_id = getattr(task, "api_key_id", None)
        if not key_id:
            return
        try:
            from db import PostgresClient

            if await PostgresClient.set_api_key_disabled_by_id(int(key_id), True):
                await self._refresh_api_key_snapshot()
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} api key park failed", task.id
            )

    async def _resume_task_api_key(self, task: Task) -> None:
        """Re-enable the parked task child key before the runtime resumes.

        A key revoked via the explicit endpoint clears ``api_key_id``, so it
        never reaches this path; only a lifecycle-parked key flips back.
        """
        key_id = getattr(task, "api_key_id", None)
        if not key_id:
            return
        try:
            from db import PostgresClient

            if await PostgresClient.set_api_key_disabled_by_id(int(key_id), False):
                await self._refresh_api_key_snapshot()
        except Exception:
            logger.exception(
                "[monkeycode-compat] task {} api key resume failed", task.id
            )

    async def update_task(
        self, user_id: str, task_id: str, fields: dict, *, role: str | None = None
    ) -> dict | None:
        """Patch task metadata and/or resource config; hot-resync live runtime.

        Resource configs (``mcp_config``/``skill_config``/``plugin_config``)
        are persisted as the authoritative base before any resync attempt, so a
        runtime that is offline, stopping, or restarting still picks up the new
        config on its next dispatch/restart. A live runtime receives the new
        config through ``apply_node_session_*`` without a restart; the MCP base
        is merged with the task's per-task overlay so issue-workflow survives.
        """
        task = await self._owned_task(user_id, task_id, role)
        if task is None:
            return None
        changed: list[str] = []
        if "title" in fields:
            task.title = fields["title"]
            changed.append("title")
        if "summary" in fields:
            task.summary = fields["summary"]
            changed.append("summary")
        if "mode" in fields:
            task.mode = (fields["mode"] or "").strip() or None
            changed.append("mode")
        if "mode_label" in fields:
            task.mode_label = (fields["mode_label"] or "").strip() or None
            changed.append("mode_label")
        if "reasoning_effort" in fields:
            snapshot = dict(task.config_snapshot or {}) if isinstance(task.config_snapshot, dict) else {}
            snapshot["reasoning_effort"] = str(fields["reasoning_effort"] or "")
            task.config_snapshot = snapshot
            changed.append("config_snapshot")
        resource_keys = ("mcp_config", "skill_config", "plugin_config")
        for key in resource_keys:
            if key in fields:
                value = fields[key]
                setattr(
                    task, key,
                    [dict(item) for item in value if isinstance(item, dict)]
                    if isinstance(value, list) else [],
                )
                changed.append(key)
        resync: dict[str, list[str]] = {"applied": [], "failed": [], "skipped": []}
        if changed:
            changed.append("updated_at")
            await task.save(update_fields=changed)
        # MCP config changed: atomically replace the task principal's authorized
        # services so the principal stays in lock-step with the task's resolved
        # references. This re-runs the same owner-resolved path used at create time.
        if "mcp_config" in fields:
            if task.mcp_user_id:
                import mcp_plugin_store

                service_ids = await self._task_mcp_service_ids(task)
                try:
                    await mcp_plugin_store.set_services_for_mcp_user(
                        task.mcp_user_id, service_ids,
                    )
                except Exception:
                    logger.exception(
                        "[monkeycode-compat] task {} principal service replace failed",
                        task.id,
                    )
            else:
                # No principal yet (task was storage-only at create): mint one
                # now that MCP config has been set.
                await self._ensure_task_mcp_principal(task)
        # Hot-resync only the config fields the caller actually changed, and
        # only against a live runtime. Stopped/errored tasks keep the persisted
        # config; it takes effect on the next restart.
        config_changed = [k for k in resource_keys if k in fields]
        node_session_id = (task.node_session_id or "").strip()
        live = node_session_id and task.status in ("pending", "processing")
        if config_changed and not live:
            resync["skipped"].append("runtime_offline")
            return {"ok": True, "config_resync": resync}
        if config_changed:
            client = get_local_node_client()
            if not client.enabled:
                resync["skipped"].append("node_server_disabled")
            else:
                overlay = list(task.mcp_overlay_json or [])
                try:
                    if "mcp_config" in config_changed:
                        await client.apply_node_session_mcps(
                            node_session_id,
                            _merge_mcp_config(task.mcp_config, overlay),
                        )
                    if "skill_config" in config_changed:
                        await client.apply_node_session_skills(
                            node_session_id, list(task.skill_config or [])
                        )
                    if "plugin_config" in config_changed:
                        await client.apply_node_session_plugins(
                            node_session_id, list(task.plugin_config or [])
                        )
                    resync["applied"].append(node_session_id)
                except Exception as exc:  # noqa: BLE001 - 下发失败如实上报
                    resync["failed"].append(f"{node_session_id}: {exc}")
        return {"ok": True, "config_resync": resync}

    async def _stop_runtime(self, task: Task) -> None:
        """Stop the live process while preserving task placement and workspace."""
        session_id = (task.node_session_id or "").strip()
        if not session_id:
            return
        client = get_local_node_client()
        if not client.enabled:
            return
        try:
            await client.delete_node_session(session_id)
        except Exception:
            logger.exception("[monkeycode-compat] task {} runtime stop failed", task.id)
            raise
        task.node_session_id = None
        if task.workspace_state == "pending":
            task.workspace_state = "ready"
        await task.save(update_fields=["node_session_id", "workspace_state", "updated_at"])

    async def _release_node(self, task_id: uuid.UUID) -> None:
        """Free a task's live runtime and any review lease (terminal-state hook).

        Deletes the daemon session best-effort so the node stops the run, then
        drops a webhook review's ``ReviewNodeLease`` if present. Normal tasks
        track their session on ``Task.node_session_id`` (no occupation row);
        reviews hold a per-node slot on ``ReviewNodeLease``. Safe to call when
        neither exists.
        """
        from .models_review import ReviewNodeLease

        task = await Task.get_or_none(id=task_id)
        lease = await ReviewNodeLease.get_or_none(task_id=task_id, status="active")
        session_id = ((task.node_session_id if task else None) or "") or (
            (lease.session_id if lease else None) or ""
        )
        if session_id:
            client = get_local_node_client()
            if client.enabled:
                try:
                    await client.delete_node_session(session_id)
                except Exception:
                    logger.exception(
                        "[monkeycode-compat] task {} session delete failed", task_id
                    )
        if lease is not None:
            await ReviewNodeLease.filter(id=lease.id).delete()

    async def stop_task(self, user_id: str, task_id: str, *, role: str | None = None) -> bool:
        task = await self._owned_task(user_id, task_id, role)
        if task is None:
            return False
        # Stopping a task tears down only its live runtime. The placement and
        # workspace remain so the task can resume without re-creating its environment.
        await self._stop_runtime(task)
        prev = task.status
        task.status = "stopped"
        await task.save(update_fields=["status", "updated_at"])
        await self._record_status_transition(
            task.id, "status", "stopped", frm=prev, reason="user stop",
        )
        # Stop = temporary pause: tear down the live runtime, then disable the
        # task principal so its MCP grants cannot be used while the task is idle.
        await self._disable_task_mcp_principal(task)
        return True

    async def delete_task(self, user_id: str, task_id: str, *, role: str | None = None) -> bool:
        task = await self._owned_task(user_id, task_id, role)
        if task is None:
            return False
        # Release the node before dropping task rows so its session is stopped.
        await self._release_node(task.id)
        # Delete the task principal before the Task row so MCP grants do not
        # outlive the task they were minted for.
        await self._delete_task_mcp_principal(task)
        await ProjectTask.filter(task_id=task.id).delete()
        await TaskVirtualMachine.filter(task_id=task.id).delete()        # Legacy cleanup: older builds pinned an exclusive TaskNodeBinding per
        # task. The occupation model is gone, but drop any leftover row so a
        # migrated DB does not keep stale rows around.
        await TaskNodeBinding.filter(task_id=task.id).delete()
        await task.delete()
        return True

    async def _owned_task_or_raise(self, user_id: str, task_id: str, role: str | None) -> Task:
        """Resolve an owned task or raise ValueError the route maps to 404/403."""
        task = await self._owned_task(user_id, task_id, role)
        if task is None:
            raise ValueError("task_not_found")
        return task

    async def _live_node_session_id(self, task: Task) -> str:
        """Return the node runtime handle for a task or raise a ValueError.

        The task itself persists across runtime restarts; this only guards the
        endpoints that drive the live runtime (messages/cancel/restart/events).
        A stopped task with no node_session_id is rejected so callers map it to
        a 409, not a silent dispatch to a dead handle.
        """
        node_session_id = (task.node_session_id or "").strip()
        if not node_session_id:
            raise ValueError("runtime_not_bound")
        return node_session_id

    @staticmethod
    async def _clear_stale_node_session(task: Task, session_id: str, reason: str) -> None:
        """Drop a persisted handle the control plane no longer knows about.

        ``task.node_session_id`` outlives the control plane's session binding:
        deleting the node cascades its ``mc_ac_node_sessions`` rows away, and an
        explicit teardown unbinds one directly. The task row keeps pointing at
        the vanished handle, so every later drive attempt hits a ``not_found``
        RPC. Clearing it here puts the task back in the ``runtime_not_bound``
        state the recovery paths (start / first message / recovery worker)
        already know how to dispatch from.
        """
        logger.info(
            "[monkeycode-compat] task {} node session {} is stale ({}); clearing handle",
            task.id, session_id, reason,
        )
        await Task.filter(id=task.id, node_session_id=session_id).update(node_session_id=None)
        task.node_session_id = None

    async def send_task_message(
        self,
        user_id: str,
        task_id: str,
        content: str,
        *,
        attachments: list[dict] | None = None,
        role: str | None = None,
        client_message_id: str | None = None,
    ) -> dict:
        """Deliver one user turn, idempotently.

        ``client_message_id``（发送端生成的 UUID）是端到端幂等键：先落
        ``mc_task_events``（部分唯一约束兜底并发），再投递 runtime。同 ID 重复
        请求按行上已有的 delivery_status 决定行为——已 received/running/completed
        的直接回现状不重投；pending/failed/cancelled 允许用同一 ID 重投递。这样
        HTTP 超时重试、网络重放、用户重复点击都不会把同一句话执行两遍。
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        # 标题候选取自用户原话（在拼接附件块之前），空标题任务靠它补齐名字。
        title_candidate = _derive_task_title(content or "")
        content = (content or "").strip()
        workspace_paths: list[str] = []
        for item in attachments or []:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if not url.startswith("workspace://"):
                continue
            rel = url[len("workspace://"):].replace("\\", "/").lstrip("/")
            if not rel or any(part == ".." for part in rel.split("/")):
                continue
            workspace_paths.append(rel)
        if workspace_paths:
            attachment_block = "\n".join(f"- {path}" for path in workspace_paths)
            content = f"{content}\n\n[Attachments in task workspace]\n{attachment_block}".strip()
        if not content:
            raise ValueError("content_required")
        # 幂等键：客户端没带就由服务端生成（兼容旧客户端），语义一致。
        client_message_id = (client_message_id or "").strip() or str(uuid.uuid4())
        # check → claim → deliver 全程持有每任务发送锁：同进程的并发同 ID 请求
        # 串行化；跨 API 进程的并发由下面的数据库原子领取（pending → dispatching）
        # 兜底。不能用 _runtime_dispatch_lock——它会在恢复派发
        # （_dispatch_persisted_task_runtime）里被再次获取，asyncio.Lock 不可
        # 重入，直接死锁；这里保持 发送锁 → 派发锁 的单向嵌套顺序。
        async with self._message_send_lock(task_id):
            message_row = await self._ensure_user_input_row(task, content, client_message_id)
            claim, attempt = await self._claim_message_for_dispatch(task, message_row)
            if claim in ("received", "running", "completed", "dispatching"):
                # 已在投递/执行/完成，或另一个请求正在投递（领取失败）：
                # 同一句话不执行第二遍，返回现有状态。
                return self._message_ack(task, client_message_id, claim, attempt)
            try:
                node_session_id = await self._ensure_task_runtime_for_send(task)
            except ValueError:
                # 恢复失败：消息行标 failed，保留原因；同 ID 可再试。
                await self._transition_message_status(
                    task.id, client_message_id, "failed",
                    delivery_attempt=attempt, failure_reason="runtime_unavailable",
                )
                raise
            await self._deliver_turn(
                task, node_session_id, content, client_message_id, attempt, title_candidate,
            )
        return self._message_ack(task, client_message_id, "dispatching", attempt)

    def _message_ack(
        self, task: Task, client_message_id: str, delivery_status: str, delivery_attempt: int = 1,
    ) -> dict:
        return {
            "accepted": True,
            "task_id": str(task.id),
            "client_message_id": client_message_id,
            "delivery_status": delivery_status,
            "delivery_attempt": delivery_attempt,
        }

    async def _ensure_user_input_row(
        self, task: Task, content: str, client_message_id: str
    ) -> TaskEvent:
        """Persist (or fetch) the user_input row for one idempotent send.

        The user's input is part of the conversation timeline, so it must land
        in mc_task_events the same way agent items do — otherwise a page
        reopened after a turn only shows the agent's replies with no question
        above them. Written before the turn starts so seq orders it first; the
        partial unique index on (task_id, client_message_id) makes concurrent
        duplicate sends collapse onto one row.
        """
        return await self._persist_user_input_item(
            task.id, content, client_message_id=client_message_id, delivery_status="pending",
        )

    async def _claim_message_for_dispatch(self, task: Task, row: TaskEvent) -> tuple[str, int]:
        """Atomically claim a message row for delivery.

        Returns ``(effective_status, attempt)`` where status is what the caller
        should act on:
        - ``"pending"``  → 本请求赢得了领取，可以投递；
        - 其它状态       → 不投递，直接把该状态回给调用方（含另一请求刚领取的
          ``dispatching``）。跨进程的并发重复发送在这里收敛为一投递。

        状态机走条件 UPDATE：
        - failed/cancelled → pending：同 ID 显式重试，清旧原因并递增
          delivery_attempt——节点/runtime 按 (client_message_id, attempt)
          去重，传输层重放（同 attempt）不执行两遍，显式重试（新 attempt）可以；
        - pending → dispatching（唯一领取点）；
        - dispatching 但 ``started_at`` 已超过 _DISPATCHING_STALE_SECONDS → 结果
          未知（进程崩溃 / 投递超时），允许重新领取；真正的 exactly-once 由
          节点与 runtime 的 (message-id, attempt) 幂等兜底。
        """
        attempt = int(row.delivery_attempt or 1)
        status = (row.delivery_status or "").strip()
        if status in ("failed", "cancelled"):
            # 同 ID 显式重试：清掉旧失败原因，重新入队并递增尝试号。
            updated = await TaskEvent.filter(
                id=row.id, delivery_status__in=("failed", "cancelled"),
            ).update(
                delivery_status="pending",
                failure_reason=None,
                delivery_attempt=attempt + 1,
            )
            if updated:
                prev_status = status
                row.delivery_status = "pending"
                row.delivery_attempt = attempt + 1
                attempt += 1
                status = "pending"
                await self._record_status_transition(
                    row.task_id, "delivery_status", "pending",
                    frm=prev_status, reason="explicit retry",
                    message_id=row.client_message_id, delivery_attempt=attempt,
                )
        if status == "pending":
            # 原子领取：只有一个请求能把 pending → dispatching。
            claimed = await TaskEvent.filter(
                id=row.id, delivery_status="pending",
            ).update(delivery_status="dispatching", started_at=_utcnow())
            if claimed:
                row.delivery_status = "dispatching"
                await self._record_status_transition(
                    row.task_id, "delivery_status", "dispatching",
                    frm="pending", reason="claimed for dispatch",
                    message_id=row.client_message_id, delivery_attempt=attempt,
                )
                return "pending", attempt
        # 领取没赢（或不是 pending）：读一次最新状态决定行为。failed/cancelled
        # 并发重试有一个特殊窗口：另一个进程刚把它改成 pending、还没领取；本请求
        # 此时也必须再次做条件领取，不能把“看见 pending”误当成自己已拥有。
        fresh = await TaskEvent.get_or_none(id=row.id)
        status = (fresh.delivery_status if fresh else "") or status
        attempt = int((fresh.delivery_attempt if fresh else None) or attempt or 1)
        if status == "pending":
            claimed = await TaskEvent.filter(
                id=row.id, delivery_status="pending",
            ).update(delivery_status="dispatching", started_at=_utcnow())
            if claimed:
                row.delivery_status = "dispatching"
                row.delivery_attempt = attempt
                return "pending", attempt
            fresh = await TaskEvent.get_or_none(id=row.id)
            status = (fresh.delivery_status if fresh else "") or status
            attempt = int((fresh.delivery_attempt if fresh else None) or attempt or 1)
        if status == "dispatching":
            # mc_* 表按朴素 UTC（use_tz=False，见 database.py），naive 与 naive 比较。
            stale_before = _utcnow() - timedelta(seconds=self._DISPATCHING_STALE_SECONDS)
            started = fresh.started_at if fresh else None
            if started is not None and started < stale_before:
                # 投递结果未知（进程崩溃/超时）：用 started_at 条件原子重新
                # 领取。第一个请求把时间推到现在，后到者的 lt 条件立即失配；
                # 真正的去重由节点与 runtime 的 (message-id, attempt) 幂等继续兜底。
                reclaimed = await TaskEvent.filter(
                    id=row.id,
                    delivery_status="dispatching",
                    started_at__lt=stale_before,
                ).update(started_at=_utcnow())
                if reclaimed:
                    row.delivery_status = "dispatching"
                    return "pending", attempt
        return status or "pending", attempt

    async def _ensure_task_runtime_for_send(self, task: Task) -> str:
        """Ensure a healthy/running runtime handle for any non-deleted task.

        Always go through ``_dispatch_persisted_task_runtime`` even when the Task
        row still has a handle: that helper probes/starts the node-side runtime.
        A successful result frame leaves the session object + closed executor in
        the node map; trusting the mere string handle would write the new message
        to a closed stdin (node only logged the failure, while the control plane
        had already returned accepted) and silently lose it. Probe semantics:

        - running session → StartSessionRuntime is an idempotent no-op;
        - stopped session → starts the stream process again against the same
          workspace/state;
        - stale binding → clear + re-dispatch;
        - no handle → dispatch a fresh runtime.

        Delete is the only unrecoverable boundary (the task row no longer exists).
        """
        return await self._dispatch_persisted_task_runtime(task)

    async def _deliver_turn(
        self,
        task: Task,
        node_session_id: str,
        content: str,
        client_message_id: str,
        delivery_attempt: int,
        title_candidate: str,
    ) -> dict:
        """Push one human turn to the node with the current model/mode/llm snapshot."""
        client = get_local_node_client()
        # Prepend the git-credential policy note. Done here (not at persistence)
        # so the stored conversation stays the user's own text while every turn
        # the runtime sees carries the note.
        wire_content = _with_git_credential_note(content)
        active_model = await self._resolve_task_active_model(task, {})
        llm = None
        if task.api_key_id:
            from db import PostgresClient

            key_row = await PostgresClient.get_api_key_by_id(int(task.api_key_id))
            if key_row and key_row.get("key"):
                # 把已解析的默认模型传下去，保证本帧 model 与 llm.model 完全一致。
                llm = await self._task_llm_config(
                    {"model_id": active_model}, task, str(key_row["key"])
                )
        try:
            result = await client.send_session_input(
                node_session_id,
                "human_message",
                wire_content,
                model=active_model,
                mode=task.mode or "",
                llm=llm,
                client_message_id=client_message_id,
                delivery_attempt=delivery_attempt,
            )
        except NodeServerUnavailable as exc:
            # Control plane unreachable (node-server down / refused): the
            # transport raises NodeServerUnavailable, NOT RPCError, so the
            # ``except RPCError`` arm below does not catch it — without this
            # handler the raw exception surfaced as a 500 with no reason.
            await self._transition_message_status(
                task.id, client_message_id, "failed",
                delivery_attempt=delivery_attempt, failure_reason=str(exc),
            )
            raise ValueError("node_server_unavailable") from exc
        except RPCError as exc:
            if exc.code == Code.UNAVAILABLE:
                # Node offline / control plane unreachable mid-send: retryable,
                # not a 500. Surface as a mapped unavailable state.
                await self._transition_message_status(
                    task.id, client_message_id, "failed",
                    delivery_attempt=delivery_attempt, failure_reason=str(exc),
                )
                raise ValueError("node_server_unavailable") from exc
            if exc.code != Code.NOT_FOUND:
                await self._transition_message_status(
                    task.id, client_message_id, "failed",
                    delivery_attempt=delivery_attempt, failure_reason=str(exc),
                )
                raise
            # The control plane no longer has a binding for this handle (node
            # deleted, session unbound on the node side). The task row still
            # points at it. Clear the stale handle and re-dispatch once so
            # this message lands as the first turn of a fresh session instead
            # of surfacing as a 500.
            await self._clear_stale_node_session(task, node_session_id, "not_found on send")
            try:
                node_session_id = await self._dispatch_persisted_task_runtime(task)
                result = await client.send_session_input(
                    node_session_id,
                    "human_message",
                    wire_content,
                    model=active_model,
                    mode=task.mode or "",
                    llm=llm,
                    client_message_id=client_message_id,
                    delivery_attempt=delivery_attempt,
                )
            except NodeServerUnavailable as exc2:
                await self._transition_message_status(
                    task.id, client_message_id, "failed",
                    delivery_attempt=delivery_attempt, failure_reason=str(exc2),
                )
                raise ValueError("node_server_unavailable") from exc2
            except RPCError as exc2:
                await self._transition_message_status(
                    task.id, client_message_id, "failed",
                    delivery_attempt=delivery_attempt, failure_reason=str(exc2),
                )
                if exc2.code == Code.UNAVAILABLE:
                    raise ValueError("node_server_unavailable") from exc2
                # A freshly re-dispatched session that still cannot be placed
                # means the control plane / node is not actually serving right
                # now. That is a retryable 503, not a "task has no runtime"
                # 409 — the handle was just rebuilt and the node cannot honour it.
                if exc2.code == Code.NOT_FOUND:
                    raise ValueError("node_server_unavailable") from exc2
                raise ValueError("runtime_not_bound") from exc2
        # 控制面已受理。不乐观置 received——「runtime 收到才有回应」：节点写
        # runtime stdin 成功后回 input_status(received)，node_server 在 choke
        # point 推进 DB 状态。这里保持 dispatching 直到 ACK 到达；同 ID 的
        # 重复请求在 stale 窗口内只回当前状态，不会重投。
        # A human turn is now in flight — this is what ``processing`` means. The
        # lazy-dispatch helper leaves the task ``pending`` (runtime up, idle); it
        # is this successful send, not the dispatch, that starts a turn, so the
        # promotion belongs here. Persist it so the list/detail badges reflect
        # "running" without waiting for a stream event.
        if title_candidate and not str(getattr(task, "title", "") or "").strip():
            # 补齐空标题：条件更新只认「仍是空标题」的行，并发首条消息
            # first-writer-wins，手动命名（或另一请求已写的标题）不会被覆盖。
            # 两次尝试分别覆盖 NULL 与空串两种历史空值形态。
            try:
                backfilled = await Task.filter(id=task.id, title__isnull=True).update(title=title_candidate)
                if not backfilled:
                    backfilled = await Task.filter(id=task.id, title="").update(title=title_candidate)
                if backfilled:
                    task.title = title_candidate
            except Exception:
                # 消息已经送达；标题是展示元数据，回填失败不能把成功的
                # 首条对话报告成失败。下次消息/刷新仍可再次尝试补齐。
                logger.warning("[monkeycode-compat] failed to backfill title for task {}", task.id)
        if task.status != "processing":
            prev = task.status
            task.status = "processing"
            await task.save(update_fields=["status", "last_active_at", "updated_at"])
            await self._record_status_transition(
                task.id, "status", "processing", frm=prev, reason="turn delivered",
            )
        else:
            await task.save(update_fields=["last_active_at", "updated_at"])
        return {"accepted": result.get("accepted", True), "task_id": str(task.id)}

    async def cancel_task_turn(self, user_id: str, task_id: str, *, role: str | None = None) -> dict:
        task = await self._owned_task_or_raise(user_id, task_id, role)
        node_session_id = await self._live_node_session_id(task)
        client = get_local_node_client()
        try:
            result = await client.send_session_input(node_session_id, "cancel")
        except NodeServerUnavailable as exc:
            raise ValueError("node_server_unavailable") from exc
        except RPCError as exc:
            if exc.code == Code.UNAVAILABLE:
                # Node offline (control plane up, node disconnected): the service
                # reports it as a UNAVAILABLE envelope, not a transport error.
                # Same retryable mapping as NodeServerUnavailable.
                raise ValueError("node_server_unavailable") from exc
            if exc.code != Code.NOT_FOUND:
                raise
            # Stale handle (binding vanished node-side): there is nothing left
            # to cancel. Clear it so the task shows its real state instead of
            # erroring on every later drive attempt.
            await self._clear_stale_node_session(task, node_session_id, "not_found on cancel")
            raise ValueError("runtime_not_bound") from exc
        return {"accepted": result.get("accepted", True), "task_id": str(task.id)}

    async def restart_task_runtime(
        self,
        user_id: str,
        task_id: str,
        *,
        load_session: bool = True,
        role: str | None = None,
    ) -> dict:
        task = await self._owned_task_or_raise(user_id, task_id, role)
        node_session_id = await self._live_node_session_id(task)
        client = get_local_node_client()
        # Re-enable the task principal before the runtime resumes so MCP grants
        # are live again for the next turn.
        await self._enable_task_mcp_principal(task)
        # And the parked task child key — same closed loop: the restart's first
        # /v1 request must authenticate against a key the gateway knows.
        await self._resume_task_api_key(task)
        # Drop the previous bring-up trail before the node starts a new one.
        # Restart is the "重新运行" action offered after a failed step, so leaving
        # the old failure in place would keep the page saying the task is stuck at
        # a step that is now being retried — and if the fresh stage frames were
        # lost, it would say so forever.
        await self._clear_runtime_stage(task)
        try:
            result = await client.restart_node_session_runtime(
                node_session_id,
                fresh=not load_session,
            )
        except NodeServerUnavailable as exc:
            raise ValueError("node_server_unavailable") from exc
        except RPCError as exc:
            if exc.code == Code.UNAVAILABLE:
                # Node offline (control plane up, node disconnected): the service
                # reports it as a UNAVAILABLE envelope, not a transport error.
                # Same retryable mapping as NodeServerUnavailable.
                raise ValueError("node_server_unavailable") from exc
            if exc.code != Code.NOT_FOUND:
                raise
            await self._clear_stale_node_session(task, node_session_id, "not_found on restart")
            raise ValueError("runtime_not_bound") from exc
        return {
            "restarted": True,
            "load_session": load_session,
            "task_id": str(task.id),
            "detail": result,
        }

    async def switch_task_model(
        self, user_id: str, task_id: str, model_id: str, *, role: str | None = None
    ) -> dict:
        """Switch the active model of a Task's explicitly-added model set.

        快照非空 = 只能在已添加集合内切换；集合外目标直接失败，绝不静默回退
        （静默回退曾导致接口 200、toast 成功，但实际模型和 UI 都没变）。快照为空
        = 不限制，首次切换即以该模型建立集合并同步收窄任务子 Key 白名单。
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        model_id = (model_id or "").strip()
        if not model_id:
            raise ValueError("model_required")
        snapshot = list(task.models_snapshot or []) if isinstance(task.models_snapshot, list) else []
        if snapshot and model_id not in snapshot:
            raise ValueError("model_not_available_for_key")
        validate_key = task.api_key_id or task.parent_api_key_id
        models = [model_id] + [m for m in snapshot if m != model_id]
        models, resolved = await _validate_models_for_key(
            validate_key, models, active_model=model_id
        )
        # _validate_models_for_key 会在目标被过滤时回退 legal[0]；switch 不能把
        # 这种回退当成功。
        if resolved != model_id:
            raise ValueError("model_not_available_for_key")
        task.models_snapshot = models
        await task.save(update_fields=["models_snapshot", "updated_at"])
        if not snapshot and task.api_key_id:
            from db import PostgresClient

            await PostgresClient.update_task_key_model_whitelist(int(task.api_key_id), models)
            await self._refresh_api_key_snapshot()
        return {"task_id": str(task.id), "model_id": model_id, "models": models}

    async def add_task_models(
        self, user_id: str, task_id: str, model_ids: list[str], *, role: str | None = None
    ) -> dict:
        """Append models to the Task's explicitly-added set.

        候选按创建任务时的父 API Key 校验；合法者追加在尾部、不移动头部活跃
        模型，并同步更新任务子 Key 白名单。快照为空（不限制）时本次添加建立
        初始集合，首个新模型成为活跃模型。
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        candidates: list[str] = []
        for raw in model_ids or []:
            name = str(raw or "").strip()
            if name and name not in candidates:
                candidates.append(name)
        if not candidates:
            raise ValueError("model_required")
        validate_key = task.parent_api_key_id or task.api_key_id
        legal, _ = await _validate_models_for_key(validate_key, candidates, active_model=None)
        snapshot = list(task.models_snapshot or []) if isinstance(task.models_snapshot, list) else []
        merged = list(snapshot)
        for model in legal:
            if model not in merged:
                merged.append(model)
        task.models_snapshot = merged
        await task.save(update_fields=["models_snapshot", "updated_at"])
        if task.api_key_id:
            from db import PostgresClient

            await PostgresClient.update_task_key_model_whitelist(int(task.api_key_id), merged)
            await self._refresh_api_key_snapshot()
        return {"task_id": str(task.id), "models": merged}

    async def _resolve_task_active_model(self, task: Task, req: dict) -> str:
        """Resolve the model for the next turn; empty snapshot means unrestricted.

        快照为空且调用方未指定模型时，从「该任务 Key 允许 ∩ 当前可供给」取第
        一个稳定候选，避免 provider CLI 默认模型不可路由时首轮 404。
        """
        models = task.models_snapshot if isinstance(task.models_snapshot, list) else []
        active = models[0] if models else str(req.get("model_id") or "").strip()
        if active:
            return active
        try:
            from rate_limiter import ModelClientPool
            from model_catalog import is_internal_model_id
            from db import PostgresClient
            import config as gateway_config

            key_id = task.api_key_id or task.parent_api_key_id
            if not key_id:
                return ""
            key_row = await PostgresClient.get_api_key_by_id(int(key_id))
            if not key_row or key_row.get("disabled") or not str(key_row.get("key") or "").strip():
                return ""
            api_key = str(key_row["key"])
            for candidate in ModelClientPool.available_model_ids():
                if is_internal_model_id(candidate):
                    continue
                if await gateway_config.Config.api_key_allows_model(api_key, candidate):
                    return candidate
        except Exception:
            logger.warning("[monkeycode-compat] task {} default model resolve failed", task.id)
        return ""

    async def task_events(self, user_id: str, task_id: str, *, role: str | None = None):
        """Yield decoded runtime events for a task (async generator).

        Bridges the control plane's FollowNodeSession server stream into dicts the
        SSE route serializes. Raises ValueError when the task is missing or has no
        live runtime so the route can respond before opening the stream. Emits are
        also durably appended to ``mc_task_events`` so a reconnecting client can
        replay history (``list_task_events``) without re-running the agent.
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        node_session_id = await self._live_node_session_id(task)
        client = get_local_node_client()
        try:
            iterator = client.follow_session_events(node_session_id)
            async for event in iterator:
                await self._persist_task_event(task.id, event)
                kind = str(event.get("kind") or "message")
                # A node result is terminal only when it actually reports a
                # successful zero-exit run. The old code treated *every* result
                # as finished, so an instant startup failure
                # (success=false/exit_code=1, e.g. no agent runtime installed)
                # was falsely rendered as “已完成”. Keep this idempotent with
                # node_server.task_finalize: use a DB-side non-terminal guard so
                # an SSE follower with a stale Task instance cannot overwrite a
                # result already finalized by the control service.
                if kind == "result":
                    succeeded = event.get("success") is True and int(event.get("exit_code") or 0) == 0
                    terminal_status = "finished" if succeeded else "error"
                    prev_status = task.status
                    updated = await Task.filter(
                        id=task.id, status__not_in=("finished", "error")
                    ).update(status=terminal_status)
                    if updated:
                        from tortoise.connection import connections

                        await connections.get("default").execute_query(
                            "UPDATE mc_tasks SET completed_at=now() WHERE id=$1 AND completed_at IS NULL",
                            [task.id],
                        )
                        task.status = terminal_status
                        node_reason = _runtime_failure_reason(
                            str(event.get("error") or ""),
                            int(event.get("exit_code") or 0),
                        ) if not succeeded else None
                        await self._record_status_transition(
                            task.id, "status", terminal_status, frm=prev_status,
                            reason=node_reason or "node result success",
                        )
                        if not succeeded:
                            # Carry the node's reason to the UI. Without this the
                            # detail page can only say "失败" with no cause, which
                            # is useless to the person who has to fix it.
                            await self._record_runtime_error(task, node_reason or "")
                        await self._disable_task_mcp_principal(task)
                        # finished/error parks the task child key too: the LLM
                        # gateway must refuse it until the task resumes.
                        await self._park_task_api_key(task)
                elif kind == "error" and task.status != "error":
                    prev_status = task.status
                    updated = await Task.filter(
                        id=task.id, status__not_in=("finished", "error")
                    ).update(status="error")
                    if updated:
                        task.status = "error"
                        await self._record_status_transition(
                            task.id, "status", "error", frm=prev_status,
                            reason=str(event.get("error") or "") or "stream error frame",
                        )
                        await self._disable_task_mcp_principal(task)
                        await self._park_task_api_key(task)
                yield event
                if kind == "result":
                    return
        except NodeServerUnavailable:
            # Control plane unreachable while streaming. Surfacing it as an
            # ``error`` event (instead of a silent close) lets the page tell
            # the user "节点控制面暂不可用" and stop reconnecting, rather
            # than retrying forever with no visible reason. Not persisted to
            # ``mc_task_events`` — it is a transport condition, not a runtime
            # event; the generator yields it straight to the SSE serializer.
            yield {
                "kind": "error",
                "message": "节点控制面暂不可用，请确认节点服务已启动后重试",
            }
            return
        except RPCError as exc:
            if exc.code != Code.NOT_FOUND:
                raise
            # The binding vanished node-side while the task row still points
            # at it. Clear the stale handle and stop the stream rather than
            # surfacing a 500; the client reconnects through start/first
            # message, which re-dispatches a fresh session.
            await self._clear_stale_node_session(task, node_session_id, "not_found on follow events")
            return

    async def _next_event_seq(self, task_id: uuid.UUID) -> int:
        """Monotonic per-task sequence; starts at 1.

        Uses the shared ``mc_task_events_seq`` PG SEQUENCE so allocation is
        atomic across the gateway and node_server (both write
        ``mc_task_events``). Previously ``MAX(seq)+1`` raced across two
        connections, producing duplicate seqs that broke the ``(seq, id)``
        paging cursor (rows repeated or skipped at a page boundary inside a
        tie). The value is globally unique + monotonic; per-task it is not
        contiguous once other tasks consume seqs, but the ``seq < before``
        cursor only needs monotonic + unique, which holds. ``task_id`` is
        kept for the signature; it no longer scopes the allocation.
        """
        from tortoise.connection import connections

        rows = await connections.get("default").execute_query_dict(
            "SELECT nextval('mc_task_events_seq')"
        )
        return int(rows[0]["nextval"])

    async def _record_status_transition(
        self,
        task_id: uuid.UUID | str,
        field: str,
        to_val: str,
        *,
        frm: str = "",
        reason: str | None = None,
        message_id: str | None = None,
        delivery_attempt: int | None = None,
    ) -> None:
        """Append one accepted state transition to ``mc_task_status_history``.

        Four state machines (``status``, ``workspace_state``, ``runtime_stage``,
        ``delivery_status``) are all mutable columns updated in place, so a task
        that ended somewhere unexpected left no path behind — only surviving
        timestamps to guess from. Every writer calls this right after its UPDATE
        lands so the path is replayable: which machine moved, from where to where,
        why, and (via ``source='gateway'``) which process moved it.

        Strictly best-effort and never raises: diagnostics must not abort the
        state change they describe. Call it only when the transition actually
        took effect — for conditional updates that means after checking the
        affected-row count, so history does not claim moves the machine rejected.
        """
        try:
            await TaskStatusHistory.create(
                id=uuid.uuid4(),
                task_id=task_id if isinstance(task_id, uuid.UUID) else uuid.UUID(str(task_id)),
                field=field[:32],
                from_val=(frm or "")[:64],
                to_val=(to_val or "")[:64],
                reason=(reason or None),
                source="gateway",
                message_id=(message_id or None),
                delivery_attempt=delivery_attempt,
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "[monkeycode-compat] status history {} {}→{} for task {} not recorded",
                field, frm or "?", to_val, task_id,
            )

    async def _persist_user_input_item(
        self,
        task_id: uuid.UUID,
        text: str,
        *,
        client_message_id: str | None = None,
        delivery_status: str = "pending",
    ) -> TaskEvent:
        """Append the user's turn to the conversation trail.

        The runtime never echoes user input — it only reports what the agent did.
        So the user's own message has to be recorded here, in the same timeline
        and the same normalized ``item`` shape the node writes agent items in.
        Without it a reopened task shows answers with no questions above them.

        Content goes to ClickHouse ``task_messages`` and the PostgreSQL row keeps
        state + index: ``client_message_id``, ``logical_event_id`` (the join key),
        the delivery state machine, and its timestamps. The machine has to stay in
        PostgreSQL — it advances by conditional UPDATE, which ClickHouse cannot
        do — and the content has to leave, so a long conversation stops growing
        the table the machine is queried from. If ClickHouse is unavailable the
        payload is written inline instead, and the row is then indistinguishable
        from a pre-split one; the read path handles both without branching.

        ``client_message_id`` is the sender-minted end-to-end idempotency key
        (partial unique index ``uq_mc_task_events_client_message_id`` guards it);
        the delivery state machine starts at ``pending`` and advances via
        :meth:`_transition_message_status`. The public send/create paths always
        provide an id (generating one server-side for legacy clients); direct
        legacy callers without an id retain the historical NULL-id row shape.

        Best-effort for history, but the row itself is returned so the send path
        can react to ``IntegrityError`` (duplicate id) instead of double-sending.
        """
        text = (text or "").strip()
        if not text:
            raise ValueError("content_required")
        seq = await self._next_event_seq(task_id)
        item_id = client_message_id or f"user:{seq}"
        item = {"id": item_id, "type": "user_input", "text": text}
        stored_in_clickhouse = await task_message_store.insert_row(
            task_message_store.build_row(
                task_id=str(task_id),
                logical_event_id=item_id,
                seq=seq,
                item_type="user_input",
                item=item,
            )
        )
        try:
            return await TaskEvent.create(
                id=uuid.uuid4(),
                task_id=task_id,
                seq=seq,
                # 'item_ref' = content in ClickHouse; 'item' = content inline here.
                kind="item_ref" if stored_in_clickhouse else "item",
                event_type="user_input",
                logical_event_id=item_id,
                payload=None if stored_in_clickhouse else {"item": item, "agent_id": ""},
                client_message_id=client_message_id or None,
                delivery_status=delivery_status if client_message_id else None,
                delivery_attempt=1,
            )
        except IntegrityError:
            # Same (task_id, client_message_id) already persisted: the sender
            # retried. Return the existing row; the caller decides from its
            # delivery_status whether to (re)deliver or just report state.
            # Legacy NULL-id rows are outside the partial unique index: an
            # IntegrityError there is unrelated and must not collapse onto an
            # arbitrary NULL-id row.
            # The ClickHouse row inserted above is harmless on this path: it
            # carries the same logical_event_id and seq as the first attempt's,
            # so ReplacingMergeTree(version=seq) collapses the pair on merge and
            # FINAL reads see one frame either way.
            if not client_message_id:
                raise
            existing = await TaskEvent.get_or_none(
                task_id=task_id, client_message_id=client_message_id
            )
            if existing is not None:
                return existing
            raise

    # Allowed delivery-status transitions live in the shared module so the
    # gateway and node control service can never drift apart.
    from .message_status_machine import MESSAGE_STATUS_TRANSITIONS as _MESSAGE_STATUS_TRANSITIONS

    _DISPATCHING_STALE_SECONDS = 120

    async def _transition_message_status(
        self,
        task_id: uuid.UUID,
        client_message_id: str,
        to_status: str,
        *,
        delivery_attempt: int | None = None,
        failure_reason: str | None = None,
    ) -> bool:
        """Advance one message's delivery_status through the allowed transitions.

        Conditional single-statement update: only rows currently in a state that
        may move to ``to_status`` are touched, so a replayed ACK or a race
        between the SSE consumer and the send path can never regress the state
        machine. Terminal states (completed/cancelled/failed) are only left by
        an explicit same-id re-send (→ pending). Returns whether the row moved.

        A move is appended to ``mc_task_status_history`` so a stuck message can be
        replayed after the fact. The pre-image is read before the update (same
        connection, cheap indexed lookup) purely for that trail; the update itself
        stays conditional on ``from_states`` and is what actually gates the move,
        so a lost race just means history records the state it saw.
        """
        # Reverse index: which states may transition INTO to_status.
        from_states = [
            state for state, targets in self._MESSAGE_STATUS_TRANSITIONS.items()
            if to_status in targets
        ]
        if not from_states:
            return False
        payload: dict[str, object] = {"delivery_status": to_status}
        # mc_* 表按朴素 UTC（use_tz=False，见 database.py 注释）。
        now = _utcnow()
        if to_status == "received":
            payload["received_at"] = now
        elif to_status == "running":
            payload["started_at"] = now
        elif to_status in ("completed", "cancelled"):
            payload["completed_at"] = now
        if failure_reason is not None:
            payload["failure_reason"] = failure_reason
        previous = await TaskEvent.filter(
            task_id=task_id, client_message_id=client_message_id
        ).values_list("delivery_status", flat=True)
        updated = await TaskEvent.filter(
            task_id=task_id,
            client_message_id=client_message_id,
            # 迟到的旧 attempt ACK 不推进新 attempt 的状态：重试后 attempt 已
            # 递增，旧帧（若节点还持有）会被这里挡住。
            **({"delivery_attempt": int(delivery_attempt)} if delivery_attempt else {}),
            delivery_status__in=from_states,
        ).update(**payload)
        if updated > 0:
            await self._record_status_transition(
                task_id,
                "delivery_status",
                to_status,
                frm=str(previous[0] or "") if previous else "",
                reason=failure_reason,
                message_id=client_message_id,
                delivery_attempt=int(delivery_attempt) if delivery_attempt else None,
            )
        return updated > 0

    async def _persist_task_event(self, task_id: uuid.UUID, event: dict) -> None:
        """Persist a live-stream event that node_server does not already record.

        Conversation items (``structured``) are written by node_server at the
        single upstream choke point, so they land whether or not a browser is
        following — recording them again here would double every row. Raw
        ``output`` is the same runtime NDJSON a second time. Both are skipped;
        what remains are transport-level conditions worth keeping in history.

        ``seq`` is allocated per write from the table's current maximum, not
        cached and self-incremented from the value read when the stream opened.
        node_server writes to the same table concurrently, so a cached counter
        drifted into seq values the node had already used: duplicates broke the
        strict ordering that ``list_task_events``' ``seq < before`` cursor relies
        on, which is how a row could reappear on the next page or be skipped.
        """
        kind = str(event.get("kind") or "message")
        if kind in ("structured", "output"):
            return
        payload = event.get("payload") if isinstance(event.get("payload"), (dict, list)) else dict(event)
        try:
            import json

            blob = json.dumps(payload, ensure_ascii=False, default=str)
            if len(blob) > 256 * 1024:  # 256 KiB cap per event
                payload = {"truncated": True, "kind": kind, "size": len(blob)}
        except Exception:
            payload = {"kind": kind}
        try:
            await TaskEvent.create(
                id=uuid.uuid4(),
                task_id=task_id,
                seq=await self._next_event_seq(task_id),
                kind=kind,
                event_type=str(event.get("event_type") or ""),
                payload=payload,
            )
        except Exception:
            # Persistence must never break the live stream.
            logger.exception("[monkeycode-compat] task {} event persist failed", task_id)

    async def list_task_events(
        self,
        user_id: str,
        task_id: str,
        *,
        before: int | None = None,
        limit: int = 50,
        role: str | None = None,
    ) -> dict:
        """Paginate persisted events for conversation replay (newest-first cursor).

        ``before`` is an exclusive ``seq`` upper bound; the caller pages backward
        by passing the oldest seq seen so far. Returns rows oldest-first for
        direct render, plus the next cursor.

        PostgreSQL is paged, not ClickHouse, because it is the *ordered index* over
        everything a replay must show: conversation frames sit interleaved with
        transport rows (stage / error / message) that exist only here, so paging the
        content store would silently drop them from history. What ClickHouse
        supplies is the content for the ``item_ref`` rows on the page, fetched in one
        batch by ``logical_event_id``.

        ``id`` breaks ties in the ordering. ``seq`` now comes from a shared
        ``mc_task_events_seq`` sequence so ties no longer occur, but rows written
        before that change can still share a value; with only ``seq`` to sort by,
        Postgres was free to return tied rows in a different order on each query,
        and a page boundary landing inside a tie could repeat a row on the next
        page or skip it. Sorting by ``(seq, id)`` keeps paging stable over them.
        """
        task = await self._owned_task(user_id, task_id, role)
        if task is None:
            return {"rows": [], "next_before": None}
        limit = max(1, min(limit, 200))
        query = TaskEvent.filter(task_id=task.id)
        if before is not None:
            query = query.filter(seq__lt=before)
        rows = await query.order_by("-seq", "-id").limit(limit)
        rows_list = [r for r in rows]
        # The cursor advances over raw rows, before any collapsing below, so a
        # page always consumes exactly ``limit`` rows of the index and the caller
        # cannot loop forever on a page that collapsed to fewer items.
        next_before = rows_list[-1].seq if len(rows_list) == limit else None
        ordered = list(reversed(rows_list))  # oldest-first for render

        contents = await self._load_frame_contents(task.id, ordered)
        seen_logical: set[str] = set()
        out: list[dict] = []
        for r in ordered:
            logical = str(r.logical_event_id or "")
            payload = r.payload
            if payload is None and logical:
                # Content lives in ClickHouse. Every frame of this logical item is
                # merged, not just the ones whose seq fell inside this page — a
                # page boundary between a tool call's opening and closing frame
                # would otherwise render an untitled card missing its input.
                payload = contents.get(logical)
                if payload is None:
                    # ClickHouse unreachable or the frame never landed. Say so
                    # rather than dropping the turn: an empty gap in a transcript
                    # reads as data loss, a marked one reads as a degraded store.
                    payload = {
                        "item": {
                            "id": logical,
                            "type": r.event_type or "agent_message",
                            "content_unavailable": True,
                        },
                        "agent_id": "",
                        "logical_event_id": logical,
                    }
            if logical:
                # One entry per logical item. Append-only means the index holds a
                # row per frame, and each resolves to the same merged content, so
                # emitting them all would repeat the bubble once per frame. The
                # first (lowest seq) row wins, which places the item where it
                # started in the conversation.
                if logical in seen_logical:
                    continue
                seen_logical.add(logical)
            out.append(
                {
                    "seq": r.seq,
                    "kind": r.kind,
                    "event_type": r.event_type,
                    "logical_event_id": r.logical_event_id,
                    "payload": self._backfill_canonical_envelope(payload),
                    "client_message_id": r.client_message_id,
                    "delivery_status": r.delivery_status,
                    "delivery_attempt": int(r.delivery_attempt or 1),
                    "failure_reason": r.failure_reason,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
            )
        return {"rows": out, "next_before": next_before}

    @staticmethod
    async def _load_frame_contents(task_id: uuid.UUID, rows: list) -> dict[str, dict]:
        """Merge the ClickHouse frames behind a page's ``item_ref`` rows.

        Only rows with a NULL ``payload`` need it — a non-NULL payload means the
        content is inline (written before the split, or while ClickHouse was
        down), and re-fetching it would be both wasteful and wrong. Returns
        ``logical_event_id`` → merged envelope; a missing key means the content
        could not be loaded, which the caller renders as unavailable rather than
        as an empty turn.
        """
        wanted = [
            str(r.logical_event_id)
            for r in rows
            if r.payload is None and r.logical_event_id
        ]
        if not wanted:
            return {}
        try:
            grouped = await task_message_store.fetch_by_logical_ids(str(task_id), wanted)
        except Exception:  # noqa: BLE001
            # fetch_by_logical_ids already degrades internally; this is the
            # backstop so a replay never fails outright on a content-store fault.
            logger.exception("[monkeycode-compat] task {} frame content load failed", task_id)
            return {}
        merged: dict[str, dict] = {}
        for logical, frames in grouped.items():
            envelope = task_message_store.merge_frames(frames)
            if envelope is not None:
                merged[logical] = envelope
        return merged

    @staticmethod
    def _backfill_canonical_envelope(payload: object) -> object:
        """Project the canonical envelope onto rows written before it existed.

        New rows carry ``logical_event_id``/``event_kind``/``tool_name``/
        ``subagent_id`` top-level in the payload; old rows only have the legacy
        ``agent_id`` alias and the raw item. The frontend routes on
        ``subagent_id`` alone (empty = root transcript, non-empty = sub-agent
        detail), so the derivation happens here, once, at the read boundary —
        never in the client. Rows that already carry the canonical fields pass
        through untouched; non-item payloads (transport conditions, usage rows)
        are returned verbatim.
        """
        if not isinstance(payload, dict) or not isinstance(payload.get("item"), dict):
            return payload
        if payload.get("subagent_id") is not None:
            return payload
        projected = dict(payload)
        projected["subagent_id"] = str(payload.get("agent_id") or "")
        projected["compat_derived"] = True
        return projected

    async def task_stats(self, user_id: str, task_id: str, *, role: str | None = None) -> dict:
        task = await self._owned_task(user_id, task_id, role)
        if task is None:
            return {}
        rows = await TaskUsageStat.filter(task_id=task.id)
        return {
            "input_tokens": sum(r.input_tokens for r in rows),
            "output_tokens": sum(r.output_tokens for r in rows),
            "total_tokens": sum(r.total_tokens for r in rows),
        }

    async def list_task_request_logs(
        self,
        user_id: str,
        task_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        role: str | None = None,
    ) -> dict:
        """List request logs attributed to a Task the caller owns.

        Ownership is resolved through the Task ledger (monkeycode_compat
        connection); the request_logs rows themselves are read from the main
        PostgresClient pool by the immutable ``task_id`` snapshot written at
        request time. Legacy editor-attributed rows are surfaced through the
        editor routes during the compatibility window.
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        from db import PostgresClient

        return await PostgresClient.query_task_request_logs(
            str(task.id), limit=limit, offset=offset
        )

    async def get_task_request_log_detail(
        self, user_id: str, task_id: str, log_id: int, *, role: str | None = None
    ) -> dict | None:
        task = await self._owned_task_or_raise(user_id, task_id, role)
        from db import PostgresClient

        return await PostgresClient.get_task_request_log_detail(log_id, str(task.id))

    async def task_workspace_node_session_id(
        self, user_id: str, task_id: str, *, role: str | None = None
    ) -> str:
        """Resolve an owned Task's live session for workspace transports.

        The Task's workspace directory survives stop/delete of its runtime, but
        the files and terminal tunnel needs a live node session to address it.
        Callers map ``runtime_not_bound`` to a 409 and may restart the Task to
        reuse the same persistent workspace.
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        return await self._live_node_session_id(task)

    async def disable_task_api_key(
        self, user_id: str, task_id: str, *, role: str | None = None
    ) -> dict:
        """Disable the Task's current child key (owner only).

        The key row is retained disabled so historical request-log lineage
        remains resolvable. A stopped Task keeps its key; this is only called
        by the explicit disable endpoint.
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        from db import PostgresClient

        result = await PostgresClient.disable_task_api_key(str(task.id), str(task.user_id))
        if result is None:
            raise ValueError("task_key_not_found")
        # Without this the disabled key keeps authenticating from the stale
        # snapshot until the 60s reconcile — a revoke that does not revoke.
        await self._refresh_api_key_snapshot()
        return result

    async def rotate_task_api_key(
        self, user_id: str, task_id: str, *, role: str | None = None
    ) -> dict:
        """Stage a replacement task key and push it to the live runtime.

        The DB stages the new key with the old key still enabled, then we push
        the new LLM config to the live node session. Only on a successful
        configure do we disable the old key. A node-config failure (or an
        offline runtime) rolls back: the Task keeps its old key and the
        replacement is disabled.
        """
        task = await self._owned_task_or_raise(user_id, task_id, role)
        from db import PostgresClient

        staged = await PostgresClient.rotate_task_api_key(str(task.id), str(task.user_id))
        if staged is None:
            raise ValueError("task_key_not_found")
        old_key_id = staged.get("old_key_id")
        new_key_id = staged.get("new_key_id")
        # The replacement must be resolvable by auth before the runtime is told
        # to use it, or the first request on the new key 401s.
        await self._refresh_api_key_snapshot()
        # Best-effort live runtime reconfigure. If the runtime is offline the
        # Task keeps its old key; the staged replacement is rolled back.
        node_session_id = (task.node_session_id or "").strip()
        llm = await self._task_llm_config({}, task, staged["key"])
        if node_session_id:
            try:
                client = get_local_node_client()
                await client.configure_node_session_llm(node_session_id, llm)
            except Exception:
                logger.exception(
                    "[monkeycode-compat] task {} key rotation node-config failed; rolling back",
                    task.id,
                )
                if new_key_id is not None:
                    await PostgresClient.disable_api_key_by_id(new_key_id)
                if old_key_id is not None:
                    await PostgresClient.restore_task_api_key_id(
                        str(task.id), str(task.user_id), old_key_id
                    )
                await self._refresh_api_key_snapshot()
                raise ValueError("runtime_not_bound")
        # Runtime acked (or no live runtime to reconfigure): disable the old key.
        if old_key_id is not None and old_key_id != new_key_id:
            await PostgresClient.disable_api_key_by_id(old_key_id)
            await self._refresh_api_key_snapshot()
        return {
            "task_id": str(task.id),
            "key_id": new_key_id,
            "key": staged["key"],
            "key_masked": staged["key_masked"],
            "version": staged["version"],
            "expires_at": staged["expires_at"],
            "usage_limit": staged["usage_limit"],
        }



def _safe_subdir(name: str | None) -> str:
    """Sanitize a project name into a workspace-safe single path segment.

    The node clones associated repos into ``<workDir>/<subdir>``, so the value
    must not escape the workspace: path separators, ``..`` and control chars are
    replaced. Falls back to ``dep`` when nothing usable remains.
    """
    raw = (name or "").strip()
    cleaned = []
    for ch in raw:
        if ch.isalnum() or ch in "-_.":
            cleaned.append(ch)
        elif ch.isspace():
            cleaned.append("-")
    slug = "".join(cleaned).strip(".-_")
    # Reject traversal remnants like ".." after stripping separators.
    while ".." in slug:
        slug = slug.replace("..", ".")
    slug = slug.strip(".-_")
    return slug[:128] or "dep"


def _maybe_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


task_service = TaskService()
