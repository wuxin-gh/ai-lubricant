"""Project domain service (projects / issues / collaborators / comments).

Storage + access control for the project surface. Multi-user isolation is
enforced by ``user_id``: a project is accessible to its owner, to collaborators
(``mc_project_collaborators``), and to privileged (admin) callers.
Write access on issues/comments additionally requires owner or read_write
collaborator role, mirroring the upstream contract.

This layer is storage only — it never touches the model request pipeline.
"""
from __future__ import annotations

import base64
import binascii
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from loguru import logger

from . import git_clients, stack_detector
from .git_service import git_service
from .models import User
from .models_project import (
    Project,
    ProjectAssociation,
    ProjectCollaborator,
    ProjectIssue,
    ProjectIssueComment,
)
from .models_task import ProjectTask, Task
from .task_service import task_service

# Sidebar/project menus render a bounded newest-first slice per project. The
# full task list lives on the project page; this only populates the submenu.
_PROJECT_TASKS_LIMIT = 10

# 技术栈扫描（scan_stack_profile）的输入护栏：cnb/atomgit 浅树下钻的
# 广度/深度边界、路径数封顶（防超巨仓库把 profile 撑爆）、清单内容上限
# （对齐 leaderboard_probe._MAX_FILE_BYTES 的纪律）。
_SCAN_WALK_BREADTH = 40
_SCAN_MAX_PATHS = 20_000
_SCAN_MAX_MANIFEST_BYTES = 256 * 1024
# 子模块扫描数量上限——防 .gitmodules 声明巨量子模块把扫描拖垮；每个子模块
# 自带 1 次取树 + ≤_MAX_FETCH_BUDGET_DEFAULT 次清单取数的预算自律。
_SCAN_MAX_SUBMODULES = 10


def _decode_blob_text(content_b64: str | None) -> str | None:
    """base64 解出 UTF-8 文本；超预算/解不开返回 None（调方跳过该清单）。"""
    if not content_b64:
        return None
    try:
        raw = base64.b64decode(content_b64, validate=True)
    except (binascii.Error, ValueError):
        return None
    if len(raw) > _SCAN_MAX_MANIFEST_BYTES:
        return None
    return raw.decode("utf-8", errors="replace")


def _domain_user(user: User) -> dict:
    """DomainUser shape for project creators (mirrors team_users_service).

    Kept local to avoid a cross-module dependency on a private helper in the
    team admin service; the field layout matches ``DomainUser`` so the manager
    project list renders creator name/email consistently.
    """
    return {
        "id": str(user.id),
        "name": user.name,
        "email": user.email,
        "avatar_url": user.avatar_url,
        "role": user.role,
        "status": user.status,
        "has_password": bool(user.password),
        "is_blocked": bool(user.is_blocked),
        "team": None,
        "identities": [],
    }


async def _enrich_project_rows(rows: list[dict]) -> list[dict]:
    """Attach creator + requirement/task counts to a page of project rows.

    Runs a fixed number of bulk queries over the current page's ids only (no
    N+1). Counts default to 0 when no rows match; creator is ``None`` when the
    owner is missing or soft-deleted. Mutates and returns the input list.
    """
    if not rows:
        return rows

    project_ids = [uuid.UUID(r["id"]) for r in rows if r.get("id")]
    owner_ids = [uuid.UUID(r["user_id"]) for r in rows if r.get("user_id")]

    creators: dict[str, dict] = {}
    if owner_ids:
        owner_rows = await User.filter(id__in=owner_ids, is_deleted=False)
        creators = {str(u.id): _domain_user(u) for u in owner_rows}

    # Requirement-only count: the manager "需求" column must not include bugs.
    requirement_counts: dict[str, int] = {}
    if project_ids:
        req_rows = await ProjectIssue.filter(
            project_id__in=project_ids, issue_type="requirement"
        ).values_list("project_id", flat=True)
        for pid in req_rows:
            requirement_counts[str(pid)] = requirement_counts.get(str(pid), 0) + 1

    # Task count via ProjectTask bindings (one row per task↔project link).
    task_counts: dict[str, int] = {}
    # Sidebar/project menus render ``project.tasks`` as children, so each row
    # carries a bounded, newest-first slice of its tasks. Two bulk queries only
    # (bindings, then the tasks themselves) — never N+1 per project.
    task_lists: dict[str, list[dict]] = {}
    if project_ids:
        binding_rows = await ProjectTask.filter(
            project_id__in=project_ids
        ).values("task_id", "project_id")
        project_of_task: dict[uuid.UUID, str] = {}
        for binding in binding_rows:
            pid = str(binding["project_id"])
            task_counts[pid] = task_counts.get(pid, 0) + 1
            project_of_task[binding["task_id"]] = pid
        if project_of_task:
            task_rows = (
                await Task.filter(id__in=list(project_of_task.keys()), deleted_at=None)
                .order_by("-created_at")
                .values("id", "title", "content", "status", "created_at")
            )
            for task_row in task_rows:
                pid = project_of_task.get(task_row["id"])
                if pid is None:
                    continue
                bucket = task_lists.setdefault(pid, [])
                if len(bucket) >= _PROJECT_TASKS_LIMIT:
                    continue
                bucket.append({
                    "id": str(task_row["id"]),
                    "title": task_row["title"] or task_row["content"] or "",
                    "status": task_row["status"],
                    "created_at": _unix(task_row["created_at"]),
                })

    for row in rows:
        row["creator"] = creators.get(str(row["user_id"])) if row.get("user_id") else None
        row["issue_count"] = requirement_counts.get(str(row["id"]), 0)
        row["task_count"] = task_counts.get(str(row["id"]), 0)
        row["tasks"] = task_lists.get(str(row["id"]), [])
    return rows

_PRIVILEGED_ROLES = {"admin"}
_WRITE_ROLES = {"read_write"}

ISSUE_TYPES = ("requirement", "bug")

# Terminal-ish states that close an issue out; used to stamp ``closed_at``.
_CLOSING_STATES = {"completed", "fixed", "closed"}

# Requirement and bug each run their own flow. Values are the states a given
# state may transition to; anything else is rejected by ``update_issue``.
# "已确认" transitions are intentionally reachable so a human can confirm, but
# the MCP tool layer restricts agents from performing them (see plan §6).
ISSUE_TRANSITIONS: dict[str, dict[str, set[str]]] = {
    "requirement": {
        "unassigned": {"designing", "developing", "closed"},
        "designing": {"design_pending_confirmation", "closed"},
        "design_pending_confirmation": {"design_confirmed", "designing", "closed"},
        "design_confirmed": {"developing", "closed"},
        "developing": {"completed", "closed"},
        "completed": {"closed"},
        "closed": set(),
    },
    "bug": {
        "unassigned": {"diagnosing", "fixing", "closed"},
        "diagnosing": {"reason_pending_confirmation", "closed"},
        "reason_pending_confirmation": {"reason_confirmed", "diagnosing", "closed"},
        "reason_confirmed": {"fixing", "closed"},
        "fixing": {"fixed", "closed"},
        "fixed": {"closed"},
        "closed": set(),
    },
}

_STATUS_LABELS = {
    "unassigned": "待分配",
    "designing": "设计中",
    "design_pending_confirmation": "设计文档待确认",
    "design_confirmed": "设计文档已确认",
    "developing": "开发中",
    "completed": "已完成",
    "diagnosing": "定位中",
    "reason_pending_confirmation": "原因待确认",
    "reason_confirmed": "原因已确认",
    "fixing": "修复中",
    "fixed": "已完成修复",
    "closed": "已关闭",
}


class IssueTransitionError(ValueError):
    """Raised when a status change is not allowed by the issue's state flow."""


# When a human clicks 分配 on an unassigned issue, we create a task and move the
# issue into its first active state. Keyed by issue type -> the (task_role,
# task_type, sub_type, active status) tuple to apply.
_ASSIGN_PLANS: dict[str, dict[str, dict[str, str]]] = {
    "requirement": {
        "analysis": {
            "task_role": "design",
            "task_type": "design",
            "sub_type": "generate_design",
            "status": "designing",
        },
        "fix": {
            "task_role": "develop",
            "task_type": "develop",
            "sub_type": "execute_task",
            "status": "developing",
        },
    },
    "bug": {
        "analysis": {
            "task_role": "diagnose",
            "task_type": "develop",
            "sub_type": "diagnose_bug",
            "status": "diagnosing",
        },
        "fix": {
            "task_role": "fix",
            "task_type": "develop",
            "sub_type": "fix_bug",
            "status": "fixing",
        },
    },
}

# After a design/reason has been confirmed by a human, this maps the confirmed
# state to the follow-up task to create and the active status to move into.
_FOLLOWUP_PLAN: dict[str, dict[str, dict[str, str]]] = {
    "requirement": {
        "design_confirmed": {
            "task_role": "develop",
            "task_type": "develop",
            "sub_type": "execute_task",
            "status": "developing",
        }
    },
    "bug": {
        "reason_confirmed": {
            "task_role": "fix",
            "task_type": "develop",
            "sub_type": "fix_bug",
            "status": "fixing",
        }
    },
}

# The pending-confirmation state each type sits in before a human confirms, and
# the confirmed state to move to. Confirmation is human-only; agents advance an
# issue INTO the pending state but must never confirm it themselves.
_CONFIRM_FLOW: dict[str, tuple[str, str]] = {
    "requirement": ("design_pending_confirmation", "design_confirmed"),
    "bug": ("reason_pending_confirmation", "reason_confirmed"),
}


def _normalize_issue_type(value: Any) -> str:
    kind = str(value or "").strip().lower()
    return kind if kind in ISSUE_TYPES else "requirement"


def _initial_status(issue_type: str) -> str:
    """Every new issue starts unassigned regardless of type."""
    return "unassigned"


def _validate_transition(issue_type: str, current: str, target: str) -> None:
    """Reject illegal status moves, naming both states in the user's language."""
    if current == target:
        return
    allowed = ISSUE_TRANSITIONS.get(issue_type, {}).get(current)
    if allowed is None:
        # Unknown current state (e.g. a row written before the state machine
        # landed). Let it move to any state this type declares so operators can
        # recover it rather than being stuck.
        if target in ISSUE_TRANSITIONS.get(issue_type, {}):
            return
        raise IssueTransitionError(
            f"未知状态「{current}」无法变更为「{_STATUS_LABELS.get(target, target)}」"
        )
    if target not in allowed:
        raise IssueTransitionError(
            f"当前状态「{_STATUS_LABELS.get(current, current)}」"
            f"不能直接变更为「{_STATUS_LABELS.get(target, target)}」"
        )


def _normalize_pending_items(value: Any) -> list[dict]:
    """Coerce pending items into a list of ``{id, content, status}`` dicts.

    Accepts a list of plain strings (convenience for callers/agents) or a list
    of dicts. Anything else yields an empty list rather than raising, so a bad
    payload cannot wedge an issue update.
    """
    if not isinstance(value, list):
        return []
    items: list[dict] = []
    for entry in value:
        if isinstance(entry, str):
            content = entry.strip()
            if content:
                items.append(
                    {"id": str(uuid.uuid4()), "content": content, "status": "pending"}
                )
            continue
        if not isinstance(entry, dict):
            continue
        content = str(entry.get("content") or "").strip()
        if not content:
            continue
        status = str(entry.get("status") or "pending").strip().lower()
        items.append(
            {
                "id": str(entry.get("id") or uuid.uuid4()),
                "content": content,
                "status": status if status in ("pending", "resolved") else "pending",
            }
        )
    return items


def _unix(dt: Any) -> int | None:
    """Convert a datetime to unix seconds (int), or None when absent.

    The team frontend (``Api.ts`` types + ``formatTime``/``dayjs.unix``) reads
    these fields as unix seconds, matching the rest of the team surface
    (``team_users_service`` / ``team_models_service``). Emitting ISO strings
    here would render as "Invalid Date".
    """
    if dt is None:
        return None
    return int(dt.timestamp())


def _derive_full_name(repo_url: str | None, platform: str | None = None) -> str:
    """Derive ``owner/repo`` (repo full name) from a stored ``repo_url``.

    Takes the URL path, strips the leading ``/`` and any ``.git`` suffix, and
    preserves multi-level namespaces (e.g. GitLab groups/subgroups). Returns an
    empty string when ``repo_url`` is absent or has no path. ``platform`` is
    accepted for future per-platform quirks but currently unused — the path
    heuristic works for all supported platforms.
    """
    if not repo_url:
        return ""
    # urlparse handles scheme://host/path; a bare "owner/repo" (no scheme) lands
    # entirely in ``path`` too, which is what we want.
    parsed = urlparse(repo_url.strip())
    path = parsed.path if parsed.netloc else (parsed.path or repo_url.strip())
    path = path.strip("/")
    if path.endswith(".git"):
        path = path[: -len(".git")]
    return path


def parse_gitmodules(text: str) -> list[dict]:
    """Parse a ``.gitmodules`` file into ``[{name, path, url, branch}]``.

    Hand-rolled rather than ``configparser``: ``.gitmodules`` indents its
    key/value lines with a Tab, which configparser treats as a continuation of
    the previous value instead of a new key. Sections missing ``path`` or
    ``url`` are dropped — they cannot identify a submodule.
    """
    sections: list[dict] = []
    current: dict | None = None
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            if current is not None:
                sections.append(current)
            header = line[1:-1].strip()
            if not header.lower().startswith("submodule"):
                current = None
                continue
            # ``submodule "name"`` — the quoted part is the section name.
            name = ""
            if '"' in header:
                name = header.split('"', 2)[1] if header.count('"') >= 2 else ""
            current = {"name": name, "path": "", "url": "", "branch": ""}
            continue
        if current is None or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().lower()
        if key in ("path", "url", "branch"):
            current[key] = value.strip()
    if current is not None:
        sections.append(current)
    return [s for s in sections if s.get("path") and s.get("url")]


def _resolve_submodule_url(parent_repo_url: str | None, sub_url: str) -> str:
    """Resolve a possibly-relative ``.gitmodules`` url against the parent repo.

    Git resolves ``./x.git`` / ``../x.git`` against the superproject's remote,
    so ``https://host/owner/parent.git`` + ``../other.git`` becomes
    ``https://host/owner/other.git``. An absolute url is returned verbatim.
    """
    sub_url = (sub_url or "").strip()
    if not sub_url.startswith(("./", "../")):
        return sub_url
    if not parent_repo_url:
        return sub_url
    parsed = urlparse(parent_repo_url.strip())
    if not parsed.netloc:
        return sub_url
    segments = [seg for seg in parsed.path.split("/") if seg]
    for segment in sub_url.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if segments:
                segments.pop()
            continue
        segments.append(segment)
    return f"{parsed.scheme}://{parsed.netloc}/{'/'.join(segments)}"


def _repo_identity(url: str | None) -> tuple[str, str]:
    """``(host, owner/repo)`` lowercased — the key used to match a repo url to
    an existing platform project regardless of ``.git`` suffix or casing."""
    if not url:
        return ("", "")
    parsed = urlparse(url.strip())
    return (parsed.netloc.lower(), _derive_full_name(url).lower())


@dataclass
class PageResult:
    total: int
    page: int
    page_size: int
    rows: list[dict]


def _project_dict(p: Project) -> dict:
    return {
        "id": str(p.id),
        "user_id": str(p.user_id),
        "name": p.name,
        "description": p.description,
        "platform": p.platform,
        "repo_url": p.repo_url,
        "full_name": _derive_full_name(p.repo_url, p.platform),
        "branch": p.branch,
        "git_identity_id": str(p.git_identity_id) if p.git_identity_id else None,
        "env_variables": p.env_variables,
        "stack": p.stack_profile,
        "is_team_shared": bool(p.is_team_shared),
        "team_id": str(p.team_id) if p.team_id else None,
        "created_at": _unix(p.created_at),
        "updated_at": _unix(p.updated_at),
    }


def _issue_dict(i: ProjectIssue) -> dict:
    return {
        "id": str(i.id),
        "user_id": str(i.user_id),
        "project_id": str(i.project_id),
        "type": i.issue_type,
        "status": i.status,
        "title": i.title,
        "requirement_document": i.requirement_document,
        "design_document": i.design_document,
        "bug_reason": i.bug_reason,
        "pending_items": i.pending_items or [],
        "resolution_note": i.resolution_note,
        "summary": i.summary,
        "assignee_id": str(i.assignee_id) if i.assignee_id else None,
        "priority": i.priority,
        "tags": i.tags or [],
        "created_at": _unix(i.created_at),
        "updated_at": _unix(i.updated_at),
        "closed_at": _unix(i.closed_at),
    }


def _comment_dict(c: ProjectIssueComment) -> dict:
    return {
        "id": str(c.id),
        "user_id": str(c.user_id),
        "issue_id": str(c.issue_id),
        "parent_id": str(c.parent_id) if c.parent_id else None,
        "comment": c.comment,
        "created_at": _unix(c.created_at),
        "updated_at": _unix(c.updated_at),
    }


def _collaborator_dict(c: ProjectCollaborator) -> dict:
    return {
        "id": str(c.id),
        "project_id": str(c.project_id),
        "user_id": str(c.user_id),
        "role": c.role,
        "created_at": _unix(c.created_at),
    }


def _association_dict(a: ProjectAssociation) -> dict:
    """Serialize one association row.

    Repo-level accessibility is NOT part of the row — the caller resolves it by
    probing the target repository with the source project's git identity token.
    """
    return {
        "id": str(a.id),
        "source_project_id": str(a.source_project_id),
        "target_project_id": str(a.target_project_id),
        "relation": a.relation,
        "target_subdir": a.target_subdir,
        "created_at": _unix(a.created_at),
        "updated_at": _unix(a.updated_at),
    }


def _maybe_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


class ProjectService:
    """Projects/issues/collaborators/comments with per-user access control."""

    def _is_privileged(self, role: str | None) -> bool:
        return (role or "") in _PRIVILEGED_ROLES

    async def _access_role(self, project: Project, user_id: str, role: str | None) -> str | None:
        """Return effective access: 'owner' | 'read_write' | 'read_only' | None."""
        if self._is_privileged(role) or str(project.user_id) == str(user_id):
            return "owner"
        # 团队共享：同 team 成员视作 read_write（仍不可删除/管授权，那些仅 owner）。
        if project.is_team_shared and project.team_id:
            from .deps import resolve_team_id
            try:
                caller_team = await resolve_team_id(user_id)
            except Exception:
                caller_team = None
            if caller_team and str(caller_team) == str(project.team_id):
                return "read_write"
        collab = await ProjectCollaborator.get_or_none(
            project_id=project.id, user_id=uuid.UUID(user_id)
        )
        return collab.role if collab else None

    async def _get_accessible_project(
        self, project_id: str, user_id: str, role: str | None
    ) -> tuple[Project | None, str | None]:
        pid = _maybe_uuid(project_id)
        if pid is None:
            return None, None
        project = await Project.get_or_none(id=pid)
        if project is None:
            return None, None
        access = await self._access_role(project, user_id, role)
        return (project, access) if access else (None, None)

    # ---- Repository read (tree / blob) ------------------------------------
    async def _resolve_repo_context(
        self, project: Project
    ) -> tuple[str, str, git_clients.RepositoryOptions] | None:
        """Resolve ``(platform, full_name, opts)`` for a project's repository.

        Loads the project's linked git identity for its credentials. Returns
        ``None`` when the project has no linked identity, the identity is gone,
        the platform is unsupported, or credentials/full_name are missing — the
        caller turns that into an empty result rather than an error.
        """
        if not project.git_identity_id:
            return None
        identity = await git_service.load_identity_for_read(str(project.git_identity_id))
        if identity is None or not identity.access_token:
            return None
        platform = (identity.platform or project.platform or "").lower()
        if not git_clients.supports_platform(platform):
            return None
        full_name = _derive_full_name(project.repo_url, project.platform)
        if not full_name:
            return None
        opts = git_service._build_repo_options(identity)
        return platform, full_name, opts

    @staticmethod
    def _match_submodule_path(submodule_map: dict[str, dict], path: str) -> tuple[dict, str] | None:
        """Locate the submodule owning ``path``, plus the path inside it.

        An exact match means the submodule root (inner path ``""``). A longer
        path must be separated by ``/`` so ``nodes-extra`` never matches the
        submodule ``nodes``. The longest matching prefix wins, so a submodule
        nested inside another still resolves to the innermost one.
        """
        target = (path or "").strip("/")
        if not target:
            return None
        best: tuple[dict, str] | None = None
        best_len = -1
        for sub_path, info in submodule_map.items():
            key = (sub_path or "").strip("/")
            if not key:
                continue
            if target == key:
                inner = ""
            elif target.startswith(key + "/"):
                inner = target[len(key) + 1:]
            else:
                continue
            if len(key) > best_len:
                best, best_len = (info, inner), len(key)
        return best

    async def scan_stack_profile(self, project_id: str) -> dict | None:
        """对项目根仓库跑规则识别，结果落 ``stack_profile`` 列。

        **绝不抛**（调用方是 fire-and-forget 任务/同步重扫路由）：上游失败只
        记 ``{"schema", "error"}`` 进列，下次重扫覆盖。根仓库扫完后再对
        ``.gitmodules`` 声明的每个子模块单独跑识别（一层，不递归嵌套子
        模块），结果挂 ``profile["submodules"]``；``.gitmodules`` 读不到时
        置 truncated 提示「子模块未纳入本结果」。

        取数链路全部复用仓库读取基础设施：``_resolve_repo_context`` 解析平台
        凭据，``git_clients.fetch_tree`` 递归取树，``fetch_blob`` 按预算取
        清单内容（256KB 上限，对齐 leaderboard_probe._MAX_FILE_BYTES 的纪律）。
        """
        pid = _maybe_uuid(project_id)
        if pid is None:
            return None
        project = await Project.get_or_none(id=pid)
        if project is None:
            return None

        ctx = await self._resolve_repo_context(project)
        if ctx is None:
            profile = {"schema": stack_detector.PROFILE_SCHEMA, "error": "no_repo_context",
                       "scanned_at": datetime.now(timezone.utc).isoformat()}
            project.stack_profile = profile
            await project.save(update_fields=["stack_profile"])
            return profile
        platform, full_name, opts = ctx
        ref = project.branch or ""

        try:
            profile, paths = await self._detect_repo_stack(
                platform=platform, full_name=full_name, opts=opts, ref=ref
            )
        except git_clients.GitClientError as exc:
            profile = {"schema": stack_detector.PROFILE_SCHEMA, "error": str(exc)[:500],
                       "scanned_at": datetime.now(timezone.utc).isoformat()}
            project.stack_profile = profile
            await project.save(update_fields=["stack_profile"])
            logger.warning(
                "[user-platform] stack scan tree failed: platform={} full_name={} project_id={} error={}",
                platform, full_name, project.id, exc,
            )
            return profile

        # .gitmodules 在场即对每个子模块仓库单独跑识别（父项目凭据可读同 host
        # 的子仓库）。子模块内容不在父递归树里（gitlink 是 commit 类型条目），
        # 必须各取其树；结果挂 profile["submodules"]。读不到 .gitmodules 时
        # 保留 truncated 提示「子模块未纳入本结果」。
        if any(p == ".gitmodules" for p in paths):
            submodules, sub_ok = await self._scan_submodules(
                project, platform=platform, full_name=full_name, opts=opts, parent_ref=ref
            )
            if submodules:
                profile["submodules"] = submodules
            if not sub_ok:
                profile["truncated"] = True
        project.stack_profile = profile
        await project.save(update_fields=["stack_profile"])
        return profile

    async def _detect_repo_stack(
        self,
        *,
        platform: str,
        full_name: str,
        opts: "git_clients.RepositoryOptions",
        ref: str,
    ) -> tuple[dict, list[str]]:
        """单仓库取树+取清单+跑识别（根仓库与子模块扫描共用的内核）。

        返回 ``(profile, 根仓库路径清单)``。cnb/atomgit 浅树有界逐层下钻
        并标 truncated；``GitClientError`` 向上抛由调用方决定语义：根仓库
        失败记 ``profile.error``，子模块失败记该子模块 ``error``。子模块
        不再递归其自身的 ``.gitmodules``——嵌套子模块由调用方标 truncated。
        """
        truncated_hint = False
        entries = await git_clients.fetch_tree(
            platform, full_name, opts, ref=ref, path="", recursive=True
        )
        # cnb/atomgit 的树接口不递归（fetch_tree 一次 GET 返回单层）：
        # 检测到浅树就有界逐层下钻（深度 2 / 广度 40），结果标 truncated。
        if platform in ("cnb", "atomgit") and not any(
            "/" in str(e.path or "") for e in entries
        ):
            shallow = entries
            entries = list(entries)
            for top in shallow[:_SCAN_WALK_BREADTH]:
                if top.mode != git_clients._MODE_DIRECTORY:
                    continue
                try:
                    sub = await git_clients.fetch_tree(
                        platform, full_name, opts, ref=ref,
                        path=str(top.path or ""), recursive=False,
                    )
                except git_clients.GitClientError:
                    continue
                entries.extend(sub[:_SCAN_WALK_BREADTH])
            truncated_hint = True

        paths = [str(e.path) for e in entries if e.mode == git_clients._MODE_REGULAR][:_SCAN_MAX_PATHS]

        async def _fetch_text(path: str) -> str | None:
            try:
                blob = await git_clients.fetch_blob(platform, full_name, opts, path=path, ref=ref)
            except git_clients.GitClientError:
                return None
            if blob is None or blob.is_binary:
                return None
            return _decode_blob_text(blob.content)

        profile = await stack_detector.detect_stack(paths, _fetch_text, truncated_hint=truncated_hint)
        return profile, paths

    async def _scan_submodules(
        self,
        project: Project,
        *,
        platform: str,
        full_name: str,
        opts: "git_clients.RepositoryOptions",
        parent_ref: str,
    ) -> tuple[list[dict], bool]:
        """读 ``.gitmodules`` 并对每个子模块仓库单独跑识别（复用父项目凭据）。

        相对 url（``../x.git``）按父仓库地址解析后与父仓库同 host 同账号——
        父 token 天然可读；绝对 url 指向异 host 时平台不匹配会取树失败，按
        子模块逐个记 ``error`` 不拖垮整体。返回 ``(子模块结果列表, 是否读到
        .gitmodules)``；``.gitmodules`` 不可读 → ``([], False)`` 让调用方
        置 truncated 提示。每个结果项是紧凑投影（不含 languages/evidence，
        控 JSONB 体积）：``{path, name, url, primary_language, frameworks,
        project_types, package_managers, containers, truncated, scanned_at}``
        或失败时 ``{path, name, url, error, scanned_at}``。
        """
        try:
            blob = await git_clients.fetch_blob(
                platform, full_name, opts, path=".gitmodules", ref=parent_ref
            )
        except git_clients.GitClientError as exc:
            logger.warning(
                "[user-platform] stack scan .gitmodules failed: platform={} project_id={} error={}",
                platform, project.id, exc,
            )
            return [], False
        if blob is None:
            return [], False
        text = _decode_blob_text(blob.content)
        if not text:
            # 读到 .gitmodules 但内容为空/解不开——视作无子模块，不提示。
            return [], True
        sections = parse_gitmodules(text)
        if not sections:
            return [], True
        results: list[dict] = []
        for section in sections[:_SCAN_MAX_SUBMODULES]:
            url = _resolve_submodule_url(project.repo_url, section["url"])
            sub_full_name = _derive_full_name(url)
            entry: dict = {
                "path": section["path"],
                "name": section.get("name") or section["path"],
                "url": url,
            }
            if not sub_full_name:
                results.append({**entry, "error": "unresolvable_url",
                               "scanned_at": datetime.now(timezone.utc).isoformat()})
                continue
            sub_ref = section.get("branch") or ""
            try:
                sub_profile, sub_paths = await self._detect_repo_stack(
                    platform=platform, full_name=sub_full_name, opts=opts, ref=sub_ref
                )
            except git_clients.GitClientError as exc:
                results.append({**entry, "error": str(exc)[:500],
                               "scanned_at": datetime.now(timezone.utc).isoformat()})
                continue
            # 子模块自身声明 .gitmodules（嵌套子模块）——v1 不再下钻，标 truncated。
            nested_hint = any(p == ".gitmodules" for p in sub_paths)
            results.append({
                **entry,
                "primary_language": sub_profile.get("primary_language", ""),
                "frameworks": sub_profile.get("frameworks", []),
                "project_types": sub_profile.get("project_types", []),
                "package_managers": sub_profile.get("package_managers", []),
                "containers": sub_profile.get("containers", []),
                "truncated": bool(sub_profile.get("truncated") or nested_hint),
                "scanned_at": sub_profile.get("scanned_at", ""),
            })
        return results, True

    async def get_tree(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        ref: str = "",
        path: str = "",
        recursive: bool = False,
    ) -> list[dict] | None:
        """List one tree level for a project's repository.

        Access is enforced via :meth:`_get_accessible_project`. Missing repo
        context or any upstream error degrade to an empty list. Returns ``None``
        only when the project is absent or inaccessible (→ 404 at the route).
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        ctx = await self._resolve_repo_context(project)
        if ctx is None:
            return []
        platform, full_name, opts = ctx
        # 请求的路径可能落在一个子模块里。子模块的仓库地址就写在父仓库的
        # .gitmodules 里，父项目的 git 凭据同样可用，所以直接拿那个地址去 git
        # 平台拉树 —— 不需要子模块被注册成平台项目，也不需要额外鉴权：调用方
        # 已经能读父仓库，就能读它自己声明的子模块。
        sub_prefix = ""
        sub_ref = ref
        if (path or "").strip("/"):
            submodule_map = await self._resolve_submodule_map(
                project, ref=ref, user_id=user_id, role=role
            )
            matched = self._match_submodule_path(submodule_map, path)
            if matched is not None:
                info, inner = matched
                sub_full_name = _derive_full_name(info.get("url"))
                if sub_full_name:
                    full_name = sub_full_name
                    sub_prefix = (info.get("path") or "").strip("/")
                    # 子模块的分支只认它自己声明的；父仓库的 ref 在子模块里
                    # 通常不存在，传过去只会 404。
                    sub_ref = str(info.get("branch") or "")
                    path = inner
        try:
            entries = await git_clients.fetch_tree(
                platform, full_name, opts, ref=sub_ref, path=path, recursive=recursive
            )
        except git_clients.GitClientError as exc:
            logger.warning(
                "[user-platform] get tree failed: platform={} full_name={} project_id={} error={}",
                platform,
                full_name,
                project.id,
                exc,
            )
            return []
        # 子模块内部的条目 path 是相对子模块仓库的，补回子模块前缀，前端拿到的
        # 就是一棵连续的树（同一套 path 语义，点下一层还能再传回来）。
        if sub_prefix:
            result: list[dict] = []
            for entry in entries:
                data = entry.to_dict()
                inner_path = str(data.get("path") or "").strip("/")
                data["path"] = f"{sub_prefix}/{inner_path}" if inner_path else sub_prefix
                result.append(data)
            return result
        return [e.to_dict() for e in entries]

    async def get_blob(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        path: str,
        ref: str = "",
    ) -> dict | None:
        """Fetch one file blob (base64 content) for a project's repository.

        Access is enforced via :meth:`_get_accessible_project`. Returns ``None``
        when the project is absent/inaccessible (→ 404), or an empty-content
        blob dict when the repo context/file is unavailable. A non-file target
        yields ``{"content": ""}``.

        子模块路径（如 node_server/xxx.py）被识别后用子模块自己的仓库 URL 拉取。
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        ctx = await self._resolve_repo_context(project)
        if ctx is None:
            return {"content": "", "is_binary": False, "sha": ""}
        platform, full_name, opts = ctx
        # 和 get_tree 同样的子模块识别逻辑：父仓库能读 = 子模块能读。
        sub_ref = ref
        if (path or "").strip("/"):
            submodule_map = await self._resolve_submodule_map(
                project, ref=ref, user_id=user_id, role=role
            )
            matched = self._match_submodule_path(submodule_map, path)
            if matched is not None:
                info, inner = matched
                sub_full_name = _derive_full_name(info.get("url"))
                if sub_full_name:
                    full_name = sub_full_name
                    sub_ref = str(info.get("branch") or "")
                    path = inner
        try:
            blob = await git_clients.fetch_blob(platform, full_name, opts, path=path, ref=sub_ref)
        except git_clients.GitClientError as exc:
            logger.warning(
                "[user-platform] get blob failed: platform={} full_name={} project_id= error={}",
                platform,
                full_name,
                project.id,
                exc,
            )
            return {"content": "", "is_binary": False, "sha": ""}
        if blob is None:
            return {"content": "", "is_binary": False, "sha": ""}
        return blob.to_dict()

    async def list_submodules(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        ref: str = "",
    ) -> list[dict] | None:
        """List the git submodules this project's repository declares.

        Derived read-only from the repository's ``.gitmodules`` — nothing is
        stored. Each relative url is resolved against the project's own
        ``repo_url`` (git's rule), then matched against the projects this
        caller can see so the UI can link to one instead of the remote.

        Returns ``None`` only when the project is absent/inaccessible (→ 404).
        A repository without submodules yields ``[]``, not an error.
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        submap = await self._resolve_submodule_map(project, ref=ref, user_id=user_id, role=role)
        return list(submap.values())

    async def _resolve_submodule_map(
        self,
        project: Project,
        *,
        ref: str,
        user_id: str,
        role: str | None,
    ) -> dict[str, dict]:
        """Read ``.gitmodules`` and match each submodule to a platform project.

        Returns a dict keyed by submodule tree path; each value carries
        ``path``/``name``/``url``/``branch`` and, when matched, ``project_id``
        and ``project_name``. Reused by :meth:`list_submodules` (returns the
        values) and :meth:`get_tree`/:meth:`get_blob` (to route submodule paths
        to the submodule's own repo).

        直接走 ``git_clients.fetch_blob`` 读 ``.gitmodules``，**不经过**
        :meth:`get_blob`：get_blob 会因为 path 非空再调本方法做子模块识别，
        而 ``.gitmodules`` 本身不在任何子模块下，那样只会无限递归。
        """
        ctx = await self._resolve_repo_context(project)
        if ctx is None:
            return {}
        platform, full_name, opts = ctx
        try:
            blob = await git_clients.fetch_blob(
                platform, full_name, opts, path=".gitmodules", ref=ref
            )
        except git_clients.GitClientError as exc:
            logger.warning(
                "[user-platform] fetch .gitmodules failed: platform={} project_id={} error={}",
                platform,
                project.id,
                exc,
            )
            return {}
        content = str((blob.to_dict() if blob else {}).get("content") or "")
        if not content:
            return {}
        try:
            text = base64.b64decode(content, validate=True).decode("utf-8", errors="replace")
        except (binascii.Error, ValueError):
            return {}
        sections = parse_gitmodules(text)
        if not sections:
            return {}
        ids = await self._accessible_project_ids(user_id, role)
        query = Project.all() if ids is None else Project.filter(id__in=list(ids))
        by_identity: dict[tuple[str, str], Project] = {}
        for candidate in await query:
            if str(candidate.id) == str(project.id) or not candidate.repo_url:
                continue
            by_identity.setdefault(_repo_identity(candidate.repo_url), candidate)
        result: dict[str, dict] = {}
        for section in sections:
            url = _resolve_submodule_url(project.repo_url, section["url"])
            item: dict = {
                "path": section["path"],
                "name": section.get("name") or section["path"],
                "url": url,
                "branch": section.get("branch") or "",
            }
            matched = by_identity.get(_repo_identity(url))
            if matched is not None:
                item["project_id"] = str(matched.id)
                item["project_name"] = matched.name
            result[section["path"]] = item
        return result

    # ---- Projects ---------------------------------------------------------
    async def _accessible_project_ids(self, user_id: str, role: str | None) -> set | None:
        """Ids of every project this caller may see, or ``None`` for "all".

        Owned + collaborator + team-shared. ``None`` means the caller is
        privileged and no id filter applies.
        """
        if self._is_privileged(role):
            return None
        collab_rows = await ProjectCollaborator.filter(
            user_id=uuid.UUID(user_id)
        ).values_list("project_id", flat=True)
        owned = await Project.filter(user_id=uuid.UUID(user_id)).values_list("id", flat=True)
        ids = set(collab_rows) | set(owned)
        team_id = None
        try:
            from .deps import resolve_team_id
            team_id = await resolve_team_id(user_id)
        except Exception:
            team_id = None
        if team_id:
            team_shared_rows = await Project.filter(
                is_team_shared=True, team_id=uuid.UUID(team_id)
            ).values_list("id", flat=True)
            ids |= set(team_shared_rows)
        return ids

    async def list_projects(
        self, user_id: str, *, role: str | None = None, page: int = 1, page_size: int = 24
    ) -> PageResult:
        page = max(1, page)
        page_size = max(1, min(page_size, 200))
        ids = await self._accessible_project_ids(user_id, role)
        if ids is None:
            query = Project.all()
        else:
            if not ids:
                return PageResult(total=0, page=page, page_size=page_size, rows=[])
            query = Project.filter(id__in=list(ids))
        total = await query.count()
        rows = await query.order_by("-created_at").offset((page - 1) * page_size).limit(page_size)
        return PageResult(
            total=total,
            page=page,
            page_size=page_size,
            rows=await _enrich_project_rows([_project_dict(r) for r in rows]),
        )

    async def get_project(self, user_id: str, project_id: str, *, role: str | None = None) -> dict | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        data = (await _enrich_project_rows([_project_dict(project)]))[0]
        data["access_role"] = access
        try:
            from .models_webhook import ProjectWebhook

            webhook = await ProjectWebhook.get_or_none(project_id=project.id)
            data["auto_review_enabled"] = bool(webhook and webhook.review_enabled)
        except Exception:
            data["auto_review_enabled"] = False
        return data

    async def create_project(self, user_id: str, req: dict) -> dict:
        name = (req.get("name") or "").strip()
        if not name:
            raise ValueError("name_required")
        # 团队归属：建项目时从 owner 解析 team，供团队共享可见性使用。
        team_id = None
        try:
            from .deps import resolve_team_id
            team_id = _maybe_uuid(await resolve_team_id(user_id))
        except Exception:
            team_id = None
        project = await Project.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            name=name,
            description=req.get("description") or None,
            platform=req.get("platform") or None,
            repo_url=req.get("repo_url") or None,
            branch=req.get("branch") or None,
            git_identity_id=_maybe_uuid(req.get("git_identity_id")),
            env_variables=req.get("env_variables") or None,
            is_team_shared=bool(req.get("is_team_shared")),
            team_id=team_id,
        )
        return _project_dict(project)

    async def update_project(
        self, user_id: str, project_id: str, req: dict, *, role: str | None = None
    ) -> bool:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return False
        changed: list[str] = []
        for field in ("name", "description", "platform", "repo_url", "branch"):
            if field in req:
                setattr(project, field, req[field])
                changed.append(field)
        if "env_variables" in req:
            project.env_variables = req["env_variables"]
            changed.append("env_variables")
        # 团队共享开关：仅 owner 可改（access 已校验为 owner/read_write；这里再收紧到 owner）
        if "is_team_shared" in req and access == "owner":
            project.is_team_shared = bool(req["is_team_shared"])
            changed.append("is_team_shared")
        if not changed:
            return True
        changed.append("updated_at")
        await project.save(update_fields=changed)
        return True

    async def delete_project(self, user_id: str, project_id: str, *, role: str | None = None) -> bool:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access != "owner":
            return False
        issue_ids = await ProjectIssue.filter(project_id=project.id).values_list("id", flat=True)
        if issue_ids:
            await ProjectIssueComment.filter(issue_id__in=list(issue_ids)).delete()
        await ProjectIssue.filter(project_id=project.id).delete()
        await ProjectCollaborator.filter(project_id=project.id).delete()
        # Associations are bidirectional references with no FK cascade — clean
        # both directions so no dangling links survive project deletion.
        await ProjectAssociation.filter(source_project_id=project.id).delete()
        await ProjectAssociation.filter(target_project_id=project.id).delete()
        await project.delete()
        return True

    # ---- Collaborators ----------------------------------------------------
    async def list_collaborators(
        self, user_id: str, project_id: str, *, role: str | None = None
    ) -> list[dict] | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        rows = await ProjectCollaborator.filter(project_id=project.id).order_by("created_at")
        return [_collaborator_dict(r) for r in rows]

    async def add_collaborator(
        self, user_id: str, project_id: str, target_user_id: str, collab_role: str, *, role: str | None = None
    ) -> dict | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access != "owner":
            return None
        target = _maybe_uuid(target_user_id)
        if target is None:
            raise ValueError("invalid_user_id")
        collab_role = collab_role if collab_role in ("read_only", "read_write") else "read_only"
        existing = await ProjectCollaborator.get_or_none(project_id=project.id, user_id=target)
        if existing:
            existing.role = collab_role
            await existing.save(update_fields=["role", "updated_at"])
            return _collaborator_dict(existing)
        collab = await ProjectCollaborator.create(
            id=uuid.uuid4(), project_id=project.id, user_id=target, role=collab_role
        )
        return _collaborator_dict(collab)

    async def remove_collaborator(
        self, user_id: str, project_id: str, target_user_id: str, *, role: str | None = None
    ) -> bool:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access != "owner":
            return False
        target = _maybe_uuid(target_user_id)
        if target is None:
            return False
        deleted = await ProjectCollaborator.filter(project_id=project.id, user_id=target).delete()
        return deleted > 0

    # ---- Associations -------------------------------------------------------
    async def _resolve_repo_context_for(
        self, target_project: Project, identity_id: uuid.UUID | None
    ) -> tuple[str, str, git_clients.RepositoryOptions] | None:
        """Resolve ``(platform, full_name, opts)`` for ``target_project`` but
        authenticate with an explicitly supplied identity (the *source*
        project's), not the target's own. Used to probe the target repo with the
        source project's credentials — the token-based permission boundary."""
        if not identity_id:
            return None
        identity = await git_service.load_identity_for_read(str(identity_id))
        if identity is None or not identity.access_token:
            return None
        platform = (identity.platform or target_project.platform or "").lower()
        if not git_clients.supports_platform(platform):
            return None
        full_name = _derive_full_name(target_project.repo_url, target_project.platform)
        if not full_name:
            return None
        opts = git_service._build_repo_options(identity)
        return platform, full_name, opts

    async def _probe_target_capability(
        self, source_project: Project, target_project: Project
    ) -> tuple[bool, bool]:
        """Probe ``(can_read, can_write)`` on the target repo using the SOURCE
        project's git identity token. Fails closed to (False, False)."""
        ctx = await self._resolve_repo_context_for(target_project, source_project.git_identity_id)
        if ctx is None:
            return False, False
        platform, full_name, opts = ctx
        try:
            return await git_clients.fetch_repo_capability(platform, full_name, opts)
        except git_clients.GitClientError:
            return False, False

    async def list_associations(
        self, user_id: str, project_id: str, *, role: str | None = None
    ) -> list[dict] | None:
        """List associations where this project is the source (A references B).

        Anyone with access to A can view the list. Each entry resolves repo-level
        ``can_read``/``can_write`` by probing B's repository with A's git identity
        token. Only when ``can_read`` is the target's repo_url exposed — otherwise
        name+id suffice for an in-app navigate without leaking the remote.
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        rows = await ProjectAssociation.filter(source_project_id=project.id).order_by("created_at")
        if not rows:
            return []
        target_ids = [a.target_project_id for a in rows]
        targets = {
            str(p.id): p
            for p in await Project.filter(id__in=target_ids)
        }
        result: list[dict] = []
        for a in rows:
            item = _association_dict(a)
            target = targets.get(str(a.target_project_id))
            if target is None:
                item.update({"can_read": False, "can_write": False, "target_project": {"id": str(a.target_project_id), "name": "(deleted)"}})
                result.append(item)
                continue
            can_read, can_write = await self._probe_target_capability(project, target)
            item["can_read"] = can_read
            item["can_write"] = can_write
            item["target_project"] = {
                "id": str(target.id),
                "name": target.name,
                "platform": target.platform,
                "full_name": _derive_full_name(target.repo_url, target.platform),
                "description": target.description,
                **({"repo_url": target.repo_url} if can_read else {}),
            }
            result.append(item)
        return result

    async def add_association(
        self,
        user_id: str,
        project_id: str,
        target_project_id: str,
        relation: str = "related",
        target_subdir: str | None = None,
        *,
        role: str | None = None,
    ) -> dict | None:
        """Create the directed association A → B. Owner-only on A.

        Self-association is rejected. A duplicate (same source+target) updates
        the relation/subdir rather than erroring (idempotent upsert).
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access != "owner":
            return None
        target_uuid = _maybe_uuid(target_project_id)
        if target_uuid is None:
            raise ValueError("invalid_target_project_id")
        if str(target_uuid) == str(project.id):
            raise ValueError("self_association_not_allowed")
        # Association is a reference only; the target need not be accessible to
        # the caller — repo access is probed at read/execute time with A's token.
        target = await Project.get_or_none(id=target_uuid)
        if target is None:
            raise ValueError("target_project_not_found")
        relation = (relation or "related").strip() or "related"
        subdir = (target_subdir or "").strip() or None
        existing = await ProjectAssociation.get_or_none(
            source_project_id=project.id, target_project_id=target_uuid
        )
        if existing:
            existing.relation = relation
            existing.target_subdir = subdir
            await existing.save(update_fields=["relation", "target_subdir", "updated_at"])
            return _association_dict(existing)
        assoc = await ProjectAssociation.create(
            id=uuid.uuid4(),
            source_project_id=project.id,
            target_project_id=target_uuid,
            relation=relation,
            target_subdir=subdir,
        )
        return _association_dict(assoc)

    async def remove_association(
        self, user_id: str, project_id: str, target_project_id: str, *, role: str | None = None
    ) -> bool:
        """Remove the association A → B. Owner-only on A."""
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access != "owner":
            return False
        target_uuid = _maybe_uuid(target_project_id)
        if target_uuid is None:
            return False
        deleted = await ProjectAssociation.filter(
            source_project_id=project.id, target_project_id=target_uuid
        ).delete()
        return deleted > 0

    # ---- Issues -----------------------------------------------------------
    async def list_issues(
        self,
        user_id: str,
        project_id: str,
        *,
        role: str | None = None,
        issue_type: str | None = None,
        status: str | None = None,
        priority: int | None = None,
        only_assigned_to_me: bool = False,
    ) -> list[dict] | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        query = ProjectIssue.filter(project_id=project.id)
        if issue_type:
            if issue_type not in ISSUE_TYPES:
                raise ValueError("invalid_issue_type")
            query = query.filter(issue_type=issue_type)
        if status:
            query = query.filter(status=status)
        if priority is not None:
            if priority not in (1, 2, 3):
                raise ValueError("invalid_priority")
            query = query.filter(priority=priority)
        # 可见性：owner/admin/创建者看全部；其他协作者/团队成员只看分配给自己的。
        # only_assigned_to_me 显式收窄（前端"分配给我"开关）。
        is_owner_or_privileged = access == "owner" or self._is_privileged(role)
        if only_assigned_to_me or not is_owner_or_privileged:
            query = query.filter(assignee_id=uuid.UUID(user_id))
        rows = await query.order_by("-created_at")
        return [_issue_dict(r) for r in rows]

    async def create_issue(
        self, user_id: str, project_id: str, req: dict, *, role: str | None = None
    ) -> dict | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return None
        title = (req.get("title") or "").strip()
        if not title:
            raise ValueError("title_required")
        # 分配者必填：非 owner/创建者只看分配给自己的需求，缺 assignee 会让需求对这些人不可见。
        assignee = _maybe_uuid(req.get("assignee_id"))
        if assignee is None:
            raise ValueError("assignee_required")
        priority = req.get("priority")
        priority = priority if priority in (1, 2, 3) else 2
        issue_type = _normalize_issue_type(req.get("type"))
        # New issues always start at the flow's initial state; callers cannot
        # jump straight to a mid-flow status.
        tags_raw = req.get("tags")
        tags = [str(t).strip() for t in tags_raw if str(t).strip()] if isinstance(tags_raw, list) else []
        issue = await ProjectIssue.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            project_id=project.id,
            issue_type=issue_type,
            status=_initial_status(issue_type),
            title=title,
            requirement_document=req.get("requirement_document") or None,
            design_document=req.get("design_document") or None,
            bug_reason=req.get("bug_reason") or None,
            pending_items=_normalize_pending_items(req.get("pending_items")),
            resolution_note=req.get("resolution_note") or None,
            summary=req.get("summary") or None,
            assignee_id=assignee,
            priority=priority,
            tags=tags,
        )
        return _issue_dict(issue)

    async def update_issue(
        self, user_id: str, project_id: str, issue_id: str, req: dict, *, role: str | None = None
    ) -> bool:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return False
        iid = _maybe_uuid(issue_id)
        if iid is None:
            return False
        issue = await ProjectIssue.get_or_none(id=iid, project_id=project.id)
        if issue is None:
            return False
        changed: list[str] = []
        # Status change must be validated against the issue's own flow before we
        # touch anything else, so an illegal move rejects the whole update.
        if "status" in req and req["status"] != issue.status:
            _validate_transition(issue.issue_type, issue.status, str(req["status"]))
            issue.status = str(req["status"])
            changed.append("status")
            # Stamp closed_at once when the issue reaches a closing state.
            if issue.status in _CLOSING_STATES and issue.closed_at is None:
                issue.closed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                changed.append("closed_at")
        for field in ("title", "requirement_document", "design_document",
                      "bug_reason", "resolution_note", "summary"):
            if field in req:
                setattr(issue, field, req[field])
                changed.append(field)
        if "pending_items" in req:
            issue.pending_items = _normalize_pending_items(req["pending_items"])
            changed.append("pending_items")
        if "priority" in req and req["priority"] in (1, 2, 3):
            issue.priority = req["priority"]
            changed.append("priority")
        if "assignee_id" in req:
            issue.assignee_id = _maybe_uuid(req["assignee_id"])
            changed.append("assignee_id")
        if "tags" in req:
            tags_raw = req["tags"]
            issue.tags = [str(t).strip() for t in tags_raw if str(t).strip()] if isinstance(tags_raw, list) else []
            changed.append("tags")
        if not changed:
            return True
        changed.append("updated_at")
        await issue.save(update_fields=changed)
        return True

    # ---- Issue comments ---------------------------------------------------
    async def list_comments(
        self, user_id: str, project_id: str, issue_id: str, *, role: str | None = None
    ) -> list[dict] | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None:
            return None
        iid = _maybe_uuid(issue_id)
        if iid is None:
            return None
        # IDOR 防护：校验 issue 属于该 project，否则传别项目的 issue_id 能读到其评论。
        if await ProjectIssue.get_or_none(id=iid, project_id=project.id) is None:
            return None
        rows = await ProjectIssueComment.filter(issue_id=iid).order_by("created_at")
        return [_comment_dict(r) for r in rows]

    async def add_comment(
        self, user_id: str, project_id: str, issue_id: str, req: dict, *, role: str | None = None
    ) -> dict | None:
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return None
        iid = _maybe_uuid(issue_id)
        if iid is None:
            return None
        # IDOR 防护：评论只能挂在属于该 project 的 issue 上。
        if await ProjectIssue.get_or_none(id=iid, project_id=project.id) is None:
            return None
        text = (req.get("comment") or "").strip()
        if not text:
            raise ValueError("comment_required")
        comment = await ProjectIssueComment.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            issue_id=iid,
            parent_id=_maybe_uuid(req.get("parent_id")),
            comment=text,
        )
        return _comment_dict(comment)

    # ---- Reassign (改派) ----------------------------------------------------
    async def reassign_issue(
        self, user_id: str, project_id: str, issue_id: str, target_user_id: str, *, role: str | None = None
    ) -> dict | None:
        """把需求改派给另一个人。区别于 assign_issue：不 spawn task、不推状态，只换 assignee。

        需 owner 或 read_write 权限；不校验目标用户是否在团队内（前端从成员列表选）。
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return None
        iid = _maybe_uuid(issue_id)
        if iid is None:
            return None
        target = _maybe_uuid(target_user_id)
        if target is None:
            raise ValueError("invalid_user_id")
        issue = await ProjectIssue.get_or_none(id=iid, project_id=project.id)
        if issue is None:
            return None
        issue.assignee_id = target
        issue.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)  # type: ignore[assignment]
        await issue.save(update_fields=["assignee_id", "updated_at"])
        return _issue_dict(issue)

    # ---- Issue assignment / confirmation -----------------------------------
    async def _existing_role_task(self, issue_id: uuid.UUID, task_role: str) -> dict | None:
        """Return the live task already filling this slot, if any.

        Idempotency guard: clicking 分配 twice (or a retried request) must not
        spawn a second task for the same issue+role. Only non-terminal tasks
        count — once a task finished or errored the slot is free to be retried.
        """
        bindings = await ProjectTask.filter(issue_id=issue_id, task_role=task_role)
        if not bindings:
            return None
        # 一次取回所有候选 task，避免在循环里逐条 get_or_none 产生 N+1。
        tasks = await Task.filter(
            id__in=[b.task_id for b in bindings]
        ).only("id", "status")
        for task in tasks:
            if task.status in ("pending", "processing"):
                return {"id": str(task.id), "status": task.status}
        return None

    async def _spawn_issue_task(
        self,
        user_id: str,
        project: Project,
        issue: ProjectIssue,
        plan: dict[str, str],
        req: dict,
    ) -> dict:
        """Create the task backing an issue transition.

        Delegates to ``task_service`` so node claiming, dispatch and failure
        handling stay in one place. The prompt is assembled from the issue's own
        documents so the agent starts with the full context.
        """
        payload: dict[str, Any] = {
            "content": str(req.get("content") or "").strip() or _issue_prompt(issue, plan["task_role"]),
            "task_type": plan["task_type"],
            "sub_type": plan["sub_type"],
            "task_role": plan["task_role"],
            "extra": {
                "project_id": str(project.id),
                "issue_id": str(issue.id),
                **({"skill_ids": req["skill_ids"]} if req.get("skill_ids") else {}),
            },
        }
        # Caller-selected runtime knobs pass straight through; mode validity is
        # checked against the node's advertised editor capabilities at dispatch.
        for key in (
            "mode", "mode_label", "mode_capability_snapshot",
            "model_id", "node_id", "cli_name", "git_identity_id",
            "parent_api_key_id", "usage_limit", "expires_at",
            "expected_client_id", "bootstrap_content",
            "skill_config", "mcp_config", "plugin_config",
        ):
            if req.get(key) is not None:
                payload[key] = req[key]
        if project.repo_url:
            payload["repo"] = {
                "repo_url": project.repo_url,
                "branch": req.get("branch") if req.get("branch") is not None else (project.branch or ""),
            }
        return await task_service.create_task(user_id, payload)

    async def assign_issue(
        self, user_id: str, project_id: str, issue_id: str, req: dict, *, role: str | None = None
    ) -> dict | None:
        """Assign an unassigned issue: create its first task and activate it.

        Two-phase by design: the task is created first, and the issue only
        advances once that task actually landed in a live state. A dispatch
        failure leaves the issue untouched so it can be assigned again, rather
        than stranding it in 设计中 with no task behind it.
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return None
        iid = _maybe_uuid(issue_id)
        if iid is None:
            return None
        issue = await ProjectIssue.get_or_none(id=iid, project_id=project.id)
        if issue is None:
            return None
        requested_role = str(req.get("task_role") or "").strip()
        intent = "fix" if requested_role in ("develop", "fix") else "analysis"
        plan = _ASSIGN_PLANS.get(issue.issue_type, {}).get(intent)
        if plan is None:
            raise IssueTransitionError(f"未知类型「{issue.issue_type}」无法分配")
        requested_plan = {
            "task_role": str(req.get("task_role") or "").strip(),
            "task_type": str(req.get("task_type") or "").strip(),
            "sub_type": str(req.get("sub_type") or "").strip(),
        }
        if any(requested_plan.values()) and requested_plan != {
            key: plan[key] for key in ("task_role", "task_type", "sub_type")
        }:
            raise IssueTransitionError("任务类型与需求/bug 类型不匹配")
        if issue.status != "unassigned":
            raise IssueTransitionError(
                f"当前状态「{_STATUS_LABELS.get(issue.status, issue.status)}」不可分配"
            )
        existing = await self._existing_role_task(issue.id, plan["task_role"])
        if existing is not None:
            return {"issue": _issue_dict(issue), "task": existing, "reused": True}

        task = await self._spawn_issue_task(user_id, project, issue, plan, req)
        if task.get("status") not in ("pending", "processing"):
            # Orchestration failed; the task row survives for diagnosis but the
            # issue stays assignable.
            return {"issue": _issue_dict(issue), "task": task, "assigned": False}
        await self._advance(issue, plan["status"], assignee_id=req.get("assignee_id"))
        return {"issue": _issue_dict(issue), "task": task, "assigned": True}

    async def confirm_issue(
        self, user_id: str, project_id: str, issue_id: str, req: dict, *, role: str | None = None
    ) -> dict | None:
        """Human confirm / reject of a pending design document or bug reason.

        ``approve=False`` sends the issue back for rework. Approving optionally
        starts the follow-up task (开发 / 修复) in the same call when
        ``start_task`` is set, which is what the console's 确认并开始 does.
        """
        project, access = await self._get_accessible_project(project_id, user_id, role)
        if project is None or access not in ("owner", "read_write"):
            return None
        iid = _maybe_uuid(issue_id)
        if iid is None:
            return None
        issue = await ProjectIssue.get_or_none(id=iid, project_id=project.id)
        if issue is None:
            return None
        flow = _CONFIRM_FLOW.get(issue.issue_type)
        if flow is None:
            raise IssueTransitionError(f"未知类型「{issue.issue_type}」无法确认")
        pending_state, confirmed_state = flow
        if issue.status != pending_state:
            raise IssueTransitionError(
                f"当前状态「{_STATUS_LABELS.get(issue.status, issue.status)}」无待确认内容"
            )

        if not req.get("approve", True):
            # Send back to the working state so the agent can revise.
            rework = "designing" if issue.issue_type == "requirement" else "diagnosing"
            await self._advance(issue, rework)
            await self._maybe_note(user_id, issue, req.get("note"))
            return {"issue": _issue_dict(issue), "approved": False}

        await self._advance(issue, confirmed_state)
        await self._maybe_note(user_id, issue, req.get("note"))
        result: dict[str, Any] = {"issue": _issue_dict(issue), "approved": True}
        if not req.get("start_task"):
            return result

        plan = _FOLLOWUP_PLAN.get(issue.issue_type, {}).get(confirmed_state)
        if plan is None:
            return result
        existing = await self._existing_role_task(issue.id, plan["task_role"])
        if existing is not None:
            result["task"] = existing
            result["reused"] = True
            return result
        task = await self._spawn_issue_task(user_id, project, issue, plan, req)
        result["task"] = task
        if task.get("status") in ("pending", "processing"):
            await self._advance(issue, plan["status"])
            result["issue"] = _issue_dict(issue)
        return result

    async def _advance(
        self, issue: ProjectIssue, target: str, *, assignee_id: Any = None
    ) -> None:
        """Move an issue to ``target`` through the same validation as a manual
        edit, so assignment can never bypass the state machine."""
        _validate_transition(issue.issue_type, issue.status, target)
        changed = ["status", "updated_at"]
        issue.status = target
        if assignee_id is not None:
            issue.assignee_id = _maybe_uuid(assignee_id)
            changed.append("assignee_id")
        if target in _CLOSING_STATES and issue.closed_at is None:
            issue.closed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            changed.append("closed_at")
        await issue.save(update_fields=changed)

    async def _maybe_note(self, user_id: str, issue: ProjectIssue, note: Any) -> None:
        text = str(note or "").strip()
        if not text:
            return
        await ProjectIssueComment.create(
            id=uuid.uuid4(),
            user_id=uuid.UUID(user_id),
            issue_id=issue.id,
            comment=text,
        )


def _issue_prompt(issue: ProjectIssue, task_role: str) -> str:
    """Assemble the opening prompt for an issue-backed task.

    Includes only the documents that exist, so a bug fix task carries the
    confirmed root cause while a design task carries just the requirement.
    """
    header = {
        "design": f"请为以下需求编写设计文档：{issue.title}",
        "diagnose": f"请定位以下缺陷的根本原因：{issue.title}",
        "develop": f"请按已确认的设计文档实现以下需求：{issue.title}",
        "fix": f"请按已确认的原因修复以下缺陷：{issue.title}",
    }.get(task_role, issue.title)
    blocks = [header]
    for label, body in (
        ("需求描述" if issue.issue_type == "requirement" else "问题现象", issue.requirement_document),
        ("设计文档", issue.design_document),
        ("缺陷原因", issue.bug_reason),
    ):
        text = (body or "").strip()
        if text:
            blocks.append(f"\n## {label}\n{text}")
    pending = [i.get("content") for i in (issue.pending_items or []) if i.get("status") == "pending"]
    if pending:
        blocks.append("\n## 待确认项\n" + "\n".join(f"- {p}" for p in pending))
    return "\n".join(blocks)


project_service = ProjectService()
