"""Node-level system environment (``env_mode="system"``) visibility and maintenance.

The system tier runs the editor against the node operator's REAL HOME, so the
providers already discover whatever the operator installed there (claude reads
``~/.claude/skills``, the neutral runners read ``~/.agents/{skills,plugins}``,
gemini ``~/.gemini/extensions``, MCP ``~/.mcp.json``). None of that was visible
server-side: a system-env task silently got capabilities nobody could see, list,
or manage. This module closes that gap.

Why it is NOT :mod:`environment_service` with a different id
------------------------------------------------------------
A shared environment is a directory the node created and owns, so an exact-set
sync (install the desired list, prune the rest) is correct there. The operator's
HOME is not ours. Everything here follows from that:

* **Read first.** The inventory is the product — the console finally shows what a
  system-env session actually gets.
* **Incremental writes.** Installing never prunes. An existing entry is skipped
  unless the user explicitly chose to overwrite, because the copy on disk may be
  the operator's own work.
* **Ownership-scoped removal.** The node tracks what the platform installed in its
  own manifest and refuses to delete anything else — so the boundary is enforced
  on the node, not merely in this module's SQL.

Two directions
--------------
* node → server: :func:`refresh_system_env` (scan + record) and
  :func:`archive_system_env_resource` (pack a local resource into the platform
  library so other nodes/environments can reuse it).
* server → node: :func:`install_to_system_env` (put a granted platform resource
  into the operator's HOME) and :func:`remove_from_system_env` (node-level
  uninstall: delete the files AND drop the platform-side record).

MCP is reported but never written: materializing MCP config into the operator's
HOME would put per-task credentials in a directory other things read.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from .models_environment import (
    ENV_RESOURCE_MCP,
    ENV_RESOURCE_PLUGIN,
    ENV_RESOURCE_SKILL,
    NodeSystemEnvEntry,
)

# Kinds that exist as files and can therefore be installed/removed/archived. MCP
# is deliberately excluded from every write path (see the module docstring).
_FILE_KINDS = (ENV_RESOURCE_SKILL, ENV_RESOURCE_PLUGIN)
_KINDS = (ENV_RESOURCE_SKILL, ENV_RESOURCE_PLUGIN, ENV_RESOURCE_MCP)

# Mirror module names used by resource_mirror_store / resources_api.
_KIND_TO_MODULE = {ENV_RESOURCE_SKILL: "skills", ENV_RESOURCE_PLUGIN: "plugins"}


class SystemEnvError(Exception):
    """Domain failure with a stable code the routes map onto an HTTP status."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _normalize_kind(value: str, *, file_only: bool = False) -> str:
    kind = str(value or "").strip().lower()
    allowed = _FILE_KINDS if file_only else _KINDS
    if kind not in allowed:
        raise SystemEnvError("invalid_argument", f"不支持的资源类型 {kind or '(空)'}")
    return kind


def _entry_dict(row: NodeSystemEnvEntry) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "kind": row.kind,
        "name": row.name,
        "version": row.version or "",
        "provider": row.provider or "",
        "path": row.path or "",
        "platform_managed": bool(row.platform_managed),
        "archived_reference_id": str(row.archived_reference_id) if row.archived_reference_id else None,
        "reported_at": row.reported_at.isoformat() if row.reported_at else None,
    }


async def _require_node(user_id: str, node_id: str) -> dict:
    """Authorize the caller for this node and return its live info.

    Two checks, both necessary: the grant check is the anti-horizontal-access gate
    (a user may only touch nodes their groups were granted), and the capability
    check keeps us from asking a node to read/write an operator HOME it never
    opted into — the node would refuse anyway, but a 412 here is a far better
    message than a node-side RPC error.
    """
    from .nodes_service import nodes_service

    node_id = (node_id or "").strip()
    if not node_id:
        raise SystemEnvError("invalid_argument", "node_id 不能为空")
    if await nodes_service.user_can_use_node(user_id, node_id) is None:
        raise SystemEnvError("permission_denied", "无权使用该节点")
    live = await nodes_service.node_live_info(node_id) or {}
    caps = live.get("capabilities") or {}
    if caps and caps.get("system_env") != "true":
        raise SystemEnvError(
            "failed_precondition",
            "该节点未开启系统内置环境（宿主机安装默认开启，容器节点默认关闭）",
        )
    return live


# ── node → server: scan and record ───────────────────────────────────────────


async def refresh_system_env(user_id: str, node_id: str, *, provider: str = "") -> list[dict]:
    """Ask the node what is in its operator HOME and replace the stored snapshot.

    Wholesale replacement is the point: this table is *observed* state, so a
    resource the operator deleted must disappear here rather than linger. The
    ``archived_reference_id`` of surviving entries is carried over, because
    "already in the library" is a fact about the platform, not about the disk.
    """
    await _require_node(user_id, node_id)
    from .nodes_service import nodes_service

    result = await nodes_service.inspect_system_env(node_id, provider)
    observed = [item for item in (result.get("installed") or []) if isinstance(item, dict)]

    previous = await NodeSystemEnvEntry.filter(node_id=node_id)
    archived_by_key = {
        (row.kind, row.name): row.archived_reference_id
        for row in previous
        if row.archived_reference_id
    }
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    await NodeSystemEnvEntry.filter(node_id=node_id).delete()
    rows: list[NodeSystemEnvEntry] = []
    for item in observed:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        kind = str(item.get("kind") or "").strip().lower() or ENV_RESOURCE_SKILL
        rows.append(
            await NodeSystemEnvEntry.create(
                id=uuid.uuid4(),
                node_id=node_id,
                kind=kind,
                name=name,
                version=str(item.get("version") or ""),
                provider=str(item.get("provider") or ""),
                path=str(item.get("path") or ""),
                platform_managed=bool(item.get("platform_managed")),
                archived_reference_id=archived_by_key.get((kind, name)),
                reported_at=now,
            )
        )
    return [_entry_dict(row) for row in rows]


async def system_env_detail(user_id: str, node_id: str) -> dict:
    """The stored inventory for one node, grouped by kind.

    Reads the snapshot rather than hitting the node, so opening the panel is cheap
    and works while the node is offline; the caller refreshes explicitly.
    """
    from .nodes_service import nodes_service

    node_id = (node_id or "").strip()
    if not node_id:
        raise SystemEnvError("invalid_argument", "node_id 不能为空")
    if await nodes_service.user_can_use_node(user_id, node_id) is None:
        raise SystemEnvError("permission_denied", "无权使用该节点")
    live = await nodes_service.node_live_info(node_id) or {}
    caps = live.get("capabilities") or {}

    rows = await NodeSystemEnvEntry.filter(node_id=node_id).order_by("kind", "name")
    grouped: dict[str, list[dict]] = {kind: [] for kind in _KINDS}
    for row in rows:
        grouped.setdefault(row.kind, []).append(_entry_dict(row))
    return {
        "node_id": node_id,
        # The panel needs both: enabled=false explains an empty inventory, and
        # online=false explains why refresh/install will fail right now.
        "system_env_enabled": caps.get("system_env") == "true",
        "online": bool(live.get("online")),
        "resources": grouped,
        "last_reported_at": max(
            (row.reported_at.isoformat() for row in rows if row.reported_at), default=None
        ),
    }


# ── server → node: install and remove ────────────────────────────────────────


async def install_to_system_env(
    user_id: str,
    node_id: str,
    *,
    kind: str,
    resource_id: str,
    overwrite: bool,
    team_id: str,
    request_base_url: str = "",
) -> list[dict]:
    """Install one granted platform resource into the operator's HOME.

    The authorization is the same rule tasks follow — the reference must be
    visible to the caller's groups — so system-env management cannot smuggle in a
    resource the user was never granted. The spec is rebuilt server-side from the
    stored manifest (mirror-aware), never from client fields.
    """
    kind = _normalize_kind(kind, file_only=True)
    if not str(resource_id or "").strip():
        raise SystemEnvError("invalid_argument", "resource_id 不能为空")
    await _require_node(user_id, node_id)

    from .resource_reference_service import resolve_reference_specs
    from .nodes_service import nodes_service

    bindings = [{"resource_id": str(resource_id)}]
    resolved = await resolve_reference_specs(
        user_id, team_id, kind, bindings, request_base_url=request_base_url
    )
    if not resolved:
        raise SystemEnvError("invalid_argument", "资源无法解析为可安装的 spec")

    skills = resolved if kind == ENV_RESOURCE_SKILL else []
    plugins = resolved if kind == ENV_RESOURCE_PLUGIN else []
    result = await nodes_service.sync_system_env(
        node_id, skills, plugins, overwrite=overwrite, remove=[]
    )
    touched = [item for item in (result.get("touched") or []) if isinstance(item, dict)]
    logger.info("[system-env] install on {} of {} {}: {}",
                node_id, len(skills) + len(plugins), kind,
                [t.get("path") for t in touched])
    # Refresh right away so the panel reflects the new file without a second click.
    try:
        await refresh_system_env(user_id, node_id)
    except Exception as exc:  # noqa: BLE001
        logger.info("[system-env] post-install refresh failed for {}: {}", node_id, exc)
    return touched


async def remove_from_system_env(
    user_id: str, node_id: str, *, kind: str, name: str
) -> dict:
    """Node-level uninstall: delete the files AND drop the platform-side record.

    Only platform-installed entries qualify — the node enforces the same boundary
    against its manifest, so this check is UX (a clean 412 instead of an RPC
    error), not the security boundary. That matters: an operator's hand-installed
    resource is reported and never removable through this API, whatever this
    module says.
    """
    kind = _normalize_kind(kind, file_only=True)
    name = str(name or "").strip()
    if not name:
        raise SystemEnvError("invalid_argument", "name 不能为空")
    await _require_node(user_id, node_id)

    row = await NodeSystemEnvEntry.get_or_none(node_id=node_id, kind=kind, name=name)
    if row is None:
        raise SystemEnvError("not_found", "该资源不在系统环境清单里")
    if not row.platform_managed:
        raise SystemEnvError(
            "permission_denied",
            "该资源是节点操作者本机安装的，平台不能删除",
        )

    from .nodes_service import nodes_service

    await nodes_service.sync_system_env(
        node_id, [], [], overwrite=False, remove=[f"{kind}/{name}"]
    )
    archived = row.archived_reference_id
    await row.delete()
    if archived:
        # The library copy is independent of the disk copy; removing the local
        # files does not withdraw the archived resource other nodes may use.
        logger.info("[system-env] removed {} {} on {} (library reference {} kept)",
                    kind, name, node_id, archived)
    else:
        logger.info("[system-env] removed {} {} on {}", kind, name, node_id)
    return {"removed": True, "kind": kind, "name": name}


# ── node → server: archive into the platform library ─────────────────────────

_UPLOAD_TOKEN_TTL_SECONDS = 600


async def archive_system_env_resource(
    user_id: str, node_id: str, *, kind: str, name: str, team_id: str
) -> dict:
    """Pack a locally installed resource and enter it into the platform library.

    The user-initiated half of "从节点同步并上传到服务器": the node tars the
    directory and POSTs it to a one-time upload endpoint; the server stores the
    archive as a mirror and creates a team resource reference for it, so the
    resource becomes installable on other nodes and into shared environments.
    Re-archiving an already-archived entry refreshes the mirror in place.
    """
    kind = _normalize_kind(kind, file_only=True)
    name = str(name or "").strip()
    if not name:
        raise SystemEnvError("invalid_argument", "name 不能为空")
    module = _KIND_TO_MODULE[kind]
    await _require_node(user_id, node_id)

    row = await NodeSystemEnvEntry.get_or_none(node_id=node_id, kind=kind, name=name)
    if row is None:
        raise SystemEnvError("not_found", "该资源不在系统环境清单里")

    from . import system_env_upload
    from .nodes_service import nodes_service

    # Stable library key derived from (node, kind, name): re-archiving the same
    # resource must refresh the mirror and reference in place, not fork a second
    # library entry every time the operator updates their local copy.
    market_id = _archive_market_id(node_id, kind, name)
    upload_token = system_env_upload.issue_upload_token(
        market_id, module, ttl_seconds=_UPLOAD_TOKEN_TTL_SECONDS
    )
    upload_url = f"{system_env_upload.base_url()}/api/v1/resources/system-env/upload"

    try:
        await nodes_service.archive_system_env_resource(
            node_id, kind, name, upload_url, upload_token
        )
    except Exception as exc:  # noqa: BLE001
        raise SystemEnvError("failed_precondition", f"节点上传失败：{exc}") from exc

    # The node POSTed the tar.gz; the upload endpoint staged it under this
    # market_id. Finalize: promote it to a ready mirror and create/refresh the team
    # reference that points at it, which is what makes it installable elsewhere.
    try:
        archive_path, _size, manifest = await system_env_upload.finalize_upload(
            module, market_id, name
        )
        reference = await system_env_upload.upsert_reference_from_manifest(
            team_id=team_id,
            module=module,
            market_id=market_id,
            name=name,
            manifest=manifest,
            created_by=user_id,
            archive_path=archive_path,
        )
    except Exception as exc:  # noqa: BLE001
        raise SystemEnvError("failed_precondition", f"归档入库失败：{exc}") from exc

    row.archived_reference_id = uuid.UUID(str(reference["id"]))
    await row.save(update_fields=["archived_reference_id"])
    logger.info("[system-env] archived {} {} from {} as reference {}",
                kind, name, node_id, reference["id"])
    return {
        "archived": True,
        "kind": kind,
        "name": name,
        "reference_id": str(reference["id"]),
        "reference_name": reference.get("display_name") or reference.get("name"),
    }


def _archive_market_id(node_id: str, kind: str, name: str) -> str:
    """Deterministic library key for an archived system-env resource.

    Deterministic on purpose: re-archiving the same resource must refresh the same
    mirror/reference instead of forking a new library entry each time. Prefixed so
    an archived local resource is recognizable in the library, and reduced to
    ``[A-Za-z0-9_-]`` because this value becomes a filename in the mirror tree —
    dot runs are collapsed too so no ``..`` segment can ever appear there.
    """
    raw = f"sysenv-{node_id}-{kind}-{name}"
    safe = re.sub(r"[^A-Za-z0-9._@-]", "_", raw)
    return re.sub(r"\.+", ".", safe).strip(".")[:180]
