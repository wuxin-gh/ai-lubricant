"""Independent SOP catalog + file CRUD.

SOP bodies are Markdown files under a controlled source directory. Catalog rows
exist only for platform management; the GenericAgent runtime copies the *effective*
SOP set into each Agent's private GA memory directory.

Effective SOPs for one Agent = every built-in catalog SOP (unconditional runtime
default, needs no user install) + custom catalog SOPs explicitly bound to that
Agent via ``AgentSopBinding`` + Agent-private distilled SOPs under
``memory/sop/`` (written by ``start_long_term_update`` or seeded on first MCP
use, never stored in the catalog).

There is no "enabled = installed for all Agents" global semantic: a custom SOP
is synced to an Agent only when an ``AgentSopBinding`` row exists for that
(agent_id, sop_id) pair. The resource page's per-Agent install interaction is
not wired yet — the binding helpers below are the backend capability future UIs
will call. Built-in SOPs remain auto-effective for every Agent.
"""
from __future__ import annotations

import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any

from monkeycode_compat.models_skill import AgentSop, AgentSopBinding

_SOP_SOURCE_ROOT = Path(os.getenv("GENERIC_AGENT_SOP_SOURCE_ROOT", "data/agent-sops")).resolve()
_BUILTIN_ROOT = Path(__file__).resolve().parent / "sop"
_MAX_SOP_BYTES = 512 * 1024


def _slug(value: str) -> str:
    result = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip()).strip(".-")
    return result or "sop"


def _source_path(file_ref: str) -> Path:
    ref = str(file_ref or "").replace("\\", "/").strip()
    path = Path(ref)
    if not ref or path.is_absolute() or ".." in path.parts or path.suffix.lower() != ".md":
        raise PermissionError("Invalid SOP file reference")
    target = (_SOP_SOURCE_ROOT / path).resolve()
    if not target.is_relative_to(_SOP_SOURCE_ROOT):
        raise PermissionError("SOP file escapes source directory")
    return target


def _atomic_write(path: Path, content: str) -> None:
    data = content.encode("utf-8")
    if len(data) > _MAX_SOP_BYTES:
        raise ValueError("SOP is too large")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=str(path.parent), delete=False) as handle:
        handle.write(data)
        temp = handle.name
    os.replace(temp, path)


def _present(row: AgentSop) -> dict[str, Any]:
    path = _source_path(row.file_ref)
    return {
        "id": str(row.id),
        "name": row.name,
        "description": row.description or "",
        "filename": row.file_ref,
        "enabled": bool(row.enabled),
        "is_builtin": bool(row.is_builtin),
        "revision": int(row.revision or 1),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "exists": path.is_file(),
    }


async def ensure_builtin_sops() -> None:
    """Seed existing ``agent/sop/*.md`` into the independent SOP catalog.

    Built-in SOPs are authoritative defaults sourced from ``agent/sop/*.md``.
    On each startup we refresh the catalog copies from the source so renamed
    tools / new SOPs propagate without requiring a manual reset. Administrator
    edits live on catalog rows created via ``create_sop`` (file_ref under
    ``custom/...``), never under ``builtin/``, so this refresh is safe.
    """
    _SOP_SOURCE_ROOT.mkdir(parents=True, exist_ok=True)
    if not _BUILTIN_ROOT.is_dir():
        return
    for source in sorted(_BUILTIN_ROOT.glob("*.md")):
        destination = _source_path(f"builtin/{source.name}")
        _atomic_write(destination, source.read_text(encoding="utf-8"))
        name = source.stem.replace("_", " ").strip().title()
        await AgentSop.update_or_create(
            defaults={
                "name": name,
                "description": "Built-in GenericAgent SOP",
                "scope_type": "global",
                "scope_id": "global",
                "enabled": True,
                "is_builtin": True,
            },
            file_ref=f"builtin/{source.name}",
        )


async def list_catalog(*, include_disabled: bool = True) -> list[dict[str, Any]]:
    query = AgentSop.all().order_by("is_builtin", "name")
    if not include_disabled:
        query = query.filter(enabled=True)
    rows = await query
    return [_present(row) for row in rows if _source_path(row.file_ref).is_file()]


async def create_sop(name: str, description: str, content: str, *, created_by: str | None = None) -> dict[str, Any]:
    ref = f"custom/{_slug(name)}-{uuid.uuid4().hex[:8]}.md"
    _atomic_write(_source_path(ref), content)
    row = await AgentSop.create(
        name=name.strip(), description=description.strip(), file_ref=ref,
        scope_type="global", scope_id="global",
        created_by=uuid.UUID(created_by) if created_by else None,
        enabled=True, is_builtin=False,
    )
    return _present(row)


async def update_sop(sop_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    row = await AgentSop.get_or_none(id=uuid.UUID(str(sop_id)))
    if row is None:
        raise FileNotFoundError("SOP not found")
    expected = patch.get("expected_revision")
    if expected is not None and int(expected) != int(row.revision or 1):
        raise ValueError("SOP revision conflict")
    if "content" in patch and patch["content"] is not None:
        _atomic_write(_source_path(row.file_ref), str(patch["content"]))
    for field in ("name", "description", "enabled"):
        if field in patch and patch[field] is not None:
            setattr(row, field, patch[field])
    row.revision = int(row.revision or 1) + 1
    await row.save()
    return _present(row)


async def delete_sop(sop_id: str) -> bool:
    row = await AgentSop.get_or_none(id=uuid.UUID(str(sop_id)))
    if row is None:
        return False
    if row.is_builtin:
        raise ValueError("Built-in SOP cannot be deleted")
    path = _source_path(row.file_ref)
    if path.exists():
        path.unlink()
    await row.delete()
    return True


async def get_content(sop_id: str) -> str:
    row = await AgentSop.get_or_none(id=uuid.UUID(str(sop_id)))
    if row is None:
        raise FileNotFoundError("SOP not found")
    path = _source_path(row.file_ref)
    if not path.is_file():
        raise FileNotFoundError("SOP file not found")
    return path.read_text(encoding="utf-8")


async def list_for_agent(agent_id: int) -> dict[str, Any]:
    """Read-only view of the SOPs in effect for one Agent.

    Effective SOPs = every built-in catalog SOP (unconditional runtime default)
    + custom catalog SOPs explicitly bound to this Agent via ``AgentSopBinding``
    + Agent-private distilled SOPs under ``memory/sop/`` that are not in the
    catalog (written by ``start_long_term_update`` or seeded on first MCP use).
    Unbound custom SOPs are NOT effective for this Agent.
    """
    from agent import file_memory as fm

    builtin_rows = await AgentSop.filter(is_builtin=True)
    bindings = await AgentSopBinding.filter(agent_id=agent_id, enabled=True).values("sop_id")
    bound_ids = [binding["sop_id"] for binding in bindings]
    bound_rows = (
        await AgentSop.filter(id__in=bound_ids, is_builtin=False, enabled=True)
        if bound_ids
        else []
    )
    catalog = list(builtin_rows) + list(bound_rows)
    effective: list[dict[str, Any]] = [
        _present(row) for row in catalog if _source_path(row.file_ref).is_file()
    ]

    # Discover private distilled SOPs: files in memory/sop/ whose name is not a
    # catalog SOP filename. Catalog filenames are like "builtin/x.md" /
    # "custom/x.md"; the on-disk name is the basename only.
    catalog_names = {Path(row["filename"]).name for row in effective}
    try:
        sop_root = fm.agent_sop_root(agent_id)
    except Exception:  # noqa: BLE001 — Agent dir may not exist yet
        sop_root = None
    if sop_root and sop_root.is_dir():
        for path in sorted(sop_root.glob("*.md")):
            if path.name in catalog_names:
                continue
            effective.append({
                "id": f"private:{path.name}",
                "name": path.stem.replace("_", " "),
                "description": "Agent 私有蒸馏 SOP",
                "filename": f"private/{path.name}",
                "enabled": True,
                "is_builtin": False,
                "revision": 1,
                "updated_at": None,
                "exists": True,
            })
    return {"available": effective}


async def list_bound_sop_ids(agent_id: int) -> list[str]:
    """SOP ids explicitly bound to one Agent (backend capability for future install UI)."""
    bindings = await AgentSopBinding.filter(agent_id=agent_id, enabled=True).values("sop_id")
    return [str(binding["sop_id"]) for binding in bindings]


async def bind_sop(agent_id: int, sop_id: str) -> None:
    """Bind one custom SOP to an Agent (backend capability for future install UI).

    Built-in SOPs are auto-effective and reject explicit binding. This helper is
    not yet wired to a resource-page action — the install interaction design is
    pending unified planning.
    """
    row = await AgentSop.get_or_none(id=uuid.UUID(str(sop_id)))
    if row is None:
        raise FileNotFoundError("SOP not found")
    if row.is_builtin:
        raise ValueError("Built-in SOPs are auto-effective; binding is not needed")
    await AgentSopBinding.update_or_create(
        agent_id=agent_id, sop_id=row.id, defaults={"enabled": True}
    )


async def unbind_sop(agent_id: int, sop_id: str) -> bool:
    """Remove an Agent ↔ custom SOP binding. Returns False if no binding existed."""
    binding = await AgentSopBinding.filter(
        agent_id=agent_id, sop_id=uuid.UUID(str(sop_id))
    ).first()
    if binding is None:
        return False
    await binding.delete()
    return True


async def sync_agent_sops(agent_id: int) -> None:
    """Copy the effective SOP set into the Agent-private L3 directory and refresh L1.

    GA-style incremental upsert (not a full rebuild). Effective SOPs are:

    - every built-in catalog SOP — unconditionally synced to every Agent (runtime
      default capability; no user install needed);
    - custom catalog SOPs explicitly bound to this Agent via ``AgentSopBinding``
      (``enabled`` catalog flag must also be true).

    Agent-private distilled SOPs (written by ``start_long_term_update`` or seeded
    on first MCP use, e.g. ``mcp.<service>`` keys) are preserved. Only catalog
    SOP files that are no longer effective (built-in removed from catalog, or
    custom unbound / disabled) are removed.
    """
    from agent import file_memory as fm

    fm.ensure_agent_memory(agent_id)
    builtin_rows = await AgentSop.filter(is_builtin=True)
    bindings = await AgentSopBinding.filter(agent_id=agent_id, enabled=True).values("sop_id")
    bound_ids = [binding["sop_id"] for binding in bindings]
    bound_rows = (
        await AgentSop.filter(id__in=bound_ids, is_builtin=False, enabled=True)
        if bound_ids
        else []
    )
    desired: set[str] = set()
    for row in [*builtin_rows, *bound_rows]:
        source = _source_path(row.file_ref)
        if not source.is_file():
            continue
        name = Path(row.file_ref).name
        desired.add(name)
        fm.write_sop(agent_id, name, source.read_text(encoding="utf-8"))
        # Managed SOP pointers use the SOP name as key; description stays in the
        # file body (L1 only stores existence + path).
        fm.upsert_l1_pointer(agent_id, row.name, f"memory/sop/{name}")
    # Remove catalog SOP files that are no longer effective. Leave private
    # distilled SOPs (under mcp/ or written by start_long_term_update) and any
    # file whose name is not a catalog basename untouched.
    try:
        sop_root = fm.agent_sop_root(agent_id)
    except Exception:  # noqa: BLE001
        sop_root = None
    if sop_root and sop_root.is_dir():
        catalog_rows = await AgentSop.all()
        catalog_basenames = {Path(row.file_ref).name for row in catalog_rows}
        for path in sop_root.glob("*.md"):
            if path.name in catalog_basenames and path.name not in desired:
                path.unlink()


__all__ = [
    "bind_sop", "create_sop", "delete_sop", "ensure_builtin_sops", "get_content",
    "list_bound_sop_ids", "list_catalog", "list_for_agent", "sync_agent_sops",
    "unbind_sop", "update_sop",
]
