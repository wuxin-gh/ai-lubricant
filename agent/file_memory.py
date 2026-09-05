"""Agent-private GA-style file memory.

The runtime deliberately exposes files rather than database-shaped memory
objects.  L1/L2/L3 are per-agent directories so the same implementation can be
moved to an Agent-bound node later without changing the model-facing layout.
"""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any

# GA-style real directory tree: every Agent owns ``data/agents/<agent_id>/``
# with ``memory/`` (L1/L2/L3) and ``workspace/`` (ordinary working files) as real
# subdirectories. ``file_read`` resolves relative paths against the Agent's
# workspace_root (the agent dir) so ``memory/sop/x.md`` and ``workspace/x.txt``
# both hit real files with no virtual mapping.
_MEMORY_ROOT = Path(os.getenv("GENERIC_AGENT_MEMORY_ROOT", "data/agents")).resolve()
_LEGACY_MEMORY_ROOT = Path(os.getenv("GENERIC_AGENT_LEGACY_MEMORY_ROOT", "data/generic-agent-memory")).resolve()
_BUILTIN_SOP_ROOT = Path(__file__).resolve().parent / "sop"
_MAX_INDEX_CHARS = 32_000
_MAX_FACT_CHARS = 256_000
_MAX_SOP_BYTES = 512 * 1024


def agent_root(agent_id: int) -> Path:
    """The Agent's private working directory (parent of memory/ and workspace/)."""
    return _MEMORY_ROOT / str(int(agent_id))


def agent_memory_root(agent_id: int) -> Path:
    return agent_root(agent_id) / "memory"


def agent_workspace_root(agent_id: int) -> Path:
    return agent_root(agent_id) / "workspace"


def agent_l1_path(agent_id: int) -> Path:
    return agent_memory_root(agent_id) / "global_mem_insight.txt"


def agent_l2_path(agent_id: int) -> Path:
    return agent_memory_root(agent_id) / "global_mem.txt"


def agent_sop_root(agent_id: int) -> Path:
    return agent_memory_root(agent_id) / "sop"


def _safe_ref(ref: str) -> str:
    value = str(ref or "").replace("\\", "/").strip()
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise PermissionError("Invalid Agent SOP reference")
    if path.suffix.lower() not in {".md", ".py", ".ps1", ".sh", ".txt"}:
        raise PermissionError("Agent memory files must be Markdown/text/SOP scripts")
    return "/".join(path.parts)


def _safe_sop_path(agent_id: int, ref: str) -> Path:
    safe = _safe_ref(ref)
    root = agent_sop_root(agent_id).resolve()
    target = (root / Path(*safe.split("/"))).resolve()
    if not target.is_relative_to(root):
        raise PermissionError("SOP reference escapes Agent memory")
    return target


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        handle.write(content)
        temp_name = handle.name
    os.replace(temp_name, path)


# Scenario trigger words for the L1 index, GA-style.
#
# GA's L1 is not a path list — it is a compact "scenario keyword -> SOP name" routing
# table (see assets/global_mem_insight_template in the GA repo). An unindexed capability
# is an unknown capability, so the trigger word is what lets the model decide whether to
# spend a file_read. Per the L0 SOP sync red line: a parenthesised trigger carries only a
# counter-intuitive scenario hint (2-4 words) — never a mechanism, method or step, and
# nothing at all when the SOP name already explains itself.
_SOP_TRIGGERS: dict[str, str] = {
    "browser_sop": "page read/click/JS; new task/site → open_tab, in-site nav → click",
    "web_setup_sop": "first-time browser env prep",
    "mcp_usage_sop": "capability_call, external services",
    "scheduled_task_sop": "cron/recurring",
    "autonomous_operation_sop": "idle TODO-driven work",
    "goal_mode_sop": "open objective + time budget",
    "reflect_mode_sop": "scheduled check then dispatch",
    "subagent_sop": "delegate independent investigation",
    "plan_sop": "multi-step with dependencies",
    "memory_management_sop": "L0 META-SOP, read before any memory write",
    "memory_cleanup_sop": "index bloat/stale entries",
    "keychain_sop": "credentials must not enter memory",
    "skill_search_sop": "no verified procedure yet",
    "ocr_sop": "text inside images",
    "failure_escalation_sop": "same failure repeated",
    "deliverable_audit_sop": "verify a deliverable, must run not read",
    "code_review_principles": "judging code quality",
    "review_sop": "adversarial code review, false-positive gate",
    "supervisor_sop": "watch a subagent without doing its work",
}

# Always-on behavioural rules. GA ships these in the L1 template's [RULES] block and
# every turn pays for them, because they are the pitfalls that silently produce wrong
# results rather than errors. Keep each entry to one compressed line.
_L1_RULES: list[str] = [
    "Search first: check cwd before guessing paths; no recursive full-disk scans.",
    "Cross-verify: never trust a summary; confirm numbers on the detail source.",
    "Encoding safety: read with file_read, not shell cat/type; read before modify.",
    "Close the loop: after any state change, verify it; escalate after 3 failures.",
    "SOP: read the SOP file, never rely on remembered SOP content.",
    "Memory: memory/ is patch-only via start_long_term_update; never file_write it.",
    "Web input: use the native setter + event chain; check disabled before click.",
    "Web scan: an empty or partial scan means wait and rescan, not a conclusion.",
]


def _sop_trigger(path: Path) -> str:
    """The parenthesised scenario hint for one SOP, or '' when the name self-explains."""
    return _SOP_TRIGGERS.get(path.stem, "")


def _builtin_sop_index_lines(builtin_sop_dir: str | os.PathLike[str] | None) -> list[str]:
    """Build the GA-shaped L1 body: an L3 routing table plus the always-on [RULES].

    Format mirrors GA's ``global_mem_insight_template``: SOP names are aggregated into
    wrapped ``L3:`` lines with counter-intuitive scenarios in parentheses, rather than
    one ``- **Name**: path`` line per file. That keeps L1 inside its <= 30-line budget
    while still carrying the trigger words the model routes on.
    """
    source = Path(builtin_sop_dir) if builtin_sop_dir else _BUILTIN_SOP_ROOT
    entries: list[str] = []
    if source.is_dir():
        for path in sorted(source.glob("*.md")):
            if not path.is_file() or path.stem.lower() == "readme":
                continue
            trigger = _sop_trigger(path)
            entries.append(f"{path.stem}({trigger})" if trigger else path.stem)
    lines: list[str] = []
    if entries:
        # Wrap the aggregated list so no single line runs away; continuation lines
        # start with "| " exactly like GA's template.
        current = "L3: "
        for entry in entries:
            piece = entry if current.endswith(": ") else f" | {entry}"
            if len(current) + len(piece) > 150 and not current.endswith(": "):
                lines.append(current)
                current = f"| {entry}"
            else:
                current += piece
        lines.append(current)
    lines.append("L4: archived sessions in ClickHouse (tracing/audit only)")
    lines.append("")
    lines.append("[RULES]")
    lines.extend(f"{i}. {rule}" for i, rule in enumerate(_L1_RULES, start=1))
    return lines


_L1_LEARNED_MARKER = "[LEARNED]"
_L1_RULES_MARKER = "[RULES]"


def _l1_header_lines() -> list[str]:
    """The fixed navigation preamble, mirroring GA's L1 template head."""
    return [
        "# [Global Memory Insight]",
        "Read L2 or list memory/sop/ for L3 when needed",
        "L0(META-SOP): memory_management_sop",
        "L2: memory/global_mem.txt",
    ]


def _build_l1(source_dir: Path | None, learned: list[str]) -> str:
    """Compose the whole L1 body: header + L3 routing + [LEARNED] + [RULES].

    Layout follows GA (routing table -> learned pointers -> always-on rules) so the
    model reads capability existence first and pitfall rules last.
    """
    lines = _l1_header_lines()
    lines.extend(_builtin_sop_index_lines(source_dir))
    body = "\n".join(lines)
    learned_block = "\n".join([_L1_LEARNED_MARKER, *learned]) if learned else _L1_LEARNED_MARKER
    # _builtin_sop_index_lines appends [RULES] last; split it back out so the learned
    # block (distilled SOPs, per-service MCP SOPs) sits above the rules.
    if f"\n{_L1_RULES_MARKER}" in body:
        head, _, rules = body.partition(f"\n{_L1_RULES_MARKER}")
        return f"{head.rstrip()}\n\n{learned_block}\n\n{_L1_RULES_MARKER}{rules}".rstrip() + "\n"
    return f"{body.rstrip()}\n\n{learned_block}".rstrip() + "\n"


def _extract_learned(text: str, agent_id: int | None = None) -> list[str]:
    """Pull learned pointer lines out of an existing L1, old or new format.

    The old format wrote one ``- **Name**: memory/sop/x.md`` line per built-in SOP.
    Only non-built-in pointers (distilled experience, ``mcp.<service>``) are worth
    carrying forward — the built-in routing table is regenerated from disk each time.
    Pointers whose target file no longer exists (a stale catalog SOP that was later
    removed) are dropped, so the index never advertises a dead path.
    """
    builtin = set(_SOP_TRIGGERS) | {"readme"}
    root = agent_root(agent_id) if agent_id is not None else None
    learned: list[str] = []
    seen: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("- **"):
            continue
        key = line[4:].split("**", 1)[0].strip()
        slug = key.lower().replace(" ", "_")
        if slug in builtin or key.lower() in builtin:
            continue
        if slug in seen:
            continue
        # Drop dead pointers. Pointer values come in two conventions: the current
        # ``memory/sop/x.md`` (relative to the agent root) and the legacy
        # ``sop/x.md`` (relative to the memory root), so resolve against both and
        # keep the line if either target exists.
        if root is not None:
            value = line.split("**:", 1)[1].strip() if "**:" in line else ""
            if not value or not any(
                (base / value).exists() for base in (root, root / "memory")
            ):
                continue
        seen.add(slug)
        learned.append(line)
    return learned


def _migrate_legacy_memory(agent_id: int, target_root: Path) -> None:
    """One-shot migration from the old flat layout to the GA-style tree.

    Old: ``data/generic-agent-memory/<id>/{global_mem_insight.txt,global_mem.txt,sop/}``
    New: ``data/agents/<id>/memory/{global_mem_insight.txt,global_mem.txt,sop/}``

    Runs only when the legacy directory exists and the new L1 file is absent, so
    re-running is a no-op once an Agent has been initialised under the new tree.
    """
    legacy = _LEGACY_MEMORY_ROOT / str(int(agent_id))
    if not legacy.is_dir():
        return
    if target_root.joinpath("global_mem_insight.txt").exists():
        return
    target_root.mkdir(parents=True, exist_ok=True)
    import shutil

    for name in ("global_mem_insight.txt", "global_mem.txt"):
        src = legacy / name
        if src.is_file():
            shutil.copy2(src, target_root / name)
    legacy_sop = legacy / "sop"
    if legacy_sop.is_dir():
        target_sop = target_root / "sop"
        target_sop.mkdir(parents=True, exist_ok=True)
        for item in legacy_sop.iterdir():
            if item.is_file():
                shutil.copy2(item, target_sop / item.name)


def ensure_agent_memory(agent_id: int, builtin_sop_dir: str | os.PathLike[str] | None = None) -> Path:
    """Create (and keep current) an Agent's private GA memory layout.

    The built-in routing table and [RULES] are regenerated from disk on every call,
    carrying forward any [LEARNED] pointers. Previously L1 was written once at
    creation, so a built-in SOP added later never appeared in an existing Agent's
    index — the file was on disk but the model was never told it existed.
    """
    source_dir = Path(builtin_sop_dir) if builtin_sop_dir else _BUILTIN_SOP_ROOT
    root = agent_memory_root(agent_id)
    # Migrate the old flat layout before seeding so existing L1/L2/SOP survive.
    _migrate_legacy_memory(agent_id, root)
    root.mkdir(parents=True, exist_ok=True)
    # The workspace sibling is created lazily by file_write; ensure it exists too
    # so the Agent's real working directory is always present alongside memory/.
    agent_workspace_root(agent_id).mkdir(parents=True, exist_ok=True)
    l1 = agent_l1_path(agent_id)
    l2 = agent_l2_path(agent_id)
    existing = l1.read_text(encoding="utf-8", errors="replace") if l1.exists() else ""
    desired = _build_l1(source_dir, _extract_learned(existing, agent_id))
    if existing != desired:
        _atomic_write(l1, desired)
    if not l2.exists():
        _atomic_write(l2, "# Verified Environment Facts\n")
    sop_root = agent_sop_root(agent_id)
    sop_root.mkdir(parents=True, exist_ok=True)
    if source_dir.is_dir():
        for item in source_dir.iterdir():
            if item.is_file() and item.suffix.lower() in {".md", ".py", ".ps1", ".sh", ".txt"}:
                destination = sop_root / item.name
                # Seed only. ``sync_agent_sops`` is the single writer for built-in SOP
                # bodies (it refreshes them from the catalog on every run); refreshing
                # here too would let the two writers fight over the same file.
                if not destination.exists():
                    destination.write_bytes(item.read_bytes())
    return root


def read_l1(agent_id: int) -> str:
    ensure_agent_memory(agent_id)
    return agent_l1_path(agent_id).read_text(encoding="utf-8")[:_MAX_INDEX_CHARS]


def write_l1(agent_id: int, content: str) -> None:
    if len(content) > _MAX_INDEX_CHARS:
        raise ValueError("L1 index is too large")
    ensure_agent_memory(agent_id)
    _atomic_write(agent_l1_path(agent_id), content)


def read_l2(agent_id: int) -> str:
    ensure_agent_memory(agent_id)
    return agent_l2_path(agent_id).read_text(encoding="utf-8")[:_MAX_FACT_CHARS]


def append_l2_fact(agent_id: int, key: str, value: Any, *, source: str = "agent", verified: bool = False) -> None:
    if not verified:
        raise ValueError("Only verified facts may be written to L2")
    ensure_agent_memory(agent_id)
    text = read_l2(agent_id).rstrip() + "\n"
    marker = f"- **{str(key).strip()}**: {value} (source: {source})\n"
    # Upsert the human-readable fact line by key instead of accumulating duplicates.
    pattern = re.compile(rf"^- \*\*{re.escape(str(key).strip())}\*\*:.*(?:\n|$)", re.MULTILINE)
    text = pattern.sub("", text)
    text = text.rstrip() + "\n" + marker
    if len(text) > _MAX_FACT_CHARS:
        raise ValueError("L2 facts file is too large")
    _atomic_write(agent_l2_path(agent_id), text)


def list_sops(agent_id: int) -> list[dict[str, str]]:
    ensure_agent_memory(agent_id)
    root = agent_sop_root(agent_id)
    rows: list[dict[str, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".md", ".py", ".ps1", ".sh", ".txt"}:
            continue
        ref = path.relative_to(root).as_posix()
        first = path.read_text(encoding="utf-8", errors="replace").splitlines()
        title = next((line.lstrip("#").strip() for line in first if line.strip()), path.stem)
        rows.append({"ref": ref, "title": title[:240]})
    return rows


def read_sop(agent_id: int, ref: str) -> str:
    path = _safe_sop_path(agent_id, ref)
    if not path.is_file():
        raise FileNotFoundError("SOP not found")
    if path.stat().st_size > _MAX_SOP_BYTES:
        raise ValueError("SOP is too large")
    return path.read_text(encoding="utf-8")


def write_sop(agent_id: int, ref: str, content: str) -> None:
    path = _safe_sop_path(agent_id, ref)
    if path.suffix.lower() != ".md":
        raise ValueError("Only Markdown files may be used as SOPs")
    if len(content.encode("utf-8")) > _MAX_SOP_BYTES:
        raise ValueError("SOP is too large")
    ensure_agent_memory(agent_id)
    _atomic_write(path, content)


def upsert_l1_pointer(agent_id: int, key: str, value: str) -> None:
    """Add or replace a learned pointer in the L1 [LEARNED] block.

    Built-in SOP routing is regenerated from disk by ``ensure_agent_memory`` and must
    never be duplicated here, so a built-in key (or a pointer whose target is a
    built-in SOP file) is a no-op. Learned pointers are distilled experience and
    per-service MCP SOPs — the only L1 entries the model (via
    ``start_long_term_update``) or the MCP seeder is allowed to add.
    """
    key_clean = str(key).strip()
    if not key_clean:
        return
    value_clean = str(value).strip()
    slug = key_clean.lower().replace(" ", "_")
    if slug in _SOP_TRIGGERS or key_clean.lower() in _SOP_TRIGGERS:
        return  # built-in routing is managed by _build_l1; never duplicate
    # Also skip when the pointer target IS a built-in SOP file (the catalog uses the
    # SOP's display name as the key, e.g. "Memory Management" -> memory/sop/...).
    target_stem = Path(value_clean).stem.lower()
    if target_stem in _SOP_TRIGGERS:
        return
    ensure_agent_memory(agent_id)
    text = read_l1(agent_id)
    line = f"- **{key_clean}**: {value_clean}"
    # Drop any existing entry for this key (old or new format) before reinserting.
    pattern = re.compile(rf"^- \*\*{re.escape(key_clean)}\*\*:.*(?:\n|$)", re.MULTILINE)
    cleaned = pattern.sub("", text)
    learned_block = _extract_learned(cleaned, agent_id)
    learned_block = [ln for ln in learned_block if not ln.startswith(f"- **{key_clean}**")]
    learned_block.append(line)
    _atomic_write(agent_l1_path(agent_id), _build_l1(_BUILTIN_SOP_ROOT, learned_block))


__all__ = [
    "agent_l1_path", "agent_l2_path", "agent_memory_root", "agent_root",
    "agent_sop_root", "agent_workspace_root",
    "append_l2_fact", "ensure_agent_memory", "list_sops", "read_l1", "read_l2",
    "read_sop", "write_l1", "write_sop", "upsert_l1_pointer",
]
