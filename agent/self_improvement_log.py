"""Agent self-improvement log (GA ch12.3.3).

Records three classes of always-on learning (distinct from L3 SOPs, which are
task-specific reusable procedures):
- observed errors and their corrections,
- explicit user preferences,
- verified successful patterns.

The log lives at ``memory/self_improvement.txt`` (Agent-private) and is injected
into the system prompt every turn via ``META_SELF_IMPROVEMENT`` so the Agent
respects these cross-task lessons without re-reading a SOP.
"""
from __future__ import annotations

from pathlib import Path

from agent import file_memory as fm

_LOG_FILENAME = "self_improvement.txt"
_MAX_LOG_CHARS = 32_000


def _log_path(agent_id: int) -> Path:
    return fm.agent_memory_root(agent_id) / _LOG_FILENAME


def read_log(agent_id: int) -> str:
    """Return the Agent's self-improvement log, seeding an empty header on first use."""
    fm.ensure_agent_memory(agent_id)
    path = _log_path(agent_id)
    if not path.exists():
        path.write_text("# Self-Improvement Log\n", encoding="utf-8")
    return path.read_text(encoding="utf-8")[:_MAX_LOG_CHARS]


def append_entry(agent_id: int, category: str, text: str) -> None:
    """Append one verified lesson. Categories: error_correction, user_preference, success_pattern."""
    fm.ensure_agent_memory(agent_id)
    category = (category or "").strip().lower() or "success_pattern"
    if category not in {"error_correction", "user_preference", "success_pattern"}:
        raise ValueError(f"unknown self-improvement category: {category}")
    text = (text or "").strip()
    if not text:
        return
    path = _log_path(agent_id)
    existing = path.read_text(encoding="utf-8") if path.exists() else "# Self-Improvement Log\n"
    entry = f"- [{category}] {text}\n"
    updated = (existing.rstrip() + "\n" + entry).rstrip() + "\n"
    if len(updated) > _MAX_LOG_CHARS:
        # Keep the most recent entries by trimming the oldest lines after the header.
        lines = updated.splitlines()
        header = lines[0] if lines and lines[0].startswith("#") else "# Self-Improvement Log"
        body = [ln for ln in lines[1:] if ln.strip()]
        body = body[-( _MAX_LOG_CHARS // 120 ):]
        updated = header + "\n" + "\n".join(body) + "\n"
    path.write_text(updated, encoding="utf-8")


def system_prompt_segment(agent_id: int) -> str:
    """Compact segment injected into the system prompt each turn."""
    log = read_log(agent_id).strip()
    if not log or log == "# Self-Improvement Log":
        return ""
    return f"[Self-Improvement Log]\n{log}"


__all__ = ["read_log", "append_entry", "system_prompt_segment"]
