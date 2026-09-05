"""Autonomous worker: idle-triggered TODO queue (GA chapter6.1).

Distinct from goal mode (one open objective + budget). Autonomous mode waits
until the Agent has been idle for ``idle_interval`` seconds, then claims one
TODO line from ``temp/TODO.txt`` under the Agent workspace and runs it through
the normal GenericAgent loop.

Idle is measured from the Agent's last conversation activity
(``agent_conversations.updated_at``); if ClickHouse is unavailable the worker
falls back to the workspace TODO file's mtime so it never blocks on the DB.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from agent.file_memory import agent_root

logger = logging.getLogger(__name__)

# How long an Agent must be idle before the worker claims a TODO item.
DEFAULT_IDLE_INTERVAL = 1800  # 30 minutes, per GA SOP


@dataclass
class TodoItem:
    raw: str
    priority: int  # lower = higher priority; 99 when no prefix
    text: str


def parse_todo_line(line: str) -> TodoItem | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    priority = 99
    text = stripped
    if stripped[:3] in ("[P1", "[P2", "[P3") and len(stripped) > 3 and stripped[3] in "]":
        # [P1] ... / [P2] ...
        try:
            priority = int(stripped[2])
        except ValueError:
            pass
        text = stripped[4:].strip()
    elif stripped.startswith("[") and "]" in stripped:
        head, _, rest = stripped.partition("]")
        inner = head[1:].strip()
        if inner and inner[0] in "Pp" and inner[1:].isdigit():
            try:
                priority = int(inner[1:])
            except ValueError:
                pass
            text = rest.strip()
    if not text:
        return None
    return TodoItem(raw=stripped, priority=priority, text=text)


class AutonomousWorker:
    """Poll one Agent's TODO queue and dispatch idle-triggered work."""

    def __init__(
        self,
        agent_id: int,
        dispatch: Callable[[str], Awaitable[object]],
        *,
        idle_interval: int = DEFAULT_IDLE_INTERVAL,
        poll_interval: float = 60.0,
        last_activity_fn: Callable[[int], Awaitable[float]] | None = None,
    ) -> None:
        self.agent_id = int(agent_id)
        self.dispatch = dispatch
        self.idle_interval = max(60, int(idle_interval))
        self.poll_interval = max(5.0, float(poll_interval))
        self._last_activity_fn = last_activity_fn
        self._stopped = asyncio.Event()
        self._claimed: TodoItem | None = None

    @property
    def root(self) -> Path:
        return agent_root(self.agent_id) / "temp"

    @property
    def todo_path(self) -> Path:
        return self.root / "TODO.txt"

    def stop(self) -> None:
        self._stopped.set()

    def _read_queue(self) -> list[TodoItem]:
        if not self.todo_path.exists():
            return []
        items: list[TodoItem] = []
        for line in self.todo_path.read_text(encoding="utf-8").splitlines():
            item = parse_todo_line(line)
            if item is not None:
                items.append(item)
        return items

    def _rewrite_queue(self, items: list[TodoItem], claimed: TodoItem) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        remaining = [it for it in items if it.raw != claimed.raw]
        self.todo_path.write_text(
            "\n".join(it.raw for it in remaining) + ("\n" if remaining else ""),
            encoding="utf-8",
        )

    def _requeue(self, item: TodoItem, note: str) -> None:
        """Put a failed item back with a short failure note (per SOP).

        The SOP says a blocked item stays in the queue with a note rather than
        being retried indefinitely; the note is what stops the next tick from
        silently re-running it as if it were fresh.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        existing = ""
        if self.todo_path.exists():
            existing = self.todo_path.read_text(encoding="utf-8").rstrip("\n")
        line = f"{item.raw}  # failed: {note}".strip()
        body = f"{existing}\n{line}\n" if existing else f"{line}\n"
        self.todo_path.write_text(body, encoding="utf-8")

    async def _last_activity_ts(self) -> float:
        if self._last_activity_fn is not None:
            try:
                return float(await self._last_activity_fn(self.agent_id))
            except Exception as exc:  # noqa: BLE001 — best-effort
                logger.debug("[autonomous] last_activity lookup failed agent=%s: %s", self.agent_id, exc)
        # Fallback: TODO file mtime; 0 forces "not idle" first tick then advances.
        try:
            return self.todo_path.stat().st_mtime
        except FileNotFoundError:
            return 0.0

    async def claim_if_idle(self) -> str | None:
        """Claim one TODO item if the Agent has been idle long enough.

        Returns the claimed prompt, or None when not idle / queue empty.
        Both sides of the comparison are wall-clock epoch seconds: the activity
        timestamp comes from the conversation store / file mtime, so a monotonic
        clock (loop.time) would not be comparable.
        """
        items = self._read_queue()
        if not items:
            return None
        now = time.time()
        last = await self._last_activity_ts()
        if last <= 0 or (now - last) < self.idle_interval:
            return None
        ordered = sorted(
            enumerate(items), key=lambda pair: (pair[1].priority, pair[0])
        )
        claimed = ordered[0][1]
        self._claimed = claimed
        self._rewrite_queue(items, claimed)
        return claimed.text

    def requeue_claimed(self, note: str = "dispatch failed") -> None:
        """Put the most recently claimed item back (with a note) after a failed dispatch."""
        if self._claimed is not None:
            self._requeue(self._claimed, note)
            self._claimed = None
    async def tick(self) -> str | None:
        """Run one polling tick; return the dispatched prompt (testable)."""
        try:
            prompt = await self.claim_if_idle()
        except Exception as exc:  # noqa: BLE001 — one bad tick must not stop worker
            logger.warning("[autonomous] agent=%s tick failed: %s", self.agent_id, exc)
            return None
        if prompt:
            try:
                await self.dispatch(prompt)
            except Exception as exc:  # noqa: BLE001 — dispatch failures requeue, not fatal
                logger.warning("[autonomous] agent=%s dispatch failed: %s", self.agent_id, exc)
                self.requeue_claimed(f"{type(exc).__name__}: {exc}")
                return prompt
            self._claimed = None
            return prompt
        return None

    async def run(self) -> None:
        while not self._stopped.is_set():
            await self.tick()
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass


__all__ = ["AutonomousWorker", "TodoItem", "parse_todo_line", "DEFAULT_IDLE_INTERVAL"]
