"""Reflection worker with hot-reloadable check scripts (GA chapter6.3).

Each Agent may place Python scripts under ``workspace/reflect/*.py``. A script
exports ``INTERVAL`` (seconds), ``ONCE`` (bool), and ``check() -> str | None``.
When check returns a prompt, the worker dispatches it through ``GenericAgent``.
Scripts are reloaded when their mtime changes; no Agent restart is required.
"""
from __future__ import annotations

import asyncio
import importlib.util
import logging
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Awaitable, Callable

from agent.file_memory import agent_workspace_root

logger = logging.getLogger(__name__)


@dataclass
class ReflectScript:
    path: Path
    mtime_ns: int = 0
    module: ModuleType | None = None
    interval: int = 300
    once: bool = False
    last_run: float = 0.0
    fired_once: bool = False


class ReflectWorker:
    """Poll and hot-reload one Agent's reflect scripts."""

    def __init__(
        self,
        agent_id: int,
        dispatch: Callable[[str], Awaitable[object]],
        *,
        poll_interval: float = 5.0,
    ) -> None:
        self.agent_id = int(agent_id)
        self.dispatch = dispatch
        self.poll_interval = max(1.0, float(poll_interval))
        self.scripts: dict[Path, ReflectScript] = {}
        self._stopped = asyncio.Event()

    @property
    def root(self) -> Path:
        return agent_workspace_root(self.agent_id) / "reflect"

    def stop(self) -> None:
        self._stopped.set()

    def _load(self, state: ReflectScript) -> bool:
        """Load/reload a script. Return True when the module changed."""
        stat = state.path.stat()
        if state.module is not None and stat.st_mtime_ns == state.mtime_ns:
            return False
        spec = importlib.util.spec_from_file_location(
            f"agent_reflect_{self.agent_id}_{state.path.stem}", state.path
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load reflect script: {state.path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        check = getattr(module, "check", None)
        if not callable(check):
            raise ValueError(f"reflect script {state.path.name} must define check()")
        interval = int(getattr(module, "INTERVAL", 300))
        state.module = module
        state.mtime_ns = stat.st_mtime_ns
        state.interval = max(1, interval)
        state.once = bool(getattr(module, "ONCE", False))
        # Hot reload resets ONCE and the interval gate so the edited check() can
        # take effect immediately on this tick.
        state.fired_once = False
        state.last_run = 0.0
        return True

    async def tick(self) -> list[str]:
        """Run one polling tick; return prompts that were dispatched (testable)."""
        self.root.mkdir(parents=True, exist_ok=True)
        present = set(self.root.glob("*.py"))
        for removed in set(self.scripts) - present:
            self.scripts.pop(removed, None)
        for path in present:
            self.scripts.setdefault(path, ReflectScript(path=path))

        loop = asyncio.get_running_loop()
        now = loop.time()
        dispatched: list[str] = []
        for path, state in list(self.scripts.items()):
            try:
                self._load(state)
                if state.once and state.fired_once:
                    continue
                if state.last_run and now - state.last_run < state.interval:
                    continue
                state.last_run = now
                prompt = state.module.check() if state.module else None
                if prompt:
                    text = str(prompt).strip()
                    if text:
                        await self.dispatch(text)
                        dispatched.append(text)
                        state.fired_once = True
            except Exception as exc:  # noqa: BLE001 — one bad script must not stop worker
                logger.warning("[reflect] script=%s failed: %s", path, exc)
        return dispatched

    async def run(self) -> None:
        """Run until stop() is called."""
        while not self._stopped.is_set():
            await self.tick()
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass


__all__ = ["ReflectScript", "ReflectWorker"]
