"""Background worker orchestrator for GenericAgent (GA chapter6.1/6.3).

Owns one ReflectWorker and one AutonomousWorker per Agent that has either
reflect scripts or autonomous mode enabled. Workers dispatch prompts by
constructing a fresh GenericAgent and calling run_task, mirroring the
scheduled-task execution path (scheduler._execute_job).

Started from main.py lifespan; stopped on shutdown.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from agent.autonomous_worker import AutonomousWorker, DEFAULT_IDLE_INTERVAL
from agent.reflect_worker import ReflectWorker
from db import PostgresClient

logger = logging.getLogger(__name__)


class AgentBackgroundOrchestrator:
    """Class-level singleton managing per-agent background workers."""

    _started: bool = False
    _lock: asyncio.Lock = asyncio.Lock()
    _workers: dict[int, dict[str, Any]] = {}  # agent_id -> {"reflect": t, "autonomous": t, "stop": fn}
    _scan_task: asyncio.Task | None = None

    @classmethod
    async def start(cls, *, poll_interval: float = 60.0) -> None:
        if cls._started:
            return
        cls._started = True
        cls._scan_task = asyncio.create_task(cls._scan_loop(poll_interval), name="agent-bg-scan")
        logger.info("[agent-bg] orchestrator started")

    @classmethod
    async def stop(cls) -> None:
        if not cls._started:
            return
        if cls._scan_task:
            cls._scan_task.cancel()
            try:
                await cls._scan_task
            except (asyncio.CancelledError, Exception):
                pass
            cls._scan_task = None
        for workers in list(cls._workers.values()):
            for key in ("reflect", "autonomous"):
                w = workers.get(key)
                if w is not None:
                    try:
                        w.stop()
                    except Exception:  # noqa: BLE001
                        pass
            for key in ("reflect", "autonomous"):
                t = workers.get(f"{key}_task")
                if t is not None:
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass
        cls._workers.clear()
        cls._started = False
        logger.info("[agent-bg] orchestrator stopped")

    @classmethod
    async def _scan_loop(cls, poll_interval: float) -> None:
        while True:
            try:
                await cls._reconcile_workers()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[agent-bg] scan cycle failed: %s", exc)
            await asyncio.sleep(poll_interval)

    @classmethod
    async def _reconcile_workers(cls) -> None:
        """Start/stop per-agent workers based on DB flags and reflect scripts."""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, autonomous_enabled, guardian_interval FROM agents WHERE enabled=true"
            )
        live_ids: set[int] = set()
        for record in rows:
            row = dict(record)
            agent_id = int(row["id"])
            live_ids.add(agent_id)
            autonomous_on = bool(row.get("autonomous_enabled"))
            idle_interval = int(row.get("guardian_interval") or DEFAULT_IDLE_INTERVAL)
            # Reflect worker: always on for every enabled Agent; it's a no-op
            # until the Agent drops a script under workspace/reflect/*.py.
            await cls._ensure_reflect(agent_id)
            if autonomous_on:
                await cls._ensure_autonomous(agent_id, idle_interval)
            else:
                await cls._stop_autonomous(agent_id)
        # Stop workers for agents that disappeared / got disabled.
        for agent_id in list(cls._workers):
            if agent_id not in live_ids:
                await cls._stop_agent(agent_id)

    @classmethod
    async def _ensure_reflect(cls, agent_id: int) -> None:
        entry = cls._workers.setdefault(agent_id, {})
        if entry.get("reflect"):
            return
        worker = ReflectWorker(agent_id, lambda prompt: cls._dispatch(prompt, agent_id))
        entry["reflect"] = worker
        entry["reflect_task"] = asyncio.create_task(
            worker.run(), name=f"agent-reflect-{agent_id}"
        )

    @classmethod
    async def _ensure_autonomous(cls, agent_id: int, idle_interval: int) -> None:
        entry = cls._workers.setdefault(agent_id, {})
        existing: AutonomousWorker | None = entry.get("autonomous")
        if existing is not None and existing.idle_interval == idle_interval:
            return
        if existing is not None:
            existing.stop()
            t = entry.get("autonomous_task")
            if t is not None:
                t.cancel()
        worker = AutonomousWorker(
            agent_id, lambda prompt: cls._dispatch(prompt, agent_id),
            idle_interval=idle_interval,
            last_activity_fn=cls._last_activity_ts,
        )
        entry["autonomous"] = worker
        entry["autonomous_task"] = asyncio.create_task(
            worker.run(), name=f"agent-autonomous-{agent_id}"
        )

    @classmethod
    async def _stop_autonomous(cls, agent_id: int) -> None:
        entry = cls._workers.get(agent_id)
        if not entry:
            return
        w: AutonomousWorker | None = entry.pop("autonomous", None)
        if w is not None:
            w.stop()
        t = entry.pop("autonomous_task", None)
        if t is not None:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    @classmethod
    async def _stop_agent(cls, agent_id: int) -> None:
        entry = cls._workers.pop(agent_id, None)
        if not entry:
            return
        for key in ("reflect", "autonomous"):
            w = entry.get(key)
            if w is not None:
                try:
                    w.stop()
                except Exception:  # noqa: BLE001
                    pass
            t = entry.get(f"{key}_task")
            if t is not None:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

    @staticmethod
    async def _dispatch(prompt: str, agent_id: int) -> None:
        """Run a claimed TODO/reflect prompt through a fresh GenericAgent loop."""
        try:
            from agent.agent_main import GenericAgent

            agent = GenericAgent(agent_id=agent_id)
            await agent.run_task(prompt, max_turns=40)
        except Exception as exc:  # noqa: BLE001 — background dispatch must not crash the worker
            logger.warning("[agent-bg] dispatch failed agent=%s: %s", agent_id, exc)

    @staticmethod
    async def _last_activity_ts(agent_id: int) -> float:
        """Seconds-since-epoch of the Agent's most recent conversation activity.

        Falls back to 0.0 (not idle) when ClickHouse is unavailable; the worker
        then degrades to the TODO file mtime.
        """
        try:
            from agent import conversation_store

            if not conversation_store.is_ready():
                return 0.0
            ts = await conversation_store.last_activity_for_agent(agent_id)
            return float(ts) if ts else 0.0
        except Exception as exc:  # noqa: BLE001
            logger.debug("[agent-bg] last_activity lookup failed agent=%s: %s", agent_id, exc)
            return 0.0


__all__ = ["AgentBackgroundOrchestrator"]
