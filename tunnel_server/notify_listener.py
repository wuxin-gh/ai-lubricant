"""PostgreSQL LISTEN listener that wakes the runtime worker.

The main service writes desired state to ``mc_tunnel_*`` and then ``NOTIFY``s
this channel. A dedicated asyncpg connection listens and forwards each
notification to the runtime worker. A low-frequency fallback scan guarantees
recovery if a notification is dropped or this service was down when it fired.

The database remains the source of truth; NOTIFY is only a low-latency wake-up
signal.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import asyncpg
from loguru import logger

from .config import settings


def _dsn() -> str:
    """Normalize the Tortoise database URL for direct asyncpg.connect()."""
    url = settings.database_url.strip()
    if url.startswith("asyncpg://"):
        return "postgresql://" + url[len("asyncpg://"):]
    if url.startswith("postgres://") or url.startswith("postgresql://"):
        return url
    # A bare value is not a DSN. This usually means an unrelated environment
    # variable (for example ``AI_LUBRICANT_DATABASE_URL=docmost``) leaked into
    # the shell; fail with a useful URL rather than retrying an invalid one.
    raise ValueError(
        "AI_LUBRICANT_DATABASE_URL must be a PostgreSQL URL "
        "(postgresql://... or asyncpg://...), got a bare value"
    )


class NotifyListener:
    """Owns one asyncpg connection that stays in LISTEN."""

    def __init__(self, on_notify) -> None:
        self._on_notify = on_notify
        self._task: asyncio.Task | None = None
        self._conn: asyncpg.Connection | None = None
        self._stop = asyncio.Event()
        self.running = False

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="tunnel-runtime-notify")
        self.running = True

    async def stop(self) -> None:
        self.running = False
        self._stop.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._listen()
                backoff = 1.0
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    return
                logger.warning("[tunnel-server] LISTEN disconnected: {}; retrying in {}s", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _listen(self) -> None:
        self._conn = await asyncpg.connect(dsn=_dsn())
        channel = settings.notify_channel
        await self._conn.add_listener(channel, self._dispatch)
        logger.info("[tunnel-server] LISTEN {} ready", channel)
        await self._stop.wait()
        with contextlib.suppress(Exception):
            await self._conn.remove_listener(channel, self._dispatch)

    def _dispatch(
        self,
        _connection: asyncpg.Connection,
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        try:
            data = json.loads(payload) if payload else {}
        except Exception:  # noqa: BLE001
            logger.debug("[tunnel-server] unparseable NOTIFY payload: {}", payload)
            data = {"_raw": payload}
        # asyncpg invokes listeners on the event-loop thread, so schedule the
        # coroutine directly. The callback is not a worker thread callback.
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._on_notify(data), name="tunnel-runtime-notify-wake")
        except RuntimeError:
            logger.debug("[tunnel-server] LISTEN callback arrived without a running loop")


notify_listener = NotifyListener(on_notify=lambda _payload: None)
