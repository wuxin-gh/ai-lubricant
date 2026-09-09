"""Post-commit wake-up signal to the standalone tunnel runtime service.

The main service writes desired state to the ``mc_tunnel_*`` tables and then
fires a PostgreSQL ``NOTIFY`` on a shared channel. The standalone
:mod:`tunnel_server` process holds a ``LISTEN`` connection and reconciles; the
database remains the source of truth, so a dropped notification is recovered by
the worker's low-frequency fallback scan.

This helper must run *after* the mutation is durably committed. Tortoise
autocommits each ``save()``/``create()``/``delete()``, so calling it right
after the write is naturally post-commit. Never call it inside an open
transaction: a later rollback would deliver a spurious wake-up.

The payload carries only locator fields (no secrets, no full config) — the
worker reloads everything from the DB.
"""
from __future__ import annotations

import json
from typing import Any

from loguru import logger

# Channel name shared with ``tunnel_server.config.notify_channel``. Kept as a
# literal here so the data service has no dependency on the runtime package;
# both sides read the same default string.
NOTIFY_CHANNEL = "tunnel_runtime_changed"


async def notify_tunnel_changed(
    *, runtime_id: str | None = None, target_id: str | None = None
) -> None:
    """Wake the tunnel runtime worker for one runtime / target.

    A missing payload (no ids) means "reconcile everything"; the worker's
    fallback scan re-scans the whole table anyway. Failures are logged at
    debug level only — the DB is the source of truth, and the next periodic
    scan recovers.
    """
    payload: dict[str, Any] = {}
    if runtime_id:
        payload["runtime_id"] = runtime_id
    if target_id:
        payload["target_id"] = target_id
    body = json.dumps(payload, separators=(",", ":"))
    try:
        from tortoise import Tortoise

        conn = Tortoise.get_connection("user_platform")
        # pg_notify(text, text) is a regular function, so $1/$2 parameter
        # binding works and keeps the payload out of the SQL string.
        await conn.execute_query_dict(
            "SELECT pg_notify($1, $2)", [NOTIFY_CHANNEL, body]
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "[tunnel] notify failed (worker fallback scan will recover): {}", exc
        )
