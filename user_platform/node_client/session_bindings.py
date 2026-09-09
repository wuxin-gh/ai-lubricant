"""Direct PG read of node session bindings (data service side).

Per the split design, the data service may read static node placement
information directly from PostgreSQL (the shared truth source), but must not
operate on live nodes. This small reader supports the editor workspace
idempotency check: ``dispatch_session`` on the control side is NOT idempotent
(it re-sends CreateSession), so the data side first checks whether a binding
already exists before asking the control side to provision it.

We use the shared asyncpg pool (``PostgresClient.pool``) directly rather than a
Tortoise model: ``node_server`` owns the node-ledger Tortoise registration, so
the data process must not register those models.
"""
from __future__ import annotations


async def get_editor_workspace_session_node_id(session_id: str) -> str | None:
    """Return the bound node_id for ``session_id`` (or None if not bound)."""
    from db import PostgresClient

    if not PostgresClient.pool or not session_id:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT node_id FROM mc_ac_node_sessions WHERE session_id=$1",
            session_id,
        )
    return str(row["node_id"]) if row else None
