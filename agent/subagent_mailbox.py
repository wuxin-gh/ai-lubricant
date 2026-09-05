"""Parent/subagent mailbox backed by ``agent_subagent_messages``.

The DB mailbox replaces GA's file protocol while preserving context isolation:
parents and children exchange explicit messages, and SSE remains the live
progress channel. IDs are task UUIDs matching the existing DB schema.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from db import PostgresClient

_VALID_DIRECTIONS = {"parent", "subagent"}


async def send_message(
    *,
    parent_task_id: str | uuid.UUID,
    subagent_task_id: str | uuid.UUID,
    direction: str,
    message_type: str,
    content: str | dict[str, Any],
) -> int:
    """Write one parent→child or child→parent mailbox message."""
    if direction not in _VALID_DIRECTIONS:
        raise ValueError("direction must be parent or subagent")
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO agent_subagent_messages(
                parent_task_id, subagent_task_id, direction, type, content
            ) VALUES($1, $2, $3, $4, $5)
            RETURNING id
            """,
            uuid.UUID(str(parent_task_id)), uuid.UUID(str(subagent_task_id)),
            direction, str(message_type), text,
        )
    return int(row["id"])


async def read_messages(
    *,
    parent_task_id: str | uuid.UUID,
    subagent_task_id: str | uuid.UUID,
    direction: str | None = None,
    consume: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Read unconsumed mailbox messages and optionally mark them consumed."""
    if not PostgresClient.pool:
        return []
    clauses = ["parent_task_id=$1", "subagent_task_id=$2", "consumed=FALSE"]
    args: list[Any] = [uuid.UUID(str(parent_task_id)), uuid.UUID(str(subagent_task_id))]
    if direction:
        if direction not in _VALID_DIRECTIONS:
            raise ValueError("invalid direction")
        args.append(direction)
        clauses.append(f"direction=${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    sql = (
        "SELECT id, parent_task_id, subagent_task_id, direction, type, content, "
        "consumed, created_at FROM agent_subagent_messages WHERE "
        + " AND ".join(clauses)
        + f" ORDER BY id ASC LIMIT ${len(args)}"
    )
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        result = [dict(row) for row in rows]
        if consume and result:
            await conn.execute(
                "UPDATE agent_subagent_messages SET consumed=TRUE WHERE id=ANY($1::bigint[])",
                [row["id"] for row in result],
            )
    return result


__all__ = ["send_message", "read_messages"]
