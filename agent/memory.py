"""Legacy PostgreSQL memory helpers for GenericAgent.

L1 ``agent_insight_index`` remains the durable compact index.
L2 ``agent_global_facts`` remains available while the Redis-backed working-fact
migration is staged.
L3 ``agent_skills`` is legacy learned-Skill text; the managed Skill catalog
lives in ``mc_agent_skills`` (resource center) and is never installed into an
Agent — GenericAgent resource delivery goes through AgentSop.
L4 ``agent_session_archives`` is legacy: ClickHouse ``agent_conversations`` and
``agent_messages`` already retain complete conversations, tool calls/results,
ownership, soft deletion and TTL-based retention.

Do not add new consumers to the legacy L3/L4 tables; migration must preserve
existing rows before their write paths are removed.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from db import PostgresClient

logger = logging.getLogger(__name__)


def _dumps(value: Any) -> str:
    """Serialize a JSON-compatible value exactly once for JSONB columns.

    Guard: if value is already a string, return it as-is to avoid
    double-serialization (the original bug).
    """
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


class MemorySystem:
    """Agent memory backed by PostgreSQL tables.

    All methods are async and use ``PostgresClient.pool.acquire()`` directly.
    When the pool is not initialised, writes are no-ops and reads return empty lists.

    Parameters:
        agent_id: Optional agent ID for multi-tenant isolation.
                  When set, all queries filter by agent_id.
    """

    def __init__(self, agent_id: int | None = None):
        self.agent_id = agent_id

    # ==================== L1 Insights ====================

    async def get_l1_insights(
        self,
        category: str | None = None,
        limit: int = 50,
        include_expired: bool = False,
    ) -> list[dict]:
        """Get L1 insights, filtering expired ones by default."""
        if not PostgresClient.pool:
            return []

        conditions = []
        params: list[Any] = []
        param_idx = 1

        if self.agent_id is not None:
            conditions.append(f"agent_id=${param_idx}")
            params.append(self.agent_id)
            param_idx += 1

        if category:
            conditions.append(f"category=${param_idx}")
            params.append(category)
            param_idx += 1

        if not include_expired:
            conditions.append("(expires_at IS NULL OR expires_at > now())")

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = (
            "SELECT id, key, value, category, priority, expires_at, created_at, updated_at "
            "FROM agent_insight_index "
            f"WHERE {where} "
            "ORDER BY priority DESC, category ASC, updated_at DESC "
            f"LIMIT ${param_idx}"
        )
        params.append(limit)

        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def upsert_l1_insight(
        self,
        key: str,
        value: str,
        category: str = "general",
        priority: int = 0,
        expires_at: Any = None,
    ) -> None:
        if not PostgresClient.pool:
            return
        if self.agent_id is not None:
            sql = """
                INSERT INTO agent_insight_index(agent_id, key, value, category, priority, expires_at, updated_at)
                VALUES($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT(agent_id, key) DO UPDATE SET
                    value=EXCLUDED.value, category=EXCLUDED.category,
                    priority=EXCLUDED.priority, expires_at=EXCLUDED.expires_at, updated_at=now()
            """
            params = (self.agent_id, key, value, category, priority, expires_at)
        else:
            sql = """
                INSERT INTO agent_insight_index(key, value, category, priority, expires_at, updated_at)
                VALUES($1, $2, $3, $4, $5, now())
                ON CONFLICT(key) DO UPDATE SET
                    value=EXCLUDED.value, category=EXCLUDED.category,
                    priority=EXCLUDED.priority, expires_at=EXCLUDED.expires_at, updated_at=now()
            """
            params = (key, value, category, priority, expires_at)
        async with PostgresClient.pool.acquire() as conn:
            await conn.execute(sql, *params)

    async def delete_l1_insight(self, key: str) -> bool:
        """Delete an L1 insight by key. Returns True if deleted."""
        if not PostgresClient.pool:
            return False
        if self.agent_id is not None:
            sql = "DELETE FROM agent_insight_index WHERE key=$1 AND agent_id=$2"
            params = (key, self.agent_id)
        else:
            sql = "DELETE FROM agent_insight_index WHERE key=$1"
            params = (key,)
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute(sql, *params)
        return result.endswith("1")

    async def search_l1_insights(self, query: str, limit: int = 20) -> list[dict]:
        """Search L1 insights by key/value pattern (ILIKE)."""
        if not PostgresClient.pool:
            return []
        pattern = f"%{query}%"
        if self.agent_id is not None:
            sql = (
                "SELECT id, key, value, category, priority, expires_at, created_at, updated_at "
                "FROM agent_insight_index "
                "WHERE agent_id=$1 AND (key ILIKE $2 OR value ILIKE $2) "
                "AND (expires_at IS NULL OR expires_at > now()) "
                "ORDER BY priority DESC, updated_at DESC LIMIT $3"
            )
            params = (self.agent_id, pattern, limit)
        else:
            sql = (
                "SELECT id, key, value, category, priority, expires_at, created_at, updated_at "
                "FROM agent_insight_index "
                "WHERE (key ILIKE $1 OR value ILIKE $1) "
                "AND (expires_at IS NULL OR expires_at > now()) "
                "ORDER BY priority DESC, updated_at DESC LIMIT $2"
            )
            params = (pattern, limit)
        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    # ==================== L2 Facts (Redis working set) ====================

    def _l2_redis(self):
        try:
            from rd import JdbcClient

            return JdbcClient.redis
        except Exception:
            return None

    def _l2_key(self) -> str:
        return f"agent:l2:{self.agent_id if self.agent_id is not None else 'global'}"

    @staticmethod
    def _decode_redis_value(value: Any) -> str:
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    async def get_l2_facts(
        self,
        source: str | None = None,
        verified_only: bool = False,
        limit: int = 100,
    ) -> list[dict]:
        """Read the Redis L2 working set, falling back to legacy PG rows."""
        redis = self._l2_redis()
        if redis is not None:
            try:
                raw = await redis.hgetall(self._l2_key())
                rows: list[dict] = []
                for key, value in (raw or {}).items():
                    fact_key = self._decode_redis_value(key)
                    try:
                        payload = json.loads(self._decode_redis_value(value))
                    except (TypeError, ValueError):
                        continue
                    if source and payload.get("source") != source:
                        continue
                    if verified_only and not payload.get("verified"):
                        continue
                    rows.append({"fact_key": fact_key, **payload})
                rows.sort(key=lambda item: float(item.get("updated_at") or 0), reverse=True)
                return rows[:limit]
            except Exception:
                logger.warning("L2 Redis read failed; falling back to PostgreSQL", exc_info=True)

        if not PostgresClient.pool:
            return []
        conditions = []
        params: list[Any] = []
        param_idx = 1
        if self.agent_id is not None:
            conditions.append(f"agent_id=${param_idx}")
            params.append(self.agent_id)
            param_idx += 1
        if source:
            conditions.append(f"source=${param_idx}")
            params.append(source)
            param_idx += 1
        if verified_only:
            conditions.append("verified=true")
        where = " AND ".join(conditions) if conditions else "1=1"
        sql = (
            "SELECT id, fact_key, fact_value, source, confidence, verified, created_at, updated_at "
            "FROM agent_global_facts "
            f"WHERE {where} ORDER BY updated_at DESC, created_at DESC LIMIT ${param_idx}"
        )
        params.append(limit)
        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def upsert_l2_fact(
        self,
        fact_key: str,
        fact_value: dict | list | str | int | float | bool | None,
        source: str = "agent",
        confidence: float = 1.0,
        verified: bool = False,
    ) -> None:
        redis = self._l2_redis()
        if redis is not None:
            payload = json.dumps({
                "fact_value": fact_value,
                "source": source,
                "confidence": confidence,
                "verified": verified,
                "updated_at": time.time(),
            }, ensure_ascii=False)
            await redis.hset(self._l2_key(), {fact_key: payload})
            # L2 is a working set, not the durable source. Refresh its TTL on use.
            await redis.expire(self._l2_key(), 30 * 24 * 3600)
            return
        if not PostgresClient.pool:
            return
        serialized = _dumps(fact_value)
        if self.agent_id is not None:
            sql = """
                INSERT INTO agent_global_facts(agent_id, fact_key, fact_value, source, confidence, verified, updated_at)
                VALUES($1, $2, $3::jsonb, $4, $5, $6, now())
                ON CONFLICT(agent_id, fact_key) DO UPDATE SET
                    fact_value=EXCLUDED.fact_value, source=EXCLUDED.source,
                    confidence=EXCLUDED.confidence, verified=EXCLUDED.verified, updated_at=now()
            """
            params = (self.agent_id, fact_key, serialized, source, confidence, verified)
        else:
            sql = """
                INSERT INTO agent_global_facts(fact_key, fact_value, source, confidence, verified, updated_at)
                VALUES($1, $2::jsonb, $3, $4, $5, now())
                ON CONFLICT(fact_key) DO UPDATE SET
                    fact_value=EXCLUDED.fact_value, source=EXCLUDED.source,
                    confidence=EXCLUDED.confidence, verified=EXCLUDED.verified, updated_at=now()
            """
            params = (fact_key, serialized, source, confidence, verified)
        async with PostgresClient.pool.acquire() as conn:
            await conn.execute(sql, *params)

    async def delete_l2_fact(self, fact_key: str) -> bool:
        redis = self._l2_redis()
        if redis is not None:
            # RedisJdbc does not wrap hdel; prefix the key the same way session.py does.
            removed = await redis.hdel(redis.get_key(self._l2_key()), [fact_key])
            return bool(removed)
        if not PostgresClient.pool:
            return False
        if self.agent_id is not None:
            sql = "DELETE FROM agent_global_facts WHERE fact_key=$1 AND agent_id=$2"
            params = (fact_key, self.agent_id)
        else:
            sql = "DELETE FROM agent_global_facts WHERE fact_key=$1"
            params = (fact_key,)
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute(sql, *params)
        return result.endswith("1")

    async def search_l2_facts(self, query: str, limit: int = 20) -> list[dict]:
        redis = self._l2_redis()
        if redis is not None:
            rows = await self.get_l2_facts(limit=1000)
            needle = query.casefold()
            return [row for row in rows if needle in str(row.get("fact_key") or "").casefold()][:limit]
        if not PostgresClient.pool:
            return []
        pattern = f"%{query}%"
        if self.agent_id is not None:
            sql = (
                "SELECT id, fact_key, fact_value, source, confidence, verified, created_at, updated_at "
                "FROM agent_global_facts WHERE agent_id=$1 AND fact_key ILIKE $2 "
                "ORDER BY updated_at DESC LIMIT $3"
            )
            params = (self.agent_id, pattern, limit)
        else:
            sql = (
                "SELECT id, fact_key, fact_value, source, confidence, verified, created_at, updated_at "
                "FROM agent_global_facts WHERE fact_key ILIKE $1 "
                "ORDER BY updated_at DESC LIMIT $2"
            )
            params = (pattern, limit)
        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    # ==================== L3 Skills ====================

    async def list_skills(
        self,
        category: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        if not PostgresClient.pool:
            return []

        conditions = []
        params: list[Any] = []
        param_idx = 1

        if self.agent_id is not None:
            conditions.append(f"agent_id=${param_idx}")
            params.append(self.agent_id)
            param_idx += 1
        if category:
            conditions.append(f"category=${param_idx}")
            params.append(category)
            param_idx += 1

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = (
            "SELECT id, name, description, category, content, trigger_patterns, version, "
            "parent_skill_id, usage_count, success_count, avg_duration_ms, created_at, updated_at, metadata "
            "FROM agent_skills "
            f"WHERE {where} "
            "ORDER BY updated_at DESC, created_at DESC "
            f"LIMIT ${param_idx}"
        )
        params.append(limit)

        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def learn_skill(
        self,
        name: str,
        content: str,
        trigger_patterns: list[str] | None = None,
        description: str = "",
        category: str = "general",
        metadata: dict | None = None,
    ) -> None:
        """Create or update a skill (upsert by name).

        FIX: original was pure INSERT, causing duplicates or failures on re-learn.
        Uses SELECT + INSERT/UPDATE in a transaction for upsert.
        """
        if not PostgresClient.pool:
            return
        patterns = trigger_patterns or []
        meta = metadata or {}
        async with PostgresClient.pool.acquire() as conn:
            async with conn.transaction():
                if self.agent_id is not None:
                    existing = await conn.fetchval(
                        "SELECT id FROM agent_skills WHERE name=$1 AND agent_id=$2",
                        name, self.agent_id,
                    )
                else:
                    existing = await conn.fetchval(
                        "SELECT id FROM agent_skills WHERE name=$1", name,
                    )

                if existing:
                    await conn.execute(
                        """
                        UPDATE agent_skills SET
                            description=$2, category=$3, content=$4,
                            trigger_patterns=$5, metadata=$6::jsonb,
                            version=version+1, updated_at=now()
                        WHERE id=$1
                        """,
                        existing, description, category, content, patterns, _dumps(meta),
                    )
                else:
                    if self.agent_id is not None:
                        await conn.execute(
                            """
                            INSERT INTO agent_skills(agent_id, name, description, category, content, trigger_patterns, metadata)
                            VALUES($1, $2, $3, $4, $5, $6, $7::jsonb)
                            """,
                            self.agent_id, name, description, category, content, patterns, _dumps(meta),
                        )
                    else:
                        await conn.execute(
                            """
                            INSERT INTO agent_skills(name, description, category, content, trigger_patterns, metadata)
                            VALUES($1, $2, $3, $4, $5, $6::jsonb)
                            """,
                            name, description, category, content, patterns, _dumps(meta),
                        )

    async def update_skill_stats(
        self,
        skill_id: int,
        success: bool = True,
        duration_ms: int = 0,
    ) -> None:
        """Update skill usage statistics after execution.

        FIX: these fields existed in the schema but had no update method.
        """
        if not PostgresClient.pool:
            return
        async with PostgresClient.pool.acquire() as conn:
            if success:
                await conn.execute(
                    """
                    UPDATE agent_skills SET
                        usage_count = usage_count + 1,
                        success_count = success_count + 1,
                        avg_duration_ms = CASE
                            WHEN usage_count = 0 THEN $2
                            ELSE ((avg_duration_ms * usage_count) + $2) / (usage_count + 1)
                        END,
                        updated_at = now()
                    WHERE id = $1
                    """,
                    skill_id, duration_ms,
                )
            else:
                await conn.execute(
                    """
                    UPDATE agent_skills SET
                        usage_count = usage_count + 1,
                        avg_duration_ms = CASE
                            WHEN usage_count = 0 THEN $2
                            ELSE ((avg_duration_ms * usage_count) + $2) / (usage_count + 1)
                        END,
                        updated_at = now()
                    WHERE id = $1
                    """,
                    skill_id, duration_ms,
                )

    async def delete_skill(self, skill_id: int) -> bool:
        """Delete a skill by ID. Returns True if deleted."""
        if not PostgresClient.pool:
            return False
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM agent_skills WHERE id=$1", skill_id)
        return result.endswith("1")

    async def search_skills(self, query: str, limit: int = 20) -> list[dict]:
        """Search skills by name/description/content (ILIKE)."""
        if not PostgresClient.pool:
            return []
        pattern = f"%{query}%"
        if self.agent_id is not None:
            sql = (
                "SELECT id, name, description, category, content, trigger_patterns, version, "
                "parent_skill_id, usage_count, success_count, avg_duration_ms, created_at, updated_at, metadata "
                "FROM agent_skills "
                "WHERE agent_id=$1 AND (name ILIKE $2 OR description ILIKE $2 OR content ILIKE $2) "
                "ORDER BY usage_count DESC, updated_at DESC LIMIT $3"
            )
            params = (self.agent_id, pattern, limit)
        else:
            sql = (
                "SELECT id, name, description, category, content, trigger_patterns, version, "
                "parent_skill_id, usage_count, success_count, avg_duration_ms, created_at, updated_at, metadata "
                "FROM agent_skills "
                "WHERE (name ILIKE $1 OR description ILIKE $1 OR content ILIKE $1) "
                "ORDER BY usage_count DESC, updated_at DESC LIMIT $2"
            )
            params = (pattern, limit)
        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    # ==================== L4 Session Archives ====================

    async def archive_session(
        self,
        session_id: str,
        task_description: str = "",
        summary: str = "",
        key_insights: list[str] | None = None,
        skills_used: list[int] | None = None,
        total_turns: int = 0,
        total_duration_ms: int = 0,
        success: bool | None = None,
        request_log_id: int | None = None,
        metadata: dict | None = None,
    ) -> None:
        if not PostgresClient.pool:
            return
        insights = key_insights or []
        skills = skills_used or []
        meta = metadata or {}
        async with PostgresClient.pool.acquire() as conn:
            if self.agent_id is not None:
                await conn.execute(
                    """
                    INSERT INTO agent_session_archives(
                        agent_id, session_id, request_log_id, task_description, summary,
                        key_insights, skills_used, total_turns, total_duration_ms, success, metadata
                    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                    """,
                    self.agent_id, session_id, request_log_id, task_description, summary,
                    insights, skills, total_turns, total_duration_ms, success, _dumps(meta),
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO agent_session_archives(
                        session_id, request_log_id, task_description, summary,
                        key_insights, skills_used, total_turns, total_duration_ms, success, metadata
                    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                    """,
                    session_id, request_log_id, task_description, summary,
                    insights, skills, total_turns, total_duration_ms, success, _dumps(meta),
                )

    async def get_l4_archive(
        self,
        limit: int = 10,
        session_id: str | None = None,
    ) -> list[dict]:
        """Get L4 archives. Can filter by session_id (FIX: was missing)."""
        if not PostgresClient.pool:
            return []

        conditions = []
        params: list[Any] = []
        param_idx = 1

        if self.agent_id is not None:
            conditions.append(f"agent_id=${param_idx}")
            params.append(self.agent_id)
            param_idx += 1
        if session_id:
            conditions.append(f"session_id=${param_idx}")
            params.append(session_id)
            param_idx += 1

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = (
            "SELECT id, session_id, request_log_id, task_description, summary, "
            "key_insights, skills_used, total_turns, total_duration_ms, success, "
            "archived_at, metadata "
            "FROM agent_session_archives "
            f"WHERE {where} "
            "ORDER BY archived_at DESC "
            f"LIMIT ${param_idx}"
        )
        params.append(limit)

        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def delete_l4_archive(self, session_id: str) -> bool:
        """Delete an L4 archive by session_id. Returns True if deleted."""
        if not PostgresClient.pool:
            return False
        if self.agent_id is not None:
            sql = "DELETE FROM agent_session_archives WHERE session_id=$1 AND agent_id=$2"
            params = (session_id, self.agent_id)
        else:
            sql = "DELETE FROM agent_session_archives WHERE session_id=$1"
            params = (session_id,)
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute(sql, *params)
        return result.endswith("1")
