"""Independent C-side user session store (Redis-backed).

This session mechanism is fully separate from the admin-side ``admin token``
auth used by ``/admin/*``. It retains the Redis-Hash session model inherited
from the compatibility layer:

* Hash key   = ``{name}:{uid}``   field = cookie UUID  value = JSON payload
* Lookup key = ``lookup:{name}:{cookie}`` -> uid   (reverse lookup on read)

Two Ai Lubricant cookie namespaces:
* ``ai_lubricant_session``      -> normal user session
* ``ai_lubricant_team_session`` -> team-scoped session

The store uses the shared coredis client (``JdbcClient.redis``) so it inherits
the same connection pool and key prefix as the rest of the platform. It never
touches the admin token path.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from loguru import logger

USER_SESSION_COOKIE = "ai_lubricant_session"
TEAM_SESSION_COOKIE = "ai_lubricant_team_session"

_DEFAULT_EXPIRE_DAYS = 30


def _hash_key(name: str, uid: str) -> str:
    return f"{name}:{uid}"


def _lookup_key(name: str, cookie: str) -> str:
    return f"lookup:{name}:{cookie}"


@dataclass(frozen=True)
class SessionData:
    uid: str
    cookie: str
    payload: dict


class SessionStore:
    """Redis-backed session store for C-side users, isolated from admin auth."""

    def __init__(self, expire_days: int = _DEFAULT_EXPIRE_DAYS) -> None:
        self.expire_seconds = max(1, expire_days) * 24 * 3600

    def _redis(self):
        from rd import JdbcClient

        if JdbcClient.redis is None:
            raise RuntimeError("redis client not initialized")
        return JdbcClient.redis

    async def save(self, name: str, uid: str, payload: dict) -> str:
        """Create a session, returning the generated cookie value."""
        redis = self._redis()
        cookie = uuid.uuid4().hex
        key = _hash_key(name, uid)
        await redis.hset(key, {cookie: json.dumps(payload, ensure_ascii=False)})
        await redis.expire(key, self.expire_seconds)
        await redis.set(_lookup_key(name, cookie), uid, ex=self.expire_seconds)
        return cookie

    async def get(self, name: str, cookie: str) -> SessionData | None:
        """Resolve a session by cookie via the lookup key."""
        if not cookie:
            return None
        redis = self._redis()
        uid = await redis.get(_lookup_key(name, cookie))
        if not uid:
            return None
        if isinstance(uid, bytes):
            uid = uid.decode("utf-8")
        raw = await redis.hget(_hash_key(name, uid), cookie)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            payload = json.loads(raw)
        except (ValueError, TypeError):
            payload = {}
        return SessionData(uid=uid, cookie=cookie, payload=payload)

    async def delete(self, name: str, uid: str, cookie: str) -> None:
        """Delete a single session (logout)."""
        if not cookie:
            return
        redis = self._redis()
        try:
            # RedisJdbc overrides hset/hget/expire with prefixing but NOT hdel,
            # so prefix the key manually to hit the same key hset wrote.
            await redis.hdel(redis.get_key(_hash_key(name, uid)), [cookie])
        except Exception:
            logger.debug("[monkeycode-compat] session hdel failed", exc_info=True)
        await redis.delete(_lookup_key(name, cookie))

    async def delete_user_sessions(self, name: str, uid: str) -> int:
        """Delete **all** sessions for a user (every cookie on their hash).

        Used when an admin mutates role / is_blocked / is_deleted / status: the
        Redis payload is a login-time snapshot, so the only way to guarantee a
        stale role or block flag does not authorize the next request is to drop
        every cookie the user currently holds. They re-login to mint a fresh
        payload. Returns the number of sessions removed.
        """
        redis = self._redis()
        hash_key = _hash_key(name, uid)
        try:
            # hgetall is not prefix-wrapped in RedisJdbc either, so mirror the
            # manual prefix used by delete().
            cookies = await redis.hgetall(redis.get_key(hash_key))
        except Exception:
            logger.debug("[monkeycode-compat] user session hgetall failed", exc_info=True)
            return 0
        if not cookies:
            return 0
        count = 0
        for cookie in list(cookies.keys()):
            if isinstance(cookie, bytes):
                cookie = cookie.decode("utf-8", errors="replace")
            cookie = str(cookie)
            if not cookie:
                continue
            try:
                await redis.delete(_lookup_key(name, cookie))
            except Exception:
                logger.debug("[monkeycode-compat] session lookup delete failed", exc_info=True)
            count += 1
        try:
            await redis.delete(redis.get_key(hash_key))
        except Exception:
            logger.debug("[monkeycode-compat] session hash delete failed", exc_info=True)
        return count


session_store = SessionStore()
