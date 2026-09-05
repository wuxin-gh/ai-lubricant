from typing import Union

from loguru import logger
from redis.asyncio import Redis
from redis.asyncio.connection import BlockingConnectionPool

import config


def _flatten(values):
    """Flatten one level of list/tuple/set members for coredis call compatibility."""
    flattened = []
    for value in values:
        if isinstance(value, (list, tuple, set, frozenset)):
            flattened.extend(value)
        else:
            flattened.append(value)
    return flattened


class RedisJdbc(Redis):
    """Official redis-py async client with the project's legacy key helpers."""

    def __init__(self, *args, prefix_key=None, **kwargs):
        kwargs.pop("is_cluster", None)
        kwargs.pop("connection_pool_cls", None)
        pool_timeout = kwargs.pop("pool_timeout", 10)
        stream_timeout = kwargs.pop("stream_timeout", 10)
        connect_timeout = kwargs.pop("connect_timeout", 10)
        self.prefix_key = prefix_key

        if "connection_pool" not in kwargs:
            pool = BlockingConnectionPool(
                max_connections=kwargs.pop("max_connections", 50),
                timeout=pool_timeout,
                socket_timeout=stream_timeout,
                socket_connect_timeout=connect_timeout,
                **kwargs,
            )
            kwargs = {"connection_pool": pool}
        super().__init__(*args, **kwargs)

    def get_key(self, key) -> str:
        if self.prefix_key is not None:
            return f"{self.prefix_key}:{key}"
        return key

    async def list_all(self, key) -> list:
        key = self.get_key(key)
        count = await super().llen(key)
        if count == 0:
            return []
        return await super().lrange(key, 0, count - 1)

    async def lset(self, key, values) -> list:
        return await super().lset(self.get_key(key), values)

    def lock(self, key, *args, **kwargs):
        return super().lock(self.get_key(key), *args, **kwargs)

    async def get(self, key):
        return await super().get(self.get_key(key))

    async def set(self, key, *args, **kwargs):
        return await super().set(self.get_key(key), *args, **kwargs)

    async def delete(self, *keys):
        flattened = _flatten(keys)
        if not flattened:
            return 0
        return await super().delete(*(self.get_key(key) for key in flattened))

    async def hgetall(self, key):
        return await super().hgetall(self.get_key(key))

    async def hget(self, key, field):
        return await super().hget(self.get_key(key), field)

    async def hset(self, key, *args, **kwargs):
        # coredis accepted hset(key, mapping) positionally; redis-py requires mapping=.
        if len(args) == 1 and isinstance(args[0], dict) and "mapping" not in kwargs:
            kwargs["mapping"] = args[0]
            args = ()
        return await super().hset(self.get_key(key), *args, **kwargs)

    async def hdel(self, key, *fields):
        flattened = _flatten(fields)
        if not flattened:
            return 0
        return await super().hdel(key, *flattened)

    async def hincrby(self, key, field, amount=1):
        return await super().hincrby(self.get_key(key), field, amount)

    async def expire(self, key, seconds):
        return await super().expire(self.get_key(key), seconds)

    async def sadd(self, key, *values):
        flattened = _flatten(values)
        if not flattened:
            return 0
        return await super().sadd(key, *flattened)

    async def srem(self, key, *values):
        flattened = _flatten(values)
        if not flattened:
            return 0
        return await super().srem(key, *flattened)

    async def zadd(self, key, *args, **kwargs):
        return await super().zadd(self.get_key(key), *args, **kwargs)

    async def zcard(self, key):
        return await super().zcard(self.get_key(key))

    async def zrange(self, key, *args, **kwargs):
        return await super().zrange(self.get_key(key), *args, **kwargs)

    async def zremrangebyscore(self, key, *args, **kwargs):
        return await super().zremrangebyscore(self.get_key(key), *args, **kwargs)

    async def zcount(self, key, *args, **kwargs):
        return await super().zcount(self.get_key(key), *args, **kwargs)

    async def eval(self, script, numkeys=None, *keys_and_args, keys=None, args=None):
        """Accept coredis ``keys=``/``args=`` and redis-py positional forms."""
        if keys is not None or args is not None:
            if numkeys is not None or keys_and_args:
                raise TypeError("eval positional arguments cannot be mixed with keys/args")
            script_keys = list(keys or ())
            script_args = list(args or ())
            return await super().eval(script, len(script_keys), *(script_keys + script_args))
        if numkeys is None:
            numkeys = 0
        return await super().eval(script, numkeys, *keys_and_args)


class JdbcClient:
    redis: Union[None, RedisJdbc] = None

    @classmethod
    def pool_snapshot(cls) -> dict[str, int | float | str | None]:
        """Best-effort snapshot for diagnostics; no pool internals are mutated."""
        redis = cls.redis
        pool = getattr(redis, "connection_pool", None) if redis is not None else None
        if pool is None:
            return {"active": 0, "in_use": 0, "available": 0, "waiting": 0,
                    "constructing": 0, "deficit": 0, "max": 0, "pool_timeout": None}
        try:
            connections = getattr(pool, "_connections", ()) or ()
            in_use = len(getattr(pool, "_in_use_connections", ()) or ())
            max_connections = int(getattr(pool, "max_connections", 0) or 0)
            return {
                "active": len(connections),
                "in_use": in_use,
                "available": max(0, max_connections - in_use),
                "waiting": -1,
                "constructing": 0,
                "deficit": 0,
                "max": max_connections,
                "pool_timeout": getattr(pool, "timeout", None),
            }
        except Exception as exc:
            return {"active": 0, "in_use": -1, "available": -1, "waiting": -1,
                    "constructing": -1, "deficit": -1,
                    "max": int(getattr(pool, "max_connections", 0) or 0),
                    "pool_timeout": getattr(pool, "timeout", None),
                    "snapshot_error": type(exc).__name__}

    @classmethod
    async def ping(cls):
        redis_config = config.Config.get_redis_config()
        cls.redis = RedisJdbc(**redis_config)
        await cls.redis.ping()
        logger.debug("redis连接成功")

    @classmethod
    async def close(cls) -> None:
        redis = cls.redis
        cls.redis = None
        if redis is None:
            return
        await redis.aclose()
        logger.debug("redis连接池已关闭")
