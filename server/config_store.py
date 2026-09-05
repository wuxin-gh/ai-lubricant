"""配置存储抽象。"""
import asyncio
import json
from typing import Protocol
from bootstrap_config import ConfigurationError
from db import PostgresClient



def _main_config_missing_error() -> ConfigurationError:
    return ConfigurationError("PostgreSQL 缺少必填主配置 app_config['main']，请先完成初始化配置")

class ConfigStore(Protocol):
    def read_main(self) -> dict:
        ...

    def write_main(self, data: dict) -> None:
        ...

    def read_provider(self, name: str) -> dict:
        ...

    def write_provider(self, name: str, data: dict) -> None:
        ...

    def list_providers(self) -> dict[str, dict]:
        ...


class PostgresConfigStore:
    def __init__(self):
        self._main_cache: dict | None = None
        self._providers_cache: dict[str, dict] | None = None

    async def init(self):
        await PostgresClient.init()
        await self.refresh_cache()

    async def refresh_cache(self):
        data = await PostgresClient.get_config("main")
        if data is None:
            raise _main_config_missing_error()
        self._main_cache = data
        self._providers_cache = await PostgresClient.list_provider_configs()

    def _run(self, coro):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        raise RuntimeError("PostgresConfigStore 的同步方法不能在事件循环内调用，请使用 async 方法")

    def read_main(self) -> dict:
        if self._main_cache is not None:
            return self._main_cache
        if PostgresClient.pool is None:
            raise ConfigurationError("PostgreSQL 尚未初始化，无法读取主配置")
        data = self._run(PostgresClient.get_config("main"))
        if data is None:
            raise _main_config_missing_error()
        self._main_cache = data
        return self._main_cache

    def write_main(self, data: dict) -> None:
        self._main_cache = data
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._run(self._write_main_storage(data))
        else:
            asyncio.create_task(self._write_main_storage(data))

    def read_provider(self, name: str) -> dict:
        if self._providers_cache is not None and name in self._providers_cache:
            return self._providers_cache[name]
        if PostgresClient.pool is None:
            raise FileNotFoundError(f"provider '{name}' 不存在 (数据库未连接)")
        data = self._run(PostgresClient.get_provider_config(name))
        if data is None:
            raise FileNotFoundError(f"provider '{name}' 不存在")
        if self._providers_cache is not None:
            self._providers_cache[name] = data
        return data

    def write_provider(self, name: str, data: dict) -> None:
        if self._providers_cache is not None:
            self._providers_cache[name] = data
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._run(PostgresClient.set_provider_config(name, data))
        else:
            asyncio.create_task(PostgresClient.set_provider_config(name, data))

    def delete_provider(self, name: str) -> None:
        if self._providers_cache is not None:
            self._providers_cache.pop(name, None)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._run(PostgresClient.delete_provider_config(name))
            self._run(RedisConfigCache.delete_provider_cache(name))
        else:
            asyncio.create_task(PostgresClient.delete_provider_config(name))
            asyncio.create_task(RedisConfigCache.delete_provider_cache(name))

    def list_providers(self) -> dict[str, dict]:
        if self._providers_cache is not None:
            return self._providers_cache
        if PostgresClient.pool is None:
            return {}
        self._providers_cache = self._run(PostgresClient.list_provider_configs()) or {}
        return self._providers_cache

    async def read_main_async(self) -> dict:
        if PostgresClient.pool is None:
            raise ConfigurationError("PostgreSQL 尚未初始化，无法读取主配置")
        data = await PostgresClient.get_config("main")
        if data is None:
            raise _main_config_missing_error()
        self._main_cache = data
        return self._main_cache

    async def write_main_async(self, data: dict) -> None:
        await self._write_main_storage(data)
        self._main_cache = data

    async def _write_main_storage(self, data: dict) -> None:
        await PostgresClient.set_config("main", data)
        await RedisConfigCache.set_main_cache(data)

    async def read_provider_async(self, name: str) -> dict:
        data = await PostgresClient.get_provider_config(name)
        if data is None:
            raise FileNotFoundError(f"provider '{name}' 不存在")
        if self._providers_cache is not None:
            self._providers_cache[name] = data
        return data

    async def read_provider_base_async(self, name: str) -> dict:
        """只读渠道基础配置，不查账号表，供基础配置接口使用。"""
        data = await PostgresClient.get_provider_base_config(name)
        if data is None:
            raise FileNotFoundError(f"provider '{name}' 不存在")
        return data

    async def write_provider_async(self, name: str, data: dict) -> None:
        await PostgresClient.set_provider_config(name, data)
        if self._providers_cache is not None:
            self._providers_cache[name] = data

    async def write_provider_base_async(self, name: str, data: dict, *, drop_accounts: bool = False) -> None:
        """窄写渠道基础配置（不含 accounts 表写入）。

        仅刷新内存快照的 base 部分；accounts 保持原样（除非 drop_accounts）。
        """
        await PostgresClient.set_provider_base_config(name, data, drop_accounts=drop_accounts)
        if self._providers_cache is not None:
            cached = self._providers_cache.get(name)
            if cached is None or drop_accounts:
                self._providers_cache[name] = data
            else:
                merged = dict(data)
                merged["accounts"] = cached.get("accounts", [])
                self._providers_cache[name] = merged

    async def upsert_provider_account_async(self, name: str, account: dict) -> None:
        """窄写单账号行：UPSERT provider_accounts；同时同步内存快照里的 accounts。"""
        await PostgresClient.upsert_provider_account(name, account)
        if self._providers_cache is not None:
            cached = self._providers_cache.get(name)
            if cached is None:
                return
            accounts = list(cached.get("accounts") or [])
            username = account.get("username")
            replaced = False
            for i, acc in enumerate(accounts):
                if acc.get("username") == username:
                    accounts[i] = account
                    replaced = True
                    break
            if not replaced:
                accounts.append(account)
            cached["accounts"] = accounts

    async def delete_provider_account_async(self, name: str, username: str) -> bool:
        """窄写单账号行：DELETE provider_accounts；同步从内存快照移除。"""
        removed = await PostgresClient.delete_provider_account(name, username)
        if self._providers_cache is not None:
            cached = self._providers_cache.get(name)
            if cached is not None:
                cached["accounts"] = [
                    a for a in (cached.get("accounts") or []) if a.get("username") != username
                ]
        return removed

    async def delete_provider_async(self, name: str) -> None:
        await PostgresClient.delete_provider_config(name)
        await RedisConfigCache.delete_provider_cache(name)
        if self._providers_cache is not None:
            self._providers_cache.pop(name, None)

    async def list_providers_async(self) -> dict[str, dict]:
        self._providers_cache = await PostgresClient.list_provider_configs()
        return self._providers_cache


class RedisConfigCache:
    MAIN_KEY = "ai-lubricant:config:main"
    PROVIDER_KEY_PREFIX = "ai-lubricant:config:provider:"
    TTL_SECONDS = 300

    def __init__(self, store: PostgresConfigStore):
        self.store = store

    @staticmethod
    def _redis():
        try:
            from rd import JdbcClient
            return JdbcClient.redis
        except Exception:
            return None

    @classmethod
    async def get(cls, key: str) -> dict | None:
        redis = cls._redis()
        if redis is None:
            return None
        try:
            value = await redis.get(key)
        except Exception:
            return None
        if value is None:
            return None
        try:
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return json.loads(value)
        except Exception:
            return None

    @classmethod
    async def set(cls, key: str, data: dict) -> None:
        redis = cls._redis()
        if redis is None:
            return
        try:
            await redis.set(key, json.dumps(data, ensure_ascii=False), ex=cls.TTL_SECONDS)
        except TypeError:
            await redis.set(key, json.dumps(data, ensure_ascii=False))
            try:
                await redis.expire(key, cls.TTL_SECONDS)
            except Exception:
                pass
        except Exception:
            pass

    @classmethod
    async def delete(cls, key: str) -> None:
        redis = cls._redis()
        if redis is None:
            return
        try:
            await redis.delete(key)
        except Exception:
            pass

    @classmethod
    async def get_main_cache(cls) -> dict | None:
        return await cls.get(cls.MAIN_KEY)

    @classmethod
    async def set_main_cache(cls, data: dict) -> None:
        await cls.set(cls.MAIN_KEY, data)

    @classmethod
    async def get_provider_cache(cls, name: str) -> dict | None:
        return await cls.get(f"{cls.PROVIDER_KEY_PREFIX}{name}")

    @classmethod
    async def set_provider_cache(cls, name: str, data: dict) -> None:
        await cls.set(f"{cls.PROVIDER_KEY_PREFIX}{name}", data)

    @classmethod
    async def delete_provider_cache(cls, name: str) -> None:
        await cls.delete(f"{cls.PROVIDER_KEY_PREFIX}{name}")

    def _run(self, coro):
        return self.store._run(coro)

    async def init(self):
        await self.store.init()

    async def refresh_cache(self):
        await self.store.refresh_cache()
        if self.store._main_cache is not None:
            await self.set_main_cache(self.store._main_cache)
        if self.store._providers_cache is not None:
            for name, data in self.store._providers_cache.items():
                await self.set_provider_cache(name, data)

    def read_main(self) -> dict:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            cached = self._run(self.get_main_cache())
            if cached is not None:
                self.store._main_cache = cached
                return cached
            data = self.store.read_main()
            self._run(self.set_main_cache(data))
            return data
        return self.store.read_main()

    async def read_main_async(self) -> dict:
        cached = await self.get_main_cache()
        if cached is not None:
            self.store._main_cache = cached
            return cached
        data = await self.store.read_main_async()
        await self.set_main_cache(data)
        return data

    def write_main(self, data: dict) -> None:
        self.store.write_main(data)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._run(self.set_main_cache(data))
        else:
            asyncio.create_task(self.set_main_cache(data))

    async def write_main_async(self, data: dict) -> None:
        await self.store.write_main_async(data)
        await self.set_main_cache(data)

    def read_provider(self, name: str) -> dict:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            cached = self._run(self.get_provider_cache(name))
            if cached is not None:
                if self.store._providers_cache is not None:
                    self.store._providers_cache[name] = cached
                return cached
            data = self.store.read_provider(name)
            self._run(self.set_provider_cache(name, data))
            return data
        return self.store.read_provider(name)

    async def read_provider_async(self, name: str) -> dict:
        cached = await self.get_provider_cache(name)
        if cached is not None:
            if self.store._providers_cache is not None:
                self.store._providers_cache[name] = cached
            return cached
        data = await self.store.read_provider_async(name)
        await self.set_provider_cache(name, data)
        return data

    async def read_provider_base_async(self, name: str) -> dict:
        return await self.store.read_provider_base_async(name)

    async def write_provider_base_async(self, name: str, data: dict, *, drop_accounts: bool = False) -> None:
        await self.store.write_provider_base_async(name, data, drop_accounts=drop_accounts)
        cached = (self.store._providers_cache or {}).get(name)
        await self.set_provider_cache(name, cached or data)

    async def upsert_provider_account_async(self, name: str, account: dict) -> None:
        await self.store.upsert_provider_account_async(name, account)
        cached = (self.store._providers_cache or {}).get(name)
        if cached is not None:
            await self.set_provider_cache(name, cached)

    async def delete_provider_account_async(self, name: str, username: str) -> bool:
        removed = await self.store.delete_provider_account_async(name, username)
        cached = (self.store._providers_cache or {}).get(name)
        if cached is not None:
            await self.set_provider_cache(name, cached)
        return removed

    def write_provider(self, name: str, data: dict) -> None:
        self.store.write_provider(name, data)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._run(self.set_provider_cache(name, data))
        else:
            asyncio.create_task(self.set_provider_cache(name, data))

    async def write_provider_async(self, name: str, data: dict) -> None:
        await self.store.write_provider_async(name, data)
        await self.set_provider_cache(name, data)

    def delete_provider(self, name: str) -> None:
        self.store.delete_provider(name)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._run(self.delete_provider_cache(name))
        else:
            asyncio.create_task(self.delete_provider_cache(name))

    async def delete_provider_async(self, name: str) -> None:
        await self.store.delete_provider_async(name)
        await self.delete_provider_cache(name)

    def list_providers(self) -> dict[str, dict]:
        return self.store.list_providers()

    async def list_providers_async(self) -> dict[str, dict]:
        providers = await self.store.list_providers_async()
        for name, data in providers.items():
            await self.set_provider_cache(name, data)
        return providers


RedisConfigStore = RedisConfigCache
