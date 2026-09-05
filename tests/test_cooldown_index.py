"""冻结索引读写行为测试。

覆盖 RedisLimitBackend 的渠道级冻结索引：写入 cooldown 时维护索引，
读取（batch_get_provider_model_cooldowns / scan_all_cooldowns）时只读索引 +
精确 key TTL 校验，不再全库 SCAN；清除时同步摘除索引；失效成员惰性自愈。
"""
import asyncio
import time

import pytest

from limits.backend import RedisLimitBackend


class _FakePipe:
    """模拟 redis-py pipeline：queue ttl/get，显式 execute 返回结果。"""

    def __init__(self, store: dict):
        self._store = store
        self._queue: list[tuple[str, str]] = []

    def ttl(self, key):
        self._queue.append(("ttl", key))

    def get(self, key):
        self._queue.append(("get", key))

    async def execute(self):
        results = []
        for op, key in self._queue:
            entry = self._store.get(key)
            if entry is None:
                results.append(-2 if op == "ttl" else None)
                continue
            value, expire_at = entry
            remaining = expire_at - time.time()
            if remaining <= 0:
                results.append(-2 if op == "ttl" else None)
                continue
            results.append(int(remaining) if op == "ttl" else value)
        return results

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeRedis:
    """最小内存 Redis：prefix_key=None 让 _key/get_key 恒等，专注测索引逻辑。

    - 字符串：set(ex)/get/delete/ttl
    - 集合：sadd/srem/smembers/scard
    所有 key 按裸 key 存储（因 prefix_key=None）。
    """

    prefix_key = None

    def __init__(self):
        self.kv: dict[str, tuple[str, float]] = {}   # key -> (value, expire_at)
        self.sets: dict[str, set[str]] = {}

    def _alive(self, key):
        entry = self.kv.get(key)
        if entry is None:
            return None
        value, expire_at = entry
        if expire_at - time.time() <= 0:
            self.kv.pop(key, None)
            return None
        return value

    async def set(self, key, value, ex=None):
        expire_at = time.time() + (ex if ex else 10_000)
        self.kv[key] = (str(value), expire_at)

    async def get(self, key):
        return self._alive(key)

    async def delete(self, *keys):
        # RedisJdbc.delete 传入的是 list；这里两种调用形态都兼容
        flat = []
        for k in keys:
            if isinstance(k, (list, tuple)):
                flat.extend(k)
            else:
                flat.append(k)
        n = 0
        for k in flat:
            if self.kv.pop(k, None) is not None:
                n += 1
            if self.sets.pop(k, None) is not None:
                n += 1
        return n

    async def ttl(self, key):
        entry = self.kv.get(key)
        if entry is None:
            return -2
        remaining = entry[1] - time.time()
        return int(remaining) if remaining > 0 else -2

    async def expire(self, key, seconds):
        entry = self.kv.get(key)
        if entry is not None:
            self.kv[key] = (entry[0], time.time() + seconds)

    async def sadd(self, key, members):
        s = self.sets.setdefault(key, set())
        before = len(s)
        for m in members:
            s.add(str(m))
        return len(s) - before

    async def srem(self, key, members):
        s = self.sets.get(key)
        if not s:
            return 0
        n = 0
        for m in members:
            if str(m) in s:
                s.discard(str(m))
                n += 1
        if not s:
            self.sets.pop(key, None)
        return n

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def scard(self, key):
        return len(self.sets.get(key, set()))

    def pipeline(self, transaction=False):
        return _FakePipe(self.kv)


@pytest.fixture
def fake_redis(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(RedisLimitBackend, "redis", classmethod(lambda cls: fake))
    return fake


def test_set_account_cooldown_populates_index(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_cooldown("p1", "u1", 100, "rate"))

    assert "u1" in fake_redis.sets["limit:cooldown_index:p1:accounts"]
    assert "p1" in fake_redis.sets["limit:cooldown_index:providers"]


def test_set_account_model_cooldown_populates_index(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u1", "m1", 100, "rate"))

    members = fake_redis.sets["limit:cooldown_index:p1:models"]
    assert f"u1{RedisLimitBackend._MODEL_SEP}m1" in members
    assert "p1" in fake_redis.sets["limit:cooldown_index:providers"]


def test_batch_get_provider_model_cooldowns_reads_index(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u1", "m1", 100, "over"))
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u2", "m2", 100, ""))

    result = asyncio.run(RedisLimitBackend.batch_get_provider_model_cooldowns("p1"))

    assert set(result.keys()) == {"u1", "u2"}
    assert result["u1"][0]["model"] == "m1"
    assert result["u1"][0]["reason"] == "over"
    assert result["u1"][0]["remaining"] > 0


def test_batch_get_provider_model_cooldowns_prunes_stale_member(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u1", "m1", 100, "keep"))
    # 精确 key 过期但索引成员残留 → 读取时应惰性 SREM
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u2", "m2", 100, "gone"))
    fake_redis.kv.pop("limit:cooldown:p1:u2:model:m2", None)

    result = asyncio.run(RedisLimitBackend.batch_get_provider_model_cooldowns("p1"))

    assert set(result.keys()) == {"u1"}
    assert f"u2{RedisLimitBackend._MODEL_SEP}m2" not in fake_redis.sets["limit:cooldown_index:p1:models"]


def test_clear_account_cooldown_removes_index(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_cooldown("p1", "u1", 100, "rate"))
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u1", "m1", 100, "rate"))

    asyncio.run(RedisLimitBackend.clear_account_cooldown("p1", "u1"))

    assert "u1" not in fake_redis.sets.get("limit:cooldown_index:p1:accounts", set())
    assert not fake_redis.sets.get("limit:cooldown_index:p1:models")
    # 渠道再无冻结成员 → 从 providers 索引摘除
    assert "p1" not in fake_redis.sets.get("limit:cooldown_index:providers", set())


def test_clear_account_model_cooldown_removes_index_member(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u1", "m1", 100, ""))
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u1", "m2", 100, ""))

    asyncio.run(RedisLimitBackend.clear_account_model_cooldown("p1", "u1", "m1"))

    members = fake_redis.sets["limit:cooldown_index:p1:models"]
    assert f"u1{RedisLimitBackend._MODEL_SEP}m1" not in members
    assert f"u1{RedisLimitBackend._MODEL_SEP}m2" in members


def test_clear_all_provider_cooldowns_clears_index(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_cooldown("p1", "u1", 100, ""))
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u2", "m1", 100, ""))

    asyncio.run(RedisLimitBackend.clear_all_provider_cooldowns("p1"))

    assert "limit:cooldown_index:p1:accounts" not in fake_redis.sets
    assert "limit:cooldown_index:p1:models" not in fake_redis.sets
    assert "p1" not in fake_redis.sets.get("limit:cooldown_index:providers", set())


def test_scan_all_cooldowns_reads_index(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_cooldown("p1", "u1", 100, "acc"))
    asyncio.run(RedisLimitBackend.set_account_model_cooldown("p1", "u2", "m1", 100, "mdl"))
    asyncio.run(RedisLimitBackend.set_account_cooldown("p2", "ux", 100, "acc2"))

    result = asyncio.run(RedisLimitBackend.scan_all_cooldowns())

    assert result["accounts"]["p1"]["u1"]["reason"] == "acc"
    assert result["accounts"]["p2"]["ux"]["reason"] == "acc2"
    assert result["models"]["p1"]["u2"][0]["model"] == "m1"


def test_scan_all_cooldowns_prunes_stale_and_empty_provider(fake_redis):
    asyncio.run(RedisLimitBackend.set_account_cooldown("p1", "u1", 100, "gone"))
    # 精确 key 过期，索引成员残留
    fake_redis.kv.pop("limit:cooldown:p1:u1", None)

    result = asyncio.run(RedisLimitBackend.scan_all_cooldowns())

    assert "p1" not in result["accounts"]
    assert "u1" not in fake_redis.sets.get("limit:cooldown_index:p1:accounts", set())
    # 渠道无有效冻结 → providers 索引摘除
    assert "p1" not in fake_redis.sets.get("limit:cooldown_index:providers", set())
