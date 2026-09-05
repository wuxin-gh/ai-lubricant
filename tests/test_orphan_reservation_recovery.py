import asyncio
import json

from limits.backend import RedisLimitBackend


class _RecoveryRedis:
    prefix_key = None

    def __init__(self):
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.heartbeats: set[str] = set()
        self.rollback_calls: list[tuple[str, str | None, str, dict | None]] = []

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def exists(self, key):
        return 1 if key in self.heartbeats else 0

    async def srem(self, key, members):
        values = self.sets.setdefault(key, set())
        removed = 0
        for member in members:
            if member in values:
                values.remove(member)
                removed += 1
        return removed

    async def delete(self, *keys):
        for key in keys:
            self.hashes.pop(key, None)
        return len(keys)


async def _fake_rollback(cls, account_key, model_id, reservation_id, *, window_ids=None):
    redis = cls.redis()
    redis.rollback_calls.append((account_key, model_id, reservation_id, window_ids))
    return {"removed": 1}


def _meta(reservation_id: str, boot_id: str) -> dict[str, str]:
    return {
        "account_key": "account:p1:u1",
        "model_id": "m1",
        "window_ids": json.dumps({
            "rpm": "1", "rph": "1", "rpd": "2026-08-19",
            "tpm": "1", "tph": "1", "tpd": "2026-08-19", "model_tpm": "1",
        }),
        "boot_id": boot_id,
    }


def test_reap_orphan_reservation_rolls_back_dead_owner(monkeypatch):
    redis = _RecoveryRedis()
    reservation_id = "lease:old-boot:abc"
    index_key = RedisLimitBackend._RESERVATION_INDEX
    redis.sets[index_key] = {reservation_id}
    redis.hashes[RedisLimitBackend._reservation_meta_key(reservation_id)] = _meta(
        reservation_id, "old-boot",
    )
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: redis))
    monkeypatch.setattr(
        RedisLimitBackend, "rollback_account_reservation", classmethod(_fake_rollback),
    )

    reaped = asyncio.run(RedisLimitBackend.reap_orphan_reservations(
        current_boot_id="new-boot",
    ))

    assert reaped == 1
    assert redis.rollback_calls == [(
        "account:p1:u1", "m1", reservation_id,
        {"rpm": "1", "rph": "1", "rpd": "2026-08-19", "tpm": "1",
         "tph": "1", "tpd": "2026-08-19", "model_tpm": "1"},
    )]
    assert reservation_id not in redis.sets[index_key]
    assert RedisLimitBackend._reservation_meta_key(reservation_id) not in redis.hashes


def test_reap_keeps_live_owner_reservation(monkeypatch):
    redis = _RecoveryRedis()
    reservation_id = "lease:live-boot:abc"
    index_key = RedisLimitBackend._RESERVATION_INDEX
    redis.sets[index_key] = {reservation_id}
    redis.hashes[RedisLimitBackend._reservation_meta_key(reservation_id)] = _meta(
        reservation_id, "live-boot",
    )
    redis.heartbeats.add(RedisLimitBackend._process_heartbeat_key("live-boot"))
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: redis))
    monkeypatch.setattr(
        RedisLimitBackend, "rollback_account_reservation", classmethod(_fake_rollback),
    )

    reaped = asyncio.run(RedisLimitBackend.reap_orphan_reservations(
        current_boot_id="new-boot",
    ))

    assert reaped == 0
    assert redis.rollback_calls == []
    assert reservation_id in redis.sets[index_key]


def test_reap_is_idempotent(monkeypatch):
    redis = _RecoveryRedis()
    reservation_id = "lease:old-boot:abc"
    index_key = RedisLimitBackend._RESERVATION_INDEX
    redis.sets[index_key] = {reservation_id}
    redis.hashes[RedisLimitBackend._reservation_meta_key(reservation_id)] = _meta(
        reservation_id, "old-boot",
    )
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: redis))
    monkeypatch.setattr(
        RedisLimitBackend, "rollback_account_reservation", classmethod(_fake_rollback),
    )

    first = asyncio.run(RedisLimitBackend.reap_orphan_reservations(current_boot_id="new-boot"))
    second = asyncio.run(RedisLimitBackend.reap_orphan_reservations(current_boot_id="new-boot"))

    assert first == 1
    assert second == 0
    assert len(redis.rollback_calls) == 1
