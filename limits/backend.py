"""Storage backends for runtime limit counters."""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta, timezone

from loguru import logger


def _decode_meta_field(meta: dict, name: str, default: str = "") -> str:
    value = meta.get(name)
    if value is None:
        value = meta.get(name.encode("utf-8"))
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else default


class RedisLimitBackend:
    """Small Redis helper for sliding counters and expiring concurrency leases."""

    @staticmethod
    def redis():
        try:
            from rd import JdbcClient
            return JdbcClient.redis
        except Exception:
            return None

    @classmethod
    def _key(cls, key: str) -> str:
        """拼接 Redis 客户端的 key 前缀。

        coredis 原生方法（scan_iter/pipeline/ttl/keys 等）不会经过
        RedisJdbc.get_key 加前缀，只有被显式覆写的方法（get/set/...）才加。
        因此所有直接走原生方法的 key 都必须在这里显式补前缀，否则读写不对称。
        """
        redis = cls.redis()
        prefix = getattr(redis, "prefix_key", None)
        if prefix:
            return f"{prefix}:{key}"
        return key

    @classmethod
    def _bare_key(cls, key: str) -> str:
        """把 scan 返回的带前缀 key 还原成裸 key（供覆写方法 get/set/delete 使用）。"""
        redis = cls.redis()
        prefix = getattr(redis, "prefix_key", None)
        if prefix and key.startswith(f"{prefix}:"):
            return key[len(prefix) + 1:]
        return key

    # ==================== 冻结索引（渠道级 / 账号级 / 模型级） ====================
    # 目的：查询"哪些渠道/账号/模型被冻结"时，只读该渠道范围的索引集合 + 精确 key 校验，
    # 彻底避免全库 SCAN（SCAN MATCH 无论命中几个 key 都要遍历整库）。
    # 索引集合（成员就是冻结主体，key 存在性由精确 cooldown key 的 TTL 最终裁定）：
    #   limit:cooldown_index:providers            成员 = {provider}          有任一冻结的渠道
    #   limit:cooldown_index:{provider}:accounts  成员 = {username}          账号级冻结
    #   limit:cooldown_index:{provider}:models    成员 = {username}\x1f{m}   账号-模型级冻结
    # 冷却 key 会随 TTL 自然过期，索引成员因此可能残留；读取时按精确 key TTL 惰性 SREM 清理，
    # 保证展示自愈。是否冻结的权威判定始终以精确 cooldown key 的 TTL 为准，索引仅用于缩小扫描范围。
    # 注意：sadd/srem/smembers/scard 是 coredis 原生方法，不经 RedisJdbc.get_key 加前缀，
    # 因此这里所有索引 key 都必须显式走 cls._key() 补前缀（与 _key 文档一致）。

    _INDEX_PROVIDERS = "limit:cooldown_index:providers"
    _MODEL_SEP = "\x1f"

    @classmethod
    def _index_accounts_key(cls, provider: str) -> str:
        return f"limit:cooldown_index:{provider}:accounts"

    @classmethod
    def _index_models_key(cls, provider: str) -> str:
        return f"limit:cooldown_index:{provider}:models"

    @classmethod
    async def _index_mark(cls, provider: str, username: str, model: str | None) -> None:
        """写入冻结索引：账号级传 model=None，账号-模型级传具体 model。"""
        redis = cls.redis()
        if redis is None or not provider or not username:
            return
        try:
            if model is None:
                await redis.sadd(cls._key(cls._index_accounts_key(provider)), [username])
            elif model:
                await redis.sadd(
                    cls._key(cls._index_models_key(provider)),
                    [f"{username}{cls._MODEL_SEP}{model}"],
                )
            await redis.sadd(cls._key(cls._INDEX_PROVIDERS), [provider])
        except Exception:
            pass

    @classmethod
    async def _index_unmark_account(cls, provider: str, username: str) -> None:
        """清除账号级 + 该账号所有模型级索引成员；若渠道再无冻结成员则从渠道索引摘除。"""
        redis = cls.redis()
        if redis is None or not provider or not username:
            return
        acc_key = cls._key(cls._index_accounts_key(provider))
        mdl_key = cls._key(cls._index_models_key(provider))
        try:
            await redis.srem(acc_key, [username])
            # 移除该账号名下的所有模型级成员（成员形如 {username}\x1f{model}）
            members = await redis.smembers(mdl_key)
            prefix = f"{username}{cls._MODEL_SEP}"
            to_remove = []
            for m in members or []:
                if isinstance(m, bytes):
                    m = m.decode("utf-8", errors="replace")
                if str(m).startswith(prefix):
                    to_remove.append(str(m))
            if to_remove:
                await redis.srem(mdl_key, to_remove)
            await cls._index_prune_provider(provider)
        except Exception:
            pass

    @classmethod
    async def _index_unmark_model(cls, provider: str, username: str, model: str) -> None:
        redis = cls.redis()
        if redis is None or not provider or not username or not model:
            return
        try:
            await redis.srem(
                cls._key(cls._index_models_key(provider)),
                [f"{username}{cls._MODEL_SEP}{model}"],
            )
            await cls._index_prune_provider(provider)
        except Exception:
            pass

    @classmethod
    async def _index_clear_provider(cls, provider: str) -> None:
        """整渠道解冻：删除该渠道的账号 / 模型索引集合，并从渠道索引摘除。"""
        redis = cls.redis()
        if redis is None or not provider:
            return
        try:
            await redis.delete(cls._index_accounts_key(provider))
            await redis.delete(cls._index_models_key(provider))
            await redis.srem(cls._key(cls._INDEX_PROVIDERS), [provider])
        except Exception:
            pass

    @classmethod
    async def _index_prune_provider(cls, provider: str) -> None:
        """若渠道的账号索引与模型索引都空了，则把渠道从 providers 索引里摘除。"""
        redis = cls.redis()
        if redis is None or not provider:
            return
        try:
            acc = await redis.scard(cls._key(cls._index_accounts_key(provider)))
            mdl = await redis.scard(cls._key(cls._index_models_key(provider)))
            if not int(acc or 0) and not int(mdl or 0):
                await redis.srem(cls._key(cls._INDEX_PROVIDERS), [provider])
        except Exception:
            pass

    @classmethod
    async def count_events(cls, key: str, window_seconds: int) -> int | None:
        redis = cls.redis()
        if redis is None:
            return None
        redis_key = f"limit:events:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - window_seconds)
            return int(await redis.zcard(redis_key) or 0)
        except Exception:
            return None

    @classmethod
    async def record_event(cls, key: str, window_seconds: int, amount: int = 1) -> None:
        redis = cls.redis()
        if redis is None:
            return
        redis_key = f"limit:events:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now - window_seconds)
            await redis.zadd(redis_key, {f"{now}:{amount}:{random.random()}": now})
            await redis.expire(redis_key, window_seconds + 60)
        except Exception:
            pass

    @classmethod
    async def try_record_event(cls, key: str, window_seconds: int, limit: int, amount: int = 1) -> bool | None:
        if limit <= 0:
            await cls.record_event(key, window_seconds, amount)
            return True
        redis = cls.redis()
        if redis is None:
            return None
        redis_key = f"limit:events:{key}"
        now = time.time()
        member = f"{now}:{amount}:{random.random()}"
        script = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[2])
        local count = redis.call('ZCARD', KEYS[1])
        if count >= tonumber(ARGV[3]) then
            return 0
        end
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
        redis.call('EXPIRE', KEYS[1], ARGV[2] + 60)
        return 1
        """
        try:
            result = await redis.eval(script, keys=[redis_key], args=[now, window_seconds, limit, member])
            return bool(int(result))
        except Exception:
            return None

    # ==================== 固定窗口计数（触线即冻结用）====================
    # 语义：INCR/INCRBY 累加，首次写时 EXPIRE 设窗口长；TTL 即剩余窗口秒数（也就是触线后应冻结的时长）。
    # 与滑动窗口（events/*）分离：这套只服务"只要请求了就计数、越线冻结"的账号/API Key 限流。
    # 用 eval 保证 INCR+EXPIRE 原子，且显式走 cls._key() 补前缀（eval 不经 RedisJdbc.get_key）。

    @classmethod
    async def incr_window(cls, key: str, ttl_seconds: int, amount: int = 1) -> tuple[int, int] | None:
        """固定窗口累加。返回 (累加后的值, 当前 TTL 秒)；无 Redis 返回 None。

        首次创建 key 时设 EXPIRE=ttl_seconds；后续 INCR 不重置 TTL（窗口从首个请求算起）。
        """
        redis = cls.redis()
        if redis is None:
            return None
        redis_key = cls._key(f"limit:window:{key}")
        script = """
        local v = redis.call('INCRBY', KEYS[1], ARGV[1])
        if v == tonumber(ARGV[1]) then
            redis.call('EXPIRE', KEYS[1], ARGV[2])
        end
        local ttl = redis.call('TTL', KEYS[1])
        return {v, ttl}
        """
        try:
            result = await redis.eval(script, keys=[redis_key], args=[amount, ttl_seconds])
            count = int(result[0])
            ttl = int(result[1])
            # ttl 可能是 -1（无过期，异常）/-2（不存在）；兜底成窗口长，避免冻结时长算成负数
            if ttl < 0:
                ttl = ttl_seconds
            return count, ttl
        except Exception:
            return None

    @classmethod
    async def incrby_window(cls, key: str, amount: int, ttl_seconds: int) -> tuple[int, int] | None:
        """固定窗口按量累加（token 用）。语义同 incr_window。"""
        if amount <= 0:
            return None
        return await cls.incr_window(key, ttl_seconds, amount)

    @classmethod
    async def check_and_add_ip(
        cls,
        key: str,
        ip: str,
        limit: int,
        ttl_seconds: int,
    ) -> tuple[bool, int] | None:
        """原子判定并记录客户端 IP，返回 ``(是否放行, 当前去重 IP 数)``。

        IP 已存在时直接放行；新 IP 未达上限时加入集合，首个 IP 启动固定窗口；
        已达上限时拒绝且不修改集合。Redis 不可用或执行失败时返回 ``None``，
        由调用方按现有限流策略 fail-open。
        """
        redis = cls.redis()
        if redis is None or not ip or limit <= 0:
            return None
        redis_key = cls._key(f"limit:ipset:{key}")
        script = """
        if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
          return {1, redis.call('SCARD', KEYS[1])}
        end
        local count = redis.call('SCARD', KEYS[1])
        if count >= tonumber(ARGV[2]) then
          return {0, count}
        end
        redis.call('SADD', KEYS[1], ARGV[1])
        if count == 0 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
        return {1, redis.call('SCARD', KEYS[1])}
        """
        try:
            result = await redis.eval(
                script,
                keys=[redis_key],
                args=[ip, limit, max(1, ttl_seconds)],
            )
            return bool(int(result[0])), int(result[1])
        except Exception:
            return None

    @classmethod
    async def set_cardinality(cls, key: str) -> int | None:
        """读取固定窗口 IP 集合当前去重数（展示用）。"""
        redis = cls.redis()
        if redis is None:
            return None
        try:
            return int(await redis.scard(cls._key(f"limit:ipset:{key}")))
        except Exception:
            return None

    @classmethod
    async def record_tpm_and_freeze(
        cls,
        *,
        account_key: str,
        model_id: str,
        tokens: int,
        ttl_seconds: int,
        account_limit: int,
        model_limit: int,
        account_freeze_key: str,
        model_freeze_key: str,
    ) -> dict | None:
        """原子累加账号/模型 TPM，并在触线时同时写冻结 Key。"""
        if tokens <= 0:
            return None
        redis = cls.redis()
        if redis is None:
            return None
        account_window = cls._key(f"limit:window:{account_key}:tpm")
        model_window = cls._key(f"limit:window:model:{model_id}:tpm")
        account_freeze = cls._key(account_freeze_key)
        model_freeze = cls._key(model_freeze_key)
        script = """
        local amount = tonumber(ARGV[1])
        local ttl_seconds = tonumber(ARGV[2])
        local account_limit = tonumber(ARGV[3])
        local model_limit = tonumber(ARGV[4])
        local account_used = 0
        local model_used = 0
        local account_ttl = 0
        local model_ttl = 0

        if account_limit > 0 then
            account_used = redis.call('INCRBY', KEYS[1], amount)
            if account_used == amount then redis.call('EXPIRE', KEYS[1], ttl_seconds) end
            account_ttl = redis.call('TTL', KEYS[1])
            if account_used >= account_limit and account_ttl > 0 then
                redis.call('SET', KEYS[3], 'tpm', 'EX', account_ttl)
            end
        end
        if model_limit > 0 then
            model_used = redis.call('INCRBY', KEYS[2], amount)
            if model_used == amount then redis.call('EXPIRE', KEYS[2], ttl_seconds) end
            model_ttl = redis.call('TTL', KEYS[2])
            if model_used >= model_limit and model_ttl > 0 then
                redis.call('SET', KEYS[4], 'tpm', 'EX', model_ttl)
            end
        end
        return {account_used, account_ttl, model_used, model_ttl}
        """
        try:
            result = await redis.eval(
                script,
                keys=[account_window, model_window, account_freeze, model_freeze],
                args=[tokens, ttl_seconds, account_limit, model_limit],
            )
            return {
                "account_used": int(result[0] or 0),
                "account_ttl": max(0, int(result[1] or 0)),
                "model_used": int(result[2] or 0),
                "model_ttl": max(0, int(result[3] or 0)),
            }
        except Exception:
            return None

    @classmethod
    async def record_model_tpm_window(
        cls,
        model_id: str,
        tokens: int,
        ttl_seconds: int = 60,
    ) -> tuple[int, int] | None:
        """响应后原子累加模型固定窗口 token；TTL 从本窗口首次 token 回写开始计算。"""
        return await cls.incrby_window(f"model:{model_id}:tpm", tokens, ttl_seconds)

    @classmethod
    async def get_api_key_block_until(cls, subject: str, field: str) -> float | None:
        """从 Redis 固定窗口 TTL 推算 API Key block 到期时间戳。

        RateLimiter.acquire 对窗口做 incr 后，越线时在内存记 blocked_until=now+ttl 并广播。
        权威剩余时间就是窗口 key 的 TTL；窗口消失或无 TTL 时返回 None。
        """
        used, ttl = await cls.get_window(f"api-key:{subject}:{field}")
        if used <= 0 or ttl <= 0:
            return None
        return time.time() + float(ttl)

    @classmethod
    async def get_model_tpm_cooldown_until(cls, model_id: str) -> float | None:
        """从 Redis 模型 TPM 窗口的 TTL 推算冷却到期时间戳。"""
        if not model_id:
            return None
        used, ttl = await cls.get_window(f"model:{model_id}:tpm")
        if used <= 0 or ttl <= 0:
            return None
        return time.time() + float(ttl)

    @classmethod
    async def list_model_tpm_windows(cls) -> list[tuple[str, int, int]]:
        """启动恢复用：返回仍存活的模型 TPM 窗口 (model_id, used, ttl)。不进请求热路径。"""
        redis = cls.redis()
        if redis is None:
            return []
        prefix = cls._key("limit:window:model:")
        suffix = ":tpm"
        result: list[tuple[str, int, int]] = []
        try:
            async for raw_key in redis.scan_iter(match=f"{prefix}*{suffix}", count=200):
                if isinstance(raw_key, bytes):
                    raw_key = raw_key.decode("utf-8", errors="replace")
                full_key = str(raw_key)
                if not full_key.startswith(prefix) or not full_key.endswith(suffix):
                    continue
                model_id = full_key[len(prefix):-len(suffix)]
                if not model_id:
                    continue
                bare_key = cls._bare_key(full_key)
                raw_value = await redis.get(bare_key)
                ttl = await redis.ttl(full_key)
                used = int(raw_value or 0)
                remaining = int(ttl or 0)
                if used > 0 and remaining > 0:
                    result.append((model_id, used, remaining))
        except Exception:
            return []
        return result

    @classmethod
    async def get_window(cls, key: str) -> tuple[int, int]:
        """读固定窗口当前 (值, 剩余TTL秒)；不存在返回 (0, 0)。仅用于展示，不建议放热路径。"""
        redis = cls.redis()
        if redis is None:
            return 0, 0
        redis_key = cls._key(f"limit:window:{key}")
        try:
            raw = await redis.get(f"limit:window:{key}")
            value = int(raw) if raw is not None else 0
            ttl = await redis.ttl(redis_key)
            ttl = int(ttl) if isinstance(ttl, (int, float)) and ttl > 0 else 0
            return value, ttl
        except Exception:
            return 0, 0

    @classmethod
    async def get_account_rpd_used(cls, account_key: str) -> tuple[int, int]:
        """读账号 RPD 固定窗口当前 (已用值, 剩余TTL秒)；不存在返回 (0, 0)。仅用于展示。

        对应 reserve_account_limits 中的 ``limit:account-reserve:{account_key}:rpd``
        （INCR 累加，TTL=86400）。account_key 由 LimitSubject.account_key 生成。
        """
        redis = cls.redis()
        if redis is None:
            return 0, 0
        bare_key = f"limit:account-reserve:{account_key}:rpd"
        redis_key = cls._key(bare_key)
        try:
            raw = await redis.get(bare_key)
            value = int(raw) if raw is not None else 0
            ttl = await redis.ttl(redis_key)
            ttl = int(ttl) if isinstance(ttl, (int, float)) and ttl > 0 else 0
            return value, ttl
        except Exception:
            return 0, 0

    @classmethod
    async def batch_get_account_rpd(cls, account_keys: list[str]) -> dict[str, int]:
        """批量读多个账号的 RPD 已用值（给渠道详情页用）。

        单次 pipeline（GET），替代逐账号 Redis 调用。返回 {account_key: used}；
        不存在或 Redis 不可用的账号不进结果（调用方按缺省 0 处理）。
        """
        redis = cls.redis()
        if redis is None or not account_keys:
            return {}
        try:
            bare_keys = [f"limit:account-reserve:{k}:rpd" for k in account_keys]
            full_keys = [cls._key(k) for k in bare_keys]

            async with redis.pipeline(transaction=False) as pipe:
                for key in full_keys:
                    pipe.get(key)
                raw_results = await pipe.execute()

            result: dict[str, int] = {}
            for i, account_key in enumerate(account_keys):
                raw = raw_results[i] if i < len(raw_results) else None
                if raw is None:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    used = int(raw)
                except (TypeError, ValueError):
                    continue
                if used > 0:
                    result[account_key] = used
            return result
        except Exception:
            return {}

    @classmethod
    async def reserve_account_limits(
        cls,
        account_key: str,
        lease_id: str,
        *,
        freeze_key: str,
        model_freeze_key: str,
        account_model_freeze_key: str,
        concurrent_limit: int = 0,
        concurrency_ttl: int = 900,
        rpm_limit: int = 0,
        rpm_ttl: int = 60,
        rpd_limit: int = 0,
        rpd_ttl: int = 86400,
        rph_limit: int = 0,
        rph_ttl: int = 3600,
        model_id: str | None = None,
        token_amount: int = 0,
        tpm_limit: int = 0,
        tpm_ttl: int = 60,
        tph_limit: int = 0,
        tph_ttl: int = 3600,
        tpd_limit: int = 0,
        tpd_ttl: int = 86400,
        model_tpm_limit: int = 0,
        model_tpm_ttl: int = 60,
        allow_token_overflow: bool = True,
        window_ids: dict[str, str] | None = None,
    ) -> dict | None:
        """原子预占账号并发 + 请求数窗口 + token 窗口（可精确回滚的 per-reservation ledger）。

        请求数（rpm/rph/rpd）沿用「触线放行本次 + 冻结后续」语义。token（账号 tpm/tph/tpd 与
        共享 model tpm）按 ``token_amount`` 预占，成功后由 ``reconcile_account_reservation``
        用真实 usage 多退少补，失败由 ``rollback_account_reservation`` 精确回退。

        ``allow_token_overflow=False`` 时，若任一 token 维度 projected 触线，则在写入任何
        ledger / 并发 lease 之前返回 ``token_limit`` 拒绝，交由选路器换下一个候选。
        每个 metric 以 ``lease_id`` 作为 HASH member，重复用同一 lease_id 预占是 replace 而非
        累加，因此内层同账号重试不会重复计量。
        """
        redis = cls.redis()
        if redis is None:
            return None
        now = time.time()
        wids = window_ids or cls._reservation_window_ids(now)
        base = f"limit:account-reserve:{account_key}"
        concurrency_key = cls._key(f"{base}:concurrency")
        acc = cls._account_ledger_keys(account_key, wids)
        # 无 model 时用占位 key：Lua 侧按 amount/limit<=0 跳过，不会写入。
        model_tpm_key = (
            cls._model_ledger_key(model_id, wids["model_tpm"]) if model_id
            else cls._ledger_key("model", "model:__none__", "tpm", wids["model_tpm"])
        )
        reservation_index_key = cls._key(cls._RESERVATION_INDEX)
        reservation_meta_key = cls._key(cls._reservation_meta_key(lease_id))
        freeze_redis_key = cls._key(freeze_key)
        model_freeze_redis_key = cls._key(model_freeze_key)
        account_model_freeze_redis_key = cls._key(account_model_freeze_key)
        token_amount = max(0, int(token_amount or 0))
        script = """
        local freeze_ttl = redis.call('TTL', KEYS[4])
        if freeze_ttl > 0 then
            return {0, 'frozen', freeze_ttl, 0, 0}
        end
        local model_freeze_ttl = redis.call('TTL', KEYS[5])
        if model_freeze_ttl > 0 then
            return {0, 'model_frozen', model_freeze_ttl, 0, 0}
        end
        local account_model_freeze_ttl = redis.call('TTL', KEYS[6])
        if account_model_freeze_ttl > 0 then
            return {0, 'account_model_frozen', account_model_freeze_ttl, 0, 0}
        end

        local concurrent_limit = tonumber(ARGV[2])
        if concurrent_limit > 0 then
            redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
            local concurrent_used = redis.call('ZCARD', KEYS[1])
            if concurrent_used >= concurrent_limit then
                local earliest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
                local retry_after = 1
                if earliest[2] then
                    retry_after = math.max(1, math.ceil(tonumber(earliest[2]) - tonumber(ARGV[1])))
                end
                return {0, 'concurrent', retry_after, 0, 0, concurrent_used, 0}
            end
        end

        local rid = ARGV[4]
        local grace = tonumber(ARGV[26])

        -- projected：把本 reservation 的旧值换成新 amount 后该窗口的总量（replace 语义）
        local function projected(key, amount)
            local old = redis.call('HGET', key, rid)
            local old_n = 0
            if old ~= false then old_n = tonumber(old) or 0 end
            local total = tonumber(redis.call('HGET', key, '__total') or '0') or 0
            local next_total = total - old_n + amount
            if next_total < 0 then next_total = 0 end
            return next_total
        end

        local function write(key, amount, next_total, ttl)
            redis.call('HSET', key, rid, tostring(amount), rid .. ':state', 'reserved', '__total', tostring(next_total))
            if redis.call('TTL', key) < 0 then
                redis.call('EXPIRE', key, ttl + grace)
            end
        end

        local token_amount = tonumber(ARGV[14])
        local allow_overflow = tonumber(ARGV[25]) == 1
        -- token ledger: {key, limit, ttl}
        local token_specs = {
            {KEYS[8],  tonumber(ARGV[15]), tonumber(ARGV[16])},
            {KEYS[9],  tonumber(ARGV[17]), tonumber(ARGV[18])},
            {KEYS[10], tonumber(ARGV[19]), tonumber(ARGV[20])},
            {KEYS[11], tonumber(ARGV[21]), tonumber(ARGV[22])},
        }

        -- 触线拒绝必须发生在任何写入之前，保证被拒的候选不留下任何计量痕迹。
        if token_amount > 0 and not allow_overflow then
            for i = 1, #token_specs do
                local key, limit, ttl = token_specs[i][1], token_specs[i][2], token_specs[i][3]
                if limit > 0 then
                    if projected(key, token_amount) >= limit then
                        local remaining = redis.call('TTL', key)
                        if remaining < 0 then remaining = ttl end
                        return {0, 'token_limit', remaining, 0, 0, 0, 0, 0}
                    end
                end
            end
        end

        local rpm = 0
        local rpd = 0
        local rph = 0
        local freeze_reason = ''
        local freeze_for = 0
        local rpm_limit = tonumber(ARGV[5])
        local rpd_limit = tonumber(ARGV[7])
        local rph_limit = tonumber(ARGV[9])

        if rpm_limit > 0 then
            rpm = projected(KEYS[2], 1)
            write(KEYS[2], 1, rpm, tonumber(ARGV[6]))
            if rpm >= rpm_limit then
                freeze_reason = 'rpm'
                freeze_for = redis.call('TTL', KEYS[2])
            end
        end
        if rpd_limit > 0 then
            rpd = projected(KEYS[3], 1)
            write(KEYS[3], 1, rpd, tonumber(ARGV[8]))
            if rpd >= rpd_limit then
                local ttl = redis.call('TTL', KEYS[3])
                if freeze_for == 0 or ttl > freeze_for then
                    freeze_reason = 'rpd'
                    freeze_for = ttl
                end
            end
        end
        if rph_limit > 0 then
            rph = projected(KEYS[7], 1)
            write(KEYS[7], 1, rph, tonumber(ARGV[10]))
            if rph >= rph_limit then
                local ttl = redis.call('TTL', KEYS[7])
                if freeze_for == 0 or ttl > freeze_for then
                    freeze_reason = 'rph'
                    freeze_for = ttl
                end
            end
        end

        -- token 预占：allow_overflow 时触线仍写入并放行，靠 freeze 挡住后续请求。
        local token_names = {'tpm', 'tph', 'tpd', 'model_tpm'}
        local token_used = {0, 0, 0, 0}
        local model_freeze_for = 0
        if token_amount > 0 then
            for i = 1, #token_specs do
                local key, limit, ttl = token_specs[i][1], token_specs[i][2], token_specs[i][3]
                if limit > 0 then
                    local next_total = projected(key, token_amount)
                    write(key, token_amount, next_total, ttl)
                    token_used[i] = next_total
                    if next_total >= limit then
                        local remaining = redis.call('TTL', key)
                        if remaining > 0 then
                            if i == 4 then
                                -- 模型级共享窗口：冻结模型而非账号
                                redis.call('SET', KEYS[5], 'reservation:model_tpm', 'EX', remaining)
                                model_freeze_for = remaining
                            elseif freeze_for == 0 or remaining > freeze_for then
                                freeze_reason = token_names[i]
                                freeze_for = remaining
                            end
                        end
                    end
                end
            end
        end

        if freeze_for > 0 then
            redis.call('SET', KEYS[4], 'reservation:' .. freeze_reason, 'EX', freeze_for)
        end

        -- 可恢复索引：只在预占真正写入后登记；meta 保存精确 rollback 所需上下文。
        -- TTL 覆盖最长（日）窗口 + grace，正常 reconcile/rollback 会主动摘除。
        redis.call('SADD', KEYS[12], rid)
        redis.call('HSET', KEYS[13],
            'account_key', ARGV[27],
            'model_id', ARGV[28],
            'window_ids', ARGV[29],
            'boot_id', ARGV[30])
        redis.call('EXPIRE', KEYS[13], tonumber(ARGV[31]))

        local lease_expires_at = 0
        if concurrent_limit > 0 then
            lease_expires_at = tonumber(ARGV[1]) + tonumber(ARGV[3])
            redis.call('ZADD', KEYS[1], lease_expires_at, rid)
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]) + 60)
        end
        return {1, freeze_reason, freeze_for, rpm, rpd, 0, lease_expires_at, rph,
                token_used[1], token_used[2], token_used[3], token_used[4], model_freeze_for}
        """
        try:
            result = await redis.eval(
                script,
                keys=[
                    concurrency_key,
                    acc["rpm"],
                    acc["rpd"],
                    freeze_redis_key,
                    model_freeze_redis_key,
                    account_model_freeze_redis_key,
                    acc["rph"],
                    acc["tpm"],
                    acc["tph"],
                    acc["tpd"],
                    model_tpm_key,
                    reservation_index_key,
                    reservation_meta_key,
                ],
                args=[
                    now,
                    concurrent_limit,
                    concurrency_ttl,
                    lease_id,
                    rpm_limit,
                    rpm_ttl,
                    rpd_limit,
                    rpd_ttl,
                    rph_limit,
                    rph_ttl,
                    0,
                    0,
                    0,
                    token_amount,
                    tpm_limit,
                    tpm_ttl,
                    tph_limit,
                    tph_ttl,
                    tpd_limit,
                    tpd_ttl,
                    model_tpm_limit if model_id else 0,
                    model_tpm_ttl,
                    0,
                    0,
                    1 if allow_token_overflow else 0,
                    cls._RESERVATION_GRACE,
                    account_key,
                    model_id or "",
                    json.dumps(wids, separators=(",", ":")),
                    lease_id.split(":", 2)[1] if lease_id.startswith("lease:") and lease_id.count(":") >= 2 else "",
                    max(rpd_ttl, tpd_ttl, 86400) + cls._RESERVATION_GRACE,
                ],
            )
            reason = str(result[1] or "")
            raw_ttl = max(0, int(result[2] or 0))
            transient = reason in ("concurrent", "token_limit")
            return {
                "allowed": bool(int(result[0])),
                "reason": reason,
                "ttl": 0 if transient else raw_ttl,
                "retry_after": raw_ttl if transient else 0,
                "rpm": int(result[3] or 0),
                "rpd": int(result[4] or 0),
                "rph": int(result[7] or 0) if len(result) > 7 else 0,
                "concurrent_used": int(result[5] or 0) if len(result) > 5 else 0,
                "lease_expires_at": float(result[6] or 0) if len(result) > 6 else 0,
                "tpm": int(result[8] or 0) if len(result) > 8 else 0,
                "tph": int(result[9] or 0) if len(result) > 9 else 0,
                "tpd": int(result[10] or 0) if len(result) > 10 else 0,
                "model_tpm": int(result[11] or 0) if len(result) > 11 else 0,
                "model_tpm_freeze_ttl": max(0, int(result[12] or 0)) if len(result) > 12 else 0,
                "window_ids": wids,
            }
        except Exception:
            return None

    # ==================== 预占式计量 ledger（可精确回滚）====================
    # 每个 (metric, 固定窗口) 一个 HASH：__total=窗口总量，{reservation_id}=本次预占量，
    # {reservation_id}:state=reserved/committed。回滚/补差按 reservation_id 精确匹配 member，
    # 不裸 DECR：不会误删并发请求、不跨窗口、不为负。key 含 window_id，窗口切换天然隔离，
    # 窗口整体 TTL 过期后回滚/补差自然是 no-op。
    # reserve_account_limits 负责 HSET 写入；reconcile 成功补差；rollback 失败回退。

    _RESERVATION_PREFIX = "limit:reservation:v1"
    _RESERVATION_INDEX = "limit:reservation:v1:active"
    _PROCESS_HEARTBEAT_PREFIX = "limit:process-heartbeat"
    _RESERVATION_GRACE = 60  # 窗口边界附近结算竞态的缓冲秒

    @classmethod
    def _reservation_meta_key(cls, reservation_id: str) -> str:
        return f"{cls._RESERVATION_PREFIX}:meta:{reservation_id}"

    @classmethod
    def _process_heartbeat_key(cls, boot_id: str) -> str:
        return f"{cls._PROCESS_HEARTBEAT_PREFIX}:{boot_id}"

    @classmethod
    def _day_window_id_ttl(cls, now: float) -> tuple[str, int]:
        """自然日窗口 id（本地时区当日）+ 到本地次日 00:00 的秒数。

        与 limits/manager._seconds_until_day_end 对齐，使 RPD 与 TPD 共用同一自然日边界。
        """
        dt = datetime.fromtimestamp(now)
        nxt = (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.strftime("%Y-%m-%d"), max(1, int((nxt - dt).total_seconds()))

    @classmethod
    def _reservation_window_ids(cls, now: float) -> dict[str, str]:
        """一次性算出所有 metric 的固定窗口 id（Manager 复用以保证 reserve/补差/回滚同窗口）。"""
        day_id, _ = cls._day_window_id_ttl(now)
        return {
            "rpm": str(int(now // 60)),
            "rph": str(int(now // 3600)),
            "rpd": day_id,
            "tpm": str(int(now // 60)),
            "tph": str(int(now // 3600)),
            "tpd": day_id,
            "model_tpm": str(int(now // 60)),
        }

    @classmethod
    def _ledger_key(cls, scope: str, subject_key: str, metric: str, window_id: str) -> str:
        """构造 ledger HASH key。scope=account|model；subject_key 形如 account:p:u 或 model:m。"""
        return cls._key(f"{cls._RESERVATION_PREFIX}:{scope}:{subject_key}:{metric}:{window_id}")

    @classmethod
    def _account_ledger_keys(cls, account_key: str, window_ids: dict[str, str]) -> dict[str, str]:
        return {
            m: cls._ledger_key("account", account_key, m, window_ids[m])
            for m in ("rpm", "rph", "rpd", "tpm", "tph", "tpd")
        }

    @classmethod
    def _model_ledger_key(cls, model_id: str, window_id: str) -> str:
        return cls._ledger_key("model", f"model:{model_id}", "tpm", window_id)

    @classmethod
    async def rollback_account_reservation(
        cls,
        account_key: str,
        model_id: str | None,
        reservation_id: str,
        *,
        window_ids: dict[str, str] | None = None,
    ) -> dict | None:
        """失败回退：按 reservation_id 精确删除本次在所有 ledger 里的预占量。

        每个 ledger：``HGET reservation_id`` 得本次预占量 old，``__total=max(0,total-old)``，
        ``HDEL reservation_id`` 与 ``reservation_id:state``。member 不存在 / key 已过期 → no-op。
        绝不裸 DECR，不会误删并发请求、不跨窗口、不为负。RPM/RPH/RPD 与账号/模型 token 一并退。
        """
        redis = cls.redis()
        if redis is None or not account_key or not reservation_id:
            return None
        now = time.time()
        wids = window_ids or cls._reservation_window_ids(now)
        acc = cls._account_ledger_keys(account_key, wids)
        keys = [acc["rpm"], acc["rph"], acc["rpd"], acc["tpm"], acc["tph"], acc["tpd"]]
        if model_id:
            keys.append(cls._model_ledger_key(model_id, wids["model_tpm"]))
        ledger_count = len(keys)
        keys.extend([
            cls._key(cls._RESERVATION_INDEX),
            cls._key(cls._reservation_meta_key(reservation_id)),
        ])
        script = """
        local removed = 0
        local ledger_count = tonumber(ARGV[2])
        for i = 1, ledger_count do
            local old = redis.call('HGET', KEYS[i], ARGV[1])
            if old ~= false then
                local total = tonumber(redis.call('HGET', KEYS[i], '__total') or '0') or 0
                local new_total = total - tonumber(old)
                if new_total < 0 then new_total = 0 end
                redis.call('HSET', KEYS[i], '__total', tostring(new_total))
                redis.call('HDEL', KEYS[i], ARGV[1], ARGV[1] .. ':state')
                removed = removed + 1
            end
        end
        redis.call('SREM', KEYS[ledger_count + 1], ARGV[1])
        redis.call('DEL', KEYS[ledger_count + 2])
        return removed
        """
        try:
            result = await redis.eval(script, keys=keys, args=[reservation_id, ledger_count])
            return {"removed": int(result or 0)}
        except Exception:
            return None

    @classmethod
    async def finalize_reservation(cls, reservation_id: str) -> None:
        """reconcile/rollback 终态后摘除索引与 meta（幂等）。崩溃恢复据此判断是否已结算。"""
        redis = cls.redis()
        if redis is None or not reservation_id:
            return
        index_key = cls._key(cls._RESERVATION_INDEX)
        meta_key = cls._key(cls._reservation_meta_key(reservation_id))
        try:
            await redis.srem(index_key, [reservation_id])
            await redis.delete(cls._bare_key(meta_key))
        except Exception:
            pass

    @classmethod
    async def set_process_heartbeat(cls, boot_id: str, *, ttl_seconds: int) -> bool:
        """写/续期进程心跳；存活进程的心跳 key 存在即视为活跃，孤儿恢复不回滚其预占。"""
        redis = cls.redis()
        if redis is None or not boot_id or ttl_seconds <= 0:
            return False
        try:
            await redis.set(cls._process_heartbeat_key(boot_id), "1", ex=ttl_seconds)
            return True
        except Exception:
            return False

    @classmethod
    async def process_alive(cls, boot_id: str) -> bool:
        """心跳 key 是否存在；None（Redis 不可用）视为存活，避免误回滚。"""
        redis = cls.redis()
        if redis is None or not boot_id:
            return True
        try:
            return bool(await redis.exists(cls._key(cls._process_heartbeat_key(boot_id))))
        except Exception:
            return True

    @classmethod
    async def reap_orphan_reservations(
        cls,
        *,
        current_boot_id: str,
        batch_size: int = 200,
    ) -> int:
        """扫描预占索引，回滚心跳失效的 owner 的全部预占。幂等，按 reservation_id 精确回滚。

        ``current_boot_id`` 只用于避免误回滚当前进程；其它 owner 必须按 Redis 心跳判活。
        """
        redis = cls.redis()
        if redis is None:
            return 0
        index_key = cls._key(cls._RESERVATION_INDEX)
        try:
            members = await redis.smembers(index_key)
        except Exception:
            return 0
        if not members:
            return 0
        decoded: list[str] = []
        for m in members:
            if isinstance(m, bytes):
                try:
                    m = m.decode("utf-8")
                except Exception:
                    continue
            if not isinstance(m, str) or not m:
                continue
            if not m.startswith("lease:") or m.count(":") < 2:
                # 旧版本/测试 reservation 没有可判活 owner；不冒险回滚，等自然 TTL。
                continue
            decoded.append(m)
        reaped = 0
        for start in range(0, len(decoded), batch_size):
            batch = decoded[start:start + batch_size]
            metas: list[dict] = []
            try:
                for rid in batch:
                    metas.append(await redis.hgetall(cls._bare_key(cls._key(cls._reservation_meta_key(rid)))))
            except Exception:
                metas = [{}] * len(batch)
            for reservation_id, meta in zip(batch, metas):
                if not isinstance(meta, dict) or not meta:
                    continue
                boot_id = _decode_meta_field(meta, "boot_id")
                if not boot_id or boot_id == current_boot_id:
                    continue
                if await cls.process_alive(boot_id):
                    continue
                account_key = _decode_meta_field(meta, "account_key")
                model_id = _decode_meta_field(meta, "model_id")
                window_ids_raw = _decode_meta_field(meta, "window_ids", "{}")
                try:
                    wids = json.loads(window_ids_raw) if window_ids_raw else {}
                except Exception:
                    wids = {}
                result = await cls.rollback_account_reservation(
                    account_key, model_id or None, reservation_id, window_ids=wids or None,
                )
                if result is None:
                    continue  # Redis 异常时保留索引/meta，下轮重试；不能把未回滚的孤儿标已处理。
                await cls.finalize_reservation(reservation_id)
                reaped += 1
        return reaped

    @classmethod
    async def reconcile_account_reservation(
        cls,
        account_key: str,
        model_id: str | None,
        reservation_id: str,
        actual_tokens: int,
        *,
        account_tpm_limit: int = 0,
        account_tph_limit: int = 0,
        account_tpd_limit: int = 0,
        model_tpm_limit: int = 0,
        tpm_ttl: int = 60,
        tph_ttl: int = 3600,
        tpd_ttl: int = 86400,
        model_tpm_ttl: int = 60,
        window_ids: dict[str, str] | None = None,
        account_freeze_key: str | None = None,
        model_freeze_key: str | None = None,
    ) -> dict | None:
        """成功补差：把 4 个 token ledger 里本次预占量替换成真实 actual_tokens（多退少补）。

        每个 token ledger：``HGET reservation_id`` 得 old，``__total=max(0,total-old+actual)``，
        ``HSET reservation_id=actual; reservation_id:state=committed; __total``。
        重复同 actual → delta=0 幂等；补差后若结算总量达到 limit，写 ``reservation:<metric>`` 冻结
        （已服务不能拒绝，但要挡住后续请求）；账号级 metric 冻结账号 cooldown，model_tpm 冻结模型。
        旧 window key 过期 → no-op，不在新窗口补差。request 计数（rpm/rph/rpd）不在此调整。
        """
        if actual_tokens < 0:
            actual_tokens = 0
        redis = cls.redis()
        if redis is None or not account_key or not reservation_id:
            return None
        now = time.time()
        wids = window_ids or cls._reservation_window_ids(now)
        acc = cls._account_ledger_keys(account_key, wids)
        keys = [acc["tpm"], acc["tph"], acc["tpd"]]
        limits = [int(account_tpm_limit or 0), int(account_tph_limit or 0), int(account_tpd_limit or 0)]
        if model_id:
            keys.append(cls._model_ledger_key(model_id, wids["model_tpm"]))
            limits.append(int(model_tpm_limit or 0))
        account_freeze_redis = cls._key(account_freeze_key) if account_freeze_key else ""
        model_freeze_redis = cls._key(model_freeze_key) if model_freeze_key else ""
        metric_count = len(keys)
        # Lua 所访问的所有 Redis key 必须通过 KEYS 声明；空字符串用不存在的占位 key，
        # 脚本由对应 limit>0 / key 配置控制，不会实际写占位 key。
        keys.extend([
            account_freeze_redis or cls._key("limit:reservation:v1:__no_account_freeze__"),
            model_freeze_redis or cls._key("limit:reservation:v1:__no_model_freeze__"),
            cls._key(cls._RESERVATION_INDEX),
            cls._key(cls._reservation_meta_key(reservation_id)),
        ])
        script = """
        local rid = ARGV[1]
        local actual = tonumber(ARGV[2])
        local metric_count = tonumber(ARGV[3])
        local account_freeze_enabled = tonumber(ARGV[4]) == 1
        local model_freeze_enabled = tonumber(ARGV[5]) == 1
        local out = {}
        local account_freeze_ttl = 0
        local model_freeze_ttl = 0
        for i = 1, metric_count do
            if redis.call('EXISTS', KEYS[i]) == 0 then
                out[#out+1] = -1
            else
                local old = redis.call('HGET', KEYS[i], rid)
                local old_n = 0
                if old ~= false then old_n = tonumber(old) or 0 end
                local total = tonumber(redis.call('HGET', KEYS[i], '__total') or '0') or 0
                local new_total = total - old_n + actual
                if new_total < 0 then new_total = 0 end
                redis.call('HSET', KEYS[i], rid, tostring(actual), rid .. ':state', 'committed', '__total', tostring(new_total))
                out[#out+1] = new_total
                local limit = tonumber(ARGV[5 + i])
                if limit > 0 and new_total >= limit then
                    local remaining = redis.call('TTL', KEYS[i])
                    if remaining > 0 then
                        if i == 4 then
                            if model_freeze_enabled then
                                redis.call('SET', KEYS[metric_count + 2], 'reservation:model_tpm', 'EX', remaining)
                            end
                            model_freeze_ttl = remaining
                        elseif remaining > account_freeze_ttl then
                            account_freeze_ttl = remaining
                        end
                    end
                end
            end
        end
        if account_freeze_ttl > 0 and account_freeze_enabled then
            redis.call('SET', KEYS[metric_count + 1], 'reservation:tpm', 'EX', account_freeze_ttl)
        end
        -- 末两位返回 account/model 的冻结 TTL（0 表示未冻结），供 Manager 同步内存镜像 + 广播。
        local packed = {}
        for i = 1, #out do packed[#packed+1] = out[i] end
        packed[#packed+1] = account_freeze_ttl
        packed[#packed+1] = model_freeze_ttl
        redis.call('SREM', KEYS[metric_count + 3], rid)
        redis.call('DEL', KEYS[metric_count + 4])
        return packed
        """
        try:
            result = await redis.eval(
                script,
                keys=keys,
                args=[reservation_id, actual_tokens, metric_count,
                      1 if account_freeze_redis else 0,
                      1 if model_freeze_redis else 0,
                      *limits],
            )
            used_raw = list(result or [])
            # 末两位是冻结 TTL，剥离后不进 used。
            account_freeze_ttl_return = 0
            model_freeze_ttl_return = 0
            if len(used_raw) >= 2:
                account_freeze_ttl_return = max(0, int(used_raw[-2] or 0))
                model_freeze_ttl_return = max(0, int(used_raw[-1] or 0))
                used_raw = used_raw[:-2]
            order = ["account_tpm", "account_tph", "account_tpd"] + (["model_tpm"] if model_id else [])
            used: dict[str, int | None] = {}
            for i, name in enumerate(order):
                raw = used_raw[i] if i < len(used_raw) else None
                if raw is None:
                    used[name] = None
                else:
                    val = int(raw)
                    used[name] = None if val < 0 else val
            return {
                "used": used,
                "account_freeze_ttl": account_freeze_ttl_return,
                "model_freeze_ttl": model_freeze_ttl_return,
            }
        except Exception:
            return None

    @classmethod
    async def batch_get_account_window_usage(
        cls,
        account_key: str,
        *,
        metrics: tuple[str, ...] = ("rpm", "rph", "rpd", "tpm", "tph", "tpd"),
    ) -> dict[str, int]:
        """批量读账号各 ledger 当前窗口 __total（admin 面板权威用量，取代本地 _requests）。"""
        redis = cls.redis()
        if redis is None or not account_key:
            return {}
        now = time.time()
        wids = cls._reservation_window_ids(now)
        acc = cls._account_ledger_keys(account_key, wids)
        wanted = [m for m in metrics if m in acc]
        if not wanted:
            return {}
        keys = [acc[m] for m in wanted]
        try:
            async with redis.pipeline(transaction=False) as pipe:
                for k in keys:
                    pipe.hget(k, "__total")
                raw_results = await pipe.execute()
            result: dict[str, int] = {}
            for i, m in enumerate(wanted):
                raw = raw_results[i] if i < len(raw_results) else None
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    result[m] = int(raw) if raw is not None else 0
                except (TypeError, ValueError):
                    result[m] = 0
            return result
        except Exception:
            return {}

    @classmethod
    async def renew_account_reservation(
        cls,
        account_key: str,
        lease_id: str,
        ttl_seconds: int,
    ) -> float | None:
        redis = cls.redis()
        if redis is None or not lease_id or ttl_seconds <= 0:
            return None
        key = cls._key(f"limit:account-reserve:{account_key}:concurrency")
        now = time.time()
        script = """
        if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then
            return 0
        end
        local expires_at = tonumber(ARGV[2]) + tonumber(ARGV[3])
        redis.call('ZADD', KEYS[1], expires_at, ARGV[1])
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]) + 60)
        return expires_at
        """
        try:
            result = await redis.eval(script, keys=[key], args=[lease_id, now, ttl_seconds])
            expires_at = float(result or 0)
            return expires_at if expires_at > 0 else None
        except Exception:
            return None

    @classmethod
    async def release_account_reservation(cls, account_key: str, lease_id: str) -> bool | None:
        redis = cls.redis()
        if redis is None or not lease_id:
            return None
        key = cls._key(f"limit:account-reserve:{account_key}:concurrency")
        try:
            return bool(await redis.zrem(key, lease_id))
        except Exception:
            return None

    @classmethod
    async def acquire_concurrency(cls, key: str, limit: int, lease_id: str, ttl_seconds: int = 3600) -> bool | None:
        if limit <= 0:
            return True
        redis = cls.redis()
        if redis is None:
            return None
        redis_key = f"limit:concurrency:{key}"
        now = time.time()
        script = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
        local count = redis.call('ZCARD', KEYS[1])
        if count >= tonumber(ARGV[2]) then
            return 0
        end
        redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
        redis.call('EXPIRE', KEYS[1], ARGV[5])
        return 1
        """
        try:
            result = await redis.eval(
                script,
                keys=[redis_key],
                args=[now, limit, now + ttl_seconds, lease_id, ttl_seconds + 60],
            )
            return bool(int(result))
        except Exception:
            return None

    @classmethod
    async def release_concurrency(cls, key: str, lease_id: str) -> None:
        redis = cls.redis()
        if redis is None:
            return
        try:
            await redis.zrem(f"limit:concurrency:{key}", lease_id)
        except Exception:
            pass

    @classmethod
    async def concurrency_used(cls, key: str) -> int | None:
        redis = cls.redis()
        if redis is None:
            return None
        redis_key = f"limit:concurrency:{key}"
        now = time.time()
        try:
            await redis.zremrangebyscore(redis_key, 0, now)
            return int(await redis.zcard(redis_key) or 0)
        except Exception:
            return None

    @classmethod
    async def set_account_cooldown(cls, provider: str, username: str, seconds: int | None, reason: str = "") -> None:
        """记账号冻结到 Redis 兜底；seconds>0 临时带 TTL，seconds=None 永久无 TTL。"""
        redis = cls.redis()
        if redis is None:
            return
        if seconds is not None and seconds <= 0:
            return
        try:
            if seconds is None:
                # 永久冻结：无 TTL key，value 存原因（空串也合法）。
                await redis.set(
                    f"limit:cooldown:{provider}:{username}",
                    reason or "",
                )
            else:
                await redis.set(
                    f"limit:cooldown:{provider}:{username}",
                    reason or "",
                    ex=seconds,
                )
            await cls._index_mark(provider, username, None)
        except Exception:
            pass

    @classmethod
    async def get_account_cooldown_info(cls, provider: str, username: str) -> dict | None:
        """读取账号冷却信息；TTL 是权威剩余时间，value 为原因；兼容旧版时间戳/JSON 值。

        永久冻结（no TTL）返回 until=None, permanent=True。
        """
        redis = cls.redis()
        if redis is None:
            return None
        key = f"limit:cooldown:{provider}:{username}"
        try:
            raw = await redis.get(key)
            ttl = await redis.ttl(cls._key(key))
            if ttl == -1:
                # 永久冻结：key 存在但无 TTL
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                value = str(raw or "")
                reason = value
                # 兼容旧格式
                if value.strip().startswith("{"):
                    try:
                        data = json.loads(value)
                        reason = str(data.get("reason") or "")
                    except Exception:
                        reason = ""
                else:
                    try:
                        float(value)
                        reason = ""
                    except (TypeError, ValueError):
                        pass
                return {"until": None, "remaining": None, "reason": reason, "permanent": True}
            remaining = int(ttl) if isinstance(ttl, (int, float)) and ttl > 0 else 0
            if remaining <= 0:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            value = str(raw or "")
            reason = value
            # 兼容旧格式
            if value.strip().startswith("{"):
                try:
                    data = json.loads(value)
                    reason = str(data.get("reason") or "")
                except Exception:
                    reason = ""
            else:
                try:
                    float(value)
                    reason = ""
                except (TypeError, ValueError):
                    pass
            return {"until": time.time() + remaining, "remaining": remaining, "reason": reason, "permanent": False}
        except Exception:
            return None

    @classmethod
    async def get_account_cooldown_until(cls, provider: str, username: str) -> float | None:
        """读取账号冷却到期时间戳；不在冷却中返回 None；永久冻结返回 None。"""
        info = await cls.get_account_cooldown_info(provider, username)
        if info is None:
            return None
        u = info.get("until")
        if u is None:
            return None  # permanent freeze
        return float(u)

    @classmethod
    async def clear_account_cooldown(cls, provider: str, username: str) -> None:
        """清除账号冷却状态（解冻）——账号级 + 该账号所有模型级冷却键一并清除。"""
        redis = cls.redis()
        if redis is None:
            return
        try:
            await redis.delete(f"limit:cooldown:{provider}:{username}")
        except Exception:
            pass
        # 一并清除该账号的模型级冷却键 limit:cooldown:{provider}:{username}:model:*
        pattern = cls._key(f"limit:cooldown:{provider}:{username}:model:*")
        try:
            bare_keys: list[str] = []
            async for key in redis.scan_iter(match=pattern, count=200):
                if isinstance(key, bytes):
                    key = key.decode("utf-8", errors="replace")
                bare_keys.append(cls._bare_key(str(key)))
            if bare_keys:
                try:
                    await redis.delete(*bare_keys)
                except Exception:
                    for k in bare_keys:
                        try:
                            await redis.delete(k)
                        except Exception:
                            pass
        except Exception as exc:
            logger.warning(f"[RedisLimitBackend] 清除 {provider}:{username} 模型级冷却失败: {exc}")
        # 同步摘除该账号的冻结索引（账号级 + 名下所有模型级）
        await cls._index_unmark_account(provider, username)

    @classmethod
    async def clear_all_provider_cooldowns(cls, provider: str) -> int:
        """批量清除某个渠道的所有冷却键（账号级 + 模型级）。

        Redis key 命名：
        - 账号级：limit:cooldown:{provider}:{username}
        - 模型级：limit:cooldown:{provider}:{username}:model:{model}

        使用 SCAN + DEL 避免阻塞 Redis。
        """
        redis = cls.redis()
        if redis is None:
            return 0
        deleted = 0
        pattern = cls._key(f"limit:cooldown:{provider}:*")
        try:
            keys: list[str] = []
            async for key in redis.scan_iter(match=pattern, count=200):
                if isinstance(key, bytes):
                    key = key.decode("utf-8", errors="replace")
                keys.append(str(key))
            if keys:
                bare_keys = [cls._bare_key(k) for k in keys]
                try:
                    await redis.delete(*bare_keys)
                    deleted = len(bare_keys)
                except Exception:
                    # DELETE 可能因 key 已被 TTL 清除而部分失败，逐个再补一次
                    for k in bare_keys:
                        try:
                            await redis.delete(k)
                            deleted += 1
                        except Exception:
                            pass
        except Exception as exc:
            logger.warning(f"[RedisLimitBackend] 批量清除 {provider} 冷却失败: {exc}")
        # 整渠道解冻：清空该渠道的冻结索引集合
        await cls._index_clear_provider(provider)
        return deleted

    @classmethod
    async def get_account_cooldown_remaining(cls, provider: str, username: str) -> int:
        """直接以 Redis TTL 作为剩余冷却秒数；非冷却或无 Redis 返回 0；永久冻结返回 -1。"""
        info = await cls.get_account_cooldown_info(provider, username)
        if not info:
            return 0
        if info.get("permanent"):
            return -1
        return int(info["remaining"])

    @classmethod
    async def set_account_model_cooldown(cls, provider: str, username: str, model: str, seconds: int | None, reason: str = "") -> None:
        """记账号+模型冻结到 Redis 兜底；seconds>0 临时带 TTL，seconds=None 永久无 TTL。"""
        redis = cls.redis()
        if redis is None:
            return
        if seconds is not None and seconds <= 0:
            return
        try:
            if seconds is None:
                await redis.set(
                    f"limit:cooldown:{provider}:{username}:model:{model}",
                    reason or "",
                )
            else:
                await redis.set(
                    f"limit:cooldown:{provider}:{username}:model:{model}",
                    reason or "",
                    ex=seconds,
                )
            await cls._index_mark(provider, username, model)
        except Exception:
            pass

    @classmethod
    async def get_account_model_cooldown_info(cls, provider: str, username: str, model: str) -> dict | None:
        """读取账号+模型冷却信息；TTL 是权威剩余时间，value 为原因。永久冻结 until=None。"""
        redis = cls.redis()
        if redis is None:
            return None
        key = f"limit:cooldown:{provider}:{username}:model:{model}"
        try:
            raw = await redis.get(key)
            ttl = await redis.ttl(cls._key(key))
            if ttl == -1:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                return {"until": None, "remaining": None, "reason": str(raw or ""), "permanent": True}
            remaining = int(ttl) if isinstance(ttl, (int, float)) and ttl > 0 else 0
            if remaining <= 0:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return {"until": time.time() + remaining, "remaining": remaining, "reason": str(raw or ""), "permanent": False}
        except Exception:
            return None

    @classmethod
    async def clear_account_model_cooldown(cls, provider: str, username: str, model: str) -> None:
        """清除账号+模型冷却状态（解冻）。"""
        redis = cls.redis()
        if redis is None:
            return
        try:
            await redis.delete(f"limit:cooldown:{provider}:{username}:model:{model}")
        except Exception:
            pass
        await cls._index_unmark_model(provider, username, model)

    @classmethod
    async def get_account_model_cooldown_remaining(cls, provider: str, username: str, model: str) -> int:
        """直接以 Redis TTL 作为账号+模型剩余冷却秒数；非冷却或无 Redis 返回 0。"""
        info = await cls.get_account_model_cooldown_info(provider, username, model)
        return int(info["remaining"]) if info else 0

    @classmethod
    async def batch_get_provider_model_cooldowns(cls, provider: str) -> dict[str, list[dict]]:
        """获取单个渠道下所有账号的模型级冷却状态（给渠道详情页用）。

        从渠道冻结索引读取，不再全局 SCAN。
        返回 {username: [{model, remaining, until, reason}, ...]}
        """
        redis = cls.redis()
        if redis is None or not provider:
            return {}
        try:
            # 1. 从渠道模型冻结索引取成员
            members = await redis.smembers(cls._key(cls._index_models_key(provider)))
            if not members:
                return {}

            # 2. 构造精确 cooldown key → pipeline TTL + GET
            member_tuples: list[tuple[str, str, str]] = []  # (member_str, username, model)
            full_keys: list[str] = []
            for m in members:
                if isinstance(m, bytes):
                    m = m.decode("utf-8", errors="replace")
                m_str = str(m)
                sep = cls._MODEL_SEP
                sep_pos = m_str.find(sep)
                if sep_pos < 0:
                    continue
                username = m_str[:sep_pos]
                model_id = m_str[sep_pos + len(sep):]
                if not username or not model_id:
                    continue
                bare_key = f"limit:cooldown:{provider}:{username}:model:{model_id}"
                full_keys.append(cls._key(bare_key))
                member_tuples.append((m_str, username, model_id))

            if not full_keys:
                return {}

            async with redis.pipeline(transaction=False) as pipe:
                for k in full_keys:
                    pipe.ttl(k)
                    pipe.get(k)
                raw_results = await pipe.execute()

            # 3. 解析结果 + 惰性清理过期成员
            now = time.time()
            result: dict[str, list[dict]] = {}
            stale_members: list[str] = []
            for i, (m_str, username, model_id) in enumerate(member_tuples):
                ttl_val = raw_results[i * 2] if i * 2 < len(raw_results) else None
                raw = raw_results[i * 2 + 1] if i * 2 + 1 < len(raw_results) else None
                if ttl_val == -1:
                    # 永久冻结：无 TTL
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    reason = str(raw or "")
                    result.setdefault(username, []).append({
                        "model": model_id,
                        "remaining": None,
                        "until": None,
                        "reason": reason,
                        "permanent": True,
                    })
                    continue
                remaining = int(ttl_val) if isinstance(ttl_val, (int, float)) and ttl_val > 0 else 0
                if remaining <= 0:
                    stale_members.append(m_str)
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                reason = str(raw or "")
                result.setdefault(username, []).append({
                    "model": model_id,
                    "remaining": remaining,
                    "until": int(now + remaining),
                    "reason": reason,
                    "permanent": False,
                })

            # 惰性清除已过期的索引成员；清完后若渠道再无冻结则从渠道索引摘除
            if stale_members:
                try:
                    await redis.srem(cls._key(cls._index_models_key(provider)), stale_members)
                    await cls._index_prune_provider(provider)
                except Exception:
                    pass

            return result
        except Exception:
            return {}

    @classmethod
    async def batch_get_account_cooldowns(cls, provider: str, usernames: list[str]) -> dict[str, dict]:
        """批量获取多个账号的账号级冷却信息（给渠道详情页用）。

        单次 pipeline（GET + TTL），替代逐账号 2 次 Redis 调用。
        返回 {username: {until, remaining, reason}}
        """
        redis = cls.redis()
        if redis is None or not usernames:
            return {}
        try:
            bare_keys = [f"limit:cooldown:{provider}:{u}" for u in usernames]
            full_keys = [cls._key(k) for k in bare_keys]

            async with redis.pipeline(transaction=False) as pipe:
                for key in full_keys:
                    pipe.ttl(key)
                    pipe.get(key)
                raw_results = await pipe.execute()

            now = time.time()
            result = {}
            for i, key in enumerate(bare_keys):
                ttl_val = raw_results[i * 2] if i * 2 < len(raw_results) else None
                raw = raw_results[i * 2 + 1] if i * 2 + 1 < len(raw_results) else None
                if ttl_val == -1:
                    # 永久冻结：无 TTL
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    value = str(raw or "")
                    reason = value
                    if value.strip().startswith("{"):
                        try:
                            data = json.loads(value)
                            reason = str(data.get("reason") or "")
                        except Exception:
                            reason = ""
                    else:
                        try:
                            float(value)
                            reason = ""
                        except (TypeError, ValueError):
                            pass
                    username = key.split(f"limit:cooldown:{provider}:", 1)[1]
                    result[username] = {"until": None, "remaining": None, "reason": reason, "permanent": True}
                    continue
                remaining = int(ttl_val) if isinstance(ttl_val, (int, float)) and ttl_val > 0 else 0
                if remaining <= 0:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                value = str(raw or "")
                reason = value
                if value.strip().startswith("{"):
                    try:
                        data = json.loads(value)
                        reason = str(data.get("reason") or "")
                    except Exception:
                        reason = ""
                else:
                    try:
                        float(value)
                        reason = ""
                    except (TypeError, ValueError):
                        pass
                username = key.split(f"limit:cooldown:{provider}:", 1)[1]
                result[username] = {"until": now + remaining, "remaining": remaining, "reason": reason, "permanent": False}
            return result
        except Exception:
            return {}

    @classmethod
    async def scan_all_cooldowns(cls) -> dict[str, dict[str, dict]]:
        """获取所有账号级和账号-模型级冻结状态（给渠道总览用）。

        改为读冻结索引（providers → 各渠道 accounts/models 集合）+ 单次 pipeline 校验精确
        key 的 TTL，彻底避免全库 SCAN。是否冻结以精确 cooldown key 的 TTL 为准；索引里
        已失效的成员在读取后惰性 SREM 清理，保证自愈。
        返回结构：
        {
          "accounts": {provider: {username: {remaining, until, reason}}},
          "models": {provider: {username: [{model, remaining, until, reason}]}}
        }
        """
        redis = cls.redis()
        if redis is None:
            return {"accounts": {}, "models": {}}
        try:
            providers_raw = await redis.smembers(cls._key(cls._INDEX_PROVIDERS))
            providers = []
            for p in providers_raw or []:
                if isinstance(p, bytes):
                    p = p.decode("utf-8", errors="replace")
                p = str(p)
                if p:
                    providers.append(p)
            if not providers:
                return {"accounts": {}, "models": {}}

            # 收集每个渠道的账号级 / 模型级索引成员，拼出精确 cooldown key 列表
            # entries: [(kind, provider, username, model_or_None, exact_bare_key)]
            entries: list[tuple] = []
            # 先用一条 pipeline 批量取每个渠道的账号/模型索引成员，避免串行 smembers
            # 把往返次数乘以 provider 数量（渠道多时这里就是 admin 面板的卡点）。
            async with redis.pipeline(transaction=False) as pipe:
                for provider in providers:
                    pipe.smembers(cls._key(cls._index_accounts_key(provider)))
                    pipe.smembers(cls._key(cls._index_models_key(provider)))
                index_results = await pipe.execute()
            for i, provider in enumerate(providers):
                acc_members = index_results[i * 2]
                for u in acc_members or []:
                    if isinstance(u, bytes):
                        u = u.decode("utf-8", errors="replace")
                    u = str(u)
                    if not u:
                        continue
                    entries.append((
                        "account", provider, u, None,
                        f"limit:cooldown:{provider}:{u}",
                    ))
                mdl_members = index_results[i * 2 + 1]
                for m in mdl_members or []:
                    if isinstance(m, bytes):
                        m = m.decode("utf-8", errors="replace")
                    m = str(m)
                    if cls._MODEL_SEP not in m:
                        continue
                    username, model_id = m.split(cls._MODEL_SEP, 1)
                    if not username or not model_id:
                        continue
                    entries.append((
                        "model", provider, username, model_id,
                        f"limit:cooldown:{provider}:{username}:model:{model_id}",
                    ))
            if not entries:
                return {"accounts": {}, "models": {}}

            async with redis.pipeline(transaction=False) as pipe:
                for entry in entries:
                    pipe.ttl(cls._key(entry[4]))
                    pipe.get(cls._key(entry[4]))
                raw_results = await pipe.execute()

            now = time.time()
            accounts: dict[str, dict[str, dict]] = {}
            models: dict[str, dict[str, list[dict]]] = {}
            # 惰性清理：按渠道归集失效成员，读后一次性 SREM
            stale_accounts: dict[str, list[str]] = {}
            stale_models: dict[str, list[str]] = {}
            for i, entry in enumerate(entries):
                kind, provider, username, model_id, _bare = entry
                ttl_val = raw_results[i * 2] if i * 2 < len(raw_results) else None
                raw = raw_results[i * 2 + 1] if i * 2 + 1 < len(raw_results) else None
                remaining = int(ttl_val) if isinstance(ttl_val, (int, float)) and ttl_val > 0 else 0
                if remaining <= 0:
                    # 精确 key 已过期 → 索引成员失效，标记清理
                    if kind == "account":
                        stale_accounts.setdefault(provider, []).append(username)
                    else:
                        stale_models.setdefault(provider, []).append(
                            f"{username}{cls._MODEL_SEP}{model_id}")
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                value = str(raw or "")
                reason = value
                # 账号级兼容旧格式（JSON / 纯时间戳）
                if kind == "account":
                    if value.strip().startswith("{"):
                        try:
                            reason = str(json.loads(value).get("reason") or "")
                        except Exception:
                            reason = ""
                    else:
                        try:
                            float(value)
                            reason = ""
                        except (TypeError, ValueError):
                            pass
                status = {
                    "remaining": remaining,
                    "until": int(now + remaining),
                    "reason": reason,
                }
                if kind == "account":
                    accounts.setdefault(provider, {})[username] = status
                else:
                    models.setdefault(provider, {}).setdefault(username, []).append({
                        "model": model_id,
                        **status,
                    })

            # 惰性清理失效索引成员，并对空渠道摘除 providers 索引
            for provider, usernames in stale_accounts.items():
                try:
                    await redis.srem(cls._key(cls._index_accounts_key(provider)), usernames)
                except Exception:
                    pass
            for provider, members in stale_models.items():
                try:
                    await redis.srem(cls._key(cls._index_models_key(provider)), members)
                except Exception:
                    pass
            for provider in set(list(stale_accounts.keys()) + list(stale_models.keys())):
                await cls._index_prune_provider(provider)

            return {"accounts": accounts, "models": models}
        except Exception:
            return {"accounts": {}, "models": {}}

    @staticmethod
    def calendar_window_id(calendar: str, now: datetime | None = None) -> tuple[str, int]:
        now = now or datetime.now(timezone.utc)
        if calendar == "week":
            year, week, _ = now.isocalendar()
            start = datetime.fromisocalendar(year, week, 1).replace(tzinfo=timezone.utc)
            end = start + timedelta(days=7)
            return f"{year}-W{week:02d}", int((end - now).total_seconds()) + 3600
        if calendar == "month":
            start_next = datetime(now.year + (1 if now.month == 12 else 0), 1 if now.month == 12 else now.month + 1, 1, tzinfo=timezone.utc)
            return f"{now.year}-{now.month:02d}", int((start_next - now).total_seconds()) + 3600
        end = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
        return now.strftime("%Y-%m-%d"), int((end - now).total_seconds()) + 3600

    @classmethod
    async def get_calendar_count(cls, key: str, calendar: str) -> int | None:
        redis = cls.redis()
        if redis is None:
            return None
        window_id, _ = cls.calendar_window_id(calendar)
        try:
            value = await redis.get(f"limit:counter:{key}:{calendar}:{window_id}")
            return int(value or 0)
        except Exception:
            return None

    @classmethod
    async def batch_get_calendar_counts(cls, keys: list[str], calendar: str) -> dict[str, int]:
        """批量读多个日历计数 key 的当前值。

        单次非事务 pipeline（GET），替代逐 key 的 :meth:`get_calendar_count`。
        用于选路热路径的候选可靠性批量读取，避免 ``2 × 候选数`` 次 Redis 往返。
        返回 ``{key: 当前值}``；不存在 / 非数值 / Redis 不可用的 key 记为 0。

        注意：``pipe.get`` 是 coredis 原生方法，不经 :class:`RedisJdbc.get_key`
        自动加前缀，故必须用 :meth:`_key` 显式补前缀，与 :meth:`get_calendar_count`
        经覆写 ``redis.get`` 加前缀的读路径保持对称。
        """
        redis = cls.redis()
        if redis is None or not keys:
            return {}
        window_id, _ = cls.calendar_window_id(calendar)
        try:
            bare_keys = [f"limit:counter:{k}:{calendar}:{window_id}" for k in keys]
            full_keys = [cls._key(bk) for bk in bare_keys]

            async with redis.pipeline(transaction=False) as pipe:
                for key in full_keys:
                    pipe.get(key)
                raw_results = await pipe.execute()

            result: dict[str, int] = {}
            for i, orig_key in enumerate(keys):
                raw = raw_results[i] if i < len(raw_results) else None
                if raw is None:
                    result[orig_key] = 0
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    result[orig_key] = int(raw)
                except (TypeError, ValueError):
                    result[orig_key] = 0
            return result
        except Exception:
            return {}

    @classmethod
    async def incr_calendar_count(cls, key: str, calendar: str, amount: int = 1) -> None:
        redis = cls.redis()
        if redis is None or amount <= 0:
            return
        window_id, ttl = cls.calendar_window_id(calendar)
        redis_key = f"limit:counter:{key}:{calendar}:{window_id}"
        try:
            await redis.incrby(redis_key, amount)
            await redis.expire(redis_key, ttl)
        except Exception:
            pass

    @classmethod
    async def batch_incr_calendar_counts(cls, increments: dict[str, int], calendar: str) -> None:
        """批量累加多个日历计数 key（合并可靠性写入）。

        单次非事务 pipeline，把 ``{key: amount}`` 里每个 key 的 ``INCRBY`` + ``EXPIRE``
        合并到一次往返，替代逐事件的 :meth:`incr_calendar_count`（两条命令 × 事件数）。

        注意：pipeline 上的 ``incrby`` / ``expire`` 是 coredis 原生方法，不经
        :class:`RedisJdbc.get_key` 自动加前缀（覆写只在客户端实例上生效），故必须用
        :meth:`_key` 显式补前缀，与 :meth:`incr_calendar_count` 走覆写方法的写路径对称。
        """
        redis = cls.redis()
        if redis is None or not increments:
            return
        window_id, ttl = cls.calendar_window_id(calendar)
        items = [(k, int(v)) for k, v in increments.items() if int(v) > 0]
        if not items:
            return
        try:
            async with redis.pipeline(transaction=False) as pipe:
                for key, amount in items:
                    redis_key = f"limit:counter:{key}:{calendar}:{window_id}"
                    pipe.incrby(cls._key(redis_key), amount)
                    pipe.expire(cls._key(redis_key), ttl)
                await pipe.execute()
        except Exception:
            pass

    @classmethod
    async def set_snapshot(cls, key: str, payload: dict, ttl_seconds: int = 86400) -> None:
        redis = cls.redis()
        if redis is None:
            return
        try:
            await redis.set(f"limit:snapshot:{key}", json.dumps(payload, ensure_ascii=False), ex=ttl_seconds)
        except Exception:
            pass

    @classmethod
    async def get_snapshot(cls, key: str) -> dict | None:
        redis = cls.redis()
        if redis is None:
            return None
        try:
            raw = await redis.get(f"limit:snapshot:{key}")
            if not raw:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return json.loads(raw)
        except Exception:
            return None
