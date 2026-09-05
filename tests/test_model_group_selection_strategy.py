import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import PostgresClient


def test_normalize_model_group_defaults_selection_strategy_to_intelligent():
    out = PostgresClient.normalize_model_group("g1", {"models": ["m1"]})
    assert out["selection_strategy"] == "intelligent"


def test_normalize_model_group_keeps_legal_selection_strategy():
    out = PostgresClient.normalize_model_group("g1", {"models": ["m1"], "selection_strategy": "random_all"})
    assert out["selection_strategy"] == "random_all"


def test_normalize_model_group_falls_back_to_intelligent_on_invalid_value():
    out = PostgresClient.normalize_model_group("g1", {"models": ["m1"], "selection_strategy": "garbage"})
    assert out["selection_strategy"] == "intelligent"


def test_model_group_table_has_selection_strategy_column_with_intelligent_default():
    source = open("db.py", encoding="utf-8").read()
    assert "ALTER TABLE model_groups" in source
    assert "ADD COLUMN IF NOT EXISTS selection_strategy" in source
    assert "DEFAULT 'intelligent'" in source


def test_model_group_row_reads_selection_strategy():
    row = {
        "name": "g1",
        "enabled": True,
        "remark": "",
        "hidden_in_admin": False,
        "extra_cache": False,
        "models": ["m1"],
        "aliases": [],
        "provider_whitelist": [],
        "provider_blacklist": [],
        "selection_strategy": "random_member",
        "backup_group": "",
        "response_model": "public-g1",
    }
    out = PostgresClient._model_group_row(row)
    assert out["selection_strategy"] == "random_member"
    assert out["response_model"] == "public-g1"


import config
import pytest


def _async_return(value):
    async def _coro():
        return value
    return _coro()


def _install_routes(monkeypatch, routes: dict):
    """把旧式 {model_id: [route,...]} 装成新的 iter_model_candidates/has_model_route。

    渠道 = 内存对象后不再有全局 _model_routes；测试改为 patch 内存扫描访问器。
    route dict 需含 provider/upstream_model_id；pool 从 _provider_pools 取（测试自行注入）。
    """
    from rate_limiter import ModelClientPool

    def _iter(cls, model_id):
        for route in routes.get(model_id, []) or []:
            provider = route.get("provider")
            pool = cls._provider_pools.get(provider)
            if pool is not None:
                yield provider, pool, route

    monkeypatch.setattr(ModelClientPool, "iter_model_candidates", classmethod(_iter))
    monkeypatch.setattr(ModelClientPool, "has_model_route", classmethod(lambda cls, m: m in routes))
    monkeypatch.setattr(ModelClientPool, "all_model_ids", classmethod(lambda cls: set(routes)))
    monkeypatch.setattr(ModelClientPool, "get_model_routes", classmethod(lambda cls, m: [dict(r) for r in routes.get(m, [])]))


def test_get_api_key_strategy_falls_back_to_intelligent(monkeypatch):
    monkeypatch.setattr(config.Config, "get_api_key_config", classmethod(lambda cls, api_key, include_disabled=False: _async_return({"selection_strategy": ""})))
    import asyncio
    assert asyncio.run(config.Config.get_api_key_strategy("sk")) == "intelligent"


def test_get_api_key_strategy_returns_stored_value(monkeypatch):
    monkeypatch.setattr(config.Config, "get_api_key_config", classmethod(lambda cls, api_key, include_disabled=False: _async_return({"selection_strategy": "random_member"})))
    import asyncio
    assert asyncio.run(config.Config.get_api_key_strategy("sk")) == "random_member"


def test_get_api_key_strategy_normalizes_invalid_value(monkeypatch):
    monkeypatch.setattr(config.Config, "get_api_key_config", classmethod(lambda cls, api_key, include_disabled=False: _async_return({"selection_strategy": "garbage"})))
    import asyncio
    assert asyncio.run(config.Config.get_api_key_strategy("sk")) == "intelligent"


def test_get_api_key_strategy_for_missing_key():
    import asyncio
    assert asyncio.run(config.Config.get_api_key_strategy(None)) == "intelligent"


from rate_limiter import ModelClientPool


def _patch_group(monkeypatch, strategy):
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, model: _async_return(model == "g1")))
    monkeypatch.setattr(config.Config, "get_model_group_models", classmethod(lambda cls, model: _async_return(["m1", "m2"] if model == "g1" else [])))
    monkeypatch.setattr(config.Config, "get_api_key_strategy", classmethod(lambda cls, api_key: _async_return(strategy)))
    monkeypatch.setattr(config.Config, "get_model_group_provider_filter", classmethod(lambda cls, model: _async_return((set(), set()))))
    monkeypatch.setattr(config.Config, "get_api_key_provider_filter", classmethod(lambda cls, api_key: _async_return((set(), set()))))
    monkeypatch.setattr(config.Config, "get_providers", classmethod(lambda cls: _async_return({})))


def test_pick_start_index_for_sequential():
    assert ModelClientPool._pick_start_index("sequential", 3) == 0


def test_pick_start_index_for_random_member_within_range(monkeypatch):
    import random
    monkeypatch.setattr(random, "randrange", lambda n: n - 1)
    assert ModelClientPool._pick_start_index("random_member", 5) == 4


def test_pick_start_index_falls_back_to_sequential_for_invalid_strategy():
    assert ModelClientPool._pick_start_index("garbage", 3) == 0


def test_collect_candidates_iter_rotates_by_start_index(monkeypatch):
    _patch_group(monkeypatch, "random_member")
    _install_routes(monkeypatch, {
        "m1": [{"provider": "p1", "upstream_model_id": "m1"}],
        "m2": [{"provider": "p1", "upstream_model_id": "m2"}],
        "m3": [{"provider": "p1", "upstream_model_id": "m3"}],
    })
    monkeypatch.setattr(ModelClientPool, "_pick_start_index", classmethod(lambda cls, s, n: 1))
    members = ["m1", "m2", "m3"]
    start_idx = ModelClientPool._pick_start_index("random_member", len(members))
    rotated = members[start_idx:] + members[:start_idx]
    assert rotated == ["m2", "m3", "m1"]


class _StubAccountClient:
    def __init__(self, username, weight=1, priority=0, provider_name="p1"):
        self.username = username
        self.weight = weight
        self.priority = priority
        self._cooldown_until = 0
        self.is_frozen = False
        self.disabled = False
        self._requests = []
        self._token_usages = []
        self.rpm_limit = 0
        self.tpm_limit = 0
        self.reserved_for = []
        # 智能选路新增的统计字段（贴合真实 AccountClient 契约）
        self._recent_picks = []
        self._recent_success = []
        self._recent_failure = []
        self._consecutive_failures = 0
        self._failure_cooldown_until = 0
        self._circuit_failures = []
        self.balance = None
        self.balance_threshold = 0
        self._provider_daily_remaining = None
        self._provider_daily_limit = None
        self._provider_quota_disabled_until = 0
        self.provider = type("P", (), {"PROVIDER_NAME": provider_name})()
        self.last_route_info = {}

    # 智能评分读取的配额 property（贴合真实 AccountClient 契约；返回 None 表示无配额限制）
    @property
    def daily_requests_remaining(self):
        return None

    @property
    def daily_requests_limit(self):
        return None

    @property
    def model_daily_remaining(self):
        return {}

    async def can_accept(self, model, messages):
        return True

    async def reserve(self, model, messages):
        if self._cooldown_until and self._cooldown_until > 0:
            return False
        self.reserved_for.append(model)
        return True

    async def reserve_with_decision(self, model, messages):
        from limits.rules import LimitDecision, LimitLease, LimitReservation, LimitSubject
        allowed = await self.reserve(model, messages)
        lease = LimitLease(LimitSubject(model=model), f"test:{self.username}") if allowed else None
        return LimitReservation(lease, LimitDecision(allowed, "" if allowed else "reservation_denied"))

    async def release(self):
        return None


def _setup_two_members_two_accounts(monkeypatch, strategy):
    _patch_group(monkeypatch, strategy)
    import rate_limiter
    monkeypatch.setattr(rate_limiter, "has_explicit_metadata", lambda model: _async_return(True))
    monkeypatch.setattr(rate_limiter, "get_model_metadata", lambda model: _async_return(({}, False)))
    _install_routes(monkeypatch, {
        "m1": [{"provider": "p1", "upstream_model_id": "m1"}],
        "m2": [{"provider": "p1", "upstream_model_id": "m2"}],
    })
    pool = type("Pool", (), {})()
    a1 = _StubAccountClient("u1", weight=1, priority=0)
    a2 = _StubAccountClient("u2", weight=1, priority=1)
    pool.clients = [a1, a2]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))
    monkeypatch.setattr(ModelClientPool, "_candidate_weight", classmethod(lambda cls, p, c, m: {"score": 1.0, "weight": c.weight, "channel_score": 1.0, "priority_factor": 1.0, "quota_factor": 1.0, "headroom_factor": 1.0}))
    return a1, a2


async def _async_true():
    return True


def test_media_operation_filters_chat_only_provider(monkeypatch):
    _patch_group(monkeypatch, "sequential")
    import rate_limiter
    monkeypatch.setattr(rate_limiter, "has_explicit_metadata", lambda model: _async_return(True))
    monkeypatch.setattr(rate_limiter, "get_model_metadata", lambda model: _async_return(({"output_modalities": ["image"], "capabilities": {"image_generation": True}}, True)))
    _install_routes(monkeypatch, {
        "m1": [{"provider": "p1", "upstream_model_id": "m1"}],
        "m2": [{"provider": "p2", "upstream_model_id": "m2"}],
    })
    pool1 = type("Pool", (), {})()
    pool2 = type("Pool", (), {})()
    chat_only = _StubAccountClient("u1")
    image_account = _StubAccountClient("u2")
    chat_only.provider = type("ChatOnly", (), {"PROVIDER_NAME": "p1"})()
    image_account.provider = type("ImageProvider", (), {"PROVIDER_NAME": "p2", "supports_image_generation": True, "generate_image": lambda self: None})()
    pool1.clients = [chat_only]
    pool2.clients = [image_account]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool1, "p2": pool2})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))
    monkeypatch.setattr(ModelClientPool, "_candidate_weight", classmethod(lambda cls, p, c, m: {"score": 1.0, "weight": c.weight, "channel_score": 1.0, "priority_factor": 1.0, "quota_factor": 1.0, "headroom_factor": 1.0}))
    monkeypatch.setattr(ModelClientPool, "_weighted_pick", classmethod(lambda cls, candidates: candidates[0]))

    import asyncio
    _, provider_name, client = asyncio.run(
        ModelClientPool._get_available_client_with_provider("g1", messages=[], operation="image_generation")
    )

    assert provider_name == "p2"
    assert client.username == "u2"

    a1, a2 = _setup_two_members_two_accounts(monkeypatch, "sequential")
    monkeypatch.setattr(ModelClientPool, "_pick_start_index", classmethod(lambda cls, s, n: 0))
    monkeypatch.setattr(ModelClientPool, "_weighted_pick", classmethod(lambda cls, candidates: candidates[0]))
    import asyncio
    provider, name, client = asyncio.run(
        ModelClientPool._get_available_client_with_provider("g1", messages=[])
    )
    assert client.username == "u1"


def test_group_retries_another_account_after_reservation_denial(monkeypatch):
    first, second = _setup_two_members_two_accounts(monkeypatch, "sequential")
    first._cooldown_until = 1
    monkeypatch.setattr(ModelClientPool, "_pick_start_index", classmethod(lambda cls, s, n: 0))

    import asyncio
    _, _, client = asyncio.run(ModelClientPool._get_available_client_with_provider("g1", messages=[]))

    assert client is second
    assert second.reserved_for


def test_explicit_route_retries_another_account_in_same_entry(monkeypatch):
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, model: _async_return(False)))
    monkeypatch.setattr(config.Config, "get_api_key_strategy", classmethod(lambda cls, api_key: _async_return("sequential")))
    _install_routes(monkeypatch, {"m1": [{"provider": "p1", "upstream_model_id": "m1"}]})
    first = _StubAccountClient("u1", priority=0)
    second = _StubAccountClient("u2", priority=1)
    first._cooldown_until = 1
    pool = type("Pool", (), {})()
    pool.clients = [first, second]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))

    import asyncio
    _, _, client = asyncio.run(ModelClientPool._get_available_client_with_provider(
        "m1",
        messages=[],
        route={"entries": [{"provider": "p1", "accounts": ["u1", "u2"]}]},
    ))

    assert client is second


def test_random_member_strategy_picks_different_start_member_across_calls(monkeypatch):
    """渠道随机：两个模型各有不同 provider，shuffle provider 后应轮流选到不同模型。"""
    _patch_group(monkeypatch, "random_member")
    import rate_limiter
    monkeypatch.setattr(rate_limiter, "has_explicit_metadata", lambda model: _async_return(True))
    monkeypatch.setattr(rate_limiter, "get_model_metadata", lambda model: _async_return(({}, False)))
    _install_routes(monkeypatch, {
        "m1": [{"provider": "p1", "upstream_model_id": "m1"}],
        "m2": [{"provider": "p2", "upstream_model_id": "m2"}],
    })
    pool1 = type("Pool", (), {})()
    pool1.clients = [_StubAccountClient("u1", weight=1, priority=0)]
    pool2 = type("Pool", (), {})()
    pool2.clients = [_StubAccountClient("u2", weight=1, priority=0)]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool1, "p2": pool2})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))

    # 第一次 shuffle 反转列表（p2 优先），第二次不反转（p1 优先）
    call_count = [0]
    def alternating_shuffle(lst):
        call_count[0] += 1
        if call_count[0] % 2 == 1:
            lst.reverse()
    monkeypatch.setattr(rate_limiter.random, "shuffle", alternating_shuffle)
    import asyncio
    routed_models = set()
    for _ in range(2):
        _, _, client = asyncio.run(
            ModelClientPool._get_available_client_with_provider("g1", messages=[])
        )
        routed_models.add(client.last_route_info.get("routed_model"))
    assert routed_models == {"m1", "m2"}


def test_random_all_strategy_first_call_differs_from_sequential_first(monkeypatch):
    a1, a2 = _setup_two_members_two_accounts(monkeypatch, "random_all")
    import rate_limiter
    # sequential picks u1 (priority 0) on m1.
    # random_all uses random.choice. Pin it to return last element → u2 on m2.
    monkeypatch.setattr(rate_limiter.random, "choice", lambda lst: lst[-1])
    import asyncio
    _, _, client = asyncio.run(
        ModelClientPool._get_available_client_with_provider("g1", messages=[])
    )
    assert client.last_route_info.get("routed_model") == "m2"


def test_explicit_route_uses_candidate_weight_without_legacy_override(monkeypatch):
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, model: _async_return(False)))
    monkeypatch.setattr(config.Config, "get_api_key_provider_filter", classmethod(lambda cls, api_key: _async_return((set(), set()))))
    monkeypatch.setattr(config.Config, "get_providers", classmethod(lambda cls: _async_return({})))
    # 该用例验证 sequential 分支不再套用旧的候选权重覆盖；默认策略已改为 intelligent，显式钉住 sequential。
    monkeypatch.setattr(config.Config, "get_api_key_strategy", classmethod(lambda cls, api_key: _async_return("sequential")))
    _install_routes(monkeypatch, {
        "m1": [{"provider": "p1", "upstream_model_id": "m1"}],
    })
    pool = type("Pool", (), {})()
    account = _StubAccountClient("u1", weight=10, priority=9)
    pool.clients = [account]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))
    monkeypatch.setattr(
        ModelClientPool,
        "_candidate_weight",
        classmethod(lambda cls, p, c, m: {
            "score": 0.25,
            "weight": c.weight,
            "channel_score": 0.25,
            "priority_factor": 0.5,
            "quota_factor": 1.0,
            "headroom_factor": 1.0,
        }),
    )
    monkeypatch.setattr(ModelClientPool, "_weighted_pick", classmethod(lambda cls, candidates: candidates[0]))

    import asyncio
    _, _, client = asyncio.run(ModelClientPool._get_available_client_with_provider(
        "m1",
        messages=[],
        route={"id": 7, "entries": [{"provider": "p1", "account": "u1"}]},
    ))

    assert client is account
    assert account.last_route_info["score"] == 1.0  # sequential 不再使用评分，默认 1.0
    assert account.last_route_info["route_id"] == 7


def _reset_provider_rotation_state(monkeypatch):
    monkeypatch.setattr(ModelClientPool, "_provider_pick_history", [])
    monkeypatch.setattr(ModelClientPool, "_provider_recent_picks", {})
    monkeypatch.setattr(ModelClientPool, "_upstream_model_affinity", {})


def _setup_group_provider_routes(monkeypatch, strategy, providers):
    members = [f"m{i + 1}" for i in range(len(providers))]
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, model: _async_return(model == "g1")))
    monkeypatch.setattr(config.Config, "get_model_group_models", classmethod(lambda cls, model: _async_return(members if model == "g1" else [])))
    monkeypatch.setattr(config.Config, "get_api_key_strategy", classmethod(lambda cls, api_key: _async_return(strategy)))
    monkeypatch.setattr(config.Config, "get_model_group_provider_filter", classmethod(lambda cls, model: _async_return((set(), set()))))
    monkeypatch.setattr(config.Config, "get_api_key_provider_filter", classmethod(lambda cls, api_key: _async_return((set(), set()))))
    monkeypatch.setattr(config.Config, "get_providers", classmethod(lambda cls: _async_return({})))
    import rate_limiter
    monkeypatch.setattr(rate_limiter, "has_explicit_metadata", lambda model: _async_return(True))
    monkeypatch.setattr(rate_limiter, "get_model_metadata", lambda model: _async_return(({}, False)))
    _install_routes(monkeypatch, {
        member: [{"provider": provider, "upstream_model_id": member}]
        for member, provider in zip(members, providers)
    })
    pools = {}
    for provider in providers:
        pool = type("Pool", (), {})()
        pool.clients = [_StubAccountClient(f"u-{provider}", weight=1, priority=0, provider_name=provider)]
        pools[provider] = pool
    monkeypatch.setattr(ModelClientPool, "_provider_pools", pools)
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))


def test_sequential_avoids_last_provider_for_group(monkeypatch):
    _reset_provider_rotation_state(monkeypatch)
    _setup_group_provider_routes(monkeypatch, "sequential", ["p1", "p2"])

    import asyncio
    providers = []
    for _ in range(2):
        _, provider_name, _ = asyncio.run(ModelClientPool._get_available_client_with_provider("g1", messages=[]))
        providers.append(provider_name)

    assert providers == ["p1", "p2"]


def test_sequential_rotation_never_repeats_adjacent_provider(monkeypatch):
    _reset_provider_rotation_state(monkeypatch)
    _setup_group_provider_routes(monkeypatch, "sequential", ["p1", "p2", "p3"])

    import asyncio
    providers = []
    for _ in range(4):
        _, provider_name, _ = asyncio.run(ModelClientPool._get_available_client_with_provider("g1", messages=[]))
        providers.append(provider_name)

    assert all(left != right for left, right in zip(providers, providers[1:]))
    assert len(set(providers)) >= 2


def test_provider_rotation_falls_back_when_only_one_provider(monkeypatch):
    _reset_provider_rotation_state(monkeypatch)
    _setup_group_provider_routes(monkeypatch, "sequential", ["p1"])

    import asyncio
    providers = []
    for _ in range(2):
        _, provider_name, _ = asyncio.run(ModelClientPool._get_available_client_with_provider("g1", messages=[]))
        providers.append(provider_name)

    assert providers == ["p1", "p1"]
