"""API Key 流量驱动错误抑制、动态错误窗口与智能选择兜底测试。"""
import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from limits.backend import RedisLimitBackend
from rate_limiter import AccountClient, ModelClientPool, RateLimiter


def _client(username: str, *, balance: float | None = None, balance_threshold: float = 0) -> AccountClient:
    provider = SimpleNamespace(
        PROVIDER_NAME="custom",
        username=username,
        quotas=None,
    )
    return AccountClient(
        provider,
        rpm_limit=0,
        priority=0,
        weight=10,
        balance=balance,
        balance_threshold=balance_threshold,
    )


def _candidate(username: str, last_pick: float | None = None) -> dict:
    client = _client(username)
    if last_pick is not None:
        client._recent_picks.append(last_pick)
    return {
        "provider_name": "custom",
        "account_client": client,
        "routed_model": "m1",
        "route_info": {},
    }


@pytest.fixture(autouse=True)
def _reset_routing_state():
    old_scores = ModelClientPool._channel_scores
    old_picks = ModelClientPool._provider_recent_picks
    old_history = ModelClientPool._provider_pick_history
    ModelClientPool._channel_scores = {}
    ModelClientPool._provider_recent_picks = {}
    ModelClientPool._provider_pick_history = []
    yield
    ModelClientPool._channel_scores = old_scores
    ModelClientPool._provider_recent_picks = old_picks
    ModelClientPool._provider_pick_history = old_history


def test_api_key_volume_factor_uses_current_key_rpm(monkeypatch):
    seen = []

    async def fake_get_window(cls, key):
        seen.append(key)
        return 180, 42

    monkeypatch.setattr(RedisLimitBackend, "get_window", classmethod(fake_get_window))

    factor = asyncio.run(ModelClientPool._api_key_volume_factor("sk-volume"))

    assert seen == [f"api-key:{RateLimiter._subject('sk-volume')}:requests_per_minute"]
    assert factor == pytest.approx(0.3)


def test_api_key_volume_factor_steps_up_and_degrades_safely(monkeypatch):
    async def factor_for(used):
        async def fake_get_window(cls, key):
            return used, 30

        monkeypatch.setattr(RedisLimitBackend, "get_window", classmethod(fake_get_window))
        return await ModelClientPool._api_key_volume_factor("sk-tier")

    assert asyncio.run(factor_for(29)) == 0.0
    assert asyncio.run(factor_for(120)) == pytest.approx(0.3)
    assert asyncio.run(factor_for(300)) == pytest.approx(0.6)
    assert asyncio.run(factor_for(301)) == pytest.approx(ModelClientPool.VOLUME_RPM_MAX)
    assert asyncio.run(ModelClientPool._api_key_volume_factor(None)) == 0.0


def test_high_volume_additionally_penalizes_recent_account_error():
    now = time.time()
    low_volume = _client("low")
    high_volume = _client("high")
    for client in (low_volume, high_volume):
        client._recent_success.append(now)
        client._recent_failure.append(now)

    low = ModelClientPool._intelligent_score("custom", low_volume, "m1", volume_factor=0.0)
    high = ModelClientPool._intelligent_score("custom", high_volume, "m1", volume_factor=0.8)

    assert low["volume_error_factor"] == 1.0
    assert high["volume_error_factor"] == pytest.approx(0.6)
    assert high["score"] == pytest.approx(low["score"] * 0.6)


def test_high_volume_penalizes_channel_error_even_when_account_is_clean():
    ModelClientPool._channel_scores = {
        "custom": {
            "error_rate": 0.75,
            "recent_failures": 0,
        }
    }
    client = _client("clean")
    client._recent_success.append(time.time())

    info = ModelClientPool._fast_intelligent_score(
        "custom",
        client,
        "m1",
        volume_factor=0.8,
    )

    assert info["error_score"] == 1.0
    assert info["volume_error_factor"] == pytest.approx(0.4)


@pytest.mark.parametrize(
    ("global_failures", "expected_window"),
    [(0, 60.0), (59, 60.0), (60, 40.0), (120, 25.0), (240, 15.0)],
)
def test_effective_error_window_uses_global_failure_tiers(monkeypatch, global_failures, expected_window):
    monkeypatch.setattr(
        ModelClientPool,
        "_global_recent_failure_count",
        classmethod(lambda cls: global_failures),
    )

    assert ModelClientPool._effective_error_window() == expected_window
    assert ModelClientPool.FAILURE_WINDOW_SECONDS == 300


def test_account_error_rate_uses_shortened_window(monkeypatch):
    monkeypatch.setattr(
        ModelClientPool,
        "_global_recent_failure_count",
        classmethod(lambda cls: 240),
    )
    client = _client("recovering")
    client._recent_failure.append(time.time() - 20)

    assert ModelClientPool._account_error_rate(client) == 0.0


@pytest.mark.parametrize("selector_name", ["_select_intelligent", "_select_fast_intelligent"])
def test_intelligent_selectors_return_none_when_all_score_zero(monkeypatch, selector_name):
    # 全部候选都在失败熔断冷却 → 评分全 0 → 不再 LRU 探测，直接返回 None（交由组回退 / 429）。
    a = _candidate("a")
    b = _candidate("b")
    a["account_client"]._failure_cooldown_until = time.time() + 30
    b["account_client"]._failure_cooldown_until = time.time() + 30

    async def neutral_reliability(cls, candidates):
        return {}

    monkeypatch.setattr(ModelClientPool, "_candidate_reliability", classmethod(neutral_reliability))
    selector = getattr(ModelClientPool, selector_name)

    selected = asyncio.run(selector([a, b], messages=None))
    assert selected is None


@pytest.mark.parametrize("selector_name", ["_select_intelligent", "_select_fast_intelligent"])
def test_intelligent_selectors_pick_fresh_candidate_without_recent_success(monkeypatch, selector_name):
    # 候选都没有近期成功记录，但评分 > 0（新鲜候选）→ 仍走带权选择返回一个候选，
    # 不返回 None、不再走 LRU 兜底。单候选确定性返回。
    fresh = _candidate("fresh")

    async def neutral_reliability(cls, candidates):
        return {}

    monkeypatch.setattr(ModelClientPool, "_candidate_reliability", classmethod(neutral_reliability))
    selector = getattr(ModelClientPool, selector_name)

    selected = asyncio.run(selector([fresh], messages=None))
    assert selected is fresh
