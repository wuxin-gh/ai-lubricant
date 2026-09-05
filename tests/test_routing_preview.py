"""路由预演（preview_routing）单测：只读候选收集 + 评分排名，零 reserve 副作用。

不依赖 PG/Redis：monkeypatch 掉 is_model_group / _collect_candidates /
_candidate_reliability，用真实 AccountClient 跑真实评分函数，断言排名/分项/
被淘汰原因/零副作用。
"""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from rate_limiter import AccountClient, ModelClientPool


def _make_provider(username: str) -> SimpleNamespace:
    p = SimpleNamespace()
    p.PROVIDER_NAME = "custom"
    p.username = username
    p.quotas = None
    p.protocol = "openai"
    return p


def _make_client(username: str, priority: int = 0, weight: int = 10,
                 is_frozen: bool = False) -> AccountClient:
    return AccountClient(
        _make_provider(username), rpm_limit=0, priority=priority, weight=weight,
        is_frozen=is_frozen,
    )


def _candidate(provider_name: str, client: AccountClient, model: str = "gpt-x") -> dict:
    return {
        "provider_name": provider_name,
        "account_client": client,
        "routed_model": model,
        "upstream_model_id": model,
        "route_info": {
            "provider": provider_name,
            "account": client.username,
            "routed_model": model,
            "protocol": "openai",
            "score": 1.0,
        },
    }


@pytest.fixture(autouse=True)
def _reset_state():
    for d in (
        ModelClientPool._session_affinity, ModelClientPool._session_failures,
        ModelClientPool._channel_scores, ModelClientPool._provider_recent_picks,
        ModelClientPool._provider_pick_history,
    ):
        d.clear()
    yield
    for d in (
        ModelClientPool._session_affinity, ModelClientPool._session_failures,
        ModelClientPool._channel_scores, ModelClientPool._provider_recent_picks,
        ModelClientPool._provider_pick_history,
    ):
        d.clear()


def _patch_common(monkeypatch, candidates, skip_reasons):
    async def _is_group(model_id, *, snapshot=None):
        return False

    async def _collect(cls, *args, **kwargs):
        return (list(candidates), [], dict(skip_reasons))

    async def _reliability(cls, cand):
        return {(c["provider_name"], c["account_client"].username): 1.0 for c in cand}

    monkeypatch.setattr("rate_limiter.config.Config.is_model_group", _is_group)
    monkeypatch.setattr(ModelClientPool, "_collect_candidates", classmethod(_collect))
    monkeypatch.setattr(ModelClientPool, "_candidate_reliability", classmethod(_reliability))


@pytest.mark.asyncio
async def test_intelligent_ranks_by_score_and_excludes_frozen(monkeypatch):
    """intelligent：ranked=true、按分降序、分项齐全；冻结账号 score=0 且标注 zeroed_by。"""
    high = _make_client("high", priority=0, weight=100)   # 高优先级 + 高权重 → 高分
    low = _make_client("low", priority=5, weight=1)        # 低优先级 + 低权重 → 低分
    frozen = _make_client("frozen", priority=0, weight=100, is_frozen=True)
    cands = [_candidate("custom", low), _candidate("custom", high), _candidate("custom", frozen)]
    _patch_common(monkeypatch, cands, {"account_cooldown": 2})

    result = await ModelClientPool.preview_routing("gpt-x", "intelligent")

    assert result["ok"] is True
    assert result["ranked"] is True
    assert result["strategy"] == "intelligent"
    assert result["preview_context"]["api_key_bound"] is False
    assert result["preview_context"]["volume_factor"] == 0

    ranks = result["candidates"]
    assert [c["rank"] for c in ranks] == [1, 2, 3]
    # 冻结账号排最后、score=0、zeroed_by 标注
    frozen_row = next(c for c in ranks if c["account"] == "frozen")
    assert frozen_row["score"] == 0
    assert frozen_row["zeroed_by"] == "frozen"
    assert frozen_row["zeroed_by_label"]
    # high 分 > low 分，且都是有效分
    high_row = next(c for c in ranks if c["account"] == "high")
    low_row = next(c for c in ranks if c["account"] == "low")
    assert high_row["score"] > low_row["score"] > 0
    # 分项齐全
    assert "priority_score" in high_row["score_breakdown"]
    assert "speed_score" in high_row["score_breakdown"]
    # 被淘汰原因带中文标签
    assert result["skipped"] == [{"reason": "account_cooldown", "label": "账号处于冷却期", "count": 2}]


@pytest.mark.asyncio
async def test_intelligent_no_reserve_side_effects(monkeypatch):
    """预演不 reserve：调用前后账号并发占用/请求计数不变。"""
    c1 = _make_client("a", priority=0, weight=50)
    c2 = _make_client("b", priority=1, weight=50)
    _patch_common(monkeypatch, [_candidate("custom", c1), _candidate("custom", c2)], {})

    await ModelClientPool.preview_routing("gpt-x", "intelligent")

    for c in (c1, c2):
        assert c._in_flight == 0
        assert c._requests == []
        assert c._recent_picks == []


@pytest.mark.asyncio
async def test_random_strategy_is_not_ranked(monkeypatch):
    """随机策略：ranked=false，候选无 rank/score。"""
    c1 = _make_client("a")
    c2 = _make_client("b")
    _patch_common(monkeypatch, [_candidate("custom", c1), _candidate("custom", c2)], {})

    result = await ModelClientPool.preview_routing("gpt-x", "random_all")

    assert result["ranked"] is False
    assert result["strategy"] == "random_all"
    assert len(result["candidates"]) == 2
    for c in result["candidates"]:
        assert "rank" not in c
        assert "score" not in c


@pytest.mark.asyncio
async def test_sequential_orders_by_priority_then_weight(monkeypatch):
    """sequential：按 priority 升序、同 priority 按 weight 降序排名。"""
    p0w10 = _make_client("p0w10", priority=0, weight=10)
    p0w90 = _make_client("p0w90", priority=0, weight=90)
    p3w99 = _make_client("p3w99", priority=3, weight=99)
    _patch_common(
        monkeypatch,
        [_candidate("custom", p0w10), _candidate("custom", p3w99), _candidate("custom", p0w90)],
        {},
    )

    result = await ModelClientPool.preview_routing("gpt-x", "sequential")

    assert result["ranked"] is True
    order = [c["account"] for c in result["candidates"]]
    # priority 0 的两个在前（w90 > w10），priority 3 的最后
    assert order == ["p0w90", "p0w10", "p3w99"]
    assert result["candidates"][0]["priority"] == 0
    assert result["candidates"][0]["weight"] == 90


@pytest.mark.asyncio
async def test_unknown_strategy_falls_back_to_default(monkeypatch):
    """非法策略回落到 DEFAULT_SELECTION_STRATEGY（intelligent）。"""
    c1 = _make_client("a")
    _patch_common(monkeypatch, [_candidate("custom", c1)], {})

    result = await ModelClientPool.preview_routing("gpt-x", "not-a-strategy")

    assert result["strategy"] == "intelligent"
    assert result["ranked"] is True
