"""频率惩罚（短期滑窗降权）单元测试。

验证：用多了降权 + 窗口过期恢复 + 账号级/渠道级组合 + 最近轮次概率降档 + provider-first 带权随机。
"""
import asyncio
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rate_limiter import ModelClientPool, AccountClient


def _make_account(provider=None, username="u1"):
    prov = provider or type("P", (), {"PROVIDER_NAME": "p1", "username": username})()
    # 用最小参数构造 AccountClient，避免触发真实 provider 依赖
    return AccountClient(provider=prov, rpm_limit=0, priority=0, weight=1)


@pytest.fixture(autouse=True)
def _clear_provider_state():
    """每个测试前后清理 ModelClientPool 的渠道级命中状态，避免相互污染。"""
    ModelClientPool._provider_recent_picks.clear()
    ModelClientPool._provider_pick_history.clear()
    yield
    ModelClientPool._provider_recent_picks.clear()
    ModelClientPool._provider_pick_history.clear()


def test_frequency_factor_no_penalty_below_soft_cap():
    acc = _make_account()
    now = time.time()
    for _ in range(ModelClientPool.PICK_SOFT_CAP):
        acc._stats._picks.append(now)
    assert ModelClientPool._frequency_factor(acc) == 1.0


def test_frequency_factor_decays_after_soft_cap_with_floor():
    acc = _make_account()
    now = time.time()
    # 远超 SOFT_CAP，应被账号级地板 0.3 封住
    for _ in range(20):
        acc._stats._picks.append(now)
    factor = ModelClientPool._frequency_factor(acc)
    assert factor == 0.3


def test_frequency_factor_linear_decay_between_cap_and_floor():
    acc = _make_account()
    now = time.time()
    # 账号级仍为线性：超额 1 次：1.0 - 1*0.15 = 0.85
    for _ in range(ModelClientPool.PICK_SOFT_CAP + 1):
        acc._stats._picks.append(now)
    assert ModelClientPool._frequency_factor(acc) == 0.85


def test_frequency_factor_recovers_after_window_expires():
    acc = _make_account()
    now = time.time()
    # 全部是过期时间戳
    expired = now - (ModelClientPool.PICK_WINDOW_SECONDS + 5)
    for _ in range(20):
        acc._stats._picks.append(expired)
    assert ModelClientPool._frequency_factor(acc) == 1.0
    # 过期时间戳被顺手清理
    assert acc._stats._picks == []


def test_provider_frequency_exponential_decay_thresholds():
    """渠道级第 5 次开始明显指数降权，第 8 次触底附近。"""
    now = time.time()

    factor, _ = ModelClientPool._provider_pick_window_factor(
        [now] * (ModelClientPool.PROVIDER_PICK_THRESHOLD - 1), now
    )
    assert factor == 1.0

    factor, _ = ModelClientPool._provider_pick_window_factor(
        [now] * ModelClientPool.PROVIDER_PICK_THRESHOLD, now
    )
    assert factor == pytest.approx(0.55)

    factor, _ = ModelClientPool._provider_pick_window_factor(
        [now] * (ModelClientPool.PROVIDER_PICK_THRESHOLD + 1), now
    )
    assert factor == pytest.approx(0.55 ** 2)

    factor, _ = ModelClientPool._provider_pick_window_factor([now] * 20, now)
    assert factor == ModelClientPool.PROVIDER_PICK_FLOOR


def test_frequency_factor_penalizes_provider_even_when_account_is_fresh():
    """账号级未超限，但渠道级命中达到阈值时，整体 factor 立即明显降权。"""
    acc = _make_account()
    now = time.time()
    ModelClientPool._provider_recent_picks = {
        "p1": [now for _ in range(ModelClientPool.PROVIDER_PICK_THRESHOLD)]
    }
    assert ModelClientPool._frequency_factor(acc, "p1") == pytest.approx(0.55)


def test_provider_frequency_prevents_multi_account_bypass():
    """同一渠道两个账号轮转，账号级都低于软上限，渠道级超限仍指数降权。
    这是用户报告"重复选同一渠道一直发请求"根因的回归测试。"""
    acc1 = _make_account(username="A")
    acc2 = _make_account(username="B")
    now = time.time()
    acc1._stats._picks = [now] * 3
    acc2._stats._picks = [now] * 2
    ModelClientPool._provider_recent_picks = {
        "p1": [now for _ in range(ModelClientPool.PROVIDER_PICK_THRESHOLD + 1)]
    }
    # 任一账号参与评分都会被渠道级惩罚压低：第 6 次约 0.3025
    assert ModelClientPool._frequency_factor(acc1, "p1") == pytest.approx(0.55 ** 2)
    assert ModelClientPool._frequency_factor(acc2, "p1") == pytest.approx(0.55 ** 2)


def test_provider_frequency_recovers_after_window_expires():
    """渠道级过期命中会清理并恢复到 1.0，且空列表 key 被移除。"""
    acc = _make_account()
    now = time.time()
    expired = now - (ModelClientPool.PICK_WINDOW_SECONDS + 5)
    ModelClientPool._provider_recent_picks = {"p1": [expired for _ in range(20)]}
    assert ModelClientPool._frequency_factor(acc, "p1") == 1.0
    assert "p1" not in ModelClientPool._provider_recent_picks


def test_frequency_factor_min_of_account_and_provider():
    """账号级和渠道级都超限时，取更严的一方。"""
    acc = _make_account()
    now = time.time()
    # 账号级 20 次（封底 0.3），渠道级第 6 次（0.3025）→ min = 0.3
    acc._stats._picks = [now for _ in range(20)]
    ModelClientPool._provider_recent_picks = {
        "p1": [now for _ in range(ModelClientPool.PROVIDER_PICK_THRESHOLD + 1)]
    }
    assert ModelClientPool._frequency_factor(acc, "p1") == 0.3


def test_provider_recent_usage_score_buckets():
    """最近轮次分档：最近 3 次正常，最近 5/10/20 命中过多时逐级降到概率档。"""
    now = time.time()
    base = 100.0
    median = 50.0

    # 最近 3 次命中不直接降档
    ModelClientPool._provider_pick_history = [(now, "p1"), (now, "p2"), (now, "p1")]
    assert ModelClientPool._provider_recent_usage_score("p1", base, median, now) == base

    # 最近 5 次命中 >=3：权重打折
    ModelClientPool._provider_pick_history = [(now, p) for p in ["p1", "p2", "p1", "p3", "p1"]]
    assert ModelClientPool._provider_recent_usage_score("p1", base, median, now) == pytest.approx(base * 0.65)

    # 最近 10 次命中 >=5：进入低概率池，受 median cap 限制
    ModelClientPool._provider_pick_history = [(now, p) for p in ["p1", "p2", "p1", "p3", "p1", "p4", "p1", "p5", "p1", "p6"]]
    assert ModelClientPool._provider_recent_usage_score("p1", base, median, now) == pytest.approx(median * 0.35)

    # 最近 20 次命中 >=8：极小概率池，受更低 cap 限制
    ModelClientPool._provider_pick_history = [(now, p) for p in (["p1", "p2"] * 8 + ["p3", "p4", "p5", "p6"])]
    assert ModelClientPool._provider_recent_usage_score("p1", base, median, now) == pytest.approx(median * 0.12)


def test_provider_recent_usage_history_ttl_and_round_limit():
    now = time.time()
    expired = now - ModelClientPool.PROVIDER_USAGE_HISTORY_WINDOW_SECONDS - 1
    ModelClientPool._provider_pick_history = [(expired, "old")] * 5 + [(now, f"p{i % 3}") for i in range(25)]

    history = ModelClientPool._clean_provider_pick_history(now)

    assert len(history) == ModelClientPool.PROVIDER_USAGE_HISTORY_MAX_ROUNDS
    assert all(p != "old" for _t, p in history)


def test_try_reserve_records_provider_recent_pick_and_history(monkeypatch):
    """reserve 成功时同时记录账号级、渠道级命中和 provider 选择历史。"""

    class _StubAccountClient:
        def __init__(self):
            from rate_limiter import FailureWindow
            self.provider = type("P", (), {"PROVIDER_NAME": "p1"})()
            self._stats = FailureWindow()
            self.last_route_info = None

        async def reserve(self, routed_model, messages):
            return True

    acc = _StubAccountClient()
    candidate = {
        "provider_name": "p1",
        "account_client": acc,
        "routed_model": "m1",
        "route_info": {"score": 1.0},
    }
    monkeypatch.setattr(ModelClientPool, "_provider_recent_picks", {})
    monkeypatch.setattr(ModelClientPool, "_provider_pick_history", [])

    result, decision = asyncio.run(ModelClientPool._try_reserve(candidate, []))

    assert result is not None
    assert decision.allowed
    assert len(acc._stats._picks) == 1
    assert len(ModelClientPool._provider_recent_picks["p1"]) == 1
    assert len(ModelClientPool._provider_pick_history) == 1
    assert ModelClientPool._provider_pick_history[0][1] == "p1"


def test_provider_first_uses_provider_max_not_sum(monkeypatch):
    """同一 provider 多个候选时聚合分取 max，不因候选数量把 provider 权重线性放大。"""
    import rate_limiter as rl

    p1_a = {"provider_name": "p1", "account_client": object(), "route_info": {"score": 10.0}}
    p1_b = {"provider_name": "p1", "account_client": object(), "route_info": {"score": 10.0}}
    p2 = {"provider_name": "p2", "account_client": object(), "route_info": {"score": 15.0}}
    # 若 p1 用 sum 聚合则 p1=20，会在 target=11 时被选；用 max 聚合则 p1=10，p2=15，应选 p2。
    monkeypatch.setattr(rl.random, "uniform", lambda _a, _b: 11.0)

    picked = ModelClientPool._provider_first_weighted_pick([p1_a, p1_b, p2])

    assert picked is p2


def test_provider_first_recent_usage_bucket_can_shift_provider(monkeypatch):
    """最近轮次命中过多后，高基础分 provider 会降到概率档，不再靠高分碾压。"""
    import rate_limiter as rl

    now = time.time()
    ModelClientPool._provider_pick_history = [(now, p) for p in ["p1", "p2", "p1", "p3", "p1", "p4", "p1", "p5", "p1", "p6"]]
    p1 = {"provider_name": "p1", "account_client": object(), "route_info": {"score": 100.0}}
    p2 = {"provider_name": "p2", "account_client": object(), "route_info": {"score": 50.0}}
    # base scores median=75；p1 最近10轮命中5次 => min(45, 26.25)=26.25，p2=50；target=27 会跳过 p1 选 p2。
    monkeypatch.setattr(rl.random, "uniform", lambda _a, _b: 27.0)

    picked = ModelClientPool._provider_first_weighted_pick([p1, p2])

    assert picked is p2


def test_provider_first_then_account_weighted_pick(monkeypatch):
    """选定 provider 后，provider 内仍按账号候选分数带权选择。"""
    import rate_limiter as rl

    low = {"provider_name": "p1", "account_client": "low", "route_info": {"score": 1.0}}
    high = {"provider_name": "p1", "account_client": "high", "route_info": {"score": 9.0}}
    targets = iter([0.1, 8.0])  # 第一次选 provider；第二次在 provider 内选 high
    monkeypatch.setattr(rl.random, "uniform", lambda _a, _b: next(targets))

    picked = ModelClientPool._provider_first_weighted_pick([low, high])

    assert picked is high


def test_intelligent_score_drops_when_account_over_picked(monkeypatch):
    """同一账号在窗口内被选多次后，intelligent 评分的 frequency_score 下降，total 随之下降。"""
    acc = _make_account()
    monkeypatch.setattr(ModelClientPool, "_account_error_rate", classmethod(lambda cls, c: 0.0))
    monkeypatch.setattr(ModelClientPool, "_quota_factor", classmethod(lambda cls, c, m, *a, **k: 1.0))
    monkeypatch.setattr(ModelClientPool, "_headroom_factor", classmethod(lambda cls, c: 1.0))
    monkeypatch.setattr(ModelClientPool, "_channel_scores", {"p1": {"ttft_ewma_ms": 100}})

    fresh = ModelClientPool._intelligent_score("p1", acc, "m1")
    over_picked_score = fresh["score"]
    over_picked_freq = fresh["frequency_score"]

    now = time.time()
    for _ in range(10):
        acc._stats._picks.append(now)

    after = ModelClientPool._intelligent_score("p1", acc, "m1")
    assert after["frequency_score"] < over_picked_freq
    assert after["frequency_score"] == 0.3
    assert after["score"] < over_picked_score


def test_intelligent_score_drops_when_provider_over_picked(monkeypatch):
    """账号级清白，但渠道级命中达阈值时，intelligent 评分 frequency_score 也应明显下降。"""
    acc = _make_account()
    monkeypatch.setattr(ModelClientPool, "_account_error_rate", classmethod(lambda cls, c: 0.0))
    monkeypatch.setattr(ModelClientPool, "_quota_factor", classmethod(lambda cls, c, m, *a, **k: 1.0))
    monkeypatch.setattr(ModelClientPool, "_headroom_factor", classmethod(lambda cls, c: 1.0))
    monkeypatch.setattr(ModelClientPool, "_channel_scores", {"p1": {"ttft_ewma_ms": 100}})

    fresh = ModelClientPool._intelligent_score("p1", acc, "m1")
    now = time.time()
    ModelClientPool._provider_recent_picks = {
        "p1": [now for _ in range(ModelClientPool.PROVIDER_PICK_THRESHOLD)]
    }

    after = ModelClientPool._intelligent_score("p1", acc, "m1")
    assert after["frequency_score"] < fresh["frequency_score"]
    assert after["frequency_score"] == pytest.approx(0.55)
    assert after["score"] < fresh["score"]


def test_fast_intelligent_score_uses_provider_factor(monkeypatch):
    """fast_intelligent 同样应在渠道级超限时降权（保障调用点已传 provider_name）。"""
    acc = _make_account()
    monkeypatch.setattr(ModelClientPool, "_account_error_rate", classmethod(lambda cls, c: 0.0))
    monkeypatch.setattr(ModelClientPool, "_quota_factor", classmethod(lambda cls, c, m, *a, **k: 1.0))
    monkeypatch.setattr(ModelClientPool, "_headroom_factor", classmethod(lambda cls, c: 1.0))
    monkeypatch.setattr(ModelClientPool, "_channel_scores", {"p1": {"ttft_ewma_ms": 100}})

    fresh = ModelClientPool._fast_intelligent_score("p1", acc, "m1")
    now = time.time()
    ModelClientPool._provider_recent_picks = {
        "p1": [now for _ in range(ModelClientPool.PROVIDER_PICK_THRESHOLD)]
    }

    after = ModelClientPool._fast_intelligent_score("p1", acc, "m1")
    assert after["frequency_score"] < fresh["frequency_score"]
    assert after["frequency_score"] == pytest.approx(0.55)


def test_frequency_penalty_breaks_stickiness():
    """两个账号 A/B，A 静态分略高；接入账号级频率惩罚后 B 命中率显著上升。"""
    pa = _make_account(username="A", provider=type("P", (), {"PROVIDER_NAME": "p1", "username": "A"})())
    pb = _make_account(username="B", provider=type("P", (), {"PROVIDER_NAME": "p1", "username": "B"})())
    # A 的 speed 略优（更快的 TTFT），其余维度相同
    ModelClientPool._channel_scores = {"p1": {"ttft_ewma_ms": 100}}  # speed_score=1.0

    candidates = [
        {"provider_name": "p1", "account_client": pa, "routed_model": "m1", "route_info": {}},
        {"provider_name": "p1", "account_client": pb, "routed_model": "m1", "route_info": {}},
    ]

    a_hits = b_hits = 0
    now = time.time()
    for _ in range(50):
        # 模拟 A 已被连续选中多次（粘性场景）
        pa._stats._picks = [now] * 8
        pb._stats._picks = []
        picked = ModelClientPool._close_score_random_pick(_scored_copy(candidates))
        if picked["account_client"] is pa:
            a_hits += 1
        else:
            b_hits += 1

    # 频率惩罚后，B 应被显著选中（>30%）
    assert b_hits > 15, f"频率惩罚未生效：B 仅命中 {b_hits}/50"


def _scored_copy(candidates):
    """给候选打分（复用 intelligent 评分），返回带 route_info.score 的浅拷贝列表。"""
    out = []
    for c in candidates:
        info = ModelClientPool._intelligent_score(c["provider_name"], c["account_client"], c["routed_model"])
        cc = dict(c)
        cc["route_info"] = {"score": info["score"]}
        out.append(cc)
    return out
