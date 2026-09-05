"""智能选择短期信号 + 同上游模型亲和 单元测试：
- 同上游模型亲和因子（全局 upstream_model_id -> account）
- 短期成功率偏好因子（4分钟/10次窗口，报错触发）
- 短期响应均衡因子（最近一次远超中位数则降权）
- 全策略候选收缩 _prefer_same_upstream_model_candidates
- record_account_success 透传 upstream_model_id/duration_ms 写入运行时态
- 默认策略常量 == intelligent（config 与 db 两处）
- 回归：无新信号时因子全 1.0，评分退化为旧行为
"""
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from rate_limiter import AccountClient, ModelClientPool


def _make_provider(provider_name: str = "custom", username: str = "acct1") -> SimpleNamespace:
    p = SimpleNamespace()
    p.PROVIDER_NAME = provider_name
    p.username = username
    p.quotas = None
    return p


def _make_client(priority: int = 0, weight: int = 10, username: str = "acct1",
                 balance: float | None = None, balance_threshold: float = 0) -> AccountClient:
    provider = _make_provider("custom", username)
    return AccountClient(provider, rpm_limit=0, priority=priority, weight=weight,
                         balance=balance, balance_threshold=balance_threshold)


@pytest.fixture(autouse=True)
def _reset_runtime_state():
    """每个测试前后清理 ModelClientPool 的亲和/短期/session 运行时态。"""
    ModelClientPool._upstream_model_affinity.clear()
    ModelClientPool._short_term_outcomes.clear()
    ModelClientPool._session_affinity.clear()
    ModelClientPool._session_failures.clear()
    ModelClientPool._channel_scores.clear()
    ModelClientPool._provider_recent_picks.clear()
    ModelClientPool._provider_pick_history.clear()
    ModelClientPool._provider_billing_mode.clear()
    yield
    ModelClientPool._upstream_model_affinity.clear()
    ModelClientPool._short_term_outcomes.clear()
    ModelClientPool._session_affinity.clear()
    ModelClientPool._session_failures.clear()
    ModelClientPool._channel_scores.clear()
    ModelClientPool._provider_recent_picks.clear()
    ModelClientPool._provider_pick_history.clear()
    ModelClientPool._provider_billing_mode.clear()


# ── 一、同上游模型亲和因子 _upstream_model_affinity_factor ───────────

def test_upstream_model_affinity_matches_same_account():
    """upstream_model_id 最近被本账号成功命中且在 TTL 内 → 1.3。"""
    ModelClientPool.record_upstream_model_affinity("up-m1", "custom", "acct1")
    factor = ModelClientPool._upstream_model_affinity_factor("up-m1", "custom", "custom:acct1")
    assert factor == pytest.approx(ModelClientPool.UPSTREAM_MODEL_AFFINITY_FACTOR)


def test_upstream_model_affinity_different_account_no_boost():
    """同 upstream_model_id 但不同 account → 1.0（cache 命中要同账号）。"""
    ModelClientPool.record_upstream_model_affinity("up-m1", "custom", "acct1")
    factor = ModelClientPool._upstream_model_affinity_factor("up-m1", "custom", "custom:acct2")
    assert factor == 1.0


def test_upstream_model_affinity_expired_returns_baseline(monkeypatch):
    """TTL 外的亲和记录不升权 → 1.0。"""
    ModelClientPool.record_upstream_model_affinity("up-m1", "custom", "acct1")
    rec = ModelClientPool._upstream_model_affinity["up-m1"]
    rec["last_success_ts"] = time.time() - ModelClientPool.UPSTREAM_MODEL_AFFINITY_TTL - 1
    factor = ModelClientPool._upstream_model_affinity_factor("up-m1", "custom", "custom:acct1")
    assert factor == 1.0


def test_upstream_model_affinity_unknown_model_returns_baseline():
    factor = ModelClientPool._upstream_model_affinity_factor("unknown", "custom", "custom:acct1")
    assert factor == 1.0


def test_upstream_model_affinity_none_id_returns_baseline():
    assert ModelClientPool._upstream_model_affinity_factor(None, "custom", "custom:acct1") == 1.0


# ── 二、短期成功率偏好因子 _short_term_success_factor ──────────────

def test_short_term_success_no_failures_returns_baseline():
    """窗口内全成功（无报错触发）→ 1.0，不额外扰动。"""
    for _ in range(5):
        ModelClientPool.record_short_term_outcome("custom", "acct1", success=True, duration_ms=100)
    assert ModelClientPool._short_term_success_factor("custom:acct1") == 1.0


def test_short_term_success_with_failures_scales_by_rate():
    """窗口内有失败时，因子 = floor + (boost-floor)*success_rate，落在 [FLOOR, BOOST]。"""
    # 4 成功 1 失败 → success_rate=0.8
    for _ in range(4):
        ModelClientPool.record_short_term_outcome("custom", "acct1", success=True, duration_ms=100)
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=False, duration_ms=0)
    factor = ModelClientPool._short_term_success_factor("custom:acct1")
    expected = ModelClientPool.SHORT_TERM_SUCCESS_FLOOR + (ModelClientPool.SHORT_TERM_SUCCESS_BOOST - ModelClientPool.SHORT_TERM_SUCCESS_FLOOR) * 0.8
    assert factor == pytest.approx(expected)
    assert ModelClientPool.SHORT_TERM_SUCCESS_FLOOR <= factor <= ModelClientPool.SHORT_TERM_SUCCESS_BOOST


def test_short_term_success_all_failures_floor():
    """窗口内全失败 → success_rate=0 → 因子 = FLOOR。"""
    for _ in range(3):
        ModelClientPool.record_short_term_outcome("custom", "acct1", success=False, duration_ms=0)
    factor = ModelClientPool._short_term_success_factor("custom:acct1")
    assert factor == pytest.approx(ModelClientPool.SHORT_TERM_SUCCESS_FLOOR)


def test_short_term_success_caps_at_recent_10_samples():
    """超过 10 条只取最近 10 条计算成功率。注入 12 条（前 2 条失败，后 10 条全成功）→ 最近 10 全成功 → 1.0。"""
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=False, duration_ms=0)
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=False, duration_ms=0)
    for _ in range(10):
        ModelClientPool.record_short_term_outcome("custom", "acct1", success=True, duration_ms=100)
    factor = ModelClientPool._short_term_success_factor("custom:acct1")
    assert factor == 1.0


def test_short_term_success_no_samples_returns_baseline():
    assert ModelClientPool._short_term_success_factor("custom:acct1") == 1.0


# ── 三、短期响应均衡因子 _short_term_latency_factor ─────────────────

def _record_success_durations(account_key: str, durations_ms: list[int]):
    for d in durations_ms:
        ModelClientPool.record_short_term_outcome("custom", account_key.split(":")[-1], success=True, duration_ms=d)


def test_short_term_latency_recent_far_above_median_penalized():
    """最近一次成功耗时远超中位（>median*1.8）→ 降权 0.6。"""
    _record_success_durations("custom:acct1", [100, 110, 105, 120, 300])
    factor = ModelClientPool._short_term_latency_factor("custom:acct1")
    assert factor == pytest.approx(ModelClientPool.SHORT_TERM_LATENCY_PENALTY)


def test_short_term_latency_recent_within_median_no_penalty():
    """最近一次在正常范围 → 1.0。"""
    _record_success_durations("custom:acct1", [100, 110, 105, 120, 115])
    assert ModelClientPool._short_term_latency_factor("custom:acct1") == 1.0


def test_short_term_latency_insufficient_samples_returns_baseline():
    """样本不足（<2 条成功）→ 1.0。"""
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=True, duration_ms=100)
    assert ModelClientPool._short_term_latency_factor("custom:acct1") == 1.0


def test_short_term_latency_failures_only_no_duration_returns_baseline():
    _record_success_durations = None  # 占位，不记成功带耗时样本
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=False, duration_ms=0)
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=False, duration_ms=0)
    assert ModelClientPool._short_term_latency_factor("custom:acct1") == 1.0


def test_short_term_latency_absolute_slow_penalized():
    """最近一次成功耗时超绝对阈值(30s) -> 降权，堵「一直慢->中位也慢->相对分支不触发」漏洞。"""
    # 一直慢：中位 35s（>30s）但 last=36s 仅略高于中位，相对分支(36 > 35*1.8?)不触发；
    # 绝对分支(36s > 30000ms)触发。
    _record_success_durations("custom:acct1", [34000, 35000, 36000, 35000, 36000])
    factor = ModelClientPool._short_term_latency_factor("custom:acct1")
    assert factor == pytest.approx(ModelClientPool.SHORT_TERM_LATENCY_PENALTY)


# ── 三之二、绝对慢兜底 _absolute_slow_factor ──────────────────────────

def test_absolute_slow_factor_penalizes_slow_ttft():
    """渠道 TTFT EWMA 超绝对阈值 -> 重罚 ABSOLUTE_SLOW_PENALTY。"""
    ModelClientPool._channel_scores["custom"] = {"ttft_ewma_ms": 35000}
    assert ModelClientPool._absolute_slow_factor("custom", "custom:acct1") == pytest.approx(ModelClientPool.ABSOLUTE_SLOW_PENALTY)


def test_absolute_slow_factor_penalizes_slow_duration_median():
    """账号近期成功中位耗时超绝对阈值 -> 重罚（即使渠道 TTFT 未超标）。"""
    _record_success_durations("custom:acct1", [34000, 35000, 36000])
    assert ModelClientPool._absolute_slow_factor("custom", "custom:acct1") == pytest.approx(ModelClientPool.ABSOLUTE_SLOW_PENALTY)


def test_absolute_slow_factor_fast_channel_not_penalized():
    """正常快渠道（TTFT 与耗时都低于阈值）-> 1.0。"""
    ModelClientPool._channel_scores["custom"] = {"ttft_ewma_ms": 800}
    _record_success_durations("custom:acct1", [100, 110, 105])
    assert ModelClientPool._absolute_slow_factor("custom", "custom:acct1") == 1.0


# ── 四、全策略候选收缩 _prefer_same_upstream_model_candidates ────────

def _make_candidate(provider_name: str, username: str, upstream_model_id: str | None = None) -> dict:
    client = _make_client(username=username)
    client.provider.PROVIDER_NAME = provider_name
    return {
        "provider_name": provider_name,
        "account_client": client,
        "routed_model": "m1",
        "upstream_model_id": upstream_model_id,
        "route_info": {"upstream_model_id": upstream_model_id},
    }


def test_prefer_same_upstream_model_shrinks_to_affinity_match():
    """有亲和命中的候选时，收缩到同 upstream_model_id 同 account 的候选子集。"""
    ModelClientPool.record_upstream_model_affinity("up-A", "p1", "u1")
    c_affinity = _make_candidate("p1", "u1", "up-A")
    c_other = _make_candidate("p2", "u2", "up-B")
    out = ModelClientPool._prefer_same_upstream_model_candidates([c_affinity, c_other])
    assert out == [c_affinity]


def test_prefer_same_upstream_model_no_match_returns_all():
    """无亲和命中时返回原候选集，不饿死其他渠道。"""
    c1 = _make_candidate("p1", "u1", "up-X")
    c2 = _make_candidate("p2", "u2", "up-Y")
    out = ModelClientPool._prefer_same_upstream_model_candidates([c1, c2])
    assert out == [c1, c2]


def test_prefer_same_upstream_model_expired_match_returns_all():
    """亲和命中但 TTL 过期 → 不收缩。"""
    ModelClientPool.record_upstream_model_affinity("up-A", "p1", "u1")
    ModelClientPool._upstream_model_affinity["up-A"]["last_success_ts"] = time.time() - ModelClientPool.UPSTREAM_MODEL_AFFINITY_TTL - 1
    c_affinity = _make_candidate("p1", "u1", "up-A")
    c_other = _make_candidate("p2", "u2", "up-B")
    out = ModelClientPool._prefer_same_upstream_model_candidates([c_affinity, c_other])
    assert out == [c_affinity, c_other]


def test_prefer_same_upstream_model_account_mismatch_returns_all():
    """upstream_model_id 命中但 account 不同 → 不收缩（cache 命中要同账号）。"""
    ModelClientPool.record_upstream_model_affinity("up-A", "p1", "u1")
    c_diff_account = _make_candidate("p1", "u2", "up-A")
    c_other = _make_candidate("p2", "u3", "up-B")
    out = ModelClientPool._prefer_same_upstream_model_candidates([c_diff_account, c_other])
    assert out == [c_diff_account, c_other]


def test_prefer_same_upstream_model_empty_returns_empty():
    assert ModelClientPool._prefer_same_upstream_model_candidates([]) == []


# ── 五、record_account_success 透传写入运行时态 ────────────────────

def test_record_account_success_writes_upstream_model_affinity(monkeypatch):
    """record_account_success 透传 upstream_model_id 后亲和表被写入。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    ModelClientPool.record_account_success(
        "custom", "acct1", "m1",
        upstream_model_id="up-m1",
        duration_ms=250,
    )

    rec = ModelClientPool._upstream_model_affinity.get("up-m1")
    assert rec is not None
    assert rec["account_key"] == "custom:acct1"
    assert rec["last_success_ts"] > 0

    sample = ModelClientPool._short_term_samples("custom:acct1")
    assert sample and sample[-1]["success"] is True
    assert int(sample[-1]["duration_ms"]) == 250


def test_record_account_success_without_upstream_model_skips_affinity(monkeypatch):
    """不传 upstream_model_id 时亲和表不写入，但短期窗口仍记录成功。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    ModelClientPool.record_account_success("custom", "acct1", "m1", duration_ms=120)
    assert ModelClientPool._upstream_model_affinity == {}
    sample = ModelClientPool._short_term_samples("custom:acct1")
    assert sample and sample[-1]["success"] is True


def test_record_account_failure_records_short_term_failure(monkeypatch):
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")
    sample = ModelClientPool._short_term_samples("custom:acct1")
    assert sample and sample[-1]["success"] is False
    # 失败不记亲和
    assert ModelClientPool._upstream_model_affinity == {}


# ── 六、默认策略常量 == intelligent ─────────────────────────────────

def test_config_default_selection_strategy_is_intelligent():
    import config
    assert config.DEFAULT_SELECTION_STRATEGY == "intelligent"


def test_db_default_selection_strategy_is_intelligent():
    import db
    assert db.DEFAULT_SELECTION_STRATEGY == "intelligent"


# ── 七、回归：无新信号时因子全 1.0，评分退化为旧行为 ────────────────

def test_intelligent_score_new_signals_default_to_baseline():
    """无亲和/无短期窗口时，新因子均为 1.0 且出现在返回 dict 中。"""
    client = _make_client()
    info = ModelClientPool._intelligent_score("custom", client, "m1", upstream_model_id=None)
    assert info["upstream_model_score"] == 1.0
    assert info["short_term_success_score"] == 1.0
    assert info["short_term_latency_score"] == 1.0
    assert info["score"] > 0


def test_fast_intelligent_score_new_signals_default_to_baseline():
    client = _make_client()
    info = ModelClientPool._fast_intelligent_score("custom", client, "m1", upstream_model_id=None)
    assert info["upstream_model_score"] == 1.0
    assert info["short_term_success_score"] == 1.0
    assert info["short_term_latency_score"] == 1.0
    assert info["score"] > 0


def test_intelligent_score_affinity_upweights_above_baseline():
    """同账号同 upstream_model 亲和命中时，评分高于无亲和基线。"""
    client = _make_client()
    baseline = ModelClientPool._intelligent_score("custom", client, "m1", upstream_model_id=None)["score"]
    ModelClientPool.record_upstream_model_affinity("up-m1", "custom", "acct1")
    boosted = ModelClientPool._intelligent_score("custom", client, "m1", upstream_model_id="up-m1")["score"]
    assert boosted > baseline


def test_intelligent_score_short_term_latency_penalty_downweights():
    """近期响应远超中位数时，短期 latency 因子压低评分。"""
    client = _make_client(username="acct1")
    # 写入 4 条正常 + 1 条慢响应（最近一次）
    for d in (100, 110, 105, 120):
        ModelClientPool.record_short_term_outcome("custom", "acct1", success=True, duration_ms=d)
    ModelClientPool.record_short_term_outcome("custom", "acct1", success=True, duration_ms=300)
    # 基线用另一个账号（无窗口样本 → latency 因子 1.0）
    baseline = ModelClientPool._intelligent_score("custom", _make_client(username="baseline"), "m1")["score"]
    penalized = ModelClientPool._intelligent_score("custom", client, "m1")["score"]
    assert penalized < baseline


# ── 八、复用升权取最大值 + 连续成功让权 ─────────────────────────────

def test_reuse_affinity_uses_max_not_product_for_session_and_upstream():
    """session 亲和与 upstream 亲和同时命中时只取最大值，不做 1.3×1.3 连乘。"""
    client = _make_client()
    baseline = ModelClientPool._intelligent_score("custom", client, "m1", session_id="sess-1", upstream_model_id="up-m1")
    ModelClientPool.record_session_success("sess-1", "custom", "acct1", "m1")
    ModelClientPool.record_upstream_model_affinity("up-m1", "custom", "acct1")

    boosted = ModelClientPool._intelligent_score("custom", client, "m1", session_id="sess-1", upstream_model_id="up-m1")

    assert boosted["session_score"] == pytest.approx(ModelClientPool.SESSION_AFFINITY_FACTOR)
    assert boosted["upstream_model_score"] == pytest.approx(ModelClientPool.UPSTREAM_MODEL_AFFINITY_FACTOR)
    # reuse_score 取 session/upstream 亲和最大值（1.3），再被 INTELLIGENT_AFFINITY_CAP 封顶到 1.2
    assert boosted["reuse_score"] == pytest.approx(ModelClientPool.INTELLIGENT_AFFINITY_CAP)
    assert boosted["score"] == pytest.approx(baseline["score"] * ModelClientPool.INTELLIGENT_AFFINITY_CAP)


def test_success_rotation_factor_decays_after_grace():
    """连续成功超过 grace 后指数让权，并封底到 CONSECUTIVE_SUCCESS_FLOOR。"""
    client = _make_client()
    client._consecutive_successes = ModelClientPool.CONSECUTIVE_SUCCESS_GRACE
    assert ModelClientPool._success_rotation_factor(client) == 1.0

    client._consecutive_successes = ModelClientPool.CONSECUTIVE_SUCCESS_GRACE + 1
    assert ModelClientPool._success_rotation_factor(client) == pytest.approx(ModelClientPool.CONSECUTIVE_SUCCESS_DECAY)

    client._consecutive_successes = ModelClientPool.CONSECUTIVE_SUCCESS_GRACE + 20
    assert ModelClientPool._success_rotation_factor(client) == pytest.approx(ModelClientPool.CONSECUTIVE_SUCCESS_FLOOR)


def test_record_success_and_failure_maintain_consecutive_successes(monkeypatch):
    """成功 streak 成功+1，任意失败清零。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    ModelClientPool.record_account_success("custom", "acct1", "m1")
    ModelClientPool.record_account_success("custom", "acct1", "m1")
    assert client._consecutive_successes == 2

    ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")
    assert client._consecutive_successes == 0


def test_intelligent_strategy_keeps_full_candidates_for_soft_affinity():
    """智能策略不做同模型硬收缩，避免连续成功让权时无处切换；同模型优先由 reuse_score 软升权表达。"""
    ModelClientPool.record_upstream_model_affinity("up-A", "p1", "u1")
    c_affinity = _make_candidate("p1", "u1", "up-A")
    c_other = _make_candidate("p2", "u2", "up-B")

    out = ModelClientPool._prefer_same_upstream_model_candidates([c_affinity, c_other], strategy="intelligent")
    assert out == [c_affinity, c_other]


def test_sequential_strategy_still_hard_prefers_same_upstream_model():
    """非评分策略仍用候选收缩表达同模型优先。"""
    ModelClientPool.record_upstream_model_affinity("up-A", "p1", "u1")
    c_affinity = _make_candidate("p1", "u1", "up-A")
    c_other = _make_candidate("p2", "u2", "up-B")

    out = ModelClientPool._prefer_same_upstream_model_candidates([c_affinity, c_other], strategy="sequential")
    assert out == [c_affinity]
