"""智能选择增强的单元测试：连续失败指数降权+秒级熔断、计费滑动偏好+余量联动、session 亲和/反亲和。"""
import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from rate_limiter import (
    AccountClient,
    ModelClientPool,
    begin_routing_timing,
    end_routing_timing,
)


def _make_provider(provider_name: str = "custom", username: str = "acct1") -> SimpleNamespace:
    """构造最小可用的假 provider，满足 AccountClient 评分所需的属性访问。"""
    p = SimpleNamespace()
    p.PROVIDER_NAME = provider_name
    p.username = username
    p.quotas = None  # _quota_factor 读 daily_requests_remaining 时返回 None
    return p


def _make_client(priority: int = 0, weight: int = 10, balance: float | None = None,
                 balance_threshold: float = 0) -> AccountClient:
    provider = _make_provider()
    client = AccountClient(provider, rpm_limit=0, priority=priority, weight=weight,
                           balance=balance, balance_threshold=balance_threshold)
    return client


@pytest.fixture(autouse=True)
def _reset_session_state():
    """每个测试前后清理 ModelClientPool 的 session 状态，避免相互污染。"""
    ModelClientPool._session_affinity.clear()
    ModelClientPool._session_failures.clear()
    ModelClientPool._channel_scores.clear()
    ModelClientPool._provider_recent_picks.clear()
    ModelClientPool._provider_pick_history.clear()
    yield
    ModelClientPool._session_affinity.clear()
    ModelClientPool._session_failures.clear()
    ModelClientPool._channel_scores.clear()
    ModelClientPool._provider_recent_picks.clear()
    ModelClientPool._provider_pick_history.clear()


@pytest.mark.asyncio
async def test_model_group_records_candidate_collection_before_selection(monkeypatch):
    """模型组普通策略必须在选择前写入完整候选准备耗时。"""
    async def _is_group(model_id):
        return True

    async def _strategy(api_key):
        return "sequential"

    async def _candidate_models(cls, model_id, route=None, *, is_group=None):
        await asyncio.sleep(0.002)
        return ["member-1"]

    async def _filters(model_id):
        return set(), set()

    async def _volume(api_key):
        return 0.0

    async def _collect(cls, *args, **kwargs):
        await asyncio.sleep(0.002)
        return ([{"route_info": {}, "account_client": object(), "provider_name": "custom",
                  "routed_model": "member-1", "upstream_model_id": "member-1"}], [], {})

    async def _select(cls, candidates, messages, session_id=None):
        return candidates[0]

    async def _reserve(cls, candidate, messages, *, is_test=False, is_probe=False, token_estimate=0):
        from limits.rules import LimitDecision
        return (object(), "custom", object()), LimitDecision(True)

    monkeypatch.setattr("rate_limiter.config.Config.is_model_group", _is_group)
    monkeypatch.setattr("rate_limiter.config.Config.get_api_key_strategy", _strategy)
    monkeypatch.setattr("rate_limiter.config.Config.get_model_group_provider_filter", _filters)
    monkeypatch.setattr(ModelClientPool, "_candidate_models", classmethod(_candidate_models))
    monkeypatch.setattr(ModelClientPool, "_api_key_volume_factor", classmethod(lambda cls, key: _volume(key)))
    monkeypatch.setattr(ModelClientPool, "_collect_candidates", classmethod(_collect))
    monkeypatch.setattr(ModelClientPool, "_select_sequential", classmethod(_select))
    monkeypatch.setattr(ModelClientPool, "_try_reserve", classmethod(_reserve))

    timing = {}
    token = begin_routing_timing(timing)
    try:
        result = await ModelClientPool._get_available_client_with_provider("group-1")
    finally:
        end_routing_timing(token)

    assert result[1] == "custom"
    assert timing["candidate_collect_ms"] >= 2
    assert "strategy_select_ms" in timing


def test_consecutive_failures_exponential_decay():
    """连续失败 3 次，error_score 应乘 0.8**3；score 显著低于无失败账号。"""
    clean = _make_client()
    failing = _make_client()
    failing._consecutive_failures = 3

    clean_info = ModelClientPool._intelligent_score("custom", clean, "m1")
    fail_info = ModelClientPool._intelligent_score("custom", failing, "m1")

    assert fail_info["consecutive_factor"] == pytest.approx(0.8 ** 3)
    assert fail_info["score"] < clean_info["score"]
    assert fail_info["consecutive_failures"] == 3


def test_fast_intelligent_decays_more_aggressively():
    """fast_intelligent 用 0.7 衰减，比 intelligent 的 0.8 更激进。"""
    client = _make_client()
    client._consecutive_failures = 3
    intel = ModelClientPool._intelligent_score("custom", client, "m1")
    fast = ModelClientPool._fast_intelligent_score("custom", client, "m1")
    assert intel["consecutive_factor"] == pytest.approx(0.8 ** 3)
    assert fast["consecutive_factor"] == pytest.approx(0.7 ** 3)
    assert fast["consecutive_factor"] < intel["consecutive_factor"]


def test_circuit_breaker_after_threshold_failures():
    """连续失败 >=5 次触发秒级熔断，评分短路为 0。"""
    client = _make_client()
    client._consecutive_failures = 5
    client._failure_cooldown_until = time.time() + 30
    info = ModelClientPool._intelligent_score("custom", client, "m1")
    assert info["score"] == 0
    assert info.get("consecutive_cooldown") is True


def test_circuit_breaker_expires_and_recovers():
    """熔断到期后恢复评分（不再短路为 0）。"""
    client = _make_client()
    client._consecutive_failures = 5
    client._failure_cooldown_until = time.time() - 1  # 已过期
    info = ModelClientPool._intelligent_score("custom", client, "m1")
    assert info["score"] > 0
    assert "consecutive_cooldown" not in info


def test_record_account_failure_triggers_cooldown_at_threshold(monkeypatch):
    """record_account_failure 累加计数，>=5 次设置冷却时间戳。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    for _ in range(5):
        ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")

    assert client._consecutive_failures == 5
    assert client._failure_cooldown_until > time.time()
    assert client._failure_cooldown_until <= time.time() + 120


@pytest.mark.asyncio
async def test_record_account_failure_mirrors_window_circuit_to_freeze_path(monkeypatch):
    """窗口熔断在真实事件循环中应走账号冻结路径，使 Redis 冷却索引可被管理端统计。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    client = _make_client()
    freezes = []

    def _freeze_account(frozen_client, seconds, reason):
        freezes.append((frozen_client, seconds, reason))
        frozen_client.set_cooldown(seconds, reason)

    pool = SimpleNamespace(
        client_class=SimpleNamespace(__name__="CustomProvider"),
        clients=[client],
        _freeze_account=_freeze_account,
    )
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    for _ in range(5):
        ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")

    assert freezes
    frozen_client, seconds, reason = freezes[-1]
    assert frozen_client is client
    assert seconds > 0
    assert reason == "windowed_failures=5"
    assert client._cooldown_until > time.time()


def test_record_account_success_resets_consecutive_failures(monkeypatch):
    """成功清零连续失败计数。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    client._consecutive_failures = 3
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    ModelClientPool.record_account_success("custom", "acct1", "m1")

    assert client._consecutive_failures == 0


def test_non_account_failure_not_counted(monkeypatch):
    """参数错误等非账号级失败不计入 _consecutive_failures。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    client = _make_client()
    pool.clients = [client]
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})

    ModelClientPool.record_account_failure("custom", "acct1", reason="invalid_request_error: bad parameter")

    assert client._consecutive_failures == 0
    # 仍记入滑窗失败次数（error_rate 分子）
    assert len(client._recent_failure) == 1


# ── 二、计费滑动偏好 + 余量联动 ──────────────────────────────────

def test_billing_preference_short_messages_unchanged():
    """token 计费账号不依赖请求大小，恒为中性 1.0（不触碰 token 估算）。"""
    ModelClientPool._provider_billing_mode["custom"] = "token"
    msgs = [{"role": "user", "content": "hi"}]  # <500 字符走粗估
    factor = ModelClientPool._billing_preference("custom", msgs, "m1")
    # token 模式恒为 1.0：请求大小不改变 token 计费的性价比，无需估算 token
    assert factor == pytest.approx(1.0, abs=1e-9)


def test_billing_preference_continuous_and_monotonic():
    """token 账号恒为 1.0；request 账号偏好随 token 数单调递增。"""
    ModelClientPool._provider_billing_mode["tok"] = "token"
    ModelClientPool._provider_billing_mode["req"] = "request"

    # 构造不同长度消息，绕过 <500 粗估：用足够长内容
    def msgs_for(tokens_approx):
        # estimate_request_part_tokens 对长文本按 tiktoken 计；这里用字符数近似控制
        text = "x" * (tokens_approx * 4)
        return [{"role": "user", "content": text}]

    tok_factors = [ModelClientPool._billing_preference("tok", msgs_for(t), "m1") for t in (500, 1500, 3000, 6000)]
    req_factors = [ModelClientPool._billing_preference("req", msgs_for(t), "m1") for t in (500, 1500, 3000, 6000)]

    # token 模式恒定中性
    assert all(abs(f - 1.0) < 1e-9 for f in tok_factors), tok_factors
    # request 模式单调递增（大请求更偏好按次计费）
    assert all(req_factors[i] <= req_factors[i + 1] + 1e-9 for i in range(len(req_factors) - 1)), req_factors


def test_billing_preference_clamped():
    """超大请求偏好被钳制在区间内（token 模式下限 0.3，request 模式上限 2.0）。"""
    ModelClientPool._provider_billing_mode["tok"] = "token"
    ModelClientPool._provider_billing_mode["req"] = "request"
    big = [{"role": "user", "content": "x" * 100000}]
    assert ModelClientPool._billing_preference("tok", big, "m1") >= 0.3
    assert ModelClientPool._billing_preference("req", big, "m1") <= 2.0


def test_quota_factor_request_mode_low_remaining_protects():
    """按次计费账号余量 <20% 时 _quota_factor 被乘 0.3。"""
    ModelClientPool._provider_billing_mode["custom"] = "request"
    client = _make_client()

    # 注入 daily 配额：模拟 provider.quotas.dimension("requests_daily")
    dim = SimpleNamespace(name="requests_daily", limit=1000, remaining=100)  # 10% < 20%
    quotas = SimpleNamespace()
    quotas.dimension = lambda name: dim if name == "requests_daily" else None
    quotas.dimensions = lambda: [dim]
    quotas.daily_limit = 1000
    quotas.daily_remaining = 100
    client.provider.quotas = quotas

    factor = ModelClientPool._quota_factor(client, "m1", "custom")
    # 无 model_remaining 时 factor = 0.5 + min(1, 100/1000) = 0.6，再乘 0.3 = 0.18
    assert factor == pytest.approx(0.6 * 0.3, abs=1e-6)


def test_quota_factor_token_mode_no_protection():
    """token 计费账号低余量不触发 0.3 保护。"""
    ModelClientPool._provider_billing_mode["custom"] = "token"
    client = _make_client()
    dim = SimpleNamespace(name="requests_daily", limit=1000, remaining=100)
    quotas = SimpleNamespace()
    quotas.dimension = lambda name: dim if name == "requests_daily" else None
    quotas.dimensions = lambda: [dim]
    client.provider.quotas = quotas
    factor = ModelClientPool._quota_factor(client, "m1", "custom")
    assert factor == pytest.approx(0.6, abs=1e-6)


# ── 三、session 亲和 + 反亲和 ────────────────────────────────────

def _set_channel(provider: str, error_rate: float, requests: int = 20):
    """给渠道种下近期成功率样本，供全局避险模式按成功率加权。"""
    ModelClientPool._channel_scores[provider] = {
        "provider": provider,
        "recent_requests": requests,
        "recent_failures": int(requests * error_rate),
        "error_rate": error_rate,
        "updated_at": time.time(),
    }


def test_global_counter_neutral_below_trigger():
    """全局失败 1 次（< 阈值 2）不避险，仍为中性 1.0。"""
    client = _make_client()
    _set_channel("custom", error_rate=0.5)
    ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    assert ModelClientPool._session_anti_affinity_factor("sess-1", "custom", client) == 1.0


def test_global_counter_shared_across_everything():
    """单一全局计数器：不同 session、不同渠道、不同账号、无 session 的报错全累加到一起。"""
    client = _make_client()
    ModelClientPool.record_session_failure("sess-X", "custom", "acct1")
    ModelClientPool.record_session_failure("sess-Y", "openai", "acct2")  # 另一渠道+另一账号+另一 session
    bucket = ModelClientPool._session_failures[ModelClientPool._GLOBAL_FAILURE_BUCKET]
    # 计数器只有一个 key，累计 2 次
    assert list(bucket.keys()) == [ModelClientPool._GLOBAL_FAILURE_BUCKET]
    assert bucket[ModelClientPool._GLOBAL_FAILURE_BUCKET]["count"] == 2


def test_channel_and_account_failure_layers_count_once(monkeypatch):
    """同一次上游报错依次经过渠道层+账号层时，全局计数只加 1；渠道层是唯一入口。"""
    client = _make_client()
    pool = SimpleNamespace(
        channel=None,
        clients=[client],
        client_class=SimpleNamespace(__name__="CustomProvider"),
    )
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": pool})
    monkeypatch.setattr(ModelClientPool, "_incr_daily_outcome", classmethod(lambda *a, **k: None))

    ModelClientPool.record_channel_failure("custom", "acct1", "upstream 500")
    ModelClientPool.record_account_failure("custom", "acct1", "upstream 500")

    bucket = ModelClientPool._session_failures[ModelClientPool._GLOBAL_FAILURE_BUCKET]
    assert bucket[ModelClientPool._GLOBAL_FAILURE_BUCKET]["count"] == 1


def test_global_stress_prefers_high_success_channel():
    """避险后：高成功率渠道分数高于低成功率渠道（把流量导向成功率高的）。"""
    client = _make_client()
    _set_channel("good", error_rate=0.0)   # 100% 成功
    _set_channel("bad", error_rate=0.8)    # 20% 成功
    for _ in range(2):
        ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    good = ModelClientPool._session_anti_affinity_factor("sess-1", "good", client)
    bad = ModelClientPool._session_anti_affinity_factor("sess-1", "bad", client)
    assert good > bad
    assert good == pytest.approx(1.0)          # 成功率 100% 不被压
    assert bad == pytest.approx(ModelClientPool.GLOBAL_STRESS_FLOOR)  # 0.2**2=0.04，命中 0.05 地板


def test_global_stress_changes_both_selection_scores():
    """避险因子真正进入 intelligent / fast_intelligent 总分，不是只记录不生效。"""
    client = _make_client()
    _set_channel("good", error_rate=0.0)
    _set_channel("bad", error_rate=0.8)
    for _ in range(2):
        ModelClientPool.record_session_failure("sess-X", "whatever", "acct1")

    intelligent_good = ModelClientPool._intelligent_score("good", client, "m1")
    intelligent_bad = ModelClientPool._intelligent_score("bad", client, "m1")
    fast_good = ModelClientPool._fast_intelligent_score("good", client, "m1")
    fast_bad = ModelClientPool._fast_intelligent_score("bad", client, "m1")

    assert intelligent_good["global_stress_score"] > intelligent_bad["global_stress_score"]
    assert intelligent_good["score"] > intelligent_bad["score"]
    assert fast_good["global_stress_score"] > fast_bad["global_stress_score"]
    assert fast_good["score"] > fast_bad["score"]


def test_global_stress_deepens_with_more_failures():
    """全局失败次数越多，避险越狠（低成功率渠道被压得更低）。"""
    client = _make_client()
    _set_channel("bad", error_rate=0.5)
    for _ in range(2):
        ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    at2 = ModelClientPool._session_anti_affinity_factor("sess-1", "bad", client)
    ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    at3 = ModelClientPool._session_anti_affinity_factor("sess-1", "bad", client)
    assert at3 < at2


def test_global_stress_floor_not_starved():
    """极低成功率渠道被压到地板但不为 0（不完全饿死）。"""
    client = _make_client()
    _set_channel("terrible", error_rate=1.0)  # 0% 成功
    for _ in range(2):
        ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    factor = ModelClientPool._session_anti_affinity_factor("sess-1", "terrible", client)
    assert factor == pytest.approx(ModelClientPool.GLOBAL_STRESS_FLOOR)
    assert factor > 0


def test_global_stress_no_channel_data_uses_neutral_baseline():
    """无样本渠道不按 100% 成功算，用中性基线（避免冷渠道凭零错误碾压）。"""
    client = _make_client()
    _set_channel("proven", error_rate=0.0)
    for _ in range(2):
        ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    cold = ModelClientPool._session_anti_affinity_factor("sess-1", "never-used", client)
    proven = ModelClientPool._session_anti_affinity_factor("sess-1", "proven", client)
    assert cold == pytest.approx(ModelClientPool.GLOBAL_STRESS_NO_DATA_SUCCESS ** 2)
    assert cold < proven


def test_any_success_clears_global_counter():
    """任一请求成功（任意渠道/账号/session）清零全局计数，避险解除。"""
    client = _make_client()
    _set_channel("bad", error_rate=0.8)
    for _ in range(3):
        ModelClientPool.record_session_failure("sess-X", "custom", "acct1")
    assert ModelClientPool._session_anti_affinity_factor("sess-X", "bad", client) < 1.0
    # 另一个 session、另一个渠道的成功也清零
    ModelClientPool.record_session_success("sess-Y", "openai", "acct9", "m1")
    assert ModelClientPool._session_anti_affinity_factor("sess-X", "bad", client) == 1.0


def test_global_counter_expires_after_ttl():
    """全局计数超过 TTL 后失效，恢复中性。"""
    client = _make_client()
    _set_channel("bad", error_rate=0.8)
    for _ in range(3):
        ModelClientPool.record_session_failure("sess-1", "custom", "acct1")
    bucket = ModelClientPool._session_failures[ModelClientPool._GLOBAL_FAILURE_BUCKET]
    bucket[ModelClientPool._GLOBAL_FAILURE_BUCKET]["last_ts"] = time.time() - ModelClientPool.SESSION_FAILURE_TTL - 1
    assert ModelClientPool._session_anti_affinity_factor("sess-1", "bad", client) == 1.0


def test_session_affinity_upweights():
    """session 上次成功在此账号，权重 ×1.3（亲和仍按 session 隔离，未改）。"""
    client = _make_client()
    sid = "sess-1"
    ModelClientPool.record_session_success(sid, "custom", "acct1", "m1")
    factor = ModelClientPool._session_factor(sid, "custom", client)
    assert factor == pytest.approx(1.3)


def test_session_affinity_isolated_between_sessions():
    """亲和仍按 session 隔离：sess-1 的成功不给 sess-2 升权。"""
    client = _make_client()
    ModelClientPool.record_session_success("sess-1", "custom", "acct1", "m1")
    assert ModelClientPool._session_affinity_factor("sess-2", "custom", client) == 1.0


def test_session_affinity_expires():
    """亲和 TTL 过期后失效。"""
    client = _make_client()
    sid = "sess-1"
    ModelClientPool.record_session_success(sid, "custom", "acct1", "m1")
    # 手动老化记录
    ModelClientPool._session_affinity[sid]["last_success_ts"] = time.time() - ModelClientPool.SESSION_AFFINITY_TTL - 1
    factor = ModelClientPool._session_factor(sid, "custom", client)
    assert factor == 1.0


def test_no_session_returns_neutral():
    """无 session_id 且无全局失败记录时 session_factor 恒为 1.0。"""
    client = _make_client()
    assert ModelClientPool._session_factor(None, "custom", client) == 1.0


def test_failure_counts_without_session_header():
    """无 session 头的请求也计入全局计数（不再早退跳过）。"""
    client = _make_client()
    _set_channel("bad", error_rate=0.8)
    for _ in range(2):
        ModelClientPool.record_session_failure(None, "custom", "acct1")
    assert ModelClientPool._session_anti_affinity_factor(None, "bad", client) < 1.0
    ModelClientPool.record_session_success(None, "custom", "acct1", "m1")
    assert ModelClientPool._session_anti_affinity_factor(None, "bad", client) == 1.0


def test_cleanup_session_state_removes_expired():
    """cleanup_session_state 清理过期亲和与过期全局失败计数。"""
    sid = "sess-1"
    ModelClientPool.record_session_success(sid, "custom", "acct1", "m1")
    ModelClientPool._session_affinity[sid]["last_success_ts"] = time.time() - ModelClientPool.SESSION_AFFINITY_TTL - 1
    ModelClientPool.record_session_failure(sid, "custom", "acct2")
    bucket = ModelClientPool._session_failures[ModelClientPool._GLOBAL_FAILURE_BUCKET]
    bucket[ModelClientPool._GLOBAL_FAILURE_BUCKET]["last_ts"] = time.time() - ModelClientPool.SESSION_FAILURE_TTL - 1

    ModelClientPool.cleanup_session_state()

    assert sid not in ModelClientPool._session_affinity
    assert ModelClientPool._GLOBAL_FAILURE_BUCKET not in ModelClientPool._session_failures


# ── 四、窗口式短冷却（1-2 次失败即降权，间歇失败也触发） ───────────

def _pool_with(client, provider="custom"):
    """构造一个带 client_class.__name__ 的假 pool，供 record_account_* 通过 _score_enabled/_find_account_client。"""
    pool = SimpleNamespace()
    pool.client_class = SimpleNamespace(__name__="CustomProvider")
    pool.clients = [client]
    return pool


def test_windowed_circuit_triggers_at_two_failures(monkeypatch):
    """2 次账号级失败即触发短冷却（不再要求连续 5 次）。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    client = _make_client()
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": _pool_with(client)})
    monkeypatch.setattr(ModelClientPool, "_incr_daily_outcome", classmethod(lambda *a, **k: None))

    ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")
    # 1 次不触发（阈值 2）
    assert client._failure_cooldown_until == 0
    ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 5xx")
    assert client._failure_cooldown_until > time.time()
    assert client._failure_cooldown_until <= time.time() + ModelClientPool.WINDOW_CIRCUIT_MAX


def test_intermittent_failures_still_trigger_cooldown(monkeypatch):
    """失败-成功-失败-成功-失败…间歇失败也累计窗口失败次数并触发冷却。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    client = _make_client()
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": _pool_with(client)})
    monkeypatch.setattr(ModelClientPool, "_incr_daily_outcome", classmethod(lambda *a, **k: None))

    ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")
    ModelClientPool.record_account_success("custom", "acct1")   # 中间成功（清零连续，但窗口失败不清）
    ModelClientPool.record_account_failure("custom", "acct1", reason="timeout")
    # 2 次窗口内失败已触发，即便中间成功过
    assert client._failure_cooldown_until > time.time()
    assert len(client._circuit_failures) == 2


def test_cooldown_short_circuits_score_to_zero():
    """冷却期内评分短路为 0；到期恢复但 reliability 仍压低（解冻 ≠ 回满血）。"""
    client = _make_client()
    client._consecutive_failures = 2
    client._failure_cooldown_until = time.time() + 30
    info = ModelClientPool._intelligent_score("custom", client, "m1", reliability_factor=0.5)
    assert info["score"] == 0
    assert info.get("consecutive_cooldown") is True

    client._failure_cooldown_until = time.time() - 1  # 解冻
    after = ModelClientPool._intelligent_score("custom", client, "m1", reliability_factor=0.5)
    assert after["score"] > 0
    # 可靠性乘子仍生效：与 reliability=1.0 相比分数约为一半
    full = ModelClientPool._intelligent_score("custom", client, "m1", reliability_factor=1.0)
    assert after["score"] < full["score"]
    assert after["score"] == pytest.approx(full["score"] * 0.5, rel=1e-6)


# ── 五、长期可靠性因子（Redis 日级成功率） ──────────────────────────

def test_reliability_low_samples_returns_neutral(monkeypatch):
    """当天样本不足 MIN_SAMPLES 返回中性 1.0，不压新账号。"""
    async def fake_get(key, cal):
        kind = key.rsplit(":", 1)[-1]
        return {"success": 2, "failure": 1}.get(kind, 0)
    monkeypatch.setattr("limits.backend.RedisLimitBackend.get_calendar_count", classmethod(lambda cls, key, cal: fake_get(key, cal)))
    factor = __import__("asyncio").run(ModelClientPool._account_daily_reliability("custom", "acct1"))
    assert factor == 1.0


def test_reliability_reflects_daily_success_rate(monkeypatch):
    """succ=8 fail=2 → reliability≈0.8；全失败 → 触底 RELIABILITY_FLOOR。"""
    async def fake_get(key, cal):
        kind = key.rsplit(":", 1)[-1]
        return {"success": 8, "failure": 2}.get(kind, 0)
    monkeypatch.setattr("limits.backend.RedisLimitBackend.get_calendar_count", classmethod(lambda cls, key, cal: fake_get(key, cal)))
    asyncio = __import__("asyncio")
    factor = asyncio.run(ModelClientPool._account_daily_reliability("custom", "acct1"))
    assert factor == pytest.approx(0.8, abs=1e-6)


def test_reliability_floor_when_all_fail(monkeypatch):
    async def fake_get(key, cal):
        kind = key.rsplit(":", 1)[-1]
        return 10 if kind == "failure" else 0
    monkeypatch.setattr("limits.backend.RedisLimitBackend.get_calendar_count", classmethod(lambda cls, key, cal: fake_get(key, cal)))
    factor = __import__("asyncio").run(ModelClientPool._account_daily_reliability("custom", "acct1"))
    assert factor == pytest.approx(ModelClientPool.RELIABILITY_FLOOR)


def test_fast_score_downweights_flaky_fast_channel():
    """fast 策略：速度最快但可靠性低的渠道，分数不应碾压慢但稳定的渠道（乘性可靠性压制）。"""
    ModelClientPool._channel_scores = {
        "fast": {"ttft_ewma_ms": 100},    # 最快
        "stable": {"ttft_ewma_ms": 1000},  # 慢
    }
    flaky = _make_client()
    stable = _make_client()
    # 候选集最快 ttft=100 → fast 的 speed_score=1.0, stable=100/1000=0.1
    flaky_info = ModelClientPool._fast_intelligent_score("fast", flaky, "m1", reliability_factor=0.2, ttft_min=100)
    stable_info = ModelClientPool._fast_intelligent_score("stable", stable, "m1", reliability_factor=1.0, ttft_min=100)
    # 快但 flaky（可靠 0.2）不应明显碾压慢但稳定
    assert flaky_info["speed_score"] == 1.0
    assert stable_info["speed_score"] == 0.1
    # 乘性可靠性把 flaky 分数压到与稳定渠道同一量级甚至更低
    assert flaky_info["score"] <= stable_info["score"] * 2


def test_relative_speed_ranking(monkeypatch):
    """有 ttft_min 时速度分按相对排名：最快=1.0，5 倍慢≈0.2。"""
    ModelClientPool._channel_scores = {
        "p1": {"ttft_ewma_ms": 100},
        "p2": {"ttft_ewma_ms": 500},
        "p3": {"ttft_ewma_ms": 1000},
    }
    assert ModelClientPool._speed_score("p1", 100) == 1.0
    assert ModelClientPool._speed_score("p2", 100) == pytest.approx(100 / 500)
    assert ModelClientPool._speed_score("p3", 100) == pytest.approx(100 / 1000)
    # 下限 0.1
    assert ModelClientPool._speed_score("p3", 50) == 0.1


# ── 六、探索 epsilon（次优候选不被饿死） ─────────────────────────────

def test_exploration_epsilon_keeps_second_choice_alive():
    """带权随机中次优 provider 权重不低于 top*EXPLORE_EPSILON，跑多次仍能被选中。"""
    pa = _make_client()
    pb = _make_client()
    pa.provider.PROVIDER_NAME = "p1"
    pb.provider.PROVIDER_NAME = "p2"
    ModelClientPool._channel_scores = {"p1": {"ttft_ewma_ms": 100}, "p2": {"ttft_ewma_ms": 5000}}
    candidates = [
        {"provider_name": "p1", "account_client": pa, "routed_model": "m1", "route_info": {}},
        {"provider_name": "p2", "account_client": pb, "routed_model": "m1", "route_info": {}},
    ]
    p2_hits = 0
    for _ in range(200):
        picked = ModelClientPool._provider_first_weighted_pick([
            {**c, "route_info": {"score": ModelClientPool._intelligent_score(
                c["provider_name"], c["account_client"], c["routed_model"], None, 1.0,
                ModelClientPool._candidate_ttft_min(candidates))["score"]}} for c in candidates
        ])
        if picked["provider_name"] == "p2":
            p2_hits += 1
    # p2 明显劣势，但探索下限保证它仍偶尔命中（远高于 0）
    assert p2_hits > 0


# ── 七、record_account_* 异步写 Redis 日级计数 ──────────────────────

def test_record_failure_increments_daily_count(monkeypatch):
    """record_account_failure 合并累加日失败计数，由批量 flush 落 Redis；不可用时静默降级。"""
    calls = []
    async def fake_batch_incr(increments, cal):
        calls.append((dict(increments), cal))
    monkeypatch.setattr(
        "limits.backend.RedisLimitBackend.batch_incr_calendar_counts",
        classmethod(lambda cls, *a, **k: fake_batch_incr(*a, **k)),
    )
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    client = _make_client()
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"custom": _pool_with(client)})
    monkeypatch.setattr(ModelClientPool, "_reliability_pending", {})
    monkeypatch.setattr(ModelClientPool, "_reliability_flush_task", None)

    asyncio = __import__("asyncio")
    async def run():
        ModelClientPool.record_account_failure("custom", "acct1", reason="upstream 429")
        # 事件先进内存缓冲，flush 后才落 Redis
        assert ModelClientPool._reliability_pending.get("routing:daily:custom:acct1:failure") == 1
        await ModelClientPool._flush_reliability_counts()
    asyncio.run(run())
    assert any("routing:daily:custom:acct1:failure" in batch for batch, _cal in calls)


def test_reliability_queue_drops_when_full(monkeypatch):
    """缓冲达上限后丢弃新 key 的增量，不无限增长（Redis 故障时不反压主链路）。"""
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, name: True))
    monkeypatch.setattr(ModelClientPool, "RELIABILITY_QUEUE_MAX", 2)
    monkeypatch.setattr(ModelClientPool, "_reliability_pending", {})
    monkeypatch.setattr(ModelClientPool, "_reliability_dropped", 0)
    monkeypatch.setattr(ModelClientPool, "_reliability_flush_task", None)
    monkeypatch.setattr(ModelClientPool, "_ensure_reliability_flush_task", classmethod(lambda cls: None))

    asyncio = __import__("asyncio")
    async def run():
        for i in range(5):
            ModelClientPool._incr_daily_outcome("custom", f"acct{i}", success=True)
        # 已满后仍能对已存在 key 继续合并
        ModelClientPool._incr_daily_outcome("custom", "acct0", success=True)
    asyncio.run(run())

    assert len(ModelClientPool._reliability_pending) == 2
    assert ModelClientPool._reliability_dropped == 3
    assert ModelClientPool._reliability_pending["routing:daily:custom:acct0:success"] == 2


# ── 八、fast_intelligent 间歇性探索轮 ────────────────────────────────

def test_explore_round_picks_low_score_providers_uniformly():
    """explore=True 时忽略速度权重，池内 provider 均匀随机——低分渠道也能被选中。

    构造一个明显高分渠道 + 两个明显低分渠道；正常轮次几乎只选高分渠道，
    探索轮里三者应大致均匀（每个都拿到可观份额），证明"锁死"被打破。
    """
    fast = _make_client()
    slow1 = _make_client()
    slow2 = _make_client()
    fast.provider.PROVIDER_NAME = "fast"
    slow1.provider.PROVIDER_NAME = "slow1"
    slow2.provider.PROVIDER_NAME = "slow2"
    candidates = [
        {"provider_name": "fast", "account_client": fast, "routed_model": "m1", "route_info": {"score": 0.9}},
        {"provider_name": "slow1", "account_client": slow1, "routed_model": "m1", "route_info": {"score": 0.1}},
        {"provider_name": "slow2", "account_client": slow2, "routed_model": "m1", "route_info": {"score": 0.1}},
    ]
    hits = {"fast": 0, "slow1": 0, "slow2": 0}
    for _ in range(600):
        picked = ModelClientPool._provider_first_weighted_pick(candidates, explore=True)
        hits[picked["provider_name"]] += 1
    # 均匀随机：每个 provider 都拿到明显份额（远离 0），且不再被高分碾压
    assert hits["slow1"] > 100
    assert hits["slow2"] > 100
    assert hits["fast"] > 100


def test_normal_round_still_prefers_high_score():
    """非探索轮（explore 默认 False）仍速度优先：高分渠道占绝对多数。"""
    fast = _make_client()
    slow = _make_client()
    fast.provider.PROVIDER_NAME = "fast"
    slow.provider.PROVIDER_NAME = "slow"
    candidates = [
        {"provider_name": "fast", "account_client": fast, "routed_model": "m1", "route_info": {"score": 0.9}},
        {"provider_name": "slow", "account_client": slow, "routed_model": "m1", "route_info": {"score": 0.1}},
    ]
    fast_hits = 0
    for _ in range(400):
        picked = ModelClientPool._provider_first_weighted_pick(candidates)
        if picked["provider_name"] == "fast":
            fast_hits += 1
    # 高分渠道应拿到绝大多数（探索下限只给 slow 极小概率）
    assert fast_hits > 300

def test_fast_intelligent_normal_round_avoids_last_provider():
    """fast_intelligent 正常轮避让上一轮命中的渠道：构造两渠道，记录上次命中 fast，
    断言本轮（非探索）pick 不再落到 fast。验证 _avoid_last_provider 接入正常轮。"""
    fast = _make_client()
    slow = _make_client()
    fast.provider.PROVIDER_NAME = "fast"
    slow.provider.PROVIDER_NAME = "slow"
    ModelClientPool._provider_pick_history.append((time.time(), "fast"))
    candidates = [
        {"provider_name": "fast", "account_client": fast, "routed_model": "m1", "route_info": {"score": 0.9}},
        {"provider_name": "slow", "account_client": slow, "routed_model": "m1", "route_info": {"score": 0.1}},
    ]
    fast_hits = 0
    for _ in range(50):
        avoided = ModelClientPool._avoid_last_provider(candidates)
        picked = ModelClientPool._provider_first_weighted_pick(avoided)
        if picked["provider_name"] == "fast":
            fast_hits += 1
    assert fast_hits == 0


def test_avoid_last_provider_falls_back_when_single_provider():
    """仅剩单渠道时 _avoid_last_provider 回退原候选，不饿死。"""
    only = _make_client()
    only.provider.PROVIDER_NAME = "only"
    ModelClientPool._provider_pick_history.append((time.time(), "only"))
    candidates = [
        {"provider_name": "only", "account_client": only, "routed_model": "m1", "route_info": {"score": 0.9}},
    ]
    avoided = ModelClientPool._avoid_last_provider(candidates)
    assert len(avoided) == 1
    assert avoided[0]["provider_name"] == "only"


