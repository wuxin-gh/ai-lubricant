"""统一入口 / is_test 选账号层补测。

方案 `docs/账号测试-统一入口方案.md` 第四节「补测」要求：
- 显式白名单生效；
- account_whitelist 只命中指定账号；
- is_test=True 能选中冷却/禁用/冻结账号且失败不冻结、不重试；
- 测试落库带 router_request_path；
- 媒体测试路径。

本文件聚焦选账号层 `_collect_candidates` 的过滤行为（白名单命中、is_test 绕过
渠道 enabled / 账号 disabled / frozen / cooldown）。retry / 健康度 / 落库 的
is_test 行为由 `_chat_with_retry_for_model` 等承载，已有 test_retry_policy 覆盖；
此处只验「选账号时该不该出现这个候选」。
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rate_limiter import ModelClientPool


async def _async_true():
    return True


class _StubAccountClient:
    """贴合真实 AccountClient 选账号契约的最小 stub。"""

    def __init__(self, username, *, disabled=False, is_frozen=False, cooldown_until=0, provider_name="p1"):
        self.username = username
        self.weight = 1
        self.priority = 0
        self._cooldown_until = cooldown_until
        # 冻结桶化后，临时冷却也算「处于冻结中」（is_frozen 为真），永久/临时靠
        # cooldown_remaining 区分。stub 必须遵守同一契约，否则选路过滤不到冷却账号。
        import time as _t
        self.is_frozen = bool(is_frozen) or (bool(cooldown_until) and cooldown_until > _t.time())
        self.disabled = disabled
        self._requests = []
        self._token_usages = []
        self.rpm_limit = 0
        self.tpm_limit = 0
        self.provider = type("P", (), {"PROVIDER_NAME": provider_name})()
        self.last_route_info = {}

    def cooldown_remaining(self, now=None):
        import time as _t
        now = now or _t.time()
        return max(0, int(self._cooldown_until - now)) if self._cooldown_until else 0

    def is_model_frozen(self, model_id):
        return False

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
        return True

    async def release(self):
        return None


def _channel_stub(enabled, tags=()):
    ch = type("Ch", (), {})()
    ch.enabled = enabled
    ch.tags = frozenset(tags)
    # 空协议候选 → _collect_candidates 回退到 [None]（走 provider 默认协议）
    ch.get_chat_protocol_candidates = lambda model, protocol: []
    return ch


def _patch_pool(monkeypatch, clients, *, channel=None, provider="p1"):
    """装一个最小 pool + 必要的类方法桩，让 _collect_candidates 能跑完。"""
    pool = type("Pool", (), {})()
    pool.clients = clients
    pool.channel = channel or _channel_stub(True)
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {provider: pool})
    # 渠道=内存对象后走 iter_model_candidates 内存扫描（不再有全局 _model_routes）。
    routes = {"m1": [{"provider": provider, "upstream_model_id": "m1"}]}
    monkeypatch.setattr(ModelClientPool, "iter_model_candidates", classmethod(
        lambda cls, model_id: ((r["provider"], cls._provider_pools.get(r["provider"]), r)
                               for r in routes.get(model_id, []))))
    monkeypatch.setattr(ModelClientPool, "has_model_route", classmethod(lambda cls, m: m in routes))
    monkeypatch.setattr(ModelClientPool, "all_model_ids", classmethod(lambda cls: set(routes)))
    # _model_tpm_available / _route_supports_operation 是 async
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _async_true()))
    monkeypatch.setattr(ModelClientPool, "_route_supports_operation", classmethod(lambda cls, route, model, op, snapshot=None: _async_true()))
    # 这两个是同步
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))
    monkeypatch.setattr(ModelClientPool, "_provider_supports_operation", classmethod(lambda cls, provider, name, op: True))


def _candidates(*, key_whitelist=None, key_blacklist=None, group_whitelist=None, group_blacklist=None, **kwargs):
    """跑一次 _collect_candidates，返回 (用户名列表, skip_reasons)。

    位置参数按 _collect_candidates 契约固定：model_id, candidate_models,
    exclude_accounts, messages, operation, group_whitelist, group_blacklist,
    key_whitelist, key_blacklist（route 及其后一律走关键字）。
    """
    candidates, _, skip = asyncio.run(ModelClientPool._collect_candidates(
        "m1", ["m1"], [], [], None, group_whitelist or set(), group_blacklist or set(),
        key_whitelist or set(), key_blacklist or set(), **kwargs
    ))
    return [c["account_client"].username for c in candidates], skip


def test_account_whitelist_restricts_to_specified_account(monkeypatch):
    a1 = _StubAccountClient("u1")
    a2 = _StubAccountClient("u2")
    _patch_pool(monkeypatch, [a1, a2])
    names, skip = _candidates(account_whitelist={"u2"}, is_test=False)
    assert names == ["u2"]
    assert skip.get("account_whitelist_missing") == 1


def test_account_whitelist_empty_means_no_filter(monkeypatch):
    """account_whitelist=None（空）不限制账号 —— 正常请求语义。"""
    a1 = _StubAccountClient("u1")
    a2 = _StubAccountClient("u2")
    _patch_pool(monkeypatch, [a1, a2])
    names, skip = _candidates(account_whitelist=None, is_test=False)
    assert names == ["u1", "u2"]
    assert "account_whitelist_missing" not in skip


def test_is_test_bypasses_cooldown(monkeypatch):
    # u1 冷却中（未来时间戳），u2 健康
    a1 = _StubAccountClient("u1", cooldown_until=9999999999)
    a2 = _StubAccountClient("u2")
    _patch_pool(monkeypatch, [a1, a2])
    # 正常：u1 被冷却过滤，只剩 u2
    names, skip = _candidates(is_test=False)
    assert names == ["u2"]
    assert skip.get("account_cooldown") == 1
    # is_test：冷却被绕过，u1 也被选中
    names, skip = _candidates(is_test=True)
    assert "u1" in names
    assert "account_cooldown" not in skip


def test_is_test_bypasses_disabled(monkeypatch):
    a1 = _StubAccountClient("u1", disabled=True)
    _patch_pool(monkeypatch, [a1])
    names, skip = _candidates(is_test=False)
    assert names == []
    assert skip.get("account_disabled") == 1
    names, _ = _candidates(is_test=True)
    assert names == ["u1"]


def test_is_test_bypasses_frozen(monkeypatch):
    a1 = _StubAccountClient("u1", is_frozen=True)
    _patch_pool(monkeypatch, [a1])
    names, skip = _candidates(is_test=False)
    assert names == []
    assert skip.get("account_frozen") == 1
    names, _ = _candidates(is_test=True)
    assert names == ["u1"]


def test_is_test_bypasses_channel_disabled(monkeypatch):
    a1 = _StubAccountClient("u1")
    _patch_pool(monkeypatch, [a1], channel=_channel_stub(enabled=False))
    # 正常：渠道禁用 → provider_disabled
    names, skip = _candidates(is_test=False)
    assert names == []
    assert skip.get("provider_disabled") == 1
    # is_test：渠道 enabled 过滤被绕过
    names, skip = _candidates(is_test=True)
    assert names == ["u1"]
    assert "provider_disabled" not in skip


def test_is_probe_bypasses_cooldown(monkeypatch):
    """定时检测（is_probe）绕过冷却过滤——探测目标正是异常账号。"""
    a1 = _StubAccountClient("u1", cooldown_until=9999999999)
    a2 = _StubAccountClient("u2")
    _patch_pool(monkeypatch, [a1, a2])
    names, skip = _candidates(is_probe=True)
    assert "u1" in names
    assert "account_cooldown" not in skip


def test_is_probe_bypasses_frozen(monkeypatch):
    """is_probe 绕过 is_frozen（含永久冻结状态位），探测成功后可解除该冻结。"""
    a1 = _StubAccountClient("u1", is_frozen=True)
    _patch_pool(monkeypatch, [a1])
    names, _ = _candidates(is_probe=True)
    assert names == ["u1"]


def test_is_probe_respects_disabled(monkeypatch):
    """is_probe 尊重账号 disabled（人工开关）——禁用账号不进入检测周期。"""
    a1 = _StubAccountClient("u1", disabled=True)
    _patch_pool(monkeypatch, [a1])
    names, skip = _candidates(is_probe=True)
    assert names == []
    assert skip.get("account_disabled") == 1


def test_is_probe_respects_channel_disabled(monkeypatch):
    """is_probe 尊重渠道 enabled=False（人工禁用）——禁用渠道不检测。"""
    a1 = _StubAccountClient("u1")
    _patch_pool(monkeypatch, [a1], channel=_channel_stub(enabled=False))
    names, skip = _candidates(is_probe=True)
    assert names == []
    assert skip.get("provider_disabled") == 1


def test_provider_tag_whitelist_filters_provider(monkeypatch):
    """正常请求的 provider_whitelist 按渠道标签匹配。"""
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True, {"stable"}))
    names, skip = _candidates(key_whitelist={"other"})
    assert names == []
    assert skip.get("api_key_provider_filtered") == 1


def test_provider_tag_blacklist_filters_provider(monkeypatch):
    """正常请求命中任意黑名单标签即排除。"""
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True, {"stable", "costly"}))
    names, skip = _candidates(key_blacklist={"costly"})
    assert names == []
    assert skip.get("api_key_provider_filtered") == 1


def test_provider_tag_whitelist_any_match_passes(monkeypatch):
    """白名单多个标签采用 OR，任意命中即可通过。"""
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True, {"stable"}))
    names, skip = _candidates(key_whitelist={"other", "stable"})
    assert names == ["u1"]
    assert "api_key_provider_filtered" not in skip


def test_provider_tag_blacklist_wins_over_whitelist(monkeypatch):
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True, {"stable", "costly"}))
    names, skip = _candidates(key_whitelist={"stable"}, key_blacklist={"costly"})
    assert names == []
    assert skip.get("api_key_provider_filtered") == 1


def test_provider_tag_whitelist_excludes_untagged_channel(monkeypatch):
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True))
    names, _ = _candidates(key_whitelist={"stable"})
    assert names == []


def test_api_key_and_model_group_tag_filters_are_both_required(monkeypatch):
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True, {"stable", "cn"}))
    names, _ = _candidates(key_whitelist={"stable"}, group_whitelist={"cn"})
    assert names == ["u1"]
    names, skip = _candidates(key_whitelist={"stable"}, group_whitelist={"overseas"})
    assert names == []
    assert skip.get("group_provider_filtered") == 1


def test_test_mode_provider_whitelist_remains_exact_name(monkeypatch):
    """渠道测试的显式名单仍按 provider name，不解释成标签。"""
    a1 = _StubAccountClient("u1", provider_name="p1")
    _patch_pool(monkeypatch, [a1], provider="p1", channel=_channel_stub(True, {"stable"}))
    names, skip = _candidates(key_whitelist={"p1"}, is_test=True)
    assert names == ["u1"]
    assert "api_key_provider_filtered" not in skip


def _make_candidate(username):
    return {
        "provider_name": "p1",
        "account_client": _StubAccountClient(username),
        "routed_model": "m1",
        "route_info": {"score": 0},
    }


def test_select_reserve_test_mode_forces_first_when_strategy_returns_none(monkeypatch):
    """评分全 0（冻结/冷却/余额不足）导致策略返回 None 时：
    - 正常模式：返回 None，记 selection_returned_none；
    - 测试模式：回退取当前有效集第一个强制预占，让请求真的打到目标账号。"""
    from rate_limiter import ModelClientPool

    async def zero_score_strategy(remaining, messages, session_id):
        # 模拟 _select_intelligent 在评分全 0 时的返回
        return None

    # 预占直接放行（测试关注的是"策略返回 None 后是否还能进入预占"）
    async def fake_try_reserve(candidate, messages, *, is_test=False, is_probe=False, token_estimate=0):
        from limits.rules import LimitDecision
        return (object(), "p1", candidate["account_client"]), LimitDecision(True, "")

    monkeypatch.setattr(ModelClientPool, "_try_reserve", classmethod(
        lambda cls, candidate, messages, *, is_test=False, is_probe=False, token_estimate=0:
        fake_try_reserve(candidate, messages, is_test=is_test, is_probe=is_probe)))

    # 正常模式：策略返回 None → 直接放弃
    reasons = {}
    result = asyncio.run(ModelClientPool._select_and_reserve_candidates(
        [_make_candidate("u1")], zero_score_strategy, None, None, reasons, is_test=False))
    assert result is None
    assert reasons.get("selection_returned_none") == 1

    # 测试模式：策略返回 None → 回退取第一个强制预占
    reasons = {}
    result = asyncio.run(ModelClientPool._select_and_reserve_candidates(
        [_make_candidate("u1")], zero_score_strategy, None, None, reasons, is_test=True))
    assert result is not None
    assert result[2].username == "u1"
    assert "selection_returned_none" not in reasons
