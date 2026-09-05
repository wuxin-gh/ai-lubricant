"""账号规范状态枚举 AccountClient.state() 的单元测试。

覆盖 7 态及优先级
（disabled > frozen > cooling > model_frozen > auth_failed > checking > available），
以及模型级冻结的实时性（TTL 到期后同一方法自动改判）与冻结条目清理。
"""
from __future__ import annotations

import time
from types import SimpleNamespace

from rate_limiter import AccountClient, AccountState


def _make_client(username: str = "acct1") -> AccountClient:
    provider = SimpleNamespace(PROVIDER_NAME="custom", username=username, quotas=None)
    return AccountClient(provider, rpm_limit=0)


def test_state_available_when_auth_ok_and_not_frozen():
    c = _make_client()
    c.auth_ok = True
    assert c.state() is AccountState.AVAILABLE


def test_state_checking_when_auth_unknown():
    c = _make_client()
    c.auth_ok = None
    assert c.state() is AccountState.CHECKING


def test_state_auth_failed_when_auth_false():
    c = _make_client()
    c.auth_ok = False
    assert c.state() is AccountState.AUTH_FAILED


def test_state_cooling_for_temporary_freeze():
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account", seconds=300, reason="freeze_policy:today")
    # 临时冷却：is_frozen 为 True，但有剩余秒数 → cooling，不是永久 frozen。
    assert c.is_frozen is True
    assert c.cooldown_remaining() > 0
    assert c.state() is AccountState.COOLING


def test_state_frozen_for_permanent_freeze():
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account", seconds=None, reason="permanent")
    assert c.is_frozen is True
    assert c.cooldown_remaining() == 0
    assert c.state() is AccountState.FROZEN


def test_state_disabled_regardless_of_auth():
    c = _make_client()
    c.auth_ok = False
    c.disabled = True
    assert c.state() is AccountState.DISABLED


def test_disabled_takes_priority_over_frozen():
    """禁用优先于冻结：账号 switch 关掉后即便还留着永久冻结，也判 disabled（对齐路由）。"""
    c = _make_client()
    c.auth_ok = True
    c.disabled = True
    c.freeze(kind="account", seconds=None, reason="permanent")
    assert c.is_frozen is True
    assert c.state() is AccountState.DISABLED


def test_state_value_is_plain_string():
    """AccountState 是 str 枚举，.value 可直接进 JSON 响应。"""
    c = _make_client()
    c.auth_ok = True
    assert c.state().value == "available"
    assert AccountState.FROZEN.value == "frozen"
    assert AccountState.COOLING.value == "cooling"
    assert AccountState.MODEL_FROZEN.value == "model_frozen"


# ── 模型级冻结维度（第 7 态） ──


def test_state_model_frozen_when_only_a_model_is_frozen():
    """账号级未冻、单个模型被冻 → model_frozen（此前会被误判成 available）。"""
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account_model", model_id="m1", seconds=3600, reason="429")
    assert c.is_frozen is False
    assert c.frozen_model_ids() == ["m1"]
    assert c.state() is AccountState.MODEL_FROZEN


def test_account_cooling_takes_priority_over_model_frozen():
    """账号级冷却优先于模型级冻结（用户第 2 点：渠道冻结更短时先显示账号级）。"""
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account", seconds=300, reason="today")
    c.freeze(kind="account_model", model_id="m1", seconds=3600, reason="429")
    assert c.state() is AccountState.COOLING


def test_state_flips_to_model_frozen_when_account_ttl_expires(monkeypatch):
    """实时性：账号级 300s、模型级 3600s，时间推过 300s 后同一个 state() 自动改判。

    不需要任何刷新动作——状态是实时算出来的属性方法，这正是用户第 1 点的要求。
    """
    base = time.time()
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account", seconds=300, reason="today")
    c.freeze(kind="account_model", model_id="m1", seconds=3600, reason="429")
    assert c.state() is AccountState.COOLING

    import rate_limiter
    monkeypatch.setattr(rate_limiter.time, "time", lambda: base + 400)
    assert c.state() is AccountState.MODEL_FROZEN
    # 再推过模型 TTL → 回到 available
    monkeypatch.setattr(rate_limiter.time, "time", lambda: base + 4000)
    assert c.state() is AccountState.AVAILABLE


def test_frozen_model_ids_prunes_expired_entries(monkeypatch):
    """frozen_model_ids 惰性剔除已到期条目：过期条目此前只有 unfreeze 会删，会一直堆着。"""
    base = time.time()
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account_model", model_id="m1", seconds=60, reason="429")
    c.freeze(kind="account_model", model_id="m2", seconds=3600, reason="429")
    import rate_limiter
    monkeypatch.setattr(rate_limiter.time, "time", lambda: base + 120)
    assert c.frozen_model_ids() == ["m2"]
    assert "m1" not in c._freeze_state["models"]


def test_frozen_model_ids_sorted_by_soonest_expiry():
    """返回顺序按最快到期优先，探测封顶时先测最快恢复的模型；永久冻结排最后。"""
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account_model", model_id="slow", seconds=3600, reason="429")
    c.freeze(kind="account_model", model_id="fast", seconds=60, reason="429")
    c.freeze(kind="account_model", model_id="forever", seconds=None, reason="permanent")
    assert c.frozen_model_ids() == ["fast", "slow", "forever"]


def test_prune_freeze_for_models_removes_deleted_models():
    """模型从渠道模型表删除后，prune 清掉其冻结条目并返回被清列表（供调用方清 Redis）。"""
    c = _make_client()
    c.auth_ok = True
    c.freeze(kind="account_model", model_id="keep", seconds=3600, reason="429")
    c.freeze(kind="account_model", model_id="gone", seconds=3600, reason="429")
    removed = c.prune_freeze_for_models({"keep"})
    assert removed == ["gone"]
    assert c.frozen_model_ids() == ["keep"]
    # 只剩已删模型的冻结时，清完账号回到 available，不再有幽灵 model_frozen。
    assert c.prune_freeze_for_models(set()) == ["keep"]
    assert c.state() is AccountState.AVAILABLE


def test_model_frozen_ranks_above_auth_failed():
    """model_frozen 优先于 auth_failed：冻结是当下更具体的阻塞原因。"""
    c = _make_client()
    c.auth_ok = False
    c.freeze(kind="account_model", model_id="m1", seconds=3600, reason="429")
    assert c.state() is AccountState.MODEL_FROZEN
