"""成功后自动解除冻结的单元测试。

覆盖 clear_runtime_freeze_on_success 的语义：
- 账号永久冻结状态（is_frozen）成功后清除；
- 账号级运行时冷却（_cooldown_until）成功后清除；
- 连续失败熔断（_failure_cooldown_until / _circuit_failures / _consecutive_failures）成功后归零；
- 仅成功模型的模型级冷却被清，其它模型冷却保留；
- 绝不动 disabled（账号开关关闭，禁用不是冻结）；
- 未冻结时返回 False，不做无谓广播；
- record_account_success 会顺带触发自动解冻。
"""
from __future__ import annotations

import time
from types import SimpleNamespace

from rate_limiter import AccountClient, ModelClientPool, ProviderPool


def _make_provider(provider_name: str = "custom", username: str = "acct1") -> SimpleNamespace:
    p = SimpleNamespace()
    p.PROVIDER_NAME = provider_name
    p.username = username
    p.quotas = None
    return p


def _make_pool(provider_name: str = "custom", username: str = "acct1") -> tuple[ProviderPool, AccountClient]:
    pool = ProviderPool(provider_name, object)
    client = AccountClient(_make_provider(provider_name, username), rpm_limit=0)
    pool.clients = [client]
    return pool, client


def _reset_mirror():
    # 模型级冻结已收进 AccountClient._freeze_state；每个测试都会创建新池。
    return None


def test_account_cooldown_cleared_on_success():
    _reset_mirror()
    pool, client = _make_pool()
    client.set_cooldown(300, reason="freeze_policy:rate_limit")
    assert client._cooldown_until > time.time()

    changed = pool.clear_runtime_freeze_on_success("acct1")

    assert changed is True
    assert client._cooldown_until == 0
    assert client.cooldown_reason == ""


def test_failure_stats_do_not_create_freeze_on_success():
    _reset_mirror()
    pool, client = _make_pool()
    client._stats.record_failure(time.time(), account_level=True)
    client._stats.record_failure(time.time(), account_level=True)
    client._stats._consecutive_failures = 3

    changed = pool.clear_runtime_freeze_on_success("acct1")

    # 失败统计只参与评分，不是冻结态；成功路径无需“解冻”统计。
    assert changed is False
    assert client.is_frozen is False
    assert client._stats.consecutive_failures() == 3


def test_model_cooldown_cleared_only_for_succeeded_model():
    _reset_mirror()
    pool, client = _make_pool()
    now = time.time()
    # 两个模型都被冻结；只有 m1 成功
    client.freeze(kind="account_model", model_id="m1", seconds=300)
    client.freeze(kind="account_model", model_id="m2", seconds=300)

    changed = pool.clear_runtime_freeze_on_success("acct1", "m1")

    assert changed is True
    # m1 解冻，m2 冷却保留
    assert not client.is_model_frozen("m1")
    assert client.is_model_frozen("m2")


def test_account_cooldown_clears_all_model_cooldowns():
    _reset_mirror()
    pool, client = _make_pool()
    now = time.time()
    client.set_cooldown(300, reason="account")
    client.freeze(kind="account_model", model_id="m1", seconds=300)
    client.freeze(kind="account_model", model_id="m2", seconds=300)

    # 账号级冷却存在时，即便只成功了 m1，也证明整账号可达 → 全部模型镜像清除
    changed = pool.clear_runtime_freeze_on_success("acct1", "m1")

    assert changed is True
    assert client._cooldown_until == 0
    assert not client.is_model_frozen("m1")
    assert not client.is_model_frozen("m2")


def test_is_frozen_cleared():
    _reset_mirror()
    pool, client = _make_pool()
    client.freeze(kind="account", seconds=None)

    changed = pool.clear_runtime_freeze_on_success("acct1")

    # 即使没有附带 TTL 冷却，永久冻结本身也应被识别并解除。
    assert changed is True
    assert client.is_frozen is False


def test_disabled_not_touched():
    _reset_mirror()
    pool, client = _make_pool()
    client.disabled = True
    client.disable_reason = "switch_off"
    client.set_cooldown(300, reason="freeze")

    pool.clear_runtime_freeze_on_success("acct1")

    # 账号关闭（开关）不是冻结，绝不动
    assert client.disabled is True
    assert client.disable_reason == "switch_off"


def test_no_freeze_returns_false():
    _reset_mirror()
    pool, client = _make_pool()

    changed = pool.clear_runtime_freeze_on_success("acct1")

    assert changed is False


def test_unknown_username_returns_false():
    _reset_mirror()
    pool, _ = _make_pool()

    assert pool.clear_runtime_freeze_on_success("nobody") is False


def test_record_account_success_triggers_unfreeze(monkeypatch):
    _reset_mirror()
    pool, client = _make_pool()
    client.set_cooldown(300, reason="freeze")
    monkeypatch.setitem(ModelClientPool._provider_pools, "custom", pool)

    ModelClientPool.record_account_success("custom", "acct1", "m1")

    assert client._cooldown_until == 0
