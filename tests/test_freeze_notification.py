"""apply_freeze_policy 冻结通知的单元测试。

覆盖 _notify_freeze 的语义：
- 渠道级冻结（channel / channel_model）→ ``channel.frozen``（固定时长 error，
  禁用 critical）；账号级 → ``account.frozen``（warn）。
- 账号级原先「不发通知」以避免刷屏，现改为统一发事件：冻结是可订阅事件，刷屏
  由订阅规则（只订阅关心的渠道）+ dedupe_key 收敛，而不是在产生侧丢事件。
- 渠道级冻结影响全部账号，因此不归因到触发它的那个账号：``account_username``
  为 None，且 dedupe_key 不含 username（否则整渠道停摆会按账号拆成多条）。
- 同步无事件循环路径不抛异常、不调度通知（best-effort）。

断言落在 ``emit_notification`` 的入参而不是 ``notifications`` 表：通知中心是一种
渠道，事件是否进铃铛由订阅配置决定，产生侧只负责发事件。

冻结决策本身由 Channel.match_freeze 提供，这里 monkeypatch 它返回受控的
matched 字典，只验证 apply_freeze_policy 的通知分支，不重复覆盖策略匹配。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from rate_limiter import AccountClient, ProviderPool
import rate_limiter


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


def _patch_match(pool: ProviderPool, matched: dict) -> None:
    """让渠道的冻结匹配返回受控结果。"""
    pool.channel.match_freeze = lambda **_kw: matched


def _capture_notifications(monkeypatch) -> list[dict]:
    """截获 emit_notification_background 的调用，摊平成一个便于断言的 dict。

    _notify_freeze 是函数内 import，所以补丁必须打在 notify_core 模块上。
    """
    from user_platform import notify_core

    captured: list[dict] = []

    def _fake_emit(event_type: str, **kwargs) -> None:
        captured.append({"event_type": event_type, **kwargs})

    monkeypatch.setattr(notify_core, "emit_notification_background", _fake_emit)
    return captured


def test_no_loop_notify_freeze_does_not_raise_or_schedule(monkeypatch):
    """同步无事件循环：_notify_freeze 直接 return，不抛异常也不调度通知。

    ``apply_freeze_policy`` 走的 ``_freeze_account`` 本就依赖运行中的事件循环
    （无条件 ``asyncio.create_task``），生产中总在 async 路径调用；这里直接对
    ``_notify_freeze`` 施压，验证它在无 loop 时的 best-effort 守卫。
    """
    pool, _ = _make_pool()
    captured = _capture_notifications(monkeypatch)

    # 无 running loop 时应直接跳过，不抛异常。
    pool._notify_freeze("account", "acct1", None, 999, "auth", permanent=True)

    assert captured == []


@pytest.mark.asyncio
async def test_permanent_account_freeze_emits_account_event(monkeypatch):
    """单账号永久冻结 → ``account.frozen``，归因到该账号。

    账号级冻结原先被刻意丢弃以避免刷屏；现在它是一个可订阅事件，必须产生，
    刷屏交给订阅规则与 dedupe 收敛。
    """
    pool, _ = _make_pool()
    _patch_match(pool, {"scope": "account", "seconds": 999, "permanent": True, "freeze_mode": "account_permanent"})
    captured = _capture_notifications(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=403, reason="auth")
    await asyncio.sleep(0)

    assert len(captured) == 1
    note = captured[0]
    assert note["event_type"] == "account.frozen"
    assert note["params"]["account_username"] == "acct1"
    assert note["params"]["provider_name"] == "custom"
    assert note["params"]["permanent"] is True
    # 永久冻结需人工介入，去重窗口放宽到 1 小时。
    assert note["dedupe_window_seconds"] == 3600


@pytest.mark.asyncio
async def test_channel_disable_emits_critical_notification(monkeypatch):
    pool, client = _make_pool()
    _patch_match(pool, {"scope": "channel", "seconds": 0, "permanent": False, "disable": True, "freeze_mode": "channel_disabled"})
    captured = _capture_notifications(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=403, reason="auth")
    await asyncio.sleep(0)

    assert pool.channel.enabled is False
    assert client.is_frozen is False
    assert len(captured) == 1
    note = captured[0]
    assert note["severity"] == "critical"
    assert note["kind"] == "account"
    assert note["source"] == "freeze_policy"
    assert note["params"]["provider_name"] == "custom"
    assert note["params"]["permanent"] is False
    assert note["params"]["disabled"] is True
    assert note["params"]["scope"] == "channel"


def test_channel_permanent_freeze_is_rejected(monkeypatch):
    """后端运行态也防御非法渠道永久冻结，避免把渠道永久冻结偷换成禁用。"""
    pool, client = _make_pool()
    _patch_match(pool, {"scope": "channel", "seconds": 0, "permanent": True, "disable": False, "freeze_mode": "channel_permanent"})

    result = pool.apply_freeze_policy("acct1", status_code=403, reason="auth")

    assert result == ""
    assert pool.channel.enabled is True
    assert client.is_frozen is False


@pytest.mark.asyncio
async def test_account_model_freeze_emits_account_event_with_model(monkeypatch):
    """单账号+模型冻结 → ``account.frozen``，同时带上账号与模型。"""
    pool, _ = _make_pool()
    _patch_match(pool, {"scope": "account_model", "seconds": 60, "permanent": False, "freeze_mode": "account_model_fixed"})
    captured = _capture_notifications(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=429, reason="rate_limit", model_id="m-1")
    await asyncio.sleep(0)

    assert len(captured) == 1
    note = captured[0]
    assert note["event_type"] == "account.frozen"
    assert note["params"]["account_username"] == "acct1"
    assert note["params"]["model_id"] == "m-1"
    assert note["params"]["scope"] == "account_model"


@pytest.mark.asyncio
async def test_channel_model_freeze_emits_error_notification(monkeypatch):
    pool, _ = _make_pool()
    _patch_match(pool, {"scope": "channel_model", "seconds": 60, "permanent": False, "freeze_mode": "channel_model_fixed"})
    captured = _capture_notifications(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=500, reason="server", model_id="m-1")
    await asyncio.sleep(0)

    assert len(captured) == 1
    note = captured[0]
    assert note["severity"] == "error"
    assert note["params"]["scope"] == "channel_model"
    assert note["params"]["account_username"] is None


@pytest.mark.asyncio
async def test_plain_account_fixed_freeze_emits_warn_account_event(monkeypatch):
    """普通账号固定时长冻结 → ``account.frozen``，severity=warn。

    账号级用 warn 而非 error：单账号冻结是常态（限流退避），整渠道停摆才需要
    管理员立刻介入。级别差异让通知列表的严重度筛选仍然有意义。
    """
    pool, _ = _make_pool()
    _patch_match(pool, {"scope": "account", "seconds": 60, "permanent": False, "freeze_mode": "fixed_duration"})
    captured = _capture_notifications(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=429, reason="rate_limit")
    await asyncio.sleep(0)

    assert len(captured) == 1
    note = captured[0]
    assert note["event_type"] == "account.frozen"
    assert note["severity"] == "warn"
    assert note["params"]["account_username"] == "acct1"
    assert note["params"]["permanent"] is False


def _stub_async_tasks(monkeypatch) -> list:
    """吞掉 _apply_permanent_freeze / _notify_freeze 调度的 async 持久化任务，避免触达 DB。"""
    tasks: list = []
    real_create_task = asyncio.create_task

    def _capture(coro, **_kw):
        tasks.append(coro)
        # 不真正调度持久化；关闭协程避免「never awaited」警告。
        try:
            coro.close()
        except Exception:
            pass
        return None

    monkeypatch.setattr(rate_limiter.asyncio, "create_task", _capture)
    return tasks


@pytest.mark.asyncio
async def test_disabled_account_sets_switch_state_not_freeze_state(monkeypatch):
    """禁用账号关闭 switch 门禁，不复用 is_frozen，也不写冷却 TTL。"""
    pool, client = _make_pool()
    _patch_match(pool, {"scope": "account", "seconds": 0, "permanent": False, "disable": True, "freeze_mode": "account_disabled"})
    _stub_async_tasks(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=403, reason="auth")

    assert client.disabled is True
    assert client.disable_reason == "switch_off"
    assert client.is_frozen is False
    assert client._cooldown_until == 0


@pytest.mark.asyncio
async def test_permanent_account_freeze_sets_is_frozen_state(monkeypatch):
    """永久冻结走显式状态位 is_frozen=True，不写超长 _cooldown_until TTL。"""
    pool, client = _make_pool()
    _patch_match(pool, {"scope": "account", "seconds": 0, "permanent": True, "freeze_mode": "account_permanent"})
    _stub_async_tasks(monkeypatch)

    pool.apply_freeze_policy("acct1", status_code=403, reason="auth")
    await asyncio.sleep(0)

    assert client.is_frozen is True
    # 永久冻结不靠 TTL：_cooldown_until 不应被推到 10 年后。
    assert client._cooldown_until == 0


@pytest.mark.asyncio
async def test_clear_runtime_freeze_on_success_clears_permanent(monkeypatch):
    """探测成功会清永久冻结：永久冻结属于冻结，可由定时检测解冻。"""
    import time
    pool, client = _make_pool()
    # 模拟永久冻结 + 熔断窗口计数（探测前账号同时挂了两种状态）
    client.freeze(kind="account", seconds=None)
    client._stats._circuit_failures = [time.time(), time.time()]
    _stub_async_tasks(monkeypatch)

    cleared = pool.clear_runtime_freeze_on_success("acct1", "m-1")

    assert cleared is True
    # 永久冻结状态位被清
    assert client.is_frozen is False
    assert client._cooldown_until == 0
    # 熔断窗口计数也被清
    assert client._stats.circuit_failure_count(time.time()) == 0
