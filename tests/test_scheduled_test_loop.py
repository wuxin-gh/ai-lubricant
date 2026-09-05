"""定时渠道测试循环单测。

覆盖：
- _scheduled_test_interval_seconds 频率换算（daily/minutes/hours/兜底）。
- scheduled_test_loop 节流：enabled=False 跳过、未到间隔跳过、到点透传参数给 _run_provider_test。
"""
from __future__ import annotations

import asyncio

import pytest

import config
import rate_limiter
from rate_limiter import ModelClientPool, _scheduled_test_interval_seconds

# 真正的 asyncio.sleep：下面的 fixture/测试会 monkeypatch asyncio.sleep（那是全局模块属性），
# 必须在打补丁之前抓住原函数，否则 "让出事件循环" 会递归回到假 sleep 里。
_REAL_SLEEP = asyncio.sleep


async def _yield_to_workers():
    """让出两次事件循环，给消费者 worker 机会把队列吃掉。"""
    await _REAL_SLEEP(0)
    await _REAL_SLEEP(0)


def test_interval_daily_is_24_hours():
    assert _scheduled_test_interval_seconds({"frequency_unit": "daily", "frequency_value": 1}) == 24 * 3600


def test_interval_minutes():
    assert _scheduled_test_interval_seconds({"frequency_unit": "minutes", "frequency_value": 5}) == 300


def test_interval_hours():
    assert _scheduled_test_interval_seconds({"frequency_unit": "hours", "frequency_value": 2}) == 7200


def test_interval_minimum_floor_60s():
    # 0 / 负数 / 非数字都兜底到最小 60 秒，防止高频打上游。
    assert _scheduled_test_interval_seconds({"frequency_unit": "minutes", "frequency_value": 0}) == 60
    assert _scheduled_test_interval_seconds({"frequency_unit": "minutes", "frequency_value": -3}) == 60
    assert _scheduled_test_interval_seconds({"frequency_unit": "minutes", "frequency_value": "abc"}) == 60


def test_interval_unknown_unit_falls_back_to_minutes():
    assert _scheduled_test_interval_seconds({"frequency_unit": "weeks", "frequency_value": 1}) == 60


class _FakeClient:
    """最小 AccountClient 替身：只实现定时循环用到的接口（state/frozen_model_ids/last_request_at）。"""

    def __init__(self, username: str, *, state="available", frozen_models=None, last_request=0.0):
        from rate_limiter import AccountState
        self.username = username
        self.disabled = False
        self._state = AccountState(state)
        self._frozen_models = list(frozen_models or [])
        self._last_request = last_request

    def state(self):
        return self._state

    def frozen_model_ids(self, now=None):
        return list(self._frozen_models)

    def last_request_at(self):
        return self._last_request


class _FakePool:
    def __init__(self):
        self.clients = [_FakeClient("u1"), _FakeClient("u2")]

    @property
    def enabled(self):
        return True


@pytest.fixture(autouse=True)
def _stub_global_config(monkeypatch):
    # 定时循环启动时读全局并发/跳过窗口配置；单测环境没有 PG，直接桩掉返回默认值。
    monkeypatch.setattr(config.Config, "scheduled_test_concurrency", classmethod(lambda cls: 1))
    monkeypatch.setattr(config.Config, "scheduled_test_skip_if_requested_within", classmethod(lambda cls: 0))
    # 渠道模型表：默认给一个模型 m1，让未配 test_models 的账号也有渠道自身首模型可测。
    monkeypatch.setattr(ModelClientPool, "get_provider_routes", classmethod(lambda cls, name: [{"model_id": "m1"}]))


@pytest.fixture
def _pool_state(monkeypatch):
    pools = ModelClientPool._provider_pools
    ModelClientPool._provider_pools = {"p1": _FakePool()}
    # 跳过 60s tick 的实际 sleep，但要真让出一次事件循环，消费者 worker 才有机会把队列吃掉。
    sleeps: list[float] = []

    async def _fast_sleep(seconds):
        sleeps.append(seconds)
        # 第一轮跑完后让循环“退出”：抛 CancelledError 终止 while True。
        if len(sleeps) >= 2:
            # 退出前把上一轮入队的任务放给 worker 消费完。
            await _yield_to_workers()
            raise asyncio.CancelledError

    monkeypatch.setattr(rate_limiter.asyncio, "sleep", _fast_sleep)
    captured: dict = {}

    async def _fake_cfg(provider):
        return captured.get(provider, {})

    monkeypatch.setattr(config.Config, "get_provider_scheduled_test", classmethod(lambda cls, provider: _fake_cfg(provider)))
    # 失败是否冻结/刷新 TTL 现在是渠道级 freeze_policy 属性（不再读 scheduled_test）。
    # monkeypatch rate_limiter 命名空间内已 import 的 get_effective_provider_policy，按渠道回灌策略。
    captured_policy: dict = {}

    async def _fake_policy(provider):
        return captured_policy.get(provider) or {
            "freeze_policy": {"enabled": True, "rules": [], "refresh_freeze_on_failure": True},
        }

    monkeypatch.setattr(rate_limiter, "get_effective_provider_policy", _fake_policy)
    captured_runner: list[dict] = []

    async def _fake_run(name, pool, data, *, is_probe=False):
        captured_runner.append({"name": name, "pool": pool, "data": dict(data), "is_probe": is_probe})

    import admin
    monkeypatch.setattr(admin, "_run_provider_test", _fake_run, raising=True)

    yield captured, captured_policy, captured_runner
    ModelClientPool._provider_pools = pools


def test_loop_survives_cancelled_error_from_single_probe(_pool_state, monkeypatch):
    """永久冻结账号的单次探测内部取消，不能终止后续定时周期。"""
    captured, _policy, captured_runner = _pool_state
    # 用户场景：渠道启用，账号永久冻结；unavailable 过滤应持续选中它。
    from rate_limiter import AccountState
    pool = ModelClientPool._provider_pools["p1"]
    pool.clients = [pool.clients[0]]  # 只留一个账号，便于计数
    pool.clients[0]._state = AccountState.FROZEN
    captured["p1"] = {
        "enabled": True,
        "frequency_unit": "minutes",
        "frequency_value": 1,
        "account_filter": "unavailable",
    }

    # 每个 tick 推进 60 秒；第三次 sleep 模拟真正的调度任务 shutdown。
    sleep_count = 0

    async def _two_ticks_then_cancel(seconds):
        nonlocal sleep_count
        sleep_count += 1
        # 让出事件循环，worker 才能消费上一轮入队的任务。
        await _yield_to_workers()
        if sleep_count >= 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(rate_limiter.asyncio, "sleep", _two_ticks_then_cancel)
    clock = iter((60.0, 120.0, 180.0, 240.0))
    monkeypatch.setattr(rate_limiter.time, "time", lambda: next(clock, 240.0))

    import admin
    calls = 0

    async def _cancel_once(name, pool, data, *, is_probe=False):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise asyncio.CancelledError
        captured_runner.append({"name": name, "pool": pool, "data": dict(data), "is_probe": is_probe})

    monkeypatch.setattr(admin, "_run_provider_test", _cancel_once, raising=True)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())

    # 第一轮内部取消后，第二轮仍然执行；第三次才是外部 shutdown 取消。
    assert calls == 2
    assert len(captured_runner) == 1


def test_loop_skips_disabled(_pool_state):
    captured, _policy, captured_runner = _pool_state
    captured["p1"] = {"enabled": False, "frequency_unit": "minutes", "frequency_value": 1}
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    assert captured_runner == []


def test_loop_runs_and_passes_params(_pool_state):
    """任务粒度是「单账号 × 单模型」：每个候选账号各自一次 _run_provider_test。"""
    captured, _policy, captured_runner = _pool_state
    captured["p1"] = {
        "enabled": True,
        "frequency_unit": "minutes",
        "frequency_value": 1,
        "accounts": ["u1", "u2"],
        "account_filter": "available",
        "test_model": "m1",
        "test_type": "chat",
        "client_type": "none",
        "protocol": "openai",
    }
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    assert len(captured_runner) == 2
    assert [c["data"]["username"] for c in captured_runner] == [["u1"], ["u2"]]
    call = captured_runner[0]
    assert call["name"] == "p1"
    assert call["data"]["model"] == "m1"
    assert call["data"]["test_type"] == "chat"
    assert call["data"]["client_type"] == "none"
    assert call["data"]["protocol"] == "openai"
    # 定时检测必须走 is_probe 模式（绕过预占 + 失败可冻结），而非 is_test。
    assert call["is_probe"] is True
    # 旧配置缺省保持当前行为：失败探测日志继续保留。
    assert call["data"]["retain_failed_logs"] is True
    # 「失败刷新冻结周期」不再随定时检测透传——它是渠道级冻结策略属性，在冻结写入点判定。
    assert "refresh_freeze_on_failure" not in call["data"]


def test_loop_does_not_forward_refresh_freeze_flag(_pool_state):
    """refresh_freeze_on_failure 已收口到渠道级 freeze_policy，定时循环不再读取/透传该值。

    无论渠道策略开或关，_run_provider_test 收到的 data 都不含该键；is_probe 仍为 True。
    """
    captured, captured_policy, captured_runner = _pool_state
    captured["p1"] = {
        "enabled": True,
        "frequency_unit": "minutes",
        "frequency_value": 1,
        "accounts": ["u1"],
        "account_filter": "available",
    }
    captured_policy["p1"] = {
        "freeze_policy": {"enabled": True, "rules": [], "refresh_freeze_on_failure": False},
    }
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    assert "refresh_freeze_on_failure" not in captured_runner[0]["data"]
    assert captured_runner[0]["is_probe"] is True


def test_loop_accounts_empty_means_all_enabled(_pool_state):
    captured, _policy, captured_runner = _pool_state
    captured["p1"] = {
        "enabled": True, "frequency_unit": "minutes", "frequency_value": 1,
        "accounts": [], "account_filter": "available",
    }
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    # 每个可用账号各一条任务（单账号 × 渠道首模型 m1）。
    assert sorted(c["data"]["username"][0] for c in captured_runner) == ["u1", "u2"]


def test_loop_forwards_retain_failed_logs_false(_pool_state):
    """显式 retain_failed_logs=false 透传给 _run_provider_test（关闭检测日志）。"""
    captured, _policy, captured_runner = _pool_state
    captured["p1"] = {
        "enabled": True, "frequency_unit": "minutes", "frequency_value": 1,
        "accounts": ["u1"], "account_filter": "available", "retain_failed_logs": False,
    }
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    assert captured_runner[0]["data"]["retain_failed_logs"] is False


def test_loop_retain_failed_logs_defaults_true_on_explicit_true(_pool_state):
    """显式 retain_failed_logs=true 保持 true。"""
    captured, _policy, captured_runner = _pool_state
    captured["p1"] = {
        "enabled": True, "frequency_unit": "minutes", "frequency_value": 1,
        "accounts": ["u1"], "account_filter": "available", "retain_failed_logs": True,
    }
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    assert captured_runner[0]["data"]["retain_failed_logs"] is True


def test_manual_test_cannot_suppress_request_logs():
    """保留日志开关只对定时检测生效：手动测试即使 data 里伪造该字段也照常记日志。

    _run_provider_test 用 `if is_probe else True` 门禁 dispatch 参数，防止 /accounts/test
    的请求体关掉自己的请求日志。
    """
    source = open("admin.py", encoding="utf-8").read()
    body = source[source.index("async def _run_provider_test"):source.index("@router.post(\"/providers/{name}/accounts/detect-model\")")]
    assert 'is_save_log=(data.get("retain_failed_logs") is not False) if is_probe else True' in body


def test_loop_throttles_within_interval(_pool_state):
    """到点跑一次后，间隔内第二轮应被跳过。"""
    captured, _policy, captured_runner = _pool_state
    ModelClientPool._provider_pools["p1"].clients = [_FakeClient("u1")]
    captured["p1"] = {
        "enabled": True, "frequency_unit": "hours", "frequency_value": 1,
        "account_filter": "available",
    }  # 3600s
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ModelClientPool.scheduled_test_loop())
    # 第一轮 sleep(60) 后跑一次；第二轮 sleep(60) 后因间隔未到被跳过 → 抛 Cancelled 退出。
    assert len(captured_runner) == 1


# ── 候选筛选（生产者：只决定「测哪些账号」，不固化模型） ──


def _collect(cfg, clients, routes=None):
    """直接调生产者纯逻辑，拿到入队的账号名列表。"""
    from types import SimpleNamespace
    pool = SimpleNamespace(clients=clients, enabled=True)
    original = ModelClientPool.get_provider_routes
    ModelClientPool.get_provider_routes = classmethod(
        lambda cls, name: routes if routes is not None else [{"model_id": "m1"}]
    )
    try:
        tasks = ModelClientPool._scheduled_test_collect_tasks("p1", pool, cfg)
    finally:
        ModelClientPool.get_provider_routes = original
    return [t["username"] for t in tasks]


def _models_for(cfg, client, routes):
    return ModelClientPool._scheduled_test_models_for_client(cfg, client, [r["model_id"] for r in routes])


def test_unavailable_filter_selects_model_frozen_account():
    """部分模型冻结的账号必须算「异常」，filter=unavailable 要能选中它（此前会漏）。"""
    frozen = _FakeClient("u1", state="model_frozen", frozen_models=["m1"])
    ok = _FakeClient("u2", state="available")
    assert _collect({"account_filter": "unavailable"}, [frozen, ok]) == ["u1"]


def test_channel_without_models_and_without_config_is_skipped():
    """渠道一个模型都没有且未配 test_models → 整轮跳过，不拿全局模型硬打。"""
    client = _FakeClient("u1", state="cooling")
    assert _collect({"account_filter": "unavailable"}, [client], routes=[]) == []


def test_unavailable_filter_excludes_checking_account():
    """checking（auth_ok=None，尚未体检）不算异常：filter=unavailable 不能选中它。

    回归启动期时序问题——init_all 体检未跑完时账号停在 checking，此前会被误判成
    异常账号去探测。真正的异常（frozen/cooling/model_frozen/auth_failed）仍要选中。
    """
    checking = _FakeClient("u1", state="checking")
    frozen = _FakeClient("u2", state="frozen")
    assert _collect({"account_filter": "unavailable"}, [checking, frozen]) == ["u2"]


def test_available_filter_excludes_checking_account():
    """checking 也不是 available：filter=available 同样不该选中它。"""
    checking = _FakeClient("u1", state="checking")
    ok = _FakeClient("u2", state="available")
    assert _collect({"account_filter": "available"}, [checking, ok]) == ["u2"]


# ── 探测模型选择（消费者：发探测那一刻按当时状态重新选） ──


def test_model_frozen_account_probes_its_frozen_models():
    """未配 test_models + 账号 model_frozen → 探测的正是被冻模型，而不是随便找一个。"""
    client = _FakeClient("u1", state="model_frozen", frozen_models=["mb", "mc"])
    routes = [{"model_id": "ma"}, {"model_id": "mb"}, {"model_id": "mc"}]
    assert _models_for({}, client, routes) == ["mb", "mc"]


def test_cooling_account_probes_channel_first_model_not_global_catalog():
    """未配 test_models + 账号 cooling → 测该渠道自己的首个模型，不是全局目录第 0 个。"""
    client = _FakeClient("u1", state="cooling")
    routes = [{"model_id": "channel-first"}, {"model_id": "channel-second"}]
    assert _models_for({}, client, routes) == ["channel-first"]


def test_deleted_frozen_model_is_skipped_not_substituted():
    """被冻模型已不在渠道模型表 → 跳过该账号的模型探测，绝不改测别的无关模型。"""
    client = _FakeClient("u1", state="model_frozen", frozen_models=["deleted-model"])
    assert _models_for({}, client, [{"model_id": "alive"}]) == []


def test_frozen_models_capped_at_probe_max():
    """被冻模型数超过封顶 N → 只测前 N 个（frozen_model_ids 已按最快到期升序）。"""
    many = [f"m{i}" for i in range(12)]
    client = _FakeClient("u1", state="model_frozen", frozen_models=many)
    got = _models_for({}, client, [{"model_id": m} for m in many])
    assert got == many[:ModelClientPool.PROBE_MAX_FROZEN_MODELS]


def test_explicit_test_models_win_over_frozen_models():
    """显式配了 test_models 就尊重配置，不被「按被冻模型探测」覆盖用户意图。"""
    client = _FakeClient("u1", state="model_frozen", frozen_models=["frozen-one"])
    got = _models_for({"test_models": ["cfg-a", "cfg-b"]}, client, [{"model_id": "frozen-one"}])
    assert got == ["cfg-a", "cfg-b"]


# ── 消费者：发探测那一刻重找账号 + 重判状态 ──


def _probe_task(client, cfg=None, provider="p1"):
    from types import SimpleNamespace
    pool = SimpleNamespace(clients=[client], enabled=True)
    ModelClientPool._provider_pools = {provider: pool}
    return {"provider_name": provider, "pool": pool, "username": client.username, "cfg": cfg or {}}


@pytest.fixture
def _probe_env(monkeypatch):
    """消费者单测环境：桩掉 _run_provider_test 与渠道模型表，收集探测调用。"""
    import admin
    pools = ModelClientPool._provider_pools
    calls: list = []

    async def _fake_run(name, pool, data, *, is_probe=False):
        calls.append(data)

    monkeypatch.setattr(admin, "_run_provider_test", _fake_run, raising=True)
    monkeypatch.setattr(ModelClientPool, "get_provider_routes", classmethod(lambda cls, name: [{"model_id": "m1"}]))
    yield calls
    ModelClientPool._provider_pools = pools


def test_probe_skips_account_requested_recently(monkeypatch, _probe_env):
    """最近请求过的账号跳过本轮：刚被真实流量证明可达，无需再探。"""
    import time as _time
    monkeypatch.setattr(config.Config, "scheduled_test_skip_if_requested_within", classmethod(lambda cls: 120))
    client = _FakeClient("u1", state="cooling", last_request=_time.time() - 10)
    task = _probe_task(client)
    asyncio.run(ModelClientPool._scheduled_test_probe_one(task))
    assert _probe_env == []

    # 窗口外则正常探测。
    client._last_request = _time.time() - 500
    asyncio.run(ModelClientPool._scheduled_test_probe_one(task))
    assert len(_probe_env) == 1
    assert _probe_env[0]["model"] == "m1"


def test_probe_skips_account_disabled_at_dequeue(_probe_env):
    """入队后账号被禁用 → 发探测那一刻重判并跳过，不用陈旧状态白测。"""
    client = _FakeClient("u1", state="cooling")
    client.disabled = True
    asyncio.run(ModelClientPool._scheduled_test_probe_one(_probe_task(client)))
    assert _probe_env == []


def test_probe_skips_account_deleted_while_queued(_probe_env):
    """账号在排队期间被删除 → pool.clients 已无此账号，绝不检测已删账号。"""
    client = _FakeClient("u1", state="cooling")
    task = _probe_task(client)
    task["pool"].clients = []  # 模拟排队期间账号被删
    asyncio.run(ModelClientPool._scheduled_test_probe_one(task))
    assert _probe_env == []


def test_probe_skips_account_recovered_while_queued(_probe_env):
    """filter=unavailable 的任务在排队期间账号已恢复 available → 跳过本轮。"""
    client = _FakeClient("u1", state="available")
    task = _probe_task(client, cfg={"account_filter": "unavailable"})
    asyncio.run(ModelClientPool._scheduled_test_probe_one(task))
    assert _probe_env == []


def test_probe_skips_account_turned_checking_while_queued(_probe_env):
    """排队期间状态变成 checking（尚未体检）→ 不再匹配 unavailable，跳过本轮。

    消费者与生产者共用 _scheduled_test_is_unavailable，两处口径不会漂移。
    """
    client = _FakeClient("u1", state="cooling")
    task = _probe_task(client, cfg={"account_filter": "unavailable"})
    from rate_limiter import AccountState
    client._state = AccountState.CHECKING
    asyncio.run(ModelClientPool._scheduled_test_probe_one(task))
    assert _probe_env == []


def test_probe_reselects_frozen_models_at_dequeue_time(monkeypatch, _probe_env):
    """状态在排队期间从 cooling 变为 model_frozen → 此刻改测被冻模型（用户第 2 点）。"""
    monkeypatch.setattr(
        ModelClientPool, "get_provider_routes",
        classmethod(lambda cls, name: [{"model_id": "m1"}, {"model_id": "frozen-b"}]),
    )
    client = _FakeClient("u1", state="cooling")
    task = _probe_task(client, cfg={"account_filter": "unavailable"})
    # 入队时是 cooling；出队前账号级 TTL 到期，只剩模型级冻结。
    from rate_limiter import AccountState
    client._state = AccountState.MODEL_FROZEN
    client._frozen_models = ["frozen-b"]
    asyncio.run(ModelClientPool._scheduled_test_probe_one(task))
    assert [c["model"] for c in _probe_env] == ["frozen-b"]


# ── 手动「立即执行」：复用同一队列异步入队，不同步跑探测 ──


@pytest.fixture
def _run_now_env(monkeypatch):
    """手动触发环境：接管队列/在途集合与已保存配置，恢复原类属性避免污染其它用例。"""
    pools = ModelClientPool._provider_pools
    old_queue = ModelClientPool._scheduled_test_queue
    old_pending = ModelClientPool._scheduled_test_pending
    old_triggered = dict(ModelClientPool._last_scheduled_test_triggered_at)
    ModelClientPool._provider_pools = {"p1": _FakePool()}
    ModelClientPool._scheduled_test_queue = asyncio.Queue()
    ModelClientPool._scheduled_test_pending = set()
    saved: dict = {}

    async def _fake_cfg(provider):
        return saved.get(provider, {})

    monkeypatch.setattr(
        config.Config, "get_provider_scheduled_test",
        classmethod(lambda cls, provider: _fake_cfg(provider)),
    )
    yield saved
    ModelClientPool._provider_pools = pools
    ModelClientPool._scheduled_test_queue = old_queue
    ModelClientPool._scheduled_test_pending = old_pending
    ModelClientPool._last_scheduled_test_triggered_at = old_triggered


def test_run_now_enqueues_without_running_probe(_run_now_env):
    """立即执行只入队，不在调用内同步跑探测：返回入队数，任务留在队列里等 worker 消费。"""
    _run_now_env["p1"] = {"enabled": True, "account_filter": "available"}
    enqueued, triggered_at = asyncio.run(ModelClientPool.trigger_scheduled_test_now("p1", {}))
    assert enqueued == 2
    assert triggered_at is not None
    # 任务仍在队列中（异步语义），且在途集合已登记，避免定时循环重复入队。
    assert ModelClientPool._scheduled_test_queue.qsize() == 2
    assert ModelClientPool._scheduled_test_pending == {("p1", "u1"), ("p1", "u2")}
    # 上次执行时间被刷新，供前端立即回显。
    assert ModelClientPool.last_scheduled_test_triggered_at("p1") == triggered_at


def test_run_now_overrides_beat_saved_config(_run_now_env):
    """前端表单参数覆盖已保存配置：保存的是 available，表单选 unavailable 时按 unavailable 筛选。"""
    _run_now_env["p1"] = {"enabled": True, "account_filter": "available"}
    from rate_limiter import AccountState
    pool = ModelClientPool._provider_pools["p1"]
    pool.clients[0]._state = AccountState.FROZEN  # u1 异常，u2 仍 available
    enqueued, _ = asyncio.run(
        ModelClientPool.trigger_scheduled_test_now("p1", {"account_filter": "unavailable"})
    )
    assert enqueued == 1
    assert ModelClientPool._scheduled_test_pending == {("p1", "u1")}


def test_run_now_ignores_enabled_flag(_run_now_env):
    """定时开关关闭时也能手动执行：enabled 只管自动周期，不该拦手动触发。"""
    _run_now_env["p1"] = {"enabled": False, "account_filter": "available"}
    enqueued, _ = asyncio.run(ModelClientPool.trigger_scheduled_test_now("p1", {}))
    assert enqueued == 2


def test_run_now_skips_accounts_already_in_flight(_run_now_env):
    """已在途的账号不重复入队，与定时循环的 pending 去重同一套语义。"""
    _run_now_env["p1"] = {"enabled": True, "account_filter": "available"}
    ModelClientPool._scheduled_test_pending.add(("p1", "u1"))
    enqueued, _ = asyncio.run(ModelClientPool.trigger_scheduled_test_now("p1", {}))
    assert enqueued == 1
    assert ModelClientPool._scheduled_test_queue.qsize() == 1


def test_run_now_returns_minus_one_when_queue_not_ready(_run_now_env):
    """定时循环尚未启动（队列为 None）→ 返回 -1，由 admin 层转成「队列未就绪」提示。"""
    _run_now_env["p1"] = {"enabled": True, "account_filter": "available"}
    ModelClientPool._scheduled_test_queue = None
    enqueued, triggered_at = asyncio.run(ModelClientPool.trigger_scheduled_test_now("p1", {}))
    assert enqueued == -1
    assert triggered_at is None


def test_run_now_unknown_provider_returns_zero(_run_now_env):
    """渠道未在运行池 → 0 入队，不抛异常。"""
    enqueued, _ = asyncio.run(ModelClientPool.trigger_scheduled_test_now("nope", {}))
    assert enqueued == 0
