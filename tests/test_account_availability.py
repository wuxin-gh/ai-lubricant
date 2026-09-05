import asyncio
from types import SimpleNamespace

import pytest

import admin
import config
from limits.backend import RedisLimitBackend
from limits.manager import LimitManager
from limits.rules import LimitDecision


class _FakeClient:
    def __init__(self, username: str):
        self.username = username


class _FakeChannel:
    def __init__(self, models):
        self.models = models
        self.enabled = True


class _FakePool:
    def __init__(self, clients, channel):
        self.clients = clients
        self.channel = channel


def _install_pool(monkeypatch, name, usernames, models):
    clients = [_FakeClient(u) for u in usernames]
    channel = _FakeChannel([{"provider": name, "model_id": m, "upstream_model_id": m} for m in models])
    pool = _FakePool(clients, channel)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", classmethod(lambda cls, n: pool if n == name else None))
    return clients


# ==================== RPD 读取 ====================

@pytest.mark.asyncio
async def test_get_account_rpd_used_redis_none(monkeypatch):
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: None))
    assert await RedisLimitBackend.get_account_rpd_used("account:p1:u1") == (0, 0)


@pytest.mark.asyncio
async def test_batch_get_account_rpd_redis_none(monkeypatch):
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: None))
    assert await RedisLimitBackend.batch_get_account_rpd(["account:p1:u1"]) == {}


@pytest.mark.asyncio
async def test_batch_get_account_rpd_empty_keys(monkeypatch):
    # 即使 Redis 可用，空 keys 也应短路返回空
    monkeypatch.setattr(RedisLimitBackend, "redis", staticmethod(lambda: object()))
    assert await RedisLimitBackend.batch_get_account_rpd([]) == {}


# ==================== 可用性诊断 ====================

@pytest.mark.asyncio
async def test_availability_provider_filtered_by_whitelist(monkeypatch):
    """provider 不在模型组白名单 → 全账号 provider_filtered（非冷却/冻结）。"""
    _install_pool(monkeypatch, "jiekou", ["u1", "u2"], ["opus-model"])
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, m, **kw: _coro(True)))
    monkeypatch.setattr(config.Config, "get_model_group_models", classmethod(lambda cls, m, **kw: _coro(["opus-model"])))
    monkeypatch.setattr(config.Config, "get_model_group_provider_filter", classmethod(lambda cls, m, **kw: _coro(({"other"}, set()))))

    called = {"n": 0}

    async def _fail_locked(*a, **kw):
        called["n"] += 1
        return LimitDecision(True)

    monkeypatch.setattr(LimitManager, "account_available_locked", classmethod(lambda cls, *a, **kw: _fail_locked()))

    resp = await admin._provider_accounts_availability("jiekou", {}, "opus")
    statuses = {a["username"]: a["status"] for a in resp["accounts"]}
    assert statuses == {"u1": "provider_filtered", "u2": "provider_filtered"}
    assert "白名单" in resp["accounts"][0]["reason"]
    # provider 级过滤后不应再调用账号级判定
    assert called["n"] == 0


@pytest.mark.asyncio
async def test_availability_account_states(monkeypatch):
    """未被 provider 过滤时，逐账号映射 LimitDecision.reason。"""
    _install_pool(monkeypatch, "jiekou", ["ok", "cool", "frozen"], ["m1"])
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, m, **kw: _coro(False)))

    decisions = {
        "ok": LimitDecision(True),
        "cool": LimitDecision(False, "account_cooldown", retry_after=42),
        "frozen": LimitDecision(False, "account_frozen"),
    }

    async def _locked(cls, client, model, messages=None, is_test=False):
        return decisions[client.username]

    monkeypatch.setattr(LimitManager, "account_available_locked", classmethod(_locked))

    resp = await admin._provider_accounts_availability("jiekou", {}, "m1")
    by_user = {a["username"]: a for a in resp["accounts"]}
    assert by_user["ok"]["status"] == "available"
    assert by_user["cool"]["status"] == "account_cooldown"
    assert by_user["cool"]["retry_after"] == 42
    assert by_user["cool"]["cooldown_scope"] == "account"
    assert by_user["frozen"]["status"] == "account_frozen"
    # 请求级维度不应出现在静态视图
    all_statuses = {a["status"] for a in resp["accounts"]}
    assert not (all_statuses & {"excluded_account", "selection_returned_none", "concurrent_limit", "redis_uncertain"})


@pytest.mark.asyncio
async def test_availability_no_model_route(monkeypatch):
    """模型未在渠道注册路由 → no_model_route。"""
    _install_pool(monkeypatch, "jiekou", ["u1"], [])  # channel.models 为空
    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, m, **kw: _coro(False)))
    monkeypatch.setattr(LimitManager, "account_available_locked", classmethod(lambda cls, *a, **kw: _coro(LimitDecision(True))))

    resp = await admin._provider_accounts_availability("jiekou", {}, "missing-model")
    assert resp["accounts"][0]["status"] == "no_model_route"


def _coro(value):
    async def _inner():
        return value
    return _inner()
