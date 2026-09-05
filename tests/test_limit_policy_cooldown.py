import asyncio
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import admin
from limit_policy_store import match_cooldown, match_freeze_rules, normalize_freeze_policy, validate_cooldown_policy, validate_freeze_policy
from rate_limiter import AccountClient, ProviderPool
from db import PostgresClient


def test_normalize_freeze_policy_preserves_refresh_freeze_on_failure():
    # 渠道级开关随 freeze_policy 落库/读取，归一化必须保留并默认开（True）。
    assert normalize_freeze_policy({"enabled": True, "rules": []})["refresh_freeze_on_failure"] is True
    assert normalize_freeze_policy({"enabled": True, "rules": [], "refresh_freeze_on_failure": False})["refresh_freeze_on_failure"] is False
    # 校验链路同源保留：经 validate_freeze_policy 也要透传。
    assert validate_freeze_policy({"enabled": True, "rules": []})["refresh_freeze_on_failure"] is True
    assert validate_freeze_policy({"enabled": True, "rules": [], "refresh_freeze_on_failure": False})["refresh_freeze_on_failure"] is False


def test_freeze_rule_enabled_defaults_true_and_round_trips():
    # 历史规则没有 enabled 字段，补字段不能把它们悄悄停掉。
    policy = validate_freeze_policy({"enabled": True, "rules": [{
        "condition": "status_code", "key": "", "operator": "==", "value": "429",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
    }]})
    assert policy["rules"][0]["enabled"] is True

    # 显式关掉的规则配置原样保留，便于随时开回来。
    policy = validate_freeze_policy({"enabled": True, "rules": [{
        "condition": "status_code", "key": "", "operator": "==", "value": "429",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
        "enabled": False,
    }]})
    assert policy["rules"][0]["enabled"] is False
    assert policy["rules"][0]["freeze_value"] == 30


def test_disabled_freeze_rule_is_skipped_at_match_time():
    rule = {
        "condition": "status_code", "key": "", "operator": "==", "value": "429",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
    }
    assert match_freeze_rules({"enabled": True, "rules": [dict(rule)]}, status_code=429) is not None
    assert match_freeze_rules({"enabled": True, "rules": [dict(rule, enabled=False)]}, status_code=429) is None


def test_disabled_no_freeze_rule_loses_terminal_semantics():
    # 关掉的 none 规则不再终止匹配，后面的冻结规则要能接上。
    policy = validate_freeze_policy({"enabled": True, "rules": [
        {
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": "account", "freeze_period": "none", "freeze_value": 0,
            "enabled": False,
        },
        {
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
        },
    ]})

    matched = match_freeze_rules(policy, status_code=429)
    assert matched is not None
    assert matched["freeze_mode"] == "account_seconds"
    assert matched["seconds"] == 30


def test_match_cooldown_minus_two_disables_account():
    matched = match_cooldown(429, {"429": "-2", "error": "60"})

    assert matched["raw_seconds"] == -2
    assert matched["seconds"] == 0
    assert matched["disable_account"] is True


def test_validate_cooldown_policy_allows_minus_two():
    policy = validate_cooldown_policy({"429": "-2", "error": "60"})

    assert policy["429"] == "-2"


def test_validate_cooldown_policy_rejects_below_minus_two():
    with pytest.raises(ValueError, match="冷却秒数只能为 -2"):
        validate_cooldown_policy({"429": "-3", "error": "60"})


# ---- match_freeze_rules - exception condition ----

def test_match_freeze_rules_matches_exception_condition():
    policy = {"enabled": True, "rules": [{
        "condition": "exception", "key": "", "operator": "", "value": "",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
    }]}
    result = match_freeze_rules(policy, exception=True)

    assert result is not None
    assert result["rule"]["condition"] == "exception"
    assert result["rule"]["key"] == ""
    assert result["rule"]["operator"] == ""
    assert result["rule"]["value"] == ""
    assert result["rule"]["freeze_object"] == "account"
    assert result["rule"]["freeze_period"] == "seconds"
    assert result["rule"]["freeze_value"] == 30
    assert result["scope"] == "account"
    assert result["seconds"] == 30
    assert result["permanent"] is False
    assert result["freeze_mode"] == "account_seconds"


def test_match_freeze_rules_no_match_when_exception_is_false():
    policy = {"enabled": True, "rules": [{
        "condition": "exception", "key": "", "operator": "", "value": "",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
    }]}
    result = match_freeze_rules(policy, exception=False)

    assert result is None


def test_no_freeze_rule_is_terminal_and_normalizes_seconds():
    policy = validate_freeze_policy({"enabled": True, "rules": [
        {
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": "account", "freeze_period": "none", "freeze_value": 60,
        },
        {
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 30,
        },
    ]})

    assert policy["rules"][0]["freeze_value"] == 0
    matched = match_freeze_rules(policy, status_code=429)
    assert matched is not None
    assert matched["freeze_mode"] == "no_freeze"
    assert matched["seconds"] == 0


def test_apply_no_freeze_policy_skips_freeze(monkeypatch):
    pool = ProviderPool("test-provider", object)
    monkeypatch.setattr(
        pool.channel,
        "match_freeze",
        lambda **_kwargs: {
            "scope": "account",
            "seconds": 0,
            "permanent": False,
            "freeze_mode": "no_freeze",
        },
    )
    monkeypatch.setattr(pool, "_freeze_account", lambda *_args, **_kwargs: pytest.fail("不应冻结账号"))
    monkeypatch.setattr(pool, "_freeze_account_model", lambda *_args, **_kwargs: pytest.fail("不应冻结模型"))

    assert pool.apply_freeze_policy("account-1", status_code=429) == "no_freeze"


# ---- 失败刷新冻结周期（refresh_freeze_on_failure）----
# 渠道级配置：关时已冻结对象保留现有到期时间，未冻结对象仍正常冻结。
# 只看配置，不看请求模式（正常请求/定时检测/响应头规则共用同一判定）。


def _freeze_pool(monkeypatch, *, refresh: bool, usernames=("acct1",), seconds=300, scope="account"):
    """构造一个只按固定规则冻结的池，并拦住 Redis/广播副作用。返回 (pool, clients, redis_calls)。"""
    pool = ProviderPool("test-provider", object)
    clients = []
    for username in usernames:
        provider = SimpleNamespace(PROVIDER_NAME="test-provider", username=username, quotas=None)
        clients.append(AccountClient(provider, rpm_limit=0))
    pool.clients = clients
    monkeypatch.setattr(pool.channel, "match_freeze", lambda **_kwargs: {
        "scope": scope, "seconds": seconds, "permanent": False, "disable": False,
        "freeze_mode": f"{scope}_seconds",
    })
    monkeypatch.setattr(pool, "freeze_refresh_on_failure", lambda: refresh)
    redis_calls: list[tuple] = []
    monkeypatch.setattr(
        pool, "_broadcast_cooldown",
        lambda *args, **kwargs: redis_calls.append(("broadcast", args, kwargs)),
    )

    def _fake_create_task(coro):
        # 同步单测没有事件循环；记录调用并关掉协程避免 "never awaited" 警告。
        coro.close()
        redis_calls.append(("redis", None, None))
        return None

    monkeypatch.setattr("rate_limiter.asyncio.create_task", _fake_create_task)
    return pool, clients, redis_calls


def test_refresh_off_keeps_existing_ttl(monkeypatch):
    pool, (client,), redis_calls = _freeze_pool(monkeypatch, refresh=False)
    client.freeze(kind="account", seconds=20, reason="first")
    original_until = client._cooldown_until

    assert pool.apply_freeze_policy("acct1", status_code=429) == "freeze"
    # 已冻结：保留原到期时间，且不再写 Redis TTL / 不广播（SET EX 会无条件重设 TTL）。
    assert client._cooldown_until == original_until
    assert client.cooldown_reason == "first"
    assert redis_calls == []


def test_refresh_off_still_freezes_unfrozen_account(monkeypatch):
    pool, (client,), redis_calls = _freeze_pool(monkeypatch, refresh=False)

    assert pool.apply_freeze_policy("acct1", status_code=429) == "freeze"
    assert client.is_frozen is True
    assert client.cooldown_remaining() > 250
    assert [kind for kind, _a, _k in redis_calls] == ["redis", "broadcast"]


def test_refresh_on_resets_ttl(monkeypatch):
    pool, (client,), _redis_calls = _freeze_pool(monkeypatch, refresh=True)
    client.freeze(kind="account", seconds=20, reason="first")

    assert pool.apply_freeze_policy("acct1", status_code=429) == "freeze"
    # 开：TTL 按本次规则重设（20s → 300s）。
    assert client.cooldown_remaining() > 250


def test_refresh_off_channel_scope_mixed_state(monkeypatch):
    pool, (frozen, fresh), _redis_calls = _freeze_pool(
        monkeypatch, refresh=False, usernames=("acct1", "acct2"), scope="channel",
    )
    frozen.freeze(kind="account", seconds=20, reason="first")
    frozen_until = frozen._cooldown_until

    assert pool.apply_freeze_policy("acct1", status_code=429) == "freeze"
    # 渠道级冻结逐账号判定：已冻的保留原 TTL，没冻的按规则冻上。
    assert frozen._cooldown_until == frozen_until
    assert fresh.cooldown_remaining() > 250


def test_refresh_off_account_model_scope_is_per_model(monkeypatch):
    pool, (client,), _redis_calls = _freeze_pool(monkeypatch, refresh=False, scope="account_model")
    client.freeze(kind="account_model", model_id="m1", seconds=20, reason="first")
    m1_until = client._freeze_state["models"]["m1"]["until"]

    assert pool.apply_freeze_policy("acct1", status_code=429, model_id="m1") == "freeze"
    assert client._freeze_state["models"]["m1"]["until"] == m1_until
    # 另一个模型没冻过，仍按规则冻上。
    assert pool.apply_freeze_policy("acct1", status_code=429, model_id="m2") == "freeze"
    assert client.is_model_frozen("m2")


def test_refresh_off_blocks_fallback_cooldown(monkeypatch):
    """未命中规则时走全局兜底冷却，同样不能刷新已冻结对象的 TTL。"""
    import config as config_module

    pool = ProviderPool("test-provider", object)
    provider = SimpleNamespace(PROVIDER_NAME="test-provider", username="acct1", quotas=None)
    client = AccountClient(provider, rpm_limit=0)
    pool.clients = [client]
    monkeypatch.setattr(pool.channel, "match_freeze", lambda **_kwargs: None)
    monkeypatch.setattr(pool, "freeze_refresh_on_failure", lambda: False)
    monkeypatch.setattr("rate_limiter.asyncio.create_task", lambda coro: coro.close())
    # 本地无 PG 时 Config._load 会抛；兜底秒数只需一个正数即可覆盖本用例。
    monkeypatch.setattr(config_module.Config, "_load", classmethod(lambda cls: {"rate_limit": {"cooldown_seconds": 600}}))
    client.freeze(kind="account", seconds=20, reason="first")
    original_until = client._cooldown_until

    pool.handle_response_error("acct1", 500, "boom")

    assert client._cooldown_until == original_until
    assert client.cooldown_reason == "first"


def test_permanent_freeze_ignores_refresh_switch(monkeypatch):
    """永久冻结是冻结对象升级，不是周期刷新，关开关也要生效。"""
    pool = ProviderPool("test-provider", object)
    provider = SimpleNamespace(PROVIDER_NAME="test-provider", username="acct1", quotas=None)
    client = AccountClient(provider, rpm_limit=0)
    pool.clients = [client]
    monkeypatch.setattr(pool.channel, "match_freeze", lambda **_kwargs: {
        "scope": "account", "seconds": 0, "permanent": True, "disable": False,
        "freeze_mode": "account_permanent",
    })
    monkeypatch.setattr(pool, "freeze_refresh_on_failure", lambda: False)
    monkeypatch.setattr("rate_limiter.asyncio.create_task", lambda coro: coro.close())
    client.freeze(kind="account", seconds=20, reason="first")

    assert pool.apply_freeze_policy("acct1", status_code=403) == "freeze"
    assert client._freeze_state["account"]["until"] is None


def test_header_freeze_path_honours_refresh_switch(monkeypatch):
    """响应头命中规则的路径必须走同一判定，不能绕开开关直写 Redis。"""
    import rate_limiter
    from limits.provider_state import ProviderLimitState

    pool = ProviderPool("test-provider", object)
    provider = SimpleNamespace(PROVIDER_NAME="test-provider", username="acct1", quotas=None)
    client = AccountClient(provider, rpm_limit=0)
    pool.clients = [client]
    monkeypatch.setattr(pool.channel, "match_freeze", lambda **_kwargs: {
        "scope": "account", "seconds": 300, "permanent": False, "disable": False,
        "freeze_mode": "account_seconds",
    })
    monkeypatch.setattr(pool, "freeze_refresh_on_failure", lambda: False)
    monkeypatch.setattr(
        rate_limiter.ModelClientPool, "get_provider_pool",
        classmethod(lambda cls, name: pool),
    )
    monkeypatch.setattr("rate_limiter.asyncio.create_task", lambda coro: coro.close())

    async def _noop_quota(_headers, _model_id):
        return None

    state_provider = SimpleNamespace(
        PROVIDER_NAME="test-provider", username="acct1",
        update_quota_from_headers=_noop_quota, _channel=pool.channel,
    )
    state = ProviderLimitState(state_provider)
    client.freeze(kind="account", seconds=20, reason="first")
    original_until = client._cooldown_until

    asyncio.run(state.update_from_headers({"x-ratelimit-remaining": "0"}, None, {"account_client": client}))

    assert client._cooldown_until == original_until
    assert client.cooldown_reason == "first"



# ---- validate_freeze_policy - exception rule ----

def test_validate_freeze_policy_allows_exception_rule_empty_fields():
    policy = {"enabled": True, "rules": [{
        "condition": "exception", "key": "", "operator": "", "value": "",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
    }]}
    result = validate_freeze_policy(policy)

    assert result["enabled"] is True
    assert len(result["rules"]) == 1
    rule = result["rules"][0]
    assert rule["condition"] == "exception"
    assert rule["key"] == ""
    assert rule["operator"] == ""
    assert rule["value"] == ""
    assert rule["freeze_object"] == "account"
    assert rule["freeze_period"] == "seconds"
    assert rule["freeze_value"] == 60


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "message"),
    [
        (
            {
                "cooldown_policy": {},
                "freeze_policy": {"enabled": True, "rules": [{
                    "condition": "status_code", "key": "", "operator": "==", "value": "429",
                    "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 0,
                }]},
            },
            "第 1 条冻结规则 seconds 必须填写正整数 freeze_value",
        ),
        (
            {"cooldown_policy": {"429": "-3"}, "freeze_policy": {}},
            "冷却秒数只能为 -2、-1 或大于等于 0",
        ),
    ],
)
async def test_update_provider_limit_policy_returns_400_for_invalid_policy(monkeypatch, policy, message):
    async def allow_admin(_token):
        return None

    async def provider_exists(_name):
        return True

    async def get_policy(_name, *, refresh=False):
        return {}

    upsert_called = False

    async def upsert_policy(_name, _payload):
        nonlocal upsert_called
        upsert_called = True
        return {}

    monkeypatch.setattr(admin, "_require_admin", allow_admin)
    monkeypatch.setattr(admin, "_provider_config_exists", provider_exists)
    monkeypatch.setattr(admin, "get_effective_provider_policy", get_policy)
    monkeypatch.setattr(admin.PostgresClient, "upsert_provider_limit_policy", upsert_policy)

    with pytest.raises(HTTPException) as exc_info:
        await admin.update_provider_limit_policy("test-provider", policy)

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == message
    assert upsert_called is False


# ---- _freeze_policy_from_cooldown - migration helper ----

def test_freeze_policy_from_cooldown_429_plus_error():
    result = PostgresClient._freeze_policy_from_cooldown("x", {"429": "60", "error": "60"})

    assert result["enabled"] is True
    assert len(result["rules"]) == 2
    sc_rule = [r for r in result["rules"] if r["condition"] == "status_code"]
    exc_rule = [r for r in result["rules"] if r["condition"] == "exception"]
    assert len(sc_rule) == 1
    assert len(exc_rule) == 1
    assert sc_rule[0]["key"] == ""
    assert sc_rule[0]["operator"] == "=="
    assert sc_rule[0]["value"] == "429"
    assert sc_rule[0]["freeze_mode"] == "fixed_duration"
    assert sc_rule[0]["freeze_seconds"] == 60
    assert exc_rule[0]["key"] == ""
    assert exc_rule[0]["operator"] == ""
    assert exc_rule[0]["value"] == ""
    assert exc_rule[0]["freeze_mode"] == "fixed_duration"
    assert exc_rule[0]["freeze_seconds"] == 60


# ---- match_freeze_rules - body condition (响应体字段匹配) ----

ALIYUN_QUOTA_BODY = '{"error":{"code":"insufficient_quota","message":"You exceeded your current quota, please check your plan and billing details. For details, see: https://help.aliyun.com/zh/model-studio/error-code#token-limit","param":null,"type":"insufficient_quota"},"request_id":"57553be3-dea8-49f1-ba1e-7a820c152999"}'


def test_match_freeze_rules_body_condition_matches_error_code_string():
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.code", "operator": "==", "value": "insufficient_quota",
        "freeze_object": "account", "freeze_period": "disabled", "freeze_value": 0,
    }]}
    result = match_freeze_rules(policy, body=ALIYUN_QUOTA_BODY)

    assert result is not None
    assert result["rule"]["condition"] == "body"
    assert result["rule"]["key"] == "error.code"
    assert result["freeze_mode"] == "account_disabled"
    assert result["permanent"] is False
    assert result["disable"] is True


def test_match_freeze_rules_body_condition_matches_dict_body():
    """body 也可以是已解析的 dict（如 error.detail 恰好是 dict）。"""
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.code", "operator": "==", "value": "insufficient_quota",
        "freeze_object": "account", "freeze_period": "disabled", "freeze_value": 0,
    }]}
    import json as _json
    result = match_freeze_rules(policy, body=_json.loads(ALIYUN_QUOTA_BODY))

    assert result is not None
    assert result["permanent"] is False
    assert result["disable"] is True


def test_match_freeze_rules_body_condition_no_match_on_different_value():
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.code", "operator": "==", "value": "rate_limit",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
    }]}
    result = match_freeze_rules(policy, body=ALIYUN_QUOTA_BODY)

    assert result is None


def test_match_freeze_rules_body_condition_missing_path_returns_none():
    """字段路径不存在时跳过该规则（不命中）。"""
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.missing", "operator": "==", "value": "x",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
    }]}
    result = match_freeze_rules(policy, body=ALIYUN_QUOTA_BODY)

    assert result is None


def test_match_freeze_rules_body_condition_contains_operator():
    """contains 对 message 做子串匹配。"""
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.message", "operator": "contains", "value": "exceeded your current quota",
        "freeze_object": "account", "freeze_period": "today", "freeze_value": 0,
    }]}
    result = match_freeze_rules(policy, body=ALIYUN_QUOTA_BODY)

    assert result is not None
    assert result["freeze_mode"] == "account_today"


def test_match_freeze_rules_body_condition_non_json_body_returns_none():
    """非 JSON 字符串（如带前缀的纯文本错误）解析失败 → 不命中且不抛错。"""
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.code", "operator": "==", "value": "insufficient_quota",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
    }]}
    result = match_freeze_rules(policy, body="HTTP 500 Internal Server Error")

    assert result is None


def test_match_freeze_rules_body_condition_none_body_skips_rule():
    """无 body（如纯异常路径）时 body 规则被跳过。"""
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.code", "operator": "==", "value": "insufficient_quota",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
    }]}
    result = match_freeze_rules(policy, body=None)

    assert result is None


def test_match_freeze_rules_body_condition_in_operator_multi_value():
    policy = {"enabled": True, "rules": [{
        "condition": "body", "key": "error.code", "operator": "in", "value": "rate_limit,insufficient_quota",
        "freeze_object": "account", "freeze_period": "disabled", "freeze_value": 0,
    }]}
    result = match_freeze_rules(policy, body=ALIYUN_QUOTA_BODY)

    assert result is not None


def test_normalize_freeze_rule_body_requires_key():
    """body 条件缺 key 时被规范化丢弃。"""
    from limit_policy_store import normalize_freeze_rule
    rule = normalize_freeze_rule({
        "condition": "body", "key": "", "operator": "==", "value": "insufficient_quota",
        "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
    })
    assert rule is None


def test_validate_freeze_policy_body_requires_key():
    """body 条件缺 key 时校验抛错。"""
    with pytest.raises(ValueError, match="body 条件必须填写 key"):
        validate_freeze_policy({"enabled": True, "rules": [{
            "condition": "body", "key": "", "operator": "==", "value": "insufficient_quota",
            "freeze_object": "account", "freeze_period": "seconds", "freeze_value": 60,
        }]})


# ---- 旧 freeze_mode → 新二维模型数据库迁移 ----

def test_legacy_freeze_mode_database_migration():
    """数据库迁移直接替换旧字段，并删除 freeze_mode/freeze_seconds。"""
    cases = [
        ({"freeze_mode": "no_freeze", "freeze_seconds": 0}, "account", "none", 0),
        ({"freeze_mode": "account_daily", "freeze_seconds": 0}, "account", "today", 0),
        ({"freeze_mode": "account_model_daily", "freeze_seconds": 0}, "account_model", "today", 0),
        ({"freeze_mode": "account_permanent", "freeze_seconds": 0}, "account", "disabled", 0),
        ({"freeze_mode": "account_model_weekly", "freeze_seconds": 0}, "account_model", "week", 0),
        ({"freeze_mode": "account_model_monthly", "freeze_seconds": 0}, "account_model", "month", 0),
        ({"freeze_mode": "fixed_duration", "freeze_seconds": 120}, "account", "seconds", 120),
        ({"freeze_mode": "account_model_fixed", "freeze_seconds": 90}, "account_model", "seconds", 90),
        ({"freeze_mode": "channel_fixed", "freeze_seconds": 300}, "channel", "seconds", 300),
        ({"freeze_mode": "channel_model_fixed", "freeze_seconds": 45}, "channel_model", "seconds", 45),
    ]
    for legacy_rule, exp_obj, exp_period, exp_val in cases:
        rule = PostgresClient._migrate_freeze_rule_object_period({
            "condition": "status_code", "key": "", "operator": "==", "value": "429", **legacy_rule,
        })
        assert "freeze_mode" not in rule
        assert "freeze_seconds" not in rule
        assert rule["freeze_object"] == exp_obj
        assert rule["freeze_period"] == exp_period
        assert rule["freeze_value"] == exp_val


def test_disabled_period_rejected_for_model_objects():
    """禁用仅对账号/渠道有效；模型类对象（account_model/channel_model）选 disabled 应被丢弃。"""
    from limit_policy_store import normalize_freeze_rule
    for obj in ("account_model", "channel_model"):
        rule = normalize_freeze_rule({
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": obj, "freeze_period": "disabled", "freeze_value": 0,
        })
        assert rule is None, f"{obj} 不应支持 disabled"


def test_disabled_period_allowed_for_account_and_channel():
    """禁用对账号/渠道有效。"""
    from limit_policy_store import normalize_freeze_rule
    for obj in ("account", "channel"):
        rule = normalize_freeze_rule({
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": obj, "freeze_period": "disabled", "freeze_value": 0,
        })
        assert rule is not None and rule["freeze_period"] == "disabled"


def test_permanent_period_allowed_only_for_account():
    """永久冻结只支持账号；渠道及模型类对象不支持。"""
    from limit_policy_store import normalize_freeze_rule
    account_rule = normalize_freeze_rule({
        "condition": "status_code", "key": "", "operator": "==", "value": "429",
        "freeze_object": "account", "freeze_period": "permanent", "freeze_value": 0,
    })
    assert account_rule is not None and account_rule["freeze_period"] == "permanent"
    for obj in ("account_model", "channel", "channel_model"):
        rule = normalize_freeze_rule({
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": obj, "freeze_period": "permanent", "freeze_value": 0,
        })
        assert rule is None, f"{obj} 不应支持 permanent"


def test_value_periods_require_positive_value():
    """分钟/小时/天/秒周期必须填正整数。"""
    from limit_policy_store import normalize_freeze_rule
    for period in ("seconds", "minutes", "hours", "days"):
        rule = normalize_freeze_rule({
            "condition": "status_code", "key": "", "operator": "==", "value": "429",
            "freeze_object": "account", "freeze_period": period, "freeze_value": 0,
        })
        assert rule is None, f"{period} value=0 应被拒绝"


def test_period_to_seconds_unit_conversion():
    """分钟/小时/天/秒 换算成秒。"""
    from limit_policy_store import _period_to_seconds
    assert _period_to_seconds("seconds", 30) == 30
    assert _period_to_seconds("minutes", 5) == 300
    assert _period_to_seconds("hours", 2) == 7200
    assert _period_to_seconds("days", 1) == 86400


def test_match_freeze_rules_channel_model_scope():
    """渠道模型对象命中返回 channel_model scope。"""
    policy = {"enabled": True, "rules": [{
        "condition": "status_code", "key": "", "operator": "==", "value": "429",
        "freeze_object": "channel_model", "freeze_period": "hours", "freeze_value": 2,
    }]}
    result = match_freeze_rules(policy, status_code=429)
    assert result is not None
    assert result["scope"] == "channel_model"
    assert result["seconds"] == 7200
    assert result["permanent"] is False
