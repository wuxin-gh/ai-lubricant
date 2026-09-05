from __future__ import annotations

import asyncio
from types import MappingProxyType, SimpleNamespace

import main
import model_catalog
from rate_limiter import ModelClientPool, NoAvailableAccountError


def _snapshot(generation: int, groups: dict | None = None, metadata: dict | None = None):
    groups = groups or {}
    frozen_groups = MappingProxyType({name: MappingProxyType(dict(group)) for name, group in groups.items()})
    index = {}
    for name, group in frozen_groups.items():
        index[group.get("name") or name] = group
        for alias in group.get("aliases", ()):
            index.setdefault(alias, group)
    return model_catalog.ModelCatalogSnapshot(
        generation=generation,
        fingerprint=str(generation),
        groups=frozen_groups,
        group_index=MappingProxyType(index),
        metadata=MappingProxyType(metadata or {}),
        default=MappingProxyType({}),
        group_metadata=MappingProxyType({}),
    )


def test_outer_retry_captures_one_snapshot_per_attempt(monkeypatch):
    snapshots = [_snapshot(10), _snapshot(11), _snapshot(12)]
    current_calls = 0
    acquired = []

    def current_snapshot():
        nonlocal current_calls
        value = snapshots[min(current_calls, 2)]
        current_calls += 1
        return value

    class Client:
        username = "acct"

        async def chat(self, *args, **kwargs):
            if len(acquired) == 1:
                raise RuntimeError("retry")
            yield {"choices": [{"message": {"content": "ok"}}]}

    account = SimpleNamespace(last_route_info={}, release=lambda: asyncio.sleep(0))

    async def acquire(*args, snapshot=None, **kwargs):
        acquired.append(snapshot.generation)
        account.last_route_info = {"provider": "p", "account": "acct", "routed_model": "m", "catalog_generation": snapshot.generation}
        return Client(), "p", account

    monkeypatch.setattr(main.model_catalog, "current_snapshot", current_snapshot)
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["p"])
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: 1)
    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(lambda cls, model, snapshot=None: asyncio.sleep(0, result=False)))
    monkeypatch.setattr(ModelClientPool, "record_channel_failure", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "record_account_failure", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "record_account_success", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider, model: model)
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", lambda provider: None)

    async def collect():
        return [item async for item in main._chat_with_retry_for_model("m", [], False)]

    try:
        asyncio.run(collect())
    except Exception:
        pass
    assert acquired == [11, 12]


def test_snapshot_type_error_is_not_retried_without_snapshot():
    calls = []

    async def broken(*, snapshot=None):
        calls.append(snapshot)
        raise TypeError("invalid snapshot record")

    async def invoke():
        await main._catalog_call(broken, snapshot=_snapshot(40))

    try:
        asyncio.run(invoke())
    except TypeError as exc:
        assert str(exc) == "invalid snapshot record"
    else:
        raise AssertionError("expected TypeError")
    assert len(calls) == 1


def test_nested_response_rewrite_keeps_original_group_identity(monkeypatch):
    snapshot = _snapshot(41)
    rewritten = []

    class Client:
        username = "acct"

        async def chat(self, *args, **kwargs):
            yield {"message": {"model": "upstream", "content": []}}

    account = SimpleNamespace(
        last_route_info={"provider": "p", "account": "acct", "routed_model": "m"},
        release=lambda: asyncio.sleep(0),
    )

    async def acquire(*args, snapshot=None, **kwargs):
        return Client(), "p", account

    def rewrite(chunk, model, rewrite_nested=False):
        rewritten.append((model, rewrite_nested))
        return chunk

    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "get_model_providers", lambda model: ["p"])
    monkeypatch.setattr(ModelClientPool, "acquire_client_with_provider", acquire)
    monkeypatch.setattr(main.config.Config, "get_provider_retry_count", lambda provider: 0)
    monkeypatch.setattr(main.config.Config, "stream_incomplete_error_enabled", lambda: False)
    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(lambda cls, model, snapshot=None: asyncio.sleep(0, result=False)))
    monkeypatch.setattr(ModelClientPool, "record_account_success", lambda *a, **k: None)
    monkeypatch.setattr(ModelClientPool, "resolve_upstream_id", lambda provider, model: model)
    monkeypatch.setattr(main, "_stream_chunk_with_public_model", rewrite)

    async def collect():
        return [item async for item in main._chat_with_retry_for_model(
            "g", [], False,
            _stable_response_model="stable-public",
            _group_request_identity=True,
        )]

    try:
        asyncio.run(collect())
    except Exception:
        pass
    assert rewritten == [("stable-public", True)]


def test_internal_selection_reuses_snapshot_on_reserve_retry(monkeypatch):
    snapshot = _snapshot(21)
    seen = []
    calls = 0

    async def candidate_models(cls, model_id, route=None, *, is_group=None, snapshot=None):
        seen.append(snapshot.generation)
        return [model_id]

    async def collect(cls, *args, snapshot=None, **kwargs):
        nonlocal calls
        seen.append(snapshot.generation)
        calls += 1
        account = SimpleNamespace(username=f"a{calls}", priority=0, weight=1)
        return ([{"provider_name": "p", "account_client": account, "routed_model": "m", "route_info": {"score": 1}}], [], {})

    async def reserve(cls, candidates, *args, **kwargs):
        return "client", "p", candidates[0]["account_client"]

    monkeypatch.setattr(ModelClientPool, "_candidate_models", classmethod(candidate_models))
    monkeypatch.setattr(ModelClientPool, "_collect_candidates", classmethod(collect))
    monkeypatch.setattr(ModelClientPool, "_select_and_reserve_candidates", classmethod(reserve))
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, model: asyncio.sleep(0, result=True)))
    monkeypatch.setattr(main.config.Config, "is_model_group", classmethod(lambda cls, model, snapshot=None: asyncio.sleep(0, result=False)))
    monkeypatch.setattr(main.config.Config, "get_api_key_strategy", classmethod(lambda cls, key: asyncio.sleep(0, result="sequential")))

    # The real implementation now exhausts reserve candidates in one collected set;
    # assert every snapshot-aware stage receives the exact same object.
    result = asyncio.run(ModelClientPool._get_available_client_with_provider("m", snapshot=snapshot))
    assert result[1] == "p"
    assert seen and set(seen) == {21}


def test_models_response_build_uses_memory_snapshot_only(monkeypatch):
    snapshot = _snapshot(31, metadata={"m": MappingProxyType({"model_id": "m", "max_tokens": 10})})
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "all_model_ids", classmethod(lambda cls: {"m"}))
    monkeypatch.setattr(ModelClientPool, "available_model_ids", classmethod(lambda cls: {"m"}))
    monkeypatch.setattr(ModelClientPool, "has_model_route", classmethod(lambda cls, model, available_only=False: True))
    ModelClientPool._models = []
    ModelClientPool._models_dirty = True
    ModelClientPool._models_requested_generation = 31
    ModelClientPool._models_built_generation = -1

    result = asyncio.run(ModelClientPool.get_models_response())
    assert result["data"][0]["id"] == "m"
    assert ModelClientPool._models_built_generation == 31


def test_public_models_response_hides_scheme_fallback_nodes_but_keeps_internal_cache(monkeypatch):
    derived_id = f"public-group{model_catalog.SCHEME_GROUP_SEP}1"
    groups = {
        "public-group": {
            "name": "public-group",
            "models": ("m",),
            "aliases": ("public-alias",),
            "enabled": True,
        },
        derived_id: {
            "name": derived_id,
            "models": ("m",),
            "aliases": (),
            "enabled": True,
        },
    }
    snapshot = _snapshot(
        32,
        groups=groups,
        metadata={"m": MappingProxyType({"model_id": "m", "max_tokens": 10})},
    )
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "all_model_ids", classmethod(lambda cls: {"m"}))
    monkeypatch.setattr(ModelClientPool, "available_model_ids", classmethod(lambda cls: {"m"}))
    monkeypatch.setattr(ModelClientPool, "has_model_route", classmethod(lambda cls, model, available_only=False: model == "m"))
    ModelClientPool._models = []
    ModelClientPool._models_by_id = {}
    ModelClientPool._models_dirty = True
    ModelClientPool._models_requested_generation = 32
    ModelClientPool._models_built_generation = -1

    result = asyncio.run(ModelClientPool.get_models_response())
    public_ids = {item["id"] for item in result["data"]}

    assert {"m", "public-group", "public-alias"} <= public_ids
    assert derived_id not in public_ids
    assert derived_id in ModelClientPool._models_by_id

    result["data"][0]["id"] = "mutated"
    second = asyncio.run(ModelClientPool.get_models_response())
    assert "mutated" not in {item["id"] for item in second["data"]}


def _pool(models: list[str], *, enabled: bool = True, disabled_accounts: tuple[bool, ...] = (False,)):
    """最小 pool 替身：只提供 _pool_can_serve / all_model_ids 需要的三个面。"""
    return SimpleNamespace(
        channel=SimpleNamespace(enabled=enabled, models=[{"model_id": m} for m in models]),
        clients=[SimpleNamespace(disabled=d, username=f"acct{i}") for i, d in enumerate(disabled_accounts)],
    )


def _reset_models_cache(generation: int):
    ModelClientPool._models = []
    ModelClientPool._models_by_id = {}
    ModelClientPool._models_dirty = True
    ModelClientPool._models_requested_generation = generation
    ModelClientPool._models_built_generation = -1


def test_public_models_hide_disabled_channel_and_all_disabled_accounts(monkeypatch):
    """对外 /v1/models 只暴露未禁用渠道 + 至少一个未禁用账号的模型。"""
    ids = ("ok", "off-channel", "off-accounts")
    snapshot = _snapshot(41, metadata={m: MappingProxyType({"model_id": m}) for m in ids})
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {
        "good": _pool(["ok"]),
        "disabled-channel": _pool(["off-channel"], enabled=False),
        "no-live-account": _pool(["off-accounts"], disabled_accounts=(True, True)),
    })
    _reset_models_cache(41)

    public_ids = {item["id"] for item in asyncio.run(ModelClientPool.get_models_response())["data"]}
    assert public_ids == {"ok"}

    # 同一份 _models 缓存喂管理端时仍是全量（含禁用），过滤只发生在对外读取层。
    admin_ids = {item["id"] for item in asyncio.run(ModelClientPool.get_admin_models_response())["data"]}
    assert set(ids) <= admin_ids


def test_public_models_follow_enabled_toggle_without_cache_invalidation(monkeypatch):
    """渠道 enabled 翻转不触发 _mark_models_dirty，过滤必须读取时现算。"""
    snapshot = _snapshot(42, metadata={"m": MappingProxyType({"model_id": "m"})})
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    pool = _pool(["m"])
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p": pool})
    _reset_models_cache(42)

    assert {i["id"] for i in asyncio.run(ModelClientPool.get_models_response())["data"]} == {"m"}

    built = ModelClientPool._models_built_generation
    pool.channel.enabled = False
    assert asyncio.run(ModelClientPool.get_models_response())["data"] == []
    # 没有重建缓存，纯读取时过滤
    assert ModelClientPool._models_built_generation == built

    pool.channel.enabled = True
    pool.clients[0].disabled = True
    assert asyncio.run(ModelClientPool.get_models_response())["data"] == []


def test_public_model_group_hidden_when_no_member_available(monkeypatch):
    """自定义模型：成员全不可用则整条不对外；仍有可用成员时保留。"""
    groups = {"grp": {"name": "grp", "models": ("live", "dead"), "aliases": (), "enabled": True}}
    snapshot = _snapshot(43, groups=groups, metadata={
        "live": MappingProxyType({"model_id": "live"}),
        "dead": MappingProxyType({"model_id": "dead"}),
    })
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    pools = {"a": _pool(["live"]), "b": _pool(["dead"])}
    monkeypatch.setattr(ModelClientPool, "_provider_pools", pools)
    _reset_models_cache(43)

    assert "grp" in {i["id"] for i in asyncio.run(ModelClientPool.get_models_response())["data"]}

    # 只禁用一个成员所在渠道：组仍有可用成员（dead 由 b 提供），整条保留。
    pools["a"].channel.enabled = False
    public_ids = {i["id"] for i in asyncio.run(ModelClientPool.get_models_response())["data"]}
    assert "grp" in public_ids
    assert "live" not in public_ids and "dead" in public_ids

    # 成员全不可用：整条不对外。
    pools["b"].channel.enabled = False
    assert asyncio.run(ModelClientPool.get_models_response())["data"] == []


def test_has_model_route_available_only_gate(monkeypatch):
    """has_model_route 默认看全量，available_only 才应用禁用过滤。"""
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {
        "off": _pool(["m"], enabled=False),
        "no-acct": _pool(["n"], disabled_accounts=(True,)),
    })
    assert ModelClientPool.has_model_route("m") is True
    assert ModelClientPool.has_model_route("m", available_only=True) is False
    assert ModelClientPool.has_model_route("n", available_only=True) is False
    assert ModelClientPool.all_model_ids() == {"m", "n"}
    assert ModelClientPool.available_model_ids() == set()


def _patch_key_filter(monkeypatch, *, whitelist=None, blacklist=None):
    """把某 Key 的模型白/黑名单直接钉在 Config 上，避免真的读 DB / api_keys。"""
    import config as gateway_config

    async def _filter(cls, _api_key):
        return (set(whitelist or ()), set(blacklist or ()))

    monkeypatch.setattr(gateway_config.Config, "get_api_key_model_filter", classmethod(_filter))


def test_public_models_apply_key_whitelist(monkeypatch):
    """传 api_key 时按该 Key 的白名单裁剪顶层模型。"""
    ids = ("a", "b", "c")
    snapshot = _snapshot(51, metadata={m: MappingProxyType({"model_id": m}) for m in ids})
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {m: _pool([m]) for m in ids})
    _reset_models_cache(51)
    _patch_key_filter(monkeypatch, whitelist=("a", "b"))

    scoped = {i["id"] for i in asyncio.run(ModelClientPool.get_models_response(api_key="sk-x"))["data"]}
    assert scoped == {"a", "b"}
    # 无 Key 调用仍是全量（内部/管理端语义）。
    unscoped = {i["id"] for i in asyncio.run(ModelClientPool.get_models_response())["data"]}
    assert unscoped == {"a", "b", "c"}


def test_public_models_apply_key_blacklist(monkeypatch):
    """传 api_key 时按该 Key 的黑名单剔除顶层模型。"""
    ids = ("a", "b", "c")
    snapshot = _snapshot(52, metadata={m: MappingProxyType({"model_id": m}) for m in ids})
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {m: _pool([m]) for m in ids})
    _reset_models_cache(52)
    _patch_key_filter(monkeypatch, blacklist=("b",))

    scoped = {i["id"] for i in asyncio.run(ModelClientPool.get_models_response(api_key="sk-x"))["data"]}
    assert scoped == {"a", "c"}


def test_public_model_group_members_pruned_by_key_filter(monkeypatch):
    """组成员逐个过 Key 名单并重写 models[]；被拉黑成员不能从数组漏出。"""
    groups = {"grp": {"name": "grp", "models": ("a", "b"), "aliases": (), "enabled": True}}
    snapshot = _snapshot(53, groups=groups, metadata={
        "a": MappingProxyType({"model_id": "a"}),
        "b": MappingProxyType({"model_id": "b"}),
    })
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"a": _pool(["a"]), "b": _pool(["b"])})
    _reset_models_cache(53)
    _patch_key_filter(monkeypatch, blacklist=("b",))

    data = asyncio.run(ModelClientPool.get_models_response(api_key="sk-x"))["data"]
    by_id = {item["id"]: item for item in data}
    assert "grp" in by_id
    # 组仍在（成员 a 可用），但成员数组只保留 a——b 被 Key 黑名单剔除。
    assert by_id["grp"]["models"] == ["a"]
    assert "b" not in by_id


def test_public_model_group_dropped_when_all_members_blocked_by_key(monkeypatch):
    """组的全部成员都被 Key 名单挡掉时，整条组不返回。"""
    groups = {"grp": {"name": "grp", "models": ("a", "b"), "aliases": (), "enabled": True}}
    snapshot = _snapshot(54, groups=groups, metadata={
        "a": MappingProxyType({"model_id": "a"}),
        "b": MappingProxyType({"model_id": "b"}),
    })
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"a": _pool(["a"]), "b": _pool(["b"])})
    _reset_models_cache(54)
    # 白名单只放行组主名，成员 a/b 都不在白名单里。
    _patch_key_filter(monkeypatch, whitelist=("grp",))

    data = asyncio.run(ModelClientPool.get_models_response(api_key="sk-x"))["data"]
    assert data == []


def test_public_model_group_member_pruned_by_availability_under_key(monkeypatch):
    """即便过了 Key 名单，落在禁用渠道上的成员也要从组数组里剔除。"""
    groups = {"grp": {"name": "grp", "models": ("a", "b"), "aliases": (), "enabled": True}}
    snapshot = _snapshot(55, groups=groups, metadata={
        "a": MappingProxyType({"model_id": "a"}),
        "b": MappingProxyType({"model_id": "b"}),
    })
    monkeypatch.setattr(model_catalog, "current_snapshot", lambda: snapshot)
    pools = {"a": _pool(["a"]), "b": _pool(["b"], enabled=False)}
    monkeypatch.setattr(ModelClientPool, "_provider_pools", pools)
    _reset_models_cache(55)
    _patch_key_filter(monkeypatch, whitelist=("grp", "a", "b"))

    data = asyncio.run(ModelClientPool.get_models_response(api_key="sk-x"))["data"]
    by_id = {item["id"]: item for item in data}
    assert by_id["grp"]["models"] == ["a"]

