import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import runtime_handlers
from rate_limiter import ModelClientPool


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler", "name", "reason"),
    [
        (runtime_handlers._on_metadata_event, "gpt-test", "sync:metadata:gpt-test"),
        (runtime_handlers._on_group_event, "group-test", "sync:group:group-test"),
    ],
)
async def test_catalog_event_reloads_and_marks_dirty_when_changed(
    monkeypatch, handler, name, reason
):
    reload_from_db = AsyncMock(return_value=True)
    mark_dirty = Mock()
    monkeypatch.setitem(
        sys.modules,
        "model_catalog",
        SimpleNamespace(reload_from_db=reload_from_db),
    )
    monkeypatch.setattr(ModelClientPool, "_mark_models_dirty", mark_dirty)

    await handler(name, {})

    reload_from_db.assert_awaited_once_with(reason=reason)
    mark_dirty.assert_called_once_with()


@pytest.mark.asyncio
async def test_catalog_event_does_not_mark_dirty_when_unchanged(monkeypatch):
    reload_from_db = AsyncMock(return_value=False)
    mark_dirty = Mock()
    monkeypatch.setitem(
        sys.modules,
        "model_catalog",
        SimpleNamespace(reload_from_db=reload_from_db),
    )
    monkeypatch.setattr(ModelClientPool, "_mark_models_dirty", mark_dirty)

    await runtime_handlers._on_group_event("__all__", {})

    reload_from_db.assert_awaited_once_with(reason="sync:group:__all__")
    mark_dirty.assert_not_called()


def test_register_all_is_idempotent(monkeypatch):
    handlers = {}
    reconcilers = []
    monkeypatch.setattr(runtime_handlers.runtime_sync, "_handlers", handlers)
    monkeypatch.setattr(runtime_handlers.runtime_sync, "_reconcilers", reconcilers)

    runtime_handlers.register_all()
    runtime_handlers.register_all()

    assert all(len(items) == 1 for items in handlers.values())
    assert reconcilers == [
        runtime_handlers._reconcile_catalog,
        runtime_handlers._reconcile_providers,
        runtime_handlers._reconcile_main_config,
        runtime_handlers._reconcile_header_templates,
    ]


@pytest.mark.asyncio
async def test_catalog_reconciler_reloads_catalog(monkeypatch):
    reload_and_dirty = AsyncMock(return_value=False)
    monkeypatch.setattr(
        runtime_handlers, "_reload_catalog_and_mark_models_dirty", reload_and_dirty
    )

    await runtime_handlers._reconcile_catalog()

    reload_and_dirty.assert_awaited_once_with("reconcile:catalog")


@pytest.mark.asyncio
async def test_channel_event_deletion_cleans_runtime_pool_and_cache(monkeypatch):
    """删除渠道事件：read_provider_async 抛 FileNotFoundError 时必须清运行池 + 内存快照。

    回归：read_provider_async 对已删渠道抛 FileNotFoundError 而非返回 None，
    若只判 `if cfg is None` 则清理分支是死代码，删渠道后其它实例的运行池残留。
    """
    import config

    pool = object()  # 占位运行池对象，验证会被 pop 掉
    ModelClientPool._provider_pools["lmspeed"] = pool
    store = config.CONFIG_STORE
    had_cache = getattr(store, "_providers_cache", None) is not None
    if had_cache:
        store._providers_cache["lmspeed"] = {"base_url": "https://example.test"}

    async def raise_missing(_name):
        raise FileNotFoundError(f"provider '{_name}' 不存在")

    monkeypatch.setattr(store, "read_provider_async", raise_missing)

    await runtime_handlers._on_channel_event("lmspeed", {})

    assert "lmspeed" not in ModelClientPool._provider_pools
    if had_cache:
        assert "lmspeed" not in store._providers_cache


@pytest.mark.asyncio
async def test_account_event_refreshes_main_before_resolving_proxy(monkeypatch):
    """账号的 proxy_id → proxy_config_id 依赖 main 配置里的代理池。接收方必须先刷
    main 缓存再解析，否则本机还没收到 EVENT_PROXY 时新代理条目查不到，账号会被
    误解析成直连（出站绕过代理）。断言顺序：read_main_async 先于账号入池。"""
    import config
    import admin

    calls: list = []
    store = config.CONFIG_STORE

    async def fake_read_main():
        calls.append("read_main")
        return {}

    async def fake_read_provider(_name):
        calls.append("read_provider")
        return {"accounts": [{"username": "u1", "proxy_id": "p9"}]}

    monkeypatch.setattr(store, "read_main_async", fake_read_main)
    monkeypatch.setattr(store, "read_provider_async", fake_read_provider)
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", lambda name: object())
    monkeypatch.setattr(admin, "_provider_rpm", lambda cfg: 0)
    monkeypatch.setattr(admin, "_provider_extra", lambda name, cfg: {})
    monkeypatch.setattr(
        admin, "_reload_account_into_pool",
        lambda *a, **k: calls.append("reload_account"),
    )

    await runtime_handlers._on_account_event(
        "demo:u1", {"extra": {"provider": "demo", "username": "u1"}}
    )

    assert calls.index("read_main") < calls.index("reload_account")


@pytest.mark.asyncio
async def test_reconcile_providers_resolves_proxies_once_for_all_accounts(monkeypatch):
    """60s 对账遍历账号时只规范化一次代理池并透传，避免 O(账号数 × 代理数) 重复解析。"""
    import config
    import admin

    monkeypatch.setattr(config.Config, "reload_from_db", AsyncMock(return_value={}))
    monkeypatch.setattr(
        config.Config, "get_providers",
        AsyncMock(return_value={"demo": {"accounts": [
            {"username": "u1"}, {"username": "u2"}, {"username": "u3"},
        ]}}),
    )
    monkeypatch.setattr(config.CONFIG_STORE, "read_main_async", AsyncMock(return_value={}))
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", lambda name: object())
    monkeypatch.setattr(admin, "_provider_rpm", lambda cfg: 0)
    monkeypatch.setattr(admin, "_provider_extra", lambda name, cfg: {})
    monkeypatch.setattr(runtime_handlers, "_refresh_proxy_runtime", AsyncMock())

    read_proxies = Mock(return_value=[{"id": "p1", "url": "http://p1.example:8080"}])
    monkeypatch.setattr(admin, "_read_proxies", read_proxies)
    passed: list = []
    monkeypatch.setattr(
        admin, "_reload_account_into_pool",
        lambda *a, proxies=None, **k: passed.append(proxies),
    )

    await runtime_handlers._reconcile_providers()

    read_proxies.assert_called_once_with()
    assert len(passed) == 3
    assert all(p is read_proxies.return_value for p in passed)


@pytest.mark.asyncio
async def test_account_event_for_deleted_provider_returns_silently(monkeypatch):
    """账号事件落到已删渠道：read_provider_async 抛 FileNotFoundError 应静默返回，
    不应打成“处理失败”WARNING（渠道级清理由 channel 事件负责）。"""
    import config

    store = config.CONFIG_STORE

    async def raise_missing(_name):
        raise FileNotFoundError(f"provider '{_name}' 不存在")

    monkeypatch.setattr(store, "read_provider_async", raise_missing)

    # 不应抛出，也不应触发任何账号池操作
    await runtime_handlers._on_account_event(
        "lmspeed:u1", {"extra": {"provider": "lmspeed", "username": "u1"}}
    )

