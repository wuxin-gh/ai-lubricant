"""API Key 内存快照校验：鉴权走内存，不在请求链路上查库。

覆盖三件事：命中/未命中/禁用的判定与查库版本一致、请求链路确实不碰 DB、
DB 故障时保留旧快照（清空会让所有 Key 立刻 401，比拿着略旧的快照更危险）。
"""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config as config_module
from config import Config

ROOT_KEY = {
    "id": 1,
    "key": "sk-root",
    "name": "root",
    "disabled": False,
    "parent_id": None,
    "model_whitelist": [],
    "model_blacklist": [],
}
CHILD_KEY = {
    "id": 2,
    "key": "sk-child",
    "name": "child",
    "disabled": False,
    "parent_id": 1,
    "model_whitelist": [],
    "model_blacklist": [],
}
DISABLED_KEY = {
    "id": 3,
    "key": "sk-off",
    "name": "off",
    "disabled": True,
    "parent_id": None,
}


@pytest.fixture(autouse=True)
def _clean_cache(monkeypatch):
    """每个用例前后都清空类级快照，避免跨用例串味。"""
    monkeypatch.setattr(Config, "_api_keys_cache", [], raising=False)
    monkeypatch.setattr(Config, "_api_keys_by_value", {}, raising=False)
    monkeypatch.setattr(Config, "_api_keys_by_id", {}, raising=False)
    monkeypatch.setattr(Config, "_api_keys_cache_loaded", False, raising=False)
    Config.clear_api_key_request_config()
    yield
    Config.clear_api_key_request_config()


def _stub_db(monkeypatch, keys, counter=None):
    async def list_api_keys():
        if counter is not None:
            counter.append(1)
        return list(keys)

    monkeypatch.setattr(config_module.PostgresClient, "list_api_keys", list_api_keys)


def _no_broadcast(monkeypatch):
    """publish 需要 Redis；测试只关心内存快照，把广播打成 no-op。"""
    import runtime_sync

    async def publish(*_args, **_kwargs):
        return None

    monkeypatch.setattr(runtime_sync, "publish", publish)


def test_lookup_hits_memory_and_reads_db_once(monkeypatch):
    """首次惰性灌一次，之后多次校验都不再查库。"""
    calls: list[int] = []
    _stub_db(monkeypatch, [ROOT_KEY, CHILD_KEY, DISABLED_KEY], calls)
    _no_broadcast(monkeypatch)

    async def scenario():
        first = await Config.get_api_key_config("sk-root")
        for _ in range(5):
            await Config.get_api_key_config("sk-root")
        return first

    got = asyncio.run(scenario())

    assert got is not None and got["id"] == 1
    assert len(calls) == 1


def test_unknown_and_disabled_keys_are_rejected(monkeypatch):
    _stub_db(monkeypatch, [ROOT_KEY, DISABLED_KEY])
    _no_broadcast(monkeypatch)

    async def scenario():
        return (
            await Config.get_api_key_config("sk-nope"),
            await Config.get_api_key_config("sk-off"),
            await Config.get_api_key_config("sk-off", include_disabled=True),
        )

    unknown, disabled, disabled_included = asyncio.run(scenario())

    assert unknown is None
    # 禁用 Key 默认不返回；鉴权层要靠 include_disabled 拿到行才能区分 401/403。
    assert disabled is None
    assert disabled_included is not None and disabled_included["id"] == 3


def test_parent_lookup_by_id_uses_memory(monkeypatch):
    """子 Key 回查父 Key 也走内存，不落 PostgresClient.get_api_key_by_id。"""
    _stub_db(monkeypatch, [ROOT_KEY, CHILD_KEY])
    _no_broadcast(monkeypatch)

    async def boom(*_args, **_kwargs):
        raise AssertionError("父 Key 回查不应查库")

    monkeypatch.setattr(config_module.PostgresClient, "get_api_key_by_id", boom)

    async def scenario():
        child = await Config.get_api_key_config("sk-child")
        return child, await Config.get_api_key_by_id(child["parent_id"])

    child, parent = asyncio.run(scenario())

    assert child["parent_id"] == 1
    assert parent is not None and parent["key"] == "sk-root"


def test_refresh_failure_keeps_previous_snapshot(monkeypatch):
    """DB 故障时保留旧快照：清空会让所有在用 Key 立刻变无效而全量 401。"""
    _no_broadcast(monkeypatch)
    _stub_db(monkeypatch, [ROOT_KEY])
    asyncio.run(Config.refresh_api_keys_cache())

    async def failing():
        raise RuntimeError("pg down")

    monkeypatch.setattr(config_module.PostgresClient, "list_api_keys", failing)

    async def scenario():
        await Config.refresh_api_keys_cache()
        return await Config.get_api_key_config("sk-root")

    assert asyncio.run(scenario()) is not None


def test_refresh_picks_up_writes(monkeypatch):
    """写路径重灌后新 Key 立刻可用、被删的 Key 立刻失效。"""
    _no_broadcast(monkeypatch)
    _stub_db(monkeypatch, [ROOT_KEY])

    async def scenario():
        before_new = await Config.get_api_key_config("sk-child")
        _stub_db(monkeypatch, [CHILD_KEY])
        await Config.refresh_api_keys_cache()
        return before_new, await Config.get_api_key_config("sk-child"), await Config.get_api_key_config("sk-root")

    before_new, after_new, removed = asyncio.run(scenario())

    assert before_new is None
    assert after_new is not None and after_new["id"] == 2
    assert removed is None


def test_refresh_broadcasts_snapshot_event(monkeypatch):
    """写路径重灌要广播，否则其它进程只能等 60s 对账才看到 Key 变更。"""
    import runtime_sync

    published: list[tuple] = []

    async def publish(event_type, name=None, version=None, extra=None):
        published.append((event_type, name, extra))

    monkeypatch.setattr(runtime_sync, "publish", publish)
    _stub_db(monkeypatch, [ROOT_KEY])

    asyncio.run(Config.refresh_api_keys_cache())
    assert published == [(runtime_sync.EVENT_APIKEY, None, {"snapshot": True})]

    # 订阅端/对账回调重灌不再广播，否则两个实例会互相激发。
    published.clear()
    asyncio.run(Config.refresh_api_keys_cache(broadcast=False))
    assert published == []
