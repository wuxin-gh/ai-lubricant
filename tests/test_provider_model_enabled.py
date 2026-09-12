"""渠道模型行级「启用/停用」开关：停用行不参与路由与对外模型列表，测试路径可旁路。

覆盖面：
- iter_model_candidates / all_model_ids / available_model_ids / has_model_route /
  get_provider_routes / get_runtime_routes_snapshot 全部跳过 enabled=False 的行；
  iter_model_candidates(include_disabled=True) 供测试/探测路径取回停用行。
- reload_channel_models 灌表保留 enabled（_route_row_from_db 归一化）。
- refresh_models 改名（to_update）沿用原行 extra_config——行级开关与 max_tokens
  等逐模型配置不再被定时同步改名清空。
"""
import asyncio

import rate_limiter
from channel import Channel
from rate_limiter import ModelClientPool


def _make_pool(models, provider=None):
    """带模型表的假池。models 是 Channel.models 的运行时行；enabled 缺省为启用。"""
    class _Client:
        disabled = False

        def __init__(self):
            self.provider = provider

    class _Pool:
        clients = []
        channel = Channel("prov", {"provider_name": "prov"})

        async def get_initialized_provider(self):
            return self.clients[0].provider

    pool = _Pool()
    pool.clients = [_Client()]
    pool.channel.register_models(models)
    return pool


def _install_pool(monkeypatch, pool):
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"prov": pool})
    monkeypatch.setattr(ModelClientPool, "get_provider_names", classmethod(lambda cls: ["prov"]))
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool if name == "prov" else None))


def test_iter_model_candidates_skips_disabled_row(monkeypatch):
    """停用行默认不出现在候选里；include_disabled=True（测试/探测语义）取回。"""
    pool = _make_pool([
        {"provider": "prov", "model_id": "m1", "upstream_model_id": "m1", "enabled": True},
        {"provider": "prov", "model_id": "m2", "upstream_model_id": "m2", "enabled": False},
    ])
    _install_pool(monkeypatch, pool)

    assert [(name, row["model_id"]) for name, _p, row in ModelClientPool.iter_model_candidates("m2")] == []
    got = [(name, row["model_id"]) for name, _p, row in ModelClientPool.iter_model_candidates("m2", include_disabled=True)]
    assert got == [("prov", "m2")]
    # 启用行不受影响。
    assert [(name, row["model_id"]) for name, _p, row in ModelClientPool.iter_model_candidates("m1")] == [("prov", "m1")]


def test_disabled_row_excluded_from_id_sets_and_snapshots(monkeypatch):
    """/v1/models、编辑器校验、模型路由页快照的数据源都不含停用行。"""
    pool = _make_pool([
        {"provider": "prov", "model_id": "on", "upstream_model_id": "on", "enabled": True},
        {"provider": "prov", "model_id": "off", "upstream_model_id": "off", "enabled": False},
    ])
    _install_pool(monkeypatch, pool)

    assert ModelClientPool.all_model_ids() == {"on"}
    assert ModelClientPool.available_model_ids() == {"on"}
    assert ModelClientPool.has_model_route("on") is True
    assert ModelClientPool.has_model_route("off") is False
    assert [r["model_id"] for r in ModelClientPool.get_provider_routes("prov")] == ["on"]
    snapshot = ModelClientPool.get_runtime_routes_snapshot()
    assert set(snapshot) == {"on"}


def test_disabled_row_defaults_to_enabled_without_flag(monkeypatch):
    """旧行（无 enabled 字段，缺省 None/True）按启用处理，不会被误过滤。"""
    pool = _make_pool([
        {"provider": "prov", "model_id": "legacy", "upstream_model_id": "legacy"},
    ])
    _install_pool(monkeypatch, pool)

    assert ModelClientPool.all_model_ids() == {"legacy"}
    assert ModelClientPool.has_model_route("legacy") is True


def test_reload_channel_models_keeps_enabled_flag(monkeypatch):
    """reload_channel_models（保存模型列表后）灌表带 enabled，停用行随即不再路由。"""
    pool = _make_pool([])
    _install_pool(monkeypatch, pool)

    ModelClientPool.reload_channel_models("prov", [
        {"provider": "prov", "upstream_model_id": "on", "model_id": "on", "enabled": True},
        {"provider": "prov", "upstream_model_id": "off", "model_id": "off", "enabled": False},
    ])

    by_id = {row["model_id"]: row for row in pool.channel.models}
    assert by_id["off"]["enabled"] is False
    assert by_id["on"]["enabled"] is True
    assert ModelClientPool.all_model_ids() == {"on"}


def test_refresh_models_update_preserves_extra_config(monkeypatch):
    """定时同步改名（to_update）沿用原行 extra_config：行级 enabled 开关与
    max_tokens 等逐模型配置不再被 ON CONFLICT 整覆盖清空。"""

    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [{"id": "glm-5.2:free"}]

    pool = _make_pool([], provider=_Provider())
    pool.channel.apply({"model_id_rewrite_rules": [{"pattern": r"[-:]free$", "replacement": ""}]})
    # 规则把 glm-5.2:free 改名为 glm-5.2；库内仍是旧名且带着行内配置 → 触发 to_update。
    provider_rows = [{
        "provider": "prov",
        "upstream_model_id": "glm-5.2:free",
        "model_id": "glm-5.2:free",
        "extra_config": {"max_tokens": 1024, "enabled": False},
        "enabled": False,
    }]

    upserts = []

    async def list_provider_models(cls, provider=None):
        return list(provider_rows)

    async def upsert_provider_model(cls, provider, upstream_model_id, model_id, extra_config=None):
        upserts.append({"provider": provider, "upstream_model_id": upstream_model_id, "model_id": model_id, "extra_config": extra_config})
        provider_rows.append({"provider": provider, "upstream_model_id": upstream_model_id, "model_id": model_id})

    async def delete_provider_model(cls, provider, upstream_model_id):
        return True

    async def rebuild_model_response_cache(cls):
        return None

    monkeypatch.setattr(ModelClientPool, "get_provider_names", classmethod(lambda cls: ["prov"]))
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"prov": pool})
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))
    monkeypatch.setattr(rate_limiter.PostgresClient, "upsert_provider_model", classmethod(upsert_provider_model))
    monkeypatch.setattr(rate_limiter.PostgresClient, "delete_provider_model", classmethod(delete_provider_model))
    monkeypatch.setattr(ModelClientPool, "rebuild_model_response_cache", classmethod(rebuild_model_response_cache))

    asyncio.run(ModelClientPool.refresh_models())

    assert len(upserts) == 1
    call = upserts[0]
    assert call["upstream_model_id"] == "glm-5.2:free"
    assert call["model_id"] == "glm-5.2"
    # 行内配置原样保留（含行级 enabled 开关），不再被清成 {}。
    assert call["extra_config"] == {"max_tokens": 1024, "enabled": False}
