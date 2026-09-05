"""PostgresClient.init 轻量模式契约测试。

验证本次修复的根因解耦：MCP Runtime 以 lightweight=True 初始化 DB，只建表（幂等），
跳过主服务专属的启动期数据维护（usage 规范化 / 各类迁移）；主服务用默认完整模式，
行为不变。

这些重活会随 request_logs 增长而变慢，塞进 MCP 启动路径会拖慢启动、增加被 supervisor
健康检查误判重启的概率——所以 MCP 路径必须跳过。

用 monkeypatch 把连接池创建和所有子步骤替换成记录调用的 stub，只测 init 的分支逻辑，
不连真实 Postgres。
"""
import asyncio
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from db import PostgresClient

# 建表方法（轻量与完整模式都应调用）
_TABLE_STEPS = ["create_tables", "create_agent_tables", "create_multi_agent_tables", "create_builtin_tool_tables"]
# 主服务专属重活（仅完整模式应调用）。仪表盘小时聚合不在此列：启动不做任何
# 回填/重建（聚合纯增量，见 rate_limiter._hourly_stats_loop）。
_HEAVY_STEPS = [
    "_normalize_legacy_usage_tokens",
    "migrate_provider_configs_from_app_config",
    "migrate_provider_models_from_legacy_columns",
    "migrate_model_routing_from_app_config",
    "migrate_provider_filters_to_tags",
    "migrate_model_metadata_into_model_groups",
    "migrate_limit_policies_from_provider_configs",
    "migrate_freeze_policy_data",
]


class _FakePool:
    def terminate(self):
        pass


@pytest.fixture
def stub_init(monkeypatch):
    """把 create_pool + 所有 init 子步骤替换成调用记录器；每步返回后清理 pool。"""
    called: list[str] = []

    async def _fake_create_pool(*args, **kwargs):
        return _FakePool()

    # init() 在函数体内 `import asyncpg`，拿到的是 sys.modules["asyncpg"]，
    # 直接 patch asyncpg 模块的 create_pool 即可命中。
    import asyncpg
    monkeypatch.setattr(asyncpg, "create_pool", _fake_create_pool)

    for name in _TABLE_STEPS + _HEAVY_STEPS:
        def _make(n):
            async def _step(cls, *a, **k):
                called.append(n)
            return classmethod(_step)
        monkeypatch.setattr(PostgresClient, name, _make(name))

    # 每个测试前后确保 pool 为空，避免 init 幂等短路 / 污染其它测试。
    monkeypatch.setattr(PostgresClient, "pool", None, raising=False)
    yield called
    PostgresClient.pool = None


def test_lightweight_skips_heavy_steps(stub_init):
    called = stub_init
    asyncio.run(PostgresClient.init(lightweight=True))
    # 建表都跑
    for step in _TABLE_STEPS:
        assert step in called, f"lightweight 应建表 {step}"
    # 重活一个都不跑
    for step in _HEAVY_STEPS:
        assert step not in called, f"lightweight 不应执行 {step}"


def test_full_mode_runs_all_steps(stub_init):
    called = stub_init
    asyncio.run(PostgresClient.init(lightweight=False))
    for step in _TABLE_STEPS + _HEAVY_STEPS:
        assert step in called, f"完整模式应执行 {step}"


def test_default_mode_is_full(stub_init):
    """不传参数（主服务默认）等同完整模式。"""
    called = stub_init
    asyncio.run(PostgresClient.init())
    assert "_normalize_legacy_usage_tokens" in called
    assert "migrate_freeze_policy_data" in called


def test_init_idempotent_when_pool_exists(stub_init, monkeypatch):
    """pool 已存在时 init 直接返回，不重复建表/重活（保证 CONFIG_STORE.init 二次调用无副作用）。"""
    called = stub_init
    monkeypatch.setattr(PostgresClient, "pool", _FakePool(), raising=False)
    asyncio.run(PostgresClient.init(lightweight=False))
    assert called == [], "pool 已存在时 init 应短路，不执行任何步骤"
