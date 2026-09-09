"""日志清理的聚合水位护栏。

request_logs 是 hourly_dashboard_stats 的唯一素材，而聚合表是仪表盘历史的唯一
长期存储（超出日志保留期的历史只存在于聚合表）。因此清理必须以聚合水位
（hourly_dashboard_stats 的 max(hour)+1h）为硬下界：没聚合的行绝不删；聚合表
为空则整体跳过——否则删光素材后统计出现永久缺口。

聚合与清理还必须以同一把 advisory lock 互斥：聚合的 DELETE/INSERT 分语句自动
提交，清理若在两者之间删走原始行，该批会按残缺素材算出偏小的统计并覆盖正确值。
"""
from __future__ import annotations

import asyncio

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_source(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def _cleanup_body() -> str:
    db = load_source("server/db.py")
    return db[db.index("async def cleanup_request_logs"):db.index("async def _clean_old_hourly_stats")]


def test_cleanup_fetches_aggregation_watermark():
    body = _cleanup_body()
    assert "SELECT max(hour) + interval '1 hour' FROM hourly_dashboard_stats" in body


def test_cleanup_skips_when_aggregate_table_empty():
    body = _cleanup_body()
    assert "if watermark is None" in body
    # 跳过必须发生在任何 DELETE 之前。
    assert body.index("if watermark is None") < body.index("DELETE FROM request_logs")


def test_age_delete_branch_respects_watermark():
    body = _cleanup_body()
    age_branch = body[body.index("# 1) 按天数删"):body.index("# 2) 可选按条数删")]
    assert "created_at < now() - make_interval(days => $1)" in age_branch
    assert "created_at < $3::timestamptz" in age_branch


def test_count_delete_branch_respects_watermark():
    body = _cleanup_body()
    count_branch = body[body.index("# 2) 可选按条数删"):body.index("if deleted:")]
    # 水位在 OFFSET 子查询之外：豁免窗是“全局最新 max_entries 条”，
    # 未聚合的行即使超出豁免窗也被水位扣留。
    assert count_branch.index("created_at < $2::timestamptz") < count_branch.index("SELECT id FROM request_logs")
    assert "ORDER BY id DESC OFFSET $1" in count_branch


def test_cleanup_and_aggregation_share_advisory_lock():
    """聚合（aggregate_hourly_logs）与清理持同一把 advisory lock 互斥，
    持锁先于任何 DELETE，释锁走 finally（异常路径也在连接归还前解锁）。"""
    db = load_source("server/db.py")
    aggregate_body = db[
        db.index("async def aggregate_hourly_logs"):
        db.index("    @classmethod\n    async def _normalize_legacy_usage_tokens")
    ]
    cleanup_body = _cleanup_body()
    for body in (aggregate_body, cleanup_body):
        assert "pg_advisory_lock($1)" in body
        assert "pg_advisory_unlock($1)" in body
        assert "HOURLY_STATS_LOCK_KEY" in body
    assert aggregate_body.index("pg_advisory_lock($1)") < aggregate_body.index("DELETE FROM hourly_log_stats")
    assert cleanup_body.index("pg_advisory_lock($1)") < cleanup_body.index("DELETE FROM request_logs")


def test_cleanup_aggregates_before_deleting_runtime(monkeypatch):
    """清理入口先跑一轮聚合（aggregate-then-delete），运行时行为锁定。"""
    import user_platform.notify_core as notify_core
    import rate_limiter
    from db import PostgresClient
    from rate_limiter import ModelClientPool

    order: list[str] = []

    async def _fake_aggregate():
        order.append("aggregate")

    async def _fake_cleanup(*args, **kwargs):
        order.append("cleanup")
        return 0

    async def _fake_payload(*args, **kwargs):
        return ""

    async def _fake_emit(*args, **kwargs):
        return None

    monkeypatch.setattr(rate_limiter.config.Config, "keep_response_hours", lambda: 720)
    monkeypatch.setattr(rate_limiter.config.Config, "get_log_retention_days", lambda: 30)
    monkeypatch.setattr(rate_limiter.config.Config, "get_log_retention_max_entries", lambda: 0)
    monkeypatch.setattr(ModelClientPool, "_aggregate_hourly_stats", _fake_aggregate)
    monkeypatch.setattr(PostgresClient, "cleanup_request_logs", _fake_cleanup)
    monkeypatch.setattr(ModelClientPool, "_cleanup_payload_store", _fake_payload)
    monkeypatch.setattr(notify_core, "emit_notification", _fake_emit)

    asyncio.run(ModelClientPool.clean_response_data("daily"))

    assert order == ["aggregate", "cleanup"]
