"""小时预聚合的调度语义。

仪表盘以 current_hour 为界：current_hour 之后实时查 request_logs，之前只读
hourly_dashboard_stats。所以“已结束但未聚合”的小时在看板上就是缺数的，
这组测试锁住：补齐范围要覆盖所有这类小时（无总量上限、分批推进、失败即停）、
唤醒要对齐整点。
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from db import PostgresClient
from rate_limiter import ModelClientPool

ROOT = Path(__file__).resolve().parents[1]
# 业务模块已收进 server/（见 refactor(structure)），根级路径作为兼容回退。
RATE_LIMITER_SRC = ROOT / "server" / "rate_limiter.py"
if not RATE_LIMITER_SRC.exists():
    RATE_LIMITER_SRC = ROOT / "rate_limiter.py"


def _patch_aggregate(monkeypatch, last_hour, earliest=None):
    """记录 aggregate_hourly_logs 收到的区间，并固定聚合表/日志进度。

    聚合表非空以 last_hour 为进度（None 表示空表，此时查询 earliest_request_log_hour）。
    """
    calls: list[tuple[datetime, datetime]] = []

    async def fake_last_hour():
        return last_hour

    async def fake_earliest():
        return earliest

    async def fake_aggregate(from_hour, to_hour):
        calls.append((from_hour, to_hour))

    monkeypatch.setattr(PostgresClient, "last_aggregated_dashboard_hour", fake_last_hour)
    monkeypatch.setattr(PostgresClient, "earliest_request_log_hour", fake_earliest)
    monkeypatch.setattr(PostgresClient, "aggregate_hourly_logs", fake_aggregate)
    return calls


def _now_hour():
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def test_catchup_covers_every_unaggregated_hour(monkeypatch):
    """漂移/停机导致落后多个小时时，一轮要把它们全部补上。

    旧实现只算 now-1h，中间被跳过的小时永不回看，看板上是永久空洞。
    """
    now = _now_hour()
    calls = _patch_aggregate(monkeypatch, now - timedelta(hours=5))

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    assert calls, "落后 5 小时必须补齐"
    from_hour, to_hour = calls[0]
    # 从落后的那个小时一路补到 current_hour（不含当前小时，它走实时段）。
    assert from_hour == now - timedelta(hours=5)
    assert calls[-1][1] == now
    # 区间连续无缝、无重叠。
    for (a_start, a_end), (b_start, b_end) in zip(calls, calls[1:]):
        assert a_end == b_start
        assert a_start < a_end


def test_catchup_recomputes_last_aggregated_hour(monkeypatch):
    """从 max(hour) 本身重算，而不是它的下一个小时。

    该桶可能在聚合落笔时仍是半截数据（部分请求 status 尚为 requesting），
    只有重算它才能补正。
    """
    now = _now_hour()
    partial_hour = now - timedelta(hours=1)
    calls = _patch_aggregate(monkeypatch, partial_hour)

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    assert calls == [(partial_hour, now)]


def test_catchup_batches_long_downtime_without_cap(monkeypatch):
    """超长停机（聚合表非空、落后数月）不再被截断：按批连续调用、覆盖全程。

    旧实现把单轮区间钳到 48h，超出部分永久缺数；分批推进后任意时长一轮补完。
    """
    now = _now_hour()
    calls = _patch_aggregate(monkeypatch, now - timedelta(days=400))

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    assert calls, "落后 400 天也必须开始补"
    batch_hours = ModelClientPool.HOURLY_AGGREGATE_BATCH_HOURS
    for from_hour, to_hour in calls:
        span = (to_hour - from_hour).total_seconds() / 3600
        assert span <= batch_hours, "单批不能超过批大小"
        assert span > 0
    assert calls[0][0] == now - timedelta(days=400)
    assert calls[-1][1] == now
    for (a_start, a_end), (b_start, _b_end) in zip(calls, calls[1:]):
        assert a_end == b_start


def test_catchup_empty_table_starts_from_earliest_log(monkeypatch):
    """聚合表为空（新装/被清）时从 request_logs 最早小时起补，而非只补一小时。

    清理有聚合水位护栏，聚合表为空时原始日志会被扣留，所以这个范围天然受
    日志保留期约束，量级有限。
    """
    now = _now_hour()
    earliest = now - timedelta(days=3)
    calls = _patch_aggregate(monkeypatch, None, earliest=earliest)

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    assert calls
    assert calls[0][0] == earliest.replace(minute=0, second=0, microsecond=0)
    assert calls[-1][1] == now


def test_catchup_empty_tables_are_noop(monkeypatch):
    """聚合表与日志表都为空时无事可做，不能发起无意义的聚合。"""
    calls = _patch_aggregate(monkeypatch, None, earliest=None)

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    assert calls == []


def test_catchup_fails_fast_keeps_watermark_behind_data(monkeypatch):
    """某批失败后立即停止：聚合表 max(hour)（=清理水位依据）绝不越过未聚合数据，
    下一整点从失败批重试。fake 用状态模拟真实库：水位 = 最后一个成功批的终点。"""
    now = _now_hour()
    calls: list[tuple[datetime, datetime]] = []
    state = {"watermark": now - timedelta(days=3)}

    async def fake_last_hour():
        return state["watermark"]

    async def fake_earliest():
        return None

    async def flaky_aggregate(from_hour, to_hour):
        calls.append((from_hour, to_hour))
        if len(calls) == 2:
            raise RuntimeError("db down")
        state["watermark"] = to_hour

    monkeypatch.setattr(PostgresClient, "last_aggregated_dashboard_hour", fake_last_hour)
    monkeypatch.setattr(PostgresClient, "earliest_request_log_hour", fake_earliest)
    monkeypatch.setattr(PostgresClient, "aggregate_hourly_logs", flaky_aggregate)

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    # 第 2 批抛错后第 3 批不再执行，水位停在成功批的终点。
    assert len(calls) == 2
    assert state["watermark"] == calls[0][1]
    failed_from = calls[1][0]

    # 下次调用从失败的那一批重试（起点即第 2 批的 from，不跳过任何小时）。
    calls.clear()

    async def ok_aggregate(from_hour, to_hour):
        calls.append((from_hour, to_hour))
        state["watermark"] = to_hour

    monkeypatch.setattr(PostgresClient, "aggregate_hourly_logs", ok_aggregate)
    asyncio.run(ModelClientPool._aggregate_hourly_stats())
    assert calls[0][0] == failed_from


def test_current_hour_is_never_aggregated(monkeypatch):
    """当前小时未结束，绝不能进聚合表：它由实时段负责，重复会导致 token 翻倍。"""
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    calls = _patch_aggregate(monkeypatch, current_hour)

    asyncio.run(ModelClientPool._aggregate_hourly_stats())

    # 已聚合到当前小时时无事可做，不能把未完成的小时写进去。
    assert calls == []


def test_aggregate_failure_is_swallowed(monkeypatch):
    """单轮失败不能把后台任务打死，下一整点还要继续。"""
    now = _now_hour()

    async def fake_last_hour():
        return now - timedelta(hours=1)

    async def fake_earliest():
        return None

    async def boom(from_hour, to_hour):
        raise RuntimeError("db down")

    monkeypatch.setattr(PostgresClient, "last_aggregated_dashboard_hour", fake_last_hour)
    monkeypatch.setattr(PostgresClient, "earliest_request_log_hour", fake_earliest)
    monkeypatch.setattr(PostgresClient, "aggregate_hourly_logs", boom)

    asyncio.run(ModelClientPool._aggregate_hourly_stats())


def test_hourly_loop_aligns_to_hour_boundary():
    """唤醒必须对齐整点，不能裸 sleep(3600)。

    裸 sleep 把触发时刻锁在进程启动的分钟偏移上，于是每小时开头那段时间里，
    刚结束的小时已被看板划进预聚合段却还没聚合，表现为“最近一小时不及时”；
    且聚合耗时会让触发点累积右移，最终整小时被跳过。
    """
    source = RATE_LIMITER_SRC.read_text(encoding="utf-8")
    body = source[source.index("async def _hourly_stats_loop"):source.index("    async def stop(cls):")]

    assert "asyncio.sleep(3600)" not in body
    # 睡到下一个整点，而非固定时长。
    assert "next_hour" in body
    assert "(next_hour - now).total_seconds()" in body
    # 启动即跑一次：把停机/半截小时的空档补齐。
    assert body.index("await cls._aggregate_hourly_stats()") < body.index("while True:")


def test_cleanup_aggregates_before_deleting():
    """清理必须先聚合再删（aggregate-then-delete）：水位护栏是硬保证，
    先聚合让水位尽量追平，减少被护栏扣留的行。"""
    source = RATE_LIMITER_SRC.read_text(encoding="utf-8")
    body = source[source.index("async def clean_response_data"):source.index("    async def clean_response_loop")]
    assert body.index("await cls._aggregate_hourly_stats()") < body.index("cleanup_request_logs(")
