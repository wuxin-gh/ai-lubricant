"""ClickHouse payload 清理的条数裁剪回归测试。

max_entries 之前只作用于 Postgres 主表，ClickHouse 侧只按天数删——结果日志行
被裁到 5000 条后，对应 payload 成了查不到的孤儿继续占盘。这里锁定：
* 条数边界优先取 Postgres「第 N 条现存日志」的时间（主删从随，孤儿必被带走）；
* PG 边界拿不到时退回表内第 ``2 * max_entries`` 个最新事件的时间兜底；
* 天数、PG、兜底三个边界合并取更严格（时间更新）的一个，与 Postgres 的「或」语义一致；
* 整月早于边界的分区 DROP（立即回收磁盘），边界月走 mutation。
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime

import pytest

from integrations.clickhouse import ClickHousePayloadClient


class FakeCH:
    """按 SQL 特征分发的假客户端，记录所有 command。"""

    def __init__(self, *, boundary=None, expired=(), remaining=0):
        self.boundary = boundary
        self.expired = list(expired)
        self.remaining = remaining
        self.commands: list[str] = []

    async def query(self, sql, parameters=None):
        text = " ".join(sql.split())
        if "ORDER BY created_at DESC" in text:
            return [{"created_at": self.boundary}] if self.boundary else []
        if "FROM system.parts" in text and "GROUP BY partition" in text:
            return [{"partition": p} for p in self.expired]
        if "count() AS c" in text:
            return [{"c": self.remaining}]
        if "active = 0" in text:
            return [{"n": 0, "bytes": 0}]
        raise AssertionError(f"unexpected query: {text}")

    async def command(self, sql, parameters=None):
        self.commands.append(" ".join(sql.split()))
        return None


def _make_client(fake: FakeCH) -> ClickHousePayloadClient:
    client = ClickHousePayloadClient(addr="10.0.0.1", database="model_api")
    client._client = object()  # 绕过未连接检查；query/command 已被替换
    client.query = fake.query
    client.command = fake.command
    return client


def test_count_boundary_uses_double_event_limit():
    """边界取第 2N 个最新事件；同时配置天数时取更严格的时间。"""
    boundary = datetime(2026, 8, 31, 12, 0, 0, 123000)
    fake = FakeCH(boundary=boundary, expired=["202608"], remaining=90000)
    client = _make_client(fake)

    result = asyncio.run(client.delete_payloads_older_than(7, max_entries=5000))

    assert result["count_trim_boundary"] == "2026-08-31 12:00:00.123"
    assert result["dropped_partitions"] == ["202608"]
    assert result["mutation_submitted"] is True
    drop = [c for c in fake.commands if "DROP PARTITION" in c]
    delete = [c for c in fake.commands if "DELETE WHERE" in c]
    assert drop == ["ALTER TABLE request_log_payloads DROP PARTITION 202608"]
    # 天数边界与条数边界取 greatest（时间更新者），与 Postgres「或」语义一致。
    assert "greatest(now() - INTERVAL 7 DAY, toDateTime64('2026-08-31 12:00:00.123', 3))" in delete[0]


def test_no_trim_when_table_within_entry_limit():
    """表未超出条数上限时退回纯天数行为，不产生条数边界。"""
    fake = FakeCH(boundary=None, expired=[], remaining=0)
    client = _make_client(fake)

    result = asyncio.run(client.delete_payloads_older_than(7, max_entries=5000))

    assert result["count_trim_boundary"] is None
    assert result["dropped_partitions"] == []
    assert result["mutation_submitted"] is False
    assert fake.commands == []


def test_noop_when_neither_day_nor_entries_configured():
    """两个口径都没配时不发任何 SQL。"""
    fake = FakeCH(boundary=datetime(2026, 8, 31))
    client = _make_client(fake)

    result = asyncio.run(client.delete_payloads_older_than(0, max_entries=0))

    assert result["mutation_submitted"] is False
    assert fake.commands == []


def test_entry_only_trims_without_day_window():
    """keep_days=0 时条数边界独立生效。"""
    boundary = datetime(2026, 9, 1, 0, 0, 0)
    fake = FakeCH(boundary=boundary, remaining=5)
    client = _make_client(fake)

    result = asyncio.run(client.delete_payloads_older_than(0, max_entries=3))

    assert result["count_trim_boundary"] == "2026-09-01 00:00:00.000"
    delete = [c for c in fake.commands if "DELETE WHERE" in c]
    assert len(delete) == 1
    # 不含天数项，边界就是条数时间本身；不得引入注入面。
    assert "toDateTime64('2026-09-01 00:00:00.000', 3)" in delete[0]
    assert not re.search(r"[;`\\]", delete[0])


def test_postgres_boundary_anchors_kept_set():
    """主表第 N 条日志的时间作为边界：主表删掉的行，对应 payload 必然被带走。"""
    pg_boundary = datetime(2026, 8, 31, 18, 0, 0, 500000)
    fake = FakeCH(boundary=datetime(2026, 8, 31, 10, 0, 0), expired=["202608"], remaining=90000)
    client = _make_client(fake)

    result = asyncio.run(
        client.delete_payloads_older_than(7, max_entries=5000, entries_boundary=pg_boundary)
    )

    # PG 边界(18:00)比 CH 事件兜底边界(10:00)更严格，cutoff 取 greatest 三者合并。
    delete = [c for c in fake.commands if "DELETE WHERE" in c]
    assert "greatest(now() - INTERVAL 7 DAY, toDateTime64('2026-08-31 18:00:00.500', 3), toDateTime64('2026-08-31 10:00:00.000', 3))" in delete[0]
    assert result["count_trim_boundary"] == "2026-08-31 18:00:00.500"
    assert result["dropped_partitions"] == ["202608"]


def test_postgres_boundary_alone_applies():
    """只拿到 PG 边界（CH 兜底查询为空）时独立生效。"""
    pg_boundary = datetime(2026, 8, 31, 18, 0, 0)
    fake = FakeCH(boundary=None, remaining=10)
    client = _make_client(fake)

    result = asyncio.run(
        client.delete_payloads_older_than(0, max_entries=5000, entries_boundary=pg_boundary)
    )

    delete = [c for c in fake.commands if "DELETE WHERE" in c]
    assert delete == ["ALTER TABLE request_log_payloads DELETE WHERE created_at < toDateTime64('2026-08-31 18:00:00.000', 3)"]
    assert result["count_trim_boundary"] == "2026-08-31 18:00:00.000"


def test_postgres_boundary_sanitized():
    """边界值必须是严格时间格式，否则拒绝（防御拼接注入）。"""
    fake = FakeCH(boundary=None, remaining=10)
    client = _make_client(fake)

    result = asyncio.run(
        client.delete_payloads_older_than(0, max_entries=0, entries_boundary="2026-08-31 18:00:00'); DROP TABLE x;--")
    )

    assert fake.commands == []
    assert result["count_trim_boundary"] is None
    assert result["mutation_submitted"] is False


# ---- rate_limiter 层：PG 边界的读取与转发 ----

class _RecordingClient:
    """记录 delete_payloads_older_than 入参的桩客户端。"""

    calls: list[tuple] = []

    def __init__(self, **kwargs):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def delete_payloads_older_than(self, keep_days, max_entries=0, entries_boundary=None):
        type(self).calls.append((keep_days, max_entries, entries_boundary))
        return {"dropped_partitions": [], "mutation_submitted": False, "inactive_parts": 0, "inactive_bytes": 0}


@pytest.mark.asyncio
async def test_cleanup_payload_store_forwards_pg_boundary(monkeypatch):
    import types

    import clickhouse_config
    import db
    import integrations.clickhouse as ch_module
    from rate_limiter import ModelClientPool

    monkeypatch.setattr(
        clickhouse_config, "get_settings",
        lambda: types.SimpleNamespace(enabled=True, addr="10.0.0.1:8123", database="model_api", username="u", password="p"),
        raising=False,
    )
    pg_boundary = datetime(2026, 8, 31, 18, 0, 0)

    async def fake_boundary(keep_entries):
        assert keep_entries == 5000
        return pg_boundary

    monkeypatch.setattr(db.PostgresClient, "request_log_entry_boundary", fake_boundary)
    monkeypatch.setattr(ch_module, "ClickHousePayloadClient", _RecordingClient)
    _RecordingClient.calls = []

    note = await ModelClientPool._cleanup_payload_store(7, 5000)

    assert note == ""  # 无可删内容时无补充说明
    assert _RecordingClient.calls == [(7, 5000, pg_boundary)]


@pytest.mark.asyncio
async def test_cleanup_payload_store_degrades_when_pg_boundary_fails(monkeypatch):
    import types

    import clickhouse_config
    import db
    import integrations.clickhouse as ch_module
    from rate_limiter import ModelClientPool

    monkeypatch.setattr(
        clickhouse_config, "get_settings",
        lambda: types.SimpleNamespace(enabled=True, addr="10.0.0.1:8123", database="model_api", username="u", password="p"),
        raising=False,
    )

    async def broken_boundary(keep_entries):
        raise RuntimeError("pg down")

    monkeypatch.setattr(db.PostgresClient, "request_log_entry_boundary", broken_boundary)
    monkeypatch.setattr(ch_module, "ClickHousePayloadClient", _RecordingClient)
    _RecordingClient.calls = []

    await ModelClientPool._cleanup_payload_store(7, 5000)

    # PG 边界失败不阻断清理：仍以 CH 事件数兜底执行。
    assert _RecordingClient.calls == [(7, 5000, None)]
