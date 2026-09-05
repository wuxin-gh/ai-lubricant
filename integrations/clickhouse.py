"""ClickHouse payload integration primitives.

This module deliberately contains no request-path calls. The writer owns its
own client and is only used by the optional background payload worker.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import Any

from loguru import logger


def _parse_addr(addr: str) -> tuple[str, int | None]:
    """Split ``host[:port]`` into (host, port). Allows IPv6 literals in brackets.

    Returns (host, None) when no port is given; the caller then falls back to
    clickhouse-connect's default HTTP port (8123).
    """
    text = addr.strip()
    if not text:
        return "", None
    # IPv6 literal: [::1]:8123 or [::1]
    if text.startswith("["):
        end = text.find("]")
        if end == -1:
            return text, None
        host = text[1:end]
        rest = text[end + 1:]
        if rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return host, None
    # IPv6 without brackets but multiple colons and no port: return as-is host
    if text.count(":") > 1 and not text.rsplit(":", 1)[-1].isdigit():
        return text, None
    if ":" in text:
        host, _, port = text.rpartition(":")
        if port.isdigit():
            return host, int(port)
    return text, None


# ── task_messages（任务对话内容）DDL 与列序 ──────────────────────────────
# 这张表由两个进程写：本模块（网关/数据服务）与 node_server 自己的门面
# （node_server/task_message_store.py——它不能 import 本模块，见那边模块注释）。
# 两侧各持一份物理副本，由 tests/test_task_messages_schema_parity.py 逐字比对，
# 与 message_status_machine 的做法一致：drift 只能在 CI 里发生一次。
TASK_MESSAGES_COLUMNS = [
    "id",
    "task_id",
    "logical_event_id",
    "seq",
    "item_type",
    "item_json",
    "agent_id",
    "subagent_id",
    "event_kind",
    "tool_name",
    "phase",
    "status",
    "created_at",
    "version",
]

TASK_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS task_messages
(
    id String,
    task_id String,
    logical_event_id String,
    seq UInt64,
    item_type LowCardinality(String),
    item_json String CODEC(ZSTD(9)),
    agent_id String,
    subagent_id String,
    event_kind LowCardinality(String),
    tool_name String,
    phase LowCardinality(String),
    status LowCardinality(String),
    created_at DateTime64(3),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (task_id, seq)
TTL toDateTime(created_at) + INTERVAL 365 DAY
"""


@dataclass(frozen=True)
class ClickHousePayloadEvent:
    kind: str
    request_id: str
    attempt_key: str
    attempt_no: int
    version: int
    created_at: str
    metadata: dict[str, Any]
    payload: dict[str, Any]


class ClickHousePayloadClient:
    """Small async facade around clickhouse-connect's synchronous client."""

    def __init__(self, *, addr: str, database: str, username: str = "", password: str = ""):
        self.addr = addr
        self.database = database
        self.username = username
        self.password = password
        self._client = None
        # clickhouse-connect 的同步 Client 带 session，同一个实例不允许并发查询。
        # 本 facade 会被多个 FastAPI 请求同时 await；所有 to_thread 调用必须按 client
        # 实例串行化，否则会抛 "Attempt to execute concurrent queries within the same session"。
        self._operation_lock = asyncio.Lock()

    async def _call(self, func, /, *args, **kwargs):
        """在线程池中串行调用当前 clickhouse-connect 客户端。"""
        async with self._operation_lock:
            return await asyncio.to_thread(func, *args, **kwargs)

    async def connect(self) -> None:
        if not self.addr:
            return
        import clickhouse_connect

        host, port = _parse_addr(self.addr)
        kwargs: dict[str, Any] = {
            "host": host,
            "username": self.username or None,
            "password": self.password or None,
            "database": self.database,
        }
        # 只有显式解析出端口才传；否则交给 clickhouse-connect 用默认端口（8123）。
        if port is not None:
            kwargs["port"] = port
        self._client = await asyncio.to_thread(clickhouse_connect.get_client, **kwargs)

    async def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            close = getattr(client, "close", None)
            if close is not None:
                await asyncio.to_thread(close)

    async def insert_events(self, events: list[ClickHousePayloadEvent]) -> None:
        if not events:
            return
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        rows = []
        for event in events:
            rows.append(
                [
                    event.created_at,
                    event.request_id,
                    event.attempt_key,
                    event.attempt_no,
                    event.kind,
                    event.version,
                    json.dumps(event.metadata, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(event.payload, ensure_ascii=False, separators=(",", ":")),
                ]
            )
        await self._call(
            self._client.insert,
            "request_log_payloads",
            rows,
            column_names=[
                "created_at",
                "request_id",
                "attempt_key",
                "attempt_no",
                "event_kind",
                "version",
                "metadata_json",
                "payload_json",
            ],
        )

    async def ensure_schema(self, *, ttl_days: int = 30) -> None:
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        ttl_days = max(1, int(ttl_days))
        # payload_json 用 ZSTD(9)：同 session 相邻两轮请求相似度 95%+，ZSTD 大窗口
        # 对这类重发全量历史的 JSON 能压到 ~51x（实测默认 LZ4 只有 2.41x）。
        # codec 与 TTL 一样在下方用幂等 ALTER 对齐，老表/新部署都自动收敛。
        query = f"""
        CREATE TABLE IF NOT EXISTS request_log_payloads
        (
            created_at DateTime64(3),
            request_id String,
            attempt_key String,
            attempt_no UInt32,
            event_kind LowCardinality(String),
            version UInt32,
            metadata_json String,
            payload_json String CODEC(ZSTD(9))
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (request_id, attempt_key, version)
        TTL toDateTime(created_at) + INTERVAL {ttl_days} DAY
        """
        await self._call(self._client.command, query)
        for alter in (
            f"ALTER TABLE request_log_payloads MODIFY TTL toDateTime(created_at) + INTERVAL {ttl_days} DAY",
            "ALTER TABLE request_log_payloads MODIFY COLUMN payload_json String CODEC(ZSTD(9))",
        ):
            try:
                await self._call(self._client.command, alter)
            except Exception:
                logger.warning("[clickhouse] request_log_payloads schema alter skipped: {}", alter)

    _TIME_BOUNDARY_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$")

    @classmethod
    def _normalize_time_boundary(cls, value: Any) -> str | None:
        """把边界值规整成 `YYYY-MM-DD HH:MM:SS.mmm`，形状不符一律拒绝。

        边界会被拼进 DDL（mutation 的 WHERE 无法用绑定参数），所以这里做白名单
        校验而不是转义：datetime 走 strftime，字符串必须整体匹配严格时间格式，
        任何多余字符（引号/分号/注释）都直接判为无效，返回 None 即放弃该边界。
        """
        if value is None:
            return None
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        text = str(value).strip()
        if not cls._TIME_BOUNDARY_RE.match(text):
            return None
        if "." not in text:
            return text + ".000"
        head, _, frac = text.partition(".")
        return f"{head}.{(frac + '000')[:3]}"

    async def delete_payloads_older_than(
        self,
        keep_days: int,
        max_entries: int = 0,
        entries_boundary: Any = None,
    ) -> dict[str, Any]:
        """按天数或条数裁剪 payload，并尽量以分区删除立即回收磁盘。

        三个边界，合并取更严格（时间更新）的一个，与 Postgres 的「超过天数 OR
        超过条数即删除」语义一致：

        1. 天数：`now() - INTERVAL keep_days DAY`；
        2. ``entries_boundary``（首选条数口径）：Postgres 主表「第 max_entries 条
           现存日志」的时间，由 PostgresClient.request_log_entry_boundary 提供。
           主删从随——主表删掉的行对应的 payload 必被带走，不会留下查不到的孤儿；
        3. 表内事件兜底：PG 边界拿不到时（库不可用等），退回本表第 ``2 * max_entries``
           个最新事件的时间（一条日志最多双写 started/finalized 两个事件）。

        边界值经 _normalize_time_boundary 白名单校验后才拼进 DDL；形状不符即放弃
        该边界（不是转义），避免把外部字符串带进 mutation 的 WHERE。
        """
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        keep_days = max(0, int(keep_days))
        max_entries = max(0, int(max_entries))

        cutoff_parts: list[str] = []
        if keep_days > 0:
            cutoff_parts.append(f"now() - INTERVAL {keep_days} DAY")

        count_boundary = self._normalize_time_boundary(entries_boundary)
        if count_boundary:
            cutoff_parts.append(f"toDateTime64('{count_boundary}', 3)")

        if max_entries > 0:
            # 每条 Postgres 日志最多双写 started/finalized 两个事件。取第 N 个最新事件
            # 的时间作为边界；同毫秒并列时宁可略多保留，不误删边界事件。
            event_limit = max_entries * 2
            boundary = await self.query(
                f"""
                SELECT created_at
                FROM request_log_payloads
                ORDER BY created_at DESC
                LIMIT 1 OFFSET {event_limit - 1}
                """
            )
            if boundary and boundary[0].get("created_at") is not None:
                text = self._normalize_time_boundary(boundary[0]["created_at"])
                if text:
                    # PG 边界是权威口径；本表事件数只作兜底，两者都在时一起进 greatest。
                    if count_boundary is None:
                        count_boundary = text
                    cutoff_parts.append(f"toDateTime64('{text}', 3)")

        if len(cutoff_parts) > 1:
            cutoff = f"greatest({', '.join(cutoff_parts)})"
        elif cutoff_parts:
            cutoff = cutoff_parts[0]
        else:
            cutoff = None

        dropped: list[str] = []
        mutation_submitted = False
        if cutoff is not None:
            # 整月早于有效保留边界时直接 DROP；边界月再走一次 mutation。
            expired = await self.query(
                f"""
                SELECT partition
                FROM system.parts
                WHERE active AND table = 'request_log_payloads' AND database = currentDatabase()
                GROUP BY partition
                HAVING max(max_time) < {cutoff}
                """
            )
            for row in expired:
                partition = str(row["partition"])
                if not partition.isdigit():
                    continue
                await self.command(
                    f"ALTER TABLE request_log_payloads DROP PARTITION {partition}"
                )
                dropped.append(partition)

            remaining = await self.query(
                f"SELECT count() AS c FROM request_log_payloads WHERE created_at < {cutoff}"
            )
            if remaining and int(remaining[0].get("c") or 0) > 0:
                await self.command(
                    "ALTER TABLE request_log_payloads DELETE "
                    f"WHERE created_at < {cutoff}"
                )
                mutation_submitted = True

        # 不管本次是否删除，都报告之前 mutation/drop 留下的 inactive part。
        inactive = await self.query(
            """
            SELECT count() AS n, sum(bytes_on_disk) AS bytes
            FROM system.parts
            WHERE active = 0 AND table = 'request_log_payloads' AND database = currentDatabase()
            """
        )
        inactive_n = int(inactive[0].get("n") or 0) if inactive else 0
        inactive_bytes = int(inactive[0].get("bytes") or 0) if inactive else 0
        return {
            "dropped_partitions": dropped,
            "mutation_submitted": mutation_submitted,
            "inactive_parts": inactive_n,
            "inactive_bytes": inactive_bytes,
            "count_trim_boundary": count_boundary,
        }

    async def payload_disk_usage(self) -> dict[str, Any]:
        """返回 request_log_payloads 的行数与磁盘占用（只统计 active parts）。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        rows = await self.query(
            """
            SELECT
                sum(rows) AS rows,
                sum(bytes_on_disk) AS bytes_on_disk,
                min(min_time) AS oldest,
                max(max_time) AS newest
            FROM system.parts
            WHERE active AND table = 'request_log_payloads' AND database = currentDatabase()
            """
        )
        return dict(rows[0]) if rows else {}

    async def fetch_request_payloads(self, attempt_keys: list[str]) -> dict[str, dict[str, Any]]:
        """Load and merge started/finalized payload snapshots by attempt key."""
        keys = [str(key) for key in dict.fromkeys(attempt_keys) if key]
        if not keys:
            return {}
        rows = await self.query(
            """
            SELECT attempt_key, version, event_kind, payload_json
            FROM request_log_payloads
            WHERE attempt_key IN {attempt_keys:Array(String)}
            ORDER BY attempt_key, version ASC, event_kind ASC
            """,
            {"attempt_keys": keys},
        )
        merged: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = str(row["attempt_key"])
            try:
                payload = json.loads(row.get("payload_json") or "{}")
            except (TypeError, ValueError):
                logger.warning("invalid request payload JSON for attempt_key={}", key)
                continue
            if not isinstance(payload, dict):
                continue
            # 空值不覆盖非空值：started 行可能比 finalized 行带更完整的大字段（例如
            # 上游未返回响应体时 finalized 的 response_body 为空），若用 update 直接
            # 覆盖，started 行那份完好数据会被空串抹掉，表现为详情页「请求参数/消息体」
            # 整块空白。这里只写非空值，让完好副本存活。
            target = merged.setdefault(key, {})
            for field, value in payload.items():
                if value in (None, "", {}, []):
                    target.setdefault(field, value)
                else:
                    target[field] = value
        return merged

    # ── Agent / 聊天对话存储 ──────────────────────────────────────────
    # agent_conversations / agent_messages 用 ReplacingMergeTree + 版本列：
    # 每次「更新/删除」变成一次新插入（version 单调递增），读用 FINAL 取最新版本。
    # 软删 = 插一行同 id、status='deleted'、version=now 的新版本，避免重 mutation。
    # 详细策略见 agent/conversation_store.py。

    async def ensure_conversation_schema(self, *, ttl_days: int = 365) -> None:
        """建 agent_conversations / agent_messages 两张表（幂等）。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        ttl_days = max(1, int(ttl_days))
        conv = f"""
        CREATE TABLE IF NOT EXISTS agent_conversations
        (
            id String,
            title String,
            system_prompt String,
            model String,
            llm_model_id Nullable(Int32),
            status String,
            kind LowCardinality(String),
            agent_id Nullable(Int32),
            user_id Nullable(String),
            cdp_client_id Nullable(String),
            chat_settings String,
            created_at DateTime64(3),
            updated_at DateTime64(3),
            version UInt64
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY id
        TTL toDateTime(created_at) + INTERVAL {ttl_days} DAY
        """
        msgs = f"""
        CREATE TABLE IF NOT EXISTS agent_messages
        (
            id UInt64,
            conversation_id String,
            role LowCardinality(String),
            content String,
            tool_calls String,
            tool_results String,
            turn_number Int32,
            status String,
            error Nullable(String),
            media String,
            model String DEFAULT '',
            usage String DEFAULT '',
            reasoning String DEFAULT '',
            created_at DateTime64(3),
            version UInt64
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY (conversation_id, id)
        TTL toDateTime(created_at) + INTERVAL {ttl_days} DAY
        """
        await self._call(self._client.command, conv)
        await self._call(self._client.command, msgs)
        # 幂等 ALTER 覆盖已存在的表（CREATE IF NOT EXISTS 不加列）
        await self._call(self._client.command, "ALTER TABLE agent_messages ADD COLUMN IF NOT EXISTS model String DEFAULT ''")
        await self._call(self._client.command, "ALTER TABLE agent_messages ADD COLUMN IF NOT EXISTS usage String DEFAULT ''")
        await self._call(self._client.command, "ALTER TABLE agent_messages ADD COLUMN IF NOT EXISTS reasoning String DEFAULT ''")

    async def insert_conversation_row(self, row: list) -> None:
        """单行追加 agent_conversations（列顺序见 column_names）。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        await self._call(
            self._client.insert,
            "agent_conversations",
            [row],
            column_names=[
                "id", "title", "system_prompt", "model", "llm_model_id",
                "status", "kind", "agent_id", "user_id", "cdp_client_id",
                "chat_settings", "created_at", "updated_at", "version",
            ],
        )

    async def insert_message_row(self, row: list) -> None:
        """单行追加 agent_messages（列顺序见 column_names）。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        await self._call(
            self._client.insert,
            "agent_messages",
            [row],
            column_names=[
                "id", "conversation_id", "role", "content", "tool_calls",
                "tool_results", "turn_number", "status", "error", "media",
                "model", "usage", "reasoning", "created_at", "version",
            ],
        )

    # ── 任务对话内容存储 ─────────────────────────────────────────────
    # task_messages 存任务详情页的对话内容（user_input + agent 帧），append-only：
    # runtime 每报一次就插一行，读侧按 logical_event_id 做字段级 last-wins 合并。
    # 状态（delivery_status 等）留在 Postgres mc_task_events，靠 logical_event_id
    # 关联——CH 做不了条件 UPDATE 状态机，PG 做不了这个量级的 append 内容。
    #
    # 两个进程都写这张表：node_server 走它自己的 clickhouse-connect 门面
    # （node_server/task_message_store.py，不能 import 本模块，见其模块注释），
    # 网关在启动时建表。DDL 文本由 tests/test_task_messages_schema_parity.py 比对。

    async def ensure_task_messages_schema(self) -> None:
        """建 task_messages（幂等）。DDL 与 node_server 侧逐字一致。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        await self._call(self._client.command, TASK_MESSAGES_DDL)

    async def insert_task_message_row(self, row: list) -> None:
        """单行追加 task_messages（列顺序见 TASK_MESSAGES_COLUMNS）。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        await self._call(
            self._client.insert,
            "task_messages",
            [row],
            column_names=TASK_MESSAGES_COLUMNS,
        )

    async def insert_task_message_rows(self, rows: list[list]) -> None:
        """多行批量追加 task_messages。

        单行插入是一条 HTTP 往返；回填迁移一次可能写几十万帧，单行插会把
        ClickHouse 的 part 数撑爆（``Too many parts``）。批量插入让一页 PG 行
        落成一两个 part。列顺序仍由 ``TASK_MESSAGES_COLUMNS`` 锁定，与单行路径
        和 node_server 侧一致——parity 测试覆盖。空列表直接返回，避免空 insert。
        """
        if not rows:
            return
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        await self._call(
            self._client.insert,
            "task_messages",
            rows,
            column_names=TASK_MESSAGES_COLUMNS,
        )

    async def query_task_messages(
        self, task_id: str, *, before_seq: int | None = None, limit: int = 50
    ) -> list[dict]:
        """回放游标：按 seq 倒序取一页，最旧在前返回。

        ``before_seq`` 是排他上界（调用方传本页最旧的 seq 往前翻）。FINAL 收敛
        同 seq 的重复插入（ReplacingMergeTree 按 version 取最新），正常情况下每帧
        一个新 seq，FINAL 只是重试插入的保险。
        """
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        limit = max(1, min(int(limit), 500))
        params: dict[str, Any] = {"task_id": str(task_id), "limit": limit}
        cursor = ""
        if before_seq is not None:
            cursor = "AND seq < {before_seq:UInt64} "
            params["before_seq"] = int(before_seq)
        rows = await self.query(
            "SELECT id, task_id, logical_event_id, seq, item_type, item_json, "
            "agent_id, subagent_id, event_kind, tool_name, phase, status, created_at "
            "FROM task_messages FINAL "
            "WHERE task_id = {task_id:String} " + cursor +
            "ORDER BY seq DESC LIMIT {limit:UInt32}",
            params,
        )
        return list(reversed(rows))

    async def command(self, query: str, parameters: dict | None = None) -> Any:
        """执行任意 SQL（用于 FINAL 查询等）。同步 clickhouse-connect 包在 to_thread 里。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        if parameters:
            return await self._call(self._client.command, query, parameters)
        return await self._call(self._client.command, query)

    async def query(self, query: str, parameters: dict | None = None) -> list[dict]:
        """执行 SELECT，返回 dict 行列表。"""
        if self._client is None:
            raise RuntimeError("ClickHouse client is not connected")
        if parameters:
            rows = await self._call(self._client.query, query, parameters)
        else:
            rows = await self._call(self._client.query, query)
        cols = rows.column_names
        out = []
        for r in rows.result_rows:
            out.append({cols[i]: r[i] for i in range(len(cols))})
        return out
