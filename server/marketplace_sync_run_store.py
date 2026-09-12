"""marketplace_sync_runs 存取：各内容源同步任务的最新一次执行记录（一源一行）。

为什么存在 DB 而不是内存/文件：内存重启即失（用户明确要求「重启后打开面板还能
看到最新一份日志」）、文件不好随面板接口分发。一源一行 Upsert，不无限增长；
执行中的实时进度仍走各模块内存 `_progress`（用户口径：执行状态在内存即可）。

字段口径见 db.py 建表注释。写入口只有 ``record_run``（同步收尾时调，一次写全）；
``latest_run`` 供 last-sync/status 端点回显。
"""
from __future__ import annotations

import json
from typing import Any

VALID_SOURCES = ("agent-leaderboard", "agency-agents", "agency-agents-zh", "agentscope", "skillhub")


def _loads(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return default
    return value


def _row_to_dict(row) -> dict | None:
    if row is None:
        return None
    data = dict(row)
    for key in ("started_at", "finished_at"):
        if data.get(key) is not None:
            data[key] = data[key].isoformat()
    data["logs"] = _loads(data.get("logs"), [])
    data["counts"] = _loads(data.get("counts"), {})
    return data


async def record_run(
    source: str,
    *,
    ok: bool,
    detail: str,
    logs: list[dict] | None = None,
    run_by: str = "schedule",
    counts: dict | None = None,
    finished_at: Any = None,
) -> None:
    """记录（覆盖式）一次同步结果。失败也记——面板要能看到最近一次失败的日志。"""
    from db import PostgresClient

    if not PostgresClient.pool or source not in VALID_SOURCES:
        return
    async with PostgresClient.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO marketplace_sync_runs (source, finished_at, ok, detail, logs, run_by, counts)
            VALUES ($1, COALESCE($2::timestamptz, now()), $3, $4, $5::jsonb, $6, $7::jsonb)
            ON CONFLICT (source) DO UPDATE SET
                started_at = now(),
                finished_at = COALESCE($2::timestamptz, now()),
                ok = $3,
                detail = $4,
                logs = $5::jsonb,
                run_by = $6,
                counts = $7::jsonb
            """,
            source,
            finished_at,
            ok,
            str(detail or "")[:2000],
            json.dumps(logs or [], ensure_ascii=False),
            run_by if run_by in ("schedule", "manual") else "schedule",
            json.dumps(counts or {}, ensure_ascii=False),
        )


async def latest_run(source: str) -> dict | None:
    """某源最近一次同步记录（没有返回 None）。"""
    from db import PostgresClient

    if not PostgresClient.pool or source not in VALID_SOURCES:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM marketplace_sync_runs WHERE source=$1",
            source,
        )
    return _row_to_dict(row)
