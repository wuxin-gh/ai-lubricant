"""市场条目编辑真相源（marketplace_items）+ 发布 outbox（marketplace_publish_jobs）。

市场管理页的 CRUD 事务落这里，保存即生效；GitHub 仓库退化为发布镜像，由后台
publisher（monkeycode_compat/marketplace/publisher.py）消费 outbox 把当前 store
状态异步推送上去。此前保存一条模板要 7-9 次串行 GitHub RTT（GET sha + PUT ×
item/index/marker + 渠道目录全量重拉）全在请求里，现在写路径零出网。

关键语义：**job 是 dirty 标记，publisher 永远以 store 当前状态渲染**——同一条目
先 delete 后 re-upsert，晚到的旧 job 渲染出来的也是新状态，不会复活旧数据。
``payload`` 只在行已物理删除、需要 assets[].repo_path 做资产清理时携带删除前
manifest。

降级：``PostgresClient.pool`` 未初始化时全部安全返回空/假（照
marketplace_leaderboard_store 的风格），调用方据此回落旧的 GitHub 直读路径。

风格对齐 ``marketplace_leaderboard_store``：模块级函数 + 函数内延迟 import
PostgresClient，JSONB 列出入库 ``json.dumps``、出库兜 ``json.loads``。
"""
from __future__ import annotations

import asyncio
import copy
import json
import random
from typing import Any

# 退避重试：2^attempts * 5s 基数 + 随机抖动，超过上限置 failed 供管理页手动重试。
MAX_ATTEMPTS = 8
_BASE_BACKOFF_SECONDS = 5.0
_STALE_PUSHING_SECONDS = 300  # pushing 超 5 分钟视为消费者死亡，回 pending（与 notify_core 一致）

# 唤醒 publisher：enqueue 后 set，worker 收到立即拉起一轮，免 1s 轮询延迟。
# Event 按当前 event loop 懒建：生产只有一个 loop；测试/CLI 会多次 asyncio.run，
# 复用绑定旧 loop 的 Event 会卡死或抛错。
_wakeup: asyncio.Event | None = None
_wakeup_loop: asyncio.AbstractEventLoop | None = None

# 进程内缓存「store 是否有数据」，避免读路径每请求 count(*)。首次查询后落 True；
# bootstrap 写入后由 bootstrap 显式 reset。多实例下各持一份，语义只用于「读路径
# 选 store 还是旧 GitHub 路径」，晚几秒切换无碍。
_populated_cache: bool | None = None


def _dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def _loads(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return default
    return value if value is not None else default


def _row_to_dict(row) -> dict | None:
    if row is None:
        return None
    data = dict(row)
    data["manifest"] = _loads(data.get("manifest"), {})
    data["summary"] = _loads(data.get("summary"), {})
    data["payload"] = _loads(data.get("payload"), {})
    for key in ("updated_at", "created_at", "available_at", "started_at", "finished_at"):
        if data.get(key) is not None:
            data[key] = data[key].isoformat()
    return data


def _wakeup_event() -> asyncio.Event:
    global _wakeup, _wakeup_loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # notify_publisher 可能从纯同步上下文调用；没有 loop 时不需要即时唤醒，
        # worker 的 1s 兜底轮询会处理。
        if _wakeup is None:
            _wakeup = asyncio.Event()
        return _wakeup
    if _wakeup is None or _wakeup_loop is not loop:
        _wakeup = asyncio.Event()
        _wakeup_loop = loop
    return _wakeup


def notify_publisher() -> None:
    """唤醒 publisher worker（enqueue 后调用）。"""
    _wakeup_event().set()


async def wait_for_wakeup(timeout: float) -> None:
    """publisher worker 的睡眠原语：等唤醒或超时兜底轮询。"""
    event = _wakeup_event()
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        pass
    event.clear()


def reset_populated_cache() -> None:
    """行数可能变化后清缓存，让下一次查询重读 PG。"""
    global _populated_cache
    _populated_cache = None


def mark_populated() -> None:
    """成功写入至少一行后立即切到 store 读路径（保存即生效）。"""
    global _populated_cache
    _populated_cache = True


# ── 读 ───────────────────────────────────────────────────────────────────────


async def is_populated() -> bool:
    """store 是否有数据。读路径据此选 store（保存即生效）或旧 GitHub 路径。

    只读部署（未配 token、永不写）与 bootstrap 完成前的窗口返回 False。
    """
    global _populated_cache
    if _populated_cache is not None:
        return _populated_cache
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    try:
        async with PostgresClient.pool.acquire() as conn:
            # 空 store 也可能是「已初始化后把最后一条硬删了」，不能因此回落仓库旧快照。
            # app_config bootstrap flag 与行存在任一成立都进入 store 模式；只读部署两者
            # 都没有，继续走 raw。
            value = await conn.fetchval(
                """
                SELECT EXISTS(SELECT 1 FROM marketplace_items)
                    OR EXISTS(SELECT 1 FROM app_config WHERE key='marketplace_store_bootstrapped')
                """
            )
    except Exception:  # noqa: BLE001 - 表未建/连接失败时回落旧路径
        return False
    _populated_cache = bool(value)
    return _populated_cache


async def get_item(module: str, item_id: str) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM marketplace_items WHERE module=$1 AND item_id=$2",
            module, item_id,
        )
    return _row_to_dict(row)


async def list_summaries(module: str) -> list[dict]:
    """模块全部索引行（含 hidden，管理视图口径；消费侧自行过滤 status）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT summary FROM marketplace_items WHERE module=$1 ORDER BY item_id",
            module,
        )
    out: list[dict] = []
    for row in rows:
        summary = _loads(row["summary"], {})
        if isinstance(summary, dict):
            out.append(summary)
    return out


async def list_manifests(module: str, *, include_hidden: bool = True) -> list[dict]:
    """模块全部 manifest。publisher 渲染 index/version.json、渠道目录都用它。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    sql = "SELECT manifest FROM marketplace_items WHERE module=$1"
    if not include_hidden:
        sql += " AND status='published'"
    sql += " ORDER BY item_id"
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(sql, module)
    out: list[dict] = []
    for row in rows:
        manifest = _loads(row["manifest"], {})
        if isinstance(manifest, dict):
            out.append(manifest)
    return out


def pool_ready() -> bool:
    """publisher 启动探测：PG pool 已初始化。"""
    try:
        from db import PostgresClient
        return PostgresClient.pool is not None
    except Exception:  # noqa: BLE001
        return False


async def populated_modules() -> list[str]:
    """当前 store 有条目的模块集合（marketplace.json marker 投影源）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT module FROM marketplace_items ORDER BY module")
    return [str(r["module"]) for r in rows]


async def list_item_ids(module: str) -> set[str]:
    from db import PostgresClient

    if not PostgresClient.pool:
        return set()
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch("SELECT item_id FROM marketplace_items WHERE module=$1", module)
    return {str(r["item_id"]) for r in rows}


# ── 写（事务：改行 + 入队，一次提交）────────────────────────────────────────


async def upsert_item(module: str, manifest: dict) -> dict:
    """保存一条 manifest：summary 写入时算好、revision +1、同事务 enqueue(upsert)。

    manifest 校验由调用方（路由层 validate_manifest）负责，这里只管落库。
    """
    from db import PostgresClient
    from monkeycode_compat.marketplace.validator import index_summary

    item_id = str(manifest.get("id") or "")
    if not item_id:
        raise ValueError("manifest id 不能为空")
    summary = index_summary(module, manifest)
    status = str(manifest.get("status") or "published")
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO marketplace_items (module, item_id, manifest, summary, status, revision)
                VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, 1)
                ON CONFLICT (module, item_id) DO UPDATE SET
                    manifest = EXCLUDED.manifest,
                    summary = EXCLUDED.summary,
                    status = EXCLUDED.status,
                    revision = marketplace_items.revision + 1,
                    updated_at = now()
                RETURNING *
                """,
                module, item_id, _dumps(manifest), _dumps(summary), status,
            )
            await _enqueue(conn, module, item_id, "upsert")
    mark_populated()
    notify_publisher()
    return _row_to_dict(row) or {}


async def set_status(module: str, item_id: str, status: str) -> dict | None:
    """改状态（hide→hidden / unhide→published）：manifest.status、summary 行与行
    status 三处同步改，入队 upsert（publisher 要把 item 文件里的 status 一并镜像）。"""
    from db import PostgresClient

    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE marketplace_items
                   SET status = $3,
                       manifest = jsonb_set(manifest, '{status}', to_jsonb($3::text)),
                       summary = jsonb_set(summary, '{status}', to_jsonb($3::text)),
                       revision = revision + 1,
                       updated_at = now()
                 WHERE module = $1 AND item_id = $2
                RETURNING *
                """,
                module, item_id, status,
            )
            if row is not None:
                await _enqueue(conn, module, item_id, "upsert")
    if row is not None:
        notify_publisher()
    return _row_to_dict(row)


async def delete_item(module: str, item_id: str, *, hard: bool) -> dict | None:
    """删除条目。hard 物理删行（删除前 manifest 进 job payload 供资产清理）；
    soft 等价 set_status('hidden')。"""
    from db import PostgresClient

    if not hard:
        return await set_status(module, item_id, "hidden")
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM marketplace_items WHERE module=$1 AND item_id=$2",
                module, item_id,
            )
            if row is None:
                return None
            payload = {"manifest": _loads(row["manifest"], {})}
            await conn.execute(
                "DELETE FROM marketplace_items WHERE module=$1 AND item_id=$2",
                module, item_id,
            )
            await _enqueue(conn, module, item_id, "delete", payload)
    notify_publisher()
    return _row_to_dict(row)


async def replace_module(module: str, manifests: list[dict]) -> dict:
    """import replace 模式：仓库导入包未包含的行物理删除（hard 语义），其余 upsert。

    单事务：全部行写完后一次性入队——每条 upsert、每条删除各一个 job。
    """
    from db import PostgresClient
    from monkeycode_compat.marketplace.validator import index_summary

    keep_ids: set[str] = set()
    written: list[str] = []
    deleted: list[str] = []
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetch(
                "SELECT item_id, manifest FROM marketplace_items WHERE module=$1",
                module,
            )
            existing_by_id = {str(r["item_id"]): _loads(r["manifest"], {}) for r in existing}
            for manifest in manifests:
                item_id = str(manifest.get("id") or "")
                if not item_id:
                    continue
                summary = index_summary(module, manifest)
                status = str(manifest.get("status") or "published")
                await conn.execute(
                    """
                    INSERT INTO marketplace_items (module, item_id, manifest, summary, status, revision)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, 1)
                    ON CONFLICT (module, item_id) DO UPDATE SET
                        manifest = EXCLUDED.manifest,
                        summary = EXCLUDED.summary,
                        status = EXCLUDED.status,
                        revision = marketplace_items.revision + 1,
                        updated_at = now()
                    """,
                    module, item_id, _dumps(manifest), _dumps(summary), status,
                )
                await _enqueue(conn, module, item_id, "upsert")
                keep_ids.add(item_id)
                written.append(item_id)
            for item_id, manifest in existing_by_id.items():
                if item_id in keep_ids:
                    continue
                payload = {"manifest": manifest}
                await conn.execute(
                    "DELETE FROM marketplace_items WHERE module=$1 AND item_id=$2",
                    module, item_id,
                )
                await _enqueue(conn, module, item_id, "delete", payload)
                deleted.append(item_id)
    if written:
        mark_populated()
    notify_publisher()
    return {"written": written, "deleted": deleted}


async def recompute_summaries(module: str) -> int:
    """rebuild-index：全部行按 manifest 重算 summary 并入队 refresh。"""
    from db import PostgresClient
    from monkeycode_compat.marketplace.validator import index_summary

    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT item_id, manifest FROM marketplace_items WHERE module=$1",
            module,
        )
        count = 0
        async with conn.transaction():
            for row in rows:
                manifest = _loads(row["manifest"], {})
                if not isinstance(manifest, dict):
                    continue
                await conn.execute(
                    "UPDATE marketplace_items SET summary=$3::jsonb, updated_at=now() WHERE module=$1 AND item_id=$2",
                    module, str(row["item_id"]), _dumps(index_summary(module, manifest)),
                )
                count += 1
            if count:
                await _enqueue(conn, module, "*", "refresh")
    if count:
        notify_publisher()
    return count


# ── outbox ───────────────────────────────────────────────────────────────────


async def _enqueue(conn, module: str, item_id: str, action: str, payload: dict | None = None) -> None:
    """入队（须在事务内）。pending 同 (module,item_id) 已有任意 action 的 job 时跳过——
    publisher 渲染永远读当前状态，旧 job 已携带最新语义，不重复入队。

    例外：hard delete 后同 item 又重新 upsert 的场景，pending 里若残留 delete job，
    行已回来、渲染读当前状态会写新文件，delete 的资产清理则针对 payload（旧 manifest）
    ——两者都正确且幂等，无需特殊处理。
    """
    await conn.execute(
        """
        INSERT INTO marketplace_publish_jobs (module, item_id, action, payload)
        VALUES ($1, $2, $3, $4::jsonb)
        ON CONFLICT (module, item_id) WHERE status='pending' DO UPDATE SET
            action = EXCLUDED.action,
            payload = EXCLUDED.payload,
            available_at = now(),
            last_error = ''
        """,
        module, item_id, action, _dumps(payload or {}),
    )


async def claim_pending(limit: int = 32) -> list[dict]:
    """CAS 认领 pending jobs（available_at 已到），多实例安全。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE marketplace_publish_jobs AS j
               SET status='pushing', attempts=attempts+1, started_at=now()
             WHERE id IN (
                SELECT id FROM marketplace_publish_jobs
                 WHERE status='pending' AND available_at <= now()
                 ORDER BY id
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED
             )
            RETURNING j.*
            """,
            limit,
        )
    return [_row_to_dict(r) for r in rows]


async def mark_done(job_ids: list[int]) -> None:
    from db import PostgresClient

    if not PostgresClient.pool or not job_ids:
        return
    async with PostgresClient.pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE marketplace_publish_jobs
               SET status='done', finished_at=now(), last_error=''
             WHERE id = ANY($1::bigint[])
            """,
            job_ids,
        )


async def mark_failed(job_ids: list[int], error: str) -> list[int]:
    """失败收口：未达上限按指数退避重试，达到上限置 failed 供管理页手动重试。

    返回本轮刚进入终态 failed 的 job ids（publisher 只对它们发通知，避免每次退避
    都刷一条告警）。
    """
    from db import PostgresClient

    if not PostgresClient.pool or not job_ids:
        return []
    terminal: list[int] = []
    trimmed = (error or "")[:2000]
    for job_id in job_ids:
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT attempts FROM marketplace_publish_jobs WHERE id=$1", job_id,
            )
            if row is None:
                continue
            attempts = int(row["attempts"] or 1)
            if attempts >= MAX_ATTEMPTS:
                await conn.execute(
                    "UPDATE marketplace_publish_jobs SET status='failed', last_error=$2, finished_at=now() WHERE id=$1",
                    job_id, trimmed,
                )
                terminal.append(job_id)
            else:
                backoff = min(3600.0, _BASE_BACKOFF_SECONDS * (2 ** min(attempts, 10)))
                backoff *= 0.5 + random.random()  # 0.5x~1.5x 抖动，避免惊群
                await conn.execute(
                    """
                    UPDATE marketplace_publish_jobs
                       SET status='pending', last_error=$2, available_at=now() + make_interval(secs => $3)
                     WHERE id=$1
                    """,
                    job_id, trimmed, backoff,
                )
    return terminal


async def requeue_stale() -> int:
    """pushing 超 5 分钟（消费者死亡）回 pending。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return 0
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE marketplace_publish_jobs
               SET status='pending', available_at=now()
             WHERE status='pushing' AND started_at < now() - make_interval(secs => $1)
            RETURNING id
            """,
            _STALE_PUSHING_SECONDS,
        )
    return len(rows)


async def requeue_claimed(job_ids: list[int]) -> None:
    """advisory lock 被另一实例占用时，把本轮 CAS 认领的 jobs 立刻退回 pending。"""
    from db import PostgresClient

    if not PostgresClient.pool or not job_ids:
        return
    async with PostgresClient.pool.acquire() as conn:
        await conn.execute(
            "UPDATE marketplace_publish_jobs SET status='pending', available_at=now() WHERE id=ANY($1::bigint[]) AND status='pushing'",
            job_ids,
        )
    notify_publisher()


async def retry_failed() -> int:
    """管理页手动重试：failed → pending。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return 0
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "UPDATE marketplace_publish_jobs SET status='pending', available_at=now() WHERE status='failed' RETURNING id",
        )
    if rows:
        notify_publisher()
    return len(rows)


async def list_jobs(module: str | None = None, limit: int = 100) -> dict:
    """发布面板：pending/pushing/failed 明细与计数（done 不列，量大无查看价值）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return {"pending": [], "pushing": [], "failed": [], "counts": {"pending": 0, "pushing": 0, "failed": 0}}
    where = "WHERE module=$1" if module else ""
    args: list[Any] = [module] if module else []
    async with PostgresClient.pool.acquire() as conn:
        counts_rows = await conn.fetch(
            f"SELECT status, count(*) AS n FROM marketplace_publish_jobs {where} GROUP BY status",
            *args,
        )
        counts = {str(r["status"]): int(r["n"]) for r in counts_rows}
        limit_param = len(args) + 1
        detail_rows = await conn.fetch(
            f"""
            SELECT id, module, item_id, action, status, attempts, last_error,
                   available_at, created_at, started_at
              FROM marketplace_publish_jobs
             {where + " AND" if where else "WHERE"} status IN ('pending', 'pushing', 'failed')
             ORDER BY id DESC
             LIMIT ${limit_param}
            """,
            *(args + [limit]),
        )
    details = [_row_to_dict(r) for r in detail_rows]
    return {
        "pending": [d for d in details if d.get("status") == "pending"],
        "pushing": [d for d in details if d.get("status") == "pushing"],
        "failed": [d for d in details if d.get("status") == "failed"],
        "counts": {
            "pending": counts.get("pending", 0),
            "pushing": counts.get("pushing", 0),
            "failed": counts.get("failed", 0),
        },
    }


async def publish_counts() -> dict:
    """目录响应的轻量计数（前端徽标）。"""
    jobs = await list_jobs(limit=1)
    return jobs["counts"]


# ── bootstrap 写入（无 outbox——仓库已是该状态，无需再发布）────────────────


async def bootstrap_items(items: list[tuple[str, dict]]) -> int:
    """显式 resync 的 merge 写入（不 enqueue）：仓库有的行覆盖 store，仓库没有的
    行保留。启动 bootstrap 必须用 ``bootstrap_items_no_overwrite``，避免导入窗口
    覆盖管理员刚保存的新值。"""
    from db import PostgresClient
    from monkeycode_compat.marketplace.validator import index_summary

    if not PostgresClient.pool or not items:
        return 0
    count = 0
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            for module, manifest in items:
                item_id = str(manifest.get("id") or "")
                if not item_id:
                    continue
                summary = index_summary(module, manifest)
                status = str(manifest.get("status") or "published")
                await conn.execute(
                    """
                    INSERT INTO marketplace_items (module, item_id, manifest, summary, status, revision)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, 1)
                    ON CONFLICT (module, item_id) DO UPDATE SET
                        manifest = EXCLUDED.manifest,
                        summary = EXCLUDED.summary,
                        status = EXCLUDED.status,
                        revision = marketplace_items.revision + 1,
                        updated_at = now()
                    """,
                    module, item_id, _dumps(manifest), _dumps(summary), status,
                )
                count += 1
    reset_populated_cache()
    return count


async def bootstrap_items_no_overwrite(items: list[tuple[str, dict]]) -> int:
    """启动 bootstrap 的 DO NOTHING 版本：store 已有的行（导入窗口内管理员刚保存的）
    原样保留，只用仓库内容填空。"""
    from db import PostgresClient
    from monkeycode_compat.marketplace.validator import index_summary

    if not PostgresClient.pool or not items:
        return 0
    count = 0
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            for module, manifest in items:
                item_id = str(manifest.get("id") or "")
                if not item_id:
                    continue
                summary = index_summary(module, manifest)
                status = str(manifest.get("status") or "published")
                await conn.execute(
                    """
                    INSERT INTO marketplace_items (module, item_id, manifest, summary, status, revision)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, 1)
                    ON CONFLICT (module, item_id) DO NOTHING
                    """,
                    module, item_id, _dumps(manifest), _dumps(summary), status,
                )
                count += 1
    reset_populated_cache()
    return count
