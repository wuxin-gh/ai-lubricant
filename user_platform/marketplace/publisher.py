"""市场发布 outbox 的后台 worker：把 store 当前状态异步镜像到 GitHub 仓库。

仿 notify_core 的 outbox 模式（CAS 认领 + stale 回队 + worker/topup 双循环），
但语义不同：**job 是 dirty 标记，渲染永远读 store 当前状态**——同一条目先
delete 后 re-upsert，晚到的旧 job 渲染出的也是新状态，不会复活旧数据。因此
一轮发布对每个模块做三件事：

1. 对每个 dirty 条目按 action 写/删 ``modules/{module}/items/*.json``
   （refresh 只重渲 index 不动条目文件；delete 对 node/mobile 版本还要按
   payload 里的 assets[].repo_path 清理入仓二进制与 release manifest）；
2. 从 store 渲染并写 ``modules/{module}/index.json``；
3. node/mobile-versions 额外从 store 渲染 ``{node,mobile}-releases/version.json``
   （草稿/测试版本不进投影——线上升级提示只认已发布状态）。

marker（marketplace.json）只在模块集合变化时写。每模块经进程内锁 +
advisory lock 双保险串行，多实例下不同 worker 也不会同模块并发写 index 撞 sha。

失败按指数退避重试（store.mark_failed），达上限置 failed 供管理页手动重试，
并发一条站内通知。未配 github_token 或 PG 未初始化时 worker 直接退出——
只读部署不需要发布。
"""
from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from . import config as mp_config
from .github import MarketplaceGitHub
from .render import (
    render_device_control_release,
    render_index,
    render_marker,
    render_mobile_release,
    render_node_release,
)
from .validator import item_path, index_path, safe_item_id

# 根 marker 与发行投影的仓库路径（与 routes 常量一致，迁到此处成为发布侧唯一权威）。
MARKER_PATH = "marketplace.json"
NODE_RELEASE_PATH = "node-releases/version.json"
MOBILE_RELEASE_PATH = "mobile-releases/version.json"
DEVICE_CONTROL_RELEASE_PATH = "device-control-releases/version.json"

_WAKE_POLL_SECONDS = 1.0        # enqueue 唤醒之外的兜底轮询
_STALE_REQUEUE_SECONDS = 60.0   # stale pushing 检查周期
_CLAIM_BATCH = 64

# 进程内每模块串行锁：同模块两个发布轮次不并发写 index（advisory lock 兜多实例）。
_module_locks: dict[str, asyncio.Lock] = {}


def _module_lock(module: str) -> asyncio.Lock:
    lock = _module_locks.get(module)
    if lock is None:
        lock = asyncio.Lock()
        _module_locks[module] = lock
    return lock


def _client() -> MarketplaceGitHub:
    return MarketplaceGitHub(mp_config.settings)


async def worker_loop() -> None:
    """常驻循环：等唤醒/超时 → 发布一轮；定期把 stale pushing 回队。

    启动条件 ``settings.writable``（GitHub token）——榜单推送（module=leaderboard）
    只碰 PG、不需要 GitHub，但市场管理页本就只在 writable 部署可用，榜单推送
    不会有「无 token 但要用」的部署形态，跟着同一开关即可。
    """
    if not mp_config.settings.writable:
        logger.info("[marketplace-publisher] not writable, worker idle")
        return
    import marketplace_store as store

    if not store.pool_ready():
        logger.info("[marketplace-publisher] no PG pool, worker idle")
        return
    logger.info("[marketplace-publisher] worker started")
    stale_seconds = 0.0
    while True:
        try:
            await store.wait_for_wakeup(timeout=_WAKE_POLL_SECONDS)
            await publish_once()
            stale_seconds += _WAKE_POLL_SECONDS
            if stale_seconds >= _STALE_REQUEUE_SECONDS:
                stale_seconds = 0.0
                requeued = await store.requeue_stale()
                if requeued:
                    logger.info("[marketplace-publisher] requeued {} stale job(s)", requeued)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 循环绝不因单轮异常退出
            logger.warning("[marketplace-publisher] cycle error: {}", exc)
            await asyncio.sleep(_WAKE_POLL_SECONDS)


async def publish_once() -> dict[str, Any]:
    """认领 pending jobs 并按模块发布一轮。幂等，可独立调用（resync 后手动触发）。"""
    import marketplace_store as store

    jobs = await store.claim_pending(_CLAIM_BATCH)
    if not jobs:
        return {"claimed": 0}
    by_module: dict[str, list[dict]] = {}
    for job in jobs:
        by_module.setdefault(str(job.get("module") or ""), []).append(job)
    summary: dict[str, Any] = {"claimed": len(jobs), "modules": {}}
    for module, module_jobs in by_module.items():
        if module == "leaderboard":
            # 榜单推送不走 GitHub：worker 逐条跑 publish_items 的校验/门禁后翻
            # published。失败复用同一 outbox 状态机（退避重试→达上限 failed+通知）。
            status, error, done_ids, failed_ids = await publish_leaderboard(module_jobs)
            if done_ids:
                await store.mark_done(done_ids)
            if failed_ids:
                terminal = await store.mark_failed(failed_ids, error or "unknown error")
                if terminal:
                    _emit_publish_failed(module, error or "unknown error")
            summary["modules"][module] = {
                "status": status, "error": error, "jobs": len(done_ids) + len(failed_ids),
            }
            continue
        status, error = await publish_module(module, module_jobs)
        ids = [int(j["id"]) for j in module_jobs if j.get("id") is not None]
        if status == "ok":
            await store.mark_done(ids)
        elif status == "failed":
            terminal = await store.mark_failed(ids, error or "unknown error")
            if terminal:
                _emit_publish_failed(module, error or "unknown error")
        # "requeued"：advisory lock 被其它实例占用，jobs 已退回 pending，不做终态。
        summary["modules"][module] = {"status": status, "error": error, "jobs": len(ids)}
    return summary


async def publish_leaderboard(jobs: list[dict]) -> tuple[str, str, list[int], list[int]]:
    """榜单推送：逐条跑 ``publish_items``（校验+门禁+翻状态），按 job 收口。

    返回 ``(status, error, done_ids, failed_ids)``：任一条 failed 记入
    ``failed_ids``（worker 走退避重试），其余 published/skipped 归 done。单条失败
    不影响其余条目——与 publish_items 的批量语义一致。job 的 ``item_id`` 即榜单
    条目 id；payload.operator 透传发布人。
    """
    import marketplace_leaderboard_store as lb_store
    from .source_config import get_source_config_async

    source = await get_source_config_async()
    require_verified = bool(source.get("leaderboard_require_verified"))
    done_ids: list[int] = []
    failed_ids: list[int] = []
    first_error = ""
    for job in jobs:
        try:
            item_id = int(str(job.get("item_id") or "0"))
        except (TypeError, ValueError):
            item_id = 0
        job_id = int(job.get("id") or 0)
        if not item_id or not job_id:
            continue
        operator = str((job.get("payload") or {}).get("operator") or "publisher")
        result = await lb_store.publish_items(
            [item_id], operator=operator, require_verified=require_verified,
        )
        if result["failed"]:
            failed_ids.append(job_id)
            first_error = first_error or str(result["failed"][0].get("error") or "unknown error")
        else:
            # published 或 skipped（推送间隙已被别处发布）：job 都算完成，幂等。
            done_ids.append(job_id)
    status = "failed" if failed_ids and not done_ids else ("ok" if done_ids else "failed")
    return status, first_error, done_ids, failed_ids


def _emit_publish_failed(module: str, error: str) -> None:
    """达上限/本轮失败都发站内提醒的轻量包装；通知系统不可用时静默。"""
    label = "榜单推送" if module == "leaderboard" else f"市场发布（{module}）"
    try:
        from user_platform.notify_core import emit_notification_background

        emit_notification_background(
            "marketplace.publish_failed",
            params={"module": module, "error": (error or "")[:300]},
            owner_type="platform",
            severity="warning",
            source="marketplace_publisher",
            message=f"{label}失败：{(error or '')[:200]}",
        )
    except Exception:  # noqa: BLE001 - 通知失败不影响发布状态机
        pass


async def publish_module(module: str, jobs: list[dict]) -> tuple[str, str]:
    """发布一个模块的 dirty 集合。返回 ``(status, error)``。

    status 为 ``ok`` / ``failed`` / ``requeued``（advisory lock 被其它实例占用，
    jobs 已退回 pending）。渲染永远读 store 当前状态，因此晚到的旧 job（例如
    delete 后又 re-upsert）天然落到新状态。GitHub 逐文件写，失败整模块重试。
    """
    import marketplace_store as store

    if not module:
        return "failed", "empty module"
    async with _module_lock(module):
        from db import PostgresClient

        if PostgresClient.pool is None:
            return "failed", "no PG pool"
        # advisory lock 必须在同一连接上持有到发布结束：连接借出期间整段发布，
        # 结束显式 unlock 后归还。多实例下抢不到锁的一方退回 pending 等对方。
        conn = await PostgresClient.pool.acquire()
        try:
            locked = await conn.fetchval(
                "SELECT pg_try_advisory_lock(hashtext($1))", f"marketplace-publish:{module}"
            )
            if not locked:
                await _release_jobs_to_pending(jobs)
                return "requeued", ""
            try:
                client = _client()
                await _publish_module_locked(store, client, module, jobs)
                return "ok", ""
            finally:
                try:
                    await conn.execute(
                        "SELECT pg_advisory_unlock(hashtext($1))", f"marketplace-publish:{module}"
                    )
                except Exception:  # noqa: BLE001 - 连接异常时锁随连接释放，解锁失败可容忍
                    pass
        except Exception as exc:  # noqa: BLE001 - 单模块失败不影响其余模块
            return "failed", str(exc) or exc.__class__.__name__
        finally:
            await PostgresClient.pool.release(conn)


async def _release_jobs_to_pending(jobs: list[dict]) -> None:
    import marketplace_store as store

    ids = [int(j["id"]) for j in jobs if j.get("id") is not None]
    if ids:
        await store.requeue_claimed(ids)


async def _publish_module_locked(store, client: MarketplaceGitHub, module: str, jobs: list[dict]) -> None:
    """单模块发布主体（已持 advisory lock）。任何一步失败抛异常整模块重试。"""
    # ① 条目文件：action=refresh 只重渲 index；其余按 store 当前行状态写/删。
    # 行在（哪怕 job 是 delete——delete 后 re-upsert 的竞态）→ 写当前 manifest；
    # 行不在 → 按 payload 删除文件（含 node/mobile 资产清理）。
    by_item: dict[str, dict] = {}
    for job in jobs:
        item_id = str(job.get("item_id") or "")
        if item_id and item_id != "*":
            by_item[item_id] = job
    current_ids = await store.list_item_ids(module)
    top = (
        "node-releases" if module == "node-versions"
        else "mobile-releases" if module == "mobile-versions"
        else "device-control-releases" if module == "device-control-versions"
        else ""
    )
    for item_id, job in by_item.items():
        safe = safe_item_id(item_id.replace("/", "."))
        if not safe:
            continue
        payload = job.get("payload") or {}
        if item_id in current_ids:
            row = await store.get_item(module, item_id)
            manifest = row["manifest"] if row and isinstance(row.get("manifest"), dict) else None
            # 只镜像「已发布且非测试版」：草稿不记录到市场；测试版记录在 store（writable
            # 部署把它当正常版），但绝不进公开仓库——只读部署读仓库 raw，不该感知到
            # 测试版的存在。draft/test 若此前以 published 身份镜像过（改状态回退），
            # 这里把它的仓库文件一并删掉。GitHub Release 资产不受影响（在独立发行
            # 仓库，且 test 仍要供 writable 部署下载）。
            if manifest and str(manifest.get("status") or "") == "published" and not manifest.get("test_version"):
                await client.write_json(
                    item_path(module, safe), manifest,
                    f"market({module}): publish {item_id}",
                )
                # 发行版本另有一份 per-version release manifest（上传链路旧职责迁来）。
                if top and str(manifest.get("version") or ""):
                    await client.write_json(
                        f"{top}/{manifest['version']}/manifest.json", manifest,
                        f"market({module}): release {item_id}",
                    )
            else:
                await _delete_item_files(client, module, item_id, manifest or payload.get("manifest") or {})
        else:
            await _delete_item_files(client, module, item_id, payload.get("manifest") or {})

    # ② index：从 store 当前 summaries 渲染，只含「已发布且非测试版」——
    # 仓库索引是外部只读部署的感知面，草稿/测试版不能出现在里面。
    summaries = [
        s for s in await store.list_summaries(module)
        if isinstance(s, dict)
        and str(s.get("status") or "") == "published"
        and not s.get("test_version")
    ]
    await client.write_json(
        index_path(module, mp_config.settings.index_name),
        render_index(module, summaries),
        f"market({module}): index",
    )

    # ③ marker：模块集合与仓库现有 marker 相同则跳过写（省 2 次 RTT）。
    existing_marker = None
    try:
        got = await client.read_json_or_none(MARKER_PATH)
        if got is not None and isinstance(got[0], dict):
            existing_marker = got[0]
    except Exception:  # noqa: BLE001 - 读失败按「未知」处理，宁可多写一次
        pass
    modules_present = await store.populated_modules()
    existing_modules = set(existing_marker.get("modules") or []) if existing_marker else set()
    if existing_modules != set(modules_present):
        await client.write_json(
            MARKER_PATH, render_marker(modules_present), "market: update root marker",
        )

    # ④ node/mobile 版本投影：草稿永远不进 version.json；测试版不进仓库
    # （只读部署读仓库 raw 不该感知），但 writable 部署本进程快照把它当正常版纳入。
    if module == "node-versions":
        manifests = await store.list_manifests(module, include_hidden=True)
        # 仓库那份永远 exclude test（include_test=False）；本进程快照按 writable 决定。
        payload, errors = render_node_release(manifests, include_test=False)
        if errors:
            raise RuntimeError("version.json 渲染失败: " + "; ".join(errors))
        await client.write_json(
            NODE_RELEASE_PATH, payload,
            f"market(node-versions): version.json -> {payload.get('version') or 'empty'}",
        )
        # 本进程节点快照立即生效（与 mobile/device-control 同款 apply）。
        local_payload, _ = render_node_release(manifests, include_test=mp_config.settings.writable)
        if local_payload.get("version"):
            try:
                import node_release_catalog
                await node_release_catalog.apply_release(local_payload)
            except Exception:  # noqa: BLE001 - 缓存写失败不该让发布失败，下轮 sync 会补上
                pass
    elif module == "mobile-versions":
        manifests = await store.list_manifests(module, include_hidden=True)
        payload, errors = render_mobile_release(manifests)
        if errors:
            raise RuntimeError("mobile version.json 渲染失败: " + "; ".join(errors))
        await client.write_json(
            MOBILE_RELEASE_PATH, payload,
            f"market(mobile-versions): version.json -> {payload.get('version') or 'empty'}",
        )
        # 同进程移动端快照立即生效（与旧 _refresh_mobile_release_marker 行为一致）。
        if payload.get("version"):
            try:
                import mobile_release_catalog
                await mobile_release_catalog.apply_release(payload)
            except Exception:  # noqa: BLE001 - 缓存写失败不该让发布失败，下轮 sync 会补上
                pass
    elif module == "device-control-versions":
        manifests = await store.list_manifests(module, include_hidden=True)
        payload, errors = render_device_control_release(manifests)
        if errors:
            raise RuntimeError("device-control version.json 渲染失败: " + "; ".join(errors))
        await client.write_json(
            DEVICE_CONTROL_RELEASE_PATH, payload,
            f"market(device-control-versions): version.json -> {payload.get('version') or 'empty'}",
        )
        # 同进程设备控制 App 快照立即生效（与移动端同构）。
        if payload.get("version"):
            try:
                import device_control_release_catalog
                await device_control_release_catalog.apply_release(payload)
            except Exception:  # noqa: BLE001 - 缓存写失败不该让发布失败，下轮 sync 会补上
                pass

    # ⑤ 本进程消费缓存失效：写完立即可读（外部部署读仓库 raw 有 CDN 传播延迟，
    # 由各自 TTL 收敛，语义不变）。
    from . import consumer_cache

    consumer_cache.invalidate(module)
    if existing_modules != set(modules_present):
        consumer_cache.invalidate_marker()


async def _delete_item_files(client: MarketplaceGitHub, module: str, item_id: str, manifest: dict) -> None:
    """删条目文件；node/mobile 版本的入仓二进制与 release manifest 一并清理。

    Gitee 免费仓库容量 1GB，残留二进制会顶满镜像配额。旧 Release 时代的资产
    没有 repo_path，无从删起、自然跳过。
    """
    from ..git_clients import GitClientError

    safe = safe_item_id(item_id.replace("/", "."))
    if not safe:
        return
    await client.delete_file(item_path(module, safe), f"market({module}): delete {item_id}")
    if module not in ("node-versions", "mobile-versions", "device-control-versions"):
        return
    for asset in (manifest.get("assets") or []):
        if isinstance(asset, dict) and str(asset.get("repo_path") or "").strip():
            try:
                await client.delete_file(
                    str(asset["repo_path"]), f"market({module}): delete asset {asset.get('filename')}"
                )
            except GitClientError as exc:
                logger.warning(
                    "[marketplace-publisher] delete asset {} failed for {}: {}",
                    asset.get("repo_path"), item_id, exc,
                )
    version = str(manifest.get("version") or "")
    if version:
        top = (
            "node-releases" if module == "node-versions"
            else "mobile-releases" if module == "mobile-versions"
            else "device-control-releases"
        )
        try:
            await client.delete_file(
                f"{top}/{version}/manifest.json", f"market({module}): delete release manifest {item_id}"
            )
        except GitClientError as exc:
            logger.warning("[marketplace-publisher] delete release manifest for {} failed: {}", item_id, exc)
