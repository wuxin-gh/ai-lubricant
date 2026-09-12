"""消费侧 GitHub raw 读缓存（进程内存 + 定时后台刷新 + 写失效）。

与 :mod:`.channel_catalog` / ``server/node_release_catalog`` / ``server/mobile_release_catalog``
同一套模式：内存快照 + ``sync_loop`` 定时刷新 + 管理端写操作立即失效 + 远端失败时
保留最后一次成功快照（消费侧据此降级显示而不是空白）。

覆盖对象是消费侧全部 raw 读（``/consumer/index``、``/consumer/item``、
``/consumer/status`` 的 marketplace.json marker）。此前这些端点每个请求都现拉一次
GitHub raw（经代理普遍 1-3s），市场页 / 历史版本弹窗 / ``useMarketplaceEnabled``
探测都打在同一条链路上。改造后：

- 索引文件与根 marker 由后台循环预热（每模块一个 index.json，约 7 个文件/轮），
  热路径读请求直接命中内存，零出网延迟；
- 单条 manifest 走 read-through：首次读现拉并缓存，TTL 内命中；管理端写路径
  本进程立即失效（见 :func:`invalidate`），外部直改 GitHub 的收敛上界即 TTL。

多 worker 部署下各持一份内存：A worker 处理管理写、B worker 最多 TTL 后收敛
（已接受的取舍，与 github.py 的管理读缓存一致）。缓存不落 PG——快照丢失只是
退回今天的现拉行为，没有持久化价值。
"""
from __future__ import annotations

import asyncio
import copy
import os
import time
from typing import Any

from loguru import logger

from . import config as mp_config
from ..git_clients import GitClientError

# ── 参数 ─────────────────────────────────────────────────────────────────────

DEFAULT_ITEM_TTL_SECONDS = 300
DEFAULT_SYNC_SECONDS = 600

_CACHE_MAX_ENTRIES = 512  # 与 github.py 管理读缓存同量级：市场条目总数远小于此
_MARKER_PATH = "marketplace.json"


def _item_ttl_seconds() -> int:
    try:
        return max(30, int(os.getenv("MARKETPLACE_CONSUMER_ITEM_TTL", str(DEFAULT_ITEM_TTL_SECONDS))))
    except ValueError:
        return DEFAULT_ITEM_TTL_SECONDS


def _sync_interval_seconds() -> int:
    try:
        return max(60, int(os.getenv("MARKETPLACE_CONSUMER_CACHE_SYNC_INTERVAL", str(DEFAULT_SYNC_SECONDS))))
    except ValueError:
        return DEFAULT_SYNC_SECONDS


# ── 缓存结构 ────────────────────────────────────────────────────────────────
# path -> {"data": Any | None, "fetched_at": float}
# data 为 None 表示「确认不存在」（缓存的 404，与 github.py 同口径）。
# 缓存键是仓库相对 path（不含 owner/repo/branch）：consumer 坐标变更后旧条目
# 属于另一个仓库，必须整体失效——reload_settings 时消费侧坐标变了，调用方
# （source-config PUT 路由）清空全部即可；正常运行期坐标不会变。

_cache: dict[str, dict[str, Any]] = {}
_lock = asyncio.Lock()  # 仅用于 refresh_all 防重入；读路径不加锁（dict 操作原子够用）


def _now() -> float:
    return time.monotonic()


def _get_entry(path: str, *, now: float | None = None) -> tuple[bool, Any]:
    """返回 ``(hit, data)``。hit=False 表示未缓存或已过期。

    过期条目**不清除**——保留在缓存里，供 get_raw 在重新拉取失败时回退
    （远端故障时消费侧降级显示旧快照，而不是空白/报错）。真正清除过期
    条目只发生在：容量逐出、模块失效、拉取成功回填覆盖。
    """
    entry = _cache.get(path)
    if entry is None:
        return False, None
    at = _now() if now is None else now
    if at - float(entry["fetched_at"]) > _item_ttl_seconds():
        return False, None
    return True, copy.deepcopy(entry["data"])


def _put_entry(path: str, data: Any | None) -> None:
    if len(_cache) >= _CACHE_MAX_ENTRIES:
        oldest = min(_cache, key=lambda p: float(_cache[p]["fetched_at"]))
        _cache.pop(oldest, None)
    _cache[path] = {"data": copy.deepcopy(data), "fetched_at": _now()}


# ── 出网（与 routes._fetch_consumer_raw 同一条链路）─────────────────────────

_FETCH_TIMEOUT_SECONDS = 15


async def _fetch_raw(path: str) -> Any:
    """经 proxy_manager 按 consumer 坐标拉 raw JSON；404/失败抛 GitClientError。"""
    import aiohttp

    from providers.proxy_manager import get_proxy_manager
    from .urls import consumer_raw_url

    try:
        resp = await get_proxy_manager().request(
            url=consumer_raw_url(path),
            method="GET",
            headers={"User-Agent": "ai-lubricant-marketplace-consumer"},
            timeout=aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_SECONDS),
            proxy_config_id=mp_config.settings.proxy_id or None,
        )
    except Exception as exc:  # proxy_manager 自身异常（非 HTTP 错误）
        raise GitClientError(f"GET {path} failed: {exc}") from exc
    if resp.status == 404:
        raise GitClientError(f"GET {path} returned HTTP 404")
    if resp.status >= 400:
        raise GitClientError(f"GET {path} returned HTTP {resp.status}")
    return await resp.json()


async def _fetch_and_store(path: str) -> Any:
    """现拉一次并回填缓存。404 也缓存（确认不存在同样是一次 RTT）。"""
    try:
        data = await _fetch_raw(path)
    except GitClientError as exc:
        if "HTTP 404" in str(exc):
            _put_entry(path, None)
        raise
    _put_entry(path, data)
    return copy.deepcopy(data)


# ── 对外接口 ────────────────────────────────────────────────────────────────

async def get_raw(path: str, *, fresh: bool = False) -> tuple[Any | None, bool]:
    """读一个 raw 文件，返回 ``(data, stale)``。

    - 命中未过期直接回（stale=False）；
    - miss/过期/``fresh`` 现拉并回填；拉取失败时**若有旧快照回旧值并标 stale**
      （消费侧降级显示而不是空白），完全没有快照则向上抛 GitClientError（调用方
      按既有语义降级成空索引/404/502）。
    - data 为 None 表示「确认不存在」（缓存的 404），不视为失败。

    **仅后台同步任务调用**——它会在 miss 时同步出网。HTTP 读路径必须用
    :func:`peek`，绝不在请求线程里触发远程拉取（那是市场列表慢的根因）。
    """
    if fresh:
        _cache.pop(path, None)
    else:
        hit, data = _get_entry(path)
        if hit:
            return data, False
    try:
        data = await _fetch_and_store(path)
        return data, False
    except GitClientError:
        entry = _cache.get(path)
        if entry is not None and entry["data"] is not None:
            logger.warning("[consumer-cache] refresh {} failed; serving last snapshot", path)
            return copy.deepcopy(entry["data"]), True
        raise


async def peek(path: str) -> tuple[Any | None, bool]:
    """只读缓存、**绝不触发网络拉取**。返回 ``(data, stale)``。

    供 HTTP 读路径（``/consumer/*``）使用：缓存未命中/过期返回 ``(None, False)``，
    过期但有旧值返回 ``(旧值, True)``——由后台 ``sync_loop`` 预热，请求线程永远
    不等待远程。远端故障时消费侧照旧降级显示旧快照或空态，而不是卡在代理/GitHub。
    """
    hit, data = _get_entry(path)
    if hit:
        return data, False
    entry = _cache.get(path)
    if entry is not None and entry["data"] is not None:
        return copy.deepcopy(entry["data"]), True
    return None, False


def invalidate(module: str | None = None) -> None:
    """失效缓存。``module`` 给定只清该模块（index + items）；None 清空全部。

    publisher 成功写完仓库后调用，保证本进程 raw 读立即收敛。marker
    （marketplace.json）只在模块集合变化时写，由 publisher 单独调用
    ``invalidate_marker``。
    """
    if module is None:
        _cache.clear()
        return
    prefix = f"modules/{module}/"
    for path in [p for p in _cache if p.startswith(prefix)]:
        _cache.pop(path, None)


def invalidate_marker() -> None:
    """只失效根 marker（marketplace.json 被 publisher 改写时调用）。"""
    _cache.pop(_MARKER_PATH, None)


async def refresh_all() -> dict[str, Any]:
    """后台刷新热路径：各模块 index + 已发布条目的 manifest + 根 marker。

    逐文件容错，失败保留旧快照。既然 HTTP 读路径已改为 ``peek``（绝不出网），
    这里必须把单条 manifest 也一起预热——否则只读部署的 ``/consumer/item`` 永远
    404（后台不 warm、请求又不拉）。
    """
    from .validator import index_path, item_path, safe_item_id

    async with _lock:
        settings = mp_config.settings
        if not settings.enabled:
            return {"ok": False, "error": "marketplace disabled"}
        modules = [*settings.modules, "node-versions", "mobile-versions"]
        ok = 0
        failed = 0

        async def _warm(path: str) -> None:
            nonlocal ok, failed
            try:
                await _fetch_and_store(path)
                ok += 1
            except GitClientError as exc:
                failed += 1
                logger.warning("[consumer-cache] refresh {} failed (keeping snapshot): {}", path, exc)
            except Exception as exc:  # 刷新失败绝不打断循环
                failed += 1
                logger.warning("[consumer-cache] refresh {} failed: {}", path, exc)

        for module in dict.fromkeys(modules):
            index_file = index_path(module, mp_config.consumer_settings.index_name)
            await _warm(index_file)
            # 预热该模块已发布条目的 manifest，供 /consumer/item 的 peek 命中。
            entry = _cache.get(index_file)
            data = entry.get("data") if entry else None
            for row in (data.get("items") or []) if isinstance(data, dict) else []:
                if not isinstance(row, dict) or row.get("status") != "published":
                    continue
                raw_id = str(row.get("id") or "")
                safe = safe_item_id(raw_id.replace("/", "."))
                if not safe:
                    continue
                await _warm(row.get("item_path") or item_path(module, safe))
        await _warm(_MARKER_PATH)
        return {"ok": True, "refreshed": ok, "failed": failed}


async def sync_loop() -> None:
    """定时后台刷新。首轮即预热（启动不阻塞——冷启动首请求与旧版现拉行为一致）。"""
    while True:
        try:
            await refresh_all()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[consumer-cache] sync cycle failed: {}", exc)
        await asyncio.sleep(_sync_interval_seconds())
