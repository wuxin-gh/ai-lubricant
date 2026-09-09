"""预同步并缓存 ``node-releases/version.json``。

这是节点升级链路的唯一真相源。市场管理页上传/删除版本时会写这份文件；
本模块定期从 GitHub raw 拉取并缓存到 PostgreSQL，让节点详情不必每次都打 GitHub。

远端失败时保留最后一次成功快照并标记 ``stale``；消费侧据此显示"版本信息可能过期"
而不是直接判升级不可用。
"""
from __future__ import annotations

import asyncio
import copy
import datetime as _dt
import json
import os
from typing import Any

import aiohttp
from loguru import logger

from db import PostgresClient
from user_platform.marketplace import config as mp_config
from user_platform.marketplace.validator import validate_node_release

SNAPSHOT_KEY = "node_release_snapshot"
MAX_FILE_BYTES = 256 * 1024
FETCH_TIMEOUT_SECONDS = 20
DEFAULT_SYNC_SECONDS = 600

_lock = asyncio.Lock()
_snapshot: dict[str, Any] = {"version": "", "version_notes": "", "assets": [], "updated_at": "", "stale": True}


async def load_snapshot() -> None:
    global _snapshot
    try:
        stored = await PostgresClient.get_config(SNAPSHOT_KEY)
    except Exception as exc:
        logger.warning("[node-release] load snapshot failed: {}", exc)
        return
    if isinstance(stored, dict) and isinstance(stored.get("assets"), list):
        _snapshot = copy.deepcopy(stored)
        logger.info("[node-release] loaded snapshot version={}", _snapshot.get("version") or "(empty)")


async def get_latest_release() -> dict[str, Any]:
    """返回当前缓存的 version.json（或空骨架）。永远不抛。"""
    return copy.deepcopy(_snapshot)


async def apply_release(release: dict[str, Any], *, publish: bool = True) -> dict[str, Any]:
    """立即应用市场写侧刚生成的 ``version.json``，避免等待 raw 定时同步。

    市场上传与消费缓存通常在同一个服务进程：上传完成后已经拿到权威 payload，
    直接写 PG + 内存比再走 GitHub raw 更快，也避开 raw/CDN 短暂传播延迟。多实例
    通过 runtime_sync 广播后由其他实例从 DB 重载。

    payload 里的 asset 带仓库相对路径 ``repo_path``（入仓存储）；落库前按消费侧
    平台渲染出绝对 ``download_url``，下游（select/coverage/升级帧）零改动。
    """
    global _snapshot
    from user_platform.marketplace import urls

    errors = validate_node_release(urls.normalize_release_assets(copy.deepcopy(release)))
    if errors:
        raise ValueError("version.json invalid: " + "; ".join(errors))
    now = _dt.datetime.now(_dt.timezone.utc).isoformat()
    next_snapshot = {
        **copy.deepcopy(release),
        "updated_at": release.get("updated_at") or now,
        "stale": False,
        "fetched_at": now,
    }
    urls.normalize_release_assets(next_snapshot)
    async with _lock:
        await PostgresClient.set_config(SNAPSHOT_KEY, next_snapshot)
        _snapshot = copy.deepcopy(next_snapshot)
    if publish:
        import runtime_sync
        await runtime_sync.publish(runtime_sync.EVENT_NODE_RELEASE, "__all__")
    logger.info("[node-release] applied market release version={}", _snapshot.get("version") or "(empty)")
    return copy.deepcopy(_snapshot)


def _raw_url() -> str:
    from user_platform.marketplace import urls

    return urls.consumer_raw_url("node-releases/version.json")


def _asset_version(asset: dict, fallback: str) -> str:
    """一条 asset 的来源版本号；合并后的 version.json 每条 asset 自带 version，
    旧的单版本 version.json 没有 per-asset version，回退到顶层 version。"""
    return str(asset.get("version") or fallback or "")


def select_upgrade_assets(latest: dict[str, Any], *, node_role: str, os_name: str, arch: str) -> dict[str, Any]:
    """从 version.json 选出该节点升级所需的 runtime 资产与节点程序资产。

    返回 ``{version, version_notes, release_tag, runtime, node}``；缺资产的位为 None。
    调用方据此判断"是否可升级"以及"缺哪一项"。

    version.json 现在按 ``(component, role, platform, arch)`` 各自取最新版本合并，
    因此 runtime/node 两条资产可能来自不同版本。``target_version`` 取该平台资产自带
    的 version（回退到顶层 version），而不是全局单一 version——这样只缺某一平台
    的新版本时，老平台仍能按它自己的最新版本升级。
    """
    assets = [a for a in (latest.get("assets") or []) if isinstance(a, dict)]
    role = (node_role or "").strip()
    wanted_node_role = (
        "ios_host" if role == "ios_host"
        else "management" if role in ("management", "passive_management")
        else "execution"
    )
    top_version = str(latest.get("version") or "")

    runtime_asset = None
    node_asset = None
    for asset in assets:
        if asset.get("role") == "runtime":
            # ios_host 不跑 JS runtime（只跑自己的 node-ios 管控栈），永远不下发。
            if role == "ios_host":
                continue
            # 通用 runtime 包（platform/arch = any，node-runtime.tar.gz）所有平台
            # 共用；旧命名按 (os, arch) 分列的资产仍精确匹配，双口径并存。
            plat = str(asset.get("platform") or "")
            arch_val = str(asset.get("arch") or "")
            if (plat, arch_val) == ("any", "any") or (plat == os_name and arch_val == arch):
                runtime_asset = asset
        elif asset.get("role") == wanted_node_role and asset.get("platform") == os_name and asset.get("arch") == arch:
            node_asset = asset

    return {
        "version": top_version,
        "version_notes": str(latest.get("version_notes") or ""),
        "release_tag": str(latest.get("release_tag") or ""),
        "runtime": runtime_asset,
        "node": node_asset,
    }


def _clean_version(value: Any) -> str:
    """归一版本号：去前导 v/V、空白，统一成裸串用于比较。"""
    return str(value or "").strip().lstrip("vV").strip()


def node_upgrade_status(
    latest: dict[str, Any], *, node_role: str, os_name: str, arch: str,
    current_node_version: str, current_runtime_version: str,
) -> dict[str, Any]:
    """计算该节点「节点程序」与「runtime」两条线各自的当前/最新/是否可升级。

    version.json 按 (role, platform, arch) 分平台各自取最新版本合并，runtime 与节点
    程序是**两条独立版本线**——runtime 的最新版不一定等于节点程序的最新版。因此判定
    必须分开比，而不是拿一个 latest 同时要求两者相等（旧逻辑据此恒判可升级）。

    版本号统一在服务端去 v 前缀后再比较与返回，前端直接消费。

    版本空间兜底：market 版本号是发布号（``YYYYMMDD-HHMM``）；存量节点未升级过时上报的
    runtime_version 可能是 runtime 自带的 semver（如 ``0.6.0``），跟发布号不在一个空间，
    无法比较。此时 runtime 线的 needs_upgrade 跟随节点程序线——两者同源发布，节点程序
    需要升级就一起升；节点程序已最新则 runtime 也视为最新，避免 semver↔发布号恒判可升。
    服务端驱动的 RuntimeUpgrade 成功后会写发布号 marker，之后 runtime 上报发布号、
    进入可比空间，本兜底就不再生效。
    """
    # 复用市场校验器对发布号格式的判定，避免本模块重复正则。
    from user_platform.marketplace.validator import _NODE_VERSION_RE

    def _is_release_tag(v: str) -> bool:
        return bool(_NODE_VERSION_RE.match((v or "").strip()))

    selected = select_upgrade_assets(
        latest, node_role=node_role, os_name=os_name, arch=arch,
    )
    runtime_asset = selected.get("runtime") or {}
    node_asset = selected.get("node") or {}

    node_latest = _clean_version(node_asset.get("version") or selected.get("version") or "")
    runtime_latest = _clean_version(runtime_asset.get("version") or "")
    node_current = _clean_version(current_node_version)
    runtime_current = _clean_version(current_runtime_version)

    node_needs = bool(node_latest) and node_current != node_latest
    # runtime 与 latest 都在发布号空间才直接比；存量节点上报 semver（0.6.0）时跟随
    # 节点程序线（同源发布）——节点已最新就不报 runtime 升级，节点要升就一起升。
    if _is_release_tag(runtime_current) and _is_release_tag(runtime_latest):
        runtime_needs = bool(runtime_latest) and runtime_current != runtime_latest
    else:
        runtime_needs = node_needs

    return {
        "stale": bool(latest.get("stale")),
        "node_program": {
            "current": node_current,
            "latest": node_latest,
            "installed": bool(node_current),
            "needs_upgrade": node_needs,
        },
        "runtime": {
            "current": runtime_current,
            "latest": runtime_latest,
            "installed": bool(runtime_current),
            "needs_upgrade": runtime_needs,
        },
        "can_upgrade": bool(node_needs or runtime_needs),
    }


def coverage(latest: dict[str, Any]) -> list[dict[str, Any]]:
    """把 version.json 的 assets 投影成每个 (role, platform, arch) 的最新版本矩阵。

    前端据此按节点环境判断"能否升级/部署"，而不是只看顶层单一 version。每条带
    version/download_url/digest/size_bytes，消费侧无需再读 raw。
    """
    out: list[dict[str, Any]] = []
    for asset in (latest.get("assets") or []):
        if not isinstance(asset, dict):
            continue
        out.append({
            "role": str(asset.get("role") or ""),
            "platform": str(asset.get("platform") or ""),
            "arch": str(asset.get("arch") or ""),
            "version": _asset_version(asset, str(latest.get("version") or "")),
            "download_url": str(asset.get("download_url") or ""),
            "digest": str(asset.get("digest") or ""),
            "size_bytes": int(asset.get("size_bytes") or 0),
        })
    return out


async def _render_from_store() -> dict[str, Any]:
    """writable 部署：store 是真相源，从 store 渲染 version.json。

    ``include_test=True``——writable 部署把测试版当正常版纳入本部署升级链路。
    不读仓库 raw：仓库那份 ``include_test=False``，读它会把 ``apply_release`` 刚
    写入的测试版资产从快照里冲掉。
    """
    import marketplace_store as store
    from user_platform.marketplace.render import render_node_release

    if not await store.is_populated():
        raise RuntimeError("store 未填充（bootstrap 前）")
    manifests = await store.list_manifests("node-versions", include_hidden=True)
    payload, errors = render_node_release(manifests, include_test=True)
    if errors:
        raise RuntimeError("version.json 渲染失败: " + "; ".join(errors))
    return payload


async def _fetch_from_repo() -> dict[str, Any]:
    """只读部署：从仓库 raw 拉 version.json（仓库已 exclude test，自然不含测试版）。"""
    from providers.proxy_manager import get_proxy_manager

    resp = await get_proxy_manager().request(
        url=_raw_url(),
        method="GET",
        headers={"User-Agent": "ai-lubricant-node-release"},
        timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_SECONDS),
        proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status != 200:
        raise RuntimeError(f"version.json returned HTTP {resp.status}")
    raw = await resp.read()
    if len(raw) > MAX_FILE_BYTES:
        raise RuntimeError(f"version.json exceeds {MAX_FILE_BYTES} bytes")
    return json.loads(raw.decode("utf-8"))


async def refresh(*, publish: bool = True) -> dict[str, Any]:
    global _snapshot
    settings = mp_config.consumer_settings
    if not settings.enabled or "node-versions" not in settings.modules:
        return {"ok": False, "error": "节点版本源未启用", **await get_latest_release()}
    async with _lock:
        try:
            # writable 部署走 store 渲染（含测试版，当正常版）；只读部署走仓库 raw
            # （仓库那份 exclude test，自然不含测试版——别人无法感觉到）。
            data = (
                await _render_from_store()
                if mp_config.settings.writable
                else await _fetch_from_repo()
            )
            # 入仓资产的 repo_path 按消费侧平台（github/gitee）渲染绝对 download_url，
            # 再交给校验——校验器与下游永远看到合法 HTTPS 地址；旧资产（Release 直链
            # 时代）无 repo_path，保留原 download_url。
            from user_platform.marketplace import urls

            data = urls.normalize_release_assets(data)
            errors = validate_node_release(data)
            if errors:
                raise RuntimeError("version.json invalid: " + "; ".join(errors))
            previous_version = str(_snapshot.get("version") or "")
            next_version = str(data.get("version") or "")
            now = _dt.datetime.now(_dt.timezone.utc).isoformat()
            next_snapshot = {**data, "updated_at": data.get("updated_at") or now, "stale": False, "fetched_at": now}
            # 先更新内存快照：即使 PG 写失败（本地无 DB / 连接池未初始化），消费侧
            # 仍能拿到本次拉到的数据。DB 落库是持久化 + 跨实例共享，失败只降级不丢数据。
            _snapshot = copy.deepcopy(next_snapshot)
            try:
                await PostgresClient.set_config(SNAPSHOT_KEY, next_snapshot)
            except Exception as exc:
                logger.debug("[node-release] set_config failed (in-memory snapshot still updated): {}", exc)
            if publish:
                import runtime_sync
                await runtime_sync.publish(runtime_sync.EVENT_NODE_RELEASE, "__all__")
            if next_version and previous_version and next_version != previous_version:
                # 只在已知旧版本后发现一个不同的新版本时触发；首次启动加载不制造假通知。
                from user_platform.notify_core import emit_notification_background
                emit_notification_background(
                    "node.new_version",
                    params={
                        "version": next_version,
                        "previous_version": previous_version,
                        "version_notes": str(data.get("version_notes") or ""),
                    },
                    owner_type="platform",
                    severity="info",
                    source="node_release_catalog",
                    message=f"节点版本已从 {previous_version} 更新为 {next_version}",
                    dedupe_key=f"node.new_version:{next_version}",
                    dedupe_window_seconds=86400,
                )
            logger.info("[node-release] synced version={}", _snapshot.get("version") or "(empty)")
            return {"ok": True, **copy.deepcopy(_snapshot)}
        except Exception as exc:
            _snapshot["stale"] = True
            logger.warning("[node-release] refresh failed; keeping last snapshot: {}", exc)
            return {"ok": False, "error": str(exc), **copy.deepcopy(_snapshot)}


async def reload_from_db() -> None:
    await load_snapshot()


async def sync_loop() -> None:
    interval = max(60, int(os.getenv("NODE_RELEASE_SYNC_INTERVAL", str(DEFAULT_SYNC_SECONDS))))
    while True:
        try:
            await refresh()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[node-release] sync cycle failed: {}", exc)
        await asyncio.sleep(interval)
