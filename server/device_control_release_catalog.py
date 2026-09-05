"""预同步并缓存 ``device-control-releases/version.json``。

设备控制 App（被控端）升级链路的唯一真相源，与移动端控制 App 同构但独立：
市场管理页上传/删除版本时写这份文件；本模块定期从 GitHub raw 拉取并缓存到
PostgreSQL，让「添加设备」弹框不必每次都打 GitHub。

Android 为 APK、iOS 为侧载 IPA（被控端不上 App Store，由 iOS 宿主节点经
go-ios 安装）；与移动端控制 App 分离管理（控制端 vs 被控端）。
远端失败时保留最后一次成功快照并标记 ``stale``。
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
from monkeycode_compat.marketplace import config as mp_config
from monkeycode_compat.marketplace.validator import validate_device_control_release

SNAPSHOT_KEY = "device_control_release_snapshot"
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
        logger.warning("[device-control-release] load snapshot failed: {}", exc)
        return
    if isinstance(stored, dict) and isinstance(stored.get("assets"), list):
        _snapshot = copy.deepcopy(stored)
        logger.info("[device-control-release] loaded snapshot version={}", _snapshot.get("version") or "(empty)")


async def get_latest_release() -> dict[str, Any]:
    """返回当前缓存的 version.json（或空骨架）。永远不抛。"""
    return copy.deepcopy(_snapshot)


async def apply_release(release: dict[str, Any], *, publish: bool = True) -> dict[str, Any]:
    """立即应用市场写侧刚生成的 ``version.json``，避免等待 raw 定时同步。

    与节点/移动端侧同构：上传完成后已拿到权威 payload，直接写 PG + 内存比再走
    raw 更快，也避开 raw/CDN 短暂传播延迟。多实例通过 runtime_sync 广播后从 DB 重载。

    payload 里的 asset 带仓库相对路径 ``repo_path``（入仓存储）；落库前按消费侧
    平台渲染出绝对 ``download_url``，下游（select_asset/添加设备弹框）零改动。
    """
    global _snapshot
    from monkeycode_compat.marketplace import urls

    errors = validate_device_control_release(urls.normalize_release_assets(copy.deepcopy(release)))
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
        await runtime_sync.publish(runtime_sync.EVENT_DEVICE_CONTROL_RELEASE, "__all__")
    logger.info("[device-control-release] applied market release version={}", _snapshot.get("version") or "(empty)")
    return copy.deepcopy(_snapshot)


def _raw_url() -> str:
    from monkeycode_compat.marketplace import urls

    return urls.consumer_raw_url("device-control-releases/version.json")


def select_asset(latest: dict[str, Any], platform: str) -> dict[str, Any] | None:
    """从 version.json 选出指定平台（android=APK / ios=IPA）的资产；没有返回 None。"""
    for asset in (latest.get("assets") or []):
        if isinstance(asset, dict) and asset.get("platform") == platform:
            return asset
    return None


def select_android_asset(latest: dict[str, Any]) -> dict[str, Any] | None:
    """从 version.json 选出 Android APK 资产；没有返回 None。"""
    return select_asset(latest, "android")


def select_ios_asset(latest: dict[str, Any]) -> dict[str, Any] | None:
    """从 version.json 选出 iOS IPA 资产；没有返回 None。"""
    return select_asset(latest, "ios")


def _clean_version(value: Any) -> str:
    """归一版本号：去前导 v/V、空白，统一成裸串用于比较。"""
    return str(value or "").strip().lstrip("vV").strip()


async def refresh(*, publish: bool = True) -> dict[str, Any]:
    global _snapshot
    settings = mp_config.consumer_settings
    if not settings.enabled or "device-control-versions" not in settings.modules:
        return {"ok": False, "error": "设备控制版本源未启用", **await get_latest_release()}
    async with _lock:
        try:
            from providers.proxy_manager import get_proxy_manager

            resp = await get_proxy_manager().request(
                url=_raw_url(),
                method="GET",
                headers={"User-Agent": "ai-lubricant-device-control-release"},
                timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_SECONDS),
                proxy_config_id=mp_config.settings.proxy_id or None,
            )
            if resp.status != 200:
                raise RuntimeError(f"version.json returned HTTP {resp.status}")
            raw = await resp.read()
            if len(raw) > MAX_FILE_BYTES:
                raise RuntimeError(f"version.json exceeds {MAX_FILE_BYTES} bytes")
            data = json.loads(raw.decode("utf-8"))
            # 与节点/移动端侧同口径：入仓资产按消费侧平台渲染绝对 download_url 再校验落库。
            from monkeycode_compat.marketplace import urls

            data = urls.normalize_release_assets(data)
            errors = validate_device_control_release(data)
            if errors:
                raise RuntimeError("version.json invalid: " + "; ".join(errors))
            previous_version = str(_snapshot.get("version") or "")
            next_version = str(data.get("version") or "")
            now = _dt.datetime.now(_dt.timezone.utc).isoformat()
            next_snapshot = {**data, "updated_at": data.get("updated_at") or now, "stale": False, "fetched_at": now}
            # 先更新内存快照：即使 PG 写失败（本地无 DB），消费侧仍能拿到本次拉到的数据。
            _snapshot = copy.deepcopy(next_snapshot)
            try:
                await PostgresClient.set_config(SNAPSHOT_KEY, next_snapshot)
            except Exception as exc:
                logger.debug("[device-control-release] set_config failed (in-memory snapshot still updated): {}", exc)
            if publish:
                import runtime_sync
                await runtime_sync.publish(runtime_sync.EVENT_DEVICE_CONTROL_RELEASE, "__all__")
            if next_version and previous_version and next_version != previous_version:
                from monkeycode_compat.notify_core import emit_notification_background
                emit_notification_background(
                    "device_control.new_version",
                    params={
                        "version": next_version,
                        "previous_version": previous_version,
                        "version_notes": str(data.get("version_notes") or ""),
                    },
                    owner_type="platform",
                    severity="info",
                    source="device_control_release_catalog",
                    message=f"设备控制 App 版本已从 {previous_version} 更新为 {next_version}",
                    dedupe_key=f"device_control.new_version:{next_version}",
                    dedupe_window_seconds=86400,
                )
            logger.info("[device-control-release] synced version={}", _snapshot.get("version") or "(empty)")
            return {"ok": True, **copy.deepcopy(_snapshot)}
        except Exception as exc:
            _snapshot["stale"] = True
            logger.warning("[device-control-release] refresh failed; keeping last snapshot: {}", exc)
            return {"ok": False, "error": str(exc), **copy.deepcopy(_snapshot)}


async def reload_from_db() -> None:
    await load_snapshot()


async def sync_loop() -> None:
    interval = max(60, int(os.getenv("DEVICE_CONTROL_RELEASE_SYNC_INTERVAL", str(DEFAULT_SYNC_SECONDS))))
    while True:
        try:
            await refresh()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[device-control-release] sync cycle failed: {}", exc)
        await asyncio.sleep(interval)
