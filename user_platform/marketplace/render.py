"""仓库发布文件的纯渲染函数（publisher 专用，无 IO、无 HTTPException）。

publisher 从 store 读出当前状态后用这里把 GitHub 仓库文件渲染出来：

- ``render_index``     → modules/{module}/index.json（index 行投影，summary 在
  写入 store 时已由 validator.index_summary 算好，这里只做组装排序）
- ``render_marker``    → marketplace.json（模块白名单 marker）
- ``render_node_release``  → node-releases/version.json（升级链路唯一文件）
- ``render_mobile_release`` → mobile-releases/version.json

node/mobile 的分组算法迁自 routes._refresh_node_release_marker /
_refresh_mobile_release_marker：published 非测试版本按
(component, role, platform, arch) 分组、组内取版本最大（平台缺口回退旧版本的
语义保持——某平台可能只在较旧的版本里存在）；移动端只有一个 Android 产物，
直接取全局最大 published manifest。校验错误不再抛 HTTPException，改为
返回 errors 列表，由 publisher 决定重试或失败。
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Any

from .validator import (
    MARKET_INDEX_SCHEMA,
    identify_device_control_asset,
    identify_mobile_asset,
    identify_node_asset,
    validate_device_control_release,
    validate_mobile_release,
    validate_node_release,
)

MARKET_MARKER_SCHEMA = "ai-lubricant.market.v1"


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _version_sort_key(version: str) -> tuple:
    """semver 排序键；非法版本排到最后（视作最小）。"""
    parts = re.split(r"[.\-+]", (version or "").strip())
    key = []
    for part in parts[:3]:
        key.append(int(part) if part.isdigit() else -1)
    while len(key) < 3:
        key.append(-1)
    return tuple(key)


def render_index(module: str, summaries: list[dict]) -> dict:
    """index.json：schema + module + 排序后的 summary 行。"""
    items = sorted(
        [dict(s) for s in summaries if isinstance(s, dict)],
        key=lambda s: str(s.get("id") or ""),
    )
    return {
        "schema": MARKET_INDEX_SCHEMA,
        "module": module,
        "updated_at": _now(),
        "items": items,
    }


def render_marker(modules_present: list[str] | set[str]) -> dict:
    """marketplace.json：消费侧靠它识别「这是合法市场仓库」。集合去重排序。"""
    return {
        "schema": MARKET_MARKER_SCHEMA,
        "modules": sorted({str(m) for m in modules_present if str(m)}),
        "updated_at": _now(),
    }


def render_node_release(manifests: list[dict], *, include_test: bool = False) -> tuple[dict, list[str]]:
    """node-releases/version.json：返回 ``(payload, errors)``。

    草稿永远排除；测试版（``test_version``）默认排除——仓库镜像与只读部署消费侧都
    不该感知到测试版。writable 部署的本进程快照以 ``include_test=True`` 渲染，把
    测试版当正常版纳入升级链路（「配置了市场管理」即视为正常版）。无可用版本仍
    渲染空 schema 文件，让消费侧区分「暂无版本」与「仓库不是市场」。
    """
    payload: dict = {
        "schema": "ai-lubricant.node-release/v1",
        "version": "", "version_notes": "", "assets": [], "updated_at": _now(),
    }
    best_manifest: dict | None = None
    latest_assets: dict[tuple, tuple[tuple, dict]] = {}
    for manifest in manifests:
        if not isinstance(manifest, dict):
            continue
        if manifest.get("status") != "published":
            continue
        if manifest.get("test_version") and not include_test:
            continue
        version = str(manifest.get("version") or "")
        version_key = _version_sort_key(version)
        if best_manifest is None or version_key > _version_sort_key(str(best_manifest.get("version") or "")):
            best_manifest = manifest
        for asset in (manifest.get("assets") or []):
            if not isinstance(asset, dict):
                continue
            ident = identify_node_asset(asset.get("filename"))
            if not ident:
                continue
            key = (ident["component"], ident["role"], ident["platform"], ident["arch"])
            existing = latest_assets.get(key)
            if existing is None or version_key > existing[0]:
                latest_assets[key] = (version_key, {**asset, "version": version})

    if latest_assets:
        assets = [latest_assets[key][1] for key in latest_assets]
        # 稳定排序，避免每次写 version.json 因字典顺序漂移产生无意义的 diff。
        assets.sort(key=lambda a: (
            str(a.get("component") or ""), str(a.get("role") or ""),
            str(a.get("platform") or ""), str(a.get("arch") or ""),
        ))
        payload["assets"] = assets
    if best_manifest is not None:
        payload.update({
            "version": str(best_manifest.get("version") or ""),
            "version_notes": str(best_manifest.get("version_notes") or ""),
            "release_tag": str(best_manifest.get("release_tag") or ""),
        })
    errors = validate_node_release(payload) if best_manifest is not None else []
    return payload, errors


def render_mobile_release(manifests: list[dict]) -> tuple[dict, list[str]]:
    """mobile-releases/version.json：返回 ``(payload, errors)``。

    移动端只有一个 Android 产物、不分平台矩阵：取全局版本号最大的 published
    manifest。iOS 无可下载资产（走 App Store），最新版本与商店链接放顶层 ``ios`` 块。
    """
    payload: dict = {
        "schema": "ai-lubricant.mobile-release/v1",
        "version": "", "version_notes": "", "assets": [], "updated_at": _now(),
    }
    best_manifest: dict | None = None
    for manifest in manifests:
        if not isinstance(manifest, dict):
            continue
        if manifest.get("status") != "published":
            continue
        version = str(manifest.get("version") or "")
        if best_manifest is None or _version_sort_key(version) > _version_sort_key(str(best_manifest.get("version") or "")):
            best_manifest = manifest

    if best_manifest is not None:
        version = str(best_manifest.get("version") or "")
        assets = []
        for asset in (best_manifest.get("assets") or []):
            if not isinstance(asset, dict):
                continue
            if not identify_mobile_asset(asset.get("filename")):
                continue
            assets.append({**asset, "version": version})
        payload.update({
            "version": version,
            "version_notes": str(best_manifest.get("version_notes") or ""),
            "release_tag": str(best_manifest.get("release_tag") or ""),
            "assets": assets,
        })
        store_url = str(best_manifest.get("ios_store_url") or "")
        if store_url:
            payload["ios"] = {"version": version, "store_url": store_url}
    errors = validate_mobile_release(payload) if best_manifest is not None else []
    return payload, errors


def render_device_control_release(manifests: list[dict]) -> tuple[dict, list[str]]:
    """device-control-releases/version.json：返回 ``(payload, errors)``。

    与移动端同构但独立 schema/命名：取全局版本号最大的 published manifest，
    只投影 Android APK（设备控制被控端只有 Android，无 iOS）。
    """
    payload: dict = {
        "schema": "ai-lubricant.device-control-release/v1",
        "version": "", "version_notes": "", "assets": [], "updated_at": _now(),
    }
    best_manifest: dict | None = None
    for manifest in manifests:
        if not isinstance(manifest, dict):
            continue
        if manifest.get("status") != "published":
            continue
        version = str(manifest.get("version") or "")
        if best_manifest is None or _version_sort_key(version) > _version_sort_key(str(best_manifest.get("version") or "")):
            best_manifest = manifest

    if best_manifest is not None:
        version = str(best_manifest.get("version") or "")
        assets = []
        for asset in (best_manifest.get("assets") or []):
            if not isinstance(asset, dict):
                continue
            if not identify_device_control_asset(asset.get("filename")):
                continue
            assets.append({**asset, "version": version})
        payload.update({
            "version": version,
            "version_notes": str(best_manifest.get("version_notes") or ""),
            "release_tag": str(best_manifest.get("release_tag") or ""),
            "assets": assets,
        })
    errors = validate_device_control_release(payload) if best_manifest is not None else []
    return payload, errors
