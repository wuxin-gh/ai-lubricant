"""市场路由。

**PG（marketplace_items）是编辑真相源，GitHub/Gitee 仓库是发布镜像。**

- ``router``（公开 ``/status`` + ``/consumer/*``）：常驻挂载。本部署消费端在
  store 已填充时直接读 store（管理端保存即对用户生效）；只读部署 / bootstrap
  完成前的窗口回落旧的 raw 读路径（consumer_cache）。仓库 raw 继续服务外部
  部署，靠各自 TTL 收敛（不再有写后失效的即时性保证，窗口 = TTL）。
- ``admin_router``（``/admin/*``）：复用平台管理员登录（``_require_admin``）。
  写路径全部事务落 store 立即返回，后台 publisher（publisher.py）消费
  marketplace_publish_jobs 把当前状态异步镜像到 GitHub。直改 GitHub 不再是
  权威——采纳外部直改（如 script/publish_channel_templates.py 的产物）需
  ``POST /admin/resync-from-repo``。

仓库地址与写入 token 只在服务端 ``.env`` 配置（``MARKETPLACE_REPO_URL`` /
``MARKETPLACE_GITHUB_TOKEN``），改完重启生效。因此普通部署只填仓库地址即可看市场；
只有市场管理员需要再配 token。
"""
from __future__ import annotations

import datetime as _dt
import asyncio
import hashlib
import json
import os
from functools import wraps
from typing import Any

import aiohttp
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from loguru import logger

from ..deps import get_current_user
from ..git_clients import GitClientError
from ..models import User
from . import config as mp_config
from .github import MarketplaceGitHub
from .source_config import (
    get_source_config_async,
    public_view,
    update_source_config,
)
from .validator import (
    MARKET_EXPORT_SCHEMA,
    MARKET_INDEX_SCHEMA,
    empty_index,
    identify_mobile_asset,
    index_path,
    index_summary,
    item_path,
    normalize_import_body,
    safe_item_id,
    validate_manifest,
)

# 升级链路的仓库投影（渲染归 render.py，写动作归 publisher.py）。
# /consumer/version、/consumer/mobile-version、/consumer/device-control-version
# 继续读 node/mobile/device_control_release_catalog 快照——该快照从仓库 version.json
# 定时同步，天然只反映「已发布」状态。
MOBILE_VERSION_SCHEMA = "ai-lubricant.mobile-version/v1"
DEVICE_CONTROL_VERSION_SCHEMA = "ai-lubricant.device-control-version/v1"

router = APIRouter(prefix="/api/v1/marketplace", tags=["marketplace"])


def _require_market_writable() -> None:
    """admin 路由常驻挂载后的运行时开关：未配 github_token 时保持 404 语义。

    挂载时不再判 writable——依赖读的是请求时的 ``mp_config.settings``，
    改 .env 重启后立即按新配置放行，常驻挂载的部署无需重新部署前端。
    """
    if not mp_config.settings.writable:
        raise HTTPException(status_code=404, detail="marketplace admin not configured")


# 管理接口单独成组，常驻挂载；「是否可写」由上面的依赖在请求时判定（见 mount_routes）。
admin_router = APIRouter(
    prefix="/api/v1/marketplace",
    tags=["marketplace-admin"],
    dependencies=[Depends(_require_market_writable)],
)


async def _require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


def _client() -> MarketplaceGitHub:
    return MarketplaceGitHub(mp_config.settings)


# 发行模块（节点/移动端/设备控制 App 版本）是升级链路核心设施，不随内容模块白名单开关：
# /consumer/version、/consumer/mobile-version、/consumer/device-control-version 与上传
# 发布链路本就常驻可用，索引浏览（历史版本等）也不该因 modules 配置漏列而 400。
_RELEASE_MODULES = frozenset({"node-versions", "mobile-versions", "device-control-versions"})


def _valid_module(module: str) -> bool:
    return module in mp_config.settings.modules or module in _RELEASE_MODULES


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _explain_git_write_error(detail: str) -> str:
    """把 GitHub Contents/Releases 写失败的错误翻译成可执行的人话。

    ``detail`` 是 ``GitClientError`` 的 ``str()``，按设计不含 token，可直接回显。
    """
    if "not accessible by personal access token" in detail or "HTTP 403" in detail:
        return (
            "GitHub token 没有仓库 Contents 写权限。fine-grained PAT 需在 GitHub token "
            "设置里勾选该仓库的 Contents: Read and write；或改用具备 repo 权限的 classic PAT。"
        )
    if "HTTP 404" in detail:
        return "仓库或分支访问不到（404）：检查仓库地址/分支，以及 token 的仓库访问范围。"
    return detail


def _explain_write_failures(failed: list[dict]) -> str:
    """把 ``import_current_channels`` 的 per-item 失败汇总成一条人话。"""
    flat: list[str] = []
    for item in failed:
        flat.extend(str(e) for e in (item.get("errors") or []))
    joined = " | ".join(flat)[:800]
    if not joined:
        return "没有写入任何渠道模板（未知原因）"
    return f"没有写入任何渠道模板：{_explain_git_write_error(joined)}"


def _github_write_guard(action: str):
    """把市场写侧 GitHub 错误统一翻译后返回给前端，避免裸 500。"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except GitClientError as exc:
                raise HTTPException(status_code=502, detail=f"{action}失败：{_explain_git_write_error(str(exc))}")
        return wrapper
    return decorator


async def _read_index(client: MarketplaceGitHub, module: str, *, use_cache: bool = True) -> tuple[dict, str]:
    """读模块索引；新仓库缺文件时返回空索引（不 500）。"""
    got = await client.read_json_or_none(
        index_path(module, mp_config.settings.index_name), use_cache=use_cache
    )
    if got is None:
        return empty_index(module), ""
    data, sha = got
    if not isinstance(data, dict):
        return empty_index(module), sha
    return data, sha


# ── public ──────────────────────────────────────────────────────────────────


@router.get("/status")
async def status() -> dict:
    """消费侧市场坐标；生产侧是否可写只作为独立状态返回。"""
    from . import urls

    settings = mp_config.settings
    return {
        "enabled": settings.enabled,
        "writable": settings.writable,
        "modules": list(settings.modules),
        "owner": settings.github_owner,
        "repo": settings.github_repo,
        "branch": settings.github_branch,
        # 生产侧恒为 GitHub（写侧），展示用 GitHub 网页地址。
        "repo_url": urls.repo_web_url("github", settings.github_owner, settings.github_repo) if settings.enabled else "",
    }


@router.get("/consumer/status")
async def consumer_status() -> dict:
    """用根标识文件校验消费侧仓库，而不是只校验配置字符串。

    marker 走 consumer_cache（``useMarketplaceEnabled`` 在多个页面挂载时高频
    打这里，此前每请求现拉一次 raw）。
    """
    from . import consumer_cache, urls

    c = mp_config.consumer_settings
    result = {
        "enabled": c.enabled,
        "verified": False,
        "modules": list(c.modules),
        "owner": c.github_owner,
        "repo": c.github_repo,
        "branch": c.github_branch,
        "platform": c.platform,
        # 消费侧按平台渲染网页地址（gitee 镜像给 gitee.com 链接）。
        "repo_url": urls.repo_web_url(c.platform, c.github_owner, c.github_repo) if c.enabled else "",
    }
    if not c.enabled:
        return result
    try:
        marker, _stale = await consumer_cache.get_raw("marketplace.json")
    except Exception:
        return result
    if isinstance(marker, dict) and marker.get("schema") == "ai-lubricant.market.v1":
        modules = marker.get("modules")
        if isinstance(modules, list):
            result["modules"] = [str(item) for item in modules if str(item)]
        result["verified"] = True
    return result


# ── consumer: server-proxied reads ──────────────────────────────────────────
#
# 前端不再直读 raw.githubusercontent.com（浏览器走不到服务端代理，国内必断）。改为
# 调这些公开端点，服务端经 proxy_manager 用资源中心配置的 proxy_id 出网，把 raw
# JSON 拉回来加工成页面要的形态返回。读公开仓库 raw 不需要 token；未配 proxy_id 时
# proxy_manager 隐式直连。用户侧与管理侧共用这一套。
#
# 出网读统一走 consumer_cache（定时后台刷新 + 写失效 + 失败保留旧快照），索引与
# marker 由后台循环预热，单条 manifest read-through；3 秒去重缓存仍由前端持有。


@router.get("/consumer/index/{module}")
async def consumer_index(module: str) -> dict:
    """消费侧条目列表：过滤 hidden/deleted/draft，只留 published。

    store 已填充（writable 部署）时直接读 PG——管理端保存即对用户生效，无需等
    仓库发布传播；只读部署 / bootstrap 完成前走 consumer_cache（定时后台刷新 +
    写失效），拉取失败但有旧快照时回旧数据并标 ``stale``；完全没有快照时返回
    空索引，不 500（市场不可用不应打断主功能）。
    """
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    import marketplace_store as store

    if await store.is_populated():
        items = [
            it for it in await store.list_summaries(module)
            if isinstance(it, dict) and it.get("status") == "published"
        ]
        return {
            "schema": MARKET_INDEX_SCHEMA, "module": module,
            "items": items, "updated_at": _now(), "stale": False,
        }
    from . import consumer_cache

    try:
        data, stale = await consumer_cache.get_raw(
            index_path(module, mp_config.consumer_settings.index_name)
        )
    except GitClientError:
        return empty_index(module)
    if not isinstance(data, dict):
        return empty_index(module)
    items = [
        it for it in (data.get("items") or [])
        if isinstance(it, dict) and it.get("status") == "published"
    ]
    return {**data, "module": module, "items": items, "stale": stale}


@router.get("/consumer/item/{module}/{item}")
async def consumer_item(module: str, item: str) -> dict:
    """消费侧单条 manifest。文件不存在返回 404（调用方据此判断「无此条目」）。

    store 已填充时读 PG（保存即生效）；否则走 consumer_cache（read-through +
    写失效），404 也会被缓存，不再重复打网络。
    """
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    safe = safe_item_id(item)
    if not safe:
        raise HTTPException(status_code=400, detail="invalid item id")
    import marketplace_store as store

    if await store.is_populated():
        alt = item.replace(".", "/")
        row = await store.get_item(module, item) or await store.get_item(module, alt)
        if row is None or not isinstance(row.get("manifest"), dict):
            raise HTTPException(status_code=404, detail="item not found")
        return row["manifest"]
    from . import consumer_cache

    try:
        data, _stale = await consumer_cache.get_raw(item_path(module, safe))
    except GitClientError as exc:
        if "HTTP 404" in str(exc):
            raise HTTPException(status_code=404, detail="item not found")
        raise HTTPException(status_code=502, detail=str(exc))
    if data is None:
        raise HTTPException(status_code=404, detail="item not found")
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="manifest is not an object")
    return data


@router.get("/consumer/version")
async def consumer_version() -> dict:
    """节点程序版本信息：返回 version.json（消费侧视图）。

    读 node_release_catalog 的内存快照（refresh 定时同步）；未同步过时现拉一次。
    不暴露 test_version 草稿：publisher 渲染 version.json 时已在写入侧排除。
    """
    import node_release_catalog as nrc

    snapshot = await nrc.get_latest_release()
    if not snapshot.get("version") and not snapshot.get("assets"):
        # 内存空：触发一次同步（不阻塞响应过久——失败就返回当前空快照）。
        try:
            snapshot = await nrc.refresh(publish=False)
        except Exception:
            pass
    return {
        "version": str(snapshot.get("version") or ""),
        "version_notes": str(snapshot.get("version_notes") or ""),
        "release_tag": str(snapshot.get("release_tag") or ""),
        "assets": snapshot.get("assets") or [],
        "updated_at": str(snapshot.get("updated_at") or ""),
        "stale": bool(snapshot.get("stale")),
    }


@router.get("/consumer/mobile-version")
async def consumer_mobile_version() -> dict:
    """移动端（控制 App）版本信息：返回 mobile-releases/version.json（消费侧视图）。

    与节点 ``/consumer/version`` 同构且同样公开：APK 直链本身就是公开的 GitHub
    Release 资产，App 在登录前也要能查更新。Android 拿 assets 里的直链自行下载
    安装；iOS 只用 ``ios`` 块跳商店。``android.proxy_download_url`` 是服务端代理
    下载的站内路径——手机不一定能直连 GitHub，下载应优先走它。
    """
    import mobile_release_catalog as mrc

    snapshot = await mrc.get_latest_release()
    if not snapshot.get("version") and not snapshot.get("assets"):
        try:
            snapshot = await mrc.refresh(publish=False)
        except Exception:
            pass
    android = mrc.select_android_asset(snapshot) or {}
    ios = snapshot.get("ios") if isinstance(snapshot.get("ios"), dict) else {}
    return {
        "version": str(snapshot.get("version") or ""),
        "version_notes": str(snapshot.get("version_notes") or ""),
        "release_tag": str(snapshot.get("release_tag") or ""),
        "android": {
            "version": str(android.get("version") or snapshot.get("version") or ""),
            "download_url": str(android.get("download_url") or ""),
            "proxy_download_url": (
                "/api/v1/marketplace/consumer/download/mobile-android"
                if android.get("download_url") else ""
            ),
            "digest": str(android.get("digest") or ""),
            "size_bytes": int(android.get("size_bytes") or 0),
        },
        "ios": {
            "version": str(ios.get("version") or ""),
            "store_url": str(ios.get("store_url") or ""),
        },
        "updated_at": str(snapshot.get("updated_at") or ""),
        "stale": bool(snapshot.get("stale")),
    }


@router.get("/consumer/device-control-version")
async def consumer_device_control_version() -> dict:
    """设备控制 App（被控端）版本信息：消费侧视图，供「添加设备」弹框拿下载直链。

    与 ``/consumer/mobile-version`` 同构且同样公开（APK/IPA 直链本就公开）。
    Android 为 APK、iOS 为侧载 IPA（不上 App Store，由 iOS 宿主节点安装）；
    下载地址指向独立仓库 ai-lubricant-device-control 的 Release 资产。
    ``android/ios.proxy_download_url`` 是服务端代理下载的站内路径（带平台参数）——
    客户端不一定要能直连 GitHub，优先走它。
    """
    import device_control_release_catalog as dcrc

    snapshot = await dcrc.get_latest_release()
    if not snapshot.get("version") and not snapshot.get("assets"):
        try:
            snapshot = await dcrc.refresh(publish=False)
        except Exception:
            pass

    def _platform_block(platform: str) -> dict:
        asset = dcrc.select_asset(snapshot, platform) or {}
        return {
            "version": str(asset.get("version") or snapshot.get("version") or ""),
            "download_url": str(asset.get("download_url") or ""),
            "proxy_download_url": (
                f"/api/v1/marketplace/consumer/download/device-control/{platform}"
                if asset.get("download_url") else ""
            ),
            "digest": str(asset.get("digest") or ""),
            "size_bytes": int(asset.get("size_bytes") or 0),
        }

    return {
        "version": str(snapshot.get("version") or ""),
        "version_notes": str(snapshot.get("version_notes") or ""),
        "release_tag": str(snapshot.get("release_tag") or ""),
        "android": _platform_block("android"),
        "ios": _platform_block("ios"),
        "updated_at": str(snapshot.get("updated_at") or ""),
        "stale": bool(snapshot.get("stale")),
    }


# ── 资源中心代理下载（/consumer/download/*）──────────────────────────────────
#
# 浏览器/手机未必能直连 GitHub（国内网络），但服务端能（资源中心配置的市场代理）。
# App 下载与被控端 App 下载统一收口到这里：服务端代拉 Release 资产，流式回传。
# URL 全部取自 catalog 快照里发布时落库的直链（非用户输入，无 SSRF 面），端点保持
# 公开——App 在登录前也要能下载。


def _proxy_download_path(module: str) -> str:
    return f"/api/v1/marketplace/consumer/download/{module}"


async def _proxy_stream_release_asset(
    url: str, *, filename: str, media_type: str = "application/vnd.android.package-archive",
    fallback_size: int = 0,
) -> StreamingResponse:
    """服务端经市场代理出网拉取发行资产，流式回传客户端（浏览器/手机）。

    透传上游 Content-Length（缺失时用发布时记录的 size_bytes 兜底，手机下载进度条
    靠它）；Content-Disposition 带 Release 资产文件名。客户端中途断开时生成器被
    关闭，finally 归还上游连接。
    """
    from providers.proxy_manager import get_proxy_manager

    resp = await get_proxy_manager().request(
        url=url,
        method="GET",
        headers={"User-Agent": "ai-lubricant-marketplace"},
        timeout=aiohttp.ClientTimeout(total=1800, sock_read=120),
        proxy_config_id=mp_config.settings.proxy_id or None,
    )
    try:
        if resp.status < 200 or resp.status >= 300:
            raise HTTPException(status_code=502, detail=f"上游下载失败（HTTP {resp.status}）")

        async def stream():
            try:
                async for chunk in resp.iter_any():
                    yield chunk
            finally:
                await resp.__aexit__(None, None, None)

        headers: dict[str, str] = {"Content-Disposition": f'attachment; filename="{filename}"'}
        upstream_len = ""
        for key, value in (resp.headers or {}).items():
            if str(key).lower() == "content-length":
                upstream_len = str(value).strip()
                break
        if upstream_len.isdigit() and int(upstream_len) > 0:
            headers["Content-Length"] = upstream_len
        elif fallback_size > 0:
            headers["Content-Length"] = str(fallback_size)
        return StreamingResponse(
            stream(),
            media_type=media_type,
            headers=headers,
        )
    except BaseException:
        # 拿到上游响应但没流出去（上游 4xx/5xx / 构造响应失败）：先关连接再抛。
        await resp.__aexit__(None, None, None)
        raise


# 设备控制 App 按「设备类型」分发：android→APK / ios→IPA（侧载包）。
# 移动控制端只有 Android，沿用固定 mobile-android 路径。
_DC_PROXY_MEDIA_TYPES = {
    "android": "application/vnd.android.package-archive",
    "ios": "application/octet-stream",
}


async def _proxy_download_mobile_android_apk() -> StreamingResponse:
    """``/consumer/download/mobile-android`` 实现：移动控制端 APK（目前只有 Android）。"""
    import mobile_release_catalog as mrc

    snapshot = await mrc.get_latest_release()
    if not snapshot.get("version") and not snapshot.get("assets"):
        try:
            snapshot = await mrc.refresh(publish=False)
        except Exception:
            pass
    android = mrc.select_android_asset(snapshot) or {}
    url = str(android.get("download_url") or "")
    if not url:
        raise HTTPException(status_code=404, detail="暂未发布 Android 安装包")
    filename = str(android.get("filename") or "").replace('"', "") or f"ai-lubricant-{snapshot.get('version')}-android.apk"
    return await _proxy_stream_release_asset(
        url, filename=filename, media_type="application/vnd.android.package-archive",
        fallback_size=int(android.get("size_bytes") or 0),
    )


async def _proxy_download_device_control(platform: str) -> StreamingResponse:
    """``/consumer/download/device-control/{platform}`` 实现：按设备类型取资产。

    platform=android 取 APK、platform=ios 取 IPA；其余值 404。
    """
    import device_control_release_catalog as dcrc

    if platform not in _DC_PROXY_MEDIA_TYPES:
        raise HTTPException(status_code=404, detail=f"不支持的设备类型：{platform}")
    snapshot = await dcrc.get_latest_release()
    if not snapshot.get("version") and not snapshot.get("assets"):
        try:
            snapshot = await dcrc.refresh(publish=False)
        except Exception:
            pass
    asset = dcrc.select_asset(snapshot, platform) or {}
    url = str(asset.get("download_url") or "")
    if not url:
        label = "Android" if platform == "android" else "iOS"
        raise HTTPException(status_code=404, detail=f"暂未发布 {label} 安装包")
    default_ext = "apk" if platform == "android" else "ipa"
    filename = (
        str(asset.get("filename") or "").replace('"', "")
        or f"device-control-{snapshot.get('version')}-{platform}.{default_ext}"
    )
    return await _proxy_stream_release_asset(
        url, filename=filename, media_type=_DC_PROXY_MEDIA_TYPES[platform],
        fallback_size=int(asset.get("size_bytes") or 0),
    )


@router.get("/consumer/download/mobile-android")
async def consumer_download_mobile_android() -> StreamingResponse:
    """移动控制端（Android）APK 代理下载：与 ``/consumer/mobile-version`` 同源公开。

    App 内「检查更新」与「关于 → 下载最新安装包」都走这里；手机不需要能直连 GitHub。
    """
    return await _proxy_download_mobile_android_apk()


@router.get("/consumer/download/device-control/{platform}")
async def consumer_download_device_control(platform: str) -> StreamingResponse:
    """设备控制 App（被控端）按设备类型代理下载：「添加设备」弹框的下载按钮走这里。

    platform=android 拉 APK、platform=ios 拉 IPA；URL 取自 catalog 快照发布时
    落库的直链（非用户输入，无 SSRF 面），端点公开——添加设备前就要能下载。
    """
    return await _proxy_download_device_control(platform)


@router.get("/consumer/download/device-control-android")
async def consumer_download_device_control_android() -> StreamingResponse:
    """旧路径别名（向已部署客户端兼容）：等价于 ``/device-control/android``。"""
    return await _proxy_download_device_control("android")


# ── source config（仓库源 / 同步策略 / 代理）─────────────────────────────────
#
# 挂在公开 router 而不是 admin_router：admin_router 的端点在未配 token 时被
# _require_market_writable 挡成 404，而“配 token”本身就要走这里，否则第一次
# 部署会陷入鸡生蛋。鉴权仍是平台管理员。


def _effective_view() -> dict:
    settings = mp_config.settings
    c = mp_config.consumer_settings
    return {
        "repo_url": settings.repo_url,
        "owner": settings.github_owner,
        "repo": settings.github_repo,
        "branch": settings.github_branch,
        "modules": list(settings.modules),
        "index_name": settings.index_name,
        "enabled": settings.enabled,
        "writable": settings.writable,
        # 消费侧（读）生效值：切到 Gitee 镜像后，节点/App 下载与元数据读取都从这里走。
        "consumer_repo_url": c.repo_url,
        "consumer_platform": c.platform,
        "consumer_owner": c.github_owner,
        "consumer_repo": c.github_repo,
        "consumer_branch": c.github_branch,
    }


@router.get("/admin/source-config")
async def get_source_config_route(_: User = Depends(_require_admin)) -> dict:
    """市场源配置：DB 原值 + 已生效值。仓库地址看 ``effective``（.env 解析结果）。"""
    source = await get_source_config_async()
    return public_view(source, _effective_view())


@router.put("/admin/source-config")
async def update_source_config_route(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """保存市场源配置并热更新（部分更新，未传的字段不动）。

    接受代理 / 渠道目录同步策略 / 外部榜单同步等字段。``repo_url`` 与
    ``github_token`` 已不再接受：这两项只在服务端 ``.env`` 配置，patch 里出现也忽略。
    保存后立刻 reload，无需重启。
    """
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="配置必须是对象")
    source = await update_source_config(body)
    mp_config.reload_settings()
    # 渠道目录同步策略也来自这份配置：仓库/周期变了就立刻按新仓库重同步一次。
    try:
        from .channel_catalog import refresh

        await refresh()
    except Exception:
        pass
    return {"ok": True, **public_view(source, _effective_view())}


# ── admin: read ───────────────────────────────────────────────────────────────


@admin_router.post("/admin/cleanup-old-sync")
async def cleanup_old_sync(_: User = Depends(_require_admin)) -> dict:
    """清理旧同步路径的垃圾数据：marketplace_store 里的 agency-agents*/agentscope。

    新代码统一写候选池表，旧表数据不再被读取。返回删除统计。
    """
    from server.db import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 清理前统计
        rows = await conn.fetch("""
            SELECT module, publisher, COUNT(*) as cnt
            FROM marketplace_store
            WHERE (module = 'prompts' AND publisher LIKE 'agency-agents%')
               OR (module = 'skills' AND publisher = 'agentscope')
            GROUP BY module, publisher
            ORDER BY module, publisher
        """)
        before = {f"{r['module']}/{r['publisher']}": r["cnt"] for r in rows}
        total_before = sum(before.values())

        # 删除
        result = await conn.execute("""
            DELETE FROM marketplace_store
            WHERE (module = 'prompts' AND publisher LIKE 'agency-agents%')
               OR (module = 'skills' AND publisher = 'agentscope')
        """)
        deleted = int(result.split()[-1])

        return {
            "deleted": deleted,
            "before": before,
            "detail": f"已删除 {deleted} 条旧数据（agency-agents/agentscope 在 marketplace_store 表的残留）",
        }


@admin_router.get("/admin/catalog")
async def catalog(
    module: str = Query(...),
    q: str | None = None,
    kind: str | None = None,
    fresh: bool = Query(False, description="是否绕过服务端 GitHub 读缓存（store 路径下无操作）"),
    _: User = Depends(_require_admin),
) -> dict:
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    import marketplace_store as store

    if await store.is_populated():
        # store 即真相源：管理视图保留 hidden（隐藏可逆），只剔 deleted（行已物理删，
        # 这里天然不存在）。响应附带发布队列计数，前端据此显示「发布中/发布失败」徽标。
        items = [it for it in await store.list_summaries(module) if it.get("status") != "deleted"]
        if kind:
            items = [it for it in items if it.get("kind") == kind]
        if q:
            needle = q.lower()
            items = [it for it in items if needle in str(it).lower()]
        return {
            "schema": MARKET_INDEX_SCHEMA, "module": module, "items": items,
            "updated_at": _now(), "publish": await store.publish_counts(),
        }
    data, _sha = await _read_index(_client(), module, use_cache=not fresh)
    items = data.get("items") if isinstance(data.get("items"), list) else []
    # 管理视图保留 hidden：隐藏是可逆操作，管理员要能看到并改回 published。
    # 消费侧（前端直读 raw）自己过滤 hidden/deleted，与这里无关。
    items = [it for it in items if it.get("status") != "deleted"]
    if kind:
        items = [it for it in items if it.get("kind") == kind]
    if q:
        needle = q.lower()
        items = [it for it in items if needle in str(it).lower()]
    return {**data, "module": module, "items": items}


@admin_router.get("/admin/items/{module}/{item}")
async def get_item(module: str, item: str, _: User = Depends(_require_admin)) -> dict:
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    safe = safe_item_id(item)
    if not safe:
        raise HTTPException(status_code=400, detail="invalid item id")
    import marketplace_store as store

    if await store.is_populated():
        alt = item.replace(".", "/")
        row = await store.get_item(module, item) or await store.get_item(module, alt)
        if row is None:
            raise HTTPException(status_code=404, detail="item not found")
        return row["manifest"]
    try:
        data, _sha = await _client().read_json(item_path(module, safe))
    except GitClientError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return data


@admin_router.get("/admin/export")
async def export(
    module: str | None = None,
    fresh: bool = Query(False, description="是否绕过服务端 GitHub 读缓存（store 路径下无操作）"),
    _: User = Depends(_require_admin),
) -> dict:
    import marketplace_store as store

    if await store.is_populated():
        mods = [module] if module else list(mp_config.settings.modules)
        out: dict[str, Any] = {}
        for mod in mods:
            if not _valid_module(mod):
                raise HTTPException(status_code=400, detail=f"unknown module: {mod}")
            items = await store.list_summaries(mod)
            out[mod] = {
                "index": {"schema": MARKET_INDEX_SCHEMA, "module": mod, "items": items, "updated_at": _now()},
                "manifests": {str(m.get("id")): m for m in await store.list_manifests(mod)},
            }
        return {"schema": MARKET_EXPORT_SCHEMA, "exported_modules": mods, "modules": out}
    client = _client()
    mods = [module] if module else list(mp_config.settings.modules)
    out: dict[str, Any] = {}
    for mod in mods:
        if not _valid_module(mod):
            raise HTTPException(status_code=400, detail=f"unknown module: {mod}")
        index, _sha = await _read_index(client, mod, use_cache=not fresh)
        manifests: dict[str, Any] = {}
        for it in index.get("items", []) or []:
            raw_id = str(it.get("id") or "")
            path = it.get("item_path") or item_path(mod, raw_id.replace("/", "."))
            try:
                data, _s = await client.read_json(path, use_cache=not fresh)
                manifests[raw_id] = data
            except GitClientError:
                pass
        out[mod] = {"index": index, "manifests": manifests}
    return {"schema": MARKET_EXPORT_SCHEMA, "exported_modules": mods, "modules": out}


# ── admin: write ──────────────────────────────────────────────────────────────


@admin_router.post("/admin/leaderboard/add-github")
async def add_github_to_leaderboard(body: dict = Body(...), _: User = Depends(_require_admin)) -> dict:
    """手动添加 GitHub 仓库到候选池（source=manual）。

    body.repo: GitHub 仓库全名（owner/name）
    返回创建的条目 ID。
    """
    from .leaderboard_sync import fetch_repo_metadata, manual_item
    import marketplace_leaderboard_store as store

    repo_full_name = str(body.get("repo") or "").strip()
    if not repo_full_name or "/" not in repo_full_name:
        raise HTTPException(status_code=400, detail="repo 格式应为 owner/name")

    # 检查是否已存在
    existing = await store.find_by_repo(repo_full_name)
    if existing:
        raise HTTPException(status_code=409, detail=f"仓库 {repo_full_name} 已在候选池中（id={existing['id']}）")

    # 拉 GitHub 元数据
    try:
        meta = await fetch_repo_metadata(repo_full_name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"获取仓库元数据失败: {exc}") from exc

    # 组装条目
    item = manual_item(repo_full_name, meta)
    if not item:
        raise HTTPException(status_code=400, detail="无法从元数据生成候选池条目")

    # 写入
    row, created = await store.create_manual_item(item)
    item_id = row["id"] if row else None
    if not item_id:
        raise HTTPException(status_code=500, detail="创建候选池条目失败")

    detail = "已添加到候选池（草稿状态）" if created else f"仓库已在候选池中（id={item_id}）"
    return {"id": item_id, "repo": repo_full_name, "created": created, "detail": detail}


@admin_router.get("/admin/channels/exportable")
async def list_exportable_channels(_: User = Depends(_require_admin)) -> dict:
    """当前平台可发布为模板的渠道清单，供「从当前平台导入」弹框多选。

    只读基础配置（不碰账号/密钥），并标出该渠道是否已在市场发布过对应模板，
    弹框据此默认只勾选未发布的。
    """
    import config
    from db import PostgresClient
    from .channel_template_export import build_manifest

    import marketplace_store as store
    if await store.is_populated():
        published = {str(item.get("id") or "") for item in await store.list_summaries("channels")}
    else:
        index, _sha = await _read_index(_client(), "channels")
        published = {str(item.get("id") or "") for item in (index.get("items") or []) if isinstance(item, dict)}

    items: list[dict] = []
    for name in sorted(await config.Config.get_providers()):
        cfg = await PostgresClient.get_provider_base_config(name)
        if not isinstance(cfg, dict):
            continue
        # 复用导出器算 id，保证弹框里显示的「已发布」判定与真正写入的 id 完全一致。
        manifest = build_manifest(name, cfg, {})
        template_id = str(manifest.get("id") or "")
        items.append({
            "provider": name,
            "display_name": str(cfg.get("remark") or name),
            "builtin_type": str(cfg.get("builtin_type") or cfg.get("type") or "custom"),
            "base_url": str(cfg.get("base_url") or ""),
            "enabled": cfg.get("enabled") is not False,
            "template_id": template_id,
            "published": template_id in published,
        })
    return {"items": items}


@admin_router.post("/admin/channels/import-current")
async def import_current_channels(
    body: dict = Body(default={}), _: User = Depends(_require_admin)
) -> dict:
    """把选中渠道的基础配置发布为市场模板；从不读取账号。

    落 store 立即生效（保存即对用户可见），GitHub 镜像由后台 publisher 异步推送。
    ``body.providers`` 为空/缺省时导出全部（脚本与旧调用方沿用这一行为）。
    """
    import config
    from db import PostgresClient
    from limit_policy_store import get_effective_provider_policy
    from .channel_template_export import build_manifest, resolve_requested_providers

    import marketplace_store as store

    written: list[str] = []
    failed: list[dict] = []
    available = set(await config.Config.get_providers())
    providers, unknown = resolve_requested_providers(
        body.get("providers") if isinstance(body, dict) else None, available,
    )
    if unknown:
        raise HTTPException(status_code=400, detail=f"渠道不存在：{', '.join(unknown)}")
    for name in providers:
        try:
            cfg = await PostgresClient.get_provider_base_config(name)
            if not isinstance(cfg, dict):
                raise ValueError("渠道基础配置不存在")
            policy = await get_effective_provider_policy(name, refresh=True)
            manifest = build_manifest(name, cfg, policy)
            errors = validate_manifest("channels", manifest)
            if errors:
                failed.append({"provider": name, "errors": errors})
                continue
            await store.upsert_item("channels", manifest)
            written.append(str(manifest["id"]))
        except Exception as exc:  # noqa: BLE001 - 逐条记下，不中断其余渠道
            failed.append({"provider": name, "errors": [str(exc)]})

    summaries = await store.list_summaries("channels")
    publish = await store.publish_counts()
    if not written:
        return {
            "written": [], "failed": failed, "index_count": len(summaries),
            "error": _explain_write_failures(failed), "publish": publish,
        }
    return {
        "written": written, "failed": failed,
        "index_count": len(summaries), "publish": publish,
    }


async def _run_channel_import_job(job_id: str, providers: list[str], overwrite: bool) -> None:
    """后台顺序导入渠道模板并持续更新内存 job 状态。

    写的是 PG 编辑真相源（每条一个事务，本地立即生效），GitHub 镜像由 publisher
    异步推送。因此这里不再有 finalizing_index / finalizing_marker 这类「等仓库」
    阶段，也不再需要 ``apply_authoritative_manifests``——store 写完即是权威。
    """
    import config
    from db import PostgresClient
    from limit_policy_store import get_effective_provider_policy
    from . import channel_import_jobs
    from .channel_template_export import build_manifest

    import marketplace_store as store

    try:
        channel_import_jobs.set_phase(job_id, "importing")
        existing = {row["item_id"] for row in await store.list_summaries("channels")}
        for name in providers:
            channel_import_jobs.set_item(job_id, name, "importing")
            try:
                cfg = await PostgresClient.get_provider_base_config(name)
                if not isinstance(cfg, dict):
                    raise ValueError("渠道基础配置不存在")
                policy = await get_effective_provider_policy(name, refresh=True)
                manifest = build_manifest(name, cfg, policy)
                template_id = str(manifest.get("id") or "")
                errors = validate_manifest("channels", manifest)
                if errors:
                    raise ValueError("；".join(errors))
                if template_id in existing and not overwrite:
                    channel_import_jobs.set_item(job_id, name, "skipped", template_id=template_id)
                    continue
                if not safe_item_id(template_id.replace("/", ".")):
                    raise ValueError("模板 id 非法")
                await store.upsert_item("channels", manifest)
                existing.add(template_id)
                channel_import_jobs.set_item(job_id, name, "written", template_id=template_id)
            except Exception as exc:  # noqa: BLE001
                channel_import_jobs.set_item(job_id, name, "failed", error=str(exc))

        if channel_import_jobs.get_job(job_id) is None:
            return
        channel_import_jobs.mark_done(job_id, warning="")
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        channel_import_jobs.mark_failed(job_id, str(exc))


@admin_router.post("/admin/channels/import-jobs", status_code=202)
async def start_channel_import_job(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """启动渠道模板后台导入；job API 要求显式非空选择，绝不把空数组解释为全部。"""
    import config
    from . import channel_import_jobs
    from .channel_template_export import resolve_requested_providers

    raw = body.get("providers") if isinstance(body, dict) else None
    if not isinstance(raw, list) or not raw:
        raise HTTPException(status_code=400, detail="providers 必须是非空数组，请明确选择要导入的渠道")
    available = set(await config.Config.get_providers())
    providers, unknown = resolve_requested_providers(raw, available)
    if unknown:
        raise HTTPException(status_code=400, detail=f"渠道不存在：{', '.join(unknown)}")
    if not providers:
        raise HTTPException(status_code=400, detail="请至少选择一个渠道")
    job_id, active_job_id = channel_import_jobs.create_job(
        providers, overwrite=bool(body.get("overwrite", False)),
    )
    if not job_id:
        raise HTTPException(
            status_code=409,
            detail={"error": "已有渠道模板导入任务正在执行", "active_job_id": active_job_id},
        )
    task = asyncio.create_task(
        _run_channel_import_job(job_id, providers, bool(body.get("overwrite", False))),
        name=f"channel-import-{job_id}",
    )
    channel_import_jobs.set_task(job_id, task)
    return {"job_id": job_id, "status": "queued", "total": len(providers)}


@admin_router.get("/admin/channels/import-jobs/{job_id}")
async def get_channel_import_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    from . import channel_import_jobs
    job = channel_import_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="导入任务不存在、已过期或服务已重启")
    return channel_import_jobs.public_state(job)


@admin_router.post("/admin/channels/batch-delete")
async def batch_delete_channel_templates(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """批量硬删除渠道模板：manifest 顺序删，channels index 只重写一次。"""
    raw_ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="ids 必须是非空数组")
    requested: list[str] = []
    for raw in raw_ids:
        item_id = str(raw or "").strip()
        if item_id and item_id not in requested:
            requested.append(item_id)
    if not requested:
        raise HTTPException(status_code=400, detail="请至少选择一个渠道模板")

    import marketplace_store as store

    deleted: list[str] = []
    failed: list[dict] = []
    results: list[dict] = []
    warnings: list[str] = []

    summaries = await store.list_summaries("channels")
    by_id = {str(row.get("id") or ""): row for row in summaries}
    for requested_id in requested:
        canonical = requested_id if requested_id in by_id else requested_id.replace(".", "/")
        if canonical not in by_id:
            error = "模板不在渠道目录中"
            failed.append({"id": requested_id, "error": error, "phase": "lookup"})
            results.append({"id": requested_id, "ok": False, "error": error, "phase": "lookup"})
            continue
        if not safe_item_id(canonical.replace("/", ".")):
            error = "模板 id 非法"
            failed.append({"id": canonical, "error": error, "phase": "validate"})
            results.append({"id": canonical, "ok": False, "error": error, "phase": "validate"})
            continue
        try:
            await store.delete_item("channels", canonical, hard=True)
            deleted.append(canonical)
            results.append({"id": canonical, "ok": True})
        except Exception as exc:  # noqa: BLE001 - 逐条记错，不中断其余删除
            error = str(exc)
            failed.append({"id": canonical, "error": error, "phase": "store"})
            results.append({"id": canonical, "ok": False, "error": error, "phase": "store"})

    # index_updated / marker_updated / catalog_refreshed 是发布镜像的三件事，现在都由
    # publisher 异步完成。删除一旦落 PG 就已生效（本地消费侧立即可见），因此这里按
    # 「已受理」返回 true，仓库侧进展看 publish 计数与 /admin/publish-jobs。
    return {
        "ok": not failed,
        "requested": len(requested), "deleted": deleted, "failed": failed,
        "results": results, "count": len(deleted), "index_updated": bool(deleted),
        "marker_updated": bool(deleted), "catalog_refreshed": bool(deleted),
        "warnings": warnings,
        "publish": await store.publish_counts(),
    }


@admin_router.post("/admin/node-versions/upload")
@_github_write_guard("上传节点版本")
async def upload_node_version(
    version: str = Form(...),
    version_notes: str = Form(...),
    status: str = Form("published"),
    test_version: bool = Form(False),
    files: list[UploadFile] = File(...),
    _: User = Depends(_require_admin),
) -> dict:
    """第①阶段：收文件、识别、暂存临时目录、起后台第②阶段写入市场仓库。

    立即返回 ``{job_id}``，前端轮询 ``/admin/node-versions/upload/{job_id}`` 看进度。
    """
    version = version.strip()
    status = status.strip() or "published"
    if not version:
        raise HTTPException(status_code=400, detail="version 必填")
    if status not in ("draft", "published"):
        raise HTTPException(status_code=400, detail="status 必须为 draft 或 published")
    if not files:
        raise HTTPException(status_code=400, detail="至少上传一个节点发行文件")

    from .validator import identify_node_asset, _is_node_version
    if not _is_node_version(version):
        raise HTTPException(status_code=400, detail="version 必须是日期后缀格式（YYYYMMDD-HHMM）或 semver（1.2.3）")

    from . import upload_jobs

    # GitHub Release 资产单文件上限 2GB（新架构不再入仓，git blob 100MB 上限不适用）。
    max_bytes = 2 * 1024 * 1024 * 1024
    files_meta: list[dict] = []
    seen: set[tuple] = set()
    job_id = upload_jobs.create_job(version, [])
    tmpdir = upload_jobs.tmp_dir(job_id)
    os.makedirs(tmpdir, exist_ok=True)

    received = 0
    for upload in files:
        filename = (upload.filename or "").strip()
        ident = identify_node_asset(filename)
        if not ident:
            upload_jobs.cleanup_job(job_id)
            raise HTTPException(status_code=400, detail=f"无法识别文件「{filename or '(空)'}」；文件名必须符合节点发行产物规范")
        key = (ident["role"], ident["platform"], ident["arch"])
        if key in seen:
            upload_jobs.cleanup_job(job_id)
            raise HTTPException(status_code=400, detail=f"重复资产「{filename}」（role/platform/arch 相同）")
        seen.add(key)
        out_path = upload_jobs.tmp_path_for(job_id, filename)
        size = 0
        with open(out_path, "wb") as fh:
            while chunk := await upload.read(1024 * 1024):
                size += len(chunk)
                if size > max_bytes:
                    upload_jobs.cleanup_job(job_id)
                    raise HTTPException(status_code=413, detail=f"单文件不能超过 2GB（GitHub Release 资产上限）：{filename}")
                fh.write(chunk)
                received += len(chunk)
                upload_jobs.set_received(job_id, received)
        if size == 0:
            upload_jobs.cleanup_job(job_id)
            raise HTTPException(status_code=400, detail=f"上传文件为空：{filename}")
        files_meta.append({
            "filename": filename, "role": ident["role"], "platform": ident["platform"],
            "arch": ident["arch"], "size": size, "tmp_path": out_path,
            "content_type": upload.content_type or "application/octet-stream",
        })

    # 回填文件元信息到已创建的 job（同步 total_files，否则进度条分母恒 0）。
    job = upload_jobs.get_job(job_id)
    upload_jobs.set_files(job_id, files_meta)
    # 暂存版本说明/状态/测试标记，第②阶段写 manifest 时用。
    job["version_notes"] = version_notes
    job["status_form"] = status
    job["test_version"] = bool(test_version)

    upload_jobs.mark_to_github(job_id)
    _start_github_transfer(job_id)
    return {"job_id": job_id}


async def _run_github_transfer(job_id: str) -> None:
    """第②阶段后台任务：把发行资产发布到独立仓库 GitHub Release，manifest 落 store。

    新架构：二进制不再入 marketplace 仓库（git blob），而是上传到独立仓库
    ai-lubricant-nodes 的 GitHub Release（tag ``node-v{version}``）。manifest asset
    记 ``download_url``（Release 资产直链）；marketplace 仓库只留 JSON（item /
    index / version.json 由 publisher 从 store 投影），不再存任何二进制。
    """
    from . import upload_jobs
    from . import github as mp_github
    from . import release_repos
    import marketplace_store as store

    job = upload_jobs.get_job(job_id)
    if job is None:
        return
    version = job["version"]
    tag = f"node-v{version}"
    version_notes = job.get("version_notes") or ""
    status = job.get("status_form") or "published"
    test_version = bool(job.get("test_version"))

    try:
        new_assets: list[dict] = []
        # 上一轮已成功的资产（重试时读回，避免重复传）。真相源是 store；store 里没有
        # （旧部署 / bootstrap 前）才回落读 marketplace 仓库里的 release manifest（旧架构）。
        item_id = f"node-suite-{version}"
        prior_assets: list[dict] = []
        prior_row = await store.get_item("node-versions", item_id)
        if prior_row and isinstance(prior_row.get("manifest"), dict):
            prior_assets = [a for a in (prior_row["manifest"].get("assets") or []) if isinstance(a, dict)]
        if not prior_assets:
            existing_manifest = await _client().read_json_or_none(f"node-releases/{version}/manifest.json")
            if existing_manifest and isinstance(existing_manifest[0], dict):
                prior_assets = [a for a in (existing_manifest[0].get("assets") or []) if isinstance(a, dict)]
        prior_by_key = {(a.get("role"), a.get("platform"), a.get("arch")): a for a in prior_assets}

        release_repo = release_repos.load_node_release_repo()
        if not release_repo.enabled:
            raise RuntimeError("节点发行仓库未配置（NODE_RELEASE_REPO_URL 与 token）")
        # 已存在则复用（支持补传/重试），否则创建。test/draft 用 prerelease 标记而非
        # draft：draft Release 的资产直链未公开，会拿到打不开的 download_url。
        release_info = await mp_github.get_release_by_tag(
            release_repo.github_owner, release_repo.github_repo, release_repo.github_token, tag,
        ) or await mp_github.create_release(
            owner=release_repo.github_owner,
            repo=release_repo.github_repo,
            token=release_repo.github_token,
            tag=tag,
            name=f"节点程序套件 v{version}",
            body=version_notes,
            prerelease=(test_version or status == "draft"),
        )

        for f in job["files"]:
            if f["state"] == "done":
                # 已成功的重试场景：优先沿用 store/release manifest 的 asset；如果上轮
                # 恰好在资产传完、store 提交前失败，两边都没有 manifest，就从 Release
                # 现有资产取回直链（临时目录文件重算 digest/大小校验一致性）。
                pa = prior_by_key.get((f["role"], f["platform"], f["arch"]))
                if pa is None and os.path.isfile(f.get("tmp_path") or ""):
                    with open(f["tmp_path"], "rb") as fh:
                        prior_data = fh.read()
                    existing_asset = _find_release_asset(release_info, f["filename"])
                    if existing_asset:
                        pa = _node_release_asset(
                            f, existing_asset,
                            "sha256:" + hashlib.sha256(prior_data).hexdigest(),
                            int(f.get("size") or len(prior_data)),
                        )
                if pa:
                    new_assets.append(pa)
                continue
            f["state"] = "uploading"
            upload_jobs.set_file_state(job_id, f["filename"], "uploading")
            try:
                with open(f["tmp_path"], "rb") as fh:
                    data = fh.read()
                asset_resp = await mp_github.upload_release_asset(
                    upload_url=release_info["upload_url"],
                    token=release_repo.github_token,
                    filename=f["filename"],
                    content=data,
                    content_type=f.get("content_type") or "application/octet-stream",
                )
                new_assets.append(_node_release_asset(
                    f, asset_resp, "sha256:" + hashlib.sha256(data).hexdigest(), f["size"],
                ))
                upload_jobs.set_file_state(job_id, f["filename"], "done")
            except Exception as exc:  # noqa: BLE001
                upload_jobs.set_file_state(job_id, f["filename"], "failed", str(exc))
                continue

        failed = [f for f in upload_jobs.get_job(job_id)["files"] if f["state"] == "failed"]
        if failed:
            upload_jobs.mark_failed(job_id, f"{len(failed)} 个文件发布失败")
            return

        # 全部成功：finalizing —— manifest 落 store（编辑真相源），marketplace 仓库
        # 镜像（item 文件 / index / marker / version.json）由 publisher 异步推送。
        job["phase"] = "finalizing"
        by_key = {(a.get("role"), a.get("platform"), a.get("arch")): a for a in prior_assets}
        for asset in new_assets:
            by_key[(asset["role"], asset["platform"], asset["arch"])] = asset
        merged_assets = list(by_key.values())
        manifest = {
            "schema": "ai-lubricant.node-version/v1", "id": item_id, "kind": "node_program_version",
            "name": "node-suite", "display_name": f"节点程序套件 {version}", "version": version,
            "version_notes": version_notes, "status": status, "test_version": test_version,
            "assets": merged_assets, "release_tag": tag,
            "category": "node-suite", "tags": ["node", "agent-compose"],
        }
        errors = validate_manifest("node-versions", manifest)
        if errors:
            upload_jobs.mark_failed(job_id, "manifest 校验失败: " + "; ".join(errors))
            return
        await store.upsert_item("node-versions", manifest)
        upload_jobs.mark_done(job_id)
        upload_jobs.cleanup_job(job_id)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        upload_jobs.mark_failed(job_id, f"转传任务异常: {exc}")


def _component_for_role(role: str) -> str:
    return "agent-compose" if role == "runtime" else "node"


def _node_release_asset(f: dict, asset_resp: dict, digest: str, size: int) -> dict:
    """节点文件模式的 asset 形态：记 GitHub Release 资产直链 ``download_url``。"""
    return {
        "filename": f["filename"], "component": _component_for_role(f["role"]),
        "role": f["role"], "platform": f["platform"], "arch": f["arch"],
        "format": "tar.gz" if f["role"] == "runtime" else "executable",
        "download_url": str(asset_resp.get("browser_download_url") or ""),
        "digest": digest,
        "size_bytes": int(asset_resp.get("size") or size),
    }


def _start_github_transfer(job_id: str) -> None:
    from . import upload_jobs
    old = upload_jobs.get_task(job_id)
    if old is not None and not old.done():
        old.cancel()
    task = asyncio.create_task(_run_github_transfer(job_id), name=f"node-upload-{job_id}")
    upload_jobs.set_task(job_id, task)


@admin_router.get("/admin/node-versions/upload/{job_id}")
async def get_upload_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    """轮询上传进度。"""
    from . import upload_jobs
    job = upload_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="上传任务不存在或已过期")
    return upload_jobs.public_state(job)


@admin_router.post("/admin/node-versions/upload/{job_id}/retry")
async def retry_upload_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    """重试：把 failed 文件标回 pending，重启第②阶段后台任务。已 done 的不重传。"""
    from . import upload_jobs
    job = upload_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="上传任务不存在或已过期")
    if not upload_jobs.reset_failed_for_retry(job_id):
        raise HTTPException(status_code=400, detail="没有可重试的失败文件")
    _start_github_transfer(job_id)
    return {"job_id": job_id, "retried": True}


# ── 移动端（控制 App）版本发布 ───────────────────────────────────────────────
#
# 与节点版本上传同构、复用同一套 upload_jobs 两阶段骨架，只是产物更简单：
# 一个 Android APK（iOS 走 App Store，只在表单里填商店链接）。


@admin_router.post("/admin/mobile-versions/upload")
@_github_write_guard("上传移动端版本")
async def upload_mobile_version(
    version: str = Form(...),
    version_notes: str = Form(...),
    status: str = Form("published"),
    ios_store_url: str = Form(""),
    files: list[UploadFile] | None = File(None),
    download_url: str = Form(""),
    _: User = Depends(_require_admin),
) -> dict:
    """第①阶段：收 APK 或外链地址、识别、暂存，起后台第②阶段。

    两种模式二选一：
    - 文件模式：拖 APK 进来，第②阶段把二进制发布到独立仓库 ai-lubricant-mobile 的
      GitHub Release（tag ``mobile-v<version>``），asset 记 ``download_url``（Release
      资产直链，上限 2GB）。
    - URL 模式：填外部下载地址，APK 由管理员自行托管；第②阶段只下载校验 sha256，
      manifest 记 ``download_url``（不入仓/Release），适合已托管的大 APK。

    立即返回 ``{job_id}``，前端轮询 ``/admin/mobile-versions/upload/{job_id}``。
    """
    version = version.strip()
    status = status.strip() or "published"
    ios_store_url = ios_store_url.strip()
    download_url = download_url.strip()
    if not version:
        raise HTTPException(status_code=400, detail="version 必填")
    if status not in ("draft", "published"):
        raise HTTPException(status_code=400, detail="status 必须为 draft 或 published")

    from .validator import _is_mobile_version, is_https_url
    if not _is_mobile_version(version):
        raise HTTPException(status_code=400, detail="version 必须是数字/日期串（如 260608）或 semver（1.2.3）")
    if ios_store_url and not is_https_url(ios_store_url):
        raise HTTPException(status_code=400, detail="iOS 商店链接必须是 HTTPS URL")

    from . import upload_jobs

    has_files = bool(files)
    has_url = bool(download_url)
    if has_files and has_url:
        raise HTTPException(status_code=400, detail="不能同时上传 APK 文件与填写外部下载地址，请二选一")
    if not has_files and not has_url:
        raise HTTPException(status_code=400, detail="必须上传 APK 文件，或填写外部 APK 下载地址")

    if has_url:
        if not is_https_url(download_url):
            raise HTTPException(status_code=400, detail="APK 下载地址必须是 HTTPS URL")
        # 与校验器同口径：禁止带 token/sig 等可能含凭据的 query 字段入库。
        from .validator import _FORBIDDEN_QUERY_KEYS, _url_query
        leaked = [k for k in _url_query(download_url) if any(n in k.lower() for n in _FORBIDDEN_QUERY_KEYS)]
        if leaked:
            raise HTTPException(
                status_code=400,
                detail=f"下载地址不得携带可能含 token 的 query 字段: {sorted(set(leaked))}",
            )
        # URL basename 若符合命名契约，其内嵌版本必须与表单一致（同文件模式口径）。
        from urllib.parse import unquote
        basename = unquote(download_url.rstrip("/").rsplit("/", 1)[-1]) if "/" in download_url else ""
        url_ident = identify_mobile_asset(basename) if basename else {}
        if url_ident and url_ident["version"] != version:
            raise HTTPException(
                status_code=400,
                detail=f"下载地址文件名版本「{url_ident['version']}」与填写的版本「{version}」不一致",
            )

    # 文件模式上限 = GitHub Release 资产 2GB（新架构不入仓，blob 100MB 上限不适用）。
    # URL 模式另有 2GB 下载上限。
    max_bytes = 2 * 1024 * 1024 * 1024
    files_meta: list[dict] = []
    seen: set[str] = set()
    job_id = upload_jobs.create_job(version, [])

    if has_url:
        # URL 模式：第①阶段不收文件，只记一条伪文件条目；第②阶段下载到 tmp_path 再算 hash。
        files_meta.append({
            "filename": f"ai-lubricant-{version}-android.apk",
            "platform": "android", "format": "apk",
            "role": "mobile-app", "arch": "universal",
            "size": 0, "tmp_path": "",
            "content_type": "application/vnd.android.package-archive",
            "source": "url", "download_url": download_url,
        })
    else:
        tmpdir = upload_jobs.tmp_dir(job_id)
        os.makedirs(tmpdir, exist_ok=True)
        for upload in files:
            filename = (upload.filename or "").strip()
            ident = identify_mobile_asset(filename)
            if not ident:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(
                    status_code=400,
                    detail=f"无法识别文件「{filename or '(空)'}」；文件名必须为 ai-lubricant-<版本>-android.apk",
                )
            # 文件名里的版本号必须与表单一致，否则 App 装上后自报版本与发布版本对不上。
            if ident["version"] != version:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(
                    status_code=400,
                    detail=f"文件名版本「{ident['version']}」与填写的版本「{version}」不一致",
                )
            if ident["platform"] in seen:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(status_code=400, detail=f"重复资产「{filename}」（platform 相同）")
            seen.add(ident["platform"])
            out_path = upload_jobs.tmp_path_for(job_id, filename)
            size = 0
            with open(out_path, "wb") as fh:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        upload_jobs.cleanup_job(job_id)
                        raise HTTPException(status_code=413, detail=f"文件过大（>2GB，GitHub Release 资产上限）：{filename}")
                    fh.write(chunk)
            if size == 0:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(status_code=400, detail=f"上传文件为空：{filename}")
            files_meta.append({
                "filename": filename, "platform": ident["platform"], "format": ident["format"],
                # upload_jobs 的进度视图按 role/platform/arch 展示，这里填成移动端口径的占位值。
                "role": "mobile-app", "arch": "universal",
                "size": size, "tmp_path": out_path,
                "content_type": upload.content_type or "application/vnd.android.package-archive",
            })

    job = upload_jobs.get_job(job_id)
    upload_jobs.set_files(job_id, files_meta)
    job["version_notes"] = version_notes
    job["status_form"] = status
    job["ios_store_url"] = ios_store_url

    upload_jobs.mark_to_github(job_id)
    _start_mobile_github_transfer(job_id)
    return {"job_id": job_id}


# APK 外链下载上限：APK 实际几十 MB，2GB 留足余量；超过即中断（防恶意/错配 URL
# 把无限流灌进临时盘）。流式写盘 + sha256，不占内存。
_MOBILE_URL_MAX_BYTES = 2 * 1024 * 1024 * 1024
# APK 是 ZIP 容器，首 4 字节必为本地文件头魔数。
_ZIP_LOCAL_HEADER_MAGIC = b"PK\x03\x04"


def _mobile_url_asset(f: dict, digest: str, size: int) -> dict:
    """URL 模式的 asset 形态：只记 download_url，无 repo_path（外链不入仓）。"""
    return {
        "filename": f["filename"], "platform": f["platform"], "format": f["format"],
        "download_url": f["download_url"],
        "digest": digest,
        "size_bytes": size,
    }


async def _fetch_url_to_file(
    url: str,
    dest_path: str,
    *,
    proxy_config_id: str | None,
    max_bytes: int = _MOBILE_URL_MAX_BYTES,
    timeout_seconds: float = 600.0,
) -> tuple[int, str]:
    """流式下载外链 APK 到 ``dest_path``，边下边算 sha256，返回 ``(字节数, hex digest)``。

    经资源中心配置的市场代理出网（与 github.py 同口径）；HTTPS 强制由调用方保证。
    嗅探首 4 字节必须是 ZIP 本地文件头魔数（APK 即 ZIP），否则报「不是有效 APK」。
    超过 ``max_bytes`` 立即中断并报错。

    SSRF 取舍：服务端会主动 GET 管理员填写的 URL（可能指向内网对象存储/MinIO），
    缓解 = 仅 ``_require_admin`` 可用 + HTTPS 强制 + 禁 token 类 query 字段，不禁私网。
    """
    from providers.proxy_manager import get_proxy_manager

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    magic_checked = False
    magic_prefix = bytearray()
    resp = await get_proxy_manager().request(
        url=url,
        method="GET",
        headers={"User-Agent": "ai-lubricant-marketplace"},
        timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        proxy_config_id=proxy_config_id,
    )
    try:
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"下载失败（HTTP {resp.status}）")
        with open(dest_path, "wb") as fh:
            async for chunk in resp.iter_any():
                if not magic_checked:
                    # APK 是 ZIP 容器，文件头必为 PK\x03\x04。网络层可能把魔数拆进
                    # 多个 chunk，攒够 4 字节再判；所有 chunk 照常写盘，不因嗅探丢字节。
                    magic_prefix.extend(chunk[: max(0, 4 - len(magic_prefix))])
                    if len(magic_prefix) >= 4:
                        if bytes(magic_prefix) != _ZIP_LOCAL_HEADER_MAGIC:
                            raise RuntimeError("下载内容不是有效的 APK 文件（缺少 ZIP 头）")
                        magic_checked = True
                fh.write(chunk)
                digest.update(chunk)
                size += len(chunk)
                if size > max_bytes:
                    raise RuntimeError(f"下载内容超过 {max_bytes // (1024 * 1024)}MB 上限")
        if not magic_checked:
            raise RuntimeError("下载内容不是有效的 APK 文件（缺少 ZIP 头）")
        return size, digest.hexdigest()
    finally:
        await resp.__aexit__(None, None, None)


async def _run_mobile_github_transfer(job_id: str) -> None:
    """第②阶段后台任务：把 APK 发布到独立仓库 GitHub Release（或记外链），manifest 落 store。

    文件模式：APK 上传到独立仓库 ai-lubricant-mobile 的 GitHub Release
    （tag ``mobile-v<version>``），asset 记 ``download_url``（Release 资产直链）。
    URL 模式：下载外链校验 sha256，asset 只记 ``download_url``。
    两种模式都只把 manifest 落 store，marketplace 仓库镜像由 publisher 异步推送。
    """
    from . import upload_jobs
    from . import github as mp_github
    from . import release_repos
    import marketplace_store as store

    job = upload_jobs.get_job(job_id)
    if job is None:
        return
    version = job["version"]
    tag = f"mobile-v{version}"
    version_notes = job.get("version_notes") or ""
    status = job.get("status_form") or "published"
    ios_store_url = job.get("ios_store_url") or ""

    try:
        new_assets: list[dict] = []
        item_id = f"mobile-{version}"
        # 与节点侧同口径：prior assets 先读 store，store 没有再回落 marketplace 仓库
        # release manifest（旧架构入仓时代）。
        prior_assets: list[dict] = []
        prior_row = await store.get_item("mobile-versions", item_id)
        if prior_row and isinstance(prior_row.get("manifest"), dict):
            prior_assets = [a for a in (prior_row["manifest"].get("assets") or []) if isinstance(a, dict)]
        if not prior_assets:
            existing_manifest = await _client().read_json_or_none(f"mobile-releases/{version}/manifest.json")
            if existing_manifest and isinstance(existing_manifest[0], dict):
                prior_assets = [a for a in (existing_manifest[0].get("assets") or []) if isinstance(a, dict)]
        prior_by_key = {a.get("platform"): a for a in prior_assets}

        release_repo = release_repos.load_mobile_release_repo()
        # 文件模式才需要出网创建 Release；URL 模式只下载校验，不碰 GitHub Release。
        release_info: dict | None = None
        if any(f.get("source") != "url" for f in job["files"]):
            if not release_repo.enabled:
                raise RuntimeError("移动端发行仓库未配置（MOBILE_RELEASE_REPO_URL 与 token）")
            # 已存在则复用（支持补传/重试）；draft 用 prerelease 标记而非 draft：
            # draft Release 的资产直链未公开，会拿到打不开的 download_url。
            release_info = await mp_github.get_release_by_tag(
                release_repo.github_owner, release_repo.github_repo, release_repo.github_token, tag,
            ) or await mp_github.create_release(
                owner=release_repo.github_owner,
                repo=release_repo.github_repo,
                token=release_repo.github_token,
                tag=tag,
                name=f"移动控制端 v{version}",
                body=version_notes,
                prerelease=(status == "draft"),
            )

        for f in job["files"]:
            if f["state"] == "done":
                # 与节点侧同口径：prior 缺失（上轮在 store 提交前失败）时从 Release
                # 现有资产取回直链，或从暂存 APK 重算（URL 模式）。
                pa = prior_by_key.get(f.get("platform"))
                if pa is None and os.path.isfile(f.get("tmp_path") or ""):
                    with open(f["tmp_path"], "rb") as fh:
                        prior_data = fh.read()
                    prior_digest = "sha256:" + hashlib.sha256(prior_data).hexdigest()
                    prior_size = int(f.get("size") or len(prior_data))
                    if f.get("source") == "url":
                        pa = _mobile_url_asset(f, prior_digest, prior_size)
                    else:
                        existing_asset = _find_release_asset(release_info or {}, f["filename"])
                        if existing_asset:
                            pa = _mobile_release_asset(f, existing_asset, prior_digest, prior_size)
                if pa:
                    new_assets.append(pa)
                continue
            f["state"] = "uploading"
            upload_jobs.set_file_state(job_id, f["filename"], "uploading")
            try:
                if f.get("source") == "url":
                    # URL 模式：流式下载外链 APK 到 tmp 目录，算 sha256 与大小；不入 Release。
                    tmp_path = upload_jobs.tmp_path_for(job_id, f["filename"])
                    f["tmp_path"] = tmp_path
                    size, digest_hex = await _fetch_url_to_file(
                        f["download_url"], tmp_path,
                        proxy_config_id=mp_config.settings.proxy_id or None,
                    )
                    f["size"] = size
                    new_assets.append(_mobile_url_asset(f, "sha256:" + digest_hex, size))
                else:
                    with open(f["tmp_path"], "rb") as fh:
                        data = fh.read()
                    asset_resp = await mp_github.upload_release_asset(
                        upload_url=release_info["upload_url"],
                        token=release_repo.github_token,
                        filename=f["filename"],
                        content=data,
                        content_type=f.get("content_type") or "application/vnd.android.package-archive",
                    )
                    new_assets.append(_mobile_release_asset(
                        f, asset_resp, "sha256:" + hashlib.sha256(data).hexdigest(), f["size"],
                    ))
                upload_jobs.set_file_state(job_id, f["filename"], "done")
            except Exception as exc:  # noqa: BLE001
                logger.warning("[marketplace] mobile-versions asset {} failed: {}", f["filename"], exc)
                upload_jobs.set_file_state(job_id, f["filename"], "failed", str(exc))
                continue

        failed = [f for f in upload_jobs.get_job(job_id)["files"] if f["state"] == "failed"]
        if failed:
            upload_jobs.mark_failed(job_id, f"{len(failed)} 个文件发布失败")
            return

        # 与节点侧同构：manifest 落 store（编辑真相源），marketplace 仓库镜像
        # （item 文件 / index / marker / version.json）由 publisher 异步推送。
        job["phase"] = "finalizing"
        by_key = {a.get("platform"): a for a in prior_assets}
        for asset in new_assets:
            by_key[asset["platform"]] = asset
        merged_assets = list(by_key.values())
        manifest = {
            "schema": MOBILE_VERSION_SCHEMA, "id": item_id, "kind": "mobile_app_version",
            "name": "ai-lubricant-mobile", "display_name": f"移动控制端 {version}", "version": version,
            "version_notes": version_notes, "status": status,
            "assets": merged_assets, "release_tag": tag, "ios_store_url": ios_store_url,
            "category": "mobile-app", "tags": ["mobile", "android"],
        }
        errors = validate_manifest("mobile-versions", manifest)
        if errors:
            upload_jobs.mark_failed(job_id, "manifest 校验失败: " + "; ".join(errors))
            return
        await store.upsert_item("mobile-versions", manifest)
        upload_jobs.mark_done(job_id)
        upload_jobs.cleanup_job(job_id)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        upload_jobs.mark_failed(job_id, f"转传任务异常: {exc}")


def _mobile_release_asset(f: dict, asset_resp: dict, digest: str, size: int) -> dict:
    """移动端文件模式的 asset 形态：记 GitHub Release 资产直链 ``download_url``。"""
    return {
        "filename": f["filename"], "platform": f["platform"], "format": f["format"],
        "download_url": str(asset_resp.get("browser_download_url") or ""),
        "digest": digest,
        "size_bytes": int(asset_resp.get("size") or size),
    }


def _start_mobile_github_transfer(job_id: str) -> None:
    from . import upload_jobs
    old = upload_jobs.get_task(job_id)
    if old is not None and not old.done():
        old.cancel()
    task = asyncio.create_task(_run_mobile_github_transfer(job_id), name=f"mobile-upload-{job_id}")
    upload_jobs.set_task(job_id, task)


@admin_router.get("/admin/mobile-versions/upload/{job_id}")
async def get_mobile_upload_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    """轮询移动端上传进度。"""
    from . import upload_jobs
    job = upload_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="上传任务不存在或已过期")
    return upload_jobs.public_state(job)


@admin_router.post("/admin/mobile-versions/upload/{job_id}/retry")
async def retry_mobile_upload_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    """重试：把 failed 文件标回 pending，重启第②阶段。已 done 的不重传。"""
    from . import upload_jobs
    job = upload_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="上传任务不存在或已过期")
    if not upload_jobs.reset_failed_for_retry(job_id):
        raise HTTPException(status_code=400, detail="没有可重试的失败文件")
    _start_mobile_github_transfer(job_id)
    return {"job_id": job_id, "retried": True}


# ── 设备控制 App（被控端）版本发布 ───────────────────────────────────────────
#
# 与移动端控制 App 同构、复用同一套 upload_jobs 两阶段骨架，但二进制发布到独立
# 仓库 ai-lubricant-device-control 的 GitHub Release（不再入 marketplace 仓库）。
# asset 记 ``download_url``（GitHub Release 资产直链），无 ``repo_path``。

_DEVICE_CONTROL_URL_MAX_BYTES = 2 * 1024 * 1024 * 1024


@admin_router.post("/admin/device-control-versions/upload")
@_github_write_guard("上传设备控制 App 版本")
async def upload_device_control_version(
    version: str = Form(...),
    version_notes: str = Form(...),
    status: str = Form("published"),
    files: list[UploadFile] | None = File(None),
    download_url: str = Form(""),
    _: User = Depends(_require_admin),
) -> dict:
    """第①阶段：收 APK/IPA 或外链地址、识别、暂存，起后台第②阶段发布到独立仓库 Release。

    两种模式二选一（与移动端同口径）：
    - 文件模式：拖 APK/IPA 进来，第②阶段把二进制上传到 ai-lubricant-device-control
      仓库的 GitHub Release（tag ``device-control-v<version>``），asset 记 ``download_url``。
      Android（``-android.apk``）与 iOS 侧载包（``-ios.ipa``）可分次上传，同版本自动合并。
    - URL 模式：填外部下载地址，平台从 URL 文件名识别，第②阶段只下载校验 sha256，
      manifest 记 ``download_url``。

    立即返回 ``{job_id}``，前端轮询 ``/admin/device-control-versions/upload/{job_id}``。
    """
    version = version.strip()
    status = status.strip() or "published"
    download_url = download_url.strip()
    if not version:
        raise HTTPException(status_code=400, detail="version 必填")
    if status not in ("draft", "published"):
        raise HTTPException(status_code=400, detail="status 必须为 draft 或 published")

    from .validator import _is_device_control_version, is_https_url, identify_device_control_asset
    if not _is_device_control_version(version):
        raise HTTPException(status_code=400, detail="version 必须是数字/日期串或 semver（1.2.3）")

    from . import upload_jobs

    has_files = bool(files)
    has_url = bool(download_url)
    if has_files and has_url:
        raise HTTPException(status_code=400, detail="不能同时上传安装包文件与填写外部下载地址，请二选一")
    if not has_files and not has_url:
        raise HTTPException(status_code=400, detail="必须上传安装包文件，或填写外部安装包下载地址")

    if has_url:
        if not is_https_url(download_url):
            raise HTTPException(status_code=400, detail="安装包下载地址必须是 HTTPS URL")
        # 与校验器同口径：禁止带 token/sig 等可能含凭据的 query 字段入库。
        from .validator import _FORBIDDEN_QUERY_KEYS, _url_query
        leaked = [k for k in _url_query(download_url) if any(n in k.lower() for n in _FORBIDDEN_QUERY_KEYS)]
        if leaked:
            raise HTTPException(
                status_code=400,
                detail=f"下载地址不得携带可能含 token 的 query 字段: {sorted(set(leaked))}",
            )
        # URL 文件名必须符合命名契约（device-control-<版本>-android.apk / -ios.ipa）：
        # 被控端同时支持两个平台，平台只能从文件名识别，无法猜测，故要求文件名可识别。
        from urllib.parse import unquote
        basename = unquote(download_url.rstrip("/").rsplit("/", 1)[-1].split("?", 1)[0]) if "/" in download_url else ""
        url_ident = identify_device_control_asset(basename) if basename else {}
        if not url_ident:
            raise HTTPException(
                status_code=400,
                detail="下载地址文件名必须为 device-control-<版本>-android.apk 或 device-control-<版本>-ios.ipa（用于识别平台）",
            )
        if url_ident["version"] != version:
            raise HTTPException(
                status_code=400,
                detail=f"下载地址文件名版本「{url_ident['version']}」与填写的版本「{version}」不一致",
            )

    # GitHub Release 资产单文件上限 2GB（远大于 marketplace blob 的 100MB 入仓上限）。
    max_bytes = 2 * 1024 * 1024 * 1024
    files_meta: list[dict] = []
    seen: set[str] = set()
    job_id = upload_jobs.create_job(version, [])

    if has_url:
        content_type = (
            "application/vnd.android.package-archive"
            if url_ident["platform"] == "android" else "application/octet-stream"
        )
        files_meta.append({
            # 文件名即 URL basename：识别出的平台/格式与之一致，下游直接沿用。
            "filename": basename,
            "platform": url_ident["platform"], "format": url_ident["format"],
            "role": "device-control-app", "arch": "universal",
            "size": 0, "tmp_path": "",
            "content_type": content_type,
            "source": "url", "download_url": download_url,
        })
    else:
        tmpdir = upload_jobs.tmp_dir(job_id)
        os.makedirs(tmpdir, exist_ok=True)
        for upload in files:
            filename = (upload.filename or "").strip()
            ident = identify_device_control_asset(filename)
            if not ident:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(
                    status_code=400,
                    detail=(f"无法识别文件「{filename or '(空)'}」；文件名必须为"
                            "device-control-<版本>-android.apk 或 device-control-<版本>-ios.ipa"),
                )
            if ident["version"] != version:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(
                    status_code=400,
                    detail=f"文件名版本「{ident['version']}」与填写的版本「{version}」不一致",
                )
            if ident["platform"] in seen:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(status_code=400, detail=f"重复资产「{filename}」（platform 相同）")
            seen.add(ident["platform"])
            out_path = upload_jobs.tmp_path_for(job_id, filename)
            size = 0
            with open(out_path, "wb") as fh:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        upload_jobs.cleanup_job(job_id)
                        raise HTTPException(status_code=413, detail=f"文件过大（>2GB）：{filename}")
                    fh.write(chunk)
            if size == 0:
                upload_jobs.cleanup_job(job_id)
                raise HTTPException(status_code=400, detail=f"上传文件为空：{filename}")
            files_meta.append({
                "filename": filename, "platform": ident["platform"], "format": ident["format"],
                "role": "device-control-app", "arch": "universal",
                "size": size, "tmp_path": out_path,
                "content_type": upload.content_type or (
                    "application/vnd.android.package-archive" if ident["platform"] == "android"
                    else "application/octet-stream"
                ),
            })

    job = upload_jobs.get_job(job_id)
    upload_jobs.set_files(job_id, files_meta)
    job["version_notes"] = version_notes
    job["status_form"] = status

    upload_jobs.mark_to_github(job_id)
    _start_device_control_github_transfer(job_id)
    return {"job_id": job_id}


def _device_control_url_asset(f: dict, digest: str, size: int) -> dict:
    """URL 模式的 asset 形态：只记 download_url，无 repo_path（外链不入仓/Release）。"""
    return {
        "filename": f["filename"], "platform": f["platform"], "format": f["format"],
        "download_url": f["download_url"],
        "digest": digest,
        "size_bytes": size,
    }


async def _run_device_control_github_transfer(job_id: str) -> None:
    """第②阶段后台任务：把 APK/IPA 发布到独立仓库 GitHub Release，manifest 落 store。

    文件模式：创建 ai-lubricant-device-control 仓库的 Release（tag ``device-control-v<version>``），
    上传 APK/IPA 作为 Release asset，manifest 记 ``download_url``（browser_download_url）。
    同版本分次上传 Android/iOS 时按 platform 合并 assets。
    URL 模式：下载外链校验 sha256，asset 只记 ``download_url``。
    两种模式都只把 manifest 落 store，仓库 version.json 镜像由 publisher 异步推送。
    """
    from . import upload_jobs
    from . import github as mp_github
    from . import release_repos
    import marketplace_store as store

    job = upload_jobs.get_job(job_id)
    if job is None:
        return
    version = job["version"]
    tag = f"device-control-v{version}"
    version_notes = job.get("version_notes") or ""
    status = job.get("status_form") or "published"

    try:
        new_assets: list[dict] = []
        item_id = f"device-control-{version}"
        # prior assets 先读 store，store 没有再回落独立仓库 Release 的现有 assets。
        prior_assets: list[dict] = []
        prior_row = await store.get_item("device-control-versions", item_id)
        if prior_row and isinstance(prior_row.get("manifest"), dict):
            prior_assets = [a for a in (prior_row["manifest"].get("assets") or []) if isinstance(a, dict)]
        prior_by_key = {a.get("platform"): a for a in prior_assets}

        release_repo = release_repos.load_device_control_release_repo()
        # 文件模式才需要出网创建 Release；URL 模式只下载校验，不碰 GitHub Release
        # （与移动端口径一致）。
        release_info: dict | None = None
        if any(f.get("source") != "url" for f in job["files"]):
            if not release_repo.enabled:
                raise RuntimeError("设备控制发行仓库未配置（DEVICE_CONTROL_RELEASE_REPO_URL 与 token）")
            # 已存在则复用（支持补传/重试）；draft 用 prerelease 标记而非 draft：
            # draft Release 的资产直链未公开，会拿到打不开的 download_url。
            release_info = await mp_github.get_release_by_tag(
                release_repo.github_owner, release_repo.github_repo,
                release_repo.github_token, tag,
            ) or await mp_github.create_release(
                owner=release_repo.github_owner,
                repo=release_repo.github_repo,
                token=release_repo.github_token,
                tag=tag,
                name=f"设备控制 App v{version}",
                body=version_notes,
                prerelease=(status == "draft"),
            )

        for f in job["files"]:
            if f["state"] == "done":
                pa = prior_by_key.get(f.get("platform"))
                if pa is None and os.path.isfile(f.get("tmp_path") or ""):
                    with open(f["tmp_path"], "rb") as fh:
                        prior_data = fh.read()
                    prior_digest = "sha256:" + hashlib.sha256(prior_data).hexdigest()
                    prior_size = int(f.get("size") or len(prior_data))
                    if f.get("source") == "url":
                        pa = _device_control_url_asset(f, prior_digest, prior_size)
                    elif release_info and release_repo.enabled:
                        # 上轮 Release asset 已传、store 提交前失败：从现有 asset 取直链。
                        existing_asset = _find_release_asset(release_info, f["filename"])
                        pa = _device_control_release_asset(f, existing_asset, prior_digest, prior_size) if existing_asset else None
                if pa:
                    new_assets.append(pa)
                continue
            f["state"] = "uploading"
            upload_jobs.set_file_state(job_id, f["filename"], "uploading")
            try:
                if f.get("source") == "url":
                    tmp_path = upload_jobs.tmp_path_for(job_id, f["filename"])
                    f["tmp_path"] = tmp_path
                    size, digest_hex = await _fetch_url_to_file(
                        f["download_url"], tmp_path,
                        proxy_config_id=mp_config.settings.proxy_id or None,
                        max_bytes=_DEVICE_CONTROL_URL_MAX_BYTES,
                    )
                    f["size"] = size
                    new_assets.append(_device_control_url_asset(f, "sha256:" + digest_hex, size))
                else:
                    if not release_repo.enabled:
                        raise RuntimeError("设备控制发行仓库未配置（DEVICE_CONTROL_RELEASE_REPO_URL / token）")
                    with open(f["tmp_path"], "rb") as fh:
                        data = fh.read()
                    asset_resp = await mp_github.upload_release_asset(
                        upload_url=release_info["upload_url"],
                        token=release_repo.github_token,
                        filename=f["filename"],
                        content=data,
                        content_type=f.get("content_type") or (
                            "application/vnd.android.package-archive" if f.get("platform") == "android"
                            else "application/octet-stream"
                        ),
                    )
                    new_assets.append(_device_control_release_asset(
                        f, asset_resp, "sha256:" + hashlib.sha256(data).hexdigest(), f["size"],
                    ))
                upload_jobs.set_file_state(job_id, f["filename"], "done")
            except Exception as exc:  # noqa: BLE001
                logger.warning("[marketplace] device-control-versions asset {} failed: {}", f["filename"], exc)
                upload_jobs.set_file_state(job_id, f["filename"], "failed", str(exc))
                continue

        failed = [f for f in upload_jobs.get_job(job_id)["files"] if f["state"] == "failed"]
        if failed:
            upload_jobs.mark_failed(job_id, f"{len(failed)} 个文件发布失败")
            return

        job["phase"] = "finalizing"
        by_key = {a.get("platform"): a for a in prior_assets}
        for asset in new_assets:
            by_key[asset["platform"]] = asset
        merged_assets = list(by_key.values())
        platforms = sorted({str(a.get("platform")) for a in merged_assets if a.get("platform")})
        manifest = {
            "schema": DEVICE_CONTROL_VERSION_SCHEMA, "id": item_id,
            "kind": "device_control_app_version",
            "name": "device-control", "display_name": f"设备控制 App {version}", "version": version,
            "version_notes": version_notes, "status": status,
            "assets": merged_assets, "release_tag": tag,
            "category": "device-control-app", "tags": ["device-control", *platforms],
        }
        errors = validate_manifest("device-control-versions", manifest)
        if errors:
            upload_jobs.mark_failed(job_id, "manifest 校验失败: " + "; ".join(errors))
            return
        await store.upsert_item("device-control-versions", manifest)
        upload_jobs.mark_done(job_id)
        upload_jobs.cleanup_job(job_id)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        upload_jobs.mark_failed(job_id, f"转传任务异常: {exc}")


def _find_release_asset(release_info: dict, filename: str) -> dict | None:
    """从 Release 信息里按文件名找现有 asset；用于重试时复用已传资产。"""
    for asset in (release_info.get("assets") or []):
        if isinstance(asset, dict) and asset.get("name") == filename:
            return asset
    return None


def _device_control_release_asset(f: dict, asset_resp: dict, digest: str, size: int) -> dict:
    """文件模式的 asset 形态：记 GitHub Release 资产直链 ``download_url``。"""
    return {
        "filename": f["filename"], "platform": f["platform"], "format": f["format"],
        "download_url": str(asset_resp.get("browser_download_url") or ""),
        "digest": digest,
        "size_bytes": int(asset_resp.get("size") or size),
    }


def _start_device_control_github_transfer(job_id: str) -> None:
    from . import upload_jobs
    old = upload_jobs.get_task(job_id)
    if old is not None and not old.done():
        old.cancel()
    task = asyncio.create_task(
        _run_device_control_github_transfer(job_id), name=f"device-control-upload-{job_id}"
    )
    upload_jobs.set_task(job_id, task)


@admin_router.get("/admin/device-control-versions/upload/{job_id}")
async def get_device_control_upload_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    """轮询设备控制 App 上传进度。"""
    from . import upload_jobs
    job = upload_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="上传任务不存在或已过期")
    return upload_jobs.public_state(job)


@admin_router.post("/admin/device-control-versions/upload/{job_id}/retry")
async def retry_device_control_upload_job(job_id: str, _: User = Depends(_require_admin)) -> dict:
    """重试：把 failed 文件标回 pending，重启第②阶段。已 done 的不重传。"""
    from . import upload_jobs
    job = upload_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="上传任务不存在或已过期")
    if not upload_jobs.reset_failed_for_retry(job_id):
        raise HTTPException(status_code=400, detail="没有可重试的失败文件")
    _start_device_control_github_transfer(job_id)
    return {"job_id": job_id, "retried": True}


@admin_router.post("/admin/items")
async def upsert(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    module = str(body.get("module") or "")
    manifest = body.get("manifest") if isinstance(body.get("manifest"), dict) else body
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    errors = validate_manifest(module, manifest)
    if errors:
        raise HTTPException(status_code=400, detail={"error": "manifest 校验失败", "errors": errors})
    safe = safe_item_id(str(manifest.get("id")).replace("/", "."))
    if not safe:
        raise HTTPException(status_code=400, detail="invalid item id")

    # 落 PG 编辑真相源：保存即生效（本进程消费端立即可见），GitHub 镜像由 publisher
    # 异步推送（item/index/marker/version.json 一起渲）。零出网，请求毫秒级返回。
    import marketplace_store as store

    try:
        row = await store.upsert_item(module, manifest)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "ok": True,
        "item": row.get("summary") or index_summary(module, manifest),
        "publish": await store.publish_counts(),
    }


@admin_router.post("/admin/import")
async def import_data(body: dict = Body(...), _: User = Depends(_require_admin)) -> dict:
    mode = str(body.get("mode") or "merge")
    if mode not in ("merge", "replace"):
        raise HTTPException(status_code=400, detail="mode 必须是 merge 或 replace")

    normalized = normalize_import_body(body)
    import marketplace_store as store

    written: list[str] = []
    deleted: list[str] = []
    failed: list[dict] = []

    for module, entry in normalized.items():
        if not _valid_module(module):
            failed.append({"module": module, "id": "*", "errors": ["unknown module"]})
            continue
        incoming = entry.get("manifests") or {}

        # 校验全部；任一无效不写入并记录字段级错误。
        valid: list[dict] = []
        for key, manifest in incoming.items():
            m = {**(manifest or {})}
            m.setdefault("id", key)
            errs = validate_manifest(module, m)
            if errs:
                failed.append({"module": module, "id": str(m.get("id")), "errors": errs})
                continue
            valid.append(m)

        if mode == "replace":
            # replace：导入包未包含的现有条目物理删除（hard 语义，资产随 publisher 清理）。
            keep = {str(m.get("id")) for m in valid}
            existing_ids = await store.list_item_ids(module)
            await store.replace_module(module, [m for m in valid if safe_item_id(str(m.get("id")).replace("/", "."))])
            for mid in sorted(existing_ids - keep):
                deleted.append(f"{module}/{mid}")
            for m in valid:
                if safe_item_id(str(m.get("id")).replace("/", ".")):
                    written.append(f"{module}/{m.get('id')}")
        else:
            # merge：逐条 upsert（已存在的行 revision +1），不删任何现有条目。
            for m in valid:
                mid = str(m.get("id"))
                if not safe_item_id(mid.replace("/", ".")):
                    failed.append({"module": module, "id": mid, "errors": ["invalid id"]})
                    continue
                try:
                    await store.upsert_item(module, m)
                    written.append(f"{module}/{mid}")
                except Exception as exc:  # noqa: BLE001
                    failed.append({"module": module, "id": mid, "errors": [str(exc)]})

    return {
        "written": written, "deleted": deleted, "failed": failed,
        "count": len(written), "publish": await store.publish_counts(),
    }


@admin_router.post("/admin/rebuild-index")
async def rebuild_index(body: dict = Body(default={}), _: User = Depends(_require_admin)) -> dict:
    module = str(body.get("module") or "")
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    # 从 store 的 manifest 重新计算全部索引行（summary 是写入时的投影，重建就是
    # 以 manifest 为准重算），index/marker/发行投影由 publisher 镜像。
    import marketplace_store as store

    count = await store.recompute_summaries(module)
    return {"module": module, "count": count, "publish": await store.publish_counts()}


@admin_router.delete("/admin/items/{module}/{item}")
async def delete_item(
    module: str, item: str, hard: bool = False, _: User = Depends(_require_admin)
) -> dict:
    if not _valid_module(module):
        raise HTTPException(status_code=400, detail=f"unknown module: {module}")
    safe = safe_item_id(item)
    if not safe:
        raise HTTPException(status_code=400, detail="invalid item id")
    # 编辑真相源删行：hard 物理删除（删除前 manifest 进 job payload，publisher 据此
    # 清理入仓二进制与 release manifest——Gitee 免费仓库 1GB，残留会顶满配额）；
    # soft 只把 index 行与 manifest.status 改成 hidden。仓库镜像两件事都由 publisher
    # 异步完成；本进程消费侧在事务提交后立即可见新状态。
    import marketplace_store as store

    alt = item.replace(".", "/")
    target = await store.get_item(module, item) or await store.get_item(module, alt)
    if target is None:
        raise HTTPException(status_code=404, detail="not found")
    canonical = str(target.get("item_id") or item)
    try:
        await store.delete_item(module, canonical, hard=hard)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
    return {
        "module": module, "id": canonical, "deleted": hard, "hidden": not hard,
        "publish": await store.publish_counts(),
    }


# ── admin: 发布队列面板与仓库回采 ────────────────────────────────────────────


@admin_router.get("/admin/publish-jobs")
async def publish_jobs(
    module: str | None = None, _: User = Depends(_require_admin)
) -> dict:
    """发布队列面板：pending/pushing/failed 明细与计数（前端徽标 + 重试入口）。"""
    import marketplace_store as store

    return await store.list_jobs(module)


@admin_router.post("/admin/publish-jobs/retry")
async def retry_publish_jobs(_: User = Depends(_require_admin)) -> dict:
    """手动重试发布：failed → pending，publisher 立即被唤醒。"""
    import marketplace_store as store

    count = await store.retry_failed()
    return {"retried": count, "publish": await store.publish_counts()}


@admin_router.post("/admin/resync-from-repo")
async def resync_from_repo(_: User = Depends(_require_admin)) -> dict:
    """采纳仓库直改：把 GitHub 当前内容 merge 进 store（仓库没有的条目保留）。

    直改 GitHub 在新架构下不再是权威——script/publish_channel_templates.py 这类
    外部 writer 的产物，或人工在仓库网页上的修改，要靠这里显式采回 store，否则
    会被下一次发布覆盖。
    """
    from . import bootstrap

    return await bootstrap.resync_from_repo()


# ── agency-agents 提示词源同步 ──────────────────────────────────────────────
#
# agency-agents 仓库自带结构化清单（frontmatter + divisions.json），确定性转换
# 273 个角色 agent 为 prompts manifest，直连 merge 导入链路入 prompts 池——不需要
# agent 猜 README，也省去人工下载 import-prompts.json 手动导入。

@admin_router.post("/admin/marketplace/agency-agents/sync")
async def sync_agency_agents_route(
    body: dict = Body(default={}), _: User = Depends(_require_admin)
) -> dict:
    """手动同步 agency-agents 提示词源到市场 prompts 模块（merge）。

    body: ``{"ref": "<commit-sha 或 branch>", "flush": true, "dry_run": false}``。
    默认 pinned commit；``flush`` 忽略缓存重拉 tarball；``dry_run`` 只转换+校验不落库。
    出网经 proxy_manager（与榜单同步同一链路），出网失败返回 502。
    """
    from . import agency_agents_convert

    ref = str(body.get("ref") or "").strip() or agency_agents_convert.DEFAULT_REF
    flush = bool(body.get("flush"))
    dry = bool(body.get("dry_run"))
    try:
        return await agency_agents_convert.sync_agency_agents(ref=ref, flush=flush, dry_run=dry)
    except Exception as exc:  # noqa: BLE001 — 出网/解析失败要给人话
        raise HTTPException(status_code=502, detail=f"agency-agents 同步失败：{exc}") from exc


@admin_router.get("/admin/marketplace/agency-agents/last-sync")
async def agency_agents_last_sync_route(_: User = Depends(_require_admin)) -> dict:
    """上次同步结果（ran_at / ok / detail），管理端回显用。"""
    from . import agency_agents_convert

    return agency_agents_convert.last_result()


# ── agency-agents-zh（中文社区版，同款换仓库）──

@admin_router.post("/admin/marketplace/agency-agents-zh/sync")
async def sync_agency_agents_zh_route(
    body: dict = Body(default={}), _: User = Depends(_require_admin)
) -> dict:
    """手动同步 agency-agents-zh 提示词源（中文社区版）。与 agency-agents 同构。"""
    from . import agency_agents_convert

    ref = str(body.get("ref") or "").strip() or agency_agents_convert.DEFAULT_REF
    flush = bool(body.get("flush"))
    dry = bool(body.get("dry_run"))
    try:
        return await agency_agents_convert.sync_agency_agents(ref=ref, flush=flush, dry_run=dry, repo="jnMetaCode/agency-agents-zh")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"agency-agents-zh 同步失败：{exc}") from exc


@admin_router.get("/admin/marketplace/agency-agents-zh/last-sync")
async def agency_agents_zh_last_sync_route(_: User = Depends(_require_admin)) -> dict:
    from . import agency_agents_convert
    return agency_agents_convert.last_result_zh()


# ── agentscope（公开 API 技能源）──

@admin_router.post("/admin/marketplace/agentscope/sync")
async def sync_agentscope_route(_: User = Depends(_require_admin)) -> dict:
    """手动同步 agentscope 技能源（公开 API，无需鉴权）。"""
    from . import agentscope_convert

    try:
        return await agentscope_convert.sync_agentscope()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"agentscope 同步失败：{exc}") from exc


@admin_router.get("/admin/marketplace/agentscope/last-sync")
async def agentscope_last_sync_route(_: User = Depends(_require_admin)) -> dict:
    from . import agentscope_convert
    return agentscope_convert.last_result()


# ── consumer: 外部榜单发现视图 ──────────────────────────────────────────────
#
# 与上面 GitHub 原文消费侧不同：榜单候选池是数据库表，发布只是把 status 改成
# published。这里只读 published 行,草稿永远漏不出去（``list_published_for_consumer``
# 在 SQL 层钉死 status='published',路由参数无法覆盖）。


@router.get("/consumer/leaderboard")
async def consumer_leaderboard(
    target_module: str | None = None,
    board: str | None = None,
    installable: bool | None = None,
    stack_tag: str | None = None,
    q: str = "",
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """消费侧榜单发现视图：只返回已发布条目。

    不鉴权——榜单是公开发现物。``installable=false`` 的条目（开发框架、研究程序、
    awesome 目录）也会出现,但前端只渲染成「仅浏览」卡片(跳 GitHub,无安装入口)。
    未配同步或没有已发布条目时返回空列表,不报错。
    ``stack_tag`` 按技术栈 tag 过滤（python/typescript/web_frontend…）。
    """
    import marketplace_leaderboard_store as store

    items = await store.list_published_for_consumer(
        target_module=target_module, board=board,
        installable=installable, stack_tag=stack_tag,
        query=q, limit=limit, offset=offset,
    )
    return {"items": items, "count": len(items)}


# ── admin: 外部榜单候选池 ──────────────────────────────────────────────────────
#
# 同步进来的一律是 draft，用户不可见。发布只走这里的显式接口（含批量），
# 没有任何自动发布路径：同步与 agent 补全都只写草稿字段。


@admin_router.get("/admin/leaderboard/items")
async def list_leaderboard_items(
    board: str | None = None,
    status: str | None = None,
    source: str | None = None,
    target_module: str | None = None,
    installable: bool | None = None,
    launch_status: str | None = None,
    stack_tag: str | None = None,
    q: str = "",
    limit: int = 200,
    offset: int = 0,
    _: User = Depends(_require_admin),
) -> dict:
    import marketplace_leaderboard_store as store

    items = await store.list_items(
        board=board, status=status, source=source, target_module=target_module,
        installable=installable, launch_status=launch_status, stack_tag=stack_tag,
        query=q, limit=limit, offset=offset,
    )
    return {
        "items": items,
        "total": await store.count_items(board=board, status=status),
        "draft_count": await store.count_items(status="draft"),
        "published_count": await store.count_items(status="published"),
    }


@admin_router.get("/admin/leaderboard/status")
async def leaderboard_status(_: User = Depends(_require_admin)) -> dict:
    """同步开关与上次结果。未启用时前端据此显示引导而不是空列表。"""
    from . import leaderboard_sync
    import marketplace_leaderboard_store as store

    source = await get_source_config_async()
    return {
        "enabled": await leaderboard_sync.is_enabled(),
        "configured": bool(source.get("leaderboard_sync_enabled")),
        "interval_hours": int(source.get("leaderboard_sync_interval_hours") or 24),
        "repo": source.get("leaderboard_repo") or leaderboard_sync.DEFAULT_LEADERBOARD_REPO,
        "boards": leaderboard_sync.resolve_boards(source),
        "launch_agent_id": int(source.get("leaderboard_launch_agent_id") or 0),
        "last_sync": leaderboard_sync.last_result(),
        "draft_count": await store.count_items(status="draft"),
        "published_count": await store.count_items(status="published"),
    }


@admin_router.post("/admin/leaderboard/items", status_code=201)
async def create_leaderboard_item(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """手动添加一条 GitHub 项目为草稿（source=manual，与同步条目同形态）。

    会先查 GitHub API 拿仓库元数据（描述/stars/语言/topics）并按关键词预分类；
    同仓库已在候选池里（任意来源）则返回 409 + 既有条目 id，前端直接打开编辑。
    """
    from . import leaderboard_sync
    import marketplace_leaderboard_store as store

    raw = str(body.get("repo") or body.get("repo_url") or "").strip() if isinstance(body, dict) else ""
    # 容错：整段 URL 或 owner/repo 都收
    raw = raw.removeprefix("https://github.com/").strip("/")
    if "/" not in raw:
        raise HTTPException(status_code=400, detail="请提供 owner/repo（或仓库完整地址）")
    full_name = "/".join(part for part in raw.split("/") if part)
    if len(full_name.split("/")) != 2:
        raise HTTPException(status_code=400, detail="仓库标识形如 owner/repo")

    existing = await store.find_by_repo(full_name)
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail={"message": "该仓库已在候选池中", "existing_id": existing["id"]},
        )

    try:
        meta = await leaderboard_sync.fetch_repo_metadata(full_name)
    except Exception as exc:  # noqa: BLE001 — 出网失败要给人话
        raise HTTPException(status_code=502, detail=f"读取 GitHub 仓库信息失败：{exc}") from exc

    item = leaderboard_sync.manual_item(full_name, meta)
    if item is None:
        raise HTTPException(status_code=400, detail="无法解析仓库标识")
    # 手动添加也跑确定性探针（与同步链路同一份 attach_probe）：仓库有 .mcp.json/SKILL.md
    # 等结构化清单时，install_spec/launch_spec 直接派生，不靠 agent 猜。探针失败不挡创建。
    from . import leaderboard_probe
    item = await leaderboard_probe.attach_probe(item)
    row, _created = await store.create_manual_item(item)
    if row is None:
        raise HTTPException(status_code=500, detail="创建失败")
    return row


@admin_router.post("/admin/leaderboard/sync", status_code=202)
async def trigger_leaderboard_sync(_: User = Depends(_require_admin)) -> dict:
    """手动触发一次同步。仍然只写草稿，不发布任何条目。"""
    from . import leaderboard_sync

    if not await leaderboard_sync.is_enabled():
        raise HTTPException(status_code=409, detail="外部榜单同步未启用，请先在市场管理的外部榜单配置里打开")
    result = await leaderboard_sync.sync_once()
    return result


@admin_router.patch("/admin/leaderboard/items/{item_id}")
async def update_leaderboard_item(
    item_id: int, body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """管理员收口归类/可安装性/标签/启动方式/名次。不含发布——发布是独立动作。"""
    import marketplace_leaderboard_store as store

    source = await get_source_config_async()
    require_verified = bool(source.get("leaderboard_require_verified"))
    try:
        row = await store.update_curation(
            item_id, body if isinstance(body, dict) else {},
            require_verified=require_verified,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="条目不存在")
    return row


@admin_router.post("/admin/leaderboard/publish")
async def publish_leaderboard_items(
    body: dict = Body(...), user: User = Depends(_require_admin)
) -> dict:
    """批量发布（也用于单条：传一个 id）。

    逐条校验后放行：已发布的算 skipped（幂等），声明可装但未归类的进 failed 并说明
    原因，互不影响。发布 = 用户侧可见；仅浏览条目也能发布，只是没有安装入口。
    MCP launch_spec 门禁（软+开关）：可安装且分类含 mcp 的条目至少 filled 才放行，
    ``leaderboard_require_verified`` 开时要求 verified。
    """
    import marketplace_leaderboard_store as store

    raw_ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="ids 必须是非空数组")
    ids: list[int] = []
    for raw in raw_ids:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in ids:
            ids.append(value)
    if not ids:
        raise HTTPException(status_code=400, detail="请至少选择一个条目")
    source = await get_source_config_async()
    require_verified = bool(source.get("leaderboard_require_verified"))
    result = await store.publish_items(
        ids, operator=str(getattr(user, "username", "") or user.id),
        require_verified=require_verified,
    )
    return {**result, "requested": len(ids), "count": len(result["published"])}


@admin_router.post("/admin/leaderboard/unpublish")
async def unpublish_leaderboard_items(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """批量撤回发布（回 draft，用户侧立即不可见）。"""
    import marketplace_leaderboard_store as store

    raw_ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="ids 必须是非空数组")
    ids = [int(r) for r in raw_ids if str(r).strip().isdigit()]
    if not ids:
        raise HTTPException(status_code=400, detail="请至少选择一个条目")
    reverted = await store.unpublish_items(ids)
    return {"unpublished": reverted, "count": len(reverted)}


@admin_router.post("/admin/leaderboard/purge")
async def purge_leaderboard_items(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """清空整个榜单候选池。**破坏性**——旧模型（admin_overrides/rank）数据与新设计
    不匹配时用，清空后重新同步即按新模型 + 探针重新派生。

    body 必须带 ``{"confirm": "purge"}`` 二次确认，否则 400。
    """
    import marketplace_leaderboard_store as store

    if str(body.get("confirm") or "").strip() != "purge":
        raise HTTPException(status_code=400, detail='需带 {"confirm": "purge"} 二次确认')
    removed = await store.purge_all()
    return {"purged": removed, "detail": f"已清空 {removed} 条候选池条目，重新同步后按新模型派生"}


@admin_router.post("/admin/leaderboard/items/{item_id}/fill-launch-spec", status_code=202)
async def fill_leaderboard_launch_spec(
    item_id: int, _: User = Depends(_require_admin)
) -> dict:
    """让配置的专用 agent 去补这条 MCP 的启动方式。产出是**草稿**。

    按需触发而非同步时全量跑：候选池里绝大多数条目永远不会被发布，全量跑纯属
    浪费 token。补出的 ``launch_spec`` 需要人工确认后再发布——agent 从 README 猜
    启动命令可能猜错，错的启动方式比缺失更糟。
    """
    from . import leaderboard_launch_agent

    try:
        return await leaderboard_launch_agent.fill_launch_spec(item_id)
    except leaderboard_launch_agent.LaunchAgentUnavailable as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@admin_router.post("/admin/leaderboard/items/{item_id}/verify", status_code=200)
async def verify_leaderboard_item(
    item_id: int, body: dict = Body(default={}), _: User = Depends(_require_admin)
) -> dict:
    """重新验证一条条目的 launch_spec（可选 install_spec）。幂等。

    remote MCP 走 JSON-RPC initialize 握手；stdio 走 npm/pypi registry HEAD；
    skill/plugin 走 Contents/download_url HEAD。结果写回 launch_spec_status
    （verified/failed）与 external_data.probe.install_verify（informational）。
    """
    from . import leaderboard_verify

    try:
        return await leaderboard_verify.verify_item(
            item_id, include_install=bool(body.get("include_install")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _parse_recognize_body(body: dict) -> tuple[list[int], int | None, str]:
    """批量识别请求体解析：{ids: [...], agent_id?: int, model?: str}。"""
    raw_ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="ids 必须是非空数组")
    ids: list[int] = []
    for raw in raw_ids:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in ids:
            ids.append(value)
    if not ids:
        raise HTTPException(status_code=400, detail="请至少选择一个条目")
    agent_id: int | None = None
    try:
        agent_id = int(body.get("agent_id") or 0) or None
    except (TypeError, ValueError):
        agent_id = None
    model = str(body.get("model") or "").strip()
    return ids, agent_id, model


@admin_router.post("/admin/leaderboard/fill-launch-specs")
async def fill_leaderboard_launch_specs(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """兼容端点：批量识别仓库信息。新前端走 ``recognize-repo-info``（带 agent/模型选择）。"""
    return await _run_batch_recognition(body)


@admin_router.post("/admin/leaderboard/recognize-repo-info")
async def recognize_leaderboard_repo_info(
    body: dict = Body(...), _: User = Depends(_require_admin)
) -> dict:
    """批量识别仓库信息（新端点）：所有分类补描述/子分类/标签，MCP 顺带识别启动方式。

    请求体可带 ``agent_id``/``model``（批量弹框的下拉选择）；未带则用市场配置的
    专用 agent 及其绑定模型。逐条串行执行，单条失败不影响其余；产出全部是草稿。
    """
    return await _run_batch_recognition(body)


async def _run_batch_recognition(body: dict) -> dict:
    """批量识别实现：preflight 校验 agent 可用，然后逐条串行跑。"""
    from . import leaderboard_launch_agent
    import marketplace_leaderboard_store as store

    ids, agent_id, model = _parse_recognize_body(body)

    # agent 不可用对整批都是致命的，先探测一次再开工，省得跑一半才 409。
    try:
        await leaderboard_launch_agent.resolve_agent_id(agent_id)
    except leaderboard_launch_agent.LaunchAgentUnavailable as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # 逐条识别：所有分类补描述/子分类/标签，分类含 mcp 的顺带补启动方式。
    results: list[dict] = []
    for item_id in ids:
        item = await store.get_item(item_id)
        repo_name = str((item or {}).get("repo_full_name") or "")
        try:
            got = await leaderboard_launch_agent.recognize_item(item_id, agent_id=agent_id, model=model)
            results.append({
                "id": item_id,
                "repo_full_name": repo_name,
                "ok": bool(got.get("ok")),
                "description": got.get("description") or "",
                "error": got.get("error") or "",
            })
        except ValueError as exc:
            results.append({"id": item_id, "repo_full_name": repo_name, "ok": False, "error": str(exc)})
        except Exception as exc:  # noqa: BLE001 — 单条失败不拖垮整批
            results.append({"id": item_id, "repo_full_name": repo_name, "ok": False, "error": str(exc)})
    return {
        "results": results,
        "count": sum(1 for r in results if r["ok"]),
    }
