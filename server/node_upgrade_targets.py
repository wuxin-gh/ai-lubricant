"""节点升级代理解析、可达性探测。

下载地址不再在此模块解析——它来自服务端缓存的 ``node-releases/version.json``
（见 :mod:`node_release_catalog`），由 :func:`select_upgrade_assets` 按节点
role/os/arch 选出。本模块只负责代理池条目到升级帧 ``proxy_*`` 字段的转换与
下载前可达性探测。

「上次成功升级用的代理」记忆已迁到节点记录的 ``last_proxy_config_id`` 列
（见 :mod:`node_server.store` 的 ``set_node_last_proxy`` / 控制面
``SetNodeLastProxy``），不再用这里的全局 Postgres 槽。

安全边界：浏览器只提交 ``proxy_config_id``，下载地址与平台匹配全在服务端；
代理池的 ``node`` 隧道模式对"节点自身下载"无意义，在此直接拒绝。
"""
from __future__ import annotations

from typing import Any

import aiohttp

_PROBE_TIMEOUT_SECONDS = 20


class UpgradeTargetError(Exception):
    """A target could not be resolved or is not reachable. Message is user-facing."""


async def resolve_proxy(proxy_config_id: str | None) -> dict:
    """Resolve a proxy pool entry id into the proxy_* fields carried by the upgrade frame.

    Returns ``{}`` for empty/unknown/direct. Raises for the ``node`` mode, which
    tunnels through another execution node and is meaningless for a node's own
    download.
    """
    wanted = (proxy_config_id or "").strip()
    if not wanted:
        return {}
    from config import CONFIG_STORE
    from proxy_utils import (
        canonical_url_prefix,
        find_proxy,
        proxy_effective_url,
        proxy_mode,
    )

    try:
        main = await CONFIG_STORE.read_main_async()
    except Exception as exc:
        raise UpgradeTargetError(f"读取代理池失败: {exc}") from exc
    proxies = main.get("proxies") if isinstance(main, dict) else []
    entry = find_proxy(proxies if isinstance(proxies, list) else [], wanted)
    if entry is None:
        raise UpgradeTargetError(f"代理配置 {wanted} 不存在")
    raw_mode = str(entry.get("mode") or "").strip().lower()
    if raw_mode == "node":
        raise UpgradeTargetError("节点隧道代理不能用于节点自身下载，请选择网络代理或 URL 前缀代理")
    mode = proxy_mode(entry)
    if mode == "direct":
        return {}
    if mode == "url_prefix":
        prefix = canonical_url_prefix(entry.get("url"))
        if not prefix:
            raise UpgradeTargetError("URL 前缀代理未配置前缀地址")
        return {"proxy_mode": "url_prefix", "proxy_url_prefix": prefix}
    url = proxy_effective_url(entry)
    if not url:
        raise UpgradeTargetError("网络代理未配置地址")
    return {"proxy_mode": "network", "proxy_url": url}


async def probe_asset(download_url: str, proxy_fields: dict) -> None:
    """Verify the asset is fetchable, through the same route the node will use."""
    mode = proxy_fields.get("proxy_mode") or ""
    target = download_url
    request_kwargs: dict[str, Any] = {}
    if mode == "url_prefix":
        prefix = str(proxy_fields.get("proxy_url_prefix") or "").rstrip("/")
        target = f"{prefix}/{download_url.lstrip('/')}"
    elif mode == "network":
        request_kwargs["proxy"] = proxy_fields.get("proxy_url")

    timeout = aiohttp.ClientTimeout(total=_PROBE_TIMEOUT_SECONDS)
    # Range-limited GET rather than HEAD: GitHub release redirects to object
    # storage, and some proxies answer HEAD differently than GET.
    headers = {"Range": "bytes=0-0", "User-Agent": "ai-lubricant-node-upgrade-probe"}
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(target, headers=headers, **request_kwargs) as response:
                if response.status >= 400:
                    raise UpgradeTargetError(
                        f"探测下载地址失败: HTTP {response.status}"
                        f"{'（经代理）' if mode else '（直连）'}，请更换代理后重试"
                    )
                await response.content.read(1)
    except UpgradeTargetError:
        raise
    except Exception as exc:
        raise UpgradeTargetError(
            f"探测下载地址失败{'（经代理）' if mode else '（直连）'}: {exc}，请更换代理后重试"
        ) from exc
