"""节点升级代理解析、下载目标解析、可达性探测。

节点自升级二进制的地址来自服务端缓存的 ``node-releases/version.json``
（见 :mod:`node_release_catalog`），由 :func:`select_upgrade_assets` 按节点
role/os/arch 选出；Node.js 宿主工具归档的地址/哈希由
:func:`resolve_nodejs_asset` 按镜像 ``SHASUMS256.txt`` 实时解析。本模块还
负责代理池条目到升级帧 ``proxy_*`` 字段的转换与下载前可达性探测。

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


async def resolve_nodejs_asset(
    mirror: str,
    version: str,
    os_name: str,
    arch: str,
    proxy_fields: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Resolve the Node.js archive URL + sha256 from the mirror's real catalog.

    不凭命名规则拼归档名：节点上报的是 Go 的 GOARCH（amd64），而 Node 发行
    归档叫 x64，直接拼必 404（2026-09-09 实测）。每个版本目录里的
    SHASUMS256.txt 是权威清单（nodejs.org 与 npmmirror 布局一致、逐字节
    相同，节点端 fetchDistSHA256 已依赖它），拉这一份即可按节点 os/arch
    挑出真实存在的文件名并顺带取得 sha256（节点端就不再抓 SHASUMS 兜底）。
    """
    base = str(mirror or "").rstrip("/")
    wanted_version = str(version or "").strip().lstrip("v")
    platform = {"windows": "win", "darwin": "darwin", "linux": "linux"}.get(
        str(os_name or "").strip().lower()
    )
    node_arch = {
        "amd64": "x64",
        "386": "x86",
        "arm64": "arm64",
        "arm": "armv7l",
    }.get(str(arch or "").strip().lower(), str(arch or "").strip().lower())
    if not base or not wanted_version:
        raise UpgradeTargetError("服务端未配置 Node.js 版本/镜像")
    if not platform:
        raise UpgradeTargetError(f"不支持在 {os_name} 节点上安装 Node.js")
    if not node_arch:
        raise UpgradeTargetError(f"无法识别节点上报的架构 {arch}")

    fields = proxy_fields or {}
    mode = fields.get("proxy_mode") or ""
    sums_url = f"{base}/v{wanted_version}/SHASUMS256.txt"
    target = sums_url
    request_kwargs: dict[str, Any] = {}
    if mode == "url_prefix":
        prefix = str(fields.get("proxy_url_prefix") or "").rstrip("/")
        target = f"{prefix}/{sums_url.lstrip('/')}"
    elif mode == "network":
        request_kwargs["proxy"] = fields.get("proxy_url")

    timeout = aiohttp.ClientTimeout(total=_PROBE_TIMEOUT_SECONDS)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                target,
                headers={"User-Agent": "ai-lubricant-node-upgrade"},
                **request_kwargs,
            ) as response:
                if response.status != 200:
                    raise UpgradeTargetError(
                        f"Node.js 镜像 SHASUMS256.txt 拉取失败：HTTP {response.status}"
                        f"（{base}/v{wanted_version}），请核对镜像/版本配置"
                        f"（AGENT_COMPOSE_NODEJS_MIRROR/VERSION）或为节点绑定可用代理"
                    )
                body = await response.text()
    except UpgradeTargetError:
        raise
    except Exception as exc:
        raise UpgradeTargetError(
            f"Node.js 镜像 SHASUMS256.txt 拉取失败：{exc}，"
            f"请核对镜像配置（AGENT_COMPOSE_NODEJS_MIRROR）或为节点绑定可用代理"
        ) from exc

    sums: dict[str, str] = {}
    for line in body.splitlines():
        parts = line.split()
        if len(parts) == 2:
            sums[parts[1]] = parts[0]
    if not sums:
        raise UpgradeTargetError("Node.js 镜像 SHASUMS256.txt 内容无效")

    ext = ".zip" if platform == "win" else ".tar.gz"
    filename = f"node-v{wanted_version}-{platform}-{node_arch}{ext}"
    if filename not in sums:
        raise UpgradeTargetError(
            f"Node.js v{wanted_version} 没有适用于 {os_name}/{arch} 的官方归档 {filename}，"
            f"可对照该版本 SHASUMS256.txt 支持的平台/架构"
        )
    return {
        "download_url": f"{base}/v{wanted_version}/{filename}",
        "sha256": sums[filename],
    }


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
