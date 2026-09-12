"""节点公网 IP 探测地址备份（市场管理 → 节点公网 IP tab），存 DB 主配置 blob 的
``node_public_ip`` key。

与市场仓库完全无关、与「全局配置 → 节点全局」tab **互不关联**：全局配置仍是节点
实际在用的唯一真相源（``node_server`` store + 实时下发），这份只是**备份数据**。
两边各自编辑、各自保存；市场这份不会自动镜像到全局配置。

唯一的桥梁是管理端「同步市场数据」按钮：把这里的备份 URL **追加（不覆盖、去重）**
到全局配置——该动作走前端既有 ``PUT /api/v1/admin/global-config/node-global``，本
模块不参与下发。

数据形态（归一后）::

    {
      "ipv4_urls": ["https://ip4.me/", "http://httpbin.org/ip"],
      "ipv6_urls": ["https://ip6only.me/", "http://v6.ipv6-test.com/api/myip.php"],
    }

DB 无该 key 时回落内置默认列表——全新部署也有一份可用的备份。管理员显式保存空
列表则保留空（=该地址族无备份），不会被默认值覆盖。
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

# 内置默认备份列表：与全局配置侧「至少 3 个网站」的建议不同，这里只放每族 2 个
# 稳定公网源作为兜底；管理员可在此基础上增删。URL 不强制 HTTP(S)——node_server
# 侧 ``_normalize_lookup_urls`` 在写入全局配置时会再归一一次（非法 scheme 被丢弃）。
DEFAULT_IPV4_URLS: list[str] = ["https://ip4.me/", "http://httpbin.org/ip"]
DEFAULT_IPV6_URLS: list[str] = [
    "https://ip6only.me/",
    "http://v6.ipv6-test.com/api/myip.php",
]

_CONFIG_KEY = "node_public_ip"


def _normalize_url_list(value: Any) -> list[str]:
    """把自由形态的 URL 列表归一：取字符串、strip、丢空、保序去重。"""
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in value:
        url = str(item or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _normalize(data: dict[str, Any] | None) -> dict[str, Any]:
    """DB 无 key → 内置默认；有 key → 用已存列表（可为空，保留管理员显式清空）。"""
    if data is None:
        return {"ipv4_urls": list(DEFAULT_IPV4_URLS), "ipv6_urls": list(DEFAULT_IPV6_URLS)}
    raw = data if isinstance(data, dict) else {}
    return {
        "ipv4_urls": _normalize_url_list(raw.get("ipv4_urls")),
        "ipv6_urls": _normalize_url_list(raw.get("ipv6_urls")),
    }


async def _read_config_blob_async_impl() -> dict[str, Any]:
    try:
        from config import CONFIG_STORE

        if hasattr(CONFIG_STORE, "read_main_async"):
            data = await CONFIG_STORE.read_main_async()
        else:
            data = CONFIG_STORE.read_main()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


async def get_node_ip_config_async() -> dict[str, Any]:
    """DB 里保存的节点 IP 备份（未配置时返回内置默认列表）。"""
    blob = await _read_config_blob_async_impl()
    return _normalize(blob.get(_CONFIG_KEY))


async def update_node_ip_config(patch: dict[str, Any]) -> dict[str, Any]:
    """部分更新：``ipv4_urls`` / ``ipv6_urls`` 各自 if-in-patch 全量替换，未传的不动。"""
    current = await get_node_ip_config_async()
    merged = deepcopy(current)
    if "ipv4_urls" in patch:
        merged["ipv4_urls"] = _normalize_url_list(patch.get("ipv4_urls"))
    if "ipv6_urls" in patch:
        merged["ipv6_urls"] = _normalize_url_list(patch.get("ipv6_urls"))

    blob = await _read_config_blob_async_impl()
    blob[_CONFIG_KEY] = merged

    from config import CONFIG_STORE

    if hasattr(CONFIG_STORE, "write_main_async"):
        await CONFIG_STORE.write_main_async(blob)
    else:
        CONFIG_STORE.write_main(blob)
    return merged


def public_view(config: dict[str, Any]) -> dict[str, Any]:
    """管理端读形态：备份列表 + 内置默认列表（供前端占位/首次预填）。"""
    return {
        "ipv4_urls": list(config.get("ipv4_urls") or []),
        "ipv6_urls": list(config.get("ipv6_urls") or []),
        "default_ipv4_urls": list(DEFAULT_IPV4_URLS),
        "default_ipv6_urls": list(DEFAULT_IPV6_URLS),
    }
