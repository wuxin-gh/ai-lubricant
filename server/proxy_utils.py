"""代理池配置到运行时代理 URL 的纯转换工具。"""
from __future__ import annotations

from urllib.parse import quote, urlsplit, urlunsplit

# 代理模式：network=传统 CONNECT/forward 网络代理（aiohttp proxy=）；
# url_prefix=URL 前缀转发，把上游完整绝对 URL 拼到前缀后面（如 CF Workers 反代）；
# direct=强制直连，显式表示“此绑定不走任何代理”（运行时 proxy 与 url_prefix 均为 None）。
PROXY_MODE_NETWORK = "network"
PROXY_MODE_URL_PREFIX = "url_prefix"
PROXY_MODE_DIRECT = "direct"
_VALID_MODES = {PROXY_MODE_NETWORK, PROXY_MODE_URL_PREFIX, PROXY_MODE_DIRECT}


def canonical_proxy(value: str | None) -> str | None:
    """统一运行时“无代理”表示，避免 None/空字符串被误判为变更。"""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def canonical_url_prefix(value: str | None) -> str | None:
    """规范化 URL 前缀基址：去首尾空白、去尾部 `/`。"""
    if not isinstance(value, str):
        return None
    value = value.strip().rstrip("/")
    return value or None


def proxy_mode(proxy: dict | None) -> str:
    """返回代理条目模式，缺省/非法回退为 network（向后兼容）。"""
    if not isinstance(proxy, dict):
        return PROXY_MODE_NETWORK
    mode = str(proxy.get("mode") or "").strip().lower()
    return mode if mode in _VALID_MODES else PROXY_MODE_NETWORK


def proxy_prefix_base(proxy: dict | None) -> str | None:
    """url_prefix 模式返回规范化前缀基址；其它模式返回 None。"""
    if proxy_mode(proxy) != PROXY_MODE_URL_PREFIX:
        return None
    return canonical_url_prefix(proxy.get("url"))


def proxy_effective_url(proxy: dict | None) -> str | None:
    """network 模式的有效 aiohttp 代理 URL（含认证注入）。

    url_prefix 模式不适用（前缀基址会成为请求目标，不能注入 userinfo）；
    direct 模式表示强制直连，同样返回 None。
    """
    if not proxy:
        return None
    if proxy_mode(proxy) != PROXY_MODE_NETWORK:
        # 仅 network 模式走 aiohttp proxy=；url_prefix/direct 运行时代理字段置空。
        return None
    url = canonical_proxy(proxy.get("url"))
    username = (proxy.get("username") or "").strip()
    password = proxy.get("password") or ""
    if not url or not username:
        return url
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return url
    host = parts.hostname or ""
    if not host:
        return url
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host = f"{host}:{parts.port}"
    auth = quote(username, safe="")
    if password:
        auth += ":" + quote(str(password), safe="")
    return urlunsplit((parts.scheme, f"{auth}@{host}", parts.path, parts.query, parts.fragment))


def find_proxy(proxies: list[dict], ref: str | None) -> dict | None:
    value = canonical_proxy(ref)
    if not value:
        return None
    for proxy in proxies or []:
        if value in {proxy.get("id"), proxy.get("name"), proxy.get("url")}:
            return proxy
    return None


def resolve_account_proxy(account: dict, proxies: list[dict]) -> str | None:
    """解析账号的有效 network 代理 URL（aiohttp proxy= 用）。

    - proxy_id 命中代理池：按条目模式返回（network→有效代理 URL；url_prefix→None）。
    - 旧字面量 proxy 不在池中：按 network 语义原样返回（不视作前缀），保持兼容。
    """
    proxy_id = canonical_proxy(account.get("proxy_id"))
    if proxy_id:
        return proxy_effective_url(find_proxy(proxies, proxy_id))

    legacy_proxy = canonical_proxy(account.get("proxy"))
    proxy = find_proxy(proxies, legacy_proxy)
    if proxy:
        return proxy_effective_url(proxy)
    return legacy_proxy


def resolve_account_url_prefix(account: dict, proxies: list[dict]) -> str | None:
    """解析账号的 URL 前缀基址；仅当绑定条目为 url_prefix 模式时返回前缀。"""
    proxy_id = canonical_proxy(account.get("proxy_id"))
    if proxy_id:
        return proxy_prefix_base(find_proxy(proxies, proxy_id))
    # 旧字面量 proxy 视作 network 语义，不产生前缀。
    return None


def resolve_account_proxy_config_id(account: dict, proxies: list[dict]) -> str:
    """解析账号绑定的代理池条目 id（ProxyManager 路由用）。

    - proxy_id 命中池：返回该条目 id。
    - 旧字面量 proxy 命中池：返回命中条目 id（兼容）。
    - 无绑定 / 未命中：返回空串 -> ProxyManager 走隐式直连。
    """
    proxy_id = canonical_proxy(account.get("proxy_id"))
    if proxy_id:
        proxy = find_proxy(proxies, proxy_id)
        if proxy is not None:
            return str(proxy.get("id") or "")
        return ""
    legacy_proxy = canonical_proxy(account.get("proxy"))
    proxy = find_proxy(proxies, legacy_proxy)
    if proxy is not None:
        return str(proxy.get("id") or "")
    return ""


def to_manager_config(proxy: dict) -> dict:
    """把代理池条目标准化成 ProxyManager 认的 config。

    ProxyManager 按 mode 分叉：
    - network：需要 proxy_url（含认证注入，走 aiohttp proxy=）
    - url_prefix：需要 url_prefix（前缀基址）
    - direct：直连，无额外字段
    - node：需要 node_id（走节点隧道）

    version 交给 ProxyManager 的 update_proxy_config 兜底（内容变自动递增）。
    """
    mode = proxy_mode(proxy)  # network/url_prefix/direct（缺省 network）
    raw_mode = str(proxy.get("mode") or "").strip().lower()
    if raw_mode == "node":
        mode = "node"
    cfg: dict = {
        "id": str(proxy.get("id") or ""),
        "mode": mode,
    }
    if mode == "network":
        cfg["proxy_url"] = proxy_effective_url(proxy)
    elif mode == "url_prefix":
        cfg["url_prefix"] = canonical_url_prefix(proxy.get("url"))
    elif mode == "node":
        cfg["node_id"] = str(proxy.get("node_id") or "")
    return cfg


def runtime_accounts(accounts: list[dict], proxies: list[dict]) -> list[dict]:
    result = []
    for account in accounts or []:
        item = dict(account)
        item["proxy"] = resolve_account_proxy(item, proxies)
        item["url_prefix"] = resolve_account_url_prefix(item, proxies)
        item["proxy_config_id"] = resolve_account_proxy_config_id(item, proxies)
        result.append(item)
    return result


async def sync_proxy_manager_configs(proxies: list[dict]) -> None:
    """把代理池全量喂给共享 ProxyManager：provider 出站按 proxy_config_id 路由，
    这些配置就是路由依据。update_proxy_config 内容变自动递增 version、触发旧实例
    重建，故热更新（改代理 url/密码/模式）直接再调本函数即可及时生效。

    本函数幂等、无副作用地收敛本实例的 ProxyManager 配置缓存：
    - 全量列表中的条目：upsert（update_proxy_config 兜底 version）。
    - 不在全量列表中的旧 id：从缓存剪掉，并 on_proxy_config_changed 关掉其残留
      RequestInstance（改/删代理后旧连接立即失效，不等空闲超时）。

    调用点：启动全量加载（main.py）、代理池事件 + 60s 对账
    （runtime_handlers._refresh_proxy_runtime）、管理端改代理池（admin.update_proxies）。
    """
    from providers.proxy_manager import get_proxy_manager  # 局部导入避开循环依赖

    manager = get_proxy_manager()
    keep_ids: set[str] = set()
    for proxy in proxies or []:
        cfg = to_manager_config(proxy)
        if cfg.get("id"):
            keep_ids.add(cfg["id"])
            manager.update_proxy_config(cfg)
    # 剪掉已被删除的代理条目：否则账号解绑后其旧 id 仍留在 _proxy_configs 里，
    # _current_config 会命中旧配置而非降级直连，导致「删了代理但出站仍走旧代理」。
    stale_ids = [pid for pid in list(manager._proxy_configs.keys()) if pid not in keep_ids]
    for pid in stale_ids:
        manager._proxy_configs.pop(pid, None)
        await manager.on_proxy_config_changed(pid)
