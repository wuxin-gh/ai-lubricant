"""出站代理资源管理器：统一管理 session、代理、连接的生命周期。

外部只需调用 ProxyManager.request(url, proxy_config_id, ...)。
内部负责：实例创建、复用、回收、清理。

设计要点：
- 相同 (proxy_config_id, target_host) 的请求共用一个实例（连接复用）
- 关闭实例 != 关闭 session：session 在请求完成且实例被标记关闭后才关
- 空闲超时（IDLE_TIMEOUT）后主动回收，避免被上游当攻击
- node 模式是 request() 的一个分支，不需要额外的类层次
"""
from __future__ import annotations

import asyncio
import time
from contextvars import ContextVar
from typing import Any, AsyncIterator, Callable, Optional
from urllib.parse import urlparse, urlunparse

import aiohttp
from loguru import logger

from .base import (
    apply_url_prefix,
    install_url_prefix_interceptor,
    make_insecure_connector,
)

# 空闲超时：实例多久没被使用 -> 回收（应小于上游 keep-alive 超时）
IDLE_TIMEOUT = 15  # 秒

# 连接级 keep-alive：单条连接空闲多久 -> 关。刻意远小于常见上游/代理的 idle
# timeout（30~60s），保证【我们这侧永远先超时、主动发 FIN 干净关闭】，而不是把
# 空闲连接留到被对端 RST——后者在 Windows Proactor 上会冒出 _call_connection_lost
# 的 WinError 10054 清理噪声，且从上游视角看是一堆异常重置的复用连接（像被攻击）。
# 不取更小值是为了避免走向另一个极端：keepalive 太短 -> 每次请求都重建连接 + TLS
# 握手，握手风暴同样会被上游当扫描。8s 卡在“稳定先于上游超时”和“保留合理复用”之间。
KEEPALIVE_TIMEOUT = 8  # 秒


# 本次出站的实际代理决策（task 级）。由 ProxyManager.request() 在真正发请求处写入，
# 供请求日志 finalize 时读取——即"代理参数在请求方法内写入"，天然与实际路由一致，
# 不从渠道/账号上层的 proxy_config_id 推断（那只是配置引用，可能与实际不符）。
_last_outbound_proxy: ContextVar[dict | None] = ContextVar("_last_outbound_proxy", default=None)


def _mask_proxy_url(url: str | None) -> str:
    """脱敏代理 URL：保留 scheme://user:***@host:port，隐藏密码。空/异常时原样返回。"""
    if not url:
        return ""
    try:
        parsed = urlparse(str(url))
        if not parsed.password:
            return str(url)
        host = parsed.hostname or ""
        netloc = host
        if parsed.port:
            netloc = f"{host}:{parsed.port}"
        if parsed.username:
            netloc = f"{parsed.username}:***@{netloc}"
        return urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        return str(url)


def describe_and_record(config: dict | None, target_host: str | None) -> None:
    """由当前代理配置生成脱敏描述并写入 ContextVar（在真正出站前调用）。"""
    cfg = config or {}
    mode = cfg.get("mode", "network")
    info: dict = {
        "mode": mode,
        "proxy_config_id": cfg.get("id") or "",
        "target_host": target_host or "",
    }
    if mode == "network":
        info["proxy_url"] = _mask_proxy_url(cfg.get("proxy_url"))
    elif mode == "url_prefix":
        info["url_prefix"] = cfg.get("url_prefix") or ""
    elif mode == "node":
        info["node_id"] = cfg.get("node_id") or ""
    _last_outbound_proxy.set(info)


def take_outbound_proxy() -> dict | None:
    """读取本 task 最近一次出站的代理决策（不清空）。"""
    return _last_outbound_proxy.get()


def reset_outbound_proxy() -> None:
    """重置代理决策，避免上一 attempt 残留污染当前 attempt。"""
    _last_outbound_proxy.set(None)


class OutboundResponse:
    """统一的响应包装，是 aiohttp 响应的忠实替身，兼容 session 和 node 隧道两种底层。

    provider 代码只依赖这个接口，不关心底层是 aiohttp session 还是 node tunnel。
    覆盖 provider 实际用到的全部 aiohttp 响应 API：
      属性  .status / .headers / .content / .charset / .cookies
      方法  .iter_any() / .read() / .text() / .json()
    其中 .content 返回 self（self 自身提供 iter_any），对齐 `resp.content.iter_any()`。
    """

    def __init__(self, status: int, headers: dict, body_iter: AsyncIterator[bytes],
                 closer: Optional[callable] = None, cookies=None):
        self.status = status
        self.headers = headers
        self._body_iter = body_iter
        self._closer = closer
        self._body_cache: Optional[bytes] = None  # read()/text()/json() 缓存，允许重复调用
        # cookies 透传源：aiohttp 的 resp.cookies 已正确解析所有 Set-Cookie 头（可多条）。
        # dict(resp.headers) 会把同名多条 Set-Cookie 塌成一条 → 从 headers 重解析会丢
        # cookie（eaichat 取 2 个 cookie、qwen 遍历全部 cookie 都会因此坏）。故优先透传。
        self._cookies = cookies

    @property
    def content(self):
        """返回一个支持 iter_any() 的对象（对齐 aiohttp 响应用法）。"""
        return self

    @property
    def charset(self) -> Optional[str]:
        """从 Content-Type 解析字符集（对齐 aiohttp resp.charset）。"""
        ctype = ""
        for k, v in (self.headers or {}).items():
            if str(k).lower() == "content-type":
                ctype = str(v)
                break
        for part in ctype.split(";"):
            part = part.strip()
            if part.lower().startswith("charset="):
                return part[len("charset="):].strip().strip('"') or None
        return None

    @property
    def cookies(self):
        """对齐 aiohttp resp.cookies。优先透传底层 aiohttp 已解析好的 cookies（能
        正确保留同名多条 Set-Cookie）；没有透传源时才从 headers 兜底解析（node 模式
        等场景，headers 是普通 dict，同名 Set-Cookie 已在源头塌陷，只能尽力而为）。"""
        if self._cookies is not None:
            return self._cookies
        from http.cookies import SimpleCookie
        jar = SimpleCookie()
        for k, v in (self.headers or {}).items():
            if str(k).lower() == "set-cookie":
                try:
                    jar.load(str(v))
                except Exception:
                    pass
        return jar

    async def iter_any(self) -> AsyncIterator[bytes]:
        """流式迭代响应体（SSE 场景用）。与 read/text/json 互斥：流式场景不要再 read。"""
        async for chunk in self._body_iter:
            yield chunk

    async def read(self) -> bytes:
        """一次性读取完整响应体（非流式场景用）。可重复调用（结果缓存）。"""
        if self._body_cache is None:
            chunks = []
            async for chunk in self._body_iter:
                chunks.append(chunk)
            self._body_cache = b"".join(chunks)
        return self._body_cache

    async def text(self, encoding: Optional[str] = None) -> str:
        """读取并按字符集解码为文本（对齐 aiohttp resp.text()）。"""
        body = await self.read()
        enc = encoding or self.charset or "utf-8"
        return body.decode(enc, errors="replace")

    async def json(self, *, loads=None):
        """读取并解析 JSON（对齐 aiohttp resp.json()，但不强校验 content-type）。"""
        import json as _json
        body = await self.read()
        parse = loads or _json.loads
        return parse(body.decode(self.charset or "utf-8", errors="replace"))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._closer:
            await self._closer()
        return False


class RequestInstance:
    """一个请求实例：封装 session + 代理配置 + 连接。

    每个实例对应一个 (proxy_config_id, target_host) 组合。
    相同代理配置 + 相同目标 host 的请求复用同一个实例。

    生命周期由 ProxyManager 管理，自身只负责状态变量和请求执行。
    """

    def __init__(self, proxy_config: dict, target_host: str,
                 node_manager: Optional["NodeConnectManager"] = None,
                 on_done: Optional[callable] = None):
        self._proxy_config = proxy_config
        self._target_host = target_host
        self._node_manager = node_manager
        self._session: Optional[aiohttp.ClientSession] = None
        self._in_flight: int = 0          # 当前进行中的请求数
        self._closing: bool = False       # 是否已标记关闭（已从池移除）
        self._last_used: float = time.monotonic()
        self._config_version: int = proxy_config.get("version", 0)
        # 请求完成回调（ProxyManager 注入 cleanup）：每次请求收尾后清理一遍池子，
        # 及时回收改 url/改代理产生的孤儿实例，无需依赖外部定时器。
        self._on_done = on_done

    @property
    def key(self) -> tuple:
        return (self._proxy_config.get("id"), self._target_host)

    def stale_against(self, current_config: Optional[dict]) -> bool:
        """配置是否已变更（对比 manager 当前的配置 version）。"""
        if current_config is None:
            return True
        return current_config.get("version", 0) != self._config_version

    def is_expired(self) -> bool:
        """是否空闲超时（可被回收）。"""
        return self._in_flight == 0 and (time.monotonic() - self._last_used) > IDLE_TIMEOUT

    def is_unhealthy(self) -> bool:
        """连接是否不健康（session 已关等）。"""
        if self._session is not None and self._session.closed:
            return True
        return False

    @property
    def mode(self) -> str:
        return self._proxy_config.get("mode", "network")

    @property
    def url_prefix(self) -> Optional[str]:
        """url_prefix 模式的前缀基址。install_url_prefix_interceptor 在每次出站
        请求时实时读 owner.url_prefix；RequestInstance 作为 owner 必须暴露此属性，
        否则拦截器 getattr 恒得 None、前缀改写静默失效。"""
        return self._proxy_config.get("url_prefix")

    async def request(self, *, url: str, method: str = "GET",
                      headers: Optional[dict] = None, timeout: Optional[aiohttp.ClientTimeout] = None,
                      ssl: bool = False, **kwargs) -> OutboundResponse:
        """用本实例发送请求。返回的 OutboundResponse 关闭时会触发 _on_request_done。"""
        self._in_flight += 1
        self._last_used = time.monotonic()
        try:
            if self.mode == "node":
                resp = await self._request_via_node(
                    url=url, method=method, headers=headers, timeout=timeout, ssl=ssl, **kwargs,
                )
            else:
                resp = await self._request_via_session(
                    url=url, method=method, headers=headers, timeout=timeout, ssl=ssl, **kwargs,
                )
            # 包一层，让响应关闭时走 _on_request_done。cookies 必须透传：内层已从
            # aiohttp resp.cookies 正确解析所有 Set-Cookie，外层若不带则 .cookies 会
            # 退回从塌陷 headers 重解析、丢 cookie（eaichat/qwen 登录态依赖它）。
            return OutboundResponse(
                status=resp.status,
                headers=resp.headers,
                body_iter=resp.iter_any(),
                closer=self._on_request_done,
                cookies=resp.cookies,
            )
        except BaseException:
            await self._on_request_done()
            raise

    async def _on_request_done(self):
        """请求完成时的钩子：更新计数，判断是否需要关 session，再触发池清理。"""
        self._in_flight = max(0, self._in_flight - 1)
        self._last_used = time.monotonic()
        if self._closing and self._in_flight == 0:
            await self._close_resources()
        # 每次请求收尾清理一遍池子（回收改 url/改代理产生的孤儿实例）。
        # 清理异常绝不能冒泡到请求路径，吞掉即可。
        if self._on_done is not None:
            try:
                await self._on_done()
            except Exception as exc:
                logger.debug(f"[ProxyManager] on_done cleanup failed: {exc}")

    async def mark_closing(self):
        """标记关闭（由 ProxyManager 调用）。"""
        self._closing = True
        if self._in_flight == 0:
            await self._close_resources()

    async def _close_resources(self):
        """真正关闭底层资源（session / tunnel）。"""
        if self._session is not None and not self._session.closed:
            try:
                await self._session.close()
            except Exception as exc:
                logger.debug(f"[ProxyManager] close session failed: {exc}")
            self._session = None
        # node 模式的 tunnel 关闭由 _request_via_node 内部管理（请求级）

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建 session（连接复用）。"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=make_insecure_connector(
                    keepalive_timeout=KEEPALIVE_TIMEOUT,
                ),
                timeout=aiohttp.ClientTimeout(total=None, sock_read=120),
            )
            # url_prefix 模式需要装 URL 改写拦截器
            if self.mode == "url_prefix":
                self._session = install_url_prefix_interceptor(self._session, self)
        return self._session

    async def _request_via_session(self, *, url: str, method: str,
                                   headers: Optional[dict], timeout: Optional[aiohttp.ClientTimeout],
                                   ssl: bool, **kwargs) -> OutboundResponse:
        """network / url_prefix / direct 模式：走 aiohttp session。"""
        session = await self._get_session()
        # url_prefix 改写（拦截器实时读 self._proxy_config["url_prefix"]）
        if self.mode == "url_prefix":
            # install_url_prefix_interceptor 内部会读 url_prefix，这里只需保证字段存在
            pass
        # network 模式传 proxy= 参数
        proxy = self._proxy_config.get("proxy_url") if self.mode == "network" else None
        req_ctx = session.request(
            method, url,
            headers=headers,
            proxy=proxy,
            ssl=ssl,
            timeout=timeout,
            **kwargs,
        )
        resp = await req_ctx.__aenter__()
        # 把 aiohttp 响应转成 OutboundResponse，响应流关闭时一并关闭请求上下文
        return _AiohttpOutboundResponse(resp, req_ctx)

    async def _request_via_node(self, *, url: str, method: str,
                                headers: Optional[dict], timeout: Optional[aiohttp.ClientTimeout],
                                ssl: bool, **kwargs) -> OutboundResponse:
        """node 模式：把请求描述下发给节点，节点用自己的 HTTP client 发出，
        流式回响应。节点是纯 I/O 中继（含 TLS 终止），不处理业务。"""
        if self._node_manager is None:
            raise RuntimeError("node mode requires NodeConnectManager")

        body = kwargs.get("data") or kwargs.get("json") or b""
        if isinstance(body, str):
            body = body.encode("utf-8")
        elif isinstance(body, dict):
            import json
            body = json.dumps(body).encode("utf-8")

        try:
            proxy_resp = await self._node_manager.request(
                self._proxy_config["node_id"],
                method=method,
                url=url,
                headers=headers,
                body=body,
            )
        except Exception:
            raise

        return _NodeOutboundResponse(proxy_resp)


class _NodeOutboundResponse(OutboundResponse):
    """包装 NodeProxyResponse 为 OutboundResponse。响应流关闭时关 tunnel。"""

    def __init__(self, proxy_resp: "Any"):
        self._proxy_resp = proxy_resp
        self._closed = False
        super().__init__(
            status=proxy_resp.status,
            headers=proxy_resp.headers,
            body_iter=self._iter_chunks(),
            closer=self._close,
        )

    async def _iter_chunks(self):
        try:
            async for chunk in self._proxy_resp.iter_chunks():
                yield chunk
        finally:
            await self._close()

    async def _close(self):
        if self._closed:
            return
        self._closed = True
        try:
            await self._proxy_resp.close()
        except Exception as exc:
            logger.debug(f"[ProxyManager] close proxy tunnel failed: {exc}")


class _AiohttpOutboundResponse(OutboundResponse):
    """包装 aiohttp 响应：响应流关闭时关闭请求上下文。"""

    def __init__(self, resp: aiohttp.ClientResponse, req_ctx):
        self._resp = resp
        self._req_ctx = req_ctx
        self._closed = False
        super().__init__(
            status=resp.status,
            headers=dict(resp.headers),
            body_iter=self._iter_bytes(),
            closer=self._close,
            # aiohttp 已正确解析所有 Set-Cookie（可多条）；直接透传，避免 dict(headers)
            # 塌陷多条 Set-Cookie 导致 eaichat/qwen 取 cookie 丢失。
            cookies=resp.cookies,
        )

    async def _iter_bytes(self):
        try:
            async for chunk in self._resp.content.iter_any():
                yield chunk
        finally:
            await self._close()

    async def _close(self):
        if self._closed:
            return
        self._closed = True
        try:
            await self._req_ctx.__aexit__(None, None, None)
        except Exception as exc:
            logger.debug(f"[ProxyManager] close request context failed: {exc}")


def _cache_key(account_id: str, proxy_config_id: str, target_host: str) -> tuple:
    # account_id 为空串 -> 按 (proxy, host) 共用实例（无登录态渠道，如自定义渠道）；
    # account_id 非空 -> 该账号在此 (proxy, host) 上独占实例/session，避免有登录态
    #   渠道共用一条出网连接被上游当同源检测。
    return (account_id, proxy_config_id, target_host)


class ProxyManager:
    """出站代理资源管理器：统一管理所有实例。

    外部只需调用 request(url, proxy_config_id, ...)。
    """

    def __init__(self, node_manager: Optional["NodeConnectManager"] = None):
        # 实例池：key = (proxy_config_id, target_host) -> RequestInstance
        self._instances: dict[tuple, RequestInstance] = {}
        # 代理配置缓存：proxy_config_id -> config（含 version）
        self._proxy_configs: dict[str, dict] = {}
        # node 模式管理器（可选）
        self._node_manager = node_manager
        self._lock = asyncio.Lock()

    def update_proxy_config(self, proxy_config: dict):
        """更新代理配置缓存（运行时调用）。

        version 是实例失效的唯一判据（stale_against 比它）。为避免"改了 url/密码
        但没递增 version → 旧实例继续用旧凭据"这个坑，这里做兜底：内容（除 version
        外的字段）变了、而调用方没自己递增 version 时，自动递增。这样接线层即使只调
        update_proxy_config、不调 on_proxy_config_changed，改配置也能及时触发重建。
        显式递增 version 的调用方语义不变。
        """
        config_id = proxy_config.get("id")
        if not config_id:
            return
        new_cfg = dict(proxy_config)
        prev = self._proxy_configs.get(config_id)
        if prev is not None:
            def _content(c: dict) -> dict:
                return {k: v for k, v in c.items() if k != "version"}
            content_changed = _content(prev) != _content(new_cfg)
            version_bumped = new_cfg.get("version", 0) != prev.get("version", 0)
            if content_changed and not version_bumped:
                new_cfg["version"] = prev.get("version", 0) + 1
        self._proxy_configs[config_id] = new_cfg

    def _current_config(self, proxy_config_id: str) -> Optional[dict]:
        """按 id 取代理配置。空 id / 查不到 -> 返回隐式直连配置。

        全量收口后 99% 的账号没绑代理（proxy_config_id 为空），这些必须走直连而
        不是报错。查不到的 id（配置被删/尚未同步）同样降级为直连，绝不因路由缺失
        阻断出站——路由缺失只该回退直连，不该让请求失败。
        """
        cfg = self._proxy_configs.get(proxy_config_id)
        if cfg is not None:
            return cfg
        return {"id": proxy_config_id or "", "mode": "direct", "version": 0}

    async def request(self, *, url: str, proxy_config_id: Optional[str] = None,
                      account_id: Optional[str] = None,
                      method: str = "GET", headers: Optional[dict] = None,
                      timeout: Optional[aiohttp.ClientTimeout] = None,
                      ssl: bool = False, **kwargs) -> OutboundResponse:
        """外部唯一入口：用指定代理配置请求指定 URL。

        proxy_config_id 为空 -> 隐式直连（不走任何代理）。这样 provider 全量收口到
        本方法后，无代理账号无需特殊分支。

        account_id 非空 -> 该账号在 (proxy, host) 上独占实例/session（有登录态渠道，
        避免共用出网连接被上游当同源）；为空 -> 按 (proxy, host) 共用（自定义渠道）。"""
        proxy_config_id = proxy_config_id or ""
        account_id = account_id or ""
        parsed = urlparse(url)
        target_host = parsed.hostname
        key = _cache_key(account_id, proxy_config_id, target_host)

        instance = self._instances.get(key)
        current_config = self._current_config(proxy_config_id)

        # 实例不存在 / 已标记关闭 / 配置已变更 -> 新建
        if instance is None or instance._closing or instance.stale_against(current_config):
            if instance is not None:
                await instance.mark_closing()
            instance = RequestInstance(
                current_config, target_host, self._node_manager,
                on_done=self.cleanup,
            )
            self._instances[key] = instance

        # 真正出站前记录本次代理决策（请求日志据此展示"是否走代理/走错代理"）。
        describe_and_record(current_config, target_host)
        return await instance.request(
            url=url, method=method, headers=headers,
            timeout=timeout, ssl=ssl, **kwargs,
        )

    async def on_proxy_config_changed(self, proxy_config_id: str):
        """代理配置变更：删除所有使用该配置的实例（下次请求自动重建）。"""
        to_remove = [k for k in self._instances if k[1] == proxy_config_id]
        for k in to_remove:
            inst = self._instances.pop(k, None)
            if inst is not None:
                await inst.mark_closing()

    async def cleanup(self):
        """定期清理：删除空闲超时/不健康实例。"""
        to_remove = [k for k, inst in self._instances.items()
                     if inst.is_expired() or inst.is_unhealthy()]
        for k in to_remove:
            inst = self._instances.pop(k, None)
            if inst is not None:
                await inst.mark_closing()

    async def close_all(self):
        """应用关闭：清理所有实例。"""
        for inst in list(self._instances.values()):
            await inst.mark_closing()
        self._instances.clear()

    def session(self, *, account_id: str = "",
                proxy_config_id: "str | Callable[[], str] | None" = None,
                default_timeout: Optional[aiohttp.ClientTimeout] = None) -> "ProxyManagerSession":
        """返回一个 quack 得像 aiohttp.ClientSession 的 facade，绑定到指定账号 +
        代理配置。provider 的 _make_session 交出它，出站即统一收口到本管理器。

        proxy_config_id 可传字符串（快照，向后兼容）或一个无参 callable（每次出站
        实时求值）。Provider 长期缓存本 facade，账号换绑代理后，callable 形式能让
        下一次请求立即按新 id 路由，无需任何入口手动清 provider._session。

        default_timeout：承载原 session 级默认超时——很多调用点 session.get(url) 不带
        timeout，靠的就是建 session 时设的默认值；facade 每请求独立，必须把它兜上，
        否则这些请求丢超时（尤其 CustomProvider 的 total 上限）。"""
        return ProxyManagerSession(
            self, account_id=account_id or "", proxy_config_id=proxy_config_id,
            default_timeout=default_timeout,
        )


class _OutboundRequestContext:
    """复刻 aiohttp `session.get(url)` 的双语义：返回值既能 `await`、又能 `async with`。

    aiohttp 的 session.get/post/request 返回 `_RequestContextManager`，同时支持
    `resp = await session.get(url)` 和 `async with session.get(url) as resp:` 两种写法。
    facade 收口后各 provider 的调用点原样保留，故必须复刻这个双语义，否则会崩。

    包一个"生成 OutboundResponse 的协程工厂"，await/aenter 时才真正发请求。
    """

    __slots__ = ("_coro_factory", "_resp")

    def __init__(self, coro_factory):
        self._coro_factory = coro_factory
        self._resp: Optional[OutboundResponse] = None

    def __await__(self):
        # `resp = await session.get(...)`：调用方自己负责关闭（读完 read()/文本后
        # aiohttp 语义下连接归还；我们的 OutboundResponse.read() 会走到底触发关闭）。
        return self._coro_factory().__await__()

    async def __aenter__(self) -> OutboundResponse:
        self._resp = await self._coro_factory()
        return self._resp

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._resp is not None:
            return await self._resp.__aexit__(exc_type, exc_val, exc_tb)
        return False


class ProxyManagerSession:
    """出站 session facade：get/post/request 委托给 ProxyManager，按
    (account_id, proxy_config_id) 路由四种模式（network/url_prefix/direct/node）。

    - 忽略调用点传入的 `proxy=` kwarg：路由改由代理配置决定，历史遗留的
      `proxy=self.proxy` 自动变惰性参数，调用点一行不用动。
    - 底层真实 aiohttp session 由 ProxyManager 的实例池持有/复用；facade 自身
      无连接，close()/__aexit__ 是 no-op（绝不能关掉池里的复用 session）。
    - proxy_config_id 支持传 callable：provider 长期缓存本 facade，账号换绑代理后
      下一次请求实时读新值，无需任何入口手动清 provider._session（详见 _resolve_proxy_config_id）。
    """

    def __init__(self, manager: "ProxyManager", *, account_id: str,
                 proxy_config_id: "str | Callable[[], str] | None",
                 default_timeout: Optional[aiohttp.ClientTimeout] = None):
        self._manager = manager
        self._account_id = account_id
        # 字符串：快照语义（向后兼容，测试/直连场景一行不用改）。
        # callable：每次出站实时求值，是账号换绑代理立即生效的关键——provider.proxy_config_id
        # 为账号运行态出站绑定的唯一真相，facade 每请求读它，热更新后下一请求即按新 id 路由。
        self._proxy_config_id = proxy_config_id
        # session 级默认超时：很多调用点 session.get(url) 不带 timeout，靠 _make_session
        # 建 session 时设的默认。facade 每请求独立，须把它补上，否则这些请求丢超时
        # （尤其 CustomProvider 的 total 上限）。调用点显式传 timeout 时以显式为准。
        self._default_timeout = default_timeout
        self.closed = False

    def _resolve_proxy_config_id(self) -> str:
        """解析本次出站应使用的 proxy_config_id。

        callable 形式实时读账号当前绑定（provider.proxy_config_id）；解析失败/None
        一律回退空串——空串 = 隐式直连，与 _current_config 的降级语义一致，绝不因
        解析失败阻断出站。这与 RequestInstance.url_prefix 「每次出站实时读 owner」
        是同一类问题的同一种解法。
        """
        pid = self._proxy_config_id
        if callable(pid):
            try:
                return pid() or ""
            except Exception:
                return ""
        return pid or ""

    def request(self, method: str, url: str, **kwargs) -> _OutboundRequestContext:
        kwargs.pop("proxy", None)  # 路由由配置决定，吞掉历史 proxy= 参数
        if kwargs.get("timeout") is None and self._default_timeout is not None:
            kwargs["timeout"] = self._default_timeout
        method_up = str(method).upper()

        def _factory():
            return self._manager.request(
                url=str(url),
                method=method_up,
                account_id=self._account_id,
                proxy_config_id=self._resolve_proxy_config_id(),
                **kwargs,
            )

        return _OutboundRequestContext(_factory)

    def get(self, url: str, **kwargs) -> _OutboundRequestContext:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> _OutboundRequestContext:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> _OutboundRequestContext:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs) -> _OutboundRequestContext:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs) -> _OutboundRequestContext:
        return self.request("DELETE", url, **kwargs)

    def head(self, url: str, **kwargs) -> _OutboundRequestContext:
        return self.request("HEAD", url, **kwargs)

    async def close(self):
        # facade 无自有连接；池里的复用 session 由 ProxyManager 管，不在此关。
        self.closed = True

    async def __aenter__(self) -> "ProxyManagerSession":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False


# ── 模块级共享单例 ────────────────────────────────────────────────────────────
# provider 全量收口到同一个 ProxyManager，才能共享实例池 / 统一热更新。node_manager
# 在运行时（节点服务起来后）注入；未注入时 node 模式会在 request 时报错，其余模式正常。
_shared_manager: Optional[ProxyManager] = None


def get_proxy_manager() -> ProxyManager:
    """取全局共享 ProxyManager（懒创建）。"""
    global _shared_manager
    if _shared_manager is None:
        _shared_manager = ProxyManager()
    return _shared_manager


def set_node_manager(node_manager) -> None:
    """运行时注入 NodeConnectManager（节点服务启动后调用）。"""
    get_proxy_manager()._node_manager = node_manager
