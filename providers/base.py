"""模型提供商基类"""
import asyncio
import inspect
import json
import os
import random
import ssl
import time
import uuid
from abc import ABC, abstractmethod
from fastapi import HTTPException
from typing import AsyncGenerator, Union

import aiohttp
from loguru import logger

from message_utils import (
    parse_sse_event,
    parse_sse_data_value,
    make_openai_chunk,
    make_openai_response,
    generate_completion_id,
    combine_messages,
    MESSAGE_TAG_GUIDE,
    convert_stream_to_responses,
    openai_to_responses_response,
    iter_sse_payloads,
    openai_content_to_text,
    normalize_tool_call_arguments,
)
from usage_utils import merge_usage, normalize_usage, estimate_usage, fill_usage_with_estimate
from tool_utils import (
    generate_tools_prompt,
    parse_tool_calls_from_content,
    extract_complete_tool_calls,
    find_tool_call_start,
    hold_partial_tool_marker_index,
    render_tool_call_json,
    render_tool_result_json,
    render_tool_call_xml,
    render_tool_result_xml,
    render_tool_call_hermes,
    render_tool_result_hermes,
)
from limits import ProviderLimitState
from channel import resolve_upstream_stream

# 客户端原始 system 与工具调用转换规则之间的分隔线。合并成一条 system 时用它划界，
# 让模型看清「前面是角色设定 / 后面是工具调用规则」，而不是把两段读成一段。
_TOOLS_PROMPT_SEPARATOR = "---"


def _usage_has_nonzero_completion(usage: dict | None) -> bool:
    """usage 是否带有效输出 token（completion/output）。

    用于流式 usage 输出判定：累计型上游（如 Kimi vLLM）每帧带全量 usage，
    首帧常为 {prompt:X, completion:0}。若仅凭 prompt 非零就视为"已输出 usage"，
    会把首帧零 completion 固化进流并锁死后续累计的 completion，导致日志/DB 记 0。
    因此"是否已发过有效 usage"必须以 completion 非零为准。
    """
    if not isinstance(usage, dict):
        return False
    return normalize_usage(usage)["completion_tokens"] > 0


def _extract_usage_payload(payload: dict) -> dict | None:
    """提取 OpenAI payload 中的 usage（顶层 / response / message / choices[].usage）。"""
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    if isinstance(usage, dict):
        return usage
    for key in ("response", "message"):
        nested = payload.get(key)
        if isinstance(nested, dict) and isinstance(nested.get("usage"), dict):
            return nested["usage"]
    for choice in payload.get("choices") or []:
        if isinstance(choice, dict) and isinstance(choice.get("usage"), dict):
            return choice["usage"]
    return None


def validate_upstream_context_budget(model: str, payload: dict, model_info: dict | None = None) -> None:
    try:
        from main import validate_upstream_context_budget as _validate
    except Exception:
        return
    _validate(model, payload, model_info)


def _connector_env_int(env_name: str, default: int) -> int:
    """读出站连接池相关整型环境变量；缺省或非法回退默认值（与 bootstrap_config._optional_int 同口径）。"""
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        return default
    return value if value > 0 else default


def make_insecure_connector(
    limit: int | None = None,
    limit_per_host: int | None = None,
    keepalive_timeout: float = 8.0,
) -> aiohttp.TCPConnector:
    # 总连接数 / 单 host 连接数接环境变量，默认值保持原行为（100 / 32）。
    # limit_per_host 是更细的脖子：到同一上游域名再多账号也共用此额度，
    # 抗高并发到单一上游时必须一起调大，否则只调 limit 无效。
    if limit is None:
        limit = _connector_env_int("OUTBOUND_CONNECTOR_LIMIT", 100)
    if limit_per_host is None:
        limit_per_host = _connector_env_int("OUTBOUND_CONNECTOR_LIMIT_PER_HOST", 32)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        ssl_context.set_ciphers("DEFAULT:@SECLEVEL=0")
    except ssl.SSLError:
        pass
    # limit_per_host 防止单个热点账号/上游把连接吃爆；keepalive_timeout 让空闲连接
    # 在 8s 后自然回收（连接级空闲回收，不需要后台定时器强关 session）。取值刻意远短于
    # 典型上游/代理的 idle timeout（常见 30-60s），保证【我们这侧永远先超时、主动 FIN
    # 干净关闭】——而不是把空闲连接留到被对端 RST。后者一来在 Windows Proactor 上会冒出
    # _call_connection_lost 的 WinError 10054 清理噪声，二来从上游视角看是一批被异常重置
    # 的复用连接，容易被风控当异常流量。不取更小值是避免走向反面：太短则每请求重建连接 +
    # TLS 握手，握手风暴同样像扫描。
    # enable_cleanup_closed：上游/代理常在自己那侧先关掉 keepalive 空闲连接，池里会残留
    # 被对端 reset 的 socket；开启后 aiohttp 主动清理这类半关闭连接，减少 Windows
    # Proactor 在 _call_connection_lost 里 shutdown 时抛 WinError 10054 的清理噪声。
    return aiohttp.TCPConnector(
        ssl=ssl_context,
        limit=limit,
        limit_per_host=limit_per_host,
        keepalive_timeout=keepalive_timeout,
        enable_cleanup_closed=True,
    )


def apply_url_prefix(url: str, url_prefix: str | None) -> str:
    """URL 前缀转发：把上游完整绝对 URL 拼到前缀基址后面（CF Workers 反代等）。

    - url_prefix 为空 → 原样返回（默认场景零行为变化）；
    - 仅改写绝对 http(s) URL，其它 scheme 原样返回（相对路径由调用方先拼成绝对）；
    - 已带该前缀的 URL 不二次前缀（重试 / 嵌套调用防御）；
    - 原始 URL 的 path/query/编码整体保留，不解析重组。
    """
    prefix = (url_prefix or "").strip().rstrip("/")
    if not prefix:
        return url
    text = str(url or "")
    if not (text.startswith("http://") or text.startswith("https://")):
        return url
    if text.startswith(prefix + "/"):
        return url
    return f"{prefix}/{text}"


def install_url_prefix_interceptor(session: aiohttp.ClientSession, owner) -> aiohttp.ClientSession:
    """在 session 实例上安装 URL 前缀改写拦截：url_prefix 模式下每个出站请求 URL
    被改写为 {prefix}/{原始绝对URL}（CF Workers 反代等）。

    - session.get/post/request 内部都归到私有 _request(method, str_or_url, ...)，
      故只需包裹 _request 一处即覆盖 chat/models/auth/OAuth 全部出站，无需改调用点；
    - owner 为 provider 实例，改写时实时读 owner.url_prefix，使代理池热更新
      （改 provider.url_prefix）对已建的复用 session 立即生效；
    - url_prefix 为空时 apply_url_prefix 恒等，默认场景零行为变化；
    - 不子类化 ClientSession（子类化在类定义期即绑定真实基类，会绕过测试对
      aiohttp.ClientSession 的 patch，且 3.11 起该继承被官方标注 discouraged）。
    """
    # 真实 aiohttp：get/post/request 都归到私有 _request，包它一处即覆盖全部出站。
    # 测试用的假 session 常只实现 get/post（或 public request），无 _request：
    # 有 _request 则包 _request；否则若有 public request 则包它；两者都无（假 session
    # 只暴露 get/post）则不包裹——这类 session 只在测试中出现且 url_prefix 恒为空。
    if hasattr(session, "_request"):
        attr = "_request"
    elif hasattr(session, "request"):
        attr = "request"
    else:
        return session
    orig = getattr(session, attr)

    def _wrapped(method, str_or_url, **kwargs):
        prefix = getattr(owner, "url_prefix", None) if owner is not None else None
        if prefix:
            str_or_url = apply_url_prefix(str(str_or_url), prefix)
        return orig(method, str_or_url, **kwargs)

    setattr(session, attr, _wrapped)
    return session


UPSTREAM_DEGRADED_FUNCTION_MESSAGE = "上游函数临时不可用，已按上游异常处理"


def is_degraded_function_error(message: str | None) -> bool:
    text = str(message or "").lower()
    return "degraded function cannot be invoked" in text


def normalize_upstream_error_message(message: str | None) -> str:
    text = str(message or "").strip()
    if is_degraded_function_error(text):
        return UPSTREAM_DEGRADED_FUNCTION_MESSAGE
    return text


def _coerce_status_code(value, default: int) -> int:
    try:
        status_code = int(value)
    except (TypeError, ValueError):
        return default
    return status_code if 400 <= status_code <= 599 else default


def _openai_sse_error_exception(payload: dict) -> HTTPException | None:
    error = payload.get("error") if isinstance(payload, dict) else None
    if not error:
        return None

    if isinstance(error, dict):
        raw_message = str(error.get("message") or error)
        err_type = str(error.get("type") or "server_error")
        code = str(error.get("code") or "upstream_error")
        param = error.get("param")
        status_value = error.get("status_code") or error.get("status") or payload.get("status_code") or payload.get("status")
    else:
        raw_message = str(error)
        err_type = str(payload.get("type") or "server_error")
        code = str(payload.get("code") or "upstream_error")
        param = payload.get("param")
        status_value = payload.get("status_code") or payload.get("status")

    degraded = is_degraded_function_error(raw_message)
    status_code = _coerce_status_code(status_value, 503 if degraded else 502)
    if degraded:
        err_type = "server_error"
        code = "upstream_function_degraded"
        status_code = 503

    error_detail = {
        "message": normalize_upstream_error_message(raw_message) or f"HTTP {status_code}",
        "type": err_type,
        "code": code,
    }
    if param:
        error_detail["param"] = param
    return HTTPException(status_code=status_code, detail={"error": error_detail})


def _normalize_openai_sse_usage(chunk: str) -> str:
    lines = []
    changed = False
    for line in chunk.splitlines(keepends=True):
        prefix = line[:len(line) - len(line.lstrip())]
        stripped = line.strip()
        if not stripped.startswith("data:"):
            lines.append(line)
            continue
        data_text = stripped[5:].strip()
        if not data_text or data_text == "[DONE]":
            lines.append(line)
            continue
        try:
            payload = json.loads(data_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            lines.append(line)
            continue
        usage = _extract_usage_payload(payload)
        if isinstance(usage, dict):
            payload = dict(payload)
            payload["usage"] = normalize_usage(usage)
            newline = "\r\n" if line.endswith("\r\n") else ("\n" if line.endswith("\n") else "")
            lines.append(f"{prefix}data: {json.dumps(payload, ensure_ascii=False)}{newline}")
            changed = True
        else:
            lines.append(line)
    return "".join(lines) if changed else chunk


def format_aiohttp_error(e: Exception, url: str = "") -> str:
    prefix = f"{url} " if url else ""
    if isinstance(e, (aiohttp.ClientConnectorSSLError, aiohttp.ClientSSLError, aiohttp.ClientConnectorCertificateError)):
        return f"{prefix}SSL handshake failed: {type(e).__name__}: {e}"
    if isinstance(e, aiohttp.ClientConnectorError):
        return f"{prefix}connect failed: {type(e).__name__}: {e}"
    if isinstance(e, asyncio.TimeoutError):
        return f"{prefix}timeout: {type(e).__name__}: {e}"
    if isinstance(e, aiohttp.ClientError):
        return f"{prefix}HTTP client error: {type(e).__name__}: {e}"
    return f"{prefix}{type(e).__name__}: {e}"


def normalize_upstream_http_error(status_code: int, detail: str, *, url: str = "", provider: str = "") -> HTTPException:
    """把上游 HTTP 错误统一成对前端稳定的状态码。

    401/403/404 一律归一成 404，避免前端把上游认证/权限错误
    误判成管理员登录失效；detail 仍保留原始上游原因用于排查。
    """
    normalized_status = 404 if status_code in (401, 403, 404) else status_code
    prefix_parts = []
    if provider:
        prefix_parts.append(provider)
    if url:
        prefix_parts.append(url)
    prefix = f"[{' '.join(prefix_parts)}] " if prefix_parts else ""
    message = detail.strip() if isinstance(detail, str) and detail.strip() else f"HTTP {status_code}"
    return HTTPException(status_code=normalized_status, detail=f"{prefix}{message}")


class IncompleteStreamError(Exception):
    def __init__(self, message: str = "upstream stream ended before completion", code: str = "upstream_incomplete_stream", upstream_error=None):
        super().__init__(message)
        self.code = code
        self.upstream_error = upstream_error


class EmptyNonStreamResponseError(Exception):
    def __init__(self, message: str = "upstream returned empty non-stream response"):
        super().__init__(message)
        self.code = "upstream_empty_non_stream_response"


class BaseProvider(ABC):
    """模型提供商基类 - 所有渠道继承此类"""

    BASE_URL: str = ""
    DEFAULT_HEADERS: dict = {}
    PROVIDER_NAME: str = ""
    SUPPORTS_MULTI_MESSAGES: bool = False
    NEEDS_STREAM_TOOL_FALLBACK: bool = False
    # tools-as-prompt：上游不认 OpenAI function calling，或上游有自己的服务端工具集。
    # True 时框架统一做三件事，渠道不必各写一份：
    #   1. payload 不带 tools —— 用 tools_for_payload(kwargs) 取值，开关开时恒为空。
    #      这是关键：上游一旦收到 tools 字段就按**它自己的**服务端 function-calling 去查
    #      自家工具注册表，本地工具（如 get_weather）在它那边不存在 -> 直接回
    #      「工具名称不存在: xxx」。光靠提示词措辞拦不住，字段必须不发。
    #   2. 工具说明作为文本进 system —— build_tools_prompt(tools, TOOLS_PROMPT_FORMAT)。
    #   3. 历史轮不留原生工具结构 —— prepare_provider_messages 自动把 assistant 的
    #      tool_calls 拍成文本追加进 content、role=tool 转成 user 文本，否则历史里的
    #      原生结构同样会触发上游那套机制。
    # 模型吐的文本工具调用由框架解析（流式缓冲切块 / 非流式 parse_tool_calls 兜底）。
    TOOLS_AS_PROMPT: bool = False
    # tools-as-prompt 的文本形态——三 alias 已收敛为同一 ```tool_function 围栏
    # （提示词生成、历史轮渲染、解析三侧统一切）。保留 xml/json/hermes 取值供既有
    # 渠道配置兼容；实际形态一致。首选 "tool_function"。
    TOOLS_PROMPT_FORMAT: str = "tool_function"
    # 围栏解析（含工具名是否存在于入参 tools 的校验）由渠道 spec 自己做（如 eaichat
    # 按方法是否存在决定「按方法返回 / 原样透传」）。声明 True 后框架流式出口与
    # 非流式聚合出口不再对 content 二次解析围栏 —— 否则渠道已判为「方法不存在」
    # 原样透传的围栏文本会被框架又切出来解析成 tool_calls，渠道侧校验整个失效。
    TOOL_PARSE_IN_CHANNEL: bool = False
    # 账号凭据过期后能否由系统自动恢复（重新登录 / refresh_token 刷新 / 自动 bootstrap）。
    # True：账号失效只是暂时的，定时任务或下次请求会自愈，前端标记为“已过期（等待自动刷新）”。
    # False：凭据（cookie/token/PAT）只能人工重新获取，前端标记为“异常（需人工处理）”。
    SUPPORTS_TOKEN_AUTO_REFRESH: bool = False
    # 周期任务是否拉取该渠道的上游模型列表。生效值一律以渠道配置 auto_update_models
    # 为准（见下方 property）；此类属性仅作为渠道未注入 / 未显式配置时的兜底默认，
    # 子类可覆盖默认值（默认关闭拉取的渠道设为 False）。
    _default_auto_update_models: bool = True
    is_custom_providers: bool = False
    channel_protocol: str = "openai"
    _channel = None

    def attach_channel(self, channel) -> None:
        """注入渠道领域对象。账号构造后由 ProviderPool.add_accounts 调用。"""
        self._channel = channel
        self._on_channel_attached()

    def _on_channel_attached(self) -> None:
        """子类钩子：渠道对象注入后刷新派生字段。"""
        pass

    @property
    def auto_update_models(self) -> bool:
        """周期任务是否拉取该渠道上游模型列表。以渠道配置为准；
        未注入渠道 / 配置缺省时回退到子类的 _default_auto_update_models。"""
        channel = getattr(self, "_channel", None)
        default = self._default_auto_update_models
        if channel is None:
            return bool(default)
        return bool(getattr(channel, "auto_update_models", default))

    def __init__(self, username: str, password: str, proxy: str = None, url_prefix: str = None, **kwargs):
        self.username = username
        self.password = password
        self.proxy = proxy
        # url_prefix：URL 前缀转发型代理（与 proxy 互斥，一账号绑一条代理池条目）。
        # 非空时所有出站请求 URL 被改写为 {prefix}/{原始绝对URL}，且不走 aiohttp proxy=。
        self.url_prefix = url_prefix
        # proxy_config_id：账号绑定的代理池条目 id（ProxyManager 路由依据）。
        # 空 -> 隐式直连。network/url_prefix/direct/node 四模式统一由 ProxyManager
        # 按此 id 查配置分叉；self.proxy / self.url_prefix 仅为兼容保留的历史字段。
        self.proxy_config_id = kwargs.get("proxy_config_id") or ""
        self.redis_prefix = f'ai-lubricant:{self.PROVIDER_NAME}:{self.username}'
        self._session: aiohttp.ClientSession | None = None
        try:
            self.timeout_seconds = int(kwargs.get("timeout") or 0)
        except (TypeError, ValueError):
            self.timeout_seconds = 0
        if self.timeout_seconds <= 0:
            self.timeout_seconds = 0
        self.limits = ProviderLimitState(
            self,
            config=kwargs.get("rate_limit") or {},
            account_config=kwargs,
        )

    def _stream_request_timeout(self) -> aiohttp.ClientTimeout | None:
        """流式请求的读超时：仅用 sock_read_timeout 约束两次数据之间的最大间隔，
        不设 total 上限，避免长输出 / 思考型模型被误杀。timeout_seconds<=0 表示不限。"""
        if self.timeout_seconds and self.timeout_seconds > 0:
            return aiohttp.ClientTimeout(total=None, sock_read=self.timeout_seconds)
        return None

    def _stream_client_timeout(self) -> aiohttp.ClientTimeout:
        """复用的长连接 session 默认超时；具体单请求超时以 _stream_request_timeout 为准。"""
        t = self._stream_request_timeout()
        return t if t is not None else aiohttp.ClientTimeout(total=None)

    def _outbound_url(self, url: str) -> str:
        """按当前 url_prefix 改写出站 URL（前缀为空时恒等）。供不经 session 子类的场景显式调用。"""
        return apply_url_prefix(str(url), getattr(self, "url_prefix", None))

    @property
    def proxy_account_key(self) -> str:
        """出站资源池的账号隔离键。ProxyManager 用 (account_key, proxy_config_id, host)
        做实例池 key：非空 -> 该账号独占出网连接（做登录/OAuth 的专用渠道，避免多账号
        共用一条连接被上游当同源检测）；空串 -> 按 (proxy_config_id, host) 与同代理账号
        共用（无登录态的渠道，连接复用率最高）。

        默认返回账号身份；CustomProvider 覆盖为空串（继承自定义渠道的都无登录态）。"""
        return f"{self.PROVIDER_NAME}:{self.username}"

    def _make_session(self, timeout: aiohttp.ClientTimeout | None = None):
        """统一的出站 session 工厂：交出 ProxyManager facade，出站按本账号绑定的
        proxy_config_id 路由四种模式（network/url_prefix/direct/node）。

        facade 的 get/post/request 委托给共享 ProxyManager，故各 provider 的调用点
        （session.get(url, proxy=self.proxy) 等）一行不用改：proxy= 被 facade 吞掉，
        url_prefix / network / node 全在 ProxyManager 内按配置分叉；连接池、复用、
        清理由 ProxyManager 的实例池统一管理。

        proxy_config_id 传 callable 而非字符串快照：provider 长期缓存本 facade（见
        _get_session），账号换绑代理时 _apply_account_update_in_place 只改 self.proxy_config_id，
        callable 形式让下一次请求立即按新 id 路由，无需任何入口手动清 self._session。
        timeout 承载原 session 级默认超时（不带 timeout 的调用点靠它兜底）。
        测试普遍 monkeypatch 本方法整体替换成假 session，故 facade 在测试中被绕过。"""
        # 局部导入避开循环：proxy_manager 在模块顶层从 base 导入若干工具函数。
        from providers.proxy_manager import get_proxy_manager
        return get_proxy_manager().session(
            account_id=self.proxy_account_key,
            proxy_config_id=lambda: getattr(self, "proxy_config_id", "") or "",
            default_timeout=timeout,
        )

    def _get_session(self):
        """返回账号级复用 facade；底层连接复用由 ProxyManager 实例池负责。"""
        session = self._session
        if session is None or getattr(session, "closed", False):
            try:
                session = self._make_session(timeout=self._stream_client_timeout())
            except TypeError as exc:
                # Preserve compatibility with older provider/test overrides that
                # implemented _make_session() without the timeout keyword.
                if "unexpected keyword argument 'timeout'" not in str(exc):
                    raise
                session = self._make_session()
            self._session = session
        return session

    async def check_message(self, model_id: str, messages: list[dict] | None = None) -> bool:
        return True

    async def record_message(self, model_id: str, messages: list[dict] | None = None) -> None:
        pass

    async def update_quota_from_headers(self, headers: dict, model_id: str | None = None) -> None:
        pass

    async def check_limits(self, model_id: str | None = None, messages: list[dict] | None = None, context: dict | None = None):
        return await self.limits.check(model_id, messages, context)

    async def reserve_limits(self, model_id: str | None = None, messages: list[dict] | None = None, context: dict | None = None):
        return await self.limits.reserve(model_id, messages, context)

    async def reserve_limits_with_decision(self, model_id: str | None = None, messages: list[dict] | None = None, context: dict | None = None):
        return await self.limits.reserve_with_decision(model_id, messages, context)

    async def release_limits(self, lease=None, context: dict | None = None) -> None:
        await self.limits.release(lease, context)

    async def record_limit_usage(self, model_id: str, usage: dict | None = None, context: dict | None = None) -> None:
        await self.limits.record_usage(model_id, usage, context)

    async def on_response_headers(self, headers: dict, model_id: str | None = None, context: dict | None = None) -> None:
        await self.limits.update_from_headers(headers, model_id, context)

    async def on_response_headers_failed(
        self,
        headers: dict,
        model_id: str | None = None,
        context: dict | None = None,
        error: Exception | None = None,
    ) -> None:
        pass

    async def get_limit_snapshot(self) -> dict:
        return await self.limits.snapshot()

    async def record_response_headers(self, headers, kwargs: dict | None = None) -> None:
        callback = (kwargs or {}).get("response_headers_callback")
        if not callback:
            return
        result = callback(dict(headers or {}))
        if inspect.isawaitable(result):
            await result

    async def record_router_request_headers(self, headers, kwargs: dict | None = None) -> None:
        callback = (kwargs or {}).get("router_request_headers_callback")
        if not callback:
            return
        result = callback(dict(headers or {}))
        if inspect.isawaitable(result):
            await result

    async def record_router_request_body(self, body, kwargs: dict | None = None) -> None:
        callback = (kwargs or {}).get("router_request_body_callback")
        if not callback:
            return
        # 签名类渠道（codearts/qoderwork）把出站 body 定稿成 bytes 再交给 aiohttp，
        # 因为签名算的是精确字节。日志侧必须在这里解回文本：bytes 留在
        # router_request_body 里，ClickHouse writer 的 json.dumps 会抛 TypeError，
        # 连带同批（最多 flush_size 条）无关请求的 payload 一起被丢。
        # 解码与下面的拆包分两步：同时带 params 的多键形态拆不开，但也不能漏解码。
        if isinstance(body, dict) and isinstance(body.get("data"), (bytes, bytearray)):
            body = {**body, "data": bytes(body["data"]).decode("utf-8", errors="replace")}
        if isinstance(body, dict) and set(body.keys()) == {"data"} and isinstance(body.get("data"), str):
            try:
                body = json.loads(body["data"])
            except json.JSONDecodeError:
                # 非 JSON 出站体（如 qoderwork 的 base64 密文）原样留字符串，
                # 出站真就是这串东西。
                body = body["data"]
        result = callback(body)
        if inspect.isawaitable(result):
            await result

    async def record_router_request_path(self, url: str, kwargs: dict | None = None) -> None:
        callback = (kwargs or {}).get("router_request_path_callback")
        if not callback:
            return
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path_only = parsed.path or "/"
        result = callback(path_only)
        if inspect.isawaitable(result):
            await result

    async def record_router_response_body(self, body, kwargs: dict | None = None) -> None:
        callback = (kwargs or {}).get("router_response_body_callback")
        if not callback:
            return
        result = callback(body)
        if inspect.isawaitable(result):
            await result

    def apply_common_payload_params(self, payload: dict, kwargs: dict, key_map: dict | None = None) -> None:
        key_map = key_map or {}
        for key in (
            "top_p", "top_k", "stop", "tool_choice", "metadata", "service_tier", "container",
            "context_management", "mcp_servers", "frequency_penalty", "presence_penalty",
            "repetition_penalty", "min_p", "top_a"
        ):
            val = kwargs.get(key)
            if val is not None:
                payload[key_map.get(key, key)] = val

    def get_channel_protocol(self) -> str:
        if self.is_custom_providers:
            return str(getattr(self, "protocol", None) or self.channel_protocol or "openai").lower()
        return str(self.channel_protocol or "openai").lower()

    def same_channel_protocol(self, protocol: str, raw_body: dict | None, marker: str) -> bool:
        return self.get_channel_protocol() == protocol and isinstance(raw_body, dict) and raw_body.get(marker) is not None

    def format_channel_usage(self, usage: dict | None) -> dict | None:
        if self.get_channel_protocol() == "openai":
            return self.format_openai_usage(usage)
        return usage if isinstance(usage, dict) else None

    def format_channel_response(self, response: dict | None) -> dict | None:
        if self.is_custom_providers or self.get_channel_protocol() != "openai":
            return response
        return self.format_openai_response(response)

    def format_channel_stream_chunk(self, chunk):
        if self.is_custom_providers or self.get_channel_protocol() != "openai":
            return chunk
        return self.format_openai_stream_chunk(chunk)

    def format_openai_usage(self, usage: dict | None) -> dict | None:
        if not isinstance(usage, dict):
            return None
        normalized = normalize_usage(usage)
        if normalized["prompt_tokens"] + normalized["completion_tokens"] + normalized["cached_tokens"] + normalized["cache_creation_tokens"] + normalized["reasoning_tokens"] <= 0:
            return dict(usage) if self.is_custom_providers else None
        result = {
            "prompt_tokens": normalized["prompt_tokens"],
            "completion_tokens": normalized["completion_tokens"],
            "total_tokens": normalized["total_tokens"],
        }
        if normalized["cached_tokens"] or normalized["cache_creation_tokens"]:
            result["prompt_tokens_details"] = {
                "cached_tokens": normalized["cached_tokens"],
                "cache_creation_input_tokens": normalized["cache_creation_tokens"],
            }
        if normalized["reasoning_tokens"]:
            result["completion_tokens_details"] = {"reasoning_tokens": normalized["reasoning_tokens"]}
        result["cached_tokens"] = normalized["cached_tokens"]
        result["cache_creation_tokens"] = normalized["cache_creation_tokens"]
        result["reasoning_tokens"] = normalized["reasoning_tokens"]
        return result

    def format_openai_response(self, response: dict | None) -> dict | None:
        if not isinstance(response, dict) or self.is_custom_providers:
            return response
        result = dict(response)
        usage = self.format_openai_usage(result.get("usage"))
        if usage is not None:
            result["usage"] = usage
        return result

    def format_openai_stream_chunk(self, chunk):
        if self.is_custom_providers or not isinstance(chunk, dict):
            return chunk
        result = dict(chunk)
        usage = self.format_openai_usage(result.get("usage"))
        if usage is not None:
            result["usage"] = usage
        elif "usage" in result:
            result.pop("usage", None)
        return result

    # ==================== 认证相关 ====================
    @abstractmethod
    async def init_auth(self, is_check: bool = False) -> bool:
        """初始化认证，返回是否成功"""
        pass

    @property
    def invitation_interval(self):
        return random.uniform(2, 5)

    def is_init(self) -> bool:
        return False

    @classmethod
    def account_schema(cls) -> dict:
        """返回管理后台账号表单 schema，子类可覆盖以声明专属字段和添加说明。"""
        provider_name = getattr(cls, "PROVIDER_NAME", "") or cls.__name__
        return {
            "provider_name": provider_name,
            "add_methods": ["manual_form"],
            "fields": [
                {
                    "key": "password",
                    "label": "API Key / 密码",
                    "type": "password",
                    "secret": True,
                    "required": False,
                    "placeholder": "sk-...",
                    "section": "基础信息",
                },
            ],
            "auth_start": {"enabled": False},
            "add_guidance": "",
            "metadata_badges": [],
        }

    @classmethod
    async def build_account_auth_start(cls, provider_name: str, account: dict, redirect_uri: str, state: str, cfg: dict | None = None, auth_context: dict | None = None) -> dict:
        """构建账号网页登录/设备码授权入口。未支持的渠道保持默认错误。

        ``redirect_uri`` 由框架用「浏览器 origin + 框架回调路径」生成，渠道只消费、不自拼；
        ``auth_context`` 是同一份授权上下文（origin / host / port / callback_url / state），
        供上游只吃回调端口那类协议取用。
        """
        raise HTTPException(status_code=501, detail=f"渠道 {provider_name} 暂未实现账号回调添加")

    @classmethod
    async def handle_account_auth_callback(cls, provider_name: str, params: dict, state_data: dict, cfg: dict | None = None) -> dict:
        """处理网页登录回调并返回要写入配置的账号 dict。"""
        raise HTTPException(status_code=501, detail=f"渠道 {provider_name} 暂未实现账号回调处理")

    async def refresh_account_auth(self, account: dict, cfg: dict | None = None) -> dict:
        """刷新账号授权信息，返回需要合并回账号配置的字段。"""
        raise HTTPException(status_code=501, detail=f"渠道 {self.PROVIDER_NAME or self.__class__.__name__} 暂未实现刷新授权")

    async def clear_conversations(self, max_age_hours: int = 2):
        """清除过期对话，子类可覆盖实现

        Returns:
            删除的对话数量
        """
        return 0

    @abstractmethod
    async def check_auth(self) -> bool:
        """检查认证状态是否有效"""
        pass

    async def health_check(self) -> bool:
        """健康检查：默认回退到 check_auth。CustomProvider 由渠道对象驱动真打上游。"""
        return await self.check_auth()

    # ==================== 模型列表 ====================
    @abstractmethod
    async def fetch_upstream_model_list(self) -> list[dict]:
        """拉上游 /models，返回 [{"id": upstream_id, "name": ..., "raw": ...}]，
        不应用任何白名单或重命名 —— 那是上层 ModelClientPool / provider_models 表的职责。
        """
        pass

    def _model_with_upstream_defaults(self, model: dict, raw: dict | None = None) -> dict:
        item = dict(model)
        item.setdefault("name", item.get("display_name") or item.get("id"))
        if raw is not None:
            item["raw"] = raw
        return item

    async def generate_image(self, model_id: str, prompt: str, **kwargs) -> dict:
        raise HTTPException(status_code=400, detail=f"渠道 {self.PROVIDER_NAME or self.__class__.__name__} 不支持图片生成")

    async def generate_video(self, model_id: str, prompt: str, **kwargs) -> dict:
        raise HTTPException(status_code=400, detail=f"渠道 {self.PROVIDER_NAME or self.__class__.__name__} 不支持视频生成")

    async def generate_speech(self, model_id: str, text: str, **kwargs) -> dict:
        raise HTTPException(status_code=400, detail=f"渠道 {self.PROVIDER_NAME or self.__class__.__name__} 不支持语音合成")

    # ==================== 聊天接口 ====================
    async def chat(
        self,
        model_id: str,
        messages: list[dict],
        stream: bool = True,
        **kwargs
    ) -> AsyncGenerator[Union[str, dict], None]:
        """
        聊天接口 - 统一入参，渠道内部处理差异化逻辑。

        Args:
            model_id: 模型 ID
            messages: OpenAI 格式消息列表
            stream: 是否流式输出
            **kwargs: 通用参数
                - temperature: float
                - tools: list[dict]
                - thinking_enabled: bool
                - 等...

        Returns:
            流式: yield SSE chunk 字符串
            非流式: yield OpenAI response dict
        """
        completion_id = generate_completion_id()

        if stream:
            role_chunk = make_openai_chunk(completion_id, model_id, {"role": "assistant"}, None)
            full_content = ""
            full_thinking = ""
            full_tool_calls = []
            full_usage = None  # 收集 usage 信息
            upstream_finish_reason = None  # 保留上游明确返回的结束原因
            seen_finish_output = False
            seen_usage_output = False
            seen_nonzero_usage_output = False
            seen_done_output = False
            pending_done_output = False
            chunk_count = 0  # 统计从渠道收到的 chunk 数
            role_sent = False
            # 是否从上游收到过真实可见输出（content / reasoning / tool_calls）。
            # role chunk（role_sent）属于协议启动信号，不算真实输出。
            has_real_output = False
            # 流式工具调用解析缓冲：XML 提示词注入类渠道（eaichat/qwen/xiaomi/...）
            # 把工具调用当普通文本 delta 透传，流式出口这里把 ` <function=...>` 从
            # content 中切出来，解析成标准 OpenAI tool_calls delta 下发，文本不下发。
            # 仅当本次请求带了 tools 才启用，避免误吞无工具对话的正常文本。
            # 渠道声明 TOOL_PARSE_IN_CHANNEL（围栏解析含工具名校验在渠道内做）时，
            # 框架侧解析必须整体关掉：渠道已判「方法不存在」原样透传的围栏文本，
            # 到这里再解析会变成 tool_calls，等于把渠道的校验结果推翻。
            tools_enabled = bool(kwargs.get("tools")) and not self.TOOL_PARSE_IN_CHANNEL
            tool_call_buffer = ""

            # 统计输入消息详情
            user_msg_count = sum(1 for m in messages if m.get("role") == "user")
            assistant_msg_count = sum(1 for m in messages if m.get("role") == "assistant")
            system_msg_count = sum(1 for m in messages if m.get("role") == "system")
            tool_msg_count = sum(1 for m in messages if m.get("role") == "tool")

            logger.info(f"[Base.stream_start] ======== 渠道开始流式请求 ======== completion_id={completion_id}, model={model_id}, provider={self.PROVIDER_NAME}, "
                        f"messages_count={len(messages)} (system={system_msg_count}, user={user_msg_count}, assistant={assistant_msg_count}, tool={tool_msg_count}), "
                        f"tools_count={len(kwargs.get('tools') or [])}, "
                        f"max_tokens={kwargs.get('max_tokens')}, thinking_enabled={kwargs.get('thinking_enabled')}")
            # 调用渠道的流式实现
            try:
                async for chunk in self._do_stream_chat(model_id, messages, **kwargs):
                    # 渠道 yield 空字典表示连接成功，但此时不发送 role_chunk。
                    # 推迟到首个真实内容块再发送 role_chunk（见下方 fallback）。
                    # 若上游为空流（只有连接标记 + 终止信号、无内容），IncompleteStreamError
                    # 会在 _prime_stream_before_response 阶段、即向客户端发送任何字节之前抛出，
                    # 从而让 _chat_with_retry 透明重试，避免重试后出现重复 role_chunk 损坏 SSE。
                    if isinstance(chunk, dict) and not chunk:
                        continue

                    chunk_count += 1
                    if isinstance(chunk, str):
                        # 预解析字符串块，判断本块是否带真实内容（content / reasoning / tool_calls）。
                        # role_chunk 只在首个真实内容块之前发送（兜底）；纯终止块（[DONE] /
                        # finish_reason / 仅 usage）不触发 role_chunk，从而空流时不会泄漏 role。
                        chunk_has_real_content = False
                        chunk_has_usage = False
                        chunk_has_nonzero_usage = False
                        chunk_has_done = False
                        chunk_has_finish = False
                        parsed_payload_count = 0
                        for payload in iter_sse_payloads(chunk):
                            parsed_payload_count += 1
                            if payload == "[DONE]":
                                seen_done_output = True
                                pending_done_output = True
                            elif isinstance(payload, dict):
                                stream_error = _openai_sse_error_exception(payload)
                                if stream_error is not None:
                                    raise stream_error
                                payload_usage = _extract_usage_payload(payload)
                                if isinstance(payload_usage, dict):
                                    full_usage = merge_usage(full_usage, payload_usage)
                                    chunk_has_usage = True
                                    normalized_payload_usage = normalize_usage(payload_usage)
                                    if normalized_payload_usage["prompt_tokens"] + normalized_payload_usage["completion_tokens"] + normalized_payload_usage["cached_tokens"] + normalized_payload_usage["cache_creation_tokens"] + normalized_payload_usage["reasoning_tokens"] > 0:
                                        chunk_has_nonzero_usage = True
                                    if _usage_has_nonzero_completion(payload_usage):
                                        seen_nonzero_usage_output = True
                                    # 累计型上游首帧 usage 常为 {prompt:X, completion:0}：
                                    # “是否已向客户端发过有效 usage”必须以 completion/output 非零为准，
                                    # 否则会把零 completion 固化进流并锁死后续累计的 completion（导致日志/DB 记 0）。
                                    if _usage_has_nonzero_completion(full_usage):
                                        seen_usage_output = True
                                for choice in payload.get("choices", []) or []:
                                    reason = choice.get("finish_reason")
                                    if reason:
                                        upstream_finish_reason = str(reason)
                                        seen_finish_output = True
                                        chunk_has_finish = True
                                    delta = choice.get("delta") or {}
                                    if isinstance(delta, dict) and (
                                        delta.get("content")
                                        or delta.get("reasoning_content")
                                        or delta.get("reasoning")
                                        or delta.get("tool_calls")
                                    ):
                                        if delta.get("content"):
                                            full_content += str(delta.get("content") or "")
                                        reasoning_delta = delta.get("reasoning_content") or delta.get("reasoning")
                                        if reasoning_delta:
                                            full_thinking += str(reasoning_delta)
                                        if delta.get("tool_calls"):
                                            full_tool_calls.extend(delta.get("tool_calls") or [])
                                        has_real_output = True
                                        chunk_has_real_content = True
                        # 在首个真实内容块之前发送 role_chunk（兜底）。
                        # 纯终止块不触发 role_chunk；若仍处于空流且启用不完整流检测，则先消费终止块，
                        # 让后续 IncompleteStreamError 在 priming 阶段透明重试。
                        if chunk_has_real_content and not role_sent:
                            role_sent = True
                            yield role_chunk
                        completion_token_count = 0
                        if isinstance(full_usage, dict):
                            for usage_key in ("completion_tokens", "output_tokens"):
                                usage_value = full_usage.get(usage_key)
                                if usage_value is None:
                                    continue
                                try:
                                    completion_token_count = int(usage_value or 0)
                                except (TypeError, ValueError):
                                    completion_token_count = 0
                                break
                        suppress_terminal_before_role = (
                            not role_sent
                            and not has_real_output
                            and completion_token_count <= 0
                            and not chunk_has_real_content
                            and not chunk_has_usage
                            and parsed_payload_count > 0
                            and kwargs.get("stream_incomplete_error_enabled", True)
                        )
                        if not suppress_terminal_before_role:
                            # 字符串块本身按上游顺序透传；纯零 usage 或纯 [DONE] 会在末尾补齐后再发。
                            should_yield_chunk = not (
                                (chunk_has_done and not chunk_has_usage and not chunk_has_real_content and not chunk_has_finish)
                                or (chunk_has_usage and not chunk_has_nonzero_usage and not chunk_has_real_content and not chunk_has_finish)
                            )
                            if should_yield_chunk:
                                yield _normalize_openai_sse_usage(chunk)
                        continue

                    chunk = self.format_channel_stream_chunk(chunk)
                    if isinstance(chunk, dict):
                        # 上游可能把 content/thinking 显式置为 null（如仅带 usage 的收尾帧、
                        # reasoning_content: null）。累加前必须归一成 str，否则
                        # `full_thinking += thinking` 会抛 TypeError 并被误判成渠道异常冻结账号。
                        thinking = chunk.get("thinking") or ""
                        content = chunk.get("content") or ""
                        tool_calls = chunk.get("tool_calls") or []
                        usage = chunk.get("usage")  # 获取 usage 信息
                        chunk_finish_reason = chunk.get("finish_reason")
                        if chunk_finish_reason:
                            upstream_finish_reason = str(chunk_finish_reason)

                        chunk_done = bool(chunk.get("done") or chunk.get("_done"))

                        if content or thinking or tool_calls:
                            has_real_output = True

                        full_thinking += thinking

                        if usage:
                            full_usage = merge_usage(full_usage, usage)
                            if _usage_has_nonzero_completion(usage):
                                seen_nonzero_usage_output = True

                        # 在第一个真实输出（thinking / tool_calls / content）之前发送 role_chunk（兜底）
                        if (thinking or tool_calls or content) and not role_sent:
                            role_sent = True
                            yield role_chunk

                        if thinking:
                            if "<tool_call>" in thinking or "<function" in thinking or "function=" in thinking or ("```" in thinking and "name" in thinking):
                                content = thinking + content
                            else:
                                tc = make_openai_chunk(completion_id, model_id,
                                                        {"reasoning_content": thinking}, None)
                                yield tc
                        if tool_calls:
                            # 上游可能给 dict 形态 arguments（eaichat 等）；OpenAI 协议要求
                            # 字符串，原样下发客户端会拿到违规 delta。与非流式聚合侧
                            # (_merge_tool_calls) 同口径归一，渠道实现不必各自写一份。
                            tool_calls = normalize_tool_call_arguments(tool_calls)
                            full_tool_calls.extend(tool_calls)
                            tc = make_openai_chunk(completion_id, model_id,
                                                    {"tool_calls": tool_calls}, None)
                            # logger.info(f"[Base.stream_yield] completion_id={completion_id}, type=tool_calls_delta, count={len(tool_calls)}")
                            yield tc

                        if content:
                            # XML 提示词注入类渠道把工具调用混进 content，这里用
                            # extract_complete_tool_calls 切出完整块解析成 tool_calls
                            # delta，仅下发工具块前的普通文本；未闭合的半截留 buffer
                            # 等后续 delta 补全。无 tools 时原样透传，零缓冲零重排。
                            if tools_enabled:
                                tool_call_buffer += content
                                flush_text, blocks, tool_call_buffer = extract_complete_tool_calls(tool_call_buffer)
                                # 有完整块之前，若已出现工具块起始标记，其前的普通
                                # 文本前缀仍可即时下发；起始标记之后的任何字符都属于
                                # 未闭合块，必须留 buffer 不下发，等闭合后再切。
                                if blocks:
                                    start = find_tool_call_start(flush_text)
                                    if start >= 0:
                                        emit_text, hold = flush_text[:start], flush_text[start:]
                                        tool_call_buffer = hold + tool_call_buffer
                                    else:
                                        emit_text = flush_text
                                else:
                                    # 在 `<function=` / `<tool_call>` 尚未收全之前，extract
                                    # helper 还看不见起始标记；保留可能是标记前缀的尾部，
                                    # 避免把 `<funct...` / `<tool_c...` 分块提前泄漏成普通文本。
                                    # 标记全集是 tool_utils 的单一真相源，新增形态不用改这里。
                                    hold_start = hold_partial_tool_marker_index(flush_text)
                                    emit_text, hold = flush_text[:hold_start], flush_text[hold_start:]
                                    tool_call_buffer = hold + tool_call_buffer
                                if emit_text:
                                    full_content += emit_text
                                    tc = make_openai_chunk(completion_id, model_id,
                                                            {"content": emit_text}, None)
                                    yield tc
                                if blocks:
                                    _, parsed = parse_tool_calls_from_content("\n".join(blocks))
                                    if parsed:
                                        full_tool_calls.extend(parsed)
                                        tc = make_openai_chunk(completion_id, model_id,
                                                                {"tool_calls": parsed}, None)
                                        yield tc
                            else:
                                full_content += content
                                tc = make_openai_chunk(completion_id, model_id,
                                                        {"content": content}, None)
                                yield tc

                        if chunk_finish_reason and not seen_finish_output:
                            finish_str = str(chunk_finish_reason)
                            # dict 流式路径的 usage 可能是累计值（Kimi/vLLM 等每帧全量递增）。
                            # 不在中间帧或 finish 帧写 usage，统一到流结束后用 full_usage 发最终值，
                            # 避免首个 completion=0/1 的中间 usage 被写进日志后锁死最终统计。
                            finish_chunk = make_openai_chunk(completion_id, model_id, {}, finish_str)
                            seen_finish_output = True
                            # logger.info(f"[Base.stream_yield] completion_id={completion_id}, type=finish, finish_reason={finish_str}")
                            yield finish_chunk

                        # usage 只累计到 full_usage，不在 dict 路径中途下发；见流结束后的最终 usage 输出。

                        if chunk_done and not seen_done_output:
                            seen_done_output = True
                            yield "data: [DONE]\n\n"
                    else:
                        yield chunk
            except Exception as e:
                logger.error(f"[Base.stream_error] completion_id={completion_id}, error={type(e).__name__}: {e}")
                raise

            # 客户端可见的流式输出到这里已经按上游顺序即时发出。
            # 下面只允许做内部统计/日志，不允许再根据 full_content 解析并向客户端补发内容或工具调用。
            # 例外：tools 启用时残留的半截工具块 buffer 必须在这里 flush——否则工具调用
            # 会随 buffer 一起丢失，客户端永远收不到这次调用的 tool_calls。语义对齐
            # 非流式路径对 full_content 调 parse_tool_calls 的兜底。
            if tools_enabled and tool_call_buffer:
                cleaned, parsed = parse_tool_calls_from_content(tool_call_buffer)
                tool_call_buffer = ""
                if parsed:
                    # 残留 buffer 切出了完整工具调用，其外的普通文本（cleaned）
                    # 才作为可见内容补发。
                    full_tool_calls.extend(parsed)
                    if cleaned:
                        full_content += cleaned
                        yield make_openai_chunk(completion_id, model_id, {"content": cleaned}, None)
                    yield make_openai_chunk(completion_id, model_id, {"tool_calls": parsed}, None)
                # parsed 为空时残留的是没闭合完的半截工具块（```json 未收尾 /
                # <function 未闭合等），属工具调用噪音而非正文，丢弃，不补发
                # 成可见文本——否则会把 "get_weather" / 半截 JSON 泄漏给客户端。
            # 结束兜底：正常上游事件已即时输出，这里只补缺失的终止信号。
            finish_reason = upstream_finish_reason or ("tool_calls" if full_tool_calls else "stop")
            logger.info(f"[BaseProvider.chat] ======== 渠道流式请求完成 ======== completion_id={completion_id}, "
                        f"finish_reason={finish_reason}, tool_calls_count={len(full_tool_calls)}, "
                        f"usage={full_usage}, chunk_count_from_provider={chunk_count}, "
                        f"full_content_len={len(full_content)}, full_thinking_len={len(full_thinking)}")

            # 不完整流检测：上游只发了终止信号（[DONE]/bare done/finish_reason）却没有任何
            # 真实输出（content / reasoning / tool_calls），且 usage 也没有 completion tokens，
            # 此时不能向客户端补发一个"空但合法"的响应，应抛 IncompleteStreamError，
            # 让上层 _chat_with_retry 切换下一个账号/渠道重试。语义对齐 custom.py。
            if kwargs.get("stream_incomplete_error_enabled", True):
                def _completion_tokens(usage: Union[dict, None]) -> int:
                    if not isinstance(usage, dict):
                        return 0
                    for key in ("completion_tokens", "output_tokens"):
                        value = usage.get(key)
                        if value is None:
                            continue
                        try:
                            return int(value or 0)
                        except (TypeError, ValueError):
                            return 0
                    return 0

                has_output = has_real_output or bool(full_content) or bool(full_tool_calls)
                terminator_only = seen_done_output or seen_finish_output or bool(upstream_finish_reason)
                if (
                    not has_output
                    and finish_reason == "stop"
                    and _completion_tokens(full_usage) <= 0
                    and terminator_only
                ):
                    logger.warning(
                        f"[BaseProvider.chat] 上游仅返回终止信号但无真实输出，判定为不完整流: "
                        f"completion_id={completion_id}, seen_done={seen_done_output}, "
                        f"finish_reason={finish_reason}, usage={full_usage}"
                    )
                    raise IncompleteStreamError("upstream stream completed with empty output")

            if not seen_finish_output:
                finish_chunk = make_openai_chunk(completion_id, model_id, {}, finish_reason)
                yield finish_chunk
                seen_finish_output = True

            usage_to_emit = full_usage
            # 没向客户端发过有效 usage（completion 非零）时补发：
            # - 累计型上游（如 Kimi vLLM 每帧全量 usage），主路径在每个带 usage 的 chunk 透传
            #   时已发出，seen_usage_output=True，这里不再补发。
            # - 但若上游直到结束都只有 {prompt:X, completion:0} 型 usage，full_usage 累计的
            #   completion 仍可能为 0 → 按内容估算兜底；反之若有累计 completion 也应补发给客户端。
            if not seen_usage_output:
                normalized_usage = normalize_usage(full_usage)
                if _usage_has_nonzero_completion(full_usage):
                    # 累计到了非零 completion 但尚未下发（被首帧零 completion 锁住）→ 补发累计 usage
                    usage_to_emit = full_usage
                else:
                    estimated_usage = estimate_usage(
                        {"messages": messages, "tools": kwargs.get("tools")},
                        {"choices": [{"message": {
                            "content": full_content,
                            "reasoning_content": full_thinking,
                            "tool_calls": full_tool_calls,
                        }}]},
                    )
                    # 真实值优先、估算只补缺失分量：上游给了真实 prompt_tokens 却不发
                    # completion（eaichat 等）时，不能让估算的小 prompt 盖掉真实值。
                    usage_to_emit = fill_usage_with_estimate(full_usage, estimated_usage)
                if usage_to_emit:
                    usage_chunk = {
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model_id,
                        "choices": [],
                        "usage": normalize_usage(usage_to_emit)
                    }
                    usage_str = f"data: {json.dumps(usage_chunk, ensure_ascii=False)}\n\n"
                    # logger.info(f"[Base.stream_yield] completion_id={completion_id}, type=usage_fallback, usage={usage_to_emit}")
                    yield usage_str
                    seen_usage_output = True

            if pending_done_output or not seen_done_output:
                yield "data: [DONE]\n\n"

        else:
            result = await self._do_non_stream_chat(model_id, messages, **kwargs)
            yield self.format_channel_response(result)

    @abstractmethod
    async def _do_stream_chat(
        self,
        model_id: str,
        messages: list[dict],
        **kwargs
    ) -> AsyncGenerator[dict, None]:
        """
        流式聊天 - 渠道实现自己的逻辑。

        返回: {"content": str, "thinking": str, "tool_calls": list}
        """
        pass

    @abstractmethod
    async def _do_non_stream_chat(
        self,
        model_id: str,
        messages: list[dict],
        **kwargs
    ) -> dict:
        """
        非流式聊天 - 渠道实现自己的逻辑。

        返回: OpenAI 格式完整响应 dict
        """
        pass

    async def chat_anthropic(
        self,
        model_id: str,
        messages: list[dict],
        stream: bool = False,
        **kwargs
    ):
        """统一 anthropic 出口。

        - 同协议（provider 自身是 anthropic 协议且 kwargs 带有 _raw_anthropic_body）：
          调用 ``_anthropic_passthrough_stream`` / ``_anthropic_passthrough_nonstream``，
          上游 SSE/JSON 原样转发，保留全部字段（signature、cache_control、block 顺序、citations 等）。
        - 跨协议（openai 上游）：复用 ``self.chat`` 后用 ``convert_stream_to_anthropic`` /
          ``openai_to_anthropic_response`` 转换为 anthropic 格式。

        Yield（stream=True）：
            - ``{}`` 首个标记（用于 _chat_with_retry 记录 TTFT）
            - anthropic SSE event 字符串
            - 末尾 ``{"_passthrough_done": True, "usage": {...}, "message_id": "..."}``
        Yield（stream=False）：
            - 单个 ``{"_passthrough_anthropic": True, "body": {...}, "usage": {...}, "message_id": "..."}``
        """
        raw_body = kwargs.get("_raw_anthropic_body")
        same_protocol = self.same_channel_protocol("anthropic", raw_body, "messages")

        # 渠道可配「随客户端」(auto)：auto 时上游 stream 跟随客户端，直通判定恒成立。
        upstream_stream = resolve_upstream_stream(getattr(self, "upstream_stream", stream), stream)

        if same_protocol and stream == upstream_stream:
            passthrough_stream = getattr(self, "_anthropic_passthrough_stream", None)
            passthrough_nonstream = getattr(self, "_anthropic_passthrough_nonstream", None)
            if stream and passthrough_stream is not None:
                async for chunk in passthrough_stream(model_id, raw_body, **kwargs):
                    yield chunk
                return
            if not stream and passthrough_nonstream is not None:
                result = await passthrough_nonstream(model_id, raw_body, **kwargs)
                yield result
                return

        # 跨协议或客户端/上游 stream 模式不一致：走统一转换链路
        from message_utils import convert_stream_to_anthropic, openai_to_anthropic_response

        cross_kwargs = dict(kwargs)
        cross_kwargs["_from_anthropic"] = True

        if stream:
            usage_capture: dict = {"value": None}
            message_id_capture: dict = {"value": None}

            async def _intercept_openai():
                async for chunk in self.chat(model_id, messages, stream=True, **cross_kwargs):
                    if isinstance(chunk, dict):
                        # 首个空 dict 标记会被 convert_stream_to_anthropic 当作 falsy 跳过，安全
                        yield chunk
                        continue
                    if isinstance(chunk, str):
                        try:
                            for line in chunk.split("\n"):
                                if not line.startswith("data:"):
                                    continue
                                line_data = line[5:].strip()
                                if not line_data or line_data == "[DONE]":
                                    continue
                                obj = json.loads(line_data)
                                if not isinstance(obj, dict):
                                    # data: null 等非对象帧跳过，避免后续 .get 调用抛错
                                    continue
                                if obj.get("usage"):
                                    usage_capture["value"] = merge_usage(usage_capture["value"], obj["usage"])
                                if obj.get("id") and not message_id_capture["value"]:
                                    message_id_capture["value"] = obj["id"]
                        except Exception:
                            pass
                    yield chunk

            first_emitted = False
            async for anthropic_event in convert_stream_to_anthropic(_intercept_openai(), model_id, request_messages=messages):
                if not first_emitted:
                    first_emitted = True
                    yield {}
                yield anthropic_event

            yield {
                "_passthrough_done": True,
                "usage": usage_capture["value"],
                "message_id": message_id_capture["value"],
            }
        else:
            result = await self._do_non_stream_chat(model_id, messages, **cross_kwargs)
            anthropic_body = openai_to_anthropic_response(result, model_id) if isinstance(result, dict) else result
            usage = result.get("usage") if isinstance(result, dict) else None
            yield {
                "_passthrough_anthropic": True,
                "body": anthropic_body,
                "usage": usage,
                "message_id": anthropic_body.get("id") if isinstance(anthropic_body, dict) else None,
            }

    async def chat_responses(
        self,
        model_id: str,
        messages: list[dict],
        stream: bool = False,
        **kwargs
    ):
        raw_body = kwargs.get("_raw_responses_body")
        same_protocol = self.same_channel_protocol("responses", raw_body, "input")

        # 渠道可配「随客户端」(auto)：auto 时上游 stream 跟随客户端，直通判定恒成立。
        upstream_stream = resolve_upstream_stream(getattr(self, "upstream_stream", stream), stream)

        if same_protocol and stream == upstream_stream:
            passthrough_stream = getattr(self, "_responses_passthrough_stream", None)
            passthrough_nonstream = getattr(self, "_responses_passthrough_nonstream", None)
            if stream and passthrough_stream is not None:
                async for chunk in passthrough_stream(model_id, raw_body, **kwargs):
                    yield chunk
                return
            if not stream and passthrough_nonstream is not None:
                result = await passthrough_nonstream(model_id, raw_body, **kwargs)
                yield result
                return

        if stream:
            usage_capture: dict = {"value": None}
            response_id_capture: dict = {"value": None}

            async def _intercept_openai():
                async for chunk in self.chat(model_id, messages, stream=True, _from_responses=True, **kwargs):
                    if isinstance(chunk, str):
                        try:
                            for line in chunk.split("\n"):
                                if not line.startswith("data:"):
                                    continue
                                line_data = line[5:].strip()
                                if not line_data or line_data == "[DONE]":
                                    continue
                                obj = json.loads(line_data)
                                if isinstance(obj, dict):
                                    if obj.get("usage"):
                                        usage_capture["value"] = merge_usage(usage_capture["value"], obj["usage"])
                                    if obj.get("id") and not response_id_capture["value"]:
                                        response_id_capture["value"] = obj["id"]
                        except Exception:
                            pass
                    yield chunk

            first_emitted = False
            async for responses_event in convert_stream_to_responses(_intercept_openai(), model_id):
                if not first_emitted:
                    first_emitted = True
                    yield {}
                yield responses_event
            yield {
                "_passthrough_done": True,
                "usage": usage_capture["value"],
                "response_id": response_id_capture["value"],
            }
        else:
            result = await self._do_non_stream_chat(model_id, messages, _from_responses=True, **kwargs)
            responses_body = openai_to_responses_response(result, model_id) if isinstance(result, dict) else result
            usage = result.get("usage") if isinstance(result, dict) else None
            yield {
                "_passthrough_responses": True,
                "body": responses_body,
                "usage": usage,
                "response_id": responses_body.get("id") if isinstance(responses_body, dict) else None,
            }

    async def send_sse_request(
        self,
        method: str,
        url: str,
        headers: dict,
        on_headers=None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """发送 SSE 流式请求"""
        kwargs.pop('chunked', None)
        response_headers_callback = kwargs.pop('response_headers_callback', None)
        router_request_headers_callback = kwargs.pop('router_request_headers_callback', None)
        router_request_body_callback = kwargs.pop('router_request_body_callback', None)
        router_request_path_callback = kwargs.pop('router_request_path_callback', None)
        router_response_body_callback = kwargs.pop('router_response_body_callback', None)
        if router_request_headers_callback:
            await self.record_router_request_headers(headers, {"router_request_headers_callback": router_request_headers_callback})
        if router_request_body_callback:
            request_body = {k: v for k, v in kwargs.items() if k in ("json", "data", "params")}
            await self.record_router_request_body(request_body, {"router_request_body_callback": router_request_body_callback})
        if router_request_path_callback:
            await self.record_router_request_path(url, {"router_request_path_callback": router_request_path_callback})
        original_on_headers = on_headers
        if response_headers_callback:
            async def _record_headers(headers):
                try:
                    await self.record_response_headers(headers, {"response_headers_callback": response_headers_callback})
                except Exception as e:
                    logger.warning(f"[BaseProvider.response_headers_callback] failed provider={self.PROVIDER_NAME}: {e}")
                if original_on_headers:
                    result = original_on_headers(headers)
                    if inspect.isawaitable(result):
                        await result
            on_headers = _record_headers
        buffer = b''

        session = self._get_session()
        try:
            request_context = session.request(
                method, url,
                headers=headers,
                chunked=True,
                proxy=self.proxy,
                ssl=False,
                timeout=self._stream_request_timeout(),
                **kwargs
            )
            async with request_context as response:
                headers_with_status = dict(response.headers)
                headers_with_status[":status"] = str(response.status)
                if on_headers:
                    result = on_headers(headers_with_status)
                    if inspect.isawaitable(result):
                        await result
                if response.status != 200:
                    body = await response.read()
                    message = body.decode(response.charset or 'utf-8', errors='replace')
                    await self.record_router_response_body(message, {"router_response_body_callback": router_response_body_callback})
                    logger.debug(f"send sse request error, method={method}, url={url}, status={response.status}, body_len={len(body)}")
                    raise HTTPException(status_code=response.status, detail=message)
                async for chunk in response.content.iter_any():
                    # 第一时间记录上游原始内容，便于排查渠道卡死 / 半截断问题；
                    # 不等 SSE 解析、不等请求结束。
                    chunk_text = chunk.decode('utf-8', errors='replace')
                    if chunk_text:
                        await self.record_router_response_body(chunk_text, {"router_response_body_callback": router_response_body_callback})
                    buffer += chunk
                    while b'\n\n' in buffer:
                        idx = buffer.find(b'\n\n')
                        event = buffer[:idx + 2].decode('utf-8')
                        yield event
                        buffer = buffer[idx + 2:]
                if buffer:
                    decoded = buffer.decode('utf-8', errors='replace')
                    if decoded.strip():
                        yield decoded
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=format_aiohttp_error(e, url)) from e

    @staticmethod
    def is_sse_comment_only(event_str: str) -> bool:
        has_line = False
        for line in event_str.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            has_line = True
            if not stripped.startswith(":"):
                return False
        return has_line

    @staticmethod
    def openai_sse_completed(event_str: str) -> bool:
        for event in iter_sse_payloads(event_str):
            if event == "[DONE]":
                return True
            if not isinstance(event, dict):
                continue
            for choice in event.get("choices", []) or []:
                if choice.get("finish_reason"):
                    return True
        return False

    @staticmethod
    def openai_error_sse(message: str, code: str = "upstream_incomplete_stream") -> str:
        payload = {"error": {"message": message, "type": code, "code": code}}
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    @staticmethod
    def anthropic_error_sse(message: str, code: str = "upstream_incomplete_stream") -> str:
        payload = {"type": "error", "error": {"type": "api_error", "message": message, "code": code}}
        return f"event: error\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    @staticmethod
    def parse_sse_data(event_str: str) -> tuple[str, str, list]:
        """解析 SSE 事件内容"""
        content = ""
        thinking = ""
        tool_calls = []

        try:
            for event in iter_sse_payloads(event_str):
                if event == "[DONE]" or not isinstance(event, dict):
                    continue

                # Qwen 格式
                if "data" in event and isinstance(event["data"], dict):
                    d = event["data"]
                    thinking += d.get("thinking") or d.get("reasoning") or d.get("reasoning_content") or ""
                    content += d.get("text") or d.get("content") or ""
                    if d.get("tool_calls"):
                        tool_calls.extend(d["tool_calls"])

                thinking += event.get("reasoning_content") or event.get("reasoning") or event.get("thinking") or ""

                # OpenAI 格式
                # 部分上游会发送 "choices": null 的帧（例如仅带 usage 的结束帧），
                # 需判断取值是否可迭代，避免 for 在 None 上抛 TypeError。
                choices = event.get("choices")
                if isinstance(choices, list):
                    for choice in choices:
                        if not isinstance(choice, dict):
                            continue
                        delta = choice.get("delta")
                        if not isinstance(delta, dict):
                            # 结束帧可能出现 "delta": [] 这类非法形态，兜底成空 dict。
                            delta = {}
                        message = choice.get("message")
                        if not isinstance(message, dict):
                            message = {}
                        content += delta.get("content") or ""
                        thinking += (
                            delta.get("reasoning_content")
                            or delta.get("reasoning")
                            or delta.get("thinking")
                            or message.get("reasoning_content")
                            or message.get("thinking")
                            or ""
                        )
                        if delta.get("tool_calls"):
                            tool_calls.extend(delta["tool_calls"])
        except json.JSONDecodeError as e:
            logger.warning(f"[BaseProvider.parse_sse_data] JSON decode failed: {e}, raw_len={len(event_str)}")

        return content, thinking, tool_calls

    @staticmethod
    def parse_sse_usage(event_str: str) -> dict | None:
        """从 SSE 事件中提取 usage（OpenAI 顶层/choices 嵌套 / Responses/Anthropic 嵌套 / SSE 注释行）。"""
        try:
            _, payload = parse_sse_event(event_str)
            if isinstance(payload, dict):
                usage = _extract_usage_payload(payload)
                if usage is not None:
                    return usage
        except (AttributeError, json.JSONDecodeError, ValueError):
            pass
        # 兜底：部分上游（如 MiniMax）把真实 token 统计放在 SSE 注释行（": {...}"）里，
        # 而 data: chunk 的 usage 恒为 null。仅当注释 JSON 带 token 字段时才当作 usage，
        # 避免把 keep-alive / ping 之类普通注释误判成 usage。
        return BaseProvider._parse_sse_comment_usage(event_str)

    @staticmethod
    def _parse_sse_comment_usage(event_str: str) -> dict | None:
        if not isinstance(event_str, str):
            return None
        token_keys = (
            "input_tokens", "output_tokens",
            "prompt_tokens", "completion_tokens", "total_tokens",
        )
        for line in event_str.splitlines():
            stripped = line.strip()
            if not stripped.startswith(":"):
                continue
            parsed = parse_sse_data_value(stripped[1:].strip())
            if isinstance(parsed, dict) and any(k in parsed for k in token_keys):
                return parsed
        return None

    @staticmethod
    def parse_sse_type(event_str: str) -> tuple[str, dict]:
        """解析 SSE 事件类型"""
        return parse_sse_event(event_str)

    @staticmethod
    def build_openai_response(
        completion_id: str,
        model: str,
        content: str,
        thinking: str = "",
        tool_calls: list = None
    ) -> dict:
        """构建 OpenAI 格式响应"""
        return make_openai_response(completion_id, model, content, thinking, tool_calls)

    @staticmethod
    def prepare_messages(messages: list[dict], tools_prompt: str = "") -> str:
        """准备消息内容"""
        return combine_messages(messages, tools_prompt)

    def get_latest_user_message(self, messages: list[dict], tools_prompt: str = "") -> str:
        """获取最后一条用户消息，拼接 tools_prompt 前缀。

        适用于使用服务端对话管理的渠道（Qwen/Xiaomi 等），每次请求只需发送当前用户
        消息，历史由服务端维护。

        客户端可能带了自己的 system（角色设定）。旧实现只取最后一条 user、把 system
        整个丢了——角色设定没了、只剩工具规则，同样是「两个提示词打架」的另一面。
        这里把客户端 system 一并带进 ``<system_instruction>``（角色设定在前、工具规则
        在后，用分隔线隔开），不丢角色设定。
        """
        # 找到最后一条 role=user 的消息
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = openai_content_to_text(msg.get("content", ""))
                # 收集客户端 system（可能多条，按序拼），与 tools_prompt 合并成一段说明
                client_system = "\n\n".join(
                    text for m in messages if m.get("role") == "system"
                    and (text := openai_content_to_text(m.get("content", "")).strip())
                )
                instruction = ""
                if client_system and tools_prompt:
                    instruction = f"{client_system}\n\n{_TOOLS_PROMPT_SEPARATOR}\n\n{tools_prompt}"
                elif client_system:
                    instruction = client_system
                elif tools_prompt:
                    instruction = tools_prompt
                if instruction:
                    return (
                        f"{MESSAGE_TAG_GUIDE}\n\n"
                        f"<system_instruction>\n{instruction}\n</system_instruction>\n\n"
                        f"<user_message>\n{content}\n</user_message>"
                    )
                return content
        # 兜底：没有 user 消息时使用原有拼接
        return self.prepare_messages(messages, tools_prompt)

    # 历史轮工具调用/结果的文本化渲染器。三 alias (xml/json/hermes) 语义已统一为
    # ```tool_function 围栏，render 调用产出一致；render_result 仍各自独立
    # ``<tool_result>`` 文本块。缺设 TOOLS_PROMPT_FORMAT 时回同一对。
    _TOOLS_PROMPT_RENDERERS = {
        "tool_function": (render_tool_call_xml, render_tool_result_xml),
        "xml": (render_tool_call_xml, render_tool_result_xml),
        "json": (render_tool_call_json, render_tool_result_json),
        "hermes": (render_tool_call_hermes, render_tool_result_hermes),
    }

    def tools_for_payload(self, tools: list[dict] | None) -> list[dict] | None:
        """返回该发给上游 payload 的 ``tools``；tools-as-prompt 渠道返回 None。

        ``TOOLS_AS_PROMPT = True`` 时**绝不能**把 ``tools`` 放进上游请求体：上游一旦
        收到该字段就会启用**它自己的服务端 function-calling**，去查自家工具注册表，
        而本地工具（如 ``get_weather``）在它那边不存在，直接回「工具名称不存在: xxx」。
        这是字段驱动的，靠提示词措辞（"不要调用你自己的工具"）拦不住。

        各渠道自己拼 body，故由渠道在拼 payload 时调用本方法取值，语义集中在一处，
        避免每个渠道各写一遍开关判断。
        """
        if self.TOOLS_AS_PROMPT:
            return None
        return tools or []

    def flatten_tool_history(self, messages: list[dict]) -> list[dict]:
        """把历史轮的原生工具结构拍成文本（tools-as-prompt 渠道用）。

        当前轮不发原生 ``tools``，历史轮就同样不能留原生结构，否则一样会触发上游
        自己的 function-calling 机制：
        - assistant 的 ``tool_calls`` -> 按提示词同形态追加进 ``content``；
        - ``role=tool`` 的结果 -> 转成 ``role=user`` 文本。
        这样多轮对话对「不认 function calling 的上游」自洽。
        """
        render_call, render_result = self._TOOLS_PROMPT_RENDERERS.get(
            self.TOOLS_PROMPT_FORMAT, self._TOOLS_PROMPT_RENDERERS["tool_function"]
        )
        normalized: list[dict] = []
        for msg in messages:
            role = msg.get("role")
            if role == "assistant" and msg.get("tool_calls"):
                rendered_parts: list[str] = []
                dropped = 0
                for tc in msg["tool_calls"]:
                    # wire shape = {id, type, function:{name, arguments}}. A
                    # display-shape entry ({name, args, status}) has no
                    # function.name and is silently useless to the upstream —
                    # drop it but make the loss visible so a misbehaving caller
                    # (e.g. an agent replay path that forgot to normalize) is
                    # discoverable in logs instead of degrading into a blank
                    # assistant turn that the model fills with hallucination.
                    if not isinstance(tc, dict) or not (tc.get("function") or {}).get("name"):
                        dropped += 1
                        continue
                    rendered_parts.append(render_call(tc))
                if dropped:
                    logger.warning(
                        "flatten_tool_history: dropped {} malformed tool_call(s) on an "
                        "assistant message — entries lacked function.name. The caller must "
                        "emit wire shape {{id, type, function: {{name, arguments}}}} plus "
                        "paired role=tool messages; see agent.context_manager.rebuild_history_messages.",
                        dropped,
                    )
                rendered = "".join(rendered_parts)
                new_msg = {k: v for k, v in msg.items() if k != "tool_calls"}
                new_msg["content"] = (msg.get("content") or "") + rendered
                normalized.append(new_msg)
            elif role == "tool":
                normalized.append({"role": "user", "content": render_result(
                    msg.get("tool_call_id") or "",
                    msg.get("name") or "",
                    str(msg.get("content") or ""),
                )})
            else:
                normalized.append(msg)
        return normalized

    def prepare_provider_messages(self, messages: list[dict], tools_prompt: str = "") -> list[dict]:
        """为支持多 messages 的渠道准备消息，避免丢失 reasoning_content/tool_calls 等字段。

        ``TOOLS_AS_PROMPT = True`` 的渠道自动做历史轮文本化（见 flatten_tool_history），
        渠道侧无需覆盖本方法。

        tools_prompt 的拼接口径（避免「两个 system 打架」）：客户端可能已在 messages 里
        带了自己的 system（角色设定）。若直接**前置一条新 system** 装 tools_prompt，就会
        出现两条 system——模型分不清谁优先，弱模型尤其乱。所以这里**合并**：已有 system
        时把 tools_prompt 追加进第一条 system 的 content（客户端角色在前、工具规则在后，
        用分隔线隔开，读起来是一段连贯的 system）；没有 system 时才前置一条新 system。
        """
        if self.TOOLS_AS_PROMPT:
            messages = self.flatten_tool_history(messages)
        if not tools_prompt:
            return messages
        merged: list[dict] = list(messages)
        if merged and merged[0].get("role") == "system":
            client_system = openai_content_to_text(merged[0].get("content", ""))
            combined = (
                f"{client_system}\n\n{_TOOLS_PROMPT_SEPARATOR}\n\n{tools_prompt}"
                if client_system.strip() else tools_prompt
            )
            merged[0] = {**merged[0], "content": combined}
            return merged
        return [{"role": "system", "content": tools_prompt}, *merged]

    def build_tools_prompt(self, tools: list[dict], format_type: str | None = None) -> str:
        """构建工具 prompt。缺省形态取类级 ``TOOLS_PROMPT_FORMAT``（默认 tool_function）。

        format_type:
        - ``tool_function``（默认）：教模型吐 ```tool_function 围栏。意图驱动——围栏
          是模型明确表示「这里要调用工具」的唯一信号，解析器只认围栏内的内容，
          围栏外的裸 JSON / ```json 代码块 / XML / Hermes 标签一律不当工具调用。
        - ``xml`` / ``json`` / ``hermes``：兼容 alias，语义已收敛为同一 ```tool_function
          围栏，保留取值供既有渠道配置不改代码即生效。

        形态的解析都由框架负责且流式 / 非流式同源：``BaseProvider.chat`` 在
        请求带 tools 时缓冲切块成 tool_calls delta，非流式兜底走
        ``self.parse_tool_calls``。模型吐的脏 JSON（缺引号 / 尾随逗号 / 被流截断）
        由 json_repair 容错还原。
        """
        return generate_tools_prompt(
            tools, format_type=format_type or self.TOOLS_PROMPT_FORMAT
        )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    @staticmethod
    def parse_tool_calls(content: str) -> tuple[str, list]:
        """解析工具调用"""
        return parse_tool_calls_from_content(content)
