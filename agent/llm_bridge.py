"""LLM Bridge — agent 服务侧的 LLM HTTP 客户端。

基于「LLM 管理」配置（agent_llm_configs：base_url / chat_path / api_key / models）
直接通过 HTTP 调用上游 LLM，**不走主系统 providers/渠道体系**。

不在 agent 侧写 request_logs：上游 LLM 服务本身会记录，这里再记会重复。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncGenerator, Awaitable, Callable

import aiohttp

from message_utils import iter_sse_payloads
from providers.base import make_insecure_connector
from usage_utils import merge_usage

# 可重试的网络异常：连接失败/断流/超时。asyncio.TimeoutError 在 3.11+ 即内置
# TimeoutError，覆盖 ClientTimeout 触发的那类超时。非 200（_LLMHttpError）不在
# 此列——那属于「其他异常」，由 agent_runner_loop 层决定是否重试。
NETWORK_EXCEPTIONS = (aiohttp.ClientError, asyncio.TimeoutError)
_NETWORK_EXC = NETWORK_EXCEPTIONS  # 内部别名

# 指数退避（内部常量，不暴露为配置）：delay = min(base * 2**attempt, cap)
_RETRY_BACKOFF_BASE_MS = 1000
_RETRY_BACKOFF_CAP_MS = 8000


def backoff_delay_ms(attempt: int) -> int:
    """第 attempt 次重试前的退避毫秒数（attempt 从 0 开始）。"""
    return min(_RETRY_BACKOFF_BASE_MS * (2 ** attempt), _RETRY_BACKOFF_CAP_MS)


_backoff_delay_ms = backoff_delay_ms  # 内部别名


# on_retry 回调类型：桥本身不持有 on_event，靠此回调把重试信息冒泡给上层。
OnRetry = Callable[[dict], Awaitable[None]]


@dataclass(slots=True)
class LLMResponse:
    content: str = ""
    tool_calls: list[dict] = field(default_factory=list)
    usage: dict | None = None
    finish_reason: str | None = None
    raw: dict = field(default_factory=dict)
    # 思考/推理增量。与 content 分开承载：只用于实时展示与落库，绝不并入
    # 回灌给上游的 assistant content（否则会把思考当正文喂回去）。
    reasoning: str = ""


def _extract_reasoning(source: dict) -> str:
    """从 delta / message 里取思考文本。

    各上游字段名不统一：OpenAI 系 reasoning_content，部分兼容层用 reasoning 或
    thinking。providers 层已把它们收敛进 delta，这里按同样的优先级读。
    """
    if not isinstance(source, dict):
        return ""
    for key in ("reasoning_content", "reasoning", "thinking"):
        value = source.get(key)
        if isinstance(value, str) and value:
            return value
    return ""



class LLMBridge:
    """基于 LLM 配置的 OpenAI 兼容 HTTP 客户端。

    llm_config: agent_llm_configs 行 dict，含 base_url/chat_path/api_key/protocol/name。
    """

    def __init__(self, llm_config: dict, model: str, *, session_id: str | None = None):
        self.llm_config = llm_config or {}
        self.model = model
        self.session_id = session_id
        self.base_url = (self.llm_config.get("base_url") or "").rstrip("/")
        self.chat_path = self.llm_config.get("chat_path") or "/chat/completions"
        self.api_key = self.llm_config.get("api_key") or ""
        self.name = self.llm_config.get("name") or "llm"
        self.protocol = (self.llm_config.get("protocol") or "openai").lower()
        # 连接级超时/重试（可在「LLM 管理」配置），独立于主系统 retry 体系。
        # 缺省走 DB 列默认值（600 / 2）；显式 0 是有效值（关闭重试），故用 _DEFAULT 兜底而非 `or`。
        self.timeout_seconds = int(self.llm_config.get("timeout_seconds") if self.llm_config.get("timeout_seconds") is not None else 600)
        self.max_retries = max(0, int(self.llm_config.get("max_retries") if self.llm_config.get("max_retries") is not None else 2))
        # agent 级思考模式（随 Agent 走，注入 LLM 请求体；fork 会逐字拷贝 self.llm_config 自动继承）
        self.thinking_enabled = bool(self.llm_config.get("thinking_enabled"))
        self.reasoning_effort = (str(self.llm_config.get("reasoning_effort") or "")).strip()

    def fork(
        self,
        model: str | None = None,
        *,
        session_id: str | None = None,
        thinking_enabled: bool | None = None,
        reasoning_effort: str | None = None,
    ) -> "LLMBridge":
        config = dict(self.llm_config)
        if thinking_enabled is not None:
            config["thinking_enabled"] = thinking_enabled
        if reasoning_effort is not None:
            config["reasoning_effort"] = reasoning_effort
        return LLMBridge(
            config,
            self.model if model is None else model,
            session_id=self.session_id if session_id is None else session_id,
        )

    def _endpoint(self) -> str:
        path = self.chat_path if self.chat_path.startswith("/") else f"/{self.chat_path}"
        return f"{self.base_url}{path}"

    def _make_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
            connector=make_insecure_connector(),
        )

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _build_body(self, messages: list[dict], tools: list[dict] | None, stream: bool) -> dict:
        body: dict = {"model": self.model, "messages": messages, "stream": stream}
        if tools:
            body["tools"] = tools
        # agent 级思考模式：openai/chat 走 reasoning_effort；responses 走 reasoning.effort。
        # 其他协议（含 anthropic）本次不注入，避免误伤 OpenAI 兼容端点。
        if self.thinking_enabled and self.reasoning_effort:
            if self.protocol in ("openai", "chat"):
                body["reasoning_effort"] = self.reasoning_effort
            elif self.protocol == "responses":
                body["reasoning"] = {"effort": self.reasoning_effort}
        return body

    async def _emit_retry(
        self,
        on_retry: OnRetry | None,
        *,
        attempt: int,
        error: Exception,
        scope: str,
        delay_ms: int,
    ) -> None:
        if not on_retry:
            return
        await on_retry({
            "attempt": attempt,
            "max": self.max_retries,
            "reason": self._format_exception(error),
            "scope": scope,
            "delay_ms": delay_ms,
        })

    async def chat(self, messages: list[dict], tools: list[dict] | None = None, on_retry: OnRetry | None = None) -> LLMResponse:
        """非流式调用上游 LLM，返回 LLMResponse。失败抛异常（保真上游错误信息）。

        不在此记录 request_log：LLM 上游服务本身会记录，agent 侧再记会重复。
        """
        body = self._build_body(messages, tools, stream=False)
        for retry_index in range(self.max_retries + 1):
            try:
                async with self._make_session() as session:
                    async with session.post(self._endpoint(), headers=self._headers(), json=body) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            error_text = self._extract_error_message(text) or f"HTTP {resp.status}"
                            raise _LLMHttpError(resp.status, error_text, str(resp.status))
                        try:
                            payload = await resp.json()
                        except Exception:
                            raise _LLMHttpError(502, "invalid JSON response", str(resp.status))
                return self._response_from_dict(payload or {})
            except _NETWORK_EXC as exc:
                if retry_index >= self.max_retries:
                    raise
                delay_ms = _backoff_delay_ms(retry_index)
                await self._emit_retry(
                    on_retry,
                    attempt=retry_index + 1,
                    error=exc,
                    scope="network",
                    delay_ms=delay_ms,
                )
                await asyncio.sleep(delay_ms / 1000)
        raise RuntimeError("unreachable LLMBridge.chat retry state")

    async def stream_chat(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
        on_retry: OnRetry | None = None,
    ) -> AsyncGenerator[LLMResponse, None]:
        """流式调用上游 LLM，逐 chunk yield LLMResponse。

        不在此记录 request_log：LLM 上游服务本身会记录。
        """
        body = self._build_body(messages, tools, stream=True)
        for retry_index in range(self.max_retries + 1):
            # 已 yield 过任何 chunk 后再断流 → 不能重试（会重复内容），直接抛。
            # 只对「建连 + 状态检查 + 首个 chunk 之前」的网络异常做重试。
            yielded_any = False
            try:
                async with self._make_session() as session:
                    async with session.post(self._endpoint(), headers=self._headers(), json=body) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            error_text = self._extract_error_message(text) or f"HTTP {resp.status}"
                            raise _LLMHttpError(resp.status, error_text, str(resp.status))
                        tool_call_state: dict[int, dict] = {}
                        usage: dict | None = None
                        async for raw_line in resp.content:
                            if not raw_line:
                                continue
                            line = raw_line.decode("utf-8", errors="replace")
                            for payload in iter_sse_payloads(line):
                                if payload == "[DONE]":
                                    continue
                                if not isinstance(payload, dict):
                                    continue
                                if isinstance(payload.get("usage"), dict):
                                    usage = merge_usage(usage, payload["usage"]) if usage else payload["usage"]
                                    yielded_any = True
                                    yield LLMResponse(usage=usage, raw=payload)
                                for choice in payload.get("choices", []) or []:
                                    delta = choice.get("delta") or {}
                                    content = delta.get("content") or ""
                                    reasoning = _extract_reasoning(delta)
                                    tool_calls = self._merge_stream_tool_calls(tool_call_state, delta.get("tool_calls") or [])
                                    finish_reason = choice.get("finish_reason") or ""
                                    if content or reasoning or tool_calls or finish_reason:
                                        yielded_any = True
                                        yield LLMResponse(content=content, reasoning=reasoning, tool_calls=tool_calls, finish_reason=finish_reason or None, raw=payload)
                return
            except _NETWORK_EXC as exc:
                # 已吐过 chunk 后再断流 → 重试会重复内容，直接抛给上层。
                if yielded_any or retry_index >= self.max_retries:
                    raise
                delay_ms = _backoff_delay_ms(retry_index)
                await self._emit_retry(
                    on_retry,
                    attempt=retry_index + 1,
                    error=exc,
                    scope="network",
                    delay_ms=delay_ms,
                )
                await asyncio.sleep(delay_ms / 1000)

    # ── 工具方法 ──────────────────────────────────────────────
    @staticmethod
    def _format_exception(exc: Exception) -> str:
        """把异常转成人类可读摘要，供 retry 事件的 reason 展示。

        裸的 TimeoutError 之类 str() 为空，补上类型名，避免前端显示空原因。
        """
        msg = str(exc).strip()
        name = type(exc).__name__
        return f"{name}: {msg}" if msg else name

    @staticmethod
    def _extract_error_message(text: str) -> str:
        try:
            import json as _json
            data = _json.loads(text)
            err = data.get("error") if isinstance(data, dict) else None
            if isinstance(err, dict) and err.get("message"):
                return str(err["message"])
            if isinstance(data, dict) and data.get("message"):
                return str(data["message"])
        except Exception:
            pass
        return text[:200]

    @staticmethod
    def _response_from_dict(payload: dict) -> LLMResponse:
        choices = payload.get("choices") or []
        first_choice = choices[0] if choices else {}
        message = first_choice.get("message") or {}
        return LLMResponse(
            content=message.get("content") or "",
            reasoning=_extract_reasoning(message),
            tool_calls=list(message.get("tool_calls") or []),
            usage=payload.get("usage") if isinstance(payload.get("usage"), dict) else None,
            raw=payload,
        )

    @staticmethod
    def _merge_stream_tool_calls(state: dict[int, dict], deltas: list[dict]) -> list[dict]:
        changed: list[dict] = []
        for delta_call in deltas:
            if not isinstance(delta_call, dict):
                continue
            index = int(delta_call.get("index", 0))
            current = state.setdefault(index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
            if delta_call.get("id"):
                current["id"] = delta_call["id"]
            if delta_call.get("type"):
                current["type"] = delta_call["type"]
            function_delta = delta_call.get("function") or {}
            function_state = current.setdefault("function", {"name": "", "arguments": ""})
            if function_delta.get("name"):
                function_state["name"] = function_delta["name"]
            if function_delta.get("arguments"):
                function_state["arguments"] += function_delta["arguments"]
            changed.append({
                "id": current.get("id") or "",
                "type": current.get("type") or "function",
                "function": {
                    "name": function_state.get("name") or "",
                    "arguments": function_state.get("arguments") or "",
                },
            })
        return changed


class _LLMHttpError(Exception):
    """上游 LLM 返回非 200，承载 status_code 与保真错误信息。"""

    def __init__(self, status_code: int, message: str, upstream_status: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.upstream_status = upstream_status or str(status_code)
        self.message = message


class GatewayLLMBridge:
    """走本系统网关（main.dispatch_entry）的 LLM 客户端，与 LLMBridge 同接口。

    与 LLMBridge 的区别：不直连上游 base_url，而是用一个**网关 api_keys 明文 key**
    调主链路 dispatch_entry（/v1/chat/completions）。这样 agent 的每次 LLM 调用都
    经过网关的限流（TPM/并发）、计费、请求日志与归属，形成"谁在用、用了多少"的
    管理闭环。模型名必须是网关（providers）提供的模型。

    agent_runner_loop 只依赖 chat / stream_chat / max_retries / model 接口，故本类
    可与 LLMBridge 互换。网关自身的换候选/组回退在一次 dispatch 调用内独立完成；
    本层 max_retries 是 agent 层对**瞬时 429** 的退避重试次数（由
    AgentConfig.llm_retry_429 注入），在 agent_loop 里门控，与网关重试不叠加：
    配置类 429（no_available_account）不重试，瞬时码（rate_limit_exceeded/
    service_busy/concurrent_limit_exceeded 等）带退避重试 max_retries 次。
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_key_name: str = "",
        model: str,
        provider_whitelist: set[str] | None = None,
        provider_blacklist: set[str] | None = None,
        thinking_enabled: bool = False,
        reasoning_effort: str = "",
        session_id: str | None = None,
        max_retries: int = 0,
    ):
        self.api_key = api_key or ""
        self.api_key_name = api_key_name or ""
        self.model = model
        self.session_id = session_id
        self.provider_whitelist = set(provider_whitelist or set())
        self.provider_blacklist = set(provider_blacklist or set())
        self.thinking_enabled = bool(thinking_enabled)
        self.reasoning_effort = (str(reasoning_effort or "")).strip()
        # agent 层 429 退避重试次数（AgentConfig.llm_retry_429）；0=关闭。
        # agent_loop 读取此值决定循环级重试次数。
        self.max_retries = max(0, int(max_retries or 0))
        # 与 LLMBridge 对齐的展示字段，供日志/调试用。
        self.name = self.api_key_name or "gateway"
        self.protocol = "openai"

    def fork(
        self,
        model: str | None = None,
        *,
        session_id: str | None = None,
        thinking_enabled: bool | None = None,
        reasoning_effort: str | None = None,
    ) -> "GatewayLLMBridge":
        """派生桥，可按本轮覆盖模型/会话/思考配置。"""
        return GatewayLLMBridge(
            api_key=self.api_key,
            api_key_name=self.api_key_name,
            model=self.model if model is None else model,
            provider_whitelist=self.provider_whitelist,
            provider_blacklist=self.provider_blacklist,
            thinking_enabled=self.thinking_enabled if thinking_enabled is None else thinking_enabled,
            reasoning_effort=self.reasoning_effort if reasoning_effort is None else reasoning_effort,
            session_id=self.session_id if session_id is None else session_id,
            max_retries=self.max_retries,
        )

    def _build_body(self, messages: list[dict], tools: list[dict] | None, stream: bool) -> dict:
        body: dict = {"model": self.model, "messages": messages, "stream": stream}
        if tools:
            body["tools"] = tools
        # 思考模式：openai 协议走 reasoning_effort，dispatch_entry 的 _build_protocol_kwargs 会透传。
        if self.thinking_enabled and self.reasoning_effort:
            body["reasoning_effort"] = self.reasoning_effort
        return body

    async def _dispatch(self, messages: list[dict], tools: list[dict] | None, stream: bool) -> dict:
        import main as _main

        body = self._build_body(messages, tools, stream)
        # 不向 dispatch_entry 传 on_retry：网关内部重试（换候选 / 渠道内层 / 模型组
        # 不可用 / 回退备份组）是 LLM 服务的内部容错，和普通 /v1/chat/completions 调用
        # 一样后台静默进行，不该冒泡到 agent 对话界面。网关自身已按 request_id 把
        # 每次尝试落 request_logs，要看链路走管理端请求详情，不走用户对话。
        return await _main.dispatch_entry(
            endpoint="/v1/chat/completions",
            body=body,
            headers=None,
            api_key=self.api_key,
            api_key_name=self.api_key_name,
            provider_whitelist=self.provider_whitelist,
            provider_blacklist=self.provider_blacklist,
            request_protocol="openai",
            chat_method="chat",
            client_type="agent",
        )

    async def chat(self, messages: list[dict], tools: list[dict] | None = None) -> LLMResponse:
        """非流式：用网关明文 key 调主链路，收集整段结果转 LLMResponse。

        网关的限流/计费/日志/内部重试在 dispatch_entry 内完成，本层只解析结果。
        网关内部重试静默，不向 agent 冒泡（与普通 /v1/chat/completions 调用一致）。
        """
        import main as _main

        dispatch = await self._dispatch(messages, tools, stream=False)
        result = await _main._collect_non_stream_result(dispatch["generator"])
        if isinstance(result, dict):
            result.pop("_last_route_info", None)
            result.pop("_first_token_ms", None)
        return LLMBridge._response_from_dict(result if isinstance(result, dict) else {})

    async def stream_chat(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
    ) -> AsyncGenerator[LLMResponse, None]:
        """流式：迭代网关 SSE 生成器，复用 LLMBridge 的 SSE 解析逐 chunk yield LLMResponse。"""
        dispatch = await self._dispatch(messages, tools, stream=True)
        generator = dispatch["generator"]
        if not dispatch.get("stream"):
            # 上游按非流式返回（dispatch 判定 stream=False）：整段收集后一次性 yield。
            import main as _main

            result = await _main._collect_non_stream_result(generator)
            if isinstance(result, dict):
                result.pop("_last_route_info", None)
                result.pop("_first_token_ms", None)
            yield LLMBridge._response_from_dict(result if isinstance(result, dict) else {})
            return

        tool_call_state: dict[int, dict] = {}
        usage: dict | None = None
        async for chunk in generator:
            # 路由信息标记 dict 不是内容，跳过。
            if isinstance(chunk, dict):
                continue
            if not chunk:
                continue
            for payload in iter_sse_payloads(chunk):
                if payload == "[DONE]":
                    continue
                if not isinstance(payload, dict):
                    continue
                if isinstance(payload.get("usage"), dict):
                    usage = merge_usage(usage, payload["usage"]) if usage else payload["usage"]
                    yield LLMResponse(usage=usage, raw=payload)
                for choice in payload.get("choices", []) or []:
                    delta = choice.get("delta") or {}
                    content = delta.get("content") or ""
                    reasoning = _extract_reasoning(delta)
                    tool_calls = LLMBridge._merge_stream_tool_calls(tool_call_state, delta.get("tool_calls") or [])
                    finish_reason = choice.get("finish_reason") or ""
                    if content or reasoning or tool_calls or finish_reason:
                        yield LLMResponse(content=content, reasoning=reasoning, tool_calls=tool_calls, finish_reason=finish_reason or None, raw=payload)
