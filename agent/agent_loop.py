"""Agent runner loop — the core execution engine.

Enhanced with GenericAgent-aligned mechanisms:
- Anchor prompt injection (working memory, turn counter)
- no_tool handling (max_tokens truncation, large code block detection)
- turn_end_callback (summary extraction, DANGER warnings)
- Context management (compress + trim before LLM call)
- Hook trigger points (7 hooks)
- ask_user support
"""
from __future__ import annotations

import asyncio
import inspect
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import aiohttp
from loguru import logger

from agent.context_manager import compress_history, serialize_tool_result, trim_messages
from agent.hooks import hooks

_NETWORK_EXC = (aiohttp.ClientError, asyncio.TimeoutError)
_RETRY_BACKOFF_BASE_MS = 1000
_RETRY_BACKOFF_CAP_MS = 8000


def _backoff_delay_ms(attempt: int) -> int:
    return min(_RETRY_BACKOFF_BASE_MS * (2 ** attempt), _RETRY_BACKOFF_CAP_MS)


# agent 层 429 退避重试：仅这些业务码重试（与 main.py:_RETRYABLE_CODES 对齐，
# 本地定义避免 import main 形成环）。配置类 no_available_account 不在内 →
# 不重试（白名单/禁用，重试无意义），直接抛交手动重试。
_RETRYABLE_429_CODES = {
    "rate_limit_exceeded",
    "concurrent_limit_exceeded",
    "service_busy",
    "retry_exhausted",
    "incomplete_response",
    "upstream_exception",
    "api_key_rate_limit_exceeded",
    "api_key_concurrent_limit_exceeded",
}


def _http_error_retry_after_ms(exc: BaseException) -> int | None:
    """从 429 detail 取 retry_after（秒），转毫秒；无则 None（退化为指数退避）。"""
    try:
        detail = getattr(exc, "detail", None)
    except Exception:  # noqa: BLE001
        detail = None
    if isinstance(detail, dict):
        ra = detail.get("retry_after")
        if isinstance(ra, (int, float)) and ra > 0:
            return int(ra * 1000)
    return None


def _is_retryable_429(exc: BaseException) -> bool:
    """HTTPException(429) 且业务码属可重试集合 → True。

    - NoAvailableAccountError（HTTPException 子类，detail 为字符串）：取 .code
      属性；no_available_account 不在可重试集合 → False（配置类不重试）。
    - 普通 HTTPException(429)：detail 为 dict 时取 detail["code"]；detail 为字符串
      时无 code → 视为 no_available_account 文本路径 → False。
    """
    try:
        from fastapi import HTTPException
    except Exception:  # noqa: BLE001
        return False
    if not isinstance(exc, HTTPException) or getattr(exc, "status_code", None) != 429:
        return False
    code = getattr(exc, "code", None)
    if not code:
        detail = getattr(exc, "detail", None)
        code = detail.get("code") if isinstance(detail, dict) else None
    return bool(code) and code in _RETRYABLE_429_CODES



# 工具执行后若未显式给出 next_prompt，用此默认提示把工具结果喂回 LLM，
# 让其在下一轮基于结果继续或给出最终回答。通用 ToolRegistry 工具不会主动
# 设置 next_prompt，因此「无 next_prompt」不能被当作任务完成（否则会在第一次
# 工具调用后就提前结束，看不到最终回答）。任务结束由以下三种情况决定：
# 模型返回纯文本（无 tool_calls）、should_exit、或达到 max_turns。
_DEFAULT_CONTINUE_PROMPT = (
    "请根据上面的工具结果继续完成用户的任务；"
    "如果任务已经完成，请直接给出最终回答（不要再调用工具）。"
)

# 连续空响应 / 连续 max_tokens 截断的容忍上限，超过则以 ERROR 收尾而不是
# 假装任务完成或一直 continue 到 max_turns。
_MAX_CONSECUTIVE_EMPTY = 3
_MAX_CONSECUTIVE_TRUNCATED = 3


@dataclass
class StepOutcome:
    data: Any = None
    next_prompt: str | None = None
    should_exit: bool = False


class BaseHandler:
    def __init__(self, tools_registry, max_turns: int = 40):
        self.max_turns = max_turns
        self.tools_registry = tools_registry
        self.current_turn = 0
        # Working memory (GA ch10 anchor components)
        self.goal: str = ""
        self.completed: list[str] = []
        self.current_state: str = ""
        self.next_steps: list[str] = []
        self.key_info: str = ""
        self.related_files: list[str] = []
        # History info for summary tracking
        self.history_info: list[str] = []
        # Consecutive empty / truncated response counters (three-strike guards)
        self._consecutive_empty: int = 0
        self._consecutive_truncated: int = 0

    async def dispatch(
        self,
        tool_name: str,
        args: dict,
        response,
        index: int = 0,
        tool_num: int = 1,
    ) -> StepOutcome:
        # Hook: tool_before
        if hooks.has_hooks("tool_before"):
            await hooks.trigger("tool_before", {
                "tool_name": tool_name,
                "args": args,
                "response": response,
                "index": index,
                "turn": self.current_turn,
            })

        method_name = f"do_{tool_name}"
        if hasattr(self, method_name):
            outcome = await getattr(self, method_name)(args, response)
            if not isinstance(outcome, StepOutcome):
                outcome = StepOutcome(data=outcome)
        elif self.tools_registry is None:
            raise NotImplementedError(f"No handler for tool '{tool_name}'")
        else:
            # Some tools gate on how far the task has progressed (long-term memory
            # distillation is meaningless a couple of turns in), so the registry needs
            # the turn number the loop owns.
            self.tools_registry.current_turn = self.current_turn
            # 本轮工具数决定每个结果的字符预算：一轮 5 个 file_read 不能各自吃满窗口。
            self.tools_registry.current_tool_num = max(1, tool_num)
            result = await self.tools_registry.execute(tool_name, args)
            outcome = result if isinstance(result, StepOutcome) else StepOutcome(data=result)

        # Update the in-process working anchor from the GA-shaped result.
        if tool_name == "update_working_checkpoint" and isinstance(outcome.data, dict):
            data = outcome.data
            if data.get("goal") is not None:
                self.goal = str(data.get("goal") or "")
            if isinstance(data.get("completed"), list):
                self.completed = [str(v) for v in data["completed"]]
            if data.get("current_state") is not None:
                self.current_state = str(data.get("current_state") or "")
            if isinstance(data.get("next_steps"), list):
                self.next_steps = [str(v) for v in data["next_steps"]]
            if data.get("key_info") is not None:
                self.key_info = str(data.get("key_info") or "")
            if isinstance(data.get("related_files"), list):
                self.related_files = [str(v) for v in data["related_files"]]

        # Hook: tool_after
        if hooks.has_hooks("tool_after"):
            await hooks.trigger("tool_after", {
                "tool_name": tool_name,
                "args": args,
                "outcome": outcome,
                "index": index,
                "turn": self.current_turn,
            })

        return outcome

    # Recent turn summaries replayed verbatim in the anchor; older ones are folded.
    _HISTORY_WINDOW = 30

    def _fold_earlier(self, lines: list[str]) -> str:
        """Collapse pre-window turns into one line per user request.

        A run of consecutive agent turns carries little value individually once it has
        scrolled out of the window, but the fact that N turns happened after a given
        request does. So each `[USER]` line is kept and the agent turns after it become
        ``<last summary>（N turns）``.
        """
        parts: list[str] = []
        count = 0
        last = ""

        def flush() -> None:
            if count:
                parts.append(f"{last}（{count} turns）" if last else f"[Agent]（{count} turns）")

        for line in lines:
            if line.startswith("[USER]"):
                flush()
                parts.append(line)
                count = 0
                last = ""
            else:
                count += 1
                last = line
        flush()
        return "\n".join(parts[-70:])

    def build_anchor_prompt(self) -> str:
        """Build the GA working-memory anchor for injection after tool calls.

        Injection is throttled by turn parity the way GA does it: the replayed turn
        history lands on odd turns and the folded earlier context every fourth turn,
        while the checkpoint fields (goal / state / key_info) go out every turn. The
        cheap always-on part is what keeps the task anchored; resending the whole
        history each turn would spend the context budget re-stating what the model just
        did.
        """
        turn = self.current_turn
        parts = [f"turn: {turn}/{self.max_turns}"]
        if self.goal:
            parts.append(f"goal: {self.goal}")
        if self.completed:
            parts.append("completed: " + "; ".join(self.completed))
        if self.current_state:
            parts.append(f"current_state: {self.current_state}")
        if self.next_steps:
            parts.append("next_steps: " + "; ".join(self.next_steps))
        if self.key_info:
            parts.append(f"key_info: {self.key_info}")
        if self.related_files:
            parts.append(
                "related_files: " + ", ".join(self.related_files)
                + " (Re-read via file_read)"
            )
        blocks = ["[working memory]\n" + "\n".join(parts)]
        history = self.history_info
        # Older-than-window context, folded, on every 4th turn.
        if len(history) > self._HISTORY_WINDOW and turn % 4 == 1:
            folded = self._fold_earlier(history[: -self._HISTORY_WINDOW])
            if folded:
                blocks.append(f"<earlier_context>\n{folded}\n</earlier_context>")
        # Recent turn summaries on odd turns.
        if history and turn % 2 == 1:
            recent = "\n".join(history[-self._HISTORY_WINDOW:])
            blocks.append(f"<history>\n{recent}\n</history>")
        return "\n".join(blocks)


def _extract_summary(content: str, tool_calls: list[dict]) -> tuple[str, bool]:
    """Extract a turn summary, and report whether the model supplied the tag.

    Returns ``(summary, tagged)``. ``tagged`` is False when no ``<summary>`` tag was
    present, which lets the caller nudge the model — the working-memory anchor replays
    these lines after earlier context is evicted, so a model-authored summary is worth
    far more than the ``[Tool: x]`` fallback we can synthesise here.
    """
    if not content:
        content = ""

    # Strip code blocks and thinking
    clean = re.sub(r'```[\s\S]*?```', '', content)
    clean = re.sub(r'<thinking>[\s\S]*?</thinking>', '', clean)

    # Try summary tag
    match = re.search(r'<summary>(.*?)</summary>', clean, re.DOTALL)
    if match:
        return match.group(1).strip()[:200], True

    # Fallback: first tool call
    if tool_calls:
        tc = tool_calls[0]
        name = tc.get("function", {}).get("name", "unknown")
        if name != "no_tool":
            return f"[Tool: {name}]", False

    return "Responded without tool call", False


def _detect_max_tokens_truncation(response) -> bool:
    """Check if LLM output was truncated by max_tokens."""
    # Check stop_reason if available
    stop = getattr(response, "stop_reason", None) or getattr(response, "finish_reason", None)
    if stop and "max_tokens" in str(stop).lower():
        return True
    # Check content for truncation markers
    content = getattr(response, "content", "") or ""
    if content and ("max_tokens !!!]" in content[-100:] or "[!!! 流异常中断" in content[-100:]):
        return True
    return False


def _detect_large_code_block(content: str) -> bool:
    """Detect if response is just a large code block with minimal explanation."""
    if not content:
        return False
    code_blocks = re.findall(r'```(\w*)\n(.*?)```', content, re.DOTALL)
    if len(code_blocks) != 1:
        return False
    code_content = code_blocks[0][1]
    if len(code_content) < 50:
        return False
    # Check if residual text (outside code block) is very short
    plain = re.sub(r'```[\s\S]*?```', '', content, flags=re.DOTALL)
    plain = re.sub(r'<thinking>[\s\S]*?</thinking>', '', plain)
    plain = re.sub(r'<summary>[\s\S]*?</summary>', '', plain)
    return len(plain.strip()) < 30


async def agent_runner_loop(
    client,
    system_prompt: str,
    user_input: str,
    handler: BaseHandler,
    tools_schema: list[dict],
    max_turns: int = 40,
    verbose: bool = True,
    yield_info: bool = False,
    initial_user_content: str | None = None,
    on_event: Callable[[dict], Awaitable[None]] | None = None,
    initial_messages: list[dict] | None = None,
    context_window: int = 90000,
    on_tool_batch: Callable[[list[dict]], Awaitable[None]] | None = None,
):
    """Core agent loop: LLM → tools → repeat.

    Args:
        client: LLM client with async chat() method
        system_prompt: System prompt
        user_input: User's input text
        handler: Tool handler (BaseHandler subclass)
        tools_schema: OpenAI function-calling tool schemas
        max_turns: Maximum turns before forced exit
        yield_info: Whether to yield turn info dicts
        on_event: SSE event callback
        initial_messages: Pre-existing message history
        context_window: Character budget for context management
        on_tool_batch: Called with the turn's whole tool_calls list before any
            of them run. Lets a tool family (node_shell_exec) resolve one
            all-or-nothing approval for the set instead of prompting per call.
    """
    del verbose

    # Scopes the tool_call ids minted below to this run. ``turn`` restarts at 0
    # on every call, and goal mode calls this loop repeatedly against one
    # accumulating history, so ``turn``/``index`` alone would mint the same id
    # twice in a conversation — and results are paired back by id, so the second
    # would overwrite the first.
    run_tag = uuid.uuid4().hex[:8]

    if initial_messages:
        messages = list(initial_messages)
    else:
        messages = [{"role": "system", "content": system_prompt}]
        messages.append({"role": "user", "content": initial_user_content or user_input})
    # Seed the turn-history boundary so _fold_earlier can group agent turns under the
    # request that produced them. Without a [USER] marker the whole run would fold
    # into one undifferentiated blob and the model could not tell which summaries
    # belonged to which request.
    if not handler.history_info or not handler.history_info[-1].startswith("[USER]"):
        handler.history_info.append(f"[USER] {(user_input or '')[:200]}")
    last_data = None
    # done 事件只带「最后一轮」的 usage，不跨轮累加。
    # prompt_tokens 是本轮重发的整个上下文大小（存量），不是增量流量；累加它会让
    # 徽章在 N 轮工具调用后显示约 N 倍真实占用。计费仍由网关按每次请求独立结算，
    # 不依赖这里的累计值。
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "cached_tokens": 0, "cache_creation_tokens": 0, "reasoning_tokens": 0}

    def _record_usage(u: dict | None) -> None:
        if not isinstance(u, dict):
            return
        try:
            from usage_utils import normalize_usage
            norm = normalize_usage(u)
        except Exception:
            norm = u
        # 整体替换而不是逐键覆盖：只补本轮出现的键会把上一轮的 cached/reasoning
        # 留在结果里，拼出一个哪一轮都不是的混合值。
        for key in total_usage:
            try:
                total_usage[key] = int(norm.get(key) or 0)
            except (TypeError, ValueError):
                total_usage[key] = 0

    for turn in range(1, max_turns + 1):
        handler.current_turn = turn

        if yield_info:
            yield {"turn": turn}
        if on_event:
            await on_event({"type": "turn_start", "turn": turn})

        # Hook: turn_before
        if hooks.has_hooks("turn_before"):
            await hooks.trigger("turn_before", {
                "turn": turn, "messages": messages, "handler": handler,
            })

        # Context management: compress and trim before LLM call
        compress_history(messages, keep_recent=10, interval=5)
        trim_messages(messages, context_window=context_window)

        # Hook: llm_before
        if hooks.has_hooks("llm_before"):
            await hooks.trigger("llm_before", {
                "turn": turn, "messages": messages,
            })

        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_calls: list[dict] = []
        usage: dict | None = None
        finish_reason: str | None = None
        streamed = False
        did_fallback = False

        def _absorb_non_stream(resp) -> None:
            """把非流式整段响应并入本轮状态，正文按统一格式发一次 content 事件。"""
            nonlocal usage, finish_reason
            delta = getattr(resp, "content", "") or ""
            if delta:
                content_parts.append(delta)
            rdelta = getattr(resp, "reasoning", "") or ""
            if rdelta:
                reasoning_parts.append(rdelta)
            tc = getattr(resp, "tool_calls", None)
            if tc:
                tool_calls[:] = tc
            u = getattr(resp, "usage", None)
            if u:
                usage = u
            fr = getattr(resp, "finish_reason", None)
            if not fr:
                raw = getattr(resp, "raw", None)
                if isinstance(raw, dict):
                    choices = raw.get("choices") or []
                    fr = (choices[0] if choices else {}).get("finish_reason")
            finish_reason = fr

        async def _emit_non_stream_parts() -> None:
            """非流式 fallback 的事件出口：思考先于正文，与流式顺序一致。"""
            if not on_event:
                return
            if reasoning_parts:
                await on_event({"type": "reasoning", "text": reasoning_parts[-1]})
            if content_parts:
                await on_event({"type": "content", "text": content_parts[-1]})

        # 先尝试流式；上游不支持 stream（报错或无 chunk）则 fallback 非流式，
        # 但事件格式保持统一：正文只走 content，done 只带 usage。
        #
        # 分层重试（与主系统 retry 体系完全独立）：
        # - 网络异常（连接/超时/断流）已在 LLMBridge 内重试并发 retry 事件；
        #   此处再收到网络异常即代表 bridge 已耗尽，直接抛。
        # - 其他异常（HTTP 非 200、JSON 解析失败等）在此层重试：仅当本轮尚未
        #   吐过任何 content/tool_call 时重试，否则直接抛（避免重复回答）。
        client_max_retries = int(getattr(client, "max_retries", 0) or 0)
        loop_max_retries = max(0, client_max_retries)

        async def _emit_loop_retry(attempt: int, reason: str, delay_ms: int) -> None:
            if not on_event:
                return
            await on_event({
                "type": "retry",
                "attempt": attempt,
                "max": loop_max_retries,
                "reason": reason,
                "scope": "stream",
                "delay_ms": delay_ms,
            })

        def _supports_on_retry(method_name: str) -> bool:
            """真 LLMBridge 的 stream_chat/chat 接受 on_retry；测试用假 client 不接受。
            用签名探测，避免改动测试桩就能透传网络层重试事件。"""
            method = getattr(client, method_name, None)
            if method is None:
                return False
            try:
                return "on_retry" in inspect.signature(method).parameters
            except (ValueError, TypeError):
                return False

        stream_kwargs: dict[str, Any] = {}
        if _supports_on_retry("stream_chat"):

            async def _bridge_on_retry(info: dict) -> None:
                if on_event:
                    await on_event({"type": "retry", "scope": "network", **info})

            stream_kwargs["on_retry"] = _bridge_on_retry
        chat_kwargs: dict[str, Any] = {}
        if _supports_on_retry("chat"):
            chat_kwargs["on_retry"] = stream_kwargs.get("on_retry")

        for attempt in range(loop_max_retries + 1):
            content_parts.clear()
            reasoning_parts.clear()
            tool_calls[:] = []
            usage = None
            finish_reason = None
            streamed = False
            did_fallback = False

            async def _retry_or_raise(err: Exception) -> bool:
                """返回 True 表示已安排下一次重试；False 不会返回（直接 raise）。"""
                if content_parts or tool_calls:
                    raise err
                if isinstance(err, _NETWORK_EXC):
                    # 网络异常由 LLMBridge 负责重试；到这里说明已耗尽，本层不再 fallback/retry。
                    raise err
                # 非网络异常：只对瞬时 429（可重试业务码）做 agent 层退避重试。
                # 配置类 429（no_available_account：白名单/禁用）与其余 4xx/5xx/
                # JSON 解析错误不重试——重试无意义，直接抛，交上层落 error + 前端手动重试。
                if not _is_retryable_429(err):
                    raise err
                if attempt >= loop_max_retries:
                    raise err
                # 429 常带 Retry-After：优先用它，否则指数退避。
                delay_ms = _http_error_retry_after_ms(err) or _backoff_delay_ms(attempt)
                await _emit_loop_retry(
                    attempt + 1,
                    f"{type(err).__name__}: {err}".strip(),
                    delay_ms,
                )
                logger.debug(f"[agent_loop] retry {attempt + 1}/{loop_max_retries} after 429: {err}")
                await asyncio.sleep(delay_ms / 1000)
                return True

            try:
                async for chunk in client.stream_chat(messages=messages, tools=tools_schema, **stream_kwargs):
                    streamed = True
                    if getattr(chunk, "content", ""):
                        delta = chunk.content or ""
                        content_parts.append(delta)
                        if on_event:
                            await on_event({"type": "content", "text": delta})
                    # 思考增量与正文分开推送：前端单独渲染折叠的思考块，思考不并入
                    # content_parts（不回灌给上游）。
                    if getattr(chunk, "reasoning", ""):
                        rdelta = chunk.reasoning or ""
                        reasoning_parts.append(rdelta)
                        if on_event:
                            await on_event({"type": "reasoning", "text": rdelta})
                    if getattr(chunk, "tool_calls", None):
                        tool_calls = chunk.tool_calls or tool_calls
                    if getattr(chunk, "usage", None):
                        usage = chunk.usage
                    if getattr(chunk, "finish_reason", None):
                        finish_reason = chunk.finish_reason
            except Exception as stream_err:
                # 已吐过部分内容才失败 → 不重试，直接抛给上层（避免重复回答）。
                # 网络异常也直接抛：它只归 LLMBridge 层处理。
                if content_parts or tool_calls or isinstance(stream_err, _NETWORK_EXC):
                    raise
                # 瞬时 429：跳过非流式 fallback（429 再打一次也是 429，省一次 dispatch），
                # 直接走 agent 层退避重试。_retry_or_raise 要么 raise 要么返回 True。
                if _is_retryable_429(stream_err):
                    await _retry_or_raise(stream_err)
                    continue
                # 还没产出任何内容 → 先退回非流式整段拉取（兼容不支持 stream 的上游）。
                try:
                    resp = await client.chat(messages=messages, tools=tools_schema, **chat_kwargs)
                    _absorb_non_stream(resp)
                    did_fallback = True
                    await _emit_non_stream_parts()
                    logger.debug(f"[agent_loop] stream fallback to non-stream: {stream_err}")
                    break
                except Exception as fallback_err:
                    if await _retry_or_raise(fallback_err):
                        continue
            else:
                # 流式没报错但一个 chunk 都没解析出（上游把响应当整段 JSON 返回，非 SSE）→ fallback 非流式。
                if not did_fallback and not content_parts and not tool_calls:
                    try:
                        resp = await client.chat(messages=messages, tools=tools_schema, **chat_kwargs)
                        _absorb_non_stream(resp)
                        await _emit_non_stream_parts()
                        logger.debug("[agent_loop] stream produced no output, fell back to non-stream")
                    except Exception as fallback_err:
                        if await _retry_or_raise(fallback_err):
                            continue
                break  # 本轮成功，跳出重试循环

        content = "".join(content_parts)
        _record_usage(usage)

        class _StreamResponse:
            __slots__ = ("content", "tool_calls", "usage", "finish_reason")
            def __init__(self):
                self.content = content
                self.tool_calls = tool_calls
                self.usage = usage
                self.finish_reason = finish_reason
        response = _StreamResponse()

        # Hook: llm_after
        if hooks.has_hooks("llm_after"):
            await hooks.trigger("llm_after", {
                "turn": turn, "response": response, "content": content,
            })

        # --- no_tool handling ---
        if not tool_calls:
            # 既没有工具调用又没有正文：上游打嗝。以前这里直接落到下面的
            # CURRENT_TASK_DONE，把一次空响应当成任务完成上报。改成三振：
            # 前两次要求模型重新作答，第三次才认定失败并明确报错。
            if not content.strip():
                handler._consecutive_empty += 1
                if handler._consecutive_empty >= _MAX_CONSECUTIVE_EMPTY:
                    err = (
                        f"[ERROR] Model returned an empty response "
                        f"{handler._consecutive_empty} times in a row; aborting instead of "
                        "reporting the task as complete."
                    )
                    if on_event:
                        await on_event({"type": "error", "error": err})
                    yield {"result": "ERROR", "data": err}
                    return
                messages.append({"role": "assistant", "content": content or ""})
                messages.append({
                    "role": "user",
                    "content": (
                        "[SYSTEM] Your last reply was empty. Respond again: either call a "
                        "tool or give the final answer in text."
                    ),
                })
                continue  # next turn

            # max_tokens truncation → inject "continue" prompt
            if _detect_max_tokens_truncation(response):
                # 截断续跑也要有上限，否则一个总是撞 max_tokens 的模型会一直
                # continue 到 max_turns 才停，中间全是半截输出。
                handler._consecutive_truncated += 1
                if handler._consecutive_truncated >= _MAX_CONSECUTIVE_TRUNCATED:
                    err = (
                        f"[ERROR] Output hit the token limit "
                        f"{handler._consecutive_truncated} turns in a row; aborting. "
                        "Break the work into smaller steps."
                    )
                    if on_event:
                        await on_event({"type": "error", "error": err})
                    yield {"result": "ERROR", "data": err}
                    return
                messages.append({"role": "assistant", "content": content})
                messages.append({
                    "role": "user",
                    "content": "[SYSTEM] Output was truncated. Continue from where you left off. Use smaller steps.",
                })
                if on_event:
                    await on_event({"type": "content", "text": "[SYSTEM] Truncation detected, continuing..."})
                continue  # next turn
            handler._consecutive_truncated = 0

            # Large code block detection → ask to use tools
            if _detect_large_code_block(content):
                messages.append({"role": "assistant", "content": content})
                messages.append({
                    "role": "user",
                    "content": "[SYSTEM] Code block detected. Please use code_run to execute it or file_write to save it.",
                })
                handler._consecutive_empty = 0
                continue  # next turn

            # Normal text response → exit
            handler._consecutive_empty = 0
            # turn_end_callback for final turn
            summary, _ = _extract_summary(content, [])
            handler.history_info.append(f"[Agent] {summary}")

            if on_event:
                await on_event({"type": "done", "usage": dict(total_usage), "model": getattr(client, "model", "") or ""})
            yield {"result": "CURRENT_TASK_DONE", "data": content}
            return

        # --- Tool dispatch ---
        handler._consecutive_empty = 0
        tool_results = []
        should_exit = False
        next_prompt = None

        # Give the batch coordinator the whole set first: a turn that asks for
        # several node commands gets one all-or-nothing approval, and the tools
        # below then execute in order against that single decision.
        if on_tool_batch is not None:
            await on_tool_batch(tool_calls)

        for index, tool_call in enumerate(tool_calls):
            function = tool_call.get("function", {})
            tool_name = function.get("name", "")
            raw_args = function.get("arguments") or "{}"
            # 上游把参数拼坏（截断、多余逗号、非 JSON）时不能让异常穿出生成器把整个
            # 任务打死——那是模型可以自己改正的错误。转成一条工具错误结果喂回去。
            bad_args_error: str | None = None
            if isinstance(raw_args, dict):
                args = raw_args
            else:
                try:
                    args = json.loads(raw_args)
                except (json.JSONDecodeError, TypeError, ValueError) as parse_err:
                    args = {}
                    bad_args_error = (
                        f"[TOOL ARGS PARSE ERROR] {tool_name}: arguments are not valid JSON "
                        f"({type(parse_err).__name__}: {parse_err}). "
                        f"Raw arguments (truncated): {str(raw_args)[:500]}\n"
                        "Re-issue the tool call with well-formed JSON arguments."
                    )
            if not isinstance(args, dict):
                bad_args_error = (
                    f"[TOOL ARGS PARSE ERROR] {tool_name}: arguments must be a JSON object, "
                    f"got {type(args).__name__}. Re-issue the call with an object."
                )
                args = {}

            # The agent owns this id, it is never the upstream's. A conversation
            # here is routed per-turn across channels of different protocol
            # families, so trusting the upstream id leaves history with a mix of
            # namespaces (an OpenAI-style ``call_...`` next to a Bedrock
            # ``toolu_bdrk_...``) that later turns replay to whichever upstream
            # they land on. Minting our own keeps stored history
            # protocol-neutral, and the pair below stays correlated because both
            # the assistant tool_call and its role=tool reply use this value.
            # Upstream ids are still honoured while assembling the streamed call
            # (see _merge_stream_tool_calls); they just stop at this boundary.
            tool_call_id = f"call_agent_{run_tag}_{turn}_{index}"
            tool_call["id"] = tool_call_id
            tool_call.setdefault("type", "function")

            if on_event:
                await on_event({
                    "type": "tool_call", "id": tool_call_id, "name": tool_name, "args": args, "index": index,
                })

            if bad_args_error:
                outcome = StepOutcome(data={"error": bad_args_error}, next_prompt=bad_args_error)
            else:
                outcome = await handler.dispatch(
                    tool_name, args, response, index=index, tool_num=len(tool_calls)
                )
            last_data = outcome.data

            # Keep results structured for events/history, but send each result as
            # a standard role=tool message below instead of misclassifying it as
            # user-authored content.
            result_with_anchor = {
                "tool_name": tool_name,
                "tool_call_id": tool_call_id,
                "args": args,
                "data": outcome.data,
            }
            tool_results.append(result_with_anchor)

            if on_event:
                await on_event({
                    "type": "tool_result", "id": tool_call_id, "name": tool_name, "data": outcome.data, "index": index,
                })

            if outcome.should_exit:
                should_exit = True
                break
            if outcome.next_prompt:
                next_prompt = outcome.next_prompt

        # 走到这里说明本轮 LLM 返回了 tool_calls 且工具已执行（无 tool_calls 的情况
        # 已在上方 no_tool 分支处理并 return）。若工具未显式给出 next_prompt，
        # 用默认续跑提示把结果喂回 LLM，避免第一次工具调用后就误判任务完成而提前结束。
        if not should_exit and not next_prompt:
            next_prompt = _DEFAULT_CONTINUE_PROMPT

        # --- turn_end_callback ---
        summary, had_summary_tag = _extract_summary(content, tool_calls)
        handler.history_info.append(f"[Agent] {summary}")

        # The anchor's turn history is only as good as these summaries, so a turn that
        # called a tool without tagging one gets told to tag the next one. Reaching here
        # always means tools ran — the no-tool path returned above.
        if next_prompt and not had_summary_tag:
            next_prompt += (
                "\n\n[TIPS] Your reply must contain <summary>one line: what you did "
                "and what it showed</summary>. It becomes this task's turn history."
            )

        # Turn-cadence escalation. Long runs fail by repeating a broken approach, so the
        # nudge names the concrete recovery moves — re-reading the governing SOP first,
        # since remembered SOP content is exactly what drifts.
        if next_prompt and turn > 0 and turn % 13 == 0:
            next_prompt += (
                f"\n\n[DANGER] Turn {turn}. Stop ineffective retries. Call "
                "update_working_checkpoint to save key context, then change strategy: "
                "1) probe the actual state instead of assuming it "
                "2) **re-read the relevant SOP with file_read** (memory/sop/...) — do "
                "not rely on remembered SOP content."
            )
        elif next_prompt and turn > 0 and turn % 10 == 0 and handler.key_info:
            next_prompt += f"\n\n[SYSTEM] Working memory refresh:\n{handler.key_info}"

        # Past this point autonomous recovery has failed; hand the decision back.
        if next_prompt and turn > 0 and turn % 45 == 0:
            next_prompt += (
                f"\n\n[DANGER] Turn {turn}. Call ask_user to summarize progress and get "
                "direction. No more blind retries."
            )

        # Force exit at 75 turns
        if turn >= 75:
            if on_event:
                await on_event({"type": "done", "usage": dict(total_usage), "model": getattr(client, "model", "") or ""})
            yield {"result": "MAX_TURNS_EXCEEDED", "data": "75 turn limit reached"}
            return

        if on_event:
            await on_event({"type": "turn_end", "turn": turn})

        # Hook: turn_after
        if hooks.has_hooks("turn_after"):
            await hooks.trigger("turn_after", {
                "turn": turn,
                "tool_calls": tool_calls,
                "tool_results": tool_results,
                "next_prompt": next_prompt,
                "handler": handler,
            })

        # --- Exit/continue logic ---
        if should_exit:
            # Distinguish ask_user question from a hard exit so the client can
            # render an input dialog instead of treating it as task completion.
            is_question = (
                isinstance(last_data, dict)
                and last_data.get("status") == "question"
            )
            if on_event:
                if is_question:
                    await on_event({"type": "question", "data": last_data})
                await on_event({"type": "done", "usage": dict(total_usage), "model": getattr(client, "model", "") or ""})
            yield {"result": "EXITED", "data": last_data}
            return

        # Append anchor to next_prompt
        anchor = handler.build_anchor_prompt()
        if anchor:
            next_prompt += f"\n\n<anchor>\n{anchor}\n</anchor>"

        # Build assistant message. Only advertise the tool_calls we actually
        # executed and can answer — a mid-batch should_exit must not leave a
        # declared tool_call without its matching role=tool reply, which some
        # providers reject.
        executed_ids = {r["tool_call_id"] for r in tool_results}
        executed_tool_calls = [tc for tc in tool_calls if tc.get("id") in executed_ids]
        messages.append({
            "role": "assistant",
            "content": content,
            "tool_calls": executed_tool_calls,
        })
        # One standard role=tool message per executed call, in dispatch order.
        # Tool data stays attributed to the tool role instead of being folded
        # into a user message (which upstreams may treat as user-authored bulk
        # content and moderate).
        for result in tool_results:
            messages.append({
                "role": "tool",
                "tool_call_id": result["tool_call_id"],
                "name": result["tool_name"],
                "content": serialize_tool_result(result["data"]),
            })
        # Continuation prompt is a separate user message carrying no tool data.
        messages.append({
            "role": "user",
            "content": next_prompt,
        })

    if on_event:
        await on_event({"type": "done", "usage": dict(total_usage), "model": getattr(client, "model", "") or ""})
    yield {"result": "MAX_TURNS_EXCEEDED", "data": last_data}
