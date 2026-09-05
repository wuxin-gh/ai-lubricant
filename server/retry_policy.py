"""请求主链路的失败分类（纯函数，零 IO）。

设计目标：把原来散落在 main.py 各处的错误判定收敛到一个地方，且**不依赖**
config store / DB / Redis —— 所有配置（可重试规则、rate-limit 状态码等）由调用方
以参数注入。这样：

- 单元测试无需初始化 PostgreSQL / Redis 即可覆盖全部分类分支；
- 主链路只在一个入口做「这次失败该怎么办」的决策，不再让协议层 / stream 层 /
  选择层各自维护一套 retryable 判断。

本模块只回答两类问题：
1. 上游返回的错误是否属于「客户端请求本身不可继续」（参数错 / 模型不支持等），
   这类必须原样透传，不能重试、不能包装成 429。
2. 给定一次失败发生的阶段与「是否已向客户端输出」，主编排器下一步该做什么
   （重试 / 直接返回客户端错误 / 已输出后发 SSE 错误并结束 / 取消 / 最终 429）。

字段口径与旧实现保持一致，便于灰度对拍。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ── 错误明细解析 ────────────────────────────────────────────────

def parse_error_detail(detail: Any) -> tuple[str, str, str, str]:
    """把上游/内部错误 detail 解析成 (err_type, code, param, message)，全部小写。

    与旧 main._parse_error_detail 行为一致：dict 直接用；str 尝试 JSON，失败则整体
    当 message；其它类型 str 化当 message。
    """
    if isinstance(detail, dict):
        data = detail
    elif isinstance(detail, str):
        try:
            data = json.loads(detail)
        except (json.JSONDecodeError, ValueError):
            return "", "", "", detail.lower()
    else:
        return "", "", "", str(detail).lower()

    error = data.get("error") if isinstance(data, dict) else None
    if isinstance(error, dict):
        err_type = str(error.get("type") or "").lower()
        code = str(error.get("code") or "").lower()
        param = str(error.get("param") or "").lower()
        message = str(error.get("message") or data).lower()
        return err_type, code, param, message
    return (
        str(data.get("type") or "").lower(),
        str(data.get("code") or "").lower(),
        str(data.get("param") or "").lower(),
        str(data).lower(),
    )


def as_lower_str_list(value: Any) -> list[str]:
    """把配置里的字符串/列表规整成去空的小写列表。"""
    if isinstance(value, (str, int)):
        value = [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip().lower() for item in value if str(item).strip()]


def as_int_set(value: Any) -> set[int]:
    """把配置里的状态码规整成 int 集合，忽略非法项。"""
    if isinstance(value, (str, int)):
        value = [value]
    if not isinstance(value, list):
        return set()
    codes: set[int] = set()
    for item in value:
        try:
            codes.add(int(item))
        except (TypeError, ValueError):
            continue
    return codes


# 参数类错误的通用标志词（与旧 main._is_protocol_parameter_error 一致）。
_PARAMETER_MARKERS: tuple[str, ...] = (
    "invalid_request_error",
    "invalid_request",
    "invalid parameter",
    "invalid_param",
    "unsupported parameter",
    "unsupported_param",
    "unsupported value",
    "invalid value",
    "invalid messages",
    "invalid message",
    "invalid tools",
    "invalid tool",
    "invalid schema",
    "json schema",
    "context_too_large",
    "context_length_exceeded",
    "context window",
    "context length",
    "maximum context",
    "too many tokens",
    "max tokens",
    "token limit",
    "request too large",
    "payload too large",
)


def is_protocol_parameter_error(err_type: str, code: str, param: str, message: str) -> bool:
    """是否是「协议/参数」类错误：有 param 字段，或命中参数标志词。"""
    haystack = f"{err_type} {code} {param} {message}"
    return bool(param) or any(marker in haystack for marker in _PARAMETER_MARKERS)


def is_non_retryable_upstream_error(status_code: int, detail: Any, rules: dict | None) -> bool:
    """上游错误是否「客户端请求本身不可继续」→ 不重试、原样透传。

    ``rules`` 即 config.Config.get_non_retryable_parameter_errors() 的返回值，由调用方
    读取后注入（本模块不碰 config）。判定顺序与旧实现一致：先确认是参数类错误，再按
    status_codes / types / codes / params / markers 命中任一即判为不可重试。
    """
    err_type, code, param, message = parse_error_detail(detail)
    if not is_protocol_parameter_error(err_type, code, param, message):
        return False
    rules = rules or {}
    if status_code in as_int_set(rules.get("status_codes")):
        return True
    if err_type and err_type in as_lower_str_list(rules.get("types")):
        return True
    if code and code in as_lower_str_list(rules.get("codes")):
        return True
    if param and param in as_lower_str_list(rules.get("params")):
        return True
    haystack = f"{err_type} {code} {param} {message}"
    return any(marker in haystack for marker in as_lower_str_list(rules.get("markers")))


# ── 上游错误语义归一 ─────────────────────────────────────────────
# 上游把超限错误以各种形态返回（HTTP 413、200+error 体、SSE error 帧、各种 code/文案），
# 客户端靠 error.code / HTTP 状态码判断错误类别。这里把**可识别**的上游超限错误归一到
# 统一的 (status=400, type=invalid_request_error, code, message)；message 是按 code 查表
# 得到的**固定文案**，不来自上游、不加前缀——上游原文只进日志。
#
# 表驱动：要支持新的超限类别，只需在 _ERROR_CANONICAL_RULES 加一条，无需改任何调用点。
# 非超限错误一律返回 None，交回原透传/配置规则逻辑，绝不越权改写。

@dataclass(frozen=True, slots=True)
class CanonicalError:
    """一条上游超限错误归一后的统一出口字段。

    message 由 code 查表得到，固定、跟上游解耦；不再拼上游原文、不再加英文锚点。
    """

    category: str
    type: str          # 统一 type，恒为 invalid_request_error
    code: str          # 统一小写 code
    status_code: int    # 统一 400
    message: str       # 固定文案


@dataclass(frozen=True, slots=True)
class _CanonicalRule:
    category: str
    # 识别：上游状态码命中任一即归类（仅兜底规则用，如纯 413→context_length_exceeded）
    trigger_status_codes: tuple[int, ...]
    # 识别：上游 error.code 精确命中（小写）任一即归类
    codes: tuple[str, ...]
    # 识别：type/code/param/message 拼成的 haystack 命中任一子串（小写）即归类
    markers: tuple[str, ...]
    # 统一出口
    type: str
    code: str
    status_code: int
    message: str

    def result(self) -> CanonicalError:
        return CanonicalError(
            category=self.category,
            type=self.type,
            code=self.code,
            status_code=self.status_code,
            message=self.message,
        )


# 归一规则表：三趟匹配（先 code、再 markers、最后 trigger_status_codes），先命中先返回。
# 顺序上把更具体的类别放前面；context_length_exceeded 放最后并带 trigger_status_codes=(413,)
# 作为「纯 413、别的都没命中」的兜底——对应"只要状态码 413 就按超长窗口返回"。
#
# 这批特征是**硬编码**的（不走管理端可配置的 non_retryable_parameter_errors），
# 是否启用由 config 的 context_overflow_not_retryable_enabled() 开关控制（见 main 出口层）。
_ERROR_CANONICAL_RULES: tuple[_CanonicalRule, ...] = (
    _CanonicalRule(
        category="thinking_budget_invalid",
        trigger_status_codes=(),
        # 上游把这类参数冲突错标成 server_error（见下方 marker 对应的真实响应），
        # 按 code 判会误伤真正的 5xx，因此这条规则**只按 message 文案**判定。
        codes=(),
        markers=(
            "budget_tokens must be less than max_tokens",
            "budget_tokens must be smaller than max_tokens",
        ),
        type="invalid_request_error",
        code="invalid_request_error",
        status_code=400,
        message="thinking.budget_tokens must be less than max_tokens",
    ),
    _CanonicalRule(
        category="max_tokens_exceeded",
        trigger_status_codes=(),
        codes=("max_tokens_exceeded",),
        markers=(
            "max_tokens exceeds",
            "max_tokens too large",
            "requested max_tokens exceeds",
            "max_tokens exceeds remaining context",
        ),
        type="invalid_request_error",
        code="max_tokens_exceeded",
        status_code=400,
        message="max_tokens exceeds remaining context window",
    ),
    _CanonicalRule(
        category="prompt_too_long",
        trigger_status_codes=(),
        codes=("prompt_too_long",),
        markers=(
            "prompt is too long",
            "prompt too long",
        ),
        type="invalid_request_error",
        code="prompt_too_long",
        status_code=400,
        message="Prompt is too long",
    ),
    _CanonicalRule(
        category="input_too_long",
        trigger_status_codes=(),
        codes=("input_token_limit_exceeded", "input_too_long"),
        markers=(
            "input is too long",
            "input tokens exceed",
            # 中文超限文案：上游中继常以 200+error 体或 SSE error 帧承载（状态码被回退成 502、code 丢失），靠 message 里的中文标记兜底归类。
            "输入太长",
        ),
        type="invalid_request_error",
        code="input_too_long",
        status_code=400,
        message="Input tokens exceed the maximum allowed limit",
    ),
    _CanonicalRule(
        category="model_context_limit",
        trigger_status_codes=(),
        codes=("model_context_limit",),
        markers=(
            "model context window",
            "model's context window",
            "model context limit",
        ),
        type="invalid_request_error",
        code="model_context_limit",
        status_code=400,
        message="Model context window exceeded",
    ),
    _CanonicalRule(
        category="context_length_exceeded",
        # 兜底：纯 413（没有更具体的 code/文案）落这里
        trigger_status_codes=(413,),
        codes=(
            "context_too_large",
            "context_length_exceeded",
            "request_too_large",
            "payload_too_large",
        ),
        markers=(
            "context_too_large",
            "context_length_exceeded",
            "context window",
            "context length",
            "maximum context length",
            "too many tokens",
            "token limit",
            "reduce the length",
            "request too large",
            "payload too large",
        ),
        type="invalid_request_error",
        code="context_length_exceeded",
        status_code=400,
        message="The context length exceeds the model's maximum limit",
    ),
)


def _is_structured_error_detail(detail: Any) -> bool:
    """detail 是否为结构化错误体（dict 或可解析为 JSON 对象），而非裸 HTML/纯文本。

    用于区分模型上游的结构化 413（带 error.code/type/message 的 JSON）与网关层
    nginx/cloudflare 的 HTML 413 页面：后者不应被当成模型上下文超限。
    """
    if isinstance(detail, dict):
        return True
    if isinstance(detail, str):
        s = detail.lstrip()
        if not s or s[0] != "{":
            return False
        try:
            data = json.loads(detail)
        except (json.JSONDecodeError, ValueError):
            return False
        return isinstance(data, dict)
    return False


def canonicalize_upstream_error(status_code: int | None, detail: Any) -> CanonicalError | None:
    """把**可识别**的上游超限错误归一为统一出口字段；识别不出时返回 ``None``。

    纯函数、零 IO。三趟匹配（先 code、再 markers、最后 trigger_status_codes）：
      1. 上游 error.code 精确命中任一规则的 codes → 归该类；
      2. type/code/param/message 拼成的文本命中任一规则 markers → 归该类；
      3. 上游状态码命中任一规则 trigger_status_codes（如纯 413）→ 归该类（兜底）。
    顺序保证"更具体的类别"优先于"通用兜底"：一个 413 带 prompt_too_long code 会先被
    prompt_too_long 命中，而不是落到 context_length_exceeded 的 413 兜底。

    第三趟要求 detail 是结构化错误体（dict / JSON 对象）：网关层 nginx/cloudflare 的
    HTML 413（请求体超过 client_max_body_size，与模型上下文无关）不再被归一为超限，
    落回普通可重试分类，交由 classify_failure 切候选。

    非超限错误（鉴权/限流/普通参数错/5xx）一律返回 None，交回原透传/配置规则逻辑。
    """
    err_type, code, param, message = parse_error_detail(detail)
    haystack = f"{err_type} {code} {param} {message}"
    if code:
        for rule in _ERROR_CANONICAL_RULES:
            if code in rule.codes:
                return rule.result()
    for rule in _ERROR_CANONICAL_RULES:
        if any(marker in haystack for marker in rule.markers):
            return rule.result()
    if status_code is not None and _is_structured_error_detail(detail):
        for rule in _ERROR_CANONICAL_RULES:
            if status_code in rule.trigger_status_codes:
                return rule.result()
    return None


# ── 失败决策 ────────────────────────────────────────────────────

class FailureAction(str, Enum):
    """一次 attempt 失败后主编排器的下一步动作。"""

    RETRY = "retry"                          # 释放本次候选预占，继续选下一个候选
    RETURN_CLIENT_ERROR = "return_client_error"  # 原样透传 4xx，不重试
    STREAM_ERROR_AND_STOP = "stream_error_and_stop"  # 已向客户端输出，发 SSE 错误并结束
    CANCEL_AND_STOP = "cancel_and_stop"      # 客户端取消，释放资源直接退出
    NO_RESOURCE_429 = "no_resource_429"      # 无可用资源，最终 429


@dataclass(slots=True)
class FailureDecision:
    action: FailureAction
    # 冻结/排除范围交给现有 limits 逻辑执行，这里只表达意图
    freeze_scope: str | None = None
    exclusion_scope: str | None = None  # None / "candidate" / "account" / "provider"
    rollback_reservation: bool = True
    # 需要透传给客户端时的错误载荷
    client_status: int | None = None
    client_detail: Any = None
    # 诊断
    reason: str = ""
    details: dict = field(default_factory=dict)


@dataclass(slots=True)
class FailureInput:
    """classify_failure 的输入快照，全部由调用方填好，避免函数内部再查 IO。"""

    is_http_exception: bool
    status_code: int | None
    detail: Any
    # 是否已经向客户端输出过任何可见内容（stream 首个有效 SSE / 非流式已返回）
    downstream_started: bool
    stream: bool
    cancelled: bool
    # 是否还有候选可继续尝试（选择次数/候选是否耗尽由调用方判断后传入）
    candidates_remaining: bool
    # 上游是否已真正发起请求（区分「发送前构造失败」与「上游返回失败」）
    upstream_started: bool
    # 调用方已完成兼容/配置判定时可显式覆盖；None 表示按 rules 自行判定。
    non_retryable_override: bool | None = None
    non_retryable_rules: dict | None = None
    # 是否明确属于网络/传输/超时类瞬态异常；普通业务/编程异常不重试。
    transient_exception: bool = False
    # 上游流在向客户端输出前被判定为不完整（空流、零 completion、缺终止帧等）。
    # 这属于候选渠道响应质量问题，应切换候选；已输出时仍由 downstream_started 优先阻止重试。
    retryable_incomplete_response: bool = False
    extra_retry_status_codes: set[int] = field(default_factory=set)


def classify_failure(inp: FailureInput) -> FailureDecision:
    """统一失败分类。语义对齐已确认的规则：

    1. 客户端取消 → 直接停止，不重试。
    2. 已向客户端输出（stream 输出后）→ 发 SSE 错误并结束，不再重试/切备选。
    3. 客户端请求本身不可继续（参数错/模型不支持）→ 原样透传 4xx。
    4. 其它失败 → 能继续选候选就重试；否则最终 429。
    上游 5xx / 429 / 网络错误等中间失败不直接暴露给客户端，统一走重试或最终 429。
    """
    # 1) 取消优先，避免被后续分支吞掉
    if inp.cancelled:
        return FailureDecision(
            action=FailureAction.CANCEL_AND_STOP,
            rollback_reservation=True,
            reason="client_cancelled",
        )

    # 2) 已经输出给客户端：无论上游什么错，都只能收尾
    if inp.downstream_started:
        return FailureDecision(
            action=FailureAction.STREAM_ERROR_AND_STOP,
            rollback_reservation=False,  # 已经消费，不回滚计量
            reason="downstream_already_started",
        )

    # 3) 只有明确判定为客户端请求本身不可继续（参数错/模型不支持/上下文超限归一）时才透传。
    if inp.is_http_exception and inp.status_code is not None:
        non_retryable = inp.non_retryable_override
        if non_retryable is None:
            non_retryable = is_non_retryable_upstream_error(
                inp.status_code, inp.detail, inp.non_retryable_rules
            )
        if non_retryable:
            return FailureDecision(
                action=FailureAction.RETURN_CLIENT_ERROR,
                rollback_reservation=True,
                client_status=inp.status_code,
                client_detail=inp.detail,
                reason="non_retryable_client_error",
            )

    # 4) 外层换候选资格：
    #    - 走到这里的 HTTP 错误已排除「客户端请求本身不可继续」，因此都视为当前候选失败。
    #      渠道 ``extra_retry_status_codes`` 只控制同账号内层原地重试，不得阻断外层换账号/
    #      换渠道；否则未配置的 401/403/404 会在第一次失败时直接穿透全部重试循环。
    #    - 非 HTTP 异常只有明确的瞬态故障（网络/传输/超时），或上游流在未向客户端输出前
    #      被判定为不完整（空流/零 completion），才重试；其余（如内部逻辑错误）不重试，
    #      避免把非瞬态 bug 放大成多次上游请求。
    #    不完整流是候选渠道的响应质量问题，换个账号/渠道通常就能拿到正常响应；
    #    切候选耗尽后统一 429。
    retryable = (
        inp.is_http_exception
        or inp.transient_exception
        or inp.retryable_incomplete_response
    )

    if retryable and inp.candidates_remaining:
        return FailureDecision(
            action=FailureAction.RETRY,
            rollback_reservation=True,
            exclusion_scope="candidate",
            freeze_scope=None,
            reason="retryable_failure",
            details={"is_rate_limit": False},
        )

    return FailureDecision(
        action=FailureAction.NO_RESOURCE_429,
        rollback_reservation=True,
        reason="candidates_exhausted",
    )


def classify_inner_retry(inp: FailureInput) -> bool:
    """本次失败是否**够资格在同一账号上原地重发**（渠道内层重试）。

    与 ``classify_failure`` 的分工：本函数只回答「能不能不换账号再发一次」，次数预算
    由调用方按渠道 ``retry_count`` 控制；``classify_failure`` 仍然独立决定内层耗尽后
    外层换候选/透传/收尾。两者互不覆盖——内层不够资格并不代表外层不能换候选。

    够资格（上游或链路的瞬时故障，换账号也是一样的错，先原地重试更省候选）：

    - 429：上游限流，同账号退避后常能过。
    - 5xx：上游服务端错误。
    - 命中渠道配置的 ``extra_retry_status_codes``（如 408 超时）。
    - 明确的网络/传输/超时瞬态异常。

    不够资格（原地重发只会放大同一个确定性失败）：

    - 已向客户端输出过内容：重发会损坏已发出的 SSE，必须收尾。
    - 客户端请求本身不可继续（参数错/模型不支持/上下文超限）：换几次都一样。
    - 非瞬态的内部异常：属于本进程 bug，不放大成多次上游请求。
    - 未配置为额外重试码的普通 4xx（401/403/404 等）：同账号重发无意义，交外层换候选。
    - 不完整/空响应（``retryable_incomplete_response``）：这是**候选渠道的响应质量问题**，
      换个账号/渠道通常就能拿到正常响应，所以走外层换候选，不在同账号上原地重发。
    """
    # 已输出给客户端，或客户端已取消：一律不再原地重发。
    if inp.cancelled or inp.downstream_started:
        return False

    # 客户端请求本身不可继续：与 classify_failure 第 3) 步同源判定，避免两处口径漂移。
    if inp.is_http_exception and inp.status_code is not None:
        non_retryable = inp.non_retryable_override
        if non_retryable is None:
            non_retryable = is_non_retryable_upstream_error(
                inp.status_code, inp.detail, inp.non_retryable_rules
            )
        if non_retryable:
            return False

    # 不完整/空响应属于候选质量问题，交外层换候选，不在同账号原地重发。
    if inp.retryable_incomplete_response:
        return False

    # 网络/传输/超时瞬态故障。
    if inp.transient_exception:
        return True

    if inp.is_http_exception and inp.status_code is not None:
        if inp.status_code == 429 or inp.status_code >= 500:
            return True
        return inp.status_code in (inp.extra_retry_status_codes or set())

    # 其余非 HTTP 异常（非瞬态）：视为本进程逻辑错误，不原地重发。
    return False
