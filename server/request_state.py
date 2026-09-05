"""请求主链路的状态对象（纯数据，零 IO）。

把原来散落在 ``dispatch_entry`` / ``_chat_with_retry`` / ``_chat_with_retry_for_model``
的一大堆 kwargs 与临时变量，收敛成三层明确的状态：

- :class:`RequestContext` —— 请求级，只在入口固定一次的东西（request_id、原始 body、
  协议、api key、白黑名单、deadline、预算、API Key 预占句柄、以及跨 attempt 累计的
  计数与 downstream 输出标记）。**不含**任何与「当前选中渠道/账号/模型」相关的字段。
- :class:`CandidateKey` —— ``(provider, account, model)`` 三元组的稳定标识，候选、
  排除、预占全部以它为单位，杜绝「有的地方只有 account、有的地方又临时补 model」。
- :class:`AttemptContext` —— 一次真实上游请求的状态：选中的候选、本次重建的 body、
  本次决定的 upstream_model / response_model、预占句柄、以及 started/first_byte/
  committed 等生命周期标记。

设计约束（见重构 plan）：
- ``RequestContext.original_body`` 不可变；每个 attempt 从它深拷贝重建，不在原地累积改。
- 模型默认参数、response_model、upstream_model 不在请求级固定，只在 attempt 级决定。
- 这些对象只承载状态，不承载行为（预占/选择/发送逻辑在各自模块）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class CandidateKey:
    """``(provider, account, model)`` 稳定标识。

    ``model`` 用 routed_model（对外/路由模型名），与现有排除逻辑口径一致：
    account_model 级排除用完整三元组，account 级排除只比对 (provider, account)。
    """

    provider: str
    account: str
    model: str

    @property
    def account_scope(self) -> tuple[str, str]:
        return (self.provider, self.account)


@dataclass(slots=True)
class RequestContext:
    """请求级状态：入口固定一次，跨所有 attempt 复用。"""

    request_id: str
    # 原始客户端请求体，深拷贝源，进入循环后不可变
    original_body: dict
    original_model: str
    messages: list[dict]

    request_protocol: str = "openai"
    chat_method: str = "chat"
    stream: bool = False

    api_key: str | None = None
    api_key_name: str | None = None

    # 渠道/模型白黑名单（入口显式提取，不在选择层回推）
    provider_whitelist: set[str] = field(default_factory=set)
    provider_blacklist: set[str] = field(default_factory=set)
    account_whitelist: set[str] | None = None

    client_type: str = "unknown"
    session_id: str | None = None
    request_headers: dict = field(default_factory=dict)
    client_request_path: str | None = None

    is_test: bool = False
    # 定时检测模式：与 is_test 同样绕过选路/预占（冻结键不再拦测试），但失败时走与正常请求
    # 完全一致的冻结/健康度收口。is_test 失败不冻结。
    # 「失败刷新冻结周期」不随请求传递，由失败对象所属渠道的 freeze_policy 在冻结写入点判定。
    is_probe: bool = False

    # 客户端可见的稳定模型名（模型组请求下固定），随 attempt 传递给协议改写
    stable_response_model: str | None = None
    group_request_identity: bool = False

    # API Key 请求级预占句柄（整个请求只占/释一次）
    api_key_reservation: Any | None = None

    # 是否已向客户端输出过任何可见内容（stream 首个有效 SSE / 非流式已返回）
    downstream_started: bool = False
    cancelled: bool = False

    # 透传给下游/协议层的附加 kwargs 基底（不含模型相关默认参数）
    base_kwargs: dict = field(default_factory=dict)

    # 最近一次 route_info（用于最终错误附带 / 统计收口）
    last_route_info: dict = field(default_factory=dict)


@dataclass(slots=True)
class AttemptContext:
    """一次真实上游请求的 attempt 级状态。"""

    attempt_no: int
    candidate_key: CandidateKey

    # 选中候选后重建的上游请求体与协议 kwargs（从 RequestContext.original_body 深拷贝）
    request_body: dict = field(default_factory=dict)
    attempt_kwargs: dict = field(default_factory=dict)

    # attempt 级才决定的模型身份
    public_model_id: str = ""      # 对外/路由模型名
    upstream_model_id: str = ""    # 发给渠道的上游模型名
    response_model: str = ""       # 回给客户端的 model 字段

    route_info: dict = field(default_factory=dict)
    reservation: Any | None = None  # 候选级预占句柄

    # 生命周期标记
    started_at: float = 0.0
    upstream_started: bool = False   # 已真正向上游发起请求
    downstream_started: bool = False  # 本 attempt 已向客户端输出内容
    first_byte_at: float | None = None
    logged_started: bool = False
    finalized: bool = False
