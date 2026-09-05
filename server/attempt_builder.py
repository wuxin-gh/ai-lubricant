"""每次 attempt 的请求构造（选中候选之后才决定的东西）。

对齐重构 plan 的两条硬约束：

1. **body 隔离**：每个 attempt 从 ``RequestContext.original_body`` 深拷贝重建，绝不在
   上一次失败 attempt 改过的 body 上继续叠加。杜绝「第一个渠道注入的默认参数/协议转换
   字段泄漏到第二个渠道」「model 名被重复替换」等原地累积污染。
2. **模型相关参数在 attempt 级决定**：模型元数据默认参数、upstream_model、response_model
   都在选中 ``(provider, account, model)`` 之后按「当前 routed_model」计算，不在请求入口固定。

四种模型名严格区分（见 plan §14.2），不再共用一个 ``model`` 变量：

- ``requested_model``    客户端请求名（RequestContext.original_model）
- ``routed_model``       本次选中的对外/路由模型名（route_info.routed_model）
- ``upstream_model``     发给渠道的上游模型名
- ``response_model``     回给客户端 model 字段（模型组请求用 stable_response_model）

本模块尽量做成纯/可注入：``resolve_model_identity`` / ``apply_model_defaults`` 不碰 IO，
默认参数由调用方从 model_metadata 读好后以 ``defaults`` 注入，便于离线单测。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass


@dataclass(slots=True)
class ModelIdentity:
    """一次 attempt 的四种模型名解析结果。"""

    requested_model: str
    routed_model: str
    upstream_model: str
    response_model: str


def resolve_model_identity(
    *,
    requested_model: str,
    route_info: dict,
    resolved_upstream_id: str | None,
    stable_response_model: str | None,
) -> ModelIdentity:
    """解析四种模型名。语义对齐旧 _chat_with_retry_for_model 内联逻辑：

    - routed_model = route_info.routed_model or requested_model
    - upstream_model = route_info.upstream_model_id or resolved_upstream_id or routed_model
    - response_model = stable_response_model（模型组请求固定）否则 routed_model
    """
    routed = route_info.get("routed_model") or requested_model
    upstream = route_info.get("upstream_model_id") or resolved_upstream_id or routed
    response = stable_response_model if stable_response_model is not None else routed
    return ModelIdentity(
        requested_model=requested_model,
        routed_model=routed,
        upstream_model=upstream,
        response_model=response,
    )


def build_attempt_body(original_body: dict) -> dict:
    """从原始请求体深拷贝出本次 attempt 独立的 body。

    深拷贝保证 messages / tools / metadata / 各种嵌套 dict 都不与原始 body 或其它
    attempt 共享引用（见 plan 坑点 §2：浅拷贝仍会共享嵌套对象被污染）。
    """
    return copy.deepcopy(original_body)


def apply_model_defaults(kwargs: dict, defaults: dict | None) -> dict:
    """把「当前 routed_model」的默认参数注入 kwargs。

    与旧 _apply_request_defaults 完全一致：只填值为 None 的 key，跳过 None 值与
    下划线开头的内部键；客户端显式值优先，绝不覆盖。原地修改并返回同一 dict。
    """
    if not isinstance(defaults, dict) or not defaults:
        return kwargs
    for key, value in defaults.items():
        if value is None or str(key).startswith("_"):
            continue
        if kwargs.get(key) is None:
            kwargs[key] = value
    return kwargs


# extra_config 中已被其它机制消费、不应作为出站请求字段覆盖的保留键。
# - client_preset: 由 providers/custom.py 链路消费（渠道级客户端模板）
# - enable_1m_context: 由 rate_limiter.py route_info 消费（1M 上下文开关）
# - output_modalities / modalities: 由 _route_supports_operation 消费（媒体操作筛选）
# - max_tokens / reasoning_effort / thinking: 走「默认值」语义（客户端优先，见
#   apply_extra_config_defaults），不参与 apply_extra_config_overrides 的强制覆盖。
_EXTRA_CONFIG_RESERVED_KEYS = frozenset({
    "client_preset",
    "enable_1m_context",
    "output_modalities",
    "modalities",
    "max_tokens",
    "reasoning_effort",
    "thinking",
})


# extra_config 里以「默认值」语义（客户端优先，只填 None key）注入的键。
# 与 apply_extra_config_overrides 的「强制覆盖」相反：客户端显式传值时保留客户端值，
# 仅当客户端未传时用渠道配置兜底。max_context_tokens 不在此列——它只参与选路过滤，
# 不注入出站请求。
_EXTRA_CONFIG_DEFAULT_KEYS = ("max_tokens", "reasoning_effort", "thinking")


def apply_extra_config_defaults(kwargs: dict, extra_config: dict | None) -> dict:
    """用 extra_config 的白名单键按「默认值」语义注入 kwargs（客户端优先）。

    与 ``apply_model_defaults`` 同语义：仅当 kwargs 里该键为 None（客户端未传）时才填入
    渠道配置值，客户端显式值永远优先。只作用于 ``_EXTRA_CONFIG_DEFAULT_KEYS`` 这几个键
    （max_tokens / reasoning_effort / thinking）——它们在渠道模型编辑弹框里作为「出站默认值」
    配置，无其它实际作用。

    原地修改并返回同一 dict。
    """
    if not isinstance(extra_config, dict) or not extra_config:
        return kwargs
    for key in _EXTRA_CONFIG_DEFAULT_KEYS:
        value = extra_config.get(key)
        if value is None:
            continue
        if kwargs.get(key) is None:
            kwargs[key] = value
    return kwargs


def apply_extra_config_overrides(kwargs: dict, extra_config: dict | None) -> dict:
    """用 extra_config 顶层字段强制覆盖出站请求 kwargs。

    与 ``apply_model_defaults`` / ``apply_extra_config_defaults`` 语义相反：那两者只填 None key
    （客户端优先），本函数是渠道管理员显式覆盖——强制改写出站字段，无论客户端是否带值。

    跳过：None 值、下划线开头的内部键、已被其它机制消费的保留键（见
    ``_EXTRA_CONFIG_RESERVED_KEYS``）。原地修改并返回同一 dict。

    注意：max_tokens / reasoning_effort / thinking 已移入保留键，走
    ``apply_extra_config_defaults`` 的默认值语义，不再在此强制覆盖。

    典型用法（extra_config JSON）：
        {"max_output_tokens": 8192}                       # 钳制/覆盖出站 max_output_tokens
        {"top_p": 0.9}                                    # 强制 top_p
        {"client_preset": "claude-code"}                  # 实际由 _route_row_from_db 提取，不在此覆盖
    """
    if not isinstance(extra_config, dict) or not extra_config:
        return kwargs
    for key, value in extra_config.items():
        if value is None or str(key).startswith("_") or key in _EXTRA_CONFIG_RESERVED_KEYS:
            continue
        kwargs[key] = value
    return kwargs
