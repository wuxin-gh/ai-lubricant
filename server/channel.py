"""Runtime channel domain object.

A Channel owns provider-level configuration and behavior that used to be
snapshotted onto every account/provider instance. Account objects keep
credentials and runtime state; channel-level reads go through this object.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

logger = logging.getLogger(__name__)


# upstream_stream 三态枚举：发给上游的 stream 模式。
# 存储形态是 provider_configs.config 这个 JSONB 里 chat_protocols 行的一个 key，
# 故用 JSON 原生的布尔 + 字面量 "auto"，保持库里裸读可懂、存量布尔零迁移。
UPSTREAM_STREAM_ON = True       # 固定开：无论客户端，上游一律流式
UPSTREAM_STREAM_OFF = False     # 固定关：无论客户端，上游一律非流式
UPSTREAM_STREAM_AUTO = "auto"   # 随客户端：上游 stream 跟随客户端请求

UPSTREAM_STREAM_CHOICES = (UPSTREAM_STREAM_ON, UPSTREAM_STREAM_OFF, UPSTREAM_STREAM_AUTO)


def is_upstream_stream_auto(value) -> bool:
    """value 是否为「随客户端」。大小写与两端空白不敏感。"""
    return isinstance(value, str) and value.strip().lower() == UPSTREAM_STREAM_AUTO


def normalize_upstream_stream(value):
    """规范化 upstream_stream 为三态枚举之一。

    - "auto"（大小写不敏感）→ UPSTREAM_STREAM_AUTO
    - 缺省/None → UPSTREAM_STREAM_ON（沿用既有默认，存量行为不变）
    - 显式假值 → UPSTREAM_STREAM_OFF；其余真值 → UPSTREAM_STREAM_ON
    """
    if is_upstream_stream_auto(value):
        return UPSTREAM_STREAM_AUTO
    if value is None:
        return UPSTREAM_STREAM_ON
    return value is not False


def resolve_upstream_stream(value, client_stream: bool) -> bool:
    """按客户端请求的 stream 把三态解析成最终发给上游的布尔 stream。

    auto 时跟随 client_stream；固定开/关直接返回对应布尔。
    这是所有运行时判定的唯一入口——不要在调用点自己比对 "auto" 字面量。
    """
    if is_upstream_stream_auto(value):
        return bool(client_stream)
    if value is None:
        return True
    return value is not False


# ── model_id 改写规则 ──────────────────────────────────────────────────────
# 上游模型 ID 到对外 model_id 的改写。存储形态是 provider_configs.config 这个
# JSONB 里的 model_id_rewrite_rules 列表。顺序敏感：自上而下逐条 re.sub，不是
# 命中即停，这样多条规则可以叠加。
#
# 列表是混合条目：kind="rule" 是内联规则，kind="template" 是对全局模版的引用。
# kind 缺省视为 "rule"，所以存量的纯规则数组读出来行为不变。模版引用是活引用：
# 展开发生在 apply 时，改模版立刻影响所有引用它的渠道，位置即优先级——模版与
# 内联规则之间没有高低之分，完全由用户在同一个列表里排序决定。
#
# 作用域仅限「本次新获取到的模型」——已跟踪模型沿用库里既有 model_id，加规则
# 不会追溯改名。切 "/" 前缀由 apply 内部先做，规则跑在切完之后。
MODEL_ID_RULE_FIELDS = ("name", "enabled", "pattern", "replacement")
MODEL_ID_TEMPLATE_FIELDS = ("kind", "template_id", "enabled")

MODEL_ID_ENTRY_RULE = "rule"
MODEL_ID_ENTRY_TEMPLATE = "template"

# 全局模型规则模版缓存：{template_id: [rule, ...]}。真相源是 app_config 表的
# model_rule_templates，由 admin 写入后经 runtime_sync 广播回灌到这里。
# 缓存放在 channel.py 而不是 ModelClientPool：rate_limiter 导入 channel，
# 反向引用会成环。
_MODEL_RULE_TEMPLATES: dict[str, list[dict]] = {}


def set_model_rule_template_cache(templates) -> None:
    """全量替换模版缓存。templates 为 [{"id","name","rules"}, ...]。

    模版内只允许内联规则（保存侧拒绝模版嵌模版）；这里再兜一层，丢掉任何嵌套的
    模版引用，保证缓存里存的一定是纯内联规则——展开时直接摊平即可，绝不递归。
    """
    cache: dict[str, list[dict]] = {}
    for item in templates or []:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("id") or "").strip()
        if not tid:
            continue
        cache[tid] = [
            entry for entry in normalize_model_id_rewrite_rules(item.get("rules"))
            if not is_model_id_template_entry(entry)
        ]
    global _MODEL_RULE_TEMPLATES
    _MODEL_RULE_TEMPLATES = cache


def get_model_rule_template(template_id: str) -> list[dict] | None:
    """按 id 取模版规则；未命中返回 None（区别于「模版存在但没有规则」）。"""
    return _MODEL_RULE_TEMPLATES.get(str(template_id or "").strip())


def strip_model_owner_prefix(model_id: str) -> str:
    """owner/model 形式（如 1111/glm-5.2）只保留首个 "/" 之后的部分。

    无 "/" 或后半为空则原样返回，避免把显式指定的 model_id 改坏。
    """
    if not model_id or "/" not in model_id:
        return model_id
    tail = model_id.split("/", 1)[1].strip()
    return tail or model_id


def normalize_model_id_rewrite_rules(raw) -> list[dict]:
    """规范化 model_id 改写条目列表（内联规则 + 模版引用混合）。

    丢弃非 dict 项、pattern 为空的规则行、template_id 为空的引用行；不在此处
    编译正则、也不校验模版是否存在（校验由保存侧负责，见 admin 的写入路径），
    因为运行时读配置不该因为一条坏规则或一个后建的模版整体失败。

    返回的规则条目不带 kind 键，与改动前逐字节一致；模版条目带 kind。
    """
    if not isinstance(raw, list):
        return []
    entries: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or MODEL_ID_ENTRY_RULE) == MODEL_ID_ENTRY_TEMPLATE:
            template_id = str(item.get("template_id") or "").strip()
            if not template_id:
                continue
            entries.append({
                "kind": MODEL_ID_ENTRY_TEMPLATE,
                "template_id": template_id,
                "enabled": item.get("enabled", True) is not False,
            })
            continue
        pattern = item.get("pattern")
        if not isinstance(pattern, str) or not pattern.strip():
            continue
        replacement = item.get("replacement")
        entries.append({
            "name": str(item.get("name") or "").strip(),
            "enabled": item.get("enabled", True) is not False,
            "pattern": pattern,
            "replacement": replacement if isinstance(replacement, str) else "",
        })
    return entries


def is_model_id_template_entry(entry) -> bool:
    """条目是否为模版引用。kind 缺省视为内联规则。"""
    return isinstance(entry, dict) and entry.get("kind") == MODEL_ID_ENTRY_TEMPLATE


def expand_model_id_rewrite_rules(raw) -> list[dict]:
    """把混合条目列表展开成扁平的内联规则列表，保持原有顺序。

    模版引用就地展开成模版自己的规则序列——所以「规则、模版、规则」三条的效果
    等价于把模版内容原地摊平。停用的模版整条跳过；模版不存在时记日志后跳过，
    不抛错：模版是活引用，可能后建，也可能随渠道模板跨实例导入时尚未同步。
    """
    expanded: list[dict] = []
    for entry in normalize_model_id_rewrite_rules(raw):
        if not is_model_id_template_entry(entry):
            expanded.append(entry)
            continue
        if not entry["enabled"]:
            continue
        rules = get_model_rule_template(entry["template_id"])
        if rules is None:
            logger.warning(
                f"model_id 改写跳过模版引用（模版不存在）template_id={entry['template_id']!r}"
            )
            continue
        expanded.extend(rules)
    return expanded


def compile_model_id_rewrite_rules(raw) -> tuple[list[dict], list[str]]:
    """编译校验条目列表，返回 (规范化条目, 错误信息列表)。

    保存前用这个做校验：任何一条正则不合法都应当被拒绝，而不是等到模型同步时
    才炸。错误信息带上行号与规则名，便于前端定位是哪一条坏了。

    只校验内联规则的正则。模版引用不校验 template_id 是否存在——模版可以后建，
    渠道也可能随渠道模板跨实例导入，引用先落库、模版后同步是合法状态。
    """
    entries = normalize_model_id_rewrite_rules(raw)
    errors: list[str] = []
    for idx, entry in enumerate(entries, start=1):
        if is_model_id_template_entry(entry):
            continue
        try:
            re.compile(entry["pattern"])
        except re.error as exc:
            label = entry["name"] or f"第 {idx} 条"
            errors.append(f"{label}: 正则不合法 ({exc})")
    return entries, errors


def apply_model_id_rewrite_rules(model_id: str, rules, *, strip_prefix: bool = True) -> str:
    """把上游模型 ID 转成对外 model_id：先切 "/" 前缀，再逐条跑改写规则。

    这是所有改写判定的唯一入口——不要在调用点自己切前缀、展开模版或跑 re.sub。
    模版引用在这里展开（活引用，取当前模版内容）。
    单条规则抛异常时跳过该条并记日志，不让一个坏规则打断整轮模型同步。
    结果为空则回退到改写前的值，避免把模型名抹成空串。
    """
    result = strip_model_owner_prefix(model_id) if strip_prefix else (model_id or "")
    if not result:
        return result
    for rule in expand_model_id_rewrite_rules(rules):
        if not rule["enabled"]:
            continue
        try:
            rewritten = re.sub(rule["pattern"], rule["replacement"], result)
        except re.error as exc:
            logger.warning(
                f"model_id 改写规则跳过（正则错误）name={rule['name'] or '-'} "
                f"pattern={rule['pattern']!r}: {exc}"
            )
            continue
        if rewritten.strip():
            result = rewritten.strip()
    return result or strip_model_owner_prefix(model_id)


def normalize_chat_protocols(raw) -> list[dict]:
    """规范化 chat_protocols 列表。"""
    if not isinstance(raw, list):
        return []
    seen_ids: set[str] = set()
    result: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        protocol = str(item.get("protocol") or "openai").lower()
        path = str(item.get("path") or "").strip()
        cp_id = str(item.get("id") or "").strip()
        if not cp_id:
            cp_id = f"{protocol}-chat-{idx}"
        while cp_id in seen_ids:
            cp_id = f"{cp_id}-{idx}"
        seen_ids.add(cp_id)
        models = item.get("models")
        if not isinstance(models, list):
            models = []
        else:
            models = [str(m) for m in models if isinstance(m, str) and str(m).strip()]
        client_preset = str(item.get("client_preset") or "none").lower()
        header_template = str(item.get("header_template") or "").strip()
        upstream_stream = normalize_upstream_stream(item.get("upstream_stream"))
        # openai 协议行专用：是否把 assistant.reasoning_content 透传给上游。
        # 默认 true（沿用既有直通行为）；显式假值（含 "false" 字符串）才剥离。
        send_reasoning_content = item.get("send_reasoning_content")
        if isinstance(send_reasoning_content, str):
            send_reasoning_content = send_reasoning_content.strip().lower() not in ("false", "0", "no", "off")
        else:
            send_reasoning_content = send_reasoning_content is not False
        result.append({
            "id": cp_id,
            "enabled": bool(item.get("enabled", True)),
            "protocol": protocol,
            "path": path,
            "upstream_stream": upstream_stream,
            "client_preset": client_preset,
            "header_template": header_template,
            "system_type": str(item.get("system_type") or "auto").lower(),
            "send_reasoning_content": send_reasoning_content,
            "models": models,
        })
    return result


# 顶层旧字段 → chat_protocols 行的迁移键（不再作为运行时读取来源，仅供一次性合成）。
_LEGACY_CHAT_KEYS = ("protocol", "chat_path", "upstream_stream", "client_preset", "supports_stream")


def synth_chat_protocol_row(
    protocol: str = "openai",
    path: str = "",
    upstream_stream=True,
    client_preset: str = "none",
    header_template: str = "",
    models=None,
    cp_id: str | None = None,
    system_type: str = "auto",
    send_reasoning_content=True,
) -> dict:
    """按 chat_protocols 行 schema 合成单行。"""
    protocol = str(protocol or "openai").lower()
    return {
        "id": cp_id or f"{protocol}-chat-0",
        "enabled": True,
        "protocol": protocol,
        "path": str(path or ""),
        "upstream_stream": normalize_upstream_stream(upstream_stream),
        "client_preset": str(client_preset or "none").lower(),
        "header_template": str(header_template or ""),
        "system_type": str(system_type or "auto").lower(),
        "send_reasoning_content": send_reasoning_content is not False,
        "models": [str(m) for m in (models or []) if isinstance(m, str) and str(m).strip()],
    }


def chat_protocols_from_legacy(cfg: dict) -> list[dict]:
    """从顶层 protocol/chat_path/upstream_stream/client_preset 合成一条协议行（迁移/派生默认用）。

    已有非空 chat_protocols 时原样返回其规范化结果；否则依据顶层旧字段合成一行。
    顶层无任何可迁移信息（protocol 与 chat_path 均缺）时返回空列表，交由上层判定为无效配置。
    """
    if not isinstance(cfg, dict):
        return []
    existing = cfg.get("chat_protocols")
    if isinstance(existing, list) and any(isinstance(x, dict) for x in existing):
        return normalize_chat_protocols(existing)
    protocol = cfg.get("protocol")
    chat_path = cfg.get("chat_path")
    if protocol in (None, "") and not chat_path:
        return []
    upstream_stream = cfg.get("upstream_stream")
    if upstream_stream is None:
        upstream_stream = cfg.get("supports_stream", True)
    return [synth_chat_protocol_row(
        protocol=protocol or "openai",
        path=chat_path or "",
        upstream_stream=upstream_stream,
        client_preset=cfg.get("client_preset") or "none",
    )]


def default_config_with_chat_protocols(cfg: dict | None) -> dict | None:
    """把派生/内置默认配置里的顶层协议字段折叠成 chat_protocols 行，并移除顶层旧字段。

    渠道创建时写入 DB 的即是此结果：单一事实来源是 chat_protocols，不再落顶层 protocol/chat_path 等。
    """
    if not isinstance(cfg, dict):
        return cfg
    result = dict(cfg)
    rows = chat_protocols_from_legacy(result)
    if rows:
        result["chat_protocols"] = rows
    for key in _LEGACY_CHAT_KEYS:
        result.pop(key, None)
    return result


@dataclass(frozen=True)
class ScoringConfig:
    # 选择 / 频率 / 熔断常量（原 ModelClientPool 类属性，数值不变）
    pick_window_seconds: int = 180
    pick_soft_cap: int = 5
    error_window_seconds: int = 60
    provider_pick_threshold: int = 5
    provider_pick_decay_base: float = 0.55
    provider_pick_floor: float = 0.10
    provider_usage_history_window_seconds: int = 600
    provider_usage_history_max_rounds: int = 20
    provider_usage_5_count: int = 3
    provider_usage_5_factor: float = 0.65
    provider_usage_10_count: int = 5
    provider_usage_10_factor: float = 0.45
    provider_usage_10_median_cap: float = 0.35
    provider_usage_20_count: int = 8
    provider_usage_20_factor: float = 0.25
    provider_usage_20_median_cap: float = 0.12
    consecutive_circuit_threshold: int = 5
    consecutive_circuit_base: int = 30
    consecutive_circuit_step: int = 15
    consecutive_circuit_max: int = 120
    consecutive_decay_intelligent: float = 0.8
    consecutive_decay_fast: float = 0.7
    session_affinity_ttl: int = 600
    session_failure_ttl: int = 300
    session_failure_exclude: int = 4
    # 评分公式常量
    speed_divisor: float = 10.0
    speed_fallback: float = 0.5
    weight_norm: float = 100.0
    intelligent_speed_weight: float = 0.20
    intelligent_error_weight: float = 0.25
    intelligent_balance_weight: float = 0.15
    intelligent_priority_weight: float = 0.25
    intelligent_weight_weight: float = 0.15
    fast_speed_weight: float = 0.67
    fast_error_weight: float = 0.10
    fast_balance_weight: float = 0.10
    fast_priority_weight: float = 0.08
    fast_weight_weight: float = 0.05
    billing_crossover_tokens: float = 2000.0
    ttft_ewma_old: float = 0.8
    ttft_ewma_new: float = 0.2
    ttft_min_ewma: float = 50.0
    channel_score_floor: float = 0.1
    channel_score_cap: float = 10.0
    close_score_threshold: float = 0.95


DEFAULT_SCORING = ScoringConfig()


# 派生渠道的固有默认值（从子类 __init__ 的 setdefault 抽出）。
# 协议 / chat_path / upstream_stream / client_preset 不再作为顶层默认；协议路由统一走 chat_protocols。
_PROVIDER_DEFAULTS: dict[str, dict[str, Any]] = {
    "copilot": {"base_url": "https://api.githubcopilot.com", "models_path": "/models"},
    "edgeone-ai": {"models_path": "", "auto_update_models": False},
    "codebuddy": {"models_path": "/plugin/v1/models", "auto_update_models": True, "timeout": 120},
    "atomcode": {"models_path": "/models", "auto_update_models": True, "timeout": 120},
    "cloudflare": {"auto_update_models": True},
}

_CODEBUDDY_CN_BASE_URL = "https://www.codebuddy.cn"
_CODEBUDDY_INTL_BASE_URL = "https://www.codebuddy.ai"


def provider_defaults(provider_name: str, cfg: dict) -> dict:
    """该渠道类的固有默认值（DB 没给时兜底）。"""
    key = (provider_name or cfg.get("provider_name") or cfg.get("channel_name") or cfg.get("builtin_type") or cfg.get("type") or "").strip()
    defaults = dict(_PROVIDER_DEFAULTS.get(key) or {})
    if key == "codebuddy":
        region = str(cfg.get("region") or "cn").lower()
        defaults["base_url"] = _CODEBUDDY_INTL_BASE_URL if region == "intl" else _CODEBUDDY_CN_BASE_URL
        if str(cfg.get("chat_path") or "") in ("", "/plugin/chat/completions", "plugin/chat/completions"):
            defaults["chat_path"] = "/v2/chat/completions"
        if str(cfg.get("models_path") or "") in ("", "/v2/models", "v2/models"):
            defaults["models_path"] = "/plugin/v1/models"
    if key == "atomcode":
        legacy_base = str(cfg.get("base_url") or "").rstrip("/")
        legacy_chat_path = str(cfg.get("chat_path") or "")
        if cfg.get("protocol") in (None, "anthropic"):
            defaults["protocol"] = "openai"
        if not legacy_base or legacy_base == "https://coding.atomgit.com":
            defaults["base_url"] = "https://llm-api.atomgit.com/v1"
        if legacy_chat_path in ("", "/v1/messages", "v1/messages"):
            defaults["chat_path"] = "/chat/completions"
        if str(cfg.get("models_path") or "") in ("", "/v1/models", "v1/models"):
            defaults["models_path"] = "/coding-plan/models-v2"
    return defaults


class Channel:
    """渠道运行时领域对象：持有渠道级配置与行为，账号通过它读取渠道参数。"""

    def __init__(self, provider_name: str, cfg: dict | None = None):
        self.provider_name = provider_name
        self.scoring = DEFAULT_SCORING
        # 本渠道完整模型表：每行保留 upstream_model_id、model_id 及其元数据。
        # 这是"渠道具备哪些模型"的内存真相源，请求分发时被扫描（零查库）。
        self.models: list[dict] = []
        self._ttft_ewma_ms: float | None = None
        self._channel_score: float = 1.0
        self._enabled = (cfg or {}).get("enabled", True) is not False
        self._state = self._build_state(cfg or {})

    @property
    def enabled(self) -> bool:
        """渠道手动禁用开关；独立于配置快照派生状态，避免 apply 重建时丢失。"""
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        # 兼容旧同步调用；持久化入口应使用 disable/enable。
        self._enabled = bool(value)

    def disable(self) -> None:
        """在内存中禁用渠道；持久化由调用方异步完成。"""
        self._enabled = False

    def enable(self) -> None:
        """在内存中启用渠道；持久化由调用方异步完成。"""
        self._enabled = True

    def apply(self, cfg: dict | None = None) -> None:
        """应用新配置快照；只更新配置桶，保留模型表及渠道运行统计。"""
        snapshot = dict(cfg or {})
        if "enabled" in snapshot:
            self._enabled = snapshot.get("enabled") is not False
        self._state = self._build_state(snapshot)

    def apply_models(self, rows: list[dict]) -> None:
        """应用新模型表快照；模型行是纯配置数据，不承载冻结状态。"""
        self.models = [dict(row) for row in (rows or []) if isinstance(row, dict)]

    def update(self, cfg: dict | None = None) -> None:
        """兼容旧调用；配置同步统一走 apply。"""
        self.apply(cfg)

        """第一条 enabled 协议行的 protocol；无协议行时返回 'openai' 仅为占位（has_chat_protocols=False 时上游不会发起）。"""
        cps = self.all_chat_protocols()
        for cp in cps:
            if cp.get("enabled", True):
                return str(cp.get("protocol") or "openai").lower()
        if cps:
            return str(cps[0].get("protocol") or "openai").lower()
        return "openai"

    def _merged_config(self, cfg: dict) -> dict:
        defaults = provider_defaults(self.provider_name, cfg)
        merged = {**defaults, **dict(cfg or {})}
        merged.setdefault("provider_name", self.provider_name)
        key = (self.provider_name or merged.get("provider_name") or merged.get("channel_name") or merged.get("builtin_type") or merged.get("type") or "").strip()
        if key == "codebuddy":
            legacy_chat = str(merged.get("chat_path") or "")
            legacy_models = str(merged.get("models_path") or "")
            legacy_base = str(merged.get("base_url") or "").rstrip("/")
            region = str(merged.get("region") or "cn").lower()
            merged["base_url"] = _CODEBUDDY_INTL_BASE_URL if region == "intl" else _CODEBUDDY_CN_BASE_URL
            if legacy_chat in ("", "/plugin/chat/completions", "plugin/chat/completions"):
                legacy_chat = "/v2/chat/completions"
            if legacy_models in ("", "/v2/models", "v2/models"):
                legacy_models = "/plugin/v1/models"
            existing = merged.get("chat_protocols")
            if not isinstance(existing, list) or not existing:
                merged["chat_protocols"] = [{
                    "id": "openai-chat-0",
                    "enabled": True,
                    "protocol": "openai",
                    "path": legacy_chat,
                    "upstream_stream": True,
                    "client_preset": "none",
                    "header_template": "",
                    "send_reasoning_content": True,
                    "models": [],
                }]
            merged.setdefault("models_path", legacy_models)
        if key == "atomcode":
            legacy_base = str(merged.get("base_url") or "").rstrip("/")
            if not legacy_base or legacy_base == "https://coding.atomgit.com":
                merged["base_url"] = "https://llm-api.atomgit.com/v1"
            legacy_chat = str(merged.get("chat_path") or "")
            if legacy_chat in ("", "/v1/messages", "v1/messages"):
                legacy_chat = "/chat/completions"
            legacy_models = str(merged.get("models_path") or "")
            if legacy_models in ("", "/v1/models", "v1/models"):
                legacy_models = "/coding-plan/models-v2"
            existing = merged.get("chat_protocols")
            if not isinstance(existing, list) or not existing:
                merged["chat_protocols"] = [{
                    "id": "openai-chat-0",
                    "enabled": True,
                    "protocol": "openai",
                    "path": legacy_chat,
                    "upstream_stream": True,
                    "client_preset": "none",
                    "header_template": "",
                    "send_reasoning_content": True,
                    "models": [],
                }]
            merged.setdefault("models_path", legacy_models)
        # 通用兜底：池外临时实例可能仅带顶层 protocol/chat_path（旧 kwargs 或探测流程）。
        # 无 chat_protocols 时按顶层字段合成一行，等价于迁移转换在构造期发生；DB 落库仍由保存校验把关。
        existing = merged.get("chat_protocols")
        if not (isinstance(existing, list) and any(isinstance(x, dict) for x in existing)):
            rows = chat_protocols_from_legacy(merged)
            if rows:
                merged["chat_protocols"] = rows
        return merged

    def _build_state(self, cfg: dict) -> SimpleNamespace:
        merged = self._merged_config(cfg)
        raw_chat_protocols = merged.get("chat_protocols")
        chat_protocols = normalize_chat_protocols(raw_chat_protocols)
        primary_chat = next((c for c in chat_protocols if c.get("enabled", True)), None)
        if primary_chat is None and chat_protocols:
            primary_chat = chat_protocols[0]
        # protocol/chat_path/upstream_stream/client_preset 均为协议行派生视图；不再读取顶层旧字段。
        protocol = str((primary_chat or {}).get("protocol") or "openai").lower()
        chat_path = (primary_chat or {}).get("path") or ""
        upstream_stream = (primary_chat or {}).get("upstream_stream", True)
        if upstream_stream is None:
            upstream_stream = True
        client_preset = str((primary_chat or {}).get("client_preset") or "none").lower()
        base_url = (merged.get("base_url") or "").rstrip("/")
        models_path = merged.get("models_path") or ("/v1beta/models" if protocol == "gemini" else "/v1/models")
        supported = merged.get("supported_protocols") or [c.get("protocol") for c in chat_protocols if c.get("protocol")] or [protocol]
        supported_protocols = [str(p).lower() for p in supported if isinstance(p, str) and str(p).strip()]
        if not supported_protocols:
            supported_protocols = [protocol] if protocol else ["openai"]
        try:
            timeout_seconds = int(merged.get("timeout") or 120)
        except (TypeError, ValueError):
            timeout_seconds = 120
        if timeout_seconds <= 0:
            timeout_seconds = 120
        health_check = merged.get("health_check") or {}
        raw_tags = merged.get("tags")
        tags = frozenset(
            tag.strip() for tag in raw_tags
            if isinstance(tag, str) and tag.strip()
        ) if isinstance(raw_tags, list) else frozenset()
        raw_extra_retry_codes = merged.get("extra_retry_status_codes")
        if isinstance(raw_extra_retry_codes, (int, str)):
            raw_extra_retry_codes = [raw_extra_retry_codes]
        extra_retry_status_codes: set[int] = set()
        if isinstance(raw_extra_retry_codes, list):
            for item in raw_extra_retry_codes:
                if isinstance(item, bool):
                    continue
                try:
                    code = int(item)
                except (TypeError, ValueError):
                    continue
                if 100 <= code <= 599:
                    extra_retry_status_codes.add(code)
        return SimpleNamespace(
            raw=merged,
            enabled=merged.get("enabled", True) is not False,
            provider_name=self.provider_name,
            protocol=protocol,
            base_url=base_url,
            chat_path=chat_path,
            models_path=models_path,
            gemini_api_version=merged.get("gemini_api_version") or "v1beta",
            auth_header_style=(merged.get("auth_header_style") or merged.get("gemini_auth_style") or "api_key_header").lower(),
            image_path=merged.get("image_path") or "/v1/images/generations",
            video_path=merged.get("video_path") or "/v1/videos/generations",
            speech_path=merged.get("speech_path") or "/v1/audio/speech",
            supports_tts=bool(merged.get("supports_tts", False)),
            supports_image_generation=bool(merged.get("supports_image_generation", False)),
            supports_video_generation=bool(merged.get("supports_video_generation", False)),
            upstream_stream=upstream_stream,
            auto_update_models=merged.get("auto_update_models", True),
            model_id_rewrite_rules=normalize_model_id_rewrite_rules(merged.get("model_id_rewrite_rules")),
            channel_remark=merged.get("remark", ""),
            tags=tags,
            price_remark=merged.get("price_remark", ""),
            test_model=merged.get("test_model") or health_check.get("test_model"),
            client_preset=client_preset,
            has_chat_protocols=bool(chat_protocols),
            balance_config=merged.get("balance") or {},
            timeout_seconds=timeout_seconds,
            extra_retry_status_codes=frozenset(extra_retry_status_codes),
            response_compat_mode=bool(merged.get("response_compat_mode", False)),
            chat_protocols=chat_protocols,
            supported_protocols=supported_protocols,
            priority=int(merged.get("account_priority", merged.get("priority", 0)) or 0),
            weight=max(1, int(merged.get("account_weight", merged.get("weight", 1)) or 1)),
            billing_mode=merged.get("billing_mode", "token"),
            health_check=health_check,
            region=merged.get("region"),
        )

    def get(self, key: str, default=None):
        return self._state.raw.get(key, default)

    def __getattr__(self, item: str):
        state = object.__getattribute__(self, "_state")
        if hasattr(state, item):
            return getattr(state, item)
        raise AttributeError(item)

    # ── cloudflare：base_url 模板写死在渠道层，account_id 由账号提供 ──
    def cloudflare_base_url(self, account_id: str | None = None) -> str:
        if self.provider_name == "cloudflare" and account_id:
            return f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1"
        return self.base_url

    def cloudflare_models_url(self, account_id: str | None = None) -> str:
        if self.provider_name == "cloudflare" and account_id:
            return f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/models/search"
        return self.models_path

    def _url(self, path: str, account_id: str | None = None) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        base_url = self.cloudflare_base_url(account_id)
        return f"{base_url}/{path.lstrip('/')}"

    def chat_url(self, kwargs: dict | None = None, account_id: str | None = None) -> str:
        """按已选择/配置的 path 构造 URL；protocol 只在旧配置无 path 时兜底。"""
        active = self.active_chat_protocol(kwargs)
        if active and active.get("path"):
            return self._url(active["path"], account_id)
        if self.chat_path:
            return self._url(self.chat_path, account_id)
        # 仅兼容没有任何 path 的旧配置。调用方不得因请求协议或客户端模拟进入此分支。
        if self.protocol == "anthropic":
            return self._url("/v1/messages", account_id)
        if self.protocol == "responses":
            return self._url("/v1/responses", account_id)
        return self._url("/v1/chat/completions", account_id)

    def image_url(self, account_id: str | None = None) -> str:
        return self._url(self.image_path or "/v1/images/generations", account_id)

    def video_url(self, account_id: str | None = None) -> str:
        return self._url(self.video_path or "/v1/videos/generations", account_id)

    def speech_url(self, account_id: str | None = None) -> str:
        return self._url(self.speech_path or "/v1/audio/speech", account_id)

    # ── 协议筛选（渠道层只取一次，账号复用）──
    def all_chat_protocols(self) -> list[dict]:
        raw = self.chat_protocols
        return [c for c in raw if isinstance(c, dict)] if isinstance(raw, list) else []

    @staticmethod
    def _chat_protocol_rank(cp: dict, model_id: str | None, request_protocol: str | None) -> int:
        """协议行优先级：显式勾选模型 > 通用行；同一档内同协议优先。

        勾选模型代表"该模型就要走这条链路"，是管理员的显式绑定，
        优先级高于协议直通；协议只在同一档内做次级排序。
        """
        bound = bool(model_id and (cp.get("models") or []))
        same_protocol = bool(request_protocol and cp.get("protocol") == request_protocol)
        if bound:
            return 0 if same_protocol else 1
        return 2 if same_protocol else 3

    def chat_protocols_for(self, model_id: str | None = None, protocol: str | None = None) -> list[dict]:
        result: list[dict] = []
        for cp in self.all_chat_protocols():
            if not cp.get("enabled", True):
                continue
            if protocol and cp.get("protocol") != protocol:
                continue
            models = cp.get("models") or []
            if model_id and models and model_id not in models:
                continue
            result.append(cp)
        return result

    def select_chat_protocol(self, model_id: str | None = None, request_protocol: str | None = None) -> dict | None:
        candidates = self.get_chat_protocol_candidates(model_id, request_protocol)
        return candidates[0] if candidates else None

    def get_chat_protocol_candidates(self, model_id: str | None = None, request_protocol: str | None = None) -> list[dict]:
        """按「勾选模型优先 → 同协议优先」排序，保持同档内的配置原顺序。"""
        configs = self.chat_protocols_for(model_id)
        return sorted(configs, key=lambda cp: self._chat_protocol_rank(cp, model_id, request_protocol))

    @staticmethod
    def active_chat_protocol(kwargs: dict | None = None) -> dict | None:
        kwargs = kwargs or {}
        cp = kwargs.get("_endpoint_config")
        if not isinstance(cp, dict):
            cp = kwargs.get("_chat_protocol")
        return cp if isinstance(cp, dict) else None

    # ── 模型表（渠道级，内存真相源）──
    def register_models(self, rows: list[dict]) -> None:
        """灌入本渠道完整模型行，保留同一 model_id 的多条 upstream 映射。"""
        self.models = [dict(row) for row in (rows or []) if isinstance(row, dict)]

    def resolve_upstream_id(self, model_id: str) -> str:
        """按现有路由顺序返回第一个匹配的上游模型；无匹配时保留旧兜底。"""
        for row in self.models:
            if row.get("model_id") == model_id:
                return row.get("upstream_model_id") or model_id
        return model_id

    def public_model_id(self, model_id: str) -> str:
        """上游模型 ID → 对外 model_id：切 "/" 前缀后套本渠道的改写规则。

        纯粹回答「按当前配置这个模型该叫什么」，不关心库里现状。已跟踪的行要不要
        跟着改由调用方按 has_model_id_rewrite_rules 判定。
        """
        return apply_model_id_rewrite_rules(model_id, self.model_id_rewrite_rules)

    def has_model_id_rewrite_rules(self) -> bool:
        """本渠道是否配了生效中的 model_id 改写规则。

        用来区分两种"改名"：切 owner/ 前缀是无配置时的默认形态，而改写规则是管理员
        的显式意图。只有配了规则时才允许重算已入库的行——否则一轮定时同步就会把
        管理端里的手工改名冲回默认形态。

        看的是**展开后**的结果：模版引用展开为空（模版被删、或整条停用）等于没规则，
        全部规则都 enabled=False 也等于没规则。
        """
        return any(
            rule["enabled"]
            for rule in expand_model_id_rewrite_rules(self.model_id_rewrite_rules)
        )

    # ── 渠道级 TTFT/响应时间统计 ──
    def record_ttft(self, ttft_ms: int | float | None) -> None:
        if ttft_ms is None:
            return
        try:
            value = float(ttft_ms)
        except (TypeError, ValueError):
            return
        if value <= 0:
            return
        if self._ttft_ewma_ms is None:
            self._ttft_ewma_ms = value
        else:
            self._ttft_ewma_ms = self.scoring.ttft_ewma_old * self._ttft_ewma_ms + self.scoring.ttft_ewma_new * value
        self._channel_score = max(
            self.scoring.channel_score_floor,
            min(self.scoring.channel_score_cap, 1000 / max(self._ttft_ewma_ms, self.scoring.ttft_min_ewma)),
        )

    def record_failure(self) -> None:
        self._channel_score = max(self.scoring.channel_score_floor, self._channel_score * 0.8)

    def channel_score(self) -> float:
        return self._channel_score

    def ttft_ewma_ms(self) -> float | None:
        return self._ttft_ewma_ms

    # ── 冻结决策（策略已是渠道级，这里暴露）──
    def match_freeze(self, *, status_code=None, headers=None, exception=None, error_code=None):
        try:
            from limit_policy_store import get_effective_provider_policy_sync, match_freeze_rules
            policy = get_effective_provider_policy_sync(self.provider_name)
            return match_freeze_rules(policy.get("freeze_policy"), status_code=status_code, headers=headers, exception=exception, error_code=error_code)
        except Exception:
            return None

    def freeze_refresh_on_failure(self) -> bool:
        """渠道级「失败刷新冻结周期」：关时已冻结对象保留现有到期时间，不被后续失败刷新。

        只看渠道配置，与请求模式（正常/定时检测/手动测试）无关。读不到策略时按默认开处理，
        与 normalize_freeze_policy 的缺省一致——宁可刷新也不要静默留住一个过长的冻结。
        """
        try:
            from limit_policy_store import get_effective_provider_policy_sync
            policy = get_effective_provider_policy_sync(self.provider_name)
            freeze_policy = policy.get("freeze_policy") or {}
            return bool(freeze_policy.get("refresh_freeze_on_failure", True))
        except Exception:
            return True