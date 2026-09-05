"""Provider limit policy helpers."""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from functools import lru_cache

import config
from db import PostgresClient

DEFAULT_COOLDOWN_POLICY = {"429": "60", "error": "60"}
DEFAULT_FREEZE_POLICY = {
    "enabled": True,
    "refresh_freeze_on_failure": True,
    "rules": [
        {
            "condition": "status_code",
            "key": "",
            "operator": "==",
            "value": "403",
            "freeze_object": "account",
            "freeze_period": "today",
            "freeze_value": 0,
        },
        {
            "condition": "exception",
            "key": "",
            "operator": "",
            "value": "",
            "freeze_object": "account",
            "freeze_period": "seconds",
            "freeze_value": 60,
        },
        {
            "condition": "status_code",
            "key": "",
            "operator": "==",
            "value": "429",
            "freeze_object": "account",
            "freeze_period": "seconds",
            "freeze_value": 60,
        },
    ],
}


def default_freeze_policy() -> dict:
    return {
        "enabled": bool(DEFAULT_FREEZE_POLICY["enabled"]),
        "refresh_freeze_on_failure": bool(DEFAULT_FREEZE_POLICY["refresh_freeze_on_failure"]),
        "rules": [dict(rule) for rule in DEFAULT_FREEZE_POLICY["rules"]],
    }


def get_default_freeze_policy() -> dict:
    """全局配置里的「新增渠道默认冻结策略」；缺失/非法回退硬编码默认。

    只在创建渠道与管理端读写全局配置时调用；读的是 CONFIG_STORE 内存快照，
    代价可忽略。永不抛错——新建渠道不能因为一份坏配置而失败。
    """
    try:
        raw = (config.CONFIG_STORE.read_main() or {}).get("default_freeze_policy")
    except Exception:
        raw = None
    if isinstance(raw, dict):
        normalized = normalize_freeze_policy(raw)
        if normalized.get("rules"):
            return normalized
    return default_freeze_policy()


def _int_value(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_cooldown_seconds(value, default: int = 0, *, strict: bool = False) -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        if strict:
            raise ValueError("冷却秒数必须是整数")
        return default
    if seconds < -2:
        if strict:
            raise ValueError("冷却秒数只能为 -2、-1 或大于等于 0")
        return default
    return seconds


def normalize_cooldown_policy(policy: dict | None) -> dict[str, str]:
    raw = policy if isinstance(policy, dict) else {}
    normalized: dict[str, str] = {}
    for key, value in raw.items():
        key_s = str(key or "").strip().lower()
        if not key_s:
            continue
        if key_s == "error":
            normalized["error"] = str(_parse_cooldown_seconds(value, 60))
            continue
        codes = []
        valid = True
        for part in key_s.split(","):
            part = part.strip()
            try:
                code = int(part)
            except (TypeError, ValueError):
                valid = False
                break
            if code < 100 or code > 599:
                valid = False
                break
            codes.append(str(code))
        if valid and codes:
            normalized[",".join(codes)] = str(_parse_cooldown_seconds(value, 0))
    if "error" not in normalized:
        normalized["error"] = "60"
    if not any("429" in key.split(",") for key in normalized if key != "error"):
        normalized["429"] = "60"
    return normalized


def validate_cooldown_policy(policy: dict | None) -> dict[str, str]:
    if not isinstance(policy, dict):
        raise ValueError("冷却策略必须是 JSON 对象")
    # 自动补齐默认 error 键，避免冻结策略-only 提交时抛错
    if "error" not in {str(k).strip().lower() for k in policy.keys()}:
        policy = dict(policy)
        policy["error"] = "60"
    for value in policy.values():
        _parse_cooldown_seconds(value, strict=True)
    return normalize_cooldown_policy(policy)


def seconds_until_today_end() -> int:
    now = datetime.now()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((tomorrow - now).total_seconds()))


def seconds_until_week_end() -> int:
    """到本周日 23:59:59 的秒数（周一为一周起点）。"""
    now = datetime.now()
    # weekday(): 周一=0 ... 周日=6；本周日 = 今天 + (6 - weekday) 天
    days_to_sunday = 6 - now.weekday()
    sunday_end = (now + timedelta(days=days_to_sunday)).replace(hour=23, minute=59, second=59, microsecond=0)
    return max(1, int((sunday_end - now).total_seconds()))


def seconds_until_month_end() -> int:
    """到当月最后一天 23:59:59 的秒数。"""
    now = datetime.now()
    # 下月 1 号 0 点 减 1 秒 = 本月最后一天 23:59:59
    if now.month == 12:
        next_month_first = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month_first = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = next_month_first - timedelta(seconds=1)
    return max(1, int((month_end - now).total_seconds()))


# 冻结条件类型
FREEZE_CONDITION_TYPES = ("status_code", "headers", "exception", "error_type", "body")
# 比较运算符
#   in       "包括"：value 为逗号分隔的多值列表，actual 命中其中任意一个即为真（默认运算符，状态码可填 501,502 等）
#   contains "包含"：actual 字符串包含 expected 子串（用于 error.message 文本匹配）
FREEZE_OPERATORS = ("in", "==", "!=", "<=", ">=", "<", ">", "exists", "contains")

# ── 冻结模型：对象 × 周期 二维正交 ──
# 冻结对象（freeze_object）：冻结的作用范围
#   account        账号：冻结命中的单个账号
#   account_model  账号模型：冻结命中账号的对应模型（不冻结账号本身的其他模型）
#   channel        渠道：冻结本渠道所有账号
#   channel_model  渠道模型：冻结本渠道所有账号的该模型（不冻结账号本身）
# 冻结周期（freeze_period）：冻结多久
#   none      不冻结：命中规则后跳过冻结（兼容旧 no_freeze 语义）
#   disabled  禁用：账号 switch 关闭 / 渠道 enabled 关闭（仅 account/channel，不进入定时检测）
#   permanent 永久冻结：置账号 is_frozen=True（仅 account，可被定时检测成功解冻）
#   seconds   秒：冻结 freeze_value 秒
#   minutes   分钟：冻结 freeze_value 分钟
#   hours     小时：冻结 freeze_value 小时
#   days      天：冻结 freeze_value 天
#   today     今天：冻结到当天结束（含自然边界缓冲）
#   week      本周：冻结到本周日结束（含自然边界缓冲）
#   month     本月：冻结到本月最后一天结束（含自然边界缓冲）
# freeze_value：分钟/小时/天/秒 周期填正整数数值；其余周期填 0（忽略）
# enabled：单条规则开关（缺省视为开）。关掉的规则原样保留配置，只是匹配时整条跳过，
# 便于临时停用某条规则而不必删掉再重建。与策略级 enabled 是两层：策略关＝全部不生效。
FREEZE_OBJECTS = ("account", "account_model", "channel", "channel_model")
FREEZE_PERIODS = ("none", "disabled", "permanent", "seconds", "minutes", "hours", "days", "today", "week", "month")
# 需要 freeze_value 的周期
FREEZE_PERIODS_WITH_VALUE = ("seconds", "minutes", "hours", "days")
# disabled（禁用）仅对账号/渠道对象有效：账号 switch off / 渠道停用，非冻结，不被探测解冻。
FREEZE_OBJECTS_SUPPORT_DISABLED = ("account", "channel")
# permanent（永久冻结）仅对账号有效：置 is_frozen=True，是冻结，可被定时检测成功解冻。
# 渠道没有永久冻结（渠道级停摆走 disabled）。
FREEZE_OBJECTS_SUPPORT_PERMANENT = ("account",)

# 自然边界周期：到当天/本周日/月末结束的冻结，在接近边界时触发会很快解冻，
# 解冻后又立刻被请求、再次触发、又只冻到边界结束，反复横跳。
# 给这类冻结额外加一段缓冲，跨过自然边界再多冻一会儿。
FREEZE_PERIODS_NATURAL_BOUNDARY = ("today", "week", "month")

# 旧版永久冻结曾用约 10 年 TTL 模拟（已废弃：永久冻结改走 is_frozen 状态位）。
# 保留常量仅供读取侧兼容旧数据，新代码不应再用。
PERMANENT_FREEZE_SECONDS = 10 * 365 * 24 * 3600

# 自然边界冻结（daily/weekly/monthly）到当天/本周日/月末 23:59:59 结束的，
# 在接近边界时触发会很快解冻（如 23:58 触发 daily 只剩 2 分钟），
# 解冻后又立刻被请求、再次触发、又只冻到边界结束，反复横跳。
# 给这类冻结额外加一段缓冲，跨过自然边界再多冻一会儿。
NATURAL_BOUNDARY_FREEZE_BUFFER_SECONDS = 30 * 60


def _to_number(value):
    """尽量把值转成数字以便比较；失败返回 None。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        text = str(value).strip()
    except Exception:
        return None
    if text == "":
        return None
    try:
        if "." in text or "e" in text or "E" in text:
            return float(text)
        return int(text)
    except (TypeError, ValueError):
        try:
            return float(text)
        except (TypeError, ValueError):
            return None


def _compare(actual, operator: str, expected) -> bool:
    """按 operator 比较 actual 与 expected。

    - exists：只判断 actual 是否存在（非 None）。
    - in（包括）：expected 为逗号分隔的多值列表，actual 命中其中任意一个即为真。
      数值优先按数字比较，退化时按字符串比较，便于状态码填写 501,502 这类多值。
    - 其余数值优先按数字比较，否则退化为字符串比较（仅 == / != 有意义）。
    """
    op = str(operator or "").strip()
    if op == "exists":
        return actual is not None
    if actual is None:
        return False

    if op == "contains":
        return str(expected or "") in str(actual)

    if op == "in":
        actual_num = _to_number(actual)
        actual_s = str(actual).strip()
        for part in str(expected or "").split(","):
            part = part.strip()
            if part == "":
                continue
            part_num = _to_number(part)
            if actual_num is not None and part_num is not None:
                if actual_num == part_num:
                    return True
            elif actual_s == part:
                return True
        return False

    actual_num = _to_number(actual)
    expected_num = _to_number(expected)
    if actual_num is not None and expected_num is not None:
        if op == "==":
            return actual_num == expected_num
        if op == "!=":
            return actual_num != expected_num
        if op == "<=":
            return actual_num <= expected_num
        if op == ">=":
            return actual_num >= expected_num
        if op == "<":
            return actual_num < expected_num
        if op == ">":
            return actual_num > expected_num
        return False

    # 非数值：仅支持相等/不等的字符串比较
    actual_s = str(actual).strip()
    expected_s = str(expected).strip()
    if op == "==":
        return actual_s == expected_s
    if op == "!=":
        return actual_s != expected_s
    return False


def normalize_freeze_rule(rule: dict | None) -> dict | None:
    """规范化单条冻结规则；非法返回 None。"""
    if not isinstance(rule, dict):
        return None
    condition = str(rule.get("condition") or "").strip().lower()
    if condition not in FREEZE_CONDITION_TYPES:
        return None

    key = str(rule.get("key") or "").strip()
    # status_code / error_type 条件不需要 key（默认比较状态码/错误码本身）；
    # headers / body 条件必须有 key（headers 是响应头名，body 是响应体点分路径如 error.code）。
    if condition in ("headers", "body") and not key:
        return None
    if condition in ("status_code", "exception", "error_type"):
        key = ""

    operator = str(rule.get("operator") or "").strip()
    if condition == "exception":
        operator = ""
    elif operator not in FREEZE_OPERATORS:
        return None

    value = rule.get("value")
    if condition == "exception":
        value = ""
    elif operator != "exists":
        if value is None:
            return None
        value = str(value).strip()
        if value == "":
            return None
        if operator == "in":
            # "包括"：value 为逗号分隔多值，规范化为去空白、去空项、去重后的列表。
            parts = [p.strip() for p in value.split(",")]
            parts = list(dict.fromkeys(p for p in parts if p))
            if not parts:
                return None
            value = ",".join(parts)
    else:
        value = ""

    # ── 冻结对象/周期/数值（新二维模型）──
    freeze_object = str(rule.get("freeze_object") or "").strip().lower()
    freeze_period = str(rule.get("freeze_period") or "").strip().lower()
    freeze_value = _int_value(rule.get("freeze_value"), 0)

    if freeze_object not in FREEZE_OBJECTS:
        return None
    if freeze_period not in FREEZE_PERIODS:
        return None
    # disabled 仅对账号/渠道有效；模型类对象不支持禁用
    if freeze_period == "disabled" and freeze_object not in FREEZE_OBJECTS_SUPPORT_DISABLED:
        return None
    # permanent（永久冻结）仅对账号有效；渠道没有永久冻结
    if freeze_period == "permanent" and freeze_object not in FREEZE_OBJECTS_SUPPORT_PERMANENT:
        return None
    # 需要数值的周期必须填正整数
    if freeze_period in FREEZE_PERIODS_WITH_VALUE and freeze_value <= 0:
        return None
    # none / disabled / today / week / month 周期不消费数值，统一归零
    if freeze_period not in FREEZE_PERIODS_WITH_VALUE:
        freeze_value = 0

    return {
        "condition": condition,
        "key": key,
        "operator": operator,
        "value": value,
        "freeze_object": freeze_object,
        "freeze_period": freeze_period,
        "freeze_value": freeze_value,
        # 缺省为开：历史规则没有这个字段，不能因为补字段就把它们悄悄停掉。
        "enabled": rule.get("enabled", True) is not False,
    }


def normalize_freeze_policy(policy: dict | None) -> dict:
    """规范化冻结策略；丢弃非法规则。

    refresh_freeze_on_failure 是渠道级开关（默认开）：失败命中冻结规则时，已冻结对象是否按本次规则
    重新设置 TTL；关时已冻结对象保留现有到期时间，未冻结对象仍正常冻结。所有请求模式共用，
    不由定时检测或测试链路单独透传。随 freeze_policy 一起存取。
    """
    raw = policy if isinstance(policy, dict) else {}
    rules_in = raw.get("rules")
    rules_out: list[dict] = []
    if isinstance(rules_in, list):
        for rule in rules_in:
            normalized = normalize_freeze_rule(rule)
            if normalized is not None:
                rules_out.append(normalized)
    return {
        "enabled": bool(raw.get("enabled", False)),
        "refresh_freeze_on_failure": bool(raw.get("refresh_freeze_on_failure", True)),
        "rules": rules_out,
    }


def validate_freeze_policy(policy: dict | None) -> dict:
    """校验冻结策略；非法直接抛 ValueError。"""
    if policy is None:
        return default_freeze_policy()
    if not isinstance(policy, dict):
        raise ValueError("冻结策略必须是 JSON 对象")
    rules_in = policy.get("rules")
    if rules_in is not None and not isinstance(rules_in, list):
        raise ValueError("冻结策略 rules 必须是数组")
    for idx, rule in enumerate(rules_in or []):
        if not isinstance(rule, dict):
            raise ValueError(f"第 {idx + 1} 条冻结规则必须是 JSON 对象")
        condition = str(rule.get("condition") or "").strip().lower()
        if condition not in FREEZE_CONDITION_TYPES:
            raise ValueError(f"第 {idx + 1} 条冻结规则 condition 非法（只能是 status_code/headers/exception/error_type）")
        if condition == "exception":
            pass
        else:
            operator = str(rule.get("operator") or "").strip()
            if operator not in FREEZE_OPERATORS:
                raise ValueError(f"第 {idx + 1} 条冻结规则 operator 非法")
            if condition in ("headers", "body") and not str(rule.get("key") or "").strip():
                raise ValueError(f"第 {idx + 1} 条冻结规则 {condition} 条件必须填写 key")
            if operator != "exists" and (rule.get("value") is None or str(rule.get("value")).strip() == ""):
                raise ValueError(f"第 {idx + 1} 条冻结规则缺少 value")
            if operator == "in" and not [p for p in str(rule.get("value") or "").split(",") if p.strip()]:
                raise ValueError(f"第 {idx + 1} 条冻结规则「包括」运算至少需要一个匹配值")

        freeze_object = str(rule.get("freeze_object") or "").strip().lower()
        freeze_period = str(rule.get("freeze_period") or "").strip().lower()
        freeze_value = _int_value(rule.get("freeze_value"), 0)
        if freeze_object not in FREEZE_OBJECTS:
            raise ValueError(f"第 {idx + 1} 条冻结规则 freeze_object 非法（只能是 account/account_model/channel/channel_model）")
        if freeze_period not in FREEZE_PERIODS:
            raise ValueError(f"第 {idx + 1} 条冻结规则 freeze_period 非法")
        if freeze_period == "disabled" and freeze_object not in FREEZE_OBJECTS_SUPPORT_DISABLED:
            raise ValueError(f"第 {idx + 1} 条冻结规则 禁用仅对账号/渠道有效，模型类对象不支持禁用")
        if freeze_period == "permanent" and freeze_object not in FREEZE_OBJECTS_SUPPORT_PERMANENT:
            raise ValueError(f"第 {idx + 1} 条冻结规则 永久冻结仅对账号有效")
        if freeze_period in FREEZE_PERIODS_WITH_VALUE and freeze_value <= 0:
            raise ValueError(f"第 {idx + 1} 条冻结规则 {freeze_period} 必须填写正整数 freeze_value")
    return normalize_freeze_policy(policy)


def _period_to_seconds(freeze_period: str, freeze_value: int = 0) -> int:
    """把冻结周期翻译为冷却秒数。仅处理数值/自然边界周期；none/disabled 由调用方单独判定。"""
    period = str(freeze_period or "").strip().lower()
    if period == "seconds":
        return max(1, int(freeze_value or 0))
    if period == "minutes":
        return max(1, int(freeze_value or 0) * 60)
    if period == "hours":
        return max(1, int(freeze_value or 0) * 3600)
    if period == "days":
        return max(1, int(freeze_value or 0) * 86400)
    if period == "today":
        return seconds_until_today_end() + NATURAL_BOUNDARY_FREEZE_BUFFER_SECONDS
    if period == "week":
        return seconds_until_week_end() + NATURAL_BOUNDARY_FREEZE_BUFFER_SECONDS
    if period == "month":
        return seconds_until_month_end() + NATURAL_BOUNDARY_FREEZE_BUFFER_SECONDS
    return 0


def freeze_rule_to_action(freeze_object: str, freeze_period: str, freeze_value: int = 0) -> dict:
    """把新二维模型（对象+周期）翻译为冷却动作。

    返回:
      scope: "account" | "account_model" | "channel" | "channel_model"
      seconds: 临时冻结的实际冷却秒数；禁用/永久冻结/不冻结均为 0（不走 TTL）
      permanent: 是否为永久冻结（is_frozen 状态位，可被定时检测解冻）
      disable: 是否为禁用（账号 switch off / 渠道 enabled off，非冻结，不被探测解冻）
      freeze_mode: 供读取侧 reason 展示用的兼容字符串
    """
    obj = str(freeze_object or "").strip().lower()
    period = str(freeze_period or "").strip().lower()
    scope = obj if obj in FREEZE_OBJECTS else "account"
    if period == "none":
        return {"scope": scope, "seconds": 0, "permanent": False, "disable": False, "freeze_mode": "no_freeze"}
    if period == "disabled":
        # 禁用：账号 switch off / 渠道 enabled off。是禁用不是冻结，不写 TTL、不写 is_frozen，
        # 不被定时检测解冻（禁用账号永不进入检测候选）。需人工启用。
        return {"scope": scope, "seconds": 0, "permanent": False, "disable": True, "freeze_mode": f"{obj}_disabled"}
    if period == "permanent":
        # 永久冻结：仅账号，置 is_frozen=True。是冻结，可被定时检测成功解冻。
        return {"scope": "account", "seconds": 0, "permanent": True, "disable": False, "freeze_mode": "account_permanent"}
    seconds = _period_to_seconds(period, freeze_value)
    return {"scope": scope, "seconds": seconds, "permanent": False, "disable": False, "freeze_mode": f"{obj}_{period}"}


def _resolve_body_field(body, key: str):
    """从上游响应体按点分路径取字段值。

    body 支持已解析的 dict 或 JSON 字符串（上游错误响应体文本）；
    key 为点分路径，如 ``error.code`` / ``error.message`` / ``request_id``。
    解析失败或路径不存在时返回 None。
    """
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    cur = body
    for part in str(key or "").split("."):
        part = part.strip()
        if not part:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def match_freeze_rules(
    policy: dict | None,
    *,
    status_code: int | None = None,
    headers: dict | None = None,
    exception: bool = False,
    error_code: str | None = None,
    body=None,
) -> dict | None:
    """根据响应上下文匹配冻结规则，返回第一条命中的冻结动作。

    返回 None 表示无命中。命中返回:
      {
        "rule": <规范化后的规则>,
        "scope": "account" | "account_model",
        "seconds": int,
        "permanent": bool,
        "disable": bool,
        "freeze_mode": str,
      }
    """
    normalized = normalize_freeze_policy(policy)
    if not normalized.get("enabled"):
        return None
    rules = normalized.get("rules") or []
    if not rules:
        return None

    # headers 统一成小写 key 便于匹配
    header_map: dict[str, str] = {}
    if isinstance(headers, dict):
        for hk, hv in headers.items():
            header_map[str(hk or "").strip().lower()] = hv

    for rule in rules:
        # 单条规则关掉时整条跳过，后面的规则继续匹配（含 none 的终止语义也一并失效）。
        if not rule.get("enabled", True):
            continue
        condition = rule["condition"]
        operator = rule["operator"]
        if condition == "status_code":
            if status_code is None:
                continue
            actual = status_code
        elif condition == "headers":
            actual = header_map.get(rule["key"].strip().lower())
        elif condition == "body":
            actual = _resolve_body_field(body, rule["key"])
            if actual is None and operator != "exists":
                continue
        elif condition == "error_type":
            if not error_code:
                continue
            actual = error_code
        else:  # exception
            if not exception:
                continue
            action = freeze_rule_to_action(rule["freeze_object"], rule["freeze_period"], rule["freeze_value"])
            return {
                "rule": rule,
                "scope": action["scope"],
                "seconds": action["seconds"],
                "permanent": action["permanent"],
                "disable": action["disable"],
                "freeze_mode": action["freeze_mode"],
            }
            continue
        if not _compare(actual, operator, rule["value"]):
            continue
        action = freeze_rule_to_action(rule["freeze_object"], rule["freeze_period"], rule["freeze_value"])
        return {
            "rule": rule,
            "scope": action["scope"],
            "seconds": action["seconds"],
            "permanent": action["permanent"],
            "disable": action["disable"],
            "freeze_mode": action["freeze_mode"],
        }
    return None


def match_cooldown(status_code: int | None, policy: dict | None) -> dict:
    normalized = normalize_cooldown_policy(policy)
    matched: list[int] = []
    if status_code is not None:
        code_s = str(status_code)
        for key, value in normalized.items():
            if key == "error":
                continue
            if code_s in {part.strip() for part in key.split(",")}:
                matched.append(_int_value(value, 0))
    source = "status" if matched else "error"
    if -2 in matched:
        raw_seconds = -2
    elif -1 in matched:
        raw_seconds = -1
    else:
        raw_seconds = max(matched) if matched else _int_value(normalized.get("error"), 60)
    disable_account = raw_seconds == -2
    seconds = 0 if disable_account else (seconds_until_today_end() if raw_seconds == -1 else max(0, raw_seconds))
    return {"seconds": seconds, "raw_seconds": raw_seconds, "source": source, "policy": normalized, "disable_account": disable_account}


def policy_fields_from_rate_limit(rate_limit: dict | None) -> dict:
    """从渠道 rate_limit 派生限流/冷却字段（与 _legacy_effective_policy 同口径）。

    创建渠道种子 provider_limit_policies 行时复用，保证显式策略行不因屏蔽
    _legacy_effective_policy 而把用户创建时填的限流静默清零。
    """
    rl = rate_limit if isinstance(rate_limit, dict) else {}
    cooldown_policy = {
        "429": str(_int_value(rl.get("cooldown_seconds"), 60)),
        "error": str(_int_value(rl.get("exception_cooldown_seconds"), 60)),
    }
    for code in rl.get("status_codes") or []:
        try:
            cooldown_policy[str(int(code))] = str(_int_value(rl.get("cooldown_seconds"), 60))
        except (TypeError, ValueError):
            continue
    return {
        "account_rpm": _int_value(rl.get("rpm_per_account", rl.get("requests_per_minute_per_account", 0)), 0),
        "account_tpm": _int_value(rl.get("tpm_per_account"), 0),
        "model_tpm": _int_value(rl.get("tpm_per_model"), 0),
        "account_concurrent": _int_value(rl.get("concurrent_per_account"), 0),
        "account_rph": 0,
        "account_tph": 0,
        "account_rpd": _int_value(rl.get("rpd_per_account"), 0),
        "account_tpd": 0,
        "cooldown_policy": normalize_cooldown_policy(cooldown_policy),
    }


async def _legacy_effective_policy(provider_name: str) -> dict:
    provider_config = (await config.Config.get_providers()).get(provider_name, {})
    derived = policy_fields_from_rate_limit(provider_config.get("rate_limit") or {})
    return {
        "provider_name": provider_name,
        "name": "default",
        "enabled": True,
        **derived,
        "freeze_policy": default_freeze_policy(),
        "extra": {},
    }


_policy_cache: dict[str, tuple[float, dict]] = {}


async def get_effective_provider_policy(provider_name: str, *, refresh: bool = False) -> dict:
    now = time.time()
    cached = _policy_cache.get(provider_name)
    if cached and not refresh and now - cached[0] < 30:
        return dict(cached[1])
    try:
        policy = await PostgresClient.get_provider_limit_policy(provider_name)
    except Exception:
        policy = None
    if not policy:
        policy = await _legacy_effective_policy(provider_name)
    else:
        policy = dict(policy)
        policy["cooldown_policy"] = normalize_cooldown_policy(policy.get("cooldown_policy"))
        policy["freeze_policy"] = normalize_freeze_policy(policy.get("freeze_policy"))
        if policy.get("enabled") is False:
            policy["account_rpm"] = 0
            policy["account_tpm"] = 0
            policy["model_tpm"] = 0
            policy["account_concurrent"] = 0
            policy["account_rph"] = 0
            policy["account_tph"] = 0
            policy["account_rpd"] = 0
            policy["account_tpd"] = 0
    _policy_cache[provider_name] = (now, policy)
    return dict(policy)


def get_effective_provider_policy_sync(provider_name: str) -> dict:
    cached = _policy_cache.get(provider_name)
    if cached:
        return dict(cached[1])
    return {
        "provider_name": provider_name,
        "name": "default",
        "enabled": True,
        "account_rpm": 0,
        "account_tpm": 0,
        "model_tpm": 0,
        "account_concurrent": 0,
        "account_rph": 0,
        "account_tph": 0,
        "account_rpd": 0,
        "account_tpd": 0,
        "cooldown_policy": normalize_cooldown_policy(DEFAULT_COOLDOWN_POLICY),
        "freeze_policy": default_freeze_policy(),
        "extra": {},
    }


def clear_policy_cache(provider_name: str | None = None):
    if provider_name:
        _policy_cache.pop(provider_name, None)
    else:
        _policy_cache.clear()
