"""配置管理模块"""
import os
import secrets
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any, Optional
from bootstrap_config import get_redis_config
from config_store import PostgresConfigStore, RedisConfigStore
from db import PostgresClient
from model_catalog import current_snapshot


CONFIG_STORE = RedisConfigStore(PostgresConfigStore())
_api_key_request_config: ContextVar[dict | None] = ContextVar("api_key_request_config", default=None)

# 异常输出拦截编译后的正则缓存：键是规则集的 JSON 序列化，配置 reload 后键变化、旧缓存自然失效。
_OUTPUT_INTERCEPTION_CACHE: tuple[str, list] | None = None

SELECTION_STRATEGIES = ("sequential", "random_member", "model_random", "random_all", "intelligent", "fast_intelligent")
DEFAULT_SELECTION_STRATEGY = "intelligent"
# 可配置的「不可重试上游错误」默认值。上下文/token 超限已从这里抽走，改由 retry_policy
# 的硬编码归一器（canonicalize_upstream_error）统一识别+归一，受开关
# context_overflow_not_retryable_enabled 控制；这里只保留与超限无关的通用参数错标志。
DEFAULT_NON_RETRYABLE_PARAMETER_ERRORS = {
    "status_codes": [],
    "types": [],
    "codes": [],
    "params": [],
    "markers": [
        "invalid value",
    ],
}

# 「上下文超限不重试」策略默认开启。开启时：命中超限特征即判为不可重试、直接毙掉，
# 并给下游返回归一后的标准 context_length_exceeded 报错（供 Claude Code 等自动压缩重发）。
# 关闭时：既不做超限判定、也不做归一返回，退回普通错误流程（能重试则重试，否则原样透传）。
DEFAULT_CONTEXT_OVERFLOW_NOT_RETRYABLE_ENABLED = True

# 「允许 token 预占触线放行」默认开启。渠道账号的 token（TPM/TPH/TPD）与模型 TPM 采用
# 预占式计量：请求开始按估算预占，成功用真实 usage 补差，失败原子回滚。
# 开启时：预占算出的 projected 超过限额，本次请求仍放行（并写冻结挡住后续请求），
#         与请求数（RPM/RPH/RPD）现有的「触线放行 + 冻结后续」口径一致。
# 关闭时：预占触线直接拒绝该候选并换下一个账号，能更严格地卡住超额，但由于预占量是
#         估算值，估高会误拒本可正常服务的请求。
# 该开关只作用于 token 维度；请求数维度始终是「触线放行 + 冻结后续」，不随开关变化。
DEFAULT_ALLOW_TOKEN_RESERVATION_OVERFLOW = True

# 上下文 Token 超限检测默认开启；关闭时不因本地估算直接拒绝请求。
DEFAULT_CONTEXT_TOKEN_DETECTION_ENABLED = True

# Tokenizer 词表版本检查：默认关闭（多数部署不配 huggingface 规则，开着也只是空转）。
# 开启后后台按间隔检查内置 HF 词表是否有新版本，只记录状态、不自动下载。
DEFAULT_TOKENIZER_VOCAB_CHECK_ENABLED = False
DEFAULT_TOKENIZER_VOCAB_MIRROR = "https://hf-mirror.com"

# 「异常输出拦截」默认规则。上游返回 HTTP 200 但响应内容是无意义占位文本（如
# "No response requested."）时，按这些规则匹配；命中即视为本次候选响应无效，
# 走外层换候选重试（与 IncompleteStreamError / EmptyNonStreamResponseError 同语义）。
# 每条规则的 match_type：'regex' 按 Python 正则 search 匹配；'text' 按大小写敏感
# 子串匹配（pattern in content），不对正则元字符做解析。整体开关 enabled 默认关——
# 需管理员在「渠道配置」里显式开启后才生效，避免误伤。
DEFAULT_OUTPUT_INTERCEPTION_RULES = {
    "enabled": False,
    "rules": [
        {"name": "No response requested", "enabled": True, "match_type": "regex", "pattern": r"No response requested\.?"},
    ],
}

# ==================== 账号测试类型默认值 ====================
# 账号测试的「测试类型」现在可由管理员在渠道配置弹框里维护。每个类型的粒度：
#   key       : 唯一标识（前端下拉 value、后端分派 key）
#   label     : 中文显示名
#   operation : 媒体类型的操作名（image / video / tts_generation）；对话类为 None
#   messages  : 对话消息数组（媒体类型为空）
#   body      : 附加到请求体的字段（不含 model/messages；stream 由 body.stream 或 key=="stream" 决定）
# 下面 8 项是历史硬编码类型落成的默认值（openai 形态），配置缺失时回退到它们。
# 注意：管理员自定义/覆盖某类型后，该类型走「扁平 messages+body」，不再按目标协议自动
# 改写 tools/thinking 形态；未在配置里出现的 key 仍回退 _build_test_body 的协议感知逻辑。
_TEST_TOOL_PROMPT = "What's the weather in Beijing? Use the get_weather tool."
_TEST_THINKING_PROMPT = "Think step by step: prove 17 is prime."
_TEST_TOOL_OPENAI = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string", "description": "City name"}},
            "required": ["city"],
        },
    },
}]
_TEST_MULTI_MESSAGES = [
    {"role": "user", "content": "Hi"},
    {"role": "assistant", "content": "Hello! How can I help?"},
    {"role": "user", "content": "Say bye."},
]

DEFAULT_TEST_TYPES = [
    {"key": "chat", "label": "聊天", "operation": None,
     "messages": [{"role": "user", "content": "hi"}], "body": {}},
    {"key": "stream", "label": "流式", "operation": None,
     "messages": [{"role": "user", "content": "hi"}], "body": {"stream": True}},
    {"key": "tool", "label": "工具", "operation": None,
     "messages": [{"role": "user", "content": _TEST_TOOL_PROMPT}],
     "body": {"tools": _TEST_TOOL_OPENAI, "tool_choice": "auto"}},
    {"key": "thinking", "label": "思考", "operation": None,
     "messages": [{"role": "user", "content": _TEST_THINKING_PROMPT}],
     "body": {"reasoning_effort": "medium"}},
    {"key": "multi", "label": "多messages", "operation": None,
     "messages": list(_TEST_MULTI_MESSAGES), "body": {}},
    {"key": "image", "label": "图片生成", "operation": "image",
     "messages": [],
     "body": {"prompt": "A simple test image of a red apple on a desk.", "n": 1, "size": "1024x1024"}},
    {"key": "video", "label": "视频生成", "operation": "video",
     "messages": [],
     "body": {"prompt": "A short test video of ocean waves."}},
    {"key": "tts", "label": "语音合成", "operation": "tts_generation",
     "messages": [],
     "body": {"input": "Hello, this is an account test.", "voice": "alloy", "response_format": "mp3"}},
]


def _catalog_field(snapshot: Any, *names: str) -> Mapping:
    for name in names:
        value = getattr(snapshot, name, None)
        if isinstance(value, Mapping):
            return value
    return {}


def _mutable_catalog_value(value: Any) -> Any:
    """Return legacy-compatible mutable containers from a frozen snapshot."""
    if isinstance(value, Mapping):
        return {key: _mutable_catalog_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable_catalog_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return {_mutable_catalog_value(item) for item in value}
    return value


class Config:
    """配置管理"""

    # API Key 内存快照：启动灌一次，写路径 refresh_api_keys_cache() 重灌，
    # 60s 对账兜底。校验走内存，不在请求链路上查库。
    # 两个索引服务鉴权链路的两次查找：_api_keys_by_value 是 key 明文 → 行（请求
    # 带的是明文），_api_keys_by_id 是 id → 行（子 Key 回查父 Key 用 parent_id）。
    _api_keys_cache: list[dict] = []
    _api_keys_by_value: dict[str, dict] = {}
    _api_keys_by_id: dict[int, dict] = {}
    _api_keys_cache_loaded: bool = False

    @classmethod
    def reload(cls) -> dict:
        """兼容管理端写配置后的同步刷新；当前配置存储本身维护缓存。"""
        if hasattr(CONFIG_STORE, "refresh_cache"):
            try:
                import asyncio
                asyncio.get_running_loop()
            except RuntimeError:
                try:
                    asyncio.run(CONFIG_STORE.refresh_cache())
                except Exception:
                    pass
        return cls._load()

    @classmethod
    async def reload_async(cls) -> dict:
        """兼容管理端写配置后的异步刷新。"""
        if hasattr(CONFIG_STORE, "refresh_cache"):
            await CONFIG_STORE.refresh_cache()
        return await CONFIG_STORE.read_main_async() if hasattr(CONFIG_STORE, "read_main_async") else cls._load()

    @classmethod
    async def persist_account_fields(cls, provider_name: str, username: str, fields: dict) -> bool:
        """窄写单账号字段（供 provider 运行时 token 轮换回写用）。

        按 username 精确匹配到目标账号行，只 UPSERT 该行 + 刷新内存快照 + 广播
        account 事件；绝不整份重写渠道或删空其它账号。找不到该账号返回 False。
        """
        try:
            provider_cfg = await CONFIG_STORE.read_provider_async(provider_name)
        except FileNotFoundError:
            return False
        if not provider_cfg:
            return False
        accounts = provider_cfg.get("accounts") or []
        target = None
        for acc in accounts:
            if acc.get("username") == username:
                target = acc
                break
        if target is None:
            return False
        merged = dict(target)
        merged.update(fields)
        await CONFIG_STORE.upsert_provider_account_async(provider_name, merged)
        try:
            import runtime_sync
            await runtime_sync.publish(
                runtime_sync.EVENT_ACCOUNT,
                f"{provider_name}:{username}",
                extra={"provider": provider_name, "username": username},
            )
        except Exception:
            pass
        return True

    @classmethod
    async def refresh_api_keys_cache(cls, broadcast: bool = True) -> None:
        """从 DB 重灌 API Key 内存快照。

        校验走内存：鉴权是每个请求的必经路径，逐请求查库会把 api_keys 全表扫描
        压在主链路上。写路径（admin 增删改 / editor 派生子 Key / 团队授权）改完
        都会调这里，再由 EVENT_APIKEY 广播给其它进程；60s 对账兜底漏掉的事件。

        广播放在这里而不是各写路径：所有写路径本来就已经调用本函数，在这里发一次
        就覆盖全部，不会漏掉将来新增的写路径。``broadcast=False`` 供订阅端和对账
        回调使用——它们是被广播触发的，再发一次会自激。

        失败时保留旧快照而非清空——清空会让所有 Key 立刻变成「无效 Key」而全量
        401，比拿着略旧的快照继续服务危险得多。
        """
        try:
            keys = await PostgresClient.list_api_keys()
        except Exception as exc:
            from loguru import logger

            logger.warning(f"[config] 刷新 API Key 缓存失败，保留旧快照: {exc}")
            return None
        cls._api_keys_cache = keys
        cls._api_keys_by_value = {
            str(k.get("key") or ""): k for k in keys if k.get("key")
        }
        by_id: dict[int, dict] = {}
        for k in keys:
            try:
                by_id[int(k["id"])] = k
            except (KeyError, TypeError, ValueError):
                continue
        cls._api_keys_by_id = by_id
        cls._api_keys_cache_loaded = True
        if broadcast:
            try:
                import runtime_sync

                # 不带 name 发布：_dispatch 只对 (type, name) 都存在的消息做版本
                # 去重，而快照事件不针对单个实体、各实例时钟也不同步——带 name 会让
                # 时钟略慢的实例发出的失效被当成「旧消息」丢掉。重灌本身幂等。
                await runtime_sync.publish(
                    runtime_sync.EVENT_APIKEY,
                    extra={"snapshot": True},
                )
            except Exception:
                pass
        return None

    @classmethod
    async def refresh_model_routing_cache(cls) -> None:
        """兼容旧 DB 写路径；运行目录由 admin 写后统一原子重载。"""
        return None

    @classmethod
    def _load(cls) -> dict:
        return CONFIG_STORE.read_main()

    # ==================== API Key ====================
    @classmethod
    def api_keys_enabled(cls) -> bool:
        return cls._load().get("api_keys", {}).get("enabled", False)

    @classmethod
    def set_api_key_request_config(cls, key_config: dict | None) -> None:
        _api_key_request_config.set(key_config)

    @classmethod
    def clear_api_key_request_config(cls) -> None:
        _api_key_request_config.set(None)

    @classmethod
    def clear_model_group_request_cache(cls) -> None:
        """Compatibility no-op: catalog snapshots need no request cache."""
        return None

    @classmethod
    async def _ensure_api_keys_cache(cls) -> None:
        """首次访问时惰性灌一次，之后由写路径/对账维护。

        正常启动 lifespan 会显式调 refresh_api_keys_cache()；这里只兜底「缓存
        从未加载」的边角场景（如单测直接调用校验、启动顺序异常），避免第一次
        请求因为快照空而误判所有 Key 无效。
        """
        if cls._api_keys_cache_loaded:
            return
        await cls.refresh_api_keys_cache()

    @classmethod
    async def get_api_key_config(cls, api_key: str, include_disabled: bool = False) -> Optional[dict]:
        if not api_key:
            return None
        # 请求级 ContextVar 优先：同一请求内校验一次后复用，避免重复比对整表。
        cached = _api_key_request_config.get()
        if cached is not None:
            cached_key = cached.get("key") or ""
            if secrets.compare_digest(str(cached_key).encode("utf-8"), str(api_key).encode("utf-8")):
                return cached if include_disabled or not cached.get("disabled") else None
        # 内存快照查找：不在请求链路上查库。快照由启动/写路径/对账维护。
        # 先用索引定位候选行，再用 compare_digest 确认，保持定时安全的比较语义。
        await cls._ensure_api_keys_cache()
        k = cls._api_keys_by_value.get(str(api_key))
        if k is None:
            return None
        if not secrets.compare_digest(
            str(k.get("key") or "").encode("utf-8"), str(api_key).encode("utf-8")
        ):
            return None
        if not include_disabled and k.get("disabled"):
            return None
        return k

    @classmethod
    async def get_api_key_by_id(cls, api_key_id: int) -> Optional[dict]:
        """按 id 从内存快照取 Key 行（含被禁用的行，由调用方判断）。

        子 Key 请求要回查父 Key 的 disabled/expires_at，这条查找和明文查找一样在
        每个请求链路上，同样不该落到 DB。返回的行是 list_api_keys 的完整行，字段
        是 PostgresClient.get_api_key_by_id 所选列的超集。
        """
        try:
            key_id = int(api_key_id)
        except (TypeError, ValueError):
            return None
        await cls._ensure_api_keys_cache()
        return cls._api_keys_by_id.get(key_id)

    @classmethod
    async def get_api_key_rate_limit(cls, api_key: str) -> dict:
        key_config = await cls.get_api_key_config(api_key)
        if key_config:
            rate_limit = key_config.get("rate_limit") or {}
            return rate_limit if isinstance(rate_limit, dict) else {}
        return {"requests_per_minute": 60, "requests_per_day": 1000}

    @classmethod
    async def get_api_key_provider_filter(cls, api_key: str | None) -> tuple[set[str], set[str]]:
        """返回 (渠道标签白名单, 渠道标签黑名单)，空集合表示不限制。"""
        if not api_key:
            return set(), set()
        key_config = await cls.get_api_key_config(api_key)
        if not key_config:
            return set(), set()
        whitelist = {p for p in (key_config.get("provider_whitelist") or []) if isinstance(p, str) and p}
        blacklist = {p for p in (key_config.get("provider_blacklist") or []) if isinstance(p, str) and p}
        return whitelist, blacklist

    @classmethod
    async def api_key_allows_provider(cls, api_key: str | None, provider: str) -> bool:
        whitelist, blacklist = await cls.get_api_key_provider_filter(api_key)
        provider_config = (await cls.get_providers()).get(provider) or {}
        tags = {
            tag.strip() for tag in (provider_config.get("tags") or [])
            if isinstance(tag, str) and tag.strip()
        }
        if whitelist and not tags.intersection(whitelist):
            return False
        if blacklist and tags.intersection(blacklist):
            return False
        return True

    # ==================== Model Routing ====================
    @classmethod
    async def get_model_groups(cls, *, snapshot: Any = None) -> dict:
        snapshot = snapshot or current_snapshot()
        return _mutable_catalog_value(_catalog_field(snapshot, "groups"))

    @classmethod
    async def get_enabled_model_groups(cls, *, snapshot: Any = None) -> dict:
        snapshot = snapshot or current_snapshot()
        enabled = _catalog_field(snapshot, "enabled_groups")
        if enabled:
            return _mutable_catalog_value(enabled)
        return {
            name: group
            for name, group in (await cls.get_model_groups(snapshot=snapshot)).items()
            if isinstance(group, dict)
            and group.get("enabled", True)
            and isinstance(group.get("models"), list)
            and group.get("models")
        }

    @classmethod
    async def get_model_group_index(cls, *, snapshot: Any = None) -> dict:
        """名字（组主名 + 别名）→ 组配置 的扁平索引。"""
        snapshot = snapshot or current_snapshot()
        index = _catalog_field(snapshot, "group_index")
        if index:
            return _mutable_catalog_value(index)

        built: dict = {}
        for group in (await cls.get_enabled_model_groups(snapshot=snapshot)).values():
            name = group.get("name")
            if isinstance(name, str) and name and name not in built:
                built[name] = group
            for alias in group.get("aliases", []) or []:
                if isinstance(alias, str) and alias and alias not in built:
                    built[alias] = group
        return built

    @classmethod
    async def resolve_model_group(cls, model: str, *, snapshot: Any = None) -> dict | None:
        """按组主名或别名解析出自定义模型；未命中返回 None。"""
        if not model:
            return None
        catalog = snapshot or current_snapshot()
        group = _catalog_field(catalog, "group_index").get(model)
        if group is None:
            if snapshot is None:
                group = (await cls.get_model_group_index()).get(model)
            else:
                group = (await cls.get_model_group_index(snapshot=snapshot)).get(model)
        return _mutable_catalog_value(group) if group is not None else None

    @classmethod
    async def _resolve_model_group_compatible(
        cls, model: str, snapshot: Any
    ) -> dict | None:
        """Pass snapshots through while tolerating legacy monkeypatched callables."""
        try:
            return await cls.resolve_model_group(model, snapshot=snapshot)
        except TypeError as exc:
            message = str(exc)
            if "unexpected keyword argument" not in message or "snapshot" not in message:
                raise
            return await cls.resolve_model_group(model)

    @classmethod
    async def is_model_group(cls, model: str, *, snapshot: Any = None) -> bool:
        return (await cls._resolve_model_group_compatible(model, snapshot)) is not None

    @classmethod
    async def get_model_group_models(cls, model: str, *, snapshot: Any = None) -> list[str]:
        """返回组内成员（真实模型名）。"""
        group = await cls._resolve_model_group_compatible(model, snapshot) or {}
        return [m for m in group.get("models", []) if isinstance(m, str) and m]

    @classmethod
    async def get_model_group_remark(cls, model: str, *, snapshot: Any = None) -> str:
        group = await cls._resolve_model_group_compatible(model, snapshot) or {}
        return str(group.get("remark") or group.get("display_name") or "").strip()

    @classmethod
    async def get_model_group_response_model(cls, model: str, *, snapshot: Any = None) -> str:
        group = await cls._resolve_model_group_compatible(model, snapshot) or {}
        return str(group.get("response_model") or "").strip()

    @classmethod
    async def get_model_group_backup(cls, model: str, *, snapshot: Any = None) -> str:
        """返回主组配置的备份组名（已启用且非自身）。不存在时返回空串。"""
        snapshot = snapshot or current_snapshot()
        group = await cls._resolve_model_group_compatible(model, snapshot)
        if not group:
            return ""
        primary = group.get("name")
        backup = str(group.get("backup_group") or "").strip()
        if not backup or backup == primary:
            return ""
        if not await cls._resolve_model_group_compatible(backup, snapshot):
            return ""
        return backup

    @classmethod
    async def list_model_group_backup_chain(cls, model: str, *, snapshot: Any = None) -> list[str]:
        """按主组 → 备份组 → 备份组的备份组… 展开成有序链。"""
        snapshot = snapshot or current_snapshot()
        chain: list[str] = []
        seen: set[str] = set()
        current = model
        while current:
            group = await cls._resolve_model_group_compatible(current, snapshot)
            if not group:
                break
            primary = group.get("name")
            if primary in seen:
                break
            seen.add(primary)
            chain.append(current)
            backup = str(group.get("backup_group") or "").strip()
            if not backup or backup == primary or backup in seen:
                break
            current = backup
        return chain

    @classmethod
    async def get_model_group_provider_filter(
        cls, model: str, *, snapshot: Any = None
    ) -> tuple[set[str], set[str]]:
        """返回 (渠道标签白名单, 渠道标签黑名单)；空集合表示不限制。"""
        group = await cls._resolve_model_group_compatible(model, snapshot) or {}
        whitelist = {p for p in (group.get("provider_whitelist") or []) if isinstance(p, str) and p}
        blacklist = {p for p in (group.get("provider_blacklist") or []) if isinstance(p, str) and p}
        return whitelist, blacklist

    @classmethod
    async def get_real_model_provider_filter(
        cls, model: str, *, snapshot: Any = None
    ) -> tuple[set[str], set[str]]:
        """返回真实模型行（kind='real'）的渠道标签过滤；非 real 行/未配置返回空集合。

        与模型组过滤同口径（渠道标签白/黑名单），但作用域是按模型选路：请求直连一个
        真实模型时，若该模型行配置了过滤，仅命中标签的渠道可承接。直接读快照 ``groups``
        （real 行天然不进 ``group_index``，见 model_catalog._group_index 的显式排除），
        不建新索引，不与自定义模型组选路互相污染。
        """
        if not model:
            return set(), set()
        catalog = snapshot or current_snapshot()
        row = _catalog_field(catalog, "groups").get(model)
        if not isinstance(row, Mapping) or str(row.get("kind") or "custom") != "real":
            return set(), set()
        whitelist = {p for p in (row.get("provider_whitelist") or []) if isinstance(p, str) and p}
        blacklist = {p for p in (row.get("provider_blacklist") or []) if isinstance(p, str) and p}
        return whitelist, blacklist

    @classmethod
    async def model_group_allows_provider(
        cls, model: str, provider: str, *, snapshot: Any = None
    ) -> bool:
        try:
            whitelist, blacklist = await cls.get_model_group_provider_filter(
                model, snapshot=snapshot
            )
        except TypeError as exc:
            if "unexpected keyword argument 'snapshot'" not in str(exc):
                raise
            whitelist, blacklist = await cls.get_model_group_provider_filter(model)
        provider_config = (await cls.get_providers()).get(provider) or {}
        tags = {
            tag.strip() for tag in (provider_config.get("tags") or [])
            if isinstance(tag, str) and tag.strip()
        }
        if whitelist and not tags.intersection(whitelist):
            return False
        if blacklist and tags.intersection(blacklist):
            return False
        return True

    # ==================== API Key 策略 / 模型访问控制 ====================
    @classmethod
    async def get_api_key_strategy(cls, api_key: str | None) -> str:
        """选择策略已从组/专线收敛到 API Key：返回该 key 的策略，缺省 sequential。"""
        key_config = await cls.get_api_key_config(api_key) if api_key else None
        raw = str((key_config or {}).get("selection_strategy") or DEFAULT_SELECTION_STRATEGY).strip()
        return raw if raw in SELECTION_STRATEGIES else DEFAULT_SELECTION_STRATEGY

    @classmethod
    async def get_api_key_model_filter(cls, api_key: str | None) -> tuple[set[str], set[str]]:
        """返回 (whitelist, blacklist)，空集合表示不限制。匿名/未知 key 返回 (set(), set())。"""
        if not api_key:
            return set(), set()
        key_config = await cls.get_api_key_config(api_key)
        if not key_config:
            return set(), set()
        whitelist = {m for m in (key_config.get("model_whitelist") or []) if isinstance(m, str) and m}
        blacklist = {m for m in (key_config.get("model_blacklist") or []) if isinstance(m, str) and m}
        return whitelist, blacklist

    @classmethod
    async def api_key_allows_model(cls, api_key: str | None, model: str) -> bool:
        """按 API Key 的模型白/黑名单判断是否放行请求的模型名（组主名/别名/真实模型名均按请求名匹配）。"""
        whitelist, blacklist = await cls.get_api_key_model_filter(api_key)
        if whitelist and model not in whitelist:
            return False
        if blacklist and model in blacklist:
            return False
        return True

    @classmethod
    def context_token_detection_enabled(cls) -> bool:
        try:
            value = (cls._load().get("tokenizer") or {}).get("context_detection_enabled")
        except Exception:
            return DEFAULT_CONTEXT_TOKEN_DETECTION_ENABLED
        return value if isinstance(value, bool) else DEFAULT_CONTEXT_TOKEN_DETECTION_ENABLED

    @classmethod
    def get_tokenizer_rules(cls) -> list[dict]:
        rules = cls._load().get("tokenizer", {}).get("rules", [])
        return rules if isinstance(rules, list) else []

    @classmethod
    def get_non_retryable_parameter_errors(cls) -> dict:
        retry = cls._load().get("retry", {}) or {}
        raw = retry.get("non_retryable_parameter_errors")
        if raw is None:
            raw = retry.get("non_retryable_upstream_errors")
        if not isinstance(raw, dict):
            raw = DEFAULT_NON_RETRYABLE_PARAMETER_ERRORS
        defaults = DEFAULT_NON_RETRYABLE_PARAMETER_ERRORS
        return {
            "status_codes": raw.get("status_codes", defaults["status_codes"]),
            "types": raw.get("types", defaults["types"]),
            "codes": raw.get("codes", defaults["codes"]),
            "params": raw.get("params", defaults["params"]),
            "markers": raw.get("markers", defaults["markers"]),
        }

    @classmethod
    def get_output_interception_rules(cls) -> dict:
        """异常输出拦截规则（全局，所有渠道共用一份）。

        返回 {"enabled": bool, "rules": [{"name","enabled","match_type","pattern"}, ...]}。
        配置缺失/非法时回退 DEFAULT_OUTPUT_INTERCEPTION_RULES；逐条校验 name/pattern，
        仅 regex 规则编译失败时跳过并告警，text 规则按字面量保留。
        """
        import logging
        import re
        logger = logging.getLogger(__name__)
        retry = cls._load().get("retry", {}) or {}
        raw = retry.get("output_interception_rules")
        if not isinstance(raw, dict):
            defaults = DEFAULT_OUTPUT_INTERCEPTION_RULES
            return {"enabled": defaults["enabled"], "rules": [dict(r) for r in defaults["rules"]]}
        enabled = raw.get("enabled", DEFAULT_OUTPUT_INTERCEPTION_RULES["enabled"])
        if not isinstance(enabled, bool):
            enabled = DEFAULT_OUTPUT_INTERCEPTION_RULES["enabled"]
        rules_raw = raw.get("rules")
        if not isinstance(rules_raw, list):
            rules_raw = DEFAULT_OUTPUT_INTERCEPTION_RULES["rules"]
        rules: list[dict] = []
        for item in rules_raw:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            pattern = str(item.get("pattern") or "").strip()
            if not name or not pattern:
                continue
            item_enabled = item.get("enabled", True)
            if not isinstance(item_enabled, bool):
                item_enabled = True
            match_type = item.get("match_type", "regex")
            if match_type not in {"regex", "text"}:
                match_type = "regex"
            if match_type == "regex":
                try:
                    re.compile(pattern)
                except re.error as exc:
                    logger.warning(f"异常输出拦截规则「{name}」正则无效，已跳过: {pattern} ({exc})")
                    continue
            rules.append({
                "name": name,
                "enabled": item_enabled,
                "match_type": match_type,
                "pattern": pattern,
            })
        return {"enabled": enabled, "rules": rules}

    @classmethod
    def get_compiled_output_interception_patterns(cls) -> list[tuple[str, str, "re.Pattern | str"]]:
        """准备并缓存启用的拦截规则，返回 (name, match_type, matcher) 列表。

        regex 的 matcher 是编译后的 ``re.Pattern``；text 的 matcher 是原始字面量，
        由调用方以大小写敏感子串匹配。缓存键用规则集的 JSON 序列化——配置写入后
        reload 会产生新键，旧缓存自然失效。整体开关关闭或无启用规则时返回空列表，
        调用方据此零成本跳过匹配。
        """
        global _OUTPUT_INTERCEPTION_CACHE
        import json
        import re
        rules = cls.get_output_interception_rules()
        cache_key = json.dumps(rules, sort_keys=True, ensure_ascii=False)
        cached = _OUTPUT_INTERCEPTION_CACHE
        if cached is not None and cached[0] == cache_key:
            return cached[1]
        compiled: list[tuple[str, str, "re.Pattern | str"]] = []
        if rules.get("enabled"):
            for item in rules.get("rules") or []:
                if not item.get("enabled"):
                    continue
                match_type = item.get("match_type", "regex")
                if match_type == "text":
                    compiled.append((item["name"], "text", item["pattern"]))
                    continue
                try:
                    compiled.append((item["name"], "regex", re.compile(item["pattern"])))
                except re.error:
                    continue
        _OUTPUT_INTERCEPTION_CACHE = (cache_key, compiled)
        return compiled

    @classmethod
    def context_overflow_not_retryable_enabled(cls) -> bool:
        """「上下文超限不重试」开关（默认开）。

        开：命中上下文/token 超限特征即判为不可重试、直接毙掉，并给下游返回归一后的标准
        context_length_exceeded 报错，供客户端自动压缩重发。
        关：不做超限判定、也不做归一返回，退回普通错误流程。
        """
        retry = cls._load().get("retry", {}) or {}
        value = retry.get("context_overflow_not_retryable_enabled")
        if value is None:
            return DEFAULT_CONTEXT_OVERFLOW_NOT_RETRYABLE_ENABLED
        return value is not False

    @classmethod
    def allow_token_reservation_overflow(cls) -> bool:
        """渠道 token 预占触线是否放行本次请求（默认开）。

        开：预占后的 projected 用量达到/超过账号或模型 token 限额时，本次仍放行并冻结后续。
        关：预占触线直接拒绝当前候选，选路器继续尝试其它账号。
        仅作用于渠道账号/模型 token 维度，不影响 API Key 或 RPM/RPH/RPD 口径。
        """
        try:
            rate_limit = cls._load().get("rate_limit", {}) or {}
        except Exception:
            rate_limit = {}
        value = rate_limit.get("allow_token_reservation_overflow")
        if value is None:
            return DEFAULT_ALLOW_TOKEN_RESERVATION_OVERFLOW
        return value is not False

    # ==================== 账号测试类型 ====================
    @classmethod
    def get_test_types(cls) -> list[dict]:
        """账号测试类型列表。配置缺失/非法/存储不可用时回退 DEFAULT_TEST_TYPES；对每项补全字段。"""
        try:
            raw = cls._load().get("account_test", {}) or {}
        except Exception:
            # 配置存储不可用（如未初始化）时不阻断测试构造：回退后端默认类型。
            raw = {}
        items = raw.get("types")
        if not isinstance(items, list) or not items:
            items = DEFAULT_TEST_TYPES
        result: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            if not key:
                continue
            operation = item.get("operation")
            if operation is not None:
                operation = str(operation).strip() or None
            messages = item.get("messages")
            if not isinstance(messages, list):
                messages = []
            body = item.get("body")
            if not isinstance(body, dict):
                body = {}
            result.append({
                "key": key,
                "label": str(item.get("label") or key),
                "operation": operation,
                "messages": messages,
                "body": body,
            })
        return result or [dict(t) for t in DEFAULT_TEST_TYPES]

    # ==================== Reasoning & Thinking ====================
    @classmethod
    def get_reasoning_owned_by(cls) -> list[str]:
        return cls._load().get("reasoning", {}).get("owned_by", ["openai", "deepseek", "google"])

    @classmethod
    def set_reasoning_owned_by(cls, value: list[str]) -> None:
        config = cls._load()
        reasoning = dict(config.get("reasoning", {}))
        reasoning["owned_by"] = value
        config["reasoning"] = reasoning
        CONFIG_STORE.write_main(config)

    @classmethod
    def get_thinking_defaults(cls) -> dict:
        return {
            "thinking_mode": "Fast",
            "thinking_enabled": False,
            "auto_thinking": False,
            "thinking_format": "summary",
            "auto_search": False,
        }

    @classmethod
    def get_reasoning_defaults(cls) -> dict:
        return {
            "reasoning_effort": "xhigh",
            "research_mode": "normal",
        }

    # ==================== 模拟客户端默认配置 ====================
    # 仅在"模拟客户端"（client preset 伪装）场景下补缺省字段：
    # 非模拟请求不注入默认思考等级 / 默认搜索。
    @classmethod
    def get_simulated_client_defaults(cls) -> dict:
        cfg = cls._load().get("simulated_client") or {}
        if not isinstance(cfg, dict):
            cfg = {}
        return {
            "reasoning_effort": cfg.get("reasoning_effort") or cfg.get("thinking_level") or "",
            "auto_search": bool(cfg.get("auto_search", False)),
        }

    @classmethod
    def set_simulated_client_defaults(cls, value: dict | None) -> None:
        config = cls._load()
        current = dict(config.get("simulated_client") or {})
        if isinstance(value, dict):
            if "reasoning_effort" in value:
                current["reasoning_effort"] = str(value.get("reasoning_effort") or "").strip()
            if "thinking_level" in value:
                current["thinking_level"] = str(value.get("thinking_level") or "").strip()
            if "auto_search" in value:
                current["auto_search"] = bool(value.get("auto_search"))
        config["simulated_client"] = current
        CONFIG_STORE.write_main(config)

    @classmethod
    def get_thinking_global_enabled(cls) -> bool:
        return cls._load().get("reasoning", {}).get("thinking_global_enabled", True)

    @classmethod
    def set_thinking_global_enabled(cls, value: bool) -> None:
        config = cls._load()
        reasoning = dict(config.get("reasoning", {}))
        reasoning["thinking_global_enabled"] = value
        config["reasoning"] = reasoning
        CONFIG_STORE.write_main(config)

    # ==================== Providers ====================
    @classmethod
    def get_providers_snapshot(cls) -> dict:
        """返回已灌入的渠道配置内存快照；热路径专用，未就绪时返回空字典。"""
        return getattr(CONFIG_STORE.store, "_providers_cache", None) or {}

    @classmethod
    async def get_providers(cls) -> dict:
        """返回渠道配置内存快照（启动灌 + 写路径/pubsub 维护），不查库。

        内存快照未就绪时（如极早期启动）才回退查库并灌入。强制刷库走
        reload_from_db()（对账循环用）。
        """
        cache = getattr(CONFIG_STORE.store, "_providers_cache", None)
        if cache is not None:
            return cache
        return await CONFIG_STORE.list_providers_async()

    @classmethod
    async def reload_from_db(cls) -> dict:
        """强制从 DB 重灌渠道配置内存快照（对账循环 / 显式刷新用）。"""
        return await CONFIG_STORE.list_providers_async()

    @classmethod
    async def get_provider_accounts(cls, provider: str) -> list[dict]:
        provider_config = (await cls.get_providers()).get(provider)
        if provider_config:
            return provider_config.get("accounts", [])
        return []

    @classmethod
    async def get_provider_rpm_limit(cls, provider: str) -> int:
        provider_config = (await cls.get_providers()).get(provider)
        if provider_config:
            rate_limit = provider_config.get("rate_limit", {})
            return rate_limit.get("rpm_per_account", rate_limit.get("requests_per_minute_per_account", 30))
        return 30

    @classmethod
    def stream_incomplete_error_enabled(cls) -> bool:
        stream = cls._load().get("stream", {}) or {}
        return stream.get("incomplete_error_enabled", True) is not False

    @classmethod
    def get_global_retry_count(cls) -> int:
        """返回外层换路重试次数；与渠道内层原地重试次数相互独立。"""
        try:
            retry_config = cls._load().get("retry", {}) or {}
        except Exception:
            retry_config = {}
        try:
            return max(0, min(10, int(retry_config.get("max_retries", 3) or 0)))
        except (TypeError, ValueError):
            return 3

    @classmethod
    def get_provider_retry_count(cls, provider: str) -> int:
        """返回渠道内层同账号原地重试次数；未设置时不做内层重试。"""
        provider_config = cls.get_providers_snapshot().get(provider) or {}
        raw = provider_config.get("retry_count")
        if raw is None:
            return 0
        try:
            return max(0, min(10, int(raw or 0)))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def get_provider_extra_retry_status_codes(cls, provider: str) -> set[int]:
        """返回渠道额外 HTTP 重试状态码；默认 429/5xx 不在此集合中重复表达。"""
        provider_config = cls.get_providers_snapshot().get(provider) or {}
        raw = provider_config.get("extra_retry_status_codes")
        if isinstance(raw, (int, str)):
            raw = [raw]
        if not isinstance(raw, list):
            return set()
        codes: set[int] = set()
        for item in raw:
            if isinstance(item, bool):
                continue
            try:
                code = int(item)
            except (TypeError, ValueError):
                continue
            if 100 <= code <= 599:
                codes.add(code)
        return codes

    @classmethod
    async def get_provider_check_interval(cls, provider: str) -> int:
        """渠道账号检测间隔（分钟）。custom 渠道取 health_check.interval_minutes，未设置则 30。"""
        provider_config = (await cls.get_providers()).get(provider) or {}
        health = provider_config.get("health_check") or {}
        raw = health.get("interval_minutes")
        try:
            minutes = int(raw) if raw is not None else 30
        except (TypeError, ValueError):
            minutes = 30
        return max(1, minutes)

    @classmethod
    async def get_provider_scheduled_test(cls, provider: str) -> dict:
        """渠道定时测试配置（开关/频率/账号/测试类型等）。未配置返回 {}，调用方按未启用处理。"""
        provider_config = (await cls.get_providers()).get(provider) or {}
        scheduled = provider_config.get("scheduled_test")
        return scheduled if isinstance(scheduled, dict) else {}

    @classmethod
    async def get_rate_limit_status_codes(cls, provider: str | None = None) -> list[int]:
        """限频判定的 HTTP 状态码列表。优先使用渠道级配置，否则回退到全局；默认 [429]。"""
        raw = None
        if provider:
            pcfg = (await cls.get_providers()).get(provider) or {}
            raw = (pcfg.get("rate_limit") or {}).get("status_codes")
            if isinstance(raw, list) and not raw:
                raw = None
        if raw is None:
            raw = cls._load().get("rate_limit", {}).get("status_codes")
        if raw is None:
            return [429]
        if isinstance(raw, (int, str)):
            raw = [raw]
        codes: list[int] = []
        for item in raw:
            try:
                code = int(item)
            except (TypeError, ValueError):
                continue
            if 100 <= code <= 599 and code not in codes:
                codes.append(code)
        return codes or [429]

    @classmethod
    async def get_rate_limit_cooldown(cls, provider: str | None = None) -> int:
        """触发限频后账号冷却秒数。优先渠道级，回退全局，默认 60。"""
        raw = None
        if provider:
            pcfg = (await cls.get_providers()).get(provider) or {}
            raw = (pcfg.get("rate_limit") or {}).get("cooldown_seconds")
        if raw is None:
            raw = cls._load().get("rate_limit", {}).get("cooldown_seconds")
        try:
            seconds = int(raw) if raw is not None else 60
        except (TypeError, ValueError):
            seconds = 60
        return max(0, seconds)

    @classmethod
    async def get_exception_cooldown(cls, provider: str | None = None) -> int:
        """非限频类异常的账号冷却秒数。优先渠道级 rate_limit.exception_cooldown_seconds，回退全局，默认 30。"""
        raw = None
        if provider:
            pcfg = (await cls.get_providers()).get(provider) or {}
            raw = (pcfg.get("rate_limit") or {}).get("exception_cooldown_seconds")
        if raw is None:
            raw = cls._load().get("rate_limit", {}).get("exception_cooldown_seconds")
        try:
            seconds = int(raw) if raw is not None else 30
        except (TypeError, ValueError):
            seconds = 30
        return max(0, seconds)

    @classmethod
    async def is_provider_enabled(cls, provider: str) -> bool:
        provider_config = (await cls.get_providers()).get(provider)
        return provider_config.get("enabled", True) if provider_config else False

    # ==================== Model Refresh ====================
    @classmethod
    def model_refresh_interval(cls) -> int:
        return cls._load().get("model_refresh", {}).get("interval_minutes", 30)

    # ==================== Scheduled Test (定时检测全局配置) ====================
    @classmethod
    def scheduled_test_concurrency(cls) -> int:
        """定时检测的并发度：同时可有多少个探测在途。>=1，兜底 1（串行）。

        全局配置（非渠道级）：一个消费者池给所有渠道共用，防止某渠道账号多时把
        整轮检测拖长、拖垮别的渠道的到点检测。配置不可读（如 PG 未就绪）时兜底默认值，
        绝不让定时检测循环因读配置失败而起不来。
        """
        try:
            value = int(cls._load().get("scheduled_test", {}).get("concurrency", 3))
        except Exception:
            value = 3
        return max(1, value)

    @classmethod
    def scheduled_test_skip_if_requested_within(cls) -> int:
        """若账号在最近 N 秒内有过真实请求，则本轮跳过对它的检测。0=不跳过（关闭）。

        全局配置：账号刚被真实流量打过就已证明可达，没必要再探测，省一次上游调用，
        也进一步压低堆积。取账号 _requests 里的最后一次请求时间判定。
        """
        try:
            value = int(cls._load().get("scheduled_test", {}).get("skip_if_requested_within_seconds", 0))
        except Exception:
            value = 0
        return max(0, value)

    # ==================== Tokenizer 词表版本检查 ====================
    # 只检查远端是否有新版本并记录状态，绝不自动下载：词表可达数十 MB，
    # 而估算函数在请求热路径上（token 预占 / 超限拦截），下载必须由管理员手动触发。
    @classmethod
    def tokenizer_vocab_check_enabled(cls) -> bool:
        try:
            value = (cls._load().get("tokenizer_vocab_check") or {}).get("enabled")
        except Exception:
            return DEFAULT_TOKENIZER_VOCAB_CHECK_ENABLED
        return value if isinstance(value, bool) else DEFAULT_TOKENIZER_VOCAB_CHECK_ENABLED

    @classmethod
    def tokenizer_vocab_check_interval_hours(cls) -> int:
        """检查间隔（小时）。词表极少变动，默认 24 小时；最小 1 小时防误配成高频请求。"""
        try:
            value = int((cls._load().get("tokenizer_vocab_check") or {}).get("interval_hours", 24))
        except Exception:
            value = 24
        return max(1, value)

    @classmethod
    def tokenizer_vocab_mirror(cls) -> str:
        """词表镜像地址。huggingface.co 在部分网络下不可达，默认走 hf-mirror.com。"""
        try:
            value = str((cls._load().get("tokenizer_vocab_check") or {}).get("mirror") or "").strip()
        except Exception:
            value = ""
        mirror = value.rstrip("/") or DEFAULT_TOKENIZER_VOCAB_MIRROR
        return mirror if mirror.startswith(("http://", "https://")) else DEFAULT_TOKENIZER_VOCAB_MIRROR

    # ==================== Chat Delete ====================
    @classmethod
    def message_delete_enabled(cls) -> bool:
        return cls._load().get("message_delete", {}).get("enabled", False)

    @classmethod
    def message_delete_interval(cls) -> int:
        return cls._load().get("message_delete", {}).get("interval_minutes", 30)

    @classmethod
    async def get_provider_clear_conversation_config(cls, provider: str) -> dict:
        """获取提供商清除对话配置"""
        provider_config = (await cls.get_providers()).get(provider)
        if provider_config:
            return provider_config.get("clear_conversation", {})
        return {}

    # ==================== Logging ====================
    @classmethod
    def get_log_level(cls) -> str:
        return cls._load().get("logging", {}).get("level", "DEBUG").upper()

    @classmethod
    def system_debug_enabled(cls) -> bool:
        return bool(cls._load().get("system", {}).get("debug", False))

    # ==================== Server ====================
    @classmethod
    def get_server_host(cls) -> str:
        return cls._load().get("server", {}).get("host", "0.0.0.0")

    @classmethod
    def get_server_port(cls) -> int:
        return cls._load().get("server", {}).get("port", 8000)

    # ==================== Log Retention ====================
    @classmethod
    def get_log_retention_max_entries(cls) -> int:
        """日志条数上限（数据库删除阈值）。0 = 不按条数删，只按保留天数删。

        默认 0：按条数裁剪会把尚未聚合进 hourly_dashboard_stats 的原始日志提前
        删掉（旧默认 200 曾导致日志只剩最新几百行、看板历史只剩几天）。
        """
        return cls._load().get("log_retention", {}).get("max_entries", 0)

    # ==================== Environment ====================
    @classmethod
    def is_debug(cls) -> bool:
        return os.getenv("DEBUG", "true").lower() == "true"

    @classmethod
    def get_redis_config(cls) -> dict:
        return get_redis_config()

    # ==================== Data Retention ====================
    @classmethod
    def keep_response_hours(cls) -> int:
        """响应体保留小时数，跟随请求日志保留天数。

        响应体与日志行同生命周期清理，不再单独配置保留时长，
        避免用户误以为 request_logs 每天被清空。
        """
        return max(24, cls.get_log_retention_days() * 24)

    @classmethod
    def get_log_retention_days(cls) -> int:
        val = cls._load().get("data_retention", {}).get("log_days")
        return max(1, int(val)) if val is not None else 30

    @classmethod
    def get_cleanup_hour(cls) -> int:
        """每日清理任务执行的本地小时（0-23），默认 2（凌晨 2 点）。

        控制 clean_response_loop：按日志保留天数清理主表旧行
        统一在这个整点触发。超出 0-23 的值回退到 2。
        """
        val = cls._load().get("data_retention", {}).get("cleanup_hour")
        try:
            h = int(val) if val is not None else 2
        except (TypeError, ValueError):
            return 2
        return h if 0 <= h <= 23 else 2

    # ==================== Security ====================
    @classmethod
    def security_enabled(cls) -> bool:
        """安全模块是否启用"""
        return cls._load().get("security", {}).get("enabled", True)

    @classmethod
    def security_block_on_high(cls) -> bool:
        """high 级别风险是否阻断请求"""
        return cls._load().get("security", {}).get("block_on_high", True)

    @classmethod
    def security_masking_enabled(cls) -> bool:
        """凭据遮蔽是否启用"""
        return cls._load().get("security", {}).get("masking_enabled", True)

    @classmethod
    def security_detector_enabled(cls, detector_id: str) -> bool:
        """单个检测器是否启用"""
        return cls._load().get("security", {}).get("detectors", {}).get(detector_id, True)

    @classmethod
    def security_input_limits(cls) -> dict:
        """输入校验阈值"""
        defaults = {"max_message_size_bytes": 512000, "max_messages": 500}
        limits = cls._load().get("security", {}).get("input_limits", {})
        defaults.update(limits)
        return defaults


# 兼容旧代码（tool_url 已无使用方；默认值不再指向内网地址）
is_debug = Config.is_debug()
tool_url = os.getenv("TOOL_URL", "")
