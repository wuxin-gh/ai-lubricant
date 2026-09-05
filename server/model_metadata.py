"""模型基础信息配置。

真相源是 model_groups 表的 kind='real' 行（metadata 列承载元数据 dict）；运行时读取由
model_catalog 发布的进程内不可变快照。管理接口写 model_groups real 行，并通过既有失效
机制（catalog reload）触发快照重载。旧的 model_metadata 表仅作迁移来源/备份，不再读写。
"""
import asyncio
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loguru import logger

from db import PostgresClient
from model_catalog import current_snapshot

from project_paths import model_metadata_json

# 全新库的一次性种子来源（DB 已有 real 行时运行时完全不读它）。文件缺失即跳过种子导入。
CONFIG_PATH = model_metadata_json()
_WRITE_LOCK = asyncio.Lock()

FALLBACK_DEFAULT_MODEL_METADATA = {
    "max_tokens": 4096,
    "max_context_tokens": 4096,
    "input_modalities": ["text"],
    "output_modalities": ["text"],
    "function_calling": False,
    "auto_search": False,
    "auto_thinking": False,
    "is_thinking": True,
    "capabilities": {},
}


def _snapshot_field(snapshot: Any, *names: str) -> Mapping:
    for name in names:
        value = getattr(snapshot, name, None)
        if isinstance(value, Mapping):
            return value
    return {}


def _mutable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _mutable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return {_mutable(item) for item in value}
    return value


def clear_request_cache() -> None:
    """Compatibility no-op: catalog snapshots need no request cache."""
    return None


async def _load_combined_indices(*, snapshot: Any = None) -> tuple[dict, dict, dict]:
    snapshot = snapshot or current_snapshot()
    by_id = _snapshot_field(snapshot, "metadata_by_id", "metadata")
    groups_by_id = _snapshot_field(
        snapshot, "group_metadata_by_id", "group_metadata"
    )
    default = _snapshot_field(snapshot, "default_metadata", "default")
    return _mutable(by_id), _mutable(groups_by_id), _mutable(default)


async def init_cache() -> None:
    """应用启动时调用：JSON → model_groups(kind=real)（仅当无 real 行时）。

    DB init 阶段的 migrate_model_metadata_into_model_groups 已把旧 model_metadata 表搬进
    model_groups；这里只负责全新库的 JSON 种子导入，直接落 model_groups real 行，避免再
    经过已废弃的 model_metadata 表中转。
    """
    try:
        count = await PostgresClient.count_real_model_metadata()
    except Exception as e:
        logger.warning(f"model_metadata 计数失败，跳过迁移: {e}")
        return
    if count == 0 and CONFIG_PATH.exists():
        await _migrate_from_json()


async def _migrate_from_json() -> None:
    """把 model_metadata.json 灌进 model_groups（kind=real），仅在无 real 行时调用。"""
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            file_cfg = json.load(f)
    except Exception as e:
        logger.warning(f"读取 model_metadata.json 失败: {e}")
        return
    default = file_cfg.get("default") or {}
    models_obj = file_cfg.get("models") or {}
    if default:
        await PostgresClient.set_model_metadata_default(default)
    items = []
    for key, value in models_obj.items():
        if not isinstance(value, dict):
            continue
        item = dict(value)
        item["model_id"] = key
        items.append(item)
    if items:
        n = await PostgresClient.bulk_upsert_real_model_metadata(items)
        logger.info(f"已从 model_metadata.json 迁移 {n} 个模型元数据到 model_groups(kind=real)")


# ==================== 对外接口 ====================

async def get_default_metadata(*, snapshot: Any = None) -> dict:
    snapshot = snapshot or current_snapshot()
    default = _snapshot_field(snapshot, "default_metadata", "default")
    return {**FALLBACK_DEFAULT_MODEL_METADATA, **_mutable(default)}


async def get_model_metadata(
    model_id: str, *, snapshot: Any = None
) -> tuple[dict, bool]:
    """返回 (metadata, is_default_only)。按 model_id 命中；模型组命中合成的特殊记录。"""
    by_id, groups_by_id, default = await _load_combined_indices(snapshot=snapshot)
    metadata = {**FALLBACK_DEFAULT_MODEL_METADATA, **default}
    record = by_id.get(model_id) or groups_by_id.get(model_id)
    if not record:
        return metadata, True
    record_copy = {k: v for k, v in record.items() if k != "model_id"}
    metadata.update(record_copy)
    return metadata, False


async def has_explicit_metadata(model_id: str, *, snapshot: Any = None) -> bool:
    if not model_id:
        return False
    by_id, groups_by_id, _ = await _load_combined_indices(snapshot=snapshot)
    return model_id in by_id or model_id in groups_by_id


async def get_explicit_metadata(model_id: str, *, snapshot: Any = None) -> dict | None:
    by_id, groups_by_id, _ = await _load_combined_indices(snapshot=snapshot)
    record = by_id.get(model_id) or groups_by_id.get(model_id)
    if not record:
        return None
    return dict(record)


async def apply_model_metadata(
    provider: str, model: dict, *, snapshot: Any = None
) -> dict:
    metadata, is_default_model = await get_model_metadata(
        model["id"], snapshot=snapshot
    )
    for key, value in metadata.items():
        if model.get(key) is None:
            model[key] = value

    if is_default_model and model.get('max_tokens') == 4096:
        logger.warning(f"模型：{model['id']} 使用默认模型，max_tokens 为 4096，请进行配置")

    if "multimodal" not in model and "input_modalities" in model:
        model["multimodal"] = model["input_modalities"]

    return model


async def reapply_to_models(models: list[dict], *, snapshot: Any = None) -> int:
    """把同一 catalog 快照中的元数据重新打到一组 model dict 上（in-place）。"""
    by_id, _, default = await _load_combined_indices(snapshot=snapshot)
    touched = 0
    for m in models or []:
        mid = m.get("id")
        if not mid:
            continue
        record = by_id.get(mid)
        if record:
            for key, value in record.items():
                if key == "model_id":
                    continue
                m[key] = value
        else:
            for key, value in default.items():
                if m.get(key) is None:
                    m[key] = value
        if "multimodal" not in m and "input_modalities" in m:
            m["multimodal"] = m["input_modalities"]
        touched += 1
    return touched


# ==================== 管理接口（异步） ====================

async def list_metadata_from_db_async() -> dict:
    """纯内存返回：从当前 catalog 快照读 default + 全部 real 元数据行。

    快照由 load_model_catalog_source 从 model_groups(kind=real) 构建；管理端列表不再查库。
    每条附行上的渠道标签过滤（provider_whitelist/provider_blacklist）——real 行的路由
    配置存于行顶层列而非 metadata JSONB，这里从快照 groups 并入，供管理端展示/回填。
    """
    snap = current_snapshot()
    by_id = _snapshot_field(snap, "metadata_by_id", "metadata")
    groups = _snapshot_field(snap, "groups")
    models = []
    for record in by_id.values():
        item = dict(record)
        model_id = str(item.get("model_id") or "")
        group = groups.get(model_id) if model_id else None
        if isinstance(group, Mapping) and str(group.get("kind") or "custom") == "real":
            item["provider_whitelist"] = list(group.get("provider_whitelist") or [])
            item["provider_blacklist"] = list(group.get("provider_blacklist") or [])
            item["schemes"] = [dict(s) for s in (group.get("schemes") or [])]
            item["active_scheme"] = str(group.get("active_scheme") or "")
        else:
            item.setdefault("provider_whitelist", [])
            item.setdefault("provider_blacklist", [])
        models.append(item)
    default = _snapshot_field(snap, "default_metadata", "default")
    return {"default": _mutable(default), "models": models}


async def list_metadata_async() -> dict:
    return await list_metadata_from_db_async()


async def upsert_metadata_async(model_id: str, data: dict) -> dict:
    model_id = (model_id or "").strip()
    if not model_id:
        raise ValueError("model_id 必填")
    async with _WRITE_LOCK:
        saved = await PostgresClient.upsert_real_model_metadata(model_id, data or {})
    merged = dict(saved.get("metadata") or {})
    merged["model_id"] = model_id
    # 行顶层列（渠道标签过滤）一并返回，供管理端保存后回显。
    for fld in ("provider_whitelist", "provider_blacklist"):
        merged[fld] = list(saved.get(fld) or [])
    return merged


async def delete_metadata_async(model_id: str) -> bool:
    ok = await PostgresClient.delete_real_model_metadata(model_id)
    return ok


async def update_default_async(default: dict) -> dict:
    cleaned = {**FALLBACK_DEFAULT_MODEL_METADATA, **(default or {})}
    await PostgresClient.set_model_metadata_default(cleaned)
    return cleaned


async def bulk_import_async(items: list[dict]) -> int:
    n = await PostgresClient.bulk_upsert_real_model_metadata(items)
    return n


# ==================== OpenRouter ====================

_OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"


async def fetch_openrouter_models(timeout: float = 30.0) -> list[dict]:
    import aiohttp

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with session.get(_OPENROUTER_MODELS_URL, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"OpenRouter HTTP {resp.status}: {text[:200]}")
            data = await resp.json()
    return list(data.get("data") or [])


def normalize_openrouter_model(item: dict) -> dict:
    """OpenRouter 单条 → 本系统 metadata（不含 model_id）。"""
    arch = item.get("architecture") or {}
    top = item.get("top_provider") or {}
    inputs = list(arch.get("input_modalities") or []) or ["text"]
    outputs = list(arch.get("output_modalities") or []) or ["text"]
    supported = set(item.get("supported_parameters") or [])
    function_calling = bool({"tools", "tool_choice"} & supported)
    context_length = item.get("context_length") or top.get("context_length") or 0
    max_completion = top.get("max_completion_tokens") or context_length or 0
    created = item.get("created")
    owner = (item.get("id") or "").split("/", 1)[0] if "/" in (item.get("id") or "") else ""
    return {
        "id": item.get("id"),
        "name": item.get("name") or item.get("id"),
        "owned_by": owner,
        "object": "model",
        "created": int(created) if isinstance(created, (int, float)) else None,
        "max_context_tokens": int(context_length or 0) or None,
        "max_tokens": int(max_completion or 0) or None,
        "input_modalities": inputs,
        "output_modalities": outputs,
        "multimodal": inputs,
        "function_calling": function_calling,
        "auto_search": False,
        "auto_thinking": "reasoning" in supported or "include_reasoning" in supported,
        "is_thinking": "reasoning" in supported or "include_reasoning" in supported,
        "capabilities": {},
    }


# ==================== models.dev ====================

_MODELSDEV_URL = "https://models.dev/api.json"


async def fetch_modelsdev(timeout: float = 30.0) -> dict:
    """拉取 models.dev 全量 catalog，原样返回 {provider: {models: {id: item}, ...}}。"""
    import aiohttp

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with session.get(_MODELSDEV_URL, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"models.dev HTTP {resp.status}: {text[:200]}")
            data = await resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("models.dev 返回不是对象")
    return data


def flatten_modelsdev(catalog: dict) -> list[dict]:
    """{provider: {models: {id: item}}} → [{provider, id, item}]"""
    flat: list[dict] = []
    for provider_key, provider_data in (catalog or {}).items():
        if not isinstance(provider_data, dict):
            continue
        models = provider_data.get("models") or {}
        if not isinstance(models, dict):
            continue
        for model_id, item in models.items():
            if not isinstance(item, dict):
                continue
            flat.append({"provider": provider_key, "id": item.get("id") or model_id, "item": item})
    return flat


def normalize_modelsdev_model(provider_key: str, item: dict) -> dict:
    """models.dev 单条 → 本系统 metadata（不含 model_id）。"""
    modalities = item.get("modalities") or {}
    inputs = list(modalities.get("input") or []) or ["text"]
    outputs = list(modalities.get("output") or []) or ["text"]
    limit = item.get("limit") or {}
    context_length = int(limit.get("context") or 0) or None
    max_output = int(limit.get("output") or 0) or None
    return {
        "name": item.get("name") or item.get("id"),
        "owned_by": provider_key or item.get("family") or "",
        "object": "model",
        "max_context_tokens": context_length,
        "max_tokens": max_output,
        "input_modalities": inputs,
        "output_modalities": outputs,
        "multimodal": inputs,
        "function_calling": bool(item.get("tool_call")),
        "auto_search": False,
        "auto_thinking": bool(item.get("reasoning")),
        "is_thinking": bool(item.get("reasoning")),
        "capabilities": {},
    }


# ==================== llm-metadata ====================

# llm-metadata 是 models.dev 目录 + 社区 overrides 的构建产物（详见
# https://github.com/basellm/llm-metadata）。dist/api/all.json 已经把基础目录与
# overrides 深度合并为 {providerId: {models: {modelId: {...}}}}，无需再逐文件抓取
# overrides 也无需单独拉 models.dev 基线。结构与 models.dev/api.json 同形。
_LLM_METADATA_ALL_URL = "https://raw.githubusercontent.com/basellm/llm-metadata/main/dist/api/all.json"


async def fetch_llm_metadata(timeout: float = 30.0) -> dict:
    """拉取 llm-metadata 已构建的全量 catalog，原样返回 {provider: {models: {id: item}, ...}}。"""
    import aiohttp

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with session.get(_LLM_METADATA_ALL_URL, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"llm-metadata HTTP {resp.status}: {text[:200]}")
            # GitHub raw 返回 text/plain；放开 content_type 校验，避免误判。
            data = await resp.json(content_type=None)
    if not isinstance(data, dict):
        raise RuntimeError("llm-metadata 返回不是对象")
    return data


def flatten_llm_metadata(catalog: dict) -> list[dict]:
    """{provider: {models: {id: item}}} → [{provider, id, item}]。

    与 ``flatten_modelsdev`` 同形；llm-metadata 的 provider 对象还会带 name/iconURL
    等字段，这里只取 models 子表，附加字段由调用方按需读取。
    """
    return flatten_modelsdev(catalog)


def normalize_llm_metadata_model(provider_key: str, item: dict) -> dict:
    """llm-metadata 单条 → 本系统 metadata（不含 model_id）。

    llm-metadata 的字段与 models.dev 同形，但额外有 attachment、structured_output、
    family、release_date、iconURL 等信息。按用户口径只导入核心字段：limit/modality/
    reasoning/tool_call；attachment 放进 capabilities 保留，不导入 cost/icon/description。
    """
    normalized = normalize_modelsdev_model(provider_key, item)
    capabilities: dict[str, Any] = dict(normalized.get("capabilities") or {})
    if "attachment" in item:
        capabilities["attachment"] = bool(item.get("attachment"))
    if "structured_output" in item:
        capabilities["structured_output"] = bool(item.get("structured_output"))
    if capabilities:
        normalized["capabilities"] = capabilities
    return normalized
