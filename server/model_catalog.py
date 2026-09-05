"""Immutable, process-local model catalog snapshots.

The database remains the source of truth. Readers use :func:`current_snapshot`
without I/O, while reloads build a complete replacement and publish it with a
single assignment.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class ModelCatalogSnapshot:
    generation: int
    fingerprint: str
    groups: Mapping[str, Mapping[str, Any]]
    group_index: Mapping[str, Mapping[str, Any]]
    metadata: Mapping[str, Mapping[str, Any]]
    default: Mapping[str, Any]
    group_metadata: Mapping[str, Mapping[str, Any]]

    @property
    def metadata_by_id(self) -> Mapping[str, Mapping[str, Any]]:
        return self.metadata

    @property
    def default_metadata(self) -> Mapping[str, Any]:
        return self.default

    @property
    def group_metadata_by_id(self) -> Mapping[str, Mapping[str, Any]]:
        return self.group_metadata


# Shorter compatibility name for callers that do not need the implementation
# detail in the type name.
CatalogSnapshot = ModelCatalogSnapshot


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def _json_value(value: Any) -> Any:
    """Return a deterministic JSON-compatible representation."""
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_json_value(item) for item in value), key=repr)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _fingerprint(content: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _json_value(content),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _metadata_by_id(raw_metadata: Any) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    if isinstance(raw_metadata, Mapping):
        entries = raw_metadata.items()
    elif isinstance(raw_metadata, (list, tuple)):
        entries = ((None, item) for item in raw_metadata)
    else:
        entries = ()

    for key, item in entries:
        if not isinstance(item, Mapping):
            continue
        record = dict(item)
        model_id = record.get("model_id") or record.get("id") or key
        if model_id is None:
            continue
        model_id = str(model_id).strip()
        if not model_id:
            continue
        record["model_id"] = model_id
        by_id[model_id] = record
    return by_id


# 非激活方案在内存快照里派生的组条目名分隔符。DB 里的组名/别名经 normalize_model_group
# 清洗不会含此串（admin 校验层再兜底拒绝），因此这些派生条目不会与任何真实组/别名撞名，
# 也不会被客户端直接请求命中。
SCHEME_GROUP_SEP = "#scheme:"


def is_internal_model_id(model_id: Any) -> bool:
    """Return whether a model ID names a runtime-only scheme fallback node."""
    return isinstance(model_id, str) and SCHEME_GROUP_SEP in model_id


def _expand_scheme_fallback_groups(
    groups: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    """把每个组的非激活方案按数组顺序派生成额外的内存组条目，并串进 backup 链。

    DB 里一个组永远只有一行（含 schemes 数组）；这里只在内存快照里展开——非激活方案各成
    一个组条目，其 models/白名单/黑名单取该方案，其余字段（remark/response_model/
    metadata_model）继承主组以保证降级后对外身份不变。主组的 backup_group 改写为指向第一个
    派生条目，派生条目依次相连，链尾接回主组原本的 backup_group（用户配的备用自定义模型）。

    引擎 _run_group_fallback_pipeline 沿 backup_group 链降级即得到「方案降级」，这些派生条目
    对它就是普通组，无需任何引擎改动。单方案（或只有激活方案）的组零副作用。
    """
    result: dict[str, Mapping[str, Any]] = {}
    derived: dict[str, Mapping[str, Any]] = {}
    for key, group in groups.items():
        result[key] = group
        if not isinstance(group, Mapping):
            continue
        if not group.get("enabled", True):
            continue
        schemes = group.get("schemes")
        if not isinstance(schemes, (list, tuple)) or len(schemes) <= 1:
            continue
        group_name = str(group.get("name") or key or "").strip()
        if not group_name:
            continue
        active_id = str(group.get("active_scheme") or "").strip()
        # 方案身份是 id（不是 name）。只有**显式标记 is_backup** 的非激活方案才进降级链，
        # 按数组顺序降级；激活方案不派生（它就是主组的顶层投影）。未标记 is_backup 的方案
        # 只是可切换的备选配置，不参与自动降级。
        fallback_schemes = [
            s for s in schemes
            if isinstance(s, Mapping) and str(s.get("id") or "").strip()
            and str(s.get("id") or "").strip() != active_id
            and s.get("is_backup")
            and [m for m in (s.get("models") or []) if isinstance(m, str) and m]
        ]
        if not fallback_schemes:
            continue
        original_backup = str(group.get("backup_group") or "").strip()
        derived_names = [
            f"{group_name}{SCHEME_GROUP_SEP}{i + 1}" for i in range(len(fallback_schemes))
        ]
        # 主组 backup 改写为第一个派生条目（原 backup 顺延到链尾）。
        result[key] = {**group, "backup_group": derived_names[0]}
        for i, scheme in enumerate(fallback_schemes):
            next_backup = derived_names[i + 1] if i + 1 < len(derived_names) else original_backup
            derived[derived_names[i]] = {
                "name": derived_names[i],
                # 派生条目是方案降级用的路由组，身份随主组（custom）；继承主组 metadata
                # 以保证降级后元数据视图不变。real 行无 schemes，不会走到这里。
                "kind": group.get("kind", "custom"),
                "enabled": True,
                "remark": group.get("remark", ""),
                "models": list(scheme.get("models") or []),
                "aliases": [],
                "provider_whitelist": list(scheme.get("provider_whitelist") or []),
                "provider_blacklist": list(scheme.get("provider_blacklist") or []),
                "selection_strategy": group.get("selection_strategy", "intelligent"),
                "backup_group": next_backup,
                "response_model": group.get("response_model", ""),
                "metadata_model": group.get("metadata_model", ""),
                "metadata": dict(group.get("metadata") or {}),
                "schemes": [],
                "active_scheme": "",
                "created_at": group.get("created_at", 0),
            }
    result.update(derived)
    return result


def _group_index(groups: Mapping[str, Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    index: dict[str, Mapping[str, Any]] = {}
    for key, group in groups.items():
        if not isinstance(group, Mapping):
            continue
        # real 行是真实模型元数据载体，不参与路由选路（无 models/schemes），
        # 显式按 kind 排除，避免任何 real 行意外带 models 时污染选路索引。
        if str(group.get("kind") or "custom") == "real":
            continue
        if not group.get("enabled", True) or not group.get("models"):
            continue
        name = str(group.get("name") or key or "").strip()
        if name and name not in index:
            index[name] = group
        aliases = group.get("aliases") or ()
        if not isinstance(aliases, (list, tuple)):
            continue
        for alias in aliases:
            if isinstance(alias, str) and alias and alias not in index:
                index[alias] = group
    return index


def _effective_max_tokens(record: Mapping[str, Any], default: Mapping[str, Any]) -> int:
    raw = record.get("max_tokens") or default.get("max_tokens") or 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _group_metadata(
    group_index: Mapping[str, Mapping[str, Any]],
    metadata: Mapping[str, Mapping[str, Any]],
    default: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for display_id, group in group_index.items():
        metadata_model = str(group.get("metadata_model") or "").strip()
        selected = metadata.get(metadata_model) if metadata_model else None
        if selected is None:
            candidates = [
                metadata[model_id]
                for model_id in group.get("models", ())
                if isinstance(model_id, str) and model_id in metadata
            ]
            if candidates:
                selected = min(candidates, key=lambda item: _effective_max_tokens(item, default))
        if selected is not None:
            result[display_id] = {
                key: value for key, value in selected.items() if key != "model_id"
            }
    return result


def _build_snapshot(source: Mapping[str, Any], generation: int) -> ModelCatalogSnapshot:
    raw_groups = source.get("groups") or {}
    groups = (
        {
            str(group.get("name") or name): dict(group)
            for name, group in raw_groups.items()
            if isinstance(group, Mapping)
        }
        if isinstance(raw_groups, Mapping)
        else {}
    )
    metadata = _metadata_by_id(source.get("metadata"))
    default = dict(source.get("default") or {}) if isinstance(source.get("default") or {}, Mapping) else {}
    # 把每个组的非激活方案按数组顺序派生为额外的内存组条目，串进 backup_group 链。
    # 纯内存派生（不落 DB）；下游 index/metadata/freeze/fingerprint 都看这份完整集合。
    groups = _expand_scheme_fallback_groups(groups)
    group_index = _group_index(groups)
    group_metadata = _group_metadata(group_index, metadata, default)
    content = {
        # Group insertion order is significant: first-created wins index collisions.
        "groups": list(groups.items()),
        "metadata": metadata,
        "default": default,
    }
    frozen_groups = _freeze(groups)
    frozen_group_index = MappingProxyType(
        {
            display_id: frozen_groups[str(group.get("name") or display_id)]
            for display_id, group in group_index.items()
        }
    )
    return ModelCatalogSnapshot(
        generation=generation,
        fingerprint=_fingerprint(content),
        groups=frozen_groups,
        group_index=frozen_group_index,
        metadata=_freeze(metadata),
        default=_freeze(default),
        group_metadata=_freeze(group_metadata),
    )


_snapshot = _build_snapshot({"groups": {}, "metadata": {}, "default": {}}, generation=0)
_reload_lock = asyncio.Lock()


def current_snapshot() -> ModelCatalogSnapshot:
    """Return the current snapshot without database or network access."""
    return _snapshot


async def reload_from_db(
    *, reason: str = "", db_client: Any = None
) -> bool:
    """Load and atomically publish a consistent catalog snapshot.

    Returns whether the published content changed. A load/build failure is
    allowed to propagate; publication is the final operation, so readers keep
    the previous complete snapshot. ``reason`` is accepted for caller-side
    diagnostics and future logging without affecting snapshot identity.
    """
    global _snapshot

    if db_client is None:
        from db import PostgresClient

        db_client = PostgresClient

    async with _reload_lock:
        source = await db_client.load_model_catalog_source()
        if not isinstance(source, Mapping):
            raise TypeError("model catalog source must be a mapping")
        candidate = _build_snapshot(source, generation=_snapshot.generation + 1)
        if candidate.fingerprint == _snapshot.fingerprint:
            return False
        _snapshot = candidate
        return True
