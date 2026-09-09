"""统一资源存储：``resources``（资源本体）+ ``resource_references``（团队引用）。

设计（与 ``server/db.py`` 的建表注释一致）：资源本体只描述「是什么、来自哪里、
如何交付」（来源/类型/形态化数据/关联子项/编辑器）；团队参数与授权放在引用侧
（``params`` + 分组 grants），同一资源被不同团队引用时互不覆盖。

风格对齐 ``resource_mirror_store`` / ``marketplace_leaderboard_store``：模块级
函数 + 函数内延迟 import PostgresClient。本模块只做新表读写，**不迁移**旧表
（``mc_resource_references`` / ``marketplace_leaderboard_items`` 仍在服役，
双轨过渡：新链路写新表，旧链路照旧，等切换完成后再清理旧数据）。

字段口径：

* ``resource_type`` 单选：skills|skill|plugin|mcp|prompt（skills=技能集合）。
* ``resource_data`` 按类型形态化：skills → {install_method, ref, clone_url,
  entries:[{name,path,entry}]}；skill → {install_method, ref, clone_url, path,
  entry}；plugin → {download_url, provider, entry}；mcp → {kind, command, args,
  env, url, transport}；prompt → {content, target_files, providers}。
* ``association`` 关联子项：skills 集合的子 skill 名列表（前端搜索命中此字段，
  命中后显示集合这一行，任务期再按 entries 勾选，不拆成独立行）。
* ``editors`` 适用编辑器并集：claude|codex|opencode|cursor|gemini。
* ``source_type`` + ``source_data``：leaderboard_sync | manual | github_recognize
  | personal_upload | market_mirror，明细形态见 ``_SOURCE_DATA_KEYS``。
* ``status``：draft|published|active——榜单来源走 draft→published 策展；非榜单
  来源恒 active（识别即入库即可用）。
* 引用表 ``params``：团队私有参数（MCP 的 token/headers/env 等）；``version``
  钉死的版本（引用时 pin 的 commit sha；空 = 跟分支浮动）。
"""
from __future__ import annotations

import json
import uuid
from typing import Any

# 单选资源类型（新 schema 收口旧 target_modules 多值）。skills=技能集合。
RESOURCE_TYPES = frozenset({"skills", "skill", "plugin", "mcp", "prompt"})
SOURCE_TYPES = frozenset({
    "leaderboard_sync", "manual", "github_recognize", "personal_upload", "market_mirror",
})

# 允许的编辑器取值（与 user_platform 的 _ALL_EDITORS / prompt providers 对齐）。
EDITORS = frozenset({"claude", "codex", "opencode", "cursor", "gemini"})

# JSONB 列在 asyncpg 里以 str 进出（老 store 同款约定）：行 → dict 时解码，写库时编码。
_JSON_COLUMNS = (
    "resource_data", "association", "editors", "source_data", "probe_data", "params",
)


def _pool():
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("postgres pool not initialized")
    return PostgresClient.pool


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _decode_json_columns(row: dict) -> dict:
    """asyncpg 返回的 JSONB 是 str：逐列 json.loads，坏值兜成类型默认。"""
    defaults = {
        "resource_data": dict, "source_data": dict, "probe_data": dict, "params": dict,
        "association": list, "editors": list,
    }
    for col in _JSON_COLUMNS:
        if col not in row:
            continue
        raw = row[col]
        if isinstance(raw, (dict, list)) or raw is None:
            continue
        try:
            row[col] = json.loads(raw)
        except (TypeError, ValueError):
            row[col] = defaults.get(col, dict)()
    return row


def resource_dict(row: dict) -> dict:
    """resources 行 → API dict（解码 JSONB + 时间戳 ISO）。"""
    row = _decode_json_columns(dict(row))
    for key in ("published_at", "created_at", "updated_at"):
        if row.get(key) is not None and not isinstance(row[key], str):
            row[key] = row[key].isoformat()
    return row


def reference_dict(row: dict, *, resource: dict | None = None) -> dict:
    """resource_references 行 → API dict；带出 resource 本体投影（一次 join 免二次查）。"""
    row = _decode_json_columns(dict(row))
    for key in ("created_at", "updated_at"):
        if row.get(key) is not None and not isinstance(row[key], str):
            row[key] = row[key].isoformat()
    out = {
        "id": str(row["id"]),
        "team_id": str(row["team_id"]),
        "resource_id": row["resource_id"],
        "params": row.get("params") or {},
        "display_name": row.get("display_name") or "",
        "description": row.get("description") or "",
        "version": row.get("version") or "",
        "enabled": bool(row.get("enabled", True)),
        "created_by": str(row["created_by"]) if row.get("created_by") else None,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }
    if resource is not None:
        out["resource"] = resource_dict(resource)
    return out


# ── resources：资源本体 ─────────────────────────────────────────────────────


async def create_resource(
    *,
    resource_type: str,
    resource_data: dict | None = None,
    association: list | None = None,
    editors: list | None = None,
    source_type: str,
    source_data: dict | None = None,
    name: str,
    display_name: str = "",
    description: str = "",
    version: str = "",
    status: str = "active",
    probe_data: dict | None = None,
) -> dict:
    """插入一行资源。类型/来源/状态不在白名单直接 ValueError（CHECK 前置校验）。

    幂等去重由调用方决定（``find_resource_by_source`` + update），本函数只管插入——
    榜单同步的 upsert 语义（保留探针、字段合并）各源口径不同，塞在通用层会漂移。
    """
    if resource_type not in RESOURCE_TYPES:
        raise ValueError(f"invalid_resource_type:{resource_type}")
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"invalid_source_type:{source_type}")
    if status not in ("draft", "published", "active"):
        raise ValueError(f"invalid_status:{status}")
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO resources (
                resource_type, resource_data, association, editors,
                source_type, source_data, name, display_name, description,
                version, status, probe_data
            ) VALUES (
                $1, $2::jsonb, $3::jsonb, $4::jsonb,
                $5, $6::jsonb, $7, $8, $9,
                $10, $11, $12::jsonb
            ) RETURNING *
            """,
            resource_type,
            json.dumps(resource_data or {}, ensure_ascii=False),
            json.dumps(association or [], ensure_ascii=False),
            json.dumps(_clean_editors(editors), ensure_ascii=False),
            source_type,
            json.dumps(source_data or {}, ensure_ascii=False),
            name,
            display_name or name,
            description or "",
            version or "",
            status,
            json.dumps(probe_data or {}, ensure_ascii=False),
        )
    return resource_dict(dict(row))


async def get_resource(resource_id: int | str) -> dict | None:
    """按 id 取一行资源（含草稿；读侧不筛 status，可见性由调用方决定）。"""
    try:
        rid = int(resource_id)
    except (TypeError, ValueError):
        return None
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM resources WHERE id=$1", rid)
    return resource_dict(dict(row)) if row else None


async def update_resource(resource_id: int | str, patch: dict) -> dict | None:
    """按 patch 白名单局部更新资源。只认已知列，未知键忽略（不拼动态 SQL）。"""
    try:
        rid = int(resource_id)
    except (TypeError, ValueError):
        return None
    allowed = {
        "resource_type", "name", "display_name", "description", "version",
        "status", "sort_order",
    }
    sets: list[str] = []
    args: list[Any] = []
    for column in allowed:
        if column not in patch:
            continue
        args.append(patch[column])
        sets.append(f"{column} = ${len(args)}")
    # JSONB 列整体替换（形态化数据由调用方构造完整对象传入）。
    for column in ("resource_data", "association", "editors", "source_data", "probe_data"):
        if column not in patch:
            continue
        args.append(json.dumps(patch[column], ensure_ascii=False))
        sets.append(f"{column} = ${len(args)}::jsonb")
    if not sets:
        return await get_resource(rid)
    args.append(rid)
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE resources SET {', '.join(sets)}, updated_at = now() WHERE id = ${len(args)} RETURNING *",
            *args,
        )
    return resource_dict(dict(row)) if row else None


async def find_resource_by_source(
    source_type: str, source_key: str, source_value: str
) -> dict | None:
    """按来源定位资源（github_recognize → repo_full_name；market_mirror → market_id）。

    ``source_data`` 是 GIN 索引的 jsonb_path_ops：``@>`` 单键包含查询能吃到索引。
    """
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM resources WHERE source_type=$1 AND source_data @> $2::jsonb LIMIT 1",
            source_type, json.dumps({source_key: source_value}),
        )
    return resource_dict(dict(row)) if row else None


async def upsert_resource_by_source(
    source_type: str,
    source_key: str,
    source_value: str,
    values: dict,
) -> dict:
    """按来源键 upsert（存在 → 局部更新；不存在 → 插入）。GitHub 识别/榜单同步共用。

    不会覆盖 ``status``/``published_at``/``published_by``——发布是策展动作，识别与
    同步永远不能替管理员发布（与榜单 store 的「同步只写 draft」不变量同源）。
    """
    existing = await find_resource_by_source(source_type, source_key, source_value)
    if existing is None:
        return await create_resource(source_type=source_type, **values)
    patch = {k: v for k, v in values.items() if k not in ("status", "published_at", "published_by")}
    return await update_resource(existing["id"], patch) or existing


async def search_resources(
    *,
    resource_type: str | None = None,
    source_type: str | None = None,
    status: str | None = None,
    q: str = "",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """搜索资源：名称/描述/关联子项（association GIN 前提下的 ILIKE 兜底）。

    关联子项搜索语义：搜 "pdf" 能命中 ``anthropics/skills`` 集合（association
    含 "pdf"），命中显示集合这一行——不把子技能拆成独立行。q 走
    ``name ILIKE OR display_name ILIKE OR description ILIKE OR association::text ILIKE``
    （association 转 text 模糊匹配，JSONB 数组的字符串包含；量大后再换
    jsonb_path 查询）。
    """
    where: list[str] = []
    args: list[Any] = []

    def _add(cond: str, value: Any) -> None:
        args.append(value)
        where.append(cond.format(n=len(args)))

    if resource_type:
        _add("resource_type = ${n}", resource_type)
    if source_type:
        _add("source_type = ${n}", source_type)
    if status:
        _add("status = ${n}", status)
    if q.strip():
        needle = f"%{q.strip()}%"
        _add(
            "(name ILIKE ${n} OR display_name ILIKE ${n} OR description ILIKE ${n}"
            " OR association::text ILIKE ${n})",
            needle,
        )
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    limit = max(1, min(200, limit))
    pool = _pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM resources {where_sql}", *args)
        rows = await conn.fetch(
            f"SELECT * FROM resources {where_sql} ORDER BY sort_order NULLS LAST, id DESC LIMIT {limit} OFFSET {offset}",
            *args,
        )
    return [resource_dict(dict(r)) for r in rows], int(total or 0)


def _clean_editors(editors: list | None) -> list[str]:
    return [e for e in (editors or []) if str(e) in EDITORS]


# ── resource_references：团队引用 ──────────────────────────────────────────


async def create_reference(
    *,
    team_id: str,
    resource_id: int | str,
    params: dict | None = None,
    display_name: str = "",
    description: str = "",
    version: str = "",
    enabled: bool = True,
    created_by: str | None = None,
) -> dict:
    """团队引用一行资源（带参数）。同 (team, resource) 已存在 → ValueError。"""
    team_uuid = _uuid(team_id)
    if team_uuid is None:
        raise ValueError("invalid_team_id")
    try:
        rid = int(resource_id)
    except (TypeError, ValueError):
        raise ValueError("invalid_resource_id") from None
    pool = _pool()
    async with pool.acquire() as conn:
        # FK 之外先查存在性：报「资源不存在」比裸 IntegrityError 可读。
        if not await conn.fetchval("SELECT 1 FROM resources WHERE id=$1", rid):
            raise ValueError("resource_not_found")
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO resource_references (
                    team_id, resource_id, params, display_name, description,
                    version, enabled, created_by
                ) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8)
                ON CONFLICT (team_id, resource_id) DO UPDATE SET
                    params = EXCLUDED.params,
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    version = EXCLUDED.version,
                    updated_at = now()
                RETURNING *
                """,
                team_uuid, rid,
                json.dumps(params or {}, ensure_ascii=False),
                display_name or "", description or "", version or "", enabled,
                _uuid(created_by),
            )
        except Exception as exc:
            if "resource_references_resource_id_fkey" in str(exc):
                raise ValueError("resource_not_found") from exc
            raise
    resource = await get_resource(rid)
    return reference_dict(dict(row), resource=resource)


async def list_references(
    team_id: str,
    *,
    resource_type: str | None = None,
    include_disabled: bool = False,
) -> list[dict]:
    """列团队引用（join resources 一次带回本体投影）。resource_type 按本体过滤。"""
    team_uuid = _uuid(team_id)
    if team_uuid is None:
        raise ValueError("invalid_team_id")
    where = ["r.team_id = $1"]
    args: list[Any] = [team_uuid]
    if resource_type:
        args.append(resource_type)
        where.append(f"res.resource_type = ${len(args)}")
    if not include_disabled:
        where.append("r.enabled = true")
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT r.*, res.id AS resource_pk, res.resource_type, res.association,
                   res.editors, res.source_type, res.source_data, res.name AS res_name,
                   res.display_name AS res_display_name, res.description AS res_description,
                   res.version AS res_version, res.status AS res_status,
                   res.resource_data, res.probe_data
            FROM resource_references r
            JOIN resources res ON res.id = r.resource_id
            WHERE {' AND '.join(where)}
            ORDER BY r.created_at DESC
            """,
            *args,
        )
    out: list[dict] = []
    for row in rows:
        data = dict(row)
        resource = {
            "id": data.pop("resource_pk"),
            "resource_type": data.pop("resource_type"),
            "resource_data": data.pop("resource_data"),
            "association": data.pop("association"),
            "editors": data.pop("editors"),
            "source_type": data.pop("source_type"),
            "source_data": data.pop("source_data"),
            "name": data.pop("res_name"),
            "display_name": data.pop("res_display_name"),
            "description": data.pop("res_description"),
            "version": data.pop("res_version"),
            "status": data.pop("res_status"),
            "probe_data": data.pop("probe_data"),
        }
        out.append(reference_dict(data, resource=resource))
    return out


async def get_reference(team_id: str, reference_id: str) -> dict | None:
    team_uuid = _uuid(team_id)
    ref_uuid = _uuid(reference_id)
    if team_uuid is None or ref_uuid is None:
        return None
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT r.*, res.id AS resource_pk, res.resource_type, res.resource_data,
                   res.association, res.editors, res.source_type, res.source_data,
                   res.name AS res_name, res.display_name AS res_display_name,
                   res.description AS res_description, res.version AS res_version,
                   res.status AS res_status, res.probe_data
            FROM resource_references r
            JOIN resources res ON res.id = r.resource_id
            WHERE r.id = $1 AND r.team_id = $2
            """,
            ref_uuid, team_uuid,
        )
    if row is None:
        return None
    data = dict(row)
    resource = {
        "id": data.pop("resource_pk"),
        "resource_type": data.pop("resource_type"),
        "resource_data": data.pop("resource_data"),
        "association": data.pop("association"),
        "editors": data.pop("editors"),
        "source_type": data.pop("source_type"),
        "source_data": data.pop("source_data"),
        "name": data.pop("res_name"),
        "display_name": data.pop("res_display_name"),
        "description": data.pop("res_description"),
        "version": data.pop("res_version"),
        "status": data.pop("res_status"),
        "probe_data": data.pop("probe_data"),
    }
    return reference_dict(data, resource=resource)


async def update_reference(
    team_id: str, reference_id: str, patch: dict
) -> dict | None:
    """按 patch 白名单局部更新引用（params/display_name/description/version/enabled）。"""
    team_uuid = _uuid(team_id)
    ref_uuid = _uuid(reference_id)
    if team_uuid is None or ref_uuid is None:
        return None
    sets: list[str] = []
    args: list[Any] = []
    for column in ("display_name", "description", "version", "enabled"):
        if column not in patch:
            continue
        args.append(patch[column])
        sets.append(f"{column} = ${len(args)}")
    if "params" in patch:
        args.append(json.dumps(patch["params"] or {}, ensure_ascii=False))
        sets.append(f"params = ${len(args)}::jsonb")
    if not sets:
        return await get_reference(team_id, reference_id)
    args.extend([ref_uuid, team_uuid])
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE resource_references SET {', '.join(sets)}, updated_at = now() "
            f"WHERE id = ${len(args) - 1} AND team_id = ${len(args)} RETURNING *",
            *args,
        )
    if row is None:
        return None
    return reference_dict(dict(row), resource=await get_resource(row["resource_id"]))


async def delete_reference(team_id: str, reference_id: str) -> bool:
    team_uuid = _uuid(team_id)
    ref_uuid = _uuid(reference_id)
    if team_uuid is None or ref_uuid is None:
        return False
    pool = _pool()
    async with pool.acquire() as conn:
        # 有分组授权（resource_grants）仍挂着的引用不删——授权先撤，引用才能删。
        if await conn.fetchval(
            "SELECT 1 FROM resource_grants WHERE reference_id = $1::uuid LIMIT 1", ref_uuid
        ):
            raise ValueError("reference_has_grants")
        deleted = await conn.execute(
            "DELETE FROM resource_references WHERE id=$1 AND team_id=$2", ref_uuid, team_uuid
        )
    return deleted.endswith(" 1")


# ── resource_grants：分组授权 ──────────────────────────────────────────────


async def set_group_grants(
    *,
    team_id: str,
    group_id: str,
    reference_ids: list[str],
    created_by: str | None = None,
    resource_type: str | None = None,
) -> list[dict]:
    """按类型整组替换分组的引用授权（事务内）。

    ``resource_type`` 非空时只替换该资源类型，避免 Skills tab 保存时清掉插件/MCP
    授权；为空时保留全量替换语义供明确的管理调用使用。reference_ids 必须都
    属于本团队；不属于的直接 ValueError（不静默丢）。"""
    team_uuid = _uuid(team_id)
    group_uuid = _uuid(group_id)
    if team_uuid is None or group_uuid is None:
        raise ValueError("invalid_team_or_group")
    wanted = [rid for rid in (_uuid(r) for r in reference_ids) if rid is not None]
    pool = _pool()
    async with pool.acquire() as conn:
        if wanted:
            rows = await conn.fetch(
                """
                SELECT r.id, res.resource_type
                FROM resource_references r
                JOIN resources res ON res.id = r.resource_id
                WHERE r.id = ANY($1::uuid[]) AND r.team_id=$2
                """,
                wanted, team_uuid,
            )
            if {r["id"] for r in rows} != set(wanted):
                raise ValueError("reference_not_in_team")
            if resource_type:
                allowed_types = {"skill", "skills"} if resource_type == "skill" else {resource_type}
                if any(r["resource_type"] not in allowed_types for r in rows):
                    raise ValueError("reference_type_mismatch")
        async with conn.transaction():
            if resource_type:
                # skill 主类型连带 skills 集合（同属技能维度，一并替换避免残留）。
                type_scope = ["skill", "skills"] if resource_type == "skill" else [resource_type]
                await conn.execute(
                    """DELETE FROM resource_grants g
                        WHERE g.team_id=$1 AND g.group_id=$2
                        AND EXISTS (
                            SELECT 1 FROM resource_references r JOIN resources res ON res.id = r.resource_id
                            WHERE r.id = g.reference_id AND res.resource_type = ANY($3::text[])
                        )""",
                    team_uuid, group_uuid, type_scope,
                )
            else:
                await conn.execute(
                    "DELETE FROM resource_grants WHERE team_id=$1 AND group_id=$2",
                    team_uuid, group_uuid,
                )
            for rid in wanted:
                await conn.execute(
                    "INSERT INTO resource_grants (team_id, group_id, reference_id, created_by)"
                    " VALUES ($1, $2, $3, $4) ON CONFLICT (group_id, reference_id) DO NOTHING",
                    team_uuid, group_uuid, rid, _uuid(created_by),
                )
    return await list_group_references(team_id, group_id)


async def list_group_references(
    team_id: str, group_id: str, resource_type: str | None = None
) -> list[dict]:
    """列某分组被授权的引用（含 resource 本体投影）。"""
    team_uuid = _uuid(team_id)
    group_uuid = _uuid(group_id)
    if team_uuid is None or group_uuid is None:
        return []
    pool = _pool()
    where = ["g.team_id = $1", "g.group_id = $2", "r.enabled = true"]
    args: list[Any] = [team_uuid, group_uuid]
    if resource_type:
        args.append(resource_type)
        where.append(f"res.resource_type = ${len(args)}")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT r.*, res.id AS resource_pk, res.resource_type, res.resource_data,
                   res.association, res.editors, res.source_type, res.source_data,
                   res.name AS res_name, res.display_name AS res_display_name,
                   res.description AS res_description, res.version AS res_version,
                   res.status AS res_status, res.probe_data
            FROM resource_grants g
            JOIN resource_references r ON r.id = g.reference_id
            JOIN resources res ON res.id = r.resource_id
            WHERE {' AND '.join(where)}
            ORDER BY r.created_at DESC
            """,
            *args,
        )
    return [_row_with_resource(dict(row)) for row in rows]


async def visible_references(
    user_id: str, team_id: str, *, resource_type: str | None = None, is_admin: bool = False
) -> list[dict]:
    """用户可见的引用：团队管理员看全团队；普通成员看被授权分组的并集。

    ``is_admin`` 由调用方（已解析团队角色）传入——本 store 不查团队成员表，
    保持与 db 无关的纯资源语义。
    """
    if is_admin:
        return await list_references(team_id, resource_type=resource_type)
    team_uuid = _uuid(team_id)
    user_uuid = _uuid(user_id)
    if team_uuid is None or user_uuid is None:
        return []
    where = ["g.team_id = $1", "gm.user_id = $2", "r.enabled = true"]
    args: list[Any] = [team_uuid, user_uuid]
    if resource_type:
        args.append(resource_type)
        where.append(f"res.resource_type = ${len(args)}")
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT DISTINCT ON (r.id)
                   r.*, res.id AS resource_pk, res.resource_type, res.resource_data,
                   res.association, res.editors, res.source_type, res.source_data,
                   res.name AS res_name, res.display_name AS res_display_name,
                   res.description AS res_description, res.version AS res_version,
                   res.status AS res_status, res.probe_data
            FROM resource_grants g
            JOIN resource_references r ON r.id = g.reference_id
            JOIN resources res ON res.id = r.resource_id
            JOIN mc_team_group_members gm ON gm.group_id = g.group_id
            WHERE {' AND '.join(where)}
            """,
            *args,
        )
    return [_row_with_resource(dict(row)) for row in rows]


def _row_with_resource(data: dict) -> dict:
    """把 JOIN 出来的扁平行拆成 reference dict + 内嵌 resource 投影。"""
    resource = {
        "id": data.pop("resource_pk"),
        "resource_type": data.pop("resource_type"),
        "resource_data": data.pop("resource_data"),
        "association": data.pop("association"),
        "editors": data.pop("editors"),
        "source_type": data.pop("source_type"),
        "source_data": data.pop("source_data"),
        "name": data.pop("res_name"),
        "display_name": data.pop("res_display_name"),
        "description": data.pop("res_description"),
        "version": data.pop("res_version"),
        "status": data.pop("res_status"),
        "probe_data": data.pop("probe_data"),
    }
    return reference_dict(data, resource=resource)


# ── 解析（节点交付 wire spec：新表版 resolve）──────────────────────────────


async def resolve_specs(
    references: list[dict],
    bindings: list[dict],
    *,
    request_base_url: str = "",
) -> list[dict]:
    """把引用列表解析成节点 wire spec（新表版，旧 resolve_reference_specs 的承接）。

    绑定形态：``{reference_id}``（可带 ``entries`` 按子技能过滤，见集合语义）。
    每个 wire spec 形状与旧 resolve 对齐（skill → {name,source,url,path,ref}；
    plugin → {name,url,version}；mcp → 由 params + resource_data 产 spec），
    节点侧零改动。

    集合（resource_type=skills）按 ``association``/``resource_data.entries`` 展开：
    绑定带 ``entries`` → 只展开勾中的子技能；不带 → 全量。子技能名
    ``owner/repo/entry_name``，与任务期 activeSkills 勾选标识一致。
    """
    by_id = {str(r["id"]): r for r in references}
    out: list[dict] = []
    for binding in bindings or []:
        ref_id = str(binding.get("reference_id") or binding.get("resource_id") or binding.get("id") or "")
        reference = by_id.get(ref_id)
        if reference is None:
            continue
        resource = reference.get("resource") or {}
        rtype = str(resource.get("resource_type") or "")
        data = resource.get("resource_data") or {}
        name = reference.get("display_name") or resource.get("display_name") or resource.get("name") or ""
        params = reference.get("params") or {}

        if rtype == "skills":
            out.extend(_expand_skills_collection(resource, reference, binding, name))
        elif rtype == "skill":
            out.append({
                "name": name,
                "source": data.get("source") or "github",
                "url": data.get("clone_url") or "",
                "path": data.get("path") or "",
                "ref": reference.get("version") or data.get("ref") or "",
            })
        elif rtype == "plugin":
            out.append({
                "name": name,
                "url": data.get("download_url") or "",
                "version": reference.get("version") or resource.get("version") or "",
            })
        elif rtype == "mcp":
            out.append(_mcp_spec_from_params(data, params, name))
    return out


def _expand_skills_collection(resource: dict, reference: dict, binding: dict, name: str) -> list[dict]:
    """集合 → N 个子技能 spec；绑定 entries 过滤，缺省全量。子技能名 owner/repo/name。

    ref 优先取引用钉死的版本（``reference.version`` = pin 的 commit sha），否则回落
    资源本体的分支——与单技能 spec 的钉死口径一致。
    """
    data = resource.get("resource_data") or {}
    entries = data.get("entries") or []
    wanted = {str(e) for e in (binding.get("entries") or []) if str(e).strip()}
    source_data = resource.get("source_data") or {}
    repo = str(source_data.get("repo_full_name") or "")
    ref = str(reference.get("version") or data.get("ref") or "")
    out: list[dict] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_name = str(entry.get("name") or "")
        if not entry_name or (wanted and entry_name not in wanted):
            continue
        out.append({
            "name": f"{repo}/{entry_name}" if repo else f"{name}/{entry_name}",
            "source": data.get("source") or "github",
            "url": data.get("clone_url") or "",
            "path": str(entry.get("path") or ""),
            "ref": ref,
        })
    return out


def _mcp_spec_from_params(data: dict, params: dict, name: str) -> dict:
    """MCP 引用 → 节点 spec：本体 resource_data 给形态，params 给团队私有连接参数。

    与旧链路（mcp_services 行）产出的字段集合对齐：stdio → command/args/env；
    remote → url/transport（网关代理 token 由调用方叠加，本层不签发）。

    ``params`` 是团队私有连接凭证：``token``（remote → ``Authorization: Bearer``，
    与旧 mcp_services「前端传明文 token、后端组装 Bearer」口径一致）、
    ``headers``（自定义头，已有 Authorization 不覆盖）、``env``（stdio 环境变量覆盖）、
    ``url``/``transport``（remote 形态运行期覆盖本体）。

    ``data.env`` 兼容两种形态：github_recognize 落池的是 ``{name: ""}`` 占位字典
    （probe 的 launch_spec.env 是变量名列表，已在落池时归一）；榜单同步直存
    launch_spec 时可能是变量名列表——这里也展开成 ``{name: ""}`` 兜底，绝不抛
    ``dict(list)`` 的 ValueError。
    """
    kind = str(data.get("kind") or "")
    if kind == "stdio":
        return {
            "name": name,
            "type": "stdio",
            "transport": "stdio",
            "command": str(data.get("command") or ""),
            "args": [str(a) for a in (data.get("args") or []) if str(a).strip()],
            "env": _mcp_env_dict(params.get("env"), data.get("env")),
            "url": "",
            "headers": {},
        }
    headers = _mcp_headers(params)
    return {
        "name": name,
        "type": "remote",
        "transport": str(params.get("transport") or data.get("transport") or "sse"),
        "command": "",
        "args": [],
        "env": {},
        "url": str(params.get("url") or data.get("url") or ""),
        "headers": headers,
    }


def _mcp_env_dict(params_env: Any, data_env: Any) -> dict:
    """params.env（优先）与 data.env 合并成 ``{name: value}`` 字典。

    - dict → 字符串化键值；
    - list → probe 的变量名列表，展开成 ``{name: ""}``（值由 params 提供，本层兜底空）。
    params.env 覆盖 data.env 同名键（团队私有值优先于池行占位）。
    """
    base: dict[str, str] = {}
    if isinstance(data_env, dict):
        base = {str(k): str(v) for k, v in data_env.items()}
    elif isinstance(data_env, list):
        base = {str(e).strip(): "" for e in data_env if str(e).strip()}
    if isinstance(params_env, dict):
        for k, v in params_env.items():
            base[str(k)] = str(v)
    elif isinstance(params_env, list):
        for e in params_env:
            if str(e).strip():
                base[str(e).strip()] = ""
    return base


def _mcp_headers(params: dict) -> dict:
    """params.headers + params.token → 节点头。token 组装成 ``Authorization: Bearer``
    （与旧 mcp_services 一致），已有 Authorization 不覆盖。"""
    headers = {str(k): str(v) for k, v in (params.get("headers") or {}).items()}
    token = str(params.get("token") or "").strip()
    if token and "Authorization" not in headers and "authorization" not in headers:
        headers["Authorization"] = f"Bearer {token}"
    return headers
