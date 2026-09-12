"""外部榜单候选池存储——统一资源池（``resources`` 表）实现。

替代 ``marketplace_leaderboard_store`` 的旧表（``marketplace_leaderboard_items``）
实现：榜单条目落 ``resources``（source_type=leaderboard_sync|manual），发布生命
周期用 ``status``（draft→published），MCP 启动方式/探针/技术栈等榜单专属数据放
``source_data``（上游真相）+ ``probe_data``（探针证据）。

对外 API 与旧 store 完全同名同签名（``upsert_item``/``list_items``/…），旧表不再
写入——调用方（leaderboard_sync / publisher / verify / routes / 前端）零改动切换。

字段映射（榜单行 ↔ resources 行）：

=========================  =====================================
榜单旧列                    resources 落点
=========================  =====================================
source                     source_data.source
board                      source_data.board
repo_full_name             name（=repo_full_name）+ source_data.repo_full_name
repo_url                   source_data.repo_url
description/stars/forks/…  source_data（上游真相投影）
target_module(s)（多值）    resource_type（**单选主类型**：derive_primary_type）
install_spec                resource_data（skill→github_clone 坐标 / plugin→download_url / prompt→content）
launch_spec(_status/_error) source_data.launch_spec / launch_spec_status / launch_spec_error
installable                source_data.installable
labels                     source_data.labels
upstream_rank/display_rank source_data.upstream_rank / sort_order
external_data              source_data（整个 external_data 原样内嵌）
stack / stack_tags         source_data.stack / stack_tags
categories / tags          source_data.categories / tags
status                     status（draft|published）
=========================  =====================================

分类单选化：旧 ``target_modules`` 多值数组收口成 ``resource_type`` 单值（探针在场
时用 ``derive_primary_type`` 判定；否则 board 推导：skills→skill / mcp→mcp /
prompts→prompt）。前端筛选/徽标随之单选。
"""
from __future__ import annotations

import json
from typing import Any

# 允许的资源类型（与 resource_store 对齐；skills=技能集合；unknown=仅浏览/未定类）。
_RESOURCE_TYPES = frozenset({"skills", "skill", "plugin", "mcp", "prompt", "unknown"})
# 榜单条目允许的来源（source_type 列）。
_SOURCE_TYPES = frozenset({"leaderboard_sync", "manual"})

_TIME_KEYS = ("published_at", "created_at", "updated_at")


def _pool():
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("postgres pool not initialized")
    return PostgresClient.pool


async def _get_resource_type(pool, item_id: int) -> str | None:
    """读当前行的 resource_type（patch 不改分类时，install_spec 拆包要用它）。"""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT resource_type FROM resources WHERE id=$1", item_id,
        )


# ── 纯工具（与旧 store 同名同口径，供外部继续 import）─────────────────────


def derive_target_modules(board: str, upstream_category: str = "") -> list[str]:
    """board/上游 category → 安装形态（**单选语义**：返回至多一项）。

    旧实现返回多值（skill+plugin 并存），新 schema 分类单选——这里收口成主类型：
    board 映射优先（skills→skill / mcp→mcp / prompts→prompt），上游 category 关键
    词只在 board 无映射时兜底。frameworks/research/manual 无对应形态回空。
    """
    board_map = {"mcp": "mcp", "skills": "skill", "prompts": "prompt"}
    if board in board_map:
        return [board_map[board]]
    category = str(upstream_category or "").lower()
    for keyword, module in (
        ("mcp", "mcp"), ("skill", "skill"), ("prompt", "prompt"),
        ("plugin", "plugin"), ("extension", "plugin"),
    ):
        if keyword in category:
            return [module]
    return []


def derive_target_module(board: str, upstream_category: str = "") -> str:
    mods = derive_target_modules(board, upstream_category)
    return mods[0] if mods else ""


def _normalize_skill_install_spec(spec):
    """install_spec.skill 收口成 entries 形态（与旧 store 同口径，幂等）。"""
    from user_platform.marketplace.leaderboard_probe import normalize_skill_install_spec

    if not isinstance(spec, dict):
        return {}
    return normalize_skill_install_spec(spec)


def _as_modules_list(value) -> list[str]:
    """多选分类数组收口（str JSON 兜底；单选语义下恒 ≤1 项）。"""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for m in value:
        s = str(m).strip()
        if s and s not in out:
            out.append(s)
    return out


def apply_overrides(data: dict) -> dict:
    """兼容保留：新表资源字段即最终值，admin_overrides 间接层已删。"""
    data.pop("admin_overrides", None)
    return data


# ── 榜单条目 ↔ resources 行映射 ────────────────────────────────────────────


def _primary_type(item: dict, probe: dict | None) -> str:
    """单选主类型：探针在场用 derive_primary_type（skills>plugin>skill>mcp>prompt）；
    否则 board/分类推导兜底。都推不出 → ``unknown``（仅浏览/未定类，如 frameworks/
    research/awesome 目录——有发现价值但无安装形态）。"""
    if probe:
        from user_platform.marketplace.leaderboard_probe import derive_primary_type

        t = derive_primary_type(probe, str(item.get("repo_full_name") or ""))
        if t:
            return t
    modules = item.get("target_modules")
    if isinstance(modules, list) and modules:
        first = str(modules[0]).strip()
        if first in _RESOURCE_TYPES:
            return first
    single = str(item.get("target_module") or "").strip()
    if single in _RESOURCE_TYPES:
        return single
    return "unknown"


def _item_to_resource_values(item: dict) -> dict:
    """榜单 item dict（同步/手动添加形态）→ resources 行值。"""
    external = item.get("external_data") if isinstance(item.get("external_data"), dict) else {}
    probe = external.get("probe") if isinstance(external.get("probe"), dict) else None
    source_type = "manual" if str(item.get("source") or "") == "manual" else "leaderboard_sync"
    full_name = str(item.get("repo_full_name") or "").strip()
    install_spec = item.get("install_spec") if isinstance(item.get("install_spec"), dict) else {}
    launch_spec = item.get("launch_spec") if isinstance(item.get("launch_spec"), dict) else {}
    resource_type = _primary_type(item, probe)
    resource_data = _resource_data_for_item(resource_type, install_spec, launch_spec)

    source_data = {
        "source": item.get("source") or "agent-leaderboard",
        "board": item.get("board") or "",
        "repo_full_name": full_name,
        "repo_url": item.get("repo_url") or "",
        # 上游投影缓存（列表/搜索用；真相在 external_data，这里全量内嵌）。
        "description": item.get("description") or "",
        "stars": int(item.get("stars") or 0),
        "forks": int(item.get("forks") or 0),
        "language": item.get("language") or "",
        "topics": item.get("topics") or [],
        "upstream_category": item.get("upstream_category") or "",
        "use_cases": item.get("use_cases") or [],
        "upstream_rank": item.get("upstream_rank"),
        "upstream_updated_at": _iso(item.get("upstream_updated_at")),
        "installable": bool(item.get("installable", True)),
        "labels": item.get("labels") or [],
        "categories": item.get("categories") or [],
        "tags": item.get("tags") or [],
        "publisher": item.get("publisher") or "",
        # 榜单专属草稿字段。
        "launch_spec": item.get("launch_spec") or {},
        "launch_spec_status": item.get("launch_spec_status") or "",
        "launch_spec_error": item.get("launch_spec_error") or "",
        "stack": item.get("stack") or {},
        "stack_tags": item.get("stack_tags") or [],
        "external_data": external,
    }

    return {
        "resource_type": resource_type,
        "resource_data": resource_data,
        "source_type": source_type,
        "source_data": source_data,
        "name": item.get("name") or full_name,
        "display_name": item.get("display_name") or item.get("name") or full_name,
        "description": item.get("description") or "",
        "version": item.get("version") or "",
        "status": "draft",
        "sort_order": item.get("sort_order") if item.get("sort_order") is not None else item.get("upstream_rank"),
        "probe_data": probe or {},
    }


def _iso(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _resource_data_for_item(resource_type: str, install_spec: dict, launch_spec: dict) -> dict:
    """旧榜单 install_spec（按模块包裹）→ 新 resources.resource_data（按类型直接存）。

    plugin 是容器：榜单派生时 ``install_spec.plugin``（marketplace.json 仓库）与
    ``install_spec.skill.entries``（多技能仓库，原 skills 集合）都归 plugin——
    entries 在场即容器形态（resource_data 同时带 clone 坐标与 download_url）。
    """
    if resource_type in ("skill", "skills"):
        value = install_spec.get("skill") if isinstance(install_spec.get("skill"), dict) else install_spec
        return dict(value or {})
    if resource_type == "plugin":
        if isinstance(install_spec.get("skill"), dict) and isinstance(
            (install_spec["skill"] or {}).get("entries"), list
        ) and install_spec["skill"]["entries"]:
            # 多技能插件容器（原 skills 集合）：entries + clone 坐标 + zip 地址。
            skill = install_spec["skill"]
            plugin = install_spec.get("plugin") if isinstance(install_spec.get("plugin"), dict) else {}
            data = dict(skill)
            data["download_url"] = str(
                plugin.get("download_url") or ""
            ) or data.get("download_url") or ""
            return data
        value = install_spec.get("plugin") if isinstance(install_spec.get("plugin"), dict) else install_spec
        return dict(value or {})
    if resource_type == "prompt":
        value = install_spec.get("prompt") if isinstance(install_spec.get("prompt"), dict) else install_spec
        return dict(value or {})
    if resource_type == "mcp":
        return dict(launch_spec or {})
    return dict(install_spec or {})


def _install_spec_for_item(resource_type: str, resource_data: dict) -> dict:
    """新 resources.resource_data → 旧榜单 API/前端期望的包裹 install_spec。

    兼容修 bug 前的坏行：update_curation 曾把包裹形态 ``{"skill": {...}}`` 原样
    落进 resource_data，读回来会再包一层（前端 entries 读不到）。这里发现双层
    壳就地剥掉——坏行读侧自动恢复，无需人工修库。
    """
    if not isinstance(resource_data, dict):
        return {}
    if resource_type in ("skill", "skills"):
        data = dict(resource_data)
        # 剥旧坏行的双层壳：{"skill": {...}} / {"plugin": ...} / {"prompt": ...}。
        inner = next((data[k] for k in ("skill", "plugin", "prompt") if isinstance(data.get(k), dict)), None)
        if isinstance(inner, dict) and any(k in inner for k in ("entries", "install_method", "download_url", "content", "path", "ref")):
            data = dict(inner)
        return {"skill": data}
    if resource_type == "plugin":
        data = dict(resource_data)
        if isinstance(data.get("plugin"), dict):
            data = dict(data["plugin"])
        return {"plugin": data}
    if resource_type == "prompt":
        data = dict(resource_data)
        if isinstance(data.get("prompt"), dict):
            data = dict(data["prompt"])
        return {"prompt": data}
    return {}


def _association_for(resource_type: str, resource_data: dict) -> list[str]:
    """从容器 resource_data.entries 得搜索用的关联子项（plugin 容器与存量 skills）。"""
    if resource_type not in ("plugin", "skills") or not isinstance(resource_data, dict):
        return []
    entries = resource_data.get("entries")
    if not isinstance(entries, list):
        return []
    return [str(e.get("name") or "") for e in entries
            if isinstance(e, dict) and str(e.get("name") or "").strip()]


def _editors_for(resource_type: str, resource_data: dict) -> list[str]:
    """从 entries/provider 推导 resources.editors，并集。

    plugin：适用客户端多选落 ``resource_data.editors``（数组）；容器插件
    （entries 在场）按子技能 editors 并集；旧单值形态回落 ``provider`` 包成
    数组——读写两侧都兼容。
    """
    if resource_type in ("skill", "skills"):
        entries = resource_data.get("entries") if resource_type == "skills" else [resource_data]
        out: list[str] = []
        for entry in entries or []:
            for editor in (entry.get("editors") or []):
                if str(editor) not in out:
                    out.append(str(editor))
        return out
    if resource_type == "plugin" and isinstance(resource_data.get("entries"), list):
        out: list[str] = []
        for entry in resource_data["entries"]:
            if not isinstance(entry, dict):
                continue
            for editor in (entry.get("editors") or []):
                if str(editor) not in out:
                    out.append(str(editor))
        if out:
            return out
    editors = resource_data.get("editors")
    if isinstance(editors, list) and editors:
        out = [str(e) for e in editors if str(e).strip()]
        if out:
            return out
    provider = str(resource_data.get("provider") or "")
    return [provider] if provider else []


def _resource_to_item(row: dict) -> dict:
    """resources 行 → 旧榜单 item 形态（调用方/前端契约不变）。"""
    sd = row.get("source_data") or {}
    external = sd.get("external_data") or {}
    item = {
        "id": row["id"],
        "source": sd.get("source") or ("manual" if row.get("source_type") == "manual" else "agent-leaderboard"),
        "board": sd.get("board") or "",
        "repo_full_name": sd.get("repo_full_name") or row.get("name") or "",
        "repo_url": sd.get("repo_url") or "",
        "description": row.get("description") or "",
        "stars": int(sd.get("stars") or 0),
        "forks": int(sd.get("forks") or 0),
        "language": sd.get("language") or "",
        "topics": sd.get("topics") or [],
        "upstream_category": sd.get("upstream_category") or "",
        "use_cases": sd.get("use_cases") or [],
        "upstream_rank": sd.get("upstream_rank"),
        "upstream_updated_at": sd.get("upstream_updated_at"),
        # 分类单选：resource_type 回填旧 target_module(s)（≤1 项）；unknown=仅浏览
        # → 空分类，与前端"仅浏览"契约一致（modulesOf 空数组走 browse-only 渲染）。
        "target_module": (row.get("resource_type") or "") if row.get("resource_type") != "unknown" else "",
        "target_modules": ([row["resource_type"]] if row.get("resource_type") and row["resource_type"] != "unknown" else []),
        "installable": bool(sd.get("installable", True)),
        "labels": sd.get("labels") or [],
        "launch_spec": sd.get("launch_spec") or {},
        "launch_spec_status": sd.get("launch_spec_status") or "",
        "launch_spec_error": sd.get("launch_spec_error") or "",
        "install_spec": _install_spec_for_item(row.get("resource_type") or "", row.get("resource_data") or {}),
        "stack": sd.get("stack") or {},
        "stack_tags": sd.get("stack_tags") or [],
        "categories": sd.get("categories") or [],
        "tags": sd.get("tags") or [],
        "publisher": sd.get("publisher") or "",
        "name": row.get("name") or "",
        "display_name": row.get("display_name") or row.get("name") or "",
        "version": row.get("version") or "",
        "sort_order": row.get("sort_order"),
        "status": row.get("status") or "draft",
        "published_at": row.get("published_at"),
        "published_by": row.get("published_by") or "",
        "external_data": external,
    }
    for key in _TIME_KEYS:
        if item.get(key) is not None and not isinstance(item[key], str):
            item[key] = item[key].isoformat()
    return item


def _decode_row(row) -> dict:
    data = dict(row)
    for key in ("resource_data", "association", "editors", "source_data", "probe_data"):
        value = data.get(key)
        if isinstance(value, str):
            try:
                data[key] = json.loads(value)
            except (TypeError, ValueError):
                data[key] = {} if key != "association" and key != "editors" else []
    return data


def _load_rows(rows) -> list[dict]:
    return [_resource_to_item(_decode_row(r)) for r in rows]


# ── 对外 API（与旧 store 同名同签名）──────────────────────────────────────


async def upsert_item(
    item: dict,
    *,
    overwrite_published: bool = False,
    overwrite_draft: bool = False,
) -> dict | None:
    """同步/手动添加一条候选（resources upsert，按 (source_type, repo) 去重）。

    与旧表 upsert 同语义：新条目 INSERT 完整草稿；已存在只刷新 source_data（上游
    真相 + 投影），资源字段（name/display/分类/install_spec）不动——管理员改过的
    不被同步冲掉。probe 保留规则同旧：传入 external_data 带 probe 键 → 新值；不带
    → 保留行里旧 probe（不丢探针证据）。

    覆盖模式（手动「立即同步」弹框勾选；定时同步不传恒为不覆盖）：
    - ``overwrite_draft`` / ``overwrite_published``：按行当前状态命中才覆盖，
      hidden 行不覆盖（管理员显式藏的）。覆盖 = 资源字段跟上游最新走
      （name/display/description/version 直接写列）；resource_type/resource_data
      **仅在本轮带新鲜识别**时才覆盖——榜单源看 external_data.probe（探针复用/
      预算跳过的行 incoming install_spec 是关键词分类的默认形态（无 entries），
      直接写列会把行里探针派生的安装配置降级成空壳）；无探针源（agentscope /
      skillhub / agency-agents，install_spec 每轮从上游确定性派生）由调用方在
      item 顶层带 ``install_spec_fresh=True`` 标记。状态（draft/published）
      永远不翻——覆盖是刷新数据，不是隐式发布/撤回。
    """
    full_name = str(item.get("repo_full_name") or "").strip()
    if not full_name or "/" not in full_name:
        return None
    values = _item_to_resource_values(item)
    external = values["source_data"]["external_data"]
    # 无探针源的安装配置每轮都是上游权威值（skillhub/agentscope 从当轮 zip 的
    # SKILL.md 派生、agency-agents 从当轮 tarball 派生），不存在榜单源「探针复用
    # →默认空壳 spec」的降级问题——install_spec_fresh=True 的行覆盖时一并重写。
    spec_fresh = "probe" in (external or {}) or item.get("install_spec_fresh") is True
    source_data = values["source_data"]
    pool = _pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            """
            SELECT id, source_data, probe_data, status, resource_type, resource_data,
                   name, display_name, description, version, sort_order
            FROM resources
            WHERE source_type = $1 AND source_data->>'repo_full_name' = $2
            ORDER BY id ASC LIMIT 1
            """,
            values["source_type"], full_name,
        )
        if existing is None:
            row = await conn.fetchrow(
                """
                INSERT INTO resources (
                    resource_type, resource_data, association, editors,
                    source_type, source_data, name, display_name, description,
                    version, status, sort_order, probe_data
                ) VALUES (
                    $1, $2::jsonb, $3::jsonb, $4::jsonb,
                    $5, $6::jsonb, $7, $8, $9, $10, 'draft', $11, $12::jsonb
                ) RETURNING *
                """,
                values["resource_type"] or "unknown",
                json.dumps(values["resource_data"], ensure_ascii=False),
                json.dumps(_association_for(values["resource_type"] or "", values["resource_data"]), ensure_ascii=False),
                json.dumps(_editors_for(values["resource_type"] or "", values["resource_data"]), ensure_ascii=False),
                values["source_type"],
                json.dumps(source_data, ensure_ascii=False),
                values["name"], values["display_name"], values["description"],
                values["version"],
                values["sort_order"],
                json.dumps(values["probe_data"], ensure_ascii=False),
            )
            return _resource_to_item(_decode_row(row)) if row else None

        # 已存在：默认只刷新 source_data；probe 不带键则保留旧值；资源字段不动。
        old_sd = existing["source_data"]
        if isinstance(old_sd, str):
            try:
                old_sd = json.loads(old_sd)
            except (TypeError, ValueError):
                old_sd = {}
        old_probe = existing["probe_data"]
        if isinstance(old_probe, str):
            try:
                old_probe = json.loads(old_probe)
            except (TypeError, ValueError):
                old_probe = {}
        carry_probe = "probe" not in (external or {})
        merged_sd = {**source_data, "external_data": external or {}}
        new_probe = old_probe if carry_probe else values["probe_data"]
        # 覆盖模式命中判定：行状态 × 调用方勾选（hidden 不覆盖）。
        row_status = str(existing["status"] or "")
        apply_overwrite = (
            (overwrite_draft and row_status == "draft")
            or (overwrite_published and row_status == "published")
        )
        if apply_overwrite:
            sets = ["source_data = $2::jsonb", "probe_data = $3::jsonb",
                    "sort_order = COALESCE($4, sort_order)"]
            args = [existing["id"],
                    json.dumps(merged_sd, ensure_ascii=False),
                    json.dumps(new_probe or {}, ensure_ascii=False),
                    values["sort_order"]]
            # 文本资源字段：上游描述/版本直接覆盖（管理员手改的描述按覆盖语义让位）。
            for col in ("name", "display_name", "description", "version"):
                args.append(values[col])
                sets.append(f"{col} = ${len(args)}")
            # 分类 + 安装配置：仅本轮带新鲜识别才覆盖（榜单源=探针在场；无探针源=
            # install_spec_fresh 标记——见 docstring）。派生列 association/editors
            # 随 resource_data 一并刷新，否则列表/搜索仍显示旧集合。
            if spec_fresh:
                res_type = values["resource_type"] or "unknown"
                res_data = values["resource_data"] or {}
                args.append(res_type)
                sets.append(f"resource_type = ${len(args)}")
                args.append(json.dumps(res_data, ensure_ascii=False))
                sets.append(f"resource_data = ${len(args)}::jsonb")
                args.append(json.dumps(_association_for(res_type, res_data), ensure_ascii=False))
                sets.append(f"association = ${len(args)}::jsonb")
                args.append(json.dumps(_editors_for(res_type, res_data), ensure_ascii=False))
                sets.append(f"editors = ${len(args)}::jsonb")
            sets.append("updated_at = now()")
            row = await conn.fetchrow(
                f"UPDATE resources SET {', '.join(sets)} WHERE id = $1 RETURNING *",
                *args,
            )
        else:
            row = await conn.fetchrow(
                """
                UPDATE resources
                   SET source_data = $2::jsonb,
                       probe_data = $3::jsonb,
                       sort_order = COALESCE($4, sort_order),
                       updated_at = now()
                 WHERE id = $1
                RETURNING *
                """,
                existing["id"],
                json.dumps(merged_sd, ensure_ascii=False),
                json.dumps(new_probe or {}, ensure_ascii=False),
                values["sort_order"],
            )
        return _resource_to_item(_decode_row(row)) if row else None


async def load_probe_reuse_hints() -> dict[tuple[str, str, str], dict]:
    """探针复用轻量投影（同旧口径；读 resources.probe_data）。"""
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT source_data->>'source' AS source,
                   source_data->>'board' AS board,
                   source_data->>'repo_full_name' AS repo_full_name,
                   probe_data->>'error' AS probe_error,
                   probe_data->>'fetched_at' AS probe_fetched_at,
                   source_data->>'upstream_updated_at' AS upstream_updated_at,
                   source_data->'stack' AS stack,
                   source_data->'stack_tags' AS stack_tags
            FROM resources
            WHERE source_type IN ('leaderboard_sync', 'manual')
            """
        )
    out: dict[tuple[str, str, str], dict] = {}
    for row in rows:
        def _loads(value, default):
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (TypeError, ValueError):
                    return default
            return value if value is not None else default

        out[(row["source"], row["board"], row["repo_full_name"])] = {
            "probe_error": row["probe_error"],
            "fetched_at": row["probe_fetched_at"],
            "upstream_updated_at": row["upstream_updated_at"],
            "stack": _loads(row["stack"], {}) or {},
            "stack_tags": _loads(row["stack_tags"], []) or [],
        }
    return out


async def list_items(
    *,
    board: str | None = None,
    status: str | None = None,
    source: str | None = None,
    target_module: str | None = None,
    installable: bool | None = None,
    launch_status: str | None = None,
    stack_tag: str | None = None,
    query: str = "",
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    pool = _pool()
    where = ["source_type IN ('leaderboard_sync', 'manual')"]
    args: list[Any] = []
    if board:
        args.append(board)
        where.append(f"source_data->>'board' = ${len(args)}")
    if status:
        args.append(status)
        where.append(f"status = ${len(args)}")
    if source:
        args.append(source)
        where.append(f"source_data->>'source' = ${len(args)}")
    if target_module:
        args.append(target_module)
        where.append(f"resource_type = ${len(args)}")
    if stack_tag:
        args.append(json.dumps([str(stack_tag)]))
        where.append(f"source_data->'stack_tags' @> ${len(args)}::jsonb")
    if installable is not None:
        args.append(installable)
        where.append(f"(source_data->>'installable')::boolean = ${len(args)}")
    if launch_status == "unfilled":
        where.append("source_data->>'launch_spec_status' IN ('', 'failed')")
    elif launch_status in ("pending", "filled", "failed", "verified"):
        args.append(launch_status)
        where.append(f"source_data->>'launch_spec_status' = ${len(args)}")
    if query.strip():
        args.append(f"%{query.strip().lower()}%")
        # 同消费侧：子技能名命中技能集行（association 展平子串）。
        where.append(
            f"(lower(name) LIKE ${len(args)} OR lower(description) LIKE ${len(args)}"
            f" OR lower(association::text) LIKE ${len(args)})"
        )
    args.extend([max(1, min(1000, limit)), max(0, offset)])
    sql = (
        "SELECT * FROM resources"
        + f" WHERE {' AND '.join(where)}"
        + " ORDER BY sort_order ASC NULLS LAST, (source_data->>'stars')::int DESC NULLS LAST, id ASC"
        + f" LIMIT ${len(args) - 1} OFFSET ${len(args)}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return _load_rows(rows)


async def count_items(*, board: str | None = None, status: str | None = None) -> int:
    pool = _pool()
    where = ["source_type IN ('leaderboard_sync', 'manual')"]
    args: list[Any] = []
    if board:
        args.append(board)
        where.append(f"source_data->>'board' = ${len(args)}")
    if status:
        args.append(status)
        where.append(f"status = ${len(args)}")
    async with pool.acquire() as conn:
        return int(await conn.fetchval(
            "SELECT count(*) FROM resources" + (f" WHERE {' AND '.join(where)}" if where else ""),
            *args,
        ) or 0)


async def list_published_for_consumer(
    *,
    target_module: str | None = None,
    board: str | None = None,
    installable: bool | None = None,
    stack_tag: str | None = None,
    query: str = "",
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    pool = _pool()
    where = ["status = 'published'", "source_type IN ('leaderboard_sync', 'manual')"]
    args: list[Any] = []
    if target_module:
        # 消费侧 Skill tab 混排：单 skill + 存量 skills 集合 + plugin 容器（带
        # entries 的插件，子技能在 association 里搜/勾）——前端只发 skill，这里
        # 扩成三值。纯 zip 插件（无 entries）不混进——那是插件 tab 的整包。
        # admin 侧分类筛选走 list_items 的精确匹配，不受影响。
        if target_module == "skill":
            where.append(
                "(resource_type IN ('skill', 'skills')"
                " OR (resource_type = 'plugin' AND resource_data ? 'entries'))"
            )
        else:
            args.append(target_module)
            where.append(f"resource_type = ${len(args)}")
    if stack_tag:
        args.append(json.dumps([str(stack_tag)]))
        where.append(f"source_data->'stack_tags' @> ${len(args)}::jsonb")
    if board:
        args.append(board)
        where.append(f"source_data->>'board' = ${len(args)}")
    if installable is not None:
        args.append(installable)
        where.append(f"(source_data->>'installable')::boolean = ${len(args)}")
    if query.strip():
        args.append(f"%{query.strip().lower()}%")
        # 搜子技能名（association JSONB 数组展平后子串匹配）：搜 "docx" 能命中
        # anthropics/skills 技能集这一行，不需要把集合拆成 N 行。
        where.append(
            f"(lower(name) LIKE ${len(args)} OR lower(description) LIKE ${len(args)}"
            f" OR lower(association::text) LIKE ${len(args)})"
        )
    args.extend([max(1, min(500, limit)), max(0, offset)])
    sql = (
        "SELECT * FROM resources"
        f" WHERE {' AND '.join(where)}"
        f" ORDER BY sort_order ASC NULLS LAST, (source_data->>'stars')::int DESC NULLS LAST, id ASC"
        f" LIMIT ${len(args) - 1} OFFSET ${len(args)}"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return _load_rows(rows)


async def get_item(item_id: int) -> dict | None:
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM resources WHERE id=$1 AND source_type IN ('leaderboard_sync','manual')",
            item_id,
        )
    return _resource_to_item(_decode_row(row)) if row else None


async def find_by_repo(repo_full_name: str) -> dict | None:
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM resources
            WHERE source_type IN ('leaderboard_sync','manual')
              AND lower(source_data->>'repo_full_name') = lower($1)
            ORDER BY id ASC LIMIT 1
            """,
            repo_full_name,
        )
    return _resource_to_item(_decode_row(row)) if row else None


async def create_manual_item(item: dict) -> tuple[dict | None, bool]:
    repo = str(item.get("repo_full_name") or "").strip()
    existing = await find_by_repo(repo)
    if existing is not None:
        return existing, False
    row = await upsert_item(item)
    return row, True


async def update_curation(item_id: int, patch: dict) -> dict | None:
    """管理员收口（资源字段编辑 + 安装配置 + 状态直改）。

    列字段（resource_type/name/…/resource_data/sort_order）走 sets；榜单专属字段
    （categories/tags/installable/launch_spec）合并进 source_data，一次 UPDATE 落库。
    ``status`` 接受 draft|published|hidden——编辑弹框直接翻状态的捷径（与列表页
    发布队列/批量撤回并存）；非发布队列路径翻 published 只翻状态不进 GitHub 推送
    （榜单发布 worker 的市场镜像同步仍是独立动作）。
    """
    pool = _pool()
    sets: list[str] = []
    args: list[Any] = []
    source_data_patch: dict[str, Any] = {}
    resource_type_for_update: str | None = None
    def _set_col(column: str, value, jsonb: bool = False) -> None:
        args.append(json.dumps(value, ensure_ascii=False) if jsonb else value)
        sets.append(f"{column} = ${len(args)}" + ("::jsonb" if jsonb else ""))

    if "target_modules" in patch or "target_module" in patch:
        raw = patch.get("target_modules") if "target_modules" in patch else None
        if isinstance(raw, list):
            modules = [str(m).strip() for m in raw if str(m).strip()]
        else:
            single = str(patch.get("target_module") or "").strip()
            modules = [single] if single else []
        invalid = [m for m in modules if m not in _RESOURCE_TYPES]
        if invalid:
            raise ValueError(f"unsupported target_module: {', '.join(invalid)}")
        # 清空分类 = 仅浏览：落 unknown（CHECK 不允许空串）。
        resource_type_for_update = modules[0] if modules else "unknown"
        _set_col("resource_type", resource_type_for_update)
    if "categories" in patch:
        cats = patch.get("categories")
        if not isinstance(cats, list):
            raise ValueError("categories must be an array")
        source_data_patch["categories"] = list(dict.fromkeys(str(c).strip() for c in cats if str(c).strip()))
    if "tags" in patch:
        tag_list = patch.get("tags")
        if not isinstance(tag_list, list):
            raise ValueError("tags must be an array")
        source_data_patch["tags"] = list(dict.fromkeys(str(t).strip() for t in tag_list if str(t).strip()))
    for text_field in ("name", "display_name", "description", "version"):
        if text_field in patch:
            _set_col(text_field, str(patch.get(text_field) or "").strip())
    if "publisher" in patch:
        source_data_patch["publisher"] = str(patch.get("publisher") or "").strip()
    if "installable" in patch:
        source_data_patch["installable"] = bool(patch.get("installable"))
    if "launch_spec" in patch:
        spec = patch.get("launch_spec")
        if spec is not None and not isinstance(spec, dict):
            raise ValueError("launch_spec must be an object")
        kind = str((spec or {}).get("kind") or "none").strip().lower()
        if kind not in ("stdio", "remote", "none"):
            raise ValueError(f"unsupported launch_spec kind: {kind}")
        source_data_patch["launch_spec"] = spec or {}
        source_data_patch["launch_spec_status"] = "filled"
        source_data_patch["launch_spec_error"] = ""
    if "install_spec" in patch:
        spec = patch.get("install_spec")
        if spec is not None and not isinstance(spec, dict):
            raise ValueError("install_spec must be an object")
        # resource_data 存**拆包**形态（与 upsert_item/_resource_data_for_item 同口径）：
        # 调用方（编辑弹框/重新识别）送的是包裹形态 {"skill": {...entries}}，这里按
        # 生效类型拆掉外壳再落列——否则读侧 _install_spec_for_item 会再包一层成
        # {"skill": {"skill": ...}}，前端 entries 永远读不到（识别结果"没变化"的根因）。
        effective_type = resource_type_for_update
        if effective_type is None:
            existing_type = await _get_resource_type(pool, item_id)
            effective_type = existing_type or ""
        # skill 旧单对象形态先收口成 entries（normalize 接受包裹形态，幂等）。
        spec = _normalize_skill_install_spec(spec if isinstance(spec, dict) else {}) or {}
        resource_data = _resource_data_for_item(effective_type, spec, {})
        _set_col("resource_data", resource_data, jsonb=True)
        # 派生列随 resource_data 一并刷新：技能集的子技能列表（association，搜索用）
        # 与适用编辑器（editors）——否则列表/搜索侧仍显示旧集合内容。
        _set_col("association", _association_for(effective_type, resource_data), jsonb=True)
        _set_col("editors", _editors_for(effective_type, resource_data), jsonb=True)
    if "sort_order" in patch:
        raw = patch.get("sort_order")
        if raw is None or str(raw).strip() == "":
            _set_col("sort_order", None)
        else:
            try:
                sort_value = int(str(raw).strip())
            except (TypeError, ValueError):
                raise ValueError("排序需为非负整数") from None
            if sort_value < 0:
                raise ValueError("排序需为非负整数")
            _set_col("sort_order", sort_value)

    if source_data_patch:
        args.append(json.dumps(source_data_patch, ensure_ascii=False))
        sets.append(f"source_data = source_data || ${len(args)}::jsonb")

    if "status" in patch:        # 编辑弹框直接翻状态（与列表页发布队列/撤回并存的捷径）：
        # draft|published|hidden；published 时补 published_at（已发布过不重置）。
        new_status = str(patch.get("status") or "").strip()
        if new_status not in ("draft", "published", "hidden"):
            raise ValueError(f"unsupported status: {new_status}")
        _set_col("status", new_status)
        if new_status == "published":
            sets.append("published_at = COALESCE(published_at, now())")

    if not sets:
        return await get_item(item_id)
    args.append(item_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE resources SET {', '.join(sets)}, updated_at = now() "
            f"WHERE id = ${len(args)} AND source_type IN ('leaderboard_sync','manual') RETURNING *",
            *args,
        )
    result = _resource_to_item(_decode_row(row)) if row else None
    return result


async def _patch_sd_async(conn, item_id: int, patch: dict) -> None:
    """source_data 顶层键定点合并（set_launch_spec / merge_probe 等复用）。"""
    await conn.execute(
        """
        UPDATE resources
           SET source_data = source_data || $2::jsonb, updated_at = now()
         WHERE id = $1 AND source_type IN ('leaderboard_sync','manual')
        """,
        item_id, json.dumps(patch, ensure_ascii=False),
    )


async def set_launch_spec(item_id: int, *, spec: dict | None, status: str, error: str = "") -> dict | None:
    pool = _pool()
    async with pool.acquire() as conn:
        await _patch_sd_async(conn, item_id, {
            "launch_spec": spec or {}, "launch_spec_status": status, "launch_spec_error": error,
        })
        row = await conn.fetchrow(
            "SELECT * FROM resources WHERE id=$1 AND source_type IN ('leaderboard_sync','manual')",
            item_id,
        )
    return _resource_to_item(_decode_row(row)) if row else None


async def merge_external_data_probe(item_id: int, probe_patch: dict) -> dict | None:
    """定点合并 probe_data（verify 等写入），同步整替不丢——见 upsert 保留规则。"""
    pool = _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE resources
               SET probe_data = COALESCE(probe_data, '{}'::jsonb) || $2::jsonb,
                   updated_at = now()
             WHERE id = $1 AND source_type IN ('leaderboard_sync','manual')
            RETURNING *
            """,
            item_id, json.dumps(probe_patch or {}, ensure_ascii=False),
        )
    return _resource_to_item(_decode_row(row)) if row else None


def _launch_spec_publish_gate(item: dict, require_verified: bool) -> str | None:
    """MCP 可安装条目的 launch_spec 发布门禁（同旧口径；分类单选）。"""
    if not item.get("installable", True):
        return None
    if str(item.get("target_module") or "") != "mcp":
        return None
    kind = str((item.get("launch_spec") or {}).get("kind") or "none")
    if kind == "none":
        return None
    status = str(item.get("launch_spec_status") or "")
    if require_verified:
        if status != "verified":
            return "MCP 条目发布要求启动方式验证通过（verified）——点「验证」跑一次握手"
        return None
    if status not in ("filled", "verified"):
        return "MCP 条目发布前需要补启动方式（探测/识别/手填任一），或把启动方式留空（kind=none）"
    return None


async def publish_items(item_ids: list[int], *, operator: str, require_verified: bool = False) -> dict:
    """批量发布（draft→published）。门禁同旧：分类兜底 + MCP launch_spec 门禁。"""
    pool = _pool()
    published: list[int] = []
    skipped: list[int] = []
    failed: list[dict] = []
    async with pool.acquire() as conn:
        for item_id in item_ids:
            row = await conn.fetchrow(
                "SELECT * FROM resources WHERE id=$1 AND source_type IN ('leaderboard_sync','manual')",
                item_id,
            )
            if row is None:
                failed.append({"id": item_id, "error": "条目不存在"})
                continue
            item = _resource_to_item(_decode_row(row))
            if item["status"] == "published":
                skipped.append(item_id)
                continue
            primary = item["target_module"]
            if not primary and item["installable"]:
                # 空分类兜底：board/上游 category 推一次并落库。
                primary = derive_target_module(
                    item.get("board") or "", item.get("upstream_category") or ""
                )
                if not primary:
                    failed.append({
                        "id": item_id,
                        "error": "上游未给出安装形态，请在资源编辑弹框里勾选分类，或保持仅浏览",
                    })
                    continue
                await conn.execute(
                    "UPDATE resources SET resource_type=$2, updated_at=now() WHERE id=$1",
                    item_id, primary,
                )
            gate_err = _launch_spec_publish_gate(item, require_verified)
            if gate_err:
                failed.append({"id": item_id, "error": gate_err})
                continue
            await conn.execute(
                """
                UPDATE resources
                   SET status='published', published_at=now(), published_by=$2, updated_at=now()
                 WHERE id=$1
                """,
                item_id, operator,
            )
            published.append(item_id)
    return {"published": published, "skipped": skipped, "failed": failed}


async def unpublish_items(item_ids: list[int]) -> list[int]:
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE resources
               SET status='draft', published_at=NULL, published_by='', updated_at=now()
             WHERE id = ANY($1::bigint[]) AND status='published'
               AND source_type IN ('leaderboard_sync','manual')
            RETURNING id
            """,
            item_ids,
        )
    return [int(r["id"]) for r in rows]


async def enqueue_publish_jobs(item_ids: list[int], *, operator: str = "") -> tuple[list[int], list[int]]:
    """入队推送（outbox module='leaderboard'；与市场资源共用同一 job 表）。"""
    import marketplace_store

    enqueued: list[int] = []
    skipped: list[int] = []
    pool = _pool()
    async with pool.acquire() as conn:
        for item_id in item_ids:
            row = await conn.fetchrow(
                "SELECT id, status FROM resources WHERE id=$1 AND source_type IN ('leaderboard_sync','manual')",
                item_id,
            )
            if row is None or row["status"] == "published":
                skipped.append(item_id)
                continue
            await marketplace_store._enqueue(
                conn, "leaderboard", str(item_id), "publish", {"operator": operator}
            )
            enqueued.append(item_id)
    if enqueued:
        marketplace_store.notify_publisher()
    return enqueued, skipped


async def delete_items(item_ids: list[int]) -> list[int]:
    """硬删（管理端批量/单条删除）。发布过的行删掉后用户侧立即不可见。"""
    pool = _pool()
    if not item_ids:
        return []
    text_ids = [str(i) for i in item_ids]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM marketplace_publish_jobs"
                " WHERE module='leaderboard' AND status IN ('pending','pushing')"
                " AND item_id = ANY($1::text[])",
                text_ids,
            )
            rows = await conn.fetch(
                "DELETE FROM resources WHERE id = ANY($1::bigint[])"
                " AND source_type IN ('leaderboard_sync','manual') RETURNING id",
                item_ids,
            )
    return [int(r["id"]) for r in rows]


async def purge_all() -> int:
    """清空榜单候选池（只删 leaderboard_sync/manual 来源的 resources 行）。"""
    pool = _pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM resources WHERE source_type IN ('leaderboard_sync','manual')"
        )
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM marketplace_publish_jobs"
                " WHERE module='leaderboard' AND status IN ('pending','pushing')"
            )
            await conn.execute(
                "DELETE FROM resources WHERE source_type IN ('leaderboard_sync','manual')"
            )
    return int(count or 0)


async def list_pending_launch_spec(limit: int = 50) -> list[dict]:
    """待补启动方式的 MCP 草稿（同旧口径）。"""
    pool = _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM resources
            WHERE source_type IN ('leaderboard_sync','manual')
              AND resource_type = 'mcp'
              AND (source_data->>'installable')::boolean = true
              AND source_data->>'launch_spec_status' IN ('', 'failed')
            ORDER BY (source_data->>'stars')::int DESC NULLS LAST
            LIMIT $1
            """,
            max(1, min(200, limit)),
        )
    return _load_rows(rows)
