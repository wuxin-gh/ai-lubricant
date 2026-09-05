"""外部榜单候选池存储（Agent-Leaderboard 这类外部目录的同步落地层）。

外部榜单只提供「仓库元数据」（stars / 分类 / 描述），不是可安装清单：一条记录
的 ``repo_url`` 是仓库主页，既不是 MCP endpoint，也不是 skill 包，更不是提示词
正文。因此同步一律落 ``status='draft'``（用户不可见）当候选池，由管理员挑选、
必要时补齐启动方式，再**显式点击**转 ``published``。

不变量（与产品口径一一对应）：

* 同步只写 draft，从不发布。``upsert_item`` 永不改已发布行的 ``status``。
* agent 补全只写 ``launch_spec``（草稿字段），同样不动 ``status``。
* 发布必须人工触发。发布 = 对用户可见；``installable`` 决定有没有安装入口，
  两者是独立的两个轴（仅浏览条目也能发布，只是渲染成没有安装按钮的卡片）。
* draft 永久保留：不做 TTL 清理，翻旧候选随时可发布。

风格对齐 ``resource_mirror_store``：模块级函数 + 函数内延迟 import PostgresClient。
"""
from __future__ import annotations

import json
from typing import Any

_TIME_KEYS = ("first_synced_at", "last_synced_at", "published_at", "upstream_updated_at")
_JSON_KEYS = ("topics", "use_cases", "labels", "launch_spec", "admin_overrides",
              "target_modules", "install_spec", "external_data", "categories", "tags",
              "stack", "stack_tags")

# 我们市场只有这四种安装形态。外部榜单的 board 是「主题」，与之不等价：
# 一个 skills 榜的仓库可能是插件包，一个 mcp 榜的仓库可能只是 awesome 目录。
TARGET_MODULES = frozenset({"mcp", "skill", "prompt", "plugin"})

# board（上游主题榜）→ 我们的安装形态。同步预归类与发布兜底共用这一份，避免两处漂移。
# frameworks / research 不在表里：开发库与研究程序在四类里没有对应安装形态。
BOARD_TO_MODULE = {
    "mcp": "mcp",
    "skills": "skill",
    "prompts": "prompt",
}

# 上游 category（Agent-Leaderboard 的正则分类）里出现这些词就直接采信。比 board 更细
# （board 是整张榜的主题，category 是这一条的），所以优先级更高。
_CATEGORY_HINTS = (
    ("mcp", "mcp"),
    ("skill", "skill"),
    ("prompt", "prompt"),
    ("plugin", "plugin"),
    ("extension", "plugin"),
)


def derive_target_modules(board: str, upstream_category: str = "") -> list[str]:
    """从上游事实推出安装形态（**多选**）。推不出来回空列表。

    口径：**榜单类型必选，上游 category 只负责追加**。Skills 榜的条目至少勾上
    Skill（榜单本身就是类型声明），上游 category 里再识别出别的形态（比如该仓库
    同时是个插件包）就并进去——所以「Skills 榜 + category 含 plugin」= Skill+插件，
    而不是只剩插件。同步预填默认分类、发布兜底、编辑弹框的逐项「同步」按钮
    都走这一份：两处各写必然漂移。
    """
    # 榜单类型打底（skills→skill 等）；frameworks/research/manual 无对应形态则空。
    primary = BOARD_TO_MODULE.get(str(board or "").strip().lower(), "")
    modules = [primary] if primary else []
    # 上游 category 关键词追加（保序去重）。
    category = str(upstream_category or "").strip().lower()
    hits = [module for keyword, module in _CATEGORY_HINTS if keyword in category]
    modules.extend(hits)
    return list(dict.fromkeys(modules))


def derive_target_module(board: str, upstream_category: str = "") -> str:
    """单值兼容包装：取多选推导的第一个（空列表回空串）。"""
    modules = derive_target_modules(board, upstream_category)
    return modules[0] if modules else ""

# 运营标签。前端据此筛选与提示，不参与安装逻辑。
LABEL_DIRECTORY_LIST = "directory_list"        # awesome 目录：本身不是可装物
LABEL_NEEDS_CREDENTIALS = "needs_credentials"  # 真 server，但要先配凭据
LABEL_NEEDS_LAUNCH_SPEC = "needs_launch_spec"  # MCP 启动方式待补

# 管理员可覆盖的上游展示字段。**不含 repo_full_name / repo_url**：这两个是条目的身份
# （唯一键的一半 + 用户点开要去的真实地址），改了就等于换了个仓库，也会让下次同步
# 认不出这一行而插入重复条目。
#
# display_name / publisher 没有对应的原列——榜单行只有仓库事实（repo_full_name 等），
# 这两个是管理员在「资源编辑」里起的资源身份。空覆盖=删键，读取侧就回落
# repo 短名 / 仓库 owner 的默认推导，不另设列。
OVERRIDABLE_TEXT_FIELDS = ("description", "language", "upstream_category", "display_name", "publisher")
OVERRIDABLE_LIST_FIELDS = ("topics", "use_cases")
OVERRIDABLE_FIELDS = frozenset(OVERRIDABLE_TEXT_FIELDS + OVERRIDABLE_LIST_FIELDS)


def apply_overrides(data: dict) -> dict:
    """把 ``admin_overrides`` 盖在上游原值上。

    读取侧统一走这里，于是「管理员改过的字段一直是改过的值，没改的继续跟着上游
    同步更新」。原值保留在各自列里没被破坏，清掉覆盖即可回到上游值。
    """
    overrides = data.get("admin_overrides")
    if not isinstance(overrides, dict) or not overrides:
        return data
    for key, value in overrides.items():
        if key in OVERRIDABLE_FIELDS:
            data[key] = value
    return data


def _normalize_skill_install_spec(spec):
    """install_spec.skill 收口成 entries 形态——委托 monkeycode_compat 探针模块的唯一实现。

    store 保持零模块级依赖（风格对齐 resource_mirror_store），这里函数内晚 import；
    monkeycode_compat 不可用时退化为原样返回（读侧降级不炸，写侧同样安全）。
    """
    if not isinstance(spec, dict):
        return {} if spec is None else spec
    try:
        from monkeycode_compat.marketplace.leaderboard_probe import normalize_skill_install_spec
    except Exception:  # noqa: BLE001 — 导入失败时旧行照读，不挡消费侧
        return spec
    return normalize_skill_install_spec(spec)


def row_to_dict(row) -> dict:
    data = dict(row)
    for key in _TIME_KEYS:
        if data.get(key) is not None:
            data[key] = data[key].isoformat()
    # asyncpg 回 JSONB 为 str，前端要结构化值。
    for key in _JSON_KEYS:
        value = data.get(key)
        if isinstance(value, str):
            try:
                data[key] = json.loads(value)
            except (TypeError, ValueError):
                data[key] = {} if key in ("launch_spec", "admin_overrides", "install_spec", "external_data") else []
    # install_spec.skill 收口成 entries 形态（旧单对象读侧即升级）；单一读口，admin/消费侧都规范化。
    if "install_spec" in data:
        data["install_spec"] = _normalize_skill_install_spec(data.get("install_spec"))
    return apply_overrides(data)


async def upsert_item(item: dict) -> dict | None:
    """同步/手动添加一条候选。**external_data 是唯一上游真相**：

    - INSERT（新条目）：external_data 落库 + 据 external_data 派生的资源字段
      （名称/显示名/摘要/发布者/版本/子分类/标签/分类/排序/安装参数）一并写入，
      status=draft——同步即完整草稿。
    - ON CONFLICT（已存在）：**只更新 external_data**，资源字段一律不动——
      管理员改没改过都不碰。上游变化对已入池资源的影响只能通过编辑弹框里
      各字段旁的「同步」按钮逐项主动拉取。

    旧上游列（description/stars/topics/use_cases 等）仍随 INSERT 写入、随
    CONFLICT 刷新，保持列表/搜索可用——它们只是 external_data 的投影缓存。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    raw_rank = item.get("upstream_rank")
    rank = int(raw_rank) if raw_rank is not None else None
    modules = item.get("target_modules")
    if not isinstance(modules, list):
        modules = [item.get("target_module") or ""] if item.get("target_module") else []
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO marketplace_leaderboard_items (
                source, board, repo_full_name, repo_url, description, stars, forks,
                language, topics, upstream_category, use_cases,
                name, display_name, publisher, version, categories, tags,
                target_module, target_modules, install_spec, external_data,
                stack, stack_tags,
                installable, labels, upstream_rank, display_rank, sort_order,
                upstream_updated_at
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::jsonb,
                $12,$13,$14,$15,$16::jsonb,$17::jsonb,
                $18,$19::jsonb,$20::jsonb,$21::jsonb,$22::jsonb,$23::jsonb,
                $24,$25::jsonb,$26,$27,$28,$29
            )
            ON CONFLICT (source, board, repo_full_name) DO UPDATE SET
                external_data = EXCLUDED.external_data,
                stack = EXCLUDED.stack,
                stack_tags = EXCLUDED.stack_tags,
                repo_url = EXCLUDED.repo_url,
                stars = EXCLUDED.stars,
                forks = EXCLUDED.forks,
                upstream_updated_at = EXCLUDED.upstream_updated_at,
                last_synced_at = now()
            RETURNING *
            """,
            item.get("source") or "agent-leaderboard",
            item["board"],
            item["repo_full_name"],
            item.get("repo_url") or "",
            item.get("description") or "",
            int(item.get("stars") or 0),
            int(item.get("forks") or 0),
            item.get("language") or "",
            json.dumps(item.get("topics") or []),
            item.get("upstream_category") or "",
            json.dumps(item.get("use_cases") or []),
            item.get("name") or "",
            item.get("display_name") or "",
            item.get("publisher") or "",
            item.get("version") or "",
            json.dumps(item.get("categories") or []),
            json.dumps(item.get("tags") or []),
            modules[0] if modules else "",
            json.dumps(modules),
            json.dumps(item.get("install_spec") or {}),
            json.dumps(item.get("external_data") or {}),
            json.dumps(item.get("stack") or {}),
            json.dumps(item.get("stack_tags") or []),
            bool(item.get("installable", True)),
            json.dumps(item.get("labels") or []),
            rank,
            rank,
            item.get("sort_order") if item.get("sort_order") is not None else rank,
            item.get("upstream_updated_at"),
        )
    return row_to_dict(row) if row else None


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
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    where: list[str] = []
    args: list[Any] = []
    if board:
        args.append(board)
        where.append(f"board = ${len(args)}")
    if status:
        args.append(status)
        where.append(f"status = ${len(args)}")
    if source:
        # 来源筛选：agent-leaderboard（榜单同步）vs manual（手动添加）
        args.append(source)
        where.append(f"source = ${len(args)}")
    if target_module:
        # 分类是多选数组：命中即算（一条既是 mcp 又是 skill 的项在两个模块 tab 都该出现）
        args.append(json.dumps([str(target_module)]))
        where.append(f"target_modules @> ${len(args)}::jsonb")
    if stack_tag:
        # 技术栈 tag 命中即算（stack_tags 是扁平小写数组，GIN 索引支撑）。
        args.append(json.dumps([str(stack_tag)]))
        where.append(f"stack_tags @> ${len(args)}::jsonb")
    if installable is not None:
        args.append(installable)
        where.append(f"installable = ${len(args)}")
    if launch_status == "unfilled":
        # 「待补」= 没补过或上次失败（可重试）；pending 是 agent 正在跑
        where.append("launch_spec_status IN ('', 'failed')")
    elif launch_status in ("pending", "filled", "failed", "verified"):
        args.append(launch_status)
        where.append(f"launch_spec_status = ${len(args)}")
    if query.strip():
        args.append(f"%{query.strip().lower()}%")
        where.append(f"(lower(repo_full_name) LIKE ${len(args)} OR lower(description) LIKE ${len(args)})")
    args.extend([max(1, min(1000, limit)), max(0, offset)])
    sql = (
        "SELECT * FROM marketplace_leaderboard_items"
        + (f" WHERE {' AND '.join(where)}" if where else "")
        # 排序（sort_order，资源中心索引）优先于热度；没排序的排最后再看 stars。
        + " ORDER BY sort_order ASC NULLS LAST, stars DESC, id ASC"
        + f" LIMIT ${len(args) - 1} OFFSET ${len(args)}"
    )
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [row_to_dict(r) for r in rows]


async def count_items(*, board: str | None = None, status: str | None = None) -> int:
    from db import PostgresClient

    if not PostgresClient.pool:
        return 0
    where: list[str] = []
    args: list[Any] = []
    if board:
        args.append(board)
        where.append(f"board = ${len(args)}")
    if status:
        args.append(status)
        where.append(f"status = ${len(args)}")
    sql = "SELECT count(*) FROM marketplace_leaderboard_items" + (
        f" WHERE {' AND '.join(where)}" if where else ""
    )
    async with PostgresClient.pool.acquire() as conn:
        return int(await conn.fetchval(sql, *args) or 0)


# 用户侧只允许看到 published。这里不暴露 status/installable 的原值，只投影成
# 用户卡字段，并在「仅浏览」条目上去掉安装入口——避免把候选池的草稿状态或内部
# 归类标签泄漏给前端。admin_overrides 只为让 apply_overrides 盖出管理员改过的
# description / display_name 等，盖完在函数末尾删掉该键。
_CONSUMER_FIELDS = (
    "id, source, board, repo_full_name, repo_url, description, stars, forks, language, "
    "name, display_name, publisher, version, categories, tags, "
    "target_module, target_modules, installable, "
    "launch_spec, install_spec, "
    "stack, stack_tags, "
    "sort_order, upstream_updated_at, published_at"
)


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
    """消费侧发现视图：只取 published。``status`` 在这里被钉死为 published,
    调用方无法绕过——草稿永远不可能从这里漏出去。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    where: list[str] = ["status = 'published'"]
    args: list[Any] = []
    if target_module:
        # 与管理端同口径：多选数组命中即算
        args.append(json.dumps([str(target_module)]))
        where.append(f"target_modules @> ${len(args)}::jsonb")
    if stack_tag:
        # 与管理端同口径：技术栈 tag 命中即算
        args.append(json.dumps([str(stack_tag)]))
        where.append(f"stack_tags @> ${len(args)}::jsonb")
    if board:
        args.append(board)
        where.append(f"board = ${len(args)}")
    if installable is not None:
        args.append(installable)
        where.append(f"installable = ${len(args)}")
    if query.strip():
        args.append(f"%{query.strip().lower()}%")
        where.append(
            f"(lower(repo_full_name) LIKE ${len(args)} OR lower(description) LIKE ${len(args)})"
        )
    args.extend([max(1, min(500, limit)), max(0, offset)])
    sql = (
        f"SELECT {_CONSUMER_FIELDS} FROM marketplace_leaderboard_items"
        f" WHERE {' AND '.join(where)}"
        # 与管理端 list_items 同一口径：sort_order（资源中心索引）优先于热度。
        f" ORDER BY sort_order ASC NULLS LAST, stars DESC, id ASC LIMIT ${len(args) - 1} OFFSET ${len(args)}"
    )
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [row_to_dict(r) for r in rows]


async def get_item(item_id: int) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM marketplace_leaderboard_items WHERE id=$1", item_id)
    return row_to_dict(row) if row else None


async def find_by_repo(repo_full_name: str) -> dict | None:
    """按 owner/repo 查任意来源的既有条目（手动添加防跨来源重复）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM marketplace_leaderboard_items WHERE lower(repo_full_name)=lower($1) "
            "ORDER BY id ASC LIMIT 1",
            repo_full_name,
        )
    return row_to_dict(row) if row else None


async def create_manual_item(item: dict) -> tuple[dict | None, bool]:
    """手动添加一条 GitHub 草稿。返回 (row, created)。

    跨来源去重：同仓库已在任意榜单里就返回既有行，让前端直接打开编辑，而不是
    再插一条 source=manual 的重复卡片。新行仍走 upsert_item，字段形态与同步条目一致。
    """
    repo = str(item.get("repo_full_name") or "").strip()
    existing = await find_by_repo(repo)
    if existing is not None:
        return existing, False
    row = await upsert_item(item)
    return row, True


async def update_curation(item_id: int, patch: dict, *, require_verified: bool = False) -> dict | None:
    """管理员收口：资源字段编辑（与市场资源同一套）+ 安装配置 + 状态。

    字段直接写列：同步遇到已存在条目时只更新 external_data，不碰资源字段，
    所以不再需要 admin_overrides 间接层（列即最终值）。

    ``status`` 支持 draft/published/hidden：选 published 时执行与批量发布相同
    的分类校验/兜底（installable 且无分类 → 按 external_data 的 board/上游分类
    推导；仍推不出 → 报错）。hidden = 下架但保留（消费侧只读 published）。

    ``require_verified`` 透传给发布兜底的 MCP launch_spec 门禁（软/硬）。

    ``sort_order`` 是资源中心排序索引：数字 = 固定位置；null/空 = 清掉（排最后按热度）。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = []
    args: list[Any] = []
    if "target_modules" in patch or "target_module" in patch:
        # 分类是多选。target_module（单值）作为旧客户端兼容入口：收到就折成一项数组。
        if "target_modules" in patch:
            raw = patch.get("target_modules")
            if not isinstance(raw, list):
                raise ValueError("target_modules must be an array")
            modules = [str(m).strip() for m in raw if str(m).strip()]
        else:
            single = str(patch.get("target_module") or "").strip()
            modules = [single] if single else []
        invalid = [m for m in modules if m not in TARGET_MODULES]
        if invalid:
            raise ValueError(f"unsupported target_module: {', '.join(invalid)}")
        modules = list(dict.fromkeys(modules))
        args.append(json.dumps(modules))
        sets.append(f"target_modules = ${len(args)}::jsonb")
        # 主分类（数组第一个）同步进旧列，老查询（launch agent 等）不用改口径
        args.append(modules[0] if modules else "")
        sets.append(f"target_module = ${len(args)}")
    if "categories" in patch:
        cats = patch.get("categories")
        if not isinstance(cats, list):
            raise ValueError("categories must be an array")
        cats = list(dict.fromkeys(str(c).strip() for c in cats if str(c).strip()))
        args.append(json.dumps(cats))
        sets.append(f"categories = ${len(args)}::jsonb")
    if "tags" in patch:
        tag_list = patch.get("tags")
        if not isinstance(tag_list, list):
            raise ValueError("tags must be an array")
        tag_list = list(dict.fromkeys(str(t).strip() for t in tag_list if str(t).strip()))
        args.append(json.dumps(tag_list))
        sets.append(f"tags = ${len(args)}::jsonb")
    for text_field in ("name", "display_name", "publisher", "description", "version"):
        if text_field in patch:
            args.append(str(patch.get(text_field) or "").strip())
            sets.append(f"{text_field} = ${len(args)}")
    if "installable" in patch:
        args.append(bool(patch.get("installable")))
        sets.append(f"installable = ${len(args)}")
    if "launch_spec" in patch:
        spec = patch.get("launch_spec")
        if spec is not None and not isinstance(spec, dict):
            raise ValueError("launch_spec must be an object")
        kind = str((spec or {}).get("kind") or "none").strip().lower()
        if kind not in ("stdio", "remote", "none"):
            raise ValueError(f"unsupported launch_spec kind: {kind}")
        args.append(json.dumps(spec or {}))
        sets.append(f"launch_spec = ${len(args)}::jsonb")
        # 人工改过启动方式即视为已核对，徽标从 needs/failed 翻成 filled。
        args.append("filled")
        sets.append(f"launch_spec_status = ${len(args)}")
        args.append("")
        sets.append(f"launch_spec_error = ${len(args)}")
    if "install_spec" in patch:
        # skill/plugin/prompt 的安装配置（与 launch_spec 同级的人工草稿字段）。
        # 整体替换：前端只送勾选分类对应的部分，取消勾选即清掉该分类的配置。
        spec = patch.get("install_spec")
        if spec is not None and not isinstance(spec, dict):
            raise ValueError("install_spec must be an object")
        # install_spec.skill 写前收口成 entries 形态，DB 不再存旧单对象形状。
        if isinstance(spec, dict):
            spec = _normalize_skill_install_spec(spec)
        args.append(json.dumps(spec or {}))
        sets.append(f"install_spec = ${len(args)}::jsonb")
    if "sort_order" in patch:
        raw = patch.get("sort_order")
        if raw is None or str(raw).strip() == "":
            # 清空 = 撤销手动排序，排最后按热度。
            args.append(None)
            sets.append(f"sort_order = ${len(args)}")
        else:
            try:
                sort_value = int(str(raw).strip())
            except (TypeError, ValueError):
                raise ValueError("排序需为非负整数") from None
            if sort_value < 0:
                raise ValueError("排序需为非负整数")
            args.append(sort_value)
            sets.append(f"sort_order = ${len(args)}")
    if "status" in patch:
        status = str(patch.get("status") or "").strip().lower()
        if status not in ("draft", "published", "hidden"):
            raise ValueError(f"unsupported status: {status}")
        args.append(status)
        sets.append(f"status = ${len(args)}")
        if status == "published":
            sets.append("published_at = now()")
        else:
            sets.append("published_at = NULL")

    if not sets:
        return await get_item(item_id)
    args.append(item_id)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE marketplace_leaderboard_items SET {', '.join(sets)} WHERE id=${len(args)} RETURNING *",
            *args,
        )
    if row is not None and str(row["status"]) == "published":
        row = await _ensure_publishable_modules(conn, row, require_verified=require_verified)
    return row_to_dict(row) if row else None


async def _ensure_publishable_modules(conn, row, *, require_verified: bool = False) -> Any:
    """发布兜底：installable 且分类为空 → 按 external_data 的 board/上游分类推导一次；
    仍推不出则报错（用户侧不知道往哪个模块装）。推出的分类随发布落库。

    分类落定后再过 MCP launch_spec 门禁（软/硬由 require_verified 控制）——
    与批量发布 ``publish_items`` 同一口径。"""
    modules = row["target_modules"] if "target_modules" in row.keys() else None
    module_list = list(modules or [])
    if not module_list and row["target_module"]:
        module_list = [row["target_module"]]
    if not row["installable"] or module_list:
        gate_err = _launch_spec_publish_gate(row, module_list, require_verified)
        if gate_err:
            raise ValueError(gate_err)
        return row
    external = row["external_data"] if "external_data" in row.keys() else None
    if isinstance(external, str):
        try:
            external = json.loads(external)
        except (TypeError, ValueError):
            external = {}
    board = str((external or {}).get("board") or row["board"] or "")
    upstream_category = str((external or {}).get("upstream_category") or "")
    module_list = derive_target_modules(board, upstream_category)
    if not module_list:
        raise ValueError("上游未给出安装形态，请先在分类里勾选，或改为仅浏览")
    await conn.execute(
        "UPDATE marketplace_leaderboard_items SET target_module=$2, target_modules=$3::jsonb WHERE id=$1",
        row["id"], module_list[0], json.dumps(module_list),
    )
    row = dict(row)
    row["target_module"] = module_list[0]
    row["target_modules"] = module_list
    return row


async def set_launch_spec(
    item_id: int, *, spec: dict | None, status: str, error: str = ""
) -> dict | None:
    """写 agent 补出的 MCP 启动方式。**只动草稿字段，status 保持不变。**

    ``status`` 参数是 launch_spec 自己的状态（pending/filled/failed/verified），
    与条目的发布状态无关——补全永远不等于发布；verified 由 verify 步骤显式写。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE marketplace_leaderboard_items
               SET launch_spec = $2::jsonb,
                   launch_spec_status = $3,
                   launch_spec_error = $4
             WHERE id = $1
            RETURNING *
            """,
            item_id, json.dumps(spec or {}), status, error,
        )
    return row_to_dict(row) if row else None


async def merge_external_data_probe(item_id: int, probe_patch: dict) -> dict | None:
    """JSONB 只合并 ``external_data.probe``（不整替）——verify 等定点写入用。

    重同步的 ON CONFLICT 会整体换 external_data（含 probe），所以这里的定点补丁
    是"同步间隙内的当前事实"，丢了会在下次重同步后由新探针重建。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE marketplace_leaderboard_items
               SET external_data = jsonb_set(
                       external_data, '{probe}',
                       COALESCE(external_data->'probe', '{}'::jsonb) || $2::jsonb)
             WHERE id = $1
            RETURNING *
            """,
            item_id, json.dumps(probe_patch or {}),
        )
    return row_to_dict(row) if row else None


def _launch_spec_publish_gate(row, modules: list[str], require_verified: bool) -> str | None:
    """MCP 可安装条目的 launch_spec 发布门禁。返回错误文案（拦）或 None（放行）。

    - 不可装 / 分类不含 mcp / launch_spec.kind == "none" → 不过门（browse-only
      与无启动方式诉求的条目照常发布）。
    - require_verified=True（硬门）：必须 verified（真的握手成功过）。
    - require_verified=False（软门，默认）：filled 或 verified 都放行——探测/识别
      /人工填过即算，verified 只是徽标。
    """
    if isinstance(row, dict):
        if not row.get("installable", True):
            return None
        row_modules = row.get("target_modules") if "target_modules" in row.keys() else None
        module_list = list(row_modules or []) if row_modules is not None else list(modules or [])
        spec = row.get("launch_spec") if "launch_spec" in row.keys() else None
        status = row.get("launch_spec_status") if "launch_spec_status" in row.keys() else ""
    else:
        if not getattr(row, "installable", True):
            return None
        module_list = list(modules or [])
        spec = None
        status = ""
    if "mcp" not in module_list:
        return None
    kind = str((spec or {}).get("kind") or "none")
    if kind == "none":
        return None
    if require_verified:
        if status != "verified":
            return "MCP 条目发布要求启动方式验证通过（verified）——点「验证」跑一次握手"
        return None
    if status not in ("filled", "verified"):
        return "MCP 条目发布前需要补启动方式（探测/识别/手填任一），或把启动方式留空（kind=none）"
    return None


async def publish_items(
    item_ids: list[int], *, operator: str, require_verified: bool = False
) -> dict:
    """批量发布。发布 = 「对用户可见」，与「能不能装」是两回事。

    ``installable=false`` 的条目（开发框架、研究程序、awesome 目录）照样可以发布，
    用户侧渲染成仅浏览卡片（跳 GitHub，没有安装入口）——它们有发现价值，只是四类
    里没有对应的安装形态。所以这里只要求：可安装的条目必须有安装形态，仅浏览的不需要。

    归类为空不直接判失败：先从 board/上游 category 推一次并落库。上游榜单已经隐含了
    形态，让管理员再手填一遍是重复劳动；只有上游确实没给线索才要求在资源编辑弹窗选分类。

    MCP 门禁（软+开关，默认软）：可安装且分类含 mcp 的条目，launch_spec 至少
    filled 才能发布；``require_verified=True`` 时要求 verified。见
    ``_launch_spec_publish_gate``。

    返回 ``{published, skipped, failed}``：skipped 是已发布的（幂等），failed 带
    原因（推不出形态 / 不存在 / 门禁不过）。批量里的失败不影响其余条目。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return {"published": [], "skipped": [], "failed": []}
    published: list[int] = []
    skipped: list[int] = []
    failed: list[dict] = []
    async with PostgresClient.pool.acquire() as conn:
        for item_id in item_ids:
            row = await conn.fetchrow(
                "SELECT id, status, installable, target_module, target_modules, board, upstream_category, external_data,"
                " launch_spec, launch_spec_status"
                " FROM marketplace_leaderboard_items WHERE id=$1",
                item_id,
            )
            if row is None:
                failed.append({"id": item_id, "error": "条目不存在"})
                continue
            if row["status"] == "published":
                skipped.append(item_id)
                continue
            # fetchrow 桩可能是普通 dict（缺新列），用 .get 兜底取多选数组
            row_modules = row["target_modules"] if "target_modules" in row.keys() else None
            modules = list(row_modules or [])
            if not modules and row["target_module"]:
                modules = [row["target_module"]]
            if row["installable"] and not modules:
                # 空分类不拦发布：从 external_data（唯一上游真相）推一次并落库；
                # 老数据行可能只有列值，兜底用列。
                external = row["external_data"] if "external_data" in row.keys() else None
                if isinstance(external, str):
                    try:
                        external = json.loads(external)
                    except (TypeError, ValueError):
                        external = {}
                board = str((external or {}).get("board") or row["board"] or "")
                category = str((external or {}).get("upstream_category")
                               or row["upstream_category"] or "")
                modules = derive_target_modules(board, category)
                if not modules:
                    # 上游真的没给线索（frameworks/research/awesome 目录）才要人工：
                    # 声明可装却认不出形态，用户侧不知道往哪个模块装。
                    failed.append({
                        "id": item_id,
                        "error": "上游未给出安装形态，请在资源编辑弹框里勾选分类，或保持仅浏览",
                    })
                    continue
            # MCP launch_spec 门禁（软：filled/verified；硬：verified）。launch_spec 在
            # asyncpg 里是 JSONB str，dict 桩直接用——gate 两种形态都兜。
            spec = row["launch_spec"] if "launch_spec" in row.keys() else None
            if isinstance(spec, str):
                try:
                    spec = json.loads(spec)
                except (TypeError, ValueError):
                    spec = {}
            gate_row = {"installable": bool(row["installable"]),
                        "target_modules": modules,
                        "launch_spec": spec if isinstance(spec, dict) else {},
                        "launch_spec_status": row["launch_spec_status"] if "launch_spec_status" in row.keys() else ""}
            gate_err = _launch_spec_publish_gate(gate_row, modules, require_verified)
            if gate_err:
                failed.append({"id": item_id, "error": gate_err})
                continue
            await conn.execute(
                """
                UPDATE marketplace_leaderboard_items
                   SET status = 'published', published_at = now(), published_by = $2,
                       target_module = $3, target_modules = $4::jsonb
                 WHERE id = $1
                """,
                item_id, operator, modules[0] if modules else "", json.dumps(modules),
            )
            published.append(item_id)
    return {"published": published, "skipped": skipped, "failed": failed}


async def unpublish_items(item_ids: list[int]) -> list[int]:
    """撤回发布（回 draft）。用户侧立即不可见。"""
    from db import PostgresClient

    if not PostgresClient.pool or not item_ids:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE marketplace_leaderboard_items
               SET status = 'draft', published_at = NULL, published_by = ''
             WHERE id = ANY($1::bigint[]) AND status = 'published'
            RETURNING id
            """,
            item_ids,
        )
    return [int(r["id"]) for r in rows]


async def purge_all() -> int:
    """清空整个候选池（DELETE 全表）。破坏性操作——旧模型（admin_overrides/rank）数据
    与新设计不匹配时用，清空后重新同步即按新模型 + 探针重新派生。调用方必须二次确认。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return 0
    async with PostgresClient.pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM marketplace_leaderboard_items")
        await conn.execute("TRUNCATE marketplace_leaderboard_items RESTART IDENTITY")
    return int(count or 0)


async def list_pending_launch_spec(limit: int = 50) -> list[dict]:
    """待补启动方式的 MCP 草稿：分类含 mcp、可安装、还没补过（或上次失败）。

    分类是多选数组：一条既是 mcp 又是 skill 的条目同样需要补 MCP 启动方式。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM marketplace_leaderboard_items
             WHERE target_modules @> '["mcp"]'::jsonb AND installable = true
               AND launch_spec_status IN ('', 'failed')
             ORDER BY stars DESC
             LIMIT $1
            """,
            max(1, min(200, limit)),
        )
    return [row_to_dict(r) for r in rows]
