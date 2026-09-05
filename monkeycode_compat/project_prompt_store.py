"""项目提示词存储层（project_prompts 表）。

提示词分两类：
- 系统/管理端：``owner_user_id IS NULL``，由管理端维护，用户只读启用项。
- 用户私有：``owner_user_id = 当前用户``，用户自己在编辑器配置弹框内增删改。

编辑器通过 ``editors.prompt_id`` 绑定其一。切换提示词后，后端把提示词内容写入
编辑器工作目录的 CLAUDE.md / AGENTS.md（见 routes_editors / routes_project）。

Storage only -- 不触碰模型请求主链路。
"""
from __future__ import annotations

import json
import uuid
from typing import Any

_TIME_KEYS = ("created_at", "updated_at")
_UPDATABLE = {"name", "content", "providers", "enabled", "market_id", "market_version"}


def _row_to_dict(row) -> dict:
    d = dict(row)
    for key in _TIME_KEYS:
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    providers = d.get("providers")
    if isinstance(providers, str):
        try:
            providers = json.loads(providers)
        except (TypeError, ValueError):
            providers = []
    d["providers"] = providers if isinstance(providers, list) else []
    owner = d.get("owner_user_id")
    d["scope"] = "mine" if owner else "system"
    return d


async def list_prompts(*, enabled_only: bool = False, owner_user_id: str | None = None) -> list[dict]:
    """列出提示词。owner_user_id=None 列系统项；非空列该用户的私有项 + 系统启用项。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    if owner_user_id is not None:
        clauses.append(f"(owner_user_id IS NULL AND enabled=true OR owner_user_id=${idx})")
        params.append(str(owner_user_id))
        idx += 1
    elif enabled_only:
        clauses.append("enabled=true")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT * FROM project_prompts {where} ORDER BY owner_user_id NULLS FIRST, updated_at DESC", *params)
    return [_row_to_dict(r) for r in rows]


async def list_admin_prompts() -> list[dict]:
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM project_prompts WHERE owner_user_id IS NULL ORDER BY updated_at DESC")
    return [_row_to_dict(r) for r in rows]


async def get_prompt(prompt_id: str) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM project_prompts WHERE id=$1", str(prompt_id))
    return _row_to_dict(row) if row else None


async def create_prompt(
    *, name: str, content: str, providers: list[str], enabled: bool = True,
    owner_user_id: str | None = None, market_id: str | None = None,
    market_version: str = "",
) -> dict:
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    prompt_id = f"pp_{uuid.uuid4().hex[:20]}"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO project_prompts(
                id, name, content, providers, enabled, owner_user_id, market_id, market_version
            ) VALUES($1,$2,$3,$4::jsonb,$5,$6,$7,$8) RETURNING *""",
            prompt_id, name, content, json.dumps(providers or []), enabled,
            str(owner_user_id) if owner_user_id else None, market_id, market_version,
        )
    return _row_to_dict(row)


async def update_prompt(prompt_id: str, patch: dict, *, owner_user_id: str | None = None) -> dict | None:
    """局部更新。owner_user_id 非空时只能改自己的私有项，不可改系统项。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = []
    params: list[Any] = []
    idx = 1
    for key in _UPDATABLE:
        if key in patch:
            if key == "providers":
                sets.append(f"providers=${idx}::jsonb")
                params.append(json.dumps(patch[key] or []))
            elif key in ("market_id", "market_version"):
                sets.append(f"{key}=${idx}")
                params.append(patch[key] or (None if key == "market_id" else ""))
            else:
                sets.append(f"{key}=${idx}")
                params.append(patch[key])
            idx += 1
    if not sets:
        return await get_prompt(prompt_id)
    sets.append("updated_at=now()")
    params.append(str(prompt_id))
    owner_clause = ""
    if owner_user_id:
        owner_clause = f" AND owner_user_id IS NOT NULL AND owner_user_id=${idx + 1}"
        params.append(str(owner_user_id))
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE project_prompts SET {', '.join(sets)} WHERE id=${idx}{owner_clause} RETURNING *",
            *params,
        )
    return _row_to_dict(row) if row else None


async def delete_prompt(prompt_id: str, *, owner_user_id: str | None = None) -> bool:
    """删除。owner_user_id 非空时只能删自己的私有项。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    if owner_user_id:
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM project_prompts WHERE id=$1 AND owner_user_id=$2",
                str(prompt_id), str(owner_user_id),
            )
    else:
        async with PostgresClient.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM project_prompts WHERE id=$1", str(prompt_id))
    return result.endswith(" 1")
