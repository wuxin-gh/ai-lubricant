"""market skill/插件的服务端镜像存储（Phase 2）。

为什么要镜像：节点原本在会话启动时才 git clone 市场 skill（每次 RemoveAll 后重拉、
写 per-session 目录、无共享缓存、强依赖外网）。镜像后下发给节点的 SkillSpec.url 指向
我们服务端的 archive，于是同时拿到：预下载、跨会话共享缓存、不依赖外网、可鉴权。

digest 既是变更检测依据，也是节点侧缓存键（<cache>/{module}/{id}@{digest}/）。

风格对齐 mcp_plugin_store：模块级函数 + 函数内延迟 import PostgresClient。
"""
from __future__ import annotations

import secrets
from typing import Any

_TIME_KEYS = ("created_at", "updated_at", "last_fetched_at")

ALLOWED_MODULES = frozenset({"skills", "plugins"})


def row_to_dict(row) -> dict:
    d = dict(row)
    for key in _TIME_KEYS:
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    # fetch_token 不出库：它是节点拉取凭据，只在下发 spec 时用，列表接口不回传明文
    if "fetch_token" in d:
        d["has_token"] = bool(d.pop("fetch_token"))
    return d


def gen_fetch_token() -> str:
    return "rmf_" + secrets.token_urlsafe(32)


async def list_mirrors(module: str | None = None) -> list[dict]:
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        if module:
            rows = await conn.fetch(
                "SELECT * FROM resource_mirrors WHERE module=$1 ORDER BY id DESC", module
            )
        else:
            rows = await conn.fetch("SELECT * FROM resource_mirrors ORDER BY id DESC")
    return [row_to_dict(r) for r in rows]


async def get_mirror(mirror_id: int) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM resource_mirrors WHERE id=$1", mirror_id)
    return row_to_dict(row) if row else None


async def find_mirror(module: str, market_id: str, version: str = "") -> dict | None:
    """按 (module, market_id, version) 找镜像。version 空串表示「未指定版本」那一行。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM resource_mirrors WHERE module=$1 AND market_id=$2 AND version=$3",
            module, market_id, version,
        )
    return row_to_dict(row) if row else None


async def find_ready_mirror(module: str, market_id: str, version: str = "") -> dict | None:
    """Find a ready mirror, preferring the requested version then latest ready."""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        if version:
            row = await conn.fetchrow(
                "SELECT * FROM resource_mirrors WHERE module=$1 AND market_id=$2 "
                "AND version=$3 AND status='ready'",
                module, market_id, version,
            )
        else:
            row = await conn.fetchrow(
                "SELECT * FROM resource_mirrors WHERE module=$1 AND market_id=$2 "
                "AND status='ready' ORDER BY updated_at DESC LIMIT 1",
                module, market_id,
            )
    return row_to_dict(row) if row else None


async def get_mirror_secret(mirror_id: int) -> str:
    """取 fetch_token 明文（仅下发 spec / 校验拉取时用，不经列表接口）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return ""
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT fetch_token FROM resource_mirrors WHERE id=$1", mirror_id)
    return (row["fetch_token"] if row else "") or ""


async def verify_fetch_token(module: str, market_id: str, token: str) -> dict | None:
    """校验节点拉取凭据。命中且 status=ready 才返回行，否则 None。"""
    from db import PostgresClient

    if not PostgresClient.pool or not token:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM resource_mirrors WHERE module=$1 AND market_id=$2 "
            "AND fetch_token=$3 AND status='ready'",
            module, market_id, token,
        )
    return dict(row) if row else None


async def upsert_pending(
    module: str,
    market_id: str,
    *,
    name: str = "",
    version: str = "",
    source_url: str = "",
    source_ref: str = "",
    source_path: str = "",
) -> dict | None:
    """建/复位一条 pending 镜像行。已存在则复位为 pending 并保留 fetch_token。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO resource_mirrors(
                module, market_id, name, version, source_url, source_ref, source_path,
                status, fetch_token
            ) VALUES($1,$2,$3,$4,$5,$6,$7,'pending',$8)
            ON CONFLICT (module, market_id, version) DO UPDATE SET
                name=EXCLUDED.name,
                source_url=EXCLUDED.source_url,
                source_ref=EXCLUDED.source_ref,
                source_path=EXCLUDED.source_path,
                status='pending',
                error='',
                updated_at=now()
            RETURNING *
            """,
            module, market_id, name, version, source_url, source_ref, source_path,
            gen_fetch_token(),
        )
    return row_to_dict(row) if row else None


async def mark_status(
    mirror_id: int,
    status: str,
    *,
    error: str = "",
    digest: str = "",
    archive_path: str = "",
    size_bytes: int = 0,
) -> dict | None:
    """更新镜像状态。ready 时一并写 digest/archive_path/size_bytes 与 last_fetched_at。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets = ["status=$2", "error=$3", "updated_at=now()"]
    params: list[Any] = [mirror_id, status, error[:4000]]
    idx = 4
    if status == "ready":
        sets += [f"digest=${idx}", f"archive_path=${idx+1}", f"size_bytes=${idx+2}", "last_fetched_at=now()"]
        params += [digest, archive_path, size_bytes]
    sql = f"UPDATE resource_mirrors SET {', '.join(sets)} WHERE id=$1 RETURNING *"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return row_to_dict(row) if row else None


async def delete_mirror(mirror_id: int) -> bool:
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute("DELETE FROM resource_mirrors WHERE id=$1", mirror_id)
    return result.endswith("1")
