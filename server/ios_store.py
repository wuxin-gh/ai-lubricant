"""iOS 签名配置存储（Stage 3）。

签名材料（App Store Connect p8 / p12+mobileprovision）服务端托管，job 启动时
经 TLS NodeConnect 一次性下发到节点；secret_data 永不回显（GET 仅返 id/name/kind）。

WDA 产物不再单独入库——prepare 时从市场 device-control iOS 发行版解析 GitHub
Release 直链，宿主节点经自身出口代理下载后重签安装（见 routes_ios.prepare_wda）。
``ios_wda_artifacts`` 表保留建表语句以防已部署库报错，但已无 API/store 引用。
"""
from __future__ import annotations

from typing import Any

from server.db import PostgresClient


async def create_signing_profile(
    owner_user_id: str, name: str, kind: str, secret_data: dict[str, Any]
) -> dict:
    """Create an iOS signing profile (asc or p12).

    kind='asc': secret_data must contain {p8_key, key_id, issuer_id, team_id}
    kind='p12': secret_data must contain {p12_base64, p12_password, mobileprovision_base64}
    """
    if kind not in ("asc", "p12"):
        raise ValueError(f"Invalid signing profile kind: {kind}")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO ios_signing_profiles (owner_user_id, name, kind, secret_data)
            VALUES ($1, $2, $3, $4)
            RETURNING id, owner_user_id, name, kind, created_at
            """,
            owner_user_id,
            name,
            kind,
            secret_data,
        )
        return dict(row)


async def get_signing_profile(profile_id: int, owner_user_id: str | None = None) -> dict | None:
    """Retrieve one signing profile metadata (without secret_data).

    If owner_user_id is given, enforce ownership check.
    """
    async with PostgresClient.pool.acquire() as conn:
        if owner_user_id is not None:
            row = await conn.fetchrow(
                """
                SELECT id, owner_user_id, name, kind, created_at
                FROM ios_signing_profiles
                WHERE id = $1 AND owner_user_id = $2
                """,
                profile_id,
                owner_user_id,
            )
        else:
            row = await conn.fetchrow(
                """
                SELECT id, owner_user_id, name, kind, created_at
                FROM ios_signing_profiles
                WHERE id = $1
                """,
                profile_id,
            )
        return dict(row) if row else None


async def get_signing_profile_secret(profile_id: int, owner_user_id: str) -> dict | None:
    """Retrieve the full signing profile including secret_data (job dispatch only)."""
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, owner_user_id, name, kind, secret_data, created_at
            FROM ios_signing_profiles
            WHERE id = $1 AND owner_user_id = $2
            """,
            profile_id,
            owner_user_id,
        )
        return dict(row) if row else None


async def list_signing_profiles(owner_user_id: str) -> list[dict]:
    """List all signing profiles owned by the user (no secret_data)."""
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, owner_user_id, name, kind, created_at
            FROM ios_signing_profiles
            WHERE owner_user_id = $1
            ORDER BY created_at DESC
            """,
            owner_user_id,
        )
        return [dict(r) for r in rows]


async def delete_signing_profile(profile_id: int, owner_user_id: str) -> bool:
    """Delete a signing profile (owner-check enforced)."""
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM ios_signing_profiles WHERE id = $1 AND owner_user_id = $2",
            profile_id,
            owner_user_id,
        )
        return result.split()[-1] != "0"
