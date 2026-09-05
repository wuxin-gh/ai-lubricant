"""Database helpers for MCP Runtime plugins and versions.

MCP Runtime keeps service metadata in the existing mcp_services table and stores
custom/plugin revisions in mcp_plugin_versions. Versions are immutable; a
service's active_version_id points at the revision currently published by the
runtime.
"""
from __future__ import annotations

import json
import secrets
import uuid
from typing import Any


_VERSION_KEYS = ("created_at", "security_checked_at")
_SERVICE_TIME_KEYS = (
    "created_at", "updated_at", "tools_cached_at", "runtime_last_ping",
    # 安装编排时间戳（Phase 2）：created->configuring->starting->testing->ready
    "configured_at", "started_at", "tested_at", "ready_at",
)


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def service_row_to_dict(row) -> dict:
    d = dict(row)
    for key in _SERVICE_TIME_KEYS:
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    for key, default in (("args", []), ("env_template", {}), ("headers", {}), ("tools_cache", None), ("group_ids", [])):
        if key in d:
            d[key] = _json_value(d.get(key), default)
    if isinstance(d.get("group_ids"), list):
        d["group_ids"] = [str(value) for value in d["group_ids"]]
    return d


def version_row_to_dict(row) -> dict:
    d = dict(row)
    for key in _VERSION_KEYS:
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    d["config_json"] = _json_value(d.get("config_json"), {})
    d["security_report"] = _json_value(d.get("security_report"), {})
    return d


async def list_services(include_templates: bool = False) -> list[dict]:
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    where = "" if include_templates else "WHERE template=FALSE"
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM mcp_services {where} ORDER BY builtin DESC, id ASC"
        )
    return [service_row_to_dict(row) for row in rows]


async def list_services_for_group(group_id: str) -> list[dict]:
    """List internal MCP services with binding state for one group.
    Excludes built-in services (cdp-bridge, mail) which are user-owned, not group-level.
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []

    gid = str(group_id).strip()
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM mcp_services
            WHERE template=FALSE
              AND builtin = FALSE
            ORDER BY builtin DESC, id ASC
        """
        )
    result = []
    for row in rows:
        item = service_row_to_dict(row)
        group_ids = [str(value) for value in (item.get("group_ids") or [])]
        item["bound"] = gid in group_ids
        item["group_ids"] = group_ids
        result.append(item)
    return result


async def set_group_services(group_id: str, service_ids: list[int]) -> None:
    """Grant exactly ``service_ids`` to one group without touching other groups.

    Built-in services are user-owned resources and cannot be group-authorized.
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return
    gid = str(group_id).strip()
    selected = sorted({int(value) for value in (service_ids or [])})
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            if selected:
                await conn.execute(
                    "UPDATE mcp_services SET group_ids = group_ids - $1, updated_at=now() "
                    "WHERE group_ids ? $1 AND NOT (id = ANY($2::int[]))",
                    gid, selected,
                )
                await conn.execute(
                    """
                    UPDATE mcp_services
                    SET group_ids = group_ids || to_jsonb($1::text), updated_at=now()
                    WHERE id = ANY($2::int[])
                      AND builtin = FALSE
                      AND template = FALSE
                      AND NOT (group_ids ? $1)
                    """,
                    gid, selected,
                )
            else:
                await conn.execute(
                    "UPDATE mcp_services SET group_ids = group_ids - $1, updated_at=now() WHERE group_ids ? $1",
                    gid,
                )
            # Clean up bindings created before built-ins were excluded.
            await conn.execute(
                "UPDATE mcp_services SET group_ids = group_ids - $1, updated_at=now() "
                "WHERE group_ids ? $1 AND builtin = TRUE",
                gid,
            )


async def list_services_by_user(user_id: str | None) -> list[dict]:
    """列出某 owner 的 MCP 服务（user_id IS NULL → 平台/管理端；指定 user_id → 个人）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        if user_id is None:
            rows = await conn.fetch(
                "SELECT * FROM mcp_services WHERE user_id IS NULL AND template=FALSE ORDER BY builtin DESC, id ASC"
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM mcp_services WHERE user_id=$1 AND template=FALSE ORDER BY id ASC",
                user_id,
            )
    return [service_row_to_dict(row) for row in rows]


async def get_service(service_id: int) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM mcp_services WHERE id=$1", service_id)
    return service_row_to_dict(row) if row else None


async def get_service_by_name(name: str) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM mcp_services WHERE name=$1", name)
    return service_row_to_dict(row) if row else None


async def set_service_enabled(service_id: int, enabled: bool) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE mcp_services SET enabled=$2, updated_at=now() WHERE id=$1 RETURNING *",
            service_id,
            enabled,
        )
    return service_row_to_dict(row) if row else None


async def list_catalog() -> list[dict]:
    """市场目录：自带模板 + 系统内置服务。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM mcp_services
            WHERE template=TRUE OR (builtin=TRUE AND source='system')
            ORDER BY source NULLS LAST, category, name
            """
        )
    return [service_row_to_dict(row) for row in rows]


async def toggle_service_enabled(service_id: int) -> dict | None:
    """翻转 enabled，返回更新后的服务行。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE mcp_services SET enabled = NOT enabled, updated_at=now() "
            "WHERE id=$1 RETURNING *",
            service_id,
        )
    return service_row_to_dict(row) if row else None


# mcp_services 写操作的归一化出口：列名 → 是否 JSONB。INSERT/UPDATE 经此函数，
# 路由层不再直接拼 SQL，字段反序列化统一在 service_row_to_dict 收口。
_JSONB_SERVICE_FIELDS = frozenset({"args", "env_template", "headers", "tools_cache", "group_ids"})


async def create_service(fields: dict) -> dict:
    """插入一条 mcp_services 行，返回归一化后的服务字典。

    fields 的 key 必须是合法列名；JSONB 字段调用方传 Python 对象，这里负责序列化。
    name 冲突抛 LookupError，调用方据此回 409。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    cols = list(fields.keys())
    placeholders: list[Any] = []
    values: list[Any] = []
    for i, col in enumerate(cols, start=1):
        placeholders.append(f"${i}{'::jsonb' if col in _JSONB_SERVICE_FIELDS else ''}")
        value = fields[col]
        values.append(json.dumps(value, ensure_ascii=False) if col in _JSONB_SERVICE_FIELDS else value)
    col_sql = ", ".join(cols)
    ph_sql = ", ".join(placeholders)
    try:
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"INSERT INTO mcp_services({col_sql}) VALUES({ph_sql}) RETURNING *",
                *values,
            )
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise LookupError(f"MCP service name already exists") from e
        raise
    return service_row_to_dict(row)


async def patch_service(service_id: int, updates: dict) -> dict | None:
    """局部更新 mcp_services，updates 的 key 是列名、value 是 Python 对象。

    JSONB 字段自动序列化。空 updates 直接返回当前行。返回更新后的归一化服务字典
    （不存在返回 None）。这是 mcp_services 写操作的统一收口，路由层不要自己拼 SET 子句。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    if not updates:
        return await get_service(service_id)
    sets: list[str] = []
    params: list[Any] = [service_id]
    idx = 2
    for col, value in updates.items():
        sets.append(f"{col}=${idx}{'::jsonb' if col in _JSONB_SERVICE_FIELDS else ''}")
        params.append(json.dumps(value, ensure_ascii=False) if col in _JSONB_SERVICE_FIELDS else value)
        idx += 1
    sets.append("updated_at=now()")
    sql = f"UPDATE mcp_services SET {', '.join(sets)} WHERE id=$1 RETURNING *"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return service_row_to_dict(row) if row else None


async def delete_service(service_id: int, *, allow_builtin: bool = False) -> bool:
    """删除一条 mcp_services。内置服务默认拒绝（allow_builtin=False 时）。

    返回是否真的删了一行。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        if not allow_builtin:
            builtin = await conn.fetchval("SELECT builtin FROM mcp_services WHERE id=$1", service_id)
            if builtin:
                raise PermissionError("Cannot delete builtin MCP service")
        result = await conn.execute("DELETE FROM mcp_services WHERE id=$1", service_id)
    return result.endswith(" 1")


async def delete_service_owned(service_id: int, user_id: str) -> bool:
    """用户侧删除：限定 owner。返回是否真的删了一行。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM mcp_services WHERE id=$1 AND user_id=$2", service_id, user_id,
        )
    return result.endswith(" 1")


async def set_deploy_scope(service_id: int, scope: str, *, install_state: str | None = None, install_step: str | None = None) -> dict | None:
    """改 deploy_scope（形态切换：server↔session↔node_hosted）。可选一并改安装态。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = ["deploy_scope=$2", "updated_at=now()"]
    params: list[Any] = [service_id, scope]
    idx = 3
    if install_state is not None:
        sets.append(f"install_state=${idx}")
        params.append(install_state)
        idx += 1
    if install_step is not None:
        sets.append(f"install_step=${idx}")
        params.append(install_step)
        idx += 1
    sql = f"UPDATE mcp_services SET {', '.join(sets)} WHERE id=$1 RETURNING *"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return service_row_to_dict(row) if row else None


async def set_tools_cache(service_id: int, tools: list[dict]) -> None:
    """刷新 tools_cache（tools/list 发现结果）。无返回；不存在则静默。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return
    async with PostgresClient.pool.acquire() as conn:
        await conn.execute(
            "UPDATE mcp_services SET tools_cache=$1::jsonb, tools_cached_at=now(), updated_at=now() WHERE id=$2",
            json.dumps(tools or [], ensure_ascii=False), service_id,
        )


async def list_versions(service_id: int) -> list[dict]:
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM mcp_plugin_versions WHERE service_id=$1 ORDER BY version DESC",
            service_id,
        )
    return [version_row_to_dict(row) for row in rows]


async def get_version(version_id: int) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM mcp_plugin_versions WHERE id=$1", version_id)
    return version_row_to_dict(row) if row else None


async def get_active_version(service_id: int) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT v.* FROM mcp_services s
            JOIN mcp_plugin_versions v ON v.id = s.active_version_id
            WHERE s.id=$1
            """,
            service_id,
        )
    return version_row_to_dict(row) if row else None


async def create_version(
    service_id: int,
    *,
    code: str = "",
    config_json: dict | None = None,
    author: str = "",
    source: str = "manual",
) -> dict:
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH next_version AS (
                SELECT COALESCE(MAX(version), 0) + 1 AS version
                FROM mcp_plugin_versions
                WHERE service_id=$1
            )
            INSERT INTO mcp_plugin_versions(service_id, version, code, config_json, author, source)
            SELECT $1, version, $2, $3::jsonb, $4, $5 FROM next_version
            RETURNING *
            """,
            service_id,
            code or "",
            json.dumps(config_json or {}, ensure_ascii=False),
            author or "",
            source or "manual",
        )
    return version_row_to_dict(row)


async def update_security_result(
    version_id: int,
    *,
    status: str,
    report: dict,
    model: str = "",
) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE mcp_plugin_versions
            SET security_status=$2,
                security_report=$3::jsonb,
                security_model=$4,
                security_checked_at=now()
            WHERE id=$1
            RETURNING *
            """,
            version_id,
            status,
            json.dumps(report or {}, ensure_ascii=False),
            model or "",
        )
    return version_row_to_dict(row) if row else None


async def activate_version(version_id: int, *, tools_cache: list[dict] | None = None) -> dict | None:
    """Mark a checked version as active for its service.

    Caller must already have loaded the plugin successfully. This function keeps
    DB state consistent and marks previous versions as not loaded.
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            version_row = await conn.fetchrow(
                "SELECT * FROM mcp_plugin_versions WHERE id=$1 FOR UPDATE",
                version_id,
            )
            if not version_row:
                return None
            service_id = version_row["service_id"]
            await conn.execute(
                "UPDATE mcp_plugin_versions SET loaded=false WHERE service_id=$1",
                service_id,
            )
            await conn.execute(
                "UPDATE mcp_plugin_versions SET loaded=true WHERE id=$1",
                version_id,
            )
            row = await conn.fetchrow(
                """
                UPDATE mcp_services
                SET active_version_id=$2,
                    runtime_status='loaded',
                    runtime_last_error='',
                    tools_cache=$3::jsonb,
                    tools_cached_at=now(),
                    updated_at=now()
                WHERE id=$1
                RETURNING *
                """,
                service_id,
                version_id,
                json.dumps(tools_cache or [], ensure_ascii=False),
            )
    return service_row_to_dict(row) if row else None


async def mark_runtime_error(service_id: int, error: str) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE mcp_services
            SET runtime_status='error', runtime_last_error=$2, updated_at=now()
            WHERE id=$1
            RETURNING *
            """,
            service_id,
            error[:4000],
        )
    return service_row_to_dict(row) if row else None


async def mark_runtime_status(
    service_id: int,
    status: str,
    *,
    tools_cache: list[dict] | None = None,
    error: str = "",
) -> dict | None:
    """更新 runtime_status（stopped|loaded|error|restarting）并可选刷新工具缓存。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        if tools_cache is not None and status == "loaded":
            row = await conn.fetchrow(
                """
                UPDATE mcp_services
                SET runtime_status=$2, runtime_last_error=$3,
                    tools_cache=$4::jsonb, tools_cached_at=now(), updated_at=now()
                WHERE id=$1
                RETURNING *
                """,
                service_id,
                status,
                error,
                json.dumps(tools_cache or [], ensure_ascii=False),
            )
        else:
            row = await conn.fetchrow(
                """
                UPDATE mcp_services
                SET runtime_status=$2, runtime_last_error=$3, updated_at=now()
                WHERE id=$1
                RETURNING *
                """,
                service_id,
                status,
                error,
            )
    return service_row_to_dict(row) if row else None


# ── 安装编排状态（Phase 2）──
#
# install_state 与 runtime_status 语义不同，不要混：
#   install_state：安装进度 created->configuring->starting->testing->ready|error
#   runtime_status：运行态 stopped|loaded|error|restarting
# session 形态（stdio 会话内）不走安装流程，直接标 ready。

# 进入某状态时一并打的时间戳，缺失的保持原样
_INSTALL_STATE_STAMPS = {
    "configuring": "configured_at",
    "starting": "started_at",
    "testing": "tested_at",
    "ready": "ready_at",
}


async def mark_install_state(
    service_id: int,
    state: str,
    *,
    step: str = "",
    error: str = "",
) -> dict | None:
    """更新 install_state，并按状态打对应时间戳。error 时写 install_error。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    stamp_col = _INSTALL_STATE_STAMPS.get(state)
    sets = ["install_state=$2", "install_step=$3", "install_error=$4", "updated_at=now()"]
    params: list[Any] = [service_id, state, step[:200], error[:4000]]
    if stamp_col:
        sets.append(f"{stamp_col}=now()")
    sql = f"UPDATE mcp_services SET {', '.join(sets)} WHERE id=$1 RETURNING *"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return service_row_to_dict(row) if row else None


async def mark_host_state(
    service_id: int,
    *,
    node_id: str | None = None,
    port: int | None = None,
    pid: int | None = None,
    status: str | None = None,
) -> dict | None:
    """更新 node_hosted 形态的托管信息（哪个节点、哪个端口、进程存活）。

    只更新显式传入的字段：节点掉线时只改 host_status，不能把 node_id/port 一并清掉，
    否则节点回来后无法对账重拉（要靠 node_id+port 找回那个进程）。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = ["updated_at=now()"]
    params: list[Any] = [service_id]
    idx = 2
    for col, value in (
        ("host_node_id", node_id),
        ("host_port", port),
        ("host_pid", pid),
        ("host_status", status),
    ):
        if value is not None:
            sets.append(f"{col}=${idx}")
            params.append(value)
            idx += 1
    if len(sets) == 1:
        return await get_service(service_id)
    sql = f"UPDATE mcp_services SET {', '.join(sets)} WHERE id=$1 RETURNING *"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return service_row_to_dict(row) if row else None


async def list_node_hosted(node_id: str = "") -> list[dict]:
    """列出 node_hosted 形态的服务（可按节点过滤）。节点掉线对账 / 重启重拉用。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        if node_id:
            rows = await conn.fetch(
                "SELECT * FROM mcp_services WHERE deploy_scope='node_hosted' AND host_node_id=$1",
                node_id,
            )
        else:
            rows = await conn.fetch("SELECT * FROM mcp_services WHERE deploy_scope='node_hosted'")
    return [service_row_to_dict(r) for r in rows]


async def allocate_host_port(node_id: str, *, start: int = 47000, end: int = 47999) -> int:
    """给节点上的新托管进程分配一个未被本节点其他托管服务占用的端口。

    只避开我们自己记账的端口——节点上其他进程占用无法从服务端看到，那种冲突由节点侧
    启动失败上报（install_state=error），比在服务端猜更诚实。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return start
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT host_port FROM mcp_services WHERE host_node_id=$1 AND host_port IS NOT NULL",
            node_id,
        )
    used = {int(r["host_port"]) for r in rows if r["host_port"]}
    for port in range(start, end + 1):
        if port not in used:
            return port
    raise RuntimeError(f"节点 {node_id} 上没有可用的托管端口（{start}-{end} 已用尽）")


async def get_install_state(service_id: int) -> dict | None:
    """读单个服务的安装状态字段（前端轮询用）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, deploy_scope, install_state, install_step, install_error, "
            "configured_at, started_at, tested_at, ready_at, "
            "host_node_id, host_port, host_status FROM mcp_services WHERE id=$1",
            service_id,
        )
    return service_row_to_dict(row) if row else None


# ── MCP Runtime 统一配置 ──

ALLOWED_RUNTIME_CONFIG_TYPES = frozenset({
    "env_var", "mail_account", "mail_address", "cdp_driver", "cdp_client",
    "device_driver", "device",
})
_RUNTIME_META_KEYS = {"id", "service_id", "config_type", "instance_key", "revision", "token_hash", "token_hint", "created_at", "updated_at", "_secrets"}
_SECRET_FIELDS = {"env_var": frozenset({"value"}), "mail_account": frozenset({"password", "secret_key"})}


def hash_runtime_token(token: str) -> str:
    """Return the only representation of a CDP client token that may be persisted."""
    import hashlib
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_runtime_token() -> str:
    """Generate a high-entropy token. Callers must return it once and never persist it."""
    import secrets
    return "cdp_" + secrets.token_urlsafe(32)


def runtime_token_hint(token: str) -> str:
    return f"{token[:8]}...{token[-4:]}"


def _require_config_type(config_type: str) -> str:
    if config_type not in ALLOWED_RUNTIME_CONFIG_TYPES:
        raise ValueError(f"unsupported MCP runtime config type: {config_type}")
    return config_type


def runtime_config_row_to_dict(row, *, include_secrets: bool = False) -> dict:
    envelope = dict(row)
    data = _json_value(envelope.get("data"), {})
    secret_data = _json_value(envelope.get("secret_data"), {})
    result = {**data, "id": envelope["id"], "service_id": envelope["service_id"], "config_type": envelope["config_type"], "instance_key": envelope["instance_key"], "revision": int(envelope.get("revision") or 1), "token_hash": envelope.get("token_hash"), "token_hint": envelope.get("token_hint"), "_secrets": {key: {"state": "set" if bool(value) else "unset"} for key, value in secret_data.items()}}
    if include_secrets:
        result.update(secret_data)
    for key in ("created_at", "updated_at"):
        if envelope.get(key) is not None:
            result[key] = envelope[key].isoformat()
    return result


async def list_runtime_configs(service_id: int, config_type: str, *, include_secrets: bool = False) -> list[dict]:
    from db import PostgresClient
    _require_config_type(config_type)
    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM mcp_runtime_configs WHERE service_id=$1 AND config_type=$2 ORDER BY id", service_id, config_type)
    return [runtime_config_row_to_dict(row, include_secrets=include_secrets) for row in rows]


async def get_runtime_config(config_id: int, *, include_secrets: bool = False) -> dict | None:
    from db import PostgresClient
    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM mcp_runtime_configs WHERE id=$1", config_id)
    return runtime_config_row_to_dict(row, include_secrets=include_secrets) if row else None


async def get_runtime_config_by_key(service_id: int, config_type: str, instance_key: str, *, include_secrets: bool = False) -> dict | None:
    from db import PostgresClient
    _require_config_type(config_type)
    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM mcp_runtime_configs WHERE service_id=$1 AND config_type=$2 AND instance_key=$3", service_id, config_type, instance_key)
    return runtime_config_row_to_dict(row, include_secrets=include_secrets) if row else None


def _split_runtime_value(config_type: str, value: dict, existing_secrets: dict | None = None) -> tuple[dict, dict]:
    secret_fields = _SECRET_FIELDS.get(config_type, frozenset())
    data, secrets = {}, dict(existing_secrets or {})
    env_value_is_secret = config_type == "env_var" and bool(value.get("secret"))
    for key, raw in value.items():
        if key in _RUNTIME_META_KEYS:
            continue
        is_secret = key in secret_fields and (config_type != "env_var" or env_value_is_secret)
        if is_secret:
            if raw not in (None, ""):
                secrets[key] = raw
            data.pop(key, None)
        else:
            if key in secret_fields and raw in (None, "") and key in secrets:
                continue
            data[key] = raw
            if key in secret_fields:
                secrets.pop(key, None)
    return data, secrets


async def create_runtime_config(service_id: int, config_type: str, value: dict, *, instance_key: str | None = None, token_hash: str | None = None, token_hint: str | None = None) -> dict:
    from db import PostgresClient
    _require_config_type(config_type)
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    key = str(instance_key or uuid.uuid4().hex)
    data, secret_data = _split_runtime_value(config_type, value)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO mcp_runtime_configs(service_id,config_type,instance_key,data,secret_data,token_hash,token_hint) VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7) RETURNING *", service_id, config_type, key, json.dumps(data, ensure_ascii=False), json.dumps(secret_data, ensure_ascii=False), token_hash, token_hint)
    return runtime_config_row_to_dict(row)


async def update_runtime_config(config_id: int, patch: dict, *, expected_revision: int | None = None, replace_token: bool = False) -> dict | None:
    from db import PostgresClient
    current = await get_runtime_config(config_id, include_secrets=True)
    if current is None or not PostgresClient.pool:
        return None
    data, secret_data = _split_runtime_value(current["config_type"], current)
    patch_data, secret_data = _split_runtime_value(current["config_type"], patch, secret_data)
    data.update(patch_data)
    revision = int(expected_revision if expected_revision is not None else current["revision"])
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("UPDATE mcp_runtime_configs SET data=$2::jsonb,secret_data=$3::jsonb,token_hash=CASE WHEN $7 THEN $4 ELSE COALESCE($4,token_hash) END,token_hint=CASE WHEN $7 THEN $5 ELSE COALESCE($5,token_hint) END,revision=revision+1,updated_at=now() WHERE id=$1 AND revision=$6 RETURNING *", config_id, json.dumps(data, ensure_ascii=False), json.dumps(secret_data, ensure_ascii=False), patch.get("token_hash"), patch.get("token_hint"), revision, replace_token)
    if row is None and expected_revision is not None:
        raise ValueError("MCP runtime config revision conflict")
    return runtime_config_row_to_dict(row) if row else None


async def upsert_runtime_singleton(service_id: int, config_type: str, patch: dict, *, defaults: dict | None = None, expected_revision: int | None = None) -> dict:
    current = await get_runtime_config_by_key(service_id, config_type, "singleton", include_secrets=True)
    if current:
        return await update_runtime_config(current["id"], patch, expected_revision=expected_revision) or current
    if expected_revision is not None:
        raise ValueError("MCP runtime config revision conflict")
    return await create_runtime_config(service_id, config_type, {**(defaults or {}), **patch}, instance_key="singleton")


async def delete_runtime_config(config_id: int, *, expected_revision: int | None = None) -> bool:
    from db import PostgresClient
    if not PostgresClient.pool:
        return False
    current = await get_runtime_config(config_id)
    if not current:
        return False
    revision = int(expected_revision if expected_revision is not None else current["revision"])
    if expected_revision is not None and revision != int(current["revision"]):
        raise ValueError("MCP runtime config revision conflict")
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.execute(
                "DELETE FROM mcp_runtime_configs WHERE id=$1 AND revision=$2", config_id, revision,
            )
            deleted = not result.endswith("0")
            if deleted and current.get("config_type") == "mail_account":
                await conn.execute(
                    "DELETE FROM mcp_runtime_configs WHERE service_id=$1 AND config_type='mail_address' AND data->>'parent_instance_key'=$2",
                    int(current["service_id"]), str(current["instance_key"]),
                )
    if not deleted and expected_revision is not None:
        raise ValueError("MCP runtime config revision conflict")
    return deleted


async def replace_runtime_configs(service_id: int, config_type: str, items: list[dict]) -> list[dict]:
    from db import PostgresClient
    _require_config_type(config_type)
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    prepared = []
    for item in items:
        key = str(item.get("instance_key") or item.get("key") or item.get("session_id") or uuid.uuid4().hex)
        data, secret_data = _split_runtime_value(config_type, item)
        prepared.append((key, data, secret_data, item.get("token_hash"), item.get("token_hint")))
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM mcp_runtime_configs WHERE service_id=$1 AND config_type=$2", service_id, config_type)
            for key, data, secret_data, token_hash, token_hint in prepared:
                await conn.execute("INSERT INTO mcp_runtime_configs(service_id,config_type,instance_key,data,secret_data,token_hash,token_hint) VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7)", service_id, config_type, key, json.dumps(data, ensure_ascii=False), json.dumps(secret_data, ensure_ascii=False), token_hash, token_hint)
    return await list_runtime_configs(service_id, config_type)


# ── 环境变量兼容 API（只读写统一表） ──

async def list_service_env_vars(service_id: int, *, mask_secret: bool = True) -> list[dict]:
    rows = await list_runtime_configs(service_id, "env_var", include_secrets=not mask_secret)
    for row in rows:
        secret = bool(row.get("secret"))
        row["key"] = str(row.get("key") or row["instance_key"])
        row["value"] = "" if secret and mask_secret else str(row.get("value") or "")
        row["description"] = row.get("description") or ""
        row["masked"] = secret and mask_secret
    return rows


async def get_service_env(service_id: int) -> dict[str, str]:
    rows = await list_runtime_configs(service_id, "env_var", include_secrets=True)
    return {str(row.get("key") or row["instance_key"]): str(row.get("value") or "") for row in rows}


async def replace_service_env_vars(service_id: int, items: list[dict]) -> list[dict]:
    cleaned = []
    for item in items:
        key = (item.get("key") or "").strip()
        if key:
            cleaned.append({"instance_key": key, "key": key, "value": item.get("value") or "", "secret": bool(item.get("secret")), "description": item.get("description") or ""})
    await replace_runtime_configs(service_id, "env_var", cleaned)
    return await list_service_env_vars(service_id, mask_secret=False)


async def delete_service_env_var(service_id: int, key: str) -> bool:
    row = await get_runtime_config_by_key(service_id, "env_var", key)
    return await delete_runtime_config(row["id"]) if row else False


# ── 内置邮件 MCP：只读写统一表 ──

def _mail_config_to_dict(row: dict, *, mask_secret: bool = True) -> dict:
    data = dict(row)
    states = data.get("_secrets") or {}
    for key in ("password", "secret_key"):
        if mask_secret:
            data[key] = ""
            data[f"{key}_masked"] = states.get(key, {}).get("state") == "set"
    return data


async def list_mail_configs(service_id: int, *, mask_secret: bool = True) -> list[dict]:
    return [_mail_config_to_dict(row, mask_secret=mask_secret) for row in await list_runtime_configs(service_id, "mail_account", include_secrets=not mask_secret)]


async def get_mail_config(config_id: int, *, mask_secret: bool = True) -> dict | None:
    row = await get_runtime_config(config_id, include_secrets=not mask_secret)
    return _mail_config_to_dict(row, mask_secret=mask_secret) if row and row.get("config_type") == "mail_account" else None


async def create_mail_config(service_id: int, item: dict) -> dict:
    value = {"display_name": (item.get("display_name") or "").strip(), "username": (item.get("username") or "").strip(), "password": item.get("password") or "", "base_url": (item.get("base_url") or "").strip().rstrip("/"), "secret_key": item.get("secret_key") or "", "enabled": bool(item.get("enabled", True))}
    return _mail_config_to_dict(await create_runtime_config(service_id, "mail_account", value), mask_secret=True)


async def update_mail_config(config_id: int, item: dict) -> dict | None:
    patch = {key: item[key] for key in ("display_name", "username", "base_url", "enabled", "password", "secret_key", "revision") if key in item and item[key] is not None}
    for key in ("display_name", "username", "base_url"):
        if key in patch:
            patch[key] = str(patch[key]).strip()
    if "base_url" in patch:
        patch["base_url"] = patch["base_url"].rstrip("/")
    row = await update_runtime_config(config_id, patch, expected_revision=patch.get("revision"))
    return _mail_config_to_dict(row, mask_secret=True) if row else None


async def delete_mail_config(config_id: int) -> bool:
    if not await get_mail_config(config_id):
        return False
    for address in await list_mail_addresses(config_id):
        await delete_runtime_config(address["id"])
    return await delete_runtime_config(config_id)


async def list_mail_addresses(config_id: int) -> list[dict]:
    account = await get_runtime_config(config_id)
    if not account or account.get("config_type") != "mail_account":
        return []
    rows = await list_runtime_configs(int(account["service_id"]), "mail_address")
    return sorted((row for row in rows if row.get("parent_instance_key") == account["instance_key"]), key=lambda row: (not bool(row.get("is_primary")), int(row["id"])))


async def create_mail_address(config_id: int, item: dict) -> dict:
    account = await get_runtime_config(config_id)
    if not account or account.get("config_type") != "mail_account":
        raise ValueError("mail config not found")
    value = {"parent_instance_key": account["instance_key"], "address": (item.get("address") or "").strip().lower(), "source_address": (item.get("source_address") or "").strip().lower(), "is_primary": bool(item.get("is_primary", False))}
    return await create_runtime_config(int(account["service_id"]), "mail_address", value)


async def update_mail_address(address_id: int, item: dict) -> dict | None:
    patch = {key: item[key] for key in ("address", "source_address", "is_primary", "revision") if key in item and item[key] is not None}
    for key in ("address", "source_address"):
        if key in patch:
            patch[key] = str(patch[key]).strip().lower()
    return await update_runtime_config(address_id, patch, expected_revision=patch.get("revision"))


async def delete_mail_address(address_id: int) -> bool:
    return await delete_runtime_config(address_id)


async def get_mail_address_for_service(service_id: int, address: str) -> dict | None:
    normalized = (address or "").strip().lower()
    accounts = {row["id"]: row for row in await list_mail_configs(service_id, mask_secret=False) if row.get("enabled")}
    for account_id, account in accounts.items():
        for mapping in await list_mail_addresses(account_id):
            if str(mapping.get("address") or "").lower() == normalized:
                return {"address_id": mapping["id"], "address": mapping.get("address"), "source_address": mapping.get("source_address"), "is_primary": mapping.get("is_primary", False), "config_id": account["id"], "display_name": account.get("display_name"), "username": account.get("username"), "password": account.get("password"), "base_url": account.get("base_url"), "secret_key": account.get("secret_key"), "enabled": True}
    return None


async def get_mail_runtime_configs(service_id: int) -> list[dict]:
    configs = await list_mail_configs(service_id, mask_secret=False)
    for config in configs:
        config["addresses"] = await list_mail_addresses(config["id"])
    return configs


# ── MCP 用户体系 + 服务鉴权 ──

_TOKEN_PREFIX = "mcpu_"


def _gen_token() -> str:
    """生成 MCP 用户 token；带前缀便于识别、日志中好认。"""
    return _TOKEN_PREFIX + secrets.token_urlsafe(32)


def _mask_token(token: str) -> str:
    """token 脱敏：保留前缀 + 末 4 位，中间打码。"""
    if not token:
        return ""
    tail = token[-4:] if len(token) > 4 else ""
    return f"{_TOKEN_PREFIX}****{tail}"


def _mcp_user_to_dict(row, *, mask_token: bool = True) -> dict:
    d = dict(row)
    for key in ("created_at", "updated_at", "expires_at"):
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    usage_type = d.get("usage_type") or "external"
    # agent/task 永不返回明文 token；external 按 mask_token 决定。
    force_mask = usage_type in ("agent", "task")
    raw = d.get("token") or ""
    hint = d.get("token_hint") or (_mask_token(raw) if raw else "")
    d["token"] = hint if (mask_token or force_mask) else raw
    d.pop("token_hash", None)
    d["masked"] = mask_token or force_mask
    d.setdefault("usage_type", usage_type)
    return d


def _principal_row_to_dict(row) -> dict:
    d = dict(row)
    for key in ("created_at", "updated_at", "expires_at"):
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    d.pop("token", None)
    d.pop("token_hash", None)
    d["token"] = d.get("token_hint") or ""
    d["masked"] = True
    d.setdefault("usage_type", "external")
    return d


# token 可见性策略：agent/task 永不返回明文；external 在创建/轮换时返回一次。
USAGE_TYPES = ("agent", "external", "task")


def _should_expose_plaintext(usage_type: str) -> bool:
    return (usage_type or "external") == "external"


def _select_columns() -> str:
    return (
        "id, name, enabled, chat_enabled, description, token_hint, token_status, "
        "expires_at, owner_user_id, usage_type, created_at, updated_at"
    )


async def list_owned_mcp_principals(owner_user_id: str) -> list[dict]:
    """列出平台用户可见的 MCP principals；永不返回明文或 token hash。

    口径：自建(external 等 owner_user_id=$1) 归属创建者；平台级 agent/task
    principal（owner_user_id IS NULL）对用户侧可见——它们绑定公共 Agent/任务，
    不属于某个用户但仍需在用户侧 MCP 用户列表展示。管理端 /mcp/users 只列平台级
    external（agent/task 是会话级身份，不在管理端混显）。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT {_select_columns()} FROM mcp_users "
            "WHERE owner_user_id=$1 "
            "OR (owner_user_id IS NULL AND usage_type IN ('agent','task')) "
            "ORDER BY id",
            str(owner_user_id),
        )
    return await enrich_principal_descriptions([_principal_row_to_dict(row) for row in rows])


# agent/task principal 的备注展示长度：名字（task-6a3092e9）看不出在做什么，
# 用它绑定的任务内容 / Agent 名替代，截断到这个长度。
_PRINCIPAL_DESC_MAX = 40


async def enrich_principal_descriptions(principals: list[dict]) -> list[dict]:
    """把 agent/task principal 的 description 换成「它在做什么」。

    - task：绑定该 principal 的任务的 content（第一个问题）前 N 字
    - agent：绑定该 principal 的 Agent 的 display_name/name
      （Agent 的首条会话消息在 ClickHouse，跨库不 join；Agent 名本身可读）

    external 不动（用户自己填的备注）。查不到绑定关系时保留原 description。
    """
    from db import PostgresClient

    if not principals or not PostgresClient.pool:
        return principals
    task_ids = [int(p["id"]) for p in principals if p.get("usage_type") == "task"]
    agent_ids = [int(p["id"]) for p in principals if p.get("usage_type") == "agent"]
    if not task_ids and not agent_ids:
        return principals

    desc_by_principal: dict[int, str] = {}
    async with PostgresClient.pool.acquire() as conn:
        if task_ids:
            # mc_tasks 与 mcp_users 同库（monkeycode_compat 的 Tortoise 表建在同一 PG）。
            try:
                rows = await conn.fetch(
                    "SELECT mcp_user_id, content FROM mc_tasks "
                    "WHERE mcp_user_id = ANY($1::int[])",
                    task_ids,
                )
                for row in rows:
                    text = (row["content"] or "").strip().replace("\n", " ")
                    if text:
                        desc_by_principal[int(row["mcp_user_id"])] = text[:_PRINCIPAL_DESC_MAX]
            except Exception:  # noqa: BLE001 — 表缺失/未启用 compat 时保留原 description
                pass
        if agent_ids:
            try:
                rows = await conn.fetch(
                    "SELECT mcp_user_id, display_name, name FROM agents "
                    "WHERE mcp_user_id = ANY($1::int[])",
                    agent_ids,
                )
                for row in rows:
                    text = (row["display_name"] or row["name"] or "").strip()
                    if text:
                        desc_by_principal[int(row["mcp_user_id"])] = text[:_PRINCIPAL_DESC_MAX]
            except Exception:  # noqa: BLE001
                pass

    for p in principals:
        new_desc = desc_by_principal.get(int(p["id"]))
        if new_desc:
            p["description"] = new_desc
    return principals


async def get_owned_mcp_principal(principal_id: int, owner_user_id: str) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_select_columns()} FROM mcp_users "
            "WHERE id=$1 AND owner_user_id=$2",
            int(principal_id), str(owner_user_id),
        )
    return _principal_row_to_dict(row) if row else None


async def get_manageable_mcp_principal(principal_id: int, owner_user_id: str) -> dict | None:
    """读取可被某用户管理的 principal 脱敏 DTO。

    与 ``get_owned_mcp_principal`` 的区别：平台级 principal（owner_user_id IS
    NULL，绑定公共 Agent）对所有登录用户开放管理——公共 Agent 本身允许用户配置，
    其 principal 的 grants 需要同样可读可改。其它 principal 仍要求 owner 匹配。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_select_columns()} FROM mcp_users "
            "WHERE id=$1 AND (owner_user_id=$2 OR owner_user_id IS NULL)",
            int(principal_id), str(owner_user_id),
        )
    return _principal_row_to_dict(row) if row else None


async def create_owned_mcp_principal(
    owner_user_id: str, name: str, *, description: str = "", enabled: bool = True,
    usage_type: str = "external",
) -> dict:
    """创建用户自有 principal；明文 token 只注入本次返回值，不落库。

    usage_type 默认 external；agent/task 由各自入口传入。external 创建时返回明文，
    agent/task 不返回明文（masked=True，token=hint）。
    """
    from db import PostgresClient

    if usage_type not in USAGE_TYPES:
        raise ValueError("invalid usage_type")
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    token = _gen_token()
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO mcp_users(
                name, token, token_hash, token_hint, token_status,
                owner_user_id, description, enabled, chat_enabled, usage_type
            ) VALUES($1, NULL, $2, $3, 'active', $4, $5, $6, FALSE, $7)
            RETURNING {_select_columns()}
            """,
            name.strip(), hash_runtime_token(token), runtime_token_hint(token),
            str(owner_user_id), description or "", bool(enabled), usage_type,
        )
    result = _principal_row_to_dict(row)
    if _should_expose_plaintext(usage_type):
        result["token"] = token
        result["masked"] = False
    return result


async def update_owned_mcp_principal(
    principal_id: int, owner_user_id: str, *, name: str | None = None,
    description: str | None = None, enabled: bool | None = None,
) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = []
    params: list[Any] = []
    for field, value in (("name", name.strip() if name is not None else None),
                         ("description", description), ("enabled", enabled)):
        if value is not None:
            params.append(value)
            sets.append(f"{field}=${len(params)}")
    if not sets:
        return await get_owned_mcp_principal(principal_id, owner_user_id)
    sets.append("updated_at=now()")
    params.extend((int(principal_id), str(owner_user_id)))
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE mcp_users SET {', '.join(sets)} "
            f"WHERE id=${len(params)-1} AND owner_user_id=${len(params)} "
            "RETURNING id, name, enabled, chat_enabled, description, token_hint, token_status, "
            "expires_at, owner_user_id, created_at, updated_at",
            *params,
        )
    return _principal_row_to_dict(row) if row else None


async def update_manageable_mcp_principal(
    principal_id: int, owner_user_id: str, *, name: str | None = None,
    description: str | None = None, enabled: bool | None = None,
) -> dict | None:
    """与 ``update_owned_mcp_principal`` 同语义，但 owner 匹配放宽到 ``owner_user_id
    IS NULL``（平台级 principal）。

    调用方必须先用 ``get_manageable_mcp_principal`` 确认 usage_type 允许用户侧写——
    平台 external principal 是管理端专属配置，不能走这里。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = []
    params: list[Any] = []
    for field, value in (("name", name.strip() if name is not None else None),
                         ("description", description), ("enabled", enabled)):
        if value is not None:
            params.append(value)
            sets.append(f"{field}=${len(params)}")
    if not sets:
        return await get_manageable_mcp_principal(principal_id, owner_user_id)
    sets.append("updated_at=now()")
    params.extend((int(principal_id), str(owner_user_id)))
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE mcp_users SET {', '.join(sets)} "
            f"WHERE id=${len(params)-1} "
            f"AND (owner_user_id=${len(params)} OR owner_user_id IS NULL) "
            f"RETURNING {_select_columns()}",
            *params,
        )
    return _principal_row_to_dict(row) if row else None


async def rotate_owned_mcp_principal_token(principal_id: int, owner_user_id: str) -> dict | None:
    """轮换 principal token；旧 hash 原子失效，明文只返回一次。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    token = _gen_token()
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE mcp_users
            SET token=NULL, token_hash=$3, token_hint=$4, token_status='active', updated_at=now()
            WHERE id=$1 AND owner_user_id=$2
            RETURNING {_select_columns()}
            """,
            int(principal_id), str(owner_user_id), hash_runtime_token(token), runtime_token_hint(token),
        )
    if not row:
        return None
    result = _principal_row_to_dict(row)
    if _should_expose_plaintext(result.get("usage_type", "external")):
        result["token"] = token
        result["masked"] = False
    return result


async def set_owned_mcp_principal_token_status(
    principal_id: int, owner_user_id: str, status: str,
) -> dict | None:
    from db import PostgresClient

    if status not in ("active", "disabled"):
        raise ValueError("unsupported principal token status")
    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE mcp_users SET token_status=$3, updated_at=now()
            WHERE id=$1 AND owner_user_id=$2
            RETURNING {_select_columns()}
            """,
            int(principal_id), str(owner_user_id), status,
        )
    return _principal_row_to_dict(row) if row else None


async def create_platform_mcp_principal(
    name: str, *, description: str = "", enabled: bool = True, chat_enabled: bool = False,
    usage_type: str = "external",
) -> dict:
    """管理端创建平台级 principal（owner_user_id=NULL）。

    usage_type 只允许 agent|external；task 由任务服务内部建。external 返回明文 token 一次；
    agent 不返回明文（masked=True）。明文 token 均不落库。
    """
    from db import PostgresClient

    if usage_type not in ("agent", "external"):
        raise ValueError("invalid usage_type")
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    token = _gen_token()
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO mcp_users(
                name, token, token_hash, token_hint, token_status,
                owner_user_id, description, enabled, chat_enabled, usage_type
            ) VALUES($1, NULL, $2, $3, 'active', NULL, $4, $5, $6, $7)
            RETURNING {_select_columns()}
            """,
            name.strip(), hash_runtime_token(token), runtime_token_hint(token),
            description or "", bool(enabled), bool(chat_enabled), usage_type,
        )
    result = _principal_row_to_dict(row)
    if _should_expose_plaintext(usage_type):
        result["token"] = token
        result["masked"] = False
    return result


async def set_mcp_user_token_status(user_id: int, status: str) -> dict | None:
    """管理端禁用/启用 principal token（token_status: active|disabled）。"""
    if status not in ("active", "disabled"):
        raise ValueError("unsupported principal token status")
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE mcp_users SET token_status=$2, updated_at=now()
            WHERE id=$1
            RETURNING {_select_columns()}
            """,
            int(user_id), status,
        )
    return _principal_row_to_dict(row) if row else None


async def delete_owned_mcp_principal(principal_id: int, owner_user_id: str) -> bool:
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM mcp_users WHERE id=$1 AND owner_user_id=$2",
            int(principal_id), str(owner_user_id),
        )
    return result.endswith(" 1")


# ── 任务/agent 内部生命周期助手 ──────────────────────────────────────
# 这些由使用方（task_service / agent 入口）调用，创建 usage_type=task 的普通 principal。
# mcp_users 不反向存使用方关系；使用方在自己表里持有 mcp_user_id。

async def create_task_mcp_principal(owner_user_id: str, name: str, *, description: str = "") -> int:
    """任务创建时建一个普通 task principal；返回 principal id。

    token 明文不返回（task token 对使用侧全程隐藏）。操作参数（如绑定的 CDP
    客户端 / 邮箱账户）由调用方随后通过 set_principal_params 写入。
    """
    principal = await create_owned_mcp_principal(
        owner_user_id, name, description=description or "task principal",
        enabled=True, usage_type="task",
    )
    return int(principal["id"])


async def create_agent_mcp_principal(owner_user_id: str, name: str, *, description: str = "") -> int:
    """Agent 创建时建一个普通 agent principal；返回 principal id。

    token 明文不返回（agent token 对使用侧全程隐藏）。操作参数（绑定的 CDP
    客户端 / 邮箱账户等）由用户在 Agent 入口通过 set_principal_params 配置。
    """
    principal = await create_owned_mcp_principal(
        owner_user_id, name, description=description or "agent principal",
        enabled=True, usage_type="agent",
    )
    return int(principal["id"])


async def set_principal_enabled(principal_id: int, enabled: bool) -> bool:
    """启用/禁用整个 principal（task 暂停/结束的安全闭环用）。owner 无关。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE mcp_users SET enabled=$2, updated_at=now() WHERE id=$1",
            int(principal_id), bool(enabled),
        )
    return result.endswith(" 1")


async def delete_mcp_principal(principal_id: int) -> bool:
    """按 id 删除 principal（task 删除时调用）。owner 无关；FK 级联清 grants。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute("DELETE FROM mcp_users WHERE id=$1", int(principal_id))
    return result.endswith(" 1")


async def get_mcp_principal(principal_id: int) -> dict | None:
    """按 id 读取单个 principal 的脱敏 DTO（与用户侧列表同序列化器）。

    owner 无关；调用方已在路由层做 ownership 校验。永不返回明文 token。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_select_columns()} FROM mcp_users WHERE id=$1",
            int(principal_id),
        )
    return _principal_row_to_dict(row) if row else None


async def get_mcp_principal_usage_type(principal_id: int) -> str | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT usage_type FROM mcp_users WHERE id=$1", int(principal_id),
        )
    return value


# 调用者拿到 mcp_id 不能直接用：agent 勾选 / 任务勾选 / 外部 grant 保存都必须先过这里。
# 三条任一成立即放行：自己的个人服务 / 所在分组被授权 / 显式 principal grant。

async def _user_group_ids(user_id: str | None) -> list[str]:
    """调用者所在分组 id（字符串列表）。复用 TeamGroupMember 的标准查询口径。"""
    if not user_id:
        return []
    try:
        from monkeycode_compat.models import TeamGroupMember
    except Exception:  # noqa: BLE001 — compat 层未启用时无分组概念
        return []
    try:
        ids = await TeamGroupMember.filter(user_id=str(user_id)).values_list("group_id", flat=True)
    except Exception:  # noqa: BLE001
        return []
    return [str(g) for g in ids]


async def can_use_service(*, user_id: str | None, service_id: int, groups: list[str] | None = None) -> bool:
    """调用者能否使用某 MCP 服务。

    1. 自己的个人服务：mcp_services.user_id == user_id
    2. 分组授权：mcp_services.group_ids ∩ groups 非空
    3. 显式 grant：该 user_id 拥有的 principal 对该 service 有 enabled、child_mode<>none 的 grant
    user_id 为 None（管理员）→ 全量放行。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    if user_id is None:
        return True
    gid_list = [str(g) for g in (groups if groups is not None else await _user_group_ids(user_id))]
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id, group_ids FROM mcp_services WHERE id=$1",
            int(service_id),
        )
    if not row:
        return False
    # 1. 自己的个人服务
    if row["user_id"] is not None and str(row["user_id"]) == str(user_id):
        return True
    # 2. 分组授权（group_ids 是 JSONB 字符串数组，用 ?| 交集）
    svc_groups = [str(g) for g in (row["group_ids"] or [])]
    if gid_list and any(g in svc_groups for g in gid_list):
        return True
    # 3. 服务级授权（mcp_service_users，principal 与服务的关系）
    async with PostgresClient.pool.acquire() as conn:
        granted = await conn.fetchval(
            """
            SELECT 1 FROM mcp_service_users su
            JOIN mcp_users u ON u.id=su.user_id
            WHERE su.service_id=$1 AND u.owner_user_id=$2 AND u.enabled=TRUE
            LIMIT 1
            """,
            int(service_id), str(user_id),
        )
    return bool(granted)


async def can_use_principal(*, user_id: str | None, principal_id: int) -> bool:
    """调用者能否绑定某 principal：
    - user_id 为 None（管理员）→ 放行
    - principal.owner_user_id == user_id（自己的）→ 放行
    - owner_user_id IS NULL 的平台 principal → 仅管理员可用（上面已放行）
    其余 → 拒绝。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    if user_id is None:
        return True
    async with PostgresClient.pool.acquire() as conn:
        owner = await conn.fetchval(
            "SELECT owner_user_id FROM mcp_users WHERE id=$1", int(principal_id),
        )
    if owner is None:
        return False  # 平台 principal，非管理员不能绑
    return str(owner) == str(user_id)


def _param_row_to_dict(row) -> dict:
    d = dict(row)
    if d.get("created_at") is not None:
        d["created_at"] = d["created_at"].isoformat()
    return d


async def list_principal_params(principal_id: int) -> list[dict]:
    """返回 principal 的全部操作参数 [{param_key, param_value, created_at}]。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT param_key, param_value, created_at FROM mcp_user_params "
            "WHERE principal_id=$1 ORDER BY id",
            int(principal_id),
        )
    return [_param_row_to_dict(r) for r in rows]


async def get_principal_param(principal_id: int, key: str) -> str | None:
    """driver 鉴权定位用：取 principal 的某个参数值，无则 None。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT param_value FROM mcp_user_params WHERE principal_id=$1 AND param_key=$2",
            int(principal_id), str(key),
        )


# param_key → resource_type：校验 param_value 指向的 builtin_tool_resources 资源真实存在。
# device_id：device_control 插件的每 principal 设备绑定（adapter._resolve_device_id
# 读它把请求 token 解析到具体设备）。
_PARAM_DETAIL_TYPE = {
    "cdp_client_id": "cdp_client",
    "mail_account_id": "mail_account",
    "device_id": "device",
}


async def set_principal_params(
    principal_id: int, params: list[dict], *, owner_user_id: str | None = None,
) -> list[dict]:
    """全量替换 principal 的操作参数。校验 param_value 指向的资源存在且归属正确。

    owner_user_id=None：管理员视角（只校验存在）。指定时校验资源属于该 owner。
    principal 不存在抛 LookupError；param 校验失败抛 ValueError。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    normalized: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw in params:
        key = str(raw.get("param_key") or "").strip()
        value = str(raw.get("param_value") or "").strip()
        if not key or not value:
            raise ValueError("param_key/param_value 不能为空")
        if key in seen:
            raise ValueError(f"重复的 param_key: {key}")
        seen.add(key)
        normalized.append((key, value))

    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            if owner_user_id is None:
                exists = await conn.fetchval(
                    "SELECT 1 FROM mcp_users WHERE id=$1", int(principal_id),
                )
            else:
                exists = await conn.fetchval(
                    "SELECT 1 FROM mcp_users WHERE id=$1 "
                    "AND (owner_user_id=$2 OR owner_user_id IS NULL)",
                    int(principal_id), str(owner_user_id),
                )
            if not exists:
                raise LookupError("MCP principal 不存在")

            for key, value in normalized:
                detail_type = _PARAM_DETAIL_TYPE.get(key)
                if detail_type is not None:
                    if owner_user_id is None:
                        row = await conn.fetchrow(
                            "SELECT 1 FROM builtin_tool_resources "
                            "WHERE id=$1 AND resource_type=$2 "
                            "AND COALESCE((data->>'enabled')::boolean, TRUE)=TRUE",
                            int(value), detail_type,
                        )
                    else:
                        row = await conn.fetchrow(
                            "SELECT 1 FROM builtin_tool_resources "
                            "WHERE id=$1 AND resource_type=$2 "
                            "AND COALESCE((data->>'enabled')::boolean, TRUE)=TRUE "
                            "AND owner_user_id=$3",
                            int(value), detail_type, str(owner_user_id),
                        )
                    if not row:
                        raise ValueError(f"参数 {key}={value} 指向的资源不存在或无权访问")

            await conn.execute("DELETE FROM mcp_user_params WHERE principal_id=$1", int(principal_id))
            for key, value in normalized:
                await conn.execute(
                    "INSERT INTO mcp_user_params(principal_id, param_key, param_value) VALUES($1,$2,$3)",
                    int(principal_id), key, value,
                )
    return await list_principal_params(principal_id)


async def list_owned_resource_principal_params(owner_user_id: str) -> dict[int, list[dict]]:
    """资源页只读概览：当前 owner 的工具资源分别配给了哪些 principals（按 param）。

    返回 {resource_id: [{principal_id, name, param_key, param_value}]}。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return {}
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.param_key, p.param_value, u.id AS principal_id, u.name,
                   r.id AS resource_id
            FROM mcp_user_params p
            JOIN mcp_users u ON u.id=p.principal_id
            JOIN builtin_tool_resources r ON r.id=p.param_value::bigint
            WHERE p.param_key IN ('cdp_client_id', 'mail_account_id')
              AND u.enabled=TRUE AND u.owner_user_id=$1
              AND r.owner_user_id=$1
            ORDER BY u.name, u.id
            """,
            str(owner_user_id),
        )
    result: dict[int, list[dict]] = {}
    for row in rows:
        result.setdefault(int(row["resource_id"]), []).append({
            "principal_id": int(row["principal_id"]), "name": row["name"],
            "param_key": row["param_key"], "param_value": row["param_value"],
        })
    return result


async def list_mcp_users(*, mask_token: bool = True) -> list[dict]:
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, token, token_hint, token_status, expires_at, owner_user_id, enabled, chat_enabled, description, usage_type, created_at, updated_at FROM mcp_users ORDER BY id"
        )
    return [_mcp_user_to_dict(r, mask_token=mask_token) for r in rows]


async def list_platform_mcp_users(*, mask_token: bool = True) -> list[dict]:
    """管理端视角：只列平台级 principal（owner_user_id IS NULL）。用户侧建的
    external/agent/task 不在管理端混显，归用户侧「用户管理」入口。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, token, token_hint, token_status, expires_at, owner_user_id, enabled, chat_enabled, description, usage_type, created_at, updated_at FROM mcp_users WHERE owner_user_id IS NULL ORDER BY id"
        )
    return [_mcp_user_to_dict(r, mask_token=mask_token) for r in rows]


async def get_mcp_user(user_id: int, *, mask_token: bool = True) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, token, token_hint, token_status, expires_at, owner_user_id, enabled, chat_enabled, description, usage_type, created_at, updated_at FROM mcp_users WHERE id=$1",
            user_id,
        )
    return _mcp_user_to_dict(row, mask_token=mask_token) if row else None


async def create_mcp_user(
    name: str,
    *,
    token: str | None = None,
    description: str = "",
    enabled: bool = True,
    chat_enabled: bool = False,
) -> dict:
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    token = (token or "").strip() or _gen_token()
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO mcp_users(name, token, token_hash, token_hint, description, enabled, chat_enabled)
            VALUES($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, name, token, token_hint, token_status, expires_at, owner_user_id,
                      enabled, chat_enabled, description, usage_type, created_at, updated_at
            """,
            name.strip(),
            token,
            hash_runtime_token(token),
            runtime_token_hint(token),
            description or "",
            enabled,
            chat_enabled,
        )
    # 创建后返回真实 token，方便管理员立刻复制。
    return _mcp_user_to_dict(row, mask_token=False)


async def update_mcp_user(
    user_id: int,
    *,
    name: str | None = None,
    token: str | None = None,
    description: str | None = None,
    enabled: bool | None = None,
    chat_enabled: bool | None = None,
) -> dict | None:
    """局部更新 MCP 用户。传入的字段才更新；token 传空串不覆盖（走 rotate 接口重置）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    sets: list[str] = []
    params: list[Any] = []
    idx = 1
    if name is not None:
        sets.append(f"name=${idx}"); params.append(name.strip()); idx += 1
    if token:
        cleaned_token = token.strip()
        sets.extend((f"token=${idx}", f"token_hash=${idx + 1}", f"token_hint=${idx + 2}"))
        params.extend((cleaned_token, hash_runtime_token(cleaned_token), runtime_token_hint(cleaned_token)))
        idx += 3
    if description is not None:
        sets.append(f"description=${idx}"); params.append(description); idx += 1
    if enabled is not None:
        sets.append(f"enabled=${idx}"); params.append(enabled); idx += 1
    if chat_enabled is not None:
        sets.append(f"chat_enabled=${idx}"); params.append(chat_enabled); idx += 1
    if not sets:
        return await get_mcp_user(user_id, mask_token=False)
    sets.append("updated_at=now()")
    params.append(user_id)
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            if enabled is False:
                await _strip_user_from_instances(conn, user_id)
            row = await conn.fetchrow(
                f"UPDATE mcp_users SET {', '.join(sets)} WHERE id=${idx} "
                "RETURNING id, name, token, token_hint, token_status, expires_at, owner_user_id, enabled, chat_enabled, description, usage_type, created_at, updated_at",
                *params,
            )
    return _mcp_user_to_dict(row, mask_token=False) if row else None


async def rotate_mcp_user_token(user_id: int) -> dict | None:
    """重新生成 token。返回含真实 token 的用户。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    token = _gen_token()
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE mcp_users
            SET token=$2, token_hash=$3, token_hint=$4, token_status='active', updated_at=now()
            WHERE id=$1
            RETURNING id, name, token, token_hint, token_status, expires_at, owner_user_id,
                      enabled, chat_enabled, description, usage_type, created_at, updated_at
            """,
            user_id,
            token,
            hash_runtime_token(token),
            runtime_token_hint(token),
        )
    return _mcp_user_to_dict(row, mask_token=False) if row else None


async def get_mcp_user_by_token(token: str) -> dict | None:
    """按 token 精确查用户（走 idx_mcp_users_token）。返回含真实 token 的行。

    供 cdp-bridge 聊天链路做「会话 token → 用户 → chat_enabled/绑定 agent」解析；
    仅服务端内部调用，不对外透出。
    """
    from db import PostgresClient

    token = (token or "").strip()
    if not token or not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, token, token_hint, token_status, expires_at, owner_user_id, enabled, "
            "chat_enabled, description, created_at, updated_at FROM mcp_users "
            "WHERE token_hash=$1 OR (token_hash IS NULL AND token=$2)",
            hash_runtime_token(token),
            token,
        )
    return _mcp_user_to_dict(row, mask_token=False) if row else None


async def list_agents_for_mcp_user(user_id: int) -> list[dict]:
    """该 MCP 用户绑定且 enabled 的 agent 概要（历史：按 agents.mcp_user_id 反推）。

    已弃用于网页对话面板路径——面板可选 agent 现按连接的 CDP 客户端 agent_ids
    决定（见 list_agents_by_ids）。保留以兼容其它调用方。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, display_name, description FROM agents "
            "WHERE mcp_user_id=$1 AND enabled=TRUE ORDER BY id",
            user_id,
        )
    return [dict(r) for r in rows]


async def list_agents_by_ids(agent_ids: list[int]) -> list[dict]:
    """按 id 集合返回 enabled agent 概要（网页对话面板可选 agent 列表）。

    保持传入顺序无关、只返回存在且启用的；空输入返回空。
    """
    from db import PostgresClient

    ids = list(dict.fromkeys(int(a) for a in (agent_ids or [])))
    if not ids or not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, display_name, description FROM agents "
            "WHERE id=ANY($1::int[]) AND enabled=TRUE ORDER BY id",
            ids,
        )
    return [dict(r) for r in rows]


async def list_agent_ids(agent_ids: list[int]) -> list[int]:
    """返回 agent_ids 里存在且 enabled 的 id 子集（供配置层校验）。"""
    return [row["id"] for row in await list_agents_by_ids(agent_ids)]


async def list_all_agents_brief() -> list[dict]:
    """全部 enabled agent 概要，供配置契约 reference_options.agents 使用。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, display_name, description FROM agents "
            "WHERE enabled=TRUE ORDER BY id",
        )
    return [dict(r) for r in rows]


async def get_agent_mcp_principal_id(agent_id: int) -> int | None:
    """把 Agent 身份映射到其绑定的运行 principal；Agent 与 MCP 仍是独立实体。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT mcp_user_id FROM agents WHERE id=$1 AND enabled=TRUE",
            int(agent_id),
        )
    return int(value) if value is not None else None


async def list_agent_ids_authorized_for_cdp_client(client_id: str) -> set[int]:
    """反查：绑定的 principal 有 ``cdp_client_id`` param 指向该客户端的 enabled agent id 集。

    这是「谁真正能操作这个浏览器」的口径——agent 的 browser_* 工具在运行时按其
    principal 的 cdp_client_id param 定位会话池（见 cdp_bridge_plugin._principal_cdp_client_id），
    没有该 param 的 agent 即便被客户端勾选也调不动。client_id = cdp_client 明细行 id
    的字符串（mcp_user_params.param_value 存的就是字符串）。
    """
    from db import PostgresClient

    cid = (client_id or "").strip()
    if not cid or not PostgresClient.pool:
        return set()
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT a.id FROM agents a "
            "JOIN mcp_user_params p ON p.principal_id = a.mcp_user_id "
            "WHERE a.enabled = TRUE "
            "AND p.param_key = 'cdp_client_id' AND p.param_value = $1",
            cid,
        )
    return {int(r["id"]) for r in rows}


async def get_cdp_client_agent_ids(client_id: str) -> list[int] | None:
    """按 client_id 取其可操作 agent_ids。client_id = cdp_client 资源行 id。

    用户侧 CDP 客户端存在 ``builtin_tool_resources``（resource_type='cdp_client'，
    client_id = 资源行 id，见 ``builtin_tool_store.cdp_client_present``）。旧
    ``mcp_runtime_configs`` 的 cdp_client 已随迁移废弃，故这里直接读资源表——否则
    用户页存的 agent_ids 网页对话网关读不到（表错位）。

    返回 None 表示该 client 不存在或已停用；返回列表（可能为空）表示存在。
    空列表 ⇒ 该客户端不允许操作任何 agent（网页对话面板不列出任何 agent）。
    """
    from db import PostgresClient

    cid = (client_id or "").strip()
    if not cid or not PostgresClient.pool:
        return None
    try:
        resource_id = int(cid)
    except (TypeError, ValueError):
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT data FROM builtin_tool_resources "
            "WHERE resource_type='cdp_client' AND id=$1 LIMIT 1",
            resource_id,
        )
    if not row:
        return None
    data = _json_value(row.get("data"), {})
    if not data.get("enabled", True):
        return []
    return _coerce_id_list(data.get("agent_ids"))


async def get_cdp_client_owner_user_id(client_id: str) -> str | None:
    """CDP 客户端资源所属的 C 端用户 uid（builtin_tool_resources.owner_user_id）。

    网页对话没有 C 端 session cookie，只有 X-Cdp-Client-Id，所以「谁在用」这条身份
    要从客户端资源反查 owner——AI 在网页对话里建的定时任务等副作用产物要归到这个
    人名下，否则用户态列表看不到。
    """
    from db import PostgresClient

    cid = (client_id or "").strip()
    if not cid or not PostgresClient.pool:
        return None
    try:
        resource_id = int(cid)
    except (TypeError, ValueError):
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT owner_user_id FROM builtin_tool_resources "
            "WHERE resource_type='cdp_client' AND id=$1 LIMIT 1",
            resource_id,
        )
    owner = row["owner_user_id"] if row else None
    return str(owner) if owner else None


async def list_agents_for_cdp_client(client_id: str) -> list[dict]:
    """网页对话面板可选 agent：**客户端勾选的 agent_ids ∩ 拥有该 CDP 客户端资源授权**。

    勾选集来自 builtin_tool_resources 里的 agent_ids（管理员/用户在客户端设置里选）；
    授权集来自绑定 principal 的 cdp_client_id param（真正能驱动这个浏览器）。取交集
    避免列出「勾了但 browser_* 工具调不动」的 agent。任一为空 ⇒ 空列表。
    """
    selected = await get_cdp_client_agent_ids(client_id)
    if not selected:
        return []
    authorized = await list_agent_ids_authorized_for_cdp_client(client_id)
    if not authorized:
        return []
    eligible = [aid for aid in selected if aid in authorized]
    if not eligible:
        return []
    return await list_agents_by_ids(eligible)


async def recompute_service_users_from_resources(service_id: int) -> list[int]:
    """按服务下所有配置资源的 user_ids 并集，回写服务级 mcp_service_users。

    方向翻转后，cdp_client / mail_account 每个资源携带「可操作用户」列表；
    该并集即为该服务连接鉴权（get_service_auth.allowed_tokens）的授权用户集。
    对没有资源携带 user_ids 的服务（如通用服务），本函数不应被调用——它们的
    授权仍由「服务管理弹层」直接 set_service_users 维护。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    union: list[int] = []
    for config_type in ("cdp_client", "mail_account"):
        for row in await list_runtime_configs(service_id, config_type):
            for uid in _coerce_id_list(row.get("user_ids"), fallback=row.get("user_id")):
                if uid not in union:
                    union.append(uid)
    return await set_service_users(service_id, union)


def _coerce_id_list(raw: Any, *, fallback: Any = None) -> list[int]:
    """规整 JSONB 里的 id 列表为去重 int 列表；兼容旧单值 fallback。"""
    items = raw if isinstance(raw, list) else None
    if items is None and fallback not in (None, ""):
        items = [fallback]
    out: list[int] = []
    for item in items or []:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value not in out:
            out.append(value)
    return out


_STRIP_USER_FROM_INSTANCES_SQL = """
UPDATE mcp_runtime_configs
SET data = jsonb_set(
        data,
        '{user_ids}',
        COALESCE((
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(data->'user_ids') elem
            WHERE elem <> to_jsonb($1::int)
        ), '[]'::jsonb)
    ),
    updated_at = now()
WHERE config_type IN ('cdp_client', 'mail_account')
  AND jsonb_typeof(data->'user_ids') = 'array'
  AND data->'user_ids' @> to_jsonb($1::int)
"""


async def _strip_user_from_instances(conn, user_id: int) -> None:
    """从所有 cdp_client / mail_account 的 user_ids 数组里移除该用户 id。

    方向翻转后「可操作用户」存在实例的 user_ids 里；用户删除/停用时需同步清理，
    否则会残留失效 id（虽然运行时映射与 get_service_auth 都会按 enabled 过滤，
    但保持数据整洁避免误导）。
    """
    await conn.execute(_STRIP_USER_FROM_INSTANCES_SQL, int(user_id))


async def delete_mcp_user(user_id: int) -> bool:
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            await _strip_user_from_instances(conn, user_id)
            result = await conn.execute("DELETE FROM mcp_users WHERE id=$1", user_id)
    return result.endswith("0") is False


async def list_service_users(service_id: int) -> list[int]:
    """返回某服务已授权的 MCP 用户 id 列表。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM mcp_service_users WHERE service_id=$1 ORDER BY user_id",
            service_id,
        )
    return [r["user_id"] for r in rows]


async def list_services_for_mcp_user(user_id: int) -> list[int]:
    """Return service ids currently authorized for one shared MCP user."""
    from db import PostgresClient
    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT service_id FROM mcp_service_users WHERE user_id=$1 ORDER BY service_id", user_id
        )
    return [row["service_id"] for row in rows]


async def set_services_for_mcp_user(user_id: int, service_ids: list[int]) -> tuple[list[int], list[int]]:
    """Atomically replace one user's allowed services and return (before, after)."""
    from db import PostgresClient
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    unique_ids = list(dict.fromkeys(int(service_id) for service_id in service_ids))
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            before_rows = await conn.fetch(
                "SELECT service_id FROM mcp_service_users WHERE user_id=$1", user_id
            )
            before = [row["service_id"] for row in before_rows]
            if unique_ids:
                found_rows = await conn.fetch(
                    "SELECT id FROM mcp_services WHERE id=ANY($1::int[])", unique_ids
                )
                found = {row["id"] for row in found_rows}
                missing = sorted(set(unique_ids) - found)
                if missing:
                    raise ValueError(f"MCP services not found: {missing}")
            removed = set(before) - set(unique_ids)
            if removed:
                await conn.execute(
                    "DELETE FROM mcp_runtime_configs WHERE config_type='cdp_client' AND data->>'user_id'=$1 AND service_id=ANY($2::int[])",
                    str(user_id), list(removed),
                )
            await conn.execute("DELETE FROM mcp_service_users WHERE user_id=$1", user_id)
            for service_id in unique_ids:
                await conn.execute(
                    "INSERT INTO mcp_service_users(service_id, user_id) VALUES($1, $2)",
                    service_id, user_id,
                )
    return before, unique_ids


async def set_service_users(service_id: int, user_ids: list[int]) -> list[int]:
    """全量替换某服务的授权用户（事务内 DELETE + INSERT）。

    只写授权关系本身。cdp_client/mail_account 实例的清理由用户删除/停用路径负责，
    不在此处联动删除实例（服务级用户集现在由实例 user_ids 反推，见
    recompute_service_users_from_resources）。只插入仍存在的用户，避免 FK 冲突。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    unique_ids = list(dict.fromkeys(int(u) for u in user_ids))
    async with PostgresClient.pool.acquire() as conn:
        async with conn.transaction():
            if unique_ids:
                existing_rows = await conn.fetch(
                    "SELECT id FROM mcp_users WHERE id=ANY($1::int[])", unique_ids,
                )
                existing = {row["id"] for row in existing_rows}
                unique_ids = [uid for uid in unique_ids if uid in existing]
            await conn.execute("DELETE FROM mcp_service_users WHERE service_id=$1", service_id)
            for uid in unique_ids:
                await conn.execute(
                    "INSERT INTO mcp_service_users(service_id, user_id) VALUES($1, $2) "
                    "ON CONFLICT(service_id, user_id) DO NOTHING",
                    service_id,
                    uid,
                )
            # mcp_service_users 是普通服务级鉴权的唯一真相源（grants 表已移除，
            # principal 的内置工具资源绑定走 mcp_user_params，与 service 级授权无关）。
    return await list_service_users(service_id)


async def set_service_auth_enabled(service_id: int, enabled: bool) -> dict | None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE mcp_services SET auth_enabled=$2, updated_at=now() WHERE id=$1 RETURNING *",
            service_id,
            enabled,
        )
    return service_row_to_dict(row) if row else None


async def get_service_auth(service_id: int) -> dict:
    """Return the service auth snapshot and non-secret authorized user IDs.

    ``allowed_tokens`` is runtime-only authentication material. API responses may
    use ``allowed_user_ids`` to expose identities without serializing tokens.
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return {"auth_enabled": False, "allowed_tokens": set(), "allowed_user_ids": set()}
    async with PostgresClient.pool.acquire() as conn:
        enabled = await conn.fetchval(
            "SELECT auth_enabled FROM mcp_services WHERE id=$1", service_id
        )
        rows = await conn.fetch(
            """
            SELECT u.id, u.token FROM mcp_service_users su
            JOIN mcp_users u ON u.id = su.user_id
            WHERE su.service_id=$1 AND u.enabled=TRUE
            """,
            service_id,
        )
    return {
        "auth_enabled": bool(enabled),
        "allowed_tokens": {r["token"] for r in rows},
        "allowed_user_ids": {r["id"] for r in rows},
    }


# CDP session grants were removed: pages are owned exclusively by their
# authenticated client and cannot be borrowed across clients or users.
