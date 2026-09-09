"""内置工具（CDP / 邮箱 / 设备控制）统一存储层——资源一级模型。

浏览器客户端（cdp_client）、邮箱账户（mail_account + mail_address）、已配对设备
（device）都是用户直接拥有的一级资源，建在两张通用表上
（db.PostgresClient.create_builtin_tool_tables）：

- builtin_tool_resources：一行 = 一个用户拥有的一级资源。owner_user_id 直挂资源，
  resource_type 区分类型，data/secret 分列（沿用 mcp_runtime_configs 的遮蔽口径）。
- builtin_tool_tokens：统一 MCP 访问 token（内置工具 + 外部 MCP 共用）。目标二选一：
  resource_id（内置工具资源）或 service_id（外部 MCP 服务），恰好一个非空。每行一个
  token，绑一个 target（agent/node/user/external）；自用（agent/node/user）不显示
  明文；分享外部（external）可显示。带 status（active/disabled）与 expires_at 时效。
  token 明文永不落库，仅存 sha256 哈希 + hint。鉴权收口只查这一张表。

安全约束（本层保证）：
- token 只存哈希；签发时明文只回一次，调用方负责一次性返回、绝不再存。
- secret 真值仅在 include_secrets=True 时读取，默认遮蔽。
- 鉴权入口 resolve_token 按 token_hash 精确定位，校验 status=active 且未过期。

Storage only —— 不触碰模型请求主链路。token 生成/哈希/hint 复用 mcp_plugin_store 的口径，
保持与既有 CDP token 相同的形态（前缀 cdp_）。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

# 复用既有 token 口径：哈希算法、hint 格式与现有 CDP token 一致，迁移时不破坏形态。
from mcp_plugin_store import (
    generate_runtime_token,
    hash_runtime_token,
    runtime_token_hint,
)


# 每种 detail_type 的 secret 字段（进 secret_data，明文不外泄）。
# 与 mcp_plugin_store._SECRET_FIELDS 的 mail_account 口径一致。
_DETAIL_SECRET_FIELDS: dict[str, frozenset[str]] = {
    "cdp_client": frozenset(),
    "mail_account": frozenset({"password", "secret_key"}),
    "mail_address": frozenset(),
    # device_control 设备：token 只存 sha256（放 data，与 cdp_client 同口径——
    # 哈希不可逆，不是明文凭据，且放 data 才能在解除配对时传空清除）。
    "device": frozenset(),
}
# detail 信封元字段：不进 data / secret 分列。
_RESOURCE_META_KEYS = {"id", "owner_user_id", "resource_type", "detail_type", "revision",
                      "created_at", "updated_at", "_secrets"}
_RESOURCE_TIME_KEYS = ("created_at", "updated_at")


def _split_detail_value(
    resource_type: str, value: dict, existing_secrets: dict | None = None
) -> tuple[dict, dict]:
    """把资源值按类型的 secret 字段拆成 (data, secret_data)。"""
    secret_fields = _DETAIL_SECRET_FIELDS.get(resource_type, frozenset())
    data: dict = {}
    secrets: dict = dict(existing_secrets or {})
    for key, raw in value.items():
        if key in _RESOURCE_META_KEYS:
            continue
        if key in secret_fields:
            if raw not in (None, ""):
                secrets[key] = raw
            data.pop(key, None)
        else:
            data[key] = raw
            secrets.pop(key, None)
    return data, secrets


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


# ── 资源行 → dict ────────────────────────────────────────────────────────────

def resource_row_to_dict(row, *, include_secrets: bool = False) -> dict:
    """一级资源行 → dict；默认永不暴露 secret 真值或 token hash。"""
    if row is None:
        return None
    d = dict(row)
    for key in ("created_at", "updated_at"):
        if d.get(key) is not None and hasattr(d[key], "isoformat"):
            d[key] = d[key].isoformat()
    if not include_secrets:
        d.pop("token_hash", None)
        d.pop("secret_data", None)
    return d


def _resource_value(row, *, include_secrets: bool = False) -> dict:
    """把资源表 envelope 展平为既有 detail 风格 DTO。"""
    data = _json_value(row.get("data"), {})
    secrets = _json_value(row.get("secret_data"), {})
    result = {**data, "id": row["id"], "owner_user_id": row["owner_user_id"],
              "resource_type": row["resource_type"], "detail_type": row["resource_type"],
              "revision": int(row.get("revision") or 1)}
    result["_secrets"] = {k: {"state": "set" if bool(v) else "unset"} for k, v in secrets.items()}
    if include_secrets:
        result.update(secrets)
    for key in ("created_at", "updated_at"):
        if row.get(key) is not None and hasattr(row[key], "isoformat"):
            result[key] = row[key].isoformat()
    return result


async def list_resources(
    resource_type: str | None = None,
    owner_user_id: str | None = None,
    enabled: bool | None = None,
    *, include_secrets: bool = False,
) -> list[dict]:
    """列出一级资源，按 owner/type/enabled 过滤，不依赖实例表。"""
    from db import PostgresClient
    if not PostgresClient.pool:
        return []
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1
    if resource_type is not None:
        clauses.append(f"resource_type=${idx}"); params.append(resource_type); idx += 1
    if owner_user_id is not None:
        clauses.append(f"owner_user_id=${idx}"); params.append(str(owner_user_id)); idx += 1
    if enabled is not None:
        clauses.append(f"COALESCE((data->>'enabled')::boolean, TRUE)=${idx}"); params.append(enabled); idx += 1
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT * FROM builtin_tool_resources {where} ORDER BY id", *params)
    return [_resource_value(dict(row), include_secrets=include_secrets) for row in rows]


async def get_resource(resource_id: int, *, owner_user_id: str | None = None, include_secrets: bool = False) -> dict | None:
    from db import PostgresClient
    if not PostgresClient.pool:
        return None
    clauses = ["id=$1"]
    params: list[Any] = [int(resource_id)]
    if owner_user_id is not None:
        clauses.append("owner_user_id=$2"); params.append(str(owner_user_id))
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT * FROM builtin_tool_resources WHERE {' AND '.join(clauses)}", *params)
    return _resource_value(dict(row), include_secrets=include_secrets) if row else None


async def create_resource(owner_user_id: str, resource_type: str, value: dict) -> dict:
    from db import PostgresClient
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    data, secret_data = _split_detail_value(resource_type, value)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO builtin_tool_resources(owner_user_id, resource_type, data, secret_data)
               VALUES($1,$2,$3::jsonb,$4::jsonb) RETURNING *""",
            str(owner_user_id), resource_type, json.dumps(data, ensure_ascii=False),
            json.dumps(secret_data, ensure_ascii=False),
        )
    return _resource_value(dict(row))


async def update_resource(resource_id: int, patch: dict, *, owner_user_id: str | None = None, expected_revision: int | None = None) -> dict | None:
    from db import PostgresClient
    if not PostgresClient.pool:
        return None
    current = await get_resource(resource_id, owner_user_id=owner_user_id, include_secrets=True)
    if current is None:
        return None
    resource_type = current["resource_type"]
    secret_fields = _DETAIL_SECRET_FIELDS.get(resource_type, frozenset())
    existing_secret = {k: v for k, v in current.items() if k in secret_fields}
    base_data = {k: v for k, v in current.items() if k not in _RESOURCE_META_KEYS and k not in secret_fields}
    data, secret_data = _split_detail_value(resource_type, {**base_data, **patch}, existing_secret)
    revision = int(expected_revision if expected_revision is not None else current["revision"])
    clauses = ["id=$1", "revision=$4"]
    params: list[Any] = [int(resource_id), json.dumps(data, ensure_ascii=False), json.dumps(secret_data, ensure_ascii=False), revision]
    if owner_user_id is not None:
        clauses.append("owner_user_id=$5"); params.append(str(owner_user_id))
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""UPDATE builtin_tool_resources SET data=$2::jsonb, secret_data=$3::jsonb,
                revision=revision+1, updated_at=now() WHERE {' AND '.join(clauses)} RETURNING *""", *params,
        )
    if row is None and expected_revision is not None:
        raise ValueError("builtin tool resource revision conflict")
    return _resource_value(dict(row)) if row else current


async def delete_resource(resource_id: int, *, owner_user_id: str | None = None) -> bool:
    from db import PostgresClient
    if not PostgresClient.pool:
        return False
    clauses = ["id=$1"]
    params: list[Any] = [int(resource_id)]
    if owner_user_id is not None:
        clauses.append("owner_user_id=$2"); params.append(str(owner_user_id))
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute(f"DELETE FROM builtin_tool_resources WHERE {' AND '.join(clauses)}", *params)
    return result.endswith(" 1")


# Backward-compatible internal aliases are deliberately absent: callers must use
# resource semantics so the removed instance table cannot silently return.

# ── token 行 → dict ──────────────────────────────────────────────────────────

def token_row_to_dict(row) -> dict:
    """token 行 → dict。永不含明文（明文只在签发时一次性返回），只暴露 hint 与状态。"""
    d = dict(row)
    d.pop("token_hash", None)  # 哈希不外泄
    for key in _RESOURCE_TIME_KEYS + ("expires_at",):
        if d.get(key) is not None:
            d[key] = d[key].isoformat()
    return d


# ── token 签发 / 轮换 / 禁用 / 删除 ──────────────────────────────────────────

async def list_tokens(
    *, resource_id: int | None = None, service_id: int | None = None
) -> list[dict]:
    """列出某内置工具资源或某外部 MCP 服务的 token。二者传其一。"""
    from db import PostgresClient
    if not PostgresClient.pool:
        return []
    if resource_id is not None:
        clause, param = "resource_id=$1", int(resource_id)
    elif service_id is not None:
        clause, param = "service_id=$1", int(service_id)
    else:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM builtin_tool_tokens WHERE {clause} ORDER BY id", param
        )
    return [token_row_to_dict(r) for r in rows]


async def list_all_tokens() -> list[dict]:
    """列出全部 token（管理端用）：用户建的 external + 服务自动建的 agent/node/user。

    管理端视角，不按 owner 隔离；按 id 倒序方便看最新签发。附带 resource/service 的
    名称快照，前端直接渲染无需二次查询。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        return []
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT t.*, r.owner_user_id AS owner_user_id,
                   COALESCE(r.data->>'name', r.data->>'display_name', r.data->>'device_id') AS resource_name,
                   r.resource_type AS resource_type,
                   s.name AS service_name, s.display_name AS service_display_name
            FROM builtin_tool_tokens t
            LEFT JOIN builtin_tool_resources r ON r.id = t.resource_id
            LEFT JOIN mcp_services s ON s.id = t.service_id
            ORDER BY t.id DESC
            """
        )
    return [token_row_to_dict(r) for r in rows]


async def issue_token(
    target_type: str,
    target_id: str | None,
    *,
    resource_id: int | None = None,
    service_id: int | None = None,
    display_token: bool = False,
    expires_at: datetime | None = None,
) -> tuple[dict, str]:
    """签发一个 token，返回 (token行dict, 明文token)。明文只此一次可见。

    目标至多一个：
      - resource_id（内置工具一级资源：客户端/邮箱账户/设备）或 service_id（外部
        MCP 服务）绑定具体授权目标，用于「分享」类 token（target_type=user/external）。
      - 二者都空 = 身份/连接 token（target_type=agent/node）：只标识调用方，
        具体权限由调用方（agent/node）自身推导，不绑死单一目标。
    target_type ∈ {agent, node, user, external}。自用（agent/node/user）建议 display_token=False；
    分享外部（external）用 display_token=True，owner 可复制明文出去。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    if resource_id is not None and service_id is not None:
        raise ValueError("issue_token 的 resource_id 与 service_id 至多传一个")
    token = generate_runtime_token()
    token_hash = hash_runtime_token(token)
    token_hint = runtime_token_hint(token)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO builtin_tool_tokens(
                resource_id, service_id, token_hash, token_hint, target_type,
                target_id, display_token, expires_at
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            RETURNING *
            """,
            (int(resource_id) if resource_id is not None else None),
            (int(service_id) if service_id is not None else None),
            token_hash, token_hint, target_type,
            (str(target_id) if target_id is not None else None),
            display_token, expires_at,
        )
    result = token_row_to_dict(row)
    return result, token


async def rotate_token(token_id: int) -> tuple[dict, str]:
    """轮换 token：换新明文，旧明文立即失效。返回 (token行dict, 新明文)。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")
    token = generate_runtime_token()
    token_hash = hash_runtime_token(token)
    token_hint = runtime_token_hint(token)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE builtin_tool_tokens
            SET token_hash=$2, token_hint=$3, status='active', updated_at=now()
            WHERE id=$1
            RETURNING *
            """,
            int(token_id), token_hash, token_hint,
        )
    if row is None:
        raise ValueError("builtin tool token not found")
    return token_row_to_dict(row), token


async def set_token_status(token_id: int, status: str) -> dict | None:
    """启用 / 禁用 token。status ∈ {active, disabled}。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE builtin_tool_tokens SET status=$2, updated_at=now() WHERE id=$1 RETURNING *",
            int(token_id), status,
        )
    return token_row_to_dict(row) if row else None


async def delete_token(token_id: int) -> bool:
    from db import PostgresClient

    if not PostgresClient.pool:
        return False
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute("DELETE FROM builtin_tool_tokens WHERE id=$1", int(token_id))
    return result.endswith(" 1")


def _token_is_valid(row) -> bool:
    """token 行是否可用：status=active 且未过期。纯函数，零 IO。"""
    if row is None:
        return False
    if (row["status"] or "").lower() != "active":
        return False
    expires_at = row["expires_at"]
    if expires_at is not None:
        now = datetime.now(timezone.utc)
        # asyncpg 返回带 tz 的 datetime；防御性补 tz。
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if now > expires_at:
            return False
    return True


async def resolve_token(token: str) -> dict | None:
    """统一鉴权入口：明文 token → principal、内置工具资源、外部服务或身份。

    按 sha256(token) 精确定位 token 行，校验 status=active 且未过期，再按目标类型取：
    - 身份 token（resource_id / service_id 都空）：调用方身份凭证（agent/node 启动时下发），
      权限由 target（agent_id/node_id）自身推导，不绑死某个具体目标。返回 kind='identity'。
    - 内置工具 token（resource_id 非空）：取资源，资源须 enabled。返回 kind='resource'。
    - 外部 MCP token（service_id 非空）：取服务，服务须 enabled。返回 kind='service'。

    返回 {kind: 'identity'|'resource'|'service', target, token}；identity 时 target 为 None。
    token 无效 / 目标被禁用 / 不存在都返回 None。所有 MCP 访问（内置 + 外部）统一走这里判权。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        return None
    token_hash = hash_runtime_token(token)
    async with PostgresClient.pool.acquire() as conn:
        principal_row = await conn.fetchrow(
            "SELECT id, name, owner_user_id, token_hint, token_status, expires_at, enabled "
            "FROM mcp_users WHERE token_hash=$1",
            token_hash,
        )
        if principal_row is not None:
            status = (principal_row["token_status"] or "").lower()
            expires_at = principal_row["expires_at"]
            if not principal_row["enabled"] or status != "active":
                return None
            if expires_at is not None:
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) > expires_at:
                    return None
            return {
                "kind": "principal",
                "target": {
                    "id": int(principal_row["id"]),
                    "name": principal_row["name"],
                    "owner_user_id": principal_row["owner_user_id"],
                    "token_hint": principal_row["token_hint"] or "",
                },
                "token": None,
            }
        token_row = await conn.fetchrow(
            "SELECT * FROM builtin_tool_tokens WHERE token_hash=$1", token_hash
        )
        if not _token_is_valid(token_row):
            return None
        if token_row["resource_id"] is not None:
            res_row = await conn.fetchrow(
                "SELECT * FROM builtin_tool_resources WHERE id=$1", token_row["resource_id"]
            )
            if res_row is None or not COALESCE_ENABLED(res_row):
                return None
            return {
                "kind": "resource",
                "target": _resource_value(dict(res_row)),
                "token": token_row_to_dict(token_row),
            }
        if token_row["service_id"] is not None:
            svc_row = await conn.fetchrow(
                "SELECT * FROM mcp_services WHERE id=$1", token_row["service_id"]
            )
            if svc_row is None or not svc_row["enabled"]:
                return None
            return {
                "kind": "service",
                "target": dict(svc_row),
                "token": token_row_to_dict(token_row),
            }
    # 身份 token：不绑具体目标，权限由 target_type/target_id（agent/node）推导。
    return {
        "kind": "identity",
        "target": None,
        "token": token_row_to_dict(token_row),
    }


def COALESCE_ENABLED(resource_row) -> bool:
    """资源行是否启用（data.enabled 缺省视为启用）。"""
    data = _json_value(resource_row.get("data"), {})
    return bool(data.get("enabled", True))


# ── 外部 MCP 访问 token → 连接层隔离键 ────────────────────────────────────────
#
# 外部 MCP 访问 token 存 builtin_tool_tokens（绑 resource_id），与 CDP 连接 token
# （存 cdp_client 明细行的 token_hash）是两回事：driver.authenticate_client 只认连接
# token，认不出外部 MCP token。这两个 resolver 把外部 MCP token 解析成连接层需要的
# 隔离键——CDP 是 target_id 指定的 cdp_client 明细 id，邮箱是实例 id——让连接层按其
# 绑定实例/客户端严格隔离，杜绝一个外部 token 越权操作同实例其他客户端或其他实例。


async def resolve_external_cdp_client(token: str) -> str | None:
    """外部 MCP token（绑 CDP 客户端资源）→ 其 resource id（会话池隔离键）。

    external 类 token 必须在签发时绑定一个具体 cdp_client（target_id）；这里复核该
    资源存在、已启用、且有连接 token（否则扩展没连过、无法驱动）。校验不过返回 None。
    仅认 external+cdp_client 的 resource token，其它 token 一律 None。
    """
    resolved = await resolve_token(token)
    if not resolved or resolved["kind"] != "resource":
        return None
    resource = resolved["target"]
    if resource.get("resource_type") != "cdp_client":
        return None
    token_row = resolved["token"]
    if token_row.get("target_type") != "external":
        return None
    if not resource.get("enabled", True) or not resource.get("token_hash"):
        return None
    return str(resource["id"])


async def resolve_external_device_id(token: str) -> str | None:
    """外部 MCP token（绑 device 资源）→ 其协议 device_id。

    与 ``resolve_external_cdp_client`` 同构：external token 签发时必须绑一台具体
    设备（target_id = device resource id），这里复核其已启用、有 token。
    """
    resolved = await resolve_token(token)
    if not resolved or resolved["kind"] != "resource":
        return None
    resource = resolved["target"]
    if resource.get("resource_type") != "device":
        return None
    token_row = resolved["token"]
    if token_row.get("target_type") != "external":
        return None
    if not resource.get("enabled", True) or not resource.get("token_hash"):
        return None
    return str(resource.get("device_id") or "") or None


async def resolve_mail_resource_id(token: str) -> int | None:
    """外部 MCP token（绑邮箱账户资源）→ mail_account resource id。否则 None。

    邮件插件据此把可查询账户收窄到本资源，防一个外部 token 读到其他用户的邮箱。
    """
    resolved = await resolve_token(token)
    if not resolved or resolved["kind"] != "resource":
        return None
    resource = resolved["target"]
    if resource.get("resource_type") != "mail_account":
        return None
    return int(resource["id"])


# ── 运行时数据源：把资源表喂成 driver / 插件期望的形状 ───────────────────────
#
# CDP driver 与 mail 插件各有既定的输入形状（driver 认 clients_by_hash 的 client 行；
# mail 认 upstream_accounts）。这里做「资源表 → 旧形状」的投影，把连接层的数据源
# 切到资源表而不动它们一行代码。


async def cdp_driver_clients() -> list[dict]:
    """产出 CDP driver 期望的 client 行列表：每个 cdp_client 资源一行。

    连接 token 存资源行（token_hash + token_hint 落 data；哈希非明文凭据，明文
    永不落库）。``client_id = 资源行 id``。driver 的 TokenManager 按 token_hash 建
    索引（clients_by_hash），authenticate_client 据此把连接 token → client 行 →
    client_id=资源id，会话池按 client_id 隔离（一个客户端=一个浏览器）。

    只产出 enabled 且有 token_hash 的 cdp_client 资源（撤销会清 token_hash 并停用，
    故被过滤，driver 侧对应 token 立即失效）。
    """
    resources = await list_resources("cdp_client", enabled=True)
    clients: list[dict] = []
    for d in resources:
        if not d.get("enabled", True) or not d.get("token_hash"):
            continue
        client_id = str(d["id"])
        clients.append({
            "id": client_id,
            "instance_key": client_id,
            "name": d.get("name") or "",
            "owner_user_id": d.get("owner_user_id"),
            "enabled": True,
            "token_hash": str(d["token_hash"]),
            "token_hint": d.get("token_hint") or "",
        })
    return clients


# ── CDP 客户端连接 token 生命周期（存 cdp_client 资源行） ──────────────────────
#
# 连接 token 给 Chrome 扩展 WS 首帧 auth protocol 2 用，与统一 MCP 访问 token
# （builtin_tool_tokens 表）是两回事。口径照抄旧 mcp/configuration.py 的 cdp_client：
# 明文只在 create/rotate 响应注入一次，DB 只存 token_hash + token_hint。token_hash 放
# data（哈希不可逆，非明文凭据；放 data 才能在 revoke 时传空清除——secret 字段传空会
# 保留旧值）。对外展示 strip 掉 token_hash，只给 client_id + token_hint。


def cdp_client_present(resource: dict | None) -> dict | None:
    """CDP 客户端资源对外形状：strip token_hash，补 client_id=资源 id。"""
    if resource is None:
        return None
    result = dict(resource)
    result.pop("token_hash", None)  # 哈希不外泄
    result["client_id"] = str(result.get("id") or "")
    return result


async def create_cdp_client(owner_user_id: str, name: str, extra: dict | None = None) -> tuple[dict, str]:
    """建一个浏览器客户端资源并签发连接 token。返回 (资源dict, 明文token)。

    明文只此一次返回；DB 只存 token_hash + token_hint。client_id = 资源行 id。
    """
    token = generate_runtime_token()
    value = dict(extra or {})
    value["name"] = name
    value["enabled"] = True
    value["token_hint"] = runtime_token_hint(token)
    value["token_hash"] = hash_runtime_token(token)
    resource = await create_resource(owner_user_id, "cdp_client", value)
    return cdp_client_present(resource), token


async def rotate_cdp_token(resource_id: int) -> tuple[dict, str]:
    """轮换某 CDP 客户端的连接 token：旧明文立即失效，返回新明文。"""
    token = generate_runtime_token()
    resource = await update_resource(int(resource_id), {
        "token_hash": hash_runtime_token(token),
        "token_hint": runtime_token_hint(token),
        "enabled": True,
    })
    if resource is None:
        raise ValueError("cdp client resource not found")
    return cdp_client_present(resource), token


async def revoke_cdp_token(resource_id: int) -> dict | None:
    """撤销某 CDP 客户端：清 token_hash 并停用；下次 apply_clients 后扩展连接被断开。"""
    resource = await update_resource(int(resource_id), {
        "token_hash": "",
        "token_hint": "",
        "enabled": False,
    })
    return cdp_client_present(resource) if resource else None


# ── device-control 设备凭据（存 device 资源行，给 Android App 连 WS 用） ──────────
#
# 与 CDP 客户端同形态：每条 ``device`` 资源 = 一台已配对的手机。长期 token 只存
# sha256 + hint，明文仅在配对成功那一次响应里返回（App 落盘到 DeviceCredential）。
# device_id 是协议里的设备标识（spec §3.3 要求不透明），不用资源行 id——它要发给设备
# 并在每条 call 帧里回来，用自增 id 会把内部主键暴露出去。


def device_present(resource: dict | None) -> dict | None:
    """设备资源对外形状：strip token_hash，只留 device_id + token_hint。"""
    if resource is None:
        return None
    result = dict(resource)
    result.pop("token_hash", None)  # 哈希不外泄
    return result


async def device_authorized_hashes() -> set[str]:
    """产出 device-control driver 的已授权 token_hash 快照。

    只收 enabled 且有 token_hash 的 device 资源。解除配对会清 token_hash 并停用，
    因此那台设备的 hash 会从快照里消失，driver 的 ``apply_config`` 当场断掉它的
    活连接（不等心跳超时）。
    """
    resources = await list_resources("device", enabled=True)
    hashes: set[str] = set()
    for d in resources:
        if not d.get("enabled", True) or not d.get("token_hash"):
            continue
        hashes.add(str(d["token_hash"]))
    return hashes


async def authenticate_device(device_id: str, token: str) -> dict | None:
    """握手认证：(device_id, 明文 token) → 设备资源行。不匹配返回 None。

    先按 device_id 定位再比对 hash，且比对走常量时间：只比 hash 不看 device_id
    会让一台设备用别人的 token 冒名注册成任意 device_id。
    """
    from mcp_builtin.device_control.store import hash_secret, secrets_equal

    device_id = str(device_id or "").strip()
    if not device_id or not token:
        return None
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM builtin_tool_resources
            WHERE resource_type='device' AND data->>'device_id' = $1
            """,
            device_id,
        )
    if row is None:
        return None
    resource = _resource_value(dict(row))
    if not resource.get("enabled", True):
        return None
    stored = str(resource.get("token_hash") or "")
    if not stored or not secrets_equal(stored, hash_secret(token)):
        return None
    return resource


async def create_device(
    owner_user_id: str, label: str = "", device_info: dict | None = None
) -> tuple[dict, str, str]:
    """配对成功后登记一台设备资源并签发长期 token。

    返回 ``(资源dict, device_id, 明文token)``。明文只此一次返回；DB 只存
    token_hash + token_hint。
    """
    from mcp_builtin.device_control.store import (
        generate_token, hash_secret, new_device_id,
    )

    token = generate_token()
    device_id = new_device_id()
    value: dict[str, Any] = {
        "device_id": device_id,
        "name": label or (device_info or {}).get("model") or "设备",
        "enabled": True,
        "token_hint": runtime_token_hint(token),
        "token_hash": hash_secret(token),
        "device_info": dict(device_info or {}),
    }
    resource = await create_resource(owner_user_id, "device", value)
    return device_present(resource), device_id, token


async def revoke_device(resource_id: int) -> dict | None:
    """解除配对：清 token_hash 并停用。apply_config 后活连接被 close 4003。"""
    resource = await update_resource(int(resource_id), {
        "token_hash": "",
        "token_hint": "",
        "enabled": False,
    })
    return device_present(resource) if resource else None


async def update_device_runtime(
    resource_id: int,
    *,
    device_info: dict | None = None,
    last_seen_at: str | None = None,
    last_error: str | None = None,
    last_error_at: str | None = None,
) -> dict | None:
    """落库设备运行态：register/断连/device_status 事件时刷新。

    在线快照（online/device_info/last_seen）只活在每个 Uvicorn worker 的 driver
    内存里——一旦设备离线或进程重启，网页就只剩配对时那条空 device_info。把
    app 上报的 device_info（版本/系统/机型/无障碍开关/上次错误）与 last_seen_at
    落到 data JSONB，离线后网页仍能展示「最后一次上报 / 最后在线 / 上次错误」。

    只 merge 传入的字段，其余（token_hash/enabled/device_id/...）原样保留。
    用 update_resource 的读改写以复用 revision 自增，不破坏既有并发口径。
    """
    patch: dict[str, Any] = {}
    if device_info is not None:
        patch["device_info"] = device_info
    if last_seen_at is not None:
        patch["last_seen_at"] = last_seen_at
    if last_error is not None:
        patch["last_error"] = last_error
    if last_error_at is not None:
        patch["last_error_at"] = last_error_at
    if not patch:
        return None
    resource = await update_resource(int(resource_id), patch)
    return device_present(resource) if resource else None


async def mail_upstream_accounts() -> list[dict]:
    """产出 mail 插件期望的 upstream_accounts：账户 + 其地址列表（含 secret 真值）。

    每个 mail_account 资源一行；mail_address 资源按 owner 归属（同 owner 的地址
    集合挂到账户，沿用旧父子形态）。secret（password/secret_key）需要真值供
    MailClient 调用，故 include_secrets=True。
    """
    accounts_rows = await list_resources("mail_account", enabled=True, include_secrets=True)
    address_rows = await list_resources("mail_address", include_secrets=False)
    addresses_by_owner: dict[str, list[dict]] = {}
    for addr in address_rows:
        addresses_by_owner.setdefault(str(addr.get("owner_user_id") or ""), []).append(addr)
    accounts: list[dict] = []
    for account in accounts_rows:
        owner = str(account.get("owner_user_id") or "")
        acct = {k: v for k, v in account.items() if k not in _RESOURCE_META_KEYS}
        acct["id"] = int(account["id"])
        acct["instance_key"] = str(account["id"])
        acct["enabled"] = True
        acct["addresses"] = [
            {k: v for k, v in addr.items() if k not in _RESOURCE_META_KEYS} | {"id": addr["id"]}
            for addr in addresses_by_owner.get(owner, [])
        ]
        accounts.append(acct)
    return accounts


async def mail_query_for_resource(
    resource_id: int, *, address: str = "", keyword: str = "", limit: int = 20, offset: int = 0,
) -> dict:
    """在某邮箱服务（mail_account 资源）上实时查询邮件（用户侧「获取/搜索邮件」）。

    照抄 ``mcp_runtime.builtin_plugins.mail_plugin._query_messages`` 的口径，但数据源
    换成该资源自己（owner 隔离由调用方保证）：取 mail_account（含 secret 真值）+
    同 owner 的 mail_address 列表 → 解析地址（别名替换成 source_address）→ 用
    ``MailClient.list_mail`` 拉上游 → 回填 received_address/requested_address。

    - address 为空：查全部（不带地址过滤）。
    - address 命中某转发别名：用其 source_address 作为上游查询地址。
    - address 未命中任何配置地址：直接按原地址查（宽松，不报错——用户可能查主邮箱）。
    """
    from mcp_builtin.mail.client import MailClient, MailClientError
    import aiohttp
    from providers.base import make_insecure_connector

    account = await get_resource(int(resource_id), include_secrets=True)
    if account is None or account.get("resource_type") != "mail_account":
        raise ValueError("邮箱服务不存在")
    addresses = await list_resources("mail_address", owner_user_id=str(account.get("owner_user_id") or ""))

    normalized = (address or "").strip().lower()
    requested_address = normalized
    source_address = normalized
    if normalized:
        mapping = next(
            (a for a in addresses if (a.get("address") or "").strip().lower() == normalized),
            None,
        )
        if mapping:
            source_address = (mapping.get("source_address") or normalized).strip().lower()

    limit = max(1, min(int(limit or 20), 100))
    offset = max(0, int(offset or 0))
    keyword = (str(keyword or "").strip()[:500]) or None

    client = MailClient(
        account.get("username") or "", account.get("password") or "",
        account.get("base_url") or "", account.get("secret_key") or "",
    )
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=120), connector=make_insecure_connector(),
    )
    try:
        messages = await client.list_mail(
            session, limit=limit, offset=offset,
            address=source_address or None, keyword=keyword,
        )
    except MailClientError as exc:
        raise ValueError(str(exc)) from exc
    finally:
        await session.close()

    for message in messages:
        if not message.get("received_address"):
            message["received_address"] = source_address or normalized
        message["requested_address"] = normalized
    return {
        "requested_address": requested_address,
        "received_address": source_address or normalized,
        "messages": messages, "count": len(messages), "limit": limit, "offset": offset,
    }
