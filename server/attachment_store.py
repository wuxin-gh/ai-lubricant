"""Agent 附件资源存储层。

把 Agent 工作区里不可外访问的文件，登记成可跨 Web/App 访问、带鉴权、
带生命周期的附件资源。消息协议里只存稳定的 ``attachment_id``，URL 由各端
用登录态解析——避免签名 URL 过期 / 跨端不可用 / 历史消息打不开。

模型见 docs/attachment-feature-design.md（如有）与本文件 docstring：

- ``attachments`` 表一行 = 一个附件资源。``owner_user_id`` 是发起 Agent 运行的
  C 端用户（鉴权主体）；``created_by_agent_id`` 仅审计追溯。
- 存储两种：``workspace_ref``（原地登记工作区路径 + sha256）/ ``object_store``
  （复制进附件存储目录，临时路径或 persist=true 时用）。
- 生命周期：默认 30 天 TTL，``status`` 三态 ``active|expired|purged``；过期后
  ``renew`` 续期 +30 天；宽限期满物理删除。

安全约束（本层保证）：
- ``register_attachment`` 入口的路径校验不在此层做（由 attachment_plugin 按
  agent 的 allowed_roots 把关）；本层只管存储与索引。
- 下载鉴权收口在 ``get_for_owner``：按 ``owner_user_id`` 精确匹配，未认领
  （owner IS NULL）的附件不允许通过 HTTP 下载——防止越权。
- object_store 物理文件路径不外泄；只按 attachment_id 取。

Storage only —— 不触碰模型请求主链路。依赖 db.PostgresClient.pool。
"""
from __future__ import annotations

import hashlib
import os
import secrets
import shutil
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger


# 默认 TTL：30 天。续期也是 +30 天。
DEFAULT_TTL_DAYS = 30
# 过期后宽限期：90 天。宽限期内可续期；满则物理删除。
EXPIRY_GRACE_DAYS = 90

# 附件物理存储根目录（object_store 类型）。相对工作目录，可在环境变量覆盖。
ATTACHMENTS_ROOT = os.environ.get("ATTACHMENTS_ROOT", "agent/attachments")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _default_expires_at(now: datetime | None = None) -> datetime:
    base = now or _now()
    return base + timedelta(days=DEFAULT_TTL_DAYS)


def _ensure_root() -> str:
    """确保附件存储根目录存在。返回绝对路径。"""
    root = os.path.abspath(ATTACHMENTS_ROOT)
    os.makedirs(root, exist_ok=True)
    return root


def _guess_mime(name: str, fallback: str = "application/octet-stream") -> str:
    """按扩展名推断 mime。简单实现，避免引入 python-magic 依赖。"""
    if not name:
        return fallback
    lower = name.lower()
    _, dot, ext = lower.rpartition(".")
    if not dot:
        return fallback
    table = {
        "pdf": "application/pdf",
        "zip": "application/zip",
        "gz": "application/gzip",
        "tar": "application/x-tar",
        "json": "application/json",
        "csv": "text/csv",
        "txt": "text/plain",
        "md": "text/markdown",
        "html": "text/html",
        "xml": "application/xml",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
        "webp": "image/webp",
        "svg": "image/svg+xml",
        "mp3": "audio/mpeg",
        "wav": "audio/wav",
        "mp4": "video/mp4",
        "webm": "video/webm",
        "mov": "video/quicktime",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xls": "application/vnd.ms-excel",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "doc": "application/msword",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }
    return table.get(ext, fallback)


def _sha256_file(path: str) -> tuple[str, int]:
    """流式算 sha256 + 字节数，避免大文件整读进内存。"""
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)  # 1MB
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _row_to_dict(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    for key in ("created_at", "expires_at", "last_renewed_at", "last_accessed_at", "purged_at"):
        v = d.get(key)
        if isinstance(v, datetime) and v.tzinfo is None:
            d[key] = v.replace(tzinfo=timezone.utc)
    return d


# ── 注册 ────────────────────────────────────────────────────────────

async def register_attachment(
    *,
    source_path: str,
    name: str | None = None,
    mime_type: str | None = None,
    owner_user_id: str | None = None,
    created_by_agent_id: int | None = None,
    context_ref: str | None = None,
    persist: bool = False,
    ttl_days: int | None = None,
) -> dict:
    """登记一个附件。

    - persist=False：原地登记（workspace_ref）。要求调用方已校验路径安全
      （在 agent allowed_roots 内）。本函数不复制文件，工作区文件须存活至 TTL。
    - persist=True 或路径判定为临时区：复制进 ATTACHMENTS_ROOT（object_store），
      工作区原文件可随后清理。

    owner_user_id 可空——未认领的附件 HTTP 下载会 403。通常 agent 调用时拿不到
    用户身份，留空；由 API 层在持久化 media part 时调 claim_for_owner 认领。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        raise RuntimeError("Database not available")

    src = os.path.abspath(source_path)
    if not os.path.isfile(src):
        raise FileNotFoundError(f"attachment source not found: {source_path}")
    if not os.access(src, os.R_OK):
        raise PermissionError(f"attachment source not readable: {source_path}")

    display_name = name or os.path.basename(src)
    mime = (mime_type or _guess_mime(display_name)).strip() or "application/octet-stream"
    sha256, size_bytes = _sha256_file(src)

    storage_kind = "workspace_ref"
    object_key: str | None = None
    if persist:
        storage_kind, object_key = _materialize_object(src, sha256, display_name)

    now = _now()
    days = ttl_days if ttl_days is not None else DEFAULT_TTL_DAYS
    if days < 1 or days > 365:
        raise ValueError("ttl_days must be between 1 and 365")
    expires_at = now + timedelta(days=days)

    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO attachments(
                owner_user_id, created_by_agent_id, context_ref,
                storage_kind, workspace_path, object_key, sha256,
                size_bytes, name, mime_type,
                created_at, expires_at, renewed_count, last_renewed_at,
                status, last_accessed_at, access_count, purged_at
            ) VALUES(
                $1, $2, $3,
                $4, $5, $6, $7,
                $8, $9, $10,
                $11, $12, 0, $11,
                'active', NULL, 0, NULL
            )
            RETURNING *
            """,
            (str(owner_user_id) if owner_user_id else None),
            (int(created_by_agent_id) if created_by_agent_id else None),
            (str(context_ref) if context_ref else None),
            storage_kind,
            (src if storage_kind == "workspace_ref" else None),
            object_key,
            sha256,
            size_bytes,
            display_name,
            mime,
            now,
            expires_at,
        )

    logger.info(
        "[attachment] registered id={} sha256={:.12} kind={} size={} owner={}",
        row["id"], sha256, storage_kind, size_bytes, owner_user_id or "<unclaimed>",
    )
    return _row_to_dict(row)


def _materialize_object(src: str, sha256: str, name: str) -> tuple[str, str]:
    """把源文件复制进附件存储区，返回 (storage_kind, object_key)。

    object_key 用 sha256 前缀做去重目录，避免单目录爆炸；保留扩展名方便预览。
    """
    root = _ensure_root()
    # agent/attachments/<sha前2>/<sha前4>/<sha>_<随机>.<ext>
    ext = os.path.splitext(name)[1].lower()
    sub = os.path.join(root, sha256[:2], sha256[:4])
    os.makedirs(sub, exist_ok=True)
    object_key = os.path.join(sub, f"{sha256}_{secrets.token_hex(4)}{ext}")
    shutil.copyfile(src, object_key)
    return "object_store", object_key


# ── 认领（API 层在持久化 media part 时调用）─────────────────────────

async def claim_for_owner(attachment_id: int, owner_user_id: str, context_ref: str | None = None) -> dict | None:
    """把未认领的附件标记归属。已认领的不覆盖（防越权抢占）。

    返回认领后的附件 dict；附件不存在/已被他人认领时返回 None。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE attachments
            SET owner_user_id = $2,
                context_ref = COALESCE(context_ref, $3)
            WHERE id = $1 AND owner_user_id IS NULL AND status != 'purged'
            RETURNING *
            """,
            int(attachment_id),
            str(owner_user_id),
            (str(context_ref) if context_ref else None),
        )
    return _row_to_dict(row)


# ── 读取 ────────────────────────────────────────────────────────────

async def get_active_by_id(attachment_id: int, *, include_purged: bool = False) -> dict | None:
    """按 id 取附件，不做 owner 过滤——仅供签名 URL 验证通过后使用。

    签名 URL 自身即凭证（持有者=已获服务端签发），验签通过后不需要再校 owner。
    仍推进 expired 状态、默认过滤 purged，purged/expired 由调用方按 410 处理。
    未认领（owner IS NULL）的附件也允许经签名 URL 访问：消息已 serve 给会话用户
    即视为已授权，认领状态不影响字节可读性。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    purged_filter = "" if include_purged else "AND status != 'purged'"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE attachments
            SET last_accessed_at = CASE WHEN status != 'purged' THEN now() ELSE last_accessed_at END,
                access_count = CASE WHEN status != 'purged' THEN access_count + 1 ELSE access_count END
            WHERE id = $1 {purged_filter}
            RETURNING *
            """,
            int(attachment_id),
        )
    if row is None:
        return None
    d = _row_to_dict(row)
    if d and d.get("status") == "active" and d.get("expires_at") and _now() > d["expires_at"]:
        d["status"] = "expired"
    return d


async def get_for_owner(
    attachment_id: int,
    owner_user_id: str,
    *,
    include_purged: bool = False,
) -> dict | None:
    """按 owner 取附件，供 HTTP/MCP 鉴权入口使用。

    默认过滤 purged；需要向客户端明确返回 410 时传 include_purged=True。无论哪种
    模式都精确匹配 owner_user_id，未认领/非本人返回 None。只有非 purged 行才累计
    访问次数，避免读取已清理元数据也被记成一次内容访问。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    purged_filter = "" if include_purged else "AND status != 'purged'"
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE attachments
            SET last_accessed_at = CASE WHEN status != 'purged' THEN now() ELSE last_accessed_at END,
                access_count = CASE WHEN status != 'purged' THEN access_count + 1 ELSE access_count END
            WHERE id = $1 AND owner_user_id = $2 {purged_filter}
            RETURNING *
            """,
            int(attachment_id),
            str(owner_user_id),
        )
    if row is None:
        return None
    d = _row_to_dict(row)
    # 推进 expired 状态：过期但未清理的，HTTP 层据此返回 410。
    if d and d.get("status") == "active" and d.get("expires_at") and _now() > d["expires_at"]:
        d["status"] = "expired"
    return d


async def get_meta(attachment_id: int, *, agent_id: int | None = None, owner_user_id: str | None = None) -> dict | None:
    """工具侧只读元信息。可选按创建 agent / 归属用户收窄，防止跨租户枚举。

    不传过滤参数时退回「按 id 任意取」——仅供内部诊断用。工具入口始终带身份：
    已认领附件必须 owner_user_id 匹配；owner 仍为空的即时产物才允许创建 agent 查看。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    clauses = ["id = $1"]
    params: list[Any] = [int(attachment_id)]
    identity_clauses: list[str] = []
    if owner_user_id:
        params.append(str(owner_user_id))
        identity_clauses.append(f"owner_user_id = ${len(params)}")
    if agent_id is not None:
        params.append(int(agent_id))
        # Agent identity只能兜底查看尚未认领的即时产物；认领后必须靠 owner 匹配。
        # 否则平台公共 Agent 会按 created_by_agent_id 看到其他用户会话的附件。
        identity_clauses.append(f"(owner_user_id IS NULL AND created_by_agent_id = ${len(params)})")
    if identity_clauses:
        clauses.append(f"({' OR '.join(identity_clauses)})")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM attachments WHERE {' AND '.join(clauses)}",
            *params,
        )
    d = _row_to_dict(row)
    if d and d.get("status") == "active" and d.get("expires_at") and _now() > d["expires_at"]:
        d["status"] = "expired"
    return d


async def list_for_owner(
    owner_user_id: str,
    *,
    include_expired: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """列本人附件（工具的 list_attachments 与 HTTP 列表共用）。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    status_filter = "" if include_expired else "AND status = 'active'"
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT * FROM attachments
            WHERE owner_user_id = $1 AND status != 'purged' {status_filter}
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            str(owner_user_id),
            max(1, min(int(limit), 500)),
            max(0, int(offset)),
        )
    out = []
    for r in rows:
        d = _row_to_dict(r)
        if d and d.get("status") == "active" and d.get("expires_at") and _now() > d["expires_at"]:
            d["status"] = "expired"
        out.append(d)
    return out


async def list_for_agent(
    created_by_agent_id: int,
    *,
    include_expired: bool = True,
    unclaimed_only: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """按 agent 列其创建的附件（工具侧在未认领前用）。

    平台公共 Agent 没有 owner_user_id，必须传 unclaimed_only=True，避免它按共同的
    created_by_agent_id 看到其他用户已经认领的附件元数据。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        return []
    status_filter = "" if include_expired else "AND status = 'active'"
    owner_filter = "AND owner_user_id IS NULL" if unclaimed_only else ""
    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT * FROM attachments
            WHERE created_by_agent_id = $1 AND status != 'purged' {owner_filter} {status_filter}
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            int(created_by_agent_id),
            max(1, min(int(limit), 500)),
            max(0, int(offset)),
        )
    out = []
    for r in rows:
        d = _row_to_dict(r)
        if d and d.get("status") == "active" and d.get("expires_at") and _now() > d["expires_at"]:
            d["status"] = "expired"
        out.append(d)
    return out


# ── 续期 ────────────────────────────────────────────────────────────

async def renew(attachment_id: int, owner_user_id: str, *, ttl_days: int | None = None) -> dict | None:
    """续期 +ttl_days（默认 30）。仅 owner 可续。purged / 非本人返回 None。"""
    from db import PostgresClient

    if not PostgresClient.pool:
        return None
    days = ttl_days or DEFAULT_TTL_DAYS
    now = _now()
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE attachments
            SET expires_at = $3,
                status = 'active',
                renewed_count = renewed_count + 1,
                last_renewed_at = $3,
                purged_at = NULL
            WHERE id = $1 AND owner_user_id = $2 AND status != 'purged'
            RETURNING *
            """,
            int(attachment_id),
            str(owner_user_id),
            now + timedelta(days=days),
        )
    if row is not None:
        logger.info("[attachment] renewed id={} by={} count={}", attachment_id, owner_user_id, row["renewed_count"])
    return _row_to_dict(row)


# ── 清理 ────────────────────────────────────────────────────────────

async def sweep_expired() -> dict:
    """定时清理：

    1. active 但 expires_at 已过 → 标记 expired（不删文件，留续期窗口）。
    2. expired 且超过宽限期（expires_at + EXPIRY_GRACE_DAYS）→ 标记 purged +
       删除 object_store 物理文件（workspace_ref 不删——那是 agent 的文件）。

    返回 {marked_expired, purged} 计数。幂等，可定时跑。
    """
    from db import PostgresClient

    stats = {"marked_expired": 0, "purged": 0}
    if not PostgresClient.pool:
        return stats
    now = _now()
    grace_cutoff = now - timedelta(days=EXPIRY_GRACE_DAYS)
    async with PostgresClient.pool.acquire() as conn:
        # 1. active → expired
        marked = await conn.execute(
            """
            UPDATE attachments
            SET status = 'expired'
            WHERE status = 'active' AND expires_at < $1
            """,
            now,
        )
        stats["marked_expired"] = _parse_affected(marked)

        # 2. expired → purged（过宽限期），先取 object_key 再删文件再标
        rows = await conn.fetch(
            """
            SELECT id, object_key, storage_kind FROM attachments
            WHERE status = 'expired' AND expires_at < $1
            """,
            grace_cutoff,
        )
        for r in rows:
            if r["storage_kind"] == "object_store" and r["object_key"]:
                _safe_remove_file(r["object_key"])
            await conn.execute(
                """
                UPDATE attachments
                SET status = 'purged', purged_at = $2
                WHERE id = $1 AND status = 'expired'
                """,
                int(r["id"]),
                now,
            )
            stats["purged"] += 1
    if stats["marked_expired"] or stats["purged"]:
        logger.info("[attachment] sweep {}", stats)
    return stats


def _parse_affected(result: str) -> int:
    """asyncpg execute 返回 'UPDATE N'，取 N。"""
    try:
        return int(str(result).split()[-1])
    except Exception:
        return 0


def _safe_remove_file(path: str) -> None:
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except Exception as exc:  # noqa: BLE001 — 清理失败不阻断
        logger.warning("[attachment] failed to remove object {}: {}", path, exc)


def object_path(attachment: dict) -> str | None:
    """取附件的物理读取路径（供 HTTP content 端点流式读）。"""
    if not isinstance(attachment, dict):
        return None
    kind = attachment.get("storage_kind")
    if kind == "object_store":
        return attachment.get("object_key")
    if kind == "workspace_ref":
        return attachment.get("workspace_path")
    return None


# Raster 图片 magic-byte 签名表：(签名前缀, 对应 mime)。SVG 不在内——它同源 inline
# 可执行脚本/引用外部资源，必须走 attachment 下载，不能进 inline 预览。
_RASTER_IMAGE_SIGNATURES: tuple[tuple[bytes, str], ...] = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"RIFF", "image/webp"),  # WebP 以 RIFF....WEBP 开头，下方再校验 WEBP 标记
    (b"\x00\x00\x01\x00", "image/x-icon"),
    (b"\x00\x00\x02\x00", "image/x-icon"),
)

# SVG 关键字：在文件首部出现即认定为 SVG（不拘泥于 <?xml 前导空白/注释）。
_SVG_MARKER = b"<svg"


def _read_head(path: str, size: int = 512) -> bytes:
    """读文件首部少量字节用于 magic-byte 校验，不整读大文件。"""
    try:
        with open(path, "rb") as f:
            return f.read(size)
    except Exception:  # noqa: BLE001 — 读失败由调用方按「校验不过」处理
        return b""


def detect_image_kind(path: str) -> str | None:
    """按 magic-byte 判定真实图片类型。返回 raster mime，或 'image/svg+xml'，
    或 None（不是受支持的内联图片）。

    inline 预览只允许 raster；SVG 由调用方判定后强制 attachment 下载。
    """
    head = _read_head(path)
    if not head:
        return None
    # SVG：先看首部是否直接以 <svg 开头（最常见），否则剥掉 BOM/空白再找标记。
    if head.lstrip()[:4].lower() == b"<svg" or _SVG_MARKER in head[:512]:
        return "image/svg+xml"
    for sig, mime in _RASTER_IMAGE_SIGNATURES:
        if head.startswith(sig):
            if mime == "image/webp" and b"WEBP" not in head[:16]:
                continue
            return mime
    return None


def is_svg_attachment(attachment: dict, path: str | None = None) -> bool:
    """附件是否 SVG。优先按 magic-byte，无法读字节时退回 mime_type/扩展名。"""
    if not isinstance(attachment, dict):
        return False
    p = path or object_path(attachment) or ""
    if p:
        kind = detect_image_kind(p)
        if kind is not None:
            return kind == "image/svg+xml"
    mime = str(attachment.get("mime_type") or "").lower()
    if mime == "image/svg+xml":
        return True
    name = str(attachment.get("name") or "").lower()
    return name.endswith(".svg")


def to_public_dict(attachment: dict) -> dict:
    """投影成对外安全的 dict：不暴露 object_key / workspace_path。"""
    if not isinstance(attachment, dict):
        return {}
    return {
        "id": attachment.get("id"),
        "name": attachment.get("name"),
        "mime_type": attachment.get("mime_type"),
        "size": attachment.get("size_bytes"),
        "status": attachment.get("status"),
        "expires_at": attachment.get("expires_at").isoformat() if attachment.get("expires_at") else None,
        "created_at": attachment.get("created_at").isoformat() if attachment.get("created_at") else None,
        "last_renewed_at": attachment.get("last_renewed_at").isoformat() if attachment.get("last_renewed_at") else None,
        "renewed_count": attachment.get("renewed_count") or 0,
        "renewable": attachment.get("status") != "purged",
    }


def to_media_part(attachment: dict) -> dict:
    """投影成消息 media part（写进 agent_messages.media）。"""
    if not isinstance(attachment, dict):
        return {}
    return {
        "type": "attachment",
        "attachment_id": attachment.get("id"),
        "name": attachment.get("name"),
        "mime_type": attachment.get("mime_type"),
        "size": attachment.get("size_bytes"),
        "status": attachment.get("status"),
        "expires_at": attachment.get("expires_at").isoformat() if attachment.get("expires_at") else None,
    }


def to_signed_media_part(attachment: dict) -> dict:
    """读路径用：在 to_media_part 基础上补签名 content_url/thumbnail_url。

    签名 URL 短时（默认 2h），不写库——每次 serve 消息时现签，过期后前端重新
    请求消息会拿到新签名。公网 url part 不走这里（无 attachment_id，原样透传）。
    密钥未配置时 sign 返回 None，part 不带 content_url，前端回退登录态路径。
    """
    import attachment_signing

    part = to_media_part(attachment)
    attachment_id = attachment.get("id")
    if isinstance(attachment_id, int) and attachment_id > 0:
        content_url = attachment_signing.sign_attachment_url(attachment_id, "content")
        thumbnail_url = attachment_signing.sign_attachment_url(attachment_id, "thumbnail")
        if content_url:
            part["content_url"] = content_url
        if thumbnail_url:
            part["thumbnail_url"] = thumbnail_url
    return part
