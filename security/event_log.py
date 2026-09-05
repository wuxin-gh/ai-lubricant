"""安全事件日志 —— 写入 security_events 表 + 通知服务"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from security.types import RiskTag

logger = logging.getLogger("security.event_log")


# 通知标题映射（pipeline 与本模块共用，避免两份映射漂移）
TAG_TITLE_MAP: dict[str, str] = {
    "secret:anthropic-key": "检测到 Anthropic Key 泄露",
    "secret:openai-key": "检测到 OpenAI Key 泄露",
    "secret:aws-akid": "检测到 AWS Key 泄露",
    "secret:github-pat": "检测到 GitHub Token 泄露",
    "secret:bearer-token": "检测到 Bearer Token 泄露",
    "secret:google-key": "检测到 Google Key 泄露",
    "secret:slack-token": "检测到 Slack Token 泄露",
    "secret:stripe-key": "检测到 Stripe Key 泄露",
    "secret:aws-secret": "检测到 AWS Secret 泄露",
    "injection:instruction-override": "检测到指令覆盖注入攻击",
    "injection:role-override": "检测到角色注入攻击",
    "injection:exfiltration": "检测到数据外泄指令",
    "injection:hidden-chars": "检测到隐藏字符",
    "input:empty-messages": "请求消息为空",
    "input:invalid-role": "请求包含无效 Role",
    "input:message-too-large": "请求消息过大",
    "input:too-many-messages": "请求消息数量过多",
}


async def _maybe_create_notification(
    *,
    tag: str,
    severity: str,
    detail: dict | None = None,
    api_key: str | None = None,
    model: str | None = None,
    request_id: str = "",
    request_log_id: int | None = None,
) -> None:
    """高危事件自动创建通知（复用现有 notifications 表）"""
    if severity not in ("high", "warn"):
        return
    try:
        from db import PostgresClient
        if not PostgresClient.pool:
            return

        title = TAG_TITLE_MAP.get(tag, f"安全事件: {tag}")
        message_parts = []
        # 命中规则维度：title 已含规则名，但 message 也要带，便于通知列表里直接看到原因
        if tag:
            message_parts.append(f"规则: {title}")
        if api_key:
            message_parts.append(f"API Key: {api_key[:8]}...")
        if model:
            message_parts.append(f"模型: {model}")
        if request_id:
            message_parts.append(f"请求ID: {request_id}")
        if detail:
            for k, v in detail.items():
                # prefix/match_preview 含敏感前缀，脱敏后保留类型信息；其余明细原样带出
                if k in ("prefix", "match_preview"):
                    message_parts.append(f"{k}: ***")
                else:
                    message_parts.append(f"{k}: {v}")
        message = " | ".join(message_parts) if message_parts else ""

        params = {
            "tag": tag,
            "request_id": request_id,
            "model": model,
            "api_key": f"{api_key[:8]}..." if api_key else None,
        }
        # 统一通知域：站内 + 出站 outbox（worker 按订阅规则推送 webhook）。
        from monkeycode_compat.notify_core import emit_notification
        await emit_notification(
            "security.warning",
            params=params,
            owner_type="platform",
            severity=severity,
            kind="security",
            source="security_scanner",
            title=title,
            message=message,
            dedupe_key=f"security:{tag}:{api_key or 'anon'}",
            dedupe_window_seconds=300,
            request_log_id=request_log_id,
        )
    except Exception as e:
        logger.warning(f"安全通知创建失败: {e}")


async def log_security_event(
    *,
    request_id: str = "",
    event_type: str = "risk_detected",
    tag: str = "",
    severity: str = "info",
    detail: dict | None = None,
    api_key: str | None = None,
    model: str | None = None,
    source_ip: str | None = None,
    request_log_id: int | None = None,
) -> int | None:
    """写入一条安全事件到 DB，高危事件同时创建通知。

    返回新建事件的 id（无连接或写库失败时返回 None）。
    """
    try:
        from db import PostgresClient
        if not PostgresClient.pool:
            return None
        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO security_events
                    (request_id, request_log_id, event_type, severity, tag, detail, api_key, model, source_ip)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
                RETURNING id
                """,
                request_id,
                request_log_id,
                event_type,
                severity,
                tag,
                # asyncpg 的 jsonb 列默认未注册 dict 编解码器，必须传 JSON 字符串
                json.dumps(detail or {}, ensure_ascii=False),
                api_key,
                model,
                source_ip,
            )
            event_id = row["id"] if row else None
        # 高危/警告事件同步创建通知
        await _maybe_create_notification(
            tag=tag, severity=severity, detail=detail,
            api_key=api_key, model=model, request_id=request_id,
            request_log_id=request_log_id,
        )
        return event_id
    except Exception as e:
        # 安全日志写入失败不应阻断业务
        logger.warning(f"安全事件日志写入失败: {e}")
        return None


async def log_security_events(
    tags: list[RiskTag],
    *,
    request_id: str = "",
    api_key: str | None = None,
    model: str | None = None,
    source_ip: str | None = None,
    request_log_id: int | None = None,
) -> list[int]:
    """批量记录一批 RiskTag，返回成功写入的事件 id 列表。"""
    event_ids: list[int] = []
    for t in tags:
        eid = await log_security_event(
            request_id=request_id,
            request_log_id=request_log_id,
            event_type="risk_detected",
            tag=t.tag,
            severity=t.severity,
            detail=t.detail,
            api_key=api_key,
            model=model,
            source_ip=source_ip,
        )
        if eid is not None:
            event_ids.append(eid)
    return event_ids
