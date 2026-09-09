"""Agent / 聊天对话存储层（ClickHouse）。

镜像原 ``db.PostgresClient`` 的对话方法签名，让 ``agent/api.py`` /
``agent/client_api.py`` 最小改动地从 Postgres 切到 ClickHouse。

存储模型：``agent_conversations`` / ``agent_messages`` 两张
ReplacingMergeTree(version) 表（建表见 ``integrations/clickhouse.py`` 的
``ensure_conversation_schema``）。每次「更新/删除」都变成一次新插入：

* ``version`` 单调递增（纳秒时间戳），ReplacingMergeTree 在后台合并时保留
  最大 version 的行。读用 ``FINAL`` 取最新版本。
* 删除 = 插一行同 id、``status='deleted'``、version=now 的新版本（软删），
  避免重 mutation。列表/查询都过滤 ``status != 'deleted'``。

ClickHouse 未启用时，所有方法抛 ``ClickHouseUnavailable``，由 API 层转成明确
错误（不 500）。
"""
from __future__ import annotations

import asyncio
import base64
import json
import time
from typing import Any

from loguru import logger


class ClickHouseUnavailable(RuntimeError):
    """ClickHouse 未启用或未连接，对话存储不可用。"""


_client = None  # ClickHousePayloadClient，由 main.py 生命周期 connect/close
_lock = asyncio.Lock()  # 序列化同一纳秒内的并发写，保证 version 单调
_msg_counter: int = 0  # 进程内 message id 计数器，启动时种子化
_ready: bool = False


def attach_client(client) -> None:
    """main.py 启动时注入已 connect 的 ClickHousePayloadClient。"""
    global _client, _ready
    _client = client
    _ready = client is not None


def is_ready() -> bool:
    return _ready and _client is not None


async def seed_message_id() -> None:
    """启动时从 agent_messages 取 max(id) 种子化进程内计数器（单实例假设）。"""
    global _msg_counter
    if not is_ready():
        return
    try:
        rows = await _client.query("SELECT max(id) AS m FROM agent_messages")
        if rows and rows[0].get("m") is not None:
            _msg_counter = int(rows[0]["m"])
            logger.info("[conversation_store] message id seeded to {}", _msg_counter)
    except Exception:
        logger.exception("[conversation_store] seed message id failed")


def _next_version() -> int:
    """纳秒时间戳作为 version，天然单调。配合 _lock 保证写序。"""
    return time.time_ns()


def _now_ch() -> str:
    """ClickHouse DateTime64(3) 期望的 'YYYY-MM-DD HH:MM:SS.mmm' UTC 字符串。"""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + f".{int((time.time() % 1) * 1000):03d}"


def _extract_cdp_client_id(chat_settings: dict | None) -> str | None:
    if not chat_settings:
        return None
    v = chat_settings.get("cdp_client_id")
    return v if v else None


def _conv_row_to_dict(row: dict) -> dict:
    """把 ClickHouse 查询行还原成与原 PostgresClient 一致的 dict 形状。"""
    d = dict(row)
    cs = d.get("chat_settings")
    if isinstance(cs, str) and cs:
        try:
            d["chat_settings"] = json.loads(cs)
        except (json.JSONDecodeError, TypeError):
            pass
    # ReplacingMergeTree 行不含 version 字段对外；移除避免泄露。
    d.pop("version", None)
    return d


def _msg_row_to_dict(row: dict) -> dict:
    d = dict(row)
    for f in ("tool_calls", "tool_results", "media", "usage"):
        v = d.get(f)
        if isinstance(v, str) and v:
            try:
                d[f] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                pass
    _sign_media_urls(d)
    d.pop("version", None)
    return d


def _sign_media_urls(d: dict) -> None:
    """serve 时给每个 attachment media part 补签名 content_url/thumbnail_url。

    短时签名不写库；这里现签，保证每次读消息拿到有效期内 URL。已带 content_url
    的 part（历史/其它来源）跳过。无 attachment_id 的公网 url part 不动。
    """
    media = d.get("media")
    if not isinstance(media, list):
        return
    import attachment_signing
    for part in media:
        if not isinstance(part, dict) or part.get("type") != "attachment":
            continue
        attachment_id = part.get("attachment_id")
        if not isinstance(attachment_id, int) or attachment_id <= 0:
            continue
        if not part.get("content_url"):
            url = attachment_signing.sign_attachment_url(attachment_id, "content")
            if url:
                part["content_url"] = url
        if not part.get("thumbnail_url"):
            url = attachment_signing.sign_attachment_url(attachment_id, "thumbnail")
            if url:
                part["thumbnail_url"] = url


async def _require() -> None:
    if not is_ready():
        raise ClickHouseUnavailable(
            "ClickHouse is required for agent conversations but not enabled"
        )


# ── 对话 CRUD ────────────────────────────────────────────────────────


async def create_conversation(
    title: str = "新对话",
    system_prompt: str = "",
    model: str = "",
    agent_id: int | None = None,
    llm_model_id: int | None = None,
    kind: str = "agent",
    chat_settings: dict | None = None,
    user_id: str | None = None,
) -> dict:
    await _require()
    import uuid as _uuid

    conv_id = str(_uuid.uuid4())
    now = _now_ch()
    ver = _next_version()
    cs_json = json.dumps(chat_settings, ensure_ascii=False) if chat_settings else ""
    cdp = _extract_cdp_client_id(chat_settings)
    await _client.insert_conversation_row([
        conv_id, title or "新对话", system_prompt or "", model or "",
        llm_model_id, "active", kind, agent_id, user_id, cdp,
        cs_json, now, now, ver,
    ])
    return {
        "id": conv_id,
        "title": title or "新对话",
        "system_prompt": system_prompt or "",
        "model": model or "",
        "llm_model_id": llm_model_id,
        "status": "active",
        "kind": kind,
        "agent_id": agent_id,
        "user_id": user_id,
        "cdp_client_id": cdp,
        "chat_settings": chat_settings,
        "created_at": now,
        "updated_at": now,
    }


async def list_conversations(limit: int = 50, kind: str = "agent") -> list[dict]:
    await _require()
    rows = await _client.query(
        f"""
        SELECT id, title, system_prompt, model, llm_model_id, status, kind,
               agent_id, user_id, cdp_client_id, chat_settings, created_at, updated_at
        FROM agent_conversations FINAL
        WHERE status = 'active' AND kind = %(kind)s
        ORDER BY updated_at DESC
        LIMIT %(limit)s
        """,
        {"kind": kind, "limit": int(limit)},
    )
    return [_conv_row_to_dict(r) for r in rows]


async def list_conversations_for_caller(
    user_id: str | None, limit: int = 50, kind: str = "agent"
) -> list[dict]:
    """user_id=None 管理员模式（不过滤,管理端用,走本函数）；字符串则只看自己的。

    历史上 None 也回落到全量 list_conversations(),但这让应急 Bearer 管理员与用户态
    共用同一回落,普通用户态不会触发(用户态 caller 永远是 uid)。为消除歧义,管理端
    看全量改走独立的 list_conversations_admin();用户态 caller 为 uid 走 user_id 过滤。
    本函数 user_id=None 时仍返回全量(供管理端兼容),但用户列表端点不会传 None 进来。
    """
    if user_id is None:
        return await list_conversations(limit=limit, kind=kind)
    await _require()
    rows = await _client.query(
        f"""
        SELECT id, title, system_prompt, model, llm_model_id, status, kind,
               agent_id, user_id, cdp_client_id, chat_settings, created_at, updated_at
        FROM agent_conversations FINAL
        WHERE status = 'active' AND kind = %(kind)s AND user_id = %(uid)s
        ORDER BY updated_at DESC
        LIMIT %(limit)s
        """,
        {"kind": kind, "uid": user_id, "limit": int(limit)},
    )
    return [_conv_row_to_dict(r) for r in rows]


async def list_conversations_for_caller_paged(
    user_id: str | None,
    *,
    limit: int = 20,
    kind: str = "agent",
    cursor: str | None = None,
    agent_id: int | None = None,
    node_id: str | None = None,
    search: str | None = None,
) -> dict:
    """按归属列出会话，使用 ``updated_at + id`` 稳定游标倒序分页。

    ``search`` 按标题不区分大小写子串过滤。过滤在 SQL 里做而不是前端筛当前页，
    否则关键字命中的会话只要不在已加载那一页就搜不到，看起来像「历史丢了」。
    """
    await _require()
    page_size = max(1, min(int(limit), 100))
    fetch_n = page_size + 1
    clauses = ["status = 'active'", "kind = %(kind)s"]
    params: dict[str, Any] = {"kind": kind, "limit": fetch_n}
    if user_id is not None:
        clauses.append("user_id = %(uid)s")
        params["uid"] = user_id
    if agent_id is not None:
        clauses.append("agent_id = %(aid)s")
        params["aid"] = int(agent_id)
    if node_id:
        clauses.append("JSONExtractString(chat_settings, 'node_id') = %(nid)s")
        params["nid"] = str(node_id)
    keyword = (search or "").strip()
    if keyword:
        clauses.append("positionCaseInsensitive(title, %(kw)s) > 0")
        params["kw"] = keyword
    if cursor:
        try:
            raw = base64.urlsafe_b64decode(cursor.encode("ascii") + b"=" * (-len(cursor) % 4))
            cursor_time, cursor_id = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ValueError("invalid conversation cursor") from exc
        clauses.append("(updated_at < parseDateTime64BestEffort(%(cur_time)s, 3) OR (updated_at = parseDateTime64BestEffort(%(cur_time)s, 3) AND id < %(cur_id)s))")
        params.update({"cur_time": str(cursor_time), "cur_id": str(cursor_id)})
    rows = await _client.query(
        f"""
        SELECT id, title, system_prompt, model, llm_model_id, status, kind,
               agent_id, user_id, cdp_client_id, chat_settings, created_at, updated_at
        FROM agent_conversations FINAL
        WHERE {' AND '.join(clauses)}
        ORDER BY updated_at DESC, id DESC
        LIMIT %(limit)s
        """,
        params,
    )
    has_next = len(rows) > page_size
    page_rows = rows[:page_size]
    items = [_conv_row_to_dict(row) for row in page_rows]
    next_cursor = None
    if has_next and items:
        last = items[-1]
        payload = json.dumps([_conv_updated_at_str(last), str(last["id"])], separators=(",", ":"))
        next_cursor = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
    return {"items": items, "page": {"next_cursor": next_cursor, "has_next_page": has_next}}


async def list_conversations_admin(
    *, kind: str, limit: int = 20, cursor: str | None = None
) -> dict:
    """管理端游标分页：按 updated_at 倒序，cursor 是上一页末条的 updated_at 字符串。

    返回 {conversations, page:{cursor, has_next_page}}。
    多取 1 条判断 has_next_page（与 user_platform audit 分页同口径）。
    """
    await _require()
    fetch_n = max(1, int(limit)) + 1
    params: dict[str, Any] = {"kind": kind, "limit": fetch_n}
    where = "status = 'active' AND kind = %(kind)s"
    if cursor:
        params["cur"] = cursor
        where += " AND toString(updated_at) < %(cur)s"
    rows = await _client.query(
        f"""
        SELECT id, title, system_prompt, model, llm_model_id, status, kind,
               agent_id, user_id, cdp_client_id, chat_settings, created_at, updated_at
        FROM agent_conversations FINAL
        WHERE {where}
        ORDER BY updated_at DESC
        LIMIT %(limit)s
        """,
        params,
    )
    has_next = len(rows) > limit
    page_rows = rows[:limit] if has_next else rows
    convs = [_conv_row_to_dict(r) for r in page_rows]
    next_cursor = None
    if has_next and convs:
        next_cursor = _conv_updated_at_str(convs[-1])
    return {"conversations": convs, "page": {"cursor": next_cursor, "has_next_page": has_next}}


def _conv_updated_at_str(conv: dict) -> str:
    """把 updated_at 归一成可比较的字符串（ClickHouse toString 风格），用作游标。"""
    v = conv.get("updated_at")
    if v is None:
        return ""
    # ClickHouse 返回 datetime；转成 'YYYY-MM-DD HH:MM:SS.mmm' 字符串以匹配 toString()。
    try:
        return v.strftime("%Y-%m-%d %H:%M:%S") + f".{v.microsecond // 1000:03d}"
    except AttributeError:
        return str(v)


async def list_conversations_for_cdp_client(
    client_id: str,
    *,
    session_key: str | None = None,
    agent_id: int | None = None,
    limit: int = 50,
) -> list[dict]:
    """列出某 CDP 客户端名下的 agent 会话。

    cdp_client_id 在写入时从 chat_settings 拆成独立列，这里直接列过滤（避免
    JSONExtract 全表扫描）。session_key 仍在 chat_settings JSON 内，用
    JSONExtractString 兜底（CDP 会话量小，可接受）。
    """
    await _require()
    cid = (client_id or "").strip()
    if not cid:
        return []
    clauses = ["status = 'active'", "kind = 'agent'", "cdp_client_id = %(cid)s"]
    params: dict[str, Any] = {"cid": cid, "limit": int(limit)}
    if session_key:
        params["sk"] = session_key
        clauses.append("JSONExtractString(chat_settings, 'cdp_session_key') = %(sk)s")
    if agent_id is not None:
        params["aid"] = int(agent_id)
        clauses.append("agent_id = %(aid)s")
    where = " AND ".join(clauses)
    rows = await _client.query(
        f"""
        SELECT id, title, system_prompt, model, llm_model_id, status, kind,
               agent_id, user_id, cdp_client_id, chat_settings, created_at, updated_at
        FROM agent_conversations FINAL
        WHERE {where}
        ORDER BY updated_at DESC
        LIMIT %(limit)s
        """,
        params,
    )
    return [_conv_row_to_dict(r) for r in rows]


async def get_conversation(conv_id: str) -> dict | None:
    await _require()
    rows = await _client.query(
        """
        SELECT id, title, system_prompt, model, llm_model_id, status, kind,
               agent_id, user_id, cdp_client_id, chat_settings, created_at, updated_at
        FROM agent_conversations FINAL
        WHERE id = %(id)s
        LIMIT 1
        """,
        {"id": conv_id},
    )
    if not rows:
        return None
    return _conv_row_to_dict(rows[0])


async def get_conversation_owned(user_id: str | None, conv_id: str) -> dict | None:
    """user_id=None 管理员等价 get_conversation；否则需 user_id 匹配。"""
    conv = await get_conversation(conv_id)
    if conv is None:
        return None
    if conv.get("status") == "deleted":
        return None
    if user_id is None:
        return conv
    if str(conv.get("user_id") or "") != str(user_id):
        return None
    return conv


async def delete_conversation(conv_id: str) -> bool:
    """软删：插一行同 id、status='deleted'、version=now 的新版本。"""
    await _require()
    conv = await get_conversation(conv_id)
    if conv is None or conv.get("status") == "deleted":
        return False
    async with _lock:
        ver = _next_version()
        now = _now_ch()
        cs = conv.get("chat_settings")
        cs_json = json.dumps(cs, ensure_ascii=False) if cs else ""
        await _client.insert_conversation_row([
            conv_id, conv.get("title", ""), conv.get("system_prompt", ""),
            conv.get("model", ""), conv.get("llm_model_id"), "deleted",
            conv.get("kind", "agent"), conv.get("agent_id"), conv.get("user_id"),
            _extract_cdp_client_id(cs) if isinstance(cs, dict) else None,
            cs_json, conv.get("created_at", now), now, ver,
        ])
    return True


async def update_conversation(conv_id: str, **fields) -> dict:
    """更新 = 读当前行 + 写一行带新字段、version=now 的副本。"""
    await _require()
    allowed = {"title", "system_prompt", "model", "status", "llm_model_id", "chat_settings"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    conv = await get_conversation(conv_id)
    if conv is None:
        return {}
    if not updates:
        return conv
    conv.update(updates)
    async with _lock:
        ver = _next_version()
        now = _now_ch()
        cs = conv.get("chat_settings")
        cs_json = json.dumps(cs, ensure_ascii=False) if cs else ""
        await _client.insert_conversation_row([
            conv_id, conv.get("title", ""), conv.get("system_prompt", ""),
            conv.get("model", ""), conv.get("llm_model_id"),
            conv.get("status", "active"), conv.get("kind", "agent"),
            conv.get("agent_id"), conv.get("user_id"),
            _extract_cdp_client_id(cs) if isinstance(cs, dict) else None,
            cs_json, conv.get("created_at", now), now, ver,
        ])
    return conv


async def add_message(
    conversation_id: str,
    role: str,
    content: str = "",
    tool_calls=None,
    tool_results=None,
    turn_number: int = 0,
    status: str = "done",
    error: str | None = None,
    media=None,
    model: str = "",
    usage: dict | None = None,
    reasoning: str = "",
) -> dict:
    await _require()
    global _msg_counter
    async with _lock:
        _msg_counter += 1
        msg_id = _msg_counter
        ver = _next_version()
    now = _now_ch()
    tc = json.dumps(tool_calls, ensure_ascii=False) if tool_calls else ""
    tr = json.dumps(tool_results, ensure_ascii=False) if tool_results else ""
    md = json.dumps(media, ensure_ascii=False) if media else ""
    us = json.dumps(usage, ensure_ascii=False) if usage else ""
    await _client.insert_message_row([
        msg_id, conversation_id, role, content, tc, tr, turn_number,
        status, error, md, model, us, reasoning or "", now, ver,
    ])
    return {
        "id": msg_id,
        "conversation_id": conversation_id,
        "role": role,
        "content": content,
        "tool_calls": tool_calls,
        "tool_results": tool_results,
        "turn_number": turn_number,
        "status": status,
        "error": error,
        "media": media,
        "model": model,
        "usage": usage or None,
        "reasoning": reasoning or "",
        "created_at": now,
    }


async def get_messages_page_by_user_turns(
    conversation_id: str,
    *,
    limit: int = 7,
    cursor: int | None = None,
) -> dict:
    """按用户轮次分页消息；cursor 是较新一页最早的 user message id。"""
    await _require()
    page_size = max(1, min(int(limit), 50))
    params: dict[str, Any] = {"cid": conversation_id, "limit": page_size + 1}
    cursor_clause = ""
    if cursor is not None:
        params["cursor"] = int(cursor)
        cursor_clause = "AND id < %(cursor)s"
    user_rows = await _client.query(
        f"""
        SELECT id
        FROM agent_messages FINAL
        WHERE conversation_id = %(cid)s AND role = 'user' AND status != 'deleted' {cursor_clause}
        ORDER BY id DESC
        LIMIT %(limit)s
        """,
        params,
    )
    has_more = len(user_rows) > page_size
    selected = user_rows[:page_size]
    if not selected:
        return {"messages": [], "page": {"next_cursor": None, "has_more": False}}
    oldest_user_id = int(selected[-1]["id"])
    message_params: dict[str, Any] = {"cid": conversation_id, "oldest": oldest_user_id}
    upper_clause = ""
    if cursor is not None:
        message_params["cursor"] = int(cursor)
        upper_clause = "AND id < %(cursor)s"
    rows = await _client.query(
        f"""
        SELECT id, conversation_id, role, content, tool_calls, tool_results,
               turn_number, status, error, media, model, usage, reasoning, created_at
        FROM agent_messages FINAL
        WHERE conversation_id = %(cid)s AND id >= %(oldest)s AND status != 'deleted' {upper_clause}
        ORDER BY id ASC
        """,
        message_params,
    )
    return {
        "messages": [_msg_row_to_dict(row) for row in rows],
        "page": {"next_cursor": oldest_user_id if has_more else None, "has_more": has_more},
    }


async def get_messages(conversation_id: str) -> list[dict]:
    await _require()
    rows = await _client.query(
        """
        SELECT id, conversation_id, role, content, tool_calls, tool_results,
               turn_number, status, error, media, model, usage, reasoning, created_at
        FROM agent_messages FINAL
        WHERE conversation_id = %(cid)s AND status != 'deleted'
        ORDER BY id ASC
        """,
        {"cid": conversation_id},
    )
    return [_msg_row_to_dict(r) for r in rows]


async def update_message(message_id: int, **fields) -> None:
    """更新消息 = 读旧行 + 写新 version 副本（带新字段）。"""
    await _require()
    allowed = {"content", "tool_calls", "tool_results", "status", "error", "turn_number", "media", "model", "usage", "reasoning"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    rows = await _client.query(
        """
        SELECT id, conversation_id, role, content, tool_calls, tool_results,
               turn_number, status, error, media, model, usage, reasoning, created_at
        FROM agent_messages FINAL
        WHERE id = %(id)s
        LIMIT 1
        """,
        {"id": int(message_id)},
    )
    if not rows:
        return
    msg = _msg_row_to_dict(rows[0])
    msg.update(updates)
    async with _lock:
        ver = _next_version()
    tc = json.dumps(msg.get("tool_calls"), ensure_ascii=False) if msg.get("tool_calls") else ""
    tr = json.dumps(msg.get("tool_results"), ensure_ascii=False) if msg.get("tool_results") else ""
    md = json.dumps(msg.get("media"), ensure_ascii=False) if msg.get("media") else ""
    us = json.dumps(msg.get("usage"), ensure_ascii=False) if msg.get("usage") else ""
    await _client.insert_message_row([
        int(msg["id"]), msg["conversation_id"], msg["role"], msg.get("content", ""),
        tc, tr, msg.get("turn_number", 0), msg.get("status", "done"),
        msg.get("error"), md, msg.get("model", ""), us, msg.get("reasoning", "") or "",
        msg.get("created_at", _now_ch()), ver,
    ])


async def reap_stale_streaming_messages() -> int:
    """进程重启后收尾上一进程遗留的 streaming assistant 消息。

    重启 = 所有 in-flight run_agent task 已死，streaming 状态必然无效。把这类
    消息软更新成 error（保留已产出的 content/tool_calls/tool_results/reasoning/
    usage），让前端加载历史时显示中断 + 重试按钮，而不是永久转圈。
    """
    await _require()
    rows = await _client.query(
        "SELECT id FROM agent_messages FINAL WHERE role = 'assistant' AND status = 'streaming'"
    )
    if not rows:
        return 0
    count = 0
    for row in rows:
        await update_message(int(row["id"]), status="error", error="进程重启，任务中断")
        count += 1
    return count


async def delete_message(message_id: int) -> bool:
    """软删一条消息：插一行同 id、status='deleted'、内容清空的新版本。

    与 delete_conversation 同型（ClickHouse ReplacingMergeTree 的 version 覆盖）。
    get_messages / get_messages_page_by_user_turns 已过滤 status='deleted'，故历史
    回放与 agent 上下文都不会再含本条。用于前端驱动聊天重试失败轮时清掉残留
    assistant 消息。
    """
    await _require()
    rows = await _client.query(
        """
        SELECT id, conversation_id, role, content, tool_calls, tool_results,
               turn_number, status, error, media, model, usage, reasoning, created_at
        FROM agent_messages FINAL
        WHERE id = %(id)s
        LIMIT 1
        """,
        {"id": int(message_id)},
    )
    if not rows:
        return False
    msg = _msg_row_to_dict(rows[0])
    if msg.get("status") == "deleted":
        return False
    async with _lock:
        ver = _next_version()
    await _client.insert_message_row([
        int(msg["id"]), msg["conversation_id"], msg["role"], "",
        "", "", msg.get("turn_number", 0), "deleted",
        None, "", "", "", "", msg.get("created_at", _now_ch()), ver,
    ])
    return True


async def last_activity_for_agent(agent_id: int) -> float:
    """该 Agent 最近一次会话活动的 epoch 秒；无会话返回 0.0。

    供自主模式（agent/autonomous_worker.py）判定闲置时长用。ClickHouse 未启用
    时返回 0.0，由调用方降级到 TODO 文件 mtime，不阻断后台 worker。
    """
    if not is_ready():
        return 0.0
    rows = await _client.query(
        """
        SELECT max(updated_at) AS m
        FROM agent_conversations FINAL
        WHERE agent_id = %(aid)s AND status != 'deleted'
        """,
        {"aid": int(agent_id)},
    )
    if not rows or rows[0].get("m") is None:
        return 0.0
    value = rows[0]["m"]
    if hasattr(value, "timestamp"):
        return float(value.timestamp())
    try:
        import datetime as _dt

        return _dt.datetime.fromisoformat(str(value)).replace(
            tzinfo=_dt.timezone.utc
        ).timestamp()
    except Exception:  # noqa: BLE001 — unparseable timestamp degrades to "unknown"
        return 0.0


async def touch_conversation(conv_id: str) -> None:
    """刷新 updated_at = 插一行同内容、version=now 的副本。"""
    await _require()
    conv = await get_conversation(conv_id)
    if conv is None:
        return
    async with _lock:
        ver = _next_version()
        now = _now_ch()
        cs = conv.get("chat_settings")
        cs_json = json.dumps(cs, ensure_ascii=False) if cs else ""
        await _client.insert_conversation_row([
            conv_id, conv.get("title", ""), conv.get("system_prompt", ""),
            conv.get("model", ""), conv.get("llm_model_id"),
            conv.get("status", "active"), conv.get("kind", "agent"),
            conv.get("agent_id"), conv.get("user_id"),
            _extract_cdp_client_id(cs) if isinstance(cs, dict) else None,
            cs_json, conv.get("created_at", now), now, ver,
        ])
