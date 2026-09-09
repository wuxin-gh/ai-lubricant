"""Read-only mail MCP built-in adapter."""
from __future__ import annotations

from typing import Any

from mcp_builtin.mail.client import MailClient, MailClientError
from mcp_runtime.plugin_loader import PluginContext, PluginRegistrar


MAX_LIMIT = 100

# Agent/LLM 返回预算。邮件正文可能是整封 MIME + HTML，原样回给模型既浪费上下文，
# 也会让下一轮请求变成一大坨原始邮件文本。这里只在 MCP 工具边界做投影与截断，
# 管理端 action 仍拿完整数据。
MAIL_TEXT_BUDGET = 4000        # 单封正文最大字符数
MAIL_TOTAL_TEXT_BUDGET = 24000  # 单次响应所有正文合计最大字符数

# 只暴露模型确实需要的字段；raw / raw_content / html_content 等完整 MIME 与重复
# 正文一律不出现在工具结果里。
_MAIL_FIELDS = (
    "id",
    "message_id",
    "subject",
    "from",
    "sender",
    "to",
    "date",
    "created_at",
    "is_read",
    "received_address",
    "requested_address",
)


def _attachment_meta(message: dict) -> list[dict]:
    """附件只回元信息（名字/类型/大小），绝不回内容字节。"""
    items = message.get("attachments")
    if not isinstance(items, list):
        return []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append({
            "filename": item.get("filename") or item.get("name") or "",
            "content_type": item.get("content_type") or item.get("type") or "",
            "size": item.get("size"),
        })
    return out


def _project_message(message: dict, *, text_budget: int) -> tuple[dict, int]:
    """把一封归一化邮件投影成给模型看的安全字段集。

    返回 (投影结果, 本封正文实际占用字符数)。正文优先用纯文本；只有 HTML 的邮件
    在 normalize_mail 里已折算成 text_content，因此这里不再单独回 HTML。
    截断时显式标注，不静默丢内容。
    """
    projected = {key: message[key] for key in _MAIL_FIELDS if message.get(key) not in (None, "")}

    body = message.get("text_content") or ""
    if not isinstance(body, str):
        body = str(body)
    original_length = len(body)
    if text_budget <= 0:
        projected["text_content"] = ""
        projected["content_truncated"] = True
        projected["content_length"] = original_length
        used = 0
    elif original_length > text_budget:
        projected["text_content"] = body[:text_budget]
        projected["content_truncated"] = True
        projected["content_length"] = original_length
        used = text_budget
    else:
        projected["text_content"] = body
        used = original_length

    attachments = _attachment_meta(message)
    if attachments:
        projected["attachments"] = attachments
    return projected, used


def _project_result(result: dict) -> dict:
    """给 MCP 工具（LLM）用的投影结果，带预算与分页元信息。"""
    messages = result.get("messages") or []
    projected: list[dict] = []
    remaining = MAIL_TOTAL_TEXT_BUDGET
    truncated_count = 0

    for message in messages:
        if not isinstance(message, dict):
            continue
        item, used = _project_message(message, text_budget=min(MAIL_TEXT_BUDGET, remaining))
        remaining -= used
        if item.get("content_truncated"):
            truncated_count += 1
        projected.append(item)

    limit = result.get("limit") or 0
    offset = result.get("offset") or 0
    count = result.get("count") or 0
    return {
        "requested_address": result.get("requested_address"),
        "received_address": result.get("received_address"),
        "messages": projected,
        "returned_count": len(projected),
        "count": count,
        "limit": limit,
        "offset": offset,
        "has_more": bool(limit) and count >= limit,
        "next_offset": offset + count if limit and count >= limit else None,
        "truncated_count": truncated_count,
        "body_budget_exhausted": remaining <= 0,
    }



def _accounts(ctx: PluginContext, *, instance_key: str | None = None) -> list[dict]:
    accounts = list(ctx.resources.get("upstream_accounts") or [])
    if instance_key is not None:
        # 外部 MCP token 绑定单个邮箱实例：只暴露该实例的账户，杜绝跨实例读取。
        accounts = [a for a in accounts if str(a.get("instance_key") or "") == instance_key]
    return accounts


def _configured_address(ctx: PluginContext, address: str, *, instance_key: str | None = None) -> dict | None:
    normalized = (address or "").strip().lower()
    for config in _accounts(ctx, instance_key=instance_key):
        if not config.get("enabled", True):
            continue
        for mapping in config.get("addresses") or []:
            if (mapping.get("address") or "").strip().lower() == normalized:
                return {"config": config, "mapping": mapping, "requested_address": normalized}
    return None


def _bounded_int(value: Any, *, default: int, low: int, high: int, name: str) -> int:
    if value is None:
        return default
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if number < low or number > high:
        raise ValueError(f"{name} 必须在 {low} 到 {high} 之间")
    return number


def _configured_account(ctx: PluginContext, config_id: Any, *, instance_key: str | None = None) -> dict | None:
    try:
        wanted = int(config_id)
    except (TypeError, ValueError):
        return None
    return next(
        (item for item in _accounts(ctx, instance_key=instance_key)
         if item.get("enabled", True) and int(item.get("id") or -1) == wanted),
        None,
    )


async def _resolve_account_scope(token: str) -> tuple[str | None, set[int] | None]:
    """解析邮箱 token 的实例/账户范围；principal 返回显式允许的 mail_account ids。"""
    if not token:
        return None, None
    try:
        import builtin_tool_store
        import mcp_plugin_store

        resolved = await builtin_tool_store.resolve_token(token)
    except Exception:
        return None, None
    if not resolved:
        return None, None
    if resolved.get("kind") == "resource":
        target = resolved.get("target") or {}
        # 资源模型：外部 token 绑单个 mail_account 资源，scope 即该账户 id。
        if target.get("resource_type") == "mail_account":
            return None, {int(target.get("id") or -1)}
        return None, None
    if resolved.get("kind") in ("principal", "identity"):
        # 公共解析（principal 直取；identity agent/task → principal 反查）。
        principal_id = await mcp_plugin_store.resolve_identity_to_principal(token)
        if principal_id is None:
            return None, None
        # principal 绑了 mail_account_id grants（多行=多账户）→ 显式允许集。
        values = await mcp_plugin_store.get_principal_grant_values(principal_id, "mail_account_id")
        if values:
            return None, {int(v) for v in values}
        # 未绑实例 → 默认全量：owner 名下全部 enabled 邮箱账户都可用（account_ids=None
        # 即不限制，与 mail 的账户过滤语义天然一致）。解析不出 owner（平台级
        # principal 等）维持 None（交由上游账户列表自身按 instance_key 收敛）。
        return None, None


async def _resolve_instance_key(token: str) -> str | None:
    """外部 MCP token（绑定单个邮箱账户资源）→ 账户隔离键，用于隔离账户范围。

    资源模型下外部 token 绑 mail_account 资源；返回该资源 id 字符串作隔离键
    （upstream_accounts 的 instance_key 已是资源 id）。其它情形返回 None，不限制。
    """
    if not token:
        return None
    try:
        import builtin_tool_store
        resolved = await builtin_tool_store.resolve_token(token)
    except Exception:
        return None
    if not resolved or resolved.get("kind") != "resource":
        return None
    target = resolved.get("target") or {}
    if target.get("resource_type") != "mail_account":
        return None
    rid = target.get("id")
    return str(rid) if rid is not None else None


async def _query_messages(
    args: dict, ctx: PluginContext, *, require_address: bool, instance_key: str | None = None,
    allowed_account_ids: set[int] | None = None,
) -> dict:
    args = args or {}
    account_ids = allowed_account_ids

    def account_allowed(item: dict) -> bool:
        return account_ids is None or int(item.get("id") or -1) in account_ids

    address = str(args.get("address") or "").strip().lower()
    config_id = args.get("config_id")
    resolved = _configured_address(ctx, address, instance_key=instance_key) if address else None
    if resolved and not account_allowed(resolved["config"]):
        resolved = None
    if require_address and not address:
        raise ValueError("address 为必填项")
    if config_id is not None:
        config = _configured_account(ctx, config_id, instance_key=instance_key)
        if not config or not account_allowed(config):
            raise ValueError(f"邮件配置 {config_id} 不存在、已禁用或未授权")
        if address and (not resolved or resolved["config"] is not config):
            raise ValueError(f"该邮箱地址不属于所选邮箱配置: {address}")
    elif resolved:
        config = resolved["config"]
    elif not address:
        # 无地址、无 config_id：取本实例（或全部）已启用的第一个账户，查全部邮件。
        accounts = [
            a for a in _accounts(ctx, instance_key=instance_key)
            if a.get("enabled", True) and account_allowed(a)
        ]
        if not accounts:
            raise ValueError("未找到已启用的邮箱配置")
        config = accounts[0]
    else:
        raise ValueError(f"未找到已启用的邮箱配置: {address}")

    limit = _bounded_int(args.get("limit"), default=20, low=1, high=MAX_LIMIT, name="limit")
    offset = _bounded_int(args.get("offset"), default=0, low=0, high=100000, name="offset")
    keyword = str(args.get("keyword") or "").strip()[:500] or None
    source_address = ""
    if resolved:
        source_address = (resolved["mapping"].get("source_address") or address).strip().lower()

    client = MailClient(
        config.get("username") or "", config.get("password") or "",
        config.get("base_url") or "", config.get("secret_key") or "",
    )
    try:
        async with ctx.http_session() as session:
            messages = await client.list_mail(
                session, limit=limit, offset=offset,
                address=source_address or None, keyword=keyword,
            )
    except MailClientError as exc:
        raise ValueError(str(exc)) from exc

    for message in messages:
        # received_address 由 normalize_mail 从邮件 To 头解析得到；为空时回退到本次查询替换后的邮箱。
        if not message.get("received_address"):
            message["received_address"] = source_address or address
        message["requested_address"] = address
    return {
        "requested_address": address, "received_address": source_address or address,
        "messages": messages, "count": len(messages), "limit": limit, "offset": offset,
    }


async def _mail_info(_args: dict, ctx: PluginContext) -> dict:
    """Return the configured mail suffix and aliases visible to this token."""
    from mcp_runtime.plugin_loader import current_request_token

    instance_key, account_ids = await _resolve_account_scope(current_request_token.get(""))
    accounts = []
    for account in _accounts(ctx, instance_key=instance_key):
        account_id = int(account.get("id") or -1)
        if not account.get("enabled", True):
            continue
        if account_ids is not None and account_id not in account_ids:
            continue

        addresses = []
        for mapping in account.get("addresses") or []:
            if not isinstance(mapping, dict):
                continue
            addresses.append({
                "address": mapping.get("address") or "",
                "source_address": mapping.get("source_address") or "",
                "is_primary": bool(mapping.get("is_primary")),
            })
        accounts.append({
            "config_id": account_id,
            "display_name": account.get("display_name") or "",
            "username": account.get("username") or "",
            "mail_suffix": account.get("mail_suffix") or "",
            "addresses": addresses,
        })
    return {"accounts": accounts}


async def _mail_list(args: dict, ctx: PluginContext) -> dict:
    # MCP 工具调用：从每请求 token 解析绑定的邮箱实例，只允许查该实例的账户/别名。
    from mcp_runtime.plugin_loader import current_request_token

    instance_key, account_ids = await _resolve_account_scope(current_request_token.get(""))
    result = await _query_messages(
        args, ctx, require_address=True, instance_key=instance_key,
        allowed_account_ids=account_ids,
    )
    return _project_result(result)


async def _query_messages_action(args: dict, ctx: PluginContext) -> dict:
    # 管理端 action：显式 config_id 选账户，不按实例隔离（admin 全量视角）。
    return await _query_messages(args, ctx, require_address=False)


def register(reg: PluginRegistrar) -> None:
    reg.tool(
        name="mail_info",
        description=(
            "List the configured mail services visible to the current caller, including each service's "
            "mail suffix and forwarding aliases. Use this before choosing an address for mail_list."
        ),
        params={"type": "object", "properties": {}},
    )(_mail_info)
    reg.tool(
        name="mail_list",
        description=(
            "List mail for a configured address. The address may be a configured forwarding alias; "
            "results explicitly identify the source mailbox when an alias is used."
        ),
        params={
            "type": "object",
            "properties": {
                "address": {"type": "string", "description": "A configured primary or forwarding-alias email address."},
                "keyword": {"type": "string", "description": "Optional upstream keyword filter."},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "required": ["address"],
        },
    )(_mail_list)
    reg.action(
        name="query_messages",
        description="Query messages for a selected mailbox configuration.",
        params={
            "type": "object",
            "properties": {
                "config_id": {"type": "integer"},
                "address": {"type": "string"},
                "keyword": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_LIMIT, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "required": ["config_id"],
        },
    )(_query_messages_action)


def apply_config(_ctx: PluginContext) -> None:
    """Config is read dynamically from PluginContext by each tool call."""


__all__ = ["register", "apply_config"]
