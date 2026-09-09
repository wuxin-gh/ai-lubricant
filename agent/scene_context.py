"""Scene presets for Agent conversation entry points.

Three entry points open a conversation with an implicit *scene*: the cdp-bridge
webpage panel drives a browser, the node terminal page drives a host shell, and
the marketplace admin chat drives the marketplace MCP. Each used to hand-roll
its own prompt text (or, for marketplace, none at all), so the model was told
about its situation in three different shapes and one entry point not at all.

This module is the single source of truth: an entry point passes its own native
inputs to :func:`normalize`, gets a :class:`SceneSpec`, and uses
:func:`render_prompt` for the one-time system prompt segment plus
:func:`persist` for the ``chat_settings`` keys.

Two deliberate properties:

- **Identity keys are written once, and rewritten only when one dies.** A
  scene's identity keys (CDP client_id + tab_id, node_id) hold for the whole
  conversation in the normal case, so they belong in the conversation's
  ``system_prompt`` rather than a per-turn rewrite. A Chrome ``tabId`` survives
  full-page navigation (the CDP session key *is* ``{client_id}:{tabId}``), which
  is why the page's current URL is explicitly NOT part of the scene: it is
  volatile content the model fetches on demand with ``web operation=scan/tabs``.
  A URL snapshot from the previous turn is worse than no URL at all, because the
  model cannot tell it is stale. The one key that *can* die is ``tabId`` — a
  conversation continued from a different tab would leave the model driving a
  closed one — so that case re-resolves the scene and swaps the written segment
  via :func:`replace_prompt` instead of rewriting every turn.
- **Scene-bound capabilities are inlined, not pointed at.** ``services`` names
  the MCP services this scene exists to drive; ``agent_main`` inlines their full
  seeded SOP (method list with parameter schemas) into the first system prompt
  instead of advertising an ``(SOP: ...)`` pointer the model must ``file_read``
  before it can make its first call.

Volatile per-turn facts the model *cannot* fetch itself (terminal cwd, recent
terminal output) stay outside this module — see ``terminal_context`` in
``agent/api.py``.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# Scene type identifiers. ``marketplace_admin`` matches the value the frontend
# already sends as ``CreateConversationRequest.context``, so no client change is
# needed to recognise that scene.
BROWSER = "browser"
NODE_TERMINAL = "node_terminal"
MARKETPLACE_ADMIN = "marketplace_admin"
# 代码渠道 spec 生成场景：管理员在「源码」Tab 的「agent 生成」子 Tab 里描述上游，
# agent 按代码渠道编写规则产出 spec 源码（不自动写入编辑器，用户自行复制/粘贴）。
CODE_CHANNEL = "code_channel"
# 定时任务场景：由调度器按 cron 唤醒，没有人在场。与其它场景的区别不在于驱动哪个
# 外部系统，而在于「现场没有用户」——这决定了 ask_user 不可用、结论必须自己落地。
SCHEDULED = "scheduled"

# The browser-class MCP service backing the CDP scene, and the reserved
# marketplace service granted by ``_provision_marketplace_mcp``.
_CDP_SERVICE = "cdp-bridge"
_MARKETPLACE_SERVICE = "marketplace-status"


@dataclass(frozen=True)
class SceneSpec:
    """One resolved scene: who the agent is driving, and how it must behave."""

    type: str
    # Stable identity keys, rendered into the prompt once. Never volatile state.
    identity: dict[str, str] = field(default_factory=dict)
    # MCP services this scene drives; their full SOP is inlined on turn one.
    services: tuple[str, ...] = ()
    # Scene behaviour rules, rendered after the identity block.
    rules: str = ""


def _clean(value: object) -> str:
    return str(value or "").strip()


def normalize(
    *,
    cdp: dict | None = None,
    node_id: str | None = None,
    context: str | None = None,
    scheduled: dict | None = None,
) -> SceneSpec | None:
    """Resolve a scene from an entry point's native inputs.

    Each entry point passes only what it actually has:

    - CDP panel: ``cdp={"client_id", "session_key", "client_alias"}`` (the
      identity headers injected by the authenticated mcp_runtime connection).
    - Node terminal page: ``node_id``.
    - Marketplace admin chat: ``context="marketplace_admin"``.
    - Scheduler: ``scheduled={"job_id", "name", "cron"}``.

    Returns ``None`` for a plain conversation with no scene.
    """
    if scheduled:
        job_id = _clean(scheduled.get("job_id"))
        if not job_id:
            return None
        identity = {"job_id": job_id}
        task_name = _clean(scheduled.get("name"))
        if task_name:
            identity["task"] = task_name
        cron = _clean(scheduled.get("cron"))
        if cron:
            identity["schedule"] = cron
        return SceneSpec(
            type=SCHEDULED,
            identity=identity,
            rules=(
                "本轮由调度器按上面的 schedule 唤醒，现场没有用户。\n"
                "ask_user 在这里问不到人：需要人裁决的事不要停下等，把判断依据和"
                "建议写进结论，让下一个看报告的人能直接接手。同理，需要人确认的"
                "工具（code_run 等）被拒绝时不要改写命令绕过，记录受阻原因即止。\n"
                "结论只能靠自己落地：没有对话框会替你保留上下文，重要产出写进文件"
                "或按任务要求的位置存档，别只写在回复里。\n"
                "任务背景与上次执行情况（含退出码、连续失败次数）在下面的 "
                "[Scheduled Context] 里给出；判断本轮该做什么以它为准，不要臆测"
                "上次跑成了什么样。"
            ),
        )
    if cdp:
        client_id = _clean(cdp.get("client_id"))
        if not client_id:
            return None
        session_key = _clean(cdp.get("session_key"))
        # session_key is "{client_id}:{tabId}"; the tab id is the stable page
        # identity (it survives full-page navigation).
        prefix = f"{client_id}:"
        tab_id = session_key[len(prefix):] if session_key.startswith(prefix) else ""
        identity = {"client_id": client_id}
        if tab_id:
            identity["tab_id"] = tab_id
        alias = _clean(cdp.get("client_alias"))
        if alias and alias != client_id:
            identity["client_alias"] = alias
        return SceneSpec(
            type=BROWSER,
            identity=identity,
            services=(_CDP_SERVICE,),
            rules=(
                "你正通过 cdp-bridge 操作用户的浏览器。client_id 在整个会话内稳定不变；"
                "tab_id 在当前标签页内稳定，用户换标签页继续这个会话时会更新成本轮所在"
                "标签页。需要 client_id 参数的工具一律传上面这个值，不要向用户追问，也"
                "不要操作其它客户端的会话。\n"
                "当前页面的 URL、标题和内容是会变的，不在本段里给出——需要时用 "
                "web operation=scan 或 operation=tabs 现取，不要沿用历史消息里出现过的"
                "旧 URL。\n"
                "默认操作上面这个 tab。跳转到另一个页面时优先点击页内链接/元素"
                "（web operation=execute），避免直接 navigate 打开新 URL——那会打断当前"
                "页面会话。"
            ),
        )

    node = _clean(node_id)
    if node:
        return SceneSpec(
            type=NODE_TERMINAL,
            identity={"node_id": node},
            rules=(
                "目标节点由服务端绑定（就是上面的 node_id），不要要求用户重复提供节点 ID。"
                "需要读取或修改节点宿主机时，使用 node_shell_exec 工具；没有工具结果时"
                "不要声称操作成功。如果工具返回 confirmation_required 或 denied，明确告诉"
                "管理员需要确认或已被拒绝，不要尝试改写命令绕过确认。\n"
                "当前工作目录与最近终端输出会按轮次单独提供，不在本段里给出。"
            ),
        )

    if _clean(context) == MARKETPLACE_ADMIN:
        return SceneSpec(
            type=MARKETPLACE_ADMIN,
            identity={"scope": "marketplace_admin"},
            services=(_MARKETPLACE_SERVICE,),
            rules=(
                "你处在市场管理场景：通过 marketplace-status 的工具维护市场资源——普通市场 "
                "manifest（marketplace_list_items / marketplace_get_item / marketplace_upsert_item 等）"
                "和外部榜单草稿（marketplace_leaderboard_list / marketplace_leaderboard_get / "
                "marketplace_leaderboard_update / marketplace_leaderboard_publish / "
                "marketplace_leaderboard_unpublish / marketplace_leaderboard_delete / "
                "marketplace_leaderboard_set_sort / marketplace_leaderboard_sync_field / "
                "marketplace_leaderboard_add_github）。\n"
                "这些 MCP 服务是市场管理页面的保留能力：只有本场景的对话临时获权，普通 Agent "
                "的 MCP 选择器里看不到，也不要在其它场景里调用。\n"
                "市场清单与条目明细是会变的——隐藏、删除、导入、发布都会改写它：每次查询与"
                "操作前用工具现取，不要沿用历史消息里出现过的旧清单。\n"
                "写操作（新增/更新/隐藏/删除/导入/发布/撤回）完成后，用对应的查询方法回读一次"
                "再向用户汇报；没有工具结果时不要推测市场的当前状态。"
            ),
        )

    if _clean(context) == CODE_CHANNEL:
        return SceneSpec(
            type=CODE_CHANNEL,
            identity={"scope": "code_channel"},
            rules=(
                "你在帮管理员为「代码渠道」编写 spec 源码。代码渠道是本平台让用户贴一个"
                "普通类（不继承任何基类）、方法用 @staticmethod、首参 p 是渠道实例，"
                "即可造一个完整渠道的机制；没写的钩子回落到 CustomProvider 配置驱动实现。\n"
                "编写规则（严格遵守）：\n"
                "- 类名形如 class XxxChannel，不要写 PROVIDER_NAME（加载器会用渠道 id 覆盖）。\n"
                "- 只用合法钩子名：init_auth / check_auth / is_init / health_check / refresh_auth / "
                "stream_chat / non_stream_chat / fetch_models / headers / build_url / base_url / payload / "
                "parse_chunk / on_response_headers / update_quota / check_message / record_message / "
                "account_schema / build_auth_start / handle_auth_callback / begin_device_flow / "
                "poll_device_flow / on_channel_attached / clear_conversations / close。拼错的钩子名"
                "会让加载报错。\n"
                "- 不要 import aiohttp 自己发请求；用 p._make_session() / p.send_sse_request()，"
                "出站代理、连接复用、请求留痕才生效。\n"
                "- 渠道地址读 p.base_url（用户在管理端填的「渠道地址」），不要把上游域名写死"
                "在代码里——写死等于骗用户填了不生效。确实不打外网的 spec 写 REQUIRES_BASE_URL = False。\n"
                "- usage 不要自己算：上游没回 token 统计时框架按内容自动估算；只有拿到上游真实"
                "usage 时才覆盖：流式 yield {\"usage\": {...}}，非流式在响应体里放 usage。\n"
                "- 账号字段在 account_schema()[\"fields\"] 声明表单字段；schema 之外的运行时字段"
                "（如 access_token_expires_at_ms）写到类级 ACCOUNT_FIELDS。两者都自动挂成 p.xxx 实例属性。\n"
                "- 运行时刷新出的 token 要落库时用 await p.persist_account_fields({...})（窄写本账号+同步池内实例）；"
                "每日/手动刷新走 refresh_auth 钩子，返回字典由 admin 统一落库，spec 不用自己写。\n"
                "- 类级开关：SUPPORTS_TOKEN_AUTO_REFRESH / SCHEDULED_REFRESH / SUPPORTS_MULTI_MESSAGES / "
                "TOOLS_AS_PROMPT / TOOLS_PROMPT_FORMAT / REQUIRES_BASE_URL / ACCOUNT_FIELDS / APPLY_CLIENT_PRESET。\n\n"
                "产出方式：直接把完整 spec 源码放在代码块里给用户，用户自己复制到「代码」Tab；"
                "不要尝试调用工具写文件或操作渠道配置。先问清上游形态（OpenAI/Anthropic 兼容、"
                "OAuth 设备码、账密换 ticket、非标 SSE 等）再写对应钩子，能不写就不写——"
                "纯兼容接口零钩子即可，只填渠道配置。"
            ),
        )

    return None


def from_chat_settings(settings: dict | None) -> SceneSpec | None:
    """Rebuild the scene from a persisted conversation's ``chat_settings``.

    Continuation turns load the scene from storage rather than from request
    inputs, so a running conversation keeps the same scene across process
    restarts. Reads the keys :func:`persist` writes.
    """
    if not isinstance(settings, dict):
        return None
    job_id = _clean(settings.get("scheduled_job_id"))
    if job_id:
        return normalize(scheduled={
            "job_id": job_id,
            "name": settings.get("scheduled_task"),
            "cron": settings.get("scheduled_schedule"),
        })
    client_id = _clean(settings.get("cdp_client_id"))
    if client_id:
        return normalize(cdp={
            "client_id": client_id,
            "session_key": settings.get("cdp_session_key"),
            "client_alias": settings.get("cdp_client_alias"),
        })
    node_id = _clean(settings.get("node_id"))
    if node_id:
        return normalize(node_id=node_id)
    return normalize(context=settings.get("context"))


def render_prompt(spec: SceneSpec | None) -> str:
    """Render the one-time scene segment appended to a conversation's system prompt.

    Contains only stable facts, so it never needs rewriting on later turns.
    """
    if spec is None:
        return ""
    lines = [f"[场景: {spec.type}]"]
    lines.extend(f"- {key}: {value}" for key, value in spec.identity.items())
    rules = spec.rules.strip()
    if rules:
        lines.append(rules)
    return "\n".join(lines).strip()


def append_prompt(base: str | None, spec: SceneSpec | None) -> str:
    """Append the scene segment to an existing system prompt (either may be empty)."""
    segment = render_prompt(spec)
    base_clean = (base or "").strip()
    if not segment:
        return base_clean
    return f"{base_clean}\n\n{segment}".strip() if base_clean else segment


def replace_prompt(base: str | None, old_spec: SceneSpec | None, new_spec: SceneSpec | None) -> str:
    """Swap an already-written scene segment for a re-resolved one.

    A conversation's scene segment is written once at creation, which assumes its
    identity keys stay valid for the conversation's lifetime. One key does not: a
    Chrome ``tabId`` dies with its tab, so continuing a CDP conversation from a
    different tab would otherwise leave the model driving a closed tab. The
    caller rebuilds ``old_spec`` from the stored ``chat_settings`` and passes the
    current one as ``new_spec``.

    ``render_prompt`` is deterministic, so the old segment is removed by exact
    string match; anything else in ``base`` is preserved. With no old segment to
    remove this is just :func:`append_prompt`.
    """
    old_segment = render_prompt(old_spec)
    remaining = (base or "").strip()
    if old_segment and old_segment in remaining:
        remaining = remaining.replace(old_segment, "", 1).strip()
    return append_prompt(remaining, new_spec)


def persist(spec: SceneSpec | None) -> dict:
    """Return the ``chat_settings`` keys for this scene.

    Key names are the ones already in use (``cdp_client_id`` / ``cdp_session_key``
    / ``cdp_client_alias`` / ``node_id`` / ``context``), so existing rows, admin
    queries and frontend readers keep working unchanged.
    """
    if spec is None:
        return {}
    if spec.type == BROWSER:
        client_id = spec.identity.get("client_id", "")
        tab_id = spec.identity.get("tab_id", "")
        out: dict = {"cdp_client_id": client_id}
        if tab_id:
            out["cdp_session_key"] = f"{client_id}:{tab_id}"
        out["cdp_client_alias"] = spec.identity.get("client_alias") or client_id
        return out
    if spec.type == NODE_TERMINAL:
        return {"node_id": spec.identity.get("node_id", "")}
    if spec.type == SCHEDULED:
        out: dict = {"scheduled_job_id": spec.identity.get("job_id", "")}
        task_name = spec.identity.get("task")
        if task_name:
            out["scheduled_task"] = task_name
        # schedule 也要落：from_chat_settings 会读它重建场景，只写 job_id/task 会让
        # 重建出的 spec 少一行 identity，render_prompt 的精确串匹配就对不上，
        # replace_prompt 会把旧段留在 system_prompt 里再追加一段。
        schedule = spec.identity.get("schedule")
        if schedule:
            out["scheduled_schedule"] = schedule
        return out
    if spec.type == MARKETPLACE_ADMIN:
        return {"context": MARKETPLACE_ADMIN}
    if spec.type == CODE_CHANNEL:
        return {"context": CODE_CHANNEL}
    return {}


__all__ = [
    "BROWSER",
    "NODE_TERMINAL",
    "MARKETPLACE_ADMIN",
    "CODE_CHANNEL",
    "SCHEDULED",
    "SceneSpec",
    "normalize",
    "from_chat_settings",
    "render_prompt",
    "append_prompt",
    "replace_prompt",
    "persist",
]
