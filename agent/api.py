from __future__ import annotations

import asyncio
import base64
import json
import mimetypes
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Literal, TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Depends, Header, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field

from agent.config import AgentConfig
from agent.context_manager import rebuild_history_messages, attach_tool_result, _collect_attachment_media
from agent.memory import MemorySystem
from agent.tools import ToolContext, ToolRegistry
from agent import conversation_store, scene_context
# 审批超时中止本轮的信号异常（仅 stdlib 依赖，模块级导入无循环风险）。
from user_platform.node_client.approvals import ApprovalDenied, ApprovalTimeout

if TYPE_CHECKING:
    from agent.agent_main import GenericAgent


router = APIRouter(prefix="/agent", tags=["agent"])

_REASONING_EFFORTS = {"", "low", "medium", "high", "xhigh"}

# 审批等待时长的可配置区间。0 = 不过期（一直等人裁决）；下限 60s 是为了避免
# 配出「卡片还没看到就过期」的值；上限 7 天纯属兜底——审批挂起会钉住 asyncio
# task + SSE 连接 + 绑定的节点 PTY，且注册表是纯内存的，重启即丢。
_APPROVAL_TIMEOUT_MIN_SECONDS = 60
_APPROVAL_TIMEOUT_MAX_SECONDS = 7 * 24 * 60 * 60


def _validate_approval_timeout(value: int | None) -> None:
    """校验 approval_timeout_seconds：0 或 [60, 7 天]，越界抛 400。"""
    if value is None:
        return
    seconds = int(value)
    if seconds == 0:
        return
    if seconds < _APPROVAL_TIMEOUT_MIN_SECONDS or seconds > _APPROVAL_TIMEOUT_MAX_SECONDS:
        raise HTTPException(
            status_code=400,
            detail="approval_timeout_seconds 必须为 0（不过期）或 60 秒到 7 天之间",
        )


def _normalize_reasoning_effort(value: str | None) -> str:
    effort = (value or "").strip().lower()
    if effort == "max":
        effort = "xhigh"
    if effort not in _REASONING_EFFORTS:
        raise HTTPException(status_code=400, detail="reasoning_effort 必须为 low/medium/high/xhigh 或空")
    return effort

# 附件上传上限与「按文本发给模型」的 mime 白名单。
_ATTACHMENT_LIMIT = 10 << 20  # 10 MiB
_TEXTUAL_MIMES = {
    "application/json",
    "application/xml",
    "application/javascript",
    "application/x-yaml",
    "application/yaml",
    "application/toml",
}


async def _read_attachment_body(request: Request) -> bytes:
    """读取原始附件字节，超限即 413，空则 400。"""
    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="附件内容为空")
    if len(raw) > _ATTACHMENT_LIMIT:
        raise HTTPException(status_code=413, detail="附件不能超过 10 MiB")
    return raw


def _attachment_target(root: str, conv_id: str, filename: str) -> tuple[str, Path]:
    """把 root/.chat-attachments/<conv_id>/<uuid>-<safe> 解析成 (相对路径, 绝对路径)。

    剥目录 + 白名单字符 + uuid 前缀，并校验最终路径落在 root 内（防穿越）。
    """
    safe_name = Path(filename.replace("\\", "/")).name
    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in safe_name)
    if safe_name in ("", ".", ".."):
        safe_name = "attachment.bin"
    safe_conv = "".join(ch for ch in conv_id if ch.isalnum() or ch in ("-", "_")) or "conv"
    relative = f".chat-attachments/{safe_conv}/{uuid.uuid4().hex}-{safe_name}"
    base = Path(root).resolve()
    target = (base / relative).resolve()
    if not target.is_relative_to(base):
        raise HTTPException(status_code=400, detail="非法的附件路径")
    target.parent.mkdir(parents=True, exist_ok=True)
    return relative, target


# ── Agent → 用户 文件/图片展示（outbound media）──────────────────────────
# _media_part_from_result / _collect_attachment_media 已移到 agent.context_manager，
# 与定时任务路径（agent/scheduler.py 的 on_event）共用，避免两边各抄一份漂移。
# 这里直接 import 使用，不再重复定义。


# ── 调用者鉴权 ────────────────────────────────────────────────────────
# 一套 /agent/* 接口同时服务两类调用者：
#   - C 端用户（user_platform session cookie）：get_agent_caller 返回其 user_id(str)，
#     数据按 user_id 隔离（只看/管自己的 + 平台公共 Agent）。
#   - 管理员（/admin/login 换取的 Bearer token）：返回 None，不过滤，保持旧行为。
# 两者都拿不到 → 401。

async def _resolve_user_from_session() -> str | None:
    """从 user_platform C 端 session cookie 解析当前用户 id；无则 None。"""
    try:
        from admin import _current_user_cookie
        from user_platform.session import session_store, USER_SESSION_COOKIE
        cookie = _current_user_cookie.get(None)
        if not cookie:
            return None
        data = await session_store.get(USER_SESSION_COOKIE, cookie)
        return data.uid if data and data.uid else None
    except Exception:
        return None


async def _is_valid_admin(authorization: str | None) -> bool:
    """校验应急管理员 Bearer token（复用 admin session 存储）。"""
    if not authorization or not authorization.startswith("Bearer "):
        return False
    try:
        import time
        from admin import _get_admin_session
        session = await _get_admin_session(authorization[7:])
        return bool(session and time.time() < session["expires_at"])
    except Exception:
        return False


async def get_agent_caller(authorization: str | None = Header(None)) -> str | None:
    """返回 user_id(str)=用户模式；None=管理员模式。两者皆无 → 401。"""
    uid = await _resolve_user_from_session()
    if uid:
        return uid
    if await _is_valid_admin(authorization):
        return None
    raise HTTPException(status_code=401, detail="未登录")


async def _resolve_agent_owner(caller: str | None) -> str:
    """create_agent 的 owner 落库值。

    caller 为 uid（C 端用户或经 C 端 session 登录的管理员）时直接用它；caller=None
    （仅应急管理员 Bearer）时解析到一个真实 admin user id 作为 owner，避免落库
    user_id=NULL 变成全员可见的平台 Agent。团队共享改由显式 is_team_shared 表达。
    """
    if caller is not None:
        return caller
    from user_platform.deps import _ensure_emergency_admin
    admin = await _ensure_emergency_admin()
    return str(admin.id)


async def _resolve_team_id_safe(user_id: str) -> str | None:
    """解析用户所属 team；失败返回 None（不阻断建 agent）。"""
    try:
        from user_platform.deps import resolve_team_id
        return await resolve_team_id(user_id)
    except Exception:
        return None


def _require_ch() -> None:
    """对话存储依赖 ClickHouse；未启用 -> 503 明确错误，不进 handler 抛 500。"""
    if not conversation_store.is_ready():
        raise HTTPException(
            status_code=503,
            detail="ClickHouse 未启用，Agent/聊天对话存储不可用",
        )

TaskStatus = Literal["queued", "running", "completed", "failed", "aborted"]


class AgentTaskRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    api_key: str | None = None
    model: str | None = None
    max_turns: int | None = Field(default=None, ge=1)
    workspace_root: str | None = None
    allowed_roots: list[str] | None = None
    denied_patterns: list[str] | None = None
    system_prompt: str = ""


class AgentTaskResponse(BaseModel):
    task_id: str
    status: TaskStatus
    result: list[dict[str, Any]] | None = None
    error: str | None = None


class AgentStatusResponse(BaseModel):
    tasks: dict[str, int]


class ToolsSchemaResponse(BaseModel):
    tools: list[dict[str, Any]]


class SopCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str = ""
    content: str = ""


class SopUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    content: str | None = None
    enabled: bool | None = None
    expected_revision: int | None = None


class MemoryResponse(BaseModel):
    l1_insights: list[dict[str, Any]]
    l2_facts: list[dict[str, Any]]
    l4_archive: list[dict[str, Any]]


_TASKS: dict[str, AgentTaskResponse] = {}
_TASK_HANDLES: dict[str, asyncio.Task] = {}
_AGENT_CONFIG = AgentConfig()


def _build_tool_registry(agent_id: int | None = None, owner_user_id: str | None = None) -> ToolRegistry:
    """Build the fixed GA tool registry (GA atomic tools + capability_call).

    owner_user_id 传当前发起者（C 端 uid）：capability_call 建定时任务时用它落归属，
    否则任务 user_id 为 NULL、用户态列表看不到。
    """
    return ToolRegistry(ToolContext(AgentConfig()), agent_id=agent_id, owner_user_id=owner_user_id)


@router.post("/task", response_model=AgentTaskResponse)
async def create_task(request: AgentTaskRequest) -> AgentTaskResponse:
    task_id = uuid.uuid4().hex
    task_state = AgentTaskResponse(task_id=task_id, status="queued")
    _TASKS[task_id] = task_state
    _TASK_HANDLES[task_id] = asyncio.create_task(_run_agent_task(task_id, request))
    return task_state


@router.get("/task/{task_id}", response_model=AgentTaskResponse)
async def get_task(task_id: str) -> AgentTaskResponse:
    task_state = _TASKS.get(task_id)
    if task_state is None:
        raise HTTPException(status_code=404, detail="task not found")
    return task_state


@router.post("/task/{task_id}/abort", response_model=AgentTaskResponse)
async def abort_task(task_id: str) -> AgentTaskResponse:
    task_state = _TASKS.get(task_id)
    if task_state is None:
        raise HTTPException(status_code=404, detail="task not found")

    handle = _TASK_HANDLES.get(task_id)
    if handle is not None and not handle.done():
        handle.cancel()

    if task_state.status in {"queued", "running"}:
        task_state.status = "aborted"
        task_state.result = None
        task_state.error = "task aborted"
    return task_state


@router.get("/status", response_model=AgentStatusResponse)
async def get_status() -> AgentStatusResponse:
    counts = {status: 0 for status in ("queued", "running", "completed", "failed", "aborted")}
    for task in _TASKS.values():
        counts[task.status] += 1
    counts["total"] = len(_TASKS)
    counts["active"] = counts["queued"] + counts["running"]
    ordered = {
        "total": counts["total"],
        "queued": counts["queued"],
        "running": counts["running"],
        "active": counts["active"],
        "completed": counts["completed"],
        "failed": counts["failed"],
        "aborted": counts["aborted"],
    }
    return AgentStatusResponse(tasks=ordered)


@router.get("/tools/schema", response_model=ToolsSchemaResponse)
async def get_tools_schema() -> ToolsSchemaResponse:
    return ToolsSchemaResponse(tools=_build_tool_registry().get_schema())


@router.get("/sops")
async def list_sops(caller: str | None = Depends(get_agent_caller)) -> dict:
    from agent.sop_service import get_content, list_catalog

    rows = await list_catalog(include_disabled=True)
    for row in rows:
        try:
            row["content"] = await get_content(row["id"])
        except FileNotFoundError:
            row["content"] = ""
    return {"sops": rows}


@router.post("/sops")
async def create_sop(request: SopCreateRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    if caller is not None:
        raise HTTPException(403, "SOP 资源管理仅管理员可用")
    from agent.sop_service import create_sop as create

    return await create(request.name, request.description, request.content)


@router.patch("/sops/{sop_id}")
async def update_sop(sop_id: str, request: SopUpdateRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    if caller is not None:
        raise HTTPException(403, "SOP 资源管理仅管理员可用")
    from agent.sop_service import update_sop as update

    try:
        return await update(sop_id, request.model_dump(exclude_unset=True))
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (PermissionError, ValueError) as exc:
        raise HTTPException(409 if "revision" in str(exc).lower() else 400, str(exc)) from exc


@router.delete("/sops/{sop_id}")
async def delete_sop(sop_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    if caller is not None:
        raise HTTPException(403, "SOP 资源管理仅管理员可用")
    from agent.sop_service import delete_sop as remove

    try:
        deleted = await remove(sop_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if not deleted:
        raise HTTPException(404, "SOP not found")
    return {"status": "ok"}


@router.get("/skills")
async def get_skills(category: str | None = None, limit: int = 100) -> dict:
    """Deprecated legacy endpoint; the platform Skill catalog was removed."""
    return {"skills": []}


@router.get("/memory", response_model=MemoryResponse)
async def get_memory(limit: int = 50) -> MemoryResponse:
    """Compatibility diagnostic view over GA files plus legacy L4 archive."""
    from agent import file_memory as fm

    memory = MemorySystem()
    l4_archive = await memory.get_l4_archive(limit=min(limit, 50))
    return MemoryResponse(
        l1_insights=[{"content": ""}],
        l2_facts=[{"content": ""}],
        l4_archive=l4_archive,
    )


@router.get("/config")
async def get_config() -> dict[str, Any]:
    return _AGENT_CONFIG.to_dict()


@router.post("/config")
async def update_config(config_update: dict[str, Any]) -> dict[str, Any]:
    global _AGENT_CONFIG
    merged = _AGENT_CONFIG.to_dict()
    merged.update(config_update)
    _AGENT_CONFIG = AgentConfig.from_dict(merged)
    return _AGENT_CONFIG.to_dict()


# ---------------------------------------------------------------------------
# Agent CRUD endpoints
# ---------------------------------------------------------------------------

class CreateAgentRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    display_name: str | None = None
    description: str = ""
    system_prompt: str = ""
    max_turns: int = 80
    enabled: bool = True
    memory_enabled: bool = True
    skill_auto_learn: bool = True
    scheduler_enabled: bool = False
    # Empty/legacy default → runtime computes the GA-style per-Agent dir
    # ``data/agents/<id>/``. Set explicitly only for node-bound workspaces.
    workspace_root: str = ""
    allowed_roots: list[str] = []
    denied_patterns: list[str] = ["/etc/", "/var/", ".env", ".git/", "node_modules/"]
    guardian_enabled: bool = False
    guardian_interval: int = 300
    autonomous_enabled: bool = False
    thinking_enabled: bool = False
    reasoning_effort: str = ""
    # agent 层 429 自动重试次数（仅瞬时码；0=关闭）。见 AgentConfig.llm_retry_429。
    llm_retry_429: int = 2
    # 需确认工具挂起等人裁决的上限（秒）；0=不过期。见 AgentConfig.approval_timeout_seconds。
    approval_timeout_seconds: int = 24 * 60 * 60
    # 浏览器（CDP 网页对话）能否执行 code_run。默认关：CDP 路径不挂审批协调器，
    # code_run 直接 denied 并提示开启本配置。用户端聊天页不受影响（照旧走审批）。
    browser_code_run_enabled: bool = False
    # 网关秘钥绑定：agent LLM 走网关 dispatch_entry（计费/限流/归属闭环）。
    # 主 Agent 必须选（网关 api_keys.id + 模型名）；子 Agent 可空（跟随主）。
    main_api_key_id: int | None = None
    main_model: str = ""
    subagent_api_key_id: int | None = None
    subagent_model: str = ""
    # 定时任务默认 key/模型：无人值守跑批常要换个更便宜/更稳的模型，且不该借用
    # subagent_*（那是「主 agent 派生的并行子任务」）。留空则回退主 Agent。
    scheduled_api_key_id: int | None = None
    scheduled_model: str = ""
    mcp_user_id: int | None = None
    # 团队共享：true 时同 team 成员可见可用；false 仅 owner 可见。
    is_team_shared: bool = False


class UpdateAgentRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    system_prompt: str | None = None
    max_turns: int | None = None
    enabled: bool | None = None
    memory_enabled: bool | None = None
    skill_auto_learn: bool | None = None
    scheduler_enabled: bool | None = None
    workspace_root: str | None = None
    allowed_roots: list[str] | None = None
    denied_patterns: list[str] | None = None
    guardian_enabled: bool | None = None
    guardian_interval: int | None = None
    autonomous_enabled: bool | None = None
    thinking_enabled: bool | None = None
    reasoning_effort: str | None = None
    llm_retry_429: int | None = None
    approval_timeout_seconds: int | None = None
    browser_code_run_enabled: bool | None = None
    main_api_key_id: int | None = None
    main_model: str | None = None
    subagent_api_key_id: int | None = None
    subagent_model: str | None = None
    scheduled_api_key_id: int | None = None
    scheduled_model: str | None = None
    mcp_user_id: int | None = None
    is_team_shared: bool | None = None


async def _validate_agent_gateway_key(api_key_id: int, caller: str | None) -> None:
    """校验 api_key_id 对该 Agent owner 可用（自有/分组系统 key）；不可用 → 抛 HTTP。

    复用聊天发送链路的解析器：个人 Agent（caller=用户）只能绑自己名下或所在分组
    授权的网关 key；平台 Agent（caller=None，管理员建）走全量查找。绑定不可用的 key
    会让 Agent 运行时报错，故在写库前就挡掉。
    """
    await _resolve_caller_api_key(api_key_id, caller)


async def _create_agent_principal_for(owner: str | None, name: str) -> int:
    """按 Agent 归属建 usage_type=agent 的 principal：平台 Agent 建平台级（owner=NULL），
    用户 Agent 建 user-owned。建 Agent 与启动对账共用这一条路径。"""
    import mcp_plugin_store

    if owner is None:
        principal = await mcp_plugin_store.create_platform_mcp_principal(
            f"agent-{name}",
            description=f"agent principal for {name}",
            enabled=True,
            chat_enabled=False,
            usage_type="agent",
        )
        return int(principal["id"])
    return await mcp_plugin_store.create_agent_mcp_principal(
        str(owner), f"agent-{name}", description=f"agent principal for {name}",
    )


async def reconcile_agent_mcp_principals() -> dict[str, int]:
    """启动时幂等对账：每个 Agent 都应绑定一条 usage_type='agent' 的 principal。

    需要补建/改绑的情形（含历史数据）：
      - mcp_user_id 为 NULL（老 Agent 从未建过）；
      - 指向的 principal 不存在（被删）；
      - 指向的 principal 存在但 usage_type != 'agent'（早期绑了 external principal）。

    错绑的旧 principal 在改绑后若没有别处引用，就地删除——避免留孤儿 principal
    占位且继续出现在用户侧 MCP 用户列表里。并发/重复执行安全：新建后用条件 UPDATE
    落绑，仅在当前 old 值未被并发改写时才删旧 principal。
    """
    from db import PostgresClient
    import mcp_plugin_store

    created = rebound = removed = kept = 0
    if not PostgresClient.pool:
        return {"agents": 0, "created": 0, "rebound": 0, "removed": 0, "already_ok": 0}

    async with PostgresClient.pool.acquire() as conn:
        agents = await conn.fetch("SELECT id, name, user_id, mcp_user_id FROM agents ORDER BY id")
    for row in agents:
        agent_id = int(row["id"])
        current = row["mcp_user_id"]
        ok = False
        if current is not None:
            bound = await mcp_plugin_store.get_mcp_principal(int(current))
            ok = bound is not None and bound.get("usage_type") == "agent"
        if ok:
            kept += 1
            continue
        new_id = await _create_agent_principal_for(row["user_id"], str(row["name"]))
        async with PostgresClient.pool.acquire() as conn:
            bound_id = await conn.fetchval(
                """
                UPDATE agents SET mcp_user_id=$2, updated_at=now()
                WHERE id=$1 AND (mcp_user_id IS NULL OR mcp_user_id=$3)
                RETURNING mcp_user_id
                """,
                agent_id, new_id, current,
            )
        if bound_id is None:
            # 并发下别的写入赢了（如用户同时在建 Agent）：丢弃本次新建，避免孤儿。
            await mcp_plugin_store.delete_mcp_principal(new_id)
            continue
        created += 1
        if current is not None and current != new_id:
            rebound += 1
            # 旧 principal 仅当无任何 agent 引用、且没有外部接入授权时才删——
            # 避免误删用户正在用的 external 身份（有 mcp_grants 授权说明在用）。
            async with PostgresClient.pool.acquire() as conn:
                in_use = await conn.fetchval(
                    "SELECT 1 FROM agents WHERE mcp_user_id=$1 AND id<>$2 LIMIT 1",
                    int(current), agent_id,
                )
            if in_use is None:
                granted = await mcp_plugin_store.list_services_for_mcp_user(int(current))
                if not granted:
                    await mcp_plugin_store.delete_mcp_principal(int(current))
                    removed += 1
    summary = {"agents": len(agents), "created": created, "rebound": rebound,
               "removed": removed, "already_ok": kept}
    if created:
        logger.info("[agent] startup principal reconcile: {}", summary)
    return summary


@router.post("/agents")
async def create_agent(request: CreateAgentRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    # 主 Agent 必须绑定可用网关 key + 模型（LLM 走网关，形成计费/限流/归属闭环）。
    if not request.main_api_key_id:
        raise HTTPException(400, "请选择主 Agent 的网关 API Key")
    if not (request.main_model or "").strip():
        raise HTTPException(400, "请选择主 Agent 的模型")
    await _validate_agent_gateway_key(request.main_api_key_id, caller)
    if request.subagent_api_key_id:
        await _validate_agent_gateway_key(request.subagent_api_key_id, caller)
    if request.scheduled_api_key_id:
        await _validate_agent_gateway_key(request.scheduled_api_key_id, caller)
    _validate_approval_timeout(request.approval_timeout_seconds)
    # mcp_user_id 创建时由后端自动建一个 usage_type=agent 的 principal 并绑定，
    # 不再接受请求传入（用户在 Agent 详情页配置该 principal 的权限）。
    # 平台 Agent（caller=None）建平台级 principal（owner=NULL）。
    auto_principal_id: int | None = None
    try:
        auto_principal_id = await _create_agent_principal_for(caller, request.name)
    except Exception as exc:  # noqa: BLE001
        # MCP 配置主体是 Agent MCP 管理的必要组成，创建失败不能留下一个永远空白、
        # 无法管理 MCP 的 Agent。
        logger.exception("[agent] auto-create mcp principal failed for %s", request.name)
        raise HTTPException(500, f"初始化 Agent MCP 配置失败: {exc}") from exc
    # owner 永远是真实 uid（caller 为 uid 时用它；应急 Bearer 管理员解析真实 admin），
    # 不再写 user_id NULL（NULL 曾被当作平台 Agent 全员共享，与多用户隔离冲突）。
    # 团队共享由显式 is_team_shared + team_id 表达。
    owner_uid = await _resolve_agent_owner(caller)
    team_id = await _resolve_team_id_safe(owner_uid)
    async with PostgresClient.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO agents(
                    name, display_name, description, api_key, model, system_prompt, max_turns,
                    enabled, memory_enabled, skill_auto_learn, scheduler_enabled,
                    workspace_root, allowed_roots, denied_patterns,
                    guardian_enabled, guardian_interval, autonomous_enabled,
                    thinking_enabled, reasoning_effort, llm_retry_429, mcp_user_id, user_id,
                    main_api_key_id, main_model, subagent_api_key_id, subagent_model,
                    approval_timeout_seconds, team_id, is_team_shared,
                    scheduled_api_key_id, scheduled_model, browser_code_run_enabled
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)
                RETURNING *
                """,
                request.name, request.display_name or request.name, request.description,
                "", request.main_model, request.system_prompt, request.max_turns,
                request.enabled, request.memory_enabled, request.skill_auto_learn,
                request.scheduler_enabled,
                request.workspace_root, request.allowed_roots, request.denied_patterns,
                request.guardian_enabled, request.guardian_interval, request.autonomous_enabled,
                request.thinking_enabled, request.reasoning_effort, request.llm_retry_429,
                auto_principal_id, owner_uid,
                request.main_api_key_id, request.main_model,
                request.subagent_api_key_id, request.subagent_model or "",
                request.approval_timeout_seconds,
                team_id, request.is_team_shared,
                request.scheduled_api_key_id, request.scheduled_model or "",
                request.browser_code_run_enabled,
            )
        except Exception as e:
            # Agent 落库失败：回滚刚建的 principal，避免孤儿 principal。
            if auto_principal_id is not None:
                try:
                    import mcp_plugin_store as _store
                    await _store.delete_mcp_principal(auto_principal_id)
                except Exception:  # noqa: BLE001
                    pass
            raise HTTPException(500, str(e))
    agent_id = row["id"]
    result = await _agent_row_to_dict(row)
    # GA memory is Agent-private and file-backed. Initialise it immediately so
    # the first conversation has L1/L2 files and built-in SOP pointers even
    # before SOP selection.
    try:
        from agent.file_memory import ensure_agent_memory
        ensure_agent_memory(agent_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[memory] initialize Agent files failed agent=%s: %s", agent_id, exc)
    # 为 agent 签发一个身份 token，作为其启动时连内部 MCP runtime 的环境变量。
    # 明文只此一次返回（token 表只存哈希）；失败不阻断建 agent。
    try:
        import builtin_tool_store
        _token_row, mcp_token = await builtin_tool_store.issue_token(
            "agent", str(agent_id), display_token=False,
        )
        result["mcp_token"] = mcp_token
    except Exception as exc:  # noqa: BLE001
        logger.warning("[mcp] issue agent token failed agent=%s: %s", agent_id, exc)
    return result


@router.get("/agents")
async def list_agents(caller: str | None = Depends(get_agent_caller)) -> list[dict]:
    from db import PostgresClient
    if not PostgresClient.pool:
        return []
    # caller=None（管理员）看全部；caller=用户 看自己创建的 + 团队共享的 Agent。
    team_id = await _resolve_team_id_safe(caller) if caller else None
    rows = await PostgresClient.list_agents_for_caller(caller, team_id)
    return [await _agent_row_to_dict(row) for row in rows]


@router.get("/agents/{agent_id}")
async def get_agent(agent_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    if not row:
        raise HTTPException(404, f"Agent {agent_id} not found")
    # 可见性：自己创建的 + 团队共享（is_team_shared 且 team_id 匹配调用者团队）；
    # 看不到别人的私有 Agent。改/删权限在 update/delete 里按 owner 校验。
    await _require_agent_access(agent_id, caller)
    result = await _agent_row_to_dict(row)
    # Add stats
    async with PostgresClient.pool.acquire() as conn:
        result["stats"] = {
            "conversations": await conn.fetchval(
                "SELECT COUNT(*) FROM agent_conversations WHERE agent_id=$1", agent_id
            ) or 0,
            "insights": 0,
            "facts": 0,
            "sops": 0,
            # L3 learned-skill text count (agent.memory legacy table), matching
            # the "L3 技能" label in the agent manager UI.
            "skills": await conn.fetchval(
                "SELECT COUNT(*) FROM agent_skills WHERE agent_id=$1", agent_id
            ) or 0,
            "scheduled_tasks": await conn.fetchval(
                "SELECT COUNT(*) FROM agent_scheduled_tasks WHERE agent_id=$1", agent_id
            ) or 0,
        }
    try:
        from agent.sop_service import list_for_agent

        result["stats"]["sops"] = len((await list_for_agent(agent_id)).get("selected_ids", []))
    except Exception as exc:  # noqa: BLE001
        logger.warning("[agent] SOP stats unavailable agent=%s: %s", agent_id, exc)
    return result


@router.get("/agents/{agent_id}/skills")
async def get_agent_skills(agent_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """Deprecated: the platform Skill catalog was removed; agents use SOPs."""
    return {"skills": []}


@router.get("/agents/{agent_id}/sops")
async def get_agent_sops(agent_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """Read-only view of the SOPs in effect for one Agent.

    Effective SOPs = built-in catalog SOPs (auto-effective for every Agent) +
    custom catalog SOPs explicitly bound to this Agent + Agent-private distilled
    SOPs under ``memory/sop/``. Per-Agent binding is a backend capability; the
    resource page's install interaction is not wired yet.
    """
    from db import PostgresClient
    from agent.sop_service import list_for_agent

    row = await PostgresClient.get_agent(agent_id)
    if not row:
        raise HTTPException(404, f"Agent {agent_id} not found")
    owner = row.get("user_id")
    if caller is not None and owner is not None and str(owner) != str(caller):
        raise HTTPException(404, f"Agent {agent_id} not found")
    return await list_for_agent(agent_id)


@router.get("/agents/by-mcp-user/{principal_id}")
async def get_agent_by_mcp_user(principal_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """反查 principal → Agent：返回绑定该 mcp_user_id 的 agent_id（无则 null）。

    供 MCP 用户管理弹框「进入 Agent 详情」使用。ownership：用户只能反查自己的 Agent；
    管理员可查平台 Agent。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, user_id FROM agents WHERE mcp_user_id=$1", int(principal_id))
    if not row:
        return {"agent_id": None}
    # 归属/共享校验：owner 或同团队共享可反查；否则视作不存在（不泄露 agent 存在性）。
    try:
        await _require_agent_access(int(row["id"]), caller)
    except HTTPException:
        return {"agent_id": None}
    return {"agent_id": int(row["id"])}


@router.get("/agents/{agent_id}/mcp-diagnostics")
async def get_agent_mcp_diagnostics(agent_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """会话诊断：这个 Agent 实际生效的 MCP 服务 + 绑定的 principal + params + token 类型。

    供 AgentConversation 右上角「查看详情」弹框用——回答用户「这个会话到底挂了哪些
    MCP 工具、以什么身份调用」。只读，不含明文 token。
    """
    from db import PostgresClient

    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    if not row:
        raise HTTPException(404, f"Agent {agent_id} not found")
    # 可见性：同 get_agent——owner 或团队共享成员可见；别人的私有 Agent → 404。
    await _require_agent_access(agent_id, caller)

    import mcp_plugin_store
    import builtin_tool_store
    from agent.mcp_client import resolve_effective_services

    agent_d = await _agent_row_to_dict(row)

    # principal 信息 + params（params 含 param_key→param_value，param_value 是资源 id）。
    principal_id = agent_d.get("mcp_user_id")
    principal: dict | None = None
    params: list[dict] = []
    if principal_id is not None:
        principal = await mcp_plugin_store.get_mcp_user(int(principal_id), mask_token=True)
        try:
            params = await mcp_plugin_store.list_principal_params(int(principal_id))
        except Exception as exc:  # noqa: BLE001
            logger.warning("[agent] diagnostics list_principal_params failed: %s", exc)
            params = []
    param_by_key = {p["param_key"]: p["param_value"] for p in params if p.get("param_key")}

    # 有效服务完全由 principal 授权推导（agent 不再自带 mcp_servers）。
    try:
        effective = await resolve_effective_services(principal_id)
    except Exception as exc:  # noqa: BLE001 — 解析失败不阻断，返回空 + 错误原因
        logger.warning("[agent] diagnostics resolve_effective_services failed: %s", exc)
        effective = []

    # 内置服务 → 该服务鉴权用的 param_key。单一来源在 sse_gateway（鉴权热路径），
    # 这里导入复用，避免与网关判权用的映射漂移。
    from mcp_runtime.sse_gateway import _BUILTIN_SERVICE_PARAM_KEY as builtin_param_key

    services: list[dict] = []
    for svc in effective:
        svc_id = svc.get("id")
        name = svc.get("name")
        tools_cache = svc.get("tools_cache") or []
        try:
            auth = await mcp_plugin_store.get_service_auth(int(svc_id)) if svc_id is not None else None
        except Exception:
            auth = None
        # 该 principal 在本服务下能操控的资源（按 param 绑定的 builtin_tool_resources）。
        resources: list[dict] = []
        pkey = builtin_param_key.get(name or "")
        if pkey:
            bound_value = param_by_key.get(pkey)
            if bound_value and bound_value.isdigit():
                try:
                    resource = await builtin_tool_store.get_resource(int(bound_value))
                except Exception:
                    resource = None
                if resource:
                    resources.append({
                        "id": int(resource["id"]),
                        "kind": resource.get("resource_type"),
                        "name": resource.get("name") or resource.get("display_name") or str(resource["id"]),
                        "enabled": bool(resource.get("enabled", True)),
                    })
        services.append({
            "id": svc_id, "name": name,
            "display_name": svc.get("display_name") or name,
            "kind": svc.get("kind"),
            "builtin": bool(svc.get("builtin")),
            "enabled": bool(svc.get("enabled")),
            "auth_enabled": bool(auth and auth.get("auth_enabled")),
            # 本服务能操控的资源（cdp→浏览器客户端、mail→邮箱账户）；无则空。
            "param_key": pkey,
            "resources": resources,
            # 工具列表：tools_cache 里的 name + description（不回传 input_schema 全量，太长）。
            # 备用——按需求方法在工具处展示，这里仍返回供需要时取用。
            "tools": [
                {"name": t.get("name"), "description": t.get("description") or ""}
                for t in tools_cache if isinstance(t, dict) and t.get("name")
            ] if isinstance(tools_cache, list) else [],
        })

    return {
        "agent_id": int(agent_id),
        "agent_name": agent_d.get("display_name") or agent_d.get("name"),
        "agent_owner_user_id": agent_d.get("user_id"),
        "mcp_user_id": principal_id,
        "principal": principal,
        "params": params,
        # 本次会话调用 MCP 的身份类型：绑了 principal→identity token；否则走 service token。
        "token_kind": "identity" if principal_id is not None else "service",
        "effective_services": services,
    }


@router.patch("/agents/{agent_id}")
async def update_agent(agent_id: int, request: UpdateAgentRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")

    # 编辑权限：仅 owner 可改自己的 Agent；不再有「平台 Agent 任意人可改」的口径
    # （user_id IS NULL 的历史行已迁移归属）。删除权限在 delete_agent 里同样按 owner。
    if caller is not None:
        owned = await PostgresClient.get_agent_owned(caller, agent_id)
        if owned is None:
            raise HTTPException(404, f"Agent {agent_id} not found")

    # Build dynamic SET clause from non-None fields
    updates = []
    params: list[Any] = [agent_id]
    param_idx = 2

    # 网关秘钥绑定：显式出现在请求里才更新。写库前校验 key 对该 Agent owner 可用。
    # owner=None 表示平台 Agent（走管理员全量查找）；否则用其 owner 校验归属。
    owner = None if caller is None else caller
    if request.main_api_key_id is not None:
        await _validate_agent_gateway_key(request.main_api_key_id, owner)
    if request.subagent_api_key_id is not None:
        await _validate_agent_gateway_key(request.subagent_api_key_id, owner)
    if request.scheduled_api_key_id is not None:
        await _validate_agent_gateway_key(request.scheduled_api_key_id, owner)
    # MCP 服务授权由绑定 principal 管理，Agent 本身不再保存服务清单。
    # mcp_user_id 由建 Agent 时后端自动绑定，详情页不允许换绑——用户通过通用权限
    # 编辑器配置该已绑定 principal 的权限，而非改绑别的 principal。
    # （request.mcp_user_id 即使传入也忽略。）
    _validate_approval_timeout(request.approval_timeout_seconds)

    field_map = {
        "display_name": str, "description": str,
        "system_prompt": str, "max_turns": int, "enabled": bool, "memory_enabled": bool,
        "skill_auto_learn": bool, "scheduler_enabled": bool, "workspace_root": str,
        "allowed_roots": list, "denied_patterns": list, "guardian_enabled": bool,
        "guardian_interval": int, "autonomous_enabled": bool,
        "thinking_enabled": bool, "reasoning_effort": str, "llm_retry_429": int,
        "approval_timeout_seconds": int, "browser_code_run_enabled": bool,
        "main_model": str, "subagent_model": str, "scheduled_model": str,
    }
    for field_name, field_type in field_map.items():
        value = getattr(request, field_name, None)
        if value is not None:
            updates.append(f"{field_name}=${param_idx}")
            params.append(value)
            param_idx += 1

    # 网关 key id：显式出现在请求里才更新（允许传 null 解绑，子 Agent 回退到跟随主）。
    for key_field in ("main_api_key_id", "subagent_api_key_id", "scheduled_api_key_id"):
        if key_field in request.model_fields_set:
            updates.append(f"{key_field}=${param_idx}")
            params.append(getattr(request, key_field))
            param_idx += 1

    # 团队共享开关：仅 owner 可切换（owner 校验已在上面通过）。
    if request.is_team_shared is not None:
        updates.append(f"is_team_shared=${param_idx}")
        params.append(bool(request.is_team_shared))
        param_idx += 1

    # mcp_user_id 不再支持 update 改绑（建 Agent 时自动绑定，详情页只配权限）。

    if not updates:
        raise HTTPException(400, "No fields to update")

    updates.append("updated_at=now()")
    set_clause = ", ".join(updates)
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE agents SET {set_clause} WHERE id=$1 RETURNING *", *params
        )
    if not row:
        raise HTTPException(404, f"Agent {agent_id} not found")
    return await _agent_row_to_dict(row)


@router.delete("/agents/{agent_id}")
async def delete_agent(agent_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    # 用户模式：只能删自己创建的 Agent。
    if caller is not None:
        if await PostgresClient.get_agent_owned(caller, agent_id) is None:
            raise HTTPException(404, f"Agent {agent_id} not found")
    async with PostgresClient.pool.acquire() as conn:
        result = await conn.execute("DELETE FROM agents WHERE id=$1", agent_id)
    if result.endswith("0"):
        raise HTTPException(404, f"Agent {agent_id} not found")
    try:
        from user_platform.models_skill import AgentSopBinding

        await AgentSopBinding.filter(agent_id=agent_id).delete()
    except Exception as exc:  # noqa: BLE001 - main Agent deletion remains authoritative
        logger.warning("[agent] delete SOP bindings failed agent=%s: %s", agent_id, exc)
    return {"ok": True, "msg": f"Agent {agent_id} deleted"}


@router.post("/agents/{agent_id}/toggle")
async def toggle_agent(agent_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    # 用户模式：只能切换自己创建的 Agent。
    if caller is not None and await PostgresClient.get_agent_owned(caller, agent_id) is None:
        raise HTTPException(404, f"Agent {agent_id} not found")
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE agents SET enabled = NOT enabled, updated_at=now() "
            "WHERE id=$1 RETURNING id, enabled", agent_id
        )
    if not row:
        raise HTTPException(404, f"Agent {agent_id} not found")
    return {"id": row["id"], "enabled": row["enabled"]}


@router.get("/agents/{agent_id}/memory")
async def get_agent_memory(
    agent_id: int, limit: int = 50, caller: str | None = Depends(get_agent_caller)
) -> MemoryResponse:
    from db import PostgresClient
    # 记忆是 Agent 私有数据：仅 owner（或同团队共享 agent 的成员）可读。
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    await _require_agent_access(agent_id, caller)
    from agent import file_memory as fm

    fm.ensure_agent_memory(agent_id)
    memory = MemorySystem(agent_id=agent_id)
    l4 = await memory.get_l4_archive(limit=limit)
    return MemoryResponse(
        l1_insights=[{"content": fm.read_l1(agent_id)}],
        l2_facts=[{"content": fm.read_l2(agent_id)}],
        l4_archive=l4,
    )


async def _agent_row_to_dict(row) -> dict:
    """Convert an agent DB row to a JSON-serializable dict."""
    d = dict(row)
    # Serialize datetime fields
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    # Mask API key in responses
    if "api_key" in d and d["api_key"]:
        key = d["api_key"]
        if len(key) > 8:
            d["api_key_masked"] = key[:4] + "****" + key[-4:]
        else:
            d["api_key_masked"] = "****"
        # 不向前端回传明文 key
        d.pop("api_key", None)
    # 网关秘钥绑定回显：主/子 Agent 各自的 api_key_id + 模型名（秘钥值不回传，
    # 只回 id 供选择器回显，脱敏名由前端按 listUsableKeys 匹配展示）。
    d["main_api_key_id"] = d.get("main_api_key_id")
    d["main_model"] = d.get("main_model") or d.get("model") or ""
    d["subagent_api_key_id"] = d.get("subagent_api_key_id")
    d["subagent_model"] = d.get("subagent_model") or ""
    # 定时任务默认绑定回显（空 = 跟随主 Agent）。
    d["scheduled_api_key_id"] = d.get("scheduled_api_key_id")
    d["scheduled_model"] = d.get("scheduled_model") or ""
    return d



async def _run_agent_task(task_id: str, request: AgentTaskRequest) -> None:
    from agent.agent_main import GenericAgent

    task_state = _TASKS[task_id]
    task_state.status = "running"
    try:
        config = AgentConfig.from_dict(request.model_dump(exclude_none=True, exclude={"prompt", "system_prompt", "max_turns"}))
        result = await GenericAgent(config, tools=_build_tool_registry()).run_task(
            request.prompt,
            system_prompt=request.system_prompt,
            max_turns=request.max_turns,
        )
        task_state.status = "completed"
        task_state.result = result
        task_state.error = None
    except asyncio.CancelledError:
        task_state.status = "aborted"
        task_state.result = None
        task_state.error = "task aborted"
        raise
    except Exception as exc:
        task_state.status = "failed"
        task_state.result = None
        task_state.error = "".join(traceback.format_exception_only(type(exc), exc)).strip()


# ── 对话管理 ─────────────────────────────────────────────────────────

_CONV_TASKS: dict[str, asyncio.Task] = {}


async def _reconcile_orphaned_streaming(conv_id: str) -> None:
    """把「没有活任务却仍标记 streaming 的最后一条 assistant 消息」翻成 error。

    发生场景：进程重启 / run_agent 被 GC / SSE 客户端断开后任务丢失，但
    assistant 占位消息从未走到结尾的 update_message(status="done")。结果就是
    刷新页面看到一条永远在「思考中…」的空壳。这里在发送新消息或读取会话
    元数据时对账：只要本进程没有该会话的活任务，就把残留 streaming 标记为
    已中断，前端回灌时不再无限转圈，也不会卡住新一轮发送的 409 检查。
    """
    try:
        messages = await conversation_store.get_messages(conv_id)
    except Exception:  # noqa: BLE001 — 对账失败不能阻断主流程
        return
    # 找最后一条 assistant 且 status=streaming 的消息
    orphan_id = None
    for msg in reversed(messages):
        if msg.get("role") != "assistant":
            continue
        if msg.get("status") == "streaming":
            orphan_id = msg.get("id")
        break
    if orphan_id is None:
        return
    await conversation_store.update_message(
        orphan_id,
        status="error",
        error="任务未正常结束（进程重启或连接中断，已自动标记为中断）",
    )


class CreateConversationRequest(BaseModel):
    title: str = "新对话"
    system_prompt: str = ""
    model: str = ""
    llm_model_id: int | None = None
    agent_id: int | None = None
    node_id: str | None = None
    context: Literal["marketplace_admin"] | None = None
    reasoning_effort: str | None = None


class UpdateConversationRequest(BaseModel):
    title: str | None = None
    system_prompt: str | None = None
    model: str | None = None
    llm_model_id: int | None = None
    reasoning_effort: str | None = None


class BatchDeleteConversationsRequest(BaseModel):
    ids: list[str] = Field(..., min_length=1, max_length=100)


class TerminalContext(BaseModel):
    """Per-turn terminal snapshot from the node terminal page.

    ``cwd`` is the directory the operator is currently looking at — it is also
    the working directory agent commands run in, so the model never picks it.
    ``terminal_id`` binds this turn to the PTY the browser is attached to, so an
    approved command runs in the terminal the operator can actually see.
    ``recent_lines`` is a tail of the xterm scrollback. All are transient
    context for this turn only — never persisted into the system_prompt.
    """
    cwd: str = ""
    terminal_id: str = ""
    recent_lines: list[str] = Field(default_factory=list)


class GoalConfig(BaseModel):
    """Optional configuration for goal mode (GA chapter6.6)."""
    objective: str = Field(..., min_length=1)
    budget_seconds: int = Field(..., ge=60)
    max_turns: int | None = Field(default=None, ge=1)


class SendMessageRequest(BaseModel):
    content: str = Field(..., min_length=1)
    max_turns: int | None = Field(default=None, ge=1)
    terminal_context: TerminalContext | None = None
    reasoning_effort: str | None = None
    # GA chapter6 execution modes. interact = one-shot Q&A (default).
    # plan = complex-task planning (plan_sop, plan.md). goal = open objective +
    # time budget driven by Guardian until budget/turns exhausted.
    # auto = 与 interact 同样的执行流程，只是审批门槛放宽：命中该执行节点
    # 管理员维护的免审名单（auto_allow_keys）的只读命令不再弹卡。code_run 在
    # auto 下仍然必审（服务端执行代码，没有分级风险判定之前不放行）。
    mode: Literal["interact", "plan", "goal", "auto"] = "interact"
    goal_config: GoalConfig | None = None


class CreateScheduledTaskRequest(BaseModel):
    name: str = Field(..., min_length=1)
    cron_expression: str = Field(..., min_length=1)
    task_prompt: str | None = None
    skill_id: int | None = None
    enabled: bool = True
    # 任务归属的 Agent：决定到点用哪个 Agent 的配置跑（prompt 模式）/ 哪个沙箱边界执行脚本。
    agent_id: int | None = None
    # 脚本型任务 + 定时背景 + 报错自愈（task_kind='script' 时生效）
    task_kind: str = "prompt"
    script_code: str | None = None
    script_type: str = "python"
    script_timeout: int = 300
    background: str | None = None
    on_error: str = "diagnose"
    allow_ai_script_fix: bool = False
    # 任务级模型覆盖（prompt 模式）。留空 → Agent 的 scheduled_* → 主 Agent。
    # 同一个 Agent 的不同定时任务可以各用一个模型。
    api_key_id: int | None = None
    model: str | None = None


class UpdateScheduledTaskRequest(BaseModel):
    name: str | None = None
    cron_expression: str | None = None
    task_prompt: str | None = None
    enabled: bool | None = None
    background: str | None = None
    on_error: str | None = None
    allow_ai_script_fix: bool | None = None
    # 改 script_code 会让 approved_hash 失配 → 下次执行被哈希锁拦下，需重新 approve-script。
    script_code: str | None = None
    script_type: str | None = None
    script_timeout: int | None = None
    api_key_id: int | None = None
    model: str | None = None


def _stream_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
        "X-Agent-SSE-Version": "2",
    }


# ── 对话端点 ────────────────────────────────────────────────────────

async def _provision_marketplace_mcp(agent_id: int, caller: str | None) -> None:
    """Grant the reserved marketplace MCP only from the admin conversation flow."""
    if caller is not None:
        # C-side admin sessions resolve to a user id; verify the actual role.
        from user_platform.models import User

        user = await User.get_or_none(id=caller)
        if user is None or user.role != "admin" or user.is_deleted or user.is_blocked:
            raise HTTPException(403, "市场管理 Agent 仅管理员可用")
    from db import PostgresClient
    import mcp_plugin_store

    row = await PostgresClient.get_agent(agent_id)
    if not row or row.get("user_id") is not None:
        raise HTTPException(403, "市场管理只能使用管理员创建的平台 Agent")
    principal_id = row.get("mcp_user_id")
    if principal_id is None:
        # Agent principal 由服务启动对账保证；请求热路径不再隐式创建数据。
        raise HTTPException(503, "Agent MCP principal 尚未初始化，请重启服务完成启动对账")
    service = await mcp_plugin_store.get_service_by_name("marketplace-status")
    if not service or not service.get("enabled"):
        raise HTTPException(503, "市场管理 MCP 不可用")
    current = set(await mcp_plugin_store.list_services_for_mcp_user(int(principal_id)))
    if int(service["id"]) not in current:
        current.add(int(service["id"]))
        await mcp_plugin_store.set_services_for_mcp_user(int(principal_id), sorted(current))


@router.post("/conversations")
async def create_conversation(request: CreateConversationRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    from db import PostgresClient

    title = request.title
    system_prompt = request.system_prompt
    model = request.model
    llm_model_id = request.llm_model_id

    if request.node_id and not request.agent_id:
        raise HTTPException(status_code=400, detail="节点 AI 对话必须选择 Agent")

    # 用户模式：只能用自己的 Agent 或团队共享的 Agent 开对话；别人的私有 Agent → 404。
    # 不再放行 user_id IS NULL 的历史「平台 Agent」（那会让任意人拿它开对话、共享其记忆）。
    if caller is not None and request.agent_id:
        await _require_agent_access(int(request.agent_id), caller)

    if request.context == "marketplace_admin":
        if not request.agent_id:
            raise HTTPException(400, "市场管理对话必须选择 Agent")
        await _provision_marketplace_mcp(request.agent_id, caller)

    if request.node_id:
        if not await _caller_authorized_for_node(caller, request.node_id):
            raise HTTPException(status_code=403, detail="无权操作该节点")
        agent_row = await PostgresClient.get_agent(request.agent_id) if request.agent_id else None
        if not agent_row or not agent_row.get("enabled", True):
            raise HTTPException(status_code=400, detail="所选 Agent 不可用")
        if not agent_row.get("main_api_key_id") or not agent_row.get("main_model"):
            raise HTTPException(status_code=400, detail="所选 Agent 未配置网关 API Key 或模型")

    # 模型跟随 Agent 的主网关模型（main_model）；对话级不再引用旧 agent_llm_models。
    # request.model 若显式给出则作为对话级覆盖（须是该 Agent 网关 key 允许的模型名）。
    if request.agent_id and (not system_prompt or not model or not title):
        agent = await PostgresClient.get_agent(request.agent_id)
        if agent:
            system_prompt = system_prompt or agent.get("system_prompt") or ""
            model = model or agent.get("main_model") or ""
            title = title or agent.get("display_name") or agent.get("name") or "新对话"

    # 场景预置统一走 scene_context：按入口入参（node_id / context）归一出场景，
    # 稳定身份键 + 场景规则一次性写进对话 system_prompt，后续轮次不再重复追加。
    # 易变的每轮事实（终端 cwd/最近输出）不在这里，见 send_message 的 terminal_context。
    scene = scene_context.normalize(node_id=request.node_id, context=request.context)
    system_prompt = scene_context.append_prompt(system_prompt, scene)

    chat_settings: dict[str, Any] = scene_context.persist(scene)
    if request.reasoning_effort is not None:
        chat_settings["reasoning_effort"] = _normalize_reasoning_effort(request.reasoning_effort)
    conv = await conversation_store.create_conversation(
        title=title,
        system_prompt=system_prompt,
        model=model,
        agent_id=request.agent_id,
        llm_model_id=llm_model_id,
        user_id=caller,
        chat_settings=chat_settings or None,
    )
    return conv


@router.get("/conversations")
async def list_conversations(
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
    agent_id: int | None = None,
    node_id: str | None = None,
    search: str | None = None,
    paged: bool = False,
    caller: str | None = Depends(get_agent_caller),
) -> Any:
    _require_ch()
    if not paged:
        return await conversation_store.list_conversations_for_caller(caller, limit=limit, kind="agent")
    try:
        return await conversation_store.list_conversations_for_caller_paged(
            caller, limit=limit, kind="agent", cursor=cursor, agent_id=agent_id,
            node_id=node_id, search=search,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/conversations/{conv_id}/meta")
async def get_conversation_meta(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    # 刷新页面读会话元数据时顺手对账残留 streaming：进程重启/断线后留下的
    # 永久 streaming 占位消息会被翻成 error，前端回灌不再无限转圈。
    await _reconcile_orphaned_streaming(conv_id)
    return conv


@router.get("/conversations/{conv_id}/messages")
async def list_conversation_messages(
    conv_id: str,
    limit: int = Query(7, ge=1, le=50),
    cursor: int | None = None,
    caller: str | None = Depends(get_agent_caller),
) -> dict:
    _require_ch()
    if await conversation_store.get_conversation_owned(caller, conv_id) is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    return await conversation_store.get_messages_page_by_user_turns(conv_id, limit=limit, cursor=cursor)


@router.get("/conversations/{conv_id}")
async def get_conversation(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    messages = await conversation_store.get_messages(conv_id)
    return {"conversation": conv, "messages": messages}


@router.delete("/conversations/{conv_id}")
async def delete_conversation(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    # 归属校验：用户只能删自己的对话
    if await conversation_store.get_conversation_owned(caller, conv_id) is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    # 如果有正在运行的任务，先取消
    task = _CONV_TASKS.pop(conv_id, None)
    if task and not task.done():
        task.cancel()
    deleted = await conversation_store.delete_conversation(conv_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="conversation not found")
    return {"deleted": True}


async def _batch_delete_conversations(conv_ids: list[str], caller: str | None) -> dict:
    """批量删除会话：逐条做归属校验后删除，未找到/无权的 id 一律归入 not_found，
    不区分两者以免泄漏存在性（与单删的 get_conversation_owned→404 口径一致）。
    运行中的会话任务同样要先取消（与单删一致）。
    """
    deleted = 0
    not_found: list[str] = []
    for conv_id in conv_ids:
        if await conversation_store.get_conversation_owned(caller, conv_id) is None:
            not_found.append(conv_id)
            continue
        task = _CONV_TASKS.pop(conv_id, None)
        if task and not task.done():
            task.cancel()
        if await conversation_store.delete_conversation(conv_id):
            deleted += 1
        else:
            not_found.append(conv_id)
    return {"deleted": deleted, "not_found": not_found}


@router.post("/conversations/batch-delete")
async def batch_delete_conversations(
    request: BatchDeleteConversationsRequest, caller: str | None = Depends(get_agent_caller)
) -> dict:
    _require_ch()
    return await _batch_delete_conversations(request.ids, caller)


@router.patch("/conversations/{conv_id}")
async def update_conversation(conv_id: str, request: UpdateConversationRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    current = await conversation_store.get_conversation_owned(caller, conv_id)
    if current is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    fields = {k: v for k, v in request.model_dump(exclude_none=True).items()}
    if "reasoning_effort" in fields:
        effort = _normalize_reasoning_effort(fields.pop("reasoning_effort"))
        settings = dict(current.get("chat_settings") or {})
        settings["reasoning_effort"] = effort
        fields["chat_settings"] = settings
    if not fields:
        raise HTTPException(status_code=400, detail="no fields to update")
    conv = await conversation_store.update_conversation(conv_id, **fields)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    return conv


@router.post("/conversations/{conv_id}/attachments")
async def upload_agent_conversation_attachment(
    conv_id: str,
    request: Request,
    filename: str = Query(..., min_length=1, max_length=255),
    caller: str | None = Depends(get_agent_caller),
) -> dict:
    """把一个文件落到该 Agent 的工作区，供运行时用 file 工具读取。

    浏览器不选路径：服务端剥掉目录、加 uuid 前缀，统一收进
    ``<workspace_root>/.chat-attachments/<conv_id>/``。返回的相对路径可直接
    追加到消息正文，Agent 用 read_file 之类的工具读。
    """
    _require_ch()
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")

    raw = await _read_attachment_body(request)
    agent_id = conv.get("agent_id")
    if agent_id:
        # GA-style default is the per-Agent directory. Respect an explicit
        # non-legacy DB workspace for future node binding; otherwise compute
        # data/agents/<id>/ so uploads land where file_read can see them.
        from agent.file_memory import agent_root
        from db import PostgresClient

        workspace_root = str(agent_root(int(agent_id)))
        agent_row = await PostgresClient.get_agent(agent_id)
        explicit = str((agent_row or {}).get("workspace_root") or "").strip()
        if explicit and explicit != "agent/workspace":
            workspace_root = explicit
    else:
        # Non-Agent conversations retain the legacy shared fallback.
        workspace_root = "agent/workspace"

    relative, target = _attachment_target(workspace_root, conv_id, filename)
    target.write_bytes(raw)
    return {"path": relative, "filename": target.name, "size": len(raw)}


@router.post("/chat/conversations/{conv_id}/attachments")
async def upload_chat_conversation_attachment(
    conv_id: str,
    request: Request,
    filename: str = Query(..., min_length=1, max_length=255),
    caller: str | None = Depends(get_agent_caller),
) -> dict:
    """把一个文件落到 temp 目录，并按类型给出可直接发给 LLM 的形态。

    聊天没有 Agent 工作区也没有文件工具，所以内容必须随消息一起进模型：
    图片回 ``data_url``（OpenAI ``image_url`` content part），文本类回 ``text``，
    其余二进制只回落盘路径（模型读不到内容，前端只在正文标注文件名）。
    """
    _require_ch()
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")

    raw = await _read_attachment_body(request)
    relative, target = _attachment_target("agent/temp", conv_id, filename)
    target.write_bytes(raw)

    mime = (
        (request.headers.get("content-type") or "").split(";")[0].strip()
        or mimetypes.guess_type(target.name)[0]
        or "application/octet-stream"
    )
    result: dict[str, Any] = {"path": relative, "filename": target.name, "size": len(raw), "mime": mime}
    if mime.startswith("image/"):
        result["data_url"] = f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"
    elif mime.startswith("text/") or mime in _TEXTUAL_MIMES:
        try:
            result["text"] = raw.decode("utf-8")
        except UnicodeDecodeError:
            # 声称是文本但解不出 UTF-8：当二进制处理，不猜编码。
            pass
    return result


async def _stream_conversation_turn(
    *,
    conv: dict,
    conv_id: str,
    assistant_msg_id: int,
    user_input: str,
    caller: str | None,
    mode: str | None,
    goal_config: Any,
    terminal_context: Any,
    max_turns: int | None,
    turn_reasoning_effort: str | None,
) -> StreamingResponse:
    """运行一次 agent 对话轮并把事件以 SSE 流回投。

    send_message 与 retry_message 共用本函数：前者新建 user+assistant 占位后调用，
    后者复用既有失败 assistant 消息（reset 为 streaming）后调用。assistant_msg_id
    决定本轮写回的目标消息；history 由 get_messages 读取并排除 assistant_msg_id，
    两种入口的上下文构建因此同形。mode/goal_config/terminal_context/max_turns
    由调用方按入口语义传入（retry 固定 interact / 无 goal / 无 terminal_context）。
    """
    from db import PostgresClient

    queue: asyncio.Queue = asyncio.Queue()
    collected_tool_calls: list[dict[str, Any]] = []
    collected_tool_results: list[Any] = []
    collected_media: list[dict[str, Any]] = []
    collected_usage: dict[str, Any] | None = None
    collected_reasoning = ""
    collected_content = ""  # 流式正文增量累积，供中途落库；run_agent 结尾仍以 final_content 为准。
    _last_persist_ts = 0.0  # 节流：正文/思考高频到达时最多每 1.5s 落一次中间态。

    async def _persist_progress() -> None:
        """把当前已累积的正文/思考/工具调用/结果/媒体渐进写回 assistant 消息，status 仍留 streaming。

        目的：审批挂起 / 进程重启 / 连接中断时，用户刷新页面仍能看到本轮已经
        产生的内容与工具调用，而不是一条空壳。正常结束时结尾的 update_message
        (status="done") 会覆盖成最终态，渐进写不会造成错乱。
        """
        nonlocal _last_persist_ts
        try:
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                media=collected_media or None,
                status="streaming",
                reasoning=collected_reasoning,
            )
            _last_persist_ts = time.monotonic()
        except Exception:  # noqa: BLE001 — 中途落库失败不能中断主流程
            pass

    async def on_event(event: dict):
        nonlocal collected_usage, collected_reasoning, collected_content, _last_persist_ts
        # 主 agent 工具调用落库，供历史回放显示；子 agent 事件只实时展示，不混入主轮次。
        etype = event.get("type")
        if etype == "tool_call":
            collected_tool_calls.append({
                # id 是 assistant.tool_calls 与 role=tool 的唯一相关性键。agent_loop
                # 已为每次调用生成好并随事件下发，这里必须一起落库：历史回放要靠它
                # 还原成 OpenAI 线协议形状，缺了就只能丢弃整条调用。
                "id": event.get("id"),
                "name": event.get("name"),
                "args": event.get("args"),
                "status": "running",
            })
            # 工具调用是天然检查点：立即落库，保证刷新后看得到「说了什么、做了哪步」。
            await _persist_progress()
        elif etype == "reasoning":
            # 思考增量按顺序累积，随 assistant 消息落库供历史回放；子 agent 的
            # subagent_reasoning 不在此收集（前缀不同），只实时展示。
            collected_reasoning += str(event.get("text") or "")
            # 高频事件：节流落库，1.5s 一次，保证审批挂起前已流出的思考不丢。
            if time.monotonic() - _last_persist_ts > 1.5:
                await _persist_progress()
        elif etype == "content":
            collected_content += str(event.get("text") or "")
            if time.monotonic() - _last_persist_ts > 1.5:
                await _persist_progress()
        elif etype == "tool_result":
            # agent_loop 的 index 是「本轮第几个」，跨轮重置；按 id(tool_call_id)
            # 匹配，否则第二轮的 result 会错配到第一轮的 call，让真正在跑的那个
            # 永远停在 running（历史回放就显示永久转圈）。与 cdp_chat_service 同口径。
            attach_tool_result(
                collected_tool_calls, collected_tool_results,
                call_id=event.get("id"),
                index=event.get("index"),
                result=event.get("data"),
            )
            # 附件工具结果 → 抽成 media part 并认领归属（仅 owner=NULL 时生效，防越权抢占）。
            await _collect_attachment_media(event, collected_media, caller, conv_id)
            await _persist_progress()
        elif etype == "question":
            # ask_user 把问题并入正文展示，同时作为检查点立即落库。
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            q = str(data.get("question") or event.get("message") or "")
            if q:
                collected_content = f"{collected_content}\n\n{q}".strip("\n")
            # ask_user 的 media 与工具结果同一套形状驱动收集 + 认领；否则刷新后历史
            # 回放看不到图。question 事件的 media 挂在 data.media 里。
            await _collect_attachment_media(event, collected_media, caller, conv_id)
            await _persist_progress()
        elif etype == "done" and isinstance(event.get("usage"), dict):
            collected_usage = event["usage"]
        await queue.put(event)

    async def run_agent():
        agent = None
        try:
            from agent.agent_main import GenericAgent
            from agent.agent_loop import agent_runner_loop, BaseHandler as _BH

            # Agent 配置一律由 GenericAgent 按 agent_id 从 DB 加载（含网关秘钥绑定
            # main_api_key_id/main_model 与 owner），不再手工预建 config —— 预建 config
            # 会让 _ensure_config 提前返回、跳过 DB 加载，导致网关列/owner 缺失。
            agent_id = conv.get("agent_id")
            agent_system_prompt = ""
            agent_max_turns = 80
            if agent_id:
                agent_row = await PostgresClient.get_agent(agent_id)
                if agent_row:
                    agent_system_prompt = agent_row.get("system_prompt") or ""
                    agent_max_turns = agent_row.get("max_turns") or 80
            # system_prompt：对话级覆盖优先，否则用 Agent 当前配置
            effective_system_prompt = conv.get("system_prompt") or agent_system_prompt or ""

            # 构建历史消息上下文（排除本轮那条 streaming assistant）。
            # rebuild_history_messages 把落库的展示态 tool_calls 归一成 OpenAI 线
            # 形状并补出配对的 role=tool，与在途轮次同形；旧的非标准 tool_results
            # 字段在那里被消费掉，不再上行。
            history = await conversation_store.get_messages(conv_id)
            initial_messages = rebuild_history_messages(
                history,
                skip_msg_id=assistant_msg_id,
                system_prompt=effective_system_prompt,
            )

            # 终端上下文作为本轮 system 上下文注入（不污染持久 system_prompt）：
            # 让 AI 感知管理员当前所在目录与最近终端输出，回答更贴合现场。仅本轮生效，
            # 不写入消息表，刷新或下一轮不带 terminal_context 时自然消失。
            tc = terminal_context
            if tc and (tc.cwd or tc.recent_lines):
                ctx_lines = ["[当前终端上下文]"]
                if tc.cwd:
                    ctx_lines.append(f"当前目录：{tc.cwd}")
                if tc.recent_lines:
                    shown = tc.recent_lines[-20:]
                    ctx_lines.append("最近终端输出：")
                    ctx_lines.extend(shown)
                initial_messages.append({"role": "system", "content": "\n".join(ctx_lines)})

            # run_as_caller=caller：网关 key 权限按当前发起用户校验（无权 → 发送失败），
            # 而非按 Agent owner。平台 Agent 对所有用户可见但各自用自己的身份跑。
            # scene= 从 chat_settings 还原（建会话时 persist 写入），让场景绑定的 MCP
            # 服务把完整方法/参数说明内联进本轮 system_prompt，首轮不必先 file_read SOP。
            scene = scene_context.from_chat_settings(conv.get("chat_settings"))
            agent = GenericAgent(
                agent_id=agent_id,
                tools=_build_tool_registry(agent_id, owner_user_id=caller),
                run_as_caller=caller,
                scene=scene,
            )
            # 配置先解析：审批超时是 per-Agent 配置，下面建 node shell / code_run 两个
            # 审批批次时都要用。_ensure_config 幂等（已解析则直接返回缓存）。
            config = await agent._ensure_config()
            approval_timeout = int(getattr(config, "approval_timeout_seconds", 0) or 0)
            node_id = ((conv.get("chat_settings") or {}).get("node_id") or "").strip()
            node_shell_batch = None
            if node_id:
                from user_platform.node_client.tools import register_node_shell_exec

                # 终端绑定：命令注入浏览器当前连着的那个 PTY，工作目录用页面正在看的
                # 目录（模型不自己选 cwd）。没有 terminal_id 时工具直接报错，不退回
                # 独立 host_exec —— 否则用户看不到命令与输出，违背「像用户输入一样」。
                tctx = terminal_context
                shell_flavor = await _node_shell_flavor(node_id)
                # Persistent authorization is owned by this execution node and is
                # shared by every caller. A lookup failure is fail-closed: use no
                # allow-list and keep the normal one-time confirmation flow.
                auto_allow_keys: set[str] | None = None
                try:
                    from user_platform.shell_approval_service import list_node_auto_allow_keys

                    auto_allow_keys = await list_node_auto_allow_keys(node_id, shell_flavor)
                except Exception:  # noqa: BLE001 — policy must never break sending
                    auto_allow_keys = None
                node_shell_batch = register_node_shell_exec(
                    agent._tools,
                    node_id,
                    caller,
                    conv_id,
                    emit=on_event,
                    terminal_id=(tctx.terminal_id if tctx else "") or "",
                    cwd=(tctx.cwd if tctx else "") or "",
                    shell_flavor=shell_flavor,
                    auto_allow_keys=auto_allow_keys,
                    timeout_seconds=approval_timeout,
                )
            # 注入事件出口：子 agent spawn 时把 subagent_* 事件透传回主对话 SSE 流。
            agent.set_event_sink(on_event)
            llm = await agent._ensure_llm()
            # 对话级模型覆盖（conv.model 与 Agent 主模型不同）：复用主 Agent 的网关 key，
            # 仅换模型名 —— fork 保留同一 api_key，计费/归属仍归主 key。
            conv_model = conv.get("model")
            if conv_model and conv_model != getattr(llm, "model", None):
                llm = llm.fork(model=conv_model)
            if turn_reasoning_effort is not None:
                llm = llm.fork(
                    thinking_enabled=bool(turn_reasoning_effort),
                    reasoning_effort=turn_reasoning_effort,
                )
            # 本轮实际生效的模型，随 assistant 消息落库供历史回放标注
            effective_model = getattr(llm, "model", "") or ""
            tools, skill_index = await agent._ensure_resources()
            from agent.code_run_approval import CodeRunApprovalBatch

            code_run_batch = CodeRunApprovalBatch(
                conversation_id=conv_id,
                agent_id=int(agent_id or 0),
                caller=caller,
                emit=on_event,
                timeout_seconds=approval_timeout,
            )
            tools.set_code_run_approval(code_run_batch)
            if skill_index:
                effective_system_prompt = f"{effective_system_prompt}\n\n{skill_index}".strip()
            # The history was assembled before resource initialization. Refresh or
            # insert its system message so the first LLM call sees the Skill index.
            if effective_system_prompt:
                if initial_messages and initial_messages[0].get("role") == "system":
                    initial_messages[0]["content"] = effective_system_prompt
                else:
                    initial_messages.insert(0, {"role": "system", "content": effective_system_prompt})
            # show_file merged into ask_user; no longer a standalone tool.
            handler = _BH(tools_registry=tools, max_turns=max_turns or config.max_turns or agent_max_turns)

            # GA chapter6 execution modes.
            mode_eff = (mode or "interact").lower()
            goal_state_file = ""
            if mode_eff == "plan":
                from agent.plan_mode import PlanModeManager
                plan_mgr = PlanModeManager(int(agent_id or 0))
                # Key the plan by conversation id so it persists across turns
                # instead of fragmenting per user message.
                plan_session = plan_mgr.create(
                    task_name=f"conv-{conv_id}",
                    objective=user_input.strip() or "complex task",
                )
                plan_hint = (
                    "\n\n[Plan Mode] Read memory/sop/plan_sop.md and follow the "
                    "five-phase flow (explore → plan → user-confirm via ask_user → "
                    f"execute → verify with a subagent). The plan file is "
                    f"{plan_session.plan_path}; "
                    "file_read it to resume, fill in steps, and mark them "
                    "[ ]/[D]/[P]/[✓]/[✗]/[FIX] as you progress."
                )
                effective_system_prompt = (effective_system_prompt or "") + plan_hint
                if initial_messages and initial_messages[0].get("role") == "system":
                    initial_messages[0]["content"] = effective_system_prompt
                else:
                    initial_messages.insert(0, {"role": "system", "content": effective_system_prompt})
            elif mode_eff == "goal":
                goal = goal_config
                if goal is None:
                    raise HTTPException(400, "goal mode requires goal_config")
                from agent.guardian import Guardian
                guardian = Guardian(agent_id=int(agent_id or 0))
                await guardian.start_goal_mode(
                    objective=goal.objective,
                    budget_seconds=goal.budget_seconds,
                    max_turns=goal.max_turns or max_turns or config.max_turns or 50,
                )
                goal_hint = (
                    f"\n\n[Goal Mode] Objective: {goal.objective}. Budget: "
                    f"{goal.budget_seconds}s. Read memory/sop/goal_mode_sop.md. "
                    f"Alternate creation/inspection/improvement phases; execute "
                    f"meaningful work each turn, do not merely report progress; "
                    f"if blocked ask the user via ask_user."
                )
                effective_system_prompt = (effective_system_prompt or "") + goal_hint
                if initial_messages and initial_messages[0].get("role") == "system":
                    initial_messages[0]["content"] = effective_system_prompt
                else:
                    initial_messages.insert(0, {"role": "system", "content": effective_system_prompt})
                goal_state_file = "temp/goal_state.json"

            async def _prepare_tool_batch(tool_calls: list[dict]) -> None:
                await code_run_batch.prepare(tool_calls)
                if node_shell_batch is not None:
                    await node_shell_batch.prepare(tool_calls)

            final_content = ""
            async for item in agent_runner_loop(
                llm,
                system_prompt=effective_system_prompt,
                user_input=user_input,
                handler=handler,
                tools_schema=tools.get_schema(),
                max_turns=max_turns or config.max_turns,
                on_event=on_event,
                initial_messages=initial_messages if initial_messages else None,
                on_tool_batch=_prepare_tool_batch,
            ):
                final_content = item.get("data", final_content)

            # Goal mode: drive continuation prompts from Guardian until the budget
            # is exhausted (next_goal_prompt returns the wrap-up, then None).
            if mode_eff == "goal":
                # Drive Guardian continuations until it emits one wrap-up prompt.
                # Each continuation is a fresh bounded Agent loop; Guardian owns
                # the overall goal budget/phase count.
                wrapped_up = False
                while True:
                    cont = await guardian.next_goal_prompt()
                    if cont is None:
                        break
                    wrapped_up = "[GOAL MODE WRAP-UP]" in cont
                    async for item in agent_runner_loop(
                        llm,
                        system_prompt=effective_system_prompt,
                        user_input=cont,
                        handler=handler,
                        tools_schema=tools.get_schema(),
                        max_turns=max_turns or config.max_turns,
                        on_event=on_event,
                        initial_messages=None,
                        on_tool_batch=_prepare_tool_batch,
                    ):
                        final_content = item.get("data", final_content)
                    if wrapped_up:
                        break
                try:
                    await guardian.mark_goal_done(budget_exhausted=wrapped_up)
                    # Mirror the final state after status transition.
                    from agent.file_memory import agent_root
                    import json as _json
                    state = await guardian.get_goal_status()
                    if state:
                        gpath = agent_root(int(agent_id)) / "temp" / "goal_state.json"
                        gpath.parent.mkdir(parents=True, exist_ok=True)
                        gpath.write_text(_json.dumps(state, ensure_ascii=False, default=str), encoding="utf-8")
                except Exception:  # noqa: BLE001
                    pass

            # 更新 assistant 消息为完成状态，并持久化工具调用供历史回放。
            # content 用累积全文（collected_content）而非只取末轮 final_content：
            # 多轮工具调用中模型的中间叙述也是用户要看的历史，末轮只有最终回答。
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content or final_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                media=collected_media or None,
                status="done",
                model=effective_model,
                usage=collected_usage or None,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "end"})
        except asyncio.CancelledError:
            await conversation_store.update_message(
                assistant_msg_id,
                media=collected_media or None,
                status="error",
                error="已中止",
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "end"})
        except ApprovalDenied:
            # 用户主动拒绝不是故障：正常结束本轮，不发 error 事件，也绝不把 denied
            # 工具结果喂回模型继续跑。审批卡已由 resolveApproval 翻成「已拒绝」。
            # on_tool_batch 在 tool_call 事件之前抛出，所以这里没有悬空的 running 工具卡；
            # 已流出的前置正文仍完整落库。
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                media=collected_media or None,
                status="done",
                model=effective_model,
                usage=collected_usage or None,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "end"})
        except ApprovalTimeout as timeout_exc:
            # 审批超时：本轮就此结束，不把「被拒绝」喂回模型跑下一轮。已流出的正文/
            # 工具调用由 _persist_progress 落过库，刷新后能看到停在哪一步。
            error_text = str(timeout_exc)
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                media=collected_media or None,
                status="error",
                error=error_text,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "error", "message": error_text})
            await queue.put({"type": "end"})
        except Exception as e:
            error_text = "".join(traceback.format_exception_only(type(e), e)).strip()
            await conversation_store.update_message(
                assistant_msg_id,
                media=collected_media or None,
                status="error",
                error=error_text,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "error", "message": error_text})
            await queue.put({"type": "end"})
        finally:
            try:
                manager = getattr(agent, "_mcp_manager", None)
                if manager is not None:
                    await manager.close()
            except Exception:
                pass

    task = asyncio.create_task(run_agent())
    _CONV_TASKS[conv_id] = task

    async def event_generator():
        try:
            while True:
                event = await queue.get()
                if event.get("type") == "end":
                    break
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        finally:
            _CONV_TASKS.pop(conv_id, None)

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=_stream_headers())


@router.post("/conversations/{conv_id}/messages")
async def send_message(conv_id: str, request: SendMessageRequest, caller: str | None = Depends(get_agent_caller)):
    _require_ch()
    from db import PostgresClient

    # 检查对话存在 + 归属
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    node_id = ((conv.get("chat_settings") or {}).get("node_id") or "").strip()
    if request.reasoning_effort is not None:
        effort = _normalize_reasoning_effort(request.reasoning_effort)
        settings = dict(conv.get("chat_settings") or {})
        settings["reasoning_effort"] = effort
        await conversation_store.update_conversation(conv_id, chat_settings=settings)
        conv["chat_settings"] = settings
        turn_reasoning_effort = effort
    conversation_effort = (conv.get("chat_settings") or {}).get("reasoning_effort")
    if request.reasoning_effort is None:
        turn_reasoning_effort = (
            _normalize_reasoning_effort(conversation_effort)
            if conversation_effort is not None
            else None
        )
    if node_id and not await _caller_authorized_for_node(caller, node_id):
        raise HTTPException(status_code=403, detail="无权操作该节点")

    # 检查是否已有运行中的任务
    existing = _CONV_TASKS.get(conv_id)
    if existing and not existing.done():
        raise HTTPException(status_code=409, detail="a task is already running for this conversation")
    # 本进程没有该会话的活任务，却可能残留一条 streaming 的 assistant 占位消息
    # （进程重启或连接中断造成）。对账成 error，避免刷新后永远「思考中」并解除
    # 下一轮发送被残留状态卡住的情况。
    await _reconcile_orphaned_streaming(conv_id)

    # 保存用户消息
    user_msg = await conversation_store.add_message(conv_id, "user", request.content)
    # 创建 assistant 占位消息
    assistant_msg = await conversation_store.add_message(conv_id, "assistant", "", status="streaming")
    assistant_msg_id = assistant_msg["id"]

    # 刷新对话时间
    await conversation_store.touch_conversation(conv_id)

    # 首次对话自动用用户输入截断做标题
    if conv.get("title") == "新对话":
        title = request.content[:30] + ("..." if len(request.content) > 30 else "")
        await conversation_store.update_conversation(conv_id, title=title)

    return await _stream_conversation_turn(
        conv=conv,
        conv_id=conv_id,
        assistant_msg_id=assistant_msg_id,
        user_input=request.content,
        caller=caller,
        mode=request.mode,
        goal_config=request.goal_config,
        terminal_context=request.terminal_context,
        max_turns=request.max_turns,
        turn_reasoning_effort=turn_reasoning_effort,
    )


@router.post("/conversations/{conv_id}/messages/{message_id}/retry")
async def retry_message(
    conv_id: str,
    message_id: int,
    caller: str | None = Depends(get_agent_caller),
):
    """重试一条失败的 assistant 消息：复位同 id 消息为 streaming，用其前一条 user
    消息内容重跑一轮 agent。只允许重试最后一条 assistant 消息（避免重放历史中间
    轮破坏上下文）；mode 固定 interact，不带 terminal_context/goal（goal 模式的
    失败轮不支持重试，请重新发起）。
    """
    _require_ch()

    # 校验对话存在 + 归属
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    node_id = ((conv.get("chat_settings") or {}).get("node_id") or "").strip()
    if node_id and not await _caller_authorized_for_node(caller, node_id):
        raise HTTPException(status_code=403, detail="无权操作该节点")

    # 检查是否已有运行中的任务
    existing = _CONV_TASKS.get(conv_id)
    if existing and not existing.done():
        raise HTTPException(status_code=409, detail="a task is already running for this conversation")
    await _reconcile_orphaned_streaming(conv_id)

    # 定位目标消息：必须是 assistant、status in {error,streaming}、且是最后一条 assistant
    history = await conversation_store.get_messages(conv_id)
    target = None
    last_assistant_id = None
    for msg in history:
        if msg.get("role") == "assistant":
            last_assistant_id = msg["id"]
            if msg["id"] == int(message_id):
                target = msg
    if target is None:
        raise HTTPException(status_code=404, detail="message not found in this conversation")
    if target.get("role") != "assistant":
        raise HTTPException(status_code=400, detail="只能重试 assistant 消息")
    if target.get("status") not in ("error", "streaming"):
        raise HTTPException(status_code=400, detail="只能重试失败或中断的消息")
    if last_assistant_id != int(message_id):
        raise HTTPException(status_code=400, detail="只能重试最后一条 assistant 消息")

    # 取前一条 user 消息作为本轮输入
    user_input = ""
    for msg in history:
        if msg["id"] == int(message_id):
            break
        if msg.get("role") == "user":
            user_input = msg.get("content") or ""
    if not user_input.strip():
        raise HTTPException(status_code=400, detail="无法重试：上一条用户消息为空")

    # 复位同一条 assistant 消息（保留 id，前端可原地更新）
    await conversation_store.update_message(
        int(message_id),
        content="",
        tool_calls=None,
        tool_results=None,
        media=None,
        status="streaming",
        error=None,
        model=None,
        usage=None,
        reasoning="",
    )
    await conversation_store.touch_conversation(conv_id)

    # 重试沿用会话已存的 reasoning_effort；mode=interact，无 goal/terminal_context。
    conversation_effort = (conv.get("chat_settings") or {}).get("reasoning_effort")
    turn_reasoning_effort = (
        _normalize_reasoning_effort(conversation_effort)
        if conversation_effort is not None
        else None
    )

    return await _stream_conversation_turn(
        conv=conv,
        conv_id=conv_id,
        assistant_msg_id=int(message_id),
        user_input=user_input,
        caller=caller,
        mode="interact",
        goal_config=None,
        terminal_context=None,
        max_turns=None,
        turn_reasoning_effort=turn_reasoning_effort,
    )



# node_shell_exec 命中需确认命令时，工具在 approval_registry 上挂起 future；
# 这两个端点让管理员批准/拒绝，并支持断线重连后查 pending。


async def _caller_authorized_for_node(caller: str | None, node_id: str) -> bool:
    """管理员（caller=None）可用任意节点；用户模式必须持有该节点绑定。"""
    if not node_id:
        return False
    if caller is None:
        return True  # 管理员
    try:
        from user_platform.nodes_service import nodes_service
        binding = await nodes_service.user_can_use_node(str(caller), node_id)
        return binding is not None
    except Exception:  # noqa: BLE001 — 授权失败按拒绝处理
        return False


async def _node_shell_flavor(node_id: str) -> str:
    """Derive the terminal shell from trusted node telemetry, failing closed."""
    try:
        from user_platform.node_client.client import get_local_node_client
        from user_platform.shell_approval_service import infer_node_shell_flavor

        rows = await get_local_node_client().list_nodes()
        node = next((row for row in rows if row.get("node_id") == node_id), None)
        return infer_node_shell_flavor(node)
    except Exception:  # noqa: BLE001 — unknown shell must use the conservative policy
        return "unknown"


async def _node_available_for_host_exec(node_id: str) -> bool:
    """Re-check approval, liveness and terminal capability before approval.

    Agent commands now run inside the browser-attached PTY, so the relevant
    capability is ``terminal`` (interactive shell), not ``host_exec``.
    """
    try:
        from user_platform.node_client.client import get_local_node_client

        rows = await get_local_node_client().list_nodes()
        node = next((row for row in rows if row.get("node_id") == node_id), None)
        if not node or node.get("status") != "approved" or not node.get("online"):
            return False
        return str((node.get("capabilities") or {}).get("terminal", "")).lower() == "true"
    except Exception:  # noqa: BLE001 — control-plane failure must fail closed
        return False


class ApprovalRequest(BaseModel):
    result: str  # "allow" | "deny"
    command_hash: str = ""


@router.post("/conversations/{conv_id}/approvals/{confirmation_id}")
async def resolve_approval(
    conv_id: str,
    confirmation_id: str,
    body: ApprovalRequest,
    caller: str | None = Depends(get_agent_caller),
) -> dict:
    _require_ch()
    from user_platform.node_client.approvals import approval_registry

    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")

    action = approval_registry.get(confirmation_id)
    if action is None or action.conversation_id != conv_id:
        raise HTTPException(status_code=404, detail="confirmation not found")
    if action.resolved is not None:
        raise HTTPException(status_code=409, detail=f"confirmation already {action.resolved}")

    # Re-validate the caller is still authorized for this node (a binding may
    # have been revoked since the conversation started) and that the command the
    # admin is approving is the one that produced the prompt (hash match).
    #
    # 节点门只对真正绑了节点的审批（node_shell_exec）生效。code_run 在服务端本地
    # 执行、没有节点，建 action 时 node_id="";此时归属已由上面的
    # get_conversation_owned + action.conversation_id == conv_id 把住，跳过节点校验。
    # 否则 _caller_authorized_for_node 里 `if not node_id: return False` 会让每个
    # code_run 审批都吃 403，卡片点不动、只能干等超时。
    if action.node_id:
        if not await _caller_authorized_for_node(caller, action.node_id):
            raise HTTPException(status_code=403, detail="无权操作该节点")
        if not await _node_available_for_host_exec(action.node_id):
            raise HTTPException(status_code=409, detail="节点已离线、未批准或不支持宿主机命令")
    if not body.command_hash or body.command_hash != action.command_hash:
        raise HTTPException(status_code=409, detail="command hash mismatch")

    result = (body.result or "").strip().lower()
    if result not in {"allow", "deny"}:
        raise HTTPException(status_code=400, detail="result must be allow or deny")

    resolved = approval_registry.resolve(confirmation_id, result)
    if resolved is None:
        # Lost a race with a concurrent resolve / timeout between the get() and
        # resolve() above.
        raise HTTPException(status_code=409, detail="confirmation already resolved")
    return {"ok": True, "confirmation_id": confirmation_id, "result": result}


@router.get("/conversations/{conv_id}/approvals")
async def list_approvals(
    conv_id: str, caller: str | None = Depends(get_agent_caller)
) -> dict:
    """Pending confirmations for a conversation (reconnect recovery)."""
    _require_ch()
    from user_platform.node_client.approvals import approval_registry

    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    approval_registry.reap_expired()
    pending = [a.to_dict() for a in approval_registry.list_pending(conv_id)]
    return {"approvals": pending}


@router.post("/conversations/{conv_id}/abort")
async def abort_conversation(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    if await conversation_store.get_conversation_owned(caller, conv_id) is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    task = _CONV_TASKS.get(conv_id)
    if task and not task.done():
        task.cancel()
        return {"aborted": True}
    return {"aborted": False, "detail": "no running task"}


# ── 聊天页（操练场/聊天）对话管理 ──────────────────────────────────────
# 复用 agent_conversations/agent_messages，以 kind='chat' 与 Agent 会话隔离。
# 仅做持久化 CRUD；流式发送由前端直连 /v1/* 端点，不跑 agent_runner_loop。

class CreateChatConversationRequest(BaseModel):
    title: str = "新对话"
    system_prompt: str = ""
    model: str = ""
    chat_settings: dict | None = None


class UpdateChatConversationRequest(BaseModel):
    title: str | None = None
    system_prompt: str | None = None
    model: str | None = None
    chat_settings: dict | None = None


class AppendChatMessageRequest(BaseModel):
    role: str = Field(..., min_length=1)
    content: str = ""
    status: str = "done"
    error: str | None = None
    media: list | None = None
    model: str = ""
    usage: dict | None = None


async def _get_chat_conversation_or_404(conv_id: str, caller: str | None) -> dict:
    _require_ch()
    conv = await conversation_store.get_conversation_owned(caller, conv_id)
    if not conv or conv.get("kind") != "chat":
        raise HTTPException(status_code=404, detail="conversation not found")
    return conv


@router.post("/chat/conversations")
async def create_chat_conversation(request: CreateChatConversationRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    return await conversation_store.create_conversation(
        title=request.title,
        system_prompt=request.system_prompt,
        model=request.model,
        kind="chat",
        chat_settings=request.chat_settings,
        user_id=caller,
    )


@router.get("/chat/conversations")
async def list_chat_conversations(
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = None,
    paged: bool = False,
    caller: str | None = Depends(get_agent_caller),
) -> Any:
    _require_ch()
    if not paged:
        return await conversation_store.list_conversations_for_caller(caller, limit=limit, kind="chat")
    try:
        return await conversation_store.list_conversations_for_caller_paged(
            caller, limit=limit, kind="chat", cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/chat/conversations/{conv_id}/meta")
async def get_chat_conversation_meta(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    return await _get_chat_conversation_or_404(conv_id, caller)


@router.get("/chat/conversations/{conv_id}/messages")
async def list_chat_conversation_messages(
    conv_id: str,
    limit: int = Query(7, ge=1, le=50),
    cursor: int | None = None,
    caller: str | None = Depends(get_agent_caller),
) -> dict:
    _require_ch()
    await _get_chat_conversation_or_404(conv_id, caller)
    return await conversation_store.get_messages_page_by_user_turns(conv_id, limit=limit, cursor=cursor)


@router.get("/chat/conversations/{conv_id}")
async def get_chat_conversation(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    conv = await _get_chat_conversation_or_404(conv_id, caller)
    messages = await conversation_store.get_messages(conv_id)
    return {"conversation": conv, "messages": messages}


@router.delete("/chat/conversations/{conv_id}")
async def delete_chat_conversation(conv_id: str, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    await _get_chat_conversation_or_404(conv_id, caller)
    deleted = await conversation_store.delete_conversation(conv_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="conversation not found")
    return {"deleted": True}


@router.post("/chat/conversations/batch-delete")
async def batch_delete_chat_conversations(
    request: BatchDeleteConversationsRequest, caller: str | None = Depends(get_agent_caller)
) -> dict:
    _require_ch()
    return await _batch_delete_conversations(request.ids, caller)


@router.patch("/chat/conversations/{conv_id}")
async def update_chat_conversation(conv_id: str, request: UpdateChatConversationRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    _require_ch()
    await _get_chat_conversation_or_404(conv_id, caller)
    fields = {k: v for k, v in request.model_dump(exclude_none=True).items()}
    if not fields:
        raise HTTPException(status_code=400, detail="no fields to update")
    conv = await conversation_store.update_conversation(conv_id, **fields)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    return conv


@router.post("/chat/conversations/{conv_id}/messages")
async def append_chat_message(conv_id: str, request: AppendChatMessageRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    conv = await _get_chat_conversation_or_404(conv_id, caller)
    msg = await conversation_store.add_message(
        conv_id, request.role, request.content, status=request.status, error=request.error,
        media=request.media, model=request.model, usage=request.usage,
    )
    await conversation_store.touch_conversation(conv_id)
    # 首条用户消息自动取标题
    if conv.get("title") == "新对话" and request.role == "user" and request.content:
        title = request.content[:30] + ("..." if len(request.content) > 30 else "")
        await conversation_store.update_conversation(conv_id, title=title)
    return msg


@router.delete("/chat/conversations/{conv_id}/messages/{message_id}")
async def delete_chat_message(conv_id: str, message_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """删除一条聊天消息（软删）。前端驱动聊天重试失败轮时用来清掉残留的
    error assistant 消息，使历史回放与重发上下文干净。
    """
    await _get_chat_conversation_or_404(conv_id, caller)
    deleted = await conversation_store.delete_message(int(message_id))
    if not deleted:
        raise HTTPException(status_code=404, detail="message not found")
    return {"deleted": True}


# ── 聊天发送（服务端内部调主链路，前端不碰 api key 明文）────────────────
# 前端传 api_key_id（整数），后端校验该 key 属于当前用户后取明文，
# 用它调 main.dispatch_entry 走完整主链路（限流/计费/日志归属）。
# 用户模式：只能用自己名下的 key；越权 key_id → 403。

class ChatSendRequest(BaseModel):
    api_key_id: int
    model: str = Field(..., min_length=1)
    messages: list[dict] = Field(..., min_length=1)
    stream: bool = True
    temperature: float | None = None
    max_tokens: int | None = None
    reasoning_effort: str | None = None


class ChatMediaRequest(BaseModel):
    api_key_id: int
    mode: Literal["image", "video", "tts"]
    model: str = Field(..., min_length=1)
    prompt: str | None = None          # image / video
    input: str | None = None           # tts
    size: str | None = None
    n: int | None = None
    seconds: int | None = None         # video
    voice: str | None = None           # tts
    response_format: str | None = None
    speed: float | None = None         # tts


async def _resolve_group_system_key(api_key_id: int, caller: str) -> dict | None:
    """在调用者所在分组绑定的系统 key 里找 api_key_id（平台代配 key 兜底）。

    系统 key 的 user_id 为 NULL（不属于个人），靠 group_ids 授权给分组成员。
    compat 层未启用 / 无绑定 / 越权 id → None，交由调用方按 403 处理。任何
    import/查询失败都吞成 None，不影响自有 key 的既有路径。
    """
    try:
        from user_platform.routes import resolve_system_api_key_for_user
    except Exception:
        return None
    try:
        rows = await resolve_system_api_key_for_user(str(caller))
    except Exception:
        return None
    for row in rows:
        if row.get("id") == api_key_id:
            return row
    return None


async def _resolve_caller_derived_key(api_key_id: int, caller: str) -> dict | None:
    """通过根 Key 的授权解析 task/editor/copy 派生 Key。

    分组系统 Key 的 ``user_id`` 为 NULL；由它派生的子 Key 也没有个人 owner，且
    分组授权查询只返回根 Key，因此普通的 owner / group 两条路径都无法命中子 Key。
    这里沿 ``parent_id`` 上溯到根，再用完全相同的个人归属/分组授权规则校验根；
    通过后返回子 Key 本身，使子 Key 自己的模型范围与 disabled 状态继续生效。
    """
    from db import PostgresClient

    child = await PostgresClient.get_api_key_by_id(api_key_id)
    if not child or not child.get("parent_id"):
        return None

    root = child
    # 写入侧会把派生链拍平到根；循环上限仅防御历史脏数据或意外环。
    seen = {int(child["id"])}
    for _ in range(8):
        parent_id = root.get("parent_id")
        if not parent_id:
            break
        parent_id = int(parent_id)
        if parent_id in seen:
            return None
        seen.add(parent_id)
        parent = await PostgresClient.get_api_key_by_id(parent_id)
        if not parent:
            return None
        root = parent
    else:
        return None

    root_id = int(root["id"])
    if str(root.get("user_id") or "") == str(caller):
        return child
    if await _resolve_group_system_key(root_id, caller):
        return child
    return None


async def _resolve_caller_api_key(api_key_id: int, caller: str | None) -> dict:
    """按调用者解析 api_key_id → 含明文 key 的行；无权/不存在 → 403。

    用户模式：依次检查个人 Key、分组系统根 Key，以及根 Key 对调用者已授权的
    task/editor/copy 派生 Key。管理员模式按 id 直接取。
    """
    from db import PostgresClient
    if caller is not None:
        row = await PostgresClient.get_api_key_by_id_for_user(api_key_id, caller)
        if not row:
            row = await _resolve_group_system_key(api_key_id, caller)
        if not row:
            row = await _resolve_caller_derived_key(api_key_id, caller)
    else:
        # 管理员模式：按 id 直接取，不必全量 list_api_keys 再线性找。
        row = await PostgresClient.get_api_key_by_id(api_key_id)
    if not row or not row.get("key"):
        raise HTTPException(status_code=403, detail="无权使用该 API Key")
    if row.get("disabled"):
        raise HTTPException(status_code=403, detail="该 API Key 已禁用")
    return row


def _humanize_context_window(tokens: int | None) -> str:
    """把上下文 token 数格式化成人类可读的窗口大小（如 1M / 200K）。"""
    try:
        n = int(tokens or 0)
    except (TypeError, ValueError):
        return ""
    if n <= 0:
        return ""
    if n >= 1_000_000:
        value = n / 1_000_000
        return (f"{value:.1f}".rstrip("0").rstrip(".")) + "M"
    if n >= 1_000:
        return f"{n // 1000}K"
    return str(n)


def _model_description(m: dict) -> str:
    """从模型元数据合成一段人类可读的说明（上下文窗口 + 能力标签）。

    模型元数据没有独立的 description 字段，可读信息分散在 name/上下文/能力/模态
    里。这里把它们拼成聊天与 Agent 选择器要展示的「模型 ID 下方说明」。
    """
    parts: list[str] = []
    if m.get("type") == "model_group":
        parts.append("自定义模型组")
    ctx = _humanize_context_window(m.get("max_context_tokens"))
    if ctx:
        parts.append(f"{ctx} 上下文")
    if m.get("function_calling"):
        parts.append("工具调用")
    if m.get("is_thinking") or m.get("auto_thinking"):
        parts.append("思考")
    outputs = m.get("output_modalities") or []
    modality_labels = {"image": "图片", "video": "视频", "audio": "语音"}
    extra_out = [modality_labels[o] for o in outputs if o in modality_labels]
    if extra_out:
        parts.append("生成" + "/".join(extra_out))
    inputs = m.get("input_modalities") or []
    if any(i in ("image", "video", "audio") for i in inputs):
        parts.append("多模态输入")
    return " · ".join(parts)


@router.get("/chat/models")
async def chat_available_models(api_key_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """按 api_key_id 的白/黑名单过滤后的可用模型列表（含自定义 model_group）。

    每条附带合成的 ``description``：上下文窗口 + 能力标签，供前端在模型 ID 下方
    展示（元数据无独立说明字段，故在此拼装）。
    """
    key_row = await _resolve_caller_api_key(api_key_id, caller)
    from rate_limiter import ModelClientPool
    # 白/黑名单与自定义模型成员裁剪都由 get_models_response 收口，这里只补 description。
    resp = await ModelClientPool.get_models_response(api_key=key_row["key"])
    all_models = resp.get("data", []) if isinstance(resp, dict) else []
    out = []
    for m in all_models:
        if not m.get("id"):
            continue
        item = dict(m)
        item["description"] = _model_description(item)
        out.append(item)
    return {"object": "list", "data": out}


@router.get("/agent-llm-models")
async def list_agent_llm_models_for_caller(caller: str | None = Depends(get_agent_caller)) -> list[dict]:
    """Agent 创建/编辑时可选的 LLM 模型库存（仅启用项，脱敏）。

    仅返回 id/model_name/display_name/config 名称等展示字段，绝不回传 api_key
    等密钥。用户和管理员都能读（共享的 LLM 基础设施），依赖 get_agent_caller 拦未登录。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        return []
    rows = await PostgresClient.list_agent_llm_models(enabled_only=True)
    out = []
    for r in rows:
        out.append({
            "id": r.get("id"),
            "llm_config_id": r.get("llm_config_id"),
            "model_name": r.get("model_name"),
            "display_name": r.get("display_name"),
            "config_name": r.get("config_name"),
            "enabled": r.get("enabled"),
            "config_enabled": r.get("config_enabled"),
        })
    return out


@router.get("/available-mcp")
async def list_available_mcp(caller: str | None = Depends(get_agent_caller)) -> dict:
    """Agent 编辑器可选的 MCP 服务，按来源分组返回。

    三类：
      - builtin：内置服务（cdp-bridge/mail），用户登录即可挂（受服务本身 enabled 门禁）。
      - admin：管理端配置且 enabled 的服务，user_id IS NULL；用户需被 mcp_service_users 授权。
      - upstream：当前用户自配的 kind=sse 服务（user_id=自己）。

    每项字段：{id, name, display_name, source, kind, tool_count, enabled, description}。
    管理员模式（caller=None）不区分个人 upstream，admin 段返回全部管理端服务。

    场景保留服务（marketplace-status / issue-workflow / review-result）不在选择器里
    露出：它们只由对应场景的对话临时获权（见 ``agent_main._collect_service_tokens``
    的 scene.services 门禁），普通 Agent 不应能勾选。
    """
    import mcp_plugin_store

    # 场景保留：只在对应场景的对话里临时签发身份，不进 MCP 选择器。
    reserved_scene_services = {"marketplace-status", "issue-workflow", "review-result"}

    async def _summarize(svc: dict, source: str) -> dict:
        tools_cache = svc.get("tools_cache") or []
        return {
            "id": svc.get("id"),
            "name": svc.get("name"),
            "display_name": svc.get("display_name") or svc.get("name"),
            "source": source,
            "kind": svc.get("kind") or ("builtin" if svc.get("builtin") else "custom"),
            "tool_count": len(tools_cache) if isinstance(tools_cache, list) else 0,
            "enabled": bool(svc.get("enabled")),
            "description": svc.get("description") or "",
        }

    try:
        all_services = await mcp_plugin_store.list_services()
    except Exception as exc:  # noqa: BLE001 — DB 不可用不阻断 Agent 编辑器打开
        logger.warning("[agent] available-mcp list_services failed: %s", exc)
        return {"builtin": [], "admin": [], "upstream": []}

    builtin_items: list[dict] = []
    admin_items: list[dict] = []
    upstream_items: list[dict] = []

    # 注：mcp_service_users 表的 user_id 引用 mcp_users.id（INTEGER），与 C 端 session 的
    # user_platform user_id（UUID）不是同一个用户模型。此处 admin 段不做按用户授权过滤，
    # 返回所有 user_id IS NULL 且 enabled 的管理端服务；per-user 授权在此处无意义。
    for svc in all_services:
        name = str(svc.get("name") or "")
        if name in reserved_scene_services:
            continue
        is_builtin = bool(svc.get("builtin")) or (svc.get("kind") or "stdio") == "builtin"
        owner = svc.get("user_id")
        if is_builtin:
            builtin_items.append(await _summarize(svc, "builtin"))
            continue
        if owner is None:
            admin_items.append(await _summarize(svc, "admin"))
        elif caller is None or owner == caller:
            # 用户模式只看自己；管理员模式看所有个人 upstream。
            upstream_items.append(await _summarize(svc, "upstream"))

    return {"builtin": builtin_items, "admin": admin_items, "upstream": upstream_items}


@router.post("/chat/send")
async def chat_send(request: ChatSendRequest, caller: str | None = Depends(get_agent_caller)):
    """文本对话：服务端用 api_key_id 对应的明文 key 调主链路 dispatch_entry。"""
    key_row = await _resolve_caller_api_key(request.api_key_id, caller)
    api_key = key_row["key"]
    api_key_name = key_row.get("name") or ""

    import main as _main
    import config as _config_mod

    body: dict[str, Any] = {
        "model": request.model,
        "messages": request.messages,
        "stream": request.stream,
    }
    if request.temperature is not None:
        body["temperature"] = request.temperature
    if request.max_tokens is not None:
        body["max_tokens"] = request.max_tokens
    if request.reasoning_effort is not None:
        effort = _normalize_reasoning_effort(request.reasoning_effort)
        if effort:
            body["reasoning_effort"] = effort

    provider_whitelist, provider_blacklist = await _config_mod.Config.get_api_key_provider_filter(api_key)
    dispatch = await _main.dispatch_entry(
        endpoint="/v1/chat/completions",
        body=body,
        headers=None,
        api_key=api_key,
        api_key_name=api_key_name,
        provider_whitelist=provider_whitelist,
        provider_blacklist=provider_blacklist,
        request_protocol="openai",
        chat_method="chat",
        client_type="user-chat",
    )
    stream_gen = dispatch["generator"]

    if dispatch["stream"]:
        async def event_stream():
            try:
                async for chunk in stream_gen:
                    if isinstance(chunk, dict) and "_last_route_info" in chunk:
                        continue
                    yield chunk
            except Exception as e:
                err = _main._openai_stream_error_chunk(str(e)) if hasattr(_main, "_openai_stream_error_chunk") else f"data: {json.dumps({'error': {'message': str(e)}}, ensure_ascii=False)}\n\n"
                yield err
                yield "data: [DONE]\n\n"
        return StreamingResponse(event_stream(), media_type="text/event-stream", headers=_stream_headers())

    result = await _main._collect_non_stream_result(stream_gen)
    if isinstance(result, dict):
        result.pop("_last_route_info", None)
        result.pop("_first_token_ms", None)
    return result


@router.post("/chat/media")
async def chat_media(request: ChatMediaRequest, caller: str | None = Depends(get_agent_caller)):
    """图片 / 视频 / 语音生成：服务端用 api_key_id 对应明文 key 调主链路。"""
    key_row = await _resolve_caller_api_key(request.api_key_id, caller)
    api_key = key_row["key"]
    api_key_name = key_row.get("name") or ""

    import main as _main
    import config as _config_mod

    body: dict[str, Any] = {"model": request.model}
    if request.mode in ("image", "video"):
        if not request.prompt:
            raise HTTPException(status_code=400, detail="缺少 prompt 参数")
        body["prompt"] = request.prompt
        if request.size:
            body["size"] = request.size
        if request.n is not None:
            body["n"] = request.n
        if request.mode == "video" and request.seconds is not None:
            body["seconds"] = request.seconds
        body["response_format"] = request.response_format or "url"
        operation = request.mode
    else:  # tts
        if not request.input:
            raise HTTPException(status_code=400, detail="缺少 input 参数")
        body["input"] = request.input
        body["voice"] = request.voice or "alloy"
        body["response_format"] = request.response_format or "mp3"
        if request.speed is not None:
            body["speed"] = request.speed
        operation = "tts_generation"

    endpoint_map = {
        "image": "/v1/images/generations",
        "video": "/v1/videos/generations",
        "tts": "/v1/audio/speech",
    }
    provider_whitelist, provider_blacklist = await _config_mod.Config.get_api_key_provider_filter(api_key)
    dispatch = await _main.dispatch_entry(
        endpoint=endpoint_map[request.mode],
        body=body,
        headers=None,
        api_key=api_key,
        api_key_name=api_key_name,
        provider_whitelist=provider_whitelist,
        provider_blacklist=provider_blacklist,
        operation=operation,
        client_type="user-chat",
    )
    result = dispatch["result"]
    # TTS 二进制结果转 base64 交给前端播放；image/video 返回 dict（含 url/b64）
    if request.mode == "tts":
        import base64 as _b64
        if isinstance(result, (bytes, bytearray)):
            audio_b64 = _b64.b64encode(bytes(result)).decode("ascii")
            return {"audio": audio_b64, "content_type": _main._audio_content_type(request.response_format or "mp3")}
        if isinstance(result, dict):
            return result
        return {"audio": None, "raw": result}
    return result


# ── 定时任务管理 ─────────────────────────────────────────────────────

def _get_scheduler():
    from agent.scheduler import AgentScheduler
    return AgentScheduler()


@router.get("/scheduled-tasks")
async def list_scheduled_tasks(caller: str | None = Depends(get_agent_caller)) -> list[dict]:
    scheduler = _get_scheduler()
    # 管理员(caller=None)看全部；用户只看自己的（user_id=caller 或历史平台行 NULL 不返）
    user_filter = caller if caller is not None else None
    jobs = await scheduler.get_jobs(user_id=user_filter)
    if caller is not None:
        # 历史平台行 user_id NULL 仅管理端可见,用户态过滤掉
        jobs = [j for j in jobs if j.get("user_id") is not None]
    return jobs


@router.post("/scheduled-tasks")
async def create_scheduled_task(request: CreateScheduledTaskRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    task_kind = (request.task_kind or "prompt").lower()
    if task_kind == "script" and not (request.script_code or "").strip():
        raise HTTPException(status_code=400, detail="task_kind=script requires script_code")
    if task_kind == "prompt" and not (request.task_prompt or "").strip():
        raise HTTPException(status_code=400, detail="task_kind=prompt requires task_prompt")
    owner = await _resolve_agent_owner(caller)
    # 任务级网关 key 必须对调用者可用，否则到点才失败（无人在场，只留一条 last_result）。
    if request.api_key_id is not None:
        await _validate_agent_gateway_key(request.api_key_id, caller)
    scheduler = _get_scheduler()
    # 脚本任务一律**不在创建时授权**：approved_hash 留空，任务落库后处于待授权态，
    # 必须显式调 approve-script 才会执行。前端建完脚本任务即引导用户当场授权，
    # 这样「谁授权了这段代码」始终是一个独立的、可审计的人工动作，而不是创建的副作用。
    job_id = await scheduler.add_job(
        name=request.name,
        cron_expression=request.cron_expression,
        task_prompt=request.task_prompt,
        skill_id=request.skill_id,
        enabled=request.enabled,
        agent_id=request.agent_id,
        user_id=owner,
        task_kind=task_kind,
        script_code=request.script_code,
        script_type=request.script_type,
        script_timeout=request.script_timeout,
        background=request.background,
        on_error=request.on_error,
        allow_ai_script_fix=request.allow_ai_script_fix,
        api_key_id=request.api_key_id,
        model=request.model,
    )
    if not job_id:
        raise HTTPException(status_code=500, detail="failed to create scheduled task")
    return {
        "id": job_id,
        "name": request.name,
        "cron_expression": request.cron_expression,
        "task_kind": task_kind,
        # 前端据此判断是否要弹授权引导：脚本任务创建后必然待授权。
        "needs_approval": task_kind == "script",
    }


@router.patch("/scheduled-tasks/{job_id}")
async def update_scheduled_task(job_id: int, request: UpdateScheduledTaskRequest, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")

    # 任务级网关 key 换绑同样先校验可用性：与创建端点同口径，避免把「到点才发现
    # 没权限」留给无人值守的那一刻。
    if request.api_key_id is not None:
        await _validate_agent_gateway_key(request.api_key_id, caller)

    # 先更新 DB
    updates = {k: v for k, v in request.model_dump(exclude_none=True).items()}
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")

    # 改脚本正文/类型/超时 → 撤销授权（approved_hash 置空），任务转待授权并停止调度。
    # 不在这里顺手重签：授权必须始终是一次独立的人工动作（approve-script），否则「编辑」
    # 就成了绕过审批的后门——改一行代码即自动获得执行权，哈希锁形同虚设。
    if any(k in updates for k in ("script_code", "script_type", "script_timeout")):
        updates["approved_hash"] = None
        updates["enabled"] = False

    set_parts = [f"{k}=${i+2}" for i, k in enumerate(updates)]
    values = list(updates.values())
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE agent_scheduled_tasks SET {', '.join(set_parts)} WHERE id=$1 RETURNING *",
            job_id, *values,
        )
    if not row:
        raise HTTPException(status_code=404, detail="scheduled task not found")

    # 如果更新了 cron 或 enabled，重新注册到 APScheduler。
    # 用 build_trigger（支持 daily/every_N 等 repeat 关键字）+ 传函数对象 _execute_job，
    # 不用 CronTrigger.from_crontab（对 repeat 关键字会抛错）和字符串引用（解析失败）。
    if "cron_expression" in updates or "enabled" in updates:
        try:
            scheduler._scheduler.remove_job(str(job_id))
        except Exception:
            pass
        if row["enabled"]:
            try:
                from agent.scheduler import _execute_job, build_trigger
                trigger = build_trigger(row["cron_expression"])
                scheduler._scheduler.add_job(
                    _execute_job, trigger=trigger,
                    id=str(job_id), kwargs={"job_id": job_id},
                    replace_existing=True,
                )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"invalid cron expression: {e}")

    return dict(row)


@router.delete("/scheduled-tasks/{job_id}")
async def delete_scheduled_task(job_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")
    # 从 APScheduler 移除
    try:
        scheduler._scheduler.remove_job(str(job_id))
    except Exception:
        pass
    # 从 DB 删除
    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow("DELETE FROM agent_scheduled_tasks WHERE id=$1 RETURNING id", job_id)
    if not row:
        raise HTTPException(status_code=404, detail="scheduled task not found")
    return {"deleted": True}


@router.post("/scheduled-tasks/{job_id}/run")
async def run_scheduled_task(job_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    scheduler = _get_scheduler()
    ok = await scheduler.run_now(job_id, user_id=caller)
    if not ok:
        raise HTTPException(status_code=404, detail="scheduled task not found or disabled")
    return {"triggered": True}


@router.post("/scheduled-tasks/{job_id}/toggle")
async def toggle_scheduled_task(job_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    from db import PostgresClient
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")

    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE agent_scheduled_tasks SET enabled = NOT enabled WHERE id=$1 RETURNING id, enabled",
            job_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="scheduled task not found")

    new_enabled = row["enabled"]
    # 同步 APScheduler。同 update：build_trigger + 函数对象，别用 from_crontab/字符串引用。
    if new_enabled:
        async with PostgresClient.pool.acquire() as conn:
            task = await conn.fetchrow("SELECT cron_expression FROM agent_scheduled_tasks WHERE id=$1", job_id)
        if task:
            try:
                from agent.scheduler import _execute_job, build_trigger
                trigger = build_trigger(task["cron_expression"])
                scheduler._scheduler.add_job(
                    _execute_job, trigger=trigger,
                    id=str(job_id), kwargs={"job_id": job_id},
                    replace_existing=True,
                )
            except Exception:
                pass
    else:
        try:
            scheduler._scheduler.remove_job(str(job_id))
        except Exception:
            pass

    return {"id": job_id, "enabled": new_enabled}


@router.get("/scheduled-tasks/{job_id}")
async def get_scheduled_task(job_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """单条任务全字段（含 script_code / pending_script_code）——列表接口刻意不带这些大字段。"""
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")
    task = await scheduler.get_task(job_id)
    if not task:
        raise HTTPException(status_code=404, detail="scheduled task not found")
    if caller is not None and task.get("user_id") is None:
        raise HTTPException(status_code=404, detail="scheduled task not found")
    return task


@router.post("/scheduled-tasks/{job_id}/approve-script")
async def approve_scheduled_task_script(job_id: int, caller: str | None = Depends(get_agent_caller)) -> dict:
    """人工授权脚本：把 pending_script_code（或现有 script_code）提升为授权态并 enable。

    这是唯一写 approved_hash 的人工入口——脚本必须经此才跑得起来（哈希锁真相源）。
    """
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")
    ok = await scheduler.approve_script(job_id)
    if not ok:
        raise HTTPException(status_code=400, detail="not a script task or no script to approve")
    return {"approved": True, "id": job_id}


@router.get("/scheduled-tasks/{job_id}/runs")
async def list_scheduled_task_runs(
    job_id: int,
    limit: int = Query(50, ge=1, le=100),
    cursor: int | None = None,
    caller: str | None = Depends(get_agent_caller),
) -> list[dict]:
    """某定时任务的执行记录列表（run_at 倒序）。详情走单条 runs/{run_id}。"""
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")
    return await scheduler.list_runs(job_id, limit=limit, cursor=cursor)


@router.get("/scheduled-tasks/{job_id}/runs/{run_id}")
async def get_scheduled_task_run(
    job_id: int, run_id: int, caller: str | None = Depends(get_agent_caller),
) -> dict:
    """单条执行记录全字段（含 stdout/stderr/conversation_id）。

    prompt/heal 执行的 Agent 对话不在此返回：前端拿 conversation_id 走既有
    /conversations/{conv_id} 系列端点渲染（归属 = 任务 owner 建会话时已写入）。
    """
    scheduler = _get_scheduler()
    if caller is not None and not await scheduler.job_owned_by(job_id, caller):
        raise HTTPException(status_code=404, detail="scheduled task not found")
    run = await scheduler.get_run(run_id)
    if not run or int(run.get("job_id") or 0) != int(job_id):
        raise HTTPException(status_code=404, detail="run not found")
    return run


# ---------------------------------------------------------------------------
# Guardian mode endpoints (Goal Mode + Autonomous Mode)
# ---------------------------------------------------------------------------

class GoalModeRequest(BaseModel):
    objective: str = Field(..., min_length=1)
    budget_seconds: int = Field(..., ge=60)
    max_turns: int = 50


class AutonomousRequest(BaseModel):
    enabled: bool


async def _require_agent_access(agent_id: int, caller: str | None) -> None:
    """goal-mode / autonomous 等端点的归属校验：owner 或同团队共享成员可操作。

    管理员（caller=None）放行。非 owner 且非同团队共享 → 404。
    """
    if caller is None:
        return
    from db import PostgresClient
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    row = await PostgresClient.get_agent(agent_id)
    if not row:
        raise HTTPException(404, f"Agent {agent_id} not found")
    owner = row.get("user_id")
    if owner is not None and str(owner) != str(caller):
        is_shared = bool(row.get("is_team_shared"))
        agent_team = row.get("team_id")
        caller_team = await _resolve_team_id_safe(caller)
        if not (is_shared and agent_team is not None and caller_team is not None
                and str(agent_team) == str(caller_team)):
            raise HTTPException(404, f"Agent {agent_id} not found")


@router.post("/agents/{agent_id}/goal-mode")
async def start_goal_mode(
    agent_id: int, request: GoalModeRequest, caller: str | None = Depends(get_agent_caller)
) -> dict:
    """Start a goal-mode run for the agent."""
    await _require_agent_access(agent_id, caller)
    from agent.guardian import Guardian
    guardian = Guardian(agent_id=agent_id)
    try:
        return await guardian.start_goal_mode(
            objective=request.objective,
            budget_seconds=request.budget_seconds,
            max_turns=request.max_turns,
        )
    except RuntimeError as e:
        raise HTTPException(503, str(e))


@router.post("/agents/{agent_id}/goal-mode/stop")
async def stop_goal_mode(
    agent_id: int, caller: str | None = Depends(get_agent_caller)
) -> dict:
    """Stop active goal mode for the agent."""
    await _require_agent_access(agent_id, caller)
    from agent.guardian import Guardian
    guardian = Guardian(agent_id=agent_id)
    stopped = await guardian.stop_goal_mode()
    return {"agent_id": agent_id, "stopped": stopped}


@router.get("/agents/{agent_id}/goal-mode/status")
async def goal_mode_status(
    agent_id: int, caller: str | None = Depends(get_agent_caller)
) -> dict:
    """Get the latest goal-mode state for the agent."""
    await _require_agent_access(agent_id, caller)
    from agent.guardian import Guardian
    guardian = Guardian(agent_id=agent_id)
    status = await guardian.get_goal_status()
    return status or {"agent_id": agent_id, "status": "idle"}


@router.post("/agents/{agent_id}/autonomous")
async def toggle_autonomous(
    agent_id: int, request: AutonomousRequest, caller: str | None = Depends(get_agent_caller)
) -> dict:
    """Enable or disable autonomous mode for the agent."""
    await _require_agent_access(agent_id, caller)
    from agent.guardian import Guardian
    guardian = Guardian(agent_id=agent_id)
    try:
        return await guardian.set_autonomous_enabled(request.enabled)
    except RuntimeError as e:
        raise HTTPException(404, str(e))


@router.get("/agents/{agent_id}/autonomous/status")
async def autonomous_status(
    agent_id: int, caller: str | None = Depends(get_agent_caller)
) -> dict:
    """Get autonomous mode status for the agent."""
    await _require_agent_access(agent_id, caller)
    from agent.guardian import Guardian
    guardian = Guardian(agent_id=agent_id)
    return await guardian.get_autonomous_status()


# ── 附件资源访问（agent 产出文件的外部访问层）──────────────────────
# show_file 工具产出 attachment_id（工作区文件）或 url（公网）；消息 media part
# 只存 id/url；各端用登录态在这里解析成可下载/预览 URL。鉴权收口在 attachment_store
# .get_for_owner：按 owner_user_id 精确匹配，未认领/非本人/已 purge → 404/410。
# to_public_dict 已带 renewable 字段，不暴露物理路径。

@router.get("/attachments/{attachment_id}")
async def get_attachment_meta_endpoint(
    attachment_id: int, caller: str | None = Depends(get_agent_caller),
) -> dict:
    """附件元信息（不含文件字节）。purged 返回 410；过期仍可读元信息以便续期。"""
    if caller is None:
        raise HTTPException(status_code=403, detail="仅登录用户可访问附件")
    import attachment_store
    row = await attachment_store.get_for_owner(attachment_id, caller, include_purged=True)
    if not row:
        raise HTTPException(status_code=404, detail="附件不存在或无权访问")
    if row.get("status") == "purged":
        raise HTTPException(status_code=410, detail="附件已被清理")
    d = attachment_store.to_public_dict(row)
    d["download_url"] = f"/agent/attachments/{attachment_id}/content"
    d["thumbnail_url"] = f"/agent/attachments/{attachment_id}/thumbnail"
    # 同时签发短时签名 URL，前端/mobile 优先用签名 URL 访问，不再依赖登录态。
    import attachment_signing
    signed_content = attachment_signing.sign_attachment_url(attachment_id, "content")
    signed_thumb = attachment_signing.sign_attachment_url(attachment_id, "thumbnail")
    if signed_content:
        d["content_url"] = signed_content
    if signed_thumb:
        d["thumbnail_url"] = signed_thumb
    return d


@router.get("/attachments/{attachment_id}/content")
async def get_attachment_content_endpoint(
    attachment_id: int,
    caller: str | None = Depends(get_agent_caller),
    exp: int | None = Query(None),
    sig: str | None = Query(None),
):
    """流式返回附件文件内容。图片/PDF/文本 inline 预览，其余下载。

    鉴权优先级：带 ``exp``+``sig`` 且签名校验通过 → 签名 URL 即凭证，免登录态，
    按 id 取行；否则回退到登录态 owner 校验（旧客户端/管理端兼容）。
    """
    import attachment_store
    import attachment_signing
    signed_ok = exp is not None and attachment_signing.verify_attachment_token(attachment_id, "content", int(exp), sig)
    if signed_ok:
        row = await attachment_store.get_active_by_id(attachment_id)
        if not row:
            # 签名有效但附件不存在/已清理：与登录态 404 一致，不暴露归属。
            raise HTTPException(status_code=404, detail="附件不存在或无权访问")
    else:
        if caller is None:
            raise HTTPException(status_code=403, detail="仅登录用户可访问附件")
        row = await attachment_store.get_for_owner(attachment_id, caller)
        if not row:
            raise HTTPException(status_code=404, detail="附件不存在或无权访问")
    status = row.get("status")
    if status == "purged":
        raise HTTPException(status_code=410, detail="附件已被清理")
    if status == "expired":
        raise HTTPException(status_code=410, detail="附件已过期，需续期后访问")
    path = attachment_store.object_path(row)
    if not path:
        raise HTTPException(status_code=410, detail="附件内容不可用（可能已被清理）")
    import os as _os
    if not _os.path.isfile(path):
        raise HTTPException(status_code=410, detail="附件文件不存在")
    mime = row.get("mime_type") or "application/octet-stream"
    name = row.get("name") or f"attachment-{attachment_id}"
    # 媒体安全：SVG 一律强制下载（同源 inline SVG 可执行脚本/引用外部资源）；
    # 声称 image/* 的文件按 magic-byte 校验真实类型，不符则回退 octet-stream
    # + attachment，避免扩展名/MIME 欺骗触发 inline 渲染。
    svg = attachment_store.is_svg_attachment(row, path)
    detected = attachment_store.detect_image_kind(path)
    if svg:
        return FileResponse(
            path,
            media_type="image/svg+xml",
            filename=name,
            content_disposition_type="attachment",  # type: ignore[arg-type]
            headers={
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "private, max-age=300",
            },
        )
    declared_image = (mime or "").startswith("image/")
    if declared_image and detected and detected != "image/svg+xml":
        mime = detected
    elif declared_image and (not detected or detected == "image/svg+xml"):
        # 声称是图但字节不是 raster：不 inline，按二进制下载。
        mime = "application/octet-stream"
    inline_prefixes = ("image/", "text/", "application/pdf", "application/json")
    disposition = "inline" if any(mime.startswith(p) for p in inline_prefixes) else "attachment"
    headers = {"X-Content-Type-Options": "nosniff"}
    if disposition == "inline" and mime.startswith("image/"):
        headers["Cache-Control"] = "private, max-age=300"
    return FileResponse(
        path,
        media_type=mime,
        filename=name,
        content_disposition_type=disposition,  # type: ignore[arg-type]
        headers=headers,
    )


@router.get("/attachments/{attachment_id}/thumbnail")
async def get_attachment_thumbnail_endpoint(
    attachment_id: int,
    caller: str | None = Depends(get_agent_caller),
    exp: int | None = Query(None),
    sig: str | None = Query(None),
):
    """图片附件缩略：直接回原图 inline（够轻量）；非图片返回 404，前端自然不渲染。

    与 content 端点同：签名 URL 验证通过即放行（免登录态），否则回退 owner 校验。
    缩略图签名与 content 不可互换（域分离 kind 区分）。
    """
    import attachment_store
    import attachment_signing
    signed_ok = exp is not None and attachment_signing.verify_attachment_token(attachment_id, "thumbnail", int(exp), sig)
    if signed_ok:
        row = await attachment_store.get_active_by_id(attachment_id)
        if not row:
            raise HTTPException(status_code=404, detail="附件不存在或无权访问")
    else:
        if caller is None:
            raise HTTPException(status_code=403, detail="仅登录用户可访问附件")
        row = await attachment_store.get_for_owner(attachment_id, caller)
        if not row:
            raise HTTPException(status_code=404, detail="附件不存在或无权访问")
    status = row.get("status")
    if status == "purged":
        raise HTTPException(status_code=410, detail="附件已被清理")
    if status == "expired":
        raise HTTPException(status_code=410, detail="附件已过期，需续期后访问")
    mime = row.get("mime_type") or ""
    if not mime.startswith("image/"):
        raise HTTPException(status_code=404, detail="非图片附件无缩略图")
    path = attachment_store.object_path(row)
    if not path:
        raise HTTPException(status_code=410, detail="附件内容不可用")
    import os as _os
    if not _os.path.isfile(path):
        raise HTTPException(status_code=410, detail="附件文件不存在")
    # SVG 与非 raster 不给缩略 inline：前者同源可执行，后者扩展名/MIME 可能欺骗；
    # 前端 raster-only 预览取不到图时自然回退附件卡，不强行 inline 任何内容。
    if attachment_store.is_svg_attachment(row, path):
        raise HTTPException(status_code=404, detail="非图片附件无缩略图")
    detected = attachment_store.detect_image_kind(path)
    if not detected or detected == "image/svg+xml":
        raise HTTPException(status_code=404, detail="非图片附件无缩略图")
    return FileResponse(
        path,
        media_type=detected,
        content_disposition_type="inline",  # type: ignore[arg-type]
        headers={
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, max-age=300",
        },
    )


@router.post("/attachments/{attachment_id}/renew")
async def renew_attachment_endpoint(
    attachment_id: int, caller: str | None = Depends(get_agent_caller),
) -> dict:
    """续期 +30 天（仅 owner）。purged 返回 410 不可续；active/expired 都可续。"""
    if caller is None:
        raise HTTPException(status_code=403, detail="仅登录用户可续期附件")
    import attachment_store
    # 带 include_purged 取本人附件：purged 也能命中以便返回 410 而非 404。
    owned = await attachment_store.get_for_owner(attachment_id, caller, include_purged=True)
    if not owned:
        raise HTTPException(status_code=404, detail="附件不存在或无权访问")
    if owned.get("status") == "purged":
        raise HTTPException(status_code=410, detail="附件已被清理，无法续期")
    row = await attachment_store.renew(attachment_id, caller)
    if not row:
        # 理论上不会到这（上面已确认 owner 且非 purged），兜底防越权。
        raise HTTPException(status_code=403, detail="无权续期该附件")
    d = attachment_store.to_public_dict(row)
    d["renewable"] = True
    return d
