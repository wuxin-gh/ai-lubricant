"""C-side task domain routes (``/api/v1/users/tasks``).

Mirrors the upstream task handler contract so the ported frontend behaves
identically. Storage + orchestration weaving lives in ``task_service``; the
VM/agent runtime is external (agent-compose) and disabled by default, so task
creation degrades to storage-only cleanly.

Scope of this first slice: task CRUD (list / info / create / update / stop /
delete / stats). The realtime ``stream``/``control`` (WebSocket) and
speech-to-text surfaces depend on the agent-compose runtime + external ASR and
are layered on later without blocking this core.
"""
from __future__ import annotations

import uuid

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, model_validator

from .deps import audit_user_action, get_current_user, resolve_team_id
from .models import User
from .models_task import Task
from .node_client.errors import RPCError
from .resource_reference_service import (
    normalize_mcp_bindings,
    resolve_prompt_for_user,
    resolve_reference_specs,
)
from .task_service import task_service

router = APIRouter(prefix="/api/v1/users/tasks", tags=["user-platform-tasks"])

_TASK_STATUSES = {"pending", "processing", "finished", "error"}


class TaskRepoReq(BaseModel):
    repo_url: str | None = None
    branch: str | None = None
    # 分支策略，对齐编辑器：default=跟随仓库默认分支；existing=checkout 已有分支
    # （branch 必填）；auto=首次初始化时从默认分支确定性创建 task/<task_id>。
    branch_mode: str | None = None


class TaskExtraConfig(BaseModel):
    project_id: str | None = None
    issue_id: str | None = None
    # 普通项传引用 id 字符串；技能集合传 {resource_id, entries:[子技能名…]}——
    # resolve_reference_specs 按 entries 过滤，只下发勾选的子技能（缺省=全集合）。
    skill_ids: list[Any] | None = None
    plugin_ids: list[str] | None = None


class VMResource(BaseModel):
    core: int | None = 1
    memory: int | None = 1024
    life: int | None = None


class CreateTaskReq(BaseModel):
    # 可为空：有节点时建交互式等待任务，无节点时建存储态草稿。
    content: str = ""
    # 首条消息的端到端幂等键；旧客户端不传时服务端生成 UUID。
    client_message_id: str | None = Field(None, min_length=1, max_length=64)
    # The runtime is now a chosen agent-compose node (not a server-defined
    # image/host). ``node_id`` must be one the user's groups were granted; it is
    # occupied exclusively for the task's lifetime.
    node_id: str | None = None
    model_id: str | None = None
    git_identity_id: str | None = None
    repo: TaskRepoReq | None = None
    cli_name: str | None = None
    provider: str | None = None
    models: list[str] | None = None
    environment: dict[str, str] | None = None
    # Execution-environment isolation tier. "isolated" (default/None) = throwaway
    # per-session home; "shared" = a named persistent home on the node (env_id
    # required) reused across tasks; "system" = the node operator's real home
    # (the node must have opted into system mode, else dispatch is refused).
    env_mode: str | None = None
    env_id: str | None = None
    env_name: str | None = None
    # 共用/系统环境自带资源的激活子集（按环境清单里的名字）：仅激活列出的
    # 技能/插件，其余环境已装项本次任务不启用。空/缺失 = 激活环境全部
    # 已装项。skill 在节点侧真正生效；plugin 协议已下发但节点暂不消费。
    active_skills: list[str] | None = None
    active_plugins: list[str] | None = None
    skill_config: list[dict] | None = None
    mcp_config: list[dict] | None = None
    plugin_config: list[dict] | None = None
    resource: VMResource | None = None
    extra: TaskExtraConfig | None = None
    system_prompt: str | None = None
    task_type: str | None = None
    sub_type: str | None = None
    # Provider/editor-defined mode id; empty means the editor's default.
    mode: str | None = None
    mode_label: str | None = None
    reasoning_effort: str | None = None
    mode_capability_snapshot: dict | None = None
    task_role: str | None = None
    # Canonical Task key lineage. A runnable Task must choose a parent API Key;
    # a ``scope='task'`` child is minted from it and bound to this Task. Codex
    # Tasks additionally require ``expected_client_id`` + ``bootstrap_content``
    # so the first provider request can be matched against this Task only.
    parent_api_key_id: int | None = None
    usage_limit: dict | None = None
    expires_at: float | None = None
    # 子 Key 收窄参数，复用既有 api_keys 列（不加新 schema）。省略(None)=继承父级；
    # model_whitelist 与 models 同源——既写 models_snapshot(运行态菜单)又收窄子 Key
    # 的 model_whitelist，让这把 Key 真正只允许所选模型。editor_provider_whitelist
    # 由前端「可用编辑器」单选映射而来。rate_limit 不做父子约束（按单 Key 计数）。
    rate_limit: dict | None = None
    model_whitelist: list[str] | None = None
    editor_provider_whitelist: list[str] | None = None
    selection_strategy: str | None = None
    expected_client_id: str | None = None
    bootstrap_content: str | None = None
    prompt_id: str | None = None


class UpdateTaskReq(BaseModel):
    title: str | None = None
    summary: str | None = None
    # Provider-defined permission/approval mode；下一条消息生效，不要求活跃运行时。
    mode: str | None = None
    mode_label: str | None = None
    reasoning_effort: str | None = None
    # Resource config hot-update. ``resource_id``-bearing entries are resolved
    # to server-trusted wire specs before persistence; full wire specs from
    # authorized callers pass through. Secrets are never rendered back.
    mcp_config: list[dict] | None = None
    skill_config: list[dict] | None = None
    plugin_config: list[dict] | None = None


class TaskAttachmentReq(BaseModel):
    url: str
    filename: str | None = None


class TaskMessageReq(BaseModel):
    content: str
    attachments: list[TaskAttachmentReq] | None = None
    # 端到端幂等键：同 ID 重复请求不会把同一句话投递/执行两遍；不传时服务端生成。
    client_message_id: str | None = Field(None, min_length=1, max_length=64)


class TaskBatchReq(BaseModel):
    # 批量停止/删除。去重 + 非空 + 上限在路由层校验，服务层只按 id 逐条处理，
    # 单条失败不中断其余——批量场景下一条不可停止/已删的 id 不应让整批回滚。
    task_ids: list[str]

    @model_validator(mode="after")
    def _normalize(self) -> "TaskBatchReq":
        seen: list[str] = []
        for tid in self.task_ids:
            tid = (tid or "").strip()
            if tid and tid not in seen:
                seen.append(tid)
        self.task_ids = seen
        return self


class TaskModelSwitchReq(BaseModel):
    model_id: str


class TaskRestartReq(BaseModel):
    # Historical Task contract: true resumes the provider conversation; false
    # starts with a clean runtime context while retaining the Task workspace and
    # its persisted resource/provider configuration.
    load_session: bool = True


def _dedup_by_name(items: list[dict]) -> list[dict]:
    """Drop duplicate resource entries by ``name`` (first wins; unnamed kept)."""
    seen: dict[str, dict] = {}
    unnamed: list[dict] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name:
            seen.setdefault(name, dict(item))
        else:
            unnamed.append(dict(item))
    return unnamed + list(seen.values())


async def _resolve_task_resource_configs(
    user: User, req: dict, request: Request
) -> dict:
    """Resolve resource references into server-trusted wire specs.

    Folds ``extra.skill_ids`` / ``extra.plugin_ids`` into the matching
    ``*_config`` lists and resolves them alongside any explicit specs. A
    ``resource_id`` the caller cannot see is rejected (→ 403) before the task
    row is written. Returns the patch dict of resource keys to persist.
    """
    extra = req.get("extra") if isinstance(req.get("extra"), dict) else {}
    base_url = str(request.base_url).rstrip("/")
    team_id = await resolve_team_id(str(user.id))
    patch: dict = {}
    for ids_key, public_key, resource_type in (
        ("skill_ids", "skill_config", "skill"),
        ("plugin_ids", "plugin_config", "plugin"),
    ):
        bindings: list[dict] = []
        for item in (extra.get(ids_key) or []):
            if isinstance(item, dict):
                # reference_id = 新表引用（统一资源池）；resource_id = 旧表引用。
                rid = str(item.get("reference_id") or item.get("resource_id") or "").strip()
                if not rid:
                    continue
                binding = (
                    {"reference_id": rid} if item.get("reference_id") else {"resource_id": rid}
                )
                entries = [str(e).strip() for e in (item.get("entries") or []) if str(e).strip()]
                if entries:
                    binding["entries"] = entries
                bindings.append(binding)
            else:
                rid = str(item or "").strip()
                if rid:
                    bindings.append({"resource_id": rid})
        explicit = req.get(public_key)
        if isinstance(explicit, list):
            bindings.extend(dict(item) for item in explicit if isinstance(item, dict))
        if not bindings:
            continue
        try:
            resolved = await resolve_reference_specs(
                str(user.id), team_id, resource_type, bindings, request_base_url=base_url,
            )
        except ValueError as exc:
            raise HTTPException(status_code=403, detail=f"资源未授权或配置无效: {exc}") from exc
        patch[public_key] = _dedup_by_name(resolved)
    mcp_explicit = req.get("mcp_config")
    if isinstance(mcp_explicit, list) and mcp_explicit:
        try:
            # 任务行不落 wire spec——mcp_config 以服务绑定形态进 task_service
            # （_sync_task_mcp_service_grants → principal service grants），
            # 派发时 _principal_mcp_specs 现造安全 spec（密钥不下发节点）。
            patch["mcp_config"] = await normalize_mcp_bindings(
                str(user.id), team_id, mcp_explicit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=403, detail=f"资源未授权或配置无效: {exc}") from exc
    return patch


def _audit_resource_counts(req: dict) -> dict:
    """Audit bodies never carry full wire specs — only per-kind counts."""
    out = dict(req)
    for key in ("mcp_config", "skill_config", "plugin_config"):
        if key in out:
            out[key] = len(out[key] or []) if isinstance(out.get(key), list) else 0
    return out


async def _install_missing_env_resources(
    user: User, payload: dict, request: Request
) -> list[str]:
    """「没有就安装」：勾选了环境里还没有的平台资源时，随任务创建真装进去。

    shared → 加进共用环境配置并同步到节点（真安装、跨任务复用，环境维护同款路径）；
    system → 经系统环境通道装进节点操作者本机（platform_managed，可在系统环境页卸载）。
    隔离档不走这里（session 私有安装由派发时的 session spec 自带）。best-effort：
    单条失败记为警告随响应返回，不阻断任务创建——任务照常派发，只是可能暂缺某资源。
    """
    env_mode = str(payload.get("env_mode") or "").strip().lower()
    if env_mode not in ("shared", "system"):
        return []
    extra = payload.get("extra") or {}
    wanted: list[tuple[str, str]] = []
    for kind, key in (("skill", "skill_ids"), ("plugin", "plugin_ids")):
        for item in (extra.get(key) or []):
            # v2 引用绑定是 {reference_id, ...}：走 resolve_reference_specs 的
            # 新表路径产出 skill_config/plugin_config 随 session payload 下发，
            # 节点在 session setup 时直接 materialize（git clone / archive fetch）。
            # 不能交给 add_environment_resource / install_to_system_env——那条路
            # 验证旧 mc_resource_references 表，v2 引用 UUID 不在其中必报错。
            if isinstance(item, dict) and item.get("reference_id"):
                continue
            # 集合绑定是 {resource_id, entries}：装整个集合进环境（entries 子集由
            # active_skills 承担，见派发侧）；普通项是 id 字符串。
            rid = str(item.get("resource_id") or "").strip() if isinstance(item, dict) else str(item or "").strip()
            if rid:
                wanted.append((kind, rid))
    if not wanted:
        return []

    user_id = str(user.id)
    team_id = await resolve_team_id(user_id)
    base_url = str(request.base_url).rstrip("/")
    warnings: list[str] = []
    kind_label = {"skill": "技能", "plugin": "插件"}

    if env_mode == "shared":
        from .environment_service import (
            EnvironmentError,
            add_environment_resource,
            sync_environment,
        )

        env_id = str(payload.get("env_id") or "").strip()
        if not env_id:
            return []
        added = 0
        for kind, rid in wanted:
            try:
                await add_environment_resource(
                    user_id, env_id, kind=kind, resource_id=rid, team_id=team_id
                )
                added += 1
            except EnvironmentError as exc:
                if exc.code == "conflict":
                    # 已在环境里：勾选页把它当环境自带处理过，无需再装。
                    continue
                warnings.append(f"{kind_label[kind]} {rid} 加入环境失败：{exc.message}")
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"资源 {rid} 加入环境失败：{exc}")
        if added:
            try:
                await sync_environment(
                    user_id, env_id, team_id=team_id, request_base_url=base_url
                )
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"环境同步失败，新资源待手动同步：{exc}")
        return warnings

    # system 档：每条走系统环境增量安装（覆盖同名=false，已存在自动跳过不报错）。
    from .system_env_service import SystemEnvError, install_to_system_env

    node_id = str(payload.get("node_id") or "").strip()
    if not node_id:
        return []
    for kind, rid in wanted:
        try:
            await install_to_system_env(
                user_id, node_id, kind=kind, resource_id=rid,
                overwrite=False, team_id=team_id, request_base_url=base_url,
            )
        except SystemEnvError as exc:
            warnings.append(f"{kind_label[kind]} {rid} 安装到节点本机失败：{exc.message}")
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"资源 {rid} 安装到节点本机失败：{exc}")
    return warnings


@router.get("/legacy-session/{session_id}")
async def resolve_legacy_task_session(
    session_id: str, user: User = Depends(get_current_user)
) -> dict:
    """Resolve a deployed editor-session bookmark to its canonical Task."""
    query = Task.filter(source_editor_session_id=session_id, deleted_at=None)
    if user.role != "admin":
        query = query.filter(user_id=user.id)
    task = await query.order_by("-created_at").first()
    if task is None:
        raise HTTPException(status_code=404, detail="旧会话尚未迁移为任务")
    return {"task_id": str(task.id)}


@router.get("")
async def list_tasks(
    user: User = Depends(get_current_user),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=200),
    # The frontend project tab sends ``size``; keep it as an alias for
    # ``page_size`` so both spellings work (``size`` wins when provided).
    size: int | None = Query(None, ge=1, le=200),
    project_id: str | None = Query(None),
    status: str | None = Query(None),
) -> dict:
    if project_id is not None:
        project_id = project_id.strip()
        if not project_id:
            project_id = None
        else:
            try:
                uuid.UUID(project_id)
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail="project_id 格式不正确")
    statuses: list[str] | None = None
    if status is not None:
        statuses = [item.strip().lower() for item in status.split(",") if item.strip()]
        invalid = sorted(set(statuses) - _TASK_STATUSES)
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"不支持的任务状态: {', '.join(invalid)}",
            )
        if not statuses:
            statuses = None
    result = await task_service.list_tasks(
        str(user.id),
        role=user.role,
        page=page,
        page_size=size or page_size,
        project_id=project_id,
        statuses=statuses,
    )
    return {
        "total": result.total,
        "page": result.page,
        "page_size": result.page_size,
        "rows": result.rows,
    }


@router.get("/{task_id}")
async def get_task(task_id: str, user: User = Depends(get_current_user)) -> dict:
    task = await task_service.get_task(str(user.id), task_id, role=user.role)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@router.post("")
async def create_task(
    body: CreateTaskReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    payload = body.model_dump(exclude_none=True)
    if "reasoning_effort" in payload:
        effort = str(payload["reasoning_effort"] or "").strip().lower()
        if effort == "max":
            effort = "xhigh"
        if effort not in {"", "low", "medium", "high", "xhigh"}:
            raise HTTPException(status_code=400, detail="reasoning_effort 必须为 low/medium/high/xhigh 或空")
        payload["reasoning_effort"] = effort
    # 规范化分支策略：existing 必须带 branch；非 existing 忽略 branch。
    repo = payload.get("repo")
    if isinstance(repo, dict):
        mode = (repo.get("branch_mode") or "default").strip()
        branch = str(repo.get("branch") or "").strip()
        if mode not in ("default", "existing", "auto"):
            raise HTTPException(status_code=400, detail="不支持的分支策略")
        if mode == "existing" and not branch:
            raise HTTPException(status_code=400, detail="选择已有分支时必须填写分支名")
        repo["branch_mode"] = mode
        repo["branch"] = branch if mode == "existing" else ""
        payload["repo"] = repo
    # Resolve resource references to trusted wire specs before persistence, so
    # unauthorized ids never reach the row or the node dispatch payload.
    payload.update(await _resolve_task_resource_configs(user, payload, request))
    # 「没有就安装」：勾选了环境里还没有的平台资源（shared/system 档）时，先真装
    # 进环境/节点本机再创建派发——任务启动时资源已就位。best-effort：失败记为
    # 警告随响应返回，不阻断创建（任务照常派发，只是可能暂缺个别资源）。
    try:
        env_install_warnings = await _install_missing_env_resources(user, payload, request)
    except Exception as exc:  # noqa: BLE001 — 安装链路的任何意外都不挡任务创建
        env_install_warnings = [f"环境资源安装失败：{exc}"]
    # Resolve an optional project prompt. The dispatch session has no prompt
    # slot, so — matching the webhook review path — the granted prompt's content
    # is prepended to the task body. ``prompt_id`` itself is not persisted.
    prompt_id = (payload.pop("prompt_id", None) or "").strip()
    if prompt_id:
        team_id = await resolve_team_id(str(user.id))
        provider = (payload.get("provider") or payload.get("cli_name") or "").strip().lower()
        prompt = await resolve_prompt_for_user(str(user.id), team_id, prompt_id, provider)
        if not prompt:
            raise HTTPException(status_code=403, detail="提示词未授权或不适用该执行客户端")
        prefix = str(prompt.get("content") or "").strip()
        if prefix:
            payload["content"] = f"{prefix}\n\n{payload.get('content', '')}".strip()
    try:
        # The node only acks after clone + resource sync + runtime start. Public
        # creation must not block on that slow phase: return the persisted task
        # immediately and expose its background progress as「准备中」.
        result = await task_service.create_task(str(user.id), payload, defer_dispatch=True)
    except ValueError as exc:
        reason = str(exc)
        detail_map = {
            "content_required": "缺少任务内容",
            "node_required": "请选择一个执行节点",
            "node_forbidden": "无权使用该节点（不在你所属分组的授权范围内）",
            "node_occupied": "该节点已被占用，请选择其它空闲节点",
            "node_system_env_disabled": "该节点未开启系统内置环境，请换一个节点或改用隔离/共用环境",
            "parent_api_key_required": "运行任务需要选择一个父 API Key",
            "parent_api_key_invalid": "父 API Key 不可用或已停用",
            "model_not_available_for_key": "所选模型不在父 API Key 的可用范围内",
            "不支持的选择策略": "不支持的 API Key 选择策略",
            "codex_bootstrap_required": "Codex 任务需预注册客户端实例与首条内容",
        }
        # 子 Key 白名单越权消息是动态的（带越权项列表），消息本身即面向用户，
        # 直接透出。
        if reason.startswith((
            "子 Key 的模型白名单不能超出父 Key 范围",
            "子 Key 的编辑器客户端白名单不能超出父 Key 范围",
        )):
            detail = reason
        else:
            detail = detail_map.get(reason, "创建任务失败")
        status_code = (
            409 if reason == "node_occupied"
            else 403 if reason in ("node_forbidden", "parent_api_key_required")
            else 400
        )
        raise HTTPException(status_code=status_code, detail=detail) from exc
    # 环境资源安装结果（成功无话，失败逐条警告）——前端创建后 toast 提示。
    result["env_install_warnings"] = env_install_warnings
    # The create response may include a one-time plaintext ``api_key``; the
    # audit log must not retain it. Resource wire specs are also redacted —
    # only per-kind counts are audited.
    masked_response = {k: v for k, v in result.items() if k != "api_key"}
    if "api_key" in result:
        masked_response["api_key"] = {**result["api_key"], "key": "<redacted>"}
    await audit_user_action(
        request, user, "task.create",
        request_body=_audit_resource_counts(payload), response=masked_response,
    )
    return result


# NOTE: ``PUT /stop`` MUST be declared before ``PUT /{task_id}``; otherwise the
# path param route captures "stop" as a task id.
@router.put("/stop")
async def stop_task(
    request: Request, task_id: str = Query(...), user: User = Depends(get_current_user)
) -> dict:
    ok = await task_service.stop_task(str(user.id), task_id, role=user.role)
    if not ok:
        raise HTTPException(status_code=404, detail="任务不存在")
    await audit_user_action(
        request, user, "task.stop", request_body={"task_id": task_id}, response={"ok": True},
    )
    return {"ok": True}


@router.post("/batch-stop")
async def batch_stop_tasks(
    body: TaskBatchReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    if not body.task_ids:
        raise HTTPException(status_code=400, detail="至少选择一个任务")
    if len(body.task_ids) > 100:
        raise HTTPException(status_code=400, detail="一次最多操作 100 个任务")

    stopped = 0
    failed: list[dict[str, str]] = []
    for task_id in body.task_ids:
        try:
            ok = await task_service.stop_task(str(user.id), task_id, role=user.role)
            if not ok:
                failed.append({"id": task_id, "error": "任务不存在"})
                continue
            stopped += 1
            await audit_user_action(
                request, user, "task.stop",
                request_body={"task_id": task_id, "batch": True},
                response={"ok": True},
            )
        except Exception as exc:  # noqa: BLE001 — 单条失败不影响批量其余项
            failed.append({"id": task_id, "error": str(exc) or "停止任务失败"})
    return {"stopped": stopped, "failed": failed}


@router.post("/batch-delete")
async def batch_delete_tasks(
    body: TaskBatchReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    if not body.task_ids:
        raise HTTPException(status_code=400, detail="至少选择一个任务")
    if len(body.task_ids) > 100:
        raise HTTPException(status_code=400, detail="一次最多操作 100 个任务")

    deleted = 0
    failed: list[dict[str, str]] = []
    for task_id in body.task_ids:
        try:
            ok = await task_service.delete_task(str(user.id), task_id, role=user.role)
            if not ok:
                failed.append({"id": task_id, "error": "任务不存在"})
                continue
            deleted += 1
            await audit_user_action(
                request, user, "task.delete",
                request_body={"task_id": task_id, "batch": True},
                response={"deleted": True},
            )
        except Exception as exc:  # noqa: BLE001 — 单条失败不影响批量其余项
            failed.append({"id": task_id, "error": str(exc) or "删除任务失败"})
    return {"deleted": deleted, "failed": failed}


@router.put("/{task_id}")
async def update_task(
    task_id: str, body: UpdateTaskReq, request: Request, user: User = Depends(get_current_user)
) -> dict:
    fields = body.model_dump(exclude_none=True)
    if "reasoning_effort" in fields:
        effort = str(fields["reasoning_effort"] or "").strip().lower()
        if effort == "max":
            effort = "xhigh"
        if effort not in {"", "low", "medium", "high", "xhigh"}:
            raise HTTPException(status_code=400, detail="reasoning_effort 必须为 low/medium/high/xhigh 或空")
        fields["reasoning_effort"] = effort
    # Hot-update resource configs resolve the same way as create: resource_id
    # refs become trusted specs; unauthorized ids are rejected before persist.
    fields.update(await _resolve_task_resource_configs(user, fields, request))
    result = await task_service.update_task(
        str(user.id), task_id, fields, role=user.role
    )
    if result is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    await audit_user_action(
        request, user, "task.update",
        request_body={"task_id": task_id, **_audit_resource_counts(fields)},
        response=result,
    )
    return result


@router.delete("/{task_id}")
async def delete_task(
    task_id: str, request: Request, user: User = Depends(get_current_user)
) -> dict:
    ok = await task_service.delete_task(str(user.id), task_id, role=user.role)
    if not ok:
        raise HTTPException(status_code=404, detail="任务不存在")
    await audit_user_action(
        request, user, "task.delete", request_body={"task_id": task_id}, response={"deleted": True},
    )
    return {"deleted": True}


@router.post("/{task_id}/messages")
async def send_task_message(
    task_id: str,
    body: TaskMessageReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.send_task_message(
            str(user.id),
            task_id,
            body.content,
            attachments=[item.model_dump() for item in (body.attachments or [])],
            role=user.role,
            client_message_id=body.client_message_id,
        )
    except ValueError as exc:
        reason = str(exc)
        status = (
            404 if reason == "task_not_found"
            else 403 if reason == "node_forbidden"
            else 503 if reason in ("node_server_unavailable", "task_api_key_unavailable")
            else 409 if reason in ("runtime_not_bound", "runtime_dispatch_failed")
            else 422 if reason == "content_required"
            else 409
        )
        detail = {
            "task_not_found": "任务不存在",
            "runtime_not_bound": "任务当前没有可用运行时",
            "node_required": "任务未绑定执行节点",
            "node_forbidden": "无权使用任务绑定的执行节点",
            "node_system_env_disabled": "该节点未开启系统内置环境，请换一个节点或改用隔离/共用环境",
            "node_server_unavailable": "节点控制面暂不可用，请确认节点服务已启动后重试",
            "task_api_key_unavailable": "任务 API Key 不可用，无法启动运行时",
            "task_not_startable": "当前任务状态不能启动运行时",
            "runtime_dispatch_conflict": "任务运行时启动冲突，请刷新后重试",
            "runtime_dispatch_failed": "任务运行时派发失败，请查看任务详情中的派发错误",
            "content_required": "消息不能为空",
        }.get(reason, "消息发送失败")
        raise HTTPException(status_code=status, detail=detail) from exc
    except RPCError as exc:
        # Any control-plane RPC not normalized to a ValueError above (e.g. a
        # second stale placement, or an unexpected code) must not surface as a
        # raw 500. Map to a retryable 503 so the client can re-send.
        raise HTTPException(
            status_code=503,
            detail=f"节点运行时暂不可用：{exc.message}",
        ) from exc
    await audit_user_action(
        request, user, "task.message",
        request_body={
            "task_id": task_id,
            "content_length": len(body.content or ""),
            "attachment_count": len(body.attachments or []),
            "client_message_id": body.client_message_id,
        },
        response=result,
    )
    return result


@router.post("/{task_id}/cancel")
async def cancel_task_turn(
    task_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.cancel_task_turn(str(user.id), task_id, role=user.role)
    except ValueError as exc:
        reason = str(exc)
        if reason == "task_not_found":
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        if reason == "node_server_unavailable":
            raise HTTPException(
                status_code=503, detail="节点控制面暂不可用，请确认节点服务已启动后重试"
            ) from exc
        raise HTTPException(status_code=409, detail="任务当前没有可用运行时") from exc
    await audit_user_action(request, user, "task.cancel", request_body={"task_id": task_id}, response=result)
    return result


@router.post("/{task_id}/model")
async def switch_task_model(
    task_id: str,
    body: TaskModelSwitchReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.switch_task_model(
            str(user.id), task_id, body.model_id, role=user.role
        )
    except ValueError as exc:
        reason = str(exc)
        status = 404 if reason == "task_not_found" else 400
        detail = {
            "task_not_found": "任务不存在",
            "model_required": "请选择一个模型",
            "model_not_available_for_key": "目标模型当前不可用：不在所属 API Key 的可用范围内，或该模型暂无可用渠道供给",
            "parent_api_key_invalid": "任务 API Key 不可用，无法校验模型",
        }.get(reason, "切换模型失败")
        raise HTTPException(status_code=status, detail=detail) from exc
    await audit_user_action(
        request, user, "task.model_switch",
        request_body={"task_id": task_id, "model_id": body.model_id}, response=result,
    )
    return result


class TaskModelsAddReq(BaseModel):
    models: list[str]


@router.post("/{task_id}/models")
async def add_task_models(
    task_id: str,
    body: TaskModelsAddReq,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.add_task_models(
            str(user.id), task_id, body.models, role=user.role
        )
    except ValueError as exc:
        reason = str(exc)
        status = 404 if reason == "task_not_found" else 400
        detail = {
            "task_not_found": "任务不存在",
            "model_required": "请至少选择一个模型",
            "model_not_available_for_key": "所选模型不在该 API Key 可用范围内",
            "parent_api_key_invalid": "任务 API Key 不可用，无法校验模型",
        }.get(reason, "添加模型失败")
        raise HTTPException(status_code=status, detail=detail) from exc
    await audit_user_action(
        request, user, "task.models.add",
        request_body={"task_id": task_id, "models": body.models}, response=result,
    )
    return result


@router.post("/{task_id}/start")
async def start_task_runtime(
    task_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.start_task_runtime(
            str(user.id), task_id, role=user.role
        )
    except ValueError as exc:
        reason = str(exc)
        status = (
            404 if reason == "task_not_found"
            else 403 if reason == "node_forbidden"
            else 503 if reason in ("node_server_unavailable", "task_api_key_unavailable")
            else 409
        )
        detail = {
            "task_not_found": "任务不存在",
            "node_required": "任务未绑定执行节点",
            "node_forbidden": "无权使用任务绑定的执行节点",
            "node_system_env_disabled": "该节点未开启系统内置环境，请换一个节点或改用隔离/共用环境",
            "node_server_unavailable": "节点控制面暂不可用，请确认节点服务已启动后重试",
            "task_api_key_unavailable": "任务 API Key 不可用，无法启动运行时",
            "task_not_startable": "当前任务状态不能启动运行时",
            "runtime_dispatch_conflict": "任务运行时启动冲突，请刷新后重试",
            "runtime_dispatch_failed": "节点拒绝或未能启动任务运行时，请检查节点状态后重试",
        }.get(reason, "任务运行时启动失败")
        raise HTTPException(status_code=status, detail=detail) from exc
    await audit_user_action(
        request, user, "task.start", request_body={"task_id": task_id}, response=result,
    )
    return result


@router.post("/{task_id}/restart")
async def restart_task_runtime(
    task_id: str,
    request: Request,
    body: TaskRestartReq | None = None,
    user: User = Depends(get_current_user),
) -> dict:
    load_session = body.load_session if body is not None else True
    try:
        result = await task_service.restart_task_runtime(
            str(user.id), task_id, load_session=load_session, role=user.role
        )
    except ValueError as exc:
        reason = str(exc)
        if reason == "task_not_found":
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        if reason == "node_server_unavailable":
            raise HTTPException(
                status_code=503, detail="节点控制面暂不可用，请确认节点服务已启动后重试"
            ) from exc
        raise HTTPException(status_code=409, detail="任务当前没有可用运行时") from exc
    await audit_user_action(
        request, user, "task.restart",
        request_body={"task_id": task_id, "load_session": load_session}, response=result,
    )
    return result


@router.post("/{task_id}/api-key/disable")
async def disable_task_api_key(
    task_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.disable_task_api_key(str(user.id), task_id, role=user.role)
    except ValueError as exc:
        reason = str(exc)
        status = 404 if reason == "task_not_found" else 409
        detail = "任务不存在" if status == 404 else "任务没有可用的 API Key"
        raise HTTPException(status_code=status, detail=detail) from exc
    await audit_user_action(
        request, user, "task.api-key.disable",
        request_body={"task_id": task_id}, response=result,
    )
    return result


@router.post("/{task_id}/api-key/rotate")
async def rotate_task_api_key(
    task_id: str,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        result = await task_service.rotate_task_api_key(str(user.id), task_id, role=user.role)
    except ValueError as exc:
        reason = str(exc)
        status = (
            404 if reason == "task_not_found"
            else 409 if reason == "runtime_not_bound"
            else 400
        )
        detail = (
            "任务不存在" if status == 404
            else "任务当前没有可用运行时，无法轮换 Key" if status == 409
            else "任务没有可用的 API Key"
        )
        raise HTTPException(status_code=status, detail=detail) from exc
    # The rotate response carries a one-time plaintext key; redact it from audit.
    masked = {k: v for k, v in result.items() if k != "key"}
    masked["key"] = "<redacted>"
    await audit_user_action(
        request, user, "task.api-key.rotate",
        request_body={"task_id": task_id}, response=masked,
    )
    return result


@router.get("/{task_id}/events/history")
async def list_task_events_history(
    task_id: str,
    before: int | None = Query(None, ge=1),
    limit: int = Query(50, ge=1, le=200),
    user: User = Depends(get_current_user),
) -> dict:
    """Replay persisted task events (conversation history, newest-first cursor).

    The live ``GET /{task_id}/events`` stream is fire-and-forget; this endpoint
    paginates the durable ``mc_task_events`` rows so a reconnecting client can
    show prior turns. ``before`` is an exclusive seq upper bound.
    """
    return await task_service.list_task_events(
        str(user.id), task_id, before=before, limit=limit, role=user.role
    )


@router.get("/{task_id}/events")
async def stream_task_events(task_id: str, user: User = Depends(get_current_user)):
    import json as _json
    from fastapi.responses import StreamingResponse

    # Resolve ownership/runtime before opening the response so invalid tasks get
    # a normal HTTP status instead of a late error inside an SSE stream.
    try:
        events = task_service.task_events(str(user.id), task_id, role=user.role)
        first = await events.__anext__()
    except ValueError as exc:
        reason = str(exc)
        status = 404 if reason == "task_not_found" else 409
        detail = "任务不存在" if status == 404 else "任务当前没有可用运行时"
        raise HTTPException(status_code=status, detail=detail) from exc
    except StopAsyncIteration:
        first = None

    async def event_generator():
        yield ": ok\n\n"
        try:
            if first is not None:
                kind = first.get("kind")
                name = "agent_event" if kind == "structured" else kind or "message"
                yield f"event: {name}\ndata: {_json.dumps(first, ensure_ascii=False)}\n\n"
            async for event in events:
                kind = event.get("kind")
                name = "agent_event" if kind == "structured" else kind or "message"
                yield f"event: {name}\ndata: {_json.dumps(event, ensure_ascii=False)}\n\n"
                if kind == "result":
                    break
        except Exception as exc:  # noqa: BLE001
            yield f"event: error\ndata: {_json.dumps({'message': str(exc)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/{task_id}/stats")
async def task_stats(task_id: str, user: User = Depends(get_current_user)) -> dict:
    return await task_service.task_stats(str(user.id), task_id, role=user.role)


@router.get("/{task_id}/logs")
async def list_task_request_logs(
    task_id: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
) -> dict:
    try:
        return await task_service.list_task_request_logs(
            str(user.id), task_id, limit=limit, offset=offset, role=user.role
        )
    except ValueError as exc:
        if str(exc) == "task_not_found":
            raise HTTPException(status_code=404, detail="任务不存在")
        raise


@router.get("/{task_id}/logs/{log_id}")
async def get_task_request_log(
    task_id: str,
    log_id: int,
    user: User = Depends(get_current_user),
) -> dict:
    try:
        log = await task_service.get_task_request_log_detail(
            str(user.id), task_id, log_id, role=user.role
        )
    except ValueError as exc:
        if str(exc) == "task_not_found":
            raise HTTPException(status_code=404, detail="任务不存在")
        raise
    if log is None:
        raise HTTPException(status_code=404, detail="日志不存在")
    return log
