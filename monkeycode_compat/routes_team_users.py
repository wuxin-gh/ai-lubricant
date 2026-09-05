"""Team users / groups / audit routes (``/api/v1/teams``).

Pure-CRUD team-admin surface: platform users, team groups and the audit log.
These endpoints mirror the MonkeyCode team handler contract so the ported
frontend behaves identically. They never touch ``/admin/*`` or the ``/v1/*``
model pipeline, and never run any VM/orchestration runtime.

Handlers return plain dict/list; the frontend endpoint map wraps them in the
Go ``web.Resp`` envelope. Timestamps are unix seconds.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, time, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from .deps import client_meta as _client_meta, get_current_team_id, get_current_user
from .models import TeamGroup, TeamGroupMember, TeamMember, User
from .models_project import Project
from .models_task import Task, TaskUsageStat
from .session import USER_SESSION_COOKIE, session_store
from .team_users_service import team_users_service

router = APIRouter(prefix="/api/v1/teams", tags=["monkeycode-team-users"])


async def require_team_admin(user: User = Depends(get_current_user)) -> User:
    """用户管理写操作（重置密码/提权/删除/改名封禁）仅平台 admin 可用。

    C 端 session 已解析出当前用户；非 admin 一律 403，杜绝任意用户越权接管他人账号。
    """
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可执行此操作")
    return user


# ── request bodies ────────────────────────────────────────────────────────────


class CreateUsersReq(BaseModel):
    emails: list[str]
    group_id: str | None = None


class ChangePasswordReq(BaseModel):
    current_password: str | None = None
    new_password: str


class GroupReq(BaseModel):
    name: str


class SetGroupUsersReq(BaseModel):
    user_ids: list[str]


class SetGroupApiKeysReq(BaseModel):
    key_ids: list[int]


class SetGroupMcpServicesReq(BaseModel):
    service_ids: list[int]


class SetGroupSkillsReq(BaseModel):
    skill_ids: list[str]


class SetAdminReq(BaseModel):
    is_admin: bool


class CreateUserReq(BaseModel):
    email: str
    name: str | None = None
    is_admin: bool = False


class UpdateUserReq(BaseModel):
    name: str | None = None
    is_blocked: bool | None = None



def _as_utc(dt: Any) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _unix(dt: Any) -> int | None:
    dt = _as_utc(dt)
    if dt is None:
        return None
    return int(dt.timestamp())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _day_start(dt: datetime) -> datetime:
    return datetime.combine(dt.date(), time.min, tzinfo=timezone.utc)


def _range_start(range_key: str, now: datetime) -> datetime:
    if range_key == "today":
        return _day_start(now)
    if range_key == "30d":
        return _day_start(now) - timedelta(days=29)
    return _day_start(now) - timedelta(days=6)


def _trend_points(rows: list[Any], start: datetime, end: datetime, attr: str = "created_at") -> list[dict]:
    counts: Counter[str] = Counter()
    for row in rows:
        dt = _as_utc(getattr(row, attr, None))
        if dt is None or dt < start or dt > end:
            continue
        counts[dt.date().isoformat()] += 1
    days = (end.date() - start.date()).days + 1
    return [
        {"date": (start.date() + timedelta(days=i)).isoformat(), "value": counts[(start.date() + timedelta(days=i)).isoformat()]}
        for i in range(max(days, 1))
    ]


async def _team_user_map(team_id: str) -> tuple[list[TeamMember], dict[str, User]]:
    members = await TeamMember.filter(team_id=team_id)
    user_ids = [m.user_id for m in members]
    users = await User.filter(id__in=user_ids, is_deleted=False) if user_ids else []
    return members, {str(u.id): u for u in users}


# ── dashboard ─────────────────────────────────────────────────────────────────


@router.get("/dashboard")
async def dashboard(
    range: str = Query(default="7d"),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Team manager dashboard summary for ``/manager/overview``.

    The frontend expects the MonkeyCode ``TeamDashboardResp`` shape. Keep this
    endpoint storage-only: aggregate existing team/project/task/usage rows, and
    return empty arrays rather than failing when optional usage data is absent.
    """
    range_key = range if range in {"today", "7d", "30d"} else "7d"
    now = _utc_now()
    end = now
    start = _range_start(range_key, now)
    today_start = _day_start(now)
    last_7d_start = today_start - timedelta(days=6)

    members, users_by_id = await _team_user_map(team_id)
    user_ids = [m.user_id for m in members if str(m.user_id) in users_by_id]
    user_id_set = {str(uid) for uid in user_ids}

    projects = await Project.filter(user_id__in=user_ids) if user_ids else []
    tasks = await Task.filter(user_id__in=user_ids) if user_ids else []
    usage_rows = await TaskUsageStat.filter(user_id__in=user_ids) if user_ids else []

    project_total = len(projects)
    task_total = len(tasks)
    project_stats = {
        "total": project_total,
        "active_today": sum(1 for p in projects if _as_utc(p.updated_at) and _as_utc(p.updated_at) >= today_start),
        "active_7d": sum(1 for p in projects if _as_utc(p.updated_at) and _as_utc(p.updated_at) >= last_7d_start),
        "daily_created": _trend_points(projects, start, end),
    }
    task_stats = {
        "total": task_total,
        "active_today": sum(1 for t in tasks if _as_utc(t.last_active_at) and _as_utc(t.last_active_at) >= today_start),
        "active_7d": sum(1 for t in tasks if _as_utc(t.last_active_at) and _as_utc(t.last_active_at) >= last_7d_start),
        "daily_created": _trend_points(tasks, start, end),
    }
    conversation_stats = {
        "total": task_total,
        "count_today": sum(1 for t in tasks if _as_utc(t.created_at) and _as_utc(t.created_at) >= today_start),
        "count_7d": sum(1 for t in tasks if _as_utc(t.created_at) and _as_utc(t.created_at) >= last_7d_start),
        "daily_created": _trend_points(tasks, start, end),
    }

    total_tokens = sum(int(u.total_tokens or 0) for u in usage_rows)
    input_tokens = sum(int(u.input_tokens or 0) for u in usage_rows)
    output_tokens = sum(int(u.output_tokens or 0) for u in usage_rows)
    running_tasks = [t for t in tasks if t.status in {"pending", "processing"}]
    finished_tasks = [t for t in tasks if t.status == "finished"]
    durations = [
        (_as_utc(t.completed_at) - _as_utc(t.created_at)).total_seconds()
        for t in finished_tasks
        if _as_utc(t.completed_at) is not None and _as_utc(t.created_at) is not None and _as_utc(t.completed_at) >= _as_utc(t.created_at)
    ]
    active_members = sum(1 for m in members if _as_utc(m.last_active_at) is not None and _as_utc(m.last_active_at) >= last_7d_start)
    total_members = len(user_id_set)

    task_count_by_user: Counter[str] = Counter(str(t.user_id) for t in tasks)
    token_by_user: Counter[str] = Counter()
    requests_by_user: Counter[str] = Counter()
    for row in usage_rows:
        uid = str(row.user_id)
        token_by_user[uid] += int(row.total_tokens or 0)
        requests_by_user[uid] += 1

    groups = await TeamGroup.filter(team_id=team_id, is_deleted=False)
    group_name_by_user: dict[str, str] = {}
    if groups:
        group_by_id = {str(g.id): g.name for g in groups}
        links = await TeamGroupMember.filter(group_id__in=[g.id for g in groups])
        for link in links:
            uid = str(link.user_id)
            if uid in user_id_set and uid not in group_name_by_user:
                group_name_by_user[uid] = group_by_id.get(str(link.group_id), "")

    active_member_rows = sorted(
        (
            {
                "user_id": uid,
                "name": users_by_id[uid].name,
                "email": users_by_id[uid].email,
                "group_name": group_name_by_user.get(uid),
                "task_count": task_count_by_user[uid],
                "last_active_at": _unix(next((m.last_active_at for m in members if str(m.user_id) == uid), None)),
            }
            for uid in user_id_set
        ),
        key=lambda item: (item["task_count"], item["last_active_at"] or 0),
        reverse=True,
    )[:5]

    high_consumption = [
        {
            "id": uid,
            "name": users_by_id[uid].name or users_by_id[uid].email or uid,
            "type": "member",
            "total_tokens": tokens,
            "llm_requests": requests_by_user[uid],
            "percent": round(tokens / total_tokens * 100, 2) if total_tokens > 0 else 0,
        }
        for uid, tokens in token_by_user.most_common(5)
        if uid in users_by_id and tokens > 0
    ]
    long_running_tasks = sorted(
        (
            {
                "task_id": str(t.id),
                "title": t.title or t.content,
                "status": t.status,
                "creator": (users_by_id.get(str(t.user_id)).name if users_by_id.get(str(t.user_id)) else None),
                "host_name": None,
                "created_at": _unix(t.created_at),
                "duration": int(((_as_utc(t.completed_at) or now) - _as_utc(t.created_at)).total_seconds()) if _as_utc(t.created_at) else 0,
            }
            for t in tasks
            if t.created_at is not None and t.status in {"pending", "processing"}
        ),
        key=lambda item: item["duration"],
        reverse=True,
    )[:5]

    return {
        "range": range_key,
        "start_at": int(start.timestamp()),
        "end_at": int(end.timestamp()),
        "metrics": {
            "total_members": total_members,
            "active_members": active_members,
            "active_rate": round(active_members / total_members * 100, 2) if total_members else 0,
            "task_count": task_total,
            "running_task_count": len(running_tasks),
            "finished_task_count": len(finished_tasks),
            "average_duration": int(sum(durations) / len(durations)) if durations else 0,
            "llm_requests": len(usage_rows),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cached_tokens": 0,
            "cache_hit_rate": 0,
        },
        "project_stats": project_stats,
        "task_stats": task_stats,
        "conversation_stats": conversation_stats,
        "trends": {
            "task_counts": task_stats["daily_created"],
            "token_usage": _trend_points(usage_rows, start, end),
            "active_members": [],
        },
        "insights": {
            "active_members": active_member_rows,
            "high_consumption": high_consumption,
            "long_running_tasks": long_running_tasks,
        },
    }


# ── users ─────────────────────────────────────────────────────────────────────


@router.get("/users")
async def list_users(
    role: str | None = Query(default=None),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    return await team_users_service.list_users(team_id, role=role)


@router.get("/users/status")
async def users_status(
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    return await team_users_service.status(user, team_id)


@router.post("/users/logout")
async def users_logout(
    request: Request,
    user: User = Depends(get_current_user),
) -> dict:
    cookie = request.cookies.get(USER_SESSION_COOKIE) or ""
    await session_store.delete(USER_SESSION_COOKIE, str(user.id), cookie)
    return {}


@router.post("/users/with-password")
async def create_users_with_password(
    body: CreateUsersReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    result = await team_users_service.create_users_with_password(
        team_id, body.emails, body.group_id
    )
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.create_with_password",
        request=body.model_dump(), response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    request: Request,
    user: User = Depends(require_team_admin),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    result = await team_users_service.delete_user(user_id)
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.delete",
        request={"user_id": user_id}, response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/users/passwords/change")
async def change_password(
    body: ChangePasswordReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        result = await team_users_service.change_password(
            user, body.current_password, body.new_password
        )
    except ValueError as exc:
        reason = str(exc)
        detail = {
            "weak_password": "密码长度需为 8-32 位",
            "invalid_current_password": "当前密码不正确",
        }.get(reason, "修改密码失败")
        status_code = 400 if reason == "weak_password" else 403
        raise HTTPException(status_code=status_code, detail=detail) from exc
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.change_password",
        request={"user_id": str(user.id)}, response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/users/{user_id}/passwords/reset")
async def reset_password(
    user_id: str,
    request: Request,
    user: User = Depends(require_team_admin),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        result = await team_users_service.reset_password(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="用户不存在") from exc
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.reset_password",
        request={"user_id": user_id}, response=result, source_ip=ip, user_agent=ua,
    )
    return result


# ── unified users + admin toggle (用户与权限 页) ────────────────────────────────


@router.get("/users/all")
async def list_all_users(
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """一张全量用户表，每行标注是否管理员 / 是否受保护的首个管理员。"""
    return await team_users_service.list_all_users(team_id)


@router.post("/users")
async def create_user(
    body: CreateUserReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        result = await team_users_service.create_user(
            team_id, body.email, body.name, body.is_admin
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="邮箱格式不正确") from exc
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.create",
        request=body.model_dump(), response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/users/{user_id}/admin")
async def set_user_admin(
    user_id: str,
    body: SetAdminReq,
    request: Request,
    user: User = Depends(require_team_admin),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        result = await team_users_service.set_admin(team_id, user_id, body.is_admin)
    except ValueError as exc:
        reason = str(exc)
        if reason == "protected_first_admin":
            raise HTTPException(status_code=403, detail="不能取消第一个管理员的管理员权限") from exc
        raise HTTPException(status_code=404, detail="用户不存在") from exc
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.set_admin",
        request={"user_id": user_id, "is_admin": body.is_admin},
        response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/users/{user_id}")
async def update_user(
    user_id: str,
    body: UpdateUserReq,
    request: Request,
    user: User = Depends(require_team_admin),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        result = await team_users_service.update_user(
            user_id, name=body.name, is_blocked=body.is_blocked
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="用户不存在") from exc
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "user.update",
        request={"user_id": user_id, **body.model_dump(exclude_none=True)},
        response=result, source_ip=ip, user_agent=ua,
    )
    return result


# ── groups ────────────────────────────────────────────────────────────────────


@router.get("/groups")
async def list_groups(
    team_id: str = Depends(get_current_team_id),
) -> dict:
    return await team_users_service.list_groups(team_id)


@router.post("/groups")
async def create_group(
    body: GroupReq,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        return await team_users_service.create_group(team_id, body.name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="分组名称不能为空") from exc


@router.put("/groups/{group_id}")
async def rename_group(
    group_id: str,
    body: GroupReq,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        return await team_users_service.rename_group(team_id, group_id, body.name)
    except ValueError as exc:
        reason = str(exc)
        if reason == "group_not_found":
            raise HTTPException(status_code=404, detail="分组不存在") from exc
        raise HTTPException(status_code=400, detail="分组名称不能为空") from exc


@router.delete("/groups/{group_id}")
async def delete_group(
    group_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    result = await team_users_service.delete_group(team_id, group_id)
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "group.delete",
        request={"group_id": group_id}, response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/groups/{group_id}/users")
async def set_group_users(
    group_id: str,
    body: SetGroupUsersReq,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        return await team_users_service.set_group_users(team_id, group_id, body.user_ids)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="分组不存在") from exc


# ── group → API Key 授权（权限统一在分组侧配，不再从 key 侧配「适用分组」） ──────
#
# 语义：给一个分组勾选它能用哪些系统 Key。写入落到各 key 的 api_keys.group_ids
# （key↔分组多对多），运行时仍由 resolve_system_api_key_for_user 按用户所属分组
# 的并集解析。只增删本分组这一项，不动 key 上的其它分组。


@router.get("/groups/{group_id}/api-keys")
async def list_group_api_keys(
    group_id: str,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """列出所有系统 Key（脱敏），标记哪些已绑定本分组。供分组配置页勾选。"""
    from db import PostgresClient

    # 归属校验：group 必须属于当前 team，否则传别团 group_id 能看到其 Key 绑定。
    if await team_users_service._owned_group(team_id, group_id) is None:
        raise HTTPException(status_code=404, detail="分组不存在或无权访问")
    keys = await PostgresClient.list_api_keys_brief(group_id)
    return {"keys": keys}


@router.put("/groups/{group_id}/api-keys")
async def set_group_api_keys(
    group_id: str,
    body: SetGroupApiKeysReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """设置本分组能用的系统 Key 集合。只增删本分组这一项，不影响 key 的其它分组。"""
    from db import PostgresClient
    import config

    if await team_users_service._owned_group(team_id, group_id) is None:
        raise HTTPException(status_code=404, detail="分组不存在或无权访问")
    await PostgresClient.set_group_api_keys(group_id, body.key_ids)
    await config.Config.refresh_api_keys_cache()
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "group.set_api_keys",
        request={"group_id": group_id, "key_ids": body.key_ids},
        response={"ok": True}, source_ip=ip, user_agent=ua,
    )
    return {"ok": True}


# ── group → internal MCP service authorization ───────────────────────────────


@router.get("/groups/{group_id}/mcp-services")
async def list_group_mcp_services(
    group_id: str,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    from mcp_plugin_store import list_services_for_group

    if await team_users_service._owned_group(team_id, group_id) is None:
        raise HTTPException(status_code=404, detail="分组不存在")
    return {"services": await list_services_for_group(group_id)}


@router.put("/groups/{group_id}/mcp-services")
async def set_group_mcp_services(
    group_id: str,
    body: SetGroupMcpServicesReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    from mcp_plugin_store import set_group_services

    if await team_users_service._owned_group(team_id, group_id) is None:
        raise HTTPException(status_code=404, detail="分组不存在")
    await set_group_services(group_id, body.service_ids)
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "group.set_mcp_services",
        request={"group_id": group_id, "service_ids": body.service_ids},
        response={"ok": True}, source_ip=ip, user_agent=ua,
    )
    return {"ok": True}


# ── audits ────────────────────────────────────────────────────────────────────


@router.get("/skills")
async def list_team_skills(team_id: str = Depends(get_current_team_id)) -> dict:
    return {"skills": await team_users_service.list_team_skills(team_id)}


@router.get("/groups/{group_id}/skills")
async def list_group_skills(
    group_id: str,
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        return {"skills": await team_users_service.list_group_skills(team_id, group_id)}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="分组不存在") from exc


@router.put("/groups/{group_id}/skills")
async def set_group_skills(
    group_id: str,
    body: SetGroupSkillsReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        result = await team_users_service.set_group_skills(team_id, group_id, body.skill_ids)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="分组不存在") from exc
    ip, ua = _client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "group.set_skills",
        request={"group_id": group_id, "skill_ids": body.skill_ids},
        response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.get("/audits")
async def list_audits(
    cursor: str | None = Query(default=None),
    limit: int = Query(default=20),
    operation: str | None = Query(default=None),
    user_id: str | None = Query(default=None),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    return await team_users_service.list_audits(
        team_id,
        cursor=cursor,
        limit=limit,
        operation=operation,
        user_id=user_id,
    )
