"""Team login-methods routes.

The team-admin surface lives under ``/api/v1/teams/*``; every path is written in
full and no ``prefix`` is set on the router. All handlers are storage-side only;
the test endpoint issues a minimal outbound HTTP probe (reachability only).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from .deps import client_meta, get_current_team_id, get_current_user
from .models import User
from .team_models_service import team_models_service
from .team_users_service import team_users_service

router = APIRouter(tags=["monkeycode-team-models"])


# ── models ────────────────────────────────────────────────────────────────────


@router.get("/api/v1/teams/models")
async def list_team_models(team_id: str = Depends(get_current_team_id)) -> dict:
    return await team_models_service.list_models(team_id)


@router.post("/api/v1/teams/models")
async def create_team_model(
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    body = await request.json()
    result = await team_models_service.create_model(team_id, body)
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "model.create",
        request=body, response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/api/v1/teams/models/{model_id}")
async def update_team_model(
    model_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    body = await request.json()
    result = await team_models_service.update_model(team_id, model_id, body)
    if result is None:
        raise HTTPException(status_code=404, detail="模型不存在或无权访问")
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "model.update",
        request={"model_id": model_id, **body} if isinstance(body, dict) else body,
        response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.delete("/api/v1/teams/models/{model_id}")
async def delete_team_model(
    model_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    ok = await team_models_service.delete_model(team_id, model_id)
    if not ok:
        raise HTTPException(status_code=404, detail="模型不存在或无权访问")
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "model.delete",
        request={"model_id": model_id}, response={"deleted": True},
        source_ip=ip, user_agent=ua,
    )
    return {}


@router.post("/api/v1/teams/models/health-check")
async def health_check_team_model(
    request: Request, team_id: str = Depends(get_current_team_id)
) -> dict:
    body = await request.json()
    return await team_models_service.health_check_adhoc(body)


@router.get("/api/v1/teams/models/{model_id}/health-check")
async def health_check_stored_team_model(
    model_id: str, team_id: str = Depends(get_current_team_id)
) -> dict:
    result = await team_models_service.health_check_stored(team_id, model_id)
    if result is None:
        raise HTTPException(status_code=404, detail="模型不存在或无权访问")
    return result


# ── login methods ─────────────────────────────────────────────────────────────


@router.get("/api/v1/teams/login-methods")
async def list_login_methods(
    request: Request, team_id: str = Depends(get_current_team_id)
) -> dict:
    return await team_models_service.list_methods(
        team_id, base_url=str(request.base_url).rstrip("/")
    )


@router.post("/api/v1/teams/login-methods")
async def create_login_method(
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    body = await request.json()
    try:
        result = await team_models_service.create_method(
            team_id, body, base_url=str(request.base_url).rstrip("/")
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "login_method.create",
        request=body, response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.put("/api/v1/teams/login-methods/{method_id}")
async def update_login_method(
    method_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    body = await request.json()
    try:
        result = await team_models_service.update_method(
            team_id, method_id, body, base_url=str(request.base_url).rstrip("/")
        )
    except ValueError as exc:
        code = str(exc)
        status = 404 if code == "method_not_found" else 400
        raise HTTPException(status_code=status, detail=code) from exc
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "login_method.update",
        request={"method_id": method_id, **body} if isinstance(body, dict) else body,
        response=result, source_ip=ip, user_agent=ua,
    )
    return result


@router.delete("/api/v1/teams/login-methods/{method_id}")
async def delete_login_method(
    method_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    try:
        await team_models_service.delete_method(team_id, method_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    ip, ua = client_meta(request)
    await team_users_service.record_audit(
        team_id, str(user.id), "login_method.delete",
        request={"method_id": method_id}, response={"deleted": True},
        source_ip=ip, user_agent=ua,
    )
    return {"ok": True}


@router.post("/api/v1/teams/login-methods/test")
async def test_login_method(
    request: Request, team_id: str = Depends(get_current_team_id)
) -> dict:
    body = await request.json()
    return await team_models_service.test_method(body)
