"""C-side user model routes (``/api/v1/users/models``).

The vendored upstream frontend (mobile + console data-provider) calls these
under the ``/api/v1/users/*`` prefix, while the storage/service layer is shared
with the team-admin surface (``/api/v1/teams/models``). Rather than fork the
logic, these routes are thin delegations onto the existing
``team_models_service`` scoped to the caller's team.

Image routes were removed: the task runtime is now a chosen agent-compose node,
not a server-defined image (see routes_nodes / routes_task).

Storage only; never touches ``/admin/*`` or the ``/v1/*`` model pipeline.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from .deps import get_current_team_id
from .team_models_service import team_models_service

models_router = APIRouter(prefix="/api/v1/users/models", tags=["user-platform-user-models"])


# ── models ──────────────────────────────────────────────────────────────────


@models_router.get("")
async def list_user_models(team_id: str = Depends(get_current_team_id)) -> dict:
    return await team_models_service.list_models(team_id)


@models_router.get("/available")
async def list_available_user_models(team_id: str = Depends(get_current_team_id)) -> dict:
    """Available models for the caller. We surface the configured team models;
    the frontend filters hidden ones. Returns the same ``{models:[...]}`` shape."""
    return await team_models_service.list_models(team_id)


@models_router.post("")
async def create_user_model(
    request: Request, team_id: str = Depends(get_current_team_id)
) -> dict:
    body = await request.json()
    return await team_models_service.create_model(team_id, body)


@models_router.post("/health-check")
async def health_check_user_model(
    request: Request, _team_id: str = Depends(get_current_team_id)
) -> dict:
    body = await request.json()
    return await team_models_service.health_check_adhoc(body)


@models_router.put("/{model_id}")
async def update_user_model(
    model_id: str, request: Request, team_id: str = Depends(get_current_team_id)
) -> dict:
    body = await request.json()
    result = await team_models_service.update_model(team_id, model_id, body)
    if result is None:
        raise HTTPException(status_code=404, detail="模型不存在或无权访问")
    return result


@models_router.delete("/{model_id}")
async def delete_user_model(
    model_id: str, team_id: str = Depends(get_current_team_id)
) -> dict:
    ok = await team_models_service.delete_model(team_id, model_id)
    if not ok:
        raise HTTPException(status_code=404, detail="模型不存在或无权访问")
    return {}


@models_router.get("/{model_id}/health-check")
async def health_check_stored_user_model(
    model_id: str, team_id: str = Depends(get_current_team_id)
) -> dict:
    result = await team_models_service.health_check_stored(team_id, model_id)
    if result is None:
        raise HTTPException(status_code=404, detail="模型不存在或无权访问")
    return result
