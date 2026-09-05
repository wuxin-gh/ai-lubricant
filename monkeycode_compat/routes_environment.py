"""User-facing task-environment routes.

An environment is what a user picks before creating a task: it decides which HOME
the editor runs against and, for the shared tier, carries a reusable skill/MCP/
plugin set. These routes are the *user* surface (``/api/v1/teams/environments``) —
environments are user-owned, not administered from the platform console.

Only the shared tier has rows: ``system`` and ``isolated`` are resolved from the
tier name at dispatch time, and ``isolated`` is the initial clean-slate
environment with nothing to manage.

Editing an environment's resources is a real install/uninstall on the node
(``POST .../sync``). A *task* deselecting a resource only changes what that run
activates and never touches these rows — see ``task_service._build_session``.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from . import environment_service
from .deps import get_current_team_id, get_current_user
from .environment_service import EnvironmentError
from .models import User
from . import system_env_service

router = APIRouter(prefix="/api/v1/teams", tags=["monkeycode-environments"])


_STATUS_BY_CODE = {
    "invalid_argument": 400,
    "permission_denied": 403,
    "not_found": 404,
    "conflict": 409,
    "failed_precondition": 412,
}


async def _guard(coro):
    """Map domain errors onto HTTP status codes.

    Node-transport failures are surfaced as 503 rather than 500: an offline node
    is an expected operational state for environment work (the ledger still
    holds the configuration), not a server bug.
    """
    from .node_client import NodeServerUnavailable
    from .node_client.errors import RPCError

    try:
        return await coro
    except EnvironmentError as exc:
        raise HTTPException(status_code=_STATUS_BY_CODE.get(exc.code, 400), detail=exc.message)
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"节点控制面不可用：{exc}")
    except RPCError as exc:
        raise HTTPException(status_code=502, detail=f"节点调用失败：{exc}")


class CreateEnvReq(BaseModel):
    node_id: str
    name: str
    description: str | None = None


class UpdateEnvReq(BaseModel):
    name: str | None = None
    description: str | None = None


class AddEnvResourceReq(BaseModel):
    kind: str
    resource_id: str


@router.get("/environments")
async def list_environments(
    node_id: str | None = None, user: User = Depends(get_current_user)
) -> dict:
    """The caller's shared environments, optionally narrowed to one node.

    The task-creation flow passes ``node_id``: an environment's HOME lives on the
    node it was created on, so environments from other nodes cannot host the task.
    """
    return {
        "environments": await _guard(
            environment_service.list_environments(str(user.id), node_id=node_id or "")
        )
    }


@router.post("/environments")
async def create_environment(body: CreateEnvReq, user: User = Depends(get_current_user)) -> dict:
    """Create a shared environment (id is database-generated)."""
    return await _guard(
        environment_service.create_environment(
            str(user.id),
            node_id=body.node_id,
            name=body.name,
            description=body.description,
        )
    )


@router.get("/environments/{env_id}")
async def get_environment(env_id: str, user: User = Depends(get_current_user)) -> dict:
    """Environment + configured resource set + node inventory, already diffed.

    Each configured skill/plugin is ``installed`` or ``pending``; things the node
    reported with no configured counterpart are ``extra`` (installed by hand).
    MCP rows are ``config`` — never on disk, resolved per task.
    """
    return await _guard(environment_service.environment_detail(str(user.id), env_id))


@router.patch("/environments/{env_id}")
async def update_environment(
    env_id: str, body: UpdateEnvReq, user: User = Depends(get_current_user)
) -> dict:
    """Rename / re-describe. The id (and the node-side directory) never moves."""
    return await _guard(
        environment_service.update_environment(
            str(user.id), env_id, name=body.name, description=body.description
        )
    )


@router.delete("/environments/{env_id}")
async def delete_environment(env_id: str, user: User = Depends(get_current_user)) -> dict:
    """Delete the environment and its node-side directory."""
    return await _guard(environment_service.delete_environment(str(user.id), env_id))


@router.get("/environments/{env_id}/resources")
async def list_environment_resources(env_id: str, user: User = Depends(get_current_user)) -> dict:
    return {
        "resources": await _guard(
            environment_service.list_environment_resources(str(user.id), env_id)
        )
    }


@router.post("/environments/{env_id}/resources")
async def add_environment_resource(
    env_id: str,
    body: AddEnvResourceReq,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Add a granted skill/MCP/plugin to the environment's configured set.

    Bumps the environment revision, so the page shows 待同步 until the next sync
    actually installs it on the node.
    """
    return await _guard(
        environment_service.add_environment_resource(
            str(user.id), env_id, kind=body.kind, resource_id=body.resource_id, team_id=team_id
        )
    )


@router.delete("/environments/{env_id}/resources/{entry_id}")
async def remove_environment_resource(
    env_id: str, entry_id: str, user: User = Depends(get_current_user)
) -> dict:
    """Remove a resource from the environment (uninstalled on the next sync)."""
    return await _guard(
        environment_service.remove_environment_resource(str(user.id), env_id, entry_id)
    )


@router.post("/environments/{env_id}/sync")
async def sync_environment(
    env_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Install the configured set onto the node — the real install/uninstall.

    ``request_base_url`` is needed because a mirrored skill resolves to a
    server-hosted archive URL the node fetches back from us.
    """
    base = str(request.base_url).rstrip("/")
    return await _guard(
        environment_service.sync_environment(
            str(user.id), env_id, team_id=team_id, request_base_url=base
        )
    )


@router.post("/environments/{env_id}/inventory")
async def refresh_environment_inventory(
    env_id: str, user: User = Depends(get_current_user)
) -> dict:
    """Re-read what the node has on disk for this environment."""
    return {
        "installed": await _guard(
            environment_service.refresh_environment_inventory(str(user.id), env_id)
        )
    }


# ── system environment (the node operator's real HOME) ────────────────────────
# Node-addressed, not environment-addressed: the system tier has no environment
# row, so these hang off /nodes/{node_id} instead of /environments/{env_id}.
# Semantics differ from the shared tier on purpose — see system_env_service's
# module docstring (read-first, incremental writes, ownership-scoped removal).


class InstallSystemEnvReq(BaseModel):
    kind: str
    resource_id: str
    # Whether to replace an entry that already exists on disk. Defaults to false
    # because the existing copy may be the operator's own work; the UI asks.
    overwrite: bool = False


class ArchiveSystemEnvReq(BaseModel):
    kind: str
    name: str


async def _guard_system_env(coro):
    """Same mapping as :func:`_guard`, for :class:`SystemEnvError` codes."""
    from .node_client import NodeServerUnavailable
    from .node_client.errors import RPCError
    from .system_env_service import SystemEnvError

    try:
        return await coro
    except SystemEnvError as exc:
        raise HTTPException(status_code=_STATUS_BY_CODE.get(exc.code, 400), detail=exc.message)
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"节点控制面不可用：{exc}")
    except RPCError as exc:
        raise HTTPException(status_code=502, detail=f"节点调用失败：{exc}")


@router.get("/nodes/{node_id}/system-env")
async def get_system_env(node_id: str, user: User = Depends(get_current_user)) -> dict:
    """The stored inventory of the node operator's HOME, grouped by kind.

    Reads the snapshot, not the node, so the panel opens instantly and still shows
    the last known state while the node is offline.
    """
    return await _guard_system_env(system_env_service.system_env_detail(str(user.id), node_id))


@router.post("/nodes/{node_id}/system-env/refresh")
async def refresh_system_env(
    node_id: str, provider: str | None = None, user: User = Depends(get_current_user)
) -> dict:
    """Re-scan the operator's HOME on the node and replace the stored snapshot."""
    return {
        "installed": await _guard_system_env(
            system_env_service.refresh_system_env(
                str(user.id), node_id, provider=provider or ""
            )
        )
    }


@router.post("/nodes/{node_id}/system-env/install")
async def install_system_env_resource(
    node_id: str,
    body: InstallSystemEnvReq,
    request: Request,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Install one granted platform resource into the operator's HOME.

    Incremental: nothing else on disk is touched, and an existing same-named entry
    is skipped unless ``overwrite`` — the response reports which happened.
    """
    base = str(request.base_url).rstrip("/")
    return {
        "touched": await _guard_system_env(
            system_env_service.install_to_system_env(
                str(user.id),
                node_id,
                kind=body.kind,
                resource_id=body.resource_id,
                overwrite=body.overwrite,
                team_id=team_id,
                request_base_url=base,
            )
        )
    }


@router.post("/nodes/{node_id}/system-env/archive")
async def archive_system_env_resource(
    node_id: str,
    body: ArchiveSystemEnvReq,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> dict:
    """Archive a locally installed resource into the platform library.

    The node packs the directory and uploads it; the server stores it as a mirror
    and creates a team reference, making the resource reusable on other nodes and
    in shared environments.
    """
    return await _guard_system_env(
        system_env_service.archive_system_env_resource(
            str(user.id), node_id, kind=body.kind, name=body.name, team_id=team_id
        )
    )


@router.delete("/nodes/{node_id}/system-env/{kind}/{name}")
async def remove_system_env_resource(
    node_id: str, kind: str, name: str, user: User = Depends(get_current_user)
) -> dict:
    """Node-level uninstall: delete the files and drop the platform-side record.

    Only platform-installed entries qualify; the node refuses anything absent from
    its own manifest, so an operator's hand-installed resource can be listed but
    never removed here.
    """
    return await _guard_system_env(
        system_env_service.remove_from_system_env(str(user.id), node_id, kind=kind, name=name)
    )
