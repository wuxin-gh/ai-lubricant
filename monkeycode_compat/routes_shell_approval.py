"""C-side user shell-approval policy routes (``/api/v1/users/shell-approval``).

Each user manages their own auto-allow list for the Agent ``node_shell_exec``
approval card. ``GET /catalog`` returns the static preset catalog (per shell);
``GET /`` returns the caller's checked items; ``PUT /{command_key}`` /
``DELETE /{command_key}`` toggle one item (body carries the shell flavor); a
custom command is validated to a single safe segment before being normalized
and stored, so dangerous structures (redirects, substitution, script blocks)
can never be auto-allowed.

Scope is the calling user only (read via ``get_current_user``); admin-proxy
turns (``caller=None``) never read a policy. Returns plain dicts — the frontend
endpoint map wraps them in the response envelope, matching the other user-side
routers.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .deps import get_current_user
from .models import User
from .node_client.tools import PRESET_APPROVAL_CATALOG, _segment_command_key, _split_shell_command, _tokens
from . import shell_approval_service

router = APIRouter(prefix="/api/v1/users/shell-approval", tags=["monkeycode-user-shell-approval"])

_SHELL_FLAVORS = ("posix", "powershell", "cmd", "unknown")
_MAX_KEY_LEN = 64
_MAX_NOTE_LEN = 255


class PutPolicyReq(BaseModel):
    shell: str = Field(..., description="posix / powershell / cmd / unknown")
    note: str | None = None


def _normalize_shell(value: str) -> str:
    flavor = (value or "").strip().lower()
    if flavor not in _SHELL_FLAVORS:
        raise HTTPException(status_code=422, detail="shell 必须为 posix/powershell/cmd/unknown")
    return flavor


def _validate_command_key(raw: str, shell_flavor: str) -> str:
    """Normalize a user-supplied command to a safe single auto-allow key.

    Custom input must reduce to EXACTLY one provably-safe tokenizable segment:
    shell connectors (``;``/``|``/``&``), redirects (``>``/``<``), command
    substitution (``$()``/backticks), and PowerShell script blocks (``{}``/``@``)
    all make ``_split_shell_command`` return None or >1 segment, which we reject
    as 400 — they can never be safely auto-allowed. Preset keys also pass
    through here (constants we authored, so they always tokenize cleanly).
    """
    text = (raw or "").strip()
    if not text:
        raise HTTPException(status_code=422, detail="command_key 不能为空")
    if len(text) > _MAX_KEY_LEN:
        raise HTTPException(status_code=422, detail="command_key 过长")
    segments = _split_shell_command(text, shell_flavor)
    if not segments or len(segments) != 1:
        raise HTTPException(
            status_code=400,
            detail="无法安全归一化该命令：含命令连接、重定向、命令替换或脚本块等结构，不能加入免审。",
        )
    # Re-tokenize the lone segment; require it to actually parse to tokens.
    if not _tokens(segments[0], shell_flavor):
        raise HTTPException(status_code=400, detail="无法解析该命令为单一可执行命令token。")
    key = _segment_command_key(segments[0], shell_flavor)
    if not key:
        raise HTTPException(
            status_code=400,
            detail="无法安全归一化该命令：含重定向、管道、命令替换或脚本块等结构，不能加入免审。",
        )
    return key


@router.get("/catalog")
async def get_catalog(_: User = Depends(get_current_user)) -> dict:
    """Return the static preset catalog grouped by shell flavor."""
    grouped: dict[str, list[dict]] = {flavor: [] for flavor in _SHELL_FLAVORS}
    for item in PRESET_APPROVAL_CATALOG:
        grouped.setdefault(item["shell"], []).append(item)
    return {"catalog": grouped}


@router.get("")
async def list_my_policy(user: User = Depends(get_current_user)) -> dict:
    rows = await shell_approval_service.list_user_policy(str(user.id))
    return {"policies": rows}


@router.put("/{command_key}")
async def put_my_policy(
    command_key: str,
    body: PutPolicyReq,
    user: User = Depends(get_current_user),
) -> dict:
    shell = _normalize_shell(body.shell)
    note = (body.note or "").strip() or None
    if len(body.note or "") > _MAX_NOTE_LEN:
        raise HTTPException(status_code=422, detail="note 过长")
    # Always re-validate the key (works for both preset and custom): rejects
    # connectors/redirects/substitution as a single safe segment.
    key = _validate_command_key(command_key, shell)
    return await shell_approval_service.upsert_policy(str(user.id), key, shell, note=note)


@router.delete("/{command_key}")
async def delete_my_policy(
    command_key: str,
    shell: str,
    user: User = Depends(get_current_user),
) -> dict:
    flavor = _normalize_shell(shell)
    key = (command_key or "").strip().lower()
    if not key:
        raise HTTPException(status_code=422, detail="command_key 不能为空")
    deleted = await shell_approval_service.delete_policy(str(user.id), key, flavor)
    return {"deleted": deleted}


@router.delete("")
async def clear_my_policy(user: User = Depends(get_current_user)) -> dict:
    count = await shell_approval_service.clear_policy(str(user.id))
    return {"deleted": count}
