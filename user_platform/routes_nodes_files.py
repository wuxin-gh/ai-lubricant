"""Node host file browser backed by the structured host exec channel.

Two surfaces over one implementation:

* **Admin** ``/api/v1/admin/nodes/{node_id}/files*`` — platform administrators,
  any node in any state (an offline node can still be inspected for triage).
* **User** ``/api/v1/teams/my-nodes/{node_id}/files*`` — the signed-in user may
  only touch a node one of their groups was granted, and only while that node is
  an approved, online, real client. This mirrors the user terminal WebSocket
  gate in :mod:`.routes_nodes_terminal` — the file browser lives inside that
  terminal, so it must not be reachable where the terminal itself is refused.

The path parsing, command construction and response decoding are shared; only
the authorization differs. Writes on the user surface are audited.
"""
from __future__ import annotations

import base64
import binascii
import json
import ntpath
import posixpath
import re
import shlex
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field

from .deps import client_meta, get_current_user
from .models import User
from .node_client import get_local_node_client

router = APIRouter(prefix="/api/v1/admin/nodes", tags=["user-platform-nodes-admin"])
team_router = APIRouter(prefix="/api/v1/teams/my-nodes", tags=["user-platform-nodes-team"])

# File operations that change the node host filesystem — audited on the user
# surface (reads are not: listing a directory is not a state change).
_MUTATING_OPS = {"mkdir", "write", "rename", "delete"}

_WINDOWS_PATH_RE = re.compile(r"^/([A-Za-z]):(?:/(.*))?$")
_MAX_READ_BYTES = 1024 * 1024
_MAX_WINDOWS_WRITE_BYTES = 1024
_MAX_UPLOAD_BYTES = 10 * 1024 * 1024


class HostFileRequest(BaseModel):
    path: str = "/"
    operation: str = "list"
    content: str = Field(default="", max_length=2_000_000)
    destination: str = ""


def _path(value: str) -> str:
    raw = (value or "/").replace("\\", "/")
    if "\x00" in raw or any(part == ".." for part in raw.split("/")):
        raise HTTPException(status_code=400, detail="非法路径")
    clean = posixpath.normpath("/" + raw.lstrip("/"))
    if clean == "." or clean.startswith("/../") or clean == "/..":
        raise HTTPException(status_code=400, detail="非法路径")
    return clean


def _windows_path(value: str) -> tuple[str, str | None]:
    raw = value or "/"
    if "\x00" in raw or "\\" in raw or any(part == ".." for part in raw.split("/")):
        raise HTTPException(status_code=400, detail="非法 Windows 路径")
    if raw == "/":
        return "/", None
    match = _WINDOWS_PATH_RE.fullmatch(raw)
    if match is None:
        raise HTTPException(status_code=400, detail="Windows 路径必须为 /C:/... 格式")
    drive = match.group(1).upper()
    raw_tail_parts = (match.group(2) or "").split("/")
    tail_parts = [part for part in raw_tail_parts if part not in ("", ".")]
    if any(
        ":" in part or part.rstrip(". ") in {"", ".", ".."} or part != part.rstrip(". ")
        for part in tail_parts
    ):
        raise HTTPException(status_code=400, detail="非法 Windows 路径")
    virtual = f"/{drive}:" + (f"/{'/'.join(tail_parts)}" if tail_parts else "")
    native = f"{drive}:\\" + "\\".join(tail_parts)
    return virtual, native


def _native_to_windows_path(value: str) -> str:
    drive, tail = ntpath.splitdrive(value)
    if not re.fullmatch(r"[A-Za-z]:", drive) or not tail.startswith(("\\", "/")):
        raise ValueError("invalid Windows absolute path")
    parts = [part for part in tail.replace("\\", "/").split("/") if part]
    return f"/{drive[0].upper()}:" + (f"/{'/'.join(parts)}" if parts else "")


def _ps_data(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _powershell_command(body: str) -> str:
    prelude = (
        "$ErrorActionPreference='Stop';$ProgressPreference='SilentlyContinue';"
        "$utf8=[Text.UTF8Encoding]::new($false);"
        "$OutputEncoding=$utf8;[Console]::OutputEncoding=$utf8;"
    )
    encoded = base64.b64encode((prelude + body).encode("utf-16le")).decode("ascii")
    return f"powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand {encoded}"


def _ps_path_assignment(native_path: str, name: str = "p") -> str:
    return (
        f"${name}=[Text.Encoding]::UTF8.GetString("
        f"[Convert]::FromBase64String('{_ps_data(native_path)}'));"
    )


def _windows_command(op: str, native_path: str | None, content: str, destination: str | None) -> str:
    if op == "list" and native_path is None:
        return _powershell_command(
            "$entries=@(Get-PSDrive -PSProvider FileSystem | ForEach-Object {"
            "[pscustomobject]@{name=($_.Name+':');path=($_.Name+':\\');is_dir=$true;size=0}"
            "});[pscustomobject]@{entries=$entries}|ConvertTo-Json -Depth 4 -Compress"
        )
    if native_path is None:
        raise HTTPException(status_code=400, detail="不能修改文件系统根目录")

    assign = _ps_path_assignment(native_path)
    if op == "list":
        body = assign + (
            "if (-not (Test-Path -LiteralPath $p -PathType Container)){throw 'directory not found'};"
            "$entries=@(Get-ChildItem -LiteralPath $p -Force | ForEach-Object {"
            "[pscustomobject]@{name=$_.Name;path=$_.FullName;is_dir=$_.PSIsContainer;"
            "size=$(if ($_.PSIsContainer){0}else{$_.Length})}"
            "});[pscustomobject]@{entries=$entries}|ConvertTo-Json -Depth 4 -Compress"
        )
    elif op == "read":
        body = assign + (
            "if (-not (Test-Path -LiteralPath $p -PathType Leaf)){throw 'file not found'};"
            "$s=[IO.File]::OpenRead($p);try{$b=New-Object byte[] 1048576;"
            "$n=$s.Read($b,0,$b.Length);$data=[Convert]::ToBase64String($b,0,$n);"
            "$truncated=$s.Length -gt 1048576}finally{$s.Dispose()};"
            "[pscustomobject]@{data=$data;truncated=$truncated}|ConvertTo-Json -Compress"
        )
    elif op == "mkdir":
        body = assign + "[IO.Directory]::CreateDirectory($p)|Out-Null"
    elif op == "write":
        body = assign + f"$data=[Convert]::FromBase64String('{_ps_data(content)}');[IO.File]::WriteAllBytes($p,$data)"
    elif op == "rename":
        if destination is None:
            raise HTTPException(status_code=400, detail="缺少目标路径")
        body = assign + _ps_path_assignment(destination, "d") + "Move-Item -LiteralPath $p -Destination $d"
    else:
        body = assign + "if (-not (Test-Path -LiteralPath $p)){throw 'path not found'};Remove-Item -LiteralPath $p -Recurse -Force"
    return _powershell_command(body)


def _host_exec_error(result: dict[str, Any]) -> str | None:
    error = str(result.get("error") or "").strip()
    if error:
        return error
    exit_code = result.get("exitCode", result.get("exit_code", 0))
    try:
        failed_exit = int(exit_code) != 0
    except (TypeError, ValueError):
        return "节点返回了无效退出码"
    if failed_exit or ("success" in result and result.get("success") is False):
        return str(result.get("stderr") or "文件操作失败")
    return None


def _json_output(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("stdoutTruncated") or result.get("stdout_truncated"):
        raise HTTPException(status_code=502, detail="节点文件操作响应被截断")
    try:
        payload = json.loads(result.get("stdout") or "")
    except (TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail="节点返回了无效文件数据") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="节点返回了无效文件数据")
    return payload


def _windows_entries(result: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _json_output(result)
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise HTTPException(status_code=502, detail="节点返回了无效目录数据")
    entries = []
    for item in raw_entries:
        if not isinstance(item, dict):
            raise HTTPException(status_code=502, detail="节点返回了无效目录项")
        try:
            entry_path = _native_to_windows_path(str(item.get("path") or ""))
            size = int(item.get("size") or 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=502, detail="节点返回了无效目录项") from exc
        entries.append({
            "name": str(item.get("name") or entry_path.rsplit("/", 1)[-1]),
            "path": entry_path,
            "is_dir": bool(item.get("is_dir")),
            "size": size,
        })
    entries.sort(key=lambda item: (not item["is_dir"], item["name"].lower()))
    return entries


def _require_admin(user: User = Depends(get_current_user)) -> User:
    if user is None or user.role != "admin" or user.is_deleted or user.is_blocked or user.status != "active":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


async def _require_node_access(node_id: str, user: User = Depends(get_current_user)) -> User:
    """User-surface gate: a granted node, in a state that supports host access.

    Same two checks the user terminal WebSocket makes (see
    ``routes_nodes_terminal.user_node_terminal``): the caller must hold a
    GroupNode grant for this node, and the node must be an approved, online,
    real client. A grouping container (``is_passive``) has no host to browse.

    Kept intentionally strict — unlike the admin surface, a user may not reach
    into an offline or unapproved node.
    """
    from .nodes_service import nodes_service

    if await nodes_service.user_can_use_node(str(user.id), node_id) is None:
        raise HTTPException(status_code=403, detail="无权访问该节点")
    live = (await nodes_service._live_node_map()).get(node_id)
    if live is None:
        raise HTTPException(status_code=404, detail="节点不存在")
    online = live.get("online") if "online" in live else live.get("connected")
    if live.get("is_passive") or live.get("status") != "approved" or not online:
        raise HTTPException(status_code=409, detail="节点未审批、离线或不支持文件浏览")
    return user


async def _audit_user_file_op(
    request: Request,
    user: User,
    node_id: str,
    operation: str,
    payload: dict[str, Any],
) -> None:
    """Record a user-surface host filesystem write (best-effort).

    Mirrors the terminal audit: host-grade actions are always recorded, and a
    failure here never fails an operation that already happened.
    """
    try:
        from .deps import resolve_team_id
        from .team_users_service import team_users_service

        ip, ua = client_meta(request)
        await team_users_service.record_audit(
            await resolve_team_id(str(user.id)),
            str(user.id),
            f"node.file_{operation}",
            request={"node_id": node_id, **payload},
            response={"ok": True},
            source_ip=ip,
            user_agent=ua,
        )
    except Exception:  # noqa: BLE001 - audit must never break the file action
        return


async def _node_is_windows(node_id: str) -> tuple[Any, bool]:
    """Resolve the live node client + whether the target node runs Windows."""
    client = get_local_node_client()
    try:
        nodes = await client.list_nodes()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    node = next((item for item in nodes if item.get("node_id") == node_id), None)
    if node is None:
        raise HTTPException(status_code=404, detail="节点不存在")
    return client, str((node.get("capabilities") or {}).get("os") or "").lower() == "windows"


async def _do_host_file(node_id: str, req: HostFileRequest) -> dict[str, Any]:
    """Run one host file operation. Authorization is the caller's job."""
    op = (req.operation or "list").strip().lower()
    if op not in {"home", "list", "read", "mkdir", "write", "rename", "delete"}:
        raise HTTPException(status_code=400, detail="不支持的文件操作")

    client, is_windows = await _node_is_windows(node_id)

    if is_windows:
        if op == "home":
            path, native_path = "/", None
            command = _powershell_command("[Console]::Out.Write((Get-Location).Path)")
            cwd = ""
        else:
            path, native_path = _windows_path(req.path)
            destination = None
            if op == "rename":
                _, destination = _windows_path(req.destination)
                if destination is None:
                    raise HTTPException(status_code=400, detail="不能重命名到文件系统根目录")
            if op in {"write", "mkdir", "rename", "delete"} and native_path is None:
                raise HTTPException(status_code=400, detail="不能修改文件系统根目录")
            if op in {"rename", "delete"} and path.count("/") == 1:
                raise HTTPException(status_code=400, detail="不能修改驱动器根目录")
            if op == "write" and len(req.content.encode("utf-8")) > _MAX_WINDOWS_WRITE_BYTES:
                raise HTTPException(status_code=400, detail="Windows 节点单次写入最多支持 1 KiB")
            command = _windows_command(op, native_path, req.content, destination)
            cwd = ""
    else:
        if op == "home":
            path = "/"
            command = "printf '%s' \"$HOME\""
        else:
            path = _path(req.path)
            qpath = shlex.quote(path)
            if op == "list":
                command = f"if [ -d {qpath} ]; then find {qpath} -mindepth 1 -maxdepth 1 -printf '%y\\t%s\\t%p\\n'; else exit 2; fi"
            elif op == "read":
                command = f"if [ -f {qpath} ]; then head -c {_MAX_READ_BYTES} {qpath}; else exit 2; fi"
            elif op == "mkdir":
                command = f"mkdir -p {qpath}"
            elif op == "write":
                encoded = base64.b64encode(req.content.encode("utf-8")).decode("ascii")
                command = f"printf %s {shlex.quote(encoded)} | base64 -d > {qpath}"
            elif op == "rename":
                destination_path = _path(req.destination)
                command = f"mv {qpath} {shlex.quote(destination_path)}"
            else:
                command = f"rm -rf -- {qpath}"
        cwd = "/"

    try:
        result = await client.host_exec(node_id, command, cwd=cwd, max_output_bytes=2_000_000)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if error := _host_exec_error(result):
        raise HTTPException(status_code=400, detail=error)

    if op == "home":
        try:
            if is_windows:
                raw_home = str(result.get("stdout") or "").strip()
                home_path = _native_to_windows_path(raw_home)
            else:
                raw_home = str(result.get("stdout") or "").strip()
                if not raw_home.startswith("/"):
                    raise ValueError("home path is not absolute")
                home_path = _path(raw_home)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=502, detail="节点返回了无效用户目录") from exc
        return {"path": home_path}

    if op == "read":
        if is_windows:
            payload = _json_output(result)
            try:
                raw = base64.b64decode(str(payload.get("data") or ""), validate=True)
                content = raw.decode("utf-8")
            except (binascii.Error, UnicodeDecodeError) as exc:
                raise HTTPException(status_code=400, detail="仅支持预览 UTF-8 文本文件") from exc
            return {"path": path, "content": content, "truncated": bool(payload.get("truncated"))}
        return {"path": path, "content": result.get("stdout", ""), "truncated": result.get("stdoutTruncated", False)}
    if op == "list":
        if is_windows:
            entries = _windows_entries(result)
        else:
            entries = []
            for line in (result.get("stdout") or "").splitlines():
                kind, size, entry_path = (line.split("\t", 2) + ["", "", ""])[:3]
                if entry_path:
                    entries.append({"name": entry_path.rsplit("/", 1)[-1], "path": entry_path, "is_dir": kind == "d", "size": int(size or 0)})
            entries.sort(key=lambda item: (not item["is_dir"], item["name"].lower()))
        return {"path": path, "entries": entries}
    return {"ok": True, "path": path, "operation": op}


@router.post("/{node_id}/files")
async def node_host_file(
    node_id: str, req: HostFileRequest, user: User = Depends(_require_admin)
) -> dict[str, Any]:
    del user  # 管理端不需要调用者身份，鉴权已由依赖完成
    return await _do_host_file(node_id, req)


@team_router.post("/{node_id}/files")
async def user_node_host_file(
    node_id: str,
    req: HostFileRequest,
    request: Request,
    user: User = Depends(_require_node_access),
) -> dict[str, Any]:
    """Host file operation on a node the signed-in user is authorized to use."""
    result = await _do_host_file(node_id, req)
    op = (req.operation or "list").strip().lower()
    if op in _MUTATING_OPS:
        await _audit_user_file_op(
            request, user, node_id, op, {"path": req.path, "destination": req.destination or ""}
        )
    return result


def _upload_destination(is_windows: bool, directory: str, filename: str) -> tuple[str, str]:
    """Resolve (virtual_path, node_native_path) for uploading ``filename`` into
    ``directory``. The filename is validated as a single path segment — no
    separators, no traversal, no drive letter — so it cannot escape the chosen
    directory or smuggle in an absolute path."""
    name = (filename or "").strip()
    if not name or name in {".", ".."}:
        raise HTTPException(status_code=400, detail="非法文件名")
    if "/" in name or "\\" in name or "\x00" in name or ":" in name:
        raise HTTPException(status_code=400, detail="文件名不能包含路径分隔符")
    if is_windows:
        virtual, native = _windows_path(directory)
        if native is None:
            raise HTTPException(status_code=400, detail="不能上传到文件系统根目录")
        # Reuse the strict segment checks by validating the full joined path.
        joined_virtual = f"{virtual.rstrip('/')}/{name}"
        _, joined_native = _windows_path(joined_virtual)
        if joined_native is None:  # pragma: no cover - defensive
            raise HTTPException(status_code=400, detail="非法 Windows 路径")
        return joined_virtual, joined_native
    directory_path = _path(directory)
    joined = posixpath.normpath(f"{directory_path.rstrip('/')}/{name}")
    if joined in {"/", "."} or ".." in joined.split("/"):
        raise HTTPException(status_code=400, detail="非法路径")
    return joined, joined


async def _do_host_file_upload(
    node_id: str, path: str, file: UploadFile, overwrite: bool
) -> dict[str, Any]:
    """Upload one file to a node host directory. Authorization is the caller's job."""
    data = await file.read(_MAX_UPLOAD_BYTES + 1)
    if len(data) > _MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="文件不能超过 10 MiB")

    client, is_windows = await _node_is_windows(node_id)

    virtual_path, native_path = _upload_destination(is_windows, path, file.filename or "")
    try:
        result = await client.upload_file(node_id, native_path, data, overwrite=bool(overwrite))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "上传失败")
    return {
        "ok": True,
        "path": virtual_path,
        "bytes_written": int(result.get("bytes_written") or 0),
    }


@router.post("/{node_id}/files/upload")
async def node_host_file_upload(
    node_id: str,
    path: str = Form(...),
    file: UploadFile = File(...),
    overwrite: bool = Form(False),
    user: User = Depends(_require_admin),
) -> dict[str, Any]:
    """Upload a binary file to a node host directory via chunked HostFileUpload.

    ``path`` is the target directory (virtual form: ``/`` for POSIX, ``/C:/..``
    for Windows). The uploaded part's filename becomes the destination name; the
    node writes to a temp file and atomically renames on the final chunk.
    """
    del user  # 管理端不需要调用者身份，鉴权已由依赖完成
    return await _do_host_file_upload(node_id, path, file, overwrite)


@team_router.post("/{node_id}/files/upload")
async def user_node_host_file_upload(
    node_id: str,
    request: Request,
    path: str = Form(...),
    file: UploadFile = File(...),
    overwrite: bool = Form(False),
    user: User = Depends(_require_node_access),
) -> dict[str, Any]:
    """Upload to a node the signed-in user is authorized to use (audited)."""
    result = await _do_host_file_upload(node_id, path, file, overwrite)
    await _audit_user_file_op(
        request, user, node_id, "upload", {"path": result.get("path") or path}
    )
    return result
