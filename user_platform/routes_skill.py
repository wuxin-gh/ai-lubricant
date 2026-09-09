"""C-side skill/plugin routes (``/api/v1/skills`` and ``/api/v1/plugins``).

Local resources are imported from an uploaded package or an HTTP(S) URL. Every
successful import creates an active version; malformed sources never leave a
bare resource row behind.
"""
from __future__ import annotations

import io
import hashlib
import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from datetime import datetime, timezone
from ipaddress import ip_address
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field

from .deps import get_current_team_id, get_current_user
from .models import User
from .models_skill import (
    AgentPlugin,
    AgentPluginRepo,
    AgentPluginVersion,
    AgentSkill,
    AgentSkillRepo,
    AgentSkillVersion,
)
from .skill_service import _plugin_dict, _skill_dict, skill_service

# NOTE: these live at /api/v1/skills and /api/v1/plugins (not under /users),
# matching the upstream route groups.
skill_router = APIRouter(prefix="/api/v1/skills", tags=["user-platform-skills"])
plugin_router = APIRouter(prefix="/api/v1/plugins", tags=["user-platform-plugins"])

_MAX_IMPORT_BYTES = 20 * 1024 * 1024
_MAX_REDIRECTS = 3
_RESOURCE_STORE = Path(os.getenv("AGENT_RESOURCE_STORE", "data/agent-resources"))


class ResourcePatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    enabled: bool | None = None
    is_force_delivery: bool | None = None


class ExperienceWriteRequest(BaseModel):
    content: str
    create: bool = False


class ImportUrlPayload(BaseModel):
    url: str = Field(min_length=1, max_length=2048)
    name: str | None = Field(default=None, max_length=255)
    description: str | None = None
    enabled: bool = True
    is_force_delivery: bool = False
    # 技能包：显式指定 zip 内要用的 SKILL.md 路径（覆盖默认最短路径 finder）。
    skill_md_path: str | None = Field(default=None, max_length=512)


def _user_scope(user: User) -> tuple[str, str]:
    return "user", str(user.id)


def _resource_error(message: str) -> HTTPException:
    return HTTPException(status_code=400, detail=message)


def _safe_filename(value: str, fallback: str) -> str:
    name = Path(value.replace("\\", "/")).name.strip()
    return name or fallback


def _parse_frontmatter(content: str) -> dict[str, Any]:
    """Parse the small YAML subset accepted by the original Skill importer."""
    lines = content.replace("\r\n", "\n").split("\n")
    if not lines or lines[0].strip() != "---":
        return {}
    try:
        end = next(index for index, line in enumerate(lines[1:], 1) if line.strip() == "---")
    except StopIteration:
        return {}
    meta: dict[str, Any] = {}
    index = 1
    while index < end:
        match = re.match(r"^([A-Za-z0-9_-]+):(?:\s*(.*))?$", lines[index])
        if not match:
            index += 1
            continue
        key, raw = match.groups()
        raw = (raw or "").strip()
        if raw:
            if raw.startswith("[") and raw.endswith("]"):
                meta[key] = [part.strip().strip("\"'") for part in raw[1:-1].split(",") if part.strip()]
            else:
                meta[key] = raw.strip("\"'")
            index += 1
            continue
        values: list[str] = []
        index += 1
        while index < end:
            item = re.match(r"^\s*-\s+(.+)$", lines[index])
            if not item:
                break
            values.append(item.group(1).strip().strip("\"'"))
            index += 1
        if values:
            meta[key] = values
    return meta


def _normalize_tags(value: Any) -> list[str]:
    raw = value if isinstance(value, list) else str(value or "").split(",")
    seen: set[str] = set()
    return [tag for item in raw if (tag := str(item).strip()) and not (tag in seen or seen.add(tag))]


def _find_skill_markdown(archive: zipfile.ZipFile) -> tuple[str, bytes]:
    candidates = sorted(
        (name for name in archive.namelist() if Path(name).name == "SKILL.md" and not name.endswith("/")),
        key=lambda name: (len(Path(name).parts), len(name)),
    )
    if not candidates:
        raise _resource_error("压缩包中未找到 SKILL.md")
    path = candidates[0]
    return path, archive.read(path)


def _parse_skill_source(data: bytes, filename: str) -> dict[str, Any]:
    lower = filename.lower()
    if lower.endswith(".zip"):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                skill_path, raw = _find_skill_markdown(archive)
        except zipfile.BadZipFile as exc:
            raise _resource_error("Skill 压缩包无效") from exc
    elif lower.endswith(".md") or Path(filename).name == "SKILL.md":
        skill_path, raw = "SKILL.md", data
    else:
        raise _resource_error("Skill 仅支持 .zip、SKILL.md 或 Markdown 文件")
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _resource_error("SKILL.md 必须是 UTF-8 编码") from exc
    if not content.strip():
        raise _resource_error("SKILL.md 不能为空")
    frontmatter = _parse_frontmatter(content)
    return {
        "name": str(frontmatter.get("name") or "").strip(),
        "description": str(frontmatter.get("description") or "").strip(),
        "tags": _normalize_tags(frontmatter.get("tags")),
        "content": content,
        "skill_md_path": skill_path,
    }


def _parse_plugin_source(data: bytes, filename: str) -> dict[str, Any]:
    """Require a package manifest and a concrete entry so plugins are consumable."""
    if not filename.lower().endswith(".zip"):
        raise _resource_error("插件仅支持包含 package.json 的 .zip 包")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            candidates = sorted(
                (name for name in archive.namelist() if Path(name).name == "package.json" and not name.endswith("/")),
                key=lambda name: (len(Path(name).parts), len(name)),
            )
            if not candidates:
                raise _resource_error("插件压缩包中未找到 package.json")
            manifest_path = candidates[0]
            try:
                manifest = json.loads(archive.read(manifest_path).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _resource_error("package.json 无法解析") from exc
            entry = manifest.get("main") or manifest.get("module") or manifest.get("exports")
            if isinstance(entry, dict):
                entry = entry.get(".") or entry.get("default")
            if not isinstance(entry, str) or not entry.strip():
                raise _resource_error("插件 package.json 必须包含 main、module 或 exports entry")
            root = str(Path(manifest_path).parent).replace(".", "").strip("/")
            entry_path = "/".join(part for part in (root, entry.lstrip("./")) if part)
            if entry_path not in archive.namelist():
                raise _resource_error(f"插件 entry 不存在：{entry}")
    except zipfile.BadZipFile as exc:
        raise _resource_error("插件压缩包无效") from exc
    return {
        "name": str(manifest.get("name") or "").strip(),
        "description": str(manifest.get("description") or "").strip(),
        "entry": entry,
        "manifest_path": manifest_path,
        "manifest": manifest,
    }


def _validate_download_url(value: str) -> urllib.parse.ParseResult:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise _resource_error("下载链接必须是公开的 http(s) URL")
    try:
        addresses = socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80))
    except socket.gaierror as exc:
        raise _resource_error("下载链接的主机无法解析") from exc
    for address in addresses:
        ip = ip_address(address[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise _resource_error("下载链接不能指向内网或本机地址")
    return parsed


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _download_remote(url: str) -> tuple[bytes, str]:
    """Bounded download with protocol, redirect, content-length, and SSRF checks."""
    current = url
    opener = urllib.request.build_opener(_NoRedirect)
    for _ in range(_MAX_REDIRECTS + 1):
        _validate_download_url(current)
        request = urllib.request.Request(current, headers={"User-Agent": "ai-lubricant-resource-import/1.0"})
        try:
            response = opener.open(request, timeout=15)
        except urllib.error.HTTPError as exc:
            if exc.code not in {301, 302, 303, 307, 308}:
                raise _resource_error(f"下载失败：HTTP {exc.code}") from exc
            location = exc.headers.get("Location")
            if not location:
                raise _resource_error("下载重定向缺少 Location")
            current = urllib.parse.urljoin(current, location)
            continue
        except urllib.error.URLError as exc:
            raise _resource_error(f"下载失败：{exc.reason}") from exc
        with response:
            length = response.headers.get("Content-Length")
            if length and int(length) > _MAX_IMPORT_BYTES:
                raise _resource_error("下载文件超过 20 MB 限制")
            data = response.read(_MAX_IMPORT_BYTES + 1)
            if len(data) > _MAX_IMPORT_BYTES:
                raise _resource_error("下载文件超过 20 MB 限制")
            filename = _safe_filename(
                urllib.parse.unquote(Path(urllib.parse.urlparse(response.url).path).name),
                "resource.zip",
            )
            return data, filename
    raise _resource_error("下载重定向次数超过限制")


def _store_archive(kind: str, resource_id: str, filename: str, data: bytes) -> str:
    suffix = Path(filename).suffix.lower() or ".bin"
    key = Path(kind) / resource_id / f"{uuid.uuid4().hex}{suffix}"
    target = _RESOURCE_STORE / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return key.as_posix()


def _import_version() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


async def _get_owned_skill(skill_id: str, user: User) -> AgentSkill:
    row = await AgentSkill.get_or_none(id=skill_id, is_deleted=False)
    if not row or (row.scope_type, row.scope_id) != _user_scope(user):
        raise HTTPException(404, "Skill 不存在")
    return row


async def _get_owned_plugin(plugin_id: str, user: User) -> AgentPlugin:
    row = await AgentPlugin.get_or_none(id=plugin_id, is_deleted=False)
    if not row or (row.scope_type, row.scope_id) != _user_scope(user):
        raise HTTPException(404, "插件不存在")
    return row


async def _create_skill_import(
    *, user: User, data: bytes, filename: str, source_type: str, source_url: str | None,
    name_override: str | None, description_override: str | None, enabled: bool, is_force_delivery: bool,
    skill_md_path: str | None = None,
) -> dict:
    from .skill_domain import skill_domain

    try:
        return await skill_domain.import_skill(
            user_id=str(user.id),
            data=data,
            filename=filename,
            source_type=source_type,
            source_url=source_url,
            name_override=name_override,
            description_override=description_override,
            enabled=enabled,
            is_force_delivery=is_force_delivery,
            skill_md_path=skill_md_path,
        )
    except ValueError as exc:
        raise _resource_error(str(exc)) from exc


async def _create_plugin_import(
    *, user: User, data: bytes, filename: str, source_type: str, source_url: str | None,
    name_override: str | None, description_override: str | None, enabled: bool, is_force_delivery: bool,
) -> dict:
    parsed = _parse_plugin_source(data, filename)
    name = (name_override or parsed["name"] or Path(filename).stem).strip()
    if not name:
        raise _resource_error("插件必须在 package.json 中提供 name 或填写名称")
    scope_type, scope_id = _user_scope(user)
    repo = await AgentPluginRepo.create(
        name=name, scope_type=scope_type, scope_id=scope_id, created_by=user.id,
        source_type=source_type, github_url=source_url, last_upload_filename=filename,
        last_upload_at=datetime.now(timezone.utc),
    )
    resource = await AgentPlugin.create(
        repo_id=repo.id, name=name, description=description_override if description_override is not None else parsed["description"],
        scope_type=scope_type, scope_id=scope_id, created_by=user.id, enabled=enabled,
        is_force_delivery=is_force_delivery,
    )
    try:
        key = _store_archive("plugins", str(resource.id), filename, data)
        version = await AgentPluginVersion.create(
            resource_id=resource.id, version=_import_version(), s3_key=key, parsed_meta=parsed,
        )
        resource.active_version_id = version.id
        await resource.save(update_fields=["active_version_id", "updated_at"])
    except Exception:
        resource.is_deleted = True
        repo.is_deleted = True
        await resource.save(update_fields=["is_deleted", "updated_at"])
        await repo.save(update_fields=["is_deleted", "updated_at"])
        raise
    return _plugin_dict(resource)


@skill_router.get("")
async def list_skills(
    manage: bool = False,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> list[dict]:
    return await skill_service.list_skills(
        str(user.id), team_id=team_id, role=user.role, include_disabled_owned=manage,
    )


@skill_router.post("/import/upload")
async def import_skill_upload(
    file: UploadFile = File(...), name: str | None = Form(None), description: str | None = Form(None),
    enabled: bool = Form(True), is_force_delivery: bool = Form(False),
    skill_md_path: str | None = Form(None),
    user: User = Depends(get_current_user),
) -> dict:
    filename = _safe_filename(file.filename or "SKILL.md", "SKILL.md")
    data = await file.read(_MAX_IMPORT_BYTES + 1)
    if len(data) > _MAX_IMPORT_BYTES:
        raise _resource_error("上传文件超过 20 MB 限制")
    return await _create_skill_import(
        user=user, data=data, filename=filename, source_type="upload", source_url=None,
        name_override=name, description_override=description, enabled=enabled, is_force_delivery=is_force_delivery,
        skill_md_path=skill_md_path,
    )


@skill_router.post("/import/url")
async def import_skill_url(payload: ImportUrlPayload, user: User = Depends(get_current_user)) -> dict:
    data, filename = _download_remote(payload.url)
    # 技能包场景：前端按 install_spec.skill.entries[i].path+entry 拼出完整路径传入，
    # 服务端用该路径覆盖默认 finder（最短 SKILL.md），确保装的是用户选中的那个。
    return await _create_skill_import(
        user=user, data=data, filename=filename, source_type="github", source_url=payload.url,
        name_override=payload.name, description_override=payload.description, enabled=payload.enabled,
        is_force_delivery=payload.is_force_delivery,
        skill_md_path=(payload.skill_md_path or "").strip() or None,
    )


@skill_router.post("/parse")
async def parse_skill_source(
    request: Request, user: User = Depends(get_current_user),
) -> dict:
    """Dry-run 解析：上传文件或提交 URL，返回识别出的 Skill 元数据，不落库。

    前端导入两步式弹框第一步调用：先识别 → 用户确认 → 再调 import 落库。
    请求体形态：
      - multipart：file=<file>
      - JSON：{url: "..."}
    """
    content_type = (request.headers.get("content-type") or "").lower()
    if content_type.startswith("multipart/"):
        form = await request.form()
        file = form.get("file")
        if not isinstance(file, UploadFile):
            raise _resource_error("请上传文件")
        filename = _safe_filename(file.filename or "SKILL.md", "SKILL.md")
        data = await file.read(_MAX_IMPORT_BYTES + 1)
        if len(data) > _MAX_IMPORT_BYTES:
            raise _resource_error("上传文件超过 20 MB 限制")
    else:
        body = await request.json()
        url = str((body or {}).get("url") or "").strip()
        if not url:
            raise _resource_error("请填写下载链接")
        data, filename = _download_remote(url)
    parsed = _parse_skill_source(data, filename)
    return {
        "kind": "skill",
        "filename": filename,
        "name": parsed.get("name") or Path(filename).stem,
        "description": parsed.get("description") or "",
        "tags": parsed.get("tags") or [],
        "extra": parsed,
    }


@skill_router.get("/{skill_id}/experience")
async def list_skill_experience(skill_id: str, user: User = Depends(get_current_user)) -> dict:
    from .skill_domain import skill_domain
    try:
        return {"files": await skill_domain.list_experience(skill_id, user_id=str(user.id))}
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc


@skill_router.put("/{skill_id}/experience/{ref:path}")
async def write_skill_experience(
    skill_id: str, ref: str, payload: ExperienceWriteRequest,
    user: User = Depends(get_current_user),
) -> dict:
    from .skill_domain import skill_domain
    try:
        return await skill_domain.write_experience(
            skill_id, ref, payload.content, user_id=str(user.id), create=payload.create,
        )
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@skill_router.delete("/{skill_id}/experience/{ref:path}")
async def delete_skill_experience(
    skill_id: str, ref: str, user: User = Depends(get_current_user),
) -> dict:
    from .skill_domain import skill_domain
    try:
        deleted = await skill_domain.delete_experience(skill_id, ref, user_id=str(user.id))
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if not deleted:
        raise HTTPException(404, "SOP 不存在")
    return {"deleted": True}


@skill_router.get("/{skill_id}/download")
async def download_skill(skill_id: str, user: User = Depends(get_current_user)):
    """Download the current full Skill package after scope authorization."""
    from fastapi.responses import FileResponse
    from .skill_domain import _skill_archive_path

    rows = await skill_service.list_skills(str(user.id), role=user.role)
    if skill_id not in {row["id"] for row in rows}:
        raise HTTPException(404, "Skill 不存在")
    resource = await AgentSkill.get_or_none(id=skill_id, is_deleted=False)
    version = await AgentSkillVersion.get_or_none(id=resource.active_version_id) if resource else None
    if version is None:
        raise HTTPException(404, "Skill 尚未发布")
    try:
        path = _skill_archive_path(version)
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    return FileResponse(path, filename=f"{resource.name}{path.suffix}")


@skill_router.patch("/{skill_id}")
async def update_skill(skill_id: str, payload: ResourcePatch, user: User = Depends(get_current_user)) -> dict:
    from .skill_domain import skill_domain

    try:
        return await skill_domain.update_skill_meta(
            skill_id,
            user_id=str(user.id),
            **payload.model_dump(exclude_none=True),
        )
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc


@skill_router.delete("/{skill_id}")
async def delete_skill(skill_id: str, user: User = Depends(get_current_user)) -> dict:
    from .skill_domain import skill_domain

    try:
        await skill_domain.delete_skill(skill_id, user_id=str(user.id))
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {"deleted": True}


@plugin_router.get("")
async def list_plugins(
    manage: bool = False,
    user: User = Depends(get_current_user),
    team_id: str = Depends(get_current_team_id),
) -> list[dict]:
    return await skill_service.list_plugins(
        str(user.id), team_id=team_id, role=user.role, include_disabled_owned=manage,
    )


@plugin_router.post("/import/upload")
async def import_plugin_upload(
    file: UploadFile = File(...), name: str | None = Form(None), description: str | None = Form(None),
    enabled: bool = Form(True), is_force_delivery: bool = Form(False), user: User = Depends(get_current_user),
) -> dict:
    filename = _safe_filename(file.filename or "plugin.zip", "plugin.zip")
    data = await file.read(_MAX_IMPORT_BYTES + 1)
    if len(data) > _MAX_IMPORT_BYTES:
        raise _resource_error("上传文件超过 20 MB 限制")
    return await _create_plugin_import(
        user=user, data=data, filename=filename, source_type="upload", source_url=None,
        name_override=name, description_override=description, enabled=enabled, is_force_delivery=is_force_delivery,
    )


@plugin_router.post("/import/url")
async def import_plugin_url(payload: ImportUrlPayload, user: User = Depends(get_current_user)) -> dict:
    data, filename = _download_remote(payload.url)
    return await _create_plugin_import(
        user=user, data=data, filename=filename, source_type="github", source_url=payload.url,
        name_override=payload.name, description_override=payload.description, enabled=payload.enabled,
        is_force_delivery=payload.is_force_delivery,
    )


@plugin_router.post("/parse")
async def parse_plugin_source(
    request: Request, user: User = Depends(get_current_user),
) -> dict:
    """Dry-run 解析：上传文件或提交 URL，返回识别出的插件元数据，不落库。

    前端导入两步式弹框第一步调用：先识别 → 用户确认 → 再调 import 落库。
    """
    content_type = (request.headers.get("content-type") or "").lower()
    if content_type.startswith("multipart/"):
        form = await request.form()
        file = form.get("file")
        if not isinstance(file, UploadFile):
            raise _resource_error("请上传文件")
        filename = _safe_filename(file.filename or "plugin.zip", "plugin.zip")
        data = await file.read(_MAX_IMPORT_BYTES + 1)
        if len(data) > _MAX_IMPORT_BYTES:
            raise _resource_error("上传文件超过 20 MB 限制")
    else:
        body = await request.json()
        url = str((body or {}).get("url") or "").strip()
        if not url:
            raise _resource_error("请填写下载链接")
        data, filename = _download_remote(url)
    parsed = _parse_plugin_source(data, filename)
    return {
        "kind": "plugin",
        "filename": filename,
        "name": parsed.get("name") or Path(filename).stem,
        "description": parsed.get("description") or "",
        "entry": parsed.get("entry") or "",
        "root": parsed.get("root") or "",
        "extra": parsed,
    }


@plugin_router.patch("/{plugin_id}")
async def update_plugin(plugin_id: str, payload: ResourcePatch, user: User = Depends(get_current_user)) -> dict:
    row = await _get_owned_plugin(plugin_id, user)
    for key, value in payload.model_dump(exclude_none=True).items():
        setattr(row, key, value)
    await row.save()
    return _plugin_dict(row)


@plugin_router.delete("/{plugin_id}")
async def delete_plugin(plugin_id: str, user: User = Depends(get_current_user)) -> dict:
    row = await _get_owned_plugin(plugin_id, user)
    row.is_deleted = True
    await row.save(update_fields=["is_deleted", "updated_at"])
    return {"deleted": True}
