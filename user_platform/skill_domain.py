"""Managed Skill domain service: one catalog authority plus bindings.

The catalog row (``mc_agent_skills``) is the discovery authority; the Skill
package file is the content authority.  This module replaces the split
``AgentSkillRepo`` / ``AgentSkillVersion`` write path with one import that
records current delivery info (source url, version, digest) on the catalog row
and a single lightweight version row for the active archive pointer.

SOP experience documents live inside a Skill package's ``experience/`` tree and
are edited through here: saving a Markdown file republishes the package with a
new version and digest, then marks every enabled binding pending re-sync.
"""
from __future__ import annotations

import hashlib
import io
import os
import re
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from .models_skill import AgentSkill, AgentSkillVersion

_RESOURCE_STORE = Path(os.getenv("AGENT_RESOURCE_STORE", "data/agent-resources")).resolve()
_MAX_EXPERIENCE_BYTES = 512 * 1024
_ALLOWED_EXPERIENCE_SUFFIX = ".md"


@dataclass(frozen=True)
class ParsedSkillSource:
    name: str
    description: str
    tags: list[str]
    content: str
    skill_md_path: str


def _now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _version_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _store_archive(skill_id: Any, filename: str, data: bytes) -> str:
    suffix = Path(filename).suffix.lower() or ".bin"
    key = Path("skills") / str(skill_id) / f"{uuid.uuid4().hex}{suffix}"
    target = _RESOURCE_STORE / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return key.as_posix()


def _skill_archive_path(version: AgentSkillVersion) -> Path:
    target = (_RESOURCE_STORE / str(version.s3_key or "")).resolve()
    if not target.is_relative_to(_RESOURCE_STORE) or not target.is_file():
        raise FileNotFoundError("Skill archive is missing")
    return target


def _read_package_tree(version: AgentSkillVersion) -> dict[str, bytes]:
    """Return ``{relative_posix_path: bytes}`` for one Skill archive."""
    archive = _skill_archive_path(version)
    if archive.suffix.lower() == ".zip":
        with zipfile.ZipFile(archive) as zf:
            return {PurePosixPath(info.filename.replace("\\", "/")).as_posix(): zf.read(info.filename) for info in zf.infolist() if not info.is_dir()}
    if archive.suffix.lower() in {".md", ".markdown"}:
        return {"SKILL.md": archive.read_bytes()}
    raise ValueError(f"Unsupported Skill archive: {archive.name}")


def _build_zip(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, data in entries.items():
            normalized = PurePosixPath(path.replace("\\", "/")).as_posix()
            if normalized in {".", ".."} or normalized.startswith("/"):
                continue
            zf.writestr(normalized, data)
    return buffer.getvalue()


def _parse_frontmatter(content: str) -> dict[str, Any]:
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


def parse_skill_source(data: bytes, filename: str, skill_md_path: str | None = None) -> ParsedSkillSource:
    """解析 skill 源（zip / .md / SKILL.md）。

    ``skill_md_path``：显式指定 zip 内要用的 SKILL.md 路径（技能包场景：一个 zip 含多个
    SKILL.md，调用方按 install_spec.skill.entries[i].path + entry 拼出完整路径传入）。
    给则校验该路径在 archive 内且 basename=SKILL.md；不给沿用现行 finder（最短路径）。
    """
    lower = filename.lower()
    if lower.endswith(".zip"):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                names = {name for name in archive.namelist() if not name.endswith("/")}
                if skill_md_path is not None:
                    normalized = PurePosixPath(skill_md_path).as_posix().lstrip("/")
                    if normalized not in names:
                        raise ValueError(f"指定的 SKILL.md 路径不在压缩包内：{normalized}")
                    if PurePosixPath(normalized).name != "SKILL.md":
                        raise ValueError(f"skill_md_path 必须是 SKILL.md 文件：{normalized}")
                    skill_path = normalized
                else:
                    candidates = sorted(
                        (name for name in names if Path(name).name == "SKILL.md"),
                        key=lambda name: (len(Path(name).parts), len(name)),
                    )
                    if not candidates:
                        raise ValueError("压缩包中未找到 SKILL.md")
                    skill_path = candidates[0]
                raw = archive.read(skill_path)
        except zipfile.BadZipFile as exc:
            raise ValueError("Skill 压缩包无效") from exc
    elif lower.endswith(".md") or Path(filename).name == "SKILL.md":
        skill_path, raw = "SKILL.md", data
    else:
        raise ValueError("Skill 仅支持 .zip、SKILL.md 或 Markdown 文件")
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("SKILL.md 必须是 UTF-8 编码") from exc
    if not content.strip():
        raise ValueError("SKILL.md 不能为空")
    frontmatter = _parse_frontmatter(content)
    return ParsedSkillSource(
        name=str(frontmatter.get("name") or "").strip(),
        description=str(frontmatter.get("description") or "").strip(),
        tags=_normalize_tags(frontmatter.get("tags")),
        content=content,
        skill_md_path=skill_path,
    )


def _script_manifest(data: bytes, filename: str) -> tuple[list[dict[str, Any]], str]:
    """Return executable file hashes and a conservative static risk level."""
    entries: dict[str, bytes]
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = {info.filename.replace("\\", "/"): archive.read(info) for info in archive.infolist() if not info.is_dir()}
    else:
        entries = {"SKILL.md": data}
    executable_suffixes = {".py", ".ps1", ".sh", ".bat", ".cmd", ".js", ".ts"}
    high_markers = (b"subprocess", b"os.system", b"powershell", b"Invoke-Expression", b"eval(", b"exec(")
    medium_markers = (b"http://", b"https://", b"socket", b"requests.", b"urllib", b"os.environ")
    manifest: list[dict[str, Any]] = []
    risk = "none"
    for path, content in sorted(entries.items()):
        suffix = PurePosixPath(path).suffix.lower()
        in_scripts = "scripts" in PurePosixPath(path).parts
        if suffix not in executable_suffixes and not in_scripts:
            continue
        file_risk = "high" if any(marker in content for marker in high_markers) else "medium" if any(marker in content for marker in medium_markers) else "low"
        risk = "high" if "high" in (risk, file_risk) else "medium" if "medium" in (risk, file_risk) else "low"
        manifest.append({
            "path": PurePosixPath(path).as_posix(),
            "sha256": hashlib.sha256(content).hexdigest(),
            "bytes": len(content),
            "risk": file_risk,
        })
    return manifest, risk


def _skill_dict(s: AgentSkill, *, version: AgentSkillVersion | None = None) -> dict:
    return {
        "id": str(s.id),
        "name": s.name,
        "description": s.admin_description or s.description,
        "scope_type": s.scope_type,
        "scope_id": s.scope_id,
        "is_force_delivery": s.is_force_delivery,
        "enabled": s.enabled,
        "admin_tags": s.admin_tags,
        "version": s.version or (version.version if version else ""),
        "digest": s.digest or ((version.parsed_meta or {}).get("digest") if version else None),
        "source_type": s.source_type,
        "source_url": s.source_url or ((version.parsed_meta or {}).get("source_url") if version else None),
        "source_filename": s.source_filename or ((version.parsed_meta or {}).get("source_filename") if version else None),
        "download_url": s.download_url,
        "script_manifest": s.script_manifest or [],
        "script_risk": s.script_risk or "none",
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


class SkillDomainService:
    """Import, update, list and bind managed Skills."""

    async def import_skill(
        self, *,
        user_id: str, data: bytes, filename: str, source_type: str, source_url: str | None,
        name_override: str | None, description_override: str | None,
        enabled: bool, is_force_delivery: bool,
        skill_md_path: str | None = None,
    ) -> dict:
        parsed = parse_skill_source(data, filename, skill_md_path=skill_md_path)
        name = (name_override or parsed.name or Path(filename).stem).strip()
        if not name:
            raise ValueError("Skill 必须提供名称或在 SKILL.md frontmatter 中提供 name")
        scope_type, scope_id = "user", str(user_id)
        resource = await AgentSkill.create(
            # Legacy NOT NULL compatibility only. New code never dereferences a
            # Skill repo row; source/delivery metadata lives with the current
            # Skill catalog entry/version payload.
            repo_id=uuid.uuid4(),
            name=name,
            description=description_override if description_override is not None else parsed.description,
            scope_type=scope_type,
            scope_id=scope_id,
            created_by=__import__("uuid").UUID(str(user_id)),
            enabled=enabled,
            is_force_delivery=is_force_delivery,
            admin_tags=parsed.tags,
        )
        try:
            version = await self._publish_archive(
                resource, data=data, filename=filename, source_type=source_type, source_url=source_url,
                parsed=parsed,
            )
        except Exception:
            resource.is_deleted = True
            await resource.save(update_fields=["is_deleted", "updated_at"])
            raise
        return _skill_dict(resource, version=version)

    async def _publish_archive(
        self, resource: AgentSkill, *, data: bytes, filename: str, source_type: str,
        source_url: str | None, parsed: ParsedSkillSource,
    ) -> AgentSkillVersion:
        key = _store_archive(resource.id, filename, data)
        version = await AgentSkillVersion.create(
            resource_id=resource.id,
            version=_version_stamp(),
            s3_key=key,
            parsed_meta={
                "name": parsed.name,
                "description": parsed.description,
                "tags": parsed.tags,
                "skill_md_path": parsed.skill_md_path,
                "digest": _digest(data),
                "source_type": source_type,
                "source_url": source_url,
                "source_filename": filename,
            },
        )
        resource.active_version_id = version.id
        resource.source_type = source_type
        # For managed upload/builtin packages source_url is the stable server-side
        # archive pointer. The original remote URL is not needed by consumers.
        resource.source_url = key if source_type in {"upload", "builtin"} else source_url
        resource.source_filename = filename
        resource.version = version.version
        resource.digest = _digest(data)
        resource.script_manifest, resource.script_risk = _script_manifest(data, filename)
        # ``download_url`` is resolved by the serving endpoint at request time;
        # the catalog stores a stable relative id-based path.
        resource.download_url = f"/api/v1/skills/{resource.id}/download"
        await resource.save(update_fields=[
            "active_version_id", "source_type", "source_url", "source_filename",
            "version", "digest", "script_manifest", "script_risk", "download_url", "updated_at",
        ])
        return version

    async def update_skill_meta(
        self, skill_id: str, *, user_id: str, name: str | None = None,
        description: str | None = None, enabled: bool | None = None,
        is_force_delivery: bool | None = None,
    ) -> dict:
        resource = await self._get_owned(skill_id, user_id)
        if name is not None:
            resource.name = name
        if description is not None:
            resource.admin_description = description
        if enabled is not None:
            resource.enabled = enabled
        if is_force_delivery is not None:
            resource.is_force_delivery = is_force_delivery
        await resource.save()
        version = await AgentSkillVersion.get_or_none(id=resource.active_version_id)
        return _skill_dict(resource, version=version)

    async def delete_skill(self, skill_id: str, *, user_id: str) -> bool:
        resource = await self._get_owned(skill_id, user_id)
        resource.is_deleted = True
        await resource.save(update_fields=["is_deleted", "updated_at"])
        return True

    async def list_catalog(self, *, user_id: str, role: str | None = None, include_disabled: bool = False) -> list[dict]:
        rows = await AgentSkill.filter(is_deleted=False).order_by("name")
        scoped = [row for row in rows if row.scope_type == "global" or row.scope_id == str(user_id)]
        scoped = [row for row in scoped if (row.enabled or include_disabled)]
        out: list[dict] = []
        # 批量预取活跃版本，避免逐行 get_or_none N+1。只取过滤后仍保留的行，
        # 不为被过滤掉的行白白查 version。
        version_ids = [row.active_version_id for row in scoped if row.active_version_id]
        versions_by_id: dict = {}
        if version_ids:
            versions_by_id = {
                v.id: v for v in await AgentSkillVersion.filter(id__in=version_ids)
            }
        for row in scoped:
            version = versions_by_id.get(row.active_version_id) if row.active_version_id else None
            out.append(_skill_dict(row, version=version))
        return out

    async def list_experience(self, skill_id: str, *, user_id: str) -> list[dict]:
        resource = await self._get_owned(skill_id, user_id)
        version = await AgentSkillVersion.get_or_none(id=resource.active_version_id)
        if version is None:
            return []
        entries: list[dict] = []
        for path, data in sorted(_read_package_tree(version).items()):
            normalized = PurePosixPath(path.replace("\\", "/"))
            if normalized.parent.name != "experience" or normalized.suffix.lower() != _ALLOWED_EXPERIENCE_SUFFIX:
                continue
            ref = normalized.name
            try:
                title = data.decode("utf-8").splitlines()[0].lstrip("#").strip()[:240]
            except (UnicodeDecodeError, IndexError):
                title = normalized.stem
            entries.append({"ref": ref, "title": title or normalized.stem, "bytes": len(data)})
        return entries

    async def write_experience(
        self, skill_id: str, ref: str, content: str, *, user_id: str, create: bool = False,
    ) -> dict:
        resource = await self._get_owned(skill_id, user_id)
        version = await AgentSkillVersion.get_or_none(id=resource.active_version_id)
        if version is None:
            raise ValueError("Skill 尚未发布任何版本")
        safe = self._safe_experience_ref(ref)
        if not safe.endswith(_ALLOWED_EXPERIENCE_SUFFIX):
            raise ValueError("SOP 文件必须是 .md")
        encoded = content.encode("utf-8")
        if len(encoded) > _MAX_EXPERIENCE_BYTES:
            raise ValueError("SOP 文件过大")
        tree = _read_package_tree(version)
        key = f"experience/{safe}"
        if key not in tree and not create:
            raise FileNotFoundError("SOP 不存在")
        tree[key] = encoded
        data = _build_zip(tree)
        new_version = await self._publish_archive(
            resource, data=data, filename=f"{resource.name}.zip", source_type="upload",
            source_url=None, parsed=ParsedSkillSource(
                name=resource.name,
                description=resource.admin_description or resource.description or "",
                tags=resource.admin_tags or [],
                content=tree.get("SKILL.md", b"").decode("utf-8", "ignore"),
                skill_md_path="SKILL.md",
            ),
        )
        return {"ref": safe, "version": new_version.version, "digest": _digest(data), "bytes": len(encoded)}

    async def delete_experience(self, skill_id: str, ref: str, *, user_id: str) -> bool:
        resource = await self._get_owned(skill_id, user_id)
        version = await AgentSkillVersion.get_or_none(id=resource.active_version_id)
        if version is None:
            return False
        safe = self._safe_experience_ref(ref)
        tree = _read_package_tree(version)
        key = f"experience/{safe}"
        if key not in tree:
            return False
        tree.pop(key)
        data = _build_zip(tree)
        await self._publish_archive(
            resource, data=data, filename=f"{resource.name}.zip", source_type="upload",
            source_url=None, parsed=ParsedSkillSource(
                name=resource.name,
                description=resource.admin_description or resource.description or "",
                tags=resource.admin_tags or [],
                content=tree.get("SKILL.md", b"").decode("utf-8", "ignore"),
                skill_md_path="SKILL.md",
            ),
        )
        return True

    def _safe_experience_ref(self, ref: str) -> str:
        path = PurePosixPath(str(ref or "").replace("\\", "/"))
        if path.is_absolute() or not path.parts or ".." in path.parts:
            raise ValueError("非法的 SOP 路径")
        return path.as_posix()

    async def _get_owned(self, skill_id: str, user_id: str) -> AgentSkill:
        row = await AgentSkill.get_or_none(id=skill_id, is_deleted=False)
        if not row or row.scope_type != "user" or str(row.scope_id) != str(user_id):
            raise LookupError("Skill 不存在")
        return row


skill_domain = SkillDomainService()

__all__ = [
    "ParsedSkillSource",
    "SkillDomainService",
    "parse_skill_source",
    "skill_domain",
]
