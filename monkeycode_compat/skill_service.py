"""Agent skill / plugin domain service (user-facing listing).

The MonkeyCode user surface exposes ``/api/v1/skills`` and ``/api/v1/plugins``
as read-oriented catalogs: skills/plugins are synced from git repos by an
external worker, versioned, and delivered by scope (global/team/user). This
service provides the read/list side scoped to the caller.

Scope resolution mirrors MonkeyCode: a user sees global resources, their team's
resources, and their own. Sync/upload/version-activation depend on object
storage + a sync worker and are layered on later; this slice is storage-read.

Storage only — never touches the model request pipeline.
"""
from __future__ import annotations

import uuid
from typing import Any

from .models_skill import AgentPlugin, AgentRule, AgentSkill

_PRIVILEGED_ROLES = {"admin"}


def _skill_dict(s: AgentSkill) -> dict:
    return {
        "id": str(s.id),
        "repo_id": str(s.repo_id),
        "name": s.name,
        "description": s.admin_description or s.description,
        "scope_type": s.scope_type,
        "scope_id": s.scope_id,
        "active_version_id": str(s.active_version_id) if s.active_version_id else None,
        "is_force_delivery": s.is_force_delivery,
        "enabled": s.enabled,
        "admin_tags": s.admin_tags,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


def _plugin_dict(p: AgentPlugin) -> dict:
    return {
        "id": str(p.id),
        "repo_id": str(p.repo_id),
        "name": p.name,
        "description": p.description,
        "scope_type": p.scope_type,
        "scope_id": p.scope_id,
        "active_version_id": str(p.active_version_id) if p.active_version_id else None,
        "is_force_delivery": p.is_force_delivery,
        "enabled": p.enabled,
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "updated_at": p.updated_at.isoformat() if p.updated_at else None,
    }


class SkillService:
    """Read-side listing of agent skills / plugins scoped to the caller."""

    def _is_privileged(self, role: str | None) -> bool:
        return (role or "") in _PRIVILEGED_ROLES

    def _visible_scopes(self, user_id: str, team_id: str | None) -> list[tuple[str, str]]:
        """Return (scope_type, scope_id) pairs visible to this caller."""
        scopes: list[tuple[str, str]] = [("global", "global")]
        scopes.append(("user", str(user_id)))
        if team_id:
            scopes.append(("team", str(team_id)))
        return scopes

    async def list_skills(
        self, user_id: str, *, team_id: str | None = None, role: str | None = None,
        include_disabled_owned: bool = False,
    ) -> list[dict]:
        rows = await AgentSkill.filter(is_deleted=False).order_by("name")
        if self._is_privileged(role):
            return [_skill_dict(r) for r in rows if r.active_version_id and (r.enabled or include_disabled_owned)]
        visible = set(self._visible_scopes(user_id, team_id))
        return [
            _skill_dict(r) for r in rows
            if r.active_version_id
            and (r.scope_type, r.scope_id) in visible
            and (r.enabled or (include_disabled_owned and r.scope_type == "user" and r.scope_id == str(user_id)))
        ]

    async def list_plugins(
        self, user_id: str, *, team_id: str | None = None, role: str | None = None,
        include_disabled_owned: bool = False,
    ) -> list[dict]:
        rows = await AgentPlugin.filter(is_deleted=False).order_by("name")
        if self._is_privileged(role):
            return [_plugin_dict(r) for r in rows if r.active_version_id and (r.enabled or include_disabled_owned)]
        visible = set(self._visible_scopes(user_id, team_id))
        return [
            _plugin_dict(r) for r in rows
            if r.active_version_id
            and (r.scope_type, r.scope_id) in visible
            and (r.enabled or (include_disabled_owned and r.scope_type == "user" and r.scope_id == str(user_id)))
        ]


skill_service = SkillService()
