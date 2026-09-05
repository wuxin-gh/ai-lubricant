"""Team models + login-methods domain service (storage only).

Pure storage + configuration for the MonkeyCode team-admin surface. This layer
never touches the VM runtime. Secrets never leave in cleartext:
``TeamModel.api_key`` is masked via ``mask_key`` and ``TeamOIDCConfig.client_secret``
is reduced to a boolean presence flag (``has_client_secret``).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from .masking import mask_key
from .models import Team, TeamGroup, TeamMember
from .models_team_admin import TeamModel, TeamModelGroup, TeamOIDCConfig

_PROBE_TIMEOUT_SECONDS = 10


# ── helpers ──────────────────────────────────────────────────────────────────


def _unix(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _is_masked_key(value: str | None) -> bool:
    return bool(value) and "***" in value


def _group_dict(g: TeamGroup) -> dict:
    return {
        "id": str(g.id),
        "name": g.name,
        "created_at": _unix(g.created_at),
        "updated_at": _unix(g.updated_at),
    }


def _model_dict(m: TeamModel, groups: list[dict]) -> dict:
    return {
        "id": str(m.id),
        "provider": m.provider,
        "model": m.model,
        "base_url": m.base_url,
        "api_key": mask_key(m.api_key or ""),
        "interface_type": m.interface_type,
        "temperature": m.temperature,
        "support_image": m.support_image,
        "remark": m.remark,
        "is_hidden": m.is_hidden,
        "groups": groups,
        "last_check_at": _unix(m.last_check_at),
        "last_check_success": m.last_check_success,
        "last_check_error": m.last_check_error,
        "created_at": _unix(m.created_at),
        "updated_at": _unix(m.updated_at),
    }


def _method_dict(c: TeamOIDCConfig, *, base_url: str = "") -> dict:
    login_url = ""
    redirect_uri = ""
    if c.enabled and base_url:
        base = base_url.rstrip("/")
        login_url = f"{base}/api/v1/users/oidc/login?method_id={c.id}"
        redirect_uri = f"{base}/api/v1/users/oidc/callback"
    return {
        "id": str(c.id),
        "team_id": str(c.team_id),
        "type": c.type or "oidc",
        "name": c.name or "",
        "issuer": c.issuer or "",
        "client_id": c.client_id or "",
        "has_client_secret": bool(c.client_secret),
        "display_name": c.display_name or "",
        "scopes": c.scopes or "",
        "email_domain": c.email_domain or "",
        "enabled": c.enabled,
        "auto_create_member": c.auto_create_member,
        "login_url": login_url,
        "redirect_uri": redirect_uri,
        "created_at": _unix(c.created_at),
        "updated_at": _unix(c.updated_at),
    }


def _password_builtin_entry() -> dict:
    """Password login is always available; listed as a read-only built-in method."""
    return {
        "id": "password",
        "type": "password",
        "name": "账号密码",
        "display_name": "账号密码",
        "enabled": True,
        "built_in": True,
        "login_url": "",
        "redirect_uri": "",
    }


async def _groups_for_model(model_id: uuid.UUID | str) -> list[dict]:
    links = await TeamModelGroup.filter(model_id=model_id)
    group_ids = [link.group_id for link in links]
    if not group_ids:
        return []
    groups = await TeamGroup.filter(id__in=group_ids)
    return [_group_dict(g) for g in groups]


async def _probe(url: str, api_key: str | None = None, payload: dict | None = None) -> tuple[bool, str]:
    """Minimal outbound HTTP probe. 2xx => success. Never raises."""
    import aiohttp

    try:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        timeout = aiohttp.ClientTimeout(total=_PROBE_TIMEOUT_SECONDS)
        method = "POST" if payload is not None else "GET"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, json=payload, headers=headers) as resp:
                if 200 <= resp.status < 300:
                    return True, ""
                return False, f"HTTP {resp.status}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _chat_probe_url(base_url: str, interface_type: str | None) -> tuple[str, dict]:
    base = (base_url or "").rstrip("/")
    if interface_type == "anthropic":
        url = f"{base}/v1/messages" if not base.endswith("/v1") else f"{base}/messages"
        return url, {"model": "probe", "max_tokens": 1, "messages": [{"role": "user", "content": "ping"}]}
    if base.endswith("/v1"):
        url = f"{base}/chat/completions"
    elif base.endswith("/chat/completions"):
        url = base
    else:
        url = f"{base}/v1/chat/completions"
    return url, {"model": "probe", "messages": [{"role": "user", "content": "ping"}], "max_tokens": 1}


_METHOD_TYPES = {"oidc", "oauth_github", "oauth_google"}


class TeamModelsService:
    """Storage-side team models + login-methods operations."""

    # ---- models ---------------------------------------------------------------
    async def list_models(self, team_id: str) -> dict:
        rows = await TeamModel.filter(team_id=team_id, is_deleted=False).order_by("-created_at")
        models = []
        for m in rows:
            groups = await _groups_for_model(m.id)
            models.append(_model_dict(m, groups))
        return {"models": models}

    async def create_model(self, team_id: str, body: dict) -> dict:
        m = await TeamModel.create(
            team_id=team_id,
            provider=body.get("provider") or "",
            model=body.get("model") or "",
            base_url=body.get("base_url") or "",
            api_key=body.get("api_key"),
            interface_type=body.get("interface_type") or "openai_chat",
            temperature=body.get("temperature"),
            support_image=bool(body.get("support_image", False)),
            remark=body.get("remark"),
        )
        await self._set_groups(m.id, body.get("group_ids") or [])
        groups = await _groups_for_model(m.id)
        return _model_dict(m, groups)

    async def update_model(self, team_id: str, model_id: str, body: dict) -> dict | None:
        m = await TeamModel.get_or_none(id=model_id, team_id=team_id, is_deleted=False)
        if m is None:
            return None
        if "provider" in body:
            m.provider = body.get("provider") or ""
        if "model" in body:
            m.model = body.get("model") or ""
        if "base_url" in body:
            m.base_url = body.get("base_url") or ""
        if "interface_type" in body:
            m.interface_type = body.get("interface_type") or "openai_chat"
        if "temperature" in body:
            m.temperature = body.get("temperature")
        if "support_image" in body:
            m.support_image = bool(body.get("support_image"))
        if "remark" in body:
            m.remark = body.get("remark")
        api_key = body.get("api_key")
        if api_key and not _is_masked_key(api_key):
            m.api_key = api_key
        await m.save()
        if "group_ids" in body:
            await self._set_groups(m.id, body.get("group_ids") or [])
        groups = await _groups_for_model(m.id)
        return _model_dict(m, groups)

    async def delete_model(self, team_id: str, model_id: str) -> bool:
        m = await TeamModel.get_or_none(id=model_id, team_id=team_id, is_deleted=False)
        if m is None:
            return False
        m.is_deleted = True
        await m.save()
        return True

    async def _set_groups(self, model_id: uuid.UUID | str, group_ids: list[str]) -> None:
        await TeamModelGroup.filter(model_id=model_id).delete()
        seen: set[str] = set()
        for gid in group_ids:
            if not gid or gid in seen:
                continue
            seen.add(gid)
            await TeamModelGroup.create(model_id=model_id, group_id=gid)

    # ---- health-check ---------------------------------------------------------
    async def health_check_adhoc(self, body: dict) -> dict:
        url, payload = _chat_probe_url(body.get("base_url") or "", body.get("interface_type"))
        success, error = await _probe(url, body.get("api_key"), payload)
        return {"success": success, "error": error}

    async def health_check_stored(self, team_id: str, model_id: str) -> dict | None:
        m = await TeamModel.get_or_none(id=model_id, team_id=team_id, is_deleted=False)
        if m is None:
            return None
        url, payload = _chat_probe_url(m.base_url, m.interface_type)
        success, error = await _probe(url, m.api_key, payload)
        m.last_check_at = datetime.now(timezone.utc)
        m.last_check_success = success
        m.last_check_error = error or None
        await m.save()
        return {"success": success, "error": error}

    # ---- login methods --------------------------------------------------------
    async def list_methods(self, team_id: str, *, base_url: str = "") -> dict:
        rows = await TeamOIDCConfig.filter(team_id=team_id).order_by("created_at")
        methods = [_password_builtin_entry()]
        methods.extend(_method_dict(c, base_url=base_url) for c in rows)
        return {"methods": methods}

    async def create_method(self, team_id: str, body: dict, *, base_url: str = "") -> dict:
        method_type = (body.get("type") or "oidc").strip()
        if method_type not in _METHOD_TYPES:
            raise ValueError("invalid_type")
        c = await TeamOIDCConfig.create(
            id=uuid.uuid4(),
            team_id=team_id,
            type=method_type,
            name=(body.get("name") or "").strip() or None,
            issuer=(body.get("issuer") or "").strip() or None,
            client_id=(body.get("client_id") or "").strip() or None,
            client_secret=(body.get("client_secret") or "").strip() or None,
            display_name=(body.get("display_name") or "").strip() or None,
            scopes=(body.get("scopes") or "").strip() or None,
            email_domain=(body.get("email_domain") or "").strip() or None,
            enabled=bool(body.get("enabled", False)),
            auto_create_member=bool(body.get("auto_create_member", False)),
        )
        return {"method": _method_dict(c, base_url=base_url)}

    async def update_method(
        self, team_id: str, method_id: str, body: dict, *, base_url: str = ""
    ) -> dict:
        c = await TeamOIDCConfig.get_or_none(id=method_id, team_id=team_id)
        if c is None:
            raise ValueError("method_not_found")
        client_secret = body.get("client_secret")
        if client_secret is not None and not _is_masked_key(client_secret):
            c.client_secret = client_secret or None
        for field in ("name", "issuer", "client_id", "display_name", "scopes", "email_domain"):
            if field in body:
                setattr(c, field, (body[field] or "").strip() or None)
        if "type" in body and body["type"] != (c.type or "oidc"):
            raise ValueError("type_immutable")
        if "enabled" in body:
            c.enabled = bool(body["enabled"])
        if "auto_create_member" in body:
            c.auto_create_member = bool(body["auto_create_member"])
        await c.save()
        return {"method": _method_dict(c, base_url=base_url)}

    async def delete_method(self, team_id: str, method_id: str) -> dict:
        c = await TeamOIDCConfig.get_or_none(id=method_id, team_id=team_id)
        if c is None:
            raise ValueError("method_not_found")
        await c.delete()
        return {"ok": True}

    async def test_method(self, body: dict) -> dict:
        method_type = (body.get("type") or "oidc").strip()
        if method_type == "oidc":
            issuer = (body.get("issuer") or "").rstrip("/")
            if not issuer:
                return {"success": False, "message": "issuer 为空"}
            url = f"{issuer}/.well-known/openid-configuration"
        else:
            from .oidc_service import _OAUTH_ENDPOINTS

            endpoints = _OAUTH_ENDPOINTS.get(method_type)
            if endpoints is None:
                return {"success": False, "message": "不支持的类型"}
            url = endpoints["userinfo"]
        success, error = await _probe(url)
        message = "可达" if success else error
        return {"success": success, "message": message}


team_models_service = TeamModelsService()
