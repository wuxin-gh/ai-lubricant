"""用户侧 agent principal 写入权限：owner=NULL 的 agent/task principal 允许用户配置。

回归背景：Agent 详情页保存 MCP 授权时报 403「平台 MCP 用户只能在管理端修改」——
读路径（list/get params）已放行 owner=NULL 的 agent principal，写路径却只认严格
owner 匹配，两边口径不一致。这里锁住新口径：agent/task 可写，external 仍 403。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import HTTPException

import monkeycode_compat.routes_mcp_principals as routes


class _FakeUser:
    id = "user-1"


def _install_store(monkeypatch, *, owned=None, manageable=None):
    """把 routes 内部 `import mcp_plugin_store as store` 换成假 store。"""
    calls: dict[str, object] = {}

    async def get_owned_mcp_principal(pid, owner):
        return owned

    async def get_manageable_mcp_principal(pid, owner):
        return manageable

    async def update_owned_mcp_principal(pid, owner, **kw):
        return owned

    async def update_manageable_mcp_principal(pid, owner, **kw):
        calls["update_manageable"] = (pid, owner, kw)
        return {**(manageable or {}), **kw}

    async def set_principal_params(pid, payload, *, owner_user_id=None):
        calls["set_params"] = (pid, payload, owner_user_id)
        return payload

    fake = types.SimpleNamespace(
        get_owned_mcp_principal=get_owned_mcp_principal,
        get_manageable_mcp_principal=get_manageable_mcp_principal,
        update_owned_mcp_principal=update_owned_mcp_principal,
        update_manageable_mcp_principal=update_manageable_mcp_principal,
        set_principal_params=set_principal_params,
    )
    monkeypatch.setitem(sys.modules, "mcp_plugin_store", fake)

    async def _noop_resync():
        calls["resync"] = True

    monkeypatch.setattr(routes, "_resync_builtin_principals", _noop_resync)
    return calls


@pytest.mark.asyncio
async def test_replace_params_allows_platform_agent_principal(monkeypatch):
    calls = _install_store(
        monkeypatch, owned=None,
        manageable={"id": 5, "owner_user_id": None, "usage_type": "agent"},
    )
    body = routes.ReplaceParamsReq(params=[{"param_key": "cdp_client_id", "param_value": "9"}])
    result = await routes.replace_params(5, body, user=_FakeUser())
    assert result["params"][0]["param_value"] == "9"
    assert calls["set_params"][0] == 5
    assert calls["resync"] is True


@pytest.mark.asyncio
async def test_replace_params_still_blocks_platform_external(monkeypatch):
    _install_store(
        monkeypatch, owned=None,
        manageable={"id": 6, "owner_user_id": None, "usage_type": "external"},
    )
    body = routes.ReplaceParamsReq(params=[{"param_key": "cdp_client_id", "param_value": "9"}])
    with pytest.raises(HTTPException) as exc:
        await routes.replace_params(6, body, user=_FakeUser())
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_replace_params_unknown_principal_is_404(monkeypatch):
    _install_store(monkeypatch, owned=None, manageable=None)
    body = routes.ReplaceParamsReq(params=[{"param_key": "cdp_client_id", "param_value": "9"}])
    with pytest.raises(HTTPException) as exc:
        await routes.replace_params(7, body, user=_FakeUser())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_patch_principal_allows_platform_agent_principal(monkeypatch):
    calls = _install_store(
        monkeypatch, owned=None,
        manageable={"id": 5, "owner_user_id": None, "usage_type": "agent", "enabled": True},
    )
    row = await routes.update_principal(
        5, routes.UpdatePrincipalReq(enabled=False), user=_FakeUser(),
    )
    assert row["enabled"] is False
    assert calls["update_manageable"][2] == {"enabled": False}


@pytest.mark.asyncio
async def test_patch_principal_still_blocks_platform_external(monkeypatch):
    _install_store(
        monkeypatch, owned=None,
        manageable={"id": 6, "owner_user_id": None, "usage_type": "external"},
    )
    with pytest.raises(HTTPException) as exc:
        await routes.update_principal(
            6, routes.UpdatePrincipalReq(enabled=False), user=_FakeUser(),
        )
    assert exc.value.status_code == 403
