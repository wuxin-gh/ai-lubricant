from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from monkeycode_compat import routes_editors_workspace
from monkeycode_compat.node_client.tunnel import TunnelResponse


@pytest.mark.asyncio
@pytest.mark.parametrize("header_name", ["Content-Type", "content-type"])
async def test_editor_directory_content_type_is_case_insensitive(monkeypatch, header_name):
    async def resolve(_editor_id: str, _user_id: str):
        return {"id": "editor-1"}, "workspace-1"

    async def tunnel(**_kwargs):
        return TunnelResponse(
            status=200,
            headers={header_name: "application/json; charset=utf-8"},
            body=json.dumps([{"name": ".agent-compose", "size": 0, "is_dir": True}]).encode(),
        )

    monkeypatch.setattr(routes_editors_workspace, "_resolve_editor_workspace", resolve)
    monkeypatch.setattr(routes_editors_workspace, "request_session_tunnel", tunnel)

    result = await routes_editors_workspace.list_editor_files(
        "editor-1", "/", SimpleNamespace(id="user-1")
    )

    assert result["is_dir"] is True
    assert result["entries"] == [{"name": ".agent-compose", "size": 0, "is_dir": True}]


@pytest.mark.asyncio
async def test_editor_text_file_remains_a_file(monkeypatch):
    async def resolve(_editor_id: str, _user_id: str):
        return {"id": "editor-1"}, "workspace-1"

    async def tunnel(**_kwargs):
        return TunnelResponse(
            status=200,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            body=b"hello",
        )

    monkeypatch.setattr(routes_editors_workspace, "_resolve_editor_workspace", resolve)
    monkeypatch.setattr(routes_editors_workspace, "request_session_tunnel", tunnel)

    result = await routes_editors_workspace.list_editor_files(
        "editor-1", "/README.md", SimpleNamespace(id="user-1")
    )

    assert result["is_dir"] is False
    assert result["content"] == "hello"
