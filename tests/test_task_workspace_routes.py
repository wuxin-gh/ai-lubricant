from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from monkeycode_compat import routes_task_workspace as workspace


USER = SimpleNamespace(id="00000000-0000-0000-0000-000000000001", role="user")
TASK_ID = "00000000-0000-0000-0000-000000000002"
NODE_SESSION_ID = "node-session-task-1"


@pytest.mark.asyncio
async def test_list_task_files_uses_owned_task_runtime_session(monkeypatch):
    seen: dict[str, object] = {}

    async def resolve(user_id, task_id, *, role=None):
        seen["user_id"] = user_id
        seen["task_id"] = task_id
        seen["role"] = role
        return NODE_SESSION_ID

    async def tunnel(**kwargs):
        seen["tunnel"] = kwargs
        return SimpleNamespace(
            status=200,
            body=b'[{"name":"README.md","type":"file"}]',
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    result = await workspace.list_task_files(TASK_ID, "/src", USER)

    assert seen["user_id"] == str(USER.id)
    assert seen["task_id"] == TASK_ID
    assert seen["role"] == USER.role
    assert seen["tunnel"] == {
        "session_id": NODE_SESSION_ID,
        "service": "files",
        "method": "GET",
        "path": "/src",
        "body_limit": workspace._FILE_READ_LIMIT,
    }
    assert result == {
        "path": "/src",
        "is_dir": True,
        "entries": [{"name": "README.md", "type": "file"}],
        "count": 1,
    }


@pytest.mark.asyncio
async def test_list_task_files_rejects_stopped_runtime(monkeypatch):
    async def stopped(*_args, **_kwargs):
        raise ValueError("runtime_not_bound")

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", stopped)

    with pytest.raises(HTTPException) as exc:
        await workspace.list_task_files(TASK_ID, "/", USER)

    assert exc.value.status_code == 409
    assert "发送一条消息" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_list_task_files_maps_dead_file_service(monkeypatch):
    """节点文件服务已死（runtime 退出）→ 明确 503 提示，不透传原始 dial 错误。"""

    async def resolve(*_args, **_kwargs):
        return NODE_SESSION_ID

    async def tunnel(**_kwargs):
        return SimpleNamespace(
            status=503,
            body=(
                b'session service "files" is not reachable (its runtime has exited): '
                b'dial tcp 127.0.0.1:9693: connectex: A connection attempt failed'
            ),
            headers={},
        )

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    with pytest.raises(HTTPException) as exc:
        await workspace.list_task_files(TASK_ID, "/", USER)

    assert exc.value.status_code == 503
    assert "文件服务当前不可用" in str(exc.value.detail)
    assert "127.0.0.1" not in str(exc.value.detail)
    assert "发送一条消息" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_list_task_files_preserves_binary_encoding(monkeypatch):
    async def resolve(*_args, **_kwargs):
        return NODE_SESSION_ID

    async def tunnel(**_kwargs):
        return SimpleNamespace(
            status=200,
            body=b"\xff\xfe\x00",
            headers={"content-type": "application/octet-stream"},
        )

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    result = await workspace.list_task_files(TASK_ID, "/binary.bin", USER)

    assert result["path"] == "/binary.bin"
    assert result["is_dir"] is False
    assert result["encoding"] == "base64"
    assert result["content"] == "//4A"


@pytest.mark.asyncio
async def test_task_workspace_session_helper_maps_missing_task(monkeypatch):
    async def missing(*_args, **_kwargs):
        raise ValueError("task_not_found")

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", missing)

    with pytest.raises(HTTPException) as exc:
        await workspace._resolve_task_session(TASK_ID, str(USER.id), USER.role)

    assert exc.value.status_code == 404
    assert exc.value.detail == "任务不存在"


@pytest.mark.asyncio
async def test_task_file_changes_preserves_historical_contract(monkeypatch):
    seen: dict[str, object] = {}

    async def resolve(*_args, **_kwargs):
        return NODE_SESSION_ID

    async def tunnel(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(
            status=200,
            body=b'{"changes":[{"path":"a.py","status":"M","additions":2,"deletions":1}],"branch":"main","commit_hash":"abc","success":true}',
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    result = await workspace.list_task_file_changes(TASK_ID, USER)

    assert seen["session_id"] == NODE_SESSION_ID
    assert seen["service"] == "files"
    assert seen["method"] == "GET"
    assert seen["path"] == "__git_changes__"
    assert result["success"] is True
    assert result["changes"][0] == {
        "path": "a.py", "status": "M", "additions": 2, "deletions": 1,
    }


@pytest.mark.asyncio
async def test_task_file_diff_encodes_path_and_context(monkeypatch):
    seen: dict[str, object] = {}

    async def resolve(*_args, **_kwargs):
        return NODE_SESSION_ID

    async def tunnel(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(
            status=200,
            body=b'{"path":"src/a b.py","diff":"@@ -1 +1 @@","success":true}',
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    result = await workspace.get_task_file_diff(TASK_ID, "src/a b.py", 12, USER)

    assert seen["path"] == "__git_diff__?path=src%2Fa+b.py&context_lines=12"
    assert result == {"path": "src/a b.py", "diff": "@@ -1 +1 @@", "success": True}


@pytest.mark.asyncio
async def test_task_file_diff_maps_node_error(monkeypatch):
    async def resolve(*_args, **_kwargs):
        return NODE_SESSION_ID

    async def tunnel(**_kwargs):
        return SimpleNamespace(
            status=409,
            body=b'{"success":false,"error":"not a git repository"}',
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    with pytest.raises(HTTPException) as exc:
        await workspace.get_task_file_diff(TASK_ID, "a.py", 20, USER)

    assert exc.value.status_code == 409
    assert exc.value.detail == "not a git repository"


@pytest.mark.asyncio
async def test_upload_task_attachment_confines_destination(monkeypatch):
    seen: dict[str, object] = {}

    async def resolve(*_args, **_kwargs):
        return NODE_SESSION_ID

    async def tunnel(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(status=204, body=b"", headers={})

    request = SimpleNamespace(
        body=lambda: _async_value(b"image-bytes"),
        headers={"content-type": "image/jpeg"},
    )
    monkeypatch.setattr(workspace.task_service, "task_workspace_node_session_id", resolve)
    monkeypatch.setattr(workspace, "request_session_tunnel", tunnel)

    result = await workspace.upload_task_attachment(
        TASK_ID, request, "../../unsafe name.jpg", USER
    )

    assert seen["session_id"] == NODE_SESSION_ID
    assert seen["service"] == "files"
    assert seen["method"] == "PUT"
    assert seen["body"] == b"image-bytes"
    assert str(seen["path"]).startswith(".task-attachments/")
    assert ".." not in str(seen["path"])
    assert result["url"].startswith("workspace://.task-attachments/")
    assert result["filename"] == "unsafe_name.jpg"


async def _async_value(value):
    return value
