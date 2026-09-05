import base64
import json

import pytest
from fastapi import HTTPException

from monkeycode_compat import routes_nodes_files as files


class FakeNodeClient:
    def __init__(self, os_name: str = "windows", result: dict | None = None, upload_result: dict | None = None):
        self.nodes = [{"node_id": "node-1", "capabilities": {"os": os_name}}]
        self.result = result or {"success": True, "stdout": json.dumps({"entries": []})}
        self.calls = []
        self.upload_calls: list[tuple[str, str, bytes, dict]] = []
        self.upload_result: dict = upload_result or {"ok": True, "bytes_written": 0, "path": "", "error": ""}

    async def list_nodes(self):
        return self.nodes

    async def host_exec(self, node_id, command, **kwargs):
        self.calls.append((node_id, command, kwargs))
        return self.result

    async def upload_file(self, node_id, path, data, *, overwrite=False):
        self.upload_calls.append((node_id, path, data, {"overwrite": overwrite}))
        result = dict(self.upload_result)
        if not result.get("path"):
            result["path"] = path
        if result.get("ok"):
            result["bytes_written"] = len(data)
        return result


def install_client(monkeypatch, client: FakeNodeClient) -> None:
    monkeypatch.setattr(files, "get_local_node_client", lambda: client)


@pytest.mark.asyncio
async def test_windows_home_returns_virtual_absolute_path(monkeypatch):
    client = FakeNodeClient(result={
        "success": True,
        "stdout": "  C:\\Users\\张三\r\n",
    })
    install_client(monkeypatch, client)

    result = await files.node_host_file(
        "node-1", files.HostFileRequest(operation="home"), user=None
    )

    assert result == {"path": "/C:/Users/张三"}
    _, command, kwargs = client.calls[0]
    assert command.startswith("powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand ")
    encoded_command = command.rsplit(" ", 1)[-1]
    script = base64.b64decode(encoded_command).decode("utf-16le")
    assert "Get-Location" in script
    assert "[Console]::Out.Write" in script
    assert "GetFolderPath" not in script
    assert "ConvertTo-Json" not in script
    assert "张三" not in command
    assert kwargs["cwd"] == ""


@pytest.mark.asyncio
async def test_posix_home_returns_absolute_path(monkeypatch):
    client = FakeNodeClient(os_name="linux", result={"success": True, "stdout": "/home/tester"})
    install_client(monkeypatch, client)

    result = await files.node_host_file(
        "node-1", files.HostFileRequest(operation="home"), user=None
    )

    assert result == {"path": "/home/tester"}
    _, command, kwargs = client.calls[0]
    assert command == "printf '%s' \"$HOME\""
    assert kwargs["cwd"] == "/"


@pytest.mark.asyncio
@pytest.mark.parametrize("native_path", ["", "C:relative"])
async def test_home_rejects_invalid_node_path(monkeypatch, native_path):
    client = FakeNodeClient(result={"success": True, "stdout": native_path})
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file(
            "node-1", files.HostFileRequest(operation="home"), user=None
        )

    assert exc.value.status_code == 502
    assert "无效用户目录" in exc.value.detail


@pytest.mark.asyncio
async def test_windows_root_lists_drives_and_accepts_omitted_exit_code(monkeypatch):
    client = FakeNodeClient(result={
        "success": True,
        "stdout": json.dumps({
            "entries": [
                {"name": "D:", "path": "D:\\", "is_dir": True, "size": 0},
                {"name": "C:", "path": "C:\\", "is_dir": True, "size": 0},
            ]
        }),
    })
    install_client(monkeypatch, client)

    result = await files.node_host_file("node-1", files.HostFileRequest(path="/"), user=None)

    assert result == {
        "path": "/",
        "entries": [
            {"name": "C:", "path": "/C:", "is_dir": True, "size": 0},
            {"name": "D:", "path": "/D:", "is_dir": True, "size": 0},
        ],
    }
    _, command, kwargs = client.calls[0]
    assert command.startswith("powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand ")
    assert kwargs["cwd"] == ""


@pytest.mark.asyncio
async def test_windows_directory_maps_native_paths(monkeypatch):
    client = FakeNodeClient(result={
        "stdout": json.dumps({
            "entries": [
                {"name": "read me.txt", "path": "C:\\Users\\张三\\read me.txt", "is_dir": False, "size": 12},
                {"name": "子目录", "path": "C:\\Users\\张三\\子目录", "is_dir": True, "size": 0},
            ]
        })
    })
    install_client(monkeypatch, client)

    result = await files.node_host_file(
        "node-1", files.HostFileRequest(path="/c:/Users/张三", operation="list"), user=None
    )

    assert result["path"] == "/C:/Users/张三"
    assert [entry["path"] for entry in result["entries"]] == [
        "/C:/Users/张三/子目录", "/C:/Users/张三/read me.txt"
    ]
    command = client.calls[0][1]
    assert "张三" not in command
    assert "read me" not in command


@pytest.mark.asyncio
async def test_windows_read_decodes_utf8_and_truncation(monkeypatch):
    content = "你好，Windows"
    client = FakeNodeClient(result={
        "stdout": json.dumps({
            "data": base64.b64encode(content.encode()).decode(),
            "truncated": True,
        })
    })
    install_client(monkeypatch, client)

    result = await files.node_host_file(
        "node-1", files.HostFileRequest(path="/C:/临时/a.txt", operation="read"), user=None
    )

    assert result == {"path": "/C:/临时/a.txt", "content": content, "truncated": True}


@pytest.mark.asyncio
async def test_windows_write_hides_path_and_content_from_cmd(monkeypatch):
    client = FakeNodeClient(result={"success": True})
    install_client(monkeypatch, client)

    result = await files.node_host_file(
        "node-1",
        files.HostFileRequest(path="/C:/临时/a & b.txt", operation="write", content="内容 | % !"),
        user=None,
    )

    assert result["ok"] is True
    command = client.calls[0][1]
    assert "临时" not in command
    assert "内容" not in command
    assert "&" not in command


@pytest.mark.asyncio
async def test_windows_rejects_large_write_before_host_exec(monkeypatch):
    client = FakeNodeClient(result={"success": True})
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file(
            "node-1",
            files.HostFileRequest(path="/C:/tmp/large.txt", operation="write", content="x" * 1025),
            user=None,
        )

    assert exc.value.status_code == 400
    assert "1 KiB" in exc.value.detail
    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/../Windows", "//server/share", "/C:/a/../b", "/C:/.. ", "/C:/name. ", "C:/Windows", "/C:/bad\\path"])
async def test_windows_rejects_unsafe_paths(monkeypatch, path):
    client = FakeNodeClient()
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file(
            "node-1", files.HostFileRequest(path=path, operation="list"), user=None
        )

    assert exc.value.status_code == 400
    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["rename", "delete"])
async def test_windows_protects_drive_root_from_destructive_operations(monkeypatch, operation):
    client = FakeNodeClient()
    install_client(monkeypatch, client)
    request = files.HostFileRequest(
        path="/C:", operation=operation, destination="/D:/moved" if operation == "rename" else ""
    )

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file("node-1", request, user=None)

    assert exc.value.status_code == 400
    assert client.calls == []


@pytest.mark.asyncio
async def test_posix_list_keeps_existing_protocol(monkeypatch):
    client = FakeNodeClient(os_name="linux", result={"stdout": "f\t12\t/tmp/z.txt\nd\t0\t/tmp/a\n"})
    install_client(monkeypatch, client)

    result = await files.node_host_file(
        "node-1", files.HostFileRequest(path="/tmp", operation="list"), user=None
    )

    assert [entry["name"] for entry in result["entries"]] == ["a", "z.txt"]
    assert client.calls[0][2]["cwd"] == "/"
    assert client.calls[0][1].startswith("if [ -d /tmp ]")


@pytest.mark.asyncio
async def test_explicit_host_exec_failure_is_reported(monkeypatch):
    client = FakeNodeClient(result={"exit_code": 2, "stderr": "access denied"})
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file("node-1", files.HostFileRequest(), user=None)

    assert exc.value.status_code == 400
    assert exc.value.detail == "access denied"


@pytest.mark.asyncio
async def test_windows_rejects_truncated_json(monkeypatch):
    client = FakeNodeClient(result={"stdout": "{", "stdoutTruncated": True})
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file("node-1", files.HostFileRequest(), user=None)

    assert exc.value.status_code == 502


# ── /files/upload ────────────────────────────────────────────────────────────


def _upload_file(name: str, data: bytes):
    """Build a FastAPI UploadFile without a real multipart request."""
    from fastapi import UploadFile
    import io

    uf = UploadFile(filename=name, file=io.BytesIO(data))
    return uf


@pytest.mark.asyncio
async def test_upload_windows_routes_native_path_and_streams_bytes(monkeypatch):
    client = FakeNodeClient(os_name="windows")
    install_client(monkeypatch, client)
    payload = bytes(range(256)) * 4  # 1 KiB incl. NULs and non-UTF-8 bytes

    result = await files.node_host_file_upload(
        "node-1", path="/C:/Users/张三", file=_upload_file("report.bin", payload), overwrite=True, user=None
    )

    assert result["ok"] is True
    assert result["path"] == "/C:/Users/张三/report.bin"
    assert result["bytes_written"] == len(payload)
    node_id, native_path, data, opts = client.upload_calls[0]
    assert node_id == "node-1"
    assert native_path == "C:\\Users\\张三\\report.bin"
    assert data == payload
    assert opts == {"overwrite": True}


@pytest.mark.asyncio
async def test_upload_posix_keeps_absolute_path(monkeypatch):
    client = FakeNodeClient(os_name="linux")
    install_client(monkeypatch, client)

    result = await files.node_host_file_upload(
        "node-1", path="/tmp", file=_upload_file("notes.txt", b"hello"), user=None
    )

    assert result["path"] == "/tmp/notes.txt"
    assert client.upload_calls[0][1] == "/tmp/notes.txt"


@pytest.mark.asyncio
async def test_upload_rejects_path_traversal_filename(monkeypatch):
    client = FakeNodeClient()
    install_client(monkeypatch, client)

    for bad in ["../x", "a/b", "a\\b", "a:b", "", ".", "..", "x\x00y"]:
        with pytest.raises(HTTPException) as exc:
            await files.node_host_file_upload(
                "node-1", path="/tmp", file=_upload_file(bad, b"x"), user=None
            )
        assert exc.value.status_code == 400
    assert client.upload_calls == []


@pytest.mark.asyncio
async def test_upload_rejects_oversize_file(monkeypatch):
    client = FakeNodeClient()
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file_upload(
            "node-1", path="/tmp", file=_upload_file("big", b"x" * (files._MAX_UPLOAD_BYTES + 1)), user=None
        )
    assert exc.value.status_code == 413
    assert client.upload_calls == []


@pytest.mark.asyncio
async def test_upload_rejects_windows_root_directory(monkeypatch):
    client = FakeNodeClient(os_name="windows")
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file_upload(
            "node-1", path="/", file=_upload_file("x", b"x"), user=None
        )
    assert exc.value.status_code == 400
    assert client.upload_calls == []


@pytest.mark.asyncio
async def test_upload_unknown_node_returns_404(monkeypatch):
    client = FakeNodeClient()
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file_upload(
            "missing", path="/tmp", file=_upload_file("x", b"x"), user=None
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_upload_surfaces_node_failure_as_400(monkeypatch):
    client = FakeNodeClient(os_name="linux", upload_result={"ok": False, "error": "destination exists", "bytes_written": 0, "path": ""})
    install_client(monkeypatch, client)

    with pytest.raises(HTTPException) as exc:
        await files.node_host_file_upload(
            "node-1", path="/tmp", file=_upload_file("x", b"x"), user=None
        )
    assert exc.value.status_code == 400
    assert "destination exists" in exc.value.detail


@pytest.mark.asyncio
async def test_upload_empty_file_single_final_chunk(monkeypatch):
    client = FakeNodeClient(os_name="linux")
    install_client(monkeypatch, client)

    result = await files.node_host_file_upload(
        "node-1", path="/tmp", file=_upload_file("empty", b""), user=None
    )

    assert result["ok"] is True
    assert result["bytes_written"] == 0
    assert client.upload_calls[0][2] == b""
