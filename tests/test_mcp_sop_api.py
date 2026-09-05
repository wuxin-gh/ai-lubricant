"""MCP SOP 在线读写端点测试。

覆盖：
- GET /mcp/services/{name}/sop 读取内置服务 sop/*.md
- PUT /mcp/services/{name}/sop 写回源文件（覆盖已存在）
- 路径穿越 / 非 .md / 不存在文件 的防御
- 自定义服务（无 sop 目录）返回空/拒绝写
"""
import os
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import mcp.api as mcp_api


CDP = "cdp-bridge"
SOP_FILE = "tmwebdriver_sop.md"


@pytest.fixture
def client(monkeypatch, tmp_path):
    # 绕过 admin 鉴权
    async def _noop_admin(authorization):
        return None

    monkeypatch.setattr(mcp_api, "_require_admin", _noop_admin)

    # 把内置服务目录指向临时目录，避免污染仓库真实 SOP
    fake_dir = tmp_path / "cdp_bridge"
    (fake_dir / "sop").mkdir(parents=True)
    (fake_dir / "sop" / SOP_FILE).write_text("# 原始 SOP\n内容 A", encoding="utf-8")
    monkeypatch.setitem(mcp_api._BUILTIN_MANIFEST_DIRS, CDP, fake_dir)

    # service lookup：内置服务返回 builtin 记录，其它名字返回 None
    async def _fake_get_by_name(name):
        if name == CDP:
            return {"id": 1, "name": CDP, "builtin": True}
        if name == "custom-x":
            return {"id": 2, "name": "custom-x", "builtin": False}
        return None

    monkeypatch.setattr(mcp_api.mcp_plugin_store, "get_service_by_name", _fake_get_by_name)

    app = FastAPI()
    app.include_router(mcp_api.router)
    return TestClient(app), fake_dir


def test_get_sop_returns_files(client):
    c, _ = client
    r = c.get(f"/mcp/services/{CDP}/sop")
    assert r.status_code == 200
    body = r.json()
    assert body["service_name"] == CDP
    names = {f["name"] for f in body["files"]}
    assert SOP_FILE in names
    content = next(f["content"] for f in body["files"] if f["name"] == SOP_FILE)
    assert "原始 SOP" in content


def test_get_sop_unknown_service_404(client):
    c, _ = client
    r = c.get("/mcp/services/nope/sop")
    assert r.status_code == 404


def test_get_sop_custom_service_empty(client):
    c, _ = client
    # 自定义服务没有内置 sop 目录 → files 为空
    r = c.get("/mcp/services/custom-x/sop")
    assert r.status_code == 200
    assert r.json()["files"] == []


def test_put_sop_overwrites_source_file(client):
    c, fake_dir = client
    r = c.put(
        f"/mcp/services/{CDP}/sop",
        json={"files": [{"name": SOP_FILE, "content": "# 新内容\n改了"}]},
    )
    assert r.status_code == 200
    # 磁盘已更新
    on_disk = (fake_dir / "sop" / SOP_FILE).read_text(encoding="utf-8")
    assert on_disk == "# 新内容\n改了"


def test_put_sop_rejects_path_traversal(client):
    c, _ = client
    for bad in ["../evil.md", "sub/dir.md", "a\\b.md", "..md/../x.md"]:
        r = c.put(f"/mcp/services/{CDP}/sop", json={"files": [{"name": bad, "content": "x"}]})
        assert r.status_code == 400, f"{bad!r} should be rejected"


def test_put_sop_rejects_non_md(client):
    c, _ = client
    r = c.put(f"/mcp/services/{CDP}/sop", json={"files": [{"name": "notes.txt", "content": "x"}]})
    assert r.status_code == 400


def test_put_sop_rejects_new_file(client):
    c, _ = client
    r = c.put(f"/mcp/services/{CDP}/sop", json={"files": [{"name": "brand_new.md", "content": "x"}]})
    assert r.status_code == 404


def test_put_sop_custom_service_no_dir_400(client):
    c, _ = client
    r = c.put("/mcp/services/custom-x/sop", json={"files": [{"name": SOP_FILE, "content": "x"}]})
    assert r.status_code == 400
