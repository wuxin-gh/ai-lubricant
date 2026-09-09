"""系统内置环境（env_mode=system）的节点级资源服务。

与共用环境的关键差别在语义而非实现：操作者的 HOME 不是平台的目录，所以
- 读是主体（清单让用户第一次看见系统档实际会拿到什么）；
- 写是增量（同名默认跳过，覆盖要显式选）；
- 卸载有归属边界（只能删平台装过的，节点侧按自己的 manifest 复核）。

这里只测服务层的编排与守卫；节点侧的文件语义由 Go 侧 systemenv_test.go 覆盖。
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from user_platform import system_env_service
from user_platform.system_env_service import SystemEnvError

NODE_ID = "node-sys-1"
USER_ID = str(uuid.uuid4())
TEAM_ID = str(uuid.uuid4())


class _FakeNodesService:
    """替掉 nodes_service：记录调用参数，返回可控结果。"""

    def __init__(self, *, granted=True, online=True, system_env="true"):
        self.granted = granted
        caps = {"os": "linux"}
        if system_env is not None:
            caps["system_env"] = system_env
        self.live = {"online": online, "capabilities": caps}
        self.inspect_result = {"installed": []}
        self.sync_calls: list[dict] = []
        self.archive_calls: list[dict] = []

    async def user_can_use_node(self, user_id, node_id):
        return SimpleNamespace(node_id=node_id) if self.granted else None

    async def node_live_info(self, node_id):
        return self.live

    async def inspect_system_env(self, node_id, provider=""):
        return self.inspect_result

    async def sync_system_env(self, node_id, skills, plugins, overwrite=False, remove=None):
        self.sync_calls.append({
            "node_id": node_id, "skills": skills, "plugins": plugins,
            "overwrite": overwrite, "remove": list(remove or []),
        })
        return {"ok": True, "touched": [{"kind": "skill", "name": "s", "path": "installed"}]}

    async def archive_system_env_resource(self, node_id, kind, name, upload_url, upload_token):
        self.archive_calls.append({
            "node_id": node_id, "kind": kind, "name": name,
            "upload_url": upload_url, "upload_token": upload_token,
        })
        return {"ok": True}


class _FakeRow:
    """替掉 NodeSystemEnvEntry 行对象（只用到本模块读写的字段）。"""

    def __init__(self, *, kind="skill", name="x", platform_managed=False, archived=None):
        self.id = uuid.uuid4()
        self.node_id = NODE_ID
        self.kind = kind
        self.name = name
        self.version = ""
        self.provider = ""
        self.readers = ""
        self.path = ""
        self.description = ""
        self.platform_managed = platform_managed
        self.archived_reference_id = archived
        self.reported_at = None
        self.deleted = False
        self.saved_fields: list[str] = []

    async def save(self, update_fields=None):
        self.saved_fields.extend(update_fields or [])

    async def delete(self):
        self.deleted = True


def _install_nodes(monkeypatch, fake):
    """system_env_service 里 nodes_service 是函数内延迟 import，故打模块属性。"""
    from user_platform import nodes_service as nodes_module

    monkeypatch.setattr(nodes_module, "nodes_service", fake)


def _install_entry_model(monkeypatch, *, get_result=None):
    """替掉 ORM 入口，返回可断言的假行。"""
    calls: dict = {"filter": [], "created": [], "lock": []}

    class _Query:
        def __init__(self, **kw):
            calls["filter"].append(kw)

        async def delete(self):
            calls["deleted"] = True
            return 1

        def order_by(self, *_a):
            return self

        def using_db(self, _db):
            # refresh_system_env pins each queryset to the transaction conn.
            return self

        def __await__(self):
            async def _rows():
                return []
            return _rows().__await__()

    class _FakeConn:
        async def execute_query(self, sql, params=None):
            calls["lock"].append((sql, params))

    class _FakeTxn:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, *_exc):
            return False

    class _Model:
        @staticmethod
        def filter(**kw):
            return _Query(**kw)

        @staticmethod
        async def get_or_none(**_kw):
            return get_result

        @staticmethod
        async def create(**kw):
            # using_db is dropped: the real ORM takes it as a create kwarg.
            kw.pop("using_db", None)
            row = _FakeRow(kind=kw.get("kind", "skill"), name=kw.get("name", "x"),
                           platform_managed=kw.get("platform_managed", False),
                           archived=kw.get("archived_reference_id"))
            calls["created"].append(kw)
            return row

    monkeypatch.setattr(system_env_service, "NodeSystemEnvEntry", _Model)
    monkeypatch.setattr(system_env_service, "in_transaction", lambda **_kw: _FakeTxn())
    return calls


# ── 守卫 ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_requires_node_grant(monkeypatch):
    """越权节点直接拒，不打节点。"""
    _install_nodes(monkeypatch, _FakeNodesService(granted=False))
    with pytest.raises(SystemEnvError) as exc:
        await system_env_service.refresh_system_env(USER_ID, NODE_ID)
    assert exc.value.code == "permission_denied"


@pytest.mark.asyncio
async def test_requires_system_env_enabled(monkeypatch):
    """节点没开系统环境时给明确的 412，而不是等节点侧报错。"""
    _install_nodes(monkeypatch, _FakeNodesService(system_env="false"))
    with pytest.raises(SystemEnvError) as exc:
        await system_env_service.refresh_system_env(USER_ID, NODE_ID)
    assert exc.value.code == "failed_precondition"


@pytest.mark.asyncio
async def test_empty_capabilities_does_not_block(monkeypatch):
    """能力表为空 = 控制面降级快照，不据此判死（与派发前守卫一致）。"""
    fake = _FakeNodesService(system_env=None)
    fake.live = {"online": True, "capabilities": {}}
    _install_nodes(monkeypatch, fake)
    _install_entry_model(monkeypatch)
    await system_env_service.refresh_system_env(USER_ID, NODE_ID)


# ── 刷新（node → server 快照）──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_replaces_snapshot(monkeypatch):
    """整表替换：节点上删掉的资源必须从平台侧消失，不能留残行。"""
    fake = _FakeNodesService()
    fake.inspect_result = {"installed": [
        {"kind": "skill", "name": "a", "version": "1.0", "platform_managed": True,
         "provider": "claude", "path": ".claude/skills/a"},
        {"kind": "mcp", "name": "m", "platform_managed": False},
        {"kind": "skill", "name": "", "version": "x"},  # 无名条目丢弃
    ]}
    _install_nodes(monkeypatch, fake)
    calls = _install_entry_model(monkeypatch)

    rows = await system_env_service.refresh_system_env(USER_ID, NODE_ID)

    assert calls.get("deleted") is True
    assert len(calls["created"]) == 2
    assert {item["name"] for item in calls["created"]} == {"a", "m"}
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_refresh_keeps_cross_provider_same_name_mcp(monkeypatch):
    """同名 MCP server 在两个编辑器配置里 = 节点按 (provider, name) 上报两行。

    存储唯一键含 provider，refresh 必须两行都落库——曾因 (node_id, kind, name)
    唯一键在第二个 insert 上 500（操作者把 mcpServers 键错嵌进两个编辑器配置）。
    """
    fake = _FakeNodesService()
    fake.inspect_result = {"installed": [
        {"kind": "mcp", "name": "mcpServers", "provider": "gemini", "readers": ["gemini"]},
        {"kind": "mcp", "name": "mcpServers", "provider": "opencode", "readers": ["opencode"]},
    ]}
    _install_nodes(monkeypatch, fake)
    calls = _install_entry_model(monkeypatch)

    rows = await system_env_service.refresh_system_env(USER_ID, NODE_ID)

    assert len(calls["created"]) == 2
    assert {(c["kind"], c["name"], c["provider"]) for c in calls["created"]} == {
        ("mcp", "mcpServers", "gemini"),
        ("mcp", "mcpServers", "opencode"),
    }
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_refresh_serializes_via_per_node_advisory_lock(monkeypatch):
    """整表替换必须先拿 per-node advisory lock 再删再插，串行化并发 refresh。"""
    fake = _FakeNodesService()
    _install_nodes(monkeypatch, fake)
    calls = _install_entry_model(monkeypatch)

    await system_env_service.refresh_system_env(USER_ID, NODE_ID)

    assert len(calls["lock"]) == 1
    sql, params = calls["lock"][0]
    assert sql.strip().startswith("SELECT pg_advisory_xact_lock")
    assert params == [f"system-env-refresh:{NODE_ID}"]


# ── 安装（server → node，增量）────────────────────────────────────────────


@pytest.mark.asyncio
async def test_install_requires_file_kind(monkeypatch):
    """MCP 不落盘，故不能安装（写操作者的 ~/.mcp.json 会泄任务凭据）。"""
    _install_nodes(monkeypatch, _FakeNodesService())
    with pytest.raises(SystemEnvError) as exc:
        await system_env_service.install_to_system_env(
            USER_ID, NODE_ID, kind="mcp", resource_id=str(uuid.uuid4()),
            overwrite=False, team_id=TEAM_ID,
        )
    assert exc.value.code == "invalid_argument"


@pytest.mark.asyncio
async def test_install_passes_resolved_spec_and_overwrite(monkeypatch):
    """spec 由服务端按 manifest 重建（不信客户端字段），overwrite 原样透传。"""
    fake = _FakeNodesService()
    _install_nodes(monkeypatch, fake)
    _install_entry_model(monkeypatch)

    async def resolve(user_id, team_id, kind, bindings, request_base_url=""):
        assert kind == "skill"
        assert bindings == [{"resource_id": "rid-1"}]
        return [{"name": "resolved-skill", "source": "archive", "url": "http://x/fetch"}]

    monkeypatch.setattr(
        "user_platform.resource_reference_service.resolve_reference_specs", resolve
    )

    touched = await system_env_service.install_to_system_env(
        USER_ID, NODE_ID, kind="skill", resource_id="rid-1",
        overwrite=True, team_id=TEAM_ID, request_base_url="http://console",
    )

    assert touched and touched[0]["path"] == "installed"
    assert len(fake.sync_calls) == 1
    call = fake.sync_calls[0]
    assert call["overwrite"] is True
    assert call["skills"][0]["name"] == "resolved-skill"
    assert call["plugins"] == []
    assert call["remove"] == []


@pytest.mark.asyncio
async def test_install_rejects_unresolvable_resource(monkeypatch):
    """未授权/无法解析的资源不下发（授权在 resolve_reference_specs 里）。"""
    _install_nodes(monkeypatch, _FakeNodesService())

    async def resolve(*_a, **_kw):
        return []

    monkeypatch.setattr(
        "user_platform.resource_reference_service.resolve_reference_specs", resolve
    )
    with pytest.raises(SystemEnvError) as exc:
        await system_env_service.install_to_system_env(
            USER_ID, NODE_ID, kind="skill", resource_id="rid-x",
            overwrite=False, team_id=TEAM_ID,
        )
    assert exc.value.code == "invalid_argument"


# ── 卸载（节点级：删文件 + 删平台记录）────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_refuses_operator_owned(monkeypatch):
    """本机自有的只展示不删——与节点侧 manifest 复核同一边界。"""
    fake = _FakeNodesService()
    _install_nodes(monkeypatch, fake)
    row = _FakeRow(platform_managed=False, name="operator-skill")
    _install_entry_model(monkeypatch, get_result=row)

    with pytest.raises(SystemEnvError) as exc:
        await system_env_service.remove_from_system_env(
            USER_ID, NODE_ID, kind="skill", name="operator-skill"
        )
    assert exc.value.code == "permission_denied"
    assert fake.sync_calls == []  # 未向节点下发任何删除
    assert row.deleted is False


@pytest.mark.asyncio
async def test_remove_platform_managed_deletes_files_and_row(monkeypatch):
    fake = _FakeNodesService()
    _install_nodes(monkeypatch, fake)
    row = _FakeRow(platform_managed=True, name="platform-skill")
    _install_entry_model(monkeypatch, get_result=row)

    result = await system_env_service.remove_from_system_env(
        USER_ID, NODE_ID, kind="skill", name="platform-skill"
    )

    assert result["removed"] is True
    assert fake.sync_calls[0]["remove"] == ["skill/platform-skill"]
    assert row.deleted is True


@pytest.mark.asyncio
async def test_remove_missing_entry_is_not_found(monkeypatch):
    _install_nodes(monkeypatch, _FakeNodesService())
    _install_entry_model(monkeypatch, get_result=None)
    with pytest.raises(SystemEnvError) as exc:
        await system_env_service.remove_from_system_env(
            USER_ID, NODE_ID, kind="skill", name="ghost"
        )
    assert exc.value.code == "not_found"


# ── 归档（node → 平台资源库）──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_archive_uses_stable_market_id_and_creates_reference(monkeypatch):
    """同一资源重复归档要落到同一个 market_id（刷新而非新增一条）。"""
    fake = _FakeNodesService()
    _install_nodes(monkeypatch, fake)
    row = _FakeRow(platform_managed=False, name="local-skill")
    _install_entry_model(monkeypatch, get_result=row)

    reference_id = uuid.uuid4()
    seen: dict = {}

    from user_platform import system_env_upload

    monkeypatch.setattr(system_env_upload, "base_url", lambda: "http://console")
    monkeypatch.setattr(
        system_env_upload, "issue_upload_token",
        lambda market_id, module, ttl_seconds=600: seen.setdefault("token_for", (market_id, module)) and "tok" or "tok",
    )

    async def finalize(module, market_id, name):
        seen["finalize"] = (module, market_id, name)
        return ("static/resource_mirrors/skills/x.tar.gz", 10, {"id": market_id, "name": name})

    async def upsert(**kwargs):
        seen["upsert"] = kwargs
        return {"id": str(reference_id), "name": kwargs["name"], "display_name": kwargs["name"]}

    monkeypatch.setattr(system_env_upload, "finalize_upload", finalize)
    monkeypatch.setattr(system_env_upload, "upsert_reference_from_manifest", upsert)

    result = await system_env_service.archive_system_env_resource(
        USER_ID, NODE_ID, kind="skill", name="local-skill", team_id=TEAM_ID
    )

    assert result["archived"] is True
    assert result["reference_id"] == str(reference_id)
    # market_id 由 (node, kind, name) 决定：两次归档同一资源必然同键。
    again = system_env_service._archive_market_id(NODE_ID, "skill", "local-skill")
    assert seen["finalize"][1] == again
    assert fake.archive_calls[0]["upload_url"].endswith("/api/v1/resources/system-env/upload")
    assert row.archived_reference_id == reference_id
    assert "archived_reference_id" in row.saved_fields


@pytest.mark.asyncio
async def test_archive_market_id_is_filename_safe():
    """market_id 会进文件名，必须净化掉路径分隔符与 .. 段。"""
    key = system_env_service._archive_market_id("node/../x", "skill", "a b/c")
    assert "/" not in key
    assert "\\" not in key
    assert ".." not in key
    assert len(key) <= 180
    # 同输入必须同输出：重复归档要落回同一条镜像/引用。
    assert key == system_env_service._archive_market_id("node/../x", "skill", "a b/c")
