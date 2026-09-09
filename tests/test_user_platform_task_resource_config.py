"""Task resource config resolution + redaction + merge (Phase A1).

Pure-logic tests for the canonical Task resource path: the DTO never leaks
secret-bearing wire-spec keys (tokens, headers, env), the MCP base merges with
the per-task overlay so issue-workflow survives a hot-update, ``resource_id``
refs are folded from ``extra.skill_ids``/``extra.plugin_ids`` and unauthorized
ids are rejected (→ 403) before persistence, and audit bodies carry only
per-kind counts. No DB needed — the service helpers operate on plain dicts.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from user_platform import routes_task
from user_platform import task_service as task_service_module
from user_platform.task_service import (
    TaskService,
    _merge_mcp_config,
    _redact_resource_config,
)


# ---- redaction: secrets and URL token never leave the service -------------

def test_redact_strips_secret_keys_and_url_token():
    redacted = _redact_resource_config([
        {
            "name": "issue-workflow",
            "type": "sse",
            "url": "https://host/mcp/issue-workflow/sse?token=secret-abc&keep=1",
            "token": "secret-abc",
            "headers": {"Authorization": "Bearer x"},
            "env": {"KEY": "v"},
        },
        {"name": "plain", "url": "https://host/no-token"},
    ])
    first = redacted[0]
    assert "token" not in first
    assert "headers" not in first
    assert "env" not in first
    assert "token=" not in first["url"]
    assert "keep=1" in first["url"]  # non-token query params preserved
    assert redacted[1]["url"] == "https://host/no-token"


def test_redact_handles_non_list_and_non_dict_entries():
    assert _redact_resource_config(None) == []
    assert _redact_resource_config([None, "x", 5, {"name": "ok"}]) == [{"name": "ok"}]


# ---- merge: overlay wins, issue-workflow survives base update -------------

def test_merge_overlay_wins_over_base_by_name():
    merged = _merge_mcp_config(
        [{"name": "shared", "url": "https://old"}, {"name": "issue-workflow", "url": "https://base"}],
        [{"name": "issue-workflow", "url": "https://overlay-token"}],
    )
    by_name = {item["name"]: item for item in merged}
    assert by_name["shared"]["url"] == "https://old"
    assert by_name["issue-workflow"]["url"] == "https://overlay-token"


def test_merge_keeps_unnamed_entries_prepended():
    merged = _merge_mcp_config([{"url": "https://unnamed"}], [{"name": "x"}])
    assert merged[0]["url"] == "https://unnamed"
    assert merged[1]["name"] == "x"


# ---- resolver: skill_ids/plugin_ids folded + unauthorized rejected --------

def _user():
    return SimpleNamespace(id="user-1", role="user")


def _request():
    # request.base_url is a property on Starlette Request; the helper reads
    # ``str(request.base_url).rstrip("/")``.
    return SimpleNamespace(base_url="https://api.test/")


async def _fake_team_id(_uid: str) -> str:
    return "team-1"


@pytest.mark.asyncio
async def test_resolve_folds_skill_ids_into_config(monkeypatch):
    async def fake_resolve(user_id, team_id, resource_type, bindings, *, request_base_url=""):
        assert resource_type == "skill"
        assert bindings == [{"resource_id": "sk-1"}, {"resource_id": "sk-2"}]
        return [{"name": "sk-1", "source": "archive", "url": "https://x"}, {"name": "sk-2", "source": "archive", "url": "https://y"}]

    monkeypatch.setattr(routes_task, "resolve_reference_specs", fake_resolve)
    monkeypatch.setattr(routes_task, "resolve_team_id", _fake_team_id)

    req = {"extra": {"skill_ids": ["sk-1", "sk-2"]}}
    patch = await routes_task._resolve_task_resource_configs(_user(), req, _request())
    assert [s["name"] for s in patch["skill_config"]] == ["sk-1", "sk-2"]


@pytest.mark.asyncio
async def test_resolve_folds_collection_dict_bindings_with_entries(monkeypatch):
    """集合子技能勾选：skill_ids 里 {resource_id, entries} 原样下传给 resolve，
    resolve 按 entries 过滤只下发勾中的子技能（第二期任务勾选链路）。"""
    captured: list[dict] = []

    async def fake_resolve(user_id, team_id, resource_type, bindings, *, request_base_url=""):
        assert resource_type == "skill"
        captured.extend(bindings)
        return [{"name": b.get("resource_id"), "source": "github"} for b in bindings]

    monkeypatch.setattr(routes_task, "resolve_reference_specs", fake_resolve)
    monkeypatch.setattr(routes_task, "resolve_team_id", _fake_team_id)

    req = {"extra": {"skill_ids": [
        "sk-plain",
        {"resource_id": "col-1", "entries": ["pdf", "slides"]},
        {"resource_id": "col-2"},  # 集合不带 entries = 整个集合
    ]}}
    patch = await routes_task._resolve_task_resource_configs(_user(), req, _request())
    assert {"resource_id": "sk-plain"} in captured
    assert {"resource_id": "col-1", "entries": ["pdf", "slides"]} in captured
    assert {"resource_id": "col-2"} in captured
    assert len(patch["skill_config"]) == 3


@pytest.mark.asyncio
async def test_resolve_routes_reference_id_bindings_to_new_store(monkeypatch):
    """{reference_id} 绑定路由到新表 resource_store（统一资源池）；旧 resource_id 走旧链路。"""
    import resource_store as rstore
    from user_platform import resource_reference_service as refsvc

    async def fake_team_admin(user_id, team_id):
        return False

    async def fake_visible(user_id, team_id, *, resource_type=None, is_admin=False):
        assert resource_type in ("skill", "skills")  # skill 主类型会连查 skills 集合
        return [{
            "id": "new-ref-1", "params": {}, "display_name": "", "version": "",
            "resource": {
                "resource_type": "skills",
                "resource_data": {
                    "clone_url": "https://github.com/o/r.git", "ref": "main",
                    "entries": [{"name": "pdf", "path": "skills/pdf", "entry": "SKILL.md"}],
                },
                "source_data": {"repo_full_name": "o/r"}, "name": "o/r",
                "display_name": "", "version": "",
            },
        }]

    async def fake_specs(references, bindings, *, request_base_url=""):
        assert bindings == [{"reference_id": "new-ref-1", "entries": ["pdf"]}]
        return [{"name": "o/r/pdf", "source": "github", "url": "https://github.com/o/r.git",
                 "path": "skills/pdf", "ref": "main"}]

    async def no_refs(*_a, **_kw):
        return []

    monkeypatch.setattr(refsvc, "_team_is_admin", fake_team_admin)
    monkeypatch.setattr(rstore, "visible_references", fake_visible)
    monkeypatch.setattr(rstore, "resolve_specs", fake_specs)
    monkeypatch.setattr(refsvc, "visible_references", no_refs)

    specs = await refsvc.resolve_reference_specs(
        "user-1", "team-1", "skill",
        [{"reference_id": "new-ref-1", "entries": ["pdf"]}],
    )
    assert specs == [{"name": "o/r/pdf", "source": "github",
                      "url": "https://github.com/o/r.git", "path": "skills/pdf", "ref": "main"}]


@pytest.mark.asyncio
async def test_resolve_merges_explicit_config_with_ids(monkeypatch):
    async def fake_resolve(user_id, team_id, resource_type, bindings, *, request_base_url=""):
        # Mirror the real resolver: resource_id refs become resolved specs,
        # plain dicts (no resource_id) pass through verbatim.
        out = []
        for b in bindings:
            rid = b.get("resource_id")
            out.append({"name": rid, "source": "archive"} if rid else dict(b))
        return out

    monkeypatch.setattr(routes_task, "resolve_reference_specs", fake_resolve)
    monkeypatch.setattr(routes_task, "resolve_team_id", _fake_team_id)

    req = {
        "extra": {"plugin_ids": ["p-1"]},
        "plugin_config": [{"name": "explicit", "url": "https://x"}],
    }
    patch = await routes_task._resolve_task_resource_configs(_user(), req, _request())
    names = [p["name"] for p in patch["plugin_config"]]
    assert "p-1" in names and "explicit" in names


@pytest.mark.asyncio
async def test_resolve_rejects_unauthorized_resource_id(monkeypatch):
    async def fake_resolve(user_id, team_id, resource_type, bindings, *, request_base_url=""):
        raise ValueError("resource_not_granted:nope")

    monkeypatch.setattr(routes_task, "resolve_reference_specs", fake_resolve)
    monkeypatch.setattr(routes_task, "resolve_team_id", _fake_team_id)

    req = {"extra": {"skill_ids": ["nope"]}}
    with pytest.raises(HTTPException) as exc:
        await routes_task._resolve_task_resource_configs(_user(), req, _request())
    assert exc.value.status_code == 403
    assert "资源未授权" in exc.value.detail


@pytest.mark.asyncio
async def test_resolve_dedups_by_name(monkeypatch):
    async def fake_normalize(user_id, team_id, bindings):
        # normalize 返回服务绑定（service_id / resource_id），按绑定键去重。
        seen = set()
        out = []
        for b in bindings:
            key = str(b.get("service_id") or b.get("resource_id") or b.get("name") or "")
            if key in seen:
                continue
            seen.add(key)
            out.append({"service_id": 1} if not b.get("resource_id") else dict(b))
        return out

    from user_platform.resource_reference_service import normalize_mcp_bindings

    monkeypatch.setattr(routes_task, "normalize_mcp_bindings", fake_normalize)
    monkeypatch.setattr(routes_task, "resolve_team_id", _fake_team_id)

    req = {"mcp_config": [{"name": "dup"}, {"name": "dup"}]}
    patch = await routes_task._resolve_task_resource_configs(_user(), req, _request())
    assert len(patch["mcp_config"]) == 1  # 同服务两条 = 一条绑定
    assert patch["mcp_config"][0] == {"service_id": 1}


# ---- audit: only counts, never full wire specs ---------------------------

def test_audit_replaces_config_lists_with_counts():
    audited = routes_task._audit_resource_counts({
        "content": "hi",
        "mcp_config": [{"name": "a"}, {"name": "b"}],
        "skill_config": [{"name": "s"}],
        "plugin_config": [],
        "title": "t",
    })
    assert audited["mcp_config"] == 2
    assert audited["skill_config"] == 1
    assert audited["plugin_config"] == 0
    assert audited["content"] == "hi"
    assert audited["title"] == "t"


@pytest.mark.asyncio
@pytest.mark.parametrize(("load_session", "fresh"), [(True, False), (False, True)])
async def test_restart_task_runtime_maps_load_session_to_fresh(monkeypatch, load_session, fresh):
    task = SimpleNamespace(id="task-1", user_id="user-1", mcp_user_id=None)
    calls = []

    async def owned(*_args, **_kwargs):
        return task

    async def live(bound_task):
        assert bound_task is task
        return "session-1"

    class Client:
        async def restart_node_session_runtime(self, session_id, *, fresh=False):
            calls.append((session_id, fresh))
            return {"ok": True}

    # restart re-enables the task principal before resuming; stub it so the
    # minimal fixture does not need a live DB / mcp_plugin_store.
    async def enable(self, bound_task):
        return None

    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", owned)
    monkeypatch.setattr(service, "_live_node_session_id", live)
    monkeypatch.setattr(TaskService, "_enable_task_mcp_principal", enable)
    monkeypatch.setattr(task_service_module, "get_local_node_client", lambda: Client())

    result = await service.restart_task_runtime(
        "user-1", "task-1", load_session=load_session
    )

    assert calls == [("session-1", fresh)]
    assert result["load_session"] is load_session
    assert result["detail"] == {"ok": True}


# ---- resolve_reference_specs: service_id 绑定（内置/平台/个人 MCP 直选） ----


class _FakeConn:
    """Stub asyncpg connection carrying one mcp_services row."""

    def __init__(self, row):
        self.row = row

    async def fetchrow(self, query, *params):
        if "FROM mcp_services" in query:
            return self.row if self.row is not None else None
        raise AssertionError(f"unexpected query: {query}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return self.conn


@pytest.mark.asyncio
async def test_service_id_binding_resolves_wire_spec(monkeypatch):
    """{service_id} 绑定 → 读 mcp_services 行 + 授权校验 → 节点可用的 wire spec。"""
    import mcp_plugin_store
    import db as db_module
    from user_platform import resource_reference_service as rrs

    async def can_use(*, user_id, service_id, groups=None):
        return user_id == "user-1" and service_id == 42

    monkeypatch.setattr(mcp_plugin_store, "can_use_service", can_use)

    async def no_refs(*_a, **_kw):
        return []

    monkeypatch.setattr(rrs, "visible_references", no_refs)
    conn = _FakeConn({
        "id": 42, "name": "cdp-bridge", "transport": "remote", "command": "",
        "args": [], "env_template": {}, "url": "http://127.0.0.1:9693/sse",
        "headers": {"X-K": "v"}, "enabled": True,
    })
    monkeypatch.setattr(db_module.PostgresClient, "pool", _FakePool(conn))

    specs = await rrs.resolve_reference_specs(
        "user-1", "team-1", "mcp",
        [{"service_id": 42}, {"name": "legacy", "command": "echo"}],
    )

    assert specs[0] == {
        "name": "cdp-bridge",
        "type": "remote",
        "transport": "remote",
        "command": "",
        "args": [],
        "env": {},
        "url": "http://127.0.0.1:9693/sse",
        "headers": {"X-K": "v"},
    }
    # 无 resource_id/service_id 的旧形态原样透传。
    assert specs[1] == {"name": "legacy", "command": "echo"}


@pytest.mark.asyncio
async def test_service_id_binding_rejects_unauthorized(monkeypatch):
    """未授权的服务 → 明确报错，绝不静默丢弃或透传浏览器摘要。"""
    import mcp_plugin_store
    from user_platform import resource_reference_service as rrs

    async def can_use(*, user_id, service_id, groups=None):
        return False

    monkeypatch.setattr(mcp_plugin_store, "can_use_service", can_use)

    async def no_refs(*_a, **_kw):
        return []

    monkeypatch.setattr(rrs, "visible_references", no_refs)

    with pytest.raises(ValueError, match="mcp_not_authorized:42"):
        await rrs.resolve_reference_specs("user-1", "team-1", "mcp", [{"service_id": 42}])


@pytest.mark.asyncio
async def test_service_id_binding_rejects_disabled_service(monkeypatch):
    """已停用的服务不可挂载（mcp_not_ready），与团队引用口径一致。"""
    import mcp_plugin_store
    import db as db_module
    from user_platform import resource_reference_service as rrs

    async def can_use(*, user_id, service_id, groups=None):
        return True

    monkeypatch.setattr(mcp_plugin_store, "can_use_service", can_use)

    async def no_refs(*_a, **_kw):
        return []

    monkeypatch.setattr(rrs, "visible_references", no_refs)
    monkeypatch.setattr(db_module.PostgresClient, "pool", _FakePool(_FakeConn(None)))

    with pytest.raises(ValueError, match="mcp_not_ready:42"):
        await rrs.resolve_reference_specs("user-1", "team-1", "mcp", [{"service_id": 42}])


# ---- _task_mcp_service_ids: service_id spec → 授权服务集 ----


@pytest.mark.asyncio
async def test_task_mcp_service_ids_reads_service_id_specs(monkeypatch):
    """任务 principal 授权直接认 spec 里的 service_id，不再只靠 name 回查。"""
    import mcp_plugin_store

    async def get_service_by_name(name):
        return {"id": 7} if name == "team-mcp" else None

    monkeypatch.setattr(mcp_plugin_store, "get_service_by_name", get_service_by_name)

    service = TaskService()
    task = SimpleNamespace(mcp_config=[
        {"name": "cdp-bridge", "type": "remote", "service_id": 42},
        {"name": "team-mcp", "command": "npx"},
        {"name": "overlay-local", "url": "ws://session-local"},  # 非 catalog 服务
        {"service_id": 42},  # 与上面重复 → 去重
        "junk",
    ])
    ids = await service._task_mcp_service_ids(task)
    assert ids == [42, 7]
