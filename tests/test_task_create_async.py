"""Deferred create dispatch: the「准备中」entrypoint.

The public create route passes ``defer_dispatch=True`` so the HTTP request
returns as soon as the row is written, while the node does the slow part
(git clone, skill/plugin/MCP downloads, runtime start — up to the 30s ack
timeout) in a background coroutine. These tests pin the contract that matters
to the UI and to recovery:

- the response comes back with the row in ``workspace_state='dispatching'``
  (projected as a virtual「准备中」stage) before the dispatch is awaited;
- synchronous validations (parent key, node grant) still raise inside the
  request, not minutes later;
- the background coroutine lands the session handle, pushes the first turn,
  and marks retryable/terminal failures exactly like the sync path did;
- the pre-persisted first turn never survives a terminal failure as
  ``pending`` — replay must not show a phantom in-flight message.
"""
from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest

from monkeycode_compat import task_service as task_service_module
from monkeycode_compat.task_service import TaskService

TASK_ID = uuid.uuid4()
USER_ID = uuid.uuid4()
NODE_ID = "node-1"


def _task(status="pending", node_session_id=None, content="hello world"):
    task = SimpleNamespace(
        id=TASK_ID,
        user_id=USER_ID,
        provider="claude",
        node_id=NODE_ID,
        node_session_id=node_session_id,
        workspace_state="pending",
        status=status,
        title=None,
        mode=None,
        models_snapshot=["m1"],
        config_snapshot={},
        mcp_config=[],
        skill_config=[],
        plugin_config=[],
        content=content,
        api_key_id=42,
        mcp_user_id=None,
        created_at=None,
        completed_at=None,
        summary=None,
        updated_at=None,
        last_active_at=None,
        kind="develop",
        sub_type=None,
        workspace_key=None,
        workspace_source_task_id=None,
        source_editor_id=None,
        source_editor_session_id=None,
        runtime_stage="",
        runtime_stage_detail=None,
        runtime_stage_ok=True,
        expires_at=None,
    )

    async def _save(**_kw):
        return None

    task.save = _save
    return task


class _Client:
    enabled = True

    def __init__(self, dispatch_response=None, raise_exc=None):
        self.dispatch_response = dispatch_response or {"accepted": True, "sessionId": "session-1"}
        self.raise_exc = raise_exc
        self.dispatch_calls = 0
        self.sent = []
        # dispatch_session blocks until released; lets a test observe the row
        # state while the background dispatch is still in flight.
        self.gate = asyncio.Event()
        self.gate.set()

    async def dispatch_session(self, node_id, session):
        self.dispatch_calls += 1
        await self.gate.wait()
        if self.raise_exc:
            raise self.raise_exc
        return self.dispatch_response

    async def send_session_input(self, session_id, kind, content, **kwargs):
        self.sent.append((session_id, kind, content, kwargs))
        return {"accepted": True}


def _install(monkeypatch, client, task=None):
    task = task or _task()

    async def disable(*_a, **_kw):
        return None

    async def ensure(*_a, **_kw):
        return None

    async def attach(*_a, **_kw):
        return None

    async def persist_stub(*args, **kwargs):
        return SimpleNamespace(
            id=uuid.uuid4(),
            client_message_id=kwargs.get("client_message_id"),
            delivery_status=kwargs.get("delivery_status", "pending"),
            delivery_attempt=1,
            started_at=None,
        )

    async def claim_stub(*args, **kwargs):
        return "pending", 1

    async def transition_stub(*args, **kwargs):
        return True

    monkeypatch.setattr(task_service_module, "get_local_node_client", lambda: client)
    monkeypatch.setattr(TaskService, "_disable_task_mcp_principal", disable)
    monkeypatch.setattr(TaskService, "_ensure_task_mcp_principal", ensure)
    monkeypatch.setattr(TaskService, "_enable_task_mcp_principal", ensure)
    monkeypatch.setattr(TaskService, "_park_task_api_key", disable)
    monkeypatch.setattr(TaskService, "_resume_task_api_key", ensure)
    monkeypatch.setattr(TaskService, "_attach_issue_workflow_mcp", attach)
    monkeypatch.setattr(TaskService, "_persist_user_input_item", persist_stub)
    monkeypatch.setattr(TaskService, "_claim_message_for_dispatch", claim_stub)
    monkeypatch.setattr(TaskService, "_transition_message_status", transition_stub)
    monkeypatch.setattr(TaskService, "_refresh_api_key_snapshot", ensure)
    monkeypatch.setattr(TaskService, "_rewrite_repo_for_git_proxy", attach)
    monkeypatch.setattr(
        task_service_module.nodes_service, "user_can_use_node",
        lambda uid, nid: _binding_link(),
    )
    # create_task ORM surface: Task.create / ProjectTask.create / conditional
    # Task.filter(...).update(...) / detail enrichment reads.
    async def task_create(**kwargs):
        for key, value in kwargs.items():
            setattr(task, key, value)
        return task

    async def project_create(**kwargs):
        return SimpleNamespace(**kwargs)

    async def task_filter_first():
        return None

    async def task_filter_get_or_none(**_kw):
        return task

    async def task_filter_update(**_kw):
        return 1

    monkeypatch.setattr(task_service_module.Task, "create", classmethod(lambda cls, **kw: task_create(**kw)))
    monkeypatch.setattr(task_service_module.ProjectTask, "create", classmethod(lambda cls, **kw: project_create(**kw)))
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(
            update=lambda **u: task_filter_update(**u),
            first=lambda: task_filter_first(),
            order_by=lambda *a: SimpleNamespace(first=lambda: task_filter_first()),
        ),
    )
    monkeypatch.setattr(
        task_service_module.Task, "get_or_none",
        classmethod(lambda cls, **kw: task_filter_get_or_none(**kw)),
    )
    monkeypatch.setattr(
        task_service_module.TaskEvent, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: task_filter_update(**u)),
    )
    import db as db_module

    async def get_key(_id):
        return {"key": "sk-test", "disabled": False}

    async def create_child_key(*_a, **_kw):
        return {"id": 7, "key": "sk-child", "usage_limit": None}

    monkeypatch.setattr(db_module.PostgresClient, "get_api_key_by_id", classmethod(lambda cls, _id: get_key(_id)))
    monkeypatch.setattr(
        db_module.PostgresClient, "create_task_child_api_key",
        classmethod(lambda cls, *a, **kw: create_child_key(*a, **kw)),
    )
    # _task_detail reads a pile of enrichment; stub to the row itself.
    async def detail(t):
        return {"id": str(t.id), "status": t.status, "workspace_state": t.workspace_state}

    monkeypatch.setattr(task_service_module, "_task_detail", detail)
    emitted = []

    def emit(*_a, **_kw):
        emitted.append(1)

    monkeypatch.setattr(TaskService, "_emit_task_created", emit)
    return task, emitted


async def _binding_link():
    return SimpleNamespace(node_id=NODE_ID)


def _req(**overrides):
    req = {
        "content": "hello world",
        "cli_name": "claude",
        "node_id": NODE_ID,
        "parent_api_key_id": 1,
        "task_type": "develop",
    }
    req.update(overrides)
    return req


@pytest.mark.asyncio
async def test_deferred_create_returns_before_dispatch_completes(monkeypatch):
    """创建响应不等节点：dispatch 挂起时响应已返回，行处于 dispatching。"""
    client = _Client()
    client.gate.clear()  # hold the dispatch open
    task, _emitted = _install(monkeypatch, client)
    service = TaskService()

    result = await service.create_task(str(USER_ID), _req(), defer_dispatch=True)

    assert result["workspace_state"] == "dispatching"
    assert result["status"] == "pending"
    # The background coroutine is merely scheduled — create returned before the
    # node was even contacted, which is the whole point of defer_dispatch.
    assert client.dispatch_calls == 0
    await asyncio.sleep(0)  # background dispatch starts and blocks on the gate
    assert client.dispatch_calls == 1
    # The row the UI will project as 准备中 · 正在连接执行节点.
    assert task.workspace_state == "dispatching"
    client.gate.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_deferred_dispatch_completes_and_pushes_first_turn(monkeypatch):
    client = _Client()
    task, emitted = _install(monkeypatch, client)
    service = TaskService()

    await service.create_task(str(USER_ID), _req(), defer_dispatch=True)
    # Let the background coroutine run to completion.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert client.dispatch_calls == 1
    assert task.node_session_id == "session-1"
    assert task.workspace_state == "ready"
    assert task.status == "processing"  # first turn delivered
    assert client.sent and client.sent[0][1] == "human_message"
    # Every delivered turn is prefixed with the git-credential policy note; the
    # user's own text follows it verbatim.
    assert client.sent[0][2] == task_service_module._with_git_credential_note("hello world")
    assert client.sent[0][2].endswith("hello world")
    # task.created is emitted exactly once (by the create response, not again
    # when the background dispatch finishes).
    assert len(emitted) == 1


@pytest.mark.asyncio
async def test_deferred_create_still_validates_node_grant_synchronously(monkeypatch):
    """越权节点必须在请求内 403（node_forbidden），不能变成后台失败行。"""
    client = _Client()
    task, _emitted = _install(monkeypatch, client)
    service = TaskService()

    async def no_link(uid, nid):
        return None

    monkeypatch.setattr(task_service_module.nodes_service, "user_can_use_node", no_link)

    with pytest.raises(ValueError, match="node_forbidden"):
        await service.create_task(str(USER_ID), _req(), defer_dispatch=True)
    assert client.dispatch_calls == 0


@pytest.mark.asyncio
async def test_deferred_create_without_parent_key_raises(monkeypatch):
    client = _Client()
    _install(monkeypatch, client)
    service = TaskService()

    with pytest.raises(ValueError, match="parent_api_key_required"):
        await service.create_task(str(USER_ID), _req(parent_api_key_id=None), defer_dispatch=True)


@pytest.mark.asyncio
async def test_deferred_retryable_failure_marks_dispatch_failed(monkeypatch):
    from monkeycode_compat.node_client import NodeServerUnavailable

    client = _Client(raise_exc=NodeServerUnavailable("down"))
    task, _emitted = _install(monkeypatch, client)
    service = TaskService()

    await service.create_task(str(USER_ID), _req(), defer_dispatch=True)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert task.workspace_state == "dispatch_failed"
    assert task.status == "pending"  # retryable: recovery worker picks it up
    assert task.config_snapshot.get("dispatch_error")


@pytest.mark.asyncio
async def test_deferred_terminal_failure_cancels_pending_first_turn(monkeypatch):
    """永久派发失败时，预落库的首条消息不能永远停在 pending（回放会把
    未投递的消息渲染成发送中）。"""
    from monkeycode_compat.node_client.errors import Code, RPCError

    client = _Client(raise_exc=RPCError(Code.PERMISSION_DENIED, "node rejected"))
    task, _emitted = _install(monkeypatch, client)
    service = TaskService()
    cancelled = {"count": 0}

    async def cancel(self, task):
        cancelled["count"] += 1

    async def mark(self, task, message, **kw):
        task.workspace_state = "dispatch_failed"
        snapshot = dict(task.config_snapshot or {})
        snapshot["dispatch_error"] = message
        task.config_snapshot = snapshot
        task.status = "error"

    monkeypatch.setattr(TaskService, "_cancel_pending_first_turn", cancel)
    monkeypatch.setattr(TaskService, "_mark_dispatch_failed", mark)

    await service.create_task(str(USER_ID), _req(), defer_dispatch=True)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert task.status == "error"
    assert task.workspace_state == "dispatch_failed"
    assert cancelled["count"] == 1


@pytest.mark.asyncio
async def test_sync_create_still_awaits_dispatch(monkeypatch):
    """内部调用方（webhook review / issue 流程）默认路径保持同步语义。"""
    client = _Client()
    task, emitted = _install(monkeypatch, client)
    service = TaskService()

    result = await service.create_task(str(USER_ID), _req())

    # The dispatch already ran to completion before create_task returned.
    assert client.dispatch_calls == 1
    assert task.node_session_id == "session-1"
    assert task.workspace_state == "ready"
    assert result["status"] == "processing"
    assert len(emitted) == 1
