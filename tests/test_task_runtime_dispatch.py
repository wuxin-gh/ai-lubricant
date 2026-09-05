"""Lazy runtime dispatch for storage-state tasks.

A task created with a node but no live runtime (control plane disabled or
unavailable at create time) should still be startable from the task detail
page, and the first message should trigger dispatch instead of 409ing.
These tests stub the node client, principal helpers, and the key store so the
dispatch path runs without a live DB / agent-compose control plane.
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


def _task(status="pending", node_session_id=None, title=None):
    return SimpleNamespace(
        id=TASK_ID,
        user_id=USER_ID,
        provider="claude",
        node_id=NODE_ID,
        node_session_id=node_session_id,
        workspace_state="pending",
        status=status,
        title=title,
        mode=None,
        models_snapshot=["m1"],
        config_snapshot={},
        mcp_config=[],
        skill_config=[],
        plugin_config=[],
        api_key_id=42,
        mcp_user_id=None,
        save=lambda **kw: _return(None),
    )


def _binding():
    return SimpleNamespace(
        task_id=TASK_ID,
        model_id=uuid.UUID(int=0),
        cli_name="claude",
        project_id=None,
        issue_id=None,
        repo_url=None,
        branch=None,
    )


class _Client:
    enabled = True

    def __init__(self, dispatch_response=None, raise_exc=None, send_errors=None):
        self.dispatch_response = dispatch_response or {"accepted": True, "sessionId": "session-1"}
        self.raise_exc = raise_exc
        self.dispatch_calls = 0
        self.start_calls: list[str] = []
        self.sent = []
        # Per-call send failures: index N raises send_errors[N] when present.
        self.send_errors = list(send_errors or [])

    async def dispatch_session(self, node_id, session):
        self.dispatch_calls += 1
        if self.raise_exc:
            raise self.raise_exc
        return self.dispatch_response

    async def start_node_session_runtime(self, session_id):
        # The dispatch helper probes an existing handle here before trusting it.
        # A stubbed binding that never existed surfaces as NOT_FOUND so the
        # helper clears the stale handle and re-dispatches.
        self.start_calls.append(session_id)
        if session_id == "stale-session":
            from monkeycode_compat.node_client.errors import Code, RPCError

            raise RPCError(Code.NOT_FOUND, f"session {session_id} is not placed on any node")
        return {"accepted": True}

    async def send_session_input(self, session_id, kind, content, **kwargs):
        index = len(self.sent)
        self.sent.append((session_id, kind, content, kwargs))
        if index < len(self.send_errors) and self.send_errors[index] is not None:
            raise self.send_errors[index]
        return {"accepted": True}


def _install(monkeypatch, client, task=None, binding=None, key_row=None):
    import db as db_module

    task = task or _task()
    binding_obj = binding or _binding()
    key = key_row or {"key": "sk-test", "disabled": False}

    async def get_or_none(**_kw):
        return task

    async def filter_first():
        return binding_obj

    async def disable(*_a, **_kw):
        return None

    async def ensure(*_a, **_kw):
        return None

    async def attach(*_a, **_kw):
        return None

    async def get_key(_id):
        return key

    monkeypatch.setattr(task_service_module.Task, "get_or_none", classmethod(lambda cls, **kw: get_or_none(**kw)))
    monkeypatch.setattr(
        task_service_module.ProjectTask,
        "filter",
        lambda **kw: SimpleNamespace(
            first=lambda: filter_first(),
            order_by=lambda *a: SimpleNamespace(first=lambda: filter_first()),
        ),
    )
    monkeypatch.setattr(task_service_module, "get_local_node_client", lambda: client)
    monkeypatch.setattr(TaskService, "_disable_task_mcp_principal", disable)
    monkeypatch.setattr(TaskService, "_ensure_task_mcp_principal", ensure)
    monkeypatch.setattr(TaskService, "_enable_task_mcp_principal", ensure)
    monkeypatch.setattr(TaskService, "_park_task_api_key", disable)
    monkeypatch.setattr(TaskService, "_resume_task_api_key", ensure)
    monkeypatch.setattr(TaskService, "_attach_issue_workflow_mcp", attach)
    monkeypatch.setattr(db_module.PostgresClient, "get_api_key_by_id", classmethod(lambda cls, _id: get_key(_id)))
    monkeypatch.setattr(
        task_service_module.nodes_service, "user_can_use_node",
        lambda uid, nid: _binding_link(),
    )
    monkeypatch.setattr(task_service_module.Task, "save", lambda self, **kw: _return(None))

    # 消息投递协议层打桩：dispatch 用例关心的是派发/投递编排，不关心
    # mc_task_events 行存取（那些由 test_task_message_protocol.py 覆盖）。
    # 注意 monkeypatch 挂到类上会把实例绑成第一个参数，统一 *args 吞掉。
    async def persist_stub(*args, **kwargs):
        return SimpleNamespace(
            id=uuid.uuid4(),
            delivery_status=kwargs.get("delivery_status", "pending"),
            delivery_attempt=1,
            started_at=None,
        )

    async def claim_stub(*args, **kwargs):
        # 永远返回"本请求赢得领取"，让用例继续走投递路径。
        return "pending", 1

    async def transition_stub(*args, **kwargs):
        return True

    monkeypatch.setattr(TaskService, "_persist_user_input_item", persist_stub)
    monkeypatch.setattr(TaskService, "_claim_message_for_dispatch", claim_stub)
    monkeypatch.setattr(TaskService, "_transition_message_status", transition_stub)

    return task


async def _return(value):
    return value


async def _binding_link():
    return SimpleNamespace(node_id=NODE_ID)


@pytest.mark.asyncio
async def test_send_task_message_lazy_dispatches_storage_state(monkeypatch):
    client = _Client()
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(
        service,
        "_owned_task_or_raise",
        lambda *a, **kw: _return(task),
    )
    monkeypatch.setattr(
        task_service_module.Task,
        "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )

    await service.send_task_message(str(USER_ID), str(TASK_ID), "hello")

    assert client.dispatch_calls == 1
    assert client.sent[0][1] == "human_message"
    assert client.sent[0][2] == task_service_module._with_git_credential_note("hello")
    assert task.node_session_id == "session-1"
    assert task.status == "processing"


@pytest.mark.asyncio
async def test_send_task_message_reuses_existing_runtime(monkeypatch):
    client = _Client()
    task = _task(node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    # The title backfill issues a conditional Task.filter(...).update(...).
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )

    await service.send_task_message(str(USER_ID), str(TASK_ID), "hi")

    assert client.dispatch_calls == 0
    assert client.sent[0][0] == "existing"


@pytest.mark.asyncio
async def test_send_task_message_revives_finished_task_by_probing_runtime(monkeypatch):
    """发送即恢复：finished 任务再次发消息时探活既有句柄并启动，而非拒绝。

    之前状态白名单会把 finished 挡在 task_not_startable；现在只要任务行存在
    就允许，且既有句柄经 StartSessionRuntime 探活（stopped session 被节点重新
    拉起），消息照常投递。principal 在此路径上被重新启用。
    """
    client = _Client()
    task = _task(status="finished", node_session_id="existing")
    enables: list[object] = []
    _install(monkeypatch, client, task=task)
    service = TaskService()

    async def enable(self, t):
        enables.append(t)

    monkeypatch.setattr(TaskService, "_enable_task_mcp_principal", enable)
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )

    await service.send_task_message(str(USER_ID), str(TASK_ID), "again")

    assert client.start_calls == ["existing"]  # probed/started, not re-dispatched
    assert client.dispatch_calls == 0
    assert client.sent[0][0] == "existing"
    assert client.sent[0][2] == task_service_module._with_git_credential_note("again")
    assert task.status == "processing"
    assert len(enables) == 1


@pytest.mark.asyncio
async def test_send_task_message_redispatches_stale_placement(monkeypatch):
    """DB keeps a node_session_id the control plane no longer places.

    The node's binding can vanish under a live task row (node deleted, session
    unbound node-side). The first send then gets ``not_found``; the handle must
    be cleared and re-dispatched once so the message lands instead of 500ing.
    """
    from monkeycode_compat.node_client.errors import Code, RPCError

    task = _task(node_session_id="stale-session")
    client = _Client()
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    cleared = {"count": 0}

    def filter_(**kw):
        if "node_session_id" in kw:
            cleared["count"] += 1
        return SimpleNamespace(update=lambda **u: _return(1))

    monkeypatch.setattr(task_service_module.Task, "filter", filter_)

    result = await service.send_task_message(str(USER_ID), str(TASK_ID), "hello")

    assert result["accepted"] is True
    assert cleared["count"] >= 1
    assert client.dispatch_calls == 1
    # The stale handle was rejected by the pre-send probe, so only the fresh
    # session ever receives the message (no doomed first write to stale stdin).
    assert [entry[0] for entry in client.sent] == ["session-1"]
    assert client.sent[0][2] == task_service_module._with_git_credential_note("hello")


@pytest.mark.asyncio
async def test_send_task_message_stale_placement_retries_only_once(monkeypatch):
    """A freshly dispatched session that is still unplaced must not loop.

    Two ``not_found`` in a row means the control plane / node is not actually
    serving right now. That is a retryable 503 (``node_server_unavailable``),
    not a "task has no runtime" 409 — the handle was just rebuilt and the node
    cannot honour it, so the client re-sends after the node returns.
    """
    from monkeycode_compat.node_client.errors import Code, RPCError

    stale = RPCError(Code.NOT_FOUND, "session is not placed on any node")
    client = _Client(send_errors=[stale, stale])
    task = _task(node_session_id="stale-session")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1))
    )

    with pytest.raises(ValueError) as exc:
        await service.send_task_message(str(USER_ID), str(TASK_ID), "hello")

    assert str(exc.value) == "node_server_unavailable"
    # One re-dispatch after the stale-handle probe, then exactly one send-time
    # re-dispatch; the second NOT_FOUND stops (no unbounded loop).
    assert client.dispatch_calls == 2
    assert len(client.sent) == 2


@pytest.mark.asyncio
async def test_send_task_message_unavailable_maps_to_node_server_unavailable(monkeypatch):
    """Node offline mid-send is retryable, so it must not surface as a 500."""
    from monkeycode_compat.node_client.errors import Code, RPCError

    offline = RPCError(Code.UNAVAILABLE, "node node-1 for session x is offline")
    client = _Client(send_errors=[offline])
    task = _task(node_session_id="live-session")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.send_task_message(str(USER_ID), str(TASK_ID), "hello")

    assert str(exc.value) == "node_server_unavailable"
    # The handle is still valid; an offline node must not clear it.
    assert task.node_session_id == "live-session"
    assert client.dispatch_calls == 0


@pytest.mark.asyncio
async def test_concurrent_first_messages_dispatch_once(monkeypatch):
    client = _Client()
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    updates = {"count": 0}

    def update(**u):
        updates["count"] += 1
        return _return(1 if updates["count"] == 1 else 0)

    monkeypatch.setattr(task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=update))

    await asyncio.gather(
        service.send_task_message(str(USER_ID), str(TASK_ID), "m1"),
        service.send_task_message(str(USER_ID), str(TASK_ID), "m2"),
    )

    assert client.dispatch_calls == 1


# ── First-message title backfill ──────────────────────────────────────────────
#
# Tasks can be created without a name (empty interactive tasks wait for the
# first message). That first real conversation turn should supply the missing
# task title, while a manual title is never overwritten.


def _title_filter(title_state, calls):
    """Stub Task.filter recording title backfill attempts.

    ``title_state`` is a mutable {"title": value} so tests can flip it mid-run
    (e.g. the concurrent test simulating first-writer-wins). A backfill is
    recognized by its ``title=...`` update kwarg; other filter/update calls
    (stale-handle clearing) fall through to a generic ok stub.
    """

    def filter_(**kw):
        def do_update(**u):
            if "title" in u:
                # Only a still-unnamed row accepts the backfill.
                if title_state.get("title") is None or title_state.get("title") == "":
                    title_state["title"] = u["title"]
                    calls.append("backfill")
                    return _return(1)
                calls.append("rejected")
                return _return(0)
            return _return(1)

        return SimpleNamespace(update=do_update)

    return filter_


@pytest.mark.asyncio
async def test_first_message_backfills_empty_title(monkeypatch):
    """An unnamed task takes its title from the first delivered message."""
    client = _Client()
    task = _task(title=None, node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    title_state = {"title": None}
    calls: list[str] = []
    monkeypatch.setattr(task_service_module.Task, "filter", _title_filter(title_state, calls))

    await service.send_task_message(str(USER_ID), str(TASK_ID), "帮我修复登录超时的 bug")

    assert title_state["title"] == "帮我修复登录超时的 bug"
    assert task.title == "帮我修复登录超时的 bug"
    assert calls == ["backfill"]


@pytest.mark.asyncio
async def test_first_message_title_collapses_whitespace_and_caps_length(monkeypatch):
    """Line breaks fold to spaces and the title caps at the editor-session 40 chars."""
    client = _Client()
    task = _task(title=None, node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    title_state = {"title": None}
    monkeypatch.setattr(task_service_module.Task, "filter", _title_filter(title_state, []))

    long_text = "  第一行\n\n第二行   还有空格  " + "x" * 60
    await service.send_task_message(str(USER_ID), str(TASK_ID), long_text)

    expected = " ".join(long_text.split())[:40]
    assert title_state["title"] == expected
    assert len(title_state["title"]) == 40


@pytest.mark.asyncio
async def test_first_message_title_ignores_slash_commands(monkeypatch):
    """/compact is an operational command, not a conversation name."""
    client = _Client()
    task = _task(title=None, node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    calls: list[str] = []
    monkeypatch.setattr(task_service_module.Task, "filter", _title_filter({"title": None}, calls))

    await service.send_task_message(str(USER_ID), str(TASK_ID), "/compact")

    assert calls == []
    assert task.title is None


@pytest.mark.asyncio
async def test_existing_title_is_never_overwritten(monkeypatch):
    """A manual (or previously backfilled) title survives later messages."""
    client = _Client()
    task = _task(title="手动命名", node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    calls: list[str] = []
    monkeypatch.setattr(task_service_module.Task, "filter", _title_filter({"title": "手动命名"}, calls))

    await service.send_task_message(str(USER_ID), str(TASK_ID), "完全不同的第二条消息")

    assert calls == []
    assert task.title == "手动命名"


@pytest.mark.asyncio
async def test_failed_send_does_not_backfill_title(monkeypatch):
    """A message that never reaches the runtime must not name the task."""
    from monkeycode_compat.node_client.errors import Code, RPCError

    offline = RPCError(Code.UNAVAILABLE, "node offline")
    client = _Client(send_errors=[offline])
    task = _task(title=None, node_session_id="live-session")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    calls: list[str] = []
    monkeypatch.setattr(task_service_module.Task, "filter", _title_filter({"title": None}, calls))

    with pytest.raises(ValueError) as exc:
        await service.send_task_message(str(USER_ID), str(TASK_ID), "本该成为标题的消息")

    assert str(exc.value) == "node_server_unavailable"
    assert calls == []
    assert task.title is None


@pytest.mark.asyncio
async def test_concurrent_first_messages_title_first_writer_wins(monkeypatch):
    """Two simultaneous first messages: only one title lands, once."""
    client = _Client()
    task = _task(title=None, node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    title_state = {"title": None}
    calls: list[str] = []
    monkeypatch.setattr(task_service_module.Task, "filter", _title_filter(title_state, calls))

    await asyncio.gather(
        service.send_task_message(str(USER_ID), str(TASK_ID), "第一条候选标题"),
        service.send_task_message(str(USER_ID), str(TASK_ID), "第二条候选标题"),
    )

    assert calls.count("backfill") == 1
    assert title_state["title"] in ("第一条候选标题", "第二条候选标题")


@pytest.mark.asyncio
async def test_start_task_runtime_dispatches(monkeypatch):
    client = _Client()
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1)))

    result = await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert result["started"] is True
    assert result["node_session_id"] == "session-1"
    assert client.dispatch_calls == 1


@pytest.mark.asyncio
async def test_start_task_runtime_redispatches_stale_handle(monkeypatch):
    """A DB handle whose control-plane binding vanished must be re-dispatched.

    This is the core on-disk inconsistency: ``mc_tasks.node_session_id`` is set
    and ``status='processing'``, but ``mc_ac_node_sessions`` has no binding
    (node restart tore it down, or a prior build wrote the handle without the
    binding landing). Trusting the handle makes start return a phantom session
    that 404s on the first send. The helper must probe, clear, and re-dispatch.
    """
    client = _Client()
    # Existing handle probes as NOT_FOUND (binding gone) so the helper clears it
    # and re-dispatches; the stub's dispatch_session returns session-1.
    task = _task(node_session_id="stale-session")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1))
    )

    result = await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert result["started"] is True
    assert result["node_session_id"] == "session-1"
    assert client.dispatch_calls == 1
    assert task.node_session_id == "session-1"


@pytest.mark.asyncio
async def test_start_task_runtime_keeps_handle_when_node_offline(monkeypatch):
    """An offline node with a live binding must NOT clear the handle.

    The probe distinguishes binding-gone (NOT_FOUND → re-dispatch) from
    node-offline (UNAVAILABLE → 503, handle preserved). Clearing a valid handle
    on a transient offline would discard a session the user could resume.
    """
    from monkeycode_compat.node_client.errors import Code, RPCError

    client = _Client()
    # Override the probe to report the node as offline for the existing handle.
    original_probe = client.start_node_session_runtime

    async def probe(session_id):
        raise RPCError(Code.UNAVAILABLE, f"node for session {session_id} is offline")

    client.start_node_session_runtime = probe
    task = _task(node_session_id="live-session")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert str(exc.value) == "node_server_unavailable"
    # Handle preserved: an offline node is transient, not a stale binding.
    assert task.node_session_id == "live-session"
    assert client.dispatch_calls == 0


@pytest.mark.asyncio
async def test_start_task_runtime_node_server_unavailable(monkeypatch):
    from monkeycode_compat.node_client import NodeServerUnavailable

    client = _Client(raise_exc=NodeServerUnavailable("down"))
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.start_task_runtime(str(USER_ID), str(TASK_ID))
    assert str(exc.value) == "node_server_unavailable"
    assert client.dispatch_calls == 1
    assert task.workspace_state == "dispatch_failed"
    assert task.status == "pending"
    assert task.config_snapshot["dispatch_error"] == "down"


@pytest.mark.asyncio
async def test_rejected_dispatch_is_not_treated_as_success(monkeypatch):
    """A dispatch the node rejects must surface as a real failure.

    The node-server's unary JSON omits ``accepted`` when it is the proto default
    ``False`` (``MessageToDict`` drops default-valued fields). The client therefore
    sees ``accepted=None``, not ``False``. The dispatch helper must treat anything
    that is not an explicit ``True`` as a rejection so a failed create-session
    (e.g. git clone: destination path already exists after a restart) is recorded
    as ``dispatch_failed`` with the real error — not persisted as a phantom
    session id that 404s on the first send and collapses into a misleading 503.
    """
    # No "accepted" key: mirrors the wire shape produced by MessageToDict when the
    # node rejects the session (accepted=False is the proto default, omitted).
    client = _Client(
        dispatch_response={"sessionId": "session-1", "error": "git clone failed: destination path already exists"}
    )
    task = _install(monkeypatch, client)  # storage-state: no node_session_id
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.send_task_message(str(USER_ID), str(TASK_ID), "hello")

    assert str(exc.value) == "runtime_dispatch_failed"
    assert client.dispatch_calls == 1
    # The rejection is terminal + visible: no phantom handle, real error recorded.
    assert task.node_session_id is None
    assert task.workspace_state == "dispatch_failed"
    assert task.status == "error"
    assert "git clone failed" in task.config_snapshot["dispatch_error"]


@pytest.mark.asyncio
async def test_accepted_dispatch_missing_session_id_is_rejected(monkeypatch):
    """A dispatch that accepts but returns no session id is also a rejection.

    Guards against a malformed ack letting the helper persist an empty handle.
    """
    client = _Client(dispatch_response={"accepted": True})
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.send_task_message(str(USER_ID), str(TASK_ID), "hello")

    assert str(exc.value) == "runtime_dispatch_failed"
    assert task.workspace_state == "dispatch_failed"
    assert task.status == "error"



@pytest.mark.asyncio
async def test_dispatch_node_offline_is_retryable_and_shows_reason(monkeypatch):
    """A node that is offline must stay retryable AND expose why.

    ``dispatch_session`` reports an offline node as ``RPCError(UNAVAILABLE)``,
    which is a ``RuntimeError`` — not ``NodeServerUnavailable``. It used to fall
    into the generic handler, terminalizing the task as ``error`` while leaving
    ``workspace_state='pending'``, so the UI showed neither the offline reason
    nor a retry path and the recovery worker (pending-only) skipped it.
    """
    from monkeycode_compat.node_client.errors import Code, RPCError

    offline = RPCError(Code.UNAVAILABLE, "node node-1 is offline")
    client = _Client(raise_exc=offline)
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert str(exc.value) == "node_server_unavailable"
    # Retryable: pending keeps the recovery worker and the retry button alive.
    assert task.status == "pending"
    # dispatch_failed is what makes the frontend render dispatch_error at all.
    assert task.workspace_state == "dispatch_failed"
    assert "offline" in task.config_snapshot["dispatch_error"]


@pytest.mark.asyncio
async def test_dispatch_node_rejected_is_terminal_but_still_shows_reason(monkeypatch):
    """A permanent rejection terminalizes, yet the reason stays visible.

    Node deleted / not approved / cannot run the session will never succeed on
    retry, so the task goes ``error`` — but ``workspace_state`` must still be
    ``dispatch_failed`` so the detail page shows the cause instead of a bare
    "未启动".
    """
    from monkeycode_compat.node_client.errors import Code, RPCError

    rejected = RPCError(Code.FAILED_PRECONDITION, "node node-1 is not approved")
    client = _Client(raise_exc=rejected)
    task = _install(monkeypatch, client)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert str(exc.value) == "runtime_dispatch_failed"
    assert task.status == "error"
    assert task.workspace_state == "dispatch_failed"
    assert "not approved" in task.config_snapshot["dispatch_error"]


@pytest.mark.asyncio
async def test_start_task_runtime_requires_node(monkeypatch):
    client = _Client()
    task = _task()
    task.node_id = None
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError) as exc:
        await service.start_task_runtime(str(USER_ID), str(TASK_ID))
    assert str(exc.value) == "node_required"
    assert client.dispatch_calls == 0


@pytest.mark.asyncio
async def test_runtime_worker_recovers_storage_state_task(monkeypatch):
    from monkeycode_compat import task_runtime_worker as worker_module

    client = _Client()
    task = _install(monkeypatch, client)
    task.config_snapshot = {}
    monkeypatch.setattr(task_service_module.Task, "get_or_none", classmethod(lambda cls, **kw: _return(task)))
    monkeypatch.setattr(
        task_service_module.Task,
        "filter",
        lambda **kw: SimpleNamespace(
            order_by=lambda *a: _return([task]),
            update=lambda **u: _return(1),
            first=lambda: _return(None),
            # recover_pending now chains .exclude(...) to skip fresh
            # workspace_state='dispatching' rows owned by the deferred-create
            # background coroutine. Stub it as a no-op passthrough.
            exclude=lambda **_kw: SimpleNamespace(
                order_by=lambda *a: _return([task]),
            ),
        ),
    )
    monkeypatch.setattr(worker_module, "get_local_node_client", lambda: client)
    monkeypatch.setattr(
        task_service_module.ProjectTask,
        "filter",
        lambda **kw: SimpleNamespace(
            first=lambda: _return(_binding()),
            order_by=lambda *a: SimpleNamespace(first=lambda: _return(_binding())),
        ),
    )

    worked = await worker_module.task_runtime_worker.recover_pending()

    assert worked is True
    assert client.dispatch_calls == 1
    assert task.node_session_id == "session-1"
    # Re-dispatch brings the runtime up but pushes no turn, so the task is idle-
    # ready, not running. ``pending`` + a session handle is the "就绪·等待输入"
    # state; only a real human turn (send_task_message) promotes it to processing.
    assert task.status == "pending"


@pytest.mark.asyncio
async def test_runtime_worker_skips_when_node_client_disabled(monkeypatch):
    from monkeycode_compat import task_runtime_worker as worker_module

    client = _Client()
    client.enabled = False
    _install(monkeypatch, client)
    monkeypatch.setattr(worker_module, "get_local_node_client", lambda: client)
    scanned = {"count": 0}
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: (scanned.__setitem__("count", 1), _return([]))[1]
    )

    worked = await worker_module.task_runtime_worker.recover_pending()

    assert worked is False
    assert scanned["count"] == 0


@pytest.mark.asyncio
async def test_runtime_worker_schedules_retry_on_unavailable(monkeypatch):
    from monkeycode_compat.node_client import NodeServerUnavailable
    from monkeycode_compat import task_runtime_worker as worker_module

    client = _Client(raise_exc=NodeServerUnavailable("down"))
    task = _install(monkeypatch, client)
    task.config_snapshot = {}
    monkeypatch.setattr(task_service_module.Task, "get_or_none", classmethod(lambda cls, **kw: _return(task)))
    monkeypatch.setattr(
        task_service_module.Task,
        "filter",
        lambda **kw: SimpleNamespace(
            order_by=lambda *a: _return([task]),
            update=lambda **u: _return(1),
            first=lambda: _return(None),
            # recover_pending now chains .exclude(...) to skip fresh
            # workspace_state='dispatching' rows owned by the deferred-create
            # background coroutine. Stub it as a no-op passthrough.
            exclude=lambda **_kw: SimpleNamespace(
                order_by=lambda *a: _return([task]),
            ),
        ),
    )
    monkeypatch.setattr(worker_module, "get_local_node_client", lambda: client)

    worked = await worker_module.task_runtime_worker.recover_pending()

    assert worked is False
    assert client.dispatch_calls == 1
    # Failure is recorded on the task as a retryable dispatch_failed state.
    assert task.workspace_state == "dispatch_failed"
    assert task.config_snapshot.get("dispatch_error")


@pytest.mark.asyncio
async def test_runtime_worker_heals_processing_task_with_stale_handle(monkeypatch):
    """A processing task whose handle's binding is gone must self-heal.

    The Task row still says ``processing`` and holds the dead handle, but the
    control-plane binding vanished (node deleted cascade, or the binding row was
    cleared). The worker probes via the dispatch helper, sees ``NOT_FOUND``,
    clears the stale handle, and re-dispatches so the task recovers without the
    user clicking retry on the detail page.
    """
    from monkeycode_compat import task_runtime_worker as worker_module

    client = _Client()
    task = _task(status="processing", node_session_id="stale-session")
    _install(monkeypatch, client, task=task)
    task.config_snapshot = {}
    monkeypatch.setattr(task_service_module.Task, "get_or_none", classmethod(lambda cls, **kw: _return(task)))
    monkeypatch.setattr(
        task_service_module.Task,
        "filter",
        lambda **kw: SimpleNamespace(
            order_by=lambda *a: _return([task]),
            update=lambda **u: _return(1),
            first=lambda: _return(None),
            # recover_pending now chains .exclude(...) to skip fresh
            # workspace_state='dispatching' rows owned by the deferred-create
            # background coroutine. Stub it as a no-op passthrough.
            exclude=lambda **_kw: SimpleNamespace(
                order_by=lambda *a: _return([task]),
            ),
        ),
    )
    monkeypatch.setattr(worker_module, "get_local_node_client", lambda: client)
    monkeypatch.setattr(
        task_service_module.ProjectTask,
        "filter",
        lambda **kw: SimpleNamespace(
            first=lambda: _return(_binding()),
            order_by=lambda *a: SimpleNamespace(first=lambda: _return(_binding())),
        ),
    )

    worked = await worker_module.task_runtime_worker.recover_pending()

    assert worked is True
    # Probe cleared the stale handle, then dispatch produced a fresh session.
    assert client.dispatch_calls == 1
    assert task.node_session_id == "session-1"
    # Re-dispatch does not replay a turn, so the healed task returns to idle-ready
    # (pending + handle), not processing.
    assert task.status == "pending"


@pytest.mark.asyncio
async def test_runtime_worker_leaves_healthy_processing_task_alone(monkeypatch):
    """A processing task with a live binding must not be re-dispatched.

    The probe (start_node_session_runtime) acks ok for a live session, so the
    dispatch helper short-circuits and the worker does zero dispatch work.
    Re-dispatching a healthy task would create a duplicate session on the node.
    """
    from monkeycode_compat import task_runtime_worker as worker_module

    client = _Client()
    task = _task(status="processing", node_session_id="live-session")
    _install(monkeypatch, client, task=task)
    task.config_snapshot = {}
    monkeypatch.setattr(task_service_module.Task, "get_or_none", classmethod(lambda cls, **kw: _return(task)))
    monkeypatch.setattr(
        task_service_module.Task,
        "filter",
        lambda **kw: SimpleNamespace(
            order_by=lambda *a: _return([task]),
            update=lambda **u: _return(1),
            first=lambda: _return(None),
            # recover_pending now chains .exclude(...) to skip fresh
            # workspace_state='dispatching' rows owned by the deferred-create
            # background coroutine. Stub it as a no-op passthrough.
            exclude=lambda **_kw: SimpleNamespace(
                order_by=lambda *a: _return([task]),
            ),
        ),
    )
    monkeypatch.setattr(worker_module, "get_local_node_client", lambda: client)

    worked = await worker_module.task_runtime_worker.recover_pending()

    # Probe confirmed the live handle; no dispatch happened.
    assert client.dispatch_calls == 0
    assert task.node_session_id == "live-session"
    assert task.status == "processing"
    # worked is informational: the helper confirmed the runtime is placed.
    assert worked is True


@pytest.mark.asyncio
async def test_start_task_runtime_heals_not_ok_probe_ack(monkeypatch):
    """A live binding whose node lost the in-memory session must re-dispatch.

    ``start_node_session_runtime`` answers HTTP 200 with ``ok=False`` (no
    RPCError) when the node cannot place the session — e.g. the execution node
    restarted and its in-memory session map is gone while the control-plane
    binding row survived. The probe must read the ack body and treat the handle
    as stale, or start would return a phantom session that 404s on first send.
    """
    client = _Client()

    async def probe(session_id):
        return {"ok": False, "error": f"start session {session_id}: not found"}

    client.start_node_session_runtime = probe
    task = _task(node_session_id="live-session")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1))
    )

    result = await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert result["started"] is True
    assert result["node_session_id"] == "session-1"
    assert client.dispatch_calls == 1
    assert task.node_session_id == "session-1"


# --- server-side git proxy URL rewrite -------------------------------------

def _binding_with_repo(repo_url="https://gitea.example.com/o/r.git", git_identity_id=None, project_id=None):
    return SimpleNamespace(
        task_id=TASK_ID,
        model_id=uuid.UUID(int=0),
        cli_name="claude",
        project_id=project_id,
        issue_id=None,
        repo_url=repo_url,
        branch=None,
        git_identity_id=git_identity_id,
    )


@pytest.mark.asyncio
async def test_repo_url_rewritten_to_proxy_when_identity_present(monkeypatch):
    """A private-repo task with a git identity dispatches a proxy clone URL.

    The node must never receive the real credential: the proxy token is embedded
    in the URL userinfo and the username/password/token fields are stripped.
    """
    captured = {}

    async def fake_dispatch(node_id, session):
        captured["session"] = dict(session)
        return {"accepted": True, "sessionId": "session-1"}

    client = _Client()
    client.dispatch_session = fake_dispatch
    task = _install(
        monkeypatch,
        client,
        binding=_binding_with_repo(git_identity_id=uuid.uuid4()),
    )
    monkeypatch.setattr(task_service_module, "_git_proxy_origin", lambda: "http://127.0.0.1:8003")
    monkeypatch.setattr(task_service_module, "_git_proxy_control_token", lambda: "control-secret")
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1))
    )

    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    git = captured["session"]["git"]
    url = git["url"]
    assert url.startswith("http://") and "@127.0.0.1:8003/api/v1/public/nodes/git/" in url
    # The userinfo is the signed token, not a real PAT. The main repo is minted
    # rw so the agent can push its work back; scope is ``main``.
    userinfo = url.split("://", 1)[1].split("@", 1)[0]
    assert userinfo == task_service_module._git_proxy_token(str(TASK_ID), "main", "rw")
    assert userinfo.split(".")[1:3] == ["main", "rw"]
    # No real credential is shipped to the node.
    for secret in ("username", "password", "token"):
        assert secret not in git


@pytest.mark.asyncio
async def test_repo_url_left_untouched_without_identity(monkeypatch):
    """A public repo (no git identity) keeps direct-clone behavior."""
    captured = {}

    async def fake_dispatch(node_id, session):
        captured["session"] = dict(session)
        return {"accepted": True, "sessionId": "session-1"}

    client = _Client()
    client.dispatch_session = fake_dispatch
    task = _install(
        monkeypatch,
        client,
        binding=_binding_with_repo(repo_url="https://example.com/open/r.git", git_identity_id=None),
    )
    monkeypatch.setattr(task_service_module, "_git_proxy_origin", lambda: "http://127.0.0.1:8003")
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1))
    )

    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert captured["session"]["git"]["url"] == "https://example.com/open/r.git"


@pytest.mark.asyncio
async def test_repo_url_left_untouched_without_origin(monkeypatch):
    """No ``node_server_public_url`` → no proxy; direct clone, even with identity."""
    captured = {}

    async def fake_dispatch(node_id, session):
        captured["session"] = dict(session)
        return {"accepted": True, "sessionId": "session-1"}

    client = _Client()
    client.dispatch_session = fake_dispatch
    task = _install(
        monkeypatch,
        client,
        binding=_binding_with_repo(git_identity_id=uuid.uuid4()),
    )
    monkeypatch.setattr(task_service_module, "_git_proxy_origin", lambda: "")
    monkeypatch.setattr(
        task_service_module.Task, "filter", lambda **kw: SimpleNamespace(update=lambda **u: _return(1))
    )

    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert captured["session"]["git"]["url"] == "https://gitea.example.com/o/r.git"


# ── API Key snapshot refresh ─────────────────────────────────────────────────
#
# Auth resolves keys from Config's in-memory snapshot, never from the DB. A
# freshly minted task child key is therefore invisible to /v1/* until some write
# path reloads that snapshot. Before these calls existed the only thing that
# reloaded it was the 60s reconcile, so the node's first request raced it and
# 401'd — the task produced no output and no error. Ordering matters as much as
# the call itself: the refresh has to land before whoever will use the key.


def _install_key_refresh_spy(monkeypatch, events):
    async def refresh():
        events.append("refresh")

    monkeypatch.setattr(TaskService, "_refresh_api_key_snapshot", staticmethod(refresh))


@pytest.mark.asyncio
async def test_disable_task_key_refreshes_snapshot(monkeypatch):
    """A revoke that leaves the key in the snapshot is not a revoke."""
    import db as db_module

    events: list[str] = []
    _install_key_refresh_spy(monkeypatch, events)

    async def disable_task_api_key(_task_id, _user_id):
        events.append("db_disable")
        return {"task_id": str(TASK_ID), "key_id": 42, "disabled": True}

    monkeypatch.setattr(
        db_module.PostgresClient, "disable_task_api_key",
        classmethod(lambda cls, t, u: disable_task_api_key(t, u)),
    )

    task = _task()
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    await service.disable_task_api_key(str(USER_ID), str(TASK_ID))

    assert events == ["db_disable", "refresh"]


@pytest.mark.asyncio
async def test_rotate_task_key_refreshes_snapshot_before_node_config(monkeypatch):
    """The runtime must never be handed a key auth cannot yet resolve."""
    import db as db_module

    events: list[str] = []
    _install_key_refresh_spy(monkeypatch, events)

    async def rotate(_task_id, _user_id):
        events.append("db_rotate")
        return {
            "old_key_id": 42, "new_key_id": 43, "key": "sk-new",
            "key_masked": "sk-new...", "version": 2,
            "expires_at": None, "usage_limit": {},
        }

    async def disable_by_id(_key_id):
        events.append("db_disable_old")
        return True

    monkeypatch.setattr(
        db_module.PostgresClient, "rotate_task_api_key",
        classmethod(lambda cls, t, u: rotate(t, u)),
    )
    monkeypatch.setattr(
        db_module.PostgresClient, "disable_api_key_by_id",
        classmethod(lambda cls, k: disable_by_id(k)),
    )

    class _RotateClient(_Client):
        async def configure_node_session_llm(self, session_id, llm):
            events.append("node_config")
            return {"accepted": True}

    client = _RotateClient()
    task = _task(node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    await service.rotate_task_api_key(str(USER_ID), str(TASK_ID))

    # The new key enters the snapshot before the node is told to use it, and the
    # old key is only dropped after the runtime acked.
    assert events == ["db_rotate", "refresh", "node_config", "db_disable_old", "refresh"]


@pytest.mark.asyncio
async def test_rotate_task_key_refreshes_snapshot_after_rollback(monkeypatch):
    """A rolled-back rotation must not leave the disabled replacement usable."""
    import db as db_module

    events: list[str] = []
    _install_key_refresh_spy(monkeypatch, events)

    async def rotate(_task_id, _user_id):
        return {
            "old_key_id": 42, "new_key_id": 43, "key": "sk-new",
            "key_masked": "sk-new...", "version": 2,
            "expires_at": None, "usage_limit": {},
        }

    async def disable_by_id(_key_id):
        events.append("db_disable_new")
        return True

    async def restore(_task_id, _user_id, _key_id):
        events.append("db_restore_old")
        return True

    monkeypatch.setattr(
        db_module.PostgresClient, "rotate_task_api_key",
        classmethod(lambda cls, t, u: rotate(t, u)),
    )
    monkeypatch.setattr(
        db_module.PostgresClient, "disable_api_key_by_id",
        classmethod(lambda cls, k: disable_by_id(k)),
    )
    monkeypatch.setattr(
        db_module.PostgresClient, "restore_task_api_key_id",
        classmethod(lambda cls, t, u, k: restore(t, u, k)),
    )

    class _FailingClient(_Client):
        async def configure_node_session_llm(self, session_id, llm):
            raise RuntimeError("runtime gone")

    client = _FailingClient()
    task = _task(node_session_id="existing")
    _install(monkeypatch, client, task=task)
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    with pytest.raises(ValueError, match="runtime_not_bound"):
        await service.rotate_task_api_key(str(USER_ID), str(TASK_ID))

    assert events[-1] == "refresh"
    assert "db_disable_new" in events and "db_restore_old" in events


# ── Task key lifecycle: park on finished/error, resume on runtime restart ────
#
# The task child key (scope='task') is a credential handed to the runtime. A
# terminal task state (finished/error) parks it — disabled=true, binding kept
# — so /v1 auth refuses it; any resume path (re-dispatch, live-handle send,
# restart) flips it back BEFORE the runtime could issue a request with it.
# Park/resume are idempotent and skip the snapshot refresh when nothing
# flipped (PostgresClient.set_api_key_disabled_by_id returns the row-change).


def _install_key_toggle_spy(monkeypatch, events, *, flip_results=None):
    """Stub the DB toggle; record calls as 'db_park'/'db_resume'.

    ``flip_results`` is a queue of row-change results per call (default True):
    the helper refreshes the snapshot only when the DB actually flipped.
    """
    import db as db_module

    remaining = list(flip_results or [])

    async def toggle(key_id, disabled):
        events.append("db_park" if disabled else "db_resume")
        return remaining.pop(0) if remaining else True

    monkeypatch.setattr(
        db_module.PostgresClient, "set_api_key_disabled_by_id",
        classmethod(lambda cls, k, d: toggle(k, d)),
    )


@pytest.mark.asyncio
async def test_park_task_api_key_noop_without_binding(monkeypatch):
    """An explicitly revoked key clears api_key_id; park must not resurrect it."""
    events: list[str] = []
    _install_key_refresh_spy(monkeypatch, events)
    _install_key_toggle_spy(monkeypatch, events)

    task = _task()
    task.api_key_id = None
    service = TaskService()

    await service._park_task_api_key(task)
    await service._resume_task_api_key(task)

    assert events == []


@pytest.mark.asyncio
async def test_park_task_api_key_refreshes_snapshot_only_on_flip(monkeypatch):
    events: list[str] = []
    _install_key_refresh_spy(monkeypatch, events)
    _install_key_toggle_spy(monkeypatch, events, flip_results=[True, False])

    task = _task()
    service = TaskService()

    await service._park_task_api_key(task)    # flipped -> refresh
    await service._park_task_api_key(task)    # already parked -> no refresh

    assert events == ["db_park", "refresh", "db_park"]


@pytest.mark.asyncio
async def test_task_events_result_parks_key(monkeypatch):
    """A terminal result frame parks the key together with the principal."""
    import tortoise.connection as tortoise_connection

    events: list[str] = []

    async def disable(self, task):
        events.append("principal_disable")

    async def park(self, task):
        events.append("key_park")

    task = _task(status="processing", node_session_id="existing")

    class _EventsClient(_Client):
        async def follow_session_events(self, session_id):
            yield {"kind": "result", "success": True, "exit_code": 0}

    _install(monkeypatch, _EventsClient(), task=task)
    monkeypatch.setattr(TaskService, "_disable_task_mcp_principal", disable)
    monkeypatch.setattr(TaskService, "_park_task_api_key", park)
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )
    monkeypatch.setattr(
        tortoise_connection, "connections",
        SimpleNamespace(get=lambda _name: SimpleNamespace(execute_query=lambda *a, **kw: _return(None))),
    )
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(service, "_persist_task_event", lambda *a, **kw: _return(None))

    consumed = [event async for event in service.task_events(str(USER_ID), str(TASK_ID))]

    assert consumed[0]["kind"] == "result"
    assert events == ["principal_disable", "key_park"]


@pytest.mark.asyncio
async def test_task_events_error_event_parks_key(monkeypatch):
    events: list[str] = []

    async def disable(self, task):
        events.append("principal_disable")

    async def park(self, task):
        events.append("key_park")

    task = _task(status="processing", node_session_id="existing")

    class _EventsClient(_Client):
        async def follow_session_events(self, session_id):
            yield {"kind": "error", "message": "boom"}

    _install(monkeypatch, _EventsClient(), task=task)
    monkeypatch.setattr(TaskService, "_disable_task_mcp_principal", disable)
    monkeypatch.setattr(TaskService, "_park_task_api_key", park)
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))
    monkeypatch.setattr(service, "_persist_task_event", lambda *a, **kw: _return(None))

    [event async for event in service.task_events(str(USER_ID), str(TASK_ID))]

    assert events == ["principal_disable", "key_park"]


@pytest.mark.asyncio
async def test_mark_dispatch_failed_terminal_parks_key(monkeypatch):
    """A permanent dispatch rejection goes error: same key lifecycle closure."""
    events: list[str] = []
    _install_key_refresh_spy(monkeypatch, events)
    _install_key_toggle_spy(monkeypatch, events)

    task = _task()
    service = TaskService()

    await service._mark_dispatch_failed(task, "node rejected", terminal=True)
    assert events == ["db_park", "refresh"]

    # Retryable stays pending: the recovery worker re-dispatches soon and
    # would only have to resume the key it just parked.
    events.clear()
    await service._mark_dispatch_failed(task, "node offline", terminal=False)
    assert events == []


@pytest.mark.asyncio
async def test_dispatch_persisted_task_resumes_key_before_dispatch(monkeypatch):
    """The re-dispatch path (start / first message / recovery worker) must
    resume a parked key before the node session starts issuing /v1 requests."""
    events: list[str] = []
    client = _Client()
    task = _install(monkeypatch, client)
    _install_key_refresh_spy(monkeypatch, events)
    _install_key_toggle_spy(monkeypatch, events)
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    captured: dict = {}

    async def resume(self, t):
        events.append("resume")
        # The DB flip must precede dispatch: once the node accepts the session
        # it starts authenticating immediately.
        assert client.dispatch_calls == 0
        captured["task"] = t

    monkeypatch.setattr(TaskService, "_resume_task_api_key", resume)

    await service.start_task_runtime(str(USER_ID), str(TASK_ID))

    assert captured["task"].id == TASK_ID
    assert client.dispatch_calls == 1
    assert events == ["resume"]


@pytest.mark.asyncio
async def test_send_to_finished_task_resumes_key_before_delivery(monkeypatch):
    """Live-handle send on a finished task: the parked key is resumed before
    _deliver_turn re-reads the key row and pushes the LLM config."""
    events: list[str] = []
    client = _Client()
    task = _task(status="finished", node_session_id="existing")
    _install(monkeypatch, client, task=task)
    _install_key_refresh_spy(monkeypatch, events)
    _install_key_toggle_spy(monkeypatch, events)
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    async def resume(self, t):
        events.append("resume")
        # No send may leave before the key is live in the snapshot.
        assert client.sent == []

    monkeypatch.setattr(TaskService, "_resume_task_api_key", resume)

    await service.send_task_message(str(USER_ID), str(TASK_ID), "again")

    assert client.start_calls == ["existing"]
    assert client.dispatch_calls == 0
    assert events == ["resume"]
    assert client.sent[0][2] == task_service_module._with_git_credential_note("again")


@pytest.mark.asyncio
async def test_restart_task_runtime_resumes_key(monkeypatch):
    events: list[str] = []

    class _RestartClient(_Client):
        async def restart_node_session_runtime(self, session_id, fresh=True):
            events.append("node_restart")
            return {"accepted": True}

    client = _RestartClient()
    task = _task(node_session_id="existing")
    _install(monkeypatch, client, task=task)
    _install_key_refresh_spy(monkeypatch, events)
    _install_key_toggle_spy(monkeypatch, events)
    monkeypatch.setattr(
        task_service_module.Task, "filter",
        lambda **kw: SimpleNamespace(update=lambda **u: _return(1)),
    )
    service = TaskService()
    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: _return(task))

    async def resume(self, t):
        events.append("resume")
        assert "node_restart" not in events

    monkeypatch.setattr(TaskService, "_resume_task_api_key", resume)

    await service.restart_task_runtime(str(USER_ID), str(TASK_ID))

    assert events == ["resume", "node_restart"]
