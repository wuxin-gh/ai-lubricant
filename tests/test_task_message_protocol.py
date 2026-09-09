"""User-message delivery protocol unit tests.

Covers the idempotent send state machine in TaskService:
- claim/reserve (pending → dispatching) is atomic and returns the attempt
- failed/cancelled re-sends bump delivery_attempt (new dedupe key)
- stale dispatching claims are reclaimable, fresh ones are not
- status transitions are one-way (replayed ACKs cannot regress state)
- duplicate (task_id, client_message_id) rows collapse onto the existing row
- switch_task_model always moves the selected model to models_snapshot[0]
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from tortoise.exceptions import IntegrityError

from user_platform import task_service as task_service_module
from user_platform.task_service import TaskService

TASK_ID = uuid.uuid4()


class _FilterRecorder:
    """Stand-in for a Tortoise QuerySet: records filter kwargs and updates.

    ``values_list`` is the pre-image read the status-history trail does before
    each conditional update; it is a separate query on the same stub so the
    recorder must answer it without counting it as a mutation.
    """

    def __init__(self, update_result: int = 1, pre_image: str = "dispatching"):
        self.update_result = update_result
        self.pre_image = pre_image
        self.calls: list[tuple[dict, dict]] = []

    def make(self):
        recorder = self

        class _Query:
            def __init__(self, kwargs: dict):
                self._kwargs = kwargs

            async def update(self, **payload: object) -> int:
                recorder.calls.append((self._kwargs, payload))
                return recorder.update_result

            async def values_list(self, *_fields: str, flat: bool = False):
                return [recorder.pre_image]

        def _filter(**kwargs: object):
            return _Query(kwargs)

        return _filter


def _row(status: str = "pending", attempt: int = 1, started_at=None, cid="msg-1"):
    return SimpleNamespace(
        id=uuid.uuid4(),
        task_id=TASK_ID,
        client_message_id=cid,
        delivery_status=status,
        delivery_attempt=attempt,
        started_at=started_at,
    )


@pytest.mark.asyncio
async def test_claim_pending_reserves_dispatching(monkeypatch):
    service = TaskService()
    row = _row("pending", attempt=1)
    recorder = _FilterRecorder(update_result=1)
    monkeypatch.setattr(task_service_module.TaskEvent, "filter", recorder.make())

    status, attempt = await service._claim_message_for_dispatch(None, row)

    assert (status, attempt) == ("pending", 1)
    # The claim update must be conditional on pending (atomic reserve).
    filter_kwargs, payload = recorder.calls[-1]
    assert filter_kwargs == {"id": row.id, "delivery_status": "pending"}
    assert payload["delivery_status"] == "dispatching"
    assert "started_at" in payload


@pytest.mark.asyncio
async def test_claim_failed_retry_bumps_attempt(monkeypatch):
    service = TaskService()
    row = _row("failed", attempt=1)
    recorder = _FilterRecorder(update_result=1)
    monkeypatch.setattr(task_service_module.TaskEvent, "filter", recorder.make())

    status, attempt = await service._claim_message_for_dispatch(None, row)

    assert (status, attempt) == ("pending", 2)
    requeue_kwargs, requeue_payload = recorder.calls[0]
    assert requeue_kwargs == {"id": row.id, "delivery_status__in": ("failed", "cancelled")}
    assert requeue_payload["delivery_attempt"] == 2
    assert requeue_payload["failure_reason"] is None


@pytest.mark.asyncio
async def test_claim_lost_retry_race_claims_fresh_pending(monkeypatch):
    """Cross-process failed→pending race: seeing pending is not ownership.

    Process A already requeued the failed row; process B's requeue update loses
    (0 rows), then reads pending. B must still perform the conditional
    pending→dispatching claim before returning "deliver" — otherwise both
    processes could send the same attempt.
    """
    service = TaskService()
    row = _row("failed", attempt=1)
    fresh_pending = _row("pending", attempt=2)
    calls: list[tuple[dict, dict]] = []
    update_results = iter([0, 1])  # lost requeue, then won pending claim

    class Query:
        def __init__(self, kwargs):
            self.kwargs = kwargs

        async def update(self, **payload):
            calls.append((self.kwargs, payload))
            return next(update_results)

    monkeypatch.setattr(task_service_module.TaskEvent, "filter", lambda **kw: Query(kw))
    monkeypatch.setattr(
        task_service_module.TaskEvent,
        "get_or_none",
        classmethod(lambda cls, **kw: _async_return(fresh_pending)),
    )

    status, attempt = await service._claim_message_for_dispatch(None, row)

    assert (status, attempt) == ("pending", 2)
    assert calls[-1][0] == {"id": row.id, "delivery_status": "pending"}
    assert calls[-1][1]["delivery_status"] == "dispatching"


@pytest.mark.asyncio
async def test_claim_stale_dispatching_is_reclaimable(monkeypatch):
    service = TaskService()
    stale_started = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=TaskService._DISPATCHING_STALE_SECONDS * 2)
    row = _row("dispatching", attempt=1, started_at=stale_started)
    fresh_row = _row("dispatching", attempt=1, started_at=stale_started)
    recorder = _FilterRecorder(update_result=1)
    monkeypatch.setattr(task_service_module.TaskEvent, "filter", recorder.make())

    async def get_or_none(**_kw):
        return fresh_row

    monkeypatch.setattr(
        task_service_module.TaskEvent, "get_or_none", classmethod(lambda cls, **kw: get_or_none(**kw))
    )

    status, attempt = await service._claim_message_for_dispatch(None, row)
    assert (status, attempt) == ("pending", 1)
    # Reclaim must be conditional on the stale timestamp so concurrent callers
    # cannot both win.
    filter_kwargs, _payload = recorder.calls[-1]
    assert filter_kwargs["delivery_status"] == "dispatching"
    assert "started_at__lt" in filter_kwargs


@pytest.mark.asyncio
async def test_claim_fresh_dispatching_is_not_reclaimable(monkeypatch):
    service = TaskService()
    fresh_started = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=1)
    row = _row("dispatching", attempt=1, started_at=fresh_started)
    monkeypatch.setattr(
        task_service_module.TaskEvent,
        "filter",
        _FilterRecorder(update_result=0).make(),
    )
    fresh_row = _row("dispatching", attempt=1, started_at=fresh_started)
    monkeypatch.setattr(
        task_service_module.TaskEvent,
        "get_or_none",
        classmethod(lambda cls, **kw: _async_return(fresh_row)),
    )

    status, attempt = await service._claim_message_for_dispatch(None, row)
    assert status == "dispatching"
    assert attempt == 1


async def _async_return(value):
    return value


@pytest.mark.asyncio
async def test_transition_is_one_way(monkeypatch):
    service = TaskService()
    recorder = _FilterRecorder()
    monkeypatch.setattr(task_service_module.TaskEvent, "filter", recorder.make())

    moved = await service._transition_message_status(TASK_ID, "msg-1", "received")

    assert moved is True
    filter_kwargs, payload = recorder.calls[0]
    # received may only be entered from dispatching (pending claims first).
    assert set(filter_kwargs["delivery_status__in"]) == {"dispatching"}
    assert filter_kwargs["client_message_id"] == "msg-1"
    assert payload["delivery_status"] == "received"
    assert "received_at" in payload


@pytest.mark.asyncio
async def test_transition_rejects_unknown_target(monkeypatch):
    service = TaskService()
    recorder = _FilterRecorder()
    monkeypatch.setattr(task_service_module.TaskEvent, "filter", recorder.make())

    # completed is terminal: nothing may transition into it from completed itself.
    moved = await service._transition_message_status(TASK_ID, "msg-1", "pending")
    assert moved is True  # failed/cancelled → pending is the retry path
    # A target with no inbound edges (none here) returns False without touching DB.
    moved = await service._transition_message_status(TASK_ID, "msg-1", "bogus")
    assert moved is False
    assert len(recorder.calls) == 1


@pytest.mark.asyncio
async def test_persist_duplicate_id_returns_existing_row(monkeypatch):
    service = TaskService()
    existing = _row("received", attempt=1)

    async def next_event_seq(_task_id):
        return 1

    monkeypatch.setattr(service, "_next_event_seq", next_event_seq)

    async def create(**_kw):
        raise IntegrityError()

    async def get_or_none(**_kw):
        return existing

    async def first(**_kw):
        return None

    monkeypatch.setattr(task_service_module.TaskEvent, "create", classmethod(lambda cls, **kw: create(**kw)))
    monkeypatch.setattr(
        task_service_module.TaskEvent, "get_or_none", classmethod(lambda cls, **kw: get_or_none(**kw))
    )
    monkeypatch.setattr(task_service_module.TaskEvent, "filter", lambda **kw: SimpleNamespace(order_by=lambda *a: SimpleNamespace(first=lambda: first())))

    row = await service._persist_user_input_item(
        TASK_ID, "hello", client_message_id="msg-1", delivery_status="pending",
    )
    assert row is existing


@pytest.mark.asyncio
async def test_switch_task_model_moves_selection_to_head(monkeypatch):
    service = TaskService()
    task = SimpleNamespace(
        id=TASK_ID,
        api_key_id=None,
        parent_api_key_id=None,
        models_snapshot=["model-a", "model-b", "model-c"],
        save=lambda **_kw: _async_return(None),
    )

    async def owned(user_id, task_id, role=None):
        return task

    async def validate(key, models, *, active_model=None):
        # Identity: every model is legal for the key.
        return models, active_model

    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: owned(*a, **kw))
    monkeypatch.setattr(task_service_module, "_validate_models_for_key", validate)

    result = await service.switch_task_model("user", str(TASK_ID), "model-c")

    assert result["model_id"] == "model-c"
    assert result["models"] == ["model-c", "model-a", "model-b"]
    assert task.models_snapshot[0] == "model-c"


@pytest.mark.asyncio
async def test_switch_task_model_adds_target_outside_snapshot(monkeypatch):
    """集合外目标：只要父 Key 目录里合法就直接切到头部——不要求先经「添加模型」
    进集合，旧成员保序跟在后面，白名单同步为并集。下拉列表口径是 Key 目录，
    切换永不收窄列表，故这里绝不能 reject。"""
    service = TaskService()
    whitelist_updates: list[tuple[int, list[str]]] = []
    task = SimpleNamespace(
        id=TASK_ID,
        api_key_id=7,
        parent_api_key_id=1,
        models_snapshot=["model-a"],
        save=lambda **_kw: _async_return(None),
    )

    async def owned(user_id, task_id, role=None):
        return task

    async def validate(key, models, *, active_model=None):
        # 校验永远对父 Key（目录归属），且只带目标本身，不带快照成员。
        assert key == 1
        assert models == ["model-x"]
        return models, active_model

    async def update_whitelist(key_id, whitelist):
        whitelist_updates.append((key_id, list(whitelist)))
        return True

    import db as db_module

    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: owned(*a, **kw))
    monkeypatch.setattr(task_service_module, "_validate_models_for_key", validate)
    monkeypatch.setattr(
        db_module.PostgresClient, "update_task_key_model_whitelist",
        classmethod(lambda cls, key_id, whitelist: update_whitelist(key_id, whitelist)),
    )
    refreshed = {"count": 0}

    async def refresh():
        refreshed["count"] += 1

    monkeypatch.setattr(TaskService, "_refresh_api_key_snapshot", staticmethod(refresh))

    result = await service.switch_task_model("user", str(TASK_ID), "model-x")

    assert result["model_id"] == "model-x"
    assert result["models"] == ["model-x", "model-a"]
    assert task.models_snapshot == ["model-x", "model-a"]
    assert whitelist_updates == [(7, ["model-x", "model-a"])]
    assert refreshed["count"] == 1


@pytest.mark.asyncio
async def test_switch_task_model_rejects_key_filtered_target(monkeypatch):
    """目标在集合内但被 Key/供给过滤：也必须明确失败而非回退。"""
    service = TaskService()
    task = SimpleNamespace(
        id=TASK_ID,
        api_key_id=7,
        parent_api_key_id=None,
        models_snapshot=["model-a", "model-b"],
        save=lambda **_kw: _async_return(None),
    )

    async def owned(user_id, task_id, role=None):
        return task

    async def validate(key, models, *, active_model=None):
        # 新约定只传目标本身。model-b 被过滤掉时 _validate_models_for_key 会回退
        # resolved 到剩余合法模型——服务层必须拦截这个回退，而不是静默切过去。
        assert models == ["model-b"]
        return ["model-a"], "model-a"

    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: owned(*a, **kw))
    monkeypatch.setattr(task_service_module, "_validate_models_for_key", validate)

    with pytest.raises(ValueError, match="model_not_available_for_key"):
        await service.switch_task_model("user", str(TASK_ID), "model-b")
    # 被拒的切换不得动快照。
    assert task.models_snapshot == ["model-a", "model-b"]


@pytest.mark.asyncio
async def test_switch_task_model_first_selection_on_empty_snapshot(monkeypatch):
    """快照为空（不限制）：首次切换建立集合并同步子 Key 白名单；连续切换
    只换头部，历史成员保留——列表（Key 目录）永不塌缩成最后一次的选择。"""
    service = TaskService()
    whitelist_updates: list[tuple[int, list[str]]] = []
    task = SimpleNamespace(
        id=TASK_ID,
        api_key_id=7,
        parent_api_key_id=1,
        models_snapshot=None,
        save=lambda **_kw: _async_return(None),
    )

    async def owned(user_id, task_id, role=None):
        return task

    async def validate(key, models, *, active_model=None):
        # 校验永远对父 Key（目录归属），且只带目标本身。
        assert key == 1
        assert models == [active_model]
        return models, active_model

    async def update_whitelist(key_id, whitelist):
        whitelist_updates.append((key_id, list(whitelist)))
        return True

    import db as db_module

    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: owned(*a, **kw))
    monkeypatch.setattr(task_service_module, "_validate_models_for_key", validate)
    monkeypatch.setattr(
        db_module.PostgresClient, "update_task_key_model_whitelist",
        classmethod(lambda cls, key_id, whitelist: update_whitelist(key_id, whitelist)),
    )
    refreshed = {"count": 0}

    async def refresh():
        refreshed["count"] += 1

    monkeypatch.setattr(TaskService, "_refresh_api_key_snapshot", staticmethod(refresh))

    result = await service.switch_task_model("user", str(TASK_ID), "model-x")

    assert result["models"] == ["model-x"]
    assert task.models_snapshot == ["model-x"]
    assert whitelist_updates == [(7, ["model-x"])]
    assert refreshed["count"] == 1

    # 连续切换：新目标进头部，旧成员保序跟在后面，白名单同步为整个快照。
    result = await service.switch_task_model("user", str(TASK_ID), "model-y")
    assert result["model_id"] == "model-y"
    assert result["models"] == ["model-y", "model-x"]

    result = await service.switch_task_model("user", str(TASK_ID), "model-x")
    assert result["model_id"] == "model-x"
    assert task.models_snapshot == ["model-x", "model-y"]
    assert whitelist_updates == [
        (7, ["model-x"]),
        (7, ["model-y", "model-x"]),
        (7, ["model-x", "model-y"]),
    ]
    assert refreshed["count"] == 3


@pytest.mark.asyncio
async def test_add_task_models_appends_without_moving_active(monkeypatch):
    """添加模型：合法者并入集合（追加，不动头部活跃模型），白名单同步为并集。"""
    service = TaskService()
    whitelist_updates: list[tuple[int, list[str]]] = []
    task = SimpleNamespace(
        id=TASK_ID,
        api_key_id=7,
        parent_api_key_id=1,
        models_snapshot=["model-a"],
        save=lambda **_kw: _async_return(None),
    )

    async def owned(user_id, task_id, role=None):
        return task

    async def validate(key, models, *, active_model=None):
        # 按父 Key 全部合法。
        return models, None

    async def update_whitelist(key_id, whitelist):
        whitelist_updates.append((key_id, list(whitelist)))
        return True

    import db as db_module

    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: owned(*a, **kw))
    monkeypatch.setattr(task_service_module, "_validate_models_for_key", validate)
    monkeypatch.setattr(
        db_module.PostgresClient, "update_task_key_model_whitelist",
        classmethod(lambda cls, key_id, whitelist: update_whitelist(key_id, whitelist)),
    )
    monkeypatch.setattr(
        TaskService, "_refresh_api_key_snapshot", staticmethod(lambda: _async_return(None))
    )

    result = await service.add_task_models("user", str(TASK_ID), ["model-b", "model-c", "model-a"])

    assert result["models"] == ["model-a", "model-b", "model-c"]
    assert task.models_snapshot[0] == "model-a"
    assert whitelist_updates == [(7, ["model-a", "model-b", "model-c"])]


@pytest.mark.asyncio
async def test_add_task_models_first_add_on_empty_snapshot(monkeypatch):
    """快照为空（不限制）时首次添加建立初始集合，首个模型成为活跃模型。"""
    service = TaskService()
    task = SimpleNamespace(
        id=TASK_ID,
        api_key_id=7,
        parent_api_key_id=1,
        models_snapshot=None,
        save=lambda **_kw: _async_return(None),
    )

    async def owned(user_id, task_id, role=None):
        return task

    async def validate(key, models, *, active_model=None):
        return models, None

    import db as db_module

    monkeypatch.setattr(service, "_owned_task_or_raise", lambda *a, **kw: owned(*a, **kw))
    monkeypatch.setattr(task_service_module, "_validate_models_for_key", validate)
    monkeypatch.setattr(
        db_module.PostgresClient, "update_task_key_model_whitelist",
        classmethod(lambda cls, key_id, whitelist: _async_return(True)),
    )
    monkeypatch.setattr(
        TaskService, "_refresh_api_key_snapshot", staticmethod(lambda: _async_return(None))
    )

    result = await service.add_task_models("user", str(TASK_ID), ["model-b", "model-a"])

    assert result["models"] == ["model-b", "model-a"]
    assert task.models_snapshot[0] == "model-b"


def test_backfill_canonical_envelope_derives_subagent_id_for_old_rows():
    """旧行只有 agent_id 别名：读取边界一次性派生 subagent_id 并标记来源。"""
    old_payload = {"item": {"id": "t1", "type": "tool_call"}, "agent_id": "call_child"}
    projected = TaskService._backfill_canonical_envelope(old_payload)

    assert projected["subagent_id"] == "call_child"
    assert projected["compat_derived"] is True
    assert projected["agent_id"] == "call_child"  # 原字段保留


def test_backfill_canonical_envelope_passes_new_rows_through():
    """新行已带 canonical 字段：原样返回，不重复派生。"""
    payload = {
        "item": {"id": "t2", "type": "tool_call"},
        "agent_id": "",
        "subagent_id": "",
        "logical_event_id": "t2",
    }
    projected = TaskService._backfill_canonical_envelope(payload)

    assert projected is payload


def test_backfill_canonical_envelope_ignores_non_item_rows():
    """非 item 行（传输态条件、usage 行）原样返回。"""
    payload = {"kind": "error", "message": "节点控制面暂不可用"}
    assert TaskService._backfill_canonical_envelope(payload) is payload
    assert TaskService._backfill_canonical_envelope(None) is None
    assert TaskService._backfill_canonical_envelope("text") == "text"
