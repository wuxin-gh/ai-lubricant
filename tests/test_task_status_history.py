"""Gateway status-history writer tests."""
from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from user_platform import task_service as task_service_module
from user_platform.task_service import TaskService


@pytest.mark.asyncio
async def test_record_status_transition_writes_gateway_attribution(monkeypatch):
    calls = []

    class History:
        @classmethod
        async def create(cls, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(**kwargs)

    monkeypatch.setattr(task_service_module, "TaskStatusHistory", History)
    task_id = uuid.uuid4()
    await TaskService()._record_status_transition(
        task_id,
        "delivery_status",
        "received",
        frm="dispatching",
        reason="runtime ACK",
        message_id="msg-1",
        delivery_attempt=2,
    )

    assert calls == [{
        "id": calls[0]["id"],
        "task_id": task_id,
        "field": "delivery_status",
        "from_val": "dispatching",
        "to_val": "received",
        "reason": "runtime ACK",
        "source": "gateway",
        "message_id": "msg-1",
        "delivery_attempt": 2,
    }]
    assert isinstance(calls[0]["id"], uuid.UUID)


@pytest.mark.asyncio
async def test_record_status_transition_converts_string_task_id(monkeypatch):
    calls = []

    class History:
        @classmethod
        async def create(cls, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(task_service_module, "TaskStatusHistory", History)
    task_id = uuid.uuid4()
    await TaskService()._record_status_transition(str(task_id), "status", "finished")
    assert calls[0]["task_id"] == task_id


@pytest.mark.asyncio
async def test_record_status_transition_truncates_bounded_columns(monkeypatch):
    calls = []

    class History:
        @classmethod
        async def create(cls, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(task_service_module, "TaskStatusHistory", History)
    await TaskService()._record_status_transition(
        str(uuid.uuid4()), "f" * 80, "t" * 80, frm="p" * 80,
    )
    row = calls[0]
    assert len(row["field"]) == 32
    assert len(row["from_val"]) == 64
    assert len(row["to_val"]) == 64


@pytest.mark.asyncio
async def test_record_status_transition_swallows_history_failure(monkeypatch):
    class History:
        @classmethod
        async def create(cls, **kwargs):
            raise RuntimeError("history table unavailable")

    monkeypatch.setattr(task_service_module, "TaskStatusHistory", History)
    # A diagnostic write cannot turn the state transition caller into an error.
    await TaskService()._record_status_transition(
        str(uuid.uuid4()), "status", "error", reason="dispatch failed",
    )
