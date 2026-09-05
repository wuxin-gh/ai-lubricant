"""Task DTO projection of the node-reported bring-up stage.

``_task_runtime_stage`` decides what the task page shows while a session is being
prepared, so its output is the contract between the node's stage frames and the
UI. These pin the parts the frontend must not have to re-derive: the display
label, the step counter, and ``preparing`` (which gates whether input is allowed).
"""
from __future__ import annotations

import pytest

from monkeycode_compat.task_service import RUNTIME_STAGE_READY, _task_runtime_stage


class _FakeTask:
    """Only the fields the projection reads. Not a Tortoise model: the function
    must work off plain attributes so it can be called on a partially-loaded row."""

    def __init__(self, stage="", detail=None, ok=True):
        self.runtime_stage = stage
        self.runtime_stage_detail = detail
        self.runtime_stage_ok = ok


def test_never_dispatched_task_has_no_stage():
    """A task that never reached a node has nothing to report — the UI must fall
    back to its own "not started" copy rather than render an empty step."""
    assert _task_runtime_stage(_FakeTask()) is None


def test_in_flight_stage_is_preparing_with_a_step_counter():
    stage = _task_runtime_stage(_FakeTask("git_clone", detail="拉取代码（分支 main）"))
    assert stage is not None
    # Label comes from the server so the frontend never maps stage keys itself.
    assert stage["label"] == "拉取代码"
    assert stage["preparing"] is True
    assert stage["ok"] is True
    assert stage["detail"] == "拉取代码（分支 main）"
    # 2nd of 5 preparation steps: the counter drives "第 2/5 步" in the UI.
    assert stage["index"] == 2
    assert stage["total"] == 5


def test_stages_are_numbered_in_execution_order():
    order = [
        _task_runtime_stage(_FakeTask(key))["index"]
        for key in ("workspace_prepare", "git_clone", "resource_sync", "runtime_preflight", "runtime_start")
    ]
    assert order == [1, 2, 3, 4, 5]


def test_ready_stage_is_not_preparing():
    """Reaching the ready stage means preparation finished. If this leaked through
    as ``preparing`` the composer would stay disabled forever on a healthy task."""
    stage = _task_runtime_stage(_FakeTask(RUNTIME_STAGE_READY))
    assert stage["preparing"] is False
    assert stage["ok"] is True


def test_failed_stage_is_not_preparing_and_keeps_its_reason():
    """A failed step is stuck, not in progress: the page must offer a retry, not
    a spinner. The reason travels in ``detail`` (the node redacts it first)."""
    stage = _task_runtime_stage(
        _FakeTask("runtime_preflight", detail="node agent runtime is not usable", ok=False)
    )
    assert stage["preparing"] is False
    assert stage["ok"] is False
    assert stage["label"] == "检查运行环境"
    assert "not usable" in stage["detail"]


def test_unknown_stage_key_still_renders():
    """A stage added to the proto before this map is updated must stay visible —
    silently dropping it would reintroduce the "no progress shown" gap."""
    stage = _task_runtime_stage(_FakeTask("some_future_step"))
    assert stage["label"] == "some_future_step"
    assert stage["preparing"] is True
    # Unknown key has no position in the sequence; 0 tells the UI to omit "n/N"
    # rather than print a wrong step number.
    assert stage["index"] == 0


def test_blank_detail_is_null_not_empty_string():
    """The frontend uses `detail || fallback`; an empty string would defeat that
    and render a blank line where the fallback copy belongs."""
    stage = _task_runtime_stage(_FakeTask("workspace_prepare", detail=""))
    assert stage["detail"] is None


@pytest.mark.parametrize("stage_value", ["", "   "])
def test_blank_stage_is_treated_as_absent(stage_value):
    assert _task_runtime_stage(_FakeTask(stage_value)) is None
