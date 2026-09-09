"""code_run 现在必须经过一次性审批，且默认 fail closed。"""
import asyncio

import pytest

from agent.code_run_approval import CodeRunApprovalBatch
from agent.config import AgentConfig
from agent.tools import ToolContext, ToolRegistry
from user_platform.node_client.approvals import ApprovalDenied, ApprovalTimeout, approval_registry


def _registry(tmp_path) -> ToolRegistry:
    workspace = tmp_path / "workspace"
    temp = tmp_path / "temp"
    workspace.mkdir()
    temp.mkdir()
    config = AgentConfig(
        workspace_root=str(workspace),
        allowed_roots=[str(workspace), str(temp)],
    )
    return ToolRegistry(ToolContext(config), agent_id=7)


def _call(code: str = "print(1)") -> list[dict]:
    return [{"function": {"name": "code_run", "arguments": {"code": code, "type": "python"}}}]


@pytest.mark.asyncio
async def test_code_run_without_approval_coordinator_is_denied(tmp_path):
    registry = _registry(tmp_path)

    result = await registry.execute("code_run", {"code": "print('x')", "type": "python"})

    assert result["status"] == "denied"
    assert result["code"] == "approval_required"


@pytest.mark.asyncio
async def test_code_run_denied_when_no_event_sink(tmp_path):
    registry = _registry(tmp_path)
    batch = CodeRunApprovalBatch(conversation_id="c1", agent_id=7, caller="u1", emit=None)
    registry.set_code_run_approval(batch)

    await batch.prepare(_call())
    result = await registry.execute("code_run", {"code": "print(1)", "type": "python"})

    assert result["status"] == "denied"


@pytest.mark.asyncio
async def test_code_run_runs_once_after_allow(tmp_path):
    registry = _registry(tmp_path)
    events: list[dict] = []

    async def emit(event: dict) -> None:
        events.append(event)
        if event.get("type") == "confirmation_required":
            approval_registry.resolve(event["confirmation_id"], "allow")

    batch = CodeRunApprovalBatch(conversation_id="c1", agent_id=7, caller="u1", emit=emit)
    registry.set_code_run_approval(batch)

    await batch.prepare(_call("print('approved')"))
    first = await registry.execute("code_run", {"code": "print('approved')", "type": "python"})
    second = await registry.execute("code_run", {"code": "print('approved')", "type": "python"})

    assert events and events[0]["type"] == "confirmation_required"
    assert events[0]["metadata"]["agent_id"] == 7
    assert first["status"] == "ok"
    # A single approval authorizes a single execution.
    assert second["status"] == "denied"


@pytest.mark.asyncio
async def test_code_run_denied_when_code_changes_after_approval(tmp_path):
    registry = _registry(tmp_path)

    async def emit(event: dict) -> None:
        if event.get("type") == "confirmation_required":
            approval_registry.resolve(event["confirmation_id"], "allow")

    batch = CodeRunApprovalBatch(conversation_id="c1", agent_id=7, caller="u1", emit=emit)
    registry.set_code_run_approval(batch)

    await batch.prepare(_call("print('original')"))
    result = await registry.execute("code_run", {"code": "print('tampered')", "type": "python"})

    assert result["status"] == "denied"


@pytest.mark.asyncio
async def test_code_run_denied_when_user_rejects(tmp_path):
    registry = _registry(tmp_path)

    async def emit(event: dict) -> None:
        if event.get("type") == "confirmation_required":
            approval_registry.resolve(event["confirmation_id"], "deny")

    batch = CodeRunApprovalBatch(conversation_id="c1", agent_id=7, caller="u1", emit=emit)
    registry.set_code_run_approval(batch)

    with pytest.raises(ApprovalDenied):
        await batch.prepare(_call())


@pytest.mark.asyncio
async def test_code_run_timeout_aborts_turn_instead_of_denying(tmp_path):
    """没人裁决时 prepare 抛 ApprovalTimeout，而不是降级成 deny 让循环继续。

    早先超时返回的是普通「已拒绝」工具结果，agent_loop 照常把它喂回模型跑下一轮，
    表现就是「审批过期了却还在调模型」。现在这一轮直接中止，等人回来重发。
    """
    registry = _registry(tmp_path)
    events: list[dict] = []

    async def emit(event: dict) -> None:
        events.append(event)  # 只收事件，不裁决 —— 模拟人不在

    # timeout_seconds 极小，避免测试真的等下去。
    batch = CodeRunApprovalBatch(
        conversation_id="c1", agent_id=7, caller="u1", emit=emit, timeout_seconds=1,
    )
    registry.set_code_run_approval(batch)

    with pytest.raises(ApprovalTimeout):
        await asyncio.wait_for(batch.prepare(_call()), timeout=5)

    assert events and events[0]["type"] == "confirmation_required"
    action = approval_registry.get(events[0]["confirmation_id"])
    assert action is not None and action.resolved == "timeout"


@pytest.mark.asyncio
async def test_code_run_zero_timeout_never_expires(tmp_path):
    """approval_timeout_seconds=0 表示不过期：注册表不给到期时刻，reap 也不碰它。"""
    action = approval_registry.create(
        conversation_id="c-never",
        node_id="",
        tool_name="code_run",
        command="print(1)",
        requester="u1",
        timeout_seconds=0,
    )
    try:
        assert action.expires_at == 0
        assert action.is_expired() is False
        assert action not in approval_registry.reap_expired()
        assert action.resolved is None
    finally:
        approval_registry.remove(action.confirmation_id)
