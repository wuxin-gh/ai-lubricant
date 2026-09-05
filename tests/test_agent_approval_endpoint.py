"""审批裁决端点的 HTTP 层行为。

原有审批测试都在进程内直接调 approval_registry.resolve，绕过了这个端点，
因此漏掉了「code_run 审批永远 403」——code_run 的 action 没有节点
（node_id=""），而端点先判空 node_id 再放行管理员，于是每次点「允许」都被
当成越权。这里补上端点级覆盖：绑节点的审批仍要过节点门，不绑节点的不过。
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import agent.api as agent_api
from agent import conversation_store as real_store
from monkeycode_compat.node_client.approvals import ApprovalRegistry


class _Store:
    conversations: dict[str, dict] = {}

    @classmethod
    async def get_conversation_owned(cls, caller, conv_id):
        conv = cls.conversations.get(conv_id)
        if not conv:
            return None
        if caller is None or str(conv.get("user_id")) == str(caller):
            return conv
        return None


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(agent_api.router)
    app.dependency_overrides[agent_api.get_agent_caller] = lambda: None
    return TestClient(app)


@pytest.fixture
def env(monkeypatch):
    """就绪的会话存储 + 一个干净的审批注册表（不污染模块单例）。

    ApprovalRegistry.create 要在当前线程拿 event loop 建 Future。pytest-asyncio 的前置
    测试会关闭它自己的 loop；这个文件是同步 TestClient 测试，因此每例显式建立一条，
    保证单独跑和与 async 审批测试合跑口径一致。
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    monkeypatch.setattr(real_store, "is_ready", lambda: True)
    monkeypatch.setattr(agent_api, "_require_ch", lambda: None)
    monkeypatch.setattr(real_store, "get_conversation_owned", _Store.get_conversation_owned)
    _Store.conversations = {"conv-1": {"id": "conv-1", "user_id": "u1"}}

    registry = ApprovalRegistry()
    import monkeycode_compat.node_client.approvals as approvals_mod

    monkeypatch.setattr(approvals_mod, "approval_registry", registry)
    yield registry
    loop.close()
    asyncio.set_event_loop(None)


def test_code_run_approval_allows_without_node_binding(env):
    """code_run 的审批没有节点，不能被节点门挡成 403。"""
    action = env.create(
        conversation_id="conv-1",
        node_id="",  # code_run 在服务端本地执行，没有节点
        tool_name="code_run",
        command="print(1)",
        requester="u1",
    )

    with _client() as client:
        response = client.post(
            f"/agent/conversations/conv-1/approvals/{action.confirmation_id}",
            json={"result": "allow", "command_hash": action.command_hash},
        )

    assert response.status_code == 200, response.text
    assert response.json()["result"] == "allow"
    assert action.resolved == "allow"


def test_code_run_approval_still_checks_command_hash(env):
    """跳过节点门不等于放弃防篡改：命令哈希不匹配仍要拒。"""
    action = env.create(
        conversation_id="conv-1",
        node_id="",
        tool_name="code_run",
        command="print(1)",
        requester="u1",
    )

    with _client() as client:
        response = client.post(
            f"/agent/conversations/conv-1/approvals/{action.confirmation_id}",
            json={"result": "allow", "command_hash": "deadbeef"},
        )

    assert response.status_code == 409
    assert action.resolved is None


def test_node_shell_approval_still_enforces_node_gate(env, monkeypatch):
    """绑了节点的审批照旧要过节点在线/能力校验，本次改动不放宽它。"""
    action = env.create(
        conversation_id="conv-1",
        node_id="node-1",
        tool_name="node_shell_exec",
        command="rm -rf /tmp/x",
        requester="u1",
    )

    async def _authorized(caller, node_id):
        return True

    async def _unavailable(node_id):
        return False

    monkeypatch.setattr(agent_api, "_caller_authorized_for_node", _authorized)
    monkeypatch.setattr(agent_api, "_node_available_for_host_exec", _unavailable)

    with _client() as client:
        response = client.post(
            f"/agent/conversations/conv-1/approvals/{action.confirmation_id}",
            json={"result": "allow", "command_hash": action.command_hash},
        )

    assert response.status_code == 409
    assert action.resolved is None
