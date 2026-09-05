"""CDP 网页对话的 code_run 审批链路。

网页对话此前压根没接审批协调器：``tools.set_code_run_approval`` 没调、
``agent_runner_loop`` 没传 ``on_tool_batch``，于是 ``_code_run_approval is None``
让 code_run 第一道守卫直接返回 ``approval_required``，连 confirmation 都生成不出来。
这里锁住修复后的契约：CDP 侧有自己的裁决端点（internal token + client 归属鉴权，
不认 C 端 cookie），且 code_run 的 ``node_id==""`` 不被节点门挡掉。
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent import cdp_chat_service
from agent import conversation_store as real_store
from monkeycode_compat.node_client.approvals import ApprovalRegistry


CLIENT_ID = "7"
HEADERS = {"X-Internal-Token": "it", "X-Cdp-Client-Id": CLIENT_ID}


def _conv(client_id: str = CLIENT_ID) -> dict:
    return {"id": "conv-1", "agent_id": 5, "chat_settings": {"cdp_client_id": client_id}}


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(cdp_chat_service.router)
    return TestClient(app)


@pytest.fixture
def env(monkeypatch):
    """内部 token + 已授权客户端 + 干净的审批注册表（不污染模块单例）。

    ApprovalRegistry.create 要在当前线程拿 event loop 建 Future；同步 TestClient
    测试里显式建一条，与 test_agent_approval_endpoint.py 同口径。
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    monkeypatch.setenv("AGENT_INTERNAL_TOKEN", "it")
    monkeypatch.setattr(cdp_chat_service, "_require_ch", lambda: None)

    async def _chat_client(client_id):
        assert client_id == CLIENT_ID
        return [5]

    monkeypatch.setattr(cdp_chat_service, "_require_chat_client", _chat_client)

    conversations = {"conv-1": _conv()}

    async def _get_conversation(conv_id):
        return conversations.get(conv_id)

    monkeypatch.setattr(real_store, "get_conversation", _get_conversation)

    registry = ApprovalRegistry()
    import monkeycode_compat.node_client.approvals as approvals_mod

    monkeypatch.setattr(approvals_mod, "approval_registry", registry)
    yield registry, conversations
    loop.close()
    asyncio.set_event_loop(None)


def _code_run_action(registry: ApprovalRegistry):
    return registry.create(
        conversation_id="conv-1",
        node_id="",  # code_run 在服务端本地执行，没有节点
        tool_name="code_run",
        command='{"code":"print(1)","timeout":60,"type":"python"}',
        requester="u1",
    )


def test_cdp_allow_resolves_code_run_approval(env):
    """面板点「允许」→ 端点 resolve → agent 侧 future 被唤醒继续执行。"""
    registry, _ = env
    action = _code_run_action(registry)

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "allow", "command_hash": action.command_hash},
        )

    assert response.status_code == 200, response.text
    assert response.json()["result"] == "allow"
    assert action.resolved == "allow"
    assert action.future.done() and action.future.result() == "allow"


def test_cdp_deny_resolves_as_denied(env):
    registry, _ = env
    action = _code_run_action(registry)

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "deny", "command_hash": action.command_hash},
        )

    assert response.status_code == 200, response.text
    assert action.resolved == "deny"


def test_cdp_approval_checks_command_hash(env):
    """防篡改：哈希不匹配不放行，审批仍挂起。"""
    registry, _ = env
    action = _code_run_action(registry)

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "allow", "command_hash": "deadbeef"},
        )

    assert response.status_code == 409
    assert action.resolved is None


def test_cdp_approval_rejects_other_clients_conversation(env):
    """会话属于别的浏览器客户端 → 403，不能跨客户端放行代码执行。"""
    registry, conversations = env
    conversations["conv-1"] = _conv(client_id="other")
    action = _code_run_action(registry)

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "allow", "command_hash": action.command_hash},
        )

    assert response.status_code == 403
    assert action.resolved is None


def test_cdp_approval_refuses_node_bound_action(env):
    """CDP 客户端 token 不该能放行节点命令：带 node_id 的审批 fail-closed。"""
    registry, _ = env
    action = registry.create(
        conversation_id="conv-1",
        node_id="node-1",
        tool_name="node_shell_exec",
        command="rm -rf /tmp/x",
        requester="u1",
    )

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "allow", "command_hash": action.command_hash},
        )

    assert response.status_code == 403
    assert action.resolved is None


def test_cdp_approval_requires_internal_token(env):
    registry, _ = env
    action = _code_run_action(registry)

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers={"X-Cdp-Client-Id": CLIENT_ID},
            json={"result": "allow", "command_hash": action.command_hash},
        )

    assert response.status_code == 401
    assert action.resolved is None


def test_cdp_approval_rejects_unknown_confirmation(env):
    with _client() as client:
        response = client.post(
            "/agent/client/conversations/conv-1/approvals/does-not-exist",
            headers=HEADERS,
            json={"result": "allow", "command_hash": "x"},
        )

    assert response.status_code == 404


def test_cdp_approval_rejects_already_resolved(env):
    registry, _ = env
    action = _code_run_action(registry)
    registry.resolve(action.confirmation_id, "allow")

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "deny", "command_hash": action.command_hash},
        )

    assert response.status_code == 409


def test_cdp_approval_validates_result_value(env):
    registry, _ = env
    action = _code_run_action(registry)

    with _client() as client:
        response = client.post(
            f"/agent/client/conversations/conv-1/approvals/{action.confirmation_id}",
            headers=HEADERS,
            json={"result": "maybe", "command_hash": action.command_hash},
        )

    assert response.status_code == 400
    assert action.resolved is None
