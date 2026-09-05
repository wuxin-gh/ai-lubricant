"""Remote node-client parity: the data service dials the control service.

The in-process ``LocalNodeClient`` is gone; the data service always dials the
separate node control process over the Connect unary contract. These tests mock
aiohttp and assert the request shape (URL path, headers, payload) and the
response/error handling — no real network, no Redis/PG, no node_server import.
"""
from __future__ import annotations

import pytest

from monkeycode_compat.node_client import NodeServerUnavailable, RPCError, get_local_node_client
from monkeycode_compat.node_client.client import NodeClient


class _FakeResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    async def json(self, content_type=None):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    def __init__(self):
        self.calls: list[tuple[str, dict, dict]] = []
        self._next = _FakeResponse(200, {})

    def queue(self, status: int, payload):
        self._next = _FakeResponse(status, payload)
        return self

    def post(self, url: str, *, json=None, headers=None):
        self.calls.append((url, json or {}, dict(headers or {})))
        return self._next

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def remote_client(monkeypatch):
    """A NodeClient with mocked aiohttp and a fresh capture."""
    from dataclasses import replace

    from monkeycode_compat import config

    monkeypatch.setattr(
        config,
        "settings",
        replace(
            config.settings,
            agent_compose_base_url="http://control.local",
            node_control_token="tok-123",
            agent_compose_timeout=7,
        ),
    )

    fake_session = _FakeSession()
    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: fake_session)
    monkeypatch.setattr(aiohttp, "ClientTimeout", lambda **k: ("timeout", k))

    return NodeClient(), fake_session


@pytest.mark.asyncio
async def test_remote_list_nodes_request_shape(remote_client):
    client, sess = remote_client
    sess.queue(
        200,
        {
            "nodes": [
                {
                    "nodeId": "node-x",
                    "role": "NODE_ROLE_EXECUTION",
                    "status": "NODE_STATUS_APPROVED",
                    "startupMethod": "NODE_STARTUP_METHOD_STANDALONE",
                    "connected": True,
                    "activeSessionIds": ["sess-a", "sess-b"],
                    "capacity": {"max_sessions": 4},
                }
            ]
        },
    )

    rows = await client.list_nodes(status="approved")

    url, payload, headers = sess.calls[0]
    assert url == "http://control.local/agentcompose.v2.NodeService/ListNodes"
    assert headers["Content-Type"] == "application/json"
    assert headers["Connect-Protocol-Version"] == "1"
    assert headers["Authorization"] == "Bearer tok-123"
    assert payload == {"status": "NODE_STATUS_APPROVED"}

    assert rows[0]["node_id"] == "node-x"
    assert rows[0]["role"] == "execution"
    assert rows[0]["status"] == "approved"
    assert rows[0]["active_session_ids"] == ["sess-a", "sess-b"]
    assert rows[0]["active_sessions"] == 2
    assert rows[0]["capacity"] == {"max_sessions": 4}


@pytest.mark.asyncio
async def test_remote_list_nodes_no_filter_sends_empty_body(remote_client):
    client, sess = remote_client
    sess.queue(200, {"nodes": []})
    await client.list_nodes()
    assert sess.calls[0][1] == {}


@pytest.mark.asyncio
async def test_remote_dispatch_session_request_shape(remote_client):
    client, sess = remote_client
    sess.queue(200, {"sessionId": "sess-1", "nodeId": "node-1"})

    out = await client.dispatch_session("node-1", {"editor": "codex", "prompt": "hi"})

    url, payload, headers = sess.calls[0]
    assert url == "http://control.local/agentcompose.v2.NodeService/DispatchSession"
    assert payload == {"nodeId": "node-1", "session": {"editor": "codex", "prompt": "hi"}}
    assert out == {"sessionId": "sess-1", "nodeId": "node-1"}


@pytest.mark.asyncio
async def test_remote_dispatch_normalizes_null_repeated_session_fields(remote_client):
    client, sess = remote_client
    sess.queue(200, {"sessionId": "sess-1", "nodeId": "node-1"})

    await client.dispatch_session(
        "node-1",
        {
            "provider": "claude",
            "mcps": "[]",
            "skills": "[]",
            "plugins": "[]",
            "env": "[]",
            "volumes": "[]",
        },
    )

    _url, payload, _headers = sess.calls[0]
    assert payload["session"] == {
        "provider": "claude",
        "mcps": [],
        "skills": [],
        "plugins": [],
        "env": [],
        "volumes": [],
    }


@pytest.mark.asyncio
async def test_remote_delete_node_request_shape(remote_client):
    client, sess = remote_client
    sess.queue(200, {"deleted": True})

    out = await client.delete_node("node-9", force=False)

    url, payload, _ = sess.calls[0]
    assert url == "http://control.local/agentcompose.v2.NodeService/DeleteNode"
    assert payload == {"nodeId": "node-9", "force": False}
    assert out == {"deleted": True}


@pytest.mark.asyncio
async def test_remote_send_session_input_request_shape(remote_client):
    client, sess = remote_client
    sess.queue(200, {"ack": True})

    out = await client.send_session_input("sess-1", "user", "hello")

    url, payload, _ = sess.calls[0]
    assert url == "http://control.local/agentcompose.v2.NodeService/SendSessionInput"
    assert payload == {"sessionId": "sess-1", "kind": "user", "text": "hello"}
    assert out == {"ack": True}


@pytest.mark.asyncio
async def test_remote_send_session_input_carries_snapshot(remote_client):
    """Explicit model/mode/llm snapshot fields are forwarded to the node."""
    client, sess = remote_client
    sess.queue(200, {"ack": True})

    out = await client.send_session_input(
        "sess-1",
        "human_message",
        "continue",
        model="gpt-5",
        mode="workspace-write",
        llm={"endpoint": "https://gw.example.com/v1", "apiKey": "sk-snap", "model": "gpt-5"},
    )

    url, payload, _ = sess.calls[0]
    assert url == "http://control.local/agentcompose.v2.NodeService/SendSessionInput"
    assert payload["sessionId"] == "sess-1"
    assert payload["kind"] == "human_message"
    assert payload["text"] == "continue"
    assert payload["model"] == "gpt-5"
    assert payload["mode"] == "workspace-write"
    assert payload["llm"] == {"endpoint": "https://gw.example.com/v1", "apiKey": "sk-snap", "model": "gpt-5"}
    assert out == {"ack": True}


@pytest.mark.asyncio
async def test_remote_connect_error_envelope_raises_rpcerror(remote_client):
    client, sess = remote_client
    sess.queue(404, {"code": "not_found", "message": "node missing"})

    with pytest.raises(RPCError, match="node missing") as excinfo:
        await client.list_nodes()
    assert excinfo.value.code == "not_found"


@pytest.mark.asyncio
async def test_remote_http_error_without_code_raises_unavailable(remote_client):
    client, sess = remote_client
    sess.queue(503, {"reason": "control down"})

    with pytest.raises(NodeServerUnavailable, match="503"):
        await client.list_nodes()


@pytest.mark.asyncio
async def test_remote_missing_config_raises_unavailable(monkeypatch):
    from dataclasses import replace

    from monkeycode_compat import config

    monkeypatch.setattr(
        config,
        "settings",
        replace(config.settings, agent_compose_base_url="", node_control_token=""),
    )

    with pytest.raises(NodeServerUnavailable, match="agent_compose_base_url/token"):
        await NodeClient().list_nodes()


@pytest.mark.asyncio
async def test_remote_control_methods_use_unary_json(remote_client):
    client, sess = remote_client
    sess.queue(200, {"nodeId": "n1", "capacity": {"max_sessions": 1}})
    result = await client.set_node_capacity("n1", {"max_sessions": 1})
    assert result["nodeId"] == "n1"
    assert sess.calls[-1][0].endswith("/SetNodeCapacity")
    assert sess.calls[-1][1] == {"nodeId": "n1", "capacity": {"max_sessions": 1}}

    sess.queue(200, {"node": {"nodeId": "n1"}})
    await client.move_node("n1", "mgr")
    assert sess.calls[-1][0].endswith("/MoveNode")
    assert sess.calls[-1][1] == {"nodeId": "n1", "managerNodeId": "mgr"}


@pytest.mark.asyncio
async def test_remote_host_exec_uses_unary_json(remote_client):
    client, sess = remote_client
    sess.queue(200, {"success": True, "stdout": "ok"})
    result = await client.host_exec("n1", "ls", cwd="/tmp", timeout_ms=1000)
    assert result == {"success": True, "stdout": "ok"}
    assert sess.calls[-1][0].endswith("/HostExec")
    assert sess.calls[-1][1] == {
        "nodeId": "n1", "command": "ls", "cwd": "/tmp",
        "timeoutMs": 1000, "maxOutputBytes": 0,
    }


def test_get_local_node_client_returns_remote_client():
    assert get_local_node_client() is get_local_node_client()
