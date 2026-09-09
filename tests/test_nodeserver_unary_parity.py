"""Unary Connect-RPC parity: the NodeService router speaks the exact
JSON wire shape the data-side ``user_platform.node_client`` sends and parses.

We mount :func:`build_unary_router` on a bare FastAPI app and drive it with the
same camelCase request bodies + ``NODE_STATUS_*`` enum-name shapes the client's
``_rpc`` produces, then assert the responses flow cleanly through the client's
``normalize_node_info`` projection. No network, no Go daemon: an in-memory
Tortoise DB backs the store and a fresh registry holds (no) live connections.

The app is driven with an **async** httpx client over ``ASGITransport`` so the
request tasks run on the same event loop the fixture bound Tortoise to (a sync
``TestClient`` spins its own loop, stranding the global-fallback connection).
"""
from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from node_server import agentcompose_v2_pb2 as pb  # noqa: F401
from node_server import crypto
from node_server.registry import Registry
from node_server.service import NodeService
from node_server.store import node_store
from node_server.unary_api import build_unary_router

_MASTER_KEY_HEX = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
_SERVICE_PATH = "/agentcompose.v2.NodeService"


@pytest_asyncio.fixture
async def db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"node_server": ["node_server.store"]},
        use_tz=False,
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


def _build_app(*, api_token: str = "") -> FastAPI:
    service = NodeService(
        master_key=crypto.parse_master_key(_MASTER_KEY_HEX),
        server_url="http://127.0.0.1:8001",
        store=node_store,
        registry=Registry(),
    )
    app = FastAPI()
    app.include_router(build_unary_router(service, api_token=api_token))
    return app


def _make_client(app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


async def _post(client: httpx.AsyncClient, method: str, body: dict, **kwargs):
    return await client.post(f"{_SERVICE_PATH}/{method}", json=body, **kwargs)


@pytest.mark.asyncio
async def test_onboard_passive_container_then_list_normalizes(db):
    async with _make_client(_build_app()) as client:
        # Onboard a passive grouping container — approved + online, no credential.
        resp = await _post(
            client, "OnboardNode", {"role": "NODE_ROLE_PASSIVE_MANAGEMENT", "nodeName": "mgr"}
        )
        assert resp.status_code == 200
        data = resp.json()
        # camelCase fields + proto enum names, exactly what _normalize_node_info reads.
        assert data["nodeId"].startswith("node-")
        # A passive container mints no credential / install command.
        assert not data.get("secret")
        node = data["node"]
        assert node["status"] == "NODE_STATUS_APPROVED"
        assert node["role"] == "NODE_ROLE_PASSIVE_MANAGEMENT"
        assert node["online"] is True

        # Feed the response through the real client projection.
        from user_platform.node_client.normalization import normalize_node_info

        norm = normalize_node_info(node)
        assert norm["status"] == "approved"
        # Both management kinds project to the top-level "management" role; the
        # passive container is distinguished by the explicit is_passive flag.
        assert norm["role"] == "management"
        assert norm["is_passive"] is True
        assert norm["connected"] is False
        assert norm["online"] is True
        # Passive nodes have no heartbeat; do not disguise connected_at as one.
        assert norm["last_heartbeat_at"] == ""


@pytest.mark.asyncio
async def test_onboard_management_client_mints_secret(db):
    async with _make_client(_build_app()) as client:
        # A management-with-client node is a real client: pending + credential +
        # install command, exactly like an execution node.
        resp = await _post(client, "OnboardNode", {"role": "NODE_ROLE_MANAGEMENT", "nodeName": "mgr"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["secret"]  # base32 secret returned once
        assert "install-management-" in data["scriptUrl"]
        node = data["node"]
        assert node["status"] == "NODE_STATUS_PENDING"
        assert node["role"] == "NODE_ROLE_MANAGEMENT"

        from user_platform.node_client.normalization import normalize_node_info

        norm = normalize_node_info(node)
        assert norm["role"] == "management"
        assert norm["is_passive"] is False


@pytest.mark.asyncio
async def test_onboard_execution_requires_manager(db):
    async with _make_client(_build_app()) as client:
        resp = await _post(client, "OnboardNode", {"role": "NODE_ROLE_EXECUTION"})
        # Missing manager_node_id → invalid_argument → HTTP 400 with a Connect error body.
        assert resp.status_code == 400
        body = resp.json()
        assert body["code"] == "invalid_argument"
        assert "manager_node_id" in body["message"]


@pytest.mark.asyncio
async def test_onboard_execution_mints_secret_and_lists(db):
    async with _make_client(_build_app()) as client:
        mgr = (await _post(client, "OnboardNode", {"role": "NODE_ROLE_PASSIVE_MANAGEMENT"})).json()
        resp = await _post(
            client,
            "OnboardNode",
            {
                "role": "NODE_ROLE_EXECUTION",
                "startupMethod": "NODE_STARTUP_METHOD_SYSTEMD",
                "managerNodeId": mgr["nodeId"],
                "nodeName": "worker-1",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["secret"]  # base32 secret returned once
        assert "install-execution-systemd.sh" in data["scriptUrl"]
        assert data["otpauthUri"].startswith("otpauth://totp/")
        assert data["node"]["status"] == "NODE_STATUS_PENDING"

        # ListNodes returns both; the client normalizes each NodeInfo.
        listed = (await _post(client, "ListNodes", {})).json()
        assert len(listed["nodes"]) == 2

        # Filtered list by status.
        pending = (await _post(client, "ListNodes", {"status": "NODE_STATUS_PENDING"})).json()
        assert len(pending["nodes"]) == 1
        assert pending["nodes"][0]["nodeId"] == data["nodeId"]


@pytest.mark.asyncio
async def test_approve_and_revoke_via_unary(db):
    async with _make_client(_build_app()) as client:
        mgr = (await _post(client, "OnboardNode", {"role": "NODE_ROLE_PASSIVE_MANAGEMENT"})).json()
        ex = (
            await _post(
                client,
                "OnboardNode",
                {"role": "NODE_ROLE_EXECUTION", "managerNodeId": mgr["nodeId"]},
            )
        ).json()
        node_id = ex["nodeId"]

        approved = (await _post(client, "ApproveNode", {"nodeId": node_id})).json()
        assert approved["node"]["status"] == "NODE_STATUS_APPROVED"

        revoked = (await _post(client, "RevokeNode", {"nodeId": node_id})).json()
        assert revoked["node"]["status"] == "NODE_STATUS_REVOKED"


@pytest.mark.asyncio
async def test_dispatch_no_node_unavailable(db):
    async with _make_client(_build_app()) as client:
        # No connected node → the server returns Connect "unavailable" (HTTP 503).
        resp = await _post(client, "DispatchSession", {"session": {"provider": "claude"}})
        assert resp.status_code == 503
        assert resp.json()["code"] == "unavailable"


@pytest.mark.asyncio
async def test_dispatch_normalizes_null_repeated_session_fields(db):
    """Empty config lists sent as null must parse as [] instead of failing 400."""
    async with _make_client(_build_app()) as client:
        resp = await _post(
            client,
            "DispatchSession",
            {
                "session": {
                    "provider": "claude",
                    "skills": "[]",
                    "plugins": "[]",
                    "mcps": "[]",
                    "env": "[]",
                }
            },
        )
        # Parsing succeeded and reached node selection; there is simply no live
        # node in this test. Before normalization this returned HTTP 400 with
        # "repeated field skills must be in []".
        assert resp.status_code == 503
        assert resp.json()["code"] == "unavailable"


@pytest.mark.asyncio
async def test_unknown_method_not_found(db):
    async with _make_client(_build_app()) as client:
        resp = await _post(client, "NoSuchMethod", {})
        assert resp.status_code == 404
        assert resp.json()["code"] == "not_found"


@pytest.mark.asyncio
async def test_bearer_token_gate(db):
    """When a token is configured, calls without it are permission-denied."""
    async with _make_client(_build_app(api_token="s3cr3t")) as client:
        # No Authorization header → 403.
        r = await _post(client, "ListNodes", {})
        assert r.status_code == 403
        # Wrong token → 403.
        r = await _post(client, "ListNodes", {}, headers={"Authorization": "Bearer nope"})
        assert r.status_code == 403
        # Correct token → 200.
        r = await _post(client, "ListNodes", {}, headers={"Authorization": "Bearer s3cr3t"})
        assert r.status_code == 200
