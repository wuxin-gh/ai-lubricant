"""Split-topology proxies: node forward-proxy + FollowNodeSession passthrough.

These cover the data-role side without a real control process:

* :class:`RemoteNodeConnectManager` posts the correct descriptor and rebuilds
  the ``status`` / ``headers`` / ``iter_chunks()`` surface from the control
  stream, and raises ``ConnectionError`` (not a hang) on error/misconfig.
* The control-side ``/internal/node-proxy`` router gates on the bearer token.
"""
from __future__ import annotations

import base64
import json

import pytest

from monkeycode_compat.node_client import proxy as npg
from node_server import node_proxy_gateway as control_npg


class _FakeContent:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk


class _FakeResponse:
    def __init__(self, status, headers, chunks, text=""):
        self.status = status
        self.headers = headers
        self.content = _FakeContent(chunks)
        self._text = text
        self.released = False

    async def text(self):
        return self._text

    def release(self):
        self.released = True


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.closed = False
        self.posted = None

    async def post(self, url, *, json=None, headers=None):
        self.posted = (url, json, headers)
        return self._response

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_remote_manager_streams_and_projects_headers(monkeypatch):
    resp = _FakeResponse(
        status=200,
        headers={
            npg._STATUS_HEADER: "201",
            npg._HEADERS_HEADER: json.dumps({"content-type": "text/plain"}),
        },
        chunks=[b"foo", b"bar"],
    )
    session = _FakeSession(resp)

    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: session)
    monkeypatch.setattr(aiohttp, "ClientTimeout", lambda **k: ("timeout", k))

    manager = npg.RemoteNodeConnectManager(base_url="http://control.local", token="tok")
    proxy = await manager.request("node-1", method="POST", url="https://api.test/v1", body=b"payload")

    url, descriptor, headers = session.posted
    assert url == "http://control.local/internal/node-proxy/node-1"
    assert headers["Authorization"] == "Bearer tok"
    assert descriptor["method"] == "POST"
    assert descriptor["url"] == "https://api.test/v1"
    assert base64.b64decode(descriptor["body"]) == b"payload"

    assert proxy.status == 201
    assert proxy.headers == {"content-type": "text/plain"}
    chunks = [chunk async for chunk in proxy.iter_chunks()]
    assert chunks == [b"foo", b"bar"]
    # Draining closes the upstream session + releases the response.
    assert resp.released is True
    assert session.closed is True


@pytest.mark.asyncio
async def test_remote_manager_missing_config_raises_not_hang():
    manager = npg.RemoteNodeConnectManager(base_url="", token="")
    with pytest.raises(ConnectionError):
        await manager.request("node-1", method="GET", url="https://api.test")


@pytest.mark.asyncio
async def test_remote_manager_error_status_raises(monkeypatch):
    resp = _FakeResponse(status=502, headers={}, chunks=[], text="node offline")
    session = _FakeSession(resp)

    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: session)
    monkeypatch.setattr(aiohttp, "ClientTimeout", lambda **k: ("timeout", k))

    manager = npg.RemoteNodeConnectManager(base_url="http://control.local", token="tok")
    with pytest.raises(ConnectionError, match="502"):
        await manager.request("node-1", method="GET", url="https://api.test")
    assert resp.released is True
    assert session.closed is True


def test_control_router_rejects_bad_token():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.include_router(control_npg.build_node_proxy_router(lambda: object(), "secret"))
    client = TestClient(app)

    resp = client.post("/internal/node-proxy/node-1", json={"method": "GET", "url": "x"})
    assert resp.status_code == 403

    resp = client.post(
        "/internal/node-proxy/node-1",
        json={"method": "GET", "url": "x"},
        headers={"Authorization": "Bearer wrong"},
    )
    assert resp.status_code == 403
