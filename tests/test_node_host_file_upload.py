"""NodeClient.upload_file chunking and the HostFileUpload unary path.

Mocks aiohttp so each ``HostFileUpload`` call returns a queued response in
order; asserts the data service drives chunks at ≤1 MiB, advances ``offset``
by the acked ``bytesWritten``, sends ``sha256``+``final`` only on the last
chunk, and stops on the first non-ok chunk. No real network / Redis / PG.
"""
from __future__ import annotations

import base64
import hashlib

import pytest

from user_platform.node_client import RPCError
from user_platform.node_client.client import NodeClient


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


class _QueuedSession:
    """Aiohttp stand-in that returns queued responses in FIFO order."""

    def __init__(self):
        self.calls: list[tuple[str, dict, dict]] = []
        self._queue: list[_FakeResponse] = []

    def queue(self, payload) -> _FakeResponse:
        resp = _FakeResponse(200, payload)
        self._queue.append(resp)
        return resp

    def post(self, url: str, *, json=None, headers=None):
        self.calls.append((url, json or {}, dict(headers or {})))
        if not self._queue:
            raise AssertionError("no queued response for HostFileUpload call")
        return self._queue.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def remote_client(monkeypatch):
    from dataclasses import replace

    from user_platform import config

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

    fake_session = _QueuedSession()
    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: fake_session)
    monkeypatch.setattr(aiohttp, "ClientTimeout", lambda **k: ("timeout", k))

    return NodeClient(), fake_session


def _chunk_calls(sess: _QueuedSession):
    """Return the JSON payloads sent to HostFileUpload, in order."""
    return [payload for url, payload, _ in sess.calls if url.endswith("/HostFileUpload")]


@pytest.mark.asyncio
async def test_upload_single_chunk_sets_final_and_sha256(remote_client):
    client, sess = remote_client
    data = b"hello world"
    sess.queue({"ok": True, "bytesWritten": len(data), "path": "C:\\tmp\\f.txt", "error": ""})

    result = await client.upload_file("node-1", "C:\\tmp\\f.txt", data)

    assert result["ok"] is True
    assert result["bytes_written"] == len(data)
    calls = _chunk_calls(sess)
    assert len(calls) == 1
    expected_sha = hashlib.sha256(data).hexdigest()
    assert calls[0]["final"] is True
    assert calls[0]["sha256"] == expected_sha
    assert calls[0]["offset"] == 0
    assert calls[0]["totalSize"] == len(data)
    assert base64.b64decode(calls[0]["data"]) == data


@pytest.mark.asyncio
async def test_upload_empty_file_single_final_chunk(remote_client):
    client, sess = remote_client
    sess.queue({"ok": True, "bytesWritten": 0, "path": "/tmp/empty", "error": ""})

    result = await client.upload_file("node-1", "/tmp/empty", b"")

    assert result["ok"] is True
    assert result["bytes_written"] == 0
    calls = _chunk_calls(sess)
    assert len(calls) == 1
    assert calls[0]["final"] is True
    assert calls[0]["sha256"] == hashlib.sha256(b"").hexdigest()
    assert calls[0]["data"] == ""  # base64 of b""


@pytest.mark.asyncio
async def test_upload_splits_at_one_mib_and_advances_offset(remote_client):
    client, sess = remote_client
    # 2.5 MiB → three chunks: 1 MiB, 1 MiB, 0.5 MiB.
    chunk_size = 1024 * 1024
    data = bytes((i % 251 for i in range(chunk_size * 2 + 500)))
    # Queue one ok per chunk; bytesWritten tracks cumulative offset.
    written = 0
    for size in (chunk_size, chunk_size, len(data) - 2 * chunk_size):
        written += size
        sess.queue({"ok": True, "bytesWritten": written, "path": "/tmp/big", "error": ""})

    result = await client.upload_file("node-1", "/tmp/big", data)

    assert result["ok"] is True
    assert result["bytes_written"] == len(data)
    calls = _chunk_calls(sess)
    assert len(calls) == 3
    assert calls[0]["offset"] == 0
    assert calls[1]["offset"] == chunk_size
    assert calls[2]["offset"] == 2 * chunk_size
    assert [c["final"] for c in calls] == [False, False, True]
    # sha256 only on the final chunk.
    assert calls[0]["sha256"] == ""
    assert calls[1]["sha256"] == ""
    assert calls[2]["sha256"] == hashlib.sha256(data).hexdigest()
    # Reassemble the data from base64 chunks.
    reassembled = b"".join(base64.b64decode(c["data"]) for c in calls)
    assert reassembled == data


@pytest.mark.asyncio
async def test_upload_stops_on_first_non_ok_chunk(remote_client):
    client, sess = remote_client
    data = b"x" * (1024 * 1024 * 2)  # 2 chunks
    # First chunk fails (e.g. offset mismatch); node drops the temp file.
    sess.queue({"ok": False, "bytesWritten": 0, "path": "/tmp/x", "error": "offset mismatch: expected 0 got 1"})

    result = await client.upload_file("node-1", "/tmp/x", data)

    assert result["ok"] is False
    assert "offset mismatch" in result["error"]
    calls = _chunk_calls(sess)
    assert len(calls) == 1  # did not send the second chunk


@pytest.mark.asyncio
async def test_upload_rejects_oversize_file_before_any_rpc(remote_client):
    client, sess = remote_client
    data = b"x" * (10 * 1024 * 1024 + 1)

    with pytest.raises(RPCError):
        await client.upload_file("node-1", "/tmp/x", data)
    assert _chunk_calls(sess) == []


@pytest.mark.asyncio
async def test_upload_passes_overwrite_flag(remote_client):
    client, sess = remote_client
    sess.queue({"ok": True, "bytesWritten": 3, "path": "/tmp/f", "error": ""})

    await client.upload_file("node-1", "/tmp/f", b"abc", overwrite=True)

    assert _chunk_calls(sess)[0]["overwrite"] is True
