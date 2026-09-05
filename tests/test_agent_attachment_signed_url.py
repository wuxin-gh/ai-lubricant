"""Signed-URL access on the agent attachment content/thumbnail endpoints.

Verifies the presigned URL replaces the session-coupled path: a valid signature
grants access with no login state, a tampered/expired one is refused, and the
login-state fallback still works for old clients.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import agent.api as agent_api
import attachment_signing

SIGNING_KEY = "presigned-url-test-key"


@pytest.fixture()
def png_attachment(tmp_path, monkeypatch):
    """A real PNG on disk plus a fake attachment_store bound to it."""
    monkeypatch.setenv("AGENT_ATTACHMENT_SIGNING_KEY", SIGNING_KEY)
    path = tmp_path / "shot.png"
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
    row = {
        "id": 4242,
        "name": "shot.png",
        "mime_type": "image/png",
        "size_bytes": 72,
        "status": "active",
        "storage_kind": "workspace_ref",
        "workspace_path": str(path),
        "expires_at": None,
    }

    real_store = __import__("attachment_store")
    calls: dict[str, list] = {"by_id": [], "by_owner": []}

    class _Store:
        # Only the lookup functions are faked; the media-security helpers are real.
        detect_image_kind = staticmethod(real_store.detect_image_kind)
        is_svg_attachment = staticmethod(real_store.is_svg_attachment)
        object_path = staticmethod(real_store.object_path)
        to_public_dict = staticmethod(real_store.to_public_dict)

        @staticmethod
        async def get_active_by_id(attachment_id, *, include_purged=False):
            calls["by_id"].append(attachment_id)
            return row if attachment_id == row["id"] else None

        @staticmethod
        async def get_for_owner(attachment_id, owner_user_id, *, include_purged=False):
            calls["by_owner"].append((attachment_id, owner_user_id))
            return row if attachment_id == row["id"] and owner_user_id == "u1" else None

    import sys
    monkeypatch.setitem(sys.modules, "attachment_store", _Store)
    return row, calls


def _client(caller: str | None) -> TestClient:
    app = FastAPI()
    app.include_router(agent_api.router)
    app.dependency_overrides[agent_api.get_agent_caller] = lambda: caller
    return TestClient(app)


def _signed_params(attachment_id: int, kind: str) -> dict[str, str]:
    url = attachment_signing.sign_attachment_url(attachment_id, kind)
    assert url is not None
    query = parse_qs(urlparse(url).query)
    return {"exp": query["exp"][0], "sig": query["sig"][0]}


def test_signed_url_serves_content_without_login_state(png_attachment):
    row, calls = png_attachment
    params = _signed_params(row["id"], "content")

    # caller=None means "no user session"; only the signature can authorize here.
    with _client(None) as client:
        response = client.get(f"/agent/attachments/{row['id']}/content", params=params)

    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["content-type"].startswith("image/png")
    assert calls["by_id"] == [row["id"]]
    assert calls["by_owner"] == []


def test_tampered_signature_falls_back_and_is_refused(png_attachment):
    row, _ = png_attachment
    params = _signed_params(row["id"], "content")
    params["sig"] = params["sig"][:-1] + ("a" if params["sig"][-1] != "a" else "b")

    with _client(None) as client:
        response = client.get(f"/agent/attachments/{row['id']}/content", params=params)

    assert response.status_code == 403


def test_expired_signature_is_refused(png_attachment):
    row, _ = png_attachment
    params = _signed_params(row["id"], "content")
    # Re-sign against a past deadline so only expiry (not the digest) fails.
    past = int(params["exp"]) - 10_000
    import hashlib
    import hmac
    sig = hmac.new(
        SIGNING_KEY.encode("utf-8"),
        f"agent-attachment:{row['id']}:content:{past}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    with _client(None) as client:
        response = client.get(
            f"/agent/attachments/{row['id']}/content", params={"exp": str(past), "sig": sig},
        )

    assert response.status_code == 403


def test_content_signature_cannot_open_thumbnail(png_attachment):
    row, _ = png_attachment
    params = _signed_params(row["id"], "content")

    with _client(None) as client:
        response = client.get(f"/agent/attachments/{row['id']}/thumbnail", params=params)

    assert response.status_code == 403


def test_thumbnail_signature_serves_thumbnail(png_attachment):
    row, _ = png_attachment
    params = _signed_params(row["id"], "thumbnail")

    with _client(None) as client:
        response = client.get(f"/agent/attachments/{row['id']}/thumbnail", params=params)

    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"


def test_login_state_fallback_still_serves_content(png_attachment):
    row, calls = png_attachment

    with _client("u1") as client:
        response = client.get(f"/agent/attachments/{row['id']}/content")

    assert response.status_code == 200
    assert calls["by_owner"] == [(row["id"], "u1")]
    assert calls["by_id"] == []


def test_unsigned_request_without_session_is_refused(png_attachment):
    row, _ = png_attachment

    with _client(None) as client:
        response = client.get(f"/agent/attachments/{row['id']}/content")

    assert response.status_code == 403
