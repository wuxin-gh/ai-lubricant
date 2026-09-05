"""Agent attachment signing + media URL security tests.

Covers:
- sign/verify round-trip
- expiry rejection
- kind interchange (content sig cannot open thumbnail, and vice versa)
- tampered sig
- empty-key fail closed
- attachment_store media magic-byte detection
- conversation_store _msg_row_to_dict injects signed URLs
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from unittest import mock

import attachment_signing
import attachment_store


def _set_key(monkeypatch, value: str | None):
    if value is None:
        monkeypatch.delenv("AGENT_ATTACHMENT_SIGNING_KEY", raising=False)
    else:
        monkeypatch.setenv("AGENT_ATTACHMENT_SIGNING_KEY", value)


def test_sign_verify_roundtrip(monkeypatch):
    _set_key(monkeypatch, "a-shared-secret-for-signing")
    url = attachment_signing.sign_attachment_url(42, "content", ttl_seconds=3600)
    assert url is not None
    assert url.startswith("/agent/attachments/42/content?")
    assert "exp=" in url and "sig=" in url
    # Parse query params
    from urllib.parse import parse_qs, urlparse
    q = parse_qs(urlparse(url).query)
    exp = int(q["exp"][0])
    sig = q["sig"][0]
    assert exp > int(time.time())
    assert attachment_signing.verify_attachment_token(42, "content", exp, sig) is True


def test_expired_signature_rejected(monkeypatch):
    _set_key(monkeypatch, "a-shared-secret-for-signing")
    past_exp = int(time.time()) - 60
    # Manually build a sig for an already-expired timestamp.
    from hashlib import sha256
    import hmac as _hmac
    key = b"a-shared-secret-for-signing"
    message = f"agent-attachment:7:content:{past_exp}".encode()
    sig = _hmac.new(key, message, sha256).hexdigest()
    assert attachment_signing.verify_attachment_token(7, "content", past_exp, sig) is False


def test_kind_interchange_rejected(monkeypatch):
    _set_key(monkeypatch, "a-shared-secret-for-signing")
    url = attachment_signing.sign_attachment_url(10, "content", ttl_seconds=3600)
    from urllib.parse import parse_qs, urlparse
    q = parse_qs(urlparse(url).query)
    exp = int(q["exp"][0])
    sig = q["sig"][0]
    # Content sig cannot open thumbnail
    assert attachment_signing.verify_attachment_token(10, "thumbnail", exp, sig) is False
    # Content sig opens content
    assert attachment_signing.verify_attachment_token(10, "content", exp, sig) is True


def test_tampered_signature_rejected(monkeypatch):
    _set_key(monkeypatch, "a-shared-secret-for-signing")
    url = attachment_signing.sign_attachment_url(5, "thumbnail", ttl_seconds=3600)
    from urllib.parse import parse_qs, urlparse
    q = parse_qs(urlparse(url).query)
    exp = int(q["exp"][0])
    # Tamper: flip a character
    sig = q["sig"][0][:-1] + ("a" if q["sig"][0][-1] != "a" else "b")
    assert attachment_signing.verify_attachment_token(5, "thumbnail", exp, sig) is False


def test_wrong_attachment_id_rejected(monkeypatch):
    _set_key(monkeypatch, "a-shared-secret-for-signing")
    url = attachment_signing.sign_attachment_url(99, "content", ttl_seconds=3600)
    from urllib.parse import parse_qs, urlparse
    q = parse_qs(urlparse(url).query)
    exp = int(q["exp"][0])
    sig = q["sig"][0]
    # Sig was for id=99, verifying with id=100 must fail
    assert attachment_signing.verify_attachment_token(100, "content", exp, sig) is False


def test_empty_key_fail_closed(monkeypatch):
    _set_key(monkeypatch, None)
    assert attachment_signing.sign_attachment_url(1, "content") is None
    assert attachment_signing.verify_attachment_token(1, "content", int(time.time()) + 3600, "fakesig") is False


def test_ensure_signing_key_generates_and_persists(monkeypatch, tmp_path):
    """缺失时生成随机密钥、注入环境、写回 .env，返回同一值。"""
    _set_key(monkeypatch, None)
    env_file = tmp_path / ".env"
    env_file.write_text("EXISTING=1\n", encoding="utf-8")
    import dotenv_loader
    monkeypatch.setattr(dotenv_loader, "ENV_FILE", env_file)

    key = attachment_signing.ensure_signing_key()
    assert key and len(key) >= 32
    # 注入了当前进程环境
    assert os.environ["AGENT_ATTACHMENT_SIGNING_KEY"] == key
    # 写回了 .env
    assert "AGENT_ATTACHMENT_SIGNING_KEY=" in env_file.read_text(encoding="utf-8")
    # 生成后签发/验签立即可用
    url = attachment_signing.sign_attachment_url(1, "content")
    assert url is not None


def test_ensure_signing_key_respects_existing(monkeypatch):
    """已配置时原样返回，不覆盖。"""
    _set_key(monkeypatch, "already-configured-key")
    assert attachment_signing.ensure_signing_key() == "already-configured-key"


def test_invalid_kind_rejected(monkeypatch):
    _set_key(monkeypatch, "a-shared-secret-for-signing")
    assert attachment_signing.sign_attachment_url(1, "invalid-kind") is None
    assert attachment_signing.verify_attachment_token(1, "invalid-kind", int(time.time()) + 3600, "sig") is False


def test_detect_image_kind_uses_magic_bytes(tmp_path: Path) -> None:
    cases = {
        "image.png": (b"\x89PNG\r\n\x1a\n\x00", "image/png"),
        "image.jpg": (b"\xff\xd8\xff\xe0\x00", "image/jpeg"),
        "image.gif": (b"GIF89a\x00", "image/gif"),
        "image.webp": (b"RIFF\x00\x00\x00\x00WEBPVP8 ", "image/webp"),
        "image.svg": (b'<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>', "image/svg+xml"),
    }
    for name, (body, expected) in cases.items():
        path = tmp_path / name
        path.write_bytes(body)
        assert attachment_store.detect_image_kind(str(path)) == expected


def test_declared_image_with_non_image_bytes_is_not_inline_candidate(tmp_path: Path) -> None:
    path = tmp_path / "not-really.png"
    path.write_bytes(b"plain text, not a PNG")
    row = {"mime_type": "image/png", "name": path.name, "workspace_path": str(path), "storage_kind": "workspace_ref"}

    assert attachment_store.detect_image_kind(str(path)) is None
    assert attachment_store.is_svg_attachment(row, str(path)) is False


def test_msg_row_to_dict_injects_signed_urls(monkeypatch):
    """_msg_row_to_dict should inject content_url/thumbnail_url for attachment media parts."""
    _set_key(monkeypatch, "test-signing-key-for-injection")
    from agent.conversation_store import _msg_row_to_dict
    import json

    media = [{"type": "attachment", "attachment_id": 42, "name": "test.png", "mime_type": "image/png"}]
    row = {
        "id": 1,
        "conversation_id": "conv-1",
        "role": "assistant",
        "content": "here is an image",
        "media": json.dumps(media),
        "tool_calls": None,
        "tool_results": None,
        "usage": None,
    }
    result = _msg_row_to_dict(row)
    assert isinstance(result["media"], list)
    assert len(result["media"]) == 1
    part = result["media"][0]
    assert "content_url" in part
    assert "thumbnail_url" in part
    assert part["content_url"].startswith("/agent/attachments/42/content?")
    assert part["thumbnail_url"].startswith("/agent/attachments/42/thumbnail?")
    # Verify the injected URLs are valid
    from urllib.parse import parse_qs, urlparse
    cq = parse_qs(urlparse(part["content_url"]).query)
    assert attachment_signing.verify_attachment_token(42, "content", int(cq["exp"][0]), cq["sig"][0])


def test_msg_row_to_dict_skips_public_url_parts(monkeypatch):
    """Media parts with url (no attachment_id) should not get signed URLs."""
    _set_key(monkeypatch, "test-signing-key")
    from agent.conversation_store import _msg_row_to_dict
    import json

    media = [{"type": "attachment", "url": "https://example.com/img.png", "name": "remote.png"}]
    row = {
        "id": 2,
        "conversation_id": "conv-2",
        "role": "assistant",
        "content": "remote image",
        "media": json.dumps(media),
        "tool_calls": None,
        "tool_results": None,
        "usage": None,
    }
    result = _msg_row_to_dict(row)
    part = result["media"][0]
    assert "content_url" not in part
    assert "thumbnail_url" not in part


def test_msg_row_to_dict_no_key_means_no_injection(monkeypatch):
    """When signing key is absent, no content_url is injected."""
    _set_key(monkeypatch, None)
    from agent.conversation_store import _msg_row_to_dict
    import json

    media = [{"type": "attachment", "attachment_id": 99, "name": "test.png"}]
    row = {
        "id": 3,
        "conversation_id": "conv-3",
        "role": "assistant",
        "content": "no key",
        "media": json.dumps(media),
        "tool_calls": None,
        "tool_results": None,
        "usage": None,
    }
    result = _msg_row_to_dict(row)
    part = result["media"][0]
    assert "content_url" not in part
    assert "thumbnail_url" not in part


def test_to_signed_media_part(monkeypatch):
    _set_key(monkeypatch, "test-key-for-signed-part")
    attachment = {
        "id": 77,
        "name": "photo.jpg",
        "mime_type": "image/jpeg",
        "size_bytes": 1024,
        "status": "active",
        "expires_at": None,
    }
    part = attachment_store.to_signed_media_part(attachment)
    assert part["attachment_id"] == 77
    assert "content_url" in part
    assert "thumbnail_url" in part
    assert part["content_url"].startswith("/agent/attachments/77/content?")
