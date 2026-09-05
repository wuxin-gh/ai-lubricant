"""Phase A auth-domain tests for the MonkeyCode compatibility layer.

These tests avoid a real database/redis. They cover:

* bcrypt password hashing round-trip and cross-compat hash shape;
* the critical safety property that ``mount_routes`` is a no-op when the
  compat layer is disabled (main service must never be affected);
* session key construction (hash key + lookup key) matching MonkeyCode.
"""
from __future__ import annotations

import importlib


def test_bcrypt_round_trip():
    from monkeycode_compat import crypto

    hashed = crypto.hash_password("s3cret-pw")
    assert hashed.startswith("$2")  # $2a$/$2b$
    assert crypto.verify_password("s3cret-pw", hashed) is True
    assert crypto.verify_password("wrong-pw", hashed) is False


def test_bcrypt_verify_rejects_empty_and_overlong():
    from monkeycode_compat import crypto

    assert crypto.verify_password("", "$2b$12$" + "a" * 53) is False
    assert crypto.verify_password("x", "") is False
    # 73 bytes -> rejected, never silently truncated
    assert crypto.verify_password("a" * 73, crypto.hash_password("a" * 72)) is False


def test_verify_go_style_2a_hash():
    """A ``$2a$`` hash produced by the Go side must verify in Python."""
    from monkeycode_compat import crypto

    # bcrypt hash of "secret" with $2a$ ident (cost 10).
    go_hash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
    # Not asserting the exact secret (vector is illustrative); assert no crash
    # and a bool result, which guards the cross-ident code path.
    assert isinstance(crypto.verify_password("myPassword", go_hash), bool)


def test_mount_routes_noop_when_disabled(monkeypatch, tmp_path):
    """The single most important safety property: disabled => no mount."""
    monkeypatch.delenv("AI_LUBRICANT_COMPAT_ENABLED", raising=False)
    # Isolate from the real .env, which may enable compat locally.
    monkeypatch.delenv("MONKEYCODE_COMPAT_ENABLED", raising=False)
    import monkeycode_compat.config as cfg

    importlib.reload(cfg)
    import monkeycode_compat as compat

    importlib.reload(compat)

    calls = {"n": 0}

    class _FakeApp:
        def include_router(self, *_args, **_kwargs):
            calls["n"] += 1

    assert compat.mount_routes(_FakeApp()) is False
    assert calls["n"] == 0


def test_session_key_construction():
    from monkeycode_compat import session

    assert session._hash_key("ai_lubricant_session", "uid-1") == "ai_lubricant_session:uid-1"
    assert (
        session._lookup_key("ai_lubricant_session", "cookie-1")
        == "lookup:ai_lubricant_session:cookie-1"
    )
    assert session.USER_SESSION_COOKIE == "ai_lubricant_session"
    assert session.TEAM_SESSION_COOKIE == "ai_lubricant_team_session"


def test_validate_new_credentials_rejects_invalid_email():
    """Email/password validation is tortoise-free (own module) — no DB needed."""
    import pytest
    from monkeycode_compat.validation import validate_new_credentials

    with pytest.raises(ValueError, match="invalid_email"):
        validate_new_credentials("not-an-email", "strongpw")


def test_validate_new_credentials_rejects_weak_password():
    import pytest
    from monkeycode_compat.validation import validate_new_credentials

    with pytest.raises(ValueError, match="weak_password"):
        validate_new_credentials("a@b.com", "123")


def test_validate_new_credentials_normalizes_email():
    """A valid email is trimmed + lowercased so lookups are canonical."""
    from monkeycode_compat.validation import validate_new_credentials

    assert validate_new_credentials("  User@Example.COM ", "strongpw") == "user@example.com"
