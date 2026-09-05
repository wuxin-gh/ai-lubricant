"""Unit tests for the team-audit secret masking helper.

These are pure-function tests (no DB/redis): they only exercise
``monkeycode_compat.team_users_service._mask_secrets``, the redaction applied
before an ``mc_audits`` row is written. The contract:

* plaintext-credential keys (``password`` / ``new_password`` /
  ``current_password``) have their *values* replaced with ``"***"``, at any
  nesting depth;
* every other field is preserved verbatim;
* the input object is never mutated (deep copy).
"""
from __future__ import annotations

from monkeycode_compat.team_users_service import _mask_secrets


def test_masks_top_level_password_keys_only():
    payload = {
        "password": "p",
        "new_password": "n",
        "current_password": "c",
        "email": "a@b.com",
        "name": "Alice",
    }
    masked = _mask_secrets(payload)
    assert masked["password"] == "***"
    assert masked["new_password"] == "***"
    assert masked["current_password"] == "***"
    # Everything non-credential is preserved verbatim.
    assert masked["email"] == "a@b.com"
    assert masked["name"] == "Alice"


def test_masks_nested_password_keys():
    payload = {"email": "a@b.com", "nested": {"new_password": "x", "keep": 1}}
    masked = _mask_secrets(payload)
    assert masked["email"] == "a@b.com"
    assert masked["nested"]["new_password"] == "***"
    assert masked["nested"]["keep"] == 1


def test_masks_password_inside_list_of_dicts():
    payload = {"users": [{"password": "p1", "email": "x@y.com"}, {"password": "p2"}]}
    masked = _mask_secrets(payload)
    assert masked["users"][0]["password"] == "***"
    assert masked["users"][0]["email"] == "x@y.com"
    assert masked["users"][1]["password"] == "***"


def test_does_not_mutate_input():
    payload = {"password": "secret", "nested": {"new_password": "y"}}
    _mask_secrets(payload)
    # Original object is untouched — masking works on a deep copy.
    assert payload["password"] == "secret"
    assert payload["nested"]["new_password"] == "y"


def test_passthrough_non_container_values():
    assert _mask_secrets("plain") == "plain"
    assert _mask_secrets(42) == 42
    assert _mask_secrets(None) is None


def test_partial_password_present():
    """Only the keys that exist are redacted; absent ones are simply not added."""
    payload = {"new_password": "n", "email": "a@b.com"}
    masked = _mask_secrets(payload)
    assert masked == {"new_password": "***", "email": "a@b.com"}
    assert "password" not in masked


def test_masks_cside_credential_keys():
    """C-side write bodies carry Git tokens / MCP-channel secrets / API keys;
    all of them must be redacted, while descriptive fields survive."""
    payload = {
        "platform": "github",
        "access_token": "ghp_xxx",
        "oauth_refresh_token": "rt_xxx",
        "token": "bot_tok",
        "secret_token": "st_xxx",
        "secret": "chan_secret",
        "api_key": "sk-xxx",
        "name": "my-identity",
    }
    masked = _mask_secrets(payload)
    for k in ("access_token", "oauth_refresh_token", "token", "secret_token", "secret", "api_key"):
        assert masked[k] == "***", k
    # Non-secret descriptive fields are preserved.
    assert masked["platform"] == "github"
    assert masked["name"] == "my-identity"


def test_masks_headers_wholesale():
    """``headers`` carry auth under arbitrary key names, so the whole value is
    redacted rather than trying to match individual header names."""
    payload = {"url": "https://mcp.example", "headers": {"Authorization": "Bearer t"}}
    masked = _mask_secrets(payload)
    assert masked["headers"] == "***"
    assert masked["url"] == "https://mcp.example"
