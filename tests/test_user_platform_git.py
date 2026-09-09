"""Phase C.3 tests: git domain secret masking.

These tests avoid a real database. They assert the critical security property
of the git service: secret fields (access_token / oauth_refresh_token / token /
secret_token) are NEVER returned in full — only presence + a short masked tail.
"""
from __future__ import annotations


def test_mask_secret_never_leaks_full_value():
    from user_platform.masking import mask_secret

    masked = mask_secret("ghp_abcdefghijklmnop")
    assert "abcdefghijklmnop" not in masked
    assert masked.endswith("mnop")
    assert masked.startswith("***")


def test_mask_secret_short_and_empty():
    from user_platform.masking import mask_secret

    # Short secrets must not reveal any tail.
    assert mask_secret("short") == "***"
    assert mask_secret("") == ""
    assert mask_secret(None) == ""


def test_has_secret_flag():
    from user_platform.masking import has_secret

    assert has_secret("anything") is True
    assert has_secret("") is False
    assert has_secret(None) is False
