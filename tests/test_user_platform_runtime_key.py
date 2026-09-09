"""Phase B tests: runtime-key user/vm attribution.

These tests avoid a real database. They assert:
* ``_api_key_row`` carries the additive ``user_id``/``vm_id`` fields through
  cleanly without disturbing existing fields (backward compatible).
* The runtime-key masking helper never leaks the full key.
"""
from __future__ import annotations


def test_api_key_row_carries_user_vm():
    """The main-pipeline key row must surface user_id/vm_id when present."""
    from db import PostgresClient

    row = {
        "id": 7,
        "key": "sk-abcdef",
        "name": "runtime",
        "rate_limit": {},
        "provider_whitelist": [],
        "provider_blacklist": [],
        "model_whitelist": [],
        "model_blacklist": [],
        "selection_strategy": "intelligent",
        "thinking_config": {},
        "disabled": False,
        "user_id": "11111111-1111-1111-1111-111111111111",
        "vm_id": "vm-42",
    }
    out = PostgresClient._api_key_row(row)
    assert out["user_id"] == "11111111-1111-1111-1111-111111111111"
    assert out["vm_id"] == "vm-42"
    # Existing fields still normalize as before.
    assert out["selection_strategy"] == "intelligent"
    assert out["provider_whitelist"] == []


def test_api_key_row_without_user_vm_is_unchanged():
    """Ordinary keys (no user/vm) must behave exactly as before."""
    from db import PostgresClient

    row = {
        "id": 1,
        "key": "sk-plain",
        "name": "",
        "rate_limit": {},
        "provider_whitelist": [],
        "provider_blacklist": [],
        "model_whitelist": [],
        "model_blacklist": [],
        "selection_strategy": "bogus-strategy",
        "thinking_config": {},
        "disabled": False,
    }
    out = PostgresClient._api_key_row(row)
    # Missing user/vm must not raise; falls back to normalized strategy.
    assert out.get("user_id") is None
    assert out.get("vm_id") is None
    assert out["selection_strategy"] != "bogus-strategy"


def test_mask_key_never_leaks_full_key():
    from user_platform.masking import mask_key

    masked = mask_key("sk-abcdef1234567890")
    assert "1234567890" not in masked
    assert masked.endswith("7890")
    assert mask_key("") == ""
    assert "***" in mask_key("sk-short")
