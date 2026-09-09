"""Pure masking helpers with no ORM/runtime dependencies.

Kept dependency-free so it can be imported and unit-tested without optional
packages (e.g. tortoise) being installed.
"""
from __future__ import annotations


def mask_key(key: str) -> str:
    """Mask a runtime/API key for listing: keep prefix + last 4 chars."""
    if not key:
        return ""
    if len(key) <= 12:
        return key[:3] + "***"
    return f"{key[:6]}***{key[-4:]}"


def mask_secret(value: str | None) -> str:
    """Mask a secret (git token etc.): keep only a short tail, never the full value."""
    if not value:
        return ""
    return f"***{value[-4:]}" if len(value) > 8 else "***"


def has_secret(value: str | None) -> bool:
    """Return whether a secret is present, without exposing it."""
    return bool(value)
