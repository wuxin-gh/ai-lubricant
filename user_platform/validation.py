"""Dependency-free input validation for C-side user creation.

Kept free of any ORM/tortoise import so it can be unit-tested without a
database and reused by ``auth_service`` without pulling extra deps. Mirrors the
validation upstream applies before persisting a user.
"""
from __future__ import annotations

import re

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_MIN_PASSWORD_LEN = 6


def normalize_email(email: str) -> str:
    """Trim + lowercase an email for storage/lookup."""
    return (email or "").strip().lower()


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email or ""))


def is_strong_enough(password: str) -> bool:
    return bool(password) and len(password) >= _MIN_PASSWORD_LEN


def validate_new_credentials(email: str, password: str) -> str:
    """Return a normalized email or raise ValueError with a stable reason code.

    Reason codes: ``invalid_email`` | ``weak_password``. Callers layer the
    ``email_taken`` uniqueness check on top after a DB lookup.
    """
    normalized = normalize_email(email)
    if not is_valid_email(normalized):
        raise ValueError("invalid_email")
    if not is_strong_enough(password):
        raise ValueError("weak_password")
    return normalized
