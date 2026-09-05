"""Password hashing for C-side users.

Uses the ``bcrypt`` library directly rather than ``passlib``: passlib 1.7.4
reads ``bcrypt.__about__.__version__`` which was removed in bcrypt 5.x, and its
``detect_wrap_bug`` probe crashes on bcrypt 5. The bcrypt library is
byte-compatible with the MonkeyCode Go side (``golang.org/x/crypto/bcrypt``),
so ``$2a$``/``$2b$`` hashes verify across both implementations.

bcrypt only hashes the first 72 bytes of a password. We reject longer inputs
explicitly instead of silently truncating, matching a safe default.
"""
from __future__ import annotations

import bcrypt

_MAX_PASSWORD_BYTES = 72


def hash_password(password: str) -> str:
    """Hash a plaintext password with bcrypt (DefaultCost-equivalent)."""
    raw = password.encode("utf-8")
    if len(raw) > _MAX_PASSWORD_BYTES:
        raise ValueError("password too long (max 72 bytes)")
    return bcrypt.hashpw(raw, bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    """Verify a plaintext password against a stored bcrypt hash."""
    if not password or not hashed:
        return False
    raw = password.encode("utf-8")
    if len(raw) > _MAX_PASSWORD_BYTES:
        return False
    try:
        return bcrypt.checkpw(raw, hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False
