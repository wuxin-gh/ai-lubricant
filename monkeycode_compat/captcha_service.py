"""Self-hosted cap.js proof-of-work CAPTCHA backend.

The vendored MonkeyCode frontend uses the ``@cap.js/widget`` (SHA-256 PoW
CAPTCHA). The widget talks to a self-hosted backend via two POST endpoints:

* ``POST {apiEndpoint}challenge`` -> ``{"challenge": {c, s, d}, "token": ...,
  "expires": <ms>}``. The widget derives ``c`` challenges from ``token`` using a
  deterministic PRNG (fnv1a-seeded xorshift32): for challenge ``i``
  (1-indexed), ``salt   = prng(f"{token}{i}",  s)`` (length ``s`` hex chars) and
  ``target = prng(f"{token}{i}d", d)`` (length ``d`` hex chars). It then finds a
  ``nonce`` such that ``sha256(salt + nonce)`` hex-encoded starts with
  ``target``.
* ``POST {apiEndpoint}redeem`` with ``{"token", "solutions": [nonce, ...]}``
  -> ``{"success": true, "token": <verification>, "expires": <ms>}``. The
  backend recomputes salt/target for each challenge, verifies each nonce, and
  on success issues a short-lived verification token the app then submits with
  login/register/etc.

This module mirrors the reference cap.js server behavior exactly (the widget's
PRNG and PoW check are the contract). It never touches ``/admin/*`` or the
``/v1/*`` model pipeline. State lives in Redis (shared coredis client) so it
survives across workers and expires automatically.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import time
from dataclasses import dataclass

from loguru import logger

# Challenge tuning — matches cap.js defaults. ``count`` PoW puzzles, each with
# ``salt`` hex chars of salt and ``difficulty`` hex chars of required prefix.
_CHALLENGE_COUNT = 18
_SALT_LENGTH = 32
_DIFFICULTY = 4
# Lifetimes (seconds). The challenge must be solved within this window; the
# redeemed verification token is valid for the login request that follows.
_CHALLENGE_TTL = 600
_TOKEN_TTL = 600

_CHALLENGE_PREFIX = "mc_captcha:challenge:"
_TOKEN_PREFIX = "mc_captcha:token:"


def _fnv1a(text: str) -> int:
    """32-bit FNV-1a, byte-for-byte identical to the widget's ``prng`` seed.

    The widget hashes over ``charCodeAt`` (UTF-16 code units). For the ASCII
    tokens/sal">s we use (hex + digits) that equals the code point, so iterating
    Python ``ord(ch)`` over the string matches exactly.
    """
    h = 2166136261
    for ch in text:
        h ^= ord(ch)
        # hash += (hash<<1)+(hash<<4)+(hash<<7)+(hash<<8)+(hash<<24), 32-bit wrap
        h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) & 0xFFFFFFFF
    return h & 0xFFFFFFFF


def prng(seed: str, length: int) -> str:
    """Deterministic hex string generator matching cap.js widget ``prng``.

    fnv1a-seeded xorshift32; emits 8 hex chars per step, truncated to ``length``.
    """
    state = _fnv1a(seed)
    out: list[str] = []
    produced = 0
    while produced < length:
        # xorshift32, 32-bit wrap after every step
        state ^= (state << 13) & 0xFFFFFFFF
        state ^= state >> 17
        state ^= (state << 5) & 0xFFFFFFFF
        state &= 0xFFFFFFFF
        out.append(format(state, "08x"))
        produced += 8
    return "".join(out)[:length]


def _derive_challenges(token: str, count: int, salt_len: int, difficulty: int) -> list[tuple[str, str]]:
    """Recreate the (salt, target) pairs the widget solves for ``token``."""
    challenges: list[tuple[str, str]] = []
    for i in range(1, count + 1):
        salt = prng(f"{token}{i}", salt_len)
        target = prng(f"{token}{i}d", difficulty)
        challenges.append((salt, target))
    return challenges


def _pow_ok(salt: str, target: str, nonce) -> bool:
    """True when ``sha256(salt + nonce)`` hex starts with ``target``.

    Mirrors the worker: it compares ``target.length / 2`` whole bytes, i.e. the
    hex digest must start with the ``target`` hex prefix. Comparing hex-prefix
    strings is equivalent when ``len(target)`` is even (always true here: even
    ``_DIFFICULTY``).
    """
    digest = hashlib.sha256(f"{salt}{nonce}".encode("utf-8")).hexdigest()
    return digest.startswith(target)


@dataclass(frozen=True)
class ChallengeResult:
    token: str
    count: int
    salt_length: int
    difficulty: int
    expires_ms: int


class CaptchaService:
    """cap.js-compatible PoW CAPTCHA issuer/verifier, backed by Redis."""

    def _redis(self):
        from rd import JdbcClient

        if JdbcClient.redis is None:
            raise RuntimeError("redis client not initialized")
        return JdbcClient.redis

    async def create_challenge(self) -> ChallengeResult:
        """Issue a new challenge; persist its parameters for later redeem."""
        token = secrets.token_hex(25)  # 50 hex chars, matches cap.js token size
        params = {
            "c": _CHALLENGE_COUNT,
            "s": _SALT_LENGTH,
            "d": _DIFFICULTY,
            "ts": int(time.time()),
        }
        redis = self._redis()
        await redis.set(
            f"{_CHALLENGE_PREFIX}{token}",
            json.dumps(params),
            ex=_CHALLENGE_TTL,
        )
        return ChallengeResult(
            token=token,
            count=_CHALLENGE_COUNT,
            salt_length=_SALT_LENGTH,
            difficulty=_DIFFICULTY,
            expires_ms=int((time.time() + _CHALLENGE_TTL) * 1000),
        )

    async def redeem(self, token: str, solutions: list) -> tuple[bool, str, int]:
        """Verify the PoW solutions for ``token``.

        Returns ``(success, verification_token, expires_ms)``. On success a fresh
        verification token is stored (single-use) that the caller submits with
        the protected action; the challenge is consumed either way.
        """
        if not token or not isinstance(solutions, list):
            return False, "", 0
        redis = self._redis()
        key = f"{_CHALLENGE_PREFIX}{token}"
        raw = await redis.get(key)
        if not raw:
            return False, "", 0
        # Consume the challenge immediately — one attempt per issued challenge.
        await redis.delete(key)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            params = json.loads(raw)
        except (ValueError, TypeError):
            return False, "", 0

        count = int(params.get("c", _CHALLENGE_COUNT))
        salt_len = int(params.get("s", _SALT_LENGTH))
        difficulty = int(params.get("d", _DIFFICULTY))
        if len(solutions) != count:
            return False, "", 0

        challenges = _derive_challenges(token, count, salt_len, difficulty)
        for (salt, target), nonce in zip(challenges, solutions):
            if not _pow_ok(salt, target, nonce):
                return False, "", 0

        verification = secrets.token_hex(25)
        expires_ms = int((time.time() + _TOKEN_TTL) * 1000)
        await redis.set(
            f"{_TOKEN_PREFIX}{verification}",
            json.dumps({"ts": int(time.time())}),
            ex=_TOKEN_TTL,
        )
        return True, verification, expires_ms

    async def validate_token(self, token: str, *, consume: bool = True) -> bool:
        """True when ``token`` is a live verification token from a prior redeem.

        Single-use by default: a valid token is deleted on validation so it can
        gate exactly one protected action (login/register).
        """
        if not token:
            return False
        redis = self._redis()
        key = f"{_TOKEN_PREFIX}{token}"
        raw = await redis.get(key)
        if not raw:
            return False
        if consume:
            try:
                await redis.delete(key)
            except Exception:
                logger.debug("[monkeycode-compat] captcha token delete failed", exc_info=True)
        return True


captcha_service = CaptchaService()
