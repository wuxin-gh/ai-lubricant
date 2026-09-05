"""Device credentials and short-lived pairing codes for device-control.

Mirrors ``server/internal/store/store.go`` from the device-control reference
server, with two deliberate changes for this deployment:

- **Pairing codes live in Redis, not process memory.** The Go server keeps them
  in a map because it is a single process; we run multiple workers, so an
  in-memory code minted by worker A could not be redeemed on worker B. TTL and
  single-use are enforced by Redis (``SET NX EX`` + ``GETDEL``).
- **Device credentials live in the builtin-tool detail rows**, not a JSON file,
  so they participate in the existing instance/token governance and survive
  restarts like every other builtin resource.

Tokens and pairing codes are only ever stored as SHA-256 hashes: a leaked state
row does not yield a usable credential. Comparison is constant-time.

The code alphabet and normalization MUST stay byte-identical to
``store.go:31`` / ``store.go:148-156`` — the Android app normalizes locally
before sending (``PairingClient.normalizeCode``), so any divergence here turns
a correct code into a 403.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets

from .protocol import new_id

# Omits I/O/0/1 so a code survives being read aloud or retyped (store.go:31).
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

CODE_LEN = 8

# How long a freshly minted pairing code stays redeemable (httpapi.go:25).
PAIRING_CODE_TTL_SECONDS = 600

_PAIRING_KEY_PREFIX = "device_control:pairing"


class PairingError(Exception):
    """Base for pairing failures."""


class UnknownCodeError(PairingError):
    """Unknown, expired, or already-redeemed pairing code.

    Deliberately covers all three: the caller must not be able to tell them
    apart (store.go:25-27), and the HTTP layer answers 403 for all of them.
    """


class RedisUnavailableError(PairingError):
    """Redis is required for pairing and is not reachable.

    Pairing cannot fall back to process memory: a code minted on one worker
    would be unredeemable on another, which looks like a random failure to the
    user. Failing loudly is better than pairing that works one time in N.
    """


def hash_secret(value: str) -> str:
    """SHA-256 hex of a token or normalized pairing code."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def secrets_equal(a: str, b: str) -> bool:
    """Constant-time comparison of two hex digests."""
    return hmac.compare_digest(a, b)


def normalize_code(code: str) -> str:
    """Uppercase and strip everything outside ``[A-Z0-9]`` (store.go:148-156).

    Makes codes forgiving to type: case and separators are ignored, so
    ``abcd-efgh`` and ``ABCDEFGH`` are the same code. Characters outside the
    alphabet are not rejected here — they simply will not match any minted
    code, and the caller gets the same undistinguished 403.
    """
    return "".join(ch for ch in code.upper() if ch.isascii() and (ch.isdigit() or "A" <= ch <= "Z"))


def generate_code() -> str:
    """Mint a display code like ``K7QM-3XPD`` (store.go:119-145)."""
    chars = [secrets.choice(CODE_ALPHABET) for _ in range(CODE_LEN)]
    chars.insert(CODE_LEN // 2, "-")
    return "".join(chars)


def generate_token() -> str:
    """256 bits of URL-safe randomness for a long-lived device token."""
    return secrets.token_urlsafe(32)


def new_device_id() -> str:
    return new_id("dev_")


def _redis():
    try:
        from rd import JdbcClient
        return JdbcClient.redis
    except Exception:
        return None


def _pairing_key(code_hash: str) -> str:
    """裸 key（不含客户端前缀），供覆写方法 ``set`` 使用。"""
    return f"{_PAIRING_KEY_PREFIX}:{code_hash}"


def _prefixed_pairing_key(redis, code_hash: str) -> str:
    """带前缀的完整 key，供**未被覆写**的原生方法使用。

    ``RedisJdbc`` 只给一部分方法（get/set/delete/expire/...）加了前缀，
    ``getdel``/``eval`` 不在其中（见 rd.py）。所以写用裸 key、读用完整 key，
    两边最终指向同一个物理 key；这个不对称是 RedisJdbc 的既有设计，
    与 limits/backend.py 的 ``_key`` 处理同源。
    """
    return redis.get_key(_pairing_key(code_hash))


async def mint_pairing_code(instance_id: int, label: str = "") -> tuple[str, int]:
    """Create a single-use pairing code bound to a builtin-tool resource owner.

    Returns ``(plaintext_code, ttl_seconds)``. Only the code's hash is stored,
    so the plaintext exists solely in the caller's response — the same
    one-shot-secret shape the CDP client tokens use.

    ``SET NX`` guards against the (vanishingly unlikely) case of minting a code
    whose hash already exists; a collision returns a fresh code rather than
    silently rebinding someone else's pending pairing.
    """
    redis = _redis()
    if redis is None:
        raise RedisUnavailableError("Redis 不可用，无法生成配对码")

    payload = f"owner:{int(instance_id)}|{label}"
    for _ in range(5):
        code = generate_code()
        key = _pairing_key(hash_secret(normalize_code(code)))
        try:
            created = await redis.set(key, payload, nx=True, ex=PAIRING_CODE_TTL_SECONDS)
        except Exception as exc:
            raise RedisUnavailableError(f"写入配对码失败: {exc}") from exc
        if created:
            return code, PAIRING_CODE_TTL_SECONDS
    raise RedisUnavailableError("生成配对码失败，请重试")


async def mint_pairing_code_for_owner(owner_user_id: str, label: str = "") -> tuple[str, int]:
    """Create a single-use pairing code bound to a C-side owner (user id).

    Identical one-shot semantics to :func:`mint_pairing_code`; the payload
    distinguishes the owner-scoped form so redemption knows how to attribute
    the new device resource without any instance layer.
    """
    redis = _redis()
    if redis is None:
        raise RedisUnavailableError("Redis 不可用，无法生成配对码")

    payload = f"owner:{owner_user_id}|{label}"
    for _ in range(5):
        code = generate_code()
        key = _pairing_key(hash_secret(normalize_code(code)))
        try:
            created = await redis.set(key, payload, nx=True, ex=PAIRING_CODE_TTL_SECONDS)
        except Exception as exc:
            raise RedisUnavailableError(f"写入配对码失败: {exc}") from exc
        if created:
            return code, PAIRING_CODE_TTL_SECONDS
    raise RedisUnavailableError("生成配对码失败，请重试")


async def redeem_pairing_code(code: str) -> tuple[str, str]:
    """Consume a pairing code, returning ``(owner_payload, label)``.

    Single use is enforced by ``GETDEL``: two devices racing the same code mean
    exactly one wins and the other gets :class:`UnknownCodeError`. A plain
    ``GET`` then ``DELETE`` would let both through.

    The first tuple element is the raw scoped payload — ``owner:<uid>`` for
    owner-scoped codes (new form) or a bare legacy instance id. The caller
    decides how to attribute the new device resource.
    """
    normalized = normalize_code(code)
    if not normalized:
        raise UnknownCodeError("unknown or expired pairing code")

    redis = _redis()
    if redis is None:
        raise RedisUnavailableError("Redis 不可用，无法完成配对")

    full_key = _prefixed_pairing_key(redis, hash_secret(normalized))
    try:
        raw = await redis.getdel(full_key)
    except AttributeError:
        # Older redis-py without GETDEL: fall back to an atomic Lua swap rather
        # than GET+DELETE, which would let two devices redeem one code.
        script = "local v = redis.call('GET', KEYS[1]) if v then redis.call('DEL', KEYS[1]) end return v"
        try:
            raw = await redis.eval(script, keys=[full_key], args=[])
        except Exception as exc:
            raise RedisUnavailableError(f"读取配对码失败: {exc}") from exc
    except Exception as exc:
        raise RedisUnavailableError(f"读取配对码失败: {exc}") from exc

    if not raw:
        raise UnknownCodeError("unknown or expired pairing code")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    owner_part, _, label = str(raw).partition("|")
    if not owner_part:
        raise UnknownCodeError("unknown or expired pairing code")
    return owner_part, label


__all__ = [
    "CODE_ALPHABET",
    "mint_pairing_code_for_owner",
    "CODE_LEN",
    "PAIRING_CODE_TTL_SECONDS",
    "PairingError",
    "UnknownCodeError",
    "RedisUnavailableError",
    "hash_secret",
    "secrets_equal",
    "normalize_code",
    "generate_code",
    "generate_token",
    "new_device_id",
    "mint_pairing_code",
    "redeem_pairing_code",
]
