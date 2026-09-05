"""API Key 固定窗口 IP 个数限制离线测试（无 Redis/PG）。"""

import asyncio

import pytest
from fastapi import HTTPException

import rate_limiter
from rate_limiter import RateLimiter


@pytest.fixture(autouse=True)
def reset_rate_limiter(monkeypatch):
    RateLimiter._blocked_until.clear()

    async def fake_get_rate_limit(_api_key: str) -> dict:
        return {
            "max_ips": 2,
            "ip_window_seconds": 21600,
        }

    monkeypatch.setattr(
        rate_limiter.config.Config,
        "get_api_key_rate_limit",
        staticmethod(fake_get_rate_limit),
    )
    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "acquire_concurrency",
        classmethod(lambda cls, *args, **kwargs: _async_return(None)),
    )
    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "incr_window",
        classmethod(lambda cls, *args, **kwargs: _async_return(None)),
    )


async def _async_return(value):
    return value


def _install_ip_set(monkeypatch):
    ips: set[str] = set()
    calls: list[tuple[str, str, int, int]] = []

    async def check_and_add(_cls, key: str, ip: str, limit: int, ttl: int):
        calls.append((key, ip, limit, ttl))
        if ip in ips:
            return True, len(ips)
        if len(ips) >= limit:
            return False, len(ips)
        ips.add(ip)
        return True, len(ips)

    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "check_and_add_ip",
        classmethod(check_and_add),
    )
    return ips, calls


def test_new_ips_are_allowed_until_limit_and_existing_ip_stays_allowed(monkeypatch):
    ips, calls = _install_ip_set(monkeypatch)

    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.1"))
    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.2"))
    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.1"))

    assert ips == {"10.0.0.1", "10.0.0.2"}
    assert all(call[2:] == (2, 21600) for call in calls)


def test_new_ip_over_limit_is_rejected_without_changing_set(monkeypatch):
    ips, _calls = _install_ip_set(monkeypatch)
    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.1"))
    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.2"))

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.3"))

    assert exc_info.value.status_code == 429
    assert exc_info.value.detail == "API Key IP limit exceeded"
    assert ips == {"10.0.0.1", "10.0.0.2"}


def test_missing_ip_skips_ip_backend(monkeypatch):
    calls = []

    async def check_and_add(*args, **kwargs):
        calls.append((args, kwargs))
        return False, 2

    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "check_and_add_ip",
        classmethod(check_and_add),
    )

    asyncio.run(RateLimiter.acquire("sk-ip", client_ip=None))
    assert calls == []


def test_zero_limit_skips_ip_backend(monkeypatch):
    async def no_ip_limit(_api_key: str) -> dict:
        return {"max_ips": 0, "ip_window_seconds": 3600}

    rate_limiter.config.Config.get_api_key_rate_limit = staticmethod(no_ip_limit)
    calls = []

    async def check_and_add(*args, **kwargs):
        calls.append((args, kwargs))
        return False, 0

    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "check_and_add_ip",
        classmethod(check_and_add),
    )

    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.1"))
    assert calls == []


def test_redis_failure_fails_open(monkeypatch):
    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "check_and_add_ip",
        classmethod(lambda cls, *args, **kwargs: _async_return(None)),
    )
    asyncio.run(RateLimiter.acquire("sk-ip", client_ip="10.0.0.3"))


def test_usage_reports_ip_limit_and_current_count(monkeypatch):
    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "get_window",
        classmethod(lambda cls, *args, **kwargs: _async_return((0, 0))),
    )
    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "concurrency_used",
        classmethod(lambda cls, *args, **kwargs: _async_return(0)),
    )
    monkeypatch.setattr(
        rate_limiter.RedisLimitBackend,
        "set_cardinality",
        classmethod(lambda cls, *args, **kwargs: _async_return(2)),
    )

    usage = asyncio.run(RateLimiter.get_usage("sk-ip"))

    assert usage["ip_limit"] == 2
    assert usage["ip_used"] == 2
