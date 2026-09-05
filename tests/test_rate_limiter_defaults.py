"""RateLimiter 默认限制回归测试。

验证：当 API Key 已存在但未配置 rate_limit（即留空）时，rpm/rpd 应解析为 0（不限），
而非旧的硬编码兜底 60/1000。未知 key 仍走 config 的安全兜底。
"""
import asyncio

import pytest

import rate_limiter
from rate_limiter import RateLimiter


@pytest.fixture(autouse=True)
def stub_config(monkeypatch):
    """已知 key 返回空 rate_limit；未知 key 返回 config 安全兜底。"""
    async def fake_get_rate_limit(api_key: str) -> dict:
        if api_key == "known-key":
            return {}
        if api_key == "unknown-key":
            return {"requests_per_minute": 60, "requests_per_day": 1000}
        return {}

    monkeypatch.setattr(
        rate_limiter.config.Config,
        "get_api_key_rate_limit",
        staticmethod(fake_get_rate_limit),
    )
    # 确保 Redis 不可用，走内存计数器（空 key 无记录）
    monkeypatch.setattr(rate_limiter.RedisLimitBackend, "redis", lambda *a, **kw: None)


def test_known_key_with_empty_rate_limit_is_unlimited():
    limits = asyncio.run(RateLimiter._limits("known-key"))
    assert limits["requests_per_minute"] == 0
    assert limits["requests_per_day"] == 0
    assert limits["requests_per_5h"] == 0
    assert limits["requests_per_week"] == 0
    assert limits["tokens_per_minute"] == 0
    assert limits["concurrent_requests"] == 0


def test_known_key_usage_reports_unlimited():
    usage = asyncio.run(RateLimiter.get_usage("known-key"))
    assert usage["rpm_limit"] == 0
    assert usage["rpd_limit"] == 0
    assert usage["rpm_used"] == 0
    assert usage["rpd_used"] == 0


def test_unknown_key_keeps_config_safety_default():
    """未知 key 仍由 config 返回 60/1000，RateLimiter 透传。"""
    limits = asyncio.run(RateLimiter._limits("unknown-key"))
    assert limits["requests_per_minute"] == 60
    assert limits["requests_per_day"] == 1000


def test_known_key_with_explicit_limits_passthrough():
    async def fake_get_rate_limit(api_key: str) -> dict:
        return {"requests_per_minute": 120, "requests_per_day": 5000}

    # 复用 stub_config 的 monkeypatch 需重新设置
    import rate_limiter as rl
    rl.config.Config.get_api_key_rate_limit = staticmethod(fake_get_rate_limit)  # type: ignore[assignment]
    limits = asyncio.run(RateLimiter._limits("known-key"))
    assert limits["requests_per_minute"] == 120
    assert limits["requests_per_day"] == 5000
