"""Startup dependency configuration loaded from process environment and .env."""
from __future__ import annotations

import os
from typing import Any

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


class ConfigurationError(RuntimeError):
    """Startup dependency configuration is missing or invalid."""


def _error(message: str) -> ConfigurationError:
    return ConfigurationError(f"启动配置无效: {message}")


def _required_text(env_names: tuple[str, ...], label: str) -> str:
    for env_name in env_names:
        value = os.getenv(env_name)
        if value is not None and value.strip():
            return value.strip()
    raise _error(f"缺少 {label}；请设置 {env_names[0]} 或 .env 对应变量")


def _required_int(env_name: str, label: str, *, minimum: int, maximum: int | None = None) -> int:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        raise _error(f"缺少 {label}；请设置 {env_name} 或 .env 对应变量")
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise _error(f"{env_name} 必须是整数") from exc
    if value < minimum or (maximum is not None and value > maximum):
        range_text = f"{minimum}-{maximum}" if maximum is not None else f">= {minimum}"
        raise _error(f"{env_name} 必须在范围 {range_text} 内")
    return value


def _optional_text(env_names: tuple[str, ...], default: str) -> str:
    for env_name in env_names:
        raw = os.getenv(env_name)
        if raw is not None and raw.strip():
            return raw.strip()
    return default


def _optional_bool(env_name: str, default: bool = False) -> bool:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in _TRUE_VALUES


def _optional_int(env_name: str, default: int) -> int:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def get_postgres_config() -> dict[str, Any]:
    return {
        "host": _required_text(("POSTGRES_HOST",), "PostgreSQL host"),
        "port": _required_int("POSTGRES_PORT", "postgres.port", minimum=1, maximum=65535),
        "user": _required_text(("POSTGRES_USER",), "PostgreSQL user"),
        "password": _required_text(("POSTGRES_PASSWORD",), "PostgreSQL password"),
        "database": _required_text(("POSTGRES_DATABASE", "POSTGRES_DB"), "PostgreSQL database"),
    }


def get_postgres_pool_limits() -> dict[str, int]:
    min_size = _optional_int("POSTGRES_POOL_MIN_SIZE", 1)
    max_size = _optional_int("POSTGRES_POOL_MAX_SIZE", 10)
    if min_size < 1 or max_size < 1:
        raise _error("PostgreSQL 连接池大小必须大于等于 1")
    if min_size > max_size:
        raise _error("postgres.pool_min_size 不能大于 postgres.pool_max_size")
    return {"min_size": min_size, "max_size": max_size}


def get_redis_config() -> dict[str, Any]:
    return {
        "prefix_key": _required_text(("REDIS_PREFIX_KEY",), "redis.prefix_key"),
        "host": _required_text(("REDIS_HOST",), "Redis host"),
        "port": _required_int("REDIS_PORT", "redis.port", minimum=1, maximum=65535),
        "db": _required_int("REDIS_DB", "redis.db", minimum=0),
        "decode_responses": _required_bool("REDIS_DECODE_RESPONSES", "redis.decode_responses"),
        "max_connections": _required_int("REDIS_MAX_CONNECTIONS", "redis.max_connections", minimum=1),
        "stream_timeout": _optional_int("REDIS_STREAM_TIMEOUT", 10),
        "pool_timeout": _optional_int("REDIS_POOL_TIMEOUT", 10),
    }


def _required_bool(env_name: str, label: str) -> bool:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        raise _error(f"缺少 {label}；请设置 {env_name} 或 .env 对应变量")
    text = raw.strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    raise _error(f"{env_name} 必须是 boolean（true/false）")


def get_clickhouse_config() -> dict[str, Any]:
    return {
        "enabled": _optional_bool("CLICKHOUSE_REQUEST_PAYLOAD_ENABLED"),
        "addr": _optional_text(("CLICKHOUSE_ADDR",), ""),
        "database": _optional_text(("CLICKHOUSE_DATABASE",), "ai_lubricant_logs"),
        "username": _optional_text(("CLICKHOUSE_USERNAME",), ""),
        "password": _optional_text(("CLICKHOUSE_PASSWORD",), ""),
        "ttl_days": _optional_int("CLICKHOUSE_REQUEST_PAYLOAD_TTL_DAYS", 30),
        "max_payload_bytes": _optional_int("CLICKHOUSE_MAX_PAYLOAD_BYTES", 4194304),
    }
