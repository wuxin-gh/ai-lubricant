"""启动配置与主配置持久化契约测试。"""
import asyncio
import json
import os
from pathlib import Path

import pytest


_PROJ = Path(__file__).resolve().parents[1]
import sys
if str(_PROJ) not in sys.path:
    sys.path.insert(0, str(_PROJ))

import bootstrap_config
from bootstrap_config import ConfigurationError
from config_store import PostgresConfigStore
from db import PostgresClient


def _clear_config_env(monkeypatch) -> None:
    """Strip all bootstrap env vars so each test sees only what it sets."""
    for key in (
        "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD",
        "POSTGRES_DATABASE", "POSTGRES_DB", "POSTGRES_POOL_MIN_SIZE", "POSTGRES_POOL_MAX_SIZE",
        "REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PREFIX_KEY", "REDIS_DECODE_RESPONSES",
        "REDIS_MAX_CONNECTIONS", "REDIS_STREAM_TIMEOUT", "REDIS_POOL_TIMEOUT",
        "CLICKHOUSE_REQUEST_PAYLOAD_ENABLED",
    ):
        monkeypatch.delenv(key, raising=False)


def test_postgres_environment_overrides_bootstrap_file(monkeypatch):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv("POSTGRES_HOST", "env-host")
    monkeypatch.setenv("POSTGRES_PORT", "15432")
    monkeypatch.setenv("POSTGRES_USER", "env-user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "env-pass")
    monkeypatch.setenv("POSTGRES_DATABASE", "env-db")

    assert bootstrap_config.get_postgres_config() == {
        "host": "env-host", "port": 15432, "user": "env-user", "password": "env-pass", "database": "env-db",
    }


def _minimal_redis_env(monkeypatch, **overrides) -> None:
    _clear_config_env(monkeypatch)
    defaults = {
        "REDIS_PREFIX_KEY": "marsview",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_DB": "0",
        "REDIS_DECODE_RESPONSES": "true",
        "REDIS_MAX_CONNECTIONS": "500",
    }
    defaults.update(overrides)
    for key, value in defaults.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, str(value))


def test_missing_redis_prefix_key_fails(monkeypatch):
    _minimal_redis_env(monkeypatch, REDIS_PREFIX_KEY=None)
    monkeypatch.delenv("REDIS_PREFIX_KEY", raising=False)
    with pytest.raises(ConfigurationError, match="redis.prefix_key"):
        bootstrap_config.get_redis_config()


def test_missing_redis_host_fails_without_hardcoded_target(monkeypatch):
    _minimal_redis_env(monkeypatch, REDIS_HOST=None)
    monkeypatch.delenv("REDIS_HOST", raising=False)
    with pytest.raises(ConfigurationError, match="Redis host"):
        bootstrap_config.get_redis_config()


def test_invalid_redis_port_fails(monkeypatch):
    _minimal_redis_env(monkeypatch, REDIS_PORT="0")
    with pytest.raises(ConfigurationError, match="REDIS_PORT"):
        bootstrap_config.get_redis_config()


def test_missing_redis_decode_responses_fails(monkeypatch):
    _clear_config_env(monkeypatch)
    for key in ("REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PREFIX_KEY", "REDIS_MAX_CONNECTIONS"):
        monkeypatch.setenv(key, {"REDIS_HOST": "localhost", "REDIS_PORT": "6379", "REDIS_DB": "0", "REDIS_PREFIX_KEY": "marsview", "REDIS_MAX_CONNECTIONS": "500"}[key])
    monkeypatch.delenv("REDIS_DECODE_RESPONSES", raising=False)
    with pytest.raises(ConfigurationError, match="redis.decode_responses"):
        bootstrap_config.get_redis_config()


def test_missing_redis_max_connections_fails(monkeypatch):
    _minimal_redis_env(monkeypatch, REDIS_MAX_CONNECTIONS=None)
    monkeypatch.delenv("REDIS_MAX_CONNECTIONS", raising=False)
    with pytest.raises(ConfigurationError, match="redis.max_connections"):
        bootstrap_config.get_redis_config()


def test_missing_main_configuration_does_not_seed_or_write_file(monkeypatch):
    store = PostgresConfigStore()
    writes = []

    async def get_config(key):
        assert key == "main"
        return None

    async def set_config(*args):
        writes.append(args)

    monkeypatch.setattr(PostgresClient, "get_config", get_config)
    monkeypatch.setattr(PostgresClient, "set_config", set_config)

    with pytest.raises(ConfigurationError, match="app_config"):
        asyncio.run(store.read_main_async())
    assert writes == []
