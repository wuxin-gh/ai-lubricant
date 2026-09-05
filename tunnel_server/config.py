"""Settings for the standalone tunnel runtime service.

The runtime service shares the compatibility database (``mc_tunnel_*`` tables)
with the main service but never imports its process-local executors: it is the
only process that spawns tunnel clients for the ``__main__`` target and drives
node-dispatched runtimes.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote

from loguru import logger

_TRUE = {"1", "true", "yes", "on"}


def _value(name: str, default: str = "", legacy: str | None = None) -> str:
    """Read ``name``; fall back to a pre-rebrand ``legacy`` name with a warning."""
    value = os.environ.get(name, "").strip()
    if value or legacy is None:
        return value or default
    legacy_value = os.environ.get(legacy, "").strip()
    if legacy_value:
        logger.warning("[tunnel-server] legacy env {} used; rename to {}", legacy, name)
        return legacy_value
    return default


def _boolean(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in _TRUE


def _integer(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _floating(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _database_url() -> str:
    url = _value("AI_LUBRICANT_DATABASE_URL", legacy="MONKEYCODE_DATABASE_URL")
    if url:
        return url
    user = quote(_value("POSTGRES_USER"), safe="")
    password = quote(_value("POSTGRES_PASSWORD"), safe="")
    host = _value("POSTGRES_HOST", "127.0.0.1")
    port = _integer("POSTGRES_PORT", 5432)
    database = _value("POSTGRES_DATABASE", "ai-lubricant")
    return f"asyncpg://{user}:{password}@{host}:{port}/{database}"


@dataclass(frozen=True)
class TunnelRuntimeSettings:
    database_url: str
    host: str
    port: int
    node_server_url: str
    node_control_token: str
    notify_channel: str
    reconcile_poll_seconds: float
    ready_timeout_seconds: float
    quick_ready_timeout_seconds: float
    binary_cache_dir: str
    enabled: bool
    instance_id: str
    lease_ttl_seconds: int


def load_settings() -> TunnelRuntimeSettings:
    # Owner identity for the DB lease. Explicit env wins (an operator may want
    # a stable id across restarts); otherwise derive from hostname+pid so two
    # replicas never claim the same owner. Date.now/random are avoided here
    # because the settings module is imported once at process start.
    instance_id = _value("TUNNEL_RUNTIME_INSTANCE_ID")
    if not instance_id:
        import socket
        import os
        instance_id = f"{socket.gethostname()}-{os.getpid()}"
    return TunnelRuntimeSettings(
        database_url=_database_url(),
        host=_value("TUNNEL_RUNTIME_HOST", "0.0.0.0"),
        port=_integer("TUNNEL_RUNTIME_PORT", 8004),
        node_server_url=_value("AGENT_COMPOSE_BASE_URL", "http://127.0.0.1:8003"),
        node_control_token=_value("NODE_CONTROL_TOKEN"),
        notify_channel=_value("TUNNEL_RUNTIME_NOTIFY_CHANNEL", "tunnel_runtime_changed"),
        reconcile_poll_seconds=max(5.0, _floating("TUNNEL_RUNTIME_RECONCILE_POLL_SECONDS", 30.0)),
        ready_timeout_seconds=max(5.0, _floating("TUNNEL_RUNTIME_READY_TIMEOUT_SECONDS", 30.0)),
        quick_ready_timeout_seconds=max(15.0, _floating("TUNNEL_RUNTIME_QUICK_READY_TIMEOUT_SECONDS", 90.0)),
        binary_cache_dir=_value("MC_TUNNEL_BIN_DIR"),
        enabled=_boolean("TUNNEL_RUNTIME_ENABLED", True),
        instance_id=instance_id,
        lease_ttl_seconds=max(10, _integer("TUNNEL_RUNTIME_LEASE_TTL_SECONDS", 60)),
    )


settings = load_settings()
