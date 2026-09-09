"""Standalone tunnel runtime service application.

Mirrors node_server's process shape: Hypercorn + h2c, Tortoise on the compat
database, and a lifespan that owns the single runtime worker. Unlike the main
service, this process owns the ``__main__`` target tunnel clients (it forks
frpc/cloudflared/npc directly) and reconciles node-dispatched runtimes.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from loguru import logger

from fastapi import FastAPI

from .config import settings


async def _init_database() -> None:
    """Initialize the compat Tortoise connection for mc_tunnel_* tables."""
    from tortoise import Tortoise

    await Tortoise.init(
        config={
            "connections": {"user_platform": settings.database_url},
            "apps": {
                "user_platform": {
                    "models": ["user_platform.models_tunnel"],
                    "default_connection": "user_platform",
                }
            },
        },
        use_tz=False,
        _enable_global_fallback=True,
    )
    await Tortoise.generate_schemas(safe=True)
    # Reconcile the new tunnel models with their declarations so additive columns
    # added since the table was first created exist (same contract as the main
    # service's _ensure_additive_columns, scoped here to the tunnel models).
    from user_platform.database import _ensure_additive_columns

    await _ensure_additive_columns()
    from user_platform.database import _migrate_tunnel_runtime

    await _migrate_tunnel_runtime()


async def _close_database() -> None:
    from tortoise import Tortoise

    await Tortoise.close_connections()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if not settings.enabled:
        logger.warning("[tunnel-server] disabled by TUNNEL_RUNTIME_ENABLED; serving health only")
        yield
        return

    await _init_database()
    from .wiring import runtime_worker, notify_listener

    await notify_listener.start()
    await runtime_worker.start()
    try:
        yield
    finally:
        await runtime_worker.stop()
        await notify_listener.stop()
        await _close_database()


def create_app() -> FastAPI:
    from .health import router as health_router

    app = FastAPI(title="ai-lubricant tunnel runtime", lifespan=lifespan)
    app.include_router(health_router)
    return app


app = create_app()
