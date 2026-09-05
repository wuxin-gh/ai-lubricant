"""Health endpoints for the tunnel runtime service."""
from __future__ import annotations

from fastapi import APIRouter

from .config import settings

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "tunnel-runtime", "enabled": settings.enabled}


@router.get("/ready")
async def ready() -> dict:
    from .wiring import notify_listener, runtime_worker

    return {
        "ready": bool(runtime_worker.running and notify_listener.running),
        "notify": notify_listener.running,
        "reconciler": runtime_worker.running,
    }
