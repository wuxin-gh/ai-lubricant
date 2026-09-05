"""Idempotent system user bootstrap.

Creates exactly one deterministic system user used to anchor legacy/global
resources. Multiple startups must converge to the same row, never duplicate.
"""
from __future__ import annotations

import uuid

from loguru import logger

from .config import settings
from .models import SystemUser


async def ensure_system_user() -> SystemUser:
    """Idempotently upsert the deterministic system user."""
    user_id = uuid.UUID(settings.system_user_id)
    user, created = await SystemUser.get_or_create(
        id=user_id,
        defaults={
            "name": settings.system_user_name,
            "email": settings.system_user_email,
            "role": "system_admin",
            "status": "active",
        },
    )
    if created:
        logger.info("[monkeycode-compat] system user created id={}", user_id)
    return user


async def seed_bootstrap_admin() -> None:
    """Idempotently create the first login-able C-side platform admin.

    Driven by ``[ai_lubricant] bootstrap_admin_email/password`` (env overrides
    ``AI_LUBRICANT_BOOTSTRAP_ADMIN_*``（旧名 MONKEYCODE_BOOTSTRAP_ADMIN_* 迁移期仍识别）). Empty email disables seeding — the layer
    then starts with an empty ``mc_users`` table. Existing email is left
    untouched (never resets a password on restart). This exists so the platform
    has a working login out of the box for verification without a separate
    registration round-trip.
    """
    email = (settings.bootstrap_admin_email or "").strip().lower()
    password = settings.bootstrap_admin_password or ""
    if not email or not password:
        return
    from .auth_service import auth_service

    from .models import User

    existing = await User.filter(email=email, is_deleted=False).first()
    if existing is not None:
        return
    try:
        await auth_service.create_user(
            email=email,
            password=password,
            name=settings.bootstrap_admin_name or None,
            role="admin",
            status="active",
        )
        logger.info("[monkeycode-compat] bootstrap admin created email={}", email)
    except ValueError as exc:
        logger.warning("[monkeycode-compat] bootstrap admin skipped: {}", exc)
