"""Contract tests for the optional MonkeyCode compatibility layer.

These tests do not require a real database: they only assert that the module
imports cleanly when the optional deps are missing, and that the config
defaults keep the layer disabled.
"""
from __future__ import annotations

import importlib
import os

import pytest


def _isolate_from_real_env(monkeypatch):
    """Strip any real .env-supplied variables so these default-value contract
    tests see only the builtin defaults unless the test sets a variable."""
    for key in (
        "AI_LUBRICANT_COMPAT_ENABLED",
        "AI_LUBRICANT_USER_ADAPTER_ENABLED",
        "AI_LUBRICANT_SYSTEM_USER_ID",
        "AI_LUBRICANT_SYSTEM_USER_NAME",
        "AI_LUBRICANT_SYSTEM_USER_EMAIL",
        "AI_LUBRICANT_DATABASE_URL",
        "MONKEYCODE_COMPAT_ENABLED",
        "MONKEYCODE_USER_ADAPTER_ENABLED",
        "MONKEYCODE_SYSTEM_USER_ID",
        "MONKEYCODE_SYSTEM_USER_NAME",
        "MONKEYCODE_SYSTEM_USER_EMAIL",
        "MONKEYCODE_DATABASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)


def test_compat_disabled_by_default(monkeypatch):
    """Without env flags the layer must be disabled and init must be a no-op."""
    _isolate_from_real_env(monkeypatch)
    for key in (
        "AI_LUBRICANT_COMPAT_ENABLED",
        "AI_LUBRICANT_USER_ADAPTER_ENABLED",
    ):
        monkeypatch.delenv(key, raising=False)
    import monkeycode_compat.config as cfg

    importlib.reload(cfg)
    assert cfg.settings.enabled is False
    assert cfg.settings.user_adapter_enabled is False


def test_system_user_id_is_deterministic(monkeypatch):
    _isolate_from_real_env(monkeypatch)
    monkeypatch.delenv("AI_LUBRICANT_SYSTEM_USER_ID", raising=False)
    import monkeycode_compat.config as cfg

    importlib.reload(cfg)
    assert cfg.settings.system_user_id == "00000000-0000-0000-0000-000000000001"


def test_legacy_env_names_still_enable_compat(monkeypatch):
    """旧部署只设 MONKEYCODE_* 时兼容层照常工作（迁移期回退）。"""
    _isolate_from_real_env(monkeypatch)
    monkeypatch.setenv("MONKEYCODE_COMPAT_ENABLED", "true")
    monkeypatch.setenv("MONKEYCODE_SYSTEM_USER_EMAIL", "ops@example.com")
    import monkeycode_compat.config as cfg

    importlib.reload(cfg)
    assert cfg.settings.enabled is True
    assert cfg.settings.system_user_email == "ops@example.com"


def test_conflicting_new_and_legacy_env_raises(monkeypatch):
    """新旧名都设但值不同：必须报错，防止两个进程各读一个库。"""
    _isolate_from_real_env(monkeypatch)
    monkeypatch.setenv("AI_LUBRICANT_COMPAT_ENABLED", "false")
    monkeypatch.setenv("MONKEYCODE_COMPAT_ENABLED", "true")
    import monkeycode_compat.config as cfg

    with pytest.raises(RuntimeError, match="MONKEYCODE_COMPAT_ENABLED"):
        importlib.reload(cfg)
