"""主配置管理接口的安全 patch 契约。"""
import asyncio
import os
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException


_PROJ = Path(__file__).resolve().parents[1]
if str(_PROJ) not in sys.path:
    sys.path.insert(0, str(_PROJ))

import admin
import config


def test_allow_token_reservation_overflow_defaults_on(monkeypatch):
    monkeypatch.setattr(config.Config, "_load", classmethod(lambda cls: {}))
    assert config.Config.allow_token_reservation_overflow() is True


def test_allow_token_reservation_overflow_explicit_false(monkeypatch):
    monkeypatch.setattr(
        config.Config,
        "_load",
        classmethod(lambda cls: {"rate_limit": {"allow_token_reservation_overflow": False}}),
    )
    assert config.Config.allow_token_reservation_overflow() is False


def test_main_config_patch_allows_token_reservation_overflow():
    merged = admin._merge_main_config_patch(
        {"rate_limit": {"status_codes": [429], "allow_token_reservation_overflow": True}},
        {"rate_limit": {"allow_token_reservation_overflow": False}},
    )
    assert merged["rate_limit"]["status_codes"] == [429]
    assert merged["rate_limit"]["allow_token_reservation_overflow"] is False


def test_main_config_patch_preserves_unrelated_and_nested_siblings():
    current = {
        "postgres": {"password": "secret"},
        "redis": {"host": "redis"},
        "security": {"masking_enabled": True},
        "retry": {
            "max_retries": 3,
            "non_retryable_parameter_errors": {"status_codes": [413], "codes": ["old"]},
        },
    }
    patch = {"retry": {"max_retries": 5}}

    merged = admin._merge_main_config_patch(current, patch)

    assert merged["postgres"] == {"password": "secret"}
    assert merged["redis"] == {"host": "redis"}
    assert merged["security"] == {"masking_enabled": True}
    assert merged["retry"]["max_retries"] == 5
    assert merged["retry"]["non_retryable_parameter_errors"] == {"status_codes": [413], "codes": ["old"]}


def test_main_config_patch_rejects_bootstrap_fields():
    with pytest.raises(HTTPException, match="postgres"):
        admin._merge_main_config_patch({}, {"postgres": {"host": "other"}})


def test_main_config_routes_do_not_expose_bootstrap_mutators():
    routes = {route.path for route in admin.router.routes}
    assert "/admin/config/main/postgres" not in routes
    assert "/admin/config/main/redis" not in routes


def test_update_main_config_writes_merged_document(monkeypatch):
    current = {"postgres": {"password": "secret"}, "retry": {"max_retries": 3}}
    saved = []

    async def no_auth(_token):
        return None

    async def read_main(_path):
        return current

    async def write_main(_path, data):
        saved.append(data)

    monkeypatch.setattr(admin, "_require_admin", no_auth)
    monkeypatch.setattr(admin, "_read_json_async", read_main)
    monkeypatch.setattr(admin, "_write_json_async", write_main)

    response = asyncio.run(admin.update_main_config({"retry": {"max_retries": 7}}, token="Bearer test"))

    assert saved == [{"postgres": {"password": "secret"}, "retry": {"max_retries": 7}}]
    assert response == {"retry": {"max_retries": 7}}
