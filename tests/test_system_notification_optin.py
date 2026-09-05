"""账号/系统类通知的 opt-in 行为回归测试。

历史上「账号初始化失败 / 账号退登 / 日志清理完成 / 日志清理失败」是直接
``PostgresClient.upsert_notification`` 写 ``notifications`` 表的——绕过了事件
订阅配置，导致没配事件也冒通知。这些入口已迁到 ``emit_notification``，是否进
通知中心 / 推 webhook 由订阅配置决定。本测试锁定这一行为：产生侧只发事件，
绝不直接写 ``notifications`` 表。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from monkeycode_compat import notify_core, notify_service
import rate_limiter
from rate_limiter import AccountClient, ModelClientPool, ProviderPool


class FakeProvider:
    """最小 provider：可控 init_auth / health_check 返回值，不触达真实认证。"""

    PROVIDER_NAME = "custom"
    SUPPORTS_TOKEN_AUTO_REFRESH = False

    def __init__(self, username: str, *, init_raises: bool = False, auth_ok: bool = False):
        self.username = username
        self.quotas = None
        self.invitation_interval = 0
        self._init_raises = init_raises
        self._auth_ok = auth_ok

    async def init_auth(self, refresh: bool = True):
        if self._init_raises:
            raise RuntimeError("init boom")
        return self._auth_ok

    def is_init(self) -> bool:
        return True

    async def health_check(self):
        return self._auth_ok


def _make_pool(provider: FakeProvider) -> ProviderPool:
    pool = ProviderPool("custom", object)
    pool.clients = [AccountClient(provider, rpm_limit=0)]
    return pool


def _capture(monkeypatch) -> tuple[list[dict], list[dict]]:
    """截获 emit_notification / emit_notification_background，并盯住 upsert_notification。

    返回 (emits, upserts)。upserts 非空说明产生侧又绕过订阅直接写铃铛——回归。
    """
    emits: list[dict] = []
    bg_emits: list[dict] = []
    upserts: list[dict] = []

    async def _fake_emit(event_type: str, **kwargs):
        emits.append({"event_type": event_type, **kwargs})

    def _fake_bg(event_type: str, **kwargs):
        bg_emits.append({"event_type": event_type, **kwargs})

    async def _fake_upsert(data: dict):
        upserts.append(data)
        return None

    monkeypatch.setattr(notify_core, "emit_notification", _fake_emit)
    monkeypatch.setattr(notify_core, "emit_notification_background", _fake_bg)
    monkeypatch.setattr(rate_limiter.PostgresClient, "upsert_notification", _fake_upsert)
    return emits, upserts


# ---- 事件目录必须立项，否则用户在通知中心根本配不了这些事件 ----

def test_catalogue_carries_the_new_system_event_types():
    all_events = {e["type"] for e in notify_service.NOTIFY_EVENT_TYPES}
    for t in ("account.init_failed", "account.logged_out",
              "log.cleanup_done", "log.cleanup_failed"):
        assert t in all_events, f"事件类型 {t} 不在目录里，无法配置订阅"


# ---- 账号初始化失败：发 account.init_failed 事件，不直接写 notifications ----

@pytest.mark.asyncio
async def test_init_failure_emits_event_without_direct_bell_write(monkeypatch):
    pool = _make_pool(FakeProvider("acct1", init_raises=True))
    emits, upserts = _capture(monkeypatch)

    await pool.init_all()

    assert len(emits) == 0, "init 失败走 background 发射，不该有 await 发射"
    assert len(upserts) == 0, "产生侧禁止直接 upsert_notification"
    assert pool.clients[0].auth_ok is False
    assert "初始化失败" in pool.clients[0].auth_error


@pytest.mark.asyncio
async def test_init_failure_calls_emit_notification_background(monkeypatch):
    pool = _make_pool(FakeProvider("acct1", init_raises=True))
    bg_calls: list[dict] = []

    def _bg(event_type: str, **kwargs):
        bg_calls.append({"event_type": event_type, **kwargs})

    async def _noop_emit(*a, **k):
        return None

    async def _no_upsert(data):
        return None

    monkeypatch.setattr(notify_core, "emit_notification_background", _bg)
    monkeypatch.setattr(notify_core, "emit_notification", _noop_emit)
    monkeypatch.setattr(rate_limiter.PostgresClient, "upsert_notification", _no_upsert)

    await pool.init_all()

    assert len(bg_calls) == 1
    call = bg_calls[0]
    assert call["event_type"] == "account.init_failed"
    assert call["params"]["provider_name"] == "custom"
    assert call["params"]["account_username"] == "acct1"
    assert call["severity"] == "error"
    assert call["source"] == "account_init"


# ---- 账号退登：发 account.logged_out 事件，不直接写 notifications ----

@pytest.mark.asyncio
async def test_logged_out_accounts_emit_event(monkeypatch):
    pool = _make_pool(FakeProvider("acct1", auth_ok=False))
    emits, upserts = _capture(monkeypatch)
    monkeypatch.setattr(rate_limiter.random, "uniform", lambda *a: 0)

    await pool.check_account(force=True)

    assert len(upserts) == 0, "产生侧禁止直接 upsert_notification"
    assert len(emits) == 1
    call = emits[0]
    assert call["event_type"] == "account.logged_out"
    assert call["params"]["provider_name"] == "custom"
    assert call["params"]["account_usernames"] == ["acct1"]
    assert call["severity"] == "warn"
    assert call["source"] == "account_check"


# ---- 日志清理：完成发 log.cleanup_done，失败发 log.cleanup_failed ----

@pytest.mark.asyncio
async def test_log_cleanup_success_emits_done_event(monkeypatch):
    emits, upserts = _capture(monkeypatch)
    monkeypatch.setattr(rate_limiter.config.Config, "keep_response_hours", lambda: 24)
    monkeypatch.setattr(rate_limiter.config.Config, "get_log_retention_days", lambda: 7)
    monkeypatch.setattr(rate_limiter.config.Config, "get_log_retention_max_entries", lambda: 0)

    async def _no_aggregate():
        return None

    monkeypatch.setattr(ModelClientPool, "_aggregate_hourly_stats", _no_aggregate)

    async def _cleanup_payload(_days, _max_entries=0):
        return ""

    monkeypatch.setattr(ModelClientPool, "_cleanup_payload_store", _cleanup_payload)

    async def _cleanup_logs(*a, **k):
        return 5

    monkeypatch.setattr(rate_limiter.PostgresClient, "cleanup_request_logs", _cleanup_logs)

    archived = await ModelClientPool.clean_response_data("daily")

    assert archived == 5
    assert len(upserts) == 0, "产生侧禁止直接 upsert_notification"
    assert len(emits) == 1
    call = emits[0]
    assert call["event_type"] == "log.cleanup_done"
    assert call["severity"] == "info"
    assert call["source"] == "log_cleanup"
    assert call["dedupe_key"] == "log_cleanup:daily"


@pytest.mark.asyncio
async def test_manual_log_cleanup_failure_emits_failed_event(monkeypatch):
    emits, upserts = _capture(monkeypatch)
    monkeypatch.setattr(rate_limiter.config.Config, "keep_response_hours", lambda: 24)
    monkeypatch.setattr(rate_limiter.config.Config, "get_log_retention_days", lambda: 7)
    monkeypatch.setattr(rate_limiter.config.Config, "get_log_retention_max_entries", lambda: 0)

    async def _no_aggregate():
        return None

    monkeypatch.setattr(ModelClientPool, "_aggregate_hourly_stats", _no_aggregate)

    async def _cleanup_payload(_days, _max_entries=0):
        return ""

    monkeypatch.setattr(ModelClientPool, "_cleanup_payload_store", _cleanup_payload)

    async def _cleanup_logs(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(rate_limiter.PostgresClient, "cleanup_request_logs", _cleanup_logs)

    archived = await ModelClientPool.clean_response_data("manual")

    assert archived == 0
    assert len(upserts) == 0, "产生侧禁止直接 upsert_notification"
    failed = [c for c in emits if c["event_type"] == "log.cleanup_failed"]
    assert len(failed) == 1
    assert failed[0]["severity"] == "error"
    assert failed[0]["source"] == "log_cleanup"
