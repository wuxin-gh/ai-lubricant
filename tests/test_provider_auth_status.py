"""渠道徽章认证口径与自动刷新分类的单元测试。

覆盖三件事：
1. `_provider_summary_response` 的 auth 计数改用真实 `auth_ok`（而非 is_init），
   并新增 auth_failed / checking / supports_token_auto_refresh 字段；
2. provider 的 SUPPORTS_TOKEN_AUTO_REFRESH 标记正确（自动 vs 人工）；
3. ProviderPool.check_account 对自动刷新渠道走 init_auth（可刷新），
   对人工凭据渠道只走 health_check（不隐藏登录）。
"""
from __future__ import annotations

import asyncio

import admin
import rate_limiter
from providers.base import BaseProvider
from providers.cloudflare import CloudflareProvider
from providers.custom import CustomProvider
from rate_limiter import AccountClient, ProviderPool


def _make_client(provider: BaseProvider, auth_ok, disabled=False) -> AccountClient:
    client = AccountClient(provider, rpm_limit=0, disabled=disabled)
    client.auth_ok = auth_ok
    return client


def test_summary_auth_counts_use_real_auth_ok_not_is_init(monkeypatch):
    """auth_account_count 应统计 auth_ok is True，而非 is_init()。
    人工凭据渠道 API Key 仍存在（is_init=True）但 auth_ok=False（已过期）时，
    徽章不能再误报为"正常"。"""
    # 人工凭据账号：API Key + base_url 存在 → is_init()=True，但体检失败 auth_ok=False
    manual = CustomProvider(username="u1", password="dead-token", base_url="https://api.example.com")
    assert manual.is_init() is True  # 凭据存在
    client = _make_client(manual, auth_ok=False)

    pool = ProviderPool("manual", CustomProvider)
    pool.clients = [client]

    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)

    summary = admin._provider_summary_response(
        "manual",
        {"enabled": True, "base_url": "https://api.example.com", "accounts": [{"username": "u1", "password": "dead-token"}]},
    )

    assert summary["auth_account_count"] == 0          # 真实认证通过数
    assert summary["auth_failed_account_count"] == 1   # 过期账号
    assert summary["checking_account_count"] == 0
    assert summary["supports_token_auto_refresh"] is False  # 自定义人工
    # 关键：不再因为 is_init()=True 就把过期账号计入 auth_account_count
    assert summary["auth_account_count"] != 1


def test_summary_exposes_auto_refresh_flag_for_auto_provider(monkeypatch):
    """可自动重新登录的渠道，supports_token_auto_refresh 应为 True。"""
    class AutoRefreshProvider(CustomProvider):
        PROVIDER_NAME = "auto"
        SUPPORTS_TOKEN_AUTO_REFRESH = True

    auto = AutoRefreshProvider(username="u", password="p")
    client = _make_client(auto, auth_ok=True)
    pool = ProviderPool("auto", AutoRefreshProvider)
    pool.clients = [client]
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)

    summary = admin._provider_summary_response("auto", {"enabled": True, "accounts": [{"username": "u"}]})

    assert summary["supports_token_auto_refresh"] is True
    assert summary["auth_account_count"] == 1


def test_summary_checking_state_when_auth_ok_none(monkeypatch):
    """刚启动尚未体检（auth_ok=None）：不算过期，计入 checking。"""
    manual = CustomProvider(username="u", password="t", base_url="https://api.example.com")
    client = _make_client(manual, auth_ok=None)
    pool = ProviderPool("manual", CustomProvider)
    pool.clients = [client]
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)

    summary = admin._provider_summary_response("manual", {"enabled": True, "accounts": [{"username": "u"}]})

    assert summary["auth_account_count"] == 0
    assert summary["auth_failed_account_count"] == 0
    assert summary["checking_account_count"] == 1


def test_summary_does_not_count_disabled_accounts(monkeypatch):
    """switch off 的禁用账号不污染徽章认证计数。"""
    manual = CustomProvider(username="u", password="t", base_url="https://api.example.com")
    ok_client = _make_client(manual, auth_ok=True)
    disabled_client = _make_client(manual, auth_ok=False, disabled=True)
    pool = ProviderPool("manual", CustomProvider)
    pool.clients = [ok_client, disabled_client]
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)

    summary = admin._provider_summary_response(
        "manual",
        {"enabled": True, "accounts": [{"username": "a"}, {"username": "b", "switch": False}]},
    )

    assert summary["auth_account_count"] == 1
    assert summary["auth_failed_account_count"] == 0


def test_auto_refresh_classification_on_classes():
    """自动刷新 vs 人工凭据 的分类标记。

    CLI 逆向渠道（copilot/codebuddy/atomcode/eaichat）已下架为「代码渠道」spec，
    它们的 SUPPORTS_TOKEN_AUTO_REFRESH 由 spec 自己声明、在 loader 加载后生效，
    不再是这里静态枚举的类。这里只锁住剩余内置渠道的人工凭据口径。
    """

    class AutoRefreshProvider(CustomProvider):
        PROVIDER_NAME = "auto"
        SUPPORTS_TOKEN_AUTO_REFRESH = True

    assert AutoRefreshProvider.SUPPORTS_TOKEN_AUTO_REFRESH is True

    # 人工凭据渠道：custom / cloudflare
    for cls in [CustomProvider, CloudflareProvider]:
        assert cls.SUPPORTS_TOKEN_AUTO_REFRESH is False, f"{cls.__name__} 应为人工凭据"


def test_check_account_auto_refresh_calls_init_auth(monkeypatch):
    """可自愈渠道定时体检应走 init_auth(True)（触发 refresh_token 刷新），
    而非仅 health_check。"""
    calls: list[str] = []

    class AutoProvider(CustomProvider):
        PROVIDER_NAME = "auto-test"
        SUPPORTS_TOKEN_AUTO_REFRESH = True

        async def init_auth(self, is_check: bool = False) -> bool:
            calls.append("init_auth")
            return True

        async def health_check(self) -> bool:
            calls.append("health_check")
            return True

        async def check_auth(self) -> bool:
            calls.append("check_auth")
            return True

    provider = AutoProvider(username="u", password="")
    client = _make_client(provider, auth_ok=None)
    pool = ProviderPool("auto-test", AutoProvider)
    pool.clients = [client]

    asyncio.run(pool.check_account())

    assert "init_auth" in calls
    assert "health_check" not in calls
    assert client.auth_ok is True


def test_check_account_manual_provider_only_health_check(monkeypatch):
    """人工凭据渠道定时体检只走 health_check，不执行隐藏登录。"""
    calls: list[str] = []

    class ManualProvider(CustomProvider):
        PROVIDER_NAME = "manual-test"
        SUPPORTS_TOKEN_AUTO_REFRESH = False

        def is_init(self) -> bool:
            return True

        async def init_auth(self, is_check: bool = False) -> bool:
            calls.append("init_auth")
            return False

        async def health_check(self) -> bool:
            calls.append("health_check")
            return False

        async def check_auth(self) -> bool:
            calls.append("check_auth")
            return False

    provider = ManualProvider(username="u", password="")
    client = _make_client(provider, auth_ok=None)
    pool = ProviderPool("manual-test", ManualProvider)
    pool.clients = [client]

    asyncio.run(pool.check_account())

    assert "health_check" in calls
    assert "init_auth" not in calls
    assert client.auth_ok is False
    assert "需人工" in client.auth_error


def test_check_account_force_bypasses_channel_gates_but_skips_disabled_accounts(monkeypatch):
    """手动全量检查绕过渠道级门禁，但不检查 switch off 账号。"""
    calls: list[str] = []

    class ManualProvider(CustomProvider):
        PROVIDER_NAME = "manual-force-test"
        SUPPORTS_TOKEN_AUTO_REFRESH = False

        def is_init(self) -> bool:
            return True

        async def health_check(self) -> bool:
            calls.append(self.username)
            return True

    enabled_provider = ManualProvider(username="enabled", password="")
    disabled_provider = ManualProvider(username="disabled", password="")
    pool = ProviderPool("manual-force-test", ManualProvider)
    pool.clients = [
        _make_client(enabled_provider, auth_ok=None),
        _make_client(disabled_provider, auth_ok=None, disabled=True),
    ]
    pool.channel.update({"enabled": False, "health_check": {"enabled": False}})
    monkeypatch.setattr("rate_limiter.random.uniform", lambda *_: 0)

    asyncio.run(pool.check_account(force=True))

    assert calls == ["enabled"]
    assert pool.clients[0].auth_ok is True
    assert pool.clients[1].auth_ok is None


def test_check_account_default_respects_channel_gates(monkeypatch):
    """定时检查仍遵守禁用渠道和 health_check.enabled。"""
    calls: list[str] = []

    class ManualProvider(CustomProvider):
        PROVIDER_NAME = "manual-gated-test"
        SUPPORTS_TOKEN_AUTO_REFRESH = False

        def is_init(self) -> bool:
            return True

        async def health_check(self) -> bool:
            calls.append(self.username)
            return True

    provider = ManualProvider(username="u", password="")
    pool = ProviderPool("manual-gated-test", ManualProvider)
    pool.clients = [_make_client(provider, auth_ok=None)]
    pool.channel.update({"enabled": True, "health_check": {"enabled": False}})
    debug_messages = []
    monkeypatch.setattr(rate_limiter.logger, "debug", lambda message: debug_messages.append(message))

    asyncio.run(pool.check_account())

    assert calls == []
    assert pool.clients[0].auth_ok is None
    assert debug_messages == []


def test_clean_message_only_uses_enabled_authenticated_initialized_accounts():
    """历史清理不得请求禁用、认证失败、尚未检查或未初始化的账号。"""
    calls: list[str] = []

    class ClearProvider(CustomProvider):
        PROVIDER_NAME = "clear-gated-test"

        def __init__(self, username: str, *, initialized: bool = True):
            super().__init__(username=username, password="")
            self.initialized = initialized

        @property
        def invitation_interval(self):
            return 0

        def is_init(self) -> bool:
            return self.initialized

        async def clear_conversations(self, max_age_hours: int = 2):
            calls.append(f"{self.username}:{max_age_hours}")
            return 0

    pool = ProviderPool("clear-gated-test", ClearProvider)
    pool.clients = [
        _make_client(ClearProvider("ready"), auth_ok=True),
        _make_client(ClearProvider("auth-failed"), auth_ok=False),
        _make_client(ClearProvider("checking"), auth_ok=None),
        _make_client(ClearProvider("disabled"), auth_ok=True, disabled=True),
        _make_client(ClearProvider("not-initialized", initialized=False), auth_ok=True),
    ]

    asyncio.run(pool.clean_message(6))

    assert calls == ["ready:6"]
