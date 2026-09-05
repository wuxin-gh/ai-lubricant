"""_set_account_disabled：自定义渠道启用即置已认证单测。

覆盖修复：自定义渠道（贴码/jiekou/自定义等，均继承 CustomProvider）走 api_key 直连，
无 OAuth 体检需求；账号从禁用改启用时直接把 auth_ok 置 True，避免前端久显「待检查」。
非自定义渠道与禁用方向不受影响。
"""
from __future__ import annotations

from providers import CustomProvider
import admin


class _CustomProviderStub(CustomProvider):
    """贴码/jiekou/自定义渠道运行时实例都是 CustomProvider 子类。"""

    def __init__(self):
        self.username = "u1"
        self._channel = None


class _NonCustomProviderStub:
    """非 CustomProvider 渠道 provider（内置渠道）的极简替身。

    _set_account_disabled 只 isinstance(..., CustomProvider) 判断，不需要 BaseProvider
    的全部抽象方法；用一个普通对象即可验证非自定义渠道不被强制置已认证。
    """

    def __init__(self):
        self.username = "u1"
        self._channel = None


class _AccountClientStub:
    def __init__(self, provider):
        self.provider = provider
        self.username = "u1"
        self.disabled = True
        self.disable_reason = "switch_off"
        self.auth_ok = None
        self.auth_error = ""


class _PoolStub:
    def __init__(self, client):
        self.clients = [client]


def _install_pool(monkeypatch, pool):
    monkeypatch.setattr(
        admin.ModelClientPool,
        "get_provider_pool",
        classmethod(lambda cls, name: pool),
    )


def test_enable_custom_account_marks_authenticated(monkeypatch):
    client = _AccountClientStub(_CustomProviderStub())
    _install_pool(monkeypatch, _PoolStub(client))

    assert admin._set_account_disabled("p1", "u1", disabled=False) is True

    assert client.disabled is False
    assert client.disable_reason == ""
    # 自定义渠道启用即认证：auth_ok 从 None 直接置 True。
    assert client.auth_ok is True
    assert client.auth_error == ""


def test_enable_builtin_account_not_touched(monkeypatch):
    """内置渠道（非 CustomProvider）不强制置已认证，仍走原有体检路径。"""
    client = _AccountClientStub(_NonCustomProviderStub())
    _install_pool(monkeypatch, _PoolStub(client))

    assert admin._set_account_disabled("p1", "u1", disabled=False) is True

    assert client.disabled is False
    assert client.auth_ok is None  # 保留 None，交给后台 init_all/check_account 体检


def test_disable_does_not_set_authenticated(monkeypatch):
    """禁用方向不应触碰认证状态。"""
    client = _AccountClientStub(_CustomProviderStub())
    client.auth_ok = False
    client.auth_error = "上次体检失败"
    _install_pool(monkeypatch, _PoolStub(client))

    assert admin._set_account_disabled("p1", "u1", disabled=True) is True

    assert client.disabled is True
    assert client.disable_reason == "switch_off"
    # 禁用保留原 auth_ok/auth_error，不被清成已认证。
    assert client.auth_ok is False
    assert client.auth_error == "上次体检失败"


def test_missing_pool_returns_false(monkeypatch):
    monkeypatch.setattr(
        admin.ModelClientPool,
        "get_provider_pool",
        classmethod(lambda cls, name: None),
    )
    assert admin._set_account_disabled("p1", "u1", disabled=False) is False


def test_missing_account_returns_false(monkeypatch):
    client = _AccountClientStub(_CustomProviderStub())
    client.username = "other"
    _install_pool(monkeypatch, _PoolStub(client))
    assert admin._set_account_disabled("p1", "u1", disabled=False) is False
