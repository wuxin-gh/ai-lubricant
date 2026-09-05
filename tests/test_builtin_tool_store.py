"""builtin_tool_store 纯函数契约（零 IO）——见 docs 设计方案。

只锁定不连库的纯函数：
- _token_is_valid：status + 过期判定（鉴权入口 get_instance_by_token 的核心）。
- token_row_to_dict：永不外泄 token_hash，暴露 hint / 状态。
- _resource_value：data/secret 分列 + _secrets 遮蔽（明文只在 include_secrets 时带出）。
- _split_detail_value：secret 字段拆分、传空保留原 secret。
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import builtin_tool_store as store  # noqa: E402


class _Row(dict):
    """dict 直接当 asyncpg Record 用（本层只按键取值）。"""


# ── _token_is_valid ──────────────────────────────────────────────────────────

def test_token_valid_active_no_expiry():
    row = _Row(status="active", expires_at=None)
    assert store._token_is_valid(row) is True


def test_token_invalid_when_none():
    assert store._token_is_valid(None) is False


def test_token_invalid_when_disabled():
    row = _Row(status="disabled", expires_at=None)
    assert store._token_is_valid(row) is False


def test_token_valid_before_expiry():
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    assert store._token_is_valid(_Row(status="active", expires_at=future)) is True


def test_token_invalid_after_expiry():
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert store._token_is_valid(_Row(status="active", expires_at=past)) is False


def test_token_expiry_naive_datetime_treated_as_utc():
    # 无 tzinfo 的过期时间应被当作 UTC，不抛异常。
    past_naive = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(tzinfo=None)
    assert store._token_is_valid(_Row(status="active", expires_at=past_naive)) is False


# ── token_row_to_dict ────────────────────────────────────────────────────────

def test_token_row_hides_hash():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = _Row(
        id=5, instance_id=2, service_id=None, token_hash="deadbeef", token_hint="cdp_abcd...wxyz",
        target_type="agent", target_id="42", display_token=False, status="active",
        expires_at=None, created_at=ts, updated_at=ts,
    )
    d = store.token_row_to_dict(row)
    assert "token_hash" not in d
    assert d["token_hint"] == "cdp_abcd...wxyz"
    assert d["target_type"] == "agent"
    assert d["instance_id"] == 2
    assert d["created_at"] == ts.isoformat()


def test_token_row_external_service_target():
    # 外部 MCP token：service_id 非空、instance_id 空，序列化保留两者。
    row = _Row(
        id=7, instance_id=None, service_id=3, token_hash="cafe", token_hint="cdp_1234...abcd",
        target_type="external", target_id="partner-x", display_token=True, status="active",
        expires_at=None, created_at=None, updated_at=None,
    )
    d = store.token_row_to_dict(row)
    assert "token_hash" not in d
    assert d["service_id"] == 3
    assert d["instance_id"] is None
    assert d["display_token"] is True


def test_token_row_preserves_admin_join_columns():
    # list_all_tokens 靠 token_row_to_dict 的 dict(row) 捎带 JOIN 出的名称快照，
    # 管理端全量视图直接渲染无需二次查询。这些列必须原样透传（且仍不含 hash）。
    row = _Row(
        id=9, instance_id=4, service_id=None, token_hash="beef", token_hint="cdp_aaaa...zzzz",
        target_type="agent", target_id=None, display_token=False, status="active",
        expires_at=None, created_at=None, updated_at=None,
        instance_name="我的浏览器", instance_tool_kind="cdp",
        service_name=None, service_display_name=None,
    )
    d = store.token_row_to_dict(row)
    assert "token_hash" not in d
    assert d["instance_name"] == "我的浏览器"
    assert d["instance_tool_kind"] == "cdp"
    assert d["service_name"] is None


# ── _resource_value：遮蔽 / 揭示 ──────────────────────────────────────────

def test_detail_masks_secrets_by_default():
    row = _Row(
        id=1, owner_user_id="owner", resource_type="mail_account", revision=1,
        data={"username": "bob", "base_url": "imap.x.com"},
        secret_data={"password": "s3cr3t", "secret_key": ""},
        created_at=None, updated_at=None,
    )
    d = store._resource_value(row)
    assert d["username"] == "bob"
    assert "password" not in d  # 明文不外泄
    assert d["_secrets"] == {"password": {"state": "set"}, "secret_key": {"state": "unset"}}


def test_detail_reveals_secrets_when_requested():
    row = _Row(
        id=1, owner_user_id="owner", resource_type="mail_account", revision=1,
        data={"username": "bob"}, secret_data={"password": "s3cr3t"},
        created_at=None, updated_at=None,
    )
    d = store._resource_value(row, include_secrets=True)
    assert d["password"] == "s3cr3t"


# ── _split_detail_value ──────────────────────────────────────────────────────

def test_split_detail_puts_secret_fields_in_secret_data():
    data, secret = store._split_detail_value(
        "mail_account", {"username": "bob", "password": "pw", "secret_key": "sk"}
    )
    assert data == {"username": "bob"}
    assert secret == {"password": "pw", "secret_key": "sk"}


def test_split_detail_empty_secret_keeps_existing():
    data, secret = store._split_detail_value(
        "mail_account", {"username": "bob", "password": ""}, existing_secrets={"password": "old"}
    )
    assert data == {"username": "bob"}
    assert secret == {"password": "old"}  # 传空保留原 secret


def test_split_detail_ignores_meta_keys():
    data, secret = store._split_detail_value(
        "cdp_client", {"name": "browser#3", "id": 9, "revision": 2, "_secrets": {}}
    )
    assert data == {"name": "browser#3"}
    assert secret == {}


def test_split_detail_cdp_has_no_secret_fields():
    data, secret = store._split_detail_value("cdp_client", {"name": "b", "client_id": "c1"})
    assert data == {"name": "b", "client_id": "c1"}
    assert secret == {}


# ── mail_query_for_resource：账户/别名解析 + received/requested 回填 ──────────────
#
# 用户侧「获取/搜索邮件」的核心逻辑：取本实例账户（含 secret）→ 解析地址（别名替换成
# source_address）→ 调 MailClient.list_mail → 回填 received_address/requested_address。
# 用 monkeypatch 替掉 list_resources（喂 mock 账户 + 别名）与 MailClient（拦截 list_mail
# 的入参、返回假邮件），断言地址解析与回填口径与邮件插件一致，且不触库不发网络。


def _run(coro):
    return asyncio.run(coro)


class _FakeMailClient:
    """记录构造参数与 list_mail 入参，返回预置邮件（不发网络）。"""

    last_init: dict = {}
    last_call: dict = {}
    to_return: list = []

    def __init__(self, username, password, base_url, secret_key):
        _FakeMailClient.last_init = {
            "username": username, "password": password,
            "base_url": base_url, "secret_key": secret_key,
        }

    async def list_mail(self, _session, *, limit, offset, address, keyword):
        _FakeMailClient.last_call = {
            "limit": limit, "offset": offset, "address": address, "keyword": keyword,
        }
        return [dict(m) for m in _FakeMailClient.to_return]


def _patch_mail_deps(monkeypatch, details):
    """替掉 list_resources（返回给定明细）与 MailClient（假客户端）。"""
    import mcp_builtin.mail.client as mail_client_mod

    async def fake_list_resources(resource_type=None, owner_user_id=None, *, include_secrets=False):
        rows = list(details)
        if resource_type is not None:
            rows = [d for d in rows if d["resource_type"] == resource_type]
        return rows

    async def fake_get_resource(resource_id, **kwargs):
        return next((d for d in details if d.get("id") == resource_id and d.get("resource_type") == "mail_account"), None)
    monkeypatch.setattr(store, "list_resources", fake_list_resources)
    monkeypatch.setattr(store, "get_resource", fake_get_resource)
    monkeypatch.setattr(mail_client_mod, "MailClient", _FakeMailClient)


def test_mail_query_raises_when_no_account(monkeypatch):
    _patch_mail_deps(monkeypatch, [])
    import pytest as _pytest
    with _pytest.raises(ValueError, match="邮箱服务不存在"):
        _run(store.mail_query_for_resource(1))


def test_mail_query_uses_account_secrets_and_no_address_filter(monkeypatch):
    details = [
        {"resource_type": "mail_account", "id": 10, "owner_user_id": "owner",
         "username": "bob@x.com", "password": "pw", "base_url": "https://mail.x.com",
         "secret_key": "sk"},
    ]
    _patch_mail_deps(monkeypatch, details)
    _FakeMailClient.to_return = [{"subject": "hi"}]
    out = _run(store.mail_query_for_resource(10, keyword="code", limit=5, offset=0))
    # 账户 secret 真值透传给 MailClient。
    assert _FakeMailClient.last_init["username"] == "bob@x.com"
    assert _FakeMailClient.last_init["password"] == "pw"
    assert _FakeMailClient.last_init["secret_key"] == "sk"
    # address 为空 → 不带地址过滤（None）；keyword 透传。
    assert _FakeMailClient.last_call["address"] is None
    assert _FakeMailClient.last_call["keyword"] == "code"
    assert out["count"] == 1


def test_mail_query_alias_replaced_with_source_address(monkeypatch):
    details = [
        {"resource_type": "mail_account", "id": 10, "owner_user_id": "owner",
         "username": "bob@x.com", "password": "pw", "base_url": "https://mail.x.com",
         "secret_key": ""},
        {"resource_type": "mail_address", "id": 20, "owner_user_id": "owner",
         "address": "alias@x.com", "source_address": "real@x.com"},
    ]
    _patch_mail_deps(monkeypatch, details)
    _FakeMailClient.to_return = [{"subject": "hi"}]  # 无 received_address → 应回填
    out = _run(store.mail_query_for_resource(10, address="Alias@X.com"))
    # 别名 → 上游按 source_address 查询。
    assert _FakeMailClient.last_call["address"] == "real@x.com"
    # requested = 用户输入（归一小写）；received 回填成 source_address。
    assert out["requested_address"] == "alias@x.com"
    assert out["received_address"] == "real@x.com"
    assert out["messages"][0]["received_address"] == "real@x.com"
    assert out["messages"][0]["requested_address"] == "alias@x.com"


def test_mail_query_unmapped_address_queried_as_is(monkeypatch):
    details = [
        {"resource_type": "mail_account", "id": 10, "owner_user_id": "owner",
         "username": "bob@x.com", "password": "pw", "base_url": "https://mail.x.com",
         "secret_key": ""},
    ]
    _patch_mail_deps(monkeypatch, details)
    _FakeMailClient.to_return = []
    out = _run(store.mail_query_for_resource(10, address="main@x.com"))
    # 未命中任何别名 → 按原地址（归一小写）查询，不报错。
    assert _FakeMailClient.last_call["address"] == "main@x.com"
    assert out["requested_address"] == "main@x.com"


# ── 外部 MCP token 目标解析：CDP 必须绑本实例已启用的客户端 ─────────────────────
#
# resolve_external_cdp_client / resolve_mail_resource_id 都先 resolve_token 拿到
# (kind, target=instance, token行)，再对 CDP 复核 target_id 指向的 cdp_client 明细。
# 用 monkeypatch 替掉 resolve_token / get_detail（不触库），只锁「谁能过、谁被拒」。


def _patch_token(monkeypatch, *, resolved, detail=None):
    async def fake_resolve_token(_token):
        return resolved

    async def fake_get_resource(_detail_id, **_kw):
        return detail

    monkeypatch.setattr(store, "resolve_token", fake_resolve_token)
    monkeypatch.setattr(store, "get_resource", fake_get_resource)


def test_external_cdp_resolves_bound_client(monkeypatch):
    resolved = {
        "kind": "resource",
        "target": {"id": 42, "resource_type": "cdp_client", "enabled": True, "token_hash": "abc"},
        "token": {"target_type": "external", "target_id": "42"},
    }
    detail = {"detail_type": "cdp_client", "id": 42, "owner_user_id": "owner",
              "enabled": True, "token_hash": "abc"}
    _patch_token(monkeypatch, resolved=resolved, detail=detail)
    assert _run(store.resolve_external_cdp_client("tok")) == "42"


def test_external_cdp_rejects_disabled_or_tokenless_client(monkeypatch):
    resolved = {
        "kind": "resource",
        "target": {"id": 42, "resource_type": "cdp_client", "enabled": True, "token_hash": "abc"},
        "token": {"target_type": "external", "target_id": "42"},
    }
    resolved["target"]["enabled"] = False
    _patch_token(monkeypatch, resolved=resolved)
    assert _run(store.resolve_external_cdp_client("tok")) is None


def test_external_cdp_rejects_non_external_token(monkeypatch):
    # agent/node/user 等自用 token 不当外部 CDP 目标用。
    resolved = {
        "kind": "resource",
        "target": {"id": 42, "resource_type": "cdp_client", "enabled": True, "token_hash": "abc"},
        "token": {"target_type": "agent", "target_id": "42"},
    }
    _patch_token(monkeypatch, resolved=resolved, detail=None)
    assert _run(store.resolve_external_cdp_client("tok")) is None


def test_mail_resource_resolves_only_mail_kind(monkeypatch):
    resolved = {
        "kind": "resource",
        "target": {"id": 7, "resource_type": "mail_account", "enabled": True},
        "token": {"target_type": "external", "target_id": None},
    }
    _patch_token(monkeypatch, resolved=resolved)
    assert _run(store.resolve_mail_resource_id("tok")) == 7

    cdp_resolved = {"kind": "resource", "target": {"id": 7, "resource_type": "cdp_client", "enabled": True, "token_hash": "abc"},
                    "token": {"target_type": "external"}}
    _patch_token(monkeypatch, resolved=cdp_resolved)
    assert _run(store.resolve_mail_resource_id("tok")) is None
