"""Apple ID 免费签名引擎/路由测试（纯函数 + monkeypatch 引擎，无 PG 无网络）。

覆盖：team 域作用域 bundle id、缓存有效性判定、物化→p12 形状转换、_enrich_profile
apple_id 分支、2FA pending TTL/用后即焚、login 端点契约（假引擎分流 + 密码不落响应）。
带 PG 池的路由不在此测（参照 tests/test_ios_wda_dispatch.py 的纯函数约定）。
"""
from __future__ import annotations

import asyncio
import base64
import plistlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from user_platform.apple_signing import provision, signing
from user_platform.routes_ios import (
    _APPLE_2FA_PENDING,
    _enrich_profile,
    _prune_apple_2fa_pending,
)

NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)
TEAM = "TEAM123456"
UDID = "00008030-001A2B3C4D5E"


def _profile_bytes(
    *,
    expires_at: datetime,
    udids: list[str] | None = None,
    team: str = TEAM,
) -> bytes:
    plist = plistlib.dumps(
        {
            "Name": "iOS Team Provisioning Profile: com.wda",
            "TeamIdentifier": [team],
            "ExpirationDate": expires_at,
            "ProvisionedDevices": udids if udids is not None else [UDID],
        }
    )
    return b"\x30\x82cms-garbage" + plist + b"\x00tail"


def _apple_secret(
    *,
    expires_at: datetime | None = None,
    udids: list[str] | None = None,
    session_expired: bool = False,
) -> dict[str, Any]:
    """构造 apple_id secret_data；expires_at=None 表示无 profile 缓存。"""
    profile_blob = _profile_bytes(
        expires_at=expires_at or NOW + timedelta(days=5),
        udids=udids if udids is not None else [UDID],
    )
    key_pem, csr_pem = signing.generate_key_and_csr()
    key = serialization.load_pem_private_key(key_pem, password=None)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "WDA Test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - timedelta(days=1))
        .not_valid_after(NOW + timedelta(days=30))
        .sign(key, hashes.SHA256())
    )
    cert_der = cert.public_bytes(serialization.Encoding.DER)
    secret: dict[str, Any] = {
        "email": "u@icloud.com",
        "adsid": "ADSID1",
        "idms_token": "IDMS1",
        "auth_token": "TOKEN1",
        "session_expired": session_expired,
        "team_id": TEAM,
        "cert": {
            "serial": "SER1",
            "key_pem": base64.b64encode(key_pem).decode(),
            "cert_der": base64.b64encode(cert_der).decode(),
        },
        "app_id": f"com.facebook.WebDriverAgentRunner.xctrunner.{TEAM}",
        "profile": {
            "mobileprovision_base64": base64.b64encode(profile_blob).decode(),
            "expires_at": (expires_at or NOW + timedelta(days=5)).isoformat(),
            "udids": [provision._normalize_udid(u) for u in (udids if udids is not None else [UDID])],
        },
    }
    return secret


def _apple_row(secret: dict[str, Any]) -> dict:
    return {
        "id": 3,
        "owner_user_id": "u1",
        "name": "我的 Apple ID",
        "kind": "apple_id",
        "created_at": "2026-09-07T00:00:00+00:00",
        "secret_data": secret,
    }


# ── 纯函数 ─────────────────────────────────────────────────────────────────────


def test_team_scoped_bundle_id():
    base = "com.facebook.WebDriverAgentRunner.xctrunner"
    assert provision.team_scoped_bundle_id(base, TEAM) == f"{base}.{TEAM}"
    # 已带后缀（renew 回退绑定值）原样穿透——prepare/renew 一致性。
    assert provision.team_scoped_bundle_id(f"{base}.{TEAM}", TEAM) == f"{base}.{TEAM}"
    # 大小写不敏感。
    assert provision.team_scoped_bundle_id(base, TEAM.lower()) == f"{base}.{TEAM}"


def test_cached_profile_usable():
    secret = _apple_secret(expires_at=NOW + timedelta(days=5))
    assert provision.cached_profile_usable(secret, UDID, now=NOW) is True
    # 不覆盖当前设备 → 失效（新设备注册后要重下团队描述文件）。
    assert provision.cached_profile_usable(secret, "FFFF-OTHER", now=NOW) is False
    # 无缓存 → 失效。
    assert provision.cached_profile_usable({"profile": {}}, UDID, now=NOW) is False
    # 过期 → 失效。
    expired = _apple_secret(expires_at=NOW - timedelta(days=1))
    assert provision.cached_profile_usable(expired, UDID, now=NOW) is False
    # 进 24h 缓冲窗口 → 失效（留给自动续签下一轮重试的余量）。
    soon = _apple_secret(expires_at=NOW + timedelta(hours=2))
    assert provision.cached_profile_usable(soon, UDID, now=NOW) is False


def test_profile_metadata():
    blob = _profile_bytes(expires_at=NOW + timedelta(days=7), udids=[UDID, "AA-BB"])
    meta = provision.profile_metadata(blob)
    assert meta is not None
    assert meta["team_id"] == TEAM
    assert meta["udids"] == [provision._normalize_udid(UDID), "aabb"]
    assert meta["expires_at"].startswith("2026-09-14")
    assert provision.profile_metadata(b"not-a-profile") is None


# ── 物化 → p12 形状 ───────────────────────────────────────────────────────────


def test_build_dispatch_material_is_p12_shaped():
    secret = _apple_secret()
    material = provision.build_dispatch_material(secret)
    assert set(material.keys()) == {"p12_base64", "p12_password", "mobileprovision_base64"}
    assert material["p12_password"] == signing.P12_PASSWORD
    # p12 真的能解出私钥（BestAvailableEncryption + 固定密码）。
    from cryptography.hazmat.primitives.serialization import pkcs12

    key, cert, _ = pkcs12.load_key_and_certificates(
        base64.b64decode(material["p12_base64"]),
        material["p12_password"].encode(),
    )
    assert key is not None and cert is not None
    assert base64.b64decode(material["mobileprovision_base64"]).startswith(b"\x30\x82")


def test_build_dispatch_material_requires_cache():
    # 无 cert 缓存（未物化过的新登录）→ 明确报错而不是空 p12。
    with pytest.raises(Exception):
        provision.build_dispatch_material({"profile": {}})


@pytest.mark.asyncio
async def test_materialize_apple_id_cache_hit(monkeypatch):
    """缓存命中：零 Apple 请求（ensure 不被调），p12 组装走缓存证书。"""
    from user_platform import routes_ios

    engine = SimpleNamespace(provision=provision)
    monkeypatch.setattr(routes_ios, "_load_apple_engine", lambda: engine)
    monkeypatch.setattr(
        provision, "ensure_signing_assets",
        lambda *a, **k: pytest.fail("cache hit must not call ensure_signing_assets"),
    )

    secret = _apple_secret(expires_at=NOW + timedelta(days=5))
    profile = {"id": 3, "kind": "apple_id", "secret_data": secret}
    material, final_id = await routes_ios._materialize_apple_id(
        profile, owner_user_id="u1", udid=UDID, wda_bundle_id="com.wda.base"
    )
    assert material["kind"] == "p12"
    assert set(material["secret_data"]) == {"p12_base64", "p12_password", "mobileprovision_base64"}
    assert final_id == f"com.wda.base.{TEAM}"


@pytest.mark.asyncio
async def test_materialize_apple_id_cache_miss_refreshes(monkeypatch):
    """缓存失效（设备未覆盖）：走 ensure_signing_assets，结果回写 PG（mock store）。"""
    from user_platform import routes_ios

    calls: dict[str, Any] = {}

    def fake_ensure(session, secret, *, udid, bundle_id, team_id=None, **_):
        calls["bundle_id"] = bundle_id
        calls["team_id"] = team_id
        blob = _profile_bytes(expires_at=NOW + timedelta(days=7))
        secret["profile"] = {
            "mobileprovision_base64": base64.b64encode(blob).decode(),
            "expires_at": (NOW + timedelta(days=7)).isoformat(),
            "udids": [provision._normalize_udid(UDID)],
        }
        return secret

    monkeypatch.setattr(provision, "ensure_signing_assets", fake_ensure)
    engine = SimpleNamespace(provision=provision)
    monkeypatch.setattr(routes_ios, "_load_apple_engine", lambda: engine)

    writes: list[tuple[int, dict]] = []

    import server.ios_store as ios_store

    async def fake_update_secret(pid, secret):
        writes.append((pid, secret))
        return True

    monkeypatch.setattr(ios_store, "update_signing_profile_secret", fake_update_secret)

    secret = _apple_secret(udids=["OTHER-UDID"])  # 不覆盖当前设备
    profile = {"id": 3, "kind": "apple_id", "secret_data": secret}
    material, final_id = await routes_ios._materialize_apple_id(
        profile, owner_user_id="u1", udid=UDID, wda_bundle_id=f"com.wda.base"
    )
    assert material["kind"] == "p12"
    assert final_id == f"com.wda.base.{TEAM}"
    # ensure 拿到的是 team-scoped bundle id + 团队 id。
    assert calls["bundle_id"] == final_id
    assert calls["team_id"] == TEAM
    # 刷新结果回写了 PG。
    assert writes and writes[0][0] == 3


@pytest.mark.asyncio
async def test_materialize_apple_id_session_expired(monkeypatch):
    """会话失效：session_expired 标记回写 + HTTP 412（自动续签据此可感知）。"""
    from user_platform import routes_ios
    from user_platform.apple_signing.errors import GsaError

    def broken_ensure(*a, **k):
        raise GsaError("Apple error 201: session expired")

    monkeypatch.setattr(provision, "ensure_signing_assets", broken_ensure)
    engine = SimpleNamespace(provision=provision)
    monkeypatch.setattr(routes_ios, "_load_apple_engine", lambda: engine)

    import server.ios_store as ios_store

    writes: list[dict] = []

    async def fake_update_secret(pid, secret):
        writes.append(secret)
        return True

    monkeypatch.setattr(ios_store, "update_signing_profile_secret", fake_update_secret)
    # 通知也 mock 掉（物化失败路径会尝试 emit）。
    import user_platform.notify_core as notify_core

    async def fake_notify(kind, **kwargs):
        return None

    monkeypatch.setattr(notify_core, "emit_notification", fake_notify)

    secret = _apple_secret(udids=["OTHER-UDID"])
    profile = {"id": 3, "kind": "apple_id", "secret_data": secret}
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        await routes_ios._materialize_apple_id(
            profile, owner_user_id="u1", udid=UDID, wda_bundle_id="com.wda.base"
        )
    assert excinfo.value.status_code == 412
    assert writes and writes[0]["session_expired"] is True


@pytest.mark.asyncio
async def test_materialize_passthrough_non_apple_id():
    """asc/p12 原样穿透，bundle id 不动。"""
    from user_platform import routes_ios

    profile = {"id": 1, "kind": "p12", "secret_data": {"p12_base64": "x"}}
    out, bundle = await routes_ios._materialize_apple_id(
        profile, owner_user_id="u1", udid=UDID, wda_bundle_id="com.wda"
    )
    assert out is profile and bundle == "com.wda"


# ── enrich apple_id 分支 ──────────────────────────────────────────────────────


def test_enrich_apple_id():
    valid = _enrich_profile(_apple_row(_apple_secret()), now=NOW)
    assert valid["status"] == "valid"
    assert valid["team_id"] == TEAM
    assert valid["expires_at"] is not None

    expired_session = _enrich_profile(
        _apple_row(_apple_secret(session_expired=True)), now=NOW
    )
    assert expired_session["status"] == "expired"


# ── 2FA pending TTL / 用后即焚 ────────────────────────────────────────────────


def test_prune_apple_2fa_pending():
    _APPLE_2FA_PENDING.clear()
    try:
        _APPLE_2FA_PENDING["fresh"] = {
            "expires_at": NOW + timedelta(minutes=5),
        }
        _APPLE_2FA_PENDING["stale"] = {
            "expires_at": NOW - timedelta(minutes=1),
        }
        _prune_apple_2fa_pending(NOW)
        assert "fresh" in _APPLE_2FA_PENDING
        assert "stale" not in _APPLE_2FA_PENDING
    finally:
        _APPLE_2FA_PENDING.clear()


def test_login_endpoint_contract(monkeypatch):
    """login 端点契约：引擎 2fa_required → login_token（pending 不含密码）；
    authenticated → 走 PG 落库（此处 mock）。"""
    # 引擎假对象：begin_login 返回 2fa_required（受信设备推送）。
    engine = SimpleNamespace(
        gsa=SimpleNamespace(
            begin_login=lambda email, password: {
                "status": "2fa_required",
                "method": "trusteddevice",
                "pending": {"email": email, "adsid": "A1", "idms": "I1", "method": "trusteddevice"},
            }
        ),
        developer=SimpleNamespace(list_teams=lambda session: []),
    )
    assert engine.gsa.begin_login("u@icloud.com", "pw")["status"] == "2fa_required"
    result = engine.gsa.begin_login("u@icloud.com", "pw")
    # pending 里绝无密码字段（引擎契约；密码只进函数参数）。
    assert "password" not in result["pending"]
    assert result["pending"]["adsid"] == "A1"


def test_engine_import_available():
    """依赖就位时引擎全链可导入（装完 Anisette/srp 后此测试才有意义）。"""
    from user_platform.apple_signing import developer, gsa  # noqa: F401
    from user_platform.apple_signing import anisette, errors, paths, tls  # noqa: F401

    assert callable(gsa.begin_login)
    assert callable(gsa.complete_2fa)
    assert callable(developer.list_teams)
    assert "iPASide" == signing.P12_PASSWORD
