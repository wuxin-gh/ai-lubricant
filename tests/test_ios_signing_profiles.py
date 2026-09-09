"""iOS 签名配置展示元数据测试（纯函数，无 PG 依赖）。

覆盖服务端状态计算：mobileprovision 内嵌 plist 解析 + _enrich_profile 的
valid / expiring / expired / unknown 决策表。需要 PG 池的路由不在此测。
"""
from __future__ import annotations

import base64
import plistlib
from datetime import datetime, timezone

from server import ios_store
from user_platform.routes_ios import _enrich_profile


def _mobileprovision_b64(
    expires_at: datetime,
    *,
    team: str = "TEAM123456",
    name: str = "iOS Team Provisioning Profile: com.example.wda",
) -> str:
    plist = plistlib.dumps(
        {
            "Name": name,
            "TeamIdentifier": [team],
            "ExpirationDate": expires_at,
        }
    )
    # CMS 包装的描述文件：plist 原文嵌在二进制垃圾中间；解析器只关心内嵌 plist。
    blob = b"\x30\x82garbage-prefix" + plist + b"\x00garbage-suffix"
    return base64.b64encode(blob).decode()


def _p12_row(b64: str) -> dict:
    return {
        "id": 1,
        "owner_user_id": "u1",
        "name": "自备证书",
        "kind": "p12",
        "created_at": "2026-09-07T00:00:00+00:00",
        "secret_data": {
            "p12_base64": "x",
            "p12_password": "",
            "mobileprovision_base64": b64,
        },
    }


NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_mobileprovision_metadata():
    b64 = _mobileprovision_b64(datetime(2026, 9, 14))
    meta = ios_store.parse_mobileprovision_metadata(b64)
    assert meta is not None
    assert meta["team_id"] == "TEAM123456"
    assert meta["profile_name"].startswith("iOS Team Provisioning Profile")
    assert meta["expires_at"].startswith("2026-09-14T00:00:00")


def test_parse_mobileprovision_garbage_returns_none():
    assert ios_store.parse_mobileprovision_metadata(base64.b64encode(b"\x00\x01garbage").decode()) is None
    assert ios_store.parse_mobileprovision_metadata("") is None


def test_enrich_p12_valid_expiring_expired():
    valid = _enrich_profile(_p12_row(_mobileprovision_b64(datetime(2026, 9, 20))), now=NOW)
    assert valid["status"] == "valid"

    expiring = _enrich_profile(_p12_row(_mobileprovision_b64(datetime(2026, 9, 9))), now=NOW)
    assert expiring["status"] == "expiring"

    expired = _enrich_profile(_p12_row(_mobileprovision_b64(datetime(2026, 9, 1))), now=NOW)
    assert expired["status"] == "expired"
    assert expired["expires_at"].startswith("2026-09-01")
    assert expired["team_id"] == "TEAM123456"
    assert expired["profile_name"].startswith("iOS Team Provisioning Profile")


def test_enrich_p12_unknown_when_unparseable():
    row = _p12_row(base64.b64encode(b"not-a-plist").decode())
    out = _enrich_profile(row, now=NOW)
    assert out["status"] == "unknown"
    assert out["expires_at"] is None


def test_enrich_asc_long_lived():
    row = {
        "id": 2,
        "owner_user_id": "u1",
        "name": "我的 ASC",
        "kind": "asc",
        "created_at": "2026-09-07T00:00:00+00:00",
        "secret_data": {"p8_key": "k", "key_id": "1", "issuer_id": "2", "team_id": "ASCTEAM1"},
    }
    out = _enrich_profile(row, now=NOW)
    assert out["status"] == "valid"
    assert out["expires_at"] is None
    assert out["team_id"] == "ASCTEAM1"
