"""Apple Developer Services 客户端（移植自 iPASide developer.py，session 显式传参）。

用 :mod:`.gsa` 铸出的按域 Xcode token 驱动 Xcode 用的同一套私有开发者门户 API：
列团队、CSR 换开发证书、注册设备、建/复用 App ID、下载描述文件。这些步骤让
免费 Apple ID 能给特定设备签名。

请求一律 plist-over-HTTPS POST，带 Xcode 身份（clientId + protocolVersion）+
anisette + GS token，钉 Apple CA bundle 校验。

与 iPASide 的差别：``gsa.load_session()`` 的全局 active-account 机制没了——
服务端多用户并存，session（``{adsid, auth_token}``）由调用方显式传入每个函数。
裁掉 LiveContainer 专用块（application groups）。

同步 requests 网络——路由层 asyncio.to_thread 包装。
"""

from __future__ import annotations

import plistlib
import uuid
from typing import Any

import requests

from . import anisette, gsa, tls
from .errors import DeveloperServicesError

_BASE = "https://developerservices2.apple.com/services/QH65B2/"
_CLIENT_ID = "XABBG36SBA"
_PROTOCOL_VERSION = "QH65B2"
_APP_INFO = "com.apple.gs.xcode.auth"
_TIMEOUT = 30


def _headers(session: dict[str, Any], anisette_headers: dict[str, str]) -> dict[str, str]:
    # isideload DeveloperSession::get_headers：anisette 三头 + GS token + 身份 id，
    # 叠在 GrandSlam base_headers 上（plist Accept + Client-Info + Xcode 头）。
    # 远程 v3 anisette 不返回 X-Mme-Client-Info，取空会 404——落 isideload 固定值。
    headers = {
        "Content-Type": "text/x-xml-plist",
        "User-Agent": "akd/1.0 CFNetwork/808.1.4",
        "Accept": "text/x-xml-plist",
        "Accept-Language": "en-us",
        "X-Mme-Client-Info": anisette_headers.get("X-Mme-Client-Info")
        or "<Mac15,7> <macOS;27.0;26A5378j> <com.apple.AuthKit/1 (com.apple.akd/1.0)>",
        "X-Apple-App-Info": _APP_INFO,
        "X-Xcode-Version": "27.0 (27A5218g)",
        "X-Apple-I-Identity-Id": session["adsid"],
        "X-Apple-GS-Token": session["auth_token"],
    }
    # v3 服务器 JSON 带 result 等非头键——只透传 X- 头，绝不把垃圾键发给 Apple。
    for key, value in anisette_headers.items():
        if key.startswith("X-"):
            headers[key] = value
    return headers


def _request(
    session: dict[str, Any],
    endpoint: str,
    params: dict[str, Any] | None = None,
    team_id: str | None = None,
) -> dict[str, Any]:
    """POST plist 请求到 developerservices2 端点并解析结果。"""
    body: dict[str, Any] = {
        "clientId": _CLIENT_ID,
        "protocolVersion": _PROTOCOL_VERSION,
        "requestId": str(uuid.uuid4()).upper(),
    }
    if team_id:
        body["teamId"] = team_id
    if params:
        body.update(params)

    # GSA 引擎的统一出口：transport（节点隧道）优先，否则 requests + proxies。
    # 与 gsa 同口径——登录用的出口在物化签名请求时也复用，Apple 对 IP 一致。
    transport = gsa.current_transport()
    if transport is not None:
        try:
            status, _hdrs, content = transport(
                "POST",
                f"{_BASE}{endpoint}?clientId={_CLIENT_ID}",
                _headers(session, anisette.get_headers()),
                plistlib.dumps(body),
            )
        except Exception as exc:  # noqa: BLE001
            raise DeveloperServicesError(f"节点隧道签名请求失败：{exc}") from exc
        if status >= 400:
            raise DeveloperServicesError(f"developer services HTTP {status}")
        result = plistlib.loads(content)
    else:
        resp = requests.post(
            f"{_BASE}{endpoint}?clientId={_CLIENT_ID}",
            headers=_headers(session, anisette.get_headers()),
            data=plistlib.dumps(body),
            timeout=_TIMEOUT,
            verify=tls.ca_bundle(),
            proxies=gsa.current_proxies(),
        )
        resp.raise_for_status()
        result = plistlib.loads(resp.content)

    result_code = result.get("resultCode")
    if result_code not in (0, None, "0"):
        message = result.get("userString") or result.get("resultString") or "unknown error"
        raise DeveloperServicesError(f"{result_code}: {message}")
    return result


def list_teams(session: dict[str, Any]) -> list[dict[str, Any]]:
    """返回账号可用的开发团队。"""
    result = _request(session, "listTeams.action")
    return result.get("teams", [])


def _sanitize_name(name: str) -> str:
    """Apple 要求 App ID / 设备名只含字母数字 + 空格。"""
    cleaned = "".join(ch for ch in name if ch.isalnum() or ch == " ").strip()
    return cleaned or "WDA"


# --------------------------------------------------------------------------- #
# 设备
# --------------------------------------------------------------------------- #
def list_devices(session: dict[str, Any], team_id: str) -> list[dict[str, Any]]:
    return _request(session, "ios/listDevices.action", team_id=team_id).get("devices", [])


def register_device(
    session: dict[str, Any], team_id: str, udid: str, name: str = "WDA device"
) -> dict[str, Any]:
    result = _request(
        session,
        "ios/addDevice.action",
        {"deviceNumber": udid, "name": _sanitize_name(name)},
        team_id=team_id,
    )
    return result.get("device", {})


# --------------------------------------------------------------------------- #
# 证书
# --------------------------------------------------------------------------- #
def list_certificates(session: dict[str, Any], team_id: str) -> list[dict[str, Any]]:
    result = _request(session, "ios/listAllDevelopmentCerts.action", team_id=team_id)
    return result.get("certificates", [])


def submit_csr(
    session: dict[str, Any], team_id: str, csr_pem: str, machine_name: str = "iPASide"
) -> dict[str, Any]:
    result = _request(
        session,
        "ios/submitDevelopmentCSR.action",
        {"csrContent": csr_pem, "machineId": str(uuid.uuid4()), "machineName": machine_name},
        team_id=team_id,
    )
    return result.get("certRequest", {})


def revoke_certificate(session: dict[str, Any], team_id: str, serial_number: str) -> None:
    _request(
        session,
        "ios/revokeDevelopmentCert.action",
        {"serialNumber": serial_number},
        team_id=team_id,
    )


# --------------------------------------------------------------------------- #
# App IDs
# --------------------------------------------------------------------------- #
def list_app_ids(session: dict[str, Any], team_id: str) -> list[dict[str, Any]]:
    return _request(session, "ios/listAppIds.action", team_id=team_id).get("appIds", [])


def add_app_id(
    session: dict[str, Any], team_id: str, bundle_id: str, name: str
) -> dict[str, Any]:
    result = _request(
        session,
        "ios/addAppId.action",
        {"identifier": bundle_id, "name": _sanitize_name(name)},
        team_id=team_id,
    )
    return result.get("appId", {})


# --------------------------------------------------------------------------- #
# 描述文件
# --------------------------------------------------------------------------- #
def download_profile(session: dict[str, Any], team_id: str, app_id_id: str) -> dict[str, Any]:
    """下载团队描述文件——**包含该团队全部已注册设备**，重下即可覆盖新设备。"""
    result = _request(
        session,
        "ios/downloadTeamProvisioningProfile.action",
        {"appIdId": app_id_id},
        team_id=team_id,
    )
    return result.get("provisioningProfile", {})
