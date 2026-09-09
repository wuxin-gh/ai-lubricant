"""Provisioning 编排（移植自 iPASide provision.py，磁盘缓存改为 secret_data 字段）。

把一个已认证 session 变成节点重签 WDA 需要的一切（免费/付费 Apple ID 皆可）：

1. 开发证书（生成密钥 + CSR、提交、复用缓存），
2. 目标设备注册进团队，
3. App ID（bundle id 团队域作用域），
4. 绑定证书 + 设备 + App ID 的描述文件。

私钥绝不出服务端（PG secret_data）；p12 由 (cert, key) 现组装（本地毫秒级，
不做缓存）。缓存有效性由 :func:`cached_profile_usable` 判定：描述文件未到期
（留 24h 缓冲，自动续签 12h 一轮，至少两次机会）**且**覆盖当前 UDID——团队描述
文件含全部团队设备，新设备注册后重下即覆盖。

与 iPASide 的差别：``certificate.json`` / ``bundle.json`` 磁盘缓存 → 调用方持有
的 ``secret`` dict（``cert`` / ``app_id`` / ``profile`` 字段，由路由层落 PG）；
``team_id`` 显式传参（多用户并存，不再隐式记 active account）。

同步 requests 网络——路由层 asyncio.to_thread 包装。
"""

from __future__ import annotations

import base64
import plistlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from . import developer, signing
from .errors import GsaError

#: 注册证书用的 machine name。
#:
#: 承载而非装饰：Apple 的每机器证书配额靠它区分（一个账号同时可有 Xcode 一张、
#: SideStore 一张、本工具一张），也是重签发时认出「自己那张」的依据。改了它，
#: 旧名下已签发的证书就成了孤儿。
CERTIFICATE_MACHINE_NAME = "iPASide"

#: 描述文件剩余寿命低于此值即判缓存失效、重下（秒）。24h > 自动续签的 12h
#: 防风暴间隔，保证到期前至少还有一轮重试机会。
_PROFILE_FRESHNESS_SECONDS = 24 * 3600

_MOBILEPROVISION_PLIST_RE = re.compile(rb"<\?xml[\s\S]*?</plist>")


def team_scoped_bundle_id(base_bundle_id: str, team_id: str) -> str:
    """返回免费账号必可注册的 bundle id。

    免费 Apple ID 注册不了真实 App Store 应用的标识符（Apple 回 9401 "not
    available"）。追加团队 id 后唯一且必可注册——一切免费签名流程都这么做。
    已带团队后缀（renew 回退绑定值）时原样返回，保证 prepare/renew 一致。
    """
    if base_bundle_id.lower().endswith("." + team_id.lower()):
        return base_bundle_id
    # Apple 团队 id 恒大写；显式 upper 兜底，保证派生 id 与回退穿透完全一致。
    return f"{base_bundle_id}.{team_id.upper()}"


def _normalize_udid(udid: str) -> str:
    return udid.replace("-", "").lower()


def profile_metadata(profile_bytes: bytes) -> dict[str, Any] | None:
    """解析描述文件内嵌 plist 的编排元数据（复用 ios_store 的正则策略）。

    返回 ``{"expires_at": ISO, "udids": [...], "team_id": str}``；解析失败 None。
    """
    match = _MOBILEPROVISION_PLIST_RE.search(profile_bytes)
    if not match:
        return None
    try:
        plist = plistlib.loads(match.group(0))
    except Exception:  # noqa: BLE001
        return None
    expires_at = plist.get("ExpirationDate")
    if not isinstance(expires_at, datetime):
        return None
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    udids = [
        _normalize_udid(str(u)) for u in (plist.get("ProvisionedDevices") or []) if u
    ]
    return {
        "expires_at": expires_at.isoformat(),
        "udids": udids,
        "team_id": str((plist.get("TeamIdentifier") or [""])[0] or ""),
    }


def cached_profile_usable(secret: dict[str, Any], udid: str, now: datetime | None = None) -> bool:
    """缓存判定：描述文件存在、未进 24h 缓冲窗口、且覆盖当前设备。

    Pure function（路由物化层 + 测试都直接用）。``profile`` 字段结构：
    ``{"mobileprovision_base64", "expires_at", "udids"}``。
    """
    profile = secret.get("profile") or {}
    b64 = str(profile.get("mobileprovision_base64") or "")
    if not b64:
        return False
    expires_raw = str(profile.get("expires_at") or "")
    try:
        exp = datetime.fromisoformat(expires_raw)
    except ValueError:
        return False
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    if now + timedelta(seconds=_PROFILE_FRESHNESS_SECONDS) >= exp:
        return False
    cached_udids = {_normalize_udid(str(u)) for u in profile.get("udids") or []}
    return _normalize_udid(udid) in cached_udids


def build_dispatch_material(secret: dict[str, Any]) -> dict[str, Any]:
    """apple_id secret → p12 形状 secret_data（节点派发链零改动的桥）。

    从缓存证书现组装 p12（本地 RSA 序列化，毫秒级）；node_server 把
    ``p12_base64`` / ``mobileprovision_base64`` 映射进 proto 的 MANUAL_P12 模式。
    """
    cert = secret.get("cert") or {}
    key_pem = base64.b64decode(cert.get("key_pem") or "")
    cert_der = base64.b64decode(cert.get("cert_der") or "")
    if not key_pem or not cert_der:
        raise GsaError("签名配置缺少缓存的证书材料；请重新登录 Apple ID")
    p12 = signing.build_p12(cert_der, key_pem)
    return {
        "p12_base64": base64.b64encode(p12).decode(),
        "p12_password": signing.P12_PASSWORD,
        "mobileprovision_base64": str((secret.get("profile") or {}).get("mobileprovision_base64") or ""),
    }


def _cert_content(cert: dict[str, Any]) -> bytes | None:
    """从门户证书 dict 里抠 DER 证书字节（形态多变）。"""
    for key in ("certContent", "certificateContent", "certificate"):
        value = cert.get(key)
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, dict):
            inner = value.get("certContent") or value.get("certificateContent")
            if isinstance(inner, (bytes, bytearray)):
                return bytes(inner)
    return None


def _public_key_der(obj: Any) -> bytes:
    return obj.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )


def _find_cert_for_key(certs: list[dict[str, Any]], key_pem: bytes) -> tuple[bytes | None, str | None]:
    """返回与我们私钥公钥匹配的 (der, serial)。"""
    my_public = _public_key_der(serialization.load_pem_private_key(key_pem, password=None))
    for cert in certs:
        content = _cert_content(cert)
        if not content:
            continue
        try:
            parsed = x509.load_der_x509_certificate(content)
        except ValueError:
            continue
        if _public_key_der(parsed) == my_public:
            return content, cert.get("serialNumber")
    return None, None


def _ensure_certificate(
    session: dict[str, Any], team_id: str, cached_cert: dict[str, Any] | None
) -> dict[str, Any]:
    """返回 ``{serial, key_pem, cert_der}``（均为 b64 str），必要时新签发。

    缓存证书在服务端（同一台「机器」的延续），与磁盘缓存同理：序列号仍列在
    Apple 服务端的证书列表里才算有效。重签发只吊销 machineName 相同的证书
    （见 :data:`CERTIFICATE_MACHINE_NAME`）——账号配额按机器算，Xcode / SideStore
    的证书与此并存，吊销它们会弄死人家签的所有应用。
    """
    server_certs = developer.list_certificates(session, team_id)
    if cached_cert and cached_cert.get("serial"):
        if any(c.get("serialNumber") == cached_cert["serial"] for c in server_certs):
            return cached_cert

    # 要签新证书就得腾配额。只吊销「自己机器名」的旧证书。
    revoke_failure: Exception | None = None
    for cert in server_certs:
        serial = cert.get("serialNumber")
        if serial and cert.get("machineName") == CERTIFICATE_MACHINE_NAME:
            try:
                developer.revoke_certificate(session, team_id, serial)
            except developer.DeveloperServicesError as exc:
                # 单独看非致命——证书可能已不在，下面的提交才是真正的检验。
                revoke_failure = exc

    key_pem, csr_pem = signing.generate_key_and_csr()
    try:
        developer.submit_csr(session, team_id, csr_pem, machine_name=CERTIFICATE_MACHINE_NAME)
    except developer.DeveloperServicesError as exc:
        if revoke_failure is not None:
            raise GsaError(
                f"Apple 拒发签名证书 ({exc})，且旧证书吊销失败 ({revoke_failure})。"
                "请到 Apple Developer 门户 Certificates 手动吊销后重试。"
            ) from exc
        raise
    # 用公钥匹配新签发的证书（稳健：CSR 响应的 serial 不总与列表对上）。
    cert_der, serial = _find_cert_for_key(developer.list_certificates(session, team_id), key_pem)
    if cert_der is None:
        raise GsaError("未能从 Apple 取得证书内容")

    return {
        "serial": serial,
        "key_pem": base64.b64encode(key_pem).decode(),
        "cert_der": base64.b64encode(cert_der).decode(),
    }


def _ensure_device(session: dict[str, Any], team_id: str, udid: str, name: str) -> None:
    target = _normalize_udid(udid)
    for existing in developer.list_devices(session, team_id):
        if _normalize_udid(existing.get("deviceNumber", "")) == target:
            return
    try:
        developer.register_device(session, team_id, udid, name)
    except developer.DeveloperServicesError as exc:
        if "already exist" not in str(exc).lower():
            raise


def _ensure_app_id(
    session: dict[str, Any], team_id: str, bundle_id: str, name: str
) -> dict[str, Any]:
    for existing in developer.list_app_ids(session, team_id):
        if existing.get("identifier") == bundle_id:
            return existing
    try:
        return developer.add_app_id(session, team_id, bundle_id, name)
    except developer.DeveloperServicesError:
        for existing in developer.list_app_ids(session, team_id):
            if existing.get("identifier") == bundle_id:
                return existing
        raise


def ensure_signing_assets(
    session: dict[str, Any],
    secret: dict[str, Any],
    *,
    udid: str,
    bundle_id: str,
    team_id: str | None = None,
    team_name: str | None = None,
    app_name: str = "WDA",
    device_name: str = "WDA device",
) -> dict[str, Any]:
    """编排证书 + 设备 + App ID + 描述文件，写回 secret 并返回（调用方落 PG）。

    ``session``：``{adsid, auth_token}``（gsa 登录产物）。
    ``secret``：apple_id 配置的 secret_data（就地更新 ``cert`` / ``profile`` /
    ``team_id`` / ``team_name`` 字段）。
    ``bundle_id``：**已团队域作用域的**最终 id（物化层先过
    :func:`team_scoped_bundle_id`——renew 回退绑定值时它已带后缀，会原样穿透）。
    """
    if not team_id:
        teams = developer.list_teams(session)
        if not teams:
            raise GsaError("Apple ID 没有可用的开发团队（可能需要先接受开发者协议）")
        team = teams[0]
        team_id = str(team.get("teamId") or "")
        team_name = str(team.get("name") or team_name or "")

    secret["team_id"] = team_id
    if team_name:
        secret["team_name"] = team_name

    secret["cert"] = _ensure_certificate(session, team_id, secret.get("cert"))
    _ensure_device(session, team_id, udid, device_name)
    app_id = _ensure_app_id(session, team_id, bundle_id, app_name)
    app_id_id = app_id.get("appIdId") or app_id.get("identifier")

    profile = developer.download_profile(session, team_id, app_id_id)
    profile_data = profile.get("encodedProfile")
    if not isinstance(profile_data, (bytes, bytearray)):
        raise GsaError("provisioning profile download did not return profile data")

    meta = profile_metadata(bytes(profile_data))
    if meta is None:
        raise GsaError("下载的描述文件无法解析")
    secret["app_id"] = bundle_id
    secret["profile"] = {
        "mobileprovision_base64": base64.b64encode(bytes(profile_data)).decode(),
        "expires_at": meta["expires_at"],
        "udids": meta["udids"],
    }
    return secret
