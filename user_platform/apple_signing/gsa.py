"""Apple GrandSlam（GSA）认证（移植自 iPASide gsa.py，服务端化为纯函数）。

对 ``gsa.apple.com/grandslam`` 实现 Apple 修改版 SRP-6a 登录，anisette 头来自本包
anisette 模块。社区 GrandSlam 实现（JJTech0130 / nythepegasus）的忠实移植，
三处生产化改动：(1) anisette 进程内提供而非远端服务器，(2) TLS 校验保持开启，
(3) 2FA 两步流（触发推送 → 提交验证码）跨两次 HTTP 请求，pending 状态由路由层
持有（进程内 TTL dict），引擎不再自管磁盘 session。

**密码绝不落库/落日志**：只进函数参数，用完即弃。持久化的是 session token
（adsid / GsIdmsToken / auth_token），由路由层写 PG secret_data。
``complete_2fa`` 需重发密码——它内部再跑一遍 SRP 认证拿 fresh session 才能换
developer token（iPASide 原设计，勿改成缓存密码）。

同步 requests 网络——路由层 asyncio.to_thread 包装。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import plistlib
from typing import Any

import requests
import srp._pysrp as srp
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from . import anisette, tls
from .errors import GsaError

# Apple 变体 SRP：SHA-256、2048-bit group、x 计算不含用户名。
srp.rfc5054_enable()
srp.no_username_in_x()

_GS_ENDPOINT = "https://gsa.apple.com/grandslam/GsService2"
_TRUSTED_TRIGGER = "https://gsa.apple.com/auth/verify/trusteddevice"
_VALIDATE = "https://gsa.apple.com/grandslam/GsService2/validate"

_GS_USER_AGENT = "akd/1.0 CFNetwork/978.0.7 Darwin/18.7.0"
_XCODE_APP_INFO = "com.apple.gs.xcode.auth"
_XCODE_VERSION = "11.2 (11B41)"
_TIMEOUT = 30

_PLIST_PROLOG = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
    b'"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
)


# --------------------------------------------------------------------------- #
# 请求管线
# --------------------------------------------------------------------------- #
def _cpd(headers: dict[str, str]) -> dict[str, Any]:
    """Client-provided data：标志位 + anisette（client-info 在 header）。"""
    cpd: dict[str, Any] = {
        "bootstrap": True,
        "icscrec": True,
        "pbe": False,
        "prkgen": True,
        "svct": "iCloud",
        "loc": headers.get("X-Apple-Locale", "en_US"),
    }
    for key, value in headers.items():
        if key != "X-MMe-Client-Info":
            cpd[key] = value
    return cpd


def _gs_request(params: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    body = {
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": _cpd(headers), **params},
    }
    req_headers = {
        "Content-Type": "text/x-xml-plist",
        "Accept": "*/*",
        "User-Agent": _GS_USER_AGENT,
        "X-MMe-Client-Info": headers.get("X-MMe-Client-Info", ""),
    }
    resp = requests.post(
        _GS_ENDPOINT,
        headers=req_headers,
        data=plistlib.dumps(body),
        timeout=_TIMEOUT,
        verify=tls.ca_bundle(),
    )
    resp.raise_for_status()
    return plistlib.loads(resp.content)["Response"]


def _check(response: dict[str, Any]) -> None:
    status = response.get("Status", response)
    ec = status.get("ec", 0)
    if ec != 0:
        raise GsaError(f"Apple error {ec}: {status.get('em', 'unknown error')}")


# --------------------------------------------------------------------------- #
# SRP 密码学
# --------------------------------------------------------------------------- #
def _derive_password(password: str, salt: bytes, iterations: int, s2k_fo: bool) -> bytes:
    digest = hashlib.sha256(password.encode("utf-8")).digest()
    if s2k_fo:
        digest = digest.hex().encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", digest, salt, iterations, 32)


def _session_hmac(session_key: bytes, label: str) -> bytes:
    return hmac.new(session_key, label.encode(), hashlib.sha256).digest()


def _loads_plist(raw: bytes) -> dict[str, Any]:
    """解析 GSA plist，容忍缺失的 XML prolog。"""
    try:
        return plistlib.loads(raw)
    except Exception:  # noqa: BLE001 — GSA 部分响应缺 prolog
        return plistlib.loads(_PLIST_PROLOG + raw)


def _decrypt_spd(session_key: bytes, data: bytes) -> dict[str, Any]:
    key = _session_hmac(session_key, "extra data key:")
    iv = _session_hmac(session_key, "extra data iv:")[:16]
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    raw = decryptor.update(data) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    raw = unpadder.update(raw) + unpadder.finalize()
    return _loads_plist(raw)


def _authenticate_once(
    email: str, password: str, headers: dict[str, str]
) -> tuple[dict[str, Any], str | None]:
    """跑一遍完整 SRP 握手。返回 (session-data, 二次认证类型)。"""
    usr = srp.User(email, b"", hash_alg=srp.SHA256, ng_type=srp.NG_2048)
    _, a_pub = usr.start_authentication()

    init = _gs_request(
        {"A2k": a_pub, "ps": ["s2k", "s2k_fo"], "u": email, "o": "init"}, headers
    )
    _check(init)

    protocol = init.get("sp", "s2k")
    if protocol not in ("s2k", "s2k_fo"):
        raise GsaError(f"unsupported SRP protocol from server: {protocol}")

    # 拿到盐后喂入加盐迭代密码。
    usr.p = _derive_password(password, init["s"], init["i"], protocol == "s2k_fo")
    m1 = usr.process_challenge(init["s"], init["B"])
    if m1 is None:
        raise GsaError("failed to process SRP challenge (bad server response)")

    complete = _gs_request(
        {"c": init["c"], "M1": m1, "u": email, "o": "complete"}, headers
    )
    _check(complete)

    usr.verify_session(complete["M2"])
    if not usr.authenticated():
        raise GsaError("server session verification failed (possible MITM)")

    spd = _decrypt_spd(usr.get_session_key(), complete["spd"])
    secondary = complete.get("Status", {}).get("au")
    return spd, secondary


# --------------------------------------------------------------------------- #
# 两步验证（受信设备推送）
# --------------------------------------------------------------------------- #
def _identity_token(adsid: str, idms_token: str) -> str:
    return base64.b64encode(f"{adsid}:{idms_token}".encode()).decode()


def _twofa_headers(adsid: str, idms_token: str, headers: dict[str, str]) -> dict[str, str]:
    out = {
        "Content-Type": "text/x-xml-plist",
        "User-Agent": "Xcode",
        "Accept": "text/x-xml-plist",
        "Accept-Language": "en-us",
        "X-Apple-Identity-Token": _identity_token(adsid, idms_token),
        "X-Apple-App-Info": _XCODE_APP_INFO,
        "X-Xcode-Version": _XCODE_VERSION,
    }
    out.update(headers)
    return out


def _trigger_trusted(adsid: str, idms_token: str, headers: dict[str, str]) -> None:
    """让 Apple 往受信设备推送 2FA 验证码。

    响应体即使成功也是 HTML 中间页，**status 才是信号**。忽略非 2xx（旧版做法）
    会让 issue #5 看起来像「码已发出」，实际 Apple 因坏 X-Apple-Locale 回了 500。
    """
    resp = requests.get(
        _TRUSTED_TRIGGER,
        headers=_twofa_headers(adsid, idms_token, headers),
        timeout=_TIMEOUT,
        verify=tls.ca_bundle(),
    )
    if resp.status_code != 200:
        raise GsaError(
            f"Apple 未发送验证码 (HTTP {resp.status_code})。请检查网络后重试。"
        )


def _submit_trusted(adsid: str, idms_token: str, code: str, headers: dict[str, str]) -> None:
    req_headers = _twofa_headers(adsid, idms_token, headers)
    req_headers["security-code"] = code
    resp = requests.get(_VALIDATE, headers=req_headers, timeout=_TIMEOUT, verify=tls.ca_bundle())
    _check(plistlib.loads(resp.content))


# --------------------------------------------------------------------------- #
# App-token 交换（developer services 用的按服务 Xcode token）
# --------------------------------------------------------------------------- #
_XCODE_AUTH_APP = "com.apple.gs.xcode.auth"


def _coerce_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return base64.b64decode(value)
    raise GsaError("expected bytes or base64 string in session data")


def _app_tokens_checksum(sk: bytes, adsid: str, apps: list[str]) -> bytes:
    mac = hmac.new(sk, b"apptokens" + adsid.encode(), hashlib.sha256)
    for app in apps:
        mac.update(app.encode())
    return mac.digest()


def _decrypt_gcm(sk: bytes, encrypted: bytes) -> bytes:
    # Apple 线格式："XYZ"（3 字节，同时是 AAD）| IV(16) | 密文 | tag(16)。
    if len(encrypted) < 35 or encrypted[:3] != b"XYZ":
        raise GsaError("malformed encrypted app token")
    iv = encrypted[3:19]
    ciphertext_and_tag = encrypted[19:]
    return AESGCM(sk).decrypt(iv, ciphertext_and_tag, b"XYZ")


def _fetch_app_token(
    spd: dict[str, Any], headers: dict[str, str], app: str = _XCODE_AUTH_APP
) -> dict[str, Any]:
    """把 GSA session 换成按域 Xcode token（X-Apple-GS-Token）。"""
    sk = _coerce_bytes(spd["sk"])
    adsid = spd["adsid"]
    params = {
        "u": adsid,
        "app": [app],
        "c": spd["c"],
        "t": spd["GsIdmsToken"],
        "checksum": _app_tokens_checksum(sk, adsid, [app]),
        "o": "apptokens",
    }
    response = _gs_request(params, headers)
    _check(response)
    token_plist = _loads_plist(_decrypt_gcm(sk, response["et"]))
    token_info = token_plist["t"][app]
    return {"token": token_info["token"], "expiry": token_info.get("expiry")}


def _finalize_session(
    email: str, spd: dict[str, Any], headers: dict[str, str]
) -> dict[str, Any]:
    """铸出 developer-services token，返回路由层落库的 session dict。"""
    app_token = _fetch_app_token(spd, headers)
    return {
        "status": "authenticated",
        "session": {
            "email": email,
            "adsid": spd.get("adsid"),
            "GsIdmsToken": spd.get("GsIdmsToken"),
            "auth_token": app_token.get("token"),
            "auth_token_expiry": app_token.get("expiry"),
        },
    }


# --------------------------------------------------------------------------- #
# 公共 API（纯函数：状态由调用方持有）
# --------------------------------------------------------------------------- #
def begin_login(email: str, password: str) -> dict[str, Any]:
    """启动登录。返回 authenticated（带 session）或 2fa_required（带 pending 数据）。

    - ``{"status": "authenticated", "session": {email, adsid, GsIdmsToken,
       auth_token, auth_token_expiry}}``
    - ``{"status": "2fa_required", "method": "trusteddevice"|"sms",
       "pending": {"email", "adsid", "idms", "method"}}``
      （pending 由路由层存 TTL dict，SMS 未实现——iPASide 也没实现。）
    """
    headers = anisette.get_headers()
    spd, secondary = _authenticate_once(email, password, headers)
    if not secondary:
        return _finalize_session(email, spd, headers)

    adsid, idms = spd["adsid"], spd["GsIdmsToken"]
    method = "trusteddevice" if secondary == "trustedDeviceSecondaryAuth" else "sms"
    if method == "trusteddevice":
        _trigger_trusted(adsid, idms, anisette.get_headers())
    return {
        "status": "2fa_required",
        "method": method,
        "pending": {"email": email, "adsid": adsid, "idms": idms, "method": method},
    }


def complete_2fa(
    email: str, password: str, code: str, pending: dict[str, Any]
) -> dict[str, Any]:
    """提交 2FA 码后**重新认证**拿 session token（密码必须重发，见模块 docstring）。

    pending 是 begin_login 返回的 ``pending`` dict（路由层存取）。码错 → GsaError。
    """
    if pending.get("method") == "trusteddevice":
        _submit_trusted(
            pending["adsid"], pending["idms"], code, anisette.get_headers()
        )
    else:
        raise GsaError("SMS 2FA 提交未实现")

    headers = anisette.get_headers()
    spd, secondary = _authenticate_once(email, password, headers)
    if secondary:
        raise GsaError(f"2FA 后仍要求二次认证: {secondary}")
    return _finalize_session(email, spd, headers)
