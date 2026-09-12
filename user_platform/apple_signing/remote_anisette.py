# -*- coding: utf-8 -*-
"""远程 anisette v3 协议（移植自 isideload RemoteV3AnisetteProvider）。

v2（GET / 根路径）返回共享 anisette 头，Apple 边缘识别为可疑直接 503；v3 用
专属 provisioning 数据（adi_pb）取头，Apple 认。iloader 用的就是 v3。

流程：
  1. fetch URL bag: GET gsa.apple.com/grandslam/GsService2/lookup（基础 anisette 头）
  2. provision: WebSocket wss://<server>/v3/provisioning_session，与服务器交互
     （GiveIdentifier / GiveStartProvisioningData / GiveEndProvisioningData /
     ProvisioningSuccess），中间调 Apple mid 端点，拿到 adi_pb
  3. get_headers: POST <server>/v3/get_headers {identifier, adi_pb} → 完整 anisette 头

依赖：websockets（纯 Python，pip install websockets）；requests；certifi；plistlib。
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import plistlib
import secrets
from typing import Any, Optional

import certifi
import requests

_GS_LOOKUP = "https://gsa.apple.com/grandslam/GsService2/lookup"
_CLIENT_INFO = "<Mac15,7> <macOS;27.0;26A5378j> <com.apple.AuthKit/1 (com.apple.akd/1.0)>"
_UA = "akd/1.0 CFNetwork/808.1.4"
_TIMEOUT = 30


def _ca_bundle() -> str:
    """certifi + 项目内嵌的 Apple 私有 CA（gsa.apple.com 证书链到 Apple Root CA）。"""
    import tempfile
    from pathlib import Path
    tmp = Path(tempfile.gettempdir()) / "apple_v3_ca_bundle.pem"
    apple_pem = Path(__file__).parent / "certs" / "apple_gsa_ca.pem"
    apple_bytes = apple_pem.read_bytes() if apple_pem.exists() else b""
    pub = Path(certifi.where()).read_bytes()
    tmp.write_bytes(pub + b"\n" + apple_bytes)
    return str(tmp)


def _state_path() -> str:
    from . import paths
    return str(paths.cache_dir() / "anisette_v3_state.plist")


def _load_state() -> dict:
    """AnisetteState: {keychain_identifier (16 bytes), adi_pb (bytes or None)}."""
    try:
        with open(_state_path(), "rb") as f:
            d = plistlib.load(f)
        ident = d.get("keychain_identifier")
        if isinstance(ident, (bytes, bytearray)) and len(ident) == 16:
            return {"keychain_identifier": bytes(ident), "adi_pb": d.get("adi_pb")}
    except Exception:
        pass
    return {"keychain_identifier": secrets.token_bytes(16), "adi_pb": None}


def _save_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_state_path()), exist_ok=True)
        with open(_state_path(), "wb") as f:
            plistlib.dump(state, f)
    except Exception:
        pass


def _md_lu(ident: bytes) -> bytes:
    return hashlib.sha256(ident).digest()


def _device_id(ident: bytes) -> str:
    import uuid
    return str(uuid.UUID(bytes=ident))


def _base_headers(state: dict) -> dict:
    """provisioning 用的基础 anisette 头（无 OTP，含 MD-LU + Device-Id）。"""
    return {
        "X-Apple-I-MD-LU": _md_lu(state["keychain_identifier"]).hex(),
        "X-Mme-Device-Id": _device_id(state["keychain_identifier"]),
    }


def _base_request_headers() -> dict[str, str]:
    """isideload GrandSlam base_headers required by the URL bag/mid endpoints."""
    return {
        "Content-Type": "text/x-xml-plist",
        "Accept": "text/x-xml-plist",
        "X-Mme-Client-Info": _CLIENT_INFO,
        "User-Agent": _UA,
        "X-Xcode-Version": "27.0 (27A5218g)",
        "X-Apple-App-Info": "com.apple.gs.xcode.auth",
    }


def _fetch_url_bag() -> dict:
    """GET GsService2/lookup，返回 URL bag dict（mid 端点都在里面）。"""
    r = requests.get(_GS_LOOKUP, headers=_base_request_headers(), timeout=_TIMEOUT, verify=_ca_bundle())
    r.raise_for_status()
    d = plistlib.loads(r.content)
    urls = d.get("urls") if isinstance(d, dict) else None
    if not isinstance(urls, dict):
        raise RuntimeError("URL bag 缺 urls")
    return urls


def _plist_post(url: str, body: dict, extra_headers: dict | None = None) -> dict:
    headers = _base_request_headers()
    if extra_headers:
        headers.update(extra_headers)
    r = requests.post(url, headers=headers,
                    data=plistlib.dumps(body), timeout=_TIMEOUT, verify=_ca_bundle())
    r.raise_for_status()
    d = plistlib.loads(r.content)
    # isideload GrandSlam.plist_request 只返回 Response 子字典；spim/ptm/tk
    # 都位于这里，不能从顶层 plist 直接取，否则会向 anisette server 发 null。
    response = d.get("Response") if isinstance(d, dict) else None
    if not isinstance(response, dict):
        raise RuntimeError("grandslam response 缺 Response: %s" % str(d)[:200])
    return response


def provision(state: dict, anisette_server: str) -> dict:
    """WebSocket provisioning，拿到 adi_pb。state 原地更新并持久化。"""
    import websockets
    import asyncio

    if not anisette_server.startswith("http"):
        anisette_server = "https://" + anisette_server
    ws_url = anisette_server.rstrip("/") + "/v3/provisioning_session"
    ws_url = ws_url.replace("https://", "wss://").replace("http://", "ws://")

    urls = _fetch_url_bag()
    start_url = urls.get("midStartProvisioning")
    end_url = urls.get("midFinishProvisioning")
    if not start_url or not end_url:
        raise RuntimeError("URL bag 缺 mid 端点")

    async def _do() -> bytes:
        async with websockets.connect(ws_url, max_size=None, open_timeout=30) as ws:
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                kind = data.get("result")
                if kind == "GiveIdentifier":
                    ident_b64 = base64.b64encode(state["keychain_identifier"]).decode()
                    await ws.send(json.dumps({"identifier": ident_b64}))
                elif kind == "GiveStartProvisioningData":
                    resp = _plist_post(start_url, {"Header": {}, "Request": {}}, _base_headers(state))
                    spim = resp.get("spim")
                    await ws.send(json.dumps({"spim": spim}))
                elif kind == "GiveEndProvisioningData":
                    cpim = data.get("cpim")
                    resp = _plist_post(end_url, {"Header": {}, "Request": {"cpim": cpim}}, _base_headers(state))
                    await ws.send(json.dumps({"ptm": resp.get("ptm"), "tk": resp.get("tk")}))
                elif kind == "ProvisioningSuccess":
                    adi = data.get("adi_pb")
                    if not adi:
                        raise RuntimeError("ProvisioningSuccess 缺 adi_pb")
                    return base64.b64decode(adi)
                elif kind == "Timeout":
                    raise RuntimeError("anisette provisioning 超时")
                elif kind == "InvalidIdentifier":
                    raise RuntimeError("anisette provisioning：无效 identifier")
                else:
                    msg_text = data.get("message", "")
                    raise RuntimeError("anisette provisioning 失败: %s %s" % (kind, msg_text))

    adi_pb = asyncio.run(_do())
    state["adi_pb"] = adi_pb
    _save_state(state)
    return state


def get_headers(anisette_server: str) -> dict:
    """v3: 取完整 anisette 头（先用本地缓存的 adi_pb，没有则先 provision）。"""
    if not anisette_server.startswith("http"):
        anisette_server = "https://" + anisette_server
    state = _load_state()
    if not state.get("adi_pb"):
        state = provision(state, anisette_server)
    ident_b64 = base64.b64encode(state["keychain_identifier"]).decode()
    adi_b64 = base64.b64encode(state["adi_pb"]).decode()
    r = requests.post(anisette_server.rstrip("/") + "/v3/get_headers",
                      headers={"Content-Type": "application/json"},
                      data=json.dumps({"identifier": ident_b64, "adi_pb": adi_b64}),
                      timeout=_TIMEOUT, verify=_ca_bundle())
    r.raise_for_status()
    data = r.json()
    if "X-Apple-I-MD" not in data:
        raise RuntimeError("v3 get_headers 返回无效: %s" % str(data)[:200])
    out = {k: str(v) for k, v in data.items()}
    # X-Mme-Device-Id 和 X-Apple-I-MD-LU 必须由本地 keychain_identifier 派生
    # （isideload AnisetteData 用的就是 state.get_device_id() / get_md_lu()，不取 server
    # 的值）。setdefault 在 server 也返回这俩时会用错值，导致 Apple -80009 MID is invalid。
    out["X-Apple-I-MD-LU"] = _md_lu(state["keychain_identifier"]).hex()
    out["X-Mme-Device-Id"] = _device_id(state["keychain_identifier"])
    return out
