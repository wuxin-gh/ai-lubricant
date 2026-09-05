"""iOS WDA job 路由（Stage 3）：签名配置管理 + WDA job 调度。

用户上传签名配置（asc p8 / p12+mobileprovision），服务端托管；WDA 产物不再单独
入库——prepare 时从市场 device-control iOS 发行版解析（GitHub Release 直链 +
sha256），宿主节点经自身出口代理下载后重签安装。job 路由发起
prepare/renew/reinstall，轮询 job 进度，取消 job。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .deps import get_current_user
from .models import User

router = APIRouter(prefix="/api/v1/users/ios", tags=["ios-management"])


class CreateSigningProfileReq(BaseModel):
    name: str
    kind: str  # "asc" or "p12"
    # For asc: p8_key, key_id, issuer_id, team_id
    # For p12: p12_base64, p12_password, mobileprovision_base64
    secret_data: dict[str, Any]


class WdaJobRequest(BaseModel):
    device_id: str
    action: str  # "prepare", "renew", "reinstall"
    signing_profile_id: int | None = None
    wda_bundle_id: str = "com.facebook.WebDriverAgentRunner.xctrunner"
    xctest_config_name: str = ""


async def _persist_wda_binding(resource_id: int, ios_info: dict, *, signing_profile_id: int | None, wda_version: str | None = None) -> None:
    """把本次 prepare 用的签名配置/WDA 市场版本落进 device.data.ios。

    自动续签（ios_auto_renew）靠这个字段知道「用哪套签名配置给哪台设备的
    WDA 续期」；不落库就只能每次要用户手选。wda_version 只是记录便于排查
    （prepare 用的市场 device-control iOS 版本），renew 本身复用设备上已装的
    产物（artifact=None）。
    best-effort：落库失败不阻塞 job（job 已派发，回滚无意义），只记日志。
    """
    import builtin_tool_store as store

    payload = dict(ios_info or {})
    if signing_profile_id is not None:
        payload["signing_profile_id"] = int(signing_profile_id)
    if wda_version:
        payload["wda_version"] = str(wda_version)
    try:
        await store.update_resource(resource_id, {"ios": payload})
    except Exception:  # noqa: BLE001
        import logging

        logging.getLogger(__name__).warning(
            "[ios-wda] persist signing binding failed (resource=%s)", resource_id, exc_info=True
        )


async def _owned_device(resource_id: int, user: User) -> dict:
    import builtin_tool_store as store

    resource = await store.get_resource(resource_id, owner_user_id=str(user.id))
    if resource is None or resource.get("resource_type") != "device":
        raise HTTPException(status_code=404, detail="设备不存在")
    return resource


# ── 签名配置管理 ──────────────────────────────────────────────────────────────

@router.post("/signing-profiles")
async def create_signing_profile(body: CreateSigningProfileReq, user: User = Depends(get_current_user)) -> dict:
    """上传签名配置（asc p8 或 p12+mobileprovision）。"""
    from server import ios_store

    if not body.name.strip():
        raise HTTPException(status_code=400, detail="签名配置名称必填")
    if body.kind not in ("asc", "p12"):
        raise HTTPException(status_code=400, detail="kind 必须是 asc 或 p12")

    # Validate required fields
    if body.kind == "asc":
        required = {"p8_key", "key_id", "issuer_id", "team_id"}
        missing = required - set(body.secret_data.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"ASC 配置缺少字段: {', '.join(missing)}")
    elif body.kind == "p12":
        required = {"p12_base64", "p12_password", "mobileprovision_base64"}
        missing = required - set(body.secret_data.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"P12 配置缺少字段: {', '.join(missing)}")

    try:
        profile = await ios_store.create_signing_profile(
            str(user.id), body.name.strip(), body.kind, body.secret_data
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return profile


@router.get("/signing-profiles")
async def list_signing_profiles(user: User = Depends(get_current_user)) -> dict:
    """列出当前用户的所有签名配置（不含 secret_data）。"""
    from server import ios_store

    profiles = await ios_store.list_signing_profiles(str(user.id))
    return {"profiles": profiles}


@router.delete("/signing-profiles/{profile_id}")
async def delete_signing_profile(profile_id: int, user: User = Depends(get_current_user)) -> dict:
    """删除签名配置。"""
    from server import ios_store

    deleted = await ios_store.delete_signing_profile(profile_id, str(user.id))
    if not deleted:
        raise HTTPException(status_code=404, detail="签名配置不存在")
    return {"deleted": True}


# ── WDA Job 调度 ──────────────────────────────────────────────────────────────

@router.post("/devices/{resource_id}/wda/prepare")
async def prepare_wda(resource_id: int, body: WdaJobRequest, user: User = Depends(get_current_user)) -> dict:
    """发起 WDA 准备 job：从市场解析 ipa → 下载 → 签名 → 安装 → 启动。

    产物不再单独入库：直接从市场 device-control iOS 发行版解析 GitHub Release
    直链 + sha256，节点经自身出口代理下载。
    """
    import uuid
    from .node_client import get_node_client
    from server import ios_store

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")
    udid = ios_info.get("udid")
    device_id = data.get("device_id")

    if not node_id or not udid or not device_id:
        raise HTTPException(status_code=400, detail="设备缺少 iOS 节点绑定信息")

    # Fetch signing profile secret (one-time dispatch)
    signing_profile = None
    if body.signing_profile_id:
        profile = await ios_store.get_signing_profile_secret(body.signing_profile_id, str(user.id))
        if not profile:
            raise HTTPException(status_code=404, detail="签名配置不存在")
        signing_profile = {
            "kind": profile.get("kind"),
            "secret_data": profile.get("secret_data"),
        }

    # 产物从市场 device-control iOS 发行版解析（GitHub Release 直链 + sha256），
    # 宿主节点经自身出口代理下载——没有单独的产物库。
    from server import device_control_release_catalog as dcrc

    snapshot = await dcrc.get_latest_release()
    asset = dcrc.select_asset(snapshot, "ios")
    if asset is None:
        raise HTTPException(
            status_code=412,
            detail="市场尚未发布 WDA iOS 产物（需运营方先在 device-control-versions 上传 iOS 包）",
        )
    digest = str(asset.get("digest") or "")
    # 市场侧 digest 形如 "sha256:<hex>"，剥前缀给节点 Fetch 比对
    sha256 = digest[7:] if digest.lower().startswith("sha256:") else digest
    if not sha256 or not asset.get("download_url"):
        raise HTTPException(status_code=412, detail="市场 iOS 产物缺少下载地址或校验值")
    wda_version = str(asset.get("version") or snapshot.get("version") or "")
    artifact = {
        "sha256": sha256,
        "download_url": asset.get("download_url"),
        "size_bytes": asset.get("size_bytes"),
        "version": wda_version,
    }

    # Generate job_id
    job_id = f"wda-{resource_id}-{uuid.uuid4().hex[:8]}"

    # Dispatch job to node
    client = get_node_client()
    try:
        result = await client.ios_start_wda_job(
            node_id,
            job_id,
            udid,
            device_id,
            action="prepare",
            artifact=artifact,
            signing_profile=signing_profile,
            wda_bundle_id=body.wda_bundle_id,
            xctest_config_name=body.xctest_config_name,
        )
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "permission_denied":
            raise HTTPException(status_code=403, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        if exc.code == "deadline_exceeded":
            raise HTTPException(status_code=504, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

    # 落库签名配置绑定：自动续签据此决定用哪套配置（A1）。
    await _persist_wda_binding(
        resource_id,
        ios_info,
        signing_profile_id=body.signing_profile_id,
        wda_version=wda_version,
    )

    return {
        "job_id": job_id,
        "device_id": device_id,
        "action": "prepare",
        "status": "accepted",
    }


@router.post("/devices/{resource_id}/wda/renew")
async def renew_wda(resource_id: int, body: WdaJobRequest, user: User = Depends(get_current_user)) -> dict:
    """发起 WDA 续期 job：复用证书 ID 只重建 profile → 重签 → 重装 → 验证。"""
    import uuid
    from .node_client import get_node_client
    from server import ios_store

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")
    udid = ios_info.get("udid")
    device_id = data.get("device_id")

    if not node_id or not udid or not device_id:
        raise HTTPException(status_code=400, detail="设备缺少节点绑定信息")

    # 续期时若未显式传 signing_profile_id，回退到 prepare 时落库的绑定（自动续签依赖此路径）。
    effective_profile_id = body.signing_profile_id
    if effective_profile_id is None:
        effective_profile_id = ios_info.get("signing_profile_id")

    # Fetch signing profile if provided
    signing_profile = None
    if effective_profile_id:
        profile = await ios_store.get_signing_profile_secret(int(effective_profile_id), str(user.id))
        if not profile:
            raise HTTPException(status_code=404, detail="签名配置不存在")
        signing_profile = {
            "kind": profile.get("kind"),
            "secret_data": profile.get("secret_data"),
        }

    job_id = f"wda-{resource_id}-{uuid.uuid4().hex[:8]}"

    client = get_node_client()
    try:
        result = await client.ios_start_wda_job(
            node_id,
            job_id,
            udid,
            device_id,
            action="renew",
            artifact=None,  # renew reuses existing artifact
            signing_profile=signing_profile,
            wda_bundle_id=body.wda_bundle_id,
            xctest_config_name=body.xctest_config_name,
        )
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        if exc.code == "deadline_exceeded":
            raise HTTPException(status_code=504, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

    return {
        "job_id": job_id,
        "device_id": device_id,
        "action": "renew",
        "status": "accepted",
    }


@router.post("/devices/{resource_id}/wda/reinstall")
async def reinstall_wda(resource_id: int, body: WdaJobRequest, user: User = Depends(get_current_user)) -> dict:
    """发起 WDA 重装 job：使用现有签名材料重新安装。"""
    import uuid
    from .node_client import get_node_client

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")
    udid = ios_info.get("udid")
    device_id = data.get("device_id")

    if not node_id or not udid or not device_id:
        raise HTTPException(status_code=400, detail="设备缺少节点绑定信息")

    job_id = f"wda-{resource_id}-{uuid.uuid4().hex[:8]}"

    client = get_node_client()
    try:
        result = await client.ios_start_wda_job(
            node_id,
            job_id,
            udid,
            device_id,
            action="reinstall",
            artifact=None,
            signing_profile=None,  # reinstall reuses cached credentials
            wda_bundle_id=body.wda_bundle_id,
            xctest_config_name=body.xctest_config_name,
        )
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        if exc.code == "deadline_exceeded":
            raise HTTPException(status_code=504, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

    return {
        "job_id": job_id,
        "device_id": device_id,
        "action": "reinstall",
        "status": "accepted",
    }


@router.get("/devices/{resource_id}/wda/jobs/{job_id}")
async def get_wda_job_status(resource_id: int, job_id: str, user: User = Depends(get_current_user)) -> dict:
    """查询 WDA job 状态（轮询端点）。"""
    from .node_client import get_node_client

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")

    if not node_id:
        raise HTTPException(status_code=400, detail="设备缺少节点绑定信息")

    client = get_node_client()
    try:
        snapshot = await client.get_ios_wda_job_status(node_id, job_id)
        return snapshot
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc


@router.post("/devices/{resource_id}/wda/jobs/{job_id}/cancel")
async def cancel_wda_job(resource_id: int, job_id: str, user: User = Depends(get_current_user)) -> dict:
    """取消运行中的 WDA job。"""
    from .node_client import get_node_client

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")

    if not node_id:
        raise HTTPException(status_code=400, detail="设备缺少节点绑定信息")

    client = get_node_client()
    try:
        result = await client.ios_cancel_wda_job(node_id, job_id)
        return result
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

