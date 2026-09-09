"""项目页「构建」tab 路由（Stage 4 最小落地）：WDA recipe 端到端。

设计保持通用形状（recipe_kind + 服务端渲染 steps + 一次性上传 token），本轮
只实现 WDA 一个 recipe。构建只编译/打包、绝不签名——签名一律走 go-ios ASC
路径（任意 OS 可签），在设备「准备 WDA」时按签名配置重签安装。

产物**不入库**：构建出的 .ipa 只暂存服务端供运营方下载，再由运营方手动拖进
市场管理页 device-control-versions 上传弹框发布（iOS 资产 = WDA runner ipa）。
使用者的 prepare_wda 从市场 catalog 解析产物，节点经出口代理下载——没有单独
的 WDA 产物库。

安全：上传端点 public+token（与 system-env 上传同款），token 绑 build_id、
TTL 10min、HMAC 签名；构建节点是受信主机（已能跑 host_exec），token 经 TLS
NodeConnect 一次性下发到节点，明文不进日志。
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from .deps import get_current_user
from .models import User
from .system_env_upload import base_url, issue_upload_token, verify_upload_token

# 项目维度：构建发起 + 状态轮询 + 取消
router = APIRouter(prefix="/api/v1/users/projects", tags=["project-build"])
# 公开上传端点：节点构建完把产物 POST 回来（token 鉴权，无 user session）
upload_router = APIRouter(prefix="/api/v1/public/nodes/build-artifacts", tags=["project-build"])


# ── WDA recipe（服务端预渲染，节点不感知 WDA 语义） ──────────────────────────
#
# Appium WebDriverAgent 固定源 + 固定 tag。v9.15.3 已验证存在（2026-09-08
# git ls-remote）且含 WebDriverAgent.xcodeproj + 共享 scheme
# WebDriverAgentRunner.xcscheme；产物为 WebDriverAgentRunner-Runner.app。
# Mac 的 Xcode 过老带不动新 tag 时，换老 tag 只改 source_ref/artifact_version。
#
# build-for-testing 出 .xctestrun + .app；打 .ipa 只是 Payload 目录 zip。
# 真机构建三要素：
#   - destination generic/platform=iOS（模拟器构建产 *-iphonesimulator，真机装不上）；
#   - -derivedDataPath build 把产物收敛进工作区（默认 DerivedData 步骤 2 拷不到）；
#   - 设备产物在 <Config>-iphoneos/ 下。
# 有出入时改这里的 STEPS / GLOB 即可，不动架构。签名一律走 go-ios ASC 路径，
# 所以这里 CODE_SIGN_* 全关。
WDA_RECIPE = {
    "source_url": "https://github.com/appium/WebDriverAgent.git",
    "source_ref": "v9.15.3",
    "artifact_name": "wda-unsigned",
    "artifact_version": "9.15.3",
    "steps": [
        # 只编译不签名：CODE_SIGN_* 全关，签名一律走 go-ios ASC 路径（任意 OS 可签）。
        "xcodebuild build-for-testing "
        "-project WebDriverAgent.xcodeproj "
        "-scheme WebDriverAgentRunner "
        "-destination 'generic/platform=iOS' "
        "-derivedDataPath build "
        "CODE_SIGN_IDENTITY='' CODE_SIGNING_REQUIRED=NO CODE_SIGNING_ALLOWED=NO",
        # 设备构建产物在 <Config>-iphoneos/（模拟器构建才是 iphonesimulator）。
        "mkdir -p Payload && cp -R build/Build/Products/*-iphoneos/WebDriverAgentRunner-Runner.app Payload/",
        "zip -qry unsigned.ipa Payload",
    ],
    # zip 在 srcDir 产出 unsigned.ipa。
    "artifact_glob": "unsigned.ipa",
    "timeout_seconds": 1800,  # 30 min：xcodebuild 首次编译较慢
}

# recipe_kind -> recipe。本轮只 WDA；后续 recipe 只加一条。
_RECIPES: dict[str, dict[str, Any]] = {
    "xcode_wda": WDA_RECIPE,
}


class StartBuildReq(BaseModel):
    recipe_kind: str
    node_id: str


# 模块级 build_id -> owner + 产物元数据（上传端点回填 sha/路径）。与 registry
# snapshot 同生命周期（都在内存，服务重启都丢；ipa 文件仍在磁盘，重建需重跑构建）。
_BUILD_META: dict[str, dict[str, Any]] = {}


async def _project_or_404(project_id: str, user: User) -> dict:
    from .project_service import project_service

    project = await project_service.get_project(str(user.id), project_id, role=user.role)
    if project is None:
        raise HTTPException(status_code=404, detail="项目不存在或无权访问")
    return project


@router.post("/{project_id}/builds")
async def start_project_build(
    project_id: str, body: StartBuildReq, user: User = Depends(get_current_user)
) -> dict:
    """发起一次项目构建。产物不入库，构建成功后在构建 tab 下载 ipa 手动发布市场。"""
    await _project_or_404(project_id, user)
    from .nodes_service import user_can_use_node

    node_id = (body.node_id or "").strip()
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id 必填")
    if not await user_can_use_node(user.id, node_id):
        raise HTTPException(status_code=403, detail="您无权使用此节点")

    recipe = _RECIPES.get((body.recipe_kind or "").strip())
    if recipe is None:
        raise HTTPException(status_code=400, detail=f"未知 recipe: {body.recipe_kind}")

    # 校验节点具备 xcodebuild 能力（按 capability 标签，非 macOS 自然缺席）
    from .nodes_service import list_my_nodes

    my_nodes = (await list_my_nodes(user.id)).get("nodes") or []
    node = next((n for n in my_nodes if n.get("node_id") == node_id), None)
    if node is None:
        raise HTTPException(status_code=404, detail="节点不存在或无权使用")
    caps = node.get("capabilities") or {}
    if not caps.get("xcodebuild_version"):
        raise HTTPException(
            status_code=412,
            detail="该节点不具备 Xcode 构建能力（需 macOS + Xcode）",
        )

    # 一次性上传 token：绑 build_id / module=wda_build / TTL 10min
    build_id = f"build-{uuid.uuid4().hex[:12]}"
    token = issue_upload_token(build_id, "wda_build", ttl_seconds=600)
    upload_url = f"{base_url()}/api/v1/public/nodes/build-artifacts/upload"

    # 记录 owner + 产物元数据（公开上传端点据此回填 sha/路径、下载端点据此鉴权）
    _BUILD_META[build_id] = {
        "owner_user_id": str(user.id),
        "node_id": node_id,
        "project_id": project_id,
        "artifact_name": recipe["artifact_name"],
        "artifact_version": recipe["artifact_version"],
        "artifact_sha256": None,  # 构建完成后由上传端点回填
        "storage_path": None,
    }

    from .node_client import get_node_client

    client = get_node_client()
    try:
        result = await client.start_node_build(
            node_id,
            build_id,
            recipe_kind=body.recipe_kind,
            source_url=recipe["source_url"],
            source_ref=recipe["source_ref"],
            steps=recipe["steps"],
            artifact_glob=recipe["artifact_glob"],
            upload_url=upload_url,
            upload_token=token,
            timeout_seconds=int(recipe["timeout_seconds"]),
            artifact_name=recipe["artifact_name"],
            artifact_version=recipe["artifact_version"],
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
        "build_id": build_id,
        "node_id": node_id,
        "recipe_kind": body.recipe_kind,
        "status": "accepted",
    }


@router.get("/builds/{build_id}")
async def get_project_build(build_id: str, user: User = Depends(get_current_user)) -> dict:
    """轮询构建进度。"""
    cfg = _BUILD_META.get(build_id)
    if cfg is None or cfg.get("owner_user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="构建不存在或无权访问")

    from .node_client import get_node_client

    client = get_node_client()
    node_id = cfg["node_id"]
    try:
        snapshot = await client.get_node_build_status(node_id, build_id)
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

    return snapshot


@router.post("/builds/{build_id}/cancel")
async def cancel_project_build(build_id: str, user: User = Depends(get_current_user)) -> dict:
    """取消运行中的构建。"""
    cfg = _BUILD_META.get(build_id)
    if cfg is None or cfg.get("owner_user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="构建不存在或无权访问")

    from .node_client import get_node_client

    client = get_node_client()
    try:
        result = await client.cancel_node_build(cfg["node_id"], build_id)
    except client.NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except client.RPCError as exc:
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc
    return result


# ── 公开上传端点（节点 POST 产物回来） ─────────────────────────────────────


@upload_router.post("/upload")
async def upload_build_artifact(request: Request):
    """接收节点构建的 .ipa，校验 token 后暂存服务端（不入产物库）。

    token 绑 build_id、module=wda_build、TTL 10min、HMAC 签名。鉴权无 user
    session——节点只有 token。sha/存储路径回填 _BUILD_META[build_id]，运营方
    从 GET /builds/{build_id}/artifact 下载后手动上传市场发布。
    """
    from urllib.parse import parse_qs

    authorization = request.headers.get("authorization") or ""
    bearer = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
    query = parse_qs(request.url.query)
    token = bearer or (query.get("token", [""])[0] or "").strip()
    try:
        payload = verify_upload_token(token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if payload.get("module") != "wda_build":
        raise HTTPException(status_code=400, detail="上传凭据类型不匹配")
    build_id = payload.get("market_id") or ""

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="上传内容为空")
    # 512MB 上限（对齐 wdajob Fetch 下载上限，WDA .ipa 远小于此）
    if len(data) > 512 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="产物超过大小上限（512 MiB）")
    # .ipa = zip（PK 头）；非 zip 拒收
    if data[:2] != b"PK":
        raise HTTPException(status_code=400, detail="产物必须是 .ipa（zip）")

    import hashlib
    import os

    sha256 = hashlib.sha256(data).hexdigest()
    storage_dir = os.environ.get("IOS_ARTIFACT_STORAGE_DIR", "/tmp/ios-artifacts")
    os.makedirs(storage_dir, exist_ok=True)
    storage_path = os.path.join(storage_dir, f"{sha256}.ipa")
    with open(storage_path, "wb") as f:
        f.write(data)

    cfg = _BUILD_META.get(build_id)
    if cfg is not None:
        cfg["artifact_sha256"] = sha256
        cfg["storage_path"] = storage_path
    name = (cfg or {}).get("artifact_name") or f"build-{build_id[:8]}"
    version = (cfg or {}).get("artifact_version") or ""
    return {
        "build_id": build_id,
        "name": name,
        "version": version,
        "sha256": sha256,
        "size_bytes": len(data),
    }


@router.get("/builds/{build_id}/artifact")
async def download_project_build_artifact(build_id: str, user: User = Depends(get_current_user)):
    """下载构建产物 .ipa（运营方据此拖进市场 device-control-versions 上传弹框）。"""
    import os

    from fastapi.responses import FileResponse

    cfg = _BUILD_META.get(build_id)
    if cfg is None or cfg.get("owner_user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="构建不存在或无权访问")
    storage_path = cfg.get("storage_path") or ""
    if not storage_path or not os.path.exists(storage_path):
        raise HTTPException(status_code=404, detail="产物文件不存在（构建可能尚未完成）")

    version = cfg.get("artifact_version") or ""
    # 市场契约名：device-control-<version>-ios.ipa（运营方下载后可直接拖入上传弹框）
    filename = f"device-control-{version}-ios.ipa" if version else "wda-unsigned.ipa"
    return FileResponse(storage_path, media_type="application/octet-stream", filename=filename)
