"""iOS WDA job 路由（Stage 3）：签名配置管理 + WDA job 调度。

用户上传签名配置（asc p8 / p12+mobileprovision），服务端托管；WDA 产物不再单独
入库——prepare 时从市场 device-control iOS 发行版解析（GitHub Release 直链 +
sha256），宿主节点经自身出口代理下载后重签安装。job 路由发起
prepare/renew/reinstall，轮询 job 进度，取消 job。

第三种签名方式 ``apple_id``（免费 Apple ID 全自动，2026-09 移植 iPASide 引擎到
服务端）：两步登录端点（login → verify-2fa，密码绝不落库）+ 派发前物化——
apple_id 配置现算成 p12 形状 secret_data 走既有派发链，节点零改动。
"""
from __future__ import annotations

import asyncio
import secrets as _secrets
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .deps import get_current_user
from .models import User

router = APIRouter(prefix="/api/v1/users/ios", tags=["ios-management"])

# WDA runner 的官方默认 bundle id。付费（ASC / 他人 P12）可直接用；免费 personal-team
# 的描述文件只覆盖使用者自己在 Xcode 创建的 App ID——prepare 时必须传使用者自己的
# bundle id（节点重签会把 app id 改写成它，profile 不匹配则安装失败）。
DEFAULT_WDA_BUNDLE_ID = "com.facebook.WebDriverAgentRunner.xctrunner"


class CreateSigningProfileReq(BaseModel):
    name: str
    kind: str  # "asc" | "p12"
    # For asc: p8_key, key_id, issuer_id, team_id
    # For p12: p12_base64, p12_password, mobileprovision_base64
    secret_data: dict[str, Any]


class UpdateSigningProfileReq(BaseModel):
    name: str
    # 提供 = 整包替换材料（p12 需两个文件都传 + 密码）；省略 = 只改名、保留现有材料。
    # p12 材料过期后原地换（id 不变），设备绑定 signing_profile_id 链不断。
    secret_data: dict[str, Any] | None = None


class WdaJobRequest(BaseModel):
    device_id: str
    action: str  # "prepare", "renew", "reinstall"
    signing_profile_id: int | None = None
    # 留空 = 回退 prepare 落库的绑定值（renew / 自动续签不传）；再没有才用
    # WDA 官方默认。免费签名（自备 P12）在 prepare 时必须传使用者自己的 App ID。
    # apple_id 配置免传：服务端按团队域作用域自动派生，前端隐藏此输入。
    wda_bundle_id: str = ""
    xctest_config_name: str = ""


class AppleIdLoginReq(BaseModel):
    name: str = ""
    email: str
    # 密码只在请求体内走一遭（2FA 完成需重发，引擎设计如此），绝不落库/落日志。
    password: str
    # 原地更新已有配置（重新登录）：id 不变，设备绑定的 signing_profile_id 链不断。
    profile_id: int | None = None
    # 出口代理池条目 id（network 模式）：gsa.apple.com 对数据中心 IP 直接 503，
    # 服务端所在网络被拒时经代理出境登录。空 = 直连。
    proxy_config_id: str = ""
    # 远程 anisette 服务器 URL（如 ani.sidestore.io）。本地 anisette 库生成虚拟
    # 设备指纹，Apple 会 503 拒收；远程服务器用真实 provisioning 数据生成头。
    # 空 = 用本地 anisette 库。
    anisette_server: str = ""


class AppleIdVerify2faReq(BaseModel):
    login_token: str
    email: str
    password: str
    code: str
    profile_id: int | None = None
    # 与 login 同口径：2FA 完成那步请求也经同一代理出网。
    proxy_config_id: str = ""
    # 与 login 同口径：远程 anisette 服务器（2FA 完成那步也用它取真实指纹）。
    anisette_server: str = ""


# ── Apple ID 登录中间态（进程内 TTL dict，仿 _BUILD_META 模式）────────────────
# login 成功但需 2FA 时，把 pending（adsid/idms——不含密码）暂存 10 分钟，
# login_token 是随机 token。多 worker 部署下 2FA 必须打回同一 worker——单进程
# 部署（本项目现状）无此问题。
_APPLE_2FA_TTL_SECONDS = 10 * 60
_APPLE_2FA_PENDING: dict[str, dict[str, Any]] = {}


def _prune_apple_2fa_pending(now: datetime) -> None:
    expired = [
        token
        for token, entry in _APPLE_2FA_PENDING.items()
        if (entry.get("expires_at") or now) < now
    ]
    for token in expired:
        _APPLE_2FA_PENDING.pop(token, None)


def _load_apple_engine() -> Any:
    """惰性加载 Apple ID 签名引擎（apple_signing 包）。

    依赖（Anisette / srp）缺失时 503 降级——不影响 asc / p12 既有路径。
    返回 SimpleNamespace(gsa, developer, provision)；测试 monkeypatch 本函数
    注入假引擎。
    """
    try:
        from .apple_signing import developer as apple_developer
        from .apple_signing import gsa as apple_gsa
        from .apple_signing import provision as apple_provision
    except ImportError as exc:
        raise HTTPException(
            status_code=503, detail=f"Apple ID 签名引擎不可用（依赖未安装）：{exc}"
        ) from exc
    return SimpleNamespace(gsa=apple_gsa, developer=apple_developer, provision=apple_provision)


async def _apple_gsa_egress(proxy_config_id: str) -> None:
    """按签名配置的代理池条目设置 GSA 引擎出口。

    三种路径：
    * node 模式：node_id 指向一台执行节点，GSA 请求经 NodeProxyRequest 帧发给该
      节点出网（节点 IP 可能是 Apple 认可的住宅/宽带 IP）。同步桥在工作线程内
      asyncio.run() 跑帧往返。
    * network 模式：requests proxies 形态（http://...），直接给 requests 用。
    * 空/直连：什么都不设置，引擎走默认直连。

    url_prefix 模式对 GSA API 请求无意义（它是下载前缀改写），400 拒绝。
    gsa.apple.com 对数据中心 IP 直接 503——服务端所在网络被拒时必须经代理出境。
    """
    wanted = (proxy_config_id or "").strip()
    if not wanted:
        return
    # node 模式拿不到 node_id，得绕过 resolve_proxy（它对 node 模式直接 raise）。
    from config import CONFIG_STORE

    try:
        main = await CONFIG_STORE.read_main_async()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"读取代理池失败：{exc}") from exc
    proxies = main.get("proxies") if isinstance(main, dict) else []
    entry = None
    if isinstance(proxies, list):
        for p in proxies:
            if isinstance(p, dict) and (p.get("id") or "") == wanted:
                entry = p
                break
    if entry is None:
        raise HTTPException(status_code=400, detail=f"代理配置 {wanted} 不存在")
    mode = str(entry.get("mode") or "network").strip().lower()

    if mode == "node":
        node_id = str(entry.get("node_id") or "").strip()
        if not node_id:
            raise HTTPException(status_code=400, detail="节点隧道代理未配置目标节点")
        engine = _load_apple_engine()
        engine.gsa.set_gsa_transport(_node_tunnel_transport(node_id))
        return

    if mode == "network":
        from proxy_utils import canonical_proxy, proxy_effective_url

        url = proxy_effective_url(entry)
        if not url:
            raise HTTPException(status_code=400, detail="网络代理未配置地址")
        engine = _load_apple_engine()
        engine.gsa.set_gsa_proxies({"http": url, "https": url})
        return

    if mode == "direct":
        # 显式直连：清掉同一进程上一次登录可能留下的 node/network 出口。
        engine = _load_apple_engine()
        engine.gsa.set_gsa_proxies(None)
        return

    raise HTTPException(
        status_code=400,
        detail="Apple ID 登录仅支持直连、网络代理或节点隧道代理（direct/network/node 模式）",
    )


def _node_tunnel_transport(node_id: str):
    """构造同步 transport：GSA 引擎线程内调，经节点隧道发请求取完整响应。

    工作线程（asyncio.to_thread）无 running loop，asyncio.run() 安全。
    RemoteNodeConnectManager 每次请求新建 aiohttp session，无跨 loop 亲和问题。
    """
    from providers.proxy_manager import get_proxy_manager

    manager = getattr(get_proxy_manager(), "_node_manager", None)
    if manager is None:
        raise RuntimeError("节点代理未就绪（node manager 未注入）")

    def transport(method: str, url: str, headers: dict, body: bytes) -> tuple[int, dict, bytes]:
        import asyncio

        async def _do() -> tuple[int, dict, bytes]:
            resp = await manager.request(
                node_id, method=method, url=url, headers=headers, body=body
            )
            chunks: list[bytes] = []
            try:
                async for chunk in resp.iter_chunks():
                    chunks.append(chunk)
            finally:
                await resp.close()
            return resp.status, dict(resp.headers), b"".join(chunks)

        return asyncio.run(_do())

    return transport


async def _resolve_market_wda_asset() -> dict:
    """从市场 device-control iOS 发行版解析 WDA 产物（GitHub Release 直链 + sha256）。

    prepare / renew / reinstall 都走这条：节点收到 artifact 后才下载校验
    （artifact=None 会在节点侧 artifact_missing 即停）。市场产物由宿主节点
    经自身出口代理下载。

    raises HTTPException(412) when the market has no iOS asset / no url+digest.
    """
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
    return {
        "sha256": sha256,
        "download_url": asset.get("download_url"),
        "size_bytes": asset.get("size_bytes"),
        "version": str(asset.get("version") or snapshot.get("version") or ""),
    }


async def _resolve_signing_profile(
    profile_id: int | None, owner_user_id: str, *, fallback_binding_id=None
) -> dict | None:
    """取签名配置（含 secret_data + id），renew/reinstall 未显式传时回退到 binding。

    返回的 dict 里带 id：apple_id 物化层要按 id 加锁串行 + 回写刷新的材料。
    （p12/asc 原样派发，多出的 id 键节点侧忽略。）
    """
    from server import ios_store

    effective_id = profile_id
    if effective_id is None and fallback_binding_id is not None:
        effective_id = fallback_binding_id
    if not effective_id:
        return None
    profile = await ios_store.get_signing_profile_secret(int(effective_id), owner_user_id)
    if not profile:
        raise HTTPException(status_code=404, detail="签名配置不存在")
    return {
        "id": profile.get("id"),
        "kind": profile.get("kind"),
        "secret_data": profile.get("secret_data"),
    }


def _resolve_wda_bundle_id(body_value: str | None, ios_info: dict) -> str:
    """bundle id 解析优先级：本次显式传入 > prepare 落库绑定 > WDA 官方默认。

    renew / 自动续签不带 bundle id（WdaJobRequest 默认空串），必须回退绑定值——
    免费签名的设备装的是使用者自己的 bundle id，回落官方默认会重签+启动失败。
    """
    explicit = (body_value or "").strip()
    if explicit:
        return explicit
    bound = str((ios_info or {}).get("wda_bundle_id") or "").strip()
    return bound or DEFAULT_WDA_BUNDLE_ID


async def _persist_wda_binding(
    resource_id: int,
    ios_info: dict,
    *,
    signing_profile_id: int | None,
    wda_version: str | None = None,
    wda_bundle_id: str | None = None,
) -> None:
    """把本次 prepare 用的签名配置/WDA 市场版本/bundle id 落进 device.data.ios。

    自动续签（ios_auto_renew）靠 signing_profile_id 知道「用哪套签名配置续期」，
    靠 wda_bundle_id 知道「签成哪个 app id」（免费签名是使用者自己的 id，renew
    不带值必须从这里回退，不能回落官方默认）。wda_version 只是记录便于排查
    （prepare 用的市场 device-control iOS 版本）。
    best-effort：落库失败不阻塞 job（job 已派发，回滚无意义），只记日志。
    """
    import builtin_tool_store as store

    payload = dict(ios_info or {})
    if signing_profile_id is not None:
        payload["signing_profile_id"] = int(signing_profile_id)
    if wda_version:
        payload["wda_version"] = str(wda_version)
    if wda_bundle_id:
        payload["wda_bundle_id"] = str(wda_bundle_id)
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


def _enrich_profile(row: dict, *, now: datetime | None = None) -> dict:
    """剥掉 secret_data 并计算签名配置的展示元数据（status/expiry/team_id）。

    p12 现解析 secret_data 里的 mobileprovision：ExpirationDate 决定 status
    （已过期 / ≤3天即将过期 / 可用；存量脏数据解析失败回退 unknown）；asc 的 p8
    无自然过期，恒 valid。服务端统一算，前端只渲染不重复日期逻辑。
    """
    from server import ios_store

    kind = str(row.get("kind") or "")
    out: dict[str, Any] = {
        "id": row.get("id"),
        "owner_user_id": row.get("owner_user_id"),
        "name": row.get("name"),
        "kind": kind,
        "created_at": row.get("created_at"),
        "expires_at": None,
        "team_id": None,
        "profile_name": None,
        "status": "valid",
    }
    secret = row.get("secret_data") or {}
    if kind == "asc":
        out["team_id"] = str(secret.get("team_id") or "") or None
        return out
    if kind == "apple_id":
        # 会话过期标记（物化失败时置 true）驱动「需重新登录」徽章；登录有效时
        # expires_at 展示缓存描述文件的到期（自动续签会刷新它，无 expiring 态）。
        out["team_id"] = str(secret.get("team_id") or "") or None
        profile = secret.get("profile") or {}
        out["expires_at"] = str(profile.get("expires_at") or "") or None
        if secret.get("session_expired"):
            out["status"] = "expired"
        return out
    if kind != "p12":
        return out
    meta = ios_store.parse_mobileprovision_metadata(
        str(secret.get("mobileprovision_base64") or "")
    )
    if meta is None:
        out["status"] = "unknown"
        return out
    out["expires_at"] = meta["expires_at"]
    out["team_id"] = meta["team_id"] or None
    out["profile_name"] = meta["profile_name"] or None
    exp = datetime.fromisoformat(meta["expires_at"])
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    if now > exp:
        out["status"] = "expired"
    elif now + timedelta(days=3) >= exp:
        out["status"] = "expiring"
    return out


@router.post("/signing-profiles")
async def create_signing_profile(body: CreateSigningProfileReq, user: User = Depends(get_current_user)) -> dict:
    """上传签名配置（asc p8 / p12+mobileprovision），返回富化展示元数据。"""
    from server import ios_store

    if not body.name.strip():
        raise HTTPException(status_code=400, detail="签名配置名称必填")
    if body.kind not in ("asc", "p12"):
        raise HTTPException(status_code=400, detail="kind 必须是 asc / p12")

    # Validate required fields.
    if body.kind == "asc":
        required = {"p8_key", "key_id", "issuer_id", "team_id"}
        missing = required - set(body.secret_data.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"ASC 配置缺少字段: {', '.join(missing)}")
    else:  # p12
        required = {"p12_base64", "p12_password", "mobileprovision_base64"}
        missing = required - set(body.secret_data.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"P12 配置缺少字段: {', '.join(missing)}")
        # 入门口拦脏数据：mobileprovision 必须可解析（之后列表 status 恒可算；
        # 存量脏数据行由 _enrich_profile 回退 unknown，不再兜底入库）。
        if (
            ios_store.parse_mobileprovision_metadata(
                str(body.secret_data.get("mobileprovision_base64") or "")
            )
            is None
        ):
            raise HTTPException(
                status_code=400,
                detail="mobileprovision 无法解析，可能不是有效的描述文件",
            )

    try:
        profile = await ios_store.create_signing_profile(
            str(user.id), body.name.strip(), body.kind, body.secret_data
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    # store 返回行不含 secret_data——把本次材料塞回内存供富化（不落响应）。
    profile["secret_data"] = body.secret_data
    return _enrich_profile(profile)


@router.get("/signing-profiles")
async def list_signing_profiles(user: User = Depends(get_current_user)) -> dict:
    """列出当前用户的所有签名配置（含 status/expiry，绝不含 secret_data）。"""
    from server import ios_store

    rows = await ios_store.list_signing_profiles(str(user.id), include_secret=True)
    return {"profiles": [_enrich_profile(r) for r in rows]}


@router.put("/signing-profiles/{profile_id}")
async def update_signing_profile(
    profile_id: int, body: UpdateSigningProfileReq, user: User = Depends(get_current_user)
) -> dict:
    """更新签名配置：只改名，或原地整包替换材料（id 不变）。

    p12 材料过期后只能原地换——删了重建会换 id，设备绑定的 signing_profile_id
    会立即失效（自动续签报 signing_profile_missing）。asc 同理（key 被吊销换新）。
    apple_id 只能改名或走重新登录端点（材料是 Apple 侧签发的，手改必坏）。
    """
    from server import ios_store

    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="签名配置名称必填")

    existing = await ios_store.get_signing_profile(profile_id, str(user.id))
    if not existing:
        raise HTTPException(status_code=404, detail="签名配置不存在")

    secret_data = body.secret_data
    if secret_data is not None:
        kind = str(existing.get("kind") or "")
        if kind == "apple_id":
            raise HTTPException(
                status_code=400,
                detail="Apple ID 配置只能通过重新登录更新（材料由 Apple 侧签发）",
            )
        if kind == "asc":
            required = {"p8_key", "key_id", "issuer_id", "team_id"}
        else:
            required = {"p12_base64", "p12_password", "mobileprovision_base64"}
        missing = required - set(secret_data.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"配置缺少字段: {', '.join(missing)}")
        if kind == "p12" and (
            ios_store.parse_mobileprovision_metadata(
                str(secret_data.get("mobileprovision_base64") or "")
            )
            is None
        ):
            raise HTTPException(
                status_code=400,
                detail="mobileprovision 无法解析，可能不是有效的描述文件",
            )

    updated = await ios_store.update_signing_profile(profile_id, str(user.id), name, secret_data)
    if not updated:
        raise HTTPException(status_code=404, detail="签名配置不存在")
    return _enrich_profile(updated)


@router.delete("/signing-profiles/{profile_id}")
async def delete_signing_profile(profile_id: int, user: User = Depends(get_current_user)) -> dict:
    """删除签名配置。"""
    from server import ios_store

    deleted = await ios_store.delete_signing_profile(profile_id, str(user.id))
    if not deleted:
        raise HTTPException(status_code=404, detail="签名配置不存在")
    return {"deleted": True}


# ── Apple ID 登录（两步：login → verify-2fa）──────────────────────────────────


def _apple_secret_from_session(session: dict[str, Any]) -> dict[str, Any]:
    """gsa 会话产物 → apple_id secret_data 骨架（密码绝不在内）。"""
    return {
        "email": session.get("email"),
        "adsid": session.get("adsid"),
        "idms_token": session.get("GsIdmsToken"),
        "auth_token": session.get("auth_token"),
        "auth_token_expiry": session.get("auth_token_expiry"),
        "session_expired": False,
        # cert / app_id / profile 由物化时 ensure_signing_assets 写入
    }


async def _apple_persist_secret(
    engine: Any, secret: dict[str, Any], session: dict[str, Any], *, user: User,
    name: str, profile_id: int | None
) -> dict:
    """登录/2FA 成功后的落库路径：捕团队信息（首次）→ 新建或原地更新配置。

    team_id 在登录时尽力捕获（失败不阻塞——物化时 ensure 会兜底 listTeams）；
    重新登录（profile_id 给到）保留既有 cert/profile 缓存（旧会话签发的材料
    在新会话下仍可用，团队一致时无需重签发），只换 session token。
    """
    from server import ios_store

    if not secret.get("team_id"):
        try:
            teams = await asyncio.to_thread(
                engine.developer.list_teams,
                {"adsid": secret["adsid"], "auth_token": secret["auth_token"]},
            )
            if teams:
                secret["team_id"] = str(teams[0].get("teamId") or "")
                secret["team_name"] = str(teams[0].get("name") or "")
        except Exception:  # noqa: BLE001 — 团队信息只是锦上添花，物化时兜底
            pass

    if profile_id:
        existing = await ios_store.get_signing_profile_secret(profile_id, str(user.id))
        if not existing:
            raise HTTPException(status_code=404, detail="签名配置不存在")
        if str(existing.get("kind") or "") != "apple_id":
            raise HTTPException(status_code=400, detail="该配置不是 Apple ID 类型")
        old_secret = existing.get("secret_data") or {}
        # 保留旧 cert/app_id/profile 缓存（团队相同时材料仍有效）。
        for carry_key in ("cert", "app_id", "profile", "team_id", "team_name"):
            if old_secret.get(carry_key) and not secret.get(carry_key):
                secret[carry_key] = old_secret[carry_key]
        updated = await ios_store.update_signing_profile(
            profile_id, str(user.id), name or str(existing.get("name") or ""), secret
        )
        row = {**updated, "secret_data": secret}
    else:
        try:
            row = await ios_store.create_signing_profile(
                str(user.id), name or (secret.get("email") or "Apple ID"), "apple_id", secret
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        row["secret_data"] = secret
    return _enrich_profile(row)


async def _apple_finalize_login(
    engine: Any, result: dict[str, Any], *, user: User, name: str, profile_id: int | None
) -> dict:
    """begin_login/complete_2fa 成功结果 → 持久化 + 富化响应。"""
    secret = _apple_secret_from_session(result["session"])
    profile = await _apple_persist_secret(
        engine, secret, result["session"], user=user, name=name, profile_id=profile_id
    )
    return {"status": "authenticated", "profile": profile}


def _apple_engine_error_to_http(exc: Exception) -> HTTPException:
    """引擎错误 → HTTP 状态（GsaError 用户可纠正 400；其余依赖/网络 503）。"""
    from .apple_signing.errors import AnisetteError, EngineError, GsaError

    if isinstance(exc, GsaError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, AnisetteError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, EngineError):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=502, detail=f"Apple 服务请求失败: {exc}")


@router.post("/signing-profiles/apple-id/login")
async def apple_id_login(body: AppleIdLoginReq, user: User = Depends(get_current_user)) -> dict:
    """Apple ID 登录第一步：SRP 认证；无 2FA 直接建配置，有 2FA 返回 login_token。

    密码只在请求体内走一遭（引擎用完即弃）；2FA 完成时**前端要再发一遍密码**
    （complete_2fa 内部重新 SRP 认证拿 fresh session，引擎设计如此——绝不缓存）。
    """
    engine = _load_apple_engine()
    email = body.email.strip()
    if not email or not body.password:
        raise HTTPException(status_code=400, detail="Apple ID 邮箱与密码必填")

    if body.profile_id:
        from server import ios_store

        existing = await ios_store.get_signing_profile(body.profile_id, str(user.id))
        if not existing:
            raise HTTPException(status_code=404, detail="签名配置不存在")
        if str(existing.get("kind") or "") != "apple_id":
            raise HTTPException(status_code=400, detail="该配置不是 Apple ID 类型")

    # 出口路由：必须在 begin_login 前设置——登录/2FA/物化全链路用同一出口。
    await _apple_gsa_egress(body.proxy_config_id)
    # 远程 anisette 服务器（真实设备指纹，避开本地虚拟指纹被 Apple 503）。
    engine.gsa.set_gsa_transport(None)  # 防御：清掉上次的 transport
    try:
        from .apple_signing import anisette as _anisette_mod
        _anisette_mod.set_remote_server(body.anisette_server or "")
    except Exception:  # noqa: BLE001
        pass

    try:
        result = await asyncio.to_thread(engine.gsa.begin_login, email, body.password)
    except Exception as exc:  # noqa: BLE001 — 引擎错误统一映射 HTTP
        raise _apple_engine_error_to_http(exc) from exc

    if result.get("status") == "2fa_required":
        now = datetime.now(timezone.utc)
        _prune_apple_2fa_pending(now)
        token = _secrets.token_urlsafe(24)
        _APPLE_2FA_PENDING[token] = {
            **result["pending"],
            "owner_user_id": str(user.id),
            "expires_at": now + timedelta(seconds=_APPLE_2FA_TTL_SECONDS),
        }
        return {
            "status": "2fa_required",
            "method": result.get("method"),
            "login_token": token,
        }
    return await _apple_finalize_login(
        engine, result, user=user, name=body.name.strip(), profile_id=body.profile_id
    )


@router.post("/signing-profiles/apple-id/verify-2fa")
async def apple_id_verify_2fa(body: AppleIdVerify2faReq, user: User = Depends(get_current_user)) -> dict:
    """Apple ID 登录第二步：提交受信设备验证码（密码随请求重发，见 login 端点 docstring）。

    pending 用后即焚（pop）；login_token 错/过期/换人 400。
    """
    engine = _load_apple_engine()
    entry = _APPLE_2FA_PENDING.pop(body.login_token, None)
    if not entry or datetime.now(timezone.utc) >= entry["expires_at"]:
        _prune_apple_2fa_pending(datetime.now(timezone.utc))
        raise HTTPException(status_code=400, detail="登录会话已过期，请重新登录")
    if entry.get("owner_user_id") != str(user.id):
        raise HTTPException(status_code=403, detail="验证码不属于当前用户")
    if str(entry.get("email") or "") != body.email.strip():
        raise HTTPException(status_code=400, detail="邮箱与登录时不一致")

    # 与 login 同口径出口代理；2FA 完成那步请求同样要经同一代理出网。
    await _apple_gsa_egress(body.proxy_config_id)
    # 远程 anisette 服务器（与 login 同口径）。
    try:
        from .apple_signing import anisette as _anisette_mod
        _anisette_mod.set_remote_server(body.anisette_server or "")
    except Exception:  # noqa: BLE001
        pass

    pending = {
        "email": entry.get("email"),
        "adsid": entry.get("adsid"),
        "idms": entry.get("idms"),
        "method": entry.get("method"),
    }
    try:
        result = await asyncio.to_thread(
            engine.gsa.complete_2fa, body.email.strip(), body.password, body.code.strip(), pending
        )
    except Exception as exc:  # noqa: BLE001
        # 码错时 pending 已被 pop——用户要重来整个登录（Apple 侧推送码也只一次）。
        raise _apple_engine_error_to_http(exc) from exc
    return await _apple_finalize_login(
        engine, result, user=user, name="", profile_id=body.profile_id
    )


# ── apple_id 物化（派发前现算成 p12 形状，节点零改动）──────────────────────────

# 每 profile 一把锁：并发派发同一 apple_id 配置时串行物化，防止双跑 Apple
# 请求（配额敏感）+ secret 回写互踩。锁本身泄漏无妨（dict 有界：每配置一把）。
_APPLE_MATERIALIZERS: dict[int, asyncio.Lock] = {}


async def _materialize_apple_id(
    signing_profile: dict | None,
    *,
    owner_user_id: str,
    udid: str,
    wda_bundle_id: str,
) -> tuple[dict | None, str]:
    """apple_id 配置 → (p12 形状派发材料, team-scoped bundle id)。

    缓存命中（描述文件未到期+24h 缓冲、覆盖当前 UDID）时零 Apple 请求——
    p12 从缓存证书本地组装（毫秒级）。缓存失效才 ensure_signing_assets（证书/
    设备/App ID/描述文件全兜底）并回写 secret。非 apple_id 配置原样穿透。

    会话失效（GsaError/DeveloperServicesError）→ secret.session_expired=true
    回写 + 用户通知 + HTTP 412（重新登录后自动恢复；否则自动续签静默失败）。
    """
    if not signing_profile or str(signing_profile.get("kind") or "") != "apple_id":
        return signing_profile, wda_bundle_id

    from server import ios_store

    engine = _load_apple_engine()
    profile_id = int(signing_profile.get("id") or 0)
    secret = dict(signing_profile.get("secret_data") or {})
    lock = _APPLE_MATERIALIZERS.setdefault(profile_id, asyncio.Lock())

    def _sync_materialize(secret: dict) -> tuple[dict, str]:
        """同步块（asyncio.to_thread 里跑）：缓存命中直接组装，否则全量 ensure。"""
        from .apple_signing.provision import (
            build_dispatch_material,
            cached_profile_usable,
            ensure_signing_assets,
            team_scoped_bundle_id,
        )

        team_id = str(secret.get("team_id") or "")
        final_id = team_scoped_bundle_id(wda_bundle_id, team_id) if team_id else wda_bundle_id
        if team_id and cached_profile_usable(secret, udid):
            return build_dispatch_material(secret), final_id
        session = {"adsid": secret.get("adsid"), "auth_token": secret.get("auth_token")}
        ensure_signing_assets(
            session, secret, udid=udid, bundle_id=final_id, team_id=team_id or None
        )
        return build_dispatch_material(secret), final_id

    async with lock:
        try:
            material, final_id = await asyncio.to_thread(_sync_materialize, secret)
        except Exception as exc:  # noqa: BLE001 — 分类：会话失效 vs 网络抖动
            from .apple_signing.errors import AnisetteError, GsaError
            from .apple_signing.developer import DeveloperServicesError

            if isinstance(exc, (GsaError, DeveloperServicesError)):
                # 会话/权限失效：标记 + 通知 + 412（网络类 AnisetteError 原样 503，
                # 自动续签下一轮自然重试，不打扰用户）。
                secret["session_expired"] = True
                try:
                    await ios_store.update_signing_profile_secret(profile_id, secret)
                except Exception:  # noqa: BLE001 — 标记失败不影响本次报错
                    pass
                try:
                    from .notify_core import emit_notification

                    await emit_notification(
                        "ios.wda.apple_session_expired",
                        params={"profile_id": profile_id, "udid": udid},
                        owner_type="user",
                        owner_id=owner_user_id or None,
                        severity="error",
                        kind="ios",
                        source="ios-wda-materialize",
                        title="Apple ID 会话已过期",
                        message=(
                            "Apple ID 签名配置的登录会话已过期，自动续签已暂停。"
                            "请到「资源 → 签名配置」重新登录该 Apple ID。"
                        ),
                        detail=str(exc),
                        dedupe_key=f"ios-wda-apple-session-expired:{profile_id}",
                        dedupe_window_seconds=12 * 3600,
                    )
                except Exception:  # noqa: BLE001 — 通知失败不影响报错
                    pass
                raise HTTPException(
                    status_code=412,
                    detail=f"Apple ID 会话已过期，请重新登录后重试（{exc}）",
                ) from exc
            if isinstance(exc, AnisetteError):
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            raise HTTPException(status_code=502, detail=f"Apple 签名材料准备失败: {exc}") from exc

        # 成功：回写刷新后的 secret（新 profile 缓存/首次 cert/team 信息）。
        # 比较旧值避免每次 prepare 都写库（缓存命中路径 secret 未变）。
        if secret != (signing_profile.get("secret_data") or {}):
            try:
                await ios_store.update_signing_profile_secret(profile_id, secret)
            except Exception:  # noqa: BLE001 — 回写失败不阻塞本次派发
                import logging

                logging.getLogger(__name__).warning(
                    "[ios-wda] apple_id secret write-back failed (profile=%s)",
                    profile_id,
                    exc_info=True,
                )

    return {"kind": "p12", "secret_data": material}, final_id


# ── WDA Job 调度 ──────────────────────────────────────────────────────────────

@router.post("/devices/{resource_id}/wda/prepare")
async def prepare_wda(resource_id: int, body: WdaJobRequest, user: User = Depends(get_current_user)) -> dict:
    """发起 WDA 准备 job：从市场解析 ipa → 下载 → 签名 → 安装 → 启动。

    产物不再单独入库：直接从市场 device-control iOS 发行版解析 GitHub Release
    直链 + sha256，节点经自身出口代理下载。
    """
    import uuid
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

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
        signing_profile = await _resolve_signing_profile(
            body.signing_profile_id, str(user.id)
        )

    # 产物从市场 device-control iOS 发行版解析（GitHub Release 直链 + sha256），
    # 宿主节点经自身出口代理下载——没有单独的产物库，节点重签后安装。
    artifact = await _resolve_market_wda_asset()

    # Generate job_id
    job_id = f"wda-{resource_id}-{uuid.uuid4().hex[:8]}"

    # bundle id：prepare 显式传入（免费签名须为使用者自己的 App ID），解析完落库，
    # renew / 自动续签不带值时从绑定回退。
    wda_bundle_id = _resolve_wda_bundle_id(body.wda_bundle_id, ios_info)

    # apple_id：物化成 p12 形状 + 团队域作用域 bundle id（前端免传——从默认值
    # 派生 base.TEAMID；绑定值落的是 final id，renew 回退穿透 team_scope）。
    signing_profile, wda_bundle_id = await _materialize_apple_id(
        signing_profile, owner_user_id=str(user.id), udid=udid, wda_bundle_id=wda_bundle_id
    )

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
            wda_bundle_id=wda_bundle_id,
            xctest_config_name=body.xctest_config_name,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "permission_denied":
            raise HTTPException(status_code=403, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        if exc.code == "deadline_exceeded":
            raise HTTPException(status_code=504, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

    # 落库签名配置绑定：自动续签据此决定用哪套配置（A1）+ 续期沿用同一 bundle id。
    await _persist_wda_binding(
        resource_id,
        ios_info,
        signing_profile_id=body.signing_profile_id,
        wda_version=artifact.get("version"),
        wda_bundle_id=wda_bundle_id,
    )

    return {
        "job_id": job_id,
        "device_id": device_id,
        "action": "prepare",
        "status": "accepted",
    }


@router.post("/devices/{resource_id}/wda/renew")
async def renew_wda(resource_id: int, body: WdaJobRequest, user: User = Depends(get_current_user)) -> dict:
    """发起 WDA 续期 job：重新下载市场产物 → 用签名配置重签 → 重装 → 验证。

    renew 与 prepare 走同一条产物解析路径（节点不留存上次产物，artifact=None
    会在节点侧 artifact_missing 即停）。签名配置未显式传时回退到 prepare 时
    落库的绑定（自动续签依赖此路径）。
    """
    import uuid
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")
    udid = ios_info.get("udid")
    device_id = data.get("device_id")

    if not node_id or not udid or not device_id:
        raise HTTPException(status_code=400, detail="设备缺少节点绑定信息")

    # 续期时若未显式传 signing_profile_id，回退到 prepare 时落库的绑定（自动续签依赖此路径）。
    signing_profile = await _resolve_signing_profile(
        body.signing_profile_id,
        str(user.id),
        fallback_binding_id=ios_info.get("signing_profile_id"),
    )
    # renew 复用市场同一产物（re-sign with a fresh profile, same artifact）。
    artifact = await _resolve_market_wda_asset()

    job_id = f"wda-{resource_id}-{uuid.uuid4().hex[:8]}"

    # bundle id 从绑定回退：renew 不带值（自动续签也不带），落到 prepare 时存的使用者 id。
    wda_bundle_id = _resolve_wda_bundle_id(body.wda_bundle_id, ios_info)

    signing_profile, wda_bundle_id = await _materialize_apple_id(
        signing_profile,
        owner_user_id=str(user.id),
        udid=udid,
        wda_bundle_id=wda_bundle_id,
    )

    client = get_node_client()
    try:
        result = await client.ios_start_wda_job(
            node_id,
            job_id,
            udid,
            device_id,
            action="renew",
            artifact=artifact,
            signing_profile=signing_profile,
            wda_bundle_id=wda_bundle_id,
            xctest_config_name=body.xctest_config_name,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
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
    """发起 WDA 重装 job：重新下载市场产物 → 用绑定签名配置重签 → 重装 → 验证。

    reinstall 同样需要产物 + 签名（节点不留存上次签名材料，每次 job 由服务端
    下发）。签名配置回退到 prepare 时落库的绑定。
    """
    import uuid
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

    device = await _owned_device(resource_id, user)
    data = device.get("data") or {}
    ios_info = data.get("ios") or {}
    node_id = ios_info.get("node_id")
    udid = ios_info.get("udid")
    device_id = data.get("device_id")

    if not node_id or not udid or not device_id:
        raise HTTPException(status_code=400, detail="设备缺少节点绑定信息")

    signing_profile = await _resolve_signing_profile(
        body.signing_profile_id,
        str(user.id),
        fallback_binding_id=ios_info.get("signing_profile_id"),
    )
    artifact = await _resolve_market_wda_asset()

    job_id = f"wda-{resource_id}-{uuid.uuid4().hex[:8]}"

    wda_bundle_id = _resolve_wda_bundle_id(body.wda_bundle_id, ios_info)

    signing_profile, wda_bundle_id = await _materialize_apple_id(
        signing_profile,
        owner_user_id=str(user.id),
        udid=udid,
        wda_bundle_id=wda_bundle_id,
    )

    client = get_node_client()
    try:
        result = await client.ios_start_wda_job(
            node_id,
            job_id,
            udid,
            device_id,
            action="reinstall",
            artifact=artifact,
            signing_profile=signing_profile,
            wda_bundle_id=wda_bundle_id,
            xctest_config_name=body.xctest_config_name,
        )
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
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
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

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
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
        if exc.code == "not_found":
            raise HTTPException(status_code=404, detail=exc.message) from exc
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc


@router.post("/devices/{resource_id}/wda/jobs/{job_id}/cancel")
async def cancel_wda_job(resource_id: int, job_id: str, user: User = Depends(get_current_user)) -> dict:
    """取消运行中的 WDA job。"""
    from .node_client import get_node_client, NodeServerUnavailable, RPCError

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
    except NodeServerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RPCError as exc:
        if exc.code == "failed_precondition":
            raise HTTPException(status_code=412, detail=exc.message) from exc
        raise HTTPException(status_code=500, detail=exc.message) from exc

