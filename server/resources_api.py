"""市场资源镜像 API（Phase 2）。

admin 在资源页点「镜像」后，服务端把市场 skill/插件从原始来源拉下来打成 tar.gz 存档；
节点在会话启动时不再直连 GitHub，而是从我们服务端拉这个存档。

收益：预下载（admin 提前备好）、跨会话共享缓存（节点按 digest 缓存）、不依赖外网、
可鉴权（fetch_token）。

下发复用 mcp/api.py::download_service_asset 的 StreamingResponse 范式。
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field

import resource_mirror_store as store

router = APIRouter(prefix="/api/v1/resources", tags=["resources"])

from project_paths import repo_root as _repo_root

_BASE_DIR = _repo_root()
# 存档目录。放 static 下便于运维查看，但**不经 /static 直接暴露**——下发一律走
# /fetch 路由校验 fetch_token，避免存档被匿名拖走。
_MIRROR_ROOT = _BASE_DIR / "static" / "resource_mirrors"


async def _require_admin(authorization: str | None) -> None:
    from admin import _require_admin as _ra
    await _ra(authorization)


class MirrorRequest(BaseModel):
    market_id: str = Field(..., min_length=1, max_length=200)
    name: str = ""
    version: str = ""
    source_url: str = ""
    source_ref: str = ""
    source_path: str = ""


def _safe_name(value: str) -> str:
    """把 market_id/version 变成安全文件名（避免路径穿越）。"""
    return "".join(c if (c.isalnum() or c in "-_.@") else "_" for c in value)[:180]


def _archive_path(module: str, market_id: str, version: str) -> Path:
    tag = _safe_name(market_id) + ("@" + _safe_name(version) if version else "")
    return _MIRROR_ROOT / module / f"{tag}.tar.gz"


def _run_git_clone(url: str, ref: str, dest: Path) -> None:
    """浅克隆到 dest。失败抛 RuntimeError（带 stderr 便于前端显示原因）。"""
    args = ["git", "clone", "--depth", "1"]
    if ref:
        args += ["--branch", ref]
    args += [url, str(dest)]
    proc = subprocess.run(args, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "git clone failed").strip()[:2000])


def _pack_tar_gz(src_dir: Path, out_path: Path) -> tuple[str, int]:
    """把 src_dir 打成 tar.gz 到 out_path，返回 (sha256, size)。

    排除 .git：节点只需要内容，带上 .git 会让存档大出数倍。

    **必须可复现**：digest 是节点侧的缓存键，同内容必须得到同 digest，否则每次镜像
    刷新都会让所有节点缓存失效、退化成每次重下。因此：
      - gzip 头的 mtime 固定为 0（默认会写当前时间）
      - tar 成员的 mtime/uid/gid/uname/gname 全部归一
      - 成员按名字排序（os.scandir 顺序不保证）
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")

    def _filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        parts = Path(info.name).parts
        if ".git" in parts:
            return None
        # 归一元数据，保证可复现
        info.mtime = 0
        info.uid = 0
        info.gid = 0
        info.uname = ""
        info.gname = ""
        return info

    # gzip mtime=0：否则头部写入当前时间，同内容也会得到不同 digest
    with open(tmp, "wb") as raw:
        import gzip

        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w", format=tarfile.GNU_FORMAT) as tf:
                for child in sorted(src_dir.iterdir(), key=lambda p: p.name):
                    tf.add(child, arcname=child.name, filter=_filter, recursive=True)
    digest = hashlib.sha256(tmp.read_bytes()).hexdigest()
    size = tmp.stat().st_size
    tmp.replace(out_path)
    return digest, size


async def _do_mirror(mirror_id: int, module: str, req: MirrorRequest) -> None:
    """后台镜像任务：clone -> 取子树 -> 打包 -> 标 ready。失败标 error。"""
    await store.mark_status(mirror_id, "downloading")
    tmp_root: str | None = None
    try:
        if not req.source_url.strip():
            raise RuntimeError("缺少 source_url，无法镜像（市场 manifest 未提供来源地址）")
        tmp_root = tempfile.mkdtemp(prefix="mirror_")
        clone_dir = Path(tmp_root) / "repo"
        await asyncio.to_thread(_run_git_clone, req.source_url.strip(), req.source_ref.strip(), clone_dir)

        # resource.path 指定了仓库内子路径时只镜像该子树（如 skills 目录）
        src = clone_dir
        if req.source_path.strip():
            candidate = (clone_dir / req.source_path.strip()).resolve()
            # 防路径穿越：子路径必须仍在 clone_dir 内
            if not str(candidate).startswith(str(clone_dir.resolve())):
                raise RuntimeError(f"source_path 非法: {req.source_path}")
            if not candidate.exists():
                raise RuntimeError(f"仓库内不存在该子路径: {req.source_path}")
            src = candidate

        out = _archive_path(module, req.market_id, req.version)
        digest, size = await asyncio.to_thread(_pack_tar_gz, src, out)
        await store.mark_status(
            mirror_id, "ready",
            digest=digest, archive_path=str(out.relative_to(_BASE_DIR)), size_bytes=size,
        )
        logger.info("[mirror] {} {} ready, {} bytes, sha256={}", module, req.market_id, size, digest[:12])
    except Exception as e:
        logger.warning("[mirror] {} {} failed: {}", module, req.market_id, e)
        await store.mark_status(mirror_id, "error", error=str(e))
    finally:
        if tmp_root:
            shutil.rmtree(tmp_root, ignore_errors=True)


@router.get("/{module}/mirrors")
async def list_mirrors(module: str, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    if module not in store.ALLOWED_MODULES:
        raise HTTPException(400, f"unsupported module: {module}")
    items = await store.list_mirrors(module)
    return {"code": 0, "message": "ok", "data": items}


@router.post("/{module}/mirror")
async def create_mirror(module: str, req: MirrorRequest, authorization: str | None = Header(None)) -> dict:
    """建镜像并后台开始拉取。已存在则复位重拉。"""
    await _require_admin(authorization)
    if module not in store.ALLOWED_MODULES:
        raise HTTPException(400, f"unsupported module: {module}")
    row = await store.upsert_pending(
        module, req.market_id,
        name=req.name, version=req.version,
        source_url=req.source_url, source_ref=req.source_ref, source_path=req.source_path,
    )
    if not row:
        raise HTTPException(503, "Database not available")
    asyncio.create_task(_do_mirror(int(row["id"]), module, req))
    return {"code": 0, "message": "ok", "data": row}


@router.post("/{module}/mirror/{mirror_id}/refresh")
async def refresh_mirror(module: str, mirror_id: int, authorization: str | None = Header(None)) -> dict:
    """按原来源重新拉取（digest 变了就换存档）。"""
    await _require_admin(authorization)
    row = await store.get_mirror(mirror_id)
    if not row or row.get("module") != module:
        raise HTTPException(404, "mirror not found")
    req = MirrorRequest(
        market_id=row["market_id"], name=row.get("name") or "", version=row.get("version") or "",
        source_url=row.get("source_url") or "", source_ref=row.get("source_ref") or "",
        source_path=row.get("source_path") or "",
    )
    asyncio.create_task(_do_mirror(mirror_id, module, req))
    return {"code": 0, "message": "ok", "data": {"id": mirror_id, "status": "downloading"}}


@router.delete("/{module}/mirror/{mirror_id}")
async def delete_mirror(module: str, mirror_id: int, authorization: str | None = Header(None)) -> dict:
    await _require_admin(authorization)
    row = await store.get_mirror(mirror_id)
    if not row or row.get("module") != module:
        raise HTTPException(404, "mirror not found")
    archive = row.get("archive_path")
    if archive:
        target = (_BASE_DIR / archive).resolve()
        # 只删镜像根目录下的文件，避免 archive_path 被污染后误删
        if str(target).startswith(str(_MIRROR_ROOT.resolve())) and target.is_file():
            target.unlink(missing_ok=True)
    ok = await store.delete_mirror(mirror_id)
    return {"code": 0, "message": "ok", "data": {"deleted": ok}}


@router.get("/{module}/spec/{market_id}")
async def mirror_reference_spec(
    module: str,
    market_id: str,
    request: Request,
    version: str = Query(""),
    authorization: str | None = Header(None),
) -> dict:
    """返回指向服务端存档的引用 spec（SkillSpec / NodePluginSpec 形态）。

    这是「引用已镜像资源」的唯一正确出口：fetch_token 是明文凭据，不能经列表接口
    回传（见 store.row_to_dict 只回 has_token），只在这里按需下发。

    节点据 source='archive' 走 HTTP 拉 tar.gz 而非 git clone，因此 path/ref 必须留空
    ——否则节点会把 path 当本地目录、把 ref 当 git 分支处理。
    """
    await _require_admin(authorization)
    if module not in store.ALLOWED_MODULES:
        raise HTTPException(400, f"unsupported module: {module}")
    row = await store.find_mirror(module, market_id, version)
    if not row:
        raise HTTPException(404, "mirror not found")
    if row.get("status") != "ready":
        raise HTTPException(409, f"mirror not ready (status={row.get('status')})")
    token = await store.get_mirror_secret(int(row["id"]))
    base = str(request.base_url).rstrip("/")
    url = f"{base}/api/v1/resources/{module}/fetch/{market_id}"
    if version:
        url += f"?version={version}"
    spec = {
        "name": row.get("name") or market_id,
        # archive：节点走 HTTP + 解 tar，并按 digest 做节点级缓存
        "source": "archive",
        "url": url,
        # 必须留空：archive 形态下 path/ref 是 git 语义，填了会走错分支
        "path": "",
        "ref": "",
        "token": token,
    }
    return {"code": 0, "message": "ok", "data": {"spec": spec, "digest": row.get("digest") or ""}}


@router.get("/{module}/fetch/{market_id}")
async def fetch_mirror(
    module: str,
    market_id: str,
    version: str = Query(""),
    token: str = Query(""),
    authorization: str | None = Header(None),
) -> StreamingResponse:
    """节点拉取存档。凭 fetch_token 鉴权（也接受 admin 鉴权，便于 curl 验证）。

    响应头带 X-Resource-Digest，节点据此校验内容并作为本地缓存键。
    """
    if module not in store.ALLOWED_MODULES:
        raise HTTPException(400, f"unsupported module: {module}")
    fetch_token = token.strip()
    auth = (authorization or "").strip()
    if not fetch_token and auth.lower().startswith("bearer "):
        fetch_token = auth[7:].strip()
    row = await store.verify_fetch_token(module, market_id, fetch_token)
    if not row:
        # token 不过时允许 admin 直接取（人工验证/排障用）
        await _require_admin(authorization)
        found = await store.find_mirror(module, market_id, version)
        if not found or found.get("status") != "ready":
            raise HTTPException(404, "mirror not ready")
        row = found

    archive = row.get("archive_path") or ""
    target = (_BASE_DIR / archive).resolve()
    if not str(target).startswith(str(_MIRROR_ROOT.resolve())) or not target.is_file():
        raise HTTPException(404, "archive missing")
    data = target.read_bytes()
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{_safe_name(market_id)}.tar.gz"',
            "X-Resource-Digest": row.get("digest") or "",
        },
    )
