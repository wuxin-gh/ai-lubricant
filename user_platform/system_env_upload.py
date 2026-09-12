"""Ingest side of system-env archiving: the node POSTs its tar.gz here.

The archive flow is split in two because the bytes travel over HTTP while the
command travels over the node's gRPC stream (see the archive frame's docstring):

* the data service (:mod:`system_env_service`) issues a one-time upload token,
  tells the node where to POST, and after the node's ack finalizes the archive
  into a ready mirror + a team resource reference;
* this module owns the token mint/verify, the HTTP endpoint the node POSTs to,
  and the promote-to-mirror step both sides agree on.

Why a dedicated endpoint instead of reusing the admin mirror creation: that flow
starts from a git URL the server clones itself; here the content already exists
on the node, so the node ships it and the server only stores it. The token is
single-purpose (one market_id + module, short TTL, consumed on use) so a leaked
token cannot be replayed to overwrite an arbitrary mirror.
"""
from __future__ import annotations

import secrets
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from loguru import logger

router = APIRouter(prefix="/api/v1/resources/system-env", tags=["system-env"])

_MAX_UPLOAD_BYTES = 64 << 20  # 64 MiB — a skill/plugin tarball, not a workspace


def _repo_root() -> Path:
    """Repo root, matching ``server/resources_api.py``'s ``_BASE_DIR``.

    The mirror rows store ``archive_path`` relative to this, and the /fetch route
    resolves it the same way, so both sides must agree on the anchor.
    """
    return Path(__file__).resolve().parent.parent


def _mirror_root() -> Path:
    """``static/resource_mirrors`` — the only tree /fetch will serve from."""
    return _repo_root() / "static" / "resource_mirrors"


def _staged_path(module: str, market_id: str) -> Path:
    """Where the node's POST lands before finalization.

    Inside the mirror tree (under ``.staging``) rather than a separate directory,
    so promotion is a rename on the same filesystem instead of a copy.
    """
    return _mirror_root() / module / ".staging" / f"{_safe_name(market_id)}.tar.gz"


def _final_path(module: str, market_id: str) -> Path:
    return _mirror_root() / module / f"{_safe_name(market_id)}.tar.gz"


def _safe_name(value: str) -> str:
    """Same filename sanitization as ``resources_api._safe_name``."""
    return "".join(c if (c.isalnum() or c in "-_.@") else "_" for c in str(value))[:180]


def base_url() -> str:
    """The origin the node should POST its archive to.

    ``gateway_public_url`` is the public origin of THIS data service as reachable
    from a node — the same value task dispatch bakes into a runtime's LLM base URL
    — so the archive upload lands on the process that serves these routes. Using
    ``node_server_public_url`` here would point the node at the control plane,
    which serves no ``/api/v1/resources`` route at all.

    A remote node cannot reach the loopback default: ``gateway_public_url`` unset
    resolves to ``http://127.0.0.1:{port}``, and a node handed that URL POSTs to
    its OWN loopback and the upload dies in a fraction of a second. So the origin
    goes through ``gateway_base_url_for_nodes`` — which keeps an explicitly
    configured non-loopback origin verbatim and, for the loopback default with a
    non-loopback control plane, swaps in the control-plane host (same box, two
    ports) — the same rule MCP gateway URLs already use.
    """
    from . import config as compat_config
    from .config import gateway_base_url_for_nodes

    origin = gateway_base_url_for_nodes(compat_config.settings).strip().rstrip("/")
    if not origin:
        raise RuntimeError("gateway_public_url 未配置，无法生成节点上传地址")
    return origin


def _sign(payload: dict[str, str]) -> str:
    import hashlib
    import hmac
    import json

    from . import config as compat_config

    # Bound to the compat service's own DB URL: it is a per-deployment secret that
    # always exists (unlike an optional signing key), so tokens minted by one
    # deployment are worthless against another.
    secret = str(getattr(compat_config.settings, "database_url", "") or "system-env-upload")
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()


def issue_upload_token(market_id: str, module: str, *, ttl_seconds: int = 600) -> str:
    """Mint a short-TTL, single-purpose upload token.

    Format: ``seu_<base64url(payload)>.<hmac>``. The payload is embedded rather
    than stored, so no cleanup job is needed; the HMAC binds it to this
    market_id/module pair, making the token useless against any other mirror.
    ``.`` is the separator precisely because it is absent from the base64url
    alphabet, so the split is unambiguous.
    """
    import base64
    import json

    payload = {
        "market_id": market_id,
        "module": module,
        "exp": int(time.time()) + max(int(ttl_seconds), 60),
        "nonce": secrets.token_urlsafe(8),
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    encoded = base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")
    return f"seu_{encoded}.{_sign(payload)}"


def verify_upload_token(token: str) -> dict[str, Any]:
    """Verify and decode a minted token. Raises ValueError on any failure."""
    import base64
    import json

    token = (token or "").strip()
    if not token.startswith("seu_"):
        raise ValueError("上传凭据格式错误")
    encoded, sep, signature = token[4:].partition(".")
    if not sep or not encoded or not signature:
        raise ValueError("上传凭据格式错误")
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("上传凭据格式错误") from exc
    if not isinstance(payload, dict):
        raise ValueError("上传凭据格式错误")
    if not secrets.compare_digest(signature, _sign(payload)):
        raise ValueError("上传凭据无效")
    if int(payload.get("exp") or 0) < int(time.time()):
        raise ValueError("上传凭据已过期，请重新发起归档")
    return payload


async def finalize_upload(
    module: str, market_id: str, name: str
) -> tuple[str, int, dict[str, Any]]:
    """Promote the staged archive to a ready mirror row.

    Returns (archive_path relative to the repo root, size, manifest). The staged
    file is renamed into the canonical mirror name so the existing
    ``/api/v1/resources/{module}/fetch/{market_id}`` route serves it — that route
    resolves ``archive_path`` against the repo root and refuses anything outside
    ``static/resource_mirrors``, which is why staging happens inside that tree.
    """
    import hashlib

    import resource_mirror_store as store

    staged = _staged_path(module, market_id)
    if not staged.is_file():
        raise RuntimeError(f"节点上传的存档不存在: {staged}")

    final = _final_path(module, market_id)
    staged.replace(final)
    size = final.stat().st_size
    digest = hashlib.sha256(final.read_bytes()).hexdigest()

    mirror = await store.upsert_pending(
        module, market_id, name=name, version="", source_url="",
    )
    if mirror is None:
        raise RuntimeError("镜像存储不可用，无法归档")
    relative = final.relative_to(_repo_root()).as_posix()
    await store.mark_status(
        int(mirror["id"]), "ready",
        digest=digest, archive_path=relative, size_bytes=size,
    )
    manifest = {
        "id": market_id,
        "name": name,
        "display_name": name,
        "version": "",
        # Consumed by resolve_reference_specs when it builds the node wire spec.
        # Relative on purpose: the caller prefixes its own request origin, so one
        # archived resource works from every origin the console is reached on.
        "download_url": f"/api/v1/resources/{module}/fetch/{market_id}",
        "resource": {"source": "archive"},
        "source": "system_env_archive",
    }
    return relative, size, manifest


async def upsert_reference_from_manifest(
    *,
    team_id: str,
    module: str,
    market_id: str,
    name: str,
    manifest: dict[str, Any],
    created_by: str,
    archive_path: str,
) -> dict:
    """Create or refresh the team resource reference pointing at the archive."""
    from .resource_reference_service import upsert_reference

    enriched = dict(manifest)
    enriched["id"] = market_id
    enriched["archive_path"] = archive_path
    return await upsert_reference(
        team_id,
        enriched,
        market_module=module,
        created_by=created_by,
        owned_entity_type="system_env_archive",
        owned_entity_id=market_id,
    )


@router.post("/upload")
async def upload_system_env_archive(
    request: Request,
    authorization: str | None = Header(None),
):
    """Receive the tar.gz the node POSTed and stage it for finalization.

    Auth is the one-time token minted by :func:`issue_upload_token` (Bearer, or a
    ``?token=`` query fallback — the node's archive POST sets the header). The
    market_id/module live INSIDE the signed token, so a caller cannot point one
    token at another mirror: the body is staged exactly where the token says.
    """
    from urllib.parse import parse_qs

    header_token = (authorization or "").strip()
    bearer = header_token[7:].strip() if header_token.lower().startswith("bearer ") else ""
    query = parse_qs(request.url.query)
    token = bearer or (query.get("token", [""])[0] or "").strip()
    payload = verify_upload_token(token)
    if payload["module"] not in ("skills", "plugins"):
        raise HTTPException(status_code=400, detail=f"不支持的资源类型 {payload['module']}")

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="上传内容为空")
    if len(data) > _MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="归档超过大小上限（64 MiB）")
    if data[:2] != b"\x1f\x8b":
        raise HTTPException(status_code=400, detail="归档必须是 tar.gz")

    ingest = _staged_path(payload["module"], payload["market_id"])
    ingest.parent.mkdir(parents=True, exist_ok=True)
    ingest.write_bytes(data)
    logger.info("[system-env] staged upload {} ({} bytes)", ingest, len(data))
    return {"staged": True, "bytes": len(data)}
