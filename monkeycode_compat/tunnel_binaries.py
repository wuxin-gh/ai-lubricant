"""Tunnel client binaries for the **main service's own machine**.

The node side downloads its own binaries through a POSIX ``sh`` snippet (see
``tunnel_node_dispatch._ensure_binary_script``) because the control plane can
only hand a node a command line. The main service has no such constraint, so it
resolves and caches binaries in pure Python instead:

* No shell, no ``curl``/``tar``/``sed`` — the slim Python image ships none of
  them and Windows has none of them either.
* One code path for Linux, macOS, and Windows: the platform only changes which
  release asset is picked and how it is unpacked (``.tar.gz`` vs ``.zip`` vs a
  bare binary).

Binaries land in a per-user cache dir and are reused across restarts. A scheme
may override the URL (``binary_url``) and pin a digest (``binary_sha256``) for
airgapped mirrors, mirroring the node-side contract.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import platform
import shutil
import stat
import tarfile
import tempfile
import zipfile
from pathlib import Path

import aiohttp
from loguru import logger

_FRP_VERSION = "0.61.1"
_NPS_VERSION = "0.26.10"
_DOWNLOAD_TIMEOUT = 300.0

# Per-kind release sources. ``{os}``/``{arch}`` are substituted with the values
# resolved by :func:`_platform_tokens`; ``member`` is the path inside an archive
# (ignored when ``archive`` is None).
_SOURCES: dict[str, dict] = {
    "frpc": {
        # fatedier/frp: frp_<ver>_<os>_<arch>.(tar.gz|zip) containing
        # frp_<ver>_<os>_<arch>/frpc[.exe]
        "url": f"https://github.com/fatedier/frp/releases/download/v{_FRP_VERSION}/frp_{_FRP_VERSION}_{{os}}_{{arch}}.{{ext}}",
        "member": f"frp_{_FRP_VERSION}_{{os}}_{{arch}}/frpc{{exe}}",
    },
    "cloudflared": {
        # cloudflared ships a bare per-platform binary, no archive.
        "url": "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-{os}-{arch}{exe}",
        "member": None,
    },
    "npc": {
        # ehang-io/nps: _<os>_<arch>_client.tar.gz containing npc[.exe]
        "url": f"https://github.com/ehang-io/nps/releases/download/v{_NPS_VERSION}/_{{os}}_{{arch}}_client.tar.gz",
        "member": "npc{exe}",
    },
}


class BinaryError(Exception):
    """The client binary could not be resolved, downloaded, or unpacked."""


def _platform_tokens() -> tuple[str, str, str]:
    """Return ``(os_token, arch_token, exe_suffix)`` for this machine."""
    system = platform.system().lower()
    if system.startswith("win"):
        os_token, exe = "windows", ".exe"
    elif system == "darwin":
        os_token, exe = "darwin", ""
    else:
        os_token, exe = "linux", ""
    machine = (platform.machine() or "").lower()
    if machine in ("x86_64", "amd64"):
        arch = "amd64"
    elif machine in ("aarch64", "arm64"):
        arch = "arm64"
    elif machine in ("i386", "i686", "x86"):
        arch = "386"
    else:
        arch = "amd64"
    return os_token, arch, exe


def _archive_kind(url: str) -> str | None:
    """``"tar.gz"``, ``"zip"``, or None for a bare binary, inferred from the URL."""
    lowered = url.lower()
    if lowered.endswith(".tar.gz") or lowered.endswith(".tgz"):
        return "tar.gz"
    if lowered.endswith(".zip"):
        return "zip"
    return None


def cache_dir() -> Path:
    """Where downloaded binaries live.

    Honours ``MC_TUNNEL_BIN_DIR`` first, then the platform's user cache dir so
    the download survives restarts without polluting the working tree.
    """
    override = os.environ.get("MC_TUNNEL_BIN_DIR", "").strip()
    if override:
        return Path(override)
    if platform.system().lower().startswith("win"):
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or str(Path.home())
        return Path(base) / "mc-tunnel-bins"
    return Path.home() / ".cache" / "mc-tunnel-bins"


def resolve_source(kind: str, config: dict | None = None) -> tuple[str, str | None, str | None]:
    """Return ``(url, archive_kind, member)`` for this platform.

    ``config.binary_url`` overrides the built-in release URL (``{os}``/``{arch}``/
    ``{exe}``/``{ext}`` placeholders are still substituted) so an operator can
    serve a private mirror.
    """
    source = _SOURCES.get((kind or "").strip())
    if source is None:
        raise BinaryError(f"unsupported tunnel client kind {kind!r}")
    os_token, arch, exe = _platform_tokens()
    config = config or {}
    template = str(config.get("binary_url") or source["url"])
    # frp publishes .zip for Windows and .tar.gz elsewhere.
    ext = "zip" if os_token == "windows" else "tar.gz"
    url = template.format(os=os_token, arch=arch, exe=exe, ext=ext)
    member_template = source["member"]
    member = (
        member_template.format(os=os_token, arch=arch, exe=exe, ext=ext)
        if member_template
        else None
    )
    return url, _archive_kind(url), member


def binary_name(kind: str) -> str:
    """The cached binary's file name, with ``.exe`` on Windows."""
    _, _, exe = _platform_tokens()
    return f"{kind}{exe}"


async def ensure_binary(kind: str, config: dict | None = None) -> Path:
    """Return a runnable path to ``kind``'s client, downloading it if missing.

    Idempotent: an already-cached, executable binary is returned untouched.
    A prepared binary is also accepted from ``PATH`` (``shutil.which``) so an
    operator can pre-install the client and skip the download entirely.
    """
    kind = (kind or "").strip()
    if kind not in _SOURCES:
        raise BinaryError(f"unsupported tunnel client kind {kind!r}")

    target = cache_dir() / binary_name(kind)
    if target.exists() and os.access(target, os.X_OK):
        logger.debug("[tunnel] {} binary already cached: {}", kind, target)
        return target

    on_path = shutil.which(kind)
    if on_path:
        logger.debug("[tunnel] {} binary found on PATH: {}", kind, on_path)
        return Path(on_path)

    url, archive, member = resolve_source(kind, config)
    expected_sha = str((config or {}).get("binary_sha256") or "").strip().lower()
    target.parent.mkdir(parents=True, exist_ok=True)

    # 主服务没有节点记录，下载代理跟随资源中心配置的代理池条目
    # （mp_config.settings.proxy_id，与市场/榜单同一条出网链路）。
    proxy_fields = await _resource_center_proxy_fields()
    proxy_mode = proxy_fields.get("proxy_mode") or ""
    if proxy_mode == "network":
        logger.info(
            "[tunnel] fetching {} for main service via network proxy (resource-center): {}",
            kind, url,
        )
    elif proxy_mode == "url_prefix":
        logger.info(
            "[tunnel] fetching {} for main service via url-prefix proxy (resource-center): {}",
            kind, url,
        )
    else:
        logger.info("[tunnel] fetching {} for main service (direct): {}", kind, url)

    # Download + unpack in a temp dir, then atomically move into place so a
    # crashed download never leaves a half-written binary in the cache.
    import time
    start_time = time.time()
    try:
        with tempfile.TemporaryDirectory(prefix="mc-tunnel-") as tmp:
            tmp_dir = Path(tmp)
            asset = tmp_dir / "asset"
            await _download(url, asset, proxy_fields)
            elapsed = time.time() - start_time
            size_mb = asset.stat().st_size / (1024 * 1024)
            logger.info("[tunnel] {} download complete: {:.2f} MB in {:.1f}s", kind, size_mb, elapsed)
            if expected_sha:
                logger.debug("[tunnel] verifying {} checksum", kind)
                actual = await asyncio.to_thread(_sha256, asset)
                if actual != expected_sha:
                    raise BinaryError(
                        f"{kind} checksum mismatch: {actual} != {expected_sha}"
                    )
                logger.debug("[tunnel] {} checksum verified", kind)
            extracted = await asyncio.to_thread(
                _unpack, asset, tmp_dir, archive, member, kind
            )
            _make_executable(extracted)
            # os.replace is atomic within a filesystem; the temp dir may be on a
            # different one, so copy then replace.
            staged = target.with_name(target.name + ".partial")
            await asyncio.to_thread(shutil.copy2, extracted, staged)
            _make_executable(staged)
            os.replace(staged, target)
        logger.info("[tunnel] {} binary ready: {}", kind, target)
    except Exception as exc:
        elapsed = time.time() - start_time
        logger.error("[tunnel] {} download failed after {:.1f}s: {}", kind, elapsed, exc)
        raise

    return target


async def _resource_center_proxy_fields() -> dict:
    """Resolve the resource-center proxy pool entry into proxy_* fields.

    The main service has no node record, so its tunnel-binary download follows
    the resource center's configured proxy (``mp_config.settings.proxy_id``) —
    the same egress the marketplace/leaderboard uses. Returns ``{}`` (direct)
    when unset, unknown, or resolution fails; the download never 500s on proxy
    misconfiguration.
    """
    try:
        from .marketplace import config as mp_config
        proxy_id = (mp_config.settings.proxy_id or "").strip()
        if not proxy_id:
            return {}
        from node_upgrade_targets import resolve_proxy, UpgradeTargetError
        return await resolve_proxy(proxy_id)
    except Exception as exc:  # noqa: BLE001 - download must degrade to direct
        logger.debug("[tunnel] resource-center proxy resolve failed: {}", exc)
        return {}


async def _download(url: str, dest: Path, proxy_fields: dict | None = None) -> None:
    """Download ``url`` to ``dest`` through the resolved proxy fields.

    ``network`` mode → aiohttp ``proxy=``; ``url_prefix`` mode → rewrite the
    URL to ``<prefix>/<url>`` (mirrors :func:`node_upgrade_targets.probe_asset`);
    empty/``direct`` → bare GET.
    """
    proxy_fields = proxy_fields or {}
    mode = proxy_fields.get("proxy_mode") or ""
    request_kwargs: dict = {}
    target = url
    if mode == "url_prefix":
        prefix = str(proxy_fields.get("proxy_url_prefix") or "").rstrip("/")
        if prefix:
            target = f"{prefix}/{url.lstrip('/')}"
    elif mode == "network":
        request_kwargs["proxy"] = proxy_fields.get("proxy_url")

    timeout = aiohttp.ClientTimeout(total=_DOWNLOAD_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(target, **request_kwargs) as response:
                if response.status >= 400:
                    raise BinaryError(f"download {url} returned HTTP {response.status}")
                with dest.open("wb") as handle:
                    async for chunk in response.content.iter_chunked(1 << 16):
                        handle.write(chunk)
    except aiohttp.ClientError as exc:
        raise BinaryError(f"download {url} failed: {exc}") from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest().lower()


def _unpack(
    asset: Path, work_dir: Path, archive: str | None, member: str | None, kind: str
) -> Path:
    """Extract ``member`` from ``asset``, or return the bare downloaded binary."""
    if archive is None:
        return asset
    out_dir = work_dir / "unpacked"
    out_dir.mkdir(parents=True, exist_ok=True)
    if archive == "tar.gz":
        with tarfile.open(asset, "r:gz") as tar:
            names = tar.getnames()
            chosen = _match_member(names, member, kind)
            info = tar.getmember(chosen)
            source = tar.extractfile(info)
            if source is None:
                raise BinaryError(f"{chosen!r} is not a regular file in archive")
            destination = out_dir / Path(chosen).name
            with source, destination.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            return destination
    if archive == "zip":
        with zipfile.ZipFile(asset) as zf:
            names = zf.namelist()
            chosen = _match_member(names, member, kind)
            destination = out_dir / Path(chosen).name
            with zf.open(chosen) as source, destination.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            return destination
    raise BinaryError(f"unsupported archive type {archive!r} for {kind}")


def _match_member(names: list[str], member: str | None, kind: str) -> str:
    """Pick the binary inside an archive.

    Prefers the exact expected path, then falls back to any entry whose base
    name matches the client (release layouts shift between versions, and an
    operator mirror may repackage).
    """
    if member and member in names:
        return member
    wanted = {binary_name(kind), kind}
    for name in names:
        if Path(name).name in wanted:
            return name
    raise BinaryError(
        f"{kind} binary not found in archive (looked for {member or kind!r})"
    )


def _make_executable(path: Path) -> None:
    """Add the executable bits on POSIX; a no-op on Windows."""
    if platform.system().lower().startswith("win"):
        return
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
