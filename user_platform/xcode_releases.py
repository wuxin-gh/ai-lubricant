"""xcodereleases.com 目录解析：Xcode 版本 → .xip 直链下载目标。

异步 Xcode 安装任务的目标解析在**服务端**完成（与 runtime/Node.js 升级同
职责划分）：节点只拿到 frame 里的 https .xip 直链，不接触目录源、不做版本
选择。数据源是 xcodereleases.com 的公开 ``data.json``（版本、下载直链、最低
macOS），无需 App Store / Apple ID —— 2026-09 定稿，替代旧的「只检测、无法
自动安装」流程。

本模块只做两件事：带缓存地拉目录（TTL 6h，失败时退回上次快照并打 stale
标记），以及纯函数 ``resolve_from_data`` 做版本匹配/筛选。解析防御式：没有
可用 .xip 直链的条目（例如指向 developer.apple.com 需登录的链接）一律跳过，
节点侧的 HTTPS+.xip 强校验是最后一道防线。
"""
from __future__ import annotations

import time
from typing import Any

import aiohttp

XCODE_RELEASES_URL = "https://xcodereleases.com/data.json"
_CACHE_TTL_SECONDS = 6 * 3600
_FETCH_TIMEOUT_SECONDS = 30

_CACHE: dict[str, Any] = {"fetched_at": 0.0, "releases": []}


class XcodeReleasesError(Exception):
    """A target could not be resolved. Message is user-facing."""


def _version_parts(text: str) -> tuple[int, int]:
    """Parse the leading major.minor out of "16.0" / "16.0 beta 2" → (16, 0)."""
    digits = ""
    dot = False
    for ch in str(text or "").strip():
        if ch.isdigit():
            digits += ch
        elif ch == "." and not dot and digits:
            dot = True
            digits += ch
        else:
            break
    parts = digits.strip(".").split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        return major, minor
    except (ValueError, IndexError):
        return (0, 0)


def _normalize_entry(raw: dict) -> dict | None:
    """One data.json entry → our release shape, or None when unusable.

    An entry without a directly-downloadable https .xip is useless to the node
    (it enforces the same rule) — skip it rather than dispatch a job that can
    only fail.
    """
    version = raw.get("version") if isinstance(raw, dict) else None
    if not isinstance(version, dict):
        return None
    number = str(version.get("number") or "").strip()
    if not number:
        return None
    links = raw.get("links") if isinstance(raw.get("links"), dict) else {}
    download = links.get("download") if isinstance(links.get("download"), dict) else {}
    url = str(download.get("url") or "").strip()
    if not url.lower().startswith("https://") or not url.lower().endswith(".xip"):
        return None
    size = 0
    try:
        size = int(float(download.get("size") or 0))
    except (TypeError, ValueError):
        size = 0
    return {
        "version": number,
        "build": str(version.get("build") or "").strip(),
        "beta": bool(version.get("beta")),
        "requires": str(raw.get("requires") or "").strip(),
        "download_url": url,
        "size_bytes": max(size, 0),
    }


def resolve_from_data(
    raw_releases: list,
    desired: str,
    *,
    allow_beta: bool = False,
    macos_version: str = "",
    stale: bool = False,
) -> dict:
    """Pure resolver over a data.json list (newest-first, per the source).

    ``desired``: "latest" = newest qualifying stable release, or an explicit
    version number ("16.0"). Beta releases are skipped unless allow_beta (an
    explicit beta number counts as explicit). When ``macos_version`` is given
    ("15.4"), releases requiring a newer macOS are skipped for "latest" and
    rejected with a clear error for an explicit version.
    """
    entries = [e for e in (_normalize_entry(r) for r in raw_releases or []) if e]
    if not entries:
        raise XcodeReleasesError("xcodereleases 目录中没有可直接下载的 .xip 条目")

    wanted = str(desired or "latest").strip()
    if wanted.lower().startswith("xcode"):
        wanted = wanted[len("xcode"):].strip()

    node_parts = _version_parts(macos_version) if macos_version else None

    def macos_ok(entry: dict) -> bool:
        if not node_parts:
            return True
        req = _version_parts(entry.get("requires"))
        return node_parts >= req

    if wanted.lower() != "latest":
        for entry in entries:
            if entry["version"] == wanted:
                # An explicit version is an explicit choice — betas selectable
                # by number; only the macOS gate still applies.
                if not macos_ok(entry):
                    raise XcodeReleasesError(
                        f"Xcode {entry['version']} 需要 macOS {entry['requires']}，"
                        f"节点当前是 {macos_version}，无法安装该版本"
                    )
                return _result(entry, stale)
        raise XcodeReleasesError(f"xcodereleases 目录中找不到 Xcode {wanted}")

    for entry in entries:
        if entry["beta"] and not allow_beta:
            continue
        if not macos_ok(entry):
            continue
        return _result(entry, stale)
    raise XcodeReleasesError(
        "没有满足条件的 Xcode 版本（无 beta 且兼容当前 macOS）"
    )


def _result(entry: dict, stale: bool) -> dict:
    return {
        "target_version": entry["version"],
        "download_url": entry["download_url"],
        "download_size_bytes": entry["size_bytes"],
        "requires_macos": entry["requires"],
        "beta": entry["beta"],
        "stale": stale,
    }


async def _load_releases(proxy_fields: dict | None = None) -> tuple[list, bool]:
    """Fetch (or serve from cache) the release catalog. Returns (raw, stale)."""
    now = time.monotonic()
    if _CACHE["releases"] and now - _CACHE["fetched_at"] < _CACHE_TTL_SECONDS:
        return _CACHE["releases"], False

    fields = proxy_fields or {}
    mode = fields.get("proxy_mode") or ""
    target = XCODE_RELEASES_URL
    request_kwargs: dict[str, Any] = {}
    if mode == "url_prefix":
        prefix = str(fields.get("proxy_url_prefix") or "").rstrip("/")
        target = f"{prefix}/{XCODE_RELEASES_URL.lstrip('/')}"
    elif mode == "network":
        request_kwargs["proxy"] = fields.get("proxy_url")
    timeout = aiohttp.ClientTimeout(total=_FETCH_TIMEOUT_SECONDS)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                target, headers={"User-Agent": "ai-lubricant-node-upgrade"}, **request_kwargs
            ) as response:
                if response.status != 200:
                    raise XcodeReleasesError(f"Xcode 目录拉取失败：HTTP {response.status}")
                data = await response.json(content_type=None)
    except XcodeReleasesError:
        raise
    except Exception as exc:
        # A stale snapshot beats a hard failure — the operator still gets a
        # resolution; the stale flag lets the UI say the list may be old.
        if _CACHE["releases"]:
            return _CACHE["releases"], True
        raise XcodeReleasesError(f"Xcode 目录拉取失败：{exc}") from exc

    if not isinstance(data, list):
        raise XcodeReleasesError("Xcode 目录格式无效（期望数组）")
    _CACHE["releases"] = data
    _CACHE["fetched_at"] = now
    return data, False


async def resolve(
    desired: str = "",
    *,
    proxy_fields: dict | None = None,
    macos_version: str = "",
    allow_beta: bool = False,
) -> dict:
    """Resolve ``desired`` ("latest" or explicit version) into a .xip target."""
    try:
        raw, stale = await _load_releases(proxy_fields)
    except XcodeReleasesError:
        raise
    return resolve_from_data(
        raw, desired, allow_beta=allow_beta, macos_version=macos_version, stale=stale
    )
