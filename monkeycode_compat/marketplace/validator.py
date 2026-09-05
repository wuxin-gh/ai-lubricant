"""市场 manifest 校验与导入体归一化。

从原 Cloudflare Worker 实现原样移植（该目录已删除），保持契约不变：
PG（marketplace_items）里的 manifest 是编辑真相源，GitHub 文件是 publisher 渲染的
发布镜像；manifest 只描述资源本身，不声明安装目标。安装到平台还是个人由
Ai Lubricant 的操作入口决定，与本模块无关。

校验在写 store 之前执行，坏数据不落库；错误按字段返回，供管理页逐项展示。
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

MARKET_INDEX_SCHEMA = "ai-lubricant.market.index.v1"
MARKET_EXPORT_SCHEMA = "ai-lubricant.market.export.v1"

_ITEM_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_DIGEST_RE = re.compile(r"^sha256:[a-fA-F0-9]{64}$")

_MCP_TRANSPORTS = ("sse", "streamable-http")
_NODE_PLATFORMS = ("windows", "linux", "darwin")
_NODE_ARCHES = ("amd64", "arm64")
_NODE_COMPONENTS = ("node", "agent-compose")
_NODE_ASSET_ROLES = ("execution", "management", "runtime")
_NODE_VERSION_SCHEMA = "ai-lubricant.node-version/v1"
# 2026-08 品牌改名前发布的旧市场文件仍写 model-api.*；校验双认避免旧仓库被拒。
_NODE_VERSION_SCHEMA_LEGACY = "model-api.node-version/v1"
_NODE_RELEASE_SCHEMA = "ai-lubricant.node-release/v1"
_NODE_RELEASE_SCHEMA_LEGACY = "model-api.node-release/v1"
# 移动端（控制 App）发行：与节点走同一套仓库/上传/缓存骨架，但独立管线。
# version.json 是「最新正式发行版本」的指针，Android 走下载安装，iOS 只给商店链接。
_MOBILE_VERSION_SCHEMA = "ai-lubricant.mobile-version/v1"
_MOBILE_VERSION_SCHEMA_LEGACY = "model-api.mobile-version/v1"
_MOBILE_RELEASE_SCHEMA = "ai-lubricant.mobile-release/v1"
_MOBILE_RELEASE_SCHEMA_LEGACY = "model-api.mobile-release/v1"
# 设备控制 App 发行：与 mobile 同骨架但独立模块与 schema（被控端 vs 控制端）
_DEVICE_CONTROL_VERSION_SCHEMA = "ai-lubricant.device-control-version/v1"
_DEVICE_CONTROL_RELEASE_SCHEMA = "ai-lubricant.device-control-release/v1"
_CHANNEL_TEMPLATE_SCHEMA = "ai-lubricant.channel-template/v1"
_CHANNEL_TEMPLATE_SCHEMA_LEGACY = "model-api.channel-template/v1"
_CHANNEL_ICON_KEYS = {
    "bot", "brain", "cloud", "code", "cpu", "database", "flame", "globe",
    "json", "layers", "message", "rocket", "server", "sparkles", "terminal", "wand",
}
# 节点版本下载地址禁止携带 query string：GitHub release assets 本就无 query，
# 出现 query 几乎一定是带签名 token 的临时直链，不该入库。
_FORBIDDEN_QUERY_KEYS = ("token", "sig", "signature", "key", "access", "secret", "auth")
_CHANNEL_FORBIDDEN_KEYS = {
    "accounts", "api_key", "apikey", "password", "token", "cookie", "cookies",
    "authorization", "proxy_password", "provider_id", "header_template",
    "system_version", "node_version", "node_program_version",
}


def _find_forbidden_key(value: Any, path: str = "manifest") -> str | None:
    if isinstance(value, list):
        for index, item in enumerate(value):
            found = _find_forbidden_key(item, f"{path}[{index}]")
            if found:
                return found
        return None
    if not isinstance(value, dict):
        return None
    for key, item in value.items():
        if str(key).lower() in _CHANNEL_FORBIDDEN_KEYS:
            return f"{path}.{key}"
        found = _find_forbidden_key(item, f"{path}.{key}")
        if found:
            return found
    return None


def safe_item_id(item_id: str) -> str | None:
    """把 manifest id 收敛成安全文件名；非法返回 None。

    与 Worker 同规则：长度 <=200、仅字母数字点下划线连字符、不含 ``..``（路径穿越）。
    """
    if not item_id or len(item_id) > 200:
        return None
    if not _ITEM_ID_RE.match(item_id):
        return None
    if ".." in item_id:
        return None
    return item_id


def is_https_url(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        return urlparse(value).scheme == "https"
    except Exception:
        return False


def is_http_url(value: Any) -> bool:
    """完整 HTTP/HTTPS URL，用于本地渠道 website_url/icon（marketplace 仍要求 HTTPS）。"""
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        return urlparse(value).scheme in {"http", "https"} and bool(urlparse(value).hostname)
    except Exception:
        return False


_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-(?:[0-9A-Za-z-]+)(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)

# 节点发行版本号：日期 + 后缀，每次打包都是新版本。
# 形如 20260813-1430、20260813-1430-5（同分钟多次可再加序号）。
# 也兼容旧的 semver，避免历史已发布版本被新校验拒掉。
_NODE_VERSION_RE = re.compile(r"^\d{8}-\d{2,4}(?:-\d+)?$")


def _is_semver(value: str) -> bool:
    """宽松 semver：要求 ``MAJOR.MINOR.PATCH``，允许预发布号与 build metadata。"""
    return bool(_SEMVER_RE.match(value.strip()))


def _is_node_version(value: str) -> bool:
    """节点发行版本号：接受 ``YYYYMMDD-HHMM[-序号]`` 日期后缀格式，或宽松 semver。

    日期后缀格式保证每次打包都是新版本（同一天多次打靠时分区分），同时兼容
    历史 semver 版本号，不破坏已发布的版本。
    """
    v = value.strip()
    return bool(_NODE_VERSION_RE.match(v) or _is_semver(v))



def _url_query(value: Any) -> list[str]:
    """返回 URL 的 query key 列表；非法 URL 返回空。"""
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = urlparse(value)
    except Exception:
        return []
    if not parsed.query:
        return []
    return [segment.split("=", 1)[0] for segment in parsed.query.split("&") if segment]


# ── 资产入仓路径（repo_path）校验 ──────────────────────────────────────────────
#
# 发行资产直接提交进市场仓库（node-releases/{version}/files/...），manifest 记
# 仓库相对路径而非绝对下载地址——Gitee 镜像走 git fetch 同步原生带走二进制，
# 消费端按自己的平台（github/gitee）渲染绝对 URL。旧资产只有 GitHub Release
# 直链 download_url、没有 repo_path，双口径并存。
_REPO_PATH_RE = re.compile(r"^[A-Za-z0-9._\-/]+$")


def validate_repo_path(value: Any, *, prefix: str) -> str:
    """校验资产的仓库相对路径，返回错误消息（空串 = 通过）。

    ``prefix`` 限定资产必须存放的顶层目录（node-releases/ 或 mobile-releases/），
    防止把市场外/任意路径写进 manifest。
    """
    path = str(value or "").strip()
    if not path:
        return "repo_path 不能为空"
    if path.startswith("/") or ".." in path.split("/") or path != path.strip():
        return f"repo_path 必须是相对路径且不含 ..（应为 {prefix}<版本>/files/<文件>）"
    if not _REPO_PATH_RE.match(path):
        return "repo_path 只能包含字母、数字、点、下划线、连字符和斜杠"
    if not path.startswith(prefix):
        return f"repo_path 必须位于 {prefix} 目录下"
    return ""


# 通用 Runtime 包：JS runtime 平台无关（节点用宿主 Node.js 跑 dist/cli.js），
# 只打一份 node-runtime.tar.gz，不再按 (os, arch) 出 6 份重复归档。
# 旧命名 agent-compose-runtime-<os>-<arch>.tar.gz 仍识别（存量市场仓库兼容），
# 但新上传只产出通用名。
_NODE_ASSET_RE = re.compile(
    r"^(node-execution|agent-compose-node-management|node-ios)-"
    r"(linux|darwin|windows)-(amd64|arm64)(\.exe)?$"
)
_LEGACY_RUNTIME_ASSET_RE = re.compile(
    r"^agent-compose-runtime-(linux|darwin|windows)-(amd64|arm64)\.tar\.gz$"
)
_RUNTIME_ASSET_RE = re.compile(r"^node-runtime\.tar\.gz$")


def identify_node_asset(filename: str) -> dict:
    """按构建产物命名规范识别节点发行资产，识别失败返回空 dict。"""
    text = (filename or "").strip()
    match = _NODE_ASSET_RE.match(text)
    if match:
        base, platform, arch, exe = match.groups()
        if base == "node-execution":
            role = "execution"
        elif base == "node-ios":
            role = "ios_host"
        else:
            role = "management"
        if platform == "windows" and not exe:
            return {}
        if platform != "windows" and exe:
            return {}
        return {"component": "node", "role": role, "platform": platform, "arch": arch, "format": "executable"}
    if _RUNTIME_ASSET_RE.match(text):
        # 通用包：platform/arch 恒为 any，所有非 ios_host 角色共用这一份。
        return {"component": "agent-compose", "role": "runtime", "platform": "any", "arch": "any", "format": "tar.gz"}
    legacy = _LEGACY_RUNTIME_ASSET_RE.match(text)
    if legacy:
        return {"component": "agent-compose", "role": "runtime", "platform": legacy.group(1), "arch": legacy.group(2), "format": "tar.gz"}
    return {}


def validate_node_assets(assets: Any) -> list[str]:
    """校验节点版本 manifest 的 assets 数组。"""
    errors: list[str] = []
    if not isinstance(assets, list) or not assets:
        errors.append("assets 必须是非空数组")
        return errors
    seen: set[tuple] = set()
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict):
            errors.append(f"assets[{index}] 必须是对象")
            continue
        ident = identify_node_asset(asset.get("filename"))
        if not ident:
            errors.append(f"assets[{index}].filename 不符合节点发行产物命名规范")
            continue
        for key in ("component", "role", "platform", "arch", "format"):
            if asset.get(key) != ident[key]:
                errors.append(f"assets[{index}].{key} 与文件名不一致（应为 {ident[key]}）")
        # 入仓资产（repo_path）免 download_url 必填：绝对地址由消费侧按平台渲染。
        # 旧资产（GitHub Release 直链时代）仍要求 download_url。
        repo_path = str(asset.get("repo_path") or "").strip()
        if repo_path:
            path_error = validate_repo_path(repo_path, prefix="node-releases/")
            if path_error:
                errors.append(f"assets[{index}].{path_error}")
        if not repo_path and not is_https_url(asset.get("download_url")):
            errors.append(f"assets[{index}].download_url 必须是 HTTPS URL")
        elif is_https_url(asset.get("download_url")):
            query = _url_query(asset.get("download_url"))
            if query:
                leaked = [k for k in query if any(needle in k.lower() for needle in _FORBIDDEN_QUERY_KEYS)]
                if leaked:
                    errors.append(f"assets[{index}].download_url 不得携带可能含 token 的 query 字段: {sorted(set(leaked))}")
        digest = asset.get("digest")
        if not digest or not _DIGEST_RE.match(str(digest)):
            errors.append(f"assets[{index}].digest 必须是 sha256: 加 64 位十六进制")
        size = asset.get("size_bytes")
        if not isinstance(size, int) or size <= 0:
            errors.append(f"assets[{index}].size_bytes 必须是正整数")
        # version.json 合并了多个版本的资产，每条 asset 带它来源版本的 version；
        # 单版本 manifest 里该字段不存在也允许，存在时必须合法。
        asset_version = asset.get("version")
        if asset_version is not None and str(asset_version) != "":
            if not _is_node_version(str(asset_version)):
                errors.append(f"assets[{index}].version 不是合法的节点发行版本号")
        key = (ident["role"], ident["platform"], ident["arch"])
        if key in seen:
            errors.append(f"assets[{index}] 与其他资产重复（role/platform/arch）")
        seen.add(key)
    return errors


def validate_node_release(data: Any) -> list[str]:
    """校验 ``node-releases/version.json`` —— 升级链路的唯一真相源。

    与 node-versions manifest 不同：这不是一条可枚举的市场资源，而是仓库当前
    "最新正式发行版本"的汇总指针。只反映 published、非测试版本；消费侧据此
    决定节点是否需要升级。
    """
    if not isinstance(data, dict):
        return ["version.json 必须是一个对象"]
    errors: list[str] = []
    if data.get("schema") not in (_NODE_RELEASE_SCHEMA, _NODE_RELEASE_SCHEMA_LEGACY):
        errors.append(f'schema 必须为 "{_NODE_RELEASE_SCHEMA}"')
    version = _text(data.get("version"))
    if not version:
        errors.append("version 必填")
    elif not _is_node_version(version):
        errors.append("version 必须是日期后缀格式（YYYYMMDD-HHMM）或 semver（1.2.3）")
    if not _text(data.get("version_notes")):
        errors.append("version_notes 必填")
    errors.extend(validate_node_assets(data.get("assets")))
    forbidden = _find_forbidden_key(data)
    if forbidden:
        errors.append(f"version.json 禁止携带敏感或本地字段: {forbidden}")
    return errors


# ── 移动端（控制 App）发行 ────────────────────────────────────────────────────
#
# 与节点发行同构：一个 version.json 指针 + 每版本 manifest。命名契约只认 Android
# 的 APK（iOS 走 App Store，不入 assets）。版本号沿用移动端 app.json 的可比较
# 数字/日期串（如 260608），不引入 semver。

# ai-lubricant-<version>-android.apk —— version 为数字/日期串（≥1 位数字，可含 - 分隔）
_MOBILE_ASSET_RE = re.compile(r"^ai-lubricant-(\d[\w.-]*)-android\.apk$")
# 移动端版本号：纯数字或数字加日期后缀（与 app.json version 一致，字符串可比较）。
_MOBILE_VERSION_RE = re.compile(r"^\d{4,}(?:-\d+)?$")


def _is_mobile_version(value: str) -> bool:
    """移动端发行版本号：接受纯数字/日期串（如 260608、26070603），或宽松 semver。"""
    v = (value or "").strip()
    return bool(_MOBILE_VERSION_RE.match(v) or _is_semver(v))


def identify_mobile_asset(filename: str) -> dict:
    """按命名规范识别移动端发行资产，识别失败返回空 dict。

    只认 Android APK：``ai-lubricant-<version>-android.apk``。iOS 不产出可下载资产
    （走 App Store），因此不在此识别，改由 version.json 顶层 ``ios.store_url`` 承载。
    """
    match = _MOBILE_ASSET_RE.match((filename or "").strip())
    if not match:
        return {}
    return {"platform": "android", "version": match.group(1), "format": "apk"}


def validate_mobile_assets(assets: Any) -> list[str]:
    """校验移动端 version.json / manifest 的 assets 数组（当前只有 Android APK）。"""
    errors: list[str] = []
    if not isinstance(assets, list) or not assets:
        errors.append("assets 必须是非空数组")
        return errors
    seen: set[str] = set()
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict):
            errors.append(f"assets[{index}] 必须是对象")
            continue
        ident = identify_mobile_asset(asset.get("filename"))
        if not ident:
            errors.append(f"assets[{index}].filename 不符合移动端发行产物命名规范（应为 ai-lubricant-<版本>-android.apk）")
            continue
        if asset.get("platform") != ident["platform"]:
            errors.append(f"assets[{index}].platform 与文件名不一致（应为 {ident['platform']}）")
        if asset.get("format") != ident["format"]:
            errors.append(f"assets[{index}].format 与文件名不一致（应为 {ident['format']}）")
        # 与节点侧同口径：入仓资产（repo_path）免 download_url 必填，旧资产仍要求。
        repo_path = str(asset.get("repo_path") or "").strip()
        if repo_path:
            path_error = validate_repo_path(repo_path, prefix="mobile-releases/")
            if path_error:
                errors.append(f"assets[{index}].{path_error}")
        if not repo_path and not is_https_url(asset.get("download_url")):
            errors.append(f"assets[{index}].download_url 必须是 HTTPS URL")
        elif is_https_url(asset.get("download_url")):
            query = _url_query(asset.get("download_url"))
            if query:
                leaked = [k for k in query if any(needle in k.lower() for needle in _FORBIDDEN_QUERY_KEYS)]
                if leaked:
                    errors.append(f"assets[{index}].download_url 不得携带可能含 token 的 query 字段: {sorted(set(leaked))}")
        digest = asset.get("digest")
        if not digest or not _DIGEST_RE.match(str(digest)):
            errors.append(f"assets[{index}].digest 必须是 sha256: 加 64 位十六进制")
        size = asset.get("size_bytes")
        if not isinstance(size, int) or size <= 0:
            errors.append(f"assets[{index}].size_bytes 必须是正整数")
        asset_version = asset.get("version")
        if asset_version is not None and str(asset_version) != "":
            if not _is_mobile_version(str(asset_version)):
                errors.append(f"assets[{index}].version 不是合法的移动端发行版本号")
        if ident["platform"] in seen:
            errors.append(f"assets[{index}] 与其他资产重复（platform）")
        seen.add(ident["platform"])
    return errors


def validate_mobile_release(data: Any) -> list[str]:
    """校验 ``mobile-releases/version.json`` —— 移动端升级链路的唯一真相源。

    结构镜像 node-releases：顶层 version/version_notes 指向最新正式版本，assets 为
    可下载的 Android APK。iOS 无可下载资产，最新版本与商店链接放顶层可选 ``ios`` 块
    （``{version, store_url}``），仅供 App 展示与跳转，不参与自检。
    """
    if not isinstance(data, dict):
        return ["version.json 必须是一个对象"]
    errors: list[str] = []
    if data.get("schema") not in (_MOBILE_RELEASE_SCHEMA, _MOBILE_RELEASE_SCHEMA_LEGACY):
        errors.append(f'schema 必须为 "{_MOBILE_RELEASE_SCHEMA}"')
    version = _text(data.get("version"))
    if not version:
        errors.append("version 必填")
    elif not _is_mobile_version(version):
        errors.append("version 必须是数字/日期串（如 260608）或 semver（1.2.3）")
    if not _text(data.get("version_notes")):
        errors.append("version_notes 必填")
    errors.extend(validate_mobile_assets(data.get("assets")))
    ios = data.get("ios")
    if ios is not None:
        if not isinstance(ios, dict):
            errors.append("ios 必须是对象")
        else:
            ios_version = _text(ios.get("version"))
            if ios_version and not _is_mobile_version(ios_version):
                errors.append("ios.version 不是合法的移动端发行版本号")
            store_url = ios.get("store_url")
            if store_url and not is_https_url(store_url):
                errors.append("ios.store_url 必须是 HTTPS URL")
    forbidden = _find_forbidden_key(data)
    if forbidden:
        errors.append(f"version.json 禁止携带敏感或本地字段: {forbidden}")
    return errors


# ── 设备控制 App 发行资产识别与校验 ──────────────────────────────────────
# Android：device-control-<version>-android.apk
# iOS：device-control-<version>-ios.ipa（侧载包，被控端不上 App Store）
_DEVICE_CONTROL_ASSET_RE = re.compile(r"^device-control-(\d[\w.-]*)-(android\.apk|ios\.ipa)$")


def _is_device_control_version(value: str) -> bool:
    """设备控制 App 版本号：接受纯数字/日期串或 semver。"""
    v = (value or "").strip()
    return bool(_MOBILE_VERSION_RE.match(v) or _is_semver(v))


def identify_device_control_asset(filename: str) -> dict:
    """按命名规范识别设备控制 App 发行资产。

    只认两种：device-control-<version>-android.apk / device-control-<version>-ios.ipa
    """
    match = _DEVICE_CONTROL_ASSET_RE.match((filename or "").strip())
    if not match:
        return {}
    suffix = match.group(2)
    if suffix == "android.apk":
        return {"platform": "android", "version": match.group(1), "format": "apk"}
    return {"platform": "ios", "version": match.group(1), "format": "ipa"}


def validate_device_control_assets(assets: Any) -> list[str]:
    """校验设备控制 App assets（与 validate_mobile_assets 同构）。"""
    errors: list[str] = []
    if not isinstance(assets, list) or not assets:
        errors.append("assets 必须是非空数组")
        return errors
    seen: set[str] = set()
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict):
            errors.append(f"assets[{index}] 必须是对象")
            continue
        ident = identify_device_control_asset(asset.get("filename"))
        if not ident:
            errors.append(
                f"assets[{index}].filename 不符合设备控制 App 命名规范"
                f"（应为 device-control-<版本>-android.apk 或 device-control-<版本>-ios.ipa）"
            )
            continue
        if asset.get("platform") != ident["platform"]:
            errors.append(f"assets[{index}].platform 与文件名不一致（应为 {ident['platform']}）")
        if asset.get("format") != ident["format"]:
            errors.append(f"assets[{index}].format 与文件名不一致（应为 {ident['format']}）")
        repo_path = str(asset.get("repo_path") or "").strip()
        if repo_path:
            path_error = validate_repo_path(repo_path, prefix="device-control-releases/")
            if path_error:
                errors.append(f"assets[{index}].{path_error}")
        if not repo_path and not is_https_url(asset.get("download_url")):
            errors.append(f"assets[{index}].download_url 必须是 HTTPS URL")
        elif is_https_url(asset.get("download_url")):
            query = _url_query(asset.get("download_url"))
            if query:
                leaked = [k for k in query if any(needle in k.lower() for needle in _FORBIDDEN_QUERY_KEYS)]
                if leaked:
                    errors.append(
                        f"assets[{index}].download_url 不得携带可能含 token 的 query 字段: "
                        f"{sorted(set(leaked))}"
                    )
        digest = asset.get("digest")
        if not digest or not _DIGEST_RE.match(str(digest)):
            errors.append(f"assets[{index}].digest 必须是 sha256: 加 64 位十六进制")
        size = asset.get("size_bytes")
        if not isinstance(size, int) or size <= 0:
            errors.append(f"assets[{index}].size_bytes 必须是正整数")
        asset_version = asset.get("version")
        if asset_version is not None and str(asset_version) != "":
            if not _is_device_control_version(str(asset_version)):
                errors.append(f"assets[{index}].version 不是合法的设备控制 App 版本号")
        if ident["platform"] in seen:
            errors.append(f"assets[{index}] 与其他资产重复（platform）")
        seen.add(ident["platform"])
    return errors


def validate_device_control_release(data: Any) -> list[str]:
    """校验 device-control-releases/version.json。"""
    if not isinstance(data, dict):
        return ["version.json 必须是一个对象"]
    errors: list[str] = []
    if data.get("schema") != _DEVICE_CONTROL_RELEASE_SCHEMA:
        errors.append(f'schema 必须为 "{_DEVICE_CONTROL_RELEASE_SCHEMA}"')
    version = _text(data.get("version"))
    if not version:
        errors.append("version 必填")
    elif not _is_device_control_version(version):
        errors.append("version 必须是数字/日期串或 semver")
    if not _text(data.get("version_notes")):
        errors.append("version_notes 必填")
    errors.extend(validate_device_control_assets(data.get("assets")))
    forbidden = _find_forbidden_key(data)
    if forbidden:
        errors.append(f"version.json 禁止携带敏感或本地字段: {forbidden}")
    return errors


def _text(value: Any) -> str:
    return str(value or "").strip()


def validate_manifest(module: str, manifest: Any) -> list[str]:
    """校验单个 manifest，返回错误列表（空列表 = 通过）。"""
    if not isinstance(manifest, dict):
        return ["manifest 必须是一个对象"]

    errors: list[str] = []
    item_id = _text(manifest.get("id"))
    if not safe_item_id(item_id.replace("/", ".")):
        errors.append("id 只能包含字母、数字、点、下划线和连字符")
    if not _text(manifest.get("name")):
        errors.append("name 必填")
    if not _text(manifest.get("display_name")):
        errors.append("display_name 必填")
    if not _text(manifest.get("version")):
        errors.append("version 必填")
    # node-versions / mobile-versions / device-control-versions 用 version_notes 作为唯一版本说明，不再要求 summary。
    # channels 同样不要求：渠道没有真实说明时宁可贵留空，也不生成占位文案。
    if module not in ("node-versions", "mobile-versions", "device-control-versions", "channels") and not _text(manifest.get("summary")):
        errors.append("summary 必填")

    resource = manifest.get("resource")
    resource = resource if isinstance(resource, dict) else {}

    if module == "mcp":
        if manifest.get("kind") != "mcp":
            errors.append('mcp 模块的 kind 必须为 "mcp"')
        res_type = resource.get("type")
        if res_type == "remote_mcp":
            if not is_https_url(resource.get("url")):
                errors.append("远程 MCP 的 resource.url 必须是 HTTPS URL")
            if resource.get("transport") not in _MCP_TRANSPORTS:
                errors.append("远程 MCP transport 必须是 sse 或 streamable-http")
        elif res_type == "stdio_mcp":
            if not _text(resource.get("command")):
                errors.append("stdio MCP 的 resource.command 必填")
        else:
            errors.append("resource.type 必须是 remote_mcp 或 stdio_mcp")
    elif module == "plugins":
        if manifest.get("kind") != "editor_plugin":
            errors.append('plugins 模块的 kind 必须为 "editor_plugin"')
        if not is_https_url(manifest.get("download_url")):
            errors.append("download_url 必须是 HTTPS URL")
        if resource.get("type") != "editor_plugin":
            errors.append('resource.type 必须为 "editor_plugin"')
        if not _text(resource.get("provider")):
            errors.append("resource.provider 必填")
        if not _text(resource.get("entry")):
            errors.append("resource.entry 必填")
    elif module == "skills":
        if manifest.get("kind") != "skill":
            errors.append('skills 模块的 kind 必须为 "skill"')
        if not is_https_url(manifest.get("source_url")) and not is_https_url(
            manifest.get("download_url")
        ):
            errors.append("source_url 或 download_url 至少填写一个 HTTPS URL")
        if resource.get("type") != "skill_package":
            errors.append('resource.type 必须为 "skill_package"')
        if not _text(resource.get("entry")):
            errors.append("resource.entry 必填")
        # 下载方式：显式声明节点怎么拿这个包。缺省（空）= 服务端自动（有镜像走服务端，
        # 否则直连 GitHub clone），旧条目不带此字段以兼容口径处理。
        install_method = _text(manifest.get("install_method"))
        if install_method and install_method not in ("github_clone", "server_mirror"):
            errors.append('install_method 只能是 github_clone 或 server_mirror')

    elif module == "channels":
        if manifest.get("kind") != "channel_template":
            errors.append('channels 模块的 kind 必须为 "channel_template"')
        if manifest.get("schema") not in (_CHANNEL_TEMPLATE_SCHEMA, _CHANNEL_TEMPLATE_SCHEMA_LEGACY):
            errors.append(f'schema 必须为 "{_CHANNEL_TEMPLATE_SCHEMA}"')
        icon = _text(manifest.get("icon"))
        if icon and icon not in _CHANNEL_ICON_KEYS and not is_https_url(icon):
            errors.append("icon 只能是内置图标键或 HTTPS 图片 URL")
        if resource.get("type") != "channel_template":
            errors.append('resource.type 必须为 "channel_template"')
        channel = resource.get("channel")
        freeze_policy = resource.get("freeze_policy")
        if not isinstance(channel, dict):
            errors.append("resource.channel 必须是对象")
        else:
            if not _text(channel.get("name")):
                errors.append("resource.channel.name 必填")
            # 渠道模板必须是「加完账号就能跑」的完整配置：base_url 与每条协议行的 path
            # 都是创建渠道时必需的，缺了就退化成之前那种只有名字的假模板。
            if not _text(channel.get("base_url")):
                errors.append("resource.channel.base_url 必填")
            elif not is_https_url(channel.get("base_url")):
                errors.append("resource.channel.base_url 必须是 HTTPS URL")
            builtin_type = channel.get("builtin_type")
            if builtin_type is not None and not _text(builtin_type):
                errors.append("resource.channel.builtin_type 必须是非空字符串")
            website_url = _text(channel.get("website_url"))
            if channel.get("website_url") is not None and website_url and not is_https_url(website_url):
                errors.append("resource.channel.website_url 必须是 HTTPS URL")
            if channel.get("billing_mode") not in ("token", "request"):
                errors.append("resource.channel.billing_mode 必须为 token 或 request")
            protocols = channel.get("chat_protocols")
            if not isinstance(protocols, list) or not protocols:
                errors.append("resource.channel.chat_protocols 至少需要一条")
            elif not any(isinstance(row, dict) and row.get("enabled", True) is not False for row in protocols):
                errors.append("resource.channel.chat_protocols 至少启用一条")
            else:
                for index, row in enumerate(protocols):
                    if not isinstance(row, dict):
                        errors.append(f"resource.channel.chat_protocols[{index}] 必须是对象")
                        continue
                    if not _text(row.get("protocol")):
                        errors.append(f"resource.channel.chat_protocols[{index}].protocol 必填")
                    if not _text(row.get("path")):
                        errors.append(
                            f"resource.channel.chat_protocols[{index}].path 必填"
                            "（模板要带可创建渠道的路径，不能只有名字）"
                        )
        if not isinstance(freeze_policy, dict) or not isinstance(freeze_policy.get("rules"), list):
            errors.append("resource.freeze_policy.rules 必须是数组")
        forbidden = _find_forbidden_key(manifest)
        if forbidden:
            errors.append(f"渠道模板禁止携带敏感或本地字段: {forbidden}")

    elif module == "prompts":
        if manifest.get("kind") != "project_prompt":
            errors.append('prompts 模块的 kind 必须为 "project_prompt"')
        if resource.get("type") != "project_prompt":
            errors.append('resource.type 必须为 "project_prompt"')
        content = resource.get("content")
        if not isinstance(content, str) or not content.strip():
            errors.append("resource.content 必须为非空 Markdown 文本")
        elif len(content.encode("utf-8")) > 65536:
            errors.append("resource.content 不能超过 64KB")
        providers = resource.get("providers")
        if not isinstance(providers, list) or not providers:
            errors.append("resource.providers 至少需要一个编辑器类型")
        else:
            allowed = {"claude", "codex", "opencode", "cursor"}
            unknown = [p for p in providers if p not in allowed]
            if unknown:
                errors.append(f"resource.providers 仅允许 {sorted(allowed)}")
        target_files = resource.get("target_files")
        if target_files is not None:
            if not isinstance(target_files, list) or not target_files:
                errors.append("resource.target_files 必须是非空数组")
            else:
                allowed_files = {"CLAUDE.md", "AGENTS.md"}
                bad = [f for f in target_files if f not in allowed_files]
                if bad:
                    errors.append(f"resource.target_files 仅允许 {sorted(allowed_files)}")
        compatibility = manifest.get("compatibility")
        if isinstance(compatibility, dict) and isinstance(compatibility.get("providers"), list):
            allowed = {"claude", "codex", "opencode", "cursor"}
            unknown = [p for p in compatibility["providers"] if p not in allowed]
            if unknown:
                errors.append(f"compatibility.providers 仅允许 {sorted(allowed)}")

    elif module == "node-versions":
        if manifest.get("kind") != "node_program_version":
            errors.append('node-versions 模块的 kind 必须为 "node_program_version"')
        if manifest.get("schema") not in (_NODE_VERSION_SCHEMA, _NODE_VERSION_SCHEMA_LEGACY):
            errors.append(f'schema 必须为 "{_NODE_VERSION_SCHEMA}"')
        version = _text(manifest.get("version"))
        if not version:
            errors.append("version 必填")
        elif not _is_node_version(version):
            errors.append("version 必须是日期后缀格式（YYYYMMDD-HHMM）或 semver（1.2.3，可带预发布号）")
        if manifest.get("status") not in ("draft", "published"):
            errors.append('status 必须为 "draft" 或 "published"')
        if not isinstance(manifest.get("test_version"), bool):
            errors.append("test_version 必须是布尔值")
        if not _text(manifest.get("version_notes")):
            errors.append("version_notes 必填")
        errors.extend(validate_node_assets(manifest.get("assets")))
        forbidden = _find_forbidden_key(manifest)
        if forbidden:
            errors.append(f"节点版本禁止携带敏感或本地字段: {forbidden}")

    elif module == "mobile-versions":
        if manifest.get("kind") != "mobile_app_version":
            errors.append('mobile-versions 模块的 kind 必须为 "mobile_app_version"')
        if manifest.get("schema") not in (_MOBILE_VERSION_SCHEMA, _MOBILE_VERSION_SCHEMA_LEGACY):
            errors.append(f'schema 必须为 "{_MOBILE_VERSION_SCHEMA}"')
        version = _text(manifest.get("version"))
        if not version:
            errors.append("version 必填")
        elif not _is_mobile_version(version):
            errors.append("version 必须是数字/日期串（如 260608）或 semver（1.2.3）")
        if manifest.get("status") not in ("draft", "published"):
            errors.append('status 必须为 "draft" 或 "published"')
        if not _text(manifest.get("version_notes")):
            errors.append("version_notes 必填")
        errors.extend(validate_mobile_assets(manifest.get("assets")))
        ios_store_url = manifest.get("ios_store_url")
        if ios_store_url and not is_https_url(ios_store_url):
            errors.append("ios_store_url 必须是 HTTPS URL")
        forbidden = _find_forbidden_key(manifest)
        if forbidden:
            errors.append(f"移动端版本禁止携带敏感或本地字段: {forbidden}")

    elif module == "device-control-versions":
        if manifest.get("kind") != "device_control_app_version":
            errors.append('device-control-versions 模块的 kind 必须为 "device_control_app_version"')
        if manifest.get("schema") != _DEVICE_CONTROL_VERSION_SCHEMA:
            errors.append(f'schema 必须为 "{_DEVICE_CONTROL_VERSION_SCHEMA}"')
        version = _text(manifest.get("version"))
        if not version:
            errors.append("version 必填")
        elif not _is_device_control_version(version):
            errors.append("version 必须是数字/日期串或 semver")
        if manifest.get("status") not in ("draft", "published"):
            errors.append('status 必须为 "draft" 或 "published"')
        if not _text(manifest.get("version_notes")):
            errors.append("version_notes 必填")
        errors.extend(validate_device_control_assets(manifest.get("assets")))
        forbidden = _find_forbidden_key(manifest)
        if forbidden:
            errors.append(f"设备控制 App 版本禁止携带敏感或本地字段: {forbidden}")

    if module not in ("node-versions", "mobile-versions", "device-control-versions"):
        digest = manifest.get("digest")
        if digest and not _DIGEST_RE.match(str(digest)):
            errors.append("digest 必须是 sha256: 加 64 位十六进制")

    return errors


def item_path(module: str, item_id: str) -> str:
    return f"modules/{module}/items/{item_id}.json"


def index_path(module: str, index_name: str = "index.json") -> str:
    return f"modules/{module}/{index_name}"


def empty_index(module: str) -> dict:
    return {"schema": MARKET_INDEX_SCHEMA, "module": module, "items": []}


def index_summary(module: str, manifest: dict) -> dict:
    """从完整 manifest 抽出索引行（轻量列表，供消费侧直读 raw）。

    不含 install_targets：安装目标由使用入口决定，市场数据不声明。
    """
    raw_id = str(manifest.get("id") or "")
    publisher = manifest.get("publisher") or raw_id.split(".")[0]
    summary = manifest.get("summary")
    if module in ("node-versions", "mobile-versions", "device-control-versions") and not summary:
        notes = manifest.get("version_notes") or ""
        summary = (notes.strip().splitlines()[0] if notes.strip() else "")[:120]
    assets = manifest.get("assets") if module == "node-versions" else None
    return {
        "id": manifest.get("id"),
        "module": module,
        "kind": manifest.get("kind"),
        "name": manifest.get("name"),
        "display_name": manifest.get("display_name"),
        "summary": summary,
        "publisher": publisher,
        "category": manifest.get("category"),
        "tags": manifest.get("tags") or [],
        "item_path": item_path(module, raw_id.replace("/", ".")),
        "latest_version": manifest.get("version"),
        "status": manifest.get("status") or "published",
        "test_version": manifest.get("test_version") if module == "node-versions" else None,
        "node_assets": sum(1 for a in assets if isinstance(a, dict) and a.get("component") == "node") if isinstance(assets, list) else 0,
        "runtime_assets": sum(1 for a in assets if isinstance(a, dict) and a.get("component") == "agent-compose") if isinstance(assets, list) else 0,
    }


def normalize_import_body(body: Any) -> dict[str, dict[str, Any]]:
    """把三种导入形态归一成 ``{module: {manifests: {id: manifest}}}``。

    支持：完整 export 包 ``{modules: {...}}``、``{module, manifests: []|{}}``、
    以及单条 ``{module, manifest}``。
    """
    if isinstance(body, dict) and isinstance(body.get("modules"), dict):
        out: dict[str, dict[str, Any]] = {}
        for module, entry in body["modules"].items():
            raw = entry.get("manifests") if isinstance(entry, dict) else entry
            if raw is None:
                raw = entry if isinstance(entry, (list, dict)) else {}
            items = list(raw.values()) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
            out[str(module)] = {
                "manifests": {
                    str(m.get("id")): m
                    for m in items
                    if isinstance(m, dict) and m.get("id")
                }
            }
        return out

    module = str((body or {}).get("module") or "") if isinstance(body, dict) else ""
    raw = None
    if isinstance(body, dict):
        raw = body.get("manifests")
        if raw is None:
            raw = body.get("manifest")
        if raw is None:
            raw = body
    items: list[Any]
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        # 单条 manifest（带 id）与 {id: manifest} 映射都要支持
        items = [raw] if raw.get("id") else list(raw.values())
    else:
        items = []
    return {
        module: {
            "manifests": {
                str(m.get("id")): m
                for m in items
                if isinstance(m, dict) and m.get("id")
            }
        }
    }
