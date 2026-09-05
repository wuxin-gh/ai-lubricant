"""GitHub Contents API 客户端（市场写侧）。

GitHub 仓库是市场的发布镜像（PG marketplace_items 是编辑真相源）。本模块用服务端
保存的 token 调 Contents API：读 bootstrap/resync 所需的 index.json / items/*.json，
由后台 publisher 带 sha 写 JSON 与二进制（发行资产入仓）、删除文件。
token 只在服务端，前端与消费侧都不接触；错误统一走 GitClientError，异常信息里
绝不含 token。

出站请求一律走 :mod:`providers.proxy_manager`，代理取资源中心配置的 ``proxy_id``
（市场拉取/写入共用同一条代理池条目，留空=直连）。这样服务端代理能通到 GitHub，
浏览器/前端不再直连 raw。

布局与消费侧（user-frontend/src/api/marketplaceRaw.ts）约定一致：
  modules/<module>/index.json
  modules/<module>/items/<id>.json
  node-releases/<version>/manifest.json + node-releases/<version>/files/<资产>
  mobile-releases/<version>/manifest.json + mobile-releases/<version>/files/<资产>

发行资产直接提交进仓库（git blob）而不是 GitHub Release 附件：Release 附件存在
GitHub 对象存储、不属于 git 对象，Gitee 镜像同步永远搬不过来；入仓后 git fetch
原生带走，消费端按平台（见 :mod:`.urls`）从仓库相对路径渲染下载地址。
"""
from __future__ import annotations

import base64
import copy
import json
import time
from typing import Any
from urllib.parse import quote

import aiohttp

from ..git_clients import GitClientError, _HTTP_TIMEOUT
from .config import MarketplaceSettings

# ── 进程内读缓存 ──────────────────────────────────────────────────────────────
# Contents API 每次读都是一次完整 RTT（经代理普遍 1-3s），而管理页的目录/编辑/导出
# 会反复读同一批文件。这里缓存 GET 成功结果与 404（「确认不存在」同样是一次 RTT）：
#   - 写/删内部取 sha 永远绕过缓存，成功后失效该 path —— 保证 PUT 拿到的 sha 是
#     GitHub 当前的，不会因旧 sha 冲突 409；
#   - 缓存存取一律深拷贝，调用方（routes 里 index.update 等就地改写）改不动缓存本体；
#   - 其他实例 / 网页端的外部改动最多在 TTL 内不可见；管理页「刷新」带 fresh=1 可
#     强制绕过（见 routes._read_index）。
_CACHE_TTL_SECONDS = 300
_CACHE_MAX_ENTRIES = 512
_read_cache: dict[tuple[str, str, str, str], tuple[float, Any, str]] = {}


def _cache_key(settings: MarketplaceSettings, path: str) -> tuple[str, str, str, str]:
    return (settings.github_owner, settings.github_repo, settings.github_branch or "main", path)


def _cache_lookup(key: tuple[str, str, str, str]) -> tuple[Any, str] | None:
    entry = _read_cache.get(key)
    if entry is None:
        return None
    expires_at, data, sha = entry
    if time.monotonic() > expires_at:
        _read_cache.pop(key, None)
        return None
    return data, sha


def _cache_put(key: tuple[str, str, str, str], data: Any, sha: str) -> None:
    if len(_read_cache) >= _CACHE_MAX_ENTRIES:
        oldest = min(_read_cache, key=lambda k: _read_cache[k][0])
        _read_cache.pop(oldest, None)
    _read_cache[key] = (time.monotonic() + _CACHE_TTL_SECONDS, copy.deepcopy(data), sha)


def _cache_invalidate(key: tuple[str, str, str, str]) -> None:
    _read_cache.pop(key, None)


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ai-lubricant-marketplace",
    }


def _contents_url(settings: MarketplaceSettings, path: str) -> str:
    owner = settings.github_owner
    repo = settings.github_repo
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{quote(path, safe='/')}"


def _decode_content(raw: str) -> str:
    return base64.b64decode(raw.replace("\n", "")).decode("utf-8")


def _encode_content(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _encode_bytes(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


class MarketplaceGitHub:
    """薄封装：读/写/删 GitHub 上的市场 JSON 文件。

    所有出站请求经 ``proxy_manager``，``proxy_config_id`` 取自资源中心配置的
    ``proxy_id``（写侧与拉取侧共用），空则隐式直连。这样 GitHub 不可达时只需在
    资源中心配一条代理，读写两侧同时生效。
    """

    def __init__(self, settings: MarketplaceSettings) -> None:
        self._settings = settings

    @property
    def _ref(self) -> str:
        return self._settings.github_branch or "main"

    @property
    def _proxy_config_id(self) -> str | None:
        return self._settings.proxy_id or None

    async def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: Any = None,
        data: bytes | None = None,
        params: dict[str, Any] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ):
        """统一出口：经 proxy_manager 发请求，返回 OutboundResponse。

        ``proxy_config_id`` 空时 proxy_manager 隐式直连，无需特判。透传 aiohttp
        风格的 ``json``/``data``/``params`` 给底层 session。
        """
        from providers.proxy_manager import get_proxy_manager

        kwargs: dict[str, Any] = {}
        if json_body is not None:
            kwargs["json"] = json_body
        if data is not None:
            kwargs["data"] = data
        if params is not None:
            kwargs["params"] = params
        return await get_proxy_manager().request(
            url=url,
            method=method,
            headers=headers or {},
            timeout=timeout or aiohttp.ClientTimeout(total=_HTTP_TIMEOUT),
            proxy_config_id=self._proxy_config_id,
            **kwargs,
        )

    async def read_json(self, path: str, *, use_cache: bool = True) -> tuple[Any, str]:
        """读文件，返回 ``(data, sha)``。404 抛 GitClientError（调用方决定是否当空）。

        ``use_cache=True`` 优先命中 TTL 读缓存；``use_cache=False`` 跳过读直接回源，
        并把回源结果回填缓存（强制刷新语义——管理页「刷新」后，后续常规读立即拿到
        新值）。写路径取 sha 也传 False，确保拿到 GitHub 当前 sha 而非缓存快照。
        """
        key = _cache_key(self._settings, path)
        if use_cache:
            hit = _cache_lookup(key)
            if hit is not None:
                data, sha = hit
                if data is None:  # 缓存的 404：确认不存在也是一次 RTT
                    raise GitClientError(f"GET {path} returned HTTP 404")
                return copy.deepcopy(data), sha
        url = f"{_contents_url(self._settings, path)}?ref={quote(self._ref, safe='')}"
        resp = await self._request("GET", url, headers=_headers(self._settings.github_token))
        if resp.status >= 400:
            if resp.status == 404:
                _cache_put(key, None, "")
            raise GitClientError(f"GET {path} returned HTTP {resp.status}")
        body = await resp.json()
        if not isinstance(body, dict) or body.get("type") != "file":
            raise GitClientError(f"{path} 不是文件")
        content = _decode_content(body.get("content") or "") if body.get("encoding") == "base64" else ""
        data = json.loads(content)
        sha = str(body.get("sha") or "")
        _cache_put(key, data, sha)
        return data, sha

    async def read_json_or_none(self, path: str, *, use_cache: bool = True) -> tuple[Any, str] | None:
        """读文件；文件不存在（404）返回 None，其余错误照抛。"""
        try:
            return await self.read_json(path, use_cache=use_cache)
        except GitClientError as exc:
            if "HTTP 404" in str(exc):
                return None
            raise

    async def _read_sha_or_none(self, path: str) -> str | None:
        """只读 Contents 元数据里的 sha，不解码/解析文件内容。

        发行二进制不是 JSON，不能复用 ``read_json_or_none``（它会对二进制执行
        json.loads；大于 1MB 时 GitHub Contents API 还可能不给内联 content）。
        写入重试与删除二进制都只需要 sha，走这个窄 helper。
        """
        url = f"{_contents_url(self._settings, path)}?ref={quote(self._ref, safe='')}"
        resp = await self._request("GET", url, headers=_headers(self._settings.github_token))
        if resp.status == 404:
            return None
        if resp.status >= 400:
            raise GitClientError(f"GET {path} returned HTTP {resp.status}")
        body = await resp.json()
        if not isinstance(body, dict) or body.get("type") != "file":
            raise GitClientError(f"{path} 不是文件")
        return str(body.get("sha") or "") or None

    async def write_json(self, path: str, data: Any, message: str) -> None:
        """带 sha 创建/更新文件。若已存在先取 sha 再 PUT（绕缓存取真实 sha，写后失效缓存）。"""
        sha = ""
        existing = await self.read_json_or_none(path, use_cache=False)
        if existing is not None:
            sha = existing[1]
        payload: dict[str, Any] = {
            "message": message,
            "branch": self._ref,
            "content": _encode_content(json.dumps(data, ensure_ascii=False, indent=2) + "\n"),
        }
        if sha:
            payload["sha"] = sha
        resp = await self._request(
            "PUT",
            _contents_url(self._settings, path),
            headers=_headers(self._settings.github_token),
            json_body=payload,
        )
        if resp.status >= 400:
            text = await resp.text()
            raise GitClientError(f"PUT {path} returned HTTP {resp.status}: {text[:300]}")
        _cache_invalidate(_cache_key(self._settings, path))

    async def write_bytes(self, path: str, data: bytes, message: str) -> None:
        """带 sha 创建/更新二进制文件（发行资产入仓）。

        与 ``write_json`` 同骨架但走 ``_read_sha_or_none`` 取 sha：二进制不能用
        JSON 读取路径。超时放宽到 300s 档——几 MB 到几十 MB 的 base64 PUT 一次 RTT
        远超常规 JSON 写入。注意 GitHub blob 硬限 100MB/文件，调用方需自行控制上限。
        """
        sha = await self._read_sha_or_none(path) or ""
        payload: dict[str, Any] = {
            "message": message,
            "branch": self._ref,
            "content": _encode_bytes(data),
        }
        if sha:
            payload["sha"] = sha
        resp = await self._request(
            "PUT",
            _contents_url(self._settings, path),
            headers=_headers(self._settings.github_token),
            json_body=payload,
            timeout=aiohttp.ClientTimeout(total=max(_HTTP_TIMEOUT, 300)),
        )
        if resp.status >= 400:
            text = await resp.text()
            raise GitClientError(f"PUT {path} returned HTTP {resp.status}: {text[:300]}")
        _cache_invalidate(_cache_key(self._settings, path))

    async def delete_file(self, path: str, message: str) -> bool:
        """删除文件；文件不存在返回 False，删除成功返回 True。

        GitHub Contents API 的 DELETE 需要 JSON body（message/sha/branch）。sha 只读
        元数据即可，不解析内容——二进制资产（发行包入仓）与 JSON 都能删。
        """
        sha = await self._read_sha_or_none(path)
        if sha is None:
            return False
        body = {"message": message, "branch": self._ref, "sha": sha}
        url = _contents_url(self._settings, path)
        resp = await self._request("DELETE", url, headers=_headers(self._settings.github_token), json_body=body)
        if resp.status >= 400 and resp.status != 404:
            text = await resp.text()
            raise GitClientError(f"DELETE {path} returned HTTP {resp.status}: {text[:300]}")
        if resp.status < 400:
            _cache_invalidate(_cache_key(self._settings, path))
        return resp.status < 400


# ── GitHub Releases API ────────────────────────────────────────────────────────
# 程序发行（节点、移动端 App、设备控制 App）使用独立仓库的 GitHub Releases 存储二进制，
# 不再入仓（git blob）。以下函数操作 Releases API，创建 Release、上传 assets。


async def create_release(
    owner: str,
    repo: str,
    token: str,
    tag: str,
    name: str,
    body: str,
    draft: bool = False,
    prerelease: bool = False,
) -> dict:
    """创建 GitHub Release。

    返回 release 信息（含 upload_url），供后续上传 assets。
    如果 tag 已存在且不是 draft，会返回 422 错误；调用前应先检查 tag 是否存在。
    """
    from ..git_clients import _http_session

    url = f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/releases"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ai-lubricant-marketplace",
    }
    payload = {
        "tag_name": tag,
        "name": name,
        "body": body,
        "draft": draft,
        "prerelease": prerelease,
    }

    async with _http_session() as session:
        async with session.post(
            url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                raise GitClientError(f"create_release failed: HTTP {resp.status} {text[:300]}")
            return await resp.json()


async def upload_release_asset(
    upload_url: str,
    token: str,
    filename: str,
    content: bytes,
    content_type: str = "application/octet-stream",
) -> dict:
    """上传 asset 到 Release。

    upload_url 格式：``https://uploads.github.com/repos/{owner}/{repo}/releases/{id}/assets{?name,label}``
    需要移除模板参数部分，添加 ``?name=<filename>``。
    """
    from ..git_clients import _http_session

    # 移除模板参数，添加文件名
    base_url = upload_url.split("{?")[0] if "{?" in upload_url else upload_url
    url = f"{base_url}?name={quote(filename)}"

    headers = {
        "Authorization": f"token {token}",
        "Content-Type": content_type,
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ai-lubricant-marketplace",
    }

    async with _http_session() as session:
        async with session.post(
            url,
            data=content,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=300),
        ) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                raise GitClientError(f"upload_release_asset {filename} failed: HTTP {resp.status} {text[:300]}")
            return await resp.json()


async def get_release_by_tag(owner: str, repo: str, token: str, tag: str) -> dict | None:
    """获取指定 tag 的 Release 信息，不存在返回 None。"""
    from ..git_clients import _http_session

    url = f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/releases/tags/{quote(tag)}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ai-lubricant-marketplace",
    }

    async with _http_session() as session:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 404:
                return None
            if resp.status != 200:
                text = await resp.text()
                raise GitClientError(f"get_release_by_tag {tag} failed: HTTP {resp.status} {text[:300]}")
            return await resp.json()


async def get_latest_release(owner: str, repo: str, token: str = "") -> dict | None:
    """获取 latest Release（公开仓库可不传 token）。"""
    from ..git_clients import _http_session

    url = f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/releases/latest"
    headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "ai-lubricant-marketplace"}
    if token:
        headers["Authorization"] = f"token {token}"

    async with _http_session() as session:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 404:
                return None
            if resp.status != 200:
                text = await resp.text()
                raise GitClientError(f"get_latest_release failed: HTTP {resp.status} {text[:300]}")
            return await resp.json()
