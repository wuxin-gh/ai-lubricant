"""Read/write clients for the Git hosting platforms a project can be linked to.

The console lets a user register a credential (``GitIdentity``) for a Git host
and then browse it: list the repositories that credential can reach, list a
repository's branches, walk one level of its tree, preview a single file, create
a new remote repository, and manage the project's review webhook.

Seven hosts are supported. Four of them (GitHub, GitLab, Gitea, Gitee) speak a
broadly GitHub-shaped REST API and also expose repository webhooks; the other
three (Aliyun Yunxiao Codeup, cnb.cool, AtomGit) are read-only here. Each host
gets its own small set of functions, and a thin dispatch layer at the bottom of
this module picks the right one from a platform string.

Two invariants hold everywhere in this module:

* **Credentials never appear in an error or a log line.** Every failure is
  reported as method + URL + HTTP status + a truncated response body. Request
  headers are never interpolated into a message, so a token cannot leak through
  an exception that bubbles up to an API response.
* **Degradation is the caller's choice.** These functions raise
  :class:`GitClientError` on any upstream failure; the service layer above
  decides whether that becomes an empty list or a 502.
"""
from __future__ import annotations

import base64
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import quote

import aiohttp

# ---- File modes -----------------------------------------------------------
# Tree entries are reported to the frontend as a small integer so the file-tree
# component can pick an icon without knowing per-platform vocabulary.
_MODE_UNKNOWN = 0
_MODE_REGULAR = 1
_MODE_EXECUTABLE = 2
_MODE_SYMLINK = 3
_MODE_DIRECTORY = 4
_MODE_SUBMODULE = 5

# ---- Request tuning -------------------------------------------------------
_HTTP_TIMEOUT = 30
# Upstream page size when we walk a paginated list ourselves.
_FETCH_ALL_PAGE_SIZE = 100
# Hard stop on the walk so a misbehaving host cannot spin us forever.
_MAX_FETCH_ALL_PAGES = 50
# How much of an upstream error body we keep in a raised message.
_ERROR_BODY_LIMIT = 300

# Hosts whose repository listing can be paged server-side. The rest always
# return a full list, which we slice in memory.
PAGINATED_PLATFORMS = {"github", "gitlab", "gitea", "gitee"}
# Hosts where we can create a new remote repository.
CREATE_CAPABLE_PLATFORMS = {"github", "gitlab", "gitea", "gitee"}
# Hosts with a repository-webhook API we can manage.
WEBHOOK_CAPABLE_PLATFORMS = {"github", "gitlab", "gitea", "gitee"}

_SUPPORTED_PLATFORMS = {
    "github",
    "gitlab",
    "gitea",
    "gitee",
    "codeup",
    "cnb",
    "atomgit",
}

# Default hosts, used when an identity carries no ``base_url``.
_GITHUB_API_HOST = "api.github.com"
_GITLAB_DEFAULT_HOST = "gitlab.com"
_GITEA_DEFAULT_HOST = "gitea.com"
_GITEE_DEFAULT_HOST = "gitee.com"
_CODEUP_DEFAULT_HOST = "openapi-rdc.aliyuncs.com"
_CNB_API_HOST = "api.cnb.cool"
_CNB_WEB_HOST = "cnb.cool"
_ATOMGIT_API_HOST = "api.atomgit.com"
_ATOMGIT_WEB_HOST = "atomgit.com"


class GitClientError(RuntimeError):
    """An upstream Git platform call failed.

    Carries method, URL, status and a truncated response body — never a token.
    """


# ---- Value types ----------------------------------------------------------
@dataclass
class AuthRepository:
    """One repository a credential can reach, in the shape the console renders."""

    full_name: str
    url: str
    description: str = ""

    def to_dict(self) -> dict:
        return {
            "full_name": self.full_name,
            "url": self.url,
            "description": self.description,
        }


@dataclass
class RepositoryPage:
    """A repository listing result.

    ``page_info`` is ``None`` for a full (unpaged) listing and carries
    ``total_count`` / ``has_next_page`` when the caller asked for a page.
    """

    repositories: list[AuthRepository]
    page_info: dict | None = None


@dataclass
class RepositoryOptions:
    """Credentials + listing parameters for one upstream read."""

    token: str
    base_url: str = ""
    organization_id: str = ""
    installation_id: int = 0
    is_oauth: bool = False
    page: int = 0
    size: int = 0
    keyword: str = ""


@dataclass
class Branch:
    name: str

    def to_dict(self) -> dict:
        return {"name": self.name}


@dataclass
class TreeEntry:
    """One entry of a repository tree level."""

    name: str
    path: str
    mode: int
    sha: str = ""
    size: int | None = None
    last_modified_at: int | None = None

    def to_dict(self) -> dict:
        # Optional keys stay absent rather than null: the frontend treats a
        # missing size/mtime as "host did not report it" and hides the column.
        payload: dict[str, Any] = {
            "name": self.name,
            "path": self.path,
            "mode": self.mode,
            "sha": self.sha,
        }
        if self.size is not None:
            payload["size"] = self.size
        if self.last_modified_at is not None:
            payload["last_modified_at"] = self.last_modified_at
        return payload


@dataclass
class Blob:
    """A single file's contents, base64-encoded for transport."""

    content: str
    is_binary: bool = False
    sha: str = ""
    size: int | None = None

    def to_dict(self) -> dict:
        payload: dict[str, Any] = {
            "content": self.content,
            "is_binary": self.is_binary,
            "sha": self.sha,
        }
        if self.size is not None:
            payload["size"] = self.size
        return payload


@dataclass
class CreateRepoOptions:
    """Credentials + metadata for creating a remote repository."""

    token: str
    base_url: str = ""
    is_oauth: bool = False
    name: str = ""
    description: str = ""
    private: bool = True
    owner: str = ""


@dataclass
class CreatedRepo:
    full_name: str
    url: str
    description: str = ""
    web_url: str = ""

    def to_dict(self) -> dict:
        return {
            "full_name": self.full_name,
            "url": self.url,
            "description": self.description,
            "web_url": self.web_url,
        }


@dataclass
class Webhook:
    """A repository webhook as the platform reports it back."""

    hook_id: str
    url: str
    events: list[str]
    active: bool
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "hook_id": self.hook_id,
            "url": self.url,
            "events": list(self.events),
            "active": self.active,
        }


# ---- Internal HTTP helpers -----------------------------------------------
# Tests patch ``aiohttp.ClientSession`` so we always go through the module
# attribute (never ``from aiohttp import ClientSession``).
#
# These four are the only HTTP primitives. Each returns the parsed JSON body
# (delete returns the status instead) and raises :class:`GitClientError` on a
# transport failure or a non-2xx response. The message is always
# ``f"{method} {url} returned HTTP {status}: {truncated body}"`` — no headers,
# so a token carried in an ``Authorization`` header cannot appear in it.


@asynccontextmanager
async def _http_session() -> aiohttp.ClientSession:
    """Context manager yielding a session with the module default timeout.

    MarketplaceGitHub (``github.py``) uses this for release/asset operations
    instead of the low-level ``_get_json`` etc. (those four handle errors
    internally; release operations need finer control over 404/422 status).
    Tests patch ``aiohttp.ClientSession`` to mock network access.
    """
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        yield session


def _short_body(body: Any) -> str:
    """Render an upstream response body for an error message, truncated."""
    text = body if isinstance(body, str) else repr(body)
    return text[:_ERROR_BODY_LIMIT]


async def _get_json(url: str, *, headers: dict[str, str], params: dict | None = None) -> tuple[Any, dict[str, str]]:
    """GET ``url`` and return ``(parsed body, response headers)``."""
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, params=params or None) as resp:
                try:
                    body = await resp.json(content_type=None)
                except Exception:  # noqa: BLE001 — body may be empty or non-JSON
                    body = None
                if resp.status >= 400:
                    raise GitClientError(
                        f"GET {url} returned HTTP {resp.status}: {_short_body(body)}"
                    )
                resp_headers = dict(getattr(resp, "headers", {}) or {})
                return body, resp_headers
    except aiohttp.ClientError as exc:  # network / DNS / TLS failures
        raise GitClientError(f"GET {url} failed: {exc}") from exc


async def _post_json(url: str, *, headers: dict[str, str], json_body: dict | None = None) -> Any:
    """POST ``json_body`` to ``url`` and return the parsed body."""
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=json_body) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise GitClientError(
                        f"POST {url} returned HTTP {resp.status}: {_short_body(text)}"
                    )
                if not text:
                    return None
                try:
                    return await resp.json(content_type=None)
                except Exception:  # noqa: BLE001
                    return None
    except aiohttp.ClientError as exc:
        raise GitClientError(f"POST {url} failed: {exc}") from exc


async def _put_json(url: str, *, headers: dict[str, str], json_body: dict | None = None) -> Any:
    """PUT ``json_body`` to ``url`` and return the parsed body."""
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.put(url, headers=headers, json=json_body) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise GitClientError(
                        f"PUT {url} returned HTTP {resp.status}: {_short_body(text)}"
                    )
                if not text:
                    return None
                try:
                    return await resp.json(content_type=None)
                except Exception:  # noqa: BLE001
                    return None
    except aiohttp.ClientError as exc:
        raise GitClientError(f"PUT {url} failed: {exc}") from exc


async def _patch_json(url: str, *, headers: dict[str, str], json_body: dict | None = None) -> Any:
    """PATCH ``json_body`` to ``url`` and return the parsed body."""
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.patch(url, headers=headers, json=json_body) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise GitClientError(
                        f"PATCH {url} returned HTTP {resp.status}: {_short_body(text)}"
                    )
                if not text:
                    return None
                try:
                    return await resp.json(content_type=None)
                except Exception:  # noqa: BLE001
                    return None
    except aiohttp.ClientError as exc:
        raise GitClientError(f"PATCH {url} failed: {exc}") from exc


async def _delete_json(url: str, *, headers: dict[str, str], params: dict | None = None) -> int:
    """DELETE ``url`` and return the HTTP status.

    A 404 is returned (not raised) so a stale local row whose platform hook was
    already deleted can be cleaned up idempotently. Any other >=400 raises.
    """
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.delete(url, headers=headers, params=params or None) as resp:
                if resp.status == 404:
                    return 404
                if resp.status >= 400:
                    text = await resp.text()
                    raise GitClientError(
                        f"DELETE {url} returned HTTP {resp.status}: {_short_body(text)}"
                    )
                return resp.status
    except aiohttp.ClientError as exc:
        raise GitClientError(f"DELETE {url} failed: {exc}") from exc


# ---- Small shared utilities -----------------------------------------------
def _first_non_empty(*values: Any) -> str:
    """Return the first truthy value coerced to a string, else ``""``."""
    for value in values:
        if value:
            return str(value)
    return ""


def _clean_base64(content: str | None) -> str:
    """Strip every whitespace run from a base64 payload.

    GitHub/Gitea wrap base64 at 60–76 columns with embedded newlines; the
    browser's ``atob`` rejects any character outside the alphabet, so we
    collapse all whitespace before handing it to the frontend.
    """
    if not content:
        return ""
    return "".join(content.split())


def _is_probably_binary(b64_content: str) -> bool:
    """Heuristic: report binary when the payload does not decode as UTF-8."""
    if not b64_content:
        return False
    try:
        raw = base64.b64decode(b64_content, validate=False)
    except Exception:  # noqa: BLE001 — undecodable is treated as binary
        return True
    if b"\x00" in raw:
        return True
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        return True
    return False


def _contents_type_to_mode(entry_type: str) -> int:
    """Map a GitHub/Gitea/Gitee ``contents`` entry ``type`` to a file mode."""
    mapping = {
        "dir": _MODE_DIRECTORY,
        "symlink": _MODE_SYMLINK,
        "submodule": _MODE_SUBMODULE,
    }
    return mapping.get(str(entry_type or "").lower(), _MODE_REGULAR)


def _gitlab_mode(entry_type: str, mode: str) -> int:
    """Map a GitLab tree entry (``type`` + git ``mode``) to a file mode."""
    entry_type = str(entry_type or "").lower()
    if entry_type == "tree":
        return _MODE_DIRECTORY
    if entry_type == "commit":
        return _MODE_SUBMODULE
    mode = str(mode or "")
    if mode == "120000":
        return _MODE_SYMLINK
    if mode == "160000":
        return _MODE_SUBMODULE
    if mode == "040000":
        return _MODE_DIRECTORY
    if mode == "100755":
        return _MODE_EXECUTABLE
    return _MODE_REGULAR


def _header_value(headers: dict[str, str] | None, name: str) -> str:
    """Case-insensitively read a response header."""
    if not headers:
        return ""
    for key, value in headers.items():
        if str(key).lower() == name.lower():
            return str(value)
    return ""


def _parse_link_header(value: str) -> dict[str, str]:
    """Parse a ``Link`` header into ``{rel: url}``.

    The header lists ``<url>; rel="next", <url>; rel="last"`` and friends; we
    only keep the rel→URL pairs so pagination callers can ask for ``next`` or
    ``last``.
    """
    links: dict[str, str] = {}
    if not value:
        return links
    for part in value.split(","):
        match = re.search(r"<([^>]+)>\s*;\s*rel=\"([^\"]+)\"", part)
        if match:
            links[match.group(2)] = match.group(1)
    return links


# ---- GitHub rate-limit detection -------------------------------------------
# GitHub 限流有两类：primary（认证用户默认 5000 次/小时滚动窗口，403 +
# "API rate limit exceeded for user ID ..."）与 secondary（请求过于密集触发
# 滥用检测，403/429 + Retry-After）。原始报错是一大段 JSON，看不出原因也不知
# 道要等多久；这里统一识别成「直白原因 + 恢复时刻」。消息固定以
# RATE_LIMIT_PREFIX 开头，上层（leaderboard 同步循环等）用 is_rate_limit_error
# 识别后即可停止继续发无谓的 403 请求。
RATE_LIMIT_PREFIX = "GitHub API 限流"


def is_rate_limit_error(message: Any) -> bool:
    """消息是否为限流错误（由 :func:`github_rate_limit_message` 生成）。"""
    return RATE_LIMIT_PREFIX in str(message or "")


def github_rate_limit_message(resp: Any, body: str = "") -> str | None:
    """识别 GitHub 限流响应并给出直白原因与恢复时间；非限流返回 ``None``。

    401/403 凭证错误、404 等不归限流管的响应一律返回 ``None``，调用方走既有
    的通用报错。恢复时刻读 ``X-RateLimit-Reset``（epoch 秒）或 ``Retry-After``
    （相对秒）；代理层把两个头都剥掉时只给原因不给时间（仍比裸 JSON 强）。
    """
    status = getattr(resp, "status", 0)
    if status not in (403, 429):
        return None
    headers = getattr(resp, "headers", None)
    remaining = _header_value(headers, "X-RateLimit-Remaining")
    low = str(body or "").lower()
    primary = remaining == "0" or "rate limit exceeded" in low
    secondary = status == 429 or "secondary rate limit" in low or "abuse detection" in low
    if not (primary or secondary):
        return None
    now = time.time()
    reset_at: float | None = None
    retry_after = _header_value(headers, "Retry-After")
    if retry_after.isdigit():
        reset_at = now + int(retry_after)
    if reset_at is None:
        reset_epoch = _header_value(headers, "X-RateLimit-Reset")
        if reset_epoch.isdigit():
            reset_at = float(reset_epoch)
    limit = _header_value(headers, "X-RateLimit-Limit")
    if secondary and not primary:
        reason = "请求过于密集，触发二级限流（secondary rate limit）"
    else:
        reason = f"配额 {limit or 5000} 次/小时已用尽"
    if reset_at is None:
        return f"{RATE_LIMIT_PREFIX}：{reason}，请稍后重试"
    when = datetime.fromtimestamp(reset_at)
    today = datetime.now()
    clock = when.strftime("%H:%M") if when.date() == today.date() else when.strftime("%m-%d %H:%M")
    wait_min = int(max(0, reset_at - now) // 60)
    wait = f"还有 {wait_min} 分钟" if wait_min < 60 else f"还有 {wait_min // 60} 小时 {wait_min % 60} 分"
    return f"{RATE_LIMIT_PREFIX}：{reason}，预计 {clock} 恢复（{wait}）"


def _normalize_base(input_url: str, fallback_host: str) -> tuple[str, str]:
    """Split a user-supplied base URL into ``(scheme, host)``.

    Empty input falls back to ``("https", fallback_host)``; a bare host is
    assumed ``https``; a trailing slash is stripped. The returned host may
    itself carry a path prefix (e.g. an on-prem Gitea mounted under a subpath).
    """
    text = (input_url or "").strip().rstrip("/")
    if not text:
        return "https", fallback_host
    if text.startswith("http://"):
        return "http", text[len("http://"):]
    if text.startswith("https://"):
        return "https", text[len("https://"):]
    return "https", text


def paginate_repos(all_repos: list[AuthRepository], keyword: str, page: int, size: int) -> RepositoryPage:
    """Filter + slice a fully-fetched repository list in memory.

    ``keyword`` filters by a case-insensitive substring of ``full_name``;
    ``page`` is 1-based and ``size`` the page length. Returns a
    :class:`RepositoryPage` whose ``page_info`` reflects the filtered total.
    """
    keyword = (keyword or "").strip().lower()
    if keyword:
        filtered = [r for r in all_repos if keyword in r.full_name.lower()]
    else:
        filtered = list(all_repos)
    total = len(filtered)
    start = min(max((page - 1) * size, 0), total)
    end = min(start + size, total)
    return RepositoryPage(
        repositories=filtered[start:end],
        page_info={"total_count": total, "has_next_page": end < total},
    )


# ---- Per-platform auth + API roots ----------------------------------------
def _github_headers(token: str) -> dict[str, str]:
    """GitHub REST v3 accepts a personal/installation token as ``token <tok>``."""
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }


def _github_api_base(base_url: str) -> str:
    """``https://api.github.com`` by default; an Enterprise host uses ``/api/v3``."""
    if not (base_url or "").strip():
        return f"https://{_GITHUB_API_HOST}"
    scheme, host = _normalize_base(base_url, _GITHUB_API_HOST)
    if host == _GITHUB_API_HOST:
        return f"{scheme}://{host}"
    return f"{scheme}://{host}/api/v3"


def _gitlab_headers(token: str, is_oauth: bool) -> dict[str, str]:
    """GitLab takes a PAT in ``PRIVATE-TOKEN`` and an OAuth token as a bearer."""
    if is_oauth:
        return {"Authorization": f"Bearer {token}"}
    return {"PRIVATE-TOKEN": token}


def _gitlab_api_base(base_url: str) -> str:
    scheme, host = _normalize_base(base_url, _GITLAB_DEFAULT_HOST)
    return f"{scheme}://{host}/api/v4"


def _gitlab_project_id(full_name: str) -> str:
    """GitLab addresses a project by its URL-encoded full path."""
    return quote(full_name, safe="")


def _gitea_headers(token: str) -> dict[str, str]:
    """Gitea's API accepts ``Authorization: token <tok>``."""
    return {"Authorization": f"token {token}", "Accept": "application/json"}


def _gitea_api_base(base_url: str) -> str:
    scheme, host = _normalize_base(base_url, _GITEA_DEFAULT_HOST)
    return f"{scheme}://{host}/api/v1"


def _gitee_api_base(base_url: str) -> str:
    scheme, host = _normalize_base(base_url, _GITEE_DEFAULT_HOST)
    return f"{scheme}://{host}/api/v5"


def _codeup_api_base(base_url: str) -> str:
    """Yunxiao's OpenAPI root; the service endpoint is per-tenant configurable."""
    scheme, host = _normalize_base(base_url, _CODEUP_DEFAULT_HOST)
    return f"{scheme}://{host}/oapi/v1"


def _codeup_headers(token: str) -> dict[str, str]:
    """Yunxiao authenticates with a personal access token header."""
    return {"x-yunxiao-token": token, "Content-Type": "application/json"}


def _cnb_api_base(base_url: str) -> str:
    scheme, host = _normalize_base(base_url, _CNB_API_HOST)
    return f"{scheme}://{host}"


def _cnb_headers(token: str) -> dict[str, str]:
    """cnb.cool's OpenAPI uses a bearer token."""
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _atomgit_api_base(base_url: str) -> str:
    scheme, host = _normalize_base(base_url, _ATOMGIT_API_HOST)
    return f"{scheme}://{host}/api/v5"


def _atomgit_headers(token: str) -> dict[str, str]:
    """AtomGit reads the credential from ``private-token``.

    A bearer header is sent alongside because some deployments front the API
    with a gateway that expects it; both name the same token.
    """
    return {
        "private-token": token,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def _as_list(body: Any) -> list[dict]:
    """Coerce an upstream list body into a list of dicts, ignoring junk."""
    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]
    if isinstance(body, dict):
        for key in ("result", "data", "items", "repositories", "list"):
            nested = body.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


# ---- Repository listing ---------------------------------------------------
def _github_repo_from(item: dict) -> AuthRepository:
    return AuthRepository(
        full_name=item.get("full_name") or "",
        url=_first_non_empty(item.get("clone_url"), item.get("html_url")),
        description=item.get("description") or "",
    )


def _gitlab_repo_from(item: dict) -> AuthRepository:
    return AuthRepository(
        full_name=item.get("path_with_namespace") or "",
        url=_first_non_empty(
            item.get("http_url_to_repo"),
            item.get("ssh_url_to_repo"),
            item.get("web_url"),
        ),
        description=item.get("description") or "",
    )


def _gitea_repo_from(item: dict) -> AuthRepository:
    return AuthRepository(
        full_name=item.get("full_name") or "",
        url=item.get("clone_url") or "",
        description=item.get("description") or "",
    )


def _gitee_repo_from(item: dict) -> AuthRepository:
    return AuthRepository(
        full_name=item.get("full_name") or "",
        url=item.get("html_url") or "",
        description=item.get("description") or "",
    )


def _codeup_repo_from(item: dict) -> AuthRepository:
    url = item.get("httpCloneUrl") or item.get("webUrl") or ""
    if url and not url.endswith(".git"):
        url = url + ".git"
    return AuthRepository(
        full_name=item.get("pathWithNamespace") or "",
        url=url,
        description=item.get("description") or "",
    )


def _cnb_repo_from(item: dict) -> AuthRepository:
    full_name = item.get("path") or ""
    url = item.get("clone_url") or item.get("ssh_url") or ""
    if not url and full_name:
        url = f"https://{_CNB_WEB_HOST}/{full_name}"
    return AuthRepository(
        full_name=full_name,
        url=url,
        description=item.get("description") or "",
    )


def _atomgit_repo_from(item: dict) -> AuthRepository:
    full_name = item.get("full_name") or ""
    url = item.get("clone_url") or item.get("ssh_url") or ""
    if not url and full_name:
        url = f"https://{_ATOMGIT_WEB_HOST}/{full_name}"
    return AuthRepository(
        full_name=full_name,
        url=url,
        description=item.get("description") or "",
    )


def _github_page_info(link_header: str, page_size: int) -> dict:
    """Derive ``total_count`` from GitHub's ``Link: rel="last"``.

    GitHub's list endpoint does not report a total, but the ``last`` link's
    ``page`` query parameter is the final page number; multiplying it by the
    requested size gives a total that the frontend's pager understands.
    """
    links = _parse_link_header(link_header)
    last = links.get("last", "")
    match = re.search(r"[?&]page=(\d+)", last)
    last_page = int(match.group(1)) if match else 1
    return {
        "total_count": last_page * max(page_size, 1),
        "has_next_page": "next" in links,
    }


async def _github_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    base = _github_api_base(opts.base_url)
    headers = _github_headers(opts.token)
    url = f"{base}/user/repos"
    # GitHub's list endpoint has no search; with a keyword we fetch the full
    # list and filter in memory, so the ``search`` param is never sent upstream.
    if opts.page > 0 and not opts.keyword:
        size = opts.size or 100
        params: dict[str, Any] = {"page": opts.page, "per_page": size}
        body, resp_headers = await _get_json(url, headers=headers, params=params)
        repos = [_github_repo_from(item) for item in _as_list(body)]
        page_info = _github_page_info(_header_value(resp_headers, "Link"), size)
        return RepositoryPage(repositories=repos, page_info=page_info)
    if opts.page > 0 and opts.keyword:
        all_repos = await _github_fetch_all_repositories(opts)
        return paginate_repos(all_repos, opts.keyword, opts.page, opts.size or 20)
    return RepositoryPage(repositories=await _github_fetch_all_repositories(opts))


async def _github_fetch_all_repositories(opts: RepositoryOptions) -> list[AuthRepository]:
    base = _github_api_base(opts.base_url)
    headers = _github_headers(opts.token)
    url = f"{base}/user/repos"
    collected: list[AuthRepository] = []
    for page in range(1, _MAX_FETCH_ALL_PAGES + 1):
        body, _ = await _get_json(
            url, headers=headers, params={"page": page, "per_page": _FETCH_ALL_PAGE_SIZE}
        )
        batch = _as_list(body)
        collected.extend(_github_repo_from(item) for item in batch)
        if len(batch) < _FETCH_ALL_PAGE_SIZE:
            break
    return collected


async def _gitlab_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    base = _gitlab_api_base(opts.base_url)
    headers = _gitlab_headers(opts.token, opts.is_oauth)
    url = f"{base}/projects"
    # ``membership=true`` restricts the listing to projects the credential is a
    # member of, which is what "repositories this identity can reach" means.
    common: dict[str, Any] = {"membership": "true", "order_by": "last_activity_at"}
    if opts.keyword:
        common["search"] = opts.keyword
    if opts.page > 0:
        size = opts.size or 100
        params = {**common, "page": opts.page, "per_page": size}
        body, resp_headers = await _get_json(url, headers=headers, params=params)
        repos = [_gitlab_repo_from(item) for item in _as_list(body)]
        total_raw = _header_value(resp_headers, "X-Total")
        next_page = _header_value(resp_headers, "X-Next-Page")
        try:
            total = int(total_raw)
        except (TypeError, ValueError):
            total = len(repos)
        page_info = {"total_count": total, "has_next_page": bool(next_page)}
        return RepositoryPage(repositories=repos, page_info=page_info)
    collected: list[AuthRepository] = []
    for page in range(1, _MAX_FETCH_ALL_PAGES + 1):
        params = {**common, "page": page, "per_page": _FETCH_ALL_PAGE_SIZE}
        body, _ = await _get_json(url, headers=headers, params=params)
        batch = _as_list(body)
        collected.extend(_gitlab_repo_from(item) for item in batch)
        if len(batch) < _FETCH_ALL_PAGE_SIZE:
            break
    return RepositoryPage(repositories=collected)


async def _fetch_all_pages(
    url: str,
    *,
    headers: dict[str, str],
    extra_params: dict | None = None,
    page_param: str = "page",
    size_param: str = "per_page",
) -> list[dict]:
    """Walk a page-numbered list endpoint until it returns a short page."""
    collected: list[dict] = []
    for page in range(1, _MAX_FETCH_ALL_PAGES + 1):
        params: dict[str, Any] = dict(extra_params or {})
        params[page_param] = page
        params[size_param] = _FETCH_ALL_PAGE_SIZE
        body, _ = await _get_json(url, headers=headers, params=params)
        batch = _as_list(body)
        collected.extend(batch)
        if len(batch) < _FETCH_ALL_PAGE_SIZE:
            break
    return collected


async def _gitea_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    base = _gitea_api_base(opts.base_url)
    headers = _gitea_headers(opts.token)
    items = await _fetch_all_pages(f"{base}/user/repos", headers=headers)
    repos = [_gitea_repo_from(item) for item in items]
    if opts.page > 0:
        return paginate_repos(repos, opts.keyword, opts.page, opts.size or 20)
    return RepositoryPage(repositories=repos)


async def _gitee_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    base = _gitee_api_base(opts.base_url)
    # Gitee carries the credential as an ``access_token`` query parameter.
    items = await _fetch_all_pages(
        f"{base}/user/repos",
        headers={"Accept": "application/json"},
        extra_params={"access_token": opts.token, "affiliation": "owner,collaborator,organization_member"},
    )
    repos = [_gitee_repo_from(item) for item in items]
    if opts.page > 0:
        return paginate_repos(repos, opts.keyword, opts.page, opts.size or 20)
    return RepositoryPage(repositories=repos)


async def _codeup_resolve_organization_id(opts: RepositoryOptions) -> str:
    """Return the organization id to list repositories under.

    A stored ``organization_id`` wins; otherwise we list the organizations the
    token can see and take the first one, which is the tenant the personal
    access token was minted in.
    """
    if (opts.organization_id or "").strip():
        return opts.organization_id.strip()
    base = _codeup_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/platform/organizations", headers=_codeup_headers(opts.token)
    )
    for item in _as_list(body):
        org_id = _first_non_empty(item.get("id"), item.get("organizationId"))
        if org_id:
            return org_id
    raise GitClientError("codeup: no organization is reachable with this token")


async def _codeup_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    org_id = await _codeup_resolve_organization_id(opts)
    base = _codeup_api_base(opts.base_url)
    items = await _fetch_all_pages(
        f"{base}/codeup/organizations/{quote(org_id, safe='')}/repositories",
        headers=_codeup_headers(opts.token),
        size_param="perPage",
    )
    return RepositoryPage(repositories=[_codeup_repo_from(item) for item in items])


async def _cnb_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    base = _cnb_api_base(opts.base_url)
    items = await _fetch_all_pages(
        f"{base}/user/repos", headers=_cnb_headers(opts.token), size_param="page_size"
    )
    return RepositoryPage(repositories=[_cnb_repo_from(item) for item in items])


async def _atomgit_fetch_repositories(opts: RepositoryOptions) -> RepositoryPage:
    base = _atomgit_api_base(opts.base_url)
    items = await _fetch_all_pages(f"{base}/user/repos", headers=_atomgit_headers(opts.token))
    return RepositoryPage(repositories=[_atomgit_repo_from(item) for item in items])


# ---- Branches -------------------------------------------------------------
def _branch_names(body: Any) -> list[Branch]:
    """Map a branch-list body to ``Branch`` rows, tolerating wrapper shapes."""
    names: list[Branch] = []
    for item in _as_list(body):
        name = item.get("name")
        if name:
            names.append(Branch(name=str(name)))
    return names


async def _github_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    base = _github_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/branches", headers=_github_headers(opts.token)
    )
    return _branch_names(body)


async def _gitlab_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    base = _gitlab_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/projects/{_gitlab_project_id(full_name)}/repository/branches",
        headers=_gitlab_headers(opts.token, opts.is_oauth),
    )
    return _branch_names(body)


async def _gitea_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    base = _gitea_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/branches", headers=_gitea_headers(opts.token)
    )
    return _branch_names(body)


async def _gitee_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    base = _gitee_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/branches",
        headers={"Accept": "application/json"},
        params={"access_token": opts.token},
    )
    return _branch_names(body)


async def _codeup_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    org_id = await _codeup_resolve_organization_id(opts)
    base = _codeup_api_base(opts.base_url)
    # Codeup addresses a repository by its organization-scoped path.
    repo_path = quote(full_name, safe="")
    body, _ = await _get_json(
        f"{base}/codeup/organizations/{quote(org_id, safe='')}/repositories/{repo_path}/branches",
        headers=_codeup_headers(opts.token),
    )
    return _branch_names(body)


async def _cnb_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    base = _cnb_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/{full_name}/-/git/branches", headers=_cnb_headers(opts.token)
    )
    return _branch_names(body)


async def _atomgit_fetch_branches(full_name: str, opts: RepositoryOptions) -> list[Branch]:
    base = _atomgit_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/branches", headers=_atomgit_headers(opts.token)
    )
    return _branch_names(body)


# ---- Tree -----------------------------------------------------------------
def _int_or_none(value: Any) -> int | None:
    """Return ``value`` as an int, or ``None`` when it is absent/unparsable."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _contents_entry_to_tree(item: dict) -> TreeEntry:
    """Map one GitHub/Gitea/Gitee ``contents`` entry to a tree entry."""
    mode = _contents_type_to_mode(item.get("type", ""))
    size = None if mode == _MODE_DIRECTORY else _int_or_none(item.get("size"))
    return TreeEntry(
        name=item.get("name") or "",
        path=item.get("path") or "",
        mode=mode,
        sha=_first_non_empty(item.get("sha"), item.get("id")),
        size=size,
    )


def _git_tree_entry_to_tree(item: dict) -> TreeEntry:
    """Map one entry of a recursive ``git/trees`` response to a tree entry.

    The git-database view reports the raw git mode string rather than a type
    word, so ``040000``/``120000``/``160000``/``100755`` drive the mapping.
    """
    raw_type = "tree" if item.get("type") == "tree" else ("commit" if item.get("type") == "commit" else "blob")
    path = item.get("path") or ""
    return TreeEntry(
        name=path.rsplit("/", 1)[-1],
        path=path,
        mode=_gitlab_mode(raw_type, item.get("mode", "")),
        sha=item.get("sha") or "",
        size=_int_or_none(item.get("size")),
    )


async def _github_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    base = _github_api_base(opts.base_url)
    headers = _github_headers(opts.token)
    if recursive:
        # The git-database tree view is the only way to get a whole subtree in
        # one call; an empty ref means the repository's default branch (HEAD).
        tree_ref = ref or "HEAD"
        body, _ = await _get_json(
            f"{base}/repos/{full_name}/git/trees/{quote(tree_ref, safe='')}",
            headers=headers,
            params={"recursive": "1"},
        )
        entries = body.get("tree") if isinstance(body, dict) else None
        prefix = path.strip("/")
        result = [
            _git_tree_entry_to_tree(item)
            for item in (entries or [])
            if isinstance(item, dict)
            and (not prefix or str(item.get("path", "")).startswith(prefix + "/"))
        ]
        return result
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=headers,
        params=params,
    )
    return [_contents_entry_to_tree(item) for item in _as_list(body)]


async def _gitlab_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    base = _gitlab_api_base(opts.base_url)
    url = f"{base}/projects/{_gitlab_project_id(full_name)}/repository/tree"
    # 树列表分页：翻到取空或 _MAX_FETCH_ALL_PAGES 上限（gitlab 同样每页
    # _FETCH_ALL_PAGE_SIZE 条，只取第一页会漏掉后面的源码/清单）。
    out: list[TreeEntry] = []
    prev_first_id = ""
    for page in range(1, _MAX_FETCH_ALL_PAGES + 1):
        params: dict[str, Any] = {"per_page": _FETCH_ALL_PAGE_SIZE, "page": page}
        if ref:
            params["ref"] = ref
        if path:
            params["path"] = path.strip("/")
        if recursive:
            params["recursive"] = "true"
        body, _ = await _get_json(
            url,
            headers=_gitlab_headers(opts.token, opts.is_oauth),
            params=params,
        )
        batch = _as_list(body)
        if not batch:
            break
        # 服务端不认分页、每页回相同首条 id 时止步（防死循环）。
        first_id = str(batch[0].get("id") or "") if isinstance(batch[0], dict) else ""
        if page > 1 and first_id == prev_first_id:
            break
        out.extend(
            TreeEntry(
                name=item.get("name") or "",
                path=item.get("path") or "",
                mode=_gitlab_mode(item.get("type", ""), item.get("mode", "")),
                sha=item.get("id") or "",
            )
            for item in batch
        )
        prev_first_id = first_id
        if len(batch) != _FETCH_ALL_PAGE_SIZE:
            break
    return out


async def _gitea_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    base = _gitea_api_base(opts.base_url)
    headers = _gitea_headers(opts.token)
    if recursive:
        # 递归树分页（每页 _FETCH_ALL_PAGE_SIZE）：gitea 1.26+ 的 git/trees 接口
        # 真分页，单页只回 100 条且按路径序——只取第一页会漏掉排在后面的大
        # 批源码/清单。翻到取空或 _MAX_FETCH_ALL_PAGES 上限为止。
        url = f"{base}/repos/{full_name}/git/trees/{quote(ref or 'HEAD', safe='')}"
        prefix = path.strip("/")
        out: list[TreeEntry] = []
        prev_first_sha = ""
        for page in range(1, _MAX_FETCH_ALL_PAGES + 1):
            body, _ = await _get_json(
                url,
                headers=headers,
                params={"recursive": "true", "per_page": _FETCH_ALL_PAGE_SIZE, "page": page},
            )
            entries = body.get("tree") if isinstance(body, dict) else None
            if not entries:
                break
            # 老版 gitea 无视分页参数整树返回：条目超过单页上限说明这就是全树；
            # 翻页后首条 sha 没变说明服务端不认 page——都别重复抓同一页。
            first_sha = str(entries[0].get("sha") or "") if isinstance(entries[0], dict) else ""
            if page > 1 and first_sha == prev_first_sha:
                break
            for item in entries:
                if not isinstance(item, dict):
                    continue
                if prefix and not str(item.get("path", "")).startswith(prefix + "/"):
                    continue
                out.append(_git_tree_entry_to_tree(item))
            prev_first_sha = first_sha
            if len(entries) != _FETCH_ALL_PAGE_SIZE:
                break
        return out
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=headers,
        params=params,
    )
    return [_contents_entry_to_tree(item) for item in _as_list(body)]


async def _gitee_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    base = _gitee_api_base(opts.base_url)
    headers = {"Accept": "application/json"}
    if recursive:
        # 递归树同样分页：翻到取空或 _MAX_FETCH_ALL_PAGES 上限（对齐 gitea）。
        url = f"{base}/repos/{full_name}/git/trees/{quote(ref or 'master', safe='')}"
        prefix = path.strip("/")
        out: list[TreeEntry] = []
        prev_first_sha = ""
        for page in range(1, _MAX_FETCH_ALL_PAGES + 1):
            body, _ = await _get_json(
                url,
                headers=headers,
                params={
                    "access_token": opts.token, "recursive": 1,
                    "per_page": _FETCH_ALL_PAGE_SIZE, "page": page,
                },
            )
            entries = body.get("tree") if isinstance(body, dict) else None
            if not entries:
                break
            # 同 gitea：老版 gitee 无视分页参数整树返回时不重复翻页。
            first_sha = str(entries[0].get("sha") or "") if isinstance(entries[0], dict) else ""
            if page > 1 and first_sha == prev_first_sha:
                break
            for item in entries:
                if not isinstance(item, dict):
                    continue
                if prefix and not str(item.get("path", "")).startswith(prefix + "/"):
                    continue
                out.append(_git_tree_entry_to_tree(item))
            prev_first_sha = first_sha
            if len(entries) != _FETCH_ALL_PAGE_SIZE:
                break
        return out
    params: dict[str, Any] = {"access_token": opts.token}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=headers,
        params=params,
    )
    return [_contents_entry_to_tree(item) for item in _as_list(body)]


async def _codeup_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    org_id = await _codeup_resolve_organization_id(opts)
    base = _codeup_api_base(opts.base_url)
    params: dict[str, Any] = {"path": path.strip("/") or "/", "type": "RECURSIVE" if recursive else "DIRECT"}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/codeup/organizations/{quote(org_id, safe='')}/repositories/{quote(full_name, safe='')}/files/tree",
        headers=_codeup_headers(opts.token),
        params=params,
    )
    entries: list[TreeEntry] = []
    for item in _as_list(body):
        item_type = str(item.get("type") or "").lower()
        entries.append(
            TreeEntry(
                name=item.get("name") or "",
                path=item.get("path") or "",
                mode=_gitlab_mode(item_type, item.get("mode", "")),
                sha=_first_non_empty(item.get("sha"), item.get("id")),
                size=_int_or_none(item.get("size")),
            )
        )
    return entries


def _cnb_content_entry_to_tree(item: dict) -> TreeEntry:
    """Map one cnb.cool content entry; directories arrive under ``entries``."""
    return TreeEntry(
        name=item.get("name") or "",
        path=item.get("path") or "",
        mode=_contents_type_to_mode(item.get("type", "")),
        sha=item.get("sha") or "",
        size=_int_or_none(item.get("size")),
    )


async def _cnb_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    base = _cnb_api_base(opts.base_url)
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    clean_path = path.strip("/")
    if clean_path:
        body, _ = await _get_json(
            f"{base}/{full_name}/-/git/contents/{quote(clean_path)}",
            headers=_cnb_headers(opts.token),
            params=params,
        )
    else:
        body, _ = await _get_json(
            f"{base}/{full_name}/-/git/contents",
            headers=_cnb_headers(opts.token),
            params=params,
        )
    if isinstance(body, dict):
        entries = body.get("entries")
        if isinstance(entries, list):
            return [
                _cnb_content_entry_to_tree(item) for item in entries if isinstance(item, dict)
            ]
        return [_cnb_content_entry_to_tree(body)]
    return [_cnb_content_entry_to_tree(item) for item in _as_list(body)]


async def _atomgit_fetch_tree(
    full_name: str, opts: RepositoryOptions, *, ref: str, path: str, recursive: bool
) -> list[TreeEntry]:
    base = _atomgit_api_base(opts.base_url)
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    if recursive:
        params["recursive"] = 1
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=_atomgit_headers(opts.token),
        params=params,
    )
    return [_contents_entry_to_tree(item) for item in _as_list(body)]


# ---- Blob -----------------------------------------------------------------
def _blob_from_contents(body: Any) -> Blob | None:
    """Build a blob from a GitHub-shaped ``contents`` response.

    Returns ``None`` when the target is not a regular file (a directory listing
    arrives as a list, a submodule/symlink as a non-``file`` type).
    """
    if not isinstance(body, dict):
        return None
    if str(body.get("type") or "file").lower() != "file":
        return None
    encoding = str(body.get("encoding") or "").lower()
    raw = body.get("content")
    if encoding == "base64" or raw is None:
        content = _clean_base64(raw)
    else:
        # A plain-text payload still travels to the frontend as base64.
        content = base64.b64encode(str(raw).encode("utf-8")).decode("ascii")
    return Blob(
        content=content,
        is_binary=_is_probably_binary(content),
        sha=_first_non_empty(body.get("sha"), body.get("id")),
        size=_int_or_none(body.get("size")),
    )


async def _github_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    base = _github_api_base(opts.base_url)
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=_github_headers(opts.token),
        params=params,
    )
    return _blob_from_contents(body)


async def _gitlab_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    base = _gitlab_api_base(opts.base_url)
    # GitLab's single-file endpoint returns base64 content plus the blob id.
    body, _ = await _get_json(
        f"{base}/projects/{_gitlab_project_id(full_name)}/repository/files/{quote(path.strip('/'), safe='')}",
        headers=_gitlab_headers(opts.token, opts.is_oauth),
        params={"ref": ref or "HEAD"},
    )
    if not isinstance(body, dict):
        return None
    content = _clean_base64(body.get("content"))
    return Blob(
        content=content,
        is_binary=_is_probably_binary(content),
        sha=_first_non_empty(body.get("blob_id"), body.get("commit_id")),
        size=_int_or_none(body.get("size")),
    )


async def _gitea_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    base = _gitea_api_base(opts.base_url)
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=_gitea_headers(opts.token),
        params=params,
    )
    return _blob_from_contents(body)


async def _gitee_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    base = _gitee_api_base(opts.base_url)
    params: dict[str, Any] = {"access_token": opts.token}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers={"Accept": "application/json"},
        params=params,
    )
    return _blob_from_contents(body)


async def _codeup_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    org_id = await _codeup_resolve_organization_id(opts)
    base = _codeup_api_base(opts.base_url)
    params: dict[str, Any] = {"filePath": path.strip("/")}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/codeup/organizations/{quote(org_id, safe='')}/repositories/{quote(full_name, safe='')}/files/content",
        headers=_codeup_headers(opts.token),
        params=params,
    )
    if not isinstance(body, dict):
        return None
    payload = body.get("result") if isinstance(body.get("result"), dict) else body
    encoding = str(payload.get("encoding") or "").lower()
    raw = payload.get("content")
    if encoding == "base64" or raw is None:
        content = _clean_base64(raw)
    else:
        content = base64.b64encode(str(raw).encode("utf-8")).decode("ascii")
    return Blob(
        content=content,
        is_binary=_is_probably_binary(content),
        sha=_first_non_empty(payload.get("blobId"), payload.get("commitId")),
        size=_int_or_none(payload.get("size")),
    )


async def _cnb_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    base = _cnb_api_base(opts.base_url)
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/{full_name}/-/git/contents/{quote(path.strip('/'))}",
        headers=_cnb_headers(opts.token),
        params=params,
    )
    if isinstance(body, dict) and isinstance(body.get("entries"), list):
        return None  # a directory, not a file
    return _blob_from_contents(body)


async def _atomgit_fetch_blob(full_name: str, opts: RepositoryOptions, *, path: str, ref: str) -> Blob | None:
    base = _atomgit_api_base(opts.base_url)
    params: dict[str, Any] = {}
    if ref:
        params["ref"] = ref
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/contents/{quote(path.strip('/'))}",
        headers=_atomgit_headers(opts.token),
        params=params,
    )
    return _blob_from_contents(body)


# ---- Repository creation --------------------------------------------------
async def _github_create_repository(opts: CreateRepoOptions) -> CreatedRepo:
    base = _github_api_base(opts.base_url)
    headers = _github_headers(opts.token)
    payload = {
        "name": opts.name,
        "description": opts.description,
        "private": opts.private,
    }
    # An owner means "create inside this organization" rather than under the
    # authenticated user.
    url = f"{base}/orgs/{opts.owner}/repos" if opts.owner else f"{base}/user/repos"
    body = await _post_json(url, headers=headers, json_body=payload)
    item = body if isinstance(body, dict) else {}
    return CreatedRepo(
        full_name=item.get("full_name") or "",
        url=_first_non_empty(item.get("clone_url"), item.get("html_url")),
        description=item.get("description") or "",
        web_url=item.get("html_url") or "",
    )


async def _gitlab_create_repository(opts: CreateRepoOptions) -> CreatedRepo:
    base = _gitlab_api_base(opts.base_url)
    headers = _gitlab_headers(opts.token, opts.is_oauth)
    payload: dict[str, Any] = {
        "name": opts.name,
        "path": opts.name,
        "description": opts.description,
        "visibility": "private" if opts.private else "public",
    }
    if opts.owner:
        # A group path has to be resolved to its numeric id first.
        group, _ = await _get_json(
            f"{base}/groups/{quote(opts.owner, safe='')}", headers=headers
        )
        if isinstance(group, dict) and group.get("id") is not None:
            payload["namespace_id"] = group["id"]
    body = await _post_json(f"{base}/projects", headers=headers, json_body=payload)
    item = body if isinstance(body, dict) else {}
    return CreatedRepo(
        full_name=item.get("path_with_namespace") or "",
        url=_first_non_empty(item.get("http_url_to_repo"), item.get("ssh_url_to_repo")),
        description=item.get("description") or "",
        web_url=item.get("web_url") or "",
    )


async def _gitea_create_repository(opts: CreateRepoOptions) -> CreatedRepo:
    base = _gitea_api_base(opts.base_url)
    headers = _gitea_headers(opts.token)
    payload = {
        "name": opts.name,
        "description": opts.description,
        "private": opts.private,
    }
    url = f"{base}/orgs/{opts.owner}/repos" if opts.owner else f"{base}/user/repos"
    body = await _post_json(url, headers=headers, json_body=payload)
    item = body if isinstance(body, dict) else {}
    return CreatedRepo(
        full_name=item.get("full_name") or "",
        url=item.get("clone_url") or "",
        description=item.get("description") or "",
        web_url=item.get("html_url") or "",
    )


async def _gitee_create_repository(opts: CreateRepoOptions) -> CreatedRepo:
    base = _gitee_api_base(opts.base_url)
    payload: dict[str, Any] = {
        "access_token": opts.token,
        "name": opts.name,
        "description": opts.description,
        "private": opts.private,
    }
    # Gitee distinguishes personal from organization repositories by endpoint.
    if opts.owner:
        url = f"{base}/orgs/{opts.owner}/repos"
    else:
        url = f"{base}/user/repos"
    body = await _post_json(url, headers={"Accept": "application/json"}, json_body=payload)
    item = body if isinstance(body, dict) else {}
    return CreatedRepo(
        full_name=item.get("full_name") or "",
        url=_first_non_empty(item.get("html_url"), item.get("clone_url")),
        description=item.get("description") or "",
        web_url=item.get("html_url") or "",
    )


# ---- Webhooks --------------------------------------------------------------
# Callers speak the generic event vocabulary the console offers ("push",
# "pull_request"). GitHub/Gitea take literal event names; GitLab and Gitee take
# one boolean flag per event, so those two need a translation both ways.
_GENERIC_TO_GITLAB_FLAG = {
    "push": "push_events",
    "tag_push": "tag_push_events",
    "pull_request": "merge_requests_events",
    "merge_request": "merge_requests_events",
    "issues": "issues_events",
    "issue": "issues_events",
    "note": "note_events",
    "comment": "note_events",
    "pipeline": "pipeline_events",
    "release": "releases_events",
}
_GITLAB_FLAG_TO_GENERIC = {
    "push_events": "push",
    "tag_push_events": "tag_push",
    "merge_requests_events": "pull_request",
    "issues_events": "issues",
    "note_events": "note",
    "pipeline_events": "pipeline",
    "releases_events": "release",
}
_GENERIC_TO_GITEE_FLAG = {
    "push": "push_events",
    "tag_push": "tag_push_events",
    "pull_request": "merge_requests_events",
    "merge_request": "merge_requests_events",
    "issues": "issues_events",
    "issue": "issues_events",
    "note": "note_events",
    "comment": "note_events",
}
_GITEE_FLAG_TO_GENERIC = {
    "push_events": "push",
    "tag_push_events": "tag_push",
    "merge_requests_events": "pull_request",
    "issues_events": "issues",
    "note_events": "note",
}
# GitHub/Gitea name pull-request events "pull_request"; the console's generic
# "merge_request" alias maps onto it.
_GENERIC_TO_LITERAL_EVENT = {
    "merge_request": "pull_request",
    "issue": "issues",
    "comment": "issue_comment",
}


def _literal_events(events: list[str] | None) -> list[str]:
    """Normalize generic event names for hosts that take literal event names."""
    resolved: list[str] = []
    for name in events or []:
        mapped = _GENERIC_TO_LITERAL_EVENT.get(name, name)
        if mapped and mapped not in resolved:
            resolved.append(mapped)
    return resolved or ["push"]


def _flag_payload(events: list[str] | None, mapping: dict[str, str]) -> dict[str, bool]:
    """Turn generic event names into the boolean flags a host expects."""
    payload = {flag: False for flag in set(mapping.values())}
    for name in events or ["push"]:
        flag = mapping.get(name)
        if flag:
            payload[flag] = True
    return payload


def _events_from_flags(body: dict, mapping: dict[str, str]) -> list[str]:
    """Read a host's boolean flags back into generic event names."""
    resolved: list[str] = []
    for flag, generic in mapping.items():
        if body.get(flag) and generic not in resolved:
            resolved.append(generic)
    return resolved


def _github_style_webhook(item: dict) -> Webhook:
    """Build a webhook from a GitHub/Gitea hook object (``config.url``)."""
    config = item.get("config") if isinstance(item.get("config"), dict) else {}
    return Webhook(
        hook_id=str(item.get("id") or ""),
        url=_first_non_empty(config.get("url"), item.get("url")),
        events=[str(e) for e in (item.get("events") or [])],
        active=bool(item.get("active")),
        raw=item,
    )


async def _github_list_webhooks(full_name: str, opts: RepositoryOptions) -> list[Webhook]:
    base = _github_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/hooks",
        headers=_github_headers(opts.token),
        params={"per_page": _FETCH_ALL_PAGE_SIZE},
    )
    return [_github_style_webhook(item) for item in _as_list(body)]


async def _github_create_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _github_api_base(opts.base_url)
    config: dict[str, Any] = {"url": callback_url, "content_type": "json"}
    if secret:
        config["secret"] = secret
    payload = {
        "name": "web",
        "active": active,
        "events": _literal_events(events),
        "config": config,
    }
    body = await _post_json(
        f"{base}/repos/{full_name}/hooks",
        headers=_github_headers(opts.token),
        json_body=payload,
    )
    return _github_style_webhook(body if isinstance(body, dict) else {})


async def _github_update_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    hook_id: str,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _github_api_base(opts.base_url)
    config: dict[str, Any] = {"url": callback_url, "content_type": "json"}
    if secret:
        # Omitting the secret leaves the stored one untouched, so a resync that
        # only repairs the URL does not rotate the credential.
        config["secret"] = secret
    payload = {"active": active, "events": _literal_events(events), "config": config}
    body = await _patch_json(
        f"{base}/repos/{full_name}/hooks/{hook_id}",
        headers=_github_headers(opts.token),
        json_body=payload,
    )
    return _github_style_webhook(body if isinstance(body, dict) else {})


async def _github_delete_webhook(full_name: str, opts: RepositoryOptions, *, hook_id: str) -> int:
    base = _github_api_base(opts.base_url)
    return await _delete_json(
        f"{base}/repos/{full_name}/hooks/{hook_id}", headers=_github_headers(opts.token)
    )


def _gitlab_webhook(item: dict) -> Webhook:
    return Webhook(
        hook_id=str(item.get("id") or ""),
        url=item.get("url") or "",
        events=_events_from_flags(item, _GITLAB_FLAG_TO_GENERIC),
        active=bool(item.get("active", True)),
        raw=item,
    )


async def _gitlab_list_webhooks(full_name: str, opts: RepositoryOptions) -> list[Webhook]:
    base = _gitlab_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/projects/{_gitlab_project_id(full_name)}/hooks",
        headers=_gitlab_headers(opts.token, opts.is_oauth),
        params={"per_page": _FETCH_ALL_PAGE_SIZE},
    )
    return [_gitlab_webhook(item) for item in _as_list(body)]


def _gitlab_hook_payload(
    callback_url: str, secret: str | None, events: list[str], active: bool
) -> dict[str, Any]:
    payload: dict[str, Any] = {"url": callback_url, "enable_ssl_verification": True}
    payload.update(_flag_payload(events, _GENERIC_TO_GITLAB_FLAG))
    if secret:
        # GitLab echoes this back in the ``X-Gitlab-Token`` delivery header.
        payload["token"] = secret
    if not active:
        # GitLab has no active flag on hooks; disabling means no events fire.
        payload.update({flag: False for flag in set(_GENERIC_TO_GITLAB_FLAG.values())})
    return payload


async def _gitlab_create_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _gitlab_api_base(opts.base_url)
    body = await _post_json(
        f"{base}/projects/{_gitlab_project_id(full_name)}/hooks",
        headers=_gitlab_headers(opts.token, opts.is_oauth),
        json_body=_gitlab_hook_payload(callback_url, secret, events, active),
    )
    return _gitlab_webhook(body if isinstance(body, dict) else {})


async def _gitlab_update_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    hook_id: str,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _gitlab_api_base(opts.base_url)
    body = await _put_json(
        f"{base}/projects/{_gitlab_project_id(full_name)}/hooks/{hook_id}",
        headers=_gitlab_headers(opts.token, opts.is_oauth),
        json_body=_gitlab_hook_payload(callback_url, secret, events, active),
    )
    return _gitlab_webhook(body if isinstance(body, dict) else {})


async def _gitlab_delete_webhook(full_name: str, opts: RepositoryOptions, *, hook_id: str) -> int:
    base = _gitlab_api_base(opts.base_url)
    return await _delete_json(
        f"{base}/projects/{_gitlab_project_id(full_name)}/hooks/{hook_id}",
        headers=_gitlab_headers(opts.token, opts.is_oauth),
    )


async def _gitea_list_webhooks(full_name: str, opts: RepositoryOptions) -> list[Webhook]:
    base = _gitea_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/hooks",
        headers=_gitea_headers(opts.token),
        params={"limit": _FETCH_ALL_PAGE_SIZE},
    )
    return [_github_style_webhook(item) for item in _as_list(body)]


async def _gitea_create_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _gitea_api_base(opts.base_url)
    config: dict[str, Any] = {"url": callback_url, "content_type": "json"}
    if secret:
        config["secret"] = secret
    payload = {
        "type": "gitea",
        "active": active,
        "events": _literal_events(events),
        "config": config,
    }
    body = await _post_json(
        f"{base}/repos/{full_name}/hooks",
        headers=_gitea_headers(opts.token),
        json_body=payload,
    )
    return _github_style_webhook(body if isinstance(body, dict) else {})


async def _gitea_update_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    hook_id: str,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _gitea_api_base(opts.base_url)
    config: dict[str, Any] = {"url": callback_url, "content_type": "json"}
    if secret:
        # Gitea, like GitHub, keeps the stored secret when it is omitted.
        config["secret"] = secret
    payload = {"active": active, "events": _literal_events(events), "config": config}
    body = await _patch_json(
        f"{base}/repos/{full_name}/hooks/{hook_id}",
        headers=_gitea_headers(opts.token),
        json_body=payload,
    )
    return _github_style_webhook(body if isinstance(body, dict) else {})


async def _gitea_delete_webhook(full_name: str, opts: RepositoryOptions, *, hook_id: str) -> int:
    base = _gitea_api_base(opts.base_url)
    return await _delete_json(
        f"{base}/repos/{full_name}/hooks/{hook_id}", headers=_gitea_headers(opts.token)
    )


def _gitee_webhook(item: dict) -> Webhook:
    return Webhook(
        hook_id=str(item.get("id") or ""),
        url=item.get("url") or "",
        events=_events_from_flags(item, _GITEE_FLAG_TO_GENERIC),
        active=bool(item.get("active", True)),
        raw=item,
    )


async def _gitee_list_webhooks(full_name: str, opts: RepositoryOptions) -> list[Webhook]:
    base = _gitee_api_base(opts.base_url)
    body, _ = await _get_json(
        f"{base}/repos/{full_name}/hooks",
        headers={"Accept": "application/json"},
        params={"access_token": opts.token, "per_page": _FETCH_ALL_PAGE_SIZE},
    )
    return [_gitee_webhook(item) for item in _as_list(body)]


def _gitee_hook_payload(
    opts: RepositoryOptions,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "access_token": opts.token,
        "url": callback_url,
        "active": active,
        # ``encryption_type=0`` = password mode; the secret is echoed in the
        # ``X-Gitee-Token`` delivery header for signature verification.
        "encryption_type": 0,
    }
    payload.update(_flag_payload(events, _GENERIC_TO_GITEE_FLAG))
    if secret:
        payload["password"] = secret
    return payload


async def _gitee_create_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _gitee_api_base(opts.base_url)
    body = await _post_json(
        f"{base}/repos/{full_name}/hooks",
        headers={"Accept": "application/json"},
        json_body=_gitee_hook_payload(opts, callback_url, secret, events, active),
    )
    return _gitee_webhook(body if isinstance(body, dict) else {})


async def _gitee_update_webhook(
    full_name: str,
    opts: RepositoryOptions,
    *,
    hook_id: str,
    callback_url: str,
    secret: str | None,
    events: list[str],
    active: bool,
) -> Webhook:
    base = _gitee_api_base(opts.base_url)
    body = await _patch_json(
        f"{base}/repos/{full_name}/hooks/{hook_id}",
        headers={"Accept": "application/json"},
        json_body=_gitee_hook_payload(opts, callback_url, secret, events, active),
    )
    return _gitee_webhook(body if isinstance(body, dict) else {})


async def _gitee_delete_webhook(full_name: str, opts: RepositoryOptions, *, hook_id: str) -> int:
    base = _gitee_api_base(opts.base_url)
    return await _delete_json(
        f"{base}/repos/{full_name}/hooks/{hook_id}",
        headers={"Accept": "application/json"},
        params={"access_token": opts.token},
    )


# ---- Public dispatch API --------------------------------------------------
def supports_platform(platform: str) -> bool:
    """Return ``True`` when ``platform`` is a supported Git host slug."""
    return (platform or "").lower() in _SUPPORTED_PLATFORMS


_REPO_FETCHERS = {
    "github": _github_fetch_repositories,
    "gitlab": _gitlab_fetch_repositories,
    "gitea": _gitea_fetch_repositories,
    "gitee": _gitee_fetch_repositories,
    "codeup": _codeup_fetch_repositories,
    "cnb": _cnb_fetch_repositories,
    "atomgit": _atomgit_fetch_repositories,
}
_BRANCH_FETCHERS = {
    "github": _github_fetch_branches,
    "gitlab": _gitlab_fetch_branches,
    "gitea": _gitea_fetch_branches,
    "gitee": _gitee_fetch_branches,
    "codeup": _codeup_fetch_branches,
    "cnb": _cnb_fetch_branches,
    "atomgit": _atomgit_fetch_branches,
}
_TREE_FETCHERS = {
    "github": _github_fetch_tree,
    "gitlab": _gitlab_fetch_tree,
    "gitea": _gitea_fetch_tree,
    "gitee": _gitee_fetch_tree,
    "codeup": _codeup_fetch_tree,
    "cnb": _cnb_fetch_tree,
    "atomgit": _atomgit_fetch_tree,
}
_BLOB_FETCHERS = {
    "github": _github_fetch_blob,
    "gitlab": _gitlab_fetch_blob,
    "gitea": _gitea_fetch_blob,
    "gitee": _gitee_fetch_blob,
    "codeup": _codeup_fetch_blob,
    "cnb": _cnb_fetch_blob,
    "atomgit": _atomgit_fetch_blob,
}
_REPO_CREATORS = {
    "github": _github_create_repository,
    "gitlab": _gitlab_create_repository,
    "gitea": _gitea_create_repository,
    "gitee": _gitee_create_repository,
}
_HOOK_LISTERS = {
    "github": _github_list_webhooks,
    "gitlab": _gitlab_list_webhooks,
    "gitea": _gitea_list_webhooks,
    "gitee": _gitee_list_webhooks,
}
_HOOK_CREATORS = {
    "github": _github_create_webhook,
    "gitlab": _gitlab_create_webhook,
    "gitea": _gitea_create_webhook,
    "gitee": _gitee_create_webhook,
}
_HOOK_UPDATERS = {
    "github": _github_update_webhook,
    "gitlab": _gitlab_update_webhook,
    "gitea": _gitea_update_webhook,
    "gitee": _gitee_update_webhook,
}
_HOOK_DELETERS = {
    "github": _github_delete_webhook,
    "gitlab": _gitlab_delete_webhook,
    "gitea": _gitea_delete_webhook,
    "gitee": _gitee_delete_webhook,
}


async def fetch_repositories(platform: str, opts: RepositoryOptions) -> RepositoryPage:
    """List the repositories an identity's credential can reach.

    Returns an empty page (``page_info=None``) on an unsupported platform so the
    caller can render an identity detail without crashing.
    """
    platform = (platform or "").lower()
    fetcher = _REPO_FETCHERS.get(platform)
    if fetcher is None:
        return RepositoryPage(repositories=[], page_info=None)
    return await fetcher(opts)


async def fetch_branches(
    platform: str, full_name: str, opts: RepositoryOptions
) -> list[Branch]:
    """List branches for ``full_name`` under the identity's credential."""
    platform = (platform or "").lower()
    fetcher = _BRANCH_FETCHERS.get(platform)
    if fetcher is None or not (full_name or "").strip():
        return []
    return await fetcher(full_name.strip(), opts)


async def fetch_tree(
    platform: str,
    full_name: str,
    opts: RepositoryOptions,
    *,
    ref: str = "",
    path: str = "",
    recursive: bool = False,
) -> list[TreeEntry]:
    """List one level of ``full_name``'s tree (or the whole subtree recursively)."""
    platform = (platform or "").lower()
    fetcher = _TREE_FETCHERS.get(platform)
    if fetcher is None or not (full_name or "").strip():
        return []
    return await fetcher(full_name.strip(), opts, ref=ref, path=path, recursive=recursive)


async def fetch_blob(
    platform: str,
    full_name: str,
    opts: RepositoryOptions,
    *,
    path: str,
    ref: str = "",
) -> Blob | None:
    """Fetch a single file blob, or ``None`` when the target is not a regular file."""
    platform = (platform or "").lower()
    fetcher = _BLOB_FETCHERS.get(platform)
    if fetcher is None or not (full_name or "").strip() or not (path or "").strip():
        return None
    return await fetcher(full_name.strip(), opts, path=path.strip(), ref=ref)


# ---- Repository capability probe (read/write) ---------------------------
async def fetch_repo_capability(
    platform: str,
    full_name: str,
    opts: RepositoryOptions,
) -> tuple[bool, bool]:
    """Probe whether ``opts.token`` can read and write ``full_name``.

    Returns ``(can_read, can_write)``. Read is probed via ``fetch_tree`` (a
    single-level contents call). Write is probed per-platform against the
    repository's permission payload; platforms whose write-permission shape is
    not implemented fail closed to ``can_write=False`` so push stays disabled
    while read still works. Network errors degrade to ``(False, False)``.
    """
    platform = (platform or "").lower()
    full_name = (full_name or "").strip()
    if not full_name or platform not in _SUPPORTED_PLATFORMS:
        return False, False
    try:
        await fetch_tree(platform, full_name, opts, ref="", path="", recursive=False)
    except GitClientError:
        return False, False
    can_write = await _probe_write_permission(platform, full_name, opts)
    return True, can_write


async def _probe_write_permission(
    platform: str, full_name: str, opts: RepositoryOptions
) -> bool:
    """Best-effort write-permission probe. Fail closed on any uncertainty."""
    try:
        if platform == "github":
            base = _github_api_base(opts.base_url)
            body, _ = await _get_json(
                f"{base}/repos/{full_name}", headers=_github_headers(opts.token)
            )
            perms = body.get("permissions") if isinstance(body, dict) else None
            return bool(isinstance(perms, dict) and perms.get("push"))
        if platform == "gitlab":
            base = _gitlab_api_base(opts.base_url)
            body, _ = await _get_json(
                f"{base}/projects/{_gitlab_project_id(full_name)}",
                headers=_gitlab_headers(opts.token, opts.is_oauth),
            )
            perms = body.get("permissions") if isinstance(body, dict) else None
            access_level = _first_int(perms.get("project_access"), "access_level") if isinstance(perms, dict) else None
            if access_level is None:
                access_level = _first_int(perms.get("group_access"), "access_level") if isinstance(perms, dict) else None
            return access_level is not None and access_level >= 30  # Developer+
        if platform == "gitea":
            base = _gitea_api_base(opts.base_url)
            body, _ = await _get_json(
                f"{base}/repos/{full_name}", headers=_gitea_headers(opts.token)
            )
            perms = body.get("permissions") if isinstance(body, dict) else None
            return bool(isinstance(perms, dict) and perms.get("push"))
        if platform == "gitee":
            base = _gitee_api_base(opts.base_url)
            body, _ = await _get_json(
                f"{base}/repos/{full_name}", headers=_gitea_headers(opts.token)
            )
            perms = body.get("permissions") if isinstance(body, dict) else None
            return bool(isinstance(perms, dict) and perms.get("push"))
    except GitClientError:
        return False
    return False


def _first_int(*values: Any) -> int | None:
    """Return the first dict carrying ``access_level`` as an int, else None."""
    for value in values:
        if isinstance(value, dict):
            level = value.get("access_level")
            if isinstance(level, int) and not isinstance(level, bool):
                return level
    return None


async def create_repository(platform: str, opts: CreateRepoOptions) -> CreatedRepo:
    """Create a new remote repository under the identity's credential."""
    platform = (platform or "").lower()
    creator = _REPO_CREATORS.get(platform)
    if creator is None:
        raise GitClientError(f"platform not supported for repository creation: {platform or 'none'}")
    if not (opts.name or "").strip():
        raise GitClientError("repository name is required")
    if not (opts.token or "").strip():
        raise GitClientError("access token is required")
    return await creator(opts)


async def list_webhooks(
    platform: str, full_name: str, opts: RepositoryOptions
) -> list[Webhook]:
    """List the existing webhooks on ``full_name``."""
    platform = (platform or "").lower()
    lister = _HOOK_LISTERS.get(platform)
    if lister is None or not (full_name or "").strip():
        return []
    return await lister(full_name.strip(), opts)


async def create_webhook(
    platform: str,
    full_name: str,
    opts: RepositoryOptions,
    *,
    url: str,
    secret: str | None,
    events: list[str],
    active: bool = True,
) -> Webhook:
    """Register a new webhook pointing at ``url``.

    ``secret`` is the shared secret the platform echoes back in delivery
    signatures; it is never persisted here, only forwarded to the platform.
    """
    platform = (platform or "").lower()
    creator = _HOOK_CREATORS.get(platform)
    if creator is None:
        raise GitClientError(f"platform not supported for webhooks: {platform or 'none'}")
    return await creator(
        full_name.strip(), opts,
        callback_url=url, secret=secret, events=events, active=active,
    )


async def update_webhook(
    platform: str,
    full_name: str,
    opts: RepositoryOptions,
    *,
    hook_id: str,
    url: str,
    secret: str | None,
    events: list[str],
    active: bool = True,
) -> Webhook:
    """PATCH an existing webhook's URL / events / active flag in place."""
    platform = (platform or "").lower()
    updater = _HOOK_UPDATERS.get(platform)
    if updater is None:
        raise GitClientError(f"platform not supported for webhooks: {platform or 'none'}")
    return await updater(
        full_name.strip(), opts,
        hook_id=str(hook_id), callback_url=url, secret=secret, events=events, active=active,
    )


async def delete_webhook(
    platform: str,
    full_name: str,
    opts: RepositoryOptions,
    *,
    hook_id: str,
) -> int:
    """Delete a webhook; a 404 is returned (the hook is already gone)."""
    platform = (platform or "").lower()
    deleter = _HOOK_DELETERS.get(platform)
    if deleter is None:
        raise GitClientError(f"platform not supported for webhooks: {platform or 'none'}")
    return await deleter(full_name.strip(), opts, hook_id=str(hook_id))
