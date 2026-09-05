"""MarketplaceGitHub 读缓存行为回归。

管理页的目录/编辑/导出会反复读同一批 GitHub 文件，读缓存让重复读取不再吃 RTT；
这些用例锁住正确性边界：写/删绕缓存取 sha、写后失效、404 也缓存、fresh 绕过。
"""
from __future__ import annotations

import asyncio
import base64
import json

from monkeycode_compat.git_clients import GitClientError
from monkeycode_compat.marketplace import github as gh
from monkeycode_compat.marketplace.config import MarketplaceSettings


def _settings() -> MarketplaceSettings:
    return MarketplaceSettings(
        repo_url="https://github.com/o/r",
        github_owner="o",
        github_repo="r",
        github_branch="main",
        github_token="tok",
        modules=("channels",),
        index_name="index.json",
        proxy_id="",
    )


class _FakeResponse:
    def __init__(self, status: int, payload: dict | None = None):
        self.status = status
        self._payload = payload

    async def json(self):
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload

    async def text(self):
        return "" if self._payload is None else json.dumps(self._payload)


class FakeProxyManager:
    """按 path 记录文件内容与 sha 的假出站管理器；calls 记录每个上游请求。"""

    def __init__(self, files: dict[str, tuple[dict, str]] | None = None):
        self.files: dict[str, tuple[dict, str]] = files or {}
        self.calls: list[tuple[str, str]] = []

    async def request(self, *, url, method, headers=None, timeout=None, proxy_config_id=None, **kwargs):
        path = url.split("/contents/", 1)[1].split("?ref=", 1)[0]
        self.calls.append((method, path))
        if method == "GET":
            entry = self.files.get(path)
            if entry is None:
                return _FakeResponse(404)
            body, sha = entry
            raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
            return _FakeResponse(200, {
                "type": "file", "encoding": "base64",
                "content": base64.b64encode(raw).decode("ascii"), "sha": sha,
            })
        if method == "PUT":
            payload = kwargs["json"]
            body = json.loads(base64.b64decode(payload["content"]).decode("utf-8"))
            self.files[path] = (body, "sha-after-put")
            return _FakeResponse(200, {"content": {"sha": "sha-after-put"}})
        raise AssertionError(f"unexpected method {method}")


def _client(monkeypatch, fake: FakeProxyManager) -> gh.MarketplaceGitHub:
    import providers.proxy_manager as pm

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: fake)
    gh._read_cache.clear()
    return gh.MarketplaceGitHub(_settings())


def teardown_function(_):
    gh._read_cache.clear()


def test_read_json_hits_cache_within_ttl_and_returns_copies(monkeypatch):
    fake = FakeProxyManager({"modules/channels/items/a.json": ({"display_name": "A"}, "sha-1")})
    client = _client(monkeypatch, fake)

    first = asyncio.run(client.read_json("modules/channels/items/a.json"))
    second = asyncio.run(client.read_json("modules/channels/items/a.json"))
    assert [call for call in fake.calls if call[0] == "GET"] and len(fake.calls) == 1
    assert first == second == ({"display_name": "A"}, "sha-1")

    # 返回值是深拷贝：调用方就地改写（routes 里 index.update 这类）不能污染缓存。
    first[0]["display_name"] = "mutated"
    third = asyncio.run(client.read_json("modules/channels/items/a.json"))
    assert third[0]["display_name"] == "A"


def test_read_json_use_cache_false_bypasses_and_refreshes(monkeypatch):
    fake = FakeProxyManager({"modules/channels/items/a.json": ({"v": 1}, "sha-1")})
    client = _client(monkeypatch, fake)

    asyncio.run(client.read_json("modules/channels/items/a.json"))
    fake.files["modules/channels/items/a.json"] = ({"v": 2}, "sha-2")
    fresh = asyncio.run(client.read_json("modules/channels/items/a.json", use_cache=False))
    assert fresh == ({"v": 2}, "sha-2")
    assert len(fake.calls) == 2

    # use_cache=False 回填缓存（强制刷新语义）：点「刷新」后，后续常规读立即拿到新值。
    cached = asyncio.run(client.read_json("modules/channels/items/a.json"))
    assert cached == ({"v": 2}, "sha-2")
    assert len(fake.calls) == 2


def test_404_is_cached_until_write(monkeypatch):
    fake = FakeProxyManager()
    client = _client(monkeypatch, fake)
    path = "modules/channels/index.json"

    assert asyncio.run(client.read_json_or_none(path)) is None
    assert asyncio.run(client.read_json_or_none(path)) is None
    assert len(fake.calls) == 1  # 第二次 404 命中缓存，不再出网

    # 写入后缓存失效：下一次读能看到新文件。
    asyncio.run(client.write_json(path, {"items": []}, "create index"))
    index, sha = asyncio.run(client.read_json_or_none(path))
    assert index == {"items": []} and sha == "sha-after-put"


def test_write_takes_sha_outside_cache_and_invalidates(monkeypatch):
    fake = FakeProxyManager({"modules/channels/items/a.json": ({"v": 1}, "sha-old")})
    client = _client(monkeypatch, fake)

    # 预热缓存（旧内容旧 sha），随后外部直接改了远端文件。
    asyncio.run(client.read_json("modules/channels/items/a.json"))
    fake.files["modules/channels/items/a.json"] = ({"v": "external"}, "sha-external")

    # 写路径的 sha 读取必须绕过缓存，否则旧 sha 会撞 409。
    asyncio.run(client.write_json("modules/channels/items/a.json", {"v": 9}, "update"))
    assert fake.files["modules/channels/items/a.json"] == ({"v": 9}, "sha-after-put")

    # 写后缓存已失效：下一次读拿到写后的新内容，而不是缓存里的 v=1。
    data, sha = asyncio.run(client.read_json("modules/channels/items/a.json"))
    assert data == {"v": 9} and sha == "sha-after-put"


def test_cache_expires_after_ttl(monkeypatch):
    fake = FakeProxyManager({"modules/channels/items/a.json": ({"v": 1}, "sha-1")})
    client = _client(monkeypatch, fake)

    # 人为放入一条已过期的缓存条目（TTL 判定用 time.monotonic，直接构造过期时间戳，
    # 不依赖把 TTL 设 0——Windows 单调时钟精度下 expires==now 会被当作仍有效，不稳定）。
    key = gh._cache_key(client._settings, "modules/channels/items/a.json")
    gh._read_cache[key] = (gh.time.monotonic() - 1, {"v": "stale"}, "sha-old")

    data, sha = asyncio.run(client.read_json("modules/channels/items/a.json"))
    assert data == {"v": 1} and sha == "sha-1"
    assert len(fake.calls) == 1  # 过期条目不命中，回源并刷新缓存


def test_read_cache_is_scoped_per_repo(monkeypatch):
    import providers.proxy_manager as pm

    fake = FakeProxyManager({"modules/channels/items/a.json": ({"v": 1}, "sha-1")})
    client = _client(monkeypatch, fake)
    asyncio.run(client.read_json("modules/channels/items/a.json"))
    assert len(fake.calls) == 1

    # 缓存 key 含 owner/repo：另一个仓库的同名 path 必须回源，不能吃到 r 仓库的缓存。
    other_fake = FakeProxyManager()
    monkeypatch.setattr(pm, "get_proxy_manager", lambda: other_fake)
    other = gh.MarketplaceGitHub(
        MarketplaceSettings(
            repo_url="https://github.com/o/other", github_owner="o", github_repo="other",
            github_branch="main", github_token="tok", modules=("channels",),
            index_name="index.json", proxy_id="",
        )
    )
    try:
        asyncio.run(other.read_json("modules/channels/items/a.json"))
    except GitClientError as exc:
        assert "404" in str(exc)
    else:
        raise AssertionError("another repo must not hit this repo's cache")
    assert len(other_fake.calls) == 1
