"""Node.js 宿主工具归档解析（镜像 SHASUMS256.txt 真实清单驱动）回归。

历史缺陷：下载 URL 按 Go GOARCH 直接拼接（amd64），而 Node 官方归档命名是
x64，Intel/Windows/Linux 节点全部 404。现在服务端拉镜像 ``v{version}/
SHASUMS256.txt`` 权威清单，按节点 os/arch 挑出确实存在的归档名并带 sha256
下发。这里对 HTTP 层打桩（路由表精确匹配目标 URL），契约：
- GOARCH 别名归一（amd64→x64、386→x86、arm→armv7l、arm64 原样）；
- 从清单挑真实存在的文件名、回带同表 sha256；
- 节点 os 不在官方发行范围 / 清单无该平台归档 → 明确报错且不发请求或带文件名；
- 代理三形态（network/url_prefix/直连）解析时走与归档下载同一条路线。
"""
from __future__ import annotations

import pytest

import node_upgrade_targets as targets
from node_upgrade_targets import UpgradeTargetError, resolve_nodejs_asset

DEFAULT_SUMS_URL = "https://nodejs.org/dist/v22.17.0/SHASUMS256.txt"


def _sums_body() -> str:
    # 摘自 nodejs.org v22.17.0 SHASUMS256.txt（2026-09-09 实测，与 npmmirror 逐字节一致）。
    return "\n".join(
        [
            "615dda58b5fb41fad2be43940b6398ca56554cbe05800953afadc724729cb09e  node-v22.17.0-darwin-arm64.tar.gz",
            "c39c8ec3cdadedfcc75de0cb3305df95ae2aecebc5db8d68a9b67bd74616d2ad  node-v22.17.0-darwin-x64.tar.gz",
            "3e99df8b01b27dc8b334a2a30d1cd500442b3b0877d217b308fd61a9ccfc33d4  node-v22.17.0-linux-arm64.tar.gz",
            "ce120efe921de3eaaba2394edaacfab3e61376a56199cb93fc7e9bf0b3f14a16  node-v22.17.0-linux-armv7l.tar.gz",
            "0fa01328a0f3d10800623f7107fbcd654a60ec178fab1ef5b9779e94e0419e1a  node-v22.17.0-linux-x64.tar.gz",
            "78355dc9ca117bb71d3f081e4b1b281855e2b134f3939bb0ca314f7567b0e621  node-v22.17.0-win-arm64.zip",
            "721ab118a3aac8584348b132767eadf51379e0616f0db802cc1e66d7f0d98f85  node-v22.17.0-win-x64.zip",
            "7817aab24c310a52f12063a8c1748c80c7bd02666f4869c090cbc4edefa24b62  node-v22.17.0-win-x86.zip",
        ]
    )


class _FakeResponse:
    def __init__(self, body: str, status: int = 200) -> None:
        self._body = body
        self.status = status

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def text(self) -> str:
        return self._body


class _FakeSession:
    """按目标 URL 精确路由 GET；未登记目标抛 KeyError（模拟传输故障）。"""

    def __init__(self) -> None:
        self.routes: dict[str, _FakeResponse] = {}
        self.calls: list[tuple[str, dict]] = []

    def get(self, target: str, **kwargs) -> _FakeResponse:
        self.calls.append((target, kwargs))
        return self.routes[target]


@pytest.fixture
def fake_aiohttp(monkeypatch: pytest.MonkeyPatch) -> _FakeSession:
    session = _FakeSession()

    class _ClientSession:
        async def __aenter__(self) -> _FakeSession:
            return session

        async def __aexit__(self, *_exc) -> None:
            return None

    monkeypatch.setattr(targets.aiohttp, "ClientSession", lambda *args, **kwargs: _ClientSession())
    return session


def _route(session: _FakeSession, url: str, body: str = "", status: int = 200) -> None:
    session.routes[url] = _FakeResponse(body, status)


@pytest.mark.asyncio
async def test_amd64_maps_to_official_x64_archive_with_sha(fake_aiohttp: _FakeSession) -> None:
    """Go 的 amd64 必须解析成官方 x64 归档名——历史 404 的直接回归点。"""
    _route(fake_aiohttp, DEFAULT_SUMS_URL, _sums_body())

    asset = await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "linux", "amd64", {})

    assert asset == {
        "download_url": "https://nodejs.org/dist/v22.17.0/node-v22.17.0-linux-x64.tar.gz",
        "sha256": "0fa01328a0f3d10800623f7107fbcd654a60ec178fab1ef5b9779e94e0419e1a",
    }


@pytest.mark.asyncio
async def test_windows_amd64_maps_to_win_x64_zip(fake_aiohttp: _FakeSession) -> None:
    url = "https://npmmirror.com/mirrors/node/v22.17.0/SHASUMS256.txt"
    _route(fake_aiohttp, url, _sums_body())

    asset = await resolve_nodejs_asset("https://npmmirror.com/mirrors/node/", "v22.17.0", "windows", "amd64", {})

    assert asset["download_url"] == "https://npmmirror.com/mirrors/node/v22.17.0/node-v22.17.0-win-x64.zip"
    assert asset["sha256"] == "721ab118a3aac8584348b132767eadf51379e0616f0db802cc1e66d7f0d98f85"


@pytest.mark.asyncio
async def test_arm64_passthrough_and_arm_maps_to_armv7l(fake_aiohttp: _FakeSession) -> None:
    _route(fake_aiohttp, DEFAULT_SUMS_URL, _sums_body())

    asset = await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "darwin", "arm64", {})
    assert asset["download_url"].endswith("node-v22.17.0-darwin-arm64.tar.gz")

    asset = await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "linux", "arm", {})
    assert asset["download_url"].endswith("node-v22.17.0-linux-armv7l.tar.gz")


@pytest.mark.asyncio
async def test_386_maps_to_x86(fake_aiohttp: _FakeSession) -> None:
    _route(fake_aiohttp, DEFAULT_SUMS_URL, _sums_body())

    asset = await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "windows", "386", {})

    assert asset["download_url"].endswith("node-v22.17.0-win-x86.zip")


@pytest.mark.asyncio
async def test_missing_archive_for_platform_names_the_file(fake_aiohttp: _FakeSession) -> None:
    _route(fake_aiohttp, DEFAULT_SUMS_URL, _sums_body())

    with pytest.raises(UpgradeTargetError) as excinfo:
        await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "linux", "riscv64", {})

    assert "node-v22.17.0-linux-riscv64.tar.gz" in str(excinfo.value)
    assert "SHASUMS256.txt" in str(excinfo.value)


@pytest.mark.asyncio
async def test_unsupported_os_errors_before_any_fetch(fake_aiohttp: _FakeSession) -> None:
    with pytest.raises(UpgradeTargetError) as excinfo:
        await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "freebsd", "amd64", {})

    assert "freebsd" in str(excinfo.value)
    assert fake_aiohttp.calls == []


@pytest.mark.asyncio
async def test_shasums_fetch_404_surfaces_version_hint(fake_aiohttp: _FakeSession) -> None:
    _route(fake_aiohttp, "https://nodejs.org/dist/v99.99.99/SHASUMS256.txt", status=404)

    with pytest.raises(UpgradeTargetError) as excinfo:
        await resolve_nodejs_asset("https://nodejs.org/dist", "99.99.99", "linux", "amd64", {})

    assert "404" in str(excinfo.value)


@pytest.mark.asyncio
async def test_transport_failure_raises_actionable_error(fake_aiohttp: _FakeSession) -> None:
    # 不登记任何路由：GET 抛 KeyError，走传输失败分支。
    with pytest.raises(UpgradeTargetError) as excinfo:
        await resolve_nodejs_asset("https://nodejs.org/dist", "22.17.0", "linux", "amd64", {})

    assert "SHASUMS256.txt" in str(excinfo.value)


@pytest.mark.asyncio
async def test_metadata_fetch_routes_through_node_egress_proxy(fake_aiohttp: _FakeSession) -> None:
    """解析必须与归档下载同路线：network 传 proxy=、url_prefix 重写目标。"""
    _route(fake_aiohttp, DEFAULT_SUMS_URL, _sums_body())

    await resolve_nodejs_asset(
        "https://nodejs.org/dist",
        "22.17.0",
        "linux",
        "amd64",
        {"proxy_mode": "network", "proxy_url": "http://127.0.0.1:7890"},
    )
    target, kwargs = fake_aiohttp.calls[-1]
    assert target == DEFAULT_SUMS_URL
    assert kwargs.get("proxy") == "http://127.0.0.1:7890"

    prefixed = "https://gh.example.com/https://nodejs.org/dist/v22.17.0/SHASUMS256.txt"
    _route(fake_aiohttp, prefixed, _sums_body())
    await resolve_nodejs_asset(
        "https://nodejs.org/dist",
        "22.17.0",
        "linux",
        "amd64",
        {"proxy_mode": "url_prefix", "proxy_url_prefix": "https://gh.example.com/"},
    )
    assert fake_aiohttp.calls[-1][0] == prefixed
