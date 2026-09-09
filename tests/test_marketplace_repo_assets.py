"""发行资产入仓（repo_path）与消费侧平台渲染（github/gitee）回归。

设计契约：
- manifest/version.json 的 asset 带 ``repo_path``（仓库相对路径，存储真相），
  绝对 ``download_url`` 由消费端按平台渲染；旧资产只有 download_url（GitHub
  Release 直链时代），双口径并存。
- catalog 摄取点（refresh/apply_release）先渲染 download_url 再校验落库，
  下游 select/coverage/升级帧零改动。
- 上传链路把二进制 ``write_bytes`` 进 ``node-releases/<v>/files/``（或
  ``mobile-releases/...``），manifest asset 不再写 download_url/asset_id；
  硬删除把仓里的资产文件与版本 manifest 一并清掉。
"""
from __future__ import annotations

import asyncio
import base64
import copy

from user_platform.marketplace import urls
from user_platform.marketplace.config import (
    MarketplaceConsumerSettings,
    MarketplaceSettings,
    detect_platform,
    parse_repo_url,
)
from user_platform.marketplace.validator import (
    validate_manifest,
    validate_mobile_assets,
    validate_mobile_release,
    validate_node_assets,
    validate_node_release,
    validate_repo_path,
)

_SHA = "sha256:" + "a" * 64


def _consumer(platform: str = "gitee", branch: str = "main") -> MarketplaceConsumerSettings:
    return MarketplaceConsumerSettings(
        repo_url="https://gitee.com/mirror/market",
        github_owner="mirror",
        github_repo="market",
        github_branch=branch,
        modules=("node-versions",),
        index_name="index.json",
        platform=platform,
    )


# ── 平台识别与 URL 渲染 ───────────────────────────────────────────────────────


def test_detect_platform_recognizes_gitee_hosts():
    assert detect_platform("https://gitee.com/xiongrun/ai-lubricant-marketplace") == "gitee"
    assert detect_platform("https://www.gitee.com/xiongrun/market") == "gitee"
    assert detect_platform("git@gitee.com:xiongrun/market.git") == "gitee"
    assert detect_platform("gitee.com/xiongrun/market") == "gitee"
    assert detect_platform("https://github.com/wuxin-gh/market") == "github"
    assert detect_platform("github.com/wuxin-gh/market") == "github"
    assert detect_platform("") == "github"
    # 路径里出现 gitee 字样不误判：host 段才看。
    assert detect_platform("https://github.com/o/gitee-mirror") == "github"


def test_parse_repo_url_accepts_gitee_forms():
    assert parse_repo_url("https://gitee.com/xiongrun/market") == ("xiongrun", "market", "")
    assert parse_repo_url("gitee.com/xiongrun/market") == ("xiongrun", "market", "")
    assert parse_repo_url("git@gitee.com:xiongrun/market.git") == ("xiongrun", "market", "")
    # /tree/<branch> 分支页地址照常识别分支。
    assert parse_repo_url("https://gitee.com/xiongrun/market/tree/dev") == ("xiongrun", "market", "dev")


def test_raw_url_renders_both_platforms():
    path = "node-releases/20260830-1200/files/node-execution-linux-amd64"
    assert urls.raw_url("github", "o", "r", "main", path) == (
        f"https://raw.githubusercontent.com/o/r/main/{path}"
    )
    assert urls.raw_url("gitee", "o", "r", "main", path) == (
        f"https://gitee.com/o/r/raw/main/{path}"
    )
    # branch 空回落 main；未知平台按 github 渲染。
    assert "/raw/main/" in urls.raw_url("gitee", "o", "r", "", "x.json")
    assert "raw.githubusercontent.com" in urls.raw_url("unknown", "o", "r", "main", "x.json")
    # repo 网页地址（前端展示用）。
    assert urls.repo_web_url("gitee", "o", "r") == "https://gitee.com/o/r"
    assert urls.repo_web_url("github", "o", "r") == "https://github.com/o/r"


def test_render_asset_download_url_prefers_repo_path_with_fallback():
    settings = _consumer("gitee")
    in_repo = {"repo_path": "node-releases/v1/files/a.bin"}
    assert urls.render_asset_download_url(in_repo, settings) == (
        "https://gitee.com/mirror/market/raw/main/node-releases/v1/files/a.bin"
    )
    # 旧资产：无 repo_path，原样回退 download_url。
    legacy = {"download_url": "https://github.com/e/r/d/old-asset"}
    assert urls.render_asset_download_url(legacy, settings) == legacy["download_url"]
    # 都没有 → 空串。
    assert urls.render_asset_download_url({}, settings) == ""


# ── 校验器：repo_path 双口径 ──────────────────────────────────────────────────


def _node_asset(**over) -> dict:
    base = {
        "filename": "node-execution-linux-amd64", "component": "node", "role": "execution",
        "platform": "linux", "arch": "amd64", "format": "executable",
        "digest": _SHA, "size_bytes": 1024,
    }
    base.update(over)
    return base


def test_repo_path_validation_rules():
    ok = "node-releases/20260830-1200/files/node-execution-linux-amd64"
    assert validate_repo_path(ok, prefix="node-releases/") == ""
    # 前缀不对 / 路径穿越 / 绝对路径 / 非法字符，全部拒绝。
    assert "必须位于" in validate_repo_path("clients/foo.exe", prefix="node-releases/")
    assert "必须位于" in validate_repo_path("mobile-releases/v/f/x", prefix="node-releases/")
    assert validate_repo_path("node-releases/../evil", prefix="node-releases/")
    assert validate_repo_path("/etc/passwd", prefix="node-releases/")
    assert validate_repo_path("node-releases/v/files/evil name.exe", prefix="node-releases/")
    assert validate_repo_path("  ", prefix="node-releases/")
    # 移动端口径同理。
    assert validate_repo_path("mobile-releases/260608/files/ai-lubricant-260608-android.apk", prefix="mobile-releases/") == ""


def test_node_asset_repo_path_replaces_download_url_requirement():
    # 新的通用 runtime 文件名映射到 any/any，旧 runtime 命名保持兼容。
    from user_platform.marketplace.validator import identify_node_asset

    universal = identify_node_asset("node-runtime.tar.gz")
    assert universal["role"] == "runtime"
    assert universal["platform"] == "any"
    assert universal["arch"] == "any"
    legacy = identify_node_asset("agent-compose-runtime-linux-amd64.tar.gz")
    assert legacy["platform"] == "linux"
    assert legacy["arch"] == "amd64"

    # 旧口径：download_url 必填，repo_path 不存在时仍强制。
    errs = validate_node_assets([_node_asset(download_url="")])
    assert any("download_url 必须是 HTTPS URL" in e for e in errs)
    # 新口径：repo_path 在 → download_url 可省。
    assert validate_node_assets([_node_asset(repo_path="node-releases/v1/files/node-execution-linux-amd64")]) == []
    # 两者都带也合法（catalog 渲染后就长这样）。
    both = _node_asset(
        repo_path="node-releases/v1/files/node-execution-linux-amd64",
        download_url="https://gitee.com/o/r/raw/main/node-releases/v1/files/node-execution-linux-amd64",
    )
    assert validate_node_assets([both]) == []
    # repo_path 非法仍然报错，即使带了合法 download_url。
    errs = validate_node_assets([_node_asset(repo_path="clients/foo.exe", download_url="https://x.example/a")])
    assert any("repo_path" in e for e in errs)
    # version.json 校验走同一套 assets 规则。
    payload = {
        "schema": "ai-lubricant.node-release/v1", "version": "20260830-1200",
        "version_notes": "n", "assets": [_node_asset(repo_path="node-releases/v1/files/node-execution-linux-amd64")],
    }
    assert validate_node_release(payload) == []


def test_mobile_asset_repo_path_replaces_download_url_requirement():
    def apk(**over) -> dict:
        base = {
            "filename": "ai-lubricant-260608-android.apk", "platform": "android",
            "format": "apk", "digest": _SHA, "size_bytes": 2048,
        }
        base.update(over)
        return base

    # 旧口径仍要求 download_url；新口径 repo_path 免除。
    errs = validate_mobile_assets([apk(download_url="")])
    assert any("download_url 必须是 HTTPS URL" in e for e in errs)
    assert validate_mobile_assets([apk(repo_path="mobile-releases/260608/files/ai-lubricant-260608-android.apk")]) == []
    payload = {
        "schema": "ai-lubricant.mobile-release/v1", "version": "260608", "version_notes": "n",
        "assets": [apk(repo_path="mobile-releases/260608/files/ai-lubricant-260608-android.apk")],
    }
    assert validate_mobile_release(payload) == []


def test_node_version_manifest_with_repo_path_is_accepted():
    manifest = {
        "schema": "ai-lubricant.node-version/v1", "id": "node-suite-20260830-1200",
        "kind": "node_program_version", "name": "node-suite",
        "display_name": "节点程序套件 20260830-1200", "version": "20260830-1200",
        "version_notes": "note", "status": "published", "test_version": False,
        "assets": [
            _node_asset(
                repo_path="node-releases/20260830-1200/files/node-execution-linux-amd64",
                # manifest 里不再有 asset_id；带 repo_path 的资产该字段已无意义。
            ).pop("asset_id", None) or _node_asset(
                repo_path="node-releases/20260830-1200/files/node-execution-linux-amd64",
            ),
        ],
        "release_tag": "node-suite-20260830-1200",
        "category": "node-suite", "tags": ["node"],
    }
    assert validate_manifest("node-versions", manifest) == []


# ── catalog 摄取点归一：渲染发生在校验之前 ────────────────────────────────────


def test_normalize_release_assets_renders_gitee_urls_in_place(monkeypatch):
    monkeypatch.setattr(
        "user_platform.marketplace.config.consumer_settings", _consumer("gitee")
    )
    data = {
        "schema": "ai-lubricant.node-release/v1", "version": "20260830-1200", "version_notes": "n",
        "assets": [
            _node_asset(repo_path="node-releases/20260830-1200/files/node-execution-linux-amd64"),
            # 旧资产保留原 download_url，不被改写。
            _node_asset(download_url="https://github.com/e/r/d/legacy"),
        ],
    }
    out = urls.normalize_release_assets(copy.deepcopy(data))
    rendered, legacy = out["assets"]
    assert rendered["download_url"] == (
        "https://gitee.com/mirror/market/raw/main/"
        "node-releases/20260830-1200/files/node-execution-linux-amd64"
    )
    assert legacy["download_url"] == "https://github.com/e/r/d/legacy"


def test_node_release_catalog_apply_release_renders_platform_urls(monkeypatch):
    """apply_release 在校验前归一：repo_path-only payload 经渲染后通过校验、
    快照里的 download_url 是平台渲染结果。"""
    import sys

    sys.path.insert(0, "server")
    import node_release_catalog as nrc

    monkeypatch.setattr(
        "user_platform.marketplace.config.consumer_settings", _consumer("gitee")
    )

    release = {
        "schema": "ai-lubricant.node-release/v1", "version": "20260830-1200", "version_notes": "n",
        "assets": [_node_asset(repo_path="node-releases/20260830-1200/files/node-execution-linux-amd64")],
    }

    class _PG:
        @staticmethod
        async def set_config(key, value):
            _PG.saved = (key, copy.deepcopy(value))

    class _Sync:
        @staticmethod
        async def publish(event, scope):
            pass

    monkeypatch.setattr(nrc.PostgresClient, "set_config", _PG.set_config)
    monkeypatch.setattr(nrc, "_snapshot", {"version": ""})
    import runtime_sync as _rs

    monkeypatch.setattr(_rs, "EVENT_NODE_RELEASE", "node_release", raising=False)
    monkeypatch.setattr(_rs, "publish", _Sync.publish)

    snap = asyncio.run(nrc.apply_release(release, publish=False))
    asset = snap["assets"][0]
    assert asset["download_url"].startswith("https://gitee.com/mirror/market/raw/")
    assert asset["download_url"].endswith("node-releases/20260830-1200/files/node-execution-linux-amd64")


# ── 上传链路：write_bytes 入仓 + manifest 记 repo_path ─────────────────────────


class _FakeRepoClient:
    """记录 write_bytes / delete_file 调用的假写客户端；manifest/index 走内存。"""

    def __init__(self, files: dict[str, bytes] | None = None):
        self.blobs: dict[str, bytes] = files or {}
        self.json_files: dict[str, dict] = {}
        self.writes: list[tuple[str, int]] = []
        self.deletes: list[str] = []

    async def read_json_or_none(self, path, use_cache: bool = True):
        if path in self.json_files:
            return (copy.deepcopy(self.json_files[path]), "sha")
        return None

    async def read_json(self, path, use_cache: bool = True):
        if path in self.json_files:
            return (copy.deepcopy(self.json_files[path]), "sha")
        raise FileNotFoundError(path)

    async def write_json(self, path, payload, message):
        self.json_files[path] = copy.deepcopy(payload)

    async def write_bytes(self, path, data, message):
        self.blobs[path] = data
        self.writes.append((path, len(data)))

    async def delete_file(self, path, message):
        existed = path in self.json_files or path in self.blobs
        self.deletes.append(path)
        self.json_files.pop(path, None)
        self.blobs.pop(path, None)
        return existed

    # 防御：上传链路绝不允许再调 Release 时代的接口。
    def get_or_create_release(self, *a, **k):
        raise AssertionError("release upload must not be used")

    def upload_release_asset(self, *a, **k):
        raise AssertionError("release upload must not be used")


def _make_upload_job(version: str, files: list[dict]):
    from user_platform.marketplace import upload_jobs

    job_id = upload_jobs.create_job(version, [])
    tmpdir = upload_jobs.tmp_dir(job_id)
    import os

    os.makedirs(tmpdir, exist_ok=True)
    meta = []
    for f in files:
        path = upload_jobs.tmp_path_for(job_id, f["filename"])
        with open(path, "wb") as fh:
            fh.write(f["data"])
        meta.append({
            "filename": f["filename"], "role": f["role"], "platform": f["platform"],
            "arch": f["arch"], "format": f.get("format", ""), "size": len(f["data"]),
            "tmp_path": path, "content_type": "application/octet-stream",
        })
    upload_jobs.set_files(job_id, meta)
    job = upload_jobs.get_job(job_id)
    job["version_notes"] = "note"
    job["status_form"] = "published"
    job["test_version"] = False
    return job_id


def _runtime_asset(**over) -> dict:
    base = {
        "filename": "node-runtime.tar.gz", "component": "agent-compose",
        "role": "runtime", "platform": "any", "arch": "any", "format": "tar.gz",
        "digest": _SHA, "size_bytes": 2048,
    }
    base.update(over)
    return base


def test_universal_runtime_selected_for_any_platform():
    """select_upgrade_assets：通用 runtime（any/any）对所有平台命中；ios_host 不选 runtime。"""
    import sys

    sys.path.insert(0, "server")
    import node_release_catalog as nrc

    latest = {
        "version": "20260831-1200",
        "assets": [
            _runtime_asset(repo_path="node-releases/20260831-1200/files/node-runtime.tar.gz"),
            _node_asset(role="execution", platform="linux", arch="amd64",
                        filename="node-execution-linux-amd64",
                        repo_path="node-releases/20260831-1200/files/node-execution-linux-amd64"),
        ],
    }
    for os_name, arch in (("linux", "amd64"), ("linux", "arm64"), ("darwin", "arm64"), ("windows", "amd64")):
        selected = nrc.select_upgrade_assets(latest, node_role="execution", os_name=os_name, arch=arch)
        assert selected["runtime"] is not None, f"runtime not selected for {os_name}/{arch}"
        assert selected["runtime"]["filename"] == "node-runtime.tar.gz"
    # ios_host：只拿自己的 node-ios 二进制，永远不下发 runtime。
    ios = nrc.select_upgrade_assets(latest, node_role="ios_host", os_name="linux", arch="amd64")
    assert ios["runtime"] is None


def test_collect_install_assets_expands_universal_runtime():
    """_collect_install_assets：通用 runtime 登记到每个 (os, arch) 键，安装脚本照常取值。"""
    import asyncio
    import sys

    sys.path.insert(0, "server")
    from user_platform import nodes_service

    latest = {
        "version": "20260831-1200",
        "assets": [
            _runtime_asset(download_url="https://gitee.com/o/r/raw/main/node-releases/20260831-1200/files/node-runtime.tar.gz"),
        ],
    }
    import node_release_catalog

    async def fake_latest():
        return latest

    orig = node_release_catalog.get_latest_release
    node_release_catalog.get_latest_release = fake_latest
    try:
        maps = asyncio.run(nodes_service._collect_install_assets())
    finally:
        node_release_catalog.get_latest_release = orig
    rt = maps["runtime"]
    assert set(rt.keys()) == {(p, a) for p in ("linux", "darwin", "windows") for a in ("amd64", "arm64")}
    assert all(entry["url"].endswith("node-runtime.tar.gz") for entry in rt.values())


def test_node_upload_transfer_writes_binaries_into_repo(monkeypatch, tmp_path):
    """新架构下第②阶段：二进制发布到独立仓库 GitHub Release，asset 记 download_url；
    manifest 落 store 真相源，marketplace 仓库镜像（item / index / version.json）由 publisher 异步渲染。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import github as mp_github
    from user_platform.marketplace import release_repos
    import marketplace_store as store

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    files = [
        {"filename": "node-execution-linux-amd64", "role": "execution", "platform": "linux", "arch": "amd64", "data": b"ELF-exec"},
        {"filename": "agent-compose-runtime-linux-amd64.tar.gz", "role": "runtime", "platform": "linux", "arch": "amd64", "data": b"rt-tgz"},
    ]
    job_id = _make_upload_job("20260830-1200", files)

    # 假 Release：create_release 产出 upload_url，upload_release_asset 记下文件名→直链。
    uploaded: dict[str, bytes] = {}

    async def fake_get_release_by_tag(owner, repo, token, tag):
        return None  # 新 Release，不复用旧资产

    async def fake_create_release(owner, repo, token, tag, name, body, draft=False, prerelease=False):
        return {"upload_url": "https://uploads.github.com/repos/test/repo/releases/1/assets{?name,label}", "html_url": "https://github.com/test/repo/releases/tag/node-v20260830-1200"}

    async def fake_upload_release_asset(upload_url, token, filename, content, content_type="application/octet-stream"):
        uploaded[filename] = content
        return {
            "browser_download_url": f"https://github.com/test/repo/releases/download/node-v20260830-1200/{filename}",
            "size": len(content),
            "name": filename,
        }

    monkeypatch.setattr(mp_github, "get_release_by_tag", fake_get_release_by_tag)
    monkeypatch.setattr(mp_github, "create_release", fake_create_release)
    monkeypatch.setattr(mp_github, "upload_release_asset", fake_upload_release_asset)
    # store 没有时回落读 marketplace 仓库的旧 release manifest（兼容旧架构重试）。
    client = _FakeRepoClient()
    monkeypatch.setattr(mp_routes, "_client", lambda: client)

    # 独立仓库配置：启用（有 token）。release_repos 在 routes 里是函数内局部 import，
    # 直接打模块属性才能被运行时解析到。
    monkeypatch.setattr(release_repos, "load_node_release_repo", lambda: release_repos.ReleaseRepoSettings(
        repo_url="https://github.com/wuxin-gh/ai-lubricant-nodes",
        github_owner="wuxin-gh", github_repo="ai-lubricant-nodes", github_token="t",
    ))

    stored: dict = {}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_github_transfer(job_id))

    job = upload_jobs.get_job(job_id)
    assert job["status"] == "done"
    # 二进制作为 Release asset 上传（不入 marketplace 仓库）。
    assert uploaded["node-execution-linux-amd64"] == b"ELF-exec"
    assert uploaded["agent-compose-runtime-linux-amd64.tar.gz"] == b"rt-tgz"
    # manifest 落 store：asset 记 download_url（GitHub Release 直链），无 repo_path。
    manifest = stored[("node-versions", "node-suite-20260830-1200")]["manifest"]
    assert manifest["release_tag"] == "node-v20260830-1200"
    by_role = {a["role"]: a for a in manifest["assets"]}
    assert by_role["execution"]["download_url"].endswith("node-execution-linux-amd64")
    assert "repo_path" not in by_role["execution"]
    assert by_role["runtime"]["download_url"].endswith("agent-compose-runtime-linux-amd64.tar.gz")
    upload_jobs.cleanup_job(job_id)


def test_mobile_upload_transfer_writes_apk_into_repo(monkeypatch, tmp_path):
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import github as mp_github
    from user_platform.marketplace import release_repos
    import marketplace_store as store

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    files = [{
        "filename": "ai-lubricant-260608-android.apk", "role": "mobile-app",
        "platform": "android", "arch": "universal", "format": "apk", "data": b"PK-apk",
    }]
    job_id = _make_upload_job("260608", files)
    job = upload_jobs.get_job(job_id)
    job["ios_store_url"] = ""
    import mobile_release_catalog

    async def _skip_apply_release(payload, *, publish=True):
        return payload

    monkeypatch.setattr(mobile_release_catalog, "apply_release", _skip_apply_release)

    uploaded: dict[str, bytes] = {}

    async def fake_get_release_by_tag(owner, repo, token, tag):
        return None

    async def fake_create_release(owner, repo, token, tag, name, body, draft=False, prerelease=False):
        return {"upload_url": "https://uploads.github.com/repos/test/repo/releases/1/assets{?name,label}"}

    async def fake_upload_release_asset(upload_url, token, filename, content, content_type="application/octet-stream"):
        uploaded[filename] = content
        return {"browser_download_url": f"https://github.com/test/repo/releases/download/mobile-v260608/{filename}", "size": len(content)}

    monkeypatch.setattr(mp_github, "get_release_by_tag", fake_get_release_by_tag)
    monkeypatch.setattr(mp_github, "create_release", fake_create_release)
    monkeypatch.setattr(mp_github, "upload_release_asset", fake_upload_release_asset)
    # store 没有时回落读 marketplace 仓库的旧 release manifest（兼容旧架构重试）。
    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())
    monkeypatch.setattr(release_repos, "load_mobile_release_repo", lambda: release_repos.ReleaseRepoSettings(
        repo_url="https://github.com/wuxin-gh/ai-lubricant-mobile",
        github_owner="wuxin-gh", github_repo="ai-lubricant-mobile", github_token="t",
    ))

    stored: dict = {}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_mobile_github_transfer(job_id))

    assert upload_jobs.get_job(job_id)["status"] == "done", upload_jobs.get_job(job_id)
    assert uploaded["ai-lubricant-260608-android.apk"] == b"PK-apk"
    manifest = stored[("mobile-versions", "mobile-260608")]["manifest"]
    asset = manifest["assets"][0]
    assert asset["download_url"].endswith("ai-lubricant-260608-android.apk")
    assert "repo_path" not in asset
    assert manifest["release_tag"] == "mobile-v260608"
    upload_jobs.cleanup_job(job_id)


def test_device_control_upload_transfer_writes_apk_into_release(monkeypatch, tmp_path):
    """设备控制 App 第②阶段：APK 发布到独立仓库 Release（tag device-control-v<版本>），
    asset 记 download_url；manifest 落 store，marketplace 仓库零写入。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import github as mp_github
    from user_platform.marketplace import release_repos
    import marketplace_store as store

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    files = [{
        "filename": "device-control-0.1.0-android.apk", "role": "device-control-app",
        "platform": "android", "arch": "universal", "format": "apk", "data": b"PK-dc",
    }]
    job_id = _make_upload_job("0.1.0", files)

    uploaded: dict[str, bytes] = {}

    async def fake_get_release_by_tag(owner, repo, token, tag):
        return None

    async def fake_create_release(owner, repo, token, tag, name, body, draft=False, prerelease=False):
        return {"upload_url": "https://uploads.github.com/repos/test/repo/releases/9/assets{?name,label}"}

    async def fake_upload_release_asset(upload_url, token, filename, content, content_type="application/octet-stream"):
        uploaded[filename] = content
        return {"browser_download_url": f"https://github.com/test/repo/releases/download/device-control-v0.1.0/{filename}", "size": len(content)}

    monkeypatch.setattr(mp_github, "get_release_by_tag", fake_get_release_by_tag)
    monkeypatch.setattr(mp_github, "create_release", fake_create_release)
    monkeypatch.setattr(mp_github, "upload_release_asset", fake_upload_release_asset)
    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())
    monkeypatch.setattr(release_repos, "load_device_control_release_repo", lambda: release_repos.ReleaseRepoSettings(
        repo_url="https://github.com/wuxin-gh/ai-lubricant-device-control",
        github_owner="wuxin-gh", github_repo="ai-lubricant-device-control", github_token="t",
    ))

    stored: dict = {}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_device_control_github_transfer(job_id))

    assert upload_jobs.get_job(job_id)["status"] == "done", upload_jobs.get_job(job_id)
    assert uploaded["device-control-0.1.0-android.apk"] == b"PK-dc"
    manifest = stored[("device-control-versions", "device-control-0.1.0")]["manifest"]
    asset = manifest["assets"][0]
    assert asset["download_url"].endswith("device-control-0.1.0-android.apk")
    assert "repo_path" not in asset
    assert manifest["release_tag"] == "device-control-v0.1.0"
    upload_jobs.cleanup_job(job_id)


def test_device_control_ios_ipa_publishes_and_merges_by_platform(monkeypatch, tmp_path):
    """iOS 发布：IPA 走同一 Release（tag device-control-v<版本>）；同版本分次上传
    Android/iOS 时按 platform 合并，manifest tags 带上两个平台。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import github as mp_github
    from user_platform.marketplace import release_repos
    from user_platform.marketplace.validator import (
        identify_device_control_asset,
        validate_device_control_assets,
    )
    import marketplace_store as store

    # 识别与校验：android.apk / ios.ipa 两种命名，平台互斥不重复。
    assert identify_device_control_asset("device-control-0.2.0-ios.ipa") == {
        "platform": "ios", "version": "0.2.0", "format": "ipa",
    }
    assert identify_device_control_asset("device-control-0.2.0-android.apk") == {
        "platform": "android", "version": "0.2.0", "format": "apk",
    }
    assert identify_device_control_asset("device-control-0.2.0-ios.apk") == {}
    assert validate_device_control_assets([
        {"filename": "device-control-0.2.0-android.apk", "platform": "android", "format": "apk",
         "download_url": "https://github.com/x/y/releases/download/t/a.apk", "digest": _SHA, "size_bytes": 6},
        {"filename": "device-control-0.2.0-ios.ipa", "platform": "ios", "format": "ipa",
         "download_url": "https://github.com/x/y/releases/download/t/a.ipa", "digest": _SHA, "size_bytes": 7},
    ]) == []

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    files = [{
        "filename": "device-control-0.2.0-ios.ipa", "role": "device-control-app",
        "platform": "ios", "arch": "universal", "format": "ipa", "data": b"IPA-dc",
    }]
    job_id = _make_upload_job("0.2.0", files)

    uploaded: dict[str, bytes] = {}

    async def fake_get_release_by_tag(owner, repo, token, tag):
        # 同版本已发过 android：Release 已存在（复用，不新建）。
        return {"upload_url": "https://uploads.github.com/repos/test/repo/releases/9/assets{?name,label}",
                "assets": [{"name": "device-control-0.2.0-android.apk"}]}

    async def fake_upload_release_asset(upload_url, token, filename, content, content_type="application/octet-stream"):
        uploaded[filename] = content
        return {"browser_download_url": f"https://github.com/test/repo/releases/download/device-control-v0.2.0/{filename}", "size": len(content)}

    monkeypatch.setattr(mp_github, "get_release_by_tag", fake_get_release_by_tag)

    async def fail_create_release(*a, **kw):
        raise AssertionError("Release 已存在，不应新建")

    monkeypatch.setattr(mp_github, "create_release", fail_create_release)
    monkeypatch.setattr(mp_github, "upload_release_asset", fake_upload_release_asset)
    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())
    monkeypatch.setattr(release_repos, "load_device_control_release_repo", lambda: release_repos.ReleaseRepoSettings(
        repo_url="https://github.com/wuxin-gh/ai-lubricant-device-control",
        github_owner="wuxin-gh", github_repo="ai-lubricant-device-control", github_token="t",
    ))

    stored: dict = {}
    # store 里已有同版本的 android 资产（上一轮上传）。
    android_asset = {
        "filename": "device-control-0.2.0-android.apk", "platform": "android", "format": "apk",
        "download_url": "https://github.com/test/repo/releases/download/device-control-v0.2.0/device-control-0.2.0-android.apk",
        "digest": _SHA, "size_bytes": 6,
    }
    stored[("device-control-versions", "device-control-0.2.0")] = {"manifest": {
        "schema": "ai-lubricant.device-control-version/v1", "id": "device-control-0.2.0",
        "kind": "device_control_app_version", "name": "device-control", "display_name": "设备控制 App 0.2.0",
        "version": "0.2.0", "version_notes": "note", "status": "published",
        "assets": [android_asset], "release_tag": "device-control-v0.2.0",
        "category": "device-control-app", "tags": ["device-control", "android"],
    }}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_device_control_github_transfer(job_id))

    assert upload_jobs.get_job(job_id)["status"] == "done", upload_jobs.get_job(job_id)
    assert uploaded["device-control-0.2.0-ios.ipa"] == b"IPA-dc"
    manifest = stored[("device-control-versions", "device-control-0.2.0")]["manifest"]
    by_platform = {a["platform"]: a for a in manifest["assets"]}
    assert set(by_platform) == {"android", "ios"}
    assert by_platform["ios"]["download_url"].endswith("device-control-0.2.0-ios.ipa")
    assert manifest["tags"] == ["device-control", "android", "ios"]
    upload_jobs.cleanup_job(job_id)


# ── 资源中心代理下载（/consumer/download/*）───────────────────────────────────


async def _drain_body(response) -> bytes:
    chunks: list[bytes] = []
    async for chunk in response.body_iterator:
        chunks.append(chunk)
    return b"".join(chunks)


class _FakeProxyResp:
    """OutboundResponse 形态：status/headers/iter_any/__aexit__。"""

    def __init__(self, status: int = 200, body: bytes = b"", headers: dict | None = None):
        self.status = status
        self.headers = headers or {}
        self._body = body
        self.closed = False

    async def iter_any(self):
        yield self._body

    async def __aexit__(self, *a):
        self.closed = True
        return False


class _FakeProxyManager:
    def __init__(self, resp: _FakeProxyResp):
        self.resp = resp
        self.calls: list[dict] = []

    async def request(self, *, url, method="GET", headers=None, timeout=None, proxy_config_id=None, **kw):
        self.calls.append({"url": url, "proxy_config_id": proxy_config_id, "method": method})
        return self.resp


def _snapshot_with_android(download_url: str) -> dict:
    return {
        "version": "260608", "version_notes": "note", "release_tag": "mobile-v260608",
        "assets": [{
            "filename": "ai-lubricant-260608-android.apk", "platform": "android", "format": "apk",
            "download_url": download_url, "digest": _SHA, "size_bytes": 10,
        }],
        "updated_at": "", "stale": False,
    }


def test_consumer_proxy_download_streams_release_asset(monkeypatch):
    """/consumer/download/mobile-android：服务端经资源中心代理拉 Release 资产流式回传，
    透传 Content-Length / Content-Disposition——手机不需要直连 GitHub。"""
    from user_platform.marketplace import routes as mp_routes
    import mobile_release_catalog as mrc
    import providers.proxy_manager as pm

    url = "https://github.com/test/repo/releases/download/mobile-v260608/ai-lubricant-260608-android.apk"

    async def fake_latest():
        return _snapshot_with_android(url)

    async def fake_refresh(publish=True):
        return _snapshot_with_android(url)

    monkeypatch.setattr(mrc, "get_latest_release", fake_latest)
    monkeypatch.setattr(mrc, "refresh", fake_refresh)

    mgr = _FakeProxyManager(_FakeProxyResp(body=b"PK-apk-apk", headers={"Content-Length": "10"}))
    monkeypatch.setattr(pm, "get_proxy_manager", lambda: mgr)

    response = asyncio.run(mp_routes.consumer_download_mobile_android())
    assert response.media_type == "application/vnd.android.package-archive"
    assert response.headers["content-disposition"] == 'attachment; filename="ai-lubricant-260608-android.apk"'
    assert response.headers["content-length"] == "10"
    assert asyncio.run(_drain_body(response)) == b"PK-apk-apk"
    assert mgr.calls[0]["url"] == url
    assert mgr.calls[0]["method"] == "GET"


def test_consumer_proxy_download_device_control_streams(monkeypatch):
    """/consumer/download/device-control/{platform}：被控端 APK 走 platform=android，
    同口径代理下载；旧路径 device-control-android 仍是它的别名。"""
    from user_platform.marketplace import routes as mp_routes
    import device_control_release_catalog as dcrc
    import providers.proxy_manager as pm

    url = "https://github.com/test/repo/releases/download/device-control-v0.1.0/device-control-0.1.0-android.apk"
    snapshot = _snapshot_with_android(url)
    snapshot["release_tag"] = "device-control-v0.1.0"
    snapshot["assets"][0]["filename"] = "device-control-0.1.0-android.apk"
    snapshot["assets"][0]["size_bytes"] = 6

    async def fake_latest():
        return snapshot

    async def fake_refresh(publish=True):
        return snapshot

    monkeypatch.setattr(dcrc, "get_latest_release", fake_latest)
    monkeypatch.setattr(dcrc, "refresh", fake_refresh)

    resp = _FakeProxyResp(body=b"PK-dc!", headers={})  # 上游 chunked：Content-Length 用 size_bytes 兜底
    mgr = _FakeProxyManager(resp)
    monkeypatch.setattr(pm, "get_proxy_manager", lambda: mgr)

    response = asyncio.run(mp_routes.consumer_download_device_control("android"))
    assert response.media_type == "application/vnd.android.package-archive"
    assert response.headers["content-length"] == "6"
    assert response.headers["content-disposition"] == 'attachment; filename="device-control-0.1.0-android.apk"'
    assert asyncio.run(_drain_body(response)) == b"PK-dc!"
    assert resp.closed  # 流读完上游连接已归还
    # 旧固定路径别名仍指向 android。
    legacy = asyncio.run(mp_routes.consumer_download_device_control_android())
    assert legacy.headers["content-disposition"] == 'attachment; filename="device-control-0.1.0-android.apk"'


def test_consumer_proxy_download_device_control_ios_ipa(monkeypatch):
    """/consumer/download/device-control/ios：设备类型参数选 iOS 侧载 IPA，
    media_type 用通用字节流，文件名/兜底长度取发布记录。"""
    from user_platform.marketplace import routes as mp_routes
    import device_control_release_catalog as dcrc
    import providers.proxy_manager as pm

    url = "https://github.com/test/repo/releases/download/device-control-v0.1.0/device-control-0.1.0-ios.ipa"
    snapshot = _snapshot_with_android(url)
    snapshot["assets"][0] = {
        "filename": "device-control-0.1.0-ios.ipa", "platform": "ios", "format": "ipa",
        "download_url": url, "digest": _SHA, "size_bytes": 42,
    }

    async def fake_latest():
        return snapshot

    async def fake_refresh(publish=True):
        return snapshot

    monkeypatch.setattr(dcrc, "get_latest_release", fake_latest)
    monkeypatch.setattr(dcrc, "refresh", fake_refresh)

    resp = _FakeProxyResp(body=b"IPA!", headers={"Content-Length": "4"})
    mgr = _FakeProxyManager(resp)
    monkeypatch.setattr(pm, "get_proxy_manager", lambda: mgr)

    response = asyncio.run(mp_routes.consumer_download_device_control("ios"))
    assert response.media_type == "application/octet-stream"
    assert response.headers["content-length"] == "4"
    assert response.headers["content-disposition"] == 'attachment; filename="device-control-0.1.0-ios.ipa"'
    assert asyncio.run(_drain_body(response)) == b"IPA!"
    # 未发布该平台时不误回另一平台的包；未知平台 404。
    snapshot["assets"] = [snapshot["assets"][0].copy() | {"platform": "android", "filename": "device-control-0.1.0-android.apk"}]
    try:
        asyncio.run(mp_routes.consumer_download_device_control("ios"))
        raised, status = False, 0
    except Exception as exc:
        raised, status = True, getattr(exc, "status_code", 0)
    assert raised and status == 404
    try:
        asyncio.run(mp_routes.consumer_download_device_control("harmonyos"))
        raised, status = False, 0
    except Exception as exc:
        raised, status = True, getattr(exc, "status_code", 0)
    assert raised and status == 404


def test_consumer_proxy_download_without_release_returns_404(monkeypatch):
    """快照空且刷新仍空：404（手机端据此提示未发布，而不是拿到 HTML 错误页）。"""
    from fastapi import HTTPException

    from user_platform.marketplace import routes as mp_routes
    import mobile_release_catalog as mrc

    async def fake_latest():
        return {"version": "", "assets": []}

    async def fake_refresh(publish=True):
        return {"version": "", "assets": []}

    monkeypatch.setattr(mrc, "get_latest_release", fake_latest)
    monkeypatch.setattr(mrc, "refresh", fake_refresh)

    try:
        asyncio.run(mp_routes.consumer_download_mobile_android())
        raised, status = False, 0
    except HTTPException as exc:
        raised, status = True, exc.status_code
    assert raised and status == 404


def test_consumer_version_includes_proxy_download_url(monkeypatch):
    """/consumer/mobile-version 与 /consumer/device-control-version 都带代理下载路径；
    未发布时为空串（客户端回落 download_url / 显示未发布）。"""
    from user_platform.marketplace import routes as mp_routes
    import mobile_release_catalog as mrc
    import device_control_release_catalog as dcrc

    url = "https://github.com/test/repo/releases/download/mobile-v260608/ai-lubricant-260608-android.apk"

    async def fake_latest():
        return _snapshot_with_android(url)

    async def fake_refresh(publish=True):
        return _snapshot_with_android(url)

    async def empty_latest():
        return {"version": "", "assets": []}

    async def empty_refresh(publish=True):
        return {"version": "", "assets": []}

    monkeypatch.setattr(mrc, "get_latest_release", fake_latest)
    monkeypatch.setattr(mrc, "refresh", fake_refresh)
    payload = asyncio.run(mp_routes.consumer_mobile_version())
    # 尾段带真实 APK 文件名：忽略 Content-Disposition 的手机浏览器按 URL 尾段命名文件。
    assert payload["android"]["proxy_download_url"] == (
        "/api/v1/marketplace/consumer/download/mobile-android/ai-lubricant-260608-android.apk"
    )

    monkeypatch.setattr(mrc, "get_latest_release", empty_latest)
    monkeypatch.setattr(mrc, "refresh", empty_refresh)
    payload = asyncio.run(mp_routes.consumer_mobile_version())
    assert payload["android"]["proxy_download_url"] == ""

    monkeypatch.setattr(dcrc, "get_latest_release", fake_latest)
    monkeypatch.setattr(dcrc, "refresh", fake_refresh)
    payload = asyncio.run(mp_routes.consumer_device_control_version())
    assert payload["android"]["proxy_download_url"] == (
        "/api/v1/marketplace/consumer/download/device-control/android/ai-lubricant-260608-android.apk"
    )
    # device-control 现在带 ios 块（侧载 IPA，与 android 同字段）；未发布 iOS 时 download_url 为空。
    assert "ios" in payload
    assert payload["ios"]["proxy_download_url"] == ""


# ── 移动端 URL 模式（外链 APK 不入仓，绕开 GitHub API 大文件限制） ─────────────


def _fake_fetch(data: bytes):
    """假 _fetch_url_to_file：把固定字节写进 dest_path，返回 (size, sha256 hex)。"""
    import hashlib

    async def fetch(url, dest_path, *, proxy_config_id, **kwargs):
        import os

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as fh:
            fh.write(data)
        return len(data), hashlib.sha256(data).hexdigest()

    return fetch


def _make_url_upload_job(version: str, download_url: str, download_data: bytes):
    """URL 模式的 job：一条伪文件条目（source=url），无暂存文件。"""
    from user_platform.marketplace import upload_jobs

    job_id = upload_jobs.create_job(version, [])
    upload_jobs.set_files(job_id, [{
        "filename": f"ai-lubricant-{version}-android.apk",
        "role": "mobile-app", "platform": "android", "arch": "universal", "format": "apk",
        "size": 0, "tmp_path": "",
        "content_type": "application/vnd.android.package-archive",
        "source": "url", "download_url": download_url,
    }])
    job = upload_jobs.get_job(job_id)
    job["version_notes"] = "note"
    job["status_form"] = "published"
    job["ios_store_url"] = ""
    job["_download_data"] = download_data  # 测试提示用，transfer 不读它
    return job_id


def test_mobile_url_transfer_records_download_url_without_repo(monkeypatch, tmp_path):
    """URL 模式第②阶段：下载外链算 sha256，asset 只记 download_url（无 repo_path），
    manifest 落 store；仓库零写入（绕开 GitHub API 大文件限制）。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    import marketplace_store as store
    import mobile_release_catalog

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    apk = b"PK\x03\x04" + b"fake-apk-body"
    job_id = _make_url_upload_job("260830", "https://host/ai-lubricant-260830-android.apk", apk)
    monkeypatch.setattr(mp_routes, "_fetch_url_to_file", _fake_fetch(apk))
    client = _FakeRepoClient()
    monkeypatch.setattr(mp_routes, "_client", lambda: client)

    async def _skip_apply_release(payload, *, publish=True):
        return payload

    monkeypatch.setattr(mobile_release_catalog, "apply_release", _skip_apply_release)

    stored: dict = {}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_mobile_github_transfer(job_id))

    job = upload_jobs.get_job(job_id)
    assert job["status"] == "done", job
    # 仓库零写入：不入仓、不写 manifest/index（publisher 的活）。
    assert client.blobs == {} and client.json_files == {} and not client.writes
    manifest = stored[("mobile-versions", "mobile-260830")]["manifest"]
    asset = manifest["assets"][0]
    assert asset["download_url"] == "https://host/ai-lubricant-260830-android.apk"
    assert "repo_path" not in asset
    assert asset["digest"] == "sha256:" + __import__("hashlib").sha256(apk).hexdigest()
    assert asset["size_bytes"] == len(apk)
    # job 文件条目回填了真实大小（public_state 进度可见）。
    assert job["files"][0]["size"] == len(apk)
    upload_jobs.cleanup_job(job_id)


def test_mobile_url_transfer_download_failure_marks_file_failed(monkeypatch, tmp_path):
    """外链下载失败：文件标 failed、job 标 failed、错误进 job 状态（UI 可重试）。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    import marketplace_store as store

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    job_id = _make_url_upload_job("260830", "https://host/gone.apk", b"")

    async def _boom(url, dest_path, *, proxy_config_id, **kwargs):
        raise RuntimeError("下载失败（HTTP 404）")

    monkeypatch.setattr(mp_routes, "_fetch_url_to_file", _boom)
    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())

    stored: dict = {}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_mobile_github_transfer(job_id))

    job = upload_jobs.get_job(job_id)
    assert job["status"] == "failed"
    assert job["files"][0]["state"] == "failed"
    assert "HTTP 404" in job["files"][0]["error"]
    assert stored == {}  # manifest 未落 store
    upload_jobs.cleanup_job(job_id)


def test_mobile_url_transfer_rejects_non_apk_content(monkeypatch, tmp_path):
    """下载内容不是 ZIP/APK（缺 PK 头）：_fetch_url_to_file 报可读错误，文件 failed。"""
    from user_platform.marketplace import routes as mp_routes

    class _Resp:
        status = 200

        async def iter_any(self):
            yield b"<html>not an apk</html>"

        async def __aexit__(self, *a):
            return False

    class _Mgr:
        async def request(self, **kwargs):
            return _Resp()

    import providers.proxy_manager as pm

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Mgr())
    from user_platform.marketplace import upload_jobs

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    job_id = _make_url_upload_job("260830", "https://host/x.apk", b"")

    # 走真实的 _fetch_url_to_file 验证嗅探逻辑：monkeypatch proxy_manager 即可，
    # 不要替换 _fetch_url_to_file 本身。
    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())

    import marketplace_store as store

    stored: dict = {}
    monkeypatch.setattr(store, "get_item", _fake_store_get_item(stored))
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_mobile_github_transfer(job_id))

    job = upload_jobs.get_job(job_id)
    assert job["status"] == "failed"
    assert "不是有效的 APK" in job["files"][0]["error"]
    upload_jobs.cleanup_job(job_id)


def test_mobile_url_transfer_enforces_size_cap(monkeypatch, tmp_path):
    """下载超过 max_bytes：立即中断报错（防无限流灌盘）。"""
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import upload_jobs

    class _Resp:
        status = 200

        async def iter_any(self):
            for _ in range(10):
                yield b"PK\x03\x04" + b"x" * 1024  # 每块 1028B，10 块共 ~10KB

        async def __aexit__(self, *a):
            return False

    class _Mgr:
        async def request(self, **kwargs):
            return _Resp()

    import providers.proxy_manager as pm

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Mgr())

    # 直接调 _fetch_url_to_file，max_bytes 压到 2KB：第 2 块就超。
    import pytest

    with pytest.raises(RuntimeError, match="上限"):
        asyncio.run(mp_routes._fetch_url_to_file(
            "https://host/big.apk", str(tmp_path / "out.apk"),
            proxy_config_id=None, max_bytes=2 * 1024,
        ))


def test_mobile_url_fetch_keeps_bytes_across_chunked_magic(monkeypatch, tmp_path):
    """魔数 PK\\x03\\x04 被网络层拆进多个 chunk：仍能正确识别，且逐字节落盘不丢失。"""
    from user_platform.marketplace import routes as mp_routes

    payload = b"PK\x03\x04" + b"apk-rest" * 3

    class _Resp:
        status = 200

        async def iter_any(self):
            # 首块只有 2 字节魔数，第 2 块补齐 —— 嗅探不得吞掉任何字节。
            yield b"PK"
            yield payload[2:7]
            yield payload[7:]

        async def __aexit__(self, *a):
            return False

    class _Mgr:
        async def request(self, **kwargs):
            return _Resp()

    import providers.proxy_manager as pm

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Mgr())

    import hashlib

    dest = str(tmp_path / "out.apk")
    size, digest_hex = asyncio.run(mp_routes._fetch_url_to_file(
        "https://host/x.apk", dest, proxy_config_id=None,
    ))
    assert size == len(payload)
    assert digest_hex == hashlib.sha256(payload).hexdigest()
    assert open(dest, "rb").read() == payload


def test_mobile_url_retry_rebuilds_asset_from_tmp(monkeypatch, tmp_path):
    """上轮「下载成功、store 提交前」失败的重试：从暂存 APK 重算 digest/size，
    download_url 来自伪文件条目（URL 模式的 done-fallback 分支）。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    import marketplace_store as store

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    apk = b"PK\x03\x04" + b"retry-body"
    job_id = _make_url_upload_job("260830", "https://host/ai-lubricant-260830-android.apk", apk)
    # 模拟上轮已成功：state=done + 暂存文件就位 + 大小回填。
    upload_jobs.set_file_state(job_id, "ai-lubricant-260830-android.apk", "done")
    tmp_path_file = upload_jobs.tmp_path_for(job_id, "ai-lubricant-260830-android.apk")
    import os

    os.makedirs(os.path.dirname(tmp_path_file), exist_ok=True)
    with open(tmp_path_file, "wb") as fh:
        fh.write(apk)
    upload_jobs.get_job(job_id)["files"][0]["size"] = len(apk)
    upload_jobs.get_job(job_id)["files"][0]["tmp_path"] = tmp_path_file

    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())

    async def none_item(module, item_id):
        return None

    monkeypatch.setattr(store, "get_item", none_item)
    stored: dict = {}
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_mobile_github_transfer(job_id))

    assert upload_jobs.get_job(job_id)["status"] == "done"
    manifest = stored[("mobile-versions", "mobile-260830")]["manifest"]
    asset = manifest["assets"][0]
    assert asset["download_url"] == "https://host/ai-lubricant-260830-android.apk"
    assert "repo_path" not in asset
    assert asset["digest"] == "sha256:" + __import__("hashlib").sha256(apk).hexdigest()
    assert asset["size_bytes"] == len(apk)
    upload_jobs.cleanup_job(job_id)


def test_mobile_url_route_validations(monkeypatch, tmp_path):
    """路由层 URL 校验：HTTPS 强制、禁 token 类 query、basename 版本一致性。
    合法 URL 用假下载器短路第②阶段（这里只测路由校验，不真下载）。"""
    from fastapi import HTTPException
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import upload_jobs

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    monkeypatch.setattr(mp_routes, "_fetch_url_to_file", _fake_fetch(b"PK\x03\x04ok"))
    monkeypatch.setattr(mp_routes, "_start_mobile_github_transfer", lambda job_id: None)

    async def _call(**kwargs):
        base = {
            "version": "260830", "version_notes": "n", "status": "published",
            "ios_store_url": "", "files": None, "download_url": "",
        }
        base.update(kwargs)
        try:
            await mp_routes.upload_mobile_version(**base)
        except HTTPException as exc:
            return exc
        finally:
            for jid in list(upload_jobs._jobs):
                upload_jobs.cleanup_job(jid)
                upload_jobs._jobs.pop(jid, None)
        return None

    # 都没提供 → 400
    exc = asyncio.run(_call())
    assert exc is not None and "必须上传" in exc.detail
    # 非 HTTPS → 400
    exc = asyncio.run(_call(download_url="http://host/x.apk"))
    assert exc is not None and "HTTPS" in exc.detail
    # 带 token 类 query → 400
    exc = asyncio.run(_call(download_url="https://host/x.apk?token=abc"))
    assert exc is not None and "token" in exc.detail
    # basename 版本不一致 → 400
    exc = asyncio.run(_call(download_url="https://host/ai-lubricant-260608-android.apk"))
    assert exc is not None and "不一致" in exc.detail
    # 合法 URL + basename 匹配 → 通过（下载在后台被假下载器短路）。
    exc = asyncio.run(_call(download_url="https://host/ai-lubricant-260830-android.apk"))
    assert exc is None


def test_hard_delete_removes_repo_binaries_and_release_manifest(monkeypatch):
    """硬删除发行版本：store 行删除，payload 携带删除前 manifest；publisher 阶段
    清理仓里的 files/ 资产与 node-releases/<v>/manifest.json（Gitee 1GB 配额不能
    留残渣）。本测试直接驱动 publisher 的 _delete_item_files 验证清理行为。"""
    from user_platform.marketplace import publisher
    from user_platform.marketplace import routes as mp_routes

    client = _FakeRepoClient()
    manifest = {
        "schema": "ai-lubricant.node-version/v1", "id": "node-suite-20260830-1200",
        "kind": "node_program_version", "name": "node-suite",
        "display_name": "x", "version": "20260830-1200", "version_notes": "n",
        "status": "published", "test_version": False,
        "assets": [_node_asset(repo_path="node-releases/20260830-1200/files/node-execution-linux-amd64")],
    }
    item_path = "modules/node-versions/items/node-suite-20260830-1200.json"
    client.json_files[item_path] = manifest
    client.json_files["node-releases/20260830-1200/manifest.json"] = copy.deepcopy(manifest)
    client.blobs["node-releases/20260830-1200/files/node-execution-linux-amd64"] = b"ELF"
    monkeypatch.setattr(publisher, "_client", lambda: client)

    asyncio.run(publisher._delete_item_files(client, "node-versions", "node-suite-20260830-1200", manifest))

    assert "node-releases/20260830-1200/files/node-execution-linux-amd64" in client.deletes
    assert "node-releases/20260830-1200/manifest.json" in client.deletes
    assert item_path in client.deletes
    assert client.blobs == {}


def test_node_upload_retry_rebuilds_prior_asset_from_tmp(monkeypatch, tmp_path):
    """上轮在「Release 资产传完、store 提交前」失败的重试：store/仓库 manifest 都读不到
    done 文件的资产元数据，必须从独立仓库 Release 的现有资产取回直链（临时文件重算
    digest/size 校验一致），否则已传文件会从新 manifest 里丢失。"""
    from user_platform.marketplace import upload_jobs
    from user_platform.marketplace import routes as mp_routes
    from user_platform.marketplace import github as mp_github
    from user_platform.marketplace import release_repos
    import marketplace_store as store

    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: str(tmp_path / job_id))
    files = [{"filename": "node-execution-linux-amd64", "role": "execution", "platform": "linux", "arch": "amd64", "data": b"ELF-exec"}]
    job_id = _make_upload_job("20260830-1200", files)
    upload_jobs.set_file_state(job_id, "node-execution-linux-amd64", "done")  # 模拟上轮已成功

    # 独立仓库 Release 已存在该资产（上轮传完），tag 查询返回它。
    async def fake_get_release_by_tag(owner, repo, token, tag):
        return {
            "upload_url": "https://uploads.github.com/repos/test/repo/releases/1/assets{?name,label}",
            "assets": [{
                "name": "node-execution-linux-amd64",
                "browser_download_url": "https://github.com/test/repo/releases/download/node-v20260830-1200/node-execution-linux-amd64",
                "size": len(b"ELF-exec"),
            }],
        }

    async def fake_create_release(*a, **k):
        raise AssertionError("release 已存在，不应再创建")

    monkeypatch.setattr(mp_github, "get_release_by_tag", fake_get_release_by_tag)
    monkeypatch.setattr(mp_github, "create_release", fake_create_release)
    # store 没有时回落读 marketplace 仓库的旧 release manifest——这里也没有，给假
    # 客户端兜成 None（重试必须走 Release 现有资产取回直链，不能触发真实网络）。
    monkeypatch.setattr(mp_routes, "_client", lambda: _FakeRepoClient())
    monkeypatch.setattr(release_repos, "load_node_release_repo", lambda: release_repos.ReleaseRepoSettings(
        repo_url="https://github.com/wuxin-gh/ai-lubricant-nodes",
        github_owner="wuxin-gh", github_repo="ai-lubricant-nodes", github_token="t",
    ))

    async def none_item(module, item_id):
        return None

    monkeypatch.setattr(store, "get_item", none_item)
    stored: dict = {}
    monkeypatch.setattr(store, "upsert_item", _fake_store_upsert_item(stored))

    asyncio.run(mp_routes._run_github_transfer(job_id))

    assert upload_jobs.get_job(job_id)["status"] == "done"
    manifest = stored[("node-versions", "node-suite-20260830-1200")]["manifest"]
    asset = manifest["assets"][0]
    assert asset["download_url"] == "https://github.com/test/repo/releases/download/node-v20260830-1200/node-execution-linux-amd64"
    assert asset["digest"] == "sha256:" + __import__("hashlib").sha256(b"ELF-exec").hexdigest()
    assert asset["size_bytes"] == len(b"ELF-exec")
    upload_jobs.cleanup_job(job_id)


def _fake_store_get_item(stored: dict):
    async def get_item(module, item_id):
        row = stored.get((module, item_id))
        return copy.deepcopy(row) if row else None

    return get_item


def _fake_store_upsert_item(stored: dict):
    async def upsert_item(module, manifest):
        stored[(module, str(manifest.get("id")))] = {"manifest": copy.deepcopy(manifest)}
        return {"module": module, "item_id": str(manifest.get("id"))}

    return upsert_item


async def _async_none():
    return None


async def _async_dict():
    return {}


# ── MarketplaceGitHub 二进制写/删（sha 只读元数据，不走 JSON 解析） ────────────


class _MetaResponse:
    """Contents API GET 的元数据形态（type=file + sha）。二进制不给内联 content。"""

    def __init__(self, status: int, payload: dict | None = None):
        self.status = status
        self._payload = payload

    async def json(self):
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload

    async def text(self):
        import json as _json

        return "" if self._payload is None else _json.dumps(self._payload)


class _MetaProxyManager:
    """按调用序列回放响应的假 proxy_manager；同时收集 PUT/DELETE body。"""

    def __init__(self, responses: list):
        self.responses = list(responses)
        self.requests: list[tuple[str, str, dict | None]] = []

    async def request(self, *, url, method, headers=None, timeout=None, proxy_config_id=None, **kwargs):
        path = url.split("/contents/", 1)[1].split("?ref=", 1)[0]
        # _request 把 json_body 归一成 json 传给底层 session；两种名字都收。
        body = kwargs.get("json_body")
        if body is None and "json" in kwargs:
            body = kwargs["json"]
        self.requests.append((method, path, body))
        resp = self.responses.pop(0) if self.responses else _MetaResponse(404)
        return resp


def _producer_settings() -> MarketplaceSettings:
    return MarketplaceSettings(
        repo_url="https://github.com/o/r",
        github_owner="o",
        github_repo="r",
        github_branch="main",
        github_token="tok",
        modules=("node-versions",),
        index_name="index.json",
        proxy_id="",
    )


def test_write_bytes_puts_base64_blob_with_sha(monkeypatch):
    """新文件写入：先 GET 元数据（404 → 无 sha），再 PUT base64 内容。二进制
    绝不能走 read_json 的 JSON 解析路径。"""
    from user_platform.marketplace import github as gh

    pm = _MetaProxyManager([
        _MetaResponse(404),  # _read_sha_or_none: 文件不存在
        _MetaResponse(200, {"content": {"sha": "abc"}}),  # PUT 结果
    ])
    monkeypatch.setattr(
        "providers.proxy_manager.get_proxy_manager", lambda: _PMLazy(pm)
    )
    client = gh.MarketplaceGitHub(_producer_settings())
    data = bytes(range(256)) * 4
    asyncio.run(client.write_bytes("node-releases/v1/files/bin", data, "msg"))

    get_call = pm.requests[0]
    put_method, put_path, put_body = pm.requests[1]
    assert get_call[0] == "GET" and get_call[1] == "node-releases/v1/files/bin"
    assert put_method == "PUT" and put_path == "node-releases/v1/files/bin"
    assert put_body["content"] == base64.b64encode(data).decode("ascii")
    assert "sha" not in put_body  # 新文件不需要 sha


def test_write_bytes_overwrites_with_current_sha(monkeypatch):
    """已存在文件：GET 元数据拿真实 sha，PUT 带上 sha 覆盖。"""
    from user_platform.marketplace import github as gh

    pm = _MetaProxyManager([
        _MetaResponse(200, {"type": "file", "sha": "existing-sha"}),
        _MetaResponse(200, {"content": {"sha": "new"}}),
    ])
    monkeypatch.setattr(
        "providers.proxy_manager.get_proxy_manager", lambda: _PMLazy(pm)
    )
    client = gh.MarketplaceGitHub(_producer_settings())
    asyncio.run(client.write_bytes("node-releases/v1/files/bin", b"\x00\x01", "msg"))
    put_body = pm.requests[1][2]
    assert put_body["sha"] == "existing-sha"


def test_delete_file_deletes_binary_asset_by_sha(monkeypatch):
    """删除二进制资产：只读元数据取 sha（不解析内容），DELETE 带 sha。"""
    from user_platform.marketplace import github as gh

    pm = _MetaProxyManager([
        _MetaResponse(200, {"type": "file", "sha": "blob-sha"}),
        _MetaResponse(204),
    ])
    monkeypatch.setattr(
        "providers.proxy_manager.get_proxy_manager", lambda: _PMLazy(pm)
    )
    client = gh.MarketplaceGitHub(_producer_settings())
    assert asyncio.run(client.delete_file("node-releases/v1/files/bin", "msg")) is True
    delete_method, delete_path, delete_body = pm.requests[1]
    assert delete_method == "DELETE" and delete_path == "node-releases/v1/files/bin"
    assert delete_body["sha"] == "blob-sha"


class _PMLazy:
    """get_proxy_manager() 的懒代理：proxy_manager 是按调用导入的。"""

    def __init__(self, pm):
        self._pm = pm

    def request(self, **kwargs):
        return self._pm.request(**kwargs)


# ── Release asset 重传幂等（upload_release_asset 422 先删后传）───────────────


class _Resp:
    def __init__(self, status: int, payload: dict | None = None):
        self.status = status
        self._payload = payload

    async def json(self):
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload

    async def text(self):
        return "" if self._payload is None else str(self._payload)


class _AsyncCM:
    """async with _http_session() as session 的薄包装，直接回放假 session。"""
    def __init__(self, session): self._session = session
    async def __aenter__(self): return self._session
    async def __aexit__(self, *exc): return False


class _RelSession:
    """回放式假 ClientSession：按调用顺序吐响应，记录请求。"""

    def __init__(self, responses: list[_Resp]):
        self.responses = list(responses)
        self.requests: list[tuple[str, str]] = []

    class _Ctx:
        def __init__(self, resp): self.resp = resp
        async def __aenter__(self): return self.resp
        async def __aexit__(self, *exc): return False

    def post(self, url, **kwargs): 
        self.requests.append(("POST", url))
        return self._Ctx(self.responses.pop(0) if self.responses else _Resp(500))

    def get(self, url, **kwargs):
        self.requests.append(("GET", url))
        return self._Ctx(self.responses.pop(0) if self.responses else _Resp(404))

    def delete(self, url, **kwargs):
        self.requests.append(("DELETE", url))
        return self._Ctx(self.responses.pop(0) if self.responses else _Resp(404))

    async def __aenter__(self): return self
    async def __aexit__(self, *exc): return False


def test_upload_release_asset_replaces_same_name_asset_on_422(monkeypatch):
    """重传幂等：同名 asset 已在 Release 上时 GitHub 回 422——必须先查 Release
    拿 asset id、DELETE 旧 asset、再重传一次成功。这是「过期后用已提交数据整包
    重传」链路的服务端前提（新 job 所有文件都是 pending，不走 done 跳过路径）。"""
    from user_platform.marketplace import github as gh

    session = _RelSession([
        _Resp(422, {"message": "already_exists"}),      # 第一次 POST 上传：撞同名
        _Resp(200, {"assets": [{"id": 77, "name": "app.apk"}]}),  # GET release by id
        _Resp(204),                                      # DELETE 旧 asset
        _Resp(201, {"browser_download_url": "https://x/app.apk", "size": 3}),  # 重传成功
    ])
    # github.py 的 Releases 函数用 ..git_clients._http_session（async with session.post(...)）
    import user_platform.git_clients as gc
    monkeypatch.setattr(gc, "_http_session", lambda: _AsyncCM(session))

    result = asyncio.run(gh.upload_release_asset(
        "https://uploads.github.com/repos/o/r/releases/9/assets{?name,label}",
        "tok", "app.apk", b"PK\x03",
    ))
    assert result["browser_download_url"].endswith("app.apk")
    methods = [m for m, _ in session.requests]
    assert methods == ["POST", "GET", "DELETE", "POST"]
    # DELETE 打的是 api.github.com 的 asset 端点，不是 uploads.github.com
    delete_url = session.requests[2][1]
    assert delete_url == "https://api.github.com/repos/o/r/releases/assets/77"


def test_upload_release_asset_raises_when_retry_still_fails(monkeypatch):
    """删了旧 asset 重传仍失败：按原路径抛 GitClientError，不吞错。"""
    from user_platform.git_clients import GitClientError
    from user_platform.marketplace import github as gh

    session = _RelSession([
        _Resp(422, {"message": "already_exists"}),
        _Resp(200, {"assets": [{"id": 77, "name": "app.apk"}]}),
        _Resp(204),
        _Resp(422, {"message": "still_bad"}),
    ])
    import user_platform.git_clients as gc
    monkeypatch.setattr(gc, "_http_session", lambda: _AsyncCM(session))

    import pytest
    with pytest.raises(GitClientError):
        asyncio.run(gh.upload_release_asset(
            "https://uploads.github.com/repos/o/r/releases/9/assets{?name,label}",
            "tok", "app.apk", b"PK\x03",
        ))


def test_upload_job_sweep_keeps_live_task(monkeypatch):
    """sweep 不杀仍活着的转传任务（2GB/URL 模式超 30 分钟是合法长任务），
    只回收终态或任务已死的过期 job。"""
    import time as _time
    from user_platform.marketplace import upload_jobs

    upload_jobs._jobs.clear()
    upload_jobs._tasks.clear()
    upload_jobs._last_sweep_at = 0.0
    monkeypatch.setattr(upload_jobs, "tmp_dir", lambda job_id: f"/tmp/nonexistent-{job_id}")

    old = _time.time() - upload_jobs._TTL_SECONDS - 10
    live_id = upload_jobs.create_job("v1", [])
    dead_id = upload_jobs.create_job("v2", [])
    upload_jobs._jobs[live_id]["created_at"] = old
    upload_jobs._jobs[dead_id]["created_at"] = old

    class _FakeTask:
        def done(self): return False

    upload_jobs._tasks[live_id] = _FakeTask()  # type: ignore[assignment]

    upload_jobs.sweep_expired()
    assert upload_jobs.get_job(live_id) is not None, "仍活着的任务不能被清"
    assert upload_jobs.get_job(dead_id) is None, "无任务且过期的 job 应被清"

    # TTL 内的 job 不动
    fresh_id = upload_jobs.create_job("v3", [])
    upload_jobs.sweep_expired()
    assert upload_jobs.get_job(fresh_id) is not None
    upload_jobs._jobs.clear()
    upload_jobs._tasks.clear()
