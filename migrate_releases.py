#!/usr/bin/env python
"""一次性迁移：把 marketplace store 里各类程序的「最新版本」搬到独立仓库 GitHub Releases。

背景（新架构）：节点程序 / 移动端控制 App / 设备控制被控端 App 的二进制改存到
各自独立仓库的 GitHub Release（tag: node-v{v} / mobile-v{v} / device-control-v{v}），
marketplace 仓库只留元数据（manifest/version.json 由 publisher 投影）。

迁移策略（与用户确认一致）：**只迁最新版本**，旧版本保持旧链接（marketplace 仓库
repo_path 直链）向后兼容。对每个模块：

1. store.list_manifests(module) 里挑最新 published 版本；
2. 只迁还带 repo_path（旧架构入仓）的资产——已有 download_url 的（新架构产物 /
   URL 模式外链）跳过；
3. 从 marketplace 仓库 raw 下载每个 repo_path 资产，算 sha256；
4. 在独立仓库创建 Release（已存在则复用）并上传资产（已存在同名资产跳过）；
5. manifest 资产改成 download_url 直链后 upsert 回 store；
6. publisher 下次推送时自动把新 manifest 渲染进 marketplace 仓库。

用法（项目根目录）：
    python migrate_releases.py            # 全部三个模块（dry-run 预览）
    python migrate_releases.py --apply    # 实际执行
    python migrate_releases.py --only node-versions --apply
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_env() -> None:
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and value:
            os.environ.setdefault(key, value)


def _module_latest(manifests: list[dict]) -> dict | None:
    """挑最新 published（非测试）版本；没有则退而挑最新任意状态。"""
    published = [m for m in manifests if m.get("status") == "published" and not m.get("test_version")]
    pool = published or list(manifests)
    if not pool:
        return None
    # 版本号不可字典序直接比（20260830-1200 可以，semver 0.9 vs 0.10 不行）——按元组
    # 拆数字段比较，兜底字符串比较。
    def sort_key(m: dict):
        v = str(m.get("version") or "")
        parts = []
        for seg in v.replace("-", ".").split("."):
            parts.append((0, int(seg)) if seg.isdigit() else (1, seg))
        return (parts, v)

    return max(pool, key=sort_key)


async def migrate_module(module: str, *, tag_prefix: str, release_name_tpl: str, apply: bool) -> bool:
    import marketplace_store as store
    from monkeycode_compat.marketplace import config as mp_config
    from monkeycode_compat.marketplace import github as mp_github
    from monkeycode_compat.marketplace import release_repos

    loader = {
        "node-versions": release_repos.load_node_release_repo,
        "mobile-versions": release_repos.load_mobile_release_repo,
        "device-control-versions": release_repos.load_device_control_release_repo,
    }[module]
    repo = loader()
    if not repo.enabled:
        print(f"  × 跳过 {module}：独立仓库未配置 token（{repo.repo_url}）")
        return False

    manifests = await store.list_manifests(module, include_hidden=False)
    if not manifests:
        print(f"  - 跳过 {module}：store 无 published 版本")
        return False
    latest = _module_latest(manifests)
    version = str(latest.get("version") or "")
    assets = [a for a in (latest.get("assets") or []) if isinstance(a, dict)]
    to_migrate = [a for a in assets if a.get("repo_path") and not a.get("download_url")]
    if not to_migrate:
        print(f"  - 跳过 {module} v{version}：资产已是 download_url 直链（无需迁移）")
        return False

    tag = f"{tag_prefix}-v{version}"
    print(f"  → {module} v{version}：{len(to_migrate)} 个资产迁到 {repo.github_owner}/{repo.github_repo} Release {tag}")
    for a in to_migrate:
        print(f"      {a.get('filename')}  ← repo_path {a.get('repo_path')}")

    if not apply:
        return True

    # 消费坐标渲染 raw 直链（与消费端同一套渲染，含 github/gitee 平台差异）。
    consumer = mp_config.load_consumer_settings()
    if consumer.platform == "gitee":
        print("  × 中止：消费侧平台是 gitee 镜像，repo_path raw 渲染请按 urls 模块补齐后重试")
        return False
    raw_base = f"https://raw.githubusercontent.com/{consumer.github_owner}/{consumer.github_repo}/{consumer.github_branch}"

    release_info = await mp_github.get_release_by_tag(
        repo.github_owner, repo.github_repo, repo.github_token, tag,
    )
    if not release_info:
        release_info = await mp_github.create_release(
            owner=repo.github_owner,
            repo=repo.github_repo,
            token=repo.github_token,
            tag=tag,
            name=release_name_tpl.format(version=version),
            body=str(latest.get("version_notes") or ""),
            prerelease=bool(latest.get("test_version")),
        )
        print(f"      已创建 Release {tag}")
    else:
        print(f"      复用已有 Release {tag}")
    existing_names = {str(a.get("name") or "") for a in (release_info.get("assets") or [])}

    new_assets: list[dict] = []
    changed = False
    for a in to_migrate:
        filename = str(a.get("filename"))
        if filename in existing_names:
            print(f"      = 资产已存在，跳过上传: {filename}")
            existing = next(x for x in (release_info.get("assets") or []) if str(x.get("name")) == filename)
            new_assets.append({**a, "download_url": str(existing.get("browser_download_url") or "")})
            changed = True
            continue
        url = f"{raw_base}/{a['repo_path']}"
        print(f"      下载 {url} …")
        content, digest = await _download(url)
        if not content:
            print(f"      × 下载失败（空内容），资产保持原样: {filename}")
            new_assets.append(a)
            continue
        await mp_github.upload_release_asset(
            upload_url=release_info["upload_url"],
            token=repo.github_token,
            filename=filename,
            content=content,
            content_type=str(a.get("format") == "apk" and "application/vnd.android.package-archive" or "application/octet-stream"),
        )
        new_assets.append({
            **a,
            "download_url": f"https://github.com/{repo.github_owner}/{repo.github_repo}/releases/download/{tag}/{filename}",
            "digest": digest,
            "size_bytes": len(content),
        })
        changed = True
        print(f"      ✓ 已上传 {filename}（{len(content)} 字节）")

    if not changed:
        print("      无变化，不更新 store")
        return True

    # 合并回 manifest：迁过的资产换 download_url，未迁的保持原样。
    by_key = {a.get("repo_path") or a.get("filename"): a for a in assets}
    for a in new_assets:
        by_key[a.get("repo_path") or a.get("filename")] = {k: v for k, v in a.items() if k != "repo_path"}
    manifest = {**latest, "assets": list(by_key.values()), "release_tag": tag}
    await store.upsert_item(module, manifest)
    print(f"      ✓ store 已更新（download_url 直链，repo_path 已剥）；publisher 将自动投影 marketplace 仓库")
    return True


async def _download(url: str) -> tuple[bytes, str]:
    """流式下载 raw 资产，返回 (内容, sha256:hex)。经 proxy_manager 统一出网。"""
    from monkeycode_compat.marketplace.github import _HTTP_TIMEOUT  # noqa: F401  保持超时口径一致

    from providers.proxy_manager import get_proxy_manager

    buf = bytearray()
    resp = await get_proxy_manager().request(url=url, method="GET", headers={}, timeout=None)
    if resp.status != 200:
        raise RuntimeError(f"GET {url} returned HTTP {resp.status}")
    async for chunk in resp.iter_any():
        buf.extend(chunk)
    content = bytes(buf)
    return content, "sha256:" + hashlib.sha256(content).hexdigest()


async def main() -> None:
    load_env()
    sys.path.insert(0, str(ROOT / "server"))
    sys.path.insert(0, str(ROOT))

    apply = "--apply" in sys.argv
    only: list[str] = []
    for i, arg in enumerate(sys.argv):
        if arg == "--only" and i + 1 < len(sys.argv):
            only = sys.argv[i + 1].split(",")

    from db import PostgresClient

    await PostgresClient.init(lightweight=True)  # 只需要连接池 + 市场表

    modules = [
        ("node-versions", "node", "节点程序套件 v{version}"),
        ("mobile-versions", "mobile", "移动控制端 v{version}"),
        ("device-control-versions", "device-control", "设备控制 App v{version}"),
    ]
    if only:
        modules = [m for m in modules if m[0] in only]

    print(f"{'实际执行' if apply else 'DRY-RUN 预览（--apply 才执行）'}：迁移最新版本到独立仓库 GitHub Releases")
    for module, tag_prefix, name_tpl in modules:
        try:
            await migrate_module(module, tag_prefix=tag_prefix, release_name_tpl=name_tpl, apply=apply)
        except Exception as exc:  # noqa: BLE001
            print(f"  × {module} 迁移失败: {exc}")
    await PostgresClient.close()


if __name__ == "__main__":
    asyncio.run(main())
