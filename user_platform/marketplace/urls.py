"""市场 raw URL 的平台渲染（github / gitee）。

生产侧（写）永远是 GitHub；Gitee 只作为只读镜像，靠 git fetch 同步把仓库内容
原生带走。消费侧的 raw 读取与资产下载则按部署配置的平台渲染绝对 URL：

- github → ``https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}``
- gitee → ``https://gitee.com/{owner}/{repo}/raw/{branch}/{path}``

资产 manifest 里存的 ``repo_path`` 是仓库相对路径（存储真相）；这里负责把它
按消费侧平台渲染成 ``download_url``。旧资产只有绝对 ``download_url``（GitHub
Release 直链）没有 repo_path，渲染时原样回退，双口径并存兼容存量。
"""
from __future__ import annotations

from urllib.parse import quote

PLATFORMS = ("github", "gitee")


def raw_url(platform: str, owner: str, repo: str, branch: str, path: str) -> str:
    """按平台渲染 raw 直链。未知平台按 github 处理。"""
    branch = branch or "main"
    quoted_path = quote(path.lstrip("/"), safe="/")
    if platform == "gitee":
        return (
            f"https://gitee.com/{quote(owner, safe='')}/{quote(repo, safe='')}"
            f"/raw/{quote(branch, safe='')}/{quoted_path}"
        )
    return (
        f"https://raw.githubusercontent.com/{quote(owner, safe='')}/"
        f"{quote(repo, safe='')}/{quote(branch, safe='')}/{quoted_path}"
    )


def repo_web_url(platform: str, owner: str, repo: str) -> str:
    """仓库网页地址（前端展示/跳转用）。未知平台按 github 处理。"""
    if platform == "gitee":
        return f"https://gitee.com/{quote(owner, safe='')}/{quote(repo, safe='')}"
    return f"https://github.com/{quote(owner, safe='')}/{quote(repo, safe='')}"


def consumer_raw_url(path: str) -> str:
    """消费侧 raw 直链：owner/repo/branch/platform 全部来自 ``consumer_settings``。"""
    from . import config as mp_config

    c = mp_config.consumer_settings
    return raw_url(c.platform, c.github_owner, c.github_repo, c.github_branch, path)


def render_asset_download_url(asset: dict, settings=None) -> str:
    """把 asset 的 ``repo_path`` 按消费侧平台渲染成绝对下载地址。

    没有仓库相对路径的旧资产（GitHub Release 直链时代）原样回退
    ``download_url``。settings 可显式传入（测试用），默认读 consumer_settings。
    """
    repo_path = str((asset or {}).get("repo_path") or "").strip()
    if not repo_path:
        return str((asset or {}).get("download_url") or "")
    if settings is not None:
        return raw_url(
            settings.platform, settings.github_owner, settings.github_repo,
            settings.github_branch, repo_path,
        )
    return consumer_raw_url(repo_path)


def normalize_release_assets(data: dict) -> dict:
    """就地（深改前需先拷贝）把 release payload 里每条 asset 渲染 download_url。

    在 catalog 摄取点调用：拉到/收到 version.json 后、validate 之前执行，让
    校验看到的 asset 永远带合法 HTTPS download_url，落库快照里的 download_url
    也永远是渲染好的绝对 URL —— 下游 select/coverage/安装脚本/升级帧零改动。
    无 repo_path 的旧资产保留原 download_url（可能为空，交给校验器报错）。
    """
    for asset in (data.get("assets") or []):
        if isinstance(asset, dict) and str(asset.get("repo_path") or "").strip():
            asset["download_url"] = render_asset_download_url(asset)
    return data
