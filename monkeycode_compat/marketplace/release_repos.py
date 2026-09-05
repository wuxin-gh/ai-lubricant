"""程序发行独立仓库配置（nodes, mobile, device-control）。

三类程序的二进制资产存放在各自的独立仓库 GitHub Releases 中，
与 marketplace 仓库（插件资源）分离。本模块负责解析这些独立仓库的配置。
"""
from __future__ import annotations

from dataclasses import dataclass

from ..config import _resolve
from .config import parse_repo_url


@dataclass(frozen=True)
class ReleaseRepoSettings:
    """程序发行仓库配置（独立于 marketplace 仓库）。

    用于节点程序、移动端 App、设备控制 App 的二进制资产存储。
    """

    repo_url: str
    github_owner: str
    github_repo: str
    github_token: str

    @property
    def enabled(self) -> bool:
        """是否可写入（需要 owner/repo/token 全部配置）。"""
        return bool(self.github_owner and self.github_repo and self.github_token)


def load_node_release_repo() -> ReleaseRepoSettings:
    """加载节点程序发行仓库配置。

    优先级：NODE_RELEASE_REPO_URL > 默认 ai-lubricant-nodes
    token 优先级：NODE_RELEASE_GITHUB_TOKEN > MARKETPLACE_GITHUB_TOKEN
    """
    repo_url = _resolve("NODE_RELEASE_REPO_URL", "https://github.com/wuxin-gh/ai-lubricant-nodes")
    owner, repo, _ = parse_repo_url(repo_url)
    token = _resolve("NODE_RELEASE_GITHUB_TOKEN", "") or _resolve("MARKETPLACE_GITHUB_TOKEN", "")
    return ReleaseRepoSettings(
        repo_url=repo_url,
        github_owner=owner,
        github_repo=repo,
        github_token=token,
    )


def load_mobile_release_repo() -> ReleaseRepoSettings:
    """加载移动端 App 发行仓库配置。

    优先级：MOBILE_RELEASE_REPO_URL > 默认 ai-lubricant-mobile
    token 优先级：MOBILE_RELEASE_GITHUB_TOKEN > MARKETPLACE_GITHUB_TOKEN
    """
    repo_url = _resolve("MOBILE_RELEASE_REPO_URL", "https://github.com/wuxin-gh/ai-lubricant-mobile")
    owner, repo, _ = parse_repo_url(repo_url)
    token = _resolve("MOBILE_RELEASE_GITHUB_TOKEN", "") or _resolve("MARKETPLACE_GITHUB_TOKEN", "")
    return ReleaseRepoSettings(
        repo_url=repo_url,
        github_owner=owner,
        github_repo=repo,
        github_token=token,
    )


def load_device_control_release_repo() -> ReleaseRepoSettings:
    """加载设备控制 App 发行仓库配置。

    优先级：DEVICE_CONTROL_RELEASE_REPO_URL > 默认 ai-lubricant-device-control
    token 优先级：DEVICE_CONTROL_RELEASE_GITHUB_TOKEN > MARKETPLACE_GITHUB_TOKEN
    """
    repo_url = _resolve(
        "DEVICE_CONTROL_RELEASE_REPO_URL",
        "https://github.com/wuxin-gh/ai-lubricant-device-control"
    )
    owner, repo, _ = parse_repo_url(repo_url)
    token = _resolve("DEVICE_CONTROL_RELEASE_GITHUB_TOKEN", "") or _resolve("MARKETPLACE_GITHUB_TOKEN", "")
    return ReleaseRepoSettings(
        repo_url=repo_url,
        github_owner=owner,
        github_repo=repo,
        github_token=token,
    )
