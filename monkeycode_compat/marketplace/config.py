"""市场配置（仓库地址与写入 token 仅 .env；其余项 DB 优先 / 环境变量兜底 / 内置默认）。

市场仓库地址同时决定市场展示、渠道目录同步、节点程序版本下载 —— 这三件事共用
同一个 GitHub 仓库，所以只有这一份配置。优先级：

1. **仓库地址 ``repo_url`` 与 ``github_token``：只认服务端 ``.env``**
   （``MARKETPLACE_REPO_URL`` / ``MARKETPLACE_GITHUB_TOKEN``），管理端不提供编辑，
   改完需重启；历史 DB 值一律忽略。
2. 其余项（分支 / modules / index_name / 代理）：DB 主配置 ``marketplace`` key
   > 环境变量 > 内置默认。
3. 内置默认仓库 ``https://github.com/wuxin-gh/ai-lubricant-marketplace``

因此全新部署不配任何东西也能看市场；只有要代管写入才需要额外配 token。
GitHub 写入 token 只保存在服务端，前端与消费侧从不接触。

两级开关，对应「看市场」与「管市场」两件事：

- ``enabled``  —— 只要能解析出 owner/repo（含内置默认）。读公开仓库 raw 不需要凭据。
- ``writable`` —— 额外要求 github_token。只有代管写入 GitHub 才需要，因此只有
  市场管理员需要配 token；未配时市场照常可看，只是管理页不出现。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

from ..config import _resolve, _resolve_first
from .source_config import DEFAULT_REPO_URL, get_source_config

_DEFAULT_MODULES = ("mcp", "plugins", "skills", "channels", "prompts", "node-versions", "mobile-versions", "device-control-versions")

# 仓库名允许的字符（GitHub 规则），顺带剥掉结尾的 .git
_REPO_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def parse_repo_url(raw: str) -> tuple[str, str, str]:
    """从仓库地址解析 ``(owner, repo, branch)``；解析不出返回三个空串。

    尽量宽容，让人怎么复制都能用：

    - ``https://github.com/owner/repo``（最常见，浏览器地址栏直接复制）
    - 结尾带 ``/``、``.git``、``/tree/main`` 或更深的子路径
    - ``git@github.com:owner/repo.git``（SSH 形态）
    - ``github.com/owner/repo`` / ``owner/repo``（省略协议或裸写）

    ``/tree/<branch>`` 会被识别成 branch 一并返回，这样贴非默认分支的页面地址也对。
    """
    text = (raw or "").strip()
    if not text:
        return "", "", ""

    # SSH 形态先归一成 path，避免 urlparse 把 host 当 scheme
    if text.startswith("git@"):
        text = text.split(":", 1)[-1] if ":" in text else text
        path = text
    else:
        if "://" in text:
            path = urlparse(text).path
        else:
            # github.com/owner/repo、gitee.com/owner/repo 或 owner/repo：没协议时
            # urlparse 会把整串当 path，但 host 段会混进来，这里手动剥掉已知 host 前缀。
            path = text
            for host in ("github.com/", "www.github.com/", "gitee.com/", "www.gitee.com/"):
                if path.lower().startswith(host):
                    path = path[len(host):]
                    break

    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        return "", "", ""

    owner, repo = parts[0], parts[1]
    if repo.lower().endswith(".git"):
        repo = repo[:-4]
    if not _REPO_NAME_RE.match(owner) or not _REPO_NAME_RE.match(repo):
        return "", "", ""

    # .../tree/<branch>：贴分支页地址时顺带认出分支
    branch = ""
    if len(parts) >= 4 and parts[2] in ("tree", "blob"):
        branch = parts[3]
    return owner, repo, branch


def detect_platform(repo_url: str) -> str:
    """从仓库地址识别平台：host 含 gitee → gitee，其余（含空）→ github。

    消费侧读 Gitee 镜像时只需把仓库地址换成 Gitee URL，平台随之切换——
    不引入独立于地址的第二个平台开关。
    """
    text = (repo_url or "").strip().lower()
    if not text:
        return "github"
    # 只看 host 段，避免路径里恰好出现 "gitee" 字样误判。
    if "://" in text:
        host = urlparse(text).hostname or ""
    elif "@" in text.split("/", 1)[0]:  # git@gitee.com:owner/repo.git
        host = text.split("@", 1)[1].split("/", 1)[0].split(":", 1)[0]
    else:
        host = text.split("/", 1)[0]
    return "gitee" if "gitee" in host else "github"


@dataclass(frozen=True)
class MarketplaceSettings:
    """已解析的市场配置。

    ``repo_url`` 是用户填的原文（回显用），``github_owner``/``github_repo``/
    ``github_branch`` 是从它解析出来的结果，内部一律用后者。
    """

    repo_url: str
    github_owner: str
    github_repo: str
    github_branch: str
    github_token: str
    modules: tuple[str, ...]
    index_name: str
    # 资源中心配置的代理池条目 id；空=直连。读写两侧共用这一条（proxy_manager 隐式直连）。
    proxy_id: str

    @property
    def enabled(self) -> bool:
        """市场是否可用（消费侧）。

        只要仓库地址能解析出 owner/repo 就为真：读公开仓库的 raw 不需要任何凭据，
        前端拿到 owner/repo/branch 就能直读市场。token 只跟写入有关，见 ``writable``。
        """
        return bool(self.github_owner and self.github_repo)

    @property
    def writable(self) -> bool:
        """市场管理页是否可用（写入侧）。

        额外要求 github_token：只有代管写入 GitHub 才需要它，因此只有市场管理员
        需要配这一项。未配则市场照常可看，只是管理页不出现、管理接口不可写。
        """
        return self.enabled and bool(self.github_token)


def load_settings() -> MarketplaceSettings:
    """DB 全局配置优先；环境变量与内置默认兜底。"""
    modules_raw = _resolve("MARKETPLACE_MODULES", "")
    modules = tuple(m.strip() for m in modules_raw.split(",") if m.strip()) or _DEFAULT_MODULES

    # DB 全局配置（已归一；未配字符串项为空串，下面回落到 env/ini/默认）。
    source = get_source_config()

    # 主配置项：整条仓库地址。repo_url 只认 .env（管理端不再提供编辑，历史 DB 值
    # 一律忽略），旧变量名 MARKETPLACE_GITHUB_REPO_URL 也认，容忍手写差异。
    repo_url = _resolve_first(
        ("MARKETPLACE_REPO_URL", "MARKETPLACE_GITHUB_REPO_URL"),
        DEFAULT_REPO_URL,
    )
    owner, repo, url_branch = parse_repo_url(repo_url)

    # 兼容旧的拆字段写法：填了 repo_url 就以它为准，否则回落到 owner/repo。
    if not (owner and repo):
        owner = _resolve("MARKETPLACE_GITHUB_OWNER", "")
        repo = _resolve("MARKETPLACE_GITHUB_REPO", "")

    # 分支优先级：DB 配置 > env/ini > 地址里的 /tree/<branch> > main
    branch = (
        source.get("github_branch")
        or _resolve("MARKETPLACE_GITHUB_BRANCH", "")
        or url_branch
        or "main"
    )

    # token 优先级：只认 .env（管理端不再提供编辑，历史 DB 值一律忽略）。
    # 空串表示未配（不可写，但仍可看）；改 token 需重启生效。
    token = _resolve("MARKETPLACE_GITHUB_TOKEN", "")

    # modules / index_name 同样允许 DB 覆盖。
    if source.get("modules"):
        modules = tuple(m.strip() for m in str(source["modules"]).split(",") if m.strip()) or _DEFAULT_MODULES
    index_name = source.get("index_name") or _resolve("MARKETPLACE_INDEX_NAME", "index.json")

    # 资源中心配置的代理池 id（DB > env > 空）。读写两侧共用，空=直连。
    proxy_id = source.get("proxy_id") or _resolve("MARKETPLACE_PROXY_ID", "")

    return MarketplaceSettings(
        repo_url=repo_url,
        github_owner=owner,
        github_repo=repo,
        github_branch=branch,
        github_token=token,
        modules=modules,
        index_name=index_name,
        proxy_id=proxy_id,
    )


@dataclass(frozen=True)
class MarketplaceConsumerSettings:
    """消费侧 raw 坐标与平台；不包含任何写权限凭据。

    ``platform`` 决定 raw 与资产下载 URL 的渲染形态（github/gitee），见
    :mod:`.urls`。生产侧（写）恒为 GitHub，不在此配置。
    """

    repo_url: str
    github_owner: str
    github_repo: str
    github_branch: str
    modules: tuple[str, ...]
    index_name: str
    platform: str = "github"

    @property
    def enabled(self) -> bool:
        return bool(self.github_owner and self.github_repo)


def load_consumer_settings(producer: MarketplaceSettings | None = None) -> MarketplaceConsumerSettings:
    producer = producer or load_settings()
    # 消费仓库地址优先级：DB 主配置 consumer_repo_url > env > 回落生产仓库坐标。
    # 坐标从地址解析不出时（拆字段旧写法）再回落 MARKETPLACE_CONSUMER_GITHUB_OWNER/REPO。
    source = get_source_config()
    repo_url = source.get("consumer_repo_url") or _resolve_first(("MARKETPLACE_CONSUMER_REPO_URL",), "")
    owner, repo, url_branch = parse_repo_url(repo_url)
    if not (owner and repo):
        owner = _resolve("MARKETPLACE_CONSUMER_GITHUB_OWNER", "") or producer.github_owner
        repo = _resolve("MARKETPLACE_CONSUMER_GITHUB_REPO", "") or producer.github_repo
    branch = (
        source.get("consumer_branch")
        or _resolve("MARKETPLACE_CONSUMER_GITHUB_BRANCH", "")
        or url_branch
        or producer.github_branch
        or "main"
    )
    modules_raw = _resolve("MARKETPLACE_CONSUMER_MODULES", "")
    modules = tuple(m.strip() for m in modules_raw.split(",") if m.strip()) or producer.modules or _DEFAULT_MODULES
    index_name = _resolve("MARKETPLACE_CONSUMER_INDEX_NAME", producer.index_name)
    # 平台优先级：DB > env > 按地址推导。地址没填（回落生产坐标）时用生产地址推导，
    # 生产是 GitHub → github；填了 Gitee 镜像地址 → gitee。
    platform = (
        source.get("consumer_platform")
        or _resolve("MARKETPLACE_CONSUMER_PLATFORM", "")
        or detect_platform(repo_url)
        or detect_platform(producer.repo_url)
    )
    if platform not in ("github", "gitee"):
        platform = "github"
    return MarketplaceConsumerSettings(
        repo_url=repo_url or producer.repo_url,
        github_owner=owner,
        github_repo=repo,
        github_branch=branch,
        modules=modules,
        index_name=index_name,
        platform=platform,
    )


settings = load_settings()
consumer_settings = load_consumer_settings(settings)


def reload_settings() -> MarketplaceSettings:
    """Re-read config and rebind module-level producer/consumer settings."""
    global settings, consumer_settings
    settings = load_settings()
    consumer_settings = load_consumer_settings(settings)
    _invalidate_consumer_cache()
    return settings


def reload_consumer_settings() -> MarketplaceConsumerSettings:
    global consumer_settings
    consumer_settings = load_consumer_settings(settings)
    _invalidate_consumer_cache()
    return consumer_settings


def _invalidate_consumer_cache() -> None:
    """消费侧坐标可能变了（consumer_repo_url/branch/platform）：consumer_cache 的
    缓存键只含仓库相对 path，旧条目属于另一个仓库/分支，必须整体失效。

    函数内延迟 import 避免与 consumer_cache 的循环依赖（它模块级 import 本模块）。
    """
    try:
        from . import consumer_cache

        consumer_cache.invalidate()
    except Exception:  # noqa: BLE001 - 配置重载不允许被缓存层失败打断
        pass
