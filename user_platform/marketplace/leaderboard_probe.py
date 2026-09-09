"""确定性 repo-shape 探测（榜单条目 install_spec/launch_spec 的第一来源）。

上游榜单只给仓库元数据（README/stars/topics），不是可安装清单——之前
install_spec/launch_spec 靠 agent 猜 README，猜对了能装、猜错了只是卡片好看
（"能看不可用"）。探测做的事：用 GitHub git-trees API（经 proxy_manager，与 ``leaderboard_sync``
同一条出网链路）扫仓库文件树，按命中生成**确定性** spec，不靠猜。文件内容读取
（README/SKILL.md/manifest）优先走 ``raw.githubusercontent.com`` 直链——CDN
不占 api.github.com 5000/h 配额；raw 拿不到（私有仓/raw 不可达）才回 Contents API：

- ``.mcp.json`` / ``mcp.json`` → launch_spec（stdio command/args 或 remote url）
- ``.claude-plugin/marketplace.json`` → plugin install_spec
- ``SKILL.md``（任意深度）+ ``.cursor/rules/*.mdc`` + ``AGENTS.md`` → skill
  install_spec 的 entries（含 editors 编辑器维度：SKILL.md=claude、.mdc=cursor、
  AGENTS.md=codex+opencode）
- README 代码块 ``npx`` / ``uvx`` / ``docker`` → 高置信 launch_spec 提示

结果落 ``external_data.probe``——``upsert_item`` 的 ON CONFLICT 每次重同步都整
体刷新 external_data，所以探针随重同步静默保鲜；资源字段仍冻结，已有行的派生
spec 走逐项「同步」按钮（``marketplace_leaderboard_sync_field`` 的
install_spec/launch_spec 字段）显式重派生。

**探测阶段不握手**：``.mcp.json`` 存在本身就是确定性证据；launch_spec 是否真的
能连，由 ``leaderboard_verify`` 显式验证（verified/failed），两层职责分开。
"""
from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

import aiohttp

from . import config as mp_config
from ..git_clients import github_rate_limit_message, is_rate_limit_error

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=60)
# raw 超时收紧：CDN 正常时秒回，不可达时不让每个文件都白等一整个 API 级超时。
_RAW_TIMEOUT = aiohttp.ClientTimeout(total=15)
# git-trees 递归返回量封顶：超出只标 truncated 照解析（部分树也认得出 SKILL.md/.mcp.json）。
_MAX_TREE_ENTRIES = 100_000
# 单仓 Contents 内容请求封顶（优先级：根 manifest > README > SKILL.md）。
_MAX_CONTENT_FETCHES = 8
# 技能集说明补齐：raw 直链多取的 SKILL.md 数量上限（60 个子技能足够覆盖常见集合）。
_MAX_SKILL_DESC_FETCHES = 60
# 单个文件参与解析的字节上限；超出标 too_large，不浪费内存。
_MAX_FILE_BYTES = 256 * 1024
# 存进 external_data.probe 的相关路径封顶（探针只存与派生有关的路径，不存全树）。
_MAX_RELEVANT_PATHS = 2000
# README 提示每种命令最多采信数。
_MAX_HINTS = 5
_ALL_EDITORS = ["claude", "codex", "opencode", "cursor", "gemini"]

UserAgent = "ai-lubricant-leaderboard-probe/1.0"

# README 命令提示：认行内的启动命令形态（npx -y pkg / uvx pkg / docker run image）。
_HINT_RE = {
    "npx": re.compile(r"\bnpx\s+(?:-y\s+|--yes\s+)?([@a-z0-9_][@a-z0-9_./-]*)", re.IGNORECASE),
    "uvx": re.compile(r"\buvx\s+([@a-z0-9_][@a-z0-9_./-]*)", re.IGNORECASE),
    "docker": re.compile(r"docker\s+(?:run|pull)\s+([a-z0-9_][a-z0-9_./:-]+)", re.IGNORECASE),
}

# 根 manifest（派生 launch/plugin spec 的确定性证据）。
_ROOT_MANIFESTS = (".mcp.json", "mcp.json", ".claude-plugin/marketplace.json", "package.json", "pyproject.toml")
_SKILL_ENTRY_NAME_RE = re.compile(r"^name:[ \t]*(.+)$", re.MULTILINE)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def raw_content_url(full_name: str, ref: str, path: str) -> str:
    """公开仓库单文件的 raw 直链（CDN，不占 api.github.com 5000/h 配额）。

    raw 不带 token（只读公开内容）、无 base64 包装、无 1MB 内联限制——Contents
    API 的三个坑一次全免。ref/path 均需 URL 编码（分支名可带 ``/``、路径可带
    特殊字符）。
    """
    from urllib.parse import quote

    owner, _, repo = full_name.partition("/")
    return (
        f"https://raw.githubusercontent.com/{quote(owner, safe='')}/"
        f"{quote(repo, safe='')}/{quote(ref or 'main', safe='')}/"
        f"{quote(str(path).lstrip('/'), safe='/')}"
    )


def _gh_headers() -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json", "User-Agent": UserAgent}
    token = mp_config.settings.github_token
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


async def _gh_get_json(url: str, *, proxy_id: str = "") -> Any:
    """经 proxy_manager GET 一个 GitHub API JSON。失败抛 RuntimeError（调用方兜）。

    ``proxy_id``：本源专用代理（留空回落 mp_config.settings.proxy_id 全局）。
    """
    return await get_json(url, proxy_id=proxy_id)


async def get_json(url: str, *, proxy_id: str = "") -> Any:
    """Public wrapper of the probe's GitHub GET (same proxy + token path).

    For new callers outside the probe that need a one-off GitHub API read with
    the marketplace's outbound proxy and optional ``MARKETPLACE_GITHUB_TOKEN``
    (e.g. resolving the HEAD commit sha when pinning a reference). Raises
    ``RuntimeError(f"HTTP {status}")`` on ``>= 400`` so callers can branch on 404.
    """
    from providers.proxy_manager import get_proxy_manager

    manager = get_proxy_manager()
    resp = await manager.request(
        url=url, method="GET", headers=_gh_headers(),
        timeout=_FETCH_TIMEOUT, proxy_config_id=(proxy_id or mp_config.settings.proxy_id or None),
    )
    if resp.status >= 400:
        text = await resp.text()
        rate_limited = github_rate_limit_message(resp, text)
        if rate_limited:
            raise RuntimeError(rate_limited)
        raise RuntimeError(f"HTTP {resp.status}")
    return await resp.json()


async def fetch_text(full_name: str, path: str, ref: str, *, proxy_id: str = "") -> str | None:
    """Public wrapper of the probe's single-file fetch（raw 优先，Contents API 兜底）.

    Used by the 项目提示词 GitHub 识别 flow to pull a recognized prompt file's
    text (AGENTS.md / markdown) into the prompt editor once, on explicit user
    action — the prompt stores inline content by design. Same 256KB cap and
    proxy/token path as the probe itself.
    """
    return await _gh_fetch_text(full_name, path, ref, proxy_id=proxy_id)


# raw 不可达时短暂停用（进程内）：避免每个文件都先等一次 raw 超时再回 API。
_RAW_DISABLE_SECS = 120
_raw_disabled_until = 0.0


async def fetch_raw_text(
    full_name: str, path: str, ref: str, *,
    proxy_id: str = "", max_bytes: int = _MAX_FILE_BYTES,
    timeout: aiohttp.ClientTimeout | None = None,
) -> str | None:
    """raw.githubusercontent.com 直读单文件（不计 API 配额）。拿不到返回 None。

    None 的含义是「raw 这条路没走通」，调用方自行决定兜底（如回 Contents API）：
    - 404：文件/仓库/私有仓不可读——Contents API 带 token 还能再试一次（树里已
      确认路径存在，raw 404 多半是私有仓或 CDN 异常，不该当真缺失）；
    - 网络失败/5xx：raw 在本网络不可达，停用 120 秒后其余文件直接跳过 raw，
      不再逐个等超时。
    ref 为空或 ``HEAD`` 时 raw 无法解析（raw 只收具体分支/tag/sha），直接 None。
    ``max_bytes`` 覆盖单文件字节上限（榜单 board JSON 1-16MB，远超探针默认 256KB）；
    ``timeout`` 默认收紧到 15s（CDN 秒回），大文件调用方可放宽（board 走 120s）。
    """
    global _raw_disabled_until
    ref = (ref or "").strip()
    if ref in ("", "HEAD") or not full_name or "/" not in full_name:
        return None
    if time.monotonic() < _raw_disabled_until:
        return None
    from providers.proxy_manager import get_proxy_manager

    try:
        resp = await get_proxy_manager().request(
            url=raw_content_url(full_name, ref, path), method="GET", headers=_gh_headers(),
            timeout=timeout or _RAW_TIMEOUT, proxy_config_id=(proxy_id or mp_config.settings.proxy_id or None),
        )
    except Exception:  # noqa: BLE001 — raw 不可达，回 API
        _raw_disabled_until = time.monotonic() + _RAW_DISABLE_SECS
        return None
    if resp.status == 200:
        body = await resp.read()
        if len(body) > max_bytes:
            return None
        return body.decode("utf-8", "replace")
    if resp.status != 404:
        # 403/5xx 等：raw 异常，停用一会儿再试（404 是正常答案，不停用）。
        _raw_disabled_until = time.monotonic() + _RAW_DISABLE_SECS
    return None


async def _gh_fetch_text(full_name: str, path: str, ref: str, *, proxy_id: str = "") -> str | None:
    """取单个文件文本。**raw 直链优先**（CDN、不占 5000/h 配额），拿不到再走
    Contents API（base64，私有仓带 token）。>上限无 content 字段 / 解不出 → None。

    限流错误向上重抛（不再吞成 None）：配额耗尽时继续逐文件拉取只会徒增 403，
    由 probe_repo_shape 统一兜成 ``{"error": ...}`` 停住本轮探测。
    """
    raw_text = await fetch_raw_text(full_name, path, ref, proxy_id=proxy_id)
    if raw_text is not None:
        return raw_text
    try:
        meta = await _gh_get_json(f"https://api.github.com/repos/{full_name}/contents/{path}?ref={ref}", proxy_id=proxy_id)
    except RuntimeError as exc:
        if is_rate_limit_error(exc):
            raise
        return None
    if not isinstance(meta, dict) or meta.get("encoding") != "base64":
        return None
    try:
        raw = base64.b64decode(meta.get("content") or "")
    except (TypeError, ValueError):
        return None
    if len(raw) > _MAX_FILE_BYTES:
        return None
    return raw.decode("utf-8", "replace")


async def probe_repo_shape(full_name: str, ref: str = "main", *, proxy_id: str = "") -> dict:
    """确定性探针。**不抛**——失败返回 ``{"error": str[:500], "fetched_at": iso}``。

    ``proxy_id``：本源专用代理（留空回落 mp_config.settings.proxy_id 全局）。

    步骤：git-trees 递归拿全树 → 选相关路径 → 按预算取内容（根 manifest 优先，
    其次 README，再 SKILL.md）→ README 命令提示正则。树大小/截断都记进结果，
    解析照做（部分树也认得出 SKILL.md 与 .mcp.json）。
    """
    full_name = str(full_name or "").strip().strip("/")
    if not full_name or "/" not in full_name:
        return {"error": "仓库标识形如 owner/repo", "fetched_at": _now_iso()}

    try:
        # ref 兜底：main 404（默认分支不是 main 的仓库）回 HEAD（=默认分支，一定存在）。
        tree_used_ref = ref
        try:
            tree = await _gh_get_json(f"https://api.github.com/repos/{full_name}/git/trees/{ref}?recursive=1", proxy_id=proxy_id)
        except RuntimeError as exc:
            if "HTTP 404" not in str(exc) or ref == "HEAD":
                raise
            tree_used_ref = "HEAD"
            tree = await _gh_get_json(f"https://api.github.com/repos/{full_name}/git/trees/HEAD?recursive=1", proxy_id=proxy_id)
        if not isinstance(tree, dict) or not isinstance(tree.get("tree"), list):
            raise RuntimeError("git-trees 返回形态异常")
    except Exception as exc:  # noqa: BLE001 — 探针绝不让同步失败
        return {"error": str(exc)[:500], "fetched_at": _now_iso()}

    entries = tree["tree"]
    blob_paths = [
        str(e.get("path") or "") for e in entries
        if isinstance(e, dict) and e.get("type") == "blob" and e.get("path")
    ]
    truncated = bool(tree.get("truncated")) or len(entries) > _MAX_TREE_ENTRIES

    # 相关路径：manifest / README / skill 形态文件（派生要用的全在这；不存全树，控 JSONB 体积）。
    lower = {p.lower(): p for p in blob_paths}
    relevant: list[str] = []
    for manifest in _ROOT_MANIFESTS:
        if manifest in lower:
            relevant.append(lower[manifest])
    for p in blob_paths:
        base = PurePosixPath(p).name.lower()
        if base in ("readme.md", "readme.rst") or base == "skill.md" \
                or (p.startswith(".cursor/rules/") and p.endswith(".mdc")) \
                or base == "agents.md" or base == ".cursorrules":
            relevant.append(p)
    # 去重保序 + 封顶。
    relevant = list(dict.fromkeys(relevant))[:_MAX_RELEVANT_PATHS]

    # 内容预算：根 manifest 优先（数量 ≤5），README 一个，SKILL.md 用余量（最短路径优先=更可能是本体）。
    skill_paths = [p for p in relevant if PurePosixPath(p).name.lower() == "skill.md"]
    skill_paths.sort(key=lambda p: (len(PurePosixPath(p).parts), len(p)))
    readme_path = next((p for p in relevant if PurePosixPath(p).name.lower() in ("readme.md", "readme.rst")), "")
    fetch_plan = [p for p in relevant if PurePosixPath(p).name.lower() != "skill.md" and p != readme_path]
    if readme_path:
        fetch_plan.append(readme_path)
    fetch_plan.extend(skill_paths[: max(0, _MAX_CONTENT_FETCHES - len(fetch_plan))])

    files: dict[str, Any] = {}
    for path in fetch_plan[:_MAX_CONTENT_FETCHES]:
        text = await _gh_fetch_text(full_name, path, tree_used_ref, proxy_id=proxy_id)
        if text is None:
            files[path] = {"too_large": True}
            continue
        base = PurePosixPath(path).name.lower()
        if base in (".mcp.json", "mcp.json", "package.json") or path.endswith("marketplace.json"):
            try:
                files[path] = json.loads(text)
            except ValueError:
                files[path] = text
        else:  # pyproject.toml / README / SKILL.md 存原文（hints 提示 / frontmatter name 提取用）
            files[path] = text

    # 技能集说明补齐：fetch_plan 的 8 个内容名额被 manifest/README 占掉后，多数
    # SKILL.md 拿不到 frontmatter description。用 raw 直链（不占 API 配额）把剩下
    # 的 SKILL.md 补进来——只给 name/description 解析用，不参与 hints/manifest 派生。
    # raw 不可达（cooldown）或 404 时跳过：该条目说明留空，不阻塞探针。
    extra_skill = [p for p in skill_paths if p not in files][:_MAX_SKILL_DESC_FETCHES]
    for path in extra_skill:
        text = await fetch_raw_text(full_name, path, tree_used_ref, proxy_id=proxy_id)
        if text is not None:
            files[path] = text

    hints: dict[str, list[dict]] = {"npx": [], "uvx": [], "docker": []}
    readme_text = files.get(readme_path) if readme_path and isinstance(files.get(readme_path), str) else ""
    if readme_text:
        for kind, pattern in _HINT_RE.items():
            seen: set[str] = set()
            for match in pattern.finditer(readme_text):
                pkg = (match.group(1) or "").strip().strip("`")
                if not pkg or pkg in seen:
                    continue
                seen.add(pkg)
                hints[kind].append({"pkg": pkg, "text": match.group(0).strip("`\n")})
                if len(hints[kind]) >= _MAX_HINTS:
                    break

    return {
        "ref": tree_used_ref,
        "fetched_at": _now_iso(),
        "tree_count": len(blob_paths),
        "truncated": truncated,
        # 全部文件路径（封顶）：技术栈识别 detect_stack 靠扩展名分布统计语言，
        # relevant（只含 manifest/README/skill 形态）不够用，需全量 blob 路径。
        "blob_paths": blob_paths[:_MAX_RELEVANT_PATHS],
        "tree_paths": relevant,
        "files": files,
        "hints": hints,
        "error": "",
    }


def _parse_frontmatter_name(text: str) -> str:
    """极简 frontmatter name 提取（只认顶层 ``name: value`` 单行）。"""
    for line in text.replace("\r\n", "\n").split("\n"):
        if line.startswith((" ", "\t", "-")):
            continue
        m = _SKILL_ENTRY_NAME_RE.match(line)
        if m:
            value = m.group(1).strip()
            if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            return value
    return ""


_SKILL_ENTRY_DESC_RE = re.compile(r"^description:[ \t]*(.*)$", re.MULTILINE)


def _parse_frontmatter_description(text: str) -> str:
    """极简 frontmatter description 提取：单行值或块标量（``|-``/``>`` 折叠）。

    只认顶层的 ``description:``（缩进行跳过，避免嵌套字段误命中）。块标量取
    后续缩进行按序拼接（不严格重 YAML 折叠语义，够展示用）。>200 字截断。
    """
    if not isinstance(text, str) or not text:
        return ""
    lines = text.replace("\r\n", "\n").split("\n")
    for i, line in enumerate(lines):
        if line.startswith((" ", "\t", "-")):
            continue
        m = _SKILL_ENTRY_DESC_RE.match(line)
        if not m:
            continue
        value = (m.group(1) or "").strip()
        if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
            return value[1:-1][:200]
        if value not in ("|-", "|", ">", ">-", "|-", ">", ""):
            return value[:200] if value else ""
        # 块标量：收集后续缩进行直到回到顶层。
        block: list[str] = []
        for cont in lines[i + 1:]:
            if not cont.strip():
                block.append("")
                continue
            if cont.startswith((" ", "\t")):
                block.append(cont.strip())
            else:
                break
        joined = " ".join(part for part in block if part)
        return joined[:200]
    return ""


def _group_skill_entries(probe: dict, repo_short: str) -> list[dict]:
    """从探针文件树分出 skill entries（含 editors 维度）。

    - SKILL.md（任意深度）→ claude entry，按父目录分组；name 优先 frontmatter，
      回落父目录 basename。
    - .cursor/rules/*.mdc（+ 旧版根 .cursorrules）→ cursor entry。
    - AGENTS.md（任意深度）→ codex+opencode entry，按父目录分组。
    同 (path, entry) 被多规则产出 → editors 并集保序去重。

    ``.cursor/rules`` 的 path 归一到 ``""``（仓库根）：它不是技能目录，而是根技能
    的 cursor 形态文件——若保留 ``.cursor/rules`` 当独立父目录，根目录 skill +
    cursor 规则会被 :func:`derive_primary_type` 误判成"多目录技能集合"（如
    obra/superpowers：根 SKILL.md + AGENTS.md + .cursor/rules/*.mdc）。
    """
    files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
    repo_short = repo_short or ""
    groups: dict[tuple[str, str], dict] = {}

    def _ensure(path: str, entry: str, name: str, editors: list[str], description: str = "") -> None:
        key = (path, entry)
        existing = groups.get(key)
        if existing is None:
            groups[key] = {"name": name or repo_short, "path": path, "entry": entry,
                           "editors": list(editors), "description": description}
            return
        for editor in editors:
            if editor not in existing["editors"]:
                existing["editors"].append(editor)
        # 多形态文件命中同一条目（SKILL.md + .mdc）：说明取第一个非空。
        if description and not existing.get("description"):
            existing["description"] = description

    for p in probe.get("tree_paths") or []:
        base = PurePosixPath(p).name
        parent = str(PurePosixPath(p).parent) if str(PurePosixPath(p).parent) != "." else ""
        lower_base = base.lower()
        if lower_base == "skill.md":
            text = files.get(p)
            name = _parse_frontmatter_name(text) if isinstance(text, str) else ""
            desc = _parse_frontmatter_description(text) if isinstance(text, str) else ""
            if not name:
                name = PurePosixPath(parent).name if parent else repo_short
            _ensure(parent, "SKILL.md", name, list(_ALL_EDITORS), desc)
        elif p.startswith(".cursor/rules/") and lower_base.endswith(".mdc"):
            # 归一 path=""（仓库根）：.cursor/rules 不是技能目录，是根技能的
            # cursor 形态；entry 用 .mdc 文件名与 SKILL.md/AGENTS.md 区分开。
            _ensure("", f".cursor/rules/{base}", PurePosixPath(p).stem, list(_ALL_EDITORS))
        elif lower_base == "agents.md":
            name = PurePosixPath(parent).name if parent else repo_short
            _ensure(parent, "AGENTS.md", name, list(_ALL_EDITORS))
    # 根 .cursorrules（旧版 cursor 规则文件）→ cursor entry。按全量 blob 路径
    # 判存在性（与 probe_repo_shape 的 lower 同源：整路径小写键）。path 同样
    # 归一 ""（根技能的形态文件，不当独立技能）。
    blob_paths = probe.get("blob_paths") or probe.get("tree_paths") or []
    if ".cursorrules" in {str(p).lower() for p in blob_paths}:
        _ensure("", ".cursorrules", repo_short, list(_ALL_EDITORS))
    return list(groups.values())


def _derive_launch_spec(probe: dict) -> tuple[dict, str]:
    """launch_spec 证据链：.mcp.json（确定性）> README 唯一 npx/uvx 提示。都没有 → 空。"""
    files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
    for name in (".mcp.json", "mcp.json"):
        value = files.get(name)
        if not isinstance(value, dict):
            continue
        servers = value.get("mcpServers") or value.get("servers") or {}
        if not isinstance(servers, dict) or not servers:
            continue
        first = next(iter(servers.values()))
        if not isinstance(first, dict):
            continue
        url = str(first.get("url") or "").strip()
        if url:
            transport = str(first.get("transport") or "").strip().lower()
            if transport not in ("sse", "streamable-http"):
                transport = "sse" if url.rstrip("/").endswith("/sse") else "streamable-http"
            return {"kind": "remote", "url": url, "transport": transport, "source": "probe:.mcp.json"}, "probe:.mcp.json"
        command = str(first.get("command") or "").strip()
        if command:
            args = [str(a) for a in (first.get("args") or []) if str(a).strip()]
            env = [str(e) for e in (first.get("env") or []) if str(e).strip()]
            return {"kind": "stdio", "command": command, "args": args, "env": env,
                    "source": "probe:.mcp.json"}, "probe:.mcp.json"

    hints = probe.get("hints") if isinstance(probe.get("hints"), dict) else {}
    npx = hints.get("npx") or []
    uvx = (probe.get("hints") or {}).get("uvx") or []
    if len(npx) == 1:
        return {"kind": "stdio", "command": "npx", "args": ["-y", npx[0]["pkg"]], "env": [],
                "source": "probe:readme"}, "probe:readme"
    if not npx and len(uvx) == 1:
        return {"kind": "stdio", "command": "uvx", "args": [uvx[0]["pkg"]], "env": [],
                "source": "probe:readme"}, "probe:readme"
    return {}, ""


def _derive_plugin_spec(probe: dict, repo_full_name: str) -> dict:
    files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
    marketplace = files.get(".claude-plugin/marketplace.json")
    if not isinstance(marketplace, dict):
        return {}
    provider = str(marketplace.get("provider") or "claude")
    used_ref = str(probe.get("ref") or "main")
    return {
        "download_url": f"https://github.com/{repo_full_name}/archive/refs/heads/{used_ref}.zip",
        "provider": provider,
        "entry": ".claude-plugin/marketplace.json",
    }


def derive_specs_from_probe(probe: dict, modules: list[str], repo_full_name: str) -> dict:
    """PURE：探针证据 → spec。只填有证据的字段，不臆造。

    launch_spec 优先级：``.mcp.json`` 解析 > README 命中唯一 npx/uvx 包。都没有 →
    空 spec（**不动** agent/管理员已填的——合并只在调用侧做）。
    """
    repo_short = repo_full_name.split("/")[-1] if repo_full_name else ""
    install_spec: dict[str, Any] = {}

    entries = _group_skill_entries(probe, repo_short)
    if not entries and "skill" in (modules or []):
        # skill 在 modules 但没探到 SKILL.md → 兜底单 entry（verify 之后会标记）。
        entries = [{"name": repo_short, "path": "", "entry": "SKILL.md", "editors": list(_ALL_EDITORS)}]
    if entries:
        install_spec["skill"] = {
            "install_method": "github_clone",
            "ref": str(probe.get("ref") or "main"),
            "entries": entries,
        }

    plugin = _derive_plugin_spec(probe, repo_full_name)
    if plugin:
        install_spec["plugin"] = plugin

    launch_spec, launch_source = _derive_launch_spec(probe)
    return {"install_spec": install_spec, "launch_spec": launch_spec, "launch_source": launch_source}


def normalize_skill_install_spec(spec: dict | None) -> dict:
    """把 install_spec.skill 收口成 entries 形态（幂等）。旧单对象读侧即升级。

    旧：{"install_method": "github_clone", "path": "skills", "ref": "main"}
    新：{"install_method": "github_clone", "ref": "main",
         "entries": [{"name": ..., "path": "skills", "entry": "SKILL.md", "editors": [...]}]}
    """
    if not isinstance(spec, dict):
        return {}
    skill = spec.get("skill")
    if not isinstance(skill, dict):
        return dict(spec)
    if isinstance(skill.get("entries"), list) and skill["entries"]:
        return dict(spec)
    path = str(skill.get("path") or "").strip().strip("/")
    entry = {"name": PurePosixPath(path).name if path else "", "path": path,
             "entry": "SKILL.md", "editors": list(_ALL_EDITORS)}
    canonical = {k: v for k, v in skill.items() if k not in ("path", "entries")}
    canonical.setdefault("ref", str(skill.get("ref") or "main"))
    canonical["entries"] = [entry]
    return {**spec, "skill": canonical}


def derive_target_modules_from_probe(probe: dict) -> list[str]:
    """从探针证据推导分类（安装形态）。PURE。

    - .mcp.json / mcp.json / README npx|uvx 提示 → mcp
    - .claude-plugin/marketplace.json → plugin
    - SKILL.md / .cursor rules / AGENTS.md / .cursorrules → skill
    - package.json bin / pyproject [project.scripts] → mcp（npx/uvx 派生，较弱但合理）

    与 classify()（board+category 关键词）互补：classify 是同步时零网络先验，探针是
    网络后验（能看到真实文件树）。手动添加的草稿 classify("manual") 常推不出分类，
    全靠这里回填；同步条目则用它补 classify 漏掉的形态。
    """
    files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
    tree_paths = [str(p).lower() for p in (probe.get("tree_paths") or [])]
    modules: list[str] = []

    def _add(module: str) -> None:
        if module not in modules:
            modules.append(module)

    if isinstance(files.get(".mcp.json"), dict) or isinstance(files.get("mcp.json"), dict):
        _add("mcp")
    if isinstance(files.get(".claude-plugin/marketplace.json"), dict):
        _add("plugin")
    if any(p.endswith("skill.md") or p.endswith("agents.md") or p.endswith(".cursorrules")
           or (".cursor/rules/" in p and p.endswith(".mdc")) for p in tree_paths):
        _add("skill")
    hints = probe.get("hints") if isinstance(probe.get("hints"), dict) else {}
    if hints.get("npx") or hints.get("uvx"):
        _add("mcp")
    # package.json bin / pyproject scripts 也是 mcp 启动证据（npx/uvx）
    pkg = files.get("package.json")
    if isinstance(pkg, dict) and pkg.get("bin"):
        _add("mcp")
    return modules


def derive_primary_type(probe: dict, repo_full_name: str = "") -> str:
    """单选主类型，优先级 ``skills > plugin > skill > mcp > prompt``。PURE。

    与 :func:`derive_target_modules_from_probe`（多模块清单）互补：那个返回"仓库
    能装成什么"的全部形态，这个返回"默认按哪种形态装"的唯一答案。一个仓库同时
    是插件（``.claude-plugin/marketplace.json``）又含多个 skill（如 anthropics/skills）
    时按 skills 走——集合形态更细，能让任务按子技能勾选。

    判定（按优先级短路）：
    - ``skills``：≥2 个**不同父目录**的 skill 条目（SKILL.md/AGENTS.md/.cursor 规则
      按父目录去重——同目录多编辑器形态算一个技能）。
    - ``plugin``：根 ``.claude-plugin/marketplace.json`` 在场（且 skill 条目 ≤1）。
    - ``skill``：恰好 1 个 skill 条目。
    - ``mcp``：launch_spec 有 kind（.mcp.json 或 README 唯一 npx/uvx 提示）。
    - ``prompt``：命中 CLAUDE.md/AGENTS.md 提示词文件、且以上都没命中。
    """
    repo_short = repo_full_name.split("/")[-1] if repo_full_name else ""
    entries = _group_skill_entries(probe, repo_short)
    distinct_paths = {str(e.get("path") or "") for e in entries}
    files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
    has_plugin = isinstance(files.get(".claude-plugin/marketplace.json"), dict)
    launch, _ = _derive_launch_spec(probe)
    if len(distinct_paths) >= 2:
        return "skills"
    if has_plugin:
        return "plugin"
    if entries:
        return "skill"
    if launch:
        return "mcp"
    tree_paths = [str(p).lower() for p in (probe.get("tree_paths") or [])]
    if any(p.endswith("claude.md") or p.endswith("agents.md") for p in tree_paths):
        return "prompt"
    return ""


async def attach_probe(item: dict, *, proxy_id: str = "") -> dict:
    """对榜单 item 跑探针：结果落 ``external_data.probe``，派生 spec 合并进 item。

    ``proxy_id``：本源专用代理（留空回落 mp_config.settings.proxy_id 全局）。

    合并口径（**只填能填的键**）：install_spec 按 evidence 键合并（prompt 等已有
    键保留；skill/plugin 仅在 item 尚无该键时填——新条目首次入池即带全量，已入池
    行资源字段冻结、重派生走「同步」按钮）；launch_spec 仅在现值无有效 kind（空或
    none）时填，不覆盖 agent/人工结果。**分类（target_modules）也回填**：探针发现的
    形态（skill/mcp/plugin）并入现有分类，解决了手动添加草稿 classify 推不出、分类
    为空的问题。失败容错：探针异常 → probe={"error":...}，已有 spec 一律不动。返回原 item。

    技术栈识别（stack/stack_tags）在同一探针内顺带做——喂探针已抓的文件给共享
    引擎，零新增网络；失败只记 probe.stack_error，不影响 spec 派生与发布。
    """
    external = item.get("external_data") if isinstance(item.get("external_data"), dict) else {}
    full_name = str(external.get("repo_full_name") or item.get("repo_full_name") or "").strip().strip("/")
    if not full_name or "/" not in full_name:
        return item

    probe = await probe_repo_shape(full_name, proxy_id=proxy_id)
    external["probe"] = probe
    item["external_data"] = external
    if probe.get("error"):
        return item

    # 技术栈识别：把探针已抓的树路径与文件喂共享引擎（detect_stack 的
    # fetch_text 只查 files 字典，零新增网络）。files 里 JSON manifest 存的是
    # 已解析对象，喂引擎前序列化回文本；探测预算没抓到的清单引擎拿不到，
    # 识别偏保守但不假（框架弱识别是已知限制，扩 _ROOT_MANIFESTS 即自动受益）。
    # 放在 spec 派生之前：识别只读 probe，先算好，派生失败也不丢 stack。
    try:
        from ..stack_detector import detect_stack, stack_tags

        probe_files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
        probe_paths = [str(p) for p in (probe.get("blob_paths") or []) if p]

        async def _probe_fetch_text(path: str) -> str | None:
            value = probe_files.get(path)
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                # too_large 标记没有可解析文本。
                if value.get("too_large"):
                    return None
                try:
                    return json.dumps(value)
                except (TypeError, ValueError):
                    return None
            return None

        profile = await detect_stack(
            probe_paths,
            _probe_fetch_text,
            truncated_hint=bool(probe.get("truncated")),
        )
        item["stack"] = profile
        item["stack_tags"] = stack_tags(profile)
    except Exception as exc:  # noqa: BLE001 — 技术栈识别失败不影响主链路
        probe["stack_error"] = str(exc)[:500]

    modules = item.get("target_modules")
    if not isinstance(modules, list):
        modules = [item.get("target_module")] if item.get("target_module") else []
    try:
        derived = derive_specs_from_probe(probe, modules, full_name)
    except Exception as exc:  # noqa: BLE001 — 派生失败等同探针失败，不动已有 spec
        probe["error"] = str(exc)[:500]
        return item

    existing_spec = item.get("install_spec") if isinstance(item.get("install_spec"), dict) else {}
    merged = dict(existing_spec)
    for key, value in derived["install_spec"].items():
        if value and not merged.get(key):
            merged[key] = value
    item["install_spec"] = merged

    current = item.get("launch_spec") if isinstance(item.get("launch_spec"), dict) else {}
    if derived["launch_spec"] and str(current.get("kind") or "").strip() in ("", "none"):
        item["launch_spec"] = derived["launch_spec"]
        probe["launch_source"] = derived["launch_source"]

    # 分类回填：探针发现的形态并入（保序去重）；有则置 installable。
    probe_modules = derive_target_modules_from_probe(probe)
    if probe_modules:
        merged_modules = list(dict.fromkeys([m for m in modules if m] + probe_modules))
        item["target_modules"] = merged_modules
        item["target_module"] = merged_modules[0]
        item["installable"] = True
    return item