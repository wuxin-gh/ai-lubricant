"""确定性 repo-shape 探测（榜单条目 install_spec/launch_spec 的第一来源）。

上游榜单只给仓库元数据（README/stars/topics），不是可安装清单——之前
install_spec/launch_spec 靠 agent 猜 README，猜对了能装、猜错了只是卡片好看
（"能看不可用"）。探测做的事：用 GitHub git-trees/Contents API（经
proxy_manager，与 ``leaderboard_sync`` 同一条出网链路）扫仓库文件树，按命中
生成**确定性** spec，不靠猜：

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
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

import aiohttp

from . import config as mp_config

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=60)
# git-trees 递归返回量封顶：超出只标 truncated 照解析（部分树也认得出 SKILL.md/.mcp.json）。
_MAX_TREE_ENTRIES = 100_000
# 单仓 Contents 内容请求封顶（优先级：根 manifest > README > SKILL.md）。
_MAX_CONTENT_FETCHES = 8
# 单个文件参与解析的字节上限；超出标 too_large，不浪费内存。
_MAX_FILE_BYTES = 256 * 1024
# 存进 external_data.probe 的相关路径封顶（探针只存与派生有关的路径，不存全树）。
_MAX_RELEVANT_PATHS = 2000
# README 提示每种命令最多采信数。
_MAX_HINTS = 5

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


def _gh_headers() -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json", "User-Agent": UserAgent}
    token = mp_config.settings.github_token
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


async def _gh_get_json(url: str) -> Any:
    """经 proxy_manager GET 一个 GitHub API JSON。失败抛 RuntimeError（调用方兜）。"""
    from providers.proxy_manager import get_proxy_manager

    manager = get_proxy_manager()
    resp = await manager.request(
        url=url, method="GET", headers=_gh_headers(),
        timeout=_FETCH_TIMEOUT, proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status >= 400:
        raise RuntimeError(f"HTTP {resp.status}")
    return await resp.json()


async def _gh_fetch_text(full_name: str, path: str, ref: str) -> str | None:
    """取单个文件文本（Contents API，base64）。>1MB 无 content 字段 / 解不出 → None。"""
    try:
        meta = await _gh_get_json(f"https://api.github.com/repos/{full_name}/contents/{path}?ref={ref}")
    except RuntimeError:
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


async def probe_repo_shape(full_name: str, ref: str = "main") -> dict:
    """确定性探针。**不抛**——失败返回 ``{"error": str[:500], "fetched_at": iso}``。

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
            tree = await _gh_get_json(f"https://api.github.com/repos/{full_name}/git/trees/{ref}?recursive=1")
        except RuntimeError as exc:
            if "HTTP 404" not in str(exc) or ref == "HEAD":
                raise
            tree_used_ref = "HEAD"
            tree = await _gh_get_json(f"https://api.github.com/repos/{full_name}/git/trees/HEAD?recursive=1")
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
        text = await _gh_fetch_text(full_name, path, tree_used_ref)
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


def _group_skill_entries(probe: dict, repo_short: str) -> list[dict]:
    """从探针文件树分出 skill entries（含 editors 维度）。

    - SKILL.md（任意深度）→ claude entry，按父目录分组；name 优先 frontmatter，
      回落父目录 basename。
    - .cursor/rules/*.mdc（+ 旧版根 .cursorrules）→ cursor entry。
    - AGENTS.md（任意深度）→ codex+opencode entry，按父目录分组。
    同 (path, entry) 被多规则产出 → editors 并集保序去重。
    """
    files = probe.get("files") if isinstance(probe.get("files"), dict) else {}
    repo_short = repo_short or ""
    groups: dict[tuple[str, str], dict] = {}

    def _ensure(path: str, entry: str, name: str, editors: list[str]) -> None:
        key = (path, entry)
        existing = groups.get(key)
        if existing is None:
            groups[key] = {"name": name or repo_short, "path": path, "entry": entry,
                           "editors": list(editors)}
            return
        for editor in editors:
            if editor not in existing["editors"]:
                existing["editors"].append(editor)

    for p in probe.get("tree_paths") or []:
        base = PurePosixPath(p).name
        parent = str(PurePosixPath(p).parent) if str(PurePosixPath(p).parent) != "." else ""
        lower_base = base.lower()
        if lower_base == "skill.md":
            text = files.get(p)
            name = _parse_frontmatter_name(text) if isinstance(text, str) else ""
            if not name:
                name = PurePosixPath(parent).name if parent else repo_short
            _ensure(parent, "SKILL.md", name, ["claude"])
        elif p.startswith(".cursor/rules/") and lower_base.endswith(".mdc"):
            _ensure(".cursor/rules", base, PurePosixPath(p).stem, ["cursor"])
        elif lower_base == "agents.md":
            name = PurePosixPath(parent).name if parent else repo_short
            _ensure(parent, "AGENTS.md", name, ["codex", "opencode"])
    # 根 .cursorrules（旧版 cursor 规则文件）→ cursor entry。按全量 blob 路径
    # 判存在性（与 probe_repo_shape 的 lower 同源：整路径小写键）。
    blob_paths = probe.get("blob_paths") or probe.get("tree_paths") or []
    if ".cursorrules" in {str(p).lower() for p in blob_paths}:
        _ensure(".cursor/rules", ".cursorrules", repo_short, ["cursor"])
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
        # skill 在 modules 但没探到 SKILL.md → 兜底单 claude entry（verify 之后会标记）。
        entries = [{"name": repo_short, "path": "", "entry": "SKILL.md", "editors": ["claude"]}]
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
         "entries": [{"name": ..., "path": "skills", "entry": "SKILL.md", "editors": ["claude"]}]}
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
             "entry": "SKILL.md", "editors": ["claude"]}
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


async def attach_probe(item: dict) -> dict:
    """对榜单 item 跑探针：结果落 ``external_data.probe``，派生 spec 合并进 item。

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

    probe = await probe_repo_shape(full_name)
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