"""GitHub repo recognition: probe a repo and build reference manifests.

Wraps :mod:`user_platform.marketplace.leaderboard_probe` (the repo-shape
识别 we already wrote) so the add-MCP/Skill/Plugin/项目提示词 flows can offer
"从 GitHub 识别" without re-implementing probing. **代码做引用, 不做复制** —
the probe is *called*, not duplicated, and the manifests built here hold only
GitHub coordinates (repo / ref / path / entry); no file bytes are copied.

The probe's ``install_spec`` shape (entries / download_url) is mapped into the
manifest shape :func:`resource_reference_service.resolve_reference_specs`
reads at delivery time — so a reference created from these coords resolves
through the existing ``install_method=github_clone`` (node ``git clone``) or
archive-zip path with **no new node-side code**.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlsplit

from .marketplace import config as mp_config
from .marketplace import leaderboard_probe


# ── URL parsing ──────────────────────────────────────────────────────────────

def parse_github_input(value: str) -> tuple[str, str, str]:
    """Turn user input into ``(full_name, ref, path)``.

    Accepts ``owner/repo``, ``https://github.com/owner/repo[.git]``,
    ``.../tree/<ref>[/sub/path]``, ``.../blob/<ref>/<file>``,
    ``git@github.com:owner/repo.git``. ``ref``/``path`` are empty when absent.
    Returns ``("", "", "")`` when the input is not a GitHub repo — callers turn
    that into a 400. Only GitHub is recognized: the probe's fetch layer is
    hardcoded to ``api.github.com``.
    """
    s = (value or "").strip()
    if not s:
        return "", "", ""
    # SSH form git@github.com:owner/repo.git → normalize to https URL.
    if s.startswith("git@"):
        _, _, rest = s.partition(":")
        s = f"https://github.com/{rest}"
    parts = urlsplit(s)
    if parts.scheme and parts.netloc:
        host = parts.netloc.lower()
        if "@" in host:  # strip user:pass@
            host = host.rpartition("@")[2]
        if host not in ("github.com", "www.github.com"):
            return "", "", ""
        seg = parts.path or ""
    else:
        # Shorthand: "owner/repo[/tree/ref/...]" with no scheme.
        seg = (parts.path or s)
    if seg.endswith(".git"):
        seg = seg[:-4]
    seg = seg.strip("/")
    crumbs = [unquote(c) for c in seg.split("/") if c]
    if len(crumbs) < 2:
        return "", "", ""
    full_name = f"{crumbs[0]}/{crumbs[1]}"
    ref = ""
    sub_path = ""
    # /tree/<ref>[/...] or /blob/<ref>/<file> or /commit/<sha>
    if len(crumbs) >= 4 and crumbs[2] in ("tree", "blob", "commit"):
        ref = crumbs[3]
        if len(crumbs) > 4:
            sub_path = "/".join(crumbs[4:])
    return full_name, ref, sub_path


async def _head_sha(full_name: str, ref: str, *, proxy_id: str = "") -> str:
    """Resolve the commit sha ``ref`` points at, for the pin-to-commit option.

    A 40-hex ``ref`` is already a commit. A branch name resolves via
    ``branches/{ref}``; a tag or commit via ``commits/{ref}``. Empty ``ref``
    falls back to the repo's default branch. Returns ``""`` on any failure
    (non-fatal — the caller just keeps the branch ref instead of pinning).
    """
    ref = (ref or "").strip()
    if len(ref) == 40 and all(c in "0123456789abcdef" for c in ref.lower()):
        return ref
    proxy_id = proxy_id or mp_config.settings.proxy_id or ""
    try:
        if ref:
            try:
                data = await leaderboard_probe.get_json(
                    f"https://api.github.com/repos/{full_name}/branches/{ref}",
                    proxy_id=proxy_id,
                )
                commit = data.get("commit") if isinstance(data, dict) else None
                if isinstance(commit, dict) and commit.get("sha"):
                    return str(commit["sha"])
            except RuntimeError as exc:
                if "HTTP 404" not in str(exc):
                    raise
            # Not a branch → try commits/{ref} (tags + commits).
            data = await leaderboard_probe.get_json(
                f"https://api.github.com/repos/{full_name}/commits/{ref}",
                proxy_id=proxy_id,
            )
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return str(data[0].get("sha") or "")
            if isinstance(data, dict):
                return str(data.get("sha") or "")
        else:
            repo = await leaderboard_probe.get_json(
                f"https://api.github.com/repos/{full_name}", proxy_id=proxy_id,
            )
            default_branch = str(repo.get("default_branch") or "") if isinstance(repo, dict) else ""
            if not default_branch:
                return ""
            data = await leaderboard_probe.get_json(
                f"https://api.github.com/repos/{full_name}/branches/{default_branch}",
                proxy_id=proxy_id,
            )
            commit = data.get("commit") if isinstance(data, dict) else None
            if isinstance(commit, dict) and commit.get("sha"):
                return str(commit["sha"])
    except Exception:
        return ""
    return ""


# ── Probe + normalize ────────────────────────────────────────────────────────

async def _repo_metadata(full_name: str, *, proxy_id: str = "") -> dict:
    """Read lightweight repository metadata for editable recognition defaults."""
    try:
        data = await leaderboard_probe.get_json(
            f"https://api.github.com/repos/{full_name}", proxy_id=proxy_id,
        )
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


async def recognize_repo(repo_input: str, *, ref: str = "", force_type: str = "") -> dict:
    """Probe a GitHub repo and return a normalized preview. Never raises.

    Output: ``{repo_full_name, ref, head_sha, type, install_spec, launch_spec,
    skill_entries, repo_meta, summary, error}``. ``type`` 是**单选主类型**
    （plugin|skill|mcp|prompt，优先级 plugin>skill>mcp>prompt——插件是容器：
    marketplace.json 在场或 ≥2 个技能条目都算插件）；``force_type`` 非空时按用户
    所选类型重派生（重新识别）：类型证据不足时明确报错而非静默降级。legacy
    ``skills``（技能集）自动归一成 plugin 容器。``skill_entries`` 是 skill 列表——
    type=skill 恰 1 项、type=plugin 可 N 项（容器整包安装，任务期再按子技能勾选）。
    ``error`` is ``""`` on success. Pure preview — nothing is persisted.
    """
    full_name, parsed_ref, _path = parse_github_input(repo_input)
    if not full_name:
        return {"error": "请输入 GitHub 仓库地址（owner/repo 或完整 URL）", "repo_full_name": ""}
    proxy_id = mp_config.settings.proxy_id or ""
    # 先取仓库元数据：default_branch 让 use_ref 命中真实默认分支（master 仓库不会被
    # main 404 拖累），description 给前端编辑表单预填；一次调用同时省掉 _head_sha 里
    # 对 repos/{full_name} 的重复请求。
    meta = await _repo_metadata(full_name, proxy_id=proxy_id)
    default_branch = str(meta.get("default_branch") or "")
    use_ref = (ref or parsed_ref or default_branch or "main").strip()
    probe = await leaderboard_probe.probe_repo_shape(full_name, use_ref, proxy_id=proxy_id)
    if probe.get("error"):
        return {"repo_full_name": full_name, "ref": use_ref, "error": probe["error"]}
    modules = leaderboard_probe.derive_target_modules_from_probe(probe)
    specs = leaderboard_probe.derive_specs_from_probe(probe, modules, full_name)
    head_sha = await _head_sha(full_name, use_ref, proxy_id=proxy_id)
    install_spec = specs.get("install_spec") or {}
    skill_entries = (install_spec.get("skill") or {}).get("entries") or []
    auto_type = leaderboard_probe.derive_primary_type(probe, full_name)
    # 类型可由用户改选后「重新识别」：证据支持所选类型才放行，否则明确报错。
    primary_type = auto_type
    type_error = ""
    force_type = (force_type or "").strip()
    # legacy 'skills'（技能集）→ plugin 容器：识别端点把 skills 视作多技能插件。
    # force_type 归一后，type 判定与 manifest 派生只有 plugin 一种容器口径。
    if force_type == "skills":
        force_type = "plugin"
    if force_type:
        distinct_paths = {str(e.get("path") or "") for e in skill_entries}
        if force_type not in ("plugin", "skill", "mcp", "prompt"):
            type_error = f"未知类型 {force_type}"
        elif force_type == "plugin" and not (
            install_spec.get("plugin") or len(distinct_paths) >= 2
        ):
            # 插件=容器：marketplace.json 在场 或 ≥2 个不同父目录的技能条目。
            type_error = "该仓库未识别到 .claude-plugin/marketplace.json，且技能条目不足 2 个，无法按「插件」处理"
        elif force_type == "skill" and not skill_entries:
            type_error = "该仓库未识别到 SKILL.md / AGENTS.md / .cursor 规则"
        elif force_type == "mcp" and not (specs.get("launch_spec") or {}).get("kind"):
            type_error = "该仓库未识别到 MCP 启动证据（.mcp.json / README npx·uvx）"
        elif force_type == "prompt":
            tree_paths = [str(p).lower() for p in (probe.get("tree_paths") or [])]
            if not any(p.endswith("claude.md") or p.endswith("agents.md") or p.endswith(".md") for p in tree_paths):
                type_error = "该仓库未识别到 Markdown 提示词文件"
        if not type_error:
            primary_type = force_type
    summary = {
        "tree_count": probe.get("tree_count") or 0,
        "truncated": bool(probe.get("truncated")),
        "hit_files": list((probe.get("files") or {}).keys()),
        "hints": probe.get("hints") or {},
    }
    return {
        "repo_full_name": full_name,
        "ref": use_ref,
        "head_sha": head_sha,
        "type": primary_type,
        "auto_type": auto_type,
        "modules": modules,
        "install_spec": install_spec,
        "launch_spec": specs.get("launch_spec") or {},
        "skill_entries": skill_entries,
        "repo_meta": {
            "description": str(meta.get("description") or ""),
            "default_branch": default_branch,
            "stars": _int_or_none(meta.get("stargazers_count")),
            "homepage": str(meta.get("homepage") or ""),
            "topics": [str(t) for t in (meta.get("topics") or [])][:12],
        },
        "summary": summary,
        "type_error": type_error,
        "error": type_error or "",
    }


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ── Manifest builders (shape resolve_reference_specs reads) ─────────────────

def _market_id(full_name: str, sub: str) -> str:
    base = f"github:{full_name}"
    return f"{base}:{sub.strip('/')}" if sub.strip("/") else base


def build_skill_manifest(
    full_name: str,
    ref: str,
    entry: dict,
    *,
    version: str = "",
    name: str = "",
    display_name: str = "",
    description: str = "",
) -> dict:
    """Skill manifest → resolve_reference_specs skill branch → node ``git clone``.

    ``install_method=github_clone`` (top-level) makes ``should_skip_mirror`` True,
    so the node gets ``{source:"github", url, path, ref}`` and clones at setup
    time. ``path`` is the entry's parent dir (probe's ``entry.path``); the node
    locates the skill there. Server holds zero bytes. ``name``/``display_name``/
    ``description`` override the probe defaults with user-edited values.
    """
    entry = entry or {}
    path = str(entry.get("path") or "").strip().strip("/")
    fallback_name = str(entry.get("name") or full_name.split("/")[-1])
    name = (name or fallback_name).strip()
    clone_url = f"https://github.com/{full_name}.git"
    return {
        "id": _market_id(full_name, path),
        "name": name,
        "display_name": (display_name or name).strip(),
        "description": description,
        "version": version or ref,
        "install_method": "github_clone",
        "source": "github",
        "source_url": clone_url,
        "resource": {
            "source": "github",
            "url": clone_url,
            "path": path,
            "ref": ref,
        },
    }


def build_plugin_container_manifest(
    full_name: str,
    ref: str,
    entries: list[dict],
    *,
    plugin_spec: dict | None = None,
    version: str = "",
    name: str = "",
    display_name: str = "",
    description: str = "",
    pin_commit: str = "",
) -> dict:
    """插件容器（原 skills 技能集）manifest：plugin 引用带 entries 全量清单。

    技能集本质是多技能的插件包，类型归入 plugin（容器）。manifest 同时带两种
    安装坐标：``resource``（github_clone，技能勾选按 entries 逐子技能展开）+
    ``download_url``（归档 zip，插件勾选整包安装）——装配跟着编辑器走，两条通道
    都可用，由 resource_data 实际内容决定。``market_id`` 用仓库级（无子路径）。
    ``pin_commit`` 非空（head_sha）时 ``resource.ref`` 钉到该 commit。
    """
    plugin_spec = plugin_spec or {}
    clone_url = f"https://github.com/{full_name}.git"
    download_url = str(plugin_spec.get("download_url") or "")
    if not download_url:
        download_url = f"https://github.com/{full_name}/archive/refs/heads/{ref}.zip"
    name = (name or full_name).strip()
    use_ref = (pin_commit or ref).strip()
    clean_entries = []
    for entry in entries or []:
        clean = {
            "name": str(entry.get("name") or "").strip(),
            "path": str(entry.get("path") or "").strip().strip("/"),
            "entry": str(entry.get("entry") or "SKILL.md"),
        }
        # 子技能描述（SKILL.md frontmatter description）：探针已抓到，但这里必须
        # 显式带进来——否则容器展开的子技能卡片只能显示 path，读不出该技能
        # 到底是干什么的。
        sub_desc = str(entry.get("description") or "").strip()
        if sub_desc:
            clean["description"] = sub_desc
        if entry.get("editors"):
            clean["editors"] = [str(e) for e in entry["editors"]]
        if clean["name"] and clean["path"] not in ("", None):
            clean_entries.append(clean)
    return {
        "id": _market_id(full_name, ""),
        "name": name,
        "display_name": (display_name or name).strip(),
        "description": description,
        "type": "plugin",
        "version": version or use_ref,
        "install_method": "github_clone",
        "source": "github",
        "source_url": clone_url,
        "download_url": download_url,
        "entries": clean_entries,
        "resource": {
            "source": "github",
            "url": clone_url,
            "path": "",
            "ref": use_ref,
        },
    }


def build_skills_collection_manifest(
    full_name: str,
    ref: str,
    entries: list[dict],
    *,
    version: str = "",
    name: str = "",
    display_name: str = "",
    description: str = "",
    pin_commit: str = "",
) -> dict:
    """Legacy alias of :func:`build_plugin_container_manifest`（skills→plugin 容器）。

    技能集已归入 plugin 容器类型；保留旧名只是给既有调用方/测试零改动过渡。
    产出的 manifest ``type`` 是 ``plugin``（不再是 ``skills``）。
    """
    return build_plugin_container_manifest(
        full_name, ref, entries,
        version=version, name=name, display_name=display_name,
        description=description, pin_commit=pin_commit,
    )


def build_plugin_manifest(
    full_name: str,
    ref: str,
    plugin_spec: dict,
    *,
    version: str = "",
    name: str = "",
    display_name: str = "",
    description: str = "",
) -> dict:
    """Plugin manifest → resolve_reference_specs plugin branch → node fetches zip.

    resolve reads ``download_url`` + ``version``. If the probe filled
    ``install_spec.plugin`` (a ``.claude-plugin/marketplace.json`` repo) use it;
    otherwise synthesize the branch archive zip URL. Edited name/description
    override the probe-derived defaults.
    """
    plugin_spec = plugin_spec or {}
    download_url = str(plugin_spec.get("download_url") or "")
    if not download_url:
        download_url = f"https://github.com/{full_name}/archive/refs/heads/{ref}.zip"
    fallback_name = str(plugin_spec.get("provider") or full_name.split("/")[-1])
    name = (name or fallback_name).strip()
    return {
        "id": _market_id(full_name, ""),
        "name": name,
        "display_name": (display_name or name).strip(),
        "description": description,
        "version": version or ref,
        "download_url": download_url,
        "resource": {},
    }


def pick_skill_entry(entries: list[dict], entry_index: int | None) -> dict | None:
    """Select one skill entry by index (default 0). None if the list is empty."""
    if not entries:
        return None
    idx = entry_index if isinstance(entry_index, int) and 0 <= entry_index < len(entries) else 0
    return entries[idx]


def build_mcp_resource_data(launch_spec: dict | None) -> dict:
    """launch_spec → v2 ``resources.resource_data`` for an MCP reference.

    The probe's ``launch_spec`` carries the MCP launch config (kind=stdio →
    command/args/env; kind=remote → url/transport). It lands verbatim in the
    resource pool body; the team-private connection credentials (token/headers/
    env values) live on ``resource_references.params`` instead and are merged
    at resolve time (``_mcp_spec_from_params``).

    One normalization: ``launch_spec.env`` is a *variable-name list* (the probe
    iterates ``.mcp.json`` env dict keys), so it's expanded into ``{name: ""}``
    placeholders — real values are team-private and supplied via ``params.env``.
    """
    launch = launch_spec or {}
    kind = str(launch.get("kind") or "")
    if kind not in ("stdio", "remote"):
        return {}
    launch_env = launch.get("env") if isinstance(launch.get("env"), list) else []
    return {
        "kind": kind,
        "command": str(launch.get("command") or ""),
        "args": [str(a) for a in (launch.get("args") or []) if str(a).strip()],
        "env": {str(e).strip(): "" for e in launch_env if str(e).strip()},
        "url": str(launch.get("url") or ""),
        "transport": str(launch.get("transport") or ""),
    }
