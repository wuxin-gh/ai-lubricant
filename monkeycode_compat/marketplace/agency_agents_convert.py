"""agency-agents 提示词源 → 市场 prompts 模块的确定性同步。

源：https://github.com/msitarzewski/agency-agents —— 270+ 个角色扮演型 agent
提示词合集，每个 ``<division>/<agent>.md`` 都是 YAML frontmatter（name /
description）加一段 markdown 正文，**正文本身就是完整提示词**。仓库自带
``divisions.json``（division 集合的唯一真相源）——这就是它能确定性转换、
不需要 agent 猜的原因：frontmatter + 目录约定就是它的"结构化清单"。

转换核心与 ``script/convert_agency_agents.py`` 同源（脚本已改成薄 CLI 调本模块，
两处各写必然漂移）。服务端同步比脚本多的两件事：

- tarball 拉取走 proxy_manager（codeload 主路 / api tarball 备路，与
  ``leaderboard_sync`` 同一条出网链路），出网受限部署也能同步；
- ``sync_agency_agents`` 直连市场导入链路（``normalize_import_body`` 同构的
  导入包 → ``routes.import_data``，merge 模式），273 条提示词直接进 prompts 池，
  不再需要人工下载 import-prompts.json 手动导入。

字段映射（与脚本一致，勿改口径）：

- frontmatter ``name`` → ``display_name``；``description`` → ``summary``。
- ``color`` / ``emoji`` / ``vibe`` / ``services`` 是展示层元数据，转换即丢弃。
- 正文逐字保留；``id = agency-agents.<division>.<文件名去 .md>``；
  ``publisher = agency-agents``；``category`` = divisions.json 的 label。
- 只导 divisions.json 声明的 division 目录（integrations/ strategy/ 等非 agent
  源一律跳过）。
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import time
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from . import config as mp_config
from .validator import MARKET_EXPORT_SCHEMA, validate_manifest

SOURCE_OWNER = "msitarzewski"
SOURCE_REPO = "agency-agents"
# 用户指定的基线 commit；换基线走 sync_agency_agents(ref=...)。
DEFAULT_REF = "3c9588880b7cafaec325a104899fd8bbe27e7d72"
MODULE = "prompts"
DEFAULT_VERSION = "1.0.0"
DEFAULT_PROVIDERS = ["claude", "codex", "opencode", "cursor"]

# CONTRIBUTING.md：divisions.json 是 division 集合的唯一真相源。非 division 的
# 顶层目录（convert 产物 / playbook / 示例 / CI 脚本）不是 agent 源，全部跳过。
NON_DIVISION_DIRS = frozenset({".github", "examples", "integrations", "scripts", "strategy"})

# 缓存目录：commit sha 内容不可变，命中即永久复用；分支 ref 一小时 TTL。
CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "agency-agents-src"
TTL_SECONDS = 3600

UserAgent = "ai-lubricant-agency-convert/1.0"
_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=120)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
# frontmatter 只取顶层单行 key: value。缩进行 / 列表项（services 的嵌套结构）
# 属于被丢弃的展示层元数据，不解析。
_FRONT_KEY_RE = re.compile(r"^([A-Za-z0-9_-]+):[ \t]*(.*)$")
_SLUG_RE = re.compile(r"[^a-z0-9._-]+")

_last_result: dict[str, Any] = {"ran_at": "", "ok": None, "detail": "从未同步", "logs": []}
_last_result_zh: dict[str, Any] = {"ran_at": "", "ok": None, "detail": "从未同步", "logs": []}
# 实时进度：同步进行中由前端轮询 last-sync 端点读取。
_progress: dict[str, Any] = {"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""}


def last_result() -> dict[str, Any]:
    """上次同步结果（英文源）+ 实时进度。"""
    return {**_last_result, "progress": dict(_progress)}


def last_result_zh() -> dict[str, Any]:
    """上次同步结果（中文源）+ 实时进度。"""
    return {**_last_result_zh, "progress": dict(_progress)}


# ── 源获取：tarball 一次拿全仓，逐文件 raw 要 270+ 个请求 ──────────────────

def _is_agent_path(rel: str) -> bool:
    """division 目录下的非 README ``.md``。

    多数 agent 直接放在 division 根下，但 ``game-development/`` 按引擎再分了一层
    （``unity/`` ``godot/`` ``unreal-engine/`` …）。所以这里只认「首段是 division、
    末段不是 README」，不限层数——限死一层会静默漏掉那 15 个引擎 agent。
    """
    if not rel.endswith(".md") or ".." in rel:
        return False
    parts = rel.split("/")
    if len(parts) < 2 or parts[0] in NON_DIVISION_DIRS or parts[0].startswith("."):
        return False
    return parts[-1].lower() != "readme.md"


async def _fetch_tarball(ref: str, repo: str = SOURCE_REPO) -> bytes:
    """整仓 tarball，经 proxy_manager（codeload 主路，api.github 备路；各重试 3 次）。

    ``repo`` 默认英文源；中文源传 ``jnMetaCode/agency-agents-zh``。
    """
    import asyncio

    from providers.proxy_manager import get_proxy_manager

    urls = [
        f"https://codeload.github.com/{SOURCE_OWNER}/{repo}/tar.gz/{ref}",
        f"https://api.github.com/repos/{SOURCE_OWNER}/{repo}/tar/{ref}",
    ]
    manager = get_proxy_manager()
    last_err: Exception | None = None
    for url in urls:
        for attempt in range(3):
            try:
                resp = await manager.request(
                    url=url, method="GET", headers={"User-Agent": UserAgent},
                    timeout=_FETCH_TIMEOUT, proxy_config_id=mp_config.settings.proxy_id or None,
                )
                if resp.status == 404:
                    # ref 不存在，重试没有意义，换下一条 URL。
                    last_err = RuntimeError(f"HTTP 404: {url}")
                    break
                if resp.status >= 400:
                    last_err = RuntimeError(f"HTTP {resp.status}: {url}")
                else:
                    return await resp.read()
            except Exception as exc:  # noqa: BLE001 — 网络抖动重试
                last_err = exc
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"拉取 {SOURCE_OWNER}/{SOURCE_REPO}@{ref} 失败: {last_err}")


def _extract_source(data: bytes) -> dict[str, str]:
    """从 tarball 里挑出 divisions.json + 全部 agent .md（路径→文本）。

    不用 extractall（tar 路径不可信），逐 member 读取后按白名单过滤；
    tarball 根目录是 ``<repo>-<ref>/``，剥掉首段。
    """
    files: dict[str, str] = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            _, _, rel = member.name.partition("/")
            if not rel or ".." in rel:
                continue
            if rel != "divisions.json" and not _is_agent_path(rel):
                continue
            handle = tar.extractfile(member)
            if handle is None:
                continue
            files[rel] = handle.read().decode("utf-8", "replace")
    if "divisions.json" not in files:
        raise RuntimeError("tarball 里没有 divisions.json")
    return files


def _cache_file(ref: str, repo: str = SOURCE_REPO) -> Path:
    safe = _SLUG_RE.sub("-", ref.lower()).strip("-") or "ref"
    repo_slug = _SLUG_RE.sub("-", repo.lower()).strip("-") or "repo"
    return CACHE_DIR / f"{repo_slug}-{safe}.json"


async def fetch_source(ref: str, flush: bool, *, repo: str = SOURCE_REPO) -> dict[str, str]:
    """带缓存的源读取（网络走 proxy_manager）。sha ref 永久复用；分支 ref 一小时内复用。

    ``repo`` 默认英文源；中文源传 ``jnMetaCode/agency-agents-zh``，缓存按 repo 分文件。
    """
    import asyncio

    cache = _cache_file(ref, repo)
    if not flush and cache.exists():
        try:
            cached = json.loads(cache.read_text(encoding="utf-8"))
            age = time.time() - float(cached.get("_fetched_at") or 0)
            if _SHA_RE.match(ref) or 0 <= age < TTL_SECONDS:
                return cached["files"]
        except (ValueError, KeyError, OSError):
            pass  # 缓存坏了就当没有，重新拉
    data = await _fetch_tarball(ref, repo=repo)
    files = _extract_source(data)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(
        lambda: cache.write_text(
            json.dumps({"_fetched_at": time.time(), "ref": ref, "repo": repo, "files": files}, ensure_ascii=False),
            encoding="utf-8",
        )
    )
    return files


def load_from_dir(directory: Path) -> dict[str, str]:
    """离线模式：直接读本地 clone / 解开的 tarball 目录（脚本 --from-dir 用）。"""
    files: dict[str, str] = {}
    divisions_path = directory / "divisions.json"
    if divisions_path.exists():
        files["divisions.json"] = divisions_path.read_text(encoding="utf-8", errors="replace")
    for path in sorted(directory.rglob("*.md")):
        rel = path.relative_to(directory).as_posix()
        if _is_agent_path(rel):
            files[rel] = path.read_text(encoding="utf-8", errors="replace")
    return files


# ── frontmatter / manifest 转换 ────────────────────────────────────────────

def parse_frontmatter(text: str) -> tuple[dict, str] | None:
    """极简 frontmatter 解析：只取顶层 ``key: value`` 单行。

    返回 (meta, 正文)。没有 ``---`` 开头或 name/description 缺失返回 None
    （strategy/ 下的 playbook 就长这样——没有 frontmatter 本来也不该被当 agent）。
    """
    lines = text.replace("\r\n", "\n").split("\n")
    if not lines or lines[0].strip() != "---":
        return None
    meta: dict[str, str] = {}
    end = -1
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end = i
            break
        if line.startswith((" ", "\t", "-")):
            continue  # 嵌套值（services 列表）是丢弃项
        m = _FRONT_KEY_RE.match(line)
        if m:
            value = m.group(2).strip()
            if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            meta[m.group(1)] = value
    if end < 0:
        return None
    if not meta.get("name", "").strip() or not meta.get("description", "").strip():
        return None
    return meta, "\n".join(lines[end + 1 :])


def slugify(value: str) -> str:
    """收敛成 validator 认的安全字符（字母数字点下划线连字符）。"""
    normalized = _SLUG_RE.sub("-", value.lower()).strip("-")
    return normalized or hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def build_manifest(
    *,
    division: str,
    division_label: str,
    rel_path: str,
    meta: dict,
    body: str,
    ref: str,
    version: str,
    providers: list[str],
    tags: list[str],
    repo: str = SOURCE_REPO,
) -> dict:
    """agent .md → prompts manifest。字段映射见模块 docstring。"""
    content = body.replace("\r\n", "\n").strip()
    # id 用整条相对路径（去 .md）而不是只用文件名：game-development 下按引擎再分了
    # 一层，只取文件名会让 unity/godot 的同名 agent 撞成一个 id。
    segments = [slugify(part) for part in rel_path[: -len(".md")].split("/")]
    base = slugify(Path(rel_path).stem)
    display_name = meta.get("name", "").strip()
    # publisher 区分英文/中文源，便于管理端按来源筛选。
    publisher = "agency-agents-zh" if repo != SOURCE_REPO else "agency-agents"
    return {
        "id": publisher + "." + ".".join(segments),
        "kind": "project_prompt",
        "name": base,
        "display_name": display_name or base,
        "summary": meta.get("description", "").strip(),
        "publisher": publisher,
        "category": division_label or division,
        "tags": list(tags),
        "version": version,
        "status": "published",
        "source_url": f"https://github.com/{SOURCE_OWNER}/{repo}/blob/{ref}/{rel_path}",
        # digest 是内容变更检测依据：换 ref 重跑后能看出哪些 agent 正文变了。
        "digest": "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "resource": {
            "type": "project_prompt",
            "content": content,
            "providers": list(providers),
        },
    }


def convert(
    files: dict[str, str],
    *,
    ref: str,
    divisions_filter: set[str] | None,
    version: str,
    providers: list[str],
    repo: str = SOURCE_REPO,
) -> dict[str, dict]:
    """转换全部 agent 文件。返回 report dict（不走异常通道，逐文件记录失败）。"""
    divisions: dict[str, dict] = {}
    try:
        divisions = json.loads(files.get("divisions.json") or "{}").get("divisions") or {}
    except ValueError:
        pass

    manifest_by_id: dict[str, dict] = {}
    failed: list[dict] = []
    skipped: list[dict] = []
    per_division: dict[str, int] = {}

    for rel in sorted(files):
        if rel == "divisions.json" or not _is_agent_path(rel):
            continue
        division = rel.split("/", 1)[0]
        if divisions_filter and division not in divisions_filter:
            continue

        meta_body = parse_frontmatter(files[rel])
        if meta_body is None:
            skipped.append({"path": rel, "reason": "无 frontmatter 或缺 name/description"})
            continue
        meta, body = meta_body
        label = str((divisions.get(division) or {}).get("label") or division)
        # game-development 下的引擎子目录（unity / godot / …）是真实分类信息，
        # 进 tags 供管理端筛选；division 根下的 agent 没有这一段。
        subdir = rel.split("/")[1] if rel.count("/") > 1 else ""
        tags = [t for t in dict.fromkeys([division, label, subdir, "agency-agents"]) if t]

        manifest = build_manifest(
            division=division,
            division_label=label,
            rel_path=rel,
            meta=meta,
            body=body,
            ref=ref,
            version=version,
            providers=providers,
            tags=tags,
        )
        # id 去重：同名文件 slug 撞车时加序号，保证导入 merge 的 upsert 键稳定。
        item_id = manifest["id"]
        suffix = 2
        while item_id in manifest_by_id:
            item_id = f"{manifest['id']}-{suffix}"
            suffix += 1
        manifest["id"] = item_id

        errors = validate_manifest(MODULE, manifest)
        if errors:
            failed.append({"path": rel, "id": item_id, "errors": errors})
            continue
        manifest_by_id[item_id] = manifest
        per_division[division] = per_division.get(division, 0) + 1

    return {
        "divisions": per_division,
        "converted": len(manifest_by_id),
        "failed": failed,
        "skipped": skipped,
        "manifests": manifest_by_id,
    }


def build_import_package(manifest_by_id: dict[str, dict]) -> dict:
    """与 /admin/export 的导出包同构（normalize_import_body 第一分支直接认）。"""
    return {
        "schema": MARKET_EXPORT_SCHEMA,
        "exported_modules": [MODULE],
        "modules": {MODULE: {"manifests": manifest_by_id}},
    }


def write_outputs(out_dir: Path, manifest_by_id: dict[str, dict]) -> dict[str, Path]:
    """写导入包 + 单条 manifest 文件（脚本 --out 用，便于抽查 / 选择性导入）。"""
    manifests_dir = out_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    for item_id, manifest in manifest_by_id.items():
        (manifests_dir / f"{item_id}.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    package_path = out_dir / "import-prompts.json"
    package_path.write_text(
        json.dumps(build_import_package(manifest_by_id), ensure_ascii=False), encoding="utf-8"
    )
    return {"package": package_path, "manifests_dir": manifests_dir}


def _manifest_to_leaderboard_item(manifest: dict, source_name: str) -> dict:
    """prompts manifest → 候选池条目（marketplace_leaderboard_items）。

    统一源模型：每个配置一个 source，全部落候选池表走同一条草稿→发布流水线。
    字段映射：board 固定 prompts（形态榜）；division/label → categories；repo_full_name
    取 manifest id 去掉 publisher 前缀的路径段（UNIQUE(source, board, repo_full_name)
    的一部分，保证逐条唯一）；正文进 install_spec.prompt.content。
    """
    publisher = "agency-agents-zh" if source_name == "agency-agents-zh" else "agency-agents"
    prefix = publisher + "."
    item_id = str(manifest.get("id") or "")
    rel = item_id[len(prefix):] if item_id.startswith(prefix) else item_id
    repo_full_name = rel.replace(".", "/") or item_id
    resource = manifest.get("resource") or {}
    return {
        "source": source_name,
        "board": "prompts",
        "repo_full_name": repo_full_name,
        "repo_url": str(manifest.get("source_url") or ""),
        "description": str(manifest.get("summary") or ""),
        "stars": 0,
        "forks": 0,
        "language": "",
        "topics": list(manifest.get("tags") or []),
        "upstream_category": str(manifest.get("category") or ""),
        "use_cases": [],
        "name": str(manifest.get("name") or ""),
        "display_name": str(manifest.get("display_name") or manifest.get("name") or ""),
        "publisher": publisher,
        "version": str(manifest.get("version") or "1.0.0"),
        "categories": [str(manifest.get("category") or "")],
        "tags": list(manifest.get("tags") or []),
        "target_module": "prompt",
        "target_modules": ["prompt"],
        "install_spec": {
            "prompt": {
                "content": str(resource.get("content") or ""),
                "providers": list(resource.get("providers") or DEFAULT_PROVIDERS),
            },
        },
        "external_data": {
            "source": source_name,
            "board": "prompts",
            "repo_full_name": repo_full_name,
            "repo_url": str(manifest.get("source_url") or ""),
            "description": str(manifest.get("summary") or ""),
            "raw": manifest,
        },
        "installable": True,
        "labels": [],
    }


async def sync_agency_agents(
    *, ref: str = DEFAULT_REF, flush: bool = False, dry_run: bool = False,
    repo: str = SOURCE_REPO,
) -> dict[str, Any]:
    """服务端同步：拉源 → 转换 → merge 进市场 prompts 模块。返回 report。

    ``repo`` 默认英文源（agency-agents）；中文源传 ``agency-agents-zh``。
    dry_run=True 只转换+校验不落库（管理端先看报告再真同步）。结果存模块级
    ``_last_result`` 供回显；异常向上抛（路由层转 502），但失败也记进 _last_result。
    """
    global _last_result, _last_result_zh, _progress
    started = datetime.now(timezone.utc).isoformat()
    result_store = _last_result_zh if repo != SOURCE_REPO else _last_result
    logs: list[dict[str, str]] = []

    def _log(phase: str, detail: str) -> None:
        logs.append({"ts": datetime.now(timezone.utc).isoformat(), "phase": phase, "detail": detail})

    # 进入同步态：实时进度可见
    _progress.update({"running": True, "phase": "fetch", "current": 0, "total": 0, "current_item": ""})

    try:
        _log("fetch", f"拉取 tarball {SOURCE_OWNER}/{repo}@{ref}")
        files = await fetch_source(ref, flush, repo=repo)
        _log("fetch_ok", f"拿到 {len(files)} 个文件（含 divisions.json + agent .md）")
        _log("convert", "开始转换 frontmatter → manifest")
        _progress.update({"phase": "convert"})
        report = convert(
            files, ref=ref, divisions_filter=None,
            version=DEFAULT_VERSION, providers=list(DEFAULT_PROVIDERS),
            repo=repo,
        )
        manifest_by_id = report.pop("manifests")
        _log("convert_ok", f"转换 {report['converted']} 条，校验失败 {len(report['failed'])}，跳过 {len(report['skipped'])}")
        if dry_run:
            detail = f"dry-run：转换 {report['converted']} 条（未落库）"
            result = {**report, "ref": ref, "dry_run": True, "ran_at": started, "detail": detail}
            result_store.update({"ran_at": started, "ok": not report["failed"], "detail": detail, "logs": logs})
            return result
        import marketplace_leaderboard_store as lb_store

        source_name = "agency-agents-zh" if repo != SOURCE_REPO else "agency-agents"
        written = 0
        import_failed: list[dict] = []
        _log("upsert", f"逐条写入候选池（source={source_name}，board=prompts）")
        _progress.update({"phase": "upsert", "total": len(manifest_by_id)})
        for idx, (item_id, manifest) in enumerate(manifest_by_id.items(), start=1):
            _progress.update({"current": idx, "current_item": item_id})
            try:
                await lb_store.upsert_item(_manifest_to_leaderboard_item(manifest, source_name))
                written += 1
            except Exception as exc:  # noqa: BLE001 — 单条失败不影响其余
                import_failed.append({"id": item_id, "errors": [str(exc)[:200]]})
                _log("upsert_fail", f"{item_id}: {str(exc)[:150]}")
        detail = (
            f"同步 {report['converted']} 条，入库 {written} 条"
            + (f"；{len(import_failed)} 条失败" if import_failed else "")
        )
        _log("done", detail)
        result = {
            **report,
            "ref": ref,
            "dry_run": False,
            "ran_at": started,
            "written": written,
            "import_failed": import_failed,
            "detail": detail,
            "logs": logs,
        }
        result_store.update({"ran_at": started, "ok": not report["failed"], "detail": detail, "logs": logs})
        return result
    except Exception as exc:  # noqa: BLE001 — 失败也留痕，供回显
        _log("error", str(exc)[:200])
        result_store.update({"ran_at": started, "ok": False, "detail": f"同步失败：{exc}"[:300], "logs": logs})
        raise
    finally:
        _progress.update({"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""})
