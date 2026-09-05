"""agentscope 技能源同步（公开 API → skills 模块）。

agentscope 平台（https://platform.agentscope.io）的技能是公开 API：

- ``GET /api/v1/skills?page=N&page_size=20`` 分页拉列表（data.items[]，data.total）
- ``GET /api/v1/skills/{id}/download`` 下载 zip（application/zip）

无需鉴权，服务端可定时同步。每个技能的 zip 含 SKILL.md（frontmatter name/description）
+ 可执行脚本；``category_label`` 是分类，``tags`` 是标签。

同步流程：分页拉全量 → 逐个下载 zip → 转 skill manifest 落库。与 agency-agents 同构：
拉源→转换→直接落库，无中间表。
"""
from __future__ import annotations

import asyncio
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

import aiohttp

from . import config as mp_config
from .validator import validate_manifest

BASE = "https://platform.agentscope.io"
PAGE_SIZE = 20
MAX_PAGES = 100  # 硬上限：2000 个技能足够
MODULE = "skills"
DEFAULT_VERSION = "1.0.0"

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=120)
_last_result: dict[str, Any] = {"ran_at": "", "ok": None, "detail": "从未同步", "logs": []}
# 实时进度：同步进行中由前端轮询 last-sync 端点读取。
_progress: dict[str, Any] = {"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""}


def last_result() -> dict[str, Any]:
    return {**_last_result, "progress": dict(_progress)}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _gh_get_json(url: str) -> Any:
    """经 proxy_manager GET JSON（与 leaderboard_probe 同链路）。"""
    from providers.proxy_manager import get_proxy_manager

    manager = get_proxy_manager()
    resp = await manager.request(
        url=url, method="GET", headers={"Accept": "application/json"},
        timeout=_FETCH_TIMEOUT, proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status >= 400:
        raise RuntimeError(f"HTTP {resp.status}")
    return await resp.json()


async def _gh_get_bytes(url: str) -> bytes:
    """经 proxy_manager GET 二进制（zip 下载）。"""
    from providers.proxy_manager import get_proxy_manager

    manager = get_proxy_manager()
    resp = await manager.request(
        url=url, method="GET", headers={"Accept": "application/zip, application/octet-stream"},
        timeout=_FETCH_TIMEOUT, proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status >= 400:
        raise RuntimeError(f"HTTP {resp.status}")
    return await resp.read()


async def _fetch_all_skills() -> list[dict]:
    """分页拉全量技能列表。"""
    all_items: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        data = await _gh_get_json(f"{BASE}/api/v1/skills?page={page}&page_size={PAGE_SIZE}")
        items = (data or {}).get("data", {}).get("items") or []
        if not items:
            break
        all_items.extend(items)
        total = (data or {}).get("data", {}).get("total") or 0
        if len(all_items) >= total:
            break
        await asyncio.sleep(0.3)  # 限速
    return all_items


def _parse_skill_zip(data: bytes) -> tuple[str, str, str]:
    """从 zip 里提取 SKILL.md 的 frontmatter name + 正文。返回 (name, description, content)。"""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        candidates = sorted(
            (name for name in archive.namelist() if PurePosixPath(name).name == "SKILL.md" and not name.endswith("/")),
            key=lambda name: (len(PurePosixPath(name).parts), len(name)),
        )
        if not candidates:
            raise ValueError("zip 中未找到 SKILL.md")
        content = archive.read(candidates[0]).decode("utf-8", "replace")
    # 极简 frontmatter 解析
    name = ""
    description = ""
    lines = content.replace("\r\n", "\n").split("\n")
    if lines and lines[0].strip() == "---":
        for line in lines[1:]:
            if line.strip() == "---":
                break
            if line.startswith((" ", "\t", "-")):
                continue
            if ":" in line:
                key, _, value = line.partition(":")
                value = value.strip()
                if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                if key.strip() == "name":
                    name = value
                elif key.strip() == "description":
                    description = value
    return name, description, content


async def sync_agentscope() -> dict:
    """同步 agentscope 技能源：分页拉列表 → 逐个下载 zip → 转候选池条目 → 落库。

    统一源模型：落 ``marketplace_leaderboard_items``，source='agentscope'，与
    Agent-Leaderboard / agency-agents 同一条草稿→发布流水线，来源筛选即 source 列。
    ON CONFLICT 只刷 external_data，重同步不冲掉管理员改过的资源字段。
    """
    global _last_result, _progress
    ran_at = _now_iso()
    logs: list[dict[str, str]] = []

    def _log(phase: str, detail: str) -> None:
        logs.append({"ts": _now_iso(), "phase": phase, "detail": detail})

    # 进入同步态：实时进度可见
    _progress.update({"running": True, "phase": "init", "current": 0, "total": 0, "current_item": ""})

    try:
        _log("fetch_list", f"分页拉取 {BASE}/api/v1/skills")
        _progress.update({"phase": "fetch_list", "current_item": ""})
        items = await _fetch_all_skills()
        _log("fetch_list_ok", f"拉到 {len(items)} 个技能")
        _progress.update({"phase": "download", "total": len(items)})
        converted = 0
        failed: list[dict] = []
        import marketplace_leaderboard_store as lb_store

        _log("download_loop", f"逐个下载 zip + 解析 SKILL.md + 写候选池")
        for idx, it in enumerate(items, start=1):
            skill_id = str(it.get("id") or "")
            code = str(it.get("skill_code") or skill_id)
            _progress.update({"current": idx, "current_item": code})
            try:
                _log("download", f"[{idx}/{len(items)}] {code}：下载 zip")
                zip_data = await _gh_get_bytes(f"{BASE}/api/v1/skills/{skill_id}/download")
                name, description, content = _parse_skill_zip(zip_data)
                category = str(it.get("category_label") or "")
                tags = [str(t) for t in (it.get("tags") or []) if str(t).strip()]
                summary = description or str(it.get("description") or "")
                _log("upsert", f"[{idx}/{len(items)}] {code}：写入候选池（分类={category or '未分类'}）")
                await lb_store.upsert_item({
                    "source": "agentscope",
                    "board": "skills",
                    "repo_full_name": f"agentscope/{code}",
                    "repo_url": f"{BASE}/skills/{skill_id}",
                    "description": summary,
                    "stars": 0,
                    "forks": 0,
                    "language": "",
                    "topics": tags,
                    "upstream_category": category,
                    "use_cases": [],
                    "name": name or code,
                    "display_name": name or code,
                    "publisher": "agentscope",
                    "version": DEFAULT_VERSION,
                    "categories": [category] if category else [],
                    "tags": tags,
                    "target_module": "skill",
                    "target_modules": ["skill"],
                    "install_spec": {
                        "skill": {
                            "install_method": "github_clone",
                            "ref": "main",
                            "entries": [{"name": name or code, "path": "", "entry": "SKILL.md", "editors": ["claude"]}],
                        },
                    },
                    "external_data": {
                        "source": "agentscope",
                        "board": "skills",
                        "repo_full_name": f"agentscope/{code}",
                        "repo_url": f"{BASE}/skills/{skill_id}",
                        "description": summary,
                        "raw": it,
                    },
                    "installable": True,
                    "labels": [],
                })
                converted += 1
            except Exception as exc:  # noqa: BLE001
                failed.append({"id": skill_id, "code": code, "errors": [str(exc)[:200]]})
                _log("fail", f"[{idx}/{len(items)}] {code}：{str(exc)[:150]}")
            await asyncio.sleep(0.5)  # 限速
        detail = f"同步 {converted} 条，失败 {len(failed)} 条"
        _log("done", detail)
        _last_result.update({"ran_at": ran_at, "ok": True, "detail": detail, "logs": logs})
        return {"converted": converted, "failed": failed[:20], "ran_at": ran_at, "detail": detail, "logs": logs}
    except Exception as exc:  # noqa: BLE001
        _log("error", str(exc)[:200])
        _last_result.update({"ran_at": ran_at, "ok": False, "detail": str(exc)[:200], "logs": logs})
        raise
    finally:
        _progress.update({"running": False, "phase": "", "current": 0, "total": 0, "current_item": ""})
