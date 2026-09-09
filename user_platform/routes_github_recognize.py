"""``/api/v1/github/recognize`` — GitHub repo recognition (pure preview).

Called by the add-MCP/Skill/Plugin/项目提示词 dialogs when a user pastes a
GitHub repo URL. Probes the repo via :mod:`github_recognize` (which wraps the
existing ``leaderboard_probe``) and returns the recognized kinds + install
coords + HEAD sha, so the UI can show a preview and let the user pick
引用 (reference, no bytes) vs 安装 (server-side zip copy). Nothing is persisted
here — the 引用 create lives in :mod:`routes_resources`, the 安装 import in
:mod:`routes_skill`.
"""
from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException

from . import github_recognize
from .deps import get_current_user
from .marketplace import config as mp_config
from .marketplace import leaderboard_probe
from .models import User

router = APIRouter(prefix="/api/v1/github", tags=["github-recognize"])


@router.post("/recognize")
async def recognize_github_repo(
    body: dict = Body(...),
    _: User = Depends(get_current_user),
) -> dict:
    """Probe a GitHub repo and return a normalized preview (no persist).

    Body: ``{"repo": "owner/repo 或完整 URL", "ref": "可选分支/tag/commit"}``.
    """
    repo_input = str(body.get("repo") or body.get("url") or "").strip()
    if not repo_input:
        raise HTTPException(400, "请输入 GitHub 仓库地址")
    ref = str(body.get("ref") or "").strip()
    force_type = str(body.get("type") or body.get("kind") or "").strip().lower()
    result = await github_recognize.recognize_repo(repo_input, ref=ref, force_type=force_type)
    if result.get("error"):
        raise HTTPException(422, result["error"])
    return {"code": 0, "message": "ok", "data": result}


@router.post("/file")
async def fetch_github_file(
    body: dict = Body(...),
    _: User = Depends(get_current_user),
) -> dict:
    """Fetch one file's text from a GitHub repo (for the 项目提示词 识别→安装 flow).

    Body: ``{"repo": "owner/repo 或 URL", "ref": "分支/tag/commit", "path": "..."}``.
    Returns ``{"content": <utf-8 text>}``, 404 if the file is missing/oversized.
    Reuses the probe's Contents-API fetcher (same proxy + token + 256KB cap).
    """
    repo_input = str(body.get("repo") or "").strip()
    full_name, parsed_ref, body_path = github_recognize.parse_github_input(repo_input)
    if not full_name:
        raise HTTPException(400, "请输入 GitHub 仓库地址")
    ref = str(body.get("ref") or parsed_ref or "main").strip()
    path = str(body.get("path") or body_path or "").strip().strip("/")
    if not path:
        raise HTTPException(400, "请指定要取的文件路径")
    proxy_id = mp_config.settings.proxy_id or ""
    content = await leaderboard_probe.fetch_text(full_name, path, ref, proxy_id=proxy_id)
    if content is None:
        raise HTTPException(404, "文件不存在或过大（>256KB）")
    return {"code": 0, "message": "ok", "data": {"content": content}}
