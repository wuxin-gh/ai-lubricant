"""渠道模板导入任务的进程内状态。

导入由服务端后台顺序写 GitHub，前端只拿 job_id 后每 2 秒轮询。本模块只保存状态，
不持久化任务：进程重启后记录消失，但已经完成的 GitHub commit 不会回滚。
"""
from __future__ import annotations

import asyncio
import copy
import time
import uuid
from typing import Any

_jobs: dict[str, dict[str, Any]] = {}
_tasks: dict[str, asyncio.Task] = {}
_active_job_id: str | None = None
_TTL_SECONDS = 30 * 60
_TERMINAL = {"done", "failed"}


def _now() -> float:
    return time.time()


def _sweep() -> None:
    global _active_job_id
    now = _now()
    for job_id, job in list(_jobs.items()):
        finished = float(job.get("finished_at") or 0)
        if job.get("status") in _TERMINAL and finished and now - finished > _TTL_SECONDS:
            _jobs.pop(job_id, None)
            task = _tasks.pop(job_id, None)
            if task is not None and not task.done():
                task.cancel()
            if _active_job_id == job_id:
                _active_job_id = None


def create_job(providers: list[str], *, overwrite: bool) -> tuple[str | None, str | None]:
    """创建唯一活跃任务；冲突时返回 ``(None, active_job_id)``。"""
    global _active_job_id
    _sweep()
    if _active_job_id:
        active = _jobs.get(_active_job_id)
        if active and active.get("status") not in _TERMINAL:
            return None, _active_job_id
        _active_job_id = None
    job_id = uuid.uuid4().hex
    now = _now()
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "phase": "queued",
        "overwrite": bool(overwrite),
        "total": len(providers),
        "completed": 0,
        "current_provider": "",
        "written": [],
        "skipped": [],
        "failed": [],
        "items": [
            {"provider": name, "state": "pending", "template_id": "", "error": ""}
            for name in providers
        ],
        "error": "",
        "warning": "",
        "created_at": now,
        "updated_at": now,
        "finished_at": 0.0,
    }
    _active_job_id = job_id
    return job_id, None


def get_job(job_id: str) -> dict[str, Any] | None:
    _sweep()
    return _jobs.get(job_id)


def public_state(job: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(job)


def set_task(job_id: str, task: asyncio.Task) -> None:
    _tasks[job_id] = task


def set_phase(job_id: str, status: str, *, current_provider: str = "") -> None:
    job = _jobs.get(job_id)
    if job is None:
        return
    job["status"] = status
    job["phase"] = status
    job["current_provider"] = current_provider
    job["updated_at"] = _now()


def set_item(job_id: str, provider: str, state: str, *, template_id: str = "", error: str = "") -> None:
    job = _jobs.get(job_id)
    if job is None:
        return
    for item in job["items"]:
        if item["provider"] == provider:
            item["state"] = state
            if template_id:
                item["template_id"] = template_id
            if error:
                item["error"] = error
            break
    job["current_provider"] = provider if state == "importing" else ""
    job["completed"] = sum(1 for item in job["items"] if item["state"] in {"written", "skipped", "failed"})
    job["written"] = [item["template_id"] for item in job["items"] if item["state"] == "written" and item["template_id"]]
    job["skipped"] = [item["provider"] for item in job["items"] if item["state"] == "skipped"]
    job["failed"] = [
        {"provider": item["provider"], "errors": [item["error"] or "未知错误"]}
        for item in job["items"] if item["state"] == "failed"
    ]
    job["updated_at"] = _now()


def mark_done(job_id: str, *, warning: str = "") -> None:
    global _active_job_id
    job = _jobs.get(job_id)
    if job is None:
        return
    job["status"] = "done"
    job["phase"] = "done"
    job["current_provider"] = ""
    job["warning"] = warning
    job["updated_at"] = _now()
    job["finished_at"] = job["updated_at"]
    if _active_job_id == job_id:
        _active_job_id = None


def mark_failed(job_id: str, error: str) -> None:
    global _active_job_id
    job = _jobs.get(job_id)
    if job is None:
        return
    job["status"] = "failed"
    job["phase"] = "failed"
    job["current_provider"] = ""
    job["error"] = error
    job["updated_at"] = _now()
    job["finished_at"] = job["updated_at"]
    if _active_job_id == job_id:
        _active_job_id = None
