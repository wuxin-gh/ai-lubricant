"""节点版本上传的内存态 job 记录。

上传分两阶段：① 前端把文件 POST 到服务器（暂存临时文件）；② 服务端后台把临时文件
逐个写入市场仓库（发行资产入仓，git blob）。本模块记录 job 进度供前端轮询，并用临时
文件托管文件数据（18 个文件最大约 95MB，放内存有风险）。

写入不支持字节级续传，只能按文件续传：已 done 的文件跳过，failed 的文件整个重写。
job 不落库（用户明确说内存即可），进程重启即丢失——上传中途重启服务端要重新上传，
这是可接受的代价。
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import time
import uuid
from typing import Any

# job 内存表：job_id -> JobState
_jobs: dict[str, dict[str, Any]] = {}
# 每个后台转传任务，用于重试时避免重复起
_tasks: dict[str, asyncio.Task] = {}

# 临时文件 30 分钟后连同 job 一起清，避免无限增长。
_TTL_SECONDS = 30 * 60


def _now() -> float:
    return time.time()


def _file_entry(m: dict) -> dict[str, Any]:
    """把一份 files_meta 归一成 job 内部的文件条目。

    ``source`` / ``download_url`` 仅供移动端 URL 模式（外链 APK 不入仓）透传，
    文件模式恒为空串，不影响既有路径。
    """
    return {
        "filename": m["filename"],
        "role": m.get("role", ""),
        "platform": m.get("platform", ""),
        "arch": m.get("arch", ""),
        "format": m.get("format", ""),
        "size": int(m.get("size", 0)),
        "state": "pending",   # pending / uploading / done / failed
        "error": "",
        "tmp_path": m.get("tmp_path", ""),
        "content_type": m.get("content_type", "application/octet-stream"),
        "source": m.get("source", ""),
        "download_url": m.get("download_url", ""),
    }


def create_job(version: str, files_meta: list[dict]) -> str:
    """开一个上传 job。``files_meta`` 每项含 filename/role/platform/arch/size。

    返回 job_id。调用方随后把每个文件写到 ``tmp_path_for(job_id, filename)``。
    """
    job_id = uuid.uuid4().hex
    _jobs[job_id] = {
        "job_id": job_id,
        "version": version,
        "status": "uploading",      # uploading -> to_github -> finalizing -> done/failed
        "phase": "to_server",
        "total_files": len(files_meta),
        "files": [_file_entry(m) for m in files_meta],
        "received_bytes": 0,
        "github_done": 0,
        "error": "",
        "created_at": _now(),
    }
    return job_id


def get_job(job_id: str) -> dict[str, Any] | None:
    return _jobs.get(job_id)


def set_files(job_id: str, files_meta: list[dict]) -> None:
    """回填第①阶段收齐的文件元信息，并把 ``total_files`` 与 ``files`` 同步。

    ``create_job`` 先用空 meta 占位（要先有 job_id 才能开临时目录写文件），收齐后
    用本函数补真实 files。``total_files`` 必须在这里一并改——它是前端进度条的分母，
    若漏改会出现 ``github_done / 0`` 这种分子有值、分母恒 0 的坏显示。
    """
    job = _jobs.get(job_id)
    if job is None:
        return
    job["files"] = [_file_entry(m) for m in files_meta]
    job["total_files"] = len(job["files"])


def public_state(job: dict[str, Any]) -> dict[str, Any]:
    """对外投影：去掉 tmp_path / content_type 这类内部字段。"""
    return {
        "job_id": job["job_id"],
        "version": job["version"],
        "status": job["status"],
        "phase": job["phase"],
        "total_files": job["total_files"],
        "github_done": job["github_done"],
        "error": job["error"],
        "files": [
            {
                "filename": f["filename"],
                "role": f["role"],
                "platform": f["platform"],
                "arch": f["arch"],
                "size": f["size"],
                "state": f["state"],
                "error": f["error"],
                # 前端据此区分「下载源校验中」（URL 模式）与「写入中」（文件模式）。
                # download_url 属内部字段，不外泄。
                "source": f.get("source") or "",
            }
            for f in job["files"]
        ],
    }


def set_received(job_id: str, received_bytes: int) -> None:
    job = _jobs.get(job_id)
    if job is not None:
        job["received_bytes"] = received_bytes


def mark_to_github(job_id: str) -> None:
    """第①阶段收完，切到第②阶段。"""
    job = _jobs.get(job_id)
    if job is not None:
        job["phase"] = "to_github"
        job["status"] = "to_github"


def set_file_state(job_id: str, filename: str, state: str, error: str = "") -> None:
    """更新单个文件的转传状态，并同步 github_done 计数。"""
    job = _jobs.get(job_id)
    if job is None:
        return
    for f in job["files"]:
        if f["filename"] == filename:
            f["state"] = state
            if error:
                f["error"] = error
            break
    job["github_done"] = sum(1 for f in job["files"] if f["state"] == "done")


def mark_failed(job_id: str, error: str) -> None:
    job = _jobs.get(job_id)
    if job is not None:
        job["status"] = "failed"
        job["error"] = error


def mark_done(job_id: str) -> None:
    job = _jobs.get(job_id)
    if job is not None:
        job["status"] = "done"
        job["phase"] = "done"


def reset_failed_for_retry(job_id: str) -> bool:
    """重试：把 failed 文件标回 pending，返回是否有可重试的文件。

    覆盖两种场景：
    - 常规：某些文件 GitHub 传失败 → 标回 pending，重新传
    - 特殊：所有文件都 done 但 store.upsert_item 失败（孤儿资产）→ 不需要重传文件，
      直接重跑 finalizing。此时没有 failed 文件，但 job.status=failed，标回 to_github
      让 _run_github_transfer 再跑一遍，已 done 的文件会被跳过。
    """
    job = _jobs.get(job_id)
    if job is None:
        return False
    any_reset = False
    for f in job["files"]:
        if f["state"] == "failed":
            f["state"] = "pending"
            f["error"] = ""
            any_reset = True
    # 所有文件 done 但 job 整体 failed（store 写入失败）：也允许重试
    if not any_reset and job.get("status") == "failed":
        any_reset = True
    if any_reset:
        job["status"] = "to_github"
        job["phase"] = "to_github"
        job["error"] = ""
    return any_reset


def get_task(job_id: str) -> asyncio.Task | None:
    return _tasks.get(job_id)


def set_task(job_id: str, task: asyncio.Task) -> None:
    _tasks[job_id] = task


def tmp_dir(job_id: str) -> str:
    """每个 job 一个临时目录，存放第①阶段收到的文件。"""
    return os.path.join(tempfile.gettempdir(), f"ai-lubricant-upload-{job_id}")


def tmp_path_for(job_id: str, filename: str) -> str:
    return os.path.join(tmp_dir(job_id), filename)


def cleanup_job(job_id: str) -> None:
    """删临时目录。job 留在表里（前端还能查到最终状态），由 TTL 清理统一回收。"""
    d = tmp_dir(job_id)
    try:
        if os.path.isdir(d):
            for _ in range(3):
                # Windows 上文件可能还被占用，重试几次。
                import shutil
                shutil.rmtree(d, ignore_errors=True)
                if not os.path.isdir(d):
                    break
    except Exception:
        pass


def sweep_expired() -> None:
    """清掉超时的 job 及其临时目录。由后台任务定期调。"""
    now = _now()
    expired = [jid for jid, j in _jobs.items() if now - j["created_at"] > _TTL_SECONDS]
    for jid in expired:
        cleanup_job(jid)
        _jobs.pop(jid, None)
        t = _tasks.pop(jid, None)
        if t is not None and not t.done():
            t.cancel()
