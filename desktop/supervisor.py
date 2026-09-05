"""Child-process supervisor for the desktop build.

Starts the three server processes in a strict order so that concurrent DDL on
``mc_tunnel_*`` (main + tunnel_server both reconcile it, no advisory lock) can
never interleave:

    1. main service (uvicorn, desktop.serve)  -> wait until HTTP-ready
    2. node_server  (Hypercorn h2c)           -> wait until /health ready
    3. tunnel_server (Hypercorn h2c)

All children inherit ``os.environ`` (already populated by env_bootstrap), so PG
/ Redis connection info and the pre-generated NODE_CONTROL_TOKEN /
NODE_CREDENTIAL_ENCRYPTION_KEY reach every process identically — node_server
thus finds its keys already set and skips the .env write-back path.

On Windows, all children are placed in a Job Object with
KILL_ON_JOB_CLOSE so that if this launcher dies unexpectedly, the children —
and any grandchildren tunnel_server spawns (frpc / cloudflared / npc) — are
reaped by the OS rather than orphaned.
"""
from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import time
import urllib.request

from desktop import paths

MAIN_PORT = int(os.environ.get("DESKTOP_MAIN_PORT", "8001"))
NODE_PORT = int(os.environ.get("NODE_CONTROL_PORT", "8003"))
TUNNEL_PORT = int(os.environ.get("TUNNEL_RUNTIME_PORT", "8004"))


def _http_ready(url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status < 500
    except Exception:
        return False


def _wait_ready(url: str, label: str, deadline_s: float = 60.0) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < deadline_s:
        if _http_ready(url):
            return True
        time.sleep(0.5)
    return False


class Supervisor:
    def __init__(self) -> None:
        self._procs: list[subprocess.Popen] = []
        self._job = None  # Windows Job handle
        self._log_files: list = []

    # ── Windows Job Object ────────────────────────────────────────────────
    def _create_job(self) -> None:
        if sys.platform != "win32":
            return
        try:
            import win32job  # type: ignore

            job = win32job.CreateJobObject(None, "")
            info = win32job.QueryInformationJobObject(
                job, win32job.JobObjectExtendedLimitInformation
            )
            info["BasicLimitInformation"]["LimitFlags"] |= (
                win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            )
            win32job.SetInformationJobObject(
                job, win32job.JobObjectExtendedLimitInformation, info
            )
            self._job = job
        except Exception:
            # pywin32 missing or API failure: degrade to best-effort teardown.
            self._job = None

    def _assign_to_job(self, proc: subprocess.Popen) -> None:
        if self._job is None or sys.platform != "win32":
            return
        try:
            import win32api  # type: ignore
            import win32con  # type: ignore
            import win32job  # type: ignore

            handle = win32api.OpenProcess(
                win32con.PROCESS_SET_QUOTA | win32con.PROCESS_TERMINATE,
                False, proc.pid,
            )
            win32job.AssignProcessToJobObject(self._job, handle)
        except Exception:
            pass

    # ── process launch ────────────────────────────────────────────────────
    def _popen(self, args: list[str], log_name: str) -> subprocess.Popen:
        log_path = paths.logs_dir() / log_name
        log_fh = open(log_path, "a", encoding="utf-8", buffering=1)  # noqa: SIM115
        self._log_files.append(log_fh)
        creationflags = 0
        if sys.platform == "win32":
            # No console window for children; part of a new group so we can
            # signal them independently.
            creationflags = (
                subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
                | subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
            )
        proc = subprocess.Popen(
            args,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
            cwd=str(paths.resource_dir()),
            creationflags=creationflags,
        )
        self._assign_to_job(proc)
        self._procs.append(proc)
        return proc

    def _python_args(self, module_or_script: list[str]) -> list[str]:
        """Build the argv to run a module/script under the current interpreter.

        In frozen mode ``sys.executable`` is the bundled exe. We pass a marker
        arg the exe's __main__ understands to re-exec as a plain interpreter
        (see main_window.py bootstrap dispatch).
        """
        if paths.is_frozen():
            return [sys.executable, "--run-child", *module_or_script]
        return [sys.executable, *module_or_script]

    # ── lifecycle ─────────────────────────────────────────────────────────
    def start_main(self) -> bool:
        self._create_job()
        self._popen(self._python_args(["-m", "desktop.serve"]), "main.log")
        return _wait_ready(f"http://127.0.0.1:{MAIN_PORT}/", "main")

    def start_node_server(self) -> bool:
        if os.environ.get("AGENT_COMPOSE_NODE_SERVER_ENABLED", "true").lower() in ("0", "false", "no"):
            return True
        self._popen(self._python_args(["-m", "node_server"]), "node_server.log")
        return _wait_ready(f"http://127.0.0.1:{NODE_PORT}/health", "node_server")

    def start_tunnel_server(self) -> bool:
        if os.environ.get("TUNNEL_RUNTIME_ENABLED", "true").lower() in ("0", "false", "no"):
            return True
        self._popen(self._python_args(["-m", "tunnel_server"]), "tunnel_server.log")
        return _wait_ready(f"http://127.0.0.1:{TUNNEL_PORT}/ready", "tunnel_server")

    def start_all(self) -> bool:
        """Start the three services in order. Returns True if main came up."""
        if not self.start_main():
            return False
        # node/tunnel failures are non-fatal: the main UI degrades gracefully.
        with contextlib.suppress(Exception):
            self.start_node_server()
        with contextlib.suppress(Exception):
            self.start_tunnel_server()
        return True

    def stop_all(self) -> None:
        for proc in reversed(self._procs):
            with contextlib.suppress(Exception):
                if proc.poll() is None:
                    proc.terminate()
        deadline = time.monotonic() + 5
        for proc in reversed(self._procs):
            remaining = max(0.0, deadline - time.monotonic())
            with contextlib.suppress(Exception):
                proc.wait(timeout=remaining)
        for proc in self._procs:
            with contextlib.suppress(Exception):
                if proc.poll() is None:
                    proc.kill()
        for fh in self._log_files:
            with contextlib.suppress(Exception):
                fh.close()
        # Closing the job handle triggers KILL_ON_JOB_CLOSE for any survivors.
        if self._job is not None:
            with contextlib.suppress(Exception):
                import win32api  # type: ignore

                win32api.CloseHandle(self._job)
            self._job = None
