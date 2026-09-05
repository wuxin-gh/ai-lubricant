"""Main-service tunnel runtime process supervisor.

One process is owned per :class:`TunnelRuntime`, not per binding. Grouped frpc
runtimes receive one TOML containing multiple proxies; managed cloudflared
runtimes receive one connector token for a tunnel whose ingress configuration
contains all member bindings. Quick/npc runtimes contain one binding by design.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import re
import tempfile
import uuid
from pathlib import Path

from loguru import logger

from .models_tunnel import TunnelBinding, TunnelRuntime, TunnelScheme

_STOP_GRACE_SECONDS = 5.0
_TRYCLOUDFLARE_URL = re.compile(rb"https://[A-Za-z0-9-]+\.trycloudflare\.com")


class LocalTunnelError(Exception):
    pass


class LocalTunnelSupervisor:
    def __init__(self) -> None:
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._watchers: dict[str, asyncio.Task] = {}
        self._readers: dict[str, list[asyncio.Task]] = {}
        self._revisions: dict[str, int] = {}
        self._conf_dir: Path | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        # Runtime boot recovery is owned by TunnelRuntimeReconciler. This
        # supervisor only owns local child processes.

    async def stop(self) -> None:
        self._started = False
        for runtime_id in list(self._procs):
            await self.stop_runtime(runtime_id)

    async def start_runtime(
        self,
        runtime: TunnelRuntime,
        scheme: TunnelScheme,
        bindings: list[TunnelBinding],
        revision: int,
    ) -> None:
        from . import tunnel_binaries
        from .tunnel_node_dispatch import _frpc_group_toml, _npc_conf

        key = str(runtime.id)
        await self.stop_runtime(key)
        binary = await tunnel_binaries.ensure_binary(scheme.kind, scheme.config or {})
        env: dict[str, str] = {}

        if scheme.kind == "frpc":
            path = self._conf_path(f"runtime-{runtime.id}-r{revision}.toml")
            path.write_text(_frpc_group_toml(bindings, scheme), encoding="utf-8")
            args = ["-c", str(path)]
        elif scheme.kind == "npc":
            path = self._conf_path(f"runtime-{runtime.id}-r{revision}.conf")
            path.write_text(_npc_conf(bindings[0], scheme), encoding="utf-8")
            args = [f"-config={path}"]
        elif scheme.kind == "cloudflared":
            mode = (scheme.config or {}).get("mode") or "quick"
            if mode == "managed":
                token = await _prepare_managed(runtime, scheme, bindings)
                env["TUNNEL_TOKEN"] = token
                args = ["tunnel", "--no-autoupdate", "run"]
            else:
                binding = bindings[0]
                target = f"http://{binding.local_host}:{int(binding.local_port)}"
                args = ["tunnel", "--no-autoupdate", "--url", target]
        else:
            raise LocalTunnelError(f"unsupported kind {scheme.kind!r}")

        proc = await self._spawn(binary, args, env)
        self._procs[key] = proc
        self._revisions[key] = revision
        runtime.pid = proc.pid
        runtime.run_id = f"tunnel-runtime:{runtime.id}"
        await runtime.save(update_fields=["pid", "run_id", "updated_at"])

        reader_tasks = []
        for name, stream in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            if stream is not None:
                reader_tasks.append(asyncio.create_task(
                    self._pump(uuid.UUID(key), revision, name, stream, bindings)
                ))
        self._readers[key] = reader_tasks
        self._watchers[key] = asyncio.create_task(
            self._watch(uuid.UUID(key), revision, proc)
        )

    async def stop_runtime(self, runtime_id: str) -> None:
        key = str(runtime_id)
        for task in self._readers.pop(key, []):
            task.cancel()
        watcher = self._watchers.pop(key, None)
        if watcher:
            watcher.cancel()
        proc = self._procs.pop(key, None)
        self._revisions.pop(key, None)
        if proc is None or proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=_STOP_GRACE_SECONDS)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()

    def is_live(self, runtime_id: str, revision: int) -> bool:
        """Is the child process for this runtime still running, unchanged?

        Used by the reconciler's fallback scan to skip restarting a healthy
        process. The revision guard ensures a pending reconfigure (the
        reconciler bumped config_revision but has not called start_runtime
        yet, or update_scheme bumped it to force a hot restart) is treated as
        not-live so the reconfigure proceeds.
        """
        key = str(runtime_id)
        proc = self._procs.get(key)
        if proc is None:
            return False
        if proc.returncode is not None:
            return False
        return self._revisions.get(key) == revision

    async def _pump(
        self,
        runtime_id: uuid.UUID,
        revision: int,
        kind: str,
        stream: asyncio.StreamReader,
        bindings: list[TunnelBinding],
    ) -> None:
        from .tunnel_runtime_manager import handle_event

        while True:
            data = await stream.readline()
            if not data:
                return
            match = _TRYCLOUDFLARE_URL.search(data)
            if match and len(bindings) == 1:
                binding = bindings[0]
                binding.public_addr = match.group(0).decode("ascii", "replace")
                await binding.save(update_fields=["public_addr", "updated_at"])
                # URL is the quick tunnel READY signal.
                await handle_event(
                    runtime_id, revision=revision, kind="stdout",
                    data=b"registered tunnel connection",
                )
            await handle_event(runtime_id, revision=revision, kind=kind, data=data)

    async def _watch(
        self, runtime_id: uuid.UUID, revision: int, proc: asyncio.subprocess.Process
    ) -> None:
        from .tunnel_runtime_manager import handle_event

        try:
            code = await proc.wait()
        except asyncio.CancelledError:
            return
        self._procs.pop(str(runtime_id), None)
        await handle_event(
            runtime_id, revision=revision, kind="exited", exit_code=int(code or 0)
        )

    async def _spawn(self, binary: Path, args: list[str], env: dict[str, str]):
        child_env = dict(os.environ)
        child_env.update(env)
        try:
            return await asyncio.create_subprocess_exec(
                str(binary), *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=child_env,
            )
        except (OSError, ValueError) as exc:
            raise LocalTunnelError(f"spawn {binary.name} failed: {exc}") from exc

    def _conf_path(self, name: str) -> Path:
        if self._conf_dir is None:
            self._conf_dir = Path(tempfile.gettempdir()) / "mc-tunnel-confs"
            self._conf_dir.mkdir(parents=True, exist_ok=True)
        return self._conf_dir / name


async def _prepare_managed(
    runtime: TunnelRuntime, scheme: TunnelScheme, bindings: list[TunnelBinding]
) -> str:
    from . import tunnel_cloudflare as cf

    cfg = scheme.config or {}
    api_token = str(cfg.get("api_token") or "")
    account_id = str(cfg.get("account_id") or "")
    zone_id = str(cfg.get("zone_id") or "")
    refs = dict(runtime.provider_ref or {})
    tunnel_id = str(refs.get("tunnel_id") or "")

    if not tunnel_id:
        first = bindings[0]
        provisioned = await cf.create_managed_tunnel(
            api_token=api_token,
            account_id=account_id,
            zone_id=zone_id,
            tunnel_name=f"mc-runtime-{runtime.id}",
            hostname=str(first.hostname or ""),
            service=f"http://{first.local_host}:{int(first.local_port)}",
        )
        tunnel_id = provisioned["tunnel_id"]
        runtime.provider_ref = {"tunnel_id": tunnel_id}
        # The first call created one DNS record; move its ownership to binding.
        first.provider_ref = {"dns_record_id": provisioned["dns_record_id"]}
        await first.save(update_fields=["provider_ref", "updated_at"])
        await runtime.save(update_fields=["provider_ref", "updated_at"])

    for binding in bindings:
        refs = dict(binding.provider_ref or {})
        changed = False
        if not refs.get("dns_record_id"):
            refs["dns_record_id"] = await cf.create_dns_record(
                api_token=api_token,
                zone_id=zone_id,
                tunnel_id=tunnel_id,
                hostname=str(binding.hostname or ""),
            )
            binding.provider_ref = refs
            changed = True
        # Backfill the public address for managed bindings that lack it (e.g.
        # ones created before create_binding surfaced it). The address is fully
        # determined by the hostname, so this self-heals existing rows on the
        # next reconcile instead of leaving "地址生成中…" forever.
        if not binding.public_addr and binding.hostname:
            binding.public_addr = f"https://{binding.hostname}"
            changed = True
        if changed:
            await binding.save(update_fields=["provider_ref", "public_addr", "updated_at"])

    await cf.configure_managed_tunnel(
        api_token=api_token,
        account_id=account_id,
        tunnel_id=tunnel_id,
        ingress=[
            {
                "hostname": str(binding.hostname or ""),
                "service": f"http://{binding.local_host}:{int(binding.local_port)}",
            }
            for binding in bindings
        ],
    )
    return await cf.get_managed_tunnel_token(
        api_token=api_token, account_id=account_id, tunnel_id=tunnel_id
    )


tunnel_supervisor = LocalTunnelSupervisor()
