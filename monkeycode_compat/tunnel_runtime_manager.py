"""Process-level tunnel runtime aggregation and reconciliation.

Bindings describe desired proxy mappings. Runtimes describe the actual client
processes that implement them:

* frpc + cloudflared managed: one runtime per target × scheme.
* cloudflared quick + npc: one runtime per binding (explicit exceptions).

Every binding mutation reconciles its runtime under a per-key asyncio lock:
stop the old revision, render all desired bindings, start one replacement, and
project its status back onto member bindings. The monotonically increasing
revision fences late STARTED/EXITED events from an old process.
"""
from __future__ import annotations

import asyncio
import contextlib
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from loguru import logger

from .models_tunnel import TunnelBinding, TunnelRuntime, TunnelScheme

_LOCKS: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_READY_TIMEOUT = 30.0
_STDERR_LIMIT = 4096

# Owner identity and lease TTL for this process. Defaulted lazily so the
# module is importable in tests without tunnel_server installed; the standalone
# service sets these from its settings at startup via configure_lease().
_LEASE_OWNER: str = ""
_LEASE_TTL_SECONDS: int = 60


def configure_lease(*, owner: str, ttl_seconds: int) -> None:
    """Bind this process's lease identity. Called once by tunnel_server at boot."""
    global _LEASE_OWNER, _LEASE_TTL_SECONDS
    _LEASE_OWNER = owner
    _LEASE_TTL_SECONDS = max(10, int(ttl_seconds))


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _lease_sql(sql_qmark: str) -> str:
    """Return the SQL with placeholders matching the active backend.

    Tortoise's asyncpg backend does NOT auto-translate ``?`` placeholders, so
    on postgres we must use ``$N`` positional style. aiosqlite (sqlite, used in
    tests) expects ``?``. We detect the backend from the connection class name
    so the same helper runs in both.
    """
    conn = _compat_conn()
    backend = type(conn).__module__ or ""
    if "asyncpg" in backend or "postgres" in backend:
        counter = 0

        def replace_placeholder(_match):
            nonlocal counter
            counter += 1
            return f"${counter}"

        return re.sub(r"\?", replace_placeholder, sql_qmark)
    return sql_qmark


async def _acquire_lease(runtime: TunnelRuntime) -> bool:
    """Atomically claim this runtime's lease for this process.

    Returns True if this process now owns an unexpired lease. A lease is
    acquirable when it is free, already expired, or already owned by us. Skips
    entirely when no owner is configured (single-replica / tests): there is no
    other process to protect against, so the claim always succeeds.
    """
    if not _LEASE_OWNER:
        return True
    now = _now()
    until = _now_plus(_LEASE_TTL_SECONDS)
    # One conditional UPDATE is the atomic claim: matching rows whose lease is
    # NULL, expired, or already ours. rowcount == 1 means we took it.
    conn = _compat_conn()
    sql = _lease_sql(
        """
        UPDATE mc_tunnel_runtimes
           SET lease_owner = ?, lease_until = ?
         WHERE id = ?
           AND (lease_owner IS NULL
                OR lease_owner = ?
                OR lease_until IS NULL
                OR lease_until < ?)
        """
    )
    n, _ = await conn.execute_query(sql, [_LEASE_OWNER, until, str(runtime.id), _LEASE_OWNER, now])
    return bool(n)


async def _renew_lease(runtime: TunnelRuntime) -> None:
    """Extend the lease mid-reconcile so a long stop/start is not stolen."""
    if not _LEASE_OWNER:
        return
    conn = _compat_conn()
    sql = _lease_sql(
        "UPDATE mc_tunnel_runtimes SET lease_until = ? WHERE id = ? AND lease_owner = ?"
    )
    await conn.execute_query(sql, [_now_plus(_LEASE_TTL_SECONDS), str(runtime.id), _LEASE_OWNER])


async def _release_lease(runtime: TunnelRuntime) -> None:
    """Drop our lease so another replica can take over immediately on failover."""
    if not _LEASE_OWNER:
        return
    conn = _compat_conn()
    sql = _lease_sql(
        "UPDATE mc_tunnel_runtimes SET lease_owner = NULL, lease_until = NULL "
        "WHERE id = ? AND lease_owner = ?"
    )
    with contextlib.suppress(Exception):
        await conn.execute_query(sql, [str(runtime.id), _LEASE_OWNER])


def _now_plus(seconds: int) -> datetime:
    return _now() + timedelta(seconds=seconds)


def _compat_conn():
    from tortoise import Tortoise

    return Tortoise.get_connection("monkeycode_compat")


def is_grouped(scheme: TunnelScheme) -> bool:
    if scheme.kind == "frpc":
        return True
    return scheme.kind == "cloudflared" and (scheme.config or {}).get("mode") == "managed"


def runtime_key(scheme: TunnelScheme, target_id: str, binding_id: str | None = None) -> str:
    base = f"{target_id}:{scheme.id}"
    return base if is_grouped(scheme) else f"{base}:{binding_id}"


async def ensure_runtime(
    scheme: TunnelScheme, target_id: str, binding_id: str | None = None
) -> TunnelRuntime:
    key = runtime_key(scheme, target_id, binding_id)
    row = await TunnelRuntime.get_or_none(runtime_key=key)
    if row is not None:
        return row
    row = await TunnelRuntime.create(
        id=uuid.uuid4(),
        scheme_id=scheme.id,
        target_id=target_id,
        binding_id=uuid.UUID(binding_id) if binding_id and not is_grouped(scheme) else None,
        runtime_key=key,
        kind=scheme.kind,
        desired_state="running",
        runtime_status="pending",
        config_revision=0,
    )
    row.run_id = f"tunnel-runtime:{row.id}"
    await row.save(update_fields=["run_id", "updated_at"])
    return row


async def runtime_for_binding(binding: TunnelBinding, scheme: TunnelScheme) -> TunnelRuntime:
    return await ensure_runtime(scheme, binding.node_id, str(binding.id))


async def desired_bindings(runtime: TunnelRuntime) -> list[TunnelBinding]:
    qs = TunnelBinding.filter(
        scheme_id=runtime.scheme_id,
        node_id=runtime.target_id,
        desired_state="running",
        delete_requested=False,
    )
    if runtime.binding_id:
        qs = qs.filter(id=runtime.binding_id)
    return await qs.order_by("created_at")


async def project_runtime(runtime: TunnelRuntime, bindings: list[TunnelBinding] | None = None) -> None:
    """Mirror runtime state into legacy binding fields returned by current APIs."""
    bindings = bindings if bindings is not None else await TunnelBinding.filter(
        scheme_id=runtime.scheme_id, node_id=runtime.target_id
    )
    for binding in bindings:
        if binding.desired_state == "stopped":
            binding.client_status = "stopped"
            binding.error = None
        else:
            status = runtime.runtime_status
            binding.client_status = (
                "pending" if status in ("pending", "starting") else status
            )
            binding.error = runtime.error
        binding.run_id = runtime.run_id
        await binding.save(update_fields=[
            "run_id", "client_status", "error", "updated_at"
        ])


async def reconcile_binding(binding: TunnelBinding, scheme: TunnelScheme) -> TunnelRuntime:
    runtime = await runtime_for_binding(binding, scheme)
    return await reconcile_runtime(runtime, scheme)


async def reconcile_runtime(runtime: TunnelRuntime, scheme: TunnelScheme | None = None) -> TunnelRuntime:
    lock = _LOCKS[runtime.runtime_key]
    async with lock:
        if not await _acquire_lease(runtime):
            # Another replica owns this runtime and is mid-reconcile. Leave its
            # process untouched; the fallback scan will retry next tick.
            logger.debug("[tunnel] lease held by another owner for {}", runtime.runtime_key)
            return runtime
        try:
            return await _reconcile_runtime_locked(runtime, scheme)
        finally:
            await _release_lease(runtime)


def _permanent_provisioning_error(error: str | None) -> bool:
    """Was this a provisioning failure retrying cannot fix?

    Matches only Cloudflare-API-shaped errors (auth/permission/not-found/
    hostname-occupied-by-user). Client-runtime stderr failures ("connection
    refused" etc. from cloudflared/frpc itself) deliberately do NOT match —
    those may recover on their own and keep the existing retry behavior.
    """
    if not error:
        return False
    markers = ("HTTP 401", "HTTP 403", "HTTP 404", "occupied by a user-created")
    return any(marker in str(error) for marker in markers)


def _config_changed_since(
    runtime: TunnelRuntime, scheme: TunnelScheme | None, bindings: list[TunnelBinding]
) -> bool:
    """Did the admin touch the scheme or a member binding after our last fail?

    ``runtime.updated_at`` moves on every reconcile attempt, so a failed
    attempt always stamps the runtime newer than the inputs that caused it.
    Only a later scheme/binding edit (or a fresh start click, which bumps the
    binding) moves an input past that stamp — exactly the "something changed,
    worth retrying" signal.
    """
    failure_at = runtime.updated_at
    if failure_at is None:
        return True  # never attempted
    latest = scheme.updated_at if scheme is not None else None
    for binding in bindings:
        if binding.updated_at is not None and (latest is None or binding.updated_at > latest):
            latest = binding.updated_at
    if latest is None:
        return False
    return latest > failure_at


async def _reconcile_runtime_locked(
    runtime: TunnelRuntime, scheme: TunnelScheme | None = None
) -> TunnelRuntime:
    await runtime.refresh_from_db()
    scheme = scheme or await TunnelScheme.get_or_none(id=runtime.scheme_id)
    if scheme is None or not scheme.enabled:
        # Disabled/deleted scheme: stop the running client too — marking the
        # runtime failed while leaving the process up would keep the tunnel
        # live against the admin's intent.
        await _stop_runtime(runtime)
        runtime.runtime_status = "failed"
        runtime.error = "scheme missing or disabled"
        await runtime.save(update_fields=["runtime_status", "error", "updated_at"])
        await project_runtime(runtime)
        return runtime

    # Per-binding runtimes whose single member was soft-deleted get torn
    # down here so the empty runtime can be dropped below.
    # Don't auto-retry a permanent provisioning failure every tick — the
    # reconciler would just re-walk the same auth/permission/occupied-hostname
    # error forever. Resume only when the admin actually changed something
    # (scheme or member binding updated after the failure; a fresh "start"
    # click bumps the binding's updated_at, so manual retry still works).
    # Transient failures (network/5xx) keep retrying.
    if (
        runtime.runtime_status == "failed"
        and _permanent_provisioning_error(runtime.error)
    ):
        bindings = await desired_bindings(runtime)
        if not _config_changed_since(runtime, scheme, bindings):
            await project_runtime(runtime, bindings)
            return runtime

    await _reclaim_deleted_members(runtime, scheme)

    bindings = await desired_bindings(runtime)
    runtime.config_revision += 1
    revision = runtime.config_revision
    runtime.desired_state = "running" if bindings else "stopped"
    runtime.runtime_status = "pending" if bindings else "stopped"
    runtime.error = None
    runtime.stderr_tail = None
    runtime.pid = None
    await runtime.save(update_fields=[
        "config_revision", "desired_state", "runtime_status", "error",
        "stderr_tail", "pid", "updated_at",
    ])

    await _stop_runtime(runtime)
    if not bindings:
        # No members left: free runtime-level provider resources (managed
        # cloudflared tunnel) and drop the runtime row so the next binding
        # starts clean. The per-binding DNS records were already reclaimed
        # in _reclaim_deleted_members above.
        await _drop_runtime(runtime)
        await project_runtime(runtime)
        return runtime

    # Stop/start can outlive a short lease window; refresh before dispatch.
    await _renew_lease(runtime)
    runtime.runtime_status = "starting"
    await runtime.save(update_fields=["runtime_status", "updated_at"])
    await project_runtime(runtime, bindings)

    try:
        if runtime.target_id == "__main__":
            await _start_local(runtime, scheme, bindings, revision)
        else:
            await _start_node(runtime, scheme, bindings, revision)
    except Exception as exc:  # noqa: BLE001
        # Surface the real provisioning failure to the log — without this the
        # reconciler swallows the exception into runtime.error (DB only), so the
        # admin sees only the symptom (e.g. a reclaim INFO line) every 45s tick
        # and never the cause (DNS 403, bad token, spawn failure, ...).
        logger.error(
            "[tunnel] runtime {} start failed: {}", runtime.id, exc
        )
        runtime.runtime_status = "failed"
        runtime.error = str(exc)[:500]
        await runtime.save(update_fields=["runtime_status", "error", "updated_at"])
        await project_runtime(runtime, bindings)
        return runtime

    await runtime.refresh_from_db()
    if runtime.config_revision == revision and runtime.runtime_status == "starting":
        # Dispatch returned but no monitor promoted the process yet. Mark as
        # pending rather than lying that the tunnel is ready.
        runtime.runtime_status = "pending"
        await runtime.save(update_fields=["runtime_status", "updated_at"])
    await project_runtime(runtime, bindings)
    return runtime


async def _reclaim_deleted_members(
    runtime: TunnelRuntime, scheme: TunnelScheme
) -> None:
    """Reclaim per-binding provider resources for soft-deleted members.

    A binding that the data service marked ``delete_requested`` is no longer a
    desired member. Its DNS record (managed cloudflared) is deleted here; the
    binding row itself is left for the data service (or a later housekeeping
    sweep) to drop once no runtime still references it.
    """
    qs = TunnelBinding.filter(
        scheme_id=runtime.scheme_id,
        node_id=runtime.target_id,
        delete_requested=True,
    )
    if runtime.binding_id:
        qs = qs.filter(id=runtime.binding_id)
    for binding in await qs:
        await _reclaim_binding_resources(binding, scheme)


async def _reclaim_binding_resources(
    binding: TunnelBinding, scheme: TunnelScheme
) -> None:
    """Delete provider-side resources owned by one binding (managed DNS)."""
    if not binding.provider_ref or scheme.kind != "cloudflared":
        return
    config = scheme.config or {}
    dns_record_id = str((binding.provider_ref or {}).get("dns_record_id") or "")
    if not dns_record_id:
        return
    try:
        from . import tunnel_cloudflare as cf

        await cf.delete_dns_record(
            api_token=str(config.get("api_token") or ""),
            zone_id=str(config.get("zone_id") or ""),
            dns_record_id=dns_record_id,
        )
    except Exception as exc:  # noqa: BLE001 — reclaim must not block reconcile
        logger.warning("[tunnel] cloudflare reclaim failed for {}: {}", binding.id, exc)


async def _drop_runtime(runtime: TunnelRuntime) -> None:
    """Release runtime-level provider resources (managed tunnel) and delete row.

    Called when a grouped runtime no longer has any live member binding. The
    per-binding DNS records were already reclaimed by
    :func:`_reclaim_deleted_members`; this frees the shared tunnel and removes
    the runtime row so a future first binding starts fresh.
    """
    if runtime.target_id == "__main__":
        # Local processes were stopped in _stop_runtime above; nothing else.
        pass
    if (runtime.provider_ref or {}) and runtime.kind == "cloudflared":
        scheme = await TunnelScheme.get_or_none(id=runtime.scheme_id)
        if scheme is not None and (scheme.config or {}).get("mode") == "managed":
            tunnel_id = str((runtime.provider_ref or {}).get("tunnel_id") or "")
            if tunnel_id:
                try:
                    from . import tunnel_cloudflare as cf

                    await cf.delete_managed_tunnel(
                        api_token=str(scheme.config.get("api_token") or ""),
                        account_id=str(scheme.config.get("account_id") or ""),
                        zone_id=str(scheme.config.get("zone_id") or ""),
                        provider_ref={"tunnel_id": tunnel_id},
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[tunnel] managed tunnel reclaim failed for {}: {}", runtime.id, exc)
    # Delete the soft-deleted binding rows now that their runtime is gone.
    await TunnelBinding.filter(
        scheme_id=runtime.scheme_id,
        node_id=runtime.target_id,
        delete_requested=True,
    ).delete()
    await runtime.delete()


async def _start_local(
    runtime: TunnelRuntime,
    scheme: TunnelScheme,
    bindings: list[TunnelBinding],
    revision: int,
) -> None:
    from .tunnel_supervisor import tunnel_supervisor

    await tunnel_supervisor.start_runtime(runtime, scheme, bindings, revision)


async def _start_node(
    runtime: TunnelRuntime,
    scheme: TunnelScheme,
    bindings: list[TunnelBinding],
    revision: int,
) -> None:
    from .tunnel_node_dispatch import dispatch_runtime_to_node

    await dispatch_runtime_to_node(runtime, scheme, bindings, revision)


async def _stop_runtime(runtime: TunnelRuntime) -> None:
    if runtime.target_id == "__main__":
        from .tunnel_supervisor import tunnel_supervisor

        await tunnel_supervisor.stop_runtime(str(runtime.id))
        return
    if not runtime.run_id:
        return
    from .node_client import get_local_node_client

    with contextlib.suppress(Exception):
        await get_local_node_client().stop_tool_run(runtime.target_id, runtime.run_id)


async def set_binding_desired(
    binding: TunnelBinding, scheme: TunnelScheme, desired: str
) -> TunnelRuntime:
    binding.desired_state = desired
    await binding.save(update_fields=["desired_state", "updated_at"])
    return await reconcile_binding(binding, scheme)


async def remove_binding(binding: TunnelBinding, scheme: TunnelScheme) -> None:
    runtime = await runtime_for_binding(binding, scheme)
    binding.desired_state = "stopped"
    await binding.save(update_fields=["desired_state", "updated_at"])
    await reconcile_runtime(runtime, scheme)


async def handle_event(
    runtime_id: uuid.UUID,
    *,
    revision: int,
    kind: str,
    data: bytes = b"",
    exit_code: int = 0,
    error: str = "",
    pid: int = 0,
) -> None:
    """Apply one runtime event if its revision is still current."""
    runtime = await TunnelRuntime.get_or_none(id=runtime_id)
    if runtime is None or revision != runtime.config_revision:
        return
    if kind == "started":
        runtime.pid = pid or None
        runtime.observed_revision = revision
        runtime.last_heartbeat_at = _now()
        await runtime.save(update_fields=[
            "pid", "observed_revision", "last_heartbeat_at", "updated_at"
        ])
        return
    if kind in ("stdout", "stderr"):
        text = data.decode("utf-8", "replace")
        if kind == "stderr":
            runtime.stderr_tail = ((runtime.stderr_tail or "") + text)[-_STDERR_LIMIT:]
        verdict = _readiness(runtime.kind, text)
        if verdict == "failed":
            runtime.runtime_status = "failed"
            runtime.error = text.strip()[-500:] or "client reported a connection error"
        elif verdict == "ready" and runtime.runtime_status != "running":
            runtime.runtime_status = "running"
            runtime.ready_at = _now()
            runtime.error = None
        await runtime.save(update_fields=[
            "runtime_status", "ready_at", "error", "stderr_tail", "updated_at"
        ])
        await project_runtime(runtime)
        return
    if kind == "exited":
        runtime.pid = None
        if runtime.desired_state == "stopped" and exit_code == 0:
            runtime.runtime_status = "stopped"
            runtime.error = None
        else:
            runtime.runtime_status = "failed"
            runtime.error = error or f"client exited code={exit_code}"
        await runtime.save(update_fields=[
            "pid", "runtime_status", "error", "updated_at"
        ])
        await project_runtime(runtime)


def _local_runtime_live(runtime: TunnelRuntime, scheme: TunnelScheme) -> bool:
    """Is the in-process supervisor still running this runtime unchanged?

    Used by the fallback scan to avoid restarting a healthy ``__main__`` client
    every poll. Returns False for anything other than a grouped cloudflared
    managed / frpc / npc runtime whose revision matches the supervisor's.
    """
    if runtime.target_id != "__main__":
        return False
    try:
        from .tunnel_supervisor import tunnel_supervisor
    except Exception:  # noqa: BLE001
        return False
    return tunnel_supervisor.is_live(str(runtime.id), runtime.config_revision)


def _readiness(kind: str, text: str) -> str | None:
    lower = text.lower()
    failures = (
        "authentication failed", "login failed", "connection refused",
        "permission denied", "invalid token", "failed to connect",
    )
    if any(marker in lower for marker in failures):
        return "failed"
    if kind == "frpc" and (
        "login to server success" in lower
        or "start proxy success" in lower
        or "proxy added" in lower
    ):
        return "ready"
    if kind == "cloudflared" and (
        "registered tunnel connection" in lower
        or "connection registered" in lower
    ):
        return "ready"
    return None


async def reconcile_all() -> None:
    """Boot/reconnect recovery for desired-running runtimes.

    Main-service processes never survive our process restart, so they are always
    respawned. Node runtimes first compare the node heartbeat inventory: a
    matching run_id+revision is adopted without interruption; a missing/stale
    run is recreated. Unknown tunnel-runtime runs are stopped as orphans.

    Newly created bindings (desired running, not yet delete_requested) that
    have no runtime row yet are materialized here so a NOTIFY that raced the
    insert still converges.
    """
    # Materialize missing runtime rows for live bindings.
    await _ensure_runtimes_for_live_bindings()

    rows = await TunnelRuntime.filter(desired_state="running")
    # 批量预取本批 runtime 引用的 scheme，避免下面每个 runtime 逐条查一次
    # （runtimes 多时就是 N+1，且这段是每次 fallback scan 都跑的热路径）。
    scheme_ids = {r.scheme_id for r in rows if r.scheme_id}
    schemes_by_id: dict = {
        s.id: s for s in await TunnelScheme.filter(id__in=list(scheme_ids))
    } if scheme_ids else {}
    by_target: dict[str, list[TunnelRuntime]] = defaultdict(list)
    for runtime in rows:
        by_target[runtime.target_id].append(runtime)

    for target_id, runtimes in by_target.items():
        if target_id == "__main__":
            for runtime in runtimes:
                scheme = schemes_by_id.get(runtime.scheme_id)
                if scheme is None:
                    continue
                if _local_runtime_live(runtime, scheme):
                    # Process still alive at the current revision; do not restart
                    # on every fallback scan.
                    continue
                asyncio.create_task(reconcile_runtime(runtime, scheme))
            continue
        try:
            from .node_client import get_local_node_client

            actual = await get_local_node_client().list_active_tool_runs(target_id)
        except Exception:
            for runtime in runtimes:
                runtime.runtime_status = "offline"
                await runtime.save(update_fields=["runtime_status", "updated_at"])
                await project_runtime(runtime)
            continue

        actual_by_id = {str(item.get("run_id") or ""): item for item in actual}
        expected_ids = {str(runtime.run_id or "") for runtime in runtimes}
        for runtime in runtimes:
            item = actual_by_id.get(str(runtime.run_id or ""))
            if item and int(item.get("revision") or 0) == runtime.config_revision:
                runtime.observed_revision = runtime.config_revision
                runtime.pid = int(item.get("pid") or 0) or None
                runtime.last_heartbeat_at = _now()
                # Inventory proves liveness, not protocol readiness. Preserve a
                # previously confirmed running state; otherwise stay pending.
                if runtime.runtime_status != "running":
                    runtime.runtime_status = "pending"
                await runtime.save(update_fields=[
                    "runtime_status", "observed_revision", "pid",
                    "last_heartbeat_at", "updated_at"
                ])
                await project_runtime(runtime)
            else:
                scheme = schemes_by_id.get(runtime.scheme_id)
                if scheme is not None:
                    asyncio.create_task(reconcile_runtime(runtime, scheme))

        # Stop orphaned grouped runtime processes no longer present in DB intent.
        for run_id in actual_by_id:
            if run_id.startswith("tunnel-runtime:") and run_id not in expected_ids:
                with contextlib.suppress(Exception):
                    await get_local_node_client().stop_tool_run(target_id, run_id)

    # Reconcile any runtime whose member bindings were soft-deleted so the
    # reclamation + row-drop path runs even if no fresh NOTIFY arrives.
    await _reconcile_soft_deleted()


async def _ensure_runtimes_for_live_bindings() -> None:
    """Create a runtime row for every live binding that has none yet.

    frpc + cloudflared managed are grouped (target × scheme); quick and npc
    are per-binding. This keeps a NOTIFY that landed between the binding
    insert and the runtime insert from getting lost — the fallback scan will
    still converge on the next tick.
    """
    live = await TunnelBinding.filter(
        desired_state="running", delete_requested=False
    )
    # 批量预取 binding 引用的 scheme，避免逐条 get_or_none N+1。
    scheme_ids = {b.scheme_id for b in live if b.scheme_id}
    schemes_by_id: dict = {
        s.id: s for s in await TunnelScheme.filter(id__in=list(scheme_ids))
    } if scheme_ids else {}
    for binding in live:
        scheme = schemes_by_id.get(binding.scheme_id)
        if scheme is None or not scheme.enabled:
            continue
        await ensure_runtime(scheme, binding.node_id, str(binding.id))


async def _reconcile_soft_deleted() -> None:
    """Reclaim runtimes that have soft-deleted members pending teardown."""
    deleted = await TunnelBinding.filter(delete_requested=True).only(
        "scheme_id", "node_id", "id"
    )
    # 批量预取 scheme，避免逐条 get_or_none N+1。
    scheme_ids = {b.scheme_id for b in deleted if b.scheme_id}
    schemes_by_id: dict = {
        s.id: s for s in await TunnelScheme.filter(id__in=list(scheme_ids))
    } if scheme_ids else {}
    seen: set[str] = set()
    for binding in deleted:
        scheme = schemes_by_id.get(binding.scheme_id)
        if scheme is None:
            continue
        key = runtime_key(scheme, binding.node_id, str(binding.id))
        if key in seen:
            continue
        seen.add(key)
        runtime = await TunnelRuntime.get_or_none(runtime_key=key)
        if runtime is None:
            continue
        asyncio.create_task(reconcile_runtime(runtime, scheme))


async def reconcile_target(target_id: str) -> None:
    """Reconcile one node immediately after/repeatedly during heartbeat."""
    rows = await TunnelRuntime.filter(target_id=target_id, desired_state="running")
    if not rows:
        return
    await reconcile_all()


class TunnelRuntimeReconciler:
    """Low-frequency desired/actual repair loop (heartbeats are the data source)."""

    def __init__(self, interval: float = 45.0) -> None:
        self.interval = interval
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        await reconcile_all()
        self._task = asyncio.create_task(self._run(), name="tunnel-runtime-reconciler")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            try:
                await reconcile_all()
            except Exception:  # noqa: BLE001
                logger.exception("[tunnel] periodic runtime reconcile failed")


tunnel_runtime_reconciler = TunnelRuntimeReconciler()
