"""Background reconciliation of storage-state tasks.

A task bound to a ``node_id`` can sit in storage state (no live runtime) when
the node control plane was unreachable at create time — ``create_task`` leaves
it ``pending`` with ``workspace_state='dispatch_failed'``. It can also drift
into a *phantom* runtime state: ``status='processing'`` with a
``node_session_id`` whose control-plane binding is gone (node deleted, or the
node restarted and lost its in-memory session while the DB handle survived).

This worker periodically re-attempts dispatch for both shapes so that, once the
control plane or the bound execution node comes back online, the task is picked
up automatically instead of waiting for the user to click retry on every
detail page.

It reuses :meth:`TaskService._dispatch_persisted_task_runtime`, which probes an
existing handle before trusting it (``NOT_FOUND`` / not-ok ack → clear + re-
dispatch), serializes per task, and uses a conditional ``node_session_id``
update, so it is safe to run alongside manual start / first-message dispatch.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from loguru import logger

from .models_task import Task, TaskEvent
from .node_client import get_local_node_client
from .task_service import TaskService, _derive_task_title

_POLL_SECONDS = 15.0
_RETRY_BACKOFF_SECONDS = 60
# Deferred create holds this state while the node performs clone/resource sync.
# The control-plane ack timeout is 30s; leave ample margin before takeover.
_DISPATCHING_STALE_SECONDS = 90


class TaskRuntimeWorker:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="task-runtime-worker")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.recover_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[task-runtime] recovery iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def recover_pending(self) -> bool:
        """Reconcile storage-state tasks bound to a node, dispatching or re-dispatching.

        Scans two categories together, both with ``node_id`` set:
        - ``pending`` + no session id: never dispatched, or previously cleared.
        - ``processing`` + a session id whose control-plane binding is gone (node
          restart tore it down, node was deleted, etc.). The dispatch helper
          probes the existing handle first; ``NOT_FOUND`` clears it and
          re-dispatches, so these self-heal instead of waiting for the user to
          click retry on every detail page.

        Both go through the same lazy-dispatch helper (per-task lock +
        conditional ``node_session_id`` update), so this is safe to run
        alongside manual start / first-message dispatch.
        """
        client = get_local_node_client()
        if not client.enabled:
            return False
        now = datetime.now(UTC)
        # Fresh dispatching rows are still owned by create_task's background
        # coroutine. Tortoise exclude negates the conjunction, so stale
        # dispatching rows remain candidates and recover after a process crash.
        dispatching_stale_before = now - timedelta(seconds=_DISPATCHING_STALE_SECONDS)
        candidates = await Task.filter(
            status__in=["pending", "processing"], node_id__not_isnull=True
        ).exclude(
            workspace_state="dispatching",
            updated_at__gte=dispatching_stale_before,
        ).order_by("created_at")
        if not candidates:
            return False
        service = TaskService()
        worked = False
        for task in candidates:
            node_id = (task.node_id or "").strip()
            if not node_id:
                continue
            snapshot = task.config_snapshot if isinstance(task.config_snapshot, dict) else {}
            retry_at = snapshot.get("dispatch_retry_at")
            if retry_at:
                try:
                    due = datetime.fromisoformat(str(retry_at))
                except ValueError:
                    due = None
                if due is not None and due > now:
                    continue
            try:
                await service._dispatch_persisted_task_runtime(task)
                worked = True
                # Deferred-create compensation: the first turn was persisted
                # (delivery_status='pending') before dispatch; a crash between
                # persist and deliver leaves it orphaned. Deliver it now if the
                # claim state machine still shows it unclaimed — concurrent
                # claimants (create background task, user resend) collapse into
                # one delivery.
                await self._deliver_pending_first_turn(service, task)
            except ValueError as exc:
                reason = str(exc)
                if reason == "node_server_unavailable":
                    # Retryable (node offline / control plane down). The dispatch
                    # helper already persisted dispatch_failed + the real reason;
                    # only add the backoff so we don't hammer the control plane.
                    await self._schedule_retry(task)
                elif reason == "task_not_startable":
                    continue
                # runtime_dispatch_failed is a permanent rejection: the helper
                # terminalized the task as error with the reason attached, so it
                # falls out of this pending/processing scan next tick. Re-scheduling
                # it here would resurrect it and loop forever.
                # node_required / node_forbidden / task_api_key_unavailable are
                # not transient either; leave the row for the user to see.
            except Exception:
                logger.exception("[task-runtime] recover task {} failed", task.id)
        return worked

    async def _deliver_pending_first_turn(self, service: TaskService, task: Task) -> None:
        """Deliver the deferred-create first turn after a (re)dispatch succeeded.

        Only the deferred-create path leaves a durable ``pending`` user_input
        row (normal sends claim → dispatching inside the send lock). The row's
        ``client_message_id`` participates in the same claim state machine as
        manual sends, so this is a no-op when the create background task or the
        user already delivered it.
        """
        content = (task.content or "").strip()
        if not content:
            return
        try:
            row = await TaskEvent.filter(
                task_id=task.id,
                event_type="user_input",
                delivery_status="pending",
                client_message_id__not_isnull=True,
            ).order_by("seq").first()
            if row is None or not (row.client_message_id or "").strip():
                return
            node_session_id = (task.node_session_id or "").strip()
            if not node_session_id:
                return
            claim, attempt = await service._claim_message_for_dispatch(task, row)
            if claim != "pending":
                return
            await service._deliver_turn(
                task, node_session_id, content, str(row.client_message_id), attempt,
                _derive_task_title(content),
            )
        except Exception:  # noqa: BLE001 — reconciliation is diagnostics
            logger.exception("[task-runtime] compensate first turn for {} failed", task.id)

    async def _schedule_retry(self, task: Task) -> None:
        """Add the backoff stamp to an already-marked retryable failure.

        ``_dispatch_persisted_task_runtime`` has persisted ``dispatch_failed``
        plus the human-readable ``dispatch_error`` (e.g. "node node-1 is
        offline"). This only stamps the next-attempt time — it must not
        overwrite that reason with the bare error code, nor change ``status``,
        so a task the helper terminalized is never resurrected to pending.
        """
        snapshot = dict(task.config_snapshot or {}) if isinstance(task.config_snapshot, dict) else {}
        snapshot["dispatch_retry_at"] = (
            datetime.now(UTC) + timedelta(seconds=_RETRY_BACKOFF_SECONDS)
        ).isoformat()
        try:
            await Task.filter(id=task.id).update(config_snapshot=snapshot)
        except Exception:
            logger.exception("[task-runtime] persist retry schedule for {} failed", task.id)
        else:
            task.config_snapshot = snapshot


task_runtime_worker = TaskRuntimeWorker()
