"""Shared message delivery-status state machine.

This is the single source of truth for ``mc_task_events.delivery_status``
transitions. Both the gateway (``user_platform.task_service``) and the
node control service (``node_server.task_message_status``) import from here
so the transition table can never drift between the two processes.

A transition not listed here is rejected by the conditional UPDATE guard.
"""

# Allowed delivery-status transitions (from → to). Any unlisted transition
# is rejected by the conditional UPDATE, so replayed / out-of-order ACKs
# (SSE replays, late-arriving events) can never regress the state machine.
#
# ``dispatching`` is the in-flight transient: only one request can claim
# pending → dispatching (atomic DB claim, idempotent across API processes);
# the node ACK advances it to ``received``, a delivery failure falls back to
# ``failed``. Stuck in ``dispatching`` past _DISPATCHING_STALE_SECONDS
# (default 120) means the delivery result is unknown (process crash /
# timeout); the same id may be re-claimed — true dedup is guarded by the
# node + runtime (message-id, attempt) idempotency, so the turn won't
# execute twice.
MESSAGE_STATUS_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "pending": ("dispatching", "failed", "cancelled"),
    "dispatching": ("received", "failed"),
    "received": ("running", "completed", "failed", "cancelled"),
    "running": ("completed", "failed", "cancelled"),
    # failed → pending: same-id explicit retry, re-enters the queue.
    # failed → completed: a turn that errored then finished normally;
    #   the terminal frame wins.
    "failed": ("pending", "completed"),
    # cancelled → pending: same-id explicit retry, re-enters the queue.
    "cancelled": ("pending",),
}
