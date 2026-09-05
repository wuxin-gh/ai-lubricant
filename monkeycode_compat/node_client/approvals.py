"""In-process registry for node-shell commands awaiting admin approval.

When ``node_shell_exec`` hits a command that requires confirmation, it creates a
:class:`PendingAction` here, emits a ``confirmation_required`` SSE event, and
``await``s the action's future — truly pausing the agent turn until the admin
approves or denies (or the 5-minute timeout auto-denies). The approve/deny API
endpoint resolves the future from a different request; both run in the same
asyncio loop (one data-service process), so cross-task ``set_result`` is safe.

Restart resilience: the registry is in-memory, so a server restart loses the
futures. The agent-loop task holding a lost future is also gone (the SSE stream
dropped), so there is no orphaned waiter. A reconnecting frontend re-fetches
``GET /approvals``; a confirmation id that no longer exists reads as
``interrupted`` on the client.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional


# How long a confirmation stays actionable before the turn is aborted. Long
# enough for an operator to context-switch (they may be away from the screen for
# a while); the per-Agent ``approval_timeout_seconds`` config overrides it, and a
# value of 0 there means "never expire". Kept short of literally-infinite because
# a pending approval pins an asyncio task + an SSE connection + the bound node
# PTY, and the in-memory registry is lost on restart anyway.
CONFIRMATION_TIMEOUT_SECONDS = 24 * 60 * 60


class ApprovalTimeout(Exception):
    """Raised into the agent turn when a confirmation is not answered in time.

    Unlike an auto-deny (which flows back to the model as a normal tool result
    and lets the loop keep going), this propagates up to the runner so the turn
    ends and waits for the human — the model is not called again on a timeout.
    """

    def __init__(self, confirmation_id: str, tool_name: str) -> None:
        self.confirmation_id = confirmation_id
        self.tool_name = tool_name
        super().__init__(f"审批超时未处理，已中止本轮（{tool_name}）")


class ApprovalDenied(Exception):
    """Raised into the agent turn when the admin explicitly denies a confirmation.

    Symmetric with :class:`ApprovalTimeout`: an explicit "拒绝" must not flow back
    to the model as a normal ``{"status": "denied"}`` tool result — that lets the
    loop keep calling the model on a turn the human already cancelled. Propagates
    up to the runner so the turn ends cleanly.

    Only a genuine user rejection raises this. The other "deny" outcomes (no UI
    configured, no coordinator, single-command fallback, tamper-detection) still
    return a ``denied`` tool result, because they are not a human saying no.
    """

    def __init__(self, confirmation_id: str, tool_name: str) -> None:
        self.confirmation_id = confirmation_id
        self.tool_name = tool_name
        super().__init__(f"用户已拒绝执行，已中止本轮（{tool_name}）")


@dataclass
class PendingAction:
    confirmation_id: str
    conversation_id: str
    node_id: str
    tool_name: str
    command: str
    command_hash: str
    requester: Optional[str]
    shell_flavor: str
    created_at: float
    expires_at: float
    future: asyncio.Future = field(repr=False)
    resolved: Optional[str] = None  # "allow" | "deny" | "timeout" | "interrupted"
    # Optional metadata for non-shell tools (for example code_run language,
    # timeout and Skill provenance). The approval hash still covers ``command``.
    metadata: dict = field(default_factory=dict)
    # For a multi-command turn the whole set is approved or denied together, so
    # one action fronts several commands. Single commands keep this at length 1.
    commands: list[str] = field(default_factory=list)

    def is_expired(self, now: float | None = None) -> bool:
        # expires_at==0 是「不过期」哨兵（见 ApprovalRegistry.create）。
        if not self.expires_at:
            return False
        return (now or time.time()) >= self.expires_at

    def to_dict(self) -> dict:
        return {
            "confirmation_id": self.confirmation_id,
            "conversation_id": self.conversation_id,
            "node_id": self.node_id,
            "tool_name": self.tool_name,
            "command": self.command,
            "commands": self.commands or [self.command],
            "command_hash": self.command_hash,
            "requester": self.requester,
            "shell_flavor": self.shell_flavor,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "resolved": self.resolved,
            "metadata": self.metadata,
        }


def hash_command(command: str) -> str:
    return hashlib.sha256((command or "").encode("utf-8")).hexdigest()


class ApprovalRegistry:
    """Thread-safe (single-loop) map of in-flight confirmations."""

    def __init__(self) -> None:
        self._pending: dict[str, PendingAction] = {}
        self._lock = asyncio.Lock()

    def create(
        self,
        *,
        conversation_id: str,
        node_id: str,
        tool_name: str,
        command: str,
        requester: Optional[str],
        shell_flavor: str = "unknown",
        timeout_seconds: int = CONFIRMATION_TIMEOUT_SECONDS,
        commands: list[str] | None = None,
        metadata: dict | None = None,
    ) -> PendingAction:
        confirmation_id = uuid.uuid4().hex
        now = time.time()
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        batch = [str(item).strip() for item in (commands or [command]) if str(item).strip()]
        canonical = "\n".join(batch) if batch else command
        # 只有恰好 timeout_seconds==0 表示「不过期」：落成 expires_at=0 哨兵，
        # is_expired / reap_expired 都把它当永不过期，to_dict 原样发 0，前端归一层
        # 同样用 0 表示不过期。负数不是哨兵，仍是「已经过期」的正常时刻——调用方
        # （含测试）靠它构造一条立即到期的审批。
        expires_at = 0.0 if timeout_seconds == 0 else now + timeout_seconds
        action = PendingAction(
            confirmation_id=confirmation_id,
            conversation_id=conversation_id,
            node_id=node_id,
            tool_name=tool_name,
            command=canonical,
            command_hash=hash_command(canonical),
            requester=requester,
            shell_flavor=shell_flavor,
            created_at=now,
            expires_at=expires_at,
            future=future,
            metadata=dict(metadata or {}),
            commands=batch or [command],
        )
        self._pending[confirmation_id] = action
        return action

    def get(self, confirmation_id: str) -> Optional[PendingAction]:
        return self._pending.get((confirmation_id or "").strip())

    def list_pending(self, conversation_id: str) -> list[PendingAction]:
        cid = (conversation_id or "").strip()
        return [a for a in self._pending.values() if a.conversation_id == cid]

    def resolve(self, confirmation_id: str, result: str) -> Optional[PendingAction]:
        """Mark the action resolved and wake the waiting tool.

        Returns the action if it existed and was unresolved, else None (caller
        surfaces 404/409).
        """
        action = self._pending.get((confirmation_id or "").strip())
        if action is None or action.resolved is not None:
            return None
        action.resolved = result
        if not action.future.done():
            action.future.set_result(result)
        return action

    def reap_expired(self) -> list[PendingAction]:
        """Auto-deny every action past its expiry. Called lazily by list/get so a
        timed-out confirmation does not linger as "pending" on a reconnect."""
        now = time.time()
        # is_expired 而不是裸比 expires_at：0 是「不过期」哨兵，裸比会当场把它 reap 掉。
        expired = [a for a in self._pending.values() if a.resolved is None and a.is_expired(now)]
        for action in expired:
            action.resolved = "timeout"
            if not action.future.done():
                action.future.set_result("deny")
        return expired

    def remove(self, confirmation_id: str) -> None:
        self._pending.pop((confirmation_id or "").strip(), None)


# Module-level singleton. The data service is a single process; the agent-loop
# task and the API endpoint share this registry.
approval_registry = ApprovalRegistry()


# Event sink type: an async callable that receives one SSE event dict.
EventSink = Callable[[dict], "asyncio.Awaitable[None]"]


async def emit_confirmation_required(
    emit: Optional[EventSink],
    action: PendingAction,
    message: str,
) -> None:
    """Push a ``confirmation_required`` event onto the SSE stream.

    Best-effort: if the stream is already gone (emit is None or raises), the
    tool still pauses — the admin can re-fetch pending approvals via GET, and
    the timeout will eventually deny it.
    """
    if emit is None:
        return
    event = {
        "type": "confirmation_required",
        "confirmation_id": action.confirmation_id,
        "tool_name": action.tool_name,
        "node_id": action.node_id,
        "shell_flavor": action.shell_flavor,
        "command": action.command,
        "commands": action.commands or [action.command],
        "command_hash": action.command_hash,
        "message": message,
        "expires_at": action.expires_at,
        "metadata": action.metadata,
    }
    try:
        await emit(event)
    except Exception:  # noqa: BLE001 — emit must never break the tool
        pass
