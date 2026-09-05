"""Review-node capability gating and slot-lease scheduling.

Two responsibilities:

1. **Capability gating** — given a project's review *framework* + chosen *editor
   provider*, decide which of the user's execution nodes can run the review
   (base framework tools + the editor CLI must all be installed, and the node
   must be online / approved / execution-role).
2. **Slot-lease scheduling** — pick a node from the candidate pool that still
   has a free review slot (``ReviewNodeLease``), honouring the per-node review
   capacity. Unlike ``TaskNodeBinding`` (one task per node for its lifetime), a
   review lease is a short-lived slot so a node can host several reviews up to
   its capacity.
"""
from __future__ import annotations

import uuid

from .models_review import ReviewNodeLease
from .nodes_service import nodes_service
from .review_executors import (
    capability_matrix,
    get_review_framework,
    missing_capabilities,
)

# Fallback per-node review capacity when the node advertises none.
_DEFAULT_REVIEW_CAPACITY = 1


def _to_uuid(value: str | None) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _node_labels(node: dict) -> dict:
    caps = node.get("capabilities") or {}
    # Node service currently flattens NodeCapabilities.Labels into capabilities,
    # but tolerate a nested labels object during rolling upgrades.
    labels = dict(caps.get("labels") or {}) if isinstance(caps, dict) else {}
    if isinstance(caps, dict):
        labels.update({k: v for k, v in caps.items() if not isinstance(v, (dict, list))})
    return labels


def _online_approved_execution(node: dict) -> tuple[bool, str]:
    if node.get("display_only"):
        return False, "display_only"
    if (node.get("node_role") or node.get("role") or "") != "execution":
        return False, "not_execution"
    if node.get("status") != "approved":
        return False, "not_approved"
    if not bool(node.get("online", node.get("connected", False))):
        return False, "offline"
    return True, ""


def _review_capacity(node: dict) -> int:
    """Number of concurrent review slots the node offers.

    Prefers an explicit ``review_capacity`` label/capacity value; falls back to
    the node's ``max_sessions`` capacity, then a conservative default of 1.
    """
    labels = _node_labels(node)
    for source in (labels.get("review_capacity"), (node.get("capacity") or {}).get("max_sessions")):
        try:
            value = int(source)
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue
    return _DEFAULT_REVIEW_CAPACITY


class ReviewNodeService:
    def base_availability(self, node: dict) -> tuple[bool, str]:
        """Shared node-status verdict reused across user-side editor pickers.

        Mirrors the constants in ``_online_approved_execution`` so the
        review/editor availability verdict matches what other user-side
        consumers (create task, task input) show. Returns ``(ok, reason)``;
        ``reason`` is empty when the node is usable.
        """
        return _online_approved_execution(node)

    async def list_review_nodes(
        self, user_id: str, framework_name: str, editor_provider: str
    ) -> dict:
        framework = get_review_framework(framework_name)
        if framework is None:
            raise ValueError("unsupported_review_framework")
        provider = (editor_provider or "").strip().lower()
        if provider and provider not in framework.supported_editors:
            raise ValueError("unsupported_review_editor")
        result = await nodes_service.list_my_nodes(user_id)
        nodes = []
        for node in result.get("nodes") or []:
            labels = _node_labels(node)
            base_ok, reason = _online_approved_execution(node)
            missing = missing_capabilities(labels, framework_name, provider)
            capacity = _review_capacity(node)
            used = await self._active_leases(str(node.get("node_id") or ""))
            full = used >= capacity
            ready = base_ok and not missing
            schedulable = ready and not full
            nodes.append({
                **node,
                "review_capabilities": capability_matrix(labels, framework_name, provider),
                "review_missing": missing,
                "review_capacity": capacity,
                "review_in_use": used,
                "review_ready": ready,
                "review_schedulable": schedulable,
                "review_reason": reason or (
                    "capacity_full" if full else "missing_capabilities" if missing else ""
                ),
            })
        return {
            "framework": {
                "name": framework.name,
                "label": framework.label,
                "description": framework.description,
                "base_tools": list(framework.base_tools),
                "supported_editors": list(framework.supported_editors),
            },
            "editor_provider": provider,
            "nodes": nodes,
        }

    async def _active_leases(self, node_id: str) -> int:
        if not node_id:
            return 0
        return await ReviewNodeLease.filter(node_id=node_id, status="active").count()

    async def check_node_for_review(
        self, user_id: str, node_id: str, framework_name: str, editor_provider: str
    ) -> dict:
        if get_review_framework(framework_name) is None:
            raise ValueError("unsupported_review_framework")
        visible = await self.list_review_nodes(user_id, framework_name, editor_provider)
        node = next(
            (item for item in visible["nodes"] if str(item.get("node_id")) == str(node_id)),
            None,
        )
        if node is None:
            return {
                "ok": False,
                "node_id": node_id,
                "missing": [],
                "reason": "node_forbidden",
                "capabilities": {},
            }
        return {
            "ok": bool(node["review_ready"]),
            "node_id": node_id,
            "missing": list(node["review_missing"]),
            "reason": node["review_reason"],
            "capabilities": node["review_capabilities"],
            "node": node,
        }

    async def validate_candidate_nodes(
        self,
        user_id: str,
        node_ids: list[str],
        framework_name: str,
        editor_provider: str,
        *,
        require_ready: bool,
    ) -> list[dict]:
        unique = list(dict.fromkeys(str(value).strip() for value in node_ids if str(value).strip()))
        if require_ready and not unique:
            return [{"node_id": "", "reason": "nodes_required", "missing": []}]
        failures: list[dict] = []
        for node_id in unique:
            check = await self.check_node_for_review(
                user_id, node_id, framework_name, editor_provider
            )
            if require_ready and not check["ok"]:
                failures.append({
                    "node_id": node_id,
                    "reason": check["reason"],
                    "missing": check["missing"],
                    "capabilities": check["capabilities"],
                })
            elif check["reason"] == "node_forbidden":
                failures.append({"node_id": node_id, "reason": "node_forbidden", "missing": []})
        return failures

    async def acquire_review_slot(
        self,
        user_id: str,
        candidate_node_ids: list[str],
        framework_name: str,
        editor_provider: str,
        *,
        event_id: str,
        project_id: str | None = None,
    ) -> dict | None:
        """Pick a capable node with a free slot and lease it for ``event_id``.

        Returns ``{"node": <node dict>, "lease_id": ..., "slot": ...}`` or None
        when no candidate has both capability and a free slot. The lease row is
        the concurrency guard: ``unique_together (node_id, slot)`` means two
        events can never grab the same slot even under a race.
        """
        visible = await self.list_review_nodes(user_id, framework_name, editor_provider)
        candidates = set(str(value) for value in candidate_node_ids)
        ready = [
            node for node in visible["nodes"]
            if str(node.get("node_id")) in candidates and node.get("review_ready")
        ]
        # Least-loaded first: fewest active leases, then fewest sessions, then id.
        ready.sort(key=lambda item: (
            int(item.get("review_in_use") or 0),
            int(item.get("active_sessions") or 0),
            str(item.get("node_id") or ""),
        ))
        for node in ready:
            node_id = str(node.get("node_id") or "")
            if not node_id:
                continue
            # Authoritative permission check at selection time; list_my_nodes is
            # a display aggregate, this is the anti-horizontal-access guard.
            if await nodes_service.user_can_use_node(user_id, node_id) is None:
                continue
            capacity = int(node.get("review_capacity") or _DEFAULT_REVIEW_CAPACITY)
            lease = await self._try_lease(node_id, capacity, event_id, project_id)
            if lease is not None:
                return {"node": node, "lease_id": str(lease.id), "slot": lease.slot}
        return None

    async def project_inflight(self, project_id: str) -> int:
        """Active review leases for a project — the real in-flight count.

        A review holds its slot from dispatch until ``complete_review`` (or
        release on failure), so active leases are the accurate concurrency
        signal, unlike the transient ``processing`` event status.
        """
        if not project_id:
            return 0
        return await ReviewNodeLease.filter(
            project_id=project_id, status="active"
        ).count()

    async def _try_lease(
        self, node_id: str, capacity: int, event_id: str, project_id: str | None
    ) -> ReviewNodeLease | None:
        """Grab the first free slot ``[1, capacity]`` on ``node_id``.

        Relies on the ``(node_id, slot)`` unique constraint to make concurrent
        acquisition safe: a losing racer hits IntegrityError and tries the next
        slot. Reuses an existing active lease if this event already holds one.
        """
        existing = await ReviewNodeLease.get_or_none(event_id=event_id)
        if existing is not None:
            if existing.status == "active":
                return existing
            # A released lease for this event: reactivating would collide with
            # the (event_id) unique constraint, so treat it as consumed.
            return None
        taken = {
            row.slot for row in await ReviewNodeLease.filter(node_id=node_id, status="active")
        }
        from tortoise.exceptions import IntegrityError

        lease_project = _to_uuid(project_id)
        for slot in range(1, capacity + 1):
            if slot in taken:
                continue
            try:
                return await ReviewNodeLease.create(
                    id=uuid.uuid4(),
                    node_id=node_id,
                    project_id=lease_project,
                    event_id=event_id,
                    slot=slot,
                    status="active",
                )
            except IntegrityError:
                # Lost the race for this slot (or event already leased); retry.
                dup = await ReviewNodeLease.get_or_none(event_id=event_id, status="active")
                if dup is not None:
                    return dup
                continue
        return None

    async def bind_lease_task(self, lease_id: str, task_id: str) -> None:
        lease = await ReviewNodeLease.get_or_none(id=lease_id)
        if lease is not None and lease.status == "active":
            lease.task_id = uuid.UUID(str(task_id))
            await lease.save(update_fields=["task_id"])

    async def release_review_slot(
        self, *, event_id: str | None = None, lease_id: str | None = None
    ) -> bool:
        """Release a review slot so the node can host another review.

        The lease is a concurrency guard, not an audit log — the event row
        already records task/findings for history — so the row is deleted to
        free the ``(node_id, slot)`` unique constraint for reuse. ``True`` when a
        slot was actually freed.
        """
        lease = None
        if lease_id:
            lease = await ReviewNodeLease.get_or_none(id=lease_id, status="active")
        elif event_id:
            lease = await ReviewNodeLease.get_or_none(event_id=event_id, status="active")
        if lease is None:
            return False
        await lease.delete()
        return True


review_node_service = ReviewNodeService()

__all__ = ["review_node_service", "ReviewNodeService"]
