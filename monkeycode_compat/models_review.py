"""Review node lease slots for webhook review concurrency control.

Unlike ``TaskNodeBinding`` (one task pinned to one node for its whole life), a
review task occupies a *short-lived slot* on a node so several reviews can share
a node's compute up to a configured capacity. A lease row exists only while the
review holds the slot: it is created when a review event claims the slot and
deleted when the review finishes, fails, or is cancelled. ``unique_together``
on ``(node_id, slot)`` is what makes concurrency safe — two events can never
oversubscribe a node's capacity, even racing. History lives on
``ProjectWebhookEvent`` (task id, findings, summary), not here.
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class ReviewNodeLease(Model):
    """A held review concurrency slot.

    ``slot`` is an integer in ``[1, capacity]`` on the node. ``status`` is
    ``active`` for the lifetime of the row — it exists so a crashed worker's
    leases can be identified and reclaimed at boot.

    ``event_id`` ties the lease to the webhook event and ``task_id`` to the
    created review task, so the completion hook can release the exact slot.
    """

    id = fields.UUIDField(pk=True)
    # Node ids are opaque strings from the node ledger (see TaskNodeBinding),
    # not UUIDs — keep them textual so any node naming scheme works.
    node_id = fields.CharField(max_length=128)
    project_id = fields.UUIDField(null=True)
    event_id = fields.UUIDField()
    task_id = fields.UUIDField(null=True)
    slot = fields.IntField()
    status = fields.CharField(max_length=16, default="active")
    # Node session id from the dispatch reply. Reviews do not take the exclusive
    # ``TaskNodeBinding`` (that table's unique(node_id) would cap a node at one
    # review), so the lease is where the session id lives — it is what lets
    # stop/delete tear the review's node session down.
    session_id = fields.CharField(max_length=128, null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "mc_review_node_leases"
        unique_together = (("node_id", "slot"), ("event_id",))
        indexes = (("node_id",), ("event_id",), ("status",), ("task_id",), ("project_id",))
