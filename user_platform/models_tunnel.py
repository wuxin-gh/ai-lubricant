"""Tortoise models for the intranet-penetration (tunnel) manager.

A *scheme* is a reusable proxy-server configuration (one frps / cloudflared /
npc server endpoint + its connection info). A *binding* is one exposure: pick a
scheme, land it on a target, expose a local port, get a public address. A
*runtime* is the process-level aggregate: normally one client process per target
+ scheme, with per-binding exceptions for cloudflared quick and npc.

Bindings attach to either a project (``project_id`` set) or stand alone
(``project_id`` null) — never to a task or a session. The public address is
derived from the scheme config at allocation time (port-range allocation for
frpc/npc). Two cloudflared exceptions: quick (trycloudflare) captures its
random domain from the client's stdout at runtime; managed provisions a tunnel
+ DNS record via the Cloudflare API at dispatch time and sets ``public_addr``
to the resolved ``hostname``.

Tables use the ``mc_`` prefix to live in the compatibility app's namespace
alongside the other platform domain models.
"""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class TunnelScheme(Model):
    """A reusable proxy-server configuration (the admin-configured pool).

    ``config`` is a JSONB blob whose shape depends on ``kind``:

    * ``frpc``     — ``{server_addr, server_port, token, port_range:[lo, hi],
      domain?}``; optional domain is the public-facing address, while
      server_addr remains the frps connection endpoint
    * ``cloudflared`` — ``{mode:"quick"}``(trycloudflare 免登录随机域名)或
      ``{mode:"managed", api_token, account_id, zone_id, domain}``(平台全自动:
      建 tunnel/DNS/下发节点;绑定层只填 subdomain)
    * ``npc``      — same addressing contract as frpc
    """

    id = fields.UUIDField(pk=True)
    user_id = fields.UUIDField()  # owner (platform admin who created it)
    team_id = fields.UUIDField(null=True)  # team-scoped isolation (None = global)
    name = fields.CharField(max_length=128)
    kind = fields.CharField(max_length=32)  # frpc | cloudflared | npc
    config = fields.JSONField(default=dict)
    enabled = fields.BooleanField(default=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_tunnel_schemes"
        indexes = (("user_id",), ("team_id",), ("kind",))


class TunnelBinding(Model):
    """One exposure: scheme + target + local port → public address.

    ``project_id`` null means a standalone binding (configured from the node
    page or scheme page). ``node_id`` names its execution target: a real
    execution-node id, or the reserved ``"__main__"`` value for the main service
    machine. Project bindings may initially omit the target at the API boundary;
    the service resolves it to the node held by the project's live task before
    inserting this NOT NULL column.
    """

    id = fields.UUIDField(pk=True)
    scheme_id = fields.UUIDField()
    project_id = fields.UUIDField(null=True)  # null = standalone
    node_id = fields.CharField(max_length=128)  # execution node id | __main__
    user_id = fields.UUIDField()  # who created the binding

    local_host = fields.CharField(max_length=128, default="127.0.0.1")
    local_port = fields.IntField()

    # User-entered note shown in the list (e.g. "线上预览"). Optional, nullable.
    description = fields.TextField(null=True)

    # cloudflared managed: resolved FQDN composed from binding subdomain +
    # scheme domain. frpc/npc/quick do not use this column.
    hostname = fields.CharField(max_length=255, null=True)

    # Public address allocated from the scheme config. Null for cloudflared
    # quick mode until the client reports its trycloudflare domain.
    public_addr = fields.CharField(max_length=255, null=True)
    # Allocated port (frpc/npc), for recycling on close. Null when the binding
    # carries a user-chosen remote_port instead of an auto-allocated one.
    allocated_value = fields.CharField(max_length=128, null=True)
    # User-chosen overrides (frpc/npc, optional): frpc proxy name in the
    # generated config (falls back to "tunnel-{id}") and remote port (falls
    # back to allocated_value from the scheme's port range).
    proxy_name = fields.CharField(max_length=128, null=True)
    remote_port = fields.IntField(null=True)

    # Per-binding provider resources (managed cloudflared: dns_record_id).
    # The shared tunnel_id belongs to TunnelRuntime.provider_ref.
    provider_ref = fields.JSONField(default=dict)

    # User intent. Process status is owned by TunnelRuntime; client_status/run_id
    # remain API compatibility projections during migration.
    desired_state = fields.CharField(max_length=32, default="running")
    # Deletion is asynchronous: the data service marks this flag and the
    # standalone runtime service removes process/provider resources before
    # deleting the row.
    delete_requested = fields.BooleanField(default=False)
    run_id = fields.CharField(max_length=128, null=True)
    client_status = fields.CharField(max_length=32, default="pending")
    error = fields.TextField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_tunnel_bindings"
        indexes = (
            ("scheme_id",),
            ("project_id",),
            ("node_id",),
            ("user_id",),
        )


class TunnelRuntime(Model):
    """One long-lived client process for a target + scheme group.

    Grouped kinds (frpc and cloudflared managed) use one row per
    ``(target_id, scheme_id)``. Per-binding exceptions (cloudflared quick and,
    until its multi-proxy syntax is verified, npc) include the binding id in
    ``runtime_key`` and ``binding_id``.

    ``desired_state`` is control-plane intent; ``runtime_status`` is observed
    process/connection state. ``config_revision`` fences late events from a
    superseded process after a group restart.
    """

    id = fields.UUIDField(pk=True)
    scheme_id = fields.UUIDField()
    target_id = fields.CharField(max_length=128)
    binding_id = fields.UUIDField(null=True)  # per-binding runtime exception
    runtime_key = fields.CharField(max_length=512, unique=True)
    kind = fields.CharField(max_length=32)

    desired_state = fields.CharField(max_length=32, default="running")
    runtime_status = fields.CharField(max_length=32, default="pending")
    run_id = fields.CharField(max_length=128, null=True)
    config_revision = fields.IntField(default=0)
    observed_revision = fields.IntField(default=0)
    pid = fields.IntField(null=True)
    last_heartbeat_at = fields.DatetimeField(null=True)
    ready_at = fields.DatetimeField(null=True)
    error = fields.TextField(null=True)
    stderr_tail = fields.TextField(null=True)
    provider_ref = fields.JSONField(default=dict)  # managed: {tunnel_id}

    # DB lease so that multiple tunnel-server replicas (operator error or
    # rolling restart) never concurrently reconcile the same runtime. Only the
    # owner whose lease_until is still in the future may stop/start; an expired
    # lease is taken over by the next reconciler that reaches it.
    lease_owner = fields.CharField(max_length=128, null=True)
    lease_until = fields.DatetimeField(null=True)

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "mc_tunnel_runtimes"
        indexes = (
            ("scheme_id",),
            ("target_id",),
            ("runtime_status",),
        )
