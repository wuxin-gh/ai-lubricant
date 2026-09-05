"""Service layer for the tunnel manager: scheme + binding CRUD.

A binding's ``node_id`` names its *target* — where the tunnel client process
runs and whose ``local_host:local_port`` gets exposed:

* ``MAIN_SERVICE_TARGET`` (``"__main__"``) — the main service's own machine. The
  client runs as a subprocess of this process, supervised by
  :mod:`.tunnel_supervisor`.
* any other value — that execution node. The client is dispatched to the node
  over the control plane (:mod:`.tunnel_node_dispatch`).

Project-attached bindings may omit ``node_id``: it is resolved to the node the
project's live task currently occupies, since a project's previewable services
run inside that task's node.

Cloudflare tunnel/DNS provisioning for ``managed`` schemes lives in
:mod:`.tunnel_cloudflare` and is shared by both targets.

Scheme configs carry secrets (frp/npc ``token``, cloudflared ``api_token``).
They are masked on the way out (:func:`_public_config`) and a masked value sent
back on update never overwrites the stored secret — same contract as
:mod:`.team_models_service` / :mod:`.git_service`.
"""
from __future__ import annotations

import re
import uuid
from typing import Any

from loguru import logger
from tortoise.expressions import F

from .masking import mask_secret
from .models_tunnel import TunnelBinding, TunnelScheme, TunnelRuntime
from .tunnel_allocator import AllocationError, allocate, release

KINDS = ("frpc", "cloudflared", "npc")

# Reserved ``node_id`` meaning "the main service's own machine" rather than a
# control-plane node. Kept as a sentinel value so the column stays NOT NULL and
# no migration is needed; a real node id can never collide (node ids are
# control-plane assigned and never start with "__").
MAIN_SERVICE_TARGET = "__main__"

# Task statuses that still hold their node (see models_task.Task.status).
_LIVE_TASK_STATUSES = ("pending", "processing")

# Config keys holding a secret: masked in responses, preserved when the client
# echoes the masked form back on update.
_SECRET_KEYS = ("token", "api_token", "credentials")

# A single DNS label: 1-63 chars, alphanumeric + hyphen, no leading/trailing
# hyphen. A domain is one or more labels joined by dots (max 253 total).
_DNS_LABEL = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")


class TunnelServiceError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def _normalize_domain(value: str) -> str:
    """Lower-case + strip a trailing dot, and validate DNS shape.

    Rejects anything that is not a bare domain: a scheme (``https://``), a path
    (``/x``), a port (``:8080``), a wildcard (``*.``), an empty label
    (``a..b``), a bad character, or an over-length name (>253). Returns the
    cleaned domain; raises :class:`TunnelServiceError` on any violation.
    """
    domain = (value or "").strip().lower().rstrip(".")
    if not domain:
        raise TunnelServiceError("invalid_argument", "domain is empty")
    if len(domain) > 253:
        raise TunnelServiceError("invalid_argument", "domain too long")
    if "://" in domain or "/" in domain or ":" in domain or "*" in domain:
        raise TunnelServiceError("invalid_argument", f"invalid domain {value!r}")
    labels = domain.split(".")
    if len(labels) < 2 or not all(_DNS_LABEL.match(lbl) for lbl in labels):
        raise TunnelServiceError("invalid_argument", f"invalid domain {value!r}")
    return domain


def _compose_hostname(subdomain: str, domain: str) -> str:
    """Compose ``<subdomain>.<domain>`` for a cloudflared managed binding.

    ``subdomain`` is a *relative* name (``app`` or ``api.dev``). A value that
    already ends with the scheme's ``domain`` is rejected so a user pasting the
    full FQDN does not yield ``app.example.com.example.com``.
    """
    sub = (subdomain or "").strip().lower().rstrip(".")
    if not sub:
        raise TunnelServiceError("invalid_argument", "subdomain is empty")
    if "://" in sub or "/" in sub or ":" in sub or "*" in sub:
        raise TunnelServiceError("invalid_argument", f"invalid subdomain {subdomain!r}")
    if sub == domain or sub.endswith("." + domain):
        raise TunnelServiceError(
            "invalid_argument",
            f"subdomain {subdomain!r} must be relative (do not include the domain)",
        )
    labels = sub.split(".")
    if not all(_DNS_LABEL.match(lbl) for lbl in labels):
        raise TunnelServiceError("invalid_argument", f"invalid subdomain {subdomain!r}")
    hostname = f"{sub}.{domain}"
    if len(hostname) > 253:
        raise TunnelServiceError("invalid_argument", "hostname too long")
    return hostname


def _validate_scheme_config(kind: str, config: dict[str, Any]) -> None:
    """Reject a scheme config that can never allocate an address.

    Mutates ``config`` in place to store the normalized ``domain`` (lower-cased,
    trailing dot removed) so downstream allocation/dispatch sees the clean value.
    """
    if kind not in KINDS:
        raise TunnelServiceError("invalid_argument", f"unsupported kind {kind!r}")
    config = config or {}
    if kind in ("frpc", "npc"):
        if not (config.get("server_addr") or "").strip():
            raise TunnelServiceError("invalid_argument", "server_addr is required")
        rng = config.get("port_range") or []
        if len(rng) != 2 or not all(isinstance(x, int) for x in rng):
            raise TunnelServiceError("invalid_argument", "port_range must be [lo:int, hi:int]")
        # domain is optional for frpc/npc — a public-facing name for display.
        if str(config.get("domain") or "").strip():
            config["domain"] = _normalize_domain(str(config["domain"]))
    elif kind == "cloudflared":
        mode = (config.get("mode") or "quick").strip()
        if mode not in ("quick", "managed"):
            raise TunnelServiceError("invalid_argument", "cloudflared mode must be quick or managed")
        if mode == "managed":
            missing = [
                k for k in ("api_token", "account_id", "zone_id")
                if not str(config.get(k) or "").strip()
            ]
            if missing:
                raise TunnelServiceError(
                    "invalid_argument", f"cloudflared managed requires {', '.join(missing)}"
                )
            if not str(config.get("domain") or "").strip():
                raise TunnelServiceError("invalid_argument", "cloudflared managed requires domain")
            config["domain"] = _normalize_domain(str(config["domain"]))


def _is_masked(value: str | None) -> bool:
    """A value carrying our masked marker (see :func:`mask_secret`)."""
    return bool(value) and "***" in value


def _public_config(kind: str, config: dict | None) -> dict:
    """Return a copy of config with all secret fields masked.

    Mirrors the contract in :mod:`.team_models_service`: an opaque masked form
    (``***``) so the client can round-trip it back on edit without ever
    receiving or re-sending the cleartext secret.
    """
    config = dict(config or {})
    for key in _SECRET_KEYS:
        if key in config and config[key]:
            config[key] = mask_secret(str(config[key]))
    return config


def _merge_masked_secrets(kind: str, incoming: dict, stored: dict | None) -> dict:
    """Splice stored cleartext secrets into an incoming config update.

    ``incoming`` may carry the masked form (``***``) for secret fields the
    client did not re-enter. Those mean "unchanged" - replace them with the
    stored cleartext so validation sees the real value and the mask never
    overwrites the secret.
    """
    stored = stored or {}
    merged = dict(incoming)
    for key in _SECRET_KEYS:
        value = merged.get(key)
        if value is not None and _is_masked(str(value)) and stored.get(key):
            merged[key] = stored[key]
    return merged


def _scheme_dict(row: TunnelScheme) -> dict:
    return {
        "id": str(row.id),
        "name": row.name,
        "kind": row.kind,
        "config": _public_config(row.kind, row.config),
        "enabled": bool(row.enabled),
        "team_id": str(row.team_id) if row.team_id else None,
        "created_at": int(row.created_at.timestamp()) if row.created_at else 0,
        "updated_at": int(row.updated_at.timestamp()) if row.updated_at else 0,
    }


def _binding_dict(row: TunnelBinding) -> dict:
    return {
        "id": str(row.id),
        "scheme_id": str(row.scheme_id),
        "project_id": str(row.project_id) if row.project_id else None,
        "node_id": row.node_id,
        "local_host": row.local_host,
        "local_port": row.local_port,
        "description": row.description,
        "hostname": row.hostname,
        "public_addr": row.public_addr,
        "allocated_value": row.allocated_value,
        "desired_state": row.desired_state,
        "delete_requested": bool(row.delete_requested),
        "run_id": row.run_id,
        "client_status": row.client_status,
        "error": row.error,
        "created_at": int(row.created_at.timestamp()) if row.created_at else 0,
        "updated_at": int(row.updated_at.timestamp()) if row.updated_at else 0,
    }


# ── schemes ──────────────────────────────────────────────────────────────


async def list_schemes(user_id: str) -> list[dict]:
    rows = await TunnelScheme.filter(user_id=uuid.UUID(user_id)).order_by("-created_at")
    return [_scheme_dict(r) for r in rows]


async def create_scheme(
    user_id: str, *, name: str, kind: str, config: dict, team_id: str | None = None
) -> dict:
    _validate_scheme_config(kind, config)
    row = await TunnelScheme.create(
        user_id=uuid.UUID(user_id),
        team_id=uuid.UUID(team_id) if team_id else None,
        name=name.strip(),
        kind=kind,
        config=config or {},
        enabled=True,
    )
    logger.info("[tunnel] scheme created: id={} kind={} name={}", row.id, kind, name)
    return _scheme_dict(row)


async def update_scheme(
    user_id: str, scheme_id: str, *, name: str | None = None, config: dict | None = None,
    enabled: bool | None = None,
) -> dict:
    row = await TunnelScheme.get_or_none(id=uuid.UUID(scheme_id), user_id=uuid.UUID(user_id))
    if row is None:
        raise TunnelServiceError("not_found", "scheme not found")
    if config is not None:
        # The client round-trips the masked config from list/edit. A secret
        # field echoing our masked form means "unchanged": splice the stored
        # cleartext back in so the mask never overwrites the real secret.
        config = _merge_masked_secrets(row.kind, config, row.config)
        _validate_scheme_config(row.kind, config)
        row.config = config
    if name is not None:
        row.name = name.strip()
    if enabled is not None:
        row.enabled = bool(enabled)
    await row.save()
    # Config or enabled-state change: the runtime service must re-render and
    # restart affected clients. Bump every desired-running runtime's
    # config_revision so the reconciler's liveness guards (in-process is_live
    # revision match + node inventory revision match) treat the running
    # processes as stale and restart them with the new config. Stopped runtimes
    # pick up the new config on next start anyway, so they're left alone. Then
    # wake it; never block on the result here.
    await TunnelRuntime.filter(
        scheme_id=row.id, desired_state="running"
    ).update(config_revision=F("config_revision") + 1)
    from .tunnel_notify import notify_tunnel_changed

    await notify_tunnel_changed(target_id=None)
    return _scheme_dict(row)


async def delete_scheme(user_id: str, scheme_id: str) -> dict:
    row = await TunnelScheme.get_or_none(id=uuid.UUID(scheme_id), user_id=uuid.UUID(user_id))
    if row is None:
        raise TunnelServiceError("not_found", "scheme not found")
    # Refuse to delete a scheme that still has live (non-deleted) bindings —
    # caller must close them first. Soft-deleted rows pending teardown are
    # ignored so a delete racing the reclaimer does not 409.
    live = await TunnelBinding.filter(
        scheme_id=row.id, delete_requested=False,
        client_status__in=("pending", "running"),
    ).count()
    if live:
        raise TunnelServiceError(
            "in_use", f"scheme has {live} live binding(s); close them first"
        )
    await row.delete()
    return {"deleted": True}


# ── bindings ─────────────────────────────────────────────────────────────


async def list_bindings(
    user_id: str,
    *,
    project_id: str | None = None,
    node_id: str | None = None,
    scheme_id: str | None = None,
) -> list[dict]:
    qs = TunnelBinding.filter(user_id=uuid.UUID(user_id))
    # Hide rows pending async deletion (runtime service has not reclaimed them
    # yet). They reappear as "deleted" only in direct fetches, not listings.
    qs = qs.filter(delete_requested=False)
    if project_id:
        qs = qs.filter(project_id=uuid.UUID(project_id))
    if node_id:
        qs = qs.filter(node_id=node_id)
    if scheme_id:
        qs = qs.filter(scheme_id=uuid.UUID(scheme_id))
    rows = await qs.order_by("-created_at")
    return [_binding_dict(r) for r in rows]


async def _resolve_project_node(project_id: str) -> str:
    """The node a project's live task currently occupies.

    A project has no node of its own — its previewable services run inside the
    node its running task holds, so that node is the only meaningful tunnel
    target for a project-attached binding. Prefers the exclusive
    :class:`TaskNodeBinding` record (one node ↔ one task) and falls back to the
    task row's own ``node_id``.

    Raises :class:`TunnelServiceError` when the project has no live task, since
    there is then no node to expose.
    """
    from .models_task import ProjectTask, Task, TaskNodeBinding

    task_ids = [
        row.task_id
        for row in await ProjectTask.filter(project_id=uuid.UUID(project_id)).only("task_id")
    ]
    if task_ids:
        live = (
            await Task.filter(id__in=task_ids, status__in=_LIVE_TASK_STATUSES)
            .order_by("-last_active_at")
            .only("id", "node_id")
        )
        for task in live:
            lease = await TaskNodeBinding.get_or_none(task_id=task.id)
            node_id = (lease.node_id if lease else None) or (task.node_id or "")
            if node_id.strip():
                return node_id.strip()
    raise TunnelServiceError(
        "invalid_argument",
        "项目当前没有运行中的任务,无法确定穿透节点;请先启动任务,或在方案页指定目标",
    )


async def create_binding(
    user_id: str,
    *,
    scheme_id: str,
    node_id: str | None,
    local_port: int,
    local_host: str = "127.0.0.1",
    subdomain: str | None = None,
    project_id: str | None = None,
    description: str | None = None,
) -> dict:
    """Create a binding and return pending; the runtime service starts the client.

    frpc/npc allocate a port from their scheme range up front (the public
    address prefers the scheme's optional ``domain``). Cloudflared quick
    obtains its random address from client stdout; cloudflared managed composes
    ``<subdomain>.<scheme.domain>`` and provisions its tunnel/DNS on the
    runtime side. The HTTP response is always ``pending`` — actual process
    start, Cloudflare API, node RPC, and binary download happen asynchronously
    in the standalone runtime service, never in this request.
    """
    scheme = await TunnelScheme.get_or_none(
        id=uuid.UUID(scheme_id), enabled=True, user_id=uuid.UUID(user_id)
    )
    if scheme is None:
        # 兜底：scheme 可能是团队成员共享的（team_id 匹配），但 list_schemes 只返自己，
        # 此处先按 owner 严格收口；后续如需团队共享 scheme 再放宽。
        raise TunnelServiceError("not_found", "scheme not found or not accessible")

    config = scheme.config or {}
    hostname: str | None = None
    public_addr_override: str | None = None
    if scheme.kind == "cloudflared" and (config.get("mode") or "quick").strip() == "managed":
        domain = str(config.get("domain") or "").strip().lower().rstrip(".")
        if not domain:
            raise TunnelServiceError(
                "invalid_argument",
                "scheme has no domain configured; edit the scheme and set its root domain first",
            )
        if str(config.get("zone_id") or "").strip() == "":
            raise TunnelServiceError("invalid_argument", "cloudflared managed scheme missing zone_id")
        hostname = _compose_hostname(subdomain or "", domain)
        # A managed tunnel's public address is the user-chosen hostname behind
        # Cloudflare's TLS-terminating edge. It is fully determined at create
        # time, so surface it now — otherwise the list shows "地址生成中…" until
        # the runtime happens to provision DNS, which can take seconds or fail.
        public_addr_override = f"https://{hostname}"

    try:
        alloc = await allocate(scheme)
    except AllocationError as exc:
        raise TunnelServiceError("allocation_failed", str(exc)) from exc

    resolved_node = (node_id or "").strip()
    if not resolved_node:
        if project_id:
            resolved_node = await _resolve_project_node(project_id)
        else:
            raise TunnelServiceError(
                "invalid_argument", "node_id is required for a standalone tunnel"
            )

    row = await TunnelBinding.create(
        user_id=uuid.UUID(user_id),
        scheme_id=scheme.id,
        project_id=uuid.UUID(project_id) if project_id else None,
        node_id=resolved_node,
        local_host=local_host or "127.0.0.1",
        local_port=int(local_port),
        description=(description or "").strip() or None,
        hostname=hostname,
        public_addr=public_addr_override or alloc.get("public_addr"),
        allocated_value=alloc.get("allocated_value"),
        desired_state="running",
        client_status="pending",
    )
    logger.info(
        "[tunnel] binding created: id={} scheme={} node={} local={}:{} hostname={} public={}",
        row.id, scheme.kind, resolved_node, row.local_host, row.local_port, hostname, row.public_addr,
    )
    # Wake the runtime service to start the client. Never block the HTTP
    # request on process spawn / Cloudflare API / node RPC — return pending.
    from .tunnel_notify import notify_tunnel_changed

    await notify_tunnel_changed(
        runtime_id=str(row.id), target_id=resolved_node
    )
    return _binding_dict(row)


async def update_binding(
    user_id: str,
    binding_id: str,
    *,
    scheme_id: str | None,
    node_id: str | None,
    local_port: int,
    local_host: str = "127.0.0.1",
    subdomain: str | None = None,
    description: str | None = None,
) -> dict:
    """Edit a binding's mutable fields; runtime reconciles asynchronously.

    Immutable: ``scheme_id`` (runtime groups by target×scheme, so cross-scheme
    migration is delete+create). Mutable: target node, local host/port, and the
    cloudflared managed subdomain. A managed subdomain change rebuilds that
    binding's DNS record on the next runtime reconcile: we drop the old
    ``dns_record_id`` here so ``_prepare_managed`` creates a fresh one for the
    new hostname. Never blocks the HTTP request on Cloudflare or process restart.
    """
    row = await TunnelBinding.get_or_none(
        id=uuid.UUID(binding_id), user_id=uuid.UUID(user_id)
    )
    if row is None:
        raise TunnelServiceError("not_found", "binding not found")
    if scheme_id is not None and scheme_id != str(row.scheme_id):
        # Cross-scheme migration would have to tear down the old runtime group
        # and provision a new one; require delete+create instead of a silent
        # re-parent that orphans the old runtime row.
        raise TunnelServiceError(
            "invalid_argument", "scheme cannot be changed; delete and recreate"
        )
    scheme = await TunnelScheme.get_or_none(id=row.scheme_id)
    if scheme is None:
        raise TunnelServiceError("not_found", "scheme not found")

    resolved_node = (node_id or "").strip()
    if not resolved_node:
        if row.project_id:
            resolved_node = await _resolve_project_node(str(row.project_id))
        else:
            raise TunnelServiceError(
                "invalid_argument", "node_id is required for a standalone tunnel"
            )

    # cloudflared managed: a new subdomain composes a new hostname. If the
    # hostname actually changed, the old DNS record is stale — drop it (best
    # effort) and clear the stored id so the runtime re-creates one for the
    # new hostname on reconcile.
    new_hostname = row.hostname
    if scheme.kind == "cloudflared" and (scheme.config or {}).get("mode") == "managed":
        domain = str((scheme.config or {}).get("domain") or "").strip().lower().rstrip(".")
        if not domain:
            raise TunnelServiceError(
                "invalid_argument",
                "scheme has no domain configured; edit the scheme and set its root domain first",
            )
        new_hostname = _compose_hostname(subdomain or "", domain)
        # Keep the advertised address in sync with the (possibly new) hostname.
        row.public_addr = f"https://{new_hostname}"
        if new_hostname != row.hostname:
            await _drop_binding_dns(row, scheme)

    row.node_id = resolved_node
    row.local_host = local_host or "127.0.0.1"
    row.local_port = int(local_port)
    row.description = (description or "").strip() or None
    row.hostname = new_hostname
    row.desired_state = "running"
    row.client_status = "pending"
    row.error = None
    await row.save(update_fields=[
        "node_id", "local_host", "local_port", "hostname", "public_addr",
        "description", "desired_state", "client_status", "error", "updated_at",
    ])
    logger.info(
        "[tunnel] binding updated: id={} node={} local={}:{} hostname={}",
        row.id, resolved_node, row.local_host, row.local_port, row.hostname,
    )
    from .tunnel_notify import notify_tunnel_changed

    await notify_tunnel_changed(
        runtime_id=str(row.id), target_id=resolved_node
    )
    return _binding_dict(row)


async def _drop_binding_dns(row: TunnelBinding, scheme: TunnelScheme) -> None:
    """Best-effort delete a managed binding's DNS record so a hostname change
    can re-create it. Idempotent: a missing record id or a failed delete does
    not block the edit; the stale record lingers on Cloudflare but the new
    hostname still gets a fresh one.
    """
    if not row.provider_ref:
        return
    dns_record_id = str((row.provider_ref or {}).get("dns_record_id") or "")
    if not dns_record_id:
        return
    config = scheme.config or {}
    try:
        from . import tunnel_cloudflare as cf

        await cf.delete_dns_record(
            api_token=str(config.get("api_token") or ""),
            zone_id=str(config.get("zone_id") or ""),
            dns_record_id=dns_record_id,
        )
    except Exception as exc:  # noqa: BLE001 — DNS reclaim must not block the edit
        logger.warning("[tunnel] dns drop on update failed for {}: {}", row.id, exc)
    # Clear the id so the runtime re-creates a record for the new hostname.
    row.provider_ref = {k: v for k, v in (row.provider_ref or {}).items() if k != "dns_record_id"}
    await row.save(update_fields=["provider_ref", "updated_at"])


async def start_binding(user_id: str, binding_id: str) -> dict:
    """Set one binding desired-running and return pending; runtime reconciles."""
    row = await TunnelBinding.get_or_none(
        id=uuid.UUID(binding_id), user_id=uuid.UUID(user_id)
    )
    if row is None:
        raise TunnelServiceError("not_found", "binding not found")
    scheme = await TunnelScheme.get_or_none(id=row.scheme_id, enabled=True)
    if scheme is None:
        raise TunnelServiceError("not_found", "scheme not found or disabled")
    row.desired_state = "running"
    row.client_status = "pending"
    row.error = None
    await row.save(update_fields=["desired_state", "client_status", "error", "updated_at"])
    from .tunnel_notify import notify_tunnel_changed

    await notify_tunnel_changed(
        runtime_id=str(row.id), target_id=row.node_id
    )
    return _binding_dict(row)


async def stop_binding_service(user_id: str, binding_id: str) -> dict:
    """Set one binding desired-stopped and return pending; runtime reconciles."""
    row = await TunnelBinding.get_or_none(
        id=uuid.UUID(binding_id), user_id=uuid.UUID(user_id)
    )
    if row is None:
        raise TunnelServiceError("not_found", "binding not found")
    scheme = await TunnelScheme.get_or_none(id=row.scheme_id)
    if scheme is None:
        raise TunnelServiceError("not_found", "scheme not found")
    row.desired_state = "stopped"
    row.client_status = "pending"
    row.error = None
    await row.save(update_fields=["desired_state", "client_status", "error", "updated_at"])
    from .tunnel_notify import notify_tunnel_changed

    await notify_tunnel_changed(
        runtime_id=str(row.id), target_id=row.node_id
    )
    return _binding_dict(row)


async def delete_binding(user_id: str, binding_id: str) -> dict:
    """Mark a binding delete-requested and return pending.

    Actual reclamation — stop the group process, delete the binding's DNS
    record, drop the empty runtime's tunnel, delete the row — happens in the
    standalone runtime service. The row disappears from list responses
    immediately; the worker finishes the teardown asynchronously.
    """
    row = await TunnelBinding.get_or_none(id=uuid.UUID(binding_id), user_id=uuid.UUID(user_id))
    if row is None:
        raise TunnelServiceError("not_found", "binding not found")
    row.desired_state = "stopped"
    row.delete_requested = True
    row.client_status = "pending"
    row.error = None
    await row.save(update_fields=[
        "desired_state", "delete_requested", "client_status", "error", "updated_at"
    ])
    from .tunnel_notify import notify_tunnel_changed

    await notify_tunnel_changed(
        runtime_id=str(row.id), target_id=row.node_id
    )
    return {"deleted": False, "pending": True, "id": str(row.id)}


async def _reclaim_provider_resources(
    row: TunnelBinding, *, scheme: TunnelScheme | None = None
) -> None:
    """Delete provider-side resources owned by this binding.

    Managed cloudflared bindings own their DNS record; the shared tunnel belongs
    to TunnelRuntime and is retained while the runtime group exists.
    """
    if not row.provider_ref:
        return
    scheme = scheme or await TunnelScheme.get_or_none(id=row.scheme_id)
    if scheme is None or scheme.kind != "cloudflared":
        return
    config = scheme.config or {}
    try:
        from . import tunnel_cloudflare as cf

        dns_record_id = str((row.provider_ref or {}).get("dns_record_id") or "")
        if dns_record_id:
            await cf.delete_dns_record(
                api_token=str(config.get("api_token") or ""),
                zone_id=str(config.get("zone_id") or ""),
                dns_record_id=dns_record_id,
            )
        logger.info("[tunnel] reclaimed cloudflare DNS for binding={}", row.id)
    except Exception as exc:  # noqa: BLE001 — reclaim must not block deletion
        logger.warning("[tunnel] cloudflare reclaim failed for {}: {}", row.id, exc)
