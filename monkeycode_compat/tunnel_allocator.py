"""Address allocation for tunnel bindings.

For frpc / npc the public address is fully determined by the scheme config:
the scheme points at a fixed server (addr + port) and owns a port range; a
binding just claims one free port from that range, so ``public_addr`` =
``server_addr:port`` and no runtime extraction is needed.

For cloudflared both modes defer public-address resolution to dispatch:

* ``quick`` (trycloudflare) receives a random ``*.trycloudflare.com`` domain
  from the running client; the dispatcher captures it from stdout.
* ``managed`` uses the hostname explicitly entered on the binding; the
  dispatcher provisions its tunnel + DNS record through the Cloudflare API.
"""
from __future__ import annotations

from tortoise.exceptions import IntegrityError

from .models_tunnel import TunnelBinding, TunnelScheme


class AllocationError(Exception):
    """Raised when no address can be allocated from a scheme's pool."""


def _port_range(config: dict) -> tuple[int, int]:
    rng = config.get("port_range") or []
    if len(rng) != 2:
        raise AllocationError("scheme config missing port_range [lo, hi]")
    lo, hi = int(rng[0]), int(rng[1])
    if lo <= 0 or hi < lo:
        raise AllocationError(f"invalid port_range [{lo}, {hi}]")
    return lo, hi


async def _used_ports(scheme_id: str) -> set[int]:
    """Ports already claimed by live bindings of this scheme."""
    rows = await TunnelBinding.filter(
        scheme_id=scheme_id, allocated_value__not_isnull=True
    ).only("allocated_value")
    out: set[int] = set()
    for row in rows:
        try:
            out.add(int(row.allocated_value))
        except (TypeError, ValueError):
            continue
    return out


async def allocate(scheme: TunnelScheme) -> dict:
    """Allocate a public address + recycling value for a new binding.

    Returns ``{"public_addr": str|None, "allocated_value": str|None}``. A None
    public_addr means it is back-filled later (cloudflared quick mode).

    Raises AllocationError if the scheme's pool is exhausted or misconfigured.
    """
    config = scheme.config or {}
    kind = (scheme.kind or "").strip()

    if kind == "frpc" or kind == "npc":
        server_addr = (config.get("server_addr") or "").strip()
        if not server_addr:
            raise AllocationError("scheme config missing server_addr")
        # ``server_addr`` is where the client connects to frps/nps; ``domain``
        # is an optional public-facing name for users. It affects only the
        # advertised address, never the generated client config.
        public_host = (config.get("domain") or "").strip() or server_addr
        lo, hi = _port_range(config)
        used = await _used_ports(str(scheme.id))
        for port in range(lo, hi + 1):
            if port in used:
                continue
            # Claim it. The unique guard is the allocated_value + scheme pair;
            # rely on the binding insert + a re-check, but to keep this
            # allocation atomic we persist a placeholder binding with this
            # allocated_value. Callers create the real binding row themselves,
            # so here we just return the chosen port — the caller's insert is
            # the atomic claim, and we re-check used ports just above.
            return {
                "public_addr": f"{public_host}:{port}",
                "allocated_value": str(port),
            }
        raise AllocationError(f"port range [{lo},{hi}] exhausted for scheme {scheme.id}")

    if kind == "cloudflared":
        # Neither cloudflared mode allocates an address here:
        #   quick   — the random trycloudflare domain is minted by the client at
        #             runtime and captured from its stdout.
        #   managed — the public hostname is the user's own (on the binding) and
        #             the tunnel + DNS record are provisioned by the dispatcher
        #             through the Cloudflare API.
        mode = (config.get("mode") or "quick").strip()
        if mode not in ("quick", "managed"):
            raise AllocationError(f"unknown cloudflared mode {mode!r}")
        return {"public_addr": None, "allocated_value": None}

    raise AllocationError(f"unsupported scheme kind {kind!r}")


async def release(binding: TunnelBinding) -> None:
    """Free the allocated value of a binding (no-op for cloudflared quick).

    Used on close/delete so the port/subdomain returns to the pool. The binding
    row itself is deleted by the caller; this only clears in-flight state if
    the binding was never persisted.
    """
    # No persistent pool structure to mutate — allocation reads live bindings,
    # so deleting the binding row IS the release. Kept as a hook for future
    # reserved/leased pools.
    return None
