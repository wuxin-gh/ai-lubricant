"""Cloudflare tunnel + DNS provisioning for ``cloudflared`` managed schemes.

A ``managed`` cloudflared scheme stores a Cloudflare API token, account id, and
zone id. When a binding is dispatched we provision, via the Cloudflare v4 API:

1. a **remotely-managed tunnel** (``config_src: cloudflare``) so its ingress
   rules live in Cloudflare, not in a local config file on the node;
2. that tunnel's **connector token** — passed to ``cloudflared tunnel run`` on
   the node via the ``TUNNEL_TOKEN`` env var (never on argv);
3. the tunnel's **ingress config** routing the user-chosen ``hostname`` to the
   node's ``http://local_host:local_port``;
4. a **DNS CNAME** ``hostname -> <tunnel_id>.cfargotunnel.com`` (proxied) so the
   public hostname resolves to the tunnel.

On binding close the DNS record and tunnel are deleted (:func:`delete_managed_tunnel`).

The API token is a secret: it lives only in the scheme config (masked in
responses) and is used transiently here. The connector token is likewise never
persisted — it is re-fetched from the tunnel on demand.

All calls go through :mod:`aiohttp` (already a dependency; see
``git_clients.py``) and raise :class:`CloudflareError` on any non-2xx, with the
token kept out of the message.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import quote

import aiohttp
from loguru import logger

_API_BASE = "https://api.cloudflare.com/client/v4"
_HTTP_TIMEOUT = 30.0
_CFARGO_SUFFIX = "cfargotunnel.com"


class CloudflareError(Exception):
    """A Cloudflare API call failed (network error or non-2xx response).

    ``status`` is the HTTP status when the failure came from a response, and
    ``None`` for transport-level failures — callers use it to tell a
    deterministic permission/conflict error (retrying never helps) from a
    transient network/5xx one.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def is_permanent_error(exc: Exception) -> bool:
    """Would retrying this failure ever succeed without admin action?

    401/403/404 are auth/permission/not-found — deterministic. 409 is only
    surfaced when a *user-created* record holds the hostname (our own 409s are
    reclaimed inside the create helpers and never escape). Everything else
    (network errors, 5xx, 429) is transient and worth retrying.
    """
    status = getattr(exc, "status", None)
    if status in (401, 403, 404, 409):
        return True
    return "occupied by a user-created" in str(exc)


def _headers(api_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


def _unwrap(body: Any, context: str) -> Any:
    """Return the ``result`` of a Cloudflare v4 envelope or raise.

    Cloudflare wraps every response in ``{success, errors, result, ...}``. A
    2xx with ``success: false`` still means failure; surface its error list.
    """
    if not isinstance(body, dict):
        raise CloudflareError(f"{context}: unexpected response shape")
    if not body.get("success", False):
        errors = body.get("errors") or []
        raise CloudflareError(f"{context}: {errors}")
    return body.get("result")


async def _request(
    method: str, url: str, *, api_token: str, json_body: dict | None = None
) -> Any:
    """Issue one Cloudflare API call and return the unwrapped ``result``.

    The API token never appears in a raised message (only the method + URL).
    """
    timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(
                method, url, headers=_headers(api_token), json=json_body
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise CloudflareError(
                        f"{method} {url} returned HTTP {resp.status}: {text[:300]}",
                        status=resp.status,
                    )
                try:
                    body = await resp.json(content_type=None)
                except Exception as exc:  # noqa: BLE001
                    raise CloudflareError(f"{method} {url}: invalid JSON response") from exc
    except aiohttp.ClientError as exc:  # network/DNS/TLS failures
        raise CloudflareError(f"{method} {url} failed: {exc}") from exc
    return _unwrap(body, f"{method} {url}")


async def get_managed_tunnel_token(
    *, api_token: str, account_id: str, tunnel_id: str
) -> str:
    """Fetch a remotely-managed tunnel's connector token for restart."""
    token = await _request(
        "GET",
        f"{_API_BASE}/accounts/{account_id}/cfd_tunnel/{tunnel_id}/token",
        api_token=api_token,
    )
    if not isinstance(token, str) or not token:
        raise CloudflareError("get tunnel token: empty token")
    return token


async def configure_managed_tunnel(
    *,
    api_token: str,
    account_id: str,
    tunnel_id: str,
    ingress: list[dict],
) -> None:
    """Replace a remotely-managed tunnel's complete ingress configuration."""
    rules = [dict(rule) for rule in ingress]
    if not rules or rules[-1].get("service") != "http_status:404":
        rules.append({"service": "http_status:404"})
    await _request(
        "PUT",
        f"{_API_BASE}/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations",
        api_token=api_token,
        json_body={"config": {"ingress": rules}},
    )


# Every DNS record we create carries this comment. It marks ownership: when a
# create finds the hostname taken, records bearing this comment (or pointing at
# our tunnel) are ours to delete and recreate; anything else was created by the
# zone's admin by hand and must never be touched — surface an error instead.
_MANAGED_COMMENT = "mc-tunnel-managed"


async def create_dns_record(
    *, api_token: str, zone_id: str, tunnel_id: str, hostname: str
) -> str:
    """Create one proxied CNAME for a binding and return its record id.

    The record is tagged with :data:`_MANAGED_COMMENT` so a later collision can
    be resolved by ownership:

    * create fails, a record with our comment (or pointing at ``tunnel_id``)
      exists → delete ours and create a fresh one (self-heal after a crashed
      prior run left a stale record behind);
    * create fails, only user-created records exist → raise a permanent error;
      we never clobber a hand-made record;
    * create fails, the zone can't even be read (e.g. the token lacks DNS
      permission) → raise the original error, annotated when it looks like a
      permission gap.
    """
    payload = {
        "type": "CNAME",
        "name": hostname,
        "content": f"{tunnel_id}.{_CFARGO_SUFFIX}",
        "proxied": True,
        "comment": _MANAGED_COMMENT,
    }
    try:
        return await _post_dns_record(api_token, zone_id, payload)
    except CloudflareError as exc:
        try:
            records = await _list_dns_records(
                api_token=api_token, zone_id=zone_id, hostname=hostname
            )
        except CloudflareError:
            raise _annotate_dns_error(exc) from exc

        ours = [r for r in records if _is_ours(r, tunnel_id)]
        if ours:
            for record in ours:
                await delete_dns_record(
                    api_token=api_token,
                    zone_id=zone_id,
                    dns_record_id=str(record.get("id") or ""),
                )
            logger.info(
                "[tunnel] replaced {} stale managed dns record(s) for {}",
                len(ours), hostname,
            )
            return await _post_dns_record(api_token, zone_id, payload)

        if records:
            types = ", ".join(sorted({str(r.get("type") or "?") for r in records}))
            raise CloudflareError(
                f"hostname {hostname} is occupied by a user-created DNS record "
                f"({types}); delete it in the Cloudflare dashboard or choose "
                "another subdomain",
                status=409,
            )
        # Nothing occupies the name and the create still failed — original error.
        raise _annotate_dns_error(exc) from exc


async def _post_dns_record(api_token: str, zone_id: str, payload: dict) -> str:
    dns = await _request(
        "POST",
        f"{_API_BASE}/zones/{zone_id}/dns_records",
        api_token=api_token,
        json_body=payload,
    )
    record_id = str(dns.get("id") or "")
    if not record_id:
        raise CloudflareError("create DNS record: response missing record id")
    return record_id


async def _list_dns_records(
    *, api_token: str, zone_id: str, hostname: str
) -> list[dict]:
    result = await _request(
        "GET",
        f"{_API_BASE}/zones/{zone_id}/dns_records?name={quote(hostname)}",
        api_token=api_token,
    )
    return [r for r in (result if isinstance(result, list) else []) if isinstance(r, dict)]


def _is_ours(record: dict, tunnel_id: str) -> bool:
    """Is this record ours to delete?

    Ours iff it carries our managed comment, or its content points at this
    runtime's tunnel (covers records created before the comment existed).
    """
    comment = record.get("comment")
    if isinstance(comment, dict):  # some API shapes wrap the text in an object
        comment = comment.get("comment") or ""
    if _MANAGED_COMMENT in str(comment or ""):
        return True
    return str(record.get("content") or "").lower() == f"{tunnel_id}.{_CFARGO_SUFFIX}".lower()


def _annotate_dns_error(exc: CloudflareError) -> CloudflareError:
    """Attach the likely cause to a bare DNS create failure."""
    if "HTTP 403" in str(exc):
        return CloudflareError(
            f"{exc} (the API token likely lacks Zone->DNS->Edit permission "
            "for this zone)",
            status=exc.status,
        )
    return exc


async def delete_dns_record(
    *, api_token: str, zone_id: str, dns_record_id: str
) -> None:
    if not dns_record_id:
        return
    try:
        await _request(
            "DELETE",
            f"{_API_BASE}/zones/{zone_id}/dns_records/{dns_record_id}",
            api_token=api_token,
        )
    except CloudflareError:
        pass


async def create_managed_tunnel(
    *,
    api_token: str,
    account_id: str,
    zone_id: str,
    tunnel_name: str,
    hostname: str,
    service: str,
) -> dict[str, str]:
    """Provision a tunnel + ingress + DNS for one binding.

    ``service`` is the local origin (e.g. ``http://127.0.0.1:8000``); ``hostname``
    is the public domain the user chose (must live under ``zone_id``).

    Returns ``{"tunnel_id", "dns_record_id", "token"}``. The caller passes
    ``token`` to the node and persists ``tunnel_id`` + ``dns_record_id`` in the
    binding's ``provider_ref`` for later teardown. On any partial failure the
    already-created tunnel is best-effort deleted before re-raising so we do not
    leak a half-provisioned tunnel.

    Self-healing: if the create POST fails because a tunnel with that name
    already exists (HTTP 409 / code 1013 — a prior run created it but crashed
    before recording its id), the existing tunnel is looked up by name and
    reused instead of failing. Tunnel names are unique per account, so a name
    match is authoritative.
    """
    created_here = False
    try:
        result = await _request(
            "POST",
            f"{_API_BASE}/accounts/{account_id}/cfd_tunnel",
            api_token=api_token,
            json_body={"name": tunnel_name, "config_src": "cloudflare"},
        )
        tunnel_id = str(result.get("id") or "")
        if not tunnel_id:
            raise CloudflareError("create tunnel: response missing tunnel id")
        created_here = True
    except CloudflareError:
        tunnel_id = await _find_tunnel_by_name(
            api_token=api_token, account_id=account_id, tunnel_name=tunnel_name
        )
        if not tunnel_id:
            raise
        logger.info(
            "[tunnel] tunnel {!r} already exists on cloudflare, reclaiming {}",
            tunnel_name, tunnel_id,
        )

    try:
        token = await _request(
            "GET",
            f"{_API_BASE}/accounts/{account_id}/cfd_tunnel/{tunnel_id}/token",
            api_token=api_token,
        )
        if not isinstance(token, str) or not token:
            raise CloudflareError("get tunnel token: empty token")

        await configure_managed_tunnel(
            api_token=api_token,
            account_id=account_id,
            tunnel_id=tunnel_id,
            ingress=[{"hostname": hostname, "service": service}],
        )

        dns_record_id = await create_dns_record(
            api_token=api_token,
            zone_id=zone_id,
            tunnel_id=tunnel_id,
            hostname=hostname,
        )
    except CloudflareError:
        # Roll back only a tunnel this call created. A reclaimed tunnel
        # predates us; deleting it on a transient later-step failure would
        # discard the resource the previous run meant to keep.
        if created_here:
            await _delete_tunnel_quiet(api_token, account_id, tunnel_id)
        raise

    return {"tunnel_id": tunnel_id, "dns_record_id": dns_record_id, "token": token}


async def _find_tunnel_by_name(
    *, api_token: str, account_id: str, tunnel_name: str
) -> str:
    """Return the id of the live (non-deleted) tunnel named ``tunnel_name``.

    Used to reclaim a tunnel a prior provisioning run created but never
    recorded locally. Returns ``""`` when none exists or the lookup fails.
    """
    try:
        tunnels = await _request(
            "GET",
            f"{_API_BASE}/accounts/{account_id}/cfd_tunnel"
            f"?name={quote(tunnel_name)}&per_page=100",
            api_token=api_token,
        )
    except CloudflareError:
        return ""
    for tunnel in tunnels if isinstance(tunnels, list) else []:
        if not isinstance(tunnel, dict):
            continue
        if str(tunnel.get("name") or "") != tunnel_name:
            continue
        if tunnel.get("deleted_at") or tunnel.get("is_deleted"):
            continue
        found = str(tunnel.get("id") or "")
        if found:
            return found
    return ""


async def delete_managed_tunnel(
    *,
    api_token: str,
    account_id: str,
    zone_id: str,
    provider_ref: dict | None,
) -> None:
    """Reclaim the DNS record + tunnel recorded in ``provider_ref``.

    Best-effort and idempotent: a missing/already-deleted resource is ignored so
    binding deletion never blocks on Cloudflare state. Raises only if both
    deletions fail with an unexpected error.
    """
    provider_ref = provider_ref or {}
    tunnel_id = str(provider_ref.get("tunnel_id") or "")
    dns_record_id = str(provider_ref.get("dns_record_id") or "")

    if dns_record_id:
        try:
            await _request(
                "DELETE",
                f"{_API_BASE}/zones/{zone_id}/dns_records/{dns_record_id}",
                api_token=api_token,
            )
        except CloudflareError:
            pass  # already gone / not found — recycling must not block delete
    if tunnel_id:
        await _delete_tunnel_quiet(api_token, account_id, tunnel_id)


async def _delete_tunnel_quiet(api_token: str, account_id: str, tunnel_id: str) -> None:
    """Delete a tunnel, swallowing not-found / already-deleted errors."""
    try:
        await _request(
            "DELETE",
            f"{_API_BASE}/accounts/{account_id}/cfd_tunnel/{tunnel_id}",
            api_token=api_token,
        )
    except CloudflareError:
        pass
