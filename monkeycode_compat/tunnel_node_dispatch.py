"""Node-side dispatch for tunnel bindings: start/stop the client on a node.

Each binding maps to one long-running client process (frpc / cloudflared / npc)
started on its node via the control plane's ``StartToolRun`` unary RPC, which
the node handles as a ``NodeToolRunRequest`` frame and streams output back as
``NodeToolRunEvent`` frames correlated by run_id.

Supported clients:

* **frpc** — deterministic public address ``server_addr:port`` from the scheme
  config + an allocated port. Write frpc.toml to a per-node scratch dir and
  start ``frpc -c <ini>``. The binding flips to ``running`` optimistically; the
  node's stderr surfaces failures.
* **cloudflared quick (trycloudflare)** — capture the runtime-minted
  ``*.trycloudflare.com`` domain from the client's stdout via FollowToolRun,
  then back-fill ``public_addr``.
* **cloudflared managed** — provision a remotely-managed tunnel + ingress + DNS
  CNAME through the Cloudflare API (:mod:`.tunnel_cloudflare`), then run the
  connector with its token supplied via the ``TUNNEL_TOKEN`` env var. The
  public address is the user-chosen ``hostname``, so no stdout capture.
* **npc** — like frpc: deterministic ``server_addr:port``; write npc.conf and
  start ``npc -config=<conf>``.

Binary provisioning: the client binary is fetched on the node into a cache dir
under the node's home if missing. See :func:`_ensure_binary_script`.
"""
from __future__ import annotations

import re

from loguru import logger

from .models_tunnel import TunnelBinding, TunnelRuntime, TunnelScheme

# Per-node scratch dir for generated client configs + the binary cache. The node
# runs this path relative to its user home; the dispatcher only needs a stable
# location to reference from the wrapper script.
_TUNNEL_CONF_DIR = "tunnel-confs"
_TUNNEL_BIN_DIR = "tunnel-bins"

# Known client binaries and their download sources. Version pins are conservative
# defaults; an operator may override per-kind via the scheme config (binary_url
# + binary_sha256) so offline/airgapped deployments can serve their own mirror.
_CLIENT_SOURCES = {
    "frpc": {
        # fatedier/frp GitHub release asset naming: frp_<ver>_<os>_<arch>.tar.gz
        # containing frp_<ver>_<os>_<arch>/frpc.
        "url": "https://github.com/fatedier/frp/releases/download/v0.61.1/frp_0.61.1_{os}_{arch}.tar.gz",
        "extract": "tar.gz",
        "binary_in_archive": "frp_0.61.1_{os}_{arch}/frpc",
    },
    "cloudflared": {
        # cloudflared single-binary releases per os/arch (no archive).
        "url": "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-{os}-{arch}",
        "extract": None,
        "binary_in_archive": "cloudflared",
    },
    "npc": {
        # ehang-io/nps GitHub release asset: _<os>_<arch>_client.tar.gz etc.
        "url": "https://github.com/ehang-io/nps/releases/download/v0.26.10/_{os}_{arch}_client.tar.gz",
        "extract": "tar.gz",
        "binary_in_archive": "npc",
    },
}


def _binary_for_kind(kind: str) -> str:
    """The client binary name for a scheme kind (resolved on the node PATH)."""
    return {"frpc": "frpc", "cloudflared": "cloudflared", "npc": "npc"}.get(
        (kind or "").strip(), ""
    )


def _frpc_group_toml(bindings: list[TunnelBinding], scheme: TunnelScheme) -> str:
    """Render one frpc config containing every running binding in the group."""
    cfg = scheme.config or {}
    lines = [
        f'serverAddr = "{cfg.get("server_addr", "")}"',
        f'serverPort = {int(cfg.get("server_port", 0) or 0)}',
    ]
    token = cfg.get("token", "")
    if token:
        lines.append(f'auth.token = "{token}"')
    for binding in bindings:
        lines.extend([
            "",
            "[[proxies]]",
            f'name = "tunnel-{binding.id}"',
            'type = "tcp"',
            f'localIP = "{binding.local_host}"',
            f'localPort = {int(binding.local_port)}',
            f'remotePort = {int(binding.allocated_value)}',
        ])
    return "\n".join(lines) + "\n"


def _frpc_ini(binding: TunnelBinding, scheme: TunnelScheme) -> str:
    """Compatibility wrapper for a one-binding frpc config."""
    return _frpc_group_toml([binding], scheme)


def _npc_conf(binding: TunnelBinding, scheme: TunnelScheme) -> str:
    """Render an npc.conf for one binding (npc v0.26 ini format)."""
    cfg = scheme.config or {}
    server_addr = cfg.get("server_addr", "")
    server_port = int(cfg.get("server_port", 0) or 0)
    token = cfg.get("token", "")
    lines = [
        "[common]",
        f'server_addr={server_addr}',
        f'server_port={server_port}',
        "conn_type=tcp",
    ]
    if token:
        lines.append(f'vkey={token}')
    lines.append("")
    lines.append("[tcp]")
    lines.append(f'local_port={int(binding.local_port)}')
    lines.append(f'remote_port={int(binding.allocated_value)}')
    return "\n".join(lines) + "\n"


def _ensure_binary_script(kind: str, config: dict | None = None) -> str:
    """A portable sh snippet that ensures the client binary is cached on the node.

    ``config.binary_url`` optionally overrides the built-in release URL and
    ``config.binary_sha256`` enables an integrity check before activation.
    """
    src = _CLIENT_SOURCES[kind]
    config = config or {}
    url_template = str(config.get("binary_url") or src["url"])
    expected_sha = str(config.get("binary_sha256") or "").strip().lower()
    detect = (
        'os="$(uname -s | tr A-Z a-z)"; '
        'arch="$(uname -m)"; '
        'case "$arch" in x86_64|amd64) arch=amd64;; aarch64|arm64) arch=arm64;; i386|i686) arch=386;; *) arch=amd64;; esac; '
        'case "$os" in darwin) os=darwin;; *) os=linux;; esac'
    )
    # The source URL / archive member name may contain {os}/{arch} placeholders.
    archive_binary_template = src["binary_in_archive"]
    extract = src["extract"]
    binary_name = _binary_for_kind(kind)
    # We cannot pre-substitute os/arch here (node detects them), so we emit a
    # shell snippet that computes the concrete URL/member after detection.
    lines = [
        detect,
        f'bin_dir="$HOME/{_TUNNEL_BIN_DIR}"',
        f'conf_dir="$HOME/{_TUNNEL_CONF_DIR}"',
        'mkdir -p "$bin_dir" "$conf_dir"',
        f'target="$bin_dir/{binary_name}"',
        'if [ ! -x "$target" ]; then',
        '  echo "[tunnel] fetching %s binary for $os/$arch" >&2' % kind,
        '  tmp="$(mktemp -d)"',
    ]
    if extract == "tar.gz":
        # Download the tarball, extract, and move the inner binary to target.
        # {os}/{arch} placeholders in url/member are substituted on the node
        # (it detects its own platform); sed handles it portably on POSIX sh
        # (dash has no ${var//...}).
        # TUNNEL_URL_PREFIX (url_prefix 模式代理) 会整体改写下载地址。
        lines += [
            f'  member="$(echo "{archive_binary_template}" | sed "s/{{os}}/$os/g" | sed "s/{{arch}}/$arch/g")"',
            f'  url="$(echo "{url_template}" | sed "s/{{os}}/$os/g" | sed "s/{{arch}}/$arch/g")"',
            '  if [ -n "$TUNNEL_URL_PREFIX" ]; then url="$TUNNEL_URL_PREFIX/$url"; fi',
            '  echo "[tunnel] downloading %s from $url" >&2' % kind,
            '  if ! curl -fSL "$url" -o "$tmp/asset"; then',
            '    echo "[tunnel] download %s failed: $url" >&2; exit 127;' % kind,
            '  fi',
            '  echo "[tunnel] extracting %s" >&2' % kind,
            '  tar -xzf "$tmp/asset" -C "$tmp"',
            '  mv "$tmp/$member" "$target"',
        ]
    else:
        lines += [
            f'  url="$(echo "{url_template}" | sed "s/{{os}}/$os/g" | sed "s/{{arch}}/$arch/g")"',
            '  if [ -n "$TUNNEL_URL_PREFIX" ]; then url="$TUNNEL_URL_PREFIX/$url"; fi',
            '  echo "[tunnel] downloading %s from $url" >&2' % kind,
            '  if ! curl -fSL "$url" -o "$tmp/asset"; then',
            '    echo "[tunnel] download %s failed: $url" >&2; exit 127;' % kind,
            '  fi',
            '  mv "$tmp/asset" "$target"',
        ]
    lines += [
        '  chmod +x "$target"',
    ]
    if expected_sha:
        # Verify the downloaded binary matches the configured digest before
        # activating it. Prefer sha256sum, fall back to shasum -a 256 (macOS).
        lines += [
            '  echo "[tunnel] verifying %s checksum" >&2' % kind,
            '  sha_cmd="sha256sum"; command -v sha256sum >/dev/null 2>&1 || sha_cmd="shasum -a 256"',
            f'  actual="$($sha_cmd "$target" | awk \'{{print $1}}\')"',
            f'  if [ "$actual" != "{expected_sha}" ]; then',
            '    echo "[tunnel] %s checksum mismatch: $actual != %s" >&2; rm -rf "$tmp"; exit 126;' % (kind, expected_sha),
            '  fi',
        ]
    lines += [
        '  echo "[tunnel] %s binary ready: $target" >&2' % kind,
        '  rm -rf "$tmp"',
        'fi',
    ]
    return "\n".join(lines)


async def dispatch_runtime_to_node(
    runtime: TunnelRuntime,
    scheme: TunnelScheme,
    bindings: list[TunnelBinding],
    revision: int,
) -> None:
    """Start one aggregated runtime process on an execution node."""
    kind = (scheme.kind or "").strip()
    if kind == "frpc":
        conf = _frpc_group_toml(bindings, scheme)
        conf_name = f"runtime-{runtime.id}.toml"
        script = (
            _ensure_binary_script("frpc", scheme.config or {})
            + f'\nprintf %s "$_TUNNEL_CONF" > "$conf_dir/{conf_name}"'
            + f'\nexec "$target" -c "$conf_dir/{conf_name}"'
        )
        env = {"_TUNNEL_CONF": conf}
    elif kind == "npc":
        binding = bindings[0]
        conf = _npc_conf(binding, scheme)
        conf_name = f"runtime-{runtime.id}.conf"
        script = (
            _ensure_binary_script("npc", scheme.config or {})
            + f'\nprintf %s "$_TUNNEL_CONF" > "$conf_dir/{conf_name}"'
            + f'\nexec "$target" -config="$conf_dir/{conf_name}"'
        )
        env = {"_TUNNEL_CONF": conf}
    elif kind == "cloudflared":
        mode = (scheme.config or {}).get("mode") or "quick"
        if mode == "managed":
            from .tunnel_supervisor import _prepare_managed

            token = await _prepare_managed(runtime, scheme, bindings)
            script = (
                _ensure_binary_script("cloudflared", scheme.config or {})
                + '\nexec "$target" tunnel --no-autoupdate run'
            )
            env = {"TUNNEL_TOKEN": token}
        else:
            binding = bindings[0]
            origin = f"http://{binding.local_host}:{int(binding.local_port)}"
            script = (
                _ensure_binary_script("cloudflared", scheme.config or {})
                + f'\nexec "$target" tunnel --no-autoupdate --url {origin}'
            )
            env = {}
    else:
        raise RuntimeError(f"unsupported tunnel runtime kind {kind!r}")

    # 下载代理跟随目标节点自身的常驻绑定（节点记录的 proxy_config_id）——
    # 节点详情「环境」里选的那条出口代理，与节点升级走同一套。
    # 主服务目标（__main__）没有节点记录，走 tunnel_binaries 的资源中心代理。
    resolved_node = (runtime.target_id or "").strip()
    if resolved_node and resolved_node != "__main__":
        try:
            from .nodes_service import NodesService

            node = await NodesService().get_node_if_exists(resolved_node)
            node_proxy_id = str((node or {}).get("proxy_config_id") or "").strip()
            if node_proxy_id:
                proxy_fields = await NodesService().resolved_proxy_fields_for(node_proxy_id)
                mode = proxy_fields.get("proxy_mode") or proxy_fields.get("proxyMode") or ""
                proxy_url = str(proxy_fields.get("proxy_url") or proxy_fields.get("proxyUrl") or "")
                if mode == "network" and proxy_url:
                    env["HTTPS_PROXY"] = proxy_url
                    env["HTTP_PROXY"] = proxy_url
                    env["ALL_PROXY"] = proxy_url
                elif mode == "url_prefix":
                    prefix = str(
                        proxy_fields.get("proxy_url_prefix")
                        or proxy_fields.get("proxyUrlPrefix")
                        or ""
                    ).rstrip("/")
                    if prefix:
                        env["TUNNEL_URL_PREFIX"] = prefix
        except Exception:  # noqa: BLE001 - proxy resolve must not block dispatch
            logger.debug(
                "[tunnel] node proxy resolve failed for {}", resolved_node, exc_info=True
            )

    from .node_client import get_local_node_client

    await get_local_node_client().start_tool_run(
        runtime.target_id,
        runtime.run_id or f"tunnel-runtime:{runtime.id}",
        "/bin/sh",
        ["-c", script],
        env=env,
        revision=revision,
    )
    asyncio.create_task(_monitor_runtime(
        runtime.target_id,
        runtime.run_id or f"tunnel-runtime:{runtime.id}",
        runtime.id,
        revision,
        bindings,
    ))


async def _monitor_runtime(
    node_id: str,
    run_id: str,
    runtime_id,
    revision: int,
    bindings: list[TunnelBinding],
) -> None:
    """Stream node events into the revision-fenced runtime state machine."""
    from .node_client import get_local_node_client
    from .tunnel_runtime_manager import handle_event

    url_re = re.compile(rb"https://[A-Za-z0-9-]+\.trycloudflare\.com")
    try:
        async for evt in get_local_node_client().follow_tool_run_events(node_id, run_id):
            event_revision = int(evt.get("revision") or revision)
            data: bytes = evt.get("data") or b""
            if data and len(bindings) == 1:
                match = url_re.search(data)
                if match:
                    binding = bindings[0]
                    binding.public_addr = match.group(0).decode("ascii", "replace")
                    await binding.save(update_fields=["public_addr", "updated_at"])
                    await handle_event(
                        runtime_id, revision=event_revision, kind="stdout",
                        data=b"registered tunnel connection",
                    )
            await handle_event(
                runtime_id,
                revision=event_revision,
                kind=str(evt.get("kind") or "unknown"),
                data=data,
                exit_code=int(evt.get("exit_code") or 0),
                error=str(evt.get("error") or ""),
                pid=int(evt.get("pid") or 0),
            )
            if evt.get("kind") == "exited":
                return
    except Exception as exc:  # noqa: BLE001
        logger.debug("[tunnel] runtime monitor ended for {}: {}", runtime_id, exc)


async def dispatch_binding(binding: TunnelBinding, scheme: TunnelScheme) -> None:
    """Start the client for a binding on its target.

    ``node_id == MAIN_SERVICE_TARGET`` runs the client in this process (see
    :mod:`.tunnel_supervisor`); any other value dispatches a /bin/sh -c wrapper
    to that node, which ensures the client binary is present (download+extract if
    missing) then execs it. Updates the binding row in place (the caller
    persists).
    """
    kind = (scheme.kind or "").strip()
    binary_name = _binary_for_kind(kind)
    if not binary_name:
        binding.client_status = "failed"
        binding.error = f"unsupported scheme kind {kind!r}"
        return

    from .tunnel_runtime_manager import reconcile_binding

    await reconcile_binding(binding, scheme)
    return

    if kind == "cloudflared":
        return await _dispatch_cloudflared(binding, scheme)
    if kind == "frpc":
        return await _dispatch_config_client(binding, scheme, "frpc", _frpc_ini)
    if kind == "npc":
        return await _dispatch_config_client(binding, scheme, "npc", _npc_conf)
    binding.client_status = "failed"
    binding.error = f"kind {kind!r} not yet implemented"


async def _dispatch_config_client(
    binding: TunnelBinding,
    scheme: TunnelScheme,
    kind: str,
    render_conf,
) -> None:
    """Start frpc / npc: ensure binary, write config, exec client.

    The public address was allocated up front (port range), so the binding
    flips to ``running`` on a successful dispatch. Client failures surface via
    stderr on the run's event stream (a future consumer can flip to failed).
    """
    conf = render_conf(binding, scheme)
    conf_name = f"{kind}-{binding.id}.conf"
    if kind == "frpc":
        client_invocation = f'exec "$target" -c "$conf_dir/{conf_name}"'
    else:  # npc
        client_invocation = f'exec "$target" -config="$conf_dir/{conf_name}"'
    script = (
        _ensure_binary_script(kind, scheme.config or {})
        + f'\nprintf %s "$_TUNNEL_CONF" > "$conf_dir/{conf_name}"'
        + f"\n{client_invocation}"
    )
    run_id = str(binding.id)
    binding.run_id = run_id

    from .node_client import get_local_node_client

    client = get_local_node_client()
    try:
        await client.start_tool_run(
            binding.node_id,
            run_id,
            "/bin/sh",
            ["-c", script],
            env={"_TUNNEL_CONF": conf},
        )
    except Exception as exc:  # noqa: BLE001 - dispatch failure flips to failed
        binding.client_status = "failed"
        binding.error = f"start {kind} failed: {exc}"[:500]
        return
    binding.client_status = "running"
    binding.error = None
    # Keep consuming the event stream after startup so an unexpected client
    # exit is persisted instead of leaving a stale "running" binding.
    import asyncio
    asyncio.create_task(_monitor_tool_run(binding.node_id, run_id, binding.id))
    logger.info(
        "[tunnel] {} started: binding={} node={} public={}",
        kind, binding.id, binding.node_id, binding.public_addr,
    )


async def _monitor_tool_run(node_id: str, run_id: str, binding_id) -> None:
    """Persist a failed/stopped client when its long-running process exits."""
    from .node_client import get_local_node_client
    from .models_tunnel import TunnelBinding
    try:
        async for evt in get_local_node_client().follow_tool_run_events(node_id, run_id):
            if evt.get("kind") != "exited":
                continue
            row = await TunnelBinding.get_or_none(id=binding_id)
            if row is None:
                return
            if row.client_status == "running":
                row.client_status = "stopped" if not evt.get("error") else "failed"
                row.error = evt.get("error") or f"client exited code={evt.get('exit_code')}"
                await row.save(update_fields=["client_status", "error", "updated_at"])
            return
    except Exception as exc:  # noqa: BLE001
        logger.debug("[tunnel] tool monitor ended for {}: {}", binding_id, exc)


async def _dispatch_cloudflared(binding: TunnelBinding, scheme: TunnelScheme) -> None:
    """Start cloudflared for a binding, branching on the scheme's mode.

    * ``quick``   — anonymous trycloudflare: run ``cloudflared tunnel --url``
      and capture the runtime-minted ``*.trycloudflare.com`` domain from stdout.
    * ``managed`` — provision a Cloudflare tunnel + DNS via the API, then run
      ``cloudflared tunnel run`` with the connector token (via ``TUNNEL_TOKEN``
      env). The public hostname is known up front, so no stdout capture.
    """
    mode = ((scheme.config or {}).get("mode") or "quick").strip()
    if mode == "managed":
        return await _dispatch_cloudflared_managed(binding, scheme)
    return await _dispatch_cloudflared_quick(binding, scheme)


async def _dispatch_cloudflared_managed(binding: TunnelBinding, scheme: TunnelScheme) -> None:
    """Provision a managed Cloudflare tunnel + DNS, then run the connector.

    The tunnel/DNS are created via the Cloudflare API (``tunnel_cloudflare``);
    the connector token is passed to the node via the ``TUNNEL_TOKEN`` env var
    (never on argv, so it does not leak into the node's process list). The
    public address is the user-chosen hostname, known before start — no stdout
    scraping needed. ``provider_ref`` records the tunnel + DNS ids for teardown.

    A binding restarted after a stop still owns its tunnel + DNS record, so the
    token is re-fetched rather than provisioning a second, orphaning the first.
    """
    from . import tunnel_cloudflare as cf

    cfg = scheme.config or {}
    hostname = (binding.hostname or "").strip()
    if not hostname:
        binding.client_status = "failed"
        binding.error = "cloudflared managed binding requires a hostname"
        return

    api_token = str(cfg.get("api_token") or "")
    account_id = str(cfg.get("account_id") or "")
    zone_id = str(cfg.get("zone_id") or "")
    service = f"http://{binding.local_host}:{int(binding.local_port)}"
    provider_ref = dict(binding.provider_ref or {})
    existing_tunnel = str(provider_ref.get("tunnel_id") or "")
    provisioned_now = False
    try:
        if existing_tunnel:
            token = await cf.get_managed_tunnel_token(
                api_token=api_token, account_id=account_id, tunnel_id=existing_tunnel
            )
        else:
            provisioned = await cf.create_managed_tunnel(
                api_token=api_token,
                account_id=account_id,
                zone_id=zone_id,
                tunnel_name=f"mc-{binding.id}",
                hostname=hostname,
                service=service,
            )
            token = provisioned["token"]
            provider_ref = {
                "tunnel_id": provisioned["tunnel_id"],
                "dns_record_id": provisioned["dns_record_id"],
            }
            provisioned_now = True
    except cf.CloudflareError as exc:
        logger.warning(
            "[tunnel] binding {} cloudflare provisioning failed: {}", binding.id, exc
        )
        binding.client_status = "failed"
        binding.error = f"cloudflare provisioning failed: {exc}"[:500]
        # A permanent failure (auth/permission/hostname-occupied-by-user) will
        # never succeed on the next 45s tick; raise so the runtime reconciler
        # marks this runtime failed and stops auto-retrying until the admin
        # changes the scheme/binding. Transient errors are swallowed so the
        # reconciler keeps retrying as before.
        if cf.is_permanent_error(exc):
            raise
        return

    binding.provider_ref = provider_ref

    run_id = str(binding.id)
    binding.run_id = run_id
    from .node_client import get_local_node_client

    client = get_local_node_client()
    # Token via env (not argv) so it does not appear in the node's process list.
    script = (
        _ensure_binary_script("cloudflared", scheme.config or {})
        + '\nexec "$target" tunnel --no-autoupdate run'
    )
    try:
        await client.start_tool_run(
            binding.node_id,
            run_id,
            "/bin/sh",
            ["-c", script],
            env={"TUNNEL_TOKEN": token},
        )
    except Exception as exc:  # noqa: BLE001
        # Node dispatch failed. Reclaim only resources this call created, so a
        # restart failure never deletes the tunnel/DNS an earlier run still owns.
        if provisioned_now:
            await cf.delete_managed_tunnel(
                api_token=api_token,
                account_id=account_id,
                zone_id=zone_id,
                provider_ref=provider_ref,
            )
            binding.provider_ref = {}
        binding.client_status = "failed"
        binding.error = f"start cloudflared failed: {exc}"[:500]
        return

    binding.public_addr = f"https://{hostname}"
    binding.client_status = "running"
    binding.error = None
    import asyncio
    asyncio.create_task(_monitor_tool_run(binding.node_id, run_id, binding.id))
    logger.info(
        "[tunnel] cloudflared managed up: binding={} node={} hostname={}",
        binding.id, binding.node_id, hostname,
    )


async def _dispatch_cloudflared_quick(binding: TunnelBinding, scheme: TunnelScheme) -> None:
    """Start cloudflared (trycloudflare quick) and back-fill the public addr.

    cloudflared prints its trycloudflare URL to stdout shortly after start:
    ``| https://<random>.trycloudflare.com ... |``. We capture that with a
    tolerant regex and store it on the binding.
    """
    run_id = str(binding.id)
    binding.run_id = run_id
    from .node_client import get_local_node_client

    client = get_local_node_client()
    target = f"http://{binding.local_host}:{int(binding.local_port)}"
    script = (
        _ensure_binary_script("cloudflared", scheme.config or {})
        + f'\nexec "$target" tunnel --url {target}'
    )
    # NOTE: $target above is the binary path set by _ensure_binary_script; the
    # http target is passed literally (not via $target) so the two do not clash.
    try:
        await client.start_tool_run(
            binding.node_id,
            run_id,
            "/bin/sh",
            ["-c", script],
        )
    except Exception as exc:  # noqa: BLE001
        binding.client_status = "failed"
        binding.error = f"start cloudflared failed: {exc}"[:500]
        return

    url_re = re.compile(rb"https://[A-Za-z0-9-]+\.trycloudflare\.com")
    deadline_cap = 90  # seconds to wait for the domain to appear
    elapsed = 0.0
    try:
        async for evt in client.follow_tool_run_events(binding.node_id, run_id):
            kind = evt.get("kind")
            data: bytes = evt.get("data") or b""
            if kind in ("stdout", "stderr"):
                m = url_re.search(data)
                if m:
                    binding.public_addr = m.group(0).decode("ascii", "replace")
                    binding.client_status = "running"
                    binding.error = None
                    logger.info(
                        "[tunnel] cloudflared up: binding={} url={}",
                        binding.id, binding.public_addr,
                    )
                    return
                elapsed += 0.0  # frames carry no timestamps; bounded by EXITED
            elif kind == "exited":
                binding.client_status = "failed"
                binding.error = (
                    evt.get("error")
                    or f"cloudflared exited code={evt.get('exit_code')}"
                )[:500]
                return
            if elapsed > deadline_cap:
                break
    except Exception as exc:  # noqa: BLE001
        binding.client_status = "failed"
        binding.error = f"cloudflared stdout capture failed: {exc}"[:500]
        return
    binding.client_status = "pending"
    binding.error = "cloudflared did not report a trycloudflare domain in time"


async def stop_binding(binding: TunnelBinding) -> None:
    """Compatibility stop: mark this mapping stopped and reconcile its runtime."""
    scheme = await TunnelScheme.get_or_none(id=binding.scheme_id)
    if scheme is None:
        return
    from .tunnel_runtime_manager import set_binding_desired

    await set_binding_desired(binding, scheme, "stopped")
