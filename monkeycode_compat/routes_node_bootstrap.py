"""Public (pre-auth) node bootstrap surface (``/api/v1/public/nodes``).

A fresh node machine has no administrator session, yet the one-click install
flow must run there:

    curl -fsSL http://<server>/api/v1/public/nodes/<node_id>/install.sh | bash

So these endpoints are intentionally **public**:

* ``GET /{node_id}/install.sh`` — a self-contained bash installer whose
  server/node_id/secret/role are filled from a short-TTL Redis bootstrap record
  (written at onboard by the admin surface). The secret only ever appears in the
  response *body*, never in a URL or log. An unknown/expired node_id returns a
  plain-text 404 so ``curl | bash`` fails cleanly.
* ``GET /binaries/{name}`` — a public mirror of the role-specific node builds so
  the install script can download the matching platform binaries. The
  admin-authenticated ``/api/v1/admin/nodes/binaries*`` surface stays for the
  in-console manual download; this one exists purely for unattended bootstrap.
* ``GET /docker/{file}`` — a strict two-file whitelist (Dockerfile +
  entrypoint.sh) used with the binaries above to build the shared node image
  locally on the host. It contains no credential and requires no registry.

Security note: the install endpoint is gated only on the opaque node id — the
necessary trade-off for "install from a zero-state machine with no login". The
record is short-lived (``_NODE_BOOTSTRAP_TTL_SECONDS``) and carries no
administrator capability. Binary/docker artifacts are public static build
inputs with basename whitelists and contain no credential.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, PlainTextResponse

from . import config
from .nodes_service import (
    _collect_install_assets,
    get_node_bootstrap,
    nodes_service,
    render_install_bat,
    render_install_script,
    resolve_node_binary,
)

router = APIRouter(prefix="/api/v1/public/nodes", tags=["monkeycode-nodes-public"])

# Node image build artifacts served to the docker/docker-compose one-click
# installer so it can `docker build` the shared agent image locally (no registry
# push). Whitelisted filenames only; the directory is fixed.
_DOCKER_DIR = Path(__file__).resolve().parent.parent / "nodes" / "docker"
_DOCKER_FILES = {"Dockerfile", "entrypoint.sh"}


def _public_base_url(request: Request) -> str:
    """The origin the node machine should hit to download the binary.

    The install script downloads the agent binary from *ai-lubricant* (this
    service), not the agent-compose daemon, so we derive the base from the
    incoming request — whatever host/scheme the operator used to fetch the
    script is reachable from that machine for the binary too.
    """
    return str(request.base_url).rstrip("/")


@router.get("/{node_id}/install.sh")
async def install_script(node_id: str, request: Request, method: str | None = None):
    """Return a self-contained bash installer for a node, gated on node_id.

    ``method`` is an optional, tightly-whitelisted presentation override used by
    the Docker tab. Execution nodes are onboarded as ``standalone`` by default,
    but an operator may install that same credential as a container instead. The
    override changes only the rendered deployment shape; it does not mutate the
    authoritative node record or bootstrap credential.
    """
    bootstrap = await get_node_bootstrap(node_id)
    if not bootstrap:
        return PlainTextResponse(
            "# node bootstrap not found or expired; re-onboard the node to get a fresh install command\n",
            status_code=404,
        )
    if method is not None:
        resolved_method = method.strip().lower()
        if resolved_method not in {"standalone", "docker", "docker-compose"}:
            return PlainTextResponse("# unsupported node install method\n", status_code=400)
        bootstrap = {**bootstrap, "startup_method": resolved_method}
    install_assets = await _collect_install_assets()
    execution_assets = install_assets["execution"]
    management_assets = install_assets["management"]
    ios_host_assets = install_assets["ios_host"]
    runtime_assets = install_assets["runtime"]
    role = str(bootstrap.get("role") or "")
    if role == "ios_host":
        role_assets = ios_host_assets
    elif role in {"management", "passive_management"}:
        role_assets = management_assets
    else:
        role_assets = execution_assets
    # 安装阶段节点未连上，无法收推送；把节点绑定的 proxy_config_id 解析成三字段
    # 烘焙进脚本。bootstrap 只存 id 引用，这里实时解析，代理池改动下次生效。
    proxy_fields = await nodes_service.resolved_proxy_fields_for(bootstrap.get("proxy_config_id") or "")
    script = render_install_script(
        bootstrap,
        public_base_url=_public_base_url(request),
        agent_image=config.settings.agent_compose_agent_image,
        assets=role_assets,
        execution_assets=execution_assets,
        management_assets=management_assets,
        ios_host_assets=ios_host_assets,
        runtime_assets=runtime_assets,
        proxy_fields=proxy_fields,
    )
    # text/plain so `curl ... | bash` streams it straight into the shell.
    return PlainTextResponse(script, media_type="text/x-shellscript; charset=utf-8")


@router.get("/{node_id}/install.bat")
async def install_bat(node_id: str, request: Request):
    """Return a self-contained Windows batch installer for a node, gated on node_id.

    The Windows one-click flow downloads the role binary, asks once before
    replacing any old local node data/tasks, persists the new credentials in the
    user's config directory, and then creates one fixed ONLOGON task, runner and
    desktop shortcut. The runner never contains the secret; no WSL/bash is needed.
    """
    bootstrap = await get_node_bootstrap(node_id)
    if not bootstrap:
        return PlainTextResponse(
            "rem node bootstrap not found or expired; re-onboard the node to get a fresh install command\r\n",
            status_code=404,
        )
    install_assets = await _collect_install_assets()
    execution_assets = install_assets["execution"]
    management_assets = install_assets["management"]
    ios_host_assets = install_assets["ios_host"]
    runtime_assets = install_assets["runtime"]
    role = str(bootstrap.get("role") or "")
    if role == "ios_host":
        role_assets = ios_host_assets
    elif role in {"management", "passive_management"}:
        role_assets = management_assets
    else:
        role_assets = execution_assets
    proxy_fields = await nodes_service.resolved_proxy_fields_for(bootstrap.get("proxy_config_id") or "")
    script = render_install_bat(
        bootstrap,
        public_base_url=_public_base_url(request),
        assets=role_assets,
        runtime_assets=runtime_assets,
        proxy_fields=proxy_fields,
    )
    # CRLF + a .bat content type so a browser download / curl -o yields a runnable file.
    body = script.replace("\r\n", "\n").replace("\n", "\r\n")
    return PlainTextResponse(body, media_type="application/bat; charset=utf-8")


@router.get("/{node_id}/ios-sidecar/install.sh")
async def ios_sidecar_install_script(node_id: str, request: Request):
    """Install the node-ios sidecar on a host that already runs a node.

    The node_id is used only as an existence check and for logging — the script
    carries NO credential: node-ios runs in pure device mode here and pairs via
    a device-control pairing code (`node-ios pair --host-node-id <node_id>`).
    Installs into the ios-scoped state dir so it never contends with the
    host-wide agent-compose node lock. Assets come from the same
    ``node-releases/version.json`` ``ios_host`` matrix as node upgrades.
    """
    exists = False
    try:
        info = await nodes_service.get_node_if_exists(node_id)
        exists = bool(info)
    except Exception:
        exists = False
    if not exists:
        return PlainTextResponse("# node not found\\n", status_code=404)

    assets = (await _collect_install_assets()).get("ios_host") or {}

    def q(v: str) -> str:
        return "'" + v.replace("'", "'\''") + "'"

    def lines(os_name: str) -> list[str]:
        up = f"{os_name}_".upper()
        out = []
        for arch in ("AMD64", "ARM64"):
            entry = assets.get((os_name, arch.lower())) or {}
            out.append(f"URL_{up}_{arch}={q(str(entry.get('url') or ''))}")
            out.append(f"SHA_{up}_{arch}={q(str(entry.get('sha256') or ''))}")
        return out

    env_lines = []
    for os_name in ("linux", "darwin"):
        env_lines.extend(lines(os_name))
        env_lines.append("")
    env_block = "\\n".join(env_lines.rstrip())

    script = f"""#!/usr/bin/env bash
# node-ios sidecar installer for node {node_id}.
# Pure device mode: no NodeConnect identity, no lock on the agent-compose node.
set -euo pipefail
{env_block}
arch=$(uname -m); case $arch in x86_64|amd64) arch=amd64;; aarch64|arm64) arch=arm64;; *) echo "unsupported arch: $arch"; exit 1;; esac
case "$(uname -s)" in
  Linux) os=linux;;
  Darwin) os=darwin;;
  *) echo "unsupported OS: $(uname -s)"; exit 1;;
esac
var="URL_$(echo $os | tr '[:lower:]' '[:upper:]')_$(echo $arch | tr '[:lower:]' '[:upper:]')"
url="${{!var}}"; [ -n "$url" ] || {{ echo "no node-ios build published for $os/$arch"; exit 1; }}
shavar="SHA_$(echo $os | tr '[:lower:]' '[:upper:]')_$(echo $arch | tr '[:lower:]' '[:upper:]')"
sha="${{!shavar}}"
root="${{HOME}}/.agent-compose/ios"
bin="$root/bin/node-ios"
mkdir -p "$root/bin"
echo "downloading node-ios ($os/$arch) ..."
curl -fsSL "$url" -o "$bin.tmp"
[ -z "$sha" ] || echo "$sha  $bin.tmp" | sha256sum -c - >/dev/null 2>&1 || shasum -a 256 -c - >/dev/null 2>&1 || {{ echo "checksum mismatch"; rm -f "$bin.tmp"; exit 1; }}
mv "$bin.tmp" "$bin"
chmod +x "$bin"
echo "node-ios installed: $bin"
echo "next: pair an iPhone with  $bin pair --server <server> --code <CODE> --host-node-id {node_id}"
"""
    return PlainTextResponse(script, media_type="text/x-shellscript; charset=utf-8")


@router.get("/binaries/{name}")
async def public_download_node_binary(name: str):
    """Public mirror of one agent binary (basename whitelist enforced upstream)."""
    path = resolve_node_binary(name)
    if path is None:
        return PlainTextResponse("node binary not found\n", status_code=404)
    return FileResponse(
        path,
        filename=path.name,
        media_type="application/octet-stream",
        content_disposition_type="attachment",
    )


@router.get("/docker/{file}")
async def public_download_node_docker_file(file: str):
    """Public mirror of one node-image build artifact (Dockerfile / entrypoint.sh).

    The docker one-click installer curls these alongside the two role binaries
    and `docker build`s the shared agent image locally on the host — no registry
    push. Whitelisted filenames only, served from a fixed directory, so a crafted
    path cannot traverse elsewhere.
    """
    name = (file or "").strip()
    if name not in _DOCKER_FILES:
        return PlainTextResponse("node docker artifact not found\n", status_code=404)
    path = (_DOCKER_DIR / name).resolve()
    try:
        path.relative_to(_DOCKER_DIR.resolve())
    except ValueError:
        return PlainTextResponse("node docker artifact not found\n", status_code=404)
    if not path.is_file():
        return PlainTextResponse("node docker artifact not found\n", status_code=404)
    return FileResponse(
        path,
        filename=path.name,
        media_type="application/octet-stream",
        content_disposition_type="attachment",
    )
