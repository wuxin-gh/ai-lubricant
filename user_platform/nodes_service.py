"""Node orchestration service — bridges agent-compose nodes to team groups.

Two audiences share one service:

* **Admin (global)** — proxies the agent-compose daemon's NodeService control
  plane (list/onboard/approve/revoke) and owns the node↔group bindings that
  grant a group's members access to a node.
* **Team self-service** — a group's members act on the nodes bound to their
  group. The permission is *derived from the bound node's role*, not a separate
  flag: a group bound to a ``management`` node may launch new execution nodes
  through it (those auto-bind back to the group); a group bound to an
  ``execution`` node may only dispatch sessions to it.

The daemon stays the single authority for live node status/ownership. Local
``GroupNode`` rows only record which nodes a group may touch; every list call
re-reads live status from the daemon and joins it onto the local bindings so a
revoked/offline node is reflected immediately.
"""
from __future__ import annotations

import base64
import re
import uuid
from pathlib import Path

from loguru import logger

from .node_client import NodeServerUnavailable, RPCError, get_local_node_client
from . import config
from .models import TeamGroup, TeamGroupMember
from .models_team_admin import GroupNode


# Filenames the node build ships in ``nodes/dist/`` (see ``nodes/build.sh``).
# We only serve these names (basename-only, no path separators) so a client
# cannot walk out of the bin dir. Two roles, each named after its binary:
#
#   node-execution-<os>-<arch>[.exe]
#   agent-compose-node-management-<os>-<arch>[.exe]
#   node-ios-<os>-<arch>[.exe]
#
# The checksums file is included so operators can verify downloads.
_NODE_BIN_NAME_RE = re.compile(
    r"^("
    r"node-execution-(?:linux|darwin|windows)-(?:amd64|arm64)(?:\.exe)?"
    r"|agent-compose-node-management-(?:linux|darwin|windows)-(?:amd64|arm64)(?:\.exe)?"
    r"|node-ios-(?:linux|darwin|windows)-(?:amd64|arm64)(?:\.exe)?"
    r"|checksums-sha256\.txt"
    r")$"
)

# Default tag for the locally-built node image (docker / docker-compose methods).
# The one-click installer builds this on the host from the two role binaries +
# the Dockerfile served at /api/v1/public/nodes/docker/* — no registry push.
# A non-empty ``agent_compose_agent_image`` config overrides it.
_DEFAULT_NODE_IMAGE = "ai-lubricant-node:local"


def _default_node_bin_candidates() -> list[Path]:
    """Dev-friendly search paths when no explicit bin dir is configured.

    Order: the ``nodes/dist`` submodule (its ``build.sh`` writes the
    multi-platform builds), then the legacy in-repo ``node-bin``, then a
    sibling standalone clone of the nodes repo.
    """
    # user_platform/ → ai-lubricant/
    ai_lubricant_root = Path(__file__).resolve().parent.parent
    return [
        ai_lubricant_root / "nodes" / "dist",
        ai_lubricant_root / "node-bin",
        ai_lubricant_root.parent / "ai-lubricant-nodes" / "dist",
    ]


def _resolve_node_bin_dir(root: Path) -> Path | None:
    """Resolve a flat bin dir or the newest versioned release subdirectory."""
    if not root.is_dir():
        return None
    if any(entry.is_file() and _NODE_BIN_NAME_RE.match(entry.name) for entry in root.iterdir()):
        return root
    version_dirs = [
        entry
        for entry in root.iterdir()
        if entry.is_dir()
        and any(
            child.is_file() and _NODE_BIN_NAME_RE.match(child.name)
            for child in entry.iterdir()
        )
    ]
    if not version_dirs:
        return None
    return max(version_dirs, key=lambda entry: (entry.stat().st_mtime, entry.name))


def resolve_node_bin_dir() -> Path | None:
    """Return the directory that holds node binaries, or ``None`` if missing.

    Prefers the configured ``agent_compose_node_bin_dir``; otherwise walks the
    default candidates and returns the first directory that actually exists.
    Both the historical flat layout and ``nodes/pack-release.sh``'s
    ``dist/<version>/`` layout are supported.
    """
    configured = (config.settings.agent_compose_node_bin_dir or "").strip()
    if configured:
        return _resolve_node_bin_dir(Path(configured).expanduser())
    for cand in _default_node_bin_candidates():
        resolved = _resolve_node_bin_dir(cand)
        if resolved is not None:
            return resolved
    return None


def list_node_binaries() -> list[dict]:
    """Enumerate downloadable node binaries under the resolved bin dir.

    Each entry is ``{name, size, os, arch, url}``. ``url`` is the relative API
    path the frontend should hit; the route layer serves the file. Returns an
    empty list when the bin dir is not configured / missing — the console then
    shows a "未配置节点运行程序" hint rather than 500-ing.
    """
    root = resolve_node_bin_dir()
    if root is None:
        return []
    out: list[dict] = []
    for entry in sorted(root.iterdir(), key=lambda p: p.name):
        if not entry.is_file():
            continue
        name = entry.name
        if not _NODE_BIN_NAME_RE.match(name):
            continue
        os_name, arch = _parse_bin_platform(name)
        out.append(
            {
                "name": name,
                "size": entry.stat().st_size,
                "os": os_name,
                "arch": arch,
                "url": f"/api/v1/admin/nodes/binaries/{name}",
            }
        )
    return out


def resolve_node_binary(name: str) -> Path | None:
    """Resolve a single binary by basename, or ``None`` if not allowed/found."""
    if not name or not _NODE_BIN_NAME_RE.match(name):
        return None
    root = resolve_node_bin_dir()
    if root is None:
        return None
    # basename only — the regex already forbids path separators, but re-check
    # so a future regex slip cannot walk out of the bin dir.
    candidate = (root / Path(name).name).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def latest_node_version() -> str:
    """The version string of the node binaries currently served, or "".

    ``nodes/build.sh`` writes the build version to ``dist/VERSION`` (a single
    line). The console compares a node's self-reported ``client_version`` against
    this to decide whether to offer a self-upgrade. Missing file / bin dir → "".
    For the ``nodes/pack-release.sh`` layout (``dist/<version>/`` with no VERSION
    file) the directory name is the build version stamped into the binaries.
    """
    root = resolve_node_bin_dir()
    if root is None:
        return ""
    version_file = root / "VERSION"
    try:
        return version_file.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    # pack-release.sh lays out dist/<version>/ with no VERSION file; the
    # directory name is the build version (-X main.buildVersion stamped in).
    return root.name if root.name and root.name != "dist" else ""


def node_binary_sha256(name: str) -> str:
    """Look up the sha256 for a binary basename from ``checksums-sha256.txt``.

    The checksums file (``<hash>  <name>`` lines, sha256sum format) ships beside
    the binaries. Returns "" when the file or the entry is missing so the caller
    can skip verification rather than fail the upgrade.
    """
    if not name:
        return ""
    root = resolve_node_bin_dir()
    if root is None:
        return ""
    checksums = root / "checksums-sha256.txt"
    try:
        for line in checksums.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[-1].lstrip("*") == name:
                return parts[0].strip()
    except OSError:
        return ""
    return ""


def binary_name_for(role: str, os_name: str, arch: str) -> str:
    """Build the platform binary basename for a node role × os/arch.

    Mirrors ``nodes/build.sh`` naming: execution → ``node-execution-<os>-<arch>``,
    management (either kind) → ``agent-compose-node-management-<os>-<arch>``, with
    a ``.exe`` suffix on windows. The result is validated against the same
    whitelist the download routes enforce, so a bad os/arch raises rather than
    producing an unservable name.
    """
    from .node_client.constants import (
        NODE_ROLE_MANAGEMENT,
        NODE_ROLE_PASSIVE_MANAGEMENT,
        NODE_ROLE_IOS_HOST,
        normalize_node_role,
    )

    role_norm = normalize_node_role(role)
    if role_norm == NODE_ROLE_IOS_HOST:
        # The iOS host is its own binary; see node_server/binaries.py for why
        # this can't fall through to node-execution (wrong binary on self-upgrade).
        base = "node-ios"
    elif role_norm in (NODE_ROLE_MANAGEMENT, NODE_ROLE_PASSIVE_MANAGEMENT):
        base = "agent-compose-node-management"
    else:
        base = "node-execution"
    ext = ".exe" if os_name == "windows" else ""
    name = f"{base}-{os_name}-{arch}{ext}"
    if not _NODE_BIN_NAME_RE.match(name):
        raise ValueError(f"unsupported node platform: {os_name}/{arch}")
    return name


def _parse_bin_platform(name: str) -> tuple[str, str]:
    """Extract (os, arch) from a node binary filename.

    Accepts all three role prefixes (``node-execution-``,
    ``agent-compose-node-management-`` and ``node-ios-``). Checksums / unknown
    names return empty strings so the UI can still list them without inventing a
    platform label.
    """
    m = re.match(
        r"^(?:node-execution|agent-compose-node-management|node-ios)-(linux|darwin|windows)-(amd64|arm64)(?:\.exe)?$",
        name,
    )
    if not m:
        return "", ""
    return m.group(1), m.group(2)


# ── one-click bootstrap: transient install credentials + self-contained script ──
# The one-click `curl <origin>/api/v1/public/nodes/<node_id>/install.sh | bash`
# must run on a fresh node machine that has NO admin session. We gate that public
# endpoint on the node_id alone, so the install parameters (server/secret/role)
# are stashed in a short-TTL Redis record keyed by node_id at onboard time. The
# secret only ever travels in the rendered script body, never in a URL/log.

_NODE_BOOTSTRAP_PREFIX = "mc:node_bootstrap:"
# 30 minutes: long enough to copy the command to a new machine and run it, short
# enough that a leaked node_id is not a durable credential.
_NODE_BOOTSTRAP_TTL_SECONDS = 30 * 60


def _bootstrap_redis():
    """Shared coredis client (same pool/prefix as the rest of the platform)."""
    from rd import JdbcClient

    if JdbcClient.redis is None:
        raise RuntimeError("redis client not initialized")
    return JdbcClient.redis


async def save_node_bootstrap(
    node_id: str,
    *,
    secret: str,
    server_url: str,
    role: str,
    startup_method: str,
    proxy_config_id: str = "",
) -> None:
    """Stash one-click install params under ``node_id`` with a short TTL.

    Best-effort: a Redis failure must not fail onboarding (the operator can
    still use the manual install command shown in the modal), so callers wrap
    this and swallow errors.

    ``proxy_config_id`` is the node's bound egress-proxy pool entry (empty =
    direct). We stash only the *reference*, not the resolved URL with secrets,
    so an operator editing the pool entry's address/password propagates to the
    next install render instead of freezing the old credentials into the script.
    """
    import json as _json

    payload = _json.dumps(
        {
            "node_id": node_id,
            "secret": secret,
            "server_url": server_url,
            "role": role,
            "startup_method": startup_method,
            "proxy_config_id": (proxy_config_id or "").strip(),
        },
        ensure_ascii=False,
    )
    redis = _bootstrap_redis()
    await redis.set(f"{_NODE_BOOTSTRAP_PREFIX}{node_id}", payload, ex=_NODE_BOOTSTRAP_TTL_SECONDS)


async def get_node_bootstrap(node_id: str) -> dict | None:
    """Fetch the transient install params for ``node_id`` (None if missing/expired)."""
    import json as _json

    if not node_id:
        return None
    try:
        redis = _bootstrap_redis()
        raw = await redis.get(f"{_NODE_BOOTSTRAP_PREFIX}{node_id}")
    except Exception:
        logger.debug("[nodes] bootstrap lookup failed", exc_info=True)
        return None
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = _json.loads(raw)
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _sh_squote(value: str) -> str:
    """Single-quote a string for safe embedding in a bash script."""
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


async def _collect_install_assets() -> dict[str, dict[tuple[str, str], dict]]:
    """Read the latest release and split assets into per-role maps.

    Each map is ``{(os, arch) -> {version, url, sha256}}``, keyed by the wire
    role: ``execution`` / ``management`` (the two agent-compose node binaries),
    ``ios_host`` (the node-ios binary), ``runtime`` (the agent-compose-runtime
    tarball). The docker installer needs both role binaries (the locally-built
    image carries both); standalone/bat need the node's own role binary **plus**
    the runtime so a freshly-installed node can report ``runtime_version`` on
    first register without depending on a separately-installed
    agent-compose-runtime or a live ``RuntimeUpgrade`` push (which would
    require the node to already be approved — a deadlock). An ios_host node
    never runs the runtime; it only needs its own node-ios binary. A read
    failure or empty catalog degrades to empty maps — the rendered script then
    reports "no version for this platform" at install time.

    Returns a dict (not a tuple) so adding a role doesn't churn every caller.
    """
    import node_release_catalog

    empty = {"execution": {}, "management": {}, "ios_host": {}, "runtime": {}}
    try:
        latest = await node_release_catalog.get_latest_release()
    except Exception:
        logger.debug("[nodes] install asset lookup failed", exc_info=True)
        return empty
    top_version = str(latest.get("version") or "")

    out: dict[str, dict[tuple[str, str], dict]] = {}
    for wanted_role in ("execution", "management", "ios_host", "runtime"):
        role_map: dict[tuple[str, str], dict] = {}
        for asset in (latest.get("assets") or []):
            if not isinstance(asset, dict) or asset.get("role") != wanted_role:
                continue
            platform = str(asset.get("platform") or "").strip()
            arch = str(asset.get("arch") or "").strip()
            if not platform or not arch:
                continue
            digest = str(asset.get("digest") or "")
            entry = {
                "version": str(asset.get("version") or top_version),
                "url": str(asset.get("download_url") or ""),
                "sha256": digest.split(":", 1)[1] if ":" in digest else digest,
            }
            if wanted_role == "runtime" and (platform, arch) == ("any", "any"):
                # 通用 runtime 包（node-runtime.tar.gz）：登记到每个 (os, arch) 键下，
                # 下游按平台取值时都能命中同一份，安装脚本照常渲染 RT_* 行。
                for plat in ("linux", "darwin", "windows"):
                    for a in ("amd64", "arm64"):
                        role_map[(plat, a)] = entry
                continue
            role_map[(platform, arch)] = entry
        out[wanted_role] = role_map
    return out


def render_install_script(
    bootstrap: dict,
    *,
    public_base_url: str,
    agent_image: str,
    assets: dict[tuple[str, str], dict] | None = None,
    execution_assets: dict[tuple[str, str], dict] | None = None,
    management_assets: dict[tuple[str, str], dict] | None = None,
    ios_host_assets: dict[tuple[str, str], dict] | None = None,
    runtime_assets: dict[tuple[str, str], dict] | None = None,
    proxy_fields: dict | None = None,
) -> str:
    """Render a self-contained bash installer for a node from its bootstrap record.

    The script bakes in server/node_id/secret/role, auto-detects the platform,
    downloads the matching build, and installs the node as ONE host-wide daemon.
    Downloads come from ``node-releases/version.json`` GitHub direct links (the
    same source as upgrade): each platform resolves to its own newest published
    version. The script self-detects OS/arch at run time and picks the matching
    URL; if no version covers the detected platform it prints a clear error and
    exits rather than falling back to a server-side mirror.

    ``execution_assets`` / ``management_assets`` are the per-platform
    ``{(os, arch) -> {version, url, sha256}}`` maps for each role's binary.
    The standalone form only needs this node's own role; the docker form needs
    both (the locally-built image carries both binaries). When a map is empty
    the rendered script still runs and reports the missing-platform error at
    install time. ``assets`` is kept as a backward-compatible alias for
    "this node's role" and overrides the role-derived map when provided.

    ``runtime_assets`` is the per-platform map for the agent-compose-runtime
    tarball. The standalone form downloads + extracts it next to the node's
    state dir so a freshly-installed node reports ``runtime_version`` on its
    first register — without this the approve gate deadlocks (runtime can only
    be pushed via ``RuntimeUpgrade`` after approval, but approval requires
    ``runtime_version``). Empty map → the node registers without a runtime
    and the operator must push ``RuntimeUpgrade`` later.
    """
    node_id = str(bootstrap.get("node_id") or "")
    secret = str(bootstrap.get("secret") or "")
    server_url = str(bootstrap.get("server_url") or "")
    role = str(bootstrap.get("role") or "execution") or "execution"
    method = str(bootstrap.get("startup_method") or "standalone") or "standalone"
    base = (public_base_url or "").rstrip("/")
    execution_assets = execution_assets or {}
    management_assets = management_assets or {}
    ios_host_assets = ios_host_assets or {}
    runtime_assets = runtime_assets or {}
    proxy_fields = proxy_fields or {}
    proxy_mode = str(proxy_fields.get("proxy_mode") or "")
    proxy_url = str(proxy_fields.get("proxy_url") or "")
    proxy_url_prefix = str(proxy_fields.get("proxy_url_prefix") or "")
    if assets is not None:
        role_assets = assets
    elif role == "management":
        role_assets = management_assets
    elif role == "ios_host":
        role_assets = ios_host_assets
    else:
        role_assets = execution_assets

    # The role-specific binaries are named after their role (see
    # ``nodes/build.sh``); they take no ``--role`` flag. ios_host runs the
    # dedicated node-ios binary — falling through to node-execution would
    # install a binary that never speaks the device-control pairing stack.
    binary_name = (
        "node-ios" if role == "ios_host"
        else "agent-compose-node-management" if role == "management"
        else "node-execution"
    )
    image = (agent_image or "").strip()

    q_node = _sh_squote(node_id)
    q_secret = _sh_squote(secret)
    q_server = _sh_squote(server_url)
    q_role = _sh_squote(role)
    q_base = _sh_squote(base)
    q_image = _sh_squote(image)
    q_pmode = _sh_squote(proxy_mode)
    q_purl = _sh_squote(proxy_url)
    q_pprefix = _sh_squote(proxy_url_prefix)

    def _platform_lines(os_name: str, source: dict, prefix: str = "") -> str:
        """Emit ``<PFX>URL_<OS>_<ARCH>=...; <PFX>SHA_<OS>_<ARCH>=...`` for both arches."""
        lines = []
        for arch in ("amd64", "arm64"):
            entry = source.get((os_name, arch))
            url = entry["url"] if entry else ""
            sha = entry["sha256"] if entry else ""
            up = f"{os_name}_{arch}".upper()
            lines.append(f'{prefix}URL_{up}={_sh_squote(url)}')
            lines.append(f'{prefix}SHA_{up}={_sh_squote(sha)}')
        return "\n".join(lines)

    # standalone: this node's own role binary. docker: both roles (the locally
    # built image carries execution + management), keyed off execution/management
    # maps regardless of which role this node is.
    linux_lines = _platform_lines("linux", role_assets)
    darwin_lines = _platform_lines("darwin", role_assets)
    # runtime 是平台无关的通用包（node-runtime.tar.gz）：所有 (os, arch) 键在
    # _collect_install_assets 里指向同一份，取 linux/amd64 即代表全体；一个 URL
    # 一个 sha 烘焙进脚本，不再按平台分列 RT_URL_*。
    rt_entry = runtime_assets.get(("linux", "amd64")) or runtime_assets.get(("linux", "arm64")) or {}
    q_rt_url = _sh_squote(str(rt_entry.get("url") or ""))
    q_rt_sha = _sh_squote(str(rt_entry.get("sha256") or ""))
    rt_lines = "\n".join([
        f'RT_URL={q_rt_url}',
        f'RT_SHA={q_rt_sha}',
    ])
    docker_exec_lines = _platform_lines("linux", execution_assets)
    docker_mgmt_lines = _platform_lines("linux", management_assets, prefix="MGMT_")

    if method in ("docker", "docker-compose"):
        # Container form: the image is built LOCALLY on the host from the two
        # role binaries (execution + management) + the Dockerfile/entrypoint
        # served by this service — nothing is pulled from a public registry.
        # Both binaries' download URLs come from version.json (GitHub direct
        # links, per-platform newest version); the script picks the linux arch
        # of the host. The management binary is needed even for execution nodes
        # so the same local image can host child management nodes / reuse layers.
        return f"""#!/usr/bin/env bash
# agent-compose node one-click installer ({role} · {method}).
set -euo pipefail

# curl | bash inherits the caller's cwd. If that directory was removed during a
# reinstall, bash reports getcwd/cannot-access-parent-directories and later jobs
# inherit the same invalid cwd. Start installation from the user's home instead.
cd "${{HOME:-/}}" 2>/dev/null || cd /

SERVER={q_server}
NODE_ID={q_node}
NODE_SECRET={q_secret}
NODE_ROLE={q_role}
BASE={q_base}
IMAGE={q_image}
PROXY_MODE={q_pmode}
PROXY_URL={q_purl}
PROXY_URL_PREFIX={q_pprefix}
: "${{IMAGE:={_DEFAULT_NODE_IMAGE}}}"

# Proxy for GitHub downloads, baked from the server's global node egress proxy.
# Empty PROXY_MODE means direct. network uses curl --proxy; url_prefix prepends
# a mirror/reverse-proxy base to the asset URL.
download() {{
  local url="$1" dest="$2"
  if [ "$PROXY_MODE" = "url_prefix" ] && [ -n "$PROXY_URL_PREFIX" ]; then
    url="${{PROXY_URL_PREFIX%/}}/${{url#/}}"
  fi
  if [ "$PROXY_MODE" = "network" ] && [ -n "$PROXY_URL" ]; then
    curl -fsSL --proxy "$PROXY_URL" "$url" -o "$dest"
  else
    curl -fsSL "$url" -o "$dest"
  fi
}}

# Per-platform GitHub download URLs for the two role binaries (from
# node-releases/version.json, same source as upgrade). Empty = no published
# version covers that platform.
{docker_exec_lines}
{docker_mgmt_lines}
# Universal runtime archive is baked into the image on the first build.
RT_URL={q_rt_url}
RT_SHA={q_rt_sha}

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required for the {method} method" >&2
  exit 1
fi

# ── build the node image locally (once per host) ──────────────────────────────
if [ "${{AGENT_COMPOSE_NODE_REBUILD:-}}" = "1" ] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) echo "error: unsupported arch '$arch' for the node image" >&2; exit 1 ;;
  esac

  case "$arch" in
    amd64) exec_url="$URL_LINUX_AMD64"; mgmt_url="$MGMT_URL_LINUX_AMD64" ;;
    arm64) exec_url="$URL_LINUX_ARM64"; mgmt_url="$MGMT_URL_LINUX_ARM64" ;;
  esac
  if [ -z "$exec_url" ] || [ -z "$mgmt_url" ]; then
    echo "error: 当前没有支持 linux/$arch 的节点发行版本，请在市场管理页上传该平台版本" >&2
    exit 1
  fi
  if [ -z "$RT_URL" ]; then
    echo "error: 当前没有已发布的 agent-compose runtime，无法构建完整节点镜像" >&2
    exit 1
  fi

  build_dir="$(mktemp -d)"
  trap 'rm -rf "$build_dir"' EXIT

  echo "downloading node binaries (linux/$arch) ..."
  download "$exec_url" "$build_dir/node-execution"
  download "$mgmt_url" "$build_dir/agent-compose-node-management"
  # 镜像 bake runtime：Dockerfile 会把通用 node-runtime.tar.gz 解压进镜像 state 目录并
  # 装齐编辑器 CLI，执行节点首次注册即带 runtime_version/providers，不卡审批门禁。
  # 与二进制同一条 download()（同代理同镜像源），校验失败即中止 build。
  echo "downloading agent-compose runtime (universal) ..."
  download "$RT_URL" "$build_dir/node-runtime.tar.gz"
  if [ -n "$RT_SHA" ]; then
    # docker 形态只在 Linux 宿主机构建，sha256sum 必有（standalone 的 portable
    # sha256() 定义在本分支之后，这里直接用）。
    _rt_actual="$(sha256sum "$build_dir/node-runtime.tar.gz" | awk '{{print $1}}')"
    if [ "$_rt_actual" != "$RT_SHA" ]; then
      echo "error: agent-compose runtime 下载校验失败 ($_rt_actual != $RT_SHA)" >&2
      exit 1
    fi
  fi
  # Dockerfile/entrypoint are served by ai-lubricant (not GitHub release assets),
  # so they deliberately stay direct to BASE; the proxy only routes GitHub.
  curl -fsSL "$BASE/api/v1/public/nodes/docker/Dockerfile" -o "$build_dir/Dockerfile"
  curl -fsSL "$BASE/api/v1/public/nodes/docker/entrypoint.sh" -o "$build_dir/entrypoint.sh"

  echo "building node image $IMAGE (bakes runtime + editor CLIs; first build may take minutes) ..."
  docker build -t "$IMAGE" "$build_dir"
  rm -rf "$build_dir"
  trap - EXIT
else
  echo "node image $IMAGE already present; skipping build (AGENT_COMPOSE_NODE_REBUILD=1 to force)"
fi

CONTAINER="agent-compose-node"
if docker inspect "$CONTAINER" >/dev/null 2>&1; then
  if [ "${{AGENT_COMPOSE_NODE_REPLACE:-}}" != "1" ]; then
    if [ -r /dev/tty ]; then
      printf "An Agent Compose node container already exists. Replace its local node data? [y/N]: " >/dev/tty
      IFS= read -r answer </dev/tty || true
      case "${{answer:-}}" in y|Y|yes|YES) ;; *) echo "cancelled; existing container was left unchanged"; exit 0 ;; esac
    else
      echo "error: existing node container found; set AGENT_COMPOSE_NODE_REPLACE=1 for an approved non-interactive replacement" >&2
      exit 1
    fi
  fi
  docker rm -f "$CONTAINER" >/dev/null
fi
docker run -d \\
  --name "$CONTAINER" \\
  --restart always \\
  -v /var/run/docker.sock:/var/run/docker.sock \\
  -e AGENT_COMPOSE_SERVER="$SERVER" \\
  -e AGENT_COMPOSE_NODE_ID="$NODE_ID" \\
  -e AGENT_COMPOSE_NODE_SECRET="$NODE_SECRET" \\
  -e AGENT_COMPOSE_NODE_ROLE="$NODE_ROLE" \\
  -e AGENT_COMPOSE_AGENT_IMAGE="$IMAGE" \\
  "$IMAGE"
echo "agent-compose node container '$CONTAINER' started (docker logs -f $CONTAINER)"
"""

    # Standalone form: download the matching binary, persist credentials with
    # --install --install-only, then install one host-wide daemon and start it.
    return f"""#!/usr/bin/env bash
# agent-compose node one-click installer ({role} · standalone).
set -euo pipefail

# curl | bash inherits the caller's cwd. If that directory was removed during a
# reinstall, bash reports getcwd/cannot-access-parent-directories and later jobs
# inherit the same invalid cwd. Start installation from the user's home instead.
cd "${{HOME:-/}}" 2>/dev/null || cd /

SERVER={q_server}
NODE_ID={q_node}
NODE_SECRET={q_secret}
NODE_ROLE={q_role}
PROXY_MODE={q_pmode}
PROXY_URL={q_purl}
PROXY_URL_PREFIX={q_pprefix}

# Proxy for GitHub downloads, baked from the server's global node egress proxy.
download() {{
  local url="$1" dest="$2"
  if [ "$PROXY_MODE" = "url_prefix" ] && [ -n "$PROXY_URL_PREFIX" ]; then
    url="${{PROXY_URL_PREFIX%/}}/${{url#/}}"
  fi
  if [ "$PROXY_MODE" = "network" ] && [ -n "$PROXY_URL" ]; then
    curl -fsSL --proxy "$PROXY_URL" "$url" -o "$dest"
  else
    curl -fsSL "$url" -o "$dest"
  fi
}}
# node-releases/version.json, same source as upgrade).
{linux_lines}
{darwin_lines}
# Universal agent-compose runtime archive (node-runtime.tar.gz, platform-
# independent — the node runs it with the host's Node.js). Downloaded at install
# time so the node reports runtime_version on first register — otherwise the
# approve gate deadlocks (runtime can only be pushed after approval, but
# approval requires runtime_version). Empty URL = no published runtime.
{rt_lines}

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$os" in
  linux) os="linux" ;;
  darwin) os="darwin" ;;
  *) echo "error: unsupported OS '$os'; use the Docker method or install manually" >&2; exit 1 ;;
esac
case "$arch" in
  x86_64|amd64) arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) echo "error: unsupported arch '$arch'" >&2; exit 1 ;;
esac

case "$os/$arch" in
  linux/amd64)   url="$URL_LINUX_AMD64";   sha="$SHA_LINUX_AMD64" ;;
  linux/arm64)   url="$URL_LINUX_ARM64";   sha="$SHA_LINUX_ARM64" ;;
  darwin/amd64)  url="$URL_DARWIN_AMD64";  sha="$SHA_DARWIN_AMD64" ;;
  darwin/arm64)  url="$URL_DARWIN_ARM64";  sha="$SHA_DARWIN_ARM64" ;;
esac
rt_url="$RT_URL"
rt_sha="$RT_SHA"
if [ -z "$url" ]; then
  echo "error: 当前没有支持 $os/$arch 的节点发行版本，请在市场管理页上传该平台版本" >&2
  exit 1
fi

# Portable SHA-256: macOS 没有 sha256sum（自带 shasum -a 256）；两个都没有就报错。
# 之前直接调 sha256sum，在 Mac 上 `set -e` + `$(sha256sum …)` 命令替换直接把脚本
# 跪在「line 77: sha256sum: command not found」，下载校验这步整段走不下去。
sha256() {{
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{{print $1}}';
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{{print $1}}';
  else echo "error: 无 sha256 工具（sha256sum/shasum 都没有）" >&2; return 1; fi
}}

# 安装目录：默认 $HOME/.agent-compose（bin/ state/ work/ 三个子目录都落在它下面）。
# 有 tty 时交互提示，回车=用默认；无 tty（curl|bash 管道、ssh -c、cron 等无控制
# 终端的自动化场景）走默认。AGENT_COMPOSE_NODE_HOME 环境变量可绕过提示（自动化用），
# 优先级：env > 输入 > 默认。docker / docker-compose 形态不走本分支，只 standalone 提示。
default_root="$HOME/.agent-compose"
if [ -n "${{AGENT_COMPOSE_NODE_HOME:-}}" ]; then
  root="${{AGENT_COMPOSE_NODE_HOME%/}}"
else
  root="$default_root"
  # 同时检查可读可写 + 用 read 自身成败兜底：有的环境 -r /dev/tty 为真但实际无控制
  # 终端（如本仓库的 bash 工具沙箱），printf/read 会失败——此时不要卡住，落回默认。
  if [ -r /dev/tty ] && [ -w /dev/tty ]; then
    printf "安装目录（回车用默认 %s）: " "$default_root" >/dev/tty 2>/dev/null || true
    _answer=""
    IFS= read -r _answer </dev/tty 2>/dev/null || _answer=""
    [ -n "$_answer" ] && root="${{_answer%/}}"
  fi
fi
[ -n "$root" ] || root="$default_root"
bindir="${{AGENT_COMPOSE_NODE_BIN_DIR:-$root/bin}}"
state_dir="${{AGENT_COMPOSE_NODE_STATE_DIR:-$root/state}}"
work_root="${{AGENT_COMPOSE_NODE_WORK_ROOT:-$root/work}}"
mkdir -p "$bindir" "$state_dir" "$work_root"
binary="$bindir/{binary_name}"

echo "downloading {binary_name} ($os/$arch) ..."
download "$url" "$binary"
chmod +x "$binary"
if [ -n "$sha" ]; then
  actual="$(sha256 "$binary")"
  if [ "$actual" != "$sha" ]; then
    echo "error: 下载校验失败 ($actual != $sha)" >&2
    exit 1
  fi
fi

# Install the universal agent-compose-runtime archive (node-runtime.tar.gz,
# platform-independent) before the first register. pack-release.sh creates a
# tar.gz whose top-level directory is exactly ``runtime/``; extract it into the
# install root's state/ subdir so RuntimeDir() (= stateDir/runtime) sees
# runtime/dist/cli.js. stateDir 由 launcher 导出的 AGENT_COMPOSE_NODE_STATE_DIR
# 指向 $root/state，与这里解压位置一致。
if [ -z "$rt_url" ]; then
  echo "error: 当前没有已发布的 agent-compose runtime（node-runtime.tar.gz），请在市场管理页上传该通用包" >&2
  exit 1
fi
mkdir -p "$state_dir"
rt_archive="$state_dir/runtime.tar.gz.new"
rt_stage="$state_dir/runtime-stage.new"
rm -f "$rt_archive"
rm -rf "$rt_stage"
echo "downloading agent-compose runtime (universal) ..."
download "$rt_url" "$rt_archive"
if [ -n "$rt_sha" ]; then
  actual="$(sha256 "$rt_archive")"
  if [ "$actual" != "$rt_sha" ]; then
    echo "error: agent-compose runtime 下载校验失败 ($actual != $rt_sha)" >&2
    rm -f "$rt_archive"
    exit 1
  fi
fi
mkdir -p "$rt_stage"
if ! tar -xzf "$rt_archive" -C "$rt_stage"; then
  echo "error: agent-compose runtime 解压失败" >&2
  rm -f "$rt_archive"
  rm -rf "$rt_stage"
  exit 1
fi
if [ ! -f "$rt_stage/runtime/dist/cli.js" ]; then
  echo "error: agent-compose runtime 归档缺少 runtime/dist/cli.js" >&2
  rm -f "$rt_archive"
  rm -rf "$rt_stage"
  exit 1
fi
rm -rf "$state_dir/runtime"
mv "$rt_stage/runtime" "$state_dir/runtime"
rm -f "$rt_archive"
rm -rf "$rt_stage"

# Stop any old node we recognize so the new one can take the host lock. Only
# processes whose executable lives under our bindir are touched.
pkill -f "$bindir/node-execution" 2>/dev/null || true
pkill -f "$bindir/agent-compose-node-management" 2>/dev/null || true

# Persist the single set of credentials and take the host-wide lock. --install-only
# saves the config and exits; the launcher below owns staying in the foreground.
# --work-root 一并落盘：work 目录由安装时的选择决定（默认 $root/work），重启后
# 节点从持久化 config.WorkRoot 读回，不会漂回 ~/.cache。
# AGENT_COMPOSE_NODE_STATE_DIR 必须与下面 launcher 导出的完全一致：凭据
# config.json 写在 state 目录下（Go 侧 ConfigPath 与锁/owner 走同一个 stateDir
# 解析）。不设时 install 写到默认 UserConfigDir（macOS 是
# ~/Library/Application Support/agent-compose/node，Linux 是
# ~/.config/agent-compose/node），而 launcher 拉起的进程读 $root/state —— 两边
# 不一致时节点一启动就报「node is not installed on this machine」退出。
echo "saving node configuration..."
AGENT_COMPOSE_NODE_STATE_DIR="$state_dir" "$binary" --install --install-only --yes --server "$SERVER" --node-id "$NODE_ID" --secret "$NODE_SECRET" --work-root "$work_root"

# One secret-free launcher. Reinstalling with a different role overwrites it so the
# single daemon entry always runs the current node. __STATE_DIR__ 占位符在安装时
# sed 注入：state 目录不走持久化 config（Go 侧读 AGENT_COMPOSE_NODE_STATE_DIR 环境变量），
# 所以 launcher 必须导出它，systemd/crontab 拉起的进程才有正确的 state 落点。
cat > "$root/start-node.sh" <<'LAUNCHER'
#!/usr/bin/env bash
set -euo pipefail
# A startup shell may inherit a cwd that the installer just replaced/removed. Bash
# emits `getcwd: cannot access parent directories` before the node can log anything.
# Move to a durable, known-existing directory before resolving launcher-relative paths.
cd "$(dirname "$0")"
# Fixed Agent Compose node launcher. Credentials are read from the saved config.
export AGENT_COMPOSE_NODE_STATE_DIR="__STATE_DIR__"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting node: cwd=$PWD state=$AGENT_COMPOSE_NODE_STATE_DIR config=$AGENT_COMPOSE_NODE_STATE_DIR/config.json" >&2
if [ ! -f "$AGENT_COMPOSE_NODE_STATE_DIR/config.json" ]; then
  echo "node launcher: config missing: $AGENT_COMPOSE_NODE_STATE_DIR/config.json" >&2
  exit 2
fi
exec "./bin/{binary_name}"
LAUNCHER
sed -i.bak "s#__STATE_DIR__#$state_dir#g" "$root/start-node.sh"
rm -f "$root/start-node.sh.bak"
chmod +x "$root/start-node.sh"

if command -v systemctl >/dev/null 2>&1 && systemctl --user show >/dev/null 2>&1; then
  mkdir -p "$HOME/.config/systemd/user"
  cat > "$HOME/.config/systemd/user/agent-compose-node.service" <<'UNIT'
[Unit]
Description=Agent Compose node
After=network-online.target

[Service]
Type=simple
ExecStart=__ROOT__/start-node.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
UNIT
  sed -i.bak "s#__ROOT__#$root#g" "$HOME/.config/systemd/user/agent-compose-node.service"
  rm -f "$HOME/.config/systemd/user/agent-compose-node.service.bak"
  systemctl --user daemon-reload
  systemctl --user enable --now agent-compose-node.service
  echo "installed and started the agent-compose-node systemd user service."
  echo "  unit: $HOME/.config/systemd/user/agent-compose-node.service"
  echo "  logs: systemctl --user status agent-compose-node.service"
else
  # No usable systemd user session (macOS, or a systemd-less linux): do NOT
  # install crontab autostart here. The node binary asks the operator once at
  # its next interactive start (launchd on macOS / crontab-free) and manages
  # the per-user entry itself; a silent root cron row would never match that
  # choice. Start the fixed launcher now in the background so the installer
  # can return instead of becoming the node process.
  ( cd "$root" && nohup ./start-node.sh >> ./node.log 2>&1 </dev/null & )
  echo "agent-compose node started (log: $root/node.log)."
  echo "  launcher: $root/start-node.sh"
  echo "  autostart: 未配置 — 首次交互式启动节点时会询问是否开启开机自启"
fi

# Desktop entry "Ai Lubricant 节点" (macOS): a double-clickable .command that
# pops the start/stop/restart dialog served by the node binary's `service`
# subcommand. Finder runs .command files via Terminal; the dialog itself is
# native (osascript). On linux the same content lands as a Desktop .desktop
# file when a desktop dir exists.
desktop_dir="$HOME/Desktop"
if [ -d "$desktop_dir" ]; then
  if [ "$(uname -s)" = "Darwin" ]; then
    cat > "$desktop_dir/Ai Lubricant 节点.command" <<'ENTRY'
#!/bin/bash
# Ai Lubricant 节点 — 桌面服务入口：启动 / 关闭 / 重启节点服务。
exec "{root}/bin/{binary_name}" service
ENTRY
    chmod +x "$desktop_dir/Ai Lubricant 节点.command"
  else
    cat > "$desktop_dir/ai-lubricant-node.desktop" <<'ENTRY'
[Desktop Entry]
Type=Application
Name=Ai Lubricant 节点
Comment=启动 / 关闭 / 重启节点服务
Exec={root}/bin/{binary_name} service
Terminal=true
ENTRY
    chmod +x "$desktop_dir/ai-lubricant-node.desktop"
  fi
  echo "desktop entry: $desktop_dir"
fi
echo ""
echo "agent-compose node installed to: $root"
echo "  binary : $bindir/{binary_name}"
echo "  config : $state_dir/config.json"
echo "  runtime: $state_dir/runtime/dist/cli.js"
echo "  work   : $work_root (task workspaces)"
"""


def render_install_bat(
    bootstrap: dict,
    *,
    public_base_url: str,
    assets: dict[tuple[str, str], dict] | None = None,
    runtime_assets: dict[tuple[str, str], dict] | None = None,
    proxy_fields: dict | None = None,
) -> str:
    """Render a Windows batch installer for a credentialed node.

    The script downloads the Windows binary for the host's architecture and the
    one platform-independent ``node-runtime.tar.gz`` shared by all node roles
    that run agent sessions (execution/management). ``ios_host`` only needs its
    dedicated node-ios binary and does not install the runtime.

    Downloads come from ``node-releases/version.json`` direct links (the same
    source as upgrade). ``assets`` is the per-platform
    ``{(os, arch) -> {version, url, sha256}}`` map for this role's binary; the
    script picks the Windows amd64 or arm64 URL based on
    ``PROCESSOR_ARCHITECTURE`` and errors out when no published version covers
    the detected arch (no server-side mirror fallback). ``runtime_assets`` maps
    every platform key to the same universal runtime entry (see
    ``_collect_install_assets``); one ``RT_URL``/``RT_SHA`` covers all arches.
    """
    node_id = str(bootstrap.get("node_id") or "")
    secret = str(bootstrap.get("secret") or "")
    server_url = str(bootstrap.get("server_url") or "")
    role = str(bootstrap.get("role") or "execution") or "execution"
    base = (public_base_url or "").rstrip("/")
    assets = assets or {}
    runtime_assets = runtime_assets or {}

    # The role-specific binaries are named after their role (see
    # ``nodes/build.sh``); they take no ``--role`` flag. ios_host runs the
    # dedicated node-ios binary — falling through to node-execution would
    # install a binary that never speaks the device-control pairing stack.
    binary_name = (
        "node-ios" if role == "ios_host"
        else "agent-compose-node-management" if role == "management"
        else "node-execution"
    )

    amd64 = assets.get(("windows", "amd64"), {})
    arm64 = assets.get(("windows", "arm64"), {})
    url_amd64 = str(amd64.get("url") or "")
    sha_amd64 = str(amd64.get("sha256") or "")
    url_arm64 = str(arm64.get("url") or "")
    sha_arm64 = str(arm64.get("sha256") or "")
    # node-runtime.tar.gz 是平台无关的通用包，所有 Windows 架构共用同一个 URL/SHA。
    rt_entry = runtime_assets.get(("windows", "amd64")) or runtime_assets.get(("linux", "amd64")) or {}
    rt_url = str(rt_entry.get("url") or "")
    rt_sha = str(rt_entry.get("sha256") or "")
    proxy_fields = proxy_fields or {}
    proxy_mode = str(proxy_fields.get("proxy_mode") or "")
    proxy_url = str(proxy_fields.get("proxy_url") or "")
    proxy_url_prefix = str(proxy_fields.get("proxy_url_prefix") or "")

    # Values are generated from UUID/base32/HTTP inputs. Keep them in `set "..."`
    # assignments so spaces in a deployment URL do not split the command.
    # The desktop shortcut's display name is Chinese ("Ai Lubricant 节点"). The
    # .bat is served as UTF-8, but cmd.exe parses .bat files with the OEM
    # codepage, so Chinese embedded directly in the source would mangle. We ship
    # the shortcut creation as a PowerShell UTF-16LE Base64 command (`-EncodedCommand`)
    # — PowerShell decodes it itself, never touching cmd's codepage.
    ps_script = (
        "$ErrorActionPreference='Stop';"
        "$w=New-Object -ComObject WScript.Shell;"
        "$p=[Environment]::GetFolderPath('Desktop')+'\\Ai Lubricant 节点.lnk';"
        "$s=$w.CreateShortcut($p);"
        "$s.TargetPath='%LOCALAPPDATA%\\agent-compose\\bin\\{binary_name}.exe';"
        "$s.Arguments='service';"
        "$s.WorkingDirectory='%LOCALAPPDATA%\\agent-compose';"
        "$s.WindowStyle=7;"
        "$s.Description='Ai Lubricant 节点：启动 / 关闭 / 重启节点服务';"
        "$s.Save();"
        # Drop the legacy ASCII-named shortcuts after an upgrade install.
        "$old1=[Environment]::GetFolderPath('Desktop')+'\\AiLubricantNode.lnk';"
        "$old2=[Environment]::GetFolderPath('Desktop')+'\\Agent Compose Node.lnk';"
        "foreach($o in @($old1,$old2)){ if(Test-Path $o){ Remove-Item $o -Force } }"
    )
    ps_b64 = base64.b64encode(ps_script.encode("utf-16-le")).decode("ascii")
    ps_shortcut_cmd = (
        f"powershell.exe -NoProfile -ExecutionPolicy Bypass -EncodedCommand {ps_b64}"
        " >nul 2>&1\r\n"
        "if errorlevel 1 echo warning: could not create desktop shortcut >&2"
    )
    return f"""@echo off
setlocal EnableExtensions DisableDelayedExpansion
rem Agent Compose node Windows installer ({role}).
rem Installs ONE host-wide node with one fixed startup task, one launcher and one
rem desktop shortcut; migrates any older per-node tasks to the single fixed task.
set "SERVER={server_url}"
set "NODE_ID={node_id}"
set "NODE_SECRET={secret}"
set "NODE_ROLE={role}"
set "BASE={base}"
set "ROOT=%LOCALAPPDATA%\\agent-compose"
set "BINDIR=%ROOT%\\bin"
set "RUNNER=%ROOT%\\start-node.cmd"
set "BINARY=%BINDIR%\\{binary_name}.exe"
set "URL_AMD64={url_amd64}"
set "SHA_AMD64={sha_amd64}"
set "URL_ARM64={url_arm64}"
set "SHA_ARM64={sha_arm64}"
set "RT_URL={rt_url}"
set "RT_SHA={rt_sha}"
set "PROXY_MODE={proxy_mode}"
set "PROXY_URL={proxy_url}"
set "PROXY_URL_PREFIX={proxy_url_prefix}"
set "TASK=agent-compose-node"
set "CONFIG=%APPDATA%\\agent-compose\\node\\config.json"
set "STATE_DIR=%APPDATA%\\agent-compose\\node"
set "RT_DIR=%STATE_DIR%\\runtime"
set "SHORTCUT=%USERPROFILE%\\Desktop\\AiLubricantNode.lnk"

if not exist "%ROOT%" mkdir "%ROOT%"
if not exist "%BINDIR%" mkdir "%BINDIR%"
where curl.exe >nul 2>&1
if errorlevel 1 (
  echo error: curl.exe is required on Windows 10/11 >&2
  exit /b 1
)
where tar.exe >nul 2>&1
if errorlevel 1 (
  echo error: tar.exe is required on Windows 10/11 to install agent-compose runtime >&2
  echo Install a current Windows 10/11 tar tool, then rerun this installer. >&2
  exit /b 1
)

rem Pick the download URL for this machine's arch from version.json (GitHub
rem direct link, same source as upgrade). An empty URL means no published
rem version covers that arch — fail loudly so the operator can upload it.
rem The runtime (node-runtime.tar.gz) is platform-independent: one URL for all.
set "URL="
set "SHA="
if /I "%PROCESSOR_ARCHITECTURE%"=="AMD64" (
  set "URL=%URL_AMD64%"
  set "SHA=%SHA_AMD64%"
) else if /I "%PROCESSOR_ARCHITECTURE%"=="ARM64" (
  set "URL=%URL_ARM64%"
  set "SHA=%SHA_ARM64%"
)
if not defined URL (
  echo error: unrecognized Windows arch "%PROCESSOR_ARCHITECTURE%" >&2
  exit /b 1
)
if "%URL%"=="" (
  echo error: no published node release covers Windows %PROCESSOR_ARCHITECTURE%; upload that platform in the marketplace admin first >&2
  exit /b 1
)
if "%RT_URL%"=="" (
  echo error: no published agent-compose runtime archive; upload node-runtime.tar.gz in the marketplace admin first >&2
  exit /b 1
)

echo.
echo This installer keeps exactly one Agent Compose node on this computer.
echo   new node: %NODE_ID% ^(%NODE_ROLE%^)
echo Old local node data, legacy scheduled tasks and node processes under
echo %BINDIR% will be replaced. Server-side node records are NOT deleted.
echo.
set "NEEDS_CONFIRM=0"
if exist "%CONFIG%" set "NEEDS_CONFIRM=1"
schtasks.exe /Query /TN "%TASK%" >nul 2>&1 && set "NEEDS_CONFIRM=1"
schtasks.exe /Query /FO CSV /NH 2>nul | findstr /I /C:"agent-compose-node-node-" >nul 2>&1 && set "NEEDS_CONFIRM=1"
if not "%NEEDS_CONFIRM%"=="1" goto :confirmed
set "ANSWER=N"
set /p "ANSWER=Clean old local node data and install this node? [y/N]: "
if /I "%ANSWER%"=="y" goto :confirmed
if /I "%ANSWER%"=="yes" goto :confirmed
echo Cancelled. Existing node data was not changed.
exit /b 0
:confirmed

echo stopping known Agent Compose node processes...
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$root=[IO.Path]::GetFullPath('%BINDIR%').TrimEnd([char]92)+[char]92; Get-CimInstance Win32_Process | Where-Object {{ $_.ExecutablePath -and [IO.Path]::GetFullPath($_.ExecutablePath).StartsWith($root,[StringComparison]::OrdinalIgnoreCase) -and ($_.Name -like 'node-execution*' -or $_.Name -like 'agent-compose-node-management*') }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }}" >nul 2>&1

echo migrating old startup tasks...
schtasks.exe /Delete /F /TN "%TASK%" >nul 2>&1
for /f "tokens=1 delims=," %%T in ('schtasks.exe /Query /FO CSV /NH 2^>nul ^| findstr /I /C:"agent-compose-node-node-"') do schtasks.exe /Delete /F /TN %%~T >nul 2>&1
del /q "%ROOT%\\run-node-*.bat" >nul 2>&1

echo downloading {binary_name}...
set "DL_URL=%URL%"
set "DL_PROXY="
if /I "%PROXY_MODE%"=="url_prefix" if not "%PROXY_URL_PREFIX%"=="" (
  set "DL_URL=%PROXY_URL_PREFIX%/%URL%"
)
if /I "%PROXY_MODE%"=="network" if not "%PROXY_URL%"=="" set "DL_PROXY=--proxy %PROXY_URL%"
curl.exe -fL %DL_PROXY% "%DL_URL%" -o "%BINARY%.new"
if errorlevel 1 (
  del /q "%BINARY%.new" >nul 2>&1
  echo error: failed to download the node program >&2
  exit /b 1
)
if not "%SHA%"=="" (
  rem certutil prints line1=header, line2=hash, line3=completed message.
  rem The hash check runs inside a parenthesized block, so plain %ACTUAL%
  rem expands at parse time before the for-loop sets it and would always
  rem read empty. Enable delayed expansion for this block and use !ACTUAL! so
  rem the value set by the for-loop is read at run time.
  setlocal EnableDelayedExpansion
  set "ACTUAL="
  for /f "skip=1 tokens=* delims=" %%H in ('certutil.exe -hashfile "%BINARY%.new" SHA256') do if not defined ACTUAL set "ACTUAL=%%H"
  set "ACTUAL=!ACTUAL: =!"
  if /I not "!ACTUAL!"=="%SHA%" goto :bin_mismatch
  endlocal
  goto :bin_ok
)
goto :bin_ok
:bin_mismatch
del /q "%BINARY%.new" >nul 2>&1
echo error: 下载校验失败 >&2
echo   期望 %SHA% >&2
echo   实际 !ACTUAL! >&2
endlocal
exit /b 1
:bin_ok
move /y "%BINARY%.new" "%BINARY%" >nul
if errorlevel 1 (
  echo error: failed to install the node program >&2
  exit /b 1
)

rem Install the matching agent-compose-runtime archive before the first register.
rem pack-release.sh produces a tar.gz whose top-level directory is exactly
rem "runtime/" - extract it into Go's stateDir so RuntimeDir sees
rem runtime/dist/cli.js and the node reports runtime_version on its first
rem register. Without this the approve gate deadlocks: runtime can only be
rem pushed after approval, but approval requires runtime_version.
echo downloading agent-compose runtime...
set "RT_DL_URL=%RT_URL%"
set "RT_ARCHIVE=%STATE_DIR%\\runtime.tar.gz.new"
set "RT_STAGE=%STATE_DIR%\\runtime-stage.new"
set "RT_DL_PROXY="
if not exist "%STATE_DIR%" mkdir "%STATE_DIR%"
del /q "%RT_ARCHIVE%" >nul 2>&1
rmdir /s /q "%RT_STAGE%" >nul 2>&1
rem Resolve the download URL + proxy the same way the binary download does:
rem url_prefix rewrites the URL in place; network sets a --proxy flag. curl is
rem then run ONCE outside any if-block so its errorlevel is reliable (cmd's
rem "if A if B (X) else (Y)" binds the else to the inner if, silently skipping
rem the download when the outer condition is false).
if /I "%PROXY_MODE%"=="url_prefix" if not "%PROXY_URL_PREFIX%"=="" set "RT_DL_URL=%PROXY_URL_PREFIX%/%RT_URL%"
if /I "%PROXY_MODE%"=="network" if not "%PROXY_URL%"=="" set "RT_DL_PROXY=--proxy %PROXY_URL%"
curl.exe -fL %RT_DL_PROXY% "%RT_DL_URL%" -o "%RT_ARCHIVE%"
if errorlevel 1 (
  del /q "%RT_ARCHIVE%" >nul 2>&1
  echo error: failed to download the agent-compose runtime >&2
  exit /b 1
)
if not "%RT_SHA%"=="" (
  rem certutil prints line1=header, line2=hash, line3=completed message.
  rem The hash check runs inside a parenthesized block, so plain %ACTUAL%
  rem expands at parse time before the for-loop sets it. Enable delayed
  rem expansion and use !ACTUAL! so the for-loop value is read at run time.
  setlocal EnableDelayedExpansion
  set "ACTUAL="
  for /f "skip=1 tokens=* delims=" %%H in ('certutil.exe -hashfile "%RT_ARCHIVE%" SHA256') do if not defined ACTUAL set "ACTUAL=%%H"
  set "ACTUAL=!ACTUAL: =!"
  if /I not "!ACTUAL!"=="%RT_SHA%" goto :rt_mismatch
  endlocal
  goto :rt_extract
)
goto :rt_extract
:rt_mismatch
del /q "%RT_ARCHIVE%" >nul 2>&1
echo error: agent-compose runtime checksum mismatch >&2
echo   expect: %RT_SHA% >&2
echo   actual: !ACTUAL! >&2
endlocal
exit /b 1
:rt_extract
mkdir "%RT_STAGE%" 2>nul
rem Windows bsdtar treats a "C:\\path" archive argument as a host:path remote
rem spec because of the drive-letter colon, and --force-local double-escapes the
rem backslashes (surfaces as "Cannot open: C\\:\\Users..."). Pushd into the state
rem dir and use relative archive + -C paths so no drive colon reaches tar.
pushd "%STATE_DIR%"
tar.exe -xzf "runtime.tar.gz.new" -C "runtime-stage.new"
set "RT_TAR_EXIT=%errorlevel%"
popd
if not "%RT_TAR_EXIT%"=="0" (
  del /q "%RT_ARCHIVE%" >nul 2>&1
  rmdir /s /q "%RT_STAGE%" >nul 2>&1
  echo error: failed to extract the agent-compose runtime archive >&2
  exit /b 1
)
if not exist "%RT_STAGE%\\runtime\\dist\\cli.js" (
  del /q "%RT_ARCHIVE%" >nul 2>&1
  rmdir /s /q "%RT_STAGE%" >nul 2>&1
  echo error: agent-compose runtime archive is missing runtime\\dist\\cli.js >&2
  exit /b 1
)
rmdir /s /q "%RT_DIR%" >nul 2>&1
move /y "%RT_STAGE%\\runtime" "%RT_DIR%" >nul
del /q "%RT_ARCHIVE%" >nul 2>&1
rmdir /s /q "%RT_STAGE%" >nul 2>&1

rem The Go node owns the persisted credential write + host-wide single-instance
rem lock. --yes records the approval already collected above; --install-only saves
rem the config and exits. The secret is never written into the runner, the
rem scheduled task or the desktop shortcut.
echo saving node configuration...
"%BINARY%" --install --install-only --yes --server "%SERVER%" --node-id "%NODE_ID%" --secret "%NODE_SECRET%"
if errorlevel 2 (
  echo error: failed to install node configuration >&2
  exit /b 1
)

rem One fixed, secret-free launcher. Reinstalling with a different role overwrites
rem it so the single desktop entry/task always starts the current node.
>"%RUNNER%" echo @echo off
>>"%RUNNER%" echo rem Fixed Agent Compose node launcher. Credentials are read from the saved config.
>>"%RUNNER%" echo "%BINARY%"

rem Replace every old per-node task with one stable per-user login task.
schtasks.exe /Create /F /SC ONLOGON /RL LIMITED /TN "%TASK%" /TR "cmd.exe /d /c call \\\"%RUNNER%\\\"" >nul
if errorlevel 1 (
  echo error: failed to register the node in Task Scheduler >&2
  exit /b 1
)

rem One stable desktop entry. Reinstalling updates the same shortcut. It runs
rem the node binary's `service` subcommand - a start/stop/restart dialog -
rem instead of launching the node directly, so the desktop entry manages the
rem service rather than being it. The visible name is Chinese; cmd reads .bat
rem files with the OEM codepage, so the Chinese must never appear as cmd
rem source. The .lnk creation runs as a PowerShell **Base64 command** decoded
rem from UTF-16LE at runtime: the .bat stays pure ASCII on the wire.
{ps_shortcut_cmd}

rem Start it now. The host lock makes a second launch print "already running" and
rem exit, so this never produces a duplicate process.
start "Ai Lubricant node" /min cmd.exe /d /c call "%RUNNER%"

echo.
echo Agent Compose node installed.
echo   task:     %TASK% ^(logon autostart^)
echo   launcher: %RUNNER%
echo   desktop:  %SHORTCUT% ^(start/stop/restart dialog^)
echo   config:   %CONFIG%
echo You can double-click the desktop shortcut to start, stop or restart the node service.
endlocal
"""


class NodesServiceError(RuntimeError):
    """Domain error with a stable ``reason`` for the route layer to map."""

    def __init__(self, reason: str, message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason


def _maybe_uuid(value: str | None) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None


class NodesService:
    """Storage-side node↔group bindings + agent-compose control-plane proxy."""

    # ── admin: node control plane (in-process NodeService) ──────────────────

    async def list_nodes(self, status: str | None = None) -> dict:
        """List all known nodes (optionally filtered by status)."""
        client = get_local_node_client()
        nodes = await client.list_nodes(status)
        return {"nodes": nodes}

    async def get_node_if_exists(self, node_id: str) -> dict | None:
        """Look up one node in the authoritative control-plane ledger."""
        client = get_local_node_client()
        nodes = await client.list_nodes()
        wanted = (node_id or "").strip()
        return next((node for node in nodes if node.get("node_id") == wanted), None)

    async def get_node_detail(self, node_id: str) -> dict:
        """单节点详情 + 服务端算好的版本判定（节点程序 / runtime 两条线）。

        节点详情弹框打开、升级/装编辑器后回调都调这个接口，拿到该节点最新快照以及
        runtime 与节点程序各自的 current/latest/needs_upgrade。前端不再自己拿全局
        version.json 错误地把节点程序版本当 runtime 版本比——判定统一在服务端完成。
        节点不存在返回 NodesServiceError(not_found)。
        """
        wanted = (node_id or "").strip()
        node = await self.get_node_if_exists(wanted)
        if node is None:
            raise NodesServiceError("not_found", f"节点 {wanted} 不存在")
        caps = node.get("capabilities") or {}
        os_name = str(caps.get("os") or "").strip()
        arch = str(caps.get("arch") or "").strip()
        node_role = str(node.get("role") or node.get("node_role") or "").strip()

        import node_release_catalog as nrc

        latest = await nrc.get_latest_release()
        if os_name and arch:
            upgrade = nrc.node_upgrade_status(
                latest,
                node_role=node_role,
                os_name=os_name,
                arch=arch,
                current_node_version=str(caps.get("client_version") or ""),
                current_runtime_version=str(caps.get("runtime_version") or ""),
            )
        else:
            # 节点尚未上报 os/arch：无法选平台资产，给出空判定（消费侧据此不显示升级）。
            upgrade = {
                "stale": bool(latest.get("stale")),
                "node_program": {"current": "", "latest": "", "installed": False, "needs_upgrade": False},
                "runtime": {"current": "", "latest": "", "installed": False, "needs_upgrade": False},
                "can_upgrade": False,
            }
        return {"node": node, "upgrade": upgrade}

    async def onboard_node(
        self,
        role: str = "execution",
        startup_method: str = "standalone",
        node_name: str | None = None,
        manager_node_id: str | None = None,
        labels: dict[str, str] | None = None,
        proxy_config_id: str = "",
    ) -> dict:
        """Pre-create a pending node and mint its one-time TOTP secret + install command."""
        client = get_local_node_client()
        return await client.onboard_node(
            role=role,
            startup_method=startup_method,
            node_name=node_name,
            manager_node_id=manager_node_id,
            labels=labels,
            proxy_config_id=proxy_config_id,
        )

    async def approve_node(self, node_id: str) -> dict:
        client = get_local_node_client()
        return await client.approve_node(node_id)

    async def revoke_node(self, node_id: str) -> dict:
        client = get_local_node_client()
        return await client.revoke_node(node_id)

    async def _delete_bindings_for_node(self, node_id: str) -> int:
        """Delete every local group grant for a control-plane node that is gone."""
        nid = (node_id or "").strip()
        try:
            deleted = await GroupNode.filter(node_id=nid).delete()
        except Exception as exc:  # noqa: BLE001 - control-plane mutation already committed
            logger.warning("[nodes] cleanup bindings for {} failed: {}", node_id, exc)
            return 0
        if deleted:
            logger.info("[nodes] removed {} binding(s) for deleted node {}", deleted, node_id)
        # A node being hard-deleted/revoked may still carry a TaskNodeBinding
        # (exclusive occupation) whose task never reached a terminal cleanup —
        # e.g. the process died mid-dispatch. That zombie row would permanently
        # pin node_id via unique(node_id) and block any re-registration reusing
        # the same id. Drop them here alongside the group grants.
        try:
            from .models_task import TaskNodeBinding

            stale = await TaskNodeBinding.filter(node_id=nid).delete()
        except Exception as exc:  # noqa: BLE001 - control-plane mutation already committed
            logger.warning("[nodes] cleanup task bindings for {} failed: {}", node_id, exc)
            stale = 0
        if stale:
            logger.info("[nodes] removed {} stale TaskNodeBinding(s) for node {}", stale, node_id)
        return int(deleted or 0)

    async def delete_node(self, node_id: str) -> dict:
        """Hard-delete a node and remove every local group binding to it."""
        client = get_local_node_client()
        result = await client.delete_node(node_id)
        if result.get("deleted"):
            await self._delete_bindings_for_node(node_id)
            try:
                from .shell_approval_service import clear_node_policy

                await clear_node_policy(node_id)
            except Exception as exc:  # noqa: BLE001 - control-plane mutation already committed
                logger.warning("[nodes] cleanup shell approval for {} failed: {}", node_id, exc)
        return result

    async def revoke_onboard_node(self, node_id: str) -> dict:
        client = get_local_node_client()
        result = await client.revoke_onboard_node(node_id)
        # Unregistered onboard rows are physically deleted; registered rows are
        # merely revoked and must stay bound so users can see the revoked state.
        if result.get("deleted"):
            await self._delete_bindings_for_node(node_id)
        return result

    async def move_node(self, node_id: str, manager_node_id: str) -> dict:
        """把执行节点挪到另一个管理节点下。返回归一化节点信息。"""
        client = get_local_node_client()
        return await client.move_node(node_id, manager_node_id)

    async def get_public_ip_lookup_config(self) -> dict:
        client = get_local_node_client()
        return await client.get_public_ip_lookup_config()

    async def update_public_ip_lookup_config(
        self, ipv4_urls: list[str], ipv6_urls: list[str]
    ) -> dict:
        client = get_local_node_client()
        return await client.update_public_ip_lookup_config(ipv4_urls, ipv6_urls)

    async def get_node_proxy_config(self, node_id: str) -> dict:
        """读取节点绑定的出口代理（proxy_config_id + 解析后的三字段）。"""
        client = get_local_node_client()
        return await client.get_node_proxy_config(node_id)

    async def update_node_proxy_config(self, node_id: str, proxy_config_id: str = "") -> dict:
        """设置节点绑定的出口代理并推送给该在线节点；空 id 即直连。

        离线节点下次连上时由 NodeConnect 的 node_proxy_config_frame 补发。
        """
        client = get_local_node_client()
        return await client.update_node_proxy_config(node_id, proxy_config_id)

    async def resolved_proxy_fields_for(self, proxy_config_id: str) -> dict:
        """安装脚本烘焙代理用：解析单个 proxy_config_id，返回 network/url_prefix 三字段。

        安装阶段节点尚未连接，无法接收推送的代理；服务端把该节点绑定的
        ``proxy_config_id`` 解析后烘焙进 install 脚本。只存 id 引用而非解析
        后的带密 URL，代理池条目改了密码/地址下一次 install 自动生效。

        空/未知/解析失败都退化为空字典（直连），安装永不因代理问题 500。
        ``node`` 隧道模式对节点自身下载无意义，resolve_proxy 会拒绝，这里
        同样降级为直连——onboard 时应已校验拦截，这里是兜底。
        """
        from node_upgrade_targets import UpgradeTargetError, resolve_proxy

        try:
            return await resolve_proxy(proxy_config_id)
        except UpgradeTargetError:
            logger.debug("[nodes] install proxy resolve failed", exc_info=True)
            return {}

    async def set_node_capacity(self, node_id: str, capacity: dict | None) -> dict:
        """Set a node's operator-configured capacity limits (count/CPU/memory)."""
        client = get_local_node_client()
        return await client.set_node_capacity(node_id, capacity)

    async def manage_editor(self, node_id: str, editor: str, action: str) -> dict:
        """在某个在线节点上安装/升级一个编辑器 CLI（claude/codex/gemini/opencode/cursor）。"""
        client = get_local_node_client()
        return await client.manage_editor(node_id, editor, action)

    async def refresh_node_labels(self, node_id: str) -> dict:
        """让在线节点重新探测全部能力标签（无需重启节点进程）。

        控制台节点详情「刷新标签」按钮走这里：运维者在节点主机上装/卸了
        编辑器、host tool 或改了 --labels 后，点一下即可让注册口径的能力
        快照即时更新。节点回复与注册时同构的 NodeCapabilities，控制面合并
        进存量 capabilities（保留 role/hostname/server_seen_address 等服务端
        簿记）。
        """
        client = get_local_node_client()
        return await client.refresh_node_labels(node_id)

    async def install_host_tool(
        self, node_id: str, tool: str, *, proxy_config_id: str = ""
    ) -> dict:
        """让在线节点安装宿主级运行依赖（nodejs 归档安装 / xcode 检测引导）。

        环境面板 / 审批弹窗的「安装」按钮走这里。nodejs：节点进程无需 root，
        装 nodejs.org 官方归档并前置 PATH（节点端 InstallHostTool 帧处理），
        审批硬门禁缺 node/npm 时也能点——控制面守卫与 manage_editor 同口径
        （在线即可，不卡审批）。xcode 是**检测型**工具：Xcode 为 App Store
        专供（~7GB、Apple ID、交互式许可），节点无法自动下载——下发后节点只
        做 xcodebuild 探测，已装则 ok，未装则 ack 带上「为什么不能自动装 + 手动
        步骤」的原因，前端原样展示。

        nodejs 的下载地址不再凭命名规则拼接（Go 的 GOARCH 是 amd64，Node 官方
        归档叫 x64，拼出来必 404），而是调镜像真实清单解析：拉
        ``{mirror}/v{version}/SHASUMS256.txt``（nodejs.org/npmmirror 布局一致
        的权威文件列表），按节点 os/arch 挑出确实存在的归档名并取 sha256
        （节点端就不再抓 SHASUMS 兜底）。解析经节点将使用的同一条代理路线。
        """
        node = await self.get_node_if_exists(node_id)
        if node is None:
            raise NodesServiceError("not_found", f"节点 {node_id} 不存在")
        caps = node.get("capabilities") or {}
        os_name = str(caps.get("os") or "").strip()
        arch = str(caps.get("arch") or "").strip()
        if not os_name or not arch:
            raise NodesServiceError("invalid_argument", "节点尚未上报 os/arch，无法选择安装方式")

        if tool == "xcode":
            # 异步任务模式（2026-09 定稿）：解析 xcodereleases 目录 → .xip 直链
            # → 下发 HostToolJob 帧，节点后台下载/解压/激活，进度经事件帧轮询。
            # 旧的同步 InstallHostTool 检测仍可用，但不再是主入口。
            if os_name != "darwin":
                raise NodesServiceError("invalid_argument", "Xcode 仅可安装在 macOS 节点上")
            return await self.start_xcode_install_job(node_id, proxy_config_id=proxy_config_id)

        # 没显式传代理就用节点常驻绑定；再空才直连（与 upgrade_node 同规则）。
        chosen_proxy_id = (proxy_config_id or "").strip() or str(node.get("proxy_config_id") or "")
        from node_upgrade_targets import UpgradeTargetError, resolve_nodejs_asset, resolve_proxy

        try:
            proxy_fields = await resolve_proxy(chosen_proxy_id)
        except Exception as exc:
            raise NodesServiceError("invalid_argument", str(exc)) from exc

        version = (config.settings.agent_compose_nodejs_version or "").strip()
        mirror = (config.settings.agent_compose_nodejs_mirror or "https://nodejs.org/dist").rstrip("/")
        if not version or not mirror:
            raise NodesServiceError("failed_precondition", "服务端未配置 Node.js 版本/镜像")
        try:
            asset = await resolve_nodejs_asset(mirror, version, os_name, arch, proxy_fields)
        except UpgradeTargetError as exc:
            raise NodesServiceError("invalid_argument", str(exc)) from exc
        target = {
            "target_version": version,
            "download_url": asset["download_url"],
            "sha256": asset["sha256"],
            **proxy_fields,
        }
        client = get_local_node_client()
        return await client.install_host_tool(node_id, tool, target=target)

    # ── 异步 Xcode 安装任务（xcodereleases 目录解析 + HostToolJob 帧下发） ──────

    async def start_xcode_install_job(
        self, node_id: str, *, target_version: str = "", proxy_config_id: str = ""
    ) -> dict:
        """启动一次 Xcode 自动安装任务（异步任务模式，立即返回 job_id）。

        版本与 .xip 直链在服务端解析（xcodereleases.com 目录，TTL 缓存）：
        target_version 为空时取配置默认（XCODE_TARGET_VERSION，默认 latest =
        最新非 beta 且兼容节点 macOS 的版本）。下载一律直连：.xip 最终 302
        到 Apple 官方 CDN，节点直连即可，且 12GB 归档过代理易被截断/改写
        （「archive is damaged」的实测根因）。proxy_config_id 仅保留 API 兼容，
        实际被忽略——目录直连拉取，任务帧显式 proxy_mode="direct" 覆盖节点
        端残留的代理快照。job_id 由数据侧铸造（前缀 htj-），节点受理 ack 后
        立刻返回，进度/结果走事件与 result 帧，前端用 get_xcode_install_job
        轮询、cancel_xcode_install_job 取消。
        """
        node = await self.get_node_if_exists(node_id)
        if node is None:
            raise NodesServiceError("not_found", f"节点 {node_id} 不存在")
        caps = node.get("capabilities") or {}
        if str(caps.get("os") or "").strip().lower() != "darwin":
            raise NodesServiceError("invalid_argument", "Xcode 仅可安装在 macOS 节点上")

        # Xcode 的 .xip 最终 302 到 Apple 官方 CDN，节点直连下载——不经任何
        # 出口代理（12GB 大文件过代理被截断/改写是「archive is damaged」的
        # 头号成因，且该链路已实测可直连）。proxy_config_id 仍接受（API 兼容）
        # 但被忽略：目录直连拉取，任务帧显式带 proxy_mode="direct"，覆盖节点
        # 端可能残留的代理快照回退。
        proxy_fields: dict = {"proxy_mode": "direct"}
        desired = (target_version or "").strip() or str(
            config.settings.xcode_target_version or "latest"
        )
        from . import xcode_releases

        try:
            target = await xcode_releases.resolve(desired, proxy_fields=proxy_fields)
        except xcode_releases.XcodeReleasesError as exc:
            raise NodesServiceError("invalid_argument", str(exc)) from exc

        job_id = f"htj-{uuid.uuid4().hex[:12]}"
        client = get_local_node_client()
        result = await client.start_host_tool_job(
            node_id,
            job_id,
            tool="xcode",
            target_version=target["target_version"],
            download_url=target["download_url"],
            download_size_bytes=int(target.get("download_size_bytes") or 0),
            sha256="",
            timeout_seconds=0,
            **proxy_fields,
        )
        return {
            "node_id": node_id,
            "job_id": job_id,
            "status": result.get("status") or "accepted",
            "target_version": target["target_version"],
            "download_url": target["download_url"],
            "download_size_bytes": target.get("download_size_bytes") or 0,
            "requires_macos": target.get("requires_macos") or "",
            "beta": bool(target.get("beta")),
            "stale": bool(target.get("stale")),
        }

    async def get_xcode_install_job(self, node_id: str, job_id: str) -> dict:
        """轮询 Xcode 安装任务快照（控制面内存快照，转发）。"""
        client = get_local_node_client()
        return await client.get_host_tool_job_status(node_id, job_id)

    async def cancel_xcode_install_job(self, node_id: str, job_id: str) -> dict:
        """请求取消运行中的 Xcode 安装任务（节点在阶段边界协作取消）。"""
        client = get_local_node_client()
        return await client.cancel_host_tool_job(node_id, job_id)

    # ── 共享环境：node_server 只做「下发 + 回报磁盘事实」，不持台账 ──────────────
    # 用户级台账（哪些环境、各自的资源集）在 environment_service，这里只把
    # create/remove/sync/inspect 透传给 node-server 控制面。
    async def manage_environment(self, node_id: str, env_id: str, action: str) -> dict:
        """在节点上创建/删除一个共享环境目录（create 幂等；remove 回收磁盘）。"""
        client = get_local_node_client()
        return await client.manage_environment(node_id, env_id, action)

    async def sync_environment(
        self, node_id: str, env_id: str, skills: list[dict], plugins: list[dict]
    ) -> dict:
        """把环境的期望 skill/plugin 集安装进节点上该环境的 HOME（真安装/卸载）。"""
        client = get_local_node_client()
        return await client.sync_environment(node_id, env_id, skills, plugins)

    async def inspect_environment(self, node_id: str, env_id: str) -> dict:
        """回报节点上该环境 HOME 里物理装了哪些 skill/plugin，供服务端做差异。"""
        client = get_local_node_client()
        return await client.inspect_environment(node_id, env_id)

    # ── 系统内置环境（节点操作者的真实 HOME）────────────────────────────────
    # 与上面共用环境的三个方法平行，但不带 env_id：一个节点只有一个操作者 HOME，
    # node_id 本身就是地址。语义差别见 node_server/service.py 里的注释——共用环境
    # 是精确集合同步，系统环境只能增量、且只能删平台自己装的。

    async def inspect_system_env(self, node_id: str, provider: str = "") -> dict:
        """回报操作者 HOME 里各 provider 实际会发现的 skill/plugin/MCP（只读）。"""
        client = get_local_node_client()
        return await client.inspect_system_env(node_id, provider)

    async def sync_system_env(
        self,
        node_id: str,
        skills: list[dict],
        plugins: list[dict],
        overwrite: bool = False,
        remove: list[str] | None = None,
    ) -> dict:
        """把平台资源增量装进操作者 HOME / 删平台装过的（绝不 prune 操作者自有）。"""
        client = get_local_node_client()
        return await client.sync_system_env(node_id, skills, plugins, overwrite, remove)

    async def archive_system_env_resource(
        self, node_id: str, kind: str, name: str, upload_url: str, upload_token: str
    ) -> dict:
        """让节点把操作者 HOME 里的某个资源打包上传，用于归档进平台资源库。"""
        client = get_local_node_client()
        return await client.archive_system_env_resource(
            node_id, kind, name, upload_url, upload_token
        )

    async def upgrade_node(self, node_id: str, *, proxy_config_id: str = "") -> dict:
        """统一升级节点：先 Runtime，再节点程序，两个步骤共用一个代理。

        ``proxy_config_id`` 为空时回退到该节点记录上的常驻绑定
        (``proxy_config_id``)，再空才直连——升级默认沿用节点自己的代理。
        成功后才把这次用的 proxy_config_id 落到节点的 ``last_proxy_config_id``，
        供下次升级弹窗预选；失败不落，语义=「上次成功升级」。
        """
        node = await self.get_node_if_exists(node_id)
        if node is None:
            raise NodesServiceError("not_found", f"节点 {node_id} 不存在")
        caps = node.get("capabilities") or {}
        os_name = str(caps.get("os") or "").strip()
        arch = str(caps.get("arch") or "").strip()
        if not os_name or not arch:
            raise NodesServiceError("invalid_argument", "节点尚未上报 os/arch，无法选择升级资产")

        import node_release_catalog
        from node_upgrade_targets import UpgradeTargetError, probe_asset, resolve_proxy

        # 没显式传代理就用节点常驻绑定；再空才直连。
        chosen_proxy_id = (proxy_config_id or "").strip() or str(node.get("proxy_config_id") or "")

        latest = await node_release_catalog.get_latest_release()
        selected = node_release_catalog.select_upgrade_assets(
            latest,
            node_role=str(node.get("role") or node.get("node_role") or ""),
            os_name=os_name,
            arch=arch,
        )
        if not selected.get("version"):
            raise NodesServiceError("failed_precondition", "当前没有可用的节点发行版本")
        runtime_asset = selected.get("runtime")
        node_asset = selected.get("node")
        if runtime_asset is None and node_asset is None:
            raise NodesServiceError(
                "failed_precondition",
                f"版本 {selected['version']} 没有匹配节点 {os_name}/{arch} 的升级资产",
            )
        try:
            proxy_fields = await resolve_proxy(chosen_proxy_id)
            for asset in (runtime_asset, node_asset):
                if asset is not None:
                    await probe_asset(str(asset.get("download_url") or ""), proxy_fields)
        except UpgradeTargetError as exc:
            raise NodesServiceError("invalid_argument", str(exc)) from exc

        def target(asset: dict | None) -> dict | None:
            if asset is None:
                return None
            digest = str(asset.get("digest") or "")
            # version.json 按 (role,platform,arch) 各自取最新合并，runtime 与 node
            # 两条资产可能来自不同版本；target_version 用该资产自带的 version（回退
            # 到顶层 version），而不是全局单一 version。
            asset_version = str(asset.get("version") or selected["version"] or "")
            return {
                "target_version": asset_version,
                "download_url": str(asset.get("download_url") or ""),
                "sha256": digest.split(":", 1)[1] if ":" in digest else digest,
                **proxy_fields,
            }

        client = get_local_node_client()
        result = await client.upgrade_node(
            node_id,
            runtime_target=target(runtime_asset),
            node_target=target(node_asset),
        )
        # 升级 RPC 成功才落「上次成功升级用的代理」；失败不落。
        try:
            await client.set_node_last_proxy(node_id, chosen_proxy_id)
        except Exception:  # noqa: BLE001 - 记忆默认值绝不能让一次成功升级失败
            logger.debug("[nodes] set last_proxy for {} failed", node_id, exc_info=True)
        return result

    # ── node ↔ group bindings ───────────────────────────────────────────────

    async def _live_node_map(self) -> dict[str, dict]:
        """Best-effort map of node_id → live NodeInfo from the in-process server.

        Failures degrade to an empty map so a bound-node listing still returns
        the local rows (marked as status unknown) rather than 500-ing when the
        node server is unavailable (e.g. disabled via the escape hatch).
        """
        return (await self._live_node_map_ex())[0]

    async def node_live_info(self, node_id: str) -> dict | None:
        """Live NodeInfo for one node, or None when it isn't in the live ledger.

        Note the entry is present for offline nodes too (with ``online=False``
        and a possibly stale capability snapshot), so callers that gate on a
        reported capability must check ``online`` themselves rather than treat
        presence as liveness.
        """
        node_id = (node_id or "").strip()
        if not node_id:
            return None
        return (await self._live_node_map()).get(node_id)

    async def _live_node_map_ex(self) -> tuple[dict[str, dict], bool]:
        """Like ``_live_node_map`` but also reports if the control plane answered.

        The boolean lets callers tell "the node service is offline" apart from
        "the node genuinely isn't in the ledger": when ``list_nodes`` failed,
        every entry carries a stale ``unknown`` status and execution nodes
        granted only via a management binding vanish from the derived view. The
        C-side console uses this to label a task's missing node as
        节点服务离线 instead of asserting 节点不存在.
        """
        try:
            nodes = await get_local_node_client().list_nodes()
        except (NodeServerUnavailable, RPCError) as exc:
            logger.warning("[nodes] live node lookup failed; degrading: {}", exc)
            return {}, False
        return {n.get("node_id"): n for n in nodes if n.get("node_id")}, True

    async def reconcile_bindings(self) -> dict[str, int | bool]:
        """Reconcile local grants against the authoritative control-plane ledger.

        Transport failures and a transient empty ledger are deliberately non-
        destructive: neither may be interpreted as "delete every grant".
        """
        try:
            live_nodes = await get_local_node_client().list_nodes()
        except (NodeServerUnavailable, RPCError) as exc:
            logger.warning("[nodes] binding reconciliation skipped: {}", exc)
            return {"skipped": True, "deleted": 0, "updated": 0}

        live_map = {n.get("node_id"): n for n in live_nodes if n.get("node_id")}
        if not live_map:
            logger.warning("[nodes] binding reconciliation skipped: control plane returned no nodes")
            return {"skipped": True, "deleted": 0, "updated": 0}

        links = list(await GroupNode.all())
        deleted = 0
        updated = 0
        for link in links:
            live = live_map.get(link.node_id)
            if live is None:
                deleted += int(await GroupNode.filter(id=link.id).delete() or 0)
                continue
            next_role = live.get("role") or link.node_role
            next_name = live.get("node_name") or link.node_name
            changed: list[str] = []
            if next_role != link.node_role:
                link.node_role = next_role
                changed.append("node_role")
            if next_name != link.node_name:
                link.node_name = next_name
                changed.append("node_name")
            if changed:
                await link.save(update_fields=changed)
                updated += 1

        if deleted or updated:
            logger.info(
                "[nodes] binding reconciliation completed: deleted={}, updated={}",
                deleted,
                updated,
            )
        return {"skipped": False, "deleted": deleted, "updated": updated}

    async def _attach_editor_occupancy(self, live_map: dict[str, dict]) -> None:
        """Join static non-deleted editor assignments into live node rows."""
        if not live_map:
            return
        try:
            from db import PostgresClient

            counts = await PostgresClient.count_editors_by_node(list(live_map))
        except Exception:  # noqa: BLE001 - node listing must degrade, not fail
            counts = {}
        for node_id, row in live_map.items():
            row["editor_occupancy"] = int(counts.get(node_id, 0))

    def _binding_dict(self, link: GroupNode, live: dict | None) -> dict:
        """Join a local binding with live daemon state (live wins when present).

        ``is_passive``/``capabilities`` come only from live state (the local
        binding row doesn't snapshot them); they let the C-side render the same
        manager→execution tree the admin page shows (container vs. manageable
        management node, machine specs, editors). Both degrade to a safe default
        when the node server is unavailable.
        """
        return {
            "group_id": str(link.group_id),
            "node_id": link.node_id,
            "node_name": (live or {}).get("node_name") or link.node_name or link.node_id,
            # Keep the common live-node projection aligned with admin /nodes.
            # ``node_role`` remains as the C-side compatibility alias.
            "role": (live or {}).get("role") or link.node_role,
            "node_role": (live or {}).get("role") or link.node_role,
            "is_passive": bool((live or {}).get("is_passive")),
            # 节点自报的 startup_method capability（"autostart"/"standalone"，
            # 反映安装后真实的启动形态）优先；台账值（onboard 时管理员选的
            # standalone/systemd/docker 形态）只在节点还没上报时兜底。
            "startup_method": (
                ((live or {}).get("capabilities") or {}).get("startup_method")
                or (live or {}).get("startup_method")
                or ""
            ),
            "ledger_startup_method": (live or {}).get("startup_method") or "",
            "status": (live or {}).get("status") or "unknown",
            "connected": bool((live or {}).get("connected")),
            # Heartbeat freshness is distinct from a registered connection.
            "online": bool((live or {}).get("online")),
            "manager_node_id": (live or {}).get("manager_node_id") or "",
            "last_heartbeat_at": (live or {}).get("last_heartbeat_at") or "",
            "active_session_ids": list((live or {}).get("active_session_ids") or []),
            "capabilities": (live or {}).get("capabilities") or {},
            # Capacity (operator-configured limits) + live occupancy so the
            # C-side node views mirror the manager execution-node display.
            "capacity": (live or {}).get("capacity") or {},
            "active_sessions": int((live or {}).get("active_sessions") or 0),
            "editor_occupancy": int((live or {}).get("editor_occupancy") or 0),
            "bound_at": int(link.created_at.timestamp()) if link.created_at else 0,
        }

    async def list_group_node_picker(self, team_id: str, group_id: str) -> dict:
        """List all live daemon nodes with binding state for one team group."""
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise NodesServiceError("group_not_found")
        live_map = await self._live_node_map()
        links = await GroupNode.filter(group_id=group.id)
        bound_ids = {link.node_id for link in links}
        nodes = []
        for node_id, info in live_map.items():
            node = dict(info)
            node["node_id"] = node.get("node_id") or node_id
            node["bound"] = node["node_id"] in bound_ids
            nodes.append(node)
        return {"nodes": nodes}

    async def bind_team_group_node(self, team_id: str, group_id: str, node_id: str) -> dict:
        if await self._owned_group(team_id, group_id) is None:
            raise NodesServiceError("group_not_found")
        return await self.bind_node_to_group(group_id, node_id)

    async def unbind_team_group_node(self, team_id: str, group_id: str, node_id: str) -> bool:
        if await self._owned_group(team_id, group_id) is None:
            raise NodesServiceError("group_not_found")
        return await self.unbind_node_from_group(group_id, node_id)

    def _derived_child_dicts(
        self,
        links: list[GroupNode],
        live_map: dict[str, dict],
        seen: set[str],
    ) -> list[dict]:
        """Execution nodes granted indirectly by a bound *management* node.

        Binding a management node grants everything it owns: every live
        execution node whose ``manager_node_id`` is one of the bound management
        nodes is returned as a binding dict borrowing the manager's link (so
        ``group_id``/``bound_at`` point at the grant that authorizes it).
        ``seen`` is updated in place and pre-seeded with the ids already listed
        directly, so a child that also has its own binding is not duplicated.
        """
        managers: dict[str, GroupNode] = {}
        for link in links:
            if link.node_role == "management":
                managers.setdefault(link.node_id, link)
        if not managers:
            return []
        derived: list[dict] = []
        for node_id, info in live_map.items():
            if node_id in seen:
                continue
            if (info.get("role") or "") != "execution":
                continue
            parent = managers.get(info.get("manager_node_id") or "")
            if parent is None:
                continue
            seen.add(node_id)
            row = self._binding_dict(parent, info)
            row["node_id"] = node_id
            derived.append(row)
        return derived

    def _context_manager_dict(self, node_id: str, live: dict) -> dict:
        """A *display-only* management-node row, for tree context only.

        Used when an execution node is granted but its parent management node is
        NOT bound to any of the caller's groups: the frontend still needs the
        manager row to render the manager→execution tree. This row is built from
        live daemon state alone (no ``GroupNode`` binding backs it), so it grants
        **no** rights — it never enters a binding, so ``user_can_use_node`` and
        ``_group_manager_node_ids`` (launch rights) are unaffected. ``display_only``
        tells the UI to render it as read-only grouping context, not a resource
        the user controls.

        Deliberately minimal: only the id/name/role needed to label the parent
        row. Status, connectivity and machine capabilities are withheld — the
        caller was not granted this node, so it should learn nothing about the
        machine beyond the fact that its execution node hangs off it.
        """
        return {
            "group_id": "",
            "node_id": node_id,
            "node_name": live.get("node_name") or node_id,
            "node_role": live.get("role") or "management",
            "role": live.get("role") or "management",
            "is_passive": bool(live.get("is_passive")),
            "startup_method": (
                ((live.get("capabilities") or {}).get("startup_method"))
                or live.get("startup_method")
                or ""
            ),
            "status": "",
            "connected": False,
            "online": False,
            "manager_node_id": "",
            "last_heartbeat_at": "",
            "active_session_ids": [],
            "capabilities": {},
            "capacity": {},
            "active_sessions": 0,
            "editor_occupancy": 0,
            "bound_at": 0,
            "display_only": True,
        }

    def _context_manager_dicts(
        self, nodes: list[dict], live_map: dict[str, dict]
    ) -> list[dict]:
        """Display-only parents for granted execution nodes with an unbound manager.

        Scans the already-assembled ``nodes`` for execution entries whose
        ``manager_node_id`` is not itself present, and returns one display-only
        row per such manager (from live state). Without this, a directly-granted
        execution node has no visible parent and the UI files it under an
        "unmanaged" bucket even though it really belongs to a known manager.
        """
        present = {n.get("node_id") for n in nodes if n.get("node_id")}
        wanted: set[str] = set()
        for n in nodes:
            if (n.get("node_role") or "") != "execution":
                continue
            mid = n.get("manager_node_id") or ""
            if mid and mid not in present:
                wanted.add(mid)
        out: list[dict] = []
        for mid in sorted(wanted):
            info = live_map.get(mid)
            if not info:
                continue
            out.append(self._context_manager_dict(mid, info))
        return out

    async def list_group_nodes(self, group_id: str) -> dict:
        """List the nodes bound to a group, enriched with live daemon state.

        Includes the execution nodes derived from a bound management node (see
        ``_derived_child_dicts``) so the group's view matches what its members
        may actually use, plus display-only parents for execution nodes whose
        manager is not itself bound (see ``_context_manager_dicts``).
        """
        gid = _maybe_uuid(group_id)
        if gid is None:
            return {"nodes": []}
        links = list(await GroupNode.filter(group_id=gid).order_by("created_at"))
        if not links:
            return {"nodes": []}
        live_map = await self._live_node_map()
        nodes = [self._binding_dict(l, live_map.get(l.node_id)) for l in links]
        nodes.extend(self._derived_child_dicts(links, live_map, {l.node_id for l in links}))
        nodes.extend(self._context_manager_dicts(nodes, live_map))
        return {"nodes": nodes}

    async def bind_node_to_group(self, group_id: str, node_id: str) -> dict:
        """Bind a daemon node to a group, snapshotting its role at bind time.

        The node must exist on the daemon (we read its live role/name). The
        binding is idempotent — re-binding refreshes the role/name snapshot.
        """
        gid = _maybe_uuid(group_id)
        if gid is None:
            raise NodesServiceError("group_not_found")
        node_id = (node_id or "").strip()
        if not node_id:
            raise NodesServiceError("node_id_required")

        live_map = await self._live_node_map()
        live = live_map.get(node_id)
        if live is None:
            raise NodesServiceError("node_not_found", "节点不存在或 daemon 不可达")

        link, _created = await GroupNode.get_or_create(
            group_id=gid,
            node_id=node_id,
            defaults={
                "node_role": live.get("role") or "execution",
                "node_name": live.get("node_name") or node_id,
            },
        )
        # Refresh the snapshot on re-bind so a role/name change is reflected.
        link.node_role = live.get("role") or link.node_role
        link.node_name = live.get("node_name") or link.node_name
        await link.save(update_fields=["node_role", "node_name"])
        return self._binding_dict(link, live)

    async def unbind_node_from_group(self, group_id: str, node_id: str) -> bool:
        gid = _maybe_uuid(group_id)
        if gid is None:
            return False
        deleted = await GroupNode.filter(group_id=gid, node_id=(node_id or "").strip()).delete()
        return bool(deleted)

    # ── team self-service (permission derived from bound node role) ─────────

    async def _group_manager_node_ids(self, group_id: uuid.UUID) -> list[str]:
        """The management nodes bound to a group (empty = no manage rights)."""
        links = await GroupNode.filter(group_id=group_id, node_role="management")
        return [l.node_id for l in links]

    async def list_team_group_nodes(self, team_id: str, group_id: str) -> dict:
        """List a group's bound nodes for its own members (team-scoped)."""
        if await self._owned_group(team_id, group_id) is None:
            raise NodesServiceError("group_not_found")
        return await self.list_group_nodes(group_id)

    async def create_group_execution_node(
        self,
        team_id: str,
        group_id: str,
        startup_method: str = "docker",
        node_name: str | None = None,
        image: str | None = None,
        proxy_config_id: str = "",
        server_url: str = "",
    ) -> dict:
        """Launch a new execution node owned by the group's management node.

        Requires the group to be bound to at least one ``management`` node —
        that is the derived permission. The new execution node is onboarded with
        ``manager_node_id`` = the group's management node (the daemon pins
        ownership and, if that manager is online+approved, tells it to launch
        the node itself). The new node auto-binds back to this group.
        """
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise NodesServiceError("group_not_found")
        managers = await self._group_manager_node_ids(group.id)
        if not managers:
            raise NodesServiceError("no_manage_rights", "该分组未绑定管理节点，无权新建执行节点")
        manager_node_id = managers[0]

        result = await self.onboard_node(
            role="execution",
            startup_method=startup_method,
            node_name=node_name,
            manager_node_id=manager_node_id,
            labels={"guest_image": image} if image else None,
            proxy_config_id=proxy_config_id,
        )
        new_node_id = result.get("node_id")
        if new_node_id:
            # Auto-bind the freshly onboarded node back to this group so the
            # launcher can use it immediately (role snapshot = execution).
            await GroupNode.get_or_create(
                group_id=group.id,
                node_id=new_node_id,
                defaults={
                    "node_role": "execution",
                    "node_name": (result.get("node") or {}).get("node_name") or new_node_id,
                },
            )
        # Persist a bootstrap record so the public install.sh endpoint also works
        # for this node (manual reinstall / a manager that cannot auto-launch):
        # it carries the chosen egress proxy, matching the admin onboard route.
        # Best-effort like the admin path — Redis being down must not fail the
        # group node creation itself.
        if new_node_id and result.get("secret"):
            try:
                await save_node_bootstrap(
                    new_node_id,
                    secret=result["secret"],
                    server_url=server_url,
                    role="execution",
                    startup_method=startup_method,
                    proxy_config_id=proxy_config_id,
                )
            except Exception:
                logger.warning(
                    "[nodes] failed to save team node bootstrap for {}", new_node_id, exc_info=True
                )
        return result

    async def delete_group_execution_node(
        self, team_id: str, group_id: str, node_id: str
    ) -> dict:
        """Revoke a group's execution node and drop its binding.

        Guards horizontal access: the node must be bound to *this* group. The
        daemon-side revoke (which also stops a manager-launched node) is the
        authority; the local binding is removed regardless so the group's view
        is clean.
        """
        group = await self._owned_group(team_id, group_id)
        if group is None:
            raise NodesServiceError("group_not_found")
        node_id = (node_id or "").strip()
        link = await GroupNode.get_or_none(group_id=group.id, node_id=node_id)
        if link is None:
            raise NodesServiceError("node_not_bound", "节点未绑定到该分组")
        result = await self.revoke_onboard_node(node_id)
        await GroupNode.filter(group_id=group.id, node_id=node_id).delete()
        return result

    async def _owned_group(self, team_id: str, group_id: str) -> TeamGroup | None:
        tid = _maybe_uuid(team_id)
        gid = _maybe_uuid(group_id)
        if tid is None or gid is None:
            return None
        return await TeamGroup.get_or_none(id=gid, team_id=tid, is_deleted=False)

    # ── user-scoped node access (membership-derived) ────────────────────────

    async def _user_group_ids(self, user_id: str) -> list[uuid.UUID]:
        """The group ids the user belongs to (via TeamGroupMember).

        Mirrors the standard pattern used elsewhere (team routes):
        ``TeamGroupMember.filter(user_id=...)``. Returns [] on a bad user id.
        """
        uid = _maybe_uuid(user_id)
        if uid is None:
            return []
        return list(await TeamGroupMember.filter(user_id=uid).values_list("group_id", flat=True))

    async def list_my_nodes(self, user_id: str) -> dict:
        """Aggregate the nodes bound to every group the user belongs to.

        Read-only "use" view for the C-side console: a user sees the nodes their
        groups were granted, deduplicated by ``node_id`` (a node may be bound to
        more than one of the user's groups). Live daemon state is fetched once.

        Binding a *management* node also grants its execution children: any live
        execution node whose ``manager_node_id`` is a bound management node is
        surfaced here as a derived entry (the permission mirrors
        ``user_can_use_node``), even when it was never bound individually.

        The reverse case is covered too: when only an execution node is granted
        and its parent management node is NOT bound, a *display-only* manager row
        (see ``_context_manager_dicts``) is appended so the C-side can still draw
        the manager→execution tree. That row carries no binding and grants no
        management rights — it is grouping context only.
        """
        group_ids = await self._user_group_ids(user_id)
        if not group_ids:
            return {"nodes": [], "control_plane_online": True}
        links = list(await GroupNode.filter(group_id__in=group_ids).order_by("created_at"))
        if not links:
            return {"nodes": [], "control_plane_online": True}
        live_map, control_plane_online = await self._live_node_map_ex()
        await self._attach_editor_occupancy(live_map)
        seen: set[str] = set()
        nodes: list[dict] = []
        for link in links:
            if link.node_id in seen:
                continue
            seen.add(link.node_id)
            nodes.append(self._binding_dict(link, live_map.get(link.node_id)))
        nodes.extend(self._derived_child_dicts(links, live_map, seen))
        nodes.extend(self._context_manager_dicts(nodes, live_map))
        return {"nodes": nodes, "control_plane_online": control_plane_online}

    async def user_can_use_node(self, user_id: str, node_id: str) -> GroupNode | None:
        """Return the binding if ``node_id`` is bound to any of the user's groups.

        This is the anti-horizontal-access gate for task node occupation: a user
        may only occupy a node one of their groups was granted. Returns the
        matching ``GroupNode`` (any group), or None when not permitted.

        Derived management rights: if ``node_id`` is a live execution node whose
        ``manager_node_id`` is a *management* node bound to one of the user's
        groups, the management node's binding is returned — granting the child
        without a per-node binding row.
        """
        node_id = (node_id or "").strip()
        if not node_id:
            return None
        group_ids = await self._user_group_ids(user_id)
        if not group_ids:
            return None
        direct = await GroupNode.filter(group_id__in=group_ids, node_id=node_id).first()
        if direct is not None:
            return direct
        # No direct binding — check if a bound management node owns this node.
        live_map = await self._live_node_map()
        info = live_map.get(node_id)
        if not info or (info.get("role") or "") != "execution":
            return None
        manager_id = info.get("manager_node_id") or ""
        if not manager_id:
            return None
        return await GroupNode.filter(
            group_id__in=group_ids, node_id=manager_id, node_role="management"
        ).first()


nodes_service = NodesService()
