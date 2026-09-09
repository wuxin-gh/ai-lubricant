"""Environment-driven configuration for the optional upstream compatibility layer."""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote

from loguru import logger

_TRUE_VALUES = {"1", "true", "yes", "on"}


def _env_raw(env_name: str) -> str | None:
    raw = os.getenv(env_name)
    return raw.strip() if raw is not None and raw.strip() else None


def _resolve(env_name: str, default: str, legacy_name: str | None = None) -> str:
    """Read ``env_name``; fall back to the pre-rebrand ``legacy_name``.

    Both set with different values is a misconfiguration risk (e.g. two
    processes resolving different databases), so it refuses to guess.
    """
    if legacy_name is None:
        raw = _env_raw(env_name)
        return raw if raw is not None else default
    new_raw = _env_raw(env_name)
    legacy_raw = _env_raw(legacy_name)
    if new_raw is not None and legacy_raw is not None and new_raw != legacy_raw:
        raise RuntimeError(
            f"{env_name} and {legacy_name} are both set with different values; "
            f"remove the legacy {legacy_name}"
        )
    if new_raw is not None:
        return new_raw
    if legacy_raw is not None:
        logger.warning("[compat] legacy env {} used; rename to {}", legacy_name, env_name)
        return legacy_raw
    return default


def _resolve_first(env_names: tuple[str, ...], default: str) -> str:
    for env_name in env_names:
        value = _resolve(env_name, "")
        if value:
            return value
    return default


def _resolve_bool(env_name: str, default: bool = False, legacy_name: str | None = None) -> bool:
    if legacy_name is None:
        raw = os.getenv(env_name)
        if raw is None or not raw.strip():
            return default
        return raw.strip().lower() in _TRUE_VALUES
    new_raw = _env_raw(env_name)
    legacy_raw = _env_raw(legacy_name)
    if new_raw is not None and legacy_raw is not None and new_raw != legacy_raw:
        raise RuntimeError(
            f"{env_name} and {legacy_name} are both set with different values; "
            f"remove the legacy {legacy_name}"
        )
    raw = new_raw if new_raw is not None else legacy_raw
    if legacy_raw is not None and new_raw is None:
        logger.warning("[compat] legacy env {} used; rename to {}", legacy_name, env_name)
    if raw is None:
        return default
    return raw.lower() in _TRUE_VALUES


def _resolve_int(env_name: str, default: int, legacy_name: str | None = None) -> int:
    raw = _resolve(env_name, "", legacy_name=legacy_name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _default_gateway_origin() -> str:
    """Loopback origin of THIS data service, derived from its own listen port.

    The agent runtime's LLM endpoint must point at the gateway (which serves
    ``/v1/messages``), never at the node control plane (which does not). This is
    the single-host default so no extra env is required; ``SERVER_HOST`` is
    ignored on purpose — it is a *bind* address (often ``0.0.0.0``), not a
    dialable origin.
    """
    port = _resolve_int("SERVER_PORT", 8001)
    return f"http://127.0.0.1:{port}"


def _is_loopback_host(hostname: str | None) -> bool:
    return (hostname or "") in ("127.0.0.1", "localhost", "::1")


def _explicit_gateway_origin() -> str:
    """管理员显式配置的网关 origin（AI_LUBRICANT_GATEWAY_PUBLIC_URL），未配返回 ""。

    区别于 ``settings.gateway_public_url``（带回环默认值）：调用方用本函数判断
    「绝对 URL 形态」是否成立——只有显式配置过才有绝对形态的意义，未配默认
    回环值的部署一律走节点自拼（spec 发相对路径 + 节点拿 hello 通告的 origin）。
    """
    import os

    for name in ("AI_LUBRICANT_GATEWAY_PUBLIC_URL", "MONKEYCODE_GATEWAY_PUBLIC_URL"):
        raw = os.getenv(name)
        if raw and raw.strip():
            return raw.strip().rstrip("/")
    return ""


def gateway_base_url_for_nodes(settings: "UserPlatformSettings") -> str:
    """节点可达的 MCP 网关基址（scheme://host[:port]，无尾斜杠）。

    MCP 网关 spec 的 url 由服务端拼好下发给节点，节点**回连**平台——所以这个
    基址必须是节点 dial 得到的地址，不是服务端自己的视角。默认值
    （gateway_public_url 未配 → http://127.0.0.1:{port}）只在节点与服务端同机
    时成立；远程节点连 127.0.0.1 会落到节点自己的回环上，SSE 静默连不上，
    MCP 不加载。

    回退启发式：当 gateway_public_url 是回环、而节点控制面公网地址
    （node_server_public_url）是非回环，说明节点是远程的——用节点服务器的
    host 替换回环 host（保留网关自己的端口，两个服务同机不同端口）。
    显式配置了非回环的 gateway_public_url 时原样返回。
    """
    from urllib.parse import urlparse

    base = (settings.gateway_public_url or "").strip().rstrip("/")
    if not base:
        return ""
    gw = urlparse(base)
    if not _is_loopback_host(gw.hostname):
        return base
    node_public = (settings.node_server_public_url or "").strip().rstrip("/")
    if not node_public:
        return base
    node = urlparse(node_public)
    if node.hostname and not _is_loopback_host(node.hostname):
        port = f":{gw.port}" if gw.port else ""
        return f"{node.scheme or 'http'}://{node.hostname}{port}"
    return base



@dataclass(frozen=True)
class UserPlatformSettings:
    enabled: bool
    database_url: str
    user_adapter_enabled: bool
    system_user_id: str
    system_user_name: str
    system_user_email: str
    # Bootstrap admin (idempotent first C-side platform user). Empty email
    # disables seeding — the layer then starts with an empty ``mc_users`` table.
    # This gives a login-able account out of the box for verification.
    bootstrap_admin_email: str
    bootstrap_admin_password: str
    bootstrap_admin_name: str
    # Data-side remote node-control adapter. The base URL is required for calls
    # to the standalone control process; the token is the shared internal
    # secret the control service also reads.
    agent_compose_base_url: str
    node_control_token: str
    agent_compose_timeout: int
    # Directory holding the node binaries (node-execution / agent-compose-node-management)
    # served to node operators for download (the daemon no longer hosts the binary). Empty →
    # the service auto-detects ``nodes/dist`` under the repo, then a sibling
    # ``../agent-compose/dist`` (dev convenience).
    agent_compose_node_bin_dir: str
    # Container image the bootstrap install script runs for docker/docker-compose
    # startup methods, and that a management node reuses when launching its child
    # execution nodes. Defaults to ``ai-lubricant-node:local``: the install script
    # BUILDS this image locally on the node host (from the two role binaries plus
    # the Dockerfile served at /api/v1/public/nodes/docker/*), so nothing is
    # pulled from a public registry. Override to use a registry image instead.
    agent_compose_agent_image: str
    # Optional full execution image for Review-capable Docker nodes. Ordinary
    # nodes keep using agent_compose_agent_image; the Review config page opts in.
    node_review_image: str
    # Host-tool install source (环境面板「安装 Node.js」按钮): which Node.js
    # LTS the server hands to nodes, and the dist mirror the archive URLs are
    # built from. China deployments point the mirror at npmmirror.com/mirrors/node.
    # Archives are fetched by the NODE through its own egress proxy (same route
    # as self/runtime upgrade), not by the server.
    agent_compose_nodejs_version: str
    agent_compose_nodejs_mirror: str
    # In-process NodeService server (the Python reimplementation of the
    # agent-compose daemon's node control plane). Enabled by default when the
    # compatibility layer is enabled; the env switch remains an emergency
    # shutdown escape hatch.
    node_server_enabled: bool
    # AES-256 key encrypting every node's TOTP credential. Owned by the
    # standalone control service; read here only because single-host deploys
    # share one env.ini. Never transmitted.
    node_credential_encryption_key: str
    # Public origin baked into onboarding scriptURL/installCommand for node
    # operators (e.g. https://console.example.com). Empty → relative path only.
    node_server_public_url: str
    # Login/register PoW CAPTCHA. When true, password-login and register require
    # a valid cap.js verification token (redeemed via /api/v1/public/captcha/*).
    # Escape hatch: set false to disable enforcement if the CAPTCHA store (Redis)
    # is unavailable and would otherwise lock users out of login.
    captcha_required: bool
    # Global per-session resource request the scheduler charges against a node's
    # configured capacity (users do not fill these per session). CPU is in cores,
    # memory in bytes. 0 disables that dimension of admission.
    node_default_session_cpu: float
    node_default_session_memory: int
    # Session cookie attrs. Default SameSite=Lax + non-secure — correct for native
    # clients (they ignore these) and same-origin web. For cross-origin browser
    # previews (e.g. mobile web at http://localhost:11190 hitting a remote backend),
    # set samesite="none" and secure=true so the browser accepts and resends the
    # cross-site cookie. Secure=True requires an HTTPS backend (localhost is treated
    # as a secure context, so it also works for local http backends).
    session_cookie_samesite: str
    session_cookie_secure: bool
    # Public origin of THIS data service (the model gateway) as reachable from a
    # node. It is what an agent runtime's ``ANTHROPIC_BASE_URL``/``OPENAI_BASE_URL``
    # is built from, so it must point at the gateway that serves ``/v1/*``.
    #
    # This is deliberately NOT ``node_server_public_url``: that one is the node
    # CONTROL plane (a separate process, default :8003) which serves no ``/v1``
    # LLM API at all. Reusing it made every task's runtime POST to
    # ``http://…:8003/v1/messages`` and take a 404 on every turn — the agent then
    # produced nothing and the task page spun forever with no request ever
    # reaching the gateway. Keep the two origins separate.
    #
    # Empty → derived at call time from SERVER_HOST/SERVER_PORT (see
    # ``gateway_origin``), so a single-host deploy needs no extra config.
    gateway_public_url: str
    # Public origin baked into the webhook callback URL we register on Git
    # platforms (e.g. https://console.example.com). Empty → derive from the
    # request origin at call time so single-host deploys need no extra config.
    webhook_public_origin: str
    # Max concurrent reviews per project (each project gets its own queue).
    # Env-driven global default so it can be tuned for the host without a
    # per-webhook knob; the worker drains the rest of the queue past this.
    review_project_max_concurrency: int


def _resolve_float(env_name: str, default: float) -> float:
    raw = _resolve(env_name, "")
    if not raw:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _derive_main_database_url() -> str:
    """Build a Tortoise/asyncpg DSN from the main PostgreSQL bootstrap config.

    Used when ``AI_LUBRICANT_DATABASE_URL`` is not set: the compatibility layer
    then shares the *same* PostgreSQL database as the main service. This is safe
    because all compat tables are additive ``mc_*`` tables that never clash with
    the main service tables. Returns "" if the main postgres config cannot be
    resolved (compat layer then stays disabled).

    The Tortoise/asyncpg pool defaults to ``maxsize=5`` when the DSN carries no
    ``maxsize`` query param — a silent cap independent of
    ``POSTGRES_POOL_MAX_SIZE``. The main service's asyncpg pool reads that env
    via ``get_postgres_pool_limits()``; without forwarding it here, every
    ``mc_*`` query routed through Tortoise contends for 5 connections while the
    raw ``PostgresClient`` pool sits at 20–100. Forward the same limits onto the
    DSN so both pools share one configured ceiling (caveat: Tortoise parses these
    only when ``AI_LUBRICANT_DATABASE_URL`` is not explicitly set — an explicit
    URL still wins and must carry its own ``?maxsize=`` if a larger pool is
    intended).
    """
    try:
        from bootstrap_config import get_postgres_config, get_postgres_pool_limits

        pg = get_postgres_config()
        limits = get_postgres_pool_limits()
    except Exception:
        return ""
    user = quote(str(pg["user"]), safe="")
    password = quote(str(pg["password"]), safe="")
    host = pg["host"]
    port = pg["port"]
    database = pg["database"]
    base = f"asyncpg://{user}:{password}@{host}:{port}/{database}"
    # Tortoise reads ?maxsize=/?minsize= (see AsyncpgDBClient.__init__); without
    # these it falls back to maxsize=5/minsize=1 regardless of the main pool.
    return f"{base}?maxsize={limits['max_size']}&minsize={limits['min_size']}"


def load_settings() -> UserPlatformSettings:
    database_url = _resolve("AI_LUBRICANT_DATABASE_URL", "", legacy_name="MONKEYCODE_DATABASE_URL")
    if not database_url:
        # No dedicated URL configured: reuse the main service's PostgreSQL
        # database (additive mc_* tables only).
        database_url = _derive_main_database_url()
    return UserPlatformSettings(
        enabled=_resolve_bool("AI_LUBRICANT_COMPAT_ENABLED", legacy_name="MONKEYCODE_COMPAT_ENABLED"),
        database_url=database_url,
        user_adapter_enabled=_resolve_bool("AI_LUBRICANT_USER_ADAPTER_ENABLED", legacy_name="MONKEYCODE_USER_ADAPTER_ENABLED"),
        system_user_id=_resolve("AI_LUBRICANT_SYSTEM_USER_ID", "00000000-0000-0000-0000-000000000001", legacy_name="MONKEYCODE_SYSTEM_USER_ID"),
        system_user_name=_resolve("AI_LUBRICANT_SYSTEM_USER_NAME", "system", legacy_name="MONKEYCODE_SYSTEM_USER_NAME"),
        system_user_email=_resolve("AI_LUBRICANT_SYSTEM_USER_EMAIL", "system@ai-lubricant.local", legacy_name="MONKEYCODE_SYSTEM_USER_EMAIL"),
        agent_compose_base_url=_resolve("AGENT_COMPOSE_BASE_URL", ""),
        node_control_token=_resolve_first(("NODE_CONTROL_TOKEN", "AGENT_COMPOSE_NODE_API_TOKEN"), ""),
        agent_compose_timeout=_resolve_int("AGENT_COMPOSE_TIMEOUT", 30),
        agent_compose_node_bin_dir=_resolve("AGENT_COMPOSE_NODE_BIN_DIR", ""),
        agent_compose_agent_image=_resolve("AGENT_COMPOSE_AGENT_IMAGE", "ai-lubricant-node:local"),
        agent_compose_nodejs_version=_resolve("AGENT_COMPOSE_NODEJS_VERSION", "22.17.0"),
        agent_compose_nodejs_mirror=_resolve("AGENT_COMPOSE_NODEJS_MIRROR", "https://nodejs.org/dist"),
        node_review_image=_resolve("AI_LUBRICANT_NODE_REVIEW_IMAGE", "ai-lubricant-node-review:local", legacy_name="MONKEYCODE_NODE_REVIEW_IMAGE"),
        node_server_enabled=_resolve_bool("AGENT_COMPOSE_NODE_SERVER_ENABLED", default=True),
        node_credential_encryption_key=_resolve_first(("NODE_CREDENTIAL_ENCRYPTION_KEY",), ""),
        node_server_public_url=_resolve("AGENT_COMPOSE_NODE_SERVER_PUBLIC_URL", ""),
        gateway_public_url=_resolve("AI_LUBRICANT_GATEWAY_PUBLIC_URL", _default_gateway_origin(), legacy_name="MONKEYCODE_GATEWAY_PUBLIC_URL"),
        bootstrap_admin_email=_resolve("AI_LUBRICANT_BOOTSTRAP_ADMIN_EMAIL", "", legacy_name="MONKEYCODE_BOOTSTRAP_ADMIN_EMAIL"),
        bootstrap_admin_password=_resolve("AI_LUBRICANT_BOOTSTRAP_ADMIN_PASSWORD", "", legacy_name="MONKEYCODE_BOOTSTRAP_ADMIN_PASSWORD"),
        bootstrap_admin_name=_resolve("AI_LUBRICANT_BOOTSTRAP_ADMIN_NAME", "", legacy_name="MONKEYCODE_BOOTSTRAP_ADMIN_NAME"),
        captcha_required=_resolve_bool("AI_LUBRICANT_CAPTCHA_REQUIRED", True, legacy_name="MONKEYCODE_CAPTCHA_REQUIRED"),
        node_default_session_cpu=max(0.0, _resolve_float("NODE_DEFAULT_SESSION_CPU", 1.0)),
        node_default_session_memory=max(0, _resolve_int("NODE_DEFAULT_SESSION_MEMORY", 1024 * 1024 * 1024)),
        session_cookie_samesite=_resolve("AI_LUBRICANT_SESSION_SAMESITE", "lax", legacy_name="MONKEYCODE_SESSION_SAMESITE").lower(),
        session_cookie_secure=_resolve_bool("AI_LUBRICANT_SESSION_SECURE", False, legacy_name="MONKEYCODE_SESSION_SECURE"),
        webhook_public_origin=_resolve("AI_LUBRICANT_WEBHOOK_PUBLIC_ORIGIN", "", legacy_name="MONKEYCODE_WEBHOOK_PUBLIC_ORIGIN"),
        review_project_max_concurrency=max(1, _resolve_int("AI_LUBRICANT_REVIEW_PROJECT_MAX_CONCURRENCY", 2, legacy_name="MONKEYCODE_REVIEW_PROJECT_MAX_CONCURRENCY")),
    )


settings = load_settings()


def reload_settings() -> UserPlatformSettings:
    """Re-read configuration and rebind the module-level ``settings``.

    Call after mutating ``.env`` at runtime (e.g. the agent-compose server config
    endpoint) so the new values take effect without a process restart. Consumers
    that read ``config.settings`` live (rather than capturing it at import) pick
    up the change immediately.
    """
    global settings
    settings = load_settings()
    return settings

