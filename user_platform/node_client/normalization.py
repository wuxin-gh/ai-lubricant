"""Wire→friendly projection for node information.

Kept byte-compatible with the previous implementation so API responses and
frontend expectations do not change.
"""
from __future__ import annotations

from typing import Any

NODE_ROLE_TO_PROTO = {
    "execution": "NODE_ROLE_EXECUTION",
    "management": "NODE_ROLE_MANAGEMENT",
    "passive_management": "NODE_ROLE_PASSIVE_MANAGEMENT",
    "ios_host": "NODE_ROLE_IOS_HOST",
}
# Both management kinds project to "management" for the frontend; whether a
# management node is a pure grouping container is surfaced via ``is_passive``.
# ios_host keeps its own identity: it is neither an execution node (runs no
# sessions) nor a management node (owns nothing), and the frontend labels it
# separately.
PROTO_TO_NODE_ROLE = {
    "NODE_ROLE_EXECUTION": "execution",
    "NODE_ROLE_MANAGEMENT": "management",
    "NODE_ROLE_PASSIVE_MANAGEMENT": "management",
    "NODE_ROLE_IOS_HOST": "ios_host",
}

NODE_STARTUP_TO_PROTO = {
    "standalone": "NODE_STARTUP_METHOD_STANDALONE",
    "systemd": "NODE_STARTUP_METHOD_SYSTEMD",
    "docker": "NODE_STARTUP_METHOD_DOCKER",
    "docker-compose": "NODE_STARTUP_METHOD_DOCKER_COMPOSE",
}
PROTO_TO_NODE_STARTUP = {v: k for k, v in NODE_STARTUP_TO_PROTO.items()}

NODE_STATUS_TO_PROTO = {
    "pending": "NODE_STATUS_PENDING",
    "approved": "NODE_STATUS_APPROVED",
    "revoked": "NODE_STATUS_REVOKED",
}
PROTO_TO_NODE_STATUS = {v: k for k, v in NODE_STATUS_TO_PROTO.items()}


def flatten_capabilities(caps: Any) -> dict[str, Any]:
    """Flatten Connect's structured ``NodeCapabilities`` JSON for API consumers.

    String labels stay flat (back-compat). The structured ``editors`` array
    (per-provider permission/approval modes the node probed at register time) is
    passed through as-is so the frontend can drive the task "权限方式" picker
    from what the node actually supports instead of a hardcoded fallback.
    """
    if not isinstance(caps, dict):
        return {}

    flat: dict[str, Any] = {
        str(key): str(value)
        for key, value in (caps.get("labels") or {}).items()
        if value is not None
    }
    for key in ("os", "arch", "docker"):
        value = caps.get(key)
        if value not in (None, ""):
            flat[key] = str(value).lower() if isinstance(value, bool) else str(value)

    providers = caps.get("providers")
    if isinstance(providers, (list, tuple)):
        values = [str(value).strip() for value in providers if str(value).strip()]
        if values:
            flat["providers"] = ",".join(values)
    elif providers is not None and str(providers).strip():
        flat["providers"] = str(providers).strip()

    editors = caps.get("editors")
    if isinstance(editors, list) and editors:
        flat["editors"] = editors
    return flat


def normalize_node_info(info: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Connect NodeInfo JSON into the shape the service speaks."""
    if not isinstance(info, dict):
        return {}
    role_proto = str(info.get("role") or "")
    status_proto = str(info.get("status") or "")
    startup_proto = str(info.get("startupMethod") or "")
    flat_caps = flatten_capabilities(info.get("capabilities") or {})
    return {
        "node_id": info.get("nodeId") or info.get("node_id") or "",
        "node_name": info.get("nodeName") or info.get("node_name") or "",
        "status": PROTO_TO_NODE_STATUS.get(status_proto, status_proto.lower()) if status_proto else "",
        "role": PROTO_TO_NODE_ROLE.get(role_proto, role_proto.lower()) if role_proto else "",
        # A management node with no dial-in client (pure grouping container).
        "is_passive": role_proto == "NODE_ROLE_PASSIVE_MANAGEMENT",
        "startup_method": PROTO_TO_NODE_STARTUP.get(startup_proto, startup_proto.lower()) if startup_proto else "",
        "manager_node_id": info.get("managerNodeId") or info.get("manager_node_id") or "",
        "connected": bool(info.get("connected")),
        # ``connected`` only means a stream is still registered; ``online`` is
        # the server's heartbeat-freshness verdict and is the placement truth.
        "online": bool(info.get("online")),
        "last_heartbeat_at": info.get("lastHeartbeatAt") or "",
        "active_session_ids": list(info.get("activeSessionIds") or []),
        "capabilities": flat_caps,
        # Per-node egress proxy binding: the bound pool entry id (empty = direct)
        # and the last proxy used for a *successful* upgrade (prefilled in the
        # upgrade dialog). Read straight off the wire; the control service
        # projects them from the node record's ACNode columns.
        "proxy_config_id": str(info.get("proxyConfigId") or ""),
        "last_proxy_config_id": str(info.get("lastProxyConfigId") or ""),
    }
