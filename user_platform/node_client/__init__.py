"""Remote node-control client adapters for the data service.

This package is intentionally independent of the control implementation in
``node_server``. It knows only the on-wire HTTP/Connect contracts.
"""
from .client import NodeClient, NodeServerUnavailable, get_local_node_client
from .errors import Code, RPCError
from .session_bindings import get_editor_workspace_session_node_id


def get_node_client() -> NodeClient:
    """Alias of :func:`get_local_node_client` — the name the route modules
    (routes_ios / routes_build / routes_builtin_tools / ios_auto_renew) import."""
    return get_local_node_client()


__all__ = [
    "Code",
    "NodeClient",
    "NodeServerUnavailable",
    "RPCError",
    "get_editor_workspace_session_node_id",
    "get_local_node_client",
    "get_node_client",
]
