"""Vendored CDP Bridge.

The original package is a FastMCP stdio/streamable-http server. In this project
it is loaded in-process by ``mcp_runtime`` (see
``mcp_runtime/builtin_plugins/cdp_bridge_plugin.py``); all standalone HTTP and
WebSocket entry points are disabled.
"""
from __future__ import annotations

from pathlib import Path

from .server import current_token


def main() -> None:
    """Disabled: cdp-bridge is loaded by mcp_runtime, not run as a standalone server."""
    raise SystemExit(
        "cdp-bridge is vendored and loaded by mcp_runtime. "
        "Use the MCP management UI to start/stop it; the standalone server entry point is disabled."
    )


def extension_path() -> Path:
    """Return the packaged Chrome extension directory (for users to load it)."""
    return Path(__file__).resolve().parent / "tmwd_cdp_bridge"


__all__ = ["current_token", "extension_path", "main"]
