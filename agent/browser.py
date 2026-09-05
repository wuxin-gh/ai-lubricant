"""Safe browser adapter boundary for GenericAgent.

Browser automation is provided by the vendored CDP Bridge built-in MCP plugin
loaded by ``mcp_runtime``. This adapter remains a small direct-use boundary for
agent code that wants to initialize the bridge without spawning an external
stdio MCP process.
"""
from __future__ import annotations

from typing import Any

from agent.config import AgentConfig


class BrowserUnavailableError(RuntimeError):
    """Raised when browser automation is unavailable or not configured."""


class BrowserAdapter:
    """Browser adapter stub that fails safely with explicit guidance."""

    def __init__(self, config: AgentConfig | None = None) -> None:
        self.config = config or AgentConfig()
        self._client: Any | None = None

    def connect(self) -> None:
        if not self.config.browser_enabled:
            raise BrowserUnavailableError(
                "Browser automation is disabled by config (browser_enabled=False)."
            )
        raise BrowserUnavailableError(
            "Direct CDP bridge startup is removed; use the authenticated MCP runtime cdp-bridge service."
        )

    def navigate(self, url: str) -> dict[str, Any]:
        _ = url
        raise self._operation_error("navigate")

    def scan(self, text_only: bool = False) -> dict[str, Any]:
        _ = text_only
        raise self._operation_error("scan")

    def execute_js(self, script: str) -> dict[str, Any]:
        _ = script
        raise self._operation_error("execute_js")

    def screenshot(self) -> str:
        raise self._operation_error("screenshot")

    def close(self) -> None:
        raise self._operation_error("close")

    def _operation_error(self, operation: str) -> BrowserUnavailableError:
        prefix = "Browser automation is disabled by config" if not self.config.browser_enabled else "Browser automation is unavailable"
        return BrowserUnavailableError(
            f"{prefix}; cannot {operation} through the direct adapter. Use the MCP runtime cdp-bridge service."
        )


CDPBridgeBrowser = BrowserAdapter
