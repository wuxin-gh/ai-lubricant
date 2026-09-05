"""GenericAgent subsystem public API."""
from __future__ import annotations

from typing import Any

__all__ = ["router", "GenericAgent", "AgentConfig"]


def __getattr__(name: str) -> Any:
    if name == "router":
        from agent.api import router
        return router
    if name == "GenericAgent":
        from agent.agent_main import GenericAgent
        return GenericAgent
    if name == "AgentConfig":
        from agent.config import AgentConfig
        return AgentConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
