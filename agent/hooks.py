"""
Agent Hook/Plugin System

7 hook points matching GenericAgent's design:
  agent_before, agent_after,
  turn_before, turn_after,
  llm_before, llm_after,
  tool_before, tool_after

Usage:
  from agent.hooks import hooks

  @hooks.register('turn_after')
  async def log_turn(ctx):
      print(f"Turn {ctx['turn']} done, {len(ctx.get('tool_results', []))} tools used")

  # In agent_loop.py:
  await hooks.trigger('turn_before', {'turn': turn, 'messages': messages, ...})
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

# All valid hook event names
VALID_EVENTS = frozenset({
    'agent_before',
    'agent_after',
    'turn_before',
    'turn_after',
    'llm_before',
    'llm_after',
    'tool_before',
    'tool_after',
})

HookFn = Callable[[dict[str, Any]], Awaitable[None | dict[str, Any]]]


class HookRegistry:
    """Registry for agent lifecycle hooks."""

    def __init__(self):
        self._hooks: dict[str, list[HookFn]] = {e: [] for e in VALID_EVENTS}

    def register(self, event: str):
        """Decorator to register a hook function for an event.

        Usage:
            @hooks.register('turn_after')
            async def my_hook(ctx):
                ...
        """
        if event not in VALID_EVENTS:
            raise ValueError(f"Unknown hook event '{event}'. Valid: {sorted(VALID_EVENTS)}")

        def decorator(fn: HookFn) -> HookFn:
            self._hooks[event].append(fn)
            return fn

        return decorator

    def unregister(self, event: str, fn: HookFn) -> bool:
        """Remove a registered hook. Returns True if found and removed."""
        if event in self._hooks and fn in self._hooks[event]:
            self._hooks[event].remove(fn)
            return True
        return False

    async def trigger(self, event: str, ctx: dict[str, Any]) -> dict[str, Any]:
        """Trigger all hooks for an event. Returns the (possibly modified) context.

        Each hook receives a shallow copy of ctx. If a hook returns a dict,
        it is merged back into the original ctx (hook can modify context).
        Hooks that raise are logged and skipped.
        """
        if event not in self._hooks:
            return ctx

        for hook_fn in self._hooks[event]:
            try:
                result = await hook_fn(ctx)
                # If hook returns a dict, merge it back (allows modification)
                if isinstance(result, dict):
                    ctx.update(result)
            except Exception:
                logger.exception(f"Hook {hook_fn.__name__!r} for event {event!r} raised")

        return ctx

    def has_hooks(self, event: str) -> bool:
        """Check if any hooks are registered for an event."""
        return bool(self._hooks.get(event))

    def clear(self, event: str | None = None):
        """Clear hooks. If event is None, clear all."""
        if event:
            self._hooks[event] = []
        else:
            for e in self._hooks:
                self._hooks[e] = []


# Global singleton
hooks = HookRegistry()
