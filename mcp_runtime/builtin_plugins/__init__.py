"""Built-in plugin adapters loaded directly by the MCP runtime.

Unlike custom plugins (loaded via ``exec`` of user source), built-in plugins
live in-tree under ``mcp_builtin`` and are imported normally. Each adapter
exposes a ``register(reg)`` function compatible with
``mcp_runtime.plugin_loader.PluginRegistrar``, so the runtime can activate
them through the same registry as custom plugins.
"""
