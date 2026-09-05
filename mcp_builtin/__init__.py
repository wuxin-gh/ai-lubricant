"""Built-in MCP plugins vendored into this project.

Each subpackage is a self-contained MCP capability maintained in-tree. The
MCP runtime loads them via ``register(reg)`` adapters (see
``mcp_runtime/builtin_plugins``) instead of spawning external processes.
"""
