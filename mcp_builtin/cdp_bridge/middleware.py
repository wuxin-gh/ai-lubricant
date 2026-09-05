"""Legacy standalone HTTP middleware intentionally disabled.

CDP extension authentication is handled exclusively by
``mcp_runtime.sse_gateway.ext_session`` using protocol 2 client tokens.
"""


def reject_legacy_http_middleware(*_args, **_kwargs):
    raise RuntimeError("legacy CDP HTTP middleware is removed; use MCP runtime auth protocol 2")
