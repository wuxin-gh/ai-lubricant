# Web Setup SOP

## Purpose

Prepare the browser automation environment before first use of the `web` tool's browser operations.

## Check availability

Browser operations (`web` with `operation=execute`, `tabs`, `screenshot`, or `scan` with a `tab`) require a browser-class MCP service bound to the Agent. The system detects such services dynamically — never assume a service name. If none is bound, the tool returns `browser_mcp_not_configured`.

## Bind a browser MCP

1. In the Agent's MCP configuration, bind a CDP-capable MCP service to the Agent principal.
2. Reopen the conversation; the `[Available Capabilities]` index should list the service with a `browser` capability label.
3. Verify by calling `web(operation="tabs")`; a valid response means the binding works.

## First scan/execute

- Start with `web(operation="scan", url="...")` for static public content.
- Use `web(operation="execute", script="document.title")` to confirm the live session responds.
- Record the working browser procedure with `start_long_term_update` after it is verified.

## Safety

- Do not store cookies or session tokens in memory. Use the authorised browser session only for the requested task.
- Respect login and anti-bot boundaries; never bypass authentication controls.
