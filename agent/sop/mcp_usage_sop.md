# MCP Usage SOP

## Purpose

MCP (Model Context Protocol) services are external capabilities (mail, browser automation, marketplace status, etc.). They are **not** first-class tools — they are dynamic capabilities reached through `capability_call`. This SOP explains how to discover and invoke them.

## Discover capabilities

The system prompt's `[Available Capabilities]` section lists bound MCP services with their available method names and a pointer to each service's SOP:

```text
[Available Capabilities]
- mail: search_messages, send_message (SOP: memory/sop/mcp/mail_sop.md)
- scheduler: create|list|cancel|run_now (SOP: memory/sop/scheduled_task_sop.md)
```

Read a service's SOP body on demand with `file_read("memory/sop/mcp/<service>_sop.md")` to learn its method semantics before first use.

When the conversation has a scene (a browser panel, a marketplace admin chat), the
service that scene exists to drive is hoisted to the **top** of the system prompt
instead, under `[Scene Tools]` plus a `[Scene Capability: <service>]` block holding
its full method and parameter detail. That service is deliberately not repeated in
`[Available Capabilities]` below, so check the scene block first: its detail is
already inline and needs no `file_read`.

## Invoke a capability

```text
capability_call(name="<service>.<method>", args={...})
```

Examples:

```text
capability_call(name="mail.search_messages", args={"query": "invoice", "limit": 10})
capability_call(name="scheduler.create", args={"name": "daily report", "cron_expression": "0 9 * * *", "task_prompt": "..."})
```

Unknown `name` returns `capability_not_found`; a method missing on a service returns an MCP error from that service.

## First use auto-generates a SOP skeleton

The first `capability_call` against an MCP service creates `memory/sop/mcp/<service>_sop.md` with the service name, a list of available methods, and an empty "Experience" section. A short L1 pointer `mcp.<service>` is added so the index stays consistent.

## Distill experience after success

After a verified successful use of an MCP method, call `start_long_term_update` with `summary`, `verified_by`, `reuse_reason` and `evidence.steps`. The backend writes the reusable procedure into the service's SOP (L3) and updates L1, so later turns benefit from prior success.

## Limitations

- MCP services must be bound to the Agent (via its MCP principal) to appear in the index. Unbound services are invisible.
- Authenticated services require a valid token; a 401 at call time is reported as a tool error and does not block the Agent.
- Browser-class MCP services are detected dynamically (capability labels or browser primitives) — the `web` tool routes browser operations to them automatically; do not hardcode a service name.
