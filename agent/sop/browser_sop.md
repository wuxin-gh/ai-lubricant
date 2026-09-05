# Browser SOP

## Purpose

Use the least powerful web path that can finish the task. The single `web` tool aggregates public HTTP reading and live-browser operations.

## Tool

`web` takes `operation`:

- `scan`: read a URL and return compressed semantic text. By default it uses plain HTTP and does not open a browser. If `tab` is supplied, it uses the browser-class MCP for rendered DOM/text.
- `execute`: execute JavaScript in the active authorised browser tab through the browser-class MCP.
- `tabs`: list available browser tabs through the browser-class MCP.
- `screenshot`: capture a screenshot through the browser-class MCP.

1. For public/static content, start with `web(operation="scan", url="...")`. Do not launch a browser merely to read text.
2. Use `web(operation="execute", script="...")` only when the task needs a live session, JavaScript-rendered state, navigation, login state or page interaction.
3. Use `web(operation="tabs")` to inspect tabs before selecting one when the browser session is ambiguous.
4. Before changing a live page, observe its current URL/state.
5. **New task → new tab.** A fresh, independent task (a different site, an unrelated goal, anything that should not mutate the page the user is currently on) starts with `browser_open_tab(url)` — do not drive it in the current tab and pollute the user's open page session. Within that new task's own site, subsequent navigation still follows rule 6 (in-page clicks).
6. To go to another page within the same task/site, prefer clicking an in-page link / element (via `web(operation="execute")`) over directly opening a URL (`browser_navigate`). Directly opening a new URL tears down the chat panel's page context; clicking keeps the page session and referrer intact. Use direct navigation only when no in-page link exists.
7. Keep JavaScript small and reversible. After an action, inspect the returned result/page change before issuing the next action.
8. Browser services are detected dynamically by their browser capability labels/primitives; do not assume a service name.
9. `scan` with no `tab` remains usable without a browser MCP. `execute`, `tabs`, `screenshot`, and `scan` with a `tab` require a configured browser-class MCP. If unavailable, the tool returns `browser_mcp_not_configured`; ask the user to bind a CDP-capable MCP.

## New tab vs. in-page navigation

`browser_open_tab(url, new_window=False, active=True)` opens a fresh tab (or window)
through the extension's native `chrome.tabs.create`. Use it as the **entry point for
any new task** — a different site, an unrelated objective, or work that must not
touch the page the user is looking at. Driving a new task in the current tab mutates
the user's open session (history, scroll, form state) and risks losing their context.

- A newly opened tab needs a moment to load and register as a session; its id may not
  appear in `browser_get_tabs` immediately — wait briefly, then re-list.
- Once the new tab is the working surface, navigate *within its site* by clicking
  in-page elements (rule 6), not by opening further tabs.
- `browser_navigate(url)` changes the URL of the current tab only; it is not a way
  to start a new task. Reserve it for same-task URL changes where no in-page link
  exists.

## Verified pitfalls (CDP bridge)

These are measured behaviours of the Chrome-extension CDP bridge, not guesses. Skipping
them is the usual cause of "the script ran but nothing happened".

### JavaScript execution

- **`await` needs an explicit `return`.** The script is wrapped in an async function; an
  awaited expression without `return` yields `null`, not the value.
- `scan` pierces same-origin iframes automatically. Cross-origin iframes need the CDP
  path (below) — `contentDocument` is unreachable.
- **Synthetic events carry `isTrusted=false`.** File inputs and some buttons reject them.
  When a plain JS click does nothing, escalate to CDP input events rather than retrying
  the same click.
- A JS click that should open a new tab but doesn't is usually popup blocking — CDP
  clicking gets through.
- **File upload:** prefer the pure-JS DataTransfer route —
  `new File([content], name, {type})` → `new DataTransfer().items.add(file)` →
  `input.files = dt.files` → dispatch `input` and `change`. Check `input.accept` first,
  and disambiguate multiple inputs by `accept` or container semantics.

### Navigation

- **Never navigate and then act in the same script.** After `location.href = ...` the
  execution context is destroyed and the rest of the script fails with
  `Inspected target navigated or closed`. Navigate, wait for load, then act in a second
  call.
- Prefer clicking an in-page link over assigning `location.href` (see Tool rule 5).

### CDP click sequence

- A reliable click is **three events**: `mouseMoved` → `mousePressed` → `mouseReleased`,
  50-100ms apart. Omitting `mouseMoved` breaks any hover-dependent component (tooltips,
  dropdown menus) — the click lands but the menu never opened.
- **Coordinates equal `getBoundingClientRect()` once the session is stable — but not on
  first attach.** Attaching the debugger makes Chrome show the "being controlled by
  automated software" infobar (~20px), which pushes page content down. Coordinates
  measured before attach are wrong after it. Send one harmless `mouseMoved(0,0)` to warm
  up, then measure.
- Custom dropdowns need two rounds: click the trigger, *then* measure the option element
  (it doesn't exist in the DOM until the menu opens).
- If the page uses `transform: scale` or `zoom`, multiply by
  `getComputedStyle(document.documentElement).zoom` and `visualViewport.scale`.
- For an element inside an iframe, compose coordinates: `iframeRect.x + elRect.x`.
- `DOM.getBoxModel` returns eight values; take the centre as the **average of all four
  points**, not the diagonal midpoint — a rotated or skewed element is not a rectangle.
- `nodeId` goes stale after any DOM change. Prefer `backendNodeId`, or re-fetch the
  document.

### Tabs and throttling

- Background tabs are throttled: `setTimeout` fires at most about once a minute. Do not
  build polling loops on it.
- Some SPAs only load data in the foreground — `Page.bringToFront` first.
- Screenshots via `Page.captureScreenshot` work on background tabs and need no
  foregrounding; for a captcha canvas, `canvas.toDataURL()` is cleaner still.
- Cross-tab operation needs no foregrounding, only an explicit `tabId`.

### Batched CDP commands

- Batching keeps one attached session and lets later commands reference earlier results.
- **A failed earlier command makes later references silently `undefined`.** Check every
  entry's ok status instead of trusting the last result.
- Keep one lookup path per chain — don't mix selector-based and search-based node lookup
  for the same node.

## Connection troubleshooting

When browser operations fail, check in this order and only involve the user last:

1. Is the browser even running, and on a real page? Extensions do not load on
   `about:blank` and other internal pages.
2. Is the bridge listener up on the host?
3. Is the extension actually installed and enabled?
4. Only if all three are fine, ask the user for help.

## Safety

- Never enter secrets, passwords, payment data or tokens without explicit user direction and an authorised context.
- Do not bypass authentication, anti-bot protections or access controls.
- Do not browse unrelated pages.
- Do not steal or expose cookies or session tokens; use the authorised browser session only for the requested task.

## Long-term learning

After a successful, reusable browser workflow has been verified, call `start_long_term_update` with `summary`, `verified_by`, `reuse_reason` and ordered `evidence.steps`. The backend stores the result as an L3 SOP and updates L1.

## Limitations

- Browser operations (`execute`, `tabs`, `screenshot`, and `scan` with a `tab`) are unavailable when no browser-class MCP is bound; the tool returns `browser_mcp_not_configured`.
- Plain HTTP `scan` does not execute JavaScript or access authenticated browser state.