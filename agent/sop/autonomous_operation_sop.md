# Autonomous Operation SOP

## Purpose

Autonomous operation runs useful background work only when the Agent is enabled and idle. It is TODO-driven, unlike goal mode's single open objective.

## TODO queue

Create `temp/TODO.txt` under the Agent workspace. One item per line; optional priority prefix:

```text
[P1] review the latest error log
[P2] update the dependency inventory
```

The worker considers the Agent idle after the configured idle interval (default 30 minutes), claims one item atomically, runs it through the normal Agent loop, and records the result. Remove or mark a line complete only after successful execution.

## Rules

- Autonomous mode must be explicitly enabled in Agent settings; default is off.
- Never run autonomous work while the user has an active conversation/task for that Agent.
- Use normal tool approval and capability boundaries; do not bypass `code_run` approval.
- Keep checkpoints and verified reusable results using `update_working_checkpoint` and `start_long_term_update`.
- If an item is blocked, leave it in the queue with a short failure note and wait for user input rather than retrying indefinitely.

## Difference from goal mode

Goal mode: one objective + wall-clock/turn budget, Guardian phase cycle. Autonomous mode: idle-triggered TODO queue; each item is independent.
