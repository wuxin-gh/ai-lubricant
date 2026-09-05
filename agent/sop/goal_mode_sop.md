# Goal Mode SOP

## Purpose

Goal mode (GA chapter6.6) turns an open-ended objective into a bounded autonomous work session. The user supplies one objective, a time budget and optionally a maximum turn count. Guardian persists the state and alternates creation, inspection and improvement until the budget is exhausted.

## Start

Select `goal` mode in the chat composer and provide:

- `objective`: one clear sentence describing the desired outcome.
- `budget_seconds`: the maximum wall-clock budget (the backend enforces it).
- `max_turns`: an optional hard turn limit.

The backend calls `Guardian.start_goal_mode` and stores the state in `agent_goal_states`. It also mirrors the latest state to `temp/goal_state.json` under the Agent workspace.

## Phase cycle

- **creation** (first turn): make or modify real deliverables toward the objective.
- **inspection**: review the current result from tester, reader and maintainer perspectives; find concrete issues.
- **improvement**: act on inspection findings with substantive changes.
- Repeat inspection → improvement until the time or turn budget is exhausted.

Every turn must execute meaningful work. Do not merely report progress. Keep `update_working_checkpoint` current and use `related_files` for important artifacts.

## Budget exhaustion

When wall-clock budget or max turns is reached, Guardian enters `wrapping_up`. The Agent must summarize:

1. What was completed.
2. What remains unfinished.
3. Files/artifacts produced.
4. Risks or decisions needing user input.

Then stop and mark the state `done`/`done_budget` (depending on the persistence layer). Do not start new work after the wrap-up.

## Blocking and safety

If blocked by missing information or an irreversible choice, call `ask_user` rather than guessing. Respect normal approval requirements for `code_run`, node commands and external capabilities. Do not broaden the objective without user confirmation.

## Difference from autonomous mode

Goal mode is one open objective + a bounded budget, driven by the Guardian phase cycle. Autonomous operation is an idle-time TODO queue that picks independent items; it does not replace goal mode.
