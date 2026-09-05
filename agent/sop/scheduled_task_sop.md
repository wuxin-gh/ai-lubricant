# Scheduled Task SOP

## Purpose

GenericAgent can persist recurring tasks to PostgreSQL; a background scheduler polls due tasks and runs them through the Agent. The scheduler is not a first-class tool — it is reachable through `capability_call`.

## Architecture

```
┌─────────────────┐     ┌──────────────────────────────┐     ┌─────────────────┐
│  capability_call │────▶│       AgentScheduler          │◀────│  AgentTaskRunner │
│ (AI 管理任务)    │     │ (APScheduler + PostgreSQL)    │     │  (生命周期管理)   │
└─────────────────┘     └──────────────────────────────┘     └─────────────────┘
```

- **AgentScheduler** (`agent/scheduler.py`) — wraps APScheduler: cron parsing, due detection, scheduling. PostgreSQL table `agent_scheduled_tasks` stores task metadata and last result.
- **AgentTaskRunner** (`agent/runner.py`) — background asyncio loop polling due tasks and executing them via GenericAgent.

## capability_call scheduler actions

All scheduler calls go through `capability_call(name="scheduler.<action>", args={...})`.

### scheduler.create

Create a recurring task. Validates the schedule expression and computes `next_run_at`.

Two task kinds via `task_kind`:

- `prompt` (default) — `task_prompt` is fed to the Agent every run. Requires
  `name`, `cron_expression`, `task_prompt`.
- `script` — `script_code` is executed directly (Python/PowerShell subprocess,
  same executor as the `code_run` tool). Requires `name`, `cron_expression`,
  `script_code`. Optional: `script_type` (`python`|`powershell`, default python),
  `script_timeout` (seconds), `background`, `on_error`.

**A script task you create is NOT authorized to run.** The scheduler enforces a
hash lock: a script only executes when a human has approved its exact contents
(via the REST `approve-script` endpoint), which records `approved_hash`. You
cannot authorize your own script — creating one leaves it blocked until a person
approves it. This is deliberate.

Common `background` uses: state why the task exists, what "success" means, and
any constraints, so that if the script later fails the diagnosing Agent has the
context to judge the failure rather than guessing.

`on_error` (script tasks only):

- `none` — record the failure, do nothing else.
- `diagnose` (default) — on a non-zero exit code, an Agent is invoked to diagnose
  the root cause; the diagnosis is stored. No script change.
- `diagnose_fix_retry` — diagnose, and if confident, propose a fixed script via
  `scheduler.propose_script_fix`; if the task's owner pre-authorized autonomous
  fixes, the new script takes effect and is re-run once.

### scheduler.list

List tasks. `args`: optional `enabled` boolean filter. `script_code` is omitted
from the list (use the per-task detail path); each entry carries `task_kind`,
`has_pending_fix`, `last_exit_code`, `consecutive_failures`.

### scheduler.cancel

Disable a task (soft delete). `args`: `job_id` (required).

### scheduler.run_now

Trigger immediate execution of a task. `args`: `job_id` (required).

### scheduler.propose_script_fix

Called from within an `on_error` diagnosis to submit a corrected script for a
failing script task. `args`: `job_id`, `script_code` (the complete fixed script),
`reason`.

- If the task was created with autonomous fixes enabled, the new script is
  applied and re-authorized immediately (and the failing run is retried once).
- Otherwise the fix is parked as `pending_script_code`, the task is disabled, and
  a human must approve it via `approve-script`.

You cannot bypass this: there is no scheduler action that writes `approved_hash`.
Authorization is always a human decision, made either at task creation or via
`approve-script`.

## Create a task

1. Give the task a clear `name`.
2. `task_prompt` must be self-contained: include inputs, expected output and constraints.
3. `cron_expression` uses the standard 5-field format (min hour day month weekday).
4. `next_run_at` is computed automatically — do not set it manually.
5. Set `enabled=false` to create a draft task.

## Which model a run uses

A `prompt` task resolves its model in three layers, each field falling back
independently — so supplying only a model reuses the layer above's key:

1. the task's own `api_key_id` / `model`,
2. the Agent's scheduled-task binding (`scheduled_api_key_id` / `scheduled_model`),
3. the Agent's main binding.

Unattended runs usually want a cheaper or steadier model than interactive chat,
which is why this is separate from the subagent binding — that one means "a
parallel child task the main Agent spawned", a different thing entirely.

## No human is present

Every scheduled run carries a scene block saying so. It changes what "done"
means:

- `ask_user` reaches nobody. Do not stop and wait for a decision — record the
  evidence and your recommendation in the result so whoever reads the report can
  act on it.
- Tools that need confirmation (`code_run` and friends) come back denied. Report
  the blocker; never rewrite a command to slip past the gate.
- Nothing keeps your context after the run. Durable output goes to a file or
  wherever the task specifies, not just into the reply text.

## Run due work

The scheduler handles this automatically (APScheduler-driven):

1. On each due fire, `_execute_job(job_id)` reads the task row.
2. `prompt` tasks run `task_prompt` through a GenericAgent; `script` tasks run
   `script_code` in a subprocess (after the hash-lock check).
3. On a script's non-zero exit, `on_error` decides whether an Agent is invoked to
   diagnose / fix / retry.
4. The result, exit code, failure counter and next run time are written back.
5. A task that fails repeatedly can be disabled via
   `capability_call(name="scheduler.cancel", args={"job_id": ...})`.

## Result format

```text
status: completed | failed | skipped
summary: one short paragraph
next_action: what should happen before the next run
```

## Configuration

In the main config's `agent` field:

```json
{
  "agent": {
    "scheduler_enabled": true
  }
}
```

`scheduler_enabled` defaults to `false` and must be enabled manually.

## Limitations

- Script auto-fix (`diagnose_fix_retry`) retries **once** per run, and only when
  the task owner pre-authorized autonomous fixes; otherwise the fix waits for a
  human. Prompt tasks are never retried.
- No jitter: cron times are second-precise with no random offset.
- Scheduled tasks default to `max_turns=40` (heal diagnosis uses `max_turns=20`).
- Results are truncated to 2000 characters before being stored in `last_result`;
  `last_stderr` keeps up to 4000 characters of the last failing run.
- Script tasks require human authorization (`approve-script`) before they run —
  the Agent cannot authorize its own scripts.
- When the DB pool is unavailable, writes return a safe sentinel and reads return an empty list.
