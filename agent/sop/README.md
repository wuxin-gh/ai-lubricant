# Agent SOP Index

GenericAgent's L3 operating procedures. SOP bodies are real files under the Agent's `memory/sop/` directory and are read on demand with `file_read`.

## Core files

- `memory_management_sop.md` — L1/L2/L3/L4 memory layers and `start_long_term_update`.
- `browser_sop.md` — unified `web` tool (scan/execute/tabs/screenshot).
- `scheduled_task_sop.md` — scheduler via `capability_call(name="scheduler.*")`.
- `mcp_usage_sop.md` — dynamic MCP services through `capability_call`.

## Mode SOPs (GA chapter6)

- `plan_sop.md` — five-phase planning for non-trivial tasks.
- `goal_mode_sop.md` — open objective + bounded budget, Guardian-driven.
- `autonomous_operation_sop.md` — idle TODO queue.
- `reflect_mode_sop.md` — scheduled check + hot reload + watchdog pattern.
- `subagent_sop.md` — delegation, isolation and Map-Reduce.
- `failure_escalation_sop.md` — local correction → strategy switch → human.

## Capability SOPs

- `web_setup_sop.md` — bind and verify a browser-class MCP.
- `skill_search_sop.md` — find and adopt platform Skills.
- `ocr_sop.md` — image text extraction via `code_run`.
- `keychain_sop.md` — credential safety policy.
- `memory_cleanup_sop.md` — memory ROI maintenance.

## Quality SOPs

- `deliverable_audit_sop.md` — adversarial verification before claiming done.
- `code_review_principles.md` — what good code is; the yardstick for review.
- `review_sop.md` — read-only adversarial code review: P0-P3 findings, anti-false-positive rules, verdict.
- `supervisor_sop.md` — supervising a subagent without doing the work yourself.

Dynamic MCP SOPs are created at `memory/sop/mcp/<service>_sop.md` on first use.

## Tool routing reminder

The fixed first-class tool set is `file_read`, `file_write`, `file_patch`, `code_run`, `web`, `update_working_checkpoint`, `start_long_term_update`, `ask_user`, and `capability_call`. Read this index with `file_read("memory/sop/README.md")` when navigating the SOP set.

