# Plan Mode SOP

## When to enter plan mode

Trigger plan mode when the task is non-trivial:

- More than 3 steps with dependencies between them.
- Multiple file modifications or cross-component changes.
- Branching on conditions (if-A then-B else-C).
- Anything where guessing wrong is expensive.

For one-shot answers or a single tool call, use normal `interact` mode.

## Five phases

1. **Explore** — read relevant files and memory to understand the current state. Do not propose changes yet.
2. **Plan** — write `workspace/plan_<task>/plan.md` with the step list. Each step is one line starting with a status marker (below). Group independent steps for parallel work; mark delegation and verification clearly.
3. **User confirm** — use `ask_user` to present the plan and wait for approval before executing. Do not proceed past this phase without explicit confirmation.
4. **Execute** — run the steps in order, updating markers as each completes or fails. Keep `update_working_checkpoint` current so the anchor survives context compression.
5. **Verify** — spawn a subagent (via the parent's spawn_subagent flow) to review the result against the objective. The subagent returns PASS / FAIL / PARTIAL.

## plan.md step markers

- `[ ]` — not started
- `[D]` — delegate to a subagent
- `[P]` — parallelisable with sibling `[P]` steps
- `[✓]` — completed
- `[✗]` — failed
- `[FIX]` — failed and needs a corrective step added

## Rules

- The plan file lives under `workspace/plan_<task>/plan.md`. It does not pollute `memory/` unless distilled.
- Re-read the plan with `file_read("workspace/plan_<task>/plan.md")` after context compression.
- If a step fails twice, escalate: add a `[FIX]` step or `ask_user` (see `memory/sop/failure_escalation_sop.md`).
- On completion, if the workflow is reusable and verified, call `start_long_term_update` to distill it into an L3 SOP.

## Limitations

- Plan mode is a coordination pattern, not a separate tool set — it uses the same 10 first-class tools.
- `ask_user` blocks execution; the user must respond before phase 4 begins.
