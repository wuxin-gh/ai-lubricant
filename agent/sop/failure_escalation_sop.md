# Failure Escalation SOP

When the same task failure repeats, escalate instead of blindly retrying.

## Three steps

1. **Local correction** — inspect the exact error/result, fix the smallest local issue, and rerun the verification.
2. **Strategy switch** — after a repeated failure, change the approach (different tool, narrower scope, or delegated investigation). Record the failed path in the working checkpoint; do not distill it as a successful SOP.
3. **Human intervention** — if the switched strategy fails again or requires an irreversible choice, call `ask_user` with the concrete error, attempted approaches and the decision needed.

## Rules

- Keep `goal`, `current_state`, `next_steps` and evidence paths in `update_working_checkpoint`.
- Never hide an error behind a fabricated success result.
- A failure can become long-term learning only as a verified warning/procedure after the cause and safe workaround are confirmed; use `start_long_term_update` with evidence.
- Respect approval boundaries at every escalation step.
