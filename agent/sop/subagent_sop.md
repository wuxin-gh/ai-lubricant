# Subagent SOP

## When to delegate

Use a subagent for an independent investigation, a focused verification pass, or a parallel map step over separate files. Do not delegate a tiny edit whose context setup costs more than the work.

**Delegation is omission control, not throughput.** Assume one capable executor can handle
long files and coherent multi-file edits. Size is not a reason to fan out. Fan out only when
something would otherwise be *missed*:

- **Unknown list** — the set of items isn't known yet, so split by genuinely different
  evidence sources (read the code / run it / check logs / search the web) to surface what a
  single pass would skip.
- **Known list** — the items are known, independent and each is agent-sized, so assign
  ownership and nothing gets forgotten.

Do **not** parallelise coherent execution. If the work is clear, bounded and interdependent,
one executor keeps it consistent; splitting it produces seams someone has to sew back up.

**Fake parallelism to avoid:** splitting one method into sub-checklists. "Check the API",
"check parity", "check side effects" all read the same files the same way and return
overlapping findings — that's one job in three costumes. Open a lane only when its *evidence
source* differs.

**Depth vs width.** Width finds independent items. Depth is for when the next search depends
on the last one's result: find → dedupe/rank → verify or refute → hunt residuals, until dry.
A batch of results that needs cross-comparison before the next step is a barrier; if each
item can proceed alone, don't impose one.

**Adversarial verification.** For anything important, tell the verifier to *refute* the
result, not to confirm it. A verifier asked to agree will agree.

**Real execution, not smoke tests.** "It imports" is not "it works". Find the existing
tests, demo scripts or CLI entry points and run the relevant ones; if none cover the change,
build a minimal test that actually exercises it. Record the exact command and its output.
Never report pass for behaviour that was not run — mark it blocked, say why, and say what
would unblock it.

**Make your bounds visible.** If you sampled, took the top N, time-boxed, skipped retries or
excluded a subsystem, say so in the result. A silent bound reads as full coverage.

**Don't repeat a failed prompt unchanged.** Read the failure, then retry with narrower
scope, a longer timeout, a different tool, or a different decomposition.

## Isolation and communication

`spawn_subagent_async` creates a GenericAgent with an isolated conversation/context and the parent's effective LLM binding. The parent receives `subagent_*` SSE events. A subagent does not silently mutate the parent's working anchor; return conclusions explicitly.

For parent↔child coordination, use the `agent_subagent_messages` mailbox through the platform's subagent messaging path. Include the objective, allowed files, expected output and verification evidence in the task.

## Map-Reduce

1. **Map** — delegate independent slices (one file, provider, or test dimension per child).
2. **Reduce** — collect child summaries and evidence in the parent; reconcile conflicts before editing.
3. **Verify** — delegate a fresh reviewer that did not perform the changes to return PASS / FAIL / PARTIAL.

Parallel children must not edit the same file. The parent owns integration and final writes.

## Safety

Children inherit the same approval, workspace and MCP boundaries. Do not use delegation to bypass user confirmation or execute unapproved code. If a child is blocked, report the blocker and escalate with `ask_user`.
