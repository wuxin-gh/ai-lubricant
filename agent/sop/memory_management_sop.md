# Memory Management SOP (L0 / META-SOP)

## 0. Core Axioms (highest priority)

1. **Action-Verified Only**
   - Anything written to L1/L2/L3 must come from a **successful tool result**
     (`code_run` exited 0, `file_read` confirmed the content, a capability call returned ok).
   - Forbidden: the model's own prior knowledge, guesses, unexecuted plans, unverified hypotheses.
   - Slogan: **No Execution, No Memory.**
2. **Sanctity of Verified Data**
   - Verified configuration, pitfall notes and key paths must **never be dropped** during cleanup.
   - You may compress wording or move an item between layers (L2 → L3), but never lose accuracy or traceability.
   - Be extremely careful when editing memory: prefer small patches. If a safe patch is not
     possible, leave it unchanged.
3. **No Volatile State**
   - Never store data that changes per session: timestamps, session ids, running PIDs,
     one-off absolute paths, currently attached device/tab ids.
4. **Minimum Sufficient Pointer**
   - An upper layer keeps only the shortest identifier that locates the lower layer.
     One extra word is redundancy.

---

## Layer architecture

```
L1: memory/global_mem_insight.txt   (index layer — hard limit <= 30 lines)
    | navigates to (pointer)
L2: memory/global_mem.txt           (fact layer — short now, grows over time)
    | detailed reference
L3: memory/sop/*.md                 (record layer — SOP Markdown and stable scripts)
L4: ClickHouse conversations/messages (archived sessions — for tracing and audit only)
```

---

## Per-layer duties

### L1 — global index (`memory/global_mem_insight.txt`)

**Duty**: give L2 and L3 a minimal navigation index so capabilities are discoverable.

- **Size**: <= 30 lines (hard), < 1k tokens (target). No detail unless extremely high-frequency.
- **Content**: two tiers of `scenario keyword -> memory location` mapping, plus `[RULES]`.
  - Tier 1 — high-frequency scenarios: key -> value directly naming the SOP / script / L2 section.
    Self-explanatory names get one word, no restatement.
  - Tier 2 — low-frequency scenarios: keyword only; read L2 or list `memory/sop/` to locate.
  - **Scenario trigger words matter most** — an unindexed capability is an unknown capability.
    But never write how-to detail.
  - `[RULES]` — compressed pitfall rules:
    - Red line (fatal): violating it kills the process or corrupts state.
    - Red line (silent): no error raised but the result is wrong.
    - High-frequency mistakes: constraints that are easy to forget.
- **Update**: when L2/L3 gains or loses an entry, place it in the right tier by frequency.
  Patch only — never overwrite the whole file, never rewrite it with `code_run`.

**Forbidden in L1**: secrets and API keys; "how to" text; explanations; task-specific technical
detail (that belongs in L3); log records.

### L2 — global facts (`memory/global_mem.txt`)

**Duty**: environment facts — paths, credentials references, configuration, constants.

- Organised as `## [SECTION]` blocks.
- When a fact changes, update L1's navigation line only if the scenario location changed.

**Forbidden**: volatile state, guesses, general knowledge an LLM can already derive.

### L3 — task-level records (`memory/sop/`)

**Duty**: the small amount of detail that L1/L2 cannot hold but that matters for reuse.
Keep it as short as reuse allows.

- Record only what stays relevant **across sessions** and is hard to rebuild with a few
  `file_read` / `web` calls.
- Prefer: hidden preconditions and typical pitfalls whose loss causes expensive retries.
- Do not record: ordinary steps, or paths/state recoverable in a couple of probes.
- Forms: `*_sop.md` — a minimal "key preconditions + typical pitfalls" list, not a tutorial.

---

## L1 <-> L2/L3 sync rules

| Change | L1 sync |
| --- | --- |
| New L2/L3 scenario | Default to low-frequency: add the name to the L3 list (no description if self-explanatory; add a parenthesised trigger word only when the scenario is counter-intuitive) |
| L2/L3 scenario removed | Delete the matching keyword / mapping line |
| L2/L3 value changed | Leave L1 alone unless scenario location changed |
| General pitfall discovered | Compress to one line and add it to `[RULES]` |

> **Sync red line**: L1 holds keywords and names only — never move detail up. Parentheses carry
> only a counter-intuitive trigger word (2-4 words); never a mechanism, method or step.
> Bad: `sop_name(scenario A: method1 + method2 + method3)` → Good: `sop_name(scenario A)`
> Bad when the name already explains itself: `browser_sop(browser operations)` → Good: `browser_sop`

---

## Classification decision tree

```
"Which layer does this belong to?"

Is it an environment-specific fact? (IP, non-standard path, credential reference, id —
something an LLM cannot produce zero-shot)
  |- YES -> L2 (memory/global_mem.txt), then index it in L1 tier 1 or tier 2 by frequency
  |
  \- NO
       |
       Is it a general operating rule? (global pitfall guidance, diagnosis method,
       not tied to one task)
       |- YES -> L1 [RULES] (one compressed line only)
       |
       \- NO
            |
            Is it task-specific technique? (only succeeded after hard trial, reusable later)
            |- YES -> L3 (memory/sop/<name>.md)
            |
            \- NO -> general knowledge or redundancy: do not store, discard
```

---

## Write path in this runtime

`file_write` / `file_patch` must not touch `memory/`. The only path that writes long-term
memory is `start_long_term_update`, which returns this SOP plus the current index and asks
you to perform the minimal update yourself. Read this SOP before writing any memory.

`update_working_checkpoint` is per-task working memory (goal / completed / current_state /
next_steps / key_info / related_files). It is never auto-promoted to long-term memory.
When one SOP governs the task, set `related_files` to that SOP's `file_read` path so the
anchor reminds you to re-read it after context compression.
