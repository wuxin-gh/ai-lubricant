# Memory Cleanup SOP

## Core principle: encode existence, not content

The model is its own compressor and decoder. L1 only has to make it *aware that a
class of knowledge exists* — once aware, it fetches the depth itself with
`file_read`. So L1's job is: **in the fewest possible words, what memory exists for
what scenario.**

L1 holds two kinds of entries, judged by the same yardstick:

- **existence pointers** — the shortest trigger words that lead to an L2 fact or L3 SOP
- **behaviour rules** — mistakes that get made unless the rule is in front of you

```
ROI = (probability of the mistake without these words × its cost) / per-turn word cost
```

## Quick judgement

**Keep** counter-intuitive triggers — scenario words you would *not* think to look up
a SOP for. `browser_sop (httponly cookie)` earns its parenthesis: without the words
"httponly cookie" you would never guess that reading a cookie is covered by the
browser SOP.

**Cut**:

- **name translation** — `proxy-pool (代理池)`: the name already says it, the
  parenthesis is dead weight. Just `proxy-pool`.
- **content description** — `opencli_sop (66 sites CLI, reuses Chrome session)`:
  implementation detail belongs inside the SOP, not in the routing table.
- **intuitive capability** — if you would reach for it unprompted, the entry buys
  nothing and still costs every turn.
- **redundancy** — a rule L3 already states, or a fragment another L1 line covers.

## Four compression rules

1. **Self-explaining name beats an added description.** If the SOP's name says it,
   L1 adds nothing. Renaming the SOP often has higher ROI than annotating L1.
   - Corollary: a very low-ROI entry with a self-explaining filename need not get
     its own L1 line, as long as rule 2's set covers it — the set trigger plus a
     directory listing is the fallback.
2. **Minimum description for a set.** When several nearby entries share one
   higher-level scenario, name the *set* to signal that this class of ability
   exists instead of flattening the children. `qq ops / feishu ops / wecom ops` →
   `im ops: *_im_sop`. Children with self-explaining names get listed, not
   translated.
3. **An entry is scenario ↔ solution existence.** `video understanding: yt-dlp for
   subtitles`, `fofa (asset mapping)` — the scenario name is the trigger, the
   solution name encodes existence. Parentheses hold **only counter-intuitive
   triggers**; translation, content description and implementation detail are all
   waste.
   - **Trigger test**: imagine the user says this word. Would you think to look up
     the matching SOP? Yes → intuitive, drop it. No → counter-intuitive, keep it.
4. **Layer placement.** Rearrange *within* L1: entries carrying behaviour rules or
   high-frequency high-ROI content go in the upper scenario lines; pure existence
   pointers go in the flat list below. **Placement is not removal — existence must
   never be lost.**

## Cleanup procedure

1. Read L1 line by line, split on `|`, and classify each fragment first: existence
   pointer / rule / translation / content description / implementation detail /
   redundant.
2. Clear the rules first. For each: is this globally high-ROI, or a low-danger
   scenario-specific rule? Global high-ROI → keep. Scenario-specific or low-danger
   → demote into L3, or delete.
3. Then clear the existence pointers: is each one expressing **scenario ↔ solution
   existence**? Add a scenario trigger only when it is counter-intuitive; delete
   translations, content descriptions and implementation details.
4. **Audit ghost entries**: does the L3 file each L1 line points at actually exist?
   If not, either delete the pointer or create the file — decide by ROI.
5. **No values in L1.** Parentheses must not hold parameter values (IPs, ports,
   credentials). Those are L2 facts; L1 encodes existence only.
6. Check whether L3 filenames are self-explaining. Prefer a rename over an L1
   annotation. Finally verify the total line count is ≤ 30.

**Red line:** memory edits are persistent damage and a wrong one compounds every
turn. L1 may only be changed with word-level patches — never overwritten. If an
entry turns out to mislead, fix or rename it promptly.

## L2 slimming (long redundant section → L3, no fact loss)

Applies when an L2 section has grown long (server or tool detail) and needs
compressing without losing facts.

1. **Migrate before compressing.** Move the full facts into a dedicated L3 SOP —
   if a SOP on that topic exists, merge into it rather than creating a second one
   (list `memory/sop/` first). Leave 6-9 lines in L2: how to connect, the service
   endpoint, the high-frequency pitfalls, and a pointer (`see xxx_sop.md`).
2. After migrating, sync L1 with the new SOP name — self-explaining is enough, no
   redundant parenthesis.
3. **Close the loop per section**: migrate → compress L2 → sync L1, then move to
   the next section. This bounds the blast radius of a mistake.
4. Verify: each new SOP file exists and is in L1, L2 has no leftover dirty
   characters, and the total line count went down.

## Pitfall: legacy dirty characters break file_patch matching

- **Symptom**: `file_patch` repeatedly reports that the old block was not found,
  even though `old_content` looks identical to the file.
- **Cause**: old records can carry a stray real `|` at the line start, or
  full-width/arrow byte differences that are invisible when copied.
- **Diagnose**: `for i, l in enumerate(lines): print(i + 1, repr(l[:20]))` — `repr`
  shows the actual bytes.
- **Fix**: switch to a line-index slice replacement in Python:
  ```python
  lines = open(p, encoding='utf-8').read().split('\n')
  assert lines[a].startswith(...)          # anchor before
  assert lines[b - 1].startswith(...)      # anchor after
  newlines = lines[:a] + repl + lines[b:]
  open(p, 'w', encoding='utf-8').write('\n'.join(newlines))
  ```
  The two `startswith` asserts guard against off-by-one drift; re-check with `repr`
  afterwards. This also cleans the dirty characters out.

## Platform notes

- Dynamic MCP SOPs live under `memory/sop/mcp/` and are not deleted by built-in SOP
  sync.
- L4 (conversation and tool history) is archived in ClickHouse by the platform's
  retention scheduler. This implementation creates no zip archives in the workspace.
- Cleanup is maintenance, not a licence to skip confirmation. Before deleting or
  rewriting a user-edited SOP, read it and ask the user when the intent is unclear.
