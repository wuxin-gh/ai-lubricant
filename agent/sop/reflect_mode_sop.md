# Reflect Mode SOP

## Purpose

Reflect mode runs a check on a fixed schedule and, when the check signals work, dispatches a task. The check script can be changed without restarting the Agent (hot reload).

## Three elements

A reflect script lives under `workspace/reflect/<name>.py` and defines:

1. `INTERVAL` — seconds between checks.
2. `ONCE` — if True, the script runs at most once per fire; if False, the loop repeats.
3. `check() -> str | None` — returns a task prompt string when work is needed, or None to skip.

## Example

```python
INTERVAL = 600  # every 10 minutes
ONCE = False

def check() -> str | None:
    import os
    path = "workspace/inbox"
    files = os.listdir(path) if os.path.isdir(path) else []
    new = [f for f in files if f.endswith(".json")]
    if not new:
        return None
    return f"process the {len(new)} new files under {path}"
```

## Hot reload

The reflect worker detects script file mtime changes and reloads on the next cycle. Editing a reflect script does not require restarting the Agent core.

## Watchdog as a reflect application

A watchdog is just a reflect script whose `check()` inspects a directory for new files or an error log for new lines, then returns a task to triage them. Write watchdoges the same way as any reflect script.

## Limitations

- Reflect scripts share the Agent's tool boundary; they must not reach outside allowed roots.
- `check()` is synchronous and runs in the worker; keep it cheap. Heavy work happens in the dispatched `run_task`.
