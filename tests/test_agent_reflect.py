from pathlib import Path

import pytest

from agent import reflect_worker as rw


@pytest.mark.asyncio
async def test_reflect_worker_dispatches_prompt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    workspace = tmp_path / "agents" / "1" / "workspace"
    monkeypatch.setattr(rw, "agent_workspace_root", lambda aid: workspace)
    root = workspace / "reflect"
    root.mkdir(parents=True)
    (root / "watch.py").write_text(
        "INTERVAL = 1\nONCE = True\ndef check():\n    return 'inspect inbox'\n",
        encoding="utf-8",
    )
    seen: list[str] = []

    async def dispatch(prompt: str) -> None:
        seen.append(prompt)

    worker = rw.ReflectWorker(1, dispatch)
    assert await worker.tick() == ["inspect inbox"]
    assert seen == ["inspect inbox"]
    # ONCE prevents a second fire without reload.
    assert await worker.tick() == []


@pytest.mark.asyncio
async def test_reflect_worker_hot_reloads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    workspace = tmp_path / "agents" / "1" / "workspace"
    monkeypatch.setattr(rw, "agent_workspace_root", lambda aid: workspace)
    root = workspace / "reflect"
    root.mkdir(parents=True)
    script = root / "watch.py"
    script.write_text("INTERVAL = 1\nONCE = True\ndef check():\n    return 'v1'\n", encoding="utf-8")
    seen: list[str] = []

    async def dispatch(prompt: str) -> None:
        seen.append(prompt)

    worker = rw.ReflectWorker(1, dispatch)
    await worker.tick()
    # Force an mtime change; hot reload resets ONCE and loads the new result.
    script.write_text("INTERVAL = 1\nONCE = True\ndef check():\n    return 'v2'\n", encoding="utf-8")
    stat = script.stat()
    import os
    os.utime(script, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    await worker.tick()
    assert seen == ["v1", "v2"]
