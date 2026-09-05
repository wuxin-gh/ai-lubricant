from __future__ import annotations

import asyncio

import pytest

from monkeycode_compat.node_client.approvals import ApprovalRegistry, hash_command
from monkeycode_compat.node_client.tools import command_requires_confirmation, register_node_shell_exec


class _Tools:
    def __init__(self) -> None:
        self.handlers = {}

    def register(self, name, handler, schema) -> None:
        self.handlers[name] = handler


@pytest.mark.asyncio
async def test_registry_resolve_wakes_waiting_action() -> None:
    registry = ApprovalRegistry()
    action = registry.create(
        conversation_id="conv-1",
        node_id="node-1",
        tool_name="node_shell_exec",
        command="rm example.txt",
        requester="user-1",
    )

    assert action.command_hash == hash_command("rm example.txt")
    assert registry.list_pending("conv-1") == [action]
    assert registry.resolve(action.confirmation_id, "allow") is action
    assert await action.future == "allow"
    assert registry.resolve(action.confirmation_id, "deny") is None


@pytest.mark.asyncio
async def test_registry_reap_expired_auto_denies() -> None:
    registry = ApprovalRegistry()
    action = registry.create(
        conversation_id="conv-1",
        node_id="node-1",
        tool_name="node_shell_exec",
        command="mkdir example",
        requester=None,
        timeout_seconds=-1,
    )

    assert registry.reap_expired() == [action]
    assert action.resolved == "timeout"
    assert await action.future == "deny"


@pytest.mark.asyncio
async def test_dangerous_tool_emits_then_pauses_until_denied(monkeypatch) -> None:
    import monkeycode_compat.node_client.tools as module

    registry = ApprovalRegistry()
    monkeypatch.setattr(module, "approval_registry", registry)
    events = []

    async def emit(event: dict) -> None:
        events.append(event)

    tools = _Tools()
    register_node_shell_exec(tools, "node-1", "user-1", "conv-1", emit=emit)
    task = asyncio.create_task(tools.handlers["node_shell_exec"]({"command": "rm example.txt"}))

    for _ in range(20):
        if events:
            break
        await asyncio.sleep(0)

    assert events[0]["type"] == "confirmation_required"
    assert not task.done()
    action = registry.get(events[0]["confirmation_id"])
    assert action is not None
    registry.resolve(action.confirmation_id, "deny")
    result = await task
    assert result["status"] == "denied"


def test_command_confirmation_classifier() -> None:
    cases = [
        ("posix", "pwd", False),
        ("posix", "ls -la && git status", False),
        ("posix", "ls && rm example.txt", True),
        ("posix", "cat a > b", True),
        ("posix", "echo $(rm example.txt)", True),
        ("posix", "find . -name '*.py'", False),
        ("posix", "find . -delete", True),
        ("posix", "sed -i 's/a/b/' file.txt", True),
        ("posix", "date --set tomorrow", True),
        ("posix", "hostname new-name", True),
        ("posix", "git status", False),
        ("posix", "git diff", False),
        ("posix", "git reset --hard", True),
        ("posix", "git clean -fd", True),
        ("posix", "git config --list", False),
        ("posix", "git config --unset user.name", True),
        ("powershell", "Get-ChildItem", False),
        ("powershell", "Get-Content README.md | Select-String TODO", False),
        ("powershell", "Get-ChildItem; Remove-Item test.txt", True),
        ("powershell", "Set-Content test.txt value", True),
        ("powershell", "Invoke-WebRequest https://example.com", True),
        ("powershell", "& 'script.ps1'", True),
        ("cmd", "dir && whoami", False),
        ("cmd", "set", False),
        ("cmd", "set NAME=value", True),
        ("cmd", "dir & del example.txt", True),
        ("cmd", "reg add HKCU\\Software\\Example", True),
        ("cmd", "cmd /c dir", True),
        ("unknown", "pwd", False),
        ("unknown", "pwd /tmp", True),
        ("unknown", "dir", True),
    ]
    for shell_flavor, command, expected in cases:
        assert command_requires_confirmation(command, shell_flavor) is expected, (shell_flavor, command)
