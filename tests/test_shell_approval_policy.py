from __future__ import annotations

import pytest

from monkeycode_compat.node_client.tools import (
    PRESET_APPROVAL_CATALOG,
    command_key_for,
    command_requires_confirmation,
)


@pytest.mark.parametrize(
    "command, shell, expected",
    [
        # Plain command → lowercased first token.
        ("ls -la", "posix", "ls"),
        ("pwd", "posix", "pwd"),
        ("Get-ChildItem", "powershell", "get-childitem"),
        ("git.exe status", "windows", "git:status"),  # unknown shell still normalizes
        # git subcommand → git:<sub>.
        ("git status", "posix", "git:status"),
        ("git reset --hard HEAD~1", "posix", "git:reset"),
        ("git --no-pager log -n 5", "posix", "git:log"),
        # CMD lowercases.
        ("DIR", "cmd", "dir"),
    ],
)
def test_command_key_normalization(command: str, shell: str, expected: str) -> None:
    assert command_key_for(command, shell) == expected


def test_command_key_rejects_dangerous_structure() -> None:
    # command_key_for returns the FIRST segment's key for a chain — it does not
    # reject compound commands (the classifier checks every segment itself).
    # What it DOES reject (returns None) is a structure _split_shell_command
    # cannot tokenize at all: redirect, command substitution, unclosed quote.
    assert command_key_for("echo $(rm x)", "posix") is None
    assert command_key_for("cat a > b", "posix") is None
    # A connector chain tokenizes into segments → first segment's key.
    assert command_key_for("ls; rm x", "posix") == "ls"


def test_auto_allow_keys_skip_approval_for_matching_command() -> None:
    # rm would normally require confirmation; with its key allow-listed it does not.
    assert command_requires_confirmation("rm example.txt", "posix") is True
    assert command_requires_confirmation("rm example.txt", "posix", {"rm"}) is False
    # git:reset likewise.
    assert command_requires_confirmation("git reset --hard", "posix") is True
    assert command_requires_confirmation("git reset --hard", "posix", {"git:reset"}) is False


def test_auto_allow_keys_do_not_bypass_dangerous_structure_gate() -> None:
    # Even if "ls" is allow-listed, a compound/redirect command still requires approval.
    assert command_requires_confirmation("ls; rm x", "posix", {"ls", "rm"}) is True
    assert command_requires_confirmation("cat a > b", "posix", {"cat"}) is True
    assert command_requires_confirmation("echo $(rm x)", "posix", {"echo"}) is True


def test_auto_allow_keys_are_shell_scoped() -> None:
    # "dir" auto-allowed on cmd does not bleed into posix (where "dir" is unknown-safe only).
    assert command_requires_confirmation("dir", "cmd", {"dir"}) is False
    # posix "dir" is not in the posix read-only set; without an auto-allow it requires
    # confirmation, and a posix-scoped {"dir"} allow-set relaxes it.
    assert command_requires_confirmation("dir", "posix") is True
    assert command_requires_confirmation("dir", "posix", {"dir"}) is False


def test_user_isolation_via_keys_set_difference() -> None:
    # The store layer returns per-user sets; here we verify the classifier semantics
    # that user A's key does not auto-allow a command for user B: the same command run
    # under user B's (empty) set still requires confirmation.
    user_a_keys = {"rm"}
    user_b_keys: set[str] = set()
    assert command_requires_confirmation("rm x", "posix", user_a_keys) is False
    assert command_requires_confirmation("rm x", "posix", user_b_keys) is True


def test_catalog_keys_match_normalizer_output() -> None:
    # Every catalog key must round-trip through command_key_for for its shell so the
    # UI toggle and the classifier agree on what gets auto-allowed.
    for item in PRESET_APPROVAL_CATALOG:
        key = command_key_for(item["label"], item["shell"])
        assert key == item["key"], (item["shell"], item["label"], item["key"], key)


def test_catalog_grouped_by_supported_shells() -> None:
    shells = {item["shell"] for item in PRESET_APPROVAL_CATALOG}
    assert shells.issubset({"posix", "powershell", "cmd", "unknown"})


def test_custom_command_validator_rejects_compound_and_dangerous() -> None:
    # The route's custom-command validator must reject anything that is not a
    # single safe tokenizable segment: compounds, redirects, substitution, ps
    # script blocks, empty, oversized.
    from fastapi import HTTPException

    from monkeycode_compat.routes_shell_approval import _validate_command_key

    # Solo safe commands normalize through.
    assert _validate_command_key("Get-ChildItem", "powershell") == "get-childitem"
    assert _validate_command_key("git reset", "posix") == "git:reset"

    bad = [
        ("ls; rm x", "posix"),       # compound chain (2 segments)
        ("cat a > b", "posix"),       # redirect → unparseable
        ("echo $(rm x)", "posix"),    # command substitution → unparseable
        ("dir & del a", "cmd"),        # '&' is both separator and invocation → rejected
        ("Get-ChildItem | Remove-Item", "powershell"),  # pipe chain (2 segments)
        ("", "posix"),
    ]
    for command, shell in bad:
        with pytest.raises(HTTPException) as excinfo:
            _validate_command_key(command, shell)
        assert excinfo.value.status_code in (400, 422), (command, shell)

    # Oversized key rejected.
    with pytest.raises(HTTPException) as excinfo:
        _validate_command_key("x" * 100, "posix")
    assert excinfo.value.status_code == 422
