"""Node host shell tool for the data-side agent, bound to the live PTY.

The agent runs commands inside the terminal the operator is currently watching:
an approved command is written into that PTY, echoed and streamed like the
operator typed it, and the bounded result comes back for the model. Execution
is therefore refused (never silently rerouted) when no browser terminal is
attached — running unobserved would defeat the whole point.

Approval batching: an LLM turn can emit several ``node_shell_exec`` calls at
once. Rather than a prompt per command, :class:`TerminalCommandBatch` pre-scans
the turn's calls and, if any need confirmation, raises ONE all-or-nothing
approval covering the set. The tool then consumes that shared decision instead
of prompting again, and executes the commands one at a time so the terminal
shows a coherent sequence and the agent waits for each.
"""
from __future__ import annotations

import asyncio
import shlex
import time
from typing import Any, Awaitable, Callable, Optional

from .approvals import (
    CONFIRMATION_TIMEOUT_SECONDS,
    ApprovalDenied,
    ApprovalTimeout,
    approval_registry,
    emit_confirmation_required,
)

# The gate proves that a command is read-only. Anything it cannot prove is sent
# for approval; this is intentionally narrower than a shell grammar.
_POSIX_READ_ONLY = {
    "pwd", "ls", "cat", "head", "tail", "grep", "rg", "which", "whoami",
    "hostname", "uname", "date", "printenv", "id", "ps", "df", "du", "file", "stat",
}
_POWERSHELL_READ_ONLY = {
    "get-childitem", "get-content", "get-item", "get-location", "get-process",
    "get-service", "get-command", "get-date", "get-host", "get-computerinfo",
    "get-ciminstance", "get-variable", "get-alias", "get-history", "get-member",
    "select-string", "select-object", "sort-object", "group-object", "measure-object",
    "format-table", "format-list", "format-wide", "where-object", "write-output",
}
_CMD_READ_ONLY = {
    "dir", "type", "where", "whoami", "hostname", "ver", "tasklist", "systeminfo",
    "driverquery", "ipconfig", "path", "cd", "chdir", "echo",
}
_UNKNOWN_READ_ONLY = {"pwd", "whoami", "hostname", "date"}
_GIT_READ_ONLY = {
    "status", "log", "show", "grep", "blame", "rev-parse", "rev-list", "ls-files",
    "ls-tree", "cat-file", "describe", "shortlog", "diff",
}
_GIT_OUTPUT_OPTIONS = {"--output", "--output-indicator-new", "--output-indicator-old", "--output-indicator-context"}


def _split_shell_command(text: str, shell_flavor: str) -> list[str] | None:
    """Split simple command chains, rejecting shell constructs we cannot prove safe."""
    segments: list[str] = []
    current: list[str] = []
    quote = ""
    index = 0
    while index < len(text):
        char = text[index]
        if quote:
            current.append(char)
            if char == quote:
                quote = ""
            elif char == "\\" and shell_flavor == "posix" and quote == '"' and index + 1 < len(text):
                index += 1
                current.append(text[index])
            index += 1
            continue
        if char in "'\"":
            quote = char
            current.append(char)
            index += 1
            continue
        if char in "<>`" or (char == "$" and index + 1 < len(text) and text[index + 1] == "("):
            return None
        if shell_flavor == "powershell" and char in "{}@":
            return None
        if char in "\r\n;|&":
            # PowerShell's '&' is also the invocation operator; do not try to
            # distinguish it from a chain separator.
            if shell_flavor == "powershell" and char == "&":
                return None
            segment = "".join(current).strip()
            if not segment:
                return None
            segments.append(segment)
            current = []
            if index + 1 < len(text) and text[index + 1] == char and char in "|&":
                index += 1
        else:
            current.append(char)
        index += 1
    if quote:
        return None
    segment = "".join(current).strip()
    if not segment:
        return None
    segments.append(segment)
    return segments


def _tokens(segment: str, shell_flavor: str) -> list[str] | None:
    try:
        values = shlex.split(segment, posix=shell_flavor == "posix")
    except ValueError:
        return None
    return [value.strip('"\'') for value in values if value.strip('"\'')]


def _git_is_read_only(args: list[str]) -> bool:
    if not args:
        return False
    index = 0
    while index < len(args) and args[index].startswith("-"):
        if args[index] not in {"--no-pager", "--paginate", "--version", "--help"}:
            return False
        index += 1
    if index >= len(args):
        return all(arg in {"--version", "--help"} for arg in args)
    subcommand = args[index].lower()
    rest = [arg.lower() for arg in args[index + 1:]]
    if subcommand in _GIT_READ_ONLY:
        return not any(arg == option or arg.startswith(option + "=") for arg in rest for option in _GIT_OUTPUT_OPTIONS)
    if subcommand == "branch":
        return not any(not arg.startswith("-") or arg in {"-d", "-D", "-m", "-M", "-c", "-C", "--delete", "--move", "--copy", "--edit-description", "--set-upstream-to", "--unset-upstream"} for arg in rest)
    if subcommand == "remote":
        return not rest or rest[0] in {"-v", "show", "get-url"}
    if subcommand == "config":
        return bool(rest) and rest[0] in {"--get", "--get-all", "--get-regexp", "--get-urlmatch", "--list", "-l", "--show-origin", "--show-scope"}
    return False


def _segment_is_read_only(segment: str, shell_flavor: str) -> bool:
    values = _tokens(segment, shell_flavor)
    if not values:
        return False
    command = values[0].lower()
    if command.endswith(".exe"):
        command = command[:-4]
    args = values[1:]
    if command == "git":
        return _git_is_read_only(args)
    if shell_flavor == "posix":
        lowered = [arg.lower() for arg in args]
        if command == "find":
            unsafe = {"-delete", "-exec", "-execdir", "-ok", "-okdir", "-fprintf", "-fprint", "-fls"}
            return not any(arg in unsafe for arg in lowered)
        if command == "sed":
            return not any(arg == "-i" or arg.startswith("--in-place") or (arg.startswith("-i") and len(arg) > 2) for arg in lowered)
        if command == "date":
            return not any(arg in {"-s", "--set"} or arg.startswith("--set=") for arg in lowered)
        if command == "hostname":
            return not args
        return command in _POSIX_READ_ONLY
    if shell_flavor == "powershell":
        return command in _POWERSHELL_READ_ONLY
    if shell_flavor == "cmd":
        if command == "set":
            return not any("=" in arg for arg in args)
        return command in _CMD_READ_ONLY
    return command in _UNKNOWN_READ_ONLY and not args


def command_requires_confirmation(
    command: str,
    shell_flavor: str = "unknown",
    auto_allow_keys: set[str] | None = None,
) -> bool:
    """Return ``True`` unless every command segment is provably read-only.

    ``auto_allow_keys`` carries a user's opt-in auto-allow set (normalized
    ``command_key`` values for this shell). A SOLO segment whose key is in the
    set skips the "cannot prove read-only" fallback. Auto-allow NEVER applies
    to a multi-segment chain: a user pre-approving individual commands cannot
    safely pre-approve an unseen *combination*, so a chain that is not entirely
    provably read-only always requires approval (``ls; rm x`` needs a card even
    if both ``ls`` and ``rm`` are individually allow-listed). This also NEVER
    bypasses the dangerous-structure gate — ``_split_shell_command`` returns
    ``None`` for redirects, command substitution, and script blocks, so those
    always require approval. Fail-closed everywhere: unknown shell, parse
    failure, and empty input still require confirmation.
    """
    text = (command or "").strip()
    flavor = (shell_flavor or "unknown").strip().lower()
    if flavor not in {"posix", "powershell", "cmd"}:
        flavor = "unknown"
    if not text:
        return True
    segments = _split_shell_command(text, flavor)
    if not segments:
        return True
    # A multi-segment chain is auto-allowed ONLY if every segment is provably
    # read-only. Any non-read-only segment → approval, and auto_allow_keys do
    # NOT rescue it (the combination is unseen). This keeps ``ls && git status``
    # approval-free while ``ls; rm x`` always needs a card.
    if len(segments) > 1:
        return any(not _segment_is_read_only(seg, flavor) for seg in segments)
    # Single segment: read-only → ok; otherwise the user's allow-key rescues it.
    keys = auto_allow_keys or ()
    segment = segments[0]
    if _segment_is_read_only(segment, flavor):
        return False
    if keys and _segment_command_key(segment, flavor) in keys:
        return False
    return True


def _segment_command_key(segment: str, shell_flavor: str) -> str | None:
    """Normalized identifier for one command segment, or ``None`` if unparseable.

    ``ls`` / ``pwd`` → the lowercased first token; ``git status`` → ``git:status``;
    PowerShell ``Get-ChildItem`` → ``get-childitem``; a ``.exe`` suffix is
    stripped. Mirrors the tokens extracted by ``_segment_is_read_only`` so a
    key derived here matches the one the classifier checks against.
    """
    values = _tokens(segment, shell_flavor)
    if not values:
        return None
    command = values[0].lower()
    if command.endswith(".exe"):
        command = command[:-4]
    if command == "git":
        rest = values[1:]
        index = 0
        while index < len(rest) and rest[index].startswith("-"):
            index += 1
        if index < len(rest):
            return f"git:{rest[index].lower()}"
        return "git"
    return command


def command_key_for(command: str, shell_flavor: str = "unknown") -> str | None:
    """Public normalizer used by routes + the classifier for a single command.

    For a multi-segment chain this returns the key of the *first* segment only;
    callers that need per-segment keys (the classifier) use
    ``_segment_command_key`` directly. Returns ``None`` when the command cannot
    be safely tokenized (dangerous structure), which routes treat as reject.
    """
    flavor = (shell_flavor or "unknown").strip().lower()
    if flavor not in {"posix", "powershell", "cmd"}:
        flavor = "unknown"
    segments = _split_shell_command((command or "").strip(), flavor)
    if not segments:
        return None
    return _segment_command_key(segments[0], flavor)


# Static catalog of common commands the UI offers as one-click auto-allow
# toggles, grouped by shell flavor. ``default`` is the suggested initial state
# (most read-only queries default off — the classifier already auto-allows them,
# so there is nothing to gain by pre-checking — while a few mutating commands a
# user trusts are shown but default off). The catalog is presentational only;
# it never enters the database. ``key`` matches ``command_key_for`` output.
PRESET_APPROVAL_CATALOG: list[dict] = [
    # ── POSIX ──────────────────────────────────────────────────────────────
    {"shell": "posix", "key": "ls", "label": "ls", "description": "列出目录内容（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "pwd", "label": "pwd", "description": "打印当前目录（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "cat", "label": "cat", "description": "查看文件内容（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "grep", "label": "grep / rg", "description": "文本搜索（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "find", "label": "find", "description": "查找文件（只读形态默认已免审）", "default": False},
    {"shell": "posix", "key": "git:status", "label": "git status", "description": "查看工作区状态（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "git:diff", "label": "git diff", "description": "查看差异（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "git:log", "label": "git log", "description": "查看提交历史（只读，默认已免审）", "default": False},
    {"shell": "posix", "key": "git:reset", "label": "git reset", "description": "重置暂存区/HEAD（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "git:checkout", "label": "git checkout", "description": "切换分支/检出文件（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "git:clean", "label": "git clean", "description": "清理未跟踪文件（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "mkdir", "label": "mkdir", "description": "创建目录（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "touch", "label": "touch", "description": "创建空文件/更新时间戳（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "cp", "label": "cp", "description": "复制文件（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "mv", "label": "mv", "description": "移动/重命名（会修改，需勾选才免审）", "default": False},
    {"shell": "posix", "key": "rm", "label": "rm", "description": "删除文件/目录（高危，需勾选才免审）", "default": False},
    # ── PowerShell ─────────────────────────────────────────────────────────
    {"shell": "powershell", "key": "get-childitem", "label": "Get-ChildItem", "description": "列出目录内容（只读，默认已免审）", "default": False},
    {"shell": "powershell", "key": "get-content", "label": "Get-Content", "description": "查看文件内容（只读，默认已免审）", "default": False},
    {"shell": "powershell", "key": "get-location", "label": "Get-Location", "description": "当前目录（只读，默认已免审）", "default": False},
    {"shell": "powershell", "key": "select-string", "label": "Select-String", "description": "文本搜索（只读，默认已免审）", "default": False},
    {"shell": "powershell", "key": "set-content", "label": "Set-Content", "description": "写入文件（会修改，需勾选才免审）", "default": False},
    {"shell": "powershell", "key": "remove-item", "label": "Remove-Item", "description": "删除文件/目录（高危，需勾选才免审）", "default": False},
    {"shell": "powershell", "key": "new-item", "label": "New-Item", "description": "创建文件/目录（会修改，需勾选才免审）", "default": False},
    {"shell": "powershell", "key": "copy-item", "label": "Copy-Item", "description": "复制（会修改，需勾选才免审）", "default": False},
    {"shell": "powershell", "key": "move-item", "label": "Move-Item", "description": "移动/重命名（会修改，需勾选才免审）", "default": False},
    # ── CMD ─────────────────────────────────────────────────────────────────
    {"shell": "cmd", "key": "dir", "label": "dir", "description": "列出目录内容（只读，默认已免审）", "default": False},
    {"shell": "cmd", "key": "type", "label": "type", "description": "查看文件内容（只读，默认已免审）", "default": False},
    {"shell": "cmd", "key": "echo", "label": "echo", "description": "输出文本（只读形态默认已免审）", "default": False},
    {"shell": "cmd", "key": "del", "label": "del", "description": "删除文件（高危，需勾选才免审）", "default": False},
    {"shell": "cmd", "key": "mkdir", "label": "mkdir", "description": "创建目录（会修改，需勾选才免审）", "default": False},
    {"shell": "cmd", "key": "copy", "label": "copy", "description": "复制文件（会修改，需勾选才免审）", "default": False},
    {"shell": "cmd", "key": "move", "label": "move", "description": "移动/重命名（会修改，需勾选才免审）", "default": False},
    {"shell": "cmd", "key": "rmdir", "label": "rmdir", "description": "删除目录（高危，需勾选才免审）", "default": False},
]


# Async event sink (SSE) and injected terminal executor.
EventSink = Callable[[dict], Awaitable[None]]
TerminalExecutor = Callable[..., Awaitable[dict[str, Any]]]


async def _audit(
    node_id: str,
    caller: str | None,
    conversation_id: str,
    command: str,
    result: dict[str, Any],
    shell_flavor: str,
    *,
    auto_allowed: bool = False,
) -> None:
    try:
        from ..team_users_service import team_users_service
        from ..deps import resolve_team_id

        await team_users_service.record_audit(
            await resolve_team_id(caller),
            str(caller or ""),
            "node.terminal_exec",
            request={
                "node_id": node_id,
                "conversation_id": conversation_id,
                "command": command[:500],
                "shell_flavor": shell_flavor,
                "auto_allowed": auto_allowed,
            },
            response={
                "status": result.get("status"),
                "exit_code": result.get("exit_code"),
                "output_bytes": len(result.get("output", "") or ""),
                "execution_surface": "terminal",
            },
        )
    except Exception:
        # Audit must not turn a successful node operation into a failure.
        return


class TerminalCommandBatch:
    """Coordinates one send_message turn's ``node_shell_exec`` approvals.

    The agent loop hands the whole set of tool calls to :meth:`prepare` before
    dispatching them. If any command needs confirmation, one approval covering
    all of them is raised here; :meth:`decision_for` then hands each tool call
    the shared allow/deny outcome, in call order, so the model is never prompted
    per command and the set is truly all-or-nothing.
    """

    def __init__(
        self,
        *,
        conversation_id: str,
        node_id: str,
        caller: str | None,
        emit: Optional[EventSink],
        shell_flavor: str,
        auto_allow_keys: Optional[set[str]] = None,
        timeout_seconds: int = CONFIRMATION_TIMEOUT_SECONDS,
    ) -> None:
        self._conversation_id = conversation_id
        self._node_id = node_id
        self._caller = caller
        self._emit = emit
        self._shell_flavor = shell_flavor
        self._auto_allow_keys = set(auto_allow_keys) if auto_allow_keys else set()
        # 注册表到期时刻与本地 wait_for 必须同值，否则两边各按各的到期。
        # 0 = 不过期，wait_for 侧转成 None（永久等待）。
        self._timeout_seconds = int(timeout_seconds)
        # Ordered decisions for the current turn, consumed as the tool runs.
        # (command, "allow"|"deny", auto_allowed: bool)
        self._decisions: list[tuple[str, str, bool]] = []
        self._confirmation_id: str | None = None

    def _wait_timeout(self) -> float | None:
        """wait_for 的 timeout：恰好 0 表示不过期（None＝永久等待）。

        与 ApprovalRegistry.create 同口径——负数不是哨兵，仍是「立即到期」。
        """
        return None if self._timeout_seconds == 0 else self._timeout_seconds

    async def prepare(self, tool_calls: list[dict]) -> None:
        """Scan a turn's tool calls and raise one approval if any command needs it."""
        self._decisions = []
        self._confirmation_id = None
        commands: list[str] = []
        for call in tool_calls or []:
            function = call.get("function", {}) if isinstance(call, dict) else {}
            if function.get("name") != "node_shell_exec":
                continue
            raw = function.get("arguments") or "{}"
            try:
                import json as _json
                args = raw if isinstance(raw, dict) else _json.loads(raw)
            except (TypeError, ValueError):
                args = {}
            cmd = str(args.get("command") or "").strip()
            if cmd:
                commands.append(cmd)

        if not commands:
            return
        needs = [
            c for c in commands
            if command_requires_confirmation(c, self._shell_flavor, self._auto_allow_keys)
        ]
        if not needs:
            # Everything is read-only or auto-allowed by policy; run directly,
            # no approval, keep order. Track which ones were policy-allowed so
            # the executor path can mark the audit.
            for c in commands:
                auto = command_requires_confirmation(c, self._shell_flavor) and not command_requires_confirmation(
                    c, self._shell_flavor, self._auto_allow_keys
                )
                self._decisions.append((c, "allow", auto))
            return

        action = approval_registry.create(
            conversation_id=self._conversation_id,
            node_id=self._node_id,
            tool_name="node_shell_exec",
            command=commands[0],
            requester=self._caller,
            shell_flavor=self._shell_flavor,
            commands=commands,
            timeout_seconds=self._timeout_seconds,
        )
        self._confirmation_id = action.confirmation_id
        message = (
            "以下命令可能修改系统、文件或网络，需要确认后才能在终端执行；"
            "确认为一次性授权，允许则全部执行，拒绝则全部不执行。"
        )
        await emit_confirmation_required(self._emit, action, message)

        try:
            decision = await asyncio.wait_for(action.future, timeout=self._wait_timeout())
        except asyncio.TimeoutError:
            # 超时中止本轮，而不是降级成 deny 让 agent_loop 继续喂给模型：
            # 后者的表现就是「审批过期了却还在调模型」。
            action.resolved = "timeout"
            raise ApprovalTimeout(action.confirmation_id, "node_shell_exec") from None
        finally:
            if action.resolved is None:
                action.resolved = "interrupted"
        if decision == "allow":
            self._decisions = [(c, "allow", False) for c in commands]
            return
        # 用户显式拒绝：与超时对称地中止本轮，而不是把 deny 记进 _decisions 让
        # 工具返回 {"status":"denied"} 被 agent_loop 喂回模型续跑——那样用户看到
        # 的就是「我已经拒绝了它还在跑」。raise 冒泡到 api.py 的 except ApprovalDenied。
        raise ApprovalDenied(action.confirmation_id, "node_shell_exec") from None

    def decision_for(self, command: str) -> tuple[str, str | None, bool]:
        """Pop this command's pre-decided outcome; returns (outcome, confirmation_id, auto_allowed).

        outcome is "allow" / "deny" / "" (no batch decision — the tool falls
        back to its own single-command approval)."""
        for index, (cmd, outcome, auto) in enumerate(self._decisions):
            if cmd == command.strip():
                self._decisions.pop(index)
                return outcome, self._confirmation_id, auto
        return "", self._confirmation_id, False


def register_node_shell_exec(
    tools,
    node_id: str,
    caller: str | None,
    conversation_id: str,
    emit: Optional[EventSink] = None,
    *,
    terminal_id: str = "",
    cwd: str = "",
    shell_flavor: str = "unknown",
    executor: Optional[TerminalExecutor] = None,
    auto_allow_keys: Optional[set[str]] = None,
    timeout_seconds: int = CONFIRMATION_TIMEOUT_SECONDS,
) -> "TerminalCommandBatch":
    """Register the terminal-bound node shell tool.

    ``terminal_id``/``cwd`` are bound server-side from the current terminal
    page — the model cannot choose them. ``executor`` runs a command inside that
    PTY (defaults to the control-service HTTP bridge). ``auto_allow_keys`` is the
    current execution node's administrator-managed allow-list for
    ``shell_flavor``; a hit skips the approval card but the audit still records
    ``auto_allowed=True`` and dangerous structures still require approval.
    ``timeout_seconds`` is the owning Agent's ``approval_timeout_seconds``
    (0 = never expire); it drives both the registry expiry and the local wait.
    Returns the turn's :class:`TerminalCommandBatch`; pass it to
    ``on_tool_batch`` so a multi-command turn is approved all-or-nothing.
    """
    if executor is None:
        executor = _default_executor

    batch = TerminalCommandBatch(
        conversation_id=conversation_id,
        node_id=node_id,
        caller=caller,
        emit=emit,
        shell_flavor=shell_flavor,
        auto_allow_keys=auto_allow_keys,
        timeout_seconds=timeout_seconds,
    )

    async def _run(command: str, *, auto_allowed: bool = False) -> dict[str, Any]:
        if not terminal_id:
            return {
                "status": "error",
                "code": "terminal_unavailable",
                "message": "当前对话未绑定活动终端，无法在终端中执行命令",
                "command": command,
            }
        try:
            result = await executor(
                node_id=node_id,
                terminal_id=terminal_id,
                command=command,
                cwd=cwd,
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "error", "message": f"{type(exc).__name__}: {exc}", "command": command}
        await _audit(node_id, caller, conversation_id, command, result, shell_flavor, auto_allowed=auto_allowed)
        return result

    async def node_shell_exec(args: dict) -> dict[str, Any]:
        command = str(args.get("command") or "").strip()
        if not command:
            return {"status": "error", "message": "command is required"}

        # A batch decision from the turn's pre-scan wins: it already applied the
        # all-or-nothing approval, so do not prompt again.
        outcome, confirmation_id, auto = batch.decision_for(command)
        if outcome == "allow":
            result = await _run(command, auto_allowed=auto)
            if confirmation_id:
                result["confirmation_id"] = confirmation_id
            return result
        if outcome == "deny":
            result = {
                "status": "denied",
                "node_id": node_id,
                "command": command,
                "message": "命令未获批准，已跳过执行",
            }
            if confirmation_id:
                result["confirmation_id"] = confirmation_id
            await _audit(node_id, caller, conversation_id, command, result, shell_flavor, auto_allowed=False)
            return result

        # No batch decision (e.g. a lone tool call outside the pre-scan): safe
        # commands run directly, otherwise raise a single-command approval.
        # A policy auto-allow hit skips the card but is still audited as such.
        if not command_requires_confirmation(command, shell_flavor, batch._auto_allow_keys):
            auto = (
                batch._auto_allow_keys
                and command_requires_confirmation(command, shell_flavor)
                and not command_requires_confirmation(command, shell_flavor, batch._auto_allow_keys)
            )
            return await _run(command, auto_allowed=auto)

        action = approval_registry.create(
            conversation_id=conversation_id,
            node_id=node_id,
            tool_name="node_shell_exec",
            command=command,
            requester=caller,
            shell_flavor=shell_flavor,
            timeout_seconds=batch._timeout_seconds,
        )
        message = "该命令可能修改系统、文件或网络，需要确认后才能在终端执行。"
        await emit_confirmation_required(emit, action, message)

        try:
            decision = await asyncio.wait_for(action.future, timeout=batch._wait_timeout())
        except asyncio.TimeoutError:
            # 与批量路径同口径：超时中止本轮，不降级成 deny 让循环继续。
            action.resolved = "timeout"
            raise ApprovalTimeout(action.confirmation_id, "node_shell_exec") from None
        finally:
            if action.resolved is None:
                action.resolved = "interrupted"

        if decision != "allow":
            result = {
                "status": "denied",
                "node_id": node_id,
                "command": command,
                "message": "命令已被拒绝",
            }
            await _audit(node_id, caller, conversation_id, command, result, shell_flavor, auto_allowed=False)
            return result

        result = await _run(command, auto_allowed=False)
        result["confirmation_id"] = action.confirmation_id
        return result

    tools.register(
        "node_shell_exec",
        node_shell_exec,
        {
            "type": "function",
            "function": {
                "name": "node_shell_exec",
                "description": (
                    "在当前节点终端里执行一条单行 shell 命令。命令会像用户输入一样显示在终端并实时输出，"
                    "工作目录由服务端按当前终端页面绑定。只读命令可直接执行，修改文件、系统或网络的命令必须先获得确认。"
                    "不支持 vim/top/ssh 等交互式程序。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "要执行的单行 shell 命令"},
                    },
                    "required": ["command"],
                },
            },
        },
    )
    return batch


async def _default_executor(*, node_id: str, terminal_id: str, command: str, cwd: str) -> dict[str, Any]:
    """Run the command via the control-service terminal command endpoint."""
    from .. import config
    from .terminal import run_terminal_command

    base = (config.settings.agent_compose_base_url or "").rstrip("/")
    token = (config.settings.node_control_token or "").strip()
    if not base or not token:
        return {"status": "error", "code": "control_unconfigured", "message": "控制面未配置", "command": command}
    return await run_terminal_command(
        base, token, node_id, terminal_id, command, cwd=cwd,
    )
