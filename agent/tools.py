"""Agent tool registry and sandboxed file/code tools.

Design: all tool methods can be sync or async. The execute() method
auto-detects and handles both transparently — AI tools don't need to
know the underlying implementation.

Sync tools (code_run, file_*): work with blocking I/O directly.
Async tools (memory_*, scheduler_*): await DB/IO operations.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import mimetypes
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from agent.config import AgentConfig
from agent.scheduler import AgentScheduler

logger = logging.getLogger(__name__)


class WorkspaceSecurity:
    """Resolve and validate file paths against the configured workspace boundary."""

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self._workspace_root = Path(config.workspace_root).resolve()
        self._allowed_roots = [Path(r).resolve() for r in config.allowed_roots]
        self._denied_patterns = [re.compile(p) for p in config.config.denied_patterns] if hasattr(config, 'config') else [re.compile(p) for p in config.denied_patterns]

    def memory_root(self) -> Path:
        """The Agent's read-only memory subtree (L1/L2/L3).

        ``file_write``/``file_patch`` must never touch this subtree; long-term
        memory is written only by ``start_long_term_update`` through file_memory.
        """
        return self._workspace_root / "memory"

    def is_memory_path(self, resolved: Path) -> bool:
        """True when ``resolved`` falls inside the read-only memory/ subtree."""
        try:
            return resolved.is_relative_to(self.memory_root().resolve())
        except (OSError, ValueError):
            return False

    def resolve_path(self, rel_or_abs: str) -> Path:
        p = Path(rel_or_abs)
        if not p.is_absolute():
            p = self._workspace_root / p
        resolved = p.resolve()

        path_str = str(resolved).replace("\\", "/")
        for pattern in self._denied_patterns:
            if pattern.search(path_str):
                raise PermissionError(
                    f"Path '{rel_or_abs}' matches denied pattern '{pattern.pattern}'"
                )

        if not any(resolved.is_relative_to(root) for root in self._allowed_roots):
            raise PermissionError(
                f"Path '{rel_or_abs}' is outside allowed roots"
            )

        return resolved


ToolResult = dict[str, Any]

# 单个工具结果回灌进上下文的字符预算（对齐 GA _get_tool_maxlen 的量级）。
# 一轮里并发调 N 个工具时按 N 均分，否则 5 个 file_read 就能把窗口占满。
_TOOL_RESULT_MAXLEN = 15000
_TOOL_RESULT_MIN_MAXLEN = 2000
# 单行上限：压缩后的 JS bundle、base64、无换行日志会用一行吃掉整个预算。
_TOOL_RESULT_LINE_MAXLEN = 8000


def _elide_middle(text: str, max_len: int) -> str:
    """按头尾保留裁剪。

    退出码、异常栈、命令结尾的失败原因都在尾部，只留头部等于把最关键的证据丢掉。
    """
    if len(text) <= max_len:
        return text
    head = int(max_len * 0.6)
    tail = max_len - head
    dropped = len(text) - max_len
    return (
        f"{text[:head]}\n...[elided {dropped} chars; head+tail kept]...\n{text[-tail:]}"
    )


def _clamp_text(text: str, max_len: int) -> str:
    """先压超长单行，再对整体做头尾裁剪。"""
    if len(text) > _TOOL_RESULT_LINE_MAXLEN:
        lines = text.split("\n")
        if any(len(line) > _TOOL_RESULT_LINE_MAXLEN for line in lines):
            text = "\n".join(
                _elide_middle(line, _TOOL_RESULT_LINE_MAXLEN) for line in lines
            )
    return _elide_middle(text, max_len)


def _clamp_result(result: Any, max_len: int) -> Any:
    """把工具结果里的长字符串裁到预算内，保持结构不变。"""
    if isinstance(result, str):
        return _clamp_text(result, max_len)
    if isinstance(result, dict):
        # 逐个长字段裁剪：stdout/stderr/content 各自受限，而不是整体 JSON 一刀切，
        # 这样 status / exit_code 这类短字段永远不会被截掉。
        clamped = {}
        for key, value in result.items():
            clamped[key] = _clamp_result(value, max_len) if isinstance(
                value, (str, dict, list)
            ) else value
        return clamped
    if isinstance(result, list):
        if not result:
            return result
        per_item = max(_TOOL_RESULT_MIN_MAXLEN // 4, max_len // len(result))
        return [_clamp_result(item, per_item) for item in result]
    return result


class ToolContext:
    """Runtime context shared by all tools."""

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.security = WorkspaceSecurity(config)

    def temp_dir(self) -> Path:
        roots = self.config.allowed_roots
        candidate = Path(roots[1]) if len(roots) > 1 else Path("agent/temp")
        directory = candidate.resolve()
        directory.mkdir(parents=True, exist_ok=True)
        return directory


class ToolRegistry:
    """Register, describe and execute sandboxed agent tools.

    Supports both sync and async tool methods transparently.
    """

    def __init__(
        self,
        context: ToolContext,
        *,
        agent_id: int | None = None,
        owner_user_id: str | None = None,
    ) -> None:
        self.context = context
        self.agent_id = agent_id
        # 当前发起者（C 端用户 uid）：决定 AI 经 capability_call 建的定时任务等
        # 副作用产物的归属。对话路径传 caller，CDP 传客户端 owner，后台任务为 None
        # （由 add_job 兜底到 Agent owner）。
        self.owner_user_id = owner_user_id
        self._code_run_approval = None
        # Agent 级开关未开时的拒绝文案（CDP 网页对话读 agents.browser_code_run_enabled，
        # 未开就不挂审批协调器）。空串=用默认「需要交互审批」文案。
        self._code_run_denied_msg: str = ""
        self._mcp_runtime = None
        # Capability index produced by GenericAgent._attach_mcp_tools: each entry
        # describes one MCP service (name, methods, browser flag, sop_ref).
        self._capability_index: list[dict] = []
        # Name of the browser-class MCP service, if any (dynamic detection).
        self._browser_service: str | None = None
        self.tools: dict[str, Callable] = {}
        self._schemas: dict[str, dict] = {}
        # Turn counter mirrored from the loop (dispatch sets it). Lets turn-gated
        # tools like start_long_term_update reject calls made too early.
        self.current_turn: int = 0
        # 本轮 LLM 请求了几个工具（dispatch 设置）。结果预算按这个数均分。
        self.current_tool_num: int = 1
        self._register_phase1()

    def set_code_run_approval(self, approval) -> None:
        self._code_run_approval = approval

    def set_code_run_denial(self, msg: str) -> None:
        """设置 code_run 的禁用文案：不挂审批协调器时，denied 里带引导用户开配置的提示。"""
        self._code_run_denied_msg = msg

    def set_mcp_runtime(
        self,
        mcp_runtime,
        *,
        capability_index: list[dict] | None = None,
        browser_service: str | None = None,
    ) -> None:
        """Inject the MCP manager plus the capability index used by web/capability_call."""
        self._mcp_runtime = mcp_runtime
        if capability_index is not None:
            self._capability_index = capability_index
        if browser_service is not None:
            self._browser_service = browser_service

    def register(
        self,
        name: str,
        func: Callable,
        schema: dict,
    ) -> None:
        self.tools[name] = func
        self._schemas[name] = schema

    async def execute(self, name: str, args: dict | None = None) -> ToolResult:
        """Run a tool by name. Auto-detects sync/async and handles both.

        - coroutine → await
        - async generator → collect all chunks
        - sync value → return directly
        """
        if name not in self.tools:
            return {"status": "error", "msg": f"Unknown tool: {name}"}
        try:
            result = self.tools[name](args or {})
            if inspect.iscoroutine(result) or inspect.isawaitable(result):
                result = await result
            elif inspect.isasyncgen(result):
                chunks = []
                async for chunk in result:
                    chunks.append(chunk)
                result = chunks
        except PermissionError as exc:
            return {"status": "error", "msg": str(exc)}
        except Exception as exc:
            logger.exception(f"Tool {name!r} raised {type(exc).__name__}")
            return {"status": "error", "msg": f"{type(exc).__name__}: {exc}"}
        # 每个结果都过预算裁剪：一个 20MB 的日志文件或一次全量 MCP 返回，
        # 在被回灌进 messages 之前就必须瘦身，否则 trim_messages 只能靠丢
        # 整条消息来救场。
        return self._clamp(result)

    def _clamp(self, result: Any) -> Any:
        budget = max(
            _TOOL_RESULT_MIN_MAXLEN,
            _TOOL_RESULT_MAXLEN // max(1, self.current_tool_num),
        )
        return _clamp_result(result, budget)

    def get_schema(self) -> list[dict]:
        return [self._schemas[name] for name in self.tools]

    def _register_phase1(self) -> None:
        # GA's 9 atomic tools (names aligned with GA ch9). ``memory_read`` is gone:
        # the Agent's memory/ subtree is real files, read with file_read.
        self.register("code_run", self._code_run, _CODE_RUN_SCHEMA)
        self.register("file_read", self._file_read, _FILE_READ_SCHEMA)
        self.register("file_write", self._file_write, _FILE_WRITE_SCHEMA)
        self.register("file_patch", self._file_patch, _FILE_PATCH_SCHEMA)
        # web aggregates web_scan + web_execute_js (platform convergence; GA
        # exposes them as two independent tools).
        self.register("web", self._web, _WEB_SCHEMA)
        self.register("update_working_checkpoint", self._update_working_checkpoint, _UPDATE_WORKING_CHECKPOINT_SCHEMA)
        self.register("start_long_term_update", self._start_long_term_update, _START_LONG_TERM_UPDATE_SCHEMA)
        self.register("ask_user", self._ask_user, _ASK_USER_SCHEMA)
        # Platform extension: one entry for dynamic MCP + scheduler.
        self.register("capability_call", self._capability_call, _CAPABILITY_CALL_SCHEMA)

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _code_run(self, args: dict) -> ToolResult:
        """Execute approved Python or PowerShell code in a subprocess."""
        if self._code_run_approval is None:
            return {
                "status": "denied",
                "code": "approval_required",
                "msg": self._code_run_denied_msg
                or "code_run requires an interactive one-time approval",
            }
        outcome, confirmation_id = self._code_run_approval.decision_for(args)
        if outcome != "allow":
            self._schedule_code_run_audit(args, confirmation_id, "denied")
            return {
                "status": "denied",
                "code": "approval_required",
                "confirmation_id": confirmation_id,
                "msg": "code_run was not approved",
            }
        code: str = args["code"]
        code_type: str = args.get("type", "python")
        timeout: int = args.get("timeout", 60)
        max_output: int = args.get("max_output_chars", 10000)

        temp_dir = self.context.temp_dir()
        started = time.monotonic()

        if code_type == "python":
            suffix = ".py"

            def build_cmd(filepath: Path) -> list[str]:
                return [sys.executable, "-X", "utf8", "-u", str(filepath)]

        elif code_type == "powershell":
            suffix = ".ps1"

            def build_cmd(filepath: Path) -> list[str]:
                import shutil
                pwsh = shutil.which("pwsh") or "powershell"
                return [pwsh, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", code]
        else:
            return {"status": "error", "msg": f"Unsupported code type: {code_type}"}

        fd, tmp_path_str = tempfile.mkstemp(suffix=suffix, dir=str(temp_dir))
        os.close(fd)
        tmp_path = Path(tmp_path_str)

        try:
            if code_type == "python":
                tmp_path.write_text(code, encoding="utf-8")
                cmd = build_cmd(tmp_path)
            else:
                cmd = build_cmd(tmp_path)

            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    cwd=str(temp_dir),
                )
                duration_ms = int((time.monotonic() - started) * 1000)
                stdout = (proc.stdout or "")[:max_output]
                stderr = (proc.stderr or "")[:max_output]
                result = {
                    "status": "ok",
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": proc.returncode,
                    "duration_ms": duration_ms,
                    "confirmation_id": confirmation_id,
                }
                self._schedule_code_run_audit(args, confirmation_id, "executed", result)
                return result
            except subprocess.TimeoutExpired:
                duration_ms = int((time.monotonic() - started) * 1000)
                result = {
                    "status": "timeout",
                    "stdout": "",
                    "stderr": f"Process timed out after {timeout}s",
                    "exit_code": None,
                    "duration_ms": duration_ms,
                    "confirmation_id": confirmation_id,
                }
                self._schedule_code_run_audit(args, confirmation_id, "timeout", result)
                return result
        finally:
            try:
                tmp_path.unlink()
            except OSError:
                pass

    def _schedule_code_run_audit(
        self, args: dict, confirmation_id: str | None, outcome: str,
        result: dict | None = None,
    ) -> None:
        try:
            from agent.tool_audit import record_tool_audit

            asyncio.get_running_loop().create_task(record_tool_audit(
                agent_id=self.agent_id,
                conversation_id=getattr(self._code_run_approval, "conversation_id", ""),
                caller=getattr(self._code_run_approval, "caller", None),
                tool_name="code_run",
                payload={"type": args.get("type", "python"), "code": args.get("code", ""), "timeout": args.get("timeout", 60)},
                approval_id=confirmation_id,
                outcome=outcome,
                result=result,
            ))
        except RuntimeError:
            pass

    @staticmethod
    def _memory_read_hint(path_arg: str) -> str:
        """Nudge the model to pin down what it just read from memory/ or a SOP.

        Reading a SOP is worthless if the content scrolls out of context a few turns
        later and the model then works from a half-remembered version. Anchoring the
        key points (and the SOP's own path in ``related_files``) is what makes the
        turn-13 "re-read the relevant SOP" escalation actionable.
        """
        normalized = str(path_arg or "").replace("\\", "/").lower()
        if "memory/" not in normalized and "sop" not in normalized:
            return ""
        return (
            "You just read a memory/SOP file. If you intend to follow it, extract its "
            "key points (especially the later sections) into update_working_checkpoint "
            "and set related_files to this path, so the steps survive context "
            "compression instead of being recalled from memory."
        )

    def _file_read(self, args: dict) -> ToolResult:
        path = self.context.security.resolve_path(args["path"])
        start: int = args.get("start", 1)
        count: int = args.get("count", 200)
        keyword: str = str(args.get("keyword") or "").strip()
        hint = self._memory_read_hint(args.get("path", ""))

        if not path.exists():
            return {"status": "error", "msg": f"File not found: {path}"}

        text = path.read_text(encoding="utf-8")
        lines = text.splitlines(keepends=True)
        total = len(lines)
        if keyword:
            # GA keyword anchor: jump to the first matching line so large files
            # (e.g. SOP bodies, logs) are read around the relevant section.
            hit = next(
                (i for i, line in enumerate(lines) if keyword.casefold() in line.casefold()),
                None,
            )
            if hit is None:
                return {
                    "status": "ok",
                    "content": "",
                    "lines": 0,
                    "total_lines": total,
                    "truncated": False,
                    "keyword": keyword,
                    "msg": f"keyword '{keyword}' not found",
                }
            begin = hit
            end = begin + count
            selected = lines[begin:end]
            result = {
                "status": "ok",
                "content": "".join(selected),
                "lines": len(selected),
                "total_lines": total,
                "truncated": end < total,
                "keyword": keyword,
                "start_line": begin + 1,
            }
            if hint:
                result["hint"] = hint
            return result
        begin = max(0, start - 1)
        end = begin + count
        selected = lines[begin:end]
        result = {
            "status": "ok",
            "content": "".join(selected),
            "lines": len(selected),
            "total_lines": total,
            "truncated": end < total,
        }
        if hint:
            result["hint"] = hint
        return result

    def _file_write(self, args: dict) -> ToolResult:
        path = self.context.security.resolve_path(args["path"])
        # memory/ is read-only via tools; long-term memory is written only by
        # start_long_term_update through file_memory, never by ad-hoc file writes.
        if self.context.security.is_memory_path(path):
            return {
                "status": "error",
                "msg": "memory/ 是只读区，请用 start_long_term_update 沉淀长期记忆",
            }
        content: str = args["content"]
        mode: str = args.get("mode", "overwrite")

        path.parent.mkdir(parents=True, exist_ok=True)
        data = content.encode("utf-8")
        if mode == "append":
            with path.open("ab") as handle:
                handle.write(data)
        elif mode == "overwrite":
            path.write_bytes(data)
        else:
            return {"status": "error", "msg": f"Invalid write mode: {mode}"}
        return {"status": "ok", "bytes_written": len(data)}

    def _file_patch(self, args: dict) -> ToolResult:
        path = self.context.security.resolve_path(args["path"])
        if self.context.security.is_memory_path(path):
            return {
                "status": "error",
                "msg": "memory/ 是只读区，请用 start_long_term_update 沉淀长期记忆",
                "replacements": 0,
            }
        old_content: str = args["old_content"]
        new_content: str = args["new_content"]

        if not path.exists():
            return {"status": "error", "msg": f"File not found: {path}", "replacements": 0}

        text = path.read_text(encoding="utf-8")
        count = text.count(old_content)
        if count != 1:
            return {
                "status": "error",
                "msg": f"old_content is not unique (found {count} occurrences)",
                "replacements": count,
            }

        path.write_text(text.replace(old_content, new_content), encoding="utf-8")
        return {"status": "ok", "replacements": 1}

    async def _present_attachment(self, item: dict) -> ToolResult:
        """Register a workspace file or public URL for ``ask_user`` display.

        Files only, by design — no inline base64. Tools that produce binary
        (CDP screenshots) already spill it to a workspace file and hand back a
        ``path``; see ``_persist_binary_result``. Keeping base64 out of tool
        results is what stops a screenshot from costing tens of thousands of
        tokens on every replayed turn.
        """
        path = str(item.get("path") or "").strip()
        url = str(item.get("url") or "").strip()
        name = str(item.get("name") or "").strip() or None
        mime_type = str(item.get("mime_type") or "").strip() or None

        if not path and not url:
            return {"status": "error", "msg": "path 或 url 至少传一个"}
        if url:
            if not url.lower().startswith(("http://", "https://")):
                return {"status": "error", "msg": "url 必须是 http(s) 公网地址"}
            return {
                "status": "ok", "kind": "url", "url": url,
                "name": name or url.rstrip("/").split("/")[-1] or "file",
                "mime_type": mime_type or mimetypes.guess_type(url)[0] or "application/octet-stream",
            }
        try:
            resolved = self.context.security.resolve_path(path)
        except PermissionError as exc:
            return {"status": "error", "msg": str(exc)}
        if not resolved.is_file():
            return {"status": "error", "msg": f"文件不存在：{path}"}
        display_name = name or resolved.name
        mime = mime_type or mimetypes.guess_type(display_name)[0] or "application/octet-stream"
        try:
            import attachment_store
            row = await attachment_store.register_attachment(
                source_path=str(resolved), name=display_name, mime_type=mime,
                owner_user_id=None, created_by_agent_id=self.agent_id,
                context_ref=None, persist=False,
            )
        except (FileNotFoundError, PermissionError, ValueError) as exc:
            return {"status": "error", "msg": str(exc)}
        result = attachment_store.to_public_dict(row)
        result["kind"] = "attachment"
        return result

    # ------------------------------------------------------------------
    # Web tool (async) — aggregates scan + browser execution
    # ------------------------------------------------------------------

    # Browser primitives that identify a browser-class MCP service (GA's web
    # tools run against a real browser). Detection is by capability, not by a
    # hardcoded service name.
    _BROWSER_PRIMITIVES = frozenset({
        "evaluate_script", "navigate", "capture_screenshot",
        "capturescreenshot", "click", "get_url", "set_url",
        "list_tabs", "get_tabs", "screenshot",
        # Platform CDP bridge naming.
        "browser_execute_js", "browser_navigate", "browser_scan",
        "browser_get_tabs", "browser_screenshot", "browser_switch_tab",
    })

    async def _web(self, args: dict) -> ToolResult:
        """Unified web tool.

        - ``scan``: read a URL. Default path is plain HTTP+text extraction. When
          a ``tab`` is given (or scan needs rendered state), route through the
          browser-class MCP service's DOM/text extraction if one is bound.
        - ``execute``/``tabs``/``screenshot``: require a browser-class MCP service.
        """
        operation = str(args.get("operation") or "scan").strip().lower()
        if operation == "scan":
            # Public/static scan stays on the plain HTTP path by default even
            # when a browser service happens to be bound. Supplying ``tab`` is
            # the explicit request for rendered/authenticated browser state.
            tab = str(args.get("tab") or "").strip()
            if tab:
                return await self._web_browser_scan(args)
            return await self._web_http_scan(args)
        if operation in {"execute", "tabs", "screenshot"}:
            return await self._web_browser_call(operation, args)
        return {"status": "error", "msg": f"Unsupported web operation: {operation}"}

    async def _web_http_scan(self, args: dict) -> ToolResult:
        """Fetch a URL and return compressed semantic text (no browser).

        Mirrors GA's web_scan intent: never return raw HTML. Strip scripts,
        styles and tags, keep link anchors and visible text, cap output size.
        """
        import urllib.request
        import html as html_mod

        url = str(args.get("url") or "").strip()
        if not url:
            return {"status": "error", "msg": "url 不能为空"}
        if not url.lower().startswith(("http://", "https://")):
            return {"status": "error", "msg": "url 必须是 http(s) 地址"}
        max_chars = int(args.get("max_chars") or 8000)
        timeout = int(args.get("timeout") or 20)
        req = urllib.request.Request(url, headers={"User-Agent": "GenericAgent/1.0 web_scan"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                raw = resp.read(2 * 1024 * 1024)  # 2MB source cap
        except Exception as exc:
            return {"status": "error", "msg": f"请求失败：{type(exc).__name__}: {exc}"}
        try:
            text = raw.decode(charset, errors="replace")
        except LookupError:
            text = raw.decode("utf-8", errors="replace")
        title_match = re.search(r"(?is)<title\b[^>]*>(.*?)</title>", text)
        title = title_match.group(1).strip() if title_match else ""
        text = re.sub(r"(?is)<script\b.*?</script>", " ", text)
        text = re.sub(r"(?is)<style\b.*?</style>", " ", text)
        text = re.sub(r"(?is)<noscript\b.*?</noscript>", " ", text)
        text = re.sub(r"(?s)<!--.*?-->", " ", text)
        text = re.sub(r"(?is)<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
                      lambda m: f" {m.group(2)} ({m.group(1)}) ", text)
        text = re.sub(r"(?s)<[^>]+>", " ", text)
        text = html_mod.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\s*\n\s*", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        truncated = len(text) > max_chars
        body = text[:max_chars]
        return {
            "status": "ok",
            "operation": "scan",
            "url": url,
            "title": title[:200],
            "content": body,
            "chars": len(body),
            "truncated": truncated,
        }

    def _resolve_browser_service(self) -> dict | None:
        """Return the capability index entry for the browser-class MCP service."""
        if not self._capability_index:
            return None
        # Prefer an explicitly-tagged browser service, else one whose tool set
        # exposes a browser primitive.
        for entry in self._capability_index:
            if entry.get("browser"):
                return entry
        for entry in self._capability_index:
            names = {str(n).lower() for n in entry.get("method_names", [])}
            if names & self._BROWSER_PRIMITIVES:
                return entry
        return None

    async def _web_browser_scan(self, args: dict) -> ToolResult:
        """Scan via the bound browser MCP (DOM/visibility-compressed text)."""
        entry = self._resolve_browser_service()
        if entry is None:
            return {
                "status": "error",
                "code": "browser_mcp_not_configured",
                "msg": "未配置浏览器 MCP 服务，请在 Agent 配置中绑定 CDP 类 MCP",
            }
        url = str(args.get("url") or "").strip()
        tab = str(args.get("tab") or "").strip()
        # Prefer a dedicated text/dom tool if the service advertises one, else
        # fall back to evaluate_script returning document body text.
        advertised = {str(n).lower(): str(n) for n in entry.get("method_names", [])}
        preferred_order = ("browser_scan", "get_text", "scan", "extract_text", "get_dom")
        preferred = next((advertised[key] for key in preferred_order if key in advertised), None)
        tool = preferred or advertised.get("evaluate_script") or advertised.get("browser_execute_js")
        if tool is None:
            return {
                "status": "error",
                "code": "browser_mcp_not_configured",
                "msg": "浏览器 MCP 不提供 browser_scan/get_text/evaluate_script，无法扫描当前 tab",
            }
        call_args: dict = {}
        tool_key = str(tool).lower()
        if tool_key == "browser_scan":
            call_args["text_only"] = True
            if tab:
                call_args["switch_tab_id"] = tab
        else:
            if url:
                call_args["url"] = url
            if tab:
                # Platform CDP bridge calls this switch_tab_id; generic browser
                # services generally accept tab.
                call_args["switch_tab_id" if tool_key == "browser_execute_js" else "tab"] = tab
            if tool_key in {"evaluate_script", "browser_execute_js"}:
                call_args["script"] = "document.body.innerText"
        namespaced = f"{entry['service']}__{tool}"
        try:
            return await self._mcp_runtime.call_tool(namespaced, call_args)
        except Exception as exc:
            return {"status": "error", "msg": f"{type(exc).__name__}: {exc}"}

    # Result keys under which an MCP tool may hand back raw base64 bytes.
    _BINARY_RESULT_KEYS = ("base64", "data", "screenshot", "image")

    def _spill_binary_dict(
        self, result: dict, *, path_hint: str = "", default_stem: str = "shot",
    ) -> dict | None:
        """Persist a decoded binary result dict and return its path-only result.

        ``None`` means the dict did not contain a decodable binary payload. This
        helper deliberately owns only the binary conversion; MCP envelope
        unwrapping stays in ``_persist_binary_result``.
        """
        # device-control get_screen_state (spec §8.3) hands the screenshot back as
        # an OBJECT ``{format, w, h, b64}`` — the generic key scan below only
        # matches top-level STRING keys, so without this normalization the JPEG
        # bytes stayed nested and reached the model as text (no file, no
        # ask_user hint, page described from the tree instead of shown).
        shot = result.get("screenshot")
        if (
            isinstance(shot, dict)
            and isinstance(shot.get("b64"), str)
            and len(shot["b64"]) > 256
        ):
            result = {
                **{k: v for k, v in result.items() if k != "screenshot"},
                "format": str(shot.get("format") or "").strip().lower() or "jpeg",
                "base64": shot["b64"],
                "screenshot_w": shot.get("w"),
                "screenshot_h": shot.get("h"),
            }

        key = next(
            (k for k in self._BINARY_RESULT_KEYS
             if isinstance(result.get(k), str) and len(result[k]) > 256),
            None,
        )
        if key is None:
            return None
        payload = result[key]
        fmt = str(result.get("format") or "").strip().lower() or "png"
        try:
            import base64 as _b64
            raw = _b64.b64decode(payload, validate=True)
        except Exception:
            return None  # Not base64 after all — leave the original result intact.

        rel = str(path_hint or "").strip().replace("\\", "/")
        if not rel:
            rel = f"workspace/screenshots/{default_stem}_{int(time.time() * 1000)}.{fmt}"
        try:
            target = self.context.security.resolve_path(rel)
        except PermissionError as exc:
            return {"status": "error", "msg": str(exc)}
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        except OSError as exc:
            return {"status": "error", "msg": f"写入截图失败: {exc}"}

        out = {k: v for k, v in result.items() if k != key}
        out["path"] = rel
        out["bytes"] = len(raw)
        out["mime_type"] = f"image/{'jpeg' if fmt in {'jpg', 'jpeg'} else fmt}"
        if "screenshot_w" in out or "screenshot_h" in out:
            out["screenshot_size"] = {
                "w": out.pop("screenshot_w", None),
                "h": out.pop("screenshot_h", None),
            }
        out.setdefault("status", "ok")
        out["hint"] = (
            f"Saved to {rel}. To show it to the user, call ask_user with "
            f"attachments=[{{\"path\": \"{rel}\"}}]."
        )
        return out

    def _persist_binary_result(
        self, result: Any, *, path_hint: str = "", default_stem: str = "shot",
    ) -> Any:
        """Land base64 bytes from an MCP result on disk; return path, not bytes.

        MCP transports may return the producer's dict directly, a JSON string,
        or an MCP ``content[].text`` envelope. Normalize all three before
        spilling, so a CDP screenshot's base64 cannot hide inside the envelope
        and reach the model context.
        """
        import json as _json

        # The in-process SSE gateway wraps ordinary tool results as
        # {content: [{type: "text", text: "{...}"}]}. If one content item is a
        # binary result, return the path-only result instead of the outer envelope.
        if isinstance(result, dict) and isinstance(result.get("content"), list):
            for item in result["content"]:
                if not isinstance(item, dict):
                    continue
                value = item.get("text")
                if isinstance(value, str):
                    try:
                        parsed = _json.loads(value)
                    except Exception:
                        continue
                elif isinstance(value, dict):
                    parsed = value
                else:
                    continue
                if isinstance(parsed, dict):
                    spilled = self._spill_binary_dict(
                        parsed, path_hint=path_hint, default_stem=default_stem,
                    )
                    if spilled is not None:
                        return spilled
            return result

        # MCP handlers may hand back a JSON string instead of a dict. Parse it
        # here too, or a stringified screenshot would sail through untouched.
        if isinstance(result, str):
            stripped = result.lstrip()
            if not stripped.startswith("{"):
                return result
            try:
                result = _json.loads(result)
            except Exception:
                return result
            if not isinstance(result, dict):
                return result

        if not isinstance(result, dict):
            return result
        spilled = self._spill_binary_dict(
            result, path_hint=path_hint, default_stem=default_stem,
        )
        return spilled if spilled is not None else result

    async def _web_browser_call(self, operation: str, args: dict) -> ToolResult:
        """execute / tabs / screenshot — always via the browser MCP."""
        entry = self._resolve_browser_service()
        if entry is None:
            return {
                "status": "error",
                "code": "browser_mcp_not_configured",
                "msg": "未配置浏览器 MCP 服务，请在 Agent 配置中绑定 CDP 类 MCP",
            }
        advertised = {str(n).lower(): str(n) for n in entry.get("method_names", [])}
        tool_map = {
            "execute": "evaluate_script",
            "tabs": "list_tabs",
            "screenshot": "capture_screenshot",
        }
        canonical = tool_map.get(operation, "evaluate_script")
        aliases = {
            "evaluate_script": {"browser_execute_js", "eval", "run_script", "evaluate"},
            "list_tabs": {"browser_get_tabs", "get_tabs", "tabs"},
            "capture_screenshot": {"browser_screenshot", "screenshot", "take_screenshot", "capturescreenshot"},
        }
        tool = advertised.get(canonical)
        if tool is None:
            # Tolerate minor naming variants across browser MCPs while preserving
            # the service's exact advertised method spelling.
            tool = next((advertised[n] for n in aliases.get(canonical, ()) if n in advertised), None)
            if tool is None:
                return {
                    "status": "error",
                    "code": "browser_mcp_not_configured",
                    "msg": f"浏览器 MCP 不提供 {canonical}，请绑定支持该能力的浏览器 MCP",
                }
        namespaced = f"{entry['service']}__{tool}"
        forward: dict = {}
        tool_key = str(tool).lower()
        if "script" in args:
            forward["script"] = args["script"]
        if "url" in args:
            forward["url"] = args["url"]
        if "timeout" in args:
            forward["timeout"] = args["timeout"]
        tab = str(args.get("tab") or "").strip()
        if tab:
            if tool_key in {"browser_execute_js", "browser_scan"}:
                forward["switch_tab_id"] = tab
            elif tool_key == "browser_screenshot":
                forward["tab_id"] = tab
            else:
                forward["tab"] = tab
        try:
            result = await self._mcp_runtime.call_tool(namespaced, forward)
            if operation == "screenshot":
                result = self._persist_binary_result(
                    result, path_hint=str(args.get("path") or ""), default_stem="shot",
                )
            if isinstance(result, dict):
                result.setdefault("operation", operation)
            return result
        except Exception as exc:
            return {"status": "error", "msg": f"{type(exc).__name__}: {exc}"}

    # ------------------------------------------------------------------
    # Long-term memory tool (async) — GA start_long_term_update
    # ------------------------------------------------------------------

    async def _start_long_term_update(self, args: dict) -> ToolResult:
        """Trigger long-term memory distillation. The model never picks storage.

        Enforces GA's "No Execution, No Memory": ``verified_by`` must cite the
        tool result that confirmed the experience. The backend decides the
        landing layer — stable facts go to L2 ``global_mem.txt``; reusable
        procedures go to an L3 SOP file under ``memory/sop/``. Either way a short
        existence pointer is added to L1.

        GA also rejects distillation early in a task — a couple of turns in there
        is nothing yet worth pinning, and writing it would just pollute memory.
        """
        if self.agent_id is None:
            return {"status": "error", "msg": "start_long_term_update 需要在 Agent 上下文中调用"}
        if self.current_turn and self.current_turn < 10:
            return {
                "status": "error",
                "msg": (
                    "start_long_term_update 只用于完成一段较长任务（≥10 轮）后有长期价值的经验沉淀。"
                    "当前任务刚开始，还没有值得写进记忆的内容；继续执行，等确实摸出可复用的坑或事实再来。"
                ),
            }
        from agent import file_memory as fm

        summary = str(args.get("summary") or "").strip()
        if not summary:
            return {"status": "error", "msg": "summary 不能为空"}
        verified_by = str(args.get("verified_by") or "").strip()
        if not verified_by:
            return {"status": "error", "msg": "只有验证过的经验才能沉淀；请提供 verified_by（引用验证该经验的工具结果）"}
        reuse_reason = str(args.get("reuse_reason") or "").strip()
        evidence = args.get("evidence") or {}
        if not isinstance(evidence, dict):
            evidence = {}
        steps = evidence.get("steps") if isinstance(evidence.get("steps"), list) else []

        try:
            layer, ref = self._route_long_term_memory(summary, reuse_reason, steps, verified_by)
            if layer == "L3":
                title = reuse_reason or summary[:60]
                slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", title).strip(".-") or "sop"
                file_ref = f"{slug}.md"
                body_lines = [
                    f"# {title}",
                    "",
                    f"- verified_by: {verified_by}",
                    "- source: agent",
                    f"- reuse_reason: {reuse_reason or 'n/a'}",
                    "",
                    "## Summary",
                    "",
                    summary,
                ]
                if steps:
                    body_lines += ["", "## Steps", ""] + [f"{i+1}. {s}" for i, s in enumerate(steps)]
                fm.write_sop(self.agent_id, file_ref, "\n".join(body_lines) + "\n")
                fm.upsert_l1_pointer(self.agent_id, slug, f"memory/sop/{file_ref}")
                return {
                    "status": "ok",
                    "layer": "L3",
                    "ref": f"memory/sop/{file_ref}",
                    "msg": "已作为可复用流程写入 L3 SOP 并更新 L1 索引",
                }
            # L2 fact
            key = reuse_reason or summary[:60]
            fm.append_l2_fact(
                self.agent_id,
                key=key,
                value=summary,
                source="agent",
                verified=True,
            )
            fm.upsert_l1_pointer(self.agent_id, key, summary[:160])
            return {
                "status": "ok",
                "layer": "L2",
                "ref": "memory/global_mem.txt",
                "msg": "已作为稳定事实写入 L2 并更新 L1 索引",
            }
        except Exception as exc:
            logger.exception("start_long_term_update failed")
            return {"status": "error", "msg": f"{type(exc).__name__}: {exc}"}

    @staticmethod
    def _route_long_term_memory(summary: str, reuse_reason: str, steps: list, verified_by: str) -> tuple[str, str]:
        """Backend decides L2 vs L3 (GA ch12). Heuristics:

        - explicit reuse_reason or ≥2 ordered steps → reusable procedure (L3);
        - otherwise → stable fact (L2).
        """
        if reuse_reason or len(steps) >= 2:
            return "L3", "memory/sop/"
        return "L2", "memory/global_mem.txt"

    # ------------------------------------------------------------------
    # Working memory tool — GA anchor (goal/completed/current_state/next_steps/...)
    # ------------------------------------------------------------------

    def _update_working_checkpoint(self, args: dict) -> ToolResult:
        """Update the Agent's working memory anchor (temporary, per-task).

        Fields mirror GA ch10's anchor: goal, completed, current_state,
        next_steps, key_info, related_files. ``related_files`` are file_read
        paths (e.g. ``memory/sop/x.md``) the loop re-reads after compression.
        """
        related_files = args.get("related_files")
        if not isinstance(related_files, list):
            related_files = []
        # Keep only string paths; they are file_read targets, not arbitrary text.
        related_files = [str(p).replace("\\", "/").strip() for p in related_files if str(p).strip()]
        return {
            "status": "ok",
            "goal": str(args.get("goal") or "").strip(),
            "completed": args.get("completed") if isinstance(args.get("completed"), list) else [],
            "current_state": str(args.get("current_state") or "").strip(),
            "next_steps": args.get("next_steps") if isinstance(args.get("next_steps"), list) else [],
            "key_info": str(args.get("key_info") or "").strip(),
            "related_files": related_files,
            "msg": "Working checkpoint updated. It will be injected into subsequent turns.",
        }

    # ------------------------------------------------------------------
    # capability_call — single entry for dynamic MCP + scheduler (async)
    # ------------------------------------------------------------------

    async def _capability_call(self, args: dict) -> ToolResult:
        """Route a capability by dotted name.

        - ``scheduler.create|list|cancel|run_now`` → AgentScheduler.
        - ``<service>.<method>`` → the Agent's bound MCP service (the service
          index is in the system prompt). On first use of an MCP service, a
          skeleton ``memory/sop/mcp/<service>_sop.md`` is generated and an L1
          pointer added so the experience can accumulate.
        """
        name = str(args.get("name") or "").strip()
        if not name:
            return {"status": "error", "msg": "capability_call 需要 name=<scope>.<method>"}
        call_args = args.get("args") or {}
        if not isinstance(call_args, dict):
            return {"status": "error", "msg": "capability_call 的 args 必须是对象"}

        scope = name.split(".", 1)[0].lower()
        if scope == "scheduler":
            return await self._scheduler_dispatch(name, call_args)

        # MCP service.method
        if "." not in name:
            return {"status": "error", "msg": f"未知的 capability: {name}（应为 <service>.<method> 或 scheduler.<action>）"}
        service, method = name.split(".", 1)
        entry = next((e for e in self._capability_index if e.get("service") == service), None)
        if entry is None:
            return {"status": "error", "code": "capability_not_found", "msg": f"未绑定 MCP 服务: {service}"}
        if self._mcp_runtime is None:
            return {"status": "error", "msg": "MCP runtime 未挂载"}
        await self._maybe_seed_mcp_sop(service, entry)
        # 按方法 schema 先做 required 缺参校验：直接放行会换来一句 Python 的
        # ``fn() missing 1 required positional argument: 'script'``——参数名在消息
        # 末尾，模型（尤其中文提示词下）常把它当上游异常重发同样的空 args 死循环
        # （CDP 网页对话曾连发 5 次 {"args":{},"name":"cdp-bridge.browser_execute_js"}）。
        # 校验消息直接给出「缺哪些参数 + 该方法的完整参数清单」，一轮即可纠正。
        missing = self._missing_required_params(entry, method, call_args)
        if missing:
            params_hint = self._format_mcp_params(self._method_schema(entry, method)) or "    - (无参数)"
            return {
                "status": "error",
                "code": "missing_required_params",
                "msg": (
                    f"{service}.{method} 缺少必填参数: {', '.join(missing)}。"
                    "这不是上游故障，不要原样重试；把缺的参数补进 args 后再调用。"
                    f"该方法全部参数：\n{params_hint}"
                ),
            }
        namespaced = f"{service}__{method}"
        try:
            result = await self._mcp_runtime.call_tool(namespaced, call_args)
        except TypeError as exc:
            # 防御性兜底：schema 没声明 required 但目标函数签名必填（或模型传了
            # 不存在的形参）。TypeError 原文是 Python 视角，翻成模型可行动的口径。
            return {
                "status": "error",
                "code": "bad_arguments",
                "msg": f"{service}.{method} 参数不符: {exc}。请对照方法参数清单修正 args，不要原样重试。",
            }
        except Exception as exc:
            return {"status": "error", "msg": f"{type(exc).__name__}: {exc}"}
        # Any MCP method may hand back inline binary (CDP screenshots do). Spill it
        # to a file here so the base64 never reaches the model's context — the same
        # guard as the web(screenshot) path, since both funnel through call_tool.
        return self._persist_binary_result(
            result, path_hint=str(call_args.get("path") or ""), default_stem=method,
        )

    async def _scheduler_dispatch(self, name: str, args: dict) -> ToolResult:
        """scheduler.create / list / cancel / run_now / propose_script_fix（collapsed entry）。

        ``approve_script`` 刻意**不在这里**：写 approved_hash 是人工授权动作，只开放给
        REST（``POST /scheduled-tasks/{id}/approve-script``），AI 无从自我授权。
        """
        action = name.split(".", 1)[1].lower() if "." in name else ""
        scheduler = AgentScheduler()
        if action == "create":
            cron_expr = str(args.get("cron_expression") or "").strip()
            task_kind = str(args.get("task_kind") or "prompt").lower()
            if not cron_expr or not str(args.get("name") or "").strip():
                return {"status": "error", "msg": "scheduler.create 需要 name、cron_expression"}
            if task_kind == "script":
                if not str(args.get("script_code") or "").strip():
                    return {"status": "error", "msg": "task_kind=script 需要 script_code"}
            elif not str(args.get("task_prompt") or "").strip():
                return {"status": "error", "msg": "task_kind=prompt 需要 task_prompt"}
            try:
                from agent.scheduler import build_trigger
                build_trigger(cron_expr)
            except Exception:
                return {"status": "error", "msg": f"Invalid schedule expression: {cron_expr}"}
            job_id = await scheduler.add_job(
                name=args["name"],
                cron_expression=cron_expr,
                task_prompt=args.get("task_prompt"),
                skill_id=args.get("skill_id"),
                enabled=args.get("enabled", True),
                agent_id=self.agent_id,
                # 归属跟着「谁在用这个 Agent」：对话路径是 caller，CDP 是客户端 owner。
                # 为 None 时 add_job 兜底查 agents.user_id——不落 user_id 会让用户态
                # 列表把任务当平台行过滤掉，用户自己看不到自己让 AI 建的任务。
                user_id=self.owner_user_id,
                task_kind=task_kind,
                script_code=args.get("script_code"),
                script_type=str(args.get("script_type") or "python"),
                script_timeout=int(args.get("script_timeout") or 300),
                background=args.get("background"),
                on_error=str(args.get("on_error") or "diagnose"),
                # approved_hash 刻意不传：AI 建的脚本任务落库即处于未授权态，必须人工
                # approve-script 才跑得起来。allow_ai_script_fix 同理只由人在 REST 设。
            )
            if not job_id:
                return {"status": "error", "msg": "定时任务创建失败（数据库不可用或参数无效）"}
            if task_kind == "script":
                return {
                    "status": "ok", "job_id": job_id,
                    "msg": "脚本任务已创建，但未授权：需人工调 approve-script 后才会执行",
                }
            return {"status": "ok", "job_id": job_id}
        if action == "list":
            enabled = args.get("enabled")
            if enabled is not None:
                enabled = bool(enabled)
            jobs = await scheduler.get_jobs(enabled=enabled)
            return {"status": "ok", "jobs": jobs}
        if action == "propose_script_fix":
            try:
                job_id = int(args["job_id"])
            except (KeyError, TypeError, ValueError):
                return {"status": "error", "msg": "propose_script_fix 需要 job_id"}
            return await scheduler.propose_script_fix(
                job_id,
                str(args.get("script_code") or ""),
                str(args.get("reason") or ""),
            )
        if action == "cancel":
            ok = await scheduler.cancel_job(int(args["job_id"]))
            if ok:
                return {"status": "ok", "msg": "Task disabled"}
            return {"status": "error", "msg": f"Task {args['job_id']} not found or already disabled"}
        if action == "run_now":
            ok = await scheduler.run_now(int(args["job_id"]))
            if ok:
                return {"status": "ok", "msg": f"Task {args['job_id']} triggered for immediate execution"}
            return {"status": "error", "msg": f"Enabled task {args['job_id']} not found"}
        return {"status": "error", "msg": f"未知的 scheduler 动作: {action}"}

    @staticmethod
    def _method_schema(entry: dict, method: str) -> Any:
        """Return the input_schema advertised for one method, or None."""
        for m in entry.get("methods") or []:
            if isinstance(m, dict) and str(m.get("name") or "") == method:
                return m.get("input_schema")
        return None

    @classmethod
    def _missing_required_params(cls, entry: dict, method: str, call_args: dict) -> list[str]:
        """List required-but-absent params per the method's advertised schema.

        空值（``""``/``None``）不算缺：有些方法允许显式传空串。schema 缺失或没有
        required 时返回空列表——校验是尽力而为，不替代真正的 schema 验证。
        """
        schema = cls._method_schema(entry, method)
        if not isinstance(schema, dict):
            return []
        required = schema.get("required")
        if not isinstance(required, list):
            return []
        return [str(r) for r in required if str(r) not in call_args]

    @staticmethod
    def _format_mcp_params(schema: Any) -> str:
        """Render an MCP inputSchema as compact per-parameter lines.

        Without this the SOP lists only method names, so the model has to guess
        argument names on a first call and learn them from error replies. The
        schema is the one piece of information it cannot derive on its own.
        """
        if not isinstance(schema, dict):
            return "    - (no parameter schema advertised)"
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            return "    - (no parameters)"
        required = schema.get("required")
        required_set = {str(r) for r in required} if isinstance(required, list) else set()
        lines: list[str] = []
        for key, spec in properties.items():
            spec = spec if isinstance(spec, dict) else {}
            kind = str(spec.get("type") or "any")
            flag = "required" if str(key) in required_set else "optional"
            desc = str(spec.get("description") or "").strip().splitlines()
            tail = f" — {desc[0]}" if desc and desc[0] else ""
            enum = spec.get("enum")
            if isinstance(enum, list) and enum:
                tail += f" (one of: {', '.join(str(v) for v in enum[:8])})"
            lines.append(f"    - `{key}` ({kind}, {flag}){tail}")
        return "\n".join(lines)

    async def seed_mcp_sops(self) -> None:
        """Seed a usage SOP for every bound MCP service, before any of them is called.

        The system prompt advertises each service with an ``(SOP: ...)`` pointer; if the
        file only appeared after the first call, that pointer dangled exactly when the
        model most needed it — on the call it had no parameter information for.
        """
        for entry in list(self._capability_index or []):
            service = entry.get("service")
            if service:
                await self._maybe_seed_mcp_sop(str(service), entry)

    async def _maybe_seed_mcp_sop(self, service: str, entry: dict) -> None:
        """Generate a skeleton MCP usage SOP with the service's parameter schemas.

        Seeded before the first call (from ``set_mcp_runtime``) rather than after it,
        so the L1 pointer the model routes on leads to something executable. The
        Experience section stays empty until a verified run distils into it.

        已存在但缺参数块（早期版本只写方法名+描述，没写 ``_format_mcp_params`` 的
        行）时**补写**：场景段把本文件整体内联进系统提示，没有参数清单的 SOP 会
        让模型只能拿方法名猜 args——CDP 网页对话曾因此连发 5 次空
        ``args={"script":...}`` 缺参调用。补写只动 methods 区，Experience 区
        （人工/AI 沉淀的经验）原样保留。
        """
        if self.agent_id is None:
            return
        try:
            from agent import file_memory as fm
            ref = f"mcp/{service}_sop.md"
            sop_root = fm.agent_sop_root(self.agent_id)
            target = sop_root / "mcp" / f"{service}_sop.md"
            method_lines = self._render_sop_methods(service, entry)
            if target.exists():
                self._backfill_sop_params(target, method_lines)
                return
            target.parent.mkdir(parents=True, exist_ok=True)
            body = (
                f"# {service} MCP SOP\n\n"
                f"## Service\n\n{service}\n\n"
                "## Invocation\n\n"
                "MCP methods are not first-class tools. Call them through "
                f"`capability_call(name=\"{service}.<method>\", args={{...}})`.\n\n"
                f"## Available methods\n\n{method_lines}\n\n"
                "## Experience\n\n"
                "None yet. After a verified successful call, use start_long_term_update "
                "to distil what mattered here (required argument combinations, "
                "preconditions, pitfalls) — not a restatement of the schema above.\n"
            )
            target.write_text(body, encoding="utf-8")
            fm.upsert_l1_pointer(self.agent_id, f"mcp.{service}", f"memory/sop/{ref}")
        except Exception as exc:  # noqa: BLE001 — SOP seeding must never block the call
            logger.debug("[sop] seed mcp sop failed service=%s: %s", service, exc)

    def _render_sop_methods(self, service: str, entry: dict) -> str:
        """Render the SOP's method list block: one bullet per method + params."""
        methods = entry.get("methods") or []
        blocks: list[str] = []
        for m in methods:
            name = m.get("name")
            raw_desc = (m.get("description") or "").strip().splitlines()
            summary = raw_desc[0] if raw_desc else ""
            blocks.append(
                f"- `capability_call(name=\"{service}.{name}\", args={{...}})`"
                + (f": {summary}" if summary else "")
                + "\n"
                + self._format_mcp_params(m.get("input_schema"))
            )
        return "\n".join(blocks) or "- (no methods discovered)"

    def _backfill_sop_params(self, target, method_lines: str) -> None:
        """Patch a stale seeded SOP that predates parameter blocks.

        早期 seed 版本只写方法名+描述。文件已存在时 ``_maybe_seed_mcp_sop``
        直接 return，于是老 Agent 的 SOP 永远没有参数清单，而场景段把它整体
        内联进系统提示——模型只能拿方法名猜 args。这里检测「methods 区缺少
        参数行」并原位补齐；Experience 区不动。
        """
        try:
            text = target.read_text(encoding="utf-8")
        except OSError:
            return
        # 新版 seed 的 methods 区每个方法下面有 "    - `param`" 参数行；老版没有。
        if re.search(r"^- `capability_call\(name=.*\n    - `", text, flags=re.MULTILINE | re.DOTALL):
            return
        # 只重写 methods 区：## Available methods 到下一个 ## 之间。
        patched = re.sub(
            r"(## Available methods\n\n)([\s\S]*?)(\n\n## )",
            lambda m: m.group(1) + method_lines + m.group(3),
            text,
            count=1,
        )
        if patched == text:
            return
        try:
            target.write_text(patched, encoding="utf-8")
        except OSError:
            return

    async def _ask_user(self, args: dict) -> "StepOutcome":
        """Interrupt execution to ask the user a question, optionally with
        attachments (merged from the old ``show_file`` tool). Workspace files,
        public URLs and inline base64 are registered as media the client renders
        inline.
        """
        from agent.agent_loop import StepOutcome

        attachments = args.get("attachments") or []
        media: list[dict] = []
        if isinstance(attachments, list):
            for item in attachments:
                if not isinstance(item, dict):
                    continue
                result = await self._present_attachment(item)
                # Success is "has a kind", NOT status=="ok": a registered
                # attachment carries the attachment_store lifecycle status
                # ("active"/"expired"/"purged") in that field, so filtering on
                # "ok" silently dropped every workspace-file and base64 image
                # and only ever let public URLs through.
                if result.get("kind"):
                    media.append(result)
        return StepOutcome(
            data={
                "status": "question",
                "question": args.get("question") or "",
                "candidates": args.get("candidates"),
                "media": media or None,
            },
            should_exit=True,
        )


# ---------------------------------------------------------------------------
# OpenAI function-calling schemas
# ---------------------------------------------------------------------------

_CODE_RUN_SCHEMA = {
    "type": "function",
    "function": {
        "name": "code_run",
        "description": (
            "Execute Python or PowerShell in a subprocess. This is the only way to "
            "observe real state: use it to probe the environment, run commands, verify "
            "a change actually took effect, and produce the evidence that "
            "start_long_term_update requires. Prefer it over asserting an outcome. "
            "Requires a one-time interactive approval; a denied call returns "
            "status=denied instead of running. Never use it to write files under "
            "memory/ — that path is reserved for start_long_term_update."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": (
                        "Complete, self-contained source. Print what you need to see: "
                        "stdout/stderr are the only things returned."
                    ),
                },
                "type": {
                    "type": "string",
                    "enum": ["python", "powershell"],
                    "default": "python",
                    "description": "Interpreter. Use powershell only for Windows shell work.",
                },
                "timeout": {
                    "type": "integer",
                    "default": 60,
                    "description": (
                        "Seconds before the process is killed and status=timeout is "
                        "returned. Raise it for builds or installs."
                    ),
                },
            },
            "required": ["code"],
        },
    },
}

_FILE_READ_SCHEMA = {
    "type": "function",
    "function": {
        "name": "file_read",
        "description": (
            "Read a file. Call this before editing anything, and call it to pull a SOP "
            "body: the L1 index only gives you SOP names, so the actual steps must be "
            "read from memory/sop/<name>.md every time you need them — never work from "
            "a remembered version. Large files come back truncated; use keyword to jump "
            "to the relevant section or start/count to page through."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Relative path, e.g. memory/sop/browser_sop.md or workspace/notes.txt"},
                "start": {
                    "type": "integer",
                    "default": 1,
                    "description": "1-based first line to return. Use with count to page a long file.",
                },
                "count": {
                    "type": "integer",
                    "default": 200,
                    "description": (
                        "How many lines to return. The reply's truncated flag tells you "
                        "whether more remains past start+count."
                    ),
                },
                "keyword": {"type": "string", "description": "Optional anchor: start reading from the first line containing this substring"},
            },
            "required": ["path"],
        },
    },
}

_FILE_WRITE_SCHEMA = {
    "type": "function",
    "function": {
        "name": "file_write",
        "description": (
            "Create a new file or replace one wholesale. Use this for new files and for "
            "generated output; use file_patch to change part of an existing file, so an "
            "overwrite never silently drops content you never read. memory/ is rejected "
            "here — long-term memory is written only by start_long_term_update."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Relative path under the working directory, e.g. workspace/report.md",
                },
                "content": {"type": "string", "description": "Full file content to write (UTF-8)"},
                "mode": {
                    "type": "string",
                    "enum": ["overwrite", "append"],
                    "default": "overwrite",
                    "description": "overwrite replaces the whole file; append adds to the end",
                },
            },
            "required": ["path", "content"],
        },
    },
}

_FILE_PATCH_SCHEMA = {
    "type": "function",
    "function": {
        "name": "file_patch",
        "description": (
            "Preferred way to edit an existing file: swap one exact block for another. "
            "old_content must appear exactly once — read the file with file_read first "
            "and copy the block verbatim (indentation and full-width characters "
            "included), otherwise the patch is rejected rather than applied to the "
            "wrong place."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Relative path of the file to edit"},
                "old_content": {
                    "type": "string",
                    "description": "Exact block to replace; must be unique in the file",
                },
                "new_content": {"type": "string", "description": "Replacement text"},
            },
            "required": ["path", "old_content", "new_content"],
        },
    },
}

_WEB_SCHEMA = {
    "type": "function",
    "function": {
        "name": "web",
        "description": (
            "Read or drive the web. Use operation=scan to fetch page text (plain HTTP, "
            "no browser needed) — that is the right call for reading docs or checking a "
            "public page. Use tabs/execute/screenshot to act inside the user's live "
            "browser session: those need a bound browser-class MCP service and return "
            "browser_mcp_not_configured when none is attached. Before scripting a live "
            "page, read memory/sop/browser_sop.md — the element/click/navigation rules "
            "there are not guessable."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["scan", "execute", "tabs", "screenshot"],
                    "default": "scan",
                    "description": (
                        "scan = fetch page text; tabs = list open browser tabs; "
                        "execute = run JS in a tab; screenshot = capture a tab as PNG"
                    ),
                },
                "url": {"type": "string", "description": "Target URL (scan / execute / screenshot)"},
                "tab": {"type": "string", "description": "Optional browser tab identifier; forces browser-class scan when set"},
                "script": {"type": "string", "description": "JavaScript to execute (operation=execute)"},
                "path": {
                    "type": "string",
                    "description": (
                        "Where to save the image (operation=screenshot), e.g. "
                        "workspace/shots/login.png. Defaults to a timestamped file under "
                        "workspace/screenshots/. The result carries this path — pass it to "
                        "ask_user to show the user; the image bytes are never returned inline."
                    ),
                },
                "max_chars": {
                    "type": "integer",
                    "default": 8000,
                    "description": "Cap on returned page text; raise it when the part you need is cut off",
                },
                "timeout": {"type": "integer", "default": 20, "description": "Seconds to wait for the page or script"},
            },
            "required": ["operation"],
        },
    },
}

_SHOW_FILE_REMOVED = None  # merged into ask_user

_START_LONG_TERM_UPDATE_SCHEMA = {
    "type": "function",
    "function": {
        "name": "start_long_term_update",
        "description": (
            "Distill a verified experience into long-term memory. The backend decides "
            "whether it lands as a stable fact (L2) or a reusable procedure (L3) and "
            "adds a short L1 pointer. Call it only near the end of a task, only for "
            "something reusable by a *future* task, and only after a tool result proved "
            "it — no execution, no memory. Per-task state belongs in "
            "update_working_checkpoint instead; read memory/sop/memory_management_sop.md "
            "before the first write."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": (
                        "The reusable lesson in one or two sentences, written so a future "
                        "task can act on it without this conversation"
                    ),
                },
                "verified_by": {
                    "type": "string",
                    "description": "The concrete tool result that proved it (command run, output seen, file checked)",
                },
                "reuse_reason": {"type": "string", "description": "Why reusable across tasks"},
                "evidence": {
                    "type": "object",
                    "description": "Optional supporting detail backing verified_by",
                    "properties": {
                        "files": {
                            "type": "array", "items": {"type": "string"},
                            "description": "Paths that were read or changed",
                        },
                        "steps": {
                            "type": "array", "items": {"type": "string"},
                            "description": "The steps that were actually executed",
                        },
                    },
                },
            },
            "required": ["summary", "verified_by"],
        },
    },
}

_MEMORY_READ_REMOVED = None  # memory/ files are read with file_read

_CAPABILITY_CALL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "capability_call",
        "description": (
            "Invoke dynamic MCP or scheduler capabilities by dotted name — they are not "
            "separate tools, they all go through here. Examples: "
            "capability_call(name='mail.search_messages', args={...}) or "
            "capability_call(name='scheduler.create', args={...}). Only names listed "
            "under [Available Capabilities] in the system prompt exist; if a service has "
            "a (SOP: ...) pointer, read that SOP with file_read before the first call "
            "instead of guessing the arguments."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Dotted capability name, '<service>.<method>' or 'scheduler.<action>'",
                },
                "args": {
                    "type": "object",
                    "description": "Argument object for that method, per its schema in the capability index",
                },
            },
            "required": ["name"],
        },
    },
}

_UPDATE_WORKING_CHECKPOINT_SCHEMA = {
    "type": "function",
    "function": {
        "name": "update_working_checkpoint",
        "description": (
            "Rewrite the per-task working anchor, which is re-injected every turn and is "
            "the only context guaranteed to survive history compression. Call it after "
            "learning something you must not forget, before a risky step, and whenever "
            "the plan changes. Fields are replaced, not merged — resend the whole state."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "goal": {"type": "string", "description": "The task's end state in one sentence"},
                "completed": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Steps already verified done",
                },
                "current_state": {
                    "type": "string",
                    "description": "Where the work stands right now, including the last failure",
                },
                "next_steps": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Concrete remaining steps in order",
                },
                "key_info": {
                    "type": "string",
                    "description": "Facts discovered this task (ids, paths, commands, quirks) that would be expensive to rediscover",
                },
                "related_files": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Paths to re-read with file_read, e.g. the governing SOP",
                },
            },
            "required": [],
        },
    },
}

_SCHEDULER_SCHEMAS_REMOVED = None  # collapsed into capability_call

_ASK_USER_SCHEMA = {
    "type": "function",
    "function": {
        "name": "ask_user",
        "description": (
            "Ask the user a question, or show them something (a file, an "
            "image, a screenshot). Use this whenever the user needs to see "
            "something — a file you wrote, a screenshot another tool saved, "
            "a fetched image. Pass `attachments` and the client renders it "
            "inline next to the question. Note: calling this stops the run — "
            "batch everything you want to ask or show into one call. Ask only "
            "for what you cannot determine yourself (a preference, a "
            "credential, a go/no-go on an irreversible step); anything "
            "discoverable by file_read or code_run should be discovered, not "
            "asked.\n"
            "Each attachment takes a workspace `path` or a public `url`. To "
            "show a screenshot from web(operation=screenshot) or any binary a "
            "tool produced, pass the `path` that tool returned — it has "
            "already been saved to disk for you. Do not invent a path or url "
            "you have not verified exists."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "The question or message to present to the user"},
                "candidates": {"type": "array", "items": {"type": "string"}, "description": "Optional suggested answers"},
                "attachments": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string", "description": "Workspace file path (e.g. the path a screenshot tool returned)"},
                            "url": {"type": "string", "description": "Public http(s) URL"},
                            "name": {"type": "string"},
                            "mime_type": {"type": "string"},
                        },
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["question"],
        },
    },
}
