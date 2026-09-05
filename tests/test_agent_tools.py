import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.config import AgentConfig
from agent.tools import ToolContext, ToolRegistry, WorkspaceSecurity


@pytest.fixture()
def tool_config(tmp_path: Path) -> AgentConfig:
    workspace = tmp_path / "workspace"
    temp = tmp_path / "temp"
    workspace.mkdir()
    temp.mkdir()
    return AgentConfig(
        workspace_root=str(workspace),
        allowed_roots=[str(workspace), str(temp)],
        denied_patterns=[r"config\.json$", r"\.env$"],
    )


@pytest.fixture()
def registry(tool_config: AgentConfig) -> ToolRegistry:
    return ToolRegistry(ToolContext(config=tool_config), agent_id=1)


def test_tool_registry_has_ga_tools(registry: ToolRegistry) -> None:
    # GA 9 atomic tools + capability_call (10 total). No scheduler_* / memory_read /
    # distill_experience / web_scan / web_execute_js.
    expected = {
        "code_run", "file_read", "file_write", "file_patch",
        "web", "update_working_checkpoint", "start_long_term_update",
        "ask_user", "capability_call",
    }
    assert expected.issubset(registry.tools)
    for removed in ("memory_read", "distill_experience", "web_scan", "web_execute_js",
                     "scheduler_create", "scheduler_list", "scheduler_cancel", "scheduler_run_now",
                     "show_file", "bbs_post", "bbs_read"):
        assert removed not in registry.tools

    schemas = registry.get_schema()
    names = {schema["function"]["name"] for schema in schemas}
    assert expected.issubset(names)
    for schema in schemas:
        assert schema["type"] == "function"
        assert schema["function"]["description"]
        assert schema["function"]["parameters"]["type"] == "object"
        assert isinstance(schema["function"]["parameters"].get("required", []), list)


def test_tool_registry_first_level_set_is_exact(registry: ToolRegistry) -> None:
    """The fixed first-level tool set is exactly these 9 names — single source.

    The frontend (user-frontend/src/api/agentClient.ts FALLBACK_BUILTIN_TOOLS) must
    mirror this set. capability_call is the second-level capability gateway (MCP
    methods + scheduler); no second-level method is registered as a first-class tool.
    """
    expected = {
        "code_run", "file_read", "file_write", "file_patch",
        "web", "update_working_checkpoint", "start_long_term_update",
        "ask_user", "capability_call",
    }
    assert set(registry.tools.keys()) == expected
    schemas = registry.get_schema()
    assert {schema["function"]["name"] for schema in schemas} == expected
    # capability_call is the only second-level gateway; MCP/scheduler are not
    # registered as their own first-class tools.
    assert "capability_call" in registry.tools
    assert not any(name.startswith("scheduler_") for name in registry.tools)
    assert not any(name.startswith("bbs_") for name in registry.tools)
    assert not any(name.startswith("memory_") for name in registry.tools)


def test_workspace_security_allows_workspace_path(tool_config: AgentConfig) -> None:
    security = WorkspaceSecurity(tool_config)

    resolved = security.resolve_path("notes/example.txt")

    assert resolved == (Path(tool_config.workspace_root) / "notes" / "example.txt").resolve()


def test_workspace_security_rejects_denied_pattern(tool_config: AgentConfig) -> None:
    security = WorkspaceSecurity(tool_config)

    with pytest.raises(PermissionError, match="denied pattern"):
        security.resolve_path("config.json")
    with pytest.raises(PermissionError, match="denied pattern"):
        security.resolve_path("secrets/.env")


@pytest.mark.asyncio
async def test_file_write_and_read_round_trip(registry: ToolRegistry) -> None:
    write_result = await registry.execute(
        "file_write",
        {"path": "docs/hello.txt", "content": "one\ntwo\nthree\n"},
    )

    assert write_result["status"] == "ok"
    assert write_result["bytes_written"] == len("one\ntwo\nthree\n".encode("utf-8"))

    read_result = await registry.execute("file_read", {"path": "docs/hello.txt", "start": 2, "count": 1})

    assert read_result["status"] == "ok"
    assert read_result["content"] == "two\n"
    assert read_result["lines"] == 1
    assert read_result["truncated"] is True


@pytest.mark.asyncio
async def test_file_patch_replaces_unique_content(registry: ToolRegistry) -> None:
    await registry.execute("file_write", {"path": "patch.txt", "content": "alpha\nbeta\ngamma\n"})

    patch_result = await registry.execute(
        "file_patch",
        {"path": "patch.txt", "old_content": "beta", "new_content": "BETA"},
    )
    read_result = await registry.execute("file_read", {"path": "patch.txt"})

    assert patch_result == {"status": "ok", "replacements": 1}
    assert read_result["content"] == "alpha\nBETA\ngamma\n"


@pytest.mark.asyncio
async def test_file_patch_rejects_non_unique_content(registry: ToolRegistry) -> None:
    await registry.execute("file_write", {"path": "dupe.txt", "content": "same\nsame\n"})

    patch_result = await registry.execute(
        "file_patch",
        {"path": "dupe.txt", "old_content": "same", "new_content": "different"},
    )

    assert patch_result["status"] == "error"
    assert "unique" in patch_result["msg"]
    assert patch_result["replacements"] == 2


@pytest.mark.asyncio
async def test_code_run_requires_approval(registry: ToolRegistry) -> None:
    # code_run without an approval coordinator is denied by default (fail closed).
    result = await registry.execute("code_run", {"code": "print('hello')", "type": "python"})
    assert result["status"] == "denied"
    assert result["code"] == "approval_required"


@pytest.mark.asyncio
async def test_code_run_denial_message_is_configurable(registry: ToolRegistry) -> None:
    """Agent 级开关未开（CDP 网页对话）：denied 文案带引导开启配置的提示。"""
    registry.set_code_run_denial("code_run 在浏览器对话中被禁用：请开启配置")
    result = await registry.execute("code_run", {"code": "print('hello')", "type": "python"})
    assert result["status"] == "denied"
    assert result["code"] == "approval_required"
    assert result["msg"] == "code_run 在浏览器对话中被禁用：请开启配置"


def test_security_rejects_path_outside_allowed_roots(tool_config: AgentConfig) -> None:
    security = WorkspaceSecurity(tool_config)

    with pytest.raises(PermissionError, match="outside allowed roots"):
        security.resolve_path("../../../etc/passwd")


# ---------------------------------------------------------------------------
# GA-style real working directory + memory subtree
# ---------------------------------------------------------------------------


@pytest.fixture()
def ga_tool_config(tmp_path: Path) -> AgentConfig:
    """Agent working dir is tmp_path/agents/1 with memory/ + workspace/."""
    agent_dir = tmp_path / "agents" / "1"
    (agent_dir / "memory" / "sop").mkdir(parents=True, exist_ok=True)
    (agent_dir / "workspace").mkdir(parents=True, exist_ok=True)
    return AgentConfig(
        workspace_root=str(agent_dir),
        allowed_roots=[str(agent_dir)],
        denied_patterns=[],
    )


@pytest.fixture()
def ga_registry(ga_tool_config: AgentConfig) -> ToolRegistry:
    return ToolRegistry(ToolContext(config=ga_tool_config), agent_id=1)


@pytest.mark.asyncio
async def test_file_read_reads_real_memory_file(ga_registry: ToolRegistry) -> None:
    sop = Path(ga_registry.context.config.workspace_root) / "memory" / "sop" / "x.md"
    sop.parent.mkdir(parents=True, exist_ok=True)
    sop.write_text("# Test SOP\nline A\nkeyword-here\nline B\n", encoding="utf-8")

    result = await ga_registry.execute(
        "file_read", {"path": "memory/sop/x.md", "keyword": "keyword"}
    )
    assert result["status"] == "ok"
    assert "keyword-here" in result["content"]
    assert result["keyword"] == "keyword"


@pytest.mark.asyncio
async def test_file_write_rejects_memory_subtree(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute(
        "file_write",
        {"path": "memory/global_mem.txt", "content": "x"},
    )
    assert result["status"] == "error"
    assert "memory/" in result["msg"]


@pytest.mark.asyncio
async def test_file_patch_rejects_memory_subtree(ga_registry: ToolRegistry) -> None:
    sop = Path(ga_registry.context.config.workspace_root) / "memory" / "sop" / "p.md"
    sop.parent.mkdir(parents=True, exist_ok=True)
    sop.write_text("alpha\n", encoding="utf-8")

    result = await ga_registry.execute(
        "file_patch",
        {"path": "memory/sop/p.md", "old_content": "alpha", "new_content": "beta"},
    )
    assert result["status"] == "error"
    assert "memory/" in result["msg"]


@pytest.mark.asyncio
async def test_file_write_to_workspace_subtree_ok(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute(
        "file_write",
        {"path": "workspace/notes.txt", "content": "hi\n"},
    )
    assert result["status"] == "ok"
    assert result["bytes_written"] == 3


# ---------------------------------------------------------------------------
# Long-term memory tool (start_long_term_update)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_long_term_update_routes_to_l2_for_fact(
    ga_registry: ToolRegistry, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Pin file_memory's memory root at this Agent's memory dir so the write lands
    # inside the fixture tree (isolated from the real data/agents).
    mem = Path(ga_registry.context.config.workspace_root) / "memory"
    monkeypatch.setattr("agent.file_memory.agent_memory_root", lambda aid: mem)

    result = await ga_registry.execute(
        "start_long_term_update",
        {"summary": "项目使用 pytest", "verified_by": "run_42"},
    )

    assert result["status"] == "ok"
    assert result["layer"] == "L2"
    assert "项目使用 pytest" in (mem / "global_mem.txt").read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_start_long_term_update_routes_to_l3_for_procedure(
    ga_registry: ToolRegistry, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mem = Path(ga_registry.context.config.workspace_root) / "memory"
    monkeypatch.setattr("agent.file_memory.agent_memory_root", lambda aid: mem)

    result = await ga_registry.execute(
        "start_long_term_update",
        {
            "summary": "先 lint 再 test",
            "verified_by": "run_7",
            "reuse_reason": "每个 PR 前都要做",
            "evidence": {"steps": ["lint", "test"]},
        },
    )

    assert result["status"] == "ok"
    assert result["layer"] == "L3"
    assert result["ref"].startswith("memory/sop/")
    sop_path = Path(ga_registry.context.config.workspace_root) / result["ref"]
    assert "先 lint 再 test" in sop_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_start_long_term_update_requires_verified_by(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute("start_long_term_update", {"summary": "x"})
    assert result["status"] == "error"
    assert "verified_by" in result["msg"]


@pytest.mark.asyncio
async def test_start_long_term_update_requires_summary(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute("start_long_term_update", {"summary": "  ", "verified_by": "r1"})
    assert result["status"] == "error"


# ---------------------------------------------------------------------------
# update_working_checkpoint (GA anchor)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_working_checkpoint_returns_ga_anchor(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute(
        "update_working_checkpoint",
        {
            "goal": "fix bug",
            "completed": ["read file"],
            "current_state": "patching",
            "next_steps": ["run tests"],
            "key_info": "tests under tests/",
            "related_files": ["memory/sop/x.md"],
        },
    )
    assert result["status"] == "ok"
    assert result["goal"] == "fix bug"
    assert result["completed"] == ["read file"]
    assert result["next_steps"] == ["run tests"]
    assert result["related_files"] == ["memory/sop/x.md"]


# ---------------------------------------------------------------------------
# web tool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_web_execute_without_browser_mcp_returns_not_configured(ga_registry: ToolRegistry) -> None:
    # No capability index → browser MCP not configured.
    result = await ga_registry.execute("web", {"operation": "execute", "script": "1+1"})
    assert result["status"] == "error"
    assert result["code"] == "browser_mcp_not_configured"


@pytest.mark.asyncio
async def test_web_scan_missing_url_errors(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute("web", {"operation": "scan"})
    assert result["status"] == "error"


@pytest.mark.asyncio
async def test_web_scan_defaults_to_http_even_with_browser_mcp(
    ga_registry: ToolRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    http = AsyncMock(return_value={"status": "ok", "source": "http"})
    browser = AsyncMock(return_value={"status": "ok", "source": "browser"})
    monkeypatch.setattr(ga_registry, "_web_http_scan", http)
    monkeypatch.setattr(ga_registry, "_web_browser_scan", browser)
    ga_registry._browser_service = "browser-service"

    result = await ga_registry.execute("web", {"operation": "scan", "url": "https://example.com"})

    assert result["source"] == "http"
    http.assert_awaited_once()
    browser.assert_not_awaited()


@pytest.mark.asyncio
async def test_web_routes_platform_cdp_method_names(ga_registry: ToolRegistry) -> None:
    runtime = type("Runtime", (), {})()
    runtime.call_tool = AsyncMock(return_value={"status": "ok"})
    ga_registry.set_mcp_runtime(
        runtime,
        capability_index=[{
            "service": "dynamic-browser",
            "browser": True,
            "method_names": ["browser_get_tabs", "browser_scan", "browser_execute_js", "browser_screenshot"],
            "methods": [],
        }],
        browser_service="dynamic-browser",
    )

    await ga_registry.execute("web", {"operation": "execute", "script": "1+1", "tab": "tab-1"})
    runtime.call_tool.assert_awaited_with(
        "dynamic-browser__browser_execute_js",
        {"script": "1+1", "switch_tab_id": "tab-1"},
    )

    await ga_registry.execute("web", {"operation": "tabs"})
    runtime.call_tool.assert_awaited_with("dynamic-browser__browser_get_tabs", {})

    await ga_registry.execute("web", {"operation": "screenshot", "tab": "tab-1"})
    runtime.call_tool.assert_awaited_with(
        "dynamic-browser__browser_screenshot",
        {"tab_id": "tab-1"},
    )


@pytest.mark.asyncio
async def test_web_screenshot_spills_mcp_envelope_to_requested_path(ga_registry: ToolRegistry) -> None:
    """The real MCP gateway envelope must become a workspace path before ToolResult."""
    import json

    inner = {"status": "success", "format": "png", "base64": _BIG_PNG_B64}
    runtime = type("Runtime", (), {})()
    runtime.call_tool = AsyncMock(return_value={
        "status": "ok",
        "service": "dynamic-browser",
        "tool": "browser_screenshot",
        "content": [{"type": "text", "text": json.dumps(inner)}],
    })
    ga_registry.set_mcp_runtime(
        runtime,
        capability_index=[{
            "service": "dynamic-browser",
            "browser": True,
            "method_names": ["browser_screenshot"],
            "methods": [],
        }],
        browser_service="dynamic-browser",
    )

    result = await ga_registry.execute("web", {
        "operation": "screenshot",
        "tab": "tab-1",
        "path": "workspace/shots/page.png",
    })

    # path is a local Agent spill instruction, not an argument understood by the
    # browser_screenshot MCP handler.
    runtime.call_tool.assert_awaited_once_with(
        "dynamic-browser__browser_screenshot", {"tab_id": "tab-1"},
    )
    assert result["path"] == "workspace/shots/page.png"
    assert "content" not in result and "base64" not in result
    assert ga_registry.context.security.resolve_path(result["path"]).is_file()


# ---------------------------------------------------------------------------
# capability_call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capability_call_requires_name(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute("capability_call", {"args": {}})
    assert result["status"] == "error"


@pytest.mark.asyncio
async def test_capability_call_unknown_capability_errors(ga_registry: ToolRegistry) -> None:
    result = await ga_registry.execute("capability_call", {"name": "foo.bar"})
    assert result["status"] == "error"
    assert result["code"] == "capability_not_found"


def test_async_tool_executes_within_loop(ga_registry: ToolRegistry) -> None:
    async def call_in_loop() -> dict:
        return await ga_registry.execute("update_working_checkpoint", {"goal": "g"})

    result = asyncio.run(call_in_loop())
    assert result["status"] == "ok"


# ---------------------------------------------------------------------------
# Binary tool output lands on disk; base64 never reaches the model
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
# A real PNG, base64-encoded, long enough to trip the >256-char binary heuristic.
# Repeated short strings don't work: base64 padding in the middle is invalid, and
# a short payload falls under the threshold. Encode actual bytes instead.
import base64 as _b64mod
_BIG_PNG_B64 = _b64mod.b64encode(b"\x89PNG\r\n\x1a\n" + b"\x00" * 400).decode()


class _FakeAttachmentStore:
    """Stand-in for attachment_store (ask_user path=... registers workspace files)."""

    def __init__(self) -> None:
        self.captured: dict | None = None

    async def register_attachment(self, *, source_path, name, mime_type, **_kw):
        self.captured = {"source_path": source_path, "name": name, "mime_type": mime_type}
        return {
            "id": 4242, "name": name, "mime_type": mime_type,
            "size_bytes": 123, "status": "active",
            "expires_at": None, "created_at": None, "last_renewed_at": None,
            "renewed_count": 0,
        }

    def to_public_dict(self, row):
        return {
            "id": row["id"], "name": row["name"], "mime_type": row["mime_type"],
            "size": row["size_bytes"], "status": row["status"],
            "expires_at": None, "created_at": None, "renewable": True,
        }


@pytest.fixture()
def fake_attachment_store(monkeypatch: pytest.MonkeyPatch) -> _FakeAttachmentStore:
    fake = _FakeAttachmentStore()
    monkeypatch.setitem(sys.modules, "attachment_store", fake)
    return fake


def test_persist_binary_result_spills_base64_to_file(ga_registry: ToolRegistry) -> None:
    """A CDP screenshot result must come back as a path, with the base64 gone."""
    import base64
    raw_result = {"status": "success", "format": "png", "base64": _BIG_PNG_B64}

    out = ga_registry._persist_binary_result(raw_result, default_stem="shot")

    # Upstream's own status value is preserved (CDP says "success", not "ok") —
    # spilling the bytes must not rewrite the tool's reported outcome.
    assert out["status"] == "success"
    # The base64 is stripped — this is what keeps it out of the model's context.
    assert "base64" not in out
    assert out["path"].endswith(".png")
    assert out["mime_type"] == "image/png"
    assert out["bytes"] == len(base64.b64decode(_BIG_PNG_B64))
    # File really exists on disk under the agent workspace.
    written = ga_registry.context.security.resolve_path(out["path"])
    assert written.is_file()
    assert written.read_bytes() == base64.b64decode(_BIG_PNG_B64)


def test_persist_binary_result_honours_requested_path(ga_registry: ToolRegistry) -> None:
    out = ga_registry._persist_binary_result(
        {"status": "success", "format": "png", "base64": _BIG_PNG_B64},
        path_hint="workspace/shots/mine.png",
    )
    assert out["path"] == "workspace/shots/mine.png"
    assert ga_registry.context.security.resolve_path(out["path"]).is_file()


def test_persist_binary_result_parses_json_string_result(ga_registry: ToolRegistry) -> None:
    """MCP handlers may hand back a JSON string; base64 must still be spilled."""
    import json
    payload = json.dumps({"status": "success", "format": "png", "base64": _BIG_PNG_B64})

    out = ga_registry._persist_binary_result(payload)

    assert isinstance(out, dict)
    assert "base64" not in out
    assert out["path"].endswith(".png")


def test_persist_binary_result_passes_through_non_binary(ga_registry: ToolRegistry) -> None:
    result = {"status": "ok", "tabs": [{"id": "1"}]}
    assert ga_registry._persist_binary_result(result) == result


def test_persist_binary_result_unwraps_mcp_content_envelope(ga_registry: ToolRegistry) -> None:
    """The in-process SSE gateway wraps tool results as ``{content: [{text: ...}]}``.

    A CDP screenshot's base64 lives inside ``content[0].text`` as a JSON string,
    which the old top-level key scan never saw — so the base64 reached the model.
    The unwrapper must reach into the envelope, spill the bytes, and return the
    flat path-only result.
    """
    import base64, json
    inner = {"status": "success", "format": "png", "base64": _BIG_PNG_B64}
    envelope = {
        "status": "ok",
        "service": "cdp-bridge",
        "tool": "browser_screenshot",
        "content": [{"type": "text", "text": json.dumps(inner, ensure_ascii=False)}],
    }

    out = ga_registry._persist_binary_result(envelope, default_stem="shot")

    # The envelope must be flattened to the path-bearing inner result, not left
    # wrapped with base64 inside content[0].text.
    assert isinstance(out, dict)
    assert "base64" not in out
    assert "content" not in out
    assert out["status"] == "success"
    assert out["path"].endswith(".png")
    assert out["bytes"] == len(base64.b64decode(_BIG_PNG_B64))
    written = ga_registry.context.security.resolve_path(out["path"])
    assert written.is_file() and written.read_bytes() == base64.b64decode(_BIG_PNG_B64)


def test_persist_binary_result_passes_through_text_only_envelope(ga_registry: ToolRegistry) -> None:
    """An MCP envelope whose content carries no binary must be returned untouched."""
    import json
    envelope = {
        "status": "ok",
        "content": [{"type": "text", "text": json.dumps({"tabs": [{"id": "1"}]})}],
    }
    assert ga_registry._persist_binary_result(envelope) == envelope


def test_present_attachment_rejects_base64(ga_registry: ToolRegistry) -> None:
    """base64 is no longer an accepted attachment source — files only."""
    result = _run(ga_registry._present_attachment({"data": _PNG_B64, "mime_type": "image/png"}))
    assert result["status"] == "error"
    assert "path 或 url" in result["msg"]


def test_present_attachment_requires_one_source(ga_registry: ToolRegistry) -> None:
    result = _run(ga_registry._present_attachment({}))
    assert result["status"] == "error"
    assert "至少传一个" in result["msg"]


def test_present_attachment_registers_workspace_file(
    ga_registry: ToolRegistry, fake_attachment_store: _FakeAttachmentStore
) -> None:
    shot = Path(ga_registry.context.config.workspace_root) / "workspace" / "shot.png"
    shot.parent.mkdir(parents=True, exist_ok=True)
    shot.write_bytes(b"\x89PNG\r\n\x1a\n")

    result = _run(ga_registry._present_attachment({"path": "workspace/shot.png"}))

    assert result["kind"] == "attachment"
    assert result["id"] == 4242
    assert fake_attachment_store.captured["source_path"] == str(shot.resolve())


def test_ask_user_collects_file_media(
    ga_registry: ToolRegistry, fake_attachment_store: _FakeAttachmentStore
) -> None:
    shot = Path(ga_registry.context.config.workspace_root) / "workspace" / "s.png"
    shot.parent.mkdir(parents=True, exist_ok=True)
    shot.write_bytes(b"\x89PNG\r\n\x1a\n")

    outcome = _run(ga_registry.execute(
        "ask_user",
        {"question": "看这张截图", "attachments": [{"path": "workspace/s.png"}]},
    ))

    data = outcome.data
    assert data["status"] == "question"
    media = data["media"]
    # Regression guard: _ask_user must not filter on status=="ok" — a registered
    # attachment carries the store's lifecycle status ("active") instead, which
    # silently dropped every file attachment.
    assert media is not None and len(media) == 1
    assert media[0]["kind"] == "attachment"
    assert media[0]["id"] == 4242


def test_ask_user_schema_has_no_base64_fields(registry: ToolRegistry) -> None:
    schema = next(s for s in registry.get_schema() if s["function"]["name"] == "ask_user")
    item_props = schema["function"]["parameters"]["properties"]["attachments"]["items"]["properties"]
    assert set(item_props) == {"path", "url", "name", "mime_type"}
