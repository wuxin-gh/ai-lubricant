import main


CLAUDE_CODE_HEADERS = {
    "x-app": "cli",
    "user-agent": "claude-cli/2.1.165 (external, cli)",
    "anthropic-beta": "claude-code-20250219,context-1m-2025-08-07,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advanced-tool-use-2025-11-20,effort-2025-11-24,afk-mode-2026-01-31",
    "anthropic-dangerous-direct-browser-access": "true",
}


CODEX_HEADERS = {
    "originator": "Codex Desktop",
    "user-agent": "Codex Desktop/0.137.0-alpha.4 (Windows 10.0.26200; x86_64) unknown (Codex Desktop; 26.602.30954)",
    "x-codex-window-id": "019e9c6d-659f-7280-bd11-9238d0918b70:0",
    "x-client-request-id": "019e9c6d-659f-7280-bd11-9238d0918b70",
    "x-codex-beta-features": "terminal_resize_reflow",
    "x-codex-turn-metadata": '{"session_id":"019e9c6d-659f-7280-bd11-9238d0918b70","thread_id":"019e9c6d-659f-7280-bd11-9238d0918b70","thread_source":"user","turn_id":"019e9c6d-6788-7dc0-8483-ef6919340419","sandbox":"windows_elevated","turn_started_at_unix_ms":1780740876461,"workspace_kind":"projectless","request_kind":"turn","window_id":"019e9c6d-659f-7280-bd11-9238d0918b70:0"}',
}


# codex-tui 0.146.0 真实抓包头（终端版 Codex，与 Desktop 不同的 originator/UA/beta 头）。
CODEX_TUI_HEADERS = {
    "originator": "codex-tui",
    "user-agent": "codex-tui/0.146.0 (Windows 10.0.26200; x86_64) unknown (codex-tui; 0.146.0)",
    "x-codex-window-id": "01a0627a-3ef1-72f2-8900-38c31be2f087:0",
    "x-client-request-id": "01a0627a-3ef1-72f2-8900-38c31be2f087",
    "session-id": "01a0627a-3ef1-72f2-8900-38c31be2f087",
    "thread-id": "01a0627a-3ef1-72f2-8900-38c31be2f087",
    "x-codex-turn-metadata": '{"installation_id":"4de163e5-08f4-442c-9698-fc57a64ffbd7","session_id":"01a0627a-3ef1-72f2-8900-38c31be2f087"}',
}


def test_detects_claude_code_from_user_agent():
    assert main._detect_client_type({}, CLAUDE_CODE_HEADERS, "/v1/messages") == "claude-code"


def test_detects_codex_from_user_agent():
    assert main._detect_client_type({}, CODEX_HEADERS, "/v1/messages") == "codex-cli"


def test_detects_codex_tui_from_captured_headers():
    assert main._detect_client_type({}, CODEX_TUI_HEADERS, "/v1/responses") == "codex-tui"


def test_detects_codex_tui_from_codex_headers_when_user_agent_is_stripped():
    headers = {"x-codex-window-id": "thread:0", "x-codex-turn-metadata": "{}"}
    assert main._detect_client_type({}, headers, "/v1/responses") == "codex-tui"


def test_detects_opencode_from_user_agent():
    headers = {"user-agent": "opencode/1.14.48 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13"}
    assert main._detect_client_type({}, headers, "/v1/messages") == "opencode"


def test_detects_claude_code_from_first_system_message_when_user_agent_unknown():
    body = {"messages": [{"role": "system", "content": "You are Claude Code, Anthropic's official CLI for Claude. You are an interactive agent that helps users with software engineering tasks."}]}
    assert main._detect_client_type(body, {"user-agent": "unknown-client/1.0"}, "/v1/messages") == "claude-code"


def test_detects_codex_from_first_system_message_when_user_agent_unknown():
    body = {"messages": [{"role": "system", "content": "You are Codex, a coding agent based on GPT-5. You and the user share one workspace."}]}
    assert main._detect_client_type(body, {"user-agent": "unknown-client/1.0"}, "/v1/messages") == "codex-tui"


def test_detects_codex_from_responses_instructions_when_headers_unknown():
    body = {"instructions": "You are Codex, a coding agent based on GPT-5. You and the user share one workspace."}
    assert main._detect_client_type(body, {"user-agent": "unknown-client/1.0"}, "/v1/responses") == "codex-tui"


def test_detects_opencode_from_first_system_message_when_user_agent_unknown():
    body = {"messages": [{"role": "system", "content": "You are a title generator. You output ONLY a thread title. Nothing else. <task>"}]}
    assert main._detect_client_type(body, {"user-agent": "unknown-client/1.0"}, "/v1/messages") == "opencode"


def test_body_client_type_overrides_message_fallback():
    body = {
        "client_type": "cursor",
        "messages": [{"role": "system", "content": "You are Claude Code, Anthropic's official CLI for Claude."}],
    }
    assert main._detect_client_type(body, {"user-agent": "unknown-client/1.0"}, "/v1/messages") == "cursor"


def test_generic_anthropic_header_does_not_become_claude_code():
    headers = {"user-agent": "anthropic-python/1.0.0", "anthropic-beta": "claude-code-20250219"}
    assert main._detect_client_type({}, headers, "/v1/messages") == "anthropic"


WORKBUDDY_HEADERS = {
    "user-agent": "WorkBuddy/5.2.5 WorkBuddy/5.2.5 CLI/2.106.4",
    "x-ide-name": "WorkBuddy",
    "x-ide-type": "WorkBuddy",
    "x-codebuddy-request": "1",
    "x-domain": "www.codebuddy.cn",
}


def test_detects_workbuddy_from_user_agent():
    assert main._detect_client_type({}, WORKBUDDY_HEADERS, "/v1/chat/completions") == "workbuddy"


def test_detects_workbuddy_from_x_ide_name_when_user_agent_unknown():
    headers = {"user-agent": "unknown-client/1.0", "x-ide-name": "WorkBuddy"}
    assert main._detect_client_type({}, headers, "/v1/chat/completions") == "workbuddy"


def test_detects_workbuddy_from_x_codebuddy_request_marker():
    # CLI 版 CodeBuddy user-agent 不含 workbuddy/，但带 x-codebuddy-request=1 兜底命中。
    headers = {"user-agent": "CLI/2.116.0 WorkBuddy/2.116.0", "x-codebuddy-request": "1"}
    assert main._detect_client_type({}, headers, "/v1/chat/completions") == "workbuddy"


def test_generic_openai_client_does_not_become_workbuddy():
    headers = {"user-agent": "openai-python/1.0.0"}
    assert main._detect_client_type({}, headers, "/v1/chat/completions") == "openai"
