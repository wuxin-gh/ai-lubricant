import json
import uuid

from providers.custom import CustomProvider
from message_utils import responses_to_openai_messages


CLAUDE_CODE_PROMPT = "You are Claude Code, Anthropic's official CLI for Claude. You are an interactive agent that helps users with software engineering tasks. "
CODEX_PROMPT = "You are Codex, a coding agent based on GPT-5. You and the user share one workspace, and your job is to collaborate with them until their goal is genuinely handled."
OPENCODE_PROMPT = ""


def _provider(protocol="openai", preset="none"):
    return CustomProvider(
        "u",
        "sk-test",
        protocol=protocol,
        base_url="https://example.invalid",
        client_preset=preset,
    )


def test_chat_protocol_default_preserves_messages_without_opencode_system_marker():
    provider = _provider("openai")

    payload = provider._build_openai_payload("m", [{"role": "user", "content": "hi"}], False)

    assert payload["messages"] == [{"role": "user", "content": "hi"}]


def test_non_simulated_responses_default_does_not_inject_reasoning_or_search(monkeypatch):
    monkeypatch.setattr(
        "providers.custom.Config.get_simulated_client_defaults",
        lambda: {"reasoning_effort": "high", "auto_search": True},
    )
    provider = _provider("responses")

    payload = provider._build_protocol_payload(
        "responses",
        "m",
        [{"role": "user", "content": "hi"}],
        False,
    )

    assert "reasoning" not in payload
    assert "auto_search" not in payload


def test_non_simulated_anthropic_default_does_not_inject_thinking(monkeypatch):
    monkeypatch.setattr(
        "providers.custom.Config.get_simulated_client_defaults",
        lambda: {"reasoning_effort": "high", "auto_search": True},
    )
    provider = _provider("anthropic")

    payload = provider._build_protocol_payload(
        "anthropic",
        "m",
        [{"role": "user", "content": "hi"}],
        False,
    )

    assert "thinking" not in payload
    assert "reasoning" not in payload
    assert "auto_search" not in payload


def test_chat_protocol_preserves_client_system_message():
    provider = _provider("openai")

    payload = provider._build_openai_payload(
        "m",
        [
            {"role": "system", "content": "You are a coding agent. Execute the task."},
            {"role": "user", "content": "hi"},
        ],
        False,
    )

    assert payload["messages"] == [
        {"role": "system", "content": "You are a coding agent. Execute the task."},
        {"role": "user", "content": "hi"},
    ]


def test_anthropic_protocol_default_adds_claude_code_marker():
    provider = _provider("anthropic")

    payload = provider._openai_request_to_anthropic({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 1,
    })

    assert payload["system"].startswith(CLAUDE_CODE_PROMPT)


def test_responses_protocol_default_adds_codex_marker():
    provider = _provider("responses")

    payload = provider._apply_client_preset_body({"model": "m", "input": "hi"}, "responses")

    assert payload["instructions"].startswith(CODEX_PROMPT)


def test_codex_preset_on_openai_channel_builds_responses_payload():
    """protocol=openai + client_preset=codex-cli 必须按 Responses 形态构造上游 payload。

    回归 https:// 伪 codex 渠道场景：渠道 protocol=openai 但伪装 codex-cli 客户端，
    发出去的 body 应是 Responses 格式（input/instructions/max_output_tokens/reasoning），
    而不是 OpenAI 格式（messages/reasoning_effort/stream_options）。
    """
    provider = _provider("openai", "codex-cli")

    payload = provider._build_protocol_payload(
        "openai",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
        max_tokens=64000,
    )

    assert "input" in payload
    assert "messages" not in payload
    assert payload["max_output_tokens"] == 64000
    assert "max_tokens" not in payload
    assert "instructions" in payload
    assert payload["instructions"].startswith(CODEX_PROMPT)
    # Responses 协议不该混入 OpenAI chat 字段
    assert "reasoning_effort" not in payload
    assert "stream_options" not in payload
    # reasoning 应为 Responses 形态对象（或不存在），绝不可能是 "none" 字符串
    if "reasoning" in payload:
        assert isinstance(payload["reasoning"], dict)
        assert payload["reasoning"].get("effort") != "none"


def test_codex_openai_preset_keeps_openai_messages_and_adds_input(monkeypatch):
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "codex-openai")

    payload = provider._build_protocol_payload(
        "openai",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
        max_tokens=64000,
        request_id="req-codex-openai",
    )

    assert "messages" in payload
    assert "input" in payload
    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][0]["content"].startswith(CODEX_PROMPT)
    assert payload["messages"][1] == {"role": "user", "content": "hi"}
    assert payload["input"] == [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
    assert payload["max_tokens"] == 64000
    assert "max_output_tokens" not in payload
    assert payload["reasoning"] == {"effort": "medium"}
    # 无有效 tools 时不再发送 tool_choice（上游校验：'tool_choice' is only allowed when 'tools' are specified）
    assert "tool_choice" not in payload
    assert payload["tools"] == []
    # 会话身份每请求现造，与 header 同源；installation id 是机器级常量。
    headers = provider._headers("openai", kwargs={"request_id": "req-codex-openai"})
    metadata = payload["client_metadata"]
    assert metadata["x-codex-installation-id"] == CustomProvider.CODEX_INSTALLATION_ID
    assert metadata["session_id"] == headers["session-id"]
    assert metadata["x-codex-turn-metadata"] == headers["x-codex-turn-metadata"]
    assert payload["prompt_cache_key"] == headers["thread-id"]
    assert payload["parallel_tool_calls"] is True


def test_codex_openai_preset_preserves_client_input_when_present():
    provider = _provider("openai", "codex-openai")
    client_input = [{"role": "user", "content": [{"type": "input_text", "text": "from-client"}]}]

    payload = provider._build_protocol_payload(
        "openai",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
        input=client_input,
    )

    assert payload["input"] == client_input
    assert payload["messages"][1] == {"role": "user", "content": "hi"}


def test_codex_openai_preset_headers_match_codex_client():
    provider = _provider("openai", "codex-openai")

    headers = provider._headers()
    turn_metadata = json.loads(headers["x-codex-turn-metadata"])

    assert headers["originator"] == "Codex Desktop"
    assert headers["User-Agent"] == "Codex Desktop/0.137.0-alpha.4 (Windows 10.0.26200; x86_64) unknown (Codex Desktop; 26.602.40724)"
    assert turn_metadata["session_id"] == headers["session-id"]

def test_codex_preset_on_openai_channel_emits_responses_for_anthropic_client():
    """/v1/messages 入口（anthropic）路由到 protocol=openai+client_preset=codex-cli 渠道，
    上游 payload 仍应是 Responses 形态。"""
    provider = _provider("openai", "codex-cli")

    payload = provider._build_protocol_payload(
        "openai",
        "gpt-5.5",
        [
            {"role": "system", "content": "You are Codex, a coding agent based on GPT-5."},
            {"role": "user", "content": "hi"},
        ],
        True,
    )

    assert "input" in payload and payload["input"]
    assert "messages" not in payload
    assert "reasoning_effort" not in payload
    assert "stream_options" not in payload


def test_non_spoofed_openai_channel_keeps_openai_payload():
    """未伪装（client_preset=none）的 openai 渠道仍发 OpenAI 格式，不受 preset 改写影响。"""
    provider = _provider("openai", "none")

    payload = provider._build_protocol_payload(
        "openai",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
    )

    assert "messages" in payload
    assert "input" not in payload


def test_explicit_client_preset_overrides_protocol_default():
    provider = _provider("responses", "claude-code")

    payload = provider._apply_client_preset_body({"model": "m", "input": "hi"}, "responses")

    assert payload["instructions"].startswith(CLAUDE_CODE_PROMPT)


def test_client_preset_skips_when_client_already_matches():
    provider = _provider("anthropic")

    payload = provider._build_protocol_payload(
        "anthropic",
        "m",
        [{"role": "user", "content": "hi"}],
        False,
        client_type="claude-code",
    )

    assert "system" not in payload
    assert payload["messages"] == [{"role": "user", "content": "hi"}]


def test_client_preset_does_not_duplicate_existing_claude_code_marker():
    provider = _provider("anthropic")

    payload = provider._build_protocol_payload(
        "anthropic",
        "m",
        [
            {"role": "system", "content": "You are Claude Code"},
            {"role": "user", "content": "hi"},
        ],
        False,
    )

    assert payload["system"] == "You are Claude Code"
    assert payload["messages"] == [{"role": "user", "content": "hi"}]


def test_responses_tools_are_not_added_when_client_omits_tools():
    _, kwargs = responses_to_openai_messages({"model": "m", "input": "hi"})

    assert "tools" not in kwargs


def test_responses_tool_search_schema_is_dropped_for_openai_compat():
    _, kwargs = responses_to_openai_messages({
        "model": "m",
        "input": "hi",
        "tools": [
            {"type": "function", "name": "shell_command", "parameters": {"type": "object"}},
            {"type": "tool_search", "description": "discover tools"},
        ],
    })

    assert kwargs["tools"] == [{
        "type": "function",
        "function": {"name": "shell_command", "parameters": {"type": "object"}},
    }]


def test_responses_payload_normalizes_openai_function_tools():
    provider = _provider("responses")

    payload = provider._apply_client_preset_body({
        "model": "m",
        "input": "hi",
        "tools": [
            {"type": "function", "function": {"name": "shell_command", "parameters": {"type": "object"}}},
            {"type": "tool_search", "description": "discover tools"},
        ],
    }, "responses")

    assert payload["tools"] == [{
        "type": "function",
        "name": "shell_command",
        "parameters": {"type": "object"},
    }]


def test_responses_payload_drops_tool_choice_when_no_tools():
    """无有效 tools 时（非模拟客户端/纯透传），协议默认注入的 tool_choice 必须被移除，
    否则上游报错 'tool_choice' is only allowed when 'tools' are specified。"""
    provider = _provider("responses")

    payload = provider._apply_client_preset_body({"model": "m", "input": "hi"}, "responses")

    assert "tool_choice" not in payload


def test_responses_payload_keeps_tool_choice_when_tools_present():
    """有有效 tools 时 tool_choice 应保留。"""
    provider = _provider("responses")

    payload = provider._apply_client_preset_body({
        "model": "m",
        "input": "hi",
        "tools": [{"type": "function", "function": {"name": "shell_command", "parameters": {"type": "object"}}}],
        "tool_choice": "auto",
    }, "responses")

    assert payload["tools"]
    assert payload["tool_choice"] == "auto"


def test_codex_preset_body_defaults_are_added_for_responses_conversion(monkeypatch):
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("responses", "codex-cli")

    payload = provider._build_protocol_payload(
        "responses",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
        request_id="req-codex-responses",
    )

    assert payload["text"] == {"verbosity": "low"}
    assert payload["store"] is False
    assert payload["include"] == ["reasoning.encrypted_content"]
    assert payload["reasoning"] == {"effort": "medium"}
    # 无有效 tools 时不再发送 tool_choice（上游校验：'tool_choice' is only allowed when 'tools' are specified）
    assert "tool_choice" not in payload
    # 会话身份每请求现造，与 header 同源；installation id 是机器级常量。
    headers = provider._headers("responses", kwargs={"request_id": "req-codex-responses"})
    metadata = payload["client_metadata"]
    assert metadata["x-codex-installation-id"] == CustomProvider.CODEX_INSTALLATION_ID
    assert metadata["session_id"] == headers["session-id"]
    assert metadata["x-codex-turn-metadata"] == headers["x-codex-turn-metadata"]
    assert payload["prompt_cache_key"] == headers["thread-id"]
    assert payload["parallel_tool_calls"] is True


    provider = _provider("responses", "codex-cli")

    payload = provider._build_protocol_payload(
        "responses",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
        text={"verbosity": "low"},
        reasoning={"effort": "medium"},
        include=["reasoning.encrypted_content"],
        store=False,
        prompt_cache_key="session-key",
        client_metadata={"x-codex-installation-id": "install-id"},
        parallel_tool_calls=True,
        request_id="req-codex-explicit",
    )

    assert payload["text"] == {"verbosity": "low"}
    assert payload["reasoning"] == {"effort": "medium"}
    assert payload["include"] == ["reasoning.encrypted_content"]
    assert payload["store"] is False
    # 客户端显式给出的 prompt_cache_key 优先，不被现造身份覆盖。
    assert payload["prompt_cache_key"] == "session-key"
    # client_metadata 是深度缺省合并：客户端显式给的键保留，其余会话身份键按本次
    # identity 补齐（与 header 同源），不留半份 metadata。
    assert payload["client_metadata"]["x-codex-installation-id"] == "install-id"
    explicit_headers = provider._headers("responses", kwargs={"request_id": "req-codex-explicit"})
    assert payload["client_metadata"]["session_id"] == explicit_headers["session-id"]
    assert payload["client_metadata"]["x-codex-turn-metadata"] == explicit_headers["x-codex-turn-metadata"]
    assert payload["parallel_tool_calls"] is True


def test_protocol_payload_passthrough_responses_raw_body():
    provider = _provider("responses", "opencode")
    raw_body = {
        "model": "client-model",
        "input": "hi",
        "stream": True,
    }

    payload = provider._build_protocol_payload(
        "responses",
        "upstream-model",
        [{"role": "user", "content": "ignored"}],
        False,
        _raw_responses_body=raw_body,
    )

    assert payload["model"] == "upstream-model"
    assert payload["stream"] is False
    assert payload["input"] == "hi"
    # Protocol-level Responses defaults are always present in outbound payloads
    assert payload["text"] == {"verbosity": "low"}
    assert payload["store"] is False
    assert payload["include"] == ["reasoning.encrypted_content"]
    # 非模拟客户端不再注入默认 reasoning；opencode 无 responses 客户端默认
    assert "reasoning" not in payload
    # 无有效 tools 时不再发送 tool_choice（上游校验：'tool_choice' is only allowed when 'tools' are specified）
    assert "tool_choice" not in payload
    assert payload["parallel_tool_calls"] is True


def test_simulated_client_defaults_are_configurable(monkeypatch):
    monkeypatch.setattr(
        "providers.custom.Config.get_simulated_client_defaults",
        lambda: {"reasoning_effort": "high", "auto_search": True},
    )
    provider = _provider("responses", "opencode")

    payload = provider._build_protocol_payload(
        "responses",
        "upstream-model",
        [{"role": "user", "content": "ignored"}],
        False,
        _raw_responses_body={"model": "client-model", "input": "hi", "stream": False},
    )

    assert payload["reasoning"] == {"effort": "high"}
    assert payload["auto_search"] is True


def test_codex_preset_applies_defaults_for_responses_passthrough_raw_body(monkeypatch):
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("responses", "codex-cli")

    payload = provider._build_protocol_payload(
        "responses",
        "upstream-model",
        [{"role": "user", "content": "ignored"}],
        True,
        _raw_responses_body={"model": "client-model", "input": "hi", "stream": False},
        request_id="req-passthrough",
    )

    assert payload["model"] == "upstream-model"
    assert payload["stream"] is True
    assert payload["input"] == "hi"
    assert payload["text"] == {"verbosity": "low"}
    assert payload["store"] is False
    assert payload["include"] == ["reasoning.encrypted_content"]
    assert payload["reasoning"] == {"effort": "medium"}
    # 无有效 tools 时不再发送 tool_choice（上游校验：'tool_choice' is only allowed when 'tools' are specified）
    assert "tool_choice" not in payload
    headers = provider._headers("responses", kwargs={"request_id": "req-passthrough"})
    assert payload["client_metadata"]["x-codex-installation-id"] == CustomProvider.CODEX_INSTALLATION_ID
    assert payload["client_metadata"]["x-codex-turn-metadata"] == headers["x-codex-turn-metadata"]
    assert payload["prompt_cache_key"] == headers["thread-id"]
    assert payload["parallel_tool_calls"] is True


def test_codex_passthrough_reuses_inbound_client_metadata_identity(monkeypatch):
    """同协议直通：入站 codex body 自带 client_metadata 时，出站 header 照抄同一份身份。

    不照抄会让 header 的 session/turn 与 body 里客户端原有的那份错位——真实客户端
    两处逐字段同源，错位即可判别。
    """
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("responses", "codex-tui")
    inbound_metadata = '{"installation_id":"inst-x","session_id":"01a0627a-3ef1-72f2-8900-38c31be2f087","thread_id":"01a0627a-3ef1-72f2-8900-38c31be2f087","turn_id":"01a0627a-572b-7a13-ae99-dade6120a22c","window_id":"01a0627a-3ef1-72f2-8900-38c31be2f087:0","request_kind":"turn","thread_source":"user","sandbox":"windows_elevated","turn_started_at_unix_ms":1788358580101}'
    raw_body = {
        "model": "client-model",
        "input": "hi",
        "stream": True,
        "client_metadata": {
            "session_id": "01a0627a-3ef1-72f2-8900-38c31be2f087",
            "thread_id": "01a0627a-3ef1-72f2-8900-38c31be2f087",
            "turn_id": "01a0627a-572b-7a13-ae99-dade6120a22c",
            "x-codex-installation-id": "inst-x",
            "x-codex-turn-metadata": inbound_metadata,
            "x-codex-window-id": "01a0627a-3ef1-72f2-8900-38c31be2f087:0",
        },
    }
    kwargs = {"request_id": "req-inbound-cm", "_raw_responses_body": raw_body}

    payload = provider._build_protocol_payload(
        "responses",
        "upstream-model",
        [{"role": "user", "content": "ignored"}],
        True,
        **kwargs,
    )
    headers = provider._headers("responses", kwargs=kwargs)

    # 客户端原有 client_metadata 原样保留（deep merge 只补缺失键）。
    assert payload["client_metadata"] == raw_body["client_metadata"]
    # header 沿用入站身份，含原样的 turn-metadata 串。
    assert headers["session-id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087"
    assert headers["thread-id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087"
    assert headers["x-codex-window-id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087:0"
    assert headers["x-codex-turn-metadata"] == inbound_metadata


def test_claude_code_preset_body_defaults_are_added_for_anthropic_conversion():
    provider = _provider("anthropic", "claude-code")

    payload = provider._build_protocol_payload(
        "anthropic",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        False,
    )

    assert payload["thinking"] == {"type": "adaptive"}
    assert payload["max_tokens"] == 64000
    assert payload["stop_sequences"] == ["</block>"]
    assert payload["context_management"] == {"edits": []}
    assert "session_id" in payload["metadata"]["user_id"]


def test_opencode_preset_body_defaults_are_added_for_chat_conversion():
    provider = _provider("openai", "opencode")

    payload = provider._build_protocol_payload(
        "chat",
        "MiniMax-M3",
        [{"role": "user", "content": "hi"}],
        True,
    )

    assert payload["max_tokens"] == 32000
    assert payload["stream_options"] == {"include_usage": True}


def test_workbuddy_streaming_payload_drops_stream_options_to_match_client(monkeypatch):
    """WorkBuddy 客户端流式请求不带 stream_options（抓包确认），出站 body 保持一致。

    网关默认会给流式 OpenAI 请求注入 stream_options:{include_usage:true} 用于计费；
    WorkBuddy 伪装需对齐真实客户端，将其去掉，顶层只剩 model/messages/stream/tools。
    """
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    payload = provider._build_openai_payload(
        "opus",
        [{"role": "user", "content": "hi"}],
        True,
        stream_options={"include_usage": True},
        tools=[{"type": "function", "function": {"name": "foo", "parameters": {}}}],
    )

    assert payload["model"] == "opus"
    assert payload["stream"] is True
    assert "stream_options" not in payload
    assert "store" not in payload
    assert "reasoning" not in payload
    assert "client_metadata" not in payload


def test_workbuddy_prepends_powered_by_system_when_missing(monkeypatch):
    """无 system 时，WorkBuddy 在 messages 首插入 "This conversation is powered by {model}." 声明句。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [{"role": "user", "content": "hi"}],
        True,
    )

    assert payload["messages"][0] == {
        "role": "system",
        "content": "This conversation is powered by gpt-5.6-sol. Follow the user's instructions.",
    }
    assert payload["messages"][1] == {"role": "user", "content": "hi"}


def test_workbuddy_prepends_powered_by_when_existing_system_lacks_marker(monkeypatch):
    """已有 system 但首句不是声明句时，把声明句前置到该 system 内容之前，保留原内容。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "hi"},
        ],
        True,
    )

    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][0]["content"] == (
        "This conversation is powered by gpt-5.6-sol. Follow the user's instructions.\n\nBe concise."
    )
    assert payload["messages"][1] == {"role": "user", "content": "hi"}


def test_workbuddy_keeps_existing_matching_powered_by_system(monkeypatch):
    """首句已是本次出站 model 的声明句时不重复注入。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    marker = "This conversation is powered by gpt-5.6-sol. Follow the user's instructions."
    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [
            {"role": "system", "content": marker},
            {"role": "user", "content": "hi"},
        ],
        True,
    )

    systems = [m for m in payload["messages"] if m.get("role") == "system"]
    assert len(systems) == 1
    assert systems[0]["content"] == marker


def test_workbuddy_rewrites_stale_powered_by_line_for_different_model(monkeypatch):
    """首句声明了别的 model 时，原地改成本次出站 model，不叠加两句矛盾声明。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [
            {"role": "system", "content": "This conversation is powered by old-model. Follow the user's instructions.\n\nBe nice."},
            {"role": "user", "content": "hi"},
        ],
        True,
    )

    assert payload["messages"][0]["content"] == (
        "This conversation is powered by gpt-5.6-sol. Follow the user's instructions.\n\nBe nice."
    )
    systems = [m for m in payload["messages"] if m.get("role") == "system"]
    assert len(systems) == 1


def test_workbuddy_injects_even_when_inbound_client_is_workbuddy(monkeypatch):
    """入站已是 workbuddy 客户端但声明句缺失/声明了别的 model 时仍要补齐。

    通用的「同类型客户端直通」短路会跳过注入，但 WorkBuddy 的声明句必须声明本次
    出站 model（网关侧可能做过模型映射/重命名），故这里不能被短路。
    """
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [{"role": "user", "content": "hi"}],
        True,
        client_type="workbuddy",
    )

    assert payload["messages"][0] == {
        "role": "system",
        "content": "This conversation is powered by gpt-5.6-sol. Follow the user's instructions.",
    }


def test_workbuddy_marker_must_be_first_sentence_not_merely_present(monkeypatch):
    """声明句出现在 system 中间不算就绪：必须位于首句，否则前置补齐。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    marker = "This conversation is powered by gpt-5.6-sol. Follow the user's instructions."
    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [
            {"role": "system", "content": f"Be concise.\n\n{marker}"},
            {"role": "user", "content": "hi"},
        ],
        True,
    )

    assert payload["messages"][0]["content"].startswith(marker)


def test_workbuddy_cross_protocol_prepends_powered_by_system_when_missing(monkeypatch):
    """协议转换到 OpenAI 时，仍按出站 model 补 WorkBuddy 首句声明。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")

    payload = provider._build_openai_payload(
        "gpt-5.6-sol",
        [{"role": "user", "content": "hi"}],
        True,
        _from_anthropic=True,
    )

    assert payload["messages"][0] == {
        "role": "system",
        "content": "This conversation is powered by gpt-5.6-sol. Follow the user's instructions.",
    }
    assert payload["messages"][1] == {"role": "user", "content": "hi"}


def test_workbuddy_cross_protocol_keeps_matching_and_rewrites_stale_model(monkeypatch):
    """协议转换时，同 model 不重复注入，model 不一致则替换原声明。"""
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("openai", "workbuddy")
    marker = "This conversation is powered by gpt-5.6-sol. Follow the user's instructions."

    matching = provider._build_openai_payload(
        "gpt-5.6-sol",
        [
            {"role": "system", "content": marker},
            {"role": "user", "content": "hi"},
        ],
        True,
        _from_responses=True,
    )
    stale = provider._build_openai_payload(
        "gpt-5.6-sol",
        [
            {
                "role": "system",
                "content": "This conversation is powered by old-model. Follow the user's instructions.\n\nBe concise.",
            },
            {"role": "user", "content": "hi"},
        ],
        True,
        _from_responses=True,
    )

    assert matching["messages"][0]["content"] == marker
    assert sum(message.get("content") == marker for message in matching["messages"]) == 1
    assert stale["messages"][0]["content"] == f"{marker}\n\nBe concise."


def test_openai_payload_preserves_extended_client_fields():
    provider = _provider("openai", "none")

    payload = provider._build_openai_payload(
        "m",
        [{"role": "user", "content": "hi"}],
        False,
        repetition_penalty=1.1,
        min_p=0.1,
        top_a=0.2,
        stream_options={"include_usage": True},
        prompt_cache_key="cache-key",
        client_metadata={"client": "codex"},
    )

    assert payload["repetition_penalty"] == 1.1
    assert payload["min_p"] == 0.1
    assert payload["top_a"] == 0.2
    assert payload["stream_options"] == {"include_usage": True}
    assert payload["prompt_cache_key"] == "cache-key"
    assert payload["client_metadata"] == {"client": "codex"}


def test_custom_provider_chat_protocols_filter_by_model_and_protocol():
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="openai",
        base_url="https://example.invalid",
        chat_protocols=[
            {"id": "openai-chat", "protocol": "openai", "path": "/openai", "models": ["m1"], "upstream_stream": True},
            {"id": "anthropic-chat", "protocol": "anthropic", "path": "/anthropic", "models": ["m1"], "upstream_stream": False},
        ],
        image_path="/images",
    )

    # 两行都勾选了 m1：请求 anthropic 时同协议在前，但两者都是 model-bound。
    chat_candidates = provider.get_chat_protocol_candidates("m1", "anthropic")
    assert [c["id"] for c in chat_candidates] == ["anthropic-chat", "openai-chat"]

    assert provider.get_chat_protocol_candidates("m2", "openai") == []
    assert provider._image_url({}) == "https://example.invalid/images"


def test_model_bound_row_wins_over_protocol_passthrough():
    """勾选了该模型的协议行优先于协议一致但未勾选模型的直通行。"""
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="openai",
        base_url="https://example.invalid",
        chat_protocols=[
            # 直通：协议与请求一致，但未勾选任何模型（服务全部模型）。
            {"id": "responses-passthrough", "protocol": "responses", "path": "/passthrough", "models": []},
            # 绑定：勾选了 m1，但协议与请求不一致。
            {"id": "anthropic-bound", "protocol": "anthropic", "path": "/bound", "models": ["m1"]},
        ],
    )

    candidates = provider.get_chat_protocol_candidates("m1", "responses")

    # 勾选模型的行必须排在协议直通行之前。
    assert [c["id"] for c in candidates] == ["anthropic-bound", "responses-passthrough"]
    assert provider._select_chat_protocol("m1", "responses")["id"] == "anthropic-bound"


def test_model_bound_and_same_protocol_ranks_first():
    """同一模型既有同协议绑定行也有异协议绑定行时，同协议的绑定行最优。"""
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="openai",
        base_url="https://example.invalid",
        chat_protocols=[
            {"id": "anthropic-bound", "protocol": "anthropic", "path": "/a", "models": ["m1"]},
            {"id": "responses-bound", "protocol": "responses", "path": "/r", "models": ["m1"]},
            {"id": "responses-passthrough", "protocol": "responses", "path": "/p", "models": []},
        ],
    )

    candidates = provider.get_chat_protocol_candidates("m1", "responses")

    assert [c["id"] for c in candidates] == [
        "responses-bound",       # 绑定 + 同协议
        "anthropic-bound",       # 绑定 + 异协议
        "responses-passthrough", # 未绑定 + 同协议
    ]


def test_custom_provider_chat_protocol_overrides_chat_path_and_header_template(monkeypatch):
    provider = CustomProvider(
        "u",
        "sk-test",
        protocol="openai",
        base_url="https://example.invalid",
        chat_protocols=[
            {"id": "openai-chat", "protocol": "openai", "path": "/chat-alt", "header_template": "tpl-1"},
        ],
        image_path="/image-alt",
    )
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {"tpl-1": {"X-Test": "1"}})

    chat_config = provider.get_chat_protocol_candidates("m", "openai")[0]

    assert provider._chat_url({"_endpoint_config": chat_config}) == "https://example.invalid/chat-alt"
    assert provider._image_url({"_endpoint_config": chat_config}) == "https://example.invalid/image-alt"
    assert provider._headers("openai", kwargs={"_endpoint_config": chat_config})["X-Test"] == "1"


def test_client_preset_changes_payload_protocol_but_never_selected_path(monkeypatch):
    monkeypatch.setattr(
        "providers.custom.Config.get_simulated_client_defaults",
        lambda: {"reasoning_effort": "medium", "auto_search": False},
    )
    fixed_path = "/vendor/fixed-path"
    for row_protocol, preset, expected_payload_key in (
        ("anthropic", "codex-cli", "input"),
        ("responses", "claude-code", "messages"),
    ):
        endpoint_config = {
            "id": f"{row_protocol}-{preset}",
            "protocol": row_protocol,
            "path": fixed_path,
            "client_preset": preset,
        }
        provider = CustomProvider(
            "u",
            "sk-test",
            base_url="https://example.invalid",
            chat_protocols=[endpoint_config],
        )

        payload = provider._build_protocol_payload(
            row_protocol,
            "m",
            [{"role": "user", "content": "hi"}],
            False,
        )

        assert expected_payload_key in payload
        assert provider._chat_url({"_endpoint_config": endpoint_config}) == f"https://example.invalid{fixed_path}"


    provider = _provider("anthropic", "claude-code")
    raw_body = {
        "model": "client-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": True,
    }

    payload = provider._build_anthropic_payload(
        "upstream-model",
        [{"role": "user", "content": "ignored"}],
        False,
        _raw_anthropic_body=raw_body,
    )

    assert payload["model"] == "upstream-model"
    assert payload["stream"] is False
    assert payload["messages"] == [{"role": "user", "content": "hi"}]
    assert "thinking" not in payload
    assert "stop_sequences" not in payload


def test_claude_code_preset_headers_match_captured_client():
    provider = _provider("anthropic", "claude-code")

    headers = provider._headers()

    assert headers["x-app"] == "cli"
    assert headers["User-Agent"] == "claude-cli/2.1.168 (external, cli)"
    assert headers["anthropic-beta"] == "context-1m-2025-08-07"
    assert headers["x-stainless-os"] == "Windows"
    assert headers["x-stainless-lang"] == "js"
    assert headers["x-stainless-runtime"] == "node"
    assert headers["x-stainless-package-version"] == "0.94.0"
    assert headers["anthropic-dangerous-direct-browser-access"] == "true"


def test_responses_default_headers_match_codex_client():
    provider = _provider("responses")

    headers = provider._headers()
    turn_metadata = json.loads(headers["x-codex-turn-metadata"])

    assert headers["originator"] == "Codex Desktop"
    assert headers["User-Agent"] == "Codex Desktop/0.137.0-alpha.4 (Windows 10.0.26200; x86_64) unknown (Codex Desktop; 26.602.40724)"
    assert headers["x-codex-beta-features"] == "terminal_resize_reflow"
    # 会话身份每请求现造：thread/session/window/x-client-request-id 同源，window 带 :0 后缀。
    session_id = headers["session-id"]
    assert headers["thread-id"] == session_id
    assert headers["x-client-request-id"] == session_id
    assert headers["x-codex-window-id"] == f"{session_id}:0"
    assert turn_metadata["session_id"] == session_id
    assert turn_metadata["thread_id"] == headers["thread-id"]
    assert turn_metadata["window_id"] == headers["x-codex-window-id"]
    # Desktop 变体的 turn metadata 字段集：带 workspace_kind，不带 installation_id。
    assert turn_metadata["workspace_kind"] == "projectless"
    assert "installation_id" not in turn_metadata


def test_codex_tui_preset_headers_match_captured_client():
    """codex-tui 0.146.0 抓包：originator/UA/beta-features/accept-encoding 与真实客户端一致。"""
    provider = _provider("responses", "codex-tui")

    headers = provider._headers()
    turn_metadata = json.loads(headers["x-codex-turn-metadata"])

    assert headers["originator"] == "codex-tui"
    assert headers["User-Agent"] == "codex-tui/0.146.0 (Windows 10.0.26200; x86_64) unknown (codex-tui; 0.146.0)"
    assert headers["x-codex-beta-features"] == "remote_compaction_v2"
    assert headers["accept-encoding"] == "identity"
    # tui 变体的 turn metadata：带 installation_id，不带 workspace_kind。
    assert turn_metadata["installation_id"] == "4de163e5-08f4-442c-9698-fc57a64ffbd7"
    assert "workspace_kind" not in turn_metadata
    assert turn_metadata["request_kind"] == "turn"
    assert turn_metadata["thread_source"] == "user"


def test_codex_session_identity_is_generated_per_request():
    """不同请求（不同 request_id）拿到不同的 uuid7 会话/轮次身份，而非全站共用常量。"""
    provider = _provider("responses", "codex-tui")

    first = provider._headers("responses", kwargs={"request_id": "req-1"})
    second = provider._headers("responses", kwargs={"request_id": "req-2"})

    assert first["session-id"] != second["session-id"]
    # uuid7：version 位为 7，前 48bit 为毫秒时间戳（不是 uuid4 的全随机）。
    assert uuid.UUID(first["session-id"]).version == 7
    assert json.loads(first["x-codex-turn-metadata"])["turn_id"] != json.loads(second["x-codex-turn-metadata"])["turn_id"]


def test_codex_preset_headers_declare_sse_accept_for_streaming_channel():
    """codex-tui 抓包带 accept: text/event-stream；非流式渠道不声明，避免自相矛盾。

    accept 只加在有抓包依据的 codex-tui 上；Desktop 变体无样本，保持原样不加。
    """
    provider = _provider("responses", "codex-tui")

    assert provider._headers("responses")["accept"] == "text/event-stream"
    assert "accept" not in _provider("responses", "codex-cli")._headers("responses")

    non_stream = CustomProvider(
        "u",
        "sk-test",
        protocol="responses",
        base_url="https://example.invalid",
        client_preset="codex-tui",
        upstream_stream=False,
    )

    assert "accept" not in non_stream._headers("responses")


def test_codex_identity_is_stable_within_one_request():
    """同一 request_id（含渠道内层重试）复用同一 turn，与真实客户端重发行为一致。"""
    provider = _provider("responses", "codex-tui")
    kwargs = {"request_id": "req-same"}

    first = provider._headers("responses", kwargs=kwargs)
    second = provider._headers("responses", kwargs=kwargs)

    assert first["x-codex-turn-metadata"] == second["x-codex-turn-metadata"]
    assert first["session-id"] == second["session-id"]


def test_codex_body_and_header_identity_are_same_source(monkeypatch):
    """body 的 client_metadata / prompt_cache_key 必须与 header 的会话身份逐字段一致。

    真实客户端 header 的 x-codex-turn-metadata 与 body client_metadata 里那串 JSON
    完全相同；两处不一致（或写死常量）就是可判别的伪装特征。
    """
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("responses", "codex-tui")
    kwargs = {"request_id": "req-identity"}

    headers = provider._headers("responses", kwargs=kwargs)
    payload = provider._build_protocol_payload(
        "responses",
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        True,
        **kwargs,
    )

    metadata = payload["client_metadata"]
    assert metadata["session_id"] == headers["session-id"]
    assert metadata["thread_id"] == headers["thread-id"]
    assert metadata["x-codex-window-id"] == headers["x-codex-window-id"]
    assert metadata["x-codex-turn-metadata"] == headers["x-codex-turn-metadata"]
    assert metadata["turn_id"] == json.loads(headers["x-codex-turn-metadata"])["turn_id"]
    # prompt_cache_key 就是 thread id（真实客户端行为），不是全站共用常量。
    assert payload["prompt_cache_key"] == headers["thread-id"]


def test_codex_identity_follows_inbound_session_when_client_is_codex():
    """入站已是 codex 客户端（同协议直通）时沿用其 session id，不另起会话。"""
    provider = _provider("responses", "codex-tui")

    headers = provider._headers("responses", kwargs={"request_id": "req-inbound", "client_session_id": "01a0627a-3ef1-72f2-8900-38c31be2f087"})

    assert headers["session-id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087"
    assert headers["thread-id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087"
    assert headers["x-codex-window-id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087:0"
    assert json.loads(headers["x-codex-turn-metadata"])["session_id"] == "01a0627a-3ef1-72f2-8900-38c31be2f087"


def test_codex_instructions_default_is_full_captured_prompt():
    """伪装 codex 时回填的 instructions 是真实抓包全文，不是三句缩写。"""
    from providers.client_instructions import CODEX_TUI_INSTRUCTIONS

    assert CustomProvider.CODEX_DEFAULT_INSTRUCTIONS == CODEX_TUI_INSTRUCTIONS
    assert CustomProvider.CODEX_DEFAULT_INSTRUCTIONS.startswith(CustomProvider.CODEX_INSTRUCTIONS_PREFIX[:40])
    # 真实 instructions 含 Personality / Formatting rules 等段落，长度远超身份首句。
    assert "# Personality" in CustomProvider.CODEX_DEFAULT_INSTRUCTIONS
    assert "## Formatting rules" in CustomProvider.CODEX_DEFAULT_INSTRUCTIONS
    assert len(CustomProvider.CODEX_DEFAULT_INSTRUCTIONS) > 5000


def test_openai_default_headers_match_opencode_client():
    provider = _provider("openai")

    headers = provider._headers()

    assert headers["User-Agent"] == "opencode/1.16.2 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
    assert headers["x-session-affinity"] == "ses_15d3045e0ffeMENYCSJKGFbI37"


def test_context_management_body_does_not_add_beta_flag():
    provider = _provider("anthropic", "claude-code")

    headers = provider._anthropic_headers({"context_management": {"edits": []}})

    assert "context-management-2025-06-27" not in headers["anthropic-beta"]


def test_enable_1m_context_adds_beta_header_without_preset():
    provider = _provider("openai")

    headers = provider._headers("openai", kwargs={"enable_1m_context": True})

    assert headers["anthropic-beta"] == "context-1m-2025-08-07"


def test_enable_1m_context_is_deduped_with_claude_code_preset():
    provider = _provider("anthropic", "claude-code")

    headers = provider._anthropic_headers({}, {"enable_1m_context": True})

    assert headers["anthropic-beta"].split(",").count("context-1m-2025-08-07") == 1
