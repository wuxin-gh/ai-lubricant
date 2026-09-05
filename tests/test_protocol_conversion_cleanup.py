import asyncio

from providers.custom import CustomProvider


def _provider(protocol="openai", preset="none"):
    return CustomProvider(
        "u",
        "sk-test",
        protocol=protocol,
        base_url="https://example.invalid",
        client_preset=preset,
    )


def test_anthropic_to_openai_payload_drops_source_protocol_fields():
    provider = _provider("openai", "opencode")

    payload = provider._build_openai_payload(
        "gpt-5.5",
        [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
        False,
        _from_anthropic=True,
        max_tokens=32000,
        temperature=1,
        metadata={"user_id": "anthropic-user"},
        auto_search=False,
        auto_thinking=False,
        thinking_enabled=False,
        thinking_mode="Fast",
        thinking_format="summary",
        research_mode="normal",
        stream_options={"include_usage": True},
        context_management={"edits": []},
        mcp_servers=[{"name": "mcp"}],
        container={"type": "auto"},
        top_k=20,
    )

    assert payload == {
        "model": "gpt-5.5",
        "messages": [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
        "stream": False,
        "temperature": 1,
        "max_tokens": 32000,
    }


def test_cross_protocol_openai_headers_skip_client_preset():
    provider = _provider("openai", "claude-code")

    assert provider._headers(apply_preset=False) == {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-test",
    }
    assert provider._headers()["User-Agent"] == "claude-cli/2.1.168 (external, cli)"


def test_anthropic_to_responses_payload_drops_non_responses_fields(monkeypatch):
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("responses", "codex-cli")

    payload = provider._build_responses_payload(
        "gpt-5.5",
        [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
        False,
        _from_anthropic=True,
        max_tokens=64,
        temperature=1,
        metadata={"user_id": "u"},
        thinking={"type": "enabled", "budget_tokens": 1024},
        thinking_enabled=True,
        context_management={"edits": []},
        mcp_servers=[{"name": "mcp"}],
        container={"type": "auto"},
        top_k=20,
        stream_options={"include_usage": True},
        include=["reasoning.encrypted_content"],
        reasoning={"effort": "medium"},
        prompt_cache_key="cache-key",
        client_metadata={"client": "codex"},
    )

    assert payload["model"] == "gpt-5.5"
    # codex 伪装 prepend CODEX_DEFAULT_INSTRUCTIONS 到客户端 system 前，原 system 内容跟在后面。
    assert payload["instructions"] == CustomProvider.CODEX_DEFAULT_INSTRUCTIONS + "\n\nsys"
    assert payload["input"] == [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
    assert payload["max_output_tokens"] == 64
    assert payload["temperature"] == 1
    # Incompatible source-protocol fields must be dropped
    for key in (
        "metadata",
        "thinking",
        "thinking_enabled",
        "context_management",
        "mcp_servers",
        "container",
        "top_k",
        "stream_options",
    ):
        assert key not in payload
    # 客户端自带值保留；缺省仅补齐缺失字段
    assert payload["include"] == ["reasoning.encrypted_content"]
    assert payload["reasoning"] == {"effort": "medium"}
    assert payload["prompt_cache_key"] == "cache-key"
    # codex 伪装会注入完整 body identity（session/thread/turn/window + turn-metadata），
    # 这里只校验客户端标识与 installation-id 这两个核心键存在且值正确。
    assert payload["client_metadata"]["client"] == "codex"
    assert payload["client_metadata"]["x-codex-installation-id"] == "4de163e5-08f4-442c-9698-fc57a64ffbd7"
    assert payload["parallel_tool_calls"] is True


def test_openai_to_responses_payload_drops_openai_stream_options(monkeypatch):
    monkeypatch.setattr("providers.custom.Config.get_simulated_client_defaults", lambda: {})
    provider = _provider("responses", "codex-cli")

    payload = provider._build_responses_payload(
        "gpt-5.5",
        [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
        True,
        _from_openai=True,
        max_tokens=64,
        temperature=1,
        stream_options={"include_usage": True},
        include=["reasoning.encrypted_content"],
        reasoning={"effort": "medium"},
        prompt_cache_key="cache-key",
        client_metadata={"client": "codex"},
    )

    assert payload["model"] == "gpt-5.5"
    # codex 伪装 prepend CODEX_DEFAULT_INSTRUCTIONS 到客户端 system 前，原 system 内容跟在后面。
    assert payload["instructions"] == CustomProvider.CODEX_DEFAULT_INSTRUCTIONS + "\n\nsys"
    assert payload["input"] == [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
    assert payload["max_output_tokens"] == 64
    assert payload["temperature"] == 1
    assert payload["stream"] is True
    # stream_options is source-protocol-only, must be dropped
    assert "stream_options" not in payload
    # 客户端自带值保留；缺省仅补齐缺失字段
    assert payload["include"] == ["reasoning.encrypted_content"]
    assert payload["reasoning"] == {"effort": "medium"}
    assert payload["prompt_cache_key"] == "cache-key"
    # codex 伪装会注入完整 body identity（session/thread/turn/window + turn-metadata），
    # 这里只校验客户端标识与 installation-id 这两个核心键存在且值正确。
    assert payload["client_metadata"]["client"] == "codex"
    assert payload["client_metadata"]["x-codex-installation-id"] == "4de163e5-08f4-442c-9698-fc57a64ffbd7"
    assert payload["parallel_tool_calls"] is True


def test_responses_to_openai_payload_drops_responses_only_fields():
    provider = _provider("openai", "opencode")

    payload = provider._build_openai_payload(
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        False,
        _from_responses=True,
        max_tokens=64,
        stream_options={"include_usage": True},
        include=["reasoning.encrypted_content"],
        reasoning={"effort": "medium"},
        prompt_cache_key="cache-key",
        client_metadata={"client": "codex"},
    )

    assert payload == {
        "model": "gpt-5.5",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": False,
        "max_tokens": 64,
    }


def test_responses_passthrough_body_stays_original_except_model_and_stream():
    provider = _provider("responses", "codex-cli")
    raw_body = {
        "model": "client-model",
        "input": "hi",
        "stream": True,
        "include": ["reasoning.encrypted_content"],
        "client_metadata": {"client": "codex"},
    }

    data = dict(raw_body)
    data["model"] = provider._upstream_model_id("upstream-model")
    data["stream"] = False

    assert data == {
        "model": "upstream-model",
        "input": "hi",
        "stream": False,
        "include": ["reasoning.encrypted_content"],
        "client_metadata": {"client": "codex"},
    }


def _drain_chat_anthropic(provider, model_id, messages, stream, **kwargs):
    async def _run():
        chunks = []
        async for chunk in provider.chat_anthropic(model_id, messages, stream=stream, **kwargs):
            chunks.append(chunk)
        return chunks

    return asyncio.run(_run())


def test_chat_anthropic_cross_protocol_nonstream_passes_from_anthropic_flag():
    """anthropic 客户端 → openai 渠道（跨协议）时，chat_anthropic 必须把 _from_anthropic
    透传给 _do_non_stream_chat，否则 _build_openai_payload 会走全量白名单，把 anthropic
    源协议字段（context_management/mcp_servers/container 等）原样泄漏进 /v1/chat/completions。
    """
    provider = _provider("openai", "none")
    captured = {}

    async def _fake_non_stream(model_id, messages, **kwargs):
        captured.update(kwargs)
        return {
            "choices": [{"message": {"role": "assistant", "content": "ok"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }

    provider._do_non_stream_chat = _fake_non_stream

    _drain_chat_anthropic(
        provider,
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        stream=False,
        context_management={"edits": []},
        mcp_servers=[{"name": "mcp"}],
    )

    assert captured.get("_from_anthropic") is True


def test_chat_anthropic_cross_protocol_stream_passes_from_anthropic_flag():
    """流式跨协议路径同样必须下传 _from_anthropic。"""
    provider = _provider("openai", "none")
    captured = {}

    async def _fake_chat(model_id, messages, stream=True, **kwargs):
        captured.update(kwargs)
        yield {}
        yield 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n'
        yield "data: [DONE]\n\n"

    provider.chat = _fake_chat

    _drain_chat_anthropic(
        provider,
        "gpt-5.5",
        [{"role": "user", "content": "hi"}],
        stream=True,
        context_management={"edits": []},
        mcp_servers=[{"name": "mcp"}],
    )

    assert captured.get("_from_anthropic") is True
