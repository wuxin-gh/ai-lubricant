import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from providers.custom import CustomProvider


def _provider(preset: str = "none") -> CustomProvider:
    return CustomProvider(
        "user",
        "key",
        provider_name="test",
        base_url="https://example.test",
        chat_protocols=[{"id": "test-openai", "protocol": "openai", "path": "", "client_preset": preset}],
    )


def test_custom_provider_headers_maps_claude_code_session_id():
    provider = _provider("claude-code")
    headers = provider._headers({"client_session_id": "s123"})
    assert headers["x-claude-code-session-id"] == "s123"
    assert "x-client-request-id" not in headers


def test_custom_provider_headers_maps_codex_cli_session_id():
    """入站会话 id 会带动 codex 的整组身份（thread/window/turn-metadata 同源）。"""
    provider = _provider("codex-cli")
    headers = provider._headers({"client_session_id": "s456", "request_id": "req-codex-session"})
    assert headers["session-id"] == "s456"
    assert headers["thread-id"] == "s456"
    assert headers["x-codex-window-id"] == "s456:0"
    assert headers["x-client-request-id"] == "s456"
    assert '"session_id":"s456"' in headers["x-codex-turn-metadata"]


def test_custom_provider_headers_maps_opencode_session_affinity():
    provider = _provider("opencode")
    headers = provider._headers({"client_session_id": "s789"})
    assert headers["x-session-affinity"] == "s789"


def test_custom_provider_headers_defaults_to_x_session_id():
    provider = _provider("other")
    headers = provider._headers({"client_session_id": "s000"})
    assert headers["x-session-id"] == "s000"


def test_custom_provider_headers_only_sets_client_request_id_when_explicitly_provided():
    provider = CustomProvider("user", "key", provider_name="test", base_url="https://example.test")
    headers = provider._headers({"client_request_id": "req_client_123"})
    assert headers["x-client-request-id"] == "req_client_123"


def test_header_template_renders_static_and_request_variables(monkeypatch):
    provider = CustomProvider(
        "user",
        "key",
        provider_name="test",
        base_url="https://example.test",
        chat_protocols=[{
            "id": "test-openai",
            "protocol": "openai",
            "path": "",
            "header_template": "cline-cli",
        }],
    )
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "cline-cli": {
            "x-client-type": "cline-cli",
            "x-template-uuid": "{{uuid}}",
            "x-template-time": "{{time}}",
            "x-template-timestamp": "{{timestamp}}",
            "x-template-timestamp-ms": "{{timestamp_ms}}",
            "x-template-session": "{{client_session_id}}",
            "x-template-request": "{{client_request_id}}",
            "x-template-unknown": "prefix-{{unknown}}",
        },
    })
    kwargs = {
        "_endpoint_config": provider.get_chat_protocol_candidates("m", "openai")[0],
        "client_session_id": "session-1",
        "client_request_id": "request-1",
    }

    first = provider._headers("openai", kwargs=kwargs)
    second = provider._headers("openai", kwargs=kwargs)

    assert first["x-client-type"] == "cline-cli"
    assert uuid.UUID(first["x-template-uuid"]).version == 4
    assert first["x-template-uuid"] != second["x-template-uuid"]
    assert first["x-template-time"].endswith("Z")
    assert first["x-template-timestamp"].isdigit()
    assert first["x-template-timestamp-ms"].isdigit()
    assert first["x-template-session"] == "session-1"
    assert first["x-template-request"] == "request-1"
    assert first["x-template-unknown"] == "prefix-{{unknown}}"


def test_custom_provider_headers_does_not_use_internal_request_id_as_client_request_id():
    provider = CustomProvider("user", "key", provider_name="test", base_url="https://example.test")
    headers = provider._headers({"request_id": "req_internal_456"})
    assert "x-client-request-id" not in headers, "internal request_id should not be used as x-client-request-id"


def test_set_header_template_cache_updates_sync_header_view():
    """admin 保存后的模板列表应直接成为同步出站 Header 的内存视图。"""
    from rate_limiter import ModelClientPool

    ModelClientPool.set_header_template_cache([
        {"id": "tpl-1", "name": "T1", "headers": {"x-client-type": "cline-cli"}},
    ])

    assert ModelClientPool._header_templates == {"tpl-1": {"x-client-type": "cline-cli"}}
    assert ModelClientPool._header_templates_loaded is True


@pytest.mark.asyncio
async def test_header_template_pubsub_handler_reloads_other_instance_cache(monkeypatch):
    from db import PostgresClient
    from rate_limiter import ModelClientPool
    from runtime_handlers import _on_header_templates_event

    async def fake_get_header_templates():
        return [{"id": "tpl-2", "name": "T2", "headers": {"x-client-type": "other-cli"}}]

    monkeypatch.setattr(PostgresClient, "get_header_templates", classmethod(lambda cls: fake_get_header_templates()))
    ModelClientPool.set_header_template_cache([])

    await _on_header_templates_event("__default__", {})

    assert ModelClientPool._header_templates == {"tpl-2": {"x-client-type": "other-cli"}}


@pytest.mark.asyncio
async def test_update_header_templates_publishes_runtime_sync_event(monkeypatch):
    import admin

    published = []

    async def fake_require_admin(token):
        return None

    async def fake_set_header_templates(templates):
        return templates

    async def fake_publish(event_type, name=None, version=None, extra=None):
        published.append((event_type, name))

    async def fake_log(*args, **kwargs):
        return None

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin.PostgresClient, "set_header_templates", classmethod(lambda cls, templates: fake_set_header_templates(templates)))
    monkeypatch.setattr(admin.runtime_sync, "publish", fake_publish)
    monkeypatch.setattr(admin, "_log_operation", fake_log)

    result = await admin.update_header_templates({"templates": [{"id": "tpl-3", "name": "T3", "headers": {"x": "y"}}]}, "token")

    assert result["templates"] == [{"id": "tpl-3", "name": "T3", "headers": {"x": "y"}}]
    assert published == [(admin.runtime_sync.EVENT_HEADER_TEMPLATES, "__default__")]


def _provider_with_protocol(protocol: str) -> CustomProvider:
    return CustomProvider(
        "user",
        "sk-test",
        provider_name="test",
        base_url="https://example.test",
        chat_protocols=[{"id": f"test-{protocol}", "protocol": protocol, "path": ""}],
    )


def test_auth_header_follows_upstream_protocol_not_build_endpoint():
    """认证头按真实上游协议（活跃协议行 / self.protocol），不随 _headers 的 endpoint 参数走。

    各 chat 方法传给 _headers 的 endpoint 是「payload 构造 / 伪装客户端」协议，
    可能被 client_preset 解析成别的协议；真实上游鉴权方式与之无关。
    """
    # OpenAI 上游：即便被要求按 anthropic 形态构造 payload，也只发 Bearer，不发 x-api-key。
    provider = _provider_with_protocol("openai")
    headers = provider._headers("anthropic", apply_preset=False)
    assert headers["Authorization"] == "Bearer sk-test"
    assert "x-api-key" not in headers

    # Anthropic 上游：只发 x-api-key + anthropic-version，即便被要求按 openai 形态构造。
    provider = _provider_with_protocol("anthropic")
    headers = provider._headers("openai", apply_preset=False)
    assert headers["x-api-key"] == "sk-test"
    assert headers["anthropic-version"] == "2023-06-01"
    assert "Authorization" not in headers


def test_auth_header_uses_active_chat_protocol_row_for_cross_protocol_dispatch():
    """反向多协议：主协议 anthropic 的渠道路由到 responses 协议行出站。

    活跃协议行（_endpoint_config）protocol=responses → 认证走 Bearer，与出站
    URL(/v1/responses) 一致，不会把 x-api-key 发给只认 Bearer 的 responses 端点。
    """
    provider = CustomProvider(
        "user",
        "sk-test",
        provider_name="test",
        base_url="https://example.test",
        chat_protocols=[
            {"id": "row-anthropic", "protocol": "anthropic", "path": "/v1/messages"},
            {"id": "row-responses", "protocol": "responses", "path": "/v1/responses"},
        ],
    )
    # 无活跃行时用主协议（第一条：anthropic）。
    base = provider._headers("responses", apply_preset=False)
    assert base["x-api-key"] == "sk-test"

    # 路由选中 responses 协议行 → 认证随活跃行走 Bearer。
    routed = provider._headers(
        "responses",
        apply_preset=False,
        kwargs={"_endpoint_config": {"protocol": "responses", "path": "/v1/responses"}},
    )
    assert routed["Authorization"] == "Bearer sk-test"
    assert "x-api-key" not in routed




def test_custom_provider_detects_restricted_client_message_response():
    body = {
        "choices": [{
            "message": {"content": "Access Denied: This service is restricted to authorized use through the official Claude Code client only."},
            "finish_reason": "stop",
        }]
    }
    assert CustomProvider._is_restricted_client_response(body) is True


def test_custom_provider_does_not_treat_normal_content_as_restricted():
    body = {"choices": [{"message": {"content": "Access granted."}, "finish_reason": "stop"}]}
    assert CustomProvider._is_restricted_client_response(body) is False


def test_workbuddy_preset_headers_match_captured_client():
    provider = _provider("workbuddy")
    headers = provider._headers()

    # 静态结构性头（来自 CLIENT_PRESETS，与真实样本对齐）。
    assert headers["User-Agent"] == "WorkBuddy/5.2.5 WorkBuddy/5.2.5 CLI/2.106.4"
    assert headers["x-codebuddy-request"] == "1"
    assert headers["x-ide-name"] == "WorkBuddy"
    assert headers["x-ide-type"] == "WorkBuddy"
    assert headers["x-ide-version"] == "5.2.5"
    assert headers["x-domain"] == "www.codebuddy.cn"
    assert headers["x-product"] == "SaaS"
    assert headers["x-agent-intent"] == "craft"
    assert headers["x-agent-purpose"] == "conversation_topic"
    assert headers["x-stainless-os"] == "Windows"
    assert headers["x-stainless-arch"] == "x64"
    assert headers["x-stainless-lang"] == "js"
    assert headers["x-stainless-runtime"] == "node"
    assert headers["x-stainless-runtime-version"] == "v22.21.1"
    assert headers["x-stainless-package-version"] == "6.25.0"
    assert headers["x-stainless-retry-count"] == "0"
    # WorkBuddy 使用同一个账号密钥同时发送 Bearer 与 x-api-key。
    assert headers["Authorization"] == "Bearer key"
    assert headers["x-api-key"] == "key"
    # CustomProvider 没有独立 WorkBuddy user id，需由 Header 模板按需提供。
    assert "x-user-id" not in headers


def test_workbuddy_preset_generates_per_request_trace_headers():
    provider = _provider("workbuddy")
    h1 = provider._headers()
    h2 = provider._headers()

    # 动态 trace/会话类头每请求新生成，两次调用值不同。
    for key in ("x-trace-id", "x-request-id", "x-b3-traceid", "x-b3-spanid",
                "x-b3-parentspanid", "b3", "traceparent",
                "x-conversation-id", "x-conversation-request-id",
                "x-conversation-message-id", "acp-connection-id"):
        assert h1.get(key), key
        assert h2.get(key), key
        assert h1[key] != h2[key], f"{key} should differ per request"

    # ID 格式和字段关联与 WorkBuddy 5.2.5 抓包一致。
    assert len(h1["x-trace-id"]) == 32
    assert len(h1["x-request-id"]) == 32
    assert len(h1["x-conversation-request-id"]) == 32
    int(h1["x-trace-id"], 16)
    int(h1["x-request-id"], 16)
    int(h1["x-conversation-request-id"], 16)
    assert h1["x-request-id"] == h1["x-conversation-message-id"]
    assert h1["x-conversation-request-id"] != h1["x-request-id"]
    assert h1["b3"] == f"{h1['x-b3-traceid']}-{h1['x-b3-spanid']}-1"
    assert h1["traceparent"] == f"00-{h1['x-trace-id']}-{h1['x-b3-spanid']}-01"
    assert h1["x-trace-id"] == h1["x-b3-traceid"]
    assert h1["x-b3-sampled"] == "1"


def test_workbuddy_preset_passes_through_inbound_session_context():
    provider = _provider("workbuddy")
    headers = provider._headers({"client_session_id": "conv-from-client", "client_request_id": "req-from-client"})

    # 同协议直通：入站会话/请求 id 覆盖 conversation id 与共享消息 id。
    assert headers["x-conversation-id"] == "conv-from-client"
    assert headers["x-request-id"] == "req-from-client"
    assert headers["x-conversation-message-id"] == "req-from-client"
    # conversation request id 仍按真实客户端格式独立生成。
    assert len(headers["x-conversation-request-id"]) == 32
    int(headers["x-conversation-request-id"], 16)
    assert headers["x-conversation-request-id"] != "req-from-client"


def test_workbuddy_header_template_can_override_auth_and_add_user_id(monkeypatch):
    provider = CustomProvider(
        "user",
        "key",
        provider_name="test",
        base_url="https://example.test",
        chat_protocols=[{
            "id": "test-workbuddy",
            "protocol": "openai",
            "path": "",
            "client_preset": "workbuddy",
            "header_template": "workbuddy-account",
        }],
    )
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "workbuddy-account": {
            "x-api-key": "template-key",
            "x-user-id": "user-from-template",
        },
    })
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]

    headers = provider._headers("openai", kwargs={"_endpoint_config": endpoint})

    assert headers["Authorization"] == "Bearer key"
    assert headers["x-api-key"] == "template-key"
    assert headers["x-user-id"] == "user-from-template"


def _account_client_stub(username: str = "acct-1", metadata: dict | None = None,
                         provider_name: str = "custom"):
    """轻量账号运行态 stub：模拟主链路注入的 account_client（只暴露模板需要的非敏感字段）。"""
    from types import SimpleNamespace
    return SimpleNamespace(
        username=username,
        metadata=dict(metadata or {}),
        provider=SimpleNamespace(PROVIDER_NAME=provider_name),
        api_key="sk-secret-not-exposed",
    )


def _provider_with_account_template(template_id: str):
    return CustomProvider(
        "user",
        "key",
        provider_name="test",
        base_url="https://example.test",
        chat_protocols=[{
            "id": "test-openai",
            "protocol": "openai",
            "path": "",
            "header_template": template_id,
        }],
    )


def test_header_template_renders_account_username(monkeypatch):
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "acct-ctx": {"x-account": "{{account.username}}"},
    })
    provider = _provider_with_account_template("acct-ctx")
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    kwargs = {
        "_endpoint_config": endpoint,
        "account_client": _account_client_stub(username="billing-acct"),
    }

    headers = provider._headers("openai", kwargs=kwargs)

    assert headers["x-account"] == "billing-acct"


def test_header_template_renders_account_metadata_dot_path(monkeypatch):
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "acct-ctx": {
            "x-org": "{{account.metadata.org_id}}",
            "x-team": "{{account.metadata.team}}",
        },
    })
    provider = _provider_with_account_template("acct-ctx")
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    kwargs = {
        "_endpoint_config": endpoint,
        "account_client": _account_client_stub(metadata={"org_id": "org-42", "team": "infra"}),
    }

    headers = provider._headers("openai", kwargs=kwargs)

    assert headers["x-org"] == "org-42"
    assert headers["x-team"] == "infra"


def test_header_template_missing_metadata_key_renders_empty(monkeypatch):
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "acct-ctx": {"x-missing": "v={{account.metadata.no_such_key}}-end"},
    })
    provider = _provider_with_account_template("acct-ctx")
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    kwargs = {
        "_endpoint_config": endpoint,
        "account_client": _account_client_stub(metadata={"org_id": "org-42"}),
    }

    headers = provider._headers("openai", kwargs=kwargs)

    # 缺值渲染为空串，不报错、不留 {{...}} 残体。
    assert headers["x-missing"] == "v=-end"


def test_header_template_without_account_client_renders_empty(monkeypatch):
    """主链路未注入 account_client（池外/测试）时，账号变量渲染为空串不报错。"""
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "acct-ctx": {"x-account": "{{account.username}}-{{account.metadata.org_id}}"},
    })
    provider = _provider_with_account_template("acct-ctx")
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    kwargs = {"_endpoint_config": endpoint}

    headers = provider._headers("openai", kwargs=kwargs)

    assert headers["x-account"] == "-"


def test_header_template_does_not_expose_account_secret_fields(monkeypatch):
    """密钥类字段不暴露给模板：api_key 取不到，渲染为空，杜绝密钥进上游 header。"""
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "acct-ctx": {
            "x-leak-attempt": "{{account.api_key}}",
            "x-provider-object-leak": "{{account.provider.api_key}}",
        },
    })
    provider = _provider_with_account_template("acct-ctx")
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    kwargs = {
        "_endpoint_config": endpoint,
        "account_client": _account_client_stub(),
    }

    headers = provider._headers("openai", kwargs=kwargs)

    assert headers["x-leak-attempt"] == ""
    # provider 暴露的是名称字符串，非 provider 对象，故 .api_key 路径取不到。
    assert headers["x-provider-object-leak"] == ""


def test_header_template_renders_account_provider_name(monkeypatch):
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "_header_templates", {
        "acct-ctx": {"x-provider": "{{account.provider}}"},
    })
    provider = _provider_with_account_template("acct-ctx")
    endpoint = provider.get_chat_protocol_candidates("m", "openai")[0]
    kwargs = {
        "_endpoint_config": endpoint,
        "account_client": _account_client_stub(provider_name="custom"),
    }

    headers = provider._headers("openai", kwargs=kwargs)

    assert headers["x-provider"] == "custom"

