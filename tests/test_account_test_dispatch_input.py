"""账号测试构造与下发分离（tidy-petting-pony.md）的回归测试。

锁四件事：
1. `_build_test_dispatch_input` 返回的字段集 = dispatch_entry 需要的全部入参，
   下发调用方原样透传即可，无需二次判断；
2. 模拟客户端时 body 按该客户端目标协议的真实形态构造
   （codex-cli → responses 的 input、claude-code → anthropic 的 messages+max_tokens、
   opencode/codex-openai → openai 的 messages）；
3. extra_kwargs 绝不带 client_preset override —— 真实链路不下发，伪装由渠道配置决定；
4. 自定义测试模板的 messages/body 变量能力与 Headers 配置完全一致（同一份实现）。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

import admin


EXPECTED_KEYS = {
    "operation", "body", "headers", "stream", "request_protocol",
    "chat_method", "provider_whitelist", "account_whitelist", "extra_kwargs",
}


def _build(**over):
    base = dict(
        test_type="chat",
        client_type="none",
        protocol=None,
        channel_protocol="openai",
        model="m1",
        username="u1",
        provider_name="p1",
        data={},
        selected_endpoint_config=None,
    )
    base.update(over)
    return admin._build_test_dispatch_input(**base)

def test_returns_complete_dispatch_struct_for_chat():
    built = _build()
    assert set(built.keys()) == EXPECTED_KEYS
    assert built["operation"] is None
    assert built["stream"] is False
    assert built["chat_method"] == "chat"
    assert built["request_protocol"] == "openai"
    assert built["provider_whitelist"] == {"p1"}
    assert built["account_whitelist"] == {"u1"}
    assert built["extra_kwargs"] == {}
    # 通用测试不带任何客户端头 —— headers 空。
    assert built["headers"] == {}
    assert built["body"]["model"] == "m1"
    assert built["body"]["messages"] and built["body"]["stream"] is False


def test_stream_test_type_sets_stream_true():
    built = _build(test_type="stream")
    assert built["stream"] is True
    assert built["body"]["stream"] is True


def test_chat_method_matches_protocol():
    assert _build(protocol="responses")["chat_method"] == "chat_responses"
    assert _build(protocol="anthropic")["chat_method"] == "chat_anthropic"
    assert _build(protocol="gemini")["chat_method"] == "chat"


def test_protocol_falls_back_to_channel_protocol_when_unset():
    built = _build(protocol=None, channel_protocol="anthropic")
    assert built["request_protocol"] == "anthropic"
    assert built["chat_method"] == "chat_anthropic"


def test_endpoint_config_carried_only_when_set():
    endpoint_cfg = {"protocol": "responses", "path": "/responses", "enabled": True}
    built = _build(selected_endpoint_config=endpoint_cfg)
    assert built["extra_kwargs"] == {"_endpoint_config": endpoint_cfg}


def test_account_test_selects_configured_row_even_for_primary_protocol():
    primary = {"id": "primary", "protocol": "anthropic", "path": "/fixed/messages", "enabled": True}
    secondary = {"id": "secondary", "protocol": "responses", "path": "/fixed/responses", "enabled": True}
    cfg = {"chat_protocols": [primary, secondary]}

    assert admin._select_test_endpoint_config(cfg, "anthropic") is primary
    assert admin._select_test_endpoint_config(cfg, "responses") is secondary


def test_simulated_client_changes_body_protocol_but_not_selected_path():
    endpoint_cfg = {
        "id": "anthropic-custom",
        "protocol": "anthropic",
        "path": "/fixed/messages",
        "enabled": True,
        "client_preset": "codex-cli",
    }

    built = _build(client_type="codex-cli", selected_endpoint_config=endpoint_cfg)

    assert built["request_protocol"] == "responses"
    assert built["chat_method"] == "chat_responses"
    assert built["extra_kwargs"]["_endpoint_config"] is endpoint_cfg
    assert built["extra_kwargs"]["_endpoint_config"]["path"] == "/fixed/messages"


def test_no_client_preset_override_in_extra_kwargs_for_any_branch():
    """与真实链路一致：从不下发 client_preset。模拟与否由渠道 client_preset 配置决定。"""
    for client_type in ("none", "claude-code", "codex-cli", "codex-openai", "opencode", "workbuddy"):
        built = _build(client_type=client_type, selected_endpoint_config={"protocol": "openai"})
        assert "client_preset" not in built["extra_kwargs"], client_type


def test_media_branches_keep_operation_and_minimal_body():
    img = _build(test_type="image")
    assert img["operation"] == "image"
    assert img["body"]["model"] == "m1" and "prompt" in img["body"]
    vid = _build(test_type="video")
    assert vid["operation"] == "video"
    tts = _build(test_type="tts")
    assert tts["operation"] == "tts_generation"
    assert tts["body"]["input"] and tts["body"]["voice"]


def test_sim_client_carries_real_client_headers():
    """模拟客户端时 headers 用 CLIENT_PRESETS 里该客户端的真实头（含 User-Agent）。"""
    from providers.custom import CustomProvider

    for client_type, preset_key in (
        ("claude-code", "claude-code"),
        ("codex-cli", "codex-cli"),
        ("codex-openai", "codex-openai"),
        ("opencode", "opencode"),
        ("workbuddy", "workbuddy"),
    ):
        built = _build(client_type=client_type)
        preset_headers = CustomProvider.CLIENT_PRESETS[preset_key]
        assert built["headers"], client_type
        assert "User-Agent" in built["headers"]
        # 头 COPY 自 CLIENT_PRESETS，不被原地改坏。
        assert built["headers"] == preset_headers


def test_sim_codex_cli_body_is_responses_shape():
    built = _build(client_type="codex-cli")
    assert built["request_protocol"] == "responses"
    assert built["chat_method"] == "chat_responses"
    body = built["body"]
    assert body["model"] == "m1"
    # responses 形态用 input（而非 messages），按真实样本：developer + user 两条。
    inp = body.get("input")
    assert isinstance(inp, list) and len(inp) == 2
    assert inp[0]["role"] == "developer" and inp[1]["role"] == "user"
    assert inp[1]["content"][0]["type"] == "input_text"
    # 真实样本的结构性字段。
    assert body["instructions"] and body["store"] is False
    assert body["include"] == ["reasoning.encrypted_content"]
    assert body["reasoning"] == {"effort": "medium"}
    assert body["tool_choice"] == "auto" and body["parallel_tool_calls"] is True
    assert body["text"] == {"verbosity": "low"}
    assert "prompt_cache_key" in body
    cm = body["client_metadata"]
    assert cm["session_id"] and cm["x-codex-window-id"] and cm["x-codex-installation-id"]
    assert "x-codex-turn-metadata" in cm  # JSON 字符串
    # 不应残留 messages（responses 协议用 input）。
    assert "messages" not in body


def test_sim_claude_code_body_is_anthropic_shape():
    built = _build(client_type="claude-code")
    assert built["request_protocol"] == "anthropic"
    assert built["chat_method"] == "chat_anthropic"
    body = built["body"]
    assert body["model"] == "m1"
    assert isinstance(body.get("messages"), list) and body["messages"]
    # anthropic 真实样本字段。
    assert body["thinking"] == {"type": "adaptive"}
    assert body["max_tokens"] == 64000
    assert body["stop_sequences"] == ["</block>"]
    # context_management.edits 按真实样本带 clear_thinking 条目（非空）。
    assert body["context_management"] == {"edits": [{"keep": "all", "type": "clear_thinking_20251015"}]}
    assert "system" in body
    # 真实样本里 Claude Code 会带 Agent 工具定义。
    assert isinstance(body.get("tools"), list) and body["tools"]
    assert body["tools"][0]["name"] == "Agent"
    # metadata.user_id 是 JSON 字符串（含 session_id）。
    uid = body["metadata"]["user_id"]
    assert isinstance(uid, str) and "session_id" in uid


def test_sim_opencode_body_is_openai_chat_shape():
    built = _build(client_type="opencode")
    assert built["request_protocol"] == "openai"
    assert built["chat_method"] == "chat"
    body = built["body"]
    assert body["model"] == "m1"
    # opencode 真实样本：system + user 两条。
    msgs = body["messages"]
    assert isinstance(msgs, list) and len(msgs) == 2
    assert msgs[0]["role"] == "system" and msgs[1]["role"] == "user"
    # system 带 agent-identity（真实样本结构）。
    assert "<agent-identity>" in msgs[0]["content"]
    assert body["stream_options"] == {"include_usage": True}
    assert body["tool_choice"] == "auto"
    assert body["max_tokens"] == 32000
    # 真实样本里 OpenCode 会带 ast_grep_replace 工具定义。
    assert isinstance(body.get("tools"), list) and body["tools"]
    assert body["tools"][0]["function"]["name"] == "ast_grep_replace"


def test_sim_codex_openai_body_is_openai_chat_shape():
    built = _build(client_type="codex-openai")
    assert built["request_protocol"] == "openai"
    assert built["chat_method"] == "chat"
    body = built["body"]
    msgs = body.get("messages")
    assert isinstance(msgs, list) and msgs
    # codex-openai 变体带 store/include/reasoning/client_metadata。
    assert body["store"] is False
    assert body["include"] == ["reasoning.encrypted_content"]
    assert body["reasoning"] == {"effort": "medium"}
    assert "client_metadata" in body and body["client_metadata"]["session_id"]
    assert body["parallel_tool_calls"] is True


def test_sim_workbuddy_body_is_openai_chat_shape():
    built = _build(client_type="workbuddy")
    assert built["request_protocol"] == "openai"
    assert built["chat_method"] == "chat"
    body = built["body"]
    assert body["model"] == "m1"
    assert body["stream"] is False
    assert body["messages"] == [{"role": "user", "content": "hi"}]
    # WorkBuddy 不应误带 Codex OpenAI 的 Responses/推理扩展字段。
    assert "store" not in body
    assert "reasoning" not in body
    assert "client_metadata" not in body


def test_data_prompt_overrides_default_sim_text():
    built = _build(client_type="opencode", data={"prompt": "hello world"})
    # opencode: messages[0]=system, messages[1]=user；user 文本应取 data.prompt。
    user_msg = built["body"]["messages"][1]
    assert user_msg["content"] == "hello world"

    # codex-cli responses 形态：user input_text 文本应取 data.prompt。
    codex = _build(client_type="codex-cli", data={"prompt": "hello world"})
    user_input = codex["body"]["input"][1]["content"][0]
    assert user_input["text"] == "hello world"


def test_default_test_type_keeps_protocol_aware_tool_shape(monkeypatch):
    """未改动的默认 tool 类型仍走 _build_test_body 协议感知逻辑：anthropic 用 input_schema。"""
    import config as config_module

    monkeypatch.setattr(config_module.Config, "get_test_types",
                        classmethod(lambda cls: [dict(t) for t in config_module.DEFAULT_TEST_TYPES]))
    built = _build(test_type="tool", protocol="anthropic", channel_protocol="anthropic")
    tools = built["body"]["tools"]
    # anthropic 形态：工具用 input_schema（协议感知回退，而非扁平 openai function 形态）。
    assert tools and "input_schema" in tools[0]
    assert "tool_choice" not in built["body"]


def test_customized_conversational_type_goes_flat(monkeypatch):
    """管理员改动某对话类型后走扁平 messages+body，不再按协议改写。"""
    import config as config_module

    custom = [{
        "key": "tool", "label": "工具", "operation": None,
        "messages": [{"role": "user", "content": "custom prompt"}],
        "body": {"tools": [{"type": "function", "function": {"name": "x"}}], "tool_choice": "required"},
    }]
    monkeypatch.setattr(config_module.Config, "get_test_types",
                        classmethod(lambda cls: custom))
    built = _build(test_type="tool", protocol="anthropic", channel_protocol="anthropic")
    body = built["body"]
    assert body["messages"] == [{"role": "user", "content": "custom prompt"}]
    # 扁平下发：body 原样带配置字段，不做 anthropic input_schema 改写。
    assert body["tool_choice"] == "required"
    assert body["tools"][0]["type"] == "function"


def test_customized_media_type_uses_config_body(monkeypatch):
    """自定义媒体类型用配置 operation + body（model 恒在，data 同名字段可覆盖）。"""
    import config as config_module

    custom = [{
        "key": "myimg", "label": "自定义图", "operation": "image",
        "messages": [], "body": {"prompt": "cfg prompt", "size": "512x512"},
    }]
    monkeypatch.setattr(config_module.Config, "get_test_types",
                        classmethod(lambda cls: custom))
    built = _build(test_type="myimg")
    assert built["operation"] == "image"
    assert built["body"]["model"] == "m1"
    assert built["body"]["prompt"] == "cfg prompt"
    assert built["body"]["size"] == "512x512"
    # data 同名字段覆盖配置默认。
    built2 = _build(test_type="myimg", data={"prompt": "override"})
    assert built2["body"]["prompt"] == "override"


def test_unknown_test_type_falls_back_to_chat(monkeypatch):
    """配置里没有的 key 回退默认聊天构造（协议感知回退路径）。"""
    import config as config_module

    monkeypatch.setattr(config_module.Config, "get_test_types",
                        classmethod(lambda cls: [dict(t) for t in config_module.DEFAULT_TEST_TYPES]))
    built = _build(test_type="does-not-exist")
    assert built["operation"] is None
    assert built["body"]["messages"] == [{"role": "user", "content": "hi"}]


def test_get_test_types_defaults_when_empty(monkeypatch):
    """配置缺失/非法时 get_test_types 回退 DEFAULT_TEST_TYPES。"""
    import config as config_module

    monkeypatch.setattr(config_module.Config, "_load", classmethod(lambda cls: {}))
    types = config_module.Config.get_test_types()
    keys = [t["key"] for t in types]
    assert keys == [t["key"] for t in config_module.DEFAULT_TEST_TYPES]


def _custom_type(monkeypatch, key="vars", operation=None, messages=None, body=None):
    import config as config_module

    custom = [{
        "key": key, "label": key, "operation": operation,
        "messages": messages if messages is not None else [{"role": "user", "content": "hi"}],
        "body": body if body is not None else {},
    }]
    monkeypatch.setattr(config_module.Config, "get_test_types",
                        classmethod(lambda cls: custom))


def test_customized_template_renders_context_vars(monkeypatch):
    """自定义模板 messages/body 字符串里的 {{model}}/{{username}}/{{provider}} 按本次测试上下文渲染。"""
    _custom_type(monkeypatch, messages=[{"role": "user", "content": "model={{model}} user={{username}}"}],
                 body={"user": "{{username}}", "nested": {"list": ["{{provider}}"]}})
    built = _build(test_type="vars", model="m1", username="u1", provider_name="p1")
    assert built["body"]["messages"][0]["content"] == "model=m1 user=u1"
    assert built["body"]["user"] == "u1"
    assert built["body"]["nested"]["list"] == ["p1"]


def test_customized_template_renders_unique_vars_per_call(monkeypatch):
    """{{uuid}}/{{timestamp}} 逐次求值：两次构造的渲染结果不同。"""
    _custom_type(monkeypatch, messages=[{"role": "user", "content": "{{uuid}}@{{timestamp}}"}])
    first = _build(test_type="vars")
    second = _build(test_type="vars")
    assert first["body"]["messages"][0]["content"] != second["body"]["messages"][0]["content"]


def test_customized_template_keeps_unknown_tokens(monkeypatch):
    """未识别的 {{...}} 原样保留，不被清空（避免误伤合法双花括号内容）。"""
    _custom_type(monkeypatch, messages=[{"role": "user", "content": "{{not_a_var}} {{model}}"}])
    built = _build(test_type="vars")
    assert built["body"]["messages"][0]["content"] == "{{not_a_var}} m1"


def test_customized_template_keeps_non_string_values(monkeypatch):
    """变量渲染只改字符串叶子：number/bool/null 与嵌套结构原样保留。"""
    _custom_type(monkeypatch, body={"n": 1, "flag": True, "none": None, "deep": [{"x": "{{model}}"}]})
    built = _build(test_type="vars")
    assert built["body"]["n"] == 1
    assert built["body"]["flag"] is True
    assert built["body"]["none"] is None
    assert built["body"]["deep"] == [{"x": "m1"}]


def test_customized_media_template_renders_vars(monkeypatch):
    """自定义媒体类型的 body 同样渲染变量；data 同名字段覆盖优先于模板值。"""
    _custom_type(monkeypatch, operation="image",
                 body={"prompt": "apple for {{model}} on {{provider}}", "n": 1})
    built = _build(test_type="vars")
    assert built["operation"] == "image"
    assert built["body"]["prompt"] == "apple for m1 on p1"
    overridden = _build(test_type="vars", data={"prompt": "override"})
    assert overridden["body"]["prompt"] == "override"


def test_default_types_do_not_render_vars(monkeypatch):
    """未改动的默认类型不走模板内容（协议感知回退），不存在变量泄漏。"""
    import config as config_module

    monkeypatch.setattr(config_module.Config, "get_test_types",
                        classmethod(lambda cls: [dict(t) for t in config_module.DEFAULT_TEST_TYPES]))
    built = _build(test_type="chat", model="m1", username="u1", provider_name="p1")
    assert built["body"]["messages"] == [{"role": "user", "content": "hi"}]


def test_render_test_template_vars_recurse_shapes():
    """渲染器本身：字符串/list/dict 递归，其它类型原样返回。"""
    variables = {"model": "m1"}
    assert admin._render_test_template_vars("a {{model}} b", variables) == "a m1 b"
    assert admin._render_test_template_vars(["{{model}}", 3, None], variables) == ["m1", 3, None]
    assert admin._render_test_template_vars({"k": {"j": "{{model}}"}}, variables) == {"k": {"j": "m1"}}
    assert admin._render_test_template_vars(42, variables) == 42
    assert admin._render_test_template_vars("{{unknown}}", variables) == "{{unknown}}"


# ── 与 Headers 配置同一套变量能力 ──────────────────────────────────────────
# 测试模板的变量集/渲染语义必须与出站 Header 模板一致（同一份实现，见
# providers/custom.py 的 build_request_template_variables / render_account_template_paths）。


class _StubProvider:
    PROVIDER_NAME = "custom"
    api_key = "sk-must-not-leak"


class _StubAccount:
    """账号运行态桩：与 AccountClient 对模板可见的那几个属性同形。"""
    def __init__(self):
        self.username = "acct-1"
        self.metadata = {"org_id": "org-9"}
        self.provider = _StubProvider()


def test_test_template_variable_set_matches_header_template():
    """内置标量变量集与 Header 模板完全一致，测试模板只多出本次测试上下文三个。"""
    from providers.custom import build_request_template_variables

    header_vars = set(build_request_template_variables({}))
    test_vars = set(admin._test_template_variables(model="m1", username="u1", provider_name="p1"))
    assert header_vars <= test_vars
    assert test_vars - header_vars == {"model", "username", "provider"}


def test_customized_template_renders_client_session_vars(monkeypatch):
    """{{client_session_id}} 取本次测试构造出的模拟客户端 headers，与出站 Header 模板同源同值。"""
    from providers.custom import CustomProvider

    _custom_type(monkeypatch, operation="image", body={"prompt": "s={{client_session_id}}"})
    built = _build(test_type="vars", client_type="claude-code")
    expected = CustomProvider.CLIENT_PRESETS["claude-code"]["x-claude-code-session-id"]
    assert built["body"]["prompt"] == f"s={expected}"


def test_customized_template_renders_absent_session_vars_as_empty(monkeypatch):
    """通用测试（无客户端头）时会话类变量渲染为空串，不留 {{...}} 残体。"""
    _custom_type(monkeypatch, messages=[{"role": "user", "content": "s=[{{client_session_id}}][{{client_request_id}}]"}])
    built = _build(test_type="vars")
    assert built["body"]["messages"][0]["content"] == "s=[][]"


def test_customized_template_renders_account_context(monkeypatch):
    """{{account.username}}/{{account.provider}}/{{account.metadata.<字段>}} 走被测账号运行态。"""
    _custom_type(monkeypatch, messages=[{"role": "user", "content": "{{account.username}}/{{account.provider}}"}],
                 body={"org": "{{account.metadata.org_id}}"})
    built = _build(test_type="vars", account_client=_StubAccount())
    assert built["body"]["messages"][0]["content"] == "acct-1/custom"
    assert built["body"]["org"] == "org-9"


def test_customized_template_account_vars_never_expose_secrets(monkeypatch):
    """缺值与密钥路径一律渲染为空串：账号密钥拿不到（与 Header 模板同一约束）。"""
    _custom_type(monkeypatch, body={"probe": "[{{account.metadata.missing}}][{{account.provider.api_key}}]"})
    built = _build(test_type="vars", account_client=_StubAccount())
    assert built["body"]["probe"] == "[][]"
    assert "sk-must-not-leak" not in json.dumps(built["body"])


def test_customized_template_account_vars_empty_without_account(monkeypatch):
    """池外/无账号上下文时 account.* 渲染为空串，不报错。"""
    _custom_type(monkeypatch, body={"probe": "[{{account.username}}]"})
    assert _build(test_type="vars")["body"]["probe"] == "[]"


def test_customized_template_shares_one_variable_snapshot_per_test(monkeypatch):
    """同一次测试里所有叶子共享同一份 {{uuid}}（变量表只求值一次），与真实请求一致。"""
    _custom_type(monkeypatch, messages=[{"role": "user", "content": "{{uuid}}"}], body={"echo": "{{uuid}}"})
    built = _build(test_type="vars")
    assert built["body"]["echo"] == built["body"]["messages"][0]["content"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
