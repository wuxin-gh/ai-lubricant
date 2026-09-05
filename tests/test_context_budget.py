import asyncio

import pytest
from fastapi import HTTPException

import main
from providers.custom import CustomProvider


class _FakeModelClientPool:
    @staticmethod
    def get_model_routes(model):
        return [{"provider": "test"}]

    @staticmethod
    async def get_model_info(model):
        return {"id": model, "max_tokens": 16, "max_context_tokens": 20}


async def _false_model_group(model):
    return False


async def _true_explicit_metadata(model):
    return True


@pytest.fixture(autouse=True)
def patch_model_dependencies(monkeypatch):
    monkeypatch.setattr(main.config.Config, "is_model_group", _false_model_group)
    monkeypatch.setattr(main.model_metadata, "has_explicit_metadata", _true_explicit_metadata)
    monkeypatch.setattr(main, "ModelClientPool", _FakeModelClientPool)


def test_prepare_client_request_body_rejects_internal_scheme_model_before_config_lookup(monkeypatch):
    async def unexpected_lookup(api_key):
        raise AssertionError("internal model must be rejected before API key config lookup")

    monkeypatch.setattr(main.PostgresClient, "get_api_key_by_key", unexpected_lookup)
    internal_id = f"opus{main.model_catalog.SCHEME_GROUP_SEP}1"

    for protocol, endpoint in (
        ("openai", "/v1/chat/completions"),
        ("responses", "/v1/responses"),
        ("anthropic", "/v1/messages"),
        ("openai", "/v1/images/generations"),
        ("openai", "/v1/audio/speech"),
    ):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(main._prepare_client_request_body(
                {"model": internal_id},
                "sk-test",
                {},
                endpoint,
                protocol,
            ))

        assert exc.value.status_code == 400
        assert exc.value.detail == {
            "error": {
                "message": "The requested model is not available.",
                "type": "invalid_request_error",
                "code": "model_not_found",
                "param": "model",
            }
        }
        assert internal_id not in str(exc.value.detail)


def test_prepare_client_request_body_applies_matching_api_key_thinking(monkeypatch):
    async def fake_get_api_key_by_key(api_key):
        assert api_key == "sk-test"
        return {
            "thinking_config": {
                "enabled": True,
                "budget_tokens": 2048,
                "client_types": ["claude-code"],
            }
        }

    monkeypatch.setattr(main.PostgresClient, "get_api_key_by_key", fake_get_api_key_by_key)
    body = {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]}

    prepared, client_type = asyncio.run(main._prepare_client_request_body(
        body,
        "sk-test",
        {"user-agent": "claude-code"},
        "/v1/messages",
        "anthropic",
    ))

    assert client_type == "claude-code"
    assert prepared["thinking"] == {"type": "enabled", "budget_tokens": 2048}
    assert "thinking" not in body


def test_prepare_client_request_body_skips_disabled_api_key_thinking(monkeypatch):
    async def fake_get_api_key_by_key(api_key):
        return {"thinking_config": {"enabled": False, "budget_tokens": 2048}}

    monkeypatch.setattr(main.PostgresClient, "get_api_key_by_key", fake_get_api_key_by_key)
    body = {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]}

    prepared, _ = asyncio.run(main._prepare_client_request_body(
        body,
        "sk-test",
        {"user-agent": "claude-code"},
        "/v1/messages",
        "anthropic",
    ))

    assert prepared is body
    assert "thinking" not in prepared


def test_prepare_client_request_body_skips_unmatched_api_key_thinking(monkeypatch):
    async def fake_get_api_key_by_key(api_key):
        return {
            "thinking_config": {
                "enabled": True,
                "budget_tokens": 2048,
                "client_types": ["cursor"],
            }
        }

    monkeypatch.setattr(main.PostgresClient, "get_api_key_by_key", fake_get_api_key_by_key)
    body = {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]}

    prepared, client_type = asyncio.run(main._prepare_client_request_body(
        body,
        "sk-test",
        {"user-agent": "claude-code"},
        "/v1/messages",
        "anthropic",
    ))

def test_api_key_thinking_selected_scope_requires_matching_client(monkeypatch):
    async def fake_get_api_key_by_key(api_key):
        return {
            "thinking_config": {
                "enabled": True,
                "budget_tokens": 2048,
                "client_scope": "selected",
                "client_types": [],
            }
        }

    monkeypatch.setattr(main.PostgresClient, "get_api_key_by_key", fake_get_api_key_by_key)
    body = {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]}

    prepared, client_type = asyncio.run(main._prepare_client_request_body(
        body,
        "sk-test",
        {"user-agent": "claude-code"},
        "/v1/messages",
        "anthropic",
    ))

    assert client_type == "claude-code"
    assert prepared is body
    assert "thinking" not in prepared


def test_api_key_thinking_all_scope_ignores_client_type(monkeypatch):
    async def fake_get_api_key_by_key(api_key):
        return {
            "thinking_config": {
                "enabled": True,
                "budget_tokens": 2048,
                "client_scope": "all",
                "client_types": ["cursor"],
            }
        }

    monkeypatch.setattr(main.PostgresClient, "get_api_key_by_key", fake_get_api_key_by_key)
    body = {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]}

    prepared, client_type = asyncio.run(main._prepare_client_request_body(
        body,
        "sk-test",
        {"user-agent": "claude-code"},
        "/v1/messages",
        "anthropic",
    ))

    assert client_type == "claude-code"
    assert prepared["thinking"] == {"type": "enabled", "budget_tokens": 2048}


    body = {
        "model": "test-model",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 1,
        "tools": [{
            "type": "function",
            "function": {
                "name": "oversized_tool",
                "description": "x" * 200,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string", "description": "y" * 100}
                    },
                },
            },
        }],
    }

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._validate_chat_request(body))

    detail = exc.value.detail
    assert exc.value.status_code == 400
    assert detail == {
        "error": {
            "message": "Your input exceeds the context window of this model. Please adjust your input and try again.",
            "type": "invalid_request_error",
            "code": "context_too_large",
            "param": "tools, messages",
        }
    }


def test_max_tokens_limit_rejects_text_model_over_limit(monkeypatch):
    """文本模型仍按元数据 max_tokens 拦截超限请求。"""
    async def model_info(model):
        return {"id": model, "max_tokens": 10000, "output_modalities": ["text"]}

    monkeypatch.setattr(main.ModelClientPool, "get_model_info", model_info)
    monkeypatch.setattr(main.config.Config, "context_token_detection_enabled", lambda: False)
    body = {
        "model": "test-model",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 10240,
    }

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._validate_chat_request(body))

    assert exc.value.status_code == 400
    assert "超过模型上限" in str(exc.value.detail)


@pytest.mark.parametrize("modalities", [["image"], ["video"], ["audio"], "image"])
def test_max_tokens_limit_skipped_for_generation_models(monkeypatch, modalities):
    """图片/视频/语音模型的输出不按 token 计量，不该被 max_tokens 上限拦下。"""
    async def model_info(model):
        return {"id": model, "max_tokens": 10000, "output_modalities": modalities}

    monkeypatch.setattr(main.ModelClientPool, "get_model_info", model_info)
    monkeypatch.setattr(main.config.Config, "context_token_detection_enabled", lambda: False)
    body = {
        "model": "gpt-image-2",
        "messages": [{"role": "user", "content": "draw a red apple"}],
        "max_tokens": 10240,
    }

    model, messages = asyncio.run(main._validate_chat_request(body))

    assert model == "gpt-image-2"
    assert messages == body["messages"]


def test_max_tokens_limit_applies_to_multimodal_output_including_text(monkeypatch):
    """输出含 text 的多模态模型仍走 token 上限校验。"""
    async def model_info(model):
        return {"id": model, "max_tokens": 10000, "output_modalities": ["text", "image"]}

    monkeypatch.setattr(main.ModelClientPool, "get_model_info", model_info)
    monkeypatch.setattr(main.config.Config, "context_token_detection_enabled", lambda: False)
    body = {
        "model": "test-model",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 10240,
    }

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._validate_chat_request(body))

    assert exc.value.status_code == 400


def test_max_tokens_limit_applies_when_modalities_missing(monkeypatch):
    """未声明 output_modalities 时按文本处理，保持原有拦截行为。"""
    async def model_info(model):
        return {"id": model, "max_tokens": 10000}

    monkeypatch.setattr(main.ModelClientPool, "get_model_info", model_info)
    monkeypatch.setattr(main.config.Config, "context_token_detection_enabled", lambda: False)
    body = {
        "model": "test-model",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 10240,
    }

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._validate_chat_request(body))

    assert exc.value.status_code == 400


def test_chat_request_counts_independent_system_field():
    body = {
        "model": "test-model",
        "system": "x" * 200,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 1,
    }

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main._validate_chat_request(body))

    assert exc.value.status_code == 400
    assert exc.value.detail["error"]["code"] == "context_too_large"
    assert "system" in exc.value.detail["error"]["param"]


def test_final_openai_payload_context_check_counts_converted_system_message():
    provider = CustomProvider("user", "key", provider_name="test-provider", base_url="https://example.test")
    payload = provider._build_openai_payload(
        "test-model",
        [
            {"role": "system", "content": "x" * 200},
            {"role": "user", "content": "hi"},
        ],
        True,
        max_tokens=1,
        model_info={"max_context_tokens": 20},
    )

    with pytest.raises(HTTPException) as exc:
        main.validate_upstream_context_budget("test-model", payload, {"max_context_tokens": 20})

    assert exc.value.status_code == 400
    assert exc.value.detail["error"]["code"] == "context_too_large"
    assert "messages" in exc.value.detail["error"]["param"]


def test_tokenizer_builtin_chars_fallback():
    """未知模型走兜底规则：拉丁文本 3.5 字符/token。

    历史上兜底是 2 字符/token，实测对拉丁文本高估约 2 倍，是「预测入」虚高的主因之一，
    已按真实请求体校准。20 字符 → ceil(20/3.5) = 6。
    """
    assert main._estimate_request_part_tokens("unknown-model", "x" * 20) == 6


def test_tokenizer_fallback_weighs_cjk_separately():
    """CJK 每字符约占 1 个 token，不能和拉丁文本共用一个比值。"""
    latin = main._estimate_request_part_tokens("unknown-model", "x" * 30)
    cjk = main._estimate_request_part_tokens("unknown-model", "中" * 30)
    assert cjk > latin
    # 30 个 CJK 字符按 1.5 字符/token → 20
    assert cjk == 20


def test_tokenizer_openai_family_uses_o200k(monkeypatch):
    """gpt-5.x 必须走 o200k_base；历史内置规则只匹配 cl100k，对新模型口径已过时。"""
    monkeypatch.setattr(main.config.Config, "get_tokenizer_rules", lambda: [])
    import usage_utils

    assert usage_utils.resolve_tokenizer_rule("gpt-5.5")["encoding"] == "o200k_base"
    assert usage_utils.resolve_tokenizer_rule("gpt-4o")["encoding"] == "o200k_base"
    # 老模型仍应留在 cl100k。
    assert usage_utils.resolve_tokenizer_rule("gpt-4-turbo")["encoding"] == "cl100k_base"
    assert usage_utils.resolve_tokenizer_rule("gpt-3.5-turbo")["encoding"] == "cl100k_base"


def test_tokenizer_calibration_scales_result():
    import usage_utils

    base = {"type": "chars", "chars_per_token": 1}
    scaled = {"type": "chars", "chars_per_token": 1, "calibration": 0.5}
    assert usage_utils.estimate_text_tokens_by_rule("x" * 100, base) == 100
    assert usage_utils.estimate_text_tokens_by_rule("x" * 100, scaled) == 50


def test_tokenizer_huggingface_degrades_without_vocab(monkeypatch):
    """词表没预热时必须降级、不抛异常——本函数在 token 预占热路径上。"""
    import usage_utils

    monkeypatch.setattr(usage_utils, "load_hf_tokenizer", lambda repo: None)
    rule = {
        "type": "huggingface",
        "repo": "nonexistent-owner/nonexistent-model",
        "encoding": "cl100k_base",
    }
    # 有 encoding 时降级到 tiktoken，仍应给出正整数。
    assert usage_utils.estimate_text_tokens_by_rule("hello world", rule) > 0
    # 没有 encoding 时降级到字符估算。
    assert usage_utils.estimate_text_tokens_by_rule(
        "x" * 20, {"type": "huggingface", "repo": "a/b", "chars_per_token": 2}
    ) == 10


def test_tokenizer_tiktoken_falls_back_to_chars_when_package_missing(monkeypatch):
    import usage_utils

    original_getter = usage_utils._tokenizer_rules_getter
    usage_utils.set_tokenizer_rules_getter(lambda: [{
        "name": "openai",
        "enabled": True,
        "pattern": "^gpt-",
        "type": "tiktoken",
        "encoding": "cl100k_base",
    }])
    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "tiktoken":
            raise ImportError("missing tiktoken")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)
    try:
        assert main._estimate_request_part_tokens("gpt-test", "x" * 20) == 10
    finally:
        usage_utils.set_tokenizer_rules_getter(original_getter)


def test_tokenizer_custom_rule_priority():
    import usage_utils

    original_getter = usage_utils._tokenizer_rules_getter
    usage_utils.set_tokenizer_rules_getter(lambda: [{
        "name": "business gpt alias",
        "enabled": True,
        "pattern": "^gpt-5\\.5$",
        "type": "chars",
        "chars_per_token": 10,
    }])
    try:
        assert main._estimate_request_part_tokens("gpt-5.5", "x" * 100) == 10
    finally:
        usage_utils.set_tokenizer_rules_getter(original_getter)


def test_upstream_responses_payload_counts_input_not_messages():
    body = {
        "model": "test-model",
        "input": "x" * 200,
        "max_tokens": 1,
    }

    with pytest.raises(HTTPException) as exc:
        main.validate_upstream_context_budget("test-model", body, {"max_context_tokens": 20})

    assert exc.value.status_code == 400
    assert exc.value.detail["error"]["code"] == "context_too_large"
    assert "input" in exc.value.detail["error"]["param"]


def test_context_detection_disabled_skips_overflow_rejection(monkeypatch):
    """开关关闭后不再按本地估算拦截；预占仍按同一估算器工作（本测试只断言不拦截）。"""
    monkeypatch.setattr(main.config.Config, "context_token_detection_enabled", lambda: False)
    body = {"model": "test-model", "input": "x" * 200, "max_tokens": 1}

    main.validate_upstream_context_budget("test-model", body, {"max_context_tokens": 20})


def test_reservation_and_context_detection_share_model_aware_estimate(monkeypatch):
    """预占与上下文拦截必须落在同一个模型感知估算口径上。

    自定义规则把 gpt-5.5 设为 10 字符/token；两处对同一 body 必须得到同一输入量，
    且 system/tools 都要计入（历史上预占丢了 model，永远拿不到该规则）。
    """
    import usage_utils

    original_getter = usage_utils._tokenizer_rules_getter
    usage_utils.set_tokenizer_rules_getter(lambda: [{
        "name": "business gpt alias",
        "enabled": True,
        "pattern": "^gpt-5\\.5$",
        "type": "chars",
        "chars_per_token": 10,
    }])
    try:
        body = {
            "system": "s" * 100,
            "messages": [{"role": "user", "content": "u" * 100}],
            "tools": [{"type": "function", "function": {"name": "t"}}],
        }

        reservation_input = main.estimate_input_tokens("gpt-5.5", body)
        detection_input = sum(main.estimate_input_token_parts("gpt-5.5", body, "chat").values())

        assert reservation_input == detection_input
        # 模型规则生效（10 字符/token），且 system 与 tools 都计入了。
        assert reservation_input == sum(
            main._estimate_request_part_tokens("gpt-5.5", body[key]) for key in ("system", "messages", "tools")
        )
        assert main._estimate_request_part_tokens("gpt-5.5", body["system"]) == 10
        # 模型盲估（旧预占口径）会拿不到规则，因此显著更大——防回归。
        assert main.estimate_input_tokens("", body) > reservation_input
    finally:
        usage_utils.set_tokenizer_rules_getter(original_getter)


# ── 预占请求体：system 不得重复计入 ──────────────────────────────


def test_reservation_body_skips_kwargs_system_when_messages_already_have_it():
    """anthropic 路径的真实形态：system 既在 messages[0] 又在 kwargs，只能计一次。"""
    messages = [
        {"role": "system", "content": "S" * 100},
        {"role": "user", "content": "u" * 10},
    ]
    body = main._build_reservation_body(messages, {"system": "S" * 100})

    assert "system" not in body
    assert body["messages"] is messages


def test_reservation_body_keeps_kwargs_system_for_openai_path():
    """OpenAI 路径 messages 里没有 system，kwargs 的 system 必须计入，不能误删。"""
    messages = [{"role": "user", "content": "u" * 10}]
    body = main._build_reservation_body(messages, {"system": "S" * 100, "tools": [{"name": "t"}]})

    assert body["system"] == "S" * 100
    assert body["tools"] == [{"name": "t"}]


def test_reservation_body_dedup_matches_single_count_on_real_anthropic_conversion():
    """走真实转换链，断言去重后的估算等于「只算一次 system」。

    这是本次修复的核心回归：修复前该值等于估算 + 一份完整 system。
    """
    from message_utils import anthropic_to_openai_messages

    anthropic_body = {
        "model": "claude-opus-5",
        "system": "你是一个很长的系统提示。" * 500,
        "messages": [{"role": "user", "content": "你好"}],
    }
    messages, conv_kwargs = anthropic_to_openai_messages(anthropic_body)
    kwargs = main._build_protocol_kwargs(anthropic_body, "openai", "anthropic")
    kwargs.update(conv_kwargs)

    # 前置条件：system 确实同时出现在两处，否则本测试失去意义。
    assert messages[0]["role"] == "system"
    assert kwargs.get("system") is not None

    model = anthropic_body["model"]
    deduped = main.estimate_input_tokens(model, main._build_reservation_body(messages, kwargs))
    single = main.estimate_input_tokens(model, {"messages": messages})
    system_alone = main._estimate_request_part_tokens(model, anthropic_body["system"])

    assert deduped == single
    # 修复前的口径 = 去重后 + 一整份 system，差额可观（防止有人把去重改回去）。
    assert system_alone > 1000
    naive = main.estimate_input_tokens(model, {"messages": messages, "system": anthropic_body["system"]})
    assert naive - deduped == system_alone


# ── 新增估算方式 ──────────────────────────────────────────────────


def test_chars_rule_weighs_cjk_separately():
    """CJK 与拉丁字符的字符/token 比差一个数量级，必须分开计权。"""
    import usage_utils

    rule = {"type": "chars", "chars_per_token": 4, "cjk_chars_per_token": 1}
    # 20 个拉丁字符 → 5；20 个汉字 → 20。
    assert usage_utils.estimate_chars_tokens("x" * 20, rule) == 5
    assert usage_utils.estimate_chars_tokens("字" * 20, rule) == 20


def test_chars_rule_without_cjk_weight_keeps_legacy_behavior():
    """未配 cjk_chars_per_token 时必须退化为单一比值，保持老规则语义不变。"""
    import usage_utils

    rule = {"type": "chars", "chars_per_token": 2}
    assert usage_utils.estimate_chars_tokens("字" * 20, rule) == 10


def test_calibration_scales_result():
    import usage_utils

    base = usage_utils.estimate_text_tokens_by_rule("hello world " * 50, {"type": "tiktoken", "encoding": "o200k_base"})
    half = usage_utils.estimate_text_tokens_by_rule(
        "hello world " * 50, {"type": "tiktoken", "encoding": "o200k_base", "calibration": 0.5}
    )
    assert base > 0
    assert half == max(1, int(base * 0.5 + 0.5))


def test_huggingface_rule_degrades_to_encoding_when_vocab_missing(monkeypatch):
    """词表未预热时必须降级且不抛异常——估算在请求热路径上，不能影响请求本身。"""
    import usage_utils

    usage_utils.invalidate_hf_tokenizer_cache()
    rule = {"type": "huggingface", "repo": "no-such-owner/no-such-model", "encoding": "o200k_base"}
    text = "hello world " * 50

    got = usage_utils.estimate_text_tokens_by_rule(text, rule)
    expected = usage_utils.estimate_tiktoken_tokens(text, {"encoding": "o200k_base"})
    assert got == expected


def test_huggingface_rule_degrades_to_chars_without_encoding():
    import usage_utils

    usage_utils.invalidate_hf_tokenizer_cache()
    rule = {"type": "huggingface", "repo": "no-such-owner/no-such-model", "chars_per_token": 4}
    assert usage_utils.estimate_text_tokens_by_rule("x" * 40, rule) == 10


def test_builtin_rules_route_models_to_expected_tokenizer():
    """内置规则的分族路由。顺序敏感：legacy 必须先于通用 OpenAI 规则。"""
    import usage_utils

    def rule_for(model):
        resolved = usage_utils.resolve_tokenizer_rule(model)
        return resolved.get("name"), resolved.get("encoding")

    assert rule_for("gpt-4-turbo") == ("OpenAI legacy", "cl100k_base")
    # gpt-5.x 曾被错配到 cl100k_base，这里锁死为 o200k_base。
    assert rule_for("gpt-5.5")[1] == "o200k_base"
    assert rule_for("gpt-4o")[1] == "o200k_base"
    assert rule_for("claude-opus-5")[0] == "Claude"
    assert usage_utils.resolve_tokenizer_rule("glm-5.2").get("type") == "huggingface"
    # 未知模型走兜底，且兜底不再是裸 chars/2。
    fallback = usage_utils.resolve_tokenizer_rule("totally-unknown-xyz")
    assert fallback.get("type") == "chars"
    assert fallback.get("cjk_chars_per_token") is not None


