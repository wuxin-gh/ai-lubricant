"""attempt_builder 离线单测：body 隔离 + 模型名解析 + 默认参数注入顺序。"""

import attempt_builder as ab


# ── build_attempt_body：深拷贝隔离 ──────────────────────────────

def test_build_attempt_body_deep_copies_nested():
    original = {
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "tools": [{"type": "function", "function": {"name": "f"}}],
        "metadata": {"k": "v"},
    }
    a = ab.build_attempt_body(original)
    # 修改 attempt body 的嵌套结构，不能污染原始 body
    a["messages"][0]["content"] = "changed"
    a["tools"][0]["function"]["name"] = "g"
    a["metadata"]["k"] = "x"
    a["new_key"] = 1
    assert original["messages"][0]["content"] == "hi"
    assert original["tools"][0]["function"]["name"] == "f"
    assert original["metadata"]["k"] == "v"
    assert "new_key" not in original


def test_two_attempts_are_independent():
    original = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    a1 = ab.build_attempt_body(original)
    a2 = ab.build_attempt_body(original)
    a1["messages"][0]["content"] = "from-a1"
    assert a2["messages"][0]["content"] == "hi"


# ── resolve_model_identity：四种模型名 ──────────────────────────

def test_identity_direct_model_uses_routed_as_response():
    ident = ab.resolve_model_identity(
        requested_model="gpt",
        route_info={"routed_model": "gpt", "upstream_model_id": "provider-gpt"},
        resolved_upstream_id=None,
        stable_response_model=None,
    )
    assert ident.requested_model == "gpt"
    assert ident.routed_model == "gpt"
    assert ident.upstream_model == "provider-gpt"
    assert ident.response_model == "gpt"


def test_identity_group_uses_stable_response_model():
    ident = ab.resolve_model_identity(
        requested_model="my-group",
        route_info={"routed_model": "claude-main"},
        resolved_upstream_id="up-claude",
        stable_response_model="my-group",
    )
    # 上游用真实 routed/upstream，回客户端用稳定组名
    assert ident.routed_model == "claude-main"
    assert ident.upstream_model == "up-claude"
    assert ident.response_model == "my-group"


def test_identity_upstream_fallback_chain():
    # route_info 无 upstream_model_id 且无 resolved → 回退到 routed
    ident = ab.resolve_model_identity(
        requested_model="m",
        route_info={"routed_model": "r"},
        resolved_upstream_id=None,
        stable_response_model=None,
    )
    assert ident.upstream_model == "r"


def test_identity_missing_routed_falls_back_to_requested():
    ident = ab.resolve_model_identity(
        requested_model="req",
        route_info={},
        resolved_upstream_id=None,
        stable_response_model=None,
    )
    assert ident.routed_model == "req"
    assert ident.upstream_model == "req"
    assert ident.response_model == "req"


# ── apply_model_defaults：只填 None，客户端值优先 ───────────────

def test_apply_defaults_only_fills_missing():
    kwargs = {"temperature": 0.2, "top_p": None}
    ab.apply_model_defaults(kwargs, {"temperature": 0.9, "top_p": 0.8, "max_tokens": 100})
    # 客户端显式 temperature 不被覆盖
    assert kwargs["temperature"] == 0.2
    # None 值被默认填充
    assert kwargs["top_p"] == 0.8
    assert kwargs["max_tokens"] == 100


def test_apply_defaults_skips_none_and_underscore():
    kwargs = {}
    ab.apply_model_defaults(kwargs, {"a": None, "_internal": 5, "b": 1})
    assert "a" not in kwargs
    assert "_internal" not in kwargs
    assert kwargs["b"] == 1


def test_apply_defaults_empty_is_noop():
    kwargs = {"x": 1}
    assert ab.apply_model_defaults(kwargs, None) == {"x": 1}
    assert ab.apply_model_defaults(kwargs, {}) == {"x": 1}


# ── apply_extra_config_overrides：强制覆盖出站字段 ───────────────

def test_extra_config_overrides_force_overwrites_existing():
    # 与 apply_model_defaults 相反：强制覆盖，无论原值是否存在
    kwargs = {"top_p": 0.5, "model": "m"}
    out = ab.apply_extra_config_overrides(kwargs, {"top_p": 0.9})
    assert out["top_p"] == 0.9
    assert out["model"] == "m"  # 未在 extra_config 中的字段不动


def test_extra_config_overrides_adds_new_fields():
    kwargs = {"model": "m"}
    out = ab.apply_extra_config_overrides(kwargs, {"top_p": 0.9})
    assert out["top_p"] == 0.9


def test_extra_config_overrides_skips_none_internal_and_default_semantic_keys():
    kwargs = {"model": "m"}
    out = ab.apply_extra_config_overrides(
        kwargs,
        {
            "top_p": None,                    # None 跳过
            "_internal": "x",                 # 下划线内部键跳过
            "client_preset": "p",             # 保留键跳过（由 custom.py 链路消费）
            "enable_1m_context": True,         # 保留键跳过（由 route_info 消费）
            "output_modalities": ["text"],    # 保留键跳过（由操作筛选消费）
            # max_tokens/reasoning_effort/thinking 走默认值语义（apply_extra_config_defaults），
            # 不在强制覆盖里生效：
            "max_tokens": 100,
            "reasoning_effort": "high",
            "thinking": {"type": "enabled"},
        },
    )
    assert "top_p" not in out
    assert "_internal" not in out
    assert "client_preset" not in out
    assert "enable_1m_context" not in out
    assert "output_modalities" not in out
    assert "max_tokens" not in out
    assert "reasoning_effort" not in out
    assert "thinking" not in out


def test_extra_config_overrides_empty_or_non_dict_is_noop():
    assert ab.apply_extra_config_overrides({"model": "m"}, None) == {"model": "m"}
    assert ab.apply_extra_config_overrides({"model": "m"}, {}) == {"model": "m"}
    assert ab.apply_extra_config_overrides({"model": "m"}, "not-a-dict") == {"model": "m"}


# ── apply_extra_config_defaults：max_tokens/reasoning_effort/thinking 默认值（客户端优先） ─

def test_extra_config_defaults_fills_only_when_client_missing():
    # 客户端未传 max_tokens → 用渠道配置兜底
    kwargs = {"model": "m"}
    out = ab.apply_extra_config_defaults(kwargs, {"max_tokens": 8192})
    assert out["max_tokens"] == 8192
    # 客户端已传 → 保留客户端值，渠道不覆盖
    kwargs = {"model": "m", "max_tokens": 4096}
    out = ab.apply_extra_config_defaults(kwargs, {"max_tokens": 8192})
    assert out["max_tokens"] == 4096


def test_extra_config_defaults_only_whitelist_keys():
    # 只对 max_tokens/reasoning_effort/thinking 生效，其它 extra_config 键不动
    kwargs = {"model": "m"}
    out = ab.apply_extra_config_defaults(kwargs, {"max_tokens": 100, "top_p": 0.9, "client_preset": "p"})
    assert out["max_tokens"] == 100
    assert "top_p" not in out
    assert "client_preset" not in out


def test_extra_config_defaults_skips_none_values():
    kwargs = {"model": "m"}
    out = ab.apply_extra_config_defaults(kwargs, {"max_tokens": None, "thinking": None})
    assert "max_tokens" not in out
    assert "thinking" not in out


def test_extra_config_defaults_empty_or_non_dict_is_noop():
    assert ab.apply_extra_config_defaults({"model": "m"}, None) == {"model": "m"}
    assert ab.apply_extra_config_defaults({"model": "m"}, {}) == {"model": "m"}
    assert ab.apply_extra_config_defaults({"model": "m"}, "not-a-dict") == {"model": "m"}
