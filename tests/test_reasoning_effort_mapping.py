"""回归测试：reasoning_effort / thinking 在跨协议与按协议注入时，
"关闭思考"必须映射为上游可接受的值，不得再产出 OpenAI 已弃用/不接受的 `minimal`。

背景：gpt-5.5 等上游仅接受 `none/low/medium/high/xhigh`，旧实现把"关闭思考"映射成
`minimal`（OpenAI 早期 responses API 的取值），导致 422 upstream_error。
修复后统一映射为 `none`，同时仍兼容客户端可能传来的历史 `minimal`。
"""
from main import _convert_thinking_for_protocol, _inject_thinking_for_protocol


# 受 gpt-5.5 等上游拒绝的旧值，修复后任何路径都不应再产出它
DISALLOWED_EFFORTS = ("minimal",)

ALLOWED_OPENAI_EFFORTS = ("none", "low", "medium", "high", "xhigh")


class TestInjectThinkingForProtocol:
    """_inject_thinking_for_protocol：按 API key thinking 配置注入协议标准参数。"""

    def test_openai_none_mode_omits_effort(self):
        body = _inject_thinking_for_protocol({}, "none", 1024, "openai")
        assert "reasoning_effort" not in body

    def test_openai_auto_and_thinking_modes(self):
        assert _inject_thinking_for_protocol({}, "auto", 1024, "openai")["reasoning_effort"] == "medium"
        assert _inject_thinking_for_protocol({}, "thinking", 1024, "openai")["reasoning_effort"] == "high"

    def test_responses_none_mode_omits_reasoning(self):
        body = _inject_thinking_for_protocol({}, "none", 1024, "responses")
        assert "reasoning" not in body

    def test_anthropic_none_mode_disables_thinking(self):
        body = _inject_thinking_for_protocol({}, "none", 1024, "anthropic")
        assert body["thinking"] == {"type": "disabled"}

    def test_no_minimal_ever_injected(self):
        for protocol in ("openai", "responses", "anthropic"):
            for mode in ("none", "auto", "thinking"):
                body = _inject_thinking_for_protocol({}, mode, 1024, protocol)
                rendered = repr(body)
                assert "minimal" not in rendered, f"{protocol}/{mode} leaked minimal: {body}"


class TestConvertThinkingForProtocol:
    """_convert_thinking_for_protocol：跨协议 thinking 参数转换。"""

    def test_openai_none_to_openai_omits_effort(self):
        out = _convert_thinking_for_protocol({"reasoning_effort": "none"}, "openai", "openai")
        assert out == {}

    def test_legacy_minimal_input_treated_as_disabled_to_openai(self):
        # 历史客户端可能仍传 minimal，必须规范化为 none，不原样回传
        out = _convert_thinking_for_protocol({"reasoning_effort": "minimal"}, "openai", "openai")
        assert out == {}

    def test_openai_high_passes_through(self):
        out = _convert_thinking_for_protocol({"reasoning_effort": "high"}, "openai", "openai")
        assert out == {"reasoning_effort": "high"}

    def test_openai_disabled_to_responses_omits_reasoning(self):
        out = _convert_thinking_for_protocol({"reasoning_effort": "none"}, "openai", "responses")
        assert out == {}

    def test_openai_disabled_to_anthropic_disables_thinking(self):
        out = _convert_thinking_for_protocol({"reasoning_effort": "none"}, "openai", "anthropic")
        assert out == {"thinking": {"type": "disabled"}}

    def test_anthropic_disabled_to_openai_omits_effort(self):
        out = _convert_thinking_for_protocol({"thinking": {"type": "disabled"}}, "anthropic", "openai")
        assert out == {}

    def test_responses_minimal_to_openai_omits_effort(self):
        out = _convert_thinking_for_protocol({"reasoning": {"effort": "minimal"}}, "responses", "openai")
        assert out == {}

    def test_no_minimal_in_any_conversion_output(self):
        sources = [
            ("openai", {"reasoning_effort": v}) for v in ("none", "minimal", "low", "medium", "high", "xhigh")
        ] + [
            ("responses", {"reasoning": {"effort": v}}) for v in ("none", "minimal", "low", "medium", "high")
        ] + [
            ("anthropic", {"thinking": {"type": "disabled"}}),
            ("anthropic", {"thinking": {"type": "enabled", "budget_tokens": 1024}}),
        ]
        for src_proto, params in sources:
            for tgt_proto in ("openai", "responses", "anthropic"):
                out = _convert_thinking_for_protocol(params, src_proto, tgt_proto)
                rendered = repr(out)
                assert "minimal" not in rendered, (
                    f"{src_proto}->{tgt_proto} with {params} leaked minimal: {out}"
                )

    def test_openai_effort_values_stay_within_allowed_set(self):
        for v in ("none", "low", "medium", "high", "xhigh"):
            out = _convert_thinking_for_protocol({"reasoning_effort": v}, "openai", "openai")
            if v in ("none", "minimal"):
                assert out == {}
            else:
                assert out["reasoning_effort"] in ALLOWED_OPENAI_EFFORTS


class TestAdaptiveAndOutputConfig:
    """thinking.type=adaptive 与 output_config.effort 的处理。

    回归：旧实现只认 thinking.type==enabled，把 adaptive 当成关闭，
    且完全没有读取 output_config.effort，导致
    {thinking:{type:adaptive}, output_config:{effort:xhigh}} 被转成
    reasoning_effort:none。
    """

    def test_anthropic_adaptive_to_openai_uses_output_config_effort(self):
        out = _convert_thinking_for_protocol(
            {"thinking": {"type": "adaptive"}, "output_config": {"effort": "xhigh"}},
            "anthropic", "openai",
        )
        assert out == {"reasoning_effort": "xhigh"}

    def test_anthropic_adaptive_to_openai_defaults_to_high_without_output_config(self):
        out = _convert_thinking_for_protocol(
            {"thinking": {"type": "adaptive"}}, "anthropic", "openai",
        )
        assert out == {"reasoning_effort": "high"}

    def test_anthropic_adaptive_to_anthropic_preserves_effort(self):
        out = _convert_thinking_for_protocol(
            {"thinking": {"type": "adaptive"}, "output_config": {"effort": "medium"}},
            "anthropic", "anthropic",
        )
        assert out.get("output_config") == {"effort": "medium"}
        assert out.get("thinking", {}).get("type") == "enabled"

    def test_anthropic_max_effort_maps_to_openai_xhigh(self):
        out = _convert_thinking_for_protocol(
            {"thinking": {"type": "enabled", "budget_tokens": 1024}, "output_config": {"effort": "max"}},
            "anthropic", "openai",
        )
        assert out == {"reasoning_effort": "xhigh"}

    def test_openai_xhigh_maps_to_anthropic_max(self):
        out = _convert_thinking_for_protocol(
            {"reasoning_effort": "xhigh"}, "openai", "anthropic",
        )
        assert out.get("output_config") == {"effort": "max"}
        assert out.get("thinking", {}).get("type") == "enabled"

    def test_openai_xhigh_passes_through_to_openai(self):
        out = _convert_thinking_for_protocol(
            {"reasoning_effort": "xhigh"}, "openai", "openai",
        )
        assert out == {"reasoning_effort": "xhigh"}

    def test_anthropic_disabled_ignores_output_config(self):
        out = _convert_thinking_for_protocol(
            {"thinking": {"type": "disabled"}, "output_config": {"effort": "high"}},
            "anthropic", "openai",
        )
        assert out == {}


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
