"""估算兜底不得覆盖上游真实 usage 分量的回归测试。

历史 bug：流式出口兜底走 `merge_usage(full_usage, estimated_usage)`，而 merge_usage
的语义是「incoming 覆盖 current」。上游给了真实 `{prompt: 12345, completion: 0}`
（eaichat 这类不发 completion 的渠道）时，估算出的 `prompt_tokens=1` 会把真实的
12345 覆盖掉，total 随之崩成个位数——表象就是「usage 好像没记录到」。

修复后：兜底走 `fill_usage_with_estimate`，真实值优先、估算只补为 0 的分量。
"""

from usage_utils import fill_usage_with_estimate, merge_usage, normalize_usage


def test_estimate_does_not_override_real_prompt_tokens():
    """核心回归：真实 prompt 必须留住，completion 由估算补。"""
    real = {"prompt_tokens": 12345, "completion_tokens": 0, "total_tokens": 12345}
    estimated = {"prompt_tokens": 1, "completion_tokens": 4, "total_tokens": 5}

    filled = fill_usage_with_estimate(real, estimated)

    assert filled["prompt_tokens"] == 12345, filled
    assert filled["completion_tokens"] == 4, filled
    assert filled["total_tokens"] == 12349, filled


def test_merge_usage_direction_would_have_regressed():
    """钉住 bug 成因：旧写法确实会把真实 prompt 覆盖成估算值。

    这条不是要求 merge_usage 改行为——它的「incoming 覆盖 current」语义对累计型
    上游是正确的（每帧全量递增，后到的帧就是更新的真值），别处 20 余处调用都依赖它。
    这里只断言「为什么兜底不能用它」，防止有人图省事换回 merge_usage。
    """
    real = {"prompt_tokens": 12345, "completion_tokens": 0, "total_tokens": 12345}
    estimated = {"prompt_tokens": 1, "completion_tokens": 4, "total_tokens": 5}

    regressed = normalize_usage(merge_usage(real, estimated))

    assert regressed["prompt_tokens"] == 1, regressed
    assert regressed["total_tokens"] == 5, regressed


def test_estimate_fills_everything_when_upstream_sends_no_usage():
    """上游完全没 usage（eaichat 常态）时，全部分量走估算。"""
    estimated = {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}

    filled = fill_usage_with_estimate(None, estimated)

    assert filled["prompt_tokens"] == 100, filled
    assert filled["completion_tokens"] == 20, filled
    assert filled["total_tokens"] == 120, filled


def test_real_usage_wins_entirely_when_complete():
    """上游 usage 完整时，估算一个分量都不该掺进来。"""
    real = {"prompt_tokens": 500, "completion_tokens": 60, "total_tokens": 560}
    estimated = {"prompt_tokens": 7, "completion_tokens": 8, "total_tokens": 15}

    filled = fill_usage_with_estimate(real, estimated)

    assert filled["prompt_tokens"] == 500, filled
    assert filled["completion_tokens"] == 60, filled
    assert filled["total_tokens"] == 560, filled


def test_cross_protocol_key_names_do_not_pick_estimate():
    """上游 Anthropic 风格 input_tokens + 估算 OpenAI 风格 prompt_tokens。

    裸字典合并会让两个键并存，normalize_usage 优先取 prompt_tokens（估算值）而选错；
    fill_usage_with_estimate 先各自归一再按分量取值，绕开键名歧义。
    """
    real = {"input_tokens": 9000, "output_tokens": 0}
    estimated = {"prompt_tokens": 3, "completion_tokens": 11}

    filled = fill_usage_with_estimate(real, estimated)

    assert filled["prompt_tokens"] == 9000, filled
    assert filled["completion_tokens"] == 11, filled


def test_cache_components_are_not_double_counted():
    """带缓存明细的真实 usage 过一遍兜底后，总输入口径不能被二次加回。

    口径见 feedback_token_convention：total = 总输入 + 输出，缓存读/写是输入明细，
    不额外计入 total。兜底函数内部会再 normalize 一次，必须幂等。
    """
    real = {
        "prompt_tokens": 1000,
        "completion_tokens": 0,
        "prompt_tokens_details": {"cached_tokens": 400},
    }
    estimated = {"prompt_tokens": 5, "completion_tokens": 30}

    baseline = normalize_usage(real)
    filled = fill_usage_with_estimate(real, estimated)

    # 总输入不变（缓存是 1000 里的明细，不再加回），只补上 completion
    assert filled["prompt_tokens"] == baseline["prompt_tokens"] == 1000, (filled, baseline)
    assert filled["cached_tokens"] == 400, filled
    assert filled["completion_tokens"] == 30, filled
    assert filled["total_tokens"] == 1030, filled


def test_fill_is_idempotent():
    """兜底结果再过一次兜底，数值不漂移。"""
    real = {"prompt_tokens": 800, "completion_tokens": 0,
            "prompt_tokens_details": {"cached_tokens": 300}}
    estimated = {"prompt_tokens": 5, "completion_tokens": 40}

    once = fill_usage_with_estimate(real, estimated)
    twice = fill_usage_with_estimate(once, estimated)

    assert once == twice, (once, twice)
