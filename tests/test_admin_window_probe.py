from admin import _parse_window_tokens


def test_parse_window_tokens_presets_and_custom_values():
    assert _parse_window_tokens("8k") == (8000, None)
    assert _parse_window_tokens("128K") == (128000, None)
    assert _parse_window_tokens("1m") == (1_000_000, None)
    assert _parse_window_tokens("32768") == (32768, None)
    assert _parse_window_tokens(200000) == (200000, None)
    assert _parse_window_tokens("1,000,000") == (1_000_000, None)


def test_parse_window_tokens_rejects_invalid_values():
    for value in (None, "", "abc", "1.5m", 0, 999, 1_000_001, "1001k"):
        tokens, error = _parse_window_tokens(value)
        assert tokens is None
        assert error
