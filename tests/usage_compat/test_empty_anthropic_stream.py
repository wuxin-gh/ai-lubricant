"""Regression coverage for empty Anthropic-compatible streams."""

import pytest

from main import _chunk_has_stream_content, _validate_stream_completion, IncompleteStreamError


def test_empty_anthropic_structural_trace_is_not_content():
    """Structural Anthropic events with zero output must not count as content."""
    empty_trace = (
        'event: message_start\n'
        'data: {"type":"message_start","message":{"content":[],"usage":{"input_tokens":0,"output_tokens":0}}}\n\n'
        'event: ping\n'
        'data: {"type":"ping"}\n\n'
        'event: content_block_start\n'
        'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n'
        'event: content_block_stop\n'
        'data: {"type":"content_block_stop","index":0}\n\n'
        'event: message_delta\n'
        'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":0,"output_tokens":0}}\n\n'
        'event: message_stop\n'
        'data: {"type":"message_stop"}\n\n'
    )

    assert not _chunk_has_stream_content(empty_trace)
    with pytest.raises(IncompleteStreamError):
        _validate_stream_completion(None, had_content=False)


def test_anthropic_non_empty_blocks_remain_content():
    assert _chunk_has_stream_content(
        'event: content_block_start\n'
        'data: {"type":"content_block_start","content_block":{"type":"text","text":"hello"}}\n\n'
    )
    assert _chunk_has_stream_content(
        'event: content_block_start\n'
        'data: {"type":"content_block_start","content_block":{"type":"tool_use","name":"lookup"}}\n\n'
    )
