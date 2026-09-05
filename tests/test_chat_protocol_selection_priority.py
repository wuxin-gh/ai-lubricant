"""协议行选择优先级：勾选模型 > 协议直通。

勾选模型代表管理员显式把该模型绑定到这条链路，必须优先于"协议与请求一致"的
直通行。协议只在"是否绑定模型"相同的档内做次级排序。

覆盖两层——单独任一层漏改都会让绑定行被直通行顶掉：
1. Channel 层协议行排序（渠道取一次、账号复用）；
2. ModelClientPool 候选偏好（此前会把非同协议候选整体删掉）。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from channel import Channel
from rate_limiter import ModelClientPool


def _channel(rows: list[dict]) -> Channel:
    return Channel("p1", {"base_url": "https://example.invalid", "chat_protocols": rows})


def test_channel_ranks_model_bound_row_above_same_protocol_passthrough():
    ch = _channel([
        {"id": "responses-passthrough", "protocol": "responses", "path": "/p", "models": []},
        {"id": "anthropic-bound", "protocol": "anthropic", "path": "/b", "models": ["m1"]},
    ])

    candidates = ch.get_chat_protocol_candidates("m1", "responses")

    assert [c["id"] for c in candidates] == ["anthropic-bound", "responses-passthrough"]
    assert ch.select_chat_protocol("m1", "responses")["id"] == "anthropic-bound"


def test_channel_full_four_tier_order():
    ch = _channel([
        {"id": "openai-passthrough", "protocol": "openai", "path": "/op", "models": []},
        {"id": "anthropic-bound", "protocol": "anthropic", "path": "/ab", "models": ["m1"]},
        {"id": "openai-bound", "protocol": "openai", "path": "/ob", "models": ["m1"]},
        {"id": "anthropic-passthrough", "protocol": "anthropic", "path": "/ap", "models": []},
    ])

    candidates = ch.get_chat_protocol_candidates("m1", "openai")

    assert [c["id"] for c in candidates] == [
        "openai-bound",          # 绑定 + 同协议
        "anthropic-bound",       # 绑定 + 异协议
        "openai-passthrough",    # 未绑定 + 同协议
        "anthropic-passthrough", # 未绑定 + 异协议
    ]


def test_channel_unbound_model_still_prefers_same_protocol():
    """模型未被任何行勾选时，退回原有的同协议优先行为。"""
    ch = _channel([
        {"id": "anthropic-passthrough", "protocol": "anthropic", "path": "/ap", "models": []},
        {"id": "openai-passthrough", "protocol": "openai", "path": "/op", "models": []},
    ])

    candidates = ch.get_chat_protocol_candidates("m9", "openai")

    assert [c["id"] for c in candidates] == ["openai-passthrough", "anthropic-passthrough"]


def test_channel_disabled_and_unmatched_rows_are_filtered_out():
    ch = _channel([
        {"id": "bound-disabled", "protocol": "openai", "path": "/d", "models": ["m1"], "enabled": False},
        {"id": "other-model", "protocol": "openai", "path": "/o", "models": ["m2"]},
        {"id": "anthropic-bound", "protocol": "anthropic", "path": "/b", "models": ["m1"]},
    ])

    candidates = ch.get_chat_protocol_candidates("m1", "openai")

    assert [c["id"] for c in candidates] == ["anthropic-bound"]


def _candidate(cid: str, *, bound: bool, same: bool) -> dict:
    return {
        "provider_name": "p1",
        "route_info": {
            "endpoint_config_id": cid,
            "endpoint_model_bound": bound,
            "same_protocol": same,
        },
    }


def _ids(candidates: list[dict]) -> list[str]:
    return [(c.get("route_info") or {}).get("endpoint_config_id") for c in candidates]


def test_pool_keeps_model_bound_candidate_over_same_protocol_candidate():
    """回归：此前只留 same_protocol 候选，绑定模型的异协议候选会被整体删掉。"""
    candidates = [
        _candidate("responses-passthrough", bound=False, same=True),
        _candidate("anthropic-bound", bound=True, same=False),
    ]

    preferred = ModelClientPool._prefer_same_protocol_candidates(candidates, "responses")

    assert _ids(preferred) == ["anthropic-bound"]


def test_pool_prefers_bound_and_same_protocol_when_both_exist():
    candidates = [
        _candidate("anthropic-bound", bound=True, same=False),
        _candidate("responses-bound", bound=True, same=True),
        _candidate("responses-passthrough", bound=False, same=True),
    ]

    preferred = ModelClientPool._prefer_same_protocol_candidates(candidates, "responses")

    assert _ids(preferred) == ["responses-bound"]


def test_pool_falls_back_to_same_protocol_when_no_bound_candidate():
    candidates = [
        _candidate("anthropic-passthrough", bound=False, same=False),
        _candidate("responses-passthrough", bound=False, same=True),
    ]

    preferred = ModelClientPool._prefer_same_protocol_candidates(candidates, "responses")

    assert _ids(preferred) == ["responses-passthrough"]


def test_pool_prefers_bound_candidate_even_without_request_protocol():
    candidates = [
        _candidate("passthrough", bound=False, same=False),
        _candidate("bound", bound=True, same=False),
    ]

    preferred = ModelClientPool._prefer_same_protocol_candidates(candidates, None)

    assert _ids(preferred) == ["bound"]


def test_pool_returns_all_candidates_when_nothing_bound_and_no_protocol():
    candidates = [
        _candidate("a", bound=False, same=False),
        _candidate("b", bound=False, same=False),
    ]

    preferred = ModelClientPool._prefer_same_protocol_candidates(candidates, None)

    assert _ids(preferred) == ["a", "b"]
