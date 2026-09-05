"""request_state 纯数据对象的离线单元测试（零 IO，无需 PG/Redis）。"""

from request_state import CandidateKey, RequestContext, AttemptContext


def test_candidate_key_account_scope():
    k = CandidateKey(provider="p", account="a", model="m")
    assert k.account_scope == ("p", "a")
    # frozen + slots：可作 dict/set key
    assert {k: 1}[CandidateKey("p", "a", "m")] == 1


def test_request_context_defaults_have_no_model_state():
    ctx = RequestContext(
        request_id="r1",
        original_body={"model": "x", "messages": []},
        original_model="x",
        messages=[],
    )
    # 请求级不含当前渠道/账号/模型状态
    assert not hasattr(ctx, "provider")
    assert not hasattr(ctx, "upstream_model_id")
    assert ctx.downstream_started is False


def test_attempt_context_carries_model_identity():
    a = AttemptContext(attempt_no=1, candidate_key=CandidateKey("p", "a", "m"))
    a.public_model_id = "pub"
    a.upstream_model_id = "up"
    a.response_model = "resp"
    assert (a.public_model_id, a.upstream_model_id, a.response_model) == ("pub", "up", "resp")
    assert a.upstream_started is False and a.downstream_started is False
