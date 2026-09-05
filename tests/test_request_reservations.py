"""request_reservations 幂等性离线测试（fake account client / RateLimiter，无 Redis/PG）。"""

import asyncio

import request_reservations as rr


class FakeAccount:
    def __init__(self):
        self.release_calls = 0

    async def release(self):
        self.release_calls += 1


def test_candidate_reservation_release_is_idempotent():
    acct = FakeAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.release())
    asyncio.run(res.release())
    asyncio.run(res.rollback())
    assert acct.release_calls == 1
    assert res.released is True


def test_candidate_reservation_rollback_then_release_single_release():
    acct = FakeAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.rollback())
    asyncio.run(res.release())
    assert acct.release_calls == 1
    assert res.rolled_back is True
    assert res.released is True


def test_candidate_reservation_commit_does_not_release():
    acct = FakeAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.commit())
    assert res.committed is True
    assert acct.release_calls == 0
    # commit 后仍需显式 release 并发
    asyncio.run(res.release())
    assert acct.release_calls == 1


# ==================== 计量结算：成功 reconcile / 失败 rollback ====================

class _FakeLease:
    def __init__(self):
        self.settled = False
        self.rolled_back = False


class _LeaseAccount(FakeAccount):
    """带 current_limit_lease 的账号 stub（对齐 AccountClient 新增方法）。"""

    def __init__(self):
        super().__init__()
        self.lease = _FakeLease()

    def current_limit_lease(self):
        return self.lease


def _patch_manager(monkeypatch):
    calls = {"reconcile": [], "rollback": []}

    class FakeManager:
        @staticmethod
        async def reconcile_account_reservation(lease, actual_tokens):
            calls["reconcile"].append(actual_tokens)
            lease.settled = True

        @staticmethod
        async def rollback_account_reservation(lease):
            calls["rollback"].append(True)
            lease.rolled_back = True

    import limits.manager as lm
    monkeypatch.setattr(lm, "LimitManager", FakeManager)
    return calls


def test_commit_reconciles_actual_usage_once(monkeypatch):
    calls = _patch_manager(monkeypatch)
    acct = _LeaseAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.commit({"total_tokens": 321}))
    # 成功用真实 usage 结算一次
    assert calls["reconcile"] == [321]
    assert res.settled is True
    # 重复 commit 幂等，不重复结算
    asyncio.run(res.commit({"total_tokens": 321}))
    assert calls["reconcile"] == [321]
    # 已结算不回滚
    asyncio.run(res.rollback())
    assert calls["rollback"] == []


def test_rollback_rolls_back_ledger_and_releases(monkeypatch):
    calls = _patch_manager(monkeypatch)
    acct = _LeaseAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.rollback())
    assert calls["rollback"] == [True]
    assert calls["reconcile"] == []
    assert acct.release_calls == 1
    # 重复回滚幂等
    asyncio.run(res.rollback())
    assert calls["rollback"] == [True]
    assert acct.release_calls == 1
    # 已回滚后 commit 不再结算
    asyncio.run(res.commit({"total_tokens": 10}))
    assert calls["reconcile"] == []


def test_keep_served_usage_settles_without_rollback(monkeypatch):
    """流式已产出后中断（STREAM_ERROR_AND_STOP）：按已产出 usage 结算，绝不回滚。"""
    calls = _patch_manager(monkeypatch)
    acct = _LeaseAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.keep_served_usage({"total_tokens": 88}))
    assert calls["reconcile"] == [88]
    assert calls["rollback"] == []
    assert res.settled is True
    # finally 里走 settled 分支 → 只释放并发
    asyncio.run(res.release())
    assert acct.release_calls == 1


def test_settle_without_usage_counts_zero_tokens(monkeypatch):
    """媒体/TTS 成功：无 token usage，结算 0（请求数预占保留）。"""
    calls = _patch_manager(monkeypatch)
    acct = _LeaseAccount()
    res = rr.CandidateReservation(provider_name="p", account_client=acct)
    asyncio.run(res.commit({"total_tokens": 0}))
    assert calls["reconcile"] == [0]


def test_api_key_reservation_disabled_is_noop():
    res = asyncio.run(rr.ApiKeyReservation.acquire("sk-x", enabled=False))
    assert res.api_key is None
    asyncio.run(res.release())
    asyncio.run(res.release())
    assert res.released is True


def test_api_key_reservation_release_idempotent(monkeypatch):
    released = []

    class FakeRL:
        @staticmethod
        async def acquire(api_key, client_ip=None):
            return {"lease": api_key}

        @staticmethod
        async def release(lease):
            released.append(lease)

    import rate_limiter
    monkeypatch.setattr(rate_limiter, "RateLimiter", FakeRL)

    res = asyncio.run(rr.ApiKeyReservation.acquire("sk-x", enabled=True))
    assert res.lease == {"lease": "sk-x"}
    asyncio.run(res.release())
    asyncio.run(res.release())
    assert released == [{"lease": "sk-x"}]


def test_api_key_reservation_forwards_client_ip(monkeypatch):
    acquired = []

    class FakeRL:
        @staticmethod
        async def acquire(api_key, client_ip=None):
            acquired.append((api_key, client_ip))
            return {"lease": api_key}

        @staticmethod
        async def release(_lease):
            return None

    import rate_limiter
    monkeypatch.setattr(rate_limiter, "RateLimiter", FakeRL)

    asyncio.run(rr.ApiKeyReservation.acquire("sk-x", enabled=True, client_ip="10.0.0.1"))
    assert acquired == [("sk-x", "10.0.0.1")]
