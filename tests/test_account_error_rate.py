"""账号级错误率单元测试。

验证：
1. 非流式成功也计入分母（修复分母不对称）。
2. 60 秒时间窗口过期后错误率归零（修复慢衰减）。
3. 账号级隔离：A 的失败不影响 B。
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rate_limiter import ModelClientPool, AccountClient


def _make_account(username="u1"):
    prov = type("P", (), {"PROVIDER_NAME": "p1", "username": username})()
    return AccountClient(provider=prov, rpm_limit=0, priority=0, weight=1)


def test_error_rate_zero_on_no_data():
    acc = _make_account()
    assert ModelClientPool._account_error_rate(acc) == 0.0


def test_non_stream_success_counts_in_denominator():
    """非流式成功 5 次 + 失败 1 次 → error_rate = 1/6（修复前因非流式不计分母会算成 1.0）。"""
    acc = _make_account()
    now = time.time()
    for _ in range(5):
        acc._stats.record_success(now)
    acc._stats.record_failure(now, account_level=False, threshold=2, base=15, step=15, max_seconds=120)
    rate = ModelClientPool._account_error_rate(acc)
    assert abs(rate - 1 / 6) < 1e-6


def test_error_rate_expires_after_window():
    """失败后推进超过窗口 → error_rate 归零。"""
    acc = _make_account()
    now = time.time()
    acc._stats.record_failure(now, account_level=False, threshold=2, base=15, step=15, max_seconds=120)
    assert ModelClientPool._account_error_rate(acc) == 1.0

    # 时间窗口自带过期：改用过期时间戳模拟窗口滑过
    acc._stats._failure = [now - (ModelClientPool.ERROR_WINDOW_SECONDS + 5)]
    assert ModelClientPool._account_error_rate(acc) == 0.0


def test_account_level_isolation():
    """A 失败不影响 B 的 error_rate。"""
    a = _make_account("A")
    b = _make_account("B")
    now = time.time()
    a._stats.record_failure(now, account_level=False, threshold=2, base=15, step=15, max_seconds=120)
    assert ModelClientPool._account_error_rate(a) == 1.0
    assert ModelClientPool._account_error_rate(b) == 0.0


def test_record_account_success_appends_timestamp(monkeypatch):
    acc = _make_account("u1")
    pool = type("Pool", (), {"clients": [acc]})()
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool})
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, p: True))
    ModelClientPool.record_account_success("p1", "u1")
    assert len(acc._stats._success) == 1


def test_record_account_failure_appends_timestamp(monkeypatch):
    acc = _make_account("u1")
    pool = type("Pool", (), {"clients": [acc]})()
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": pool})
    monkeypatch.setattr(ModelClientPool, "_score_enabled", classmethod(lambda cls, p: True))
    ModelClientPool.record_account_failure("p1", "u1", "boom")
    assert len(acc._stats._failure) == 1
