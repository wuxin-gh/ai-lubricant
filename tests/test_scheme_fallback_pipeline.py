"""方案降级在请求编排层的端到端遍历：主组候选耗尽后，按派生的方案组条目顺序降级，
全耗尽再走用户配的 backup，最终 429。

不改 pipeline 源码，只用 monkeypatch 让 resolve_model_group 返回「主组 → 派生条目链 →
用户 backup」的解析结果，断言 _run_group_fallback_pipeline 按序遍历——与 catalog 展开出
的链形状一致（见 test_model_catalog_scheme_fallback）。
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main
from model_catalog import SCHEME_GROUP_SEP
from rate_limiter import NoAvailableAccountError


def test_pipeline_walks_scheme_fallback_chain_then_user_backup(monkeypatch):
    snapshot = type("Snapshot", (), {"generation": 3})()
    attempted = []

    d1 = f"opus{SCHEME_GROUP_SEP}1"
    d2 = f"opus{SCHEME_GROUP_SEP}2"
    # 与 catalog 展开一致：主组 → 方案派生条目 d1 → d2 → 用户 backup(haiku) → 空。
    chain = {
        "opus": {"name": "opus", "backup_group": d1},
        d1: {"name": d1, "backup_group": d2},
        d2: {"name": d2, "backup_group": "haiku"},
        "haiku": {"name": "haiku", "backup_group": ""},
    }

    async def resolve(cls, model, snapshot=None):
        return chain.get(model)

    async def response_model(cls, model, snapshot=None):
        return "opus"  # 降级后对外身份仍是主组

    async def exhausted(model, *args, **kwargs):
        attempted.append((model, kwargs.get("_stable_response_model")))
        raise NoAvailableAccountError(status_code=429, detail="exhausted")
        yield

    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(response_model))
    monkeypatch.setattr(main, "_chat_with_retry_for_model", exhausted)

    async def collect():
        with pytest.raises(NoAvailableAccountError):
            async for _ in main._chat_with_retry("opus", [], False):
                pass

    asyncio.run(collect())
    # 按序尝试：主组 → d1 → d2 → 用户 backup(haiku)，每跳对外身份都稳定为 opus。
    assert attempted == [
        ("opus", "opus"),
        (d1, "opus"),
        (d2, "opus"),
        ("haiku", "opus"),
    ]


def test_pipeline_stops_at_scheme_tail_when_no_user_backup(monkeypatch):
    snapshot = type("Snapshot", (), {"generation": 5})()
    attempted = []
    d1 = f"opus{SCHEME_GROUP_SEP}1"
    chain = {
        "opus": {"name": "opus", "backup_group": d1},
        d1: {"name": d1, "backup_group": ""},  # 链尾无用户 backup
    }

    async def resolve(cls, model, snapshot=None):
        return chain.get(model)

    async def response_model(cls, model, snapshot=None):
        return "opus"

    async def exhausted(model, *args, **kwargs):
        attempted.append(model)
        raise NoAvailableAccountError(status_code=429, detail="exhausted")
        yield

    monkeypatch.setattr(main.model_catalog, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(main.config.Config, "resolve_model_group", classmethod(resolve))
    monkeypatch.setattr(main.config.Config, "get_model_group_response_model", classmethod(response_model))
    monkeypatch.setattr(main, "_chat_with_retry_for_model", exhausted)

    async def collect():
        with pytest.raises(NoAvailableAccountError):
            async for _ in main._chat_with_retry("opus", [], False):
                pass

    asyncio.run(collect())
    assert attempted == ["opus", d1]
