"""authorization/options 的授权候选过滤回归：批量判权，N+1 消除 + 语义不变。

回归背景：routes.authorization_options 旧版对每个平台管理服务逐条
``can_use_service``（每条 2-3 次 DB 往返），服务一多列表接口线性变慢。
改为 ``usable_service_ids`` 批量后：builtin/个人/分组在内存判定，显式 principal
grant 一条查询取回。这里锁住三件事：

1. 过滤语义与旧版逐条判定一致（builtin 全放、个人自己的放、分组授权放、
   有 grant 的放、其余拒）；
2. 显式 grant 只发生**一条**查询，不随服务数增长；
3. 内置/个人/分组命中完全不发 grant 查询。
"""
from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import mcp_plugin_store as mps  # noqa: E402


def _svc(sid: int, *, builtin=False, user_id=None, group_ids=None) -> dict:
    return {
        "id": sid,
        "name": f"svc-{sid}",
        "builtin": builtin,
        "kind": "builtin" if builtin else "sse",
        "enabled": True,
        "user_id": user_id,
        "group_ids": [str(g) for g in (group_ids or [])],
    }


def _install_groups(monkeypatch, group_ids):
    async def _fake_groups(user_id):
        return [str(g) for g in group_ids]

    monkeypatch.setattr(mps, "_user_group_ids", _fake_groups)


def _install_grant_query(monkeypatch):
    """替换 _granted_service_ids：记录调用次数，返回空集（grant 不命中）。"""
    calls: dict[str, object] = {}

    async def _fake_granted(user_id, service_ids):
        calls["count"] = calls.get("count", 0) + 1
        calls["last_ids"] = list(service_ids)
        return set()

    monkeypatch.setattr(mps, "_granted_service_ids", _fake_granted)
    return calls


def test_usable_service_ids_semantics(monkeypatch):
    """builtin 全放、自己的放、分组授权放、其余交 grant 查询后拒掉。"""
    _install_groups(monkeypatch, ["g1"])
    grant_calls = _install_grant_query(monkeypatch)
    services = [
        _svc(1, builtin=True),               # 内置：放
        _svc(2, user_id="user-1"),            # 自己的：放
        _svc(3, group_ids=["g1"]),           # 分组授权：放
        _svc(4),                             # 平台服务，无 grant：拒
        _svc(5, user_id="user-2"),            # 他人的个人服务：grant 查后拒（与旧语义一致）
    ]
    usable = asyncio.run(mps.usable_service_ids(user_id="user-1", services=services))
    assert usable == {1, 2, 3}
    # grant 查询只发一条，覆盖剩下的平台服务与他人个人服务（后者自然查不到授权）。
    assert grant_calls["last_ids"] == [4, 5]
    assert grant_calls["count"] == 1


def test_usable_service_ids_grant_hits(monkeypatch):
    """显式 principal grant 命中的平台服务也放行。"""
    _install_groups(monkeypatch, [])

    async def _fake_granted(user_id, service_ids):
        return {4}

    monkeypatch.setattr(mps, "_granted_service_ids", _fake_granted)
    services = [_svc(4), _svc(5)]
    usable = asyncio.run(mps.usable_service_ids(user_id="user-1", services=services))
    assert usable == {4}


def test_usable_service_ids_admin_sees_all(monkeypatch):
    """user_id=None（管理员）全量放行，零查询。"""
    grant_calls = _install_grant_query(monkeypatch)
    services = [_svc(1), _svc(2), _svc(3)]
    usable = asyncio.run(mps.usable_service_ids(user_id=None, services=services))
    assert usable == {1, 2, 3}
    assert grant_calls == {}


def test_usable_service_ids_fixed_query_count(monkeypatch):
    """服务数增长时 grant 查询仍只有一条（N+1 消除的直接断言）。"""
    _install_groups(monkeypatch, [])
    grant_calls = _install_grant_query(monkeypatch)
    services = [_svc(i) for i in range(1, 41)]
    asyncio.run(mps.usable_service_ids(user_id="user-1", services=services))
    assert grant_calls["count"] == 1
    assert len(grant_calls["last_ids"]) == 40


def test_usable_service_ids_no_grant_query_for_owned(monkeypatch):
    """builtin/个人/分组全命中时完全不发 grant 查询。"""
    _install_groups(monkeypatch, ["g1"])
    grant_calls = _install_grant_query(monkeypatch)
    services = [
        _svc(1, builtin=True),
        _svc(2, user_id="user-1"),
        _svc(3, group_ids=["g1"]),
    ]
    usable = asyncio.run(mps.usable_service_ids(user_id="user-1", services=services))
    assert usable == {1, 2, 3}
    assert grant_calls == {}
