import os
import sys

import pytest
from fastapi import HTTPException

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import agent.api as agent_api


class _FakeRows:
    """内存版 db 查询：按 (id, user_id) / (id) / 分组授权三条路径分别命中。"""

    def __init__(self, by_id, group_root_ids_for_caller):
        self._by_id = by_id
        self._group_root_ids = set(group_root_ids_for_caller)

    async def get_api_key_by_id_for_user(self, key_id, user_id):
        row = self._by_id.get(key_id)
        if row and str(row.get("user_id") or "") == str(user_id):
            return dict(row)
        return None

    async def get_api_key_by_id(self, key_id):
        row = self._by_id.get(key_id)
        return dict(row) if row else None

    def group_system_resolver(self):
        async def _resolve(api_key_id, caller):
            if api_key_id in self._group_root_ids:
                return dict(self._by_id[api_key_id])
            return None
        return _resolve


def _patch_db(monkeypatch, fake):
    import db as db_mod
    monkeypatch.setattr(db_mod.PostgresClient, "get_api_key_by_id_for_user", fake.get_api_key_by_id_for_user)
    monkeypatch.setattr(db_mod.PostgresClient, "get_api_key_by_id", fake.get_api_key_by_id)
    monkeypatch.setattr(agent_api, "_resolve_group_system_key", fake.group_system_resolver())


@pytest.mark.asyncio
async def test_resolve_caller_derived_key_via_group_system_root(monkeypatch):
    """任务子 Key（user_id NULL，派生自分组系统根 Key）应通过根 Key 授权放行。"""
    root = {"id": 100, "key": "sk-root", "user_id": None, "parent_id": None, "disabled": False}
    child = {"id": 7, "key": "sk-child", "user_id": None, "parent_id": 100, "disabled": False}
    fake = _FakeRows({100: root, 7: child}, group_root_ids_for_caller=[100])
    _patch_db(monkeypatch, fake)

    row = await agent_api._resolve_caller_api_key(7, "user-1")
    assert row["key"] == "sk-child"
    assert row["id"] == 7


@pytest.mark.asyncio
async def test_resolve_caller_derived_key_via_own_root(monkeypatch):
    """派生自个人根 Key 的子 Key：根 user_id 命中调用者即放行。"""
    root = {"id": 200, "key": "sk-own", "user_id": "user-1", "parent_id": None, "disabled": False}
    child = {"id": 8, "key": "sk-child2", "user_id": "user-1", "parent_id": 200, "disabled": False}
    # user_id 已复制到子，owner 路径本就命中；此处验证不会因为多走一遍派生解析而错。
    fake = _FakeRows({200: root, 8: child}, group_root_ids_for_caller=[])
    _patch_db(monkeypatch, fake)

    row = await agent_api._resolve_caller_api_key(8, "user-1")
    assert row["id"] == 8


@pytest.mark.asyncio
async def test_resolve_caller_derived_key_rejects_unauthorized_root(monkeypatch):
    """根 Key 既不属调用者、也不在分组授权内 → 仍 403（不放宽授权语义）。"""
    root = {"id": 300, "key": "sk-other", "user_id": "user-2", "parent_id": None, "disabled": False}
    child = {"id": 9, "key": "sk-child3", "user_id": "user-2", "parent_id": 300, "disabled": False}
    fake = _FakeRows({300: root, 9: child}, group_root_ids_for_caller=[])
    _patch_db(monkeypatch, fake)

    with pytest.raises(HTTPException) as exc:
        await agent_api._resolve_caller_api_key(9, "user-1")
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_resolve_caller_derived_key_respects_child_disabled(monkeypatch):
    """根 Key 已授权，但子 Key 自身 disabled → 403「该 API Key 已禁用」。"""
    root = {"id": 100, "key": "sk-root", "user_id": None, "parent_id": None, "disabled": False}
    child = {"id": 7, "key": "sk-child", "user_id": None, "parent_id": 100, "disabled": True}
    fake = _FakeRows({100: root, 7: child}, group_root_ids_for_caller=[100])
    _patch_db(monkeypatch, fake)

    with pytest.raises(HTTPException) as exc:
        await agent_api._resolve_caller_api_key(7, "user-1")
    assert exc.value.status_code == 403
    assert "禁用" in exc.value.detail
