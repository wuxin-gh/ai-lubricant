"""``POST /v2/references/from-resource`` 路由回归：按 resource_id 直引已发布池行。

榜单条目本身就是统一资源池的 published ``resources`` 行（消费侧 ``item.id`` 即
行 id）——直引不应再走 GitHub 识别：``from-github-v2`` 会把同仓库再落一行
``github_recognize``，产生重复池行。handler 直调（不挂 TestClient）：鉴权依赖
由调用方保证，这里只钉业务语义（存在性/发布态校验、字段透传、错误映射）。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import resource_store
from user_platform import routes_resources


def test_from_resource_references_published_row(monkeypatch):
    """已发布资源 → create_reference 收到池行字段；返回信封带引用。"""
    resource = {
        "id": 7,
        "status": "published",
        "name": "skills",
        "display_name": "技能集",
        "description": "集合描述",
        "version": "v1",
    }
    calls: dict = {}

    async def fake_get(rid):
        return resource if rid == 7 else None

    async def fake_create(**kwargs):
        calls.update(kwargs)
        return {"id": "ref-1", "resource_id": kwargs["resource_id"]}

    monkeypatch.setattr(resource_store, "get_resource", fake_get)
    monkeypatch.setattr(resource_store, "create_reference", fake_create)

    resp = asyncio.run(
        routes_resources.create_reference_from_resource(
            {"resource_id": 7}, SimpleNamespace(id="user-1"), "team-uuid",
        )
    )

    assert resp["code"] == 0
    assert resp["message"] == "已引用"
    assert resp["data"] == {"id": "ref-1", "resource_id": 7}
    assert calls["team_id"] == "team-uuid"
    assert calls["resource_id"] == 7
    assert calls["display_name"] == "技能集"
    assert calls["description"] == "集合描述"
    assert calls["version"] == "v1"
    assert calls["created_by"] == "user-1"


def test_from_resource_rejects_non_numeric_id():
    """resource_id 非整数 → 400，不落到 store。"""
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            routes_resources.create_reference_from_resource(
                {"resource_id": "abc"}, SimpleNamespace(id="user-1"), "team-uuid",
            )
        )
    assert exc.value.status_code == 400


def test_from_resource_rejects_missing_resource(monkeypatch):
    """资源不存在 → 404。"""
    async def fake_get(_rid):
        return None

    monkeypatch.setattr(resource_store, "get_resource", fake_get)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            routes_resources.create_reference_from_resource(
                {"resource_id": 404}, SimpleNamespace(id="user-1"), "team-uuid",
            )
        )
    assert exc.value.status_code == 404


def test_from_resource_rejects_draft_resource(monkeypatch):
    """只有 published 行可直引——草稿（github_recognize 落池行等）拒绝。"""
    async def fake_get(_rid):
        return {"id": 5, "status": "draft"}

    monkeypatch.setattr(resource_store, "get_resource", fake_get)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            routes_resources.create_reference_from_resource(
                {"resource_id": 5}, SimpleNamespace(id="user-1"), "team-uuid",
            )
        )
    assert exc.value.status_code == 409


def test_from_resource_maps_store_valueerror_to_422(monkeypatch):
    """create_reference 的 ValueError（invalid_team_id 等）→ 422 明确报错。"""
    async def fake_get(_rid):
        return {"id": 9, "status": "published"}

    async def boom(**_kw):
        raise ValueError("invalid_team_id")

    monkeypatch.setattr(resource_store, "get_resource", fake_get)
    monkeypatch.setattr(resource_store, "create_reference", boom)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            routes_resources.create_reference_from_resource(
                {"resource_id": 9}, SimpleNamespace(id="user-1"), "bad-team",
            )
        )
    assert exc.value.status_code == 422
