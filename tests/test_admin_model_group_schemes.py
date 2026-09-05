"""管理端多套方案（schemes）校验：_validate_model_groups / _validate_model_group_schemes。

方案身份是 id（不是 name）：id 必填、组内唯一——重复拒绝；name 只是展示标签，允许为空、
允许重复、允许改名。每套方案 models 至少一个；白/黑名单只做标签名格式规范化（不按渠道存在性
丢弃，标签是运行时筛选维度）；active_scheme 若给出须命中某方案 id。
"""
import asyncio
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import admin
from rate_limiter import ModelClientPool


@pytest.fixture(autouse=True)
def _stub_providers(monkeypatch):
    async def get_providers(cls=None):
        return {"chan-a": {"tags": ["stable", "cn"]}, "chan-b": {"tags": ["cheap"]}}

    monkeypatch.setattr(admin.config.Config, "get_providers", classmethod(lambda cls: get_providers()))
    monkeypatch.setattr(ModelClientPool, "get_provider_names", classmethod(lambda cls: ["chan-a", "chan-b"]))


def _validate(groups):
    asyncio.run(admin._validate_model_groups(groups))


def test_valid_schemes_pass_and_keep_unknown_tags():
    groups = {
        "opus": {
            "models": ["m1"],
            "schemes": [
                {"id": "s1", "name": "高配", "models": ["m1"], "provider_whitelist": ["stable", "ghost"]},
                {"id": "s2", "name": "省配", "models": ["m2"], "provider_blacklist": ["cheap"]},
            ],
            "active_scheme": "s1",
        }
    }
    _validate(groups)
    schemes = groups["opus"]["schemes"]
    # 标签是运行时筛选维度：当下没有渠道挂 "ghost" 也要原样保留，
    # 供后续给渠道打上同义标签后在选路时命中，无需重存组。
    assert schemes[0]["provider_whitelist"] == ["stable", "ghost"]
    assert schemes[1]["provider_blacklist"] == ["cheap"]
    assert groups["opus"]["active_scheme"] == "s1"


def test_duplicate_scheme_id_rejected():
    groups = {
        "opus": {
            "models": ["m1"],
            "schemes": [
                {"id": "dup", "name": "甲", "models": ["m1"]},
                {"id": "dup", "name": "乙", "models": ["m2"]},
            ],
        }
    }
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert exc.value.status_code == 400
    assert "id 重复" in exc.value.detail


def test_duplicate_scheme_name_allowed():
    # name 只是展示标签，允许重复（靠 id 区分）。
    groups = {
        "opus": {
            "models": ["m1"],
            "schemes": [
                {"id": "s1", "name": "同名", "models": ["m1"]},
                {"id": "s2", "name": "同名", "models": ["m2"]},
            ],
        }
    }
    _validate(groups)


def test_blank_scheme_name_allowed():
    # name 允许为空，只要有 id。
    groups = {"opus": {"models": ["m1"], "schemes": [{"id": "s1", "name": "  ", "models": ["m1"]}]}}
    _validate(groups)


def test_missing_scheme_id_rejected():
    groups = {"opus": {"models": ["m1"], "schemes": [{"name": "无id", "models": ["m1"]}]}}
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "缺少 id" in exc.value.detail


def test_scheme_without_models_rejected():
    groups = {"opus": {"models": ["m1"], "schemes": [{"id": "s1", "name": "空", "models": []}]}}
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "至少需要一个模型" in exc.value.detail


def test_active_scheme_must_match_defined_scheme_id():
    groups = {
        "opus": {
            "models": ["m1"],
            "schemes": [{"id": "s1", "name": "高配", "models": ["m1"]}],
            "active_scheme": "不存在的id",
        }
    }
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "active_scheme" in exc.value.detail


def test_group_with_only_schemes_passes_without_top_level_models():
    # 前端会把激活方案投影到顶层 models；但仅给 schemes 也应通过校验。
    groups = {"opus": {"schemes": [{"id": "s1", "name": "高配", "models": ["m1"]}]}}
    _validate(groups)


def test_schemes_absent_still_requires_top_level_models():
    groups = {"opus": {"models": []}}
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "至少需要一个模型" in exc.value.detail


def test_scheme_name_with_reserved_separator_rejected():
    # #scheme: 是内存派生方案组条目的命名空间，方案名不得含它。
    groups = {
        "opus": {
            "models": ["m1"],
            "schemes": [{"id": "s1", "name": f"高配{admin._SCHEME_GROUP_SEP}1", "models": ["m1"]}],
        }
    }
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "保留串" in exc.value.detail


def test_group_name_with_reserved_separator_rejected():
    groups = {f"opus{admin._SCHEME_GROUP_SEP}1": {"models": ["m1"]}}
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "保留串" in exc.value.detail


def test_alias_with_reserved_separator_rejected():
    groups = {"opus": {"models": ["m1"], "aliases": [f"opus{admin._SCHEME_GROUP_SEP}9"]}}
    with pytest.raises(HTTPException) as exc:
        _validate(groups)
    assert "保留串" in exc.value.detail
