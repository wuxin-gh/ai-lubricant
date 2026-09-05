import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import PostgresClient


# ==================== normalize_model_group：方案规整 + 激活投影（按 id） ====================


def test_legacy_group_without_schemes_synthesizes_default_scheme():
    """旧组（无 schemes）用顶层三字段合成一条默认方案，带确定性 id legacy-0。"""
    out = PostgresClient.normalize_model_group(
        "opus",
        {"models": ["m1", "m2"], "provider_whitelist": ["p1"], "provider_blacklist": ["p2"]},
    )
    assert out is not None
    assert len(out["schemes"]) == 1
    scheme = out["schemes"][0]
    assert scheme["id"] == "legacy-0"
    assert scheme["name"] == PostgresClient.DEFAULT_SCHEME_NAME
    assert scheme["models"] == ["m1", "m2"]
    assert scheme["provider_whitelist"] == ["p1"]
    assert scheme["provider_blacklist"] == ["p2"]
    # active_scheme 存的是 id
    assert out["active_scheme"] == "legacy-0"
    # 顶层三字段 = 激活方案投影
    assert out["models"] == ["m1", "m2"]
    assert out["provider_whitelist"] == ["p1"]
    assert out["provider_blacklist"] == ["p2"]


def test_active_scheme_projects_to_top_level_fields_by_id():
    """给定多套方案 + active_scheme(=id)，顶层三字段等于该 id 指向的方案。"""
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"id": "s-a", "name": "高配", "models": ["big"], "provider_whitelist": ["pa"], "provider_blacklist": []},
                {"id": "s-b", "name": "省配", "models": ["small"], "provider_whitelist": [], "provider_blacklist": ["pb"]},
            ],
            "active_scheme": "s-b",
        },
    )
    assert out is not None
    assert out["active_scheme"] == "s-b"
    assert out["models"] == ["small"]
    assert out["provider_whitelist"] == []
    assert out["provider_blacklist"] == ["pb"]
    assert [s["id"] for s in out["schemes"]] == ["s-a", "s-b"]


def test_active_scheme_matches_by_id_not_name():
    """两套方案同名，active_scheme 用 id 精确命中——绝不因重名而错投影。"""
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"id": "s-a", "name": "同名", "models": ["big"]},
                {"id": "s-b", "name": "同名", "models": ["small"]},
            ],
            "active_scheme": "s-b",
        },
    )
    assert out is not None
    assert out["active_scheme"] == "s-b"
    assert out["models"] == ["small"]


def test_active_scheme_defaults_to_first_when_unset_or_invalid():
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"id": "s-a", "name": "高配", "models": ["big"]},
                {"id": "s-b", "name": "省配", "models": ["small"]},
            ],
            "active_scheme": "不存在的id",
        },
    )
    assert out is not None
    assert out["active_scheme"] == "s-a"
    assert out["models"] == ["big"]


def test_schemes_deduped_by_id_first_occurrence():
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"id": "dup", "name": "x", "models": ["a"]},
                {"id": "dup", "name": "y", "models": ["b"]},
            ]
        },
    )
    assert out is not None
    assert len(out["schemes"]) == 1
    assert out["schemes"][0]["models"] == ["a"]


def test_legacy_scheme_without_id_gets_positional_id():
    """无 id 的方案（旧数据）按数组位置补 legacy-{序号}。"""
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"name": "第一", "models": ["a"]},
                {"name": "第二", "models": ["b"]},
            ]
        },
    )
    assert out is not None
    assert [s["id"] for s in out["schemes"]] == ["legacy-0", "legacy-1"]


def test_scheme_name_may_be_blank_and_duplicated():
    """name 只是展示标签：允许为空、允许重复，不影响方案存在。"""
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"id": "s-a", "name": "", "models": ["a"]},
                {"id": "s-b", "name": "", "models": ["b"]},
            ]
        },
    )
    assert out is not None
    assert len(out["schemes"]) == 2
    assert all(s["name"] == "" for s in out["schemes"])


def test_group_with_no_usable_scheme_returns_none():
    """schemes 均无 models 且顶层也无 models 时视为非法。"""
    assert PostgresClient.normalize_model_group("opus", {"schemes": [{"id": "x", "models": []}]}) is None
    assert PostgresClient.normalize_model_group("opus", {"schemes": []}) is None


def test_active_scheme_with_empty_models_is_rejected():
    """激活方案 models 为空则整组非法（顶层投影为空）。"""
    out = PostgresClient.normalize_model_group(
        "opus",
        {
            "schemes": [
                {"id": "s-empty", "name": "空", "models": []},
                {"id": "s-full", "name": "满", "models": ["m1"]},
            ],
            "active_scheme": "s-empty",
        },
    )
    assert out is None


# ==================== _model_group_row：读回兜底（按 id） ====================


def test_model_group_row_synthesizes_default_scheme_for_legacy_row():
    """旧行无 schemes 列时，读回合成默认方案，id=legacy-0，active_scheme 兜底为其 id。"""
    row = {
        "name": "opus",
        "enabled": True,
        "remark": "",
        "models": ["m1"],
        "aliases": [],
        "provider_whitelist": ["p1"],
        "provider_blacklist": [],
        "selection_strategy": "intelligent",
        "backup_group": "",
        "response_model": "",
        "metadata_model": "",
        "created_at": 0,
    }
    out = PostgresClient._model_group_row(row)
    assert out is not None
    assert len(out["schemes"]) == 1
    assert out["schemes"][0]["id"] == "legacy-0"
    assert out["schemes"][0]["name"] == PostgresClient.DEFAULT_SCHEME_NAME
    assert out["schemes"][0]["models"] == ["m1"]
    assert out["active_scheme"] == "legacy-0"
    # 顶层 = 激活方案投影
    assert out["models"] == ["m1"]
    assert out["provider_whitelist"] == ["p1"]


def test_model_group_row_parses_schemes_and_projects_active_by_id():
    """读回时按 active_scheme(=id) 重新投影顶层三字段——不信任 DB 存的顶层旧值。"""
    row = {
        "name": "opus",
        "enabled": True,
        "remark": "",
        # 故意给一个与激活方案不一致的陈旧顶层值，验证读回会重新投影覆盖它。
        "models": ["stale"],
        "aliases": [],
        "provider_whitelist": ["stale-p"],
        "provider_blacklist": [],
        "selection_strategy": "intelligent",
        "backup_group": "",
        "response_model": "",
        "metadata_model": "",
        "schemes": '[{"id":"s-a","name":"高配","models":["big"]},{"id":"s-b","name":"省配","models":["small"],"provider_whitelist":["pb"]}]',
        "active_scheme": "s-b",
        "created_at": 0,
    }
    out = PostgresClient._model_group_row(row)
    assert out is not None
    assert [s["id"] for s in out["schemes"]] == ["s-a", "s-b"]
    assert out["active_scheme"] == "s-b"
    # 顶层被重新投影为 s-b，而非陈旧的 stale
    assert out["models"] == ["small"]
    assert out["provider_whitelist"] == ["pb"]


def test_model_group_row_active_id_miss_falls_back_to_first():
    """active_scheme 的 id 在 schemes 里找不到时，回落第一套并重投影。"""
    row = {
        "name": "opus",
        "enabled": True,
        "remark": "",
        "models": ["stale"],
        "aliases": [],
        "provider_whitelist": [],
        "provider_blacklist": [],
        "selection_strategy": "intelligent",
        "backup_group": "",
        "response_model": "",
        "metadata_model": "",
        "schemes": '[{"id":"s-a","name":"高配","models":["big"]},{"id":"s-b","name":"省配","models":["small"]}]',
        "active_scheme": "gone",
        "created_at": 0,
    }
    out = PostgresClient._model_group_row(row)
    assert out is not None
    assert out["active_scheme"] == "s-a"
    assert out["models"] == ["big"]


# ==================== schema 迁移 ====================


def test_model_group_table_has_schemes_and_active_scheme_columns():
    source = open("db.py", encoding="utf-8").read()
    assert "ADD COLUMN IF NOT EXISTS schemes JSONB" in source
    assert "ADD COLUMN IF NOT EXISTS active_scheme TEXT" in source
