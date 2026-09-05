from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_source(name: str) -> str:
    """按名字读业务模块源码：业务模块已收进 server/，根目录仅作兜底。"""
    for base in (ROOT / "server", ROOT):
        path = base / name
        if path.exists():
            return path.read_text(encoding="utf-8")
    raise FileNotFoundError(name)


def test_model_routing_replace_preserves_real_metadata_rows():
    source = read_source("db.py")
    body = source[
        source.index("async def replace_model_groups"):
        source.index("    @classmethod\n    async def delete_model_group", source.index("async def replace_model_groups"))
    ]

    assert "DELETE FROM model_groups WHERE kind <> 'real'" in body
    assert "real 行（kind='real'" in body


def test_metadata_admin_writes_target_real_model_group_rows():
    source = read_source("model_metadata.py")

    assert "PostgresClient.upsert_real_model_metadata" in source
    assert "PostgresClient.bulk_upsert_real_model_metadata" in source
    assert "PostgresClient.delete_real_model_metadata" in source
    assert "upsert_model_metadata" not in source
    assert "delete_model_metadata" not in source
    assert "bulk_upsert_model_metadata" not in source
    assert "invalidate_cache" not in source
    assert "_REDIS_KEY" not in source


def test_model_group_validation_excludes_real_metadata_rows():
    source = read_source("admin.py")
    write_validation = source[
        source.index("async def _validate_model_group_write"):
        source.index('@router.post("/model-groups")', source.index("async def _validate_model_group_write"))
    ]
    create_body = source[
        source.index("async def create_model_group"):
        source.index('@router.put("/model-groups/{name}")', source.index("async def create_model_group"))
    ]

    assert 'str(item.get("kind") or "custom") != "real"' in write_validation
    assert 'if str(existing.get("kind") or "custom") == "real"' in create_body
    assert "不是自定义模型组" in create_body


def test_model_routing_endpoint_filters_real_rows_from_group_payload():
    source = read_source("admin.py")
    body = source[
        source.index("async def get_model_routing"):
        source.index("async def _after_metadata_write", source.index("async def get_model_routing"))
    ]

    assert "str(group.get(\"kind\") or \"custom\") != \"real\"" in body
    assert "/model-routing 只展示自定义路由组" in body
