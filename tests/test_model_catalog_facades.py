import asyncio
from types import SimpleNamespace

import config
import model_metadata
from db import PostgresClient


async def _async_return(value):
    return value


def _snapshot(*, max_tokens: int, group_model: str = "model-a"):
    group = {
        "name": "group-a",
        "aliases": ("alias-a",),
        "models": (group_model,),
        "provider_whitelist": ("stable",),
    }
    return SimpleNamespace(
        groups={"group-a": group},
        enabled_groups={"group-a": group},
        group_index={"group-a": group, "alias-a": group},
        metadata_by_id={
            group_model: {
                "model_id": group_model,
                "max_tokens": max_tokens,
                "input_modalities": ("text", "image"),
            }
        },
        group_metadata_by_id={
            "group-a": {
                "max_tokens": max_tokens,
                "input_modalities": ("text", "image"),
            },
            "alias-a": {
                "max_tokens": max_tokens,
                "input_modalities": ("text", "image"),
            },
        },
        default_metadata={"max_tokens": 1024},
    )


def test_runtime_facades_use_snapshot_without_database(monkeypatch):
    snapshot = _snapshot(max_tokens=8192)

    async def fail_db(*args, **kwargs):
        raise AssertionError("runtime facade invoked DB")

    monkeypatch.setattr(PostgresClient, "list_model_groups", fail_db)
    monkeypatch.setattr(PostgresClient, "list_model_metadata", fail_db)
    monkeypatch.setattr(PostgresClient, "get_model_metadata_default", fail_db)
    monkeypatch.setattr(config, "current_snapshot", lambda: snapshot)
    monkeypatch.setattr(model_metadata, "current_snapshot", lambda: snapshot)

    monkeypatch.setattr(config.Config, "get_providers", classmethod(
        lambda cls: _async_return({"provider-a": {"tags": ["stable"]}})
    ))
    groups = asyncio.run(config.Config.get_model_groups())
    groups["group-a"]["models"].append("mutated")
    assert asyncio.run(config.Config.get_model_group_models("alias-a")) == ["model-a"]
    assert asyncio.run(
        config.Config.model_group_allows_provider("group-a", "provider-a")
    )

    metadata, is_default = asyncio.run(model_metadata.get_model_metadata("alias-a"))
    assert not is_default
    assert metadata["max_tokens"] == 8192
    assert metadata["input_modalities"] == ["text", "image"]

    model = {"id": "model-a", "max_tokens": None}
    asyncio.run(model_metadata.apply_model_metadata("provider-a", model))
    assert model["max_tokens"] == 8192
    assert model["multimodal"] == ["text", "image"]


def test_explicit_snapshot_remains_consistent_when_current_changes(monkeypatch):
    captured = _snapshot(max_tokens=4096, group_model="old-model")
    replacement = _snapshot(max_tokens=16384, group_model="new-model")
    monkeypatch.setattr(config, "current_snapshot", lambda: replacement)
    monkeypatch.setattr(model_metadata, "current_snapshot", lambda: replacement)

    assert asyncio.run(
        config.Config.get_model_group_models("group-a", snapshot=captured)
    ) == ["old-model"]
    metadata, is_default = asyncio.run(
        model_metadata.get_model_metadata("group-a", snapshot=captured)
    )
    assert not is_default
    assert metadata["max_tokens"] == 4096

    models = [{"id": "old-model", "max_tokens": None}]
    assert asyncio.run(
        model_metadata.reapply_to_models(models, snapshot=captured)
    ) == 1
    assert models[0]["max_tokens"] == 4096
