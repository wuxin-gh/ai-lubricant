"""llm-metadata 导入：归一化、目录 flatten、fetch/sync 路由。

llm-metadata 的 dist/api/all.json 与 models.dev/api.json 同形（{provider: {models: {id: item}}}），
但字段更全（attachment/structured_output 等）。测试只覆盖归一化字段映射与路由 happy/error 路径，
真实网络与 DB 写入全部 monkeypatch，避免依赖外部服务（参见 [[project-pg-redis-static-env-baseline-failures]]）。
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

import model_metadata as mm


def _sample_model(*, attachment=True, structured=True, reasoning=True, tool=True):
    return {
        "id": "gpt-5",
        "name": "GPT-5",
        "family": "gpt",
        "limit": {"context": 400000, "output": 128000},
        "modalities": {"input": ["text", "image"], "output": ["text"]},
        "reasoning": reasoning,
        "tool_call": tool,
        "attachment": attachment,
        "structured_output": structured,
    }


def test_normalize_llm_metadata_core_fields_and_capabilities():
    normalized = mm.normalize_llm_metadata_model("openai", _sample_model())
    assert normalized["name"] == "GPT-5"
    assert normalized["owned_by"] == "openai"
    assert normalized["object"] == "model"
    assert normalized["max_context_tokens"] == 400000
    assert normalized["max_tokens"] == 128000
    assert normalized["input_modalities"] == ["text", "image"]
    assert normalized["output_modalities"] == ["text"]
    assert normalized["multimodal"] == ["text", "image"]
    assert normalized["function_calling"] is True
    assert normalized["auto_thinking"] is True
    assert normalized["is_thinking"] is True
    # attachment / structured_output 进 capabilities，不污染核心字段。
    assert normalized["capabilities"]["attachment"] is True
    assert normalized["capabilities"]["structured_output"] is True
    assert "cost" not in normalized


def test_normalize_llm_metadata_missing_optional_fields_keeps_defaults():
    item = {"id": "bare", "modalities": {}, "limit": {}}
    normalized = mm.normalize_llm_metadata_model("prov", item)
    # 空模态回退到 text；空 limit 归零为 None。
    assert normalized["input_modalities"] == ["text"]
    assert normalized["output_modalities"] == ["text"]
    assert normalized["max_context_tokens"] is None
    assert normalized["max_tokens"] is None
    assert normalized["capabilities"] == {}


def test_flatten_llm_metadata_skips_non_objects():
    catalog = {
        "openai": {"models": {"gpt-5": _sample_model(), "bad": "x"}},
        "bad-prov": "not-a-dict",
        "empty": {"models": "oops"},
    }
    flat = mm.flatten_llm_metadata(catalog)
    assert [e["id"] for e in flat] == ["gpt-5"]
    assert flat[0]["provider"] == "openai"
    assert flat[0]["item"]["id"] == "gpt-5"


def test_fetch_llm_metadata_rejects_non_object(monkeypatch):
    class _Resp:
        status = 200

        async def json(self, **kw):
            return ["not", "an", "object"]

        async def text(self):
            return ""

    class _Ctx:
        async def __aenter__(self):
            return _Resp()

        async def __aexit__(self, *a):
            return False

    class _Session:
        def get(self, *a, **kw):
            return _Ctx()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    def _fake_session(*a, **kw):
        return _Session()

    monkeypatch.setattr("aiohttp.ClientSession", _fake_session)
    with pytest.raises(RuntimeError, match="返回不是对象"):
        asyncio.run(mm.fetch_llm_metadata())


def _patch_admin_for_sync(monkeypatch, raw):
    import admin

    monkeypatch.setattr(admin, "_require_admin", AsyncMock(), raising=False)
    # fetch_llm_metadata 返回构造好的目录，避免任何网络。
    monkeypatch.setattr(mm, "fetch_llm_metadata", AsyncMock(return_value=raw))
    imported = {"n": 0}

    async def _bulk_import(items):
        imported["n"] = len(items)
        imported["items"] = items
        return len(items)

    monkeypatch.setattr(mm, "bulk_import_async", _bulk_import)
    monkeypatch.setattr(admin, "_after_metadata_write", AsyncMock(return_value=None))
    monkeypatch.setattr(admin, "_log_operation", AsyncMock(return_value=None))
    return admin, imported


def test_sync_from_llm_metadata_imports_selected_and_reports_missing(monkeypatch):
    raw = {"openai": {"models": {"gpt-5": _sample_model()}}}
    admin, imported = _patch_admin_for_sync(monkeypatch, raw)

    result = asyncio.run(admin.sync_from_llm_metadata({
        "targets": [
            {"provider": "openai", "source_id": "gpt-5", "model_id": "openai/gpt-5"},
            {"provider": "openai", "source_id": "missing-model"},
        ],
    }))

    assert result["ok"] is True
    assert result["imported"] == 1
    assert imported["items"][0]["model_id"] == "openai/gpt-5"
    assert imported["items"][0]["max_context_tokens"] == 400000
    assert imported["items"][0]["capabilities"]["attachment"] is True
    results = {r["source_id"]: r for r in result["results"]}
    assert results["gpt-5"]["ok"] is True
    assert results["missing-model"]["ok"] is False
    assert "未找到" in results["missing-model"]["error"]


def test_sync_from_llm_metadata_empty_targets_rejected(monkeypatch):
    raw = {"openai": {"models": {"gpt-5": _sample_model()}}}
    admin, _ = _patch_admin_for_sync(monkeypatch, raw)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.sync_from_llm_metadata({"targets": []}))
    assert exc.value.status_code == 400


def test_sync_from_llm_metadata_fetch_failure_raises_502(monkeypatch):
    import admin

    monkeypatch.setattr(admin, "_require_admin", AsyncMock(), raising=False)

    async def _boom(*a, **kw):
        raise RuntimeError("network down")

    monkeypatch.setattr(mm, "fetch_llm_metadata", _boom)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin.sync_from_llm_metadata({"targets": [{"provider": "openai", "source_id": "gpt-5"}]}))
    assert exc.value.status_code == 502
    assert "拉取 llm-metadata 失败" in exc.value.detail


def test_fetch_llm_metadata_route_shape(monkeypatch):
    raw = {
        "openai": {"name": "OpenAI", "models": {"gpt-5": _sample_model()}},
        "alibaba": {"name": "Alibaba", "models": {"qwen-max": {**_sample_model(attachment=False, structured=False, reasoning=False, tool=True), "id": "qwen-max", "name": "Qwen Max", "family": "qwen", "limit": {"context": 32768, "output": 8192}, "modalities": {"input": ["text"], "output": ["text"]}}}},
    }
    import admin

    monkeypatch.setattr(admin, "_require_admin", AsyncMock(), raising=False)
    monkeypatch.setattr(mm, "fetch_llm_metadata", AsyncMock(return_value=raw))
    result = asyncio.run(admin.fetch_llm_metadata_catalog())
    items = {it["id"]: it for it in result["items"]}
    assert result["providers"] == ["alibaba", "openai"]
    assert items["gpt-5"]["context_length"] == 400000
    assert items["gpt-5"]["output_limit"] == 128000
    assert items["gpt-5"]["normalized"]["capabilities"]["attachment"] is True
    assert items["qwen-max"]["normalized"]["function_calling"] is True
    assert items["qwen-max"]["normalized"]["is_thinking"] is False
    assert items["qwen-max"]["normalized"]["capabilities"]["attachment"] is False
