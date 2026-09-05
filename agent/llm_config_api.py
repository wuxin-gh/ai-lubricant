"""LLM 管理 CRUD 端点（agent 服务专用 LLM 配置）。

配置实体 agent_llm_configs：name / base_url / chat_path / api_key / models / protocol / enabled。
模型库存实体 agent_llm_models：llm_config_id / model_name / display_name / enabled / metadata。
独立于主系统 providers/渠道体系，仅被 agent 服务的 LLMBridge 使用。

鉴权复用 admin session token（_require_admin）。
"""
from __future__ import annotations

from typing import Any, Optional

import aiohttp
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field

from db import PostgresClient

router = APIRouter(prefix="/agent", tags=["agent-llm-config"])


async def require_admin(authorization: Optional[str] = Header(None)) -> None:
    """复用 admin session token 鉴权（延迟 import admin 避免循环依赖）。"""
    from admin import _require_admin
    await _require_admin(authorization)


class CreateLlmConfigRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    base_url: str = Field(..., min_length=1)
    chat_path: str = "/chat/completions"
    api_key: str = Field(..., min_length=1)
    models: list[str] = []
    protocol: str = "openai"
    enabled: bool = True
    timeout_seconds: int = Field(default=600, ge=1)
    max_retries: int = Field(default=2, ge=0)


class UpdateLlmConfigRequest(BaseModel):
    name: str | None = None
    base_url: str | None = None
    chat_path: str | None = None
    api_key: str | None = None
    models: list[str] | None = None
    protocol: str | None = None
    enabled: bool | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)
    max_retries: int | None = Field(default=None, ge=0)


class CreateLlmModelRequest(BaseModel):
    model_name: str = Field(..., min_length=1, max_length=200)
    display_name: str | None = None
    description: str = ""
    enabled: bool = True
    sort_order: int = 0
    metadata: dict[str, Any] = {}


class UpdateLlmModelRequest(BaseModel):
    model_name: str | None = None
    display_name: str | None = None
    description: str | None = None
    enabled: bool | None = None
    sort_order: int | None = None
    metadata: dict[str, Any] | None = None


def _model_row_to_dict(row: dict) -> dict:
    d = dict(row)
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None and hasattr(d[key], "isoformat"):
            d[key] = d[key].isoformat()
    return d


async def _sync_config_models(config_id: int, models: list[str]) -> None:
    """旧 models 字符串列表同步为 agent_llm_models 库存行。"""
    seen: set[str] = set()
    for idx, raw in enumerate(models or []):
        name = str(raw).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        existing = await PostgresClient.find_agent_llm_model(config_id, name)
        if existing:
            await PostgresClient.update_agent_llm_model(
                existing["id"],
                enabled=True,
                sort_order=idx,
                display_name=existing.get("display_name") or name,
            )
        else:
            await PostgresClient.create_agent_llm_model(
                llm_config_id=config_id,
                model_name=name,
                display_name=name,
                enabled=True,
                sort_order=idx,
            )


async def _row_to_dict(row: dict, include_models: bool = True) -> dict:
    d = dict(row)
    api_key = d.get("api_key") or ""
    d["api_key_masked"] = (api_key[:6] + "***") if api_key else ""
    d["has_api_key"] = bool(api_key)
    # 不回传明文 api_key
    d.pop("api_key", None)
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None and hasattr(d[key], "isoformat"):
            d[key] = d[key].isoformat()
    if include_models:
        model_rows = await PostgresClient.list_agent_llm_models(config_id=d["id"])
        d["model_items"] = [_model_row_to_dict(r) for r in model_rows]
        if model_rows:
            d["models"] = [r.get("model_name") for r in model_rows if r.get("model_name")]
        else:
            d["models"] = d.get("models") or []
    return d


@router.get("/llm-configs")
async def list_llm_configs(authorization: Optional[str] = Header(None)) -> list[dict]:
    await require_admin(authorization)
    if not PostgresClient.pool:
        return []
    rows = await PostgresClient.list_agent_llm_configs()
    return [await _row_to_dict(r) for r in rows]


@router.get("/llm-configs/{config_id}")
async def get_llm_config(config_id: int, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    row = await PostgresClient.get_agent_llm_config(config_id)
    if not row:
        raise HTTPException(404, f"LLM config {config_id} not found")
    return await _row_to_dict(row)


@router.post("/llm-configs")
async def create_llm_config(request: CreateLlmConfigRequest, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    try:
        row = await PostgresClient.create_agent_llm_config(
            name=request.name,
            base_url=request.base_url,
            chat_path=request.chat_path,
            api_key=request.api_key,
            models=request.models,
            protocol=request.protocol,
            enabled=request.enabled,
            timeout_seconds=request.timeout_seconds,
            max_retries=request.max_retries,
        )
        await _sync_config_models(row["id"], request.models)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(409, f"LLM config name '{request.name}' already exists")
        raise HTTPException(500, str(e))
    return await _row_to_dict(row)


@router.patch("/llm-configs/{config_id}")
async def update_llm_config(config_id: int, request: UpdateLlmConfigRequest, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    fields = {k: v for k, v in request.model_dump(exclude_none=True).items()}
    if not fields:
        raise HTTPException(400, "No fields to update")
    try:
        row = await PostgresClient.update_agent_llm_config(config_id, **fields)
        if row and "models" in fields:
            await _sync_config_models(config_id, fields["models"])
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(409, f"LLM config name '{fields.get('name')}' already exists")
        raise HTTPException(500, str(e))
    if not row:
        raise HTTPException(404, f"LLM config {config_id} not found")
    return await _row_to_dict(row)


@router.delete("/llm-configs/{config_id}")
async def delete_llm_config(config_id: int, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    deleted = await PostgresClient.delete_agent_llm_config(config_id)
    if not deleted:
        raise HTTPException(404, f"LLM config {config_id} not found")
    return {"ok": True, "msg": f"LLM config {config_id} deleted"}


@router.post("/llm-configs/{config_id}/toggle")
async def toggle_llm_config(config_id: int, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    row = await PostgresClient.get_agent_llm_config(config_id)
    if not row:
        raise HTTPException(404, f"LLM config {config_id} not found")
    updated = await PostgresClient.update_agent_llm_config(config_id, enabled=not row.get("enabled"))
    return {"id": updated["id"], "enabled": updated["enabled"]} if updated else {"id": config_id, "enabled": None}


@router.get("/llm-models")
async def list_llm_models(
    config_id: int | None = None,
    enabled_only: bool = False,
    authorization: Optional[str] = Header(None),
) -> list[dict]:
    await require_admin(authorization)
    if not PostgresClient.pool:
        return []
    rows = await PostgresClient.list_agent_llm_models(config_id=config_id, enabled_only=enabled_only)
    return [_model_row_to_dict(r) for r in rows]


@router.get("/llm-configs/{config_id}/models")
async def list_llm_config_models(config_id: int, authorization: Optional[str] = Header(None)) -> list[dict]:
    await require_admin(authorization)
    if not PostgresClient.pool:
        return []
    row = await PostgresClient.get_agent_llm_config(config_id)
    if not row:
        raise HTTPException(404, f"LLM config {config_id} not found")
    rows = await PostgresClient.list_agent_llm_models(config_id=config_id)
    return [_model_row_to_dict(r) for r in rows]


@router.post("/llm-configs/{config_id}/fetch-models")
async def fetch_llm_config_models(config_id: int, authorization: Optional[str] = Header(None)) -> dict:
    """从上游 /models 拉取模型列表（仅 OpenAI 兼容模型列表接口）。"""
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    row = await PostgresClient.get_agent_llm_config(config_id)
    if not row:
        raise HTTPException(404, f"LLM config {config_id} not found")
    base_url = (row.get("base_url") or "").rstrip("/")
    if not base_url:
        raise HTTPException(400, "LLM Base URL is empty")
    url = f"{base_url}/models"
    headers = {"Authorization": f"Bearer {row.get('api_key') or ''}"}
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url, headers=headers) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise HTTPException(resp.status, text[:500] or f"HTTP {resp.status}")
                payload = await resp.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"获取模型列表失败: {e}")

    raw_models = payload.get("data") if isinstance(payload, dict) else payload
    models: list[str] = []
    if isinstance(raw_models, list):
        for item in raw_models:
            if isinstance(item, str):
                models.append(item)
            elif isinstance(item, dict):
                model_id = item.get("id") or item.get("model") or item.get("name")
                if model_id:
                    models.append(str(model_id))
    # 去重并保持顺序
    deduped = list(dict.fromkeys(m.strip() for m in models if m and str(m).strip()))
    return {"models": deduped, "count": len(deduped)}


@router.post("/llm-configs/{config_id}/models")
async def create_llm_model(config_id: int, request: CreateLlmModelRequest, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    row = await PostgresClient.get_agent_llm_config(config_id)
    if not row:
        raise HTTPException(404, f"LLM config {config_id} not found")
    try:
        created = await PostgresClient.create_agent_llm_model(
            llm_config_id=config_id,
            model_name=request.model_name.strip(),
            display_name=(request.display_name or request.model_name).strip(),
            description=request.description,
            enabled=request.enabled,
            sort_order=request.sort_order,
            metadata=request.metadata,
        )
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(409, f"Model '{request.model_name}' already exists")
        raise HTTPException(500, str(e))
    return _model_row_to_dict(created)


@router.patch("/llm-models/{model_id}")
async def update_llm_model(model_id: int, request: UpdateLlmModelRequest, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    fields = request.model_dump(exclude_none=True)
    if "model_name" in fields:
        fields["model_name"] = fields["model_name"].strip()
    if "display_name" in fields and fields["display_name"] is not None:
        fields["display_name"] = fields["display_name"].strip()
    if not fields:
        raise HTTPException(400, "No fields to update")
    try:
        row = await PostgresClient.update_agent_llm_model(model_id, **fields)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(409, "Model name already exists in this LLM config")
        raise HTTPException(500, str(e))
    if not row:
        raise HTTPException(404, f"LLM model {model_id} not found")
    return _model_row_to_dict(row)


@router.delete("/llm-models/{model_id}")
async def delete_llm_model(model_id: int, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    deleted = await PostgresClient.delete_agent_llm_model(model_id)
    if not deleted:
        raise HTTPException(404, f"LLM model {model_id} not found")
    return {"ok": True, "msg": f"LLM model {model_id} deleted"}


@router.post("/llm-models/{model_id}/toggle")
async def toggle_llm_model(model_id: int, authorization: Optional[str] = Header(None)) -> dict:
    await require_admin(authorization)
    if not PostgresClient.pool:
        raise HTTPException(503, "Database not available")
    row = await PostgresClient.get_agent_llm_model(model_id)
    if not row:
        raise HTTPException(404, f"LLM model {model_id} not found")
    updated = await PostgresClient.update_agent_llm_model(model_id, enabled=not row.get("enabled"))
    return {"id": updated["id"], "enabled": updated["enabled"]} if updated else {"id": model_id, "enabled": None}
