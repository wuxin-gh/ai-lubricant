"""provider_models extra_config 的 JSONB 读回与运行时转换测试。"""

import json

import pytest

from db import PostgresClient
from rate_limiter import ModelClientPool


class _FakeConnection:
    async def fetch(self, *_args):
        return [{
            "provider": "p",
            "upstream_model_id": "upstream-m",
            "model_id": "public-m",
            "extra_config": json.dumps({"enable_1m_context": True}),
        }]


class _Acquire:
    async def __aenter__(self):
        return _FakeConnection()

    async def __aexit__(self, *_args):
        return False


class _FakePool:
    def acquire(self):
        return _Acquire()


@pytest.mark.asyncio
async def test_list_provider_models_decodes_jsonb_extra_config(monkeypatch):
    monkeypatch.setattr(PostgresClient, "pool", _FakePool())

    rows = await PostgresClient.list_provider_models("p")

    assert rows[0]["extra_config"] == {"enable_1m_context": True}


def test_provider_model_payload_rejects_json_text_extra_config():
    payload = PostgresClient._provider_model_payload({
        "upstream_model_id": "upstream-m",
        "model_id": "public-m",
        "extra_config": json.dumps({"enable_1m_context": True}),
    })
    assert payload["extra_config"] == {}


def test_route_row_from_db_parses_json_text_extra_config():
    row = ModelClientPool._route_row_from_db({
        "provider": "p",
        "model_id": "public-m",
        "upstream_model_id": "upstream-m",
        "extra_config": json.dumps({"enable_1m_context": True}),
    })
    assert row["extra_config"] == {"enable_1m_context": True}
    assert row["enable_1m_context"] is True
