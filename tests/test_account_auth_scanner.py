import asyncio
import time
from pathlib import Path

import admin


def _auth_record(provider="scanner-test"):
    return {
        "provider": provider,
        "task_type": "device_code",
        "status": "pending",
        "poll_params": {"state": "upstream-state"},
        "interval": 7,
        "next_poll_at": 10,
        "expires_at": time.time() + 300,
        "account": {},
    }


def test_poll_one_auth_task_persists_terminal_provider_error(monkeypatch):
    state = "terminal-error-state"
    record = _auth_record()
    instance_key = f"{record['provider']}:{state}"
    persisted = []

    class Provider:
        async def poll_device_flow(self, poll_params):
            return {"status": "error", "error": "无 Copilot 订阅"}

    provider = Provider()
    admin._auth_scanner_instances[instance_key] = provider

    async def read_config(provider_name):
        return {}

    async def set_state(auth_state, data):
        persisted.append((auth_state, dict(data)))

    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_build_scanner_instance", lambda *args, **kwargs: provider)
    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)

    asyncio.run(admin._poll_one_auth_task(state, record))

    assert record["status"] == "error"
    assert record["error"] == "无 Copilot 订阅"
    assert record["next_poll_at"] == 10
    assert persisted == [(state, record)]
    assert instance_key not in admin._auth_scanner_instances


def test_poll_one_auth_task_keeps_timeout_retryable(monkeypatch):
    state = "timeout-state"
    record = _auth_record()
    instance_key = f"{record['provider']}:{state}"
    persisted = []

    class Provider:
        async def poll_device_flow(self, poll_params):
            raise asyncio.TimeoutError

    provider = Provider()
    admin._auth_scanner_instances[instance_key] = provider

    async def read_config(provider_name):
        return {}

    async def set_state(auth_state, data):
        persisted.append((auth_state, dict(data)))

    monkeypatch.setattr(admin, "_read_provider_config", read_config)
    monkeypatch.setattr(admin, "_build_scanner_instance", lambda *args, **kwargs: provider)
    monkeypatch.setattr(admin, "_set_account_auth_state", set_state)

    before = time.time()
    asyncio.run(admin._poll_one_auth_task(state, record))

    assert record["status"] == "pending"
    assert record["next_poll_at"] >= before + record["interval"] - 0.1
    assert "error" not in record
    assert persisted == [(state, record)]
    assert admin._auth_scanner_instances[instance_key] is provider

    admin._auth_scanner_instances.pop(instance_key, None)


def test_list_pending_reads_postgres_index_without_global_scan():
    """扫描器捞任务改走 Postgres 复合索引查询，不再对 Redis 做全库 SCAN。"""
    source = Path(admin.__file__).read_text(encoding="utf-8")
    body = source[source.index("async def _list_pending_device_code_tasks"):source.index("def _build_scanner_instance")]

    # 不再依赖 Redis 全库 SCAN / 内存兜底 dict
    assert "scan_iter" not in body
    assert "_account_auth_states_mem" not in body
    # 改为查 account_auth_states 表的 pending device_code 复合索引
    assert "list_pending_device_code_states" in body
    # Redis 扫描降级相关的死常量已移除
    assert not hasattr(admin, "ACCOUNT_AUTH_STATE_PREFIX")
    assert not hasattr(admin, "_account_auth_states_mem")


def test_list_pending_device_code_tasks_delegates_to_postgres(monkeypatch):
    """_list_pending_device_code_tasks 直接返回 PostgresClient 查询结果。"""
    expected = [("s1", {"task_type": "device_code", "status": "pending"})]

    async def fake_list():
        return expected

    monkeypatch.setattr(admin.PostgresClient, "list_pending_device_code_states", fake_list)
    result = asyncio.run(admin._list_pending_device_code_tasks())
    assert result == expected
