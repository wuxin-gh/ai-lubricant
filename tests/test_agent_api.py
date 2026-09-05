import asyncio
import os
import sys
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

import agent.api as agent_api
from agent.agent_main import GenericAgent


def make_client():
    agent_api._TASKS.clear()
    agent_api._TASK_HANDLES.clear()
    agent_api._AGENT_CONFIG = agent_api.AgentConfig()
    app = FastAPI()
    app.include_router(agent_api.router)
    return TestClient(app)


def wait_for_status(client: TestClient, task_id: str, status: str, timeout: float = 2.0):
    deadline = time.time() + timeout
    last_response = None
    while time.time() < deadline:
        last_response = client.get(f"/agent/task/{task_id}")
        assert last_response.status_code == 200
        body = last_response.json()
        if body["status"] == status:
            return body
        time.sleep(0.01)
    raise AssertionError(f"task {task_id} did not reach {status}; last={last_response.json() if last_response else None}")


def test_post_creates_task_and_returns_id_and_status(monkeypatch):
    async def fake_run_task(self, prompt, system_prompt="", max_turns=None):
        return [{"result": "CURRENT_TASK_DONE", "data": prompt}]

    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)
    with make_client() as client:
        response = client.post("/agent/task", json={"prompt": "do work", "model": "test-model", "max_turns": 3})

        assert response.status_code == 200
        body = response.json()
        assert body["task_id"]
        assert body["status"] in {"queued", "running"}


def test_get_returns_completed_result_after_background_run(monkeypatch):
    seen = {}

    async def fake_run_task(self, prompt, system_prompt="", max_turns=None):
        seen["prompt"] = prompt
        seen["model"] = self.config.model
        seen["max_turns"] = max_turns
        return [{"result": "CURRENT_TASK_DONE", "data": "done"}]

    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)
    with make_client() as client:
        create_response = client.post("/agent/task", json={"prompt": "finish this", "model": "patched", "max_turns": 5})
        task_id = create_response.json()["task_id"]

        body = wait_for_status(client, task_id, "completed")

        assert body["task_id"] == task_id
        assert body["result"] == [{"result": "CURRENT_TASK_DONE", "data": "done"}]
        assert body["error"] is None
        assert seen == {"prompt": "finish this", "model": "patched", "max_turns": 5}


def test_failed_background_run_records_error_and_failed_status(monkeypatch):
    async def fake_run_task(self, prompt, system_prompt="", max_turns=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)
    with make_client() as client:
        create_response = client.post("/agent/task", json={"prompt": "explode"})
        task_id = create_response.json()["task_id"]

        body = wait_for_status(client, task_id, "failed")

        assert body["task_id"] == task_id
        assert body["result"] is None
        assert "RuntimeError: boom" in body["error"]


def test_unknown_task_id_returns_404():
    with make_client() as client:
        response = client.get("/agent/task/missing")

        assert response.status_code == 404


def test_abort_unknown_task_returns_404():
    with make_client() as client:
        response = client.post("/agent/task/missing/abort")

        assert response.status_code == 404


def test_abort_running_task_cancels_background_handle(monkeypatch):
    started = asyncio.Event()

    async def fake_run_task(self, prompt, system_prompt="", max_turns=None):
        started.set()
        await asyncio.sleep(10)
        return [{"result": "should not complete"}]

    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)
    with make_client() as client:
        create_response = client.post("/agent/task", json={"prompt": "cancel me"})
        task_id = create_response.json()["task_id"]
        wait_for_status(client, task_id, "running")

        response = client.post(f"/agent/task/{task_id}/abort")

        assert response.status_code == 200
        body = response.json()
        assert body["task_id"] == task_id
        assert body["status"] == "aborted"
        assert body["result"] is None
        assert "aborted" in body["error"]


def test_status_summarizes_task_counts():
    with make_client() as client:
        agent_api._TASKS["running"] = agent_api.AgentTaskResponse(task_id="running", status="running")
        agent_api._TASKS["completed"] = agent_api.AgentTaskResponse(task_id="completed", status="completed")
        agent_api._TASKS["failed"] = agent_api.AgentTaskResponse(task_id="failed", status="failed")
        agent_api._TASKS["aborted"] = agent_api.AgentTaskResponse(task_id="aborted", status="aborted")

        response = client.get("/agent/status")

        assert response.status_code == 200
        assert response.json() == {
            "tasks": {
                "total": 4,
                "queued": 0,
                "running": 1,
                "active": 1,
                "completed": 1,
                "failed": 1,
                "aborted": 1,
            }
        }


def test_tools_schema_returns_registered_tool_schemas():
    with make_client() as client:
        response = client.get("/agent/tools/schema")

        assert response.status_code == 200
        body = response.json()
        names = {item["function"]["name"] for item in body["tools"]}
        assert {"code_run", "file_read", "file_write", "file_patch"}.issubset(names)


def test_skills_and_memory_use_safe_empty_no_pool(monkeypatch):
    calls = {"skills": 0, "insights": 0, "facts": 0, "archives": 0}

    class FakeMemorySystem:
        async def list_skills(self, category=None, limit=100):
            calls["skills"] += 1
            return []

        async def get_l1_insights(self, category=None, limit=50):
            calls["insights"] += 1
            return []

        async def get_l2_facts(self, source=None, limit=100):
            calls["facts"] += 1
            return []

        async def get_l4_archive(self, limit=10):
            calls["archives"] += 1
            return []

    monkeypatch.setattr(agent_api, "MemorySystem", FakeMemorySystem)
    with make_client() as client:
        skills_response = client.get("/agent/skills")
        memory_response = client.get("/agent/memory")

        assert skills_response.status_code == 200
        assert skills_response.json() == {"skills": []}
        assert memory_response.status_code == 200
        assert memory_response.json()["l4_archive"] == []
        assert memory_response.json()["l1_insights"]
        assert memory_response.json()["l2_facts"]
        assert calls == {"skills": 0, "insights": 0, "facts": 0, "archives": 1}


def test_config_get_and_post_use_in_memory_config():
    with make_client() as client:
        initial = client.get("/agent/config")
        assert initial.status_code == 200
        assert initial.json()["enabled"] is False

        updated = client.post("/agent/config", json={"enabled": True, "model": "phase-3", "unknown": "ignored"})

        assert updated.status_code == 200
        assert updated.json()["enabled"] is True
        assert updated.json()["model"] == "phase-3"
        assert "unknown" not in updated.json()
        assert client.get("/agent/config").json()["model"] == "phase-3"
