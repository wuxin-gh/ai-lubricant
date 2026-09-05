from __future__ import annotations

import uuid
from types import SimpleNamespace

from monkeycode_compat.routes_task import CreateTaskReq
from monkeycode_compat.task_service import TaskService


def _task() -> SimpleNamespace:
    return SimpleNamespace(id=uuid.UUID("11111111-2222-3333-4444-555555555555"))


def test_task_auto_branch_builds_create_branch_payload() -> None:
    session = TaskService()._build_session(
        {
            "provider": "claude",
            "repo": {
                "repo_url": "https://example.com/acme/repo.git",
                "branch_mode": "auto",
            },
        },
        _task(),
    )

    assert session["git"] == {
        "url": "https://example.com/acme/repo.git",
        "branch": "",
        "createBranch": True,
        "newBranch": "task/11111111-2222-3333-4444-555555555555",
    }


def test_task_existing_branch_builds_checkout_payload() -> None:
    session = TaskService()._build_session(
        {
            "provider": "claude",
            "repo": {
                "repo_url": "https://example.com/acme/repo.git",
                "branch_mode": "existing",
                "branch": "feature/existing",
            },
        },
        _task(),
    )

    assert session["git"] == {
        "url": "https://example.com/acme/repo.git",
        "branch": "feature/existing",
    }


def test_create_task_request_allows_empty_content() -> None:
    body = CreateTaskReq()
    assert body.content == ""
