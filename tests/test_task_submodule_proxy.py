"""Submodule-aware git proxy URL + allowlist derivation (gateway side).

The gateway embeds the real repo's host-relative path into the proxy clone URL
(``/git/r/<owner/repo.git>/``) so ``git clone --recurse-submodules`` resolves a
relative ``.gitmodules`` url back into the proxy, and persists the resolved
same-host submodule paths into ``config_snapshot.submodule_paths`` where the
proxy's ``_resolve`` reads them.
"""
from __future__ import annotations

import base64
import os
import sys
import uuid
from types import SimpleNamespace

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from user_platform import task_service as task_service_module  # noqa: E402
from user_platform.git_service import git_service as git_service_inst  # noqa: E402

TASK_ID = uuid.uuid4()


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _task() -> SimpleNamespace:
    return SimpleNamespace(id=TASK_ID, config_snapshot={})


@pytest.fixture
def proxy_configured(monkeypatch):
    monkeypatch.setattr(task_service_module, "_git_proxy_origin", lambda: "http://127.0.0.1:8003")
    monkeypatch.setattr(task_service_module, "_git_proxy_control_token", lambda: "control-secret")


def _patch_identity(monkeypatch, *, load_identity, fetch_blob):
    """Stub the git identity + blob fetch the allowlist builder uses.

    ``load_identity`` / ``fetch_blob`` are async funcs taking the real
    signatures' args; the stubs ignore them and return canned values.
    ``_effective_git_identity_id`` is patched separately by each test.
    """
    monkeypatch.setattr(git_service_inst, "load_identity_for_read", load_identity)
    monkeypatch.setattr(git_service_inst, "_build_repo_options", lambda _identity: SimpleNamespace())
    monkeypatch.setattr("user_platform.git_clients.fetch_blob", fetch_blob)


@pytest.mark.asyncio
async def test_proxy_url_embeds_repo_path(monkeypatch):
    monkeypatch.setattr(task_service_module, "_git_proxy_origin", lambda: "http://127.0.0.1:8003")
    monkeypatch.setattr(task_service_module, "_git_proxy_control_token", lambda: "control-secret")
    url = task_service_module._git_proxy_url(str(TASK_ID), "main", "rw", repo_path="o/r.git")
    assert url == (
        f"http://{task_service_module._git_proxy_token(str(TASK_ID), 'main', 'rw')}"
        "@127.0.0.1:8003/api/v1/public/nodes/git/r/o/r.git/"
    )
    plain = task_service_module._git_proxy_url(str(TASK_ID), "main", "ro")
    assert "/api/v1/public/nodes/git/" in plain and "/r/" not in plain


@pytest.mark.asyncio
async def test_rewrite_uses_r_form_and_persists_allowlist(monkeypatch, proxy_configured):
    """A relative ``../lib.git`` submodule resolves to the sibling path, which
    the gateway persists for the proxy's allowlist check."""
    task = _task()
    identity = SimpleNamespace(platform="gitea", access_token="tok", base_url="")

    async def fake_effective_identity(_task_id):
        return uuid.uuid4()

    async def fake_load_identity(_identity_id):
        return identity

    async def fake_fetch_blob(platform, full_name, opts, *, path, ref=""):
        assert path == ".gitmodules"
        return SimpleNamespace(
            content=_b64('[submodule "lib"]\n\tpath = lib\n\turl = ../lib.git\n'),
            is_binary=False,
            sha="x",
        )

    _patch_identity(
        monkeypatch,
        load_identity=fake_load_identity,
        fetch_blob=fake_fetch_blob,
    )
    monkeypatch.setattr(
        task_service_module.TaskService,
        "_effective_git_identity_id",
        staticmethod(fake_effective_identity),
    )

    updates: list[dict] = []

    class _FakeQuery:
        def update(self, **kw):
            updates.append(kw)

            async def _call():
                return 1

            return _call()

    monkeypatch.setattr(task_service_module.Task, "filter", classmethod(lambda cls, **kw: _FakeQuery()))

    service = task_service_module.TaskService()
    req = {"repo": {"repo_url": "https://gitea.example.com/o/r.git", "branch": ""}}
    await service._rewrite_repo_for_git_proxy(req, task)

    assert "/api/v1/public/nodes/git/r/o/r.git/" in req["repo"]["repo_url"]
    for secret in ("username", "password", "token"):
        assert secret not in req["repo"]

    assert updates and updates[0]["config_snapshot"]["submodule_paths"] == ["o/lib.git"]
    assert task.config_snapshot["submodule_paths"] == ["o/lib.git"]


@pytest.mark.asyncio
async def test_rewrite_without_gitmodules_leaves_no_allowlist(monkeypatch, proxy_configured):
    """A repo without ``.gitmodules`` still gets the r/ URL, no allowlist key."""
    task = _task()
    identity = SimpleNamespace(platform="gitea", access_token="tok", base_url="")

    async def fake_effective_identity(_task_id):
        return uuid.uuid4()

    async def fake_load_identity(_identity_id):
        return identity

    async def fake_fetch_blob(platform, full_name, opts, *, path, ref=""):
        return None  # no .gitmodules in the repo

    _patch_identity(
        monkeypatch,
        load_identity=fake_load_identity,
        fetch_blob=fake_fetch_blob,
    )
    monkeypatch.setattr(
        task_service_module.TaskService,
        "_effective_git_identity_id",
        staticmethod(fake_effective_identity),
    )

    service = task_service_module.TaskService()
    req = {"repo": {"repo_url": "https://gitea.example.com/o/r.git", "branch": ""}}
    await service._rewrite_repo_for_git_proxy(req, task)

    assert "/api/v1/public/nodes/git/r/o/r.git/" in req["repo"]["repo_url"]
    assert "submodule_paths" not in task.config_snapshot


@pytest.mark.asyncio
async def test_allowlist_failure_does_not_break_rewrite(monkeypatch, proxy_configured):
    """A .gitmodules fetch failure must not stop the main clone from being
    rewritten — the proxy then simply rejects every non-parent r/ path."""
    task = _task()

    async def fake_effective_identity(_task_id):
        return uuid.uuid4()

    async def fake_load_identity(_identity_id):
        raise RuntimeError("no db in this test")

    monkeypatch.setattr(
        task_service_module.TaskService,
        "_effective_git_identity_id",
        staticmethod(fake_effective_identity),
    )
    monkeypatch.setattr(git_service_inst, "load_identity_for_read", fake_load_identity)
    monkeypatch.setattr(git_service_inst, "_build_repo_options", lambda _identity: SimpleNamespace())

    service = task_service_module.TaskService()
    req = {"repo": {"repo_url": "https://gitea.example.com/o/r.git", "branch": ""}}
    await service._rewrite_repo_for_git_proxy(req, task)

    assert "/api/v1/public/nodes/git/r/o/r.git/" in req["repo"]["repo_url"]
    assert "submodule_paths" not in task.config_snapshot
