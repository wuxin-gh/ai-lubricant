"""Issue binding on editor session creation (分配任务 → createProjectEditorSession).

The unified task dialog sends `issue_id` (+ `task_role`/`sub_type`) on the
editor-session create endpoint. The server must:
- reject when the issue is not under the editor's project (404);
- reject when task_role doesn't match the issue type (422);
- reject when the issue isn't unassigned or the transition is illegal (409);
- reject when an active session for the same (issue, role) already exists (409);
- on success, mint an identity token, append the issue-workflow SSE MCP, and
  advance the issue status only after the runtime session actually starts.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from user_platform import routes_editors


EDITOR = {
    "id": "ed_test",
    "provider": "claude",
    "project_id": uuid.uuid4(),
    "node_id": "node-1",
    "mcp_config": [],
    "skill_config": [],
    "plugin_config": [],
    "workdir": "editors/ed_test",
    "branch_mode": "default",
    "branch": "",
}
PARENT_KEY_ID = 41
SESSION_ROW = {
    "id": "es_new",
    "editor_id": EDITOR["id"],
    "api_key_id": PARENT_KEY_ID,
    "api_key_copy": {"key": "sk-child"},
}


def _issue_row(*, issue_type="requirement", status="unassigned", project_id=None):
    class _Issue:
        def __init__(self):
            self.id = uuid.uuid4()
            self.project_id = project_id or EDITOR["project_id"]
            self.issue_type = issue_type
            self.status = status
            self.design_document = None
            self.bug_reason = None
            self.resolution_note = None
            self.pending_items = []
            self.updated_at = None

        async def save(self, update_fields=None):
            return None

    return _Issue()


def _install_basic(monkeypatch, *, issue=None, duplicate=None, session=SESSION_ROW):
    """Patch everything _create_editor_session_core touches below the issue block.

    The issue block runs first; these stubs cover the post-issue success path so
    happy-path tests can assert the MCP entry lands in dispatch.mcps.
    """
    async def get_editor_for_user(_editor_id, _user_id):
        return EDITOR

    async def resolve_allowed_parent_key_ids(_user_id):
        return {PARENT_KEY_ID}

    async def create_editor_session(_editor_id, _model, *_args, **_kwargs):
        return session

    async def get_active_editor_session_for_issue(_issue_id, _task_role):
        return duplicate

    async def set_editor_session_status(_editor_id, _session_id, _status):
        return None

    async def set_editor_session_mcp_overlay(_editor_id, _session_id, overlay):
        session["mcp_overlay_json"] = overlay or []
        return session

    async def bind_editor_session_node(_editor_id, _session_id, node_session_id, _client_id):
        return {**session, "node_session_id": node_session_id}

    issued_token = {"token_row": object(), "display_token": None}

    async def issue_token(_target_type, _target_id, display_token=False):
        issued_token["display_token"] = display_token
        return issued_token["token_row"], "issue-workflow-identity-token"

    dispatch_payload: dict = {}

    class NodeClient:
        async def dispatch_session(self, node_id, payload):
            dispatch_payload.update(payload)
            return {"accepted": True, "sessionId": "node-session-1"}

        async def start_node_session_runtime(self, _node_session_id):
            return None

    monkeypatch.setattr(routes_editors.PostgresClient, "get_editor_for_user", get_editor_for_user)
    monkeypatch.setattr(routes_editors, "resolve_allowed_parent_key_ids", resolve_allowed_parent_key_ids)
    monkeypatch.setattr(routes_editors.PostgresClient, "create_editor_session", create_editor_session)
    monkeypatch.setattr(
        routes_editors.PostgresClient,
        "get_active_editor_session_for_issue",
        get_active_editor_session_for_issue,
    )
    monkeypatch.setattr(routes_editors.PostgresClient, "set_editor_session_status", set_editor_session_status)
    monkeypatch.setattr(routes_editors.PostgresClient, "set_editor_session_mcp_overlay", set_editor_session_mcp_overlay)
    monkeypatch.setattr(routes_editors.PostgresClient, "bind_editor_session_node", bind_editor_session_node)

    import builtin_tool_store

    monkeypatch.setattr(builtin_tool_store, "issue_token", issue_token)
    monkeypatch.setattr(routes_editors, "get_local_node_client", lambda: NodeClient())

    async def audit_user_action(*_args, **_kwargs):
        return None

    monkeypatch.setattr(routes_editors, "audit_user_action", audit_user_action)

    # Explicit node requires a node-permission check; stub it as allowed.
    from user_platform import nodes_service

    async def user_can_use_node(_user_id, _node_id):
        return True

    monkeypatch.setattr(nodes_service.nodes_service, "user_can_use_node", user_can_use_node)

    # _editor_llm_config reaches into PostgresClient.get_api_key_by_id.
    async def get_api_key_by_id(_key_id):
        return {"id": PARENT_KEY_ID, "key": "sk-parent", "disabled": False}

    monkeypatch.setattr(routes_editors.PostgresClient, "get_api_key_by_id", get_api_key_by_id)

    # 创建会话与下发前的模型写入边界校验：目标模型必须可供给且被该 Key 放行。
    import config as gateway_config
    from rate_limiter import ModelClientPool

    monkeypatch.setattr(ModelClientPool, "available_model_ids", classmethod(lambda cls: {"m"}))

    async def _allows(cls, _api_key, _model):
        return True

    monkeypatch.setattr(gateway_config.Config, "api_key_allows_model", classmethod(_allows))

    # And the issue lookup itself.
    import user_platform.models_project as models_project

    async def project_issue_get_or_none(id=None, project_id=None):
        if issue is None:
            return None
        if str(id) != str(issue.id):
            return None
        if str(project_id) != str(issue.project_id):
            return None
        return issue

    monkeypatch.setattr(models_project.ProjectIssue, "get_or_none", project_issue_get_or_none)

    # Server URL for the SSE MCP entry. settings is a frozen dataclass; the route
    # reads `from .config import settings; settings.server_url`. Swap the whole
    # module attribute for a tiny stand-in so the frozen instance stays intact.
    from user_platform import config

    class _Settings:
        server_url = "https://gw.example.com"
        # 运行时 LLM endpoint 走 _editor_gateway_endpoint → gateway_public_url。
        gateway_public_url = "https://gw.example.com"
        node_server_public_url = ""

    monkeypatch.setattr(config, "settings", _Settings(), raising=False)

    return dispatch_payload


def _request_user():
    request = SimpleNamespace(url=SimpleNamespace(scheme="http", netloc="127.0.0.1:8001"))
    user = SimpleNamespace(id="user-1")
    return request, user


def test_merge_mcp_config_preserves_session_overlay_and_overlay_wins():
    merged = routes_editors._merge_mcp_config(
        [
            {"name": "shared", "url": "https://old"},
            {"name": "issue-workflow", "url": "https://editor-should-not-win"},
        ],
        [
            {"name": "issue-workflow", "url": "https://session-token"},
            {"name": "extra", "url": "https://extra"},
        ],
    )
    by_name = {item["name"]: item for item in merged}
    assert by_name["shared"]["url"] == "https://old"
    assert by_name["issue-workflow"]["url"] == "https://session-token"
    assert by_name["extra"]["url"] == "https://extra"


@pytest.mark.asyncio
async def test_issue_not_in_editors_project_is_404(monkeypatch):
    # issue.project_id differs from editor.project_id → get_or_none returns None.
    other_project = uuid.uuid4()
    issue = _issue_row(project_id=other_project)
    _install_basic(monkeypatch, issue=None)  # get_or_none sees mismatched project_id

    # Re-stub get_or_none so the project_id check fails against the editor's project.
    import user_platform.models_project as models_project

    async def get_or_none(id=None, project_id=None):
        # Route passes EDITOR["project_id"], but the row belongs to other_project.
        return (
            issue
            if str(id) == str(issue.id) and str(project_id) == str(issue.project_id)
            else None
        )

    monkeypatch.setattr(models_project.ProjectIssue, "get_or_none", get_or_none)

    request, user = _request_user()
    body = routes_editors.CreateSessionReq(
        parent_api_key_id=PARENT_KEY_ID,
        model="m",
        models=["m"],
        first_content="do it",
        task_role="design",
        sub_type="generate_design",
        issue_id=str(uuid.uuid4()),
    )
    with pytest.raises(HTTPException) as exc:
        await routes_editors._create_editor_session_core(EDITOR["id"], body, request, user, EDITOR)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_role_mismatch_with_issue_type_is_422(monkeypatch):
    # bug issue + task_role=design → not in {diagnose, fix}.
    issue = _issue_row(issue_type="bug")
    _install_basic(monkeypatch, issue=issue)

    request, user = _request_user()
    body = routes_editors.CreateSessionReq(
        parent_api_key_id=PARENT_KEY_ID,
        model="m",
        models=["m"],
        first_content="do it",
        task_role="design",
        sub_type="generate_design",
        issue_id=str(issue.id),
    )
    with pytest.raises(HTTPException) as exc:
        await routes_editors._create_editor_session_core(EDITOR["id"], body, request, user, EDITOR)
    assert exc.value.status_code == 422


@pytest.mark.asyncio
async def test_non_unassigned_issue_is_409(monkeypatch):
    issue = _issue_row(status="designing")
    _install_basic(monkeypatch, issue=issue)

    request, user = _request_user()
    body = routes_editors.CreateSessionReq(
        parent_api_key_id=PARENT_KEY_ID,
        model="m",
        models=["m"],
        first_content="do it",
        task_role="develop",
        sub_type="execute_task",
        issue_id=str(issue.id),
    )
    with pytest.raises(HTTPException) as exc:
        await routes_editors._create_editor_session_core(EDITOR["id"], body, request, user, EDITOR)
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_duplicate_active_session_for_same_role_is_409(monkeypatch):
    issue = _issue_row()
    _install_basic(monkeypatch, issue=issue, duplicate={"id": "es_existing"})

    request, user = _request_user()
    body = routes_editors.CreateSessionReq(
        parent_api_key_id=PARENT_KEY_ID,
        model="m",
        models=["m"],
        first_content="do it",
        task_role="design",
        sub_type="generate_design",
        issue_id=str(issue.id),
    )
    with pytest.raises(HTTPException) as exc:
        await routes_editors._create_editor_session_core(EDITOR["id"], body, request, user, EDITOR)
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_happy_path_appends_issue_workflow_mcp_and_advances_status(monkeypatch):
    issue = _issue_row(issue_type="requirement", status="unassigned")
    dispatch_payload = _install_basic(monkeypatch, issue=issue)

    request, user = _request_user()
    body = routes_editors.CreateSessionReq(
        parent_api_key_id=PARENT_KEY_ID,
        model="m",
        models=["m"],
        first_content="do it",
        task_role="design",
        sub_type="generate_design",
        issue_id=str(issue.id),
    )
    await routes_editors._create_editor_session_core(EDITOR["id"], body, request, user, EDITOR)

    # issue-workflow SSE entry appended to the dispatched mcps.
    mcps = dispatch_payload.get("mcps") or []
    issue_mcp = next((m for m in mcps if m.get("name") == "issue-workflow"), None)
    assert issue_mcp is not None
    assert issue_mcp["type"] == "sse"
    assert "token=issue-workflow-identity-token" in issue_mcp["url"]
    assert issue_mcp["url"].startswith("https://gw.example.com/mcp/issue-workflow/sse")

    # Status advanced only after runtime start succeeded.
    assert issue.status == "designing"
