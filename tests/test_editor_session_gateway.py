from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import main
from user_platform import routes_editors


EDITOR = {"id": "ed_test", "provider": "codex", "status": "active"}
API_KEY = {
    "id": 41,
    "name": "editor-key",
    "scope": "editor",
    "disabled": False,
    "expires_at": None,
    "version": 3,
}
FIRST_CONTENT = "implement the requested change"
CONTENT_HASH = hashlib.sha256(FIRST_CONTENT.encode("utf-8")).hexdigest()
PENDING = {
    "id": "es_pending",
    "editor_id": EDITOR["id"],
    "status": "pending_first_request",
    "expected_client_id": "install-1",
    "bootstrap_content_hash": CONTENT_HASH,
}


def codex_headers(*, thread_id: str = "thread-1", installation_id: str = "install-1") -> dict:
    return {
        "thread-id": thread_id,
        "session-id": thread_id,
        "x-codex-turn-metadata": json.dumps(
            {
                "thread_id": thread_id,
                "session_id": thread_id,
                "installation_id": installation_id,
            }
        ),
    }


def body(content: str = FIRST_CONTENT) -> dict:
    return {"input": [{"role": "user", "content": content}]}


def install_common_stubs(monkeypatch, *, pending=PENDING, bound=None, api_key=API_KEY):
    async def get_api_key_config(_key, include_disabled=False):
        assert include_disabled is True
        return api_key

    async def get_editor_by_api_key_id(api_key_id):
        assert api_key_id == API_KEY["id"]
        return EDITOR

    async def get_editor_by_session_api_key_id(api_key_id):
        # 老编辑器 key 场景：session 未持有该 key，返回 None 走编辑器兜底。
        return None

    async def get_editor_session_by_thread(editor_id, thread_id):
        assert editor_id == EDITOR["id"]
        return bound

    async def get_pending(editor_id, client_id, content_hash):
        assert editor_id == EDITOR["id"]
        if (
            pending
            and client_id == pending["expected_client_id"]
            and content_hash == pending["bootstrap_content_hash"]
        ):
            return pending
        return None

    monkeypatch.setattr(main.config.Config, "get_api_key_config", get_api_key_config)
    monkeypatch.setattr(main.PostgresClient, "get_editor_by_api_key_id", get_editor_by_api_key_id)
    monkeypatch.setattr(main.PostgresClient, "get_editor_by_session_api_key_id", get_editor_by_session_api_key_id)
    monkeypatch.setattr(main.PostgresClient, "get_editor_session_by_thread", get_editor_session_by_thread)
    monkeypatch.setattr(
        main.PostgresClient,
        "get_pending_editor_session_for_first_request",
        get_pending,
    )


@pytest.mark.asyncio
async def test_codex_first_request_matches_preregistered_client_and_content(monkeypatch):
    install_common_stubs(monkeypatch)

    context = await main._validate_editor_request_context(
        "sk-editor", codex_headers(), body()
    )

    assert context["first_request"] is True
    assert context["provider_thread_id"] == "thread-1"
    assert context["session"]["id"] == PENDING["id"]
    assert context["api_key_version"] == 3


@pytest.mark.asyncio
async def test_codex_first_request_rejects_wrong_installation(monkeypatch):
    install_common_stubs(monkeypatch)

    with pytest.raises(HTTPException, match="Codex 首请求未命中预注册会话") as exc:
        await main._validate_editor_request_context(
            "sk-editor", codex_headers(installation_id="install-other"), body()
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_codex_first_request_rejects_wrong_content(monkeypatch):
    install_common_stubs(monkeypatch)

    with pytest.raises(HTTPException, match="Codex 首请求未命中预注册会话") as exc:
        await main._validate_editor_request_context(
            "sk-editor", codex_headers(), body("different content")
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_codex_unknown_thread_without_pending_session_is_rejected(monkeypatch):
    install_common_stubs(monkeypatch, pending=None)

    with pytest.raises(HTTPException, match="Codex 首请求未命中预注册会话") as exc:
        await main._validate_editor_request_context(
            "sk-editor", codex_headers(thread_id="unknown"), body()
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_codex_bound_thread_skips_first_request_bootstrap(monkeypatch):
    active = {
        "id": "es_active",
        "editor_id": EDITOR["id"],
        "status": "active",
        "provider_thread_id": "thread-bound",
    }
    install_common_stubs(monkeypatch, bound=active)

    context = await main._validate_editor_request_context(
        "sk-editor",
        codex_headers(thread_id="thread-bound", installation_id=""),
        {"input": []},
    )

    assert context["first_request"] is False
    assert context["session"] == active


@pytest.mark.asyncio
async def test_disabled_editor_key_is_rejected_before_session_lookup(monkeypatch):
    disabled = {**API_KEY, "disabled": True}
    install_common_stubs(monkeypatch, api_key=disabled)

    with pytest.raises(HTTPException, match="Editor API Key 已停用") as exc:
        await main._validate_editor_request_context(
            "sk-editor", codex_headers(), body()
        )

    assert exc.value.status_code == 403



def test_first_user_content_supports_messages_and_responses_input():
    assert main._first_user_content(
        {"messages": [{"role": "user", "content": FIRST_CONTENT}]}
    ) == FIRST_CONTENT
    assert main._first_user_content(
        {
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "first "},
                        {"type": "input_text", "text": "message"},
                    ],
                }
            ]
        }
    ) == "first message"


def test_request_log_insert_params_includes_key_lineage_snapshot():
    from db import PostgresClient

    params = PostgresClient._request_log_insert_params(
        {
            "request_id": "req-1",
            "attempt_key": "attempt-1",
            "api_key_id": 41,
            "api_key_parent_id": 7,
            "api_key_version": 3,
            "api_key_name_snapshot": "editor-key",
            "success": True,
        }
    )

    # editor_id=12, editor_session_id=13, task_id=14, api_key_version=15,
    # api_key_name_snapshot=16, api_key_id=17, api_key_parent_id=18.
    assert params[14] is None  # task_id not provided in this row
    assert params[15] == 3
    assert params[16] == "editor-key"
    assert params[17] == 41
    assert params[18] == 7


def test_request_log_insert_params_writes_task_id_snapshot():
    from db import PostgresClient

    params = PostgresClient._request_log_insert_params(
        {
            "request_id": "req-2",
            "attempt_key": "attempt-2",
            "task_id": "00000000-0000-0000-0000-000000000abc",
            "editor_id": "ed_1",
            "editor_session_id": "es_1",
            "success": True,
        }
    )

    assert params[12] == "ed_1"
    assert params[13] == "es_1"
    assert params[14] == "00000000-0000-0000-0000-000000000abc"


def test_channel_attempt_log_preserves_task_id_snapshot():
    log = main._build_channel_attempt_log(
        request_id="req-3",
        route_info={
            "provider": "codex",
            "account": "account-1",
            "attempt_key": "attempt-3",
            "attempt_no": 1,
            "task_id": "00000000-0000-0000-0000-000000000abc",
        },
        model="gpt-test",
        messages=[],
        stream=False,
        api_key="sk-test",
        api_key_name="task-key",
        request_headers={},
        start_time=0,
        duration_ms=1,
        success=True,
        status="ok",
        response_body={},
        error="",
    )

    assert log["task_id"] == "00000000-0000-0000-0000-000000000abc"



@pytest.mark.asyncio
async def test_editor_child_usage_limit_blocks_when_request_count_reached(monkeypatch):
    limited = {**API_KEY, "usage_limit": {"max_requests": 2}}

    async def usage_totals(api_key_id, include_children=False):
        assert api_key_id == API_KEY["id"]
        assert include_children is False
        return {"requests": 2, "total_tokens": 0}

    monkeypatch.setattr(main.PostgresClient, "api_key_usage_totals", usage_totals)
    install_common_stubs(monkeypatch, api_key=limited)

    with pytest.raises(HTTPException, match="Editor API Key 请求额度已用尽") as exc:
        await main._validate_editor_request_context("sk-editor", codex_headers(), body())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_editor_parent_usage_limit_counts_children(monkeypatch):
    child = {**API_KEY, "parent_id": 7, "usage_limit": {}}
    parent = {**API_KEY, "id": 7, "parent_id": None, "usage_limit": {"max_total_tokens": 10}}
    calls: list[tuple[int, bool]] = []

    async def get_parent(api_key_id):
        assert api_key_id == 7
        return parent

    async def usage_totals(api_key_id, include_children=False):
        calls.append((api_key_id, include_children))
        if api_key_id == 7:
            return {"requests": 1, "total_tokens": 10}
        return {"requests": 0, "total_tokens": 0}

    # 父 Key 回查走内存快照（鉴权在每请求链路上，不查库），所以 stub 打在 Config 上。
    monkeypatch.setattr(main.config.Config, "get_api_key_by_id", get_parent)
    monkeypatch.setattr(main.PostgresClient, "api_key_usage_totals", usage_totals)
    install_common_stubs(monkeypatch, api_key=child)

    with pytest.raises(HTTPException, match="Editor API Key Token 额度已用尽") as exc:
        await main._validate_editor_request_context("sk-editor", codex_headers(), body())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_switch_editor_session_model_pushes_llm_then_persists(monkeypatch):
    calls: list[tuple[str, str, object]] = []
    editor = {
        "id": "ed_test",
        "provider": "codex",
        "api_key_id": 41,
        "status": "active",
    }
    session = {
        "id": "es_active",
        "editor_id": "ed_test",
        "node_session_id": "node-session-1",
        "status": "active",
        "model": "old-model",
    }

    async def get_editor_for_user(editor_id, owner_user_id):
        assert (editor_id, owner_user_id) == ("ed_test", "user-1")
        return editor

    async def get_editor_session(editor_id, session_id):
        assert (editor_id, session_id) == ("ed_test", "es_active")
        return session

    async def get_api_key_by_id(api_key_id):
        assert api_key_id == 41
        return {"id": 41, "key": "sk-editor-child", "disabled": False}

    async def set_editor_session_model(editor_id, session_id, model):
        calls.append(("persist", session_id, model))
        return {**session, "model": model}

    async def audit_user_action(*_args, **_kwargs):
        calls.append(("audit", "", None))

    class NodeClient:
        async def configure_node_session_llm(self, session_id, llm):
            calls.append(("configure", session_id, llm))

        async def restart_node_session_runtime(self, session_id):
            calls.append(("restart", session_id, None))

    monkeypatch.setattr(routes_editors.PostgresClient, "get_editor_for_user", get_editor_for_user)
    monkeypatch.setattr(routes_editors.PostgresClient, "get_editor_session", get_editor_session)
    monkeypatch.setattr(routes_editors.PostgresClient, "get_api_key_by_id", get_api_key_by_id)
    monkeypatch.setattr(routes_editors.PostgresClient, "set_editor_session_model", set_editor_session_model)
    monkeypatch.setattr(routes_editors, "get_local_node_client", lambda: NodeClient())
    monkeypatch.setattr(routes_editors, "audit_user_action", audit_user_action)
    monkeypatch.setattr(routes_editors, "_editor_gateway_endpoint", lambda _request: "https://gw.example.com/v1")
    # 模型校验收口在 _editor_llm_config：目标模型必须可供给且被该 Key 放行。
    import config as gateway_config
    from rate_limiter import ModelClientPool
    monkeypatch.setattr(ModelClientPool, "available_model_ids", classmethod(lambda cls: {"new-model"}))
    async def _allows(cls, api_key, model):
        return True
    monkeypatch.setattr(gateway_config.Config, "api_key_allows_model", classmethod(_allows))

    request = SimpleNamespace(url=SimpleNamespace(scheme="http", netloc="127.0.0.1:8001"))
    user = SimpleNamespace(id="user-1")
    result = await routes_editors.switch_editor_session_model(
        "ed_test",
        "es_active",
        routes_editors.UpdateSessionReq(model="new-model"),
        request,
        user,
    )

    assert result["model"] == "new-model"
    assert calls[0] == (
        "configure",
        "node-session-1",
        {
            "endpoint": "https://gw.example.com/v1",
            "api_key": "sk-editor-child",
            "model": "new-model",
            "protocol": "responses",
        },
    )
    # No restart: the model change is a pure disk write that takes effect on the
    # next turn (the node stamps the new model onto the next human_message frame).
    assert calls[1] == ("persist", "es_active", "new-model")
    assert not any(call[0] == "restart" for call in calls)
