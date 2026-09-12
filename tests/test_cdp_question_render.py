"""CDP 网页对话对 ask_user（question 事件）的呈现契约。

历史缺陷：``agent_loop`` 在 ask_user 退出时先发 ``question`` 再发 ``done``，
而 ``_run_cdp_conversation_turn.on_event`` 只认 tool_call/content/reasoning/
tool_result/done —— question 事件被静默丢弃，于是：

1. 扩展面板只见一张 ask_user 工具卡（问题正文一个字不显示）；
2. 收尾把 yield 出来的 ``{"status": "question", ...}`` dict 当正文写进 content；
3. 附件既不落 media 列也不认领归属。

这里锁住修复后的口径：question 文本并入 collected_content、附件抽 media part
并认领给 CDP 客户端 owner、瞬态事件附免登录签名直链、收尾 content 绝不写 dict。
"""
from __future__ import annotations

import asyncio

import pytest

from agent import cdp_chat_service


@pytest.fixture()
def media_env(monkeypatch):
    """伪造 conversation_store.update_message 与附件认领，记录调用字段。"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    updates: list[dict] = []
    claims: list[tuple[int, str, str]] = []

    async def _update_message(message_id, **fields):
        updates.append(fields)

    async def _claim_for_owner(attachment_id, caller, context_ref=None):
        claims.append((attachment_id, caller, context_ref))

    from agent import conversation_store as store_mod

    monkeypatch.setattr(store_mod, "update_message", _update_message)
    import attachment_store

    monkeypatch.setattr(attachment_store, "claim_for_owner", _claim_for_owner)

    yield updates, claims
    loop.close()
    asyncio.set_event_loop(None)


def test_question_event_merges_text_and_claims_media(media_env, monkeypatch):
    """tool_call(tool_result) → question → done 一轮：question 的文本并入正文、
    media part 认领给 owner、SSE 事件带签名直链、收尾 content 不写 dict。"""
    updates, claims = media_env

    # 1) 让 client_owner 解析拿到固定 owner（client_id 非空走 mcp_plugin_store）。
    import mcp_plugin_store

    async def _owner(client_id):
        assert client_id == "7"
        return "user-7"

    monkeypatch.setattr(mcp_plugin_store, "get_cdp_client_owner_user_id", _owner)

    # 2) 签名直链伪造（_sign_question_media 在事件流里现签）。
    import attachment_signing

    monkeypatch.setattr(
        attachment_signing, "sign_attachment_url",
        lambda attachment_id, kind, ttl_seconds=None: f"/agent/attachments/{attachment_id}/{kind}?sig=x",
    )

    # 3) 假 agent_runner_loop：不调 LLM，直接把 ask_user 的 tool_call/question/
    #    done 事件序列回放到 on_event，然后 yield EXITED-question dict（真实
    #    agent_loop 的 should_exit 路径）。
    import agent.agent_loop as loop_mod

    async def _fake_runner(llm, *, system_prompt, user_input, handler, tools_schema,
                           max_turns, on_event=None, on_tool_batch=None, initial_messages=None):
        await on_event({"type": "content", "text": "先看页面"})
        await on_event({"type": "tool_call", "id": "c1", "name": "ask_user", "args": {}, "index": 0})
        await on_event({
            "type": "tool_result", "id": "c1", "name": "ask_user", "index": 0,
            "data": {"status": "question", "question": "继续吗？", "candidates": ["继续", "停"],
                     "media": [{"kind": "attachment", "id": 42, "name": "shot.png",
                                "mime_type": "image/png", "status": "active"}]},
        })
        question = {"status": "question", "question": "继续吗？", "candidates": ["继续", "停"],
                    "media": [{"kind": "attachment", "id": 42, "name": "shot.png",
                               "mime_type": "image/png", "status": "active"}]}
        await on_event({"type": "question", "data": question})
        await on_event({"type": "done", "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}, "model": "m"})
        yield {"result": "EXITED", "data": question}

    monkeypatch.setattr(loop_mod, "agent_runner_loop", _fake_runner)

    # GenericAgent/_build_tool_registry 等在 run_agent 里被调用，全部替身。
    import agent.agent_main as main_mod

    class _FakeGA:
        def __init__(self, agent_id=None, tools=None, scene=None, **_kwargs):
            self._mcp_manager = None

        async def _ensure_config(self):
            from agent.config import AgentConfig
            return AgentConfig(max_turns=2)

        async def _ensure_llm(self):
            return object()

        async def _ensure_resources(self):
            class _Tools:
                def get_schema(self):
                    return []

                def set_code_run_denial(self, _msg):
                    pass

                def set_code_run_approval(self, _batch):
                    pass

            return _Tools(), ""

        def set_event_sink(self, _sink):
            pass

    monkeypatch.setattr(main_mod, "GenericAgent", _FakeGA)
    import agent.api as api_mod

    def _fake_build_registry(agent_id, owner_user_id=None):
        class _R:
            def get_schema(self):
                return []
        return _R()

    monkeypatch.setattr(api_mod, "_build_tool_registry", _fake_build_registry)

    import agent.conversation_store as store_mod

    async def _get_messages(conv_id):
        return []

    monkeypatch.setattr(store_mod, "get_messages", _get_messages)

    # 4) 跑一轮：直接调 client_send_message 太重（要 FastAPI/鉴权全链路），
    #    改为构造 _run_cdp_conversation_turn + 手动消费 SSE。
    loop = asyncio.get_event_loop()

    async def _drive():
        resp = await cdp_chat_service._run_cdp_conversation_turn(
            conv={"id": "conv-1", "agent_id": 5, "system_prompt": ""},
            conv_id="conv-1",
            assistant_msg_id=101,
            agent={"system_prompt": ""},
            scene=None,
            agent_id=5,
            user_input="hi",
            mode="interact",
            goal_config=None,
            max_turns=2,
            effective_effort=None,
            conv_model="",
            client_id="7",
        )
        events = []
        async for chunk in resp.body_iterator:
            text = chunk if isinstance(chunk, str) else chunk.decode("utf-8")
            for line in text.splitlines():
                if line.startswith("data: "):
                    import json as _json
                    events.append(_json.loads(line[6:]))
        return events

    events = loop.run_until_complete(_drive())

    # SSE 流里 question 事件原样回投（扩展面板据此渲染），附件带上了签名直链。
    question_events = [e for e in events if e.get("type") == "question"]
    assert len(question_events) == 1
    signed = question_events[0]["data"]["media"][0].get("content_url")
    assert signed == "/agent/attachments/42/content?sig=x"

    # 落库：question 文本并入正文（带前缀正文），media 列带上附件 part，且附件
    # 已认领给 CDP 客户端 owner。
    final = updates[-1]
    assert final["status"] == "done"
    assert "继续吗？" in final["content"]
    assert "先看页面" in final["content"]
    assert not isinstance(final["content"], dict)
    assert final["media"] and final["media"][0]["attachment_id"] == 42
    assert claims == [(42, "user-7", "conv-1")]
