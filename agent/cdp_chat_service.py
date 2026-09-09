"""Token-scoped agent endpoints for the cdp-bridge webpage chat panel.

These endpoints are called only by mcp_runtime (via X-Internal-Token), not by browsers.
The CDP identity is injected by the authenticated runtime connection; chat frames
never carry credentials. Endpoints enforce:
- CDP client exists / enabled
- requested agent is in the client's operable agent_ids
- client identity is coherent and conversations remain client-owned

网页对话由 CDP 客户端驱动（会话池按 client_id 隔离）；可操作 agent 由该客户端的
agent_ids 决定（未选则不允许操作）。这与 agent 侧绑定的 MCP 用户相互独立。

会话归属按 client_id，不按 session_key：session_key 里的 tabId 随标签页关闭失效，
拿它做归属会让用户换标签页后既看不到也续不了自己的历史（数据仍在，只是被滤掉）。
历史属于这个客户端（这个浏览器），不属于某个已经不存在的标签页；客户端之间仍严格隔离。

场景预置走 ``agent.scene_context``：建会话时把稳定身份键（client_id + tab_id）一次性
写进 conversation system_prompt，后续轮次不再重写。当前页面 URL/标题是易变内容，
不进提示词——模型需要时用 web operation=scan/tabs 现取，避免沿用上一轮的旧快照。
换标签页续聊时 ``_rebind_conv_scene`` 把场景段重绑到本轮所在标签页，避免模型照着
一个已经关掉的 tab_id 操作。
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
from typing import Any, Literal

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from loguru import logger

from agent.api import (
    _build_tool_registry,
    _CONV_TASKS,
    _stream_headers,
    _require_ch,
    _normalize_reasoning_effort,
)
from user_platform.node_client.approvals import ApprovalDenied, ApprovalTimeout
from agent.config import AgentConfig
from agent.context_manager import rebuild_history_messages, attach_tool_result
from agent import conversation_store, scene_context


router = APIRouter(prefix="/agent/client", tags=["agent-client"])


def _require_internal(authorization: str | None, internal_token: str | None) -> None:
    expected = os.environ.get("AGENT_INTERNAL_TOKEN", "")
    if not expected or not internal_token or internal_token != expected:
        raise HTTPException(status_code=401, detail="invalid internal token")


async def _require_chat_client(client_id: str | None) -> list[int]:
    """校验发起对话的 CDP 客户端存在且已启用，返回其**实际可操作** agent_ids。

    网页对话改由 CDP 客户端驱动：门槛是「client 启用且可操作 agent 非空」，
    不再依赖 mcp_users.chat_enabled。返回集 = 客户端勾选的 agent_ids ∩ 绑定
    principal 带该 cdp_client_id param 的 agent（后者才是真能驱动这个浏览器的）。
    与面板 chat_list_agents 同源，避免「列表里没有但仍能发消息」。
    """
    import mcp_plugin_store

    cid = (client_id or "").strip()
    if not cid:
        raise HTTPException(status_code=401, detail="missing cdp client identity")
    selected = await mcp_plugin_store.get_cdp_client_agent_ids(cid)
    if selected is None:
        raise HTTPException(status_code=401, detail="invalid cdp client identity")
    authorized = await mcp_plugin_store.list_agent_ids_authorized_for_cdp_client(cid)
    agent_ids = [aid for aid in selected if aid in authorized]
    if not agent_ids:
        raise HTTPException(status_code=403, detail="no operable agent for this client")
    return agent_ids


async def _require_client_agent(agent_ids: list[int], agent_id: int) -> dict:
    from db import PostgresClient

    agent = await PostgresClient.get_agent(agent_id)
    if not agent or not agent.get("enabled"):
        raise HTTPException(status_code=404, detail="agent not found")
    if agent_id not in agent_ids:
        raise HTTPException(status_code=403, detail="agent is not allowed for this client")
    return agent


class ClientGoalConfig(BaseModel):
    """goal 模式配置，与 agent.api.GoalConfig 同形。"""

    objective: str = ""
    budget_seconds: int = Field(default=900, ge=60)
    max_turns: int | None = Field(default=None, ge=1)


class ClientSendMessageRequest(BaseModel):
    agent_id: int
    content: str = Field(..., min_length=1)
    tab_id: int | str | None = None
    url: str = ""
    title: str = ""
    conversation_id: str = ""
    max_turns: int | None = Field(default=None, ge=1)
    # 对话级覆盖（面板输入区选择）。None = 本轮不改，沿用会话已存值/Agent 默认。
    model: str | None = None
    reasoning_effort: str | None = None
    # auto：与 interact 同执行流程（免审批范围待定，见 agent-mode-menu.tsx 注释）。
    mode: Literal["interact", "plan", "goal", "auto"] = "interact"
    goal_config: ClientGoalConfig | None = None


class ClientCreateConversationRequest(BaseModel):
    agent_id: int
    title: str = ""


class ClientApprovalRequest(BaseModel):
    """审批裁决，与 agent.api.ApprovalRequest 同形。command_hash 防止批准到被改写的代码。"""

    result: str  # "allow" | "deny"
    command_hash: str = ""


@router.get("/agents")
async def client_list_agents(
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    authorization: str | None = Header(None),
) -> dict:
    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    await _require_chat_client(client_id)
    import mcp_plugin_store

    agents = await mcp_plugin_store.list_agents_for_cdp_client(client_id)
    return {"agents": agents, "client": {"id": client_id}}


@router.get("/models")
async def client_list_models(
    agent_id: int,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    authorization: str | None = Header(None),
) -> dict:
    """面板模型下拉：该 agent 绑定的网关 key 白名单内可用模型（含自定义组）。

    CDP 无登录 caller：不能走 _resolve_caller_api_key 的归属校验，直接按 agent
    记录里的 main_api_key_id 取明文 key，再按该 key 的白/黑名单过滤模型列表。
    过滤/说明拼装口径与 agent/api.py:chat_available_models 一致。
    """
    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    agent_ids = await _require_chat_client(client_id)
    agent = await _require_client_agent(agent_ids, int(agent_id))

    from db import PostgresClient
    from agent.api import _model_description
    from rate_limiter import ModelClientPool

    default_model = (agent.get("main_model") or agent.get("model") or "").strip()
    api_key_id = agent.get("main_api_key_id")

    # 取该 key 的明文：复用 gateway key 解析，caller=None 走管理员上下文全量查找。
    api_key = None
    if api_key_id and PostgresClient.pool:
        from agent.api import _resolve_caller_api_key

        try:
            resolved = await _resolve_caller_api_key(int(api_key_id), None)
            api_key = resolved["key"]
        except Exception:  # noqa: BLE001 — key 不可用时不阻断面板，退化为不带 key 的目录
            api_key = None

    # 没绑 key / key 不可用时也走同一条收口：拿到的是未按 key 收窄但已按可用性与
    # 内部 id 过滤的目录，绝不把 agent 行里的 default_model 原样回显——那个字段
    # 不受渠道禁用与 key 名单约束，直接回显会让面板列出跑不通的模型。
    resp = await ModelClientPool.get_models_response(api_key=api_key)
    out: list[dict] = []
    for m in resp.get("data", []) if isinstance(resp, dict) else []:
        item = dict(m)
        item["description"] = _model_description(item)
        out.append(item)

    # default_model 只在它确实出现在过滤后的列表里才回给面板，避免下拉预选一个
    # 已被禁用或已被 key 拉黑的模型。
    if default_model and not any(m.get("id") == default_model for m in out):
        default_model = ""
    return {"data": out, "default_model": default_model}


async def _run_cdp_conversation_turn(
    *,
    conv: dict,
    conv_id: str,
    assistant_msg_id: int,
    agent: dict,
    scene,
    agent_id: int,
    user_input: str,
    mode: str | None,
    goal_config,
    max_turns: int | None,
    effective_effort: str | None,
    conv_model: str,
    client_id: str = "",
) -> StreamingResponse:
    """CDP 网页对话一轮 agent 运行 + SSE 回投。client_send_message 与
    client_retry_message 共用：前者新建 user+assistant 占位后调用，后者复用既有
    失败 assistant 消息（reset 为 streaming）后调用。history 由 get_messages 读取
    并排除 assistant_msg_id，两种入口上下文构建同形。
    """
    queue: asyncio.Queue = asyncio.Queue()
    collected_tool_calls: list[dict[str, Any]] = []
    collected_tool_results: list[Any] = []
    collected_usage: dict[str, Any] | None = None
    collected_reasoning = ""
    collected_content = ""  # 流式正文增量累积，供中途落库；run_agent 结尾仍以 final_content 为准。
    _last_persist_ts = 0.0  # 节流：正文/思考高频到达时最多每 1.5s 落一次中间态。

    # 先把 conversation_id 发给扩展，便于后续 abort / 多轮。
    await queue.put({"type": "conversation", "conversation_id": conv_id})

    async def checkpoint() -> None:
        """把当前已累积的正文/思考/工具调用/结果渐进写回 assistant 消息，status 仍留 streaming。

        与 agent/api.py 的 _persist_progress 同口径：网页面板在整页导航/刷新后会
        丢掉 SSE 监听，只能靠回读会话续看。若进度只在回合结束时才落库，重载后那条
        消息会一直是空壳 + 永久转圈。工具边界立即落，正文/思考 1.5s 节流落；正常
        结束时结尾的 update_message(status="done") 会覆盖成最终态，渐进写不会错乱。
        """
        nonlocal _last_persist_ts
        try:
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                status="streaming",
                reasoning=collected_reasoning,
            )
            _last_persist_ts = time.monotonic()
        except Exception:  # noqa: BLE001 — 中途落库失败不能中断主流程
            pass

    async def on_event(event: dict):
        nonlocal collected_usage, collected_reasoning, collected_content
        etype = event.get("type")
        if etype == "tool_call":
            collected_tool_calls.append({
                # 见 agent/api.py 同处：id 是回放时还原 role=tool 配对的唯一依据。
                "id": event.get("id"),
                "name": event.get("name"),
                "args": event.get("args"),
                "status": "running",
            })
            # 工具调用是天然检查点：立即落库，保证刷新后看得到「说了什么、做了哪步」。
            await checkpoint()
        elif etype == "content":
            collected_content += str(event.get("text") or "")
            # 高频事件：节流落库，1.5s 一次，保证重载前已流出的正文不丢。
            if time.monotonic() - _last_persist_ts > 1.5:
                await checkpoint()
        elif etype == "reasoning":
            collected_reasoning += str(event.get("text") or "")
            if time.monotonic() - _last_persist_ts > 1.5:
                await checkpoint()
        elif etype == "tool_result":
            # agent_loop 的 index 是「本轮第几个」，跨轮重置；按 id(tool_call_id)
            # 匹配，否则第二轮的 result 会错配到第一轮的 call，让真正在跑的那个
            # 永远停在 running（历史回放就显示永久转圈）。
            attach_tool_result(
                collected_tool_calls, collected_tool_results,
                call_id=event.get("id"),
                index=event.get("index"),
                result=event.get("data"),
            )
            await checkpoint()
        elif etype == "done" and isinstance(event.get("usage"), dict):
            collected_usage = event["usage"]
        await queue.put(event)

    async def run_agent():
        ga = None
        try:
            from agent.agent_main import GenericAgent
            from agent.agent_loop import agent_runner_loop, BaseHandler as _BH

            # 网页对话没有 C 端 session，只有 X-Cdp-Client-Id：反查客户端所属用户，
            # 作为本轮 AI 副作用（如建定时任务）的归属人。失败留 None——add_job 兜底
            # 会按 agent_id 查 agents.user_id；都拿不到才落 NULL（用户态不可见）。
            client_owner: str | None = None
            if client_id:
                try:
                    import mcp_plugin_store
                    client_owner = await mcp_plugin_store.get_cdp_client_owner_user_id(client_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"解析 CDP 客户端 {client_id} owner 失败: {exc}")

            agent_system_prompt = agent.get("system_prompt") or ""
            # Agent LLM 只走网关。让 GenericAgent 按 agent_id 从数据库加载权威的
            # main_api_key_id/main_model；传入任何临时 AgentConfig 都会让
            # _ensure_config 误判配置已就绪而跳过这一步。
            # scene=：让 _ensure_resources 把 cdp-bridge 的完整方法/参数说明内联进
            # 首轮系统提示，而不是只给一个还要 file_read 的 SOP 指针。
            ga = GenericAgent(agent_id=agent_id, tools=_build_tool_registry(agent_id, owner_user_id=client_owner), scene=scene)
            config = await ga._ensure_config()

            # 场景段建会话时已写进 conv.system_prompt，这里直接读，不再每轮重拼。
            effective_system_prompt = (conv.get("system_prompt") or "").strip() or agent_system_prompt or ""

            # 历史重建走 rebuild_history_messages：把落库的展示态 tool_calls
            # 归一成 OpenAI 线形状并补出配对的 role=tool，与在途轮次同形。
            history = await conversation_store.get_messages(conv_id)
            initial_messages = rebuild_history_messages(
                history,
                skip_msg_id=assistant_msg_id,
                system_prompt=effective_system_prompt,
            )

            ga.set_event_sink(on_event)
            llm = await ga._ensure_llm()
            # 对话级覆盖：复用主 Agent 网关 key，仅换模型/思考等级（fork 保留 key，
            # 计费/归属仍归主 key）。口径与 agent/api.py send_message 一致。
            if conv_model and conv_model != getattr(llm, "model", None):
                llm = llm.fork(model=conv_model)
            if effective_effort is not None:
                llm = llm.fork(
                    thinking_enabled=bool(effective_effort),
                    reasoning_effort=effective_effort,
                )
            effective_model = getattr(llm, "model", "") or ""
            tools, skill_index = await ga._ensure_resources()
            # code_run 审批协调器：与用户端 agent/api.py send_message 同口径。
            # Agent 级开关（agents.browser_code_run_enabled，默认关）没开时不挂
            # 协调器，code_run 直接 denied 并在文案里提示开启配置——开关的语义是
            # 「浏览器对话根本不能用 code_run」，不是「能用但要审批」。
            if bool(agent.get("browser_code_run_enabled")):
                from agent.code_run_approval import CodeRunApprovalBatch

                approval_timeout = int(getattr(config, "approval_timeout_seconds", 0) or 0)
                code_run_batch = CodeRunApprovalBatch(
                    conversation_id=conv_id,
                    agent_id=int(agent_id or 0),
                    # requester 落 CDP 客户端所属用户；拿不到留 None（审计里显示未知发起人）。
                    caller=client_owner,
                    emit=on_event,
                    timeout_seconds=approval_timeout,
                )
                tools.set_code_run_approval(code_run_batch)

                async def _prepare_tool_batch(tool_calls: list[dict]) -> None:
                    await code_run_batch.prepare(tool_calls)
            else:
                tools.set_code_run_denial(
                    "code_run 在浏览器对话中被禁用：该 Agent 未开启「允许浏览器执行 code_run」。"
                    "请用户在用户端 Agent 配置页（安全设置）打开该开关后再重试；"
                    "在此之前请改用其它方式完成任务，不要反复调用 code_run。"
                )

                async def _prepare_tool_batch(tool_calls: list[dict]) -> None:
                    # 开关关闭时无需审批预处理；保留空实现以维持 on_tool_batch 契约。
                    return None

            # 注入 GA 的 L1 memory index / [Available Capabilities] / SOP 指引，
            # 与用户端 agent/api.py send_message 同口径——CDP 路径此前漏了，
            # 导致 agent 没见过 browser_sop.md，跳转页面只会直接 browser_navigate。
            if skill_index:
                effective_system_prompt = f"{effective_system_prompt}\n\n{skill_index}".strip()
                if initial_messages and initial_messages[0].get("role") == "system":
                    initial_messages[0]["content"] = effective_system_prompt
                else:
                    initial_messages.insert(0, {"role": "system", "content": effective_system_prompt})

            # GA 执行模式：plan/goal 与用户端 Agent 聊天页同源（agent/api.py）。
            mode_eff = (mode or "interact").lower()
            goal_state_file = ""
            if mode_eff == "plan":
                from agent.plan_mode import PlanModeManager

                plan_mgr = PlanModeManager(int(agent_id or 0))
                plan_session = plan_mgr.create(
                    task_name=f"conv-{conv_id}",
                    objective=user_input.strip() or "complex task",
                )
                plan_hint = (
                    "\n\n[Plan Mode] Read memory/sop/plan_sop.md and follow the "
                    "five-phase flow (explore → plan → user-confirm via ask_user → "
                    f"execute → verify with a subagent). The plan file is "
                    f"{plan_session.plan_path}; "
                    "file_read it to resume, fill in steps, and mark them "
                    "[ ]/[D]/[P]/[✓]/[✗]/[FIX] as you progress."
                )
                effective_system_prompt = (effective_system_prompt or "") + plan_hint
                if initial_messages and initial_messages[0].get("role") == "system":
                    initial_messages[0]["content"] = effective_system_prompt
                else:
                    initial_messages.insert(0, {"role": "system", "content": effective_system_prompt})
            elif mode_eff == "goal":
                goal = goal_config
                if goal is None or not (goal.objective or "").strip():
                    raise HTTPException(400, "goal mode requires goal_config.objective")
                from agent.guardian import Guardian

                guardian = Guardian(agent_id=int(agent_id or 0))
                await guardian.start_goal_mode(
                    objective=goal.objective,
                    budget_seconds=goal.budget_seconds,
                    max_turns=goal.max_turns or max_turns or config.max_turns or 50,
                )
                goal_hint = (
                    f"\n\n[Goal Mode] Objective: {goal.objective}. Budget: "
                    f"{goal.budget_seconds}s. Read memory/sop/goal_mode_sop.md. "
                    f"Alternate creation/inspection/improvement phases; execute "
                    f"meaningful work each turn, do not merely report progress; "
                    f"if blocked ask the user via ask_user."
                )
                effective_system_prompt = (effective_system_prompt or "") + goal_hint
                if initial_messages and initial_messages[0].get("role") == "system":
                    initial_messages[0]["content"] = effective_system_prompt
                else:
                    initial_messages.insert(0, {"role": "system", "content": effective_system_prompt})
                goal_state_file = "temp/goal_state.json"

            handler = _BH(tools_registry=tools, max_turns=max_turns or config.max_turns)

            final_content = ""
            async for item in agent_runner_loop(
                llm,
                system_prompt=effective_system_prompt,
                user_input=user_input,
                handler=handler,
                tools_schema=tools.get_schema(),
                max_turns=max_turns or config.max_turns,
                on_event=on_event,
                on_tool_batch=_prepare_tool_batch,
                initial_messages=initial_messages if initial_messages else None,
            ):
                final_content = item.get("data", final_content)

            # Goal 模式：由 Guardian 驱动续跑，直到它给出一次收尾提示。
            if mode_eff == "goal":
                wrapped_up = False
                while True:
                    cont = await guardian.next_goal_prompt()
                    if cont is None:
                        break
                    wrapped_up = "[GOAL MODE WRAP-UP]" in cont
                    async for item in agent_runner_loop(
                        llm,
                        system_prompt=effective_system_prompt,
                        user_input=cont,
                        handler=handler,
                        tools_schema=tools.get_schema(),
                        max_turns=max_turns or config.max_turns,
                        on_event=on_event,
                        on_tool_batch=_prepare_tool_batch,
                        initial_messages=None,
                    ):
                        final_content = item.get("data", final_content)
                    if wrapped_up:
                        break
                try:
                    await guardian.mark_goal_done(budget_exhausted=wrapped_up)
                    from agent.file_memory import agent_root

                    state = await guardian.get_goal_status()
                    if state and goal_state_file:
                        gpath = agent_root(int(agent_id)) / goal_state_file
                        gpath.parent.mkdir(parents=True, exist_ok=True)
                        gpath.write_text(json.dumps(state, ensure_ascii=False, default=str), encoding="utf-8")
                except Exception:  # noqa: BLE001 — 收尾镜像失败不影响对话结果
                    pass

            await conversation_store.update_message(
                assistant_msg_id,
                content=final_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                status="done",
                model=effective_model,
                usage=collected_usage or None,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "end"})
        except asyncio.CancelledError:
            await conversation_store.update_message(
                assistant_msg_id, status="error", error="已中止", reasoning=collected_reasoning,
            )
            await queue.put({"type": "end"})
        except ApprovalDenied:
            # 用户主动拒绝不是故障：正常收尾，不发 error，也绝不把 denied 工具结果
            # 喂回模型继续跑。审批卡已由面板自己翻成「已拒绝」。on_tool_batch 在
            # tool_call 事件之前抛，所以没有悬空的 running 工具卡。
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                status="done",
                model=effective_model,
                usage=collected_usage or None,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "end"})
        except ApprovalTimeout as timeout_exc:
            # 审批超时：本轮就此结束，不把「被拒绝」喂回模型跑下一轮。已流出的正文/
            # 工具调用由 checkpoint 落过库，刷新后能看到停在哪一步。
            error_text = str(timeout_exc)
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                status="error",
                error=error_text,
                reasoning=collected_reasoning,
            )
            await queue.put({"type": "error", "message": error_text})
            await queue.put({"type": "end"})
        except Exception as e:
            error_text = "".join(traceback.format_exception_only(type(e), e)).strip()
            await conversation_store.update_message(
                assistant_msg_id, status="error", error=error_text, reasoning=collected_reasoning,
            )
            await queue.put({"type": "error", "message": error_text})
            await queue.put({"type": "end"})
        finally:
            # A chat turn owns CDP tabs through its per-agent MCP manager. Release
            # deterministically on normal completion, abort, or task failure; TTL
            # remains the fallback for a hard process crash.
            try:
                manager = getattr(ga, "_mcp_manager", None)
                if manager is not None:
                    await manager.close()
            except Exception:
                pass

    task = asyncio.create_task(run_agent())
    _CONV_TASKS[conv_id] = task

    async def event_generator():
        try:
            while True:
                event = await queue.get()
                if event.get("type") == "end":
                    break
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        finally:
            _CONV_TASKS.pop(conv_id, None)

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=_stream_headers())


@router.post("/messages")
async def client_send_message(
    request: ClientSendMessageRequest,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    x_cdp_client_alias: str | None = Header(None, alias="X-Cdp-Client-Alias"),
    x_cdp_session_key: str | None = Header(None, alias="X-Cdp-Session-Key"),
    authorization: str | None = Header(None),
):
    """Create (or continue) a conversation and stream agent events as SSE.

    Each event is `data: <json>\\n\\n`. The first event may be
    `{"type":"conversation","conversation_id":...}` so the extension can
    keep the conversation for subsequent turns / abort.
    """
    _require_ch()
    from db import PostgresClient

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    client_alias = (x_cdp_client_alias or "").strip()
    session_key = (x_cdp_session_key or "").strip()
    if not client_id or not session_key or not session_key.startswith(f"{client_id}:"):
        raise HTTPException(status_code=403, detail="invalid cdp client/session identity")
    agent_ids = await _require_chat_client(client_id)
    agent = await _require_client_agent(agent_ids, int(request.agent_id))

    # 场景由服务端按入参归一（client_id + tab_id 是稳定身份键），提示词只在建会话时
    # 写一次；当前页面 URL/标题是易变内容，不进提示词，模型需要时用 web scan/tabs 现取。
    scene = scene_context.normalize(cdp={
        "client_id": client_id,
        "session_key": session_key,
        "client_alias": client_alias,
    })

    # 本轮生效的对话级覆盖：面板显式给了就落库，没给就沿用会话已存值。
    turn_model = (request.model or "").strip()
    turn_effort: str | None = (
        _normalize_reasoning_effort(request.reasoning_effort)
        if request.reasoning_effort is not None
        else None
    )

    conv_id = (request.conversation_id or "").strip()
    if conv_id:
        conv = await conversation_store.get_conversation(conv_id)
        if not conv:
            raise HTTPException(status_code=404, detail="conversation not found")
        if conv.get("agent_id") != request.agent_id:
            raise HTTPException(status_code=403, detail="conversation agent mismatch")
        ownership = _require_conv_owned_by_client(conv, client_id)
        # system_prompt 不逐轮重写：场景身份键建会话时已写定。这里只落本轮显式选择
        # 的模型/思考等级，以及换标签页续聊时的场景重绑。
        update_fields: dict[str, Any] = {}
        if turn_model and turn_model != (conv.get("model") or ""):
            update_fields["model"] = turn_model
        if turn_effort is not None and ownership.get("reasoning_effort") != turn_effort:
            settings = dict(ownership)
            settings["reasoning_effort"] = turn_effort
            update_fields["chat_settings"] = settings
            ownership = settings
        ownership = _rebind_conv_scene(
            conv, ownership,
            client_id=client_id,
            session_key=session_key,
            client_alias=client_alias,
            update_fields=update_fields,
        )
        if update_fields:
            await conversation_store.update_conversation(conv_id, **update_fields)
        if "model" in update_fields:
            conv["model"] = turn_model
    else:
        chat_settings: dict[str, Any] = scene_context.persist(scene)
        if turn_effort is not None:
            chat_settings["reasoning_effort"] = turn_effort
        conv = await conversation_store.create_conversation(
            title=f"CDP:{request.title or request.url or '网页对话'}"[:60],
            system_prompt=scene_context.append_prompt(agent.get("system_prompt"), scene),
            model=turn_model or agent.get("model") or "",
            agent_id=request.agent_id,
            chat_settings=chat_settings,
            user_id=str(agent.get("user_id")) if agent.get("user_id") else None,
        )
        conv_id = conv["id"]
        ownership = chat_settings

    # 本轮 fork 用的思考等级：显式给了用它；否则沿用会话已存值（存过才生效）。
    conversation_effort = ownership.get("reasoning_effort")
    effective_effort = (
        turn_effort
        if turn_effort is not None
        else (_normalize_reasoning_effort(conversation_effort) if conversation_effort is not None else None)
    )
    conv_model = (conv.get("model") or "").strip()

    existing = _CONV_TASKS.get(conv_id)
    if existing and not existing.done():
        raise HTTPException(status_code=409, detail="a task is already running for this conversation")

    # 用户消息只存用户真正说的话：页面 URL/标题不再前置进正文（那会把易变快照
    # 写进历史，且下一轮就过期）。模型要当前页面时用 web operation=scan/tabs 现取。
    user_msg = await conversation_store.add_message(conv_id, "user", request.content)
    assistant_msg = await conversation_store.add_message(conv_id, "assistant", "", status="streaming")
    assistant_msg_id = assistant_msg["id"]
    await conversation_store.touch_conversation(conv_id)

    return await _run_cdp_conversation_turn(
        conv=conv,
        conv_id=conv_id,
        assistant_msg_id=assistant_msg_id,
        agent=agent,
        scene=scene,
        agent_id=int(request.agent_id),
        user_input=request.content,
        mode=request.mode,
        goal_config=request.goal_config,
        max_turns=request.max_turns,
        effective_effort=effective_effort,
        conv_model=conv_model,
        client_id=client_id,
    )


@router.post("/conversations/{conv_id}/messages/{message_id}/retry")
async def client_retry_message(
    conv_id: str,
    message_id: int,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    x_cdp_client_alias: str | None = Header(None, alias="X-Cdp-Client-Alias"),
    x_cdp_session_key: str | None = Header(None, alias="X-Cdp-Session-Key"),
    authorization: str | None = Header(None),
):
    """重试一条失败的 assistant 消息：复位同 id 消息为 streaming，用其前一条
    user 消息内容重跑一轮 agent。与 agent/api.py retry_message 同口径：只允许
    最后一条 assistant 消息，mode 固定 interact。
    """
    _require_ch()

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    client_alias = (x_cdp_client_alias or "").strip()
    session_key = (x_cdp_session_key or "").strip()
    if not client_id or not session_key or not session_key.startswith(f"{client_id}:"):
        raise HTTPException(status_code=403, detail="invalid cdp client/session identity")
    agent_ids = await _require_chat_client(client_id)

    conv = await conversation_store.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    agent_id = conv.get("agent_id")
    agent = await _require_client_agent(agent_ids, int(agent_id or 0))
    ownership = _require_conv_owned_by_client(conv, client_id)

    existing = _CONV_TASKS.get(conv_id)
    if existing and not existing.done():
        raise HTTPException(status_code=409, detail="a task is already running for this conversation")

    # 定位目标消息：assistant、error/streaming、且是最后一条 assistant
    history = await conversation_store.get_messages(conv_id)
    target = None
    last_assistant_id = None
    for msg in history:
        if msg.get("role") == "assistant":
            last_assistant_id = msg["id"]
            if msg["id"] == int(message_id):
                target = msg
    if target is None:
        raise HTTPException(status_code=404, detail="message not found in this conversation")
    if target.get("status") not in ("error", "streaming"):
        raise HTTPException(status_code=400, detail="只能重试失败或中断的消息")
    if last_assistant_id != int(message_id):
        raise HTTPException(status_code=400, detail="只能重试最后一条 assistant 消息")

    user_input = ""
    for msg in history:
        if msg["id"] == int(message_id):
            break
        if msg.get("role") == "user":
            user_input = msg.get("content") or ""
    if not user_input.strip():
        raise HTTPException(status_code=400, detail="无法重试：上一条用户消息为空")

    # 复位同一条 assistant 消息（保留 id，面板原地更新）
    await conversation_store.update_message(
        int(message_id),
        content="",
        tool_calls=None,
        tool_results=None,
        status="streaming",
        error=None,
        model=None,
        usage=None,
        reasoning="",
    )
    await conversation_store.touch_conversation(conv_id)

    # 重试沿用会话已存的模型/思考等级；mode=interact，无 goal。
    conversation_effort = ownership.get("reasoning_effort")
    effective_effort = (
        _normalize_reasoning_effort(conversation_effort) if conversation_effort is not None else None
    )
    conv_model = (conv.get("model") or "").strip()

    # 换标签页后重试同一条消息：场景段要重绑到本轮所在标签页，否则模型照着旧
    # tab_id 去操作一个已经关掉的标签页。与 client_send_message 续会话分支同口径。
    rebind_fields: dict[str, Any] = {}
    _rebind_conv_scene(
        conv, ownership,
        client_id=client_id,
        session_key=session_key,
        client_alias=client_alias,
        update_fields=rebind_fields,
    )
    if rebind_fields:
        await conversation_store.update_conversation(conv_id, **rebind_fields)

    scene = scene_context.normalize(cdp={
        "client_id": client_id,
        "session_key": session_key,
        "client_alias": client_alias,
    })

    return await _run_cdp_conversation_turn(
        conv=conv,
        conv_id=conv_id,
        assistant_msg_id=int(message_id),
        agent=agent,
        scene=scene,
        agent_id=int(agent_id or 0),
        user_input=user_input,
        mode="interact",
        goal_config=None,
        max_turns=None,
        effective_effort=effective_effort,
        conv_model=conv_model,
        client_id=client_id,
    )


@router.post("/conversations/{conv_id}/abort")
async def client_abort_conversation(
    conv_id: str,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    authorization: str | None = Header(None),
) -> dict:
    _require_ch()

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    await _require_chat_client(client_id)
    conv = await conversation_store.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    _require_conv_owned_by_client(conv, client_id)
    task = _CONV_TASKS.get(conv_id)
    if task and not task.done():
        task.cancel()
        return {"aborted": True}
    return {"aborted": False, "detail": "no running task"}


@router.post("/conversations/{conv_id}/approvals/{confirmation_id}")
async def client_resolve_approval(
    conv_id: str,
    confirmation_id: str,
    body: ClientApprovalRequest,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    authorization: str | None = Header(None),
) -> dict:
    """批准或拒绝 code_run 的一次性审批。

    与 agent/api.py 的 resolve_approval 同口径，但鉴权走 internal token + CDP
    客户端身份（网页对话没有 C 端 session/cookie）。会话归属由
    _require_conv_owned_by_client 把住；code_run 的 action.node_id=="" 跳过节点门。
    """
    _require_ch()
    from user_platform.node_client.approvals import approval_registry

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    await _require_chat_client(client_id)
    conv = await conversation_store.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    _require_conv_owned_by_client(conv, client_id)

    action = approval_registry.get(confirmation_id)
    if action is None or action.conversation_id != conv_id:
        raise HTTPException(status_code=404, detail="confirmation not found")
    if action.resolved is not None:
        raise HTTPException(status_code=409, detail=f"confirmation already {action.resolved}")
    # code_run 在服务端本地执行、node_id=""；CDP 路径不绑节点，归属已由 client_id 把住。
    # 若 action 带了 node_id（理论不会），fail-closed 拒绝——CDP 客户端不该能放行节点命令。
    if action.node_id:
        raise HTTPException(status_code=403, detail="无权操作该节点")
    if not body.command_hash or body.command_hash != action.command_hash:
        raise HTTPException(status_code=409, detail="command hash mismatch")

    result = (body.result or "").strip().lower()
    if result not in {"allow", "deny"}:
        raise HTTPException(status_code=400, detail="result must be allow or deny")

    resolved = approval_registry.resolve(confirmation_id, result)
    if resolved is None:
        raise HTTPException(status_code=409, detail="confirmation already resolved")
    return {"ok": True, "confirmation_id": confirmation_id, "result": result}


def _conv_ownership(conv: dict) -> dict:
    """从对话行取出 chat_settings（兼容字符串 JSON），归一成 dict。"""
    ownership = conv.get("chat_settings") or {}
    if isinstance(ownership, str):
        try:
            ownership = json.loads(ownership)
        except (TypeError, ValueError, json.JSONDecodeError):
            ownership = {}
    return ownership if isinstance(ownership, dict) else {}


def _require_conv_owned_by_client(conv: dict, client_id: str) -> dict:
    """校验会话属于本 CDP 客户端，返回归一后的 chat_settings。

    归属只认 client_id：session_key 里的 tabId 随标签页关闭失效，用它做归属会让
    换标签页后既看不到也续不了自己的历史。客户端之间仍严格隔离。
    """
    ownership = _conv_ownership(conv)
    if ownership.get("cdp_client_id") != client_id:
        raise HTTPException(status_code=403, detail="conversation does not belong to this client")
    return ownership


def _rebind_conv_scene(
    conv: dict,
    ownership: dict,
    *,
    client_id: str,
    session_key: str,
    client_alias: str,
    update_fields: dict[str, Any],
) -> dict:
    """换标签页续聊时把会话的场景段重绑到本轮所在标签页。

    场景段（含 tab_id）建会话时一次性写进 system_prompt。跨标签页续聊时旧 tab_id
    指向一个已经关掉的标签页，模型会照着它去操作——所以这里用当前 session_key 重解
    场景，替换掉已写入的旧段，并把 chat_settings 的 cdp_session_key/alias 更新。

    改动合并进调用方的 ``update_fields``（与模型/思考等级的更新共用一次写库）。
    返回可能已被替换的 ownership dict。
    """
    old_scene = scene_context.from_chat_settings(ownership)
    new_scene = scene_context.normalize(cdp={
        "client_id": client_id,
        "session_key": session_key,
        "client_alias": client_alias,
    })
    if new_scene is None or new_scene == old_scene:
        return ownership

    system_prompt = scene_context.replace_prompt(
        conv.get("system_prompt"), old_scene, new_scene,
    )
    update_fields["system_prompt"] = system_prompt
    conv["system_prompt"] = system_prompt

    # persist() 只给场景键；其余 chat_settings（reasoning_effort 等）原样保留。
    settings = dict(update_fields.get("chat_settings") or ownership)
    settings.update(scene_context.persist(new_scene))
    update_fields["chat_settings"] = settings
    return settings


def _iso_value(value: Any) -> str | None:
    """ClickHouse DateTime64 → ISO 字符串（面板 created_at 用）。"""
    if value is None:
        return None
    try:
        return value.isoformat()
    except AttributeError:
        return str(value)


def _conv_summary(conv: dict) -> dict:
    """列表用的对话摘要：只透出面板需要的字段，不带内部归属明细。"""
    def _iso(value: Any) -> str | None:
        if value is None:
            return None
        try:
            return value.isoformat()
        except AttributeError:
            return str(value)

    return {
        "id": conv.get("id"),
        "title": conv.get("title") or "",
        "agent_id": conv.get("agent_id"),
        "updated_at": _iso(conv.get("updated_at")),
        "created_at": _iso(conv.get("created_at")),
    }


@router.get("/conversations")
async def client_list_conversations(
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    authorization: str | None = Header(None),
    limit: int = 50,
) -> dict:
    """列出本 CDP 客户端名下的会话，最近更新在前。

    归属按 client_id，不按 session_key：Chrome 的 tabId 随标签页关闭失效，按
    session_key 过滤会让关掉标签页的历史会话彻底查不到（数据仍在，只是被滤掉）。
    历史属于这个客户端（这个浏览器），不属于某个已经不存在的标签页。
    """
    _require_ch()

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    await _require_chat_client(client_id)
    rows = await conversation_store.list_conversations_for_cdp_client(
        client_id, limit=max(1, min(limit, 200))
    )
    return {"conversations": [_conv_summary(r) for r in rows]}


@router.post("/conversations")
async def client_create_conversation(
    request: ClientCreateConversationRequest,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    x_cdp_client_alias: str | None = Header(None, alias="X-Cdp-Client-Alias"),
    x_cdp_session_key: str | None = Header(None, alias="X-Cdp-Session-Key"),
    authorization: str | None = Header(None),
) -> dict:
    """显式新建一个空会话，绑定当前页面（client_id+session_key）。"""
    _require_ch()

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    client_alias = (x_cdp_client_alias or "").strip()
    session_key = (x_cdp_session_key or "").strip()
    if not client_id or not session_key or not session_key.startswith(f"{client_id}:"):
        raise HTTPException(status_code=403, detail="invalid cdp client/session identity")
    agent_ids = await _require_chat_client(client_id)
    agent = await _require_client_agent(agent_ids, int(request.agent_id))

    # 场景段必须在这里写入：send_message 不再每轮重写 system_prompt，若这里留空，
    # 经本端点建出来的会话会永远没有场景（模型不知道自己在驱动哪个浏览器/标签页）。
    scene = scene_context.normalize(cdp={
        "client_id": client_id,
        "session_key": session_key,
        "client_alias": client_alias,
    })
    conv = await conversation_store.create_conversation(
        title=f"CDP:{request.title or '新对话'}"[:60],
        system_prompt=scene_context.append_prompt(agent.get("system_prompt"), scene),
        model=agent.get("model") or "",
        agent_id=request.agent_id,
        chat_settings=scene_context.persist(scene),
        user_id=str(agent.get("user_id")) if agent.get("user_id") else None,
    )
    return {"conversation": _conv_summary(conv)}


@router.get("/conversations/{conv_id}")
async def client_get_conversation(
    conv_id: str,
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_cdp_client_id: str | None = Header(None, alias="X-Cdp-Client-Id"),
    authorization: str | None = Header(None),
) -> dict:
    """取一个会话的消息历史，供面板切换会话时回放。归属只校验 client_id。

    不校验 session_key：换标签页后要能回看这个浏览器的历史（tabId 随标签页关闭失效）。
    """
    _require_ch()

    _require_internal(authorization, x_internal_token)
    client_id = (x_cdp_client_id or "").strip()
    await _require_chat_client(client_id)
    conv = await conversation_store.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="conversation not found")
    _require_conv_owned_by_client(conv, client_id)
    messages = await conversation_store.get_messages(conv_id)
    return {
        "conversation": _conv_summary(conv),
        "messages": [
            {
                # id 必须回：面板重试要按 id 定位消息，回放要按 id 去重/续看。
                "id": m.get("id"),
                "role": m.get("role"),
                "content": m.get("content") or "",
                "tool_calls": m.get("tool_calls") or [],
                "tool_results": m.get("tool_results") or [],
                "status": m.get("status") or "done",
                "error": m.get("error") or "",
                "created_at": _iso_value(m.get("created_at")),
            }
            for m in messages
        ],
    }
