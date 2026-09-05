"""Minimal MCP-over-SSE gateway for loaded custom plugins.

This implements the common MCP SSE shape:
- GET  /mcp/{service}/sse       opens an event stream and publishes the message endpoint
- POST /mcp/{service}/messages  accepts JSON-RPC requests and sends responses to that stream

Supported methods for first cut:
- initialize
- tools/list
- tools/call
- ping

For custom plugins this is enough for external MCP clients to discover and call tools.

鉴权：当服务的 PluginContext.auth_enabled 为真时，SSE 握手与每个 RPC 都校验
token（Authorization: Bearer 或 ?token=）。token 必须在该服务的 allowed_tokens
集合内，否则 401/403。校验通过的 token 被存进 session，并经 current_request_token
ContextVar 透传给插件 handler（如 cdp-bridge 的会话池隔离）。
"""
from __future__ import annotations

import asyncio
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from loguru import logger

from mcp_builtin.device_control import protocol as proto
from mcp_builtin.device_control.driver import DeviceContext
from .registry import registry
from .plugin_loader import current_request_token

router = APIRouter(prefix="/mcp", tags=["mcp-runtime-sse"])

# 扩展连接空闲读超时（秒）：客户端每 ~24s 发一次 keepalive ping，
# 超过这个窗口没收到任何帧即判定为半开死连接（MV3 worker 被杀、断网、休眠
# 都不会发 TCP FIN，receive_text 会永久阻塞），主动关闭以触发 finally 里的
# unregister_client 释放 client_id 槽位，让同一浏览器的正常重连能成功。
_EXT_IDLE_TIMEOUT = 60
# MCP Runtime 已并入主服务，CDP 网页对话的内部 HTTP 路由也挂在同一
# FastAPI app 上。固定回环主服务端口，不再支持独立 Agent 服务地址。
_AGENT_INTERNAL_BASE = "http://127.0.0.1:8001"


@dataclass
class _Session:
    queue: asyncio.Queue[str]
    service_name: str
    token: str = ""


_sessions: dict[str, _Session] = {}


def _sse(event: str, data: Any) -> str:
    payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"


def _jsonrpc_result(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _extract_token(request: Request) -> str:
    """Authorization: Bearer 优先，回退 query ?token=（EventSource 无法设 header）。"""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return request.query_params.get("token", "").strip()


# 内置工具服务名 → 该服务鉴权所需的 principal param key。
# principal 带这个 param 就代表能操作该服务的一个具体资源（CDP 客户端 / 邮箱账户 / 设备）。
_BUILTIN_SERVICE_PARAM_KEY = {
    "cdp-bridge": "cdp_client_id",
    "mail": "mail_account_id",
    "device-control": "device_id",
}

# 内置工具服务名 → 该服务可操作的一级资源类型。
_BUILTIN_SERVICE_RESOURCE_TYPE = {
    "cdp-bridge": {"cdp_client"},
    "mail": {"mail_account"},
    "issue-workflow": set(),
    "device-control": {"device"},
}


# （旧名称不再暴露；保留 issue-workflow 的空集合仅为文档兼容。）

def _principal_has_service_param(params: list[dict], service_name: str) -> bool:
    """principal 是否带该服务所需的操作 param（cdp-bridge→cdp_client_id 等）。

    内置工具服务按 param 判权；普通自定义 MCP 服务不在此判（走 mcp_service_users），
    这里对它们返回 False，由 _check_service_auth 的 service/identity 分支处理。
    """
    required = _BUILTIN_SERVICE_PARAM_KEY.get(service_name)
    if not required:
        return False
    return any(str(p.get("param_key") or "") == required for p in (params or []))


async def _check_service_auth(service_name: str, token: str) -> None:
    """服务开启鉴权时校验 token；不通过抛 HTTPException(401/403)。

    统一走 builtin_tool_store.resolve_token：token → 解析授权目标。
    - 外部 MCP：token 绑 service_id，解析出的服务名必须与本次 service_name 一致。
    - 内置工具（cdp-bridge/mail/device-control）：token 绑 resource_id，资源类型必须
      与本次服务对应（cdp_client/mail_account/device）。会话/设备归属再由各自
      连接层按 resource id / device_id 隔离。
    """
    plugin = registry.get(service_name)
    if not plugin:
        raise HTTPException(404, f"MCP service not loaded: {service_name}")
    ctx = plugin.ctx
    if not ctx.enabled:
        raise HTTPException(503, "该 MCP 服务已停用")
    if service_name == "marketplace-status":
        # 市场工具永远要求本次 Agent 对话签发的用户身份 token；不能被服务配置里的
        # auth_enabled=false 放宽成匿名访问。
        if not token:
            raise HTTPException(401, "市场管理 MCP 需要管理员身份 token")
        import builtin_tool_store
        resolved = await builtin_tool_store.resolve_token(token)
        if not resolved or resolved.get("kind") != "identity":
            raise HTTPException(403, "token 未被授权访问市场管理 MCP")
        if (resolved.get("token") or {}).get("target_type") == "user":
            return
        raise HTTPException(403, "市场管理 MCP 需要用户身份 token")
    if not ctx.auth_enabled:
        return  # 未开启鉴权，放行（保持向后兼容）
    if not token:
        raise HTTPException(401, "该 MCP 服务已开启访问控制，请提供 token")

    import builtin_tool_store
    resolved = await builtin_tool_store.resolve_token(token)
    if not resolved:
        raise HTTPException(403, "token 未被授权访问该 MCP 服务")
    kind = resolved["kind"]
    if kind == "principal":
        import mcp_plugin_store

        principal_id = (resolved.get("target") or {}).get("id")
        if _principal_has_service_param(await mcp_plugin_store.list_principal_params(int(principal_id)), service_name):
            return
    elif kind == "service":
        if resolved["target"].get("name") == service_name:
            return
    elif kind == "resource":
        want_type = _BUILTIN_SERVICE_RESOURCE_TYPE.get(service_name)
        if want_type and resolved["target"].get("resource_type") in want_type:
            return
    elif kind == "identity":
        # Agent identity token is not itself a resource grant. Resolve the Agent's
        # explicitly bound MCP principal, then authorize through that principal.
        token_meta = resolved.get("token") or {}
        target_type = token_meta.get("target_type")
        target_id = token_meta.get("target_id")
        if target_type == "agent" and target_id and str(target_id).isdigit():
            import mcp_plugin_store

            principal_id = await mcp_plugin_store.get_agent_mcp_principal_id(int(target_id))
            if principal_id is not None:
                if _principal_has_service_param(await mcp_plugin_store.list_principal_params(principal_id), service_name):
                    return
        # Session-scoped internal tools (e.g. issue-workflow) use an identity
        # token. The plugin itself narrows target_type/target_id to one task and
        # derives the issue from that task, so the gateway only verifies that
        # this is an authenticated identity and leaves object-level authorization
        # to the adapter.
        if service_name in {"issue-workflow", "marketplace-status"}:
            target_type = (resolved.get("token") or {}).get("target_type")
            if service_name == "issue-workflow" and target_type in ("agent", "node"):
                return
            if service_name == "marketplace-status" and target_type == "user":
                return
    raise HTTPException(403, "token 未被授权访问该 MCP 服务")


def _tools_for_service(service_name: str) -> list[dict]:
    plugin = registry.get(service_name)
    if not plugin:
        raise KeyError(service_name)
    return [
        {
            "name": t.name,
            "description": t.description or f"MCP tool {t.name}",
            "inputSchema": t.params or {"type": "object", "properties": {}},
        }
        for t in plugin.tools
    ]


async def _handle_rpc(service_name: str, payload: dict, token: str) -> dict | None:
    req_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}

    # 所有方法都受 auth_enabled 约束（已开鉴权则必须带合法 token）。
    await _check_service_auth(service_name, token)

    if method == "initialize":
        return _jsonrpc_result(req_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": f"ai-lubricant-mcp-runtime/{service_name}", "version": "0.1.0"},
        })
    if method == "notifications/initialized":
        return None
    if method == "ping":
        return _jsonrpc_result(req_id, {})
    if method == "tools/list":
        try:
            return _jsonrpc_result(req_id, {"tools": _tools_for_service(service_name)})
        except KeyError:
            return _jsonrpc_error(req_id, -32004, f"service not loaded: {service_name}")
    if method == "tools/call":
        tool_name = params.get("name")
        args = params.get("arguments") or {}
        # 把本次请求 token 注入 ContextVar，插件可读取用于隔离（cdp-bridge 会桥接）。
        tok = current_request_token.set(token)
        try:
            result = await registry.call_tool(service_name, tool_name, args)
        except Exception as e:
            return _jsonrpc_error(req_id, -32000, str(e))
        finally:
            current_request_token.reset(tok)
        if isinstance(result, dict) and "content" in result:
            content = result["content"]
        else:
            content = [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}]
        return _jsonrpc_result(req_id, {"content": content})

    return _jsonrpc_error(req_id, -32601, f"method not found: {method}")


# ── 形态 C：节点托管 stdio MCP 的代理端点 ──────────────────────────────────────
#
# 路径比 /mcp/{service_name}/sse 多一段，不与之冲突。这里**不经 registry**：托管进程
# 在节点上，本进程没有它的插件对象，只做纯 I/O 转发（服务端连不到节点内网端口，必须
# 由节点代发）。对 agent/编辑器而言这就是一个普通 remote SSE 端点。


@router.get("/node-hosted/{service_id}/sse")
async def node_hosted_sse(service_id: int, request: Request):
    """把节点上托管进程的 SSE 流原样代理出来。"""
    return await _proxy_node_hosted(service_id, request, method="GET", path="/sse")


@router.post("/node-hosted/{service_id}/messages")
async def node_hosted_messages(service_id: int, request: Request):
    """转发 JSON-RPC 消息到节点上的托管进程。"""
    query = request.url.query
    path = "/messages" + (f"?{query}" if query else "")
    return await _proxy_node_hosted(
        service_id, request, method="POST", path=path, body=await request.body()
    )


async def _proxy_node_hosted(
    service_id: int, request: Request, *, method: str, path: str, body: bytes = b""
) -> StreamingResponse:
    import mcp_plugin_store
    from mcp_runtime.node_hosted import NodeHostedError, proxy_request

    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, f"MCP service {service_id} not found")
    if (service.get("deploy_scope") or "") != "node_hosted":
        raise HTTPException(400, f"service {service_id} is not node-hosted")
    # 鉴权沿用服务级开关：托管 MCP 与其他服务同一套访问控制，不因为多了一跳就放宽。
    token = _extract_token(request)
    await _check_service_auth(service.get("name") or "", token)

    try:
        resp = await proxy_request(service, method=method, path=path, body=body)
    except NodeHostedError as e:
        raise HTTPException(502, str(e))

    async def relay():
        try:
            async for chunk in resp.iter_chunks():
                yield chunk
        finally:
            await resp.close()

    # 保留上游 content-type（SSE 必须是 text/event-stream，否则客户端不会按流处理）
    media = resp.headers.get("Content-Type") or resp.headers.get("content-type") or "text/event-stream"
    return StreamingResponse(relay(), status_code=resp.status or 200, media_type=media)


@router.get("/{service_name}/sse")
async def open_sse(service_name: str, request: Request):
    if not registry.get(service_name):
        raise HTTPException(404, f"MCP service not loaded: {service_name}")
    token = _extract_token(request)
    await _check_service_auth(service_name, token)
    session_id = uuid.uuid4().hex
    queue: asyncio.Queue[str] = asyncio.Queue()
    _sessions[session_id] = _Session(queue=queue, service_name=service_name, token=token)
    sse_url = f"/mcp/{service_name}/sse"
    if token:
        sse_url = f"{sse_url}?token={token}"

    async def gen():
        try:
            endpoint = f"/mcp/{service_name}/messages?session_id={session_id}"
            yield _sse("endpoint", endpoint)
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=20)
                    yield item
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _sessions.pop(session_id, None)

    return StreamingResponse(gen(), media_type="text/event-stream")


@router.post("/{service_name}/messages")
async def post_message(service_name: str, request: Request, session_id: str):
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(404, "SSE session not found")
    payload = await request.json()
    response = await _handle_rpc(session.service_name, payload, session.token)
    if response is not None:
        await session.queue.put(_sse("message", response))
    return {"ok": True}


class _ExtWsAdapter:
    """把 FastAPI WebSocket 适配成 driver 期望的 send_message(str) 客户端。

    driver 的命令下发在工作线程里同步调用 send_message，这里用
    run_coroutine_threadsafe 回投到事件循环执行 ws.send_text。
    """

    __slots__ = ("_ws", "_loop", "_token", "_user_id", "_client_id", "_client_alias")

    def __init__(self, ws: WebSocket, loop: asyncio.AbstractEventLoop) -> None:
        self._ws = ws
        self._loop = loop
        self._token = ""
        self._user_id = None
        self._client_id = None
        self._client_alias = ""

    def send_message(self, payload: str) -> None:
        try:
            fut = asyncio.run_coroutine_threadsafe(self._ws.send_text(payload), self._loop)
            fut.result(timeout=10)
        except Exception:
            # 发送失败（连接已断等）静默：driver 的超时轮询会兜底。
            pass
    def close_auth(self, code: int, reason: str) -> None:
        # 可能从两种上下文被调用：
        # - driver 工作线程（execute_js 等）：跨线程，用 run_coroutine_threadsafe 并等待。
        # - WebSocket 事件循环本身（config reload 撤销）：此时不能在本循环上 .result()
        #   阻塞，否则关闭协程永远排不上队 → 卡死 10s。直接 fire-and-forget 调度关闭即可。
        coro = self._ws.close(code=code, reason=reason)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            self._loop.create_task(coro)
            return
        try:
            fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
            fut.result(timeout=10)
        except Exception:
            pass


def _is_chat_frame(raw: str) -> bool:
    """聊天帧与 cdp driver 帧复用同一条扩展 WebSocket，按 type 前缀分流。"""
    try:
        return str(json.loads(raw).get("type") or "").startswith("chat_")
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


def _is_ping_frame(raw: str) -> bool:
    """扩展 keepalive 探活帧（background 每 ~24s 发一次）。必须回 pong：
    半开连接上 ws.send 永远本地成功，扩展只能靠「发了 ping 却收不到任何回帧」判死。"""
    try:
        return json.loads(raw).get("type") == "ping"
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


async def _route_ext_frame(raw: str, websocket: WebSocket, adapter: "_ExtWsAdapter", driver) -> None:
    """已认证扩展连接的逐帧分发：探活 / 聊天 / cdp driver 帧。"""
    if _is_ping_frame(raw):
        await _send_ws_protocol(websocket, "pong")
        return
    if _is_chat_frame(raw):
        await _handle_chat_frame(raw, websocket, adapter, driver)
        return
    await asyncio.to_thread(driver.ingest_message, raw, adapter)


async def _send_chat_frame(websocket: WebSocket, payload: dict) -> None:
    await websocket.send_text(json.dumps(payload, ensure_ascii=False))


def _chat_identity(adapter: _ExtWsAdapter, frame: dict) -> dict:
    """只从已认证连接构造聊天身份；页面帧中的身份/token 字段一律忽略。

    会话池按 client_id 隔离，网页对话由 CDP 客户端驱动，身份以 client_id 为主。
    """
    tab_id = frame.get("tabId")
    session_key = f"{adapter._client_id}:{tab_id}" if tab_id is not None else ""
    return {
        "user_id": adapter._user_id,
        "client_id": str(adapter._client_id or ""),
        "client_alias": adapter._client_alias or str(adapter._client_id or ""),
        "session_key": session_key,
    }


def _owned_chat_page(driver, adapter: _ExtWsAdapter, session_key: str):
    """返回该认证连接实际拥有的活动页面，拒绝仅靠帧声明的页面。"""
    if not session_key:
        return None
    try:
        # 会话池按 client_id 隔离，context key = client_id。
        page = driver.get_context(str(adapter._client_id or "")).sessions.get(session_key)
    except (TypeError, ValueError):
        return None
    if not page or not page.is_active() or page.ws_client is not adapter:
        return None
    return page


async def _handle_chat_frame(raw: str, websocket: WebSocket, adapter: _ExtWsAdapter, driver=None) -> None:
    """处理网页侧 Agent 对话帧，身份仅来自已认证 CDP WebSocket。"""
    try:
        frame = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return
    req_id = str(frame.get("reqId") or "")
    if not req_id:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "missing request id"})
        return

    import mcp_plugin_store

    identity = _chat_identity(adapter, frame)
    client_id = identity["client_id"]
    # 网页对话由 CDP 客户端驱动：可选 agent = 该客户端的 agent_ids（未选则不允许操作）。
    if not client_id:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "连接身份无效"})
        return

    kind = frame.get("type")
    if kind == "chat_list_agents":
        agents = await mcp_plugin_store.list_agents_for_cdp_client(client_id)
        await _send_chat_frame(websocket, {"type": "chat_agents", "reqId": req_id, "agents": agents})
        return

    if kind == "chat_list_models":
        await _proxy_chat_list_models(frame, identity, websocket)
        return

    if kind == "chat_abort":
        await _proxy_chat_abort(frame, {}, identity, websocket)
        return

    if kind == "chat_list_conversations":
        await _proxy_chat_list_conversations(frame, identity, websocket)
        return

    if kind == "chat_new_conversation":
        await _proxy_chat_new_conversation(frame, identity, websocket)
        return

    if kind == "chat_load_conversation":
        await _proxy_chat_load_conversation(frame, identity, websocket)
        return

    if kind == "chat_resolve_approval":
        await _proxy_chat_resolve_approval(frame, identity, websocket)
        return

    if kind == "chat_retry":
        conv_id = str(frame.get("conversationId") or "").strip()
        message_id = frame.get("messageId")
        if not conv_id or message_id is None:
            await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "conversationId and messageId are required"})
            return
        # retry 走 client_send_message 同一套鉴权，agent 由服务端从会话取，
        # 这里不重复 page 归属校验（会话已属本 client，服务端再核 client/session）。
        task = asyncio.create_task(_proxy_chat_retry(frame, identity, websocket))
        _CHAT_TASKS[req_id] = task
        task.add_done_callback(lambda _: _CHAT_TASKS.pop(req_id, None))
        return

    if kind != "chat_send":
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "unsupported chat frame"})
        return

    agent_id = frame.get("agentId")
    content = str(frame.get("content") or "").strip()
    if not isinstance(agent_id, int) or not content:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "agentId and content are required"})
        return
    page = _owned_chat_page(driver, adapter, identity["session_key"]) if driver is not None else None
    if page is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "page does not belong to this client"})
        return
    # URL/title 同样以 driver 已认证页面快照为准，不信任 chat frame。
    frame = {
        **frame,
        "url": page.info.get("url") or "",
        "title": page.info.get("title") or "",
    }

    allowed = await mcp_plugin_store.list_agents_for_cdp_client(client_id)
    if not any(agent.get("id") == agent_id for agent in allowed):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "agent is not allowed for this client"})
        return

    task = asyncio.create_task(_proxy_chat_stream(frame, {}, identity, websocket))
    _CHAT_TASKS[req_id] = task
    task.add_done_callback(lambda _: _CHAT_TASKS.pop(req_id, None))


def _is_connect_phase_error(exc: BaseException) -> bool:
    """异常是否发生在「请求还没送到 agent」的建连阶段。

    只有建连失败才能安全重发：POST body 根本没到主服务，agent 没起过这一轮，
    重试不会让已经执行过的工具再跑一遍。读流中途断开不算 —— 那时 agent 可能
    已经调过浏览器工具了。
    """
    import aiohttp

    return isinstance(exc, (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError))


def _relay_failure_message(exc: BaseException) -> str:
    """把内部异常转成给网页用户看的话术。

    异常原文带回环地址和内部路径（aiohttp 的连接超时原文就是
    "Connection timeout to host http://127.0.0.1:8001/agent/client/messages"），
    直接回投等于把内部端口暴露给页面。真实原因只进服务端日志。
    """
    import aiohttp

    # ServerTimeoutError 是 ClientConnectionError 的子类，超时判断必须在前。
    if isinstance(exc, (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError, asyncio.TimeoutError)):
        return "网络异常：连接对话服务超时，请稍后重试"
    if isinstance(exc, (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError)):
        return "网络异常：对话连接中断，请重试"
    return "对话服务异常，请稍后重试"


def _upstream_error_message(status: int, body: str) -> str:
    """把 agent 端点的 4xx/5xx 响应体转成给页面看的话术。

    响应体可能是 FastAPI 的 ``{"detail": ...}``，也可能是裸文本。取出其中人类
    可读的那段后再统一擦掉 URL —— 上游文案里也可能嵌内部地址，不能原样回投。
    """
    text = ""
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, dict):
        for key in ("message", "detail", "error"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                text = value.strip()
                break
            if isinstance(value, dict) and isinstance(value.get("message"), str):
                text = str(value["message"]).strip()
                break
    elif isinstance(body, str):
        text = body.strip()
    if not text:
        return f"对话服务返回错误（HTTP {status}），请稍后重试"
    return re.sub(r"https?://[^\s,'\"]+", "内部服务", text)[:300]


async def _relay_agent_sse(url: str, headers: dict, payload: dict | None, req_id: str, websocket: WebSocket) -> None:
    """POST 主服务的 agent SSE 端点，逐事件回投到扩展 WebSocket。

    收尾策略（稳健优先）：
    - agent 流正常结束会带 ``done``/``error`` 事件，收到即回投并返回。
    - 若底层 chunked 传输被截断（aiohttp 抛 ClientPayloadError/TransferEncodingError）
      或流在没有终止事件的情况下自然 EOF：只要已经收到过内容，就补发一个合成
      ``done`` 事件，让面板正常收尾而不是把传输层抖动当成对话失败。
    - 只有在「一个字节都没读到」时才按错误回传，避免把 agent 输出丢给用户。
    - 任何路径都保证最终有且只有一个终止帧（done/error），面板才能解锁。

    建连阶段失败（连不上/连接超时）会自动重发几次：那时 POST body 还没到主服务，
    agent 没起过这一轮，重发不会让已执行的工具重跑。一旦读到过任何事件就不再
    重发 —— agent 已经在干活了，重发会产生第二轮副作用。

    回投给页面的错误文案一律走 _relay_failure_message / _upstream_error_message
    归一，异常原文只进服务端日志（原文含回环地址与内部路径）。

    chat_send 与 chat_retry 共用：两者的事件流形态相同，只有 URL/payload 不同。
    """
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=None)

    saw_terminal = False   # 收到过 agent 明确的 done/error 事件
    saw_any_event = False  # 收到过任何可解析事件（含 conversation/content/...）

    async def _emit_event(event: dict) -> bool:
        """回投一个 agent 事件；返回 True 表示这是终止事件（done/error）。"""
        nonlocal saw_any_event
        saw_any_event = True
        await _send_chat_frame(websocket, {"type": "chat_event", "reqId": req_id, "event": event})
        return event.get("type") in {"done", "error"}

    max_attempts = 3
    backoff = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status >= 400:
                        detail = await response.text()
                        logger.warning(f"[cdp-chat] agent 端点 {url} 返回 HTTP {response.status}: {detail[:500]}")
                        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _upstream_error_message(response.status, detail)})
                        return
                    buffer = ""
                    try:
                        async for chunk in response.content.iter_any():
                            buffer += chunk.decode("utf-8", errors="replace")
                            lines = buffer.split("\n")
                            buffer = lines.pop()
                            for line in lines:
                                if not line.startswith("data: "):
                                    continue
                                try:
                                    event = json.loads(line[6:])
                                except json.JSONDecodeError:
                                    continue
                                if await _emit_event(event):
                                    saw_terminal = True
                                    return
                    except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as exc:
                        # chunked 传输被截断（"Not enough data for satisfy transfer length
                        # header." 即属此类）。已读到内容 → 按正常收尾补 done；否则才算失败。
                        if not saw_any_event:
                            logger.warning(f"[cdp-chat] agent 流在读到任何事件前中断（{url}）: {exc!r}")
                            await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
                            return
                    # EOF：flush 尾部缓冲里可能残留的最后一条完整 data 行。
                    tail = buffer.strip()
                    if tail.startswith("data: "):
                        try:
                            event = json.loads(tail[6:])
                        except json.JSONDecodeError:
                            event = None
                        if event is not None and await _emit_event(event):
                            saw_terminal = True
                            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if saw_terminal:
                return
            if attempt < max_attempts and not saw_any_event and _is_connect_phase_error(exc):
                logger.warning(f"[cdp-chat] 连接 agent 服务失败（第 {attempt}/{max_attempts} 次，{backoff}s 后重试）: {exc!r}")
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            logger.warning(f"[cdp-chat] 转发 agent 流失败（{url}）: {exc!r}")
            await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
            return
        break

    # 流结束但 agent 没发终止事件（自然 EOF / 截断后已有内容）：补一个 done 收尾，
    # 面板据此解锁，不把传输层抖动误报成对话错误。
    if not saw_terminal:
        await _send_chat_frame(websocket, {"type": "chat_event", "reqId": req_id, "event": {"type": "done"}})


def _agent_internal_headers(identity: dict) -> dict | None:
    """构造调主服务 CDP agent 端点的内部鉴权头；未配置 token 返回 None。"""
    import os

    internal_token = os.environ.get("AGENT_INTERNAL_TOKEN", "")
    if not internal_token:
        return None
    return {
        "X-Internal-Token": internal_token,
        "X-Cdp-Client-Id": identity["client_id"],
        "X-Cdp-Client-Alias": identity["client_alias"],
        "X-Cdp-Session-Key": identity["session_key"],
    }


async def _proxy_chat_stream(frame: dict, user: dict, identity: dict, websocket: WebSocket) -> None:
    """发起一轮网页对话：组装 payload 后交 _relay_agent_sse 回投事件。"""
    req_id = str(frame["reqId"])
    agent_base = _AGENT_INTERNAL_BASE
    headers = _agent_internal_headers(identity)
    if headers is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    payload = {
        "agent_id": frame["agentId"],
        "content": frame["content"],
        "tab_id": frame.get("tabId"),
        "url": frame.get("url") or "",
        "title": frame.get("title") or "",
        "conversation_id": frame.get("conversationId") or "",
    }
    # 对话级覆盖（面板输入区选择）：显式给了才透传，None/空由服务端沿用会话已存值。
    if frame.get("model"):
        payload["model"] = str(frame["model"])
    if frame.get("reasoningEffort") is not None:
        payload["reasoning_effort"] = str(frame.get("reasoningEffort") or "")
    mode = str(frame.get("mode") or "interact").lower()
    if mode in ("interact", "plan", "goal"):
        payload["mode"] = mode
    if mode == "goal":
        objective = str(frame.get("goalObjective") or "").strip()
        try:
            minutes = float(frame.get("goalBudgetMinutes") or 15)
        except (TypeError, ValueError):
            minutes = 15.0
        payload["goal_config"] = {
            "objective": objective,
            "budget_seconds": max(60, int(minutes * 60)),
        }
    await _relay_agent_sse(f"{agent_base}/agent/client/messages", headers, payload, req_id, websocket)


async def _proxy_chat_retry(frame: dict, identity: dict, websocket: WebSocket) -> None:
    """重试一条失败的 assistant 消息：调主服务 retry 端点，事件流与 send 同形。"""
    req_id = str(frame["reqId"])
    agent_base = _AGENT_INTERNAL_BASE
    headers = _agent_internal_headers(identity)
    if headers is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    conv_id = str(frame.get("conversationId") or "").strip()
    message_id = frame.get("messageId")
    if not conv_id or message_id is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "conversationId and messageId are required"})
        return
    url = f"{agent_base}/agent/client/conversations/{conv_id}/messages/{message_id}/retry"
    await _relay_agent_sse(url, headers, None, req_id, websocket)



async def _proxy_chat_abort(frame: dict, _user: dict, identity: dict, websocket: WebSocket) -> None:
    """取消本 runtime 的流任务，并通知主服务取消对应 conversation。"""
    import os
    import aiohttp

    req_id = str(frame.get("reqId") or "")
    task = _CHAT_TASKS.pop(req_id, None)
    if task and not task.done():
        task.cancel()
    conversation_id = str(frame.get("conversationId") or "")
    if conversation_id:
        base = _AGENT_INTERNAL_BASE
        internal_token = os.environ.get("AGENT_INTERNAL_TOKEN", "")
        if internal_token:
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"{base}/agent/client/conversations/{conversation_id}/abort",
                        headers={
                            "X-Internal-Token": internal_token,
                            "X-Cdp-Client-Id": identity["client_id"],
                            "X-Cdp-Client-Alias": identity["client_alias"],
                            "X-Cdp-Session-Key": identity["session_key"],
                        },
                    )
            except Exception:
                pass
    await _send_chat_frame(websocket, {"type": "chat_aborted", "reqId": req_id})


def _internal_agent_headers(identity: dict, internal_token: str) -> dict:
    return {
        "X-Internal-Token": internal_token,
        "X-Cdp-Client-Id": identity["client_id"],
        "X-Cdp-Client-Alias": identity["client_alias"],
        "X-Cdp-Session-Key": identity["session_key"],
    }


async def _agent_internal_request(method: str, path: str, identity: dict, *, json_body: dict | None = None):
    """向主服务进程内的 CDP Agent 端点发一次非流式请求，返回 (status, json_or_none)。

    未配置 internal token → (None, None)。网络/解析异常 → (status, None) 或 (None, None)。
    """
    import os
    import aiohttp

    base = _AGENT_INTERNAL_BASE
    internal_token = os.environ.get("AGENT_INTERNAL_TOKEN", "")
    if not internal_token:
        return None, None
    headers = _internal_agent_headers(identity, internal_token)
    timeout = aiohttp.ClientTimeout(total=30, sock_connect=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.request(method, f"{base}{path}", headers=headers, json=json_body) as response:
            status = response.status
            try:
                data = await response.json()
            except Exception:  # noqa: BLE001
                data = None
            return status, data


async def _proxy_chat_list_models(frame: dict, identity: dict, websocket: WebSocket) -> None:
    """列出该 agent 网关 key 白名单内的可选模型，回投 chat_models 帧。"""
    req_id = str(frame.get("reqId") or "")
    agent_id = frame.get("agentId")
    if not isinstance(agent_id, int):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "agentId is required"})
        return
    try:
        status, data = await _agent_internal_request(
            "GET", f"/agent/client/models?agent_id={agent_id}", identity
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cdp-chat] 取模型列表失败: {exc!r}")
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
        return
    if status is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    if status >= 400 or not isinstance(data, dict):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": f"agent service HTTP {status}"})
        return
    await _send_chat_frame(websocket, {
        "type": "chat_models",
        "reqId": req_id,
        "models": data.get("data") or [],
        "defaultModel": data.get("default_model") or "",
    })


async def _proxy_chat_list_conversations(frame: dict, identity: dict, websocket: WebSocket) -> None:
    """列出本页面名下的会话，回投 chat_conversations 帧。"""
    req_id = str(frame.get("reqId") or "")
    try:
        status, data = await _agent_internal_request("GET", "/agent/client/conversations", identity)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cdp-chat] 取会话列表失败: {exc!r}")
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
        return
    if status is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    if status >= 400 or not isinstance(data, dict):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": f"agent service HTTP {status}"})
        return
    await _send_chat_frame(websocket, {"type": "chat_conversations", "reqId": req_id, "conversations": data.get("conversations") or []})


async def _proxy_chat_new_conversation(frame: dict, identity: dict, websocket: WebSocket) -> None:
    """显式新建一个空会话，回投 chat_conversation_created 帧。"""
    req_id = str(frame.get("reqId") or "")
    agent_id = frame.get("agentId")
    if not isinstance(agent_id, int):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "agentId is required"})
        return
    body = {"agent_id": agent_id, "title": str(frame.get("title") or "")}
    try:
        status, data = await _agent_internal_request("POST", "/agent/client/conversations", identity, json_body=body)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cdp-chat] 新建会话失败: {exc!r}")
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
        return
    if status is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    if status >= 400 or not isinstance(data, dict):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": f"agent service HTTP {status}"})
        return
    await _send_chat_frame(websocket, {"type": "chat_conversation_created", "reqId": req_id, "conversation": data.get("conversation") or {}})


async def _proxy_chat_load_conversation(frame: dict, identity: dict, websocket: WebSocket) -> None:
    """取一个会话的消息历史，回投 chat_conversation_loaded 帧。"""
    req_id = str(frame.get("reqId") or "")
    conv_id = str(frame.get("conversationId") or "").strip()
    if not conv_id:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "conversationId is required"})
        return
    try:
        status, data = await _agent_internal_request("GET", f"/agent/client/conversations/{conv_id}", identity)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cdp-chat] 载入会话 {conv_id} 失败: {exc!r}")
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
        return
    if status is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    if status >= 400 or not isinstance(data, dict):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": f"agent service HTTP {status}"})
        return
    await _send_chat_frame(websocket, {
        "type": "chat_conversation_loaded",
        "reqId": req_id,
        "conversation": data.get("conversation") or {},
        "messages": data.get("messages") or [],
    })


async def _proxy_chat_resolve_approval(frame: dict, identity: dict, websocket: WebSocket) -> None:
    """把面板的审批裁决转给主服务，回投 chat_approval_resolved 帧。

    裁决唤醒的是主服务里 await 着的那一轮 agent（approval_registry 单例持有
    future），本 runtime 只做转发——正在跑的 SSE 流会自己继续吐事件。
    """
    req_id = str(frame.get("reqId") or "")
    conv_id = str(frame.get("conversationId") or "").strip()
    confirmation_id = str(frame.get("confirmationId") or "").strip()
    result = str(frame.get("result") or "").strip().lower()
    command_hash = str(frame.get("commandHash") or "")
    if not conv_id or not confirmation_id:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "conversationId and confirmationId are required"})
        return
    if result not in ("allow", "deny"):
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "result must be allow or deny"})
        return
    try:
        status, data = await _agent_internal_request(
            "POST",
            f"/agent/client/conversations/{conv_id}/approvals/{confirmation_id}",
            identity,
            json_body={"result": result, "command_hash": command_hash},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cdp-chat] 提交审批 {confirmation_id} 失败: {exc!r}")
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": _relay_failure_message(exc)})
        return
    if status is None:
        await _send_chat_frame(websocket, {"type": "chat_error", "reqId": req_id, "message": "AGENT_INTERNAL_TOKEN is not configured"})
        return
    if status >= 400:
        # 审批已被处理/过期/哈希不符：把可读原因回投，面板据此把卡片翻成终态。
        message = ""
        if isinstance(data, dict):
            detail = data.get("detail")
            if isinstance(detail, str):
                message = detail.strip()
        await _send_chat_frame(websocket, {
            "type": "chat_approval_resolved",
            "reqId": req_id,
            "ok": False,
            "result": result,
            "message": message or f"审批提交失败（HTTP {status}）",
        })
        return
    await _send_chat_frame(websocket, {
        "type": "chat_approval_resolved",
        "reqId": req_id,
        "ok": True,
        "result": result,
    })


_CHAT_TASKS: dict[str, asyncio.Task] = {}


async def _send_ws_protocol(websocket: WebSocket, kind: str, **payload) -> None:
    await websocket.send_text(json.dumps({"type": kind, "protocol": 2, **payload}, ensure_ascii=False))


async def _authenticate_ext_frame(raw: str, websocket: WebSocket, driver, adapter: _ExtWsAdapter) -> bool:
    try:
        frame = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        await _send_ws_protocol(websocket, "auth_error", code="invalid_json", message="first frame must be auth protocol 2")
        return False
    if frame.get("type") != "auth" or frame.get("protocol") != 2:
        await _send_ws_protocol(websocket, "auth_error", code="protocol_required", message="auth protocol 2 required")
        return False
    token, client_id = str(frame.get("token") or ""), str(frame.get("client_id") or "")
    config = driver.authenticate_client(token, client_id or None)
    if not config:
        await _send_ws_protocol(websocket, "auth_error", code="invalid_token", message="invalid or revoked CDP client token")
        return False
    try:
        driver.bind_client(adapter, config)
    except ValueError:
        await _send_ws_protocol(websocket, "auth_error", code="already_connected", message="this token already has an active connection")
        return False
    adapter._client_alias = str(config.get("name") or config["client_id"])
    # 会话池按 client_id 隔离，CDP 客户端不再归属单个 user_id；auth_ok 只回 client_id。
    await _send_ws_protocol(
        websocket,
        "auth_ok",
        client_id=config["client_id"],
    )
    return True


@router.websocket("/{service_name}/session")
async def ext_session(websocket: WebSocket, service_name: str):
    """CDP extension WebSocket using mandatory explicit auth protocol 2.

    The first frame must be ``{"type":"auth","protocol":2,"token":...}``.
    The token determines the authoritative client ID (its ``instance_key``).
    Legacy MCP-user tokens and unauthenticated ``ext_ready`` first frames are rejected.
    """
    if service_name != "cdp-bridge":
        await websocket.close(code=4404)
        return
    plugin = registry.get(service_name)
    if not plugin or not plugin.ctx.enabled:
        await websocket.close(code=4403, reason="service disabled")
        return
    if not plugin.ctx.auth_enabled:
        await websocket.close(code=4403, reason="CDP Bridge requires MCP user authentication")
        return
    try:
        from mcp_builtin.cdp_bridge.server import get_driver
        driver = get_driver()
    except Exception:
        driver = None
    if driver is None:
        await websocket.close(code=4503)
        return

    await websocket.accept()
    loop = asyncio.get_running_loop()
    adapter = _ExtWsAdapter(websocket, loop)
    try:
        first = await asyncio.wait_for(websocket.receive_text(), timeout=10)
        if not await _authenticate_ext_frame(first, websocket, driver, adapter):
            await websocket.close(code=4401)
            return
        while True:
            # 空闲读超时：半开连接（MV3 worker 被杀 / 网络切换 / 睡眠）不会发 TCP FIN，
            # receive_text() 会永远阻塞，WebSocketDisconnect 永不触发 → finally 里的
            # unregister_client 永不执行 → ws_client 一直非空 → 同一浏览器正常重连撞
            # already_connected 被锁死。客户端每 ~24s 发一次 ping（keepalive），故 60s
            # 内一帧都收不到即判定连接已死，主动关闭让 finally 释放会话槽。
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=_EXT_IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                break
            # Re-check the persisted hash snapshot on every frame so a runtime
            # reload after revoke/rotation invalidates the live connection.
            if not adapter._token or adapter._token not in driver.token_manager.clients_by_hash:
                await _send_ws_protocol(websocket, "auth_error", code="revoked", message="client authorization revoked")
                await websocket.close(code=4403)
                return
            await _route_ext_frame(raw, websocket, adapter, driver)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            driver.unregister_client(adapter)
        except Exception:
            pass


# ── device-control：Android App 长连端点（spec v0）─────────────────────────────
#
# 与 cdp-bridge 的 /session 不同协议：首帧是 ``register``（spec §4），鉴权是
# ``auth.scheme == "token"`` + ``builtin_tool_store.authenticate_device``，不是 CDP
# 的 auth protocol 2。帧用 spec 的 close code（4002-4010），不是 CDP 的 4xxx。
# 两套读循环刻意不共用骨架：close 码、帧类型、鉴权复查条件都不同，硬抽会扭曲。


def _device_driver():
    """device-control driver 单例取法。模块顶层 import 会触发重依赖，故延迟。"""
    from mcp_builtin.device_control import driver as dc_driver
    return dc_driver.get_driver()


@router.websocket("/{service_name}/ws/device")
async def device_ws(websocket: WebSocket, service_name: str):
    """device-control 设备 WebSocket（spec §4 握手 + §3 帧路由 + §7 空闲收割）。"""
    if service_name != "device-control":
        # 未知服务名直接拒，不让别的服务意外挂上设备端点。
        await websocket.close(code=4404)
        return
    plugin = registry.get(service_name)
    if not plugin or not plugin.ctx.enabled:
        await websocket.close(code=4403, reason="service disabled")
        return
    try:
        driver = _device_driver()
    except Exception:
        driver = None
    if driver is None:
        await websocket.close(code=4503, reason="device-control driver not loaded")
        return

    await websocket.accept()
    ctx = None
    try:
        try:
            first = await asyncio.wait_for(websocket.receive_text(), timeout=proto.REGISTER_TIMEOUT_S)
        except asyncio.TimeoutError:
            # spec §4.1：10s 内没收到 register → close 4008。区别于「发了坏帧」的协议错误。
            await websocket.close(code=proto.CLOSE_REGISTER_TIMEOUT, reason="no register within deadline")
            return
        if len(first.encode("utf-8")) > proto.MAX_FRAME_BYTES:
            await websocket.close(code=proto.CLOSE_FRAME_TOO_LARGE, reason="frame exceeds 4MiB")
            return
        ctx = await _device_register(first, websocket, driver)
        if ctx is None:
            return  # 握手失败已按 spec close
        await driver.register(ctx)
        await _device_read_loop(websocket, ctx, driver)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[device-control] ws error: {exc}")
    finally:
        if ctx is not None:
            # 旧连接的 read loop 走到这里收尾；unregister 仅在 ctx 仍是当前登记的那条时摘。
            driver.unregister(ctx)
            # 幂等收尾：把残留 pending future 失败掉（断连置异常）。close 已断的连接是 no-op。
            try:
                await ctx.close(proto.CLOSE_STALE, "connection ended")
            except Exception:
                pass


async def _device_register(raw: str, websocket: WebSocket, driver) -> Any:
    """处理 register 首帧（spec §4）。返回 DeviceContext 或 None（已 close）。"""
    try:
        frame = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        await websocket.close(code=1002, reason="first frame must be valid JSON")
        return None
    if not isinstance(frame, dict) or frame.get("type") != proto.TYPE_REGISTER:
        await websocket.close(code=1002, reason="first frame must be register")
        return None

    # 协议版本是服务端权威（spec §14）。不假定兼容；不匹配直接 close 4004。
    pv = frame.get("protocol_version")
    if not isinstance(pv, int) or pv != proto.VERSION:
        await websocket.close(code=proto.CLOSE_VERSION_UNSUPPORTED, reason="unsupported protocol_version")
        return None

    auth = frame.get("auth")
    if not isinstance(auth, dict) or auth.get("scheme") != proto.SCHEME_TOKEN:
        # 未知 scheme 当鉴权失败（spec §11），不是版本失败。
        await websocket.close(code=proto.CLOSE_AUTH_FAILED, reason="unsupported auth scheme")
        return None
    device_id = str(frame.get("device_id") or "").strip()
    token = str(auth.get("token") or "").strip()
    if not device_id or not token:
        await websocket.close(code=proto.CLOSE_AUTH_FAILED, reason="invalid credentials")
        return None

    # authenticate_device 走 sha256 常量时间比对；不匹配返回 None → 4003。
    import builtin_tool_store
    detail = await builtin_tool_store.authenticate_device(device_id, token)
    if detail is None:
        logger.info(f"[device-control] auth failed: device_id={device_id}")
        await websocket.close(code=proto.CLOSE_AUTH_FAILED, reason="invalid credentials")
        return None

    caps_raw = frame.get("capabilities")
    capabilities = [str(c) for c in caps_raw if isinstance(c, str)] if isinstance(caps_raw, list) else []
    device_info = frame.get("device_info") if isinstance(frame.get("device_info"), dict) else {}

    session_id = proto.new_id("ses_")
    send_lock = asyncio.Lock()

    async def send(frame: dict) -> None:
        # 串行化 send_text：并发下发 call + call-cancel 会交错，Starlette 不保证并发安全。
        async with send_lock:
            await websocket.send_text(json.dumps(frame, ensure_ascii=False))

    async def close(code: int, reason: str) -> None:
        try:
            await websocket.close(code=code, reason=reason)
        except Exception:
            pass

    ctx = DeviceContext(
        device_id=device_id,
        session_id=session_id,
        token_hash=str(detail.get("token_hash") or ""),
        capabilities=capabilities,
        device_info=device_info,
        send=send,
        close=close,
    )
    # spec §4：认证通过后回 registered 帧，把协商参数（心跳间隔/超时、session_id、
    # 接受的能力）下发给设备。设备收到后才认为握手完成、可开始收 call。
    from datetime import datetime, timezone
    await send(proto.registered_frame(
        device_id=device_id,
        session_id=session_id,
        server_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        accepted_capabilities=capabilities,
    ))
    return ctx


async def _device_read_loop(websocket: WebSocket, ctx, driver) -> None:
    """设备帧路由循环（spec §3、§7）。"""
    try:
        while True:
            # 空闲读超时：安卓半开连接（doze / 蜂窝↔WiFi 切换 / 进程被后台回收）不发 TCP FIN，
            # receive_text 会永久阻塞。心跳 15s 一次，60s 内一帧都收不到即判定连接已死，
            # close 4010 触发 driver.register 收尾（pending future 置 DeviceOffline）。
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=proto.HEARTBEAT_TIMEOUT_S)
            except asyncio.TimeoutError:
                await ctx.close(proto.CLOSE_STALE, "heartbeat timeout")
                return
            # spec §2：4 MiB 帧上限。超了 close 4002，别让恶意/异常帧撑爆内存。
            if len(raw.encode("utf-8")) > proto.MAX_FRAME_BYTES:
                await ctx.close(proto.CLOSE_FRAME_TOO_LARGE, "frame exceeds 4MiB")
                return
            # 任意帧都刷新活性（spec §7），含 heartbeat / call-response / event。
            ctx.touch()
            # 每帧复查授权快照：用户在前端解除配对/轮换 token 后，活连接当场断（close 4003），
            # 不等心跳超时。与 cdp 的 clients_by_hash 复查同一思路。
            if not driver.is_authorized(ctx.token_hash):
                await ctx.close(proto.CLOSE_AUTH_FAILED, "credential revoked")
                return
            try:
                frame = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue  # 坏帧忽略：不要因为一条坏 JSON 断掉整条设备连接
            if not isinstance(frame, dict):
                continue
            ftype = frame.get("type")
            if ftype == proto.TYPE_HEARTBEAT:
                continue  # spec §10：心跳无 ack，Touch 已记活性
            if ftype == proto.TYPE_CALL_RESPONSE:
                ctx.deliver(
                    str(frame.get("request_id") or ""),
                    bool(frame.get("ok")) if isinstance(frame.get("ok"), bool) else None,
                    frame.get("data"),
                    frame.get("error") if isinstance(frame.get("error"), dict) else None,
                )
            elif ftype == proto.TYPE_EVENT:
                await _device_handle_event(frame, ctx)
            elif ftype == proto.TYPE_REGISTER:
                # 一条连接上的第二次 register 是致命错误（spec §4.3）。
                await ctx.close(proto.CLOSE_DUPLICATE_REGISTER, "duplicate register")
                return
            # 未知 type：忽略（spec §3 前向兼容铰链），不当错误。
    except WebSocketDisconnect:
        pass


async def _device_handle_event(frame: dict, ctx) -> None:
    """处理设备上报事件（spec §9）。"""
    kind = str(frame.get("kind") or "")
    if kind == proto.EVENT_CAPABILITIES_CHANGED:
        caps = frame.get("capabilities")
        if isinstance(caps, list):
            ctx.set_capabilities([str(c) for c in caps if isinstance(c, str)])
    elif kind == proto.EVENT_CONTROL_REVOKED:
        # 设备侧主动关控制权（spec §9）：当不可控直到下次 register。
        await ctx.close(1000, "control revoked on device")
    # 未知 event kind：忽略（spec §9）。


@router.post("/device-control/pair")
async def device_pair(request: Request) -> dict:
    """兑换配对码为长期设备凭据（spec §11）。

    无鉴权——配对码本身就是凭证。返回 ``{device_id, token, protocol_version}``，
    token 明文只此一次返回；DB 只存 sha256。403 对「不存在 / 过期 / 已用」不做区分
    （spec 要求），App 据此统一提示。
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "invalid JSON body")
    if not isinstance(body, dict):
        raise HTTPException(400, "invalid JSON body")
    code = str(body.get("code") or "").strip()
    if not code:
        raise HTTPException(400, "code is required")

    import builtin_tool_store
    from mcp_builtin.device_control import store as dc_store
    try:
        payload, label = await dc_store.redeem_pairing_code(code)
    except dc_store.UnknownCodeError:
        # 不区分 unknown/expired/used（spec §11）：App 端 PairingClient 已按此实现。
        raise HTTPException(403, "unknown or expired pairing code")
    except dc_store.RedisUnavailableError as exc:
        raise HTTPException(503, str(exc)) from exc

    # 兑换成功才签发凭据；配对码此时已被 Redis GETDEL 消费（单次使用）。
    # payload 带 owner 前缀（"owner:<uid>"）——资源模型里设备直接归属用户。
    owner_part, _, instance_part = str(payload).partition("|")
    if owner_part.startswith("owner:"):
        owner_user_id = owner_part[len("owner:"):]
        if instance_part.isdigit() and int(instance_part) != 0:
            # 兼容旧 payload：数值部分曾是实例 id，改用该实例 owner。
            legacy = await builtin_tool_store.get_resource(int(instance_part))
            if legacy is not None:
                owner_user_id = str(legacy.get("owner_user_id") or owner_user_id)
        resource, device_id, token = await builtin_tool_store.create_device(owner_user_id, label)
    else:
        # 旧 payload 是裸实例 id——迁移前签出的码仍要能兑换。
        resource, device_id, token = await builtin_tool_store.create_device(instance_part or "0", label)
    # 把新凭据立刻推给活着的 driver，否则刚配对的设备要等下次 admin reload 才能连上。
    driver = _device_driver()
    if driver is not None:
        driver.apply_config(await builtin_tool_store.device_authorized_hashes())
    logger.info(f"[device-control] device paired: device_id={device_id}")
    return {
        "device_id": device_id,
        "token": token,
        "protocol_version": proto.VERSION,
    }

