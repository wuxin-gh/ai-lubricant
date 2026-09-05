"""MCP 安装编排器（形态 A：全局远程）。

把 create_service 之后的「建了记录但没启动」状态，串成一条可见进度：
  created -> configuring -> starting -> testing -> ready | error

与 runtime_status（运行态 stopped|loaded|error）语义分开：install_state 是**安装进度**，
一旦走到 ready 就稳定，不再随运行态抖动。

session 形态（stdio 会话内）不走此流程——它没有「安装」，进程由编辑器 CLI 自己在节点
拉起，create_service 时已直接标 ready。

设计要点：
- 后台任务跑（asyncio.create_task），不阻塞 create 请求——对齐 ManageEditor 的
  ack-early 风格。
- 缺必填项停在 configuring 并把缺失写 install_step，**不算 error**——补完 retry 继续。
- testing 用 discover_tools 真列一次工具（比 test_config 的 shutil.which/HTTP GET 强），
  成功则写 tools_cache，这才是真实可用性验证。
"""
from __future__ import annotations

import asyncio
from typing import Any

import mcp_plugin_store
from loguru import logger


async def run_install(service_id: int) -> None:
    """跑一次安装编排。失败写 install_state=error + install_error，不抛。"""
    try:
        await _run_install_inner(service_id)
    except Exception as e:  # 兜底：编排内部未捕获的异常
        logger.exception("install orchestrator crashed for service {}", service_id)
        await mcp_plugin_store.mark_install_state(
            service_id, "error", step="编排异常", error=str(e)[:4000]
        )


async def _run_install_inner(service_id: int) -> None:
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        return
    scope = service.get("deploy_scope") or "server"
    # 只有全局形态（server / node_hosted）走编排；session 已在 create 时标 ready。
    if scope == "session":
        return

    # ── 1. configuring：校验必填 ──
    await mcp_plugin_store.mark_install_state(service_id, "configuring", step="校验配置")
    missing = _missing_required_fields(service)
    if missing:
        # 停在 configuring，不算 error——补完 retry 继续
        await mcp_plugin_store.mark_install_state(
            service_id, "configuring", step=f"需补充: {', '.join(missing)}"
        )
        return

    # ── 2. starting ──
    # node_hosted（形态 C）的进程已由 deploy_to_node 在节点上起好，这里不再 start：
    # 服务端没有它的插件对象，start_service_core 会走 stdio 分支去找执行器而失败。
    if scope == "node_hosted":
        await mcp_plugin_store.mark_install_state(service_id, "starting", step="等待节点进程就绪")
        started = {"ok": True, "tools_cache": None}
    else:
        await mcp_plugin_store.mark_install_state(service_id, "starting", step="启动服务")
        from .admin_api import start_service_core
        started = await start_service_core(service_id)
        if not started.get("ok"):
            await mcp_plugin_store.mark_install_state(
                service_id, "error", step="启动", error=started.get("error", "启动失败")
            )
            return

    # ── 3. testing：真列一次工具，验证可用性 ──
    await mcp_plugin_store.mark_install_state(service_id, "testing", step="验证工具")
    tools_cache = started.get("tools_cache")
    if tools_cache is None and scope == "node_hosted":
        # 经隧道向节点上的托管进程真列一次工具，顺带验证隧道通
        tools_cache = await _discover_node_hosted_tools(service)
    if tools_cache is None:
        # 兜底：start 没回 tools_cache 时再探一次（经运行时 gateway 调 tools/list）
        tools_cache = await _discover_tools(service)
    if tools_cache is None:
        await mcp_plugin_store.mark_install_state(
            service_id, "error", step="验证工具", error="无法获取工具列表"
        )
        return

    # ── 4. ready ──
    await mcp_plugin_store.mark_runtime_status(
        service_id, "loaded", tools_cache=tools_cache
    )
    if scope == "node_hosted":
        # 工具列出来了，说明节点上的进程真的在跑且隧道通
        await mcp_plugin_store.mark_host_state(service_id, status="running")
    await mcp_plugin_store.mark_install_state(service_id, "ready", step="就绪")


async def _discover_node_hosted_tools(service: dict) -> list[dict] | None:
    """经节点隧道向托管进程发一次 tools/list。失败返回 None（上层标 error）。

    托管进程被 mcp-proxy 包成 SSE，标准 MCP 握手要先开 SSE 拿 endpoint 再 POST；
    这里只需验证「进程活着且隧道通」，所以直接 POST 一次 tools/list 到 /messages，
    拿到 JSON-RPC 结果即算通过。
    """
    import json as _json

    from .node_hosted import NodeHostedError, proxy_request

    payload = _json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode()
    try:
        resp = await proxy_request(
            service, method="POST", path="/messages",
            headers={"Content-Type": "application/json"}, body=payload,
        )
        chunks = b""
        async for chunk in resp.iter_chunks():
            chunks += chunk
            if len(chunks) > 1 << 20:  # 工具列表不该有 1MB，防失控
                break
        await resp.close()
    except (NodeHostedError, ConnectionError) as e:
        logger.warning("node-hosted discover failed for {}: {}", service.get("name"), e)
        return None
    except Exception as e:  # noqa: BLE001
        logger.warning("node-hosted discover error for {}: {}", service.get("name"), e)
        return None

    name = service.get("name") or ""
    try:
        data = _json.loads(chunks.decode("utf-8", "replace") or "{}")
    except ValueError:
        logger.warning("node-hosted {} returned non-JSON tools/list", name)
        return None
    tools = ((data.get("result") or {}).get("tools")) or []
    if not isinstance(tools, list) or not tools:
        return None
    return [
        {
            "name": t.get("name") or "",
            "description": t.get("description") or "",
            "input_schema": t.get("inputSchema") or t.get("input_schema") or {},
            "service_name": name,
        }
        for t in tools
        if isinstance(t, dict) and t.get("name")
    ]


def _missing_required_fields(service: dict) -> list[str]:
    """返回缺失的必填项。remote 需 url；stdio 需 command（node_hosted 也要 command）。

    只校验 transport 级必填--env_template 里的必填项由使用者在 env-vars 面板补，
    实际 env 存在 mcp_runtime_configs 表，查询耦合较重且缺 env 不一定能阻断 discover_tools，
    故不在此卡。补 env 后 retry 自然推进。
    """
    missing: list[str] = []
    transport = (service.get("transport") or "").lower()
    is_remote = transport in ("sse", "streamable-http", "http", "websocket", "ws") or bool(service.get("url"))
    if is_remote:
        if not (service.get("url") or "").strip():
            missing.append("url")
    else:
        if not (service.get("command") or "").strip():
            missing.append("command")
    return missing


async def _discover_tools(service: dict) -> list[dict] | None:
    """经运行时 gateway 真列一次工具。失败返回 None。"""
    try:
        from agent.mcp_client import MCPManager, MCPServerConfig
        from mcp.configuration import configuration_contract  # noqa: F401  保留 import 以确认可达
        name = service["name"]
        # 走运行时 gateway（in-process），与 agent 调用同路径
        mgr = MCPManager()
        config = MCPServerConfig.from_dict({
            "name": name,
            "transport": service.get("transport"),
            "url": service.get("url"),
            "command": service.get("command"),
            "args": service.get("args") or [],
            "env": {},
            "headers": service.get("headers") or {},
        })
        tools = await mgr.discover_tools(config)
        return [
            {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
            for t in tools
        ]
    except Exception as e:
        logger.warning("discover_tools failed for {}: {}", service.get("name"), e)
        return None


def kick_off_install(service_id: int) -> None:
    """create_service 后拉起后台编排（不阻塞请求）。"""
    asyncio.create_task(run_install(service_id))


__all__: list[Any] = ["run_install", "kick_off_install"]
