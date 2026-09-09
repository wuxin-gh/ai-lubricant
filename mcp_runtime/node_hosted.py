"""形态 C：节点托管 stdio MCP，经节点隧道代理成 remote（Phase 2）。

为什么需要这一形态
------------------
stdio MCP 的价值在于操作**本地环境**，所以它天然属于会话（形态 B：编辑器 CLI 自己在
节点上拉起）。但有些 stdio MCP 我们希望 **agent 和所有编辑器共用一份**——那就必须有
一个常驻进程和一个可连的地址。这就是形态 C：

  节点上常驻 stdio 进程 --(mcp-proxy 包装成 SSE)--> 节点本机端口
      ^                                                  |
      | HostExec 装/起                                    | NodeProxyRequest 隧道
      |                                                  v
  服务端 ---------------- /mcp/node-hosted/{id}/sse -----> agent + 编辑器

对 agent 和编辑器而言它就是一个普通 remote MCP，因此**完全复用形态 A 的安装状态机**
（configuring->starting->testing->ready），只是 starting 那步做的是「节点上起进程」。

复用而非新建
------------
- 隧道：``NodeProxyRequest`` / ``NodeTunnelResponse`` + 节点侧 handleProxy 已完整存在，
  不新增 proto 帧。走 ``NodeConnectManager.request()``。
- 装/起进程：``HostExec``（node_client.host_exec）已存在。
- 长连接：SSE 会长时间空闲，原本会被隧道两端的超时切断。已放宽——节点侧
  ``isStreamingResponse`` 撤掉硬超时，服务端侧 ``STREAM_RESPONSE_TIMEOUT``
  给流式响应更宽的空闲上限。

包装器
------
stdio 本身是 stdin/stdout JSON-RPC，不是 HTTP，需要一层包装把它转成 SSE。这里用现成的
``npx -y mcp-proxy``（唯一新增外部依赖），因此节点需要有 node/npx。包装器缺失时
starting 步骤会失败并把 stderr 写进 install_error，不静默降级。
"""
from __future__ import annotations

import shlex
from typing import Any

import mcp_plugin_store
from loguru import logger

# 托管进程的 SSE 端点路径（mcp-proxy 的默认形状）
HOST_SSE_PATH = "/sse"

# HostExec 的输出上限：我们只需要启动回执，不需要进程的全部输出
_EXEC_OUTPUT_LIMIT = 200_000


class NodeHostedError(RuntimeError):
    """节点托管操作失败（节点离线、包装器缺失、端口冲突等）。"""


def _service_args(service: dict) -> list[str]:
    raw = service.get("args") or []
    if isinstance(raw, str):
        import json

        try:
            raw = json.loads(raw)
        except Exception:
            raw = [raw]
    return [str(a) for a in raw]


def build_launch_command(service: dict, port: int) -> str:
    """拼出在节点上常驻启动该 stdio MCP 的命令。

    形状：``mcp-proxy --port <port> -- <command> <args...>``
    包装器把 stdio 的 stdin/stdout JSON-RPC 转成本机 <port> 上的 SSE 端点。

    命令与参数一律经 shlex.quote：它们来自市场 manifest（外部数据），拼进 shell
    字符串前必须转义，否则一个带分号的 args 就能在节点上执行任意命令。
    """
    command = (service.get("command") or "").strip()
    if not command:
        raise NodeHostedError("该 MCP 没有 command，无法在节点上启动")
    parts = ["npx", "-y", "mcp-proxy", "--port", str(int(port)), "--"]
    parts.append(shlex.quote(command))
    parts += [shlex.quote(a) for a in _service_args(service)]
    return " ".join(parts)


def host_sse_url(port: int) -> str:
    """节点本机上包装器的 SSE 地址。隧道请求的 url 就是它（由节点发出，故是 127.0.0.1）。"""
    return f"http://127.0.0.1:{int(port)}{HOST_SSE_PATH}"


async def deploy_to_node(service_id: int, node_id: str) -> dict:
    """把一个 stdio MCP 部署到指定执行节点上常驻。

    做三件事：分配端口 -> HostExec 起进程 -> 记账为 node_hosted。之后由安装状态机
    的 testing 步骤经隧道 discover_tools 验证真的连得上。
    """
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise NodeHostedError(f"MCP service {service_id} not found")
    if (service.get("transport") or "").lower() != "stdio":
        raise NodeHostedError("只有 stdio 形态才需要节点托管；remote 直接服务端接入即可")

    port = await mcp_plugin_store.allocate_host_port(node_id)
    command = build_launch_command(service, port)

    from user_platform.node_client.client import get_local_node_client

    client = get_local_node_client()
    # nohup + & ：HostExec 是一次性调用，返回后进程要继续活着
    daemonized = f"nohup {command} > /tmp/mcp-hosted-{service_id}.log 2>&1 & echo $!"
    try:
        result = await client.host_exec(
            node_id, daemonized, max_output_bytes=_EXEC_OUTPUT_LIMIT
        )
    except Exception as exc:  # noqa: BLE001
        raise NodeHostedError(f"节点 {node_id} 执行启动命令失败: {exc}") from exc

    if error := _exec_error(result):
        raise NodeHostedError(f"节点上启动托管进程失败: {error}")

    pid = _parse_pid(result)
    await mcp_plugin_store.mark_host_state(
        service_id, node_id=node_id, port=port, pid=pid or 0, status="starting"
    )
    # deploy_scope 转为 node_hosted，从此走形态 A 的安装状态机（starting->testing->ready）
    await _set_deploy_scope(service_id, "node_hosted")
    logger.info(
        "[node-hosted] service {} deployed to node {} port {} pid {}",
        service_id, node_id, port, pid,
    )
    return {"service_id": service_id, "node_id": node_id, "port": port, "pid": pid}


async def stop_on_node(service_id: int) -> bool:
    """杀掉节点上的托管进程（删除服务 / 重新部署前调用）。"""
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        return False
    node_id = (service.get("host_node_id") or "").strip()
    pid = int(service.get("host_pid") or 0)
    if not node_id or not pid:
        return False
    from user_platform.node_client.client import get_local_node_client

    try:
        await get_local_node_client().host_exec(node_id, f"kill {pid}")
    except Exception as exc:  # noqa: BLE001 - 进程可能已经不在，不该阻断删除
        logger.warning("[node-hosted] kill pid {} on {} failed: {}", pid, node_id, exc)
        return False
    await mcp_plugin_store.mark_host_state(service_id, status="dead")
    return True


async def proxy_request(
    service: dict,
    *,
    method: str = "GET",
    path: str = HOST_SSE_PATH,
    headers: dict | None = None,
    body: bytes = b"",
):
    """经节点隧道向托管进程发一个 HTTP 请求，返回流式响应。

    这是 agent / SSE 端点访问托管 MCP 的唯一通道：服务端自己**连不到**节点内网端口，
    必须由节点代发（NodeProxyRequest）。
    """
    node_id = (service.get("host_node_id") or "").strip()
    port = int(service.get("host_port") or 0)
    if not node_id or not port:
        raise NodeHostedError("该服务没有托管节点/端口记录")

    manager = _get_connect_manager()
    url = f"http://127.0.0.1:{port}{path if path.startswith('/') else '/' + path}"
    try:
        return await manager.request(node_id, method=method, url=url, headers=headers, body=body)
    except ConnectionError as exc:
        # 节点掉线 => 该 MCP 不可用。打回 error，agent 侧被就绪门挡住。
        await mcp_plugin_store.mark_host_state(int(service["id"]), status="dead")
        await mcp_plugin_store.mark_install_state(
            int(service["id"]), "error", step="节点离线", error=str(exc)[:4000]
        )
        raise NodeHostedError(f"托管节点 {node_id} 不可用: {exc}") from exc


def _get_connect_manager():
    """取已注入的 node 代理管理器。

    复用 providers.proxy_manager 里那个运行时注入的实例，而不是自己去拿 registry：
    它在 control 角色下是进程内的 NodeConnectManager，在 data 角色下是
    RemoteNodeConnectManager（经 /internal/node-proxy 转到 control）。两者接口同构，
    所以这里对部署角色无感——自己摸 registry 会在 data 角色下直接不可用。
    """
    from providers.proxy_manager import get_proxy_manager

    manager = getattr(get_proxy_manager(), "_node_manager", None)
    if manager is None:
        raise NodeHostedError("节点代理未就绪（node manager 未注入），无法访问托管 MCP")
    return manager


async def reconcile_node(node_id: str, *, online: bool) -> None:
    """节点上下线时对账其上的托管服务。

    掉线：host_status=dead + install_state=error（agent 侧被就绪门挡住，不会拿到
    一个连不上的 MCP 而在 call_tool 处炸）。上线：不自动重拉——进程可能还活着，
    交由 testing 步骤验证，避免重复起进程占端口。
    """
    services = await mcp_plugin_store.list_node_hosted(node_id)
    for svc in services:
        sid = int(svc["id"])
        if not online:
            await mcp_plugin_store.mark_host_state(sid, status="dead")
            await mcp_plugin_store.mark_install_state(
                sid, "error", step="托管节点离线", error=f"node {node_id} offline"
            )
        else:
            await mcp_plugin_store.mark_host_state(sid, status="starting")


async def _set_deploy_scope(service_id: int, scope: str) -> None:
    from db import PostgresClient

    if not PostgresClient.pool:
        return
    async with PostgresClient.pool.acquire() as conn:
        await conn.execute(
            "UPDATE mcp_services SET deploy_scope=$2, updated_at=now() WHERE id=$1",
            service_id, scope,
        )


def _exec_error(result: dict[str, Any]) -> str | None:
    """从 HostExec 结果里取错误。对齐 routes_nodes_files._host_exec_error 的判定。"""
    if not isinstance(result, dict):
        return "节点返回了非预期的结果"
    if err := (result.get("error") or "").strip():
        return err
    exit_code = result.get("exitCode", result.get("exit_code"))
    if exit_code not in (None, 0, "0"):
        stderr = (result.get("stderr") or result.get("stdout") or "").strip()
        return f"exit={exit_code} {stderr[:500]}"
    return None


def _parse_pid(result: dict[str, Any]) -> int:
    """从 `echo $!` 的输出里取 pid。取不到返回 0（不阻断部署，只是回收要靠端口）。"""
    out = (result.get("stdout") or "").strip().splitlines()
    for line in reversed(out):
        line = line.strip()
        if line.isdigit():
            return int(line)
    return 0


__all__ = [
    "NodeHostedError",
    "build_launch_command",
    "host_sse_url",
    "deploy_to_node",
    "stop_on_node",
    "proxy_request",
    "reconcile_node",
]
