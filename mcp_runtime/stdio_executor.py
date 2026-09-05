"""stdio MCP 执行器转发边界（本期只定义协议边界与转发点，不含执行器实现）。

背景与隔离动机
---------------
stdio 类 MCP 是「下载到本地跑」的第三方进程：能读写文件系统、可能越权改代码
或读数据。合并进主程序后，「主服务挂 MCP 不挂」的进程隔离消失了——所以 stdio
不在主程序内 `create_subprocess_exec` 拉起，而是交给一个**独立部署的执行器进程**
运行，主程序只做转发。污染面被隔离在执行器边界内。

本模块是主程序侧的转发客户端：把 tools/list 与 tools/call 转发给执行器。执行器
本身（进程降权、cwd 限制、文件系统白名单、健康检查/崩溃恢复）是下一阶段的独立
程序，本期不实现。未配置执行器时，所有转发以 `ExecutorUnavailable` 干净降级，
绝不在主程序内落地拉起子进程。

转发协议（主程序 → 执行器，HTTP/JSON）
-------------------------------------
鉴权沿用「内部服务 token」思路：主程序持有一个共享密钥（executor token），每次
转发带 `X-MCP-Executor-Token` 头；执行器侧校验后才受理。密钥来源见
`_executor_settings()`（env 优先，config 兜底）。

- ``POST {base}/exec/tools/list``
    body:  ``{"spec": {name, command, args, env}}``
    resp:  ``{"tools": [{name, description, input_schema}, ...]}``
- ``POST {base}/exec/tools/call``
    body:  ``{"spec": {...}, "tool": str, "arguments": {...}}``
    resp:  ``{"content": [...]}`` 或 ``{"error": str}``

``spec`` 是一次 stdio 启动所需的全部信息（命令 + 参数 + 环境），执行器据此在其
隔离沙箱内拉起 stdio MCP 子进程并代理 JSON-RPC。主程序不解析、不落地这些命令。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


class ExecutorUnavailable(RuntimeError):
    """stdio 执行器未配置或不可达；调用方据此干净降级（不在主程序内拉起子进程）。"""


@dataclass
class StdioSpec:
    """一次 stdio MCP 启动所需的全部信息，转发给执行器由其在沙箱内拉起。"""

    name: str
    command: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "command": self.command,
            "args": list(self.args),
            "env": dict(self.env),
        }

    @classmethod
    def from_service(cls, service: dict, env: dict[str, str] | None = None) -> "StdioSpec":
        """从 DB service 行构造 spec（transport=stdio 才应走这里）。"""
        raw_args = service.get("args") or []
        if isinstance(raw_args, str):
            import json
            try:
                raw_args = json.loads(raw_args)
            except Exception:
                raw_args = [raw_args]
        return cls(
            name=service["name"],
            command=service.get("command") or "",
            args=list(raw_args),
            env=dict(env or service.get("env_template") or {}),
        )


def _executor_settings() -> tuple[str, str]:
    """返回 (base_url, token)。任一为空即视为「执行器未部署」。

    env 优先（MCP_STDIO_EXECUTOR_URL / MCP_STDIO_EXECUTOR_TOKEN），config 的
    ``mcp_runtime.stdio_executor.{url,token}`` 兜底。
    """
    base = os.environ.get("MCP_STDIO_EXECUTOR_URL", "").strip()
    token = os.environ.get("MCP_STDIO_EXECUTOR_TOKEN", "").strip()
    if base and token:
        return base.rstrip("/"), token
    try:
        import config
        main_config = config.CONFIG_STORE.read_main()
        section = (main_config.get("mcp_runtime") or {}).get("stdio_executor") or {}
        base = base or str(section.get("url") or "").strip()
        token = token or str(section.get("token") or "").strip()
    except Exception:
        pass
    return base.rstrip("/"), token


def is_configured() -> bool:
    """执行器是否已配置（url + token 齐全）。未配置时 stdio 分支应跳过/标错。"""
    base, token = _executor_settings()
    return bool(base and token)


async def _post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    base, token = _executor_settings()
    if not (base and token):
        raise ExecutorUnavailable(
            "stdio 执行器未配置（MCP_STDIO_EXECUTOR_URL / MCP_STDIO_EXECUTOR_TOKEN 缺失）"
        )
    import aiohttp
    from providers.base import make_insecure_connector

    url = f"{base}{path}"
    headers = {"X-MCP-Executor-Token": token, "Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(total=120, sock_connect=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=make_insecure_connector()) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 401 or resp.status == 403:
                    raise ExecutorUnavailable(f"stdio 执行器拒绝鉴权 status={resp.status}")
                if resp.status != 200:
                    body = await resp.text()
                    raise ExecutorUnavailable(
                        f"stdio 执行器返回 status={resp.status}: {body[:200]}"
                    )
                return await resp.json()
    except ExecutorUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 — 网络/解析失败一律归为不可达，绝不阻断主程序
        raise ExecutorUnavailable(f"stdio 执行器不可达: {exc}") from exc


async def list_tools(spec: StdioSpec) -> list[dict[str, Any]]:
    """经执行器发现 stdio MCP 的工具列表；未部署/不可达抛 ExecutorUnavailable。"""
    data = await _post("/exec/tools/list", {"spec": spec.to_wire()})
    tools = data.get("tools")
    return list(tools) if isinstance(tools, list) else []


async def call_tool(spec: StdioSpec, tool: str, arguments: dict[str, Any]) -> Any:
    """经执行器调用一个 stdio MCP 工具；未部署/不可达抛 ExecutorUnavailable。"""
    data = await _post(
        "/exec/tools/call",
        {"spec": spec.to_wire(), "tool": tool, "arguments": arguments or {}},
    )
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data.get("content", data)


__all__ = [
    "ExecutorUnavailable",
    "StdioSpec",
    "is_configured",
    "list_tools",
    "call_tool",
]
