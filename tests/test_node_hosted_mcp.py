"""形态 C（节点托管 stdio MCP -> 代理成 remote）的纯函数与状态门单测。

不依赖 DB / 节点 / 网络。锁三件事：
  1. 启动命令的 shell 转义——命令与 args 来自市场 manifest（外部数据），拼进 shell
     字符串前必须转义，否则一个带分号的 args 就能在节点上执行任意命令。
  2. 就绪门对 node_hosted 的额外判定：节点掉线后 install_state 可能还停在 ready，
     但隧道已不通，host_status 才是真相源。
  3. 只有 stdio 才需要节点托管（remote 直接服务端接入）。
"""
from __future__ import annotations

import pytest

from mcp_runtime import node_hosted as nh


def test_launch_command_wraps_stdio_in_mcp_proxy():
    svc = {"command": "npx", "args": ["-y", "@playwright/mcp"], "transport": "stdio"}
    cmd = nh.build_launch_command(svc, 47001)
    # 包装器把 stdio 转成本机端口上的 SSE
    assert "mcp-proxy" in cmd
    assert "--port 47001" in cmd
    # 原始 stdio 命令在 -- 之后
    assert cmd.split(" -- ", 1)[1].startswith("npx")


def test_launch_command_escapes_shell_metacharacters():
    """manifest 里的恶意 args 必须被引号包住，不能变成第二条命令。"""
    svc = {"command": "sh", "args": ["-c; rm -rf /", "$(whoami)", "a && b"]}
    cmd = nh.build_launch_command(svc, 47002)
    # 分号/命令替换/&& 都不能以可执行形态出现在引号之外
    payload = cmd.split(" -- ", 1)[1]
    assert "'-c; rm -rf /'" in payload
    assert "'$(whoami)'" in payload
    assert "'a && b'" in payload


def test_launch_command_requires_command():
    with pytest.raises(nh.NodeHostedError):
        nh.build_launch_command({"command": "  ", "args": []}, 47003)


def test_launch_command_handles_json_string_args():
    """args 在 DB 里是 JSONB，取出来可能已是 list，也可能是 JSON 字符串。"""
    svc = {"command": "npx", "args": '["-y", "pkg"]'}
    cmd = nh.build_launch_command(svc, 47004)
    assert cmd.endswith("npx -y pkg")


def test_host_sse_url_is_node_local():
    """隧道请求由节点发出，所以目标是节点的 127.0.0.1，不是服务端地址。"""
    url = nh.host_sse_url(47005)
    assert url == "http://127.0.0.1:47005/sse"


def test_exec_error_detects_nonzero_exit():
    assert nh._exec_error({"exitCode": 1, "stderr": "npx: not found"}) is not None
    assert nh._exec_error({"error": "node offline"}) is not None
    assert nh._exec_error({"exitCode": 0, "stdout": "12345"}) is None


def test_parse_pid_reads_echoed_pid():
    assert nh._parse_pid({"stdout": "12345\n"}) == 12345
    # 包装器可能先打了别的行，取最后一个纯数字行
    assert nh._parse_pid({"stdout": "starting...\n999\n"}) == 999
    # 取不到 pid 不该崩，返回 0（回收退化为按端口）
    assert nh._parse_pid({"stdout": "no pid here"}) == 0


@pytest.mark.asyncio
async def test_ready_gate_blocks_node_hosted_with_dead_host(monkeypatch):
    """节点掉线的 node_hosted 服务不能出现在 agent 的有效集里。"""
    import agent.mcp_client as mc

    services = [
        {
            "id": 1, "name": "hosted-dead", "enabled": True, "builtin": False,
            "kind": "stdio", "deploy_scope": "node_hosted",
            "install_state": "ready", "host_status": "dead",
        },
        {
            "id": 2, "name": "hosted-live", "enabled": True, "builtin": False,
            "kind": "stdio", "deploy_scope": "node_hosted",
            "install_state": "ready", "host_status": "running",
        },
    ]

    async def fake_list_services():
        return services

    import mcp_plugin_store
    monkeypatch.setattr(mcp_plugin_store, "list_services", fake_list_services)

    effective = await mc.resolve_effective_services(
        [{"service_id": 1, "enabled": True}, {"service_id": 2, "enabled": True}]
    )
    names = {s["name"] for s in effective}
    assert "hosted-dead" not in names, "掉线节点上的托管 MCP 不该给 agent"
    assert "hosted-live" in names
