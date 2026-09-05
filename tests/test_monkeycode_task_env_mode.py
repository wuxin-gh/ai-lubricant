"""env_mode/env_id 下发字段：_build_session 按 task.config_snapshot 填档位。

环境档位（system/shared/isolated）决定节点上 session.home 指向哪。这里只验服务端
组装的下发包是否带上正确字段 + 拒非法值；节点侧重定向 home 的行为由 Go 侧覆盖。

system 档另有派发前校验（_require_system_env_allowed）：节点在线且已上报能力但未
开启 → node_system_env_disabled；离线/能力未知不拦（节点端 resolveHome 兜底）。
"""
from __future__ import annotations

import uuid

import pytest

from monkeycode_compat import task_service as task_service_module
from monkeycode_compat.models_task import Task
from monkeycode_compat.task_service import TaskService


def _task(env_mode: str | None, env_id: str | None) -> Task:
    cfg: dict = {}
    if env_mode:
        cfg["env_mode"] = env_mode
    if env_id:
        cfg["env_id"] = env_id
    return Task(id=uuid.uuid4(), config_snapshot=cfg)


def _live(online: bool = True, system_env: str | None = None) -> dict:
    """mirror nodes_service.node_live_info 的返回形状（normalize 后的 dict）。

    在线节点注册时至少会上报 os/arch，所以这里始终带一个基础标签：capabilities
    完全为空代表「控制面没给出能力」（降级），与「上报了但没开系统环境」是两种
    不同状态，守卫对它们的处理也不同。
    """
    caps: dict[str, str] = {"os": "linux"}
    if system_env is not None:
        caps["system_env"] = system_env
    return {"online": online, "capabilities": caps}


def test_build_session_isolated_omits_env_fields():
    """isolated/空档不下发 env_mode：节点默认就是 isolated，省字段更干净。"""
    s = TaskService()._build_session({"cli_name": "claude"}, _task(None, None))
    assert "envMode" not in s
    assert "envId" not in s


def test_build_session_shared_carries_env_id():
    s = TaskService()._build_session({"cli_name": "claude"}, _task("shared", "prod"))
    assert s["envMode"] == "shared"
    assert s["envId"] == "prod"


def test_build_session_system_carries_mode_only():
    s = TaskService()._build_session({"cli_name": "claude"}, _task("system", None))
    assert s["envMode"] == "system"
    assert "envId" not in s


def test_build_session_shared_without_env_id_is_rejected():
    """shared 必须带 env_id，否则派发到节点会被解析成空目录——在服务端就拦。"""
    with pytest.raises(ValueError, match="env_id"):
        TaskService()._build_session({"cli_name": "claude"}, _task("shared", None))


def test_build_session_rejects_unknown_mode():
    with pytest.raises(ValueError, match="unsupported env_mode"):
        TaskService()._build_session({"cli_name": "claude"}, _task("kubernetes", "x"))


def test_build_session_normalizes_case():
    s = TaskService()._build_session({"cli_name": "claude"}, _task("SHARED", "prod"))
    assert s["envMode"] == "shared"


# ── _require_system_env_allowed：system 档的派发前校验 ─────────────────────────


def _install_live(monkeypatch, live):
    async def node_live_info(node_id):
        return live

    monkeypatch.setattr(task_service_module.nodes_service, "node_live_info", node_live_info)


@pytest.mark.asyncio
async def test_system_env_guard_passes_when_node_reports_enabled(monkeypatch):
    _install_live(monkeypatch, _live(online=True, system_env="true"))
    await TaskService()._require_system_env_allowed("node-1", "system")


@pytest.mark.asyncio
async def test_system_env_guard_rejects_online_node_without_capability(monkeypatch):
    _install_live(monkeypatch, _live(online=True, system_env=None))
    with pytest.raises(ValueError, match="node_system_env_disabled"):
        await TaskService()._require_system_env_allowed("node-1", "system")


@pytest.mark.asyncio
async def test_system_env_guard_rejects_explicit_false_capability(monkeypatch):
    """能力位明确上报 false（docker 镜像节点）同样拒绝。"""
    _install_live(monkeypatch, _live(online=True, system_env="false"))
    with pytest.raises(ValueError, match="node_system_env_disabled"):
        await TaskService()._require_system_env_allowed("node-1", "system")


@pytest.mark.asyncio
async def test_system_env_guard_skips_offline_node(monkeypatch):
    """离线节点能力快照可能过期，不在此拦——由节点端拒绝并落 dispatch_error。"""
    _install_live(monkeypatch, _live(online=False, system_env=None))
    await TaskService()._require_system_env_allowed("node-1", "system")


@pytest.mark.asyncio
async def test_system_env_guard_skips_unknown_node(monkeypatch):
    """节点不在 live ledger（控制面离线/刚删除）时不误杀。"""
    _install_live(monkeypatch, None)
    await TaskService()._require_system_env_allowed("node-1", "system")


@pytest.mark.asyncio
async def test_system_env_guard_skips_empty_capabilities(monkeypatch):
    """在线但能力表为空（控制面降级快照）不判死，交节点端兜底。"""
    _install_live(monkeypatch, {"online": True, "capabilities": {}})
    await TaskService()._require_system_env_allowed("node-1", "system")


@pytest.mark.asyncio
async def test_system_env_guard_skips_non_system_modes(monkeypatch):
    """isolated/shared 不触发节点能力查询（shared 走共用环境逻辑）。"""
    _install_live(monkeypatch, _live(online=True, system_env=None))
    await TaskService()._require_system_env_allowed("node-1", None)
    await TaskService()._require_system_env_allowed("node-1", "isolated")
    await TaskService()._require_system_env_allowed("node-1", "shared")
