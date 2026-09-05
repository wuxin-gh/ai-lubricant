import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.config import AgentConfig
from agent.llm_bridge import LLMBridge
from agent.tools import ToolContext, ToolRegistry
from agent.agent_main import GenericAgent


def test_constructor_lazy_llm_and_builds_tools_from_config(tmp_path):
    workspace = tmp_path / "workspace"
    temp = tmp_path / "temp"
    workspace.mkdir()
    temp.mkdir()
    config = AgentConfig(
        api_key="sk-test",
        model="test-model",
        workspace_root=str(workspace),
        allowed_roots=[str(workspace), str(temp)],
    )

    agent = GenericAgent(config)

    assert agent.llm is None
    assert isinstance(agent.tools, ToolRegistry)
    assert agent.tools.context.config is config
    assert agent.sub_agents == []


@pytest.mark.asyncio
async def test_run_task_delegates_into_agent_runner_loop(monkeypatch):
    config = AgentConfig(max_turns=9)
    llm = Mock()
    tools = Mock()
    tools.get_schema.return_value = [{"type": "function", "function": {"name": "lookup"}}]
    seen = {}

    async def fake_loop(client, system_prompt, user_input, handler, tools_schema, max_turns, verbose, on_event=None):
        seen.update(
            client=client,
            system_prompt=system_prompt,
            user_input=user_input,
            handler=handler,
            tools_schema=tools_schema,
            max_turns=max_turns,
            verbose=verbose,
        )
        yield {"turn": 1}
        yield {"result": "CURRENT_TASK_DONE", "data": "done"}

    import agent.agent_main as agent_main

    monkeypatch.setattr(agent_main, "agent_runner_loop", fake_loop)
    agent = GenericAgent(config, llm=llm, tools=tools)

    outputs = await agent.run_task("do work", system_prompt="system", max_turns=3)

    assert outputs == [{"turn": 1}, {"result": "CURRENT_TASK_DONE", "data": "done"}]
    assert seen["client"] is llm
    assert seen["system_prompt"] == "system"
    assert seen["user_input"] == "do work"
    assert seen["handler"].tools_registry is tools
    assert seen["tools_schema"] == tools.get_schema.return_value
    assert seen["max_turns"] == 3
    assert seen["verbose"] is False


@pytest.mark.asyncio
async def test_spawn_subagent_uses_llm_fork_with_overrides(monkeypatch):
    config = AgentConfig(api_key="parent-key", model="parent-model")
    child_llm = SimpleNamespace(name="child")
    parent_llm = Mock()
    parent_llm.fork.return_value = child_llm
    tools = Mock()
    tools.get_schema.return_value = []
    created = []

    async def fake_run_task(self, prompt):
        return [{"result": "CURRENT_TASK_DONE", "data": prompt}]

    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)

    real_create_task = __import__("asyncio").create_task

    def tracking_create_task(coro):
        task = real_create_task(coro)
        created.append(task)
        return task

    import agent.agent_main as agent_main

    monkeypatch.setattr(agent_main.asyncio, "create_task", tracking_create_task)
    agent = GenericAgent(config, llm=parent_llm, tools=tools)

    handle = agent.spawn_subagent("child task", model="child-model", api_key="child-key")

    parent_llm.fork.assert_called_once_with(model="child-model")
    assert handle.task == "child task"
    assert handle.llm is child_llm
    assert handle.asyncio_task is created[0]
    assert await handle.asyncio_task == [{"result": "CURRENT_TASK_DONE", "data": "child task"}]


@pytest.mark.asyncio
async def test_spawn_subagent_stores_created_handles(monkeypatch):
    parent_llm = Mock()
    parent_llm.fork.side_effect = [SimpleNamespace(name="child-1"), SimpleNamespace(name="child-2")]
    tools = Mock()
    tools.get_schema.return_value = []

    async def fake_run_task(self, prompt):
        return [{"data": prompt}]

    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)
    agent = GenericAgent(AgentConfig(), llm=parent_llm, tools=tools)

    first = agent.spawn_subagent("first")
    second = agent.spawn_subagent("second")

    assert agent.sub_agents == [first, second]
    assert first.task == "first"
    assert second.task == "second"
    await first.asyncio_task
    await second.asyncio_task


def test_constructor_with_config_and_tools_defers_llm(tmp_path):
    """LLM 延迟解析（需 DB 读 agent_llm_configs），构造时不立即创建。
    send_message 路径通过 await agent._ensure_llm() 解析，因此 agent.llm 在构造后可为 None。"""
    workspace = tmp_path / "workspace"
    temp = tmp_path / "temp"
    workspace.mkdir()
    temp.mkdir()
    config = AgentConfig(
        api_key="sk-test",
        model="test-model",
        workspace_root=str(workspace),
        allowed_roots=[str(workspace), str(temp)],
    )
    tools = ToolRegistry(ToolContext(config))

    agent = GenericAgent(config, tools=tools)

    # tools passed in is reused; llm deferred (lazy) until _ensure_llm() resolves a config row
    assert agent.tools is tools
    assert agent.llm is None


@pytest.mark.asyncio
async def test_ensure_llm_builds_gateway_bridge_from_main_key(monkeypatch):
    """主 agent 用绑定的网关 api_key_id + main_model 构造 GatewayLLMBridge。"""
    from agent.llm_bridge import GatewayLLMBridge

    agent = GenericAgent(agent_id=42)
    # 让 _ensure_config 直接给出网关绑定，跳过 DB。
    agent.config = AgentConfig(main_api_key_id=3, main_model="gw-model")
    agent._agent_owner = "user-1"

    import agent.api as agent_api
    monkeypatch.setattr(
        agent_api,
        "_resolve_caller_api_key",
        AsyncMock(return_value={"id": 3, "key": "sk-gw", "name": "grp-key"}),
    )
    import config as config_mod
    monkeypatch.setattr(
        config_mod.Config,
        "get_api_key_provider_filter",
        AsyncMock(return_value=(set(), set())),
    )

    llm = await agent._ensure_llm()
    assert isinstance(llm, GatewayLLMBridge)
    assert llm.model == "gw-model"
    assert llm.api_key == "sk-gw"
    assert llm.api_key_name == "grp-key"


@pytest.mark.asyncio
async def test_ensure_llm_without_key_raises(monkeypatch):
    """未绑定网关 key → Agent 使用失败（RuntimeError）。"""
    agent = GenericAgent(agent_id=42)
    agent.config = AgentConfig(main_api_key_id=None, main_model="")
    agent._agent_owner = "user-1"

    with pytest.raises(RuntimeError):
        await agent._ensure_llm()


@pytest.mark.asyncio
async def test_run_task_falls_back_to_agent_system_prompt(monkeypatch):
    """未传 system_prompt → 回落到 Agent 行上的人设。

    定时任务/自愈诊断这类无人值守入口没有会话可读，历史上不传就跑成空系统提示，
    Agent 人设整段失效。
    """
    llm = Mock()
    tools = Mock()
    tools.get_schema.return_value = []
    seen = {}

    async def fake_loop(client, system_prompt, user_input, handler, tools_schema, max_turns, verbose, on_event=None):
        seen["system_prompt"] = system_prompt
        yield {"result": "CURRENT_TASK_DONE", "data": "ok"}

    import agent.agent_main as agent_main

    monkeypatch.setattr(agent_main, "agent_runner_loop", fake_loop)
    agent = GenericAgent(AgentConfig(), llm=llm, tools=tools)
    agent._agent_system_prompt = "你是巡检助手"

    await agent.run_task("do work")
    assert seen["system_prompt"] == "你是巡检助手"


@pytest.mark.asyncio
async def test_run_task_explicit_system_prompt_wins(monkeypatch):
    """显式传入的 system_prompt 优先于 Agent 人设兜底。"""
    llm = Mock()
    tools = Mock()
    tools.get_schema.return_value = []
    seen = {}

    async def fake_loop(client, system_prompt, user_input, handler, tools_schema, max_turns, verbose, on_event=None):
        seen["system_prompt"] = system_prompt
        yield {"result": "CURRENT_TASK_DONE", "data": "ok"}

    import agent.agent_main as agent_main

    monkeypatch.setattr(agent_main, "agent_runner_loop", fake_loop)
    agent = GenericAgent(AgentConfig(), llm=llm, tools=tools)
    agent._agent_system_prompt = "Agent 人设"

    await agent.run_task("do work", system_prompt="会话级提示")
    assert seen["system_prompt"] == "会话级提示"


@pytest.mark.asyncio
async def test_run_task_uses_caller_supplied_llm(monkeypatch):
    """调用方传入的 llm 只作用于本次运行，不写回 self._llm。"""
    tools = Mock()
    tools.get_schema.return_value = []
    main_llm = Mock()
    scheduled_llm = Mock()
    seen = {}

    async def fake_loop(client, system_prompt, user_input, handler, tools_schema, max_turns, verbose, on_event=None):
        seen["client"] = client
        yield {"result": "CURRENT_TASK_DONE", "data": "ok"}

    import agent.agent_main as agent_main

    monkeypatch.setattr(agent_main, "agent_runner_loop", fake_loop)
    agent = GenericAgent(AgentConfig(), llm=main_llm, tools=tools)

    await agent.run_task("do work", llm=scheduled_llm)
    assert seen["client"] is scheduled_llm
    # 本次覆盖不污染 Agent 自己的桥。
    assert agent._llm is main_llm


@pytest.mark.asyncio
async def test_resolve_scheduled_llm_precedence(monkeypatch):
    """定时模型解析：任务级 > Agent scheduled_* > 主 Agent，且 key/模型各自回退。"""
    config = AgentConfig(
        main_api_key_id=1, main_model="main-model",
        scheduled_api_key_id=2, scheduled_model="sched-model",
    )
    agent = GenericAgent(config, agent_id=42)
    calls = []

    async def fake_resolve(self, api_key_id, model, *, role):
        calls.append((api_key_id, model, role))
        return SimpleNamespace(api_key_id=api_key_id, model=model)

    monkeypatch.setattr(GenericAgent, "_resolve_gateway_bridge", fake_resolve)

    # 任务级全给 → 用任务级。
    await agent.resolve_scheduled_llm(api_key_id=9, model="task-model")
    assert calls[-1] == (9, "task-model", "scheduled")

    # 任务级只给模型 → 复用 Agent 的定时 key。
    await agent.resolve_scheduled_llm(model="task-model")
    assert calls[-1] == (2, "task-model", "scheduled")

    # 任务级全空 → 用 Agent 的 scheduled_*。
    await agent.resolve_scheduled_llm()
    assert calls[-1] == (2, "sched-model", "scheduled")

    # Agent 也没配定时绑定 → 回退主 Agent。
    agent.config = AgentConfig(main_api_key_id=1, main_model="main-model")
    await agent.resolve_scheduled_llm()
    assert calls[-1] == (1, "main-model", "scheduled")


@pytest.mark.asyncio
async def test_spawn_subagent_async_forks_main_when_no_subagent_key(monkeypatch):
    """子 agent 未绑定专用 key → fork 主桥（复用同一网关 key，仅可换模型）。"""
    config = AgentConfig(main_api_key_id=3, main_model="main-model")
    parent_llm = Mock()
    forked = SimpleNamespace(name="forked-child")
    parent_llm.fork.return_value = forked
    tools = Mock()
    tools.get_schema.return_value = []
    agent = GenericAgent(config, llm=parent_llm, tools=tools, agent_id=42)
    agent._agent_owner = "user-1"

    async def fake_run_task(self, prompt):
        return [{"result": "CURRENT_TASK_DONE", "data": prompt}]
    monkeypatch.setattr(GenericAgent, "run_task", fake_run_task)

    handle = await agent.spawn_subagent_async("sub task")
    assert handle.llm is forked
    parent_llm.fork.assert_called_once()
    await handle.asyncio_task



# ── system prompt assembly: scene block placement & de-duplication ────
#
# The assembled prompt must read as one section for the scene the conversation
# was opened in: scene intro (prepended by the caller) → this scene's services →
# their parameter detail, and only then the general constitution. Nothing here
# tests wording; it tests the ordering and de-duplication that regressed before.

def _resources_agent(monkeypatch, scene=None, capability_index=None, sop_bodies=None):
    """A GenericAgent whose _ensure_resources runs without DB or real files."""
    from agent import agent_main as am

    agent = GenericAgent(AgentConfig(), agent_id=7, tools=Mock())
    agent._capability_index = capability_index or []
    agent._scene = scene

    async def fake_ensure_tools(self):
        return self._tools
    monkeypatch.setattr(GenericAgent, "_ensure_tools", fake_ensure_tools)
    bodies = sop_bodies or {}
    monkeypatch.setattr(GenericAgent, "_read_sop_body", lambda self, ref: bodies.get(ref, ""))
    monkeypatch.setattr(am, "logger", Mock())

    import agent.file_memory as fm
    monkeypatch.setattr(fm, "ensure_agent_memory", lambda _id: None)
    monkeypatch.setattr(fm, "read_l1", lambda _id: "L1-INDEX-CONTENT")
    import agent.sop_service as ss
    monkeypatch.setattr(ss, "sync_agent_sops", AsyncMock(return_value=None))
    import agent.self_improvement_log as sil
    monkeypatch.setattr(sil, "system_prompt_segment", lambda _id: "")
    return agent


_MARKET_INDEX = [
    {
        "service": "marketplace-status",
        "methods": [{"name": "marketplace_get_status", "description": "Read marketplace availability."}],
        "sop_ref": "memory/sop/mcp/marketplace-status_sop.md",
    },
    {
        "service": "cdp-bridge",
        "methods": [{"name": "browser_scan", "description": "Scan the tab."}],
        "sop_ref": "memory/sop/mcp/cdp-bridge_sop.md",
    },
]
_MARKET_BODY = {"memory/sop/mcp/marketplace-status_sop.md": "# marketplace SOP\n- full schema here"}


@pytest.mark.asyncio
async def test_scene_tools_precede_constitution(monkeypatch):
    """Scene services + their detail come before [Role], not 100 lines after it."""
    scene = SimpleNamespace(services=("marketplace-status",))
    agent = _resources_agent(monkeypatch, scene=scene, capability_index=_MARKET_INDEX, sop_bodies=_MARKET_BODY)

    _tools, prefix = await agent._ensure_resources()

    assert prefix.startswith("[Scene Tools]")
    order = [prefix.index(s) for s in ("[Scene Tools]", "[Scene Capability: marketplace-status]", "[Role]")]
    assert order == sorted(order), "scene intro → scene tools → detail → constitution"
    assert "full schema here" in prefix, "scene SOP body is inlined, not pointed at"


@pytest.mark.asyncio
async def test_scene_service_not_listed_twice(monkeypatch):
    """A service whose full body is inlined is dropped from the general index."""
    scene = SimpleNamespace(services=("marketplace-status",))
    agent = _resources_agent(monkeypatch, scene=scene, capability_index=_MARKET_INDEX, sop_bodies=_MARKET_BODY)

    _tools, prefix = await agent._ensure_resources()

    cap_section = prefix.split("[Available Capabilities]")[-1]
    assert "marketplace-status" not in cap_section
    # Non-scene services stay advertised, as pointers.
    assert "cdp-bridge" in cap_section
    assert "memory/sop/mcp/cdp-bridge_sop.md" in cap_section


@pytest.mark.asyncio
async def test_capability_heading_name_is_stable(monkeypatch):
    """The constitution / capability_call errors / SOPs all cite this exact name."""
    scene = SimpleNamespace(services=("marketplace-status",))
    agent = _resources_agent(monkeypatch, scene=scene, capability_index=_MARKET_INDEX, sop_bodies=_MARKET_BODY)

    _tools, prefix = await agent._ensure_resources()

    assert "[Available Capabilities]" in prefix
    assert "- scheduler: create|list|cancel|run_now" in prefix


@pytest.mark.asyncio
async def test_no_scene_keeps_constitution_first(monkeypatch):
    """Without a scene there is no scene block and no de-duplication note."""
    agent = _resources_agent(monkeypatch, scene=None, capability_index=_MARKET_INDEX, sop_bodies=_MARKET_BODY)

    _tools, prefix = await agent._ensure_resources()

    assert prefix.startswith("[Role]")
    assert "[Scene Tools]" not in prefix
    cap_section = prefix.split("[Available Capabilities]")[-1]
    # Every service is advertised, each with its pointer.
    assert "marketplace-status" in cap_section
    assert "cdp-bridge" in cap_section


@pytest.mark.asyncio
async def test_missing_scene_sop_falls_back_to_pointer(monkeypatch):
    """No readable SOP body → the service stays in the index with its pointer."""
    scene = SimpleNamespace(services=("marketplace-status",))
    agent = _resources_agent(monkeypatch, scene=scene, capability_index=_MARKET_INDEX, sop_bodies={})

    _tools, prefix = await agent._ensure_resources()

    assert "[Scene Tools]" not in prefix
    cap_section = prefix.split("[Available Capabilities]")[-1]
    assert "memory/sop/mcp/marketplace-status_sop.md" in cap_section


def test_method_description_is_cut_at_a_word_boundary():
    """A mid-word cut ("plus th") reads as corrupted text, not an abbreviation."""
    from agent.agent_main import _short_desc

    long_desc = "Return optimized HTML or text of the active browser tab plus the console output"
    out = _short_desc(long_desc)
    assert out.endswith("…")
    assert not out.rstrip("…").endswith(" ")
    # The elided text stops at a real word, so the last token is a whole word.
    assert long_desc.startswith(out.rstrip("…"))
    # Short descriptions pass through untouched.
    assert _short_desc("Scan the tab.") == "Scan the tab."
    assert _short_desc(None) == ""
