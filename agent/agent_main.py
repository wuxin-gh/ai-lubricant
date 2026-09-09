"""GenericAgent — the top-level agent orchestrator.

Supports two modes:
1. Config-based: pass AgentConfig directly (legacy, for internal use)
2. DB-based: pass agent_id, config loaded from agents table (multi-tenant)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any

from agent.agent_loop import BaseHandler, agent_runner_loop
from agent.config import AgentConfig
from agent.llm_bridge import GatewayLLMBridge
from agent.tools import ToolContext, ToolRegistry

logger = logging.getLogger(__name__)

# Cap on an inlined scene SOP body. Scene services are few (usually one), but a
# service with many methods can produce a long schema dump; the pointer form is
# the fallback for anything larger.
_MAX_SCENE_SOP_CHARS = 6000

# Cap on the one-line method description in the capability index. Cutting at a
# raw character offset severs words mid-token ("plus th", "URLs, and ti"), which
# reads as corrupted text rather than an abbreviation, so trim at a word
# boundary and mark the elision.
_MAX_METHOD_DESC_CHARS = 60


def _short_desc(description: object) -> str:
    """First line of a method description, trimmed at a word boundary."""
    text = str(description or "").strip()
    if not text:
        return ""
    text = text.splitlines()[0].strip()
    if len(text) <= _MAX_METHOD_DESC_CHARS:
        return text
    head = text[:_MAX_METHOD_DESC_CHARS]
    cut = head.rfind(" ")
    # Keep the hard cut for scripts without spaces (Chinese descriptions).
    return (head[:cut] if cut > _MAX_METHOD_DESC_CHARS // 2 else head).rstrip(" ,;:.") + "…"


def _scene_cdp_session_key(scene) -> str:
    """Return ``client_id:tab_id`` from a resolved CDP scene, else empty.

    The tab_id in the scene identity is stable within a tab (survives full-page
    navigation), so it is the right key for a tab lease. Empty when there is no
    CDP scene (plain agent / node terminal / marketplace admin).
    """
    if scene is None:
        return ""
    identity = getattr(scene, "identity", None) or {}
    client_id = str(identity.get("client_id") or "").strip()
    tab_id = str(identity.get("tab_id") or "").strip()
    if not client_id or not tab_id:
        return ""
    return f"{client_id}:{tab_id}"


@dataclass(slots=True)
class SubagentHandle:
    task: str
    llm: GatewayLLMBridge
    asyncio_task: asyncio.Task


class _AgentHandler(BaseHandler):
    def __init__(self, tools_registry: ToolRegistry, max_turns: int) -> None:
        super().__init__(tools_registry=tools_registry, max_turns=max_turns)


class GenericAgent:
    def __init__(
        self,
        config: AgentConfig | None = None,
        agent_id: int | None = None,
        llm: GatewayLLMBridge | None = None,
        tools: ToolRegistry | None = None,
        run_as_caller: str | None = None,
        scene=None,
        model_override: str = "",
    ) -> None:
        self.agent_id = agent_id
        self._llm = llm
        self._tools = tools
        # 单次运行级模型覆盖（空=用 Agent 自己绑定的模型）。复用 Agent 绑定的网关
        # key 只换模型——与定时任务「同 key 换模型」同一口径；供批量识别这类
        # 「指定 agent + 指定模型」的调用方使用。
        self._model_override = model_override or ""
        # Resolved SceneSpec (agent/scene_context.py) or None. Drives which MCP
        # services get their full SOP inlined into the first system prompt
        # instead of an (SOP: ...) pointer the model must read first.
        self._scene = scene
        self.sub_agents: list[SubagentHandle] = []
        # 父级事件出口：子 agent spawn 时从这里取 on_event，把子 agent 的
        # content/tool 事件透传回主对话 SSE 流（详见 spawn_subagent_async）。
        self._event_sink = None
        # 子 agent 自增序号，仅用于事件聚合与展示，非持久化。
        self._subagent_seq = 0
        # MCP 工具仅需要附加一次；子 agent 复用同一 ToolRegistry 时保持幂等。
        self._mcp_attached = False
        self._mcp_manager = None
        # Capability index produced by _attach_mcp_tools: list of
        # {service, method_names, methods, browser, sop_ref}. Advertised in the
        # system prompt; MCP methods are invoked via capability_call, not as
        # first-class tools.
        self._capability_index: list[dict] = []
        # Dynamically-detected browser-class MCP service name (or None).
        self._browser_service: str | None = None
        # Agent 归属 user_id（_ensure_config 时从 DB 填充，仅供展示/日志）。
        self._agent_owner: str | None = None
        # Agent 自身的 system_prompt（_ensure_config 时从 DB 填充）。AgentConfig 不含
        # 该字段，历史上只有对话链路会从 agents 行读出来显式传给 run_task；无人值守
        # 入口（定时任务、自愈诊断）不传，于是那些场景跑的是空系统提示，Agent 人设
        # 整段失效。这里缓存一份供 run_task 兜底，让「不传」等于「用 Agent 自己的」。
        self._agent_system_prompt: str = ""
        # 运行身份：解析网关 api_key 时按「当前发起请求的用户」校验权限，而非 Agent
        # owner。这样平台 Agent（owner=NULL）对所有用户可见，但每个用户运行时用自己的
        # 身份校验绑定 key —— 无权访问该 key 就发送失败。None = 管理员上下文（全量查找）。
        self._run_as_caller: str | None = run_as_caller

        if config is not None:
            self.config = config
        elif agent_id is not None:
            self.config = None  # Lazy load from DB
        else:
            self.config = AgentConfig()

        if self.config is not None:
            # LLM 延迟解析：运行时按绑定的网关 API Key + 模型构造桥。
            if self._tools is None:
                self._tools = ToolRegistry(ToolContext(self.config), agent_id=self.agent_id)

    async def _ensure_config(self) -> AgentConfig:
        if self.config is not None:
            return self.config
        if self.agent_id is None:
            self.config = AgentConfig()
            return self.config

        from db import PostgresClient
        if not PostgresClient.pool:
            self.config = AgentConfig()
            return self.config

        async with PostgresClient.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agents WHERE id=$1", self.agent_id)
        if not row:
            logger.warning("Agent %d not found, using default config", self.agent_id)
            self.config = AgentConfig()
            return self.config

        d = dict(row)
        # 记录 owner，供网关 key 归属校验用（NULL=平台 Agent，走管理员全量查找分支）。
        self._agent_owner = d.get("user_id")
        # system_prompt 不是 AgentConfig 字段（它属于 Agent 行/会话行）。这里留一份，
        # 供 run_task 在调用方没传时兜底 —— 定时任务等无人值守入口没有会话可读。
        self._agent_system_prompt = (d.get("system_prompt") or "").strip()
        # GA-style real working directory: every Agent owns
        # ``data/agents/<agent_id>/`` with ``memory/`` and ``workspace/`` as real
        # subdirectories. Only honour an explicit non-legacy workspace_root from
        # the DB (future node binding); otherwise default to the per-Agent dir so
        # file_read reaches the Agent's real memory/ and workspace/ files.
        workspace_root = (d.get("workspace_root") or "").strip()
        legacy_default = workspace_root in {"", "agent/workspace"}
        if legacy_default:
            from agent.file_memory import agent_root
            workspace_root = str(agent_root(self.agent_id))
            allowed_roots = [workspace_root]
        else:
            allowed_roots = d.get("allowed_roots") or [workspace_root]
        self.config = AgentConfig.from_dict({
            "api_key": d.get("api_key", ""),
            "model": d.get("model", ""),
            "llm_config_id": d.get("llm_config_id"),
            # 网关秘钥绑定（新链路）：主/子 Agent 各自的网关 key + 模型名。
            "main_api_key_id": d.get("main_api_key_id"),
            "main_model": d.get("main_model", ""),
            "subagent_api_key_id": d.get("subagent_api_key_id"),
            "subagent_model": d.get("subagent_model", ""),
            # 定时任务默认绑定（无人值守场景专用；空则回退主 Agent）。
            "scheduled_api_key_id": d.get("scheduled_api_key_id"),
            "scheduled_model": d.get("scheduled_model", ""),
            "max_turns": d.get("max_turns", 80),
            "memory_enabled": d.get("memory_enabled", True),
            "skill_auto_learn": d.get("skill_auto_learn", True),
            "workspace_root": workspace_root,
            "allowed_roots": allowed_roots,
            "denied_patterns": d.get("denied_patterns", ["/etc/", "/var/", ".env"]),
            "thinking_enabled": d.get("thinking_enabled", False),
            "reasoning_effort": d.get("reasoning_effort", ""),
            "llm_retry_429": d.get("llm_retry_429", 2),
            "approval_timeout_seconds": d.get("approval_timeout_seconds", 24 * 60 * 60),
            "mcp_user_id": d.get("mcp_user_id"),
        })
        # API callers may provision a registry before lazy DB config is loaded
        # (node shell registration needs ``agent._tools`` early). Rebind that
        # registry to the resolved per-Agent context; otherwise file_read would
        # silently keep the legacy shared ``agent/workspace`` root.
        if self._tools is not None:
            self._tools.context = ToolContext(self.config)
        return self.config

    async def _ensure_llm(self) -> "GatewayLLMBridge":
        if self._llm is not None:
            return self._llm
        await self._ensure_config()
        self._llm = await self._resolve_gateway_bridge(
            self.config.main_api_key_id,
            self._model_override or self.config.main_model,
            role="main_agent",
        )
        return self._llm

    async def _resolve_gateway_bridge(
        self, api_key_id: int | None, model: str, *, role: str
    ) -> "GatewayLLMBridge":
        """按（网关 api_key_id + 模型名）构造走网关的 LLM 桥。

        - api_key 归属校验按「当前运行用户」（_run_as_caller），而非 Agent owner：
          平台 Agent（owner=NULL）对所有用户可见，但每个用户运行时用自己的身份校验
          绑定 key —— 无权访问该 key（不在自己名下、也不在所在分组授权内）就发送失败。
          _run_as_caller=None 表示管理员上下文，走全量查找。复用 agent.api 的解析器。
        - 无绑定 / 无权 / 已禁用 / 未选模型 → 抛 RuntimeError（Agent 使用失败），
          满足"没有网关 key 权限则不可用/使用失败"的诉求。
        - provider 白/黑名单由该 key 的配置决定（与聊天发送链路口径一致）。
        """
        from agent.llm_bridge import GatewayLLMBridge
        from agent.api import _resolve_caller_api_key
        import config as _config_mod
        from fastapi import HTTPException

        if not api_key_id:
            raise RuntimeError(
                f"Agent 未绑定网关 API Key（role={role}），"
                "请在 Agent 编辑器的「模型」中选择秘钥与模型"
            )
        if not model:
            raise RuntimeError(
                f"Agent 未选择 role={role} 的模型，请在「模型」中选择模型"
            )
        try:
            key_row = await _resolve_caller_api_key(api_key_id, self._run_as_caller)
        except HTTPException as exc:
            # 无权/不存在/已禁用 → 转成 agent 层可读错误（Agent 使用失败）。
            raise RuntimeError(f"Agent 绑定的网关 API Key 不可用：{exc.detail}") from exc
        api_key = key_row["key"]
        api_key_name = key_row.get("name") or ""
        cfg = self.config or AgentConfig()
        provider_whitelist, provider_blacklist = await _config_mod.Config.get_api_key_provider_filter(api_key)
        return GatewayLLMBridge(
            api_key=api_key,
            api_key_name=api_key_name,
            model=model,
            provider_whitelist=provider_whitelist,
            provider_blacklist=provider_blacklist,
            thinking_enabled=cfg.thinking_enabled,
            reasoning_effort=cfg.reasoning_effort,
            max_retries=max(0, int(cfg.llm_retry_429 or 0)),
        )

    async def resolve_scheduled_llm(
        self, api_key_id: int | None = None, model: str = "",
    ) -> "GatewayLLMBridge":
        """定时任务用的 LLM 桥。优先级：任务级 > Agent 的 scheduled_* > 主 Agent。

        每一层各字段独立回退，而不是整层二选一：只给模型（不给 key）时复用上一层的
        key 换模型，这是最常见的诉求（同一把 key 下换个便宜模型跑定时）。

        与 _resolve_subagent_llm 分开：子 agent 是「主 agent 派生的并行子任务」，
        定时任务是「无人值守的独立触发」，两者该配的模型往往不同，混用一个字段会
        让其中一方被迫将就。
        """
        config = self.config or await self._ensure_config()
        # 任务级 > Agent 级；key 与模型各自回退，允许只覆盖其中一个。
        eff_key = api_key_id or config.scheduled_api_key_id or config.main_api_key_id
        eff_model = (model or "").strip() or config.scheduled_model or config.main_model
        return await self._resolve_gateway_bridge(eff_key, eff_model, role="scheduled")

    async def _resolve_subagent_llm(self) -> "GatewayLLMBridge | None":
        """子 agent 网关桥解析；未绑定专用 key/模型则返回 None，由调用方 fallback 到主桥。"""
        config = self.config or await self._ensure_config()
        if not config.subagent_api_key_id and not config.subagent_model:
            return None
        # 子 Agent 有独立 key → 用它；仅有独立模型 → 复用主 key 换模型（在 spawn 侧 fork 处理）。
        if config.subagent_api_key_id:
            return await self._resolve_gateway_bridge(
                config.subagent_api_key_id,
                config.subagent_model or config.main_model,
                role="subagent",
            )
        return None

    async def _ensure_tools(self) -> ToolRegistry:
        if self._tools is None:
            config = await self._ensure_config()
            self._tools = ToolRegistry(ToolContext(config), agent_id=self.agent_id)
        # MCP 工具挂载与 ToolRegistry 是否预置无关：api.py 会预塞内置 registry，
        # 这里仍需把 agent 的有效 MCP 服务注册进去。挂一次即可（幂等标记）。
        if not self._mcp_attached:
            await self._attach_mcp_tools(self._tools)
            self._mcp_attached = True
        return self._tools

    async def _ensure_resources(self) -> tuple[ToolRegistry, str]:
        """Prepare tools plus GA's always-on memory context.

        GenericAgent L1/L2/L3 are Agent-private files under data/agents/<id>/.
        MCP services and the scheduler are advertised in the system prompt via a
        short capability index — they are NOT first-class tools. SOP bodies are
        read on-demand with file_read("memory/sop/...").
        """
        tools = await self._ensure_tools()
        if self.agent_id is None:
            return tools, ""
        try:
            from agent.file_memory import ensure_agent_memory, read_l1
            from agent.meta_memory import META_MEMORY_SUMMARY

            ensure_agent_memory(self.agent_id)
            try:
                from agent.sop_service import sync_agent_sops
                await sync_agent_sops(self.agent_id)
            except Exception as exc:  # noqa: BLE001 - database-backed selection is optional during startup
                logger.debug("[sop] binding sync unavailable agent=%s: %s", self.agent_id, exc)
            index = read_l1(self.agent_id).strip()
            prefix = META_MEMORY_SUMMARY
            if index:
                prefix = f"{prefix}\n\n[L1 Memory Index]\n{index}"
            # Self-improvement log (GA ch12.3.3): always-on cross-task lessons.
            try:
                from agent.self_improvement_log import system_prompt_segment
                segment = system_prompt_segment(self.agent_id)
                if segment:
                    prefix = f"{prefix}\n\n{segment}"
            except Exception as exc:  # noqa: BLE001 — best-effort injection
                logger.debug("[memory] self-improvement log unavailable agent=%s: %s", self.agent_id, exc)
            # Capability index produced by _attach_mcp_tools, split into the scene's
            # own services and everything else the Agent happens to have mounted.
            #
            # The scene's services are what this conversation exists to drive, so
            # their full method schema is inlined and placed FIRST, immediately
            # after the scene intro the caller already put at the top of the
            # prompt: intro → this scene's services → their parameter detail reads
            # as one section. Listing them again in the general index below would
            # describe the same methods twice (once truncated), so a service whose
            # body is inlined is dropped from that index.
            #
            # Everything else is a pointer-only index: present so the model knows
            # the capability exists, not detailed, because this scene has no
            # particular reason to reach for it.
            capability_index = self._capability_index or []
            scene_services = set(getattr(self._scene, "services", ()) or ())
            scene_lines: list[str] = []
            scene_blocks: list[str] = []
            other_lines: list[str] = []
            for entry in capability_index:
                # Method names alone are not enough to route on — the model cannot
                # tell what ``browser_scan`` vs ``browser_execute_js`` want without a
                # one-line description. method_meta carries it; surface it here too.
                method_meta = entry.get("methods") or []
                if method_meta:
                    methods_str = "; ".join(
                        f"{m.get('name')}" + (f": {desc}" if (desc := _short_desc(m.get("description"))) else "")
                        for m in method_meta
                    )
                else:
                    methods_str = ", ".join(entry.get("method_names") or []) or "(no methods)"
                sop = entry.get("sop_ref") or ""
                service_name = str(entry["service"])
                if service_name in scene_services:
                    # This is the scene's own service: the model will use it on the
                    # first turn, so a pointer it has to file_read first is pure
                    # latency. Inline the body and drop the pointer.
                    inline = self._read_sop_body(sop)
                    if inline:
                        scene_lines.append(f"- {service_name}: {methods_str}")
                        scene_blocks.append(f"[Scene Capability: {service_name}]\n{inline}")
                        continue
                tail = f" (SOP: {sop})" if sop else ""
                other_lines.append(f"- {service_name}: {methods_str}{tail}")
            other_lines.append("- scheduler: create|list|cancel|run_now (SOP: memory/sop/scheduled_task_sop.md)")

            head = ""
            if scene_lines:
                head = "[Scene Tools]\n" + "\n".join(scene_lines)
                for block in scene_blocks:
                    head = f"{head}\n\n{block}"
            # Heading stays [Available Capabilities]: the constitution, the
            # capability_call error text, mcp_usage_sop / web_setup_sop and the
            # Agent admin page all point the model at that exact name. When a scene
            # service was hoisted out of this index, say so — otherwise the SOP's
            # promise that it "lists every bound MCP service" reads as a gap.
            cap_heading = "[Available Capabilities]"
            if scene_lines:
                cap_heading += "\n（本场景服务已在上方 [Scene Tools] / [Scene Capability] 完整给出，此处不重复）"
            body = f"{prefix}\n\n{cap_heading}\n" + "\n".join(other_lines)
            return tools, f"{head}\n\n{body}".strip() if head else body
        except Exception as exc:  # noqa: BLE001 - memory initialization must not hide the Agent itself
            logger.warning("[memory] initialize resources failed agent=%s: %s", self.agent_id, exc)
            return tools, ""

    def _read_sop_body(self, sop_ref: str) -> str:
        """Read a seeded MCP SOP body for inlining into the system prompt.

        ``sop_ref`` is the pointer form advertised in L1 (``memory/sop/mcp/x_sop.md``),
        resolved against the Agent root. Returns "" when the file is missing or the
        Agent has no id, so the caller falls back to the pointer form.
        """
        if self.agent_id is None or not sop_ref:
            return ""
        try:
            from agent.file_memory import agent_root

            path = agent_root(self.agent_id) / str(sop_ref).replace("\\", "/")
            if not path.is_file():
                return ""
            return path.read_text(encoding="utf-8", errors="replace").strip()[:_MAX_SCENE_SOP_CHARS]
        except OSError as exc:
            logger.debug("[scene] read sop body failed ref=%s: %s", sop_ref, exc)
            return ""

    async def _attach_mcp_tools(self, tools: ToolRegistry) -> None:
        """Resolve the Agent's effective MCP services and build a capability index.

        GA alignment: MCP methods are NOT registered as first-class tools. They are
        advertised in the system prompt's [Available Capabilities] index and invoked
        through ``capability_call(name="<service>.<method>", args=...)``. The MCP
        manager (call routing) is still exposed to atomic tools that proxy into it
        (the aggregated ``web`` tool routes browser operations through it).

        - Effective services come from mcp_client.resolve_effective_services.
        - Tool schema is taken from the service's tools_cache first, falling back
          to SSE tools/list.
        - Browser-class services are detected dynamically (by capability tag or by
          exposing a browser primitive), never by a hardcoded service name.
        - Defensive: runtime down / service not loaded / cache miss only skips that
          service and warns; never blocks the Agent.
        """
        from agent.mcp_client import MCPManager, resolve_effective_services

        config = await self._ensure_config()
        try:
            services = await resolve_effective_services(config.mcp_user_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[mcp] resolve_effective_services failed: %s", exc)
            return
        if not services:
            return

        service_tokens = await self._collect_service_tokens(services)
        manager = MCPManager(
            service_tokens=service_tokens,
            cdp_session_id=_scene_cdp_session_key(self._scene),
        )
        self._mcp_manager = manager

        BROWSER_PRIMITIVES = {
            "evaluate_script", "navigate", "capture_screenshot",
            "capturescreenshot", "click", "get_url", "set_url",
            "list_tabs", "get_tabs", "screenshot",
            # Platform CDP bridge naming.
            "browser_execute_js", "browser_navigate", "browser_scan",
            "browser_get_tabs", "browser_screenshot", "browser_switch_tab",
        }
        capability_index: list[dict] = []
        browser_service: str | None = None

        for svc in services:
            service_name = svc.get("name")
            if not service_name:
                continue
            try:
                mcp_tools = await self._tools_for_service(svc)
            except Exception as exc:  # noqa: BLE001 — 单个服务失败不影响其它服务
                logger.warning("[mcp] discover tools failed service=%s: %s", service_name, exc)
                continue
            method_names: list[str] = []
            method_meta: list[dict] = []
            is_browser = False
            for tool in mcp_tools:
                namespaced = f"{service_name}__{tool.name}"
                manager.tools[namespaced] = tool  # available via capability_call routing
                method_names.append(tool.name)
                method_meta.append({
                    "name": tool.name,
                    "description": getattr(tool, "description", "") or "",
                    # Carry the inputSchema so the seeded MCP SOP can document the
                    # parameters a first-time caller would otherwise have to guess.
                    "input_schema": getattr(tool, "input_schema", None) or {},
                })
                if str(tool.name).lower() in BROWSER_PRIMITIVES:
                    is_browser = True
            if is_browser and browser_service is None:
                browser_service = service_name
            capability_index.append({
                "service": service_name,
                "method_names": method_names,
                "methods": method_meta,
                "browser": is_browser,
                "sop_ref": f"memory/sop/mcp/{service_name}_sop.md" if method_names else "",
            })

        self._capability_index = capability_index
        self._browser_service = browser_service
        # Expose the MCP manager + capability index to atomic tools (web/capability_call).
        tools.set_mcp_runtime(
            manager,
            capability_index=capability_index,
            browser_service=browser_service,
        )
        # Seed each service's usage SOP (with parameter schemas) before any call, so the
        # L1 pointer the prompt advertises resolves to an executable file immediately.
        try:
            await tools.seed_mcp_sops()
        except Exception as exc:  # noqa: BLE001 — seeding must never block the Agent
            logger.warning("[mcp] seed mcp sops failed: %s", exc)
        logger.info(
            "[mcp] indexed %d MCP service(s) (browser=%s); methods reachable via capability_call",
            len(capability_index), browser_service,
        )

    async def _collect_service_tokens(self, services: list[dict]) -> dict[str, str]:
        """为开启鉴权的服务取一个可用 token（SSE ?token= / Bearer 用）。

        取不到 token 的鉴权服务不放进映射——调用时网关会 401，届时降级为该工具报错，
        但不阻断 agent 初始化。

        统一口径：agent 绑定了 mcp_user_id 时，**所有**开启鉴权的 MCP 服务都固定
        使用该用户的 token（前提是该用户已获该服务授权，即 token 在服务
        allowed_tokens 内）。这样 agent 的全部 MCP 请求都以同一个 MCP 用户身份发出；
        对 cdp-bridge 而言，该 token 经 runtime 的 token→client 路由落到对应客户端的
        浏览器会话池。未绑定用户或该用户未获授权时，不从 allowed_tokens 任取，避免
        跨用户串台——该服务的工具调用会 401 并降级为工具报错。
        """
        import mcp_plugin_store

        config = self.config or await self._ensure_config()
        bound_user_id = getattr(config, "mcp_user_id", None)
        bound_token = ""
        if bound_user_id is not None:
            try:
                user = await mcp_plugin_store.get_mcp_user(int(bound_user_id), mask_token=False)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[mcp] get bound mcp_user failed user_id=%s: %s", bound_user_id, exc)
                user = None
            if user and user.get("enabled") and user.get("token"):
                bound_token = user["token"]
            elif user and user.get("enabled") and user.get("token_hash"):
                logger.warning(
                    "[mcp] bound principal user_id=%s has no recoverable plaintext token; "
                    "rotate it or provision the token to this Agent runtime",
                    bound_user_id,
                )

        # principal 的明文 token 不落库。Agent 调用时签发短生命周期 identity token，
        # 网关由 agent_id → agents.mcp_user_id → principal 解析身份；具体操作哪个 CDP
        # 客户端 / 邮箱账户由 driver 读 principal 的 param 定位（见 cdp_bridge_plugin /
        # mail_plugin），主链路不再做 grant scope 判断。
        identity_token = ""
        if bound_user_id is not None and self.agent_id is not None:
            try:
                import builtin_tool_store

                # target_id 必须是 agents 行 id（网关 identity 分支据此
                # get_agent_mcp_principal_id 反查绑定 principal）。AgentConfig 是配置
                # dataclass、不带行 id，这里用 self.agent_id。
                _row, identity_token = await builtin_tool_store.issue_token(
                    "agent", str(self.agent_id), display_token=False,
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("[mcp] issue agent identity token failed agent_id=%s: %s", self.agent_id, exc)

        tokens: dict[str, str] = {}
        # 市场 MCP 以本次对话发起者身份调用。token 只存在于本次 Agent
        # runtime，不返回前端；adapter 会再次核验用户仍为平台管理员。
        # 场景保留：只有市场管理场景（scene.services 含 marketplace-status）才签发
        # 身份 token——普通 Agent即使因历史配置还挂着这个服务，也拿不到可用身份。
        scene_services = set(getattr(self._scene, "services", ()) or ())
        marketplace_services = [
            svc for svc in services
            if svc.get("name") == "marketplace-status" and "marketplace-status" in scene_services
        ]
        if marketplace_services:
            try:
                import builtin_tool_store

                target_id = self._run_as_caller if self._run_as_caller is not None else "__admin__"
                _row, marketplace_token = await builtin_tool_store.issue_token(
                    "user", str(target_id), display_token=False,
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                )
                tokens["marketplace-status"] = marketplace_token
            except Exception as exc:  # noqa: BLE001
                logger.warning("[mcp] issue marketplace identity token failed: %s", exc)

        for svc in services:
            if svc.get("name") == "marketplace-status":
                continue
            name = svc.get("name")
            svc_id = svc.get("id")
            if not name or svc_id is None:
                continue
            # 鉴权一律强制：所有 MCP 服务都要 token，不再按服务的 auth_enabled 开关
            # 决定收不收集 token（曾让 admin 关过鉴权的匿名服务两边口径不一致致 401）。
            # identity token 即放行：具体操作哪个 CDP 客户端 / 邮箱账户 / 设备由 driver
            # 读 principal 的 param 定位（cdp_bridge_plugin / mail_plugin / device_control_plugin），
            # custom/sse/stdio 服务的授权由网关按 mcp_service_users 判。
            if identity_token:
                tokens[name] = identity_token
                continue
            # 兼容旧 principal plaintext token + mcp_service_users 授权（identity token 签不出
            # 时的兜底：agent_id 为空 / issue_token 失败的极少数路径）。
            try:
                auth = await mcp_plugin_store.get_service_auth(int(svc_id))
            except Exception as exc:  # noqa: BLE001
                logger.warning("[mcp] get_service_auth failed service=%s: %s", name, exc)
                continue
            allowed = auth.get("allowed_tokens") or set()
            if bound_token and bound_token in allowed:
                tokens[name] = bound_token
                continue
            # 未绑定/未授权：不任取 token，避免跨用户串台。
            logger.warning(
                "[mcp] service=%s 鉴权已强制，但 agent 未绑定获授权的 MCP 用户，调用将 401/403",
                name,
            )
        return tokens

    async def _tools_for_service(self, svc: dict) -> list:
        """取服务工具列表：优先 tools_cache，缺失回退 SSE tools/list。"""
        from agent.mcp_client import MCPServerConfig, MCPTool

        service_name = svc.get("name") or ""
        cache = svc.get("tools_cache")
        tools: list = []
        if isinstance(cache, list) and cache:
            for entry in cache:
                if not isinstance(entry, dict):
                    continue
                tools.append(
                    MCPTool(
                        name=entry.get("name") or "",
                        description=entry.get("description") or "",
                        input_schema=entry.get("input_schema") or {},
                        service_name=service_name,
                    )
                )
            tools = [t for t in tools if t.name]
            if tools:
                return tools

        # tools_cache 缺失/为空：回退 SSE tools/list。
        cfg = MCPServerConfig.from_dict({
            "name": service_name,
            "transport": svc.get("transport") or "stdio",
            "command": svc.get("command"),
            "args": svc.get("args") or [],
            "url": svc.get("url"),
            "builtin": bool(svc.get("builtin")) or svc.get("kind") == "builtin",
        })
        mgr = self._mcp_manager
        if mgr is None:
            from agent.mcp_client import MCPManager
            mgr = MCPManager()
        return await mgr.discover_tools(cfg)

    @property
    def llm(self) -> GatewayLLMBridge | None:
        return self._llm

    @property
    def tools(self) -> ToolRegistry | None:
        return self._tools

    def set_event_sink(self, on_event) -> None:
        """注入父级事件出口，spawn 出的子 agent 会把事件透传到这里。

        主对话流（agent.run_task 直接被调用，或 api.py 直接调 agent_runner_loop）
        在开始前调用本方法，spawn_subagent_async 内部取用。
        """
        self._event_sink = on_event

    async def run_task(
        self,
        prompt: str,
        system_prompt: str = "",
        max_turns: int | None = None,
        on_event=None,
        llm: "GatewayLLMBridge | None" = None,
    ) -> list[dict[str, Any]]:
        """跑一轮任务。

        ``system_prompt`` 留空时回落到 Agent 行上的 system_prompt（人设/指令），而不是
        跑成空提示：对话链路会显式传会话级 system_prompt，而无人值守入口（定时任务、
        自愈诊断）没有会话可读，之前不传就把 Agent 人设整段丢了。

        ``llm`` 允许调用方指定本次用的桥（定时任务用 resolve_scheduled_llm 换模型），
        不传则用主 Agent 的桥。传入不会写回 self._llm，只作用于这一次运行。
        """
        config = await self._ensure_config()
        if llm is None:
            llm = await self._ensure_llm()
        tools, skill_index = await self._ensure_resources()
        # 显式传入优先；没传就用 Agent 自己的人设（_ensure_config 已从 DB 读出）。
        effective_system_prompt = (system_prompt or "").strip() or self._agent_system_prompt
        if skill_index:
            effective_system_prompt = f"{effective_system_prompt}\n\n{skill_index}".strip()
        # 记录事件出口给本 agent 内 spawn 出的子 agent 透传用。
        if on_event is not None:
            self._event_sink = on_event

        handler = _AgentHandler(tools, max_turns or config.max_turns)
        outputs: list[dict[str, Any]] = []
        try:
            async for item in agent_runner_loop(
                llm,
                effective_system_prompt,
                prompt,
                handler,
                tools.get_schema(),
                max_turns=max_turns or config.max_turns,
                verbose=False,
                on_event=on_event,
            ):
                outputs.append(item)
        finally:
            # Release any CDP tab leases this Agent runtime acquired. The per-task
            # MCP manager is created in _attach_mcp_tools; close() releases by holder.
            # Errors here must not mask the real exception above.
            try:
                manager = self._mcp_manager
                if manager is not None:
                    await manager.close()
            except Exception:
                pass
        return outputs

    async def spawn_subagent_async(
        self,
        task: str,
        model: str | None = None,
        api_key: str | None = None,
        llm_model_id: int | None = None,
    ) -> SubagentHandle:
        """异步派生子 agent，选择走网关的 LLM 桥与模型。

        解析顺序：
        1. 显式 model 字符串：复用主 agent 的网关桥（同 key），仅覆盖模型名。
        2. 子 Agent 专用绑定（subagent_api_key_id）：独立网关 key + 模型。
        3. 旧配置 config.subagent_model：复用主桥（同 key）换模型。
        4. 跟随主 agent。

        ``llm_model_id`` 为历史签名保留（已不再走 agent_llm_models 库存），传入时按
        「跟随主桥」处理，避免调用方报错。
        """
        parent_llm = await self._ensure_llm()
        child_llm: "GatewayLLMBridge"
        if model is not None:
            child_llm = parent_llm.fork(model=model)
        else:
            config = self.config or await self._ensure_config()
            mapped = await self._resolve_subagent_llm()
            if mapped is not None:
                child_llm = mapped
            elif config.subagent_model:
                child_llm = parent_llm.fork(model=config.subagent_model)
            else:
                child_llm = parent_llm.fork()
        self._subagent_seq += 1
        handle_id = f"sub-{id(self)}-{self._subagent_seq}"
        handle_name = f"子 Agent {self._subagent_seq}"
        task_handle = asyncio.create_task(self._run_subagent_task(handle_id, handle_name, task, child_llm))
        handle = SubagentHandle(task=task, llm=child_llm, asyncio_task=task_handle)
        self.sub_agents.append(handle)
        return handle

    def spawn_subagent(
        self,
        task: str,
        model: str | None = None,
        api_key: str | None = None,
    ) -> SubagentHandle:
        """同步派生子 agent（兼容入口）。

        仅支持显式模型覆盖；不读取 subagent 角色映射（同步函数无法做 DB I/O）。
        需要按角色映射派生时请使用 spawn_subagent_async。
        """
        if self._llm is None:
            raise RuntimeError("LLM 尚未初始化，无法派生子 agent（请先调用 _ensure_llm）")
        child_llm = self._llm.fork(model=model)
        self._subagent_seq += 1
        handle_id = f"sub-{id(self)}-{self._subagent_seq}"
        handle_name = f"子 Agent {self._subagent_seq}"
        task_handle = asyncio.create_task(self._run_subagent_task(handle_id, handle_name, task, child_llm))
        handle = SubagentHandle(task=task, llm=child_llm, asyncio_task=task_handle)
        self.sub_agents.append(handle)
        return handle

    async def _run_subagent_task(
        self,
        handle_id: str,
        handle_name: str,
        task: str,
        llm: GatewayLLMBridge | None,
    ) -> list[dict[str, Any]]:
        child = GenericAgent(
            config=self.config,
            agent_id=self.agent_id,
            llm=llm,
            tools=self._tools,
            run_as_caller=self._run_as_caller,
        )

        # 构造事件转发器：把子 agent 的事件改写为 subagent_* 前缀并带上 handle_id，
        # 先发 subagent_start，结束后发 subagent_end。仅当父级挂了事件出口时透传。
        parent_sink = self._event_sink

        async def forward(event: dict) -> None:
            if not parent_sink:
                return
            base_type = event.get("type", "")
            out = {
                "subagent_id": handle_id,
                "subagent_name": handle_name,
                **event,
                "type": f"subagent_{base_type}" if base_type else "subagent_event",
            }
            try:
                await parent_sink(out)
            except Exception:  # noqa: BLE001 — 子 agent 事件出口失败不应阻断其自身执行
                logger.debug("[subagent] failed to forward event %s", base_type, exc_info=True)

        if parent_sink:
            try:
                await parent_sink({
                    "type": "subagent_start",
                    "subagent_id": handle_id,
                    "subagent_name": handle_name,
                    "task": task,
                })
            except Exception:  # noqa: BLE001
                logger.debug("[subagent] failed to forward start event", exc_info=True)

        outputs: list[dict[str, Any]] = []
        try:
            if parent_sink:
                outputs = await child.run_task(task, on_event=forward)
            else:
                outputs = await child.run_task(task)
        finally:
            if parent_sink:
                summary = ""
                if outputs:
                    last = outputs[-1]
                    summary = str(last.get("data") or "") if isinstance(last, dict) else str(last)
                try:
                    await parent_sink({
                        "type": "subagent_end",
                        "subagent_id": handle_id,
                        "subagent_name": handle_name,
                        "summary": summary[:500],
                    })
                except Exception:  # noqa: BLE001
                    logger.debug("[subagent] failed to forward end event", exc_info=True)
        return outputs
