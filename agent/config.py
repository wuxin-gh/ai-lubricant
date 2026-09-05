"""GA 配置模块"""
from dataclasses import dataclass, field


@dataclass
class AgentConfig:
    """GenericAgent 子系统配置。"""

    enabled: bool = False

    # LLM 配置（核心）
    api_key: str = ""
    model: str = ""

    # 关联 agent_llm_configs.id（旧：agent 服务专用 LLM 配置）。已停用，保留仅为
    # 兼容历史数据/序列化，新链路不再读取。LLM 走网关秘钥（下方 *_api_key_id）。
    llm_config_id: int | None = None

    # Subagent 默认 LLM（可选覆盖）——旧字段，保留兼容。
    subagent_api_key: str = ""
    subagent_model: str = ""

    # ── 网关秘钥绑定（新）：agent LLM 走本系统网关 dispatch_entry，形成计费/限流/归属闭环 ──
    # 主 Agent：绑定的网关 api_keys.id + 模型名（模型必须是网关 providers 提供的）。
    main_api_key_id: int | None = None
    main_model: str = ""
    # 子 Agent：空 api_key_id 表示跟随主 Agent（同 key，可另选模型）。
    subagent_api_key_id: int | None = None
    # 定时任务默认绑定：定时态是无人值守场景，通常想用更便宜/更稳的模型跑，
    # 与交互态主 Agent 分开。两者都空 → 定时任务回退到主 Agent 的 key/模型。
    # 任务行上的 api_key_id/model 优先于这里的 Agent 级默认。
    scheduled_api_key_id: int | None = None
    scheduled_model: str = ""

    # 运行控制
    max_turns: int = 80
    max_concurrent_tasks: int = 5

    # 功能开关
    memory_enabled: bool = True
    skill_auto_learn: bool = True
    browser_enabled: bool = False
    subagent_enabled: bool = True
    scheduler_enabled: bool = False

    # 思考模式（agent 级策略；注入 agent LLM 请求体，独立于主系统）
    thinking_enabled: bool = False
    reasoning_effort: str = ""

    # agent 层 429 自动重试次数：仅对瞬时可重试业务码（rate_limit_exceeded/
    # service_busy/concurrent_limit_exceeded 等）做带退避重试；配置类
    # no_available_account（白名单/禁用）不重试。0=关闭。注入 GatewayLLMBridge.max_retries。
    llm_retry_429: int = 2

    # 需确认的工具（code_run / node_shell_exec）挂起等人裁决的上限，单位秒。
    # 超时中止本轮而非自动拒绝后继续下一轮。0 = 不过期，一直等到有人裁决。
    approval_timeout_seconds: int = 24 * 60 * 60

    # 浏览器（可选）
    cdp_bridge_mode: str = "stdio"
    cdp_bridge_token: str = ""

    # 绑定的 MCP 用户（cdp-bridge 客户端）id；空表示未绑定。
    # 绑定后：agent 按该 principal 的 params/服务授权挂载 MCP 并调用。
    mcp_user_id: int | None = None

    # 安全
    workspace_root: str = "agent/workspace"
    allowed_roots: list[str] = field(default_factory=lambda: ["agent/workspace", "agent/temp"])
    denied_patterns: list[str] = field(
        default_factory=lambda: [
            r"^/etc/",
            r"^/var/",
            r"config\.json$",
            r"db\.py$",
            r"\.env$",
            r"\.git/",
            r"node_modules/",
        ]
    )

    @classmethod
    def from_dict(cls, data: dict) -> "AgentConfig":
        """从 dict 加载配置，忽略未知字段"""
        if not isinstance(data, dict):
            return cls()
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in valid_keys})

    def to_dict(self) -> dict:
        """导出为 dict"""
        return {
            "enabled": self.enabled,
            "api_key": self.api_key,
            "model": self.model,
            "llm_config_id": self.llm_config_id,
            "subagent_api_key": self.subagent_api_key,
            "subagent_model": self.subagent_model,
            "main_api_key_id": self.main_api_key_id,
            "main_model": self.main_model,
            "subagent_api_key_id": self.subagent_api_key_id,
            "scheduled_api_key_id": self.scheduled_api_key_id,
            "scheduled_model": self.scheduled_model,
            "max_turns": self.max_turns,
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "memory_enabled": self.memory_enabled,
            "skill_auto_learn": self.skill_auto_learn,
            "browser_enabled": self.browser_enabled,
            "subagent_enabled": self.subagent_enabled,
            "scheduler_enabled": self.scheduler_enabled,
            "thinking_enabled": self.thinking_enabled,
            "reasoning_effort": self.reasoning_effort,
            "llm_retry_429": self.llm_retry_429,
            "cdp_bridge_mode": self.cdp_bridge_mode,
            "cdp_bridge_token": self.cdp_bridge_token,
            "mcp_user_id": self.mcp_user_id,
            "workspace_root": self.workspace_root,
            "allowed_roots": self.allowed_roots,
            "denied_patterns": self.denied_patterns,
        }
