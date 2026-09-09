"""Single declaration source for in-process MCP built-in services.

This module is intentionally dependency-free: database initialization can import
service metadata without importing adapters or optional browser/mail drivers.
The runtime resolves ``adapter_module`` lazily through importlib.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BuiltinServiceSpec:
    name: str
    adapter_module: str
    persisted: bool
    display_name: str = ""
    description: str = ""
    category: str = "custom"
    version: str = "1.0.0"
    author: str = "ai-lubricant"
    docs_url: str = ""
    # 该服务鉴权所需的 principal param key（cdp-bridge→cdp_client_id 等）。
    # 空 = 不需 param 的内置服务（marketplace-status 等走 identity 特判）。
    # 与 mcp_runtime.sse_gateway._BUILTIN_SERVICE_PARAM_KEY 同源：本字段是单一声明源，
    # DB seed 时落 mcp_services.required_param，sse_gateway 启动时从 catalog 读，
    # 不再各处手写映射。
    required_param: str = ""


BUILTIN_SERVICE_SPECS: tuple[BuiltinServiceSpec, ...] = (
    BuiltinServiceSpec(
        name="cdp-bridge",
        adapter_module="mcp_runtime.builtin_plugins.cdp_bridge_plugin",
        persisted=True,
        display_name="CDP Bridge",
        description=(
            "Chrome browser automation via CDP protocol. Connects to a real "
            "browser with login sessions preserved."
        ),
        category="browser",
        version="0.1.19",
        author="Unagi-cq",
        docs_url="https://github.com/unagi-cq/cdp-bridge",
        required_param="cdp_client_id",
    ),
    BuiltinServiceSpec(
        name="mail",
        adapter_module="mcp_runtime.builtin_plugins.mail_plugin",
        persisted=True,
        display_name="邮件读取",
        description=(
            "Read configured upstream mailboxes by their primary address or "
            "configured forwarding alias."
        ),
        category="media",
        required_param="mail_account_id",
    ),
    BuiltinServiceSpec(
        name="device-control",
        adapter_module="mcp_runtime.builtin_plugins.device_control_plugin",
        persisted=True,
        display_name="设备控制",
        description=(
            "Android 远控：通过无障碍服务读屏/截图，下发 16 个命令驱动手机。"
            "设备经配对码接入，按用户实例隔离。"
        ),
        category="device",
        required_param="device_id",
    ),
    BuiltinServiceSpec(
        name="marketplace-status",
        adapter_module="mcp_runtime.builtin_plugins.marketplace_plugin",
        persisted=True,
        display_name="市场资源管理",
        description=(
            "市场管理场景保留能力：维护普通市场 manifest 与外部榜单草稿（查询、修改、"
            "排序、发布/撤回、手动添加 GitHub）。仅市场管理页面的管理员 Agent 临时获权，"
            "普通 Agent MCP 选择器不暴露。"
        ),
        category="code",
    ),
    BuiltinServiceSpec(
        name="issue-workflow",
        adapter_module="mcp_runtime.builtin_plugins.issue_workflow_plugin",
        persisted=False,
    ),
    BuiltinServiceSpec(
        name="review-result",
        adapter_module="mcp_runtime.builtin_plugins.review_result_plugin",
        persisted=False,
    ),
)

PERSISTED_BUILTIN_SERVICE_SPECS = tuple(spec for spec in BUILTIN_SERVICE_SPECS if spec.persisted)
INTERNAL_BUILTIN_SERVICE_SPECS = tuple(spec for spec in BUILTIN_SERVICE_SPECS if not spec.persisted)


__all__ = [
    "BuiltinServiceSpec",
    "BUILTIN_SERVICE_SPECS",
    "PERSISTED_BUILTIN_SERVICE_SPECS",
    "INTERNAL_BUILTIN_SERVICE_SPECS",
]
