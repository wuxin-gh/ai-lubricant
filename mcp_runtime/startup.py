"""Runtime startup recovery.

MCP Runtime 重启后恢复活动插件：
- internal builtin（runtime 内部会话能力）：不依赖 DB，启动即注册。
- configurable builtin（vendored in-tree）：只从持久化快照恢复，不走启停生命周期。
- custom（用户源码）：要求 enabled 且有 active_version_id，走 activate_custom（exec）。
失败的插件只标记 runtime_status=error，不阻断 Runtime 启动。
"""
from __future__ import annotations

from loguru import logger

import mcp_plugin_store
from .registry import registry
from .plugin_loader import PluginLoadError
from .configuration import build_builtin_snapshot


async def restore_active_plugins() -> None:
    # ── 1) 内部服务：runtime 会话基础设施，不依赖持久化服务配置 ──
    internal_registered = await registry.register_internal_builtins()

    # ── 2) 可配置内置服务：从 DB 恢复 env/auth/resources 快照 ──
    # 这些服务不看 DB enabled、不可启停；但必须有持久化配置才能安全加载。
    # 遗留的内部服务行不参与配置和状态生命周期。
    builtin_config: dict[str, dict] = {}
    builtin_service_ids: dict[str, int] = {}
    for service in await mcp_plugin_store.list_services():
        if not (bool(service.get("builtin")) or (service.get("kind") or "stdio") == "builtin"):
            continue
        sid = service["id"]
        name = service["name"]
        if registry.is_internal_builtin(name):
            continue
        try:
            snapshot = await build_builtin_snapshot(service)
        except Exception as exc:
            await mcp_plugin_store.mark_runtime_error(sid, str(exc))
            logger.error(f"[mcp-runtime] build builtin snapshot failed service={name}: {exc}")
            continue
        builtin_config[name] = {
            "env": snapshot.env,
            "resources": snapshot.resources,
            "enabled": snapshot.enabled,
            "auth_enabled": snapshot.auth_enabled,
            "allowed_tokens": snapshot.allowed_tokens,
        }
        builtin_service_ids[name] = sid

    registered = await registry.register_builtins(builtin_config, configured_only=True)
    for name, plugin in registered.items():
        sid = builtin_service_ids.get(name)
        if sid is None:
            continue
        tools_cache = [
            {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
            for t in plugin.tools
        ]
        await mcp_plugin_store.mark_runtime_status(sid, "loaded", tools_cache=tools_cache)
    # 内置未注册成功（依赖缺失等）也要如实标记 error，方便管理端排障。
    for name, sid in builtin_service_ids.items():
        if name not in registered:
            await mcp_plugin_store.mark_runtime_error(sid, "builtin register failed (见 runtime 日志)")

    # ── 2) custom 插件：DB enabled 门禁 + active version ──
    restored = 0
    for service in await mcp_plugin_store.list_services():
        if not service.get("enabled"):
            continue
        if (service.get("kind") or "stdio") != "custom":
            continue
        name = service["name"]
        version_id = service.get("active_version_id")
        if not version_id:
            continue
        version = await mcp_plugin_store.get_version(int(version_id))
        if not version:
            continue
        code = version.get("code") or ""
        env = await mcp_plugin_store.get_service_env(service["id"])
        if not env:
            env = dict(service.get("env_template") or {})
        auth = await mcp_plugin_store.get_service_auth(service["id"])
        try:
            plugin = await registry.activate_custom(
                name, code, env=env,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
            tools_cache = [
                {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
                for t in plugin.tools
            ]
            await mcp_plugin_store.activate_version(int(version_id), tools_cache=tools_cache)
            restored += 1
        except PluginLoadError as e:
            await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
            logger.error(f"[mcp-runtime] restore plugin failed service={name}: {e}")
        except Exception as e:
            await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
            logger.error(f"[mcp-runtime] restore plugin unexpected error service={name}: {e}")

    # ── 3) stdio 插件：不在本进程拉起，转发给独立执行器 ──
    # stdio MCP 是下载到本地跑的第三方进程，污染面（读写文件系统/越权）必须隔离在
    # 执行器边界内。本进程只做转发点：向执行器发现工具、调用时转发（见
    # registry.activate_stdio + stdio_executor）。执行器未部署时干净跳过并标错，
    # 绝不退化为本进程内 create_subprocess_exec。
    from .stdio_executor import StdioSpec, is_configured as _executor_configured
    stdio_restored = 0
    for service in await mcp_plugin_store.list_services():
        if not service.get("enabled"):
            continue
        if (service.get("kind") or "stdio") != "stdio":
            continue
        name = service["name"]
        if not _executor_configured():
            await mcp_plugin_store.mark_runtime_error(
                service["id"], "stdio 执行器未部署（MCP_STDIO_EXECUTOR_URL/TOKEN 未配置），已跳过"
            )
            logger.warning(f"[mcp-runtime] skip stdio service={name}: executor not configured")
            continue
        env = await mcp_plugin_store.get_service_env(service["id"])
        if not env:
            env = dict(service.get("env_template") or {})
        auth = await mcp_plugin_store.get_service_auth(service["id"])
        spec = StdioSpec.from_service(service, env=env)
        try:
            plugin = await registry.activate_stdio(
                name, spec,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
            tools_cache = [
                {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
                for t in plugin.tools
            ]
            await mcp_plugin_store.mark_runtime_status(service["id"], "loaded", tools_cache=tools_cache)
            stdio_restored += 1
        except PluginLoadError as e:
            await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
            logger.error(f"[mcp-runtime] restore stdio failed service={name}: {e}")
        except Exception as e:
            await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
            logger.error(f"[mcp-runtime] restore stdio unexpected error service={name}: {e}")

    # ── 4) sse 插件：外部 SSE 服务，注册到本地注册表，调用直连远端 ──
    sse_restored = 0
    for service in await mcp_plugin_store.list_services():
        if not service.get("enabled"):
            continue
        if (service.get("kind") or "stdio") != "sse":
            continue
        name = service["name"]
        url = service.get("url")
        if not url:
            await mcp_plugin_store.mark_runtime_error(service["id"], "sse 服务缺少 url，已跳过")
            logger.warning(f"[mcp-runtime] skip sse service={name}: missing url")
            continue
        auth = await mcp_plugin_store.get_service_auth(service["id"])
        try:
            plugin = await registry.activate_sse(
                name,
                url,
                headers=service.get("headers") or {},
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
            tools_cache = [
                {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
                for t in plugin.tools
            ]
            await mcp_plugin_store.mark_runtime_status(service["id"], "loaded", tools_cache=tools_cache)
            sse_restored += 1
        except PluginLoadError as e:
            await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
            logger.error(f"[mcp-runtime] restore sse failed service={name}: {e}")
        except Exception as e:
            await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
            logger.error(f"[mcp-runtime] restore sse unexpected error service={name}: {e}")

    logger.info(
        f"[mcp-runtime] restored: internal_builtins={len(internal_registered)} "
        f"builtins={len(registered)} custom={restored} stdio={stdio_restored} sse={sse_restored}"
    )
