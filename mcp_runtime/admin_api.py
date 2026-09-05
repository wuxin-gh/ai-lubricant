"""MCP Runtime 管理 API（管理员鉴权）。

热加载流程：
  custom 类：list_versions → security-check（人工触发）→ activate（要求 security_status=passed）
  builtin 类：start 直接 activate_builtin（vendored adapter，不走 exec / security-check）
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import mcp_plugin_store
from .registry import registry
from .security_review import run_security_check
from .plugin_loader import PluginLoadError
from .configuration import build_builtin_snapshot, _manifest_for_service

router = APIRouter(prefix="/mcp/admin", tags=["mcp-runtime-admin"])


async def _require_admin(request: Request) -> None:
    from admin import _require_admin as _ra
    await _ra(request.headers.get("authorization"))


async def _service_env(service: dict) -> dict:
    """读取 custom 服务环境变量；builtin 统一走配置 snapshot builder。"""
    env = await mcp_plugin_store.get_service_env(service["id"])
    return env or dict(service.get("env_template") or {})


async def _service_auth(service: dict) -> dict:
    """读取 custom 服务鉴权快照；builtin 统一走配置 snapshot builder。"""
    return await mcp_plugin_store.get_service_auth(service["id"])


@router.post("/services/{service_id}/start")
async def start_service(service_id: int, request: Request):
    """启动一个 MCP 服务。

    - builtin：runtime 的一体部分，常驻运行；start 退化为幂等「确保已注册 + 配置已下发」，
      不调用生命周期启停。工具恒在、扩展连接不断。
    - custom：active version 热加载（要求 security_status=passed）。
    """
    await _require_admin(request)
    result = await start_service_core(service_id)
    if not result["ok"]:
        raise HTTPException(result.get("code", 400), result.get("error", "启动失败"))
    return result


async def start_service_core(service_id: int) -> dict:
    """启动内核：router 与安装编排器共用，无鉴权、用结构化结果而非 HTTPException。

    返回 {"ok", "status", "tools", "error", "code"}。失败时 ok=False，error 是人话、
    code 是建议 HTTP 状态（供 router 抛）。编排器据此决定走 error 还是下一步。
    """
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        return {"ok": False, "error": "service not found", "code": 404, "status": "error", "tools": 0}
    name = service["name"]
    kind = service.get("kind") or "stdio"
    is_builtin = bool(service.get("builtin")) or kind == "builtin"
    # stdio 类：不在本进程拉起子进程，转发给独立执行器（污染面隔离在执行器边界内）。
    # 本期只留转发点；执行器未部署时 activate_stdio 抛 PluginLoadError，如实标错。
    is_stdio = (not is_builtin) and (service.get("transport") == "stdio" or kind == "stdio")
    is_sse = (not is_builtin) and (kind == "sse")
    if is_builtin:
        snapshot = await build_builtin_snapshot(service)
        env = snapshot.env
        auth = {"auth_enabled": snapshot.auth_enabled, "allowed_tokens": snapshot.allowed_tokens}
    else:
        env = await _service_env(service)
        auth = await _service_auth(service)

    try:
        if is_builtin:
            existing = registry.get(name)
            if existing is not None:
                plugin = await registry.update_builtin_config(
                    name, env=env,
                    resources=snapshot.resources,
                    enabled=snapshot.enabled,
                    auth_enabled=auth["auth_enabled"],
                    allowed_tokens=auth["allowed_tokens"],
                )
            else:
                plugin = await registry.activate_builtin(
                    name, env=env,
                    resources=snapshot.resources,
                    enabled=snapshot.enabled,
                    auth_enabled=auth["auth_enabled"],
                    allowed_tokens=auth["allowed_tokens"],
                )
        elif is_stdio:
            from .stdio_executor import StdioSpec
            spec = StdioSpec.from_service(service, env=env)
            plugin = await registry.activate_stdio(
                name, spec,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
        elif is_sse:
            url = service.get("url")
            if not url:
                return {"ok": False, "error": "sse 服务缺少 url", "code": 400, "status": "error", "tools": 0}
            plugin = await registry.activate_sse(
                name, url,
                headers=service.get("headers") or {},
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
        else:
            version = await mcp_plugin_store.get_active_version(service_id)
            if not version:
                return {"ok": False, "error": "服务没有可用的已发布版本，先创建并激活版本", "code": 409, "status": "error", "tools": 0}
            if version.get("security_status") != "passed":
                return {"ok": False, "error": f"安全检查未通过（当前: {version.get('security_status')}），不能启动", "code": 409, "status": "error", "tools": 0}
            if not version.get("code", "").strip():
                return {"ok": False, "error": "版本没有插件代码", "code": 400, "status": "error", "tools": 0}
            plugin = await registry.activate_custom(
                name, version["code"], env=env,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
    except PluginLoadError as e:
        await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
        return {"ok": False, "error": f"插件加载失败: {e}", "code": 400, "status": "error", "tools": 0}
    except Exception as e:
        await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
        return {"ok": False, "error": f"启动失败: {e}", "code": 500, "status": "error", "tools": 0}

    tools_cache = [
        {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
        for t in plugin.tools
    ]
    if not is_builtin:
        await mcp_plugin_store.set_service_enabled(service_id, True)
    version_id = service.get("active_version_id")
    if version_id:
        await mcp_plugin_store.activate_version(int(version_id), tools_cache=tools_cache)
    else:
        await mcp_plugin_store.mark_runtime_status(service_id, "loaded", tools_cache=tools_cache)
    return {"ok": True, "status": "loaded", "tools": len(plugin.tools), "tools_cache": tools_cache}



@router.post("/services/{service_id}/stop")
async def stop_service(service_id: int, request: Request):
    """停止一个 MCP 服务（从 runtime 卸载，不改 enabled/版本）。

    builtin 不可停止——它是 runtime 的常驻部分；custom 正常 deactivate。
    """
    await _require_admin(request)
    return await stop_service_core(service_id)


async def stop_service_core(service_id: int) -> dict:
    """stop 内核：router 与安装编排器共用，无鉴权、用结构化结果。

    失败时抛 HTTPException（与 router 抛错语义一致），调用方可按需捕获。
    """
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, "service not found")
    is_builtin = bool(service.get("builtin")) or (service.get("kind") or "stdio") == "builtin"
    if is_builtin:
        raise HTTPException(400, "内置服务常驻运行，不可停止")
    await registry.deactivate(service["name"])
    await mcp_plugin_store.set_service_enabled(service_id, False)
    await mcp_plugin_store.mark_runtime_status(service_id, "stopped")
    return {"ok": True, "status": "stopped"}


@router.get("/services")
async def list_services(request: Request):
    await _require_admin(request)
    return await mcp_plugin_store.list_services()


@router.get("/services/{service_id}/versions")
async def list_versions(service_id: int, request: Request):
    await _require_admin(request)
    return await mcp_plugin_store.list_versions(service_id)


@router.get("/versions/{version_id}")
async def get_version(version_id: int, request: Request):
    await _require_admin(request)
    v = await mcp_plugin_store.get_version(version_id)
    if not v:
        raise HTTPException(404, "version not found")
    return v


class CreateVersionPayload(BaseModel):
    code: str = ""
    config_json: dict = {}
    author: str = ""
    source: str = "manual"


@router.post("/services/{service_id}/versions")
async def create_version(service_id: int, payload: CreateVersionPayload, request: Request):
    await _require_admin(request)
    return await create_version_core(service_id, payload)


async def create_version_core(service_id: int, payload: CreateVersionPayload) -> dict:
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, "service not found")
    return await mcp_plugin_store.create_version(
        service_id,
        code=payload.code,
        config_json=payload.config_json,
        author=payload.author,
        source=payload.source,
    )


@router.post("/versions/{version_id}/security-check")
async def security_check(version_id: int, request: Request):
    await _require_admin(request)
    return await run_security_check(version_id)


@router.post("/versions/{version_id}/activate")
async def activate(version_id: int, request: Request):
    await _require_admin(request)
    return await activate_core(version_id)


async def activate_core(version_id: int) -> dict:
    version = await mcp_plugin_store.get_version(version_id)
    if not version:
        raise HTTPException(404, "version not found")
    if version.get("security_status") != "passed":
        raise HTTPException(409, f"安全检查未通过（当前: {version.get('security_status')}），不能发布")
    if not version.get("code", "").strip():
        raise HTTPException(400, "版本没有插件代码")

    service = await mcp_plugin_store.get_service(version["service_id"])
    if not service:
        raise HTTPException(404, "service not found")
    service_name = service["name"]

    env = await _service_env(service)
    auth = await _service_auth(service)
    try:
        plugin = await registry.activate_custom(
            service_name, version["code"], env=env,
            auth_enabled=auth["auth_enabled"],
            allowed_tokens=auth["allowed_tokens"],
        )
    except PluginLoadError as e:
        await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
        raise HTTPException(400, f"插件加载失败: {e}")
    tools_cache = [
        {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": service_name}
        for t in plugin.tools
    ]
    return await mcp_plugin_store.activate_version(version_id, tools_cache=tools_cache)


@router.post("/services/{service_id}/reload")
async def reload_service(service_id: int, request: Request):
    await _require_admin(request)
    return await reload_service_core(service_id)


async def reload_service_core(service_id: int) -> dict:
    """更新 MCP 服务运行配置（router 与安装编排器共用，无鉴权）。

    - builtin：常驻内置能力，不重建工具、不重新注册；仅把 env/auth/resources
      原地下发给 live driver 与 PluginContext。
    - custom：用当前 active version 的代码重新 activate_custom，env/auth 即时生效。
    """
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, "service not found")
    name = service["name"]
    kind = service.get("kind") or "stdio"
    is_builtin = bool(service.get("builtin")) or kind == "builtin"
    # stdio 类：转发给独立执行器（污染面隔离在执行器边界内）；本期只留转发点。
    is_stdio = (not is_builtin) and (service.get("transport") == "stdio" or kind == "stdio")
    is_sse = (not is_builtin) and (kind == "sse")
    if is_builtin:
        snapshot = await build_builtin_snapshot(service)
        env = snapshot.env
        auth = {"auth_enabled": snapshot.auth_enabled, "allowed_tokens": snapshot.allowed_tokens}
    else:
        env = await _service_env(service)
        auth = await _service_auth(service)

    # 内置服务未注册（异常路径）→ 退化为 start（首次注册）。
    if is_builtin and registry.get(name) is None:
        return await start_service_core(service_id)

    try:
        if is_builtin:
            plugin = await registry.update_builtin_config(
                name, env=env,
                resources=snapshot.resources,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
        elif is_stdio:
            from .stdio_executor import StdioSpec
            spec = StdioSpec.from_service(service, env=env)
            plugin = await registry.activate_stdio(
                name, spec,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
        elif is_sse:
            url = service.get("url")
            if not url:
                raise HTTPException(400, "sse 服务缺少 url")
            plugin = await registry.activate_sse(
                name, url,
                headers=service.get("headers") or {},
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
        else:
            version = await mcp_plugin_store.get_active_version(service_id)
            if not version:
                raise HTTPException(409, "服务没有可用的已发布版本")
            if not version.get("code", "").strip():
                raise HTTPException(400, "版本没有插件代码")
            plugin = await registry.activate_custom(
                name, version["code"], env=env,
                auth_enabled=auth["auth_enabled"],
                allowed_tokens=auth["allowed_tokens"],
            )
    except PluginLoadError as e:
        await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
        raise HTTPException(400, f"插件加载失败: {e}")
    except HTTPException:
        raise
    except Exception as e:
        await mcp_plugin_store.mark_runtime_error(service["id"], str(e))
        raise HTTPException(500, f"重载失败: {e}")

    tools_cache = [
        {"name": t.name, "description": t.description, "input_schema": t.params, "service_name": name}
        for t in plugin.tools
    ]
    version_id = service.get("active_version_id")
    if version_id:
        await mcp_plugin_store.activate_version(int(version_id), tools_cache=tools_cache)
    else:
        await mcp_plugin_store.mark_runtime_status(service_id, "loaded", tools_cache=tools_cache)
    return {"ok": True, "status": "reloaded", "tools": len(plugin.tools)}




async def _dispatch_service_capability(service_id: int, kind: str, key: str, arguments: dict) -> dict:
    service = await mcp_plugin_store.get_service(service_id)
    if not service:
        raise HTTPException(404, "service not found")
    manifest = _manifest_for_service(service)
    declared = ((manifest.get("config") or {}).get(f"{kind}s") or {})
    definition = declared.get(key)
    if not isinstance(definition, dict):
        raise HTTPException(404, f"未知配置{'动作' if kind == 'action' else '视图'}: {key}")
    binding = str(definition.get("binding") or "")
    if not binding or binding.split(".")[-1] != key:
        raise HTTPException(500, f"配置{'动作' if kind == 'action' else '视图'} binding 无效: {binding}")
    try:
        if kind == "action":
            return await registry.call_action(service["name"], key, arguments)
        return await registry.query_view(service["name"], key, arguments)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/services/{service_id}/actions/{action_key}")
async def execute_service_action(service_id: int, action_key: str, request: Request, response: Response):
    await _require_admin(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    payload = await request.json()
    arguments = payload.get("value", payload) if isinstance(payload, dict) else {}
    data = await _dispatch_service_capability(service_id, "action", action_key, arguments)
    return {"data": data, "meta": {"realtime": True}}


@router.get("/services/{service_id}/views/{view_key}")
async def query_service_view(service_id: int, view_key: str, request: Request):
    await _require_admin(request)
    data = await _dispatch_service_capability(service_id, "view", view_key, dict(request.query_params))
    return {"data": data, "meta": {"realtime": True}}
