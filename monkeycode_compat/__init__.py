"""Optional MonkeyCode compatibility services (data service only).

The compatibility layer is disabled by default and must never become a hard
runtime dependency of the model request pipeline. It runs inside the data
service (``main.py``). The node control service is a separate, self-contained
package (:mod:`node_server`) that shares no Python code with this one; the data
service reaches it only over HTTP through :mod:`monkeycode_compat.node_client`.
"""
from __future__ import annotations

from loguru import logger

from .config import settings

_initialized = False


async def init() -> bool:
    """Initialize the data service's compatibility layer when enabled."""
    global _initialized
    if _initialized:
        return True
    if not settings.enabled:
        return False
    if not settings.database_url:
        logger.warning("[monkeycode-compat] enabled but database URL is empty; skipped")
        return False

    try:
        from .database import init_database

        await init_database()
        if settings.user_adapter_enabled:
            from .user_adapter import ensure_system_user

            await ensure_system_user()
        from .user_adapter import seed_bootstrap_admin

        await seed_bootstrap_admin()
        try:
            from agent.sop_service import ensure_builtin_sops

            await ensure_builtin_sops()
        except Exception:
            logger.exception("[monkeycode-compat] built-in SOP provisioning failed")
        from .webhook_review_worker import webhook_review_worker

        await webhook_review_worker.start()
        from .task_runtime_worker import task_runtime_worker

        await task_runtime_worker.start()
        _initialized = True
        logger.info("[monkeycode-compat] initialized")
        return True
    except Exception:
        logger.exception("[monkeycode-compat] initialization failed; main service continues")
        return False


async def close() -> None:
    """Close the data service's compatibility layer."""
    global _initialized
    if not _initialized:
        return
    try:
        from .task_runtime_worker import task_runtime_worker

        await task_runtime_worker.stop()
        from .webhook_review_worker import webhook_review_worker

        await webhook_review_worker.stop()
        from .database import close_database

        await close_database()
    except Exception:
        logger.exception("[monkeycode-compat] shutdown failed")
    finally:
        _initialized = False


def mount_routes(app) -> bool:
    """Mount the data-service surface, called only by ``main.py``.

    Browser/app node routes remain on this public service, but calls behind them
    use the separate control service. This process never builds NodeService or
    holds a live Registry.
    """
    if not settings.enabled:
        return False
    try:
        from .routes import router as user_router
        from .routes_task import router as task_router
        from .routes_task_workspace import router as task_workspace_router
        from .routes_editors import router as editors_router, admin_router as editor_admin_router
        from .routes_editors_workspace import router as editors_workspace_router
        from .routes_project import router as project_router
        from .routes_webhook import router as project_webhook_router
        from .routes_webhook_receive import router as webhook_receive_router
        from .routes_git import identity_router as git_identity_router, bot_router as git_bot_router
        from .routes_mcp_principals import router as mcp_principals_router
        from .routes_skill import skill_router, plugin_router
        from .routes_notify import router as notify_router
        from .routes_user_notifications import router as user_notifications_router
        from .routes_team_notify import router as team_notify_router
        from .routes_server import router as server_router
        from .routes_captcha import router as captcha_router
        from .routes_user_models import models_router as user_models_router
        from .routes_team_users import router as team_users_router
        from .routes_team_models import router as team_models_router
        from .routes_nodes import admin_router as nodes_admin_router, team_router as nodes_team_router
        from .routes_nodes_terminal import router as nodes_terminal_router
        from .routes_nodes_files import (
            router as nodes_files_router,
            team_router as nodes_files_team_router,
        )
        from .routes_node_bootstrap import router as node_bootstrap_router
        from .routes_builtin_tools import router as builtin_tools_router
        from .routes_ios import router as ios_router
        from .routes_build import router as project_build_router, upload_router as project_build_upload_router
        from .routes_project_prompts import (
            admin_router as project_prompts_admin_router,
            user_router as project_prompts_user_router,
        )
        from .routes_resources import router as resource_references_router
        from .routes_environment import router as environment_router
        from .system_env_upload import router as system_env_upload_router
        from .routes_shell_approval import router as shell_approval_router
        from .routes_tunnel import router as tunnel_router
        app.include_router(server_router)
        app.include_router(captcha_router)
        app.include_router(user_models_router)
        app.include_router(user_router)
        app.include_router(task_router)
        app.include_router(task_workspace_router)
        app.include_router(editors_router)
        app.include_router(editors_workspace_router)
        app.include_router(editor_admin_router)
        app.include_router(project_router)
        app.include_router(project_webhook_router)
        app.include_router(webhook_receive_router)
        app.include_router(git_identity_router)
        app.include_router(git_bot_router)
        app.include_router(mcp_principals_router)
        app.include_router(skill_router)
        app.include_router(plugin_router)
        app.include_router(notify_router)
        app.include_router(user_notifications_router)
        app.include_router(team_notify_router)
        app.include_router(team_users_router)
        app.include_router(team_models_router)
        app.include_router(nodes_admin_router)
        app.include_router(nodes_team_router)
        app.include_router(environment_router)
        # Node → server archive ingest for the system-env panel. Token-authenticated
        # (one-time, signed), so it is mounted alongside the user routes rather than
        # behind the session dependency the node cannot satisfy.
        app.include_router(system_env_upload_router)
        app.include_router(nodes_terminal_router)
        app.include_router(nodes_files_router)
        app.include_router(nodes_files_team_router)
        app.include_router(node_bootstrap_router)
        app.include_router(builtin_tools_router)
        app.include_router(ios_router)
        # 项目页「构建」tab：用户路由 + 节点产物公开上传（token 鉴权）。
        app.include_router(project_build_router)
        app.include_router(project_build_upload_router)
        app.include_router(project_prompts_admin_router)
        app.include_router(project_prompts_user_router)
        app.include_router(resource_references_router)
        app.include_router(shell_approval_router)
        app.include_router(tunnel_router)

        # 市场。两组路由都常驻挂载：「看市场」与「管市场」的分界改在请求时判定——
        # - 公开 /status 一直可用，前端据此探测 enabled/writable（导航显隐靠它）。
        # - /admin/* 由 admin_router 的 _require_market_writable 按请求时配置放行，
        #   未配 github_token 时返回 404。挂载时不再按状态取舍：市场配置支持热更新
        #   （资源中心 → 配置，保存即 reload_settings），启动后才配 token 的部署
        #   无需重启即可管理市场。
        try:
            from .marketplace import config as marketplace_config
            from .marketplace import admin_router as marketplace_admin_router
            from .marketplace import router as marketplace_router

            app.include_router(marketplace_router)
            app.include_router(marketplace_admin_router)
            marketplace_settings = marketplace_config.settings
            if marketplace_settings.enabled:
                logger.info(
                    "[marketplace] mounted (writable={})",
                    marketplace_settings.writable,
                )
            else:
                logger.info("[marketplace] mounted but not enabled (no usable repo_url)")
        except Exception:
            logger.exception("[marketplace] route mount skipped")

        # Provider node mode is remote: control owns Registry/NodeConnect; data
        # receives an equivalent streaming response facade over HTTP.
        try:
            from .node_client.proxy import RemoteNodeConnectManager
            from providers.proxy_manager import set_node_manager

            set_node_manager(
                RemoteNodeConnectManager(
                    base_url=settings.agent_compose_base_url,
                    token=settings.node_control_token,
                    timeout=max(1, settings.agent_compose_timeout),
                )
            )
        except Exception:
            logger.exception("[monkeycode-compat] remote node proxy setup failed")

        # Preserve Connect server-stream framing while forwarding
        # FollowNodeSession to the control process.
        try:
            from starlette.routing import Route
            from .node_client.follow import proxy_follow_asgi

            class _FollowProxyASGI:
                async def __call__(self, scope, receive, send):
                    if scope.get("type") != "http":
                        return
                    await proxy_follow_asgi(
                        scope,
                        receive,
                        send,
                        base_url=settings.agent_compose_base_url,
                        token=settings.node_control_token,
                    )

            app.router.routes.insert(
                0,
                Route(
                    "/agentcompose.v2.NodeService/FollowNodeSession",
                    _FollowProxyASGI(),
                    methods=["POST"],
                ),
            )

            # Same Connect server-stream passthrough, for FollowToolRun (tunnel
            # manager: captures cloudflared's trycloudflare domain from stdout).
            from .node_client.follow_toolrun import proxy_follow_toolrun_asgi

            class _FollowToolRunProxyASGI:
                async def __call__(self, scope, receive, send):
                    if scope.get("type") != "http":
                        return
                    await proxy_follow_toolrun_asgi(
                        scope,
                        receive,
                        send,
                        base_url=settings.agent_compose_base_url,
                        token=settings.node_control_token,
                    )

            app.router.routes.insert(
                0,
                Route(
                    "/agentcompose.v2.NodeService/FollowToolRun",
                    _FollowToolRunProxyASGI(),
                    methods=["POST"],
                ),
            )
        except Exception:
            logger.exception("[monkeycode-compat] follow proxy mount failed")

        logger.info("[monkeycode-compat] platform routes mounted")
        return True
    except Exception:
        logger.exception("[monkeycode-compat] route mount failed; main service continues")
        return False


__all__ = ["close", "init", "mount_routes", "settings"]
