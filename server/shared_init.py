"""共享服务初始化（主服务与可选独立 MCP Runtime 进程调用）。

把 main.py lifespan 里的 DB / redis / config_store 初始化提取为可复用函数，
避免各入口重复实现导致漂移。
"""
from __future__ import annotations

from loguru import logger


async def init_shared_services(lightweight: bool = False) -> None:
    """初始化 Postgres 连接池 + redis + config_store。

    供主服务 lifespan 与可选独立 MCP Runtime 进程共用。

    lightweight=True（MCP Runtime）：DB 只建表、跳过回填/规范化/主服务迁移
    （见 PostgresClient.init）。连接池、config_store、redis 仍照常初始化，因为
    MCP Runtime 需要插件存储、配置与鉴权依赖。主服务用默认完整模式。
    """
    from db import PostgresClient
    import config
    from rd import JdbcClient

    postgres_ready = False
    try:
        await PostgresClient.init(lightweight=lightweight)
        postgres_ready = True
        logger.debug("[shared_init] postgres db init success")
        if hasattr(config.CONFIG_STORE, "init"):
            await config.CONFIG_STORE.init()
        await JdbcClient.ping()
        logger.debug("[shared_init] redis db init success")
    except Exception:
        if postgres_ready:
            await PostgresClient.close()
        raise

    # 内部服务密钥：sse_gateway 代理网页对话面板回环调 /agent/client/* 时用。
    # 主服务与可选独立 MCP Runtime 共享同一 DB config store，这里统一自动置备，
    # 避免要求手动设置环境变量导致 token 未配置而对话报错。
    try:
        await _ensure_agent_internal_token(config)
    except Exception:
        logger.exception("[shared_init] ensure AGENT_INTERNAL_TOKEN failed; webpage chat proxy may be unavailable")

    # Optional upstream compatibility layer (disabled by default). Failure
    # here must never block the main service; it is best-effort only.
    try:
        import user_platform

        await user_platform.init()
    except Exception:
        logger.exception("[shared_init] user-platform init failed; main service continues")


async def _ensure_agent_internal_token(config) -> None:
    """确保 AGENT_INTERNAL_TOKEN 在本进程 env 中就绪，并与共享配置一致。

    优先级：已有环境变量 > 共享配置已存值 > 新生成。生成/采用后写回共享配置，
    使主服务与可选独立 MCP Runtime 进程收敛到同一个 token。
    """
    import os
    import secrets

    env_token = (os.environ.get("AGENT_INTERNAL_TOKEN") or "").strip()
    main_config = await config.CONFIG_STORE.read_main_async()
    stored = str((main_config.get("agent_internal") or {}).get("token") or "").strip()

    token = env_token or stored or ("agi_" + secrets.token_urlsafe(32))
    if token != stored:
        updated = dict(main_config)
        updated["agent_internal"] = {**(main_config.get("agent_internal") or {}), "token": token}
        await config.CONFIG_STORE.write_main_async(updated)
    os.environ["AGENT_INTERNAL_TOKEN"] = token
