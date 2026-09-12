import asyncio
import aiohttp
import base64
import hashlib
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from collections import defaultdict, deque
from pathlib import Path

# 业务模块收在 server/ 目录下（根目录只保留入口与本文件绑定的配置）。
# 任何子进程入口(main/init_db/tunnel_server)都必须在最早处把 server/ 注入 sys.path，
# 这样原有的 `import channel` / `from db import ...` 等平铺 import 无需改动。
import sys as _sys

_sys.path.insert(0, str(Path(__file__).resolve().parent / "server"))

from dotenv_loader import load_project_env

load_project_env()

from typing import Optional
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response, StreamingResponse, JSONResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
import config
import model_catalog
import model_metadata
from channel import chat_protocols_from_legacy, _LEGACY_CHAT_KEYS
from admin import router as admin_router, loopback_router as admin_loopback_router
from providers import CustomProvider, EdgeOneAIProvider
from providers.cloudflare import CloudflareProvider
from providers.base import IncompleteStreamError, EmptyNonStreamResponseError, normalize_upstream_error_message
from proxy_utils import runtime_accounts, to_manager_config
from providers.proxy_manager import (
    get_proxy_manager,
    reset_outbound_proxy,
    take_outbound_proxy,
)
from rate_limiter import (
    RateLimiter,
    ModelClientPool,
    NoAvailableAccountError,
    begin_routing_timing,
    end_routing_timing,
)
from limits.manager import LimitManager, begin_routing_redis_scope, end_routing_redis_scope, routing_redis_state
from message_utils import anthropic_to_openai_messages, openai_to_anthropic_response, convert_stream_to_anthropic, responses_to_openai_messages, openai_to_responses_response, iter_sse_payloads
from rd import JdbcClient
from db import PostgresClient
from request_log_writer import request_log_writer


# 文件日志目录。默认仓库内 logs/；releases+current 布局下经 LOG_DIR 指到跨版本
# 共享目录（shared/logs），避免随旧 release 被 GC 清掉。
_LOG_DIR = Path(os.environ.get("LOG_DIR") or Path(__file__).resolve().parent / "logs")
_file_log_sink_id = None


def _configure_file_logging() -> None:
    """Add the process log file sink once during application startup."""
    global _file_log_sink_id
    if _file_log_sink_id is not None:
        return
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    _file_log_sink_id = logger.add(
        str(_LOG_DIR / "ai-lubricant-{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=False,
    )
from usage_utils import normalize_usage, usage_value, estimate_usage, set_tokenizer_rules_getter, estimate_request_part_tokens, estimate_request_part_tokens as _estimate_request_part_tokens, estimate_input_tokens, estimate_input_token_parts, request_input_parts, estimate_request_tokens
from shared_init import init_shared_services
from agent.config import AgentConfig
from agent.runner import AgentTaskRunner
import attempt_builder
import retry_policy
from request_reservations import ApiKeyReservation, CandidateReservation
from request_state import RequestContext, AttemptContext, CandidateKey


def _sync_proxy_manager_configs(proxies: list[dict]) -> None:
    """把代理池全量喂给共享 ProxyManager：provider 出站按 proxy_config_id 路由，
    这些配置就是路由依据。update_proxy_config 内容变自动递增 version、触发旧实例
    重建，故热更新（改代理 url/密码/模式）直接再调本函数即可及时生效。"""
    manager = get_proxy_manager()
    for proxy in proxies or []:
        cfg = to_manager_config(proxy)
        if cfg.get("id"):
            manager.update_proxy_config(cfg)


def _unsupported_snapshot_keyword(exc: TypeError) -> bool:
    message = str(exc)
    return "unexpected keyword argument" in message and "snapshot" in message


async def _catalog_call(callable_, *args, snapshot):
    """Call a snapshot-aware catalog API, tolerating legacy test monkeypatches only."""
    try:
        return await callable_(*args, snapshot=snapshot)
    except TypeError as exc:
        if not _unsupported_snapshot_keyword(exc):
            raise
        return await callable_(*args)


def _merge_default_request_params(*sources: dict | None) -> dict:
    merged: dict = {}
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            if value is None:
                continue
            canonical_key = "max_tokens" if key in ("max_token", "max_completion_tokens", "max_output_tokens") else key
            merged[canonical_key] = value
    return merged


async def _request_defaults_from_metadata(model: str, *, snapshot=None) -> dict:
    metadata, _ = await model_metadata.get_model_metadata(model, snapshot=snapshot)
    defaults = metadata.get("default_parameters") or metadata.get("parameters") or {}
    defaults = _merge_default_request_params(defaults if isinstance(defaults, dict) else {})
    try:
        max_tokens = int(metadata.get("max_tokens") or 0)
    except (TypeError, ValueError):
        max_tokens = 0
    if max_tokens > 0 and defaults.get("max_tokens") is None:
        defaults = {**defaults, "max_tokens": max_tokens}
    return dict(defaults)


def _apply_request_defaults(kwargs: dict, defaults: dict | None) -> dict:
    if not isinstance(defaults, dict) or not defaults:
        return kwargs
    for key, value in defaults.items():
        if value is None or str(key).startswith("_"):
            continue
        if kwargs.get(key) is None:
            kwargs[key] = value
    return kwargs


async def _apply_global_request_defaults(model: str, kwargs: dict, *, snapshot=None) -> dict:
    return _apply_request_defaults(
        kwargs,
        await _request_defaults_from_metadata(model, snapshot=snapshot),
    )

BASE_DIR = Path(__file__).resolve().parent


# Token 使用统计
_token_stats: dict[str, dict] = defaultdict(lambda: {"total_tokens": 0, "prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0, "cache_creation_tokens": 0, "requests": 0})

# 最近请求日志（环形缓冲区，大小从配置读取）
_recent_logs: deque[dict] = deque()
_MAX_LOG_ENTRIES = 200  # 可通过配置修改


def _add_recent_log(api_key: str, model: str, endpoint: str, status: str, prompt_tokens: int = 0, completion_tokens: int = 0, cached_tokens: int = 0, cache_creation_tokens: int = 0, total_tokens: int | None = None, duration_ms: int = 0, error: str = ""):
    """添加一条请求日志，超过上限时剔除旧数据"""
    global _MAX_LOG_ENTRIES
    normalized_total = int(total_tokens) if total_tokens and int(total_tokens) > 0 else prompt_tokens + completion_tokens
    _recent_logs.append({
        "time": time.time(),
        "api_key": api_key,
        "model": model,
        "endpoint": endpoint,
        "status": status,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "cached_tokens": cached_tokens,
        "cache_creation_tokens": cache_creation_tokens,
        "total_tokens": normalized_total,
        "duration_ms": duration_ms,
        "error": error,
    })
    while len(_recent_logs) > _MAX_LOG_ENTRIES:
        _recent_logs.popleft()


def update_token_stats(api_key: str, prompt_tokens: int, completion_tokens: int, cached_tokens: int = 0, cache_creation_tokens: int = 0, total_tokens: int | None = None):
    """更新 token 统计"""
    stats = _token_stats[api_key]
    stats["total_tokens"] += int(total_tokens) if total_tokens and int(total_tokens) > 0 else prompt_tokens + completion_tokens
    stats["prompt_tokens"] += prompt_tokens
    stats["completion_tokens"] += completion_tokens
    stats["cached_tokens"] += cached_tokens
    stats["cache_creation_tokens"] += cache_creation_tokens
    stats["requests"] += 1


async def _migrate_deprecated_builtin_channels(provider_configs: dict) -> None:
    """把已下架的 CLI 逆向渠道旧行迁移成空代码渠道。

    规范行有两种形态：codebuddy 等模板派生行靠 builtin_type 认领；旧的 canonical
    copilot/atomcode/eaichat 行靠渠道名认领且没有 builtin_type。两者都要覆盖。
    """
    deprecated = {"copilot", "codebuddy", "atomcode", "eaichat", "qoder"}
    for provider_name, provider_config in list(provider_configs.items()):
        if not isinstance(provider_config, dict):
            continue
        builtin_type = provider_config.get("builtin_type") or ""
        legacy_key = builtin_type or provider_name
        if legacy_key not in deprecated:
            continue
        migrated = dict(provider_config)
        migrated["builtin_type"] = "code"
        migrated["code"] = ""
        provider_configs[provider_name] = migrated
        try:
            await config.CONFIG_STORE.write_provider_async(provider_name, migrated)
            logger.warning(
                f"渠道 {provider_name} 的 builtin_type={builtin_type!r} 已下架，"
                f"已迁移为代码渠道（code 为空）。请在管理端「源码」Tab 贴上对应 spec 后保存。"
            )
        except Exception as exc:
            logger.error(f"渠道 {provider_name} 下架迁移写回失败: {exc}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """应用生命周期"""

    _configure_file_logging()
    logger.info(f"文件日志已启用: {_LOG_DIR}")
    logger.debug(f"Starting lifespan")

    # 附件签名 URL 的 HMAC 密钥：缺失即生成随机值并写回 .env，不静默降级。
    try:
        import attachment_signing
        attachment_signing.ensure_signing_key()
    except Exception:
        logger.exception("[attachment] signing key bootstrap failed; signed URLs will fall back to login-state")

    await init_shared_services()
    try:
        from db import PostgresClient
        await PostgresClient.reconcile_stale_request_logs(max_age_minutes=10, batch_size=1000)
    except Exception:
        logger.exception("[request-log-reconciler] startup reconciliation failed")
    try:
        from request_payload_writer import payload_writer
        from clickhouse_config import get_settings
        ch = get_settings()
        if ch.enabled:
            payload_writer.start()
    except Exception:
        logger.exception("[request-payload-writer] startup failed; main service continues")
    # 撤销/异常请求的 API Key token 计费：成功请求的计费已由端点 _finalize_request_success_stats
    # 同步完成；worker 写完 finalized 行后，对 success=false 且有产出（completion_tokens>0）
    # 的行补记 API Key 账本。不调渠道 token——渠道已在 keep_served_usage 结算，避免双计。
    async def _on_request_log_finalized(payload: dict) -> None:
        if payload.get("success"):
            return
        api_key = payload.get("api_key")
        if not api_key:
            return
        completion = int(payload.get("completion_tokens") or 0)
        if completion <= 0:
            return  # 无产出（撤销/失败且未服务）不误记
        prompt = int(payload.get("prompt_tokens") or 0)
        cached = int(payload.get("cached_tokens") or 0)
        cache_creation = int(payload.get("cache_creation_tokens") or 0)
        total = int(payload.get("total_tokens") or 0) or (prompt + completion)
        await _record_api_key_usage(api_key, {
            "total_tokens": total, "prompt_tokens": prompt, "completion_tokens": completion,
        })
        update_token_stats(api_key, prompt, completion, cached, cache_creation, total)

    # 预占恢复：先声明本进程存活，再回滚心跳失效进程遗留的 Redis ledger。
    # 请求日志的 requesting 对账已在上方完成；两类启动恢复都结束后才启动请求日志队列。
    # 先等一个完整心跳 TTL，避免多实例同时滚动启动时把刚停旧实例的在途请求误判为孤儿。
    # 后台 reaper 会在 TTL 后自动接管；显式恢复动作仍发生在进入长期 queue wait 之前。
    await LimitManager.start_heartbeat()

    request_log_writer.set_on_finalized(_on_request_log_finalized)
    request_log_writer.start()

    # Agent/聊天对话存储（ClickHouse）。与 payload 双写独立：用单独的客户端实例
    # 服务请求处理中的读后即写，不共用后台 writer 的连接。未启用时对话端点会 503。
    try:
        from clickhouse_config import get_settings as _ch_settings
        from integrations.clickhouse import ClickHousePayloadClient
        from agent import conversation_store
        _ch = _ch_settings()
        if _ch.enabled and _ch.addr:
            _conv_client = ClickHousePayloadClient(
                addr=_ch.addr, database=_ch.database,
                username=_ch.username, password=_ch.password,
            )
            await _conv_client.connect()
            await _conv_client.ensure_conversation_schema(ttl_days=365)
            conversation_store.attach_client(_conv_client)
            await conversation_store.seed_message_id()
            logger.info("[conversation_store] ClickHouse ready, schema ensured")
            # 任务详情页对话内容表。与 agent 对话同库，借用同一个客户端：
            # 状态/索引留 Postgres mc_task_events，内容 append 到 CH task_messages。
            # 未启用 CH 时任务侧自动回落到 PG payload 列（见 user_platform/
            # task_message_store.py），只是没有 append-only 帧轨迹。
            try:
                from user_platform import task_message_store as _task_msgs
                await _conv_client.ensure_task_messages_schema()
                _task_msgs.attach_client(_conv_client)
                logger.info("[task-messages] ClickHouse ready, schema ensured")
            except Exception:
                logger.exception("[task-messages] init failed; task content stays in postgres")
            try:
                reaped = await conversation_store.reap_stale_streaming_messages()
                if reaped:
                    logger.info(
                        "[conversation_store] reaped {} stale streaming message(s) left by prior restart",
                        reaped,
                    )
            except Exception:
                logger.exception("[conversation_store] stale streaming reap failed")
        else:
            logger.warning("[conversation_store] ClickHouse not enabled; agent/chat conversations unavailable")
    except Exception:
        logger.exception("[conversation_store] init failed; agent/chat conversations unavailable")

    # upstream 兼容层初始化（建 mc_* 表 + 可选系统用户）。默认关闭；
    # compat_enabled=false 时 init() 直接跳过，失败只 log 不影响主服务。
    try:
        import user_platform
        await user_platform.init()
    except Exception:
        logger.exception("[user-platform] init failed; main service continues")

    # 通知出站 worker：回灌未推送 outbox → set Event → 消费循环。独立于 compat 开关，
    # 用 db pool 原生 SQL（node-server 进程也能 emit，主进程统一消费推送）。
    try:
        from user_platform import notify_core
        await notify_core.start()
    except Exception:
        logger.exception("[notify] outbox worker start failed; outbound notifications unavailable")

    # Agent MCP principal 启动对账：保证每个 Agent 绑定 usage_type='agent' 的 principal，
    # 含历史 Agent 改绑到正确类型。在 mcp_users 表就绪后执行；失败只 log 不阻断启动。
    try:
        from agent.api import reconcile_agent_mcp_principals
        await reconcile_agent_mcp_principals()
    except Exception:
        logger.exception("[agent] startup principal reconcile failed; agents without bound principal will lack MCP")

    # 先订阅、再全量加载：注册 pubsub handler + 启动订阅/对账，避免加载空窗期漏消息。
    import runtime_sync
    import runtime_handlers
    runtime_handlers.register_all()
    await runtime_sync.start()

    await model_metadata.init_cache()
    # 元数据迁移完成后发布首个一致目录快照；provider 注册/初始化只能看到该版本。
    await model_catalog.reload_from_db(reason="startup")

    # 模型规则模版缓存要在渠道配置之前灌好：渠道的 model_id 改写条目可能引用模版，
    # 缓存为空时引用会被当成「模版不存在」跳过。对账循环是先 sleep 再跑，指望它
    # 兜底会留下 60s 空窗——启动时首轮拉模型正好落在这个窗口里。
    import channel as channel_module
    try:
        channel_module.set_model_rule_template_cache(await PostgresClient.get_model_rule_templates())
    except Exception:
        logger.exception("[startup] 预载模型规则模版失败；模版引用将在首次对账后生效")

    # 灌渠道配置内存快照（get_providers 之后全部读内存，不再查库）。
    await config.Config.reload_from_db()

    # 灌 API Key 内存快照：鉴权是每请求必经路径，校验走内存不查库。必须在
    # runtime_sync 已订阅 EVENT_APIKEY 之后灌，后续写路径广播与 60s 对账才能
    # 增量维护这份快照；否则首轮对账前的请求会拿到空快照、把合法 Key 误判 401。
    await config.Config.refresh_api_keys_cache()

    # 渠道目录先加载数据库最后成功快照，再由后台任务预同步 GitHub；添加渠道只读本地目录。
    from user_platform.marketplace import channel_catalog
    from admin import _channel_catalog_builtin_entries
    channel_catalog.configure_builtin_loader(_channel_catalog_builtin_entries)
    await channel_catalog.load_snapshot()
    channel_catalog_task = asyncio.create_task(channel_catalog.sync_loop(), name="channel-catalog-sync")

    # 节点发行版本快照：先加载 DB 里的上次成功快照，再由后台任务预同步 GitHub
    # node-releases/version.json；节点详情据此判断是否需要升级。
    import node_release_catalog
    await node_release_catalog.load_snapshot()
    node_release_task = asyncio.create_task(node_release_catalog.sync_loop(), name="node-release-sync")

    # 移动端（控制 App）发行版本快照：与节点同构，先加载 DB 上次快照，再后台预同步
    # GitHub mobile-releases/version.json；App「检查更新」据此判断是否需要升级。
    import mobile_release_catalog
    await mobile_release_catalog.load_snapshot()
    mobile_release_task = asyncio.create_task(mobile_release_catalog.sync_loop(), name="mobile-release-sync")

    # 设备控制 App（被控端）发行版本快照：与移动端同构，先加载 DB 上次快照，再后台
    # 预同步 GitHub device-control-releases/version.json；「添加设备」弹框据此拿下载直链。
    import device_control_release_catalog
    await device_control_release_catalog.load_snapshot()
    device_control_release_task = asyncio.create_task(
        device_control_release_catalog.sync_loop(), name="device-control-release-sync"
    )

    # WDA 自动续签扫描器：每 6h 扫一遍已认领 iOS 设备，到期窗口内自动派发 renew
    # job（护栏：data.ios.last_auto_renew_at 12h 防风暴 + running snapshot 幂等）。
    # compat 关闭时循环内部自查退出。
    from user_platform import ios_auto_renew
    ios_wda_renew_task = asyncio.create_task(
        ios_auto_renew.sync_loop(), name="ios-wda-auto-renew"
    )

    # 外部榜单候选池：默认关闭的 opt-in 同步。循环内部先查开关，未配置市场管理或
    # 未打开 leaderboard_sync_enabled 的部署只是空转复查配置，不抓取、不写库。
    from user_platform.marketplace import leaderboard_sync
    leaderboard_sync_task = asyncio.create_task(
        leaderboard_sync.sync_loop(), name="leaderboard-sync"
    )

    # 内容源定时同步（agency-agents / agency-agents-zh / agentscope / skillhub）：与
    # leaderboard 同款 asyncio 后台循环。各源的 enabled/interval_hours 在市场管理配置里；
    # 默认全关，未打开时空转每小时复查，不抓取。已在跑时由模块级 _progress.running 挡重入。
    from user_platform.marketplace import agency_agents_convert, agentscope_convert, skillhub_convert
    agency_agents_sync_task = asyncio.create_task(
        agency_agents_convert.sync_loop(), name="agency-agents-sync"
    )
    agency_agents_zh_sync_task = asyncio.create_task(
        agency_agents_convert.sync_loop(repo="jnMetaCode/agency-agents-zh"), name="agency-agents-zh-sync"
    )
    agentscope_sync_task = asyncio.create_task(
        agentscope_convert.sync_loop(), name="agentscope-sync"
    )
    skillhub_sync_task = asyncio.create_task(
        skillhub_convert.sync_loop(), name="skillhub-sync"
    )

    # 消费侧市场读缓存：索引与根 marker 后台预热，单条 manifest read-through；
    # 管理端写路径立即失效。首轮即预热，启动不阻塞（冷启动首请求与现拉行为一致）。
    from user_platform.marketplace import consumer_cache
    consumer_cache_task = asyncio.create_task(
        consumer_cache.sync_loop(), name="marketplace-consumer-cache-sync"
    )

    # 市场发布 outbox worker + 一次性 bootstrap：PG（marketplace_items）是编辑真相源，
    # 后台 publisher 把当前 store 状态异步镜像到 GitHub（外部消费者读仓库 raw）。
    # 只读部署（未配 token）两个任务内部自判退出。bootstrap 先于 publisher 数据依赖，
    # 但二者都是幂等异步任务，无需严格先后——读路径在 store 空时回落旧 GitHub 读取。
    from user_platform.marketplace import bootstrap as marketplace_bootstrap
    from user_platform.marketplace import publisher as marketplace_publisher
    marketplace_bootstrap_task = asyncio.create_task(
        marketplace_bootstrap.bootstrap_once(), name="marketplace-bootstrap"
    )
    marketplace_publisher_task = asyncio.create_task(
        marketplace_publisher.worker_loop(), name="marketplace-publisher"
    )

    # 回收上次进程崩溃/重启遗留的上传暂存目录：第①阶段收文件写盘半截就崩，
    # cleanup_job 没被调用，2GB 的半截文件留在 temp 里把磁盘吃满。内存 _jobs
    # 重启即空，孤儿目录只能靠启动时扫一遍暂存根目录回收（运行时再由 _maybe_sweep 兜底）。
    try:
        from user_platform.marketplace import upload_jobs
        upload_jobs.sweep_orphan_dirs()
    except Exception:
        logger.exception("[upload-jobs] orphan temp sweep failed")

    # Tokenizer 词表版本检查：只发 HEAD 比对 ETag 并记录状态，绝不自动下载。
    # 词表可达数十 MB，而估算函数在请求热路径上，下载必须由管理员手动触发。
    import tokenizer_vocab_check
    tokenizer_vocab_task = asyncio.create_task(
        tokenizer_vocab_check.check_loop(), name="tokenizer-vocab-check"
    )

    # 启动时从 PG 预载所有内置 HF 词表到进程内存，避免首次估算时联网或读文件。
    # 这是一个 await 调用，但只在启动时执行一次，词表数量有限（<20）。
    import usage_utils
    try:
        # 一次性迁移：本地磁盘已有词表先导入 PG（幂等），再从 PG 预载。
        await usage_utils.migrate_local_vocabs_to_pg()
        await usage_utils.load_hf_tokenizers_from_pg()
    except Exception as e:
        logger.warning(f"[startup] 预载 HF 词表失败，估算将降级: {type(e).__name__}: {e}")

    # 注册内置提供商：只保留走官方公开 API 的 Cloudflare / EdgeOne。
    # copilot/codebuddy/atomcode/eaichat/qoder 已下架为「代码渠道」形态——
    # 见下方 _DEPRECATED_BUILTIN_TYPES 迁移，spec 源码由使用者自行粘贴。
    ModelClientPool.register_provider("cloudflare", CloudflareProvider)
    ModelClientPool.register_provider("edgeone-ai", EdgeOneAIProvider)

    provider_configs = await config.Config.get_providers()

    # 一次性迁移：旧渠道只有顶层 protocol/chat_path 而无 chat_protocols 时，合成一条协议行写回 DB。
    # 迁移后单一事实来源统一为 chat_protocols；顶层旧字段被移除。
    for provider_name, provider_config in list(provider_configs.items()):
        if not isinstance(provider_config, dict):
            continue
        existing = provider_config.get("chat_protocols")
        has_rows = isinstance(existing, list) and any(isinstance(x, dict) for x in existing)
        has_legacy = any(k in provider_config for k in _LEGACY_CHAT_KEYS)
        if has_rows and not has_legacy:
            continue
        rows = chat_protocols_from_legacy(provider_config)
        migrated = dict(provider_config)
        if rows:
            migrated["chat_protocols"] = rows
        for legacy_key in _LEGACY_CHAT_KEYS:
            migrated.pop(legacy_key, None)
        if migrated == provider_config:
            continue
        provider_configs[provider_name] = migrated
        try:
            await config.CONFIG_STORE.write_provider_async(provider_name, migrated)
            logger.info(f"渠道 {provider_name} 已迁移顶层协议字段到 chat_protocols")
        except Exception as exc:
            logger.warning(f"渠道 {provider_name} 协议字段迁移写回失败: {exc}")

    # 一次性迁移：CLI 逆向内置渠道（copilot/codebuddy/atomcode/eaichat/qoder）
    # 已下架为「代码渠道」形态——产品只发框架能力，spec 源码由使用者自行粘贴与分发。
    # 旧行的 builtin_type 改写成 "code"、code 留空：渠道进入代码渠道编辑器形态，
    # 用户贴上 spec 后保存即恢复。code 为空时下面加载分支会跳过并记一条 error。
    await _migrate_deprecated_builtin_channels(provider_configs)

    # 注册自定义渠道 / 基于内置模板创建的派生渠道（排除已内置的固定名称）
    BUILTIN_PROVIDERS = {"cloudflare", "edgeone-ai"}
    BUILTIN_PROVIDER_CLASSES = {
        "cloudflare": CloudflareProvider,
        "edgeone-ai": EdgeOneAIProvider,
    }
    # builtin_type 已删除但旧行仍在的渠道只 warn 一次，按 builtin_type 记名去重
    _warned_deprecated_builtin_types = set()
    for provider_name, provider_config in provider_configs.items():
        if provider_name in BUILTIN_PROVIDERS:
            continue
        builtin_type = provider_config.get("builtin_type") or ""
        builtin_class = BUILTIN_PROVIDER_CLASSES.get(builtin_type)
        if builtin_class:
            ModelClientPool.register_provider(provider_name, builtin_class)
            continue
        # 代码渠道：贴的 Python 源码 exec 出 spec 类，包成 CustomProvider 子类适配器。
        # 启动加载即跑，后续 CRUD 热更新走 admin._get_provider_class 同一 loader。
        if builtin_type == "code" and provider_config.get("code"):
            from providers.code_loader import load_code_provider_class, CodeChannelError
            try:
                code_cls = load_code_provider_class(provider_name, provider_config["code"])
            except CodeChannelError as exc:
                logger.error(f"代码渠道 {provider_name} 加载失败，跳过: {exc}")
                continue
            ModelClientPool.register_provider(provider_name, code_cls)
            continue
        # builtin_type 非空但对应类已删除（如用户尚未清理的 qwen/gemini 旧行）：
        # 兜底走 CustomProvider，用 base_url/chat_protocols 继续服务，避免启动即崩。
        if builtin_type:
            if builtin_type not in _warned_deprecated_builtin_types:
                _warned_deprecated_builtin_types.add(builtin_type)
                logger.warning(
                    f"渠道 {provider_name} 的 builtin_type={builtin_type!r} 已下线，"
                    f"回退为通用自定义渠道；请在管理端删除或重新保存该行。"
                )
            ModelClientPool.register_provider(provider_name, CustomProvider)
            continue
        if provider_config.get("type") == "custom" or provider_config.get("custom_channel"):
            ModelClientPool.register_provider(provider_name, CustomProvider)

    # 添加账号：渠道禁用只影响发送消息路由，不影响账号加载/认证/初始化/测试/模型同步。
    main_config = config.Config._load()
    proxies = main_config.get("proxies", [])
    # 先把代理池全量喂给共享 ProxyManager，再加账号：保证 provider 出站收口到
    # ProxyManager 时路由配置已就绪，network/url_prefix 账号不会因缺配置降级直连。
    _sync_proxy_manager_configs(proxies)
    for provider_name, provider_config in provider_configs.items():
        accounts = runtime_accounts(provider_config.get("accounts", []), proxies)
        await ModelClientPool.add_accounts(provider_name, accounts)

    # 初始化账号与模型管理器
    await ModelClientPool.initialize()

    logger.info("服务启动完成")

    # 启动定时任务调度器
    try:
        main_cfg = await config.CONFIG_STORE.read_main_async() if hasattr(config.CONFIG_STORE, "read_main_async") else config.Config._load()
        agent_cfg = AgentConfig.from_dict(main_cfg.get("agent", {}))
    except Exception:
        agent_cfg = AgentConfig()
    AgentTaskRunner.start(config=agent_cfg)

    # GA chapter6.1/6.3 后台 worker：reflect 脚本热重载 + 自主 TODO 闲置触发。
    # 每个 enabled Agent 一个 ReflectWorker（无脚本即空转），autonomous_enabled 的
    # Agent 额外挂一个 AutonomousWorker。扫描周期 60s，dispatch 走新建 GenericAgent。
    try:
        from agent.background_workers import AgentBackgroundOrchestrator
        await AgentBackgroundOrchestrator.start(poll_interval=60.0)
    except Exception:
        logger.exception("[agent-bg] orchestrator start failed; reflect/autonomous unavailable")

    # 启动 device_code 授权中央扫描器
    from admin import start_auth_scanner, stop_auth_scanner, start_daily_auth_refresh, stop_daily_auth_refresh
    await start_auth_scanner()
    # 启动每日 10:00 授权定时刷新（SCHEDULED_REFRESH=True 的渠道，如 atomcode / 声明了 refresh_auth 的代码渠道）
    await start_daily_auth_refresh()

    # MCP Runtime 已合并进主程序：进程内恢复活动插件（builtin 常驻 + custom 热加载），
    # 注册进 registry 内存单例。不再拉起独立子进程/回环。失败不阻断主服务启动。
    try:
        from mcp_runtime.startup import restore_active_plugins
        await restore_active_plugins()
    except Exception:
        logger.exception("[mcp-runtime] restore_active_plugins failed; MCP 能力可能不可用")

    # Start a small ledger reaper for editor sessions. Runtime release is best
    # effort; the DB transition is authoritative and preserves history.
    editor_session_reaper: asyncio.Task | None = None

    async def _reap_editor_sessions() -> None:
        interval = max(60, int(os.environ.get("EDITOR_SESSION_REAPER_INTERVAL", "300")))
        pending_timeout = max(60, int(os.environ.get("EDITOR_SESSION_PENDING_TIMEOUT", "1800")))
        idle_timeout = max(300, int(os.environ.get("EDITOR_SESSION_IDLE_TIMEOUT", "86400")))
        while True:
            try:
                await asyncio.sleep(interval)
                from user_platform.node_client import get_local_node_client

                closed = await PostgresClient.reap_stale_editor_sessions(
                    pending_timeout_seconds=pending_timeout,
                    idle_timeout_seconds=idle_timeout,
                )
                client = get_local_node_client()
                for session in closed:
                    node_session_id = (session.get("node_session_id") or "").strip()
                    if not node_session_id:
                        continue
                    try:
                        await client.delete_node_session(node_session_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("[editor-session-reaper] release {} failed: {}", node_session_id, exc)
                if closed:
                    logger.info("[editor-session-reaper] closed {} stale sessions", len(closed))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[editor-session-reaper] cycle failed: {}", exc)

    editor_session_reaper = asyncio.create_task(_reap_editor_sessions(), name="editor-session-reaper")

    # Keep local group grants aligned with the authoritative node control ledger.
    # Immediate mutation paths clean bindings too; this catches external deletes,
    # interrupted requests, and historical orphan rows.
    node_binding_reconciler: asyncio.Task | None = None

    async def _reconcile_group_node_bindings() -> None:
        interval = max(60, int(os.environ.get("NODE_BINDING_RECONCILE_INTERVAL", "300")))
        while True:
            try:
                await asyncio.sleep(interval)
                from user_platform.node_client import get_local_node_client
                from user_platform.nodes_service import nodes_service

                if not get_local_node_client().enabled:
                    continue
                await nodes_service.reconcile_bindings()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[node-binding-reconciler] cycle failed: {}", exc)

    node_binding_reconciler = asyncio.create_task(
        _reconcile_group_node_bindings(), name="node-binding-reconciler"
    )

    # Agent 附件生命周期：active 过期 → expired（留续期窗口，不删文件）；
    # expired 超过宽限期 → purged + 删 object_store 物理文件（workspace_ref 不删，
    # 那是 agent 自己的工作区文件）。幂等，见 attachment_store.sweep_expired。
    attachment_sweeper: asyncio.Task | None = None

    async def _sweep_attachments() -> None:
        interval = max(300, int(os.environ.get("ATTACHMENT_SWEEP_INTERVAL", "3600")))
        while True:
            try:
                await asyncio.sleep(interval)
                import attachment_store
                await attachment_store.sweep_expired()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[attachment-sweeper] cycle failed: {}", exc)

    attachment_sweeper = asyncio.create_task(_sweep_attachments(), name="attachment-sweeper")

    orphan_reservation_reaper: asyncio.Task | None = None

    async def _reap_orphan_reservations_loop() -> None:
        interval = max(60, int(os.environ.get("ORPHAN_RESERVATION_REAP_INTERVAL", "60")))
        while True:
            try:
                await asyncio.sleep(interval)
                await LimitManager.reap_orphan_reservations()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("[limit-reservation] orphan reap cycle failed: {}", exc)

    orphan_reservation_reaper = asyncio.create_task(
        _reap_orphan_reservations_loop(), name="orphan-reservation-reaper",
    )

    loop_lag_probe: asyncio.Task | None = None

    async def _loop_lag_probe() -> None:
        # 事件循环阻塞探针：单进程单事件循环下，任何同步阻塞（同步 IO、CPU 密集、
        # 大 json.dumps、time.sleep）会拖住整个循环，毫秒级接口偶发超时就是签名
        # （[req] 平时 7ms 偶尔几百 ms）。每秒心跳一次，实际唤醒晚于预期即说明循环
        # 被占。阈值 LOOP_LAG_THRESHOLD_MS（默认 200ms）。
        threshold_ms = max(50, int(os.environ.get("LOOP_LAG_THRESHOLD_MS", "200")))
        while True:
            t = time.perf_counter()
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            lag_ms = (time.perf_counter() - t - 1.0) * 1000
            if lag_ms > threshold_ms:
                logger.warning(
                    "[loop-lag] event loop blocked {:.0f}ms (threshold {}ms)",
                    lag_ms, threshold_ms,
                )

    loop_lag_probe = asyncio.create_task(_loop_lag_probe(), name="loop-lag-probe")

    # 局域网发现（UDP 广播应答，见 lan_discovery.py / docs/lan-discovery.md）。
    # 失败只 warning 不阻断启动；loopback HTTP bind 时自动抑制响应。
    try:
        from lan_discovery import start_lan_discovery

        await start_lan_discovery()
    except Exception:
        logger.exception("[lan-discovery] startup failed; main service continues")

    yield

    try:
        from lan_discovery import stop_lan_discovery

        await stop_lan_discovery()
    except Exception:
        logger.exception("[lan-discovery] shutdown failed")

    if orphan_reservation_reaper:
        orphan_reservation_reaper.cancel()
        try:
            await orphan_reservation_reaper
        except asyncio.CancelledError:
            pass
    if loop_lag_probe:
        loop_lag_probe.cancel()
        try:
            await loop_lag_probe
        except asyncio.CancelledError:
            pass
    if attachment_sweeper:
        attachment_sweeper.cancel()
        try:
            await attachment_sweeper
        except asyncio.CancelledError:
            pass
    if channel_catalog_task:
        channel_catalog_task.cancel()
        try:
            await channel_catalog_task
        except asyncio.CancelledError:
            pass
    if node_release_task:
        node_release_task.cancel()
        try:
            await node_release_task
        except asyncio.CancelledError:
            pass
    if mobile_release_task:
        mobile_release_task.cancel()
        try:
            await mobile_release_task
        except asyncio.CancelledError:
            pass
    if ios_wda_renew_task:
        ios_wda_renew_task.cancel()
        try:
            await ios_wda_renew_task
        except asyncio.CancelledError:
            pass
    if leaderboard_sync_task:
        leaderboard_sync_task.cancel()
        try:
            await leaderboard_sync_task
        except asyncio.CancelledError:
            pass
    if agency_agents_sync_task:
        agency_agents_sync_task.cancel()
        try:
            await agency_agents_sync_task
        except asyncio.CancelledError:
            pass
    if agency_agents_zh_sync_task:
        agency_agents_zh_sync_task.cancel()
        try:
            await agency_agents_zh_sync_task
        except asyncio.CancelledError:
            pass
    if agentscope_sync_task:
        agentscope_sync_task.cancel()
        try:
            await agentscope_sync_task
        except asyncio.CancelledError:
            pass
    if consumer_cache_task:
        consumer_cache_task.cancel()
        try:
            await consumer_cache_task
        except asyncio.CancelledError:
            pass
    if marketplace_publisher_task:
        marketplace_publisher_task.cancel()
        try:
            await marketplace_publisher_task
        except asyncio.CancelledError:
            pass
    if marketplace_bootstrap_task:
        marketplace_bootstrap_task.cancel()
        try:
            await marketplace_bootstrap_task
        except asyncio.CancelledError:
            pass
    if tokenizer_vocab_task:
        tokenizer_vocab_task.cancel()
        try:
            await tokenizer_vocab_task
        except asyncio.CancelledError:
            pass
    if node_binding_reconciler:
        node_binding_reconciler.cancel()
        try:
            await node_binding_reconciler
        except asyncio.CancelledError:
            pass
    if editor_session_reaper:
        editor_session_reaper.cancel()
        try:
            await editor_session_reaper
        except asyncio.CancelledError:
            pass
    # 未释放的连接、provider.close 长连接收尾）都不会让 Ctrl+C 无声挂死——
    # 超时后记录告警并继续下一步，保证进程一定能退出。
    async def _shutdown_step(name: str, coro, timeout: float = 5.0):
        try:
            await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"[shutdown] {name} 超时 {timeout}s，跳过继续关闭")
        except Exception as e:
            logger.warning(f"[shutdown] {name} 失败: {e}")

    import runtime_sync
    await _shutdown_step("runtime_sync", runtime_sync.stop())
    await _shutdown_step("auth_scanner", stop_auth_scanner())
    await _shutdown_step("daily_auth_refresh", stop_daily_auth_refresh())
    await _shutdown_step("agent_task_runner", AgentTaskRunner.stop())
    try:
        from agent.background_workers import AgentBackgroundOrchestrator
        await _shutdown_step("agent_bg_orchestrator", AgentBackgroundOrchestrator.stop())
    except Exception:
        logger.exception("[agent-bg] orchestrator stop failed")
    await _shutdown_step("model_client_pool", ModelClientPool.stop())
    await _shutdown_step("request_log_writer", request_log_writer.stop(), timeout=10.0)
    try:
        from request_payload_writer import payload_writer
        await _shutdown_step("request_payload_writer", payload_writer.stop(), timeout=10.0)
    except Exception:
        logger.exception("[request-payload-writer] shutdown failed")
    try:
        from agent import conversation_store
        _conv_client = getattr(conversation_store, "_client", None)
        if _conv_client is not None:
            await _shutdown_step("conversation_store", _conv_client.close(), timeout=10.0)
            conversation_store.attach_client(None)
    except Exception:
        logger.exception("[conversation_store] shutdown failed")
    try:
        import user_platform
        await _shutdown_step("user_platform", user_platform.close())
    except Exception:
        logger.exception("[user-platform] shutdown failed")
    try:
        from user_platform import notify_core
        await _shutdown_step("notify_worker", notify_core.stop())
    except Exception:
        logger.exception("[notify] outbox worker shutdown failed")
    await _shutdown_step("postgres", PostgresClient.close())
    from rd import JdbcClient as _JdbcClient
    await _shutdown_step("redis_pool", _JdbcClient.close(), timeout=5.0)
    logger.info("服务关闭")


app = FastAPI(
    title="Multi-Model Proxy",
    description="OpenAI Compatible API",
    lifespan=lifespan
)

@app.middleware("http")
async def _capture_user_session_cookie(request, call_next):
    """把当前请求的 C 端 session cookie 存入 contextvar，供 admin._require_admin
    做「C 端 session + role==admin」主鉴权（141 处调用点无需改签名）。

    同时记录每个请求的总耗时（method path status dur_ms），用于排查
    「大量接口都 300+ms」这类公共慢点：admin 与 C 端接口都不进 request_logs
    （那套只覆盖模型请求），这里补一条 loguru 行，控制台直接可见分布。
    """
    import time as _time
    _start = _time.perf_counter()
    try:
        from admin import set_current_user_cookie
        from user_platform.session import USER_SESSION_COOKIE
        set_current_user_cookie(request.cookies.get(USER_SESSION_COOKIE))
    except Exception:
        pass
    response = await call_next(request)
    _dur_ms = (_time.perf_counter() - _start) * 1000
    try:
        from loguru import logger
        logger.info(
            "[req] {} {} {} {:.0f}ms",
            request.method, request.url.path, response.status_code, _dur_ms,
        )
    except Exception:
        pass
    return response


# CORS：/api/v1/users/* 等 C 端接口挂在主 app 上。原生客户端不受 CORS 限制，
# 但浏览器（含移动端 Web 预览 http://localhost:11180/11190）跨域且带 session
# cookie 请求时，必须回显具体 Origin —— allow_credentials=True 下不能用 "*"。
# 默认放行本机任意端口的 localhost/127.0.0.1（开发预览）；生产环境用
# MAIN_CORS_ORIGINS（逗号分隔的完整来源）追加正式域名。
_main_cors_origins = [
    o.strip()
    for o in os.environ.get("MAIN_CORS_ORIGINS", "").split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_main_cors_origins,
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.include_router(admin_router)
# 桌面上游型 OAuth 的本机回调（127.0.0.1:{port}/oauth/callback）。必须在 SPA catch-all
# 之前注册；/oauth 也进 _API_PREFIXES，未命中路由时返回 404 而不是回退 SPA 页面。
app.include_router(admin_loopback_router)


# FastAPI's APIRoute, unlike Starlette's Route, does not auto-add HEAD to a
# GET route. The SPA catch-all below is GET-only, so answer root health probes
# explicitly rather than returning 405.
@app.head("/", include_in_schema=False)
async def home_head() -> None:
    return None


@app.get("/_debug/tasks", include_in_schema=False)
async def debug_tasks(fmt: str = "text"):
    """卡死诊断：dump 当前事件循环里所有 asyncio 任务的栈。

    专为「进程活着但请求全卡、必须重启」这类死锁/永久挂起排查设计：
    - 不走鉴权、不碰 Redis / DB / 上游，只读内存中的 asyncio.all_tasks()，
      因此即使 Redis 连接池或某把锁把业务全卡死，这个端点自身仍能响应。
    - 输出每个 task 的名字、是否 done、以及它当前挂起在哪一帧（文件:行 函数）。
      卡死时打开 /_debug/tasks，看大量 task 停在同一行即是真凶。

    fmt=json 返回结构化；默认 text 便于直接肉眼看。
    """
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    dumped: list[dict] = []
    for t in tasks:
        frames = []
        # get_stack 返回该协程当前挂起点的调用栈（最外层在前）。
        for frame in t.get_stack():
            code = frame.f_code
            frames.append({
                "file": code.co_filename,
                "line": frame.f_lineno,
                "func": code.co_name,
            })
        dumped.append({
            "name": t.get_name(),
            "done": t.done(),
            "stack": frames,
        })
    # 按「最内层挂起帧」聚合，方便一眼看出多少 task 卡在同一处。
    from collections import Counter
    hotspots = Counter()
    for d in dumped:
        if d["stack"]:
            leaf = d["stack"][-1]
            hotspots[f"{leaf['file']}:{leaf['line']} {leaf['func']}"] += 1

    if fmt == "json":
        return JSONResponse({
            "task_count": len(dumped),
            "hotspots": hotspots.most_common(),
            "tasks": dumped,
        })

    lines = [f"total_tasks={len(dumped)}", "", "== 挂起热点（task 数 x 位置）=="]
    for loc, cnt in hotspots.most_common():
        lines.append(f"  {cnt:4d}  {loc}")
    lines.append("")
    lines.append("== 每个 task 的栈 ==")
    for d in dumped:
        lines.append(f"[{d['name']}] done={d['done']}")
        for fr in d["stack"]:
            lines.append(f"    {fr['file']}:{fr['line']} {fr['func']}")
        lines.append("")
    return PlainTextResponse("\n".join(lines))


async def validate_api_key(api_key: Optional[str]) -> bool:
    """校验 API Key"""
    config.Config.clear_api_key_request_config()
    config.Config.clear_model_group_request_cache()
    model_metadata.clear_request_cache()
    if not config.Config.api_keys_enabled():
        return True

    if not api_key:
        raise HTTPException(status_code=401, detail="缺少 API Key")

    apikey = await config.Config.get_api_key_config(api_key, include_disabled=True)
    if not apikey:
        raise HTTPException(status_code=401, detail="无效的 API Key")

    if apikey.get("disabled"):
        raise HTTPException(status_code=403, detail="API Key 已被禁用")
    # 顶级 Key（无 parent_id、非 editor）自身的 expires_at 由 _enforce_parent_constraints 兜底校验时
    # 早返回跳过，必须在这里补查，否则顶级 Key 过期了仍可用。
    if apikey.get("expires_at") and float(apikey["expires_at"]) <= time.time():
        raise HTTPException(
            status_code=403,
            detail={"message": "API key has expired.", "code": "api_key_expired"},
        )
    # 子 Key 兜底校验父 Key 的动态状态（停用/过期/累计配额）。editor 路径由
    # _validate_editor_request_context 单独处理，这里对其早返回。
    await _enforce_parent_constraints(apikey)
    config.Config.set_api_key_request_config(apikey)

    # 检查速率限制
    return True


async def _extract_api_key(request: Request, authorization: Optional[str], support_x_api_key: bool = False) -> Optional[str]:
    """提取并校验 API Key，支持 Bearer token 和可选的 x-api-key header。
    当 support_x_api_key=True 且请求带 x-api-key 时，x-api-key 优先于登录 Bearer，
    以便操练场等场景使用所选 API Key 而非当前登录态。"""
    api_key = None

    if support_x_api_key:
        api_key = request.headers.get("x-api-key")

    if not api_key and authorization and authorization.startswith("Bearer "):
        api_key = authorization[7:]

    await validate_api_key(api_key)
    return api_key


async def _acquire_api_key_limit(request: Request, api_key: str | None) -> None:
    # 请求级预占只执行一次。幂等句柄统一管理并发释放，内部渠道重试不会重复占用 API Key。
    reservation = await ApiKeyReservation.acquire(
        api_key,
        enabled=bool(api_key and config.Config.api_keys_enabled()),
    )
    request.state.api_key_limit_reservation = reservation


async def _release_api_key_limit(request: Request) -> None:
    reservation = getattr(request.state, "api_key_limit_reservation", None)
    if reservation is None:
        return
    request.state.api_key_limit_reservation = None
    await reservation.release()


async def _recheck_api_key_state(api_key: str | None, is_test: bool = False) -> HTTPException | None:
    """重试期 API-Key 级状态复检：纯内存 + 请求级 ContextVar 快照，零额外 DB。

    API-Key 并发/RPM/RPD 已由请求级 ApiKeyReservation 预占并持有；这里仅复检
    blocked（内存 pubsub 实时）与 disabled/expires_at（入口快照）。usage_limit 属 DB
    聚合型，仅入口检查，不在每个 attempt 重查。

    ``is_test``：管理端测试/探测链路（手动测试 is_test、定时检测 is_probe）的鉴权边界是
    admin session（入口 _require_admin 已校验），传入的 api_key 是 admin-test / admin-check
    这类占位值、本就不在 api_keys 表里。再走一遍面向外部调用方的 Key 校验属于口径错配，
    会在选账号前直接 401「Invalid API key.」且早于日志写入 —— 与「测试只跳过筛选层」一致，
    这里直接放行；两者的差异只在失败后是否记健康度/冻结。
    """
    if is_test:
        return None
    if not api_key or not config.Config.api_keys_enabled():
        return None
    try:
        from rate_limiter import RateLimiter
        subject = RateLimiter._subject(api_key)
        if RateLimiter._is_blocked(subject, time.time()):
            return HTTPException(
                status_code=429,
                detail={
                    "message": TERMINAL_ERROR_MESSAGES["api_key_rate_limit_exceeded"],
                    "code": "api_key_rate_limit_exceeded",
                    "kind": "api_key_rate_limit_exceeded",
                    "retry_after": _retry_after_for_code("api_key_rate_limit_exceeded"),
                },
            )
    except Exception:
        # 复检失败不得把正常请求误伤；入口 acquire 已做硬校验。
        pass
    apikey = await config.Config.get_api_key_config(api_key, include_disabled=True)
    if not apikey:
        return HTTPException(status_code=401, detail={"message": TERMINAL_ERROR_MESSAGES["api_key_invalid"], "code": "api_key_invalid", "kind": "api_key_invalid"})
    if apikey.get("disabled"):
        return HTTPException(status_code=403, detail={"message": TERMINAL_ERROR_MESSAGES["api_key_disabled"], "code": "api_key_disabled", "kind": "api_key_disabled"})
    if apikey.get("expires_at") and float(apikey["expires_at"]) <= time.time():
        return HTTPException(status_code=403, detail={"message": TERMINAL_ERROR_MESSAGES["api_key_expired"], "code": "api_key_expired", "kind": "api_key_expired"})
    return None


def _openai_error(message: str, error_type: str = "invalid_request_error", code: str | None = None, param: str | None = None) -> dict:
    error = {"message": normalize_upstream_error_message(message), "type": error_type, "code": code}
    if param is not None:
        error["param"] = param
    return {"error": error}


def _error_message_from_detail(detail) -> str:
    if isinstance(detail, dict):
        error = detail.get("error")
        if isinstance(error, dict) and error.get("message"):
            return normalize_upstream_error_message(str(error.get("message")))
        if detail.get("message"):
            return normalize_upstream_error_message(str(detail.get("message")))
    if isinstance(detail, str):
        return normalize_upstream_error_message(detail)
    return normalize_upstream_error_message(str(detail))


def _openai_error_type_for_status(status_code: int) -> str:
    if status_code == 401:
        return "authentication_error"
    if status_code == 403:
        return "permission_error"
    if status_code == 404:
        return "not_found_error"
    if status_code == 429:
        return "rate_limit_error"
    if status_code >= 500:
        return "server_error"
    return "invalid_request_error"


def _anthropic_error_type_for_status(status_code: int) -> str:
    if status_code == 401:
        return "authentication_error"
    if status_code == 403:
        return "permission_error"
    if status_code == 404:
        return "not_found_error"
    if status_code == 429:
        return "rate_limit_error"
    if status_code >= 500:
        return "api_error"
    return "invalid_request_error"


@app.exception_handler(HTTPException)
async def openai_http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    # 先做零 IO 的硬编码超限识别；只有命中时再按开关决定是否归一。普通错误读取开关
    # 同样会触发 config store IO，故开关读取延迟到命中后。开关关掉则不归一、原样透传。
    canonical = retry_policy.canonicalize_upstream_error(exc.status_code, detail)
    if canonical is not None and config.Config.context_overflow_not_retryable_enabled():
        # 超限统一出口：status=400、type=invalid_request_error、code/message 由分类查表固定，
        # 不来自上游、不加前缀。上游原文只进日志（detail 里保留）。
        if request.url.path in ("/v1/messages", "/messages"):
            body = {"type": "error", "error": {"type": canonical.type, "message": canonical.message, "code": canonical.code}}
            return JSONResponse(body, status_code=canonical.status_code, headers=exc.headers)
        body = _openai_error(canonical.message, canonical.type, canonical.code)
        return JSONResponse(body, status_code=canonical.status_code, headers=exc.headers)
    if isinstance(detail, dict) and isinstance(detail.get("error"), dict):
        normalized_detail = dict(detail)
        normalized_error = dict(normalized_detail["error"])
        if normalized_error.get("message") is not None:
            normalized_error["message"] = normalize_upstream_error_message(str(normalized_error.get("message")))
        normalized_detail["error"] = normalized_error
        return JSONResponse(normalized_detail, status_code=exc.status_code, headers=exc.headers)
    message = _error_message_from_detail(detail)
    extra = {}
    if isinstance(exc, NoAvailableAccountError):
        code = getattr(exc, "code", None) or "no_available_account"
        message = TERMINAL_ERROR_MESSAGES.get(code, TERMINAL_ERROR_MESSAGES["no_available_account"])
        extra = {"kind": code, "code": code}
        retry_after = _retry_after_for_code(code)
        if retry_after:
            extra["retry_after"] = retry_after
    if isinstance(detail, dict) and (detail.get("kind") or detail.get("code")):
        if detail.get("kind") is None and detail.get("code") is not None:
            extra["kind"] = detail["code"]
        for field in ("kind", "code", "retries", "upstream_status", "retry_after"):
            if detail.get(field) is not None:
                extra[field] = detail[field]
    headers = dict(exc.headers or {})
    retry_after = extra.get("retry_after")
    if exc.status_code == 429 and retry_after:
        try:
            headers["Retry-After"] = str(int(retry_after))
        except (TypeError, ValueError):
            pass
    if request.url.path in ("/v1/messages", "/messages"):
        body = {"type": "error", "error": {"type": _anthropic_error_type_for_status(exc.status_code), "message": message}}
        if extra:
            body["error"].update(extra)
        return JSONResponse(body, status_code=exc.status_code, headers=headers or None)
    body = _openai_error(message, _openai_error_type_for_status(exc.status_code), extra.get("code") or extra.get("kind"))
    if extra:
        body["error"].update(extra)
    return JSONResponse(body, status_code=exc.status_code, headers=headers or None)


@app.exception_handler(RequestValidationError)
async def openai_validation_exception_handler(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        _openai_error(str(exc), "invalid_request_error", "validation_error"),
        status_code=422,
    )


# 各协议的输入字段口径统一收敛到 usage_utils.request_input_parts：
# 上下文超限拦截与渠道 token 预占必须用同一份字段与同一个模型感知估算器，
# 否则两处对同一请求算出的输入量不一致（历史上预占丢了模型名，拿不到模型级 tokenizer 规则）。
def _context_parts(body: dict, endpoint: str = "chat") -> list[tuple[str, object]]:
    return request_input_parts(body, endpoint)


def _build_reservation_body(messages: list | None, kwargs: dict) -> dict:
    """构造 token 预占估算用的请求体。

    anthropic 客户端路径下 system 会被 anthropic_to_openai_messages 展开成 messages[0]，
    而 _build_protocol_kwargs(source_protocol="anthropic") 又把原始 system 放进 kwargs。
    两处都收会把 system 算两遍——长系统提示下预占虚高约一倍。messages 里已有 system
    时就不再从 kwargs 叠加。
    """
    body: dict = {"messages": messages}
    messages_have_system = any(
        isinstance(message, dict) and message.get("role") == "system"
        for message in (messages or [])
    )
    for key in ("system", "tools"):
        if kwargs.get(key) is None:
            continue
        if key == "system" and messages_have_system:
            continue
        body[key] = kwargs[key]
    return body


def _requested_output_tokens(body: dict, model_info: dict | None = None) -> int:
    # model_info 入参保留以兼容旧调用；实际只读 body（见 usage_utils._requested_output_tokens）。
    from usage_utils import _requested_output_tokens as _impl
    return _impl(body)


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _get_model_info(model: str) -> dict:
    return await _maybe_await(ModelClientPool.get_model_info(model)) or {}


async def _validate_context_budget(model: str, body: dict, endpoint: str = "chat") -> None:
    model_info = await _get_model_info(model)
    _validate_context_budget_for_parts(model, body, model_info, endpoint)


def validate_upstream_context_budget(model: str, payload: dict, model_info: dict | None = None) -> None:
    _validate_context_budget_for_parts(model, payload, model_info or {}, "upstream")


def _validate_context_budget_for_parts(model: str, body: dict, model_info: dict, endpoint: str = "chat") -> None:

    try:
        context_limit = int(model_info.get("max_context_tokens") or 0)
    except (TypeError, ValueError):
        context_limit = 0
    if context_limit <= 0:
        return
    if not config.Config.context_token_detection_enabled():
        return

    # 复用公共计算层（estimate_request_tokens），入口校验用 total_tokens 跟窗口比。
    tokens = estimate_request_tokens(model, body, endpoint)
    total_tokens = tokens["total_tokens"]
    if total_tokens <= context_limit:
        return

    part_tokens = tokens["part_tokens"]
    input_tokens = tokens["input_tokens"]
    output_tokens = tokens["output_tokens"]
    largest_parts = ", ".join(
        name for name, _ in sorted(part_tokens.items(), key=lambda item: item[1], reverse=True)[:3]
    ) or "request"
    logger.warning(
        f"请求上下文超限: model={model}, endpoint={endpoint}, input≈{input_tokens}, "
        f"output={output_tokens}, limit={context_limit}, fields={largest_parts}"
    )
    raise HTTPException(
        status_code=400,
        detail=_openai_error(
            "Your input exceeds the context window of this model. Please adjust your input and try again.",
            "invalid_request_error",
            "context_too_large",
            largest_parts,
        ),
    )


async def _record_api_key_usage(api_key: str | None, usage: dict) -> None:
    if api_key:
        await RateLimiter.record_usage(api_key, usage.get("total_tokens", 0))


async def _finalize_request_success_stats(
    *,
    api_key: str | None,
    requested_model: str,
    endpoint: str,
    usage: dict,
    last_route_info: dict | None,
) -> None:
    """成功请求统计的单一入口，stream / non-stream 共用。"""
    await _record_api_key_usage(api_key, usage)
    if not api_key:
        return
    update_token_stats(
        api_key,
        usage.get("prompt_tokens", 0),
        usage.get("completion_tokens", 0),
        usage.get("cached_tokens", 0),
        usage.get("cache_creation_tokens", 0),
        usage.get("total_tokens", 0),
    )
    routed_model = _last_route_field(last_route_info, "routed_model", requested_model)
    # 新候选 reservation 已在 _chat_with_retry_for_model 成功/已服务收口处用真实 usage
    # reconcile；这里保留 API Key 计量与业务统计，但不能再把渠道 token 二次 INCR。
    # 未迁移的兼容调用没有该标记，仍走旧响应后写路径。
    if not (last_route_info or {}).get("channel_usage_reconciled"):
        await ModelClientPool.record_token_usage(
            routed_model,
            _last_route_field(last_route_info, "provider"),
            _last_route_field(last_route_info, "account"),
            usage.get("total_tokens", 0),
        )
    _add_recent_log(
        api_key, requested_model, endpoint, "ok",
        usage.get("prompt_tokens", 0),
        usage.get("completion_tokens", 0),
        usage.get("cached_tokens", 0),
        usage.get("cache_creation_tokens", 0),
        usage.get("total_tokens", 0),
    )


def _outputs_text(model_info: dict) -> bool:
    """模型是否产出文本。缺失/非法 output_modalities 按文本处理（与 metadata 默认一致）。"""
    modalities = model_info.get("output_modalities")
    if isinstance(modalities, str):
        modalities = [modalities]
    if not isinstance(modalities, (list, tuple)) or not modalities:
        return True
    return "text" in [str(item).lower() for item in modalities if item]


def _request_max_tokens(body: dict) -> int:
    raw = body.get("max_tokens")
    if raw in (None, ""):
        return 0
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="max_tokens 必须是整数")
    if value < 0:
        raise HTTPException(status_code=400, detail="max_tokens 必须大于等于 0")
    return value


async def _validate_chat_request(body: dict, api_key: str | None = None):
    """校验 chat 请求的 model、messages、max_tokens 参数，返回 (model, messages)"""
    catalog_snapshot = model_catalog.current_snapshot()
    model = body.get("model")
    if not model:
        logger.warning("缺少 model 参数")
        raise HTTPException(status_code=400, detail="缺少 model 参数")

    # 诊断日志：请求体里写死的 model 值——这是用户说的"一进入请求方法就写死"
    # 的那层。任何"模型跑错"的线上问题，拿这条日志和 runtime 侧 [claude-query]
    # 对值，就能定位是 runtime 发过来的就是这个值，还是网关路由阶段改了。
    # 不含密钥：只记录 model 名、api_key 前缀、stream 标记。
    _key_prefix = (api_key[:12] + "...") if api_key else "<none>"
    logger.info(
        "[gateway] chat-validate model='{}' api_key_prefix='{}' stream={} "
        "is_model_group={} has_routes={}",
        model, _key_prefix, bool(body.get("stream")),
        await _catalog_call(config.Config.is_model_group, model, snapshot=catalog_snapshot),
        bool(ModelClientPool.get_model_routes(model)),
    )

    messages = body.get("messages")
    if not messages:
        logger.warning("缺少 messages 参数")
        raise HTTPException(status_code=400, detail="缺少 messages 参数")

    # 访问控制按请求名（组主名/别名/真实模型名）匹配 API Key 的模型白/黑名单。
    if not await config.Config.api_key_allows_model(api_key, model):
        raise HTTPException(status_code=403, detail=f"API Key 无权使用模型 '{model}'")
    if await _catalog_call(config.Config.is_model_group, model, snapshot=catalog_snapshot):
        if not await _catalog_call(model_metadata.has_explicit_metadata, model, snapshot=catalog_snapshot):
            raise HTTPException(status_code=400, detail=f"模型组 '{model}' 未配置元数据，不可使用")
        if not await ModelClientPool.get_model_group_available_members(model, snapshot=catalog_snapshot):
            raise HTTPException(status_code=429, detail=f"模型组 '{model}' 没有已配置元数据的可用成员")
    else:
        if not ModelClientPool.get_model_routes(model):
            raise HTTPException(status_code=404, detail=f"模型 '{model}' 不存在")
        if not await _catalog_call(model_metadata.has_explicit_metadata, model, snapshot=catalog_snapshot):
            raise HTTPException(status_code=400, detail=f"模型 '{model}' 未配置元数据，不可使用")

    requested_max_tokens = _request_max_tokens(body)
    model_info = await _get_model_info(model)
    # 生成类模型（图片/视频/语音）的输出不按 token 计量，元数据 max_tokens 对它们没有
    # 约束意义；客户端顺带传来的 max_tokens 不应该把这类请求拦在门口。已有 model_info
    # 时直接读它的 output_modalities，避免热路径上多一次元数据查找。
    if requested_max_tokens and model_info and _outputs_text(model_info):
        model_max_tokens = model_info.get("max_tokens")
        if model_max_tokens and requested_max_tokens > model_max_tokens:
            logger.debug(body)
            logger.warning(f"max_tokens 超限: model={model}, requested={requested_max_tokens}, model_max={model_max_tokens}")
            raise HTTPException(
                status_code=400,
                detail=f"max_tokens ({requested_max_tokens}) 超过模型上限 ({model_max_tokens})"
            )

    await _validate_context_budget(model, body, "chat")

    return model, messages


def _actual_model_from_route(route_info: dict, fallback: str) -> str:
    return route_info.get("public_model_id") or route_info.get("routed_model") or fallback


def _extract_response_model(payload) -> str:
    if isinstance(payload, str):
        for line in payload.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if not data or data == "[DONE]":
                continue
            try:
                model = _extract_response_model(json.loads(data))
            except json.JSONDecodeError:
                continue
            if model:
                return model
        return ""
    if not isinstance(payload, dict):
        return ""
    model = payload.get("model")
    if isinstance(model, str) and model.strip():
        return model.strip()
    response = payload.get("response")
    if isinstance(response, dict):
        model = response.get("model")
        if isinstance(model, str) and model.strip():
            return model.strip()
    message = payload.get("message")
    if isinstance(message, dict):
        model = message.get("model")
        if isinstance(model, str) and model.strip():
            return model.strip()
    return ""


def _response_with_public_model(payload, public_model: str, rewrite_nested: bool = False):
    if not isinstance(payload, dict) or not public_model:
        return payload
    result = payload

    def _copy():
        nonlocal result
        if result is payload:
            result = dict(payload)
        return result

    # 顶层 model：OpenAI chat SSE / OpenAI 非流式响应
    if isinstance(payload.get("model"), str):
        _copy()["model"] = public_model
    if rewrite_nested:
        # 嵌套 response.model：Responses 协议 SSE 事件（response.created 等）
        response = payload.get("response")
        if isinstance(response, dict) and isinstance(response.get("model"), str):
            _copy()["response"] = {**response, "model": public_model}
        # 嵌套 message.model：Anthropic 协议 message_start 事件
        message = payload.get("message")
        if isinstance(message, dict) and isinstance(message.get("model"), str):
            _copy()["message"] = {**message, "model": public_model}
        # 直通/转换包裹 body.model：chat_anthropic / chat_responses 非流式返回
        body = payload.get("body")
        if isinstance(body, dict) and isinstance(body.get("model"), str):
            _copy()["body"] = {**body, "model": public_model}
    return result


def _stream_chunk_with_public_model(chunk, public_model: str, rewrite_nested: bool = False):
    if not public_model:
        return chunk
    if isinstance(chunk, dict):
        return _response_with_public_model(chunk, public_model, rewrite_nested)
    if not isinstance(chunk, str):
        return chunk
    parts = []
    changed = False
    for line in chunk.splitlines(keepends=True):
        stripped = line.lstrip()
        prefix_len = len(line) - len(stripped)
        if not stripped.startswith("data:"):
            parts.append(line)
            continue
        data = stripped[5:].strip()
        if not data or data == "[DONE]":
            parts.append(line)
            continue
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            parts.append(line)
            continue
        updated = _response_with_public_model(payload, public_model, rewrite_nested)
        if updated is payload:
            parts.append(line)
            continue
        newline = "\n" if line.endswith("\n") else ""
        parts.append(f"{line[:prefix_len]}data: {json.dumps(updated, ensure_ascii=False)}{newline}")
        changed = True
    return "".join(parts) if changed else chunk


async def _log_request(data: dict):
    try:
        return await PostgresClient.insert_request_log(data)
    except Exception as e:
        logger.warning(f"写入请求日志失败: {e}")
        return None


async def _update_request_log(log_id: int, data: dict):
    try:
        await PostgresClient.update_request_log(log_id, data)
    except Exception as e:
        logger.warning(f"更新请求日志失败: {e}")


def _schedule_update_request_log(log_id: int, data: dict):
    asyncio.create_task(_update_request_log(log_id, data))


async def _finalize_after_route_log_id(route_info: dict, data: dict):
    log_id = await _ensure_route_log_id(route_info)
    if log_id:
        await _update_request_log(log_id, data)
    else:
        await _log_request(data)


def _schedule_finalize_request_log(route_info: dict, data: dict):
    log_id = route_info.get("log_id")
    if log_id:
        _schedule_update_request_log(log_id, data)
        return
    if route_info.get("log_id_task") is not None:
        asyncio.create_task(_finalize_after_route_log_id(route_info, data))
        return
    _schedule_log_request(data)


def _schedule_log_request(data: dict):
    return asyncio.create_task(_log_request(data))


async def _log_notification(data: dict):
    try:
        return await PostgresClient.upsert_notification(data)
    except Exception as e:
        logger.warning(f"写入通知失败: {e}")
        return None


def _schedule_notification(data: dict):
    asyncio.create_task(_log_notification(data))


def _client_ip(request: Request) -> str | None:
    """提取客户端 IP（兼容反代场景）。"""
    try:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real = request.headers.get("x-real-ip")
        if real:
            return real.strip()
        if request.client and request.client.host:
            return request.client.host
    except Exception:
        return None
    return None


def _build_security_block_log_data(*, request_id, api_key, api_key_name, endpoint, body, scan_result, client_type, request_headers) -> dict:
    """构造被安全拦截请求的日志数据，用于在 request_log 里留下拦截记录。

    原代码：被安全拦截的请求仅 raise HTTPException，从未写 request_log，导致
    前端请求日志页面看不到拦截发生过，也无法从记录里查到命中规则、触发片段。
    """
    return {
        "request_id": request_id,
        "api_key": api_key,
        "api_key_name": api_key_name,
        "model": body.get("model", ""),
        "endpoint": endpoint,
        "success": False,
        "status": "security_blocked",
        "stream": bool(body.get("stream")),
        "error": scan_result.block_reason,
        "client_type": client_type,
        "session_id": _extract_client_session_context(request_headers).get("session_id"),
        "request_body": body,
        "request_headers": request_headers,
    }


async def _record_security_block(*, scan_result, request_id, api_key, api_key_name, endpoint, body, client_type, request_headers, source_ip=None) -> None:
    """安全拦截闭环：写 request_log(security_blocked) + security_events(带 request_id 与关联 log_id) + 通知。

    1) 写入一条 request_log(status=security_blocked)，便于前端请求日志能查阅；
    2) 把每条安全事件的 request_id/关联的 request_log_id 都填好，便于从前端跳转到通知与详情；
    3) 由 event_log.py 内部的 _maybe_create_notification 自动建通知。
    """
    from security import log_security_events as _log_security_events  # 延迟 import 避免顶层强依赖
    model = body.get("model", "")
    log_data = _build_security_block_log_data(
        request_id=request_id, api_key=api_key, api_key_name=api_key_name,
        endpoint=endpoint, body=body, scan_result=scan_result,
        client_type=client_type, request_headers=request_headers,
    )
    # 同步等 log_id 以便关联到 security_events
    log_id = await _log_request(log_data)
    await _log_security_events(
        scan_result.tags,
        request_id=request_id,
        api_key=api_key,
        model=model,
        source_ip=source_ip,
        request_log_id=log_id,
    )


def _new_request_id() -> str:
    return f"req_{uuid.uuid4().hex[:12]}"


def _client_header(headers, name: str) -> str:
    try:
        return str(headers.get(name) or "").strip()
    except Exception:
        return ""


def _extract_client_session_context(headers) -> dict:
    claude_session = _client_header(headers, "x-claude-code-session-id")
    codex_session = _client_header(headers, "thread-id") or _client_header(headers, "session-id")
    opencode_session = _client_header(headers, "x-session-id") or _client_header(headers, "x-session-affinity")
    # WorkBuddy / CodeBuddy 的会话 id 落在 x-conversation-id。
    workbuddy_session = _client_header(headers, "x-conversation-id")
    client_request_id = (
        _client_header(headers, "x-client-request-id")
        or _client_header(headers, "x-conversation-request-id")
        or _client_header(headers, "x-request-id")
    )
    context = {}
    session_id = claude_session or codex_session or opencode_session or workbuddy_session
    if session_id:
        source = (
            "x-claude-code-session-id" if claude_session
            else "thread-id" if _client_header(headers, "thread-id")
            else "session-id" if _client_header(headers, "session-id")
            else "x-session-id" if _client_header(headers, "x-session-id")
            else "x-session-affinity" if _client_header(headers, "x-session-affinity")
            else "x-conversation-id"
        )
        context["client_session_id"] = session_id
        context["client_session_source"] = source
    if client_request_id:
        context["client_request_id"] = client_request_id
    return context


def _parse_codex_turn_metadata(headers) -> dict:
    raw = _client_header(headers, "x-codex-turn-metadata")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_provider_request_identity(headers) -> dict:
    claude_session = _client_header(headers, "x-claude-code-session-id")
    if claude_session:
        return {"provider": "claude", "provider_thread_id": claude_session}

    codex_thread = _client_header(headers, "thread-id")
    codex_session = _client_header(headers, "session-id")
    codex_meta = _parse_codex_turn_metadata(headers)
    meta_thread = str(codex_meta.get("thread_id") or "").strip()
    meta_session = str(codex_meta.get("session_id") or "").strip()
    codex_id = codex_thread or codex_session or meta_thread or meta_session
    if codex_id:
        for value in (codex_thread, codex_session, meta_thread, meta_session):
            if value and value != codex_id:
                raise HTTPException(status_code=400, detail="Codex session headers are inconsistent")
        window_id = _client_header(headers, "x-codex-window-id") or str(codex_meta.get("window_id") or "").strip()
        if window_id and not window_id.startswith(codex_id + ":"):
            raise HTTPException(status_code=400, detail="Codex window id does not match thread id")
        return {
            "provider": "codex",
            "provider_thread_id": codex_id,
            "client_id": str(codex_meta.get("installation_id") or "").strip(),
        }

    opencode_session = _client_header(headers, "x-session-id")
    opencode_affinity = _client_header(headers, "x-session-affinity")
    opencode_id = opencode_session or opencode_affinity
    if opencode_id:
        if opencode_session and opencode_affinity and opencode_session != opencode_affinity:
            raise HTTPException(status_code=400, detail="OpenCode session headers are inconsistent")
        return {"provider": "opencode", "provider_thread_id": opencode_id}

    return {}


def _first_user_content(body: dict | None) -> str:
    if not isinstance(body, dict):
        return ""
    items = body.get("messages")
    if not isinstance(items, list):
        input_value = body.get("input")
        if isinstance(input_value, str):
            return input_value.strip()
        items = input_value if isinstance(input_value, list) else []
    for message in items:
        if not isinstance(message, dict) or str(message.get("role") or "").lower() != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") in {"text", "input_text"}:
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
            return "".join(parts).strip()
    return ""


async def _enforce_api_key_usage_limit(apikey: dict, *, include_children: bool, label: str = "API Key") -> None:
    limit = apikey.get("usage_limit") or {}
    if not isinstance(limit, dict) or not limit:
        return
    max_requests = int(limit.get("max_requests") or 0)
    max_total_tokens = int(limit.get("max_total_tokens") or 0)
    if not max_requests and not max_total_tokens:
        return
    totals = await PostgresClient.api_key_usage_totals(int(apikey["id"]), include_children)
    if max_requests and totals["requests"] >= max_requests:
        _notify_usage_threshold(apikey, "requests", totals["requests"], max_requests)
        raise HTTPException(status_code=403, detail=f"{label} 请求额度已用尽")
    if max_total_tokens and totals["total_tokens"] >= max_total_tokens:
        _notify_usage_threshold(apikey, "total_tokens", totals["total_tokens"], max_total_tokens)
        raise HTTPException(status_code=403, detail=f"{label} Token 额度已用尽")


def _notify_usage_threshold(apikey: dict, dimension: str, used: int, limit: int) -> None:
    """密钥用量达阈值 → 通知（best-effort）。dedupe 1h，避免额度打满后每次请求刷屏。"""
    try:
        from user_platform.notify_core import emit_notification_background
        key_id = str(apikey.get("id"))
        emit_notification_background(
            "api_key.usage_threshold",
            params={
                "api_key_id": key_id,
                "name": apikey.get("name") or "",
                "dimension": dimension,
                "used": used,
                "limit": limit,
            },
            owner_type="platform",
            severity="warn",
            message=f"密钥 {apikey.get('name') or key_id} 的 {dimension} 用量已达上限（{used}/{limit}）",
            dedupe_key=f"api_key.usage_threshold:{key_id}:{dimension}",
            dedupe_window_seconds=3600,
        )
    except Exception:  # noqa: BLE001
        logger.debug("[notify] usage threshold emit skipped")


async def _enforce_parent_constraints(apikey: dict) -> None:
    """子 Key 请求时兜底校验父 Key 的动态状态（停用/过期/累计配额）。

    创建/编辑时白/黑名单已被收窄为父的子集（见 db._narrow_child_config），子行自身
    的列即已足够做请求过滤；这里只需补齐会随时间变化、无法在创建时定死的三项：父被
    停用、父已过期、父累计配额（含所有子）打满。任一命中 → 403，子 Key 立即失效。

    scope='editor' 的 Key 由 _validate_editor_request_context 单独处理父级校验，这里
    不重复。父已被删（parent_id 置 NULL）时子降级为独立 Key，不进入本函数。
    """
    parent_id = apikey.get("parent_id")
    if not parent_id or apikey.get("scope") in ("editor", "task"):
        return
    parent = await config.Config.get_api_key_by_id(int(parent_id))
    if not parent:
        return
    if parent.get("disabled"):
        raise HTTPException(status_code=403, detail="父 API Key 已停用")
    if parent.get("expires_at") and float(parent["expires_at"]) <= time.time():
        raise HTTPException(status_code=403, detail="父 API Key 已过期")
    await _enforce_api_key_usage_limit(parent, include_children=True)


async def _task_id_for_editor_session(session: dict | None) -> str:
    """Resolve legacy editor-session attribution to the canonical Task ledger.

    This is compatibility metadata only: a missing/uninitialized Task table must
    not reject an otherwise valid legacy editor request.
    """
    session_id = str((session or {}).get("id") or "").strip()
    if not session_id:
        return ""
    try:
        from user_platform.models_task import Task

        task = await Task.filter(
            source_editor_session_id=session_id,
            deleted_at=None,
        ).only("id").first()
    except Exception as exc:  # noqa: BLE001 - compatibility lookup is non-blocking
        logger.debug("Task attribution lookup failed for editor session {}: {}", session_id, exc)
        return ""
    return str(task.id) if task is not None else ""


async def _validate_editor_request_context(api_key: str | None, headers, body: dict | None = None) -> dict:
    if not api_key:
        return {}
    apikey = await config.Config.get_api_key_config(api_key, include_disabled=True)
    if not apikey or apikey.get("scope") != "editor":
        return {}
    if apikey.get("disabled"):
        raise HTTPException(status_code=403, detail="Editor API Key 已停用")
    if apikey.get("expires_at") and float(apikey["expires_at"]) <= time.time():
        raise HTTPException(status_code=403, detail="Editor API Key 已过期")
    await _enforce_api_key_usage_limit(apikey, include_children=False, label="Editor API Key")
    parent_id = apikey.get("parent_id")
    if parent_id:
        parent = await config.Config.get_api_key_by_id(int(parent_id))
        if not parent or parent.get("disabled"):
            raise HTTPException(status_code=403, detail="Editor 父 API Key 不可用")
        if parent.get("expires_at") and float(parent["expires_at"]) <= time.time():
            raise HTTPException(status_code=403, detail="Editor 父 API Key 已过期")
        await _enforce_api_key_usage_limit(parent, include_children=True, label="Editor API Key")
    # 新模型：session 自持 key；老编辑器 key 继续走兼容回落。
    session_from_key = await PostgresClient.get_editor_by_session_api_key_id(int(apikey["id"]))
    editor = session_from_key or await PostgresClient.get_editor_by_api_key_id(int(apikey["id"]))
    if not editor:
        raise HTTPException(status_code=403, detail="Editor API Key 未绑定编辑器")
    identity = _extract_provider_request_identity(headers)
    provider = identity.get("provider")
    if not provider:
        raise HTTPException(status_code=400, detail="缺少编辑器会话标识")
    if provider != editor.get("provider"):
        raise HTTPException(status_code=403, detail="请求 provider 与编辑器不匹配")
    provider_thread_id = identity.get("provider_thread_id") or ""
    if not provider_thread_id:
        raise HTTPException(status_code=400, detail="缺少 provider 会话 ID")

    session = None
    if session_from_key:
        # session key 唯一标识一个 session，只能访问自己绑定的 session。
        session = await PostgresClient.get_editor_session(editor["id"], session_from_key["session_id"])
        if session and session.get("status") == "active" and session.get("provider_thread_id"):
            bound_thread = str(session.get("provider_thread_id") or "")
            if bound_thread != provider_thread_id:
                raise HTTPException(status_code=403, detail="Editor API Key 与 provider 会话不匹配")
            return {
                "editor": editor,
                "session": session,
                "provider_thread_id": session.get("provider_thread_id") or provider_thread_id,
                "task_id": await _task_id_for_editor_session(session),
                "first_request": False,
                "api_key_version": apikey.get("version"),
                "api_key_name_snapshot": apikey.get("name"),
                "api_key_id": apikey.get("id"),
                "api_key_parent_id": apikey.get("parent_id"),
                "task_id": await _task_id_for_editor_session(session),
            }
        if session and session.get("status") == "pending_first_request":
            # session key 已鉴权到具体 session：首请求为所有 provider 通用绑定线程。
            # Codex 仍额外校验预注册的 installation_id + 首条内容，防止实例串号。
            if editor.get("provider") == "codex":
                client_id = (identity.get("client_id") or "").strip()
                expected_client_id = (session.get("expected_client_id") or "").strip()
                if not expected_client_id or not client_id or expected_client_id != client_id:
                    raise HTTPException(status_code=403, detail="Codex 客户端实例不匹配")
                expected_content_hash = (session.get("bootstrap_content_hash") or "").strip()
                first_content = _first_user_content(body)
                if expected_content_hash:
                    actual = hashlib.sha256(first_content.encode("utf-8")).hexdigest() if first_content else ""
                    if actual != expected_content_hash:
                        raise HTTPException(status_code=403, detail="Codex 首条消息与预注册会话不匹配")
            return {
                "editor": editor,
                "session": session,
                "provider_thread_id": provider_thread_id,
                "first_request": True,
                "api_key_version": apikey.get("version"),
                "api_key_name_snapshot": apikey.get("name"),
                "api_key_id": apikey.get("id"),
                "api_key_parent_id": apikey.get("parent_id"),
                "task_id": await _task_id_for_editor_session(session),
            }

    session = await PostgresClient.get_editor_session_by_thread(editor["id"], provider_thread_id)
    if session:
        return {
            "editor": editor,
            "session": session,
            "provider_thread_id": provider_thread_id,
            "first_request": False,
            "api_key_version": apikey.get("version"),
            "api_key_name_snapshot": apikey.get("name"),
            "api_key_id": apikey.get("id"),
            "api_key_parent_id": apikey.get("parent_id"),
            "task_id": await _task_id_for_editor_session(session),
        }

    if provider != "codex":
        raise HTTPException(status_code=403, detail="Provider 会话未预注册")

    client_id = (identity.get("client_id") or "").strip()
    first_content = _first_user_content(body)
    actual_content_hash = hashlib.sha256(first_content.encode("utf-8")).hexdigest() if first_content else ""
    pending = await PostgresClient.get_pending_editor_session_for_first_request(
        editor["id"], client_id, actual_content_hash
    )
    if not pending or pending.get("status") != "pending_first_request":
        raise HTTPException(status_code=403, detail="Codex 首请求未命中预注册会话")
    expected_client_id = (pending.get("expected_client_id") or "").strip()
    if not expected_client_id or not client_id or expected_client_id != client_id:
        raise HTTPException(status_code=403, detail="Codex 客户端实例不匹配")
    expected_content_hash = (pending.get("bootstrap_content_hash") or "").strip()
    if not expected_content_hash or not first_content:
        raise HTTPException(status_code=403, detail="Codex 首请求缺少预注册首条内容")
    actual_content_hash = hashlib.sha256(first_content.encode("utf-8")).hexdigest()
    if actual_content_hash != expected_content_hash:
        raise HTTPException(status_code=403, detail="Codex 首条消息与预注册会话不匹配")
    return {
        "editor": editor,
        "session": pending,
        "provider_thread_id": provider_thread_id,
        "first_request": True,
        "api_key_version": apikey.get("version"),
        "api_key_name_snapshot": apikey.get("name"),
        "api_key_id": apikey.get("id"),
        "api_key_parent_id": apikey.get("parent_id"),
        "task_id": await _task_id_for_editor_session(pending),
    }


async def _validate_task_request_context(api_key: str | None, headers, body: dict | None = None) -> dict:
    """Validate a ``scope='task'`` child key and resolve its Task lineage.

    Mirrors ``_validate_editor_request_context`` for the canonical Task: child
    disabled/expiry/own-usage, parent disabled/expiry/aggregate-usage, provider
    identity, provider-thread match, and Codex installation/content bootstrap.
    Returns ``{}`` for non-task keys so the caller can fall back to editor or
    generic validation.
    """
    if not api_key:
        return {}
    apikey = await config.Config.get_api_key_config(api_key, include_disabled=True)
    if not apikey or apikey.get("scope") != "task":
        return {}
    if apikey.get("disabled"):
        raise HTTPException(status_code=403, detail="Task API Key 已停用")
    if apikey.get("expires_at") and float(apikey["expires_at"]) <= time.time():
        raise HTTPException(status_code=403, detail="Task API Key 已过期")
    await _enforce_api_key_usage_limit(apikey, include_children=False, label="Task API Key")
    parent_id = apikey.get("parent_id")
    if parent_id:
        parent = await config.Config.get_api_key_by_id(int(parent_id))
        if not parent or parent.get("disabled"):
            raise HTTPException(status_code=403, detail="Task 父 API Key 不可用")
        if parent.get("expires_at") and float(parent["expires_at"]) <= time.time():
            raise HTTPException(status_code=403, detail="Task 父 API Key 已过期")
        await _enforce_api_key_usage_limit(parent, include_children=True, label="Task API Key")
    task = await PostgresClient.get_task_by_api_key_id(int(apikey["id"]))
    if not task:
        raise HTTPException(status_code=403, detail="Task API Key 未绑定任务")
    identity = _extract_provider_request_identity(headers)
    provider = identity.get("provider")
    if not provider:
        raise HTTPException(status_code=400, detail="缺少任务会话标识")
    if provider != task.get("provider"):
        raise HTTPException(status_code=403, detail="请求 provider 与任务不匹配")
    provider_thread_id = identity.get("provider_thread_id") or ""
    if not provider_thread_id:
        raise HTTPException(status_code=400, detail="缺少 provider 会话 ID")
    bound_thread = (task.get("provider_thread_id") or "").strip()
    first_seen = bool(task.get("first_request_seen"))
    bootstrap_consumed = bool(task.get("bootstrap_consumed"))
    # 诊断日志：网关收到的 task 请求 thread 与 DB 绑定值。403 "Task API Key 与
    # provider 会话不匹配" 的根因在这里——两个值不一致就是 mismatch，一致就
    # 排除这条。也记录 bound 是否为空（首请求绑定前 vs 已绑定后的分叉点）。
    logger.info(
        "[gateway] task-auth task_id='{}' provider='{}' incoming_thread='{}' "
        "db_bound_thread='{}' first_seen={} bootstrap_consumed={}",
        str(task.get("id") or ""), provider, provider_thread_id,
        bound_thread or "<unbound>", first_seen, bootstrap_consumed,
    )
    if bound_thread:
        # Already bound: the request thread must match the Task's thread.
        if bound_thread != provider_thread_id:
            raise HTTPException(status_code=403, detail="Task API Key 与 provider 会话不匹配")
        return {
            "task_id": str(task["id"]),
            "task": task,
            "provider_thread_id": bound_thread,
            "first_request": False,
            "api_key_version": apikey.get("version"),
            "api_key_name_snapshot": apikey.get("name"),
            "api_key_id": apikey.get("id"),
            "api_key_parent_id": apikey.get("parent_id"),
        }
    # Pending first request: bind on dispatch. Codex additionally checks the
    # pre-registered installation id + first-content hash.
    if provider == "codex":
        client_id = (identity.get("client_id") or "").strip()
        expected_client_id = (task.get("expected_client_id") or "").strip()
        if not expected_client_id or not client_id or expected_client_id != client_id:
            raise HTTPException(status_code=403, detail="Codex 客户端实例不匹配")
        expected_content_hash = (task.get("bootstrap_content_hash") or "").strip()
        first_content = _first_user_content(body)
        if not expected_content_hash or not first_content:
            raise HTTPException(status_code=403, detail="Codex 首请求缺少预注册首条内容")
        actual = hashlib.sha256(first_content.encode("utf-8")).hexdigest()
        if actual != expected_content_hash:
            raise HTTPException(status_code=403, detail="Codex 首条消息与预注册任务不匹配")
    return {
        "task_id": str(task["id"]),
        "task": task,
        "provider_thread_id": provider_thread_id,
        "first_request": True,
        "api_key_version": apikey.get("version"),
        "api_key_name_snapshot": apikey.get("name"),
        "api_key_id": apikey.get("id"),
        "api_key_parent_id": apikey.get("parent_id"),
    }


async def _resolve_request_context(api_key: str | None, headers, body: dict | None = None) -> dict:
    """Resolve the scoped request context: task first, then editor fallback.

    A ``scope='task'`` key must be validated by the task path and may NOT fall
    through to editor; a ``scope='editor'`` key goes through the editor path.
    Ordinary/general keys get an empty context and ride the generic validator.
    """
    task_ctx = await _validate_task_request_context(api_key, headers, body)
    if task_ctx:
        return task_ctx
    return await _validate_editor_request_context(api_key, headers, body)


def _build_channel_attempt_log(
    *,
    request_id: str,
    route_info: dict,
    model: str,
    messages: list,
    stream: bool,
    api_key: str | None,
    api_key_name: str | None,
    request_headers: dict | None,
    start_time: float,
    duration_ms: int,
    success: bool,
    status: str,
    response_body: dict | None,
    error: str,
    usage: dict | None = None,
    client_request_path: str | None = None,
    client_request_body: dict | None = None,
) -> dict:
    provider_name = route_info.get("provider")
    username = route_info.get("account")
    client_type = route_info.get("client_type") or _detect_client_type(None, request_headers, client_request_path)
    if isinstance(client_request_body, dict):
        request_body = dict(client_request_body)
    elif messages:
        request_body = {"model": model, "messages": messages, "stream": stream, "path": client_request_path, "client_type": client_type}
    else:
        request_body = {"model": model, "stream": stream, "path": client_request_path, "client_type": client_type}
    log_data = {
        "request_id": request_id,
        "attempt_key": route_info.get("attempt_key") or f"legacy:{uuid.uuid4().hex}",
        "attempt_no": int(route_info.get("attempt_no") or route_info.get("retry", 0) + 1),
        "created_at": start_time,
        "api_key": api_key,
        "api_key_name": api_key_name,
        "provider_name": provider_name,
        "account_username": username,
        "model": model,
        "actual_model": _actual_model_from_route(route_info, model),
        "upstream_returned_model": route_info.get("upstream_returned_model") or "",
        "endpoint": client_request_path or "",
        "client_type": client_type,
        "session_id": route_info.get("session_id"),
        "editor_id": route_info.get("editor_id"),
        "editor_session_id": route_info.get("editor_session_id"),
        "task_id": route_info.get("task_id"),
        "api_key_version": route_info.get("api_key_version"),
        "api_key_name_snapshot": route_info.get("api_key_name_snapshot"),
        "api_key_id": route_info.get("api_key_id"),
        "api_key_parent_id": route_info.get("api_key_parent_id"),
        "success": success,
        "status": status,
        "stream": stream,
        "duration_ms": duration_ms,
        "estimated_prompt_tokens": int(route_info.get("estimated_prompt_tokens") or 0),
        "first_token_ms": route_info.get("ttft_ms"),
        "request_body": request_body,
        "router_request_body": route_info.get("router_request_body"),
        "router_request_headers": route_info.get("router_request_headers"),
        "router_request_path": route_info.get("router_request_path"),
        "router_response_body": route_info.get("router_response_body"),
        "response_body": response_body,
        "request_headers": request_headers,
        "response_headers": route_info.get("response_headers"),
        "upstream_status": route_info.get("upstream_status"),
        "route_duration_ms": int(route_info.get("route_duration_ms") or 0),
        "candidate_collect_ms": int(route_info.get("candidate_collect_ms") or 0),
        "strategy_select_ms": int(route_info.get("strategy_select_ms") or 0),
        "account_reserve_ms": int(route_info.get("account_reserve_ms") or 0),
        "routing_redis_degraded": bool(route_info.get("routing_redis_degraded")),
        "redis_timeout_stage": route_info.get("redis_timeout_stage") or "",
        "routing_detail": {
            "model_group_check_ms": int(route_info.get("model_group_check_ms") or 0),
            "strategy_config_ms": int(route_info.get("strategy_config_ms") or 0),
            "candidate_group_members_ms": int(route_info.get("candidate_group_members_ms") or 0),
            "candidate_metadata_ms": int(route_info.get("candidate_metadata_ms") or 0),
            "provider_filter_ms": int(route_info.get("provider_filter_ms") or 0),
            "candidate_tpm_check_ms": int(route_info.get("candidate_tpm_check_ms") or 0),
            "candidate_operation_check_ms": int(route_info.get("candidate_operation_check_ms") or 0),
            "candidate_volume_factor_ms": int(route_info.get("candidate_volume_factor_ms") or 0),
            "candidate_scan_ms": int(route_info.get("candidate_scan_ms") or 0),
            # 两层重试诊断：attempt_no 是全请求单调序号；这三个字段保留它在
            # 「第几个候选 / 该候选第几次同账号请求」中的位置，供详情页打标签。
            "outer_retry_index": int(route_info.get("outer_retry_index") or 0),
            "inner_retry_index": int(route_info.get("inner_retry_index") or 0),
            "inner_retry_limit": int(route_info.get("inner_retry_limit") or 1),
            # 客户端在上游已服务后断开（status=cancelled 但 200+token 已产出）：
            # 由 _finalize_failed_attempt 设置，让取消不再被误读为上游失败。
            "termination_reason": route_info.get("termination_reason") or "",
        },
        "channel_retry_attempts": list(route_info.get("channel_retry_attempts") or []),
        "error": error,
    }
    log_data.update(usage or _zero_usage())
    return log_data


async def _ensure_route_log_id(route_info: dict) -> int | None:
    if route_info.get("log_id"):
        return route_info["log_id"]
    task = route_info.get("log_id_task")
    if task is not None:
        try:
            log_id = await task
        except Exception as e:
            logger.warning(f"等待请求日志写入失败: {e}")
            log_id = None
        route_info.pop("log_id_task", None)
        if log_id:
            route_info["log_id"] = log_id
            return log_id
    return None


async def _acquire_client_with_routing_timing(
    *, model, tried_accounts, messages, route, api_key, session_id,
    request_protocol, provider_whitelist, provider_blacklist,
    account_whitelist, is_test, is_probe: bool = False, catalog_snapshot,
    token_estimate: int = 0, input_token_estimate: int = 0,
) -> tuple:
    routing_timing: dict = {}
    route_started = time.monotonic()
    timing_token = begin_routing_timing(routing_timing)
    redis_token = begin_routing_redis_scope(routing_timing, token_estimate=token_estimate)
    try:
        result = await ModelClientPool.acquire_client_with_provider(
            model, tried_accounts, messages, route, api_key,
            session_id=session_id, request_protocol=request_protocol,
            provider_whitelist=provider_whitelist, provider_blacklist=provider_blacklist,
            account_whitelist=account_whitelist, is_test=is_test, is_probe=is_probe,
            snapshot=catalog_snapshot, input_token_estimate=input_token_estimate,
        )
        routing_timing["route_duration_ms"] = max(0, int((time.monotonic() - route_started) * 1000))
        redis_state = routing_redis_state()
        if redis_state:
            routing_timing["routing_redis_degraded"] = bool(redis_state.get("degraded"))
            if redis_state.get("timeout_stage"):
                routing_timing["redis_timeout_stage"] = redis_state["timeout_stage"]
        return result, routing_timing
    finally:
        end_routing_redis_scope(redis_token)
        end_routing_timing(timing_token)


def _enqueue_started_log(
    *, request_id: str, route_info: dict, pending_log: dict,
) -> None:
    """入队 started 事件。route_info["_is_save_log"]=False 时直接跳过——定时检测
    关闭保留日志后成功失败都不写，started 不入队就不会留下永久 requesting 行。
    """
    if not route_info.get("_is_save_log", True):
        return
    request_log_writer.enqueue_started(
        request_id=request_id,
        attempt_key=pending_log["attempt_key"],
        attempt_no=pending_log["attempt_no"],
        payload=pending_log,
    )


async def _finalize_channel_attempt_log(
    *,
    request_id: str | None = None,
    route_info: dict,
    model: str,
    messages: list,
    stream: bool,
    api_key: str | None,
    api_key_name: str | None,
    request_headers: dict | None,
    attempt_start: float,
    duration_ms_override: int | None = None,
    success: bool,
    status: str,
    response_body: dict | None,
    error: str,
    usage: dict | None = None,
    client_request_path: str | None = None,
    client_request_body: dict | None = None,
) -> None:
    # 私有写入控制绝不能进入 route 诊断/admin 响应。
    # 定时检测关闭保留日志：is_save_log=False 时 started 都没写，finalized 也不写。
    if not route_info.get("_is_save_log", True):
        return
    if not request_id or not route_info.get("provider"):
        return
    log_data = _build_channel_attempt_log(
        request_id=request_id,
        route_info=route_info,
        model=model,
        messages=messages,
        stream=stream,
        api_key=api_key,
        api_key_name=api_key_name,
        request_headers=request_headers,
        start_time=attempt_start,
        duration_ms=(
            int(duration_ms_override)
            if duration_ms_override is not None
            else _duration_ms(attempt_start)
        ),
        success=success,
        status=status,
        response_body=response_body,
        error=error,
        usage=usage,
        client_request_path=client_request_path,
        client_request_body=client_request_body,
    )
    # 实际代理：由 ProxyManager 在真正出站处写入，只在 finalize 时读取——请求发出后
    # 才有真值，且这条日志对应的就是这次模型请求。
    proxy_info = take_outbound_proxy()
    if proxy_info:
        log_data["proxy_info"] = proxy_info
    request_log_writer.enqueue_finalized(
        request_id=request_id,
        attempt_key=log_data["attempt_key"],
        attempt_no=log_data["attempt_no"],
        payload=log_data,
    )


async def _finalize_failed_attempt(
    *,
    error: Exception,
    route_info: dict,
    provider_name: str | None,
    username: str | None,
    model: str,
    retry_count: int,
    request_id: str | None,
    messages: list,
    stream: bool,
    api_key: str | None,
    api_key_name: str | None,
    request_headers: dict | None,
    client_request_path: str | None,
    client_request_body: dict | None,
    attempt_start: float,
    attempt_stream_summary: dict | None = None,
    response_body: dict | None = None,
    status: str = "error",
) -> dict:
    """统一完成一次失败/取消 attempt 的路由信息与渠道日志。"""
    detail = error.detail if isinstance(error, HTTPException) else str(error)
    error_text = _error_message_from_detail(detail) if isinstance(error, HTTPException) else str(error)
    route_info.setdefault("provider", provider_name)
    route_info.setdefault("account", username)
    route_info.setdefault("requested_model", model)
    route_info.setdefault("routed_model", route_info.get("routed_model") or model)
    route_info.setdefault("retry", retry_count)
    route_info["status"] = status
    route_info["error"] = error_text
    # 客户端断开但上游已真实服务（有已产出流/响应体）：打上 termination_reason 标记
    # （由 _build_channel_attempt_log 带进 routing_detail JSONB，无需表迁移），让
    # 日志读者不把「200 + 已出 token」的取消误读为请求失败。仅在取消+已有实际内容
    # 时标记；未开始服务就断开保持纯 cancelled 语义。
    if (
        status == "cancelled"
        and (attempt_stream_summary is not None or response_body is not None)
        and not route_info.get("termination_reason")
    ):
        route_info["termination_reason"] = "client_disconnect_after_upstream_success"
    if isinstance(error, HTTPException):
        route_info["upstream_status"] = str(error.status_code)
    error_code = getattr(error, "code", None)
    if error_code:
        route_info["error_code"] = error_code
    usage_body = _stream_summary_to_openai_response(model, attempt_stream_summary) if attempt_stream_summary is not None else None
    await _finalize_channel_attempt_log(
        request_id=request_id,
        route_info=route_info,
        model=model,
        messages=messages,
        stream=stream,
        api_key=api_key,
        api_key_name=api_key_name,
        request_headers=request_headers,
        client_request_path=client_request_path,
        client_request_body=client_request_body,
        attempt_start=attempt_start,
        success=False,
        status=status,
        # 普通失败不保存合成/占位的响应体；客户端取消时保留已经实际下发的部分流，
        # 让 finalized payload 能覆盖 started 事件，详情页可看到断开前的真实内容。
        response_body=response_body,
        error=error_text,
        usage=_usage(
            usage_body,
            {"messages": messages, "tools": route_info.get("tools")},
            estimate=(status == "cancelled"),
        ) if usage_body else None,
    )
    return dict(route_info)


def _append_channel_retry_attempt(
    attempts: list[dict],
    *,
    attempt_no: int,
    started_at: float,
    success: bool,
    error: Exception | None = None,
    upstream_status: str | int | None = None,
) -> None:
    """记录同一渠道请求内部的一次上游尝试；不创建新的 request_logs 行。"""
    finished_at = time.time()
    error_text = ""
    if error is not None:
        error_text = (
            _error_message_from_detail(error.detail)
            if isinstance(error, HTTPException)
            else str(error)
        )
    attempts.append({
        "attempt_no": int(attempt_no),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": max(1, int(round((finished_at - started_at) * 1000))),
        "status": "success" if success else "error",
        "upstream_status": int(upstream_status) if str(upstream_status or "").isdigit() else upstream_status,
        "error": error_text,
    })


def _channel_retry_total_duration(attempts: list[dict]) -> int:
    return sum(int(item.get("duration_ms") or 0) for item in attempts)


def _is_transient_request_exception(error: Exception) -> bool:
    """只把明确的网络/传输/超时异常交给渠道重试。"""
    return isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError, ConnectionError))


def _inner_retry_limit(provider_name: str | None, *, is_test: bool = False, is_probe: bool = False) -> int:
    """单个候选上最多发几次上游请求（同账号原地重发，不重新选路）。

    管理端 ``retry_count`` 是「首次之外的额外重试次数」：配 1 → 首次失败后同账号再发
    1 次（本候选最多 2 次）；配 4 → 最多 5 次。内层次数**不消耗也不受外层全局预算
    截断**：本候选的这些请求全部失败后，才交 ``retry.max_retries`` 的外层选路换候选。
    测试/探测模式不放大上游请求（探测只发一次，避免对已知异常账号反复打）。
    """
    if is_test or is_probe or not provider_name:
        return 1
    try:
        return max(1, config.Config.get_provider_retry_count(provider_name) + 1)
    except Exception:
        return 1


def _inner_retry_extra_status_codes(provider_name: str | None) -> set[int]:
    """渠道配置的额外内层重试状态码；读不到时按空集合（只保留 429/5xx 默认口径）。"""
    if not provider_name:
        return set()
    try:
        return config.Config.get_provider_extra_retry_status_codes(provider_name) or set()
    except Exception:
        return set()


def _should_inner_retry(
    *,
    error: Exception,
    provider_name: str | None,
    stream: bool,
    downstream_started: bool,
    inner_index: int,
    inner_limit: int,
) -> bool:
    """内层同账号原地重试的最终判定：既要够资格，也要还有次数余额。

    资格判定收口在 ``retry_policy.classify_inner_retry``，这里只补两件调用方才知道的
    事：内层预算是否用完，以及渠道配置的额外状态码。
    """
    if inner_index >= inner_limit - 1:
        return False
    extra_retry_status_codes = _inner_retry_extra_status_codes(provider_name)
    non_retryable_override = None
    if isinstance(error, HTTPException):
        non_retryable_override = _is_non_retryable_upstream_error(error)
    return retry_policy.classify_inner_retry(retry_policy.FailureInput(
        is_http_exception=isinstance(error, HTTPException),
        status_code=error.status_code if isinstance(error, HTTPException) else None,
        detail=error.detail if isinstance(error, HTTPException) else str(error),
        downstream_started=bool(stream and downstream_started),
        stream=stream,
        cancelled=False,
        # 内层判定与外层候选余额无关：本次是否能原地重发只看错误性质 + 内层预算。
        candidates_remaining=True,
        upstream_started=True,
        non_retryable_override=non_retryable_override,
        transient_exception=_is_transient_request_exception(error),
        retryable_incomplete_response=isinstance(error, (IncompleteStreamError, EmptyNonStreamResponseError)),
        extra_retry_status_codes=extra_retry_status_codes,
    ))


async def _handle_attempt_failure(
    *,
    error: Exception,
    route_info: dict,
    provider_name: str | None,
    username: str | None,
    model: str,
    retry_count: int,
    total_attempts: int,
    request_id: str | None,
    messages: list,
    stream: bool,
    downstream_started: bool,
    api_key: str | None,
    api_key_name: str | None,
    request_headers: dict | None,
    client_request_path: str | None,
    client_request_body: dict | None,
    attempt_start: float,
    attempt_stream_summary: dict | None,
    client,
    session_id: str | None,
    is_test: bool,
    is_probe: bool = False,
    tried_accounts: list[tuple[str, str]],
    inner_retry_pending: bool = False,
    finalize_log: bool = True,
) -> tuple[retry_policy.FailureDecision, dict]:
    """一次失败的统一收口：日志、健康度、冻结/排除和重试决策各只执行一处。

    ``inner_retry_pending=True`` 表示本次失败之后还要在**同一账号**上原地重发（渠道内层
    重试未耗尽）。此时只做「每次请求都该做」的部分——落一条独立日志行 + 记健康度——
    而跳过候选级收尾：不追加 ``tried_accounts``、不做冻结/cooldown。否则内层重试会把
    自己用的账号排除掉，导致内层耗尽后外层反而选不到它，也会让一次瞬时 429 被当成多次
    独立失败去冻结账号。

    失败收口口径随请求模式分流：
    - ``is_test``（手动测试）：不记健康度、不冻结——测试只验证连通性，不改账号态。
    - ``is_probe``（定时检测）：与正常请求完全一致地记健康度 + 冻结，不再有独立开关。
    - 默认（正常请求）：完整健康度 + 冻结。

    「失败刷新冻结周期」不再是请求级开关：已冻结对象是否按新周期重设 TTL，由失败对象
    所属渠道的 ``freeze_policy.refresh_freeze_on_failure`` 在 ProviderPool 冻结写入点判定，
    对正常请求、定时检测、响应头规则一视同仁。
    """
    error_text = _error_message_from_detail(error.detail) if isinstance(error, HTTPException) else str(error)
    retryable_status = (
        not isinstance(error, HTTPException)
        or error.status_code == 429
        or error.status_code >= 500
    )
    # 是否执行健康度/冻结收口：手动测试永不执行；定时检测与正常请求一致执行。
    # 「已冻结是否刷新 TTL」下沉到渠道配置判定，不在此按请求模式分流。
    apply_failure_handling = not is_test

    if provider_name:
        if apply_failure_handling:
            ModelClientPool.record_channel_failure(provider_name, username, error_text)
            if retryable_status:
                ModelClientPool.record_account_failure(
                    provider_name, username, error_text, session_id=session_id
                )
        if finalize_log:
            route_info = await _finalize_failed_attempt(
                error=error,
                route_info=route_info,
                provider_name=provider_name,
                username=username,
                model=model,
                retry_count=retry_count,
                request_id=request_id,
                messages=messages,
                stream=stream,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                client_request_path=client_request_path,
                client_request_body=client_request_body,
                attempt_start=attempt_start,
                attempt_stream_summary=attempt_stream_summary,
                status="empty_non_stream" if isinstance(error, EmptyNonStreamResponseError) else "error",
            )
        else:
            route_info = dict(route_info)
            route_info["status"] = "error"
            route_info["error"] = error_text
            if isinstance(error, HTTPException):
                route_info["upstream_status"] = str(error.status_code)
        if client is not None and apply_failure_handling and not inner_retry_pending:
            tried_accounts.append((provider_name, username))
            pool = ModelClientPool.get_provider_pool(provider_name)
            if pool:
                if isinstance(error, HTTPException):
                    # 所有上游 HTTP 异常先走渠道 freeze_policy（状态码/响应头/错误码/模型都是匹配
                    # 上下文，如 403 当天不可用）。命中规则即冻结；未命中且可重试时才走
                    # handle_response_error 的全局 cooldown 兜底；未命中且不可重试（普通参数错）不冻结。
                    if not pool.apply_freeze_policy(
                        username,
                        status_code=error.status_code,
                        headers=route_info.get("response_headers"),
                        model_id=route_info.get("routed_model") or model,
                        reason=error_text,
                        error_code=getattr(error, "code", None),
                    ) and retryable_status:
                        pool.handle_response_error(username, error.status_code, error_text, model_id=route_info.get("routed_model") or model)
                else:
                    if not isinstance(error, EmptyNonStreamResponseError):
                        pool.mark_account_exception(
                            username,
                            reason=error_text,
                            error_code=getattr(error, "code", None),
                            model_id=route_info.get("routed_model") or model,
                        )
    else:
        route_info = {
            "provider": None,
            "account": None,
            "requested_model": model,
            "routed_model": model,
            "retry": retry_count,
            "status": "error",
            "error": error_text,
        }
        if isinstance(error, HTTPException):
            route_info["upstream_status"] = str(error.status_code)

    non_retryable_override = None
    if isinstance(error, HTTPException):
        non_retryable_override = _is_non_retryable_upstream_error(error)
    decision = retry_policy.classify_failure(retry_policy.FailureInput(
        is_http_exception=isinstance(error, HTTPException),
        status_code=error.status_code if isinstance(error, HTTPException) else None,
        detail=error.detail if isinstance(error, HTTPException) else str(error),
        downstream_started=bool(stream and downstream_started),
        stream=stream,
        cancelled=False,
        candidates_remaining=retry_count < total_attempts - 1,
        upstream_started=client is not None,
        non_retryable_override=non_retryable_override,
        transient_exception=_is_transient_request_exception(error),
        # 流式不完整响应（空流/零 completion，且尚未向客户端输出）与非流式空响应都视为
        # 候选渠道响应质量问题，允许切候选/同账号重试；已输出由 downstream_started 分支优先阻止。
        retryable_incomplete_response=isinstance(error, (IncompleteStreamError, EmptyNonStreamResponseError)),
        # 渠道额外重试码只在内层同账号判定里起作用（见 classify_inner_retry）；传给
        # classify_failure 是为了让外层决策拿到完整输入快照，不改变外层换候选口径。
        extra_retry_status_codes=_inner_retry_extra_status_codes(provider_name),
    ))
    return decision, route_info


async def _api_key_name(api_key: str | None) -> str:
    if not api_key:
        return ""
    cfg = await config.Config.get_api_key_config(api_key) or {}
    return cfg.get("name") or cfg.get("remark") or api_key[:12]


def _sanitize_headers(headers) -> dict:
    from security import sanitize_header_value

    result = {}
    for key, value in dict(headers or {}).items():
        result[str(key)] = sanitize_header_value(key, value)
    return result


def _json_response_headers() -> dict:
    return {"content-type": "application/json"}


def _request_stream_enabled(body: dict | None) -> bool:
    """Return a strict boolean for client stream flag.

    Some clients/proxies send JSON-like booleans as strings ("false", "0").
    Treating those with Python truthiness routes a non-stream request into the
    streaming path, which can later be logged as cancelled when the client reads
    it as a normal JSON response.
    """
    if not isinstance(body, dict):
        return False
    value = body.get("stream", False)
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("", "0", "false", "no", "off", "null", "none"):
            return False
        if normalized in ("1", "true", "yes", "on"):
            return True
    return bool(value)


def _detect_header_client_type(headers: dict | None) -> str | None:
    normalized = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
    user_agent = normalized.get("user-agent", "").lower()
    # Codex 客户端身份由 originator + UA 判定（两者都是 codex 专有，不与其它客户端冲突）：
    # - 终端版（codex-tui）：originator: codex-tui、UA codex-tui/<ver> ... (codex-tui; <ver>)；
    #   旧版 UA 形如 codex_cli_rs/...，也归 tui。
    # - 桌面版：originator / UA 含 Codex Desktop。
    originator = normalized.get("originator", "").lower()
    if originator == "codex-tui" or "codex-tui/" in user_agent or "codex_cli" in user_agent:
        return "codex-tui"
    if "codex desktop" in originator or "codex desktop" in user_agent:
        return "codex-cli"

    if "claude-cli/" in user_agent or user_agent == "claude-code":
        return "claude-code"
    if "opencode/" in user_agent or user_agent.startswith("opencode "):
        return "opencode"
    if "cursor" in user_agent:
        return "cursor"
    if "cline" in user_agent:
        return "cline"
    if "roo-code" in user_agent:
        return "roo-code"
    if "geminicli" in user_agent or "gemini" in user_agent:
        return "gemini-cli"
    # WorkBuddy（腾讯 CodeBuddy 桌面客户端）：user-agent 含 workbuddy/，或显式带
    # x-ide-name=WorkBuddy / x-codebuddy-request=1 兜底（CLI 版 CodeBuddy 也会带后者）。
    if "workbuddy/" in user_agent or normalized.get("x-ide-name", "").lower() == "workbuddy" or normalized.get("x-codebuddy-request") == "1":
        return "workbuddy"
    # 兜底放在所有具名 UA 之后：中间代理剥掉 UA/originator 时，x-codex-turn-metadata /
    # x-codex-window-id 仍是 codex 专属头，出现即按终端版处理（codex 流量的大头）。
    if normalized.get("x-codex-turn-metadata") or normalized.get("x-codex-window-id"):
        return "codex-tui"
    return None


def _content_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
        return " ".join(parts)
    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"]
        if isinstance(content.get("content"), str):
            return content["content"]
    return ""


def _detect_message_client_type(body: dict | None) -> str | None:
    if not isinstance(body, dict):
        return None

    # Responses 协议的 Codex 请求没有 messages，身份声明放在 instructions；
    # 代理剥掉 headers 时仍可通过这个强特征识别。
    instructions = body.get("instructions")
    instruction_text = _content_text(instructions).strip().lower()
    if instruction_text.startswith("you are codex") or "coding agent based on gpt-5" in instruction_text:
        return "codex-tui"

    messages = body.get("messages")
    if isinstance(messages, list) and messages:
        first_message = messages[0]
        if isinstance(first_message, dict) and first_message.get("role") in ("system", "developer"):
            text = _content_text(first_message.get("content")).strip().lower()
            if text.startswith("you are claude code") or "anthropic's official cli for claude" in text:
                return "claude-code"
            if text.startswith("you are codex") or "coding agent based on gpt-5" in text:
                return "codex-tui"
            if text.startswith("you are a title generator") and "you output only a thread title" in text:
                return "opencode"

    # 某些客户端会把系统消息全部放入 Responses input（developer message）。
    input_items = body.get("input")
    if isinstance(input_items, list):
        for item in input_items[:4]:
            if not isinstance(item, dict) or item.get("role") not in ("system", "developer"):
                continue
            text = _content_text(item.get("content")).strip().lower()
            if "you are codex" in text or "coding agent based on gpt-5" in text:
                return "codex-tui"
    return None


def _detect_body_client_type(body: dict | None) -> str | None:
    if not isinstance(body, dict):
        return None
    for key in ("client_type", "client_preset"):
        value = body.get(key)
        if value:
            return str(value).lower()
    return None


def _detect_client_type(headers_or_body: dict | None, request_headers: dict | None = None, path: str | None = None) -> str:
    body = headers_or_body if request_headers is not None else None
    headers = request_headers if request_headers is not None else headers_or_body
    client_type = _detect_header_client_type(headers) or _detect_body_client_type(body) or _detect_message_client_type(body)
    if client_type:
        return client_type
    if path and "messages" in path:
        return "anthropic"
    if path and "responses" in path:
        return "responses"
    return "openai" if path else "unknown"


# client_type → 管理端/日志展示用的中文标签。_client_label 据此回退。
# 之前引用了未定义的 CLIENT_PRESET_LABELS（运行即 NameError），此处一并补齐。
CLIENT_PRESET_LABELS = {
    "claude-code": "Claude Code",
    "codex-cli": "Codex CLI",
    "codex-tui": "Codex TUI",
    "codex-openai": "Codex OpenAI",
    "opencode": "OpenCode",
    "cursor": "Cursor",
    "cline": "Cline",
    "roo-code": "Roo Code",
    "gemini-cli": "Gemini CLI",
    "workbuddy": "WorkBuddy (Tencent)",
    "anthropic": "Anthropic",
    "responses": "Responses",
    "openai": "OpenAI",
}


def _client_label(client_type: str | None) -> str:
    if not client_type or client_type == "unknown":
        return "未知"
    return CLIENT_PRESET_LABELS.get(client_type, client_type)


async def _api_key_thinking_config(api_key: str | None) -> dict:
    if not api_key:
        return {}
    cfg = await config.Config.get_api_key_config(api_key) or {}
    thinking = cfg.get("thinking_config") or {}
    return thinking if isinstance(thinking, dict) and thinking.get("enabled") else {}


def _thinking_config_supports_client(thinking: dict, client_type: str) -> bool:
    client_scope = thinking.get("client_scope") or thinking.get("scope")
    clients = thinking.get("client_types")
    if clients is None:
        clients = thinking.get("clients") or []
    if client_scope == "all":
        return True
    if client_scope == "selected":
        return client_type in clients
    return not clients or client_type in clients


def _thinking_config_mode(thinking: dict) -> str:
    mode = str(thinking.get("thinking_mode") or thinking.get("mode") or "").strip().lower()
    if mode in {"auto", "thinking", "none"}:
        return mode
    return "thinking" if thinking.get("enabled") is True else ""


def _inject_thinking_for_protocol(body: dict, mode: str, max_thinking_tokens: int, protocol: str) -> dict:
    """按协议注入 thinking/reasoning 标准参数（不注入任何非标准参数）。

    "关闭思考"（mode=none）的语义：省略对应参数，不显式下发 none
    （与 _convert_thinking_for_protocol 保持一致）。anthropic 用 {type:disabled}
    表达关闭（这是 anthropic 协议的标准关闭表示）。
    """
    body = dict(body)
    if protocol == "openai":
        if mode == "none":
            body.pop("reasoning_effort", None)
        elif mode == "auto":
            body["reasoning_effort"] = "medium"
        elif mode == "thinking":
            body["reasoning_effort"] = "high"
    elif protocol == "anthropic":
        if mode == "none":
            body["thinking"] = {"type": "disabled"}
        else:
            body["thinking"] = {"type": "enabled", "budget_tokens": max_thinking_tokens}
    elif protocol == "responses":
        if mode == "none":
            body.pop("reasoning", None)
        elif mode == "auto":
            body["reasoning"] = {"effort": "medium", "max_tokens": max_thinking_tokens}
        elif mode == "thinking":
            body["reasoning"] = {"effort": "high", "max_tokens": max_thinking_tokens}
    return body


def _apply_api_key_thinking(body: dict, thinking: dict, client_type: str, protocol: str) -> dict:
    if not thinking or thinking.get("enabled") is not True or not _thinking_config_supports_client(thinking, client_type):
        return body
    # 客户端已显式带该协议标准 thinking 参数 → 不覆盖
    if protocol == "openai" and body.get("reasoning_effort") is not None:
        return body
    if protocol == "anthropic" and body.get("thinking") is not None:
        return body
    if protocol == "responses" and body.get("reasoning") is not None:
        return body
    mode = _thinking_config_mode(thinking)
    if not mode:
        return body
    max_thinking_tokens = int(
        thinking.get("max_thinking_tokens")
        or thinking.get("thinking_max_tokens")
        or thinking.get("budget_tokens")
        or 1024
    )
    return _inject_thinking_for_protocol(body, mode, max_thinking_tokens, protocol)


async def _prepare_client_request_body(body: dict, api_key: str | None, headers: dict | None, endpoint: str, protocol: str = "openai") -> tuple[dict, str]:
    if model_catalog.is_internal_model_id(body.get("model")):
        raise HTTPException(
            status_code=400,
            detail=_openai_error(
                "The requested model is not available.",
                "invalid_request_error",
                "model_not_found",
                "model",
            ),
        )
    client_type = _detect_client_type(body, headers, endpoint)
    body = _apply_api_key_thinking(body, await _api_key_thinking_config(api_key), client_type, protocol)
    return body, client_type


def _has_first_token_content(chunk):
    if chunk is None:
        return False
    if isinstance(chunk, str):
        lines = chunk.splitlines()
        looks_like_sse = any(line.lstrip().startswith(("data:", "event:", "id:", "retry:", ":")) for line in lines)
        if not looks_like_sse:
            return bool(chunk.strip())
        for line in lines:
            line = line.strip()
            if not line or line.startswith(":"):
                continue
            if not line.startswith("data:"):
                continue
            line = line[5:].strip()
            if not line or line == "[DONE]":
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                return True
            if _has_first_token_content(payload):
                return True
        return False
    if isinstance(chunk, dict):
        for choice in chunk.get("choices", []) or []:
            delta = choice.get("delta") or {}
            if isinstance(delta, str):
                if delta.strip():
                    return True
            elif isinstance(delta, dict):
                if delta.get("content") or delta.get("text") or delta.get("reasoning_content") or delta.get("reasoning") or delta.get("thinking") or delta.get("thinking_delta") or delta.get("tool_calls") or delta.get("function_call"):
                    return True
            message = choice.get("message") or {}
            if isinstance(message, str):
                if message.strip():
                    return True
            elif isinstance(message, dict) and (message.get("content") or message.get("tool_calls") or message.get("function_call")):
                return True
            text = choice.get("text")
            if text:
                return True
        if chunk.get("type") == "content_block_start":
            content_block = chunk.get("content_block") or {}
            if isinstance(content_block, dict) and content_block.get("type") in ("text", "thinking", "tool_use"):
                return True
        if chunk.get("type") == "content_block_delta":
            delta_obj = chunk.get("delta") or {}
            if isinstance(delta_obj, str):
                if delta_obj.strip():
                    return True
            elif isinstance(delta_obj, dict) and (delta_obj.get("text") or delta_obj.get("thinking") or delta_obj.get("partial_json") or delta_obj.get("input_json_delta")):
                return True
        if chunk.get("type") == "response.output_item.added":
            item = chunk.get("item") or {}
            if isinstance(item, dict) and item.get("type") in ("message", "function_call"):
                return True
        if chunk.get("type") == "text_delta" and chunk.get("text"):
            return True
        if chunk.get("type") == "thinking_delta" and chunk.get("thinking"):
            return True
        delta = chunk.get("delta") or {}
        if isinstance(delta, str):
            if delta.strip():
                return True
        elif isinstance(delta, dict) and (delta.get("content") or delta.get("text") or delta.get("reasoning_content") or delta.get("reasoning") or delta.get("thinking") or delta.get("thinking_delta") or delta.get("tool_calls") or delta.get("function_call")):
            return True
        if chunk.get("text") or chunk.get("content") or chunk.get("tool_calls") or chunk.get("function_call") or chunk.get("thinking") or chunk.get("reasoning_content") or chunk.get("reasoning"):
            return True
        return False
    return bool(chunk)


def _stream_response_headers() -> dict:
    return {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache",
        "connection": "keep-alive",
        "x-accel-buffering": "no",
    }


def _openai_stream_error_chunk(message: str, code: str = "rate_limit_exceeded", error_type: str = "rate_limit_error", status_code: int = 429) -> str:
    payload = {"error": {"message": normalize_upstream_error_message(message) or message, "type": error_type, "code": code, "status_code": status_code}}
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


class StreamStartedChannelError(Exception):
    def __init__(self, message: str, status_code: int = 429, code: str = "rate_limit_exceeded", error_type: str = "rate_limit_error", last_route_info: dict | None = None, upstream_status: int | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.error_type = error_type
        self.last_route_info = last_route_info or {}
        self.upstream_status = upstream_status


def _stream_error_from_exception(error: Exception, last_route_info: dict | None = None) -> StreamStartedChannelError:
    upstream_status = error.status_code if isinstance(error, HTTPException) else None
    # 默认口径：绝不把上游/内部原始错误文案透传给客户端。流式失败（已开始输出后上游报错、
    # 或重试耗尽）与非流式最终 429 对齐——统一 429 + rate_limit + 英文业务 message。
    # 真实 upstream_status 仍随 last_route_info/日志保留，供内部排查。
    message = TERMINAL_ERROR_MESSAGES.get("rate_limit_exceeded", "Rate limit exceeded; please retry later.")
    code = "rate_limit_exceeded"
    error_type = "rate_limit_error"
    status_code = 429
    if isinstance(error, HTTPException):
        # 唯一例外：上下文超限。零 IO 的硬编码超限识别先行，命中且开关开时才归一。
        canonical = retry_policy.canonicalize_upstream_error(upstream_status, error.detail)
        if canonical is not None and config.Config.context_overflow_not_retryable_enabled():
            # 超限统一出口：message/code 由分类查表固定，不来自上游。
            message = canonical.message
            code = canonical.code
            error_type = canonical.type
            status_code = canonical.status_code
    return StreamStartedChannelError(
        message,
        status_code=status_code,
        code=code,
        error_type=error_type,
        last_route_info=last_route_info or getattr(error, "_last_route_info", {}) or {},
        upstream_status=upstream_status,
    )


async def _prepend_stream_chunk(first_chunk, stream_iter):
    try:
        yield first_chunk
        async for chunk in stream_iter:
            yield chunk
    finally:
        close = getattr(stream_iter, "aclose", None)
        if close:
            await close()


async def _prime_stream_before_response(stream_iter):
    while True:
        try:
            chunk = await anext(stream_iter)
        except StopAsyncIteration:
            raise HTTPException(status_code=500, detail="模型无响应")
        if isinstance(chunk, dict) and "_last_route_info" in chunk:
            continue
        return chunk


def _duration_ms(start: float) -> int:
    return max(1, int(round((time.time() - start) * 1000)))


def _last_route_field(last_route_info: dict | None, key: str, default=None):
    if isinstance(last_route_info, dict):
        return last_route_info.get(key, default)
    return default


def _last_route_first_token_ms(last_route_info: dict | None) -> int | None:
    value = _last_route_field(last_route_info, "ttft_ms")
    if value is None:
        return None
    return max(1, int(value))


def _last_route_response_headers(last_route_info: dict | None) -> dict:
    return _last_route_field(last_route_info, "response_headers", {}) or {}


def _last_route_upstream_status(last_route_info: dict | None) -> str | None:
    status = _last_route_field(last_route_info, "upstream_status")
    if status:
        return str(status)
    headers = _last_route_response_headers(last_route_info)
    status = headers.get(":status")
    return str(status) if status else None


def _append_limited_router_response(route_info: dict, body) -> None:
    if body is None:
        return
    if isinstance(body, str) and len(body) > 20000 and not config.Config.system_debug_enabled():
        body = body[:20000] + "...[truncated]"
    items = route_info.setdefault("router_response_body", [])
    if isinstance(items, list):
        items.append(body)
    else:
        route_info["router_response_body"] = body



# tokenizer 规则由 usage_utils 内置模型族策略维护，不从用户主配置读取。
# set_tokenizer_rules_getter 仍保留给兼容测试/内部校准，不在生产启动时安装配置 getter。



def _normalize_usage(usage: dict | None) -> dict:
    return normalize_usage(usage)


def _usage_value(usage: dict, *keys: str) -> int:
    return usage_value(usage, *keys)


def _usage(result: dict, request_body: dict | None = None, estimate: bool = True, model: str = "") -> dict:
    usage = result.get("usage") or {}
    normalized = normalize_usage(usage)
    if normalized["prompt_tokens"] + normalized["completion_tokens"] + normalized["cached_tokens"] + normalized["cache_creation_tokens"] + normalized["reasoning_tokens"] <= 0 and estimate and request_body is not None:
        return estimate_usage(request_body, result, model=model)
    return normalized


UPSTREAM_ZERO_COMPLETION_MESSAGE = "upstream response usage reports zero completion tokens"


def _usage_has_zero_completion(usage: dict | None) -> bool:
    if not isinstance(usage, dict):
        return False
    normalized = normalize_usage(usage)
    return normalized["completion_tokens"] <= 0


def _raise_zero_completion_usage(stream: bool) -> None:
    if stream:
        raise IncompleteStreamError(UPSTREAM_ZERO_COMPLETION_MESSAGE, "upstream_incomplete_usage")
    raise EmptyNonStreamResponseError(UPSTREAM_ZERO_COMPLETION_MESSAGE)


def _response_has_content(data: dict) -> bool:
    """检查响应块是否实际包含生成内容"""
    if not isinstance(data, dict):
        return False
    # 内部封装对象（如 chat_anthropic 非流式返回的 {"_passthrough_anthropic": True, "body": {...}}）：
    # 真实内容在嵌套的 body 里，必须递归识别。
    body = data.get("body")
    if isinstance(body, dict) and _response_has_content(body):
        return True
    choices = data.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            msg = choice.get("message") or choice.get("delta")
            if isinstance(msg, dict):
                if msg.get("content") or msg.get("tool_calls") or msg.get("reasoning_content") or msg.get("reasoning"):
                    return True
    if isinstance(data.get("content"), (list, str)) and data["content"]:
        return True
    if isinstance(data.get("events"), list) and data["events"]:
        return True
    return False


def _append_content_text(content, parts: list[str]) -> None:
    """把 OpenAI content（str 或多模态 parts 列表）里的文本片段追加到 parts。"""
    if isinstance(content, str):
        parts.append(content)
    elif isinstance(content, list):
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if isinstance(text, str):
                    parts.append(text)


def _collect_response_content_text(data, parts: list[str]) -> None:
    """递归收集响应里的可见文本（content 字段），供正则匹配。

    覆盖 OpenAI choices[].message/delta.content、Responses API 的 output_text/
    output[].content[].text、Anthropic passthrough 嵌套 body，以及顶层 content/events。
    reasoning 不计入，避免 thinking 文本误命中拦截规则。
    """
    if not isinstance(data, dict):
        return
    body = data.get("body")
    if isinstance(body, dict):
        _collect_response_content_text(body, parts)
    response = data.get("response")
    if isinstance(response, dict):
        _collect_response_content_text(response, parts)
    output_text = data.get("output_text")
    if isinstance(output_text, str):
        parts.append(output_text)
    delta = data.get("delta")
    if isinstance(delta, dict):
        _append_content_text(delta.get("text") or delta.get("content"), parts)
    elif isinstance(delta, str) and data.get("type") == "response.output_text.delta":
        parts.append(delta)
    content_block = data.get("content_block")
    if isinstance(content_block, dict):
        _append_content_text(content_block.get("text") or content_block.get("content"), parts)
    choices = data.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            msg = choice.get("message") or choice.get("delta")
            if isinstance(msg, dict):
                _append_content_text(msg.get("content"), parts)
                _append_content_text(msg.get("text"), parts)
    candidates = data.get("candidates")
    if isinstance(candidates, list):
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            candidate_content = candidate.get("content")
            if not isinstance(candidate_content, dict):
                continue
            for part in candidate_content.get("parts") or []:
                if isinstance(part, dict):
                    _append_content_text(part.get("text"), parts)
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            # 显式取 item.text；item.content（多模态 parts 列表）交给下方递归统一处理，避免重复。
            _append_content_text(item.get("text"), parts)
            _collect_response_content_text(item, parts)
    _append_content_text(data.get("content"), parts)
    events = data.get("events")
    if isinstance(events, list):
        for event in events:
            if isinstance(event, dict):
                _append_content_text(event.get("text") or event.get("content"), parts)
                _collect_response_content_text(event, parts)


def _extract_response_content_text(body) -> str:
    """从非流式响应体提取可见文本，拼成单段字符串供正则匹配。"""
    parts: list[str] = []
    _collect_response_content_text(body, parts)
    return "".join(parts)


def _intercepted_by_output_rule(content: str) -> str | None:
    """异常输出拦截：content 命中任一启用规则即返回规则名，否则 None。

    零 IO 判定（编译后的规则已缓存）；空 content 直接返回 None，让现有的
    空响应/零 completion 兜底先走。整体开关关闭时 get_compiled 返回空列表，本函数自然放行。
    """
    if not content:
        return None
    patterns = config.Config.get_compiled_output_interception_patterns()
    for name, match_type, matcher in patterns:
        if match_type == "text":
            if matcher in content:
                return name
        elif matcher.search(content):
            return name
    return None


def _validate_upstream_usage_payload(payload, stream: bool = False, had_content: bool = False) -> None:
    payload_has_content = had_content
    zero_usage_payloads: list[dict] = []
    for data in _stream_payloads(payload):
        if _response_has_content(data):
            payload_has_content = True
        usage = _extract_usage_payload(data)
        if usage is None or not _usage_has_zero_completion(usage):
            continue
        zero_usage_payloads.append(data)

    if not zero_usage_payloads:
        return

    # 有真实内容时，零 completion usage 只是不可靠的上游统计，不能判失败。
    # 删除顶层 usage，后续 _usage(..., estimate=True) / 流式估算会按内容兜底。
    if payload_has_content:
        for data in zero_usage_payloads:
            if "usage" in data:
                del data["usage"]
        return

    _raise_zero_completion_usage(stream)


def _chunk_has_stream_content(chunk) -> bool:
    """协议无关的内容检测：判断原始 chunk 是否包含实际生成内容。

    覆盖 OpenAI / Gemini（经 base.py 转为 OpenAI 格式）、
    Anthropic（content_block_delta / content_block_start）、
    Responses API（response.output_text.delta / response.output_item.added）等格式。
    """
    if isinstance(chunk, dict):
        return _response_has_content(chunk) or _has_first_token_content(chunk)
    if not isinstance(chunk, str):
        return False
    has_event_line = False
    for line in chunk.split("\n"):
        stripped = line.strip()
        if stripped.startswith("event:"):
            has_event_line = True
            continue
        if not stripped.startswith("data:"):
            continue
        data = stripped[5:].strip()
        if not data or data == "[DONE]":
            continue
        try:
            obj = json.loads(data)
        except (json.JSONDecodeError, ValueError):
            if any(marker in data for marker in (
                "content_block_delta", "content_block_start",
                "response.output_text.delta", "response.output_item.added",
            )):
                return True
            continue
        if not isinstance(obj, dict):
            continue
        for choice in obj.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta") or {}
            if isinstance(delta, dict):
                if delta.get("content") or delta.get("tool_calls") or delta.get("reasoning_content") or delta.get("reasoning"):
                    return True
        obj_type = obj.get("type", "")
        if not isinstance(obj_type, str):
            obj_type = ""
        if "content_block_delta" in obj_type:
            delta = obj.get("delta") or {}
            if isinstance(delta, dict) and (delta.get("text") or delta.get("partial_json") is not None):
                return True
        if "content_block_start" in obj_type:
            cb = obj.get("content_block") or {}
            if isinstance(cb, dict):
                if cb.get("type") == "tool_use":
                    return True
                if cb.get("type") == "text" and cb.get("text"):
                    return True
        if obj_type in ("response.output_text.delta", "response.output_item.added"):
            return True
    return False


def _validate_stream_completion(accumulated_usage: dict | None, had_content: bool) -> None:
    """流结束后统一校验：只有缺乏真实内容才视为不完整流。

    Anthropic/Responses 协议转换会产生 output_tokens=0 的中间 usage 事件；
    已有真实输出时不能因此判失败，usage 缺失/为 0 由后续统计估算兜底。
    """
    if not had_content:
        raise IncompleteStreamError(UPSTREAM_ZERO_COMPLETION_MESSAGE, "upstream_incomplete_usage")


def _zero_usage() -> dict:
    return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "cached_tokens": 0, "cache_creation_tokens": 0, "reasoning_tokens": 0}


def _stream_payloads(chunk) -> list[dict]:
    if isinstance(chunk, dict):
        return [chunk]
    if not isinstance(chunk, str):
        return []
    return [payload for payload in iter_sse_payloads(chunk) if isinstance(payload, dict)]


def _collect_stream_response_content_text(chunk, parts: list[str]) -> None:
    """Parse every SSE payload through the protocol-agnostic response text extractor."""
    for payload in _stream_payloads(chunk):
        _collect_response_content_text(payload, parts)


def _merge_usage_dict(current: dict | None, incoming: dict | None) -> dict:
    merged = dict(current or {})
    if not isinstance(incoming, dict):
        return merged
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_usage_dict(merged.get(key), value)
        elif value is not None and value != 0:
            merged[key] = value
    return merged


def _extract_usage_payload(data: dict) -> dict | None:
    if not isinstance(data, dict):
        return None
    if isinstance(data.get("usage"), dict):
        return data["usage"]
    response = data.get("response")
    if isinstance(response, dict) and isinstance(response.get("usage"), dict):
        return response["usage"]
    message = data.get("message")
    if isinstance(message, dict) and isinstance(message.get("usage"), dict):
        return message["usage"]
    for choice in data.get("choices") or []:
        if not isinstance(choice, dict):
            continue
        if isinstance(choice.get("usage"), dict):
            return choice["usage"]
        msg = choice.get("message")
        if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
            return msg["usage"]
    return None


def _usage_from_stream_chunk(chunk) -> dict | None:
    usage = {}
    for data in _stream_payloads(chunk):
        payload = _extract_usage_payload(data)
        if payload:
            usage = _merge_usage_dict(usage, payload)
    if usage:
        prompt = usage_value(usage, "prompt_tokens", "input_tokens", "promptTokens", "inputTokens")
        completion = usage_value(usage, "completion_tokens", "output_tokens", "completionTokens", "outputTokens")
        total = usage_value(usage, "total_tokens", "total")
        if total > 0 and prompt + completion > total:
            usage["total_tokens"] = prompt + completion
    return usage or None


def _accumulate_stream_usage(accumulated: dict, chunk) -> bool:
    """从 chunk 中提取 usage 并合并到 accumulated（跳过零值）。返回是否有更新。"""
    updated = False
    for data in _stream_payloads(chunk):
        usage = _extract_usage_payload(data)
        if usage is not None:
            merged = _merge_usage_dict(accumulated, usage)
            accumulated.clear()
            accumulated.update(merged)
            updated = True
    return updated


def _new_stream_summary() -> dict:
    return {"content": "", "reasoning_content": "", "tool_calls": [], "finish_reason": None, "usage": {}}


def _new_stream_log_body() -> dict:
    return {"stream": True, "events": []}


def _append_stream_log_body(log_body: dict, chunk) -> None:
    if isinstance(chunk, str):
        log_body["events"].append(chunk)
    else:
        log_body["events"].append(chunk)


def _append_stream_summary(summary: dict, chunk) -> None:
    for data in _stream_payloads(chunk):
        usage_payload = _extract_usage_payload(data)
        if usage_payload:
            summary["usage"] = _merge_usage_dict(summary.get("usage"), usage_payload)
        for choice in data.get("choices", []) or []:
            delta = choice.get("delta") or {}
            if choice.get("finish_reason"):
                summary["finish_reason"] = choice.get("finish_reason")
            if delta.get("content"):
                summary["content"] += delta["content"]
            if delta.get("reasoning_content"):
                summary["reasoning_content"] += delta["reasoning_content"]
            for tool_call in delta.get("tool_calls") or []:
                index = int(tool_call.get("index") or 0)
                while len(summary["tool_calls"]) <= index:
                    summary["tool_calls"].append({"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                target = summary["tool_calls"][index]
                if tool_call.get("id"):
                    target["id"] = tool_call["id"]
                if tool_call.get("type"):
                    target["type"] = tool_call["type"]
                function_delta = tool_call.get("function") or {}
                if function_delta.get("name"):
                    target["function"]["name"] += function_delta["name"]
                if function_delta.get("arguments"):
                    target["function"]["arguments"] += function_delta["arguments"]


def _stream_summary_to_openai_response(model: str, summary: dict) -> dict | None:
    if not (summary.get("content") or summary.get("reasoning_content") or summary.get("tool_calls") or summary.get("finish_reason") or summary.get("usage")):
        return None
    message = {"role": "assistant", "content": summary.get("content", "")}
    if summary.get("reasoning_content"):
        message["reasoning_content"] = summary["reasoning_content"]
    if summary.get("tool_calls"):
        message["tool_calls"] = summary["tool_calls"]
    return {
        "id": f"chatcmpl-log-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": summary.get("finish_reason"),
        }],
        "usage": summary.get("usage") or {},
    }


async def _collect_non_stream_result(stream_gen):
    """收集非流式结果，若为空则抛出 500"""
    result = None
    last_route_info = {}
    try:
        async for r in stream_gen:
            if isinstance(r, dict) and "_last_route_info" in r:
                last_route_info = r.get("_last_route_info") or last_route_info
                continue
            result = r
    except Exception as e:
        if last_route_info:
            setattr(e, "_last_route_info", last_route_info)
        raise
    if not result:
        raise HTTPException(status_code=500, detail="模型无响应")
    first_token_ms = _last_route_first_token_ms(last_route_info)
    if isinstance(result, dict):
        if last_route_info:
            result["_last_route_info"] = last_route_info
        if first_token_ms is not None:
            result["_first_token_ms"] = first_token_ms
    return result


@app.get("/v1/models")
@app.get("/models")
async def list_models(request: Request, authorization: Optional[str] = Header(None)):
    """获取模型列表 —— 返回内存里聚合好的 _models 中该 Key 有权调用的部分。

    鉴权与 /v1/chat/completions、/v1/messages 同源：_extract_api_key 校验 Key
    （api_keys 启用时缺失/无效 401、禁用/过期 403）。目录再按这把 Key 的白/黑
    名单裁剪，列出来的每一个 id 都是它当下真能调通的模型，客户端不会先看到再
    被 403。
    """
    api_key = await _extract_api_key(request, authorization, support_x_api_key=True)
    return await ModelClientPool.get_models_response(api_key=api_key)


# "关闭思考"的等价 effort 取值（跨协议均视为 disabled）
_DISABLED_EFFORTS = ("minimal", "none")


def _normalize_openai_effort(effort: str) -> str:
    """把 Anthropic output_config.effort 值域对齐到 OpenAI reasoning_effort 值域。

    OpenAI 接受 none/low/medium/high/xhigh；Anthropic 用 low/medium/high/max。
    仅 max↔xhigh 需要互映，其余原样透传。
    """
    if effort == "max":
        return "xhigh"
    return effort


def _normalize_anthropic_effort(effort: str) -> str:
    """把 OpenAI reasoning_effort 值域对齐到 Anthropic output_config.effort 值域。

    xhigh↔max 互映，其余原样透传。
    """
    if effort == "xhigh":
        return "max"
    return effort


def _convert_thinking_for_protocol(
    source_params: dict,
    source_protocol: str,
    target_protocol: str,
) -> dict:
    """跨协议 thinking 参数转换。

    提取源协议的 thinking 语义（是否开启 / effort 强度 / 思考预算），
    映射到目标协议标准字段。仅返回目标协议有对应字段的格式。

    约定（见记忆 feedback_protocol_passthrough / 用户确认）：
    - "关闭思考"一律通过"省略参数"表达，不再向 OpenAI 发 reasoning_effort:none。
    - Anthropic 的 thinking.type=="adaptive" 表示自适应思考（开启），
      不能当作关闭；其思考强度由 output_config.effort 决定。
    - effort 值域对齐：OpenAI 的 xhigh ↔ Anthropic 的 max，其余原样。
    """
    effort = None            # 目标为 openai 时使用（openai 值域）
    anthropic_effort = None  # 目标为 anthropic 时使用（anthropic 值域）
    budget = 1024
    enabled = True

    if source_protocol == "openai":
        raw_effort = source_params.get("reasoning_effort")
        if raw_effort:
            enabled = raw_effort not in _DISABLED_EFFORTS
            effort = raw_effort
            anthropic_effort = _normalize_anthropic_effort(raw_effort)
        else:
            # 用户用 thinking 对象传递（OpenAI 不标准但用户可能传）
            thinking_obj = source_params.get("thinking")
            if isinstance(thinking_obj, dict):
                enabled = thinking_obj.get("type") in ("enabled", "adaptive")
                budget = thinking_obj.get("budget_tokens", 1024)
                effort = "high" if enabled else "none"
                anthropic_effort = effort
    elif source_protocol == "anthropic":
        thinking_obj = source_params.get("thinking")
        if isinstance(thinking_obj, dict):
            # enabled=显式开启并给预算；adaptive=自适应开启（由模型/effort 决定深度）
            t_type = thinking_obj.get("type")
            enabled = t_type in ("enabled", "adaptive")
            budget = thinking_obj.get("budget_tokens", 1024)
            # 思考强度取自 output_config.effort（GA 参数，位于顶层 output_config 内）
            output_config = source_params.get("output_config")
            cfg_effort = output_config.get("effort") if isinstance(output_config, dict) else None
            if enabled:
                anthropic_effort = cfg_effort or "high"
                effort = _normalize_openai_effort(anthropic_effort)
            else:
                effort = "none"
                anthropic_effort = "none"
    elif source_protocol == "responses":
        reasoning_obj = source_params.get("reasoning")
        if isinstance(reasoning_obj, dict):
            raw_effort = reasoning_obj.get("effort", "medium")
            budget = reasoning_obj.get("max_tokens", 1024)
            enabled = raw_effort not in _DISABLED_EFFORTS if raw_effort else True
            effort = raw_effort
            anthropic_effort = _normalize_anthropic_effort(raw_effort) if raw_effort else None

    if effort is None:
        return {}

    disabled = not enabled or effort in _DISABLED_EFFORTS

    result = {}
    if target_protocol == "openai":
        # 关闭思考 → 省略参数（不发 reasoning_effort），开启 → 发规范化后的 effort
        if not disabled:
            result["reasoning_effort"] = _normalize_openai_effort(effort)
    elif target_protocol == "anthropic":
        if disabled:
            result["thinking"] = {"type": "disabled"}
        else:
            result["thinking"] = {"type": "enabled", "budget_tokens": budget}
            if anthropic_effort and anthropic_effort not in _DISABLED_EFFORTS:
                result["output_config"] = {"effort": anthropic_effort}
    elif target_protocol == "responses":
        # 关闭思考 → 省略 reasoning，开启 → 发规范化后的 effort
        if not disabled:
            result["reasoning"] = {"effort": _normalize_openai_effort(effort), "max_tokens": budget}
    # Gemini: 丢弃
    return result


def _build_protocol_kwargs(body: dict, target_protocol: str, source_protocol: str = "openai") -> dict:
    """按目标协议构建 kwargs。

    Args:
        body: 请求体（OpenAI / Anthropic / Responses 格式之一）
        target_protocol: 目标协议 (openai/anthropic/responses)
        source_protocol: body 的实际协议格式（默认 openai）

    Returns:
        适配目标协议的 kwargs dict，不包含任何非标准参数。
    """
    kwargs = {}

    # 通用参数（所有协议共有）。注意：stream 由调用方单独传入，不放 kwargs 以免与位置参数冲突
    common_params = (
        "temperature", "top_p", "top_k",
    )
    for key in common_params:
        val = body.get(key)
        if val is not None:
            kwargs[key] = val

    # 按源协议提取字段
    if source_protocol == "openai":
        # OpenAI body
        for key in (
            "max_tokens", "tools", "tool_choice",
            "reasoning_effort", "reasoning",
            "parallel_tool_calls", "store", "include",
            "truncation", "previous_response_id", "user", "stream_options",
            "metadata", "service_tier", "container", "context_management", "mcp_servers",
            "prompt_cache_key", "client_metadata", "input",
            "frequency_penalty", "presence_penalty", "repetition_penalty", "min_p", "top_a",
        ):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("stop") is not None:
            kwargs["stop"] = body["stop"]
        if body.get("stop_sequences") is not None and "stop" not in kwargs:
            kwargs["stop"] = body["stop_sequences"]
    elif source_protocol == "anthropic":
        # Anthropic body
        for key in ("max_tokens", "tools", "tool_choice", "metadata", "system"):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("stop_sequences") is not None:
            kwargs["stop"] = body["stop_sequences"]
        elif body.get("stop") is not None:
            kwargs["stop"] = body["stop"]
        if body.get("thinking") is not None:
            kwargs["thinking"] = body["thinking"]
        for key in ("container", "context_management", "mcp_servers"):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
    elif source_protocol == "responses":
        # Responses body
        for key in (
            "reasoning", "input", "instructions", "previous_response_id",
            "tools", "tool_choice", "parallel_tool_calls", "store", "include",
            "truncation", "user", "metadata", "prompt_cache_key", "client_metadata",
        ):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("max_output_tokens") is not None:
            kwargs["max_tokens"] = body["max_output_tokens"]
        elif body.get("max_tokens") is not None:
            kwargs["max_tokens"] = body["max_tokens"]

    # thinking 参数转换：源协议 → 目标协议
    thinking_converted = _convert_thinking_for_protocol(body, source_protocol, target_protocol)
    kwargs.update(thinking_converted)

    # stream_options 默认（仅 OpenAI 协议需要）
    if target_protocol == "openai" and body.get("stream") is True and "stream_options" not in kwargs:
        kwargs["stream_options"] = {"include_usage": True}

    return kwargs


def _post_process_anthropic_kwargs(kwargs: dict, body: dict) -> dict:
    """对 anthropic_to_openai_messages 返回的 kwargs 做后处理。

    - 将 Qwen 风格 thinking 参数转为 OpenAI 标准 reasoning_effort
    - 删除非标准 thinking 参数

    注意：思考强度以 _convert_thinking_for_protocol 的结果为准（它已读取
    anthropic 的 output_config.effort）。这里的 Qwen 风格映射仅作兜底，
    不覆盖已算好的 reasoning_effort，避免 output_config.effort 被拉平成 high。
    关闭思考一律省略 reasoning_effort，不发 none。
    """
    if kwargs.get("thinking_enabled") is True or kwargs.get("thinking_mode"):
        thinking_mode = kwargs.get("thinking_mode", "Fast")
        if "reasoning_effort" not in kwargs and kwargs.get("thinking_enabled") is True:
            kwargs["reasoning_effort"] = "medium" if thinking_mode == "Auto" else "high"
        for key in ("thinking_enabled", "thinking_mode", "auto_thinking", "thinking_budget", "thinking_format", "auto_search", "research_mode"):
            kwargs.pop(key, None)
    if body.get("stream") is True and "stream_options" not in kwargs and body.get("stream_options") is None:
        kwargs["stream_options"] = {"include_usage": True}
    return kwargs


def _responses_access_error(message: str, status_code: int, code: str) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail=_openai_error(message, _openai_error_type_for_status(status_code), code, "model"),
    )


async def _raise_responses_route_error(model: str, api_key: str | None, api_key_name: str | None, reason: str) -> None:
    is_group = await config.Config.is_model_group(model)
    has_runtime_route = bool(ModelClientPool.get_model_routes(model))
    has_info = bool(ModelClientPool.get_model_info(model))
    has_metadata = await model_metadata.has_explicit_metadata(model)
    if config.Config.system_debug_enabled():
        logger.warning(
            f"[/v1/responses] route error: model={model}, "
            f"reason={reason}, is_group={is_group}, "
            f"has_runtime_route={has_runtime_route}, has_info={has_info}, has_metadata={has_metadata}"
        )
    if not is_group and not has_runtime_route and not has_info and not has_metadata:
        raise _responses_access_error(f"The model '{model}' does not exist.", 404, "model_not_found")
    raise _responses_access_error(f"You do not have access to model '{model}'.", 403, "model_access_denied")


async def _validate_responses_request(body: dict, api_key: str | None = None):
    catalog_snapshot = model_catalog.current_snapshot()
    model = body.get("model")
    if not model:
        raise HTTPException(status_code=400, detail="缺少 model 参数")
    if "input" not in body:
        raise HTTPException(status_code=400, detail="缺少 input 参数")
    if api_key:
        api_key_name = await _api_key_name(api_key)
        if not await config.Config.api_key_allows_model(api_key, model):
            await _raise_responses_route_error(model, api_key, api_key_name, "api_key_not_allowed_for_model")
        if await _catalog_call(config.Config.is_model_group, model, snapshot=catalog_snapshot):
            if not await _catalog_call(model_metadata.has_explicit_metadata, model, snapshot=catalog_snapshot):
                raise HTTPException(status_code=400, detail=f"模型组 '{model}' 未配置元数据，不可使用")
            if not await ModelClientPool.get_model_group_available_members(model, snapshot=catalog_snapshot):
                raise HTTPException(status_code=429, detail=f"模型组 '{model}' 没有已配置元数据的可用成员")
        else:
            if not ModelClientPool.get_model_routes(model):
                await _raise_responses_route_error(model, api_key, api_key_name, "model_not_found")
            if not await _catalog_call(model_metadata.has_explicit_metadata, model, snapshot=catalog_snapshot):
                raise HTTPException(status_code=400, detail=f"模型 '{model}' 未配置元数据，不可使用")
    openai_messages, kwargs = responses_to_openai_messages(body)
    if body.get("reasoning") is not None:
        kwargs["_thinking_explicit"] = True
    if not openai_messages:
        raise HTTPException(status_code=400, detail="input 不能为空")
    await _validate_context_budget(model, body, "responses")
    return model, openai_messages, kwargs


async def dispatch_entry(
    *,
    endpoint: str,
    body: dict,
    headers: dict | None,
    api_key: str | None,
    api_key_name: str | None,
    provider_whitelist: set[str] | None = None,
    provider_blacklist: set[str] | None = None,
    account_whitelist: set[str] | None = None,
    is_test: bool = False,
    is_probe: bool = False,
    is_save_log: bool = True,
    request_protocol: str = "openai",
    chat_method: str = "chat",
    operation: str | None = None,
    client_type: str = "unknown",
    extra_kwargs: dict | None = None,
    editor_request_context: dict | None = None,
) -> dict:
    """统一入口：校验请求 → 组装 kwargs → 调对应 retry。

    正常请求由 HTTP 端点校验 api key、提取渠道白/黑名单后调用；测试额外传
    ``account_whitelist`` + ``is_test`` 直接调用。返回一个 dispatch 结果 dict：

    - 对话（``operation`` 为 None/"chat"）：``{"kind": "chat", "stream": bool,
      "generator": async-gen, "model": str,
      "request_id": str, "start_time": float}``。调用方负责迭代生成器 + 包
      Streaming/JSON + 统计。
    - 媒体（``operation`` in image/video/tts_generation）：``{"kind": operation,
      "result": dict|bytes, "model": str, "start_time": float}``。

    职责：校验后到调 retry 之前的共性准备段（协议 kwargs 组装、模型 & modality
    校验）。不含：api key 校验、白名单提取、限流、Response 包装 —— 这些留在 HTTP
    端点，测试直接调本函数天然绕开。
    """
    provider_whitelist = provider_whitelist or set()
    provider_blacklist = provider_blacklist or set()
    extra_kwargs = extra_kwargs or {}
    if editor_request_context:
        editor = editor_request_context.get("editor") or {}
        editor_session = editor_request_context.get("session") or {}
        extra_kwargs.update({
            "editor_id": editor.get("id"),
            "editor_session_id": editor_session.get("id"),
            "task_id": editor_request_context.get("task_id"),
            "api_key_version": editor_request_context.get("api_key_version"),
            "api_key_name_snapshot": editor_request_context.get("api_key_name_snapshot"),
            "api_key_id": editor_request_context.get("api_key_id"),
            "api_key_parent_id": editor_request_context.get("api_key_parent_id"),
        })
    elif api_key:
        # 通用/copy 路径也要把 api key 血缘快照写进日志，否则 request_logs 的
        # api_key_id/api_key_parent_id 为空，子 Key 用量无法按 parent_id 汇总回父，
        # 请求日志也关联不到父子关系。editor 路径已在上面单独填过。
        apikey_row = await config.Config.get_api_key_config(api_key, include_disabled=True)
        if apikey_row:
            extra_kwargs.setdefault("api_key_id", apikey_row.get("id"))
            extra_kwargs.setdefault("api_key_parent_id", apikey_row.get("parent_id"))
            extra_kwargs.setdefault("api_key_version", apikey_row.get("version"))
            extra_kwargs.setdefault("api_key_name_snapshot", apikey_row.get("name"))
    request_headers = _sanitize_headers(headers) if headers is not None else {}
    start_time = time.time()

    # ── 媒体分支：图片 / 视频 / TTS ─────────────────────────────
    if operation in ("image", "image_generation", "video", "video_generation"):
        kind = "image" if operation in ("image", "image_generation") else "video"
        model = body.get("model")
        if not model:
            raise HTTPException(status_code=400, detail="缺少 model 参数")
        prompt = body.get("prompt")
        if not prompt:
            raise HTTPException(status_code=400, detail="缺少 prompt 参数")
        required_modality = "image" if kind == "image" else "video"
        output_modalities = await _model_output_modalities(model)
        if required_modality not in output_modalities:
            raise HTTPException(status_code=400, detail=f"模型 {model} 不支持{required_modality}生成")
        result = await _media_generation_with_retry(
            kind=kind,
            model=model,
            prompt=prompt,
            body=body,
            api_key=api_key,
            api_key_name=api_key_name,
            request_headers=request_headers,
            client_request_path=endpoint,
            client_type=client_type,
            provider_whitelist=provider_whitelist,
            provider_blacklist=provider_blacklist,
            account_whitelist=account_whitelist,
            is_test=is_test,
            is_probe=is_probe,
            is_save_log=is_save_log,
        )
        return {"kind": kind, "result": result, "model": model, "start_time": start_time}

    if operation in ("tts", "tts_generation", "speech"):
        model = body.get("model")
        if not model:
            raise HTTPException(status_code=400, detail="缺少 model 参数")
        text = body.get("input") or body.get("text")
        if not text:
            raise HTTPException(status_code=400, detail="缺少 input 参数")
        result = await _tts_generation_with_retry(
            model=model,
            text=text,
            body=body,
            api_key=api_key,
            api_key_name=api_key_name,
            request_headers=request_headers,
            client_request_path=endpoint,
            client_type=client_type,
            provider_whitelist=provider_whitelist,
            provider_blacklist=provider_blacklist,
            account_whitelist=account_whitelist,
            is_test=is_test,
            is_probe=is_probe,
            is_save_log=is_save_log,
        )
        return {"kind": "tts_generation", "result": result, "model": model, "start_time": start_time}

    # ── 对话分支：chat / responses / anthropic ─────────────────
    proto = (request_protocol or "openai").lower()
    if proto == "responses":
        model, messages, full_kwargs = await _validate_responses_request(body, api_key)
        if body.get("reasoning") is not None:
            full_kwargs["_thinking_explicit"] = True
        full_kwargs["_raw_responses_body"] = body
        full_kwargs["_client_protocol"] = "responses"
    elif proto == "anthropic":
        model, _ = await _validate_chat_request(body, api_key)
        await _validate_context_budget(model, body, "anthropic")
        messages, conv_kwargs = anthropic_to_openai_messages(body)
        full_kwargs = _build_protocol_kwargs(body, "openai", "anthropic")
        full_kwargs.update(conv_kwargs)
        _post_process_anthropic_kwargs(full_kwargs, body)
        full_kwargs["_raw_anthropic_body"] = body
        full_kwargs["_client_protocol"] = "anthropic"
    else:
        model, messages = await _validate_chat_request(body, api_key)
        full_kwargs = _build_protocol_kwargs(body, "openai", "openai")

    stream = _request_stream_enabled(body)
    request_id = _new_request_id()
    if editor_request_context and editor_request_context.get("first_request"):
        session = editor_request_context.get("session") or {}
        if editor_request_context.get("task_id"):
            # Canonical Task: bind the provider thread to the Task itself.
            task = editor_request_context.get("task") or {}
            bound = await PostgresClient.bind_task_provider_thread(
                str(task.get("id") or editor_request_context["task_id"]),
                editor_request_context["provider_thread_id"],
                request_id,
            )
            if not bound:
                raise HTTPException(status_code=403, detail="Codex 首请求已被其他请求占用")
            editor_request_context = {**editor_request_context, "task": bound}
        else:
            bound = await PostgresClient.bind_provider_thread(
                session["id"], editor_request_context["provider_thread_id"], request_id
            )
            if not bound:
                raise HTTPException(status_code=403, detail="Codex 首请求已被其他请求占用")
            editor_request_context = {**editor_request_context, "session": bound}
    session_context = _extract_client_session_context(headers if headers is not None else {})
    full_kwargs.update(session_context)
    # 保留 dispatch 既有 kwargs 契约；orchestrator 会从 RequestContext 再写同值，避免调用方漂移。
    full_kwargs["request_id"] = request_id
    full_kwargs["request_headers"] = request_headers
    full_kwargs["client_request_path"] = endpoint
    full_kwargs["client_type"] = client_type
    full_kwargs["client_request_body"] = body
    full_kwargs["request_protocol"] = proto
    full_kwargs["provider_whitelist"] = provider_whitelist
    full_kwargs["provider_blacklist"] = provider_blacklist
    full_kwargs["account_whitelist"] = account_whitelist
    full_kwargs["is_test"] = is_test
    full_kwargs["is_probe"] = is_probe
    full_kwargs["is_save_log"] = is_save_log
    # 测试注入 client_preset / _endpoint_config 等
    for k, v in extra_kwargs.items():
        full_kwargs[k] = v

    context = RequestContext(
        request_id=request_id,
        original_body=attempt_builder.build_attempt_body(body),
        original_model=model,
        messages=messages,
        request_protocol=proto,
        chat_method=chat_method,
        stream=stream,
        api_key=api_key,
        api_key_name=api_key_name,
        provider_whitelist=set(provider_whitelist),
        provider_blacklist=set(provider_blacklist),
        account_whitelist=account_whitelist,
        client_type=client_type,
        session_id=session_context.get("session_id") or session_context.get("client_session_id"),
        request_headers=request_headers,
        client_request_path=endpoint,
        is_test=is_test,
        is_probe=is_probe,
        base_kwargs=full_kwargs,
    )
    generator = _run_request_orchestrator(context)
    return {
        "kind": "chat",
        "stream": stream,
        "generator": generator,
        "model": model,
        "messages": messages,
        "kwargs": full_kwargs,
        "context": context,
        "request_id": request_id,
        "start_time": start_time,
    }


@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def chat_completions(request: Request, authorization: Optional[str] = Header(None)):
    """聊天补全"""

    api_key = await _extract_api_key(request, authorization, support_x_api_key=True)
    request_headers = _sanitize_headers(request.headers)

    body = await request.json()
    body, client_type = await _prepare_client_request_body(body, api_key, request_headers, request.url.path, "openai")

    # 安全扫描：凭据泄露、提示注入、输入校验
    # 被安全拦截时同时写入 request_log 与 security_events，使前端请求记录 + 通知中心能查询命中详情
    if config.Config.security_enabled():
        from security import scan_request, ScanRequest
        scan_req = ScanRequest.from_body(body, api_key=api_key)
        scan_result = await scan_request(scan_req)
        if scan_result.has_tags:
            security_request_id = _new_request_id()
            api_key_name_preview = await _api_key_name(api_key)
            await _record_security_block(
                scan_result=scan_result, request_id=security_request_id,
                api_key=api_key, api_key_name=api_key_name_preview,
                endpoint=request.url.path, body=body, client_type=client_type,
                request_headers=request_headers, source_ip=_client_ip(request),
            )
        if scan_result.blocked:
            raise HTTPException(status_code=400, detail=_openai_error(scan_result.block_reason, "security_error"))

    editor_request_context = await _resolve_request_context(api_key, request_headers, body)

    # 凭据遮蔽：替换请求中的真实密钥为假值
    mask_restore_map = {}
    if config.Config.security_masking_enabled():
        from security import mask_credentials
        mask_result = mask_credentials(body)
        body = mask_result.masked_body
        mask_restore_map = mask_result.restore_map

    # 统一入口：校验 + kwargs 组装 + 建生成器（惰性，不触发上游）。白/黑名单由端点层显式提取。
    provider_whitelist, provider_blacklist = await config.Config.get_api_key_provider_filter(api_key)
    api_key_name = await _api_key_name(api_key)
    dispatch = await dispatch_entry(
        endpoint=request.url.path,
        body=body,
        headers=request.headers,
        api_key=api_key,
        api_key_name=api_key_name,
        provider_whitelist=provider_whitelist,
        provider_blacklist=provider_blacklist,
        request_protocol="openai",
        chat_method="chat",
        client_type=client_type,
        editor_request_context=editor_request_context,
    )
    await _acquire_api_key_limit(request, api_key)

    model = dispatch["model"]
    stream = dispatch["stream"]
    start_time = dispatch["start_time"]
    stream_gen = dispatch["generator"]

    if stream:
        response_headers = _stream_response_headers()
        try:
            first_chunk = await _prime_stream_before_response(stream_gen)
        except Exception:
            await _release_api_key_limit(request)
            raise
        primed_stream = _prepend_stream_chunk(first_chunk, stream_gen)

        async def logging_stream():
            last_route_info = {}
            status = "ok"
            error = ""
            response_summary = _new_stream_summary()
            try:
                async for chunk in primed_stream:
                    if isinstance(chunk, dict) and "_last_route_info" in chunk:
                        last_route_info = chunk.get("_last_route_info") or last_route_info
                        continue
                    _append_stream_summary(response_summary, chunk)
                    yield chunk
            except StreamStartedChannelError as e:
                status = "error"
                error = str(e)
                last_route_info = e.last_route_info or last_route_info
                yield _openai_stream_error_chunk(error, e.code, e.error_type, e.status_code)
                yield "data: [DONE]\n\n"
            except IncompleteStreamError as e:
                status = "error"
                error = str(e)
                last_route_info = getattr(e, "_last_route_info", last_route_info) or last_route_info
                yield _openai_stream_error_chunk(error)
                yield "data: [DONE]\n\n"
            except asyncio.CancelledError:
                status = "cancelled"
                error = "client disconnected"
                last_route_info = getattr(stream_gen, "_last_route_info", last_route_info) or last_route_info
                raise
            except Exception as e:
                status = "error"
                error = str(e)
                stream_error = _stream_error_from_exception(e, last_route_info)
                yield _openai_stream_error_chunk(str(stream_error), stream_error.code, stream_error.error_type, stream_error.status_code)
                yield "data: [DONE]\n\n"
            finally:
                if status == "ok":
                    usage_body = _stream_summary_to_openai_response(model, response_summary) or {}
                    usage_for_stats = _usage(usage_body, body)
                    await _finalize_request_success_stats(
                        api_key=api_key,
                        requested_model=model,
                        endpoint="/v1/chat/completions",
                        usage=usage_for_stats,
                        last_route_info=last_route_info,
                    )
                else:
                    if api_key:
                        _add_recent_log(api_key, model, "/v1/chat/completions", status, duration_ms=_duration_ms(start_time), error=error)
                await _release_api_key_limit(request)

        return StreamingResponse(
            logging_stream(),
            media_type="text/event-stream",
            headers=response_headers,
        )

    try:
        result = await _collect_non_stream_result(stream_gen)
    except NoAvailableAccountError:
        await _release_api_key_limit(request)
        raise
    except Exception:
        await _release_api_key_limit(request)
        raise

    last_route_info = result.pop("_last_route_info", {}) if isinstance(result, dict) else {}
    result.pop("_first_token_ms", None) if isinstance(result, dict) else None

    usage_for_stats = _usage(result, body)
    await _finalize_request_success_stats(
        api_key=api_key,
        requested_model=model,
        endpoint="/v1/chat/completions",
        usage=usage_for_stats,
        last_route_info=last_route_info,
    )

    await _release_api_key_limit(request)
    return JSONResponse(result)


# 错误分类的单一事实源在 retry_policy（纯函数，规则参数注入）。这里保留同名薄封装
# 维持既有调用方/测试签名不变；配置读取（get_non_retryable_parameter_errors）留在此处。
def _parse_error_detail(detail) -> tuple[str, str, str, str]:
    return retry_policy.parse_error_detail(detail)


def _as_lower_str_list(value) -> list[str]:
    return retry_policy.as_lower_str_list(value)


def _as_int_set(value) -> set[int]:
    return retry_policy.as_int_set(value)


def _is_protocol_parameter_error(err_type: str, code: str, param: str, message: str) -> bool:
    return retry_policy.is_protocol_parameter_error(err_type, code, param, message)


def _canonical_context_overflow_exception(error: HTTPException) -> HTTPException | None:
    """Return the standard client error for a recognized context overflow.

    Retry classification and response serialization are separate concerns.  A
    channel 4xx can be stopped by ``RETURN_CLIENT_ERROR`` before it reaches
    FastAPI's exception handler, so normalize it at that boundary as well.
    """
    status_code = getattr(error, "status_code", None)
    detail = getattr(error, "detail", "")
    canonical = retry_policy.canonicalize_upstream_error(status_code, detail)
    if canonical is None or not config.Config.context_overflow_not_retryable_enabled():
        return None
    return HTTPException(
        status_code=canonical.status_code,
        detail=_openai_error(
            canonical.message,
            canonical.type,
            canonical.code,
        ),
        headers=getattr(error, "headers", None),
    )


def _is_non_retryable_upstream_error(error: HTTPException) -> bool:
    status_code = getattr(error, "status_code", None)
    detail = getattr(error, "detail", "")
    # 先做零 IO 的硬编码超限识别；只有确实命中超限时才读取开关，普通错误不增加
    # config store IO，也继续完全走原来的可配置不可重试逻辑。
    canonical = retry_policy.canonicalize_upstream_error(status_code, detail)
    if canonical is not None:
        # 开关同时决定是否判超限；关闭后整套超限策略不介入。
        if config.Config.context_overflow_not_retryable_enabled():
            return True
    # 其余不可重试错误：先做纯参数类短路，再按 config 规则判定。普通 401/403/404/5xx/
    # 纯文本 unsupported 不读取配置，与旧行为一致，也避免无关错误把 config store IO
    # 带进重试热路径。
    err_type, code, param, message = retry_policy.parse_error_detail(detail)
    if not retry_policy.is_protocol_parameter_error(err_type, code, param, message):
        return False
    rules = config.Config.get_non_retryable_parameter_errors()
    return retry_policy.is_non_retryable_upstream_error(
        status_code, detail, rules
    )


def _attach_last_route(error: Exception, last_route_info: dict | None = None):
    if last_route_info:
        setattr(error, "_last_route_info", last_route_info)
    return error


RETRYABLE_CLIENT_MESSAGE = "服务端异常，请重试"


# 429 业务码 → 英文 message 映射（不做 503/402，统一 429）。客户端可按 code + Retry-After
# 决定是否重试；中文 skip 原因只进日志/admin（NoAvailableAccountError.detail）。
TERMINAL_ERROR_MESSAGES = {
    "rate_limit_exceeded":              "Rate limit exceeded; please retry later.",
    "concurrent_limit_exceeded":        "Concurrency limit exceeded; please retry later.",
    "quota_exhausted":                  "Upstream quota exhausted.",
    "no_available_account":              "No available account for the requested model.",
    "service_busy":                     "Service temporarily busy; please retry.",
    "retry_exhausted":                  "Request failed after retries.",
    "upstream_exception":               "Upstream service error; please retry.",
    "incomplete_response":              "Upstream returned an incomplete response.",
    "api_key_expired":                  "API key has expired.",
    "api_key_disabled":                 "API key has been disabled.",
    "api_key_invalid":                  "Invalid API key.",
    "api_key_rate_limit_exceeded":      "API key rate limit exceeded; please retry later.",
    "api_key_concurrent_limit_exceeded": "API key concurrency limit exceeded; please retry.",
    "api_key_ip_limit_exceeded":         "API key IP limit exceeded.",
    "api_key_quota_exhausted":          "API key quota exhausted.",
}


def _retryable_client_error_detail(code: str, retries: int, upstream_status: int | None = None, retry_after: int | None = None) -> dict:
    """终端 429 detail：业务 code + 英文 message + 重试次数 + 可选 Retry-After 秒数。

    异常处理器会把 detail 字段合进 error.*，并把 retry_after（>0）挂到响应头 Retry-After。
    """
    return {
        "upstream_status": upstream_status,
        "message": TERMINAL_ERROR_MESSAGES.get(code, RETRYABLE_CLIENT_MESSAGE),
        "retries": retries,
        "kind": code,  # kind 与 code 统一；保留 kind 字段兼容已有客户端/测试
        "code": code,
        "retry_after": retry_after,
    }


def _terminal_429_from_last_error(last_error: Exception | None, max_retries: int) -> tuple[str, int | None]:
    """从最后一次失败推导 (code, upstream_status)。retry_after 由调用方按 code 决定。"""
    if last_error is None:
        return "no_available_account", None
    # 改动 4 新增的 API-Key 级失败：detail dict 带 code，优先透传
    if isinstance(last_error, HTTPException):
        detail = last_error.detail
        if isinstance(detail, dict) and detail.get("code"):
            return str(detail["code"]), last_error.status_code
    if isinstance(last_error, NoAvailableAccountError):
        return getattr(last_error, "code", None) or "no_available_account", None
    if isinstance(last_error, (EmptyNonStreamResponseError, IncompleteStreamError)):
        return getattr(last_error, "code", None) or "incomplete_response", None
    if isinstance(last_error, HTTPException):
        sc = last_error.status_code
        if sc == 429:
            return "rate_limit_exceeded", sc
        return "upstream_exception", sc
    return "retry_exhausted", None


# 哪些业务码建议客户端短退避重试（挂 Retry-After: 1）。配额/失效类不挂——重试无意义。
_RETRYABLE_CODES = {
    "rate_limit_exceeded", "concurrent_limit_exceeded", "service_busy",
    "retry_exhausted", "incomplete_response", "upstream_exception",
    "api_key_rate_limit_exceeded", "api_key_concurrent_limit_exceeded",
}


def _retry_after_for_code(code: str) -> int | None:
    return 1 if code in _RETRYABLE_CODES else None




def _retryable_client_status(is_rate_limit: bool, upstream_status: int | None = None) -> int:
    return 429


async def _run_group_fallback_pipeline(
    model, messages, stream,
    *, api_key: str | None, api_key_name: str | None, chat_method: str,
    base_kwargs: dict,
):
    """组回退单循环：当前组账号重试耗尽后，按 catalog snapshot 解析一个备选组继续，
    visited 防环；备选也耗尽则抛 NoAvailableAccountError / 429。

    这是请求编排的**单一事实源**——_chat_with_retry（兼容签名）与 _run_request_orchestrator
    （RequestContext 入口）都委托本 generator，避免组回退逻辑两处分叉。
    """
    initial_snapshot = model_catalog.current_snapshot()
    initial_group = await _catalog_call(config.Config.resolve_model_group, model, snapshot=initial_snapshot)
    stable_response_model = None
    if initial_group:
        stable_response_model = await _catalog_call(
            config.Config.get_model_group_response_model, model, snapshot=initial_snapshot
        ) or model

    current_model = model
    visited: set[str] = set()
    last_exc: NoAvailableAccountError | None = None
    while current_model:
        selection_snapshot = model_catalog.current_snapshot()
        group = await _catalog_call(
            config.Config.resolve_model_group, current_model, snapshot=selection_snapshot
        )
        canonical = str((group or {}).get("name") or current_model)
        if canonical in visited:
            break
        visited.add(canonical)

        attempt_kwargs = dict(base_kwargs)
        if stable_response_model is not None:
            attempt_kwargs["_stable_response_model"] = stable_response_model
        attempt_kwargs["_response_model_source"] = model
        attempt_kwargs["_group_request_identity"] = bool(initial_group)
        generator = _chat_with_retry_for_model(
            current_model, messages, stream,
            api_key=api_key, api_key_name=api_key_name, chat_method=chat_method,
            **attempt_kwargs,
        )
        try:
            async for chunk in generator:
                yield chunk
            return
        except NoAvailableAccountError as exc:
            last_exc = exc
            logger.warning(
                f"[模型组不可用] group='{canonical}', generation={selection_snapshot.generation}, "
                f"status={exc.status_code}, detail={exc.detail}"
            )
        finally:
            try:
                await generator.aclose()
            except Exception:
                pass

        # Resolve exactly one next hop after this group is exhausted. A concurrent
        # catalog update may therefore change the next hop, but never this selection.
        fresh_snapshot = model_catalog.current_snapshot()
        fresh_group = await _catalog_call(
            config.Config.resolve_model_group, canonical, snapshot=fresh_snapshot
        )
        backup = str((fresh_group or {}).get("backup_group") or "").strip()
        if not backup:
            break
        backup_group = await _catalog_call(
            config.Config.resolve_model_group, backup, snapshot=fresh_snapshot
        )
        backup_canonical = str((backup_group or {}).get("name") or backup)
        if not backup_group or backup_canonical in visited:
            break
        logger.info(f"[备份组回退] 主组 '{canonical}' 全部不可用，切换到备份组 '{backup}'")
        current_model = backup

    if last_exc is not None:
        raise last_exc
    # 备份链未触发就走到这里（visited 命中等边界）：用统一业务码收口。
    raise HTTPException(
        status_code=429,
        detail=_retryable_client_error_detail(
            "no_available_account", 0, None, _retry_after_for_code("no_available_account"),
        ),
    )


async def _run_request_orchestrator(context: RequestContext):
    """统一请求编排入口（run_request_pipeline）。

    RequestContext 持请求级固定状态；模型组/备选组解析每次取最新 catalog snapshot。
    组回退单循环在 _run_group_fallback_pipeline，账号重试在 _chat_with_retry_for_model，
    AttemptContext 在每次 attempt 内承载候选身份/body/预占/lifecycle——三者合成真正单 pipeline，
    不再有"外层组回退 + 内层账号重试"两层裸堆叠。
    base_kwargs 已由 dispatch_entry 组装，这里做一次深拷贝隔离后整组透传。
    """
    kwargs = attempt_builder.build_attempt_body(context.base_kwargs)
    async for chunk in _run_group_fallback_pipeline(
        context.original_model,
        context.messages,
        context.stream,
        api_key=context.api_key,
        api_key_name=context.api_key_name,
        chat_method=context.chat_method,
        base_kwargs=kwargs,
    ):
        if isinstance(chunk, dict) and "_last_route_info" in chunk:
            context.last_route_info = chunk.get("_last_route_info") or context.last_route_info
        yield chunk


async def _chat_with_retry_for_model(model, messages, stream, api_key: str | None = None, api_key_name: str | None = None, chat_method: str = "chat", **kwargs):
    """带 429 自动切换账号的 chat 请求

    ``chat_method`` 默认 ``"chat"``（OpenAI SSE 输出）；当端点需要 anthropic 原生输出时
    传入 ``"chat_anthropic"``，由 BaseProvider.chat_anthropic 决定同协议直通或跨协议转换。
    `model` 既可指向单一模型也可指向模型组（向后兼容，由 _chat_with_retry 编排）。
    """
    route = None
    provider_whitelist = kwargs.pop("provider_whitelist", None) or set()
    provider_blacklist = kwargs.pop("provider_blacklist", None) or set()
    account_whitelist = kwargs.pop("account_whitelist", None)
    # 诊断日志：进入选路重试编排前的模型值与请求归属（正对"请求日志里写死的
    # model"这一层——它在此函数的下游 route_info 组装前就已经定型）。任何
    # "跑错模型"先拿这条和 [gateway] chat-validate / runtime [claude-query] 对值。
    logger.info(
        "[gateway] chat-retry-enter model='{}' chat_method='{}' stream={} api_key_prefix='{}' "
        "task_id='{}' editor_id='{}' session_id='{}'",
        model, chat_method, stream,
        (api_key[:12] + "...") if api_key else "<none>",
        str(kwargs.get("task_id") or "") or "<none>",
        str(kwargs.get("editor_id") or "") or "<none>",
        str(kwargs.get("session_id") or "") or "<none>",
    )
    account_whitelist = kwargs.pop("account_whitelist", None)
    is_test = bool(kwargs.pop("is_test", False))
    is_probe = bool(kwargs.pop("is_probe", False))
    # 定时检测保留日志开关：is_save_log 由 admin.py 按「is_probe 时取配置开关、
    # 手动测试/正常请求恒 True」算好透传；此处只读不判 is_probe，False 时两条闸口都不写。
    is_save_log = bool(kwargs.pop("is_save_log", True))
    stable_response_model = kwargs.pop("_stable_response_model", None)
    response_model_source = kwargs.pop("_response_model_source", None)
    group_request_identity = kwargs.pop("_group_request_identity", None)
    initial_catalog_snapshot = model_catalog.current_snapshot()
    initial_is_group = await _catalog_call(
        config.Config.is_model_group, model, snapshot=initial_catalog_snapshot
    )
    if group_request_identity is None:
        group_request_identity = initial_is_group
    if stable_response_model is None and initial_is_group:
        stable_response_model = await _catalog_call(
            config.Config.get_model_group_response_model, model, snapshot=initial_catalog_snapshot
        ) or model
    model_providers = ModelClientPool.get_model_providers(model)
    if not model_providers and initial_is_group:
        for group_model in await _catalog_call(
            config.Config.get_model_group_models, model, snapshot=initial_catalog_snapshot
        ):
            model_providers = ModelClientPool.get_model_providers(group_model)
            if model_providers:
                break
    # 两层次数相互独立：
    #   全局 retry.max_retries → 当前候选的内层次数耗尽后，最多再换几次候选。
    #   渠道 provider.retry_count → 每个候选首次失败后，同账号原地重发几次。
    # 只有内层全部失败才进入下一次外层选路，绝不能用全局预算提前截断渠道内层次数。
    max_retries = max(0, config.Config.get_global_retry_count())
    total_attempts = 1 if (is_test or is_probe) else max_retries + 1  # 测试/探测模式不重试
    tried_accounts = []
    last_error = None
    request_id = kwargs.get("request_id")
    last_route_info = {}
    request_headers = kwargs.pop("request_headers", None)
    client_request_path = kwargs.pop("client_request_path", None)
    client_type = kwargs.pop("client_type", "unknown")
    session_id = kwargs.pop("session_id", None) or kwargs.pop("client_session_id", None)
    editor_id = kwargs.pop("editor_id", None)
    editor_session_id = kwargs.pop("editor_session_id", None)
    task_id = kwargs.pop("task_id", None) or kwargs.pop("_task_id", None)
    api_key_version = kwargs.pop("api_key_version", None)
    api_key_name_snapshot = kwargs.pop("api_key_name_snapshot", None)
    api_key_id = kwargs.pop("api_key_id", None)
    api_key_parent_id = kwargs.pop("api_key_parent_id", None)
    client_request_body = kwargs.pop("client_request_body", None)
    request_protocol = kwargs.pop("request_protocol", None)

    client = None
    account_client = None
    candidate_reservation = None
    downstream_started = False
    # 两层重试：外层换候选（全局 retry.max_retries），内层同账号原地重发（渠道 retry_count）。
    # 本循环的每一轮 = 一次真实上游请求。reuse_candidate 为真时跳过选路与预占重建，
    # 直接复用上一轮的 client/account/预占，即「内层同账号原地重试」。
    reuse_candidate = False
    outer_index = 0        # 已用掉的外层候选序号（0 基），仅用于日志/诊断
    inner_index = 0        # 当前候选上已发出的请求序号（0 基）
    inner_limit = 1        # 当前候选的内层总次数，acquire 后按渠道配置确定
    actual_attempt_no = 0  # 外层候选日志序号；渠道内重试不再创建新的 request_logs 行
    channel_attempts: list[dict] = []  # 当前候选内部的实际上游请求明细
    candidate_attempt_key = ""          # 当前候选唯一主日志键；内层重试复用
    candidate_started_at = 0.0          # 主日志起点（首次渠道请求开始）
    # 渠道账号 token 预占估算：输入 prompt + 有界预期输出。只算一次，内层重试/换候选复用，
    # 避免每个候选按不同默认值重复估算，也避免直接按超大的 max_tokens 虚占额度。
    reservation_body = _build_reservation_body(messages, kwargs)
    # 复用公共计算层：input_tokens 用于选路过滤（候选按 max_context_tokens 跳过），
    # total_tokens 用于 token 预占估算口径。只算一次，内层重试/换候选复用。
    _reservation_token_est = estimate_request_tokens(model, reservation_body)
    estimated_prompt_tokens = int(_reservation_token_est["input_tokens"] or 0)
    try:
        explicit_output_tokens = int(
            kwargs.get("max_completion_tokens", kwargs.get("max_tokens")) or 0
        )
    except (TypeError, ValueError):
        explicit_output_tokens = 0
    output_cap = 1024
    expected_output_tokens = max(128, min(output_cap, max(1, estimated_prompt_tokens // 4)))
    if explicit_output_tokens > 0:
        expected_output_tokens = min(expected_output_tokens, explicit_output_tokens)
    token_reservation_estimate = estimated_prompt_tokens + expected_output_tokens
    # 候选级快照：内层同账号重试复用，换候选时重算（见下方 reuse_candidate 分支）。
    candidate_identity = None        # (ModelIdentity, is_group_request)
    candidate_model_defaults = None  # 该候选 public_model_id 的模型默认参数
    # 循环边界是「外层候选次数」：内层同账号重发不消耗 outer_index；只有当前候选
    # 内层次数全部失败、决定重新选路时，才会把 outer_index 加一。
    while outer_index < total_attempts:
        catalog_snapshot = model_catalog.current_snapshot()
        # 内层重试沿用同一候选，因此保留上一轮的 provider/account，不清空。
        if not reuse_candidate:
            provider_name = None
            username = None
        route_info = {"estimated_prompt_tokens": estimated_prompt_tokens}
        attempt_start = time.time()
        attempt_logged = False
        attempt_stream_summary = None
        accumulated_usage = {}
        attempt_kwargs: dict = {}
        # 供日志/诊断使用的兼容量：外层候选序号（旧 retry 字段语义不变）。
        retry_count = outer_index
        # 出站代理决策由 ProxyManager 在真正发请求处写入；每个 attempt 起点清空，
        # 避免上一 attempt 的残留被当成本次的实际代理。
        reset_outbound_proxy()
        key_error = await _recheck_api_key_state(api_key, is_test=(is_test or is_probe))
        if key_error is not None:
            raise _attach_last_route(key_error, last_route_info)
        try:
            if reuse_candidate:
                # 内层同账号原地重试：不选路、不重建预占，沿用同一 client/account/lease。
                # 选路耗时留空——本次没有选路，不该把上一轮的选路时间重复计入。
                routing_timing = {}
            else:
                (client, provider_name, account_client), routing_timing = await _acquire_client_with_routing_timing(
                    model=model,
                    tried_accounts=tried_accounts,
                    messages=messages,
                    route=route,
                    api_key=api_key,
                    session_id=session_id,
                    request_protocol=request_protocol,
                    provider_whitelist=provider_whitelist,
                    provider_blacklist=provider_blacklist,
                    account_whitelist=account_whitelist,
                    is_test=is_test,
                    is_probe=is_probe,
                    catalog_snapshot=catalog_snapshot,
                    token_estimate=token_reservation_estimate,
                    input_token_estimate=estimated_prompt_tokens,
                )
                username = getattr(client, 'username', None)
                candidate_reservation = CandidateReservation(
                    provider_name=provider_name,
                    account_client=account_client,
                    provider=client,
                )
                # 新候选：按渠道 retry_count 决定这一候选上最多发几次请求，并丢弃上一个
                # 候选的身份/默认参数快照（不同候选的 routed_model 与元数据可能不同）。
                inner_limit = _inner_retry_limit(provider_name, is_test=is_test, is_probe=is_probe)
                inner_index = 0
                channel_attempts = []
                candidate_attempt_key = uuid.uuid4().hex
                candidate_started_at = attempt_start
                actual_attempt_no += 1
                candidate_identity = None
                candidate_model_defaults = None
            # 本轮是否为内层重试（复用候选）。reuse_candidate 立刻复位，因为它只表达
            # 「下一轮要不要复用」——finally 靠它判断是否保留预占，换候选/收尾退出时必须
            # 是 False 才会释放。本轮自身的复用状态改用 is_inner_attempt 表达。
            is_inner_attempt = reuse_candidate
            reuse_candidate = False
            route_info = dict(getattr(account_client, "last_route_info", {}) or {})
            route_info.update(routing_timing)
            # 上一行从 account_client.last_route_info 重建 route_info，会丢掉循环外算好的
            # estimated_prompt_tokens（last_route_info / routing_timing 都不含它），这里补回；
            # 否则 request_logs.estimated_prompt_tokens 恒为 0，前端「预测入」永不显示。
            route_info["estimated_prompt_tokens"] = estimated_prompt_tokens
            route_info.setdefault("provider", provider_name)
            route_info.setdefault("account", username)
            route_info.setdefault("requested_model", model)
            route_info.setdefault("routed_model", model)
            route_info.setdefault("catalog_generation", catalog_snapshot.generation)
            route_info["_is_save_log"] = is_save_log
            route_info["client_type"] = client_type
            route_info["session_id"] = session_id
            route_info["editor_id"] = editor_id
            route_info["editor_session_id"] = editor_session_id
            route_info["task_id"] = task_id
            route_info["api_key_version"] = api_key_version
            route_info["api_key_name_snapshot"] = api_key_name_snapshot
            route_info["api_key_id"] = api_key_id
            route_info["api_key_parent_id"] = api_key_parent_id
            route_info["retry"] = retry_count
            # 一个外层候选 = 一条 request_logs。渠道内同账号重试只追加到
            # channel_retry_attempts，不改变 attempt_key/attempt_no，也不新增主表行。
            route_info["attempt_no"] = actual_attempt_no
            route_info["attempt_key"] = candidate_attempt_key
            route_info["channel_retry_attempts"] = channel_attempts
            route_info["outer_retry_index"] = outer_index
            route_info["inner_retry_index"] = inner_index
            route_info["inner_retry_limit"] = inner_limit
            # AttemptContext 真正驱动每次 attempt：载入 (provider,account,model) 身份、
            # 预占句柄、模型身份、route_info 与 lifecycle。后续 try 块读写它而非散装局部变量。
            attempt = AttemptContext(
                attempt_no=actual_attempt_no,
                candidate_key=CandidateKey(
                    provider=provider_name or "",
                    account=username or "",
                    model=route_info.get("routed_model") or model,
                ),
                reservation=candidate_reservation,
                route_info=route_info,
            )
            ttft_recorded = False
            upstream_headers = {}
            attempt_response_body = None
            attempt_usage_body = None
            attempt_intercept_text: list[str] = []
            output_interception_enabled = bool(config.Config.get_compiled_output_interception_patterns())
            buffered_stream_chunks: list = []
            attempt_stream_summary = _new_stream_summary() if stream else None
            attempt_client_response_body = _new_stream_log_body() if stream else None

            def response_headers_callback(headers):
                upstream_headers.clear()
                upstream_headers.update(_sanitize_headers(headers))
                status = upstream_headers.get(":status") or (headers or {}).get(":status")
                if status:
                    route_info["upstream_status"] = str(status)
                route_info["response_headers"] = dict(upstream_headers)

            def router_request_headers_callback(headers):
                route_info["router_request_headers"] = _sanitize_headers(headers)

            def router_request_body_callback(payload):
                route_info["router_request_body"] = payload

            def router_request_path_callback(path):
                route_info["router_request_path"] = path

            def router_response_body_callback(payload):
                _append_limited_router_response(route_info, payload)

            # 选中具体 (渠道,账号,模型) 后才决定四种模型身份和模型默认参数。
            # 每次从请求级 kwargs 深拷贝重建，避免失败 attempt 的 raw body/thinking/max_tokens
            # 改写污染下一次重试。
            #
            # 内层同账号重试沿用首次选路时算好的身份快照（candidate_identity）：本次没有
            # 重新选路，routed_model / 模型组归属都不可能变，重算只会白跑一遍 catalog 调用，
            # 还会让重试期间的模型组改动把同一候选的响应模型改掉（同一候选内必须自洽）。
            if is_inner_attempt and candidate_identity is not None:
                model_identity, attempt_group_identity = candidate_identity
            else:
                resolved_upstream_id = ModelClientPool.resolve_upstream_id(
                    provider_name, route_info.get("routed_model") or model
                )
                # response_model 也按本次候选选择使用的 catalog snapshot 决定；下一次
                # 换候选会重新取 snapshot，因此重试期间的模型组配置修改可以生效。
                attempt_response_model = stable_response_model
                attempt_group_identity = bool(group_request_identity)
                if response_model_source:
                    current_group = await _catalog_call(
                        config.Config.resolve_model_group,
                        response_model_source,
                        snapshot=catalog_snapshot,
                    )
                    attempt_group_identity = bool(current_group)
                    if current_group:
                        attempt_response_model = await _catalog_call(
                            config.Config.get_model_group_response_model,
                            response_model_source,
                            snapshot=catalog_snapshot,
                        ) or response_model_source
                    else:
                        attempt_response_model = None
                model_identity = attempt_builder.resolve_model_identity(
                    requested_model=model,
                    route_info=route_info,
                    resolved_upstream_id=resolved_upstream_id,
                    stable_response_model=attempt_response_model,
                )
                candidate_identity = (model_identity, attempt_group_identity)
            attempt.public_model_id = model_identity.routed_model
            attempt.upstream_model_id = model_identity.upstream_model
            attempt.response_model = model_identity.response_model
            public_model_id = attempt.public_model_id
            upstream_model_id = attempt.upstream_model_id
            client_response_model = attempt.response_model
            route_info["public_model_id"] = public_model_id
            route_info["upstream_model_id"] = upstream_model_id
            route_info["response_model"] = client_response_model

            attempt_kwargs = attempt_builder.build_attempt_body(kwargs)
            attempt.request_body = attempt_kwargs
            attempt.upstream_started = True
            # 模型默认参数同理按候选缓存：内层重试用完全相同的出站参数重发，
            # 不重新读模型元数据（同一候选内的请求必须是同一份参数）。
            if is_inner_attempt and candidate_model_defaults is not None:
                _apply_request_defaults(attempt_kwargs, candidate_model_defaults)
            else:
                candidate_model_defaults = await _request_defaults_from_metadata(
                    public_model_id, snapshot=catalog_snapshot
                )
                _apply_request_defaults(attempt_kwargs, candidate_model_defaults)
            # extra_config 两段语义，顺序不能反：
            # 1) 默认值段（apply_extra_config_defaults）：max_tokens / reasoning_effort /
            #    thinking 只在客户端未传时用渠道配置兜底，客户端显式值永远优先。
            # 2) 强制覆盖段（apply_extra_config_overrides）：其余字段无条件改写出站请求。
            #    保留键（client_preset / enable_1m_context / output_modalities 及上面三个
            #    默认值键）由各自机制消费，不在此覆盖。
            extra_config = route_info.get("extra_config") or {}
            attempt_kwargs = attempt_builder.apply_extra_config_defaults(attempt_kwargs, extra_config)
            attempt_kwargs = attempt_builder.apply_extra_config_overrides(attempt_kwargs, extra_config)
            for _raw_key in ("_raw_responses_body", "_raw_anthropic_body"):
                _raw = attempt_kwargs.get(_raw_key)
                if isinstance(_raw, dict):
                    _raw = attempt_builder.apply_extra_config_defaults(dict(_raw), extra_config)
                    attempt_kwargs[_raw_key] = attempt_builder.apply_extra_config_overrides(_raw, extra_config)
            if route_info.get("enable_1m_context"):
                attempt_kwargs["enable_1m_context"] = True
            attempt_kwargs["response_headers_callback"] = response_headers_callback
            attempt_kwargs["router_request_headers_callback"] = router_request_headers_callback
            attempt_kwargs["router_request_body_callback"] = router_request_body_callback
            attempt_kwargs["router_request_path_callback"] = router_request_path_callback
            attempt_kwargs["router_response_body_callback"] = router_response_body_callback
            attempt_kwargs["stream_incomplete_error_enabled"] = config.Config.stream_incomplete_error_enabled()
            attempt_kwargs["client_type"] = client_type
            # 账号运行态注入：供 header 模板 {{account.username}} / {{account.metadata.*}} 取值。
            attempt_kwargs["account_client"] = account_client
            # 请求入口显式携带的协议行（账号测试选择）锁定本次 path；路由候选只在
            # 没有显式行时提供默认协议行。协议/preset 可改变 body/header/parser，不能覆盖 path。
            if route_info.get("endpoint_config") and not isinstance(attempt_kwargs.get("_endpoint_config"), dict):
                attempt_kwargs["_endpoint_config"] = route_info.get("endpoint_config")
            logger.info(
                f"[重试调度._chat_with_retry] >>调用渠道 model={model}, provider={provider_name}, "
                f"account={username}, 候选={outer_index + 1}/{total_attempts}, "
                f"渠道内={inner_index + 1}/{inner_limit}, 实际第{actual_attempt_no}次, chat_method={chat_method}"
            )
            chat_callable = getattr(client, chat_method)
            is_group_request = attempt_group_identity
            # 一个候选只创建一条主日志；渠道内层重试复用同一条，不再 enqueue_started。
            if not is_inner_attempt:
                pending_log = _build_channel_attempt_log(
                    request_id=request_id,
                    route_info=route_info,
                    model=model,
                    messages=messages,
                    stream=stream,
                    api_key=api_key,
                    api_key_name=api_key_name,
                    request_headers=request_headers,
                    start_time=attempt_start,
                    duration_ms=0,
                    success=False,
                    status="requesting",
                    response_body=None,
                    error="",
                    client_request_path=client_request_path,
                    client_request_body=client_request_body,
                )
                _enqueue_started_log(
                    request_id=request_id,
                    route_info=route_info,
                    pending_log=pending_log,
                )
            attempt_kwargs["public_model_id"] = public_model_id
            accumulated_usage = {}
            stream_had_content = False
            nonstream_had_content = False
            downstream_started = False
            async for chunk in chat_callable(upstream_model_id, messages, stream=stream, **attempt_kwargs):
                raw_chunk = chunk
                if stream:
                    _accumulate_stream_usage(accumulated_usage, raw_chunk)
                    if not stream_had_content and _chunk_has_stream_content(raw_chunk):
                        stream_had_content = True
                else:
                    if not nonstream_had_content and _response_has_content(raw_chunk):
                        nonstream_had_content = True
                    _validate_upstream_usage_payload(raw_chunk, stream=False, had_content=nonstream_had_content)
                upstream_model = _extract_response_model(raw_chunk)
                if upstream_model:
                    route_info["upstream_returned_model"] = upstream_model
                chunk = _stream_chunk_with_public_model(raw_chunk, client_response_model, rewrite_nested=is_group_request)
                if isinstance(chunk, dict):
                    if not any(key in chunk for key in ("_passthrough_done", "_last_route_info")):
                        attempt_response_body = chunk
                        _collect_response_content_text(chunk, attempt_intercept_text)
                        if stream and attempt_stream_summary is not None:
                            # Some providers yield response dicts directly instead of SSE strings.
                            # Keep the summary content for usage estimation; interception uses the
                            # protocol-agnostic attempt_intercept_text accumulator below.
                            attempt_stream_summary["content"] += _extract_response_content_text(chunk)
                    if chunk.get("usage"):
                        attempt_usage_body = chunk
                else:
                    if attempt_stream_summary is not None:
                        _append_stream_summary(attempt_stream_summary, chunk)
                    if stream:
                        _collect_stream_response_content_text(chunk, attempt_intercept_text)
                if stream and not ttft_recorded and _has_first_token_content(chunk):
                    ttft_ms = _duration_ms(attempt_start)
                    if not is_test:  # 测试不改健康度/速度统计
                        ModelClientPool.record_channel_ttft(provider_name, username, public_model_id, ttft_ms)
                    route_info["ttft_ms"] = ttft_ms
                    ttft_recorded = True
                if attempt_client_response_body is not None and isinstance(chunk, str):
                    _append_stream_log_body(attempt_client_response_body, chunk)
                if stream and output_interception_enabled and not (isinstance(chunk, dict) and "_last_route_info" in chunk):
                    # 正则可能跨多个 delta，必须暂存当前候选的完整流；命中时一字节都不向客户端
                    # 下发，才能安全切换候选。整体开关关闭时仍沿用原来的即时流式透传。
                    buffered_stream_chunks.append(chunk)
                else:
                    if stream and not (isinstance(chunk, dict) and "_last_route_info" in chunk):
                        downstream_started = True
                        attempt.downstream_started = True
                        if attempt.first_byte_at is None:
                            attempt.first_byte_at = time.time()
                    yield chunk
            if stream and accumulated_usage:
                _validate_stream_completion(accumulated_usage, stream_had_content)
            if upstream_headers:
                route_info["response_headers"] = dict(upstream_headers)
            if attempt_stream_summary is not None and attempt_usage_body is None:
                attempt_usage_body = _stream_summary_to_openai_response(model, attempt_stream_summary)
            # 异常输出拦截：所有协议统一使用 attempt_intercept_text；流式 SSE 的每个
            # JSON payload 也经过同一个可见文本提取器，避免协议转换后绕过检测。
            intercept_content = "".join(attempt_intercept_text)
            hit_rule = _intercepted_by_output_rule(intercept_content)
            if hit_rule:
                route_info["intercepted_by"] = hit_rule
                if stream:
                    raise IncompleteStreamError(
                        f"output intercepted by rule: {hit_rule}", "output_intercepted"
                    )
                raise EmptyNonStreamResponseError(f"output intercepted by rule: {hit_rule}")
            if stream and buffered_stream_chunks:
                for buffered_chunk in buffered_stream_chunks:
                    if not (isinstance(buffered_chunk, dict) and "_last_route_info" in buffered_chunk):
                        downstream_started = True
                        attempt.downstream_started = True
                        if attempt.first_byte_at is None:
                            attempt.first_byte_at = time.time()
                    yield buffered_chunk
            route_info["status"] = "success"
            _append_channel_retry_attempt(
                channel_attempts,
                attempt_no=inner_index + 1,
                started_at=attempt_start,
                success=True,
                upstream_status=route_info.get("upstream_status") or 200,
            )
            route_info["channel_retry_attempts"] = channel_attempts
            if not is_test:  # 测试不改健康度
                ModelClientPool.record_account_success(
                    provider_name,
                    username,
                    model,
                    session_id=session_id,
                    upstream_model_id=route_info.get("upstream_model_id"),
                    duration_ms=_duration_ms(attempt_start),
                )
            last_route_info = dict(route_info)
            attempt_logged = True
            # 非流式 + 上游不回 usage 的渠道（duckai 等）：_validate_upstream_usage_payload
            # 会把零 completion 的 usage 原地删掉，导致 attempt_usage_body 不被赋值；
            # 非流式又没有 attempt_stream_summary 兜底，finalize_usage_body 会变成 None，
            # _usage 拿不到 result 也就没法按内容估算 → usage 记 0。这里兜底用
            # attempt_response_body（持有带 content 的响应体），让 _usage(estimate=True)
            # 能按 content 估出非 0 usage。流式侧有 summary 兜底，不会走到这里。
            finalize_usage_body = (attempt_usage_body
                or (_stream_summary_to_openai_response(model, attempt_stream_summary) if attempt_stream_summary is not None else None)
                or attempt_response_body)
            await _finalize_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=messages,
                stream=stream,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                client_request_path=client_request_path,
                client_request_body=client_request_body,
                attempt_start=candidate_started_at,
                duration_ms_override=_channel_retry_total_duration(channel_attempts),
                success=True,
                status="ok",
                response_body=attempt_client_response_body or attempt_response_body or {},
                error="",
                usage=_usage(
                    finalize_usage_body,
                    {"messages": messages, "tools": attempt_kwargs.get("tools")},
                    model=model,
                ) if finalize_usage_body else None,
            )
            # 与日志使用同一 usage 口径，供 reservation 成功补差；保留上面 source-slice 锚点，
            # 现有重试测试依赖该源码形态来防止空 summary 覆盖 provider usage 回归。
            final_usage = _usage(
                finalize_usage_body,
                {"messages": messages, "tools": attempt_kwargs.get("tools")},
                model=model,
            ) if finalize_usage_body else None
            if candidate_reservation is not None:
                # 成功：用真实 usage 结算 token 预占（多退少补）；请求计数保留 1。
                await candidate_reservation.commit(final_usage)
                attempt.reservation = candidate_reservation
                # 渠道计量已在候选级结算，endpoint 侧不要再写一次渠道 token（避免双计）。
                route_info["channel_usage_reconciled"] = True
                last_route_info = dict(route_info)
            attempt.finalized = True
            yield {"_last_route_info": last_route_info}
            logger.info(f"[重试调度._chat_with_retry] <<渠道返回 model={model}, provider={provider_name}, completed successfully")
            return
        except NoAvailableAccountError:
            # 选择/预占阶段已经确认没有可用 (渠道,账号,模型)。不重复做相同 acquire；
            # 立即交给外层模型组编排切备选组，备选也耗尽后统一返回 429。
            raise
        except Exception as e:
            last_error = e
            # 先判「能不能在同一账号上原地重发」：够资格且内层还有次数，就不做候选级收尾
            # （不排除账号、不冻结），只落一条本次请求的日志行。
            inner_retry_pending = _should_inner_retry(
                error=e,
                provider_name=provider_name,
                stream=stream,
                downstream_started=downstream_started,
                inner_index=inner_index,
                inner_limit=inner_limit,
            )
            _append_channel_retry_attempt(
                channel_attempts,
                attempt_no=inner_index + 1,
                started_at=attempt_start,
                success=False,
                error=e,
                upstream_status=(
                    e.status_code if isinstance(e, HTTPException)
                    else route_info.get("upstream_status")
                ),
            )
            route_info["channel_retry_attempts"] = channel_attempts
            decision, last_route_info = await _handle_attempt_failure(
                error=e, route_info=route_info, provider_name=provider_name, username=username,
                model=model, retry_count=retry_count, total_attempts=total_attempts,
                request_id=request_id, messages=messages, stream=stream,
                downstream_started=downstream_started, api_key=api_key, api_key_name=api_key_name,
                request_headers=request_headers, client_request_path=client_request_path,
                client_request_body=client_request_body, attempt_start=attempt_start,
                attempt_stream_summary=attempt_stream_summary, client=client,
                session_id=session_id, is_test=is_test, is_probe=is_probe,
                tried_accounts=tried_accounts,
                inner_retry_pending=inner_retry_pending,
                finalize_log=False,
            )
            attempt_logged = True
            if isinstance(e, IncompleteStreamError):
                setattr(e, "_last_route_info", last_route_info)
            if isinstance(e, HTTPException) and e.status_code == 403 and config.Config.system_debug_enabled():
                logger.warning(
                    f"[重试调度._chat_with_retry] 上游 403: provider={provider_name}, account={username}, "
                    f"requested_model={model}, routed_model={route_info.get('routed_model')}, "
                    f"upstream_model={route_info.get('upstream_model_id')}, detail={e.detail}, "
                    f"headers={route_info.get('response_headers')}, body={route_info.get('router_response_body')}"
                )
            if decision.action == retry_policy.FailureAction.STREAM_ERROR_AND_STOP:
                # 已向客户端输出后绝不做内层/外层重试；把当前失败收进同一主日志后再收尾。
                route_info["channel_retry_attempts"] = channel_attempts
                # 已服务：保留请求计数，按已产出的 usage 结算 token 预占，绝不回滚。
                # usage 取上游已上报的累计值，缺失时按已输出内容估算，口径与 downstream_started 同源。
                if candidate_reservation is not None:
                    served_usage = _usage(
                        (_stream_summary_to_openai_response(model, attempt_stream_summary)
                         if attempt_stream_summary is not None else None) or {"usage": accumulated_usage},
                        {"messages": messages, "tools": attempt_kwargs.get("tools")},
                    )
                    await candidate_reservation.keep_served_usage(served_usage)
                    route_info["channel_usage_reconciled"] = True
                await _finalize_channel_attempt_log(
                    request_id=request_id, route_info=route_info, model=model, messages=messages,
                    stream=stream, api_key=api_key, api_key_name=api_key_name,
                    request_headers=request_headers, client_request_path=client_request_path,
                    client_request_body=client_request_body, attempt_start=candidate_started_at,
                    duration_ms_override=_channel_retry_total_duration(channel_attempts),
                    success=False, status="error", response_body=None,
                    error=route_info.get("error") or str(e),
                    usage=None,
                )
                raise _stream_error_from_exception(e, last_route_info)
            if decision.action == retry_policy.FailureAction.RETURN_CLIENT_ERROR:
                route_info["channel_retry_attempts"] = channel_attempts
                await _finalize_channel_attempt_log(
                    request_id=request_id, route_info=route_info, model=model, messages=messages,
                    stream=stream, api_key=api_key, api_key_name=api_key_name,
                    request_headers=request_headers, client_request_path=client_request_path,
                    client_request_body=client_request_body, attempt_start=candidate_started_at,
                    duration_ms_override=_channel_retry_total_duration(channel_attempts),
                    success=False, status="error", response_body=None,
                    error=route_info.get("error") or str(e), usage=None,
                )
                yield {"_last_route_info": last_route_info}
                client_error = e
                if isinstance(e, HTTPException):
                    client_error = _canonical_context_overflow_exception(e) or e
                raise _attach_last_route(client_error, last_route_info)
            # 内层同账号原地重试：不动 outer_index、不释放预占，下一轮复用同一候选。
            # 放在 RETRY 判定之前——外层是否还有候选余额与内层能否原地重发无关，
            # 单候选场景下 candidates_remaining=False 也必须允许把渠道配的次数发完。
            if inner_retry_pending:
                if isinstance(e, IncompleteStreamError):
                    yield {"_last_route_info": last_route_info}
                inner_index += 1
                reuse_candidate = True
                logger.info(
                    f"[重试调度._chat_with_retry] 渠道内层同账号重试 provider={provider_name}, "
                    f"account={username}, 第{inner_index + 1}/{inner_limit}次: {e}"
                )
                continue
            if decision.action == retry_policy.FailureAction.RETRY:
                if isinstance(e, IncompleteStreamError):
                    yield {"_last_route_info": last_route_info}
                elif isinstance(e, EmptyNonStreamResponseError):
                    # 空非流响应通常是上游的瞬态异常，而不是账号已失效。
                    # 有其它账号时，tried_accounts 仍让下一次优先切换；单账号渠道
                    # 则不能因为候选排除把配置的重试次数变成“实际只请求一次”。
                    # 这里只在确认会继续重试时解除本次排除，次数仍由 total_attempts 控制。
                    if provider_name and username:
                        try:
                            tried_accounts.remove((provider_name, username))
                        except ValueError:
                            pass
                elif not isinstance(e, (HTTPException, EmptyNonStreamResponseError)):
                    logger.warning(
                        f"[重试调度._chat_with_retry] 账号 {username} 请求异常，切换候选重试 "
                        f"(候选={outer_index + 1}/{total_attempts}): {e}"
                    )
                # 当前候选内层已结束，先 finalize 它唯一的一条主日志，再换候选。
                route_info["channel_retry_attempts"] = channel_attempts
                await _finalize_channel_attempt_log(
                    request_id=request_id, route_info=route_info, model=model, messages=messages,
                    stream=stream, api_key=api_key, api_key_name=api_key_name,
                    request_headers=request_headers, client_request_path=client_request_path,
                    client_request_body=client_request_body, attempt_start=candidate_started_at,
                    duration_ms_override=_channel_retry_total_duration(channel_attempts),
                    success=False,
                    status="empty_non_stream" if isinstance(e, EmptyNonStreamResponseError) else "error",
                    response_body=None, error=route_info.get("error") or str(e), usage=None,
                )
                # 外层换候选：消耗一个候选名额，下一轮重新选路。
                outer_index += 1
                reuse_candidate = False
                continue
            # _handle_attempt_failure deliberately skips finalization above so each
            # terminal branch can decide its own status.  The fall-through branch
            # is also terminal (for example an unclassified upstream exception),
            # therefore it must close the started log before the test-mode error is
            # re-raised below; otherwise the row remains permanently "requesting".
            route_info["channel_retry_attempts"] = channel_attempts
            await _finalize_channel_attempt_log(
                request_id=request_id, route_info=route_info, model=model, messages=messages,
                stream=stream, api_key=api_key, api_key_name=api_key_name,
                request_headers=request_headers, client_request_path=client_request_path,
                client_request_body=client_request_body, attempt_start=candidate_started_at,
                duration_ms_override=_channel_retry_total_duration(channel_attempts),
                success=False, status="error", response_body=None,
                error=route_info.get("error") or str(e), usage=None,
            )
            break
        except (asyncio.CancelledError, GeneratorExit) as e:
            if provider_name and not attempt_logged:
                try:
                    last_route_info = await _finalize_failed_attempt(
                        error=e, route_info=route_info, provider_name=provider_name, username=username,
                        model=model, retry_count=retry_count, request_id=request_id, messages=messages,
                        stream=stream, api_key=api_key, api_key_name=api_key_name,
                        request_headers=request_headers, client_request_path=client_request_path,
                        client_request_body=client_request_body, attempt_start=attempt_start,
                        attempt_stream_summary=attempt_stream_summary,
                        response_body=attempt_client_response_body,
                        status="cancelled",
                    )
                    attempt_logged = True
                except Exception:
                    # Logging must never mask cancellation/generator shutdown.
                    logger.exception("[request-log] failed to finalize cancelled attempt")
            if candidate_reservation is not None and downstream_started:
                # 客户端在已收到流式内容后断开：上游已经真实服务，保留请求计数并按已产出量结算。
                served_usage = _usage(
                    (_stream_summary_to_openai_response(model, attempt_stream_summary)
                     if attempt_stream_summary is not None else None) or {"usage": accumulated_usage},
                    {"messages": messages, "tools": attempt_kwargs.get("tools")},
                )
                await candidate_reservation.keep_served_usage(served_usage)
            raise
        finally:
            # 预占是**候选级**资源：内层同账号重试期间必须继续持有，否则下一次内层请求
            # 就没有并发 lease 了，而且 account_client.release() 会被多调一次。
            # 本候选结束时：已成功/已服务结算只释放并发；其它（4xx/5xx/网络异常/未产出取消）
            # 回滚本次 request+token ledger 后再释放。CandidateReservation 内部保证幂等。
            if candidate_reservation is not None and not reuse_candidate:
                if candidate_reservation.settled:
                    await candidate_reservation.release()
                else:
                    await candidate_reservation.rollback()
                candidate_reservation = None
                account_client = None
                client = None

    yield {"_last_route_info": last_route_info}
    # 测试/探测模式（管理员内部诊断）：绝不做「统一 429 + RETRYABLE_CLIENT_MESSAGE」归一。
    # 归一策略是给外部客户端隐藏上游错误的，测试 tab / 定时检测恰恰要看真实报错（认证失败/
    # 上游 5xx/超时原文）。此处原样抛出真实 last_error（HTTPException 保留其 status_code +
    # detail，其它异常转 HTTPException 但保留原始文案），并挂上 last_route_info 供诊断。
    if (is_test or is_probe) and last_error is not None:
        if isinstance(last_error, HTTPException):
            raise _attach_last_route(last_error, last_route_info)
        raise _attach_last_route(
            HTTPException(status_code=502, detail=str(last_error) or type(last_error).__name__),
            last_route_info,
        )
    # 所有不可重试客户端错误已在 attempt 内原样抛出；走到这里的都是资源/上游暂时失败，
    # 对客户端统一 429 + 英文业务码（真实 upstream_status 仍在 attempt 日志）。
    code, upstream_status = _terminal_429_from_last_error(last_error, max_retries)
    raise HTTPException(
        status_code=429,
        detail=_retryable_client_error_detail(
            code, max_retries, upstream_status, _retry_after_for_code(code),
        ),
    )


async def _chat_with_retry(model, messages, stream, api_key: str | None = None, api_key_name: str | None = None, chat_method: str = "chat", **kwargs):
    """Run one exhausted group at a time, resolving each backup from a fresh snapshot.

    兼容委托：组回退事实源在 _run_group_fallback_pipeline，本函数仅为保留旧调用方/测试签名。
    """
    async for chunk in _run_group_fallback_pipeline(
        model, messages, stream,
        api_key=api_key, api_key_name=api_key_name, chat_method=chat_method,
        base_kwargs=kwargs,
    ):
        yield chunk


@app.post("/v1/responses")
@app.post("/responses")
async def responses_api(request: Request, authorization: Optional[str] = Header(None)):
    """OpenAI Responses API 兼容接口"""
    api_key = await _extract_api_key(request, authorization, support_x_api_key=True)
    request_headers = _sanitize_headers(request.headers)

    body = await request.json()
    body, client_type = await _prepare_client_request_body(body, api_key, request_headers, request.url.path, "responses")

    # 安全扫描
    if config.Config.security_enabled():
        from security import scan_request, ScanRequest
        scan_req = ScanRequest.from_body(body, api_key=api_key)
        scan_result = await scan_request(scan_req)
        if scan_result.has_tags:
            security_request_id = _new_request_id()
            api_key_name_preview = await _api_key_name(api_key)
            await _record_security_block(
                scan_result=scan_result, request_id=security_request_id,
                api_key=api_key, api_key_name=api_key_name_preview,
                endpoint=request.url.path, body=body, client_type=client_type,
                request_headers=request_headers, source_ip=_client_ip(request),
            )
        if scan_result.blocked:
            raise HTTPException(status_code=400, detail=_openai_error(scan_result.block_reason, "security_error"))

    editor_request_context = await _resolve_request_context(api_key, request_headers, body)

    # 凭据遮蔽
    if config.Config.security_masking_enabled():
        from security import mask_credentials
        mask_result = mask_credentials(body)
        body = mask_result.masked_body

    # 统一入口：校验 + kwargs 组装 + 父日志 + 建生成器（惰性）。白/黑名单由端点层显式提取。
    provider_whitelist, provider_blacklist = await config.Config.get_api_key_provider_filter(api_key)
    api_key_name = await _api_key_name(api_key)
    dispatch = await dispatch_entry(
        endpoint=request.url.path,
        body=body,
        headers=request.headers,
        api_key=api_key,
        api_key_name=api_key_name,
        provider_whitelist=provider_whitelist,
        provider_blacklist=provider_blacklist,
        request_protocol="responses",
        chat_method="chat_responses",
        client_type=client_type,
        editor_request_context=editor_request_context,
    )
    model = dispatch["model"]
    stream = dispatch["stream"]
    start_time = dispatch["start_time"]
    responses_stream = dispatch["generator"]
    try:
        await _acquire_api_key_limit(request, api_key)
    except HTTPException:
        raise

    if stream:
        response_headers = _stream_response_headers()
        try:
            first_chunk = await _prime_stream_before_response(responses_stream)
        except NoAvailableAccountError:
            await _release_api_key_limit(request)
            raise
        except Exception:
            await _release_api_key_limit(request)
            raise
        primed_stream = _prepend_stream_chunk(first_chunk, responses_stream)
        last_route_info = {}
        status = "ok"
        error = ""
        captured_usage: dict | None = None
        captured_response_id: str | None = None
        response_log_body = _new_stream_log_body()

        async def responses_stream_with_logging():
            nonlocal status, error, last_route_info, captured_usage, captured_response_id
            try:
                async for chunk in primed_stream:
                    if isinstance(chunk, dict):
                        if "_last_route_info" in chunk:
                            last_route_info = chunk.get("_last_route_info") or last_route_info
                            continue
                        if chunk.get("_passthrough_done"):
                            if chunk.get("usage"):
                                captured_usage = chunk["usage"]
                            if chunk.get("response_id"):
                                captured_response_id = chunk["response_id"]
                            continue
                        continue
                    _append_stream_log_body(response_log_body, chunk)
                    yield chunk
            except asyncio.CancelledError:
                status = "cancelled"
                error = "client disconnected"
                last_route_info = getattr(primed_stream, "_last_route_info", last_route_info) or last_route_info
                raise
            except Exception as e:
                status = "error"
                error = str(e)
                raise
            finally:
                captured_normalized = normalize_usage(captured_usage or {})
                usage_holder = {"usage": captured_usage or {}}
                if captured_normalized["completion_tokens"] <= 0:
                    usage_holder = response_log_body
                usage_for_stats = _usage(usage_holder, body)
                if status == "ok":
                    await _finalize_request_success_stats(
                        api_key=api_key,
                        requested_model=model,
                        endpoint="/v1/responses",
                        usage=usage_for_stats,
                        last_route_info=last_route_info,
                    )
                elif api_key:
                    _add_recent_log(api_key, model, "/v1/responses", status, duration_ms=_duration_ms(start_time), error=error)
                await _release_api_key_limit(request)

        return StreamingResponse(responses_stream_with_logging(), media_type="text/event-stream", headers=response_headers)

    try:
        result = await _collect_non_stream_result(responses_stream)
    except NoAvailableAccountError:
        await _release_api_key_limit(request)
        raise
    except Exception:
        await _release_api_key_limit(request)
        raise

    last_route_info = result.pop("_last_route_info", {}) if isinstance(result, dict) else {}
    result.pop("_first_token_ms", None) if isinstance(result, dict) else None
    if isinstance(result, dict) and result.get("_passthrough_responses"):
        responses_result = result.get("body") or {}
        usage_holder = dict(responses_result) if isinstance(responses_result, dict) else {}
        usage_holder["usage"] = result.get("usage") or {}
    else:
        responses_result = openai_to_responses_response(result, model)
        usage_holder = responses_result

    usage_for_stats = _usage(usage_holder, body)
    await _finalize_request_success_stats(
        api_key=api_key,
        requested_model=model,
        endpoint="/v1/responses",
        usage=usage_for_stats,
        last_route_info=last_route_info,
    )

    await _release_api_key_limit(request)
    return JSONResponse(responses_result)


@app.post("/v1/messages")
@app.post("/messages")
async def anthropic_messages(request: Request, authorization: Optional[str] = Header(None)):
    """Anthropic Messages API 兼容接口"""

    api_key = await _extract_api_key(request, authorization, support_x_api_key=True)
    request_headers = _sanitize_headers(request.headers)

    body = await request.json()
    body, client_type = await _prepare_client_request_body(body, api_key, request_headers, request.url.path, "anthropic")

    # 安全扫描
    if config.Config.security_enabled():
        from security import scan_request, ScanRequest
        scan_req = ScanRequest.from_body(body, api_key=api_key)
        scan_result = await scan_request(scan_req)
        if scan_result.has_tags:
            security_request_id = _new_request_id()
            api_key_name_preview = await _api_key_name(api_key)
            await _record_security_block(
                scan_result=scan_result, request_id=security_request_id,
                api_key=api_key, api_key_name=api_key_name_preview,
                endpoint=request.url.path, body=body, client_type=client_type,
                request_headers=request_headers, source_ip=_client_ip(request),
            )
        if scan_result.blocked:
            raise HTTPException(status_code=400, detail=_openai_error(scan_result.block_reason, "security_error"))

    editor_request_context = await _resolve_request_context(api_key, request_headers, body)

    # 凭据遮蔽
    if config.Config.security_masking_enabled():
        from security import mask_credentials
        mask_result = mask_credentials(body)
        body = mask_result.masked_body

    # 客户端原始请求统计
    client_messages = body.get("messages", [])
    client_system = body.get("system", "")
    client_tools = body.get("tools", [])
    client_msg_count = len(client_messages)
    client_system_len = len(client_system) if isinstance(client_system, str) else sum(len(b.get("text", "")) for b in client_system if isinstance(b, dict) and b.get("type") == "text")
    logger.info(f"[API.anthropic_messages] <<客户端请求>> model={body.get('model')}, stream={body.get('stream', False)}, "
                f"client_msg_count={client_msg_count}, client_system_len={client_system_len}, "
                f"client_tools_count={len(client_tools)}, max_tokens={body.get('max_tokens')}")

    # 统一入口：校验 + Anthropic→OpenAI 转换 + kwargs 组装 + 建生成器（惰性）。
    # chat_method=chat_anthropic：BaseProvider.chat_anthropic 决定同协议直通或跨协议转换；
    # 转换后的 openai messages 仅在跨协议路径被使用，同协议路径读取 kwargs._raw_anthropic_body。
    provider_whitelist, provider_blacklist = await config.Config.get_api_key_provider_filter(api_key)
    api_key_name = await _api_key_name(api_key)
    dispatch = await dispatch_entry(
        endpoint=request.url.path,
        body=body,
        headers=request.headers,
        api_key=api_key,
        api_key_name=api_key_name,
        provider_whitelist=provider_whitelist,
        provider_blacklist=provider_blacklist,
        request_protocol="anthropic",
        chat_method="chat_anthropic",
        client_type=client_type,
        editor_request_context=editor_request_context,
    )
    await _acquire_api_key_limit(request, api_key)

    model = dispatch["model"]
    stream = dispatch["stream"]
    start_time = dispatch["start_time"]
    anthropic_stream = dispatch["generator"]

    # 转换后的实际请求统计
    dispatched_kwargs = dispatch["kwargs"]
    logger.info(f"[API.anthropic_messages] <<转换后请求>> model={model}, stream={stream}, "
                f"actual_msg_count={len(dispatch['messages'])} (客户端={client_msg_count}), "
                f"actual_tools_count={len(dispatched_kwargs.get('tools') or [])}, "
                f"reasoning_effort={dispatched_kwargs.get('reasoning_effort')}, thinking={dispatched_kwargs.get('thinking')}, max_tokens={dispatched_kwargs.get('max_tokens')}")

    if stream:
        response_headers = _stream_response_headers()
        try:
            first_chunk = await _prime_stream_before_response(anthropic_stream)
        except Exception:
            await _release_api_key_limit(request)
            raise
        primed_stream = _prepend_stream_chunk(first_chunk, anthropic_stream)
        last_route_info = {}
        status = "ok"
        error = ""
        captured_usage: dict | None = None
        captured_message_id: str | None = None
        anthropic_log_body = _new_stream_log_body()

        async def anthropic_stream_with_error():
            nonlocal status, error, last_route_info, captured_usage, captured_message_id
            chunk_count = 0
            try:
                async for chunk in primed_stream:
                    if isinstance(chunk, dict):
                        if "_last_route_info" in chunk:
                            last_route_info = chunk.get("_last_route_info") or last_route_info
                            continue
                        if chunk.get("_passthrough_done"):
                            if chunk.get("usage"):
                                captured_usage = chunk["usage"]
                            if chunk.get("message_id"):
                                captured_message_id = chunk["message_id"]
                            continue
                        # 其他 dict（如 TTFT 信号空字典）忽略
                        continue
                    chunk_count += 1
                    _append_stream_log_body(anthropic_log_body, chunk)
                    yield chunk
                logger.info(f"[anthropic_stream] passthrough completed, total_chunks={chunk_count}, usage={captured_usage}, message_id={captured_message_id}")
            except asyncio.CancelledError:
                status = "cancelled"
                error = "client disconnected"
                last_route_info = getattr(primed_stream, "_last_route_info", last_route_info) or last_route_info
                raise
            except Exception as e:
                status = "error"
                error = str(e)
                logger.error(f"[anthropic_stream] error after {chunk_count} chunks: {type(e).__name__}: {e}")
                logger.exception(e)
                from message_utils import make_anthropic_sse_event
                # 默认口径：绝不透传上游/内部原始错误文案，与非流式最终 429 对齐——
                # 统一 rate_limit + 英文业务 message（真实 upstream_status 仍在日志）。
                normalized_error_message = TERMINAL_ERROR_MESSAGES.get("rate_limit_exceeded", "Rate limit exceeded; please retry later.")
                error_type = "rate_limit_error"
                error_code = "rate_limit_exceeded"
                # 唯一例外：上下文超限。命中且开关开时按分类查表给固定 code/message，
                # 不透传上游原文（上游原文只在日志里）。
                err_status = getattr(e, "status_code", None)
                err_detail = getattr(e, "detail", None) if isinstance(e, HTTPException) else None
                canonical = retry_policy.canonicalize_upstream_error(err_status, err_detail if err_detail is not None else str(e))
                if canonical is not None and config.Config.context_overflow_not_retryable_enabled():
                    normalized_error_message = canonical.message
                    error_type = canonical.type
                    error_code = canonical.code
                error_event = make_anthropic_sse_event("error", {
                    "type": "error",
                    "error": {"type": error_type, "message": normalized_error_message, "code": error_code}
                })
                yield error_event
            finally:
                captured_normalized = normalize_usage(captured_usage or {})
                usage_holder = {"usage": captured_usage or {}}
                if captured_normalized["completion_tokens"] <= 0:
                    usage_holder = anthropic_log_body
                usage_for_stats = _usage(usage_holder, body)
                if status == "ok":
                    await _finalize_request_success_stats(
                        api_key=api_key,
                        requested_model=model,
                        endpoint="/v1/messages",
                        usage=usage_for_stats,
                        last_route_info=last_route_info,
                    )
                elif api_key:
                    _add_recent_log(api_key, model, "/v1/messages", status, duration_ms=_duration_ms(start_time), error=error)
                await _release_api_key_limit(request)

        return StreamingResponse(
            anthropic_stream_with_error(),
            media_type="text/event-stream",
            headers=response_headers,
        )

    try:
        result = await _collect_non_stream_result(anthropic_stream)
    except NoAvailableAccountError:
        await _release_api_key_limit(request)
        raise
    except Exception:
        await _release_api_key_limit(request)
        raise
    last_route_info = result.pop("_last_route_info", {}) if isinstance(result, dict) else {}
    result.pop("_first_token_ms", None) if isinstance(result, dict) else None

    # chat_anthropic 非流式返回 {"_passthrough_anthropic": True, "body": ..., "usage": ..., "message_id": ...}
    if isinstance(result, dict) and result.get("_passthrough_anthropic"):
        anthropic_result = result.get("body") or {}
        usage_holder = dict(anthropic_result) if isinstance(anthropic_result, dict) else {}
        usage_holder["usage"] = result.get("usage") or {}
    else:
        # 兜底：未识别格式时按 openai 响应处理
        anthropic_result = openai_to_anthropic_response(result, model)
        usage_holder = anthropic_result
    usage_for_stats = _usage(usage_holder, body)
    await _finalize_request_success_stats(
        api_key=api_key,
        requested_model=model,
        endpoint="/v1/messages",
        usage=usage_for_stats,
        last_route_info=last_route_info,
    )
    await _release_api_key_limit(request)
    return JSONResponse(anthropic_result)


@app.get("/v1/dashboard/billing/usage")
@app.get("/dashboard/billing/usage")
async def billing_usage(authorization: Optional[str] = Header(None)):
    """获取 billing 用量信息"""
    # 提取 API Key
    api_key = None
    if authorization and authorization.startswith("Bearer "):
        api_key = authorization[7:]

    # 如果没有有效 API Key，返回空统计
    if not api_key or not await config.Config.get_api_key_config(api_key):
        api_key = "anonymous"

    stats = await PostgresClient.billing_usage(api_key)

    return {
        "object": "billing_usage",
        "total_tokens": stats["total_tokens"],
        "prompt_tokens": stats["prompt_tokens"],
        "completion_tokens": stats["completion_tokens"],
        "requests": stats["requests"],
        "timestamp": int(time.time())
    }


@app.post("/v1/token/count")
@app.post("/token/count")
async def token_count(request: Request, authorization: Optional[str] = Header(None)):
    """计算 token 数量（估算）"""
    body = await request.json()

    # 提取 API Key
    api_key = None
    if authorization and authorization.startswith("Bearer "):
        api_key = authorization[7:]

    messages = body.get("messages", [])
    model = body.get("model", "unknown")

    # 简单估算：中文约 1.5 字符/token，英文约 4 字符/token
    # 这里使用一个简单的估算公式
    total_chars = 0
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            total_chars += len(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    total_chars += len(item.get("text", ""))

    # 估算 token 数量 (混合中英文，使用约 2 字符/token 的估算)
    estimated_tokens = total_chars // 2

    return {
        "object": "token_count",
        "model": model,
        "estimated_tokens": estimated_tokens,
        "message_count": len(messages),
        "timestamp": int(time.time())
    }


async def _model_output_modalities(model: str) -> list[str]:
    model_info = ModelClientPool.get_model_info(model) or {}
    modalities = model_info.get("output_modalities")
    if not modalities:
        metadata, _ = await model_metadata.get_model_metadata(model)
        modalities = metadata.get("output_modalities")
    if isinstance(modalities, str):
        modalities = [modalities]
    if not isinstance(modalities, (list, tuple)):
        return ["text"]
    return [str(item).lower() for item in modalities if item]


def _media_generation_kwargs(body: dict, kind: str) -> dict:
    keys = ["size", "n", "response_format", "user"]
    if kind == "image":
        keys.extend(["quality", "style", "input_image", "input_image_mime_type", "image"])
    else:
        keys.extend(["duration", "seconds", "fps", "resolution", "quality", "image", "input_image", "input_image_mime_type"])
    kwargs = {key: body[key] for key in keys if body.get(key) is not None}
    extra_body = body.get("extra_body")
    if isinstance(extra_body, dict):
        kwargs.update(extra_body)
    if kind == "image":
        kwargs.setdefault("size", "1024x1024")
        kwargs.setdefault("quality", "standard")
        kwargs.setdefault("n", 1)
        kwargs.setdefault("response_format", "url")
    else:
        kwargs.setdefault("size", "1280x720")
        kwargs.setdefault("seconds", body.get("seconds") or body.get("duration") or 8)
        kwargs.setdefault("n", 1)
        kwargs.setdefault("response_format", "url")
    return kwargs


def _tts_generation_kwargs(body: dict) -> dict:
    keys = ["voice", "response_format", "speed", "instructions", "user"]
    kwargs = {key: body[key] for key in keys if body.get(key) is not None}
    extra_body = body.get("extra_body")
    if isinstance(extra_body, dict):
        kwargs.update(extra_body)
    kwargs.setdefault("voice", "alloy")
    kwargs.setdefault("response_format", "mp3")
    kwargs.setdefault("speed", 1.0)
    return kwargs


def _audio_content_type(response_format: str | None) -> str:
    mapping = {
        "mp3": "audio/mpeg",
        "opus": "audio/opus",
        "aac": "audio/aac",
        "flac": "audio/flac",
        "wav": "audio/wav",
        "pcm": "audio/pcm",
    }
    return mapping.get((response_format or "").lower(), "audio/mpeg")


def _tts_result_response(result, response_format: str | None = None) -> Response | JSONResponse:
    if isinstance(result, (bytes, bytearray)):
        return Response(content=result, media_type=_audio_content_type(response_format))
    if isinstance(result, dict):
        audio = result.get("audio")
        if isinstance(audio, str):
            try:
                audio_bytes = base64.b64decode(audio)
            except Exception:
                return JSONResponse(result)
            ct = result.get("content_type") or _audio_content_type(response_format)
            return Response(content=audio_bytes, media_type=ct)
        return JSONResponse(result)
    return JSONResponse({"created": int(time.time()), "data": [{"url": result}]})



async def _media_generation_with_retry(
    *,
    kind: str,
    model: str,
    prompt: str,
    body: dict,
    api_key: str | None,
    api_key_name: str | None,
    request_headers: dict | None,
    client_request_path: str,
    client_type: str,
    provider_whitelist: set[str] | None = None,
    provider_blacklist: set[str] | None = None,
    account_whitelist: set[str] | None = None,
    is_test: bool = False,
    is_probe: bool = False,
    is_save_log: bool = True,
) -> dict:
    operation = "image_generation" if kind == "image" else "video_generation"
    method_name = "generate_image" if kind == "image" else "generate_video"
    route = None
    provider_whitelist = provider_whitelist or set()
    provider_blacklist = provider_blacklist or set()
    model_providers = ModelClientPool.get_model_providers(model)
    max_retries = max(0, config.Config.get_global_retry_count())
    total_attempts = 1 if (is_test or is_probe) else max_retries + 1  # 测试/探测模式不重试
    tried_accounts = []
    last_error = None
    request_id = _new_request_id()
    media_kwargs = _media_generation_kwargs(body, kind)
    session_id = _extract_client_session_context(request_headers).get("session_id")

    # 与聊天主链路同构的两层重试：内层同账号原地重发（渠道 retry_count），
    # 外层换候选（全局 retry.max_retries 封顶总请求数）。详见 _chat_with_retry_for_model。
    reuse_candidate = False
    outer_index = 0
    inner_index = 0
    inner_limit = 1
    actual_attempt_no = 0
    provider_name = None
    username = None
    client = None
    account_client = None
    # 媒体生成没有 token usage（成功日志用 _zero_usage），因此只预占请求数（RPM/RPH/RPD），
    # token 预占量为 0；失败同样按 reservation 精确回退，不让失败请求占额度。
    candidate_reservation = None
    while outer_index < total_attempts:
        catalog_snapshot = model_catalog.current_snapshot()
        if not reuse_candidate:
            provider_name = None
            username = None
            client = None
            account_client = None
        route_info = {}
        attempt_start = time.time()
        retry_count = outer_index
        reset_outbound_proxy()
        key_error = await _recheck_api_key_state(api_key, is_test=(is_test or is_probe))
        if key_error is not None:
            raise key_error
        try:
            if not reuse_candidate:
                client, provider_name, account_client = await ModelClientPool.acquire_client_with_provider(
                    model,
                    tried_accounts,
                    None,
                    route,
                    api_key,
                    operation=operation,
                    session_id=session_id,
                    provider_whitelist=provider_whitelist,
                    provider_blacklist=provider_blacklist,
                    account_whitelist=account_whitelist,
                    is_test=is_test,
                    is_probe=is_probe,
                    snapshot=catalog_snapshot,
                )
                username = getattr(client, "username", None)
                candidate_reservation = CandidateReservation(
                    provider_name=provider_name,
                    account_client=account_client,
                    provider=client,
                )
                candidate_reservation._capture_lease()
                inner_limit = _inner_retry_limit(provider_name, is_test=is_test, is_probe=is_probe)
                inner_index = 0
            is_inner_attempt = reuse_candidate
            reuse_candidate = False
            route_info = dict(getattr(account_client, "last_route_info", {}) or {})
            route_info.setdefault("provider", provider_name)
            route_info.setdefault("account", username)
            route_info.setdefault("requested_model", model)
            route_info.setdefault("routed_model", model)
            route_info.setdefault("catalog_generation", catalog_snapshot.generation)
            route_info["_is_save_log"] = is_save_log
            route_info["client_type"] = client_type
            route_info["retry"] = retry_count
            # attempt_no = 同一 request_id 内真实上游请求的单调序号（含内层重试）。
            actual_attempt_no += 1
            route_info["attempt_no"] = actual_attempt_no
            route_info["attempt_key"] = uuid.uuid4().hex
            route_info["outer_retry_index"] = outer_index
            route_info["inner_retry_index"] = inner_index
            route_info["inner_retry_limit"] = inner_limit
            public_model_id = route_info.get("routed_model") or model
            upstream_model_id = route_info.get("upstream_model_id") or ModelClientPool.resolve_upstream_id(provider_name, public_model_id)
            route_info["public_model_id"] = public_model_id
            route_info["upstream_model_id"] = upstream_model_id

            def response_headers_callback(headers):
                sanitized = _sanitize_headers(headers)
                status = sanitized.get(":status") or (headers or {}).get(":status")
                if status:
                    route_info["upstream_status"] = str(status)
                route_info["response_headers"] = sanitized

            def router_request_headers_callback(headers):
                route_info["router_request_headers"] = _sanitize_headers(headers)

            def router_request_body_callback(payload):
                route_info["router_request_body"] = payload

            def router_request_path_callback(path):
                route_info["router_request_path"] = path

            def router_response_body_callback(payload):
                _append_limited_router_response(route_info, payload)

            attempt_kwargs = dict(media_kwargs)
            attempt_kwargs["response_headers_callback"] = response_headers_callback
            attempt_kwargs["router_request_headers_callback"] = router_request_headers_callback
            attempt_kwargs["router_request_body_callback"] = router_request_body_callback
            attempt_kwargs["router_request_path_callback"] = router_request_path_callback
            attempt_kwargs["router_response_body_callback"] = router_response_body_callback
            attempt_kwargs["client_type"] = client_type
            attempt_kwargs["account_client"] = account_client

            pending_log = _build_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=[],
                stream=False,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                start_time=attempt_start,
                duration_ms=0,
                success=False,
                status="requesting",
                response_body=None,
                error="",
                client_request_path=client_request_path,
                client_request_body=body,
            )
            _enqueue_started_log(
                request_id=request_id,
                route_info=route_info,
                pending_log=pending_log,
            )

            result = await getattr(client, method_name)(upstream_model_id, prompt, **attempt_kwargs)
            if not isinstance(result, dict):
                result = {"created": int(time.time()), "data": [{"url": result}]}
            route_info["status"] = "success"
            if not is_test:  # 测试不改健康度
                ModelClientPool.record_account_success(provider_name, username, model, session_id=session_id)
            await _finalize_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=[],
                stream=False,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                client_request_path=client_request_path,
                client_request_body=body,
                attempt_start=attempt_start,
                success=True,
                status="ok",
                response_body=result,
                error="",
                usage=_zero_usage(),
            )
            if candidate_reservation is not None:
                # 媒体成功：无 token usage，仅结算请求数预占（token amount 已为 0）。
                await candidate_reservation.commit(_zero_usage())
            return result
        except NoAvailableAccountError:
            # 选择/预占阶段已经确认没有可用 (渠道,账号,模型)，不写请求日志、不做失败收口，
            # 直接交给上层模型组编排切备选组，备选也耗尽后由外层统一返回 429。
            raise
        except asyncio.CancelledError as e:
            # 客户端断开/请求被取消：finalize 已入队的 started 行，避免永久 requesting。
            await _finalize_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=[],
                stream=False,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                client_request_path=client_request_path,
                client_request_body=body,
                attempt_start=attempt_start,
                success=False,
                status="cancelled",
                response_body=None,
                error="request cancelled before completion",
                usage=None,
            )
            raise
        except Exception as e:
            last_error = e
            # 先判内层同账号资格：够资格且内层有余额时，不做候选级收尾（不排除账号、不冻结）。
            inner_retry_pending = _should_inner_retry(
                error=e,
                provider_name=provider_name,
                stream=False,
                downstream_started=False,
                inner_index=inner_index,
                inner_limit=inner_limit,
            )
            # 与聊天主链路统一：失败日志、健康度、冻结策略、候选排除、重试决策都收口到
            # _handle_attempt_failure；媒体/TTS 无 stream/messages，传空即可。
            decision, last_route_info = await _handle_attempt_failure(
                error=e, route_info=route_info, provider_name=provider_name, username=username,
                model=model, retry_count=retry_count, total_attempts=total_attempts,
                request_id=request_id, messages=[], stream=False,
                downstream_started=False, api_key=api_key, api_key_name=api_key_name,
                request_headers=request_headers, client_request_path=client_request_path,
                client_request_body=body, attempt_start=attempt_start,
                attempt_stream_summary=None, client=client,
                session_id=session_id, is_test=is_test, is_probe=is_probe,
                tried_accounts=tried_accounts,
                inner_retry_pending=inner_retry_pending,
            )
            if isinstance(e, HTTPException):
                setattr(e, "_last_route_info", last_route_info)
            if decision.action == retry_policy.FailureAction.RETURN_CLIENT_ERROR:
                # 客户端请求本身不可继续（普通参数错等）：原样透传；可识别的上下文/输入/
                # prompt/max_tokens 超限先归一成统一 code/message/status 再抛。
                client_error = e
                if isinstance(e, HTTPException):
                    client_error = _canonical_context_overflow_exception(e) or e
                raise _attach_last_route(client_error, last_route_info)
            # 内层同账号原地重发：不换候选、不释放账号，下一轮复用同一 client/account。
            if inner_retry_pending:
                inner_index += 1
                reuse_candidate = True
                logger.info(
                    f"[媒体生成] 渠道内层同账号重试 provider={provider_name}, account={username}, "
                    f"第{inner_index + 1}/{inner_limit}次: {e}"
                )
                continue
            if decision.action == retry_policy.FailureAction.RETRY:
                outer_index += 1
                reuse_candidate = False
                continue
            break
        finally:
            # 账号并发 lease 是候选级资源：内层重试期间继续持有，只在本候选结束时释放。
            # 成功已结算 → 只释放并发；失败（4xx/5xx/网络异常/取消）→ 先按 reservation 精确
            # 回退请求数预占，再释放并发。CandidateReservation 内部幂等。
            if candidate_reservation is not None and not reuse_candidate:
                if candidate_reservation.settled:
                    await candidate_reservation.release()
                else:
                    await candidate_reservation.rollback()
                candidate_reservation = None
                account_client = None
            elif account_client is not None and not reuse_candidate:
                await account_client.release()

    code, upstream_status = _terminal_429_from_last_error(last_error, max_retries)
    detail = _retryable_client_error_detail(code, max_retries, upstream_status, _retry_after_for_code(code))
    raise HTTPException(status_code=429, detail=detail)


async def _tts_generation_with_retry(
    *,
    model: str,
    text: str,
    body: dict,
    api_key: str | None,
    api_key_name: str | None,
    request_headers: dict | None,
    client_request_path: str,
    client_type: str,
    provider_whitelist: set[str] | None = None,
    provider_blacklist: set[str] | None = None,
    account_whitelist: set[str] | None = None,
    is_test: bool = False,
    is_probe: bool = False,
    is_save_log: bool = True,
) -> dict | bytes:
    operation = "tts_generation"
    route = None
    provider_whitelist = provider_whitelist or set()
    provider_blacklist = provider_blacklist or set()
    model_providers = ModelClientPool.get_model_providers(model)
    max_retries = max(0, config.Config.get_global_retry_count())
    total_attempts = 1 if (is_test or is_probe) else max_retries + 1  # 测试/探测模式不重试
    tried_accounts = []
    last_error = None
    request_id = _new_request_id()
    tts_kwargs = _tts_generation_kwargs(body)
    session_id = _extract_client_session_context(request_headers).get("session_id")

    # 与聊天主链路同构的两层重试：内层同账号原地重发（渠道 retry_count），
    # 外层换候选（全局 retry.max_retries 封顶总请求数）。
    reuse_candidate = False
    outer_index = 0
    inner_index = 0
    inner_limit = 1
    actual_attempt_no = 0
    provider_name = None
    username = None
    client = None
    account_client = None
    # TTS 无 token usage（成功用 _zero_usage）：只预占请求数，token 预占量为 0，失败精确回退。
    candidate_reservation = None
    while outer_index < total_attempts:
        catalog_snapshot = model_catalog.current_snapshot()
        if not reuse_candidate:
            provider_name = None
            username = None
            client = None
            account_client = None
        route_info = {}
        attempt_start = time.time()
        retry_count = outer_index
        reset_outbound_proxy()
        key_error = await _recheck_api_key_state(api_key, is_test=(is_test or is_probe))
        if key_error is not None:
            raise key_error
        try:
            if not reuse_candidate:
                client, provider_name, account_client = await ModelClientPool.acquire_client_with_provider(
                    model,
                    tried_accounts,
                    None,
                    route,
                    api_key,
                    operation=operation,
                    session_id=session_id,
                    provider_whitelist=provider_whitelist,
                    provider_blacklist=provider_blacklist,
                    account_whitelist=account_whitelist,
                    is_test=is_test,
                    is_probe=is_probe,
                    snapshot=catalog_snapshot,
                )
                username = getattr(client, "username", None)
                candidate_reservation = CandidateReservation(
                    provider_name=provider_name,
                    account_client=account_client,
                    provider=client,
                )
                candidate_reservation._capture_lease()
                inner_limit = _inner_retry_limit(provider_name, is_test=is_test, is_probe=is_probe)
                inner_index = 0
            is_inner_attempt = reuse_candidate
            reuse_candidate = False
            route_info = dict(getattr(account_client, "last_route_info", {}) or {})
            route_info.setdefault("provider", provider_name)
            route_info.setdefault("account", username)
            route_info.setdefault("requested_model", model)
            route_info.setdefault("routed_model", model)
            route_info.setdefault("catalog_generation", catalog_snapshot.generation)
            route_info["_is_save_log"] = is_save_log
            route_info["client_type"] = client_type
            route_info["retry"] = retry_count
            # attempt_no = 同一 request_id 内真实上游请求的单调序号（含内层重试）。
            actual_attempt_no += 1
            route_info["attempt_no"] = actual_attempt_no
            route_info["attempt_key"] = uuid.uuid4().hex
            route_info["outer_retry_index"] = outer_index
            route_info["inner_retry_index"] = inner_index
            route_info["inner_retry_limit"] = inner_limit
            public_model_id = route_info.get("routed_model") or model
            upstream_model_id = route_info.get("upstream_model_id") or ModelClientPool.resolve_upstream_id(provider_name, public_model_id)
            route_info["public_model_id"] = public_model_id
            route_info["upstream_model_id"] = upstream_model_id

            def response_headers_callback(headers):
                sanitized = _sanitize_headers(headers)
                status = sanitized.get(":status") or (headers or {}).get(":status")
                if status:
                    route_info["upstream_status"] = str(status)
                route_info["response_headers"] = sanitized

            def router_request_headers_callback(headers):
                route_info["router_request_headers"] = _sanitize_headers(headers)

            def router_request_body_callback(payload):
                route_info["router_request_body"] = payload

            def router_request_path_callback(path):
                route_info["router_request_path"] = path

            def router_response_body_callback(payload):
                _append_limited_router_response(route_info, payload)

            attempt_kwargs = dict(tts_kwargs)
            attempt_kwargs["response_headers_callback"] = response_headers_callback
            attempt_kwargs["router_request_headers_callback"] = router_request_headers_callback
            attempt_kwargs["router_request_body_callback"] = router_request_body_callback
            attempt_kwargs["router_request_path_callback"] = router_request_path_callback
            attempt_kwargs["router_response_body_callback"] = router_response_body_callback
            attempt_kwargs["client_type"] = client_type
            attempt_kwargs["account_client"] = account_client

            pending_log = _build_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=[],
                stream=False,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                start_time=attempt_start,
                duration_ms=0,
                success=False,
                status="requesting",
                response_body=None,
                error="",
                client_request_path=client_request_path,
                client_request_body=body,
            )
            _enqueue_started_log(
                request_id=request_id,
                route_info=route_info,
                pending_log=pending_log,
            )

            result = await client.generate_speech(upstream_model_id, text, **attempt_kwargs)
            if not isinstance(result, (dict, bytes, bytearray)):
                result = {"created": int(time.time()), "data": [{"url": result}]}
            route_info["status"] = "success"
            if not is_test:  # 测试不改健康度
                ModelClientPool.record_account_success(provider_name, username, model, session_id=session_id)
            await _finalize_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=[],
                stream=False,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                client_request_path=client_request_path,
                client_request_body=body,
                attempt_start=attempt_start,
                success=True,
                status="ok",
                response_body=result if isinstance(result, dict) else {"audio": "bytes"},
                error="",
                usage=_zero_usage(),
            )
            if candidate_reservation is not None:
                # TTS 成功：无 token usage，仅结算请求数预占（token amount 已为 0）。
                await candidate_reservation.commit(_zero_usage())
            return result
        except NoAvailableAccountError:
            # 选择/预占阶段已经确认没有可用 (渠道,账号,模型)，不写请求日志、不做失败收口，
            # 直接交给上层模型组编排切备选组，备选也耗尽后由外层统一返回 429。
            raise
        except asyncio.CancelledError as e:
            # 客户端断开/请求被取消：finalize 已入队的 started 行，避免永久 requesting。
            await _finalize_channel_attempt_log(
                request_id=request_id,
                route_info=route_info,
                model=model,
                messages=[],
                stream=False,
                api_key=api_key,
                api_key_name=api_key_name,
                request_headers=request_headers,
                client_request_path=client_request_path,
                client_request_body=body,
                attempt_start=attempt_start,
                success=False,
                status="cancelled",
                response_body=None,
                error="request cancelled before completion",
                usage=None,
            )
            raise
        except Exception as e:
            last_error = e
            # 先判内层同账号资格：够资格且内层有余额时，不做候选级收尾（不排除账号、不冻结）。
            inner_retry_pending = _should_inner_retry(
                error=e,
                provider_name=provider_name,
                stream=False,
                downstream_started=False,
                inner_index=inner_index,
                inner_limit=inner_limit,
            )
            # 与聊天主链路统一：失败日志、健康度、冻结策略、候选排除、重试决策都收口到
            # _handle_attempt_failure；媒体/TTS 无 stream/messages，传空即可。
            decision, last_route_info = await _handle_attempt_failure(
                error=e, route_info=route_info, provider_name=provider_name, username=username,
                model=model, retry_count=retry_count, total_attempts=total_attempts,
                request_id=request_id, messages=[], stream=False,
                downstream_started=False, api_key=api_key, api_key_name=api_key_name,
                request_headers=request_headers, client_request_path=client_request_path,
                client_request_body=body, attempt_start=attempt_start,
                attempt_stream_summary=None, client=client,
                session_id=session_id, is_test=is_test, is_probe=is_probe,
                tried_accounts=tried_accounts,
                inner_retry_pending=inner_retry_pending,
            )
            if isinstance(e, HTTPException):
                setattr(e, "_last_route_info", last_route_info)
            if decision.action == retry_policy.FailureAction.RETURN_CLIENT_ERROR:
                # 客户端请求本身不可继续（普通参数错等）：原样透传；可识别的上下文/输入/
                # prompt/max_tokens 超限先归一成统一 code/message/status 再抛。
                client_error = e
                if isinstance(e, HTTPException):
                    client_error = _canonical_context_overflow_exception(e) or e
                raise _attach_last_route(client_error, last_route_info)
            # 内层同账号原地重发：不换候选、不释放账号。
            if inner_retry_pending:
                inner_index += 1
                reuse_candidate = True
                logger.info(
                    f"[语音合成] 渠道内层同账号重试 provider={provider_name}, account={username}, "
                    f"第{inner_index + 1}/{inner_limit}次: {e}"
                )
                continue
            if decision.action == retry_policy.FailureAction.RETRY:
                outer_index += 1
                reuse_candidate = False
                continue
            break
        finally:
            # 账号并发 lease 是候选级资源：内层重试期间继续持有，只在本候选结束时释放。
            # 成功已结算 → 只释放并发；失败（4xx/5xx/网络异常/取消）→ 先按 reservation 精确
            # 回退请求数预占，再释放并发。CandidateReservation 内部幂等。
            if candidate_reservation is not None and not reuse_candidate:
                if candidate_reservation.settled:
                    await candidate_reservation.release()
                else:
                    await candidate_reservation.rollback()
                candidate_reservation = None
                account_client = None
            elif account_client is not None and not reuse_candidate:
                await account_client.release()

    code, upstream_status = _terminal_429_from_last_error(last_error, max_retries)
    detail = _retryable_client_error_detail(code, max_retries, upstream_status, _retry_after_for_code(code))
    raise HTTPException(status_code=429, detail=detail)


async def _media_generation_endpoint(kind: str, request: Request, authorization: Optional[str]) -> JSONResponse:
    api_key = await _extract_api_key(request, authorization, support_x_api_key=True)
    request_headers = _sanitize_headers(request.headers)
    body = await request.json()
    body, client_type = await _prepare_client_request_body(body, api_key, request_headers, request.url.path)

    model = body.get("model")
    if not model:
        raise HTTPException(status_code=400, detail="缺少 model 参数")
    prompt = body.get("prompt")
    if not prompt:
        raise HTTPException(status_code=400, detail="缺少 prompt 参数")

    required_modality = "image" if kind == "image" else "video"
    output_modalities = await _model_output_modalities(model)
    if required_modality not in output_modalities:
        raise HTTPException(status_code=400, detail=f"模型 {model} 不支持{required_modality}生成")

    await _acquire_api_key_limit(request, api_key)
    start_time = time.time()
    api_key_name = await _api_key_name(api_key)
    provider_whitelist, provider_blacklist = await config.Config.get_api_key_provider_filter(api_key)
    try:
        dispatch = await dispatch_entry(
            endpoint=request.url.path,
            body=body,
            headers=request.headers,
            api_key=api_key,
            api_key_name=api_key_name,
            provider_whitelist=provider_whitelist,
            provider_blacklist=provider_blacklist,
            operation=kind,
            client_type=client_type,
        )
        result = dispatch["result"]
        if api_key:
            _add_recent_log(api_key, model, request.url.path, "ok", duration_ms=_duration_ms(start_time))
        return JSONResponse(result)
    except Exception as e:
        if api_key:
            _add_recent_log(api_key, model, request.url.path, "error", duration_ms=_duration_ms(start_time), error=str(e))
        raise
    finally:
        await _release_api_key_limit(request)


async def _tts_endpoint(request: Request, authorization: Optional[str]) -> Response | JSONResponse:
    api_key = await _extract_api_key(request, authorization, support_x_api_key=True)
    request_headers = _sanitize_headers(request.headers)
    body = await request.json()
    body, client_type = await _prepare_client_request_body(body, api_key, request_headers, request.url.path)

    model = body.get("model")
    if not model:
        raise HTTPException(status_code=400, detail="缺少 model 参数")
    text = body.get("input") or body.get("text")
    if not text:
        raise HTTPException(status_code=400, detail="缺少 input 参数")

    await _acquire_api_key_limit(request, api_key)
    start_time = time.time()
    api_key_name = await _api_key_name(api_key)
    provider_whitelist, provider_blacklist = await config.Config.get_api_key_provider_filter(api_key)
    try:
        dispatch = await dispatch_entry(
            endpoint=request.url.path,
            body=body,
            headers=request.headers,
            api_key=api_key,
            api_key_name=api_key_name,
            provider_whitelist=provider_whitelist,
            provider_blacklist=provider_blacklist,
            operation="tts_generation",
            client_type=client_type,
        )
        result = dispatch["result"]
        if api_key:
            _add_recent_log(api_key, model, request.url.path, "ok", duration_ms=_duration_ms(start_time))
        return _tts_result_response(result, body.get("response_format"))
    except Exception as e:
        if api_key:
            _add_recent_log(api_key, model, request.url.path, "error", duration_ms=_duration_ms(start_time), error=str(e))
        raise
    finally:
        await _release_api_key_limit(request)


@app.post("/v1/images/generations")
@app.post("/images/generations")
async def images_generations(request: Request, authorization: Optional[str] = Header(None)):
    return await _media_generation_endpoint("image", request, authorization)


@app.post("/v1/videos/generations")
@app.post("/videos/generations")
async def videos_generations(request: Request, authorization: Optional[str] = Header(None)):
    return await _media_generation_endpoint("video", request, authorization)


@app.post("/v1/audio/speech")
@app.post("/audio/speech")
async def audio_speech(request: Request, authorization: Optional[str] = Header(None)):
    return await _tts_endpoint(request, authorization)


from agent.api import router as agent_router

app.include_router(agent_router)

from agent.llm_config_api import router as agent_llm_config_router

app.include_router(agent_llm_config_router)

# CDP 网页对话的 client-scoped API 也在主进程内提供；MCP Runtime 的
# sse_gateway 通过主服务回环调用，不再依赖独立 Agent 进程。
from agent.cdp_chat_service import router as agent_client_router

app.include_router(agent_client_router)

from mcp.api import router as mcp_router

app.include_router(mcp_router)

# MCP Runtime 合并进主程序：SSE 网关（对外，token 鉴权）+ 管理 API（各路由自带
# _require_admin）。原独立子进程（supervisor + 127.0.0.1:8003 回环）已退役，
# 改为进程内挂载；插件在主 lifespan 里 restore_active_plugins() 注册进 registry 内存单例。
from fastapi import WebSocket as _FastApiWebSocket
from mcp_runtime.sse_gateway import router as mcp_runtime_sse_router
from mcp_runtime.sse_gateway import device_ws as _device_ws_handler
from mcp_runtime.admin_api import router as mcp_runtime_admin_router

app.include_router(mcp_runtime_sse_router)
app.include_router(mcp_runtime_admin_router)


@app.websocket("/ws/device")
async def _device_ws_root_alias(websocket: _FastApiWebSocket):
    """旧版 app 拨的是 `ws://host/ws/device`（无 `/mcp` 前缀），而真实路由在
    `/mcp/device-control/ws/device`——路径不匹配，app 永远 403、卡在「连接中」。
    这里挂一条根级别名兜底到同一 handler，旧 app 不重装、重启服务即连。
    新版 app 已改拨规范路径，本别名仅为存量兼容。
    """
    await _device_ws_handler(websocket, "device-control")


# 资源镜像（skill / 插件）：服务端把市场资源 clone 成 tar.gz 存档，节点按 digest
# 命中缓存后按需拉取。fetch 端点不走 admin 鉴权（节点用 mirror token），其余管理
# 路由各自 _require_admin。
from resources_api import router as resources_router

app.include_router(resources_router)


@app.get("/mcp/health", include_in_schema=False)
async def mcp_runtime_health() -> dict:
    return {"ok": True, "service": "mcp-runtime"}

# Optional upstream platform routes (/api/v1/users). Disabled by default;
# mounted only when AI_LUBRICANT_COMPAT_ENABLED is set. Import is lazy and
# failure-tolerant — it never blocks the main app or the /v1 pipeline.
try:
    import user_platform
    user_platform.mount_routes(app)
except Exception:
    logger.exception("[user-platform] mount skipped")


# ── 前端 SPA 静态资源与路由回退 ──────────────────────────────────────────
# 生产环境：将 user-frontend/dist 作为 SPA 资产目录暴露，并对未匹配的
# 浏览器路径回退到 index.html，避免刷新 /console、/manager 等子路由时 404。
# 用户门户(/console)与管理后台(/manager)共用同一套 React SPA 源码
# (user-frontend submodule，vendored upstream 前端，AGPL)，
# 构建产物输出到 user-frontend/dist。
_ADMIN_DIST_DIR = BASE_DIR / "user-frontend" / "dist"
_ADMIN_INDEX_FILE = _ADMIN_DIST_DIR / "index.html"

# API 前缀：这些路径交给后端路由处理，绝不回退到 SPA
_API_PREFIXES = ("/admin", "/agent", "/mcp", "/v1", "/api", "/static", "/docs", "/project-docs", "/openapi", "/redoc", "/oauth")

if _ADMIN_DIST_DIR.exists():
    app.mount("/admin-static", StaticFiles(directory=_ADMIN_DIST_DIR), name="admin-static")


@app.get("/{full_path:path}", include_in_schema=False)
async def admin_spa_fallback(full_path: str):
    # 命中 API 前缀时让 FastAPI 返回正常 404（由上层异常处理）
    stripped = full_path.lstrip("/")
    if any(stripped == prefix.lstrip("/") or stripped.startswith(prefix.lstrip("/") + "/") for prefix in _API_PREFIXES):
        raise HTTPException(status_code=404, detail="Not Found")
    if not _ADMIN_INDEX_FILE.exists():
        # 没有前端产物时不渲染任何页面：根路径返回构建提示，API 不受影响。
        return PlainTextResponse(
            "user-frontend/dist 未构建：请在 user-frontend 下执行 pnpm install && pnpm build 后重启服务。"
        )
    return FileResponse(_ADMIN_INDEX_FILE)


if __name__ == "__main__":
    # The data service speaks only HTTP/1.1 + WebSockets + SSE — no h2c. Node
    # NodeConnect (prior-knowledge HTTP/2 cleartext) lives in the separate
    # control service (``python -m node_server``, Hypercorn). uvicorn is the
    # standard ASGI server for this surface and needs no h2 handling.
    import socket
    import uvicorn

    # Windows resolves localhost to ::1 first. Binding only 0.0.0.0 makes
    # sequential clients wait for the failed IPv6 connection before retrying
    # 127.0.0.1 (about two seconds on this host). Uvicorn's plain host="::"
    # socket is IPv6-only on Windows, so create an explicit dual-stack socket.
    # Hosts without IPv6 support keep the previous IPv4-only behaviour.
    try:
        listen_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        # Same flag uvicorn.Config.bind_socket sets on the sockets it creates.
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        listen_socket.bind(("::", 8001))
        listen_socket.listen(2048)
    except OSError:
        if "listen_socket" in locals():
            listen_socket.close()
        uvicorn.run(app, host="0.0.0.0", port=8001)
    else:
        uvicorn.Server(uvicorn.Config(app)).run(sockets=[listen_socket])
