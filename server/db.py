"""PostgreSQL 后端：配置、请求日志和看板数据。"""
import asyncio
import hashlib
import json
import secrets
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any
from loguru import logger
from bootstrap_config import get_postgres_config, get_postgres_pool_limits


SELECTION_STRATEGIES = ("sequential", "random_member", "model_random", "random_all", "intelligent", "fast_intelligent")
DEFAULT_SELECTION_STRATEGY = "intelligent"
EDITOR_PROVIDERS = {"claude", "codex", "opencode", "cursor"}


def get_db_config() -> dict:
    return get_postgres_config()


def get_pool_limits() -> dict:
    """读取启动配置中的连接池大小。"""
    return get_postgres_pool_limits()


class PostgresClient:
    pool = None

    @classmethod
    async def init(cls, lightweight: bool = False):
        """初始化 Postgres 连接池并建表。

        lightweight=True（MCP Runtime 等轻量进程）：只建表（全部幂等，含 mcp_* 表），
        跳过 _normalize_legacy_usage_tokens 和各类主服务专属迁移。这些是主服务的
        启动期一次性数据维护（修复历史 token 口径、配置/限流/路由迁移），会随
        request_logs 增长而变慢，不属于 MCP Runtime 的启动关键路径——MCP Runtime
        只需连接池存取插件元数据。把它们塞进 MCP 启动路径会拖慢启动并增加被
        supervisor 健康检查误判重启的概率。

        仪表盘小时聚合不在这里做任何回填：聚合表是长期数据，启动 TRUNCATE 重建
        会把超出 request_logs 保留期的历史一起抹掉。聚合纯增量（_hourly_stats_loop
        每整点补齐），口径变更/历史回填用 script/rebuild_hourly_stats.py、
        script/backfill_hourly_stats.py 手动执行。

        init 幂等：进程首次调用决定是否执行重活；已初始化（cls.pool 存在）直接返回，
        因此后续 CONFIG_STORE.init() 再次调用 init() 不会重复执行。
        """
        if cls.pool:
            return
        logger.debug("import asyncpg")
        import asyncpg
        logger.debug("asyncpg import success")
        cls.pool = await asyncpg.create_pool(**get_db_config(), **get_pool_limits())
        # 建表全部幂等，轻量与完整模式都要执行（mcp_* 表在 create_multi_agent_tables 里）。
        await cls.create_tables()
        await cls.create_agent_tables()
        await cls.create_multi_agent_tables()
        await cls.create_builtin_tool_tables()
        await cls.create_attachment_tables()
        if lightweight:
            logger.info("postgres 连接成功 (lightweight: 跳过回填/规范化/迁移)")
            return
        await cls._normalize_legacy_usage_tokens()
        await cls.migrate_provider_configs_from_app_config(delete_legacy=True)
        await cls.migrate_provider_models_from_legacy_columns()
        await cls.migrate_model_routing_from_app_config()
        await cls.migrate_provider_filters_to_tags()
        await cls.migrate_api_key_groups_to_junction()
        await cls.migrate_model_metadata_into_model_groups()
        await cls.migrate_limit_policies_from_provider_configs()
        await cls.remove_legacy_account_rpd_overrides()
        await cls.migrate_freeze_policy_data()
        await cls.migrate_freeze_object_period_data()
        await cls.migrate_orphan_agents_owner()
        await cls.migrate_orphan_scheduled_tasks_owner()
        logger.info("postgres 连接成功")

    @classmethod
    async def close(cls):
        if cls.pool:
            # pool.close() 会等待所有已借出的连接归还；Ctrl+C 时可能有被取消的
            # 在途请求仍持有连接导致永久阻塞。加超时兜底，超时则强制 terminate。
            try:
                await asyncio.wait_for(cls.pool.close(), timeout=5)
            except (asyncio.TimeoutError, Exception):
                try:
                    cls.pool.terminate()
                except Exception:
                    pass
            cls.pool = None

    @classmethod
    async def create_tables(cls):
        async with cls.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS app_config (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    migration_key TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("ALTER TABLE provider_configs ADD COLUMN IF NOT EXISTS billing_mode TEXT NOT NULL DEFAULT 'token'")
            await conn.execute("ALTER TABLE provider_configs ADD COLUMN IF NOT EXISTS error_rate_threshold REAL NOT NULL DEFAULT 0.3")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS provider_configs (
                    name TEXT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT true,
                    rate_limit JSONB NOT NULL DEFAULT '{}'::jsonb,
                    config JSONB NOT NULL DEFAULT '{}'::jsonb,
                    billing_mode TEXT NOT NULL DEFAULT 'token',
                    error_rate_threshold REAL NOT NULL DEFAULT 0.3,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("ALTER TABLE provider_configs ADD COLUMN IF NOT EXISTS billing_mode TEXT NOT NULL DEFAULT 'token'")
            await conn.execute("ALTER TABLE provider_configs ADD COLUMN IF NOT EXISTS error_rate_threshold REAL NOT NULL DEFAULT 0.3")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS provider_accounts (
                    id BIGSERIAL PRIMARY KEY,
                    provider_name TEXT NOT NULL REFERENCES provider_configs(name) ON DELETE CASCADE,
                    username TEXT NOT NULL,
                    switch BOOLEAN NOT NULL DEFAULT true,
                    priority INTEGER NOT NULL DEFAULT 0,
                    weight INTEGER NOT NULL DEFAULT 1,
                    account JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(provider_name, username)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_accounts_provider ON provider_accounts(provider_name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_accounts_username ON provider_accounts(username)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS request_logs (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    attempt_key TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    api_key TEXT,
                    api_key_name TEXT,
                    provider_name TEXT,
                    account_username TEXT,
                    model TEXT,
                    actual_model TEXT,
                    upstream_returned_model TEXT,
                    endpoint TEXT,
                    success BOOLEAN NOT NULL DEFAULT false,
                    status TEXT,
                    stream BOOLEAN NOT NULL DEFAULT false,
                    duration_ms INTEGER NOT NULL DEFAULT 0,
                    first_token_ms INTEGER,
                    estimated_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    cached_tokens INTEGER NOT NULL DEFAULT 0,
                    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
                    reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                    client_type TEXT,
                    session_id TEXT,
                    router_request_path TEXT,
                    upstream_status TEXT,
                    error TEXT,
                    payload_truncated BOOLEAN NOT NULL DEFAULT false,
                    route_duration_ms INTEGER NOT NULL DEFAULT 0,
                    candidate_collect_ms INTEGER NOT NULL DEFAULT 0,
                    strategy_select_ms INTEGER NOT NULL DEFAULT 0,
                    account_reserve_ms INTEGER NOT NULL DEFAULT 0,
                    routing_redis_degraded BOOLEAN NOT NULL DEFAULT false,
                    routing_detail JSONB,
                    proxy_info JSONB,
                    channel_retry_attempts JSONB NOT NULL DEFAULT '[]'::jsonb
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_created_at ON request_logs(created_at DESC)")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS cached_tokens INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS cache_creation_tokens INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS estimated_prompt_tokens INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS reasoning_tokens INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS payload_truncated BOOLEAN NOT NULL DEFAULT false")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS route_duration_ms INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS candidate_collect_ms INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS strategy_select_ms INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS account_reserve_ms INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS routing_redis_degraded BOOLEAN NOT NULL DEFAULT false")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS routing_detail JSONB")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS proxy_info JSONB")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS channel_retry_attempts JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS router_request_path TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_status TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS actual_model TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_returned_model TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS attempt_key TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS attempt_no INTEGER")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS client_type TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS session_id TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS editor_id TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS editor_session_id TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS task_id TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS api_key_version INTEGER")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS api_key_name_snapshot TEXT")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS api_key_id INTEGER")
            await conn.execute("ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS api_key_parent_id INTEGER")
            await conn.execute("ALTER TABLE request_logs DROP COLUMN IF EXISTS parent_request_id")
            await conn.execute("ALTER TABLE request_logs DROP COLUMN IF EXISTS retry_path")
            # 历史记录按已有 request_id 保留流程关系；空 request_id 绝不合并。
            await conn.execute("UPDATE request_logs SET attempt_key='legacy:' || id::text WHERE attempt_key IS NULL OR attempt_key=''" )
            await conn.execute("""
                WITH numbered AS (
                    SELECT id, row_number() OVER (
                        PARTITION BY CASE WHEN request_id IS NULL OR request_id='' THEN 'legacy:' || id::text ELSE request_id END
                        ORDER BY created_at, id
                    ) AS n
                    FROM request_logs
                    WHERE attempt_no IS NULL
                )
                UPDATE request_logs r SET attempt_no=numbered.n FROM numbered WHERE r.id=numbered.id
            """)
            await conn.execute("ALTER TABLE request_logs ALTER COLUMN attempt_key SET NOT NULL")
            await conn.execute("ALTER TABLE request_logs ALTER COLUMN attempt_no SET NOT NULL")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_request_logs_attempt_key ON request_logs(attempt_key)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_request_attempt ON request_logs(request_id, attempt_no, created_at DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_request_id ON request_logs(request_id)")
            # parent_log_id 只是旧日志技术关联，流程聚合统一改由 request_id 完成。
            await conn.execute("ALTER TABLE request_logs DROP CONSTRAINT IF EXISTS request_logs_parent_log_id_fkey")
            await conn.execute("DROP INDEX IF EXISTS idx_request_logs_parent_log_id")
            await conn.execute("ALTER TABLE request_logs DROP COLUMN IF EXISTS parent_log_id")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_provider ON request_logs(provider_name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_account ON request_logs(account_username)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_api_key ON request_logs(api_key)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_editor ON request_logs(editor_id) WHERE editor_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_editor_session ON request_logs(editor_session_id) WHERE editor_session_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_task ON request_logs(task_id) WHERE task_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_model ON request_logs(model)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_success ON request_logs(success)")
            # api_key_id / api_key_parent_id 是后加的列（见上面的 ADD COLUMN），此前没有
            # 索引：api_key_usage_totals 因此全表扫 request_logs。它不只服务密钥详情页，
            # 还在 LLM 主链路的配额校验里（main._enforce_api_key_usage_limit），所以缺索引
            # 会让每个带额度的请求都随日志表增长而变慢，并占满连接池拖垮其它请求。
            # 用部分索引：绝大多数历史行这两列为 NULL，无需进索引。
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_api_key_id ON request_logs(api_key_id) WHERE api_key_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_api_key_parent_id ON request_logs(api_key_parent_id) WHERE api_key_parent_id IS NOT NULL")
            # recent_logs 按 created_at DESC 取最近 N 条，但过滤条件是 success AND
            # total_tokens>0。这两个条件不在 idx_request_logs_created_at 里，PG 只能沿
            # created_at 逐行回扫做过滤——近期失败/零 token 记录多时要扫很远才凑够 limit。
            # 把过滤条件编进部分索引，让排序与过滤一次走到位。
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_success_recent ON request_logs(created_at DESC) WHERE success = true AND total_tokens > 0")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS hourly_log_stats (
                    hour TIMESTAMPTZ NOT NULL,
                    provider_name TEXT NOT NULL,
                    model TEXT NOT NULL,
                    actual_model TEXT NOT NULL DEFAULT 'unknown',
                    requests INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens BIGINT NOT NULL DEFAULT 0,
                    completion_tokens BIGINT NOT NULL DEFAULT 0,
                    total_tokens BIGINT NOT NULL DEFAULT 0,
                    cached_tokens BIGINT NOT NULL DEFAULT 0,
                    cache_creation_tokens BIGINT NOT NULL DEFAULT 0,
                    reasoning_tokens BIGINT NOT NULL DEFAULT 0,
                    reasoning_requests INTEGER NOT NULL DEFAULT 0,
                    errors INTEGER NOT NULL DEFAULT 0,
                    total_duration_ms BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (hour, provider_name, model, actual_model)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_hourly_log_stats_hour ON hourly_log_stats(hour DESC)")
            await conn.execute("ALTER TABLE hourly_log_stats ADD COLUMN IF NOT EXISTS reasoning_tokens BIGINT NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE hourly_log_stats ADD COLUMN IF NOT EXISTS reasoning_requests INTEGER NOT NULL DEFAULT 0")
            # actual_model：请求真正路由到的模型广场模型 ID（区别于 model = 客户端请求名/自定义别名）。
            # 主统计维度使用 actual_model；旧行没有该列，默认 'unknown'，可用 script/rebuild_hourly_stats.py 重算。
            await conn.execute("ALTER TABLE hourly_log_stats ADD COLUMN IF NOT EXISTS actual_model TEXT NOT NULL DEFAULT 'unknown'")
            await conn.execute("ALTER TABLE hourly_log_stats DROP CONSTRAINT IF EXISTS hourly_log_stats_pkey")
            await conn.execute("ALTER TABLE hourly_log_stats ADD PRIMARY KEY (hour, provider_name, model, actual_model)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS hourly_dashboard_stats (
                    hour TIMESTAMPTZ NOT NULL,
                    provider_name TEXT NOT NULL DEFAULT 'unknown',
                    account_username TEXT NOT NULL DEFAULT '',
                    api_key_name TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT 'unknown',
                    actual_model TEXT NOT NULL DEFAULT 'unknown',
                    requests INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens BIGINT NOT NULL DEFAULT 0,
                    completion_tokens BIGINT NOT NULL DEFAULT 0,
                    total_tokens BIGINT NOT NULL DEFAULT 0,
                    cached_tokens BIGINT NOT NULL DEFAULT 0,
                    cache_creation_tokens BIGINT NOT NULL DEFAULT 0,
                    reasoning_tokens BIGINT NOT NULL DEFAULT 0,
                    reasoning_requests INTEGER NOT NULL DEFAULT 0,
                    errors INTEGER NOT NULL DEFAULT 0,
                    total_duration_ms BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (hour, provider_name, account_username, api_key_name, model, actual_model)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_hourly_dashboard_stats_hour ON hourly_dashboard_stats(hour DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_hourly_dashboard_stats_provider ON hourly_dashboard_stats(provider_name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_hourly_dashboard_stats_api_key ON hourly_dashboard_stats(api_key_name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_hourly_dashboard_stats_model ON hourly_dashboard_stats(model)")
            await conn.execute("ALTER TABLE hourly_dashboard_stats ADD COLUMN IF NOT EXISTS reasoning_tokens BIGINT NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE hourly_dashboard_stats ADD COLUMN IF NOT EXISTS reasoning_requests INTEGER NOT NULL DEFAULT 0")
            # actual_model 迁移同 hourly_log_stats：加列 + 重建主键，旧行可用 script/rebuild_hourly_stats.py 重算。
            await conn.execute("ALTER TABLE hourly_dashboard_stats ADD COLUMN IF NOT EXISTS actual_model TEXT NOT NULL DEFAULT 'unknown'")
            await conn.execute("ALTER TABLE hourly_dashboard_stats DROP CONSTRAINT IF EXISTS hourly_dashboard_stats_pkey")
            await conn.execute("ALTER TABLE hourly_dashboard_stats ADD PRIMARY KEY (hour, provider_name, account_username, api_key_name, model, actual_model)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_hourly_dashboard_stats_actual_model ON hourly_dashboard_stats(actual_model)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id BIGSERIAL PRIMARY KEY,
                    severity TEXT NOT NULL DEFAULT 'info',
                    kind TEXT NOT NULL,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'unread',
                    read_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    occurrence_count INTEGER NOT NULL DEFAULT 1,
                    dedupe_key TEXT,
                    request_log_id BIGINT REFERENCES request_logs(id) ON DELETE SET NULL,
                    provider_name TEXT,
                    account_username TEXT,
                    model TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications(created_at DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_severity ON notifications(severity)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_dedupe_key ON notifications(dedupe_key)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_request_log_id ON notifications(request_log_id)")
            # detail：长文本明细（完整堆栈/上下文/富文本），message 仅存列表页摘要。
            await conn.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS detail TEXT NOT NULL DEFAULT ''")
            # 通知类型化 + 多 owner：event_type 规范化事件类型（account.frozen / channel.created …），
            # owner_type/user_id 让站内通知按 用户/团队/平台 分桶（原表是全局单桶，仅 admin 可见）。
            await conn.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS event_type TEXT")
            await conn.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS owner_type TEXT NOT NULL DEFAULT 'platform'")
            await conn.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS user_id TEXT")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_owner ON notifications(owner_type, user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_event_type ON notifications(event_type)")
            # 出站待推送队列：hook 触发后写一行，worker 按订阅匹配+模板推送。
            # 进程重启不丢——启动时 SELECT pending 回灌入内存队列（asyncio.Event 门控）。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mc_notify_outbox (
                    id UUID PRIMARY KEY,
                    notification_id BIGINT REFERENCES notifications(id) ON DELETE SET NULL,
                    event_type TEXT NOT NULL,
                    params JSONB NOT NULL DEFAULT '{}'::jsonb,
                    owner_type TEXT NOT NULL DEFAULT 'platform',
                    owner_id UUID,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    started_at TIMESTAMPTZ,
                    pushed_at TIMESTAMPTZ
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_outbox_status ON mc_notify_outbox(status, created_at)")
            await conn.execute("ALTER TABLE mc_notify_outbox ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ")
            # 站内通知信封：通知中心不再是「所有事件的默认落库点」，而是一种通知渠道
            # （kind='notify_center'）。emit 时先把标题/正文/级别等存进 envelope，
            # 只有事件真的绑定了通知中心渠道，worker 才据此写 notifications 行。
            await conn.execute("ALTER TABLE mc_notify_outbox ADD COLUMN IF NOT EXISTS envelope JSONB NOT NULL DEFAULT '{}'::jsonb")
            # 订阅规则：一条规则 = (owner) + (出站渠道) + (事件类型) + (参数过滤)。
            # 替代 mc_notify_subscriptions 的被动字符串列表，支持"只盯某些 provider 渠道"等参数化订阅。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mc_notify_subscription_rules (
                    id UUID PRIMARY KEY,
                    owner_type TEXT NOT NULL DEFAULT 'user',
                    owner_id UUID NOT NULL,
                    channel_id UUID NOT NULL,
                    event_type TEXT NOT NULL,
                    filters JSONB NOT NULL DEFAULT '{}'::jsonb,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_sub_rules_owner ON mc_notify_subscription_rules(owner_type, owner_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_sub_rules_event ON mc_notify_subscription_rules(event_type)")
            # 事件定义与渠道绑定分离：事件本身保存生效时间、状态、触发条件和参数，
            # 绑定表只表达「这个事件推到哪些通知渠道」。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mc_notify_events (
                    id UUID PRIMARY KEY,
                    owner_type TEXT NOT NULL DEFAULT 'platform',
                    owner_id UUID,
                    name TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT 'general',
                    status TEXT NOT NULL DEFAULT 'active',
                    effective_from TIMESTAMPTZ,
                    effective_to TIMESTAMPTZ,
                    daily_start TEXT,
                    daily_end TEXT,
                    trigger_condition JSONB NOT NULL DEFAULT '{}'::jsonb,
                    event_params JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_events_owner ON mc_notify_events(owner_type, owner_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_events_type_status ON mc_notify_events(event_type, status)")
            # Normalize daily windows to text (HH:MM:SS). This avoids driver-specific
            # TIME codec differences (notably SQLite test storage) while retaining
            # exact time-of-day semantics; the matcher parses the text back to time.
            await conn.execute("ALTER TABLE mc_notify_events ALTER COLUMN daily_start TYPE TEXT USING daily_start::text")
            await conn.execute("ALTER TABLE mc_notify_events ALTER COLUMN daily_end TYPE TEXT USING daily_end::text")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mc_notify_event_channels (
                    id UUID PRIMARY KEY,
                    event_id UUID NOT NULL,
                    channel_id UUID NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(event_id, channel_id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_event_channels_event ON mc_notify_event_channels(event_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_event_channels_channel ON mc_notify_event_channels(channel_id)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mc_notify_event_states (
                    id UUID PRIMARY KEY,
                    event_id UUID NOT NULL,
                    fingerprint TEXT NOT NULL,
                    window_started_at TIMESTAMPTZ,
                    count INTEGER NOT NULL DEFAULT 0,
                    last_triggered_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(event_id, fingerprint)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mc_notify_event_states_event ON mc_notify_event_states(event_id)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    rate_limit JSONB NOT NULL DEFAULT '{}'::jsonb,
                    disabled BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key)")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS provider_whitelist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS provider_blacklist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS editor_provider_whitelist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS editor_provider_blacklist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS thinking_config JSONB NOT NULL DEFAULT '{}'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS model_whitelist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS model_blacklist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS selection_strategy TEXT NOT NULL DEFAULT 'intelligent'")
            # MonkeyCode 平台归属（可空）：user_id 绑定 C 端用户；vm_id 绑定临时下发 key 的虚拟机。
            # 仅作归属/审计标记，不参与路由/限流/预占；未绑定的存量 key 全部为 NULL，行为不变。
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS user_id TEXT")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS vm_id TEXT")
            # group_ids：该系统 Key 授权给哪些 C 端分组使用（TeamGroup UUID 字符串数组）。
            # 空数组=全局可用；非空=仅所列分组成员请求时该 key 才参与选择。
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS group_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS parent_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS usage_limit JSONB NOT NULL DEFAULT '{}'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS label TEXT NOT NULL DEFAULT ''")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'general'")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id) WHERE user_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_parent ON api_keys(parent_id) WHERE parent_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_scope ON api_keys(scope)")
            # api_key_groups：系统 Key ↔ C 端分组的授权关系（取代 api_keys.group_ids）。
            # 关系存成独立结点表而非 key 行上的 JSONB 数组，这样改 key 的名称/限流/白名单
            # 不会波及授权——旧结构下任何整行 UPDATE 漏传 group_ids 就会把绑定清空。
            #
            # 只有根 Key（parent_id IS NULL）可被授权：派生子 Key（scope=task/copy/editor）
            # 的可用性从其根推导，不复制授权，否则任务子 Key 会跟着出现在同组其他成员的
            # 父 Key 选择器里。
            #
            # group_id 无外键：本表由 PostgresClient.init 建立，而 mc_team_groups 由稍后的
            # monkeycode_compat.init 建（compat 关闭时根本不存在），跨层硬外键无法成立。
            # 分组删除时的级联由 team_users_service.delete_group 显式调用清理。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS api_key_groups (
                    api_key_id INTEGER NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
                    group_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (api_key_id, group_id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_api_key_groups_group ON api_key_groups(group_id)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS editors (
                    id TEXT PRIMARY KEY,
                    owner_user_id TEXT NOT NULL,
                    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex', 'opencode', 'cursor')),
                    project_id TEXT,
                    branch TEXT,
                    workdir TEXT,
                    node_id TEXT,
                    api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
                    mcp_config JSONB NOT NULL DEFAULT '[]'::jsonb,
                    skill_config JSONB NOT NULL DEFAULT '[]'::jsonb,
                    plugin_config JSONB NOT NULL DEFAULT '[]'::jsonb,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editors_owner ON editors(owner_user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editors_api_key ON editors(api_key_id) WHERE api_key_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editors_node ON editors(node_id) WHERE node_id IS NOT NULL")
            # Cursor 加入 provider 全集后，老库的 editors CHECK 约束还是三值白名单；
            # 用与 builtin_tool_* 相同的 DO 块手法就地替换，幂等。
            await conn.execute("""
                DO $$ DECLARE c RECORD;
                BEGIN
                    FOR c IN
                        SELECT conname FROM pg_constraint
                        WHERE conrelid = 'editors'::regclass
                          AND contype = 'c'
                          AND pg_get_constraintdef(oid) LIKE '%provider%'
                    LOOP
                        EXECUTE format('ALTER TABLE editors DROP CONSTRAINT %I', c.conname);
                    END LOOP;
                    ALTER TABLE editors
                        ADD CONSTRAINT editors_provider_check
                        CHECK (provider IN ('claude', 'codex', 'opencode', 'cursor'));
                END $$;
            """)
            await conn.execute("DROP INDEX IF EXISTS uq_editors_active_project")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editors_project_status ON editors(project_id, status) WHERE project_id IS NOT NULL")
            await conn.execute("ALTER TABLE editors ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''")
            await conn.execute("ALTER TABLE editors ADD COLUMN IF NOT EXISTS branch_mode TEXT NOT NULL DEFAULT 'default'")
            # Existing editors used a non-empty branch as an explicit checkout.
            # Preserve that behavior while empty branches continue to follow HEAD.
            await conn.execute("UPDATE editors SET branch_mode='existing' WHERE branch IS NOT NULL AND btrim(branch) <> '' AND branch_mode='default'")
            # 项目提示词：管理端维护一份提示词库，编辑器绑定 prompt_id，切换后写入工作目录
            # 的 CLAUDE.md / AGENTS.md。空/NULL 表示不注入。
            await conn.execute("ALTER TABLE editors ADD COLUMN IF NOT EXISTS prompt_id TEXT")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS project_prompts (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    content TEXT NOT NULL DEFAULT '',
                    providers JSONB NOT NULL DEFAULT '[]'::jsonb,
                    enabled BOOLEAN NOT NULL DEFAULT true,
                    owner_user_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS owner_user_id TEXT")
            # 市场提示词落地后保留稳定来源与版本，资源引用表以此解析本地实体。
            await conn.execute("ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS market_id TEXT")
            await conn.execute("ALTER TABLE project_prompts ADD COLUMN IF NOT EXISTS market_version TEXT NOT NULL DEFAULT ''")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_project_prompts_market ON project_prompts(market_id) WHERE market_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_project_prompts_enabled ON project_prompts(enabled)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_project_prompts_owner ON project_prompts(owner_user_id) WHERE owner_user_id IS NOT NULL")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS editor_sessions (
                    id TEXT PRIMARY KEY,
                    editor_id TEXT NOT NULL REFERENCES editors(id) ON DELETE RESTRICT,
                    node_session_id TEXT UNIQUE,
                    provider_thread_id TEXT,
                    model TEXT,
                    status TEXT NOT NULL DEFAULT 'provisioning',
                    expected_client_id TEXT,
                    bootstrap_token_hash TEXT,
                    first_request_seen BOOLEAN NOT NULL DEFAULT false,
                    first_request_id TEXT,
                    last_request_at TIMESTAMPTZ,
                    closed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS bootstrap_content_hash TEXT")
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS bootstrap_consumed BOOLEAN NOT NULL DEFAULT false")
            # Key 归属反转：一个 session 一把子 Key（父 Key 在建 session 时选）。
            # editors.api_key_id 保留兜底，老编辑器及其在跑 session 继续可解析。
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL")
            # 一个 session 携带一组可用模型；model 列仍是"当前激活模型"。
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS models_json JSONB NOT NULL DEFAULT '[]'::jsonb")
            # 普通创建任务的意图快照；沿用 issue workflow 的 role/sub_type 语义。
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS task_name TEXT")
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS task_type TEXT")
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS task_role TEXT")
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS sub_type TEXT")
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS issue_id UUID")
            # Provider-native permission/approval mode for this session (codex
            # read-only/workspace-write/…, claude default/plan/acceptEdits/…).
            # Empty = the provider's own default. Applied per turn, never a restart.
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS mode TEXT")
            # Session-scoped MCP overlay (e.g. issue-workflow identity URL). This is
            # added to the editor's base mcp_config on every runtime resync so
            # editor-level hot updates do not wipe per-task authorization.
            await conn.execute("ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS mcp_overlay_json JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editor_sessions_api_key ON editor_sessions(api_key_id) WHERE api_key_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editor_sessions_editor ON editor_sessions(editor_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editor_sessions_status ON editor_sessions(status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_editor_sessions_node ON editor_sessions(node_session_id) WHERE node_session_id IS NOT NULL")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_editor_sessions_pending_bootstrap ON editor_sessions(editor_id, expected_client_id, bootstrap_content_hash) WHERE status='pending_first_request' AND bootstrap_consumed=false AND expected_client_id IS NOT NULL AND bootstrap_content_hash IS NOT NULL")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_editor_sessions_provider_thread ON editor_sessions(provider_thread_id) WHERE provider_thread_id IS NOT NULL")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS operation_logs (
                    id BIGSERIAL PRIMARY KEY,
                    operator TEXT,
                    action TEXT NOT NULL,
                    target_type TEXT,
                    target_name TEXT,
                    old_data JSONB,
                    new_data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_operation_logs_created_at ON operation_logs(created_at DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_operation_logs_action ON operation_logs(action)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_operation_logs_target ON operation_logs(target_type, target_name)")
            # 账号授权任务（device_code 型需轮询，callback 型不需）。两类同表，靠 task_type 区分。
            # 明细 data 存全量 JSONB；expires_at 提为列供扫描器按时间过滤 + 过期清扫。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS account_auth_states (
                    state TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    task_type TEXT,
                    status TEXT,
                    next_poll_at DOUBLE PRECISION,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    data JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            # 扫描器每秒查 pending device_code 且未过期；用复合索引一步定位。
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_account_auth_states_scan ON account_auth_states(task_type, status, expires_at)")
            # 每轮扫完按 expires_at 清扫过期行（含 callback 型）。
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_account_auth_states_expires ON account_auth_states(expires_at)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_metadata (
                    model_id TEXT PRIMARY KEY,
                    data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            # 旧 schema 迁移：若 system_id 列还在，按"system_id 优先"合并并改名为 model_id
            has_system_id = await conn.fetchval(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_name='model_metadata' AND column_name='system_id'
                """
            )
            if has_system_id:
                async with conn.transaction():
                    await conn.execute("ALTER TABLE model_metadata DROP CONSTRAINT IF EXISTS uq_model_metadata_system_id")
                    await conn.execute("DROP INDEX IF EXISTS idx_model_metadata_system_id")
                    # 若 row B 的 model_id 与某 row A 的非空 system_id 重名（A 才是 system 公开名）：删 B
                    await conn.execute(
                        """
                        DELETE FROM model_metadata
                        WHERE COALESCE(system_id,'') = ''
                          AND model_id IN (
                              SELECT system_id FROM model_metadata
                              WHERE system_id IS NOT NULL AND system_id <> ''
                          )
                        """
                    )
                    # 把没有显式 system_id 的行的 system_id 设为自身 model_id
                    await conn.execute(
                        "UPDATE model_metadata SET system_id = model_id WHERE COALESCE(system_id,'') = ''"
                    )
                    await conn.execute("ALTER TABLE model_metadata DROP CONSTRAINT IF EXISTS model_metadata_pkey")
                    await conn.execute("ALTER TABLE model_metadata DROP COLUMN model_id")
                    await conn.execute("ALTER TABLE model_metadata RENAME COLUMN system_id TO model_id")
                    await conn.execute("ALTER TABLE model_metadata ADD PRIMARY KEY (model_id)")
            # 上一轮加过的 mapping 表合并到 model_metadata.system_id，删掉
            await conn.execute("DROP TABLE IF EXISTS model_metadata_mappings")

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS provider_models (
                    provider TEXT NOT NULL,
                    upstream_model_id TEXT NOT NULL,
                    model_id TEXT NOT NULL,
                    account_tpm INTEGER,
                    extra_config JSONB NOT NULL DEFAULT '{}'::jsonb,
                    PRIMARY KEY (provider, upstream_model_id)
                )
            """)
            await conn.execute("ALTER TABLE provider_models ADD COLUMN IF NOT EXISTS account_tpm INTEGER")
            await conn.execute("ALTER TABLE provider_models ALTER COLUMN account_tpm DROP NOT NULL")
            await conn.execute("ALTER TABLE provider_models ADD COLUMN IF NOT EXISTS extra_config JSONB NOT NULL DEFAULT '{}'::jsonb")
            # 旧版渠道模型表上的 max_tokens / thinking_mode / thinking_max_tokens 字段已废弃：
            # 能力下沉到 extra_config（通用覆盖出站请求），不再用专用列。删除遗留列。
            await conn.execute("ALTER TABLE provider_models DROP COLUMN IF EXISTS max_tokens")
            await conn.execute("ALTER TABLE provider_models DROP COLUMN IF EXISTS thinking_mode")
            await conn.execute("ALTER TABLE provider_models DROP COLUMN IF EXISTS thinking_max_tokens")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_models_model_id ON provider_models(model_id)")

            # 专线（model_routes）已收敛到自定义模型 + API Key，整表废弃。
            await conn.execute("DROP TABLE IF EXISTS model_routes")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_groups (
                    name TEXT PRIMARY KEY,
                    kind TEXT NOT NULL DEFAULT 'custom',
                    enabled BOOLEAN NOT NULL DEFAULT true,
                    remark TEXT NOT NULL DEFAULT '',
                    models JSONB NOT NULL DEFAULT '[]'::jsonb,
                    aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
                    provider_whitelist JSONB NOT NULL DEFAULT '[]'::jsonb,
                    provider_blacklist JSONB NOT NULL DEFAULT '[]'::jsonb,
                    selection_strategy TEXT NOT NULL DEFAULT 'intelligent',
                    backup_group TEXT NOT NULL DEFAULT '',
                    response_model TEXT NOT NULL DEFAULT '',
                    metadata_model TEXT NOT NULL DEFAULT '',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    schemes JSONB NOT NULL DEFAULT '[]'::jsonb,
                    active_scheme TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_model_groups_enabled ON model_groups(enabled)")
            await conn.execute("ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'custom'")
            await conn.execute("ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS aliases JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS selection_strategy TEXT NOT NULL DEFAULT 'intelligent'"
            )
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS backup_group TEXT NOT NULL DEFAULT ''"
            )
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS response_model TEXT NOT NULL DEFAULT ''"
            )
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS metadata_model TEXT NOT NULL DEFAULT ''"
            )
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            # 多套方案：每个自定义模型可保存多套 {name, models, provider_whitelist,
            # provider_blacklist}，同一时刻仅 active_scheme 一套生效；激活方案的三字段
            # 投影回顶层 models/provider_whitelist/provider_blacklist，运行时选路不变。
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS schemes JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
            await conn.execute(
                "ALTER TABLE model_groups ADD COLUMN IF NOT EXISTS active_scheme TEXT NOT NULL DEFAULT ''"
            )
            # 自定义模型的 API Key 范围收敛到 API Key 侧（模型白/黑名单），废弃组上的相关列。
            await conn.execute("ALTER TABLE model_groups DROP COLUMN IF EXISTS use_all_api_keys")
            await conn.execute("ALTER TABLE model_groups DROP COLUMN IF EXISTS api_key_ids")
            await conn.execute("ALTER TABLE model_groups DROP COLUMN IF EXISTS api_key_names")
            await conn.execute("ALTER TABLE model_groups DROP COLUMN IF EXISTS hidden_in_admin")
            await conn.execute("ALTER TABLE model_groups DROP COLUMN IF EXISTS extra_cache")
            # 选择策略收敛到 API Key 侧。
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS selection_strategy TEXT NOT NULL DEFAULT 'intelligent'")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS model_whitelist JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS model_blacklist JSONB NOT NULL DEFAULT '[]'::jsonb")

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS provider_limit_policies (
                    id BIGSERIAL PRIMARY KEY,
                    provider_name TEXT NOT NULL REFERENCES provider_configs(name) ON DELETE CASCADE,
                    name TEXT NOT NULL DEFAULT 'default',
                    enabled BOOLEAN NOT NULL DEFAULT true,
                    account_rpm INTEGER NOT NULL DEFAULT 0,
                    account_tpm INTEGER NOT NULL DEFAULT 0,
                    model_tpm INTEGER NOT NULL DEFAULT 0,
                    account_concurrent INTEGER NOT NULL DEFAULT 0,
                    cooldown_policy JSONB NOT NULL DEFAULT '{"429":"60","error":"60"}'::jsonb,
                    freeze_policy JSONB NOT NULL DEFAULT '{"enabled":false,"rules":[]}'::jsonb,
                    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(provider_name, name)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_limit_policies_provider ON provider_limit_policies(provider_name)")
            await conn.execute("ALTER TABLE provider_limit_policies ADD COLUMN IF NOT EXISTS freeze_policy JSONB NOT NULL DEFAULT '{\"enabled\":false,\"rules\":[]}'::jsonb")
            # 到量冻结维度：每小时次数/每小时 tokens/每天次数/每天 tokens（触线即冻结账号）
            await conn.execute("""
                ALTER TABLE provider_limit_policies
                    ADD COLUMN IF NOT EXISTS account_rph INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS account_tph INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS account_rpd INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS account_tpd INTEGER NOT NULL DEFAULT 0
            """)

            # HF tokenizer 词表：tokenizer.json 原文存 PG，多实例共享、容器重建不丢。
            # 运行时绝不查这张表——启动预载进进程内存，热路径只读内存。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tokenizer_vocabs (
                    repo TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    etag TEXT,
                    bytes INTEGER NOT NULL DEFAULT 0,
                    mirror TEXT,
                    downloaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_tokenizer_vocabs_updated_at ON tokenizer_vocabs(updated_at DESC)")


    @classmethod
    async def create_agent_tables(cls):
        async with cls.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_insight_index (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE NOT NULL,
                    value TEXT NOT NULL,
                    category TEXT DEFAULT 'general',
                    priority INT DEFAULT 0,
                    expires_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_global_facts (
                    id SERIAL PRIMARY KEY,
                    fact_key TEXT UNIQUE NOT NULL,
                    fact_value JSONB NOT NULL,
                    source TEXT DEFAULT 'agent',
                    confidence REAL DEFAULT 1.0,
                    verified BOOLEAN DEFAULT false,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_skills (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    category TEXT,
                    content TEXT NOT NULL,
                    trigger_patterns TEXT[],
                    version INT DEFAULT 1,
                    parent_skill_id INT REFERENCES agent_skills(id),
                    usage_count INT DEFAULT 0,
                    success_count INT DEFAULT 0,
                    avg_duration_ms INT DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    metadata JSONB DEFAULT '{}'
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_session_archives (
                    id SERIAL PRIMARY KEY,
                    session_id TEXT UNIQUE NOT NULL,
                    request_log_id INT REFERENCES request_logs(id),
                    task_description TEXT,
                    summary TEXT,
                    key_insights TEXT[],
                    skills_used INT[],
                    total_turns INT DEFAULT 0,
                    total_duration_ms INT DEFAULT 0,
                    success BOOLEAN,
                    archived_at TIMESTAMPTZ DEFAULT now(),
                    metadata JSONB DEFAULT '{}'
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_tasks (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    status TEXT DEFAULT 'pending',
                    prompt TEXT NOT NULL,
                    source TEXT DEFAULT 'api',
                    model TEXT,
                    current_turn INT DEFAULT 0,
                    max_turns INT DEFAULT 80,
                    total_tokens INT DEFAULT 0,
                    error TEXT,
                    session_id TEXT,
                    parent_task_id UUID REFERENCES agent_tasks(id),
                    created_at TIMESTAMPTZ DEFAULT now(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_scheduled_tasks (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    cron_expression TEXT NOT NULL,
                    task_prompt TEXT NOT NULL,
                    skill_id INT REFERENCES agent_skills(id),
                    enabled BOOLEAN DEFAULT true,
                    last_run_at TIMESTAMPTZ,
                    next_run_at TIMESTAMPTZ,
                    last_result TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            # user_id：定时任务归属（多用户隔离）。历史行 NULL = 平台/管理员态，仅管理端可见。
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS user_id TEXT")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_scheduled_user ON agent_scheduled_tasks(user_id)")
            # ── 脚本型定时任务 + 定时背景 + 报错自愈 ──
            # task_kind: prompt（历史行为，读 task_prompt 跑 agent）| script（跑 script_code，
            #   失败按 on_error 触发自愈）。历史行 NULL/缺失 → 读侧归一为 'prompt'，行为不变。
            # script_code/script_type/script_timeout: 脚本正文 + python|powershell + 秒级超时，
            #   与 code_run 工具同枚举同执行体（subprocess），复用而非另写执行器。
            # approved_hash: 人工授权过的脚本 sha256；NULL 或与当前 script_code 哈希不符即拒跑
            #   （哈希锁生效点：AI/人改了 script_code 就得重新授权）。
            # pending_script_code/hash: AI 自愈提出但未授权的新脚本，等人 approve-script 提升。
            # allow_ai_script_fix: 创建时人工开关；true 时 AI 的新脚本直接写入 script_code 并
            #   同步 approved_hash、自动重跑一次；false 时只落 pending 并 disable。
            # background: 「定时的背景」——任务为什么存在/判断口径/注意事项，注入给自愈 AI。
            # on_error: none | diagnose | diagnose_fix_retry（诊断 + 改脚本 + 重跑一次）。
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS task_kind TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS script_code TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS script_type TEXT DEFAULT 'python'")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS script_timeout INT DEFAULT 300")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS approved_hash TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS pending_script_code TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS pending_script_hash TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS allow_ai_script_fix BOOLEAN DEFAULT false")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS background TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS on_error TEXT DEFAULT 'diagnose'")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS last_exit_code INT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS last_stderr TEXT")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS consecutive_failures INT DEFAULT 0")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS heal_history JSONB DEFAULT '[]'::jsonb")
            # 任务级模型覆盖（prompt 模式）：优先级 任务级 > Agent 的 scheduled_* > 主 Agent。
            # 同一个 Agent 的不同定时任务可以各用一个模型（比如日报用便宜的、巡检用强的）。
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS api_key_id INTEGER")
            await conn.execute("ALTER TABLE agent_scheduled_tasks ADD COLUMN IF NOT EXISTS model VARCHAR(200)")
            # 脚本型任务没有 prompt：放宽 NOT NULL，让 task_kind='script' 也能建。
            await conn.execute("ALTER TABLE agent_scheduled_tasks ALTER COLUMN task_prompt DROP NOT NULL")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_config (
                    key TEXT PRIMARY KEY,
                    value JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_tasks_status ON agent_tasks(status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_tasks_parent ON agent_tasks(parent_task_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_skills_parent ON agent_skills(parent_skill_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_skills_category ON agent_skills(category)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_tasks_created ON agent_tasks(created_at DESC)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS security_events (
                    id BIGSERIAL PRIMARY KEY,
                    request_id VARCHAR(64),
                    request_log_id BIGINT REFERENCES request_logs(id) ON DELETE SET NULL,
                    event_time TIMESTAMPTZ DEFAULT NOW(),
                    event_type VARCHAR(64),
                    severity VARCHAR(16),
                    tag VARCHAR(128),
                    detail JSONB DEFAULT '{}',
                    api_key VARCHAR(128),
                    model VARCHAR(128),
                    source_ip VARCHAR(64)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_security_events_time ON security_events(event_time DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_security_events_tag ON security_events(tag)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_security_events_severity ON security_events(severity)")
            await conn.execute("""
                ALTER TABLE security_events
                ADD COLUMN IF NOT EXISTS request_log_id BIGINT REFERENCES request_logs(id) ON DELETE SET NULL
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_security_events_request_log_id ON security_events(request_log_id)")
            # 兼容旧 SQL 初始化文件的同步：旧版 init.sql 没带 request_log_id
            await conn.execute("""
                UPDATE security_events se
                SET request_log_id = rl.id
                FROM request_logs rl
                WHERE se.request_id = rl.request_id
                  AND se.request_log_id IS NULL
                  AND se.request_id <> ''
            """)

    @staticmethod
    async def _migrate_mcp_user_token_hashes(conn) -> None:
        """Hash legacy MCP principal tokens without requiring pgcrypto."""
        async with conn.transaction():
            rows = await conn.fetch("""
                SELECT id, token
                FROM mcp_users
                WHERE token_hash IS NULL AND token IS NOT NULL AND token <> ''
            """)
            if rows:
                await conn.executemany(
                    "UPDATE mcp_users SET token_hash=$1, token_hint=$2 WHERE id=$3",
                    [
                        (
                            hashlib.sha256(row["token"].encode("utf-8")).hexdigest(),
                            f"{row['token'][:8]}...{row['token'][-4:]}",
                            row["id"],
                        )
                        for row in rows
                    ],
                )
            await conn.execute(
                "INSERT INTO _mcp_migration_flags(key) VALUES('mcp_users_token_hashed') ON CONFLICT DO NOTHING"
            )

    @classmethod
    async def create_multi_agent_tables(cls):
        """Create multi-agent tables and migrate existing data for agent isolation."""
        async with cls.pool.acquire() as conn:
            # ── agents 表 ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agents (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    display_name VARCHAR(200),
                    description TEXT DEFAULT '',
                    api_key VARCHAR(500) NOT NULL,
                    model VARCHAR(200) NOT NULL,
                    system_prompt TEXT DEFAULT '',
                    max_turns INT DEFAULT 80,
                    enabled BOOLEAN DEFAULT TRUE,
                    memory_enabled BOOLEAN DEFAULT TRUE,
                    skill_auto_learn BOOLEAN DEFAULT TRUE,
                    scheduler_enabled BOOLEAN DEFAULT FALSE,
                    workspace_root VARCHAR(500) DEFAULT 'agent/workspace',
                    allowed_roots TEXT[] DEFAULT '{agent/workspace,agent/temp}',
                    denied_patterns TEXT[] DEFAULT '{/etc/,/var/,.env,.git/,node_modules/}',
                    guardian_enabled BOOLEAN DEFAULT FALSE,
                    guardian_interval INT DEFAULT 300,
                    autonomous_enabled BOOLEAN DEFAULT FALSE,
                    thinking_enabled BOOLEAN DEFAULT FALSE,
                    reasoning_effort VARCHAR(20) DEFAULT '',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_name ON agents(name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_enabled ON agents(enabled)")
            # mcp_user_id：把 agent 绑定到某个 MCP 用户（cdp-bridge 客户端）。
            # 绑定后：该用户的浏览器会话面板可选到此 agent；agent 跑 cdp 工具用该用户 token
            # （操作打开面板的那个浏览器），而非 _collect_service_tokens 的任取。
            # mcp_users 建表在后面，用 IF NOT EXISTS ADD COLUMN 而非内联外键，避免建表顺序耦合。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS mcp_user_id INTEGER")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_mcp_user ON agents(mcp_user_id)")
            # user_id：C 端用户归属。NULL = 平台 Agent（管理员建，所有用户可见/可用）。
            # agent 用服务端生成的整数 id 作唯一标识，name 只是显示名 → 去掉全局 name 唯一约束，
            # 允许不同用户的 agent 重名。idx_agents_name（普通索引）保留无妨。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS user_id TEXT")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_user ON agents(user_id)")
            # team_id + is_team_shared：团队共享维度。废弃「user_id IS NULL = 平台 Agent」
            # 的隐式语义——每个 agent 都有 owner（user_id），is_team_shared=true 时同 team
            # 成员可见。平台 Agent 改由显式标记表达，不再靠 user_id 留空。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS team_id TEXT")
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS is_team_shared BOOLEAN DEFAULT FALSE")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_team ON agents(team_id)")
            await conn.execute("ALTER TABLE agents DROP CONSTRAINT IF EXISTS agents_name_key")
            # agent 不再自带 MCP 服务清单：挂哪些 MCP 完全由绑定 principal（mcp_user_id）
            # 的授权 + params 推导（见 agent/mcp_client.resolve_effective_services）。
            # 幂等丢弃历史 mcp_servers 列（存量勾选数据一并作废）。
            await conn.execute("ALTER TABLE agents DROP COLUMN IF EXISTS mcp_servers")

            # ── mcp_services 表 ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_services (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    display_name VARCHAR(200),
                    description TEXT DEFAULT '',
                    category VARCHAR(50) DEFAULT 'custom',
                    transport VARCHAR(20) DEFAULT 'stdio',
                    command VARCHAR(200),
                    args JSONB DEFAULT '[]',
                    env_template JSONB DEFAULT '{}',
                    url VARCHAR(500),
                    icon VARCHAR(500),
                    version VARCHAR(50),
                    author VARCHAR(100),
                    docs_url VARCHAR(500),
                    install_command VARCHAR(500),
                    tools_cache JSONB,
                    tools_cached_at TIMESTAMPTZ,
                    builtin BOOLEAN DEFAULT FALSE,
                    template BOOLEAN DEFAULT FALSE,
                    source VARCHAR(50),
                    user_id TEXT,
                    scope VARCHAR(16) DEFAULT 'platform',
                    headers JSONB DEFAULT '{}',
                    enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            # 向后兼容：旧库可能没有 template/source/归属列，用 ALTER TABLE 兜底
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS template BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS source VARCHAR(50)")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS market_id TEXT")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS market_version TEXT NOT NULL DEFAULT ''")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mcp_services_market ON mcp_services(market_id) WHERE market_id IS NOT NULL")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_category ON mcp_services(category)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_template ON mcp_services(template)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_source ON mcp_services(source)")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS user_id TEXT")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS scope VARCHAR(16) DEFAULT 'platform'")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS headers JSONB DEFAULT '{}'::jsonb")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_user ON mcp_services(user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_scope ON mcp_services(scope)")
            # MCP Runtime: 版本化 + 安全审查 + 运行时状态字段（热加载、回滚依赖这些列）
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS kind VARCHAR(20) DEFAULT 'stdio'")  # custom | sse | stdio
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS expose VARCHAR(20) DEFAULT 'sse'")  # sse | none
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS active_version_id BIGINT")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS runtime_status VARCHAR(20) DEFAULT 'stopped'")  # stopped|loaded|error|restarting
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS runtime_last_error TEXT DEFAULT ''")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS runtime_restarts INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS runtime_last_ping TIMESTAMPTZ")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_kind ON mcp_services(kind)")

            # ── 部署形态与安装编排（Phase 2）──
            # deploy_scope 是「这个 MCP 的进程在哪里跑、谁能连它」，由 transport 客观推导，
            # 不让用户选（stdio 放服务端会操作错机器；remote 放会话是重复连同一端点）：
            #   server      = 全局远程（remote_mcp）。服务端接一次，agent + 所有编辑器共用。
            #   session     = 会话内 stdio。编辑器 CLI 自己在节点上拉起，仅该会话可见。
            #                 这类「安装」不是安装，只是让使用者的 MCP 选项里多一个可选项。
            #   node_hosted = 节点托管 stdio，再经节点隧道代理成 remote，从而全局可用。
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS deploy_scope VARCHAR(20) DEFAULT 'server'")
            # install_state 是「安装进度」，与 runtime_status（运行态）语义分开，不要混：
            # created→configuring→starting→testing→ready|error。session 形态不走此流程。
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS install_state VARCHAR(20) DEFAULT 'created'")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS install_error TEXT DEFAULT ''")
            # install_step：当前步骤的人话描述，前端直接显示（如「需补充环境变量: API_KEY」）
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS install_step VARCHAR(200) DEFAULT ''")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS configured_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS tested_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS ready_at TIMESTAMPTZ")
            # node_hosted 形态：托管该 stdio 的执行节点 + 节点本机端口（服务端分配，避免冲突）
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS host_node_id VARCHAR(100)")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS host_port INTEGER")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS host_pid INTEGER")
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS host_status VARCHAR(20) DEFAULT ''")  # starting|running|dead
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_deploy_scope ON mcp_services(deploy_scope)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_services_install_state ON mcp_services(install_state)")

            # 回填既有行：加列时默认值是 'server'，但 stdio 服务的进程本就在节点上由编辑器
            # CLI 拉起，落在 server 区会让资源页把它显示成「全局服务」并配一个永不推进的
            # 安装进度条。deploy_scope 是 transport 的客观属性，按 transport 回填一次即可。
            # host_node_id 非空的是 node_hosted，不要被覆盖回 session。
            await conn.execute(
                """
                UPDATE mcp_services SET deploy_scope='session'
                 WHERE transport='stdio'
                   AND COALESCE(host_node_id,'')=''
                   AND COALESCE(deploy_scope,'server')='server'
                """
            )
            # 同理回填安装态：session 形态没有安装过程，一律 ready；既有可用的 server 行
            # 停在 'created' 会被就绪门与前端当成「待安装」，也一并标 ready（它们在本次
            # 变更前就已在跑，安装状态机是新引入的概念，不能追溯判它们没装）。
            await conn.execute(
                """
                UPDATE mcp_services
                   SET install_state='ready',
                       install_step=CASE WHEN deploy_scope='session' THEN '随会话在节点启动' ELSE '' END
                 WHERE COALESCE(install_state,'created')='created'
                """
            )

            # ── mcp_plugin_versions 表：每次代码/配置变更 = 一个不可变版本 ──
            # custom 类存 Python 源码；sse/stdio 类把连接配置存到 config_json。
            # security_status 是热加载闸门：只有 passed 才允许 activate。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_plugin_versions (
                    id BIGSERIAL PRIMARY KEY,
                    service_id INTEGER NOT NULL REFERENCES mcp_services(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    code TEXT NOT NULL DEFAULT '',
                    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    author VARCHAR(100) DEFAULT '',
                    source VARCHAR(20) DEFAULT 'manual',   -- agent | main | manual
                    security_status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending|passed|failed|error
                    security_report JSONB NOT NULL DEFAULT '{}'::jsonb,
                    security_model VARCHAR(100) DEFAULT '',
                    security_checked_at TIMESTAMPTZ,
                    loaded BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(service_id, version)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_plugin_versions_service ON mcp_plugin_versions(service_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_plugin_versions_security ON mcp_plugin_versions(security_status)")

            # Runtime 配置迁移需要读取服务鉴权旧值；旧库先补齐该列。
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS auth_enabled BOOLEAN NOT NULL DEFAULT TRUE")
            # group_ids：内部 MCP 服务授权给哪些团队分组；外部 mcp_users token 不参与此授权。
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS group_ids JSONB NOT NULL DEFAULT '[]'::jsonb")

            # ── MCP Runtime 统一配置表 ──
            # 固定 envelope：config_type/instance_key 定位配置实例；公开字段与
            # secret 分库存放；token 只保留 hash/hint；revision 用于乐观并发与热更新。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_runtime_configs (
                    id BIGSERIAL PRIMARY KEY,
                    service_id INTEGER NOT NULL REFERENCES mcp_services(id) ON DELETE CASCADE,
                    config_type VARCHAR(100) NOT NULL,
                    instance_key TEXT NOT NULL,
                    data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    secret_data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    token_hash TEXT,
                    token_hint TEXT,
                    revision BIGINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(service_id, config_type, instance_key)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_runtime_configs_type ON mcp_runtime_configs(service_id, config_type, id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_runtime_configs_token_hash ON mcp_runtime_configs(token_hash) WHERE token_hash IS NOT NULL")

            # ── resource_mirrors：市场 skill/插件的服务端镜像（Phase 2）──
            #
            # 为什么要镜像：节点原本在会话启动时才 git clone 市场 skill（每次 RemoveAll
            # 后重拉、写 per-session 目录、无共享缓存、强依赖外网）。镜像后，下发给节点的
            # SkillSpec.url 指向我们服务端的 archive，于是同时拿到：预下载、跨会话共享缓存、
            # 不依赖外网、可鉴权。节点侧只需加一个 archive 下载分支。
            #
            # digest 既是变更检测依据，也是节点侧缓存键（<cache>/{module}/{id}@{digest}/）。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS resource_mirrors (
                    id BIGSERIAL PRIMARY KEY,
                    module VARCHAR(20) NOT NULL,          -- skills | plugins
                    market_id VARCHAR(200) NOT NULL,      -- 市场 item id，如 obra.superpowers
                    name VARCHAR(200) NOT NULL DEFAULT '',
                    version VARCHAR(50) NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',  -- 原始来源（git 仓库）
                    source_ref VARCHAR(100) NOT NULL DEFAULT '',
                    source_path TEXT NOT NULL DEFAULT '', -- 仓库内子路径（resource.path）
                    digest VARCHAR(128) NOT NULL DEFAULT '',
                    archive_path TEXT NOT NULL DEFAULT '',
                    size_bytes BIGINT NOT NULL DEFAULT 0,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending|downloading|ready|error
                    error TEXT NOT NULL DEFAULT '',
                    fetch_token VARCHAR(128) NOT NULL DEFAULT '',   -- 节点拉取凭据（走 SkillSpec.token）
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_fetched_at TIMESTAMPTZ,
                    UNIQUE(module, market_id, version)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_resource_mirrors_module ON resource_mirrors(module, status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_resource_mirrors_token ON resource_mirrors(fetch_token) WHERE fetch_token <> ''")

            # ── marketplace_leaderboard_items：外部榜单同步的候选池 ──
            # Agent-Leaderboard 这类外部目录只提供「仓库元数据」（stars/分类/描述），
            # 不是可安装清单。同步全量落这里当候选池，一律 status='draft'（用户不可见）；
            # 管理员在资源中心挑选、必要时补齐启动方式，再显式点发布转 'published'。
            # 没有任何自动发布路径：同步与 agent 补全都只写 draft。
            # installable=false 的条目（开发框架、研究程序、awesome 目录列表）同样可发布，
            # 但用户侧只渲染成「仅浏览」卡片（跳 GitHub，无安装入口）：它们有发现价值，
            # 只是四类（mcp/skill/prompt/plugin）里没有对应的安装形态。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_leaderboard_items (
                    id BIGSERIAL PRIMARY KEY,
                    source VARCHAR(40) NOT NULL DEFAULT 'agent-leaderboard',
                    board VARCHAR(30) NOT NULL,            -- skills|mcp|prompts|frameworks|research
                    repo_full_name VARCHAR(300) NOT NULL,  -- owner/repo，跨 board 唯一键的一半
                    repo_url TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    stars INTEGER NOT NULL DEFAULT 0,
                    forks INTEGER NOT NULL DEFAULT 0,
                    language VARCHAR(60) NOT NULL DEFAULT '',
                    topics JSONB NOT NULL DEFAULT '[]'::jsonb,
                    upstream_category VARCHAR(200) NOT NULL DEFAULT '',  -- AL 的正则分类，仅参考
                    use_cases JSONB NOT NULL DEFAULT '[]'::jsonb,
                    -- 归到我们四类的哪一类：mcp|skill|prompt|plugin，空=未定/仅浏览。
                    -- 注意：board 是「主题」，target_module 是「安装形态」，两者不等价。
                    target_module VARCHAR(20) NOT NULL DEFAULT '',
                    installable BOOLEAN NOT NULL DEFAULT true,
                    -- 运营标签：directory_list（awesome 目录）/needs_credentials（要凭据的
                    -- 真 server）/needs_launch_spec（启动方式待补）等，前端据此筛选与提示。
                    labels JSONB NOT NULL DEFAULT '[]'::jsonb,
                    -- agent 补全的 MCP 启动方式草稿（command/args/env 或 url/transport）。
                    -- 永远是草稿：写这里不改 status，发布仍需人工点击。
                    launch_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
                    launch_spec_status VARCHAR(20) NOT NULL DEFAULT '',  -- ''|pending|filled|failed
                    launch_spec_error TEXT NOT NULL DEFAULT '',
                    status VARCHAR(20) NOT NULL DEFAULT 'draft',        -- draft|published
                    published_at TIMESTAMPTZ,
                    published_by VARCHAR(100) NOT NULL DEFAULT '',
                    market_id VARCHAR(200) NOT NULL DEFAULT '',         -- 发布后对应的市场 item id
                    -- 名次：upstream_rank 是同步事实（条目在上游 board 文件里的位置，1=榜首）；
                    -- display_rank 是实际排序用的名次，默认跟随 upstream_rank。管理员手动
                    -- 定过名次（rank_overridden=true）后同步不再跟随上游，清空手动值即回落。
                    upstream_rank INTEGER,
                    display_rank INTEGER,
                    rank_overridden BOOLEAN NOT NULL DEFAULT false,
                    upstream_updated_at TIMESTAMPTZ,
                    first_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(source, board, repo_full_name)
                )
            """)
            # 管理员对上游展示字段的覆盖值（description/language/topics/use_cases/
            # upstream_category）。单独一列而不是直接改原列：同步的 ON CONFLICT 会刷新
            # 原列，改原列会被下次同步静默冲掉。读取时覆盖值盖在原值上，于是「管理员
            # 改过的就一直是改过的，没改的继续跟着上游更新」。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS admin_overrides JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            # 名次三列（默认=上游名次，管理员可手动固定），存量表走 ADD COLUMN IF NOT EXISTS。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS upstream_rank INTEGER, "
                "ADD COLUMN IF NOT EXISTS display_rank INTEGER, "
                "ADD COLUMN IF NOT EXISTS rank_overridden BOOLEAN NOT NULL DEFAULT false"
            )
            # 分类多选（target_modules）：一条榜单项可以同时是 MCP+Skill+插件——
            # target_module 保留为「主分类」（取数组第一个，兼容旧查询），数组才是真相。
            # 回填只在数组为空时进行，管理员改过（数组非空）的行不再动。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS target_modules JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
            await conn.execute(
                "UPDATE marketplace_leaderboard_items "
                "SET target_modules = to_jsonb(ARRAY[target_module]) "
                "WHERE target_module <> '' AND target_modules = '[]'::jsonb"
            )
            # 各分类的安装配置（install_spec）：skill={install_method,path,ref}、
            # plugin={download_url,provider}、prompt={content,providers}。与 launch_spec
            #（MCP 专用）同级的人工草稿字段，同步永不覆盖，发布前由管理员核对。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS install_spec JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            # 外部数据快照：同步时落的上游 repo 原始事实（board 原文/category/topics/
            # 语言/更新时间/stars/forks 等），只读展示用——与资源字段（可编辑）分离，
            # 同步刷新它而不碰管理员改过的资源字段。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS external_data JSONB NOT NULL DEFAULT '{}'::jsonb"
            )
            # 资源字段补齐（与市场资源同构，编辑弹框两套合一）：
            # - categories 子分类（多选，源=上游 use_cases）
            # - tags 标签（多选，源=上游 topics）
            # - name/version 资源名称与版本（version 同步按上游更新日期生成）
            # - sort_order 排序（资源中心索引；默认=上游榜单位置，管理员可改）
            # 存量行回填：categories<-use_cases、tags<-topics、sort_order<-display_rank。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS categories JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '', "
                "ADD COLUMN IF NOT EXISTS version TEXT NOT NULL DEFAULT '', "
                "ADD COLUMN IF NOT EXISTS sort_order INTEGER"
            )
            await conn.execute(
                "UPDATE marketplace_leaderboard_items "
                "SET categories = use_cases, tags = topics, "
                "    sort_order = COALESCE(display_rank, upstream_rank) "
                "WHERE (categories = '[]'::jsonb AND use_cases <> '[]'::jsonb) "
                "   OR (tags = '[]'::jsonb AND topics <> '[]'::jsonb) "
                "   OR sort_order IS NULL"
            )
            # 扩展 upstream_category 字段长度到 200（topics 空格拼接可能超过 60）
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ALTER COLUMN upstream_category TYPE VARCHAR(200)"
            )
            # 技术栈识别（规则引擎，与 mc_projects.stack_profile 同源）：
            # - stack 完整 profile JSONB（探针 attach_probe 写入）
            # - stack_tags 扁平小写数组（主语言+框架+形态），专供 @> 过滤，
            #   与完整 profile 分离避免大 JSONB 扫描。空数组 = 未扫/未识别。
            await conn.execute(
                "ALTER TABLE marketplace_leaderboard_items "
                "ADD COLUMN IF NOT EXISTS stack JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "ADD COLUMN IF NOT EXISTS stack_tags JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mlb_stack_tags "
                "ON marketplace_leaderboard_items USING GIN (stack_tags jsonb_path_ops)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mlb_sort ON marketplace_leaderboard_items(sort_order)"
            )
            # 索引必须建在上面的 ADD COLUMN 之后：CREATE TABLE IF NOT EXISTS 对存量表整条
            # 跳过，新列只由 ALTER 补上，索引先建会撞 UndefinedColumnError。
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mlb_status ON marketplace_leaderboard_items(status, board)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mlb_stars ON marketplace_leaderboard_items(stars DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mlb_rank ON marketplace_leaderboard_items(display_rank)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mlb_repo ON marketplace_leaderboard_items(repo_full_name)")

            # ── marketplace_items / marketplace_publish_jobs：市场条目编辑真相源 + 发布 outbox ──
            # 市场管理页的 CRUD 不再同步写 GitHub（每次 write = GET sha + PUT，经代理
            # 单次 RTT 1-3s，保存一条模板要 7-9 次串行 RTT），改为事务写 PG 立即返回，
            # 由后台 publisher 消费 publish_jobs 把当前 store 状态异步镜像到 GitHub
            # （外部消费者仍读仓库 raw）。manifest 是唯一真相，summary 是渲染好的
            # index 行（validator.index_summary），publisher 只做投影不做业务决策。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_items (
                    id BIGSERIAL PRIMARY KEY,
                    module TEXT NOT NULL,
                    item_id TEXT NOT NULL,          -- manifest id 原文（含 '/'）
                    manifest JSONB NOT NULL,        -- 完整 manifest（唯一真相）
                    summary JSONB NOT NULL DEFAULT '{}'::jsonb,  -- index 行，写入时算好
                    status TEXT NOT NULL DEFAULT 'published',   -- 与 manifest.status 同步
                    revision BIGINT NOT NULL DEFAULT 1,         -- 每次写 +1，审计用
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(module, item_id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_items_module ON marketplace_items(module, status)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_publish_jobs (
                    id BIGSERIAL PRIMARY KEY,
                    module TEXT NOT NULL,
                    item_id TEXT NOT NULL,          -- refresh（仅重渲 index）用 '*'
                    action TEXT NOT NULL,           -- upsert | delete | refresh
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb, -- hard delete 时存删除前 manifest（资产清理用）
                    status TEXT NOT NULL DEFAULT 'pending',      -- pending | pushing | done | failed
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 退避重试用
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mkp_jobs_status ON marketplace_publish_jobs(status, available_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mkp_jobs_module ON marketplace_publish_jobs(module, status)")
            # 同条目的 pending dirty 标记只留一行；ON CONFLICT inference 用同一谓词原子
            # coalesce，避免多实例同时保存时各插一条。done/failed 历史不受影响。
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_mkp_jobs_pending_item "
                "ON marketplace_publish_jobs(module, item_id) WHERE status='pending'"
            )

            # ── migration-only legacy tables ──
            # Retained during the observation period solely as idempotent migration
            # sources. Runtime and admin business paths must never read or write them.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_service_env_vars (
                    id SERIAL PRIMARY KEY,
                    service_id INTEGER NOT NULL REFERENCES mcp_services(id) ON DELETE CASCADE,
                    key VARCHAR(100) NOT NULL,
                    value TEXT NOT NULL DEFAULT '',
                    secret BOOLEAN NOT NULL DEFAULT FALSE,
                    description TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(service_id, key)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_service_env_vars_service ON mcp_service_env_vars(service_id)")

            # migration-only: legacy mail tables retained as idempotent sources.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_mail_configs (
                    id SERIAL PRIMARY KEY,
                    service_id INTEGER NOT NULL REFERENCES mcp_services(id) ON DELETE CASCADE,
                    display_name VARCHAR(200) NOT NULL,
                    username VARCHAR(320) NOT NULL,
                    password TEXT NOT NULL DEFAULT '',
                    base_url VARCHAR(500) NOT NULL,
                    secret_key TEXT NOT NULL DEFAULT '',
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_mail_configs_service ON mcp_mail_configs(service_id)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_mail_addresses (
                    id SERIAL PRIMARY KEY,
                    mail_config_id INTEGER NOT NULL REFERENCES mcp_mail_configs(id) ON DELETE CASCADE,
                    address VARCHAR(320) NOT NULL,
                    source_address VARCHAR(320) NOT NULL,
                    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(mail_config_id, address)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_mail_addresses_lookup ON mcp_mail_addresses(lower(address))")

            await conn.execute("CREATE TABLE IF NOT EXISTS _mcp_migration_flags(key TEXT PRIMARY KEY)")
            _runtime_config_migrated = await conn.fetchval(
                "SELECT key FROM _mcp_migration_flags WHERE key='runtime_configs_envelope_v1'"
            )
            if not _runtime_config_migrated:
                await conn.execute("""
                    INSERT INTO mcp_runtime_configs(service_id, config_type, instance_key, data, secret_data, created_at, updated_at)
                    SELECT service_id, 'env_var', key,
                           jsonb_build_object('key', key, 'secret', secret, 'description', description),
                           jsonb_build_object('value', value), created_at, updated_at
                    FROM mcp_service_env_vars
                    ON CONFLICT(service_id, config_type, instance_key) DO NOTHING
                """)
                await conn.execute("""
                    INSERT INTO mcp_runtime_configs(service_id, config_type, instance_key, data, secret_data, created_at, updated_at)
                    SELECT service_id, 'mail_account', id::text,
                           jsonb_build_object('display_name', display_name, 'username', username,
                                              'base_url', base_url, 'enabled', enabled),
                           jsonb_build_object('password', password, 'secret_key', secret_key),
                           created_at, updated_at
                    FROM mcp_mail_configs
                    ON CONFLICT(service_id, config_type, instance_key) DO NOTHING
                """)
                await conn.execute("""
                    INSERT INTO mcp_runtime_configs(service_id, config_type, instance_key, data, created_at, updated_at)
                    SELECT c.service_id, 'mail_address', a.id::text,
                           jsonb_build_object('account_instance_key', c.id::text, 'address', a.address,
                                              'source_address', a.source_address, 'is_primary', a.is_primary),
                           a.created_at, a.updated_at
                    FROM mcp_mail_addresses a JOIN mcp_mail_configs c ON c.id=a.mail_config_id
                    ON CONFLICT(service_id, config_type, instance_key) DO NOTHING
                """)
                await conn.execute("""
                    INSERT INTO mcp_runtime_configs(service_id, config_type, instance_key, data)
                    SELECT id, 'cdp_driver', 'singleton',
                           jsonb_build_object('external_ws', true)
                    FROM mcp_services WHERE name='cdp-bridge'
                    ON CONFLICT(service_id, config_type, instance_key) DO NOTHING
                """)
                await conn.execute(
                    "INSERT INTO _mcp_migration_flags(key) VALUES('runtime_configs_envelope_v1') ON CONFLICT DO NOTHING"
                )

            # v2 repairs the first unified-envelope migration without reusing its flag.
            # It is idempotent and runs after identity/authorization tables exist below.

            # ── MCP 服务鉴权：服务级开关 + MCP 用户体系 ──
            # auth_enabled 是独立开关；新建服务默认开启，连接 MCP 时需携带用户 token。
            await conn.execute("ALTER TABLE mcp_services ADD COLUMN IF NOT EXISTS auth_enabled BOOLEAN NOT NULL DEFAULT TRUE")

            # mcp_users：一个用户 = 一个密钥 token 持有者。token 系统生成、可手动改/重置。
            # 对 cdp-bridge 而言，token 同时是浏览器会话池的隔离键。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    token VARCHAR(200) NOT NULL UNIQUE,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    description TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_users_token ON mcp_users(token)")
            # principal 明文只在创建/轮换时返回一次；旧 token 列仅作迁移兼容，允许为空。
            await conn.execute("ALTER TABLE mcp_users ALTER COLUMN token DROP NOT NULL")
            # agents 在 mcp_users 之前建表，故外键在此处补建；DO 块保证重启幂等。
            await conn.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'fk_agents_mcp_user'
                    ) THEN
                        ALTER TABLE agents
                        ADD CONSTRAINT fk_agents_mcp_user
                        FOREIGN KEY (mcp_user_id) REFERENCES mcp_users(id) ON DELETE SET NULL;
                    END IF;
                END $$;
            """)
            await conn.execute("""
                DO $$ BEGIN
                    IF to_regclass('public.mc_tasks') IS NOT NULL THEN
                        ALTER TABLE mc_tasks ADD COLUMN IF NOT EXISTS mcp_user_id INTEGER;
                        CREATE INDEX IF NOT EXISTS idx_mc_tasks_mcp_user ON mc_tasks(mcp_user_id);
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'fk_mc_tasks_mcp_user'
                        ) THEN
                            ALTER TABLE mc_tasks
                            ADD CONSTRAINT fk_mc_tasks_mcp_user
                            FOREIGN KEY (mcp_user_id) REFERENCES mcp_users(id) ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """)
            # chat_enabled：该客户端（token 持有者）是否允许在网页侧打开 agent 对话面板。
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS chat_enabled BOOLEAN NOT NULL DEFAULT FALSE")

            # mcp_service_users：服务 ↔ 用户 多对多授权表。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_service_users (
                    id SERIAL PRIMARY KEY,
                    service_id INTEGER NOT NULL REFERENCES mcp_services(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES mcp_users(id) ON DELETE CASCADE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(service_id, user_id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_service_users_service ON mcp_service_users(service_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_service_users_user ON mcp_service_users(user_id)")

            _runtime_config_v2 = await conn.fetchval(
                "SELECT key FROM _mcp_migration_flags WHERE key='runtime_configs_envelope_v2'"
            )
            if not _runtime_config_v2:
                async with conn.transaction():
                    await conn.execute("""
                        UPDATE mcp_runtime_configs
                        SET data=(data - 'account_instance_key') || jsonb_build_object(
                            'parent_instance_key', COALESCE(data->>'parent_instance_key', data->>'account_instance_key')
                        )
                        WHERE config_type='mail_address'
                          AND COALESCE(data->>'parent_instance_key', data->>'account_instance_key') IS NOT NULL
                    """)
                    await conn.execute("""
                        UPDATE mcp_runtime_configs
                        SET data=data || jsonb_build_object('value', secret_data->'value'),
                            secret_data=secret_data - 'value'
                        WHERE config_type='env_var' AND COALESCE((data->>'secret')::boolean, false)=false
                          AND secret_data ? 'value'
                    """)
                    await conn.execute("UPDATE mcp_services SET auth_enabled=TRUE WHERE name='cdp-bridge'")
                    await conn.execute("""
                        UPDATE mcp_runtime_configs c
                        SET data=c.data || jsonb_build_object('enabled', false),
                            token_hash=NULL, token_hint=NULL, revision=revision+1, updated_at=now()
                        WHERE c.config_type='cdp_client' AND NOT EXISTS (
                            SELECT 1 FROM mcp_users u
                            JOIN mcp_service_users su ON su.user_id=u.id AND su.service_id=c.service_id
                            WHERE u.enabled=TRUE AND u.id::text=c.data->>'user_id'
                        )
                    """)
                    await conn.execute(
                        "INSERT INTO _mcp_migration_flags(key) VALUES('runtime_configs_envelope_v2') ON CONFLICT DO NOTHING"
                    )

            # migration-only: legacy session-grant DDL is retained during the
            # observation period. Session grants/borrowing are not migrated into
            # runtime configuration and no business path may read or write this table.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_cdp_session_grants (
                    id SERIAL PRIMARY KEY,
                    service_id INTEGER NOT NULL REFERENCES mcp_services(id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL REFERENCES mcp_users(id) ON DELETE CASCADE,
                    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    granted_by TEXT NOT NULL DEFAULT '',
                    UNIQUE(service_id, session_id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_cdp_session_grants_service ON mcp_cdp_session_grants(service_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_cdp_session_grants_user ON mcp_cdp_session_grants(user_id)")

            # 一次性把存量 MCP 服务也刷成开启鉴权（token 成为必填连接凭证）。
            # 用 _mcp_migration_flags 守卫，只跑一次，不覆盖管理员后续手动关闭鉴权的选择。
            await conn.execute("CREATE TABLE IF NOT EXISTS _mcp_migration_flags(key TEXT PRIMARY KEY)")
            _auth_backfilled = await conn.fetchval(
                "SELECT key FROM _mcp_migration_flags WHERE key='auth_default_enabled'"
            )
            if not _auth_backfilled:
                await conn.execute("UPDATE mcp_services SET auth_enabled=TRUE WHERE auth_enabled=FALSE")
                await conn.execute("INSERT INTO _mcp_migration_flags(key) VALUES('auth_default_enabled')")

            # ── MCP principal（mcp_users）token 哈希化与归属 ──
            # mcp_users 升级为 canonical MCP principal：保留 plaintext token 做过渡，
            # 运行时以 token_hash 为准；owner_user_id 标识所属平台用户（C 端用户 UUID）。
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS token_hash TEXT")
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS token_hint TEXT")
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS token_status VARCHAR(16) NOT NULL DEFAULT 'active'")
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS owner_user_id TEXT")
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_mcp_users_token_hash "
                "ON mcp_users(token_hash) WHERE token_hash IS NOT NULL"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_users_owner ON mcp_users(owner_user_id)")
            # usage_type 区分 principal 用途：agent / external / task。鉴权口径三者一致，
            # 该字段只影响创建入口与 token 展示规则；不参与运行时判权。
            await conn.execute("ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS usage_type VARCHAR(16) NOT NULL DEFAULT 'external'")
            await conn.execute(
                "UPDATE mcp_users SET usage_type='external' WHERE usage_type IS NULL OR usage_type NOT IN ('agent','external','task')"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_users_usage_type ON mcp_users(usage_type)")
            # 旧管理端 principals 仍全局唯一；用户自有 principals 只需在 owner 内唯一。
            await conn.execute("ALTER TABLE mcp_users DROP CONSTRAINT IF EXISTS mcp_users_name_key")
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_mcp_users_legacy_name "
                "ON mcp_users(name) WHERE owner_user_id IS NULL"
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_mcp_users_owner_name "
                "ON mcp_users(owner_user_id, name) WHERE owner_user_id IS NOT NULL"
            )

            # 一次性把存量 plaintext token 转 hash（sha256），token_hint 存前 8 位 + 后 4 位。
            # 在应用层计算，避免启动迁移依赖 PostgreSQL 可选的 pgcrypto 扩展。
            # 过渡期 token 列仍保留，运行时 resolve 优先查 hash；flag 保证只跑一次。
            _principal_hashed = await conn.fetchval(
                "SELECT key FROM _mcp_migration_flags WHERE key='mcp_users_token_hashed'"
            )
            if not _principal_hashed:
                await cls._migrate_mcp_user_token_hashes(conn)

            # ── MCP principal 操作参数表 ──
            # principal（mcp_users）自带它要操作的资源参数：cdp_client_id / mail_account_id /
            # term_id 等。driver 鉴权时直接从 principal 读参数定位资源，不再用 grant 树。
            # 一个 principal 一对多参数；同一 key 只绑一个值（UNIQUE principal_id+param_key）。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_user_params (
                    id BIGSERIAL PRIMARY KEY,
                    principal_id INTEGER NOT NULL REFERENCES mcp_users(id) ON DELETE CASCADE,
                    param_key VARCHAR(64) NOT NULL,
                    param_value VARCHAR(128) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (principal_id, param_key)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_mcp_user_params_principal ON mcp_user_params(principal_id)")

            # 把旧 grant 数据投影成参数行（一次性，幂等）。仅 builtin_instance 的子项
            # 选择有可靠来源：selected 模式的 children 投影成对应 param；all 模式展开成该
            # instance 下全部 enabled 的 client/account。service 类 grant 已投影到
            # mcp_service_users，不进 params。
            _mcp_user_params_v1 = await conn.fetchval(
                "SELECT key FROM _mcp_migration_flags WHERE key='mcp_user_params_v1'"
            )
            if not _mcp_user_params_v1:
                old_grants_exist = await conn.fetchval(
                    "SELECT to_regclass('public.mcp_principal_grants')"
                )
                if old_grants_exist is not None:
                    # cdp_client 子项
                    await conn.execute("""
                        INSERT INTO mcp_user_params(principal_id, param_key, param_value)
                        SELECT g.principal_id, 'cdp_client_id', gc.child_id::text
                        FROM mcp_principal_grants g
                        JOIN mcp_principal_grant_children gc ON gc.grant_id=g.id
                        JOIN builtin_tool_instances i ON i.id=g.resource_id
                        WHERE g.resource_kind='builtin_instance' AND g.enabled=TRUE
                          AND g.child_mode='selected' AND gc.child_kind='cdp_client'
                          AND i.tool_kind='cdp'
                        ON CONFLICT (principal_id, param_key) DO NOTHING
                    """)
                    # mail_account 子项
                    await conn.execute("""
                        INSERT INTO mcp_user_params(principal_id, param_key, param_value)
                        SELECT g.principal_id, 'mail_account_id', gc.child_id::text
                        FROM mcp_principal_grants g
                        JOIN mcp_principal_grant_children gc ON gc.grant_id=g.id
                        JOIN builtin_tool_instances i ON i.id=g.resource_id
                        WHERE g.resource_kind='builtin_instance' AND g.enabled=TRUE
                          AND g.child_mode='selected' AND gc.child_kind='mail_account'
                          AND i.tool_kind='mail'
                        ON CONFLICT (principal_id, param_key) DO NOTHING
                    """)
                    # device 子项（device-control 设备明细）
                    await conn.execute("""
                        INSERT INTO mcp_user_params(principal_id, param_key, param_value)
                        SELECT g.principal_id, 'device_id', gc.child_id::text
                        FROM mcp_principal_grants g
                        JOIN mcp_principal_grant_children gc ON gc.grant_id=g.id
                        JOIN builtin_tool_instances i ON i.id=g.resource_id
                        WHERE g.resource_kind='builtin_instance' AND g.enabled=TRUE
                          AND g.child_mode='selected' AND gc.child_kind='device'
                          AND i.tool_kind='device'
                        ON CONFLICT (principal_id, param_key) DO NOTHING
                    """)
                    # child_mode=all：展开成该 instance 下全部 enabled 的 client/account
                    await conn.execute("""
                        INSERT INTO mcp_user_params(principal_id, param_key, param_value)
                        SELECT g.principal_id, 'cdp_client_id', d.id::text
                        FROM mcp_principal_grants g
                        JOIN builtin_tool_instances i ON i.id=g.resource_id
                        JOIN builtin_tool_details d ON d.instance_id=i.id
                        WHERE g.resource_kind='builtin_instance' AND g.enabled=TRUE
                          AND g.child_mode='all' AND i.tool_kind='cdp'
                          AND d.detail_type='cdp_client'
                          AND COALESCE((d.data->>'enabled')::boolean, TRUE)=TRUE
                        ON CONFLICT (principal_id, param_key) DO NOTHING
                    """)
                    await conn.execute("""
                        INSERT INTO mcp_user_params(principal_id, param_key, param_value)
                        SELECT g.principal_id, 'mail_account_id', d.id::text
                        FROM mcp_principal_grants g
                        JOIN builtin_tool_instances i ON i.id=g.resource_id
                        JOIN builtin_tool_details d ON d.instance_id=i.id
                        WHERE g.resource_kind='builtin_instance' AND g.enabled=TRUE
                          AND g.child_mode='all' AND i.tool_kind='mail'
                          AND d.detail_type='mail_account'
                          AND COALESCE((d.data->>'enabled')::boolean, TRUE)=TRUE
                        ON CONFLICT (principal_id, param_key) DO NOTHING
                    """)
                    # device-control child_mode=all：展开成该 instance 下全部 enabled 的设备明细
                    await conn.execute("""
                        INSERT INTO mcp_user_params(principal_id, param_key, param_value)
                        SELECT g.principal_id, 'device_id', d.id::text
                        FROM mcp_principal_grants g
                        JOIN builtin_tool_instances i ON i.id=g.resource_id
                        JOIN builtin_tool_details d ON d.instance_id=i.id
                        WHERE g.resource_kind='builtin_instance' AND g.enabled=TRUE
                          AND g.child_mode='all' AND i.tool_kind='device'
                          AND d.detail_type='device'
                          AND COALESCE((d.data->>'enabled')::boolean, TRUE)=TRUE
                        ON CONFLICT (principal_id, param_key) DO NOTHING
                    """)
                await conn.execute(
                    "INSERT INTO _mcp_migration_flags(key) VALUES('mcp_user_params_v1') ON CONFLICT DO NOTHING"
                )

            # 旧 grant 表数据已迁移到 mcp_user_params（params）+ mcp_service_users（service 级），
            # 旧表删除。新建库不会再建这两张表。
            await conn.execute("DROP TABLE IF EXISTS mcp_principal_grant_children")
            await conn.execute("DROP TABLE IF EXISTS mcp_principal_grants")


            # ── agent_goal_states 表 ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_goal_states (
                    id SERIAL PRIMARY KEY,
                    agent_id INT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                    mode VARCHAR(20) NOT NULL,
                    objective TEXT,
                    budget_seconds INT,
                    start_time TIMESTAMPTZ,
                    turns_used INT DEFAULT 0,
                    max_turns INT DEFAULT 50,
                    status VARCHAR(20) DEFAULT 'running',
                    state JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_goal_states_agent ON agent_goal_states(agent_id)")

            # ── agent_subagent_messages 表 ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_subagent_messages (
                    id BIGSERIAL PRIMARY KEY,
                    parent_task_id UUID NOT NULL,
                    subagent_task_id UUID NOT NULL,
                    direction VARCHAR(10) NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    content TEXT NOT NULL,
                    consumed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_subagent_msg_lookup "
                "ON agent_subagent_messages(parent_task_id, subagent_task_id, direction, consumed)"
            )

            # ── agent_llm_configs 表（agent 服务专用 LLM 配置，独立于主系统渠道） ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_llm_configs (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    base_url VARCHAR(500) NOT NULL,
                    chat_path VARCHAR(200) DEFAULT '/chat/completions',
                    api_key VARCHAR(500) NOT NULL,
                    models JSONB DEFAULT '[]',
                    protocol VARCHAR(20) DEFAULT 'openai',
                    enabled BOOLEAN DEFAULT TRUE,
                    timeout_seconds INT DEFAULT 600,
                    max_retries INT DEFAULT 2,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_llm_configs_enabled ON agent_llm_configs(enabled)")
            # 兼容已建库：补 agent_llm_configs 的超时/重试列（幂等）
            await conn.execute("ALTER TABLE agent_llm_configs ADD COLUMN IF NOT EXISTS timeout_seconds INT DEFAULT 600")
            await conn.execute("ALTER TABLE agent_llm_configs ADD COLUMN IF NOT EXISTS max_retries INT DEFAULT 2")

            # ── agent_llm_models 表（LLM 配置下的模型库存） ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_llm_models (
                    id SERIAL PRIMARY KEY,
                    llm_config_id INT NOT NULL REFERENCES agent_llm_configs(id) ON DELETE CASCADE,
                    model_name VARCHAR(200) NOT NULL,
                    display_name VARCHAR(200),
                    description TEXT DEFAULT '',
                    enabled BOOLEAN DEFAULT TRUE,
                    sort_order INT DEFAULT 0,
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(llm_config_id, model_name)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_llm_models_config ON agent_llm_models(llm_config_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_llm_models_enabled ON agent_llm_models(enabled)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_llm_models_lookup ON agent_llm_models(llm_config_id, model_name)")

            # agents 表关联 LLM 配置（可空：空表示用默认 LLM 配置）
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS llm_config_id INT REFERENCES agent_llm_configs(id) ON DELETE SET NULL")
            # agent 级思考模式（独立于主系统 reasoning 配置，随 Agent 走）
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS thinking_enabled BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS reasoning_effort VARCHAR(20) DEFAULT ''")
            # agent 层 429 自动重试次数（仅对瞬时可重试业务码退避重试；配置类
            # no_available_account 不重试）。0=关闭。由 agent_loop 读 GatewayLLMBridge.max_retries。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS llm_retry_429 INT DEFAULT 2")
            # 审批等待时长（秒）：命中需确认的工具后，本轮最多挂起这么久等人裁决，
            # 超时中止本轮（不再自动拒绝后继续喂给模型）。0=不过期，一直等。
            # 默认 24h：挂起会占住 asyncio task + SSE 连接 + 绑定节点的 PTY，
            # 给足离开屏幕的时间，但不做成字面无限。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS approval_timeout_seconds INT DEFAULT 86400")
            # 浏览器（CDP 网页对话）能否执行 code_run：Agent 级开关，默认关闭。
            # 关着时 CDP 路径不挂审批协调器，code_run 直接 denied 并提示开启本配置；
            # 用户端 Agent 聊天页不受此开关影响（照旧走审批流程）。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS browser_code_run_enabled BOOLEAN DEFAULT FALSE")

            # ── Agent LLM 走网关秘钥（计费/限流/归属闭环） ──
            # 主/子 Agent 各自绑定一个网关 api_keys.id + 模型名，运行时经 dispatch_entry
            # 主链路调用（而非独立上游），使 token/TPM/归属可控。旧列 api_key/model/
            # llm_config_id 及 agent_llm_* 三表保留但停用，新链路只读以下列。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS main_api_key_id INTEGER")
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS main_model VARCHAR(200)")
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS subagent_api_key_id INTEGER")
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS subagent_model VARCHAR(200)")
            # 定时任务默认模型：无人值守场景往往要跟交互态用不同的模型（省钱/长稳），
            # 且不该借用 subagent_* —— 那是「主 agent 派生的并行子任务」的语义。留空则
            # 回退主 Agent，行为与改动前一致。任务级 api_key_id/model 再覆盖这一层。
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS scheduled_api_key_id INTEGER")
            await conn.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS scheduled_model VARCHAR(200)")

            # ── agent_llm_role_mappings 表（Agent 场景/角色 -> LLM 模型库存） ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_llm_role_mappings (
                    id SERIAL PRIMARY KEY,
                    agent_id INT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                    role VARCHAR(50) NOT NULL,
                    llm_model_id INT REFERENCES agent_llm_models(id) ON DELETE SET NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(agent_id, role),
                    CHECK (role IN ('main_agent', 'subagent'))
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_llm_role_mappings_agent ON agent_llm_role_mappings(agent_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_llm_role_mappings_model ON agent_llm_role_mappings(llm_model_id)")

            # 从旧 agent_llm_configs.models JSONB 列回填模型库存（幂等）
            await conn.execute("""
                INSERT INTO agent_llm_models(llm_config_id, model_name, display_name, sort_order)
                SELECT c.id, model_name, model_name, ordinality::int - 1
                FROM agent_llm_configs c
                CROSS JOIN LATERAL jsonb_array_elements_text(
                    CASE WHEN jsonb_typeof(c.models) = 'array' THEN c.models ELSE '[]'::jsonb END
                ) WITH ORDINALITY AS m(model_name, ordinality)
                WHERE model_name <> ''
                ON CONFLICT (llm_config_id, model_name) DO NOTHING
            """)

            # 从旧 agents.llm_config_id + agents.model 回填 main_agent 映射（幂等）
            await conn.execute("""
                INSERT INTO agent_llm_models(llm_config_id, model_name, display_name)
                SELECT a.llm_config_id, a.model, a.model
                FROM agents a
                WHERE a.llm_config_id IS NOT NULL AND COALESCE(a.model, '') <> ''
                ON CONFLICT (llm_config_id, model_name) DO NOTHING
            """)
            await conn.execute("""
                INSERT INTO agent_llm_role_mappings(agent_id, role, llm_model_id)
                SELECT a.id, 'main_agent', m.id
                FROM agents a
                JOIN agent_llm_models m ON m.llm_config_id = a.llm_config_id AND m.model_name = a.model
                WHERE a.llm_config_id IS NOT NULL AND COALESCE(a.model, '') <> ''
                ON CONFLICT (agent_id, role) DO NOTHING
            """)

            # ── 现有表增加 agent_id 列（幂等迁移） ──
            # 使用 DO $$ 块确保只在列不存在时才添加
            tables_needing_agent_id = [
                "agent_insight_index",
                "agent_global_facts",
                "agent_skills",
                "agent_session_archives",
                "agent_tasks",
                "agent_scheduled_tasks",
            ]
            for table in tables_needing_agent_id:
                await conn.execute(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='{table}' AND column_name='agent_id'
                        ) THEN
                            ALTER TABLE {table} ADD COLUMN agent_id INT REFERENCES agents(id) ON DELETE CASCADE;
                        END IF;
                    END $$;
                """)

            # ── 默认 Agent 已废弃 ──
            # 不再启动时种 id=1 的 default agent：它 user_id 留空会被当作平台 Agent
            # 全员共享，与多用户隔离冲突。历史 id=1 的归属由 migrate_orphan_agents_owner
            # 迁移到配置的孤儿 owner。保留 setval 对齐序列。
            # 对齐序列，避免新建 Agent 时拿到已占用的 id=1
            await conn.execute(
                "SELECT setval(pg_get_serial_sequence('agents','id'), "
                "GREATEST((SELECT COALESCE(MAX(id),0) FROM agents), 1))"
            )

            # ── 迁移现有数据到默认 Agent ──
            for table in tables_needing_agent_id:
                await conn.execute(f"UPDATE {table} SET agent_id=1 WHERE agent_id IS NULL")

            # ── 组合唯一约束（替代原单列唯一） ──
            # agent_insight_index: (agent_id, key) 替代 (key)
            exists = await conn.fetchval("""
                SELECT 1 FROM pg_indexes WHERE indexname='idx_insight_agent_key'
            """)
            if not exists:
                # 先删除原 UNIQUE(key) 约束（如果存在）
                try:
                    await conn.execute("ALTER TABLE agent_insight_index DROP CONSTRAINT IF EXISTS agent_insight_index_key_key")
                except Exception:
                    pass
                await conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_insight_agent_key ON agent_insight_index(agent_id, key)"
                )

            # agent_global_facts: (agent_id, fact_key) 替代 (fact_key)
            exists = await conn.fetchval("""
                SELECT 1 FROM pg_indexes WHERE indexname='idx_facts_agent_key'
            """)
            if not exists:
                try:
                    await conn.execute("ALTER TABLE agent_global_facts DROP CONSTRAINT IF EXISTS agent_global_facts_fact_key_key")
                except Exception:
                    pass
                await conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_agent_key ON agent_global_facts(agent_id, fact_key)"
                )

            # agent_skills: (agent_id, name) unique
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_skills_agent_name ON agent_skills(agent_id, name)"
            )

            # ── 补充 agent_id 索引 ──
            agent_id_indexes = [
                ("idx_insight_agent", "agent_insight_index(agent_id)"),
                ("idx_facts_agent", "agent_global_facts(agent_id)"),
                ("idx_skills_agent", "agent_skills(agent_id)"),
                ("idx_archives_agent", "agent_session_archives(agent_id)"),
                ("idx_tasks_agent", "agent_tasks(agent_id)"),
                ("idx_scheduled_agent", "agent_scheduled_tasks(agent_id)"),
            ]
            for idx_name, idx_def in agent_id_indexes:
                await conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")

            # ── 插入系统内置 MCP 服务（vendored in-tree，不可删除） ──
            # cdp-bridge / mail 的源码已内置在 mcp_builtin，由 mcp_runtime 直接加载，
            # 不再依赖 uvx 联网拉取，因此 command/install_command 留空，kind='builtin'。
            # 服务名单与元数据的唯一声明源是 mcp_builtin.catalog（registry 注册 adapter
            # 时消费同一份），这里只投影出需要落库的持久化服务，避免两处手写漂移。
            # catalog 是纯声明模块（无驱动依赖），DB 初始化导入它是安全的。
            from mcp_builtin.catalog import PERSISTED_BUILTIN_SERVICE_SPECS

            builtin_services = [
                (
                    spec.name, spec.display_name, spec.description,
                    spec.category, spec.version, spec.author, spec.docs_url,
                )
                for spec in PERSISTED_BUILTIN_SERVICE_SPECS
            ]
            for name, display_name, description, category, version, author, docs_url in builtin_services:
                await conn.execute("""
                    INSERT INTO mcp_services(name, display_name, description, category, transport, command, builtin, template, source, version, author, install_command, docs_url, kind)
                    VALUES($1, $2, $3, $4, 'sse', '', true, false, 'system', $5, $6, '', $7, 'builtin')
                    ON CONFLICT(name) DO UPDATE SET
                        display_name=EXCLUDED.display_name,
                        description=EXCLUDED.description,
                        category=EXCLUDED.category,
                        transport='sse',
                        builtin=TRUE,
                        template=FALSE,
                        source='system',
                        kind='builtin',
                        command='',
                        install_command='',
                        version=EXCLUDED.version,
                        author=EXCLUDED.author,
                        docs_url=EXCLUDED.docs_url,
                        updated_at=now()
                """, name, display_name, description, category, version, author, docs_url)

            # ── 插入 MCP 市场精选目录（template=true，仅作为安装模板，需用户主动安装） ──
            catalog = [
                # name, display_name, description, category, transport, command/url, env_template, source, version, author, install_command, docs_url
                ('fetch', 'Fetch', 'Web content fetching and conversion for efficient LLM usage. Supports HTML/Markdown/Text extraction with length limits.', 'search', 'stdio', 'uvx mcp-server-fetch', '{}', 'smithery', '2024.12.16', 'mcp-get', 'uvx mcp-server-fetch', 'https://github.com/modelcontextprotocol/servers/tree/main/src/fetch'),
                ('filesystem', 'Filesystem', 'Read/write/search local files within allowed directories. Configurable root paths.', 'file', 'stdio', 'npx -y @modelcontextprotocol/server-filesystem', '{"allowed_directories": "/tmp"}', 'official', '0.6.3', 'modelcontextprotocol', 'npx -y @modelcontextprotocol/server-filesystem', 'https://github.com/modelcontextprotocol/servers/tree/main/src/filesystem'),
                ('github', 'GitHub', 'GitHub API integration: repos, issues, PRs, workflows, code search. Requires a personal access token.', 'code', 'stdio', 'npx -y @modelcontextprotocol/server-github', '{"GITHUB_PERSONAL_ACCESS_TOKEN": "<your-pat>"}', 'official', '0.6.2', 'modelcontextprotocol', 'npx -y @modelcontextprotocol/server-github', 'https://github.com/modelcontextprotocol/servers/tree/main/src/github'),
                ('git', 'Git', 'Read/search Git repositories locally: status, diff, log, branch, commit, show.', 'code', 'stdio', 'uvx mcp-server-git', '{}', 'smithery', '2024.12.18', 'mcp-get', 'uvx mcp-server-git', 'https://github.com/modelcontextprotocol/servers/tree/main/src/git'),
                ('brave-search', 'Brave Search', 'Web search via Brave Search API. Returns clean results with snippets. Requires BRAVE_API_KEY.', 'search', 'stdio', 'npx -y @modelcontextprotocol/server-brave-search', '{"BRAVE_API_KEY": "<your-brave-api-key>"}', 'official', '0.6.2', 'modelcontextprotocol', 'npx -y @modelcontextprotocol/server-brave-search', 'https://github.com/modelcontextprotocol/servers/tree/main/src/brave-search'),
                ('playwright', 'Playwright', 'Browser automation via Playwright: navigate, click, fill forms, screenshots, accessibility tree. Headless.', 'browser', 'stdio', 'npx -y @microsoft/mcp-playwright', '{}', 'mcp.so', '0.0.1', 'microsoft', 'npx -y @microsoft/mcp-playwright', 'https://github.com/microsoft/playwright-mcp'),
                ('sqlite', 'SQLite', 'Query and modify SQLite databases. Read schema, run SELECT/INSERT/UPDATE with prepared statements.', 'file', 'stdio', 'uvx mcp-server-sqlite', '{"db_path": "./data.db"}', 'official', '0.6.2', 'modelcontextprotocol', 'uvx mcp-server-sqlite', 'https://github.com/modelcontextprotocol/servers/tree/main/src/sqlite'),
                ('memory', 'Memory', 'Knowledge graph-based persistent memory: entities, relations, observations. Useful for long-term recall.', 'code', 'stdio', 'npx -y @modelcontextprotocol/server-memory', '{}', 'official', '0.6.3', 'modelcontextprotocol', 'npx -y @modelcontextprotocol/server-memory', 'https://github.com/modelcontextprotocol/servers/tree/main/src/memory'),
                ('puppeteer', 'Puppeteer', 'Browser automation via Puppeteer: navigate, click, type, screenshots, console logs.', 'browser', 'stdio', 'npx -y @modelcontextprotocol/server-puppeteer', '{}', 'official', '0.6.1', 'modelcontextprotocol', 'npx -y @modelcontextprotocol/server-puppeteer', 'https://github.com/modelcontextprotocol/servers/tree/main/src/puppeteer'),
                ('time', 'Time', 'Get current time, convert timezones, parse datetime strings using IANA names.', 'custom', 'stdio', 'uvx mcp-server-time', '{}', 'smithery', '2024.11.4', 'modelcontextprotocol', 'uvx mcp-server-time', 'https://github.com/modelcontextprotocol/servers/tree/main/src/time'),
            ]
            for row in catalog:
                name, display_name, description, category, transport, command, env_template, source, version, author, install_command, docs_url = row
                await conn.execute(
                    """
                    INSERT INTO mcp_services(
                        name, display_name, description, category, transport, command, args, env_template,
                        builtin, template, source, version, author, install_command, docs_url, enabled
                    ) VALUES($1,$2,$3,$4,$5,$6,'[]'::jsonb,$7::jsonb, false, true, $8,$9,$10,$11,$12, false)
                    ON CONFLICT(name) DO NOTHING
                    """,
                    name, display_name, description, category, transport, command, env_template,
                    source, version, author, install_command, docs_url,
                )

    @classmethod
    async def create_builtin_tool_tables(cls):
        """内置工具（CDP / 邮箱 / 设备控制）统一数据结构——资源一级模型。

        两张通用表，无实例容器：
        - builtin_tool_resources：一行 = 一个用户拥有的一级资源（浏览器客户端 /
          邮箱账户 / 邮箱转发别名 / 已配对设备）。owner_user_id 直挂资源；
          resource_type 区分类型；data/secret 分列，沿用 mcp_runtime_configs 的
          遮蔽口径。
        - builtin_tool_tokens：访问 token，每行一个 token，绑一个 target
          （agent/node/user/external）；自用不显示明文，分享外部可显示；带状态与时效。
          目标二选一：resource_id（内置工具资源）或 service_id（外部 MCP 服务）。

        token 明文永不落库：仅存 sha256(token_hash) 与 hint。全部幂等
        （CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS + 迁移 DO 块），
        轻量与完整模式都执行。

        历史形态（builtin_tool_instances + builtin_tool_details.instance_id +
        builtin_tool_tokens.instance_id）在这里被一次性幂等迁移到资源模型；迁移
        完成后旧表彻底删除，不保留兼容视图。
        """
        async with cls.pool.acquire() as conn:
            # ── 旧库迁移：instances → resources（新库直接跳过） ──
            # 迁移只在旧表存在时执行；每一步都幂等，重复启动安全。
            old_instances_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_name='builtin_tool_instances')"
            )
            old_details_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_name='builtin_tool_details')"
            )
            if old_instances_exists:
                if not old_details_exists:
                    # 只有 instances 没有 details 的中间状态：details 建在下面会
                    # 缺 instance FK，先删掉孤儿 instances 再走全新建表。
                    await conn.execute("DROP TABLE IF EXISTS builtin_tool_instances CASCADE")
                else:
                    # 1) details 补 owner_user_id 并回填（来自所属实例；缺失即孤儿，
                    #    置空并在下面统一报错，不静默丢弃）。
                    await conn.execute(
                        "ALTER TABLE builtin_tool_details "
                        "ADD COLUMN IF NOT EXISTS owner_user_id TEXT"
                    )
                    await conn.execute(
                        "UPDATE builtin_tool_details d "
                        "SET owner_user_id = i.owner_user_id "
                        "FROM builtin_tool_instances i "
                        "WHERE d.instance_id = i.id "
                        "  AND (d.owner_user_id IS NULL OR d.owner_user_id = '')"
                    )
                    orphan_count = await conn.fetchval(
                        "SELECT count(*) FROM builtin_tool_details "
                        "WHERE owner_user_id IS NULL OR owner_user_id = ''"
                    )
                    if orphan_count:
                        raise RuntimeError(
                            f"builtin_tool 迁移中止：{orphan_count} 条明细找不到归属实例"
                            "（owner 缺失）。请先修复数据再升级。"
                        )

                    # 2) tokens 补 resource_id 并回填：
                    #    - external token 的 target_id 优先（它就是绑定的 detail id）；
                    #    - 其余按旧 instance 下的代表性 detail 归属（cdp/device 取
                    #      target_id 或该实例首个 detail；mail 取该实例 mail_account）。
                    await conn.execute(
                        "ALTER TABLE builtin_tool_tokens "
                        "ADD COLUMN IF NOT EXISTS resource_id BIGINT"
                    )
                    await conn.execute(
                        """
                        UPDATE builtin_tool_tokens t
                        SET resource_id = sub.rid
                        FROM (
                            SELECT t.id,
                                   COALESCE(
                                       NULLIF(t.target_id, '')::bigint,
                                       (SELECT d.id FROM builtin_tool_details d
                                        WHERE d.instance_id = t.instance_id
                                          AND d.detail_type = CASE
                                              WHEN i.tool_kind = 'cdp' THEN 'cdp_client'
                                              WHEN i.tool_kind = 'device' THEN 'device'
                                              ELSE 'mail_account' END
                                        ORDER BY d.id LIMIT 1)
                                   ) AS rid
                            FROM builtin_tool_tokens t
                            JOIN builtin_tool_instances i ON i.id = t.instance_id
                            WHERE t.instance_id IS NOT NULL AND t.resource_id IS NULL
                        ) sub
                        WHERE t.id = sub.id
                        """
                    )
                    # resource_id 必须指向真实存在的 detail，否则清空（token 退化为
                    # identity token，不丢行）。
                    await conn.execute(
                        "UPDATE builtin_tool_tokens t SET resource_id = NULL "
                        "WHERE t.resource_id IS NOT NULL AND NOT EXISTS "
                        "(SELECT 1 FROM builtin_tool_details d WHERE d.id = t.resource_id)"
                    )

                    # 3) 表切换：details → resources，删 instance 列；tokens 删 instance 列。
                    await conn.execute("DROP INDEX IF EXISTS idx_builtin_tool_details_instance")
                    await conn.execute(
                        "ALTER TABLE builtin_tool_details RENAME TO builtin_tool_resources"
                    )
                    await conn.execute(
                        "ALTER TABLE builtin_tool_resources RENAME COLUMN detail_type TO resource_type"
                    )
                    await conn.execute(
                        "ALTER TABLE builtin_tool_resources ALTER COLUMN owner_user_id SET NOT NULL"
                    )
                    await conn.execute(
                        "ALTER TABLE builtin_tool_resources DROP COLUMN IF EXISTS instance_id"
                    )
                    await conn.execute("DROP INDEX IF EXISTS idx_builtin_tool_tokens_instance")
                    await conn.execute(
                        "ALTER TABLE builtin_tool_tokens DROP COLUMN IF EXISTS instance_id"
                    )

                    # 4) 删除旧实例表及其索引/约束。
                    await conn.execute("DROP TABLE IF EXISTS builtin_tool_instances CASCADE")
            elif old_details_exists:
                # 有 details 但没有 instances（半迁移状态）：同样走改名收尾。
                await conn.execute(
                    "ALTER TABLE builtin_tool_details "
                    "ADD COLUMN IF NOT EXISTS owner_user_id TEXT"
                )
                await conn.execute(
                    "ALTER TABLE builtin_tool_tokens ADD COLUMN IF NOT EXISTS resource_id BIGINT"
                )
                await conn.execute("DROP INDEX IF EXISTS idx_builtin_tool_details_instance")
                await conn.execute(
                    "ALTER TABLE builtin_tool_details RENAME TO builtin_tool_resources"
                )
                await conn.execute(
                    "ALTER TABLE builtin_tool_resources RENAME COLUMN detail_type TO resource_type"
                )
                await conn.execute(
                    "ALTER TABLE builtin_tool_resources DROP COLUMN IF EXISTS instance_id"
                )
                await conn.execute("DROP INDEX IF EXISTS idx_builtin_tool_tokens_instance")
                await conn.execute(
                    "ALTER TABLE builtin_tool_tokens DROP COLUMN IF EXISTS instance_id"
                )

            # ── builtin_tool_resources：一级资源（全新库直接建，老库已改名） ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS builtin_tool_resources (
                    id BIGSERIAL PRIMARY KEY,
                    owner_user_id TEXT NOT NULL,
                    resource_type VARCHAR(32) NOT NULL,
                    data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    secret_data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    revision BIGINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_builtin_tool_resources_owner "
                "ON builtin_tool_resources(owner_user_id, resource_type)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_builtin_tool_resources_type "
                "ON builtin_tool_resources(resource_type)"
            )
            # resource_type 白名单：老库（曾是 details）的 CHECK 还叫 detail_type 口径，
            # 按定义里出现 resource_type（或 detail_type）找并全部 drop，再补具名约束，
            # 新老库重复执行都无害。
            await conn.execute("""
                DO $$
                DECLARE c record;
                BEGIN
                    FOR c IN
                        SELECT conname FROM pg_constraint
                        WHERE conrelid = 'builtin_tool_resources'::regclass
                          AND contype = 'c'
                          AND (pg_get_constraintdef(oid) LIKE '%resource_type%'
                               OR pg_get_constraintdef(oid) LIKE '%detail_type%')
                    LOOP
                        EXECUTE format('ALTER TABLE builtin_tool_resources DROP CONSTRAINT %I', c.conname);
                    END LOOP;
                    ALTER TABLE builtin_tool_resources
                        ADD CONSTRAINT builtin_tool_resources_resource_type_check
                        CHECK (resource_type IN ('cdp_client', 'mail_account',
                                                 'mail_address', 'device'));
                END $$;
            """)

            # ── builtin_tool_tokens：统一 MCP 访问 token（内置工具 + 外部 MCP 共用） ──
            # 一张表统管所有 MCP 访问 token，鉴权收口只查这一张。目标二选一：
            #   - resource_id：指向 builtin_tool_resources（CDP 客户端 / 邮箱账户 / 设备）
            #   - service_id：指向 mcp_services 里的外部 MCP 服务
            # 目标至多一个非空，分三种 token：
            #   - 身份 token（resource_id/service_id 都空）：标识调用方（agent/node），
            #     它能用什么由调用方自身推导，不绑死单一目标。作 agent/节点启动环境变量。
            #   - 内置工具 token（resource_id 非空）：分享某个客户端/邮箱/设备资源。
            #   - 外部 MCP token（service_id 非空）：分享某个 mcp_services 服务。
            # target_type=agent|node|user|external：token 发给谁。target_id 存 agent_id/
            # node_id/被分享 uid/外部标识。display_token：自用(agent/node/user)=false 不显示；
            # 分享外部(external)=true 可复制。status=active|disabled；expires_at NULL=永久。
            # token 明文永不落库，仅存 sha256 哈希 + hint。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS builtin_tool_tokens (
                    id BIGSERIAL PRIMARY KEY,
                    resource_id BIGINT REFERENCES builtin_tool_resources(id) ON DELETE CASCADE,
                    service_id INTEGER REFERENCES mcp_services(id) ON DELETE CASCADE,
                    token_hash TEXT,
                    token_hint TEXT,
                    target_type VARCHAR(16) NOT NULL,
                    target_id TEXT,
                    display_token BOOLEAN NOT NULL DEFAULT FALSE,
                    status VARCHAR(16) NOT NULL DEFAULT 'active',
                    expires_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CHECK (target_type IN ('agent', 'node', 'user', 'external')),
                    CHECK (status IN ('active', 'disabled')),
                    CHECK (resource_id IS NULL OR service_id IS NULL)
                )
            """)
            # 历史表补列（幂等）。
            await conn.execute("ALTER TABLE builtin_tool_tokens ADD COLUMN IF NOT EXISTS service_id INTEGER REFERENCES mcp_services(id) ON DELETE CASCADE")
            await conn.execute("ALTER TABLE builtin_tool_tokens ADD COLUMN IF NOT EXISTS resource_id BIGINT REFERENCES builtin_tool_resources(id) ON DELETE CASCADE")
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_builtin_tool_tokens_hash "
                "ON builtin_tool_tokens(token_hash) WHERE token_hash IS NOT NULL"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_builtin_tool_tokens_resource ON builtin_tool_tokens(resource_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_builtin_tool_tokens_service ON builtin_tool_tokens(service_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_builtin_tool_tokens_target ON builtin_tool_tokens(target_type, target_id)")
            # 老 CHECK 里写的是 instance_id；按定义里出现 resource_id（或 instance_id）
            # 的互斥约束统一收敛成 resource 口径。先按定义匹配清掉老约束，再按名字
            # 显式 drop 目标约束（防御老迁移用逻辑等价但文本不同的写法建过同名约束，
            # 导致定义匹配漏抓、ADD 时报 already exists），最后统一 ADD。
            await conn.execute("""
                DO $$
                DECLARE c record;
                BEGIN
                    FOR c IN
                        SELECT conname FROM pg_constraint
                        WHERE conrelid = 'builtin_tool_tokens'::regclass
                          AND contype = 'c'
                          AND (pg_get_constraintdef(oid) LIKE '%resource_id IS NULL OR service_id IS NULL%'
                               OR pg_get_constraintdef(oid) LIKE '%instance_id IS NULL OR service_id IS NULL%')
                    LOOP
                        EXECUTE format('ALTER TABLE builtin_tool_tokens DROP CONSTRAINT %I', c.conname);
                    END LOOP;
                    ALTER TABLE builtin_tool_tokens
                        DROP CONSTRAINT IF EXISTS builtin_tool_tokens_target_exclusive_check;
                    ALTER TABLE builtin_tool_tokens
                        ADD CONSTRAINT builtin_tool_tokens_target_exclusive_check
                        CHECK (resource_id IS NULL OR service_id IS NULL);
                END $$;
            """)

            # ── iOS 签名配置（Stage 3：WDA 自动签名/续签） ──
            # 签名材料（App Store Connect p8 key / 手动 p12+mobileprovision）服务端托管，
            # job 启动时经 TLS NodeConnect 一次性下发到节点，签完删除。secret_data 存敏感
            # 字段，永不回显（GET 响应只返 id/name/kind/created_at，不含 secret_data）。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ios_signing_profiles (
                    id BIGSERIAL PRIMARY KEY,
                    owner_user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    kind VARCHAR(16) NOT NULL,
                    secret_data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CHECK (kind IN ('asc', 'p12'))
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ios_signing_profiles_owner "
                "ON ios_signing_profiles(owner_user_id)"
            )

            # ── iOS WDA 产物库（已下线，保留建表防已部署库报错） ──
            # WDA 产物不再单独入库：prepare_wda 改从市场 device-control iOS 发行版
            # 解析 GitHub Release 直链 + sha256，宿主节点经自身出口代理下载后重签
            # 安装（见 routes_ios.prepare_wda / routes_build）。本表已无 API/store 引用，
            # 仅保留 CREATE TABLE IF NOT EXISTS 以兼容已部署库；存量数据可后续手动清理。
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ios_wda_artifacts (
                    id BIGSERIAL PRIMARY KEY,
                    owner_user_id TEXT,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL DEFAULT '',
                    sha256 TEXT NOT NULL,
                    size_bytes BIGINT NOT NULL DEFAULT 0,
                    storage_path TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(sha256)
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ios_wda_artifacts_owner "
                "ON ios_wda_artifacts(owner_user_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ios_wda_artifacts_sha256 "
                "ON ios_wda_artifacts(sha256)"
            )

    @classmethod
    async def create_attachment_tables(cls):
        """Agent 附件资源表——见 attachment_store.py。

        一行 = 一个 Agent 工作区文件登记成的可外访问附件资源。owner_user_id 是
        发起 Agent 运行的用户（HTTP 下载鉴权主体）；未认领时为 NULL，下载 403。
        存储两种：workspace_ref（原地登记）/ object_store（复制进附件存储区）。
        生命周期：默认 30 天 TTL，status 三态 active|expired|purged；过期后
        renew 续期 +30 天；宽限期满物理删除 object_store 文件。幂等。
        """
        async with cls.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS attachments (
                    id BIGSERIAL PRIMARY KEY,
                    owner_user_id TEXT,
                    created_by_agent_id BIGINT,
                    context_ref TEXT,
                    storage_kind VARCHAR(16) NOT NULL,
                    workspace_path TEXT,
                    object_key TEXT,
                    sha256 TEXT NOT NULL,
                    size_bytes BIGINT NOT NULL,
                    name TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    renewed_count INT NOT NULL DEFAULT 0,
                    last_renewed_at TIMESTAMPTZ,
                    status VARCHAR(16) NOT NULL DEFAULT 'active',
                    last_accessed_at TIMESTAMPTZ,
                    access_count BIGINT NOT NULL DEFAULT 0,
                    purged_at TIMESTAMPTZ,
                    CHECK (storage_kind IN ('workspace_ref', 'object_store')),
                    CHECK (status IN ('active', 'expired', 'purged'))
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_attachments_owner "
                "ON attachments(owner_user_id, status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_attachments_agent "
                "ON attachments(created_by_agent_id, status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_attachments_expires "
                "ON attachments(expires_at) WHERE status IN ('active','expired')"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_attachments_sha "
                "ON attachments(sha256)"
            )

    @classmethod
    async def migrate_limit_policies_from_provider_configs(cls) -> int:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT name, rate_limit, config FROM provider_configs ORDER BY name")
            migrated = 0
            for row in rows:
                provider_name = row["name"]
                exists = await conn.fetchval(
                    "SELECT 1 FROM provider_limit_policies WHERE provider_name=$1 AND name='default'",
                    provider_name,
                )
                if exists:
                    continue
                rate_limit = cls._loads_json(row["rate_limit"], {}) or {}
                cooldown_policy = {"429": str(int(rate_limit.get("cooldown_seconds") or 60)), "error": str(int(rate_limit.get("exception_cooldown_seconds") or 60))}
                for code in rate_limit.get("status_codes") or []:
                    try:
                        cooldown_policy[str(int(code))] = str(int(rate_limit.get("cooldown_seconds") or 60))
                    except (TypeError, ValueError):
                        continue
                freeze_policy = cls._freeze_policy_from_cooldown(provider_name, cooldown_policy)
                await conn.execute(
                    """
                    INSERT INTO provider_limit_policies(
                        provider_name, name, enabled, account_rpm, account_tpm, model_tpm, account_concurrent,
                        account_rph, account_tph, account_rpd, account_tpd,
                        cooldown_policy, freeze_policy, extra, updated_at
                    ) VALUES($1, 'default', true, $2, $3, $4, $5, 0, 0, $6, 0, $7::jsonb, $8::jsonb, '{}'::jsonb, now())
                    ON CONFLICT(provider_name, name) DO NOTHING
                    """,
                    provider_name,
                    int(rate_limit.get("rpm_per_account", rate_limit.get("requests_per_minute_per_account", 0)) or 0),
                    int(rate_limit.get("tpm_per_account", 0) or 0),
                    int(rate_limit.get("tpm_per_model", 0) or 0),
                    int(rate_limit.get("concurrent_per_account", 0) or 0),
                    int(rate_limit.get("rpd_per_account", 0) or 0),
                    cls._dumps(cooldown_policy),
                    cls._dumps(freeze_policy),
                )
                migrated += 1
        if migrated:
            logger.info(f"已迁移 {migrated} 个 provider 限制策略")
        return migrated

    @classmethod
    async def remove_legacy_account_rpd_overrides(cls) -> int:
        """删除账号 JSONB 中废弃的 RPD 覆盖，统一使用渠道策略 account_rpd。"""
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE provider_accounts
                SET account = account - 'rpd_limit' - 'rpd_per_account', updated_at = now()
                WHERE account ? 'rpd_limit' OR account ? 'rpd_per_account'
                """
            )
        try:
            updated = int((result or "").split()[-1])
        except (ValueError, IndexError):
            updated = 0
        if updated:
            logger.info(f"已清理 {updated} 个账号的旧 RPD 覆盖字段")
        return updated

    @classmethod
    async def migrate_freeze_policy_data(cls) -> int:
        """Backfill empty freeze_policy values from legacy cooldown_policy data."""
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, provider_name, cooldown_policy, freeze_policy FROM provider_limit_policies")
            updated = 0
            for row in rows:
                freeze_raw = row["freeze_policy"]
                try:
                    freeze_data = json.loads(freeze_raw) if isinstance(freeze_raw, str) else (freeze_raw or {})
                except Exception:
                    freeze_data = {}
                is_empty = not freeze_data.get("enabled", False) and not freeze_data.get("rules")
                if is_empty:
                    cooldown_raw = row["cooldown_policy"]
                    try:
                        cooldown_policy = json.loads(cooldown_raw) if isinstance(cooldown_raw, str) else (cooldown_raw or {})
                    except Exception:
                        cooldown_policy = {}
                    policy = cls._freeze_policy_from_cooldown(row["provider_name"], cooldown_policy)
                    await conn.execute(
                        "UPDATE provider_limit_policies SET freeze_policy=$1::jsonb, updated_at=now() WHERE id=$2",
                        cls._dumps(policy), row["id"],
                    )
                    updated += 1
            if updated:
                logger.info(f"已回填 {updated} 个 provider 的 freeze_policy")
            return updated

    @staticmethod
    def _migrate_freeze_rule_object_period(rule: dict) -> dict:
        """把单条旧冻结规则转换为对象、周期、数值三字段。"""
        mode_map = {
            "no_freeze": ("account", "none", 0),
            "account_daily": ("account", "today", 0),
            "account_model_daily": ("account_model", "today", 0),
            "account_permanent": ("account", "disabled", 0),
            "fixed_duration": ("account", "seconds", None),
            "account_model_fixed": ("account_model", "seconds", None),
            "account_model_weekly": ("account_model", "week", 0),
            "account_model_monthly": ("account_model", "month", 0),
            "channel_fixed": ("channel", "seconds", None),
            "channel_model_fixed": ("channel_model", "seconds", None),
        }
        mode = str(rule.get("freeze_mode") or "").strip().lower()
        mapped = mode_map.get(mode)
        if mapped is None:
            return rule
        freeze_object, freeze_period, fixed_value = mapped
        try:
            freeze_value = int(rule.get("freeze_seconds") or 0) if fixed_value is None else fixed_value
        except (TypeError, ValueError):
            freeze_value = 0
        migrated = dict(rule)
        migrated.pop("freeze_mode", None)
        migrated.pop("freeze_seconds", None)
        migrated["freeze_object"] = freeze_object
        migrated["freeze_period"] = freeze_period
        migrated["freeze_value"] = freeze_value
        return migrated

    @classmethod
    async def migrate_freeze_object_period_data(cls) -> int:
        """把数据库中的旧冻结规则一次性迁移到对象、周期、数值三字段。"""
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, freeze_policy FROM provider_limit_policies")
            updated = 0
            for row in rows:
                raw = row["freeze_policy"]
                try:
                    data = json.loads(raw) if isinstance(raw, str) else (raw or {})
                except Exception:
                    continue
                rules = data.get("rules") if isinstance(data, dict) else None
                if not isinstance(rules, list):
                    continue
                migrated_rules = [
                    cls._migrate_freeze_rule_object_period(rule) if isinstance(rule, dict) else rule
                    for rule in rules
                ]
                if migrated_rules == rules:
                    continue
                data["rules"] = migrated_rules
                await conn.execute(
                    "UPDATE provider_limit_policies SET freeze_policy=$1::jsonb, updated_at=now() WHERE id=$2",
                    cls._dumps(data), row["id"],
                )
                updated += 1
            if updated:
                logger.info(f"已把 {updated} 个 provider 的冻结规则迁移到对象×周期模型")
            return updated

    @classmethod
    def _dumps(cls, data: Any) -> str:
        """把日志字段序列化为 PostgreSQL jsonb 可接受的 JSON 文本。"""
        try:
            cleaned = cls._strip_nul(data)
            result = json.dumps(
                cleaned,
                ensure_ascii=False,
                allow_nan=False,
                default=lambda v: cls._clean_text(str(v)),
            )
            json.loads(result)
            return result
        except (TypeError, ValueError, OverflowError):
            return json.dumps(cls._clean_text(str(data)), ensure_ascii=False, allow_nan=False)

    @classmethod
    def _strip_nul(cls, value: Any) -> Any:
        """递归剥离 PostgreSQL jsonb/text 无法表示的 NUL 字符。"""
        if isinstance(value, str):
            return value.replace("\x00", "")
        if isinstance(value, dict):
            return {cls._strip_nul(k): cls._strip_nul(v) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._strip_nul(v) for v in value]
        if isinstance(value, tuple):
            return [cls._strip_nul(v) for v in value]
        return value

    @classmethod
    def _clean_text(cls, value: Any) -> Any:
        """剥离 NUL 字符，避免 PostgreSQL text 列写入失败。仅处理 str。"""
        if isinstance(value, str):
            return value.replace("\x00", "")
        return value

    @classmethod
    async def get_config(cls, key: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            value = await conn.fetchval("SELECT data FROM app_config WHERE key=$1", key)
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        return dict(value)

    @classmethod
    async def set_config(cls, key: str, data: dict):
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO app_config(key, data, updated_at)
                VALUES($1, $2::jsonb, now())
                ON CONFLICT(key) DO UPDATE SET data=EXCLUDED.data, updated_at=now()
                """,
                key,
                cls._dumps(data),
            )

    @classmethod
    async def delete_config(cls, key: str):
        async with cls.pool.acquire() as conn:
            await conn.execute("DELETE FROM app_config WHERE key=$1", key)

    # ── HF tokenizer 词表（存 PG，多实例共享）─────────────────────
    # 运行时绝不查这里：启动预载进进程内存，热路径只读内存（见 usage_utils）。
    # 词表可达 20MB，TEXT 存原文；不查 JSON 内部，TEXT 比 JSONB 更轻。

    @classmethod
    async def get_tokenizer_vocabs(cls) -> list[dict]:
        """全量取词表（含 content），供启动预载。行很多时也不会大——内置仓库就几个。"""
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT repo, content, etag, bytes, mirror, downloaded_at, updated_at FROM tokenizer_vocabs"
            )
        return [
            {
                "repo": r["repo"], "content": r["content"], "etag": r["etag"],
                "bytes": r["bytes"], "mirror": r["mirror"],
                "downloaded_at": r["downloaded_at"].timestamp() if r["downloaded_at"] else None,
                "updated_at": r["updated_at"].timestamp() if r["updated_at"] else None,
            }
            for r in rows
        ]

    @classmethod
    async def get_tokenizer_vocab_content(cls, repo: str) -> str | None:
        """单条取 content，供预热自检 / 广播后重载。"""
        async with cls.pool.acquire() as conn:
            return await conn.fetchval("SELECT content FROM tokenizer_vocabs WHERE repo=$1", repo)

    @classmethod
    async def get_tokenizer_vocab(cls, repo: str) -> dict | None:
        """单条取 content + meta，供广播后重载（同时刷新内存文本与 meta 快照）。"""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT repo, content, etag, bytes, mirror, downloaded_at, updated_at "
                "FROM tokenizer_vocabs WHERE repo=$1", repo
            )
        if row is None:
            return None
        return {
            "repo": row["repo"], "content": row["content"], "etag": row["etag"],
            "bytes": row["bytes"], "mirror": row["mirror"],
            "downloaded_at": row["downloaded_at"].timestamp() if row["downloaded_at"] else None,
            "updated_at": row["updated_at"].timestamp() if row["updated_at"] else None,
        }

    @classmethod
    async def upsert_tokenizer_vocab(cls, repo: str, content: str, *,
                                     etag: str | None = None, size: int = 0,
                                     mirror: str | None = None) -> None:
        """写入 / 更新一条词表。content 已在调用方校验过是合法 JSON。"""
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tokenizer_vocabs(repo, content, etag, bytes, mirror, downloaded_at, updated_at)
                VALUES($1, $2, $3, $4, $5, now(), now())
                ON CONFLICT(repo) DO UPDATE SET
                    content=EXCLUDED.content,
                    etag=EXCLUDED.etag,
                    bytes=EXCLUDED.bytes,
                    mirror=EXCLUDED.mirror,
                    downloaded_at=now(),
                    updated_at=now()
                """,
                repo, content, etag, size, mirror,
            )

    @classmethod
    async def get_tokenizer_vocab_versions(cls) -> list[tuple[str, float]]:
        """(repo, updated_at) 全量，供 60s 对账：发现 PG 比内存新就重载。"""
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT repo, updated_at FROM tokenizer_vocabs")
        return [(r["repo"], r["updated_at"].timestamp()) for r in rows]

    @classmethod
    async def insert_tokenizer_vocab_if_absent(cls, repo: str, content: str, *,
                                               etag: str | None = None, size: int = 0,
                                               mirror: str | None = None) -> None:
        """仅迁移用：本地文件导入 PG，已存在不覆盖（ON CONFLICT DO NOTHING）。"""
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tokenizer_vocabs(repo, content, etag, bytes, mirror, downloaded_at, updated_at)
                VALUES($1, $2, $3, $4, $5, now(), now())
                ON CONFLICT(repo) DO NOTHING
                """,
                repo, content, etag, size, mirror,
            )

    @classmethod
    def _provider_base(cls, data: dict) -> dict:
        return {k: v for k, v in data.items() if k != "accounts"}

    @classmethod
    def _merge_provider(cls, base: dict, accounts: list[dict]) -> dict:
        data = dict(base)
        data["accounts"] = accounts
        return data

    @classmethod
    async def migrate_provider_configs_from_app_config(cls, delete_legacy: bool = False) -> int:
        async with cls.pool.acquire() as conn:
            legacy_rows = await conn.fetch("SELECT key, data FROM app_config WHERE key LIKE 'provider:%' ORDER BY key")
            migrated = 0
            for row in legacy_rows:
                name = row["key"].split(":", 1)[1]
                data = row["data"]
                if isinstance(data, str):
                    data = json.loads(data)
                else:
                    data = dict(data)
                await cls._set_provider_config_conn(conn, name, data, delete_legacy=delete_legacy)
                migrated += 1
        if migrated:
            logger.info(f"已迁移 {migrated} 个旧 provider 配置到新表")
        return migrated

    @classmethod
    async def migrate_provider_models_from_legacy_columns(cls) -> int:
        """把 provider_configs 上历史的 model_aliases/model_whitelist + config.custom_models
        迁移到新表 provider_models，然后删旧列。幂等：旧列已删 / 该 provider 已有行就跳过。
        """
        async with cls.pool.acquire() as conn:
            has_alias_col = await conn.fetchval(
                "SELECT 1 FROM information_schema.columns WHERE table_name='provider_configs' AND column_name='model_aliases'"
            )
            has_whitelist_col = await conn.fetchval(
                "SELECT 1 FROM information_schema.columns WHERE table_name='provider_configs' AND column_name='model_whitelist'"
            )
            if not has_alias_col and not has_whitelist_col:
                return 0

            select_cols = ["name", "config"]
            if has_alias_col:
                select_cols.append("model_aliases")
            if has_whitelist_col:
                select_cols.append("model_whitelist")
            rows = await conn.fetch(f"SELECT {', '.join(select_cols)} FROM provider_configs")
            total = 0
            for row in rows:
                provider = row["name"]
                existing = await conn.fetchval(
                    "SELECT COUNT(*) FROM provider_models WHERE provider=$1", provider
                )
                cfg = json.loads(row["config"]) if isinstance(row["config"], str) else dict(row["config"] or {})

                if existing == 0:
                    aliases_raw = row["model_aliases"] if has_alias_col else {}
                    aliases = json.loads(aliases_raw) if isinstance(aliases_raw, str) else dict(aliases_raw or {})
                    whitelist_raw = row["model_whitelist"] if has_whitelist_col else []
                    whitelist = json.loads(whitelist_raw) if isinstance(whitelist_raw, str) else list(whitelist_raw or [])
                    custom_models = cfg.get("custom_models") or []

                    rows_to_insert: list[tuple[str, str]] = []
                    seen: set[str] = set()
                    W = set(whitelist)

                    for upstream, public in aliases.items():
                        upstream_s = (upstream or "").strip()
                        public_s = (public or "").strip() or upstream_s
                        if not upstream_s:
                            continue
                        if W and public_s not in W and upstream_s not in W:
                            continue
                        if upstream_s in seen:
                            continue
                        seen.add(upstream_s)
                        rows_to_insert.append((upstream_s, public_s))

                    for w in W:
                        w_s = (w or "").strip()
                        if not w_s or w_s in seen:
                            continue
                        if w_s in aliases or w_s in aliases.values():
                            continue
                        seen.add(w_s)
                        rows_to_insert.append((w_s, w_s))

                    for m in custom_models:
                        mid = ""
                        if isinstance(m, dict):
                            mid = (m.get("id") or "").strip()
                        elif isinstance(m, str):
                            mid = m.strip()
                        if not mid or mid in seen:
                            continue
                        seen.add(mid)
                        rows_to_insert.append((mid, (aliases.get(mid) or mid).strip()))

                    for upstream, public in rows_to_insert:
                        await conn.execute(
                            """
                            INSERT INTO provider_models(provider, upstream_model_id, model_id)
                            VALUES($1, $2, $3)
                            ON CONFLICT(provider, upstream_model_id) DO NOTHING
                            """,
                            provider, upstream, public,
                        )
                    total += len(rows_to_insert)

                if "custom_models" in cfg:
                    new_cfg = {k: v for k, v in cfg.items() if k != "custom_models"}
                    await conn.execute(
                        "UPDATE provider_configs SET config=$1::jsonb WHERE name=$2",
                        cls._dumps(new_cfg), provider,
                    )

            if has_alias_col:
                await conn.execute("ALTER TABLE provider_configs DROP COLUMN IF EXISTS model_aliases")
            if has_whitelist_col:
                await conn.execute("ALTER TABLE provider_configs DROP COLUMN IF EXISTS model_whitelist")
            if total:
                logger.info(f"已迁移 {total} 条 model_aliases/whitelist/custom_models 到 provider_models 表")
            return total

    @classmethod
    async def get_provider_config(cls, name: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT enabled, rate_limit, config, billing_mode, error_rate_threshold, updated_at
                FROM provider_configs WHERE name=$1
                """,
                name,
            )
            if row is None:
                legacy = await conn.fetchval("SELECT data FROM app_config WHERE key=$1", f"provider:{name}")
                if legacy is None:
                    return None
                data = json.loads(legacy) if isinstance(legacy, str) else dict(legacy)
                await cls._set_provider_config_conn(conn, name, data, delete_legacy=True)
                return data
            accounts = await cls._fetch_provider_accounts(conn, name)
        base = json.loads(row["config"]) if isinstance(row["config"], str) else dict(row["config"])
        base.update({
            "enabled": row["enabled"],
            "rate_limit": json.loads(row["rate_limit"]) if isinstance(row["rate_limit"], str) else dict(row["rate_limit"]),
            "billing_mode": row["billing_mode"] or "token",
            "error_rate_threshold": float(row["error_rate_threshold"] or 0.3),
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        })
        return cls._merge_provider(base, accounts)

    @classmethod
    async def get_provider_base_config(cls, name: str) -> dict | None:
        """只读渠道基础配置，不查 provider_accounts，供基础配置接口使用。"""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT enabled, rate_limit, config, billing_mode, error_rate_threshold, updated_at
                FROM provider_configs WHERE name=$1
                """,
                name,
            )
            if row is None:
                legacy = await conn.fetchval("SELECT data FROM app_config WHERE key=$1", f"provider:{name}")
                if legacy is None:
                    return None
                data = json.loads(legacy) if isinstance(legacy, str) else dict(legacy)
                await cls._set_provider_config_conn(conn, name, data, delete_legacy=True)
                return cls._provider_base(data)
        base = json.loads(row["config"]) if isinstance(row["config"], str) else dict(row["config"])
        base.update({
            "enabled": row["enabled"],
            "rate_limit": json.loads(row["rate_limit"]) if isinstance(row["rate_limit"], str) else dict(row["rate_limit"]),
            "billing_mode": row["billing_mode"] or "token",
            "error_rate_threshold": float(row["error_rate_threshold"] or 0.3),
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        })
        return base

    @classmethod
    async def set_provider_config(cls, name: str, data: dict):
        async with cls.pool.acquire() as conn:
            await cls._set_provider_config_conn(conn, name, data, delete_legacy=True)

    @classmethod
    async def set_provider_base_config(cls, name: str, data: dict, *, drop_accounts: bool = False):
        """窄写：只 UPSERT provider_configs 行（基础配置 + rate_limit），不碰 accounts 表。

        供渠道基础类写入（toggle / custom-config）使用，
        避免旧的"DELETE 全表 + 全量重插"模式。drop_accounts=True 时才清空 accounts。
        """
        async with cls.pool.acquire() as conn:
            base = cls._provider_base(data)
            config = {k: v for k, v in base.items() if k not in (
                "enabled", "rate_limit", "model_aliases", "model_whitelist", "custom_models", "billing_mode", "error_rate_threshold"
            )}
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO provider_configs(name, enabled, rate_limit, config, billing_mode, error_rate_threshold, updated_at)
                    VALUES($1, $2, $3::jsonb, $4::jsonb, $5, $6, now())
                    ON CONFLICT(name) DO UPDATE SET
                        enabled=EXCLUDED.enabled,
                        rate_limit=EXCLUDED.rate_limit,
                        config=EXCLUDED.config,
                        billing_mode=EXCLUDED.billing_mode,
                        error_rate_threshold=EXCLUDED.error_rate_threshold,
                        updated_at=now()
                    """,
                    name,
                    bool(base.get("enabled", True)),
                    cls._dumps(base.get("rate_limit", {})),
                    cls._dumps(config),
                    str(base.get("billing_mode", "token")),
                    float(base.get("error_rate_threshold", 0.3)),
                )
                if drop_accounts:
                    await conn.execute("DELETE FROM provider_accounts WHERE provider_name=$1", name)

    @classmethod
    async def upsert_provider_account(cls, name: str, account: dict):
        """窄写：单账号行 UPSERT。账号增/改/OAuth 落库使用，不碰其他账号。"""
        username = str(account.get("username") or "").strip()
        if not username:
            raise ValueError("upsert_provider_account: username 必填")
        account_extra = {k: v for k, v in account.items() if k not in ("username", "switch", "priority", "weight")}
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO provider_accounts(provider_name, username, switch, priority, weight, account, updated_at)
                VALUES($1, $2, $3, $4, $5, $6::jsonb, now())
                ON CONFLICT(provider_name, username) DO UPDATE SET
                    switch=EXCLUDED.switch,
                    priority=EXCLUDED.priority,
                    weight=EXCLUDED.weight,
                    account=EXCLUDED.account,
                    updated_at=now()
                """,
                name,
                username,
                bool(account.get("switch", True)),
                int(account.get("priority") or 0),
                int(account.get("weight") or 1),
                cls._dumps(account_extra),
            )

    @classmethod
    async def delete_provider_account(cls, name: str, username: str) -> bool:
        """窄写：单账号行删除。删除账号 / 改名先删后插使用。返回是否删到。"""
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM provider_accounts WHERE provider_name=$1 AND username=$2",
                name,
                username,
            )
            # asyncpg DELETE 返回形如 "DELETE 1"；解析行数
            try:
                return int((result or "").split()[-1]) > 0
            except (ValueError, IndexError):
                return False

    @classmethod
    async def _set_provider_config_conn(cls, conn, name: str, data: dict, delete_legacy: bool = False):
        base = cls._provider_base(data)
        config = {k: v for k, v in base.items() if k not in (
            "enabled", "rate_limit", "model_aliases", "model_whitelist", "custom_models", "billing_mode", "error_rate_threshold"
        )}
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO provider_configs(name, enabled, rate_limit, config, billing_mode, error_rate_threshold, updated_at)
                VALUES($1, $2, $3::jsonb, $4::jsonb, $5, $6, now())
                ON CONFLICT(name) DO UPDATE SET
                    enabled=EXCLUDED.enabled,
                    rate_limit=EXCLUDED.rate_limit,
                    config=EXCLUDED.config,
                    billing_mode=EXCLUDED.billing_mode,
                    error_rate_threshold=EXCLUDED.error_rate_threshold,
                    updated_at=now()
                """,
                name,
                bool(base.get("enabled", True)),
                cls._dumps(base.get("rate_limit", {})),
                cls._dumps(config),
                str(base.get("billing_mode", "token")),
                float(base.get("error_rate_threshold", 0.3)),
            )
            await conn.execute("DELETE FROM provider_accounts WHERE provider_name=$1", name)
            for acc in data.get("accounts", []) or []:
                username = str(acc.get("username") or "").strip()
                if not username:
                    continue
                account_extra = {k: v for k, v in acc.items() if k not in ("username", "switch", "priority", "weight")}
                await conn.execute(
                    """
                    INSERT INTO provider_accounts(provider_name, username, switch, priority, weight, account, updated_at)
                    VALUES($1, $2, $3, $4, $5, $6::jsonb, now())
                    ON CONFLICT(provider_name, username) DO UPDATE SET
                        switch=EXCLUDED.switch,
                        priority=EXCLUDED.priority,
                        weight=EXCLUDED.weight,
                        account=EXCLUDED.account,
                        updated_at=now()
                    """,
                    name,
                    username,
                    bool(acc.get("switch", True)),
                    int(acc.get("priority") or 0),
                    int(acc.get("weight") or 1),
                    cls._dumps(account_extra),
                )
            if delete_legacy:
                await conn.execute("DELETE FROM app_config WHERE key=$1", f"provider:{name}")

    @classmethod
    async def delete_provider_config(cls, name: str):
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM provider_models WHERE provider=$1", name)
                await conn.execute("DELETE FROM provider_configs WHERE name=$1", name)
                await conn.execute("DELETE FROM app_config WHERE key=$1", f"provider:{name}")

    @classmethod
    async def _fetch_provider_accounts(cls, conn, name: str) -> list[dict]:
        rows = await conn.fetch(
            """
            SELECT username, switch, priority, weight, account
            FROM provider_accounts
            WHERE provider_name=$1
            ORDER BY id
            """,
            name,
        )
        accounts = []
        for row in rows:
            account = json.loads(row["account"]) if isinstance(row["account"], str) else dict(row["account"])
            account.update({
                "username": row["username"],
                "switch": row["switch"],
                "priority": row["priority"],
                "weight": row["weight"],
            })
            accounts.append(account)
        return accounts

    @classmethod
    async def list_provider_configs(cls) -> dict[str, dict]:
        async with cls.pool.acquire() as conn:
            legacy_rows = await conn.fetch("SELECT key, data FROM app_config WHERE key LIKE 'provider:%' ORDER BY key")
            for row in legacy_rows:
                name = row["key"].split(":", 1)[1]
                exists = await conn.fetchval("SELECT 1 FROM provider_configs WHERE name=$1", name)
                if exists:
                    continue
                data = row["data"]
                await cls._set_provider_config_conn(conn, name, json.loads(data) if isinstance(data, str) else dict(data), delete_legacy=True)

            rows = await conn.fetch(
                """
                SELECT name, enabled, rate_limit, config, updated_at
                FROM provider_configs
                ORDER BY name
                """
            )
            # N+1 消除：一次性拉全部账号，按 provider 分组，避免每渠道一条 account 查询。
            account_rows = await conn.fetch(
                """
                SELECT provider_name, username, switch, priority, weight, account
                FROM provider_accounts
                ORDER BY provider_name, id
                """
            )
            accounts_by_provider: dict[str, list[dict]] = {}
            for row in account_rows:
                account = json.loads(row["account"]) if isinstance(row["account"], str) else dict(row["account"])
                account.update({
                    "username": row["username"],
                    "switch": row["switch"],
                    "priority": row["priority"],
                    "weight": row["weight"],
                })
                accounts_by_provider.setdefault(row["provider_name"], []).append(account)

            providers = {}
            for row in rows:
                base = json.loads(row["config"]) if isinstance(row["config"], str) else dict(row["config"])
                base.update({
                    "enabled": row["enabled"],
                    "rate_limit": json.loads(row["rate_limit"]) if isinstance(row["rate_limit"], str) else dict(row["rate_limit"]),
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                })
                providers[row["name"]] = cls._merge_provider(base, accounts_by_provider.get(row["name"], []))
        return providers

    @classmethod
    def _default_freeze_policy(cls) -> dict:
        from limit_policy_store import default_freeze_policy
        return default_freeze_policy()

    @classmethod
    def _freeze_policy_from_cooldown(cls, provider_name: str, cooldown_policy: dict | None) -> dict:
        """Convert a legacy ``cooldown_policy`` dict into a ``freeze_policy`` dict.

        Conversion rules (see docs/freeze-policy-migration for detail):
        - Each status-code key (single or comma-separated) becomes one
          ``status_code`` freeze rule per code.
        - ``error`` key becomes an ``exception`` freeze rule.
        - seconds == -2 -> ``account_permanent`` (freeze_seconds=0)
        - seconds == -1 -> ``account_daily`` (freeze_seconds=0)
        - seconds > 0   -> ``fixed_duration`` (freeze_seconds=seconds)
        - seconds == 0 or invalid -> skipped
        - If no valid rule exists, fall back to the default policy.
        """
        def _to_freeze_secs(seconds: int):
            if seconds == -2:
                return "account_permanent", 0
            if seconds == -1:
                return "account_daily", 0
            if seconds > 0:
                return "fixed_duration", seconds
            return None

        rules: list[dict] = []
        raw = cooldown_policy if isinstance(cooldown_policy, dict) else {}
        for key, value in raw.items():
            key_s = str(key or "").strip().lower()
            if not key_s:
                continue

            try:
                seconds = int(value)
            except (TypeError, ValueError):
                continue
            result = _to_freeze_secs(seconds)
            if result is None:
                continue
            mode, freeze_secs = result

            if key_s == "error":
                rules.append({
                    "condition": "exception",
                    "key": "",
                    "operator": "",
                    "value": "",
                    "freeze_mode": mode,
                    "freeze_seconds": freeze_secs,
                })
                continue

            # status-code key (single or comma-separated)
            codes: list[str] = []
            valid = True
            for part in key_s.split(","):
                part = part.strip()
                try:
                    code = int(part)
                except (TypeError, ValueError):
                    valid = False
                    break
                if code < 100 or code > 599:
                    valid = False
                    break
                codes.append(str(code))
            if not valid or not codes:
                continue
            for code in codes:
                rules.append({
                    "condition": "status_code",
                    "key": "",
                    "operator": "==",
                    "value": code,
                    "freeze_mode": mode,
                    "freeze_seconds": freeze_secs,
                })

        if not rules:
            return cls._default_freeze_policy()

        return {"enabled": True, "rules": rules}

    @classmethod
    def _resolve_freeze_policy(cls, value) -> dict:
        """Normalize a freeze_policy value before persistence.

        None / empty / disabled-empty fall back to the default freeze policy.
        """
        if value is None:
            return cls._default_freeze_policy()
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return cls._default_freeze_policy()
        if not isinstance(value, dict):
            return cls._default_freeze_policy()
        if not value.get("enabled", False) and not value.get("rules"):
            return cls._default_freeze_policy()
        return value

    @classmethod
    def _row_to_limit_policy(cls, row) -> dict | None:
        if not row:
            return None
        data = dict(row)
        data["cooldown_policy"] = cls._loads_json(data.get("cooldown_policy"), {"429": "60", "error": "60"}) or {"429": "60", "error": "60"}
        raw_freeze = cls._loads_json(data.get("freeze_policy"), {"enabled": False, "rules": []}) or {"enabled": False, "rules": []}
        # Fall back to default when freeze_policy is effectively empty (null, empty array, or disabled-empty)
        if raw_freeze is None or not isinstance(raw_freeze, dict) or (
            not raw_freeze.get("enabled", False) and not raw_freeze.get("rules")
        ):
            raw_freeze = cls._default_freeze_policy()
        data["freeze_policy"] = raw_freeze
        data["extra"] = cls._loads_json(data.get("extra"), {}) or {}
        return data

    @classmethod
    async def get_provider_limit_policy(cls, provider_name: str, name: str = "default") -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM provider_limit_policies WHERE provider_name=$1 AND name=$2",
                provider_name,
                name,
            )
        return cls._row_to_limit_policy(row)

    @classmethod
    async def upsert_provider_limit_policy(cls, provider_name: str, data: dict) -> dict:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO provider_limit_policies(
                    provider_name, name, enabled, account_rpm, account_tpm, model_tpm, account_concurrent,
                    account_rph, account_tph, account_rpd, account_tpd,
                    cooldown_policy, freeze_policy, extra, updated_at
                ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14::jsonb, now())
                ON CONFLICT(provider_name, name) DO UPDATE SET
                    enabled=EXCLUDED.enabled,
                    account_rpm=EXCLUDED.account_rpm,
                    account_tpm=EXCLUDED.account_tpm,
                    model_tpm=EXCLUDED.model_tpm,
                    account_concurrent=EXCLUDED.account_concurrent,
                    account_rph=EXCLUDED.account_rph,
                    account_tph=EXCLUDED.account_tph,
                    account_rpd=EXCLUDED.account_rpd,
                    account_tpd=EXCLUDED.account_tpd,
                    cooldown_policy=EXCLUDED.cooldown_policy,
                    freeze_policy=EXCLUDED.freeze_policy,
                    extra=EXCLUDED.extra,
                    updated_at=now()
                RETURNING *
                """,
                provider_name,
                str(data.get("name") or "default"),
                bool(data.get("enabled", True)),
                int(data.get("account_rpm") or 0),
                int(data.get("account_tpm") or 0),
                int(data.get("model_tpm") or 0),
                int(data.get("account_concurrent") or 0),
                int(data.get("account_rph") or 0),
                int(data.get("account_tph") or 0),
                int(data.get("account_rpd") or 0),
                int(data.get("account_tpd") or 0),
                cls._dumps(data.get("cooldown_policy") or {"429": "60", "error": "60"}),
                cls._dumps(cls._resolve_freeze_policy(data.get("freeze_policy"))),
                cls._dumps(data.get("extra") or {}),
            )
        return cls._row_to_limit_policy(row)

    @classmethod
    async def list_provider_limit_policies(cls) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM provider_limit_policies ORDER BY provider_name, name")
        return [cls._row_to_limit_policy(row) for row in rows]

    @classmethod
    def _request_log_insert_params(cls, data: dict) -> tuple:
        created_at = data.get("created_at")
        if isinstance(created_at, (int, float)):
            created_at = datetime.fromtimestamp(created_at, timezone.utc)
        elif created_at is None:
            created_at = datetime.now(timezone.utc)
        attempt_key = str(data.get("attempt_key") or f"direct:{uuid.uuid4().hex}")
        attempt_no = int(data.get("attempt_no") or 1)
        return (
            data.get("request_id") or f"legacy:{attempt_key}", attempt_key, attempt_no, created_at,
            data.get("api_key"), data.get("api_key_name"), data.get("provider_name"), data.get("account_username"),
            data.get("model"), data.get("actual_model"), data.get("upstream_returned_model"), data.get("endpoint"),
            data.get("editor_id"), data.get("editor_session_id"), data.get("task_id"),
            data.get("api_key_version"), data.get("api_key_name_snapshot"),
            data.get("api_key_id"), data.get("api_key_parent_id"),
            bool(data.get("success")), data.get("status"), bool(data.get("stream")), int(data.get("duration_ms") or 0),
            data.get("first_token_ms"), data.get("client_type"), data.get("session_id") or data.get("client_session_id"),
            int(data.get("estimated_prompt_tokens") or 0),
            int(data.get("prompt_tokens") or 0), int(data.get("completion_tokens") or 0),
            int(data.get("total_tokens") or 0), int(data.get("cached_tokens") or 0), int(data.get("cache_creation_tokens") or 0),
            int(data.get("reasoning_tokens") or 0),
            cls._clean_text(data.get("router_request_path")),
            cls._dumps(data.get("upstream_status")) if isinstance(data.get("upstream_status"), (dict, list)) else cls._clean_text(data.get("upstream_status")),
            cls._dumps(data.get("error")) if isinstance(data.get("error"), (dict, list)) else cls._clean_text(data.get("error")),
            bool(data.get("payload_truncated")),
            int(data.get("route_duration_ms") or 0),
            int(data.get("candidate_collect_ms") or 0),
            int(data.get("strategy_select_ms") or 0),
            int(data.get("account_reserve_ms") or 0),
            bool(data.get("routing_redis_degraded")),
            cls._dumps(data.get("routing_detail")) if data.get("routing_detail") is not None else None,
            cls._dumps(data.get("channel_retry_attempts") or []),
        )

    _REQUEST_LOG_INSERT_SQL = """
        INSERT INTO request_logs(
            request_id, attempt_key, attempt_no, created_at, api_key, api_key_name, provider_name, account_username,
            model, actual_model, upstream_returned_model, endpoint, editor_id, editor_session_id, task_id,
            api_key_version, api_key_name_snapshot, api_key_id, api_key_parent_id,
            success, status, stream, duration_ms, first_token_ms, client_type, session_id,
            estimated_prompt_tokens, prompt_tokens, completion_tokens, total_tokens, cached_tokens, cache_creation_tokens, reasoning_tokens,
            router_request_path, upstream_status, error, payload_truncated,
            route_duration_ms, candidate_collect_ms, strategy_select_ms, account_reserve_ms, routing_redis_degraded,
            routing_detail, channel_retry_attempts
        ) VALUES(
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43::jsonb,$44::jsonb
        ) ON CONFLICT (attempt_key) DO NOTHING
    """

    @classmethod
    async def insert_request_log_batch(cls, rows: list[dict]) -> list[str]:
        if not cls.pool or not rows:
            return []
        async with cls.pool.acquire() as conn:
            await conn.executemany(cls._REQUEST_LOG_INSERT_SQL, [cls._request_log_insert_params(row) for row in rows])
        return [str(row.get("attempt_key")) for row in rows if row.get("attempt_key")]

    @classmethod
    async def finalize_request_log_batch(cls, rows: list[dict]) -> list[str]:
        if not cls.pool or not rows:
            return []
        params = []
        for data in rows:
            key = data.get("attempt_key")
            if not key:
                continue
            params.append((
                str(key), bool(data.get("success")), data.get("status"), data.get("actual_model"),
                data.get("upstream_returned_model"), int(data.get("duration_ms") or 0), data.get("first_token_ms"),
                int(data.get("prompt_tokens") or 0), int(data.get("completion_tokens") or 0), int(data.get("total_tokens") or 0),
                int(data.get("cached_tokens") or 0), int(data.get("cache_creation_tokens") or 0), int(data.get("reasoning_tokens") or 0),
                cls._clean_text(data.get("router_request_path")),
                cls._clean_text(data.get("upstream_status")), data.get("client_type"),
                cls._dumps(data.get("error")) if isinstance(data.get("error"), (dict, list)) else cls._clean_text(data.get("error")),
                bool(data.get("payload_truncated")),
                int(data.get("route_duration_ms") or 0),
                int(data.get("candidate_collect_ms") or 0),
                int(data.get("strategy_select_ms") or 0),
                int(data.get("account_reserve_ms") or 0),
                bool(data.get("routing_redis_degraded")),
                cls._dumps(data.get("routing_detail")) if data.get("routing_detail") is not None else None,
                cls._dumps(data.get("proxy_info")) if data.get("proxy_info") is not None else None,
                cls._dumps(data.get("channel_retry_attempts") or []),
                int(data.get("estimated_prompt_tokens") or 0),
            ))
        if not params:
            return []
        async with cls.pool.acquire() as conn:
            await conn.executemany("""
                UPDATE request_logs SET
                    success=$2, status=$3, actual_model=$4, upstream_returned_model=$5,
                    duration_ms=$6, first_token_ms=$7, prompt_tokens=$8, completion_tokens=$9,
                    total_tokens=$10, cached_tokens=$11, cache_creation_tokens=$12, reasoning_tokens=$13,
                    router_request_path=$14, upstream_status=$15, client_type=$16, error=$17, payload_truncated=$18,
                    route_duration_ms=$19, candidate_collect_ms=$20, strategy_select_ms=$21,
                    account_reserve_ms=$22, routing_redis_degraded=$23, routing_detail=$24::jsonb,
                    proxy_info=$25::jsonb, channel_retry_attempts=$26::jsonb,
                    estimated_prompt_tokens=$27
                WHERE attempt_key=$1
            """, params)
        return [str(row.get("attempt_key")) for row in rows if row.get("attempt_key")]

    @classmethod
    async def insert_request_log(cls, data: dict):
        """Legacy direct insert for non-request-path callers (e.g. security events)."""
        if not cls.pool:
            return None
        row_data = dict(data)
        row_data.setdefault("attempt_key", f"direct:{uuid.uuid4().hex}")
        row_data.setdefault("attempt_no", 1)
        sql = cls._REQUEST_LOG_INSERT_SQL.replace(
            "ON CONFLICT (attempt_key) DO NOTHING",
            "ON CONFLICT (attempt_key) DO UPDATE SET attempt_key=EXCLUDED.attempt_key RETURNING id",
        )
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *cls._request_log_insert_params(row_data))
        return row["id"] if row else None

    @classmethod
    async def update_request_log(cls, log_id: int, data: dict):
        if not cls.pool or not log_id:
            return
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE request_logs SET
                    success=$2,
                    status=$3,
                    actual_model=$4,
                    upstream_returned_model=$5,
                    duration_ms=$6,
                    first_token_ms=$7,
                    prompt_tokens=$8,
                    completion_tokens=$9,
                    total_tokens=$10,
                    cached_tokens=$11,
                    cache_creation_tokens=$12,
                    reasoning_tokens=$13,
                    router_request_path=$14,
                    upstream_status=$15,
                    client_type=$16,
                    error=$17
                WHERE id=$1
                """,
                log_id,
                bool(data.get("success")),
                data.get("status"),
                data.get("actual_model"),
                data.get("upstream_returned_model"),
                int(data.get("duration_ms") or 0),
                data.get("first_token_ms"),
                int(data.get("prompt_tokens") or 0),
                int(data.get("completion_tokens") or 0),
                int(data.get("total_tokens") or 0),
                int(data.get("cached_tokens") or 0),
                int(data.get("cache_creation_tokens") or 0),
                int(data.get("reasoning_tokens") or 0),
                cls._clean_text(data.get("router_request_path")),
                cls._clean_text(data.get("upstream_status")),
                data.get("client_type"),
                cls._dumps(data.get("error")) if isinstance(data.get("error"), (dict, list)) else cls._clean_text(data.get("error")),
            )

    @classmethod
    async def query_request_logs(cls, filters: dict, limit: int = 100, offset: int = 0) -> dict:
        where = []
        args = []
        def add(cond, val):
            args.append(val)
            where.append(cond.format(len(args)))

        if filters.get("provider_name"):
            add("provider_name=${}", filters["provider_name"])
        if filters.get("account_username"):
            add("account_username=${}", filters["account_username"])
        if filters.get("api_key_name"):
            add("api_key_name=${}", filters["api_key_name"])
        if filters.get("model"):
            add("model=${}", filters["model"])
        if filters.get("success") is not None:
            add("success=${}", filters["success"])
        if filters.get("status"):
            add("status=${}", filters["status"])
        if filters.get("stream") is not None:
            add("stream=${}", filters["stream"])
        if filters.get("client_type"):
            add("client_type ILIKE ${}", f"%{filters['client_type']}%")
        if filters.get("session_id"):
            add("session_id ILIKE ${}", f"%{filters['session_id']}%")
        if filters.get("editor_id"):
            add("editor_id=${}", filters["editor_id"])
        if filters.get("editor_session_id"):
            add("editor_session_id=${}", filters["editor_session_id"])
        if filters.get("start_time"):
            add("created_at >= to_timestamp(${})", filters["start_time"])
        if filters.get("end_time"):
            add("created_at <= to_timestamp(${})", filters["end_time"])

        where_sql = " WHERE " + " AND ".join(where) if where else ""
        count_args = args.copy()
        args.extend([limit, offset])

        # 日志归档已下线，统一只查实时主表 request_logs。
        source_expr = "request_logs"

        async with cls.pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT count(*) FROM {source_expr} {where_sql}", *count_args)
            rows = await conn.fetch(
                f"""
                SELECT id, request_id, attempt_key, attempt_no, extract(epoch from created_at)::float AS time, api_key_name, provider_name,
                       account_username, model, model AS requested_model, coalesce(actual_model, model) AS actual_model, upstream_returned_model,
                       endpoint, success, status, stream, duration_ms, first_token_ms, client_type, session_id,
                       editor_id, editor_session_id, api_key_version,
                       upstream_status,
                       estimated_prompt_tokens, prompt_tokens, completion_tokens, total_tokens, cached_tokens, cache_creation_tokens,
                       payload_truncated, route_duration_ms, candidate_collect_ms, strategy_select_ms,
                       account_reserve_ms, routing_redis_degraded, routing_detail, proxy_info,
                       error IS NOT NULL AND error <> '' AS has_error,
                       CASE WHEN error IS NULL THEN '' ELSE left(error, 300) END AS error_preview
                FROM {source_expr}
                {where_sql}
                ORDER BY created_at DESC
                LIMIT ${len(args)-1} OFFSET ${len(args)}
                """,
                *args,
            )
        result_rows = []
        for row in rows:
            result_rows.append(dict(row))
        return {"total": total, "rows": result_rows}

    @classmethod
    def _loads_json(cls, value, default=None):
        if value is None:
            if isinstance(default, (dict, list)):
                return default.copy()
            return default
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                if isinstance(default, (dict, list)):
                    return default.copy()
                return default
        if isinstance(value, (dict, list)):
            return value.copy()
        return value

    @classmethod
    def _clean_string_list(cls, value) -> list[str]:
        items = cls._loads_json(value, [])
        if not isinstance(items, list):
            return []
        cleaned = []
        for item in items:
            text = str(item).strip() if item is not None else ""
            if text and text not in cleaned:
                cleaned.append(text)
        return cleaned

    @classmethod
    def _clean_key_ids(cls, value) -> list[str]:
        return cls._clean_string_list(value)

    @classmethod
    def _to_bool(cls, value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ("false", "0", "no", "off", ""):
                return False
            if normalized in ("true", "1", "yes", "on"):
                return True
        return bool(value)

    # 默认方案名：无 schemes 的旧组由顶层字段合成的那一套。
    DEFAULT_SCHEME_NAME = "默认"

    @classmethod
    def _normalize_schemes(cls, raw) -> list[dict]:
        """清洗方案数组：每项 {id, name, models, provider_whitelist, provider_blacklist}。

        方案的**身份是 id**（不是 name）：id 组内唯一、去重保留首个；name 只是展示标签，
        允许为空、允许重复、允许改名——都不影响 active_scheme 指向。旧数据无 id 时按数组
        位置生成确定性兜底 id（``legacy-{序号}``），保证读取幂等、不会每次 reload 变 id
        导致快照 fingerprint 抖动。models/名单走 _clean_string_list。非列表/非法项跳过。
        """
        if not isinstance(raw, list):
            return []
        schemes: list[dict] = []
        seen_ids: set[str] = set()
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                continue
            scheme_id = str(item.get("id") or "").strip() or f"legacy-{index}"
            if scheme_id in seen_ids:
                continue
            seen_ids.add(scheme_id)
            schemes.append({
                "id": scheme_id,
                "name": str(item.get("name") or "").strip(),
                "models": cls._clean_string_list(item.get("models", [])),
                "provider_whitelist": cls._clean_string_list(item.get("provider_whitelist", [])),
                "provider_blacklist": cls._clean_string_list(item.get("provider_blacklist", [])),
                # is_backup：显式标记「作为备用方案」。仅被标记的非激活方案进降级链，按数组顺序降级。
                "is_backup": cls._to_bool(item.get("is_backup"), False),
            })
        return schemes

    @classmethod
    def normalize_model_group(cls, name: str, group: dict) -> dict | None:
        if not isinstance(group, dict):
            return None
        group_name = str(group.get("name") or name or "").strip()
        kind = str(group.get("kind") or "custom").strip().lower()
        if kind not in {"custom", "real"} or not group_name:
            return None
        metadata = group.get("metadata") or {}
        if not isinstance(metadata, dict):
            return None
        metadata = {
            str(key): value
            for key, value in metadata.items()
            if str(key) not in {"model_id", "system_id", "id"}
        }
        if kind == "real":
            # real 行同样支持多套方案（渠道档位降级）：身份是 id，激活方案投影到顶层过滤；
            # 无 schemes 时由顶层两字段合成默认方案（与 custom 分支同构）。
            schemes = cls._normalize_schemes(group.get("schemes"))
            top_whitelist = cls._clean_string_list(group.get("provider_whitelist", []))
            top_blacklist = cls._clean_string_list(group.get("provider_blacklist", []))
            if not schemes:
                schemes = [{
                    "id": "legacy-0",
                    "name": cls.DEFAULT_SCHEME_NAME,
                    "models": [],
                    "provider_whitelist": top_whitelist,
                    "provider_blacklist": top_blacklist,
                    "is_backup": False,
                }]
            raw_active = str(group.get("active_scheme") or "").strip()
            active = next((s for s in schemes if s["id"] == raw_active), None) or schemes[0]
            return {
                "name": group_name,
                "kind": kind,
                "enabled": cls._to_bool(group.get("enabled"), True),
                "remark": str(group.get("remark") or "").strip(),
                "models": [],
                "aliases": [],
                # real 行的渠道标签过滤（按模型选路）——语义属于路由，存于行顶层列。
                "provider_whitelist": active["provider_whitelist"],
                "provider_blacklist": active["provider_blacklist"],
                "selection_strategy": DEFAULT_SELECTION_STRATEGY,
                "backup_model_group": "",
                "response_model": "",
                "metadata_model": "",
                "metadata": metadata,
                "schemes": schemes,
                "active_scheme": active["id"],
            }
        # 方案：优先取 schemes；旧组（无 schemes）用顶层三字段合成一条默认方案。
        schemes = cls._normalize_schemes(group.get("schemes"))
        if not schemes:
            top_models = cls._clean_string_list(group.get("models", []))
            if top_models:
                schemes = [{
                    "id": "legacy-0",
                    "name": cls.DEFAULT_SCHEME_NAME,
                    "models": top_models,
                    "provider_whitelist": cls._clean_string_list(group.get("provider_whitelist", [])),
                    "provider_blacklist": cls._clean_string_list(group.get("provider_blacklist", [])),
                    "is_backup": False,
                }]
        if not schemes:
            return None
        # 选激活方案：active_scheme 存的是方案 id（不是 name）。命中则用它，否则回落第一条。
        # 身份用 id 而非 name——name 可空、可重复、可改名，都不影响激活指向。
        active_scheme = str(group.get("active_scheme") or "").strip()
        active = next((s for s in schemes if s["id"] == active_scheme), None) or schemes[0]
        active_scheme = active["id"]
        # 激活方案投影到顶层三字段（运行时选路读的就是顶层）。
        models = active["models"]
        if not group_name or not models:
            return None
        strategy_raw = str(group.get("selection_strategy") or DEFAULT_SELECTION_STRATEGY).strip()
        strategy = strategy_raw if strategy_raw in SELECTION_STRATEGIES else DEFAULT_SELECTION_STRATEGY
        # 别名：去重、去空、且不得与组主名相同（组主名单独占位）。
        aliases = [a for a in cls._clean_string_list(group.get("aliases", [])) if a != group_name]
        return {
            "name": group_name,
            "kind": "custom",
            "enabled": cls._to_bool(group.get("enabled"), True),
            "remark": str(group.get("remark") or "").strip(),
            "models": models,
            "aliases": aliases,
            "provider_whitelist": active["provider_whitelist"],
            "provider_blacklist": active["provider_blacklist"],
            "selection_strategy": strategy,
            "backup_model_group": str(group.get("backup_model_group") or group.get("backup_group") or "").strip(),
            "response_model": str(group.get("response_model") or "").strip(),
            "metadata_model": str(group.get("metadata_model") or "").strip(),
            "metadata": metadata,
            "schemes": schemes,
            "active_scheme": active_scheme,
        }

    @classmethod
    def _model_group_row(cls, row) -> dict | None:
        if row is None:
            return None
        d = dict(row)
        for field in ("models", "aliases", "provider_whitelist", "provider_blacklist"):
            d[field] = cls._loads_json(d.get(field), [])
        strategy = d.get("selection_strategy") or DEFAULT_SELECTION_STRATEGY
        if strategy not in SELECTION_STRATEGIES:
            strategy = DEFAULT_SELECTION_STRATEGY
        d["selection_strategy"] = strategy
        d["backup_group"] = str(d.get("backup_group") or "").strip()
        d["response_model"] = str(d.get("response_model") or "").strip()
        d["metadata_model"] = str(d.get("metadata_model") or "").strip()
        d["kind"] = str(d.get("kind") or "custom").strip().lower()
        if d["kind"] not in {"custom", "real"}:
            d["kind"] = "custom"
        d["metadata"] = cls._loads_json(d.get("metadata"), {}) or {}
        if not isinstance(d["metadata"], dict):
            d["metadata"] = {}
        # 方案：解析 schemes（身份是 id）；旧行无该列时用顶层三字段合成默认方案。
        schemes = cls._normalize_schemes(cls._loads_json(d.get("schemes"), []))
        if not schemes and d.get("models"):
            schemes = [{
                "id": "legacy-0",
                "name": cls.DEFAULT_SCHEME_NAME,
                "models": d.get("models") or [],
                "provider_whitelist": d.get("provider_whitelist") or [],
                "provider_blacklist": d.get("provider_blacklist") or [],
                "is_backup": False,
            }]
        # active_scheme 存的是 id：命中则用它，否则兜底到第一套。找不到不静默改名——按 id 定位。
        active_scheme = str(d.get("active_scheme") or "").strip()
        active = next((s for s in schemes if s["id"] == active_scheme), None) or (schemes[0] if schemes else None)
        active_scheme = active["id"] if active else ""
        # 顶层三字段永远重投影自激活方案（不信任 DB 里存的顶层，避免陈旧/不一致）。
        if active:
            d["models"] = active["models"]
            d["provider_whitelist"] = active["provider_whitelist"]
            d["provider_blacklist"] = active["provider_blacklist"]
        d["schemes"] = schemes
        d["active_scheme"] = active_scheme
        created = d.get("created_at")
        d["created_at"] = created.timestamp() if isinstance(created, datetime) else (created or 0)
        return {k: d.get(k) for k in ("name", "kind", "enabled", "remark", "models", "aliases", "provider_whitelist", "provider_blacklist", "selection_strategy", "backup_group", "response_model", "metadata_model", "metadata", "schemes", "active_scheme", "created_at")}

    @classmethod
    async def _refresh_model_routing_cache(cls):
        try:
            import config
            if hasattr(config.Config, "refresh_model_routing_cache"):
                await config.Config.refresh_model_routing_cache()
        except Exception:
            return

    @classmethod
    async def _upsert_model_group_on_connection(cls, conn, normalized: dict):
        return await conn.fetchrow(
            """
            INSERT INTO model_groups(name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at, updated_at)
            VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15, clock_timestamp(), now())
            ON CONFLICT(name) DO UPDATE SET
                kind=EXCLUDED.kind,
                enabled=EXCLUDED.enabled,
                remark=EXCLUDED.remark,
                models=EXCLUDED.models,
                aliases=EXCLUDED.aliases,
                provider_whitelist=EXCLUDED.provider_whitelist,
                provider_blacklist=EXCLUDED.provider_blacklist,
                selection_strategy=EXCLUDED.selection_strategy,
                backup_group=EXCLUDED.backup_group,
                response_model=EXCLUDED.response_model,
                metadata_model=EXCLUDED.metadata_model,
                metadata=EXCLUDED.metadata,
                schemes=EXCLUDED.schemes,
                active_scheme=EXCLUDED.active_scheme,
                updated_at=now()
            RETURNING name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at
            """,
            normalized["name"],
            normalized["kind"],
            normalized["enabled"],
            normalized["remark"],
            cls._dumps(normalized["models"]),
            cls._dumps(normalized["aliases"]),
            cls._dumps(normalized["provider_whitelist"]),
            cls._dumps(normalized["provider_blacklist"]),
            normalized["selection_strategy"],
            normalized["backup_model_group"],
            normalized["response_model"],
            normalized["metadata_model"],
            cls._dumps(normalized["metadata"]),
            cls._dumps(normalized["schemes"]),
            normalized["active_scheme"],
        )

    @classmethod
    async def list_model_groups(cls) -> dict[str, dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at FROM model_groups ORDER BY created_at ASC, name")
        return {row["name"]: cls._model_group_row(row) for row in rows}

    @classmethod
    async def get_model_group(cls, name: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at FROM model_groups WHERE name=$1", name)
        return cls._model_group_row(row)

    @classmethod
    async def upsert_model_group(cls, name: str, data: dict) -> dict:
        normalized = cls.normalize_model_group(name, data or {})
        if normalized is None:
            raise ValueError("model group name and models are required")
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await cls._upsert_model_group_on_connection(conn, normalized)
                if normalized["name"] != name:
                    await conn.execute("DELETE FROM model_groups WHERE name=$1", name)
        result = cls._model_group_row(row)
        await cls._refresh_model_routing_cache()
        return result

    @classmethod
    async def replace_model_groups(cls, groups: dict[str, dict]) -> dict[str, dict]:
        """Atomically replace all **custom** model groups while preserving input order.

        real 行（kind='real'，真实模型元数据）不参与路由组替换——它们由元数据接口管理，
        全量替换路由组时必须保留，否则一次 PUT /model-routing 会清空所有真实模型元数据。
        """
        if not isinstance(groups, dict):
            raise ValueError("model groups must be a mapping")
        normalized_groups: list[dict] = []
        names: set[str] = set()
        for name, data in groups.items():
            normalized = cls.normalize_model_group(name, data or {})
            if normalized is None:
                raise ValueError(f"model group {name!r} requires name and models")
            if normalized["name"] in names:
                raise ValueError(f"duplicate model group name: {normalized['name']}")
            names.add(normalized["name"])
            normalized_groups.append(normalized)

        rows = []
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM model_groups WHERE kind <> 'real'")
                for normalized in normalized_groups:
                    rows.append(await cls._upsert_model_group_on_connection(conn, normalized))
        result = {row["name"]: cls._model_group_row(row) for row in rows}
        await cls._refresh_model_routing_cache()
        return result

    @classmethod
    async def delete_model_group(cls, name: str) -> bool:
        async with cls.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM model_groups WHERE name=$1", name)
        changed = int(result.split()[-1]) > 0
        if changed:
            await cls._refresh_model_routing_cache()
        return changed

    @classmethod
    async def migrate_model_routing_from_app_config(cls) -> dict:
        async with cls.pool.acquire() as conn:
            group_count = await conn.fetchval("SELECT COUNT(*) FROM model_groups")
            if (group_count or 0) != 0:
                return {"groups": 0}

            rows = await conn.fetch("SELECT data FROM app_config WHERE key='main'")
            if not rows:
                return {"groups": 0}
            data = cls._loads_json(rows[0]["data"], {})
            if not isinstance(data, dict):
                return {"groups": 0}

            model_groups_data = data.get("model_groups", {})
            if isinstance(model_groups_data, dict) and isinstance(model_groups_data.get("groups"), dict):
                groups_raw = model_groups_data.get("groups", {})
            elif isinstance(model_groups_data, dict):
                groups_raw = model_groups_data
            elif isinstance(model_groups_data, list):
                groups_raw = {group.get("name"): group for group in model_groups_data if isinstance(group, dict)}
            else:
                groups_raw = {}

            migrated_groups = 0
            for raw_name, raw_group in groups_raw.items():
                group = cls.normalize_model_group(raw_name, raw_group)
                if not group:
                    continue
                await conn.execute(
                    """
                    INSERT INTO model_groups(name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, updated_at)
                    VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13::jsonb, now())
                    ON CONFLICT(name) DO NOTHING
                    """,
                    group["name"],
                    group.get("kind", "custom"),
                    group["enabled"],
                    group["remark"],
                    cls._dumps(group["models"]),
                    cls._dumps(group["aliases"]),
                    cls._dumps(group["provider_whitelist"]),
                    cls._dumps(group["provider_blacklist"]),
                    group["selection_strategy"],
                    group["backup_model_group"],
                    group["response_model"],
                    group["metadata_model"],
                    cls._dumps(group.get("metadata") or {}),
                )
                migrated_groups += 1

        if migrated_groups:
            logger.info(f"已迁移 {migrated_groups} 个模型组到新表")
        return {"groups": migrated_groups}

    @classmethod
    async def migrate_provider_filters_to_tags(cls) -> dict:
        """一次性清空历史渠道名过滤，为渠道标签过滤语义重新配置留出干净状态。"""
        migration_key = "20260807_provider_filters_to_tags_v1"
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", migration_key)
                applied = await conn.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE migration_key=$1", migration_key
                )
                if applied:
                    return {"applied": False, "api_keys": 0, "model_groups": 0}

                api_result = await conn.execute("""
                    UPDATE api_keys
                    SET provider_whitelist='[]'::jsonb,
                        provider_blacklist='[]'::jsonb
                    WHERE provider_whitelist <> '[]'::jsonb
                       OR provider_blacklist <> '[]'::jsonb
                """)
                group_result = await conn.execute("""
                    UPDATE model_groups
                    SET provider_whitelist='[]'::jsonb,
                        provider_blacklist='[]'::jsonb,
                        schemes=COALESCE((
                            SELECT jsonb_agg(
                                CASE WHEN jsonb_typeof(item)='object'
                                     THEN item || jsonb_build_object(
                                         'provider_whitelist', '[]'::jsonb,
                                         'provider_blacklist', '[]'::jsonb
                                     )
                                     ELSE item
                                END
                                ORDER BY ordinality
                            )
                            FROM jsonb_array_elements(
                                CASE WHEN jsonb_typeof(schemes)='array' THEN schemes ELSE '[]'::jsonb END
                            ) WITH ORDINALITY AS entries(item, ordinality)
                        ), '[]'::jsonb),
                        updated_at=now()
                    WHERE provider_whitelist <> '[]'::jsonb
                       OR provider_blacklist <> '[]'::jsonb
                       OR EXISTS (
                           SELECT 1
                           FROM jsonb_array_elements(
                               CASE WHEN jsonb_typeof(schemes)='array' THEN schemes ELSE '[]'::jsonb END
                           ) AS entry
                           WHERE COALESCE(entry->'provider_whitelist', '[]'::jsonb) <> '[]'::jsonb
                              OR COALESCE(entry->'provider_blacklist', '[]'::jsonb) <> '[]'::jsonb
                       )
                """)
                await conn.execute(
                    "INSERT INTO schema_migrations(migration_key) VALUES($1)", migration_key
                )

        api_count = int(api_result.split()[-1])
        group_count = int(group_result.split()[-1])
        logger.info(
            "渠道过滤已迁移为标签语义：清空 {} 个 API Key、{} 个模型组的历史渠道名单",
            api_count, group_count,
        )
        return {"applied": True, "api_keys": api_count, "model_groups": group_count}

    @classmethod
    async def migrate_api_key_groups_to_junction(cls) -> dict:
        """把 api_keys.group_ids 里的绑定回填进 api_key_groups 结点表。

        幂等：ON CONFLICT DO NOTHING + schema_migrations 标记双保险。只回填根 Key
        （parent_id IS NULL）——派生子 Key 的授权改为从根推导，不再各自持有，历史上
        由 copy_api_key 复制下来的 group_ids 一律丢弃（那本就是泄漏源）。

        注意：回填期间 group_ids 仍是真相源之一（读侧切换前）。迁移只做增量写入，
        不清空 group_ids，留一段时间用于对账；确认无漂移后再单独删列。
        """
        migration_key = "20260824_api_key_groups_junction_v1"
        if not cls.pool:
            return {"applied": False, "bindings": 0}
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", migration_key)
                applied = await conn.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE migration_key=$1", migration_key
                )
                if applied:
                    return {"applied": False, "bindings": 0}
                result = await conn.execute("""
                    INSERT INTO api_key_groups(api_key_id, group_id)
                    SELECT k.id, entry.value
                    FROM api_keys k
                    CROSS JOIN LATERAL jsonb_array_elements_text(
                        CASE WHEN jsonb_typeof(k.group_ids)='array' THEN k.group_ids ELSE '[]'::jsonb END
                    ) AS entry(value)
                    WHERE k.parent_id IS NULL AND btrim(entry.value) <> ''
                    ON CONFLICT (api_key_id, group_id) DO NOTHING
                """)
                await conn.execute(
                    "INSERT INTO schema_migrations(migration_key) VALUES($1)", migration_key
                )
        count = int(result.split()[-1])
        logger.info("api_keys.group_ids 已回填进 api_key_groups：{} 条绑定", count)
        return {"applied": True, "bindings": count}

    @classmethod
    async def migrate_model_metadata_into_model_groups(cls) -> dict:
        """把 model_metadata 表的元数据合并进 model_groups（kind='real'）。

        每条 model_metadata(model_id, data) → model_groups(name=model_id, kind='real',
        metadata=data)。已存在同名 real 行则刷新 metadata；已存在同名 custom 行则跳过
        （保留用户自定义组，不覆盖其路由定义）。幂等：可重复执行，real 行只更新 metadata。
        迁移后 model_groups 同时承载「自定义模型（路由组）」与「真实模型元数据」两类行，
        由 kind 区分；消费层按 kind 过滤即可。
        """
        if not cls.pool:
            return {"migrated": 0}
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT model_id, data FROM model_metadata")
            migrated = 0
            for row in rows:
                model_id = row["model_id"]
                if not model_id:
                    continue
                existing_kind = await conn.fetchval(
                    "SELECT kind FROM model_groups WHERE name=$1", model_id
                )
                # 旧表只作为一次性迁移来源：既有 real 行可能已由管理端在新表中更新，
                # 重启时绝不能再被旧表的陈旧副本覆盖；同名 custom 行同样保留不动。
                if existing_kind is not None:
                    continue
                payload = cls._loads_json(row["data"], {}) or {}
                if not isinstance(payload, dict):
                    payload = {}
                await conn.execute(
                    """
                    INSERT INTO model_groups(name, kind, enabled, remark, models, aliases,
                        provider_whitelist, provider_blacklist, selection_strategy,
                        backup_group, response_model, metadata_model, metadata, schemes,
                        active_scheme, updated_at)
                    VALUES($1, 'real', true, '', '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
                        '[]'::jsonb, 'intelligent', '', '', '', $2::jsonb, '[]'::jsonb, '', now())
                    ON CONFLICT(name) DO NOTHING
                    """,
                    model_id,
                    cls._dumps(payload),
                )
                migrated += 1
        if migrated:
            logger.info(f"已迁移 {migrated} 条 model_metadata 到 model_groups(kind=real)")
        return {"migrated": migrated}

    @classmethod
    async def query_editor_request_logs(
        cls,
        editor_id: str,
        owner_user_id: str | None,
        *,
        editor_session_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """List request history owned by an editor's user."""
        conditions = ["l.editor_id=$1"]
        args: list = [editor_id]
        if owner_user_id != "__admin__":
            conditions.append("e.owner_user_id=$2")
            args.append(str(owner_user_id))
        if editor_session_id:
            args.append(editor_session_id)
            conditions.append(f"l.editor_session_id=${len(args)}")
        where = " AND ".join(conditions)
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        async with cls.pool.acquire() as conn:
            total = await conn.fetchval(
                f"SELECT count(*) FROM request_logs l JOIN editors e ON e.id=l.editor_id WHERE {where}",
                *args,
            )
            rows = await conn.fetch(
                f"""SELECT l.id, l.request_id, l.attempt_key, l.attempt_no,
                    extract(epoch from l.created_at)::float AS time,
                    l.api_key_name, l.provider_name, l.model, l.actual_model,
                    l.endpoint, l.success, l.status, l.stream, l.duration_ms,
                    l.first_token_ms, l.client_type, l.session_id,
                    l.editor_id, l.editor_session_id, l.api_key_version,
                    l.upstream_status, l.total_tokens, l.error IS NOT NULL
                    AND l.error <> '' AS has_error,
                    CASE WHEN l.error IS NULL THEN '' ELSE left(l.error, 300) END
                    AS error_preview
                FROM request_logs l JOIN editors e ON e.id=l.editor_id
                WHERE {where}
                ORDER BY l.created_at DESC, l.id DESC
                LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}""",
                *args,
                limit,
                offset,
            )
        return {"total": int(total or 0), "rows": [dict(row) for row in rows]}

    @classmethod
    async def get_editor_request_log_detail(
        cls, log_id: int, editor_id: str, owner_user_id: str
    ) -> dict | None:
        """Return one full request log only when it belongs to the editor."""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT l.* FROM request_logs l JOIN editors e ON e.id=l.editor_id
                WHERE l.id=$1 AND l.editor_id=$2 AND e.owner_user_id=$3""",
                log_id,
                editor_id,
                str(owner_user_id),
            )
        if not row:
            return None
        data = dict(row)
        for field in ("routing_detail", "proxy_info"):
            data[field] = cls._loads_json(data.get(field), {})
        from request_payload_store import fetch_payloads, hydrate

        payloads, status = await fetch_payloads([str(data.get("attempt_key") or "")])
        return hydrate(data, payloads.get(str(data.get("attempt_key") or "")), status)

    @classmethod
    async def query_task_request_logs(
        cls, task_id: str, *, limit: int = 100, offset: int = 0
    ) -> dict:
        """List request history written against a Task. Ownership is verified by the
        caller (the Task joins live on the monkeycode_compat connection, not here)."""
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        async with cls.pool.acquire() as conn:
            total = await conn.fetchval(
                "SELECT count(*) FROM request_logs l WHERE l.task_id=$1", task_id
            )
            rows = await conn.fetch(
                """SELECT l.id, l.request_id, l.attempt_key, l.attempt_no,
                    extract(epoch from l.created_at)::float AS time,
                    l.api_key_name, l.provider_name, l.model, l.actual_model,
                    l.endpoint, l.success, l.status, l.stream, l.duration_ms,
                    l.first_token_ms, l.client_type, l.session_id,
                    l.editor_id, l.editor_session_id, l.task_id, l.api_key_version,
                    l.upstream_status, l.total_tokens, l.error IS NOT NULL
                    AND l.error <> '' AS has_error,
                    CASE WHEN l.error IS NULL THEN '' ELSE left(l.error, 300) END
                    AS error_preview
                FROM request_logs l
                WHERE l.task_id=$1
                ORDER BY l.created_at DESC, l.id DESC
                LIMIT $2 OFFSET $3""",
                task_id,
                limit,
                offset,
            )
        return {"total": int(total or 0), "rows": [dict(row) for row in rows]}

    @classmethod
    async def get_task_request_log_detail(
        cls, log_id: int, task_id: str
    ) -> dict | None:
        """Return one full request log only when it belongs to the task."""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT l.* FROM request_logs l WHERE l.id=$1 AND l.task_id=$2",
                log_id,
                task_id,
            )
        if not row:
            return None
        data = dict(row)
        for field in ("routing_detail", "proxy_info"):
            data[field] = cls._loads_json(data.get(field), {})
        from request_payload_store import fetch_payloads, hydrate

        payloads, status = await fetch_payloads([str(data.get("attempt_key") or "")])
        return hydrate(data, payloads.get(str(data.get("attempt_key") or "")), status)

    @classmethod
    async def get_request_log_detail(cls, log_id: int) -> dict | None:
        """请求日志详情，直接查主表 request_logs（带 body）。查不到即返回 None。"""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT *, extract(epoch from created_at) AS time FROM request_logs WHERE id=$1", log_id)
            if not row:
                return None
            data = dict(row)
            data["routing_detail"] = cls._loads_json(data.get("routing_detail"), {})
            data["proxy_info"] = cls._loads_json(data.get("proxy_info"), {})
            data["archived"] = False
            data["source"] = "live"
            data["channel_retry_attempts"] = cls._loads_json(data.get("channel_retry_attempts"), []) or []

            # 流程聚合：同一 request_id 的全部 attempt 视为一次客户端请求的完整流程。
            # 旧 parent_log_id 已删除；空/异常 request_id 回退为单行（不合并到其他组）。
            request_id = data.get("request_id")
            if request_id:
                attempt_rows = await conn.fetch(
                    """
                    SELECT id, request_id, attempt_key, attempt_no, extract(epoch from created_at)::float AS time,
                           provider_name, account_username, model, actual_model, upstream_returned_model, endpoint, success, status, stream,
                           duration_ms, first_token_ms, client_type, session_id, upstream_status,
                           estimated_prompt_tokens, prompt_tokens, completion_tokens, total_tokens, cached_tokens, cache_creation_tokens,
                           payload_truncated, route_duration_ms, candidate_collect_ms, strategy_select_ms,
                           account_reserve_ms, routing_redis_degraded, routing_detail, proxy_info,
                           router_request_path,
                           channel_retry_attempts,
                           error IS NOT NULL AND error <> '' AS has_error,
                           CASE WHEN error IS NULL THEN '' ELSE left(error, 300) END AS error_preview,
                           'live' AS source
                    FROM request_logs
                    WHERE request_id=$1 AND id <> $2
                    ORDER BY attempt_no ASC, created_at ASC, id ASC
                    """,
                    request_id,
                    log_id,
                )
            else:
                attempt_rows = []

            # 关联安全事件：通过 request_log_id 直接关联，或回退到 request_id 匹配
            security_rows = await conn.fetch(
                """
                SELECT id, request_id, event_type, severity, tag, detail,
                       api_key, model, source_ip,
                       extract(epoch from event_time)::float AS event_time
                FROM security_events
                WHERE request_log_id = $1
                   OR (request_log_id IS NULL AND request_id = $2 AND request_id <> '')
                ORDER BY event_time DESC, id DESC
                """,
                log_id,
                data.get("request_id"),
            )
        security_events = []
        for r in security_rows:
            item = dict(r)
            item["detail"] = cls._loads_json(item.get("detail"), {})
            security_events.append(item)
        data["security_events"] = security_events
        attempts = []
        for r in attempt_rows:
            item = dict(r)
            item["routing_detail"] = cls._loads_json(item.get("routing_detail"), {})
            item["proxy_info"] = cls._loads_json(item.get("proxy_info"), {})
            item["channel_retry_attempts"] = cls._loads_json(item.get("channel_retry_attempts"), []) or []
            attempts.append(item)

        from request_payload_store import fetch_payloads, hydrate

        all_rows = [data, *attempts]
        attempt_keys = [str(item.get("attempt_key") or "") for item in all_rows]
        payloads, payload_status = await fetch_payloads(attempt_keys)
        for item, attempt_key in zip(all_rows, attempt_keys):
            hydrate(item, payloads.get(attempt_key), payload_status)
        data["attempts"] = attempts
        # 同一 request_id 的所有真实上游请求（当前行 + 兄弟 attempts）分别保留 per-call
        # duration_ms，同时提供一次客户端请求视角的合计。内层渠道重试也是独立行，因此
        # 这里会自然把每次同账号重发的耗时相加；不包含日志写入/前端渲染耗时。
        all_attempt_durations = [data.get("duration_ms"), *(item.get("duration_ms") for item in attempts)]
        data["total_duration_ms"] = sum(
            int(value) for value in all_attempt_durations if value is not None
        )
        data["actual_attempt_count"] = 1 + len(attempts)
        data["requested_model"] = data.get("model")
        actual_model = data.get("actual_model") or data.get("model")
        if attempts:
            actual_model = attempts[-1].get("actual_model") or attempts[-1].get("model") or actual_model
        data["actual_model"] = actual_model
        return data

    @classmethod
    async def last_aggregated_dashboard_hour(cls):
        """返回 hourly_dashboard_stats 里最新的小时桶，空表返回 None。

        供定时聚合判断“补到哪了”。仪表盘预聚合段读的是这张表，所以进度必须以
        它为准，不能用 hourly_log_stats。它同时是清理的聚合水位（max(hour)+1h
        之前的原始行才允许删），因此聚合推进必须分批且失败即停，保证该值
        永不越过未聚合数据。
        """
        if not cls.pool:
            return None
        async with cls.pool.acquire() as conn:
            return await conn.fetchval("SELECT max(hour) FROM hourly_dashboard_stats")

    @classmethod
    async def earliest_request_log_hour(cls):
        """返回 request_logs 里最早的小时桶，空表返回 None。

        供定时聚合在聚合表为空时确定补齐起点；受日志保留期约束，范围天然有界。
        """
        if not cls.pool:
            return None
        async with cls.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT date_trunc('hour', min(created_at)) FROM request_logs"
            )

    # 小时聚合与日志清理互斥用的会话级 advisory lock 键，aggregate_hourly_logs 与
    # cleanup_request_logs 共用。取 'HOUR' 的 ASCII 并高位置 1，落在 int4 之外，
    # 避免与库内其他 hashtext(...) 事务锁（迁移/编辑器/渠道线程）撞键。
    HOURLY_STATS_LOCK_KEY = 0x1484F5552

    @classmethod
    async def aggregate_hourly_logs(cls, from_hour: str, to_hour: str):
        """聚合指定小时区间内的顶层请求到小时统计表。

        每次聚合前先删除该区间内两张小时表的旧行，再重新 INSERT。
        这样即使主键口径发生变化（例如新增 actual_model 维度），
        也不会出现新旧主键并存导致同一小时被重复统计的问题。

        model 保留客户端请求名/自定义别名，actual_model 记录请求真正路由到的
        模型广场模型 ID（route_info.public_model_id）。主统计走 actual_model，
        自定义别名榜走 model。历史行 actual_model 为空时回退到 model。

        与清理（cleanup_request_logs）持同一把会话级 advisory lock 互斥：本方法
        先删后插且各语句独立自动提交，若清理插进两者之间删走本区间的原始行，
        该批会按残缺素材算出偏小统计并覆盖正确值。锁持满整个批次，释锁走
        finally（异常路径也在连接归还前解锁，避免带锁连接回池污染后续会话）。
        """
        if not cls.pool:
            return
        conn = await cls.pool.acquire()
        try:
            await conn.execute("SELECT pg_advisory_lock($1)", cls.HOURLY_STATS_LOCK_KEY)
            await conn.execute(
                "DELETE FROM hourly_log_stats WHERE hour >= $1::timestamptz AND hour < $2::timestamptz",
                from_hour,
                to_hour,
            )
            await conn.execute(
                "DELETE FROM hourly_dashboard_stats WHERE hour >= $1::timestamptz AND hour < $2::timestamptz",
                from_hour,
                to_hour,
            )
            await conn.execute(
                """
                INSERT INTO hourly_log_stats (hour, provider_name, model, actual_model, requests, prompt_tokens, completion_tokens, total_tokens, cached_tokens, cache_creation_tokens, reasoning_tokens, reasoning_requests, errors, total_duration_ms)
                SELECT
                    date_trunc('hour', created_at) AS hour,
                    coalesce(provider_name, 'unknown') AS provider_name,
                    coalesce(model, 'unknown') AS model,
                    coalesce(nullif(actual_model, ''), model, 'unknown') AS actual_model,
                    count(*) AS requests,
                    coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
                    coalesce(sum(completion_tokens), 0) AS completion_tokens,
                    coalesce(sum(total_tokens), 0) AS total_tokens,
                    coalesce(sum(cached_tokens), 0) AS cached_tokens,
                    coalesce(sum(cache_creation_tokens), 0) AS cache_creation_tokens,
                    coalesce(sum(reasoning_tokens), 0) AS reasoning_tokens,
                    coalesce(sum(CASE WHEN reasoning_tokens > 0 THEN 1 ELSE 0 END), 0) AS reasoning_requests,
                    coalesce(sum(CASE WHEN success=false OR (error IS NOT NULL AND error <> '') OR status NOT IN ('ok','success','200') THEN 1 ELSE 0 END), 0) AS errors,
                    coalesce(sum(CASE WHEN success=true AND coalesce(error, '') = '' AND status IN ('ok','success','200') THEN duration_ms ELSE 0 END), 0) AS total_duration_ms
                FROM (
                    SELECT created_at, provider_name, model, actual_model, prompt_tokens, completion_tokens,
                           total_tokens, cached_tokens, cache_creation_tokens, reasoning_tokens, success, error, status, duration_ms
                    FROM request_logs
                    WHERE created_at >= $1::timestamptz AND created_at < $2::timestamptz
                ) src
                WHERE status IS NOT NULL AND status <> 'requesting'
                GROUP BY date_trunc('hour', created_at), coalesce(provider_name, 'unknown'), coalesce(model, 'unknown'), coalesce(nullif(actual_model, ''), model, 'unknown')
                ON CONFLICT (hour, provider_name, model, actual_model) DO NOTHING
                """,
                from_hour,
                to_hour,
            )
            await conn.execute(
                """
                INSERT INTO hourly_dashboard_stats (
                    hour, provider_name, account_username, api_key_name, model, actual_model,
                    requests, prompt_tokens, completion_tokens, total_tokens,
                    cached_tokens, cache_creation_tokens, reasoning_tokens, reasoning_requests,
                    errors, total_duration_ms
                )
                SELECT
                    date_trunc('hour', created_at) AS hour,
                    coalesce(provider_name, 'unknown') AS provider_name,
                    coalesce(account_username, '') AS account_username,
                    coalesce(api_key_name, '') AS api_key_name,
                    coalesce(model, 'unknown') AS model,
                    coalesce(nullif(actual_model, ''), model, 'unknown') AS actual_model,
                    count(*) AS requests,
                    coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
                    coalesce(sum(completion_tokens), 0) AS completion_tokens,
                    coalesce(sum(total_tokens), 0) AS total_tokens,
                    coalesce(sum(cached_tokens), 0) AS cached_tokens,
                    coalesce(sum(cache_creation_tokens), 0) AS cache_creation_tokens,
                    coalesce(sum(reasoning_tokens), 0) AS reasoning_tokens,
                    coalesce(sum(CASE WHEN reasoning_tokens > 0 THEN 1 ELSE 0 END), 0) AS reasoning_requests,
                    coalesce(sum(CASE WHEN success=false OR (error IS NOT NULL AND error <> '') OR status NOT IN ('ok','success','200') THEN 1 ELSE 0 END), 0) AS errors,
                    coalesce(sum(CASE WHEN success=true AND coalesce(error, '') = '' AND status IN ('ok','success','200') THEN duration_ms ELSE 0 END), 0) AS total_duration_ms
                FROM (
                    SELECT created_at, provider_name, account_username, api_key_name, model, actual_model,
                           prompt_tokens, completion_tokens, total_tokens, cached_tokens, cache_creation_tokens,
                           reasoning_tokens, success, error, status, duration_ms
                    FROM request_logs
                    WHERE created_at >= $1::timestamptz AND created_at < $2::timestamptz
                ) src
                WHERE status IS NOT NULL AND status <> 'requesting'
                GROUP BY date_trunc('hour', created_at), coalesce(provider_name, 'unknown'), coalesce(account_username, ''), coalesce(api_key_name, ''), coalesce(model, 'unknown'), coalesce(nullif(actual_model, ''), model, 'unknown')
                ON CONFLICT (hour, provider_name, account_username, api_key_name, model, actual_model) DO UPDATE SET
                    requests = EXCLUDED.requests,
                    prompt_tokens = EXCLUDED.prompt_tokens,
                    completion_tokens = EXCLUDED.completion_tokens,
                    total_tokens = EXCLUDED.total_tokens,
                    cached_tokens = EXCLUDED.cached_tokens,
                    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
                    reasoning_tokens = EXCLUDED.reasoning_tokens,
                    reasoning_requests = EXCLUDED.reasoning_requests,
                    errors = EXCLUDED.errors,
                    total_duration_ms = EXCLUDED.total_duration_ms
                """,
                from_hour,
                to_hour,
            )
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", cls.HOURLY_STATS_LOCK_KEY)
            await cls.pool.release(conn)

    @classmethod
    async def _normalize_legacy_usage_tokens(cls):
        """启动时修复旧日志里被渠道扣掉缓存的输入/总 token。

        历史 request_logs 只保存归一化后的 token 字段，无法拿到每条原始 usage 的
        key 来源。因此这里使用与新 normalize_usage 一致的规则：当已存输入小于
        等于缓存总量（缓存读 + 缓存写）时，说明该输入大概率未包含缓存，将缓存
        加回输入，并用修正后的输入重算 total。修正后 prompt_tokens 会大于缓存，
        所以该 SQL 可重复执行且不会二次加回。
        """
        if not cls.pool:
            return
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE request_logs
                SET prompt_tokens = prompt_tokens + cached_tokens + cache_creation_tokens,
                    total_tokens = prompt_tokens + cached_tokens + cache_creation_tokens + completion_tokens
                WHERE cached_tokens + cache_creation_tokens > 0
                  AND prompt_tokens <= cached_tokens + cache_creation_tokens
                """
            )
        try:
            updated = int(result.split()[-1])
        except Exception:
            updated = 0
        if updated:
            logger.info(f"[usage] 已修复历史 request_logs token 口径: {updated} 行")

    @classmethod
    async def dashboard_stats(cls, filters: dict) -> dict:
        grain = filters.get("grain") if filters.get("grain") in {"hour", "day", "week"} else "hour"
        section = filters.get("section") or filters.get("metric") or "all"
        start_time = filters.get("start_time") or filters.get("start")
        end_time = filters.get("end_time") or filters.get("end")
        provider_filter = filters.get("provider_name") or filters.get("provider")
        account_filter = filters.get("account_username") or filters.get("account")
        api_key_filter = filters.get("api_key_name") or filters.get("api_key")
        model_filter = filters.get("model")

        def rows_to_series(rows, value_type=int):
            raw_rows = [
                {
                    "bucket": float(r["bucket"] or 0),
                    "name": r["name"] or "unknown",
                    "value": value_type(r["value"] or 0),
                    **({"problem": r["problem"]} if "problem" in r and r["problem"] is not None else {}),
                }
                for r in rows
            ]
            if not raw_rows:
                return []

            if grain == "week":
                step_seconds = 7 * 86400
                align_sql = "week"
            elif grain == "day":
                step_seconds = 86400
                align_sql = "day"
            else:
                step_seconds = 3600
                align_sql = "hour"

            def align_bucket(dt: datetime) -> int:
                if align_sql == "week":
                    aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    aligned = aligned - timedelta(days=aligned.weekday())
                elif align_sql == "day":
                    aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    aligned = dt.replace(minute=0, second=0, microsecond=0)
                return int(aligned.timestamp())

            start_bucket = align_bucket(start_dt) if start_dt else min(int(r["bucket"]) for r in raw_rows)
            end_bucket = align_bucket(end_dt) if end_dt else max(int(r["bucket"]) for r in raw_rows)
            if end_bucket < start_bucket:
                return raw_rows

            names = sorted({r["name"] for r in raw_rows})
            values = {(int(r["bucket"]), r["name"]): r for r in raw_rows}
            filled = []
            bucket = start_bucket
            while bucket <= end_bucket:
                for name in names:
                    filled.append(values.get((bucket, name), {"bucket": float(bucket), "name": name, "value": value_type(0)}))
                bucket += step_seconds
            return filled

        async with cls.pool.acquire() as conn:
            now = datetime.now(timezone.utc)
            current_hour = now.replace(minute=0, second=0, microsecond=0)

            def to_dt(value):
                if value is None or value == "":
                    return None
                return datetime.fromtimestamp(float(value), tz=timezone.utc)

            def floor_hour(value: datetime) -> datetime:
                return value.replace(minute=0, second=0, microsecond=0)

            def ceil_hour(value: datetime) -> datetime:
                floored = floor_hour(value)
                return floored if value == floored else floored + timedelta(hours=1)

            start_dt = to_dt(start_time)
            end_dt = to_dt(end_time)
            aggregate_start = ceil_hour(start_dt) if start_dt else None
            aggregate_end_candidates = [current_hour]
            if end_dt:
                aggregate_end_candidates.append(ceil_hour(end_dt) if end_dt == floor_hour(end_dt) else floor_hour(end_dt))
            aggregate_end = min(aggregate_end_candidates)

            combined_args = []

            def arg(value):
                combined_args.append(value)
                return f"${len(combined_args)}"

            parts = []

            if aggregate_start is not None and aggregate_start < aggregate_end:
                where_parts = ["hour >= " + arg(aggregate_start), "hour < " + arg(aggregate_end)]
                if provider_filter:
                    where_parts.append("provider_name = " + arg(provider_filter))
                if account_filter:
                    where_parts.append("account_username = " + arg(account_filter))
                if api_key_filter:
                    where_parts.append("api_key_name = " + arg(api_key_filter))
                if model_filter:
                    where_parts.append("actual_model = " + arg(model_filter))
                parts.append("""
                    SELECT hour AS bucket_at,
                           provider_name,
                           account_username,
                           api_key_name,
                           model,
                           actual_model,
                           requests,
                           prompt_tokens,
                           completion_tokens,
                           total_tokens,
                           cached_tokens,
                           cache_creation_tokens,
                           reasoning_tokens,
                           reasoning_requests,
                           errors,
                           total_duration_ms
                    FROM hourly_dashboard_stats
                    WHERE {where}
                """.format(where=' AND '.join(where_parts)))

            raw_source = """
                SELECT created_at, provider_name, account_username, api_key_name, model, actual_model,
                       prompt_tokens, completion_tokens, total_tokens, cached_tokens, cache_creation_tokens,
                       reasoning_tokens, success, error, status, duration_ms
                FROM request_logs
            """

            def add_raw_range(range_start: datetime | None, range_end: datetime | None, end_inclusive: bool = True):
                if range_start is not None and range_end is not None and range_start > range_end:
                    return
                raw_where: list[str] = []
                if range_start:
                    raw_where.append("created_at >= " + arg(range_start))
                elif start_dt:
                    raw_where.append("created_at >= " + arg(start_dt))
                if range_end:
                    op = "<=" if end_inclusive else "<"
                    raw_where.append(f"created_at {op} " + arg(range_end))
                elif end_dt:
                    raw_where.append("created_at <= " + arg(end_dt))
                if provider_filter:
                    raw_where.append("provider_name = " + arg(provider_filter))
                if account_filter:
                    raw_where.append("account_username = " + arg(account_filter))
                if api_key_filter:
                    raw_where.append("api_key_name = " + arg(api_key_filter))
                if model_filter:
                    raw_where.append("coalesce(nullif(actual_model, ''), model) = " + arg(model_filter))
                raw_where.extend(["status IS NOT NULL AND status <> 'requesting'"])
                parts.append("""
                    SELECT date_trunc('hour', created_at) AS bucket_at,
                           coalesce(provider_name, 'unknown') AS provider_name,
                           coalesce(account_username, '') AS account_username,
                           coalesce(api_key_name, '') AS api_key_name,
                           coalesce(model, 'unknown') AS model,
                           coalesce(nullif(actual_model, ''), model, 'unknown') AS actual_model,
                           count(*) AS requests,
                           coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
                           coalesce(sum(completion_tokens), 0) AS completion_tokens,
                           coalesce(sum(total_tokens), 0) AS total_tokens,
                           coalesce(sum(cached_tokens), 0) AS cached_tokens,
                           coalesce(sum(cache_creation_tokens), 0) AS cache_creation_tokens,
                           coalesce(sum(reasoning_tokens), 0) AS reasoning_tokens,
                           coalesce(sum(CASE WHEN reasoning_tokens > 0 THEN 1 ELSE 0 END), 0) AS reasoning_requests,
                           coalesce(sum(CASE WHEN success=false OR (error IS NOT NULL AND error <> '') OR status NOT IN ('ok','success','200') THEN 1 ELSE 0 END), 0) AS errors,
                           coalesce(sum(CASE WHEN success=true AND coalesce(error, '') = '' AND status IN ('ok','success','200') THEN duration_ms ELSE 0 END), 0) AS total_duration_ms
                    FROM ({raw_source}) src
                    WHERE {where}
                    GROUP BY date_trunc('hour', created_at), coalesce(provider_name, 'unknown'), coalesce(account_username, ''), coalesce(api_key_name, ''), coalesce(model, 'unknown'), coalesce(nullif(actual_model, ''), model, 'unknown')
                """.format(raw_source=raw_source, where=' AND '.join(raw_where)))

            # 实时段：补齐预聚合段未覆盖的尾部区间。预聚合段覆盖 [aggregate_start, aggregate_end)，
            # aggregate_end 通常是 current_hour（上一完整小时的下一整点），但在短窗口/边界情况下
            # 可能更早。live_start 必须从 aggregate_end 起算（而非固定 current_hour），否则
            # 当 start_dt 落在 (aggregate_end, current_hour) 之间时，这段数据既不在预聚合段
            # 也不在实时段，会被静默丢弃（典型场景："近 1 小时"跨当前整点）。
            # 两段通过 aggregate_end 严格衔接、不重叠：预聚合 < aggregate_end，实时 >= aggregate_end。
            live_start = aggregate_end if aggregate_start is not None and aggregate_start < aggregate_end else current_hour
            if start_dt is not None:
                live_start = max(live_start, start_dt)
            if end_dt is None or end_dt >= live_start:
                add_raw_range(live_start, end_dt)

            if not parts:
                fallback_where: list[str] = ["status IS NOT NULL AND status <> 'requesting'"]
                if start_dt:
                    fallback_where.append("created_at >= " + arg(start_dt))
                if end_dt:
                    fallback_where.append("created_at <= " + arg(end_dt))
                if provider_filter:
                    fallback_where.append("provider_name = " + arg(provider_filter))
                if account_filter:
                    fallback_where.append("account_username = " + arg(account_filter))
                if api_key_filter:
                    fallback_where.append("api_key_name = " + arg(api_key_filter))
                if model_filter:
                    fallback_where.append("coalesce(nullif(actual_model, ''), model) = " + arg(model_filter))
                parts.append("""
                    SELECT date_trunc('hour', created_at) AS bucket_at,
                           coalesce(provider_name, 'unknown') AS provider_name,
                           coalesce(account_username, '') AS account_username,
                           coalesce(api_key_name, '') AS api_key_name,
                           coalesce(model, 'unknown') AS model,
                           coalesce(nullif(actual_model, ''), model, 'unknown') AS actual_model,
                           count(*) AS requests,
                           coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
                           coalesce(sum(completion_tokens), 0) AS completion_tokens,
                           coalesce(sum(total_tokens), 0) AS total_tokens,
                           coalesce(sum(cached_tokens), 0) AS cached_tokens,
                           coalesce(sum(cache_creation_tokens), 0) AS cache_creation_tokens,
                           coalesce(sum(reasoning_tokens), 0) AS reasoning_tokens,
                           coalesce(sum(CASE WHEN reasoning_tokens > 0 THEN 1 ELSE 0 END), 0) AS reasoning_requests,
                           coalesce(sum(CASE WHEN success=false OR (error IS NOT NULL AND error <> '') OR status NOT IN ('ok','success','200') THEN 1 ELSE 0 END), 0) AS errors,
                           coalesce(sum(CASE WHEN success=true AND coalesce(error, '') = '' AND status IN ('ok','success','200') THEN duration_ms ELSE 0 END), 0) AS total_duration_ms
                    FROM ({raw_source}) src
                    WHERE {where}
                    GROUP BY date_trunc('hour', created_at), coalesce(provider_name, 'unknown'), coalesce(account_username, ''), coalesce(api_key_name, ''), coalesce(model, 'unknown'), coalesce(nullif(actual_model, ''), model, 'unknown')
                """.format(raw_source=raw_source, where=' AND '.join(fallback_where)))

            combined_sql = " UNION ALL ".join(parts)
            combined_cte = f"WITH combined AS ({combined_sql})"

            result = {"grain": grain}

            if section in {"all", "summary"}:
                summary = await conn.fetchrow(f"""
                    {combined_cte}
                    SELECT coalesce(sum(requests),0) AS requests,
                           coalesce(sum(total_tokens),0) AS total_tokens,
                           coalesce(sum(cached_tokens),0) AS cached_tokens,
                           coalesce(sum(cache_creation_tokens),0) AS cache_creation_tokens,
                           coalesce(sum(reasoning_tokens),0) AS reasoning_tokens,
                           coalesce(sum(reasoning_requests),0) AS reasoning_requests,
                           CASE WHEN coalesce(sum(requests) - sum(errors), 0) = 0 THEN 0 ELSE coalesce(sum(total_duration_ms),0)::float / (sum(requests) - sum(errors)) END AS avg_duration_ms,
                           CASE WHEN coalesce(sum(requests),0) = 0 THEN 0 ELSE coalesce(sum(reasoning_requests),0)::float / sum(requests) END AS reasoning_request_rate,
                           coalesce(sum(prompt_tokens),0) AS prompt_tokens,
                           coalesce(sum(completion_tokens),0) AS completion_tokens,
                           coalesce(sum(errors),0) AS error_count
                    FROM combined
                """, *combined_args)
                end_for_span = end_time or time.time()
                start_for_span = start_time or (end_for_span - 3600)
                span_minutes = max(1, (end_for_span - start_for_span) / 60)
                requests = int(summary["requests"] or 0)
                total_tokens = int(summary["total_tokens"] or 0)
                result["summary"] = {
                    "requests": requests,
                    "total_tokens": total_tokens,
                    "prompt_tokens": int(summary["prompt_tokens"] or 0),
                    "completion_tokens": int(summary["completion_tokens"] or 0),
                    "cached_tokens": int(summary["cached_tokens"] or 0),
                    "cache_creation_tokens": int(summary["cache_creation_tokens"] or 0),
                    "reasoning_tokens": int(summary["reasoning_tokens"] or 0),
                    "reasoning_requests": int(summary["reasoning_requests"] or 0),
                    "reasoning_request_rate": float(summary["reasoning_request_rate"] or 0),
                    "avg_duration_ms": float(summary["avg_duration_ms"] or 0),
                    "error_count": int(summary["error_count"] or 0),
                    "avg_prm": requests / span_minutes,
                    "avg_tpm": total_tokens / span_minutes,
                }

            if section in {"all", "tables"}:
                by_model = await conn.fetch(f"""
                    {combined_cte}
                    SELECT actual_model AS name, coalesce(sum(requests),0) AS requests, coalesce(sum(total_tokens),0) AS tokens
                    FROM combined GROUP BY actual_model ORDER BY requests DESC LIMIT 20
                """, *combined_args)
                result["by_model"] = [dict(r) for r in by_model]

                async def rank_rows(sql: str) -> list[dict]:
                    rows = await conn.fetch(f"{combined_cte}\n{sql}", *combined_args)
                    return [dict(r) for r in rows]

                def rank_select(name_expr: str, extra: str = "", where: str = "") -> str:
                    return f"""
                        SELECT {name_expr} AS name,
                               coalesce(sum(requests),0) AS requests,
                               coalesce(sum(total_tokens),0) AS tokens,
                               CASE WHEN coalesce(sum(sum(total_tokens)) OVER (),0) = 0 THEN 0 ELSE sum(total_tokens)::float / sum(sum(total_tokens)) OVER () END AS token_share,
                               CASE WHEN coalesce(sum(sum(requests)) OVER (),0) = 0 THEN 0 ELSE sum(requests)::float / sum(sum(requests)) OVER () END AS request_share
                               {extra}
                        FROM combined
                        {where}
                        GROUP BY {name_expr}
                    """

                # 使用量榜额外附带响应时间（口径同 speed_top：仅成功请求）与缓存命中率（口径同 cache_hit_top）。
                usage_top_extra = (
                    ", CASE WHEN coalesce(sum(requests) - sum(errors), 0) = 0 THEN 0 ELSE coalesce(sum(total_duration_ms),0)::float / (sum(requests) - sum(errors)) END AS avg_duration_ms"
                    ", CASE WHEN coalesce(sum(prompt_tokens),0) = 0 THEN 0 ELSE sum(cached_tokens)::float / sum(prompt_tokens) END AS cache_hit_rate"
                )
                result["rankings"] = {
                    "model_usage_top": await rank_rows(rank_select("actual_model", extra=usage_top_extra) + " ORDER BY tokens DESC, requests DESC LIMIT 10"),
                    "provider_usage_top": await rank_rows(rank_select("provider_name", extra=usage_top_extra) + " ORDER BY tokens DESC, requests DESC LIMIT 10"),
                    "model_requests_top": await rank_rows(rank_select(
                        "actual_model",
                        ", coalesce(sum(requests) - sum(errors), 0) AS success_requests"
                    ) + " HAVING sum(requests) - sum(errors) > 0 ORDER BY success_requests DESC, tokens DESC LIMIT 10"),
                    "provider_requests_top": await rank_rows(rank_select(
                        "provider_name",
                        ", coalesce(sum(requests) - sum(errors), 0) AS success_requests"
                    ) + " HAVING sum(requests) - sum(errors) > 0 ORDER BY success_requests DESC, tokens DESC LIMIT 10"),
                    "provider_success_rate_top": await rank_rows(rank_select(
                        "provider_name",
                        ", CASE WHEN coalesce(sum(requests),0) = 0 THEN 0 ELSE (sum(requests)-sum(errors))::float / sum(requests) END AS success_rate"
                    ) + " HAVING sum(requests) > 0 ORDER BY success_rate DESC, requests DESC LIMIT 10"),
                    "model_failure_rate_top": await rank_rows(rank_select(
                        "actual_model",
                        ", CASE WHEN coalesce(sum(requests),0) = 0 THEN 0 ELSE sum(errors)::float / sum(requests) END AS failure_rate"
                    ) + " HAVING sum(requests) > 0 ORDER BY failure_rate DESC, sum(errors) DESC LIMIT 10"),
                    "provider_failure_rate_top": await rank_rows(rank_select(
                        "provider_name",
                        ", CASE WHEN coalesce(sum(requests),0) = 0 THEN 0 ELSE sum(errors)::float / sum(requests) END AS failure_rate"
                    ) + " HAVING sum(requests) > 0 ORDER BY failure_rate DESC, sum(errors) DESC LIMIT 10"),
                    "model_speed_top": await rank_rows(rank_select(
                        "actual_model",
                        ", coalesce(sum(requests) - sum(errors), 0) AS success_requests"
                        ", CASE WHEN coalesce(sum(requests) - sum(errors), 0) = 0 THEN 0 ELSE coalesce(sum(total_duration_ms),0)::float / (sum(requests) - sum(errors)) END AS avg_duration_ms"
                    ) + " HAVING sum(requests) - sum(errors) > 0 ORDER BY avg_duration_ms ASC, success_requests DESC LIMIT 10"),
                    "provider_speed_top": await rank_rows(rank_select(
                        "provider_name",
                        ", coalesce(sum(requests) - sum(errors), 0) AS success_requests"
                        ", CASE WHEN coalesce(sum(requests) - sum(errors), 0) = 0 THEN 0 ELSE coalesce(sum(total_duration_ms),0)::float / (sum(requests) - sum(errors)) END AS avg_duration_ms"
                    ) + " HAVING sum(requests) - sum(errors) > 0 ORDER BY avg_duration_ms ASC, success_requests DESC LIMIT 10"),
                    "model_cache_hit_top": await rank_rows(rank_select(
                        "actual_model",
                        ", coalesce(sum(cached_tokens),0) AS cached_tokens, coalesce(sum(prompt_tokens),0) AS prompt_tokens, CASE WHEN coalesce(sum(prompt_tokens),0) = 0 THEN 0 ELSE sum(cached_tokens)::float / sum(prompt_tokens) END AS cache_hit_rate"
                    ) + " HAVING sum(prompt_tokens) > 0 ORDER BY cache_hit_rate DESC, cached_tokens DESC LIMIT 10"),
                    "provider_cache_hit_top": await rank_rows(rank_select(
                        "provider_name",
                        ", coalesce(sum(cached_tokens),0) AS cached_tokens, coalesce(sum(prompt_tokens),0) AS prompt_tokens, CASE WHEN coalesce(sum(prompt_tokens),0) = 0 THEN 0 ELSE sum(cached_tokens)::float / sum(prompt_tokens) END AS cache_hit_rate"
                    ) + " HAVING sum(prompt_tokens) > 0 ORDER BY cache_hit_rate DESC, cached_tokens DESC LIMIT 10"),
                    # 账号维度占比榜：按 account_username 聚合，过滤空账号（失败请求/安全事件直插日志无账号）。
                    # 传 provider 参数时天然限定到单渠道下各账号占比；不传则为全平台账号占比。
                    "account_usage_top": await rank_rows(rank_select(
                        "account_username",
                        extra=usage_top_extra,
                        where="WHERE account_username <> ''",
                    ) + " ORDER BY tokens DESC, requests DESC LIMIT 10"),
                    "account_requests_top": await rank_rows(rank_select(
                        "account_username",
                        ", coalesce(sum(requests) - sum(errors), 0) AS success_requests",
                        where="WHERE account_username <> ''",
                    ) + " HAVING sum(requests) - sum(errors) > 0 ORDER BY success_requests DESC, tokens DESC LIMIT 10"),
                    # 自定义模型（客户端请求名/模型组别名）占用榜：只统计请求名与真实路由模型不同的流量。
                    "custom_model_usage_top": await rank_rows(rank_select(
                        "model",
                        where="WHERE model <> actual_model"
                    ) + " ORDER BY tokens DESC, requests DESC LIMIT 10"),
                }

            series_queries = {
                "model_call_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(requests),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_token_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(total_tokens),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_input_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(prompt_tokens),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_output_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(completion_tokens),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_cache_read_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(cached_tokens),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_cache_write_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(cache_creation_tokens),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_failure_distribution": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name, coalesce(sum(errors),0) AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, int),
                "model_response_average": (f"""
                    {combined_cte}
                    SELECT extract(epoch from date_trunc('{grain}', bucket_at)) AS bucket, actual_model AS name,
                           CASE WHEN coalesce(sum(requests) - sum(errors), 0) = 0 THEN 0 ELSE coalesce(sum(total_duration_ms),0)::float / (sum(requests) - sum(errors)) END AS value
                    FROM combined GROUP BY bucket, name ORDER BY bucket, name
                """, float),
            }
            selected = list(series_queries.keys()) if section == "all" else [section] if section in series_queries else []
            if selected:
                result["series"] = {}
                for name in selected:
                    sql, value_type = series_queries[name]
                    result["series"][name] = rows_to_series(await conn.fetch(sql, *combined_args), value_type)
            return result

    @classmethod
    async def billing_usage(cls, api_key: str) -> dict:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT count(*) AS requests,
                       coalesce(sum(total_tokens),0) AS total_tokens,
                       coalesce(sum(prompt_tokens),0) AS prompt_tokens,
                       coalesce(sum(completion_tokens),0) AS completion_tokens,
                       coalesce(sum(cached_tokens),0) AS cached_tokens,
                       coalesce(sum(cache_creation_tokens),0) AS cache_creation_tokens
                FROM request_logs
                WHERE api_key=$1 AND success=true AND total_tokens > 0
                """,
                api_key,
            )
        return {
            "total_tokens": int(row["total_tokens"] or 0),
            "prompt_tokens": int(row["prompt_tokens"] or 0),
            "completion_tokens": int(row["completion_tokens"] or 0),
            "cached_tokens": int(row["cached_tokens"] or 0),
            "cache_creation_tokens": int(row["cache_creation_tokens"] or 0),
            "requests": int(row["requests"] or 0),
        }

    @classmethod
    async def api_key_usage_totals(cls, api_key_id: int, include_children: bool = False) -> dict:
        """统计一个 key（可含子 key）的成功请求数与 token 总量。

        include_children 分支刻意写成 UNION ALL 而不是
        ``WHERE api_key_id=$1 OR api_key_parent_id=$1``：两列各有自己的部分索引，但
        PG 对跨列 OR 常常放弃索引改走全表扫。拆成两个单列条件后每段都能走索引，
        再把两段的小计相加。第二段用 ``api_key_id IS DISTINCT FROM $1`` 排除
        “自己既是 api_key_id 又是 api_key_parent_id” 的行，避免重复计数。

        这条查询在 LLM 主链路上（main._enforce_api_key_usage_limit 对配了额度的 key
        每请求调用一次），所以它必须能走索引——否则请求耗时随日志表大小线性增长。
        """
        async with cls.pool.acquire() as conn:
            if include_children:
                row = await conn.fetchrow(
                    """SELECT coalesce(sum(requests), 0) AS requests,
                              coalesce(sum(total_tokens), 0) AS total_tokens
                    FROM (
                        SELECT count(*) AS requests, coalesce(sum(total_tokens), 0) AS total_tokens
                        FROM request_logs
                        WHERE api_key_id=$1 AND success=true
                        UNION ALL
                        SELECT count(*) AS requests, coalesce(sum(total_tokens), 0) AS total_tokens
                        FROM request_logs
                        WHERE api_key_parent_id=$1 AND api_key_id IS DISTINCT FROM $1 AND success=true
                    ) parts""",
                    api_key_id,
                )
            else:
                row = await conn.fetchrow(
                    """SELECT count(*) AS requests, coalesce(sum(total_tokens), 0) AS total_tokens
                    FROM request_logs
                    WHERE api_key_id=$1 AND success=true""",
                    api_key_id,
                )
        return {
            "requests": int(row["requests"] or 0),
            "total_tokens": int(row["total_tokens"] or 0),
        }

    @classmethod
    async def recent_logs(cls, limit: int = 50) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT extract(epoch from created_at) AS time, api_key, api_key_name, model, endpoint,
                       status, prompt_tokens, completion_tokens, total_tokens, duration_ms, error, client_type
                FROM request_logs
                WHERE success=true AND total_tokens > 0
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]

    @classmethod
    async def total_successful_request_count(cls) -> int:
        async with cls.pool.acquire() as conn:
            value = await conn.fetchval(
                "SELECT count(*) FROM request_logs WHERE success=true AND total_tokens > 0"
            )
        return int(value or 0)

    @classmethod
    async def token_stats(cls) -> dict[str, dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT coalesce(api_key_name, api_key, 'anonymous') AS key,
                       count(*) AS requests,
                       coalesce(sum(total_tokens),0) AS total_tokens,
                       coalesce(sum(prompt_tokens),0) AS prompt_tokens,
                       coalesce(sum(completion_tokens),0) AS completion_tokens,
                       coalesce(sum(cached_tokens),0) AS cached_tokens,
                       coalesce(sum(cache_creation_tokens),0) AS cache_creation_tokens
                FROM request_logs WHERE success=true AND total_tokens > 0
                GROUP BY coalesce(api_key_name, api_key, 'anonymous')
                ORDER BY requests DESC
                """
            )
        return {
            r["key"]: {
                "total_tokens": int(r["total_tokens"] or 0),
                "prompt_tokens": int(r["prompt_tokens"] or 0),
                "completion_tokens": int(r["completion_tokens"] or 0),
                "cached_tokens": int(r["cached_tokens"] or 0),
                "cache_creation_tokens": int(r["cache_creation_tokens"] or 0),
                "requests": int(r["requests"] or 0),
            }
            for r in rows
        }

    @classmethod
    async def account_daily_usage(cls, username: str, provider_name: str | None = None, date: datetime | None = None) -> list[dict]:
        day = date or datetime.now(timezone.utc)
        start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        where = "account_username=$1 AND success=true AND total_tokens > 0 AND created_at >= $2 AND created_at < $3"
        args = [username, start, end]
        if provider_name:
            where += " AND provider_name=$4"
            args.append(provider_name)
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT model, count(*) AS count
                FROM request_logs WHERE {where}
                GROUP BY model
                ORDER BY model
                """,
                *args,
            )
        return [{"model": r["model"], "count": int(r["count"] or 0)} for r in rows]

    # ==================== API Keys ====================
    @classmethod
    async def list_api_keys(cls) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("""SELECT k.id, k.key, k.name, k.rate_limit, k.provider_whitelist,
                    k.provider_blacklist, k.editor_provider_whitelist, k.editor_provider_blacklist,
                    k.model_whitelist, k.model_blacklist, k.selection_strategy,
                    k.thinking_config, k.disabled, k.user_id, k.vm_id, k.group_ids, k.parent_id,
                    extract(epoch from k.expires_at) AS expires_at, k.usage_limit, k.version,
                    k.label, k.scope, extract(epoch from k.created_at) AS created_at,
                    e.id AS editor_id, e.name AS editor_name
                FROM api_keys k LEFT JOIN editors e ON e.api_key_id=k.id
                ORDER BY k.id""")
        return [cls._api_key_row(r) for r in rows]

    @classmethod
    async def list_api_keys_lite(cls) -> list[dict]:
        """列表展示用的轻量 api_keys：只取展示列，不拉 rate_limit/whitelist/
        thinking_config/usage_limit/group_ids 等大 JSONB。

        明文 key 保留（前端列表的「复制完整密钥」按钮依赖它）；脱敏显示由前端
        ``maskKey`` 完成。详情走 ``get_api_key_by_id``，编辑弹窗点开时前端单独
        拉详情，避免列表把全字段搬一遍——key 多时这些 JSONB 是列表的主要
        传输/反序列化成本。
        """
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("""SELECT k.id, k.key, k.name, k.disabled, k.parent_id,
                    extract(epoch from k.expires_at) AS expires_at, k.version, k.label, k.scope,
                    e.id AS editor_id, e.name AS editor_name
                FROM api_keys k LEFT JOIN editors e ON e.api_key_id=k.id
                ORDER BY k.id""")
        out: list[dict] = []
        for r in rows:
            d = dict(r)
            # 保持与全量行一致的字段骨架：裁掉的字段给空值，下游 isinstance 判定不炸。
            d["rate_limit"] = {}
            d["provider_whitelist"] = []
            d["provider_blacklist"] = []
            d["editor_provider_whitelist"] = []
            d["editor_provider_blacklist"] = []
            d["model_whitelist"] = []
            d["model_blacklist"] = []
            d["thinking_config"] = {}
            d["usage_limit"] = {}
            d["group_ids"] = []
            d["selection_strategy"] = DEFAULT_SELECTION_STRATEGY
            out.append(d)
        return out

    @classmethod
    def _api_key_row(cls, row) -> dict:
        if row is None:
            return None
        d = dict(row)

        def decode_json(value, default):
            # 兼容历史/故障版本写入的双重 JSON 编码（例如 '"[\\"a\\"]"'）。
            # 最多解两层即可覆盖正常 jsonb 与旧副本数据，同时避免无界反序列化。
            decoded = value
            for _ in range(2):
                if not isinstance(decoded, str):
                    break
                try:
                    decoded = json.loads(decoded)
                except json.JSONDecodeError:
                    return default.copy()
            return decoded

        for fld in ("rate_limit", "thinking_config", "usage_limit"):
            value = decode_json(d.get(fld), {})
            d[fld] = dict(value) if isinstance(value, dict) else {}
        for fld in ("provider_whitelist", "provider_blacklist", "model_whitelist", "model_blacklist", "editor_provider_whitelist", "editor_provider_blacklist"):
            value = decode_json(d.get(fld), [])
            d[fld] = list(value) if isinstance(value, (list, tuple)) else []
        # group_ids 可能不在部分 SELECT 列里（老查询未取该列）；缺失时按空数组处理。
        if "group_ids" in d:
            value = decode_json(d.get("group_ids"), [])
            d["group_ids"] = list(value) if isinstance(value, (list, tuple)) else []
        strategy = d.get("selection_strategy") or DEFAULT_SELECTION_STRATEGY
        d["selection_strategy"] = strategy if strategy in SELECTION_STRATEGIES else DEFAULT_SELECTION_STRATEGY
        return d

    @staticmethod
    def _normalize_editor_providers(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            parsed = PostgresClient._loads_json(value, None)
            if isinstance(parsed, (list, tuple, set)):
                items = [str(p).strip() for p in parsed]
            else:
                items = [p.strip() for p in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(p).strip() for p in value]
        else:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            provider = item.lower()
            if provider not in EDITOR_PROVIDERS or provider in seen:
                continue
            seen.add(provider)
            result.append(provider)
        return result

    @staticmethod
    def _normalize_group_ids(value: Any) -> list[str]:
        """把入参归一为去重后的分组 UUID 字符串数组。

        接受 list / 逗号分隔字符串 / None；空值一律返回空数组（= 全局 Key，
        所有请求可用）。仅做形状归一，不校验 UUID 是否真实存在（绑定接口负责）。
        """
        if value is None:
            return []
        if isinstance(value, str):
            items = [p.strip() for p in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(p).strip() for p in value]
        else:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    @staticmethod
    def _normalize_usage_limit(value: Any) -> dict:
        """归一 usage_limit 为只含正整数上限的对象。

        只认 `max_requests` / `max_total_tokens`（运行时也只强制这两项）。<=0 或非法
        值视为不设该项上限。返回可直接 _dumps 写库的 dict。
        """
        if not isinstance(value, dict):
            return {}
        result: dict = {}
        for sub in ("max_requests", "max_total_tokens"):
            try:
                cap = int(value.get(sub) or 0)
            except (TypeError, ValueError):
                continue
            if cap > 0:
                result[sub] = cap
        return result

    @staticmethod
    def _epoch_to_seconds(value: Any) -> float | None:
        """把入参过期时间归一为 epoch 秒；None/空/0 表示永不过期（返回 None）。"""
        if value in (None, "", 0):
            return None
        try:
            epoch = float(value)
        except (TypeError, ValueError):
            return None
        return epoch if epoch > 0 else None

    @classmethod
    async def list_api_keys_for_groups(cls, group_ids: list[str]) -> list[dict]:
        """返回授权给给定分组之一、且未停用的**根** Key（含明文 key）。

        用于 C 端请求链路：把用户所在分组的并集传进来，取出该用户有权使用的
        系统 Key。空 group_ids 直接返回空数组（用户不属于任何分组 = 无系统 Key）。

        授权关系读 api_key_groups 结点表（不再读 api_keys.group_ids）。

        只返回根 Key（``parent_id IS NULL``）：派生 Key（scope=task/editor/copy）
        不持有授权，能不能用由其根 Key 决定。否则任务派生的子 Key 会跟着出现在
        同组其他成员的「父 Key」选择器里。
        """
        norm = cls._normalize_group_ids(group_ids)
        if not norm:
            return []
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT k.id, k.key, k.name, k.rate_limit, k.provider_whitelist, k.provider_blacklist, "
                "k.editor_provider_whitelist, k.editor_provider_blacklist, "
                "k.model_whitelist, k.model_blacklist, k.selection_strategy, k.thinking_config, k.disabled, "
                "k.user_id, k.vm_id, extract(epoch from k.created_at) AS created_at "
                "FROM api_keys k WHERE k.disabled = false AND k.parent_id IS NULL "
                "AND EXISTS (SELECT 1 FROM api_key_groups g "
                "WHERE g.api_key_id = k.id AND g.group_id = ANY($1::text[])) "
                "ORDER BY k.id",
                norm,
            )
        return [cls._api_key_row(r) for r in rows]

    @classmethod
    async def list_api_keys_brief(cls, group_id: str) -> list[dict]:
        """列出所有系统 Key（脱敏），标记哪些已绑定给定分组。分组配置页勾选用。

        返回 [{id, name, key_masked, disabled, bound}]，bound=该 key 是否已授权本分组。
        key 只回脱敏形态（头 6 尾 4），绝不外泄明文。

        只列根 Key：派生 Key 不可被单独授权（与 list_api_keys_for_groups 对称）。
        """
        gid = str(group_id).strip()
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT k.id, k.key, k.name, k.disabled, "
                "EXISTS (SELECT 1 FROM api_key_groups g "
                "WHERE g.api_key_id = k.id AND g.group_id = $1) AS bound "
                "FROM api_keys k WHERE k.parent_id IS NULL ORDER BY k.id",
                gid,
            )
        result: list[dict] = []
        for r in rows:
            key = r["key"] or ""
            masked = f"{key[:6]}...{key[-4:]}" if len(key) > 12 else "****"
            result.append({
                "id": r["id"],
                "name": r["name"] or "",
                "key_masked": masked,
                "disabled": bool(r["disabled"]),
                "bound": bool(r["bound"]) if gid else False,
            })
        return result

    @classmethod
    async def set_group_api_keys(cls, group_id: str, key_ids: list[int]) -> None:
        """设置某分组能用哪些系统 Key（分组侧配置权限的写入口）。

        语义：本分组的授权集合 = 传入的 key_ids。只动 api_key_groups 里 group_id
        等于本分组的行，不影响这些 key 对其它分组的授权（key↔分组多对多）。

        授权是独立关系行，不再是 key 的属性——所以任何改 api_keys 的路径（改名、
        改限流、改白名单）都不可能连带清掉授权。这是这张表存在的主要理由。

        派生 Key 不可被授权：`parent_id IS NOT NULL` 的 id 直接忽略，避免任务子 Key
        被单独授权后绕过「按根 Key 授权」的口径。
        """
        gid = str(group_id).strip()
        if not gid:
            return
        selected = [int(k) for k in (key_ids or [])]
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                if selected:
                    # 只保留仍在选中集里的授权行。
                    await conn.execute(
                        "DELETE FROM api_key_groups "
                        "WHERE group_id = $1 AND NOT (api_key_id = ANY($2::int[]))",
                        gid, selected,
                    )
                    # 新增授权；已存在的靠主键 ON CONFLICT 吃掉。派生 Key 被 JOIN 过滤掉。
                    await conn.execute(
                        "INSERT INTO api_key_groups(api_key_id, group_id) "
                        "SELECT k.id, $1 FROM api_keys k "
                        "WHERE k.id = ANY($2::int[]) AND k.parent_id IS NULL "
                        "ON CONFLICT (api_key_id, group_id) DO NOTHING",
                        gid, selected,
                    )
                else:
                    await conn.execute(
                        "DELETE FROM api_key_groups WHERE group_id = $1", gid
                    )

    @classmethod
    async def delete_api_key_group_bindings(cls, group_id: str) -> int:
        """删除某分组的全部 Key 授权。分组被删时调用。

        没有真外键可依赖（api_keys 由 PostgresClient 建表，mc_team_groups 由
        monkeycode_compat 的 Tortoise 建表，且 compat 关闭时根本不存在——跨库
        DDL 顺序不可控），所以孤儿清理必须显式做。
        """
        gid = str(group_id).strip()
        if not gid:
            return 0
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM api_key_groups WHERE group_id = $1", gid
            )
        return int(result.split()[-1])

    @classmethod
    async def get_api_key_by_key(cls, key: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT id, key, name, rate_limit, provider_whitelist, provider_blacklist, editor_provider_whitelist, editor_provider_blacklist, model_whitelist, model_blacklist, selection_strategy, thinking_config, disabled, user_id, vm_id, group_ids, parent_id, extract(epoch from expires_at) AS expires_at, usage_limit, version, label, scope FROM api_keys WHERE key=$1", key)
        return cls._api_key_row(row) if row else None

    @classmethod
    async def get_api_key_by_id(cls, api_key_id: int) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                    editor_provider_whitelist, editor_provider_blacklist,
                    model_whitelist, model_blacklist, selection_strategy, thinking_config,
                    disabled, user_id, vm_id, group_ids, parent_id,
                    extract(epoch from expires_at) AS expires_at, usage_limit, version, label, scope
                FROM api_keys WHERE id=$1""",
                api_key_id,
            )
        return cls._api_key_row(row) if row else None

    @classmethod
    async def get_api_keys_by_ids(cls, api_key_ids: list[int]) -> dict[int, dict]:
        """按 id 批量取 api_keys 行（含明文 key），返回 id→row。

        列表场景一次性取回当页全部任务的 key 元信息，避免在循环里逐条
        ``get_api_key_by_id`` 产生 N+1 DB 往返。字段与 ``get_api_key_by_id``
        完全一致，经同一 ``_api_key_row`` 归一化。
        """
        ids = [int(k) for k in api_key_ids if k is not None]
        if not ids:
            return {}
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                    editor_provider_whitelist, editor_provider_blacklist,
                    model_whitelist, model_blacklist, selection_strategy, thinking_config,
                    disabled, user_id, vm_id, group_ids, parent_id,
                    extract(epoch from expires_at) AS expires_at, usage_limit, version, label, scope
                FROM api_keys WHERE id = ANY($1::int[])""",
                ids,
            )
        return {int(r["id"]): cls._api_key_row(r) for r in rows}

    @classmethod
    async def get_editor_by_api_key_id(cls, api_key_id: int) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM editors WHERE api_key_id=$1 AND status <> 'deleted'", api_key_id)
        return dict(row) if row else None

    @classmethod
    async def get_editor_by_session_api_key_id(cls, api_key_id: int) -> dict | None:
        """Resolve the editor owning the session that holds this key.

        Key ownership reversed to one-key-per-session; gateway auth resolves the
        session first, then its editor. Returns the editor row plus the bound
        session id so callers can scope thread lookups to the exact session.
        """
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, s.id AS session_id
                FROM editor_sessions s JOIN editors e ON e.id=s.editor_id
                WHERE s.api_key_id=$1 AND e.status <> 'deleted' AND s.status <> 'closed'""",
                api_key_id,
            )
        return dict(row) if row else None

    @classmethod
    async def get_editor_session_by_thread(cls, editor_id: str, provider_thread_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("""SELECT s.*, e.provider FROM editor_sessions s JOIN editors e ON e.id=s.editor_id WHERE s.editor_id=$1 AND s.provider_thread_id=$2 AND s.status='active' AND e.status='active'""", editor_id, provider_thread_id)
        return dict(row) if row else None

    @classmethod
    async def get_pending_editor_sessions(cls, editor_id: str) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT s.*, e.provider FROM editor_sessions s JOIN editors e ON e.id=s.editor_id WHERE s.editor_id=$1 AND s.status='pending_first_request' AND e.status='active' ORDER BY s.created_at", editor_id)
        return [dict(row) for row in rows]

    @classmethod
    async def get_pending_editor_session_for_first_request(
        cls, editor_id: str, expected_client_id: str, bootstrap_content_hash: str
    ) -> dict | None:
        if not expected_client_id or not bootstrap_content_hash:
            return None
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT s.*, e.provider FROM editor_sessions s
                JOIN editors e ON e.id=s.editor_id
                WHERE s.editor_id=$1 AND s.status='pending_first_request'
                    AND s.first_request_seen=false AND s.bootstrap_consumed=false
                    AND s.expected_client_id=$2 AND s.bootstrap_content_hash=$3
                    AND e.status='active'
                ORDER BY s.created_at LIMIT 1""",
                editor_id,
                expected_client_id,
                bootstrap_content_hash,
            )
        return dict(row) if row else None

    @classmethod
    async def get_editor_session_by_id(cls, session_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT s.*, e.provider, e.id AS owner_editor_id,
                    e.owner_user_id
                FROM editor_sessions s JOIN editors e ON e.id=s.editor_id
                WHERE s.id=$1""",
                session_id,
            )
        return dict(row) if row else None

    @classmethod
    async def create_editor_with_key_copy(cls, editor: dict, parent_key_id: int) -> dict:
        """Create an editor and its scoped key atomically.

        The plaintext copy is returned once to the caller; request logs are not
        coupled to the key row and therefore survive editor/key cleanup.
        """
        provider = str(editor.get("provider") or "").strip().lower()
        if provider not in {"claude", "codex", "opencode", "cursor"}:
            raise ValueError("unsupported editor provider")
        editor_id = str(editor.get("id") or f"ed_{uuid.uuid4().hex[:20]}")
        key_value = f"sk-{secrets.token_urlsafe(32)}"
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                parent = await conn.fetchrow("SELECT * FROM api_keys WHERE id=$1 AND disabled=false", parent_key_id)
                if not parent:
                    raise ValueError("parent api key not found")
                project_id = str(editor.get("project_id") or "").strip()
                if project_id:
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"editor-project:{project_id}")
                parent_editor_whitelist = cls._normalize_editor_providers(parent["editor_provider_whitelist"])
                parent_editor_blacklist = cls._normalize_editor_providers(parent["editor_provider_blacklist"])
                if parent_editor_whitelist and provider not in parent_editor_whitelist:
                    raise ValueError("父 API Key 不允许用于该编辑器客户端")
                if provider in parent_editor_blacklist:
                    raise ValueError("父 API Key 禁止用于该编辑器客户端")
                parent_usage_limit = parent["usage_limit"] or {}
                child_usage_limit = editor.get("usage_limit")
                if child_usage_limit is None:
                    child_usage_limit = parent_usage_limit
                child_expires_at = editor.get("expires_at") or parent["expires_at"]
                child = await conn.fetchrow(
                    """INSERT INTO api_keys(
                        key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist,
                        model_whitelist, model_blacklist, selection_strategy,
                        thinking_config, user_id, parent_id, expires_at, usage_limit,
                        version, label, scope
                    ) VALUES($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11::jsonb,$12,$13,$14,$15::jsonb,1,$16,'editor')
                    RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist,
                        model_whitelist, model_blacklist, selection_strategy, thinking_config,
                        disabled, user_id, vm_id, group_ids, parent_id,
                        extract(epoch from expires_at) AS expires_at, usage_limit, version, label, scope""",
                    key_value,
                    editor.get("name") or f"editor-{editor_id}",
                    cls._dumps(parent["rate_limit"]), cls._dumps(parent["provider_whitelist"]), cls._dumps(parent["provider_blacklist"]),
                    cls._dumps(parent_editor_whitelist), cls._dumps(parent_editor_blacklist),
                    cls._dumps(parent["model_whitelist"]), cls._dumps(parent["model_blacklist"]), parent["selection_strategy"],
                    cls._dumps(parent["thinking_config"]), parent["user_id"], parent_key_id,
                    child_expires_at, cls._dumps(child_usage_limit),
                    editor.get("name") or editor_id,
                )
                await conn.execute(
                    """INSERT INTO editors(
                        id, owner_user_id, name, provider, project_id, branch, workdir, node_id,
                        api_key_id, mcp_config, skill_config, plugin_config
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb)""",
                    editor_id, str(editor["owner_user_id"]), editor.get("name") or editor_id, provider,
                    editor.get("project_id"), editor.get("branch"), editor.get("workdir"), editor.get("node_id"),
                    child["id"], cls._dumps(editor.get("mcp_config") or []),
                    cls._dumps(editor.get("skill_config") or []), cls._dumps(editor.get("plugin_config") or []),
                )
        result = dict(editor)
        result.update({"id": editor_id, "api_key_id": child["id"], "api_key_copy": cls._api_key_row(child)})
        return result

    @classmethod
    async def editor_belongs_to_user(cls, editor_id: str, owner_user_id: str) -> bool:
        async with cls.pool.acquire() as conn:
            return bool(
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM editors WHERE id=$1 AND owner_user_id=$2)",
                    editor_id,
                    str(owner_user_id),
                )
            )

    @classmethod
    async def list_all_editors_for_admin(
        cls, *, project_id: str | None = None, provider: str | None = None,
        status: str | None = None, node_id: str | None = None,
        limit: int = 100, offset: int = 0
    ) -> dict:
        conditions = ["1=1"]
        args: list = []
        if project_id:
            args.append(project_id); conditions.append(f"e.project_id=${len(args)}")
        if provider:
            args.append(provider); conditions.append(f"e.provider=${len(args)}")
        if status:
            args.append(status); conditions.append(f"e.status=${len(args)}")
        if node_id:
            args.append(node_id); conditions.append(f"e.node_id=${len(args)}")
        where = " AND ".join(conditions)
        limit = max(1, min(int(limit), 200)); offset = max(0, int(offset))
        async with cls.pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT count(*) FROM editors e WHERE {where}", *args)
            rows = await conn.fetch(
                f"""SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version, extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE {where} ORDER BY e.created_at DESC
                LIMIT ${len(args)+1} OFFSET ${len(args)+2}""",
                *args, limit, offset,
            )
        return {"total": int(total or 0), "rows": [cls._editor_row(row) for row in rows]}

    @classmethod
    async def get_editor_for_admin(cls, editor_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version, extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id WHERE e.id=$1""",
                editor_id,
            )
        return cls._editor_row(row) if row else None

    @classmethod
    async def query_editor_request_logs_admin(cls, editor_id: str, *, limit: int = 100, offset: int = 0) -> dict:
        return await cls.query_editor_request_logs(editor_id, "__admin__", limit=limit, offset=offset)

    @classmethod
    async def list_editor_summaries_for_projects(
        cls, project_ids: list[str]
    ) -> dict[str, list[dict]]:
        ids = [str(item) for item in project_ids if item]
        if not ids:
            return {}
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT e.id, e.project_id, e.name, e.provider, e.status,
                    (SELECT count(*) FROM editor_sessions s
                     WHERE s.editor_id=e.id) AS session_count,
                    COALESCE((SELECT jsonb_agg(jsonb_build_object(
                        'id', active.id,
                        'status', active.status,
                        'model', active.model,
                        'task_name', active.task_name,
                        'created_at', active.created_at
                    ) ORDER BY active.created_at DESC)
                    FROM editor_sessions active
                    WHERE active.editor_id=e.id), '[]'::jsonb) AS sessions
                FROM editors e
                WHERE e.project_id=ANY($1::text[])
                    AND e.status <> 'deleted'
                ORDER BY e.created_at""",
                ids,
            )
        result: dict[str, list[dict]] = {}
        for row in rows:
            data = dict(row)
            data["session_count"] = int(data.get("session_count") or 0)
            # asyncpg returns jsonb_agg as a JSON string unless a codec is set;
            # normalize to a real list so callers/clients always get an array.
            raw_sessions = data.get("sessions")
            if isinstance(raw_sessions, str):
                try:
                    raw_sessions = json.loads(raw_sessions)
                except (TypeError, ValueError):
                    raw_sessions = []
            data["sessions"] = raw_sessions if isinstance(raw_sessions, list) else []
            result.setdefault(str(data["project_id"]), []).append(data)
        return result

    @classmethod
    async def list_project_editors(
        cls, project_id: str, *, include_deleted: bool = False
    ) -> list[dict]:
        status_clause = "" if include_deleted else " AND e.status <> 'deleted'"
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.project_id=$1{status_clause}
                ORDER BY e.created_at DESC""",
                project_id,
            )
        return [cls._editor_row(row) for row in rows]

    @classmethod
    async def get_project_editor_by_id(
        cls, editor_id: str, project_id: str
    ) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.id=$1 AND e.project_id=$2 AND e.status <> 'deleted'""",
                editor_id, project_id,
            )
        return cls._editor_row(row) if row else None

    @classmethod
    async def list_project_editors_for_user(
        cls, project_id: str, owner_user_id: str, *, include_deleted: bool = False
    ) -> list[dict]:
        status_clause = "" if include_deleted else " AND e.status <> 'deleted'"
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.project_id=$1 AND e.owner_user_id=$2{status_clause}
                ORDER BY e.created_at DESC""",
                project_id,
                str(owner_user_id),
            )
        return [cls._editor_row(row) for row in rows]

    @classmethod
    async def get_editor_for_user_project(
        cls, editor_id: str, project_id: str, owner_user_id: str
    ) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.id=$1 AND e.project_id=$2 AND e.owner_user_id=$3
                    AND e.status <> 'deleted'""",
                editor_id, project_id, str(owner_user_id),
            )
        return cls._editor_row(row) if row else None

    @classmethod
    async def duplicate_editor_for_user(
        cls, editor_id: str, project_id: str, owner_user_id: str
    ) -> dict | None:
        # Key 反转后编辑器不再持有 Key（Key 归属 session）；复制只克隆编辑器配置。
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.name, e.provider, e.branch, e.workdir, e.node_id,
                    e.mcp_config, e.skill_config, e.plugin_config
                FROM editors e
                WHERE e.id=$1 AND e.project_id=$2 AND e.owner_user_id=$3 AND e.status='active'""",
                editor_id, project_id, str(owner_user_id),
            )
        if not row:
            return None
        clone = {
            "owner_user_id": str(owner_user_id), "provider": row["provider"],
            "project_id": project_id, "branch": row["branch"], "workdir": row["workdir"],
            "node_id": row["node_id"], "name": f"{row['name'] or editor_id} copy",
            "mcp_config": cls._loads_json(row["mcp_config"], []),
            "skill_config": cls._loads_json(row["skill_config"], []),
            "plugin_config": cls._loads_json(row["plugin_config"], []),
        }
        return await cls.create_editor_only(clone)

    @classmethod
    async def get_project_editor(cls, project_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.project_id=$1 AND e.status='active'
                ORDER BY e.created_at DESC LIMIT 1""",
                project_id,
            )
        return cls._editor_row(row) if row else None

    @classmethod
    async def get_project_editor_for_user(
        cls, project_id: str, owner_user_id: str
    ) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.project_id=$1 AND e.owner_user_id=$2 AND e.status='active'
                ORDER BY e.created_at DESC LIMIT 1""",
                project_id,
                str(owner_user_id),
            )
        return cls._editor_row(row) if row else None

    @classmethod
    async def count_editors_by_node(cls, node_ids: list[str] | None = None) -> dict[str, int]:
        """Return static non-deleted editor assignments grouped by execution node."""
        conditions = ["node_id IS NOT NULL", "status <> 'deleted'"]
        args: list = []
        if node_ids is not None:
            ids = [str(node_id) for node_id in node_ids if node_id]
            if not ids:
                return {}
            args.append(ids)
            conditions.append("node_id = ANY($1::text[])")
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT node_id, count(*) AS editor_count FROM editors WHERE {' AND '.join(conditions)} GROUP BY node_id",
                *args,
            )
        return {str(row["node_id"]): int(row["editor_count"] or 0) for row in rows}

    @classmethod
    async def list_editors_for_user(cls, owner_user_id: str) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e
                LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.owner_user_id=$1 AND e.status <> 'deleted'
                ORDER BY e.created_at DESC""",
                str(owner_user_id),
            )
        return [cls._editor_row(row) for row in rows]

    @classmethod
    async def get_editor_for_user(cls, editor_id: str, owner_user_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT e.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version,
                    extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled
                FROM editors e
                LEFT JOIN api_keys k ON k.id=e.api_key_id
                WHERE e.id=$1 AND e.owner_user_id=$2 AND e.status <> 'deleted'""",
                editor_id,
                str(owner_user_id),
            )
        return cls._editor_row(row) if row else None

    @classmethod
    def _editor_row(cls, row) -> dict:
        editor = dict(row)
        key_id = editor.get("api_key_id")
        if key_id:
            usage_limit = editor.pop("api_key_usage_limit", {})
            editor["api_key_copy"] = {
                "id": key_id,
                "name": editor.pop("api_key_name", "") or "",
                "key_masked": (
                    f"{key_value[:7]}...{key_value[-4:]}"
                    if len(key_value := str(editor.pop("api_key_value", "") or "")) > 12
                    else key_value
                ),
                "version": editor.pop("api_key_version", None),
                "expires_at": editor.pop("api_key_expires_at", None),
                "usage_limit": cls._loads_json(usage_limit, {}) or {},
                "disabled": bool(editor.pop("api_key_disabled", False)),
            }
        else:
            for field in (
                "api_key_name", "api_key_version", "api_key_expires_at",
                "api_key_usage_limit", "api_key_disabled",
            ):
                editor.pop(field, None)
        return editor

    @classmethod
    async def update_editor_for_user(cls, editor_id: str, owner_user_id: str, patch: dict) -> dict | None:
        allowed = {
            "name", "project_id", "branch", "workdir", "node_id",
            "mcp_config", "skill_config", "plugin_config", "prompt_id",
        }
        values = {key: value for key, value in patch.items() if key in allowed}
        if not values:
            return await cls.get_editor_for_user(editor_id, owner_user_id)
        sets = []
        args = []
        for key, value in values.items():
            args.append(cls._dumps(value) if key.endswith("_config") else value)
            cast = "::jsonb" if key.endswith("_config") else ""
            sets.append(f"{key}=${len(args)}{cast}")
        args.extend([editor_id, str(owner_user_id)])
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""UPDATE editors SET {', '.join(sets)}, updated_at=now()
                WHERE id=${len(args)-1} AND owner_user_id=${len(args)} AND status='active'
                RETURNING id""",
                *args,
            )
        if not row:
            return None
        return await cls.get_editor_for_user(editor_id, owner_user_id)

    @classmethod
    async def create_editor_only(cls, editor: dict) -> dict:
        """Create an editor without provisioning a runtime API key."""
        provider = str(editor.get("provider") or "").strip().lower()
        if provider not in {"claude", "codex", "opencode", "cursor"}:
            raise ValueError("unsupported editor provider")
        editor_id = str(editor.get("id") or f"ed_{uuid.uuid4().hex[:20]}")
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                project_id = str(editor.get("project_id") or "").strip()
                if project_id:
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"editor-project:{project_id}")
                row = await conn.fetchrow(
                    """INSERT INTO editors(
                        id, owner_user_id, name, provider, project_id, branch, branch_mode, workdir, node_id,
                        api_key_id, mcp_config, skill_config, plugin_config, prompt_id
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,$10::jsonb,$11::jsonb,$12::jsonb,$13)
                    RETURNING *""",
                    editor_id, str(editor["owner_user_id"]), editor.get("name") or editor_id, provider,
                    editor.get("project_id"), editor.get("branch"), editor.get("branch_mode") or "default",
                    editor.get("workdir"), editor.get("node_id"),
                    cls._dumps(editor.get("mcp_config") or []), cls._dumps(editor.get("skill_config") or []),
                    cls._dumps(editor.get("plugin_config") or []), editor.get("prompt_id"),
                )
        return dict(row) if row else {**editor, "id": editor_id, "api_key_id": None}

    @classmethod
    async def _create_child_api_key(
        cls, conn, parent_key_id: int, *, name: str, usage_limit: dict | None = None,
        expires_at=None, provider: str | None = None, scope: str = "editor",
        rate_limit: dict | None = None,
        model_whitelist: list[str] | None = None,
        editor_provider_whitelist: list[str] | None = None,
        selection_strategy: str | None = None,
    ) -> dict:
        """Clone a parent key for a scoped child and return its plaintext once.

        ``scope='editor'`` keeps the historical editor-session behavior: provider
        is validated against the parent's editor_provider_*list. ``scope='task'``
        validates provider against the parent's regular provider_*list instead.
        The scope literal is written to ``api_keys.scope`` so each validator can
        recognize its own children without a polymorphic attribution column.

        ``rate_limit`` overrides the inherited parent throttle. It is deliberately
        NOT narrowed against the parent: rate_limit counts per key in Redis and
        never rolls up, so it is this key's own throttle (quota semantics live in
        ``usage_limit``, which does roll up and is capped).

        ``model_whitelist`` / ``editor_provider_whitelist`` / ``selection_strategy``
        override the inherited parent values when non-None (task-scope 收窄);
        None = inherit parent verbatim so editor-scope callers are unaffected.
        Whitelists are narrowed against the parent (subset; 父为空=不限则原样).
        """
        scope = (scope or "editor").strip().lower()
        if scope not in ("editor", "task"):
            raise ValueError(f"unsupported child key scope: {scope}")
        parent = await conn.fetchrow("SELECT * FROM api_keys WHERE id=$1 AND disabled=false", parent_key_id)
        if not parent:
            raise ValueError("parent api key not found")
        provider = str(provider or "").strip().lower()
        if scope == "editor":
            whitelist = cls._normalize_editor_providers(parent["editor_provider_whitelist"])
            blacklist = cls._normalize_editor_providers(parent["editor_provider_blacklist"])
            if provider and whitelist and provider not in whitelist:
                raise ValueError("父 API Key 不允许用于该编辑器客户端")
            if provider and provider in blacklist:
                raise ValueError("父 API Key 禁止用于该编辑器客户端")
        else:
            whitelist = cls._normalize_editor_providers(parent["provider_whitelist"])
            blacklist = cls._normalize_editor_providers(parent["provider_blacklist"])
            if provider and whitelist and provider not in whitelist:
                raise ValueError("父 API Key 不允许用于该任务 provider")
            if provider and provider in blacklist:
                raise ValueError("父 API Key 禁止用于该任务 provider")
        child_usage_limit = parent["usage_limit"] or {} if usage_limit is None else cls._normalize_usage_limit(usage_limit)
        # 入参 expires_at 可能是 epoch 秒（会话额度）或父 Key 的 datetime；统一成 datetime 写 TIMESTAMPTZ。
        supplied_expires = cls._epoch_to_seconds(expires_at) if not hasattr(expires_at, "timestamp") else expires_at
        if supplied_expires is None:
            child_expires_at = parent["expires_at"]
        elif hasattr(supplied_expires, "timestamp"):
            child_expires_at = supplied_expires
        else:
            child_expires_at = datetime.fromtimestamp(float(supplied_expires), tz=timezone.utc)
        key_value = f"sk-{secrets.token_urlsafe(32)}"
        # A supplied rate_limit replaces the inherited parent throttle wholesale;
        # None keeps the parent's so existing callers are unaffected.
        child_rate_limit = parent["rate_limit"] if rate_limit is None else dict(rate_limit)

        # 子 Key 白名单只能在父级范围内收窄。None 表示调用方未覆盖，继承父级；
        # 非 None 的空列表也按父级范围处理，避免把父白名单意外放宽为不限。
        def _clean_str_list(value: Any) -> list[str]:
            if not isinstance(value, (list, tuple, set)):
                return []
            return [str(item).strip() for item in value if str(item).strip()]

        parent_model_whitelist = _clean_str_list(parent["model_whitelist"])
        if model_whitelist is None:
            child_model_whitelist = parent_model_whitelist
        else:
            child_model_whitelist = _clean_str_list(model_whitelist)
            if parent_model_whitelist:
                extra = [
                    item for item in child_model_whitelist
                    if item not in set(parent_model_whitelist)
                ]
                if extra:
                    raise ValueError(
                        "子 Key 的模型白名单不能超出父 Key 范围，越权项："
                        + ", ".join(sorted(set(extra)))
                    )

        parent_editor_whitelist = cls._normalize_editor_providers(
            parent["editor_provider_whitelist"]
        )
        if editor_provider_whitelist is None:
            child_editor_whitelist = parent_editor_whitelist
        else:
            child_editor_whitelist = cls._normalize_editor_providers(
                editor_provider_whitelist
            )
            if parent_editor_whitelist:
                extra = [
                    item for item in child_editor_whitelist
                    if item not in set(parent_editor_whitelist)
                ]
                if extra:
                    raise ValueError(
                        "子 Key 的编辑器客户端白名单不能超出父 Key 范围，越权项："
                        + ", ".join(sorted(set(extra)))
                    )

        child_selection_strategy = selection_strategy or parent["selection_strategy"]
        if child_selection_strategy not in SELECTION_STRATEGIES:
            raise ValueError("不支持的选择策略")

        child = await conn.fetchrow(
            """INSERT INTO api_keys(
                key, name, rate_limit, provider_whitelist, provider_blacklist,
                editor_provider_whitelist, editor_provider_blacklist,
                model_whitelist, model_blacklist, selection_strategy,
                thinking_config, user_id, parent_id, expires_at, usage_limit,
                version, label, scope
            ) VALUES($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11::jsonb,$12,$13,$14,$15::jsonb,1,$16,$17)
            RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                editor_provider_whitelist, editor_provider_blacklist,
                model_whitelist, model_blacklist, selection_strategy, thinking_config,
                disabled, user_id, vm_id, group_ids, parent_id,
                extract(epoch from expires_at) AS expires_at, usage_limit, version, label, scope""",
            key_value, name, cls._dumps(child_rate_limit), cls._dumps(parent["provider_whitelist"]),
            cls._dumps(parent["provider_blacklist"]), cls._dumps(child_editor_whitelist),
            cls._dumps(parent["editor_provider_blacklist"]),
            cls._dumps(child_model_whitelist), cls._dumps(parent["model_blacklist"]), child_selection_strategy, cls._dumps(parent["thinking_config"]),
            parent["user_id"], parent_key_id, child_expires_at, cls._dumps(child_usage_limit),
            name, scope,
        )
        return dict(child)

    @classmethod
    async def create_task_child_api_key(
        cls,
        parent_key_id: int,
        *,
        task_id: str,
        usage_limit: dict | None = None,
        expires_at=None,
        provider: str | None = None,
        rate_limit: dict | None = None,
        model_whitelist: list[str] | None = None,
        editor_provider_whitelist: list[str] | None = None,
        selection_strategy: str | None = None,
    ) -> dict:
        """Mint a ``scope='task'`` child key for a canonical Task.

        Returns the child row with the plaintext ``key`` included once so the
        caller (task_service) can hand it to the node LLM config without ever
        persisting it.
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                return await cls._create_child_api_key(
                    conn, int(parent_key_id), name=f"task:{task_id}",
                    usage_limit=usage_limit, expires_at=expires_at,
                    provider=provider, scope="task", rate_limit=rate_limit,
                    model_whitelist=model_whitelist,
                    editor_provider_whitelist=editor_provider_whitelist,
                    selection_strategy=selection_strategy,
                )

    @classmethod
    async def update_task_key_model_whitelist(
        cls, key_id: int, model_whitelist: list[str] | None
    ) -> bool:
        """Update ONLY a task child key's model whitelist.

        The generic update_api_key rewrites omitted fields (name/rate_limit/
        thinking_config), so the task add-model flow must not use it. This
        targeted transaction also enforces the parent whitelist subset rule.
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT id, parent_id FROM api_keys WHERE id=$1", int(key_id)
                )
                if not row:
                    return False
                cleaned = cls._clean_key_ids(model_whitelist)
                if row["parent_id"]:
                    parent = await conn.fetchrow(
                        "SELECT model_whitelist FROM api_keys WHERE id=$1", int(row["parent_id"])
                    )
                    if parent and parent["model_whitelist"]:
                        parent_whitelist = cls._clean_key_ids(parent["model_whitelist"])
                        cleaned = [model for model in cleaned if model in parent_whitelist]
                await conn.execute(
                    "UPDATE api_keys SET model_whitelist=$2::jsonb WHERE id=$1",
                    int(key_id), cls._dumps(cleaned),
                )
        return True


    @classmethod
    async def create_review_child_api_key(
        cls,
        parent_key_id: int,
        *,
        event_id: str,
        usage_limit: dict | None = None,
        provider: str | None = None,
    ) -> dict:
        """Mint a one-off child key for a webhook review run.

        Reuses the same derivation as an editor session (``_create_child_api_key``),
        so the ``usage_limit`` is a server-side hard cap and ``request_logs`` stay
        attributable to the parent key. Returns the child row (plaintext ``key``
        included once) for planting in the node session's LLM config.
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                return await cls._create_child_api_key(
                    conn, int(parent_key_id), name=f"review:{event_id}",
                    usage_limit=usage_limit, provider=provider,
                )

    @classmethod
    async def create_editor_session(
        cls,
        editor_id: str,
        model: str | None = None,
        expected_client_id: str | None = None,
        bootstrap_content_hash: str | None = None,
        *,
        parent_api_key_id: int | None = None,
        usage_limit: dict | None = None,
        expires_at=None,
        models: list[str] | None = None,
        task_name: str | None = None,
        task_type: str | None = None,
        task_role: str | None = None,
        sub_type: str | None = None,
        issue_id: str | None = None,
        mode: str | None = None,
    ) -> dict | None:
        session_id = f"es_{uuid.uuid4().hex[:20]}"
        selected_models = [str(item).strip() for item in (models or []) if str(item).strip()]
        if model and model.strip() and model.strip() not in selected_models:
            selected_models.insert(0, model.strip())
        active_model = (selected_models[0] if selected_models else (model or "").strip()) or None
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                editor_row = await conn.fetchrow("SELECT provider, name FROM editors WHERE id=$1 AND status='active' FOR UPDATE", editor_id)
                if not editor_row:
                    return None
                if expected_client_id and bootstrap_content_hash:
                    existing = await conn.fetchrow(
                        """SELECT id FROM editor_sessions WHERE editor_id=$1 AND expected_client_id=$2
                        AND bootstrap_content_hash=$3 AND status='pending_first_request' AND bootstrap_consumed=false LIMIT 1""",
                        editor_id, expected_client_id, bootstrap_content_hash,
                    )
                    if existing:
                        raise ValueError("duplicate pending editor session bootstrap")
                key_id = None
                key_copy = None
                if parent_api_key_id:
                    key_copy = await cls._create_child_api_key(
                        conn, int(parent_api_key_id), name=f"session:{session_id}",
                        usage_limit=usage_limit, expires_at=expires_at, provider=editor_row["provider"],
                    )
                    key_id = key_copy["id"]
                row = await conn.fetchrow(
                    """INSERT INTO editor_sessions(
                        id, editor_id, model, models_json, api_key_id, status,
                        expected_client_id, bootstrap_content_hash,
                        task_name, task_type, task_role, sub_type, issue_id, mode
                    ) VALUES($1,$2,$3,$4::jsonb,$5,'provisioning',$6,$7,$8,$9,$10,$11,$12::uuid,$13) RETURNING *""",
                    session_id, editor_id, active_model, cls._dumps(selected_models), key_id,
                    (expected_client_id or "").strip() or None, (bootstrap_content_hash or "").strip() or None,
                    (task_name or "").strip() or None, (task_type or "").strip() or None,
                    (task_role or "").strip() or None, (sub_type or "").strip() or None,
                    (issue_id or "").strip() or None, (mode or "").strip() or None,
                )
        result = dict(row) if row else None
        if result and key_copy:
            result["api_key_copy"] = cls._api_key_row(key_copy)
        return result

    @classmethod
    def _editor_session_row(cls, row) -> dict:
        session = dict(row)
        models = cls._loads_json(session.get("models_json"), [])
        session["models_json"] = models if isinstance(models, list) else []
        overlay = cls._loads_json(session.get("mcp_overlay_json"), [])
        session["mcp_overlay_json"] = overlay if isinstance(overlay, list) else []
        key_id = session.get("api_key_id")
        if key_id:
            key_value = str(session.pop("api_key_value", "") or "")
            session["api_key_copy"] = {
                "id": key_id,
                "name": session.pop("api_key_name", "") or "",
                "key_masked": f"{key_value[:7]}...{key_value[-4:]}" if len(key_value) > 12 else key_value,
                "version": session.pop("api_key_version", None),
                "expires_at": session.pop("api_key_expires_at", None),
                "usage_limit": cls._loads_json(session.pop("api_key_usage_limit", {}), {}) or {},
                "disabled": bool(session.pop("api_key_disabled", False)),
            }
        else:
            for field in (
                "api_key_value", "api_key_name", "api_key_version",
                "api_key_expires_at", "api_key_usage_limit", "api_key_disabled",
            ):
                session.pop(field, None)
        return session

    @classmethod
    async def list_editor_sessions_for_user(
        cls, owner_user_id: str, *, limit: int = 100, offset: int = 0
    ) -> dict:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        async with cls.pool.acquire() as conn:
            total = await conn.fetchval(
                """SELECT count(*) FROM editor_sessions s
                JOIN editors e ON e.id=s.editor_id
                WHERE e.owner_user_id=$1 AND e.status <> 'deleted'""",
                str(owner_user_id),
            )
            rows = await conn.fetch(
                """SELECT s.*, e.name AS editor_name, e.provider AS editor_provider,
                    e.project_id AS editor_project_id,
                    k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version, extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled,
                    COALESCE((SELECT sum(l.total_tokens) FROM request_logs l WHERE l.editor_session_id=s.id), 0)::bigint AS total_tokens
                FROM editor_sessions s JOIN editors e ON e.id=s.editor_id
                LEFT JOIN api_keys k ON k.id=s.api_key_id
                WHERE e.owner_user_id=$1 AND e.status <> 'deleted'
                ORDER BY s.created_at DESC LIMIT $2 OFFSET $3""",
                str(owner_user_id), limit, offset,
            )
        return {"total": int(total or 0), "rows": [cls._editor_session_row(row) for row in rows]}

    @classmethod
    async def list_editor_sessions(cls, editor_id: str) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version, extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled,
                    COALESCE((SELECT sum(l.total_tokens) FROM request_logs l WHERE l.editor_session_id=s.id), 0)::bigint AS total_tokens
                FROM editor_sessions s LEFT JOIN api_keys k ON k.id=s.api_key_id
                WHERE s.editor_id=$1 ORDER BY s.created_at DESC""",
                editor_id,
            )
        return [cls._editor_session_row(row) for row in rows]

    @classmethod
    async def get_editor_session(cls, editor_id: str, session_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT s.*, k.key AS api_key_value, k.name AS api_key_name,
                    k.version AS api_key_version, extract(epoch from k.expires_at) AS api_key_expires_at,
                    k.usage_limit AS api_key_usage_limit, k.disabled AS api_key_disabled,
                    COALESCE((SELECT sum(l.total_tokens) FROM request_logs l WHERE l.editor_session_id=s.id), 0)::bigint AS total_tokens
                FROM editor_sessions s LEFT JOIN api_keys k ON k.id=s.api_key_id
                WHERE s.editor_id=$1 AND s.id=$2""",
                editor_id, session_id,
            )
        return cls._editor_session_row(row) if row else None

    @classmethod
    async def delete_editor_session(cls, editor_id: str, session_id: str) -> bool:
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT api_key_id FROM editor_sessions WHERE editor_id=$1 AND id=$2 FOR UPDATE",
                    editor_id,
                    session_id,
                )
                if not row:
                    return False
                # Request logs intentionally remain as immutable audit snapshots.
                # Their editor_session_id is not a foreign key, so deleting the
                # mutable session record does not erase request history.
                await conn.execute(
                    "DELETE FROM editor_sessions WHERE editor_id=$1 AND id=$2",
                    editor_id,
                    session_id,
                )
                if row["api_key_id"]:
                    await conn.execute("DELETE FROM api_keys WHERE id=$1", row["api_key_id"])
        return True

    @classmethod
    async def get_active_editor_session_for_issue(
        cls, issue_id: str, task_role: str
    ) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT * FROM editor_sessions
                WHERE issue_id=$1::uuid AND task_role=$2
                  AND status NOT IN ('closed', 'error')
                ORDER BY created_at DESC LIMIT 1""",
                issue_id, task_role,
            )
        return dict(row) if row else None

    @classmethod
    async def close_editor(cls, editor_id: str, owner_user_id: str) -> bool:
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("UPDATE editors SET status='deleted', updated_at=now() WHERE id=$1 AND owner_user_id=$2 AND status <> 'deleted' RETURNING api_key_id", editor_id, str(owner_user_id))
                if not row:
                    return False
                # session 自持 key（Key 反转后）：删编辑器时一并回收所有 session key，避免悬挂凭据。
                session_key_ids = [
                    r["api_key_id"]
                    for r in await conn.fetch("SELECT api_key_id FROM editor_sessions WHERE editor_id=$1 AND api_key_id IS NOT NULL", editor_id)
                ]
                await conn.execute("UPDATE editor_sessions SET status='closed', closed_at=COALESCE(closed_at, now()), updated_at=now() WHERE editor_id=$1 AND status <> 'closed'", editor_id)
                await conn.execute("UPDATE editors SET api_key_id=NULL, status='deleted', updated_at=now() WHERE id=$1 AND owner_user_id=$2", editor_id, str(owner_user_id))
                if row["api_key_id"]:
                    await conn.execute("DELETE FROM api_keys WHERE id=$1", row["api_key_id"])
                if session_key_ids:
                    await conn.execute("DELETE FROM api_keys WHERE id = ANY($1::int[])", session_key_ids)
        return True

    @classmethod
    async def set_editor_session_mcp_overlay(
        cls, editor_id: str, session_id: str, overlay: list[dict] | None
    ) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE editor_sessions
                SET mcp_overlay_json=$1::jsonb, updated_at=now()
                WHERE editor_id=$2 AND id=$3
                RETURNING *""",
                cls._dumps(overlay or []), editor_id, session_id,
            )
        return dict(row) if row else None

    @classmethod
    async def bind_editor_session_node(cls, editor_id: str, session_id: str, node_session_id: str, expected_client_id: str | None = None) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("""UPDATE editor_sessions SET node_session_id=$1, expected_client_id=$2, status='pending_first_request', updated_at=now() WHERE editor_id=$3 AND id=$4 AND status='provisioning' RETURNING *""", node_session_id, expected_client_id, editor_id, session_id)
        return dict(row) if row else None

    @classmethod
    async def bind_provider_thread(cls, session_id: str, provider_thread_id: str, request_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", provider_thread_id)
                row = await conn.fetchrow(
                    """UPDATE editor_sessions SET provider_thread_id=$1, first_request_seen=true, first_request_id=$2, status='active', bootstrap_consumed=true, last_request_at=now(), updated_at=now() WHERE id=$3 AND status='pending_first_request' AND first_request_seen=false AND bootstrap_consumed=false AND provider_thread_id IS NULL AND NOT EXISTS (SELECT 1 FROM editor_sessions WHERE provider_thread_id=$1) RETURNING *""",
                    provider_thread_id,
                    request_id,
                    session_id,
                )
        return dict(row) if row else None

    @classmethod
    async def get_task_by_api_key_id(cls, api_key_id: int) -> dict | None:
        """Resolve the canonical Task that currently owns a ``scope='task'`` key.

        Ownership is external (``mc_tasks.api_key_id -> api_keys.id``); a disabled
        key row is still resolved so the validator can surface the right reason
        (disabled / expired) instead of a bare 403.
        """
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT t.id, t.user_id, t.provider, t.api_key_id, t.parent_api_key_id,
                          t.provider_thread_id, t.expected_client_id, t.bootstrap_content_hash,
                          COALESCE(t.first_request_seen, false) AS first_request_seen,
                          COALESCE(t.bootstrap_consumed, false) AS bootstrap_consumed,
                          t.status, t.deleted_at
                   FROM mc_tasks t
                   WHERE t.api_key_id=$1 AND t.deleted_at IS NULL""",
                api_key_id,
            )
        return dict(row) if row else None

    @classmethod
    async def bind_task_provider_thread(
        cls, task_id: str, provider_thread_id: str, request_id: str
    ) -> dict | None:
        """Atomically bind a provider-native thread to a canonical Task.

        Mirrors ``bind_provider_thread`` for editor sessions: advisory lock on
        the thread id, conditional UPDATE requiring a pending bootstrap, no
        prior binding, and no other Task owning the same thread. Returns the
        updated row on success, ``None`` on a lost race.
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", provider_thread_id)
                row = await conn.fetchrow(
                    """UPDATE mc_tasks
                       SET provider_thread_id=$1,
                           first_request_id=$2,
                           first_request_seen=true,
                           bootstrap_consumed=true,
                           status='processing',
                           last_request_at=now(),
                           updated_at=now()
                       WHERE id=$3::uuid
                         AND COALESCE(first_request_seen, false) = false
                         AND COALESCE(bootstrap_consumed, false) = false
                         AND provider_thread_id IS NULL
                         AND deleted_at IS NULL
                         AND NOT EXISTS (
                             SELECT 1 FROM mc_tasks WHERE provider_thread_id=$1
                         )
                       RETURNING *""",
                    provider_thread_id,
                    request_id,
                    str(task_id),
                )
        return dict(row) if row else None

    @classmethod
    async def disable_task_api_key(cls, task_id: str, owner_user_id: str) -> dict | None:
        """Disable the current ``scope='task'`` child key of an owned Task.

        The key row is retained (disabled) so historical request-log lineage
        remains resolvable; only ``Task.api_key_id`` is cleared.
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """UPDATE mc_tasks SET api_key_id=NULL, updated_at=now()
                       WHERE id=$1::uuid AND user_id=$2::uuid AND deleted_at IS NULL
                       RETURNING api_key_id""",
                    str(task_id), str(owner_user_id),
                )
                if not row or not row["api_key_id"]:
                    return None
                await conn.execute(
                    "UPDATE api_keys SET disabled=true WHERE id=$1",
                    row["api_key_id"],
                )
        return {"task_id": str(task_id), "key_id": row["api_key_id"], "disabled": True}

    @classmethod
    async def rotate_task_api_key(
        cls,
        task_id: str,
        owner_user_id: str,
        *,
        usage_limit: dict | None = None,
        expires_at=None,
    ) -> dict | None:
        """Stage a replacement ``scope='task'`` key for an owned Task.

        Creates the new child (same parent/filter/provider), atomically moves
        ``Task.api_key_id`` to it, and leaves the old key enabled. The caller is
        responsible for pushing the new LLM config to the live node session and
        only then disabling the old key via ``disable_api_key_by_id``; on
        node-config failure the caller disables the new key and restores the
        old pointer (see ``restore_task_api_key_id``).
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                task = await conn.fetchrow(
                    """SELECT api_key_id, parent_api_key_id, provider
                       FROM mc_tasks
                       WHERE id=$1::uuid AND user_id=$2::uuid AND deleted_at IS NULL
                       FOR UPDATE""",
                    str(task_id), str(owner_user_id),
                )
                if not task or not task["parent_api_key_id"]:
                    return None
                old_key_id = task["api_key_id"]
                replacement = await cls._create_child_api_key(
                    conn, int(task["parent_api_key_id"]),
                    name=f"task:{task_id}",
                    usage_limit=usage_limit, expires_at=expires_at,
                    provider=task["provider"], scope="task",
                )
                await conn.execute(
                    "UPDATE mc_tasks SET api_key_id=$1, updated_at=now() WHERE id=$2::uuid",
                    replacement["id"], str(task_id),
                )
                return {
                    "task_id": str(task_id),
                    "old_key_id": old_key_id,
                    "new_key_id": replacement["id"],
                    "key": replacement["key"],
                    "version": replacement["version"],
                    "key_masked": f"{replacement['key'][:7]}...{replacement['key'][-4:]}",
                    "expires_at": replacement.get("expires_at"),
                    "usage_limit": cls._loads_json(replacement.get("usage_limit"), {}) or {},
                }

    @classmethod
    async def disable_api_key_by_id(cls, key_id: int) -> bool:
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE api_keys SET disabled=true WHERE id=$1 AND disabled=false",
                key_id,
            )
        return result == "UPDATE 1"

    @classmethod
    async def set_api_key_disabled_by_id(cls, key_id: int, disabled: bool) -> bool:
        """Flip a key's ``disabled`` flag; True only when the row actually changed.

        The task lifecycle parks its child key on finished/error and resumes it
        on restart (``TaskService._park_task_api_key``). The row-change return
        lets those paths skip the gateway snapshot refresh when nothing flipped.
        """
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE api_keys SET disabled=$2 WHERE id=$1 AND disabled IS DISTINCT FROM $2",
                key_id, disabled,
            )
        return result == "UPDATE 1"

    @classmethod
    async def restore_task_api_key_id(
        cls, task_id: str, owner_user_id: str, key_id: int
    ) -> bool:
        """Point a Task back at a prior key after a staged rotation failed."""
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                """UPDATE mc_tasks SET api_key_id=$1, updated_at=now()
                   WHERE id=$2::uuid AND user_id=$3::uuid AND deleted_at IS NULL""",
                key_id, str(task_id), str(owner_user_id),
            )
        return result == "UPDATE 1"

    @classmethod
    async def reap_stale_editor_sessions(
        cls, *, pending_timeout_seconds: int = 1800, idle_timeout_seconds: int = 86400
    ) -> list[dict]:
        """Close sessions that never bootstrapped or have gone idle.

        The rows are retained for request-log and provider-history ownership;
        callers may release their node runtimes after this atomic ledger update.
        """
        pending_timeout_seconds = max(60, int(pending_timeout_seconds))
        idle_timeout_seconds = max(300, int(idle_timeout_seconds))
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """UPDATE editor_sessions
                SET status='closed', closed_at=COALESCE(closed_at, now()), updated_at=now()
                WHERE (status='pending_first_request'
                    AND created_at < now() - ($1::int * interval '1 second'))
                   OR (status='active'
                    AND COALESCE(last_request_at, created_at)
                        < now() - ($2::int * interval '1 second'))
                RETURNING id, editor_id, node_session_id, status, closed_at""",
                pending_timeout_seconds,
                idle_timeout_seconds,
            )
        return [dict(row) for row in rows]

    async def update_editor_session_metadata(
        cls,
        editor_id: str,
        session_id: str,
        *,
        task_name: str | None = None,
        models: list[str] | None = None,
        usage_limit: dict | None = None,
        expires_at=None,
    ) -> dict | None:
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT api_key_id FROM editor_sessions WHERE editor_id=$1 AND id=$2 FOR UPDATE", editor_id, session_id)
                if not row:
                    return None
                updated = await conn.fetchrow(
                    """UPDATE editor_sessions SET
                        task_name=COALESCE($1, task_name),
                        models_json=COALESCE($2::jsonb, models_json),
                        updated_at=now()
                    WHERE editor_id=$3 AND id=$4 RETURNING *""",
                    task_name.strip() if task_name is not None else None,
                    cls._dumps([str(item).strip() for item in (models or []) if str(item).strip()]) if models is not None else None,
                    editor_id,
                    session_id,
                )
                if row["api_key_id"] and (usage_limit is not None or expires_at is not None):
                    await cls.update_api_key(row["api_key_id"], {
                        "usage_limit": usage_limit or {},
                        "expires_at": expires_at,
                    })
        return dict(updated) if updated else None

    @classmethod
    async def set_editor_session_model(
        cls, editor_id: str, session_id: str, model: str
    ) -> dict | None:
        """Switch a session's model within its allowed models set."""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE editor_sessions SET model=$1, updated_at=now()
                WHERE editor_id=$2 AND id=$3
                    AND status IN ('pending_first_request', 'active')
                    AND ($1 = ANY(
                        SELECT jsonb_array_elements_text(
                            COALESCE(models_json, '[]'::jsonb)
                        )
                    ) OR models_json IS NULL OR models_json = '[]'::jsonb)
                RETURNING *""",
                model,
                editor_id,
                session_id,
            )
        return dict(row) if row else None

    @classmethod
    async def set_editor_session_mode(
        cls, editor_id: str, session_id: str, mode: str
    ) -> dict | None:
        """Persist the provider-native mode; the node applies it on next turn."""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE editor_sessions SET mode=$1, updated_at=now()
                WHERE editor_id=$2 AND id=$3
                    AND status IN ('pending_first_request', 'active')
                RETURNING *""",
                mode,
                editor_id,
                session_id,
            )
        return dict(row) if row else None

    @classmethod
    async def set_editor_session_status(cls, editor_id: str, session_id: str, status: str) -> dict | None:
        allowed = {"provisioning", "pending_first_request", "active", "closed", "error"}
        if status not in allowed:
            raise ValueError("invalid editor session status")
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("UPDATE editor_sessions SET status=$1, updated_at=now(), closed_at=CASE WHEN $1='closed' THEN COALESCE(closed_at, now()) ELSE closed_at END WHERE editor_id=$2 AND id=$3 RETURNING *", status, editor_id, session_id)
        return dict(row) if row else None

    @classmethod
    async def add_api_key(cls, data: dict) -> dict:
        strategy = str(data.get("selection_strategy") or DEFAULT_SELECTION_STRATEGY).strip()
        if strategy not in SELECTION_STRATEGIES:
            strategy = DEFAULT_SELECTION_STRATEGY
        expires_at = cls._epoch_to_seconds(data.get("expires_at"))
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO api_keys(key, name, rate_limit, provider_whitelist, provider_blacklist,
                    editor_provider_whitelist, editor_provider_blacklist,
                    model_whitelist, model_blacklist, selection_strategy, thinking_config,
                    usage_limit, expires_at)
                VALUES($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10, $11::jsonb,
                    $12::jsonb, CASE WHEN $13::double precision IS NULL THEN NULL ELSE to_timestamp($13::double precision) END)
                RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                    editor_provider_whitelist, editor_provider_blacklist, model_whitelist,
                    model_blacklist, selection_strategy, thinking_config, disabled, usage_limit,
                    extract(epoch from expires_at) AS expires_at""",
                f"sk-{secrets.token_urlsafe(32)}",
                data.get("name", ""),
                cls._dumps(data.get("rate_limit", {})),
                cls._dumps(data.get("provider_whitelist", [])),
                cls._dumps(data.get("provider_blacklist", [])),
                cls._dumps(cls._normalize_editor_providers(data.get("editor_provider_whitelist", []))),
                cls._dumps(cls._normalize_editor_providers(data.get("editor_provider_blacklist", []))),
                cls._dumps(data.get("model_whitelist", [])),
                cls._dumps(data.get("model_blacklist", [])),
                strategy,
                cls._dumps(data.get("thinking_config", {})),
                cls._dumps(cls._normalize_usage_limit(data.get("usage_limit"))),
                expires_at,
            )
            # 建 Key 时可顺带带上初始分组授权；关系落 api_key_groups，不写 api_keys。
            groups = cls._normalize_group_ids(data.get("group_ids", []))
            if groups:
                await conn.executemany(
                    "INSERT INTO api_key_groups(api_key_id, group_id) VALUES($1, $2) "
                    "ON CONFLICT DO NOTHING",
                    [(row["id"], gid) for gid in groups],
                )
        result = cls._api_key_row(row)
        result["group_ids"] = groups
        return result

    @staticmethod
    def _narrow_child_config(parent: dict, data: dict) -> dict:
        """把子 Key 的入参收窄到父 Key 范围内，越权直接拒绝。

        子 Key 存在的意义就是「在父的基础上收窄」——脱离父约束的子 Key 等同于新建一把
        Key，没有价值。所以这里强制：

        - 白名单：父若限定了范围，子必须是父的子集（子为空 = 继承父，不能放宽成"全部"）
        - 黑名单：子必须是父的超集（父禁的子必禁，子可以再多禁）
        - usage_limit / expires_at：子不得超过父（父无限/永不过期时子可自由设定）

        `rate_limit` 不做父子约束：它在 Redis 按单 key 独立计数、不向父汇总，语义是本
        key 的瞬时节流而非配额；配额语义由 usage_limit 承担（那个才汇总到父并强制）。

        `parent` 的 jsonb 字段须已解码为 Python 对象。越权时抛 ValueError（路由映射 400）。
        """
        result: dict = {}

        def clean_list(value) -> list[str]:
            if not isinstance(value, (list, tuple, set)):
                return []
            return [str(v).strip() for v in value if str(v).strip()]

        for field, label in (
            ("provider_whitelist", "渠道标签白名单"),
            ("model_whitelist", "模型白名单"),
        ):
            parent_values = clean_list(parent.get(field))
            child_values = clean_list(data.get(field)) if data.get(field) is not None else []
            if not child_values:
                result[field] = parent_values  # 缺省继承父，不放宽
                continue
            if parent_values:
                extra = [v for v in child_values if v not in set(parent_values)]
                if extra:
                    raise ValueError(
                        f"子 Key 的{label}不能超出父 Key 范围，越权项：{', '.join(sorted(extra))}"
                    )
            result[field] = child_values

        for field, label in (
            ("editor_provider_whitelist", "编辑器客户端白名单"),
        ):
            parent_values = cls._normalize_editor_providers(parent.get(field))
            child_values = cls._normalize_editor_providers(data.get(field)) if data.get(field) is not None else []
            if not child_values:
                result[field] = parent_values
                continue
            if parent_values:
                extra = [v for v in child_values if v not in set(parent_values)]
                if extra:
                    raise ValueError(
                        f"子 Key 的{label}不能超出父 Key 范围，越权项：{', '.join(sorted(extra))}"
                    )
            result[field] = child_values

        for field, label in (
            ("provider_blacklist", "渠道标签黑名单"),
            ("model_blacklist", "模型黑名单"),
        ):
            parent_values = clean_list(parent.get(field))
            child_values = clean_list(data.get(field)) if data.get(field) is not None else []
            # 父禁的子必禁：并集即可，子额外禁更多是允许的收窄。
            merged = list(dict.fromkeys(child_values + parent_values))
            result[field] = merged
            del label

        parent_editor_blacklist = cls._normalize_editor_providers(parent.get("editor_provider_blacklist"))
        child_editor_blacklist = (
            cls._normalize_editor_providers(data.get("editor_provider_blacklist"))
            if data.get("editor_provider_blacklist") is not None else []
        )
        result["editor_provider_blacklist"] = list(dict.fromkeys(child_editor_blacklist + parent_editor_blacklist))

        parent_usage = parent.get("usage_limit") if isinstance(parent.get("usage_limit"), dict) else {}
        child_usage_raw = data.get("usage_limit")
        if child_usage_raw is None:
            result["usage_limit"] = dict(parent_usage or {})
        else:
            if not isinstance(child_usage_raw, dict):
                raise ValueError("usage_limit 必须是对象")
            child_usage: dict = {}
            for sub, label in (("max_requests", "累计请求数"), ("max_total_tokens", "累计 Token 数")):
                parent_cap = int(parent_usage.get(sub) or 0)
                try:
                    child_cap = int(child_usage_raw.get(sub) or 0)
                except (TypeError, ValueError):
                    raise ValueError(f"usage_limit.{sub} 必须是整数") from None
                if child_cap < 0:
                    raise ValueError(f"usage_limit.{sub} 不能为负数")
                # 父有上限（>0）时，子必须给出不超过父的正值；父无限（0）则子任意。
                if parent_cap:
                    if not child_cap or child_cap > parent_cap:
                        raise ValueError(
                            f"子 Key 的{label}上限不能超过父 Key（父上限 {parent_cap}）"
                        )
                if child_cap:
                    child_usage[sub] = child_cap
            result["usage_limit"] = child_usage

        parent_expires = parent.get("expires_at")
        child_expires_raw = data.get("expires_at", "__absent__")
        if child_expires_raw == "__absent__":
            result["expires_at"] = parent_expires
        elif child_expires_raw in (None, "", 0):
            # 显式清空：父有过期时间时不允许子永不过期。
            if parent_expires is not None:
                raise ValueError("父 Key 有过期时间，子 Key 不能设为永不过期")
            result["expires_at"] = None
        else:
            try:
                child_epoch = float(child_expires_raw)
            except (TypeError, ValueError):
                raise ValueError("expires_at 必须是 epoch 秒") from None
            if parent_expires is not None:
                parent_epoch = (
                    parent_expires.timestamp()
                    if hasattr(parent_expires, "timestamp")
                    else float(parent_expires)
                )
                if child_epoch > parent_epoch:
                    raise ValueError("子 Key 的过期时间不能晚于父 Key")
            result["expires_at"] = child_epoch

        return result

    @classmethod
    async def copy_api_key(cls, parent_key_id: int, data: dict | None = None) -> dict | None:
        """按父子关系创建一枚子 Key（副本）。

        子 Key 与编辑器子 Key 同构：`parent_id` 指向父 Key，用量按
        `api_key_id OR api_key_parent_id` 汇总回父 Key。副本只签发新的明文，不复制父
        Key 的密文。名称缺省为「父名 (副本)」，可由入参覆盖。

        权限维度（白/黑名单、usage_limit、expires_at）由 `_narrow_child_config` 强制
        收窄到父范围内，越权抛 ValueError。未给出的字段继承父值。

        父 Key 自身是副本时（`parent_id` 非空）会拍平到其根 Key，避免多层链路让用量
        汇总口径出现歧义——与 `duplicate_editor_for_user` 的 `key_parent_id or key_id`
        取法一致。返回 None 表示父 Key 不存在。
        """
        data = data or {}
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                parent = await conn.fetchrow("SELECT * FROM api_keys WHERE id=$1", int(parent_key_id))
                if not parent:
                    return None
                root_id = parent["parent_id"] or parent["id"]
                name = str(data.get("name") or "").strip() or f"{parent['name'] or f'key-{root_id}'} (副本)"
                # 未注册 jsonb codec，asyncpg 取回的 jsonb 列是 JSON 文本；必须先解回
                # Python 对象再 _dumps，否则会写入双重编码的字符串，读出来不是数组/对象。
                parent_json = {
                    fld: cls._loads_json(parent[fld], default)
                    for fld, default in (
                        ("rate_limit", {}), ("thinking_config", {}), ("usage_limit", {}),
                        ("provider_whitelist", []), ("provider_blacklist", []),
                        ("editor_provider_whitelist", []), ("editor_provider_blacklist", []),
                        ("model_whitelist", []), ("model_blacklist", []),
                    )
                }
                narrowed = cls._narrow_child_config(
                    {**parent_json, "expires_at": parent["expires_at"]}, data
                )
                strategy = str(
                    data.get("selection_strategy") or parent["selection_strategy"] or DEFAULT_SELECTION_STRATEGY
                ).strip()
                if strategy not in SELECTION_STRATEGIES:
                    strategy = DEFAULT_SELECTION_STRATEGY
                # rate_limit 不受父子约束（Redis 按单 key 独立计数），子可自定义，缺省继承父。
                rate_limit = data.get("rate_limit")
                if not isinstance(rate_limit, dict):
                    rate_limit = parent_json["rate_limit"]
                expires_at = narrowed["expires_at"]
                row = await conn.fetchrow(
                    """INSERT INTO api_keys(
                        key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist,
                        model_whitelist, model_blacklist, selection_strategy, thinking_config,
                        user_id, vm_id, parent_id, expires_at, usage_limit, version, label, scope
                    ) VALUES($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11::jsonb,
                        $12,$13,$14,
                        CASE WHEN $15::double precision IS NULL THEN NULL ELSE to_timestamp($15::double precision) END,
                        $16::jsonb,1,$17,'copy')
                    RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist,
                        model_whitelist, model_blacklist, selection_strategy, thinking_config,
                        disabled, user_id, vm_id, group_ids, parent_id,
                        extract(epoch from expires_at) AS expires_at, usage_limit, version, label, scope,
                        extract(epoch from created_at) AS created_at""",
                    f"sk-{secrets.token_urlsafe(32)}",
                    name,
                    cls._dumps(rate_limit),
                    cls._dumps(narrowed["provider_whitelist"]),
                    cls._dumps(narrowed["provider_blacklist"]),
                    cls._dumps(narrowed["editor_provider_whitelist"]),
                    cls._dumps(narrowed["editor_provider_blacklist"]),
                    cls._dumps(narrowed["model_whitelist"]),
                    cls._dumps(narrowed["model_blacklist"]),
                    strategy,
                    cls._dumps(parent_json["thinking_config"]),
                    parent["user_id"],
                    parent["vm_id"],
                    root_id,
                    (
                        expires_at.timestamp()
                        if hasattr(expires_at, "timestamp")
                        else (float(expires_at) if expires_at is not None else None)
                    ),
                    cls._dumps(narrowed["usage_limit"]),
                    name,
                )
        return cls._api_key_row(row)

    @classmethod
    async def update_api_key(cls, key_id: int, data: dict) -> dict | None:
        strategy = str(data.get("selection_strategy") or DEFAULT_SELECTION_STRATEGY).strip()
        if strategy not in SELECTION_STRATEGIES:
            strategy = DEFAULT_SELECTION_STRATEGY
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                current = await conn.fetchrow("SELECT * FROM api_keys WHERE id=$1", int(key_id))
                if not current:
                    return None
                # 若被改的是子 Key，编辑同样必须收窄到父范围内——否则通过编辑放宽白名单
                # 就能绕过创建时的约束（请求时只兜底父的动态状态，不再重查白名单子集）。
                if current["parent_id"]:
                    parent = await conn.fetchrow("SELECT * FROM api_keys WHERE id=$1", int(current["parent_id"]))
                    if parent:
                        parent_json = {
                            fld: cls._loads_json(parent[fld], default)
                            for fld, default in (
                                ("usage_limit", {}),
                                ("provider_whitelist", []), ("provider_blacklist", []),
                                ("editor_provider_whitelist", []), ("editor_provider_blacklist", []),
                                ("model_whitelist", []), ("model_blacklist", []),
                            )
                        }
                        narrowed = cls._narrow_child_config(
                            {**parent_json, "expires_at": parent["expires_at"]}, data
                        )
                        provider_whitelist = narrowed["provider_whitelist"]
                        provider_blacklist = narrowed["provider_blacklist"]
                        editor_provider_whitelist = narrowed["editor_provider_whitelist"]
                        editor_provider_blacklist = narrowed["editor_provider_blacklist"]
                        model_whitelist = narrowed["model_whitelist"]
                        model_blacklist = narrowed["model_blacklist"]
                        usage_limit = narrowed["usage_limit"]
                        exp = narrowed["expires_at"]
                        expires_at = (
                            exp.timestamp() if hasattr(exp, "timestamp")
                            else (float(exp) if exp is not None else None)
                        )
                    else:
                        # 父已被删（parent_id 置 NULL 前的竞态）——按普通 Key 处理。
                        provider_whitelist = data.get("provider_whitelist", [])
                        provider_blacklist = data.get("provider_blacklist", [])
                        editor_provider_whitelist = cls._normalize_editor_providers(data.get("editor_provider_whitelist", []))
                        editor_provider_blacklist = cls._normalize_editor_providers(data.get("editor_provider_blacklist", []))
                        model_whitelist = data.get("model_whitelist", [])
                        model_blacklist = data.get("model_blacklist", [])
                        usage_limit = cls._normalize_usage_limit(data.get("usage_limit"))
                        expires_at = cls._epoch_to_seconds(data.get("expires_at"))
                else:
                    provider_whitelist = data.get("provider_whitelist", [])
                    provider_blacklist = data.get("provider_blacklist", [])
                    editor_provider_whitelist = cls._normalize_editor_providers(data.get("editor_provider_whitelist", []))
                    editor_provider_blacklist = cls._normalize_editor_providers(data.get("editor_provider_blacklist", []))
                    model_whitelist = data.get("model_whitelist", [])
                    model_blacklist = data.get("model_blacklist", [])
                    usage_limit = cls._normalize_usage_limit(data.get("usage_limit"))
                    expires_at = cls._epoch_to_seconds(data.get("expires_at"))
                row = await conn.fetchrow(
                    """UPDATE api_keys SET name=$1, rate_limit=$2::jsonb, provider_whitelist=$3::jsonb,
                        provider_blacklist=$4::jsonb, editor_provider_whitelist=$5::jsonb,
                        editor_provider_blacklist=$6::jsonb, model_whitelist=$7::jsonb, model_blacklist=$8::jsonb,
                        selection_strategy=$9, thinking_config=$10::jsonb,
                        usage_limit=$11::jsonb,
                        expires_at=CASE WHEN $12::double precision IS NULL THEN NULL ELSE to_timestamp($12::double precision) END
                    WHERE id=$13
                    RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist, model_whitelist,
                        model_blacklist, selection_strategy, thinking_config, disabled, parent_id,
                        usage_limit, extract(epoch from expires_at) AS expires_at, scope""",
                    data.get("name", ""),
                    cls._dumps(data.get("rate_limit", {})),
                    cls._dumps(provider_whitelist),
                    cls._dumps(provider_blacklist),
                    cls._dumps(editor_provider_whitelist),
                    cls._dumps(editor_provider_blacklist),
                    cls._dumps(model_whitelist),
                    cls._dumps(model_blacklist),
                    strategy,
                    cls._dumps(data.get("thinking_config", {})),
                    cls._dumps(usage_limit),
                    expires_at,
                    key_id,
                )
        return cls._api_key_row(row) if row else None

    @classmethod
    async def delete_api_key(cls, key_id: int) -> bool:
        async with cls.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM api_keys WHERE id=$1", key_id)
        return int(result.split()[-1]) > 0

    @classmethod
    async def issue_runtime_api_key(cls, data: dict) -> dict:
        """签发一枚绑定 user_id / vm_id 的运行时 Key（对应 MonkeyCode 的 ModelApiKey）。

        复用 api_keys 表：主链路把它当普通 Key 处理，只是额外带上 user/vm 归属。
        既有筛选/限流字段沿用 add_api_key 默认值，可由入参覆盖。VM 侧下发的临时 Key
        通过这里创建，请求进主链路后 get_api_key_config 会自然带出 user_id/vm_id。
        """
        strategy = str(data.get("selection_strategy") or DEFAULT_SELECTION_STRATEGY).strip()
        if strategy not in SELECTION_STRATEGIES:
            strategy = DEFAULT_SELECTION_STRATEGY
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO api_keys(key, name, rate_limit, provider_whitelist, provider_blacklist, editor_provider_whitelist, editor_provider_blacklist, model_whitelist, model_blacklist, selection_strategy, thinking_config, user_id, vm_id) VALUES($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10, $11::jsonb, $12, $13) RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist, editor_provider_whitelist, editor_provider_blacklist, model_whitelist, model_blacklist, selection_strategy, thinking_config, disabled, user_id, vm_id",
                f"sk-{secrets.token_urlsafe(32)}",
                data.get("name", ""),
                cls._dumps(data.get("rate_limit", {})),
                cls._dumps(data.get("provider_whitelist", [])),
                cls._dumps(data.get("provider_blacklist", [])),
                cls._dumps(cls._normalize_editor_providers(data.get("editor_provider_whitelist", []))),
                cls._dumps(cls._normalize_editor_providers(data.get("editor_provider_blacklist", []))),
                cls._dumps(data.get("model_whitelist", [])),
                cls._dumps(data.get("model_blacklist", [])),
                strategy,
                cls._dumps(data.get("thinking_config", {})),
                (str(data.get("user_id")) if data.get("user_id") else None),
                (str(data.get("vm_id")) if data.get("vm_id") else None),
            )
        return cls._api_key_row(row)

    @classmethod
    async def list_api_keys_by_user(cls, user_id: str) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, key, name, rate_limit, provider_whitelist, provider_blacklist, editor_provider_whitelist, editor_provider_blacklist, model_whitelist, model_blacklist, selection_strategy, thinking_config, disabled, user_id, vm_id, extract(epoch from created_at) AS created_at FROM api_keys WHERE user_id=$1 ORDER BY id",
                str(user_id),
            )
        return [cls._api_key_row(r) for r in rows]

    @classmethod
    async def get_api_key_by_id_for_user(cls, key_id: int, user_id: str) -> dict | None:
        """按整数 id 取该用户名下的 api key 行（含明文 key）。

        双条件（id + user_id）防越权：传他人 key_id 返回 None。用于聊天/媒体
        发送时前端只传 api_key_id，后端校验归属后取明文 key 调主链路。
        """
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, key, name, rate_limit, provider_whitelist, provider_blacklist, editor_provider_whitelist, editor_provider_blacklist, model_whitelist, model_blacklist, selection_strategy, thinking_config, disabled, user_id, vm_id, extract(epoch from created_at) AS created_at FROM api_keys WHERE id=$1 AND user_id=$2",
                key_id, str(user_id),
            )
        return cls._api_key_row(row) if row else None

    @classmethod
    async def delete_runtime_api_key_for_user(cls, key_id: int, user_id: str) -> bool:
        """删除运行时 Key，双条件（id + user_id）防止越权删除他人 Key。"""
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM api_keys WHERE id=$1 AND user_id=$2", key_id, str(user_id)
            )
        return int(result.split()[-1]) > 0

    @classmethod
    async def remove_provider_from_api_key_filters(cls, provider_name: str) -> int:
        changed = 0
        keys = await cls.list_api_keys()
        for key in keys:
            whitelist = [p for p in key.get("provider_whitelist", []) if p != provider_name]
            blacklist = [p for p in key.get("provider_blacklist", []) if p != provider_name]
            if whitelist == key.get("provider_whitelist", []) and blacklist == key.get("provider_blacklist", []):
                continue
            await cls.update_api_key(key["id"], {
                "name": key.get("name", ""),
                "rate_limit": key.get("rate_limit", {}),
                "provider_whitelist": whitelist,
                "provider_blacklist": blacklist,
                "editor_provider_whitelist": key.get("editor_provider_whitelist", []),
                "editor_provider_blacklist": key.get("editor_provider_blacklist", []),
                "model_whitelist": key.get("model_whitelist", []),
                "model_blacklist": key.get("model_blacklist", []),
                "selection_strategy": key.get("selection_strategy", DEFAULT_SELECTION_STRATEGY),
                "thinking_config": key.get("thinking_config", {}),
            })
            changed += 1
        return changed

    @classmethod
    async def disable_editor_api_key(cls, editor_id: str, owner_user_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE api_keys k SET disabled=true
                FROM editors e
                WHERE e.id=$1 AND e.owner_user_id=$2 AND e.api_key_id=k.id
                RETURNING k.id, k.version, k.disabled""",
                editor_id,
                str(owner_user_id),
            )
        return dict(row) if row else None

    @classmethod
    async def rotate_editor_api_key(cls, editor_id: str, owner_user_id: str) -> dict | None:
        key_value = f"sk-{secrets.token_urlsafe(32)}"
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                old = await conn.fetchrow(
                    """SELECT k.* FROM api_keys k JOIN editors e ON e.api_key_id=k.id
                    WHERE e.id=$1 AND e.owner_user_id=$2 AND e.status <> 'deleted'
                    FOR UPDATE OF k""",
                    editor_id,
                    str(owner_user_id),
                )
                if not old:
                    return None
                child = await conn.fetchrow(
                    """INSERT INTO api_keys(
                        key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist,
                        model_whitelist, model_blacklist, selection_strategy,
                        thinking_config, user_id, vm_id, parent_id,
                        expires_at, usage_limit, version, label, scope
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                    RETURNING id, key, name, disabled, parent_id, expires_at, usage_limit,
                        version, label, scope""",
                    key_value, old["name"], old["rate_limit"], old["provider_whitelist"],
                    old["provider_blacklist"], old["editor_provider_whitelist"], old["editor_provider_blacklist"],
                    old["model_whitelist"], old["model_blacklist"], old["selection_strategy"], old["thinking_config"], old["user_id"], old["vm_id"],
                    old["parent_id"], old["expires_at"], old["usage_limit"],
                    int(old["version"] or 1) + 1, old["label"], old["scope"],
                )
                await conn.execute("UPDATE api_keys SET disabled=true WHERE id=$1", old["id"])
                await conn.execute(
                    "UPDATE editors SET api_key_id=$1, updated_at=now() WHERE id=$2 AND owner_user_id=$3",
                    child["id"], editor_id, str(owner_user_id),
                )
        return cls._api_key_row(child)

    @classmethod
    async def disable_editor_session_api_key(cls, editor_id: str, session_id: str, owner_user_id: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE api_keys k SET disabled=true
                FROM editor_sessions s JOIN editors e ON e.id=s.editor_id
                WHERE s.editor_id=$1 AND s.id=$2 AND e.owner_user_id=$3 AND s.api_key_id=k.id
                RETURNING k.id, k.version, k.disabled""",
                editor_id,
                session_id,
                str(owner_user_id),
            )
        return dict(row) if row else None

    @classmethod
    async def rotate_editor_session_api_key(cls, editor_id: str, session_id: str, owner_user_id: str) -> dict | None:
        key_value = f"sk-{secrets.token_urlsafe(32)}"
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                old = await conn.fetchrow(
                    """SELECT k.* FROM api_keys k
                    JOIN editor_sessions s ON s.api_key_id=k.id
                    JOIN editors e ON e.id=s.editor_id
                    WHERE s.editor_id=$1 AND s.id=$2 AND e.owner_user_id=$3
                        AND e.status <> 'deleted' AND s.status <> 'closed'
                    FOR UPDATE OF k""",
                    editor_id,
                    session_id,
                    str(owner_user_id),
                )
                if not old:
                    return None
                child = await conn.fetchrow(
                    """INSERT INTO api_keys(
                        key, name, rate_limit, provider_whitelist, provider_blacklist,
                        editor_provider_whitelist, editor_provider_blacklist,
                        model_whitelist, model_blacklist, selection_strategy,
                        thinking_config, user_id, vm_id, parent_id,
                        expires_at, usage_limit, version, label, scope
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                    RETURNING id, key, name, disabled, parent_id, expires_at, usage_limit,
                        version, label, scope""",
                    key_value, old["name"], old["rate_limit"], old["provider_whitelist"],
                    old["provider_blacklist"], old["editor_provider_whitelist"], old["editor_provider_blacklist"],
                    old["model_whitelist"], old["model_blacklist"], old["selection_strategy"], old["thinking_config"], old["user_id"], old["vm_id"],
                    old["parent_id"], old["expires_at"], old["usage_limit"],
                    int(old["version"] or 1) + 1, old["label"], old["scope"],
                )
                await conn.execute("UPDATE api_keys SET disabled=true WHERE id=$1", old["id"])
                await conn.execute(
                    "UPDATE editor_sessions SET api_key_id=$1, updated_at=now() WHERE editor_id=$2 AND id=$3",
                    child["id"], editor_id, session_id,
                )
        return cls._api_key_row(child)

    @classmethod
    async def toggle_api_key_disabled(cls, key_id: int, disabled: bool) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE api_keys SET disabled=$1 WHERE id=$2 RETURNING id, key, name, rate_limit, provider_whitelist, provider_blacklist, editor_provider_whitelist, editor_provider_blacklist, model_whitelist, model_blacklist, selection_strategy, thinking_config, disabled",
                disabled, key_id,
            )
        return cls._api_key_row(row) if row else None

    # ==================== Notifications ====================
    @classmethod
    def _notification_row(cls, row) -> dict | None:
        if not row:
            return None
        data = dict(row)
        for key in ("metadata",):
            data[key] = cls._loads_json(data.get(key), {})
        for key in ("created_at", "first_seen_at", "last_seen_at", "read_at"):
            value = data.get(key)
            if isinstance(value, datetime):
                data[key] = value.timestamp()
        return data

    @classmethod
    async def upsert_notification(cls, data: dict) -> dict | None:
        if not cls.pool:
            return None
        severity = str(data.get("severity") or "info")[:32]
        kind = str(data.get("kind") or "system")[:80]
        source = str(data.get("source") or "system")[:120]
        title = str(data.get("title") or "通知")[:240]
        message = str(data.get("message") or "")[:4000]
        detail = str(data.get("detail") or "")[:20000]
        dedupe_key = str(data.get("dedupe_key") or "")[:500] or None
        try:
            dedupe_window_seconds = max(0, int(data.get("dedupe_window_seconds") or 300))
        except (TypeError, ValueError):
            dedupe_window_seconds = 300
        request_log_id = data.get("request_log_id")
        if request_log_id is not None:
            try:
                request_log_id = int(request_log_id)
            except (TypeError, ValueError):
                request_log_id = None
        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        # 通知类型化 + 多 owner：event_type 规范化事件类型，owner_type/user_id 按
        # 用户/团队/平台分桶（原表全局单桶，仅 admin 可见）。
        event_type = str(data.get("event_type") or "")[:64] or None
        owner_type = str(data.get("owner_type") or "platform")[:16]
        user_id = data.get("user_id")
        if user_id is not None:
            user_id = str(user_id)[:128]

        async with cls.pool.acquire() as conn:
            existing_id = None
            if dedupe_key:
                existing_id = await conn.fetchval(
                    """
                    SELECT id FROM notifications
                    WHERE dedupe_key=$1
                      AND last_seen_at >= now() - ($2::int * interval '1 second')
                      AND owner_type=$3
                      AND user_id IS NOT DISTINCT FROM $4
                    ORDER BY last_seen_at DESC, id DESC
                    LIMIT 1
                    """,
                    dedupe_key,
                    dedupe_window_seconds,
                    owner_type,
                    user_id,
                )
            if existing_id:
                row = await conn.fetchrow(
                    """
                    UPDATE notifications SET
                        severity=$2,
                        kind=$3,
                        source=$4,
                        title=$5,
                        message=$6,
                        detail=COALESCE(NULLIF($12, ''), detail),
                        last_seen_at=now(),
                        occurrence_count=occurrence_count + 1,
                        request_log_id=COALESCE($7, request_log_id),
                        provider_name=COALESCE($8, provider_name),
                        account_username=COALESCE($9, account_username),
                        model=COALESCE($10, model),
                        metadata=$11::jsonb,
                        event_type=COALESCE($13, event_type),
                        owner_type=COALESCE(NULLIF($14, ''), owner_type),
                        user_id=COALESCE($15, user_id)
                    WHERE id=$1
                    RETURNING *
                    """,
                    existing_id,
                    severity,
                    kind,
                    source,
                    title,
                    message,
                    request_log_id,
                    data.get("provider_name"),
                    data.get("account_username"),
                    data.get("model"),
                    cls._dumps(metadata),
                    detail,
                    event_type,
                    owner_type,
                    user_id,
                )
            else:
                row = await conn.fetchrow(
                    """
                    INSERT INTO notifications(
                        severity, kind, source, title, message, dedupe_key, request_log_id,
                        provider_name, account_username, model, metadata, detail,
                        event_type, owner_type, user_id
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15)
                    RETURNING *
                    """,
                    severity,
                    kind,
                    source,
                    title,
                    message,
                    dedupe_key,
                    request_log_id,
                    data.get("provider_name"),
                    data.get("account_username"),
                    data.get("model"),
                    cls._dumps(metadata),
                    detail,
                    event_type,
                    owner_type,
                    user_id,
                )
        return cls._notification_row(row)

    @classmethod
    async def query_notifications(cls, filters: dict | None = None, limit: int = 50, offset: int = 0) -> dict:
        if not cls.pool:
            return {"total": 0, "rows": [], "unread_count": 0}
        filters = filters or {}
        where = []
        args = []

        def add(cond, val):
            args.append(val)
            where.append(cond.format(len(args)))

        status = filters.get("status")
        if status and status != "all":
            add("status=${}", status)
        severity = filters.get("severity")
        if severity and severity != "all":
            add("severity=${}", severity)
        kind = filters.get("kind")
        if kind and kind != "all":
            add("kind=${}", kind)
        event_type = filters.get("event_type")
        if event_type and event_type != "all":
            add("event_type=${}", event_type)
        owner_type = filters.get("owner_type")
        if owner_type:
            add("owner_type=${}", owner_type)
        if "user_id" in filters:
            # None is a meaningful platform owner: user rows are always exact-id.
            if filters.get("user_id") is None:
                where.append("user_id IS NULL")
            else:
                add("user_id=${}", str(filters["user_id"]))
        q = str(filters.get("q") or "").strip()
        if q:
            add("(coalesce(title,'') || ' ' || coalesce(message,'') || ' ' || coalesce(source,'') || ' ' || coalesce(provider_name,'') || ' ' || coalesce(account_username,'') || ' ' || coalesce(model,'')) ILIKE '%' || ${} || '%'", q)

        where_sql = " WHERE " + " AND ".join(where) if where else ""
        count_args = args.copy()
        limit = max(1, min(int(limit or 50), 500))
        offset = max(0, int(offset or 0))
        args.extend([limit, offset])
        async with cls.pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT count(*) FROM notifications {where_sql}", *count_args)
            unread_where = ["status='unread'", *where]
            unread_where_sql = " WHERE " + " AND ".join(unread_where)
            unread_count = await conn.fetchval(
                f"SELECT count(*) FROM notifications{unread_where_sql}", *count_args
            )
            # 列表页不返回 detail（可能很长）；detail 走 get_notification_detail 单条拉取。
            rows = await conn.fetch(
                f"""
                SELECT id, severity, kind, source, event_type, owner_type, user_id,
                       title, message, status, read_at,
                       created_at, first_seen_at, last_seen_at, occurrence_count,
                       dedupe_key, request_log_id, provider_name, account_username,
                       model, metadata
                FROM notifications
                {where_sql}
                ORDER BY
                    CASE WHEN status='unread' THEN 0 ELSE 1 END,
                    CASE severity WHEN 'critical' THEN 0 WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                    last_seen_at DESC,
                    id DESC
                LIMIT ${len(args)-1} OFFSET ${len(args)}
                """,
                *args,
            )
        return {"total": int(total or 0), "rows": [cls._notification_row(row) for row in rows], "unread_count": int(unread_count or 0)}

    @classmethod
    async def notification_summary(cls, latest_limit: int = 5) -> dict:
        if not cls.pool:
            return {"unread_count": 0, "critical_count": 0, "error_count": 0, "total": 0, "latest": []}
        latest_limit = max(1, min(int(latest_limit or 5), 20))
        async with cls.pool.acquire() as conn:
            summary = await conn.fetchrow(
                """
                SELECT
                    count(*) AS total,
                    count(*) FILTER (WHERE status='unread') AS unread_count,
                    count(*) FILTER (WHERE status='unread' AND severity='critical') AS critical_count,
                    count(*) FILTER (WHERE status='unread' AND severity IN ('critical','error')) AS error_count
                FROM notifications
                """
            )
            latest_rows = await conn.fetch(
                """
                SELECT id, severity, kind, source, title, message, status, read_at,
                       created_at, first_seen_at, last_seen_at, occurrence_count,
                       dedupe_key, request_log_id, provider_name, account_username,
                       model, metadata
                FROM notifications
                ORDER BY
                    CASE WHEN status='unread' THEN 0 ELSE 1 END,
                    last_seen_at DESC,
                    id DESC
                LIMIT $1
                """,
                latest_limit,
            )
        return {
            "total": int(summary["total"] or 0),
            "unread_count": int(summary["unread_count"] or 0),
            "critical_count": int(summary["critical_count"] or 0),
            "error_count": int(summary["error_count"] or 0),
            "latest": [cls._notification_row(row) for row in latest_rows],
        }

    @classmethod
    async def get_notification_detail_for_owner(cls, notification_id: int, owner_type: str, user_id: str) -> dict | None:
        if not cls.pool:
            return None
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM notifications WHERE id=$1 AND owner_type=$2 AND user_id=$3",
                int(notification_id), owner_type, str(user_id),
            )
        return cls._notification_row(row)

    @classmethod
    async def mark_notification_read_for_owner(cls, notification_id: int, owner_type: str, user_id: str) -> bool:
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE notifications SET status='read', read_at=COALESCE(read_at, now()) "
                "WHERE id=$1 AND owner_type=$2 AND user_id=$3",
                int(notification_id), owner_type, str(user_id),
            )
        return not result.endswith(" 0")

    @classmethod
    async def mark_all_notifications_read_for_owner(cls, owner_type: str, user_id: str) -> int:
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE notifications SET status='read', read_at=COALESCE(read_at, now()) "
                "WHERE status='unread' AND owner_type=$1 AND user_id=$2",
                owner_type, str(user_id),
            )
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    @classmethod
    async def delete_notification_for_owner(cls, notification_id: int, owner_type: str, user_id: str) -> bool:
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM notifications WHERE id=$1 AND owner_type=$2 AND user_id=$3",
                int(notification_id), owner_type, str(user_id),
            )
        return not result.endswith(" 0")

    @classmethod
    async def delete_notifications_by_ids_for_owner(
        cls, notification_ids: list[int], owner_type: str, user_id: str
    ) -> int:
        """按 owner 域批量删除指定 id 的通知，返回实际删除行数。"""
        if not cls.pool or not notification_ids:
            return 0
        ids = [int(i) for i in notification_ids]
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM notifications WHERE id = ANY($1::bigint[]) "
                "AND owner_type=$2 AND user_id=$3",
                ids, owner_type, str(user_id),
            )
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    @classmethod
    async def delete_notifications_by_filter_for_owner(
        cls, owner_type: str, user_id: str, status: str | None = None
    ) -> int:
        """按 owner 域清空通知：status='read' 清已读，None 清全部。返回删除行数。"""
        if not cls.pool:
            return 0
        where = ["owner_type=$1", "user_id=$2"]
        args: list = [owner_type, str(user_id)]
        if status:
            args.append(status)
            where.append(f"status=${len(args)}")
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                f"DELETE FROM notifications WHERE {' AND '.join(where)}", *args
            )
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    @classmethod
    async def delete_notification(cls, notification_id: int) -> bool:
        """删除单条平台通知（owner_type='platform' AND user_id IS NULL）。"""
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM notifications WHERE id=$1 "
                "AND owner_type='platform' AND user_id IS NULL",
                int(notification_id),
            )
        return not result.endswith(" 0")

    @classmethod
    async def delete_notifications_by_ids(cls, notification_ids: list[int]) -> int:
        """批量删除平台通知，返回实际删除行数。"""
        if not cls.pool or not notification_ids:
            return 0
        ids = [int(i) for i in notification_ids]
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM notifications WHERE id = ANY($1::bigint[]) "
                "AND owner_type='platform' AND user_id IS NULL",
                ids,
            )
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    @classmethod
    async def delete_notifications_by_filter(cls, status: str | None = None) -> int:
        """按状态清空平台通知：status='read' 清已读，None 清全部。返回删除行数。"""
        if not cls.pool:
            return 0
        where = ["owner_type='platform'", "user_id IS NULL"]
        args: list = []
        if status:
            args.append(status)
            where.append(f"status=${len(args)}")
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                f"DELETE FROM notifications WHERE {' AND '.join(where)}", *args
            )
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    @classmethod
    async def get_notification_detail(cls, notification_id: int) -> dict | None:
        if not cls.pool:
            return None
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM notifications WHERE id=$1", int(notification_id))
        return cls._notification_row(row)

    @classmethod
    async def mark_notification_read(cls, notification_id: int) -> bool:
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE notifications SET status='read', read_at=COALESCE(read_at, now()) WHERE id=$1",
                int(notification_id),
            )
        return not result.endswith(" 0")

    @classmethod
    async def mark_all_notifications_read(cls) -> int:
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            result = await conn.execute("UPDATE notifications SET status='read', read_at=COALESCE(read_at, now()) WHERE status='unread'")
        try:
            return int(result.split()[-1])
        except (IndexError, ValueError):
            return 0

    # ==================== Operation Logs ====================
    @classmethod
    async def insert_operation_log(cls, operator: str, action: str, target_type: str = None, target_name: str = None, old_data: dict = None, new_data: dict = None):
        if not cls.pool:
            return
        async with cls.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO operation_logs(operator, action, target_type, target_name, old_data, new_data) VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb)",
                operator, action, target_type, target_name,
                cls._dumps(old_data or {}), cls._dumps(new_data or {}),
            )

    @classmethod
    async def list_operation_logs(
        cls,
        *,
        before_ts: int | None = None,
        limit: int = 20,
        action: str | None = None,
    ) -> list[dict]:
        """Read admin ``operation_logs`` rows newest-first for audit merging.

        ``before_ts`` is a ``created_at`` unix-seconds boundary (exclusive) so the
        caller can page in lockstep with the ``mc_audits`` cursor. ``old_data`` /
        ``new_data`` come back as raw JSONB text (asyncpg default) and are passed
        through unchanged — they were already secret-scrubbed at write time.
        Returns [] when the pool is unavailable (never raises for a missing table
        is not handled here; the caller wraps this in try/except).
        """
        if not cls.pool:
            return []
        clauses: list[str] = []
        args: list = []
        if before_ts is not None:
            args.append(int(before_ts))
            clauses.append(f"created_at < to_timestamp(${len(args)})")
        if action:
            args.append(action)
            clauses.append(f"action = ${len(args)}")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        args.append(int(limit))
        sql = (
            "SELECT id, operator, action, target_type, target_name, "
            "old_data, new_data, extract(epoch from created_at) AS created_at "
            f"FROM operation_logs{where} ORDER BY created_at DESC LIMIT ${len(args)}"
        )
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
        result: list[dict] = []
        for r in rows:
            ts = r["created_at"]
            result.append(
                {
                    "id": int(r["id"]),
                    "operator": r["operator"],
                    "action": r["action"],
                    "target_type": r["target_type"],
                    "target_name": r["target_name"],
                    "old_data": r["old_data"],
                    "new_data": r["new_data"],
                    "created_at": int(ts) if ts is not None else None,
                }
            )
        return result

    # ==================== Account Auth States ====================
    # device_code 型授权需扫描器每秒轮询；callback 型只等浏览器回调。两类同表，
    # 靠 task_type 区分。expires_at 是列，扫描按时间过滤 + 每轮清扫过期行——
    # 取代旧的 Redis 全库 SCAN（成本随 keyspace 线性恶化）。

    @classmethod
    async def upsert_account_auth_state(cls, state: str, data: dict) -> None:
        """写入/更新一条授权任务。data 是全量 payload，含 expires_at（unix 秒）。"""
        if not cls.pool or not state:
            return
        data = data or {}
        expires_at = float(data.get("expires_at") or 0) or (time.time() + 900)
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO account_auth_states
                    (state, provider, task_type, status, next_poll_at, created_at, expires_at, data)
                VALUES ($1, $2, $3, $4, $5, to_timestamp($6), to_timestamp($7), $8::jsonb)
                ON CONFLICT (state) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    task_type = EXCLUDED.task_type,
                    status = EXCLUDED.status,
                    next_poll_at = EXCLUDED.next_poll_at,
                    expires_at = EXCLUDED.expires_at,
                    data = EXCLUDED.data
                """,
                state,
                str(data.get("provider") or ""),
                data.get("task_type"),
                data.get("status"),
                (float(data["next_poll_at"]) if data.get("next_poll_at") is not None else None),
                float(data.get("created_at") or time.time()),
                expires_at,
                cls._dumps(data),
            )

    @classmethod
    async def get_account_auth_state(cls, state: str) -> dict | None:
        """按 state 读一条未过期的授权任务；过期或不存在返回 None。"""
        if not cls.pool or not state:
            return None
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT data FROM account_auth_states WHERE state = $1 AND expires_at > now()",
                state,
            )
        if not row:
            return None
        return cls._loads_json(row["data"], {})

    @classmethod
    async def delete_account_auth_state(cls, state: str) -> None:
        if not cls.pool or not state:
            return
        async with cls.pool.acquire() as conn:
            await conn.execute("DELETE FROM account_auth_states WHERE state = $1", state)

    @classmethod
    async def list_pending_device_code_states(cls) -> list[tuple[str, dict]]:
        """列出未过期的 pending device_code 任务 → [(state, data)]。走复合索引，不扫全表。"""
        if not cls.pool:
            return []
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT state, data FROM account_auth_states
                WHERE task_type = 'device_code' AND status = 'pending' AND expires_at > now()
                """
            )
        return [(r["state"], cls._loads_json(r["data"], {})) for r in rows]

    @classmethod
    async def delete_expired_account_auth_states(cls) -> int:
        """清扫所有过期授权任务（含 callback 型），返回删除行数。扫描器每轮调用。"""
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM account_auth_states WHERE expires_at <= now()")
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0

    # ==================== Model Metadata ====================
    MODEL_METADATA_DEFAULT_KEY = "model_metadata_default"



    @classmethod
    async def find_models_by_context_length(cls, limit: int, tolerance: float = 0.02) -> list[str]:
        """查找 data->>'max_context_tokens' 接近 limit（±容差）的 model_id 列表。"""
        if not cls.pool or limit <= 0:
            return []
        min_len = int(limit * (1 - tolerance))
        max_len = int(limit * (1 + tolerance))
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT model_id FROM model_metadata
                WHERE (data->>'max_context_tokens')::int BETWEEN $1 AND $2
                ORDER BY model_id
                """,
                min_len, max_len,
            )
        return [r["model_id"] for r in rows]

    @classmethod
    async def get_model_max_context_tokens(cls, model_id: str) -> int | None:
        """查询模型声称的 max_context_tokens；没有记录或字段为空时返回 None。"""
        if not cls.pool or not model_id:
            return None
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT data->>'max_context_tokens' AS v FROM model_metadata WHERE model_id=$1",
                model_id,
            )
        if not row or not row["v"]:
            return None
        try:
            v = int(row["v"])
            return v if v > 0 else None
        except (TypeError, ValueError):
            return None

    @classmethod
    async def get_model_metadata_default(cls) -> dict:
        if not cls.pool:
            return {}
        return await cls.get_config(cls.MODEL_METADATA_DEFAULT_KEY) or {}

    @classmethod
    async def set_model_metadata_default(cls, data: dict) -> None:
        if not cls.pool:
            return
        await cls.set_config(cls.MODEL_METADATA_DEFAULT_KEY, data or {})

    # ==================== Header Templates ====================
    HEADER_TEMPLATES_KEY = "header_templates"

    @classmethod
    async def get_header_templates(cls) -> list[dict]:
        """读取系统 header 模板库（存 app_config key=header_templates）。

        返回结构：{"templates": [{"id","name","headers"}, ...]}。
        """
        if not cls.pool:
            return []
        data = await cls.get_config(cls.HEADER_TEMPLATES_KEY) or {}
        templates = data.get("templates") if isinstance(data, dict) else None
        if not isinstance(templates, list):
            return []
        return [t for t in templates if isinstance(t, dict)]

    @classmethod
    async def set_header_templates(cls, templates: list[dict]) -> list[dict]:
        if not cls.pool:
            return []
        cleaned = [t for t in templates if isinstance(t, dict)]
        await cls.set_config(cls.HEADER_TEMPLATES_KEY, {"templates": cleaned})
        return cleaned

    # ==================== Model Rule Templates ====================
    MODEL_RULE_TEMPLATES_KEY = "model_rule_templates"

    @classmethod
    async def get_model_rule_templates(cls) -> list[dict]:
        """读取全局模型规则模版库（存 app_config key=model_rule_templates）。

        返回结构：{"templates": [{"id","name","rules"}, ...]}；rules 只含内联规则。
        """
        if not cls.pool:
            return []
        data = await cls.get_config(cls.MODEL_RULE_TEMPLATES_KEY) or {}
        templates = data.get("templates") if isinstance(data, dict) else None
        if not isinstance(templates, list):
            return []
        return [t for t in templates if isinstance(t, dict)]

    @classmethod
    async def set_model_rule_templates(cls, templates: list[dict]) -> list[dict]:
        if not cls.pool:
            return []
        cleaned = [t for t in templates if isinstance(t, dict)]
        await cls.set_config(cls.MODEL_RULE_TEMPLATES_KEY, {"templates": cleaned})
        return cleaned

    @classmethod
    async def load_model_catalog_source(cls) -> dict:
        """Read the complete model catalog from one consistent DB snapshot.

        model_groups 现在是唯一真相源：custom 行（kind='custom'）是路由组，real 行
        （kind='real'）承载真实模型元数据（metadata 列）。二者同表，由 kind 区分。
        default 元数据仍存 app_config（全局默认，不属于任何具体模型）。
        """
        if not cls.pool:
            return {"groups": {}, "metadata": [], "default": {}}
        async with cls.pool.acquire() as conn:
            async with conn.transaction(
                isolation="repeatable_read",
                readonly=True,
            ):
                group_rows = await conn.fetch(
                    "SELECT name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at "
                    "FROM model_groups ORDER BY created_at ASC, name"
                )
                default_row = await conn.fetchrow(
                    "SELECT data FROM app_config WHERE key=$1",
                    cls.MODEL_METADATA_DEFAULT_KEY,
                )

        groups: dict[str, dict] = {}
        metadata: list[dict] = []
        for row in group_rows:
            parsed = cls._model_group_row(row)
            if parsed is None:
                continue
            name = parsed["name"]
            groups[name] = parsed
            # real 行：把 metadata 列展开成「以 model_id 为键的元数据记录」，
            # 与旧 model_metadata 表的产出形状一致，供 snapshot 的 _metadata_by_id 消费。
            if parsed.get("kind") == "real":
                record = dict(parsed.get("metadata") or {})
                record["model_id"] = name
                metadata.append(record)
        default = cls._loads_json(default_row["data"], {}) if default_row else {}
        return {
            "groups": groups,
            "metadata": metadata,
            "default": default if isinstance(default, dict) else {},
        }

    @classmethod
    async def list_model_metadata(cls) -> list[dict]:
        if not cls.pool:
            return []
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT model_id, data FROM model_metadata ORDER BY model_id")
        items = []
        for row in rows:
            data = json.loads(row["data"]) if isinstance(row["data"], str) else dict(row["data"] or {})
            data["model_id"] = row["model_id"]
            items.append(data)
        return items

    @classmethod
    async def get_model_metadata(cls, model_id: str) -> dict | None:
        if not cls.pool:
            return None
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT model_id, data FROM model_metadata WHERE model_id=$1",
                model_id,
            )
        if not row:
            return None
        data = json.loads(row["data"]) if isinstance(row["data"], str) else dict(row["data"] or {})
        data["model_id"] = row["model_id"]
        return data

    @classmethod
    async def upsert_model_metadata(cls, model_id: str, data: dict) -> None:
        if not cls.pool:
            return
        payload = {k: v for k, v in (data or {}).items() if k not in ("model_id", "system_id")}
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO model_metadata(model_id, data, updated_at)
                VALUES($1, $2::jsonb, now())
                ON CONFLICT(model_id) DO UPDATE SET
                    data=EXCLUDED.data,
                    updated_at=now()
                """,
                model_id, cls._dumps(payload),
            )

    @classmethod
    async def delete_model_metadata(cls, model_id: str) -> bool:
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM model_metadata WHERE model_id=$1", model_id)
        return result.endswith("1")

    @classmethod
    async def bulk_upsert_model_metadata(cls, items: list[dict]) -> int:
        """批量 upsert。items: [{model_id, ...data}]"""
        if not cls.pool or not items:
            return 0
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                for item in items:
                    model_id = (item.get("model_id") or item.get("id") or "").strip()
                    if not model_id:
                        continue
                    data = {k: v for k, v in item.items() if k not in ("model_id", "system_id")}
                    await conn.execute(
                        """
                        INSERT INTO model_metadata(model_id, data, updated_at)
                        VALUES($1, $2::jsonb, now())
                        ON CONFLICT(model_id) DO UPDATE SET
                            data=EXCLUDED.data,
                            updated_at=now()
                        """,
                        model_id, cls._dumps(data),
                    )
        return len(items)

    @classmethod
    async def count_model_metadata(cls) -> int:
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM model_metadata")

    # ==================== Real-model metadata (model_groups kind='real') ====================
    # 真实模型元数据现在落在 model_groups 的 kind='real' 行（metadata 列承载元数据 dict）。
    # 下列方法是对这类行的读写入口；custom 路由组不得被这些入口改写——同名 custom 行受保护。

    @classmethod
    def _real_metadata_normalized(cls, model_id: str, metadata: dict) -> dict:
        # 渠道标签过滤与方案（schemes）是 real 行的路由配置（顶层列），不属于 metadata
        # JSONB——从负载里摘出来单存；未提供时由调用方决定是否从既有行回填（见
        # upsert/bulk 的保留逻辑）。方案身份是 id；active_scheme 按 id 命中，落空回落首套。
        source = metadata or {}
        schemes = cls._normalize_schemes(source.get("schemes", []))
        active_scheme = str(source.get("active_scheme") or "").strip()
        if schemes:
            active = next((s for s in schemes if s["id"] == active_scheme), None) or schemes[0]
            active_scheme = active["id"]
        else:
            active_scheme = ""
        payload = {
            str(k): v for k, v in source.items()
            if str(k) not in {"model_id", "system_id", "id", "provider_whitelist", "provider_blacklist", "schemes", "active_scheme"}
        }
        # 顶层过滤 = 激活方案投影；负载显式携带顶层对时，写回激活方案（旧单对入口与
        # 方案编辑共用一列，语义是「改当前方案」而非「覆盖投影」）。无方案时用负载对。
        active_index = next((i for i, s in enumerate(schemes) if s["id"] == active_scheme), None)
        if active_index is not None:
            patched = dict(schemes[active_index])
            if "provider_whitelist" in source:
                patched["provider_whitelist"] = cls._clean_string_list(source.get("provider_whitelist", []))
            if "provider_blacklist" in source:
                patched["provider_blacklist"] = cls._clean_string_list(source.get("provider_blacklist", []))
            schemes[active_index] = patched
            active = patched
        else:
            active = None
        return {
            "name": model_id,
            "kind": "real",
            "enabled": True,
            "remark": "",
            "models": [],
            "aliases": [],
            "provider_whitelist": active["provider_whitelist"] if active else cls._clean_string_list(source.get("provider_whitelist", [])),
            "provider_blacklist": active["provider_blacklist"] if active else cls._clean_string_list(source.get("provider_blacklist", [])),
            "selection_strategy": DEFAULT_SELECTION_STRATEGY,
            "backup_model_group": "",
            "response_model": "",
            "metadata_model": "",
            "metadata": payload,
            "schemes": schemes,
            "active_scheme": active_scheme,
        }

    @classmethod
    async def upsert_real_model_metadata(cls, model_id: str, metadata: dict) -> dict:
        """Upsert a kind='real' metadata row. Refuses to clobber a custom group.

        请求负载未携带 provider_whitelist/provider_blacklist 时保留既有行的渠道过滤
        ——元数据更新/批量导入不应静默清掉管理员配好的按模型选路。
        """
        model_id = (model_id or "").strip()
        if not model_id:
            raise ValueError("model_id 必填")
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT kind, provider_whitelist, provider_blacklist, schemes, active_scheme FROM model_groups WHERE name=$1",
                    model_id,
                )
                if existing is not None and existing["kind"] != "real":
                    raise ValueError(
                        f"模型 {model_id!r} 已是自定义模型组，不能直接覆盖其元数据"
                    )
                effective = dict(metadata or {})
                if existing is not None:
                    for fld in ("provider_whitelist", "provider_blacklist", "schemes", "active_scheme"):
                        if fld not in effective:
                            effective[fld] = cls._loads_json(existing[fld], []) if fld == "schemes" else existing[fld]
                normalized = cls._real_metadata_normalized(model_id, effective)
                row = await cls._upsert_model_group_on_connection(conn, normalized)
        return cls._model_group_row(row)

    @classmethod
    async def delete_real_model_metadata(cls, model_id: str) -> bool:
        """Delete a real-model metadata row; never touches custom groups."""
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            kind = await conn.fetchval(
                "SELECT kind FROM model_groups WHERE name=$1 AND kind='real'", model_id
            )
            if kind is None:
                return False
            result = await conn.execute(
                "DELETE FROM model_groups WHERE name=$1 AND kind='real'", model_id
            )
        return result.endswith("1")

    @classmethod
    async def update_real_model_provider_filter(
        cls, model_id: str, whitelist: list, blacklist: list,
        schemes: list | None = None, active_scheme: str | None = None,
    ) -> dict | None:
        """真实模型选路窄写：只更新路由相关列（渠道过滤两列，可选 schemes/active_scheme），
        元数据及其余列原样保留。

        仅命中 kind='real' 行；custom 组返回 None（调用方 404/400），避免该入口改写路由组。
        schemes 给出时走方案模式：normalize 后按 active_scheme（落空回落首套）投影顶层两列，
        负载的顶层对忽略——顶层永远是激活方案的投影。
        """
        if not cls.pool:
            return None
        if schemes is not None:
            normalized_schemes = cls._normalize_schemes(schemes)
            raw_active = str(active_scheme or "").strip()
            active = next((s for s in normalized_schemes if s["id"] == raw_active), None) or (
                normalized_schemes[0] if normalized_schemes else None
            )
            whitelist = active["provider_whitelist"] if active else []
            blacklist = active["provider_blacklist"] if active else []
            active_scheme = active["id"] if active else ""
        else:
            normalized_schemes = None
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                if normalized_schemes is None:
                    row = await conn.fetchrow(
                        """
                        UPDATE model_groups
                        SET provider_whitelist=$2::jsonb, provider_blacklist=$3::jsonb, updated_at=now()
                        WHERE name=$1 AND kind='real'
                        RETURNING name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at
                        """,
                        model_id,
                        cls._dumps(cls._clean_string_list(whitelist or [])),
                        cls._dumps(cls._clean_string_list(blacklist or [])),
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        UPDATE model_groups
                        SET provider_whitelist=$2::jsonb, provider_blacklist=$3::jsonb,
                            schemes=$4::jsonb, active_scheme=$5, updated_at=now()
                        WHERE name=$1 AND kind='real'
                        RETURNING name, kind, enabled, remark, models, aliases, provider_whitelist, provider_blacklist, selection_strategy, backup_group, response_model, metadata_model, metadata, schemes, active_scheme, created_at
                        """,
                        model_id,
                        cls._dumps(cls._clean_string_list(whitelist or [])),
                        cls._dumps(cls._clean_string_list(blacklist or [])),
                        cls._dumps(normalized_schemes),
                        str(active_scheme or ""),
                    )
        return cls._model_group_row(row)

    @classmethod
    async def bulk_upsert_real_model_metadata(cls, items: list[dict]) -> int:
        """批量 upsert real 元数据行。同名 custom 组的条目跳过（不覆盖）。

        条目未携带渠道过滤字段时保留既有行的值，与单条 upsert 同口径。
        """
        if not cls.pool or not items:
            return 0
        written = 0
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                for item in items:
                    model_id = (item.get("model_id") or item.get("id") or "").strip()
                    if not model_id:
                        continue
                    existing = await conn.fetchrow(
                        "SELECT kind, provider_whitelist, provider_blacklist, schemes, active_scheme FROM model_groups WHERE name=$1",
                        model_id,
                    )
                    if existing is not None and existing["kind"] != "real":
                        continue
                    effective = dict(item)
                    if existing is not None:
                        for fld in ("provider_whitelist", "provider_blacklist", "schemes", "active_scheme"):
                            if fld not in effective:
                                effective[fld] = cls._loads_json(existing[fld], []) if fld == "schemes" else existing[fld]
                    normalized = cls._real_metadata_normalized(model_id, effective)
                    await cls._upsert_model_group_on_connection(conn, normalized)
                    written += 1
        return written

    @classmethod
    async def count_real_model_metadata(cls) -> int:
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM model_groups WHERE kind='real'")

    # ==================== Provider Models ====================

    @classmethod
    def _provider_model_payload(cls, item: dict | tuple | list) -> dict:
        if isinstance(item, dict):
            upstream_id = (item.get("upstream_model_id") or item.get("upstream") or "").strip()
            model_id = (item.get("model_id") or item.get("model") or upstream_id).strip()
            extra_config = item.get("extra_config") if isinstance(item.get("extra_config"), dict) else {}
            return {
                "upstream_model_id": upstream_id,
                "model_id": model_id or upstream_id,
                "extra_config": extra_config,
            }
        upstream_id = str(item[0] if len(item) > 0 else "").strip()
        model_id = str(item[1] if len(item) > 1 else upstream_id).strip() or upstream_id
        return {
            "upstream_model_id": upstream_id,
            "model_id": model_id,
            "extra_config": {},
        }

    @staticmethod
    def _optional_int(value) -> int | None:
        if value in (None, ""):
            return None
        try:
            n = int(value)
        except (TypeError, ValueError):
            return None
        return n if n > 0 else None

    @classmethod
    async def list_provider_models(cls, provider: str | None = None, lite: bool = False) -> list[dict]:
        if not cls.pool:
            return []
        fields = "upstream_model_id, model_id" if lite else "provider, upstream_model_id, model_id, extra_config"
        async with cls.pool.acquire() as conn:
            if provider is None:
                rows = await conn.fetch(
                    f"SELECT {fields} FROM provider_models ORDER BY provider, upstream_model_id"
                )
            else:
                rows = await conn.fetch(
                    f"SELECT {fields} FROM provider_models WHERE provider=$1 ORDER BY upstream_model_id",
                    provider,
                )
        result = []
        for row in rows:
            item = dict(row)
            # asyncpg 默认将 json/jsonb 返回为 JSON 文本；在 DB 边界统一还原，
            # 否则下游的 isinstance(..., dict) 会把 extra_config 静默当成空配置。
            item["extra_config"] = cls._loads_json(item.get("extra_config"), {})
            if not isinstance(item["extra_config"], dict):
                item["extra_config"] = {}
            result.append(item)
        return result

    @classmethod
    async def upsert_provider_model(
        cls,
        provider: str,
        upstream_model_id: str,
        model_id: str,
        *,
        extra_config: dict | None = None,
    ) -> None:
        if not cls.pool:
            return
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO provider_models(provider, upstream_model_id, model_id, extra_config)
                VALUES($1, $2, $3, $4::jsonb)
                ON CONFLICT(provider, upstream_model_id) DO UPDATE SET
                    model_id=EXCLUDED.model_id,
                    extra_config=EXCLUDED.extra_config
                """,
                provider,
                upstream_model_id,
                model_id,
                cls._dumps(extra_config or {}),
            )

    @classmethod
    async def delete_provider_model(cls, provider: str, upstream_model_id: str) -> bool:
        if not cls.pool:
            return False
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM provider_models WHERE provider=$1 AND upstream_model_id=$2",
                provider, upstream_model_id,
            )
        return result.endswith("1")

    @classmethod
    async def bulk_replace_provider_models(cls, provider: str, rows: list[dict | tuple[str, str]]) -> int:
        """事务内整覆盖某渠道的模型行。rows 支持 dict 或 (upstream_model_id, model_id)。"""
        if not cls.pool:
            return 0
        payloads = []
        seen = set()
        for item in rows:
            payload = cls._provider_model_payload(item)
            upstream_id = payload["upstream_model_id"]
            if not upstream_id or upstream_id in seen:
                continue
            seen.add(upstream_id)
            payloads.append(payload)
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM provider_models WHERE provider=$1", provider)
                for payload in payloads:
                    await conn.execute(
                        """
                        INSERT INTO provider_models(provider, upstream_model_id, model_id, extra_config)
                        VALUES($1, $2, $3, $4::jsonb)
                        ON CONFLICT(provider, upstream_model_id) DO UPDATE SET
                            model_id=EXCLUDED.model_id,
                            extra_config=EXCLUDED.extra_config
                        """,
                        provider,
                        payload["upstream_model_id"],
                        payload["model_id"],
                        cls._dumps(payload["extra_config"]),
                    )
        return len(payloads)

    @classmethod
    async def count_provider_models(cls, provider: str) -> int:
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM provider_models WHERE provider=$1", provider
            )

    @classmethod
    async def count_provider_accounts(cls, provider: str) -> int:
        """Count account rows without loading any credential payload."""
        if not cls.pool:
            return 0
        async with cls.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM provider_accounts WHERE provider_name=$1", provider
            )

    @classmethod
    async def get_provider_synced_upstream_ids(cls, provider: str) -> set[str]:
        """读出 provider_configs.config.synced_upstream_ids（记录已经被自动同步过的上游 id）。"""
        if not cls.pool:
            return set()
        async with cls.pool.acquire() as conn:
            row = await conn.fetchval("SELECT config FROM provider_configs WHERE name=$1", provider)
        if row is None:
            return set()
        cfg = json.loads(row) if isinstance(row, str) else dict(row)
        synced = cfg.get("synced_upstream_ids") or []
        return {s for s in synced if isinstance(s, str) and s}

    @classmethod
    async def set_provider_synced_upstream_ids(cls, provider: str, ids: set[str]) -> None:
        if not cls.pool:
            return
        async with cls.pool.acquire() as conn:
            row = await conn.fetchval("SELECT config FROM provider_configs WHERE name=$1", provider)
            if row is None:
                return
            cfg = json.loads(row) if isinstance(row, str) else dict(row)
            cfg["synced_upstream_ids"] = sorted(ids)
            await conn.execute(
                "UPDATE provider_configs SET config=$1::jsonb, updated_at=now() WHERE name=$2",
                cls._dumps(cfg), provider,
            )

    # ==================== Data Cleanup ====================
    @classmethod
    async def reconcile_stale_request_logs(
        cls,
        max_age_minutes: int = 10,
        batch_size: int = 1000,
    ) -> int:
        """Close started request logs that never received a terminal snapshot.

        Client disconnects, process termination, or writer queue loss can leave a
        metadata row in ``requesting`` forever. Only rows older than the safety
        window are touched; batches use SKIP LOCKED to avoid contending with live
        finalization.
        """
        if not cls.pool:
            return 0
        max_age_minutes = max(1, int(max_age_minutes))
        batch_size = max(1, min(int(batch_size), 10_000))
        reconciled = 0
        async with cls.pool.acquire() as conn:
            while True:
                rows = await conn.fetch(
                    """
                    WITH stale AS (
                        SELECT id
                        FROM request_logs
                        WHERE status = 'requesting'
                          AND created_at < now() - make_interval(mins => $1)
                        ORDER BY id
                        LIMIT $2
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE request_logs AS logs
                    SET success = false,
                        status = 'cancelled',
                        duration_ms = GREATEST(
                            logs.duration_ms,
                            LEAST(
                                2147483647::bigint,
                                floor(extract(epoch FROM (now() - logs.created_at)) * 1000)::bigint
                            )::integer,
                            1
                        ),
                        error = CASE
                            WHEN coalesce(logs.error, '') = ''
                            THEN 'request cancelled or connection lost before finalization'
                            ELSE logs.error
                        END
                    FROM stale
                    WHERE logs.id = stale.id
                    RETURNING logs.id
                    """,
                    max_age_minutes,
                    batch_size,
                )
                batch = len(rows)
                reconciled += batch
                if batch < batch_size:
                    break
        if reconciled:
            logger.warning(
                "修复遗留 requesting 请求日志: {} 行（超过 {} 分钟）",
                reconciled,
                max_age_minutes,
            )
        return reconciled

    @classmethod
    async def clean_response_bodies(cls, keep_hours: int = 24):
        """已停用：改由归档动作整行 DELETE 处理 body。

        旧实现用 UPDATE request_logs SET response_body = NULL 清空大字段，反而制造大量
        TOAST 死元组、磁盘不缩（实测主表膨胀到 46GB）。现 body 随归档整行 DELETE 删除，
        TOAST 可被 autovacuum 回收。保留此空方法仅为兼容旧调用，不再执行任何写操作。
        """
        return

    @classmethod
    async def request_log_entry_boundary(cls, keep_entries: int):
        """返回「按条数保留时，最老一条被保留日志」的 created_at（UTC naive），不足则 None。

        用途是让 ClickHouse 的 payload 清理锚定到 Postgres 主表的实际删除集合：
        cleanup_request_logs 按 `ORDER BY id DESC OFFSET keep_entries` 删最老行，
        因此被保留集合的下界就是 offset keep_entries-1 那一行的时间。payload 侧只删
        `created_at < 该时间`，主表删掉的行对应的 payload 必然被带走，而仍在主表里的
        行的 payload 一定保留——否则日志查得到、请求体查不到（或反之成为孤儿占盘）。

        同一时间戳并列时宁可多保留：用严格小于，不会误删边界行的 payload。
        返回值转成 UTC naive，与 ClickHouse `request_log_payloads.created_at`
        （写入侧用 time.gmtime，是 UTC naive）口径一致，避免跨时区比较偏移。
        """
        if not cls.pool:
            return None
        keep_entries = max(0, int(keep_entries))
        if keep_entries <= 0:
            return None
        async with cls.pool.acquire() as conn:
            value = await conn.fetchval(
                "SELECT created_at FROM request_logs ORDER BY id DESC OFFSET $1 LIMIT 1",
                keep_entries - 1,
            )
        if value is None:
            return None
        if getattr(value, "tzinfo", None) is not None:
            from datetime import timezone as _tz
            value = value.astimezone(_tz.utc).replace(tzinfo=None)
        return value

    @classmethod
    async def cleanup_request_logs(cls, keep_hours: int = 24, keep_days: int | None = None, batch_size: int = 10000):
        """按「日志保留天数」直接清理主表 request_logs，并清理旧 hourly_log_stats。

        - 按天数删：created_at 超过 keep_days 的主表整行（body 随行 DELETE，TOAST 可回收）。
        - 可选按条数删：若 Config.get_log_retention_max_entries()>0，再删超额的最老行（id DESC OFFSET 取最老）。
        分批删除避免长事务。keep_days 默认取 Config.get_log_retention_days()（data_retention.log_days，默认 30）。
        返回本次删除的主表总行数。

        聚合水位护栏：request_logs 是 hourly_dashboard_stats 的唯一素材，而聚合表是
        仪表盘历史的唯一长期存储。删除必须以水位（聚合表 max(hour)+1h）为硬下界——
        水位之后的原始行即使超出天数/条数也保留，等聚合追上后在下一轮清理再删；
        聚合表为空（聚合尚未跑过/被清空）则整体跳过，否则删光素材后聚合表永远空转。

        与聚合（aggregate_hourly_logs）持同一把会话级 advisory lock 互斥：聚合批
        先删后插且各语句独立自动提交，若清理插进两者之间删走该批范围的原始行，
        聚合会按残缺素材算出偏小统计并覆盖正确值。持锁先于任何删除语句，释锁走
        finally（异常路径也在连接归还前解锁）。水位读取在锁外：只读且保守安全。
        """
        from config import Config as _Config
        if not cls.pool:
            return 0
        if keep_days is None:
            keep_days = _Config.get_log_retention_days()
        max_entries = _Config.get_log_retention_max_entries()
        deleted = 0
        async with cls.pool.acquire() as conn:
            watermark = await conn.fetchval(
                "SELECT max(hour) + interval '1 hour' FROM hourly_dashboard_stats"
            )
        if watermark is None:
            logger.info("跳过日志清理：hourly_dashboard_stats 为空，尚无可聚合数据，删除会导致统计永久缺口")
            return 0
        conn = await cls.pool.acquire()
        try:
            await conn.execute("SELECT pg_advisory_lock($1)", cls.HOURLY_STATS_LOCK_KEY)
            # 1) 按天数删：分批删除 created_at 超过 keep_days 的主表整行（水位为硬下界）
            if keep_days and keep_days > 0:
                while True:
                    result = await conn.execute(
                        """
                        DELETE FROM request_logs
                        WHERE id IN (
                            SELECT id FROM request_logs
                            WHERE created_at < now() - make_interval(days => $1)
                              AND created_at < $3::timestamptz
                            LIMIT $2
                        )
                        """,
                        keep_days,
                        batch_size,
                        watermark,
                    )
                    try:
                        batch = int(str(result).split()[-1])
                    except (ValueError, IndexError):
                        batch = 0
                    deleted += batch
                    if batch == 0:
                        break
            # 2) 可选按条数删：超出 max_entries 的最老行（id DESC OFFSET 取最老；水位为硬下界）。
            #    水位条件放在 OFFSET 子查询外：豁免窗始终是“全局最新 max_entries 条”，
            #    未聚合的行即使超出豁免窗也被水位扣留，此时总行数可能暂时多于 max_entries。
            if max_entries and max_entries > 0:
                while True:
                    result = await conn.execute(
                        """
                        DELETE FROM request_logs
                        WHERE created_at < $2::timestamptz
                          AND id IN (
                            SELECT id FROM request_logs
                            ORDER BY id DESC OFFSET $1
                            LIMIT $3
                        )
                        """,
                        max_entries,
                        watermark,
                        batch_size,
                    )
                    try:
                        batch = int(str(result).split()[-1])
                    except (ValueError, IndexError):
                        batch = 0
                    deleted += batch
                    if batch == 0:
                        break
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", cls.HOURLY_STATS_LOCK_KEY)
            await cls.pool.release(conn)
        if deleted:
            logger.info(f"清理请求日志：删除 {deleted} 行（保留 {keep_days} 天 / 最多 {max_entries} 条）")
        await cls._clean_old_hourly_stats(keep_days)
        return deleted

    @classmethod
    async def _clean_old_hourly_stats(cls, keep_days: int):
        """删除 hourly_log_stats 中超过 keep_days 天的历史行"""
        if not cls.pool:
            return
        async with cls.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM hourly_log_stats WHERE hour < date_trunc('hour', now()) - make_interval(days => $1)",
                keep_days,
            )
            logger.info(f"清理旧 hourly_log_stats: {result}")

    @classmethod
    async def get_agent(cls, agent_id: int) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
        return dict(row) if row else None

    # 历史孤儿 agent（user_id IS NULL，含已废弃的 id=1 default agent）归属到配置的
    # owner，使多用户隔离生效。uid 缺失/不存在时跳过（不阻断启动）。
    _ORPHAN_OWNER_USER_ID = "2879b473-5966-4dc7-a842-02a83b86e020"

    @classmethod
    async def migrate_orphan_agents_owner(cls) -> None:
        """把 user_id IS NULL 的 agent 归属到 _ORPHAN_OWNER_USER_ID。

        废弃 default agent 后，历史 NULL 行不再是「平台 Agent」语义；统一挂到配置
        owner 名下成为其私有 agent。owner 不存在时跳过（避免数据悬空）。
        """
        if not cls.pool:
            return
        owner = cls._ORPHAN_OWNER_USER_ID
        async with cls.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT 1 FROM mc_users WHERE id::text=$1 AND is_deleted=FALSE", owner
            )
            if not exists:
                logger.warning(
                    "[migrate] orphan owner %s not found in mc_users; skip orphan agents reparent",
                    owner,
                )
                return
            result = await conn.execute(
                "UPDATE agents SET user_id=$1 WHERE user_id IS NULL", owner
            )
            if result and result != "UPDATE 0":
                logger.info("[migrate] reparented orphan agents to %s: %s", owner, result)

    @classmethod
    async def migrate_orphan_scheduled_tasks_owner(cls) -> None:
        """给 user_id IS NULL 的定时任务补归属，否则用户态列表永远看不到它们。

        ``GET /agent/scheduled-tasks`` 用户态会过滤掉 user_id IS NULL 的行（视为
        平台/管理员行）。AI 经 capability_call 建任务的链路曾不落 user_id，留下一批
        用户自己看不见、也管不了的孤儿任务。两步补齐：

        1. 有 agent_id 的：按 agents.user_id 补（任务归属跟着 Agent 归属）；
        2. 仍为 NULL 的（agent_id 也丢了，如早期 CDP 链路）：挂到
           _ORPHAN_OWNER_USER_ID，与 migrate_orphan_agents_owner 同一约定。

        归属源头修复在 ToolRegistry.owner_user_id（对话传 caller、CDP 传客户端
        owner）+ add_job 的 Agent owner 兜底，本迁移只清理历史行。
        """
        if not cls.pool:
            return
        async with cls.pool.acquire() as conn:
            # 1) 按 Agent 归属补
            by_agent = await conn.execute(
                "UPDATE agent_scheduled_tasks t SET user_id=a.user_id "
                "FROM agents a WHERE t.agent_id=a.id "
                "AND t.user_id IS NULL AND a.user_id IS NOT NULL"
            )
            if by_agent and by_agent != "UPDATE 0":
                logger.info("[migrate] scheduled tasks reparented by agent owner: %s", by_agent)

            # 2) 剩余孤儿（无 agent_id 或 Agent 本身也是平台 Agent）挂到配置 owner
            owner = cls._ORPHAN_OWNER_USER_ID
            exists = await conn.fetchval(
                "SELECT 1 FROM mc_users WHERE id::text=$1 AND is_deleted=FALSE", owner
            )
            if not exists:
                remaining = await conn.fetchval(
                    "SELECT COUNT(*) FROM agent_scheduled_tasks WHERE user_id IS NULL"
                )
                if remaining:
                    logger.warning(
                        "[migrate] %s orphan scheduled tasks remain; owner %s not in mc_users",
                        remaining, owner,
                    )
                return
            rest = await conn.execute(
                "UPDATE agent_scheduled_tasks SET user_id=$1 WHERE user_id IS NULL", owner
            )
            if rest and rest != "UPDATE 0":
                logger.info("[migrate] orphan scheduled tasks reparented to %s: %s", owner, rest)

    @classmethod
    async def list_agents_for_caller(
        cls, user_id: str | None, team_id: str | None = None, limit: int = 200
    ) -> list[dict]:
        """列出调用者可见的 agent。

        user_id=None 表示管理员（不过滤，看全部）；user_id 为字符串表示 C 端用户，
        可见 = 自己创建的 + 团队共享（is_team_shared=true 且 team_id 匹配）。
        不再有 user_id IS NULL 的「平台 Agent」隐式语义——每个 agent 都有 owner，
        团队共享由显式 is_team_shared 表达。
        """
        async with cls.pool.acquire() as conn:
            if user_id is None:
                rows = await conn.fetch("SELECT * FROM agents ORDER BY id ASC LIMIT $1", limit)
            else:
                rows = await conn.fetch(
                    "SELECT * FROM agents WHERE user_id=$1 "
                    "OR (is_team_shared=TRUE AND team_id=$2) "
                    "ORDER BY id ASC LIMIT $3",
                    user_id, team_id, limit,
                )
        return [dict(r) for r in rows]

    @classmethod
    async def get_agent_owned(cls, user_id: str, agent_id: int) -> dict | None:
        """仅返回该用户自己创建的 agent（用于改/删鉴权）。平台 Agent（user_id IS NULL）不可改。"""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agents WHERE id=$1 AND user_id=$2", agent_id, user_id
            )
        return dict(row) if row else None

    @classmethod
    async def list_agent_llm_configs(cls) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM agent_llm_configs ORDER BY id ASC")
        return [dict(r) for r in rows]

    @classmethod
    async def get_agent_llm_config(cls, config_id: int) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agent_llm_configs WHERE id=$1", config_id)
        return dict(row) if row else None

    @classmethod
    async def get_default_agent_llm_config(cls) -> dict | None:
        """取第一个 enabled 的 LLM 配置作为默认。"""
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_llm_configs WHERE enabled=TRUE ORDER BY id ASC LIMIT 1"
            )
        return dict(row) if row else None

    @classmethod
    async def create_agent_llm_config(cls, **fields) -> dict:
        cols = [
            "name", "base_url", "chat_path", "api_key", "models",
            "protocol", "enabled", "timeout_seconds", "max_retries",
        ]
        vals = {k: v for k, v in fields.items() if k in cols}
        # 字段缺省补齐
        vals.setdefault("chat_path", "/chat/completions")
        vals.setdefault("protocol", "openai")
        vals.setdefault("enabled", True)
        cols_present = list(vals.keys())
        placeholders = []
        values = []
        for i, col in enumerate(cols_present, start=1):
            placeholders.append(f"${i}::jsonb" if col == "models" else f"${i}")
            values.append(cls._dumps(vals[col]) if col == "models" else vals[col])
        placeholders_sql = ", ".join(placeholders)
        col_list = ", ".join(cols_present)
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"INSERT INTO agent_llm_configs({col_list}) VALUES({placeholders_sql}) RETURNING *",
                *values,
            )
        return dict(row) if row else {}

    @classmethod
    async def update_agent_llm_config(cls, config_id: int, **fields) -> dict | None:
        allowed = {"name", "base_url", "chat_path", "api_key", "models", "protocol", "enabled", "timeout_seconds", "max_retries"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return await cls.get_agent_llm_config(config_id)
        updates["updated_at"] = "now()"
        # now() 不走参数占位
        set_parts = []
        values = []
        for k, v in updates.items():
            if v == "now()":
                set_parts.append(f"{k}=now()")
            else:
                placeholder = f"${len(values)+2}::jsonb" if k == "models" else f"${len(values)+2}"
                set_parts.append(f"{k}={placeholder}")
                values.append(cls._dumps(v) if k == "models" else v)
        set_clause = ", ".join(set_parts)
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE agent_llm_configs SET {set_clause} WHERE id=$1 RETURNING *",
                config_id, *values,
            )
        return dict(row) if row else None

    @classmethod
    async def delete_agent_llm_config(cls, config_id: int) -> bool:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "DELETE FROM agent_llm_configs WHERE id=$1 RETURNING id", config_id
            )
        return row is not None

    @classmethod
    async def list_agent_llm_models(cls, config_id: int | None = None, enabled_only: bool = False) -> list[dict]:
        where = []
        values: list = []
        if config_id is not None:
            values.append(config_id)
            where.append(f"m.llm_config_id=${len(values)}")
        if enabled_only:
            where.append("m.enabled=TRUE")
            where.append("c.enabled=TRUE")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT m.*,
                       c.name AS config_name, c.base_url, c.chat_path, c.protocol, c.enabled AS config_enabled
                FROM agent_llm_models m
                JOIN agent_llm_configs c ON c.id = m.llm_config_id
                {where_sql}
                ORDER BY c.id ASC, m.sort_order ASC, m.id ASC
                """,
                *values,
            )
        return [dict(r) for r in rows]

    @classmethod
    async def get_agent_llm_model(cls, model_id: int) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agent_llm_models WHERE id=$1", model_id)
        return dict(row) if row else None

    @classmethod
    async def get_agent_llm_model_with_config(cls, model_id: int) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT m.id AS llm_model_id, m.llm_config_id, m.model_name,
                       m.display_name, m.description, m.enabled AS model_enabled,
                       m.sort_order, m.metadata,
                       c.id, c.name, c.base_url, c.chat_path, c.api_key,
                       c.models, c.protocol, c.enabled AS config_enabled,
                       c.timeout_seconds, c.max_retries,
                       c.created_at AS config_created_at, c.updated_at AS config_updated_at
                FROM agent_llm_models m
                JOIN agent_llm_configs c ON c.id = m.llm_config_id
                WHERE m.id=$1
                """,
                model_id,
            )
        return dict(row) if row else None

    @classmethod
    async def find_agent_llm_model(cls, llm_config_id: int, model_name: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_llm_models WHERE llm_config_id=$1 AND model_name=$2",
                llm_config_id, model_name,
            )
        return dict(row) if row else None

    @classmethod
    async def create_agent_llm_model(cls, **fields) -> dict:
        cols = [
            "llm_config_id", "model_name", "display_name", "description",
            "enabled", "sort_order", "metadata",
        ]
        vals = {k: v for k, v in fields.items() if k in cols}
        vals.setdefault("display_name", vals.get("model_name"))
        vals.setdefault("description", "")
        vals.setdefault("enabled", True)
        vals.setdefault("sort_order", 0)
        vals.setdefault("metadata", {})
        cols_present = list(vals.keys())
        placeholders = []
        values = []
        for i, col in enumerate(cols_present, start=1):
            placeholders.append(f"${i}::jsonb" if col == "metadata" else f"${i}")
            values.append(cls._dumps(vals[col]) if col == "metadata" else vals[col])
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"INSERT INTO agent_llm_models({', '.join(cols_present)}) "
                f"VALUES({', '.join(placeholders)}) RETURNING *",
                *values,
            )
        return dict(row) if row else {}

    @classmethod
    async def update_agent_llm_model(cls, model_id: int, **fields) -> dict | None:
        allowed = {"model_name", "display_name", "description", "enabled", "sort_order", "metadata"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return await cls.get_agent_llm_model(model_id)
        updates["updated_at"] = "now()"
        set_parts = []
        values = []
        for k, v in updates.items():
            if v == "now()":
                set_parts.append(f"{k}=now()")
            else:
                placeholder = f"${len(values)+2}::jsonb" if k == "metadata" else f"${len(values)+2}"
                set_parts.append(f"{k}={placeholder}")
                values.append(cls._dumps(v) if k == "metadata" else v)
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE agent_llm_models SET {', '.join(set_parts)} WHERE id=$1 RETURNING *",
                model_id, *values,
            )
        return dict(row) if row else None

    @classmethod
    async def delete_agent_llm_model(cls, model_id: int) -> bool:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("DELETE FROM agent_llm_models WHERE id=$1 RETURNING id", model_id)
        return row is not None

    @classmethod
    async def list_agent_llm_role_mappings(cls, agent_id: int) -> list[dict]:
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.*, m.model_name, m.llm_config_id, c.name AS config_name,
                       m.enabled AS model_enabled, c.enabled AS config_enabled
                FROM agent_llm_role_mappings r
                LEFT JOIN agent_llm_models m ON m.id = r.llm_model_id
                LEFT JOIN agent_llm_configs c ON c.id = m.llm_config_id
                WHERE r.agent_id=$1
                ORDER BY r.role ASC
                """,
                agent_id,
            )
        return [dict(r) for r in rows]

    @classmethod
    async def get_agent_llm_role_mapping(cls, agent_id: int, role: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_llm_role_mappings WHERE agent_id=$1 AND role=$2",
                agent_id, role,
            )
        return dict(row) if row else None

    @classmethod
    async def upsert_agent_llm_role_mapping(cls, agent_id: int, role: str, llm_model_id: int | None) -> dict:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO agent_llm_role_mappings(agent_id, role, llm_model_id)
                VALUES($1, $2, $3)
                ON CONFLICT(agent_id, role) DO UPDATE SET
                    llm_model_id=EXCLUDED.llm_model_id,
                    updated_at=now()
                RETURNING *
                """,
                agent_id, role, llm_model_id,
            )
        return dict(row) if row else {}

    @classmethod
    async def delete_agent_llm_role_mapping(cls, agent_id: int, role: str) -> bool:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "DELETE FROM agent_llm_role_mappings WHERE agent_id=$1 AND role=$2 RETURNING id",
                agent_id, role,
            )
        return row is not None

    @classmethod
    async def resolve_agent_llm_role(cls, agent_id: int, role: str) -> dict | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT r.id AS role_mapping_id, r.agent_id, r.role,
                       m.id AS llm_model_id, m.model_name, m.display_name,
                       m.enabled AS model_enabled,
                       c.id AS llm_config_id, c.name, c.base_url, c.chat_path,
                       c.api_key, c.models, c.protocol, c.enabled AS config_enabled,
                       c.timeout_seconds, c.max_retries
                FROM agent_llm_role_mappings r
                JOIN agent_llm_models m ON m.id = r.llm_model_id
                JOIN agent_llm_configs c ON c.id = m.llm_config_id
                WHERE r.agent_id=$1 AND r.role=$2 AND m.enabled=TRUE AND c.enabled=TRUE
                """,
                agent_id, role,
            )
        return dict(row) if row else None

