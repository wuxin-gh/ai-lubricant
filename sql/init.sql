-- 创建数据库（首次执行时需要取消注释）
-- CREATE DATABASE "ai-lubricant";

CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS provider_configs (
    name TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT true,
    rate_limit JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_aliases JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_whitelist JSONB NOT NULL DEFAULT '[]'::jsonb,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
);

CREATE INDEX IF NOT EXISTS idx_provider_accounts_provider
    ON provider_accounts(provider_name);

CREATE INDEX IF NOT EXISTS idx_provider_accounts_username
    ON provider_accounts(username);

-- 将旧 app_config(provider:*) 中的渠道基础配置导入 provider_configs。
INSERT INTO provider_configs(name, enabled, rate_limit, model_aliases, model_whitelist, config, updated_at)
SELECT
    split_part(key, ':', 2) AS name,
    coalesce((data->>'enabled')::boolean, true) AS enabled,
    coalesce(data->'rate_limit', '{}'::jsonb) AS rate_limit,
    coalesce(data->'model_aliases', '{}'::jsonb) AS model_aliases,
    coalesce(data->'model_whitelist', '[]'::jsonb) AS model_whitelist,
    data - 'accounts' - 'enabled' - 'rate_limit' - 'model_aliases' - 'model_whitelist' AS config,
    now() AS updated_at
FROM app_config
WHERE key LIKE 'provider:%'
ON CONFLICT(name) DO NOTHING;

-- 将旧 app_config(provider:*) 中的账号数组导入 provider_accounts。
INSERT INTO provider_accounts(provider_name, username, switch, priority, weight, account, updated_at)
SELECT
    split_part(c.key, ':', 2) AS provider_name,
    acc.value->>'username' AS username,
    coalesce((acc.value->>'switch')::boolean, true) AS switch,
    coalesce((acc.value->>'priority')::integer, 0) AS priority,
    coalesce((acc.value->>'weight')::integer, 1) AS weight,
    acc.value - 'username' - 'switch' - 'priority' - 'weight' AS account,
    now() AS updated_at
FROM app_config c
CROSS JOIN LATERAL jsonb_array_elements(coalesce(c.data->'accounts', '[]'::jsonb)) AS acc(value)
WHERE c.key LIKE 'provider:%'
  AND coalesce(acc.value->>'username', '') <> ''
ON CONFLICT(provider_name, username) DO NOTHING;

CREATE TABLE IF NOT EXISTS request_logs (
    id BIGSERIAL PRIMARY KEY,
    request_id TEXT NOT NULL,
    parent_request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_key TEXT,
    api_key_name TEXT,
    provider_name TEXT,
    account_username TEXT,
    model TEXT,
    endpoint TEXT,
    success BOOLEAN NOT NULL DEFAULT false,
    status TEXT,
    stream BOOLEAN NOT NULL DEFAULT false,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    first_token_ms INTEGER,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    cached_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    upstream_status TEXT,
    error TEXT
);

-- Legacy payload columns are intentionally absent: bodies/headers live in ClickHouse.


ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS cache_creation_tokens INTEGER NOT NULL DEFAULT 0;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS parent_request_id TEXT;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS first_token_ms INTEGER;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS request_headers JSONB;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS router_request_headers JSONB;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS response_headers JSONB;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS upstream_status TEXT;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS router_request_body JSONB;

ALTER TABLE request_logs
    ADD COLUMN IF NOT EXISTS router_response_body JSONB;

CREATE INDEX IF NOT EXISTS idx_request_logs_created_at
    ON request_logs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_request_logs_provider
    ON request_logs(provider_name);

CREATE INDEX IF NOT EXISTS idx_request_logs_account
    ON request_logs(account_username);

CREATE INDEX IF NOT EXISTS idx_request_logs_api_key
    ON request_logs(api_key);

CREATE INDEX IF NOT EXISTS idx_request_logs_model
    ON request_logs(model);

CREATE INDEX IF NOT EXISTS idx_request_logs_success
    ON request_logs(success);

CREATE TABLE IF NOT EXISTS notifications (
    id BIGSERIAL PRIMARY KEY,
    severity TEXT NOT NULL DEFAULT 'info',
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
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
);

CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status);
CREATE INDEX IF NOT EXISTS idx_notifications_severity ON notifications(severity);
CREATE INDEX IF NOT EXISTS idx_notifications_dedupe_key ON notifications(dedupe_key);
CREATE INDEX IF NOT EXISTS idx_notifications_request_log_id ON notifications(request_log_id);

CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    rate_limit JSONB NOT NULL DEFAULT '{}'::jsonb,
    disabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key);

CREATE TABLE IF NOT EXISTS operation_logs (
    id BIGSERIAL PRIMARY KEY,
    operator TEXT,
    action TEXT NOT NULL,
    target_type TEXT,
    target_name TEXT,
    old_data JSONB,
    new_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_operation_logs_created_at ON operation_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operation_logs_action ON operation_logs(action);
CREATE INDEX IF NOT EXISTS idx_operation_logs_target ON operation_logs(target_type, target_name);

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
);

CREATE INDEX IF NOT EXISTS idx_security_events_time ON security_events(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_security_events_tag ON security_events(tag);
CREATE INDEX IF NOT EXISTS idx_security_events_severity ON security_events(severity);
CREATE INDEX IF NOT EXISTS idx_security_events_request_log_id ON security_events(request_log_id);
