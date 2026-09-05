# 配置系统

对应文件：`config.py`、`config_store.py`、`config.json`、`config/`

## 职责

配置系统负责读取和写入：

- 主配置。
- Provider 配置。
- Provider 账号。
- API Key 缓存。

## 存储链路

`CONFIG_STORE = RedisConfigStore(PostgresConfigStore(FileConfigStore(CONFIG_FILE)))`

含义：

1. Redis 缓存优先，TTL 300 秒。
2. Postgres 是主要持久化。
3. `config.json` 是主配置兜底。

## 主配置

常见字段：

- `postgres`
- `redis`
- `server`
- `api_keys`
- `model_routes`
- `model_groups`
- `model_refresh`
- `message_delete`
- `logging`
- `system`
- `data_retention`

## Provider 配置

Provider 配置会拆成：

- `provider_configs`：基础配置、限流、别名、白名单。
- `provider_accounts`：账号数组拆行。

`RedisConfigCache` 对基础配置写入、账号 upsert 和账号删除提供异步透传接口。写入 Postgres 后同步刷新对应 Provider 的 Redis 缓存，避免管理端窄写接口因缓存包装层缺失方法而失败。


