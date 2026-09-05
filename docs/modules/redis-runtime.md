# Redis 和运行态缓存

对应文件：`rd.py`、`config.py`、`config_store.py`、`rate_limiter.py`

## 职责

Redis 用于跨请求、跨实例共享运行态：

- 配置缓存。
- 管理端 session。
- API Key RPM/RPD。
- 账号 RPM/TPM。
- 部分渠道 token、ticket、cookie。

## RedisJdbc

`rd.py::RedisJdbc` 继承 `coredis.Redis`，增加：

- key 前缀。
- 常用方法包装。
- list/hash/zset 方法。

`JdbcClient.ping()` 负责初始化全局 `JdbcClient.redis`。

## 限流 key

- `ratelimit:rpm:api-key:{key}`
- `ratelimit:rpm:api-key-day:{key}`
- `ratelimit:rpm:account:{provider}:{username}`
- `ratelimit:tpm:account:{provider}:{username}`
- `ratelimit:tpm:model:{model}`

