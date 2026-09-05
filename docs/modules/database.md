# 数据库和持久化

对应文件：`db.py`、`sql/init.sql`

## 职责

数据库层负责：

- 初始化 asyncpg 连接池。
- 创建和迁移表结构。
- 主配置读写。
- Provider 配置和账号读写。
- 请求日志写入和查询。
- 统计、账单、账号用量。
- API Key 管理。
- 操作日志。
- 日志清理。

## 主要表

- `app_config`
- `provider_configs`
- `provider_accounts`
- `request_logs`
- `api_keys`
- `operation_logs`

## Provider 配置拆表

`provider_configs` 保存 Provider 基础配置，`provider_accounts` 保存账号。读取时会把账号合并回 `accounts` 数组。

## 请求日志

`request_logs` 保存：

- 请求和响应体。
- router request/response body。
- request/response headers。
- retry path。
- usage。
- 错误。
- first token time。

