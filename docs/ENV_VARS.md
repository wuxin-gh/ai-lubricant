# 环境变量与配置清单

本文件是 Ai Lubricant **唯一权威的环境变量 / 配置项清单**，从源码反查整理：

- 数据服务（`python main.py`）读取 `bootstrap_config.py`
- 控制服务（`python -m node_server`）读取 `node_server/config.py`（独立解析，不 import `bootstrap_config`）

## 优先级铁律

**环境变量 > `.env` 字段 > 内置默认值**

- 环境变量始终优先，未设置才回落到 `.env` 对应 `[section]` 字段，再未命中用默认值。
- `.env` 已在 `.gitignore`（含明文密码，禁止入库）。模板见 `.env.example`，复制后填入真实值。
- 两进程共用同一份 `.env`、同一套 PG/Redis；**`node_control_token` 必须两进程同值**。

## 配置项总表

> 「进程」列：`数据` = 数据服务（main.py）读取，`控制` = 控制服务（node_server）读取，`两` = 两者都读。
> 「必填」列指：当该项无环境变量、无 ini 值、无默认值时启动是否报 `ConfigurationError`。

### `[server]` 数据服务监听

| 环境变量 | ini 字段 | 进程 | 必填 | 默认 | 说明 |
|---|---|---|---|---|---|
| — | `host` | 数据 | 否 | `0.0.0.0` | main.py 末尾 `uvicorn.run(app, host="0.0.0.0", port=8001)` 硬编码，ini 段实际未被 bootstrap_config 读取 |
| — | `port` | 数据 | 否 | `8001` | 同上，硬编码 8001 |

### `[postgres]` 业务数据库（两进程共享）

| 环境变量 | ini 字段 | 进程 | 必填 | 默认 | 说明 |
|---|---|---|---|---|---|
| `POSTGRES_HOST` | `host` | 两 | 是 | — | PG 主机 |
| `POSTGRES_PORT` | `port` | 两 | 是 | — | PG 端口，范围 1-65535 |
| `POSTGRES_USER` | `user` | 两 | 是 | — | PG 用户 |
| `POSTGRES_PASSWORD` | `password` | 两 | 是 | — | PG 密码 |
| `POSTGRES_DATABASE` / `POSTGRES_DB` | `database` | 两 | 是 | — | PG 库名（两个环境变量名都识别，前者优先） |
| `POSTGRES_POOL_MIN_SIZE` | `pool_min_size` | 数据 | 否 | 1 | 连接池最小 |
| `POSTGRES_POOL_MAX_SIZE` | `pool_max_size` | 数据 | 否 | 10 | 连接池最大，不可小于 min_size |

### `[redis]` 运行态真相源（两进程共享）

| 环境变量 | ini 字段 | 进程 | 必填 | 默认 | 说明 |
|---|---|---|---|---|---|
| `REDIS_HOST` | `host` | 两 | 是 | — | Redis 主机 |
| `REDIS_PORT` | `port` | 两 | 是 | — | Redis 端口，1-65535 |
| `REDIS_DB` | `db` | 两 | 是 | — | Redis db 编号，>=0 |
| `REDIS_MAX_CONNECTIONS` | `max_connections` | 两 | 是 | — | 单进程连接池上限，>=1 |
| `REDIS_STREAM_TIMEOUT` | `stream_timeout` | 数据 | 否 | 10 | 单条命令等回包秒数，超时自愈重连；不影响 pubsub |
| `REDIS_POOL_TIMEOUT` | `pool_timeout` | 数据 | 否 | 10 | 从池获取连接的最长等待秒数 |
| — | `prefix_key` | 两 | 是 | — | Redis key 前缀（ini-only，无环境变量） |
| — | `decode_responses` | 两 | 是 | — | bool，ini-only，必须 `true`/`false` |

### `[ai_lubricant]` 兼容层 + 节点控制面

| 环境变量 | ini 字段 | 进程 | 必填 | 默认 | 说明 |
|---|---|---|---|---|---|
| `AI_LUBRICANT_COMPAT_ENABLED` | `compat_enabled` | 数据 | 否 | false | 兼容层总开关 |
| `AI_LUBRICANT_USER_ADAPTER_ENABLED` | `user_adapter_enabled` | 数据 | 否 | false | 用户适配器开关 |
| `AI_LUBRICANT_DATABASE_URL` | `database_url` | 控制 | 否 | 复用主库 | mc_ 表独立库连接串（留空复用 `[postgres]`） |
| `AI_LUBRICANT_SYSTEM_USER_ID` | `system_user_id` | 数据 | 否 | `00000000-...001` | 系统用户 UUID |
| `AI_LUBRICANT_SYSTEM_USER_NAME` | `system_user_name` | 数据 | 否 | system | 系统用户名 |
| `AI_LUBRICANT_SYSTEM_USER_EMAIL` | `system_user_email` | 数据 | 否 | system@ai-lubricant.local | 系统用户邮箱 |
| `NODE_CONTROL_TOKEN`（legacy `AGENT_COMPOSE_NODE_API_TOKEN`） | `node_control_token` | **两** | 否* | — | 数据服务→控制服务内部 Bearer；**两进程必须同值**。缺失时控制服务首启自动生成并回写 |
| `NODE_CREDENTIAL_ENCRYPTION_KEY` | `node_credential_encryption_key` | 控制 | 否* | — | 节点 TOTP 凭据 AES-256 密钥，**生成后不可轮换**，否则已加密节点凭据全部失效。缺失自动生成回写 |
| `AGENT_COMPOSE_NODE_SERVER_PUBLIC_URL` | `node_server_public_url` | 控制 | 否 | — | 节点拨号地址（含端口），如 `http://<控制服务地址>:8003` |
| `AGENT_COMPOSE_BASE_URL` | `agent_compose_base_url` | 数据 | 否 | `http://127.0.0.1:8003` | 数据面→控制面地址 |
| `AGENT_COMPOSE_TIMEOUT` | `agent_compose_timeout` | 数据 | 否 | 30 | 控制面调用超时秒数 |
| `AGENT_COMPOSE_NODE_SERVER_ENABLED` | `node_server_enabled` | 控制 | 否 | true | 控制服务开关 |
| `NODE_CONTROL_HOST` | `node_control_host` | 控制 | 否 | `0.0.0.0` | 控制服务监听地址 |
| `NODE_CONTROL_PORT` | `node_control_port` | 控制 | 否 | 8003 | 控制服务监听端口 |
| `AGENT_COMPOSE_AGENT_IMAGE` | `agent_compose_agent_image` | 控制 | 否 | `ai-lubricant-node:local` | 节点 agent 镜像 |
| `AGENT_COMPOSE_NODE_BIN_DIR` | `agent_compose_node_bin_dir` | 控制 | 否 | — | 节点二进制目录 |
| `AGENT_ATTACHMENT_SIGNING_KEY` | — | 数据 | 否* | — | Agent 附件签名临时 URL 的 HMAC 密钥；**多实例必须同值**。附件 `content`/`thumbnail` 端点接受 `exp`+`sig` 免登录访问，消息 serve 时为每个 media part 注入 2h 短时签名 URL。缺失时启动自动生成随机值并回写 `.env`（多实例部署须显式配置同一值，否则各实例签发的 URL 互不通过） |
| `NODE_DEFAULT_SESSION_CPU` | `node_default_session_cpu` | 控制 | 否 | 1.0 | 节点会话默认 CPU（核） |
| `NODE_DEFAULT_SESSION_MEMORY` | `node_default_session_memory` | 控制 | 否 | 1073741824 | 节点会话默认内存（字节，1Gi） |
| `NODE_TERMINAL_MAX_ACTIVE_PER_NODE` | `node_terminal_max_active_per_node` | 控制 | 否 | 10 | 单节点最大活跃终端数，>=1 |
| `NODE_TERMINAL_DETACHED_TTL_SECONDS` | `node_terminal_detached_ttl_seconds` | 控制 | 否 | 1800 | 无浏览器挂载终端存活秒数，>=60 |
| — | `review_project_max_concurrency` | 数据 | 否 | 2 | 每项目 review 任务池最大并发（ini-only） |
| — | `bootstrap_admin_email` | 数据 | 否 | 空 | 首启种入管理员邮箱，空则不种（ini-only，幂等） |
| — | `bootstrap_admin_password` | 数据 | 否 | 空 | 首启管理员密码（ini-only） |
| — | `bootstrap_admin_name` | 数据 | 否 | admin | 首启管理员显示名（ini-only） |

> \* `node_control_token` / `node_credential_encryption_key` 虽非启动必填（控制服务会自动生成回写），但**一旦生成不得更改**；两进程共用同一份 .env 即可天然一致。

### `[clickhouse]` 请求 payload 双写（可选，默认关闭）

| 环境变量 | ini 字段 | 进程 | 必填 | 默认 | 说明 |
|---|---|---|---|---|---|
| `CLICKHOUSE_REQUEST_PAYLOAD_ENABLED` | `request_payload_enabled` | 数据 | 否 | false | payload 双写总开关；false 时完整请求/响应体不入 ClickHouse，请求详情无大字段 |
| `CLICKHOUSE_ADDR` | `addr` | 数据 | 否 | 空 | CH 地址 `host:port` |
| `CLICKHOUSE_DATABASE` | `database` | 数据 | 否 | `model_api_logs` | CH 库名 |
| `CLICKHOUSE_USERNAME` | `username` | 数据 | 否 | 空 | CH 用户 |
| `CLICKHOUSE_PASSWORD` | `password` | 数据 | 否 | 空 | CH 密码 |
| `CLICKHOUSE_REQUEST_PAYLOAD_TTL_DAYS` | `request_payload_ttl_days` | 数据 | 否 | 30 | payload 保留天数 |
| `CLICKHOUSE_MAX_PAYLOAD_BYTES` | `max_payload_bytes` | 数据 | 否 | 4194304 | 单条 payload 总字节上限，超出从最大字段开始截断（0 = 不限制） |

### `[marketplace]` / `[marketplace.consumer]` 资源市场（可选）

两级开关，详见 `.env.example` 注释：

| ini 字段 | section | 说明 |
|---|---|---|
| `repo_url` | marketplace | 仓库地址，填即「可看不可管」 |
| `github_branch` | marketplace | 分支，留空回退默认 |
| `github_token` | marketplace | 写入 token，填后出现 `/manager/marketplace-admin` 管理页 |
| `modules` | marketplace | 模块列表 `mcp,plugins,skills,channels,prompts,node-versions` |
| `index_name` | marketplace | 索引文件名 `index.json` |
| `repo_url` / `github_branch` / `modules` / `index_name` | marketplace.consumer | 消费侧只读公开 raw，留空回退 `[marketplace]` |

> marketplace 的环境变量名由兼容层（user_platform）解析，优先级同铁律；生产管理需 token，消费侧无需。
