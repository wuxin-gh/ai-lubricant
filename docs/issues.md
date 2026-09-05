# 统一问题与改进清单

本文集中记录项目文档梳理时发现的问题、风险和后续改进建议。渠道文档和模块文档只保留职责、流程和能力说明，问题统一维护在这里。

## 使用方式

- 处理缺陷、重构或安全问题前，先查看本清单。
- 新发现的问题追加到对应分类，不再分散写入单个渠道或模块文档。
- 已解决的问题应移动到“已处理”小节，或在相关 PR/提交中注明。

## BaseProvider 基类

来源：[docs/providers/base.md](../docs/providers/base.md)

- `BaseProvider.chat()` 逻辑较长，混合了流式协议、工具调用修复、usage、日志和特殊渠道兜底，建议拆成更小的 stream normalizer。
- 工具调用 XML 解析较依赖模型输出格式，缺少覆盖复杂边界的测试。

## Custom 渠道

来源：[docs/providers/custom.md](../docs/providers/custom.md)

- `health_check()` 会真实消耗上游请求额度，建议支持独立 health endpoint 或轻量模型。
- `advanced_headers`、API Key 和日志脱敏需要更强约束。
- `model_redirects` 和 `model_aliases` 混用，命名语义容易混淆。
- Anthropic 协议转换只覆盖常用字段，复杂 tool/result 场景需要测试。
- 余额检查频率配置存在，但当前由账号检查循环触发，缺少单独调度和缓存策略文档。

## Gemini 渠道

来源：[docs/providers/gemini.md](../docs/providers/gemini.md)

- 多模态消息目前主要被拼成文本 prompt，真实文件/图片上传能力未完整接入。
- `function_calling=True` 主要依赖文本工具调用解析，不代表 Gemini 原生函数调用已适配。
- Cookie 属于高敏感凭据，配置和日志需严格脱敏。
- `gemini_webapi` 是外部逆向库，升级或接口变化可能影响运行。
- 错误映射主要靠字符串判断 429/rate，建议细化异常类型。

## Jiekou 渠道

来源：[docs/providers/jiekou.md](../docs/providers/jiekou.md)

- 非流式调用没有过滤初始 `{}` 空 chunk，当前聚合时依赖 `chunk.get`，还算安全但不够清晰。
- token 认证错误和额度错误没有统一映射到 401/429。
- 上游 free-trial 接口可能额度低，需更明确地接入账号级 cooldown 或 quota。

## Puter 渠道

来源：[docs/providers/puter.md](../docs/providers/puter.md)

- `check_auth()` 只判断 token 存在，没有请求上游验证。
- `fetch_model_list()` 请求没有带 `auth_token`，如果上游未来要求认证会失败。
- `_call_driver()` 对二进制返回直接返回 bytes，OpenAI Images 接口可能无法直接 JSON 序列化。
- 流式按行 JSON 解析缺少错误事件映射。

## Qwen 渠道

来源：[docs/providers/qwen.md](../docs/providers/qwen.md)

- `init_auth()` 假设 `JdbcClient.redis` 一定可用，Redis 未初始化或不可用时缺少降级。
- 非流式也走 stream 聚合，逻辑可复用但会增加链路复杂度。
- 登录和请求异常很多地方只记录日志后返回空值，调用方可能得到不明确错误。
- 会话删除使用 detached task，失败不会影响主响应，也不易追踪。

## Tabbit 渠道

来源：[docs/providers/tabbit.md](../docs/providers/tabbit.md)

- 默认 `client_id` 和 `client_secret` 硬编码在源码中，应移入配置。
- 文件底部有手工测试代码和真实 refresh token，应立即移除并轮换。
- RSC 提取 session_id 依赖页面返回文本格式，极易受前端升级影响。
- Header 中包含多个模拟浏览器和追踪字段，缺少集中维护。
- 非流式不支持上游原生非流式，只是聚合流式。

## Xiaomi 渠道

来源：[docs/providers/xiaomi.md](../docs/providers/xiaomi.md)

- 代码中有大量 `Xiaomi.DEBUG.*` 级别的完整请求和 SSE 内容日志，可能泄漏用户内容，建议改为 debug 且受 `system.debug` 控制。
- 使用 `assert resp['code'] == 0` 处理上游错误，生产环境应抛明确 HTTPException。
- 工具调用不稳定，基类中存在小米专属非流式兜底，建议改成渠道能力 hook。
- `fetch_model_list()` 对上游字段结构假设较强，缺少缺字段保护。

## 管理端

来源：[docs/modules/admin.md](../docs/modules/admin.md)

- 入参大量使用裸 `dict` 和 query 参数，应补 Pydantic schema。
- Provider 名称校验已有，但配置字段校验不足。
- 多个接口修改配置后异步触发任务，任务失败不会反馈给前端。
- admin 操作日志捕获较宽松，失败时直接吞掉异常。
- `static/admin.html` 单文件很大，建议拆分前端资源和接口层。

## 配置系统

来源：[docs/modules/config-storage.md](../docs/modules/config-storage.md)

- 文件配置、Postgres 配置和 Redis 缓存之间的优先级需要在 README 中明确。
- `PostgresConfigStore.write_main()` 只把部分主配置写回文件，容易让本地文件和数据库不一致。
- Provider 旧配置迁移逻辑存在，但缺少操作文档。
- 配置字段缺少 schema，管理端和运行时都靠约定。
- 当前仓库配置文件包含敏感信息，应清理并提供安全模板。

## 数据库和持久化

来源：[docs/modules/database.md](../docs/modules/database.md)

- 建表逻辑同时存在于 `db.py` 和 `sql/init.sql`，需要明确哪个是权威迁移来源。
- 没有版本化迁移工具，后续 schema 演进风险较高。
- request_logs 可能保存大量请求/响应内容，需要更严格的保留和脱敏策略。
- 数据库连接失败时配置系统会退回文件，但很多运行功能仍依赖数据库，应提供 readiness 检查。
- 统计 SQL 较集中在一个类中，后续可拆查询服务。

## 公共 API 与应用入口

来源：[docs/modules/main-app.md](../docs/modules/main-app.md)

- `main.py` 仍然承担路由、日志、协议转换、重试等多职责，建议拆分为 routers、services、logging 三层。
- `ModelClientPool.initialize()` 用 `asyncio.create_task()` detached 启动，异常不易发现。
- 文档原文接口需要持续维护 allowlist 或改成安全目录读取。
- `/v1/token/count` 是字符估算，准确度有限。
- 图片接口没有完整请求日志记录，和 chat/messages 不一致。

## 模型元数据

来源：[docs/modules/model-metadata.md](../docs/modules/model-metadata.md)

- `provider` 参数目前未参与实际选择，未来可支持 provider-specific metadata。
- 未配置模型只使用默认值，可能导致 max_tokens 或能力标记不准确。
- 缺少对 `model_metadata.json` 的 schema 校验。
- warning 中疑似判断 `4086`，默认值是 `4096`，可能是拼写错误。
- 能力字段与 Provider 实际能力可能不一致，需要统一校验。

## 协议转换和工具调用

来源：[docs/modules/protocol-tools.md](../docs/modules/protocol-tools.md)

- 工具 XML 不是标准协议，依赖模型遵循提示词。
- 解析逻辑用正则处理嵌套/复杂 JSON，边界情况需要测试。
- Anthropic 和 OpenAI 的 tool/result 语义不完全等价，复杂工具链可能丢字段。
- token 估算和 usage 兼容逻辑分散在 `main.py` 和 `message_utils.py`。
- 建议增加协议转换单元测试和 golden fixtures。

## Provider 层

来源：[docs/modules/providers-layer.md](../docs/modules/providers-layer.md)

- 能力声明不统一，管理端和路由层无法可靠知道某渠道是否支持图片、工具、thinking、search。
- 各渠道错误映射不一致，有的抛 HTTPException，有的返回空内容。
- 上游请求字段缺少统一 schema 和回归测试。

## Redis 和运行态缓存

来源：[docs/modules/redis-runtime.md](../docs/modules/redis-runtime.md)

- Redis 不可用时多个模块降级为内存，但多实例一致性会丢失。
- 部分代码直接访问 `JdbcClient.redis`，缺少空值保护。
- key 前缀和业务 key 前缀混用，需要统一命名规范。
- session、限流、配置缓存、渠道 token 混在同一个 Redis DB，建议文档化 key 空间。
- 需要管理端展示 Redis 连接和降级状态。

## 路由、限流和账号池

来源：[docs/modules/routing-rate-limit.md](../docs/modules/routing-rate-limit.md)

- 文件职责过多，建议拆分 limiter、account、provider_pool、model_router、scheduler。
- 多处 `asyncio.gather()` 没有 await，任务异常可能丢失。
- 定时任务间隔变更后，需要重启或重建任务才会生效。
- 权重评分策略较复杂，缺少可解释的管理端展示。
- Redis 降级为内存后，多实例限流和配额会不一致。

## 静态页面、脚本和辅助目录

来源：[docs/modules/static-scripts.md](../docs/modules/static-scripts.md)

- `static/admin.html` 文件过大，建议拆分为模块化前端。
- `script/` 中脚本命名不统一，有 `1.py`、`2.py`、`111.py` 这类临时文件。
- `wispbyte/` 与模型 API 领域不同，建议移到独立目录或补说明。
- 多个脚本中存在真实 URL、token、账号样例，需清理。

