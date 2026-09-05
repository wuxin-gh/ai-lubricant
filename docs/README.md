# 项目文档入口

这个目录是本仓库的长期文档入口。新对话、新开发者或 Codex 接手任务时，应先从这里进入，而不是直接散查源码。

## 推荐阅读路径

1. [项目文档中枢](./project-system/README.md)
2. [系统架构](./architecture.md)
3. [架构与数据流整改设计](./architecture-data-flows.md)
4. [系统模块索引](./modules/README.md)
5. [渠道文档索引](./providers/README.md)
6. [统一问题与改进清单](./issues.md)
7. [新对话交接指南](./guides/new-chat-handoff.md)

## 系统结构

- [公共 API 与应用入口](./modules/main-app.md)：`main.py`，OpenAI/Anthropic 兼容入口和公共接口。
- [架构与数据流整改设计](./architecture-data-flows.md)：按接口、数据流、缓存、DB、定时任务整理当前逻辑与整改方向。
- [管理端](./modules/admin.md)：`admin.py`、`static/admin.html`，配置、账号、模型和观测页面。
- [Provider 层](./modules/providers-layer.md)：`providers/`，所有上游渠道适配器。
- [路由、限流和账号池](./modules/routing-rate-limit.md)：`rate_limiter.py`，模型注册、账号池、重试和定时任务。
- [配置系统](./modules/config-storage.md)：`config.py`、`config_store.py`，文件、Redis、Postgres 配置存储。
- [数据库和持久化](./modules/database.md)：`db.py`，请求日志、配置和统计数据。
- [Redis 与运行态缓存](./modules/redis-runtime.md)：`rd.py` 和 Redis key 空间。
- [协议转换和工具调用](./modules/protocol-tools.md)：`message_utils.py`、`tool_utils.py`。
- [模型元数据](./modules/model-metadata.md)：`model_metadata.py`、`model_metadata.json`。
- [静态页面和脚本](./modules/static-scripts.md)：`static/`、`script/`。

## 渠道结构

- [渠道文档索引](./providers/README.md)
- 每个渠道文档只描述定位、配置、认证、模型列表、聊天链路和能力。
- 渠道问题、风险和改进点统一写入 [统一问题与改进清单](./issues.md)。

## 专题文档

- [项目文档中枢](./project-system/README.md)：四级文档体系、总计划、白皮书、实施计划和代码索引。
- [完善建议](./improvements.md)：较高层面的后续建设建议。
- [Codex 工作提示词](./codex-prompt.md)：目录结构、计划编写、改动规范和搜索规则。
- [新对话交接指南](./guides/new-chat-handoff.md)：开启新对话时如何让 AI 继续使用本项目的约定和模式。
- [指南索引](./guides/README.md)：跨模块工作指南和交接说明。
