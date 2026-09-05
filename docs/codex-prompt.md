# Codex 工作提示词

以后在本仓库工作时，优先使用这份仓库级提示词。

## 仓库背景

这是一个基于 FastAPI 的多模型代理服务，统一暴露 OpenAI 兼容和 Anthropic 兼容接口，并把请求转发到多个上游渠道。

关键路径：

- `main.py`：公共 API 路由、请求生命周期、鉴权、重试、日志。
- `admin.py`：管理端 API 和管理页面入口。
- `providers/`：渠道适配器。新增渠道应继承 `providers/base.py::BaseProvider`。
- `rate_limiter.py`：API Key 限流、账号池、渠道池、模型注册、路由选择、重试和定时任务。
- `config.py`、`config_store.py`：配置门面和配置存储层。
- `db.py`、`sql/init.sql`：Postgres 表结构和持久化逻辑。
- `rd.py`：Redis 包装和连接初始化。
- `message_utils.py`：OpenAI、Anthropic、SSE 格式转换。
- `tool_utils.py`：工具调用提示词生成和工具调用解析。
- `docs/`：长期文档。开始修改前先读。
- `plans/`：有状态计划目录。多步骤任务需要创建或更新计划。

开始改代码前，按顺序读取：

1. `docs/architecture.md`
2. `docs/improvements.md`
3. `plans/` 下相关计划
4. 相关源码文件

## 文档使用和搜索规则

查找项目知识时按这个顺序：

1. 用 `rg` 搜索 `docs/`。
2. 搜索 `plans/`，确认是否已有计划或历史决策。
3. 搜索源码。
4. 代码行为明确后，再查看配置示例。

不要把 `providers/README.md` 当成当前项目架构说明。它更像上游 Gemini 包说明，不代表本仓库整体设计。

## 计划目录规范

多步骤任务应在 `plans/` 下创建或更新 Markdown 计划。

命名规则：

- 使用 `plans/YYYY-MM-DD-short-topic.md`。
- topic 使用小写英文和中划线。

状态值：

- `pending`：未开始。
- `in_progress`：正在处理。
- `blocked`：被依赖或决策阻塞。
- `done`：已完成并验证。
- `skipped`：有意跳过，需要写明原因。

模板：

```md
# Plan: Short Topic

Created: YYYY-MM-DD
Updated: YYYY-MM-DD
Status: in_progress

## Goal

一句或一段话描述目标。

## Context

相关文件、文档、假设、约束和依赖。

## Tasks

- [ ] pending: task description
- [ ] in_progress: task description
- [ ] blocked: task description and blocker
- [x] done: completed task description

## Decisions

- YYYY-MM-DD: decision and reason.

## Verification

- command/result or manual verification note.

## Follow-ups

- optional next work.
```

任务推进时同步更新计划。回合结束时不能留下过期的 `in_progress` 项。

## 改动规范

- 改动范围保持贴合需求，不做无关重构。
- 保持现有 Provider 契约，除非任务明确要求调整。
- 公共 API 行为应保持 OpenAI 和 Anthropic 兼容。
- 渠道代码默认通过 `BaseProvider.chat()` 做统一输出规范化。
- 配置行为变化时，同步更新 `docs/architecture.md` 或新增配置说明。
- 新增会调用上游或影响用量的接口时，应考虑请求日志记录。
- 新增 Provider 字段时，同时考虑管理端、配置存储和文档。
- 不提交账号密码、Cookie、Token、私有上游 Key 等敏感信息。
- 新增管理端或公共 API 请求体时，优先使用 Pydantic 模型。
- 需要跨实例共享或重启保留的运行态限制，优先使用 Redis。

## 验证规范

按影响面选择验证方式：

- 纯文档或静态查看器：检查文件可读、页面可打开、Markdown 能渲染。
- 路由变化：测试路由匹配、API Key 作用域、模型组和重试。
- 渠道变化：至少补一个 mock 测试或定向 smoke test。
- 存储变化：检查迁移 SQL，并在可用环境中做初始化或迁移测试。
- 公共 API 变化：验证流式和非流式响应兼容性。

验证结果应记录到当前计划。
