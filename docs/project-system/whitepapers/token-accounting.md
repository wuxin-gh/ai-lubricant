# Token 计费口径白皮书

## 背景

项目同时处理 OpenAI、Anthropic 和多个上游渠道的 usage 字段。不同上游字段名不同，缓存读写字段也不同。如果没有统一内部口径，管理端展示、请求日志和统计会出现重复计算或错位。

## 问题

缓存读和缓存写曾被误当成 total 之外的额外 token 加项，导致 total 统计偏大。另一个问题是输入、输出、缓存读、缓存写在不同入口各自解析，字段映射不一致。

## 原则

- `total_tokens = prompt_tokens + completion_tokens`。
- `prompt_tokens` 表示输入 token 总数。
- `completion_tokens` 表示输出 token 总数。
- `cached_tokens` 是输入 token 的缓存读明细。
- `cache_creation_tokens` 是输入 token 的缓存写明细。
- 缓存读和缓存写要展示，但不能额外加到 total。
- 后端统计和前端展示必须使用同一套项目口径。

## 决策

内部归一化时按以下含义理解字段：

```text
raw_input = usage.prompt_tokens 或 usage.input_tokens
raw_output = usage.completion_tokens 或 usage.output_tokens
cached = usage.cached_tokens / usage.cache_read_input_tokens / details.cached_tokens
cache_creation = usage.cache_creation_tokens / usage.cache_creation_input_tokens / details.cache_creation_input_tokens
prompt = raw_input
completion = raw_output
total = prompt + completion
```

展示层可以显示输入、输出、缓存读、缓存写四类，但 total 只显示输入加输出。

## 非目标

- 不为每个上游定义独立计费语义。
- 不把缓存读写隐藏起来。
- 不在前端二次修正后端统计口径。

## Plans

- [`../plans/2026-06-01-project-documentation-system.md`](../plans/2026-06-01-project-documentation-system.md)
