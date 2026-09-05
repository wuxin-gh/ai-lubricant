# Provider 层

对应目录：`providers/`

## 职责

Provider 层负责把统一内部请求转换成各上游可接受的协议，并把上游响应转换回统一格式。

核心文件：

- `base.py`：Provider 抽象基类和统一输出。
- `custom.py`：通用 OpenAI/Anthropic API 兼容渠道。
- `quota.py`：配额追踪抽象。
- 各渠道文件：具体上游适配。

## 设计边界

Provider 应负责：

- 认证。
- 模型列表。
- 聊天请求 payload。
- 上游响应解析。
- 上游特有配额/会话/多媒体能力。

Provider 不应负责：

- API Key 校验。
- 全局模型路由。
- 全局请求日志持久化。
- 管理端参数校验。

## 输出约定

`_do_stream_chat()` yield：

- `{}`：表示上游连接成功，可以输出 role chunk。
- `{"content": "...", "thinking": "...", "tool_calls": [...], "usage": {...}}`

`_do_non_stream_chat()` 返回 OpenAI Chat Completion dict。

