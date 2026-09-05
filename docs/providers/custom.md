# Custom 渠道

对应文件：`providers/custom.py`

## 定位

Custom 渠道是可配置的 OpenAI/Anthropic 兼容渠道，供管理端动态创建第三方 API 通道。

## 配置字段

常用字段：

- `provider_name` / `channel_name`
- `protocol`: `openai` 或 `anthropic`
- `base_url`
- `chat_path`
- `models_path`
- `model_source`: `api` 或 `custom`
- `custom_models`
- `model_redirects` / `model_aliases`
- `model_whitelist`
- `upstream_stream`
- `auto_update_models`
- `test_model`
- `advanced_headers`
- `balance`
- `api_key` / `key`

账号字段：

- `username`
- `password`，可作为 API Key 兜底
- `proxy`

## 认证和健康检查

`init_auth()` 主要检查 `api_key` 和 `base_url` 是否存在。`check_auth()` 调用 `health_check()`，通过一次小 token chat 请求判断可用性。

## 模型列表

模型来源：

- `model_source=custom`：使用配置中的 `custom_models`。
- 默认从 `models_path` 请求上游模型 API。
- 如果 API 模型列表为空但配置了 `custom_models`，使用自定义模型兜底。

模型会经过 alias/redirect 和 whitelist 处理。

## 聊天链路

OpenAI 协议：

- 构造 OpenAI payload。
- 请求 `chat_path`。
- 流式读取 SSE 或非流式 JSON。

Anthropic 协议：

- 将 OpenAI payload 转成 Anthropic request。
- 解析 Anthropic SSE 或 JSON。
- 转回 OpenAI response。

## 余额检查

`check_balance()` 根据配置访问余额接口，并通过 `response_path` 从 JSON 中提取余额，低于 `min_balance` 时禁用账号。

