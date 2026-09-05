# BaseProvider 基类

对应文件：`providers/base.py`

## 定位

`BaseProvider` 是所有渠道的抽象基类，负责定义 Provider 契约，并把各渠道返回的内容统一转换成 OpenAI Chat Completions 风格的响应。

## 核心契约

子类必须实现：

- `init_auth(is_check=False)`
- `check_auth()`
- `fetch_model_list()`
- `_do_stream_chat(model_id, messages, **kwargs)`
- `_do_non_stream_chat(model_id, messages, **kwargs)`

可选覆盖：

- `fetch_upstream_model_list()`
- `clear_conversations(max_age_hours)`
- `check_message()` / `record_message()`
- `update_quota_from_headers()`
- `generate_image()` 等多媒体能力

## 统一输出逻辑

`chat()` 是统一入口：

- 流式时先等待渠道连接成功，再输出 OpenAI role chunk。
- 将渠道 chunk 中的 `content`、`thinking`、`tool_calls`、`usage` 转为 OpenAI SSE。
- 支持 `reasoning_content` 输出。
- 支持工具调用合并、完整性判断和文本兜底解析。
- 结束时输出 usage chunk、finish chunk 和 `[DONE]`。
- 非流式时直接委托 `_do_non_stream_chat()`。

## 辅助能力

- `send_sse_request()`：通用 SSE 请求读取器，支持记录上游请求体、响应体和响应头。
- `parse_sse_data()`：解析 OpenAI/Qwen 类 SSE。
- `prepare_messages()`：把多轮消息拼接成单文本。
- `prepare_provider_messages()`：给支持多消息的渠道保留消息数组。
- `build_tools_prompt()` / `parse_tool_calls()`：工具调用提示词和解析。

