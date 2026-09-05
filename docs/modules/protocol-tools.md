# 协议转换和工具调用

对应文件：`message_utils.py`、`tool_utils.py`

## 职责

该部分负责：

- OpenAI SSE chunk 生成。
- OpenAI 完整响应生成。
- Anthropic Messages 请求转换为 OpenAI messages。
- OpenAI 响应转换为 Anthropic response。
- OpenAI stream 转 Anthropic stream。
- 工具调用提示词生成。
- 从文本中解析工具调用。

## Anthropic 转换

`anthropic_to_openai_messages()` 会处理：

- `system`
- `messages`
- `tools`
- `thinking`
- `max_tokens`
- stop sequences

内部统一进入 `_chat_with_retry()`。

## 工具调用

`generate_tools_prompt()` 生成 XML 风格工具说明，让不支持原生 tools 的渠道通过文本输出：

```xml
<tool_call>
  <function=tool_name>
    <parameter=param>value</parameter>
  </function>
</tool_call>
```

`parse_tool_calls_from_content()` 支持：

- fenced `tool_call` JSON。
- `<tool_call><function=...>` XML。
- 裸 `<function=...>` XML。

