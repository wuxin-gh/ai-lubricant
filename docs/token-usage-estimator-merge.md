# Token usage 估算合并说明

## 文件

独立验证脚本在：`script/token_usage_estimator.py`。

它不导入 `main.py` / `config.py`，用于在合并前单独验证 OpenAI-style request body 的多种 token 估算口径。

## 怎么验证

直接编辑脚本顶部的 `DEBUG INPUT` 区域，然后运行：

```bash
python script/token_usage_estimator.py
```

主要变量：

- `REQUEST_BODY`：直接粘贴请求体 dict。
- `REQUEST_BODY_FILE`：从文件读取请求体；相对路径按项目根目录解析。
- `REQUEST_MODEL_NAME`：手动指定客户端请求模型；为空则用请求体里的 `model`。
- `ACTUAL_MODEL_NAME`：手动指定上游实际模型。
- `CALC_TYPES`：选择输出哪些计算口径。
- `TIKTOKEN_ENCODINGS`：选择要对比的 tiktoken encoding。

## 输出里的几种口径

- `current_main_fallback`：复刻当前 `main.py` 的 fallback 行为。当前 `_estimate_tokens_from_text()` 传空 model，所以基本会走 `chars/2`。
- `request_model_rule`：按请求体里的 `body.model` 或 `REQUEST_MODEL_NAME` 匹配 tokenizer 规则。
- `actual_model_rule`：按 `ACTUAL_MODEL_NAME` 指定的上游实际模型匹配 tokenizer 规则。
- `prompt_text_cl100k_base` / `prompt_text_o200k_base`：对当前 `_estimate_usage()` 拼出来的 prompt 文本直接跑 tiktoken。
- `chat_structure_plus_tools_*`：近似 OpenAI Chat Completions 消息结构 + tools schema 的 prompt token。
- `calibrated_gpt55_*`：基于 `script/test_data` 样本校准后的 GPT-like 口径：内容 token + tools schema + 75% assistant tool_calls + 2 tokens/message。
- `raw_body_compact_*` / `raw_body_pretty_*`：把完整 JSON body 当文本算，只做参考，不适合直接计费。

## 当前样本结论

`script/test_data/1.json` 到 `7.json` 的上游 prompt_tokens 已写入脚本顶部 `TEST_DATA_EXPECTED_PROMPT_TOKENS`。当前推荐口径是：

```python
GPT_LIKE_FINAL_CALC = "calibrated_gpt55_o200k_base"
```

这 7 组样本的批量结果：

- 平均绝对误差：约 509 tokens
- 最大绝对误差：1313 tokens
- 最大相对误差：约 2.71%（7.json）
- 1.json / 2.json / 3.json / 4.json 误差都小于 0.1%

校准后发现：

- 单纯 `messages_only_o200k` 会漏算 tools schema / assistant tool_calls 等内容。
- 单纯 `messages + tools` 又会把 assistant tool_calls 全量 JSON 结构算得偏大。
- 当前最贴近样本的是：`system + user + assistant_content + tool_content + tools + assistant_tool_calls * 0.75 + message_count * 2`。

## 当前代码问题点

当前日志 fallback usage 入口：

- `main.py` 的 `_usage(result, request_body, estimate=True)`
- 上游没有 usage 时进入 `_estimate_usage(request_body, response_body)`
- `_estimate_usage()` 调 `_estimate_tokens_from_text(prompt_text)`
- `_estimate_tokens_from_text()` 当前写死 `_estimate_request_part_tokens("", text)`

因此 fallback 估算没有按请求模型，也没有按上游实际模型，而是按空 model 匹配规则。

## 建议合并方向

等验证脚本确认误差可接受后，再合并运行时代码：

1. 让 `_estimate_tokens_from_text()` 接收 `model` 参数，而不是传空字符串。
2. `_estimate_usage()` 接收 `model` 或 `actual_model` 参数。
3. 优先用上游实际模型；没有实际模型时用客户端请求模型。
4. 保持现有原则：上游返回 usage 时永远优先使用上游 usage；只有没有 usage 才估算。
5. 对日志详情继续区分：数据库 token 标签显示规范化后的字段；`Usage` 折叠区显示响应体原始 usage。

## 合并时需要重点改的位置

- `main.py`：`_estimate_tokens_from_text()`、`_estimate_usage()`、`_usage()` 的参数链路。
- `main.py`：非流式和流式调用 `_usage(...)` 的位置，把 request model / actual model 传进去。
- `tests/`：增加覆盖用例：
  - 上游 usage 存在时不估算。
  - request model 命中 OpenAI-like 规则时用 tiktoken。
  - actual model 存在时优先 actual model。
  - 不命中规则时仍回退 chars/token。
