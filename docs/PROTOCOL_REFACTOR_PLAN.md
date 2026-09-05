# 自定义渠道协议转换与 thinking 参数重构完整计划

## 背景

自定义渠道（`CustomProvider`）支持 `openai` / `anthropic` / `responses` / `gemini` 四个标准协议。当前实现存在以下设计问题：

1. **协议字段映射不统一**：跨协议转换时各协议特色字段的处理分散在各处，缺少完整的映射表
2. **非标准参数硬塞进通用路径**：`thinking_mode`/`thinking_enabled`/`auto_thinking`/`thinking_format`/`auto_search`/`research_mode`/`thinking_budget` 等参数出现在通用 payload 白名单中
3. **强制注入默认参数**：`_build_openai_kwargs` 无差别注入非标准参数，即使请求体完全没有这些字段
4. **体系分类无意义**：`classify_thinking_system` + `apply_thinking_system_params` 在通用代理层做模型体系分类，逻辑有误
5. **兼容 hack**：`minimal_model_params` 机制是协议不通时的权宜之计，协议通了后不需要
6. **模拟客户端过度干预**：codex-cli 分支强制填充 thinking 参数，混淆了客户端模拟与协议转发的职责
7. **缺少渠道支持协议列表**：渠道不知道自己支持哪些协议，无法做协议匹配和直通判断

---

## 核心问题清单

| 问题 | 位置 | 严重度 |
|------|------|--------|
| `OPENAI_PAYLOAD_KEYS` 包含非标准参数 | `custom.py:206-207` | P0 |
| `_build_openai_kwargs` 强制注入非标准参数 + reasoning_effort → thinking_mode 映射 | `main.py:1621-1721` | P0 |
| `_apply_api_key_thinking` 注入非标准参数 | `main.py:1096-1138` | P0 |
| `classify_thinking_system` + `apply_thinking_system_params` 体系分类 | `message_utils.py:852` + `custom.py:1385-1406` | P1 |
| `codex-cli` 分支强制填充非标准参数 | `custom.py:1408-1419` | P1 |
| `minimal_model_params` + `MINIMAL_MODEL_PARAMS_STRIP_FIELDS` 兼容 hack | `config.py:10,424` + `main.py:2188-2198` | P1 |
| 路由 `thinking_mode` 注入非标准参数 | `main.py:2175-2187` | P1 |
| 渠道无协议列表配置，无法做直通判断 | 新需求 | P1 |

---

## 重构原则

1. **不做黑白名单，只做协议映射**：能映射到目标协议的参数保留，映射不了直接丢弃
2. **按协议做默认值**：每个协议有自己的默认参数，不按模型体系分类
3. **同协议直通**：客户端协议 == 渠道支持的协议时直接转发 payload（补默认参数 + headers）
4. **跨协议转换**：客户端协议 ≠ 渠道协议时，按映射表做字段转换
5. **Qwen/eaichat/puter 参数转化归属各自 provider 内部**，不进通用代理
6. **模拟客户端只做协议绑定**：有模拟时用其 headers + 协议；无模拟时走渠道协议（协议可设默认客户端）
7. **移除 `minimal_model_params` 机制**：协议通了后不需要

---

## 四协议字段完整映射

### 一、消息结构映射

#### OpenAI → messages

```json
{
  "messages": [
    {"role": "system", "content": "..."},
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "...", "tool_calls": [...]},
    {"role": "tool", "content": "...", "tool_call_id": "..."}
  ]
}
```

#### Anthropic → messages + system

```json
{
  "system": "...",
  "messages": [
    {"role": "user", "content": [{"type": "text", "text": "..."}, {"type": "image", "source": {...}}]},
    {"role": "assistant", "content": [{"type": "text", "text": "..."}, {"type": "thinking", "thinking": "...", "signature": "..."}, {"type": "tool_use", "id": "...", "name": "...", "input": {...}}]},
    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "...", "content": "..."}]}
  ]
}
```

映射规则：
- `system` message → Anthropic `system` 字段
- `tool_calls` → `tool_use` blocks（`id` → `tool_use_id`，`function` → `name`+`input`）
- `tool` message → `tool_result` block（`tool_call_id` → `tool_use_id`）
- `reasoning_content` → `thinking` block
- content 格式差异：OpenAI `string`/`list` → Anthropic `blocks`
- tool schema 风格差异：OpenAI `{"type":"function","function":{"name","parameters"}}` → Anthropic `{"name","description","input_schema"}`

#### Responses → input + instructions

```json
{
  "instructions": "...",
  "input": [
    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "..."}]},
    {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "..."}]},
    {"type": "function_call", "call_id": "...", "name": "...", "arguments": "..."},
    {"type": "function_call_output", "call_id": "...", "output": "..."}
  ]
}
```

映射规则：
- `system` message → `instructions` 字段
- `tool_calls` → `function_call` items（`id` → `call_id`，`function.arguments` 保持 string）
- `tool` message → `function_call_output`（`tool_call_id` → `call_id`）
- `developer` role → `system` role
- content 格式差异：OpenAI `string` → `input_text`/`output_text`

#### Gemini → contents + systemInstruction

```json
{
  "systemInstruction": {"parts": [{"text": "..."}]},
  "contents": [
    {"role": "user", "parts": [{"text": "..."}, {"inlineData": {"mimeType": "...", "data": "..."}}]},
    {"role": "model", "parts": [{"text": "..."}, {"functionCall": {"name": "...", "args": {...}}]},
    {"role": "function", "parts": [{"functionResponse": {"name": "...", "response": {...}}}]}
  ]
}
```

映射规则：
- `system` message → `systemInstruction.parts`
- role 映射：`assistant` → `model`，`tool` → `function`
- `tool_calls` → `functionCall` parts（`arguments` string → `args` dict）
- `tool` message → `functionResponse` parts
- content 格式差异：OpenAI `string` → Gemini `parts[{"text": "..."}]`
- image 差异：OpenAI `image_url` → Gemini `fileData`/`inlineData`

---

### 二、参数字段映射

| 参数 | OpenAI | Anthropic | Responses | Gemini |
|------|--------|-----------|-----------|--------|
| `temperature` | `temperature` | `temperature` | `temperature` | `generationConfig.temperature` |
| `top_p` | `top_p` | `top_p` | `top_p` | `generationConfig.topP` |
| `top_k` | `top_k` | ❌ 丢弃 | ❌ 丢弃 | `generationConfig.topK` |
| `max_tokens` | `max_tokens` | `max_tokens` | ❌ → `max_output_tokens` | `generationConfig.maxOutputTokens` |
| `max_output_tokens` | ❌ → `max_tokens` | ❌ → `max_tokens` | `max_output_tokens` | `generationConfig.maxOutputTokens` |
| `stop` | `stop` | ❌ → `stop_sequences` | `stop` | `generationConfig.stopSequences` |
| `stop_sequences` | ❌ → `stop` | `stop_sequences` | ❌ → `stop` | `generationConfig.stopSequences` |
| `tools` | `{type:"function",function:{name,parameters}}` | `{name,description,input_schema}` | `{type:"function",name,parameters}` | `tools[{functionDeclarations:[{name,description,parameters}]}]` |
| `tool_choice` | `auto/required/none/{type:"function",function:{name}}` | `auto/any/tool:{type:"tool",name:"..."}` | `auto/required/none` | ❌ 丢弃 (Gemini 不支持) |
| `metadata` | `metadata` | `metadata` | `metadata` | ❌ 丢弃 |
| `stream` | `stream` | `stream` | `stream` | URL 路径差异 `:streamGenerateContent` vs `:generateContent` |
| `reasoning_effort` | `reasoning_effort` | ❌ → `thinking` | ❌ → `reasoning` | ❌ 丢弃 |
| `thinking` | ❌ 丢弃 | `thinking` | ❌ → `reasoning` | ❌ 丢弃 |
| `reasoning` | ❌ 丢弃 | ❌ 丢弃 | `reasoning` | ❌ 丢弃 |
| `parallel_tool_calls` | `parallel_tool_calls` | ❌ 丢弃 | `parallel_tool_calls` | ❌ 丢弃 |
| `store` | `store` | ❌ 丢弃 | `store` | ❌ 丢弃 |
| `include` | `include` | ❌ 丢弃 | `include` | ❌ 丢弃 |
| `previous_response_id` | `previous_response_id` | ❌ 丢弃 | `previous_response_id` | ❌ 丢弃 |
| `truncation` | `truncation` | ❌ 丢弃 | `truncation` | ❌ 丢弃 |
| `system` | ❌ → messages[0] | `system` | ❌ → `instructions` | `systemInstruction` |
| `instructions` | ❌ → messages[0] | ❌ → `system` | `instructions` | ❌ → `systemInstruction` |
| `input` | ❌ → messages | ❌ → messages | `input` | ❌ → contents |
| `safety_settings` | ❌ 丢弃 | ❌ 丢弃 | ❌ 丢弃 | `safetySettings` |
| `service_tier` | `service_tier` | ❌ 丢弃 | ❌ 丢弃 | ❌ 丢弃 |
| `container` | `container` | `container` | ❌ 丢弃 | ❌ 丢弃 |
| `context_management` | `context_management` | `context_management` | ❌ 丢弃 | ❌ 丢弃 |
| `mcp_servers` | `mcp_servers` | `mcp_servers` | ❌ 丢弃 | ❌ 丢弃 |
| `stream_options` | `stream_options` | ❌ 丢弃 | ❌ 丢弃 | ❌ 丢弃 |
| `user` | `user` | ❌ 丢弃 | `user` | ❌ 丢弃 |
| `prompt_cache_key` | `prompt_cache_key` | ❌ 丢弃 | `prompt_cache_key` | ❌ 丢弃 |
| `client_metadata` | `client_metadata` | ❌ 丢弃 | `client_metadata` | ❌ 丢弃 |

---

### 三、Thinking/Reasoning 跨协议转换映射

| 源参数 | → OpenAI | → Anthropic | → Responses | → Gemini |
|--------|----------|-------------|--------------|----------|
| `reasoning_effort=high` | 保留 | `thinking={type:"enabled",budget_tokens:1024}` | `reasoning={effort:"high",max_tokens:1024}` | 丢弃 |
| `reasoning_effort=medium` | 保留 | `thinking={type:"enabled",budget_tokens:1024}` | `reasoning={effort:"medium",max_tokens:1024}` | 丢弃 |
| `reasoning_effort=low` | 保留 | `thinking={type:"enabled",budget_tokens:1024}` | `reasoning={effort:"low",max_tokens:1024}` | 丢弃 |
| `reasoning_effort=minimal` | 保留 | `thinking={type:"disabled"}` | `reasoning={effort:"minimal"}` | 丢弃 |
| `thinking={type:"enabled",budget_tokens:N}` | `reasoning_effort=high` | 保留 | `reasoning={effort:"high",max_tokens:N}` | 丢弃 |
| `thinking={type:"disabled"}` | `reasoning_effort=minimal` | 保留 | `reasoning={effort:"minimal"}` | 丢弃 |
| `reasoning={effort:"high",max_tokens:N}` | `reasoning_effort=high` | `thinking={type:"enabled",budget_tokens:N}` | 保留 | 丢弃 |
| `reasoning={effort:"medium"}` | `reasoning_effort=medium` | `thinking={type:"enabled",budget_tokens:1024}` | 保留 | 丢弃 |
| `reasoning={effort:"minimal"}` | `reasoning_effort=minimal` | `thinking={type:"disabled"}` | 保留 | 丢弃 |

---

### 四、Headers 差异

| Header | OpenAI | Anthropic | Responses | Gemini |
|--------|--------|-----------|-----------|--------|
| `Authorization` | `Bearer <key>` | ❌ | `Bearer <key>` | `Bearer <key>` (bearer 模式) |
| `x-api-key` | ❌ | `<key>` | ❌ | ❌ |
| `x-goog-api-key` | ❌ | ❌ | ❌ | `<key>` (默认模式) |
| `anthropic-version` | ❌ | `2023-06-01` | ❌ | ❌ |
| `anthropic-beta` | ❌ | 动态拼接(根据 payload) | ❌ | ❌ |
| `Content-Type` | `application/json` | `application/json` | `application/json` | `application/json` |

---

### 五、协议默认参数（PROTOCOL_DEFAULTS + CLIENT_BODY_DEFAULTS）

已有，位于 `custom.py:153-200`：

| 协议 | 默认客户端 | body 默认值 |
|------|-----------|-------------|
| responses | codex-cli | `text:{verbosity:"low"}`, `store:false`, `include:["reasoning.encrypted_content"]`, `reasoning:{effort:"medium"}` |
| anthropic | claude-code | `metadata:{user_id:...}`, `thinking:{type:"disabled"}`, `max_tokens:64` |
| openai | opencode | `max_tokens:32000`, `stream_options:{include_usage:true}` |
| gemini | gemini-cli | 无 |

> 注意：这些默认值使用的是**各协议原生的标准参数**（如 Responses 的 `reasoning`、Anthropic 的 `thinking`），已经是正确的。

---

## 渠道支持协议列表

### 新增配置字段

每个渠道（provider）配置新增 `supported_protocols` 字段：

```json
{
  "protocol": "openai",
  "supported_protocols": ["openai", "anthropic", "responses"]
}
```

### 逻辑规则

1. **直通判断**：客户端请求协议 ∈ `supported_protocols` → 直通
2. **直通时**：
   - payload 原样转发（不做字段映射，但补协议默认参数 + headers）
   - 应用 `PROTOCOL_DEFAULTS` + `CLIENT_BODY_DEFAULTS`
   - 应用 `client_preset` headers + system message
3. **跨协议时**：客户端请求协议 ∉ `supported_protocols` → 按映射表转换
   - 先选渠道的主协议（`protocol` 字段）
   - 做字段映射（消息结构 + 参数 + thinking）
   - 再补默认参数 + headers
4. **向后兼容**：`supported_protocols` 不配置时，默认 `=[protocol]`（即只有主协议直通，其他做转换）

### 实现位置

- 渠道配置：`admin.py` 的 provider CRUD + 前端渠道编辑页
- `CustomProvider.__init__`：读取 `supported_protocols`
- `_build_protocol_payload`：根据直通/跨协议选择路径

---

## 详细改动方案

### 一、渠道支持协议列表

#### 1.1 渠道配置新增

`providers/custom.py` `__init__`（236 行附近）新增：

```python
self.supported_protocols = [p.lower() for p in (kwargs.get("supported_protocols") or [self.protocol]) if isinstance(p, str)]
```

#### 1.2 直通判断逻辑

`_build_protocol_payload`（1366 行）改为：

```python
def _build_protocol_payload(self, endpoint: str, model_id: str, messages: list[dict], stream: bool, **kwargs) -> dict:
    endpoint = (endpoint or self.protocol or "openai").lower()
    
    # 直通判断：客户端协议 == 渠道支持的主协议 → 直接构建目标协议 payload
    # 跨协议：需要转换消息结构 + 参数字段
    
    if endpoint in ("openai", "chat"):
        return self._build_openai_payload(model_id, messages, stream, **kwargs)
    if endpoint == "anthropic":
        # 先构建 OpenAI payload，再转换为 Anthropic
        payload = self._build_openai_payload(model_id, messages, stream, **{**kwargs, "_skip_client_preset_body": True})
        data = self._openai_request_to_anthropic({**payload, "_skip_client_preset_body": True})
        return self._apply_client_preset_body(data, "anthropic", kwargs.get("client_type"))
    if endpoint == "responses":
        return self._build_responses_payload(model_id, messages, stream, **kwargs)
    if endpoint == "gemini":
        return self._build_gemini_payload(model_id, messages, **kwargs)
    raise ValueError(f"unsupported protocol endpoint: {endpoint}")
```

#### 1.3 直通路径

当客户端协议 ∈ `supported_protocols` 时，请求体已经符合目标协议格式，直接走：

```
请求 body → 补 PROTOCOL_DEFAULTS → 补 CLIENT_BODY_DEFAULTS → 补 client_preset headers → 发送
```

不做字段映射，不调 `_openai_request_to_anthropic`，不调 `openai_messages_to_responses_payload`，不调 `openai_messages_to_gemini`。

---

### 二、main.py — 协议级 kwargs 构建

#### 2.1 删除现有函数

- **删除**：`_build_openai_kwargs`（1621-1721 行）
- **删除**：`_apply_thinking_mode`（1096-1115 行）
- **删除**：`_apply_responses_thinking_mode`（1118-1126 行）

#### 2.2 新增：协议字段映射函数

```python
def _convert_thinking_for_protocol(
    source_params: dict,
    source_protocol: str,
    target_protocol: str,
) -> dict:
    """跨协议 thinking 参数转换"""
    result = {}
    
    # 提取源协议的 thinking 语义
    effort = None
    budget = 1024
    enabled = True
    
    if source_protocol == "openai":
        effort = source_params.get("reasoning_effort")
        if effort:
            enabled = effort != "minimal"
        elif source_params.get("reasoning") is not None:
            # 已经是 reasoning 格式，直接用
            source_protocol = "responses"
            effort = (source_params.get("reasoning") or {}).get("effort")
            budget = (source_params.get("reasoning") or {}).get("max_tokens", 1024)
            enabled = effort != "minimal" if effort else True
    elif source_protocol == "anthropic":
        thinking = source_params.get("thinking")
        if thinking:
            enabled = thinking.get("type") == "enabled"
            budget = thinking.get("budget_tokens", 1024)
            effort = "high" if enabled else "minimal"
    elif source_protocol == "responses":
        reasoning = source_params.get("reasoning")
        if reasoning:
            effort = reasoning.get("effort", "medium")
            budget = reasoning.get("max_tokens", 1024)
            enabled = effort != "minimal" if effort else True
    
    if effort is None and enabled is True:
        effort = "medium"
    elif effort is None:
        return result
    
    # 映射到目标协议
    if target_protocol == "openai":
        result["reasoning_effort"] = effort
    elif target_protocol == "anthropic":
        if not enabled or effort == "minimal":
            result["thinking"] = {"type": "disabled"}
        else:
            result["thinking"] = {"type": "enabled", "budget_tokens": budget}
    elif target_protocol == "responses":
        if not enabled or effort == "minimal":
            result["reasoning"] = {"effort": "minimal"}
        else:
            result["reasoning"] = {"effort": effort, "max_tokens": budget}
    # Gemini: 丢弃
    
    return result


def _inject_thinking_for_protocol(
    body: dict,
    mode: str,
    max_tokens: int,
    protocol: str,
) -> dict:
    """按协议注入 thinking/reasoning 标准参数"""
    body = dict(body)
    
    if protocol == "openai":
        if mode == "none":
            body["reasoning_effort"] = "minimal"
        elif mode == "auto":
            body["reasoning_effort"] = "medium"
        elif mode == "thinking":
            body["reasoning_effort"] = "high"
    
    elif protocol == "anthropic":
        if mode == "none":
            body["thinking"] = {"type": "disabled"}
        else:
            body["thinking"] = {"type": "enabled", "budget_tokens": max_tokens}
    
    elif protocol == "responses":
        if mode == "none":
            body["reasoning"] = {"effort": "minimal"}
        elif mode == "auto":
            body["reasoning"] = {"effort": "medium", "max_tokens": max_tokens}
        elif mode == "thinking":
            body["reasoning"] = {"effort": "high", "max_tokens": max_tokens}
    
    return body
```

#### 2.3 新增：协议级 kwargs 构建函数

```python
def _build_protocol_kwargs(body: dict, target_protocol: str) -> dict:
    """按目标协议构建 kwargs，只提取标准参数 + thinking 转换"""
    kwargs = {}
    
    # 通用参数
    common_params = (
        "temperature", "tools", "tool_choice", "top_p", "top_k",
        "metadata", "service_tier", "stream", "user",
    )
    for key in common_params:
        val = body.get(key)
        if val is not None:
            kwargs[key] = val
    
    # 协议特有参数
    if target_protocol == "openai":
        for key in (
            "max_tokens", "reasoning_effort", "reasoning",
            "parallel_tool_calls", "store", "include",
            "truncation", "previous_response_id", "stream_options",
            "container", "context_management", "mcp_servers",
            "prompt_cache_key", "client_metadata",
            "frequency_penalty", "presence_penalty", "repetition_penalty",
            "min_p", "top_a",
        ):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("stop") is not None:
            kwargs["stop"] = body["stop"]
        if body.get("stream") is True and body.get("stream_options") is None:
            kwargs["stream_options"] = {"include_usage": True}
    
    elif target_protocol == "anthropic":
        for key in ("thinking", "stop_sequences", "max_tokens", "system"):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("stop") is not None and body.get("stop_sequences") is None:
            stop = body["stop"]
            kwargs["stop_sequences"] = stop if isinstance(stop, list) else [stop]
        # messages 中的 system → kwargs["system"]
        if body.get("messages"):
            system_parts = []
            for msg in body["messages"]:
                if isinstance(msg, dict) and msg.get("role") == "system":
                    content = msg.get("content", "")
                    if isinstance(content, str) and content:
                        system_parts.append(content)
            if system_parts and not kwargs.get("system"):
                kwargs["system"] = "\n\n".join(system_parts)
    
    elif target_protocol == "responses":
        for key in (
            "reasoning", "input", "instructions", "previous_response_id",
            "max_output_tokens", "parallel_tool_calls", "store", "include",
            "truncation", "prompt_cache_key", "client_metadata",
        ):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("max_tokens") is not None and body.get("max_output_tokens") is None:
            kwargs["max_output_tokens"] = body["max_tokens"]
        # messages 中的 system → instructions
        if body.get("messages"):
            instruction_parts = []
            for msg in body["messages"]:
                if isinstance(msg, dict) and msg.get("role") in ("system", "developer"):
                    content = msg.get("content", "")
                    if isinstance(content, str) and content:
                        instruction_parts.append(content)
            if instruction_parts and not kwargs.get("instructions"):
                kwargs["instructions"] = "\n".join(instruction_parts)
    
    elif target_protocol == "gemini":
        for key in (
            "max_output_tokens", "stop_sequences", "safety_settings",
        ):
            val = body.get(key)
            if val is not None:
                kwargs[key] = val
        if body.get("max_tokens") is not None and body.get("max_output_tokens") is None:
            kwargs["max_output_tokens"] = body["max_tokens"]
        if body.get("stop") is not None and body.get("stop_sequences") is None:
            stop = body["stop"]
            kwargs["stop_sequences"] = stop if isinstance(stop, list) else [stop]
    
    # thinking 参数转换
    thinking_converted = _convert_thinking_for_protocol(body, "openai", target_protocol)
    kwargs.update(thinking_converted)
    
    return kwargs
```

#### 2.4 调用点修改

- `chat_completions` (1788) → `_build_protocol_kwargs(body, "openai")`
- `anthropic_messages` (2879) → `_build_protocol_kwargs(body, "anthropic")`
- `responses_api` (2598) → `_build_protocol_kwargs(body, "responses")`

---

### 三、main.py — API key 级 thinking 注入

修改 `_apply_api_key_thinking`（1129 行）按协议注入标准参数：

```python
def _apply_api_key_thinking(body: dict, thinking: dict, client_type: str, protocol: str) -> dict:
    if not thinking or thinking.get("enabled") is not True:
        return body
    if not _thinking_config_supports_client(thinking, client_type):
        return body
    
    # 客户端已显式带该协议标准 thinking 参数 → 不覆盖
    if protocol == "openai" and body.get("reasoning_effort") is not None:
        return body
    if protocol == "anthropic" and body.get("thinking") is not None:
        return body
    if protocol == "responses" and body.get("reasoning") is not None:
        return body
    
    mode = _thinking_config_mode(thinking)
    if not mode:
        return body
    
    max_tokens = int(
        thinking.get("max_thinking_tokens")
        or thinking.get("thinking_max_tokens")
        or thinking.get("budget_tokens")
        or 1024
    )
    return _inject_thinking_for_protocol(body, mode, max_tokens, protocol)
```

修改 `_prepare_client_request_body`（1141 行）传入 protocol 参数。

---

### 四、providers/custom.py — payload 纯协议映射

#### 4.1 删除

- `THINKING_PARAM_KEYS`（40-45 行）
- `REASONING_PARAM_KEYS`（47 行）
- `apply_thinking_system_params`（50-81 行）
- `_resolve_system_params`（84-91 行）
- `classify_thinking_system` import（22 行）+ 调用（1385-1406 行）
- 跨体系 `pop` 逻辑（1401-1406 行）
- codex-cli 分支非标准参数填充（1408-1419 行）
- `_openai_request_to_anthropic` 中 `thinking_enabled`/`thinking_budget` 处理（1489-1491 行）

#### 4.2 修改 `OPENAI_PAYLOAD_KEYS`

```python
OPENAI_PAYLOAD_KEYS = (
    "temperature", "max_tokens", "tools", "tool_choice", "top_p", "top_k", "stop",
    "metadata", "service_tier", "container", "context_management", "mcp_servers",
    "frequency_penalty", "presence_penalty", "repetition_penalty", "min_p", "top_a",
    "reasoning_effort", "parallel_tool_calls", "store", "include",
    "truncation", "previous_response_id", "reasoning", "user", "stream_options",
    "prompt_cache_key", "client_metadata",
)
```

#### 4.3 修改 `_build_openai_payload`

移除体系分类逻辑，直接按 payload_keys 选择 + `_copy_present`：

```python
def _build_openai_payload(self, model_id, messages, stream, **kwargs):
    if kwargs.get("_from_anthropic"):
        payload_keys = self.OPENAI_FROM_ANTHROPIC_PAYLOAD_KEYS
    elif kwargs.get("_from_responses"):
        payload_keys = self.OPENAI_FROM_RESPONSES_PAYLOAD_KEYS
    else:
        payload_keys = self.OPENAI_PAYLOAD_KEYS
    
    payload = {"model": self._upstream_model_id(model_id), "messages": messages, "stream": stream}
    self._copy_present(kwargs, payload, payload_keys)
    
    if kwargs.get("_skip_client_preset_body"):
        return payload
    if kwargs.get("_from_anthropic") or kwargs.get("_from_responses"):
        return payload
    return self._apply_client_preset_body(payload, "chat", kwargs.get("client_type"))
```

#### 4.4 修改 `_openai_request_to_anthropic`

删除 `thinking_enabled`/`thinking_budget` 处理（1489-1491 行），保留 `thinking` 字段透传（1481-1482 行）。

#### 4.5 修改 Gemini payload 构建

`build_gemini_generation_config`（`gemini_proto.py:153`）中删除 `thinking_enabled`/`thinking_budget` 处理（171-178 行）。

---

### 五、providers/custom.py — 模拟客户端重构

- `_apply_client_preset_body` 只做 headers + system message 注入
- 删除 `apply_defaults` 参数以及调用 `_apply_protocol_and_client_defaults` 的逻辑
- 协议级 body defaults 由 `PROTOCOL_DEFAULTS` + `CLIENT_BODY_DEFAULTS` 单独处理

---

### 六、main.py — 路由 thinking_mode 协议分发

修改 `main.py:2175-2187`，路由的 `thinking_mode` 按渠道协议注入对应标准参数：

```python
route_thinking_mode = (route_info.get("thinking_mode") or "").strip()
if route_thinking_mode:
    max_thinking_tokens = int(
        route_info.get("max_thinking_tokens")
        or route_info.get("thinking_max_tokens")
        or 0
    ) or int(attempt_kwargs.get("thinking_budget") or 1024)
    
    protocol = route_info.get("protocol") or "openai"
    attempt_kwargs = _inject_thinking_for_protocol(
        attempt_kwargs, route_thinking_mode, max_thinking_tokens, protocol
    )
    
    raw_responses = attempt_kwargs.get("_raw_responses_body")
    raw_anthropic = attempt_kwargs.get("_raw_anthropic_body")
    if isinstance(raw_responses, dict):
        attempt_kwargs["_raw_responses_body"] = _inject_thinking_for_protocol(
            raw_responses, route_thinking_mode, max_thinking_tokens, "responses"
        )
    if isinstance(raw_anthropic, dict):
        attempt_kwargs["_raw_anthropic_body"] = _inject_thinking_for_protocol(
            raw_anthropic, route_thinking_mode, max_thinking_tokens, "anthropic"
        )
```

---

### 七、config.py — 移除 minimal_model_params

- 删除 `MINIMAL_MODEL_PARAMS_STRIP_FIELDS`（10 行）
- 删除 `get_provider_minimal_model_params`（424-427 行）
- 删除 `main.py:2188-2198` 的调用逻辑

---

### 八、message_utils.py — 保留旧函数

- `map_reasoning_effort_to_qwen`（775 行）保留
- `normalize_legacy_thinking_mode`（800 行）保留
- `derive_auto_thinking_from_mode`（832 行）保留

---

## 文件改动清单

| 文件 | 改动行数估算 | 核心变更 |
|------|------------|----------|
| `main.py` | ~250 行 | 新增 `_convert_thinking_for_protocol` + `_build_protocol_kwargs` + `_inject_thinking_for_protocol`；修改 `_apply_api_key_thinking` + `_prepare_client_request_body`；删除旧函数；修改路由注入；删除 minimal_model_params 调用 |
| `providers/custom.py` | ~150 行 | 删除体系分类逻辑；清理 OPENAI_PAYLOAD_KEYS；新增 `supported_protocols`；修改 `_build_protocol_payload` 直通逻辑；修改 `_build_openai_payload`/`_openai_request_to_anthropic`；简化 `_apply_client_preset_body` |
| `providers/gemini_proto.py` | ~10 行 | 删除 `build_gemini_generation_config` 中 `thinking_enabled`/`thinking_budget` 处理 |
| `config.py` | ~10 行 | 删除 MINIMAL_MODEL_PARAMS_STRIP_FIELDS、get_provider_minimal_model_params |
| `admin.py` | ~20 行 | provider CRUD 支持 `supported_protocols` 字段读写 |
| `user-frontend/` | ~50 行 | 渠道编辑页新增 `supported_protocols` 多选 |
| `message_utils.py` | 0 行 | 保留旧函数 |
| `tests/` | 多 | 更新测试用例 |

---

## 影响的测试文件

| 测试文件 | 预期变更 |
|----------|----------|
| `tests/test_minimal_model_params.py` | **删除**（功能移除） |
| `tests/test_protocol_conversion.py` | 修正 payload 期望值：不含非标准参数 |
| `tests/test_protocol_conversion_cleanup.py` | 同上 |
| `tests/test_reasoning_effort_integration.py` | 断言 payload 含 `reasoning_effort` 而非 `thinking_mode` |
| `tests/test_thinking_system.py` | 重写或删除（体系分类逻辑移除） |
| `tests/test_retry_policy.py` | 检查路由 thinking_mode 注入逻辑 |
| `tests/test_agent_main.py` | 可能涉及 body 构建 |
| `tests/test_gemini_proto.py` | 验证 Gemini payload 不含 `thinking_enabled`/`thinking_budget` |

---

## 验收标准（端到端）

| 场景 | 预期上游 payload |
|------|-----------------|
| `POST /v1/chat/completions`（仅 model/messages/stream）→ 渠道协议 openai | **无** `thinking_mode`/`thinking_enabled`/`auto_thinking`/`thinking_format`/`auto_search`/`research_mode`/`thinking_budget` |
| `POST /v1/chat/completions`（带 `reasoning_effort=high`）→ 渠道协议 openai | 含 `reasoning_effort=high`，无其他 thinking 参数 |
| `POST /v1/messages`（Anthropic，带 `thinking`）→ 渠道协议 anthropic | 原样透传 `thinking` |
| `POST /v1/responses`（带 `reasoning`）→ 渠道协议 responses | 原样透传 `reasoning` |
| 路由 `thinking_mode=Auto` + 渠道协议 openai | payload 含 `reasoning_effort=medium` |
| 路由 `thinking_mode=Auto` + 渠道协议 anthropic | payload 含 `thinking={"type":"enabled","budget_tokens":1024}` |
| 路由 `thinking_mode=Auto` + 渠道协议 responses | payload 含 `reasoning={"effort":"medium","max_tokens":1024}` |
| OpenAI 入口 + 渠道协议 anthropic（跨协议）+ `reasoning_effort=high` | payload 含 `thinking={"type":"enabled","budget_tokens":1024}`，messages 转为 Anthropic 格式 |
| Anthropic 入口 + 渠道协议 openai（跨协议）+ `thinking={type:"enabled"}` | payload 含 `reasoning_effort=high`，messages 转为 OpenAI 格式 |
| Responses 入口 + 渠道协议 openai（跨协议）+ `reasoning={effort:"high"}` | payload 含 `reasoning_effort=high`，messages 转为 OpenAI 格式 |
| Gemini 渠道 + 任意 thinking 参数 | 不含任何 thinking 参数，`generationConfig` 不含 `thinkingConfig` |
| 直通模式：`supported_protocols=["anthropic"]`，Anthropic 入口 | body 原样转发 + 补 PROTOCOL_DEFAULTS + CLIENT_BODY_DEFAULTS + headers |
| 任意标准 OpenAI 兼容 API 上游 | 无未知参数报错 |

---

## 实施顺序

1. **先改 `main.py`**：`_convert_thinking_for_protocol` + `_build_protocol_kwargs` + `_inject_thinking_for_protocol` + 修改 `_apply_api_key_thinking` + 修改路由注入 + 删除旧函数
2. **改 `providers/custom.py`**：删除体系分类、清理 payload keys、新增 `supported_protocols`、修改 `_build_*_payload`/`_openai_request_to_anthropic`、简化 `_apply_client_preset_body`
3. **改 `providers/gemini_proto.py`**：删除 thinking 处理
4. **改 `config.py`**：删除 minimal_model_params
5. **改 `admin.py` + 前端**：`supported_protocols` 字段
6. **跑测试修测试**：按影响表逐个修
7. **手工端到端验证**：mock 上游抓 payload

---

## 风险点与缓解

| 风险 | 缓解 |
|------|------|
| 现有配置里存有 `thinking_mode`/`auto_search` 等字段 | 读取时忽略；payload 不发送；前端可后续清理 |
| 某些上游真需要特殊参数（如企业内部网关） | 应用独立 provider，不应走通用自定义渠道 |
| 测试用例大量依赖旧行为 | 先跑测试看红，按新行为逐个修正预期 |
| `CLIENT_BODY_DEFAULTS` 里 codex-cli 的 `reasoning.effort` 被保留 | 这是 Responses 协议原生参数，保留正确 |
| 跨协议转换时 thinking 参数丢失 | `_convert_thinking_for_protocol` 函数确保映射 |
| 跨协议转换时 messages 结构差异 | 使用已有的 `openai_messages_to_anthropic_messages` / `openai_messages_to_responses_payload` / `openai_messages_to_gemini` |
| 工具定义格式差异 | 使用已有的 `_openai_tool_to_anthropic` / `normalize_responses_tools` / `build_gemini_tools` |
| 直通模式下用户传了不属于该协议的字段 | 上游决定去留，代理不干预 |
| `supported_protocols` 配置向后兼容 | 不配置时默认 `=[protocol]`，行为与现有一致 |