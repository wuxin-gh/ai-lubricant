# 自定义代码渠道 · AI 生成需求说明

> **这是一份自包含的需求文档。** 你（AI 助手）将拿到一份脚本或仓库，任务是把它转换成 Ai Lubricant 平台「自定义代码渠道」的一个 spec 类。本文包含全部契约与约定，读完即可动手，不需要任何其他资料。
>
> 人类读者：这是文档站「自定义代码渠道」页（cap-code-channel.html）的配套文件。把第 0 节的句子连同你的脚本 / 仓库地址发给任何 AI 编码助手即可。

## 0. 给人类：怎么用这份文档

把这句话原样发给 AI（替换尖括号部分）：

```
请根据 https://ai-lubricant.vip100.de5.net/code-channel-ai.md 的要求，写一份自定义代码渠道（spec 类）。
对应的脚本 / 参考代码在：<仓库地址，或直接把代码粘贴在后面>
```

注意事项：

- 仓库地址可以是任何 AI 能访问的 URL（GitHub 仓库、单文件脚本、API 文档页都行）。
- AI 返回的是一段 Python 源码。**先人工审阅**，再贴入管理端「渠道配置 → 源码」。
- 贴入的代码以服务进程权限执行，等同部署一个 Python 文件，只允许可信管理员操作；它不是沙箱。
- 管理端源码 Tab 里另有内置「使用说明」弹框，与本文件口径一致。

## 1. 给 AI：你的任务

你要产出**一份完整的 Python 源码**，贴进平台管理端后即成为一个完整渠道。背景：

- 平台是模型网关：对外暴露 OpenAI 兼容 API，把请求路由给上游（真实模型服务）。
- 你写的类叫 **spec 类**：一个**普通类**（不继承任何框架基类），每个方法是一个**钩子**（hook）。
- 没写的钩子，平台自动回落到配置驱动的标准实现。标准协议（OpenAI / Anthropic / Responses / Gemini）、渠道地址、聊天路径、模型路径都在管理端表单里配置，代码读不到也不该写死。
- 账号池、重试、冻结、冷却、限流、出站代理、请求留痕、usage 估算、模型路由、流式↔非流式互转、工具调用解析，全部由框架负责，**你都不用写**。

核心思路：**只写你要改变的部分，能不写的钩子一个都不写。**

## 2. 硬性规则（违反任意一条，保存会被 HTTP 400 拒绝或不生效）

1. 写**恰好一个**普通类，类名任意（如 `class MyChannel:`）。**不要继承** `BaseProvider` / `CustomProvider` 或任何框架类；也不要定义第二个带公开方法的类（模块级辅助函数、常量可以有）。
2. 方法推荐 `@staticmethod`，第一个参数 `p` 是渠道实例。带网络 IO 的钩子写 `async def`；纯计算的（`headers`、`payload`、`build_url`、`account_schema`、`check_message`、`parse_chunk` 等）写 `def` 即可，框架两者都兼容。`stream_chat` 必须是 async generator。
3. **不要写 `PROVIDER_NAME`**——加载器会强制覆盖为渠道 id。
4. **不要把上游域名写死在代码里。** 渠道地址由管理员在渠道配置表单里填，spec 通过 `p.base_url` 读取。完全不打外网的 spec（本地 mock / Echo）才写 `REQUIRES_BASE_URL = False` 豁免。
5. **不要 `import aiohttp` 自己发请求。** 用 `p._make_session()` / `p.send_sse_request(...)`，出站代理、连接复用、超时、请求留痕才会生效。
6. **usage 不用你算。** 上游没回 token 统计时框架按实际内容自动估算；只有拿到上游真实 usage 才覆盖（流式 `yield {"usage": {...}}`，非流式放 `response["usage"]`）。不要写 `estimate_usage` / `normalize_usage` 之类的兜底。
7. OAuth / 多字段账号在 `account_schema()["fields"]` 里声明表单字段；表单外的运行时派生字段写类级 `ACCOUNT_FIELDS`。两者自动挂成 `p.access_token` 这类实例属性，缺值也挂空串。
8. 错误处理复用框架：`p._raise_upstream_error(error)`；不要自己复制错误归一、冻结、重试逻辑。
9. 钩子名必须与第 5 节的名称完全一致。拼错不会被静默忽略——保存时校验会把所有未识别的公开方法名列进报错文案。
10. 安全：spec 不要索取超出接入上游所需的权限；它运行在服务进程里，能 import 任何已安装的包，这是设计如此（可信管理员专用），不要在生成代码里放无关的破坏性操作。

## 3. 从脚本到 spec 的转换流程

**Step 1 — 读脚本 / 文档，回答四个问题：**

1. 登录方式：API key？账密换 ticket / Cookie？OAuth 设备码？OAuth 回调？登录后动态下发接入地址？
2. 请求形态：聊天 URL 长什么样、请求头有什么特殊的（签名、租户、UA）、请求体与标准协议差在哪。
3. 响应形态：流式的话 SSE 帧里 content / thinking 在哪个字段；非流式的响应体结构；有没有 usage、有没有配额响应头。
4. token 生命周期：会不会过期、有没有 refresh_token、过期后怎么换。

**Step 2 — 对照第 4 节情况判定表，选出最小钩子集。**

**Step 3 — 按下表把脚本里的东西映射到 spec：**

| 脚本里的东西 | 在 spec 里的归宿 |
|---|---|
| 硬编码的 `BASE_URL = "https://..."` | 删掉；管理端填渠道地址，spec 读 `p.base_url` |
| `requests.post` / `aiohttp` 聊天请求 | 标准形态直接删（回落配置驱动）；必须自写时用 `p._make_session()` / `p.send_sse_request()` |
| 登录函数（账密换 token / ticket） | `init_auth`；token 存 `JdbcClient.redis`，key 用 `p.redis_prefix` 前缀隔离 |
| token 刷新函数（提前换新 token） | `refresh_auth` + 类上写 `SCHEDULED_REFRESH = True` |
| 请求中 401 自愈刷新 | 钩子内刷新后 `await p.persist_account_fields({...})` 落库（否则重启丢） |
| 签名算法 / 租户头 / 特殊 User-Agent | `headers` 钩子 |
| 请求体附加字段 / 字段改名 | `payload` 钩子（从 `kwargs["_base_payload"]` 复制起改） |
| 非标准聊天 URL | `build_url` 钩子 |
| 登录后动态下发的接入地址 | `base_url` 钩子（地址存账号字段，登录后写入） |
| `for line in response:` SSE 解析循环 | `parse_chunk`（只解一帧）；协议怪到没法逐帧解时才整段 `stream_chat` |
| 拉模型列表的 GET 请求 | `fetch_models`；上游就是标准 `/v1/models` 则不写 |
| 轮询授权状态的 while 循环 | `poll_device_flow`（单步语义；调度、落库由框架负责，不要自己 while True） |
| 清理会话的函数 | `clear_conversations` |
| try/except + 状态码错误处理 | `p._raise_upstream_error(error)`；解析不了的 SSE 帧返回 `None` 回落标准解析 |
| 多轮 messages 拼成一段文本 | 类上 `SUPPORTS_MULTI_MESSAGES = True`，钩子里用 `p.prepare_provider_messages` |
| 正则切工具调用文本 | 删掉；框架已内置文本→工具调用解析（见第 9 节） |

**Step 4 — 写类。** 签名与返回契约见第 5 节，类级开关见第 6 节，`p` 上的能力见第 7 节。

**Step 5 — 过一遍第 11 节自查清单，按第 10 节格式交付。**

## 4. 情况判定表

| 情况 | 上游形态 | 你写什么 |
|---|---|---|
| A | OpenAI / Anthropic / Responses / Gemini 兼容接口 | 零钩子——但源码至少要有一个钩子，贴最小 `init_auth` 即可 |
| B | 兼容接口 + 额外签名头 / 租户头 | `headers` |
| C | 兼容接口 + 请求体多个字段 | `payload` |
| D | 聊天 URL 非标准路径（带租户、带模型名等） | `build_url` |
| E | SSE 帧不是 `choices[].delta` | `parse_chunk` |
| F | 完全非标流（websocket、长轮询、私有协议） | `stream_chat` 全接管；不写 `non_stream_chat`，框架自动聚合 |
| G | 登录后动态下发接入地址 | `base_url`（覆盖全部出站路径） |
| H | OAuth 设备码授权 | `account_schema` + `begin_device_flow` + `poll_device_flow` |
| I | OAuth 回调授权 | `account_schema` + `build_auth_start` + `handle_auth_callback` |
| J | 上游只回调 `127.0.0.1:{port}`（桌面客户端型） | H + `handle_loopback_callback`（框架根路径提供 `/oauth/callback` 落点） |
| K | token 需要定时刷新 | `refresh_auth` + `SCHEDULED_REFRESH = True` |
| L | 请求途中 token 过期自愈并落库 | 钩子内刷新 + `await p.persist_account_fields({...})` |
| M | 账密登录换 ticket / Cookie，非标聊天 | `init_auth` + 需要的聊天 / 模型钩子 |
| N | 模型列表要登录后拉取 / 非标 | `fetch_models` |
| O | 图片 / 视频 / 语音生成非标 | `generate_image` / `generate_video` / `generate_speech`（不写则回落 OpenAI 标准格式） |
| P | 只想拒绝某些模型或消息 | `check_message` |
| Q | 上游不认 function calling（把工具调用当正文吐） | 类上 `TOOLS_AS_PROMPT = True`（细节见第 9 节，解析框架已内置） |
| R | 上游按自家 CLI 校验请求头 | 类上 `APPLY_CLIENT_PRESET = False`（配置里写 `client_preset="none"` 关不掉，只有这个开关能硬关） |
| S | 响应头里有配额 / 余额信息 | `update_quota` |
| T | 要清理上游会话 | `clear_conversations` |
| U | 本地 mock / 不打外网 | `REQUIRES_BASE_URL = False` |

多数真实上游是「A/B/C/D/E + H 或 M」的组合。**先判定登录方式，再判定请求 / 响应形态，逐个最小化。**

## 5. 钩子签名参考（全部 29 个）

### 认证生命周期

| 钩子 | 签名 | 返回 / 语义 |
|---|---|---|
| `init_auth` | `(p, is_check=False)` | bool：凭据就绪。首次使用、失效自愈、体检时调用；`is_check=True` 是轻量校验 |
| `check_auth` | `(p)` | bool：已有登录态是否有效 |
| `is_init` | `(p)` | bool：凭据是否就绪（轻量、不打网络） |
| `health_check` | `(p)` | bool：自定义健康检查 |
| `refresh_auth` | `(p, account, cfg)` | dict：要窄写回账号的字段（如 `{"access_token": ...}`）；admin 统一落库，spec 不用自己写 |

### 聊天与媒体

| 钩子 | 签名 | 返回 / 语义 |
|---|---|---|
| `stream_chat` | `(p, model_id, messages, **kwargs)` | async generator，`yield` 统一帧（见第 8 节）。推荐先 `yield {}` 表示上游已接通，再逐段 yield 内容 |
| `non_stream_chat` | `(p, model_id, messages, **kwargs)` | dict：OpenAI 响应体，用 `p.build_openai_response(...)` 构造 |
| `generate_image` | `(p, model_id, prompt, **kwargs)` | OpenAI images 响应体 `{created, data: [{b64_json 或 url}]}` |
| `generate_video` | `(p, model_id, prompt, **kwargs)` | OpenAI 视频响应体 |
| `generate_speech` | `(p, model_id, text, **kwargs)` | OpenAI TTS 响应体 |
| `fetch_models` | `(p)` | `[{"id": "模型id", "name": "显示名"}]`；`name` 可选 |

### 请求塑形

| 钩子 | 签名 | 返回 / 语义 |
|---|---|---|
| `headers` | `(p, base_headers, kwargs)` | dict：改完的请求头；返回 `None` 保留原头。`base_headers` 已含框架按协议生成的认证头 |
| `payload` | `(p, endpoint, model_id, messages, stream, kwargs)` | dict：完整请求体。从 `kwargs["_base_payload"]`（框架按协议生成好的标准 body）复制起改 |
| `build_url` | `(p, kwargs)` | str：聊天 URL；`kwargs.get("model_id")` 取模型。只改 URL 时不要同时重写 `stream_chat` |
| `base_url` | `(p)` | str 或 None：覆盖渠道地址，影响聊天 / 模型 / 图片 / 视频 / 语音全部出站路径。返回空串或 None 回落渠道配置。**不要在这里发网络请求**——它每次出站都会被调用 |
| `parse_chunk` | `(p, event_str)` | dict 或 None：一帧原始 SSE 文本 → 统一帧（`content`/`thinking`/`tool_calls`）。返回 `None` 回落标准解析。不要在这里做 IO |

### 响应、配额与拦截

| 钩子 | 签名 | 返回 / 语义 |
|---|---|---|
| `on_response_headers` | `(p, headers, model_id, ctx)` | 响应头回调 |
| `update_quota` | `(p, headers, model_id)` | 从响应头更新配额 |
| `check_message` | `(p, model_id, messages)` | bool：`False` 拒绝该候选（请求前拦截，不是重试逻辑） |
| `record_message` | `(p, model_id, messages)` | 自定义记账 |

### 账号授权

| 钩子 | 签名 | 返回 / 语义 |
|---|---|---|
| `account_schema` | `()`（无 `p`） | dict：声明前端账号字段与授权入口（见下） |
| `begin_device_flow` | `(p, auth_context=None)` | dict：授权启动信息（见下） |
| `poll_device_flow` | `(p, poll_params, auth_context=None)` | dict：`{"status": "pending"/"authorized"/"error", "account_data": {...}, "error": ...}` |
| `handle_loopback_callback` | `(p, params, poll_params, callback_url="")` | dict 或 None：`None` = 不是我的回调，框架试下一个会话 |
| `build_auth_start` | `(name, account, redirect_uri, state, cfg, auth_context=None)`（无 `p`） | dict：`{"auth_url": ...}` |
| `handle_auth_callback` | `(name, params, state_data, cfg)`（无 `p`） | dict：`{"ok": True, "account_data": {...}}` 或 `{"ok": False, "error": ...}` |

### 生命周期

| 钩子 | 签名 | 返回 / 语义 |
|---|---|---|
| `on_channel_attached` | `(p)` | 配置注入后初始化派生状态 |
| `clear_conversations` | `(p, max_age_hours)` | int：清理数量 |
| `close` | `(p)` | 释放自定义资源 |

**account_schema 形态：**

```python
{
    "display_name": "渠道显示名",
    "fields": [
        {"key": "access_token", "label": "Access Token",
         "type": "password", "secret": True, "section": "凭据"},
        {"key": "username", "label": "账号", "type": "text"},
    ],
    "auth_start": {
        "enabled": True,
        "mode": "device_code",          # 或 "oauth_callback"
        "completion": "poll",           # 可选：poll / callback / loopback
        "label": "网页登录授权",
    },
}
```

**begin_device_flow 返回形态：**

```python
{
    "task_type": "device_code",
    "auth_url": "https://...verification_uri",
    "verification_uri": "...",
    "user_code": "ABCD",            # 没有真验证码就留空串，别塞说明文字
    "expires_in": 900,
    "interval": 5,
    "poll_params": {"device_code": "..."},   # 下一轮轮询要用的参数
}
```

**poll_device_flow 授权成功：**

```python
{"status": "authorized",
 "account_data": {"access_token": "...", "refresh_token": "..."}}
```

> **`account_data` 的 key 必须与 `account_schema` 里声明的 field key 一致**，否则授权成功后字段不回填。

`auth_context`（`begin_device_flow` / `poll_device_flow` / `build_auth_start` 的可选尾参）回答「浏览器回调该回到哪」：含 `origin`（管理员浏览器地址）、`scheme`/`host`/`port`、`callback_path`、`callback_url`、`loopback_callback_path` 等。老签名不写这个参数也照常工作（框架按签名决定传不传）。授权落库由服务端中央扫描器在轮询到 `authorized` 时完成，spec 不要自己写 `while True`。

## 6. 类级开关（写在 spec 类上）

| 开关 | 默认 | 作用 |
|---|---|---|
| `SUPPORTS_TOKEN_AUTO_REFRESH` | False | 凭据过期后能否自愈；True 时前端标「已过期（等待自动刷新）」 |
| `SCHEDULED_REFRESH` | False | 纳入 admin 每日 10:00 主动刷新（需同时写 `refresh_auth`） |
| `SUPPORTS_MULTI_MESSAGES` | False | 是否把多轮 messages 整批发上游 |
| `NEEDS_STREAM_TOOL_FALLBACK` | False | 流式工具调用降级开关 |
| `TOOLS_AS_PROMPT` | False | 上游不认 OpenAI function calling 时把工具说明进 system、payload 不带 tools（见第 9 节） |
| `TOOL_PARSE_IN_CHANNEL` | False | 围栏解析（含「调用名是否在入参 tools 里」校验）由渠道自己做；框架流式 / 非流式出口不再二次解析 |
| `TOOLS_PROMPT_FORMAT` | 默认即可 | 文本工具形态：`xml`/`json`/`hermes` 为兼容别名，语义已收敛为同一围栏，一般不用设 |
| `REQUIRES_BASE_URL` | True | 渠道地址是否必填；不打外网的 spec 写 False 豁免 |
| `ACCOUNT_FIELDS` | () | 账号字段名清单，挂实例属性；与 `account_schema` 字段求并集（username/password 已由基类持有，别列） |
| `APPLY_CLIENT_PRESET` | True | 是否套 opencode / claude-code 那套伪装头；上游校验自家 CLI 头时写 False 硬关（配置层关不掉） |

## 7. `p` 上最常用的东西

| 属性 / 方法 | 含义 |
|---|---|
| `p.base_url` | **管理端渠道配置中用户填写的渠道地址**；不要在 spec 里写死 |
| `p.api_key` | 渠道 / 账号配置派生的 key |
| `p.username` / `p.password` | 当前账号凭据 |
| `p.proxy` / `p.proxy_config_id` | 统一出口代理配置 |
| `p.timeout_seconds` | 请求超时 |
| `p.redis_prefix` | `model-api:{渠道id}:{username}`，用于隔离账号运行态 |
| `p._channel` | 渠道领域对象，可读 `.base_url`、`.protocol`、`.chat_protocols`、`.billing_mode` |
| `p._make_session()` | 创建遵循渠道代理设置的 HTTP session（不要自己 `import aiohttp`） |
| `p.send_sse_request(method, url, headers, on_headers=None, **kwargs)` | 发 SSE 请求并接入请求留痕；non-200 会抛 `HTTPException`，逐块 yield 原始文本 |
| `p.build_openai_response(id, model, content, thinking="", tool_calls=None)` | 构造统一 OpenAI 响应；`id` 传 `generate_completion_id()` |
| `generate_completion_id()` | 生成 `chatcmpl-xxx` 响应 id（已注入，无需 import） |
| `p._raise_upstream_error(error)` | 复用框架错误归一，不要自己复制一份 |
| `p.build_tools_prompt(tools)` | 生成工具说明 prompt（注入 system 用）；形态默认即可 |
| `p.prepare_provider_messages(messages, tools_prompt="")` | 把 tools_prompt 合并进首条 system 拼到 messages 前（已有 system 就合并，不前置第二条） |
| `p.parse_tool_calls(content)` | 文本→工具调用解析，返回 `(去掉工具块后的正文, OpenAI tool_calls 列表)`；流式 / 非流式同源 |
| `p.parse_sse_type(event_str)` | 解析一帧 SSE，返回 `(event_type, event_data)`；非标 SSE 渠道常用 |
| `await p.persist_account_fields({...})` | 运行时刷新出的字段窄写回本账号并同步池内实例 |
| `p.tools_for_payload(tools)` | `TOOLS_AS_PROMPT=True` 时返回 None（payload 不发 tools） |

### 免 import 的注入名单（直接用，不必写 import）

框架核心：`BaseProvider`、`CustomProvider`、`make_insecure_connector`、`JdbcClient`、`HTTPException`、`Channel`、`ModelClientPool`、`AccountClient`、`ProviderLimitState`、`get_proxy_manager`

工具调用：`generate_completion_id`、`parse_tool_calls_from_content`、`generate_tools_prompt`、`render_tool_call_json`、`render_tool_result_json`、`render_tool_call_xml`、`render_tool_result_xml`

常用：`logger`、`asyncio`、`json`、`re`、`time`、`uuid`、`hashlib`、`base64`、`secrets`、`string`、`traceback`、`datetime`、`timezone`、`AsyncGenerator`、`urlparse`、`parse_qs`

> 也能 `import` 任何已安装的包（如 `from Crypto.Cipher import AES`），但 HTTP 请求别用裸 `aiohttp`，走 `p._make_session()` / `p.send_sse_request()`。

## 8. 流式统一帧 / 非流式返回

`stream_chat` 每次 `yield` 一个字典：

```python
yield {}  # 可选但推荐：先通知主链路上游已接通
yield {"content": "正文增量", "thinking": "", "tool_calls": []}
yield {"content": "", "thinking": "思考增量", "tool_calls": []}
yield {"content": "", "thinking": "", "tool_calls": [{
    "index": 0,
    "id": "call_1",
    "type": "function",
    "function": {"name": "search", "arguments": {"q": "天气"}},
}]}
# 也可带 usage / finish_reason / done
```

`messages` 是 OpenAI 格式；`kwargs` 中常见的有 `tools`、`thinking_enabled`、`auto_search`、`max_tokens` 和请求留痕回调。

**只写一个聊天钩子的自动互补：**

- 只写 `stream_chat`：框架自动把流式帧聚合成非流式响应；
- 只写 `non_stream_chat`：框架把结果拍成单帧 `yield`（流式请求走非流式上游）；
- 两个都不写：回落配置驱动实现（标准协议自动发送、自动解析）。

## 9. 工具调用（function calling）

三种情况：

1. **上游认 OpenAI `tools` 字段** → 什么都不用做，`tools` 原样透传上游，上游做原生 function calling。
2. **上游不认 function calling**（把工具调用当正文吐） → 类上写 `TOOLS_AS_PROMPT = True`：
   - payload **绝不能**带 `tools`：用 `p.tools_for_payload(kwargs.get("tools"))` 取值（返回 None）；
   - `p.build_tools_prompt(tools)` 生成工具说明，`p.prepare_provider_messages(messages, tools_prompt)` 合并进首条 system；
   - 历史轮的 `tool_calls` / `tool` 结果框架自动文本化（`flatten_tool_history`），渠道无需处理；
   - 解析框架已内置，流式 / 非流式同源；模型吐的脏 JSON（缺引号、尾随逗号、被流截断）由 `json_repair` 容错还原。
3. **渠道自己做围栏解析**（要校验「调用名是否在入参 tools 里」） → 类上写 `TOOL_PARSE_IN_CHANNEL = True`：框架流式 / 非流式出口不再对 content 二次解析围栏，渠道判「方法不存在」时原样透传围栏文本。

`TOOLS_PROMPT_FORMAT`（xml / json / hermes）为兼容别名，语义已收敛为同一围栏，一般不用设。上游认 OpenAI `tools` 时**不要**走 prompt 注入——直接把 `tools` 原样塞请求体。


## 10. 输出契约（AI 的交付格式）

- 输出**一个 Python 代码块**，内含一个 spec 类 + 可选的模块级辅助函数 / 常量。不要再有第二个带公开方法的类。
- 代码块之外，附一段简短说明（**必须**包含）：
  1. 用了哪些钩子、为什么（一两句）；
  2. 管理端渠道配置要填什么——**协议行**（openai / anthropic / responses / gemini）、**聊天路径**、**模型路径**、**渠道地址**；这些是表单值，spec 代码读不到，漏填等于坏渠道；
  3. 账号字段怎么填（如果写了 `account_schema`，列出字段；如果走授权，说明授权方式）；
  4. 你在脚本里看到的、无法确定的信息（假设清单），让人类确认。
- 不要输出与代码无关的客套或重复解释。

## 11. 自查清单（输出前逐条核对）

- [ ] 恰好一个普通类，没继承任何基类（没有 `class X(BaseProvider)`、没有 `import` 框架基类来继承）。
- [ ] 没写 `PROVIDER_NAME`。
- [ ] 没有硬编码上游域名（注释里说明可以）；所有出站地址从 `p.base_url` 拼，或声明 `REQUIRES_BASE_URL = False`。
- [ ] 至少一个钩子（最冷的兼容接口也贴了最小 `init_auth`）。
- [ ] 钩子名与第 5 节完全一致（拼错 = 校验拒绝）。
- [ ] 网络请求全走 `p._make_session()` / `p.send_sse_request()`，没有裸 `import aiohttp` / `requests` 发主链路请求。
- [ ] `stream_chat` 是 `async def` 且至少 `yield` 了一个带内容的帧（推荐先 `yield {}`）。
- [ ] `account_schema` 的 field key 与授权流程 `account_data` 的 key 一致。
- [ ] 没有手写 usage 估算；仅在拿到上游真实 usage 时 `yield {"usage": {...}}` 或写进响应体。
- [ ] async 钩子都写了 `async def`；`stream_chat` 是 async generator。
- [ ] `base_url` 钩子里没有发网络请求（它每次出站都会被调）。
- [ ] `parse_chunk` 里没有发网络请求 / 写 Redis / 起后台任务。
- [ ] 设备码流没有自己写 `while True` 轮询。
- [ ] 常量 / 辅助函数命名不与钩子别名重名，也不以会被当成钩子的方式命名。
- [ ] Python 语法可过 `compile`（无 f-string 嵌套引号错误、无缩进错误）。

## 12. 常见错误（这些会被 400 拒绝或运行时翻车）

| 错误 | 后果 / 处理 |
|---|---|
| 渠道地址留空就保存 | 保存被拒（「渠道地址（base_url）不能为空」）；填上地址，或写 `REQUIRES_BASE_URL = False` |
| 继承 `BaseProvider` / `CustomProvider` | 加载直接报错并给迁移指引；删掉继承，写普通类 |
| 源码里定义了多个带方法的类 | 加载报错「只能定义一个 spec 类」；辅助逻辑用模块级函数 |
| 没有任何可识别钩子 | 加载报错，并列出未识别方法名（防拼错静默不生效）；至少写一个 |
| 写了 `stream_chat` 但没有 `yield` | 流式请求不产生内容；至少 `yield` 统一帧 |
| `account_schema` 字段名与 `account_data` 对不上 | 授权成功后字段不回填；两边 key 必须一致 |
| 把 `base_url` 写在代码里 | 管理端填的地址不生效；用 `p.base_url` |
| 自己 `import aiohttp` 发请求 | 绕开出站代理、连接复用和请求留痕；用 `p._make_session()` / `p.send_sse_request()` |
| 自己拼 usage | 多余；框架自动估算。要覆盖就 `yield {"usage": {...}}` |
| 复制 `_raise_error_json` / 自己做冻结重试 | 与框架状态机分叉；用 `p._raise_upstream_error` |
| 每个钩子都重写 | 容易漏掉协议、工具调用、日志和重试；优先只写一个最小钩子 |
| 在 `base_url` / `parse_chunk` 里做 IO | 把请求放大或阻塞主链路；这俩必须是纯函数 |
| `client_preset="none"` 想关伪装头 | 配置层关不掉（会兜到协议默认 preset）；必须类上写 `APPLY_CLIENT_PRESET = False` |

## 13. 完整最小样例（EchoChannel）

这是平台新建代码渠道时回填进编辑器的预设样例，可直接跑通「贴代码 → 出渠道」全链路：

```python
class EchoChannel:
    """最小样例：把用户最后一条消息原样回吐。完整说明点「使用说明」。"""

    # 本样例不打外网，所以豁免「渠道地址」必填校验。真实渠道删掉这行：
    # 渠道地址由用户在渠道配置里填，spec 用 p.base_url 读取，别写死域名。
    REQUIRES_BASE_URL = False

    @staticmethod
    async def init_auth(p, is_check=False):
        return True

    @staticmethod
    async def fetch_models(p):
        return [{"id": "echo", "name": "Echo"}]

    @staticmethod
    async def stream_chat(p, model_id, messages, **kwargs):
        msg = messages[-1].get("content", "") if messages else ""
        yield {}
        yield {"content": msg, "thinking": "", "tool_calls": []}

    # 不写 non_stream_chat：框架自动把流式帧聚合成非流式响应。
    # 不写 usage：框架按内容自动估算；要自定义就 yield {"usage": {...}}。
```

一个真实形态的骨架（兼容接口 + 自定义请求头 + 非标 SSE，组合场景 B + E）：

```python
import json


class MyCompatChannel:
    """兼容 OpenAI 上游 + 租户头 + 自定义 SSE 字段。"""

    @staticmethod
    async def init_auth(p, is_check=False):
        # p.api_key、p.base_url 都由渠道配置派生。
        return bool(p.api_key and p.base_url)

    @staticmethod
    def headers(p, base_headers, kwargs):
        # base_headers 已含框架按协议生成的认证头，只补自己的字段。
        base_headers["X-Tenant-Id"] = p.username
        return base_headers

    @staticmethod
    def parse_chunk(p, event_str):
        # 上游 SSE 把正文放在 answer 字段、思考放在 analysis 字段。
        if event_str.strip() == "[DONE]":
            return {"content": "", "thinking": "", "tool_calls": []}
        try:
            data = json.loads(event_str.removeprefix("data:").strip())
        except (TypeError, ValueError):
            return None  # 交给框架的标准解析器
        return {
            "content": data.get("answer", ""),
            "thinking": data.get("analysis", ""),
            "tool_calls": [],
        }
```

> 管理端渠道配置建议：协议 openai、聊天路径 `/v1/chat/completions`、模型路径 `/v1/models`、渠道地址填上游域名、账号里填 api_key 与 username（租户号）。

## 14. 交付后：管理员怎么把它装进平台

1. 管理端 → 渠道配置 → 新建渠道 → 类型选「自定义渠道（代码）」。
2. 「源码」Tab 贴入 AI 交付的代码 → 保存。语法错 / 无 spec 类 / 误继承基类 / 渠道地址缺失都会返回 HTTP 400，不落库。
3. 渠道配置里填：渠道地址（base_url）、协议行、聊天路径、模型路径——按 AI 交付说明里的「渠道配置建议」。
4. 按 `account_schema` 添加账号（填字段或发起授权）。
5. 保存后**热更新立即生效**，不需要重启进程。
6. 验证：模型列表能拉到 → 发一条流式消息 → 看日志（请求留痕记录了上游原始内容，便于排查卡死 / 半截断）。

---

*本文件与文档站「自定义代码渠道」页（cap-code-channel.html）配套；管理端源码 Tab 的「使用说明」弹框内容为其权威版本（docs/providers/code-channel.md）。三者口径一致。*
