# 完善建议

最后更新：2026-05-22

本文记录本次梳理代码后发现的后续改进项。它和系统架构文档分开维护，方便作为待办清单持续更新。

## 高优先级

1. 修复源码注释和 `README.md` 的编码问题。
   当前不少中文注释、docstring 和 README 内容显示为乱码。虽然多数情况下不影响运行，但会明显增加维护和评审成本。

2. 移除仓库中的敏感配置。
   `config.json`、`config_example.json`、`config/*.json` 中存在看起来像真实环境的主机、用户名、密码、Token 或 Cookie。建议迁移到环境变量、密钥管理系统或未纳入 Git 的本地配置，并轮换已暴露的凭据。

3. 为路由和重试补自动化测试。
   重点覆盖 `Config.resolve_model_route`、`ModelClientPool._get_available_client_with_provider`、API Key 作用域、模型组、RPM/TPM 限制、429 切号和失败重试。

4. 规范后台任务的创建、等待和取消。
   部分位置存在未等待或未保存的 `asyncio.gather(...)`，启动阶段也会把 `ModelClientPool.initialize()` 作为 detached task 创建。建议统一追踪任务、记录异常，并在关闭时等待取消完成。

5. 强化管理端 API 入参校验。
   多个 `/admin` 接口直接接收 `dict` 或裸查询参数。建议为 Provider 配置、账号配置、模型路由、API Key、保留策略等增加 Pydantic 模型。

## 中优先级

1. 拆分过大的模块。
   `main.py`、`admin.py`、`rate_limiter.py` 都承载了多类职责。建议逐步拆分公共 API、管理端路由组、路由策略、账号状态、定时任务和日志记录。

2. 显式声明渠道能力。
   目前渠道是否支持工具调用、图片生成、多模态、联网搜索、思考内容、Anthropic 协议、会话清理等能力主要靠代码隐式体现。建议增加统一的 capability metadata。

3. 优化请求日志保留策略。
   当前按小时清理 `response_body`，按天清理成功日志。建议增加管理端可见的清理预估、按接口定制保留策略，以及失败请求详情保护。

4. 改进 Token 统计。
   `/v1/token/count` 当前使用字符长度估算。建议按模型族接入更准确的 tokenizer，保留当前逻辑作为兜底。

5. 补 Provider 配置 Schema 文档。
   每个内置渠道都有自己的配置字段。建议为每个渠道补 Markdown 或 JSON Schema，方便管理端和自动化脚本生成配置。

6. 增加健康检查接口。
   建议增加 `/healthz` 和 `/readyz`，覆盖应用存活、Postgres、Redis、ProviderPool 初始化状态和定时任务状态。

7. 增强路由可观测性。
   管理端可展示当前模型到渠道映射、渠道评分、账号冷却、配额快照和定时任务状态。

## 低优先级

1. 整理实验脚本目录。
   `script/` 和 `wispbyte/` 中包含手工测试、实验脚本和独立遥测探索内容。建议把稳定示例移到 `examples/`，把非模型 API 主链路内容隔离说明。

2. 生成管理端 API 文档。
   FastAPI 已提供 OpenAPI，但管理端仍适合维护一份人工整理的接口说明，便于前端和自动化调用。

3. 增加格式化、Lint 和类型检查。
   建议明确项目统一命令，并写入工作提示词和 README。

4. 补配置迁移说明。
   代码支持从旧的 `app_config(provider:*)` 迁移到新表结构，应补充操作步骤、回滚方式和风险点。

5. 提供新渠道模板。
   新增 Provider 时容易遗漏鉴权、模型列表、流式输出、限流钩子、日志回调等契约。建议提供一个最小模板。


## Custom 渠道配额显示优化方案

### 现状与问题

1. **命名混淆**：部分配置字段 	pm_limit 实际语义为"每日请求次数上限"（daily request limit），与 Token Per Minute 完全无关，管理端显示"TPM"造成理解偏差。
2. **health_check() 偷跑额度**：custom.py::health_check() 为验证可用性向上游发送 max_tokens=1 的真实请求，按次计费渠道会因此消耗每日配额，但请求未走正常
ecord_message() 流程，配额计数与实际不一致。
3. **配额更新强依赖响应头**：CountQuota.update_from_headers() 仅在收到上游限流响应头时才刷新 Redis，若上游未返回或请求失败，配额数据长期陈旧。
4. **多实例不一致**：Redis 不可用时降级为内存，多实例部署下各实例独立计数，管理端看到的"每日剩余"在不同节点上完全不同。
5. **前端缺少来源标识**：管理端账号卡片显示 每日剩余 123/2000，但无法区分该数字来自上游响应头、本地计数还是默认值。

### 优化方案

#### 1. 配置层统一命名（已部分修复）

- 优先读取 daily_request_limit，同时保留 	pm_limit 兼容旧配置。
- per_model_limit 从配置读取，不再硬编码 500。
- 前端隐藏式字段：配置编辑时根据渠道类型动态展示 每日请求上限 / 单模型上限，不再使用 	pm_limit 标签。

#### 2. health_check() 改造（Custom）

- **方案 A**：health_check() 增加 dry_run 参数，默认不发送真实聊天请求，仅请求模型列表或专用 health endpoint。
- **方案 B**：若必须发真实请求，请求完成后显式调用 
ecord_message() 并同步配额，确保额度消耗被计数。
- 对按次计费的 Custom 渠道，建议优先使用 /v1/models 接口做可用性探测，避免消耗聊天配额。

#### 3. 配额数据增加来源与有效期

- CountQuota.snapshot() 返回字段增加 quota_source（headers / memory / default）和 quota_updated_at。
- 前端展示时：
  - headers：显示蓝色，表示数字来自上游最新响应。
  - memory / default：显示黄色，并提示"未收到上游配额头，数据可能不准确"。
  - 超过 10 分钟未更新：显示红色，提示"配额信息已过期"。

#### 4. 多实例配额一致性兜底

- Redis 降级为内存时，在 snapshot() 中增加 quota_instance_local: true 标识。
- 管理端账号卡片显示小图标 🖥️，hover 提示"本实例本地计数，多实例部署可能存在偏差"。
- 建议生产环境强制启用 Redis，在 config.py 中增加 quota_redis_required 配置项。

#### 5. 手动刷新配额按钮

- 在管理端 Custom 渠道账号卡片增加"刷新配额"按钮。
- 点击后调用新接口 POST /providers/{name}/accounts/{username}/refresh-quota，触发一次带响应头的请求并更新 CountQuota。
- 前端即时刷新显示，无需等待下一次真实请求。

---

## 模型组与模型专用线路需求梳理与优化方案

### 当前定位

| 概念 | 当前定义 | 客户端可见 | 路由作用 | 白名单控制 |
|------|---------|----------|----------|-----------|
| **模型组** (model_groups) | 一组真实模型的聚合，如 smart-group = [gpt-4, claude-3] | ✅ 作为模型 ID 出现在 /v1/models | 请求模型名为组名时，展开为组内成员，按默认路由选择 | ✅ pi_key_ids / use_all_api_keys |
| **模型专用线路** (model_routes) | 为某个模型（或组）固定指定渠道+账号+优先级 | ❌ 仅服务端路由配置 | 请求到达时优先匹配线路中的 provider/account，绕过默认轮询 | ✅ pi_key_ids / use_all_api_keys |

### 存在的问题

1. **管理端混为一谈**：两者都在"模型路由/分组"页面编辑，表单字段混杂，用户容易把"组"和"线路"配反。
2. **组内模型路由不可控**：模型组只能指定成员列表，无法为组内不同模型分配不同线路。例如 smart-group 里的 gpt-4 走渠道 A、claude-3 走渠道 B，当前做不到。
3. **专用线路无法引用模型组**：model_routes 的 model 字段只能写单个模型 ID，不能写组名，导致需要为组内每个模型分别建线路。
4. **路由决策不可见**：管理端没有"如果请求模型 X、API Key Y，会走哪条线路"的预览能力，排障困难。
5. **文档缺失**：docs/ 中没有专门页面解释两者的区别和使用场景，开发者只能通过代码反推。

### 优化方案

#### 1. 管理端拆分页面

- **模型组管理**（独立页面）：只负责"客户端可见模型列表的聚合"。
  - 字段：组名、启用、成员模型列表、API Key 白名单。
  - 移除与 provider、priority、weight 相关的字段。
- **模型专用线路**（独立页面）：只负责"服务端固定路由规则"。
  - 字段：请求模型（支持选择单模型或模型组）、线路 entries（provider → account → priority → weight）、API Key 白名单。

#### 2. 专用线路支持引用模型组

- model_routes 的 model 字段允许填写模型组名。
- 路由解析时（
ate_limiter.py）：
  - 若 model 是模型组名，则对组内每个成员模型分别匹配该线路规则。
  - 组内成员若有独立线路，独立线路优先；无独立线路时套用组的线路。

#### 3. 增加路由决策预览

- 在"模型专用线路"页面增加调试区域：
  - 输入：模型 ID + API Key。
  - 输出：匹配到的线路（provider、account、priority、weight）和fallback路径。
- 后端新增接口：POST /admin/route-preview {model, api_key?}，返回路由决策链。

#### 4. 配置结构简化

- model_groups 只保留：
  `json
  {"groups": {"smart": {"enabled": true, "models": ["gpt-4", "claude-3"], "api_key_ids": []}}}
  `
- model_routes 只保留：
  `json
  {"routes": [{"enabled": true, "model": "smart", "entries": [{"provider": "custom", "account": "acc1", "priority": 1, "weight": 100}], "api_key_ids": []}]}
  `

#### 5. 文档补充

- 在 docs/modules/ 下新增 model-routing.md，包含：
  - 模型组 vs 模型专用线路的对比表格。
  - 路由决策流程图（模型→专用线路→模型组展开→默认路由）。
  - 常见配置示例（负载均衡、多租户隔离、灰度发布）。

---
