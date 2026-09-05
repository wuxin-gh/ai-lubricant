# 新对话交接指南

当你开启一个新的 AI 对话，希望它继续使用本项目已经梳理出的结构、约定和模型列表设计时，可以把本指南作为第一条上下文。

## 推荐给新对话的提示词

```text
你正在协助维护 Ai Lubricant（d:\code\ai-lubricant）项目。请先阅读 docs/README.md、docs/architecture.md、docs/issues.md，以及和当前任务相关的 docs/modules 或 docs/providers 文档。

重要约定：
1. 文档入口是 docs/README.md。
2. 渠道说明在 docs/providers/，模块说明在 docs/modules/。
3. 问题、风险和待优化项统一写入 docs/issues.md，不要分散写到各渠道或模块文档。
4. 模型列表分三层：
   - fetch_upstream_model_list()：渠道上游真实全量模型列表，不按白名单过滤。
   - fetch_model_list()：客户端可见模型列表，需要按白名单过滤。
   - ModelClientPool.fetch_provider_upstream_models()：管理端使用，返回上游全量模型、白名单状态和重定向映射。
5. 管理端“模型管理”卡片只编辑当前白名单和重定向；“获取模型列表”按钮单独请求上游全量模型并弹框选择。
6. 不要自动在 provider base/detail 接口里请求上游模型列表，避免页面加载触发大量上游请求。
7. 先读代码和文档再修改，避免把客户端列表、管理端配置列表、上游全量列表混在一起。
```

## 新对话应优先读取

1. [文档入口](../README.md)
2. [系统架构](../architecture.md)
3. [统一问题与改进清单](../issues.md)
4. [Provider 层](../modules/providers-layer.md)
5. [路由、限流和账号池](../modules/routing-rate-limit.md)
6. [管理端](../modules/admin.md)

## 模型列表设计约定

### 上游全量列表

方法：`BaseProvider.fetch_upstream_model_list()`

用途：

- 初始化时建立渠道模型能力。
- 定时刷新时重新获取上游模型。
- 管理端点击“获取模型列表”时弹框选择。

返回应包含：

- `id`：系统内模型名，可能是别名。
- `alias`：对外显示或调用的别名。
- `upstream_id`：上游真实模型名。
- `raw`：可选，上游原始模型项。

### 客户端可见列表

方法：`BaseProvider.fetch_model_list()`

用途：

- `/v1/models` 和 `/models`。
- `ModelClientPool.refresh_models()` 构建客户端可调用模型注册表。

规则：

- 应基于上游全量列表。
- 应应用渠道白名单。
- 应补齐 OpenAI 模型元数据。

### 管理端全量视图

方法：`ModelClientPool.fetch_provider_upstream_models(provider_name)`

用途：

- `/admin/providers/{name}/upstream-models`。
- 管理端“获取模型列表”按钮。

返回应包含：

- `models`：上游全量模型，带 `id`、`alias`、`upstream_id`。
- `whitelist`：当前配置的白名单。
- `model_redirects`：系统模型名到上游真实模型名的映射。

## 修改文档的规则

- 新问题写入 [统一问题与改进清单](../issues.md)。
- 渠道文档只写事实说明，不写“当前问题”章节。
- 模块文档只写结构、职责、数据流，不写分散的问题列表。
- 如果调整模型列表逻辑，必须同时检查：
  - `providers/base.py`
  - 相关 provider 的 `fetch_upstream_model_list()` / `fetch_model_list()`
  - `rate_limiter.py::ModelClientPool.refresh_models()`
  - `admin.py` 管理接口
  - `static/admin.html` 模型管理卡片
