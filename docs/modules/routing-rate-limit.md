# 路由、限流和账号池

对应文件：`rate_limiter.py`

## 职责

该模块包含：

- API Key 速率限制。
- Redis RPM/TPM 追踪。
- 单账号运行态 `AccountClient`。
- 单渠道账号池 `ProviderPool`。
- 全局模型池 `ModelClientPool`。
- 定时刷新、会话清理、账号检查和日志清理。

## API Key 限流

`RateLimiter` 支持：

- 每分钟请求数。
- 每日请求数。
- Redis 可用时跨实例共享。
- Redis 不可用时进程内降级。

## AccountClient

每个账号维护：

- rpm/tpm/concurrency。
- in-flight 计数。
- priority 和 weight。
- cooldown。
- auth 状态。
- balance 状态。
- provider quota 状态。

## ProviderPool

每个渠道一个 ProviderPool：

- 持有多个 AccountClient。
- 初始化所有账号。
- 检查账号状态和余额。
- 执行会话清理。
- 标记账号 429 冷却。

## ModelClientPool

全局负责：

- 注册 Provider。
- 刷新模型列表。
- 维护 model -> providers 映射。
- 处理 model_routes 和 model_groups。
- 按权重、优先级、配额、余量、channel score 选账号。
- 记录 token 和 channel score。

