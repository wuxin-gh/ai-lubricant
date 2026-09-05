# 公共 API 与应用入口

对应文件：`main.py`

## 职责

`main.py` 是服务入口，负责：

- FastAPI app 创建和生命周期。
- 注册 Provider 和账号。
- 公共 API 路由。
- API Key 校验和限流入口。
- OpenAI/Anthropic 请求转换和响应转换。
- 请求重试和账号切换。
- 请求日志、用量统计和最近日志。

## 生命周期

启动阶段：

1. 初始化 Postgres。
2. 初始化配置存储。
3. 初始化 Redis。
4. 注册内置 Provider。
5. 注册自定义 Provider。
6. 加载启用账号。
7. 异步启动 `ModelClientPool.initialize()`。

关闭阶段：

- 停止定时任务。
- 关闭 Postgres。

## 主要路由

- `/v1/models`
- `/v1/chat/completions`
- `/v1/messages`
- `/v1/dashboard/billing/usage`
- `/v1/token/count`
- `/v1/images/generations`

## 请求日志

请求日志记录：

- 请求体、响应体和上下游 header。
- Provider、账号、模型。
- retry path。
- duration 和 first token。
- usage。
- 错误详情。

流式请求通过包装 generator 在 finally 中写日志。

