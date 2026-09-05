# 管理端

对应文件：`admin.py`、`static/admin.html`

## 职责

管理端负责运行时配置和观测：

- 管理员登录和 session。
- 主配置热更新。
- API Key 管理。
- Provider 配置和账号管理。
- 自定义 Provider 创建。
- 模型路由和模型组。
- 账号检查、初始化、测试。
- 请求日志、仪表盘和统计。

## Session 机制

- 登录成功后生成随机 token。
- Redis 可用时写入 `admin:session:{token}`。
- Redis 不可用时使用 `_admin_sessions_mem`。
- 默认 Redis TTL 7 天，配置中可调整 session duration。

## 配置热更新

写配置后会：

1. 写入 `CONFIG_STORE`。
2. 调用 `config.Config.reload_async()`。
3. 对账号或模型变更触发 pool 热加载或模型刷新。

## Provider 管理

管理端支持：

- 启停 Provider。
- 修改 retry count。
- 配置清理会话。
- 修改模型别名和白名单。
- 获取上游模型列表。
- 健康检查和账号测试。
- 新增自定义渠道。

