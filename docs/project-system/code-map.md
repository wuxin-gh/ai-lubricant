# 代码索引

这个文件只指向源码入口和相关文档，不复制实现细节。真实行为以源码和测试为准。

## 公共 API 与请求生命周期

- 关键文件：`main.py`
- 模块说明：[`../modules/main-app.md`](../modules/main-app.md)
- 相关白皮书：
  - [`whitepapers/request-logging.md`](./whitepapers/request-logging.md)
  - [`whitepapers/token-accounting.md`](./whitepapers/token-accounting.md)
  - [`whitepapers/protocol-passthrough.md`](./whitepapers/protocol-passthrough.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)

## 管理端

- 关键文件：`admin.py`、`static/admin.html`
- 模块说明：[`../modules/admin.md`](../modules/admin.md)、[`../modules/static-scripts.md`](../modules/static-scripts.md)
- 相关白皮书：
  - [`whitepapers/request-logging.md`](./whitepapers/request-logging.md)
  - [`whitepapers/model-routing.md`](./whitepapers/model-routing.md)
  - [`whitepapers/token-accounting.md`](./whitepapers/token-accounting.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)

## 数据库和请求日志

- 关键文件：`db.py`、`sql/init.sql`
- 模块说明：[`../modules/database.md`](../modules/database.md)
- 相关白皮书：
  - [`whitepapers/request-logging.md`](./whitepapers/request-logging.md)
  - [`whitepapers/token-accounting.md`](./whitepapers/token-accounting.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)

## 路由、限流和账号池

- 关键文件：`rate_limiter.py`
- 模块说明：[`../modules/routing-rate-limit.md`](../modules/routing-rate-limit.md)
- 相关白皮书：[`whitepapers/model-routing.md`](./whitepapers/model-routing.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)

## Provider 层

- 关键文件：`providers/`
- 模块说明：[`../modules/providers-layer.md`](../modules/providers-layer.md)、[`../providers/README.md`](../providers/README.md)
- 相关白皮书：
  - [`whitepapers/model-routing.md`](./whitepapers/model-routing.md)
  - [`whitepapers/protocol-passthrough.md`](./whitepapers/protocol-passthrough.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)

## 协议转换和工具调用

- 关键文件：`message_utils.py`、`tool_utils.py`
- 模块说明：[`../modules/protocol-tools.md`](../modules/protocol-tools.md)
- 相关白皮书：[`whitepapers/protocol-passthrough.md`](./whitepapers/protocol-passthrough.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)

## 模型元数据

- 关键文件：`model_metadata.py`、`model_metadata.json`
- 模块说明：[`../modules/model-metadata.md`](../modules/model-metadata.md)
- 相关白皮书：[`whitepapers/model-routing.md`](./whitepapers/model-routing.md)
- 活跃计划：[`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)
