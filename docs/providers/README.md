# 渠道文档索引

本目录按 Provider 渠道拆分说明。每篇文档只描述定位、配置、认证、模型列表、聊天链路和能力。

渠道问题、风险和后续改进项统一维护在 [统一问题与改进清单](../issues.md)。

## 基础层

- [BaseProvider 基类](./base.md)

## 可配置渠道

- [Custom 渠道](./custom.md)
- [代码渠道（Code Channel）](./code-channel.md) — 贴一个 spec 类（普通类 + `@staticmethod` 钩子）即造一个完整渠道

> CLI 逆向渠道（copilot/codebuddy/atomcode/eaichat/qoder）已下架为「代码渠道」形态：
> 产品只发框架能力，spec 源码由使用者自行粘贴进管理端「源码」Tab 与分发。
