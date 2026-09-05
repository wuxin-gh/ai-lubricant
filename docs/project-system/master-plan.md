# 项目总计划

## 当前阶段

项目正在从“需求和原因分散在代码、计划、聊天记录和 spec 中”迁移到“四级文档体系”。当前阶段的重点是先把长期规则和高频变更主题收拢，减少后续需求反复丢上下文。

## 主题优先级

| 优先级 | 主题 | 状态 | 白皮书 | 当前下一步 |
| --- | --- | --- | --- | --- |
| P0 | 请求日志 | active | [`request-logging.md`](./whitepapers/request-logging.md) | 把请求路径、渠道请求记录、详情展示规则收敛到白皮书和新计划入口。 |
| P0 | Token 计费口径 | active | [`token-accounting.md`](./whitepapers/token-accounting.md) | 确认所有统计和展示都使用 total=输入+输出，缓存读/写只作为输入明细。 |
| P1 | 模型路由 | active | [`model-routing.md`](./whitepapers/model-routing.md) | 收敛模型组、模型元数据、渠道筛选和 Public API 语义。 |
| P1 | 凭证安全（CDP 密码金库） | planned | [`credential-security.md`](./whitepapers/credential-security.md) | 按已确认设计实施 [`plans/2026-08-30-credential-vault.md`](./plans/2026-08-30-credential-vault.md)：ask_user 录入、UUID 引用、插件侧解密替换。 |
| P1 | 同协议直通 | planned | [`protocol-passthrough.md`](./whitepapers/protocol-passthrough.md) | 明确 anthropic 到 anthropic 原始转发边界，避免跨协议逻辑污染同协议路径。 |
| P2 | 文档体系迁移 | active | 本文件和 [`README.md`](./README.md) | 执行迁移计划并做迁移中、迁移后合理性审查。 |

## 状态定义

- `planned`：已确认需要做，但当前不是主线。
- `active`：正在设计、迁移或实现。
- `paused`：暂停推进，需要原因。
- `done`：当前目标完成并已验证。

## 本阶段完成标准

- 新需求默认从 `docs/project-system/README.md` 进入。
- P0/P1 主题都有白皮书，且白皮书只记录长期原因和原则。
- 新计划都写明 `Whitepaper:` 和 `Master plan item:`。
- 至少完成一次从“总计划 -> 白皮书 -> 计划 -> 代码索引”的追溯检查。
- 至少完成一次从“代码模块 -> 代码索引 -> 白皮书/计划”的反向追溯检查。

## 当前最应该做的下一步

执行 [`plans/2026-06-01-project-documentation-system.md`](./plans/2026-06-01-project-documentation-system.md)，先建立新中枢和四篇高价值白皮书，再审查结构是否真的降低混乱。
