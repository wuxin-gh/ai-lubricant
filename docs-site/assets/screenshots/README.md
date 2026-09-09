# 实机截图目录

本目录收录 Ai Lubricant 的脱敏实机截图。两类资源：

## 1. 能力详解页的预留图位（约定路径）

`cap-*.html` 的功能小节正下方可放图位。路径规则：

```text
<页面名去掉 .html>/<小节 id>.png
例：cap-llm-server/passthrough.png   ← cap-llm-server.html #passthrough
    cap-node/agent-safety.png        ← cap-node.html #agent-safety
```

把图片按此路径放进来即自动生效，无需改 HTML。若图片已在别处，也可以直接改页面上那个 `<figure class="shot">` 的 `src` / `data-shot` / `alt`。图片缺失时页面显示「截图待补」占位条并提示期望路径。

**一个图位放多张**：在基名后加 `-1`、`-2`、`-3`…（如 `cap-llm-server/testing.png + testing-1.png + testing-2.png`，或从 `testing-1.png` 起全部编号），加载时按序自动追加进同一图位并显示张数；从缺的那个编号起停止（上限 24 张）。

当前各页图位数（仅保留已有截图的图位；无图位小节要补图时按路径约定新增 `<figure class="shot">` 即可）：cap-llm-server 11、cap-editor 6、cap-agent 2、cap-android-control 2、cap-browser 2、cap-node 1、cap-security 1。待补图位保留在：cap-mobile 3、cap-resource-center 2、cap-ios-control 6。

## 2. 既有管理端截图（按主题分目录，45 张）

供手册页和上述图位取用。

## 目录清单

- `overview/`：管理端总览（1 张）
- `data-dashboard/`：数据仪表盘与用量（6 张）
- `members/`：成员管理（2 张）
- `channels/`：渠道与账号配置（14 张）
- `model-metadata/`：模型元数据（5 张）
- `api-keys/`：API Key 与权限（3 张）
- `request-logs/`：请求日志（2 张）
- `security/`：安全设置（1 张）
- `proxy-pool/`：代理池（2 张）
- `nodes/`：节点管理（3 张）
- `tunnel-schemes/`：隧道方案（6 张）

文件名使用小写、短横线和明确的语义，例如 `channels/channels-list.png`、`request-logs/request-log-detail.png`。部署静态站点时必须保留上述目录结构，不要使用空格、中文或时间戳作为新文件名。

当前资产集不包含 Console、Agent、编辑器或移动端 App 的实机截图；文档不得用管理端图片冒充这些界面。新增截图时请同步补充准确的 `alt` 文本和图注，并在发布前完成脱敏审核。

## 发布前检查

必须完全遮盖：

- API Key、模型 Token、Bearer/refresh token、Cookie、TOTP secret
- 节点安装命令、真实邮箱、用户名、IP、内网域名和仓库地址
- 客户代码、任务提示词、个人数据和浏览器敏感标签页

截图只能来自当前运行版本，不修改业务状态或伪造数据；只允许必要裁切与脱敏遮挡。保留原始 PNG 的清晰度，静态发布时确保 `assets/screenshots/` 下的所有资源可被页面访问。
