# Ai Lubricant 智能开发平台独立文档站

这是 **Ai Lubricant 智能开发平台** 的独立纯静态产品文档。它不属于数据服务、Web 前端、移动端或节点控制服务的运行时部署，应该单独作为静态网站发布。

## 产品主张

> Ai Lubricant 把模型网关、Agent、编辑器、代码 Review、远程节点、内网穿透、浏览器控制、手机控制、资源中心、邮件与安全治理串成一套完整生态。

平台负责把这些能力统一接入、配置和审计；私有化部署仍需准备服务器与依赖，模型凭据、Git、外部 MCP 与第三方系统使用你自己的合法授权。

## 文档导航

### 产品

- `index.html`：产品首页、完整能力中心与交互能力图谱
- `solutions.html`：企业模型入口、AI 开发交付、自有节点、工具自动化、私有化

### 能力详解

- `cap-llm-server.html`：免费模型监控、模型列表自动更新、渠道标签、多协议链路、多种冻结、六种选路、8 种速率限制、自定义测试与上游拒绝；含 Cloudflare Workers URL 代理说明
- `cap-code-channel.html`：自定义代码渠道的场景索引、钩子速查、类级开关、统一帧、样例与常见错误
- `cap-agent.html`：主/子 Agent、定时任务、终端 Agent、浏览器与网页内 Agent、邮箱 Agent、记忆增强
- `cap-editor.html`：Claude Code/Codex/OpenCode/Cursor/Gemini 接入、三种执行环境（隔离/共用/系统）、代码 Review、会话隔离
- `cap-node.html`：一键连接 Agent、三角色、无需 SSH、Agent 安全控制、PTY/host exec/文件/代理出口
- `cap-mobile.html`：任务跟踪、新建任务与远程操控、语音转写、附件、预签名直传、下载升级
- `cap-browser.html`：把真实 Chrome 交给 Agent 与编辑器、12 个 CDP 工具、网页内 Agent、两类 token 隔离
- `cap-device-control.html`：Android 控制 APK（非 ADB）、WebSocket v0、15 个操控命令、iOS node-ios
- `cap-mail.html`：邮件读取 MCP、按实例隔离只读、别名标注、正文长度控制、附件元数据
- `cap-security.html`：API Key / MCP 保护、子 Key 归并、token 只存哈希、节点 TOTP
- `cap-resource-center.html`：GitHub 资源榜单接入，Skill/MCP/提示词/插件/框架集中展示与搜索，勾选即用于 Agent/任务/编辑器/Review

### 操作手册

- `guide-developer.html`：开发者的项目、任务、Agent、编辑器、终端、文件与资源
- `users-admin.html`：平台管理员的渠道、账号池、路由、API Key、日志、MCP、节点
- `guide-node.html`：节点管理员的 onboard、角色、审批、主动连接、终端与代理
- `users-mobile.html`：移动端登录、项目、任务、EAS、下载升级与发布安全

### 部署与接入

- `users-api.html`：OpenAI / Anthropic 兼容 API、SDK、流式、错误与重试
- `architecture.html`：技术白皮书——同协议直通、客户端模板、选路与熔断、用量口径、CDP 工具、请求级观测
- `governance.html`：凭据、API Key、Agent、节点、MCP、日志、私有化与事件响应
- `deploy.html`：拉取代码（Gitee）、`.env` 配置、Docker Compose 五服务部署、`server/init_db.py` 建表、手动镜像与生产清单
- `desktop.html`：Windows 桌面版（自带 Python/前端/三个服务进程，PostgreSQL 与 Redis 需自备）
- `env-vars.html`：环境变量与配置项总表（按 server/postgres/redis/outbound/ai_lubricant/clickhouse/marketplace 分组，含进程归属、必需性与进程级变量）
- `local-debug.html`：四个进程与端口一览、本地依赖、逐个启动（`main.py` / `node_server` / `tunnel_server` / Vite）、探针、日志和故障排查

## 浏览与独立部署

直接双击 `index.html`，或在本目录启动任意静态文件服务器：

```bash
python -m http.server 8088
```

然后访问 `http://127.0.0.1:8088/`。

生产发布时，把整个 `docs-site/` 目录复制到 nginx、Caddy、对象存储静态网站或其他静态托管服务。必须保留目录结构，特别是 `assets/docs.css`、`assets/docs.js` 和 `assets/screenshots/`。

```nginx
server {
    listen 443 ssl;
    server_name docs.example.com;
    root /srv/ai-lubricant-docs;
    index index.html;

    location / {
        try_files $uri $uri/ =404;
    }
}
```

文档站自身不需要反向代理到 Ai Lubricant 后端；操作手册中的 API 地址、端口和命令只是被记录系统的使用说明。

### Cloudflare Pages 部署

文档站通过 Wrangler 直接上传发布（仓库 origin 是自建 Git 服务，Cloudflare 无法做 Git 直连集成）。仓库根的 `wrangler.toml` 已声明 `pages_build_output_dir = "docs-site"`；`docs-site/_headers` 提供 Pages 端的安全响应头与资产缓存策略，随目录一起上传。

```bash
# 1. 认证（二选一）
npx wrangler login                       # 浏览器授权
# 或设置环境变量 CLOUDFLARE_API_TOKEN 与 CLOUDFLARE_ACCOUNT_ID

# 2. 首次创建 Pages 项目（只做一次）
npx wrangler pages project create ai-lubricant-docs --production-branch=master

# 3. 预览部署（不进生产域名）
npx wrangler pages deploy docs-site --branch=preview

# 4. 生产部署
npx wrangler pages deploy docs-site --branch=master
```

注意：`--branch` 必须显式指定，与项目创建时的 `--production-branch=master` 一致才进生产域名，避免误发预览版本。Pages 只托管本文档静态站，不反代 Ai Lubricant 后端；`assets/cloudflare-url-proxy-worker.js` 只是页面提供的可下载示例文件，不是站点运行时，不要把它配置成 Pages Function 或 Worker。

## 实机截图

没有独立画廊页。能力详解页的功能小节（`<h2>`）正下方可放图位，图片路径按「页面名 / 小节 id」约定生成，图片文件放进去即生效：

```text
assets/screenshots/<页面名去掉 .html>/<小节 id>.png
例：assets/screenshots/cap-llm-server/passthrough.png   ← cap-llm-server.html #passthrough
    assets/screenshots/cap-node/agent-safety.png        ← cap-node.html #agent-safety
```

图位是 `<figure class="shot" data-shot="…">`，图片缺失时显示「截图待补」占位条并附上期望路径；加载成功后占位条由 `assets/docs.js` 自动移除，图片接入 Lightbox。要换成别的图片，直接改这个 figure 里的 `src`（以及 `data-shot`、`alt`）即可，不必迁移文件。要给暂无图位的小节补图，按上面的路径约定新建一个 `<figure class="shot">` 即可（参考 `cap-ios-control.html` 现有写法）。

**一个图位放多张**：在基名后加 `-1`、`-2`、`-3`… 依次命名，页面加载时会按序探测并全部追加进同一图位，右上角显示张数，Lightbox 里可左右翻页。两种写法都行：`passthrough.png + passthrough-2.png + passthrough-3.png`，或全部编号 `passthrough-1.png + passthrough-2.png`；从缺的那个编号起停止探测（单个图位上限 24 张）。

当前各页图位数（仅保留已有截图的图位）：cap-llm-server 11、cap-editor 6、cap-agent 2、cap-android-control 2、cap-browser 2、cap-node 1、cap-security 1；待补图位保留在：cap-mobile 3、cap-resource-center 2、cap-ios-control 6。

另有 45 张既有管理端（`/manager`）实机截图，按页面主题分目录存放，供手册页和上述图位取用：

```text
docs-site/assets/screenshots/
├── overview/        管理端总览
├── data-dashboard/  数据仪表盘与用量
├── members/         成员管理
├── channels/        渠道与账号配置
├── model-metadata/  模型元数据
├── api-keys/        API Key 与权限
├── request-logs/    请求日志
├── security/        安全设置
├── proxy-pool/      代理池
├── nodes/           节点管理
└── tunnel-schemes/  隧道方案
```

文件名使用小写、短横线的语义化命名（如 `nodes/nodes-list.png`），发布时必须保留目录结构。新增或替换截图时需同步更新对应 figure 的 `src`、`alt` 与图注，并先完成脱敏审核；不得用管理端截图冒充 Console、Agent、编辑器或移动端界面。详见 `assets/screenshots/README.md`。

截图发布前必须检查并遮盖：

- API Key、模型 Token、Cookie、TOTP secret、节点安装命令
- 真实邮箱、用户名、IP、内网域名和仓库 URL
- 客户代码、任务提示词、个人数据和浏览器敏感标签页

## 特性

- 不需要 npm、Node.js 或构建步骤
- 不引用 CDN 或第三方运行时资源
- 支持 `file://` 直接打开
- 浅色/深色主题、响应式导航、代码复制、打印和截图 Lightbox
- 页面内容按决策者、开发者、管理员、节点管理员、API 调用者和移动端用户分层

## 内容真值

文档根据当前源码、`docker-compose.yml`、`.env.example`、Web 路由、移动端配置和节点控制服务实现编写。仓库根目录旧 `README.md` 的部分 Qwen 单渠道、`config.json` 和 8000 端口描述已经过时，不应作为当前部署依据。

工程名/组件名 `ai-lubricant` 只在技术语境中表示统一模型网关后端；本产品文档的对外品牌统一为 **Ai Lubricant**。
