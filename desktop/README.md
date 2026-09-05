# Ai Lubricant 桌面版（Windows exe）

把现有服务端 + React 前端打包成一个双击即用的 Windows 应用。用的是**同一套服务端代码**——`main.py` / `db.py` / `node_server` / `tunnel_server` 均未改动，桌面特有逻辑全在本目录。

## 前置条件（必须）

本程序**不自带**数据库，需要可连接的：

| 依赖 | 要求 | 说明 |
|---|---|---|
| **PostgreSQL** | 必装 | 运行时状态的唯一存储（渠道、账号、Key、路由、任务、日志） |
| **Redis** | 必装，**≥ 6.0** | 限流、配额、冷却、会话、验证码。coredis 强制 RESP3 握手，`HELLO` 是 6.0 才有的命令 |
| ClickHouse | 可选 | 只影响“请求原文详情”和“Agent/网页对话历史”，核心网关不受影响 |

没有现成环境时，首次配置向导里有两条引导：`docker compose up -d postgres redis`（仓库已带 docker-compose.yml），或 Windows 原生的 Memurai + PostgreSQL 官方安装包。

## 构建

```powershell
powershell -ExecutionPolicy Bypass -File desktop\build.ps1
```

脚本依次做：前端 `pnpm build` → 安装 `requirements.txt` + `requirements-desktop.txt` → `pyinstaller desktop/AiLubricant.spec`。

产物：`dist\AiLubricant\AiLubricant.exe`（onedir 形态，依赖在同目录 `_internal\`）。目标机无需 Python。

## 运行时布局

安装目录保持只读；所有可写数据在 `%LOCALAPPDATA%\AiLubricant\`：

```
%LOCALAPPDATA%\AiLubricant\
  .env               ← 向导写入的连接信息 + 自动生成的密钥
  logs\              ← main.log / node_server.log / tunnel_server.log
  node-bin\          ← 节点二进制缓存
  mc-tunnel-bins\    ← frpc / cloudflared / npc 按需下载缓存
```

## 启动流程

1. `env_bootstrap` 把 `.env` 定位到 `%LOCALAPPDATA%`，首启生成并持久化 `NODE_CONTROL_TOKEN` 与 `NODE_CREDENTIAL_ENCRYPTION_KEY`——这样 node_server 启动时发现密钥已存在，**不会触发它自己的 `.env` 回写**（冻结目录不可写会让它 `RuntimeError` 拒启）。
2. 探测 PG + Redis。任一不通就先弹配置向导，保存成功后继续。
3. **串行**拉起三个服务：main（就绪后）→ node_server（就绪后）→ tunnel_server。串行是必须的：main 和 tunnel_server 都会对 `mc_tunnel_*` 做 DDL 对账且没有 advisory lock，并发会撞 duplicate-column。
4. 窗口打开 `http://127.0.0.1:8001/manager`。
5. 关窗口 → 子进程优雅停机；Windows Job Object 兜底回收残留（含 tunnel_server 派生的 frpc/cloudflared 孙进程）。

## 模块

| 文件 | 职责 |
|---|---|
| `main_window.py` | exe 入口：向导 → 监管器 → pywebview 窗口。冻结态下还兼任子进程解释器（`--run-child -m <module>`） |
| `paths.py` | 路径解析，兼容源码模式与 PyInstaller 冻结模式 |
| `env_bootstrap.py` | `.env` 重定位、密钥生成、桌面默认值注入 |
| `config_wizard.py` | 独立 FastAPI 配置向导 + PG/Redis/ClickHouse 连通性探测 |
| `supervisor.py` | 三进程串行启动、日志重定向、Job Object 回收 |
| `shell_asgi.py` | 静态资源前置层（见下），再委托给未改动的 `main:app` |
| `serve.py` | 桌面版 uvicorn 入口，跑 `shell_asgi:shell` |
| `_spec_check.py` | 开发用：校验 spec 的 datas 路径与 hiddenimports |

## `shell_asgi.py` 解决的问题

Vite 产物用绝对路径引资源（`<script src="/assets/index-*.js">`），但 `main.py` 只挂了 `/static` 和 `/admin-static`。`/assets/*` 没有 mount 也不在 `_API_PREFIXES` 里，会被 catch-all 当 SPA 路由返回 `index.html` → 浏览器把 HTML 当 JS 加载 → **白屏**。生产环境靠 nginx `try_files` 顶着，桌面版没有 nginx。

`shell_asgi` 在最外层包一个 ASGI 中间件：请求路径能映射到 `user-frontend/dist` 下真实文件的就直接返回该文件，否则原样透传给 `main:app`。`index.html` 故意**不**在这里处理，SPA 路由继续走 main.py 的 catch-all，保持其 API 前缀判断有效。

Docker 部署仍直接跑 `main:app`，行为零变化。

## 开发时直接跑（不打包）

```bash
# 复用仓库根的 .env，而不是 %LOCALAPPDATA% 里的那份
DESKTOP_ENV_FILE=d:/code/ai-lubricant/.env python -m desktop.main_window

# 只起服务、不开窗口（HTTP 层调试）
DESKTOP_ENV_FILE=d:/code/ai-lubricant/.env DESKTOP_MAIN_PORT=8011 python -m desktop.serve
```

## 已知限制

- **远程节点连不进来**：node_server 需要外部节点 dial h2c 进来，桌面机在 NAT 后面时只支持本机/局域网节点。桌面版默认绑 `127.0.0.1`（避免防火墙弹窗）；要放开需手动改 `.env` 的 `NODE_CONTROL_HOST`。
- **protobuf 必须精确 pin**：`agentcompose_v2_pb2.py` 在 import 时调 `ValidateProtobufRuntimeVersion`，版本不匹配直接硬失败。`requirements-desktop.txt` 锁的是 `7.35.1`，升级需同步验证。
- frpc / cloudflared / npc 不打包，首次使用穿透时从 GitHub 按需下载，需要外网。
