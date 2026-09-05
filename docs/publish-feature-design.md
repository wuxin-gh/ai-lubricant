# 项目发布会话 — 实现方案

> 自包含文档。新 session 读这一份即可上手,不必回看讨论过程。

## 1. 背景与目标

### 要做什么
给"项目"增加**发布**能力:把一个项目(有 git 仓库)跑起来作为一个可访问的服务。三种发布形态:

1. **Docker**:管理节点上 `docker build` + `docker run`(或 compose up)
2. **调试/环境**:长驻进程跑 dev server(`npm run dev` / `python app.py` 等)
3. **静态/nginx**:build 出静态产物 → 写 nginx server block → reload

发布通过**跟 agent 聊天**驱动(agent 在专门的发布会话里决定怎么发),发布结果与服务状态在**项目页「发布情况」视图**显示,停止/回滚按钮确定性执行(不开新对话)。

### 用户硬性要求(不可违背)
- **不另开旁路**:发布 MCP 必须走现有 **principal + grants** 关联机制,不能像 issue-workflow 那样在 principal 体系外另开"直接绑 token + 注入 overlay"的旁路
- **不碰 agent**:agent runtime 代码一行不动。agent 被动读 mcpServers 配置,我们只控制会话创建时往 mcpServers 里写什么
- **所有现有流程不变**:会话创建、ApplySessionMCPs、agent 聊天、现有 CDP bridge/mail MCP 全不动
- **纯新增**:现有 agent 能力不受影响
- **执行节点不操作宿主机**;**管理节点**能 host_exec + docker/nginx 控制。发布只锚在管理节点上
- 代码源:**管理节点 `git clone` 项目仓库的指定 ref**(可复现、有指纹、能回滚),不从执行节点工作目录取产物

---

## 2. 现状调研结论(已坐实,直接用)

### 2.1 agent 会话的实际形态
**agent 不是一个独立的常驻"agent 服务进程"。每个会话本身就是节点上的一个独立 editor runtime 进程。** 会话 = 进程。

- `streamExecutor`([nodes/execution/stream_executor.go](nodes/execution/stream_executor.go))起 `agent-compose-runtime` 长驻进程,stdin 吃 NDJSON 输入帧、stdout 吐 NDJSON 输出。一轮结束**不退出**,等下一个 human_message。
- 每会话**独立进程 + 独立 workDir + 独立 HOME**(`<workDir>/.agent-compose/home`)。
- `startRuntime`([nodes/execution/session.go:309](nodes/execution/session.go#L309))→ `selectExecutor`([nodes/execution/executor.go:19](nodes/execution/executor.go#L19))→ `run`。driver: `local`(宿主进程)/ `docker`(容器内,需 guest_image)。
- 热更新:每个 human_message 帧携带当前 config snapshot,model/mode/MCP/skill/plugin 改了下轮生效,不重启。

### 2.2 agent runtime 怎么拿到 MCP 列表(关键)
agent runtime(那个 stream 进程)**只读 mcpServers 配置**。链路:

1. 服务端调 `apply_node_session_mcps`([monkeycode_compat/node_client/client.py:325](monkeycode_compat/node_client/client.py#L325))下发会话的 MCP 集合
2. 节点侧 `applyMCPs`([nodes/execution/session.go:550](nodes/execution/session.go#L550))把会话的 editor MCP 配置**精确集合重写**进 `mcpServers`(codex 写 `config.toml` 的 managed mcp block;claude/gemini 走 env/config,见 [nodes/execution/editorconfig.go](nodes/execution/editorconfig.go))
3. editor runtime 读 `mcpServers` → agent 看到的工具就是这套

**会话的 MCP 集合是权威列表,隔离边界 = 列表成员资格。** agent 不"识别会话 type",它只读被重写后的 mcpServers。

### 2.3 现有两套 MCP 关联机制(必须分清)

| 机制 | 位置 | 作用 |
|---|---|---|
| **editor.mcp_config + apply_node_session_mcps**(连接层) | agent 端 | editor 有 `mcp_config`,session 有 `mcp_overlay_json`;`_merge_mcp_config` 合并后下发,节点 applyMCPs 写 mcpServers → runtime 连这些端点 |
| **MCP-user principal + grants**(授权层) | MCP 服务端 | task/agent 有 `mcp_user_id`(usage_type=agent/task/external 的 mcp_user principal),principal 对 resource(service/builtin_instance)有 grants。MCP 服务端收到请求时 token→principal→grants 校验 |

principal 路由:[monkeycode_compat/routes_mcp_principals.py](monkeycode_compat/routes_mcp_principals.py)。principal store:[mcp_plugin_store.py](mcp_plugin_store.py)(`create_owned_mcp_principal` @ 953,`get_owned_mcp_principal` @ 919,`replace_owned_mcp_principal_grants` @ 1451,`list_mcp_principal_grants` @ 1425)。task 的 principal 视图:[task_service.py:184](monkeycode_compat/task_service.py#L184) `_task_principal_view`。

### 2.4 issue-workflow 是**旁路**(不要抄)
[task_service.py:648](monkeycode_compat/task_service.py#L648) `_attach_issue_workflow_mcp` 把一个带 `issue_token(target_id=task.id)` 的 entry 直接塞进 `mcp_overlay_json` → 走 applyMCPs 注入,但 token 直接绑 task、**不经过 principal grants**。这是在 principal 体系外的旁路。**本方案明确不采用此模式**,改走 principal+grants。

### 2.5 节点能力归属
- `host_exec` / `terminal_*` / `file_upload` / `tunnel_request` 这些"干活"帧的逻辑全在共享层 `common/agent`([nodes/common/host_exec.go](nodes/common/host_exec.go)、[nodes/common/host_file_upload.go](nodes/common/host_file_upload.go))。**execution 和 management 两个 handler 都已经挂了这些 case**([nodes/execution/handler.go](nodes/execution/handler.go)、[nodes/management/handler.go](nodes/management/handler.go))。
- 管理节点独有的增量能力是 `create/delete_execution_node`(拉起/销毁执行节点)。
- **`host_exec` 派发要求**:节点必须 approved + online + capabilities 标签 `host_exec=true`([node_server/service.py:1533](node_server/service.py#L1533) `host_exec` 方法)。
- node_server 是**进程内** Python 服务(`node_server_enabled` 默认 True,[monkeycode_compat/config.py:169](monkeycode_compat/config.py#L169))。publish-MCP 处理器在服务端进程内,可直接调进程内 node-server 的 `host_exec`,把命令派发到绑定的管理节点。

### 2.6 token 机制
`builtin_tool_store.issue_token(target_type, target_id)`([builtin_tool_store.py:231](builtin_tool_store.py#L231))签发 builtin_tool_token。`target_type ∈ {agent, node, user, external}`。内置 MCP 端点形态:`/mcp/{name}/sse?token=<token>`。MCP 服务端从 token 反查 principal/scope。

---

## 3. 设计决策(最终)

### 3.1 顺着 principal+grants 走(核心)
创建 publish_session 时,给这个会话建一个 principal(`usage_type='publish'`),做两件事:
- principal 行**带绑定字段** `publish_project_id` / `publish_node_id` / `publish_git_ref`
- grant 给 publish-MCP resource

agent 通过现有 principal→grants 机制拿到 publish-MCP(跟现在 agent 拿别的 MCP 同一条路),**不另开旁路**。publish-MCP 服务端从 principal 反查绑定的 `(project_id, node_id, git_ref)` → 调进程内 node-server `host_exec` 派发到绑定的管理节点。

### 3.2 会话 type 决定挂哪个 MCP
会话表加 `type` 字段。创建会话时按 type 决定:
- `type='editor'`(现有):挂 editor 的 mcp_config
- `type='publish'`(新):建 publish principal + grant + 把 publish-MCP entry 塞进 session mcp 列表,走 `apply_node_session_mcps` 下发

### 3.3 publish-MCP 工具签名无 scope 参数
工具如 `docker_run(image, port)` / `nginx_apply(config)` / `static_serve(dir)` / `git_pull()` / `port_probe()` / `stop_publish()` —— **没有 project_id/node_id 参数**,scope 从 principal 反查。agent 物理上调不到别的项目/节点。

### 3.4 UI 收敛
- 对话:复用现有 agent 气泡聊天,只是会话换成 publish_session(绑管理节点)
- 状态:项目页加**「发布情况」**视图,列发布会话 + 发布产物(容器id/端口/nginx配置/git ref)+ 实时探活 + 停止/回滚按钮

### 3.5 权限:执行节点不碰宿主机,管理节点锚定
发布会话创建时选管理节点,校验:管理节点角色 + `host_exec=true` 能力位 + 当前用户/项目有权用它。执行节点不能被选为发布目标。

---

## 4. 具体改动点(文件级)

### 4.1 数据库迁移([db.py](db.py))
- `editor_sessions` 表([db.py:411](db.py#L411))加列:
  ```sql
  ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS session_type VARCHAR(16) NOT NULL DEFAULT 'editor';
  ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS publish_project_id TEXT;
  ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS publish_node_id TEXT;
  ALTER TABLE editor_sessions ADD COLUMN IF NOT EXISTS publish_git_ref TEXT;
  ```
- `mcp_users` 表加绑定列(沿用现有 ALTER ADD COLUMN 幂等模式,[db.py:1279](db.py#L1279) 附近):
  ```sql
  ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS publish_project_id TEXT;
  ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS publish_node_id TEXT;
  ALTER TABLE mcp_users ADD COLUMN IF NOT EXISTS publish_git_ref TEXT;
  ```
- 新建发布记录表 `publish_releases`(id, session_id, project_id, node_id, git_ref, kind=docker/dev/static, payload jsonb, status, created_at, updated_at, stopped_at, rollback_of)。状态机:`draft` → `deploying` → `running` → `stopped` → `failed` → `rolled_back`。

### 4.2 principal 扩展([mcp_plugin_store.py](mcp_plugin_store.py))
- `USAGE_TYPES`([mcp_plugin_store.py:883](mcp_plugin_store.py#L883))加 `"publish"`:`("agent", "external", "task", "publish")`
- `create_owned_mcp_principal`([mcp_plugin_store.py:953](mcp_plugin_store.py#L953))扩参:`publish_project_id` / `publish_node_id` / `publish_git_ref`,INSERT 时写入。`usage_type='publish'` 时 token masked(不返回明文,服务端自用)。
- 加查询函数 `get_publish_principal_scope(principal_id) -> {project_id, node_id, git_ref}`,供 publish-MCP 服务端反查。

### 4.3 publish-MCP 服务端(新文件)
新建 `mcp_builtin/publish_service.py` + 路由挂到 `/mcp/publish/sse`(复用现有内置 MCP 接入口径,参考 cdp-bridge/mail)。
- token 校验:从 query token 反查 principal → 取 `publish_*` 绑定。
- 工具(无 scope 参数):
  - `git_pull()`:在管理节点 workDir `git clone` / `fetch + checkout` 指定 ref(principal.git_ref)
  - `docker_build(dockerfile_path?)` / `docker_run(image, port, env?)` / `docker_stop(container_id)`
  - `nginx_apply(server_name, config)` / `nginx_reload()`
  - `static_serve(dir, port)`
  - `port_probe(port)` / `container_status(container_id?)`
  - `record_release(kind, payload)`(回写 publish_releases)
  - `stop_release(release_id)` / `rollback_release(release_id)`
- 每个工具执行:host_exec 派发到 `principal.publish_node_id`(调进程内 node-server `host_exec`,[node_server/service.py:1533](node_server/service.py#L1533))。
- 工具调用后回写 publish_releases。

### 4.4 创建 publish_session 流程(新路由 + service)
新增 `monkeycode_compat/routes_publish.py`(或并入 routes_project.py):`POST /api/v1/users/projects/{project_id}/publish`。
流程:
1. 校验:用户对 project 有写权限;选的 node_id 是管理节点 + `host_exec=true` + 在线 + 用户有权
2. 取项目 git 仓库 + 指定 ref(branch/commit)
3. 建 `editor_sessions` 行:`session_type='publish'`, `publish_project_id`, `publish_node_id`, `publish_git_ref`
4. 建 principal:`create_owned_mcp_principal(usage_type='publish', publish_project_id=..., publish_node_id=..., publish_git_ref=...)`;principal 的 token 用于 publish-MCP 鉴权
5. grant 给 publish-MCP resource(`replace_owned_mcp_principal_grants`)
6. 拼 publish-MCP entry:`{name:'publish', type:'sse', url:f'{base}/mcp/publish/sse?token={principal_token}'}`,塞进 session 的 mcp 列表(merge 进 editor.mcp_config 或单独 overlay)
7. 调 `apply_node_session_mcps` 下发到绑定的管理节点 + createSession(workDir 在管理节点给发布 clone 的目录)+ StartSessionRuntime
8. 返回 session_id

停止/回滚端点(在 routes_publish.py):
- `POST /api/v1/users/projects/{project_id}/publish/{session_id}/stop`
- `POST /api/v1/users/projects/{project_id}/publish/{session_id}/rollback`
- 都走 publish-MCP 的 stop/rollback 工具(按 publish_releases 记录确定性执行,不开新对话)

### 4.5 项目「发布情况」视图(只读查询端点 + 前端)
- `GET /api/v1/users/projects/{project_id}/publish`(list publish_sessions + 每个 session 的 publish_releases + 实时状态)
- 前端:项目页新增「发布情况」tab,列发布会话 + 发布版本 + 产物(容器/端口/nginx)+ 探活状态 + 停止/回滚按钮。点击会话进入气泡聊天(复用现有 agent 聊天组件,session_id 路由)。

### 4.6 不需要改的(明确)
- agent runtime 代码(stream_executor / executor / session.go 的 run):不动
- 节点协议 proto:不动(host_exec/terminal/file_upload 已就绪)
- 现有 CDP bridge / mail 内置 MCP:不动
- 现有 editor session 流程:不动(只是 session_type 多一种值)
- ApplySessionMCPs / applyMCPs:复用,不动

---

## 5. 风险与待确认

### 5.1 必须先验证的风险

| 风险 | 说明 | 验证方式 |
|---|---|---|
| **publish-MCP 从进程内调 node-server host_exec** | issue-workflow 是数据 MCP 不碰节点;publish-MCP 要调进程内 node-server 派发 host_exec。进程内调用是否 OK、并发是否 OK | 写一个最小 spike:publish-MCP 工具里调 `node_server.service.host_exec(req)`,验证能在管理节点上跑通 `docker ps` |
| **provider 能否是 codex** | 记忆记录过"Codex 不消费 MCP",但 [editorconfig.go](nodes/execution/editorconfig.go) 有 codex managed mcp block(`writeCodexRuntimeConfig`)。发布会话用什么 provider 要核实 | 确认 codex 是否真的读 mcpServers;不行就发布会话限 claude/gemini/opencode |
| **管理节点 workDir 与 git clone 凭据** | 管理节点 clone 项目仓库需要 git 凭据(项目有 `git_identity_id`)。workDir 怎么准备、跟管理节点自身工作目录冲突? | 看 editor 的 git provision 逻辑([session.go:704](nodes/execution/session.go#L704) `provisionGit`),复用同一套 |
| **principal 模型扩展不破坏现有用法** | 给 mcp_users 加 publish_* 列 + 新 usage_type,会不会影响现有 agent/external/task principal 的查询/鉴权 | USAGE_TYPES 加 'publish' 后,跑现有 principal 相关测试 + 鉴权链路 |

### 5.2 设计性风险(要在实现前定)

| 风险 | 处理建议 |
|---|---|
| **发布服务暴露入口** | 管理节点 docker/nginx 占宿主端口/域名 vs model-api 统一按 project_id 反代。建议:形态1/3(docker/nginx)走管理节点宿主端口 + 项目路由表反代;形态2(调试)走节点 tunnel_request 临时反代 |
| **停止/回滚确定性** | 容器清理(docker stop/rm by 容器id)、nginx 配置删除 + reload。按 publish_releases 记录的容器id/配置路径执行,不靠 agent |
| **发布状态机真相源** | 状态在服务端 PG(publish_releases)+ Redis(运行态)。管理节点掉线 → 重试/换节点,不丢状态。跟现有运行态口径一致(Redis 当真相源) |
| **并发发布** | 同一项目同时多个发布会话?建议:同项目同时只允许一个 running 发布,其余排队/拒绝 |

### 5.3 待用户拍板的设计点
1. **publish-MCP 是内置 MCP(服务端进程内)还是独立进程?** 建议:内置,复用 cdp-bridge/mail 口径,进程内调 node-server 最短路径。
2. **访问入口**:管理节点各占宿主端口/域名,还是 model-api 统一按 project_id 反代?(倾向后者)
3. **provider**:发布会话限定 claude/gemini/opencode(排除 codex,待验证),还是支持 codex?
4. **同项目并发发布**:允许还是互斥?

---

## 6. 落地顺序(建议)

1. **spike 验证最大风险**:publish-MCP 工具调进程内 node-server `host_exec`,在管理节点跑通 `docker ps`。这一步通了,后面都是套壳。
2. 数据库迁移:editor_sessions 加 type+绑定;mcp_users 加 publish_* 列;建 publish_releases 表。
3. principal 扩展:USAGE_TYPES 加 'publish';`create_owned_mcp_principal` 扩参;`get_publish_principal_scope`。
4. publish-MCP 服务端:`/mcp/publish/sse` + token 校验 + 工具(git_pull/docker/nginx/static/probe/record/stop/rollback)。
5. 创建 publish_session 流程:routes_publish.py + 校验 + 建 principal+grant+下发。
6. 项目「发布情况」查询端点 + 前端 tab + 停止/回滚按钮。
7. 气泡聊天复用接入(session_id 路由到现有 agent 聊天组件)。

---

## 7. 关键文件索引

| 用途 | 文件:行 |
|---|---|
| 会话表 | [db.py:411](db.py#L411) editor_sessions |
| mcp_users 表 | [db.py:1157](db.py#L1157) + [db.py:1279](db.py#L1279) 升级列 |
| principal store | [mcp_plugin_store.py:919](mcp_plugin_store.py#L919) get / [953](mcp_plugin_store.py#L953) create / [1425](mcp_plugin_store.py#L1425) list grants / [1451](mcp_plugin_store.py#MCP Plugin Store) replace grants |
| USAGE_TYPES | [mcp_plugin_store.py:883](mcp_plugin_store.py#L883) |
| principal 路由 | [monkeycode_compat/routes_mcp_principals.py](monkeycode_compat/routes_mcp_principals.py) |
| 下发会话 MCP | [monkeycode_compat/node_client/client.py:325](monkeycode_compat/node_client/client.py#L325) apply_node_session_mcps |
| 节点 applyMCPs | [nodes/execution/session.go:550](nodes/execution/session.go#L550) |
| editor MCP 配置 | [nodes/execution/editorconfig.go](nodes/execution/editorconfig.go) |
| 会话进程形态 | [nodes/execution/stream_executor.go](nodes/execution/stream_executor.go) / [executor.go:19](nodes/execution/executor.go#L19) / [session.go:309](nodes/execution/session.go#L309) |
| host_exec 派发 | [node_server/service.py:1533](node_server/service.py#L1533) |
| node_client host_exec | [monkeycode_compat/node_client/client.py:423](monkeycode_compat/node_client/client.py#L423) |
| issue-workflow(旁路,勿抄) | [monkeycode_compat/task_service.py:648](monkeycode_compat/task_service.py#L648) |
| token 签发 | [builtin_tool_store.py:231](builtin_tool_store.py#L231) issue_token |
| 项目路由 | [monkeycode_compat/routes_project.py](monkeycode_compat/routes_project.py) |
| node_server_enabled | [monkeycode_compat/config.py:169](monkeycode_compat/config.py#L169) |

---

## 8. 一句话总结

**改动点只有一个:会话创建时,按 session_type 关联对应的 MCP。** publish_session 走现有 principal+grants 机制建一个带 (project,node,git_ref) 绑定的 principal,grant 给 publish-MCP;agent 通过现有 principal 关联拿到 publish-MCP;publish-MCP 从 principal 反查绑定,调进程内 node-server host_exec 派发到管理节点。不碰 agent,不碰节点协议,不另开旁路。
