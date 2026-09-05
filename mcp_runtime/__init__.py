"""MCP Runtime — 统一以 SSE 对外暴露 MCP 能力。

**已合并进主程序**：SSE 网关（sse_gateway）+ 管理 API（admin_api）的 router 直接
挂在主 app 上（见 main.py），插件在主 lifespan 里 restore_active_plugins() 注册进
registry 内存单例。原先独立子进程（supervisor + 127.0.0.1:8003 回环）已彻底退役，
不再保留独立进程入口。三类上游能力：

- custom:  来自 mcp_plugin_versions 的 Python 源码，进程内 exec 加载。
- sse:     远端 SSE MCP，转发。
- stdio:   转发给独立执行器（stdio_executor），不在主程序内拉起子进程。

所有能力热注册：管理端写库 + 调 activate 即生效，无需重启。
安全闸门：custom 类版本必须 security_status='passed'（AI 代码审查通过）才能激活。

/mcp/runtime/* 与 /mcp/services/{id}/* 短路径直接调 admin_api 的 *_core 函数，
不再经 HTTP 自代理；/mcp/admin/* 路由仍保留，供直接调用 runtime 管理 API 的客户端。
"""
