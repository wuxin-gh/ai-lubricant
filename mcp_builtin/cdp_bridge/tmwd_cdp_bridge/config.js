const TID = '__cdp_bridge_mcp_request_9b8d7f6a';
// Bridge 连接地址（单一输入框，可填 ws:// 或 wss:// 完整地址）。
// MCP Runtime 已合并进主程序，桥接 WebSocket 挂在主服务端口（默认 8001）上，
// 路径 /mcp/cdp-bridge/session。以接入教程里显示的地址为准，此处仅为占位默认。
const DEFAULT_BRIDGE_URL = 'ws://127.0.0.1:8001/mcp/cdp-bridge/session';
// 客户端 token 必须由 Ai Lubricant 管理端生成。
const DEFAULT_CLIENT_TOKEN = '';
