// background.js - Ai Lubricant 浏览器助手
try { importScripts('config.js'); } catch (_) {}

// 聊天面板按 tabId 存的会话恢复状态（cdpPanel:<tabId>）要给 content script 读写，
// session storage 默认只允许扩展页面访问，这里放开到 untrusted contexts。
try { chrome.storage.session.setAccessLevel({ accessLevel: 'TRUSTED_AND_UNTRUSTED_CONTEXTS' }); } catch (_) {}
// 标签页关闭时清掉对应的面板恢复键，避免 tabId 复用时误恢复。
try {
  chrome.tabs.onRemoved.addListener((removedTabId) => {
    chrome.storage.session.remove(`cdpPanel:${removedTabId}`).catch(() => {});
  });
} catch (_) {}

const DEFAULT_BRIDGE_CONFIG = {
  bridgeUrl: typeof DEFAULT_BRIDGE_URL !== 'undefined' ? DEFAULT_BRIDGE_URL : 'ws://127.0.0.1:8001/mcp/cdp-bridge/session',
  clientToken: typeof DEFAULT_CLIENT_TOKEN !== 'undefined' ? DEFAULT_CLIENT_TOKEN : '',
  bridgeEnabled: true,
};
// already_connected 不是永久错误：另一条连接可能是半开的死连接，服务端空闲读
// 超时（~60s）会释放会话槽。故这里不把它列为永久错误——客户端继续探测重连，
// 待服务端放开后自动接管，而不是永久卡死在"已在其他浏览器连接"。
const PERMANENT_AUTH_ERRORS = new Set([
  'invalid', 'invalid_token', 'disabled', 'permission_denied',
  'protocol_required', 'revoked',
]);
let bridgeConfig = { ...DEFAULT_BRIDGE_CONFIG };
let bridgeWsUrl = normalizeBridgeUrl(bridgeConfig.bridgeUrl);
let connecting = false;
let authenticated = false;
let authError = null;
let reconnectBlocked = false;
let socketEpoch = 0;
let reprovisionRequired = false;
const CONNECTION_TIMEOUT_MS = 10000;
const AUTH_RESPONSE_TIMEOUT_MS = 10000;
// pong 判死窗口（毫秒）：keepalive 每 ~24s 发一次 ping，服务端活着就会立刻回
// pong（或任何帧）。半开连接上 ws.send 永远本地成功、onclose 永不触发，唯一可靠
// 的死活信号就是「发了 ping 却迟迟收不到任何回帧」。60s 与服务端空闲读超时
// （_EXT_IDLE_TIMEOUT）同量级，正常心跳下每 ~24s 至少有一帧，不会误杀。
const PONG_TIMEOUT_MS = 60000;
let lastFrameAt = 0;

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object || {}, key);
}

function usableClientToken(token) {
  return String(token || '').trim();
}

async function loadBridgeConfig() {
  const stored = await chrome.storage.local.get([...Object.keys(DEFAULT_BRIDGE_CONFIG), 'bridgeToken']);
  const hadLegacyToken = hasOwn(stored, 'bridgeToken');
  const hasClientToken = Boolean(usableClientToken(stored.clientToken));
  if (hadLegacyToken) {
    // A legacy bridge token is an MCP-user credential, not a CDP client token.
    // Never promote it across the identity boundary; force explicit reprovisioning.
    await chrome.storage.local.remove('bridgeToken');
    reprovisionRequired = !hasClientToken;
  } else if (hasClientToken) {
    reprovisionRequired = false;
  }
  bridgeConfig = {
    bridgeUrl: normalizeBridgeUrl(stored.bridgeUrl),
    clientToken: usableClientToken(stored.clientToken),
    bridgeEnabled: stored.bridgeEnabled !== false,
  };
  bridgeWsUrl = bridgeConfig.bridgeUrl;
  return bridgeConfig;
}

function normalizeBridgeUrl(url) {
  let u = String(url || DEFAULT_BRIDGE_CONFIG.bridgeUrl).trim();
  if (!u) return DEFAULT_BRIDGE_CONFIG.bridgeUrl;
  if (!/^wss?:\/\//i.test(u)) u = 'ws://' + u;
  return u.replace(/\/+$/, '');
}

function isLocalBridge(url) {
  return /^(ws|wss):\/\/(127\.0\.0\.1|localhost)([:\/]|$)/i.test(String(url || ''));
}

// 把当前桥接状态推送给所有可注入的标签页（content.js 监听后更新徽标）。
// 关键动机：content.js 原先只在页面加载/点击徽标时各查一次状态，SW 后续 auth_ok
// 连上后徽标不会自动更新——用户得手动刷新页面才看到「已连接」。改成 SW 在状态
// 变化点主动广播，徽标就能实时跟上。popup 自身仍走 750ms 轮询，不受影响。
async function broadcastBridgeStatus() {
  const status = getBridgeStatus();
  const tabs = await chrome.tabs.query({}).catch(() => []);
  const payload = { cmd: 'bridge_status_push', status };
  for (const t of tabs) {
    if (!isScriptable(t.url)) continue;
    chrome.tabs.sendMessage(t.id, payload).catch(() => { /* tab 不可注入/已关，忽略 */ });
  }
}

function getBridgeStatus() {
  const socketOpen = Boolean(ws && ws.readyState === WebSocket.OPEN);
  const connected = socketOpen && authenticated;
  let state = 'disconnected';
  if (!bridgeConfig.bridgeEnabled) state = 'disabled';
  else if (!bridgeConfig.bridgeUrl || !bridgeConfig.clientToken) state = 'unconfigured';
  else if (connected) state = 'connected';
  else if (authError) state = 'auth_error';
  else if (connecting || socketOpen || (ws && ws.readyState === WebSocket.CONNECTING)) state = 'authenticating';
  return {
    connected,
    authenticated,
    enabled: bridgeConfig.bridgeEnabled,
    state,
    error: authError ? { ...authError } : null,
    reprovisionRequired,
  };
}

chrome.runtime.onInstalled.addListener(() => {
  console.log('Ai Lubricant 浏览器助手 installed');
  // Strip CSP headers to allow eval/inline scripts
  chrome.declarativeNetRequest.updateDynamicRules({
    removeRuleIds: [9999],
    addRules: [{
      id: 9999, priority: 1,
      action: { type: 'modifyHeaders', responseHeaders: [
        { header: 'content-security-policy', operation: 'remove' },
        { header: 'content-security-policy-report-only', operation: 'remove' }
      ]},
      condition: { urlFilter: '*', resourceTypes: ['main_frame', 'sub_frame'] }
    }]
  });
});

async function handleExtMessage(msg, sender) {
  // 插件页面交互事件
  if (msg.cmd === 'bridge_config_get') return { ok: true, data: await loadBridgeConfig() };
  if (msg.cmd === 'bridge_status_get') return { ok: true, data: getBridgeStatus() };
  // content script 查询自己所在标签页 id（会话恢复按 tabId 键控）。
  if (msg.cmd === 'bridge_tab_id') {
    return { ok: true, data: { tabId: sender && sender.tab ? sender.tab.id : null } };
  }
  if (msg.cmd === 'bridge_config_set') {
    const input = msg.config || {};
    const nextConfig = {
      bridgeUrl: normalizeBridgeUrl(input.bridgeUrl),
      clientToken: usableClientToken(input.clientToken),
      bridgeEnabled: input.bridgeEnabled !== false,
    };
    await chrome.storage.local.set(nextConfig);
    await chrome.storage.local.remove('bridgeToken');
    reprovisionRequired = !nextConfig.clientToken;
    await loadBridgeConfig();
    authError = null;
    reconnectBlocked = false;
    authenticated = false;
    const oldSocket = ws;
    ws = null;
    socketEpoch++;
    if (oldSocket) oldSocket.close();
    chrome.alarms.clear('tmwd-ws-probe');
    chrome.alarms.clear('tmwd-ws-keepalive');
    if (bridgeConfig.bridgeEnabled && bridgeConfig.clientToken) connectWS();
    return { ok: true, data: bridgeConfig };
  }

  // 业务事件

  if (msg.cmd === 'cdp') return await handleCDP(msg, sender);
  if (msg.cmd === 'batch') return await handleBatch(msg, sender);
  if (msg.cmd === 'net_start') return await handleNetStart(msg, sender);
  if (msg.cmd === 'net_get') return handleNetGet(msg, sender);
  if (msg.cmd === 'net_stop') return await handleNetStop(msg, sender);
  if (msg.cmd === 'tabs') {
    try {
      if (msg.method === 'switch') {
        const tab = await chrome.tabs.update(msg.tabId, { active: true });
        await chrome.windows.update(tab.windowId, { focused: true });
        return { ok: true };
      } else if (msg.method === 'create') {
        const url = msg.url || 'about:blank';
        if (msg.newWindow) {
          const win = await chrome.windows.create({ url, focused: true });
          const tab = (win.tabs || [])[0] || {};
          return { ok: true, data: { id: tab.id, url: tab.url, title: tab.title, windowId: win.id } };
        }
        const tab = await chrome.tabs.create({ url, active: msg.active !== false });
        return { ok: true, data: { id: tab.id, url: tab.url, title: tab.title, windowId: tab.windowId } };
      } else {
        const tabs = (await chrome.tabs.query({})).filter(t => isScriptable(t.url));
        const data = tabs.map(t => ({ id: t.id, url: t.url, title: t.title, active: t.active, windowId: t.windowId }));
        return { ok: true, data };
      }
    } catch (e) { return { ok: false, error: e.message }; }
  }
  if (msg.cmd === 'management') {
    try {
      if (msg.method === 'list') {
        const all = await chrome.management.getAll();
        return { ok: true, data: all.map(e => ({ id: e.id, name: e.name, enabled: e.enabled, type: e.type, version: e.version })) };
      }
      if (msg.method === 'reload') {
        chrome.alarms.create('tmwd-self-reload', { when: Date.now() + 200 });
        return { ok: true };
      }
      if (msg.method === 'disable') {
        await chrome.management.setEnabled(msg.extId, false);
        return { ok: true };
      }
      if (msg.method === 'enable') {
        await chrome.management.setEnabled(msg.extId, true);
        return { ok: true };
      }
      return { ok: false, error: 'Unknown method: ' + msg.method };
    } catch (e) { return { ok: false, error: e.message }; }
  }
  if (msg.cmd === 'contentSettings') {
    try {
      const type = msg.type || 'automaticDownloads';
      const setting = msg.setting || 'allow';
      const pattern = msg.pattern || '<all_urls>';
      await chrome.contentSettings[type].set({
        primaryPattern: pattern,
        setting: setting
      });
      return { ok: true };
    } catch (e) { return { ok: false, error: e.message }; }
  }
  // 网页侧对话面板：经已鉴权的 cdp WS 通道代理到服务端 agent。
  if (msg.cmd === 'chat_list_agents' || msg.cmd === 'chat_send' || msg.cmd === 'chat_retry' || msg.cmd === 'chat_abort'
      || msg.cmd === 'chat_list_conversations' || msg.cmd === 'chat_new_conversation'
      || msg.cmd === 'chat_load_conversation' || msg.cmd === 'chat_list_models'
      || msg.cmd === 'chat_resolve_approval') {
    return await sendChatFrame(msg, sender);
  }

  return { ok: false, error: 'Unknown cmd: ' + msg.cmd };
}

// --- 网页侧 Agent 对话面板（经 WS 通道代理） ---
// reqId 关联请求与回帧；按 reqId → tabId 记录待投递目标，收到回帧时转发给对应标签页。
const _chatReqs = new Map(); // reqId -> tabId

function sendChatFrame(msg, sender) {
  if (!ws || ws.readyState !== WebSocket.OPEN || !authenticated) {
    return { ok: false, error: 'bridge websocket is not authenticated' };
  }
  const reqId = String(msg.reqId || ('chat_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8)));
  const tabId = msg.tabId != null ? msg.tabId : (sender.tab && sender.tab.id);
  const frame = {
    type: msg.cmd,
    reqId,
    tabId,
    url: msg.url || (sender.tab && sender.tab.url) || '',
    title: msg.title || (sender.tab && sender.tab.title) || '',
  };
  if (msg.cmd === 'chat_send') {
    frame.agentId = msg.agentId;
    frame.content = msg.content;
    frame.conversationId = msg.conversationId || '';
    frame.clientName = msg.clientName || '';
    if (msg.maxTurns) frame.maxTurns = msg.maxTurns;
    // 对话级覆盖：模型 / 思考等级 / 执行模式（goal 另带目标与预算）。
    if (msg.model) frame.model = msg.model;
    if (msg.reasoningEffort !== undefined) frame.reasoningEffort = msg.reasoningEffort;
    if (msg.mode) frame.mode = msg.mode;
    if (msg.mode === 'goal') {
      frame.goalObjective = msg.goalObjective || '';
      frame.goalBudgetMinutes = msg.goalBudgetMinutes || 15;
    }
  }
  if (msg.cmd === 'chat_retry') {
    // 重试一条失败的 assistant 消息：不带 content（服务端取上一条 user 消息）。
    frame.conversationId = msg.conversationId || '';
    frame.messageId = msg.messageId;
  }
  if (msg.cmd === 'chat_list_models') {
    frame.agentId = msg.agentId;
  }
  if (msg.cmd === 'chat_abort') {
    frame.conversationId = msg.conversationId || '';
  }
  if (msg.cmd === 'chat_resolve_approval') {
    frame.conversationId = msg.conversationId || '';
    frame.confirmationId = msg.confirmationId || '';
    frame.result = msg.result || '';
    frame.commandHash = msg.commandHash || '';
  }
  if (msg.cmd === 'chat_new_conversation') {
    frame.agentId = msg.agentId;
    frame.title = msg.title || '';
  }
  if (msg.cmd === 'chat_load_conversation') {
    frame.conversationId = msg.conversationId || '';
  }
  if (msg.cmd === 'chat_list_agents' || msg.cmd === 'chat_list_conversations') {
    // 仅取列表，无需额外字段。
  }
  _chatReqs.set(reqId, tabId);
  // 清理过期 reqId（防止 SW 长跑后内存累积）。
  if (_chatReqs.size > 200) {
    const firstKey = _chatReqs.keys().next().value;
    _chatReqs.delete(firstKey);
  }
  ws.send(JSON.stringify(frame));
  return { ok: true, reqId };
}

function forwardChatFrameToTab(data) {
  const tabId = _chatReqs.get(String(data.reqId));
  if (tabId == null) return;
  chrome.tabs.sendMessage(Number(tabId), { cmd: 'chat_frame', data }).catch(() => {
    // 标签页可能已关闭/不可注入，清理即可。
  });
  // 一次性应答帧（会话列表 / 新建 / 载入）与流终止帧都要清理映射，避免累积。
  const ONE_SHOT = ['chat_agents', 'chat_models', 'chat_conversations', 'chat_conversation_created', 'chat_conversation_loaded', 'chat_approval_resolved'];
  if (data.type === 'chat_error' || data.type === 'chat_aborted' ||
      ONE_SHOT.includes(data.type) ||
      (data.type === 'chat_event' && data.event && ['done', 'error'].includes(data.event.type))) {
    _chatReqs.delete(String(data.reqId));
  }
}

// 断连时把挂起的网页对话请求统一失败掉：回帧永远不会来，不主动失败的话面板的
// sendChat promise 永不 resolve → 永远转圈、state.sending 卡死、会话切换被
// sending 守卫挡住。content.js 收到 chat_error 会 finalize 成可重试的错误行。
function failPendingChatRequests(message) {
  if (_chatReqs.size === 0) return;
  for (const [reqId, tabId] of _chatReqs) {
    if (tabId == null) continue;
    chrome.tabs.sendMessage(Number(tabId), {
      cmd: 'chat_frame',
      data: { type: 'chat_error', reqId, message },
    }).catch(() => { /* 标签页可能已关，忽略 */ });
  }
  _chatReqs.clear();
}

// 断连收尾：onclose 与 pong 判死（keepalive 强杀半开连接）共用。置 ws=null 后
// 晚到的 onclose 会被 isCurrent() 幂等挡掉；返回被关闭的 socket 供调用方 close。
function markDisconnected() {
  const socket = ws;
  ws = null;
  authenticated = false;
  connecting = false;
  lastFrameAt = 0;
  chrome.alarms.clear('tmwd-ws-keepalive');
  failPendingChatRequests('桥接连接已断开，请稍后重试');
  void loadBridgeConfig().then(() => {
    void broadcastBridgeStatus(); // 断开了：推给所有 tab，徽标变灰/认证中
    if (!bridgeConfig.bridgeEnabled || !bridgeConfig.clientToken || reconnectBlocked) return;
    // 撞过 already_connected 时走长退避（旧槽位还没释放，5s 撞一次纯浪费）；
    // 正常断开仍用 5s probe（服务端存活即重连，无需等）。
    if (alreadyConnectedBackoff > 0) scheduleProbe(ALREADY_CONNECTED_BACKOFF_MS[alreadyConnectedBackoff] / 60000);
    else scheduleProbe();
  });
  return socket;
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  handleExtMessage(msg, sender).then(sendResponse);
  return true;
});

const netCaptures = new Map();
const NET_BODY_LIMIT = 1024 * 1024;

function getCapture(tabId) {
  return netCaptures.get(String(tabId));
}

async function attachDebugger(tabId) {
  if (!tabId) throw new Error('no tabId');
  const cap = getCapture(tabId);
  if (cap && cap.attached) return { reused: true };
  await chrome.debugger.attach({ tabId }, '1.3');
  return { reused: false };
}

async function detachDebugger(tabId, attachState) {
  const cap = getCapture(tabId);
  if (cap && cap.attached) return;
  if (!attachState || attachState.reused) return;
  try { await chrome.debugger.detach({ tabId }); } catch (_) {}
}

function publicRequest(req) {
  const copy = { ...req };
  delete copy._finished;
  return copy;
}

function captureSummary(cap) {
  const requests = Array.from(cap.requests.values()).map(publicRequest);
  return {
    capturing: cap.capturing,
    tabId: cap.tabId,
    startedAt: cap.startedAt,
    stoppedAt: cap.stoppedAt || null,
    total: requests.length,
    requests,
  };
}

function notifyCaptureBanner(tabId, action, count) {
  chrome.tabs.sendMessage(tabId, { cmd: 'network_capture_banner', action, count }).catch(() => {});
}

function updateCaptureBanner(tabId, force) {
  const cap = getCapture(tabId);
  if (!cap) return;
  const now = Date.now();
  if (!force && now - cap.lastBannerUpdate < 400) return;
  cap.lastBannerUpdate = now;
  notifyCaptureBanner(Number(tabId), cap.capturing ? 'update' : 'hide', cap.requests.size);
}

function shouldKeepRequest(cap, url) {
  if (!cap.urlPattern) return true;
  return String(url || '').includes(cap.urlPattern);
}

function ensureRequest(cap, requestId) {
  let req = cap.requests.get(requestId);
  if (!req) {
    req = { requestId, events: [] };
    cap.requests.set(requestId, req);
  }
  return req;
}

async function readResponseBody(tabId, cap, requestId) {
  try {
    const body = await chrome.debugger.sendCommand({ tabId }, 'Network.getResponseBody', { requestId });
    const req = cap.requests.get(requestId);
    if (!req) return;
    const text = String(body.body || '');
    req.responseBody = text.length > NET_BODY_LIMIT ? text.slice(0, NET_BODY_LIMIT) : text;
    req.responseBodyBase64Encoded = !!body.base64Encoded;
    req.responseBodyTruncated = text.length > NET_BODY_LIMIT;
  } catch (e) {
    const req = cap.requests.get(requestId);
    if (req) req.responseBodyError = e.message || String(e);
  }
}

function handleDebuggerEvent(source, method, params) {
  const tabId = source.tabId;
  if (!tabId) return;
  const cap = getCapture(tabId);
  if (!cap || !cap.capturing) return;
  const requestId = params && params.requestId;
  if (!requestId) return;

  if (method === 'Network.requestWillBeSent') {
    if (!shouldKeepRequest(cap, params.request && params.request.url)) return;
    const req = ensureRequest(cap, requestId);
    req.events.push(method);
    req.url = params.request.url;
    req.method = params.request.method;
    req.requestHeaders = params.request.headers || {};
    req.type = params.type || req.type;
    req.initiator = params.initiator || null;
    req.wallTime = params.wallTime || null;
    req.startTimestamp = params.timestamp || null;
  } else if (method === 'Network.responseReceived') {
    const req = cap.requests.get(requestId);
    if (!req) return;
    req.events.push(method);
    req.type = params.type || req.type;
    req.responseUrl = params.response && params.response.url;
    req.status = params.response && params.response.status;
    req.statusText = params.response && params.response.statusText;
    req.mimeType = params.response && params.response.mimeType;
    req.protocol = params.response && params.response.protocol;
    req.remoteIPAddress = params.response && params.response.remoteIPAddress;
    req.remotePort = params.response && params.response.remotePort;
    req.responseHeaders = params.response && params.response.headers || {};
    req.responseTimestamp = params.timestamp || null;
  } else if (method === 'Network.loadingFinished') {
    const req = cap.requests.get(requestId);
    if (!req) return;
    req.events.push(method);
    req.endTimestamp = params.timestamp || null;
    req.encodedDataLength = params.encodedDataLength || 0;
    req.durationMs = req.startTimestamp && req.endTimestamp ? Math.round((req.endTimestamp - req.startTimestamp) * 1000) : null;
    req.finished = true;
    if (!req._finished) {
      req._finished = true;
      readResponseBody(Number(tabId), cap, requestId).finally(() => updateCaptureBanner(tabId, true));
    }
  } else if (method === 'Network.loadingFailed') {
    const req = cap.requests.get(requestId);
    if (!req) return;
    req.events.push(method);
    req.endTimestamp = params.timestamp || null;
    req.durationMs = req.startTimestamp && req.endTimestamp ? Math.round((req.endTimestamp - req.startTimestamp) * 1000) : null;
    req.failed = true;
    req.errorText = params.errorText || '';
    req.canceled = !!params.canceled;
  } else {
    return;
  }
  updateCaptureBanner(tabId, false);
}

chrome.debugger.onEvent.addListener(handleDebuggerEvent);
chrome.debugger.onDetach.addListener((source) => {
  const tabId = source && source.tabId;
  if (!tabId) return;
  const cap = getCapture(tabId);
  if (!cap) return;
  cap.capturing = false;
  cap.attached = false;
  cap.stoppedAt = Date.now();
  notifyCaptureBanner(tabId, 'hide', cap.requests.size);
});

async function handleNetStart(msg, sender) {
  const tabId = msg.tabId || sender.tab?.id;
  if (!tabId) return { ok: false, error: 'no tabId' };
  const key = String(tabId);
  const existing = getCapture(tabId);
  if (existing && existing.capturing) {
    notifyCaptureBanner(Number(tabId), 'show', existing.requests.size);
    return { ok: true, data: captureSummary(existing) };
  }
  const cap = {
    tabId: Number(tabId),
    capturing: true,
    attached: false,
    startedAt: Date.now(),
    stoppedAt: null,
    requests: new Map(),
    urlPattern: String(msg.urlPattern || ''),
    lastBannerUpdate: 0,
  };
  netCaptures.set(key, cap);
  try {
    await chrome.debugger.attach({ tabId: Number(tabId) }, '1.3');
    cap.attached = true;
    await chrome.debugger.sendCommand({ tabId: Number(tabId) }, 'Network.enable', {
      maxTotalBufferSize: 100000000,
      maxResourceBufferSize: 10000000,
    });
    notifyCaptureBanner(Number(tabId), 'show', 0);
    return { ok: true, data: captureSummary(cap) };
  } catch (e) {
    netCaptures.delete(key);
    try { await chrome.debugger.detach({ tabId: Number(tabId) }); } catch (_) {}
    notifyCaptureBanner(Number(tabId), 'hide', 0);
    return { ok: false, error: e.message || String(e) };
  }
}

function handleNetGet(msg, sender) {
  const tabId = msg.tabId || sender.tab?.id;
  if (!tabId) return { ok: false, error: 'no tabId' };
  const cap = getCapture(tabId);
  if (!cap) return { ok: true, data: { capturing: false, tabId: Number(tabId), total: 0, requests: [] } };
  return { ok: true, data: captureSummary(cap) };
}

async function handleNetStop(msg, sender) {
  const tabId = msg.tabId || sender.tab?.id;
  if (!tabId) return { ok: false, error: 'no tabId' };
  const cap = getCapture(tabId);
  if (!cap) {
    notifyCaptureBanner(Number(tabId), 'hide', 0);
    return { ok: true, data: { capturing: false, tabId: Number(tabId), total: 0, requests: [] } };
  }
  cap.capturing = false;
  cap.attached = false;
  cap.stoppedAt = Date.now();
  try { await chrome.debugger.sendCommand({ tabId: Number(tabId) }, 'Network.disable'); } catch (_) {}
  try { await chrome.debugger.detach({ tabId: Number(tabId) }); } catch (_) {}
  notifyCaptureBanner(Number(tabId), 'hide', cap.requests.size);
  return { ok: true, data: captureSummary(cap) };
}

async function handleBatch(msg, sender) {
  const R = [];
  let attached = null;
  let attachState = null;
  const resolve$N = (params) => JSON.parse(JSON.stringify(params || {}).replace(/"\$(\d+)\.([^"]+)"/g,
    (_, i, path) => { let v = R[+i]; for (const k of path.split('.')) v = v[k]; return JSON.stringify(v); }));
  try {
    for (const c of msg.commands) {
      if (c.tabId === undefined && msg.tabId !== undefined) c.tabId = msg.tabId;
      if (c.cmd === 'tabs') {
        const tabs = (await chrome.tabs.query({})).filter(t => isScriptable(t.url));
        R.push({ ok: true, data: tabs.map(t => ({ id: t.id, url: t.url, title: t.title, active: t.active, windowId: t.windowId })) });
      } else if (c.cmd === 'cdp') {
        const tabId = c.tabId || msg.tabId || sender.tab?.id;
        if (attached !== tabId) {
          if (attached) { await detachDebugger(attached, attachState); attached = null; attachState = null; }
          attachState = await attachDebugger(tabId);
          attached = tabId;
        }
        R.push(await chrome.debugger.sendCommand({ tabId }, c.method, resolve$N(c.params)));
      } else {
        R.push({ ok: false, error: 'unknown cmd: ' + c.cmd });
      }
    }
    if (attached) await detachDebugger(attached, attachState);
    return { ok: true, results: R };
  } catch (e) {
    if (attached) try { await detachDebugger(attached, attachState); } catch (_) {}
    return { ok: false, error: e.message, results: R };
  }
}

async function handleCDP(msg, sender) {
  const tabId = msg.tabId || sender.tab?.id;
  if (!tabId) return { ok: false, error: 'no tabId' };
  // 截图前隐藏扩展注入的聊天面板/气泡，截完恢复——否则面板会被一起截进画面。
  const hidePanel = msg.method === 'Page.captureScreenshot';
  let panelHidden = false;
  if (hidePanel) {
    panelHidden = await sendTabMessage(tabId, { cmd: 'hide_panel_for_screenshot' }).catch(() => false);
    if (panelHidden) await new Promise((r) => setTimeout(r, 60)); // 等一帧让 display:none 生效
  }
  const cap = getCapture(tabId);
  try {
    if (cap && cap.attached) {
      try {
        const result = await chrome.debugger.sendCommand({ tabId }, msg.method, msg.params || {});
        return { ok: true, data: result };
      } catch (e) { return { ok: false, error: e.message }; }
    }
    let attachState = null;
    try {
      attachState = await attachDebugger(tabId);
      const result = await chrome.debugger.sendCommand({ tabId }, msg.method, msg.params || {});
      await detachDebugger(tabId, attachState);
      return { ok: true, data: result };
    } catch (e) {
      await detachDebugger(tabId, attachState);
      return { ok: false, error: e.message };
    }
  } finally {
    if (panelHidden) sendTabMessage(tabId, { cmd: 'show_panel_after_screenshot' }).catch(() => {});
  }
}

// 给目标 tab 的 content script 发一次性消息；content script 不在（internal 页）
// 或没装面板时返回 false，调用方据此不阻塞截图。
function sendTabMessage(tabId, message) {
  return new Promise((resolve) => {
    try {
      chrome.tabs.sendMessage(tabId, message, (resp) => {
        if (chrome.runtime.lastError) { resolve(false); return; }
        resolve(resp && resp.ok !== false);
      });
    } catch (_) { resolve(false); }
  });
}
// Filter out chrome:// and other internal tabs that can't be scripted
const isScriptable = url => url && /^https?:/.test(url);

// --- Shared page/CDP script builder core ---
function buildExecScript(code, errorHandler) {
  return `(async () => {
    function smartProcessResult(result) {
      if (result === null || result === undefined || typeof result !== 'object') return result;
      try { if (result.window === result && result.document) return '[Window: ' + (result.location?.href || 'about:blank') + ']'; } catch(_){}
      if (typeof jQuery !== 'undefined' && result instanceof jQuery) {
        const elements = []; for (let i = 0; i < result.length; i++) { if (result[i] && result[i].nodeType === 1) elements.push(result[i].outerHTML); } return elements;
      }
      if (result instanceof NodeList || result instanceof HTMLCollection) {
        const elements = []; for (let i = 0; i < result.length; i++) { if (result[i] && result[i].nodeType === 1) elements.push(result[i].outerHTML); } return elements;
      }
      if (result.nodeType === 1) return result.outerHTML;
      if (!Array.isArray(result) && typeof result === 'object' && 'length' in result && typeof result.length === 'number') {
        const firstElement = result[0];
        if (firstElement && firstElement.nodeType === 1) {
          const elements = []; const length = Math.min(result.length, 100);
          for (let i = 0; i < length; i++) { const elem = result[i]; if (elem && elem.nodeType === 1) elements.push(elem.outerHTML); } return elements;
        }
      }
      try { return JSON.parse(JSON.stringify(result, function(key, value) { if (typeof value === 'object' && value !== null) { if (value.nodeType === 1) return value.outerHTML; if (value === window || value === document) return '[Object]'; try { if (value.window === value && value.document) return '[Window]'; } catch(_){} } return value; })); } catch (e) { return '[无法序列化: ' + e.message + ']'; }
    }
    try {
      const jsCode = ${JSON.stringify(code)}.trim();
      const lines = jsCode.split(/\\r?\\n/).filter(l => l.trim());
      const lastLine = lines.length > 0 ? lines[lines.length - 1].trim() : '';
      const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
      let r;
      function _air(c) { const ls = c.split(/\\r?\\n/); let i = ls.length - 1; while (i >= 0 && !ls[i].trim()) i--; if (i < 0) return c; const t = ls[i].trim(); if (/^(return |return;|return$|let |const |var |if |if\\(|for |for\\(|while |while\\(|switch|try |throw |class |function |async |import |export |\\/\\/|})/.test(t)) return c; ls[i] = ls[i].match(/^(\\s*)/)[1] + 'return ' + t; return ls.join('\\n'); }
      if (lastLine.startsWith('return')) {
        r = await (new AsyncFunction(jsCode))();
      } else {
        try { r = eval(jsCode); if (r instanceof Promise) r = await r; } catch (e) {
          if (e instanceof SyntaxError && (/return/i.test(e.message) || /await/i.test(e.message))) { r = await (new AsyncFunction(_air(jsCode)))(); } else throw e;
        }
      }
      return { ok: true, data: smartProcessResult(r) };
    } catch (e) {
      ${errorHandler}
    }
  })()`;
}

function buildPageScript(code) {
  return buildExecScript(code, `
      const errMsg = e.message || String(e);
      return { ok: false, error: { name: e.name || 'Error', message: errMsg, stack: e.stack || '' },
        csp: errMsg.includes('Refused to evaluate') || errMsg.includes('unsafe-eval') || errMsg.includes('Content Security Policy') };
  `);
}

function buildCdpScript(code) {
  return buildExecScript(code, `
      return { ok: false, error: { name: e.name || 'Error', message: e.message || String(e), stack: e.stack || '' } };
  `);
}

// --- WebSocket Client for TMWebDriver ---
let ws = null;

// already_connected 退避表（毫秒）。旧槽位靠服务端空闲读超时释放，5s 撞一次无效
// 且长时间把徽标停在 auth_error。逐步拉长间隔给服务端时间，最多 60s（与空闲
// 超时同量级，撞到释放即接管）。auth_ok 成功后 alreadyConnectedBackoff 清零。
const ALREADY_CONNECTED_BACKOFF_MS = [5000, 15000, 30000, 60000];
let alreadyConnectedBackoff = 0;

function scheduleProbe(delayMin) {
  // Use chrome.alarms to survive MV3 service worker suspension
  const delay = delayMin != null ? delayMin : 0.083; // 默认 ~5s
  chrome.alarms.create('tmwd-ws-probe', { delayInMinutes: delay });
}

function scheduleKeepalive() {
  // Keep SW alive while WS is connected (~25s, under 30s SW timeout)
  chrome.alarms.create('tmwd-ws-keepalive', { delayInMinutes: 0.4 }); // ~24s
}

async function isServerAlive() {
  try {
    const config = await loadBridgeConfig();
    if (!isLocalBridge(config.bridgeUrl)) return true;
    // 本地探测：探主服务的 HTTP 健康端点（同 host:port）。
    // 注意：/mcp/cdp-bridge/session 是 WebSocket-only 路由，对它发 HTTP GET 会得到
    // 404（见服务端日志噪音），因此探活必须打一个真实返回 200 的 HTTP 路径。
    // /mcp/health 无鉴权、纯内存、稳定 200，且正是本插件要连接的 MCP Runtime 服务。
    const healthUrl = config.bridgeUrl
      .replace(/^ws:\/\//i, 'http://')
      .replace(/^wss:\/\//i, 'https://')
      .replace(/\/+$/, '')
      .replace(/\/mcp\/cdp-bridge\/session$/, '/mcp/health');
    const ctrl = new AbortController();
    setTimeout(() => ctrl.abort(), 2000);
    const resp = await fetch(healthUrl, { signal: ctrl.signal });
    return resp.ok; // 2xx → MCP Runtime alive；否则视为不可用
  } catch (e) {
    return false; // Network error (connection refused) or timeout → server not alive
  }
}

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (alarm.name === 'tmwd-self-reload') {
    chrome.runtime.reload();
    return;
  }
  if (alarm.name === 'tmwd-ws-keepalive') {
    // Keepalive: ping to keep SW alive + detect dead connections
    const config = await loadBridgeConfig();
    if (!config.bridgeEnabled) {
      // 开关已关：停止保活，连接应已断开。
      return;
    }
    if (ws && ws.readyState === WebSocket.OPEN && authenticated) {
      if (lastFrameAt && Date.now() - lastFrameAt > PONG_TIMEOUT_MS) {
        // 半开死连接：ping 每次都本地发送成功，但 60s 没收到任何回帧（正常心跳
        // 下服务端每个 ping 都立刻回 pong）。主动按断连收尾——广播状态、失败
        // 挂起的对话请求、排 probe 自动重连；close 只关本地句柄，onclose 若晚到
        // 会被 isCurrent() 幂等挡掉。
        console.error('[TMWD-WS] No frame within', PONG_TIMEOUT_MS, 'ms; treating socket as dead');
        const dead = markDisconnected();
        try { dead.close(); } catch (_) {}
        return;
      }
      try { ws.send(JSON.stringify({ type: 'ping' })); } catch (_) {}
      scheduleKeepalive();
    } else {
      // Connection lost, switch to probe mode
      ws = null;
      scheduleProbe();
    }
  }
  if (alarm.name === 'tmwd-ws-probe') {
    const config = await loadBridgeConfig();
    if (!config.bridgeEnabled || !config.clientToken || reconnectBlocked) return;
    if (ws && ws.readyState <= 1) return; // Already connected/connecting
    if (await isServerAlive()) {
      console.log('[TMWD-WS] Server detected, connecting...');
      connectWS();
    } else {
      scheduleProbe(); // Server not up, keep probing
    }
  }
});

function sendSocketFrame(socket, frame) {
  if (socket !== ws || !authenticated || socket.readyState !== WebSocket.OPEN) return false;
  try {
    socket.send(JSON.stringify(frame));
    return true;
  } catch (_) {
    return false;
  }
}

async function handleWsExec(data, socket) {
  const tabId = data.tabId;
  console.log('[TMWD-WS] Exec request', data.id, 'on tab', tabId);
  if (!sendSocketFrame(socket, { type: 'ack', id: data.id })) return;
  if (!tabId) {
    sendSocketFrame(socket, { type: 'error', id: data.id, error: 'No tabId provided' });
    return;
  }
  // Use onCreated listener to reliably capture new tabs (avoids race condition with query-diff)
  const newTabIds = new Set();
  const onCreated = (tab) => { newTabIds.add(tab.id); };
  chrome.tabs.onCreated.addListener(onCreated);
  try {
    let res;
    try {
      const result = await chrome.scripting.executeScript({
        target: { tabId },
        world: 'MAIN',
        func: async (s) => await eval(s),
        args: [buildPageScript(data.code)]
      });
      res = result[0]?.result;
      if (res === null || res === undefined) {
        console.log('[TMWD-WS] executeScript returned null/undefined, treating as CSP issue');
        res = { ok: false, error: { name: 'Error', message: 'executeScript returned null (possible CSP or context issue)', stack: '' }, csp: true };
      }
    } catch (e) {
      console.log('[TMWD-WS] scripting.executeScript failed:', e.message);
      res = { ok: false, error: { name: e.name || 'Error', message: e.message || String(e), stack: e.stack || '' }, csp: true };
    }
    // CDP fallback for CSP-restricted pages
    if (res && !res.ok && res.csp) {
      console.log('[TMWD-WS] CDP fallback for tab', tabId);
      const wrappedCode = buildCdpScript(data.code);
      try {
        await chrome.debugger.attach({ tabId }, '1.3');
        const cdpRes = await chrome.debugger.sendCommand({ tabId }, 'Runtime.evaluate', {
          expression: wrappedCode, awaitPromise: true, returnByValue: true
        });
        await chrome.debugger.detach({ tabId });
        if (cdpRes.exceptionDetails) {
          const desc = cdpRes.exceptionDetails.exception?.description || 'CDP Error';
          res = { ok: false, error: { name: 'Error', message: desc, stack: desc } };
        } else {
          res = cdpRes.result.value;
        }
      } catch (cdpErr) {
        try { await chrome.debugger.detach({ tabId }); } catch (_) {}
        res = { ok: false, error: { name: 'Error', message: 'CDP fallback failed: ' + cdpErr.message, stack: '' } };
      }
    }
    // Grace period for async tab creation (e.g. link click with target=_blank)
    if (newTabIds.size === 0) await new Promise(r => setTimeout(r, 200));
    chrome.tabs.onCreated.removeListener(onCreated);
    // Get full info for captured new tabs
    const newTabs = [];
    for (const id of newTabIds) {
      try { const t = await chrome.tabs.get(id); newTabs.push({id: t.id, url: t.url, title: t.title}); } catch (_) {}
    }
    if (res?.ok) {
      sendSocketFrame(socket, { type: 'result', id: data.id, result: res.data, newTabs });
    } else {
      console.log(res);
      sendSocketFrame(socket, { type: 'error', id: data.id, error: res?.error || 'Unknown error', newTabs });
    }
  } catch (e) {
    sendSocketFrame(socket, { type: 'error', id: data.id, error: { name: e.name || 'Error', message: e.message || String(e), stack: e.stack || '' } });
  } finally {
    chrome.tabs.onCreated.removeListener(onCreated);
  }
}

async function sendReadyFrames(socket) {
  const tabs = (await chrome.tabs.query({})).filter(t => isScriptable(t.url));
  if (socket !== ws || !authenticated || socket.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify({
    type: 'tabs_update',
    tabs: tabs.map(t => ({ id: t.id, url: t.url, title: t.title })),
  }));
  console.log('[TMWD-WS] Authenticated; sent tabs_update with', tabs.length, 'tabs');
  scheduleKeepalive();
}

async function connectWS() {
  if (connecting || (ws && ws.readyState <= 1)) return;
  await loadBridgeConfig();
  if (!bridgeConfig.bridgeEnabled || !bridgeConfig.clientToken || reconnectBlocked) {
    connecting = false;
    return;
  }
  connecting = true;
  authenticated = false;
  authError = null;
  const epoch = ++socketEpoch;
  lastFrameAt = Date.now(); // 连接期宽限：判死计时从建连起算，避免沿用旧值秒判
  console.log('[TMWD-WS] Connecting to', bridgeWsUrl);
  let socket;
  try {
    socket = new WebSocket(bridgeWsUrl);
    ws = socket;
  } catch (e) {
    console.error('[TMWD-WS] Constructor error:', e);
    if (epoch === socketEpoch) {
      ws = null;
      connecting = false;
      void broadcastBridgeStatus(); // 构造失败不触发 onclose，手动推一次
      scheduleProbe();
    }
    return;
  }
  const isCurrent = () => ws === socket && socketEpoch === epoch;
  let connectionTimer = setTimeout(() => {
    if (!isCurrent() || socket.readyState !== WebSocket.CONNECTING) return;
    authError = { code: 'connection_timeout', message: 'bridge websocket did not open within 10 seconds' };
    reconnectBlocked = false;
    console.error('[TMWD-WS] Connection timed out');
    socket.close();
  }, CONNECTION_TIMEOUT_MS);
  let authTimer = null;
  const clearConnectionTimer = () => {
    if (connectionTimer !== null) {
      clearTimeout(connectionTimer);
      connectionTimer = null;
    }
  };
  const clearAuthTimer = () => {
    if (authTimer !== null) {
      clearTimeout(authTimer);
      authTimer = null;
    }
  };
  socket.onopen = () => {
    clearConnectionTimer();
    if (!isCurrent()) { socket.close(); return; }
    connecting = false;
    console.log('[TMWD-WS] Socket open; authenticating');
    socket.send(JSON.stringify({ type: 'auth', protocol: 2, token: bridgeConfig.clientToken }));
    authTimer = setTimeout(() => {
      if (!isCurrent() || authenticated) return;
      authError = { code: 'auth_timeout', message: 'server did not return auth_ok or auth_error within 10 seconds' };
      reconnectBlocked = false;
      console.error('[TMWD-WS] Authentication timed out');
      socket.close();
    }, AUTH_RESPONSE_TIMEOUT_MS);
  };
  socket.onmessage = async (event) => {
    if (!isCurrent()) return;
    // 任何回帧（auth_ok/pong/chat_*/result）都是「连接活着」的证据。
    lastFrameAt = Date.now();
    try {
      const data = JSON.parse(event.data);
      if (data && data.type === 'auth_ok' && data.protocol === 2) {
        clearAuthTimer();
        authenticated = true;
        authError = null;
        reconnectBlocked = false;
        alreadyConnectedBackoff = 0; // 重连成功，清空退避计数
        await sendReadyFrames(socket);
        void broadcastBridgeStatus(); // 连上了：推给所有 tab，徽标变绿
        return;
      }
      if (data && data.type === 'auth_error') {
        clearAuthTimer();
        authenticated = false;
        authError = { code: String(data.code || 'invalid'), message: String(data.message || 'authentication failed') };
        reconnectBlocked = PERMANENT_AUTH_ERRORS.has(authError.code);
        // already_connected 专门退避：旧槽位是半开死连接，服务端要等空闲读超时
        // （~60s）才释放。probe 每 5s 撞一次纯属浪费且把徽标长时间停在 auth_error。
        // 改成 already_connected 用专门的退避表，逐步拉长间隔给服务端时间释放。
        if (authError.code === 'already_connected') {
          alreadyConnectedBackoff = Math.min(alreadyConnectedBackoff + 1, ALREADY_CONNECTED_BACKOFF_MS.length - 1);
        }
        console.error('[TMWD-WS] Authentication failed:', authError.code, authError.message);
        void broadcastBridgeStatus(); // 认证失败：推给所有 tab，徽标变红
        socket.close();
        return;
      }
      if (!authenticated) {
        console.warn('[TMWD-WS] Ignoring frame before auth_ok');
        return;
      }
      if (data && typeof data.type === 'string' && data.type.startsWith('chat_')) {
        forwardChatFrameToTab(data);
        return;
      }
      if (data.id && data.code) {
        let code = data.code;
        if (typeof code === 'string') {
          try { const p = JSON.parse(code); if (p && typeof p === 'object') code = p; } catch (_) {}
        }
        if (typeof code === 'object' && code !== null && code.cmd) {
          if (code.tabId === undefined && data.tabId !== undefined) code.tabId = data.tabId;
          const res = await handleExtMessage(code, {});
          if (isCurrent() && authenticated) socket.send(JSON.stringify({ type: res.ok ? 'result' : 'error', id: data.id, result: res.data ?? res.results ?? res, error: res.error }));
        } else if (typeof code === 'string') {
          await handleWsExec(data, socket);
        } else if (typeof code === 'object' && code !== null) {
          const msg = code.tabId === undefined && data.tabId !== undefined ? { ...code, tabId: data.tabId } : code;
          const res = await handleExtMessage(msg, {});
          if (isCurrent() && authenticated) socket.send(JSON.stringify({ type: res.ok ? 'result' : 'error', id: data.id, result: res.data ?? res.results ?? res, error: res.error }));
        }
      }
    } catch (e) {
      console.error('[TMWD-WS] message parse error', e);
    }
  };
  socket.onclose = async () => {
    clearConnectionTimer();
    clearAuthTimer();
    if (!isCurrent()) return;
    console.log('[TMWD-WS] Disconnected');
    markDisconnected();
  };
  socket.onerror = (e) => {
    if (isCurrent()) console.error('[TMWD-WS] Error:', e);
  };
}

// Initial connect + wake-up hooks
connectWS();
chrome.runtime.onStartup.addListener(() => connectWS());
chrome.runtime.onInstalled.addListener(() => connectWS());

// Sync tab list on changes
async function sendTabsUpdate() {
  const socket = ws;
  if (!socket || socket.readyState !== WebSocket.OPEN || !authenticated) return;
  const tabs = (await chrome.tabs.query({})).filter(t => isScriptable(t.url) && !/streamlit/i.test(t.title));
  if (socket !== ws || !authenticated || socket.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify({
    type: 'tabs_update',
    tabs: tabs.map(t => ({ id: t.id, url: t.url, title: t.title })),
  }));
}
chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (changeInfo.status === 'complete') {
    sendTabsUpdate();
    // 整页刷新后内容脚本会重新注入、横幅消失；若该标签页仍在监听，重新提示展示。
    const cap = getCapture(tabId);
    if (cap && cap.capturing) notifyCaptureBanner(tabId, 'show', cap.requests.size);
  }
});
chrome.tabs.onRemoved.addListener((tabId) => {
  // 标签页关闭：摘掉对该页的监听状态，避免内存泄漏与悬挂会话。
  const cap = getCapture(tabId);
  if (cap) {
    cap.capturing = false;
    cap.attached = false;
    cap.stoppedAt = Date.now();
    netCaptures.delete(String(tabId));
  }
  sendTabsUpdate();
});
chrome.tabs.onCreated.addListener(() => sendTabsUpdate());
