const DEFAULT_CONFIG = {
  bridgeUrl: 'ws://127.0.0.1:8001/mcp/cdp-bridge/session',
  clientToken: '',
  bridgeEnabled: true,
};

const STATUS_LABELS = {
  connected: '已认证并连接',
  authenticating: '正在认证…',
  auth_error: '认证失败',
  unconfigured: '尚未配置客户端 Token',
  disabled: '连接已关闭',
  disconnected: '未连接',
};
const AUTH_ERROR_LABELS = {
  invalid: '客户端 Token 无效',
  invalid_token: '客户端 Token 无效或已撤销',
  disabled: '该客户端已停用',
  permission_denied: '该客户端无连接权限',
  already_connected: '该客户端已在其他浏览器连接',
  protocol_required: '服务端要求认证协议 2',
  revoked: '客户端授权已撤销',
  connection_timeout: '连接 Bridge 超时，请检查地址、端口和网络',
  auth_timeout: '等待服务端认证响应超时，请检查 Bridge 地址和服务端版本',
};

const WS_RE = /^wss?:\/\/.+/i;

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('saveBridge').addEventListener('click', saveBridgeConfig);
  document.getElementById('bridgeUrl').addEventListener('input', updatePreview);
  document.getElementById('clientToken').addEventListener('input', updatePreview);
  document.getElementById('bridgeEnabled').addEventListener('change', updatePreview);
  loadBridgeConfig();
  const timer = setInterval(refreshStatus, 750);
  window.addEventListener('unload', () => clearInterval(timer), { once: true });
});

async function loadBridgeConfig() {
  const resp = await chrome.runtime.sendMessage({ cmd: 'bridge_config_get' });
  const config = resp?.data || DEFAULT_CONFIG;
  document.getElementById('bridgeUrl').value = config.bridgeUrl || DEFAULT_CONFIG.bridgeUrl;
  document.getElementById('clientToken').value = config.clientToken || '';
  document.getElementById('bridgeEnabled').checked = config.bridgeEnabled !== false;
  updatePreview();
  await refreshStatus();
}

async function refreshStatus() {
  const resp = await chrome.runtime.sendMessage({ cmd: 'bridge_status_get' });
  renderStatus(resp?.data);
}

function renderStatus(status) {
  const stateEl = document.getElementById('state');
  const dot = document.querySelector('.dot');
  const state = status?.state || 'disconnected';
  let label = STATUS_LABELS[state] || STATUS_LABELS.disconnected;
  if (status?.reprovisionRequired) {
    label = '检测到旧版 Bridge Token，已移除；请在管理端创建或轮换 CDP 客户端 Token 后重新粘贴';
  } else if (state === 'auth_error' && status?.error) {
    label += '：' + (AUTH_ERROR_LABELS[status.error.code] || status.error.message || status.error.code);
  }
  stateEl.textContent = label;
  stateEl.title = status?.error?.message || '';
  dot.dataset.state = state;
}

function readBridgeConfig() {
  const rawUrl = document.getElementById('bridgeUrl').value.trim();
  const clientToken = document.getElementById('clientToken').value.trim();
  const bridgeEnabled = document.getElementById('bridgeEnabled').checked;
  // 允许省略 ws:// 前缀，自动补齐。
  let bridgeUrl = rawUrl || DEFAULT_CONFIG.bridgeUrl;
  if (!/^wss?:\/\//i.test(bridgeUrl)) bridgeUrl = 'ws://' + bridgeUrl;
  return { bridgeUrl, clientToken, bridgeEnabled };
}

function updatePreview() {
  const state = document.getElementById('state');
  const cfg = readBridgeConfig();
  const urlEl = document.getElementById('bridgeUrl');
  const urlValid = WS_RE.test(cfg.bridgeUrl);
  if (!urlValid) {
    urlEl.style.borderColor = '#ef4444';
    document.getElementById('wsPreview').textContent = cfg.bridgeEnabled
      ? '⚠️ 地址需以 ws:// 或 wss:// 开头'
      : '已关闭连接（开关未开启）';
  } else {
    urlEl.style.borderColor = '';
    document.getElementById('wsPreview').textContent = cfg.bridgeEnabled
      ? cfg.bridgeUrl
      : '已关闭连接（开关未开启）';
  }
  if (state && !state.textContent) state.textContent = STATUS_LABELS.disconnected;
}

async function saveBridgeConfig() {
  const state = document.getElementById('state');
  const config = readBridgeConfig();
  if (config.bridgeEnabled && !WS_RE.test(config.bridgeUrl)) {
    document.getElementById('wsPreview').textContent = '⚠️ 地址需以 ws:// 或 wss:// 开头，不能保存';
    return;
  }
  if (config.bridgeEnabled && !config.clientToken) {
    document.getElementById('wsPreview').textContent = '请粘贴管理端生成的客户端 Token';
    return;
  }
  const resp = await chrome.runtime.sendMessage({ cmd: 'bridge_config_set', config });
  if (resp?.data?.clientToken !== undefined) {
    document.getElementById('clientToken').value = resp.data.clientToken;
  }
  updatePreview();
  await refreshStatus();
}
