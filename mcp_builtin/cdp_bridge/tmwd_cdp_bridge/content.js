// --- 注入节点存活守卫 ---
// 内容脚本只在整页导航时注入一次，而 SPA 路由切换/框架整段重渲染会把 body 里的
// 节点连同我们注入的气泡与聊天面板一起清掉，于是二者消失、只有刷新才回来。
// 这里统一登记「取节点 + 重挂」两个回调，节点一旦脱离文档就重新挂回去。
// 同一扩展的内容脚本共享 isolated world 的 window，故挂在 window 上跨 IIFE 复用。
(function () {
  if (window.__tmwdMountGuard) return;
  const guards = [];
  let scheduled = false;
  // 少数页面会主动清掉不认识的节点，重挂又触发下一轮观察，可能与页面互相拉锯。
  // 每个守卫给一个短窗口内的重挂上限，超了就停手，宁可气泡不回来也不能把页面卡死。
  const BURST_MS = 2000;
  const BURST_MAX = 12;
  function run() {
    scheduled = false;
    const now = Date.now();
    for (const g of guards) {
      try {
        const n = g.get();
        if (!n || !n.isConnected) {
          if (now - g.windowStart > BURST_MS) { g.windowStart = now; g.count = 0; }
          if (g.count >= BURST_MAX) continue;
          g.count++;
          g.mount();
        }
      } catch (_) { /* ignore */ }
    }
  }
  function schedule() {
    if (scheduled) return;
    scheduled = true;
    // 合并同一批 DOM 变更；后台标签页 rAF 不触发，退回定时器。
    if (document.hidden) setTimeout(run, 120);
    else requestAnimationFrame(run);
  }
  window.__tmwdMountGuard = function (get, mount) {
    guards.push({ get, mount, windowStart: Date.now(), count: 0 });
    schedule();
  };
  new MutationObserver(schedule).observe(document.documentElement, { childList: true, subtree: true });
  // 软导航与标签页切回：DOM 变更观察偶有漏网（先清空、后异步渲染），补一层兜底。
  for (const evt of ['popstate', 'hashchange', 'pageshow', 'visibilitychange']) {
    window.addEventListener(evt, schedule, true);
  }
})();

;(function(){ if (/streamlit/i.test(document.title)) return;

// Remove meta CSP tags
document.querySelectorAll('meta[http-equiv="Content-Security-Policy"]').forEach(e => e.remove());

// Indicator badge at bottom-right (userscript style)
(function(){
  if(window.self!==window.top)return;
  if(document.getElementById('ljq-ind'))return;
  const d=document.createElement('div');
  d.id='ljq-ind';
  d.setAttribute('role','button');
  d.setAttribute('aria-label','Ai Lubricant 正在检查连接状态');
  d.title='Ai Lubricant 正在检查连接状态';
  d.dataset.state='authenticating';
  d.innerHTML='<span class="ljq-ind-icon"><svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/></svg></span><span class="ljq-ind-dot"></span><span class="ljq-ind-label"><span class="ljq-ind-text">网页 Agent</span><span class="ljq-ind-status">正在连接</span></span>';
  const style=document.createElement('style');
  style.textContent=`
    #ljq-ind{position:fixed;right:20px;bottom:20px;display:inline-flex;align-items:center;justify-content:center;width:52px;height:52px;border-radius:50%;background:linear-gradient(135deg,#0f766e 0%,#0d9488 100%);color:#fff;box-shadow:0 10px 28px rgba(15,118,110,.38),0 4px 10px rgba(15,23,42,.14);z-index:2147483647;cursor:grab;user-select:none;transition:transform .18s ease,box-shadow .18s ease;}
    #ljq-ind:hover{transform:translateY(-2px) scale(1.04);box-shadow:0 14px 34px rgba(15,118,110,.44),0 6px 14px rgba(15,23,42,.16);}
    #ljq-ind:active{transform:scale(1)}
    #ljq-ind .ljq-ind-icon{display:inline-flex;align-items:center;justify-content:center;color:#fff;pointer-events:none}
    #ljq-ind .ljq-ind-dot{position:absolute;right:5px;bottom:5px;width:14px;height:14px;border-radius:50%;background:#94a3b8;border:2.5px solid #fff;box-shadow:0 1px 4px rgba(15,23,42,.25);flex:0 0 auto;}
    #ljq-ind[data-state="connected"] .ljq-ind-dot{background:#10b981;animation:tmwd-pulse 2.4s ease-in-out infinite}
    #ljq-ind[data-state="authenticating"] .ljq-ind-dot{background:#f59e0b}
    #ljq-ind[data-state="disconnected"] .ljq-ind-dot,#ljq-ind[data-state="disabled"] .ljq-ind-dot,#ljq-ind[data-state="auth_error"] .ljq-ind-dot{background:#ef4444}
    #ljq-ind[data-state="unconfigured"] .ljq-ind-dot{background:#94a3b8}
    @keyframes tmwd-pulse{0%,100%{box-shadow:0 0 0 0 rgba(16,185,129,.5),0 1px 4px rgba(15,23,42,.25)}50%{box-shadow:0 0 0 6px rgba(16,185,129,0),0 1px 4px rgba(15,23,42,.25)}}
    #ljq-ind .ljq-ind-label{position:absolute;right:62px;top:50%;transform:translateY(-50%) translateX(10px);display:inline-flex;align-items:center;gap:8px;background:#fff;color:#0f172a;border:1px solid rgba(15,23,42,.08);border-radius:999px;padding:0 14px;height:34px;white-space:nowrap;font:600 12px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;box-shadow:0 8px 22px rgba(15,23,42,.16);opacity:0;pointer-events:none;transition:opacity .18s ease,transform .18s ease}
    #ljq-ind:hover .ljq-ind-label{opacity:1;transform:translateY(-50%) translateX(0)}
    #ljq-ind .ljq-ind-text{white-space:nowrap}
    #ljq-ind .ljq-ind-status{padding-left:8px;border-left:1px solid rgba(15,23,42,.12);color:#64748b;font-size:11px;font-weight:500}
    #ljq-ind[data-state="connected"] .ljq-ind-status{color:#0f766e}
    #ljq-ind[data-state="authenticating"] .ljq-ind-status{color:#b45309}
    #ljq-ind[data-state="disconnected"] .ljq-ind-status,#ljq-ind[data-state="disabled"] .ljq-ind-status,#ljq-ind[data-state="auth_error"] .ljq-ind-status{color:#b91c1c}
  `;
  function setBridgeStatus(status){
    const state=status&&status.state||'disconnected';
    const labels={connected:'已监控',authenticating:'正在认证',auth_error:'认证失败',unconfigured:'未配置',disabled:'已关闭',disconnected:'未连接'};
    const label=labels[state]||labels.disconnected;
    d.dataset.state=state;
    d.title='Ai Lubricant '+label;
    d.setAttribute('aria-label','Ai Lubricant '+label);
    const statusEl=d.querySelector('.ljq-ind-status');
    if(statusEl)statusEl.textContent=label;
  }
  function showBridgeNotice(message,title){
    if(!document.getElementById('tmwd-bridge-notice-style')){
      const s=document.createElement('style');
      s.id='tmwd-bridge-notice-style';
      s.textContent=`
        .tmwd-bridge-notice{position:fixed;right:14px;bottom:52px;z-index:2147483647;width:min(360px,calc(100vw - 28px));display:grid;grid-template-columns:26px 1fr;gap:10px;align-items:start;padding:12px 13px;border:1px solid rgba(18,24,38,.10);border-radius:12px;background:rgba(255,255,255,.95);color:#182033;box-shadow:0 10px 28px rgba(18,24,38,.14);font:400 13px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;letter-spacing:0;opacity:0;transform:translateY(6px);transition:opacity .18s ease,transform .18s ease;backdrop-filter:saturate(140%) blur(10px);-webkit-backdrop-filter:saturate(140%) blur(10px);}
        .tmwd-bridge-notice.is-visible{opacity:1;transform:translateY(0);}
        .tmwd-bridge-notice-icon{width:26px;height:26px;border-radius:50%;display:grid;place-items:center;background:#eef8f3;color:#168456;font:700 13px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;}
        .tmwd-bridge-notice-title{color:#182033;font-weight:650;margin:0 0 3px;}
        .tmwd-bridge-notice-message{color:#4c566a;margin:0;max-height:96px;overflow:hidden;word-break:break-word;}
      `;
      (document.head||document.documentElement).appendChild(s);
    }
    document.querySelectorAll('.tmwd-bridge-notice').forEach(n=>n.remove());
    const n=document.createElement('div');
    n.className='tmwd-bridge-notice';
    n.innerHTML='<div class="tmwd-bridge-notice-icon">i</div><div><div class="tmwd-bridge-notice-title"></div><p class="tmwd-bridge-notice-message"></p></div>';
    n.querySelector('.tmwd-bridge-notice-title').textContent=title||'Ai Lubricant';
    n.querySelector('.tmwd-bridge-notice-message').textContent=message;
    (document.body||document.documentElement).appendChild(n);
    requestAnimationFrame(()=>n.classList.add('is-visible'));
    setTimeout(()=>n.classList.remove('is-visible'),3200);
    setTimeout(()=>n.remove(),3450);
  }
  // Drag support — make the badge draggable
  let _dragging=false,_hasDragged=false,_sX,_sY,_sL,_sT;
  function _start(cx,cy,e){
    _dragging=true;_hasDragged=false;
    const r=d.getBoundingClientRect();
    _sX=cx;_sY=cy;_sL=r.left;_sT=r.top;
    e.preventDefault();
  }
  function _move(cx,cy){
    if(!_dragging)return;
    const dx=cx-_sX,dy=cy-_sY;
    if(!_hasDragged&&(Math.abs(dx)>3||Math.abs(dy)>3)){
      _hasDragged=true;
      d.style.left=_sL+'px';d.style.top=_sT+'px';
      d.style.right='auto';d.style.bottom='auto';
      d.style.cursor='grabbing';d.style.transition='none';
    }
    if(_hasDragged){d.style.left=(_sL+dx)+'px';d.style.top=(_sT+dy)+'px';}
  }
  function _end(){
    if(!_dragging)return;
    _dragging=false;
    if(_hasDragged){d.style.cursor='grab';d.style.transition='';d._preventClick=true;}
  }
  d.addEventListener('mousedown',e=>{if(e.button===0)_start(e.clientX,e.clientY,e);});
  d.addEventListener('touchstart',e=>{const t=e.touches[0];_start(t.clientX,t.clientY,e);},{passive:false});
  document.addEventListener('mousemove',e=>_move(e.clientX,e.clientY));
  document.addEventListener('touchmove',e=>{const t=e.touches[0];_move(t.clientX,t.clientY);},{passive:false});
  document.addEventListener('mouseup',_end);
  document.addEventListener('touchend',_end);
  d._setBridgeStatus=setBridgeStatus;
  d._showBridgeNotice=showBridgeNotice;
  function mount(){
    if(!style.isConnected)(document.head||document.documentElement).appendChild(style);
    if(!d.isConnected)(document.body||document.documentElement).appendChild(d);
  }
  mount();
  // 复用同一个节点重挂：拖拽位置、data-state 徽标状态与事件监听都留在节点上，
  // SPA 重渲染后气泡回到原处而不是重置到右下角。
  window.__tmwdMountGuard&&window.__tmwdMountGuard(()=>d,mount);
})();

// --- 网络监听顶部通栏横幅 ---
(function(){
  if (window.self !== window.top) return; // 仅顶层框架显示
  const BANNER_ID = 'tmwd-net-capture-banner';

  function ensureBannerStyle(){
    if (document.getElementById(BANNER_ID + '-style')) return;
    const s = document.createElement('style');
    s.id = BANNER_ID + '-style';
    s.textContent = `
      #${BANNER_ID}{position:fixed;top:0;left:0;right:0;z-index:2147483647;display:flex;align-items:center;justify-content:center;gap:10px;height:34px;padding:0 14px;background:linear-gradient(90deg,#b32020,#d13333);color:#fff;font:600 13px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;letter-spacing:.2px;box-shadow:0 2px 10px rgba(179,32,32,.35);transform:translateY(-100%);transition:transform .2s ease;}
      #${BANNER_ID}.is-visible{transform:translateY(0);}
      #${BANNER_ID} .tmwd-net-dot{width:9px;height:9px;border-radius:50%;background:#fff;box-shadow:0 0 0 0 rgba(255,255,255,.7);animation:tmwd-net-pulse 1.4s infinite;flex:0 0 auto;}
      @keyframes tmwd-net-pulse{0%{box-shadow:0 0 0 0 rgba(255,255,255,.6);}70%{box-shadow:0 0 0 7px rgba(255,255,255,0);}100%{box-shadow:0 0 0 0 rgba(255,255,255,0);}}
      #${BANNER_ID} .tmwd-net-count{font-variant-numeric:tabular-nums;opacity:.92;font-weight:500;}
      #${BANNER_ID} .tmwd-net-stop{margin-left:6px;height:22px;padding:0 12px;border:1px solid rgba(255,255,255,.55);border-radius:999px;background:rgba(255,255,255,.14);color:#fff;font:600 12px/1 inherit;cursor:pointer;transition:background .15s ease,border-color .15s ease;}
      #${BANNER_ID} .tmwd-net-stop:hover{background:rgba(255,255,255,.26);border-color:#fff;}
    `;
    (document.head || document.documentElement).appendChild(s);
  }

  function showBanner(count){
    ensureBannerStyle();
    let b = document.getElementById(BANNER_ID);
    if (!b){
      b = document.createElement('div');
      b.id = BANNER_ID;
      b.setAttribute('role', 'status');
      b.innerHTML = '<span class="tmwd-net-dot"></span>'
        + '<span class="tmwd-net-label">正在监听网络请求</span>'
        + '<span class="tmwd-net-count"></span>'
        + '<button type="button" class="tmwd-net-stop">停止监听</button>';
      (document.body || document.documentElement).appendChild(b);
      b.querySelector('.tmwd-net-stop').addEventListener('click', () => {
        b.querySelector('.tmwd-net-stop').textContent = '停止中…';
        chrome.runtime.sendMessage({ cmd: 'net_stop' }).catch(() => {});
      });
    }
    b.querySelector('.tmwd-net-count').textContent = '· 已捕获 ' + (count || 0) + ' 条';
    requestAnimationFrame(() => b.classList.add('is-visible'));
  }

  function hideBanner(){
    const b = document.getElementById(BANNER_ID);
    if (!b) return;
    b.classList.remove('is-visible');
    setTimeout(() => b.remove(), 220);
  }

  chrome.runtime.onMessage.addListener((msg) => {
    if (!msg || msg.cmd !== 'network_capture_banner') return;
    if (msg.action === 'hide') hideBanner();
    else showBanner(msg.count); // 'show' | 'update'
  });
})();

new MutationObserver(muts => {
  for (const m of muts) for (const n of m.addedNodes) {
    if (n.id === TID || (n.querySelector && n.querySelector('#' + TID))) {
      const el = n.id === TID ? n : n.querySelector('#' + TID);
      handle(el);
    }
  }
}).observe(document.documentElement, { childList: true, subtree: true });

async function handle(el) {
  try {
    const text = el.textContent.trim();
    if (!text) { el.textContent = JSON.stringify({ ok: false, error: 'empty request' }); return; }
    const req = JSON.parse(text);
    const cmd = req.cmd;
    let resp;
    if (cmd === 'cdp') {
      resp = await chrome.runtime.sendMessage({ cmd: 'cdp', method: req.method, params: req.params || {}, tabId: req.tabId });
    } else if (cmd === 'batch') {
      resp = await chrome.runtime.sendMessage({ cmd: 'batch', commands: req.commands, tabId: req.tabId });
    } else if (cmd === 'tabs') {
      resp = await chrome.runtime.sendMessage({ cmd: 'tabs', method: req.method, tabId: req.tabId, url: req.url, newWindow: req.newWindow, active: req.active });
    } else {
      resp = { ok: false, error: 'unknown cmd: ' + cmd };
    }
    el.textContent = JSON.stringify(resp);
  } catch (e) {
    el.textContent = JSON.stringify({ ok: false, error: e.message });
  }
}
})();

// --- 网页侧 Agent 对话面板 ---
(function () {
  if (window.self !== window.top) return; // 仅顶层框架挂面板

  const PANEL_ID = 'tmwd-chat-panel';
  const reqListeners = new Map(); // reqId -> callback(event)
  let statusQueryPending = false;

  function updateBridgeIndicator(status) {
    const ind = document.getElementById('ljq-ind');
    if (ind && typeof ind._setBridgeStatus === 'function') ind._setBridgeStatus(status);
  }

  function showBridgeUnavailable(status) {
    const ind = document.getElementById('ljq-ind');
    if (!ind || typeof ind._showBridgeNotice !== 'function') return;
    const errorLabels={invalid:'客户端 Token 无效',invalid_token:'客户端 Token 无效或已撤销',disabled:'客户端已停用',permission_denied:'没有连接权限',already_connected:'客户端已在其他浏览器连接',protocol_required:'服务端要求认证协议 2',revoked:'客户端授权已撤销'};
    let message;
    if(status&&status.state==='disabled') message='连接已关闭，请先在扩展设置中启用';
    else if(status&&status.state==='unconfigured') message='尚未配置客户端 Token，请在扩展设置中粘贴管理端生成的 Token';
    else if(status&&status.state==='auth_error') message=errorLabels[status.error&&status.error.code]||(status.error&&status.error.message)||'客户端认证失败';
    else if(status&&status.state==='authenticating') message='已连上服务器，正在等待客户端认证完成';
    else message='尚未连接服务器，请检查 Ai Lubricant 服务与客户端 Token';
    ind._showBridgeNotice(message, 'Ai Lubricant 未连接');
  }

  function getBridgeStatus() {
    return new Promise((resolve) => {
      chrome.runtime.sendMessage({ cmd: 'bridge_status_get' }, (resp) => {
        if (chrome.runtime.lastError || !resp || !resp.ok || !resp.data) {
          resolve({ connected: false, authenticated: false, enabled: true, state: 'disconnected', error: null });
          return;
        }
        resolve(resp.data);
      });
    });
  }

  // 接收 background 转发的对话回帧。
  chrome.runtime.onMessage.addListener((msg) => {
    if (!msg || msg.cmd !== 'chat_frame' || !msg.data) return;
    const cb = reqListeners.get(String(msg.data.reqId));
    if (cb) cb(msg.data);
  });

  // 接收 SW 主动推送的桥接状态：连上/断开/认证失败时 background 广播，
  // 徽标实时跟上，无需刷新页面或点徽标才重查。
  // 连上后顺带恢复会话：刷新页面时 SW 多半还在重连，加载阶段的自动恢复会
  // 因 !connected 跳过；这里在收到 connected 时补拉 storage 里的 conversationId。
  chrome.runtime.onMessage.addListener((msg) => {
    if (!msg || msg.cmd !== 'bridge_status_push' || !msg.status) return;
    updateBridgeIndicator(msg.status);
    // SW 刚连上：若面板开着但还没会话内容，触发 openFlow 重拉 agents 并恢复
    // 上次会话。若此刻已有一个 openFlow 在跑（_resuming=true），挂起 _pendingResume，
    // 那个 openFlow 跑完的 finally 会据此再跑一次——避免"加载时 SW 没连→空
    // picker→SW 连上但 connected 只推一次→卡死在空列表"的时序漏洞。
    if (msg.status.connected && state.open && !state.conversationId && !state.messages.length) {
      if (state._resuming) { state._pendingResume = true; return; }
      runOpenFlow();
    }
  });

  function runOpenFlow() {
    if (state._resuming) return;
    state._resuming = true;
    void openFlow().finally(() => {
      state._resuming = false;
      if (state._pendingResume) {
        state._pendingResume = false;
        runOpenFlow(); // 跑期间又收到 connected 请求，补跑一次
      }
    });
  }

  // 截图时临时隐藏聊天面板/气泡，截完恢复。只动我们自己注入的 #tmwd-chat-panel，
  // 不碰页面其它元素。面板尚未懒加载出来时直接回 ok:false，background 照常截图。
  let screenshotHidden = false;
  let prevDisplay = '';
  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (!msg) return;
    if (msg.cmd === 'hide_panel_for_screenshot') {
      if (!host) { sendResponse && sendResponse({ ok: false }); return true; }
      if (!screenshotHidden) {
        prevDisplay = host.style.display || '';
        host.style.display = 'none';
        screenshotHidden = true;
      }
      sendResponse && sendResponse({ ok: true });
      return true;
    }
    if (msg.cmd === 'show_panel_after_screenshot') {
      if (host && screenshotHidden) {
        host.style.display = prevDisplay;
        screenshotHidden = false;
      }
      sendResponse && sendResponse({ ok: true });
      return true;
    }
  });

  function sendChat(cmd, payload, onEvent) {
    return new Promise((resolve) => {
      const reqId = 'chat_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
      let settled = false;
      const done = (value) => { if (!settled) { settled = true; reqListeners.delete(reqId); resolve(value); } };
      reqListeners.set(reqId, (frame) => {
        if (frame.type === 'chat_event' && frame.event) {
          onEvent(frame.event);
          if (frame.event.type === 'done' || frame.event.type === 'error') done(frame.event);
        } else if (frame.type === 'chat_error') {
          onEvent({ type: 'error', message: frame.message || '对话出错' });
          done({ type: 'error', message: frame.message });
        } else if (frame.type === 'chat_aborted') {
          onEvent({ type: 'aborted' });
          done({ type: 'aborted' });
        } else if (frame.type === 'chat_agents') {
          done(frame.agents || []);
        } else if (frame.type === 'chat_models') {
          done({ models: frame.models || [], defaultModel: frame.defaultModel || '' });
        } else if (frame.type === 'chat_conversations') {
          done({ conversations: frame.conversations || [] });
        } else if (frame.type === 'chat_conversation_created') {
          done({ conversation: frame.conversation || null });
        } else if (frame.type === 'chat_conversation_loaded') {
          done({ conversation: frame.conversation || null, messages: frame.messages || [] });
        } else if (frame.type === 'chat_approval_resolved') {
          done({ ok: !!frame.ok, result: frame.result || '', message: frame.message || '' });
        }
      });
      // 不带 tabId：background 用 sender.tab.id 即当前标签页。
      chrome.runtime.sendMessage({ cmd, reqId, ...payload }, (resp) => {
        if (!resp || !resp.ok) {
          onEvent({ type: 'error', message: (resp && resp.error) || '连接未就绪，请检查 Ai Lubricant 服务与客户端 Token' });
          done({ type: 'error', message: (resp && resp.error) || 'channel unavailable' });
        }
      });
    });
  }

  // 思考等级选项与用户端 Agent 聊天页同源（reasoning-effort.tsx）。
  const REASONING_OPTIONS = [
    { value: '', label: '关闭思考' },
    { value: 'low', label: '低' },
    { value: 'medium', label: '中' },
    { value: 'high', label: '高' },
    { value: 'xhigh', label: '极高' },
  ];
  const DEFAULT_REASONING = 'medium';
  const MODE_OPTIONS = [
    { value: 'interact', label: '普通', title: '普通交互：一问一答' },
    { value: 'auto', label: '自动', title: '自动模式：免审批范围待定，当前与普通模式一致' },
    { value: 'plan', label: '规划', title: '规划模式：复杂任务先规划再分步执行' },
    { value: 'goal', label: '目标', title: '目标模式：开放目标+时间预算，自驱跑到预算耗尽' },
  ];
  const AGENT_STORAGE_KEY = 'cdpChatAgentId';

  let state = {
    open: false,
    view: 'chat', // 'picker' = 首屏 Agent 选择；'chat' = 对话
    agents: [],
    selectedAgent: null,
    conversationId: '',
    sending: false,
    currentReqId: null,
    messages: [], // {role:'user'|'assistant', content, toolCalls:[{name,args,status,result}], status}
    sessions: [], // 会话列表摘要 {id,title,agent_id,updated_at}
    drawerOpen: false,
    loadingSessions: false,
    // 对话级参数（与用户端 Agent 聊天页对齐）
    models: [],
    selectedModel: '',
    defaultModel: '', // agent 记录里配置的模型，下拉里打「默认」标
    modelQuery: '',   // 模型下拉搜索词
    reasoningEffort: DEFAULT_REASONING,
    mode: 'interact',
    goalObjective: '',
    goalBudgetMinutes: 15,
    lastUsage: null, // {prompt,completion,total,cached,cacheCreation,reasoning}
    _resuming: false, // connected 推送触发的恢复进行中守卫，防重复触发
    _pendingResume: false, // 恢复期间又收到 connected 请求时挂起，跑完补跑一次
  };

  function storedAgentId() {
    return new Promise((resolve) => {
      try {
        chrome.storage.local.get([AGENT_STORAGE_KEY], (res) => {
          const v = Number(res && res[AGENT_STORAGE_KEY]);
          resolve(Number.isFinite(v) && v > 0 ? v : null);
        });
      } catch (_) { resolve(null); }
    });
  }

  function rememberAgentId(id) {
    try { chrome.storage.local.set({ [AGENT_STORAGE_KEY]: Number(id) || null }); } catch (_) { /* ignore */ }
  }

  // --- 按 tabId 的会话级持久化（整页导航后面板/会话恢复用）---
  // session_key 在网关侧是 "{client_id}:{tabId}"，导航不换 tab，key 稳定；这里按
  // tabId 存「面板是否开着 + 当前 conversationId + 选中 agent」，导航重注入后恢复。
  let tabId = null;

  function panelStorageKey() {
    return tabId != null ? `cdpPanel:${tabId}` : null;
  }

  function getTabId() {
    return new Promise((resolve) => {
      try {
        chrome.runtime.sendMessage({ cmd: 'bridge_tab_id' }, (resp) => {
          if (chrome.runtime.lastError || !resp || !resp.ok) { resolve(null); return; }
          resolve((resp.data && resp.data.tabId) || null);
        });
      } catch (_) { resolve(null); }
    });
  }

  function readPanelState() {
    return new Promise((resolve) => {
      const key = panelStorageKey();
      if (!key) { resolve(null); return; }
      try {
        chrome.storage.session.get([key], (res) => {
          resolve((res && res[key]) || null);
        });
      } catch (_) { resolve(null); }
    });
  }

  function writePanelState(patch) {
    const key = panelStorageKey();
    if (!key) return;
    try {
      chrome.storage.session.get([key], (res) => {
        const cur = (res && res[key]) || {};
        const next = { ...cur, ...patch };
        try { chrome.storage.session.set({ [key]: next }); } catch (_) { /* ignore */ }
      });
    } catch (_) { /* ignore */ }
  }

  // 本页上次手动选的模型。只在 agentId 对得上时才认，避免换 agent 后串用旧选择。
  async function savedModelForAgent(agentId) {
    const saved = await readPanelState();
    if (!saved || !saved.model) return '';
    if (saved.agentId && Number(saved.agentId) !== Number(agentId)) return '';
    return String(saved.model);
  }

  // 懒加载面板 DOM（含 Shadow DOM 隔离页面样式）。
  let host, shadow, els = {};
  function ensurePanel() {
    if (host) return;
    host = document.createElement('div');
    host.id = PANEL_ID;
    host.style.cssText = 'all:initial;position:fixed;top:0;right:0;width:0;height:100vh;z-index:2147483646;';
    shadow = host.attachShadow({ mode: 'open' });
    const ICON = {
      send: '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 2 11 13"/><path d="M22 2 15 22 11 13 2 9 22 2Z"/></svg>',
      plus: '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14M5 12h14"/></svg>',
      list: '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/></svg>',
      close: '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18M6 6l12 12"/></svg>',
      refresh: '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>',
      eraser: '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 21h13M17 5 8.5 13.5a2.12 2.12 0 0 0 0 3L11 19"/><path d="m15 7 4-4 3 3-4 4"/><path d="m5 11 6 6"/></svg>',
      wrench: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg>',
      bot: '<svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4M8 16h.01M16 16h.01"/></svg>',
      chevron: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>',
      shield: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="m9 12 2 2 4-4"/></svg>',
    };
    shadow.innerHTML = `
      <style>
        /* 配色对齐用户端：主色橙 oklch(0.555 0.163 48.998)≈#b45309；面板在第三方页
           Shadow DOM 内取不到宿主变量，故用固定近似值；暗色系统主题切绿(#4ade80)。 */
        :host{
          --tmwd-primary:#b45309;--tmwd-primary-hover:#92400e;--tmwd-primary-fg:#fff;
          --tmwd-primary-soft:rgba(180,83,9,.10);--tmwd-primary-ring:rgba(180,83,9,.12);
          --tmwd-bg:#fff;--tmwd-body:#f8fafc;--tmwd-fg:#0f172a;--tmwd-muted:#94a3b8;
          --tmwd-border:rgba(15,23,42,.10);--tmwd-border-strong:rgba(15,23,42,.22);
        }
        @media (prefers-color-scheme: dark){
          :host{
            --tmwd-primary:#4ade80;--tmwd-primary-hover:#22c55e;--tmwd-primary-fg:#0d1117;
            --tmwd-primary-soft:rgba(74,222,128,.14);--tmwd-primary-ring:rgba(74,222,128,.18);
            --tmwd-bg:#161b22;--tmwd-body:#0d1117;--tmwd-fg:#e6edf3;--tmwd-muted:#8b949e;
            --tmwd-border:rgba(240,246,252,.10);--tmwd-border-strong:rgba(240,246,252,.22);
          }
        }
        :host, *{box-sizing:border-box}
        .wrap{position:fixed;top:16px;right:16px;bottom:16px;width:420px;max-width:calc(100vw - 32px);display:flex;flex-direction:column;background:var(--tmwd-bg);border:1px solid var(--tmwd-border);border-radius:20px;box-shadow:0 24px 64px rgba(15,23,42,.22),0 8px 20px rgba(15,23,42,.10);transform:translateX(calc(100% + 32px));transition:transform .28s cubic-bezier(.4,0,.2,1);font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;color:var(--tmwd-fg);overflow:hidden;}
        .wrap.open{transform:translateX(0)}
        @media (max-width:480px){.wrap{top:0;right:0;bottom:0;left:0;width:100vw;max-width:100vw;border-radius:0;border:0;height:100vh}}
        header{display:flex;align-items:center;gap:8px;padding:14px 16px;border-bottom:1px solid var(--tmwd-border);background:var(--tmwd-bg)}
        header .title{font-weight:650;font-size:15px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .iconbtn{cursor:pointer;border:1px solid var(--tmwd-border);background:var(--tmwd-bg);border-radius:8px;height:32px;width:32px;display:grid;place-items:center;color:var(--tmwd-muted);padding:0;line-height:1;transition:color .15s ease,background .15s ease,border-color .15s ease}
        .iconbtn:hover{color:var(--tmwd-fg);background:var(--tmwd-body);border-color:var(--tmwd-border-strong)}
        .iconbtn:active{transform:translateY(1px)}
        .iconbtn:disabled{opacity:.45;cursor:not-allowed}
        .body{flex:1;overflow-y:auto;overflow-x:hidden;padding:16px;display:flex;flex-direction:column;gap:12px;background:var(--tmwd-body)}
        /* 消息气泡：用户与 Agent 均靠左，呈时间线流（不再左右分边） */
        .msg-row{display:flex;min-width:0;justify-content:flex-start}
        .bubble{max-width:82%;min-width:0;display:flex;flex-direction:column;gap:6px;padding:9px 12px;border-radius:14px;font-size:13.5px;line-height:1.55}
        .bubble.user{background:var(--tmwd-primary);color:var(--tmwd-primary-fg)}
        .bubble.assistant{background:var(--tmwd-bg);color:var(--tmwd-fg);border:1px solid var(--tmwd-border);box-shadow:0 1px 2px rgba(15,23,42,.04)}
        .bubble.assistant.err{background:#fef2f2;border-color:rgba(185,28,28,.18)}
        .msg-head{display:flex;align-items:center;gap:8px;font-size:10.5px;opacity:.65}
        .msg-head .msg-time{margin-left:auto}
        .msg-text{white-space:pre-wrap;word-break:break-word}
        /* markdown 正文：done 态切 innerHTML 渲染，流式中仍用 textContent 避免抖动 */
        .msg-text.md{white-space:normal}
        .msg-text.md p{margin:0 0 .5em}
        .msg-text.md p:last-child{margin-bottom:0}
        .msg-text.md ul,.msg-text.md ol{margin:0 0 .5em;padding-left:1.4em}
        .msg-text.md li{margin:.15em 0}
        .msg-text.md h1,.msg-text.md h2,.msg-text.md h3{font-size:1em;font-weight:700;margin:.4em 0 .25em}
        .msg-text.md a{color:var(--tmwd-primary)}
        .msg-text.md code{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.92em;background:var(--tmwd-body);padding:.1em .35em;border-radius:4px}
        .msg-text.md pre{margin:0 0 .5em;padding:8px 10px;background:var(--tmwd-body);border:1px solid var(--tmwd-border);border-radius:8px;overflow-x:auto}
        .msg-text.md pre code{background:none;padding:0;border-radius:0;font-size:.9em}
        .msg-text.md blockquote{margin:0 0 .5em;padding-left:.8em;border-left:3px solid var(--tmwd-border);color:var(--tmwd-muted)}
        @media (prefers-color-scheme: dark){
          .msg-text.md code{background:#0d1117}
          .msg-text.md pre{background:#0d1117;border-color:var(--tmwd-border)}
        }
        .bubble.assistant.streaming .msg-text.cursor::after{content:'▋';display:inline-block;margin-left:2px;animation:tmwd-blink 1s steps(2,start) infinite;color:var(--tmwd-muted)}
        @keyframes tmwd-blink{to{opacity:0}}
        .thinking{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--tmwd-muted)}
        .retry-hint{display:flex;align-items:center;gap:6px;font-size:11.5px;color:var(--tmwd-muted);padding:3px 0}
        .msg-retry{margin-top:6px;align-self:flex-start;padding:3px 10px;font-size:12px;line-height:1.6;border:1px solid var(--tmwd-border);border-radius:6px;background:rgba(148,163,184,.1);color:var(--tmwd-fg);cursor:pointer}
        .msg-retry:hover{color:var(--tmwd-primary);border-color:var(--tmwd-primary);background:var(--tmwd-primary-soft)}
        .spinner{width:12px;height:12px;border:2px solid rgba(148,163,184,.35);border-top-color:#64748b;border-radius:50%;animation:tmwd-spin .7s linear infinite;flex:0 0 auto}
        @keyframes tmwd-spin{to{transform:rotate(360deg)}}
        .empty{align-self:center;margin:auto;display:flex;flex-direction:column;align-items:center;gap:12px;color:var(--tmwd-muted);text-align:center;font-size:12.5px;max-width:250px}
        .empty-icon{width:48px;height:48px;border-radius:16px;display:grid;place-items:center;background:var(--tmwd-primary-soft);color:var(--tmwd-primary)}
        .empty-title{font-size:13.5px;font-weight:600;color:var(--tmwd-fg)}
        /* 首屏 Agent 选择 */
        .picker{flex:1;display:flex;flex-direction:column;gap:10px;padding:4px}
        .picker-head{font-size:13.5px;font-weight:650;color:var(--tmwd-fg)}
        .picker-sub{font-size:12px;color:var(--tmwd-muted);margin-top:-4px}
        .picker-list{display:flex;flex-direction:column;gap:8px;overflow-y:auto}
        .picker-card{display:flex;flex-direction:column;gap:2px;padding:11px 12px;border:1px solid var(--tmwd-border);border-radius:11px;background:var(--tmwd-bg);cursor:pointer;transition:all .14s ease}
        .picker-card:hover{border-color:var(--tmwd-primary);background:var(--tmwd-primary-soft)}
        .picker-card .pc-name{font-size:13.5px;font-weight:650;color:var(--tmwd-fg);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .picker-card .pc-desc{font-size:11.5px;color:var(--tmwd-muted);overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
        /* 工具调用卡片（可折叠）*/
        .tool-card{border:1px solid var(--tmwd-border);border-radius:9px;background:var(--tmwd-body);overflow:hidden;font-size:12px}
        .tool-card .tc-head{display:flex;align-items:center;gap:7px;width:100%;padding:7px 9px;background:transparent;border:0;cursor:pointer;text-align:left;color:#334155}
        .tool-card .tc-icon{color:#64748b;flex:0 0 auto;display:inline-flex}
        .tool-card .tc-name{font-weight:600;color:var(--tmwd-fg);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .tool-card .tc-status{font-size:9.5px;padding:2px 7px;border-radius:6px;background:#e2e8f0;color:#475569;font-weight:600;flex:0 0 auto}
        .tool-card .tc-status.running{background:#dbeafe;color:#1d4ed8}
        .tool-card .tc-status.done{background:#dcfce7;color:#15803d}
        .tool-card .tc-status.error{background:#fee2e2;color:#b91c1c}
        .tool-card .tc-caret{color:var(--tmwd-muted);flex:0 0 auto;display:inline-flex;transition:transform .15s ease}
        .tool-card.open .tc-caret{transform:rotate(180deg)}
        .tool-card .tc-body{display:none;border-top:1px solid var(--tmwd-border);padding:8px 9px;gap:6px;flex-direction:column}
        .tool-card.open .tc-body{display:flex}
        .tool-card .tc-block{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:10.5px;color:#475569;white-space:pre-wrap;word-break:break-word;max-height:140px;overflow:auto;background:var(--tmwd-bg);border:1px solid var(--tmwd-border);border-radius:6px;padding:6px 8px}
        .tool-card .tc-label{font-size:10px;color:var(--tmwd-muted);font-weight:600;margin-bottom:2px}
        /* 代码执行审批卡：待裁决时用主色描边强调，必须由人点过才继续。 */
        .approval-card{border:1px solid var(--tmwd-primary);border-radius:9px;background:var(--tmwd-primary-soft);overflow:hidden;font-size:12px}
        .approval-card .ac-head{display:flex;align-items:center;gap:7px;padding:7px 9px;color:#334155}
        .approval-card .ac-icon{color:var(--tmwd-primary);flex:0 0 auto;display:inline-flex}
        .approval-card .ac-name{font-weight:600;color:var(--tmwd-fg);flex:1;min-width:0}
        .approval-card .ac-state{font-size:9.5px;padding:2px 7px;border-radius:6px;background:#e2e8f0;color:#475569;font-weight:600;flex:0 0 auto}
        .approval-card .ac-state.pending,.approval-card .ac-state.resolving{background:#fef3c7;color:#b45309}
        .approval-card .ac-state.allowed{background:#dcfce7;color:#15803d}
        .approval-card .ac-state.denied,.approval-card .ac-state.expired{background:#fee2e2;color:#b91c1c}
        .approval-card .ac-body{display:flex;flex-direction:column;gap:6px;border-top:1px solid var(--tmwd-border);padding:8px 9px}
        .approval-card .ac-msg{font-size:11px;color:#475569;line-height:1.5}
        .approval-card .ac-code{margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:10.5px;color:#334155;white-space:pre-wrap;word-break:break-word;max-height:180px;overflow:auto;background:var(--tmwd-bg);border:1px solid var(--tmwd-border);border-radius:6px;padding:6px 8px}
        .approval-card .ac-actions{display:flex;gap:8px;padding:0 9px 9px}
        .approval-card .ac-actions button{height:28px;padding:0 14px;border:0;border-radius:8px;font:12px/1 inherit;font-weight:600;cursor:pointer}
        .approval-card .ac-actions button:disabled{opacity:.55;cursor:not-allowed}
        .approval-card .ac-allow{background:var(--tmwd-primary);color:var(--tmwd-primary-fg)}
        .approval-card .ac-allow:hover:not(:disabled){background:var(--tmwd-primary-hover)}
        .approval-card .ac-deny{background:var(--tmwd-bg);color:#b91c1c;border:1px solid var(--tmwd-border-strong)!important}
        .approval-card .ac-deny:hover:not(:disabled){background:#fef2f2}
        footer{padding:12px 14px 14px;border-top:1px solid var(--tmwd-border);background:var(--tmwd-bg)}
        .composer{display:flex;flex-direction:column;gap:8px;border:1px solid var(--tmwd-border-strong);border-radius:14px;padding:8px;background:var(--tmwd-bg);transition:border-color .15s ease,box-shadow .15s ease}
        .composer:focus-within{border-color:var(--tmwd-primary);box-shadow:0 0 0 3px var(--tmwd-primary-soft)}
        textarea{width:100%;resize:none;height:52px;min-height:52px;max-height:150px;border:0;outline:none;font:14px inherit;background:transparent;color:var(--tmwd-fg);line-height:1.5;padding:5px 4px}
        textarea::placeholder{color:var(--tmwd-muted)}
        .composer-row{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
        /* 输入区控件（思考等级/模式/模型下拉，与用户端 MessageComposer 同序） */
        .ctl{position:relative;flex:0 0 auto}
        .ctl-trigger{display:flex;align-items:center;gap:4px;height:28px;padding:0 8px;border:0;border-radius:999px;background:transparent;color:var(--tmwd-muted);font:12px/1 inherit;cursor:pointer;transition:background .15s ease,color .15s ease}
        .ctl-trigger:hover{background:var(--tmwd-body);color:var(--tmwd-fg)}
        .ctl-trigger .ct-caret{opacity:.6;flex:0 0 auto;display:inline-flex}
        .ctl-menu{position:absolute;bottom:calc(100% + 6px);left:0;min-width:140px;max-height:260px;overflow-y:auto;background:var(--tmwd-bg);border:1px solid var(--tmwd-border);border-radius:10px;box-shadow:0 12px 28px rgba(15,23,42,.16);padding:4px;z-index:6;display:none}
        .ctl-menu.open{display:block}
        .ctl-opt{display:flex;flex-direction:column;gap:1px;padding:7px 9px;border-radius:7px;cursor:pointer;font-size:12.5px;color:var(--tmwd-fg)}
        .ctl-opt:hover{background:var(--tmwd-body)}
        .ctl-opt.active{background:var(--tmwd-primary-soft);color:var(--tmwd-primary)}
        .ctl-opt .co-desc{font-size:10.5px;color:var(--tmwd-muted)}
        /* 触发器文字过长时截断，完整值放 title */
        .ctl-trigger .ct-text{max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        /* 模型下拉带搜索，菜单更宽 */
        .ctl-menu.wide{min-width:236px;max-width:300px}
        .ctl-search{position:sticky;top:0;z-index:1;background:var(--tmwd-bg);padding:2px 2px 6px}
        .ctl-search input{width:100%;box-sizing:border-box;height:28px;border:1px solid var(--tmwd-border);border-radius:8px;background:var(--tmwd-bg);color:var(--tmwd-fg);font:12px/1 inherit;padding:0 8px;outline:none}
        .ctl-search input:focus{border-color:var(--tmwd-primary)}
        .ctl-opt .co-name{display:flex;align-items:center;gap:5px;min-width:0}
        .ctl-opt .co-name .co-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .ctl-opt .co-tag{flex:0 0 auto;padding:1px 5px;border-radius:5px;background:var(--tmwd-body);color:var(--tmwd-muted);font-size:10px;line-height:1.5}
        .ctl-opt.active .co-tag{background:var(--tmwd-bg)}
        .ctl-empty{padding:7px 9px;font-size:12px;color:var(--tmwd-muted)}
        .co-badge{flex:0 0 auto;font-size:9.5px;line-height:1.5;padding:0 5px;border-radius:999px;background:var(--tmwd-body);color:var(--tmwd-muted)}
        .ctl-empty{padding:7px 9px;font-size:12px;color:var(--tmwd-muted)}
        /* goal 输入 */
        .goal-inputs{display:flex;align-items:center;gap:4px;flex:1 1 100%}
        .goal-inputs input{height:28px;border:1px solid var(--tmwd-border);border-radius:8px;background:var(--tmwd-bg);color:var(--tmwd-fg);font:12px/1 inherit;padding:0 8px;outline:none}
        .goal-inputs input.g-obj{flex:1;min-width:0}
        .goal-inputs input.g-min{width:56px}
        .goal-inputs .g-unit{font-size:11px;color:var(--tmwd-muted)}
        /* 使用量圈 */
        .usage-btn{position:relative;height:28px;width:28px;border:0;border-radius:999px;background:transparent;cursor:pointer;display:grid;place-items:center;color:var(--tmwd-muted);flex:0 0 auto}
        .usage-btn:hover{background:var(--tmwd-body)}
        .usage-ring{transform:rotate(-90deg)}
        .usage-pop{position:absolute;bottom:calc(100% + 8px);right:0;width:220px;background:var(--tmwd-bg);border:1px solid var(--tmwd-border);border-radius:10px;box-shadow:0 12px 28px rgba(15,23,42,.16);padding:10px 12px;z-index:7;display:none;font-size:12px;color:var(--tmwd-fg)}
        .usage-pop.open{display:block}
        .usage-pop .up-row{display:flex;justify-content:space-between;gap:10px;padding:2px 0}
        .usage-pop .up-row span:first-child{color:var(--tmwd-muted)}
        /* 使用量圈挪到标题栏后，弹层要向下展开 */
        #usageCtl{position:relative;flex:0 0 auto}
        #usageCtl .usage-pop{top:calc(100% + 8px);bottom:auto}
        /* Agent 选择器（自定义下拉，替代原生 select） */
        .agent-picker{position:relative;flex:0 1 auto;min-width:0;max-width:160px}
        .agent-trigger{display:flex;align-items:center;gap:6px;height:28px;min-width:0;padding:0 8px;border:1px solid var(--tmwd-border);border-radius:9px;background:var(--tmwd-bg);color:var(--tmwd-fg);font:12px/1 inherit;cursor:pointer;transition:border-color .15s ease,box-shadow .15s ease}
        .agent-trigger:hover{border-color:var(--tmwd-primary);background:var(--tmwd-body)}
        .agent-trigger.open{border-color:var(--tmwd-primary);box-shadow:0 0 0 3px var(--tmwd-primary-soft)}
        .agent-trigger .at-label{min-width:0;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600;color:var(--tmwd-fg)}
        .agent-trigger .at-label.placeholder{color:var(--tmwd-muted);font-weight:500}
        .agent-trigger .at-caret{color:var(--tmwd-muted);flex:0 0 auto;display:inline-flex;transition:transform .15s ease}
        .agent-trigger.open .at-caret{transform:rotate(180deg)}
        .agent-menu{position:absolute;bottom:calc(100% + 6px);left:0;min-width:100%;max-width:min(80vw,260px);max-height:280px;overflow-y:auto;background:var(--tmwd-bg);border:1px solid var(--tmwd-border);border-radius:10px;box-shadow:0 12px 28px rgba(15,23,42,.16);padding:4px;z-index:5;display:none}
        .agent-menu.open{display:block}
        .agent-opt{display:flex;flex-direction:column;gap:1px;padding:8px 9px;border-radius:7px;cursor:pointer;min-width:0}
        .agent-opt:hover{background:var(--tmwd-body)}
        .agent-opt.active{background:var(--tmwd-primary-soft)}
        .agent-opt .ao-name{font-size:12.5px;font-weight:600;color:var(--tmwd-fg);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .agent-opt .ao-desc{font-size:10.5px;color:var(--tmwd-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .agent-opt.disabled{opacity:.5;cursor:not-allowed}
        .agent-opt.disabled:hover{background:transparent}
        .composer-row .spacer{margin-left:auto}
        /* 流式指示：贴模型 pill 右侧，发送中才显示 */
        .stream-tip{display:none;align-items:center;gap:5px;font-size:11px;color:var(--tmwd-muted);flex:0 0 auto;white-space:nowrap}
        .stream-tip.show{display:inline-flex}
        .send{height:30px;width:30px;border:0;border-radius:9px;background:var(--tmwd-primary);color:var(--tmwd-primary-fg);cursor:pointer;display:grid;place-items:center;flex:0 0 auto;transition:background .15s ease,transform .15s ease}
        .send:hover{background:var(--tmwd-primary-hover)}
        .send:active{transform:translateY(1px)}
        .send:disabled{background:#cbd5e1;cursor:not-allowed}
        .send.stop{background:#dc2626}
        .send.stop:hover{background:#b91c1c}
        .hint{display:flex;align-items:center;justify-content:center;gap:6px;font-size:11px;color:var(--tmwd-muted);margin-top:8px;min-height:14px;text-align:center}
        .hint:empty{margin-top:0;min-height:0}
        /* 会话抽屉 */
        .drawer{position:absolute;inset:0;background:var(--tmwd-bg);display:flex;flex-direction:column;transform:translateX(100%);transition:transform .22s ease;z-index:2;border-radius:20px;overflow:hidden}
        .drawer.open{transform:translateX(0)}
        .drawer header{background:var(--tmwd-bg)}
        .drawer header .title{flex:1}
        .drawer-list{flex:1;overflow-y:auto;padding:10px 12px;display:flex;flex-direction:column;gap:6px;background:var(--tmwd-body)}
        .sess{display:flex;align-items:center;gap:10px;padding:11px 12px;border:1px solid var(--tmwd-border);border-radius:10px;cursor:pointer;background:var(--tmwd-bg);transition:all .14s ease}
        .sess:hover{background:var(--tmwd-body);border-color:var(--tmwd-border-strong)}
        .sess.active{border-color:var(--tmwd-primary);background:var(--tmwd-primary-soft);box-shadow:0 0 0 1px var(--tmwd-primary-ring)}
        .sess-main{flex:1;min-width:0}
        .sess-title{font-size:13px;font-weight:600;color:var(--tmwd-fg);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .sess-meta{font-size:10.5px;color:var(--tmwd-muted);margin-top:2px}
        .drawer-empty{align-self:center;color:var(--tmwd-muted);font-size:12.5px;margin-top:40px}
      </style>
      <div class="wrap" id="wrap">
        <header>
          <span class="title" id="title">网页 Agent 对话</span>
          <!-- 使用量（放在标题栏，输入区那一栏挤不下）-->
          <div class="ctl" id="usageCtl">
            <button type="button" class="usage-btn" id="usageTrigger" title="上下文 Token">
              <svg class="usage-ring" width="20" height="20" viewBox="0 0 20 20">
                <circle cx="10" cy="10" r="8" fill="none" stroke="var(--tmwd-border)" stroke-width="3"/>
                <circle id="usageArc" cx="10" cy="10" r="8" fill="none" stroke="var(--tmwd-primary)" stroke-width="3" stroke-linecap="round" stroke-dasharray="50.26" stroke-dashoffset="50.26"/>
              </svg>
            </button>
            <div class="usage-pop" id="usagePop"></div>
          </div>
          <button class="iconbtn" id="refreshChat" title="刷新当前会话" aria-label="刷新当前会话">${ICON.refresh}</button>
          <button class="iconbtn" id="switchAgent" title="切换 Agent" aria-label="切换 Agent">${ICON.bot}</button>
          <button class="iconbtn" id="sessions" title="会话列表" aria-label="会话列表">${ICON.list}</button>
          <button class="iconbtn" id="newChat" title="新建会话" aria-label="新建会话">${ICON.plus}</button>
          <button class="iconbtn" id="close" title="关闭" aria-label="关闭">${ICON.close}</button>
        </header>
        <div class="body" id="body"></div>
        <footer id="footer">
          <div class="composer">
            <textarea id="input" rows="1" placeholder="输入消息，Enter 发送，Shift+Enter 换行"></textarea>
            <div class="goal-inputs" id="goalInputs" style="display:none">
              <input type="text" class="g-obj" id="goalObjective" placeholder="目标（一句话）">
              <input type="number" class="g-min" id="goalMinutes" min="1" value="15">
              <span class="g-unit">分钟</span>
            </div>
            <div class="composer-row">
              <!-- 思考等级 -->
              <div class="ctl" id="reasoningCtl">
                <button type="button" class="ctl-trigger" id="reasoningTrigger" title="思考等级，下一轮消息生效" aria-haspopup="listbox">
                  <span id="reasoningLabel">中</span><span class="ct-caret">${ICON.chevron}</span>
                </button>
                <div class="ctl-menu" id="reasoningMenu" role="listbox"></div>
              </div>
              <!-- 执行模式（与思考等级同款 pill 下拉） -->
              <div class="ctl" id="modeCtl">
                <button type="button" class="ctl-trigger" id="modeTrigger" title="执行模式" aria-haspopup="listbox">
                  <span id="modeLabel">普通</span><span class="ct-caret">${ICON.chevron}</span>
                </button>
                <div class="ctl-menu" id="modeMenu" role="listbox"></div>
              </div>
              <!-- 模型 -->
              <div class="ctl" id="modelCtl">
                <button type="button" class="ctl-trigger" id="modelTrigger" title="切换模型，下一轮消息生效" aria-haspopup="listbox">
                  <span class="ct-text" id="modelLabel">选择模型</span><span class="ct-caret">${ICON.chevron}</span>
                </button>
                <div class="ctl-menu wide" id="modelMenu" role="listbox">
                  <div class="ctl-search"><input type="text" id="modelSearch" placeholder="搜索模型…" autocomplete="off" spellcheck="false"></div>
                  <div id="modelOptions"></div>
                </div>
              </div>
              <span class="stream-tip" id="streamTip"><span class="spinner"></span>流式中…</span>
              <span class="spacer"></span>
              <button class="iconbtn" id="clearChat" title="清空对话" aria-label="清空对话">${ICON.eraser}</button>
              <button class="send" id="send" title="发送" aria-label="发送">${ICON.send}</button>
            </div>
          </div>
          <div class="hint" id="hint"></div>
        </footer>
        <div class="drawer" id="drawer">
          <header>
            <span class="title">会话列表</span>
            <button class="iconbtn" id="drawerRefresh" title="刷新" aria-label="刷新">${ICON.refresh}</button>
            <button class="iconbtn" id="drawerClose" title="关闭" aria-label="关闭">${ICON.close}</button>
          </header>
          <div class="drawer-list" id="drawerList"></div>
        </div>
      </div>`;
    els.ICON = ICON;
    const mountHost = () => {
      if (!host.isConnected) (document.body || document.documentElement).appendChild(host);
    };
    mountHost();
    // 面板整棵树在 Shadow DOM 里，重挂同一个 host 即可原样恢复消息流、输入框内容
    // 与全部事件监听，不必重建也不会丢正在进行的会话。
    if (window.__tmwdMountGuard) window.__tmwdMountGuard(() => host, mountHost);
    els.wrap = shadow.getElementById('wrap');
    els.title = shadow.getElementById('title');
    els.body = shadow.getElementById('body');
    els.footer = shadow.getElementById('footer');
    els.input = shadow.getElementById('input');
    els.send = shadow.getElementById('send');
    els.hint = shadow.getElementById('hint');
    els.drawer = shadow.getElementById('drawer');
    els.drawerList = shadow.getElementById('drawerList');
    els.switchAgent = shadow.getElementById('switchAgent');
    // 思考等级
    els.reasoningCtl = shadow.getElementById('reasoningCtl');
    els.reasoningTrigger = shadow.getElementById('reasoningTrigger');
    els.reasoningLabel = shadow.getElementById('reasoningLabel');
    els.reasoningMenu = shadow.getElementById('reasoningMenu');
    // 模式
    els.modeCtl = shadow.getElementById('modeCtl');
    els.modeTrigger = shadow.getElementById('modeTrigger');
    els.modeLabel = shadow.getElementById('modeLabel');
    els.modeMenu = shadow.getElementById('modeMenu');
    els.goalInputs = shadow.getElementById('goalInputs');
    els.goalObjective = shadow.getElementById('goalObjective');
    els.goalMinutes = shadow.getElementById('goalMinutes');
    // 模型
    els.modelCtl = shadow.getElementById('modelCtl');
    els.modelTrigger = shadow.getElementById('modelTrigger');
    els.modelLabel = shadow.getElementById('modelLabel');
    els.modelMenu = shadow.getElementById('modelMenu');
    els.modelSearch = shadow.getElementById('modelSearch');
    els.modelOptions = shadow.getElementById('modelOptions');
    els.streamTip = shadow.getElementById('streamTip');
    // 使用量
    els.usageCtl = shadow.getElementById('usageCtl');
    els.usageTrigger = shadow.getElementById('usageTrigger');
    els.usageArc = shadow.getElementById('usageArc');
    els.usagePop = shadow.getElementById('usagePop');

    shadow.getElementById('close').addEventListener('click', () => togglePanel(false));
    shadow.getElementById('drawerClose').addEventListener('click', () => toggleDrawer(false));
    shadow.getElementById('clearChat').addEventListener('click', () => { if (!state.sending) startFreshSession(); });
    els.switchAgent.addEventListener('click', () => { if (!state.sending) showPicker(); });
    els.send.addEventListener('click', onSend);
    const autoGrow = () => { const t = els.input; t.style.height = 'auto'; t.style.height = Math.min(t.scrollHeight, 150) + 'px'; };
    els.input.addEventListener('input', autoGrow);
    els.input.addEventListener('keydown', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(); } });

    // 思考等级下拉
    renderReasoningMenu();
    els.reasoningTrigger.addEventListener('click', (e) => { e.stopPropagation(); toggleCtlMenu(els.reasoningMenu, !els.reasoningMenu.classList.contains('open')); });
    els.reasoningMenu.addEventListener('click', (e) => {
      const opt = e.target.closest('.ctl-opt'); if (!opt) return;
      state.reasoningEffort = opt.dataset.value || '';
      renderReasoningMenu(); renderReasoningLabel(); toggleCtlMenu(els.reasoningMenu, false);
    });
    // 模式下拉（与思考等级同款 pill）
    renderModeMenu();
    els.modeTrigger.addEventListener('click', (e) => { e.stopPropagation(); toggleCtlMenu(els.modeMenu, !els.modeMenu.classList.contains('open')); });
    els.modeMenu.addEventListener('click', (e) => {
      const opt = e.target.closest('.ctl-opt'); if (!opt) return;
      state.mode = opt.dataset.value || 'interact';
      renderModeMenu(); renderModeLabel();
      if (els.goalInputs) els.goalInputs.style.display = state.mode === 'goal' ? 'flex' : 'none';
      toggleCtlMenu(els.modeMenu, false);
    });
    // 模型下拉（带搜索框）
    els.modelTrigger.addEventListener('click', (e) => {
      e.stopPropagation();
      const open = !els.modelMenu.classList.contains('open');
      toggleCtlMenu(els.modelMenu, open);
      if (open) {
        // 每次打开都从全量列表起步，避免上次的过滤词把选项藏起来。
        state.modelQuery = '';
        if (els.modelSearch) els.modelSearch.value = '';
        renderModelMenu();
        setTimeout(() => els.modelSearch && els.modelSearch.focus(), 0);
      }
    });
    els.modelSearch.addEventListener('input', () => {
      state.modelQuery = els.modelSearch.value || '';
      renderModelMenu();
    });
    // 搜索框内的按键不要冒泡到面板/页面（Enter 选中首个匹配项，Esc 收起）。
    els.modelSearch.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Escape') { toggleCtlMenu(els.modelMenu, false); return; }
      if (e.key !== 'Enter') return;
      e.preventDefault();
      const first = filteredModels()[0];
      if (first) applyModelSelection(first.id);
    });
    els.modelMenu.addEventListener('click', (e) => {
      const opt = e.target.closest('.ctl-opt'); if (!opt || !opt.dataset.value) return;
      applyModelSelection(opt.dataset.value);
    });
    // 使用量弹层
    els.usageTrigger.addEventListener('click', (e) => { e.stopPropagation(); els.usagePop.classList.toggle('open'); });
    // 点击面板外关闭所有下拉/弹层
    document.addEventListener('click', (e) => {
      const path = e.composedPath?.() || [];
      if (els.reasoningMenu?.classList.contains('open') && !path.includes(els.reasoningCtl)) toggleCtlMenu(els.reasoningMenu, false);
      if (els.modeMenu?.classList.contains('open') && !path.includes(els.modeCtl)) toggleCtlMenu(els.modeMenu, false);
      if (els.modelMenu?.classList.contains('open') && !path.includes(els.modelCtl)) toggleCtlMenu(els.modelMenu, false);
      if (els.usagePop?.classList.contains('open') && !path.includes(els.usageCtl)) els.usagePop.classList.remove('open');
    });
    shadow.getElementById('newChat').addEventListener('click', onNewSession);
    shadow.getElementById('sessions').addEventListener('click', () => toggleDrawer(true));
    shadow.getElementById('drawerRefresh').addEventListener('click', refreshSessions);
    shadow.getElementById('refreshChat').addEventListener('click', onRefreshChat);
  }

  function toggleCtlMenu(menu, open) {
    if (!menu) return;
    // 互斥：打开一个先收起其它。
    if (open) {
      [els.reasoningMenu, els.modeMenu, els.modelMenu].forEach((m) => { if (m && m !== menu) m.classList.remove('open'); });
      if (els.usagePop) els.usagePop.classList.remove('open');
    }
    menu.classList.toggle('open', open);
  }

  function renderReasoningMenu() {
    if (!els.reasoningMenu) return;
    els.reasoningMenu.innerHTML = '';
    for (const o of REASONING_OPTIONS) {
      const el = document.createElement('div');
      el.className = 'ctl-opt' + (o.value === state.reasoningEffort ? ' active' : '');
      el.dataset.value = o.value;
      el.textContent = o.label;
      els.reasoningMenu.appendChild(el);
    }
  }

  function renderReasoningLabel() {
    if (!els.reasoningLabel) return;
    const o = REASONING_OPTIONS.find((x) => x.value === state.reasoningEffort) || REASONING_OPTIONS[0];
    els.reasoningLabel.textContent = o.label;
  }

  function renderModeMenu() {
    if (!els.modeMenu) return;
    els.modeMenu.innerHTML = '';
    for (const m of MODE_OPTIONS) {
      const el = document.createElement('div');
      el.className = 'ctl-opt' + (m.value === state.mode ? ' active' : '');
      el.dataset.value = m.value;
      el.title = m.title;
      el.textContent = m.label;
      els.modeMenu.appendChild(el);
    }
  }

  function renderModeLabel() {
    if (!els.modeLabel) return;
    const m = MODE_OPTIONS.find((x) => x.value === state.mode) || MODE_OPTIONS[0];
    els.modeLabel.textContent = m.label;
  }

  // 选项与触发器统一显示模型 ID：显示名往往很长，那一栏放不下。
  function modelText(m) {
    return (m && (m.id || m.name)) || '';
  }

  // 按搜索词过滤：名称/ID/说明任一命中即保留，空词返回全量。
  function filteredModels() {
    const q = (state.modelQuery || '').trim().toLowerCase();
    if (!q) return state.models;
    return state.models.filter((m) => {
      const hay = [m.id, m.name, m.description].filter(Boolean).join(' ').toLowerCase();
      return hay.includes(q);
    });
  }

  function applyModelSelection(id) {
    state.selectedModel = id || '';
    state.modelQuery = '';
    if (els.modelSearch) els.modelSearch.value = '';
    // 记住选择：刷新页面/重开面板后仍显示用户选的模型，而不是回退到 agent 默认。
    // 连 agentId 一起写，换 agent 时旧选择不会串到新 agent 上。
    writePanelState({ model: state.selectedModel, agentId: state.selectedAgent });
    renderModelMenu(); renderModelLabel(); renderUsage();
    toggleCtlMenu(els.modelMenu, false);
  }

  function renderModelMenu() {
    if (!els.modelOptions) return;
    els.modelOptions.innerHTML = '';
    const list = filteredModels();
    if (!list.length) {
      const el = document.createElement('div');
      el.className = 'ctl-opt';
      el.textContent = state.models.length ? '无匹配模型' : '暂无可用模型';
      els.modelOptions.appendChild(el); return;
    }
    for (const m of list) {
      const el = document.createElement('div');
      el.className = 'ctl-opt' + (m.id === state.selectedModel ? ' active' : '');
      el.dataset.value = m.id;
      // 选项只留一行 ID，显示名/说明放 tooltip，避免菜单被撑高撑宽。
      el.title = [m.id, m.name && m.name !== m.id ? m.name : '', m.description].filter(Boolean).join('\n');
      const name = document.createElement('div'); name.className = 'co-name';
      const text = document.createElement('span'); text.className = 'co-text'; text.textContent = modelText(m);
      name.appendChild(text);
      // 标出 agent 自身配置的模型，便于在长列表里认出「默认那一个」。
      if (m.id && m.id === state.defaultModel) {
        const tag = document.createElement('span'); tag.className = 'co-tag'; tag.textContent = '默认';
        name.appendChild(tag);
      }
      el.appendChild(name);
      els.modelOptions.appendChild(el);
    }
  }

  function renderModelLabel() {
    if (!els.modelLabel) return;
    const m = state.models.find((x) => x.id === state.selectedModel);
    const full = m ? modelText(m) : (state.selectedModel || '');
    els.modelLabel.textContent = full || '选择模型';
    // 触发器上做省略号截断，完整名称落在 tooltip 里。
    if (els.modelTrigger) {
      els.modelTrigger.title = full
        ? `当前模型：${state.selectedModel || full}${state.selectedModel && state.selectedModel === state.defaultModel ? '（Agent 默认）' : ''}\n点击切换，下一轮消息生效`
        : '切换模型，下一轮消息生效';
    }
  }

  function selectedModelMaxTokens() {
    const m = state.models.find((x) => x.id === state.selectedModel);
    return Number(m && m.max_context_tokens) || 0;
  }

  function renderUsage() {
    if (!els.usageArc || !els.usagePop) return;
    const u = state.lastUsage || {};
    const input = Math.max(0, Number(u.prompt || 0));
    const max = selectedModelMaxTokens();
    const ratio = max > 0 ? Math.min(Math.max(input / max, 0), 1) : 0;
    const circ = 2 * Math.PI * 8; // r=8
    els.usageArc.setAttribute('stroke-dasharray', String(circ));
    els.usageArc.setAttribute('stroke-dashoffset', String(circ * (1 - ratio)));
    const fmt = (n) => {
      n = Number(n || 0);
      if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
      if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'K';
      return String(n);
    };
    const rows = [
      ['当前输入', fmt(input) + ' tokens'],
      ['模型上限', max > 0 ? fmt(max) + ' tokens' : '上限未知'],
      ['当前输出', fmt(u.completion) + ' tokens'],
    ];
    if ((u.cached || 0) > 0) rows.push(['缓存读取', fmt(u.cached) + ' tokens']);
    if ((u.cacheCreation || 0) > 0) rows.push(['缓存写入', fmt(u.cacheCreation) + ' tokens']);
    if ((u.reasoning || 0) > 0) rows.push(['推理 Token', fmt(u.reasoning) + ' tokens']);
    rows.push(['实际占比', max > 0 ? Math.round(ratio * 100) + '%' : '无法计算']);
    if (u.total != null) rows.push(['当前总计', fmt(u.total) + ' tokens']);
    els.usagePop.innerHTML = '';
    for (const [k, v] of rows) {
      const row = document.createElement('div'); row.className = 'up-row';
      const a = document.createElement('span'); a.textContent = k;
      const b = document.createElement('span'); b.textContent = v;
      row.appendChild(a); row.appendChild(b); els.usagePop.appendChild(row);
    }
  }

  function togglePanel(open) {
    ensurePanel();
    state.open = open !== undefined ? open : !state.open;
    els.wrap.classList.toggle('open', state.open);
    // 面板打开时隐藏入口气泡，关掉再显示。
    const ind = document.getElementById('ljq-ind');
    if (ind) ind.style.display = state.open ? 'none' : '';
    writePanelState({ open: state.open });
    if (state.open) runOpenFlow();
  }

  // 面板打开流程：拉可选 agent → 有记忆且仍可用则直接进聊天，否则进首屏选择。
  // 若本 tab 存过进行中的会话（导航重注入场景），直接恢复该会话历史。
  async function openFlow() {
    await refreshAgents();
    if (!state.agents.length) { showPicker(); return; }
    if (!state.selectedAgent) {
      const remembered = await storedAgentId();
      if (remembered && state.agents.some(a => a.id === remembered)) {
        state.selectedAgent = remembered;
      }
    }
    if (state.selectedAgent) {
      await enterChat();
      if (!state.conversationId && !state.messages.length) {
        await resumeConversationIfStored();
      }
    } else { showPicker(); }
  }

  // 从 storage 恢复本 tab 上次的会话（刷新/导航重注入后 SW 一旦连上即触发）。
  // 没存 conversationId 或恢复失败都静默——空会话态让用户正常发新消息即可。
  async function resumeConversationIfStored() {
    if (state.conversationId || state.messages.length) return;
    const saved = await readPanelState();
    const savedConv = saved && saved.conversationId;
    if (!savedConv) return;
    await loadSession(savedConv);
  }

  async function refreshAgents() {
    const agents = await sendChat('chat_list_agents', {}, () => {});
    state.agents = Array.isArray(agents) ? agents : [];
    if (state.selectedAgent && !state.agents.some(a => a.id === state.selectedAgent)) {
      state.selectedAgent = null;
    }
  }

  // 首屏：正文区渲染 agent 卡片列表；隐藏输入区。
  function showPicker() {
    state.view = 'picker';
    if (els.footer) els.footer.style.display = 'none';
    els.body.innerHTML = '';
    const wrap = document.createElement('div');
    wrap.className = 'picker';
    const head = document.createElement('div'); head.className = 'picker-head'; head.textContent = '选择一个 Agent';
    const sub = document.createElement('div'); sub.className = 'picker-sub';
    sub.textContent = '这些 Agent 已获授权操作当前浏览器客户端。';
    wrap.appendChild(head); wrap.appendChild(sub);
    if (!state.agents.length) {
      const empty = document.createElement('div'); empty.className = 'drawer-empty';
      empty.textContent = '该客户端未授权任何 Agent。请在资源页为客户端勾选并授权 Agent。';
      wrap.appendChild(empty);
    } else {
      const list = document.createElement('div'); list.className = 'picker-list';
      for (const a of state.agents) {
        const card = document.createElement('div'); card.className = 'picker-card';
        const name = document.createElement('div'); name.className = 'pc-name';
        name.textContent = a.display_name || a.name || ('Agent #' + a.id);
        card.appendChild(name);
        if (a.description || a.name) {
          const d = document.createElement('div'); d.className = 'pc-desc';
          d.textContent = a.description || a.name; card.appendChild(d);
        }
        card.addEventListener('click', () => {
          state.selectedAgent = a.id;
          rememberAgentId(a.id);
          // 换 agent 清空当前选择，让 refreshModels 去读这个 agent 自己的
          // 上次选择/默认模型，避免上一个 agent 的选择串过来。
          state.selectedModel = '';
          void enterChat();
        });
        list.appendChild(card);
      }
      wrap.appendChild(list);
    }
    els.body.appendChild(wrap);
  }

  // 进入聊天视图：显示输入区、初始化控件、拉模型列表、渲染空态。
  async function enterChat() {
    state.view = 'chat';
    if (els.footer) els.footer.style.display = '';
    renderReasoningLabel();
    renderModeMenu();
    renderModeLabel();
    renderModelLabel();
    renderUsage();
    setSending(state.sending);
    renderMessages();
    setTimeout(() => els.input && els.input.focus(), 120);
    await refreshModels();
  }

  async function refreshModels() {
    if (!state.selectedAgent) return;
    const res = await sendChat('chat_list_models', { agentId: state.selectedAgent }, () => {});
    const models = (res && Array.isArray(res.models)) ? res.models : [];
    state.models = models;
    // agent 自身配置的模型（服务端 default_model），用于菜单里标注「Agent 默认」。
    state.defaultModel = (res && res.defaultModel) || '';
    // 选中优先级：内存里已选 > 本页上次手动选的 > agent 默认 > 第一项。
    // 手动选择要盖过 agent 默认，否则每次重载面板都会把用户的选择打回默认。
    if (!state.selectedModel) {
      state.selectedModel = await savedModelForAgent(state.selectedAgent);
    }
    if (!state.selectedModel || !models.some(m => m.id === state.selectedModel)) {
      state.selectedModel = state.defaultModel || (models[0] && models[0].id) || '';
    }
    renderModelMenu();
    renderModelLabel();
    renderUsage();
  }

  function renderMessages() {
    els.body.innerHTML = '';
    if (!state.messages.length) {
      const agent = state.agents.find(a => a.id === state.selectedAgent);
      const empty = document.createElement('div');
      empty.className = 'empty';
      const icon = document.createElement('div');
      icon.className = 'empty-icon';
      icon.innerHTML = els.ICON.bot;
      const title = document.createElement('div');
      title.className = 'empty-title';
      title.textContent = agent ? ('与 ' + (agent.display_name || agent.name) + ' 开始对话') : '选择一个 Agent';
      const desc = document.createElement('div');
      desc.textContent = '描述你想在当前页面完成的事，Agent 会在这里展示回复与工具调用过程。';
      empty.appendChild(icon);
      empty.appendChild(title);
      empty.appendChild(desc);
      els.body.appendChild(empty);
      return;
    }
    // 流式增量用：记录「最后一条 assistant 消息」末段 text 的 DOM，content 增量直接改它。
    let lastStreamingTextEl = null;
    for (const m of state.messages) {
      // 历史 tool 行归入独立卡片行（无宿主气泡时单独展示）。
      if (m.role === 'tool') {
        const row = document.createElement('div');
        row.className = 'msg-row assistant';
        const holder = document.createElement('div');
        holder.className = 'bubble assistant';
        holder.appendChild(renderToolCard(m));
        row.appendChild(holder);
        els.body.appendChild(row);
        continue;
      }
      const isUser = m.role === 'user';
      const isLastMsg = m === state.messages[state.messages.length - 1];
      const row = document.createElement('div');
      row.className = 'msg-row ' + m.role;
      const bubble = document.createElement('div');
      const streaming = !isUser && m.status === 'streaming';
      bubble.className = 'bubble ' + m.role
        + (!isUser && m.status === 'error' ? ' err' : '')
        + (streaming ? ' streaming' : '');

      const head = document.createElement('div');
      head.className = 'msg-head';
      const who = document.createElement('span');
      who.textContent = isUser ? '你' : 'Agent';
      head.appendChild(who);
      if (m.createdAt) {
        const time = document.createElement('span');
        time.className = 'msg-time';
        time.textContent = formatMessageTime(m.createdAt);
        head.appendChild(time);
      }
      bubble.appendChild(head);

      // 有序 parts 时间线：text 段与工具卡片按事件顺序交错渲染。
      // 旧消息没有 parts（历史会话回放）→ 退化为工具在前 + 正文在后。
      const parts = Array.isArray(m.parts) && m.parts.length ? m.parts : null;
      if (!isUser && parts) {
        for (const p of parts) {
          if (p.type === 'text' && p.text) {
            const t = document.createElement('div'); t.className = 'msg-text';
            // 流式中保持 textContent（content 事件增量直写，避免每 delta 重解析
            // markdown 抖动）；done/error/历史回放时切 innerHTML 渲染 markdown。
            if (streaming) {
              t.textContent = p.text;
              if (isLastMsg) lastStreamingTextEl = t;
            } else {
              t.classList.add('md');
              t.innerHTML = renderMarkdown(p.text);
            }
            bubble.appendChild(t);
          } else if (p.type === 'tool' && p.call) {
            bubble.appendChild(renderToolCard(p.call));
            // 工具段在 text 之后 → 之前的 text 段不再是末段，增量直写引用失效。
            if (isLastMsg) lastStreamingTextEl = null;
          } else if (p.type === 'approval' && p.approval) {
            bubble.appendChild(renderApprovalCard(p.approval));
            if (isLastMsg) lastStreamingTextEl = null;
          }
        }
      } else if (!isUser && Array.isArray(m.toolCalls) && m.toolCalls.length) {
        for (const tc of m.toolCalls) bubble.appendChild(renderToolCard(tc));
      }

      const hasTools = !isUser && Array.isArray(m.toolCalls) && m.toolCalls.length > 0;
      // 兼容只有 content（无 parts 或 parts 无 text 段）的旧消息。
      const hasTextInParts = parts && parts.some((p) => p.type === 'text' && p.text);
      if (m.content && !hasTextInParts) {
        const text = document.createElement('div');
        text.className = 'msg-text' + (streaming ? '' : ' md');
        if (streaming) {
          text.textContent = m.content;
          if (isLastMsg) lastStreamingTextEl = text;
        } else {
          text.innerHTML = renderMarkdown(m.content);
        }
        bubble.appendChild(text);
      } else if (streaming && !hasTools && !hasTextInParts) {
        const thinking = document.createElement('div');
        thinking.className = 'thinking';
        thinking.innerHTML = '<span class="spinner"></span>思考中…';
        bubble.appendChild(thinking);
      }
      if (!isUser && streaming && m.retryHint) {
        const hint = document.createElement('div');
        hint.className = 'retry-hint';
        hint.innerHTML = '<span class="spinner"></span>' + escapeHtml(m.retryHint);
        bubble.appendChild(hint);
      }

      // 失败的最后一条 assistant 消息：显示重试按钮。刚失败的 live 消息没有真实
      // 消息 id（前端对象），onRetry 会先按会话回查后端最后一条 error 消息的 id。
      // 这里不要求 state.conversationId：建连阶段就失败的那一轮从没收到过
      // conversation 事件，会话 id 是空的，而那恰恰是最需要重试的情况。
      if (!isUser && m.status === 'error' && isLastMsg) {
        const retry = document.createElement('button');
        retry.type = 'button';
        retry.className = 'msg-retry';
        retry.textContent = '重试';
        retry.addEventListener('click', () => onRetry(m));
        bubble.appendChild(retry);
      }

      // 重载后接管的进行中回复（本地无事件流，state.sending=false）：给一个手动
      // 拉取按钮。自动轮询已在跑，但用户想立刻看进度时不必等下一个周期。
      if (!isUser && streaming && isLastMsg && !state.sending && state.conversationId) {
        const refresh = document.createElement('button');
        refresh.type = 'button';
        refresh.className = 'msg-retry';
        refresh.textContent = '刷新进度';
        refresh.addEventListener('click', () => { void refreshCurrentSession(); });
        bubble.appendChild(refresh);
      }

      row.appendChild(bubble);
      els.body.appendChild(row);
    }
    // 让 onEvent 的 content 增量路径直接复用末段 text DOM，不整棵重建。
    const _lastMsg = state.messages[state.messages.length - 1];
    if (_lastMsg && _lastMsg.role === 'assistant') _lastMsg._lastTextEl = lastStreamingTextEl;
    // 流式光标只挂在「最后一条 streaming 消息的最后一个 text 段」上；前面的 text 段
    // 已输出完，不该再闪。末尾是工具卡时 lastStreamingTextEl=null，无光标（工具 spinner 替代）。
    if (lastStreamingTextEl) lastStreamingTextEl.classList.add('cursor');
    els.body.scrollTop = els.body.scrollHeight;
  }

  function formatMessageTime(value) {
    const d = value ? new Date(value) : new Date();
    if (isNaN(d.getTime())) return '';
    const now = new Date();
    const sameDay = d.toDateString() === now.toDateString();
    return sameDay
      ? d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
      : d.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
  }

  function renderToolCard(tc) {
    const card = document.createElement('div');
    // 保留折叠态：流式增量重建气泡时，工具卡的 open 状态记在 call 上，避免被重置。
    card.className = 'tool-card' + (tc._open ? ' open' : '');
    const status = tc.status === 'running' ? '运行中' : tc.status === 'error' ? '失败' : '完成';
    const head = document.createElement('button');
    head.type = 'button';
    head.className = 'tc-head';
    head.innerHTML =
      '<span class="tc-icon">' + (tc.status === 'running' ? '<span class="spinner"></span>' : els.ICON.wrench) + '</span>' +
      '<span class="tc-name"></span>' +
      '<span class="tc-status ' + (tc.status || 'done') + '">' + status + '</span>' +
      '<span class="tc-caret">' + els.ICON.chevron + '</span>';
    head.querySelector('.tc-name').textContent = tc.name || '工具';
    head.addEventListener('click', () => { tc._open = !tc._open; card.classList.toggle('open', tc._open); });
    card.appendChild(head);

    const body = document.createElement('div');
    body.className = 'tc-body';
    if (tc.args) {
      const wrap = document.createElement('div');
      const label = document.createElement('div');
      label.className = 'tc-label';
      label.textContent = '参数';
      const block = document.createElement('div');
      block.className = 'tc-block';
      block.textContent = tc.args;
      wrap.appendChild(label);
      wrap.appendChild(block);
      body.appendChild(wrap);
    }
    if (tc.result !== undefined && tc.result !== null && tc.result !== '') {
      const wrap = document.createElement('div');
      const label = document.createElement('div');
      label.className = 'tc-label';
      label.textContent = '结果';
      const block = document.createElement('div');
      block.className = 'tc-block';
      block.textContent = typeof tc.result === 'string' ? tc.result : JSON.stringify(tc.result);
      wrap.appendChild(label);
      wrap.appendChild(block);
      body.appendChild(wrap);
    }
    if (body.childNodes.length) card.appendChild(body);
    return card;
  }

  function renderApprovalCard(approval) {
    const card = document.createElement('div');
    card.className = 'approval-card' + (approval.state === 'pending' || approval.state === 'resolving' ? ' open' : '');
    const head = document.createElement('div');
    head.className = 'ac-head';
    head.innerHTML =
      '<span class="ac-icon">' + els.ICON.shield + '</span>' +
      '<span class="ac-name">代码执行审批</span>' +
      '<span class="ac-state ' + (approval.state || 'pending') + '"></span>';
    const stateLabel = {
      pending: '待裁决', resolving: '提交中…', allowed: '已允许',
      denied: '已拒绝', expired: '已过期',
    }[approval.state || 'pending'] || '待裁决';
    head.querySelector('.ac-state').textContent = stateLabel;
    card.appendChild(head);

    const body = document.createElement('div');
    body.className = 'ac-body';
    if (approval.message) {
      const msg = document.createElement('div');
      msg.className = 'ac-msg';
      msg.textContent = approval.message;
      body.appendChild(msg);
    }
    // 逐条展示要执行的代码，让人核对后再放行。
    const runs = Array.isArray(approval.runs) ? approval.runs : [];
    for (const r of runs) {
      const lang = String((r && r.type) || 'code');
      const code = String((r && r.code) || '');
      const wrap = document.createElement('div');
      wrap.className = 'ac-run';
      const tag = document.createElement('div');
      tag.className = 'tc-label';
      tag.textContent = lang;
      const block = document.createElement('pre');
      block.className = 'ac-code';
      block.textContent = code;
      wrap.appendChild(tag);
      wrap.appendChild(block);
      body.appendChild(wrap);
    }
    card.appendChild(body);

    // 待裁决：允许 / 拒绝。resolving/终态：只读。
    if (approval.state === 'pending' || approval.state === 'resolving') {
      const actions = document.createElement('div');
      actions.className = 'ac-actions';
      const allow = document.createElement('button');
      allow.type = 'button';
      allow.className = 'ac-allow';
      allow.textContent = '允许';
      allow.disabled = approval.state === 'resolving';
      allow.addEventListener('click', () => resolveApproval(approval, 'allow'));
      const deny = document.createElement('button');
      deny.type = 'button';
      deny.className = 'ac-deny';
      deny.textContent = '拒绝';
      deny.disabled = approval.state === 'resolving';
      deny.addEventListener('click', () => resolveApproval(approval, 'deny'));
      actions.appendChild(allow);
      actions.appendChild(deny);
      card.appendChild(actions);
    }
    return card;
  }

  function resolveApproval(approval, result) {
    approval.state = 'resolving';
    renderMessages();
    sendChat('chat_resolve_approval', {
      conversationId: state.conversationId,
      confirmationId: approval.confirmationId,
      result,
      commandHash: approval.commandHash,
    }, () => {}).then((resp) => {
      // 后端 ok=true 且 result=allow → 已允许；result=deny → 已拒绝；
      // ok=false（已被处理/过期/哈希不符）→ 已过期，本轮 agent 自己会因 future 拿不到 allow 收尾。
      if (resp && resp.ok && resp.result === 'allow') {
        approval.state = 'allowed';
      } else if (resp && resp.ok && resp.result === 'deny') {
        approval.state = 'denied';
      } else {
        approval.state = 'expired';
      }
      renderMessages();
    });
  }

  function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }

  // Agent 回复按 markdown 渲染。marked + DOMPurify（agent 可能回显页面/用户内容，
  // 必须做 XSS 兜底）。流式中不调用本函数（仍 textContent），只在 done/历史回放时用。
  // marked/purify 在 manifest content_scripts 里于 content.js 之前加载；若因 CSP/加载
  // 失败缺失，回退成转义纯文本，绝不让原始 markdown 直接进 innerHTML。
  function renderMarkdown(text) {
    const src = String(text || '');
    try {
      const raw = marked.parse(src, { breaks: true, gfm: true });
      if (typeof DOMPurify !== 'undefined') {
        return DOMPurify.sanitize(raw, {
          ALLOWED_TAGS: ['p','br','strong','em','del','code','pre','a','ul','ol','li','blockquote','h1','h2','h3','h4','h5','h6','hr','span','table','thead','tbody','tr','th','td','img','sup','sub','s'],
          ALLOWED_ATTR: ['href','title','src','alt','target','rel'],
        });
      }
      // DOMPurify 缺失：不能直接信 marked 输出，回退转义。安全优先于格式。
      return escapeHtml(src).replace(/\n/g, '<br>');
    } catch (_) {
      return escapeHtml(src).replace(/\n/g, '<br>');
    }
  }

  // 主链路重试事件 → 面板里显示的一行提示。scope 由后端 on_retry 回调区分
  // （group_unavailable / group_fallback / inner_retry / candidate_switch / network）。
  function retryHintText(ev) {
    const reason = ev.reason || '';
    const prov = ev.provider || '';
    const acct = ev.account ? '/' + ev.account : '';
    const at = (ev.attempt && ev.max) ? `（第${ev.attempt}/${ev.max}次）` : '';
    if (ev.scope === 'group_unavailable') return `模型组不可用，准备换组：${reason}`;
    if (ev.scope === 'group_fallback') return `切换备份组 ${ev.to || ''}`.trim();
    if (ev.scope === 'inner_retry') return `渠道内重试 ${prov}${acct}${at}`.trim();
    if (ev.scope === 'candidate_switch') return `切换候选 ${prov}${acct}${at}`.trim();
    if (ev.scope === 'network') return `网络重试：${reason}`;
    return `重试中：${reason || ev.scope || ''}`;
  }

  // 把一次 agent 事件流应用到给定 assistant 消息对象上（send 与 retry 共用）。
  function finalizeAssistant(assistant, status) {
    // 流程结束兜底：任一 tool_call 因 id/index 匹配失败仍停在 running 的，
    // 统一收尾成最终态，避免「工具其实执行完了但卡片一直转圈」。
    if (Array.isArray(assistant.toolCalls)) {
      for (const c of assistant.toolCalls) {
        if (c.status === 'running') c.status = status === 'error' ? 'error' : 'done';
      }
    }
    // 审批卡同理：流结束了还停在 pending/resolving 的，说明没人裁决（超时/中止），
    // 翻成过期态，避免永久停在可操作按钮上。
    if (Array.isArray(assistant.parts)) {
      for (const p of assistant.parts) {
        if (p.type === 'approval' && p.approval && (p.approval.state === 'pending' || p.approval.state === 'resolving')) {
          p.approval.state = 'expired';
        }
      }
    }
    assistant.retryHint = null;
  }

  function makeStreamHandler(assistant) {
    return (ev) => {
      if (ev.type === 'conversation') {
        state.conversationId = ev.conversation_id || state.conversationId;
        writePanelState({ conversationId: state.conversationId, agentId: state.selectedAgent });
        return;
      }
      if (ev.type === 'content') {
        const txt = ev.text || '';
        assistant.content = (assistant.content || '') + txt;
        const last = assistant.parts[assistant.parts.length - 1];
        if (last && last.type === 'text') {
          last.text += txt;
          // 增量：流式中只改当前气泡末段 text 的 textContent，不整棵重建，
          // 避免每次 delta 把已展开的工具卡折叠回去 + 高频闪烁。
          if (assistant._lastTextEl && assistant._lastTextEl.isConnected) {
            assistant._lastTextEl.textContent = last.text;
            els.body.scrollTop = els.body.scrollHeight;
            return;
          }
        } else {
          assistant.parts.push({ type: 'text', text: txt });
        }
        renderMessages();
      } else if (ev.type === 'tool_call') {
        // id = agent_loop 的 tool_call_id；tool_result 回帧带同一个 id，跨轮用它精确匹配。
        const call = { id: ev.id, name: ev.name, args: typeof ev.args === 'string' ? ev.args : JSON.stringify(ev.args), status: 'running', result: undefined, index: ev.index };
        assistant.toolCalls.push(call);
        assistant.parts.push({ type: 'tool', call });
        renderMessages();
      } else if (ev.type === 'tool_result') {
        // agent_loop 的 index 是「本轮第几个 tool_call」，跨轮会重置成 0，不能当
        // 全局下标用。优先按 id 匹配（同一 tool_call_id 在 call/result 上一致），
        // 匹配不到再回退 index / 末位。
        let call = null;
        if (ev.id != null) {
          const resultId = String(ev.id);
          call = assistant.toolCalls.find((c) => c.id != null && String(c.id) === resultId) || null;
        }
        if (!call && typeof ev.index === 'number') {
          // index 在每轮内重置；优先找同 index 的仍在执行调用，避免把结果
          // 错配给前一轮已经完成的同下标工具。
          call = assistant.toolCalls.find((c) => c.status === 'running' && c.index === ev.index) || null;
        }
        if (!call) {
          call = assistant.toolCalls.find((c) => c.status === 'running') || null;
        }
        if (call) { call.status = 'done'; call.result = ev.data; }
        renderMessages();
      } else if (ev.type === 'retry') {
        assistant.retryHint = retryHintText(ev);
        renderMessages();
      } else if (ev.type === 'confirmation_required') {
        // 服务端 code_run 的一次性审批：弹卡让人核对代码并放行/拒绝。
        // 本轮 agent 会 await 裁决，裁决后才继续吐事件，所以卡片必须可操作。
        const cid = ev.confirmation_id;
        if (cid) {
          // 去重：同一 confirmation（如 retry 重发）不重复挂卡。
          if (!assistant.parts.some((p) => p.type === 'approval' && p.approval && p.approval.confirmationId === cid)) {
            const meta = (ev.metadata && typeof ev.metadata === 'object') ? ev.metadata : {};
            const approval = {
              confirmationId: cid,
              commandHash: ev.command_hash || '',
              runs: Array.isArray(meta.runs) ? meta.runs : [],
              message: ev.message || '',
              expiresAt: Number(ev.expires_at) || 0,
              state: 'pending', // pending → resolving → allowed / denied / expired
            };
            assistant.parts.push({ type: 'approval', approval });
            renderMessages();
          }
        }
      } else if (ev.type === 'done') {
        if (ev.usage && typeof ev.usage === 'object') {
          const g = (k1, k2) => Number(ev.usage[k1] ?? ev.usage[k2]) || 0;
          const prompt = g('prompt_tokens', 'input_tokens');
          const completion = g('completion_tokens', 'output_tokens');
          state.lastUsage = {
            prompt, completion,
            total: Number(ev.usage.total_tokens) || (prompt + completion),
            cached: g('cached_tokens', 'cache_read_input_tokens'),
            cacheCreation: g('cache_creation_tokens', 'cache_creation_input_tokens'),
            reasoning: g('reasoning_tokens', 'reasoning_output_tokens'),
          };
          renderUsage();
        }
        finalizeAssistant(assistant, 'done');
        assistant.status = 'done'; setSending(false); renderMessages();
      } else if (ev.type === 'error') {
        finalizeAssistant(assistant, 'error');
        assistant.status = 'error';
        assistant.content = (assistant.content || '') + (ev.message || '出错');
        assistant.parts.push({ type: 'text', text: ev.message || '出错' });
        setSending(false); renderMessages();
      } else if (ev.type === 'aborted') {
        finalizeAssistant(assistant, 'error');
        assistant.status = 'error';
        assistant.content = (assistant.content || '') + '（已中止）';
        assistant.parts.push({ type: 'text', text: '（已中止）' });
        setSending(false); renderMessages();
      }
    };
  }

  async function onSend() {
    const text = els.input.value.trim();
    if (!text || state.sending || !state.selectedAgent) return;
    // 本地即将接管活跃事件流：停掉重载后可能残留的续看轮询，避免与 live 增量打架。
    stopStreamWatch();
    // goal 模式必须填目标。
    if (state.mode === 'goal') {
      state.goalObjective = (els.goalObjective && els.goalObjective.value.trim()) || '';
      state.goalBudgetMinutes = Math.max(1, Number(els.goalMinutes && els.goalMinutes.value) || 15);
      if (!state.goalObjective) { els.hint.textContent = '目标模式请填写目标'; return; }
    }
    els.hint.textContent = '';
    const now = new Date().toISOString();
    state.messages.push({ role: 'user', content: text, createdAt: now });
    // assistant 消息用有序 parts 时间线：text 段与 tool 卡片按事件顺序交错，
    // 而不是「全部工具在前 + 全部正文在后」。这样能看出哪句话对应哪个动作。
    const assistant = { role: 'assistant', content: '', toolCalls: [], parts: [], status: 'streaming', createdAt: new Date().toISOString() };
    state.messages.push(assistant);
    els.input.value = ''; els.input.style.height = 'auto'; setSending(true);
    renderMessages();

    dispatchTurn(text, assistant);
  }

  // 发一轮 chat_send。onSend 与「没有会话可重试」时的 onRetry 共用：后者要重发
  // 原来那句 user 文本，参数必须和首发完全一致，否则重试会换掉模型/模式。
  function dispatchTurn(text, assistant) {
    sendChat('chat_send', {
      agentId: state.selectedAgent,
      content: text,
      conversationId: state.conversationId,
      url: location.href,
      title: document.title,
      model: state.selectedModel || '',
      reasoningEffort: state.reasoningEffort,
      mode: state.mode,
      goalObjective: state.goalObjective,
      goalBudgetMinutes: state.goalBudgetMinutes,
    }, makeStreamHandler(assistant));
  }

  // 把一条失败的 assistant 消息复位成「正在流」，供两条重试路径共用。
  function resetForRetry(message) {
    message.status = 'streaming';
    message.content = '';
    message.parts = [];
    message.toolCalls = [];
    message._lastTextEl = null;
    setSending(true);
    renderMessages();
  }

  // 重试最后一条失败的 assistant 消息。
  //
  // 两条路径，取决于服务端到底有没有这轮对话：
  // - 有 conversationId → chat_retry，服务端复用同一条消息、用上一条 user 消息重跑。
  // - 没有 → 这轮在建连阶段就失败了（连接超时那类），服务端既没有会话也没有消息行，
  //   chat_retry 无从下手；此时按首发重发原来那句 user 文本。之前这里直接 return，
  //   于是按钮点了没有任何请求发出。
  async function onRetry(message) {
    if (state.sending || !message) return;
    if (message.role !== 'assistant' || message.status !== 'error') return;

    if (!state.conversationId) {
      // 原始 user 文本取该 assistant 之前最近的一条 user 消息。
      const at = state.messages.indexOf(message);
      let text = '';
      for (let i = (at < 0 ? state.messages.length : at) - 1; i >= 0; i -= 1) {
        if (state.messages[i].role === 'user') { text = String(state.messages[i].content || '').trim(); break; }
      }
      if (!text || !state.selectedAgent) { els.hint.textContent = '找不到可重试的消息'; return; }
      resetForRetry(message);
      dispatchTurn(text, message);
      return;
    }

    // live 消息是前端对象没有真实 id：回查会话，取后端最后一条 error assistant 的 id。
    let messageId = message.id;
    if (messageId == null) {
      const data = await sendChat('chat_load_conversation', { conversationId: state.conversationId }, () => {});
      const rows = (data && data.messages) || [];
      for (let i = rows.length - 1; i >= 0; i -= 1) {
        if (rows[i].role === 'assistant' && rows[i].status === 'error') { messageId = rows[i].id; break; }
      }
      if (messageId == null) { els.hint.textContent = '找不到可重试的消息'; return; }
      message.id = messageId;
    }
    resetForRetry(message);
    sendChat('chat_retry', {
      conversationId: state.conversationId,
      messageId,
    }, makeStreamHandler(message));
  }

  // 手动拉一次当前会话最新状态（重载后接管进行中回复时用）。不动会话/agent 选择，
  // 只替换消息流；仍在 streaming 就重新排一次自动轮询。
  async function refreshCurrentSession() {
    const convId = state.conversationId;
    if (!convId || state.sending) return;
    stopStreamWatch();
    const data = await sendChat('chat_load_conversation', { conversationId: convId }, () => {});
    if (state.conversationId !== convId || state.sending) return;
    if (data && data.type === 'error') {
      els.hint.textContent = data.message || '会话加载失败，请稍后重试';
      return;
    }
    if (data && data.conversation) {
      state.messages = mapLoadedMessages(data);
      renderMessages();
    }
    watchStreamingIfNeeded();
  }

  // 对话页常驻刷新：手动重拉当前会话最新状态。发送中不打架，交给活跃事件流；
  // 没有会话时给一行提示。
  function onRefreshChat() {
    if (state.sending) return;
    if (!state.conversationId) {
      els.hint.textContent = '当前没有会话';
      return;
    }
    void refreshCurrentSession();
  }

  // --- 会话管理：新建 / 列表 / 载入 ---
  function setSending(sending) {
    state.sending = sending;
    if (els.send) els.send.disabled = sending || !state.selectedAgent;
    // 流式提示放在模型 pill 右侧（不再占用 hint 行），发送中显示、结束即隐。
    if (els.streamTip) els.streamTip.classList.toggle('show', !!sending);
  }

  function startFreshSession() {
    // 清空当前对话上下文，回到「空会话」态（下一条消息若无 conversationId 会新建）。
    stopStreamWatch();
    state.conversationId = '';
    state.messages = [];
    state.lastUsage = null;
    writePanelState({ conversationId: '' });
    setSending(false);
    renderMessages();
    renderUsage();
  }

  async function onNewSession() {
    if (state.sending) return;
    if (!state.selectedAgent) { startFreshSession(); return; }
    // 先本地进入空会话态，避免等待网络；服务端建好后回填 conversationId。
    startFreshSession();
    const res = await sendChat('chat_new_conversation', {
      agentId: state.selectedAgent,
      title: '',
    }, () => {});
    const conv = res && res.conversation;
    if (conv && conv.id) {
      state.conversationId = conv.id;
      writePanelState({ conversationId: conv.id, agentId: state.selectedAgent });
    }
    // 抽屉若开着，刷新列表反映新会话。
    if (state.drawerOpen) refreshSessions();
  }

  function toggleDrawer(open) {
    state.drawerOpen = open !== undefined ? open : !state.drawerOpen;
    els.drawer.classList.toggle('open', state.drawerOpen);
    if (state.drawerOpen) refreshSessions();
  }

  async function refreshSessions() {
    if (state.loadingSessions) return;
    state.loadingSessions = true;
    els.drawerList.innerHTML = '<div class="drawer-empty">加载中…</div>';
    const res = await sendChat('chat_list_conversations', {}, () => {});
    state.loadingSessions = false;
    // 桥接断开等失败：明示原因而不是误导性的「还没有会话」；再点刷新即可重试。
    if (res && res.type === 'error') {
      state.sessions = [];
      const err = document.createElement('div');
      err.className = 'drawer-empty';
      err.textContent = res.message || '会话列表加载失败，请稍后重试';
      els.drawerList.innerHTML = '';
      els.drawerList.appendChild(err);
      return;
    }
    state.sessions = (res && Array.isArray(res.conversations)) ? res.conversations : [];
    renderSessions();
  }

  function renderSessions() {
    els.drawerList.innerHTML = '';
    if (!state.sessions.length) {
      const empty = document.createElement('div'); empty.className = 'drawer-empty';
      empty.textContent = '还没有会话，发送消息或点“+”新建';
      els.drawerList.appendChild(empty); return;
    }
    for (const s of state.sessions) {
      const el = document.createElement('div');
      el.className = 'sess' + (s.id === state.conversationId ? ' active' : '');
      el.innerHTML = `<div class="sess-main">` +
        `<div class="sess-title">${escapeHtml(s.title || '未命名会话')}</div>` +
        `<div class="sess-meta">${escapeHtml(formatTime(s.updated_at))}</div></div>`;
      el.addEventListener('click', () => loadSession(s.id));
      els.drawerList.appendChild(el);
    }
  }

  function formatTime(value) {
    if (!value) return '';
    const d = new Date(value);
    if (isNaN(d.getTime())) return String(value);
    return d.toLocaleString();
  }

  // 把服务端返回的会话历史展平成面板消息流。loadSession 与「重载后续看」轮询共用。
  function mapLoadedMessages(data) {
    const out = [];
    for (const m of (data.messages || [])) {
      if (m.role !== 'user' && m.role !== 'assistant') continue;
      const toolCalls = [];
      const parts = [];
      if (m.role === 'assistant' && Array.isArray(m.tool_calls)) {
        m.tool_calls.forEach((tc, i) => {
          const result = Array.isArray(m.tool_results) ? m.tool_results[i] : undefined;
          const call = {
            name: tc.name || '',
            args: typeof tc.args === 'string' ? tc.args : JSON.stringify(tc.args || {}),
            status: tc.status || 'done',
            result: result && result.data !== undefined ? result.data : result,
            index: i,
          };
          toolCalls.push(call);
          // 历史回放没有逐段顺序信息：工具在前、正文在后（保持旧观感）。
          parts.push({ type: 'tool', call });
        });
      }
      // 正文段放最后（历史回放兼容）。仅 assistant 建 text part：renderMessages 只渲染
      // assistant 的 parts，用户正文走 m.content 兜底路径。给用户消息塞 text part 会让
      // hasTextInParts 判真却又不渲染，导致重载后用户气泡只剩「你」头、正文消失。
      if (m.content && m.role === 'assistant') parts.push({ type: 'text', text: m.content });
      out.push({
        id: m.id,
        role: m.role,
        content: m.content || '',
        toolCalls,
        parts,
        status: m.status || 'done',
        createdAt: m.created_at || m.createdAt || undefined,
      });
    }
    return out;
  }

  // --- 重载后自动续看进行中的回复 ---
  // 整页导航/刷新后本地已无 live 事件监听，但 run_agent 仍在服务端跑到底并在回合
  // 结束时把最终结果落库。这里轮询会话，直到最后一条 assistant 落定（done/error），
  // 把「永久转圈」自动解成最终答复，用户无需再手动刷新。
  // 注意：正文仅在回合结束时落库，故轮询期间只见转圈、结束后一次性见完整答复；
  // 逐字进度需服务端事件重放缓冲才能恢复，不在本次范围内。
  let _streamWatch = null;
  function stopStreamWatch() {
    if (_streamWatch) { clearTimeout(_streamWatch); _streamWatch = null; }
  }
  function lastStreamingAssistant() {
    const m = state.messages[state.messages.length - 1];
    return m && m.role === 'assistant' && m.status === 'streaming' ? m : null;
  }
  function watchStreamingIfNeeded() {
    stopStreamWatch();
    if (state.sending) return; // 本地有活跃流：事件流自己更新，不轮询
    const convId = state.conversationId;
    if (!convId || !lastStreamingAssistant()) return;
    const poll = async () => {
      _streamWatch = null;
      // 期间用户又发消息 / 切了会话：交还给 live 流或新会话，停止本轮轮询。
      if (state.sending || state.conversationId !== convId) return;
      const data = await sendChat('chat_load_conversation', { conversationId: convId }, () => {});
      if (state.sending || state.conversationId !== convId) return;
      if (data && data.conversation) {
        state.messages = mapLoadedMessages(data);
        renderMessages();
      }
      if (lastStreamingAssistant()) _streamWatch = setTimeout(poll, 3000);
    };
    _streamWatch = setTimeout(poll, 3000);
  }

  async function loadSession(convId) {
    if (state.sending || !convId) return;
    stopStreamWatch();
    const data = await sendChat('chat_load_conversation', { conversationId: convId }, () => {});
    // 失败可见：以前静默 return，断流后用户点了会话什么都没发生还以为面板坏了。
    // 抽屉此刻盖着面板，footer 的 hint 看不见，错误要同时写进抽屉列表。
    if (!data || data.type === 'error' || !data.conversation) {
      const msg = (data && data.message) || '会话载入失败，请稍后重试';
      els.hint.textContent = msg;
      els.drawerList.innerHTML = '';
      const err = document.createElement('div');
      err.className = 'drawer-empty';
      err.textContent = msg;
      els.drawerList.appendChild(err);
      return;
    }
    state.conversationId = data.conversation.id || convId;
    writePanelState({ conversationId: state.conversationId, agentId: state.selectedAgent });
    // Agent 跟随会话，避免历史会话与当前所选 agent 错配；换 agent 要重拉模型列表。
    if (data.conversation.agent_id && state.agents.some(a => a.id === data.conversation.agent_id)) {
      if (state.selectedAgent !== data.conversation.agent_id) {
        state.selectedAgent = data.conversation.agent_id;
        rememberAgentId(state.selectedAgent);
        state.selectedModel = '';
        void refreshModels();
      }
    }
    // 历史消息（含用户/助手/工具）展平进面板消息流。
    state.messages = mapLoadedMessages(data);
    renderMessages();
    renderSessions();
    toggleDrawer(false);
    // 载入的最后一条 assistant 仍是 streaming（服务端还在跑）→ 自动轮询续看到落定。
    watchStreamingIfNeeded();
  }

  // 徽标点击前从 background 查询实时 WS 状态；未连接时不创建聊天面板。
  document.addEventListener('click', async (e) => {
    const ind = e.target && e.target.closest && e.target.closest('#ljq-ind');
    if (!ind) return;
    if (ind._preventClick) { ind._preventClick = false; return; } // 拖拽后不触发
    e.preventDefault(); e.stopPropagation();
    if (statusQueryPending) return;
    statusQueryPending = true;
    try {
      if (tabId == null) { try { tabId = await getTabId(); } catch (_) { /* ignore */ } }
      const status = await getBridgeStatus();
      updateBridgeIndicator(status);
      if (!status.connected) {
        if (state.open) togglePanel(false);
        showBridgeUnavailable(status);
        return;
      }
      togglePanel();
    } finally {
      statusQueryPending = false;
    }
  }, true);

  getBridgeStatus().then(updateBridgeIndicator);

  // 整页导航重注入后恢复面板：本 tab 上次开着面板就自动重开。桥还在重连
  // 时也照开（用空态占位），SW 连上后由 bridge_status_push 的 connected 分支
  // 触发 resumeConversationIfStored 把上次的会话历史拉回来——不再因刷新时 SW
  // 没连上就整段跳过、逼用户手动点徽标。
  (async () => {
    try {
      tabId = await getTabId();
    } catch (_) { tabId = null; }
    if (tabId == null) return;
    const saved = await readPanelState();
    if (!saved || !saved.open) return;
    togglePanel(true);
  })();

  // 暴露给其它脚本/调试用。
  window.__tmwdChatPanel = { toggle: togglePanel };
})();

