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
