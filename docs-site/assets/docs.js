/* Ai Lubricant 文档站 — 渐进增强脚本（无依赖） */
(function () {
  'use strict';

  var root = document.documentElement;
  var stored = localStorage.getItem('docs-theme');
  var prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  root.setAttribute('data-theme', stored || (prefersDark ? 'dark' : 'light'));

  function updateThemeLabel() {
    var btn = document.getElementById('theme-toggle');
    if (btn) btn.textContent = root.getAttribute('data-theme') === 'dark' ? '☀ 浅色' : '☾ 深色';
  }

  document.addEventListener('DOMContentLoaded', function () {
    updateThemeLabel();

    var themeBtn = document.getElementById('theme-toggle');
    if (themeBtn) {
      themeBtn.addEventListener('click', function () {
        var next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        root.setAttribute('data-theme', next);
        localStorage.setItem('docs-theme', next);
        updateThemeLabel();
      });
    }

    var printBtn = document.getElementById('print-page');
    if (printBtn) printBtn.addEventListener('click', function () { window.print(); });

    var navBtn = document.getElementById('nav-toggle');
    var sidebar = document.querySelector('.sidebar');
    if (navBtn && sidebar) {
      navBtn.addEventListener('click', function () {
        var isOpen = sidebar.classList.toggle('open');
        navBtn.setAttribute('aria-expanded', String(isOpen));
      });
    }

    // 按当前文件名高亮侧边栏链接；file:// 与 HTTP 均可用。
    var current = window.location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.sidebar nav a').forEach(function (link) {
      var target = (link.getAttribute('href') || '').split('#')[0];
      if (target === current) link.classList.add('active');
    });

    // 为每个 pre 增加复制按钮。失败时不影响文档阅读。
    document.querySelectorAll('pre').forEach(function (pre) {
      var wrap = document.createElement('div');
      wrap.className = 'code-block';
      pre.parentNode.insertBefore(wrap, pre);
      wrap.appendChild(pre);

      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'copy-btn';
      button.textContent = '复制';
      button.setAttribute('aria-label', '复制代码');
      button.addEventListener('click', function () {
        var text = pre.textContent || '';
        var done = function () {
          button.textContent = '已复制';
          window.setTimeout(function () { button.textContent = '复制'; }, 1400);
        };
        if (navigator.clipboard && window.isSecureContext) {
          navigator.clipboard.writeText(text).then(done).catch(function () {});
        } else {
          var area = document.createElement('textarea');
          area.value = text;
          area.style.position = 'fixed';
          area.style.opacity = '0';
          document.body.appendChild(area);
          area.select();
          try { document.execCommand('copy'); done(); } catch (_) {}
          document.body.removeChild(area);
        }
      });
      wrap.appendChild(button);
    });

    // 标题锚点。
    document.querySelectorAll('h2[id], h3[id]').forEach(function (heading) {
      var link = document.createElement('a');
      link.className = 'anchor';
      link.href = '#' + heading.id;
      link.textContent = '#';
      link.setAttribute('aria-label', '链接到本节');
      heading.appendChild(link);
    });

    initCapabilityGraph();
    initJumpNav();
    initShots();
    initTabs();
  });

  // ---- Tab 切换：同一页面内按部署方式分视图 -------------------------------
  // 结构：<div class="tabs"><ul role="tablist"><li><button role="tab"
  //   aria-controls="t1" aria-selected="true">…</button></li>…</ul>
  //   <div id="t1" role="tabpanel" class="tab-panel is-active">…</div>…</div>
  // 切换时仅当前 tab 加 is-active / aria-selected，其余隐藏。无 tab 的页面忽略。
  function initTabs() {
    var groups = document.querySelectorAll('.tabs');
    groups.forEach(function (group) {
      var tabs = group.querySelectorAll('ul[role="tablist"] > li > button');
      tabs.forEach(function (tab) {
        tab.addEventListener('click', function () {
          var panelId = tab.getAttribute('aria-controls');
          tabs.forEach(function (other) {
            var selected = other === tab;
            other.setAttribute('aria-selected', String(selected));
            var pid = other.getAttribute('aria-controls');
            // CSS.escape 在极老浏览器/某些 file:// 下可能缺失，用 querySelector
            // 需对 id 中的特殊字符转义；本站 id 均为安全标识符，直接拼接即可。
            var panel = pid && group.querySelector('#' + pid);
            if (panel) panel.classList.toggle('is-active', selected);
          });
        });
      });
    });
  }

  // ---- 首页能力节点图（中心 → 一级枢纽 → 二级能力） --------------------------
  // 关键约定：二级能力的连线起点是它所属的枢纽，而不是中心。
  // 中心只发出 7 条线到枢纽，因此层次在视觉上是真正的两级。

  function initCapabilityGraph() {
    var graph = document.querySelector('.cap-orbit');
    if (!graph) return;

    var stage = graph.querySelector('.orbit-stage') || graph;
    var svg = graph.querySelector('.orbit-links');
    var column = graph.closest('.cap-graph-column') || graph.parentNode;
    var reset = column.querySelector('.orbit-reset');
    var zoomIn = column.querySelector('.orbit-zoom-in');
    var zoomOut = column.querySelector('.orbit-zoom-out');
    var scaleOut = column.querySelector('.orbit-scale');
    var reducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    var namespace = 'http://www.w3.org/2000/svg';
    var MIN_SCALE = 0.7;
    var MAX_SCALE = 1.6;

    var hubs = [];
    var leaves = [];
    var width = 0;
    var height = 0;
    var scale = 1;
    var frame = 0;
    var visible = !document.hidden;

    function makeLine(className) {
      var line = document.createElementNS(namespace, 'line');
      line.setAttribute('class', className);
      svg.appendChild(line);
      return line;
    }

    function baseState(el, extra) {
      var state = {
        el: el,
        x: 0, y: 0, homeX: 0, homeY: 0,
        moved: false, dragging: false, pointerId: null
      };
      for (var key in extra) state[key] = extra[key];
      return state;
    }

    // 枢纽先建，叶子要引用它。
    graph.querySelectorAll('.orbit-hub').forEach(function (el, index) {
      var hub = baseState(el, {
        hub: Number(el.getAttribute('data-hub')),
        angle: Number(el.getAttribute('data-angle')) * Math.PI / 180,
        line: makeLine('orbit-line hub-line hub-' + el.getAttribute('data-hub')),
        children: [],
        collapsed: false,
        phase: index * 0.9
      });
      el.setAttribute('role', 'button');
      el.setAttribute('aria-expanded', 'true');
      hubs.push(hub);
    });

    graph.querySelectorAll('.orbit-spoke').forEach(function (spoke, index) {
      var node = spoke.querySelector('.orbit-node');
      var hubIndex = Number(spoke.getAttribute('data-hub'));
      var hub = hubs[hubIndex];
      if (!hub) return;
      var label = (node.querySelector('.orbit-name') || node).textContent.trim();
      var leaf = baseState(node, {
        spoke: spoke,
        parent: hub,
        indexInHub: Number(spoke.getAttribute('data-i')),
        line: makeLine('orbit-line leaf-line hub-' + hubIndex),
        phase: index * 0.73
      });
      node.setAttribute('role', 'button');
      node.setAttribute('aria-label', label + '，属于' + hub.el.querySelector('strong').textContent.trim() + '；可拖动，方向键移动，Home 复位');
      hub.children.push(leaf);
      leaves.push(leaf);
    });

    function all() { return hubs.concat(leaves); }

    function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }

    function measure(preserve) {
      var rect = graph.getBoundingClientRect();
      var oldW = width;
      var oldH = height;
      width = rect.width;
      height = rect.height;
      if (!width || !height) return;
      svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
      stage.style.width = width + 'px';
      stage.style.height = height + 'px';

      var cx = width / 2;
      var cy = height / 2;
      var base = Math.min(width, height);
      var narrow = width <= 900;
      var hubRadius = base * (narrow ? 0.19 : 0.20);
      var leafRadius = base * (narrow ? 0.38 : 0.40);
      // 叶子在自己枢纽的方位角两侧展开，扇形宽度按相邻枢纽的实际间隔自适应，
      // 不越界到相邻象限。叶子数较多时在径向分多档错开，避免同弧拥挤。
      var hubGap = hubs.length > 1 ? 360 / hubs.length : 360;
      var spread = Math.min(narrow ? 50 : 56, hubGap * 0.88);

      hubs.forEach(function (hub) {
        hub.homeX = cx + Math.cos(hub.angle) * hubRadius;
        hub.homeY = cy + Math.sin(hub.angle) * hubRadius;
        var count = hub.children.length;
        // 径向分档：叶子越多，分越多层错开，从外圈向内圈依次排布；最多 3 层。
        var rings = count <= 4 ? 2 : 3;
        // 内圈半径不得压到枢纽本身：留出 hub 节点半宽 + 间距。
        var minR = hubRadius + (narrow ? 52 : 60);
        hub.children.forEach(function (leaf, i) {
          var offset = count > 1 ? (i / (count - 1) - 0.5) * spread : 0;
          var angle = hub.angle + offset * Math.PI / 180;
          leaf.angle = angle;
          // 第 i 个叶子落到第 (i % rings) 层半径：0 号落最外圈，逐层内收，但不低于 minR。
          var ring = i % rings;
          var r = Math.max(leafRadius * (1 - ring * 0.14), minR);
          leaf.homeX = cx + Math.cos(angle) * r;
          leaf.homeY = cy + Math.sin(angle) * r;
        });
      });

      // 把 home 坐标夹进画布安全区，避免靠近水平/垂直方向的节点探出边界。
      var marginX = 58;
      var marginY = 30;
      all().forEach(function (state) {
        state.homeX = clamp(state.homeX, marginX, width - marginX);
        state.homeY = clamp(state.homeY, marginY, height - marginY);
      });

      all().forEach(function (state) {
        if (preserve && state.moved && oldW && oldH) {
          state.x = clamp(state.x / oldW * width, 30, width - 30);
          state.y = clamp(state.y / oldH * height, 24, height - 24);
        } else {
          state.x = state.homeX;
          state.y = state.homeY;
        }
      });
      render(0);
    }

    function place(state, time) {
      // 枢纽保持静止，作为可稳定点击/折叠的锚点；只有二级能力轻微漂浮。
      var drift = reducedMotion || state.children || state.dragging || state.moved
        ? 0
        : Math.sin(time / 1500 + state.phase) * 2.5;
      var angle = (state.angle || 0) + Math.PI / 2;
      state.el.style.transform = 'translate(' + (state.x + Math.cos(angle) * drift) + 'px,' +
        (state.y + Math.sin(angle) * drift) + 'px) translate(-50%,-50%)';
      return { x: state.x + Math.cos(angle) * drift, y: state.y + Math.sin(angle) * drift };
    }

    function render(time) {
      var cx = width / 2;
      var cy = height / 2;
      hubs.forEach(function (hub) {
        var at = place(hub, time);
        hub.line.setAttribute('x1', cx);
        hub.line.setAttribute('y1', cy);
        hub.line.setAttribute('x2', at.x);
        hub.line.setAttribute('y2', at.y);
        hub.children.forEach(function (leaf) {
          if (hub.collapsed) return;
          var lp = place(leaf, time);
          // 起点是枢纽，不是中心——这就是二级层次。
          leaf.line.setAttribute('x1', at.x);
          leaf.line.setAttribute('y1', at.y);
          leaf.line.setAttribute('x2', lp.x);
          leaf.line.setAttribute('y2', lp.y);
        });
      });
    }

    function loop(time) {
      if (visible) render(time);
      frame = window.requestAnimationFrame(loop);
    }

    function applyScale() {
      stage.style.transform = 'scale(' + scale + ')';
      graph.classList.toggle('is-zoomed', scale > 1.001);
      if (scaleOut) scaleOut.textContent = Math.round(scale * 100) + '%';
    }

    function setScale(next) {
      scale = clamp(Number(next.toFixed(3)), MIN_SCALE, MAX_SCALE);
      applyScale();
    }

    // 指针坐标要按当前缩放还原回未缩放的舞台坐标系。
    function point(event) {
      var rect = stage.getBoundingClientRect();
      return {
        x: (event.clientX - rect.left) / scale,
        y: (event.clientY - rect.top) / scale
      };
    }

    function setCollapsed(hub, collapsed) {
      hub.collapsed = collapsed;
      hub.el.setAttribute('aria-expanded', String(!collapsed));
      hub.el.classList.toggle('is-collapsed', collapsed);
      hub.children.forEach(function (leaf) {
        leaf.spoke.hidden = collapsed;
        leaf.line.style.display = collapsed ? 'none' : '';
      });
      render(performance.now());
    }

    function bindDrag(state) {
      var el = state.el;
      el.addEventListener('pointerdown', function (event) {
        if (event.button !== undefined && event.button !== 0) return;
        event.preventDefault();
        state.dragging = true;
        state.movedDuringDrag = false;
        state.pointerId = event.pointerId;
        el.setPointerCapture(event.pointerId);
        el.classList.add('is-dragging');
        state.line.classList.add('is-active');
      });

      el.addEventListener('pointermove', function (event) {
        if (!state.dragging || event.pointerId !== state.pointerId) return;
        var p = point(event);
        state.x = clamp(p.x, el.offsetWidth / 2, width - el.offsetWidth / 2);
        state.y = clamp(p.y, el.offsetHeight / 2, height - el.offsetHeight / 2);
        state.moved = true;
        state.movedDuringDrag = true;
        render(performance.now());
      });

      var release = function (event) {
        if (!state.dragging || event.pointerId !== state.pointerId) return;
        state.dragging = false;
        state.pointerId = null;
        el.classList.remove('is-dragging');
        state.line.classList.remove('is-active');
      };
      el.addEventListener('pointerup', release);
      el.addEventListener('pointercancel', release);

      el.addEventListener('mouseenter', function () { state.line.classList.add('is-active'); });
      el.addEventListener('mouseleave', function () { if (!state.dragging) state.line.classList.remove('is-active'); });
      el.addEventListener('focus', function () { state.line.classList.add('is-active'); });
      el.addEventListener('blur', function () { state.line.classList.remove('is-active'); });

      el.addEventListener('keydown', function (event) {
        if (event.key === 'Enter' || event.key === ' ') {
          if (state.children) {
            event.preventDefault();
            setCollapsed(state, !state.collapsed);
          }
          return;
        }
        var step = event.shiftKey ? 20 : 6;
        var dx = 0;
        var dy = 0;
        if (event.key === 'ArrowLeft') dx = -step;
        else if (event.key === 'ArrowRight') dx = step;
        else if (event.key === 'ArrowUp') dy = -step;
        else if (event.key === 'ArrowDown') dy = step;
        else if (event.key === 'Home') {
          state.x = state.homeX;
          state.y = state.homeY;
          state.moved = false;
          event.preventDefault();
          render(performance.now());
          return;
        } else return;
        event.preventDefault();
        state.x = clamp(state.x + dx, el.offsetWidth / 2, width - el.offsetWidth / 2);
        state.y = clamp(state.y + dy, el.offsetHeight / 2, height - el.offsetHeight / 2);
        state.moved = true;
        render(performance.now());
      });
    }

    all().forEach(bindDrag);

    // 点击枢纽折叠/展开其二级能力；拖动过的一次点击不触发折叠。
    hubs.forEach(function (hub) {
      hub.el.addEventListener('click', function () {
        if (hub.movedDuringDrag) { hub.movedDuringDrag = false; return; }
        setCollapsed(hub, !hub.collapsed);
      });
    });

    if (reset) {
      reset.addEventListener('click', function () {
        all().forEach(function (state) {
          state.x = state.homeX;
          state.y = state.homeY;
          state.moved = false;
        });
        hubs.forEach(function (hub) { if (hub.collapsed) setCollapsed(hub, false); });
        setScale(1);
        render(performance.now());
      });
    }

    if (zoomIn) zoomIn.addEventListener('click', function () { setScale(scale + 0.15); });
    if (zoomOut) zoomOut.addEventListener('click', function () { setScale(scale - 0.15); });

    // Ctrl/⌘ + 滚轮缩放；不按修饰键时保持页面正常滚动。
    graph.addEventListener('wheel', function (event) {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      setScale(scale + (event.deltaY < 0 ? 0.1 : -0.1));
    }, { passive: false });

    document.addEventListener('visibilitychange', function () { visible = !document.hidden; });
    graph.classList.add('is-interactive');
    applyScale();
    measure(false);
    if (window.ResizeObserver) {
      new ResizeObserver(function () { measure(true); }).observe(graph);
    } else {
      window.addEventListener('resize', function () { measure(true); });
    }
    frame = window.requestAnimationFrame(loop);
    window.addEventListener('pagehide', function () { window.cancelAnimationFrame(frame); }, { once: true });
  }

  // ---- 右侧悬浮章节菜单（fixed；一级常驻，二级 h3 点击展开）------------
  // 一级为 h2；每个 h2 下的 h3 收进手风琴子菜单，避免弹出层被滚动容器裁剪。
  // 菜单按当前页标题自动生成，因此每个页面都有，无需逐页写 HTML。

  function initJumpNav() {
    var content = document.querySelector('article.content') || document.body;
    var nodes = Array.prototype.slice.call(content.querySelectorAll('h2[id], h3[id]'));
    var sections = [];
    nodes.forEach(function (node) {
      if (node.tagName === 'H2') {
        sections.push({ heading: node, children: [] });
      } else if (sections.length) {
        sections[sections.length - 1].children.push(node);
      }
    });
    if (sections.length < 2) return;

    function headingText(heading) {
      // 标题里已被插入 .anchor（"#"），取文本时要排除它。
      var clone = heading.cloneNode(true);
      var anchor = clone.querySelector('.anchor');
      if (anchor) anchor.remove();
      return clone.textContent.trim();
    }

    var nav = document.createElement('nav');
    nav.className = 'jump-nav';
    nav.setAttribute('aria-label', '本页章节目录');

    var head = document.createElement('div');
    head.className = 'jump-head';
    head.textContent = '本页目录';
    nav.appendChild(head);

    var list = document.createElement('div');
    list.className = 'jump-list';
    nav.appendChild(list);

    var links = [];
    var rows = [];

    sections.forEach(function (section) {
      var row = document.createElement('div');
      row.className = 'jump-item';

      var link = document.createElement('a');
      link.className = 'jump-l1';
      link.href = '#' + section.heading.id;
      link.textContent = headingText(section.heading);
      row.appendChild(link);
      links.push(link);

      if (section.children.length) {
        // 有二级标题：h3 收进子菜单，点击箭头展开。
        row.classList.add('has-sub');

        var caret = document.createElement('button');
        caret.type = 'button';
        caret.className = 'jump-caret';
        caret.setAttribute('aria-expanded', 'false');
        caret.setAttribute('aria-label', '展开 ' + link.textContent + ' 的子菜单');
        caret.textContent = '▸';
        row.appendChild(caret);

        var sub = document.createElement('div');
        sub.className = 'jump-sub';
        section.children.forEach(function (child) {
          var clink = document.createElement('a');
          clink.className = 'jump-l2';
          clink.href = '#' + child.id;
          clink.textContent = headingText(child);
          sub.appendChild(clink);
          links.push(clink);
        });
        row.appendChild(sub);

        // 点击展开适用于鼠标、键盘和触屏。
        caret.addEventListener('click', function (event) {
          event.preventDefault();
          event.stopPropagation();
          var open = row.classList.toggle('is-open');
          caret.setAttribute('aria-expanded', String(open));
          if (open) closeOthers(row);
        });
        rows.push({ row: row, caret: caret });
      }

      list.appendChild(row);
    });

    function closeOthers(keep) {
      rows.forEach(function (entry) {
        if (entry.row === keep) return;
        entry.row.classList.remove('is-open');
        entry.caret.setAttribute('aria-expanded', 'false');
      });
    }

    // 点菜单外部或按 Esc，收起所有已点开的子菜单。
    document.addEventListener('click', function (event) {
      if (!nav.contains(event.target)) closeOthers(null);
    });
    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') closeOthers(null);
    });

    // 悬浮：fixed 挂在 body，不占布局、不随内容滚走。
    document.body.appendChild(nav);
    // 内联 .toc 与悬浮菜单重复，脚本可用时隐藏它；无 JS 时它仍是兜底目录。
    document.body.classList.add('has-jump-nav');

    function sync() {
      var probe = window.scrollY + 130;
      var best = null;
      var bestTop = -Infinity;
      links.forEach(function (link) {
        var target = document.getElementById((link.getAttribute('href') || '').slice(1));
        if (!target) return;
        var top = target.getBoundingClientRect().top + window.scrollY;
        if (top <= probe && top > bestTop) { bestTop = top; best = link; }
      });
      // 命中的二级项若正处于折叠状态，把高亮上提到它所属的一级项，
      // 否则高亮会落在看不见的元素上，等于没有高亮。
      if (best && best.classList.contains('jump-l2')) {
        var sub = best.parentNode;
        if (sub && sub.hidden) {
          var owner = sub.parentNode ? sub.parentNode.querySelector('.jump-l1') : null;
          if (owner) best = owner;
        }
      }
      links.forEach(function (link) { link.classList.toggle('active', link === best); });
    }

    var ticking = false;
    window.addEventListener('scroll', function () {
      if (ticking) return;
      ticking = true;
      window.requestAnimationFrame(function () { sync(); ticking = false; });
    }, { passive: true });
    sync();
  }

  // ---- 截图占位与 lightbox -------------------------------------------------
  // 每个 .shot 同时含 <img> 与 .pending 占位。图片能加载 → 移除占位；
  // 加载失败（截图尚未采集）→ 移除图片与放大按钮，只留占位说明。
  // 这样缺图时文档依然可正常发布，补图后无需改 HTML。
  //
  // 一个图位可以放多张：在基名后加 -1、-2、-3 … 依次探测，命中就追加进同一
  // frame，遇到缺失即停止（上限 MAX_SHOT_IMAGES 张，避免无限探测）。因此
  // x.png + x-2.png + x-3.png 和 x-1.png + x-2.png 两种命名都能整组加载：
  // 基名存在时从 -1 开始探，且容忍 -1 缺失（兼容"基名 + -2"这种写法）；
  // 基名不存在时把 x-1.png 提升为首图，再从 -2 往后探。

  var MAX_SHOT_IMAGES = 24;
  var gallery = [];

  // 'a/x.png' + 2 → 'a/x-2.png'。保留 ?query / #hash，无扩展名时直接追加。
  function numberedSrc(src, n) {
    var mark = src.search(/[?#]/);
    var path = mark < 0 ? src : src.slice(0, mark);
    var tail = mark < 0 ? '' : src.slice(mark);
    var dot = path.lastIndexOf('.');
    if (dot > path.lastIndexOf('/')) {
      return path.slice(0, dot) + '-' + n + path.slice(dot) + tail;
    }
    return path + '-' + n + tail;
  }

  // 离屏探测一个地址是否为可解码图片。不落 DOM，失败无副作用。
  function probeImage(src, done) {
    var probe = new Image();
    probe.addEventListener('load', function () { done(probe.naturalWidth > 0); }, { once: true });
    probe.addEventListener('error', function () { done(false); }, { once: true });
    probe.src = src;
  }

  function initShots() {
    document.querySelectorAll('.shot').forEach(function (shot, slotIndex) {
      var img = shot.querySelector('img');
      var pending = shot.querySelector('.pending');
      var opener = shot.querySelector('.shot-open');
      if (!img) return;

      var frame = shot.querySelector('.frame') || shot;
      var base = img.getAttribute('src') || '';
      // lightbox 顺序按「图位序号 × 100 + 组内序号」排，与页面顺序一致。
      var order = (slotIndex + 1) * 100;

      var fail = function () {
        if (opener) opener.remove();
        else img.remove();
      };
      // n：下一个待探测的编号；tolerateMiss：是否容忍这一个编号缺失后继续。
      var ok = function (n, tolerateMiss) {
        if (pending) pending.remove();
        registerShot(shot, img, opener, order);
        probeSiblings(shot, frame, img, base, n, tolerateMiss, order);
      };

      // 基名缺失：尝试把 -1 提升为首图，让"全部带编号"的命名也能用。
      // 不复用原 <img>（它还挂着初始 load 监听，换 src 会两条探测路径齐跑、
      // 整组图片插两遍）；丢弃原元素，走统一的追加逻辑。
      var promoteFirst = function () {
        var first = numberedSrc(base, 1);
        probeImage(first, function (found) {
          if (!found) { fail(); return; }
          if (pending) pending.remove();
          fail(); // 移除含坏图的 opener 按钮
          appendShotImage(shot, frame, img, first, 1, order);
          probeSiblings(shot, frame, img, base, 2, false, order);
        });
      };

      if (img.complete) {
        // naturalWidth 为 0 表示解码失败/文件不存在。
        if (img.naturalWidth > 0) ok(1, true);
        else promoteFirst();
      } else {
        img.addEventListener('load', function () { ok(1, true); }, { once: true });
        img.addEventListener('error', promoteFirst, { once: true });
      }
    });
  }

  // 顺序探测 base 的编号兄弟图，命中即追加；未命中就停（首个编号可容忍缺失）。
  function probeSiblings(shot, frame, baseImg, base, n, tolerateMiss, order) {
    if (frame.querySelectorAll('.shot-open').length >= MAX_SHOT_IMAGES) return;
    var src = numberedSrc(base, n);
    probeImage(src, function (found) {
      if (!found) {
        if (tolerateMiss) probeSiblings(shot, frame, baseImg, base, n + 1, false, order);
        return;
      }
      appendShotImage(shot, frame, baseImg, src, n, order);
      probeSiblings(shot, frame, baseImg, base, n + 1, false, order);
    });
  }

  function appendShotImage(shot, frame, baseImg, src, n, order) {
    var alt = baseImg.getAttribute('alt') || '';
    var ordinal = frame.querySelectorAll('.shot-open').length + 1;
    var opener = document.createElement('button');
    opener.className = 'shot-open';
    opener.type = 'button';
    opener.setAttribute('aria-label', '放大截图' + (alt ? '：' + alt : '') + '（第 ' + ordinal + ' 张）');

    var img = document.createElement('img');
    img.setAttribute('src', src);
    img.setAttribute('alt', alt ? alt + '（' + ordinal + '）' : '');
    img.setAttribute('loading', 'lazy');
    opener.appendChild(img);

    frame.appendChild(opener);
    frame.classList.add('is-multi');
    markShotCount(frame);
    registerShot(shot, img, opener, order + n);
  }

  // 在 chrome 条右侧标注这一组的张数（单张不标，避免噪音）。
  function markShotCount(frame) {
    var chrome = frame.querySelector('.chrome');
    if (!chrome) return;
    var badge = chrome.querySelector('.shot-count');
    var total = frame.querySelectorAll('.shot-open').length;
    if (total < 2) {
      if (badge) badge.remove();
      return;
    }
    if (!badge) {
      badge = document.createElement('span');
      badge.className = 'shot-count';
      chrome.appendChild(badge);
    }
    badge.textContent = total + ' 张';
  }

  function registerShot(shot, img, opener, order) {
    if (!opener) return;
    var cap = shot.querySelector('figcaption');
    var entry = {
      src: img.getAttribute('src'),
      alt: img.getAttribute('alt') || '',
      caption: cap ? cap.textContent.trim() : '',
      order: order
    };
    gallery.push(entry);
    opener.addEventListener('click', function () { openLightbox(entry); });
  }

  var lb = null;
  var lbIndex = 0;
  var lastFocus = null;

  function buildLightbox() {
    var box = document.createElement('div');
    box.className = 'lightbox';
    box.hidden = true;
    box.setAttribute('role', 'dialog');
    box.setAttribute('aria-modal', 'true');
    box.setAttribute('aria-label', '截图预览');
    box.innerHTML =
      '<button class="lb-close" type="button" aria-label="关闭预览">✕</button>' +
      '<button class="lb-prev" type="button" aria-label="上一张">‹</button>' +
      '<button class="lb-next" type="button" aria-label="下一张">›</button>' +
      '<img alt=""><div class="lb-cap"></div>';
    document.body.appendChild(box);

    box.addEventListener('click', function (event) {
      if (event.target === box) closeLightbox();
    });
    box.querySelector('.lb-close').addEventListener('click', closeLightbox);
    box.querySelector('.lb-prev').addEventListener('click', function () { step(-1); });
    box.querySelector('.lb-next').addEventListener('click', function () { step(1); });
    return box;
  }

  function renderLightbox() {
    var entry = gallery[lbIndex];
    if (!entry) return;
    var img = lb.querySelector('img');
    img.setAttribute('src', entry.src);
    img.setAttribute('alt', entry.alt);
    lb.querySelector('.lb-cap').textContent = entry.caption;
    var multi = gallery.length > 1;
    lb.querySelector('.lb-prev').hidden = !multi;
    lb.querySelector('.lb-next').hidden = !multi;
  }

  function openLightbox(entry) {
    if (!lb) lb = buildLightbox();
    // 图片按加载完成先后入组，这里按页面顺序重排，保证左右翻页符合阅读顺序。
    gallery.sort(function (a, b) { return a.order - b.order; });
    lbIndex = Math.max(0, gallery.indexOf(entry));
    lastFocus = document.activeElement;
    renderLightbox();
    lb.hidden = false;
    document.body.classList.add('lb-open');
    lb.querySelector('.lb-close').focus();
    document.addEventListener('keydown', onKey);
  }

  function closeLightbox() {
    if (!lb || lb.hidden) return;
    lb.hidden = true;
    document.body.classList.remove('lb-open');
    document.removeEventListener('keydown', onKey);
    if (lastFocus && typeof lastFocus.focus === 'function') lastFocus.focus();
  }

  function step(delta) {
    if (!gallery.length) return;
    lbIndex = (lbIndex + delta + gallery.length) % gallery.length;
    renderLightbox();
  }

  function onKey(event) {
    if (event.key === 'Escape') closeLightbox();
    else if (event.key === 'ArrowLeft') step(-1);
    else if (event.key === 'ArrowRight') step(1);
  }
})();
