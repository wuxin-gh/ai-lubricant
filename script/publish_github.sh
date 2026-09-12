#!/usr/bin/env bash
# publish_github.sh —— 一键发布：提交在途改动 → 推内网 Gitea → 造 GitHub 公开快照 → 推 6 仓 + 标签
#
# 这条命令做什么（全程不切分支、不碰工作树，多会话并行共享 worktree 时安全）：
#   0. 预检：GitHub 代理自配；自愈老脚本残留（子仓卡在 public-snapshot 分支）。
#   0.5 构建前端 dist 并随库发布（部署机一键升级从公开仓 clone，免构建部署）。
#   1. 六仓（主仓+5 子仓）自动提交在途改动并推内网 origin；内网落后别机则报错退出。
#   2. 每个子仓：用 HEAD 的树 + 上一次快照为父，git commit-tree 直接造新快照
#      commit，推 github main + 标签。纯 plumbing——零 checkout、零分支切换。
#   3. 主仓：同样用临时索引（GIT_INDEX_FILE）从 HEAD 树起步，只把 5 个子模块
#      gitlink 换成各自公开快照 SHA，commit-tree 造快照推送。
#      .gitmodules 是相对 URL（../<name>.git），内网 Gitea 与 GitHub 两边都正确解析，
#      无需改写。
#   4. 汇总报告：每仓快照 SHA、相对上次发布的变化量、GitHub 仓库地址、标签名。
#
# 用法（在主仓根目录）：
#   bash script/publish_github.sh
# 环境变量：
#   GH_BASE      GitHub 前缀（默认 https://github.com/wuxin-gh）
#   RELEASE_TAG  标签名（默认 v + 日期，如 v260909）
#   NO_TAG=1     不打标签
#   STRICT=1     不自动提交：任一仓工作树脏则报错退出（默认 0=自动提交）
#
# 安全前提：GitHub 仓库只收快照（每次发布 1 个 commit，不带开发历史）；
#   ⚠ 永远不要 git push github master——github 远端只接受 public-snapshot。
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

GH_BASE="${GH_BASE:-https://github.com/wuxin-gh}"
RELEASE_TAG="${RELEASE_TAG:-v$(date +%y%m%d)}"
NO_TAG="${NO_TAG:-0}"
STRICT="${STRICT:-0}"
REMOTE=github
PUB=public-snapshot

MAIN_REPO=ai-lubricant
SUBS=(node_server nodes user-frontend mobile device-control)
declare -A GH_REPO=(
  [node_server]=ai-lubricant-node-server
  [nodes]=ai-lubricant-nodes
  [user-frontend]=ai-lubricant-user-frontend
  [mobile]=ai-lubricant-mobile
  [device-control]=ai-lubricant-device-control
)

# ── 工具函数 ──────────────────────────────────────────────────────────────

push_with_retry() { # $@ = git push 参数。GitHub 连接常被代理/网络重置，重试至多 5 次。
  local attempt=1
  until "$@"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -gt 5 ]; then
      echo "✗ 推送重试 5 次仍失败：$*" >&2
      return 1
    fi
    echo "· 推送失败（第 $((attempt - 1)) 次），5 秒后重试…" >&2
    sleep 5
  done
}

fetch_with_retry() { # $1=仓库路径 $2=refspec；失败不致命（回退本地 public-snapshot 当父）
  local p="$1" refspec="$2" attempt=1
  until git -C "$p" fetch -q "$REMOTE" "$refspec" 2>/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -gt 5 ]; then
      return 1
    fi
    sleep 5
  done
  return 0
}

dev_branch() { # $1=仓库路径：当前开发分支名（直接读当前分支——主仓/子仓都在自己的开发分支上）
  git -C "$1" symbolic-ref --short HEAD
}

ensure_remote() { # $1=仓库路径 $2=URL
  if git -C "$1" remote get-url "$REMOTE" >/dev/null 2>&1; then
    git -C "$1" remote set-url "$REMOTE" "$2"
  else
    git -C "$1" remote add "$REMOTE" "$2"
  fi
}

ensure_proxy() {
  local cur
  cur="$(git config --global http.https://github.com/.proxy || true)"
  if [ -z "$cur" ]; then
    git config --global http.https://github.com/.proxy http://127.0.0.1:7890
    echo "· 已设置 GitHub 专用代理 http://127.0.0.1:7890（仅 github.com 走代理）"
  fi
}

# snapshot_parent <path>：该仓上一次快照 commit SHA（优先远端 main tip，回退本地分支，再回退空=首次）
# 注意：stdout 只允许输出 SHA 本身（调用方用 $() 捕获）；人类提示一律走 stderr，
# 否则多行提示混进 parent 会污染下游 rev-parse/commit-tree。
snapshot_parent() {
  local p="$1" parent=""
  if fetch_with_retry "$p" main && [ -n "$(git -C "$p" rev-parse --verify -q FETCH_HEAD)" ]; then
    parent="$(git -C "$p" rev-parse FETCH_HEAD)"
  elif git -C "$p" rev-parse --verify -q "refs/heads/${PUB}" >/dev/null 2>&1; then
    parent="$(git -C "$p" rev-parse "$PUB")"
    echo "· $p：github/main 拉不到，用本地 $PUB 当父快照（${parent:0:8}）" >&2
  else
    echo "· $p：首次发布，orphan 快照（不带历史）" >&2
  fi
  echo "$parent"
}

# diff_summary <path> <parent> <new>：相对上次发布的一行变化量（parent 空则标“首次发布”）
diff_summary() {
  local p="$1" parent="$2" new="$3"
  if [ -z "$parent" ]; then
    echo "首次发布"
  else
    git -C "$p" diff --stat "$parent" "$new" 2>/dev/null | tail -1 | sed 's/^ *//' || echo "?"
  fi
}

# publish_snapshot <path> <parent> <tree> <msg>：树不变→复用上次快照；否则 commit-tree 造新快照。
# 无论是否出新 commit，都 (a) 刷新本地 public-snapshot 指针 (b) 打当天标签——内容没变的
# 仓（如移动端当天无改动）仍要拿到当天的 v<日期> 标签。仅在新 commit 时才推 main:main。
# 输出新快照 SHA（复用/跳过时输出 parent）。人类提示走 stderr，stdout 只给 SHA。
publish_snapshot() {
  local p="$1" parent="$2" tree="$3" msg="$4" new="" PARENT_FLAGS=() made_commit=0
  if [ -n "$parent" ] && [ "$tree" = "$(git -C "$p" rev-parse -q --verify "${parent}^{tree}" 2>/dev/null)" ]; then
    echo "· $p 快照无变化，复用上次快照" >&2
    new="$parent"
  else
    if [ -n "$parent" ]; then PARENT_FLAGS=(-p "$parent"); fi
    new="$(git -C "$p" commit-tree "$tree" "${PARENT_FLAGS[@]}" -m "$msg")"
    made_commit=1
  fi
  git -C "$p" branch -f "$PUB" "$new"
  if [ "$made_commit" = "1" ]; then
    push_with_retry git -C "$p" push "$REMOTE" "$PUB:main"
  fi
  if [ "$NO_TAG" != "1" ]; then
    git -C "$p" tag -f "$RELEASE_TAG" "$new"
    push_with_retry git -C "$p" push -f "$REMOTE" "refs/tags/${RELEASE_TAG}"
  fi
  echo "$new"
}

# ── 第 0 步：预检 + 自愈 ──────────────────────────────────────────────────

ensure_proxy

# 老脚本残留自愈：子仓/主仓当前分支若是 public-snapshot（上次发布崩在切分支时留下），
# 切回各自开发分支。新脚本永不切分支，以后不会再产生此状态。
for p in node_server nodes user-frontend mobile device-control; do
  if [ "$(git -C "$p" symbolic-ref --short HEAD 2>/dev/null || true)" = "$PUB" ]; then
    dev=master; [ "$p" = mobile ] && dev=main
    git -C "$p" checkout -q "$dev"
    echo "· 自愈：$p 从 $PUB 切回 $dev（老脚本残留）"
  fi
done
if [ "$(git symbolic-ref --short HEAD 2>/dev/null || true)" = "$PUB" ]; then
  git checkout -q master
  echo "· 自愈：主仓从 $PUB 切回 master（老脚本残留）"
fi

# ── 第 0.5 步：构建前端产物（dist 随库）──────────────────────────────────
# 升级链路在部署机上 git clone GitHub 公开仓（--recurse-submodules），部署机没有
# Node 工具链——前端产物必须随库发布。放在 DIRTY 检测之前，dist 变动才能被第 1 步
# 的自动提交带上。直跑 vite（绕开 tsc -b 的存量报错），edition 与 pnpm build 默认
# 一致（online）。SKIP_FRONTEND_BUILD=1 跳过（应急用：保留上次构建的 dist）。
if [ "${SKIP_FRONTEND_BUILD:-0}" != "1" ]; then
  echo "=== 0.5/3 构建前端 dist（随库发布） ==="
  (
    cd user-frontend &&
    pnpm install --frozen-lockfile &&
    pnpm exec vite build --mode online
  ) || { echo "✗ 前端构建失败，中止发布（可 SKIP_FRONTEND_BUILD=1 应急跳过）" >&2; exit 1; }
fi

ALL_REPOS=("${SUBS[@]}" .)
declare -A DIRTY=()
for p in "${ALL_REPOS[@]}"; do
  if [ -n "$(git -C "$p" status --porcelain)" ]; then
    DIRTY[$p]=1
  fi
done

if [ "$STRICT" = "1" ] && [ "${#DIRTY[@]}" -gt 0 ]; then
  echo "✗ STRICT=1 且以下仓有未提交改动，请先提交：${!DIRTY[*]}" >&2
  exit 1
fi

for p in node_server nodes user-frontend mobile device-control; do
  ensure_remote "$p" "${GH_BASE}/${GH_REPO[$p]}.git"
done
ensure_remote . "${GH_BASE}/${MAIN_REPO}.git"

echo ""
echo "════ 发布配置 ════"
echo "  GitHub 前缀 : ${GH_BASE}"
echo "  标签       : ${RELEASE_TAG}$([ "$NO_TAG" = 1 ] && echo '（跳过）')"
echo "  自动提交   : $([ "$STRICT" = 1 ] && echo '关（STRICT=1）' || echo '开')"
echo ""

# ── 第 1 步：六仓自动提交在途改动 + 推内网 origin ────────────────────────

echo "=== 1/3 提交在途改动并推内网 Gitea ==="
for p in "${ALL_REPOS[@]}"; do
  label="$p"; [ "$p" = "." ] && label="主仓"
  if [ -n "${DIRTY[$p]:-}" ]; then
    git -C "$p" add -A
    git -C "$p" commit -qm "chore: 发布前自动提交在途改动（publish_github.sh）"
    echo "· $label：在途改动已自动提交"
  else
    echo "· $label：工作树干净"
  fi
  dev="$(dev_branch "$p")"
  git -C "$p" fetch -q origin
  behind=$(git -C "$p" rev-list --count "HEAD..origin/$dev" 2>/dev/null || echo 0)
  if [ "$behind" -gt 0 ]; then
    echo "✗ $label 落后 origin/$dev $behind 个提交（别机推过新提交），请先 git pull 处理再发布。" >&2
    exit 1
  fi
  ahead=$(git -C "$p" rev-list --count "origin/$dev..HEAD" 2>/dev/null || echo 0)
  if [ "$ahead" -gt 0 ]; then
    git -C "$p" push -q origin "$dev"
    echo "· $label：已推内网（$ahead 个提交）"
  else
    echo "· $label：内网已同步"
  fi
done

echo ""

# ── 第 2 步：各子仓造快照并推 GitHub ──────────────────────────────────────

echo "=== 2/3 子仓快照 → GitHub main ==="
declare -A PUB_SHA=()
for p in "${SUBS[@]}"; do
  parent="$(snapshot_parent "$p")"
  tree="$(git -C "$p" rev-parse -q --verify 'HEAD^{tree}')"
  src="$(git -C "$p" symbolic-ref --short HEAD) @ $(git -C "$p" rev-parse --short HEAD)"
  new="$(publish_snapshot "$p" "$parent" "$tree" "release snapshot from ${src}")"
  PUB_SHA[$p]="$new"
  echo "  [$p] 快照 $(git -C "$p" rev-parse --short "$new")（$(diff_summary "$p" "$parent" "$new")）"
done

echo ""

# ── 第 3 步：主仓快照（HEAD 树 + 子模块 gitlink 指向各公开快照）──────────

echo "=== 3/3 主仓快照 → GitHub main ==="
parent="$(snapshot_parent .)"
idx="$(mktemp)"
trap 'rm -f "$idx"' EXIT
GIT_INDEX_FILE="$idx" git read-tree HEAD
# .gitmodules 为相对 URL（../<name>.git），内网 Gitea 与 GitHub 两边解析都正确，无需改写。
for p in "${SUBS[@]}"; do
  GIT_INDEX_FILE="$idx" git update-index --cacheinfo "160000,${PUB_SHA[$p]},$p"
done
tree="$(GIT_INDEX_FILE="$idx" git write-tree)"
rm -f "$idx"; trap - EXIT
new="$(publish_snapshot . "$parent" "$tree" "release snapshot from master @ $(git rev-parse --short master)")"
echo "  [主仓] 快照 $(git rev-parse --short "$new")（$(diff_summary . "$parent" "$new")）"

# ── 报告 ──────────────────────────────────────────────────────────────────

echo ""
echo "════ 发布完成 ════"
for p in "${SUBS[@]}"; do
  echo "  ${GH_BASE}/${GH_REPO[$p]}  $(git -C "$p" rev-parse --short "${PUB_SHA[$p]}")"
done
echo "  ${GH_BASE}/${MAIN_REPO}  $(git rev-parse --short "$new")"
[ "$NO_TAG" != "1" ] && echo "  标签：${RELEASE_TAG}（6 仓同名）"
echo "  ⚠ 切记：永远不要 git push ${REMOTE} master——github 远端只接受 ${PUB} 快照。"
