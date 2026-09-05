#!/usr/bin/env bash
# publish_github.sh —— 双远端发布：内部 Gitea 保留完整开发历史，GitHub 只收发布快照。
#
# 背景：
#   * origin（内部 Gitea）承载全部开发历史，其中包含历史遗留的敏感配置与
#     AGPL 时期的在树代码，绝不能整段推到 GitHub。
#   * GitHub 仓库只接受「当前工作树的干净快照」——通过无父提交（orphan）
#     的发布分支实现：每次发布是一个全新 commit，不携带任何历史。
#
# 安全前提（缺一不可）：
#   1. 六个仓库（主仓 + 5 个 submodule）工作树必须干净——先提交在途改动。
#   2. GitHub 上已建好对应空仓库（不要初始化 README，保持空仓）。
#   3. env.ini / .env 等真实凭据已被 gitignore（已完成），凭据已轮换。
#
# 用法（在主仓根目录执行）：
#   GH_BASE="git@github.com:your-name" ./script/publish_github.sh
#   # 或 HTTPS 形式：GH_BASE="https://github.com/your-name" ...
#   # GitHub 上将创建/更新：ai-lubricant 及各 submodule 仓库的 main 分支。
#
# 之后的每次发布：直接重复运行本脚本即可——发布分支会以新快照 commit 追加，
# GitHub 侧 main 保持「每次发布一个 commit」的线性历史。
#
# ⚠️ 永远不要 `git push github master`：github 远端只允许发布分支。
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

GH_BASE="${GH_BASE:?请设置 GH_BASE，例如 git@github.com:your-name}"
RELEASE_TAG="${RELEASE_TAG:-}"  # 可选：同时给六个公开快照打同名版本标签
REMOTE=github
PUB=public-snapshot   # 本地发布分支名（仅本地，不推内部 Gitea）

# 主仓与 submodule 在 GitHub 上的仓库名
MAIN_REPO=ai-lubricant
declare -A SUBS=(
  [node_server]=ai-lubricant-node-server
  [nodes]=ai-lubricant-nodes
  [user-frontend]=ai-lubricant-user-frontend
  [mobile]=ai-lubricant-mobile
  [device-control]=ai-lubricant-device-control
)
# 内部 Gitea URL 前缀（用于改写 .gitmodules 指向 GitHub）。
# 默认从 origin 远端推导（origin 即内部 Gitea 主仓地址，剥去主仓名即为前缀），
# 避免把内网地址硬编码进脚本；也可用环境变量显式覆盖。
GITEA_PREFIX="${GITEA_PREFIX:-$(git remote get-url origin | sed 's#ai-lubricant\.git$##')}"

# ── 工具函数 ──────────────────────────────────────────────────────────────

push_with_retry() { # $@ = git push 参数。GitHub 连接常被代理/网络重置，重试至多 5 次。
  local attempt=1
  until git push "$@"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -gt 5 ]; then
      echo "✗ 推送重试 5 次仍失败：git push $*" >&2
      return 1
    fi
    echo "· 推送失败（第 $((attempt - 1)) 次），5 秒后重试…" >&2
    sleep 5
  done
}

ensure_remote() { # $1=仓库路径 $2=URL
  local path="$1" url="$2"
  (cd "$path"
    if git remote get-url "$REMOTE" >/dev/null 2>&1; then
      git remote set-url "$REMOTE" "$url"
    else
      git remote add "$REMOTE" "$url"
    fi)
}

require_clean() { # $1=仓库路径 $2=名称（用于报错信息）
  if [ -n "$(git -C "$1" status --porcelain)" ]; then
    echo "✗ $2 有未提交改动，请先提交或贮藏后再发布。" >&2
    exit 1
  fi
}

# ── 第 0 步：主仓工作树必须干净 ───────────────────────────────────────────
require_clean . "主仓"

# ── 第 1 步：各 submodule —— 当前 HEAD 树发布为 GitHub main ────────────────
declare -A PUB_SHA
for path in node_server nodes user-frontend mobile device-control; do
  repo="${SUBS[$path]}"
  url="${GH_BASE}/${repo}.git"
  ensure_remote "$path" "$url"

  require_clean "$path" "$path"
  orig_branch="$(git -C "$path" rev-parse --abbrev-ref HEAD)"

  if git -C "$path" show-ref --verify --quiet "refs/heads/${PUB}"; then
    # 已有发布分支：把当前 HEAD 的树重放到发布分支之上（新快照 commit）
    git -C "$path" checkout -q "$PUB"
    git -C "$path" read-tree --reset -u "$orig_branch"
  else
    # 首次发布：orphan 分支，无任何历史
    git -C "$path" checkout -q --orphan "$PUB"
    git -C "$path" rm -rq --cached . 2>/dev/null || true
    git -C "$path" add -A
  fi

  if git -C "$path" diff --cached --quiet; then
    echo "· $path 快照无变化，跳过 commit"
  else
    git -C "$path" commit -qm "release snapshot from ${orig_branch} @ $(git -C "$path" rev-parse --short "$orig_branch")"
  fi
  # 注意：必须在子 shell 外赋值，否则 PUB_SHA 不出 {} 范围（历史 bug：曾用 ( ... ) 包裹整个块致变量丢失）
  PUB_SHA[$path]="$(git -C "$path" rev-parse HEAD)"

  ( cd "$path" && push_with_retry "$REMOTE" "${PUB}:main" )
  if [ -n "$RELEASE_TAG" ]; then
    git -C "$path" tag -f "$RELEASE_TAG" "$PUB"
    ( cd "$path" && push_with_retry -f "$REMOTE" "refs/tags/${RELEASE_TAG}" )
  fi
  git -C "$path" checkout -q "$orig_branch"
done

# ── 第 2 步：主仓 —— 组装发布快照（改 .gitmodules + submodule 指针）──────
url="${GH_BASE}/${MAIN_REPO}.git"
ensure_remote . "$url"

if git show-ref --verify --quiet "refs/heads/${PUB}"; then
  git checkout -q "$PUB"
  git read-tree --reset -u master
else
  git checkout -q --orphan "$PUB"
  git rm -rq --cached . 2>/dev/null || true
  git add -A
fi

# 2a. .gitmodules 指向 GitHub 公开地址
sed -i "s#${GITEA_PREFIX}#${GH_BASE}/#g" .gitmodules
git add .gitmodules
git submodule sync --quiet 2>/dev/null || true

# 2b. submodule 指针指向各仓库的「公开快照 SHA」（内容与开发分支一致，SHA 属于 GitHub 历史）
for path in node_server nodes user-frontend mobile device-control; do
  (
    cd "$path"
    git checkout -q --detach "${PUB_SHA[$path]}"
  )
  git add "$path"
done

if git diff --cached --quiet; then
  echo "· 主仓快照无变化，跳过 commit"
else
  git commit -qm "release snapshot from master @ $(git rev-parse --short master)"
fi

# ── 第 3 步：推送 + 恢复开发状态 ──────────────────────────────────────────
push_with_retry "$REMOTE" "${PUB}:main"

if [ -n "$RELEASE_TAG" ]; then
  git tag -f "$RELEASE_TAG" "$PUB"
  push_with_retry -f "$REMOTE" "refs/tags/${RELEASE_TAG}"
fi

git checkout -q master
git submodule sync --quiet 2>/dev/null || true
git submodule update --init --quiet
for path in node_server nodes user-frontend mobile device-control; do
  (cd "$path" && git remote set-url "$REMOTE" "${GH_BASE}/${SUBS[$path]}.git" 2>/dev/null || true)
done

echo ""
echo "✓ 发布完成：${GH_BASE}/${MAIN_REPO}（main 分支，1 个新快照 commit）"
echo "  submodule 指针与 .gitmodules 已指向 GitHub 公开地址。"
echo "⚠  切记：永远不要执行 git push ${REMOTE} master。"
